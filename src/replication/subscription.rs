// ABOUTME: Subscription management for logical replication on target database
// ABOUTME: Creates and manages PostgreSQL subscriptions to receive replicated data

use anyhow::{Context, Result};
use std::time::Duration;
use tokio_postgres::Client;

/// Create a subscription to a publication on the source database
pub async fn create_subscription(
    client: &Client,
    subscription_name: &str,
    source_connection_string: &str,
    publication_name: &str,
) -> Result<()> {
    // Validate subscription name to prevent SQL injection
    crate::utils::validate_postgres_identifier(subscription_name).with_context(|| {
        format!(
            "Invalid subscription name '{}': must be a valid PostgreSQL identifier",
            subscription_name
        )
    })?;

    // Validate publication name to prevent SQL injection
    crate::utils::validate_postgres_identifier(publication_name).with_context(|| {
        format!(
            "Invalid publication name '{}': must be a valid PostgreSQL identifier",
            publication_name
        )
    })?;

    tracing::info!("Creating subscription '{}'...", subscription_name);

    let query = format!(
        "CREATE SUBSCRIPTION \"{}\" CONNECTION '{}' PUBLICATION \"{}\"",
        subscription_name, source_connection_string, publication_name
    );

    match client.execute(&query, &[]).await {
        Ok(_) => {
            tracing::info!(
                "✓ Subscription '{}' created successfully",
                subscription_name
            );
            Ok(())
        }
        Err(e) => {
            let err_str = e.to_string();
            // Subscription might already exist - that's okay
            if err_str.contains("already exists") {
                tracing::info!("✓ Subscription '{}' already exists", subscription_name);
                Ok(())
            } else if err_str.contains("permission denied") || err_str.contains("must be superuser")
            {
                anyhow::bail!(
                    "Permission denied: Cannot create subscription '{}'.\n\
                     Only superusers can create subscriptions in PostgreSQL.\n\
                     Contact your database administrator to:\n\
                     1. Grant superuser: ALTER ROLE <user> WITH SUPERUSER;\n\
                     2. Or create the subscription on your behalf\n\
                     Error: {}",
                    subscription_name,
                    err_str
                )
            } else if err_str.contains("publication") && err_str.contains("does not exist") {
                anyhow::bail!(
                    "Publication does not exist: Cannot create subscription '{}'.\n\
                     The publication '{}' was not found on the source database.\n\
                     Make sure the publication exists before creating the subscription.\n\
                     Error: {}",
                    subscription_name,
                    publication_name,
                    err_str
                )
            } else if err_str.contains("could not connect to the publisher")
                || err_str.contains("connection")
            {
                anyhow::bail!(
                    "Connection failed: Cannot connect to source database for subscription '{}'.\n\
                     Please verify:\n\
                     - The source database is accessible from the target\n\
                     - The connection string is correct\n\
                     - Firewall rules allow connections\n\
                     - The source user has REPLICATION privilege\n\
                     Error: {}",
                    subscription_name,
                    err_str
                )
            } else if err_str.contains("replication slot") {
                anyhow::bail!(
                    "Replication slot error: Cannot create subscription '{}'.\n\
                     The source database may have reached the maximum number of replication slots.\n\
                     Check 'max_replication_slots' on the source database.\n\
                     Error: {}",
                    subscription_name,
                    err_str
                )
            } else {
                anyhow::bail!(
                    "Failed to create subscription '{}': {}\n\
                     \n\
                     Common causes:\n\
                     - Insufficient privileges (need SUPERUSER on target)\n\
                     - Publication does not exist on source\n\
                     - Cannot connect to source database\n\
                     - max_replication_slots limit reached on source",
                    subscription_name,
                    err_str
                )
            }
        }
    }
}

/// List all subscriptions in the database
pub async fn list_subscriptions(client: &Client) -> Result<Vec<String>> {
    let rows = client
        .query("SELECT subname FROM pg_subscription ORDER BY subname", &[])
        .await
        .context("Failed to list subscriptions")?;

    let subscriptions: Vec<String> = rows.iter().map(|row| row.get(0)).collect();

    Ok(subscriptions)
}

/// Drop a subscription
pub async fn drop_subscription(client: &Client, subscription_name: &str) -> Result<()> {
    // Validate subscription name to prevent SQL injection
    crate::utils::validate_postgres_identifier(subscription_name).with_context(|| {
        format!(
            "Invalid subscription name '{}': must be a valid PostgreSQL identifier",
            subscription_name
        )
    })?;

    tracing::info!("Dropping subscription '{}'...", subscription_name);

    let query = format!("DROP SUBSCRIPTION IF EXISTS \"{}\"", subscription_name);

    client.execute(&query, &[]).await.context(format!(
        "Failed to drop subscription '{}'",
        subscription_name
    ))?;

    tracing::info!("✓ Subscription '{}' dropped", subscription_name);
    Ok(())
}

/// Subscription state enum
#[derive(Debug, Clone, PartialEq)]
pub enum SubscriptionState {
    /// Subscription is streaming changes ('r' state)
    Streaming,
    /// Subscription is initializing ('i' state)
    Initializing,
    /// Subscription is copying data ('d' state)
    Copying,
    /// Subscription is syncing ('s' state)
    Syncing,
    /// Subscription has an error or is in unknown state
    Error(String),
    /// Subscription does not exist
    NotFound,
}

/// Detect the current state of a subscription
pub async fn detect_subscription_state(
    client: &Client,
    subscription_name: &str,
) -> Result<SubscriptionState> {
    // Query pg_stat_subscription to get subscription state
    let rows = client
        .query(
            "SELECT srsubstate FROM pg_stat_subscription WHERE subname = $1",
            &[&subscription_name],
        )
        .await
        .context(format!(
            "Failed to query subscription status for '{}'",
            subscription_name
        ))?;

    if rows.is_empty() {
        return Ok(SubscriptionState::NotFound);
    }

    let state: String = rows[0].get(0);

    match state.as_str() {
        "r" => Ok(SubscriptionState::Streaming),
        "i" => Ok(SubscriptionState::Initializing),
        "d" => Ok(SubscriptionState::Copying),
        "s" => Ok(SubscriptionState::Syncing),
        other => Ok(SubscriptionState::Error(other.to_string())),
    }
}

/// Wait for subscription to complete initial sync and enter streaming state
/// Returns when subscription reaches 'r' (ready/streaming) state
pub async fn wait_for_sync(
    client: &Client,
    subscription_name: &str,
    timeout_secs: u64,
) -> Result<()> {
    tracing::info!(
        "Waiting for subscription '{}' to sync...",
        subscription_name
    );

    let start = std::time::Instant::now();
    let timeout = Duration::from_secs(timeout_secs);

    loop {
        let row = client
            .query_one(
                "SELECT srsubstate FROM pg_stat_subscription WHERE subname = $1",
                &[&subscription_name],
            )
            .await
            .context(format!(
                "Failed to query subscription status for '{}'",
                subscription_name
            ))?;

        let state: String = row.get(0);

        match state.as_str() {
            "r" => {
                tracing::info!(
                    "✓ Subscription '{}' is ready and streaming",
                    subscription_name
                );
                return Ok(());
            }
            "i" => {
                tracing::info!("Subscription '{}' is initializing...", subscription_name);
            }
            "d" => {
                tracing::info!("Subscription '{}' is copying data...", subscription_name);
            }
            "s" => {
                tracing::info!("Subscription '{}' is syncing...", subscription_name);
            }
            _ => {
                tracing::warn!(
                    "Subscription '{}' in unexpected state: {}",
                    subscription_name,
                    state
                );
            }
        }

        if start.elapsed() > timeout {
            anyhow::bail!(
                "Timeout waiting for subscription '{}' to sync after {} seconds.\n\
                 The subscription is in state '{}' and has not reached 'ready' (streaming) state.\n\
                 \n\
                 Possible causes:\n\
                 - Large database taking longer than expected to copy\n\
                 - Network issues slowing down data transfer\n\
                 - Source database under heavy load\n\
                 \n\
                 Suggestions:\n\
                 - Increase the timeout value and try again\n\
                 - Check replication status with 'status' command\n\
                 - Monitor source database load and network connectivity",
                subscription_name,
                timeout_secs,
                state
            );
        }

        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::postgres::connect;

    #[tokio::test]
    #[ignore]
    async fn test_create_and_list_subscriptions() {
        // This test requires two databases: source and target
        let source_url = std::env::var("TEST_SOURCE_URL").unwrap();
        let target_url = std::env::var("TEST_TARGET_URL").unwrap();

        let source_client = connect(&source_url).await.unwrap();
        let target_client = connect(&target_url).await.unwrap();

        let sub_name = "test_subscription";
        let pub_name = "test_publication";
        let db_name = "postgres"; // Assume testing on postgres database
        let filter = crate::filters::ReplicationFilter::empty();

        // Create publication on source
        crate::replication::create_publication(&source_client, db_name, pub_name, &filter)
            .await
            .unwrap();

        // Clean up subscription if exists
        let _ = drop_subscription(&target_client, sub_name).await;

        // Create subscription on target
        let result = create_subscription(&target_client, sub_name, &source_url, pub_name).await;
        match &result {
            Ok(_) => println!("✓ Subscription created successfully"),
            Err(e) => {
                println!("Error creating subscription: {:?}", e);
                // If target doesn't support subscriptions, skip rest of test
                if e.to_string().contains("not supported") || e.to_string().contains("permission") {
                    println!("Skipping test - target might not support subscriptions");
                    return;
                }
            }
        }
        assert!(result.is_ok(), "Failed to create subscription");

        // List subscriptions
        let subs = list_subscriptions(&target_client).await.unwrap();
        println!("Subscriptions: {:?}", subs);
        assert!(subs.contains(&sub_name.to_string()));

        // Clean up
        drop_subscription(&target_client, sub_name).await.unwrap();
        crate::replication::drop_publication(&source_client, pub_name)
            .await
            .unwrap();
    }

    #[tokio::test]
    #[ignore]
    async fn test_drop_subscription() {
        let source_url = std::env::var("TEST_SOURCE_URL").unwrap();
        let target_url = std::env::var("TEST_TARGET_URL").unwrap();

        let source_client = connect(&source_url).await.unwrap();
        let target_client = connect(&target_url).await.unwrap();

        let sub_name = "test_drop_subscription";
        let pub_name = "test_drop_publication";
        let db_name = "postgres";
        let filter = crate::filters::ReplicationFilter::empty();

        // Create publication on source
        crate::replication::create_publication(&source_client, db_name, pub_name, &filter)
            .await
            .unwrap();

        // Create subscription on target
        create_subscription(&target_client, sub_name, &source_url, pub_name)
            .await
            .unwrap();

        // Drop it
        let result = drop_subscription(&target_client, sub_name).await;
        assert!(result.is_ok());

        // Verify it's gone
        let subs = list_subscriptions(&target_client).await.unwrap();
        assert!(!subs.contains(&sub_name.to_string()));

        // Clean up publication
        crate::replication::drop_publication(&source_client, pub_name)
            .await
            .unwrap();
    }

    #[tokio::test]
    #[ignore]
    async fn test_wait_for_sync() {
        let source_url = std::env::var("TEST_SOURCE_URL").unwrap();
        let target_url = std::env::var("TEST_TARGET_URL").unwrap();

        let source_client = connect(&source_url).await.unwrap();
        let target_client = connect(&target_url).await.unwrap();

        let sub_name = "test_wait_subscription";
        let pub_name = "test_wait_publication";
        let db_name = "postgres";
        let filter = crate::filters::ReplicationFilter::empty();

        // Create publication on source
        crate::replication::create_publication(&source_client, db_name, pub_name, &filter)
            .await
            .unwrap();

        // Clean up subscription if exists
        let _ = drop_subscription(&target_client, sub_name).await;

        // Create subscription on target
        create_subscription(&target_client, sub_name, &source_url, pub_name)
            .await
            .unwrap();

        // Wait for sync (30 second timeout)
        let result = wait_for_sync(&target_client, sub_name, 30).await;
        assert!(result.is_ok(), "Failed to wait for sync: {:?}", result);

        // Clean up
        drop_subscription(&target_client, sub_name).await.unwrap();
        crate::replication::drop_publication(&source_client, pub_name)
            .await
            .unwrap();
    }
}
