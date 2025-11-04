// ABOUTME: Sync command implementation - Phase 3 of migration
// ABOUTME: Sets up logical replication between source and target databases

use anyhow::{Context, Result};
use crate::postgres::connect;
use crate::replication::{create_publication, create_subscription, wait_for_sync};

/// Set up logical replication between source and target databases
///
/// This command:
/// 1. Creates a publication on the source database for all tables
/// 2. Creates a subscription on the target database pointing to the source
/// 3. Waits for the initial sync to complete
///
/// After this command succeeds, changes on the source will continuously
/// replicate to the target until the subscription is dropped.
pub async fn sync(
    source_url: &str,
    target_url: &str,
    publication_name: Option<&str>,
    subscription_name: Option<&str>,
    sync_timeout_secs: Option<u64>,
) -> Result<()> {
    let pub_name = publication_name.unwrap_or("seren_migration_pub");
    let sub_name = subscription_name.unwrap_or("seren_migration_sub");
    let timeout = sync_timeout_secs.unwrap_or(300); // 5 minutes default

    tracing::info!("Starting logical replication setup...");
    tracing::info!("Publication: '{}'", pub_name);
    tracing::info!("Subscription: '{}'", sub_name);

    // Connect to source database
    tracing::info!("Connecting to source database...");
    let source_client = connect(source_url)
        .await
        .context("Failed to connect to source database")?;
    tracing::info!("✓ Connected to source");

    // Connect to target database
    tracing::info!("Connecting to target database...");
    let target_client = connect(target_url)
        .await
        .context("Failed to connect to target database")?;
    tracing::info!("✓ Connected to target");

    // Create publication on source
    tracing::info!("Creating publication on source database...");
    create_publication(&source_client, pub_name)
        .await
        .context("Failed to create publication on source")?;

    // Create subscription on target
    tracing::info!("Creating subscription on target database...");
    create_subscription(&target_client, sub_name, source_url, pub_name)
        .await
        .context("Failed to create subscription on target")?;

    // Wait for initial sync to complete
    tracing::info!("Waiting for initial sync to complete (timeout: {}s)...", timeout);
    wait_for_sync(&target_client, sub_name, timeout)
        .await
        .context("Failed to wait for initial sync")?;

    tracing::info!("");
    tracing::info!("========================================");
    tracing::info!("✓ Logical replication is now active!");
    tracing::info!("========================================");
    tracing::info!("");
    tracing::info!("Changes on the source database will now continuously");
    tracing::info!("replicate to the target database.");
    tracing::info!("");
    tracing::info!("Next steps:");
    tracing::info!("  1. Run 'status' to monitor replication lag");
    tracing::info!("  2. Run 'verify' to validate data integrity");
    tracing::info!("  3. When ready, cutover to the target database");

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    #[ignore]
    async fn test_sync_command() {
        // This test requires two databases: source and target
        let source_url = std::env::var("TEST_SOURCE_URL").unwrap();
        let target_url = std::env::var("TEST_TARGET_URL").unwrap();

        let pub_name = Some("test_sync_pub");
        let sub_name = Some("test_sync_sub");
        let timeout = Some(60); // 1 minute timeout for test

        let result = sync(&source_url, &target_url, pub_name, sub_name, timeout).await;

        match &result {
            Ok(_) => println!("✓ Sync command completed successfully"),
            Err(e) => {
                println!("Error in sync command: {:?}", e);
                // If either database doesn't support logical replication, skip
                if e.to_string().contains("not supported")
                    || e.to_string().contains("permission")
                {
                    println!("Skipping test - database might not support logical replication");
                    return;
                }
            }
        }

        assert!(result.is_ok(), "Sync command failed: {:?}", result);

        // Clean up
        let target_client = connect(&target_url).await.unwrap();
        crate::replication::drop_subscription(&target_client, sub_name.unwrap())
            .await
            .unwrap();

        let source_client = connect(&source_url).await.unwrap();
        crate::replication::drop_publication(&source_client, pub_name.unwrap())
            .await
            .unwrap();
    }

    #[tokio::test]
    #[ignore]
    async fn test_sync_with_defaults() {
        let source_url = std::env::var("TEST_SOURCE_URL").unwrap();
        let target_url = std::env::var("TEST_TARGET_URL").unwrap();

        let result = sync(&source_url, &target_url, None, None, Some(60)).await;

        match &result {
            Ok(_) => println!("✓ Sync with defaults completed successfully"),
            Err(e) => {
                println!("Error in sync with defaults: {:?}", e);
                if e.to_string().contains("not supported")
                    || e.to_string().contains("permission")
                {
                    println!("Skipping test - database might not support logical replication");
                    return;
                }
            }
        }

        assert!(result.is_ok(), "Sync with defaults failed: {:?}", result);

        // Clean up using default names
        let target_client = connect(&target_url).await.unwrap();
        crate::replication::drop_subscription(&target_client, "seren_migration_sub")
            .await
            .unwrap();

        let source_client = connect(&source_url).await.unwrap();
        crate::replication::drop_publication(&source_client, "seren_migration_pub")
            .await
            .unwrap();
    }
}
