// ABOUTME: Replication monitoring utilities
// ABOUTME: Queries replication status and lag from source and target databases

use anyhow::{Context, Result};
use tokio_postgres::Client;

/// Replication statistics from the source database (publisher)
#[derive(Debug, Clone)]
pub struct SourceReplicationStats {
    pub application_name: String,
    pub state: String,
    pub sent_lsn: String,
    pub write_lsn: String,
    pub flush_lsn: String,
    pub replay_lsn: String,
    pub write_lag_ms: Option<i64>,
    pub flush_lag_ms: Option<i64>,
    pub replay_lag_ms: Option<i64>,
}

/// Subscription statistics from the target database (subscriber)
#[derive(Debug, Clone)]
pub struct SubscriptionStats {
    pub subscription_name: String,
    pub pid: Option<i32>,
    pub received_lsn: Option<String>,
    pub latest_end_lsn: Option<String>,
    pub state: String,
}

/// Get replication statistics from the source database
/// Queries pg_stat_replication to see what's being replicated to subscribers
pub async fn get_replication_lag(
    client: &Client,
    subscription_name: Option<&str>,
) -> Result<Vec<SourceReplicationStats>> {
    let query = if let Some(sub_name) = subscription_name {
        format!(
            "SELECT
                application_name,
                state,
                sent_lsn::text,
                write_lsn::text,
                flush_lsn::text,
                replay_lsn::text,
                EXTRACT(EPOCH FROM write_lag) * 1000 as write_lag_ms,
                EXTRACT(EPOCH FROM flush_lag) * 1000 as flush_lag_ms,
                EXTRACT(EPOCH FROM replay_lag) * 1000 as replay_lag_ms
            FROM pg_stat_replication
            WHERE application_name = '{}'",
            sub_name
        )
    } else {
        "SELECT
            application_name,
            state,
            sent_lsn::text,
            write_lsn::text,
            flush_lsn::text,
            replay_lsn::text,
            EXTRACT(EPOCH FROM write_lag) * 1000 as write_lag_ms,
            EXTRACT(EPOCH FROM flush_lag) * 1000 as flush_lag_ms,
            EXTRACT(EPOCH FROM replay_lag) * 1000 as replay_lag_ms
        FROM pg_stat_replication"
            .to_string()
    };

    let rows = client
        .query(&query, &[])
        .await
        .context("Failed to query replication statistics")?;

    let mut stats = Vec::new();
    for row in rows {
        stats.push(SourceReplicationStats {
            application_name: row.get(0),
            state: row.get(1),
            sent_lsn: row.get(2),
            write_lsn: row.get(3),
            flush_lsn: row.get(4),
            replay_lsn: row.get(5),
            write_lag_ms: row.get(6),
            flush_lag_ms: row.get(7),
            replay_lag_ms: row.get(8),
        });
    }

    Ok(stats)
}

/// Get subscription status from the target database
/// Queries pg_stat_subscription to see subscription state and progress
pub async fn get_subscription_status(
    client: &Client,
    subscription_name: Option<&str>,
) -> Result<Vec<SubscriptionStats>> {
    let query = if let Some(sub_name) = subscription_name {
        format!(
            "SELECT
                subname,
                pid,
                received_lsn::text,
                latest_end_lsn::text,
                srsubstate
            FROM pg_stat_subscription
            WHERE subname = '{}'",
            sub_name
        )
    } else {
        "SELECT
            subname,
            pid,
            received_lsn::text,
            latest_end_lsn::text,
            srsubstate
        FROM pg_stat_subscription"
            .to_string()
    };

    let rows = client
        .query(&query, &[])
        .await
        .context("Failed to query subscription statistics")?;

    let mut stats = Vec::new();
    for row in rows {
        stats.push(SubscriptionStats {
            subscription_name: row.get(0),
            pid: row.get(1),
            received_lsn: row.get(2),
            latest_end_lsn: row.get(3),
            state: row.get(4),
        });
    }

    Ok(stats)
}

/// Check if replication is caught up (no lag)
/// Returns true if all replication slots have < 1 second of replay lag
pub async fn is_replication_caught_up(
    client: &Client,
    subscription_name: Option<&str>,
) -> Result<bool> {
    let stats = get_replication_lag(client, subscription_name).await?;

    if stats.is_empty() {
        // No active replication
        return Ok(false);
    }

    for stat in stats {
        // Check if replay lag is > 1000ms (1 second)
        if let Some(lag_ms) = stat.replay_lag_ms {
            if lag_ms > 1000 {
                return Ok(false);
            }
        } else {
            // If lag is NULL, it might be too far behind or not streaming yet
            return Ok(false);
        }
    }

    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::postgres::connect;

    #[tokio::test]
    #[ignore]
    async fn test_get_replication_lag() {
        // This test requires a source database with active replication
        let source_url = std::env::var("TEST_SOURCE_URL").unwrap();
        let client = connect(&source_url).await.unwrap();

        let result = get_replication_lag(&client, None).await;
        match &result {
            Ok(stats) => {
                println!("✓ Replication lag query succeeded");
                println!("Found {} replication slots", stats.len());
                for stat in stats {
                    println!(
                        "  - {}: {} (replay lag: {:?}ms)",
                        stat.application_name, stat.state, stat.replay_lag_ms
                    );
                }
            }
            Err(e) => {
                println!("Error querying replication lag: {:?}", e);
                // It's okay if no replication is active
                if !e.to_string().contains("relation") && !e.to_string().contains("permission") {
                    panic!("Unexpected error: {:?}", e);
                }
            }
        }
        assert!(result.is_ok());
    }

    #[tokio::test]
    #[ignore]
    async fn test_get_subscription_status() {
        // This test requires a target database with active subscription
        let target_url = std::env::var("TEST_TARGET_URL").unwrap();
        let client = connect(&target_url).await.unwrap();

        let result = get_subscription_status(&client, None).await;
        match &result {
            Ok(stats) => {
                println!("✓ Subscription status query succeeded");
                println!("Found {} subscriptions", stats.len());
                for stat in stats {
                    println!(
                        "  - {}: state={} (pid: {:?})",
                        stat.subscription_name, stat.state, stat.pid
                    );
                }
            }
            Err(e) => {
                println!("Error querying subscription status: {:?}", e);
                // It's okay if no subscriptions exist
                if !e.to_string().contains("relation") && !e.to_string().contains("permission") {
                    panic!("Unexpected error: {:?}", e);
                }
            }
        }
        assert!(result.is_ok());
    }

    #[tokio::test]
    #[ignore]
    async fn test_is_replication_caught_up() {
        let source_url = std::env::var("TEST_SOURCE_URL").unwrap();
        let client = connect(&source_url).await.unwrap();

        let result = is_replication_caught_up(&client, None).await;
        match &result {
            Ok(caught_up) => {
                println!("✓ Caught up check succeeded: {}", caught_up);
            }
            Err(e) => {
                println!("Error checking if caught up: {:?}", e);
                // It's okay if no replication is active
                if !e.to_string().contains("relation") && !e.to_string().contains("permission") {
                    panic!("Unexpected error: {:?}", e);
                }
            }
        }
        assert!(result.is_ok());
    }

    #[tokio::test]
    #[ignore]
    async fn test_get_replication_lag_with_name() {
        let source_url = std::env::var("TEST_SOURCE_URL").unwrap();
        let client = connect(&source_url).await.unwrap();

        // Query for a specific subscription name
        let result = get_replication_lag(&client, Some("seren_migration_sub")).await;
        match &result {
            Ok(stats) => {
                println!("✓ Named replication lag query succeeded");
                println!("Found {} matching replication slots", stats.len());
            }
            Err(e) => {
                println!("Error querying named replication lag: {:?}", e);
            }
        }
        assert!(result.is_ok());
    }
}
