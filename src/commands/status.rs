// ABOUTME: Status command implementation - Check replication health
// ABOUTME: Displays real-time replication lag and subscription status

use crate::replication::{get_replication_lag, get_subscription_status, is_replication_caught_up};
use crate::{migration, postgres::connect};
use anyhow::{Context, Result};

/// Format milliseconds into a human-readable duration string
fn format_duration(ms: i64) -> String {
    if ms < 1000 {
        format!("{}ms", ms)
    } else if ms < 60_000 {
        format!("{:.1}s", ms as f64 / 1000.0)
    } else if ms < 3_600_000 {
        let minutes = ms / 60_000;
        let seconds = (ms % 60_000) / 1000;
        format!("{}m {}s", minutes, seconds)
    } else {
        let hours = ms / 3_600_000;
        let minutes = (ms % 3_600_000) / 60_000;
        format!("{}h {}m", hours, minutes)
    }
}

/// Check replication status and display health information
///
/// This command performs Phase 4 of the migration process:
/// 1. Discovers databases and filters them based on criteria
/// 2. For each filtered database:
///    - Queries pg_stat_replication on source for replication lag
///    - Queries pg_stat_subscription on target for subscription status
///    - Displays health information in human-readable format
///
/// Provides real-time monitoring of replication health including:
/// - Replication lag (write/flush/replay) per database
/// - Subscription state per database
/// - Whether each database is caught up with source
///
/// # Arguments
///
/// * `source_url` - PostgreSQL connection string for source database
/// * `target_url` - PostgreSQL connection string for target (Seren) database
/// * `filter` - Optional replication filter for database selection
///
/// # Returns
///
/// Returns `Ok(())` after displaying status information.
///
/// # Errors
///
/// This function will return an error if:
/// - Cannot connect to source or target database
/// - Cannot discover databases on source
/// - Cannot query replication statistics
/// - Cannot query subscription status
///
/// # Examples
///
/// ```no_run
/// # use anyhow::Result;
/// # use postgres_seren_replicator::commands::status;
/// # use postgres_seren_replicator::filters::ReplicationFilter;
/// # async fn example() -> Result<()> {
/// // Show status for all databases
/// status(
///     "postgresql://user:pass@source.example.com/postgres",
///     "postgresql://user:pass@target.example.com/postgres",
///     None
/// ).await?;
///
/// // Show status for specific databases only
/// let filter = ReplicationFilter::new(
///     Some(vec!["mydb".to_string(), "analytics".to_string()]),
///     None,
///     None,
///     None,
/// )?;
/// status(
///     "postgresql://user:pass@source.example.com/postgres",
///     "postgresql://user:pass@target.example.com/postgres",
///     Some(filter)
/// ).await?;
/// # Ok(())
/// # }
/// ```
pub async fn status(
    source_url: &str,
    target_url: &str,
    filter: Option<crate::filters::ReplicationFilter>,
) -> Result<()> {
    let filter = filter.unwrap_or_else(crate::filters::ReplicationFilter::empty);
    let sub_name_template = "seren_migration_sub";

    tracing::info!("Checking replication status...");
    tracing::info!("");

    // Connect to source database
    tracing::info!("Connecting to source database...");
    let source_client = connect(source_url)
        .await
        .context("Failed to connect to source database")?;

    // Discover and filter databases
    tracing::info!("Discovering databases on source...");
    let all_databases = migration::list_databases(&source_client)
        .await
        .context("Failed to list databases on source")?;

    // Apply filtering rules
    let databases: Vec<_> = all_databases
        .into_iter()
        .filter(|db| filter.should_replicate_database(&db.name))
        .collect();

    if databases.is_empty() {
        tracing::warn!("⚠ No databases matched the filter criteria");
        tracing::warn!("  No replication status to show");
        return Ok(());
    }

    tracing::info!("Found {} database(s) to check:", databases.len());
    for db in &databases {
        tracing::info!("  - {}", db.name);
    }
    tracing::info!("");

    // Connect to target database
    tracing::info!("Connecting to target database...");
    let target_client = connect(target_url)
        .await
        .context("Failed to connect to target database")?;
    tracing::info!("");

    // Check status for each database
    tracing::info!("========================================");
    tracing::info!("Replication Status Report");
    tracing::info!("========================================");
    tracing::info!("");

    let mut all_caught_up = true;
    let mut any_active = false;

    for db in &databases {
        // Build subscription name for this database
        let sub_name = if databases.len() == 1 {
            // Single database - use template name as-is
            sub_name_template.to_string()
        } else {
            // Multiple databases - append database name
            format!("{}_{}", sub_name_template, db.name)
        };

        tracing::info!("Database: '{}'", db.name);
        tracing::info!("Subscription: '{}'", sub_name);
        tracing::info!("");

        // Query replication lag from source
        let source_stats = get_replication_lag(&source_client, Some(&sub_name))
            .await
            .context(format!(
                "Failed to query replication lag for database '{}'",
                db.name
            ))?;

        // Query subscription status from target
        let target_stats = get_subscription_status(&target_client, Some(&sub_name))
            .await
            .context(format!(
                "Failed to query subscription status for database '{}'",
                db.name
            ))?;

        // Check if caught up
        let caught_up = is_replication_caught_up(&source_client, Some(&sub_name))
            .await
            .unwrap_or(false);

        if source_stats.is_empty() {
            tracing::warn!("⚠ No active replication found for this database");
            tracing::warn!("  Subscription '{}' may not be set up yet", sub_name);
            tracing::info!("");
            all_caught_up = false;
        } else {
            any_active = true;
            for stat in &source_stats {
                tracing::info!("Source Replication Slot:");
                tracing::info!("  Application: {}", stat.application_name);
                tracing::info!("  State: {}", stat.state);
                tracing::info!("  Sent LSN: {}", stat.sent_lsn);
                tracing::info!("  Write LSN: {}", stat.write_lsn);
                tracing::info!("  Flush LSN: {}", stat.flush_lsn);
                tracing::info!("  Replay LSN: {}", stat.replay_lsn);

                if let Some(lag) = stat.replay_lag_ms {
                    tracing::info!("  Replay Lag: {}", format_duration(lag));
                } else {
                    tracing::info!("  Replay Lag: N/A");
                }

                if let Some(lag) = stat.flush_lag_ms {
                    tracing::info!("  Flush Lag: {}", format_duration(lag));
                }

                if let Some(lag) = stat.write_lag_ms {
                    tracing::info!("  Write Lag: {}", format_duration(lag));
                }

                tracing::info!("");
            }
        }

        if target_stats.is_empty() {
            tracing::warn!("⚠ No subscription found on target");
            tracing::warn!("  Subscription '{}' may not exist", sub_name);
            tracing::info!("");
            all_caught_up = false;
        } else {
            for stat in &target_stats {
                tracing::info!("Target Subscription:");
                tracing::info!("  Name: {}", stat.subscription_name);

                let state_str = match stat.state.as_str() {
                    "i" => "Initializing",
                    "d" => "Copying data",
                    "s" => "Syncing",
                    "r" => "Ready (streaming)",
                    _ => &stat.state,
                };
                tracing::info!("  State: {}", state_str);

                if let Some(pid) = stat.pid {
                    tracing::info!("  Worker PID: {}", pid);
                } else {
                    tracing::info!("  Worker PID: Not running");
                }

                if let Some(lsn) = &stat.received_lsn {
                    tracing::info!("  Received LSN: {}", lsn);
                }

                if let Some(lsn) = &stat.latest_end_lsn {
                    tracing::info!("  Latest End LSN: {}", lsn);
                }

                tracing::info!("");
            }
        }

        // Per-database summary
        if caught_up {
            tracing::info!("✓ Database '{}' is CAUGHT UP", db.name);
        } else {
            tracing::warn!("⚠ Database '{}' is LAGGING or NOT ACTIVE", db.name);
            all_caught_up = false;
        }

        tracing::info!("");
        tracing::info!("----------------------------------------");
        tracing::info!("");
    }

    // Overall health summary
    tracing::info!("========================================");
    tracing::info!("Overall Status Summary");
    tracing::info!("========================================");
    if all_caught_up && any_active {
        tracing::info!("✓ All databases are CAUGHT UP (lag < 1s)");
        tracing::info!("  Your target databases are fully in sync!");
    } else if !any_active {
        tracing::warn!("✗ Replication is NOT ACTIVE");
        tracing::warn!("  Run 'sync' command to set up replication");
    } else {
        tracing::warn!("⚠ Some databases are LAGGING or NOT ACTIVE");
        tracing::warn!("  Wait for replication to catch up before cutover");
    }
    tracing::info!("========================================");

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_duration() {
        assert_eq!(format_duration(0), "0ms");
        assert_eq!(format_duration(500), "500ms");
        assert_eq!(format_duration(999), "999ms");
        assert_eq!(format_duration(1000), "1.0s");
        assert_eq!(format_duration(1500), "1.5s");
        assert_eq!(format_duration(59999), "60.0s");
        assert_eq!(format_duration(60000), "1m 0s");
        assert_eq!(format_duration(65000), "1m 5s");
        assert_eq!(format_duration(135000), "2m 15s");
        assert_eq!(format_duration(3600000), "1h 0m");
        assert_eq!(format_duration(3660000), "1h 1m");
    }

    #[tokio::test]
    #[ignore]
    async fn test_status_command() {
        // This test requires both source and target databases with active replication
        let source_url = std::env::var("TEST_SOURCE_URL").unwrap();
        let target_url = std::env::var("TEST_TARGET_URL").unwrap();

        let result = status(&source_url, &target_url, None).await;

        match &result {
            Ok(_) => println!("✓ Status command completed successfully"),
            Err(e) => {
                println!("Error in status command: {:?}", e);
                // It's okay if replication is not set up yet
                if !e.to_string().contains("not supported") && !e.to_string().contains("permission")
                {
                    panic!("Unexpected error: {:?}", e);
                }
            }
        }

        assert!(result.is_ok(), "Status command failed: {:?}", result);
    }

    #[tokio::test]
    #[ignore]
    async fn test_status_with_defaults() {
        let source_url = std::env::var("TEST_SOURCE_URL").unwrap();
        let target_url = std::env::var("TEST_TARGET_URL").unwrap();

        let result = status(&source_url, &target_url, None).await;

        match &result {
            Ok(_) => println!("✓ Status with defaults completed successfully"),
            Err(e) => {
                println!("Error in status with defaults: {:?}", e);
            }
        }

        assert!(result.is_ok(), "Status with defaults failed: {:?}", result);
    }

    #[tokio::test]
    #[ignore]
    async fn test_status_with_database_filter() {
        let source_url = std::env::var("TEST_SOURCE_URL").unwrap();
        let target_url = std::env::var("TEST_TARGET_URL").unwrap();

        // Create filter that includes only postgres database
        let filter = crate::filters::ReplicationFilter::new(
            Some(vec!["postgres".to_string()]),
            None,
            None,
            None,
        )
        .expect("Failed to create filter");

        let result = status(&source_url, &target_url, Some(filter)).await;

        match &result {
            Ok(_) => println!("✓ Status with database filter completed successfully"),
            Err(e) => {
                println!("Error in status with database filter: {:?}", e);
            }
        }

        assert!(
            result.is_ok(),
            "Status with database filter failed: {:?}",
            result
        );
    }

    #[tokio::test]
    #[ignore]
    async fn test_status_with_no_matching_databases() {
        let source_url = std::env::var("TEST_SOURCE_URL").unwrap();
        let target_url = std::env::var("TEST_TARGET_URL").unwrap();

        // Create filter that matches no databases
        let filter = crate::filters::ReplicationFilter::new(
            Some(vec!["nonexistent_database".to_string()]),
            None,
            None,
            None,
        )
        .expect("Failed to create filter");

        let result = status(&source_url, &target_url, Some(filter)).await;

        // Should succeed but show no status (early return)
        assert!(result.is_ok(), "Status should succeed even with no matches");
    }
}
