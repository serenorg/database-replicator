// ABOUTME: Sync command implementation - Phase 3 of migration
// ABOUTME: Sets up logical replication between source and target databases

use crate::migration;
use crate::postgres::connect;
use crate::replication::{
    create_publication, create_subscription, detect_subscription_state, drop_subscription,
    wait_for_sync, SubscriptionState,
};
use anyhow::{Context, Result};

/// Set up logical replication between source and target databases
///
/// This command performs Phase 3 of the migration process:
/// 1. Discovers all databases on the source
/// 2. Filters databases based on the provided filter criteria
/// 3. For each database:
///    - Checks if subscription already exists (skips or recreates based on state and force flag)
///    - Creates a publication on the source database (filtered tables if specified)
///    - Creates a subscription on the target database pointing to the source
///    - Waits for the initial sync to complete
///
/// After this command succeeds, changes on the source databases will continuously
/// replicate to the target until the subscriptions are dropped.
///
/// This command is idempotent - it can be safely re-run if interrupted or if setup failed partially.
///
/// # Arguments
///
/// * `source_url` - PostgreSQL connection string for source database
/// * `target_url` - PostgreSQL connection string for target (Seren) database
/// * `filter` - Optional replication filter for database and table selection
/// * `publication_name` - Optional publication name template (defaults to "seren_migration_pub")
/// * `subscription_name` - Optional subscription name template (defaults to "seren_migration_sub")
/// * `sync_timeout_secs` - Optional timeout in seconds per database (defaults to 300)
/// * `force` - Force recreate subscriptions even if they already exist (defaults to false)
///
/// # Returns
///
/// Returns `Ok(())` if replication setup completes successfully for all databases.
///
/// # Errors
///
/// This function will return an error if:
/// - Cannot connect to source or target database
/// - Cannot discover databases on source
/// - Publication creation fails for any database
/// - Subscription creation fails for any database
/// - Initial sync doesn't complete within timeout for any database
///
/// # Examples
///
/// ```no_run
/// # use anyhow::Result;
/// # use postgres_seren_replicator::commands::sync;
/// # use postgres_seren_replicator::filters::ReplicationFilter;
/// # async fn example() -> Result<()> {
/// // Replicate all databases
/// sync(
///     "postgresql://user:pass@source.example.com/postgres",
///     "postgresql://user:pass@target.example.com/postgres",
///     None,  // No filter - replicate all databases
///     None,  // Use default publication name
///     None,  // Use default subscription name
///     Some(600),  // 10 minute timeout per database
///     false,  // Don't force recreate
/// ).await?;
///
/// // Replicate only specific databases
/// let filter = ReplicationFilter::new(
///     Some(vec!["mydb".to_string(), "analytics".to_string()]),
///     None,
///     None,
///     None,
/// )?;
/// sync(
///     "postgresql://user:pass@source.example.com/postgres",
///     "postgresql://user:pass@target.example.com/postgres",
///     Some(filter),
///     None,
///     None,
///     Some(600),
///     false,  // Don't force recreate
/// ).await?;
/// # Ok(())
/// # }
/// ```
pub async fn sync(
    source_url: &str,
    target_url: &str,
    filter: Option<crate::filters::ReplicationFilter>,
    publication_name: Option<&str>,
    subscription_name: Option<&str>,
    sync_timeout_secs: Option<u64>,
    force: bool,
) -> Result<()> {
    let pub_name_template = publication_name.unwrap_or("seren_migration_pub");
    let sub_name_template = subscription_name.unwrap_or("seren_migration_sub");
    let timeout = sync_timeout_secs.unwrap_or(300); // 5 minutes default
    let filter = filter.unwrap_or_else(crate::filters::ReplicationFilter::empty);

    tracing::info!("Starting logical replication setup...");

    // CRITICAL: Ensure source and target are different to prevent data loss
    crate::utils::validate_source_target_different(source_url, target_url)
        .context("Source and target validation failed")?;
    tracing::info!("✓ Verified source and target are different databases");

    // Connect to source database to discover databases
    tracing::info!("Connecting to source database...");
    let source_client = connect(source_url)
        .await
        .context("Failed to connect to source database")?;
    tracing::info!("✓ Connected to source");

    // Discover databases on source
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
        if filter.is_empty() {
            tracing::warn!("⚠ No user databases found on source");
            tracing::warn!("  This is unusual - the source database appears empty");
            tracing::warn!("  Only template databases exist");
        } else {
            tracing::warn!("⚠ No databases matched the filter criteria");
            tracing::warn!("  Check your --include-databases or --exclude-databases settings");
        }
        tracing::info!("✅ Logical replication setup complete (no databases to replicate)");
        return Ok(());
    }

    tracing::info!(
        "Found {} database(s) to replicate: {}",
        databases.len(),
        databases
            .iter()
            .map(|db| db.name.as_str())
            .collect::<Vec<_>>()
            .join(", ")
    );

    // Set up replication for each database
    for db in &databases {
        tracing::info!("");
        tracing::info!(
            "========================================\nDatabase: '{}'\n========================================",
            db.name
        );

        // Build database-specific connection URLs
        let source_db_url = replace_database_in_url(source_url, &db.name).context(format!(
            "Failed to build source URL for database '{}'",
            db.name
        ))?;
        let target_db_url = replace_database_in_url(target_url, &db.name).context(format!(
            "Failed to build target URL for database '{}'",
            db.name
        ))?;

        // Build database-specific publication and subscription names
        let pub_name = if databases.len() == 1 {
            // Single database - use template name as-is
            pub_name_template.to_string()
        } else {
            // Multiple databases - append database name to avoid conflicts
            format!("{}_{}", pub_name_template, db.name)
        };

        let sub_name = if databases.len() == 1 {
            // Single database - use template name as-is
            sub_name_template.to_string()
        } else {
            // Multiple databases - append database name to avoid conflicts
            format!("{}_{}", sub_name_template, db.name)
        };

        tracing::info!("Publication: '{}'", pub_name);
        tracing::info!("Subscription: '{}'", sub_name);

        // Connect to the specific database on source and target
        tracing::info!("Connecting to source database '{}'...", db.name);
        let source_db_client = connect(&source_db_url).await.context(format!(
            "Failed to connect to source database '{}'",
            db.name
        ))?;
        tracing::info!("✓ Connected to source");

        tracing::info!("Connecting to target database '{}'...", db.name);
        let target_db_client = connect(&target_db_url).await.context(format!(
            "Failed to connect to target database '{}'",
            db.name
        ))?;
        tracing::info!("✓ Connected to target");

        // Create publication on source database
        tracing::info!("Creating publication on source database...");
        create_publication(&source_db_client, &db.name, &pub_name, &filter)
            .await
            .context(format!(
                "Failed to create publication on source database '{}'",
                db.name
            ))?;

        // Check if subscription already exists
        tracing::info!("Checking subscription state...");
        let sub_state = detect_subscription_state(&target_db_client, &sub_name)
            .await
            .context(format!(
                "Failed to detect subscription state for '{}'",
                sub_name
            ))?;

        match sub_state {
            SubscriptionState::Streaming => {
                if force {
                    tracing::info!(
                        "⚠ Subscription '{}' is already streaming, but --force flag is set",
                        sub_name
                    );
                    tracing::info!("Dropping existing subscription...");
                    drop_subscription(&target_db_client, &sub_name)
                        .await
                        .context(format!("Failed to drop subscription '{}'", sub_name))?;
                    tracing::info!("Creating new subscription...");
                    create_subscription(&target_db_client, &sub_name, &source_db_url, &pub_name)
                        .await
                        .context(format!(
                            "Failed to create subscription on target database '{}'",
                            db.name
                        ))?;
                    tracing::info!(
                        "Waiting for initial sync to complete (timeout: {}s)...",
                        timeout
                    );
                    wait_for_sync(&target_db_client, &sub_name, timeout)
                        .await
                        .context(format!(
                            "Failed to wait for initial sync on database '{}'",
                            db.name
                        ))?;
                } else {
                    tracing::info!(
                        "✓ Subscription '{}' is already streaming and healthy",
                        sub_name
                    );
                    tracing::info!("  Skipping subscription creation (use --force to recreate)");
                }
            }
            SubscriptionState::Initializing
            | SubscriptionState::Copying
            | SubscriptionState::Syncing => {
                tracing::info!(
                    "ℹ Subscription '{}' already exists and is syncing (state: {:?})",
                    sub_name,
                    sub_state
                );
                tracing::info!(
                    "Waiting for existing sync to complete (timeout: {}s)...",
                    timeout
                );
                wait_for_sync(&target_db_client, &sub_name, timeout)
                    .await
                    .context(format!(
                        "Failed to wait for existing sync on database '{}'",
                        db.name
                    ))?;
            }
            SubscriptionState::Error(err_state) => {
                tracing::warn!(
                    "⚠ Subscription '{}' is in error state: {}",
                    sub_name,
                    err_state
                );
                if force {
                    tracing::info!("Dropping failed subscription and recreating...");
                    drop_subscription(&target_db_client, &sub_name)
                        .await
                        .context(format!("Failed to drop subscription '{}'", sub_name))?;
                    tracing::info!("Creating new subscription...");
                    create_subscription(&target_db_client, &sub_name, &source_db_url, &pub_name)
                        .await
                        .context(format!(
                            "Failed to create subscription on target database '{}'",
                            db.name
                        ))?;
                    tracing::info!(
                        "Waiting for initial sync to complete (timeout: {}s)...",
                        timeout
                    );
                    wait_for_sync(&target_db_client, &sub_name, timeout)
                        .await
                        .context(format!(
                            "Failed to wait for initial sync on database '{}'",
                            db.name
                        ))?;
                } else {
                    anyhow::bail!(
                        "Subscription '{}' is in error state: {}.\n\
                         Use --force flag to drop and recreate the subscription.",
                        sub_name,
                        err_state
                    );
                }
            }
            SubscriptionState::NotFound => {
                tracing::info!("Creating subscription on target database...");
                create_subscription(&target_db_client, &sub_name, &source_db_url, &pub_name)
                    .await
                    .context(format!(
                        "Failed to create subscription on target database '{}'",
                        db.name
                    ))?;
                tracing::info!(
                    "Waiting for initial sync to complete (timeout: {}s)...",
                    timeout
                );
                wait_for_sync(&target_db_client, &sub_name, timeout)
                    .await
                    .context(format!(
                        "Failed to wait for initial sync on database '{}'",
                        db.name
                    ))?;
            }
        }

        tracing::info!("✓ Replication active for database '{}'", db.name);
    }

    tracing::info!("");
    tracing::info!("========================================");
    tracing::info!("✓ Logical replication is now active!");
    tracing::info!("========================================");
    tracing::info!("");
    tracing::info!(
        "Changes on {} source database(s) will now continuously",
        databases.len()
    );
    tracing::info!("replicate to the target.");
    tracing::info!("");
    tracing::info!("Next steps:");
    tracing::info!("  1. Run 'status' to monitor replication lag");
    tracing::info!("  2. Run 'verify' to validate data integrity");
    tracing::info!("  3. When ready, cutover to the target database");

    Ok(())
}

/// Replace the database name in a PostgreSQL connection URL
///
/// # Arguments
///
/// * `url` - PostgreSQL connection URL
/// * `new_db_name` - New database name to use
///
/// # Returns
///
/// URL with the database name replaced
fn replace_database_in_url(url: &str, new_db_name: &str) -> Result<String> {
    // Split into base URL and query parameters
    let parts: Vec<&str> = url.splitn(2, '?').collect();
    let base_url = parts[0];
    let query_params = parts.get(1);

    // Split base URL by '/' to replace the database name
    let url_parts: Vec<&str> = base_url.rsplitn(2, '/').collect();

    if url_parts.len() != 2 {
        anyhow::bail!("Invalid connection URL format: cannot replace database name");
    }

    // Rebuild URL with new database name
    let new_url = if let Some(params) = query_params {
        format!("{}/{}?{}", url_parts[1], new_db_name, params)
    } else {
        format!("{}/{}", url_parts[1], new_db_name)
    };

    Ok(new_url)
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

        let pub_name = "test_sync_pub";
        let sub_name = "test_sync_sub";
        let timeout = 60; // 1 minute timeout for test

        let result = sync(
            &source_url,
            &target_url,
            None,
            Some(pub_name),
            Some(sub_name),
            Some(timeout),
            false,
        )
        .await;

        match &result {
            Ok(_) => println!("✓ Sync command completed successfully"),
            Err(e) => {
                println!("Error in sync command: {:?}", e);
                // If either database doesn't support logical replication, skip
                if e.to_string().contains("not supported") || e.to_string().contains("permission") {
                    println!("Skipping test - database might not support logical replication");
                    return;
                }
            }
        }

        assert!(result.is_ok(), "Sync command failed: {:?}", result);

        // Clean up
        let target_client = connect(&target_url).await.unwrap();
        crate::replication::drop_subscription(&target_client, sub_name)
            .await
            .unwrap();

        let source_client = connect(&source_url).await.unwrap();
        crate::replication::drop_publication(&source_client, pub_name)
            .await
            .unwrap();
    }

    #[tokio::test]
    #[ignore]
    async fn test_sync_with_defaults() {
        let source_url = std::env::var("TEST_SOURCE_URL").unwrap();
        let target_url = std::env::var("TEST_TARGET_URL").unwrap();

        let result = sync(&source_url, &target_url, None, None, None, Some(60), false).await;

        match &result {
            Ok(_) => println!("✓ Sync with defaults completed successfully"),
            Err(e) => {
                println!("Error in sync with defaults: {:?}", e);
                if e.to_string().contains("not supported") || e.to_string().contains("permission") {
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

    #[test]
    fn test_replace_database_in_url() {
        // Basic URL
        let url = "postgresql://user:pass@localhost:5432/olddb";
        let new_url = replace_database_in_url(url, "newdb").unwrap();
        assert_eq!(new_url, "postgresql://user:pass@localhost:5432/newdb");

        // URL with query parameters
        let url = "postgresql://user:pass@localhost:5432/olddb?sslmode=require";
        let new_url = replace_database_in_url(url, "newdb").unwrap();
        assert_eq!(
            new_url,
            "postgresql://user:pass@localhost:5432/newdb?sslmode=require"
        );

        // URL without port
        let url = "postgresql://user:pass@localhost/olddb";
        let new_url = replace_database_in_url(url, "newdb").unwrap();
        assert_eq!(new_url, "postgresql://user:pass@localhost/newdb");
    }

    #[tokio::test]
    #[ignore]
    async fn test_sync_with_database_filter() {
        let source_url = std::env::var("TEST_SOURCE_URL").unwrap();
        let target_url = std::env::var("TEST_TARGET_URL").unwrap();

        println!("Testing sync command with database filter...");
        println!("⚠ WARNING: This will set up replication for filtered databases!");

        // Create filter that includes only specific database
        let filter = crate::filters::ReplicationFilter::new(
            Some(vec!["postgres".to_string()]), // Include only postgres database
            None,
            None,
            None,
        )
        .expect("Failed to create filter");

        let result = sync(
            &source_url,
            &target_url,
            Some(filter),
            None,
            None,
            Some(60),
            false,
        )
        .await;

        match &result {
            Ok(_) => {
                println!("✓ Sync with database filter completed successfully");
            }
            Err(e) => {
                println!("Sync with database filter failed: {:?}", e);
                if e.to_string().contains("not supported") || e.to_string().contains("permission") {
                    println!("Skipping test - database might not support logical replication");
                    return;
                }
            }
        }

        assert!(result.is_ok(), "Sync with database filter failed");

        // Clean up - for single database, names don't have suffix
        let db_url = replace_database_in_url(&target_url, "postgres").unwrap();
        let target_client = connect(&db_url).await.unwrap();
        crate::replication::drop_subscription(&target_client, "seren_migration_sub")
            .await
            .unwrap();

        let source_url_db = replace_database_in_url(&source_url, "postgres").unwrap();
        let source_client = connect(&source_url_db).await.unwrap();
        crate::replication::drop_publication(&source_client, "seren_migration_pub")
            .await
            .unwrap();
    }
}
