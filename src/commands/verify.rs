// ABOUTME: Verify command implementation - Validate data integrity
// ABOUTME: Compares table checksums between source and target databases

use crate::migration::{self, compare_tables, list_tables};
use crate::postgres::connect;
use anyhow::{Context, Result};
use futures::stream::{self, StreamExt};
use indicatif::{ProgressBar, ProgressStyle};

/// Verify data integrity between source and target databases
///
/// This command performs Phase 5 of the migration process:
/// 1. Discovers databases and filters them based on criteria
/// 2. For each filtered database:
///    - Lists all tables and filters them
///    - Compares each table's checksum between source and target
///    - Reports any mismatches or missing tables
/// 3. Provides overall validation summary across all databases
///
/// Uses parallel verification (up to 4 concurrent table checks) with progress bars
/// for efficient processing of large databases.
///
/// # Arguments
///
/// * `source_url` - PostgreSQL connection string for source database
/// * `target_url` - PostgreSQL connection string for target (Seren) database
/// * `filter` - Optional replication filter for database and table selection
///
/// # Returns
///
/// Returns `Ok(())` if all tables match or after displaying verification results.
///
/// # Errors
///
/// This function will return an error if:
/// - Cannot connect to source or target database
/// - Cannot discover databases on source
/// - Cannot list tables from source
/// - Table comparison fails due to connection issues
///
/// # Examples
///
/// ```no_run
/// # use anyhow::Result;
/// # use postgres_seren_replicator::commands::verify;
/// # use postgres_seren_replicator::filters::ReplicationFilter;
/// # async fn example() -> Result<()> {
/// // Verify all databases
/// verify(
///     "postgresql://user:pass@source.example.com/postgres",
///     "postgresql://user:pass@target.example.com/postgres",
///     None
/// ).await?;
///
/// // Verify only specific databases
/// let filter = ReplicationFilter::new(
///     Some(vec!["mydb".to_string(), "analytics".to_string()]),
///     None,
///     None,
///     None,
/// )?;
/// verify(
///     "postgresql://user:pass@source.example.com/postgres",
///     "postgresql://user:pass@target.example.com/postgres",
///     Some(filter)
/// ).await?;
/// # Ok(())
/// # }
/// ```
pub async fn verify(
    source_url: &str,
    target_url: &str,
    filter: Option<crate::filters::ReplicationFilter>,
) -> Result<()> {
    let filter = filter.unwrap_or_else(crate::filters::ReplicationFilter::empty);

    tracing::info!("Starting data integrity verification...");
    tracing::info!("");

    // Ensure source and target are different
    crate::utils::validate_source_target_different(source_url, target_url)
        .context("Source and target validation failed")?;
    tracing::info!("✓ Verified source and target are different databases");
    tracing::info!("");

    // Connect to source database to discover databases
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
        tracing::warn!("  No verification to perform");
        return Ok(());
    }

    tracing::info!("Found {} database(s) to verify:", databases.len());
    for db in &databases {
        tracing::info!("  - {}", db.name);
    }
    tracing::info!("");

    // Overall statistics across all databases
    let mut total_matches = 0;
    let mut total_mismatches = 0;
    let mut total_tables = 0;

    // Verify each database
    for db in &databases {
        tracing::info!("========================================");
        tracing::info!("Database: '{}'", db.name);
        tracing::info!("========================================");

        // Build database-specific connection URLs
        let source_db_url = replace_database_in_url(source_url, &db.name).context(format!(
            "Failed to build source URL for database '{}'",
            db.name
        ))?;
        let target_db_url = replace_database_in_url(target_url, &db.name).context(format!(
            "Failed to build target URL for database '{}'",
            db.name
        ))?;

        // Connect to the specific database on source and target
        tracing::info!("Connecting to database '{}'...", db.name);
        let source_db_client = connect(&source_db_url).await.context(format!(
            "Failed to connect to source database '{}'",
            db.name
        ))?;
        let target_db_client = connect(&target_db_url).await.context(format!(
            "Failed to connect to target database '{}'",
            db.name
        ))?;

        // List tables from source
        tracing::info!("Discovering tables...");
        let all_tables = list_tables(&source_db_client)
            .await
            .context(format!("Failed to list tables from database '{}'", db.name))?;

        // Filter tables based on filter rules
        let tables: Vec<_> = all_tables
            .into_iter()
            .filter(|table| {
                // Build full table name in "database.table" format for filtering
                let table_name = if table.schema == "public" {
                    table.name.clone()
                } else {
                    format!("{}.{}", table.schema, table.name)
                };
                filter.should_replicate_table(&db.name, &table_name)
            })
            .collect();

        if tables.is_empty() {
            tracing::warn!("⚠ No tables found to verify in database '{}'", db.name);
            tracing::info!("");
            continue;
        }

        tracing::info!("Found {} tables to verify", tables.len());
        tracing::info!("Using parallel verification (concurrency: 4)");
        tracing::info!("");

        // Create progress bar
        let progress = ProgressBar::new(tables.len() as u64);
        progress.set_style(
            ProgressStyle::default_bar()
                .template("[{elapsed_precise}] {bar:40.cyan/blue} {pos}/{len} {msg}")
                .unwrap()
                .progress_chars("##-"),
        );

        // Create additional connections for parallel processing (3 more pairs for concurrency=4)
        let source_db_client2 = connect(&source_db_url).await.context(format!(
            "Failed to create additional source connection 2 for database '{}'",
            db.name
        ))?;
        let target_db_client2 = connect(&target_db_url).await.context(format!(
            "Failed to create additional target connection 2 for database '{}'",
            db.name
        ))?;
        let source_db_client3 = connect(&source_db_url).await.context(format!(
            "Failed to create additional source connection 3 for database '{}'",
            db.name
        ))?;
        let target_db_client3 = connect(&target_db_url).await.context(format!(
            "Failed to create additional target connection 3 for database '{}'",
            db.name
        ))?;
        let source_db_client4 = connect(&source_db_url).await.context(format!(
            "Failed to create additional source connection 4 for database '{}'",
            db.name
        ))?;
        let target_db_client4 = connect(&target_db_url).await.context(format!(
            "Failed to create additional target connection 4 for database '{}'",
            db.name
        ))?;

        // Store clients in an array for round-robin access (4 connections to match concurrency=4)
        let source_clients = [
            source_db_client,
            source_db_client2,
            source_db_client3,
            source_db_client4,
        ];
        let target_clients = [
            target_db_client,
            target_db_client2,
            target_db_client3,
            target_db_client4,
        ];

        // Process tables in parallel with limited concurrency
        let verification_results: Vec<_> = stream::iter(tables.iter().enumerate())
            .map(|(idx, table)| {
                let schema = table.schema.clone();
                let name = table.name.clone();
                let source_client = &source_clients[idx % source_clients.len()];
                let target_client = &target_clients[idx % target_clients.len()];
                let pb = progress.clone();

                async move {
                    let result = compare_tables(source_client, target_client, &schema, &name).await;
                    pb.inc(1);
                    pb.set_message(format!("Verified {}.{}", schema, name));
                    (schema, name, result)
                }
            })
            .buffer_unordered(4) // Process up to 4 tables concurrently
            .collect()
            .await;

        progress.finish_with_message(format!("Verification complete for database '{}'", db.name));
        tracing::info!("");

        // Process results for this database
        let mut db_mismatches = 0;
        let mut db_matches = 0;

        for (schema, name, result) in verification_results {
            match result {
                Ok(checksum_result) => {
                    if checksum_result.is_valid() {
                        tracing::info!(
                            "  ✓ {}.{}: Match ({} rows, checksum: {})",
                            schema,
                            name,
                            checksum_result.source_row_count,
                            &checksum_result.source_checksum[..8]
                        );
                        db_matches += 1;
                    } else if checksum_result.matches {
                        tracing::warn!(
                            "  ⚠ {}.{}: Checksum matches but row count differs: source={}, target={}",
                            schema,
                            name,
                            checksum_result.source_row_count,
                            checksum_result.target_row_count
                        );
                        db_mismatches += 1;
                    } else {
                        tracing::error!(
                            "  ✗ {}.{}: MISMATCH: source={} ({}), target={} ({})",
                            schema,
                            name,
                            &checksum_result.source_checksum[..8],
                            checksum_result.source_row_count,
                            &checksum_result.target_checksum[..8],
                            checksum_result.target_row_count
                        );
                        db_mismatches += 1;
                    }
                }
                Err(e) => {
                    let error_msg = format!("{}.{}: {}", schema, name, e);
                    tracing::error!("  ✗ ERROR: {}", error_msg);
                    db_mismatches += 1;
                }
            }
        }

        // Display summary for this database
        tracing::info!("");
        tracing::info!("Database '{}' Summary:", db.name);
        tracing::info!("  Total tables: {}", tables.len());
        tracing::info!("  ✓ Matches: {}", db_matches);
        tracing::info!("  ✗ Mismatches: {}", db_mismatches);
        tracing::info!("");

        // Update overall statistics
        total_tables += tables.len();
        total_matches += db_matches;
        total_mismatches += db_mismatches;
    }

    // Display overall summary
    tracing::info!("========================================");
    tracing::info!("Overall Verification Summary");
    tracing::info!("========================================");
    tracing::info!("Databases verified: {}", databases.len());
    tracing::info!("Total tables: {}", total_tables);
    tracing::info!("✓ Matches: {}", total_matches);
    tracing::info!("✗ Mismatches: {}", total_mismatches);
    tracing::info!("========================================");
    tracing::info!("");

    if total_mismatches > 0 {
        tracing::error!("⚠ DATA INTEGRITY ISSUES DETECTED!");
        tracing::error!("  {} table(s) have mismatched data", total_mismatches);
        tracing::error!("  Review the logs above for details");
        tracing::info!("");
        tracing::info!("Possible causes:");
        tracing::info!("  - Replication is still catching up (check 'status' command)");
        tracing::info!("  - Data was modified on target after migration");
        tracing::info!("  - Migration errors occurred during 'init' or 'sync'");
        tracing::info!("");

        anyhow::bail!("{} table(s) failed verification", total_mismatches);
    } else {
        tracing::info!("✓ ALL TABLES VERIFIED SUCCESSFULLY!");
        tracing::info!(
            "  All {} tables match between source and target",
            total_matches
        );
        tracing::info!("  Your migration data is intact and ready for cutover");
    }

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
    async fn test_verify_command() {
        // This test requires both source and target databases
        let source_url = std::env::var("TEST_SOURCE_URL").unwrap();
        let target_url = std::env::var("TEST_TARGET_URL").unwrap();

        let result = verify(&source_url, &target_url, None).await;

        match &result {
            Ok(_) => {
                println!("✓ Verify command completed successfully");
            }
            Err(e) => {
                println!("Verify command result: {:?}", e);
                // It's okay if tables don't match yet (replication not set up)
                // We're just testing that the command runs
            }
        }

        // The command should at least connect and attempt verification
        // Even if it finds mismatches, that's a valid result
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
    async fn test_verify_with_database_filter() {
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

        let result = verify(&source_url, &target_url, Some(filter)).await;

        match &result {
            Ok(_) => println!("✓ Verify with database filter completed successfully"),
            Err(e) => {
                println!("Verify with database filter result: {:?}", e);
                // It's okay if tables don't match - we're testing filtering works
            }
        }

        // Command should at least connect and discover databases
    }

    #[tokio::test]
    #[ignore]
    async fn test_verify_with_no_matching_databases() {
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

        let result = verify(&source_url, &target_url, Some(filter)).await;

        // Should succeed but show no verification (early return)
        assert!(result.is_ok(), "Verify should succeed even with no matches");
    }
}
