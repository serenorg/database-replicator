// ABOUTME: Initial replication command for snapshot schema and data copy
// ABOUTME: Performs full database dump and restore from source to target

use crate::{migration, postgres};
use anyhow::{bail, Context, Result};
use std::io::{self, Write};
use tokio_postgres::Client;

/// Initial replication command for snapshot schema and data copy
///
/// Performs a full database dump and restore from source to target in steps:
/// 1. Estimates database sizes and replication times
/// 2. Prompts for confirmation (unless skip_confirmation is true)
/// 3. Dumps global objects (roles, tablespaces) from source
/// 4. Restores global objects to target
/// 5. Discovers all user databases on source
/// 6. Replicates each database (schema and data)
/// 7. Optionally sets up continuous logical replication (if enable_sync is true)
///
/// Uses temporary directory for dump files, which is automatically cleaned up.
///
/// # Arguments
///
/// * `source_url` - PostgreSQL connection string for source (Neon) database
/// * `target_url` - PostgreSQL connection string for target (Seren) database
/// * `skip_confirmation` - Skip the size estimation and confirmation prompt
/// * `filter` - Database and table filtering rules
/// * `drop_existing` - Drop existing databases on target before copying
/// * `enable_sync` - Set up continuous logical replication after snapshot (default: true)
///
/// # Returns
///
/// Returns `Ok(())` if replication completes successfully.
///
/// # Errors
///
/// This function will return an error if:
/// - Cannot create temporary directory
/// - Global objects dump/restore fails
/// - Cannot connect to source database
/// - Database discovery fails
/// - Any database replication fails
/// - User declines confirmation prompt
/// - Logical replication setup fails (if enable_sync is true)
///
/// # Examples
///
/// ```no_run
/// # use anyhow::Result;
/// # use postgres_seren_replicator::commands::init;
/// # use postgres_seren_replicator::filters::ReplicationFilter;
/// # async fn example() -> Result<()> {
/// // With confirmation prompt and automatic sync (default)
/// init(
///     "postgresql://user:pass@neon.tech/sourcedb",
///     "postgresql://user:pass@seren.example.com/targetdb",
///     false,
///     ReplicationFilter::empty(),
///     false,
///     true  // Enable continuous replication
/// ).await?;
///
/// // Snapshot only (no continuous replication)
/// init(
///     "postgresql://user:pass@neon.tech/sourcedb",
///     "postgresql://user:pass@seren.example.com/targetdb",
///     true,
///     ReplicationFilter::empty(),
///     false,
///     false  // Disable continuous replication
/// ).await?;
/// # Ok(())
/// # }
/// ```
pub async fn init(
    source_url: &str,
    target_url: &str,
    skip_confirmation: bool,
    filter: crate::filters::ReplicationFilter,
    drop_existing: bool,
    enable_sync: bool,
) -> Result<()> {
    tracing::info!("Starting initial replication...");

    // CRITICAL: Ensure source and target are different to prevent data loss
    crate::utils::validate_source_target_different(source_url, target_url)
        .context("Source and target validation failed")?;
    tracing::info!("✓ Verified source and target are different databases");

    // Create managed temporary directory for dump files
    // Unlike TempDir, this survives SIGKILL and is cleaned up on next startup
    let temp_path =
        crate::utils::create_managed_temp_dir().context("Failed to create temp directory")?;
    tracing::debug!("Using temp directory: {}", temp_path.display());

    // Step 1: Dump global objects
    tracing::info!("Step 1/4: Dumping global objects (roles, tablespaces)...");
    let globals_file = temp_path.join("globals.sql");
    migration::dump_globals(source_url, globals_file.to_str().unwrap()).await?;

    // Step 2: Restore global objects
    tracing::info!("Step 2/4: Restoring global objects to target...");
    migration::restore_globals(target_url, globals_file.to_str().unwrap()).await?;

    // Step 3: Discover and filter databases
    tracing::info!("Step 3/4: Discovering databases...");
    let source_client = postgres::connect(source_url).await?;
    let all_databases = migration::list_databases(&source_client).await?;

    // Apply filtering rules
    let databases: Vec<_> = all_databases
        .into_iter()
        .filter(|db| filter.should_replicate_database(&db.name))
        .collect();

    if databases.is_empty() {
        if filter.is_empty() {
            tracing::warn!("⚠ No user databases found on source");
            tracing::warn!("  This is unusual - the source database appears empty");
            tracing::warn!("  Only global objects (roles, tablespaces) will be replicated");
        } else {
            tracing::warn!("⚠ No databases matched the filter criteria");
            tracing::warn!("  Check your --include-databases or --exclude-databases settings");
        }
        tracing::info!("✅ Initial replication complete (no databases to replicate)");
        return Ok(());
    }

    tracing::info!("Found {} database(s) to replicate", databases.len());

    // Estimate database sizes and get confirmation
    if !skip_confirmation {
        tracing::info!("Analyzing database sizes...");
        let size_estimates =
            migration::estimate_database_sizes(source_url, &source_client, &databases, &filter)
                .await?;

        if !confirm_replication(&size_estimates)? {
            bail!("Replication cancelled by user");
        }
    }

    // Step 4: Replicate each database
    tracing::info!("Step 4/4: Replicating databases...");
    for (idx, db_info) in databases.iter().enumerate() {
        tracing::info!(
            "Replicating database {}/{}: '{}'",
            idx + 1,
            databases.len(),
            db_info.name
        );

        // Build connection URLs for this specific database
        let source_db_url = replace_database_in_url(source_url, &db_info.name)?;
        let target_db_url = replace_database_in_url(target_url, &db_info.name)?;

        // Handle database creation/existence
        let target_client = postgres::connect(target_url).await?;

        // Check if database exists
        if database_exists(&target_client, &db_info.name).await? {
            tracing::info!("  Database '{}' already exists on target", db_info.name);

            // Check if empty
            if database_is_empty(target_url, &db_info.name).await? {
                tracing::info!(
                    "  Database '{}' is empty, proceeding with restore",
                    db_info.name
                );
            } else {
                // Database exists and has data
                let should_drop = if drop_existing {
                    // Auto-drop in automated mode with --drop-existing
                    true
                } else if skip_confirmation {
                    // In automated mode without --drop-existing, fail
                    bail!(
                        "Database '{}' already exists and contains data. \
                         Use --drop-existing to overwrite, or manually drop the database first.",
                        db_info.name
                    );
                } else {
                    // Interactive mode: prompt user
                    prompt_drop_database(&db_info.name)?
                };

                if should_drop {
                    drop_database_if_exists(&target_client, &db_info.name).await?;
                    // Continue to create fresh database below
                } else {
                    bail!("Aborted: Database '{}' already exists", db_info.name);
                }
            }
        }

        // Validate database name to prevent SQL injection
        crate::utils::validate_postgres_identifier(&db_info.name)
            .with_context(|| format!("Invalid database name: '{}'", db_info.name))?;

        // Create database if it doesn't exist (or was just dropped)
        if !database_exists(&target_client, &db_info.name).await? {
            let create_query = format!("CREATE DATABASE \"{}\"", db_info.name);
            target_client
                .execute(&create_query, &[])
                .await
                .with_context(|| format!("Failed to create database '{}'", db_info.name))?;
            tracing::info!("  Created database '{}'", db_info.name);
        }

        // Dump and restore schema
        tracing::info!("  Dumping schema for '{}'...", db_info.name);
        let schema_file = temp_path.join(format!("{}_schema.sql", db_info.name));
        migration::dump_schema(
            &source_db_url,
            &db_info.name,
            schema_file.to_str().unwrap(),
            &filter,
        )
        .await?;

        tracing::info!("  Restoring schema for '{}'...", db_info.name);
        migration::restore_schema(&target_db_url, schema_file.to_str().unwrap()).await?;

        // Dump and restore data (using directory format for parallel operations)
        tracing::info!("  Dumping data for '{}'...", db_info.name);
        let data_dir = temp_path.join(format!("{}_data.dump", db_info.name));
        migration::dump_data(
            &source_db_url,
            &db_info.name,
            data_dir.to_str().unwrap(),
            &filter,
        )
        .await?;

        tracing::info!("  Restoring data for '{}'...", db_info.name);
        migration::restore_data(&target_db_url, data_dir.to_str().unwrap()).await?;

        tracing::info!("✓ Database '{}' replicated successfully", db_info.name);
    }

    // Explicitly clean up temp directory
    // (This runs on normal completion; startup cleanup handles SIGKILL cases)
    if let Err(e) = crate::utils::remove_managed_temp_dir(&temp_path) {
        tracing::warn!("Failed to clean up temp directory: {}", e);
        // Don't fail the entire operation if cleanup fails
    }

    tracing::info!("✅ Initial replication complete");

    // Set up continuous logical replication if enabled
    if enable_sync {
        tracing::info!("");
        tracing::info!("========================================");
        tracing::info!("Step 5/5: Setting up continuous replication...");
        tracing::info!("========================================");
        tracing::info!("");

        // Call sync command with the same filter
        crate::commands::sync(
            source_url,
            target_url,
            Some(filter),
            None,
            None,
            None,
            false,
        )
        .await
        .context("Failed to set up continuous replication")?;

        tracing::info!("");
        tracing::info!("✅ Complete! Snapshot and continuous replication are active");
    } else {
        tracing::info!("");
        tracing::info!("ℹ Continuous replication was not set up (--no-sync flag)");
        tracing::info!("  To enable it later, run:");
        tracing::info!("    postgres-seren-replicator sync --source <url> --target <url>");
    }

    Ok(())
}

/// Replace the database name in a connection URL
fn replace_database_in_url(url: &str, new_database: &str) -> Result<String> {
    // Parse URL to find database name
    // Format: postgresql://user:pass@host:port/database?params

    // Split by '?' to separate params
    let parts: Vec<&str> = url.split('?').collect();
    let base_url = parts[0];
    let params = if parts.len() > 1 {
        Some(parts[1])
    } else {
        None
    };

    // Split base by '/' to get everything before database name
    let url_parts: Vec<&str> = base_url.rsplitn(2, '/').collect();
    if url_parts.len() != 2 {
        anyhow::bail!("Invalid connection URL format");
    }

    // Reconstruct URL with new database name
    let mut new_url = format!("{}/{}", url_parts[1], new_database);
    if let Some(p) = params {
        new_url = format!("{}?{}", new_url, p);
    }

    Ok(new_url)
}

/// Display database size estimates and prompt for confirmation
///
/// Shows a table with database names, sizes, and estimated replication times.
/// Prompts the user to proceed with the replication.
///
/// # Arguments
///
/// * `sizes` - Vector of database size estimates
///
/// # Returns
///
/// Returns `true` if user confirms (enters 'y'), `false` otherwise.
///
/// # Errors
///
/// Returns an error if stdin/stdout operations fail.
fn confirm_replication(sizes: &[migration::DatabaseSizeInfo]) -> Result<bool> {
    use std::time::Duration;

    // Calculate totals
    let total_bytes: i64 = sizes.iter().map(|s| s.size_bytes).sum();
    let total_duration: Duration = sizes.iter().map(|s| s.estimated_duration).sum();

    // Print table header
    println!();
    println!("{:<20} {:<12} {:<15}", "Database", "Size", "Est. Time");
    println!("{}", "─".repeat(50));

    // Print each database
    for size in sizes {
        println!(
            "{:<20} {:<12} {:<15}",
            size.name,
            size.size_human,
            migration::format_duration(size.estimated_duration)
        );
    }

    // Print totals
    println!("{}", "─".repeat(50));
    println!(
        "Total: {} (estimated {})",
        migration::format_bytes(total_bytes),
        migration::format_duration(total_duration)
    );
    println!();

    // Prompt for confirmation
    print!("Proceed with replication? [y/N]: ");
    io::stdout().flush()?;

    let mut input = String::new();
    io::stdin()
        .read_line(&mut input)
        .context("Failed to read user input")?;

    Ok(input.trim().to_lowercase() == "y")
}

/// Checks if a database exists on the target
async fn database_exists(target_conn: &Client, db_name: &str) -> Result<bool> {
    let query = "SELECT 1 FROM pg_database WHERE datname = $1";
    let rows = target_conn.query(query, &[&db_name]).await?;
    Ok(!rows.is_empty())
}

/// Checks if a database is empty (no user tables)
async fn database_is_empty(target_url: &str, db_name: &str) -> Result<bool> {
    // Need to connect to the specific database to check tables
    let db_url = replace_database_in_url(target_url, db_name)?;
    let client = postgres::connect(&db_url).await?;

    let query = "
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
    ";

    let row = client.query_one(query, &[]).await?;
    let count: i64 = row.get(0);

    Ok(count == 0)
}

/// Prompts user to drop existing database
fn prompt_drop_database(db_name: &str) -> Result<bool> {
    use std::io::{self, Write};

    print!(
        "\nWarning: Database '{}' already exists on target and contains data.\n\
         Drop and recreate database? This will delete all existing data. [y/N]: ",
        db_name
    );
    io::stdout().flush()?;

    let mut input = String::new();
    io::stdin().read_line(&mut input)?;

    Ok(input.trim().eq_ignore_ascii_case("y"))
}

/// Drops a database if it exists
async fn drop_database_if_exists(target_conn: &Client, db_name: &str) -> Result<()> {
    // Validate database name to prevent SQL injection
    crate::utils::validate_postgres_identifier(db_name)
        .with_context(|| format!("Invalid database name: '{}'", db_name))?;

    tracing::info!("  Dropping existing database '{}'...", db_name);

    // Terminate existing connections to the database
    let terminate_query = "
        SELECT pg_terminate_backend(pid)
        FROM pg_stat_activity
        WHERE datname = $1 AND pid <> pg_backend_pid()
    ";
    target_conn.execute(terminate_query, &[&db_name]).await?;

    // Drop the database
    let drop_query = format!("DROP DATABASE IF EXISTS \"{}\"", db_name);
    target_conn
        .execute(&drop_query, &[])
        .await
        .with_context(|| format!("Failed to drop database '{}'", db_name))?;

    tracing::info!("  ✓ Database '{}' dropped", db_name);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    #[ignore]
    async fn test_init_replicates_database() {
        let source = std::env::var("TEST_SOURCE_URL").unwrap();
        let target = std::env::var("TEST_TARGET_URL").unwrap();

        // Skip confirmation for automated tests, disable sync to keep test simple
        let filter = crate::filters::ReplicationFilter::empty();
        let result = init(&source, &target, true, filter, false, false).await;
        assert!(result.is_ok());
    }

    #[test]
    fn test_replace_database_in_url() {
        let url = "postgresql://user:pass@host:5432/olddb?sslmode=require";
        let result = replace_database_in_url(url, "newdb").unwrap();
        assert_eq!(
            result,
            "postgresql://user:pass@host:5432/newdb?sslmode=require"
        );

        let url_no_params = "postgresql://user:pass@host:5432/olddb";
        let result = replace_database_in_url(url_no_params, "newdb").unwrap();
        assert_eq!(result, "postgresql://user:pass@host:5432/newdb");
    }

    #[tokio::test]
    #[ignore]
    async fn test_database_exists() {
        let url = std::env::var("TEST_TARGET_URL").expect("TEST_TARGET_URL not set");
        let client = postgres::connect(&url).await.unwrap();

        // postgres database should always exist
        assert!(database_exists(&client, "postgres").await.unwrap());

        // non-existent database should not exist
        assert!(!database_exists(&client, "nonexistent_db_test_12345")
            .await
            .unwrap());
    }

    #[tokio::test]
    #[ignore]
    async fn test_database_is_empty() {
        let url = std::env::var("TEST_TARGET_URL").expect("TEST_TARGET_URL not set");

        // postgres database might be empty of user tables
        // This test just verifies the function doesn't crash
        let result = database_is_empty(&url, "postgres").await;
        assert!(result.is_ok());
    }
}
