// ABOUTME: Initial replication command for snapshot schema and data copy
// ABOUTME: Performs full database dump and restore from source to target

use crate::migration::dump::remove_restricted_role_grants;
use crate::{checkpoint, migration, postgres};
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
/// * `allow_resume` - Resume from checkpoint if available (default: true)
/// * `force_local` - If true, --local was explicitly set (fail instead of fallback to remote)
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
/// # use database_replicator::commands::init;
/// # use database_replicator::filters::ReplicationFilter;
/// # async fn example() -> Result<()> {
/// // With confirmation prompt and automatic sync (default)
/// init(
///     "postgresql://user:pass@neon.tech/sourcedb",
///     "postgresql://user:pass@seren.example.com/targetdb",
///     false,
///     ReplicationFilter::empty(),
///     false,
///     true,   // Enable continuous replication
///     true,   // Allow resume
///     false,  // Not forcing local execution
/// ).await?;
///
/// // Snapshot only (no continuous replication)
/// init(
///     "postgresql://user:pass@neon.tech/sourcedb",
///     "postgresql://user:pass@seren.example.com/targetdb",
///     true,
///     ReplicationFilter::empty(),
///     false,
///     false,  // Disable continuous replication
///     true,   // Allow resume
///     true,   // Force local execution (--local flag)
/// ).await?;
/// # Ok(())
/// # }
/// ```
#[allow(clippy::too_many_arguments)]
pub async fn init(
    source_url: &str,
    target_url: &str,
    skip_confirmation: bool,
    filter: crate::filters::ReplicationFilter,
    drop_existing: bool,
    enable_sync: bool,
    allow_resume: bool,
    force_local: bool,
) -> Result<()> {
    tracing::info!("Starting initial replication...");

    // Detect source database type and route to appropriate implementation
    let source_type =
        crate::detect_source_type(source_url).context("Failed to detect source database type")?;

    match source_type {
        crate::SourceType::PostgreSQL => {
            // PostgreSQL to PostgreSQL replication (existing logic below)
            tracing::info!("Source type: PostgreSQL");

            // Run pre-flight checks before any destructive operations
            tracing::info!("Running pre-flight checks...");

            // Get explicit database list from filters (include_databases or extracted from include_tables)
            let databases = filter.databases_to_check();
            let preflight_result = crate::preflight::run_preflight_checks(
                source_url,
                target_url,
                databases.as_deref(),
            )
            .await?;

            preflight_result.print();

            if !preflight_result.all_passed() {
                // Check if we can auto-fallback to remote
                if preflight_result.tool_version_incompatible
                    && crate::utils::is_serendb_target(target_url)
                    && !force_local
                {
                    println!();
                    tracing::info!(
                        "Tool version incompatible. Switching to SerenAI cloud execution..."
                    );
                    // Return special error that main.rs catches to trigger remote
                    bail!("PREFLIGHT_FALLBACK_TO_REMOTE");
                }

                // Cannot auto-fallback
                if force_local {
                    bail!(
                        "Pre-flight checks failed. Cannot continue with --local flag.\n\
                         Fix the issues above or remove --local to allow remote execution."
                    );
                }

                bail!("Pre-flight checks failed. Fix the issues above and retry.");
            }

            println!();
        }
        crate::SourceType::SQLite => {
            // SQLite to PostgreSQL migration (simpler path)
            tracing::info!("Source type: SQLite");

            // SQLite migrations don't support PostgreSQL-specific features
            if !filter.is_empty() {
                tracing::warn!(
                    "⚠ Filters are not supported for SQLite sources (all tables will be migrated)"
                );
            }
            if drop_existing {
                tracing::warn!("⚠ --drop-existing flag is not applicable for SQLite sources");
            }
            if !enable_sync {
                tracing::warn!(
                    "⚠ SQLite sources don't support continuous replication (one-time migration only)"
                );
            }

            return init_sqlite_to_postgres(source_url, target_url).await;
        }
        crate::SourceType::MongoDB => {
            // MongoDB to PostgreSQL migration (simpler path)
            tracing::info!("Source type: MongoDB");

            // MongoDB migrations don't support PostgreSQL-specific features
            if !filter.is_empty() {
                tracing::warn!(
                    "⚠ Filters are not supported for MongoDB sources (all collections will be migrated)"
                );
            }
            if drop_existing {
                tracing::warn!("⚠ --drop-existing flag is not applicable for MongoDB sources");
            }
            if !enable_sync {
                tracing::warn!(
                    "⚠ MongoDB sources don't support continuous replication (one-time migration only)"
                );
            }

            return init_mongodb_to_postgres(source_url, target_url).await;
        }
        crate::SourceType::MySQL => {
            // MySQL to PostgreSQL replication (simpler path)
            tracing::info!("Source type: MySQL");

            // MySQL replications don't support PostgreSQL-specific features
            if !filter.is_empty() {
                tracing::warn!(
                    "⚠ Filters are not supported for MySQL sources (all tables will be replicated)"
                );
            }
            if drop_existing {
                tracing::warn!("⚠ --drop-existing flag is not applicable for MySQL sources");
            }
            if !enable_sync {
                tracing::warn!(
                    "⚠ MySQL sources don't support continuous replication (one-time replication only)"
                );
            }

            return init_mysql_to_postgres(source_url, target_url).await;
        }
    }

    // CRITICAL: Ensure source and target are different to prevent data loss
    crate::utils::validate_source_target_different(source_url, target_url)
        .context("Source and target validation failed")?;
    tracing::info!("✓ Verified source and target are different databases");

    // Create managed temporary directory for dump files
    // Unlike TempDir, this survives SIGKILL and is cleaned up on next startup
    let temp_path =
        crate::utils::create_managed_temp_dir().context("Failed to create temp directory")?;
    tracing::debug!("Using temp directory: {}", temp_path.display());

    let checkpoint_path = checkpoint::checkpoint_path(source_url, target_url)
        .context("Failed to determine checkpoint location")?;

    // Step 1: Dump global objects
    tracing::info!("Step 1/4: Dumping global objects (roles, tablespaces)...");
    let globals_file = temp_path.join("globals.sql");
    migration::dump_globals(source_url, globals_file.to_str().unwrap()).await?;
    migration::sanitize_globals_dump(globals_file.to_str().unwrap())
        .context("Failed to update globals dump so duplicate roles are ignored during restore")?;
    migration::remove_superuser_from_globals(globals_file.to_str().unwrap())
        .context("Failed to remove SUPERUSER from globals dump")?;
    migration::remove_restricted_guc_settings(globals_file.to_str().unwrap())
        .context("Failed to remove restricted parameter settings from globals dump")?;
    remove_restricted_role_grants(globals_file.to_str().unwrap())
        .context("Failed to remove restricted role grants from globals dump")?;
    migration::remove_tablespace_statements(globals_file.to_str().unwrap())
        .context("Failed to remove CREATE TABLESPACE statements from globals dump")?;

    // Step 2: Restore global objects
    tracing::info!("Step 2/4: Restoring global objects to target...");
    migration::restore_globals(target_url, globals_file.to_str().unwrap()).await?;

    // Step 3: Discover and filter databases
    tracing::info!("Step 3/4: Discovering databases...");
    let all_databases = {
        // Scope the connection so it's dropped before subprocess operations
        let source_client = postgres::connect_with_retry(source_url).await?;
        migration::list_databases(&source_client).await?
    }; // Connection dropped here

    // Apply filtering rules
    let databases: Vec<_> = all_databases
        .into_iter()
        .filter(|db| filter.should_replicate_database(&db.name))
        .collect();

    if databases.is_empty() {
        let _ = checkpoint::remove_checkpoint(&checkpoint_path);
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

    let database_names: Vec<String> = databases.iter().map(|db| db.name.clone()).collect();
    let filter_hash = filter.fingerprint();
    let checkpoint_metadata = checkpoint::InitCheckpointMetadata::new(
        source_url,
        target_url,
        filter_hash,
        drop_existing,
        enable_sync,
    );

    let mut checkpoint_state = if allow_resume {
        match checkpoint::InitCheckpoint::load(&checkpoint_path)? {
            Some(existing) => {
                // Try to validate the checkpoint
                match existing.validate(&checkpoint_metadata, &database_names) {
                    Ok(()) => {
                        // Validation succeeded - resume from checkpoint
                        if existing.completed_count() > 0 {
                            tracing::info!(
                                "Resume checkpoint found: {}/{} databases already replicated",
                                existing.completed_count(),
                                existing.total_databases()
                            );
                        } else {
                            tracing::info!(
                                "Resume checkpoint found but no databases marked complete yet"
                            );
                        }
                        existing
                    }
                    Err(e) => {
                        // Validation failed - log warning and start fresh
                        tracing::warn!("⚠ Checkpoint metadata mismatch detected:");
                        tracing::warn!(
                            "  Previous run configuration differs from current configuration"
                        );
                        tracing::warn!("  - Schema-only tables may have changed");
                        tracing::warn!("  - Time filters may have changed");
                        tracing::warn!("  - Table selection may have changed");
                        tracing::warn!("  Error: {}", e);
                        tracing::info!("");
                        tracing::info!(
                            "✓ Automatically discarding old checkpoint and starting fresh"
                        );
                        checkpoint::remove_checkpoint(&checkpoint_path)?;
                        checkpoint::InitCheckpoint::new(
                            checkpoint_metadata.clone(),
                            &database_names,
                        )
                    }
                }
            }
            None => checkpoint::InitCheckpoint::new(checkpoint_metadata.clone(), &database_names),
        }
    } else {
        if checkpoint_path.exists() {
            tracing::info!(
                "--no-resume supplied: discarding previous checkpoint at {}",
                checkpoint_path.display()
            );
        }
        checkpoint::remove_checkpoint(&checkpoint_path)?;
        checkpoint::InitCheckpoint::new(checkpoint_metadata.clone(), &database_names)
    };

    // Persist baseline state so crashes before first database can resume cleanly
    checkpoint_state
        .save(&checkpoint_path)
        .context("Failed to persist checkpoint state")?;

    tracing::info!("Found {} database(s) to replicate", databases.len());

    // Check if user specified a target database name that doesn't match source databases
    // This is important because init preserves source database names during replication
    if let Ok(target_parts) = crate::utils::parse_postgres_url(target_url) {
        let target_db = &target_parts.database;
        let source_db_names: Vec<&str> = databases.iter().map(|db| db.name.as_str()).collect();

        if !source_db_names.contains(&target_db.as_str())
            && target_db != "postgres"
            && !target_db.is_empty()
        {
            println!();
            println!("========================================");
            println!("⚠️  IMPORTANT: Target database name ignored");
            println!("========================================");
            println!();
            println!(
                "You specified target database '{}', but replication preserves",
                target_db
            );
            println!("source database names. Data will be replicated to:");
            println!();
            for db_name in &source_db_names {
                println!("  → {}", db_name);
            }
            println!();
            println!(
                "The '{}' database in your target URL is used only for",
                target_db
            );
            println!("the initial connection. Source databases will be created");
            println!("on the target server with their original names.");
            println!();
            println!("When running 'sync' later, use the correct database name:");
            if let Some(first_db) = source_db_names.first() {
                println!("  --target \"postgresql://.../{}\"\n", first_db);
            }
            println!("========================================");
            println!();
        }
    }

    // Estimate database sizes and get confirmation
    if !skip_confirmation {
        tracing::info!("Analyzing database sizes...");
        let size_estimates = {
            // Scope the connection so it's dropped after size estimation
            let source_client = postgres::connect_with_retry(source_url).await?;
            migration::estimate_database_sizes(source_url, &source_client, &databases, &filter)
                .await?
        }; // Connection dropped here

        if !confirm_replication(&size_estimates)? {
            bail!("Replication cancelled by user");
        }
    }

    // Step 4: Replicate each database
    tracing::info!("Step 4/4: Replicating databases...");
    for (idx, db_info) in databases.iter().enumerate() {
        let filtered_tables = filter.predicate_tables(&db_info.name);
        if checkpoint_state.is_completed(&db_info.name) {
            tracing::info!(
                "Skipping database '{}' (already completed per checkpoint)",
                db_info.name
            );
            continue;
        }
        tracing::info!(
            "Replicating database {}/{}: '{}'",
            idx + 1,
            databases.len(),
            db_info.name
        );

        // Build connection URLs for this specific database
        let source_db_url = replace_database_in_url(source_url, &db_info.name)?;
        let target_db_url = replace_database_in_url(target_url, &db_info.name)?;

        // Handle database creation atomically to avoid TOCTOU race condition
        // Scope the connection so it's dropped before dump/restore subprocess operations
        {
            let target_client = postgres::connect_with_retry(target_url).await?;

            // Validate database name to prevent SQL injection
            crate::utils::validate_postgres_identifier(&db_info.name)
                .with_context(|| format!("Invalid database name: '{}'", db_info.name))?;

            // Try to create database atomically (avoids TOCTOU vulnerability)
            let create_query = format!(
                "CREATE DATABASE {}",
                crate::utils::quote_ident(&db_info.name)
            );
            match target_client.execute(&create_query, &[]).await {
                Ok(_) => {
                    tracing::info!("  Created database '{}'", db_info.name);
                }
                Err(err) => {
                    // Check if error is "database already exists" (error code 42P04)
                    if let Some(db_error) = err.as_db_error() {
                        if db_error.code() == &tokio_postgres::error::SqlState::DUPLICATE_DATABASE {
                            // Database already exists - handle based on user preferences
                            tracing::info!(
                                "  Database '{}' already exists on target",
                                db_info.name
                            );

                            // Check if empty by connecting to the SPECIFIC database
                            // (target_client is connected to default db, not the target db)
                            let is_empty = {
                                let db_client =
                                    postgres::connect_with_retry(&target_db_url).await?;
                                database_is_empty(&db_client).await?
                            }; // db_client dropped here

                            if is_empty {
                                tracing::info!(
                                    "  Database '{}' is empty, proceeding with restore",
                                    db_info.name
                                );
                            } else {
                                // Database exists and has data
                                let should_drop = if drop_existing {
                                    // Force drop with --drop-existing flag
                                    true
                                } else if skip_confirmation {
                                    // Auto-confirm drop with --yes flag (non-interactive)
                                    tracing::info!(
                                        "  Auto-confirming drop for database '{}' (--yes flag)",
                                        db_info.name
                                    );
                                    true
                                } else {
                                    // Interactive mode: prompt user
                                    prompt_drop_database(&db_info.name)?
                                };

                                if should_drop {
                                    drop_database_if_exists(&target_client, &db_info.name).await?;

                                    // Recreate the database
                                    let create_query = format!(
                                        "CREATE DATABASE {}",
                                        crate::utils::quote_ident(&db_info.name)
                                    );
                                    target_client
                                        .execute(&create_query, &[])
                                        .await
                                        .with_context(|| {
                                            format!(
                                                "Failed to create database '{}' after drop",
                                                db_info.name
                                            )
                                        })?;
                                    tracing::info!("  Created database '{}'", db_info.name);
                                } else {
                                    bail!("Aborted: Database '{}' already exists", db_info.name);
                                }
                            }
                        } else {
                            // Some other database error - propagate it
                            return Err(err).with_context(|| {
                                format!("Failed to create database '{}'", db_info.name)
                            });
                        }
                    } else {
                        // Not a database error - propagate it
                        return Err(err).with_context(|| {
                            format!("Failed to create database '{}'", db_info.name)
                        });
                    }
                }
            }
        } // Connection dropped here before dump/restore operations

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

        if !filtered_tables.is_empty() {
            tracing::info!(
                "  Applying filtered replication for {} table(s)...",
                filtered_tables.len()
            );
            migration::filtered::copy_filtered_tables(
                &source_db_url,
                &target_db_url,
                &filtered_tables,
            )
            .await?;
        }

        tracing::info!("✓ Database '{}' replicated successfully", db_info.name);

        checkpoint_state.mark_completed(&db_info.name);
        checkpoint_state
            .save(&checkpoint_path)
            .with_context(|| format!("Failed to update checkpoint for '{}'", db_info.name))?;
    }

    // Explicitly clean up temp directory
    // (This runs on normal completion; startup cleanup handles SIGKILL cases)
    if let Err(e) = crate::utils::remove_managed_temp_dir(&temp_path) {
        tracing::warn!("Failed to clean up temp directory: {}", e);
        // Don't fail the entire operation if cleanup fails
    }

    if let Err(err) = checkpoint::remove_checkpoint(&checkpoint_path) {
        tracing::warn!("Failed to remove checkpoint state: {}", err);
    }

    tracing::info!("✅ Initial replication complete");

    // Check wal_level before attempting to set up sync
    let mut should_enable_sync = enable_sync;
    if enable_sync {
        tracing::info!("Checking target wal_level for logical replication...");
        let target_wal_level = {
            // Scope the connection for quick wal_level check
            let target_client = postgres::connect_with_retry(target_url).await?;
            postgres::check_wal_level(&target_client).await?
        }; // Connection dropped here

        if target_wal_level != "logical" {
            tracing::warn!("");
            tracing::warn!("⚠ Target database wal_level is set to '{}', but 'logical' is required for continuous sync", target_wal_level);
            tracing::warn!("  Continuous replication (subscriptions) cannot be set up");
            tracing::warn!("");
            tracing::warn!("  To fix this:");
            tracing::warn!("    1. Edit postgresql.conf: wal_level = logical");
            tracing::warn!("    2. Restart PostgreSQL server");
            tracing::warn!(
                "    3. Run: postgres-seren-replicator sync --source <url> --target <url>"
            );
            tracing::warn!("");
            tracing::info!("✓ Continuing with snapshot-only replication (sync disabled)");
            should_enable_sync = false;
        }
    }

    // Set up continuous logical replication if enabled
    if should_enable_sync {
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

/// Checks if the currently connected database is empty (has no user tables).
///
/// Includes a 30-second timeout to prevent hanging on stale serverless connections.
async fn database_is_empty(client: &tokio_postgres::Client) -> Result<bool> {
    let query = "
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
    ";

    // Add timeout to prevent indefinite hang on stale serverless connections
    let row = tokio::time::timeout(
        std::time::Duration::from_secs(30),
        client.query_one(query, &[]),
    )
    .await
    .context("database_is_empty query timed out after 30 seconds")?
    .context("Failed to query information_schema.tables")?;

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
    // Skip connections owned by SUPERUSER roles (we can't terminate those on managed PostgreSQL)
    let terminate_query = "
        SELECT pg_terminate_backend(sa.pid)
        FROM pg_stat_activity sa
        JOIN pg_roles r ON sa.usename = r.rolname
        WHERE sa.datname = $1
          AND sa.pid <> pg_backend_pid()
          AND NOT r.rolsuper
    ";
    target_conn.execute(terminate_query, &[&db_name]).await?;

    // Check if any connections remain (including SUPERUSER connections we couldn't terminate)
    let remaining_query = "
        SELECT COUNT(*), STRING_AGG(DISTINCT sa.usename, ', ')
        FROM pg_stat_activity sa
        WHERE sa.datname = $1
          AND sa.pid <> pg_backend_pid()
    ";
    let row = target_conn
        .query_one(remaining_query, &[&db_name])
        .await
        .context("Failed to check remaining connections")?;
    let remaining_count: i64 = row.get(0);
    let remaining_users: Option<String> = row.get(1);

    if remaining_count > 0 {
        let users = remaining_users.unwrap_or_else(|| "unknown".to_string());
        bail!(
            "Cannot drop database '{}': {} active connection(s) from user(s): {}\n\n\
             These are likely SUPERUSER sessions that cannot be terminated by regular users.\n\
             This is common on managed PostgreSQL services (AWS RDS, SerenDB) where system\n\
             processes maintain superuser connections.\n\n\
             To resolve this:\n\
             1. Wait a few minutes and retry (system connections may be temporary)\n\
             2. Ask your database administrator to terminate the blocking sessions:\n\
                SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '{}';\n\
             3. If using AWS RDS, check for RDS-managed connections in the RDS console",
            db_name,
            remaining_count,
            users,
            db_name
        );
    }

    // Drop the database
    let drop_query = format!(
        "DROP DATABASE IF EXISTS {}",
        crate::utils::quote_ident(db_name)
    );
    target_conn
        .execute(&drop_query, &[])
        .await
        .with_context(|| format!("Failed to drop database '{}'", db_name))?;

    tracing::info!("  ✓ Database '{}' dropped", db_name);
    Ok(())
}

/// Initial replication from SQLite to PostgreSQL
///
/// Performs one-time migration of SQLite database to PostgreSQL target using JSONB storage:
/// 1. Validates SQLite file exists and is readable
/// 2. Validates PostgreSQL target connection
/// 3. Lists all tables from SQLite database
/// 4. For each table:
///    - Converts rows to JSONB format
///    - Creates JSONB table in PostgreSQL
///    - Batch inserts all data
///
/// All SQLite data is stored as JSONB with metadata:
/// - id: Original row ID (from ID column or row number)
/// - data: Complete row as JSON object
/// - _source_type: "sqlite"
/// - _migrated_at: Timestamp of migration
///
/// # Arguments
///
/// * `sqlite_path` - Path to SQLite database file (.db, .sqlite, or .sqlite3)
/// * `target_url` - PostgreSQL connection string for target (Seren) database
///
/// # Returns
///
/// Returns `Ok(())` if migration completes successfully.
///
/// # Errors
///
/// This function will return an error if:
/// - SQLite file doesn't exist or isn't readable
/// - Cannot connect to target PostgreSQL database
/// - Table conversion fails
/// - Database creation or insert operations fail
///
/// # Examples
///
/// ```no_run
/// # use anyhow::Result;
/// # use database_replicator::commands::init::init_sqlite_to_postgres;
/// # async fn example() -> Result<()> {
/// init_sqlite_to_postgres(
///     "database.db",
///     "postgresql://user:pass@seren.example.com/targetdb"
/// ).await?;
/// # Ok(())
/// # }
/// ```
pub async fn init_sqlite_to_postgres(sqlite_path: &str, target_url: &str) -> Result<()> {
    tracing::info!("Starting SQLite to PostgreSQL migration...");

    // Step 1: Validate SQLite file
    tracing::info!("Step 1/4: Validating SQLite database...");
    let canonical_path = crate::sqlite::validate_sqlite_path(sqlite_path)
        .context("SQLite file validation failed")?;
    tracing::info!("  ✓ SQLite file validated: {}", canonical_path.display());

    // Step 2: Open SQLite connection (read-only)
    tracing::info!("Step 2/4: Opening SQLite database...");
    let sqlite_conn =
        crate::sqlite::open_sqlite(sqlite_path).context("Failed to open SQLite database")?;
    tracing::info!("  ✓ SQLite database opened (read-only mode)");

    // Step 3: List all tables
    tracing::info!("Step 3/4: Discovering tables...");
    let tables = crate::sqlite::reader::list_tables(&sqlite_conn)
        .context("Failed to list tables from SQLite database")?;

    if tables.is_empty() {
        tracing::warn!("⚠ No tables found in SQLite database");
        tracing::info!("✅ Migration complete (no tables to migrate)");
        return Ok(());
    }

    tracing::info!("Found {} table(s) to migrate", tables.len());

    // Connect to PostgreSQL target
    let target_client = postgres::connect_with_retry(target_url).await?;
    tracing::info!("  ✓ Connected to PostgreSQL target");

    // Step 4: Migrate each table
    tracing::info!("Step 4/4: Migrating tables...");
    for (idx, table_name) in tables.iter().enumerate() {
        tracing::info!(
            "Migrating table {}/{}: '{}'",
            idx + 1,
            tables.len(),
            table_name
        );

        // Convert SQLite table to JSONB
        let rows = crate::sqlite::converter::convert_table_to_jsonb(&sqlite_conn, table_name)
            .with_context(|| format!("Failed to convert table '{}' to JSONB", table_name))?;

        tracing::info!("  ✓ Converted {} rows from '{}'", rows.len(), table_name);

        // Create JSONB table in PostgreSQL
        crate::jsonb::writer::create_jsonb_table(&target_client, table_name, "sqlite")
            .await
            .with_context(|| format!("Failed to create JSONB table '{}'", table_name))?;

        // Truncate existing data to make init idempotent (fixes #69)
        crate::jsonb::writer::truncate_jsonb_table(&target_client, table_name)
            .await
            .with_context(|| format!("Failed to truncate JSONB table '{}'", table_name))?;

        tracing::info!("  ✓ Created JSONB table '{}' in PostgreSQL", table_name);

        if !rows.is_empty() {
            // Batch insert all rows
            crate::jsonb::writer::insert_jsonb_batch(&target_client, table_name, rows, "sqlite")
                .await
                .with_context(|| format!("Failed to insert data into table '{}'", table_name))?;

            tracing::info!("  ✓ Inserted all rows into '{}'", table_name);
        } else {
            tracing::info!("  ✓ Table '{}' is empty (no rows to insert)", table_name);
        }
    }

    tracing::info!("✅ SQLite to PostgreSQL migration complete!");
    tracing::info!(
        "   Migrated {} table(s) from '{}' to PostgreSQL",
        tables.len(),
        sqlite_path
    );

    Ok(())
}

/// Initial replication from MongoDB to PostgreSQL
///
/// Performs one-time migration of MongoDB database to PostgreSQL target using JSONB storage:
/// 1. Validates MongoDB connection string
/// 2. Connects to MongoDB and verifies connection
/// 3. Extracts database name from connection string
/// 4. Lists all collections from MongoDB database
/// 5. For each collection:
///    - Converts documents to JSONB format
///    - Creates JSONB table in PostgreSQL
///    - Batch inserts all data
///
/// All MongoDB data is stored as JSONB with metadata:
/// - id: Original _id field (ObjectId → hex, String/Int → string)
/// - data: Complete document as JSON object
/// - _source_type: "mongodb"
/// - _migrated_at: Timestamp of migration
///
/// # Arguments
///
/// * `mongo_url` - MongoDB connection string (mongodb:// or mongodb+srv://)
/// * `target_url` - PostgreSQL connection string for target (Seren) database
///
/// # Returns
///
/// Returns `Ok(())` if migration completes successfully.
///
/// # Errors
///
/// This function will return an error if:
/// - MongoDB connection string is invalid
/// - Cannot connect to MongoDB database
/// - Database name is not specified in connection string
/// - Cannot connect to target PostgreSQL database
/// - Collection conversion fails
/// - Database creation or insert operations fail
///
/// # Examples
///
/// ```no_run
/// # use anyhow::Result;
/// # use database_replicator::commands::init::init_mongodb_to_postgres;
/// # async fn example() -> Result<()> {
/// init_mongodb_to_postgres(
///     "mongodb://localhost:27017/mydb",
///     "postgresql://user:pass@seren.example.com/targetdb"
/// ).await?;
/// # Ok(())
/// # }
/// ```
pub async fn init_mongodb_to_postgres(mongo_url: &str, target_url: &str) -> Result<()> {
    tracing::info!("Starting MongoDB to PostgreSQL migration...");

    // Step 1: Validate and connect to MongoDB
    tracing::info!("Step 1/5: Validating MongoDB connection...");
    let client = crate::mongodb::connect_mongodb(mongo_url)
        .await
        .context("MongoDB connection failed")?;
    tracing::info!("  ✓ MongoDB connection validated");

    // Step 2: Extract database name
    tracing::info!("Step 2/5: Extracting database name...");
    let db_name = crate::mongodb::extract_database_name(mongo_url)
        .await
        .context("Failed to parse MongoDB connection string")?
        .context("MongoDB URL must include database name (e.g., mongodb://host:27017/dbname)")?;
    tracing::info!("  ✓ Database name: '{}'", db_name);

    // Step 3: List all collections
    tracing::info!("Step 3/5: Discovering collections...");
    let db = client.database(&db_name);
    let collections = crate::mongodb::reader::list_collections(&client, &db_name)
        .await
        .context("Failed to list collections from MongoDB database")?;

    if collections.is_empty() {
        tracing::warn!("⚠ No collections found in MongoDB database '{}'", db_name);
        tracing::info!("✅ Migration complete (no collections to migrate)");
        return Ok(());
    }

    tracing::info!("Found {} collection(s) to migrate", collections.len());

    // Step 4: Connect to PostgreSQL target
    tracing::info!("Step 4/5: Connecting to PostgreSQL target...");
    let target_client = postgres::connect_with_retry(target_url).await?;
    tracing::info!("  ✓ Connected to PostgreSQL target");

    // Step 5: Migrate each collection
    tracing::info!("Step 5/5: Migrating collections...");
    for (idx, collection_name) in collections.iter().enumerate() {
        tracing::info!(
            "Migrating collection {}/{}: '{}'",
            idx + 1,
            collections.len(),
            collection_name
        );

        // Convert MongoDB collection to JSONB
        let rows = crate::mongodb::converter::convert_collection_to_jsonb(&db, collection_name)
            .await
            .with_context(|| {
                format!(
                    "Failed to convert collection '{}' to JSONB",
                    collection_name
                )
            })?;

        tracing::info!(
            "  ✓ Converted {} documents from '{}'",
            rows.len(),
            collection_name
        );

        // Create JSONB table in PostgreSQL
        crate::jsonb::writer::create_jsonb_table(&target_client, collection_name, "mongodb")
            .await
            .with_context(|| format!("Failed to create JSONB table '{}'", collection_name))?;

        // Truncate existing data to make init idempotent (fixes #69)
        crate::jsonb::writer::truncate_jsonb_table(&target_client, collection_name)
            .await
            .with_context(|| format!("Failed to truncate JSONB table '{}'", collection_name))?;

        tracing::info!(
            "  ✓ Created JSONB table '{}' in PostgreSQL",
            collection_name
        );

        if !rows.is_empty() {
            // Batch insert all rows
            crate::jsonb::writer::insert_jsonb_batch(
                &target_client,
                collection_name,
                rows,
                "mongodb",
            )
            .await
            .with_context(|| format!("Failed to insert data into table '{}'", collection_name))?;

            tracing::info!("  ✓ Inserted all documents into '{}'", collection_name);
        } else {
            tracing::info!(
                "  ✓ Collection '{}' is empty (no documents to insert)",
                collection_name
            );
        }
    }

    tracing::info!("✅ MongoDB to PostgreSQL migration complete!");
    tracing::info!(
        "   Migrated {} collection(s) from database '{}' to PostgreSQL",
        collections.len(),
        db_name
    );

    Ok(())
}

/// Initial replication from MySQL to PostgreSQL
///
/// Performs one-time replication of MySQL database to PostgreSQL target using JSONB storage:
/// 1. Validates MySQL connection string
/// 2. Connects to MySQL and verifies connection
/// 3. Extracts database name from connection string
/// 4. Lists all tables from MySQL database
/// 5. For each table:
///    - Converts rows to JSONB format
///    - Creates JSONB table in PostgreSQL
///    - Batch inserts all data
///
/// All MySQL data is stored as JSONB with metadata:
/// - id: Primary key or auto-generated ID
/// - data: Complete row as JSON object
/// - _source_type: "mysql"
/// - _migrated_at: Timestamp of replication
///
/// # Arguments
///
/// * `mysql_url` - MySQL connection string (mysql://...)
/// * `target_url` - PostgreSQL connection string for target (Seren) database
///
/// # Returns
///
/// Returns `Ok(())` if replication completes successfully.
///
/// # Errors
///
/// This function will return an error if:
/// - MySQL connection string is invalid
/// - Cannot connect to MySQL database
/// - Database name is not specified in connection string
/// - Cannot connect to target PostgreSQL database
/// - Table conversion fails
/// - Database creation or insert operations fail
///
/// # Examples
///
/// ```no_run
/// # use anyhow::Result;
/// # use database_replicator::commands::init::init_mysql_to_postgres;
/// # async fn example() -> Result<()> {
/// init_mysql_to_postgres(
///     "mysql://user:pass@localhost:3306/mydb",
///     "postgresql://user:pass@seren.example.com/targetdb"
/// ).await?;
/// # Ok(())
/// # }
/// ```
pub async fn init_mysql_to_postgres(mysql_url: &str, target_url: &str) -> Result<()> {
    tracing::info!("Starting MySQL to PostgreSQL replication...");

    // Step 1: Validate and connect to MySQL
    tracing::info!("Step 1/5: Validating MySQL connection...");
    let mut mysql_conn = crate::mysql::connect_mysql(mysql_url)
        .await
        .context("MySQL connection failed")?;
    tracing::info!("  ✓ MySQL connection validated");

    // Step 2: Extract database name
    tracing::info!("Step 2/5: Extracting database name...");
    let db_name = crate::mysql::extract_database_name(mysql_url)
        .context("MySQL URL must include database name (e.g., mysql://host:3306/dbname)")?;
    tracing::info!("  ✓ Database name: '{}'", db_name);

    // Step 3: List all tables
    tracing::info!("Step 3/5: Discovering tables...");
    let tables = crate::mysql::reader::list_tables(&mut mysql_conn, &db_name)
        .await
        .context("Failed to list tables from MySQL database")?;

    if tables.is_empty() {
        tracing::warn!("⚠ No tables found in MySQL database '{}'", db_name);
        tracing::info!("✅ Replication complete (no tables to replicate)");
        return Ok(());
    }

    tracing::info!("Found {} table(s) to replicate", tables.len());

    // Step 4: Connect to PostgreSQL target
    tracing::info!("Step 4/5: Connecting to PostgreSQL target...");
    let target_client = postgres::connect_with_retry(target_url).await?;
    tracing::info!("  ✓ Connected to PostgreSQL target");

    // Step 5: Replicate each table
    tracing::info!("Step 5/5: Replicating tables...");
    for (idx, table_name) in tables.iter().enumerate() {
        tracing::info!(
            "Replicating table {}/{}: '{}'",
            idx + 1,
            tables.len(),
            table_name
        );

        // Convert MySQL table to JSONB
        let rows =
            crate::mysql::converter::convert_table_to_jsonb(&mut mysql_conn, &db_name, table_name)
                .await
                .with_context(|| format!("Failed to convert table '{}' to JSONB", table_name))?;

        tracing::info!("  ✓ Converted {} rows from '{}'", rows.len(), table_name);

        // Create JSONB table in PostgreSQL
        crate::jsonb::writer::create_jsonb_table(&target_client, table_name, "mysql")
            .await
            .with_context(|| format!("Failed to create JSONB table '{}'", table_name))?;

        // Truncate existing data to make init idempotent (fixes #69)
        crate::jsonb::writer::truncate_jsonb_table(&target_client, table_name)
            .await
            .with_context(|| format!("Failed to truncate JSONB table '{}'", table_name))?;

        tracing::info!("  ✓ Created JSONB table '{}' in PostgreSQL", table_name);

        if !rows.is_empty() {
            // Batch insert all rows
            crate::jsonb::writer::insert_jsonb_batch(&target_client, table_name, rows, "mysql")
                .await
                .with_context(|| format!("Failed to insert data into table '{}'", table_name))?;

            tracing::info!("  ✓ Inserted all rows into '{}'", table_name);
        } else {
            tracing::info!("  ✓ Table '{}' is empty (no rows to insert)", table_name);
        }
    }

    tracing::info!("✅ MySQL to PostgreSQL replication complete!");
    tracing::info!(
        "   Replicated {} table(s) from database '{}' to PostgreSQL",
        tables.len(),
        db_name
    );

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
        let result = init(&source, &target, true, filter, false, false, true, false).await;
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
    async fn test_database_is_empty() {
        let url = std::env::var("TEST_TARGET_URL").expect("TEST_TARGET_URL not set");

        // Connect to the database first
        let client = crate::postgres::connect_with_retry(&url)
            .await
            .expect("Failed to connect");

        // postgres database might be empty of user tables
        // This test just verifies the function doesn't crash
        let result = database_is_empty(&client).await;
        assert!(result.is_ok());
    }
}
