// ABOUTME: CLI entry point for database-replicator
// ABOUTME: Parses commands and routes to appropriate handlers

use anyhow::Context;
use clap::{Args, Parser, Subcommand};
use database_replicator::commands;
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "database-replicator")]
#[command(about = "Universal database-to-PostgreSQL replication CLI", long_about = None)]
#[command(version)]
struct Cli {
    /// Allow self-signed TLS certificates (insecure - use only for testing)
    #[arg(
        long = "allow-self-signed-certs",
        global = true,
        default_value_t = false
    )]
    allow_self_signed_certs: bool,
    /// Set the log level (error, warn, info, debug, trace)
    #[arg(long, global = true, default_value = "info")]
    log: String,
    /// SerenDB API key for interactive target selection (falls back to SEREN_API_KEY env)
    #[arg(long = "api-key", env = "SEREN_API_KEY", global = true)]
    api_key: Option<String>,
    #[command(subcommand)]
    command: Commands,
}

#[derive(Args, Clone, Default)]
struct TableRuleArgs {
    /// Tables (optionally db.table) to replicate as schema-only
    #[arg(long = "schema-only-tables", value_delimiter = ',')]
    schema_only_tables: Vec<String>,
    /// Table-level filters in the form [db.]table:SQL-predicate (repeatable)
    #[arg(long = "table-filter")]
    table_filters: Vec<String>,
    /// Time filters in the form [db.]table:column:window (e.g., db.metrics:created_at:6 months)
    #[arg(long = "time-filter")]
    time_filters: Vec<String>,
    /// Path to replication-config.toml describing advanced table rules
    #[arg(long = "config")]
    config_path: Option<String>,
}

#[derive(Subcommand)]
enum Commands {
    /// Validate source and target databases are ready for replication
    Validate {
        #[arg(long)]
        source: String,
        #[arg(long)]
        target: Option<String>,
        /// Include only these databases (comma-separated)
        #[arg(long, value_delimiter = ',')]
        include_databases: Option<Vec<String>>,
        /// Exclude these databases (comma-separated)
        #[arg(long, value_delimiter = ',')]
        exclude_databases: Option<Vec<String>>,
        /// Include only these tables (format: database.table, comma-separated)
        #[arg(long, value_delimiter = ',')]
        include_tables: Option<Vec<String>>,
        /// Exclude these tables (format: database.table, comma-separated)
        #[arg(long, value_delimiter = ',')]
        exclude_tables: Option<Vec<String>>,
        /// Disable interactive mode (use CLI filter flags instead)
        #[arg(long)]
        no_interactive: bool,
    },
    /// Initialize replication with snapshot copy of schema and data
    Init {
        #[arg(long)]
        source: String,
        #[arg(long)]
        target: Option<String>,
        /// Skip confirmation prompt
        #[arg(short = 'y', long)]
        yes: bool,
        /// Include only these databases (comma-separated)
        #[arg(long, value_delimiter = ',')]
        include_databases: Option<Vec<String>>,
        /// Exclude these databases (comma-separated)
        #[arg(long, value_delimiter = ',')]
        exclude_databases: Option<Vec<String>>,
        /// Include only these tables (format: database.table, comma-separated)
        #[arg(long, value_delimiter = ',')]
        include_tables: Option<Vec<String>>,
        /// Exclude these tables (format: database.table, comma-separated)
        #[arg(long, value_delimiter = ',')]
        exclude_tables: Option<Vec<String>>,
        /// Disable interactive mode (use CLI filter flags instead)
        #[arg(long)]
        no_interactive: bool,
        #[command(flatten)]
        table_rules: TableRuleArgs,
        /// Drop existing databases on target before copying
        #[arg(long)]
        drop_existing: bool,
        /// Enable continuous replication after snapshot (default)
        #[arg(long)]
        sync: bool,
        /// Disable automatic continuous replication setup after snapshot
        #[arg(long)]
        no_sync: bool,
        /// Ignore any previous checkpoint and start a fresh run
        #[arg(long)]
        no_resume: bool,
        /// Execute on SerenAI's managed cloud infrastructure (requires SerenDB target)
        #[arg(long)]
        seren: bool,
        /// Execute replication locally on your machine (required for non-SerenDB targets)
        #[arg(long)]
        local: bool,
        /// API endpoint for SerenAI cloud execution
        #[arg(long, default_value_t = String::from("https://replicate.serendb.com"))]
        seren_api: String,
        /// Maximum job duration in seconds before timeout (default: 28800 = 8 hours)
        #[arg(long, default_value_t = 28800)]
        job_timeout: u64,
    },
    /// Set up continuous replication from source to target (auto-detects best method)
    ///
    /// Automatically detects source database capabilities:
    /// - If source has wal_level=logical: uses PostgreSQL logical replication (fastest)
    /// - If source has wal_level=replica: uses xmin-based polling (no config required)
    Sync {
        #[arg(long)]
        source: Option<String>,
        #[arg(long)]
        target: Option<String>,
        /// Include only these databases (comma-separated)
        #[arg(long, value_delimiter = ',')]
        include_databases: Option<Vec<String>>,
        /// Exclude these databases (comma-separated)
        #[arg(long, value_delimiter = ',')]
        exclude_databases: Option<Vec<String>>,
        /// Include only these tables (format: database.table, comma-separated)
        #[arg(long, value_delimiter = ',')]
        include_tables: Option<Vec<String>>,
        /// Exclude these tables (format: database.table, comma-separated)
        #[arg(long, value_delimiter = ',')]
        exclude_tables: Option<Vec<String>>,
        /// Disable interactive mode (use CLI filter flags instead)
        #[arg(long)]
        no_interactive: bool,
        #[command(flatten)]
        table_rules: TableRuleArgs,
        /// Force recreate subscriptions even if they already exist
        #[arg(long)]
        force: bool,
        /// SerenDB project ID (for auto-enabling logical replication)
        #[arg(long)]
        project_id: Option<String>,
        /// SerenDB Console API URL (defaults to https://api.serendb.com)
        #[arg(long, default_value = "https://api.serendb.com")]
        console_api: String,
        /// Sync interval in seconds for xmin-based sync (default: 3600 = 1 hour)
        #[arg(long, default_value_t = 3600)]
        sync_interval: u64,
        /// Reconciliation interval in seconds for xmin-based sync (default: 86400 = 1 day)
        #[arg(long, default_value_t = 86400)]
        reconcile_interval: u64,
        /// Run a single sync cycle and exit (don't run continuously)
        #[arg(long)]
        once: bool,
        /// Disable reconciliation (delete detection) for xmin-based sync
        #[arg(long)]
        no_reconcile: bool,
        /// Run sync as a background daemon (detaches from terminal)
        #[arg(long)]
        daemon: bool,
        /// Stop a running sync daemon
        #[arg(long)]
        stop: bool,
        /// Show status of the sync daemon
        #[arg(long)]
        daemon_status: bool,
    },
    /// Consume sqlite-watcher change batches and apply them to SerenDB JSONB tables
    SyncSqlite {
        /// Target PostgreSQL/Seren connection string
        #[arg(long)]
        target: String,
        /// sqlite-watcher endpoint (unix:/path or tcp:host:port)
        #[arg(long, default_value = "unix:/tmp/sqlite-watcher.sock")]
        watcher_endpoint: String,
        /// Optional shared-secret token file (defaults to ~/.seren/sqlite-watcher/token)
        #[arg(long)]
        token_file: Option<PathBuf>,
        /// Incremental mode: append (raw only) or append_deduped (maintains *_latest tables)
        #[arg(long, value_enum, default_value = "append")]
        incremental_mode: commands::sync_sqlite::IncrementalMode,
        /// Number of watcher rows to pull per batch
        #[arg(long, default_value_t = 500)]
        batch_size: u32,
    },
    /// Check replication status and lag in real-time
    Status {
        #[arg(long)]
        source: String,
        #[arg(long)]
        target: Option<String>,
        /// Include only these databases (comma-separated)
        #[arg(long, value_delimiter = ',')]
        include_databases: Option<Vec<String>>,
        /// Exclude these databases (comma-separated)
        #[arg(long, value_delimiter = ',')]
        exclude_databases: Option<Vec<String>>,
    },
    /// Verify data integrity between source and target
    Verify {
        #[arg(long)]
        source: String,
        #[arg(long)]
        target: Option<String>,
        /// Include only these databases (comma-separated)
        #[arg(long, value_delimiter = ',')]
        include_databases: Option<Vec<String>>,
        /// Exclude these databases (comma-separated)
        #[arg(long, value_delimiter = ',')]
        exclude_databases: Option<Vec<String>>,
        /// Include only these tables (format: database.table, comma-separated)
        #[arg(long, value_delimiter = ',')]
        include_tables: Option<Vec<String>>,
        /// Exclude these tables (format: database.table, comma-separated)
        #[arg(long, value_delimiter = ',')]
        exclude_tables: Option<Vec<String>>,
    },
    /// Manage the target database URL
    Target {
        #[command(flatten)]
        args: commands::target::TargetArgs,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // We need to parse CLI args early to get the log level
    let cli = Cli::parse();
    let global_api_key = cli.api_key.clone();

    // Initialize logging
    // 1. RUST_LOG environment variable has highest precedence
    // 2. --log flag is used if RUST_LOG is not set
    // 3. Default to "info" if neither are provided
    let env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(cli.log.clone()));

    tracing_subscriber::fmt().with_env_filter(env_filter).init();

    // Clean up stale temp directories from previous runs (older than 24 hours)
    // This handles temp files left behind by processes killed with SIGKILL
    if let Err(e) = database_replicator::utils::cleanup_stale_temp_dirs(86400) {
        tracing::warn!("Failed to clean up stale temp directories: {}", e);
        // Don't fail startup if cleanup fails
    }

    // Initialize TLS policy using thread-safe OnceLock
    database_replicator::postgres::connection::init_tls_policy(cli.allow_self_signed_certs);

    match cli.command {
        Commands::Validate {
            source,
            target,
            include_databases,
            exclude_databases,
            include_tables,
            exclude_tables,
            no_interactive,
        } => {
            let state = database_replicator::state::load()?;
            let target = target.or(state.target_url).ok_or_else(|| {
                anyhow::anyhow!("Target database URL not provided and not set in state. Use `--target` or `database-replicator target set`.")
            })?;

            // Detect source type - interactive mode only works with PostgreSQL
            let source_type = database_replicator::detect_source_type(&source)
                .context("Failed to detect source database type")?;
            let is_postgres_source =
                matches!(source_type, database_replicator::SourceType::PostgreSQL);

            let filter = if !no_interactive && is_postgres_source {
                // Interactive mode (default) - prompt user to select databases and tables
                let (filter, rules) =
                    database_replicator::interactive::select_databases_and_tables(&source).await?;
                filter.with_table_rules(rules)
            } else {
                // CLI mode - use provided filter arguments
                database_replicator::filters::ReplicationFilter::new(
                    include_databases,
                    exclude_databases,
                    include_tables,
                    exclude_tables,
                )?
            };
            commands::validate(&source, &target, filter).await
        }
        Commands::Init {
            source,
            target,
            yes,
            include_databases,
            exclude_databases,
            include_tables,
            exclude_tables,
            no_interactive,
            table_rules,
            drop_existing,
            sync: _, // sync is the default behavior, no_sync overrides it
            no_sync,
            no_resume,
            seren,
            local,
            seren_api,
            job_timeout,
        } => {
            let mut state = database_replicator::state::load()?;
            let mut target = target.or(state.target_url);
            let mut seren_target_state: Option<database_replicator::serendb::TargetState> = None;

            // If no target and not forcing local execution, trigger interactive project selection
            // This is the default behavior - remote execution with SerenDB target picker
            if target.is_none() && !local {
                let (conn_str, target_state) =
                    database_replicator::interactive::select_seren_database().await?;
                target = Some(conn_str);
                // Save target state for use by subsequent commands (sync, status, etc.)
                database_replicator::serendb::save_target_state(&target_state)?;
                seren_target_state = Some(target_state);
            }

            // If --seren flag explicitly set, validate target is SerenDB
            if seren {
                if let Some(t) = &target {
                    if !database_replicator::utils::is_serendb_target(t) {
                        anyhow::bail!("--seren flag is only compatible with SerenDB targets.");
                    }
                }
            }

            let target = target.ok_or_else(|| {
                anyhow::anyhow!("Target database URL not provided. Use `--target` to specify a target database, or remove `--local` to use interactive SerenDB project selection.")
            })?;

            // Check if CLI filter flags were provided (skip interactive if so)
            let has_cli_filters = include_databases.is_some()
                || exclude_databases.is_some()
                || include_tables.is_some()
                || exclude_tables.is_some();

            // Detect source type early to determine if interactive mode is supported
            let source_type = database_replicator::detect_source_type(&source)
                .context("Failed to detect source database type")?;
            let is_postgres_source =
                matches!(source_type, database_replicator::SourceType::PostgreSQL);

            // Interactive mode is default unless:
            // - --no-interactive flag is set
            // - --yes flag is set (implies automation)
            // - CLI filter flags are provided
            // - Source is not PostgreSQL (interactive mode only works with PostgreSQL sources)
            // Run this BEFORE remote execution check so interactive mode works for both local and remote
            let (
                final_include_databases,
                final_exclude_databases,
                final_include_tables,
                final_exclude_tables,
            ) = if !no_interactive && !yes && !has_cli_filters && is_postgres_source {
                // Interactive mode (default) - prompt user to select databases and tables
                let (filter, _rules) =
                    database_replicator::interactive::select_databases_and_tables(&source).await?;

                // Extract filter values to pass to init_remote or local init
                (
                    filter.include_databases().map(|v| v.to_vec()),
                    filter.exclude_databases().map(|v| v.to_vec()),
                    filter.include_tables().map(|v| v.to_vec()),
                    filter.exclude_tables().map(|v| v.to_vec()),
                )
            } else {
                // CLI mode - use provided filter arguments
                (
                    include_databases,
                    exclude_databases,
                    include_tables,
                    exclude_tables,
                )
            };

            // Determine execution mode:
            // 1. --seren flag → remote execution
            // 2. --local flag → local execution
            // 3. Non-PostgreSQL sources (SQLite, MongoDB, MySQL) → local execution (required)
            // 4. Neither → auto-detect based on target URL (SerenDB = remote)
            let use_remote = if seren {
                true
            } else if local {
                false
            } else if !is_postgres_source {
                // Non-PostgreSQL sources require local execution - remote can't access local files
                false
            } else {
                // Auto-detect: SerenDB targets default to remote execution
                database_replicator::utils::is_serendb_target(&target)
            };

            if use_remote {
                tracing::info!("Using SerenAI cloud execution");
                init_remote(
                    source,
                    target.clone(),
                    seren_target_state,
                    yes,
                    final_include_databases,
                    final_exclude_databases,
                    final_include_tables,
                    final_exclude_tables,
                    drop_existing,
                    no_sync,
                    seren_api,
                    job_timeout,
                    cli.log,
                )
                .await?;
            } else {
                // Local execution path
                // Clone filter values for potential fallback to remote
                let fallback_include_dbs = final_include_databases.clone();
                let fallback_exclude_dbs = final_exclude_databases.clone();
                let fallback_include_tables = final_include_tables.clone();
                let fallback_exclude_tables = final_exclude_tables.clone();

                let filter = database_replicator::filters::ReplicationFilter::new(
                    final_include_databases,
                    final_exclude_databases,
                    final_include_tables,
                    final_exclude_tables,
                )?;
                let table_rule_data = build_table_rules(&table_rules)?;
                let filter = filter.with_table_rules(table_rule_data);

                let enable_sync = !no_sync; // Invert the flag: by default sync is enabled

                // Run init with pre-flight checks, handle fallback to remote
                match commands::init(
                    &source,
                    &target,
                    yes,
                    filter,
                    drop_existing,
                    enable_sync,
                    !no_resume,
                    local, // Pass whether --local was explicit
                )
                .await
                {
                    Ok(_) => {}
                    Err(e) if e.to_string().contains("PREFLIGHT_FALLBACK_TO_REMOTE") => {
                        // Auto-fallback to remote execution
                        init_remote(
                            source,
                            target.clone(),
                            None, // No saved target state in fallback path
                            yes,
                            fallback_include_dbs,
                            fallback_exclude_dbs,
                            fallback_include_tables,
                            fallback_exclude_tables,
                            drop_existing,
                            no_sync,
                            seren_api,
                            job_timeout,
                            cli.log,
                        )
                        .await?;
                    }
                    Err(e) => return Err(e),
                }
            }
            state.target_url = Some(target);
            database_replicator::state::save(&state)?;
            Ok(())
        }
        Commands::Sync {
            source,
            target,
            include_databases,
            exclude_databases,
            include_tables,
            exclude_tables,
            no_interactive,
            table_rules,
            force,
            project_id,
            console_api,
            sync_interval,
            reconcile_interval,
            once,
            no_reconcile,
            daemon,
            stop,
            daemon_status,
        } => {
            // Handle daemon control commands first (don't require source/target)
            if stop {
                return match database_replicator::daemon::stop_daemon()? {
                    true => {
                        println!("Daemon stopped successfully");
                        Ok(())
                    }
                    false => {
                        println!("No daemon was running");
                        Ok(())
                    }
                };
            }

            if daemon_status {
                return database_replicator::daemon::print_status();
            }

            // For actual sync, source is required
            let source = source.ok_or_else(|| {
                anyhow::anyhow!(
                    "Source database URL is required for sync.\n\
                     Use --source to specify a source database.\n\
                     (Use --stop to stop a running daemon, or --daemon-status to check status)"
                )
            })?;

            // Handle daemon child process initialization (Windows)
            #[cfg(windows)]
            if database_replicator::daemon::is_daemon_child() {
                let _log_file = database_replicator::daemon::init_daemon_child()?;
                // Note: We can't easily re-initialize the global subscriber on Windows,
                // so we just proceed with existing logging (logs go to parent's console)
                tracing::info!("Daemon child process started (PID: {})", std::process::id());
            }

            // If --daemon flag is set, daemonize before continuing
            if daemon {
                database_replicator::daemon::daemonize()?;
                // After daemonize(), we're running in the child process
            }

            let mut app_state = database_replicator::state::load()?;
            let target_candidate = target.or(app_state.target_url.clone());
            let resolved_target = database_replicator::commands::sync::resolve_target_for_sync(
                target_candidate,
                global_api_key.clone(),
                &source,
            )
            .await?;
            app_state.target_url = Some(resolved_target.clone());
            database_replicator::state::save(&app_state)?;

            // Check if CLI filter flags were provided (skip interactive if so)
            let has_cli_filters = include_databases.is_some()
                || exclude_databases.is_some()
                || include_tables.is_some()
                || exclude_tables.is_some();

            // Detect source type - interactive mode only works with PostgreSQL
            let source_type = database_replicator::detect_source_type(&source)
                .context("Failed to detect source database type")?;
            let is_postgres_source =
                matches!(source_type, database_replicator::SourceType::PostgreSQL);

            let filter = if !no_interactive && !has_cli_filters && is_postgres_source {
                // Interactive mode (default) - prompt user to select databases and tables
                let (filter, rules) =
                    database_replicator::interactive::select_databases_and_tables(&source).await?;
                filter.with_table_rules(rules)
            } else {
                // CLI mode - use provided filter arguments
                let filter = database_replicator::filters::ReplicationFilter::new(
                    include_databases,
                    exclude_databases,
                    include_tables,
                    exclude_tables,
                )?;
                let table_rule_data = build_table_rules(&table_rules)?;
                filter.with_table_rules(table_rule_data)
            };

            // Get project_id from CLI, saved target state, or discover from target URL
            let mut effective_project_id = project_id.or_else(|| {
                database_replicator::serendb::load_target_state()
                    .ok()
                    .flatten()
                    .map(|state| state.project_id)
            });

            // If project_id is still None and target is SerenDB, try to discover it by hostname
            if effective_project_id.is_none()
                && database_replicator::utils::is_serendb_target(&resolved_target)
            {
                // Get API key from CLI/env, or prompt user interactively
                let api_key = global_api_key
                    .clone()
                    .or_else(|| database_replicator::interactive::get_api_key().ok());

                if let Some(api_key) = api_key {
                    // Extract hostname from target URL
                    if let Ok(parts) =
                        database_replicator::utils::parse_postgres_url(&resolved_target)
                    {
                        tracing::info!(
                            "Discovering SerenDB project for hostname {}...",
                            parts.host
                        );
                        let client = database_replicator::serendb::ConsoleClient::new(
                            Some(&console_api),
                            api_key,
                        );
                        match client.find_project_by_hostname(&parts.host).await {
                            Ok(Some(project_id)) => {
                                effective_project_id = Some(project_id);
                            }
                            Ok(None) => {
                                tracing::warn!(
                                    "Could not find SerenDB project matching hostname {}. \
                                     Logical replication auto-enable will be skipped.",
                                    parts.host
                                );
                            }
                            Err(e) => {
                                tracing::warn!(
                                    "Failed to discover project from hostname: {}. \
                                     Logical replication auto-enable will be skipped.",
                                    e
                                );
                            }
                        }
                    }
                } else {
                    tracing::debug!(
                        "No API key available, skipping project discovery from target hostname"
                    );
                }
            }

            // If project_id is available and target is SerenDB, check/enable logical replication
            if let Some(ref project_id) = effective_project_id {
                if database_replicator::utils::is_serendb_target(&resolved_target) {
                    check_and_enable_logical_replication(
                        project_id,
                        &console_api,
                        &resolved_target,
                    )
                    .await?;
                }
            }

            // Auto-detect source wal_level to choose sync method
            tracing::info!("Checking source database capabilities...");
            let source_client = database_replicator::postgres::connect(&source)
                .await
                .context("Failed to connect to source database for capability detection")?;
            let source_wal_level = database_replicator::postgres::check_wal_level(&source_client)
                .await
                .unwrap_or_else(|_| "unknown".to_string());
            drop(source_client); // Release connection before sync

            if source_wal_level == "logical" {
                tracing::info!("Source has wal_level=logical (logical replication available)");
                tracing::info!("Using PostgreSQL logical replication (fastest method)");

                commands::sync(
                    &source,
                    &resolved_target,
                    Some(filter),
                    None,
                    None,
                    None,
                    force,
                )
                .await
            } else {
                tracing::info!(
                    "Source has wal_level={} (logical replication not available)",
                    source_wal_level
                );
                tracing::info!("Using xmin-based sync (no source configuration required)");

                // Extract tables from filter for xmin sync
                // Filter stores "db.table" format, we need just table names for the source db
                let source_parts = database_replicator::utils::parse_postgres_url(&source)?;
                let source_db = &source_parts.database;

                let tables_to_sync: Option<Vec<String>> = filter.include_tables().map(|tables| {
                    tables
                        .iter()
                        .filter_map(|qualified| {
                            // Split "db.table" into parts
                            let parts: Vec<&str> = qualified.splitn(2, '.').collect();
                            if parts.len() == 2 {
                                let (db, table) = (parts[0], parts[1]);
                                // Only include tables from the source database
                                if db == source_db {
                                    Some(table.to_string())
                                } else {
                                    None
                                }
                            } else {
                                // No dot, treat as plain table name
                                Some(qualified.clone())
                            }
                        })
                        .collect()
                });

                // Use CLI-provided intervals or defaults
                xmin_sync(
                    source,
                    resolved_target,
                    "public".to_string(), // Default schema
                    tables_to_sync,       // Tables from filter
                    sync_interval,        // CLI: --sync-interval (default 60s)
                    reconcile_interval,   // CLI: --reconcile-interval (default 3600s)
                    database_replicator::utils::calculate_optimal_batch_size(), // Auto-detect based on available memory
                    None,         // State file: use default
                    once,         // CLI: --once (run single cycle)
                    no_reconcile, // CLI: --no-reconcile (disable delete detection)
                )
                .await
            }
        }
        Commands::Status {
            source,
            target,
            include_databases,
            exclude_databases,
        } => {
            let state = database_replicator::state::load()?;
            let target = target.or(state.target_url).ok_or_else(|| {
                anyhow::anyhow!("Target database URL not provided and not set in state. Use `--target` or `database-replicator target set`.")
            })?;

            let filter = database_replicator::filters::ReplicationFilter::new(
                include_databases,
                exclude_databases,
                None,
                None,
            )?;
            commands::status(&source, &target, Some(filter)).await
        }
        Commands::Verify {
            source,
            target,
            include_databases,
            exclude_databases,
            include_tables,
            exclude_tables,
        } => {
            let state = database_replicator::state::load()?;
            let target = target.or(state.target_url).ok_or_else(|| {
                anyhow::anyhow!("Target database URL not provided and not set in state. Use `--target` or `database-replicator target set`.")
            })?;

            let filter = database_replicator::filters::ReplicationFilter::new(
                include_databases,
                exclude_databases,
                include_tables,
                exclude_tables,
            )?;
            commands::verify(&source, &target, Some(filter)).await
        }
        Commands::SyncSqlite {
            target,
            watcher_endpoint,
            token_file,
            incremental_mode,
            batch_size,
        } => {
            commands::sync_sqlite::run(commands::sync_sqlite::SyncSqliteOptions {
                target,
                watcher_endpoint,
                token_file,
                incremental_mode,
                batch_size,
            })
            .await
        }
        Commands::Target { args } => commands::target(args).await,
    }
}

/// Check if logical replication is enabled on SerenDB project and offer to enable it
async fn check_and_enable_logical_replication(
    project_id: &str,
    console_api: &str,
    target_url: &str,
) -> anyhow::Result<()> {
    use database_replicator::serendb::ConsoleClient;
    use dialoguer::{theme::ColorfulTheme, Confirm};

    tracing::info!("Checking logical replication status for SerenDB project...");

    // Get API key from interactive module (handles env var or prompt)
    let api_key = database_replicator::interactive::get_api_key()?;

    // Create Console API client
    let client = ConsoleClient::new(Some(console_api), api_key);

    // Check if logical replication is already enabled
    let project = client.get_project(project_id).await?;

    if project.enable_logical_replication {
        tracing::info!(
            "✓ Logical replication is already enabled for project '{}'",
            project.name
        );
        // Verify the actual wal_level on the database (endpoint may still be restarting)
        match database_replicator::postgres::connect_with_retry(target_url).await {
            Ok(client) => {
                if let Ok(row) = client.query_one("SHOW wal_level", &[]).await {
                    let level: String = row.get(0);
                    if level == "logical" {
                        return Ok(());
                    }
                    // wal_level not yet 'logical', need to wait for endpoint restart
                    tracing::info!(
                        "Endpoint has wal_level='{}', waiting for restart to apply 'logical'...",
                        level
                    );
                }
            }
            Err(_) => {
                tracing::info!("Endpoint may be restarting, will poll for readiness...");
            }
        }
        // Fall through to wait for wal_level to become 'logical'
        println!();
        println!("⏳ Waiting for endpoint to restart with wal_level=logical...");
        wait_for_wal_level_logical(target_url).await?;
        return Ok(());
    }

    // Logical replication is not enabled - prompt user
    println!();
    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║  Logical Replication Required                                ║");
    println!("╚══════════════════════════════════════════════════════════════╝");
    println!();
    println!(
        "Project '{}' does not have logical replication enabled.",
        project.name
    );
    println!();
    println!("Logical replication is required for the 'sync' command to set up");
    println!("continuous replication between your source and target databases.");
    println!();
    println!("⚠️  Important:");
    println!("   • Enabling logical replication will briefly suspend all active endpoints");
    println!("   • Once enabled, logical replication CANNOT be disabled");
    println!();

    let confirm = Confirm::with_theme(&ColorfulTheme::default())
        .with_prompt("Enable logical replication for this project?")
        .default(true)
        .interact()?;

    if !confirm {
        anyhow::bail!(
            "Logical replication is required for the sync command.\n\
             \n\
             You can enable it manually at:\n\
             https://console.serendb.com/projects/{}/settings",
            project_id
        );
    }

    // Enable logical replication
    tracing::info!("Enabling logical replication...");
    let updated_project = client.enable_logical_replication(project_id).await?;

    if updated_project.enable_logical_replication {
        println!();
        println!("✓ Logical replication enabled successfully!");
        println!();
        println!("⏳ Waiting for endpoint to restart with wal_level=logical...");

        wait_for_wal_level_logical(target_url).await?;
    } else {
        anyhow::bail!(
            "Failed to enable logical replication. The API call succeeded but the setting was not updated.\n\
             Please try enabling it manually at:\n\
             https://console.serendb.com/projects/{}/settings",
            project_id
        );
    }

    Ok(())
}

/// Poll the database until wal_level becomes 'logical' (up to 60 seconds)
async fn wait_for_wal_level_logical(target_url: &str) -> anyhow::Result<()> {
    let max_attempts = 12;
    let poll_interval = tokio::time::Duration::from_secs(5);

    for attempt in 1..=max_attempts {
        tokio::time::sleep(poll_interval).await;

        match database_replicator::postgres::connect_with_retry(target_url).await {
            Ok(client) => {
                match client
                    .query_one("SHOW wal_level", &[])
                    .await
                    .map(|row| row.get::<_, String>(0))
                {
                    Ok(level) if level == "logical" => {
                        println!();
                        tracing::info!("✓ Endpoint is ready with wal_level=logical");
                        return Ok(());
                    }
                    Ok(level) => {
                        print!(
                            "\r⏳ Attempt {}/{}: wal_level={}, waiting...",
                            attempt, max_attempts, level
                        );
                        std::io::Write::flush(&mut std::io::stdout()).ok();
                    }
                    Err(_) => {
                        print!(
                            "\r⏳ Attempt {}/{}: checking wal_level...",
                            attempt, max_attempts
                        );
                        std::io::Write::flush(&mut std::io::stdout()).ok();
                    }
                }
            }
            Err(_) => {
                print!(
                    "\r⏳ Attempt {}/{}: endpoint restarting...",
                    attempt, max_attempts
                );
                std::io::Write::flush(&mut std::io::stdout()).ok();
            }
        }
    }

    println!();
    println!();
    println!("⚠️  Timed out waiting for wal_level to become 'logical'.");
    println!();
    println!("The SerenDB endpoint may need to be manually restarted:");
    println!("  1. Go to https://console.serendb.com");
    println!("  2. Navigate to your project's Compute endpoints");
    println!("  3. Click 'Restart' on the endpoint");
    println!("  4. Wait for the endpoint to become available");
    println!("  5. Re-run this command");
    println!();
    anyhow::bail!(
        "Endpoint wal_level is still 'replica' after enabling logical replication. \
         The endpoint may need to be manually restarted via the SerenDB console."
    )
}

#[allow(clippy::too_many_arguments)]
async fn init_remote(
    source: String,
    target: String,
    target_state: Option<database_replicator::serendb::TargetState>,
    _yes: bool,
    include_databases: Option<Vec<String>>,
    exclude_databases: Option<Vec<String>>,
    include_tables: Option<Vec<String>>,
    exclude_tables: Option<Vec<String>>,
    drop_existing: bool,
    no_sync: bool,
    seren_api: String,
    job_timeout: u64,
    log_level: String,
) -> anyhow::Result<()> {
    use database_replicator::migration;
    use database_replicator::postgres;
    use database_replicator::remote::{FilterSpec, JobSpec, RemoteClient};
    use std::collections::HashMap;

    println!("🌐 SerenAI cloud execution enabled");
    println!("API endpoint: {}", seren_api);

    // Get API key from interactive module (handles env var or prompt)
    let api_key = database_replicator::interactive::get_api_key()?;
    let remote_api_key = api_key.clone();

    // Extract SerenDB IDs either from saved state (API-key flow) or the target URL
    let (
        target_project_id,
        target_branch_id,
        target_databases,
        connection_string_mode,
        resolved_target_url,
    ) = if let Some(state) = target_state {
        let databases = state.databases;
        if databases.is_empty() {
            anyhow::bail!("Saved target is missing database entries");
        }
        (
            Some(state.project_id),
            Some(state.branch_id),
            Some(databases),
            SerenTargetMode::Project,
            Some(target.clone()), // Pass the connection string for remote API
        )
    } else if database_replicator::utils::is_serendb_target(&target) {
        let (p_id, b_id, _) = database_replicator::utils::parse_serendb_url_for_ids(&target)
            .context("Failed to parse SerenDB target URL for project, branch, and database IDs.")?;
        (
            Some(p_id),
            Some(b_id),
            None,
            SerenTargetMode::Url,
            Some(target.clone()),
        )
    } else {
        (None, None, None, SerenTargetMode::Url, Some(target.clone()))
    };

    // Estimate database size for automatic instance selection
    println!("Analyzing database size...");
    let filter_for_sizing = database_replicator::filters::ReplicationFilter::new(
        include_databases.clone(),
        exclude_databases.clone(),
        include_tables.clone(),
        exclude_tables.clone(),
    )?;

    let estimated_size_bytes = {
        let source_client = postgres::connect_with_retry(&source).await?;
        let all_databases = migration::list_databases(&source_client).await?;

        // Filter databases
        let databases: Vec<_> = all_databases
            .into_iter()
            .filter(|db| filter_for_sizing.should_replicate_database(&db.name))
            .collect();

        if databases.is_empty() {
            // No databases to replicate, use minimal size
            0i64
        } else {
            // Estimate total size
            let size_estimates = migration::estimate_database_sizes(
                &source,
                &source_client,
                &databases,
                &filter_for_sizing,
            )
            .await?;

            let total_bytes: i64 = size_estimates.iter().map(|s| s.size_bytes).sum();
            println!(
                "Total estimated size: {}",
                migration::format_bytes(total_bytes)
            );
            total_bytes
        }
    };

    // Build job specification
    let filter = if include_databases.is_none()
        && exclude_databases.is_none()
        && include_tables.is_none()
        && exclude_tables.is_none()
    {
        None
    } else {
        Some(FilterSpec {
            include_databases,
            exclude_databases,
            include_tables,
            exclude_tables,
        })
    };

    // Build options for remote execution (only include server-supported options)
    let mut options = HashMap::new();
    options.insert(
        "drop_existing".to_string(),
        serde_json::Value::Bool(drop_existing),
    );
    options.insert("enable_sync".to_string(), serde_json::Value::Bool(!no_sync));
    options.insert(
        "log_level".to_string(),
        serde_json::Value::String(log_level),
    );
    options.insert(
        "estimated_size_bytes".to_string(),
        serde_json::Value::Number(serde_json::Number::from(estimated_size_bytes)),
    );
    // Optional timeout hint for remote orchestrator
    options.insert(
        "job_timeout_seconds".to_string(),
        serde_json::Value::Number(serde_json::Number::from(job_timeout as i64)),
    );
    // Note: "yes" is client-side only, not sent to server

    let job_spec = match connection_string_mode {
        SerenTargetMode::Project => JobSpec {
            version: "1.0".to_string(),
            command: "init".to_string(),
            source_url: source,
            target_url: resolved_target_url.clone(),
            target_project_id,
            target_branch_id,
            target_databases,
            seren_api_key: Some(api_key.clone()),
            filter,
            options,
        },
        SerenTargetMode::Url => JobSpec {
            version: "1.0".to_string(),
            command: "init".to_string(),
            source_url: source,
            target_url: Some(
                resolved_target_url
                    .expect("Seren target URL must exist when using connection string mode"),
            ),
            target_project_id: None,
            target_branch_id: None,
            target_databases: None,
            seren_api_key: None,
            filter,
            options,
        },
    };

    // Submit job
    let client = RemoteClient::new(seren_api, Some(remote_api_key))?;
    println!("Submitting replication job...");
    tracing::debug!("Job spec: {:?}", job_spec);

    let response = client.submit_job(&job_spec).await?;
    println!("✓ Job submitted");
    println!("Job ID: {}", response.job_id);
    println!("\nPolling for status...");

    // Poll until complete
    let final_status = client
        .poll_until_complete(&response.job_id, |status| match status.status.as_str() {
            "provisioning" => println!("Status: provisioning EC2 instance..."),
            "running" => {
                if let Some(ref progress) = status.progress {
                    // Display detailed message if available
                    if let Some(ref message) = progress.message {
                        println!("{}", message);
                    } else {
                        println!(
                            "Status: running ({}/{}): {}",
                            progress.databases_completed,
                            progress.databases_total,
                            progress.current_database.as_deref().unwrap_or("unknown")
                        );
                    }
                } else {
                    println!("Status: running...");
                }
            }
            _ => {}
        })
        .await?;

    // Display result
    match final_status.status.as_str() {
        "completed" => {
            println!("\n✓ Replication completed successfully");
            Ok(())
        }
        "failed" => {
            let error_msg = final_status
                .error
                .unwrap_or_else(|| "Unknown error".to_string());
            println!("\n✗ Replication failed: {}", error_msg);
            anyhow::bail!("Replication failed");
        }
        _ => {
            anyhow::bail!("Unexpected final status: {}", final_status.status);
        }
    }
}

fn build_table_rules(
    args: &TableRuleArgs,
) -> anyhow::Result<database_replicator::table_rules::TableRules> {
    let mut rules = database_replicator::table_rules::TableRules::default();
    if let Some(path) = &args.config_path {
        let from_file = database_replicator::config::load_table_rules_from_file(path)?;
        rules.merge(from_file);
    }
    rules.apply_schema_only_cli(&args.schema_only_tables)?;
    rules.apply_table_filter_cli(&args.table_filters)?;
    rules.apply_time_filter_cli(&args.time_filters)?;
    Ok(rules)
}

/// Internal mode to track whether we're using project-based or URL-based target
enum SerenTargetMode {
    Project,
    Url,
}

/// Run xmin-based incremental sync between source and target databases
#[allow(clippy::too_many_arguments)]
async fn xmin_sync(
    source: String,
    target: String,
    schema: String,
    tables: Option<Vec<String>>,
    interval: u64,
    reconcile_interval: u64,
    batch_size: usize,
    state_file: Option<String>,
    once: bool,
    no_reconcile: bool,
) -> anyhow::Result<()> {
    use database_replicator::xmin::{DaemonConfig, SyncDaemon, SyncState};
    use std::path::PathBuf;
    use std::time::Duration;

    tracing::info!("Starting xmin-based sync...");
    tracing::info!(
        "Source: {}",
        database_replicator::utils::strip_password_from_url(&source)
            .unwrap_or_else(|_| source.clone())
    );
    tracing::info!(
        "Target: {}",
        database_replicator::utils::strip_password_from_url(&target)
            .unwrap_or_else(|_| target.clone())
    );
    tracing::info!("Schema: {}", schema);
    if let Some(ref t) = tables {
        tracing::info!("Tables: {}", t.join(", "));
    } else {
        tracing::info!("Tables: all");
    }

    // CRITICAL: Ensure source and target are different to prevent data loss
    database_replicator::utils::validate_source_target_different(&source, &target)
        .context("Source and target validation failed")?;
    tracing::info!("Verified source and target are different databases");

    // Pre-flight check: verify target database has tables to sync
    // This prevents confusing errors like "relation does not exist"
    // If target is empty, auto-switch to matching source database name (init preserves names)
    let target = {
        let target_client = database_replicator::postgres::connect(&target)
            .await
            .context("Failed to connect to target database")?;

        let target_tables = database_replicator::migration::list_tables(&target_client).await?;

        if target_tables.is_empty() {
            // Target database has no tables - likely user specified wrong database
            let target_parts = database_replicator::utils::parse_postgres_url(&target)?;
            let target_db_name = &target_parts.database;

            // Get source database name - init preserves source names on target
            let source_parts = database_replicator::utils::parse_postgres_url(&source)?;
            let source_db_name = &source_parts.database;

            // Connect to postgres database to check if source-named database exists on target
            let server_url =
                database_replicator::commands::sync::replace_database_in_url(&target, "postgres")?;
            let server_client = database_replicator::postgres::connect(&server_url)
                .await
                .context("Failed to connect to target server")?;

            let available_dbs = database_replicator::migration::list_databases(&server_client)
                .await
                .unwrap_or_default();

            // Look for a database on target that matches source database name
            let matching_db = available_dbs.iter().find(|db| db.name == *source_db_name);

            if let Some(db) = matching_db {
                // Found matching database - check if it has tables
                let db_url = database_replicator::commands::sync::replace_database_in_url(
                    &target, &db.name,
                )?;
                let table_count =
                    if let Ok(db_client) = database_replicator::postgres::connect(&db_url).await {
                        database_replicator::migration::list_tables(&db_client)
                            .await
                            .map(|t| t.len())
                            .unwrap_or(0)
                    } else {
                        0
                    };

                if table_count > 0 {
                    // Auto-switch to the matching database
                    println!();
                    println!("========================================");
                    println!("⚠️  Target database '{}' has no tables", target_db_name);
                    println!("========================================");
                    println!();
                    println!(
                        "Found matching database '{}' with {} tables.",
                        source_db_name, table_count
                    );
                    println!("(init preserves source database names on target)");
                    println!();
                    println!("✓ Automatically switching to '{}'", source_db_name);
                    tracing::info!(
                        "Auto-switched target from '{}' to '{}' (source name match)",
                        target_db_name,
                        source_db_name
                    );

                    database_replicator::commands::sync::replace_database_in_url(
                        &target,
                        source_db_name,
                    )?
                } else {
                    // Matching database exists but is empty
                    anyhow::bail!(
                        "Target database '{}' has no tables, and the matching source database \
                         '{}' on target is also empty.\n\n\
                         Did you run 'init' first to copy data from source to target?",
                        target_db_name,
                        source_db_name
                    );
                }
            } else {
                // No matching database found - user must run init first
                anyhow::bail!(
                    "Database '{}' does not exist on target server.\n\n\
                     Sync requires the target database name to match the source.\n\
                     Run 'init' first to copy the source database to target.",
                    source_db_name
                );
            }
        } else {
            target
        }
    };

    // Build daemon config
    let state_path = state_file
        .map(PathBuf::from)
        .unwrap_or_else(SyncState::default_path);

    let reconcile_interval_duration = if no_reconcile {
        None
    } else {
        Some(Duration::from_secs(reconcile_interval))
    };

    let config = DaemonConfig {
        sync_interval: Duration::from_secs(interval),
        reconcile_interval: reconcile_interval_duration,
        state_path,
        batch_size,
        tables: tables.unwrap_or_default(),
        schema,
    };

    tracing::info!("Sync interval: {}s", interval);
    if let Some(ref ri) = config.reconcile_interval {
        tracing::info!("Reconcile interval: {}s", ri.as_secs());
    } else {
        tracing::info!("Reconciliation disabled");
    }
    tracing::info!("Batch size: {}", batch_size);
    tracing::info!("State file: {:?}", config.state_path);

    // Create the daemon
    let daemon = SyncDaemon::new(source.clone(), target.clone(), config);

    if once {
        // Run a single sync cycle
        tracing::info!("Running single sync cycle...");

        let stats = daemon.run_sync_cycle().await?;

        tracing::info!("Sync cycle complete:");
        tracing::info!("  Tables synced: {}", stats.tables_synced);
        tracing::info!("  Rows synced: {}", stats.rows_synced);
        if !stats.errors.is_empty() {
            tracing::warn!("  Errors: {}", stats.errors.len());
            for err in &stats.errors {
                tracing::warn!("    - {}", err);
            }
        }

        println!();
        println!("========================================");
        println!("Xmin sync cycle complete");
        println!("========================================");
        println!("  Tables synced: {}", stats.tables_synced);
        println!("  Rows synced: {}", stats.rows_synced);
        if !stats.errors.is_empty() {
            println!("  Errors: {}", stats.errors.len());
        }
    } else {
        // Run continuous sync
        tracing::info!("Starting continuous sync daemon...");
        tracing::info!("Press Ctrl+C to stop");

        println!();
        println!("========================================");
        println!("Starting xmin-based continuous sync");
        println!("========================================");
        println!("  Sync interval: {}s", interval);
        println!("  Press Ctrl+C to stop");
        println!();

        // Create shutdown channel
        let (shutdown_tx, shutdown_rx) = tokio::sync::broadcast::channel::<()>(1);

        // Set up Ctrl+C handler
        let shutdown_tx_clone = shutdown_tx.clone();
        tokio::spawn(async move {
            tokio::signal::ctrl_c()
                .await
                .expect("Failed to listen for Ctrl+C");
            tracing::info!("Received shutdown signal");
            let _ = shutdown_tx_clone.send(());
        });

        daemon.run(shutdown_rx).await?;

        // Clean up daemon PID file on graceful shutdown
        if let Err(e) = database_replicator::daemon::cleanup() {
            tracing::warn!("Failed to clean up daemon PID file: {}", e);
        }
    }

    Ok(())
}
