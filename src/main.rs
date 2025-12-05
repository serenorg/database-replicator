// ABOUTME: CLI entry point for database-replicator
// ABOUTME: Parses commands and routes to appropriate handlers

use anyhow::Context;
use clap::{Args, Parser, Subcommand};
use database_replicator::commands;

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
    /// Set up continuous logical replication from source to target
    Sync {
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
        #[command(flatten)]
        table_rules: TableRuleArgs,
        /// Force recreate subscriptions even if they already exist
        #[arg(long)]
        force: bool,
        /// SerenDB project ID (for auto-enabling logical replication)
        #[arg(long)]
        project_id: Option<String>,
        /// SerenDB Console API URL (defaults to https://console.serendb.com)
        #[arg(long, default_value = "https://console.serendb.com")]
        console_api: String,
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

            let filter = if !no_interactive {
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

            if seren {
                if let Some(t) = &target {
                    if !database_replicator::utils::is_serendb_target(t) {
                        anyhow::bail!("--seren flag is only compatible with SerenDB targets.");
                    }
                } else {
                    target = Some(database_replicator::interactive::select_seren_database().await?);
                }
            }

            let target = target.ok_or_else(|| {
                anyhow::anyhow!("Target database URL not provided and not set in state. Use `--target` or `database-replicator target set`.")
            })?;

            // Check if CLI filter flags were provided (skip interactive if so)
            let has_cli_filters = include_databases.is_some()
                || exclude_databases.is_some()
                || include_tables.is_some()
                || exclude_tables.is_some();

            // Interactive mode is default unless:
            // - --no-interactive flag is set
            // - --yes flag is set (implies automation)
            // - CLI filter flags are provided
            // Run this BEFORE remote execution check so interactive mode works for both local and remote
            let (
                final_include_databases,
                final_exclude_databases,
                final_include_tables,
                final_exclude_tables,
            ) = if !no_interactive && !yes && !has_cli_filters {
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
            // 3. Neither → auto-detect based on target URL (SerenDB = remote)
            let use_remote = if seren {
                true
            } else if local {
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
                    None,
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
        } => {
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

            let filter = if !no_interactive {
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

            // If project_id is provided and target is SerenDB, check/enable logical replication
            if let Some(ref project_id) = project_id {
                if database_replicator::utils::is_serendb_target(&resolved_target) {
                    check_and_enable_logical_replication(project_id, &console_api).await?;
                }
            }

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
        Commands::Target { args } => commands::target(args).await,
    }
}

/// Check if logical replication is enabled on SerenDB project and offer to enable it
async fn check_and_enable_logical_replication(
    project_id: &str,
    console_api: &str,
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
        println!("⏳ Waiting for endpoints to restart (this may take up to 30 seconds)...");

        // Wait for endpoints to restart - the wal_level change requires endpoint restart
        tokio::time::sleep(tokio::time::Duration::from_secs(15)).await;

        tracing::info!("✓ Endpoints should now be ready with wal_level=logical");
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
            None,
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
            target_url: None,
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
