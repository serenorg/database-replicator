// ABOUTME: CLI entry point for database-replicator
// ABOUTME: Parses commands and routes to appropriate handlers

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
        target: String,
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
        target: String,
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
        target: String,
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
    },
    /// Check replication status and lag in real-time
    Status {
        #[arg(long)]
        source: String,
        #[arg(long)]
        target: String,
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
        target: String,
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
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // We need to parse CLI args early to get the log level
    let cli = Cli::parse();

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
                return init_remote(
                    source,
                    target,
                    yes,
                    final_include_databases,
                    final_exclude_databases,
                    final_include_tables,
                    final_exclude_tables,
                    drop_existing,
                    no_sync,
                    seren_api,
                    job_timeout,
                )
                .await;
            }

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
                Ok(_) => Ok(()),
                Err(e) if e.to_string().contains("PREFLIGHT_FALLBACK_TO_REMOTE") => {
                    // Auto-fallback to remote execution
                    init_remote(
                        source,
                        target,
                        yes,
                        fallback_include_dbs,
                        fallback_exclude_dbs,
                        fallback_include_tables,
                        fallback_exclude_tables,
                        drop_existing,
                        no_sync,
                        seren_api,
                        job_timeout,
                    )
                    .await
                }
                Err(e) => Err(e),
            }
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
        } => {
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
            commands::sync(&source, &target, Some(filter), None, None, None, force).await
        }
        Commands::Status {
            source,
            target,
            include_databases,
            exclude_databases,
        } => {
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
            let filter = database_replicator::filters::ReplicationFilter::new(
                include_databases,
                exclude_databases,
                include_tables,
                exclude_tables,
            )?;
            commands::verify(&source, &target, Some(filter)).await
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn get_api_key() -> anyhow::Result<String> {
    use dialoguer::{theme::ColorfulTheme, Input};

    // Try environment variable first
    if let Ok(key) = std::env::var("SEREN_API_KEY") {
        if !key.trim().is_empty() {
            return Ok(key.trim().to_string());
        }
    }

    // Prompt user interactively
    println!("\nRemote execution requires a SerenDB API key for authentication.");
    println!("\nYou can generate an API key at:");
    println!("  https://console.serendb.com/api-keys\n");

    let key: String = Input::with_theme(&ColorfulTheme::default())
        .with_prompt("Enter your SerenDB API key")
        .allow_empty(false)
        .interact_text()?;

    if key.trim().is_empty() {
        anyhow::bail!(
            "API key is required for remote execution.\n\
            Set the SEREN_API_KEY environment variable or run interactively.\n\
            Get your API key at: https://console.serendb.com/api-keys\n\
            Or use --local to run replication on your machine instead"
        );
    }

    Ok(key.trim().to_string())
}

#[allow(clippy::too_many_arguments)]
async fn init_remote(
    source: String,
    target: String,
    _yes: bool,
    include_databases: Option<Vec<String>>,
    exclude_databases: Option<Vec<String>>,
    include_tables: Option<Vec<String>>,
    exclude_tables: Option<Vec<String>>,
    drop_existing: bool,
    no_sync: bool,
    seren_api: String,
    job_timeout: u64,
) -> anyhow::Result<()> {
    use database_replicator::migration;
    use database_replicator::postgres;
    use database_replicator::remote::{FilterSpec, JobSpec, RemoteClient};
    use std::collections::HashMap;

    println!("🌐 SerenAI cloud execution enabled");
    println!("API endpoint: {}", seren_api);

    // Get API key (from env or prompt user)
    let api_key = get_api_key()?;

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
        "estimated_size_bytes".to_string(),
        serde_json::Value::Number(serde_json::Number::from(estimated_size_bytes)),
    );
    // Optional timeout hint for remote orchestrator
    options.insert(
        "job_timeout_seconds".to_string(),
        serde_json::Value::Number(serde_json::Number::from(job_timeout as i64)),
    );
    // Note: "yes" is client-side only, not sent to server

    let job_spec = JobSpec {
        version: "1.0".to_string(),
        command: "init".to_string(),
        source_url: source,
        target_url: target,
        filter,
        options,
    };

    // Submit job
    let client = RemoteClient::new(seren_api, Some(api_key))?;
    println!("Submitting replication job...");

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
