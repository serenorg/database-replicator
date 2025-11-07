// ABOUTME: CLI entry point for postgres-seren-replicator
// ABOUTME: Parses commands and routes to appropriate handlers

use clap::{Parser, Subcommand};
use postgres_seren_replicator::commands;

#[derive(Parser)]
#[command(name = "postgres-seren-replicator")]
#[command(about = "Zero-downtime PostgreSQL replication to Seren Cloud", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
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
        /// Interactive mode for selecting databases/tables
        #[arg(long)]
        interactive: bool,
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
        /// Interactive mode for selecting databases/tables
        #[arg(long)]
        interactive: bool,
        /// Drop existing databases on target before copying
        #[arg(long)]
        drop_existing: bool,
        /// Disable automatic continuous replication setup after snapshot (default: false, meaning sync IS enabled)
        #[arg(long)]
        no_sync: bool,
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
        /// Interactive mode for selecting databases/tables
        #[arg(long)]
        interactive: bool,
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
    // Initialize logging - default to INFO level if RUST_LOG not set
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    // Clean up stale temp directories from previous runs (older than 24 hours)
    // This handles temp files left behind by processes killed with SIGKILL
    if let Err(e) = postgres_seren_replicator::utils::cleanup_stale_temp_dirs(86400) {
        tracing::warn!("Failed to clean up stale temp directories: {}", e);
        // Don't fail startup if cleanup fails
    }

    let cli = Cli::parse();

    match cli.command {
        Commands::Validate {
            source,
            target,
            include_databases,
            exclude_databases,
            include_tables,
            exclude_tables,
            interactive,
        } => {
            let filter = if interactive {
                // Interactive mode - prompt user to select databases and tables
                postgres_seren_replicator::interactive::select_databases_and_tables(&source).await?
            } else {
                // CLI mode - use provided filter arguments
                postgres_seren_replicator::filters::ReplicationFilter::new(
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
            interactive,
            drop_existing,
            no_sync,
        } => {
            let filter = if interactive {
                // Interactive mode - prompt user to select databases and tables
                postgres_seren_replicator::interactive::select_databases_and_tables(&source).await?
            } else {
                // CLI mode - use provided filter arguments
                postgres_seren_replicator::filters::ReplicationFilter::new(
                    include_databases,
                    exclude_databases,
                    include_tables,
                    exclude_tables,
                )?
            };
            let enable_sync = !no_sync; // Invert the flag: by default sync is enabled
            commands::init(&source, &target, yes, filter, drop_existing, enable_sync).await
        }
        Commands::Sync {
            source,
            target,
            include_databases,
            exclude_databases,
            include_tables,
            exclude_tables,
            interactive,
            force,
        } => {
            let filter = if interactive {
                // Interactive mode - prompt user to select databases and tables
                postgres_seren_replicator::interactive::select_databases_and_tables(&source).await?
            } else {
                // CLI mode - use provided filter arguments
                postgres_seren_replicator::filters::ReplicationFilter::new(
                    include_databases,
                    exclude_databases,
                    include_tables,
                    exclude_tables,
                )?
            };
            commands::sync(&source, &target, Some(filter), None, None, None, force).await
        }
        Commands::Status {
            source,
            target,
            include_databases,
            exclude_databases,
        } => {
            let filter = postgres_seren_replicator::filters::ReplicationFilter::new(
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
            let filter = postgres_seren_replicator::filters::ReplicationFilter::new(
                include_databases,
                exclude_databases,
                include_tables,
                exclude_tables,
            )?;
            commands::verify(&source, &target, Some(filter)).await
        }
    }
}
