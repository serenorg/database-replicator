// ABOUTME: CLI entry point for seren-neon-migrator
// ABOUTME: Parses commands and routes to appropriate handlers

use clap::{Parser, Subcommand};
use seren_neon_migrator::commands;

#[derive(Parser)]
#[command(name = "seren-neon-migrator")]
#[command(about = "Zero-downtime migration from Neon to Seren", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Validate source and target databases are ready for migration
    Validate {
        #[arg(long)]
        source: String,
        #[arg(long)]
        target: String,
    },
    /// Initialize migration with schema and data copy
    Init {
        #[arg(long)]
        source: String,
        #[arg(long)]
        target: String,
    },
    /// Set up logical replication from source to target
    Sync {
        #[arg(long)]
        source: String,
        #[arg(long)]
        target: String,
    },
    /// Check replication status and lag
    Status {
        #[arg(long)]
        source: String,
        #[arg(long)]
        target: String,
    },
    /// Verify data integrity between source and target
    Verify {
        #[arg(long)]
        source: String,
        #[arg(long)]
        target: String,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize logging
    tracing_subscriber::fmt::init();

    let cli = Cli::parse();

    match cli.command {
        Commands::Validate { source, target } => {
            commands::validate(&source, &target).await
        }
        Commands::Init { source, target } => {
            commands::init(&source, &target).await
        }
        Commands::Sync { source, target } => {
            commands::sync(&source, &target, None, None, None).await
        }
        Commands::Status { source, target } => {
            println!("Checking replication status from {} to {}", source, target);
            // TODO: Implement
            Ok(())
        }
        Commands::Verify { source, target } => {
            println!("Verifying data integrity from {} to {}", source, target);
            // TODO: Implement
            Ok(())
        }
    }
}
