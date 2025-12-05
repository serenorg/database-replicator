use anyhow::{Context, Result};
use clap::{Args, Subcommand};

use crate::state;

#[derive(Args)]
pub struct TargetArgs {
    #[command(subcommand)]
    command: TargetCommands,
}

#[derive(Subcommand)]
enum TargetCommands {
    /// Set the target database URL
    Set {
        /// The PostgreSQL URL to set as the target
        url: String,
    },
    /// Unset the target database URL
    Unset,
    /// Show the current target database URL
    Get,
}

pub async fn command(args: TargetArgs) -> Result<()> {
    match args.command {
        TargetCommands::Set { url } => {
            let mut state = state::load().context("Failed to load state")?;
            state.target_url = Some(url.clone());
            state::save(&state).context("Failed to save state")?;
            println!("Target database URL set to: {}", url);
        }
        TargetCommands::Unset => {
            let mut state = state::load().context("Failed to load state")?;
            state.target_url = None;
            state::save(&state).context("Failed to save state")?;
            println!("Target database URL unset.");
        }
        TargetCommands::Get => {
            let state = state::load().context("Failed to load state")?;
            match state.target_url {
                Some(url) => println!("Current target database URL: {}", url),
                None => println!("Target database URL is not set."),
            }
        }
    }
    Ok(())
}
