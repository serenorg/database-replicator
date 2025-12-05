use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::PathBuf;

#[derive(Serialize, Deserialize, Default)]
pub struct AppState {
    pub target_url: Option<String>,
}

fn get_state_path() -> Result<PathBuf> {
    let home_dir =
        dirs::home_dir().ok_or_else(|| anyhow::anyhow!("Could not find home directory"))?;
    let state_dir = home_dir.join(".database-replicator");
    if !state_dir.exists() {
        fs::create_dir_all(&state_dir)?;
    }
    Ok(state_dir.join("state.json"))
}

pub fn load() -> Result<AppState> {
    let state_path = get_state_path()?;
    if !state_path.exists() {
        return Ok(AppState::default());
    }
    let state_file = fs::File::open(state_path)?;
    let state = serde_json::from_reader(state_file)?;
    Ok(state)
}

pub fn save(state: &AppState) -> Result<()> {
    let state_path = get_state_path()?;
    let state_file = fs::File::create(state_path)?;
    serde_json::to_writer_pretty(state_file, state)?;
    Ok(())
}
