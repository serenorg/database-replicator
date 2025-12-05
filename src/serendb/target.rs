// ABOUTME: Persists SerenDB target selection for reuse across commands
// ABOUTME: Stores project/branch/database selection in .seren-replicator/target.json

use anyhow::{Context, Result};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::path::PathBuf;

const TARGET_FILE: &str = ".seren-replicator/target.json";
const TARGET_FILE_ENV: &str = "SEREN_TARGET_STATE_PATH";
const STATE_VERSION: u32 = 1;

/// Persisted target selection state
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TargetState {
    /// Schema version for forward compatibility
    pub version: u32,
    /// Selected SerenDB project ID
    pub project_id: String,
    /// Human-readable project name
    pub project_name: String,
    /// Selected branch ID
    pub branch_id: String,
    /// Branch name
    pub branch_name: String,
    /// List of database names being replicated
    pub databases: Vec<String>,
    /// SHA256 hash of source URL (to detect mismatches)
    pub source_url_hash: String,
    /// When this target was configured
    pub created_at: String,
}

impl TargetState {
    /// Create a new target state snapshot
    pub fn new(
        project_id: String,
        project_name: String,
        branch_id: String,
        branch_name: String,
        databases: Vec<String>,
        source_url: &str,
    ) -> Self {
        Self {
            version: STATE_VERSION,
            project_id,
            project_name,
            branch_id,
            branch_name,
            databases,
            source_url_hash: hash_url(source_url),
            created_at: Utc::now().to_rfc3339(),
        }
    }

    /// Check if a source URL matches the stored configuration
    pub fn source_matches(&self, source_url: &str) -> bool {
        self.source_url_hash == hash_url(source_url)
    }
}

/// Hash a URL for comparison (strips password for privacy)
fn hash_url(url: &str) -> String {
    let sanitized = crate::utils::strip_password_from_url(url).unwrap_or_else(|_| url.to_string());
    let mut hasher = Sha256::new();
    hasher.update(sanitized.as_bytes());
    format!("sha256:{:x}", hasher.finalize())
}

/// Get the path to the target state file, allowing an env override for tests
fn target_file_path() -> PathBuf {
    if let Ok(custom) = std::env::var(TARGET_FILE_ENV) {
        return PathBuf::from(custom);
    }
    PathBuf::from(TARGET_FILE)
}

/// Load target state from disk. Returns Ok(None) if the file does not exist.
pub fn load_target_state() -> Result<Option<TargetState>> {
    let path = target_file_path();

    if !path.exists() {
        return Ok(None);
    }

    let content = std::fs::read_to_string(&path)
        .with_context(|| format!("Failed to read {}", path.display()))?;

    let state: TargetState = serde_json::from_str(&content).with_context(|| {
        format!(
            "Failed to parse {}. Delete it and run init again.",
            path.display()
        )
    })?;

    if state.version > STATE_VERSION {
        anyhow::bail!(
            "Target state file was created by a newer database-replicator version. \
             Upgrade this CLI or delete {}",
            path.display()
        );
    }

    Ok(Some(state))
}

/// Save target state to disk
pub fn save_target_state(state: &TargetState) -> Result<()> {
    let path = target_file_path();

    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("Failed to create directory {}", parent.display()))?;
    }

    let content =
        serde_json::to_string_pretty(state).context("Failed to serialize target state")?;

    std::fs::write(&path, content)
        .with_context(|| format!("Failed to write {}", path.display()))?;

    tracing::info!("Saved SerenDB target configuration to {}", path.display());
    Ok(())
}

/// Delete persisted target state (if present)
pub fn clear_target_state() -> Result<()> {
    let path = target_file_path();
    if path.exists() {
        std::fs::remove_file(&path)
            .with_context(|| format!("Failed to remove {}", path.display()))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn with_temp_state_path<F: FnOnce()>(func: F) {
        let _guard = crate::serendb::target_env_mutex().lock().unwrap();
        let dir = tempdir().expect("tempdir");
        let file_path = dir.path().join("target.json");
        std::env::set_var(TARGET_FILE_ENV, &file_path);
        func();
        std::env::remove_var(TARGET_FILE_ENV);
    }

    #[test]
    fn test_target_state_roundtrip() {
        with_temp_state_path(|| {
            let state = TargetState::new(
                "proj-123".to_string(),
                "my-project".to_string(),
                "branch-456".to_string(),
                "main".to_string(),
                vec!["db1".to_string(), "db2".to_string()],
                "postgresql://localhost/source",
            );

            save_target_state(&state).expect("save target state");
            let loaded = load_target_state()
                .expect("load state")
                .expect("state present");

            assert_eq!(loaded.project_id, "proj-123");
            assert_eq!(loaded.databases.len(), 2);
            assert!(loaded.source_matches("postgresql://localhost/source"));
        });
    }

    #[test]
    fn test_source_url_matching() {
        let state = TargetState::new(
            "p".to_string(),
            "proj".to_string(),
            "b".to_string(),
            "main".to_string(),
            vec![],
            "postgresql://user:pass@host/db",
        );

        assert!(state.source_matches("postgresql://user:pass@host/db"));
        assert!(state.source_matches("postgresql://user:other@host/db"));
        assert!(!state.source_matches("postgresql://user:pass@other/db"));
    }

    #[test]
    fn test_hash_url_strips_password() {
        let hash1 = hash_url("postgresql://user:secret1@host/db");
        let hash2 = hash_url("postgresql://user:secret2@host/db");
        assert_eq!(hash1, hash2);
    }
}
