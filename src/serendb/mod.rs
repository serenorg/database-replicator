// ABOUTME: SerenDB Console API client for managing project settings
// ABOUTME: Enables checking and enabling logical replication on SerenDB projects

mod client;
mod picker;
mod target;

pub use client::{Branch, ConsoleClient, Database, Project};
pub use picker::{create_missing_databases, select_target, TargetSelection};
pub use target::{clear_target_state, load_target_state, save_target_state, TargetState};

use anyhow::Result;

#[cfg(test)]
pub(crate) fn target_env_mutex() -> &'static std::sync::Mutex<()> {
    use std::sync::{Mutex, OnceLock};
    static ENV_MUTEX: OnceLock<Mutex<()>> = OnceLock::new();
    ENV_MUTEX.get_or_init(|| Mutex::new(()))
}

/// How the target database is specified
#[derive(Debug, Clone)]
pub enum TargetMode {
    /// User provided --target connection string directly
    ConnectionString(String),
    /// User provided API key, will use interactive selection
    ApiKey(String),
    /// Using saved target from previous init
    SavedState(TargetState),
}

/// Resolve which target mode to use based on CLI args and environment
pub fn resolve_target_mode(target: Option<String>, api_key: Option<String>) -> Result<TargetMode> {
    match (target, api_key) {
        (Some(url), _) => Ok(TargetMode::ConnectionString(url)),
        (None, Some(key)) => {
            if let Some(state) = load_target_state()? {
                tracing::info!(
                    "Using saved target configuration: {}/{}",
                    state.project_name,
                    state.branch_name
                );
                Ok(TargetMode::SavedState(state))
            } else {
                Ok(TargetMode::ApiKey(key))
            }
        }
        (None, None) => {
            anyhow::bail!(
                "Target database required.\n\n\
                 Option 1: Provide --target with a PostgreSQL connection string\n\
                 Option 2: Set SEREN_API_KEY or pass --api-key for interactive SerenDB selection\n\n\
                 Get your API key at: https://console.serendb.com/api-keys"
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::serendb::target::{clear_target_state, save_target_state, TargetState};
    use tempfile::tempdir;

    fn with_temp_state_path<F: FnOnce()>(func: F) {
        let _guard = crate::serendb::target_env_mutex().lock().unwrap();
        let dir = tempdir().expect("tempdir");
        let path = dir.path().join("target.json");
        std::env::set_var("SEREN_TARGET_STATE_PATH", &path);
        func();
        std::env::remove_var("SEREN_TARGET_STATE_PATH");
    }

    #[test]
    fn test_resolve_target_mode_connection_string() {
        let mode =
            resolve_target_mode(Some("postgresql://localhost/db".to_string()), None).unwrap();
        match mode {
            TargetMode::ConnectionString(url) => assert!(url.contains("localhost")),
            _ => panic!("Expected ConnectionString mode"),
        }
    }

    #[test]
    fn test_resolve_target_mode_prefers_explicit_target() {
        let mode = resolve_target_mode(
            Some("postgresql://localhost/db".to_string()),
            Some("seren_key".to_string()),
        )
        .unwrap();

        if !matches!(mode, TargetMode::ConnectionString(_)) {
            panic!("Expected ConnectionString mode");
        }
    }

    #[test]
    fn test_resolve_target_mode_uses_saved_state() {
        with_temp_state_path(|| {
            let state = TargetState::new(
                "proj".into(),
                "Project".into(),
                "branch".into(),
                "main".into(),
                vec!["db1".into()],
                "postgresql://localhost/source",
            );
            save_target_state(&state).expect("save state");

            let mode = resolve_target_mode(None, Some("seren_key".into())).unwrap();
            match mode {
                TargetMode::SavedState(saved) => assert_eq!(saved.project_id, "proj"),
                _ => panic!("Expected SavedState mode"),
            }

            clear_target_state().expect("clear state");
        });
    }

    #[test]
    fn test_resolve_target_mode_neither_fails() {
        let result = resolve_target_mode(None, None);
        assert!(result.is_err());
    }
}
