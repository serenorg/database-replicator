// ABOUTME: SyncState for xmin-based sync - tracks sync progress per table
// ABOUTME: Persists high-water mark xmin values to enable incremental syncs

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;
use tokio::fs;

/// Sync state for a single table, tracking the last synced xmin value.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableSyncState {
    /// Schema name (e.g., "public")
    pub schema: String,
    /// Table name
    pub table: String,
    /// Last successfully synced xmin value (high-water mark)
    /// Rows with xmin > this value need to be synced
    pub last_xmin: u32,
    /// Timestamp of last successful sync
    pub last_sync_at: chrono::DateTime<chrono::Utc>,
    /// Number of rows synced in last batch
    pub last_row_count: u64,
}

impl TableSyncState {
    /// Create a new TableSyncState with initial xmin of 0 (sync everything)
    pub fn new(schema: &str, table: &str) -> Self {
        Self {
            schema: schema.to_string(),
            table: table.to_string(),
            last_xmin: 0,
            last_sync_at: chrono::Utc::now(),
            last_row_count: 0,
        }
    }

    /// Update state after a successful sync
    pub fn update(&mut self, new_xmin: u32, row_count: u64) {
        self.last_xmin = new_xmin;
        self.last_sync_at = chrono::Utc::now();
        self.last_row_count = row_count;
    }

    /// Get the qualified table name (schema.table)
    pub fn qualified_name(&self) -> String {
        format!("{}.{}", self.schema, self.table)
    }
}

/// Overall sync state for a database, containing state for all tracked tables.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncState {
    /// Source database URL (sanitized - no password)
    pub source_url: String,
    /// Target database URL (sanitized - no password)
    pub target_url: String,
    /// Per-table sync states, keyed by "schema.table"
    pub tables: HashMap<String, TableSyncState>,
    /// Version of the state format for future migrations
    pub version: u32,
    /// When this state was created
    pub created_at: chrono::DateTime<chrono::Utc>,
    /// When this state was last modified
    pub updated_at: chrono::DateTime<chrono::Utc>,
}

impl SyncState {
    /// Create a new empty SyncState
    pub fn new(source_url: &str, target_url: &str) -> Self {
        let now = chrono::Utc::now();
        Self {
            source_url: sanitize_url(source_url),
            target_url: sanitize_url(target_url),
            tables: HashMap::new(),
            version: 1,
            created_at: now,
            updated_at: now,
        }
    }

    /// Get or create state for a table
    pub fn get_or_create_table(&mut self, schema: &str, table: &str) -> &mut TableSyncState {
        let key = format!("{}.{}", schema, table);
        self.tables
            .entry(key)
            .or_insert_with(|| TableSyncState::new(schema, table))
    }

    /// Get state for a table if it exists
    pub fn get_table(&self, schema: &str, table: &str) -> Option<&TableSyncState> {
        let key = format!("{}.{}", schema, table);
        self.tables.get(&key)
    }

    /// Update state for a table after successful sync
    pub fn update_table(&mut self, schema: &str, table: &str, new_xmin: u32, row_count: u64) {
        let state = self.get_or_create_table(schema, table);
        state.update(new_xmin, row_count);
        self.updated_at = chrono::Utc::now();
    }

    /// Remove state for a table (e.g., if table was dropped)
    pub fn remove_table(&mut self, schema: &str, table: &str) -> Option<TableSyncState> {
        let key = format!("{}.{}", schema, table);
        let removed = self.tables.remove(&key);
        if removed.is_some() {
            self.updated_at = chrono::Utc::now();
        }
        removed
    }

    /// Get all table names being tracked
    pub fn tracked_tables(&self) -> Vec<&str> {
        self.tables.keys().map(|s| s.as_str()).collect()
    }

    /// Load state from a JSON file
    pub async fn load(path: &Path) -> Result<Self> {
        let contents = fs::read_to_string(path)
            .await
            .with_context(|| format!("Failed to read sync state from {:?}", path))?;
        let state: SyncState = serde_json::from_str(&contents)
            .with_context(|| format!("Failed to parse sync state from {:?}", path))?;
        Ok(state)
    }

    /// Save state to a JSON file
    pub async fn save(&self, path: &Path) -> Result<()> {
        // Ensure parent directory exists
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .await
                .with_context(|| format!("Failed to create directory {:?}", parent))?;
        }

        let contents =
            serde_json::to_string_pretty(self).context("Failed to serialize sync state")?;
        fs::write(path, contents)
            .await
            .with_context(|| format!("Failed to write sync state to {:?}", path))?;
        Ok(())
    }

    /// Get the default state file path for the current directory
    pub fn default_path() -> std::path::PathBuf {
        std::path::PathBuf::from(".seren-replicator/xmin-sync-state.json")
    }
}

/// Sanitize a database URL by removing the password component
fn sanitize_url(url: &str) -> String {
    // Try to parse as URL and remove password
    if let Ok(mut parsed) = url::Url::parse(url) {
        if parsed.password().is_some() {
            let _ = parsed.set_password(Some("***"));
        }
        parsed.to_string()
    } else {
        // If not a valid URL, return as-is (might be a file path for SQLite)
        url.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_table_sync_state_new() {
        let state = TableSyncState::new("public", "users");
        assert_eq!(state.schema, "public");
        assert_eq!(state.table, "users");
        assert_eq!(state.last_xmin, 0);
        assert_eq!(state.last_row_count, 0);
    }

    #[test]
    fn test_table_sync_state_update() {
        let mut state = TableSyncState::new("public", "users");
        state.update(12345, 100);
        assert_eq!(state.last_xmin, 12345);
        assert_eq!(state.last_row_count, 100);
    }

    #[test]
    fn test_table_sync_state_qualified_name() {
        let state = TableSyncState::new("myschema", "mytable");
        assert_eq!(state.qualified_name(), "myschema.mytable");
    }

    #[test]
    fn test_sync_state_new() {
        let state = SyncState::new(
            "postgresql://user:pass@localhost/db",
            "postgresql://user:pass@remote/db",
        );
        assert!(state.tables.is_empty());
        assert_eq!(state.version, 1);
        // Passwords should be sanitized
        assert!(state.source_url.contains("***"));
        assert!(state.target_url.contains("***"));
    }

    #[test]
    fn test_sync_state_get_or_create() {
        let mut state = SyncState::new("source", "target");

        // First call creates
        let table_state = state.get_or_create_table("public", "users");
        assert_eq!(table_state.last_xmin, 0);

        // Update it
        table_state.update(100, 50);

        // Second call retrieves existing
        let table_state = state.get_or_create_table("public", "users");
        assert_eq!(table_state.last_xmin, 100);
    }

    #[test]
    fn test_sync_state_update_table() {
        let mut state = SyncState::new("source", "target");
        state.update_table("public", "users", 500, 200);

        let table_state = state.get_table("public", "users").unwrap();
        assert_eq!(table_state.last_xmin, 500);
        assert_eq!(table_state.last_row_count, 200);
    }

    #[test]
    fn test_sync_state_remove_table() {
        let mut state = SyncState::new("source", "target");
        state.update_table("public", "users", 100, 10);

        let removed = state.remove_table("public", "users");
        assert!(removed.is_some());
        assert!(state.get_table("public", "users").is_none());
    }

    #[test]
    fn test_sanitize_url() {
        assert_eq!(
            sanitize_url("postgresql://user:secret@localhost/db"),
            "postgresql://user:***@localhost/db"
        );
        assert_eq!(
            sanitize_url("postgresql://user@localhost/db"),
            "postgresql://user@localhost/db"
        );
        assert_eq!(sanitize_url("/path/to/db.sqlite"), "/path/to/db.sqlite");
    }
}
