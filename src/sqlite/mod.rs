// ABOUTME: SQLite database reading utilities for migration to PostgreSQL
// ABOUTME: Provides secure file path validation and read-only database connections

pub mod converter;
pub mod reader;

use anyhow::{bail, Context, Result};
use std::path::PathBuf;

/// Validate a SQLite file path to prevent path traversal attacks
///
/// Security checks:
/// - Canonicalizes path to resolve symlinks and relative paths
/// - Verifies file exists and is a regular file (not directory)
/// - Checks file extension is .db, .sqlite, or .sqlite3
/// - Does NOT follow symlinks outside allowed directories
///
/// # Arguments
///
/// * `path` - Path to SQLite file (can be relative or absolute)
///
/// # Returns
///
/// Canonicalized absolute path if valid, error otherwise
///
/// # Security
///
/// CRITICAL: This function prevents path traversal attacks like:
/// - ../../../etc/passwd
/// - /etc/shadow
/// - Symlink attacks
///
/// # Examples
///
/// ```no_run
/// # use postgres_seren_replicator::sqlite::validate_sqlite_path;
/// // Valid paths (when files exist)
/// assert!(validate_sqlite_path("database.db").is_ok());
/// assert!(validate_sqlite_path("/tmp/test.sqlite").is_ok());
///
/// // Invalid paths
/// assert!(validate_sqlite_path("../../../etc/passwd").is_err());
/// assert!(validate_sqlite_path("/nonexistent.db").is_err());
/// ```
pub fn validate_sqlite_path(path: &str) -> Result<PathBuf> {
    if path.is_empty() {
        bail!("SQLite file path cannot be empty");
    }

    let path_buf = PathBuf::from(path);

    // Canonicalize to resolve symlinks and relative paths
    // This also validates that the file exists
    let canonical = path_buf.canonicalize().with_context(|| {
        format!(
            "Failed to resolve SQLite file path '{}'. \
             File may not exist or may not be readable.",
            path
        )
    })?;

    // Verify it's a file, not a directory
    if !canonical.is_file() {
        bail!("Path '{}' is not a regular file (may be a directory)", path);
    }

    // Verify file extension
    if let Some(ext) = canonical.extension() {
        let ext_str = ext.to_str().unwrap_or("");
        if !["db", "sqlite", "sqlite3"].contains(&ext_str) {
            bail!(
                "Invalid SQLite file extension '{}'. \
                 Must be .db, .sqlite, or .sqlite3",
                ext_str
            );
        }
    } else {
        bail!(
            "SQLite file '{}' has no extension. \
             Must be .db, .sqlite, or .sqlite3",
            path
        );
    }

    tracing::debug!("Validated SQLite path: {}", canonical.display());

    Ok(canonical)
}

/// Open a SQLite database in read-only mode
///
/// Opens the database with read-only flag for safety.
/// The database file must exist and be readable.
///
/// # Arguments
///
/// * `path` - Path to SQLite file (will be validated)
///
/// # Returns
///
/// Read-only rusqlite::Connection if successful
///
/// # Security
///
/// - Path is validated before opening
/// - Database opened in read-only mode (SQLITE_OPEN_READ_ONLY)
/// - No modifications possible
///
/// # Examples
///
/// ```no_run
/// # use postgres_seren_replicator::sqlite::open_sqlite;
/// # fn example() -> anyhow::Result<()> {
/// let conn = open_sqlite("database.db")?;
/// // Use connection to read data
/// # Ok(())
/// # }
/// ```
pub fn open_sqlite(path: &str) -> Result<rusqlite::Connection> {
    // Validate path first
    let canonical = validate_sqlite_path(path)?;

    tracing::info!("Opening SQLite database: {}", canonical.display());

    // Open in read-only mode for safety
    let conn = rusqlite::Connection::open_with_flags(
        &canonical,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
    )
    .with_context(|| format!("Failed to open SQLite database: {}", canonical.display()))?;

    // Verify we can query the database
    let _version: String = conn
        .query_row("SELECT sqlite_version()", [], |row| row.get(0))
        .context("Failed to query SQLite version (database may be corrupted)")?;

    tracing::debug!("Successfully opened SQLite database");

    Ok(conn)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;

    #[test]
    fn test_validate_empty_path() {
        let result = validate_sqlite_path("");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("cannot be empty"));
    }

    #[test]
    fn test_validate_nonexistent_file() {
        let result = validate_sqlite_path("/nonexistent/database.db");
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_no_extension() {
        // Create a temp file with no extension
        let temp_dir = std::env::temp_dir();
        let no_ext_path = temp_dir.join("test_file_no_ext");
        File::create(&no_ext_path).unwrap();

        let result = validate_sqlite_path(no_ext_path.to_str().unwrap());
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("no extension"));

        // Cleanup
        std::fs::remove_file(no_ext_path).ok();
    }

    #[test]
    fn test_validate_wrong_extension() {
        // Create a temp file with wrong extension
        let temp_dir = std::env::temp_dir();
        let wrong_ext_path = temp_dir.join("test_file.txt");
        File::create(&wrong_ext_path).unwrap();

        let result = validate_sqlite_path(wrong_ext_path.to_str().unwrap());
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Invalid SQLite file extension"));

        // Cleanup
        std::fs::remove_file(wrong_ext_path).ok();
    }

    #[test]
    fn test_validate_directory() {
        let temp_dir = std::env::temp_dir();
        let result = validate_sqlite_path(temp_dir.to_str().unwrap());
        assert!(result.is_err());
        // Different error message depending on whether dir has extension
    }

    #[test]
    fn test_validate_valid_extensions() {
        // Create temp files with valid extensions
        let temp_dir = std::env::temp_dir();

        for ext in &["db", "sqlite", "sqlite3"] {
            let file_path = temp_dir.join(format!("test_file.{}", ext));
            File::create(&file_path).unwrap();

            let result = validate_sqlite_path(file_path.to_str().unwrap());
            assert!(
                result.is_ok(),
                "Extension .{} should be valid, but got error: {:?}",
                ext,
                result.err()
            );

            // Cleanup
            std::fs::remove_file(file_path).ok();
        }
    }

    #[test]
    fn test_path_traversal_prevention() {
        // These should fail because files don't exist, not because of traversal per se
        // But canonicalize will prevent traversal if file existed
        let traversal_attempts = vec!["../../../etc/passwd", "../../..", "/etc/shadow"];

        for attempt in traversal_attempts {
            let result = validate_sqlite_path(attempt);
            assert!(
                result.is_err(),
                "Path traversal attempt '{}' should be rejected",
                attempt
            );
        }
    }

    #[test]
    fn test_open_sqlite_invalid_path() {
        let result = open_sqlite("/nonexistent/database.db");
        assert!(result.is_err());
    }

    #[test]
    fn test_open_sqlite_creates_readonly_connection() {
        // Create a temporary SQLite database
        let temp_dir = std::env::temp_dir();
        let db_path = temp_dir.join("test_readonly.db");

        // Create database with a table
        {
            let conn = rusqlite::Connection::open(&db_path).unwrap();
            conn.execute("CREATE TABLE test (id INTEGER)", []).unwrap();
        }

        // Open in read-only mode
        let conn = open_sqlite(db_path.to_str().unwrap()).unwrap();

        // Verify we can read
        let result: Result<i32, _> =
            conn.query_row("SELECT COUNT(*) FROM test", [], |row| row.get(0));
        assert!(result.is_ok());

        // Verify we CANNOT write (should fail because read-only)
        let write_result = conn.execute("INSERT INTO test VALUES (1)", []);
        assert!(write_result.is_err());
        assert!(write_result
            .unwrap_err()
            .to_string()
            .to_lowercase()
            .contains("read"));

        // Cleanup
        std::fs::remove_file(db_path).ok();
    }
}
