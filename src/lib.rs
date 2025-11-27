// ABOUTME: Library module for database-replicator
// ABOUTME: Exports all core functionality for use in binary and tests

pub mod checkpoint;
pub mod commands;
pub mod config;
pub mod filters;
pub mod interactive;
pub mod jsonb;
pub mod migration;
pub mod mongodb;
pub mod mysql;
pub mod postgres;
pub mod preflight;
pub mod remote;
pub mod replication;
pub mod sqlite;
pub mod table_rules;
pub mod utils;

use anyhow::{bail, Result};

/// Database source types supported for replication
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SourceType {
    /// PostgreSQL database (postgresql:// or postgres:// URL)
    PostgreSQL,
    /// SQLite database file (.db, .sqlite, .sqlite3)
    SQLite,
    /// MongoDB database (mongodb:// or mongodb+srv:// URL)
    MongoDB,
    /// MySQL database (mysql:// URL)
    MySQL,
}

/// Detect the source database type from connection string or file path
///
/// Detection rules:
/// - PostgreSQL: Starts with `postgresql://` or `postgres://`
/// - SQLite: Ends with `.db`, `.sqlite`, or `.sqlite3`
/// - MongoDB: Starts with `mongodb://` or `mongodb+srv://`
/// - MySQL: Starts with `mysql://`
///
/// # Arguments
///
/// * `source` - Database connection string or file path
///
/// # Returns
///
/// Detected source type or error if type cannot be determined
///
/// # Examples
///
/// ```
/// # use database_replicator::{detect_source_type, SourceType};
/// assert_eq!(detect_source_type("postgresql://localhost/db").unwrap(), SourceType::PostgreSQL);
/// assert_eq!(detect_source_type("database.db").unwrap(), SourceType::SQLite);
/// assert_eq!(detect_source_type("data.sqlite3").unwrap(), SourceType::SQLite);
/// assert!(detect_source_type("invalid").is_err());
/// ```
pub fn detect_source_type(source: &str) -> Result<SourceType> {
    if source.starts_with("postgresql://") || source.starts_with("postgres://") {
        Ok(SourceType::PostgreSQL)
    } else if source.starts_with("mongodb://") || source.starts_with("mongodb+srv://") {
        Ok(SourceType::MongoDB)
    } else if source.starts_with("mysql://") {
        Ok(SourceType::MySQL)
    } else if source.ends_with(".db") || source.ends_with(".sqlite") || source.ends_with(".sqlite3")
    {
        Ok(SourceType::SQLite)
    } else {
        bail!(
            "Could not detect source database type from '{}'.\n\
             Supported sources:\n\
             - PostgreSQL: postgresql://... or postgres://...\n\
             - SQLite: path ending with .db, .sqlite, or .sqlite3\n\
             - MongoDB: mongodb://... or mongodb+srv://...\n\
             - MySQL: mysql://...",
            source
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_detect_postgresql() {
        assert_eq!(
            detect_source_type("postgresql://localhost/mydb").unwrap(),
            SourceType::PostgreSQL
        );
        assert_eq!(
            detect_source_type("postgres://user:pass@host:5432/db").unwrap(),
            SourceType::PostgreSQL
        );
    }

    #[test]
    fn test_detect_sqlite() {
        assert_eq!(
            detect_source_type("database.db").unwrap(),
            SourceType::SQLite
        );
        assert_eq!(
            detect_source_type("data.sqlite").unwrap(),
            SourceType::SQLite
        );
        assert_eq!(
            detect_source_type("/path/to/database.sqlite3").unwrap(),
            SourceType::SQLite
        );
    }

    #[test]
    fn test_detect_mongodb() {
        assert_eq!(
            detect_source_type("mongodb://localhost/db").unwrap(),
            SourceType::MongoDB
        );
        assert_eq!(
            detect_source_type("mongodb+srv://cluster.mongodb.net/db").unwrap(),
            SourceType::MongoDB
        );
    }

    #[test]
    fn test_detect_mysql() {
        assert_eq!(
            detect_source_type("mysql://localhost/db").unwrap(),
            SourceType::MySQL
        );
        assert_eq!(
            detect_source_type("mysql://user:pass@host:3306/db").unwrap(),
            SourceType::MySQL
        );
    }

    #[test]
    fn test_detect_invalid_source() {
        let result = detect_source_type("invalid_source");
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Could not detect source database type"));
    }

    #[test]
    fn test_detect_empty_string() {
        let result = detect_source_type("");
        assert!(result.is_err());
    }
}
