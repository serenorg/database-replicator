// ABOUTME: JSONB utilities for storing non-PostgreSQL database data
// ABOUTME: Provides schema creation and validation for SQLite, MongoDB, and MySQL data storage

pub mod writer;

use anyhow::{bail, Result};

/// Validate a table name to prevent SQL injection
///
/// Table names must contain only:
/// - Lowercase letters (a-z)
/// - Uppercase letters (A-Z)
/// - Digits (0-9)
/// - Underscores (_)
///
/// This prevents SQL injection attacks through table names.
///
/// # Arguments
///
/// * `table_name` - The table name to validate
///
/// # Returns
///
/// Ok(()) if valid, Err with message if invalid
///
/// # Examples
///
/// ```
/// # use database_replicator::jsonb::validate_table_name;
/// assert!(validate_table_name("users").is_ok());
/// assert!(validate_table_name("user_events_2024").is_ok());
/// assert!(validate_table_name("users; DROP TABLE users;").is_err());
/// assert!(validate_table_name("users'--").is_err());
/// ```
pub fn validate_table_name(table_name: &str) -> Result<()> {
    if table_name.is_empty() {
        bail!("Table name cannot be empty");
    }

    if table_name.len() > 63 {
        bail!("Table name too long (max 63 characters): {}", table_name);
    }

    // Check that all characters are alphanumeric or underscore
    for ch in table_name.chars() {
        if !ch.is_ascii_alphanumeric() && ch != '_' {
            bail!(
                "Invalid table name '{}': contains invalid character '{}'. \
                Only alphanumeric characters and underscores are allowed.",
                table_name,
                ch
            );
        }
    }

    // Prevent reserved SQL keywords (case-insensitive)
    let lower = table_name.to_lowercase();
    let reserved_keywords = [
        "select",
        "insert",
        "update",
        "delete",
        "drop",
        "create",
        "alter",
        "table",
        "database",
        "index",
        "view",
        "function",
        "procedure",
        "trigger",
        "user",
        "role",
        "grant",
        "revoke",
    ];

    if reserved_keywords.contains(&lower.as_str()) {
        bail!(
            "Invalid table name '{}': cannot use SQL reserved keyword",
            table_name
        );
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_valid_table_names() {
        assert!(validate_table_name("users").is_ok());
        assert!(validate_table_name("user_events").is_ok());
        assert!(validate_table_name("UserEvents2024").is_ok());
        assert!(validate_table_name("_private").is_ok());
        assert!(validate_table_name("table123").is_ok());
    }

    #[test]
    fn test_invalid_table_names() {
        // SQL injection attempts
        assert!(validate_table_name("users; DROP TABLE users;").is_err());
        assert!(validate_table_name("users'--").is_err());
        assert!(validate_table_name("users OR 1=1").is_err());
        assert!(validate_table_name("users/**/").is_err());

        // Special characters
        assert!(validate_table_name("users-events").is_err());
        assert!(validate_table_name("users.events").is_err());
        assert!(validate_table_name("users@host").is_err());
        assert!(validate_table_name("users$var").is_err());

        // Empty or too long
        assert!(validate_table_name("").is_err());
        assert!(validate_table_name(&"a".repeat(64)).is_err());

        // Reserved keywords
        assert!(validate_table_name("select").is_err());
        assert!(validate_table_name("SELECT").is_err());
        assert!(validate_table_name("table").is_err());
        assert!(validate_table_name("drop").is_err());
    }
}
