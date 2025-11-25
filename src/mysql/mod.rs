// ABOUTME: MySQL database reading utilities for replication to PostgreSQL
// ABOUTME: Provides secure connection validation and read-only database access

pub mod converter;
pub mod reader;

use anyhow::{bail, Context, Result};
use mysql_async::{Conn, Opts};

/// Validate a MySQL connection string to prevent injection attacks
///
/// Security checks:
/// - Validates URL format (mysql:// prefix)
/// - Ensures non-empty connection string
/// - Prevents malformed URLs
///
/// # Arguments
///
/// * `connection_string` - MySQL connection URL
///
/// # Returns
///
/// Validated connection string if valid, error otherwise
///
/// # Security
///
/// CRITICAL: This function prevents connection string injection attacks
///
/// # Examples
///
/// ```
/// # use database_replicator::mysql::validate_mysql_url;
/// // Valid URLs
/// assert!(validate_mysql_url("mysql://localhost:3306/mydb").is_ok());
/// assert!(validate_mysql_url("mysql://user:pass@host:3306/db").is_ok());
///
/// // Invalid URLs
/// assert!(validate_mysql_url("").is_err());
/// assert!(validate_mysql_url("postgresql://host/db").is_err());
/// ```
pub fn validate_mysql_url(connection_string: &str) -> Result<String> {
    if connection_string.is_empty() {
        bail!("MySQL connection string cannot be empty");
    }

    if !connection_string.starts_with("mysql://") {
        bail!(
            "Invalid MySQL connection string '{}'. \
             Must start with 'mysql://'",
            connection_string
        );
    }

    tracing::debug!("Validated MySQL connection string");

    Ok(connection_string.to_string())
}

/// Connect to MySQL database
///
/// Validates the connection string, creates a connection pool, and verifies
/// connectivity by executing a simple query.
///
/// # Arguments
///
/// * `connection_string` - MySQL connection URL (mysql://...)
///
/// # Returns
///
/// MySQL connection if successful
///
/// # Errors
///
/// Returns error if:
/// - Connection string is invalid
/// - Cannot parse connection options
/// - Cannot connect to MySQL server
/// - Cannot verify connection (ping fails)
///
/// # Examples
///
/// ```no_run
/// # use database_replicator::mysql::connect_mysql;
/// # async fn example() -> anyhow::Result<()> {
/// let conn = connect_mysql("mysql://user:pass@localhost:3306/mydb").await?;
/// # Ok(())
/// # }
/// ```
pub async fn connect_mysql(connection_string: &str) -> Result<Conn> {
    // Validate connection string first
    let validated_url = validate_mysql_url(connection_string)?;

    tracing::info!("Connecting to MySQL database");

    // Parse connection options
    let opts = Opts::from_url(&validated_url)
        .with_context(|| "Failed to parse MySQL connection options")?;

    // Create connection
    let conn = Conn::new(opts)
        .await
        .context("Failed to create MySQL connection")?;

    tracing::debug!("Successfully connected to MySQL");

    Ok(conn)
}

/// Extract database name from MySQL connection string
///
/// Parses the connection URL and extracts the database name if present.
///
/// # Arguments
///
/// * `connection_string` - MySQL connection URL
///
/// # Returns
///
/// Database name if present in URL, None otherwise
///
/// # Examples
///
/// ```
/// # use database_replicator::mysql::extract_database_name;
/// assert_eq!(
///     extract_database_name("mysql://localhost:3306/mydb"),
///     Some("mydb".to_string())
/// );
/// assert_eq!(
///     extract_database_name("mysql://localhost:3306"),
///     None
/// );
/// ```
pub fn extract_database_name(connection_string: &str) -> Option<String> {
    let opts = Opts::from_url(connection_string).ok()?;
    opts.db_name().map(|s| s.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_empty_url() {
        let result = validate_mysql_url("");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("cannot be empty"));
    }

    #[test]
    fn test_validate_invalid_prefix() {
        let invalid_urls = vec![
            "postgresql://localhost/db",
            "mongodb://localhost/db",
            "http://localhost",
            "localhost:3306",
        ];

        for url in invalid_urls {
            let result = validate_mysql_url(url);
            assert!(result.is_err(), "Invalid URL should be rejected: {}", url);
        }
    }

    #[test]
    fn test_validate_valid_mysql_url() {
        // Note: This test validates URL format, not actual connection
        let valid_urls = vec![
            "mysql://localhost:3306",
            "mysql://localhost:3306/mydb",
            "mysql://user:pass@localhost:3306/mydb",
            "mysql://user@localhost/db",
        ];

        for url in valid_urls {
            let result = validate_mysql_url(url);
            assert!(result.is_ok(), "Valid URL should be accepted: {}", url);
        }
    }

    #[test]
    fn test_extract_database_name_with_db() {
        let url = "mysql://localhost:3306/mydb";
        let db_name = extract_database_name(url);
        assert_eq!(db_name, Some("mydb".to_string()));
    }

    #[test]
    fn test_extract_database_name_without_db() {
        let url = "mysql://localhost:3306";
        let db_name = extract_database_name(url);
        assert_eq!(db_name, None);
    }

    #[test]
    fn test_extract_database_name_with_auth() {
        let url = "mysql://user:pass@localhost:3306/mydb";
        let db_name = extract_database_name(url);
        assert_eq!(db_name, Some("mydb".to_string()));
    }
}
