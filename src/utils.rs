// ABOUTME: Utility functions for validation and error handling
// ABOUTME: Provides input validation, retry logic, and resource cleanup

use anyhow::{bail, Context, Result};
use std::time::Duration;
use which::which;

/// Validate a PostgreSQL connection string
///
/// Checks that the connection string has proper format and required components:
/// - Starts with "postgres://" or "postgresql://"
/// - Contains user credentials (@ symbol)
/// - Contains database name (/ separator with at least 3 occurrences)
///
/// # Arguments
///
/// * `url` - Connection string to validate
///
/// # Returns
///
/// Returns `Ok(())` if the connection string is valid.
///
/// # Errors
///
/// Returns an error with helpful message if the connection string is:
/// - Empty or whitespace only
/// - Missing proper scheme (postgres:// or postgresql://)
/// - Missing user credentials (@ symbol)
/// - Missing database name
///
/// # Examples
///
/// ```
/// # use postgres_seren_replicator::utils::validate_connection_string;
/// # use anyhow::Result;
/// # fn example() -> Result<()> {
/// // Valid connection strings
/// validate_connection_string("postgresql://user:pass@localhost:5432/mydb")?;
/// validate_connection_string("postgres://user@host/db")?;
///
/// // Invalid - will return error
/// assert!(validate_connection_string("").is_err());
/// assert!(validate_connection_string("mysql://localhost/db").is_err());
/// # Ok(())
/// # }
/// ```
pub fn validate_connection_string(url: &str) -> Result<()> {
    if url.trim().is_empty() {
        bail!("Connection string cannot be empty");
    }

    // Check for common URL schemes
    if !url.starts_with("postgres://") && !url.starts_with("postgresql://") {
        bail!(
            "Invalid connection string format.\n\
             Expected format: postgresql://user:password@host:port/database\n\
             Got: {}",
            url
        );
    }

    // Check for minimum required components (user@host/database)
    if !url.contains('@') {
        bail!(
            "Connection string missing user credentials.\n\
             Expected format: postgresql://user:password@host:port/database"
        );
    }

    if !url.contains('/') || url.matches('/').count() < 3 {
        bail!(
            "Connection string missing database name.\n\
             Expected format: postgresql://user:password@host:port/database"
        );
    }

    Ok(())
}

/// Check that required PostgreSQL client tools are available
///
/// Verifies that the following tools are installed and in PATH:
/// - `pg_dump` - For dumping database schema and data
/// - `pg_dumpall` - For dumping global objects (roles, tablespaces)
/// - `psql` - For restoring databases
///
/// # Returns
///
/// Returns `Ok(())` if all required tools are found.
///
/// # Errors
///
/// Returns an error with installation instructions if any tools are missing.
///
/// # Examples
///
/// ```
/// # use postgres_seren_replicator::utils::check_required_tools;
/// # use anyhow::Result;
/// # fn example() -> Result<()> {
/// // Check if PostgreSQL tools are installed
/// check_required_tools()?;
/// # Ok(())
/// # }
/// ```
pub fn check_required_tools() -> Result<()> {
    let tools = ["pg_dump", "pg_dumpall", "psql"];
    let mut missing = Vec::new();

    for tool in &tools {
        if which(tool).is_err() {
            missing.push(*tool);
        }
    }

    if !missing.is_empty() {
        bail!(
            "Missing required PostgreSQL client tools: {}\n\
             \n\
             Please install PostgreSQL client tools:\n\
             - Ubuntu/Debian: sudo apt-get install postgresql-client\n\
             - macOS: brew install postgresql\n\
             - RHEL/CentOS: sudo yum install postgresql\n\
             - Windows: Download from https://www.postgresql.org/download/windows/",
            missing.join(", ")
        );
    }

    Ok(())
}

/// Retry a function with exponential backoff
///
/// Executes an async operation with automatic retry on failure. Each retry doubles
/// the delay (exponential backoff) to handle transient failures gracefully.
///
/// # Arguments
///
/// * `operation` - Async function to retry (FnMut returning Future\<Output = Result\<T\>\>)
/// * `max_retries` - Maximum number of retry attempts (0 = no retries, just initial attempt)
/// * `initial_delay` - Delay before first retry (doubles each subsequent retry)
///
/// # Returns
///
/// Returns the successful result or the last error after all retries exhausted.
///
/// # Examples
///
/// ```no_run
/// # use anyhow::Result;
/// # use std::time::Duration;
/// # use postgres_seren_replicator::utils::retry_with_backoff;
/// # async fn example() -> Result<()> {
/// let result = retry_with_backoff(
///     || async { Ok("success") },
///     3,  // Try up to 3 times
///     Duration::from_secs(1)  // Start with 1s delay
/// ).await?;
/// # Ok(())
/// # }
/// ```
pub async fn retry_with_backoff<F, Fut, T>(
    mut operation: F,
    max_retries: u32,
    initial_delay: Duration,
) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T>>,
{
    let mut delay = initial_delay;
    let mut last_error = None;

    for attempt in 0..=max_retries {
        match operation().await {
            Ok(result) => return Ok(result),
            Err(e) => {
                last_error = Some(e);

                if attempt < max_retries {
                    tracing::warn!(
                        "Operation failed (attempt {}/{}), retrying in {:?}...",
                        attempt + 1,
                        max_retries + 1,
                        delay
                    );
                    tokio::time::sleep(delay).await;
                    delay *= 2; // Exponential backoff
                }
            }
        }
    }

    Err(last_error.unwrap_or_else(|| anyhow::anyhow!("Operation failed after retries")))
}

/// Validate a PostgreSQL identifier (database name, schema name, etc.)
///
/// Validates that an identifier follows PostgreSQL naming rules to prevent SQL injection.
/// PostgreSQL identifiers must:
/// - Be 1-63 characters long
/// - Start with a letter (a-z, A-Z) or underscore (_)
/// - Contain only letters, digits (0-9), or underscores
///
/// # Arguments
///
/// * `identifier` - The identifier to validate (database name, schema name, etc.)
///
/// # Returns
///
/// Returns `Ok(())` if the identifier is valid.
///
/// # Errors
///
/// Returns an error if the identifier:
/// - Is empty or whitespace-only
/// - Exceeds 63 characters
/// - Starts with an invalid character (digit or special character)
/// - Contains invalid characters (anything except a-z, A-Z, 0-9, _)
///
/// # Security
///
/// This function is critical for preventing SQL injection attacks. All database
/// names, schema names, and table names from untrusted sources MUST be validated
/// before use in SQL statements.
///
/// # Examples
///
/// ```
/// # use postgres_seren_replicator::utils::validate_postgres_identifier;
/// # use anyhow::Result;
/// # fn example() -> Result<()> {
/// // Valid identifiers
/// validate_postgres_identifier("mydb")?;
/// validate_postgres_identifier("my_database")?;
/// validate_postgres_identifier("_private_db")?;
///
/// // Invalid - will return error
/// assert!(validate_postgres_identifier("123db").is_err());
/// assert!(validate_postgres_identifier("my-database").is_err());
/// assert!(validate_postgres_identifier("db\"; DROP TABLE users; --").is_err());
/// # Ok(())
/// # }
/// ```
pub fn validate_postgres_identifier(identifier: &str) -> Result<()> {
    // Check for empty or whitespace-only
    let trimmed = identifier.trim();
    if trimmed.is_empty() {
        bail!("Identifier cannot be empty or whitespace-only");
    }

    // Check length (PostgreSQL limit is 63 characters)
    if trimmed.len() > 63 {
        bail!(
            "Identifier '{}' exceeds maximum length of 63 characters (got {})",
            sanitize_identifier(trimmed),
            trimmed.len()
        );
    }

    // Get first character
    let first_char = trimmed.chars().next().unwrap();

    // First character must be a letter or underscore
    if !first_char.is_ascii_alphabetic() && first_char != '_' {
        bail!(
            "Identifier '{}' must start with a letter or underscore, not '{}'",
            sanitize_identifier(trimmed),
            first_char
        );
    }

    // All characters must be alphanumeric or underscore
    for (i, c) in trimmed.chars().enumerate() {
        if !c.is_ascii_alphanumeric() && c != '_' {
            bail!(
                "Identifier '{}' contains invalid character '{}' at position {}. \
                 Only letters, digits, and underscores are allowed",
                sanitize_identifier(trimmed),
                if c.is_control() {
                    format!("\\x{:02x}", c as u32)
                } else {
                    c.to_string()
                },
                i
            );
        }
    }

    Ok(())
}

/// Sanitize an identifier (table name, schema name, etc.) for display
///
/// Removes control characters and limits length to prevent log injection attacks
/// and ensure readable error messages.
///
/// **Note**: This is for display purposes only. For SQL safety, use parameterized
/// queries instead.
///
/// # Arguments
///
/// * `identifier` - The identifier to sanitize (table name, schema name, etc.)
///
/// # Returns
///
/// Sanitized string with control characters removed and length limited to 100 chars.
///
/// # Examples
///
/// ```
/// # use postgres_seren_replicator::utils::sanitize_identifier;
/// assert_eq!(sanitize_identifier("normal_table"), "normal_table");
/// assert_eq!(sanitize_identifier("table\x00name"), "tablename");
/// assert_eq!(sanitize_identifier("table\nname"), "tablename");
///
/// // Length limit
/// let long_name = "a".repeat(200);
/// assert_eq!(sanitize_identifier(&long_name).len(), 100);
/// ```
pub fn sanitize_identifier(identifier: &str) -> String {
    // Remove any control characters and limit length for display
    identifier
        .chars()
        .filter(|c| !c.is_control())
        .take(100)
        .collect()
}

/// Validate that source and target URLs are different to prevent accidental data loss
///
/// Compares two PostgreSQL connection URLs to ensure they point to different databases.
/// This is critical for preventing data loss from operations like `init --drop-existing`
/// where using the same URL for source and target would destroy the source data.
///
/// # Comparison Strategy
///
/// URLs are normalized and compared on:
/// - Host (case-insensitive)
/// - Port (defaulting to 5432 if not specified)
/// - Database name (case-sensitive)
/// - User (if present)
///
/// Query parameters (like SSL settings) are ignored as they don't affect database identity.
///
/// # Arguments
///
/// * `source_url` - Source database connection string
/// * `target_url` - Target database connection string
///
/// # Returns
///
/// Returns `Ok(())` if the URLs point to different databases.
///
/// # Errors
///
/// Returns an error if:
/// - The URLs point to the same database (same host, port, database name, and user)
/// - Either URL is malformed and cannot be parsed
///
/// # Examples
///
/// ```
/// # use postgres_seren_replicator::utils::validate_source_target_different;
/// # use anyhow::Result;
/// # fn example() -> Result<()> {
/// // Valid - different hosts
/// validate_source_target_different(
///     "postgresql://user:pass@source.com:5432/db",
///     "postgresql://user:pass@target.com:5432/db"
/// )?;
///
/// // Valid - different databases
/// validate_source_target_different(
///     "postgresql://user:pass@host:5432/db1",
///     "postgresql://user:pass@host:5432/db2"
/// )?;
///
/// // Invalid - same database
/// assert!(validate_source_target_different(
///     "postgresql://user:pass@host:5432/db",
///     "postgresql://user:pass@host:5432/db"
/// ).is_err());
/// # Ok(())
/// # }
/// ```
pub fn validate_source_target_different(source_url: &str, target_url: &str) -> Result<()> {
    // Parse both URLs to extract components
    let source_parts = parse_postgres_url(source_url)
        .with_context(|| format!("Failed to parse source URL: {}", source_url))?;
    let target_parts = parse_postgres_url(target_url)
        .with_context(|| format!("Failed to parse target URL: {}", target_url))?;

    // Compare normalized components
    if source_parts.host == target_parts.host
        && source_parts.port == target_parts.port
        && source_parts.database == target_parts.database
        && source_parts.user == target_parts.user
    {
        bail!(
            "Source and target URLs point to the same database!\\n\\\n             \\n\\\n             This would cause DATA LOSS - the target would overwrite the source.\\n\\\n             \\n\\\n             Source: {}@{}:{}/{}\\n\\\n             Target: {}@{}:{}/{}\\n\\\n             \\n\\\n             Please ensure source and target are different databases.\\n\\\n             Common causes:\\n\\\n             - Copy-paste error in connection strings\\n\\\n             - Wrong environment variables (e.g., SOURCE_URL == TARGET_URL)\\n\\\n             - Typo in database name or host",
            source_parts.user.as_deref().unwrap_or("(no user)"),
            source_parts.host,
            source_parts.port,
            source_parts.database,
            target_parts.user.as_deref().unwrap_or("(no user)"),
            target_parts.host,
            target_parts.port,
            target_parts.database
        );
    }

    Ok(())
}

/// Parse a PostgreSQL URL into its components
///
/// # Arguments
///
/// * `url` - PostgreSQL connection URL (postgres:// or postgresql://)
///
/// # Returns
///
/// Returns a `PostgresUrlParts` struct with normalized components.
fn parse_postgres_url(url: &str) -> Result<PostgresUrlParts> {
    // Remove scheme
    let url_without_scheme = url
        .trim_start_matches("postgres://")
        .trim_start_matches("postgresql://");

    // Split into base and query params (ignore query params for comparison)
    let base = url_without_scheme
        .split('?')
        .next()
        .unwrap_or(url_without_scheme);

    // Parse: [user[:password]@]host[:port]/database
    let (auth_and_host, database) = base
        .rsplit_once('/')
        .ok_or_else(|| anyhow::anyhow!("Missing database name in URL"))?;

    // Parse authentication and host
    let (user, host_and_port) = if let Some((auth, hp)) = auth_and_host.split_once('@') {
        // Has authentication
        let user = auth.split(':').next().unwrap_or(auth).to_string();
        (Some(user), hp)
    } else {
        // No authentication
        (None, auth_and_host)
    };

    // Parse host and port
    let (host, port) = if let Some((h, p)) = host_and_port.rsplit_once(':') {
        // Port specified
        let port = p
            .parse::<u16>()
            .with_context(|| format!("Invalid port number: {}", p))?;
        (h, port)
    } else {
        // Use default PostgreSQL port
        (host_and_port, 5432)
    };

    Ok(PostgresUrlParts {
        host: host.to_lowercase(), // Hostnames are case-insensitive
        port,
        database: database.to_string(), // Database names are case-sensitive in PostgreSQL
        user,
    })
}

/// Parsed components of a PostgreSQL connection URL
#[derive(Debug, PartialEq)]
struct PostgresUrlParts {
    host: String,
    port: u16,
    database: String,
    user: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_connection_string_valid() {
        assert!(validate_connection_string("postgresql://user:pass@localhost:5432/dbname").is_ok());
        assert!(validate_connection_string("postgres://user@host/db").is_ok());
    }

    #[test]
    fn test_check_required_tools() {
        // This test will pass if PostgreSQL client tools are installed
        // It will fail (appropriately) if they're not installed
        let result = check_required_tools();

        // On systems with PostgreSQL installed, this should pass
        // On systems without it, we expect a specific error message
        if let Err(err) = result {
            let err_msg = err.to_string();
            assert!(err_msg.contains("Missing required PostgreSQL client tools"));
            assert!(
                err_msg.contains("pg_dump")
                    || err_msg.contains("pg_dumpall")
                    || err_msg.contains("psql")
            );
        }
    }

    #[test]
    fn test_validate_connection_string_invalid() {
        assert!(validate_connection_string("").is_err());
        assert!(validate_connection_string("   ").is_err());
        assert!(validate_connection_string("mysql://localhost/db").is_err());
        assert!(validate_connection_string("postgresql://localhost").is_err());
        assert!(validate_connection_string("postgresql://localhost/db").is_err());
        // Missing user
    }

    #[test]
    fn test_sanitize_identifier() {
        assert_eq!(sanitize_identifier("normal_table"), "normal_table");
        assert_eq!(sanitize_identifier("table\x00name"), "tablename");
        assert_eq!(sanitize_identifier("table\nname"), "tablename");

        // Test length limit
        let long_name = "a".repeat(200);
        assert_eq!(sanitize_identifier(&long_name).len(), 100);
    }

    #[tokio::test]
    async fn test_retry_with_backoff_success() {
        let mut attempts = 0;
        let result = retry_with_backoff(
            || {
                attempts += 1;
                async move {
                    if attempts < 3 {
                        anyhow::bail!("Temporary failure")
                    } else {
                        Ok("Success")
                    }
                }
            },
            5,
            Duration::from_millis(10),
        )
        .await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "Success");
        assert_eq!(attempts, 3);
    }

    #[tokio::test]
    async fn test_retry_with_backoff_failure() {
        let mut attempts = 0;
        let result: Result<&str> = retry_with_backoff(
            || {
                attempts += 1;
                async move { anyhow::bail!("Permanent failure") }
            },
            2,
            Duration::from_millis(10),
        )
        .await;

        assert!(result.is_err());
        assert_eq!(attempts, 3); // Initial + 2 retries
    }

    #[test]
    fn test_validate_source_target_different_valid() {
        // Different hosts
        assert!(validate_source_target_different(
            "postgresql://user:pass@source.com:5432/db",
            "postgresql://user:pass@target.com:5432/db"
        )
        .is_ok());

        // Different databases on same host
        assert!(validate_source_target_different(
            "postgresql://user:pass@host:5432/db1",
            "postgresql://user:pass@host:5432/db2"
        )
        .is_ok());

        // Different ports on same host
        assert!(validate_source_target_different(
            "postgresql://user:pass@host:5432/db",
            "postgresql://user:pass@host:5433/db"
        )
        .is_ok());

        // Different users on same host/db (edge case but allowed)
        assert!(validate_source_target_different(
            "postgresql://user1:pass@host:5432/db",
            "postgresql://user2:pass@host:5432/db"
        )
        .is_ok());
    }

    #[test]
    fn test_validate_source_target_different_invalid() {
        // Exact same URL
        assert!(validate_source_target_different(
            "postgresql://user:pass@host:5432/db",
            "postgresql://user:pass@host:5432/db"
        )
        .is_err());

        // Same URL with different scheme (postgres vs postgresql)
        assert!(validate_source_target_different(
            "postgres://user:pass@host:5432/db",
            "postgresql://user:pass@host:5432/db"
        )
        .is_err());

        // Same URL with default port vs explicit port
        assert!(validate_source_target_different(
            "postgresql://user:pass@host/db",
            "postgresql://user:pass@host:5432/db"
        )
        .is_err());

        // Same URL with different query parameters (still same database)
        assert!(validate_source_target_different(
            "postgresql://user:pass@host:5432/db?sslmode=require",
            "postgresql://user:pass@host:5432/db?sslmode=prefer"
        )
        .is_err());

        // Same host with different case (hostnames are case-insensitive)
        assert!(validate_source_target_different(
            "postgresql://user:pass@HOST.COM:5432/db",
            "postgresql://user:pass@host.com:5432/db"
        )
        .is_err());
    }

    #[test]
    fn test_parse_postgres_url() {
        // Full URL with all components
        let parts = parse_postgres_url("postgresql://myuser:mypass@localhost:5432/mydb").unwrap();
        assert_eq!(parts.host, "localhost");
        assert_eq!(parts.port, 5432);
        assert_eq!(parts.database, "mydb");
        assert_eq!(parts.user, Some("myuser".to_string()));

        // URL without port (should default to 5432)
        let parts = parse_postgres_url("postgresql://user@host/db").unwrap();
        assert_eq!(parts.host, "host");
        assert_eq!(parts.port, 5432);
        assert_eq!(parts.database, "db");

        // URL without authentication
        let parts = parse_postgres_url("postgresql://host:5433/db").unwrap();
        assert_eq!(parts.host, "host");
        assert_eq!(parts.port, 5433);
        assert_eq!(parts.database, "db");
        assert_eq!(parts.user, None);

        // URL with query parameters
        let parts = parse_postgres_url("postgresql://user@host/db?sslmode=require").unwrap();
        assert_eq!(parts.host, "host");
        assert_eq!(parts.database, "db");

        // URL with postgres:// scheme (alternative)
        let parts = parse_postgres_url("postgres://user@host/db").unwrap();
        assert_eq!(parts.host, "host");
        assert_eq!(parts.database, "db");

        // Host normalization (lowercase)
        let parts = parse_postgres_url("postgresql://user@HOST.COM/db").unwrap();
        assert_eq!(parts.host, "host.com");
    }

    #[test]
    fn test_validate_postgres_identifier_valid() {
        // Valid identifiers
        assert!(validate_postgres_identifier("mydb").is_ok());
        assert!(validate_postgres_identifier("my_database").is_ok());
        assert!(validate_postgres_identifier("_private_db").is_ok());
        assert!(validate_postgres_identifier("db123").is_ok());
        assert!(validate_postgres_identifier("Database_2024").is_ok());

        // Maximum length (63 characters)
        let max_length_name = "a".repeat(63);
        assert!(validate_postgres_identifier(&max_length_name).is_ok());
    }

    #[test]
    fn test_validate_postgres_identifier_invalid() {
        // SQL injection attempts
        assert!(validate_postgres_identifier("mydb\"; DROP DATABASE production; --").is_err());
        assert!(validate_postgres_identifier("db'; DELETE FROM users; --").is_err());

        // Invalid start characters
        assert!(validate_postgres_identifier("123db").is_err()); // Starts with digit
        assert!(validate_postgres_identifier("$db").is_err()); // Starts with special char
        assert!(validate_postgres_identifier("-db").is_err()); // Starts with dash

        // Contains invalid characters
        assert!(validate_postgres_identifier("my-database").is_err()); // Contains dash
        assert!(validate_postgres_identifier("my.database").is_err()); // Contains dot
        assert!(validate_postgres_identifier("my database").is_err()); // Contains space
        assert!(validate_postgres_identifier("my@db").is_err()); // Contains @
        assert!(validate_postgres_identifier("my#db").is_err()); // Contains #

        // Empty or too long
        assert!(validate_postgres_identifier("").is_err());
        assert!(validate_postgres_identifier("   ").is_err());

        // Over maximum length (64+ characters)
        let too_long = "a".repeat(64);
        assert!(validate_postgres_identifier(&too_long).is_err());

        // Control characters
        assert!(validate_postgres_identifier("my\ndb").is_err());
        assert!(validate_postgres_identifier("my\tdb").is_err());
        assert!(validate_postgres_identifier("my\x00db").is_err());
    }
}
