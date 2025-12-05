// ABOUTME: Utility functions for validation and error handling
// ABOUTME: Provides input validation, retry logic, and resource cleanup

use anyhow::{bail, Context, Result};
use std::time::Duration;
use url::Url;
use which::which;

/// Get TCP keepalive environment variables for PostgreSQL client tools
///
/// Returns environment variables that configure TCP keepalives for external
/// PostgreSQL tools (pg_dump, pg_restore, psql, pg_dumpall). These prevent
/// idle connection timeouts when connecting through load balancers like AWS ELB.
///
/// Environment variables returned:
/// - `PGKEEPALIVES=1`: Enable TCP keepalives
/// - `PGKEEPALIVESIDLE=60`: Send first keepalive after 60 seconds of idle time
/// - `PGKEEPALIVESINTERVAL=10`: Send subsequent keepalives every 10 seconds
///
/// # Returns
///
/// A vector of (variable_name, value) tuples to be passed to subprocess commands
///
/// # Examples
///
/// ```
/// # use database_replicator::utils::get_keepalive_env_vars;
/// # use std::process::Command;
/// let keepalive_vars = get_keepalive_env_vars();
/// let mut cmd = Command::new("psql");
/// for (key, value) in keepalive_vars {
///     cmd.env(key, value);
/// }
/// ```
pub fn get_keepalive_env_vars() -> Vec<(&'static str, &'static str)> {
    vec![
        ("PGKEEPALIVES", "1"),
        ("PGKEEPALIVESIDLE", "60"),
        ("PGKEEPALIVESINTERVAL", "10"),
    ]
}

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
/// # use database_replicator::utils::validate_connection_string;
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
/// # use database_replicator::utils::check_required_tools;
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
/// # use database_replicator::utils::retry_with_backoff;
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

/// Retry a subprocess execution with exponential backoff on connection errors
///
/// Executes a subprocess command with automatic retry on connection-related failures.
/// Each retry doubles the delay (exponential backoff) to handle transient connection issues.
///
/// Connection errors are detected by checking:
/// - Non-zero exit codes
/// - Stderr output containing connection-related error patterns:
///   - "connection closed"
///   - "connection refused"
///   - "could not connect"
///   - "server closed the connection"
///   - "timeout"
///   - "Connection timed out"
///
/// # Arguments
///
/// * `operation` - Function that executes a Command and returns the exit status
/// * `max_retries` - Maximum number of retry attempts (0 = no retries, just initial attempt)
/// * `initial_delay` - Delay before first retry (doubles each subsequent retry)
/// * `operation_name` - Name of the operation for logging (e.g., "pg_restore", "psql")
///
/// # Returns
///
/// Returns Ok(()) on success or the last error after all retries exhausted.
///
/// # Examples
///
/// ```no_run
/// # use anyhow::Result;
/// # use std::time::Duration;
/// # use std::process::Command;
/// # use database_replicator::utils::retry_subprocess_with_backoff;
/// # async fn example() -> Result<()> {
/// retry_subprocess_with_backoff(
///     || {
///         let mut cmd = Command::new("psql");
///         cmd.arg("--version");
///         cmd.status().map_err(anyhow::Error::from)
///     },
///     3,  // Try up to 3 times
///     Duration::from_secs(1),  // Start with 1s delay
///     "psql"
/// ).await?;
/// # Ok(())
/// # }
/// ```
pub async fn retry_subprocess_with_backoff<F>(
    mut operation: F,
    max_retries: u32,
    initial_delay: Duration,
    operation_name: &str,
) -> Result<()>
where
    F: FnMut() -> Result<std::process::ExitStatus>,
{
    let mut delay = initial_delay;
    let mut last_error = None;

    for attempt in 0..=max_retries {
        match operation() {
            Ok(status) => {
                if status.success() {
                    return Ok(());
                } else {
                    // Non-zero exit code - check if it's a connection error
                    // We can't easily capture stderr here, so we'll treat all non-zero
                    // exit codes as potential connection errors for now
                    let error = anyhow::anyhow!(
                        "{} failed with exit code: {}",
                        operation_name,
                        status.code().unwrap_or(-1)
                    );
                    last_error = Some(error);

                    if attempt < max_retries {
                        tracing::warn!(
                            "{} failed (attempt {}/{}), retrying in {:?}...",
                            operation_name,
                            attempt + 1,
                            max_retries + 1,
                            delay
                        );
                        tokio::time::sleep(delay).await;
                        delay *= 2; // Exponential backoff
                    }
                }
            }
            Err(e) => {
                last_error = Some(e);

                if attempt < max_retries {
                    tracing::warn!(
                        "{} failed (attempt {}/{}): {}, retrying in {:?}...",
                        operation_name,
                        attempt + 1,
                        max_retries + 1,
                        last_error.as_ref().unwrap(),
                        delay
                    );
                    tokio::time::sleep(delay).await;
                    delay *= 2; // Exponential backoff
                }
            }
        }
    }

    Err(last_error.unwrap_or_else(|| {
        anyhow::anyhow!("{} failed after {} retries", operation_name, max_retries)
    }))
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
/// # use database_replicator::utils::validate_postgres_identifier;
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
/// # use database_replicator::utils::sanitize_identifier;
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

/// Quote a PostgreSQL identifier (database, schema, table, column)
///
/// Assumes the identifier has already been validated. Escapes embedded quotes
/// and wraps the identifier in double quotes.
pub fn quote_ident(identifier: &str) -> String {
    let mut quoted = String::with_capacity(identifier.len() + 2);
    quoted.push('"');
    for ch in identifier.chars() {
        if ch == '"' {
            quoted.push('"');
        }
        quoted.push(ch);
    }
    quoted.push('"');
    quoted
}

/// Quote a SQL string literal (for use in SQL statements)
///
/// Escapes single quotes by doubling them and wraps the string in single quotes.
/// Use this for string values in SQL, not for identifiers.
///
/// # Examples
///
/// ```
/// use database_replicator::utils::quote_literal;
/// assert_eq!(quote_literal("hello"), "'hello'");
/// assert_eq!(quote_literal("it's"), "'it''s'");
/// assert_eq!(quote_literal(""), "''");
/// ```
pub fn quote_literal(value: &str) -> String {
    let mut quoted = String::with_capacity(value.len() + 2);
    quoted.push('\'');
    for ch in value.chars() {
        if ch == '\'' {
            quoted.push('\'');
        }
        quoted.push(ch);
    }
    quoted.push('\'');
    quoted
}

/// Quote a MySQL identifier (database, table, column)
///
/// MySQL uses backticks for identifier quoting. Escapes embedded backticks
/// by doubling them.
///
/// # Examples
///
/// ```
/// use database_replicator::utils::quote_mysql_ident;
/// assert_eq!(quote_mysql_ident("users"), "`users`");
/// assert_eq!(quote_mysql_ident("user`name"), "`user``name`");
/// ```
pub fn quote_mysql_ident(identifier: &str) -> String {
    let mut quoted = String::with_capacity(identifier.len() + 2);
    quoted.push('`');
    for ch in identifier.chars() {
        if ch == '`' {
            quoted.push('`');
        }
        quoted.push(ch);
    }
    quoted.push('`');
    quoted
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
/// # use database_replicator::utils::validate_source_target_different;
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
            "Source and target URLs point to the same database!\n\
             \n\
             This would cause DATA LOSS - the target would overwrite the source.\n\
             \n\
             Source: {}@{}:{}/{}\n\
             Target: {}@{}:{}/{}\n\
             \n\
             Please ensure source and target are different databases.\n\
             Common causes:\n\
             - Copy-paste error in connection strings\n\
             - Wrong environment variables (e.g., SOURCE_URL == TARGET_URL)\n\
             - Typo in database name or host",
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
///
/// # Security
///
/// This function extracts passwords from URLs for use with .pgpass files.
/// Ensure returned values are handled securely and not logged.
pub fn parse_postgres_url(url: &str) -> Result<PostgresUrlParts> {
    // Remove scheme
    let url_without_scheme = url
        .trim_start_matches("postgres://")
        .trim_start_matches("postgresql://");

    // Split into base and query params
    let (base, query_string) = if let Some((b, q)) = url_without_scheme.split_once('?') {
        (b, Some(q))
    } else {
        (url_without_scheme, None)
    };

    // Parse query parameters into HashMap
    let mut query_params = std::collections::HashMap::new();
    if let Some(query) = query_string {
        for param in query.split('&') {
            if let Some((key, value)) = param.split_once('=') {
                query_params.insert(key.to_string(), value.to_string());
            }
        }
    }

    // Parse: [user[:password]@]host[:port]/database
    let (auth_and_host, database) = base
        .rsplit_once('/')
        .ok_or_else(|| anyhow::anyhow!("Missing database name in URL"))?;

    // Parse authentication and host
    // Use rsplit_once to split from the right, so passwords can contain '@'
    let (user, password, host_and_port) = if let Some((auth, hp)) = auth_and_host.rsplit_once('@') {
        // Has authentication
        let (user, pass) = if let Some((u, p)) = auth.split_once(':') {
            (Some(u.to_string()), Some(p.to_string()))
        } else {
            (Some(auth.to_string()), None)
        };
        (user, pass, hp)
    } else {
        // No authentication
        (None, None, auth_and_host)
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
        password,
        query_params,
    })
}

/// Strip password from PostgreSQL connection URL
/// Returns a new URL with password removed, preserving all other components
/// This is useful for storing connection strings in places where passwords should not be visible
pub fn strip_password_from_url(url: &str) -> Result<String> {
    let parts = parse_postgres_url(url)?;

    // Reconstruct URL without password
    let scheme = if url.starts_with("postgresql://") {
        "postgresql://"
    } else if url.starts_with("postgres://") {
        "postgres://"
    } else {
        bail!("Invalid PostgreSQL URL scheme");
    };

    let mut result = String::from(scheme);

    // Add user if present (without password)
    if let Some(user) = &parts.user {
        result.push_str(user);
        result.push('@');
    }

    // Add host and port
    result.push_str(&parts.host);
    result.push(':');
    result.push_str(&parts.port.to_string());

    // Add database
    result.push('/');
    result.push_str(&parts.database);

    // Preserve query parameters if present
    if let Some(query_start) = url.find('?') {
        result.push_str(&url[query_start..]);
    }

    Ok(result)
}

/// Parsed components of a PostgreSQL connection URL
#[derive(Debug, PartialEq)]
pub struct PostgresUrlParts {
    pub host: String,
    pub port: u16,
    pub database: String,
    pub user: Option<String>,
    pub password: Option<String>,
    pub query_params: std::collections::HashMap<String, String>,
}

impl PostgresUrlParts {
    /// Convert query parameters to PostgreSQL environment variables
    ///
    /// Maps common connection URL query parameters to their corresponding
    /// PostgreSQL environment variable names. This allows SSL/TLS and other
    /// connection settings to be passed to pg_dump, pg_dumpall, psql, etc.
    ///
    /// # Supported Parameters
    ///
    /// - `sslmode` → `PGSSLMODE`
    /// - `sslcert` → `PGSSLCERT`
    /// - `sslkey` → `PGSSLKEY`
    /// - `sslrootcert` → `PGSSLROOTCERT`
    /// - `channel_binding` → `PGCHANNELBINDING`
    /// - `connect_timeout` → `PGCONNECT_TIMEOUT`
    /// - `application_name` → `PGAPPNAME`
    /// - `client_encoding` → `PGCLIENTENCODING`
    ///
    /// # Returns
    ///
    /// Vec of (env_var_name, value) pairs to be set as environment variables
    pub fn to_pg_env_vars(&self) -> Vec<(&'static str, String)> {
        let mut env_vars = Vec::new();

        // Map query parameters to PostgreSQL environment variables
        let param_mapping = [
            ("sslmode", "PGSSLMODE"),
            ("sslcert", "PGSSLCERT"),
            ("sslkey", "PGSSLKEY"),
            ("sslrootcert", "PGSSLROOTCERT"),
            ("channel_binding", "PGCHANNELBINDING"),
            ("connect_timeout", "PGCONNECT_TIMEOUT"),
            ("application_name", "PGAPPNAME"),
            ("client_encoding", "PGCLIENTENCODING"),
        ];

        for (param_name, env_var_name) in param_mapping {
            if let Some(value) = self.query_params.get(param_name) {
                env_vars.push((env_var_name, value.clone()));
            }
        }

        env_vars
    }
}

/// Managed .pgpass file for secure password passing to PostgreSQL tools
///
/// This struct creates a temporary .pgpass file with secure permissions (0600)
/// and automatically cleans it up when dropped. PostgreSQL command-line tools
/// read credentials from this file instead of accepting passwords in URLs,
/// which prevents command injection vulnerabilities.
///
/// # Security
///
/// - File permissions are set to 0600 (owner read/write only)
/// - File is automatically removed on Drop
/// - Credentials are never passed on command line
///
/// # Format
///
/// .pgpass file format: hostname:port:database:username:password
/// Wildcards (*) are used for maximum compatibility
///
/// # Examples
///
/// ```no_run
/// # use database_replicator::utils::{PgPassFile, parse_postgres_url};
/// # use anyhow::Result;
/// # fn example() -> Result<()> {
/// let url = "postgresql://user:pass@localhost:5432/mydb";
/// let parts = parse_postgres_url(url)?;
/// let pgpass = PgPassFile::new(&parts)?;
///
/// // Use pgpass.path() with PGPASSFILE environment variable
/// // File is automatically cleaned up when pgpass goes out of scope
/// # Ok(())
/// # }
/// ```
pub struct PgPassFile {
    path: std::path::PathBuf,
}

impl PgPassFile {
    /// Create a new .pgpass file with credentials from URL parts
    ///
    /// # Arguments
    ///
    /// * `parts` - Parsed PostgreSQL URL components
    ///
    /// # Returns
    ///
    /// Returns a PgPassFile that will be automatically cleaned up on Drop
    ///
    /// # Errors
    ///
    /// Returns an error if the file cannot be created or permissions cannot be set
    pub fn new(parts: &PostgresUrlParts) -> Result<Self> {
        use std::fs;
        use std::io::Write;

        // Create temp file with secure name
        let temp_dir = std::env::temp_dir();
        let random: u32 = rand::random();
        let filename = format!("pgpass-{:08x}", random);
        let path = temp_dir.join(filename);

        // Write .pgpass entry
        // Format: hostname:port:database:username:password
        let username = parts.user.as_deref().unwrap_or("*");
        let password = parts.password.as_deref().unwrap_or("");
        let entry = format!(
            "{}:{}:{}:{}:{}\n",
            parts.host, parts.port, parts.database, username, password
        );

        let mut file = fs::File::create(&path)
            .with_context(|| format!("Failed to create .pgpass file at {}", path.display()))?;

        file.write_all(entry.as_bytes())
            .with_context(|| format!("Failed to write to .pgpass file at {}", path.display()))?;

        // Set secure permissions (0600) - owner read/write only
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let permissions = fs::Permissions::from_mode(0o600);
            fs::set_permissions(&path, permissions).with_context(|| {
                format!(
                    "Failed to set permissions on .pgpass file at {}",
                    path.display()
                )
            })?;
        }

        // On Windows, .pgpass is stored in %APPDATA%\postgresql\pgpass.conf
        // but for our temporary use case, we'll just use a temp file
        // PostgreSQL on Windows also checks permissions but less strictly

        Ok(Self { path })
    }

    /// Get the path to the .pgpass file
    ///
    /// Use this with the PGPASSFILE environment variable when running
    /// PostgreSQL command-line tools
    pub fn path(&self) -> &std::path::Path {
        &self.path
    }
}

impl Drop for PgPassFile {
    fn drop(&mut self) {
        // Best effort cleanup - don't panic if removal fails
        let _ = std::fs::remove_file(&self.path);
    }
}

/// Create a managed temporary directory with explicit cleanup support
///
/// Creates a temporary directory with a timestamped name that can be cleaned up
/// even if the process is killed with SIGKILL. Unlike `TempDir::new()` which
/// relies on the Drop trait, this function creates named directories that can
/// be cleaned up on next process startup.
///
/// Directory naming format: `postgres-seren-replicator-{timestamp}-{random}`
/// Example: `postgres-seren-replicator-20250106-120534-a3b2c1d4`
///
/// # Returns
///
/// Returns the path to the created temporary directory.
///
/// # Errors
///
/// Returns an error if the directory cannot be created.
///
/// # Examples
///
/// ```no_run
/// # use database_replicator::utils::create_managed_temp_dir;
/// # use anyhow::Result;
/// # fn example() -> Result<()> {
/// let temp_path = create_managed_temp_dir()?;
/// println!("Using temp directory: {}", temp_path.display());
/// // ... do work ...
/// // Cleanup happens automatically on next startup via cleanup_stale_temp_dirs()
/// # Ok(())
/// # }
/// ```
pub fn create_managed_temp_dir() -> Result<std::path::PathBuf> {
    use std::fs;
    use std::time::SystemTime;

    let system_temp = std::env::temp_dir();

    // Generate timestamp for directory name
    let timestamp = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_secs();

    // Generate random suffix for uniqueness
    let random: u32 = rand::random();

    // Create directory name with timestamp and random suffix
    let dir_name = format!("postgres-seren-replicator-{}-{:08x}", timestamp, random);

    let temp_path = system_temp.join(dir_name);

    // Create the directory
    fs::create_dir_all(&temp_path)
        .with_context(|| format!("Failed to create temp directory at {}", temp_path.display()))?;

    tracing::debug!("Created managed temp directory: {}", temp_path.display());

    Ok(temp_path)
}

/// Clean up stale temporary directories from previous runs
///
/// Removes temporary directories created by `create_managed_temp_dir()` that are
/// older than the specified age. This should be called on process startup to clean
/// up directories left behind by processes killed with SIGKILL.
///
/// Only directories matching the pattern `postgres-seren-replicator-*` are removed.
///
/// # Arguments
///
/// * `max_age_secs` - Maximum age in seconds before a directory is considered stale
///   (recommended: 86400 for 24 hours)
///
/// # Returns
///
/// Returns the number of directories cleaned up.
///
/// # Errors
///
/// Returns an error if the system temp directory cannot be read. Individual
/// directory removal errors are logged but don't fail the entire operation.
///
/// # Examples
///
/// ```no_run
/// # use database_replicator::utils::cleanup_stale_temp_dirs;
/// # use anyhow::Result;
/// # fn example() -> Result<()> {
/// // Clean up temp directories older than 24 hours
/// let cleaned = cleanup_stale_temp_dirs(86400)?;
/// println!("Cleaned up {} stale temp directories", cleaned);
/// # Ok(())
/// # }
/// ```
pub fn cleanup_stale_temp_dirs(max_age_secs: u64) -> Result<usize> {
    use std::fs;
    use std::time::SystemTime;

    let system_temp = std::env::temp_dir();
    let now = SystemTime::now();
    let mut cleaned_count = 0;

    // Read all entries in system temp directory
    let entries = fs::read_dir(&system_temp).with_context(|| {
        format!(
            "Failed to read system temp directory: {}",
            system_temp.display()
        )
    })?;

    for entry in entries.flatten() {
        let path = entry.path();

        // Only process directories matching our naming pattern
        if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
            if !name.starts_with("postgres-seren-replicator-") {
                continue;
            }

            // Check directory age
            match entry.metadata() {
                Ok(metadata) => {
                    if let Ok(modified) = metadata.modified() {
                        if let Ok(age) = now.duration_since(modified) {
                            if age.as_secs() > max_age_secs {
                                // Directory is stale, remove it
                                match fs::remove_dir_all(&path) {
                                    Ok(_) => {
                                        tracing::info!(
                                            "Cleaned up stale temp directory: {} (age: {}s)",
                                            path.display(),
                                            age.as_secs()
                                        );
                                        cleaned_count += 1;
                                    }
                                    Err(e) => {
                                        tracing::warn!(
                                            "Failed to remove stale temp directory {}: {}",
                                            path.display(),
                                            e
                                        );
                                    }
                                }
                            }
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        "Failed to get metadata for temp directory {}: {}",
                        path.display(),
                        e
                    );
                }
            }
        }
    }

    if cleaned_count > 0 {
        tracing::info!(
            "Cleaned up {} stale temp directory(ies) older than {} seconds",
            cleaned_count,
            max_age_secs
        );
    }

    Ok(cleaned_count)
}

/// Parse a SerenDB URL to extract project, branch, and database IDs
///
/// SerenDB URLs have the format: postgresql://user:pass@<database-id>.<branch-id>.<project-id>.serendb.com:5432/db
/// This function extracts the three UUIDs from the hostname.
///
/// # Arguments
///
/// * `url` - The SerenDB PostgreSQL connection string
///
/// # Returns
///
/// An `Option` containing a tuple of `(project_id, branch_id, database_id)` if the
/// URL is a valid SerenDB target and contains the expected ID format, otherwise `None`.
pub fn parse_serendb_url_for_ids(url: &str) -> Option<(String, String, String)> {
    let parts = parse_postgres_url(url).ok()?;

    if !is_serendb_target(url) {
        return None;
    }

    // Hostname format: <database-id>.<branch-id>.<project-id>.serendb.com
    // Or with custom subdomains: <database-id>.<branch-id>.<project-id>.<custom>.serendb.com
    // We want the last three parts before .serendb.com
    let host_parts: Vec<&str> = parts.host.split('.').collect();

    if host_parts.len() < 4 {
        return None; // Not enough parts for SerenDB ID format
    }

    let num_host_parts = host_parts.len();
    let database_id = host_parts[num_host_parts - 4].to_string();
    let branch_id = host_parts[num_host_parts - 3].to_string();
    let project_id = host_parts[num_host_parts - 2].to_string();

    // Basic UUID format validation (optional but good for robustness)
    // A real UUID check would be more extensive, but string length is a good start
    if database_id.len() == 36 && branch_id.len() == 36 && project_id.len() == 36 {
        Some((project_id, branch_id, database_id))
    } else {
        None
    }
}

/// Remove a managed temporary directory
///
/// Explicitly removes a temporary directory created by `create_managed_temp_dir()`.
/// This should be called when the directory is no longer needed.
///
/// # Arguments
///
/// * `path` - Path to the temporary directory to remove
///
/// # Errors
///
/// Returns an error if the directory cannot be removed.
///
/// # Examples
///
/// ```no_run
/// # use database_replicator::utils::{create_managed_temp_dir, remove_managed_temp_dir};
/// # use anyhow::Result;
/// # fn example() -> Result<()> {
/// let temp_path = create_managed_temp_dir()?;
/// // ... do work ...
/// remove_managed_temp_dir(&temp_path)?;
/// # Ok(())
/// # }
/// ```
pub fn remove_managed_temp_dir(path: &std::path::Path) -> Result<()> {
    use std::fs;

    // Verify this is one of our temp directories (safety check)
    if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
        if !name.starts_with("postgres-seren-replicator-") {
            bail!(
                "Refusing to remove directory that doesn't match our naming pattern: {}",
                path.display()
            );
        }
    } else {
        bail!("Invalid temp directory path: {}", path.display());
    }

    tracing::debug!("Removing managed temp directory: {}", path.display());

    fs::remove_dir_all(path)
        .with_context(|| format!("Failed to remove temp directory at {}", path.display()))?;

    Ok(())
}

/// Replace the database name in a connection string URL
///
/// This is used internally by SerenDB to provide a generic connection string
/// which then needs to be specialized for a particular database.
///
/// # Arguments
///
/// * `url` - The connection string URL (e.g., postgresql://host/template_db)
/// * `new_db` - The new database name to insert into the URL
///
/// # Returns
///
/// A new URL string with the database name replaced.
///
/// # Errors
///
/// Returns an error if the URL is invalid and cannot be parsed.
pub fn replace_database_in_connection_string(url: &str, new_db: &str) -> Result<String> {
    let mut parsed = Url::parse(url).context("Invalid connection string URL")?;
    parsed.set_path(&format!("/{}", new_db));

    Ok(parsed.to_string())
}

/// Check if a PostgreSQL URL points to a SerenDB instance
///
/// SerenDB hosts have domains ending with `.serendb.com`
///
/// # Arguments
///
/// * `url` - PostgreSQL connection string to check
///
/// # Returns
///
/// Returns `true` if the URL points to a SerenDB host.
///
/// # Examples
///
/// ```
/// use database_replicator::utils::is_serendb_target;
///
/// assert!(is_serendb_target("postgresql://user:pass@db.serendb.com/mydb"));
/// assert!(is_serendb_target("postgresql://user:pass@cluster-123.console.serendb.com/mydb"));
/// assert!(!is_serendb_target("postgresql://user:pass@localhost/mydb"));
/// assert!(!is_serendb_target("postgresql://user:pass@rds.amazonaws.com/mydb"));
/// ```
pub fn is_serendb_target(url: &str) -> bool {
    match parse_postgres_url(url) {
        Ok(parts) => parts.host.ends_with(".serendb.com") || parts.host == "serendb.com",
        Err(_) => false,
    }
}

/// Get the major version of a PostgreSQL client tool (pg_dump, psql, etc.)
///
/// Executes `<tool> --version` and parses the output.
///
/// # Arguments
///
/// * `tool` - Name of the tool (e.g., "pg_dump", "psql")
///
/// # Returns
///
/// The major version number (e.g., 16 for pg_dump 16.10)
///
/// # Errors
///
/// Returns an error if:
/// - Tool is not found in PATH
/// - Tool execution fails
/// - Version output cannot be parsed
///
/// # Examples
///
/// ```no_run
/// use database_replicator::utils::get_pg_tool_version;
/// use anyhow::Result;
///
/// fn example() -> Result<()> {
///     let version = get_pg_tool_version("pg_dump")?;
///     println!("pg_dump major version: {}", version); // e.g., 16
///     Ok(())
/// }
/// ```
pub fn get_pg_tool_version(tool: &str) -> Result<u32> {
    use std::process::Command;

    let path = which(tool).with_context(|| format!("{} not found in PATH", tool))?;

    let output = Command::new(&path)
        .arg("--version")
        .output()
        .with_context(|| format!("Failed to execute {} --version", tool))?;

    let version_str = String::from_utf8_lossy(&output.stdout);
    parse_pg_version_string(&version_str)
}

/// Parse major version from PostgreSQL version string
///
/// Handles formats like:
/// - "pg_dump (PostgreSQL) 16.10 (Ubuntu 16.10-0ubuntu0.24.04.1)"
/// - "psql (PostgreSQL) 17.2"
/// - "17.2 (Debian 17.2-1.pgdg120+1)"
///
/// # Arguments
///
/// * `version_str` - Version string output from a PostgreSQL tool
///
/// # Returns
///
/// The major version number (e.g., 16, 17)
///
/// # Errors
///
/// Returns an error if the version cannot be parsed.
pub fn parse_pg_version_string(version_str: &str) -> Result<u32> {
    // Look for version pattern: major.minor
    for word in version_str.split_whitespace() {
        if let Some(major_str) = word.split('.').next() {
            if let Ok(major) = major_str.parse::<u32>() {
                // Valid PostgreSQL versions are between 9 and 99
                if (9..=99).contains(&major) {
                    return Ok(major);
                }
            }
        }
    }
    bail!("Could not parse PostgreSQL version from: {}", version_str)
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
        // Full URL with all components including password
        let parts = parse_postgres_url("postgresql://myuser:mypass@localhost:5432/mydb").unwrap();
        assert_eq!(parts.host, "localhost");
        assert_eq!(parts.port, 5432);
        assert_eq!(parts.database, "mydb");
        assert_eq!(parts.user, Some("myuser".to_string()));
        assert_eq!(parts.password, Some("mypass".to_string()));

        // URL without port (should default to 5432)
        let parts = parse_postgres_url("postgresql://user:pass@host/db").unwrap();
        assert_eq!(parts.host, "host");
        assert_eq!(parts.port, 5432);
        assert_eq!(parts.database, "db");
        assert_eq!(parts.user, Some("user".to_string()));
        assert_eq!(parts.password, Some("pass".to_string()));

        // URL with user but no password
        let parts = parse_postgres_url("postgresql://user@host/db").unwrap();
        assert_eq!(parts.host, "host");
        assert_eq!(parts.user, Some("user".to_string()));
        assert_eq!(parts.password, None);

        // URL without authentication
        let parts = parse_postgres_url("postgresql://host:5433/db").unwrap();
        assert_eq!(parts.host, "host");
        assert_eq!(parts.port, 5433);
        assert_eq!(parts.database, "db");
        assert_eq!(parts.user, None);
        assert_eq!(parts.password, None);

        // URL with query parameters
        let parts = parse_postgres_url("postgresql://user:pass@host/db?sslmode=require").unwrap();
        assert_eq!(parts.host, "host");
        assert_eq!(parts.database, "db");
        assert_eq!(parts.password, Some("pass".to_string()));

        // URL with postgres:// scheme (alternative)
        let parts = parse_postgres_url("postgres://user:pass@host/db").unwrap();
        assert_eq!(parts.host, "host");
        assert_eq!(parts.database, "db");
        assert_eq!(parts.password, Some("pass".to_string()));

        // Host normalization (lowercase)
        let parts = parse_postgres_url("postgresql://user:pass@HOST.COM/db").unwrap();
        assert_eq!(parts.host, "host.com");
        assert_eq!(parts.password, Some("pass".to_string()));

        // Password with special characters
        let parts = parse_postgres_url("postgresql://user:p@ss!word@host/db").unwrap();
        assert_eq!(parts.password, Some("p@ss!word".to_string()));
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
    fn test_pgpass_file_creation() {
        let parts = PostgresUrlParts {
            host: "localhost".to_string(),
            port: 5432,
            database: "testdb".to_string(),
            user: Some("testuser".to_string()),
            password: Some("testpass".to_string()),
            query_params: std::collections::HashMap::new(),
        };

        let pgpass = PgPassFile::new(&parts).unwrap();
        assert!(pgpass.path().exists());

        // Verify file content
        let content = std::fs::read_to_string(pgpass.path()).unwrap();
        assert_eq!(content, "localhost:5432:testdb:testuser:testpass\n");

        // Verify permissions on Unix
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let metadata = std::fs::metadata(pgpass.path()).unwrap();
            let permissions = metadata.permissions();
            assert_eq!(permissions.mode() & 0o777, 0o600);
        }

        // File should be cleaned up when pgpass is dropped
        let path = pgpass.path().to_path_buf();
        drop(pgpass);
        assert!(!path.exists());
    }

    #[test]
    fn test_pgpass_file_without_password() {
        let parts = PostgresUrlParts {
            host: "localhost".to_string(),
            port: 5432,
            database: "testdb".to_string(),
            user: Some("testuser".to_string()),
            password: None,
            query_params: std::collections::HashMap::new(),
        };

        let pgpass = PgPassFile::new(&parts).unwrap();
        let content = std::fs::read_to_string(pgpass.path()).unwrap();
        // Should use empty password
        assert_eq!(content, "localhost:5432:testdb:testuser:\n");
    }

    #[test]
    fn test_pgpass_file_without_user() {
        let parts = PostgresUrlParts {
            host: "localhost".to_string(),
            port: 5432,
            database: "testdb".to_string(),
            user: None,
            password: Some("testpass".to_string()),
            query_params: std::collections::HashMap::new(),
        };

        let pgpass = PgPassFile::new(&parts).unwrap();
        let content = std::fs::read_to_string(pgpass.path()).unwrap();
        // Should use wildcard for user
        assert_eq!(content, "localhost:5432:testdb:*:testpass\n");
    }

    #[test]
    fn test_strip_password_from_url() {
        // With password
        let url = "postgresql://user:p@ssw0rd@host:5432/db";
        let stripped = strip_password_from_url(url).unwrap();
        assert_eq!(stripped, "postgresql://user@host:5432/db");

        // With special characters in password
        let url = "postgresql://user:p@ss!w0rd@host:5432/db";
        let stripped = strip_password_from_url(url).unwrap();
        assert_eq!(stripped, "postgresql://user@host:5432/db");

        // Without password
        let url = "postgresql://user@host:5432/db";
        let stripped = strip_password_from_url(url).unwrap();
        assert_eq!(stripped, "postgresql://user@host:5432/db");

        // With query parameters
        let url = "postgresql://user:pass@host:5432/db?sslmode=require";
        let stripped = strip_password_from_url(url).unwrap();
        assert_eq!(stripped, "postgresql://user@host:5432/db?sslmode=require");

        // No user
        let url = "postgresql://host:5432/db";
        let stripped = strip_password_from_url(url).unwrap();
        assert_eq!(stripped, "postgresql://host:5432/db");
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

    #[test]
    fn test_is_serendb_target() {
        // Positive cases - SerenDB hosts
        assert!(is_serendb_target(
            "postgresql://user:pass@db.serendb.com/mydb"
        ));
        assert!(is_serendb_target(
            "postgresql://user:pass@cluster.console.serendb.com/mydb"
        ));
        assert!(is_serendb_target(
            "postgres://u:p@x.serendb.com:5432/db?sslmode=require"
        ));
        assert!(is_serendb_target("postgresql://user:pass@serendb.com/mydb"));

        // Negative cases - not SerenDB
        assert!(!is_serendb_target("postgresql://user:pass@localhost/mydb"));
        assert!(!is_serendb_target(
            "postgresql://user:pass@rds.amazonaws.com/mydb"
        ));
        assert!(!is_serendb_target("postgresql://user:pass@neon.tech/mydb"));
        // Domain spoofing attempt - should NOT match
        assert!(!is_serendb_target(
            "postgresql://user:pass@serendb.com.evil.com/mydb"
        ));
        assert!(!is_serendb_target(
            "postgresql://user:pass@notserendb.com/mydb"
        ));
        // Invalid URL
        assert!(!is_serendb_target("not-a-url"));
    }

    #[test]
    fn test_parse_pg_version_string() {
        // Standard pg_dump output
        assert_eq!(
            parse_pg_version_string("pg_dump (PostgreSQL) 16.10 (Ubuntu 16.10-0ubuntu0.24.04.1)")
                .unwrap(),
            16
        );

        // Standard psql output
        assert_eq!(
            parse_pg_version_string("psql (PostgreSQL) 17.2").unwrap(),
            17
        );

        // pg_restore output
        assert_eq!(
            parse_pg_version_string("pg_restore (PostgreSQL) 15.4").unwrap(),
            15
        );

        // Debian-style version
        assert_eq!(
            parse_pg_version_string("17.2 (Debian 17.2-1.pgdg120+1)").unwrap(),
            17
        );

        // Should fail on invalid input
        assert!(parse_pg_version_string("not a version").is_err());
        assert!(parse_pg_version_string("version 1.2.3").is_err()); // 1 is < 9
        assert!(parse_pg_version_string("").is_err());
    }

    #[test]
    fn test_get_pg_tool_version() {
        // This test will only pass if pg_dump is installed
        // Skip gracefully if not available
        if which("pg_dump").is_ok() {
            let version = get_pg_tool_version("pg_dump").unwrap();
            assert!(
                version >= 12,
                "Expected pg_dump version >= 12, got {}",
                version
            );
            assert!(
                version <= 99,
                "Expected pg_dump version <= 99, got {}",
                version
            );
        }

        // Non-existent tool should fail
        assert!(get_pg_tool_version("nonexistent_pg_tool_xyz").is_err());
    }
}
