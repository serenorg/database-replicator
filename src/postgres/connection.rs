// ABOUTME: PostgreSQL connection utilities for Neon and Seren
// ABOUTME: Handles connection string parsing, TLS setup, and connection lifecycle

use crate::utils;
use anyhow::{Context, Result};
use native_tls::TlsConnector;
use postgres_native_tls::MakeTlsConnector;
use std::time::Duration;
use tokio_postgres::Client;

/// Add TCP keepalive parameters to a PostgreSQL connection string
///
/// Automatically adds keepalive parameters to prevent idle connection timeouts
/// when connecting through load balancers (like AWS ELB). These parameters ensure
/// that TCP keepalive packets are sent regularly to keep the connection alive.
///
/// Parameters added:
/// - `keepalives=1`: Enable TCP keepalives
/// - `keepalives_idle=60`: Send first keepalive after 60 seconds of idle time
/// - `keepalives_interval=10`: Send subsequent keepalives every 10 seconds
///
/// If any of these parameters already exist in the connection string, they are
/// not overwritten.
///
/// # Arguments
///
/// * `connection_string` - Original PostgreSQL connection URL
///
/// # Returns
///
/// Connection string with keepalive parameters added
///
/// # Examples
///
/// ```
/// # use database_replicator::postgres::connection::add_keepalive_params;
/// let url = "postgresql://user:pass@host:5432/db";
/// let url_with_keepalives = add_keepalive_params(url);
/// assert!(url_with_keepalives.contains("keepalives=1"));
/// assert!(url_with_keepalives.contains("keepalives_idle=60"));
/// assert!(url_with_keepalives.contains("keepalives_interval=10"));
/// ```
pub fn add_keepalive_params(connection_string: &str) -> String {
    // Parse to check if params already exist
    let has_query = connection_string.contains('?');
    let lower = connection_string.to_lowercase();

    let needs_keepalives = !lower.contains("keepalives=");
    let needs_idle = !lower.contains("keepalives_idle=");
    let needs_interval = !lower.contains("keepalives_interval=");

    // If all params already exist, return as-is
    if !needs_keepalives && !needs_idle && !needs_interval {
        return connection_string.to_string();
    }

    let mut url = connection_string.to_string();
    let separator = if has_query { "&" } else { "?" };

    // Add missing keepalive parameters
    let mut params = Vec::new();
    if needs_keepalives {
        params.push("keepalives=1");
    }
    if needs_idle {
        params.push("keepalives_idle=60");
    }
    if needs_interval {
        params.push("keepalives_interval=10");
    }

    if !params.is_empty() {
        url.push_str(separator);
        url.push_str(&params.join("&"));
    }

    url
}

/// Connect to PostgreSQL database with TLS support
///
/// Establishes a connection using the provided connection string with TLS enabled.
/// The connection lifecycle is managed automatically via tokio spawn.
///
/// **Automatic Keepalive:** This function automatically adds TCP keepalive parameters
/// to prevent idle connection timeouts when connecting through load balancers.
/// The following parameters are added if not already present:
/// - `keepalives=1`
/// - `keepalives_idle=60`
/// - `keepalives_interval=10`
///
/// # Arguments
///
/// * `connection_string` - PostgreSQL URL (e.g., "postgresql://user:pass@host:5432/db")
///
/// # Returns
///
/// Returns a `Client` on success, or an error with context if connection fails.
///
/// # Errors
///
/// This function will return an error if:
/// - The connection string format is invalid
/// - Authentication fails (invalid username or password)
/// - The database does not exist
/// - The database server is unreachable
/// - TLS negotiation fails
/// - Connection times out
/// - pg_hba.conf does not allow the connection
///
/// # Examples
///
/// ```no_run
/// # use anyhow::Result;
/// # use database_replicator::postgres::connect;
/// # async fn example() -> Result<()> {
/// let client = connect("postgresql://user:pass@localhost:5432/mydb").await?;
/// # Ok(())
/// # }
/// ```
pub async fn connect(connection_string: &str) -> Result<Client> {
    // Add keepalive parameters to prevent idle connection timeouts
    let connection_string_with_keepalive = add_keepalive_params(connection_string);

    // Parse connection string
    let _config = connection_string_with_keepalive
        .parse::<tokio_postgres::Config>()
        .context(
        "Invalid connection string format. Expected: postgresql://user:password@host:port/database",
    )?;

    // Set up TLS connector for cloud connections
    // TEMPORARY: Accept invalid certs to debug TLS issues
    // TODO: Remove this once we identify the certificate validation issue
    let tls_connector = TlsConnector::builder()
        .danger_accept_invalid_certs(true)
        .build()
        .context("Failed to build TLS connector")?;
    let tls = MakeTlsConnector::new(tls_connector);

    // Connect with keepalive parameters
    let (client, connection) = tokio_postgres::connect(&connection_string_with_keepalive, tls)
        .await
        .map_err(|e| {
            // Parse error and provide helpful context
            let error_msg = e.to_string();

            if error_msg.contains("password authentication failed") {
                anyhow::anyhow!(
                    "Authentication failed: Invalid username or password.\n\
                     Please verify your database credentials."
                )
            } else if error_msg.contains("database") && error_msg.contains("does not exist") {
                anyhow::anyhow!(
                    "Database does not exist: {}\n\
                     Please create the database first or check the connection URL.",
                    error_msg
                )
            } else if error_msg.contains("Connection refused")
                || error_msg.contains("could not connect")
            {
                anyhow::anyhow!(
                    "Connection refused: Unable to reach database server.\n\
                     Please check:\n\
                     - The host and port are correct\n\
                     - The database server is running\n\
                     - Firewall rules allow connections\n\
                     Error: {}",
                    error_msg
                )
            } else if error_msg.contains("timeout") || error_msg.contains("timed out") {
                anyhow::anyhow!(
                    "Connection timeout: Database server did not respond in time.\n\
                     This could indicate network issues or server overload.\n\
                     Error: {}",
                    error_msg
                )
            } else if error_msg.contains("SSL") || error_msg.contains("TLS") {
                // Log full error for debugging TLS issues
                tracing::error!("TLS/SSL connection failed with error: {:?}", e);
                anyhow::anyhow!(
                    "TLS/SSL error: Failed to establish secure connection.\n\
                     Please verify SSL/TLS configuration.\n\
                     Detailed error: {:?}\n\
                     Original error: {}",
                    e,
                    error_msg
                )
            } else if error_msg.contains("no pg_hba.conf entry") {
                anyhow::anyhow!(
                    "Access denied: No pg_hba.conf entry for host.\n\
                     The database server is not configured to accept connections from your host.\n\
                     Contact your database administrator to update pg_hba.conf.\n\
                     Error: {}",
                    error_msg
                )
            } else {
                anyhow::anyhow!("Failed to connect to database: {}", error_msg)
            }
        })?;

    // Spawn connection handler
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            tracing::error!("Connection error: {}", e);
        }
    });

    Ok(client)
}

/// Connect to PostgreSQL with automatic retry for transient failures
///
/// Attempts to connect up to 3 times with exponential backoff (1s, 2s, 4s).
/// Useful for handling temporary network issues or server restarts.
///
/// # Arguments
///
/// * `connection_string` - PostgreSQL URL
///
/// # Returns
///
/// Returns a `Client` after successful connection, or error after all retries exhausted.
///
/// # Errors
///
/// Returns the last connection error if all retry attempts fail.
///
/// # Examples
///
/// ```no_run
/// # use anyhow::Result;
/// # use database_replicator::postgres::connection::connect_with_retry;
/// # async fn example() -> Result<()> {
/// let client = connect_with_retry("postgresql://user:pass@localhost:5432/mydb").await?;
/// # Ok(())
/// # }
/// ```
pub async fn connect_with_retry(connection_string: &str) -> Result<Client> {
    utils::retry_with_backoff(
        || connect(connection_string),
        3,                      // Max 3 retries
        Duration::from_secs(1), // Start with 1 second delay
    )
    .await
    .context("Failed to connect after retries")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_add_keepalive_params_to_url_without_query() {
        let url = "postgresql://user:pass@host:5432/db";
        let result = add_keepalive_params(url);

        assert!(result.contains("keepalives=1"));
        assert!(result.contains("keepalives_idle=60"));
        assert!(result.contains("keepalives_interval=10"));
        assert!(result.starts_with("postgresql://user:pass@host:5432/db?"));
    }

    #[test]
    fn test_add_keepalive_params_to_url_with_existing_query() {
        let url = "postgresql://user:pass@host:5432/db?sslmode=require";
        let result = add_keepalive_params(url);

        assert!(result.contains("keepalives=1"));
        assert!(result.contains("keepalives_idle=60"));
        assert!(result.contains("keepalives_interval=10"));
        assert!(result.contains("sslmode=require"));
        // Should use & separator not ?
        assert!(result.contains("&keepalives=1"));
    }

    #[test]
    fn test_add_keepalive_params_already_present() {
        let url =
            "postgresql://user:pass@host:5432/db?keepalives=1&keepalives_idle=60&keepalives_interval=10";
        let result = add_keepalive_params(url);

        // Should return unchanged
        assert_eq!(result, url);
    }

    #[test]
    fn test_add_keepalive_params_partial_existing() {
        let url = "postgresql://user:pass@host:5432/db?keepalives=1";
        let result = add_keepalive_params(url);

        // Should only add missing params
        assert!(result.contains("keepalives=1"));
        assert!(result.contains("keepalives_idle=60"));
        assert!(result.contains("keepalives_interval=10"));
        // Should not duplicate keepalives=1
        assert_eq!(result.matches("keepalives=1").count(), 1);
    }

    #[test]
    fn test_add_keepalive_params_case_insensitive() {
        let url = "postgresql://user:pass@host:5432/db?KEEPALIVES=1";
        let result = add_keepalive_params(url);

        // Should detect uppercase params and still add the missing ones
        assert!(result.contains("KEEPALIVES=1"));
        assert!(result.contains("keepalives_idle=60"));
        assert!(result.contains("keepalives_interval=10"));
        // Should not add lowercase keepalives=1 because KEEPALIVES=1 already exists
        let lower_result = result.to_lowercase();
        assert_eq!(lower_result.matches("keepalives=1").count(), 1);
    }

    #[tokio::test]
    async fn test_connect_with_invalid_url_returns_error() {
        let result = connect("invalid-url").await;
        assert!(result.is_err());
    }

    // NOTE: This test requires a real PostgreSQL instance
    // Skip if TEST_DATABASE_URL is not set
    #[tokio::test]
    #[ignore]
    async fn test_connect_with_valid_url_succeeds() {
        let url = std::env::var("TEST_DATABASE_URL")
            .expect("TEST_DATABASE_URL must be set for integration tests");

        let result = connect(&url).await;
        assert!(result.is_ok());
    }
}
