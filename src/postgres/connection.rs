// ABOUTME: PostgreSQL connection utilities for Neon and Seren
// ABOUTME: Handles connection string parsing, TLS setup, and connection lifecycle

use anyhow::{Context, Result};
use native_tls::TlsConnector;
use postgres_native_tls::MakeTlsConnector;
use tokio_postgres::Client;

/// Connect to PostgreSQL database with TLS support
pub async fn connect(connection_string: &str) -> Result<Client> {
    // Parse connection string
    let _config = connection_string
        .parse::<tokio_postgres::Config>()
        .context(
        "Invalid connection string format. Expected: postgresql://user:password@host:port/database",
    )?;

    // Set up TLS connector for cloud connections
    let tls_connector = TlsConnector::builder()
        .danger_accept_invalid_certs(false)
        .build()
        .context("Failed to build TLS connector")?;
    let tls = MakeTlsConnector::new(tls_connector);

    // Connect
    let (client, connection) = tokio_postgres::connect(connection_string, tls)
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
                anyhow::anyhow!(
                    "TLS/SSL error: Failed to establish secure connection.\n\
                     Please verify SSL/TLS configuration.\n\
                     Error: {}",
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

#[cfg(test)]
mod tests {
    use super::*;

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
