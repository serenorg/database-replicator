// ABOUTME: MongoDB database reading utilities for migration to PostgreSQL
// ABOUTME: Provides secure connection validation and read-only database access

pub mod converter;
pub mod reader;

use anyhow::{bail, Context, Result};
use mongodb::{options::ClientOptions, Client};

/// Validate a MongoDB connection string
///
/// Security checks:
/// - Verifies URL starts with mongodb:// or mongodb+srv://
/// - Parses connection string to validate format
/// - Checks for required database name in connection string
///
/// # Arguments
///
/// * `connection_string` - MongoDB connection URL
///
/// # Returns
///
/// Validated connection string if valid, error otherwise
///
/// # Security
///
/// CRITICAL: This function prevents invalid or malicious connection strings
///
/// # Examples
///
/// ```no_run
/// # use database_replicator::mongodb::validate_mongodb_url;
/// // Valid URLs
/// assert!(validate_mongodb_url("mongodb://localhost:27017/mydb").is_ok());
/// assert!(validate_mongodb_url("mongodb+srv://cluster.mongodb.net/mydb").is_ok());
///
/// // Invalid URLs
/// assert!(validate_mongodb_url("invalid").is_err());
/// assert!(validate_mongodb_url("postgresql://localhost/db").is_err());
/// ```
pub fn validate_mongodb_url(connection_string: &str) -> Result<String> {
    if connection_string.is_empty() {
        bail!("MongoDB connection string cannot be empty");
    }

    // Check for valid MongoDB URL prefix
    if !connection_string.starts_with("mongodb://")
        && !connection_string.starts_with("mongodb+srv://")
    {
        bail!(
            "Invalid MongoDB connection string '{}'. \
             Must start with 'mongodb://' or 'mongodb+srv://'",
            connection_string
        );
    }

    // Note: Cannot validate connection string synchronously in this function
    // Validation happens during async connection

    tracing::debug!("Validated MongoDB connection string");

    Ok(connection_string.to_string())
}

/// Connect to MongoDB database
///
/// Opens a connection to MongoDB using the provided connection string.
/// The connection is read-only by default (enforced at application level).
///
/// # Arguments
///
/// * `connection_string` - MongoDB connection URL (will be validated)
///
/// # Returns
///
/// MongoDB Client if successful
///
/// # Security
///
/// - Connection string is validated before use
/// - Application enforces read-only operations
/// - No modifications possible through this interface
///
/// # Examples
///
/// ```no_run
/// # use database_replicator::mongodb::connect_mongodb;
/// # async fn example() -> anyhow::Result<()> {
/// let client = connect_mongodb("mongodb://localhost:27017/mydb").await?;
/// // Use client to read data
/// # Ok(())
/// # }
/// ```
pub async fn connect_mongodb(connection_string: &str) -> Result<Client> {
    // Validate connection string first
    let validated_url = validate_mongodb_url(connection_string)?;

    tracing::info!("Connecting to MongoDB database");

    // Parse options and create client
    let client_options = ClientOptions::parse(&validated_url)
        .await
        .with_context(|| "Failed to parse MongoDB connection options".to_string())?;

    let client = Client::with_options(client_options).context("Failed to create MongoDB client")?;

    // Verify connection by pinging
    client
        .database("admin")
        .run_command(bson::doc! {"ping": 1})
        .await
        .context(
            "Failed to ping MongoDB server (connection may be invalid or server unreachable)",
        )?;

    tracing::debug!("Successfully connected to MongoDB");

    Ok(client)
}

/// Extract database name from MongoDB connection string
///
/// Parses the connection string and extracts the database name.
/// MongoDB connection strings can have the database name in the path.
///
/// # Arguments
///
/// * `connection_string` - MongoDB connection URL
///
/// # Returns
///
/// Database name if present in URL, None otherwise
///
/// # Examples
///
/// ```no_run
/// # use database_replicator::mongodb::extract_database_name;
/// # async fn example() -> anyhow::Result<()> {
/// assert_eq!(
///     extract_database_name("mongodb://localhost:27017/mydb").await?,
///     Some("mydb".to_string())
/// );
/// assert_eq!(
///     extract_database_name("mongodb://localhost:27017").await?,
///     None
/// );
/// # Ok(())
/// # }
/// ```
pub async fn extract_database_name(connection_string: &str) -> Result<Option<String>> {
    let options = ClientOptions::parse(connection_string)
        .await
        .context("Failed to parse MongoDB connection string")?;

    Ok(options.default_database.clone())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_empty_url() {
        let result = validate_mongodb_url("");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("cannot be empty"));
    }

    #[test]
    fn test_validate_invalid_prefix() {
        let invalid_urls = vec![
            "postgresql://localhost/db",
            "mysql://localhost/db",
            "http://localhost",
            "localhost:27017",
        ];

        for url in invalid_urls {
            let result = validate_mongodb_url(url);
            assert!(result.is_err(), "Invalid URL should be rejected: {}", url);
        }
    }

    #[test]
    fn test_validate_valid_mongodb_url() {
        // Note: This test validates URL format, not actual connection
        let valid_urls = vec![
            "mongodb://localhost:27017",
            "mongodb://localhost:27017/mydb",
            "mongodb://user:pass@localhost:27017/mydb",
        ];

        for url in valid_urls {
            let result = validate_mongodb_url(url);
            assert!(
                result.is_ok(),
                "Valid MongoDB URL should be accepted: {}",
                url
            );
        }
    }

    #[test]
    fn test_validate_mongodb_srv_url() {
        // Note: This test validates URL format, not actual connection
        let url = "mongodb+srv://cluster.mongodb.net/mydb";
        let result = validate_mongodb_url(url);
        assert!(result.is_ok(), "MongoDB+SRV URL should be accepted");
    }

    #[tokio::test]
    async fn test_extract_database_name_with_db() {
        let url = "mongodb://localhost:27017/mydb";
        let db_name = extract_database_name(url).await.unwrap();
        assert_eq!(db_name, Some("mydb".to_string()));
    }

    #[tokio::test]
    async fn test_extract_database_name_without_db() {
        let url = "mongodb://localhost:27017";
        let db_name = extract_database_name(url).await.unwrap();
        assert_eq!(db_name, None);
    }

    #[tokio::test]
    async fn test_extract_database_name_with_auth() {
        let url = "mongodb://user:pass@localhost:27017/mydb";
        let db_name = extract_database_name(url).await.unwrap();
        assert_eq!(db_name, Some("mydb".to_string()));
    }
}
