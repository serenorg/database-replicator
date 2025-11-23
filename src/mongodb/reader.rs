// ABOUTME: MongoDB data reading functions for collection introspection and document retrieval
// ABOUTME: Provides read-only access to MongoDB collections with security validation

use anyhow::{Context, Result};
use bson::Document;
use mongodb::{Client, Database};

/// List all collection names in a MongoDB database
///
/// Retrieves names of all collections in the specified database.
/// System collections (starting with "system.") are excluded.
///
/// # Arguments
///
/// * `client` - MongoDB client connection
/// * `db_name` - Database name to list collections from
///
/// # Returns
///
/// Vector of collection names
///
/// # Security
///
/// Only lists user collections, system collections are excluded
///
/// # Examples
///
/// ```no_run
/// # use postgres_seren_replicator::mongodb::{connect_mongodb, reader::list_collections};
/// # async fn example() -> anyhow::Result<()> {
/// let client = connect_mongodb("mongodb://localhost:27017/mydb").await?;
/// let collections = list_collections(&client, "mydb").await?;
/// println!("Found {} collections", collections.len());
/// # Ok(())
/// # }
/// ```
pub async fn list_collections(client: &Client, db_name: &str) -> Result<Vec<String>> {
    tracing::info!("Listing collections in database '{}'", db_name);

    let database = client.database(db_name);

    let collection_names = database
        .list_collection_names(None)
        .await
        .with_context(|| format!("Failed to list collections in database '{}'", db_name))?;

    // Filter out system collections
    let user_collections: Vec<String> = collection_names
        .into_iter()
        .filter(|name| !name.starts_with("system."))
        .collect();

    tracing::debug!(
        "Found {} user collections in '{}'",
        user_collections.len(),
        db_name
    );

    Ok(user_collections)
}

/// Get document count for a MongoDB collection
///
/// Returns the total number of documents in the collection.
///
/// # Arguments
///
/// * `database` - MongoDB database reference
/// * `collection_name` - Collection name (must be validated)
///
/// # Returns
///
/// Number of documents in the collection
///
/// # Security
///
/// Collection name should be validated before calling this function.
///
/// # Examples
///
/// ```no_run
/// # use postgres_seren_replicator::mongodb::{connect_mongodb, reader::get_collection_count};
/// # use postgres_seren_replicator::jsonb::validate_table_name;
/// # async fn example() -> anyhow::Result<()> {
/// let client = connect_mongodb("mongodb://localhost:27017/mydb").await?;
/// let db = client.database("mydb");
/// let collection = "users";
/// validate_table_name(collection)?;
/// let count = get_collection_count(&db, collection).await?;
/// println!("Collection '{}' has {} documents", collection, count);
/// # Ok(())
/// # }
/// ```
pub async fn get_collection_count(database: &Database, collection_name: &str) -> Result<usize> {
    // Validate collection name to prevent injection
    crate::jsonb::validate_table_name(collection_name)
        .context("Invalid collection name for count query")?;

    tracing::debug!(
        "Getting document count for collection '{}'",
        collection_name
    );

    let collection = database.collection::<Document>(collection_name);

    let count = collection
        .estimated_document_count(None)
        .await
        .with_context(|| {
            format!(
                "Failed to count documents in collection '{}'",
                collection_name
            )
        })?;

    Ok(count as usize)
}

/// Read all documents from a MongoDB collection
///
/// Reads all documents from the collection and returns them as BSON documents.
/// For large collections, this may consume significant memory.
///
/// # Arguments
///
/// * `database` - MongoDB database reference
/// * `collection_name` - Collection name (must be validated)
///
/// # Returns
///
/// Vector of BSON documents from the collection
///
/// # Security
///
/// - Collection name is validated before querying
/// - Read-only operation, no modifications possible
///
/// # Examples
///
/// ```no_run
/// # use postgres_seren_replicator::mongodb::{connect_mongodb, reader::read_collection_data};
/// # use postgres_seren_replicator::jsonb::validate_table_name;
/// # async fn example() -> anyhow::Result<()> {
/// let client = connect_mongodb("mongodb://localhost:27017/mydb").await?;
/// let db = client.database("mydb");
/// let collection = "users";
/// validate_table_name(collection)?;
/// let documents = read_collection_data(&db, collection).await?;
/// println!("Read {} documents", documents.len());
/// # Ok(())
/// # }
/// ```
pub async fn read_collection_data(
    database: &Database,
    collection_name: &str,
) -> Result<Vec<Document>> {
    // Validate collection name to prevent injection
    crate::jsonb::validate_table_name(collection_name)
        .context("Invalid collection name for data reading")?;

    tracing::info!(
        "Reading all documents from collection '{}'",
        collection_name
    );

    let collection = database.collection::<Document>(collection_name);

    let mut cursor = collection
        .find(None, None)
        .await
        .with_context(|| format!("Failed to query collection '{}'", collection_name))?;

    let mut documents = Vec::new();

    use futures::stream::StreamExt;
    while let Some(result) = cursor.next().await {
        let document = result.with_context(|| {
            format!(
                "Failed to read document from collection '{}'",
                collection_name
            )
        })?;
        documents.push(document);
    }

    tracing::info!(
        "Read {} documents from collection '{}'",
        documents.len(),
        collection_name
    );

    Ok(documents)
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_validate_collection_names() {
        // Valid collection names should pass validation
        let valid_names = vec!["users", "user_events", "UserData", "_private"];

        for name in valid_names {
            let result = crate::jsonb::validate_table_name(name);
            assert!(
                result.is_ok(),
                "Valid collection name should pass: {}",
                name
            );
        }
    }

    #[test]
    fn test_reject_invalid_collection_names() {
        // Invalid collection names should be rejected
        let invalid_names = vec![
            "users; DROP DATABASE;",
            "users--",
            "select",
            "insert",
            "drop",
        ];

        for name in invalid_names {
            let result = crate::jsonb::validate_table_name(name);
            assert!(
                result.is_err(),
                "Invalid collection name should be rejected: {}",
                name
            );
        }
    }
}
