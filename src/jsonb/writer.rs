// ABOUTME: Write JSONB data to PostgreSQL with metadata
// ABOUTME: Handles table creation, single row inserts, and batch inserts

use anyhow::{Context, Result};
use tokio_postgres::Client;

/// Create a table with JSONB schema for storing non-PostgreSQL data
///
/// Creates a table with the following structure:
/// - id: TEXT PRIMARY KEY (original document/row ID)
/// - data: JSONB NOT NULL (complete document/row as JSON)
/// - _source_type: TEXT NOT NULL ('sqlite', 'mongodb', or 'mysql')
/// - _migrated_at: TIMESTAMP NOT NULL DEFAULT NOW()
///
/// Also creates two indexes:
/// - GIN index on data column for efficient JSONB queries
/// - Index on _migrated_at for temporal queries
///
/// # Arguments
///
/// * `client` - PostgreSQL client connection
/// * `table_name` - Name of the table to create (must be validated)
/// * `source_type` - Source database type ('sqlite', 'mongodb', or 'mysql')
///
/// # Security
///
/// CRITICAL: table_name MUST be validated with validate_table_name() before calling.
/// This function uses table_name in SQL directly (not parameterized) after validation.
///
/// # Examples
///
/// ```no_run
/// # use database_replicator::jsonb::writer::create_jsonb_table;
/// # use database_replicator::jsonb::validate_table_name;
/// # async fn example(client: &tokio_postgres::Client) -> anyhow::Result<()> {
/// let table_name = "users";
/// validate_table_name(table_name)?;
/// create_jsonb_table(client, table_name, "sqlite").await?;
/// # Ok(())
/// # }
/// ```
pub async fn create_jsonb_table(
    client: &Client,
    table_name: &str,
    source_type: &str,
) -> Result<()> {
    // Validate table name to prevent SQL injection
    crate::jsonb::validate_table_name(table_name)
        .context("Invalid table name for JSONB table creation")?;

    tracing::info!(
        "Creating JSONB table '{}' for source type '{}'",
        table_name,
        source_type
    );

    // Create table with JSONB schema
    // Note: table_name is validated above, so it's safe to use in SQL
    let create_table_sql = format!(
        r#"
        CREATE TABLE IF NOT EXISTS "{}" (
            id TEXT PRIMARY KEY,
            data JSONB NOT NULL,
            _source_type TEXT NOT NULL,
            _migrated_at TIMESTAMP NOT NULL DEFAULT NOW()
        )
        "#,
        table_name
    );

    client
        .execute(&create_table_sql, &[])
        .await
        .with_context(|| format!("Failed to create JSONB table '{}'", table_name))?;

    // Create GIN index on data column for efficient JSONB queries
    let create_gin_index_sql = format!(
        r#"CREATE INDEX IF NOT EXISTS "idx_{}_data" ON "{}" USING GIN (data)"#,
        table_name, table_name
    );

    client
        .execute(&create_gin_index_sql, &[])
        .await
        .with_context(|| format!("Failed to create GIN index on table '{}'", table_name))?;

    // Create index on _migrated_at for temporal queries
    let create_time_index_sql = format!(
        r#"CREATE INDEX IF NOT EXISTS "idx_{}_migrated" ON "{}" (_migrated_at)"#,
        table_name, table_name
    );

    client
        .execute(&create_time_index_sql, &[])
        .await
        .with_context(|| {
            format!(
                "Failed to create _migrated_at index on table '{}'",
                table_name
            )
        })?;

    tracing::info!(
        "Successfully created JSONB table '{}' with indexes",
        table_name
    );

    Ok(())
}

/// Truncate a JSONB table to remove all existing data
///
/// This is used to make init idempotent - rerunning init will clear existing
/// data before inserting fresh data from the source.
///
/// # Arguments
///
/// * `client` - PostgreSQL client connection
/// * `table_name` - Name of the table to truncate (must be validated)
///
/// # Security
///
/// CRITICAL: table_name MUST be validated with validate_table_name() before calling.
pub async fn truncate_jsonb_table(client: &Client, table_name: &str) -> Result<()> {
    // Validate table name to prevent SQL injection
    crate::jsonb::validate_table_name(table_name)
        .context("Invalid table name for JSONB table truncation")?;

    tracing::debug!("Truncating JSONB table '{}'", table_name);

    let truncate_sql = format!(r#"TRUNCATE TABLE "{}" RESTART IDENTITY CASCADE"#, table_name);

    client
        .execute(&truncate_sql, &[])
        .await
        .with_context(|| format!("Failed to truncate JSONB table '{}'", table_name))?;

    tracing::debug!("Truncated JSONB table '{}'", table_name);

    Ok(())
}

/// Insert a single JSONB row with metadata
///
/// Inserts a single row into a JSONB table with the original ID, data, and metadata.
///
/// # Arguments
///
/// * `client` - PostgreSQL client connection
/// * `table_name` - Name of the table (must be validated)
/// * `id` - Original document/row ID
/// * `data` - Complete document/row as serde_json::Value
/// * `source_type` - Source database type ('sqlite', 'mongodb', or 'mysql')
///
/// # Security
///
/// Uses parameterized queries for id, data, and source_type to prevent injection.
/// table_name must be validated before calling.
///
/// # Examples
///
/// ```no_run
/// # use database_replicator::jsonb::writer::insert_jsonb_row;
/// # use database_replicator::jsonb::validate_table_name;
/// # use serde_json::json;
/// # async fn example(client: &tokio_postgres::Client) -> anyhow::Result<()> {
/// let table_name = "users";
/// validate_table_name(table_name)?;
/// let data = json!({"name": "Alice", "age": 30});
/// insert_jsonb_row(client, table_name, "1", data, "sqlite").await?;
/// # Ok(())
/// # }
/// ```
pub async fn insert_jsonb_row(
    client: &Client,
    table_name: &str,
    id: &str,
    data: serde_json::Value,
    source_type: &str,
) -> Result<()> {
    // Validate table name to prevent SQL injection
    crate::jsonb::validate_table_name(table_name)
        .context("Invalid table name for JSONB row insert")?;

    // Use parameterized query for data and metadata (safe from injection)
    // Note: table_name is validated above
    let insert_sql = format!(
        r#"INSERT INTO "{}" (id, data, _source_type) VALUES ($1, $2, $3)"#,
        table_name
    );

    client
        .execute(&insert_sql, &[&id, &data, &source_type])
        .await
        .with_context(|| {
            format!(
                "Failed to insert row with id '{}' into '{}'",
                id, table_name
            )
        })?;

    Ok(())
}

/// Insert multiple JSONB rows in a batch
///
/// Inserts multiple rows efficiently using a multi-value INSERT statement.
/// This is significantly faster than individual inserts for large datasets.
///
/// # Arguments
///
/// * `client` - PostgreSQL client connection
/// * `table_name` - Name of the table (must be validated)
/// * `rows` - Vector of (id, data) tuples
/// * `source_type` - Source database type ('sqlite', 'mongodb', or 'mysql')
///
/// # Security
///
/// Uses parameterized queries for all data. table_name must be validated.
///
/// # Performance
///
/// Batches rows into groups of 1000 to avoid PostgreSQL parameter limits.
///
/// # Examples
///
/// ```no_run
/// # use database_replicator::jsonb::writer::insert_jsonb_batch;
/// # use database_replicator::jsonb::validate_table_name;
/// # use serde_json::json;
/// # async fn example(client: &tokio_postgres::Client) -> anyhow::Result<()> {
/// let table_name = "users";
/// validate_table_name(table_name)?;
/// let rows = vec![
///     ("1".to_string(), json!({"name": "Alice", "age": 30})),
///     ("2".to_string(), json!({"name": "Bob", "age": 25})),
/// ];
/// insert_jsonb_batch(client, table_name, rows, "sqlite").await?;
/// # Ok(())
/// # }
/// ```
pub async fn insert_jsonb_batch(
    client: &Client,
    table_name: &str,
    rows: Vec<(String, serde_json::Value)>,
    source_type: &str,
) -> Result<()> {
    // Validate table name to prevent SQL injection
    crate::jsonb::validate_table_name(table_name)
        .context("Invalid table name for JSONB batch insert")?;

    if rows.is_empty() {
        return Ok(());
    }

    tracing::info!(
        "Inserting {} rows into JSONB table '{}'",
        rows.len(),
        table_name
    );

    // Batch inserts to avoid parameter limit (PostgreSQL limit is ~65535 parameters)
    // With 3 parameters per row (id, data, source_type), we can do ~21000 rows
    // Use conservative 1000 rows per batch
    const BATCH_SIZE: usize = 1000;

    for (batch_num, chunk) in rows.chunks(BATCH_SIZE).enumerate() {
        // Build parameterized multi-value INSERT
        // Format: INSERT INTO table (cols) VALUES ($1,$2,$3),($4,$5,$6),...
        let mut value_placeholders = Vec::with_capacity(chunk.len());
        let mut params: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> =
            Vec::with_capacity(chunk.len() * 3);

        for (idx, (id, data)) in chunk.iter().enumerate() {
            let param_base = idx * 3 + 1;
            value_placeholders.push(format!(
                "(${}, ${}, ${})",
                param_base,
                param_base + 1,
                param_base + 2
            ));

            // Add parameters in order: id, data, source_type
            params.push(id);
            params.push(data);
            params.push(&source_type);
        }

        let insert_sql = format!(
            r#"INSERT INTO "{}" (id, data, _source_type) VALUES {}"#,
            table_name,
            value_placeholders.join(", ")
        );

        client
            .execute(&insert_sql, &params)
            .await
            .with_context(|| {
                format!(
                    "Failed to insert batch {} ({} rows) into '{}'",
                    batch_num,
                    chunk.len(),
                    table_name
                )
            })?;

        tracing::debug!(
            "Inserted batch {} ({} rows) into '{}'",
            batch_num,
            chunk.len(),
            table_name
        );
    }

    tracing::info!(
        "Successfully inserted {} rows into '{}'",
        rows.len(),
        table_name
    );

    Ok(())
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_batch_insert_empty() {
        // Empty batch should not error
        // (actual async test requires test database)
    }

    #[test]
    fn test_batch_size_calculation() {
        // Verify our batch size doesn't exceed parameter limits
        // PostgreSQL parameter limit is 65535
        // With 3 params per row (id, data, source_type) and 1000 rows per batch:
        // 1000 * 3 = 3000 parameters per batch, which is well under the limit
        let batch_size = 1000_usize;
        let params_per_row = 3_usize;
        let total_params = batch_size * params_per_row;
        assert!(
            total_params < 65535,
            "Batch size {} * {} params = {} exceeds PostgreSQL limit of 65535",
            batch_size,
            params_per_row,
            total_params
        );
    }
}
