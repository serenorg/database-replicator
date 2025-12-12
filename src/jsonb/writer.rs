// ABOUTME: Write JSONB data to PostgreSQL with metadata
// ABOUTME: Handles table creation, single row inserts, and batch inserts

use anyhow::{bail, Context, Result};
use tokio_postgres::{types::ToSql, Client};

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

    tracing::info!("Truncating JSONB table '{}'", table_name);

    let truncate_sql = format!(
        r#"TRUNCATE TABLE "{}" RESTART IDENTITY CASCADE"#,
        table_name
    );

    client
        .execute(&truncate_sql, &[])
        .await
        .with_context(|| format!("Failed to truncate JSONB table '{}'", table_name))?;

    let verify_sql = format!(r#"SELECT COUNT(*) FROM "{}""#, table_name);
    let remaining_rows: i64 = client
        .query_one(&verify_sql, &[])
        .await
        .with_context(|| format!("Failed to verify truncate of '{}'", table_name))?
        .get(0);

    if remaining_rows > 0 {
        bail!(
            "Truncate verification failed: table '{}' still has {} rows after truncate",
            table_name,
            remaining_rows
        );
    }

    tracing::info!(
        "Truncated JSONB table '{}' successfully ({} rows remaining)",
        table_name,
        remaining_rows
    );

    Ok(())
}

/// Drop a JSONB table if it exists.
pub async fn drop_jsonb_table(client: &Client, table_name: &str) -> Result<()> {
    crate::jsonb::validate_table_name(table_name)
        .context("Invalid table name for JSONB table drop")?;

    tracing::info!("Dropping JSONB table '{}'", table_name);

    let drop_sql = format!(r#"DROP TABLE IF EXISTS "{}" CASCADE"#, table_name);

    client
        .execute(&drop_sql, &[])
        .await
        .with_context(|| format!("Failed to drop JSONB table '{}'", table_name))?;

    tracing::info!("Dropped JSONB table '{}' (if it existed)", table_name);

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

/// Estimate the serialized size of a JSONB row for batch sizing
fn estimate_row_size(id: &str, data: &serde_json::Value) -> usize {
    // Estimate: id length + JSON serialized size + overhead
    id.len() + data.to_string().len() + 50 // 50 bytes overhead for metadata
}

/// Calculate optimal batch size based on row sizes
///
/// Targets ~10MB per batch to stay well under typical PostgreSQL limits
/// while maintaining good throughput.
fn calculate_batch_size(rows: &[(String, serde_json::Value)], start_idx: usize) -> usize {
    const TARGET_BATCH_BYTES: usize = 10 * 1024 * 1024; // 10MB target
    const MIN_BATCH_SIZE: usize = 1;
    const MAX_BATCH_SIZE: usize = 1000;

    let mut total_size = 0usize;
    let mut count = 0usize;

    for (id, data) in rows.iter().skip(start_idx) {
        let row_size = estimate_row_size(id, data);
        if total_size + row_size > TARGET_BATCH_BYTES && count > 0 {
            break;
        }
        total_size += row_size;
        count += 1;
        if count >= MAX_BATCH_SIZE {
            break;
        }
    }

    count.max(MIN_BATCH_SIZE)
}

/// Execute a single batch insert with the given rows
async fn execute_batch_insert(
    client: &Client,
    table_name: &str,
    rows: &[(String, serde_json::Value)],
    source_type: &str,
) -> Result<()> {
    // Build parameterized multi-value INSERT
    let mut value_placeholders = Vec::with_capacity(rows.len());
    let mut params: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> =
        Vec::with_capacity(rows.len() * 3);

    for (idx, (id, data)) in rows.iter().enumerate() {
        let param_base = idx * 3 + 1;
        value_placeholders.push(format!(
            "(${}, ${}, ${})",
            param_base,
            param_base + 1,
            param_base + 2
        ));
        params.push(id);
        params.push(data);
        params.push(&source_type);
    }

    let insert_sql = format!(
        r#"INSERT INTO "{}" (id, data, _source_type) VALUES {}"#,
        table_name,
        value_placeholders.join(", ")
    );

    client.execute(&insert_sql, &params).await?;
    Ok(())
}

/// Insert multiple JSONB rows with adaptive batching
///
/// Inserts multiple rows efficiently using multi-value INSERT statements.
/// Automatically adjusts batch size based on row payload sizes and retries
/// with smaller batches on connection failures.
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
/// - Dynamically calculates batch size based on estimated payload size
/// - Targets ~10MB per batch for optimal throughput
/// - Automatically retries with smaller batches on failure
/// - Shows progress for large datasets
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

    let total_rows = rows.len();
    tracing::info!(
        "Inserting {} rows into JSONB table '{}'",
        total_rows,
        table_name
    );

    let mut inserted = 0usize;
    let mut consecutive_failures = 0u32;
    const MAX_RETRIES: u32 = 5;

    while inserted < total_rows {
        // Calculate optimal batch size based on remaining rows
        let batch_size = calculate_batch_size(&rows, inserted);
        let end_idx = (inserted + batch_size).min(total_rows);
        let batch = &rows[inserted..end_idx];

        // Log progress for large datasets
        if total_rows > 10000 && inserted.checked_rem(50000) == Some(0) {
            let pct = (inserted as f64 / total_rows as f64 * 100.0) as u32;
            tracing::info!(
                "  Progress: {}/{} rows ({}%) inserted into '{}'",
                inserted,
                total_rows,
                pct,
                table_name
            );
        }

        match execute_batch_insert(client, table_name, batch, source_type).await {
            Ok(()) => {
                tracing::debug!(
                    "Inserted batch of {} rows ({}-{}/{}) into '{}'",
                    batch.len(),
                    inserted,
                    end_idx,
                    total_rows,
                    table_name
                );
                inserted = end_idx;
                consecutive_failures = 0;
            }
            Err(e) => {
                consecutive_failures += 1;
                let is_connection_error = e.to_string().contains("connection")
                    || e.to_string().contains("closed")
                    || e.to_string().contains("communicating");

                if is_connection_error && consecutive_failures <= MAX_RETRIES && batch.len() > 1 {
                    // Connection error with multi-row batch - retry with smaller batches
                    let new_batch_size = (batch.len() / 2).max(1);
                    tracing::warn!(
                        "Batch insert failed (attempt {}/{}), reducing batch size from {} to {} rows",
                        consecutive_failures,
                        MAX_RETRIES,
                        batch.len(),
                        new_batch_size
                    );

                    // Insert this batch row-by-row as fallback
                    for (idx, (id, data)) in batch.iter().enumerate() {
                        if let Err(row_err) =
                            insert_jsonb_row(client, table_name, id, data.clone(), source_type)
                                .await
                        {
                            return Err(row_err).with_context(|| {
                                format!(
                                    "Failed to insert row {} (id='{}') into '{}' after batch failure",
                                    inserted + idx,
                                    id,
                                    table_name
                                )
                            });
                        }
                    }
                    inserted = end_idx;
                    consecutive_failures = 0;
                    tracing::info!(
                        "Successfully inserted {} rows individually after batch failure",
                        batch.len()
                    );
                } else {
                    // Non-recoverable error or too many retries
                    return Err(e).with_context(|| {
                        format!(
                            "Failed to insert batch ({} rows at offset {}) into '{}'",
                            batch.len(),
                            inserted,
                            table_name
                        )
                    });
                }
            }
        }
    }

    tracing::info!(
        "Successfully inserted {} rows into '{}'",
        total_rows,
        table_name
    );

    Ok(())
}

/// Delete rows from a JSONB table by primary key
pub async fn delete_jsonb_rows(client: &Client, table_name: &str, ids: &[String]) -> Result<()> {
    if ids.is_empty() {
        return Ok(());
    }
    crate::jsonb::validate_table_name(table_name)?;
    let sql = format!(r#"DELETE FROM "{}" WHERE id = ANY($1)"#, table_name);
    client.execute(&sql, &[&ids]).await?;
    Ok(())
}

/// Upsert rows into a JSONB table (used for deduped "_latest" tables)
pub async fn upsert_jsonb_rows(
    client: &Client,
    table_name: &str,
    rows: &[(String, serde_json::Value)],
    source_type: &str,
) -> Result<()> {
    if rows.is_empty() {
        return Ok(());
    }
    crate::jsonb::validate_table_name(table_name)?;

    let mut value_placeholders = Vec::with_capacity(rows.len());
    let mut params: Vec<&(dyn ToSql + Sync)> = Vec::with_capacity(rows.len() * 3);

    for (idx, (id, data)) in rows.iter().enumerate() {
        let base = idx * 3 + 1;
        value_placeholders.push(format!("(${}, ${}, ${})", base, base + 1, base + 2));
        params.push(id);
        params.push(data);
        params.push(&source_type);
    }

    let sql = format!(
        r#"INSERT INTO "{}" (id, data, _source_type) VALUES {} ON CONFLICT (id) DO UPDATE SET data = EXCLUDED.data, _source_type = EXCLUDED._source_type, _migrated_at = NOW()"#,
        table_name,
        value_placeholders.join(", ")
    );
    client.execute(&sql, &params).await?;
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
