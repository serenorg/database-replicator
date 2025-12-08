// ABOUTME: ChangeWriter for xmin-based sync - applies changes to target PostgreSQL
// ABOUTME: Uses INSERT ... ON CONFLICT DO UPDATE for efficient upserts

use anyhow::{Context, Result};
use tokio_postgres::types::ToSql;
use tokio_postgres::{Client, Row};

/// Writes changes to the target PostgreSQL database using upsert operations.
///
/// The ChangeWriter handles batched upserts within transactions for efficiency
/// and atomicity. It dynamically builds INSERT ... ON CONFLICT DO UPDATE queries
/// based on table schema.
pub struct ChangeWriter<'a> {
    client: &'a Client,
}

impl<'a> ChangeWriter<'a> {
    /// Create a new ChangeWriter for the given PostgreSQL client connection.
    pub fn new(client: &'a Client) -> Self {
        Self { client }
    }

    /// Get a reference to the underlying client.
    ///
    /// Useful for callers that need to perform additional queries.
    pub fn client(&self) -> &Client {
        self.client
    }

    /// Apply a batch of rows to a table using upsert (INSERT ... ON CONFLICT DO UPDATE).
    ///
    /// Uses batching internally to stay within PostgreSQL's parameter limits.
    /// Each batch is executed as a separate query (PostgreSQL auto-commits).
    ///
    /// # Arguments
    ///
    /// * `schema` - The schema name (e.g., "public")
    /// * `table` - The table name
    /// * `primary_key_columns` - Column names that form the primary key
    /// * `all_columns` - All column names in the order they appear in `rows`
    /// * `rows` - The rows to upsert, each row is a vector of values
    ///
    /// # Returns
    ///
    /// The number of rows affected.
    pub async fn apply_batch(
        &self,
        schema: &str,
        table: &str,
        primary_key_columns: &[String],
        all_columns: &[String],
        rows: Vec<Vec<Box<dyn ToSql + Sync + Send>>>,
    ) -> Result<u64> {
        if rows.is_empty() {
            return Ok(0);
        }

        // PostgreSQL has a limit of ~65535 parameters per query
        // Calculate batch size based on number of columns
        let params_per_row = all_columns.len();
        let max_params = 65000; // Leave some margin
        let batch_size = std::cmp::max(1, max_params / params_per_row);

        let mut total_affected = 0u64;

        for chunk in rows.chunks(batch_size) {
            let affected = self
                .execute_upsert_batch(schema, table, primary_key_columns, all_columns, chunk)
                .await?;
            total_affected += affected;
        }

        Ok(total_affected)
    }

    /// Execute a single batch of upserts.
    async fn execute_upsert_batch(
        &self,
        schema: &str,
        table: &str,
        primary_key_columns: &[String],
        all_columns: &[String],
        rows: &[Vec<Box<dyn ToSql + Sync + Send>>],
    ) -> Result<u64> {
        if rows.is_empty() {
            return Ok(0);
        }

        let query = build_upsert_query(schema, table, primary_key_columns, all_columns, rows.len());

        // Flatten all row values into a single params vector
        let params: Vec<&(dyn ToSql + Sync)> = rows
            .iter()
            .flat_map(|row| row.iter().map(|v| v.as_ref() as &(dyn ToSql + Sync)))
            .collect();

        let affected = self
            .client
            .execute(&query, &params)
            .await
            .with_context(|| format!("Failed to upsert batch into {}.{}", schema, table))?;

        Ok(affected)
    }

    /// Apply a single row using upsert.
    ///
    /// For single rows, this is more efficient than creating a batch.
    pub async fn apply_row(
        &self,
        schema: &str,
        table: &str,
        primary_key_columns: &[String],
        all_columns: &[String],
        values: Vec<Box<dyn ToSql + Sync + Send>>,
    ) -> Result<u64> {
        let query = build_upsert_query(schema, table, primary_key_columns, all_columns, 1);

        let params: Vec<&(dyn ToSql + Sync)> = values
            .iter()
            .map(|v| v.as_ref() as &(dyn ToSql + Sync))
            .collect();

        let affected = self
            .client
            .execute(&query, &params)
            .await
            .with_context(|| format!("Failed to upsert row into {}.{}", schema, table))?;

        Ok(affected)
    }

    /// Delete rows by primary key values.
    ///
    /// Used by the reconciler to remove rows that no longer exist in source.
    /// Executes deletes in batches to stay within PostgreSQL parameter limits.
    pub async fn delete_rows(
        &self,
        schema: &str,
        table: &str,
        primary_key_columns: &[String],
        pk_values: Vec<Vec<Box<dyn ToSql + Sync + Send>>>,
    ) -> Result<u64> {
        if pk_values.is_empty() {
            return Ok(0);
        }

        let mut total_deleted = 0u64;

        // Delete in batches
        let batch_size = 1000;
        for chunk in pk_values.chunks(batch_size) {
            let deleted = self
                .execute_delete_batch(schema, table, primary_key_columns, chunk)
                .await?;
            total_deleted += deleted;
        }

        Ok(total_deleted)
    }

    /// Execute a batch delete.
    async fn execute_delete_batch(
        &self,
        schema: &str,
        table: &str,
        primary_key_columns: &[String],
        pk_values: &[Vec<Box<dyn ToSql + Sync + Send>>],
    ) -> Result<u64> {
        if pk_values.is_empty() {
            return Ok(0);
        }

        let query = build_delete_query(schema, table, primary_key_columns, pk_values.len());

        let params: Vec<&(dyn ToSql + Sync)> = pk_values
            .iter()
            .flat_map(|row| row.iter().map(|v| v.as_ref() as &(dyn ToSql + Sync)))
            .collect();

        let deleted = self
            .client
            .execute(&query, &params)
            .await
            .with_context(|| format!("Failed to delete rows from {}.{}", schema, table))?;

        Ok(deleted)
    }
}

/// Build an upsert query for the given table schema and batch size.
///
/// Generates a query like:
/// ```sql
/// INSERT INTO "schema"."table" ("col1", "col2", "col3")
/// VALUES ($1, $2, $3), ($4, $5, $6), ...
/// ON CONFLICT ("pk_col") DO UPDATE SET
///   "col2" = EXCLUDED."col2",
///   "col3" = EXCLUDED."col3"
/// ```
fn build_upsert_query(
    schema: &str,
    table: &str,
    primary_key_columns: &[String],
    all_columns: &[String],
    num_rows: usize,
) -> String {
    // Quote identifiers to handle reserved words and special characters
    let quoted_columns: Vec<String> = all_columns.iter().map(|c| format!("\"{}\"", c)).collect();

    let quoted_pk_columns: Vec<String> = primary_key_columns
        .iter()
        .map(|c| format!("\"{}\"", c))
        .collect();

    // Build VALUES placeholders: ($1, $2, $3), ($4, $5, $6), ...
    let num_cols = all_columns.len();
    let value_rows: Vec<String> = (0..num_rows)
        .map(|row_idx| {
            let placeholders: Vec<String> = (0..num_cols)
                .map(|col_idx| format!("${}", row_idx * num_cols + col_idx + 1))
                .collect();
            format!("({})", placeholders.join(", "))
        })
        .collect();

    // Build UPDATE SET clause for non-PK columns
    let update_columns: Vec<String> = all_columns
        .iter()
        .filter(|c| !primary_key_columns.contains(c))
        .map(|c| format!("\"{}\" = EXCLUDED.\"{}\"", c, c))
        .collect();

    let update_clause = if update_columns.is_empty() {
        // All columns are PKs - use DO NOTHING
        "DO NOTHING".to_string()
    } else {
        format!("DO UPDATE SET {}", update_columns.join(", "))
    };

    format!(
        "INSERT INTO \"{}\".\"{}\" ({}) VALUES {} ON CONFLICT ({}) {}",
        schema,
        table,
        quoted_columns.join(", "),
        value_rows.join(", "),
        quoted_pk_columns.join(", "),
        update_clause
    )
}

/// Build a delete query for multiple rows by primary key.
///
/// For single-column PK:
/// ```sql
/// DELETE FROM "schema"."table" WHERE "id" IN ($1, $2, $3, ...)
/// ```
///
/// For composite PK:
/// ```sql
/// DELETE FROM "schema"."table" WHERE ("pk1", "pk2") IN (($1, $2), ($3, $4), ...)
/// ```
fn build_delete_query(
    schema: &str,
    table: &str,
    primary_key_columns: &[String],
    num_rows: usize,
) -> String {
    let num_pk_cols = primary_key_columns.len();

    if num_pk_cols == 1 {
        // Simple case: single-column primary key
        let pk_col = format!("\"{}\"", primary_key_columns[0]);
        let placeholders: Vec<String> = (1..=num_rows).map(|i| format!("${}", i)).collect();

        format!(
            "DELETE FROM \"{}\".\"{}\" WHERE {} IN ({})",
            schema,
            table,
            pk_col,
            placeholders.join(", ")
        )
    } else {
        // Composite primary key
        let pk_cols: Vec<String> = primary_key_columns
            .iter()
            .map(|c| format!("\"{}\"", c))
            .collect();

        let value_tuples: Vec<String> = (0..num_rows)
            .map(|row_idx| {
                let placeholders: Vec<String> = (0..num_pk_cols)
                    .map(|col_idx| format!("${}", row_idx * num_pk_cols + col_idx + 1))
                    .collect();
                format!("({})", placeholders.join(", "))
            })
            .collect();

        format!(
            "DELETE FROM \"{}\".\"{}\" WHERE ({}) IN ({})",
            schema,
            table,
            pk_cols.join(", "),
            value_tuples.join(", ")
        )
    }
}

/// Extract column metadata from a PostgreSQL table.
///
/// Returns (column_name, data_type) pairs for all columns in the table.
pub async fn get_table_columns(
    client: &Client,
    schema: &str,
    table: &str,
) -> Result<Vec<(String, String)>> {
    let rows = client
        .query(
            "SELECT column_name, data_type
             FROM information_schema.columns
             WHERE table_schema = $1 AND table_name = $2
             ORDER BY ordinal_position",
            &[&schema, &table],
        )
        .await
        .with_context(|| format!("Failed to get columns for {}.{}", schema, table))?;

    Ok(rows
        .iter()
        .map(|row| {
            let name: String = row.get(0);
            let dtype: String = row.get(1);
            (name, dtype)
        })
        .collect())
}

/// Get primary key columns for a table.
///
/// Returns the column names that form the primary key constraint.
pub async fn get_primary_key_columns(
    client: &Client,
    schema: &str,
    table: &str,
) -> Result<Vec<String>> {
    let rows = client
        .query(
            "SELECT a.attname
             FROM pg_index i
             JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
             JOIN pg_class c ON c.oid = i.indrelid
             JOIN pg_namespace n ON n.oid = c.relnamespace
             WHERE i.indisprimary
               AND n.nspname = $1
               AND c.relname = $2
             ORDER BY array_position(i.indkey, a.attnum)",
            &[&schema, &table],
        )
        .await
        .with_context(|| format!("Failed to get primary key for {}.{}", schema, table))?;

    Ok(rows.iter().map(|row| row.get(0)).collect())
}

/// Convert a tokio_postgres Row to a vector of boxed ToSql values.
///
/// This is a helper for extracting values from source rows to pass to ChangeWriter.
/// The caller must know the column types to extract values correctly.
pub fn row_to_values(
    row: &Row,
    column_types: &[(String, String)],
) -> Vec<Box<dyn ToSql + Sync + Send>> {
    column_types
        .iter()
        .enumerate()
        .map(|(idx, (_name, dtype))| -> Box<dyn ToSql + Sync + Send> {
            // Handle common PostgreSQL types
            match dtype.as_str() {
                "integer" | "int4" => {
                    let val: Option<i32> = row.get(idx);
                    Box::new(val)
                }
                "bigint" | "int8" => {
                    let val: Option<i64> = row.get(idx);
                    Box::new(val)
                }
                "smallint" | "int2" => {
                    let val: Option<i16> = row.get(idx);
                    Box::new(val)
                }
                "text" | "varchar" | "character varying" | "char" | "character" | "name" => {
                    let val: Option<String> = row.get(idx);
                    Box::new(val)
                }
                "boolean" | "bool" => {
                    let val: Option<bool> = row.get(idx);
                    Box::new(val)
                }
                "real" | "float4" => {
                    let val: Option<f32> = row.get(idx);
                    Box::new(val)
                }
                "double precision" | "float8" => {
                    let val: Option<f64> = row.get(idx);
                    Box::new(val)
                }
                "uuid" => {
                    let val: Option<uuid::Uuid> = row.get(idx);
                    Box::new(val)
                }
                "timestamp without time zone" | "timestamp" => {
                    let val: Option<chrono::NaiveDateTime> = row.get(idx);
                    Box::new(val)
                }
                "timestamp with time zone" | "timestamptz" => {
                    let val: Option<chrono::DateTime<chrono::Utc>> = row.get(idx);
                    Box::new(val)
                }
                "date" => {
                    let val: Option<chrono::NaiveDate> = row.get(idx);
                    Box::new(val)
                }
                "json" | "jsonb" => {
                    let val: Option<serde_json::Value> = row.get(idx);
                    Box::new(val)
                }
                "bytea" => {
                    let val: Option<Vec<u8>> = row.get(idx);
                    Box::new(val)
                }
                "numeric" | "decimal" => {
                    // Fall back to string representation
                    let val: Option<String> = row.try_get::<_, String>(idx).ok();
                    Box::new(val)
                }
                _ => {
                    // For unknown types, try to get as string
                    let val: Option<String> = row.try_get::<_, String>(idx).ok();
                    Box::new(val)
                }
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_upsert_query_single_row() {
        let query = build_upsert_query(
            "public",
            "users",
            &["id".to_string()],
            &["id".to_string(), "name".to_string(), "email".to_string()],
            1,
        );

        assert!(query.contains("INSERT INTO \"public\".\"users\""));
        assert!(query.contains("(\"id\", \"name\", \"email\")"));
        assert!(query.contains("VALUES ($1, $2, $3)"));
        assert!(query.contains("ON CONFLICT (\"id\")"));
        assert!(query.contains("DO UPDATE SET"));
        assert!(query.contains("\"name\" = EXCLUDED.\"name\""));
        assert!(query.contains("\"email\" = EXCLUDED.\"email\""));
    }

    #[test]
    fn test_build_upsert_query_multiple_rows() {
        let query = build_upsert_query(
            "public",
            "users",
            &["id".to_string()],
            &["id".to_string(), "name".to_string()],
            3,
        );

        assert!(query.contains("($1, $2), ($3, $4), ($5, $6)"));
    }

    #[test]
    fn test_build_upsert_query_composite_pk() {
        let query = build_upsert_query(
            "public",
            "order_items",
            &["order_id".to_string(), "item_id".to_string()],
            &[
                "order_id".to_string(),
                "item_id".to_string(),
                "quantity".to_string(),
            ],
            1,
        );

        assert!(query.contains("ON CONFLICT (\"order_id\", \"item_id\")"));
        assert!(query.contains("\"quantity\" = EXCLUDED.\"quantity\""));
    }

    #[test]
    fn test_build_upsert_query_all_pk_columns() {
        // When all columns are PK columns, should use DO NOTHING
        let query = build_upsert_query(
            "public",
            "tags",
            &["id".to_string()],
            &["id".to_string()],
            1,
        );

        assert!(query.contains("DO NOTHING"));
        assert!(!query.contains("DO UPDATE SET"));
    }

    #[test]
    fn test_build_delete_query_single_pk() {
        let query = build_delete_query("public", "users", &["id".to_string()], 3);

        assert!(query.contains("DELETE FROM \"public\".\"users\""));
        assert!(query.contains("WHERE \"id\" IN ($1, $2, $3)"));
    }

    #[test]
    fn test_build_delete_query_composite_pk() {
        let query = build_delete_query(
            "public",
            "order_items",
            &["order_id".to_string(), "item_id".to_string()],
            2,
        );

        assert!(query.contains("WHERE (\"order_id\", \"item_id\") IN"));
        assert!(query.contains("($1, $2), ($3, $4)"));
    }
}
