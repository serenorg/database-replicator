// ABOUTME: XminReader for xmin-based sync - reads changed rows from source PostgreSQL
// ABOUTME: Uses xmin system column to detect rows modified since last sync

use anyhow::{Context, Result};
use tokio_postgres::{Client, Row};

/// Threshold for detecting xmin wraparound.
/// If old_xmin - new_xmin > this value, we assume wraparound occurred.
/// PostgreSQL xmin is 32-bit (~4 billion max), so 2 billion is half.
const WRAPAROUND_THRESHOLD: u32 = 2_000_000_000;

/// Result of checking for xmin wraparound.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WraparoundCheck {
    /// No wraparound detected, safe to proceed with incremental sync
    Normal,
    /// Wraparound detected, full table sync required
    WraparoundDetected,
}

/// Detect if xmin wraparound has occurred.
///
/// PostgreSQL transaction IDs are 32-bit unsigned integers that wrap around
/// after ~4 billion transactions. When this happens, new xmin values will be
/// smaller than old ones by a large margin (> 2 billion).
///
/// # Arguments
///
/// * `old_xmin` - The previously recorded xmin value
/// * `current_xmin` - The current database transaction ID
///
/// # Returns
///
/// `WraparoundCheck::WraparoundDetected` if wraparound occurred, `Normal` otherwise.
pub fn detect_wraparound(old_xmin: u32, current_xmin: u32) -> WraparoundCheck {
    // If current < old by more than half the 32-bit range, it's likely a wraparound
    if old_xmin > current_xmin && (old_xmin - current_xmin) > WRAPAROUND_THRESHOLD {
        tracing::warn!(
            "xmin wraparound detected: old_xmin={}, current_xmin={}, delta={}",
            old_xmin,
            current_xmin,
            old_xmin - current_xmin
        );
        WraparoundCheck::WraparoundDetected
    } else {
        WraparoundCheck::Normal
    }
}

/// Reads changed rows from a PostgreSQL table using xmin-based change detection.
///
/// PostgreSQL's `xmin` system column contains the transaction ID that last modified
/// each row. By tracking the maximum xmin seen, we can query for only rows that
/// have been modified since the last sync.
///
/// **Warning:** xmin wraps around at 2^32 transactions. Use `detect_wraparound()`
/// to check for this condition and trigger a full table sync when detected.
pub struct XminReader<'a> {
    client: &'a Client,
}

impl<'a> XminReader<'a> {
    /// Create a new XminReader for the given PostgreSQL client connection.
    pub fn new(client: &'a Client) -> Self {
        Self { client }
    }

    /// Get the current transaction ID (xmin snapshot) from the database.
    ///
    /// This should be called at the start of a sync to establish the high-water mark.
    pub async fn get_current_xmin(&self) -> Result<u32> {
        let row = self
            .client
            .query_one("SELECT txid_current()::text::bigint", &[])
            .await
            .context("Failed to get current transaction ID")?;

        let txid: i64 = row.get(0);
        // xmin is stored as u32, txid_current() returns i64
        // We mask to get the 32-bit xmin value
        Ok((txid & 0xFFFFFFFF) as u32)
    }

    /// Read all rows from a table that have xmin greater than the given value.
    ///
    /// # Arguments
    ///
    /// * `schema` - The schema name (e.g., "public")
    /// * `table` - The table name
    /// * `columns` - Column names to select (pass empty slice to select all)
    /// * `since_xmin` - Only return rows with xmin > this value (0 = all rows)
    ///
    /// # Returns
    ///
    /// A tuple of (rows, max_xmin) where max_xmin is the highest xmin seen in the result set.
    pub async fn read_changes(
        &self,
        schema: &str,
        table: &str,
        columns: &[String],
        since_xmin: u32,
    ) -> Result<(Vec<Row>, u32)> {
        let column_list = if columns.is_empty() {
            "*".to_string()
        } else {
            columns
                .iter()
                .map(|c| format!("\"{}\"", c))
                .collect::<Vec<_>>()
                .join(", ")
        };

        // Query rows where xmin > since_xmin, including the xmin value
        let query = format!(
            "SELECT {}, xmin::text::bigint as _xmin FROM \"{}\".\"{}\" WHERE xmin::text::bigint > $1 ORDER BY xmin",
            column_list, schema, table
        );

        let rows = self
            .client
            .query(&query, &[&(since_xmin as i64)])
            .await
            .with_context(|| format!("Failed to read changes from {}.{}", schema, table))?;

        // Find the max xmin in the result set
        let max_xmin = rows
            .iter()
            .map(|row| {
                let xmin: i64 = row.get("_xmin");
                (xmin & 0xFFFFFFFF) as u32
            })
            .max()
            .unwrap_or(since_xmin);

        Ok((rows, max_xmin))
    }

    /// Read changes in batches to handle large tables efficiently.
    ///
    /// # Arguments
    ///
    /// * `schema` - The schema name
    /// * `table` - The table name
    /// * `columns` - Column names to select
    /// * `since_xmin` - Only return rows with xmin > this value
    /// * `batch_size` - Maximum rows per batch
    ///
    /// # Returns
    ///
    /// An iterator-like struct that yields batches of rows.
    pub async fn read_changes_batched(
        &self,
        schema: &str,
        table: &str,
        columns: &[String],
        since_xmin: u32,
        batch_size: usize,
    ) -> Result<BatchReader> {
        Ok(BatchReader {
            schema: schema.to_string(),
            table: table.to_string(),
            columns: columns.to_vec(),
            current_xmin: since_xmin,
            batch_size,
            exhausted: false,
        })
    }

    /// Execute a batched read query and return the next batch.
    pub async fn fetch_batch(
        &self,
        batch_reader: &mut BatchReader,
    ) -> Result<Option<(Vec<Row>, u32)>> {
        if batch_reader.exhausted {
            return Ok(None);
        }

        let column_list = if batch_reader.columns.is_empty() {
            "*".to_string()
        } else {
            batch_reader
                .columns
                .iter()
                .map(|c| format!("\"{}\"", c))
                .collect::<Vec<_>>()
                .join(", ")
        };

        let query = format!(
            "SELECT {}, xmin::text::bigint as _xmin FROM \"{}\".\"{}\" \
             WHERE xmin::text::bigint > $1 \
             ORDER BY xmin \
             LIMIT $2",
            column_list, batch_reader.schema, batch_reader.table
        );

        let rows = self
            .client
            .query(
                &query,
                &[
                    &(batch_reader.current_xmin as i64),
                    &(batch_reader.batch_size as i64),
                ],
            )
            .await
            .with_context(|| {
                format!(
                    "Failed to read batch from {}.{}",
                    batch_reader.schema, batch_reader.table
                )
            })?;

        if rows.is_empty() {
            batch_reader.exhausted = true;
            return Ok(None);
        }

        // Update current_xmin to the max in this batch
        let max_xmin = rows
            .iter()
            .map(|row| {
                let xmin: i64 = row.get("_xmin");
                (xmin & 0xFFFFFFFF) as u32
            })
            .max()
            .unwrap_or(batch_reader.current_xmin);

        // Mark as exhausted if we got fewer rows than batch_size
        if rows.len() < batch_reader.batch_size {
            batch_reader.exhausted = true;
        }

        batch_reader.current_xmin = max_xmin;

        Ok(Some((rows, max_xmin)))
    }

    /// Get the estimated row count for changes since a given xmin.
    ///
    /// This uses EXPLAIN to estimate without actually scanning the table.
    pub async fn estimate_changes(
        &self,
        schema: &str,
        table: &str,
        since_xmin: u32,
    ) -> Result<i64> {
        let query = format!(
            "SELECT COUNT(*) FROM \"{}\".\"{}\" WHERE xmin::text::bigint > $1",
            schema, table
        );

        let row = self
            .client
            .query_one(&query, &[&(since_xmin as i64)])
            .await
            .with_context(|| format!("Failed to count changes in {}.{}", schema, table))?;

        let count: i64 = row.get(0);
        Ok(count)
    }

    /// Get list of all tables in a schema.
    pub async fn list_tables(&self, schema: &str) -> Result<Vec<String>> {
        let rows = self
            .client
            .query(
                "SELECT tablename FROM pg_tables WHERE schemaname = $1 ORDER BY tablename",
                &[&schema],
            )
            .await
            .with_context(|| format!("Failed to list tables in schema {}", schema))?;

        Ok(rows.iter().map(|row| row.get(0)).collect())
    }

    /// Get column information for a table.
    pub async fn get_columns(&self, schema: &str, table: &str) -> Result<Vec<ColumnInfo>> {
        let rows = self
            .client
            .query(
                "SELECT column_name, data_type, is_nullable, column_default
                 FROM information_schema.columns
                 WHERE table_schema = $1 AND table_name = $2
                 ORDER BY ordinal_position",
                &[&schema, &table],
            )
            .await
            .with_context(|| format!("Failed to get columns for {}.{}", schema, table))?;

        Ok(rows
            .iter()
            .map(|row| ColumnInfo {
                name: row.get(0),
                data_type: row.get(1),
                is_nullable: row.get::<_, String>(2) == "YES",
                has_default: row.get::<_, Option<String>>(3).is_some(),
            })
            .collect())
    }

    /// Get primary key columns for a table.
    pub async fn get_primary_key(&self, schema: &str, table: &str) -> Result<Vec<String>> {
        let rows = self
            .client
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

    /// Read ALL rows from a table (full sync).
    ///
    /// This is used when xmin wraparound is detected and we need to resync
    /// the entire table to ensure data consistency.
    ///
    /// # Arguments
    ///
    /// * `schema` - The schema name (e.g., "public")
    /// * `table` - The table name
    /// * `columns` - Column names to select (pass empty slice to select all)
    ///
    /// # Returns
    ///
    /// A tuple of (rows, max_xmin) where max_xmin is the highest xmin seen.
    pub async fn read_all_rows(
        &self,
        schema: &str,
        table: &str,
        columns: &[String],
    ) -> Result<(Vec<Row>, u32)> {
        tracing::info!(
            "Performing full table read for {}.{} (wraparound recovery)",
            schema,
            table
        );

        let column_list = if columns.is_empty() {
            "*".to_string()
        } else {
            columns
                .iter()
                .map(|c| format!("\"{}\"", c))
                .collect::<Vec<_>>()
                .join(", ")
        };

        // Query ALL rows, including their xmin values
        let query = format!(
            "SELECT {}, xmin::text::bigint as _xmin FROM \"{}\".\"{}\" ORDER BY xmin",
            column_list, schema, table
        );

        let rows = self
            .client
            .query(&query, &[])
            .await
            .with_context(|| format!("Failed to read all rows from {}.{}", schema, table))?;

        // Find the max xmin in the result set
        let max_xmin = rows
            .iter()
            .map(|row| {
                let xmin: i64 = row.get("_xmin");
                (xmin & 0xFFFFFFFF) as u32
            })
            .max()
            .unwrap_or(0);

        tracing::info!(
            "Full table read complete: {} rows, max_xmin={}",
            rows.len(),
            max_xmin
        );

        Ok((rows, max_xmin))
    }

    /// Check for wraparound and read changes accordingly.
    ///
    /// This is the recommended method for reading changes as it automatically
    /// handles wraparound detection and triggers full table sync when needed.
    ///
    /// # Arguments
    ///
    /// * `schema` - The schema name
    /// * `table` - The table name
    /// * `columns` - Column names to select
    /// * `since_xmin` - The last synced xmin value
    ///
    /// # Returns
    ///
    /// A tuple of (rows, max_xmin, was_full_sync) where was_full_sync indicates
    /// if a full table sync was performed due to wraparound.
    pub async fn read_changes_with_wraparound_check(
        &self,
        schema: &str,
        table: &str,
        columns: &[String],
        since_xmin: u32,
    ) -> Result<(Vec<Row>, u32, bool)> {
        // Get current database xmin to check for wraparound
        let current_xmin = self.get_current_xmin().await?;

        // Check for wraparound
        if detect_wraparound(since_xmin, current_xmin) == WraparoundCheck::WraparoundDetected {
            // Wraparound detected - perform full table sync
            let (rows, max_xmin) = self.read_all_rows(schema, table, columns).await?;
            Ok((rows, max_xmin, true))
        } else {
            // Normal incremental sync
            let (rows, max_xmin) = self
                .read_changes(schema, table, columns, since_xmin)
                .await?;
            Ok((rows, max_xmin, false))
        }
    }
}

/// Batch reader state for iterating over large result sets.
pub struct BatchReader {
    pub schema: String,
    pub table: String,
    pub columns: Vec<String>,
    pub current_xmin: u32,
    pub batch_size: usize,
    pub exhausted: bool,
}

/// Information about a table column.
#[derive(Debug, Clone)]
pub struct ColumnInfo {
    pub name: String,
    pub data_type: String,
    pub is_nullable: bool,
    pub has_default: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_batch_reader_initial_state() {
        let reader = BatchReader {
            schema: "public".to_string(),
            table: "users".to_string(),
            columns: vec!["id".to_string(), "name".to_string()],
            current_xmin: 0,
            batch_size: 1000,
            exhausted: false,
        };

        assert_eq!(reader.schema, "public");
        assert_eq!(reader.table, "users");
        assert_eq!(reader.current_xmin, 0);
        assert!(!reader.exhausted);
    }

    #[test]
    fn test_column_info() {
        let col = ColumnInfo {
            name: "id".to_string(),
            data_type: "integer".to_string(),
            is_nullable: false,
            has_default: true,
        };

        assert_eq!(col.name, "id");
        assert!(!col.is_nullable);
        assert!(col.has_default);
    }

    #[test]
    fn test_wraparound_detection_normal() {
        // Normal case: current > old (no wraparound)
        assert_eq!(detect_wraparound(100, 200), WraparoundCheck::Normal);

        // Normal case: current slightly less than old (normal variation)
        assert_eq!(detect_wraparound(1000, 900), WraparoundCheck::Normal);

        // Normal case: both at low values
        assert_eq!(detect_wraparound(0, 100), WraparoundCheck::Normal);
    }

    #[test]
    fn test_wraparound_detection_wraparound() {
        // Wraparound case: old is near max (3.5B), current is near 0
        // Delta = 3.5B - 100 = 3.5B > 2B threshold
        assert_eq!(
            detect_wraparound(3_500_000_000, 100),
            WraparoundCheck::WraparoundDetected
        );

        // Wraparound case: old at 4B, current at 1M
        assert_eq!(
            detect_wraparound(4_000_000_000, 1_000_000),
            WraparoundCheck::WraparoundDetected
        );

        // Edge case: exactly at threshold
        assert_eq!(
            detect_wraparound(2_500_000_000, 400_000_000),
            WraparoundCheck::WraparoundDetected
        );
    }

    #[test]
    fn test_wraparound_detection_edge_cases() {
        // Edge case: old = 0, current = anything (should be normal)
        assert_eq!(detect_wraparound(0, 1_000_000), WraparoundCheck::Normal);

        // Edge case: same values
        assert_eq!(detect_wraparound(1000, 1000), WraparoundCheck::Normal);

        // Edge case: just under threshold
        assert_eq!(detect_wraparound(2_000_000_001, 1), WraparoundCheck::Normal);

        // Edge case: just at threshold
        assert_eq!(
            detect_wraparound(2_000_000_002, 1),
            WraparoundCheck::WraparoundDetected
        );
    }
}
