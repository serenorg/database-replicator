// ABOUTME: Reconciler for xmin-based sync - detects deleted rows in source
// ABOUTME: Compares primary keys between source and target to find orphaned rows

use anyhow::{Context, Result};
use std::collections::HashSet;
use tokio_postgres::types::ToSql;
use tokio_postgres::Client;

use super::writer::ChangeWriter;

/// Reconciler detects rows that exist in target but not in source (deletions).
///
/// Since xmin-based sync only sees modified rows, it cannot detect deletions.
/// The Reconciler performs periodic full-table primary key comparisons to find
/// rows that need to be deleted from the target.
pub struct Reconciler<'a> {
    source_client: &'a Client,
    target_client: &'a Client,
}

impl<'a> Reconciler<'a> {
    /// Create a new Reconciler with source and target database connections.
    pub fn new(source_client: &'a Client, target_client: &'a Client) -> Self {
        Self {
            source_client,
            target_client,
        }
    }

    /// Find rows that exist in target but not in source (orphaned rows).
    ///
    /// This performs a primary key comparison between source and target tables.
    /// Returns the primary key values of rows that should be deleted from target.
    ///
    /// # Arguments
    ///
    /// * `schema` - Schema name
    /// * `table` - Table name
    /// * `primary_key_columns` - Primary key column names
    ///
    /// # Returns
    ///
    /// A vector of primary key value tuples for orphaned rows.
    pub async fn find_orphaned_rows(
        &self,
        schema: &str,
        table: &str,
        primary_key_columns: &[String],
    ) -> Result<Vec<Vec<String>>> {
        // Get all PKs from source
        let source_pks = self
            .get_all_primary_keys(self.source_client, schema, table, primary_key_columns)
            .await
            .context("Failed to get source primary keys")?;

        // Get all PKs from target
        let target_pks = self
            .get_all_primary_keys(self.target_client, schema, table, primary_key_columns)
            .await
            .context("Failed to get target primary keys")?;

        // Find PKs in target that don't exist in source
        let source_set: HashSet<Vec<String>> = source_pks.into_iter().collect();
        let orphaned: Vec<Vec<String>> = target_pks
            .into_iter()
            .filter(|pk| !source_set.contains(pk))
            .collect();

        tracing::info!(
            "Found {} orphaned rows in {}.{} that need deletion",
            orphaned.len(),
            schema,
            table
        );

        Ok(orphaned)
    }

    /// Reconcile a table by deleting orphaned rows from target.
    ///
    /// This is a convenience method that finds orphaned rows and deletes them.
    ///
    /// # Returns
    ///
    /// The number of rows deleted from target.
    pub async fn reconcile_table(
        &self,
        schema: &str,
        table: &str,
        primary_key_columns: &[String],
    ) -> Result<u64> {
        let orphaned = self
            .find_orphaned_rows(schema, table, primary_key_columns)
            .await?;

        if orphaned.is_empty() {
            tracing::info!("No orphaned rows found in {}.{}", schema, table);
            return Ok(0);
        }

        // Convert string PKs to ToSql values
        let pk_values: Vec<Vec<Box<dyn ToSql + Sync + Send>>> = orphaned
            .into_iter()
            .map(|pk| {
                pk.into_iter()
                    .map(|v| Box::new(v) as Box<dyn ToSql + Sync + Send>)
                    .collect()
            })
            .collect();

        // Delete orphaned rows
        let writer = ChangeWriter::new(self.target_client);
        let deleted = writer
            .delete_rows(schema, table, primary_key_columns, pk_values)
            .await?;

        tracing::info!(
            "Deleted {} orphaned rows from {}.{}",
            deleted,
            schema,
            table
        );

        Ok(deleted)
    }

    /// Get all primary key values from a table.
    async fn get_all_primary_keys(
        &self,
        client: &Client,
        schema: &str,
        table: &str,
        primary_key_columns: &[String],
    ) -> Result<Vec<Vec<String>>> {
        let pk_cols: Vec<String> = primary_key_columns
            .iter()
            .map(|c| format!("\"{}\"::text", c))
            .collect();

        let query = format!(
            "SELECT {} FROM \"{}\".\"{}\" ORDER BY {}",
            pk_cols.join(", "),
            schema,
            table,
            primary_key_columns
                .iter()
                .map(|c| format!("\"{}\"", c))
                .collect::<Vec<_>>()
                .join(", ")
        );

        let rows = client
            .query(&query, &[])
            .await
            .with_context(|| format!("Failed to get primary keys from {}.{}", schema, table))?;

        let pks: Vec<Vec<String>> = rows
            .iter()
            .map(|row| {
                (0..primary_key_columns.len())
                    .map(|i| row.get::<_, String>(i))
                    .collect()
            })
            .collect();

        Ok(pks)
    }

    /// Get count of rows in source and target for comparison.
    pub async fn get_row_counts(&self, schema: &str, table: &str) -> Result<(i64, i64)> {
        let query = format!("SELECT COUNT(*) FROM \"{}\".\"{}\"", schema, table);

        let source_row = self
            .source_client
            .query_one(&query, &[])
            .await
            .context("Failed to get source row count")?;
        let source_count: i64 = source_row.get(0);

        let target_row = self
            .target_client
            .query_one(&query, &[])
            .await
            .context("Failed to get target row count")?;
        let target_count: i64 = target_row.get(0);

        Ok((source_count, target_count))
    }

    /// Check if a table exists in the target database.
    pub async fn table_exists_in_target(&self, schema: &str, table: &str) -> Result<bool> {
        let query = "SELECT EXISTS (
            SELECT 1 FROM information_schema.tables
            WHERE table_schema = $1 AND table_name = $2
        )";

        let row = self
            .target_client
            .query_one(query, &[&schema, &table])
            .await
            .context("Failed to check if table exists")?;

        Ok(row.get(0))
    }
}

/// Configuration for reconciliation behavior.
#[derive(Debug, Clone)]
pub struct ReconcileConfig {
    /// Whether to actually delete orphaned rows (false = dry run)
    pub delete_orphans: bool,
    /// Maximum number of orphans to delete in one batch
    pub max_deletes: Option<usize>,
    /// Tables to skip during reconciliation
    pub skip_tables: Vec<String>,
}

impl Default for ReconcileConfig {
    fn default() -> Self {
        Self {
            delete_orphans: true,
            max_deletes: None,
            skip_tables: Vec::new(),
        }
    }
}

/// Result of a reconciliation operation.
#[derive(Debug, Clone)]
pub struct ReconcileResult {
    pub schema: String,
    pub table: String,
    pub source_count: i64,
    pub target_count: i64,
    pub orphaned_count: usize,
    pub deleted_count: u64,
}

impl ReconcileResult {
    /// Check if the table is in sync (same row count, no orphans).
    pub fn is_in_sync(&self) -> bool {
        self.source_count == self.target_count && self.orphaned_count == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_reconcile_config_default() {
        let config = ReconcileConfig::default();
        assert!(config.delete_orphans);
        assert!(config.max_deletes.is_none());
        assert!(config.skip_tables.is_empty());
    }

    #[test]
    fn test_reconcile_result_in_sync() {
        let result = ReconcileResult {
            schema: "public".to_string(),
            table: "users".to_string(),
            source_count: 100,
            target_count: 100,
            orphaned_count: 0,
            deleted_count: 0,
        };
        assert!(result.is_in_sync());
    }

    #[test]
    fn test_reconcile_result_not_in_sync() {
        let result = ReconcileResult {
            schema: "public".to_string(),
            table: "users".to_string(),
            source_count: 100,
            target_count: 105,
            orphaned_count: 5,
            deleted_count: 0,
        };
        assert!(!result.is_in_sync());
    }
}
