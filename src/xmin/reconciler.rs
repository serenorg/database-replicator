// ABOUTME: Reconciler for xmin-based sync - detects deleted rows in source
// ABOUTME: Compares primary keys between source and target to find orphaned rows

use anyhow::{Context, Result};
use std::cmp::Ordering;
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

    /// Reconcile a table using batched streaming comparison (memory-efficient).
    ///
    /// Uses merge-join comparison on sorted primary keys fetched in batches.
    /// This avoids loading all PKs into memory, making it suitable for tables
    /// with millions of rows.
    ///
    /// # Arguments
    ///
    /// * `schema` - Schema name
    /// * `table` - Table name
    /// * `primary_key_columns` - Primary key column names
    /// * `batch_size` - Number of PKs to fetch per batch
    ///
    /// # Returns
    ///
    /// The number of orphaned rows deleted from target.
    pub async fn reconcile_table_batched(
        &self,
        schema: &str,
        table: &str,
        primary_key_columns: &[String],
        batch_size: usize,
    ) -> Result<u64> {
        tracing::info!(
            "Starting batched reconciliation for {}.{} (batch size: {})",
            schema,
            table,
            batch_size
        );

        let writer = ChangeWriter::new(self.target_client);
        let mut total_deleted = 0u64;
        let mut orphans_batch: Vec<Vec<String>> = Vec::new();

        // Initialize batch readers for both source and target
        let mut source_reader = PkBatchReader::new(
            self.source_client,
            schema,
            table,
            primary_key_columns,
            batch_size,
        );
        let mut target_reader = PkBatchReader::new(
            self.target_client,
            schema,
            table,
            primary_key_columns,
            batch_size,
        );

        // Fetch initial batches
        let mut source_batch = source_reader.fetch_next().await?;
        let mut target_batch = target_reader.fetch_next().await?;
        let mut source_idx = 0;
        let mut target_idx = 0;
        let mut comparisons = 0u64;

        // Merge-join comparison loop
        loop {
            // Refill source batch if exhausted
            if source_idx >= source_batch.len() && !source_reader.exhausted {
                source_batch = source_reader.fetch_next().await?;
                source_idx = 0;
            }

            // Refill target batch if exhausted
            if target_idx >= target_batch.len() && !target_reader.exhausted {
                target_batch = target_reader.fetch_next().await?;
                target_idx = 0;
            }

            // Check termination conditions
            let source_exhausted = source_idx >= source_batch.len();
            let target_exhausted = target_idx >= target_batch.len();

            if source_exhausted && target_exhausted {
                // Both exhausted - done
                break;
            }

            if source_exhausted {
                // Source exhausted but target has more - all remaining are orphans
                while target_idx < target_batch.len() {
                    orphans_batch.push(target_batch[target_idx].clone());
                    target_idx += 1;

                    // Delete batch when full
                    if orphans_batch.len() >= batch_size {
                        total_deleted += self
                            .delete_orphan_batch(
                                &writer,
                                schema,
                                table,
                                primary_key_columns,
                                &orphans_batch,
                            )
                            .await?;
                        orphans_batch.clear();
                    }
                }

                // Fetch more from target
                if !target_reader.exhausted {
                    target_batch = target_reader.fetch_next().await?;
                    target_idx = 0;
                }
                continue;
            }

            if target_exhausted {
                // Target exhausted but source has more - no more orphans possible
                break;
            }

            // Compare current PKs
            let source_pk = &source_batch[source_idx];
            let target_pk = &target_batch[target_idx];
            comparisons += 1;

            match compare_pks(source_pk, target_pk) {
                Ordering::Equal => {
                    // PKs match - both exist, advance both
                    source_idx += 1;
                    target_idx += 1;
                }
                Ordering::Less => {
                    // Source PK < Target PK - source has row target doesn't
                    // This is fine, just advance source
                    source_idx += 1;
                }
                Ordering::Greater => {
                    // Source PK > Target PK - target has orphan
                    orphans_batch.push(target_pk.clone());
                    target_idx += 1;

                    // Delete batch when full
                    if orphans_batch.len() >= batch_size {
                        total_deleted += self
                            .delete_orphan_batch(
                                &writer,
                                schema,
                                table,
                                primary_key_columns,
                                &orphans_batch,
                            )
                            .await?;
                        orphans_batch.clear();
                    }
                }
            }

            // Log progress periodically
            if comparisons.is_multiple_of(100_000) {
                tracing::info!(
                    "Reconciliation progress for {}.{}: {} comparisons, {} orphans found",
                    schema,
                    table,
                    comparisons,
                    total_deleted + orphans_batch.len() as u64
                );
            }
        }

        // Delete remaining orphans
        if !orphans_batch.is_empty() {
            total_deleted += self
                .delete_orphan_batch(&writer, schema, table, primary_key_columns, &orphans_batch)
                .await?;
        }

        tracing::info!(
            "Completed reconciliation for {}.{}: {} comparisons, {} orphans deleted",
            schema,
            table,
            comparisons,
            total_deleted
        );

        Ok(total_deleted)
    }

    /// Delete a batch of orphan rows.
    async fn delete_orphan_batch(
        &self,
        writer: &ChangeWriter<'_>,
        schema: &str,
        table: &str,
        primary_key_columns: &[String],
        orphans: &[Vec<String>],
    ) -> Result<u64> {
        if orphans.is_empty() {
            return Ok(0);
        }

        tracing::debug!(
            "Deleting batch of {} orphan rows from {}.{}",
            orphans.len(),
            schema,
            table
        );

        // Convert string PKs to ToSql values
        let pk_values: Vec<Vec<Box<dyn ToSql + Sync + Send>>> = orphans
            .iter()
            .map(|pk| {
                pk.iter()
                    .map(|v| Box::new(v.clone()) as Box<dyn ToSql + Sync + Send>)
                    .collect()
            })
            .collect();

        writer
            .delete_rows(schema, table, primary_key_columns, pk_values)
            .await
    }
}

/// Compare two primary key tuples lexicographically.
fn compare_pks(a: &[String], b: &[String]) -> Ordering {
    for (av, bv) in a.iter().zip(b.iter()) {
        match av.cmp(bv) {
            Ordering::Equal => continue,
            other => return other,
        }
    }
    a.len().cmp(&b.len())
}

/// Batch reader for primary keys using keyset pagination.
///
/// Fetches PKs in sorted order using WHERE pk > last_pk LIMIT batch_size,
/// which is more efficient than OFFSET for large tables.
struct PkBatchReader<'a> {
    client: &'a Client,
    schema: String,
    table: String,
    pk_columns: Vec<String>,
    batch_size: usize,
    last_pk: Option<Vec<String>>,
    pub exhausted: bool,
}

impl<'a> PkBatchReader<'a> {
    fn new(
        client: &'a Client,
        schema: &str,
        table: &str,
        pk_columns: &[String],
        batch_size: usize,
    ) -> Self {
        Self {
            client,
            schema: schema.to_string(),
            table: table.to_string(),
            pk_columns: pk_columns.to_vec(),
            batch_size,
            last_pk: None,
            exhausted: false,
        }
    }

    /// Fetch the next batch of primary keys.
    async fn fetch_next(&mut self) -> Result<Vec<Vec<String>>> {
        if self.exhausted {
            return Ok(Vec::new());
        }

        let pk_cols_select: Vec<String> = self
            .pk_columns
            .iter()
            .map(|c| format!("\"{}\"::text", c))
            .collect();

        let order_by: Vec<String> = self
            .pk_columns
            .iter()
            .map(|c| format!("\"{}\"", c))
            .collect();

        let query = if self.last_pk.is_some() {
            // Keyset pagination: WHERE (pk1, pk2, ...) > ($1, $2, ...)
            let pk_tuple: Vec<String> = self
                .pk_columns
                .iter()
                .map(|c| format!("\"{}\"", c))
                .collect();

            let params: Vec<String> = (1..=self.pk_columns.len())
                .map(|i| format!("${}", i))
                .collect();

            format!(
                "SELECT {} FROM \"{}\".\"{}\" WHERE ({}) > ({}) ORDER BY {} LIMIT {}",
                pk_cols_select.join(", "),
                self.schema,
                self.table,
                pk_tuple.join(", "),
                params.join(", "),
                order_by.join(", "),
                self.batch_size
            )
        } else {
            // First batch: no WHERE clause
            format!(
                "SELECT {} FROM \"{}\".\"{}\" ORDER BY {} LIMIT {}",
                pk_cols_select.join(", "),
                self.schema,
                self.table,
                order_by.join(", "),
                self.batch_size
            )
        };

        // Build parameters for keyset pagination
        let params: Vec<&(dyn ToSql + Sync)> = if let Some(ref last) = self.last_pk {
            last.iter().map(|s| s as &(dyn ToSql + Sync)).collect()
        } else {
            Vec::new()
        };

        let rows = self.client.query(&query, &params).await.with_context(|| {
            format!(
                "Failed to fetch PK batch from {}.{}",
                self.schema, self.table
            )
        })?;

        if rows.len() < self.batch_size {
            self.exhausted = true;
        }

        let pks: Vec<Vec<String>> = rows
            .iter()
            .map(|row| {
                (0..self.pk_columns.len())
                    .map(|i| row.get::<_, String>(i))
                    .collect()
            })
            .collect();

        // Update last_pk for next iteration
        if let Some(last_row) = pks.last() {
            self.last_pk = Some(last_row.clone());
        }

        Ok(pks)
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
