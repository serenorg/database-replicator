// ABOUTME: SyncDaemon for xmin-based sync - orchestrates continuous replication
// ABOUTME: Runs sync cycles at configurable intervals with reconciliation

use anyhow::{Context, Result};
use std::path::PathBuf;
use std::time::Duration;
use tokio::time::interval;

use super::reader::{detect_wraparound, WraparoundCheck, XminReader};
use super::reconciler::Reconciler;
use super::state::SyncState;
use super::writer::{get_primary_key_columns, get_table_columns, row_to_values, ChangeWriter};

/// Configuration for the SyncDaemon.
#[derive(Debug, Clone)]
pub struct DaemonConfig {
    /// Interval between sync cycles
    pub sync_interval: Duration,
    /// Interval between reconciliation cycles (delete detection)
    /// Set to None to disable reconciliation
    pub reconcile_interval: Option<Duration>,
    /// Path to store sync state
    pub state_path: PathBuf,
    /// Maximum rows to process per batch
    pub batch_size: usize,
    /// Tables to sync (empty = all tables)
    pub tables: Vec<String>,
    /// Schema to sync from
    pub schema: String,
}

impl Default for DaemonConfig {
    fn default() -> Self {
        Self {
            sync_interval: Duration::from_secs(3600), // 1 hour
            reconcile_interval: Some(Duration::from_secs(86400)), // 1 day
            state_path: SyncState::default_path(),
            batch_size: 10_000, // 10K rows per batch for good throughput while bounding memory
            tables: Vec::new(),
            schema: "public".to_string(),
        }
    }
}

/// Statistics from a sync cycle.
#[derive(Debug, Clone, Default)]
pub struct SyncStats {
    pub tables_synced: usize,
    pub rows_synced: u64,
    pub rows_deleted: u64,
    pub errors: Vec<String>,
    pub duration_ms: u64,
}

impl SyncStats {
    /// Check if the sync cycle completed without errors.
    pub fn is_success(&self) -> bool {
        self.errors.is_empty()
    }
}

/// SyncDaemon orchestrates continuous xmin-based replication.
///
/// It runs periodic sync cycles that:
/// 1. Read changed rows from source using xmin
/// 2. Apply changes to target using upsert
/// 3. Periodically run reconciliation to detect deletes
/// 4. Persist sync state for resume capability
pub struct SyncDaemon {
    config: DaemonConfig,
    source_url: String,
    target_url: String,
}

impl SyncDaemon {
    /// Create a new SyncDaemon with the given configuration.
    pub fn new(source_url: String, target_url: String, config: DaemonConfig) -> Self {
        Self {
            config,
            source_url,
            target_url,
        }
    }

    /// Run a single sync cycle for all configured tables.
    ///
    /// This is the main entry point for synchronization. It:
    /// 1. Loads or creates sync state
    /// 2. Connects to source and target databases
    /// 3. Syncs each table
    /// 4. Saves updated state
    pub async fn run_sync_cycle(&self) -> Result<SyncStats> {
        let start = std::time::Instant::now();
        let mut stats = SyncStats::default();

        // Load or create sync state
        let mut state = self.load_or_create_state().await?;

        // Connect to databases
        let source_client = crate::postgres::connect_with_retry(&self.source_url)
            .await
            .context("Failed to connect to source database")?;
        let target_client = crate::postgres::connect_with_retry(&self.target_url)
            .await
            .context("Failed to connect to target database")?;

        let reader = XminReader::new(&source_client);
        let writer = ChangeWriter::new(&target_client);

        // Get tables to sync
        let tables = if self.config.tables.is_empty() {
            reader.list_tables(&self.config.schema).await?
        } else {
            self.config.tables.clone()
        };

        // Sync each table
        for table in &tables {
            match self
                .sync_table(&reader, &writer, &mut state, &self.config.schema, table)
                .await
            {
                Ok(rows) => {
                    stats.tables_synced += 1;
                    stats.rows_synced += rows;
                }
                Err(e) => {
                    // Log with :? to show full error chain including root cause
                    tracing::error!("Failed to sync {}.{}: {:?}", self.config.schema, table, e);
                    let error_msg =
                        format!("Failed to sync {}.{}: {}", self.config.schema, table, e);
                    stats.errors.push(error_msg);
                }
            }
        }

        // Save state
        state.save(&self.config.state_path).await?;

        stats.duration_ms = start.elapsed().as_millis() as u64;
        Ok(stats)
    }

    /// Run reconciliation to detect and delete orphaned rows.
    pub async fn run_reconciliation(&self) -> Result<SyncStats> {
        let start = std::time::Instant::now();
        let mut stats = SyncStats::default();

        // Connect to databases
        let source_client = crate::postgres::connect_with_retry(&self.source_url)
            .await
            .context("Failed to connect to source database")?;
        let target_client = crate::postgres::connect_with_retry(&self.target_url)
            .await
            .context("Failed to connect to target database")?;

        let reconciler = Reconciler::new(&source_client, &target_client);
        let reader = XminReader::new(&source_client);

        // Get tables to reconcile
        let tables = if self.config.tables.is_empty() {
            reader.list_tables(&self.config.schema).await?
        } else {
            self.config.tables.clone()
        };

        // Reconcile each table
        for table in &tables {
            // Check if table exists in target before reconciliation
            match reconciler
                .table_exists_in_target(&self.config.schema, table)
                .await
            {
                Ok(true) => {}
                Ok(false) => {
                    tracing::warn!(
                        "Skipping reconciliation for {}.{}: table does not exist in target",
                        self.config.schema,
                        table
                    );
                    continue;
                }
                Err(e) => {
                    tracing::warn!(
                        "Skipping reconciliation for {}.{}: failed to check table existence: {}",
                        self.config.schema,
                        table,
                        e
                    );
                    continue;
                }
            }

            // Get primary key columns
            let pk_columns = reader.get_primary_key(&self.config.schema, table).await?;
            if pk_columns.is_empty() {
                tracing::warn!(
                    "Skipping reconciliation for {}.{}: no primary key",
                    self.config.schema,
                    table
                );
                continue;
            }

            match reconciler
                .reconcile_table_batched(
                    &self.config.schema,
                    table,
                    &pk_columns,
                    self.config.batch_size,
                )
                .await
            {
                Ok(deleted) => {
                    stats.tables_synced += 1;
                    stats.rows_deleted += deleted;
                }
                Err(e) => {
                    let error_msg = format!(
                        "Failed to reconcile {}.{}: {}",
                        self.config.schema, table, e
                    );
                    tracing::error!("{}", error_msg);
                    stats.errors.push(error_msg);
                }
            }
        }

        stats.duration_ms = start.elapsed().as_millis() as u64;
        Ok(stats)
    }

    /// Run the daemon continuously until stopped.
    ///
    /// This starts the main loop that runs sync cycles at the configured interval.
    /// Reconciliation runs at its own interval if configured.
    pub async fn run(&self, mut shutdown: tokio::sync::broadcast::Receiver<()>) -> Result<()> {
        let mut sync_interval = interval(self.config.sync_interval);
        let mut reconcile_interval = self.config.reconcile_interval.map(|d| interval(d));

        let mut cycles = 0u64;
        let mut reconcile_cycles = 0u64;

        tracing::info!(
            "Starting SyncDaemon with sync_interval={:?}, reconcile_interval={:?}",
            self.config.sync_interval,
            self.config.reconcile_interval
        );

        loop {
            tokio::select! {
                biased; // Check shutdown first

                _ = shutdown.recv() => {
                    tracing::info!("Shutdown signal received, stopping SyncDaemon");
                    break;
                }
                _ = sync_interval.tick() => {
                    cycles += 1;
                    tracing::info!("Starting sync cycle {}", cycles);

                    // Run sync cycle with shutdown check - abort if shutdown received
                    tokio::select! {
                        biased;
                        _ = shutdown.recv() => {
                            tracing::info!("Shutdown signal received during sync cycle, aborting");
                            break;
                        }
                        result = self.run_sync_cycle() => {
                            match result {
                                Ok(stats) => {
                                    tracing::info!(
                                        "Sync cycle {} completed: {} tables, {} rows in {}ms",
                                        cycles,
                                        stats.tables_synced,
                                        stats.rows_synced,
                                        stats.duration_ms
                                    );
                                    if !stats.errors.is_empty() {
                                        tracing::warn!("Sync cycle had {} errors", stats.errors.len());
                                    }
                                }
                                Err(e) => {
                                    tracing::error!("Sync cycle {} failed: {}", cycles, e);
                                }
                            }
                        }
                    }
                }
                _ = async {
                    if let Some(ref mut interval) = reconcile_interval {
                        interval.tick().await
                    } else {
                        std::future::pending::<tokio::time::Instant>().await
                    }
                } => {
                    reconcile_cycles += 1;
                    tracing::info!("Starting reconciliation cycle {}", reconcile_cycles);

                    // Run reconciliation with shutdown check
                    tokio::select! {
                        biased;
                        _ = shutdown.recv() => {
                            tracing::info!("Shutdown signal received during reconciliation, aborting");
                            break;
                        }
                        result = self.run_reconciliation() => {
                            match result {
                                Ok(stats) => {
                                    tracing::info!(
                                        "Reconciliation cycle {} completed: {} tables, {} rows deleted in {}ms",
                                        reconcile_cycles,
                                        stats.tables_synced,
                                        stats.rows_deleted,
                                        stats.duration_ms
                                    );
                                }
                                Err(e) => {
                                    tracing::error!("Reconciliation cycle {} failed: {}", reconcile_cycles, e);
                                }
                            }
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// Sync a single table using batched processing.
    ///
    /// This method processes rows in batches to avoid loading entire tables into memory.
    /// This is critical for large tables (millions of rows) where loading everything
    /// at once would cause OOM or connection timeouts.
    async fn sync_table(
        &self,
        reader: &XminReader<'_>,
        writer: &ChangeWriter<'_>,
        state: &mut SyncState,
        schema: &str,
        table: &str,
    ) -> Result<u64> {
        // Get table state
        let table_state = state.get_or_create_table(schema, table);
        let stored_xmin = table_state.last_xmin;

        // Get table metadata from SOURCE (not target - tables may not exist there yet)
        let columns = get_table_columns(reader.client(), schema, table).await?;
        let pk_columns = get_primary_key_columns(reader.client(), schema, table).await?;

        if pk_columns.is_empty() {
            anyhow::bail!("Table {}.{} has no primary key", schema, table);
        }

        let column_names: Vec<String> = columns.iter().map(|(name, _)| name.clone()).collect();

        // Check for xmin wraparound before starting
        let current_xmin = reader.get_current_xmin().await?;
        let (since_xmin, is_full_sync) = if detect_wraparound(stored_xmin, current_xmin)
            == WraparoundCheck::WraparoundDetected
        {
            tracing::warn!(
                "xmin wraparound detected for {}.{} - performing full table sync",
                schema,
                table
            );
            (0, true) // Start from beginning
        } else {
            (stored_xmin, false)
        };

        // Use batched reading to avoid loading entire table into memory
        let batch_size = self.config.batch_size;
        let mut batch_reader = reader
            .read_changes_batched(schema, table, &column_names, since_xmin, batch_size)
            .await?;

        let mut total_rows = 0u64;
        let mut max_xmin = since_xmin;
        let mut batch_count = 0u64;

        // Process batches until exhausted
        while let Some((rows, batch_max_xmin)) = reader.fetch_batch(&mut batch_reader).await? {
            if rows.is_empty() {
                break;
            }

            batch_count += 1;
            let batch_len = rows.len();

            // Log first batch with total context, then periodic progress
            if batch_count == 1 {
                if is_full_sync {
                    tracing::info!(
                        "Starting full table sync for {}.{} (batch size: {})",
                        schema,
                        table,
                        batch_size
                    );
                } else {
                    tracing::info!(
                        "Found changes in {}.{} (xmin {} -> {}), processing in batches",
                        schema,
                        table,
                        since_xmin,
                        batch_max_xmin
                    );
                }
            }

            // Convert and apply batch immediately (memory = O(batch_size))
            let values: Vec<Vec<Box<dyn tokio_postgres::types::ToSql + Sync + Send>>> = rows
                .iter()
                .map(|row| row_to_values(row, &columns))
                .collect();

            let affected = writer
                .apply_batch(schema, table, &pk_columns, &column_names, values)
                .await?;

            total_rows += affected;
            max_xmin = batch_max_xmin;

            // Update state after each batch for resume capability
            state.update_table(schema, table, max_xmin, affected);

            // Log progress every 10 batches or 100K rows
            if batch_count.is_multiple_of(10) || total_rows % 100_000 < batch_len as u64 {
                tracing::info!(
                    "Progress: {}.{} - {} rows synced ({} batches), current xmin: {}",
                    schema,
                    table,
                    total_rows,
                    batch_count,
                    max_xmin
                );
            }
        }

        if total_rows == 0 {
            tracing::debug!(
                "No changes in {}.{} since xmin {}",
                schema,
                table,
                since_xmin
            );
        } else {
            tracing::info!(
                "Completed sync for {}.{}: {} rows in {} batches (xmin {} -> {})",
                schema,
                table,
                total_rows,
                batch_count,
                since_xmin,
                max_xmin
            );
        }

        Ok(total_rows)
    }

    /// Load existing state or create new state.
    async fn load_or_create_state(&self) -> Result<SyncState> {
        if self.config.state_path.exists() {
            match SyncState::load(&self.config.state_path).await {
                Ok(state) => {
                    tracing::info!(
                        "Loaded existing sync state from {:?}",
                        self.config.state_path
                    );
                    return Ok(state);
                }
                Err(e) => {
                    tracing::warn!(
                        "Failed to load sync state from {:?}: {}. Creating new state.",
                        self.config.state_path,
                        e
                    );
                }
            }
        }

        tracing::info!("Creating new sync state");
        Ok(SyncState::new(&self.source_url, &self.target_url))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_daemon_config_default() {
        let config = DaemonConfig::default();
        assert_eq!(config.sync_interval, Duration::from_secs(3600));
        assert_eq!(config.reconcile_interval, Some(Duration::from_secs(86400)));
        assert_eq!(config.batch_size, 10_000);
        assert_eq!(config.schema, "public");
    }

    #[test]
    fn test_sync_stats_success() {
        let stats = SyncStats {
            tables_synced: 5,
            rows_synced: 100,
            rows_deleted: 0,
            errors: vec![],
            duration_ms: 500,
        };
        assert!(stats.is_success());
    }

    #[test]
    fn test_sync_stats_with_errors() {
        let stats = SyncStats {
            tables_synced: 4,
            rows_synced: 80,
            rows_deleted: 0,
            errors: vec!["Failed to sync table X".to_string()],
            duration_ms: 500,
        };
        assert!(!stats.is_success());
    }
}
