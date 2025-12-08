// ABOUTME: Integration tests for xmin-based incremental sync
// ABOUTME: Tests the full lifecycle: insert, update, delete, reconciliation, state recovery

use database_replicator::xmin::writer::{get_primary_key_columns, get_table_columns, ChangeWriter};
use database_replicator::xmin::{DaemonConfig, Reconciler, SyncDaemon, SyncState, XminReader};
use std::env;
use std::time::Duration;
use tempfile::TempDir;

/// Helper to get test database URLs from environment
fn get_test_urls() -> Option<(String, String)> {
    let source = env::var("TEST_SOURCE_URL").ok()?;
    let target = env::var("TEST_TARGET_URL").ok()?;
    Some((source, target))
}

/// Create a unique test table name to avoid conflicts
fn test_table_name(suffix: &str) -> String {
    format!("xmin_test_{}", suffix)
}

/// Helper to create test table on both source and target
async fn setup_test_table(
    source_client: &tokio_postgres::Client,
    target_client: &tokio_postgres::Client,
    table_name: &str,
) -> anyhow::Result<()> {
    let ddl = format!(
        r#"
        DROP TABLE IF EXISTS "public"."{}";
        CREATE TABLE "public"."{}" (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL,
            value INTEGER DEFAULT 0,
            updated_at TIMESTAMP DEFAULT NOW()
        )
        "#,
        table_name, table_name
    );

    source_client.batch_execute(&ddl).await?;
    target_client.batch_execute(&ddl).await?;

    Ok(())
}

/// Helper to drop test table from both databases
async fn cleanup_test_table(
    source_client: &tokio_postgres::Client,
    target_client: &tokio_postgres::Client,
    table_name: &str,
) -> anyhow::Result<()> {
    let ddl = format!(r#"DROP TABLE IF EXISTS "public"."{}" CASCADE"#, table_name);

    let _ = source_client.batch_execute(&ddl).await;
    let _ = target_client.batch_execute(&ddl).await;

    Ok(())
}

/// Test: XminReader can read changes from a table
#[tokio::test]
#[ignore]
async fn test_xmin_reader_reads_changes() {
    let (source_url, _target_url) =
        get_test_urls().expect("TEST_SOURCE_URL and TEST_TARGET_URL must be set");

    let source_client = database_replicator::postgres::connect(&source_url)
        .await
        .expect("Failed to connect to source");

    let table_name = test_table_name("reader");

    // Setup
    source_client
        .batch_execute(&format!(
            r#"
            DROP TABLE IF EXISTS "public"."{}";
            CREATE TABLE "public"."{}" (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL
            );
            INSERT INTO "public"."{}" (name) VALUES ('test1'), ('test2'), ('test3');
            "#,
            table_name, table_name, table_name
        ))
        .await
        .expect("Failed to setup test table");

    // Test
    let reader = XminReader::new(&source_client);
    let columns = vec!["id".to_string(), "name".to_string()];
    let (rows, max_xmin) = reader
        .read_changes("public", &table_name, &columns, 0)
        .await
        .expect("Failed to read changes");

    assert_eq!(rows.len(), 3, "Should have read 3 rows");
    assert!(max_xmin > 0, "max_xmin should be positive");

    println!(
        "✓ XminReader successfully read {} rows with max_xmin={}",
        rows.len(),
        max_xmin
    );

    // Cleanup
    let _ = source_client
        .batch_execute(&format!(
            r#"DROP TABLE IF EXISTS "public"."{}" CASCADE"#,
            table_name
        ))
        .await;
}

/// Test: XminReader correctly tracks incremental changes
#[tokio::test]
#[ignore]
async fn test_xmin_reader_incremental_changes() {
    let (source_url, _target_url) =
        get_test_urls().expect("TEST_SOURCE_URL and TEST_TARGET_URL must be set");

    let source_client = database_replicator::postgres::connect(&source_url)
        .await
        .expect("Failed to connect to source");

    let table_name = test_table_name("incremental");

    // Setup - create table and insert initial data
    source_client
        .batch_execute(&format!(
            r#"
            DROP TABLE IF EXISTS "public"."{}";
            CREATE TABLE "public"."{}" (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL
            );
            INSERT INTO "public"."{}" (name) VALUES ('initial');
            "#,
            table_name, table_name, table_name
        ))
        .await
        .expect("Failed to setup test table");

    let reader = XminReader::new(&source_client);
    let columns = vec!["id".to_string(), "name".to_string()];

    // First read - get initial data
    let (rows1, xmin1) = reader
        .read_changes("public", &table_name, &columns, 0)
        .await
        .expect("Failed to read initial changes");

    assert_eq!(rows1.len(), 1, "Should have 1 initial row");

    // Insert more data
    source_client
        .batch_execute(&format!(
            r#"INSERT INTO "public"."{}" (name) VALUES ('second'), ('third');"#,
            table_name
        ))
        .await
        .expect("Failed to insert more data");

    // Second read - only get new data
    let (rows2, xmin2) = reader
        .read_changes("public", &table_name, &columns, xmin1)
        .await
        .expect("Failed to read incremental changes");

    assert_eq!(rows2.len(), 2, "Should have 2 new rows");
    assert!(xmin2 > xmin1, "xmin should increase");

    println!(
        "✓ Incremental sync working: initial={} rows, incremental={} rows",
        rows1.len(),
        rows2.len()
    );

    // Cleanup
    let _ = source_client
        .batch_execute(&format!(
            r#"DROP TABLE IF EXISTS "public"."{}" CASCADE"#,
            table_name
        ))
        .await;
}

/// Test: ChangeWriter can apply changes to target
#[tokio::test]
#[ignore]
async fn test_change_writer_applies_changes() {
    let (source_url, target_url) =
        get_test_urls().expect("TEST_SOURCE_URL and TEST_TARGET_URL must be set");

    let source_client = database_replicator::postgres::connect(&source_url)
        .await
        .expect("Failed to connect to source");
    let target_client = database_replicator::postgres::connect(&target_url)
        .await
        .expect("Failed to connect to target");

    let table_name = test_table_name("writer");

    // Setup tables on both
    setup_test_table(&source_client, &target_client, &table_name)
        .await
        .expect("Failed to setup test tables");

    // Insert data into source
    source_client
        .batch_execute(&format!(
            r#"INSERT INTO "public"."{}" (name, value) VALUES ('a', 1), ('b', 2), ('c', 3);"#,
            table_name
        ))
        .await
        .expect("Failed to insert source data");

    // Read changes from source
    let reader = XminReader::new(&source_client);
    let columns = get_table_columns(&target_client, "public", &table_name)
        .await
        .expect("Failed to get columns");
    let pk_columns = get_primary_key_columns(&target_client, "public", &table_name)
        .await
        .expect("Failed to get primary key");
    let column_names: Vec<String> = columns.iter().map(|(name, _)| name.clone()).collect();

    let (rows, _max_xmin) = reader
        .read_changes("public", &table_name, &column_names, 0)
        .await
        .expect("Failed to read source changes");

    // Convert rows to values
    let values: Vec<Vec<Box<dyn tokio_postgres::types::ToSql + Sync + Send>>> = rows
        .iter()
        .map(|row| database_replicator::xmin::writer::row_to_values(row, &columns))
        .collect();

    // Apply to target
    let writer = ChangeWriter::new(&target_client);
    let affected = writer
        .apply_batch("public", &table_name, &pk_columns, &column_names, values)
        .await
        .expect("Failed to apply changes");

    assert_eq!(affected, 3, "Should have affected 3 rows");

    // Verify target has the data
    let target_count: i64 = target_client
        .query_one(
            &format!(r#"SELECT COUNT(*) FROM "public"."{}"#, table_name),
            &[],
        )
        .await
        .expect("Failed to count target rows")
        .get(0);

    assert_eq!(target_count, 3, "Target should have 3 rows");

    println!(
        "✓ ChangeWriter successfully applied {} rows to target",
        affected
    );

    // Cleanup
    cleanup_test_table(&source_client, &target_client, &table_name)
        .await
        .expect("Failed to cleanup");
}

/// Test: Reconciler detects deleted rows
#[tokio::test]
#[ignore]
async fn test_reconciler_detects_deletes() {
    let (source_url, target_url) =
        get_test_urls().expect("TEST_SOURCE_URL and TEST_TARGET_URL must be set");

    let source_client = database_replicator::postgres::connect(&source_url)
        .await
        .expect("Failed to connect to source");
    let target_client = database_replicator::postgres::connect(&target_url)
        .await
        .expect("Failed to connect to target");

    let table_name = test_table_name("reconcile");

    // Setup tables with same data on both
    setup_test_table(&source_client, &target_client, &table_name)
        .await
        .expect("Failed to setup test tables");

    // Insert same data into both
    for client in [&source_client, &target_client] {
        client
            .batch_execute(&format!(
                r#"INSERT INTO "public"."{}" (id, name, value) VALUES (1, 'keep', 100), (2, 'delete_me', 200), (3, 'also_keep', 300);"#,
                table_name
            ))
            .await
            .expect("Failed to insert data");
    }

    // Delete a row from source (simulating a delete that xmin sync won't see)
    source_client
        .batch_execute(&format!(
            r#"DELETE FROM "public"."{}" WHERE id = 2;"#,
            table_name
        ))
        .await
        .expect("Failed to delete from source");

    // Run reconciliation
    let reconciler = Reconciler::new(&source_client, &target_client);
    let pk_columns = vec!["id".to_string()];

    let orphaned = reconciler
        .find_orphaned_rows("public", &table_name, &pk_columns)
        .await
        .expect("Failed to find orphaned rows");

    assert_eq!(orphaned.len(), 1, "Should find 1 orphaned row");
    assert_eq!(orphaned[0][0], "2", "Orphaned row should be id=2");

    // Delete the orphaned rows
    let deleted = reconciler
        .reconcile_table("public", &table_name, &pk_columns)
        .await
        .expect("Failed to reconcile");

    assert_eq!(deleted, 1, "Should have deleted 1 row");

    // Verify target now matches source
    let (source_count, target_count) = reconciler
        .get_row_counts("public", &table_name)
        .await
        .expect("Failed to get counts");

    assert_eq!(
        source_count, target_count,
        "Counts should match after reconciliation"
    );

    println!(
        "✓ Reconciler detected and removed {} orphaned rows",
        deleted
    );

    // Cleanup
    cleanup_test_table(&source_client, &target_client, &table_name)
        .await
        .expect("Failed to cleanup");
}

/// Test: SyncState persists and recovers correctly
#[tokio::test]
#[ignore]
async fn test_sync_state_persistence() {
    let (source_url, target_url) =
        get_test_urls().expect("TEST_SOURCE_URL and TEST_TARGET_URL must be set");

    // Create a temporary directory for state
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let state_path = temp_dir.path().join("xmin_state.json");

    // Create initial state
    let mut state = SyncState::new(&source_url, &target_url);
    state.update_table("public", "test_table", 12345, 100);
    state.update_table("public", "another_table", 67890, 50);

    // Save state
    state.save(&state_path).await.expect("Failed to save state");

    assert!(state_path.exists(), "State file should exist");

    // Load state back
    let loaded_state = SyncState::load(&state_path)
        .await
        .expect("Failed to load state");

    // Verify state was preserved
    let table_state = loaded_state
        .get_table("public", "test_table")
        .expect("Should have test_table state");
    assert_eq!(table_state.last_xmin, 12345, "xmin should be preserved");
    assert_eq!(
        table_state.last_row_count, 100,
        "last_row_count should be preserved"
    );

    let another_state = loaded_state
        .get_table("public", "another_table")
        .expect("Should have another_table state");
    assert_eq!(another_state.last_xmin, 67890, "xmin should be preserved");

    println!("✓ SyncState successfully persisted and recovered");

    // temp_dir is automatically cleaned up when dropped
}

/// Test: SyncDaemon runs a full sync cycle
#[tokio::test]
#[ignore]
async fn test_sync_daemon_full_cycle() {
    let (source_url, target_url) =
        get_test_urls().expect("TEST_SOURCE_URL and TEST_TARGET_URL must be set");

    let source_client = database_replicator::postgres::connect(&source_url)
        .await
        .expect("Failed to connect to source");
    let target_client = database_replicator::postgres::connect(&target_url)
        .await
        .expect("Failed to connect to target");

    let table_name = test_table_name("daemon");

    // Setup tables
    setup_test_table(&source_client, &target_client, &table_name)
        .await
        .expect("Failed to setup test tables");

    // Insert data into source
    source_client
        .batch_execute(&format!(
            r#"INSERT INTO "public"."{}" (name, value) VALUES ('row1', 10), ('row2', 20), ('row3', 30);"#,
            table_name
        ))
        .await
        .expect("Failed to insert source data");

    // Create temp state file
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let state_path = temp_dir.path().join("daemon_state.json");

    // Configure daemon for this specific table
    let config = DaemonConfig {
        sync_interval: Duration::from_secs(60),
        reconcile_interval: Some(Duration::from_secs(3600)),
        state_path: state_path.clone(),
        batch_size: 1000,
        tables: vec![table_name.clone()],
        schema: "public".to_string(),
    };

    // Create and run single sync cycle
    let daemon = SyncDaemon::new(source_url.clone(), target_url.clone(), config);
    let stats = daemon.run_sync_cycle().await.expect("Sync cycle failed");

    assert!(stats.is_success(), "Sync should succeed without errors");
    assert_eq!(stats.tables_synced, 1, "Should sync 1 table");
    assert_eq!(stats.rows_synced, 3, "Should sync 3 rows");

    // Verify target has the data
    let target_count: i64 = target_client
        .query_one(
            &format!(r#"SELECT COUNT(*) FROM "public"."{}"#, table_name),
            &[],
        )
        .await
        .expect("Failed to count target rows")
        .get(0);

    assert_eq!(target_count, 3, "Target should have 3 rows");

    // Verify state was saved
    assert!(state_path.exists(), "State file should be created");

    println!(
        "✓ SyncDaemon completed full cycle: {} tables, {} rows in {}ms",
        stats.tables_synced, stats.rows_synced, stats.duration_ms
    );

    // Cleanup
    cleanup_test_table(&source_client, &target_client, &table_name)
        .await
        .expect("Failed to cleanup");
}

/// Test: End-to-end xmin sync with updates
#[tokio::test]
#[ignore]
async fn test_xmin_sync_with_updates() {
    let (source_url, target_url) =
        get_test_urls().expect("TEST_SOURCE_URL and TEST_TARGET_URL must be set");

    let source_client = database_replicator::postgres::connect(&source_url)
        .await
        .expect("Failed to connect to source");
    let target_client = database_replicator::postgres::connect(&target_url)
        .await
        .expect("Failed to connect to target");

    let table_name = test_table_name("updates");

    // Setup tables
    setup_test_table(&source_client, &target_client, &table_name)
        .await
        .expect("Failed to setup test tables");

    // Create temp state file
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let state_path = temp_dir.path().join("updates_state.json");

    let config = DaemonConfig {
        sync_interval: Duration::from_secs(60),
        reconcile_interval: None, // Disable reconciliation for this test
        state_path,
        batch_size: 1000,
        tables: vec![table_name.clone()],
        schema: "public".to_string(),
    };

    let daemon = SyncDaemon::new(source_url.clone(), target_url.clone(), config);

    // Step 1: Insert initial data
    source_client
        .batch_execute(&format!(
            r#"INSERT INTO "public"."{}" (name, value) VALUES ('item', 100);"#,
            table_name
        ))
        .await
        .expect("Failed to insert initial data");

    // First sync
    let stats1 = daemon.run_sync_cycle().await.expect("First sync failed");
    assert_eq!(stats1.rows_synced, 1, "Should sync 1 row initially");

    // Step 2: Update the row in source
    source_client
        .batch_execute(&format!(
            r#"UPDATE "public"."{}" SET value = 999 WHERE name = 'item';"#,
            table_name
        ))
        .await
        .expect("Failed to update data");

    // Second sync - should pick up the update
    let stats2 = daemon.run_sync_cycle().await.expect("Second sync failed");
    assert_eq!(stats2.rows_synced, 1, "Should sync 1 updated row");

    // Verify target has updated value
    let target_value: i32 = target_client
        .query_one(
            &format!(
                r#"SELECT value FROM "public"."{}" WHERE name = 'item'"#,
                table_name
            ),
            &[],
        )
        .await
        .expect("Failed to query target")
        .get(0);

    assert_eq!(target_value, 999, "Target should have updated value");

    println!(
        "✓ xmin sync correctly propagated update: value={}",
        target_value
    );

    // Cleanup
    cleanup_test_table(&source_client, &target_client, &table_name)
        .await
        .expect("Failed to cleanup");
}

/// Test: Wraparound detection function
#[test]
fn test_wraparound_detection_logic() {
    use database_replicator::xmin::reader::{detect_wraparound, WraparoundCheck};

    // Normal case: current > old
    assert_eq!(detect_wraparound(100, 200), WraparoundCheck::Normal);

    // Normal case: slight decrease (not wraparound)
    assert_eq!(detect_wraparound(1000, 900), WraparoundCheck::Normal);

    // Wraparound case: large decrease
    assert_eq!(
        detect_wraparound(3_500_000_000, 100),
        WraparoundCheck::WraparoundDetected
    );

    // Edge case: at threshold
    assert_eq!(
        detect_wraparound(2_000_000_002, 1),
        WraparoundCheck::WraparoundDetected
    );

    // Edge case: just under threshold
    assert_eq!(detect_wraparound(2_000_000_001, 1), WraparoundCheck::Normal);

    println!("✓ Wraparound detection logic verified");
}
