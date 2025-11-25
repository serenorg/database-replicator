// ABOUTME: Performance benchmarks for database migrations across all supported database types
// ABOUTME: Tests migration speed, JSONB conversion performance, and resource usage

use database_replicator::commands;
use rusqlite::Connection;
use std::env;
use std::fs;
use std::time::Instant;

/// Helper to get test PostgreSQL target URL from environment
fn get_test_target_url() -> Option<String> {
    env::var("TEST_TARGET_URL").ok()
}

/// Helper to get test MongoDB source URL from environment
fn get_test_mongodb_url() -> Option<String> {
    env::var("TEST_MONGODB_URL").ok()
}

/// Helper to get test MySQL source URL from environment
fn get_test_mysql_url() -> Option<String> {
    env::var("TEST_MYSQL_URL").ok()
}

/// Create a small SQLite database (~1 MB)
fn create_small_sqlite_db() -> anyhow::Result<String> {
    let path = "/tmp/perf_sqlite_small.db";
    let _ = fs::remove_file(path);

    let conn = Connection::open(path)?;

    conn.execute_batch(
        "
        CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT NOT NULL,
            age INTEGER,
            balance REAL,
            bio TEXT,
            avatar BLOB
        );
    ",
    )?;

    // Insert 1,000 rows (~1 MB)
    for i in 0..1000 {
        conn.execute(
            "INSERT INTO users VALUES (?, ?, ?, ?, ?, ?, ?)",
            rusqlite::params![
                i,
                format!("User {}", i),
                format!("user{}@example.com", i),
                20 + (i % 50),
                100.0 + (i as f64 * 0.5),
                format!("Biography for user {} with some text to make it larger", i),
                vec![0u8; 100], // 100-byte blob
            ],
        )?;
    }

    Ok(path.to_string())
}

/// Create a medium SQLite database (~10 MB)
fn create_medium_sqlite_db() -> anyhow::Result<String> {
    let path = "/tmp/perf_sqlite_medium.db";
    let _ = fs::remove_file(path);

    let conn = Connection::open(path)?;

    conn.execute_batch(
        "
        CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT NOT NULL,
            age INTEGER,
            balance REAL,
            bio TEXT,
            avatar BLOB
        );

        CREATE TABLE posts (
            id INTEGER PRIMARY KEY,
            user_id INTEGER,
            title TEXT NOT NULL,
            content TEXT,
            published INTEGER
        );
    ",
    )?;

    // Insert 10,000 users
    for i in 0..10000 {
        conn.execute(
            "INSERT INTO users VALUES (?, ?, ?, ?, ?, ?, ?)",
            rusqlite::params![
                i,
                format!("User {}", i),
                format!("user{}@example.com", i),
                20 + (i % 50),
                100.0 + (i as f64 * 0.5),
                format!("Biography for user {} with some text to make it larger", i),
                vec![0u8; 100],
            ],
        )?;
    }

    // Insert 20,000 posts
    for i in 0..20000 {
        conn.execute(
            "INSERT INTO posts VALUES (?, ?, ?, ?, ?)",
            rusqlite::params![
                i,
                i % 10000, // user_id
                format!("Post {}", i),
                format!(
                    "Content for post {} with some longer text to simulate real posts",
                    i
                ),
                i % 2,
            ],
        )?;
    }

    Ok(path.to_string())
}

/// Create a large SQLite database (~100 MB)
fn create_large_sqlite_db() -> anyhow::Result<String> {
    let path = "/tmp/perf_sqlite_large.db";
    let _ = fs::remove_file(path);

    let conn = Connection::open(path)?;

    conn.execute_batch(
        "
        CREATE TABLE events (
            id INTEGER PRIMARY KEY,
            user_id INTEGER,
            event_type TEXT,
            timestamp INTEGER,
            data TEXT,
            metadata TEXT
        );
    ",
    )?;

    // Insert 100,000 rows
    for i in 0..100000 {
        conn.execute(
            "INSERT INTO events VALUES (?, ?, ?, ?, ?, ?)",
            rusqlite::params![
                i,
                i % 10000,
                format!("event_type_{}", i % 10),
                1700000000 + i,
                format!(
                    "Data for event {} with longer text content to increase size",
                    i
                ),
                format!("Metadata JSON-like content for event {}", i),
            ],
        )?;
    }

    Ok(path.to_string())
}

/// Cleanup test files
fn cleanup_perf_files() {
    let _ = fs::remove_file("/tmp/perf_sqlite_small.db");
    let _ = fs::remove_file("/tmp/perf_sqlite_medium.db");
    let _ = fs::remove_file("/tmp/perf_sqlite_large.db");
}

// ============================================================================
// SQLite Performance Tests
// ============================================================================

#[tokio::test]
#[ignore]
async fn benchmark_sqlite_small_migration() {
    let target_url = get_test_target_url().expect("TEST_TARGET_URL must be set");

    println!("\n=== SQLite Small Database Benchmark (~1 MB, 1K rows) ===");

    let sqlite_path = create_small_sqlite_db().expect("Failed to create small database");
    println!("Created small SQLite database at {}", sqlite_path);

    let start = Instant::now();
    let result = commands::init(
        &sqlite_path,
        &target_url,
        true,
        database_replicator::filters::ReplicationFilter::empty(),
        false,
        false,
        true,
    )
    .await;
    let elapsed = start.elapsed();

    assert!(result.is_ok(), "Migration should succeed");
    println!("✓ Migrated small SQLite database in {:?}", elapsed);
    println!(
        "  Performance: {:.2} rows/sec",
        1000.0 / elapsed.as_secs_f64()
    );

    // Performance target: < 10 seconds
    assert!(
        elapsed.as_secs() < 10,
        "Small database should migrate in < 10 seconds (took {:?})",
        elapsed
    );

    cleanup_perf_files();
}

#[tokio::test]
#[ignore]
async fn benchmark_sqlite_medium_migration() {
    let target_url = get_test_target_url().expect("TEST_TARGET_URL must be set");

    println!("\n=== SQLite Medium Database Benchmark (~10 MB, 30K rows) ===");

    let sqlite_path = create_medium_sqlite_db().expect("Failed to create medium database");
    println!("Created medium SQLite database at {}", sqlite_path);

    let start = Instant::now();
    let result = commands::init(
        &sqlite_path,
        &target_url,
        true,
        database_replicator::filters::ReplicationFilter::empty(),
        false,
        false,
        true,
    )
    .await;
    let elapsed = start.elapsed();

    assert!(result.is_ok(), "Migration should succeed");
    println!("✓ Migrated medium SQLite database in {:?}", elapsed);
    println!(
        "  Performance: {:.2} rows/sec",
        30000.0 / elapsed.as_secs_f64()
    );

    // Performance target: < 60 seconds
    assert!(
        elapsed.as_secs() < 60,
        "Medium database should migrate in < 60 seconds (took {:?})",
        elapsed
    );

    cleanup_perf_files();
}

#[tokio::test]
#[ignore]
async fn benchmark_sqlite_large_migration() {
    let target_url = get_test_target_url().expect("TEST_TARGET_URL must be set");

    println!("\n=== SQLite Large Database Benchmark (~100 MB, 100K rows) ===");

    let sqlite_path = create_large_sqlite_db().expect("Failed to create large database");
    println!("Created large SQLite database at {}", sqlite_path);

    let start = Instant::now();
    let result = commands::init(
        &sqlite_path,
        &target_url,
        true,
        database_replicator::filters::ReplicationFilter::empty(),
        false,
        false,
        true,
    )
    .await;
    let elapsed = start.elapsed();

    assert!(result.is_ok(), "Migration should succeed");
    println!("✓ Migrated large SQLite database in {:?}", elapsed);
    println!(
        "  Performance: {:.2} rows/sec",
        100000.0 / elapsed.as_secs_f64()
    );

    // Performance target: < 10 minutes
    assert!(
        elapsed.as_secs() < 600,
        "Large database should migrate in < 10 minutes (took {:?})",
        elapsed
    );

    cleanup_perf_files();
}

// ============================================================================
// MongoDB Performance Tests
// ============================================================================

#[tokio::test]
#[ignore]
async fn benchmark_mongodb_small_collection() {
    let source_url = get_test_mongodb_url().expect("TEST_MONGODB_URL must be set");
    let target_url = get_test_target_url().expect("TEST_TARGET_URL must be set");

    println!("\n=== MongoDB Small Collection Benchmark (<10K documents) ===");
    println!("Note: MongoDB must have a 'small_test' database with sample data");

    let start = Instant::now();
    let result = commands::init(
        &source_url,
        &target_url,
        true,
        database_replicator::filters::ReplicationFilter::empty(),
        false,
        false,
        true,
    )
    .await;
    let elapsed = start.elapsed();

    assert!(result.is_ok(), "Migration should succeed");
    println!("✓ Migrated small MongoDB collection in {:?}", elapsed);

    // Performance target: < 30 seconds
    assert!(
        elapsed.as_secs() < 30,
        "Small collection should migrate in < 30 seconds (took {:?})",
        elapsed
    );
}

#[tokio::test]
#[ignore]
async fn benchmark_mongodb_medium_collection() {
    let source_url = get_test_mongodb_url().expect("TEST_MONGODB_URL must be set");
    let target_url = get_test_target_url().expect("TEST_TARGET_URL must be set");

    println!("\n=== MongoDB Medium Collection Benchmark (10K-100K documents) ===");
    println!("Note: MongoDB must have a 'medium_test' database with sample data");

    let start = Instant::now();
    let result = commands::init(
        &source_url,
        &target_url,
        true,
        database_replicator::filters::ReplicationFilter::empty(),
        false,
        false,
        true,
    )
    .await;
    let elapsed = start.elapsed();

    assert!(result.is_ok(), "Migration should succeed");
    println!("✓ Migrated medium MongoDB collection in {:?}", elapsed);

    // Performance target: < 5 minutes
    assert!(
        elapsed.as_secs() < 300,
        "Medium collection should migrate in < 5 minutes (took {:?})",
        elapsed
    );
}

// ============================================================================
// MySQL Performance Tests
// ============================================================================

#[tokio::test]
#[ignore]
async fn benchmark_mysql_small_table() {
    let source_url = get_test_mysql_url().expect("TEST_MYSQL_URL must be set");
    let target_url = get_test_target_url().expect("TEST_TARGET_URL must be set");

    println!("\n=== MySQL Small Table Benchmark (<10K rows) ===");
    println!("Note: MySQL must have a 'small_test' database with sample data");

    let start = Instant::now();
    let result = commands::init(
        &source_url,
        &target_url,
        true,
        database_replicator::filters::ReplicationFilter::empty(),
        false,
        false,
        true,
    )
    .await;
    let elapsed = start.elapsed();

    assert!(result.is_ok(), "Migration should succeed");
    println!("✓ Migrated small MySQL table in {:?}", elapsed);

    // Performance target: < 30 seconds
    assert!(
        elapsed.as_secs() < 30,
        "Small table should migrate in < 30 seconds (took {:?})",
        elapsed
    );
}

#[tokio::test]
#[ignore]
async fn benchmark_mysql_medium_table() {
    let source_url = get_test_mysql_url().expect("TEST_MYSQL_URL must be set");
    let target_url = get_test_target_url().expect("TEST_TARGET_URL must be set");

    println!("\n=== MySQL Medium Table Benchmark (10K-100K rows) ===");
    println!("Note: MySQL must have a 'medium_test' database with sample data");

    let start = Instant::now();
    let result = commands::init(
        &source_url,
        &target_url,
        true,
        database_replicator::filters::ReplicationFilter::empty(),
        false,
        false,
        true,
    )
    .await;
    let elapsed = start.elapsed();

    assert!(result.is_ok(), "Migration should succeed");
    println!("✓ Migrated medium MySQL table in {:?}", elapsed);

    // Performance target: < 5 minutes
    assert!(
        elapsed.as_secs() < 300,
        "Medium table should migrate in < 5 minutes (took {:?})",
        elapsed
    );
}

// ============================================================================
// JSONB Performance Tests
// ============================================================================

#[tokio::test]
#[ignore]
async fn benchmark_jsonb_batch_insert() {
    let target_url = get_test_target_url().expect("TEST_TARGET_URL must be set");

    println!("\n=== JSONB Batch Insert Benchmark (10K rows) ===");

    // Create a small SQLite database for testing JSONB inserts
    let sqlite_path = create_small_sqlite_db().expect("Failed to create test database");

    let start = Instant::now();
    let result = commands::init(
        &sqlite_path,
        &target_url,
        true,
        database_replicator::filters::ReplicationFilter::empty(),
        false,
        false,
        true,
    )
    .await;
    let elapsed = start.elapsed();

    assert!(result.is_ok(), "Migration should succeed");
    println!("✓ Inserted JSONB batch in {:?}", elapsed);
    println!(
        "  Performance: {:.2} rows/sec",
        1000.0 / elapsed.as_secs_f64()
    );

    // Performance target: < 5 seconds
    assert!(
        elapsed.as_secs() < 5,
        "Batch insert should complete in < 5 seconds (took {:?})",
        elapsed
    );

    cleanup_perf_files();
}

// ============================================================================
// Helper Tests
// ============================================================================

#[tokio::test]
#[ignore]
async fn benchmark_connection_overhead() {
    let target_url = get_test_target_url().expect("TEST_TARGET_URL must be set");

    println!("\n=== Connection Overhead Benchmark ===");

    let start = Instant::now();
    let client = database_replicator::postgres::connection::connect(&target_url)
        .await
        .expect("Connection should succeed");
    let elapsed = start.elapsed();

    // Query to ensure connection is fully established
    let _ = client
        .query("SELECT version()", &[])
        .await
        .expect("Query should succeed");

    println!("✓ Connection established in {:?}", elapsed);

    // Performance target: < 1 second
    assert!(
        elapsed.as_secs() < 1,
        "Connection should establish in < 1 second (took {:?})",
        elapsed
    );
}

#[tokio::test]
#[ignore]
async fn benchmark_many_small_tables() {
    let target_url = get_test_target_url().expect("TEST_TARGET_URL must be set");

    println!("\n=== Many Small Tables Benchmark (10 tables, 100 rows each) ===");

    let path = "/tmp/perf_sqlite_many_tables.db";
    let _ = fs::remove_file(path);

    let conn = Connection::open(path).expect("Failed to create database");

    // Create 10 small tables with 100 rows each
    for table_num in 0..10 {
        conn.execute(
            &format!(
                "CREATE TABLE table_{} (
                    id INTEGER PRIMARY KEY,
                    data TEXT
                )",
                table_num
            ),
            [],
        )
        .expect("Failed to create table");

        for row_num in 0..100 {
            conn.execute(
                &format!("INSERT INTO table_{} VALUES (?, ?)", table_num),
                rusqlite::params![row_num, format!("Data for row {}", row_num)],
            )
            .expect("Failed to insert row");
        }
    }

    let start = Instant::now();
    let result = commands::init(
        path,
        &target_url,
        true,
        database_replicator::filters::ReplicationFilter::empty(),
        false,
        false,
        true,
    )
    .await;
    let elapsed = start.elapsed();

    assert!(result.is_ok(), "Migration should succeed");
    println!("✓ Migrated many small tables in {:?}", elapsed);

    // Performance target: < 30 seconds
    assert!(
        elapsed.as_secs() < 30,
        "Many small tables should migrate in < 30 seconds (took {:?})",
        elapsed
    );

    let _ = fs::remove_file(path);
}
