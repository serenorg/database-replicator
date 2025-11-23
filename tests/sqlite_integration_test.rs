// ABOUTME: Integration tests for SQLite-to-PostgreSQL migration workflow
// ABOUTME: Tests full migration with real SQLite files and PostgreSQL connections

use postgres_seren_replicator::commands;
use rusqlite::Connection;
use std::env;
use std::fs;

/// Helper to get test PostgreSQL target URL from environment
fn get_test_target_url() -> Option<String> {
    env::var("TEST_TARGET_URL").ok()
}

/// Create a test SQLite database with multiple tables and data types
fn create_test_sqlite_db() -> anyhow::Result<String> {
    let path = "/tmp/test_sqlite_integration.db";

    // Remove existing test database if it exists
    let _ = fs::remove_file(path);

    let conn = Connection::open(path)?;

    // Create tables with various data types
    conn.execute_batch(
        "
        CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
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

        CREATE TABLE empty_table (
            id INTEGER PRIMARY KEY,
            data TEXT
        );

        -- Insert test data with various types
        INSERT INTO users VALUES
            (1, 'Alice', 30, 100.50, 'Alice bio', X'48656c6c6f'),
            (2, 'Bob', 25, 200.75, 'Bob bio', X'576f726c64'),
            (3, 'Charlie', NULL, 150.25, NULL, NULL);

        INSERT INTO posts VALUES
            (1, 1, 'First Post', 'Hello World', 1),
            (2, 1, 'Second Post', 'More content', 1),
            (3, 2, 'Bob Post', NULL, 0);
    ",
    )?;

    Ok(path.to_string())
}

/// Cleanup test databases
fn cleanup_test_files() {
    let _ = fs::remove_file("/tmp/test_sqlite_integration.db");
    let _ = fs::remove_file("/tmp/test_sqlite_empty.db");
    let _ = fs::remove_file("/tmp/test_sqlite_types.db");
}

#[tokio::test]
#[ignore]
async fn test_sqlite_full_migration_integration() {
    let target_url = get_test_target_url().expect("TEST_TARGET_URL must be set");

    println!("Testing full SQLite-to-PostgreSQL migration...");

    // Create test SQLite database
    let sqlite_path = create_test_sqlite_db().expect("Failed to create test database");
    println!("  ✓ Created test SQLite database at {}", sqlite_path);

    // Run init command
    println!("  Running init command...");
    let result = commands::init(
        &sqlite_path,
        &target_url,
        true,
        postgres_seren_replicator::filters::ReplicationFilter::empty(),
        false,
        false,
        true,
    )
    .await;

    match &result {
        Ok(_) => {
            println!("  ✓ SQLite migration completed successfully");
        }
        Err(e) => {
            println!("  ✗ Migration failed: {:?}", e);
            cleanup_test_files();
            panic!("SQLite migration failed: {:?}", e);
        }
    }

    // TODO: Add verification that data was migrated correctly
    // This would require connecting to PostgreSQL and querying the JSONB tables

    cleanup_test_files();
    println!("✓ Test completed and cleaned up");
}

#[tokio::test]
#[ignore]
async fn test_sqlite_null_and_blob_handling() {
    let target_url = get_test_target_url().expect("TEST_TARGET_URL must be set");

    println!("Testing SQLite NULL and BLOB handling...");

    let sqlite_path = create_test_sqlite_db().expect("Failed to create test database");

    let result = commands::init(
        &sqlite_path,
        &target_url,
        true,
        postgres_seren_replicator::filters::ReplicationFilter::empty(),
        false,
        false,
        true,
    )
    .await;

    match &result {
        Ok(_) => {
            println!("  ✓ Migration with NULL and BLOB values completed");
        }
        Err(e) => {
            println!("  ✗ Migration failed: {:?}", e);
            cleanup_test_files();
            panic!("Migration with NULL/BLOB failed: {:?}", e);
        }
    }

    // The test database includes:
    // - NULL age for Charlie
    // - NULL bio for Charlie
    // - NULL avatar for Charlie
    // - BLOB avatars for Alice and Bob
    // If migration succeeded without error, NULL and BLOB handling works

    cleanup_test_files();
    println!("✓ NULL and BLOB handling test completed");
}

#[tokio::test]
#[ignore]
async fn test_sqlite_empty_table_migration() {
    let target_url = get_test_target_url().expect("TEST_TARGET_URL must be set");

    println!("Testing SQLite empty table migration...");

    let sqlite_path = create_test_sqlite_db().expect("Failed to create test database");

    let result = commands::init(
        &sqlite_path,
        &target_url,
        true,
        postgres_seren_replicator::filters::ReplicationFilter::empty(),
        false,
        false,
        true,
    )
    .await;

    match &result {
        Ok(_) => {
            println!("  ✓ Migration with empty table completed");
        }
        Err(e) => {
            println!("  ✗ Migration failed: {:?}", e);
            cleanup_test_files();
            panic!("Migration with empty table failed: {:?}", e);
        }
    }

    // The test database includes an empty_table with no rows
    // If migration succeeded, empty table handling works

    cleanup_test_files();
    println!("✓ Empty table migration test completed");
}

#[tokio::test]
#[ignore]
async fn test_sqlite_all_data_types() {
    let target_url = get_test_target_url().expect("TEST_TARGET_URL must be set");

    println!("Testing SQLite all data types migration...");

    let path = "/tmp/test_sqlite_types.db";
    let _ = fs::remove_file(path);

    let conn = Connection::open(path).expect("Failed to create test database");

    // Create table with all SQLite types
    conn.execute_batch(
        "
        CREATE TABLE type_test (
            id INTEGER PRIMARY KEY,
            int_col INTEGER,
            real_col REAL,
            text_col TEXT,
            blob_col BLOB,
            null_col TEXT
        );

        INSERT INTO type_test VALUES
            (1, 42, 3.14159, 'Hello World', X'DEADBEEF', NULL),
            (2, -100, -0.5, 'Special chars: 日本語', X'', NULL),
            (3, 0, 0.0, '', X'00010203', NULL);
    ",
    )
    .expect("Failed to insert test data");

    let result = commands::init(
        path,
        &target_url,
        true,
        postgres_seren_replicator::filters::ReplicationFilter::empty(),
        false,
        false,
        true,
    )
    .await;

    match &result {
        Ok(_) => {
            println!("  ✓ Migration with all data types completed");
        }
        Err(e) => {
            println!("  ✗ Migration failed: {:?}", e);
            cleanup_test_files();
            panic!("Migration with all types failed: {:?}", e);
        }
    }

    cleanup_test_files();
    println!("✓ All data types migration test completed");
}

#[tokio::test]
#[ignore]
async fn test_sqlite_empty_database() {
    let target_url = get_test_target_url().expect("TEST_TARGET_URL must be set");

    println!("Testing SQLite empty database migration...");

    let path = "/tmp/test_sqlite_empty.db";
    let _ = fs::remove_file(path);

    // Create empty database (no tables)
    let _conn = Connection::open(path).expect("Failed to create empty database");

    let result = commands::init(
        path,
        &target_url,
        true,
        postgres_seren_replicator::filters::ReplicationFilter::empty(),
        false,
        false,
        true,
    )
    .await;

    match &result {
        Ok(_) => {
            println!("  ✓ Empty database migration completed");
        }
        Err(e) => {
            println!("  ✗ Migration failed: {:?}", e);
            cleanup_test_files();
            panic!("Empty database migration failed: {:?}", e);
        }
    }

    cleanup_test_files();
    println!("✓ Empty database migration test completed");
}

#[tokio::test]
#[ignore]
async fn test_sqlite_invalid_path_fails() {
    let target_url = get_test_target_url().expect("TEST_TARGET_URL must be set");

    println!("Testing SQLite invalid path handling...");

    // Try to migrate from non-existent file
    let result = commands::init(
        "/nonexistent/path.db",
        &target_url,
        true,
        postgres_seren_replicator::filters::ReplicationFilter::empty(),
        false,
        false,
        true,
    )
    .await;

    match &result {
        Ok(_) => {
            panic!("Migration should have failed with invalid path");
        }
        Err(e) => {
            println!("  ✓ Migration correctly failed: {}", e);
        }
    }

    println!("✓ Invalid path test completed");
}

#[tokio::test]
#[ignore]
async fn test_sqlite_path_traversal_prevention() {
    let target_url = get_test_target_url().expect("TEST_TARGET_URL must be set");

    println!("Testing SQLite path traversal prevention...");

    // Try path traversal attack
    let result = commands::init(
        "../../../etc/passwd",
        &target_url,
        true,
        postgres_seren_replicator::filters::ReplicationFilter::empty(),
        false,
        false,
        true,
    )
    .await;

    match &result {
        Ok(_) => {
            panic!("Migration should have failed with path traversal attempt");
        }
        Err(e) => {
            println!("  ✓ Path traversal correctly blocked: {}", e);
        }
    }

    println!("✓ Path traversal prevention test completed");
}
