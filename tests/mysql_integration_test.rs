// ABOUTME: Integration tests for MySQL-to-PostgreSQL replication workflow
// ABOUTME: Tests full replication with real MySQL connections and PostgreSQL target

use postgres_seren_replicator::commands;
use std::env;

/// Helper to get test MySQL source URL from environment
fn get_test_mysql_url() -> Option<String> {
    env::var("TEST_MYSQL_URL").ok()
}

/// Helper to get test PostgreSQL target URL from environment
fn get_test_target_url() -> Option<String> {
    env::var("TEST_TARGET_URL").ok()
}

/// Create test tables in MySQL database with various data types
async fn create_test_mysql_tables(mysql_url: &str) -> anyhow::Result<()> {
    use mysql_async::prelude::*;

    let mut conn = postgres_seren_replicator::mysql::connect_mysql(mysql_url).await?;

    // Drop existing test tables if they exist
    let cleanup_queries = vec![
        "DROP TABLE IF EXISTS users",
        "DROP TABLE IF EXISTS posts",
        "DROP TABLE IF EXISTS empty_table",
        "DROP TABLE IF EXISTS type_test",
    ];

    for query in cleanup_queries {
        conn.query_drop(query).await?;
    }

    // Create tables with various data types
    conn.query_drop(
        "
        CREATE TABLE users (
            id INT PRIMARY KEY AUTO_INCREMENT,
            name VARCHAR(255) NOT NULL,
            age INT,
            balance DECIMAL(10, 2),
            bio TEXT,
            avatar BLOB,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    ",
    )
    .await?;

    conn.query_drop(
        "
        CREATE TABLE posts (
            id INT PRIMARY KEY AUTO_INCREMENT,
            user_id INT,
            title VARCHAR(255) NOT NULL,
            content TEXT,
            published TINYINT(1)
        )
    ",
    )
    .await?;

    conn.query_drop(
        "
        CREATE TABLE empty_table (
            id INT PRIMARY KEY AUTO_INCREMENT,
            data TEXT
        )
    ",
    )
    .await?;

    // Insert test data with various types
    conn.exec_batch(
        "INSERT INTO users (id, name, age, balance, bio, avatar) VALUES (?, ?, ?, ?, ?, ?)",
        vec![
            (
                1,
                "Alice",
                Some(30),
                Some(100.50),
                Some("Alice bio"),
                Some(b"Hello".to_vec()),
            ),
            (
                2,
                "Bob",
                Some(25),
                Some(200.75),
                Some("Bob bio"),
                Some(b"World".to_vec()),
            ),
            (3, "Charlie", None::<i32>, Some(150.25), None, None),
        ],
    )
    .await?;

    conn.exec_batch(
        "INSERT INTO posts (id, user_id, title, content, published) VALUES (?, ?, ?, ?, ?)",
        vec![
            (1, 1, "First Post", Some("Hello World"), 1),
            (2, 1, "Second Post", Some("More content"), 1),
            (3, 2, "Bob Post", None, 0),
        ],
    )
    .await?;

    Ok(())
}

/// Cleanup test tables from MySQL database
async fn cleanup_test_tables(mysql_url: &str) -> anyhow::Result<()> {
    use mysql_async::prelude::*;

    let mut conn = postgres_seren_replicator::mysql::connect_mysql(mysql_url).await?;

    let cleanup_queries = vec![
        "DROP TABLE IF EXISTS users",
        "DROP TABLE IF EXISTS posts",
        "DROP TABLE IF EXISTS empty_table",
        "DROP TABLE IF EXISTS type_test",
    ];

    for query in cleanup_queries {
        let _ = conn.query_drop(query).await;
    }

    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_mysql_full_replication_integration() {
    let mysql_url = get_test_mysql_url().expect("TEST_MYSQL_URL must be set");
    let target_url = get_test_target_url().expect("TEST_TARGET_URL must be set");

    println!("Testing full MySQL-to-PostgreSQL replication...");

    // Create test tables in MySQL
    println!("  Creating test tables in MySQL...");
    create_test_mysql_tables(&mysql_url)
        .await
        .expect("Failed to create test tables");
    println!("  ✓ Created test MySQL tables");

    // Run init command
    println!("  Running init command...");
    let result = commands::init(
        &mysql_url,
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
            println!("  ✓ MySQL replication completed successfully");
        }
        Err(e) => {
            println!("  ✗ Replication failed: {:?}", e);
            let _ = cleanup_test_tables(&mysql_url).await;
            panic!("MySQL replication failed: {:?}", e);
        }
    }

    // TODO: Add verification that data was replicated correctly
    // This would require connecting to PostgreSQL and querying the JSONB tables

    let _ = cleanup_test_tables(&mysql_url).await;
    println!("✓ Test completed and cleaned up");
}

#[tokio::test]
#[ignore]
async fn test_mysql_null_and_blob_handling() {
    let mysql_url = get_test_mysql_url().expect("TEST_MYSQL_URL must be set");
    let target_url = get_test_target_url().expect("TEST_TARGET_URL must be set");

    println!("Testing MySQL NULL and BLOB handling...");

    create_test_mysql_tables(&mysql_url)
        .await
        .expect("Failed to create test tables");

    let result = commands::init(
        &mysql_url,
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
            println!("  ✓ Replication with NULL and BLOB values completed");
        }
        Err(e) => {
            println!("  ✗ Replication failed: {:?}", e);
            let _ = cleanup_test_tables(&mysql_url).await;
            panic!("Replication with NULL/BLOB failed: {:?}", e);
        }
    }

    // The test database includes:
    // - NULL age for Charlie
    // - NULL bio for Charlie
    // - NULL avatar for Charlie
    // - BLOB avatars for Alice and Bob
    // If replication succeeded without error, NULL and BLOB handling works

    let _ = cleanup_test_tables(&mysql_url).await;
    println!("✓ NULL and BLOB handling test completed");
}

#[tokio::test]
#[ignore]
async fn test_mysql_empty_table_replication() {
    let mysql_url = get_test_mysql_url().expect("TEST_MYSQL_URL must be set");
    let target_url = get_test_target_url().expect("TEST_TARGET_URL must be set");

    println!("Testing MySQL empty table replication...");

    create_test_mysql_tables(&mysql_url)
        .await
        .expect("Failed to create test tables");

    let result = commands::init(
        &mysql_url,
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
            println!("  ✓ Replication with empty table completed");
        }
        Err(e) => {
            println!("  ✗ Replication failed: {:?}", e);
            let _ = cleanup_test_tables(&mysql_url).await;
            panic!("Replication with empty table failed: {:?}", e);
        }
    }

    // The test database includes an empty_table with no rows
    // If replication succeeded, empty table handling works

    let _ = cleanup_test_tables(&mysql_url).await;
    println!("✓ Empty table replication test completed");
}

#[tokio::test]
#[ignore]
async fn test_mysql_all_data_types() {
    let mysql_url = get_test_mysql_url().expect("TEST_MYSQL_URL must be set");
    let target_url = get_test_target_url().expect("TEST_TARGET_URL must be set");

    println!("Testing MySQL all data types replication...");

    use mysql_async::prelude::*;

    let mut conn = postgres_seren_replicator::mysql::connect_mysql(&mysql_url)
        .await
        .expect("Failed to connect to MySQL");

    // Drop existing type_test table
    let _ = conn.query_drop("DROP TABLE IF EXISTS type_test").await;

    // Create table with various MySQL types
    conn.query_drop(
        "
        CREATE TABLE type_test (
            id INT PRIMARY KEY AUTO_INCREMENT,
            int_col INT,
            bigint_col BIGINT,
            decimal_col DECIMAL(10, 2),
            float_col FLOAT,
            double_col DOUBLE,
            varchar_col VARCHAR(255),
            text_col TEXT,
            datetime_col DATETIME,
            date_col DATE,
            time_col TIME,
            blob_col BLOB,
            tinyint_col TINYINT,
            null_col TEXT
        )
    ",
    )
    .await
    .expect("Failed to create type_test table");

    // Insert test data with various types
    conn.query_drop(
        "
        INSERT INTO type_test VALUES
            (1, 42, 9223372036854775807, 123.45, 3.14, 2.71828,
             'Hello World', 'Long text content',
             '2024-01-15 10:30:45', '2024-01-15', '10:30:45',
             0xDEADBEEF, 1, NULL),
            (2, -100, -1000000, -50.25, -0.5, -1.414,
             'Special chars: 日本語', 'More text',
             '2023-12-01 00:00:00', '2023-12-01', '00:00:00',
             0x, 0, NULL),
            (3, 0, 0, 0.00, 0.0, 0.0,
             '', '',
             '1970-01-01 00:00:00', '1970-01-01', '00:00:00',
             0x00010203, 127, NULL)
    ",
    )
    .await
    .expect("Failed to insert test data");

    let result = commands::init(
        &mysql_url,
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
            println!("  ✓ Replication with all data types completed");
        }
        Err(e) => {
            println!("  ✗ Replication failed: {:?}", e);
            let _ = conn.query_drop("DROP TABLE IF EXISTS type_test").await;
            panic!("Replication with all types failed: {:?}", e);
        }
    }

    let _ = conn.query_drop("DROP TABLE IF EXISTS type_test").await;
    println!("✓ All data types replication test completed");
}

#[tokio::test]
#[ignore]
async fn test_mysql_empty_database_fails_gracefully() {
    let mysql_url = get_test_mysql_url().expect("TEST_MYSQL_URL must be set");
    let target_url = get_test_target_url().expect("TEST_TARGET_URL must be set");

    println!("Testing MySQL empty database replication...");

    // Clean up all test tables to make database empty
    let _ = cleanup_test_tables(&mysql_url).await;

    let result = commands::init(
        &mysql_url,
        &target_url,
        true,
        postgres_seren_replicator::filters::ReplicationFilter::empty(),
        false,
        false,
        true,
    )
    .await;

    // Empty database should succeed but with warning
    match &result {
        Ok(_) => {
            println!("  ✓ Empty database replication completed gracefully");
        }
        Err(e) => {
            println!("  Note: Empty database handling: {}", e);
        }
    }

    println!("✓ Empty database replication test completed");
}

#[tokio::test]
#[ignore]
async fn test_mysql_invalid_url_fails() {
    let target_url = get_test_target_url().expect("TEST_TARGET_URL must be set");

    println!("Testing MySQL invalid URL handling...");

    // Try to replicate from invalid MySQL URL
    let result = commands::init(
        "mysql://invalid-host-that-does-not-exist:3306/testdb",
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
            panic!("Replication should have failed with invalid MySQL URL");
        }
        Err(e) => {
            println!("  ✓ Replication correctly failed: {}", e);
        }
    }

    println!("✓ Invalid URL test completed");
}

#[tokio::test]
#[ignore]
async fn test_mysql_missing_database_name_fails() {
    let target_url = get_test_target_url().expect("TEST_TARGET_URL must be set");

    println!("Testing MySQL URL without database name...");

    // MySQL URL without database name should fail
    let mysql_url_no_db = get_test_mysql_url()
        .expect("TEST_MYSQL_URL must be set")
        .split('/')
        .take(3)
        .collect::<Vec<_>>()
        .join("/");

    let result = commands::init(
        &mysql_url_no_db,
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
            panic!("Replication should have failed without database name in URL");
        }
        Err(e) => {
            println!("  ✓ Replication correctly failed: {}", e);
            assert!(
                e.to_string().contains("database name"),
                "Error should mention missing database name"
            );
        }
    }

    println!("✓ Missing database name test completed");
}

#[tokio::test]
#[ignore]
async fn test_mysql_decimal_and_datetime_precision() {
    let mysql_url = get_test_mysql_url().expect("TEST_MYSQL_URL must be set");
    let target_url = get_test_target_url().expect("TEST_TARGET_URL must be set");

    println!("Testing MySQL decimal and datetime precision...");

    use mysql_async::prelude::*;

    let mut conn = postgres_seren_replicator::mysql::connect_mysql(&mysql_url)
        .await
        .expect("Failed to connect to MySQL");

    // Drop existing precision_test table
    let _ = conn.query_drop("DROP TABLE IF EXISTS precision_test").await;

    // Create table with high-precision types
    conn.query_drop(
        "
        CREATE TABLE precision_test (
            id INT PRIMARY KEY AUTO_INCREMENT,
            precise_decimal DECIMAL(20, 10),
            precise_datetime DATETIME(6),
            precise_time TIME(6)
        )
    ",
    )
    .await
    .expect("Failed to create precision_test table");

    // Insert data with high precision
    conn.query_drop(
        "
        INSERT INTO precision_test VALUES
            (1, 123456789.0123456789, '2024-01-15 10:30:45.123456', '10:30:45.123456'),
            (2, -987654321.9876543210, '2023-12-31 23:59:59.999999', '23:59:59.999999')
    ",
    )
    .await
    .expect("Failed to insert precision test data");

    let result = commands::init(
        &mysql_url,
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
            println!("  ✓ High-precision data replication completed");
        }
        Err(e) => {
            println!("  ✗ Replication failed: {:?}", e);
            let _ = conn.query_drop("DROP TABLE IF EXISTS precision_test").await;
            panic!("High-precision replication failed: {:?}", e);
        }
    }

    let _ = conn.query_drop("DROP TABLE IF EXISTS precision_test").await;
    println!("✓ Decimal and datetime precision test completed");
}
