// ABOUTME: SQLite database introspection and data reading
// ABOUTME: Functions to list tables, count rows, and read table data

use anyhow::{Context, Result};
use rusqlite::Connection;
use std::collections::HashMap;

/// List all user tables in a SQLite database
///
/// Queries sqlite_master system table for user-created tables.
/// Excludes:
/// - sqlite_* system tables (sqlite_sequence, sqlite_stat1, etc.)
/// - Internal tables
///
/// # Arguments
///
/// * `conn` - SQLite database connection
///
/// # Returns
///
/// Sorted vector of table names
///
/// # Examples
///
/// ```no_run
/// # use database_replicator::sqlite::{open_sqlite, reader::list_tables};
/// # fn example() -> anyhow::Result<()> {
/// let conn = open_sqlite("database.db")?;
/// let tables = list_tables(&conn)?;
/// for table in tables {
///     println!("Table: {}", table);
/// }
/// # Ok(())
/// # }
/// ```
pub fn list_tables(conn: &Connection) -> Result<Vec<String>> {
    tracing::debug!("Listing tables from SQLite database");

    let mut stmt = conn
        .prepare(
            "SELECT name FROM sqlite_master \
             WHERE type='table' \
             AND name NOT LIKE 'sqlite_%' \
             ORDER BY name",
        )
        .context("Failed to prepare statement to list tables")?;

    let tables = stmt
        .query_map([], |row| row.get::<_, String>(0))
        .context("Failed to query table list")?
        .collect::<Result<Vec<String>, _>>()
        .context("Failed to collect table names")?;

    tracing::info!("Found {} user tables in SQLite database", tables.len());

    Ok(tables)
}

/// Get row count for a specific table
///
/// Executes COUNT(*) query on the specified table.
///
/// # Arguments
///
/// * `conn` - SQLite database connection
/// * `table` - Table name (should be validated with validate_table_name)
///
/// # Returns
///
/// Number of rows in the table
///
/// # Security
///
/// IMPORTANT: Table name should be validated before calling this function
/// to prevent SQL injection. Use crate::jsonb::validate_table_name().
///
/// # Examples
///
/// ```no_run
/// # use database_replicator::sqlite::{open_sqlite, reader::get_table_row_count};
/// # use database_replicator::jsonb::validate_table_name;
/// # fn example() -> anyhow::Result<()> {
/// let conn = open_sqlite("database.db")?;
/// let table = "users";
/// validate_table_name(table)?;
/// let count = get_table_row_count(&conn, table)?;
/// println!("Table {} has {} rows", table, count);
/// # Ok(())
/// # }
/// ```
pub fn get_table_row_count(conn: &Connection, table: &str) -> Result<usize> {
    // Validate table name to prevent SQL injection
    crate::jsonb::validate_table_name(table).context("Invalid table name for row count query")?;

    tracing::debug!("Getting row count for table '{}'", table);

    // Note: table name is validated above, so it's safe to use in SQL
    let query = format!("SELECT COUNT(*) FROM {}", crate::utils::quote_ident(table));

    let count: i64 = conn
        .query_row(&query, [], |row| row.get(0))
        .with_context(|| format!("Failed to count rows in table '{}'", table))?;

    Ok(count as usize)
}

/// Batch reader state for iterating over large SQLite tables.
///
/// Uses rowid-based pagination to efficiently read tables in chunks
/// without loading all data into memory.
#[derive(Debug)]
pub struct BatchedTableReader {
    /// Table name being read
    pub table: String,
    /// Column names in the table
    pub columns: Vec<String>,
    /// Last rowid seen (for pagination)
    pub last_rowid: i64,
    /// Maximum rows per batch
    pub batch_size: usize,
    /// Whether all rows have been read
    pub exhausted: bool,
}

impl BatchedTableReader {
    /// Create a new batched reader for a table.
    ///
    /// # Arguments
    ///
    /// * `conn` - SQLite database connection
    /// * `table` - Table name (must be validated)
    /// * `batch_size` - Maximum rows per batch
    pub fn new(conn: &Connection, table: &str, batch_size: usize) -> Result<Self> {
        // Validate table name
        crate::jsonb::validate_table_name(table)
            .context("Invalid table name for batched reading")?;

        // Get column names
        let query = format!("SELECT * FROM {} LIMIT 0", crate::utils::quote_ident(table));
        let stmt = conn
            .prepare(&query)
            .with_context(|| format!("Failed to prepare statement for table '{}'", table))?;

        let columns: Vec<String> = stmt.column_names().iter().map(|s| s.to_string()).collect();

        Ok(Self {
            table: table.to_string(),
            columns,
            last_rowid: 0,
            batch_size,
            exhausted: false,
        })
    }
}

/// Read the next batch of rows from a table.
///
/// Uses rowid-based pagination for efficient batched reading.
/// SQLite's rowid is always present (even if not explicitly defined)
/// and provides stable ordering for pagination.
///
/// # Arguments
///
/// * `conn` - SQLite database connection
/// * `reader` - Mutable batch reader state
///
/// # Returns
///
/// Some(rows) if there are more rows, None if exhausted.
pub fn read_table_batch(
    conn: &Connection,
    reader: &mut BatchedTableReader,
) -> Result<Option<Vec<HashMap<String, rusqlite::types::Value>>>> {
    if reader.exhausted {
        return Ok(None);
    }

    // Query using rowid for stable pagination
    // rowid is always available in SQLite (alias for INTEGER PRIMARY KEY if defined)
    let query = format!(
        "SELECT rowid, * FROM {} WHERE rowid > ? ORDER BY rowid LIMIT ?",
        crate::utils::quote_ident(&reader.table)
    );

    let mut stmt = conn
        .prepare(&query)
        .with_context(|| format!("Failed to prepare batch query for table '{}'", reader.table))?;

    let column_names = &reader.columns;
    let last_rowid = reader.last_rowid;
    let batch_size = reader.batch_size as i64;

    let rows: Vec<HashMap<String, rusqlite::types::Value>> = stmt
        .query_map([last_rowid, batch_size], |row| {
            let mut row_map = HashMap::new();

            // First column is rowid (index 0), actual columns start at index 1
            for (idx, col_name) in column_names.iter().enumerate() {
                let value: rusqlite::types::Value = row.get(idx + 1)?;
                row_map.insert(col_name.clone(), value);
            }

            // Also store rowid for pagination tracking
            let rowid: i64 = row.get(0)?;
            row_map.insert("_rowid".to_string(), rusqlite::types::Value::Integer(rowid));

            Ok(row_map)
        })
        .with_context(|| format!("Failed to query batch from table '{}'", reader.table))?
        .collect::<Result<Vec<_>, _>>()
        .with_context(|| format!("Failed to collect batch from table '{}'", reader.table))?;

    if rows.is_empty() {
        reader.exhausted = true;
        return Ok(None);
    }

    // Update last_rowid from the last row for next iteration
    if let Some(last_row) = rows.last() {
        if let Some(rusqlite::types::Value::Integer(rowid)) = last_row.get("_rowid") {
            reader.last_rowid = *rowid;
        }
    }

    // Mark as exhausted if we got fewer rows than batch_size
    if rows.len() < reader.batch_size {
        reader.exhausted = true;
    }

    tracing::debug!(
        "Read batch of {} rows from '{}' (last_rowid={})",
        rows.len(),
        reader.table,
        reader.last_rowid
    );

    Ok(Some(rows))
}

/// Read all data from a SQLite table
///
/// Returns all rows as a vector of HashMaps, where each HashMap maps
/// column names to their values.
///
/// # Arguments
///
/// * `conn` - SQLite database connection
/// * `table` - Table name (should be validated)
///
/// # Returns
///
/// Vector of rows, each row is a HashMap<column_name, value>
///
/// # Security
///
/// Table name should be validated before calling this function.
///
/// # Performance
///
/// Loads all rows into memory. For large tables, use `BatchedTableReader`
/// and `read_table_batch()` instead.
///
/// # Examples
///
/// ```no_run
/// # use database_replicator::sqlite::{open_sqlite, reader::read_table_data};
/// # use database_replicator::jsonb::validate_table_name;
/// # fn example() -> anyhow::Result<()> {
/// let conn = open_sqlite("database.db")?;
/// let table = "users";
/// validate_table_name(table)?;
/// let rows = read_table_data(&conn, table)?;
/// println!("Read {} rows from {}", rows.len(), table);
/// # Ok(())
/// # }
/// ```
pub fn read_table_data(
    conn: &Connection,
    table: &str,
) -> Result<Vec<HashMap<String, rusqlite::types::Value>>> {
    // Validate table name to prevent SQL injection
    crate::jsonb::validate_table_name(table).context("Invalid table name for data reading")?;

    tracing::info!("Reading all data from table '{}'", table);

    // Note: table name is validated above
    let query = format!("SELECT * FROM {}", crate::utils::quote_ident(table));

    let mut stmt = conn
        .prepare(&query)
        .with_context(|| format!("Failed to prepare statement for table '{}'", table))?;

    // Get column names
    let column_names: Vec<String> = stmt.column_names().iter().map(|s| s.to_string()).collect();

    tracing::debug!(
        "Table '{}' has {} columns: {:?}",
        table,
        column_names.len(),
        column_names
    );

    // Read all rows
    let rows = stmt
        .query_map([], |row| {
            let mut row_map = HashMap::new();

            for (idx, col_name) in column_names.iter().enumerate() {
                // Get value from row
                // rusqlite::types::Value represents all SQLite types
                let value: rusqlite::types::Value = row.get(idx)?;
                row_map.insert(col_name.clone(), value);
            }

            Ok(row_map)
        })
        .with_context(|| format!("Failed to query rows from table '{}'", table))?
        .collect::<Result<Vec<_>, _>>()
        .with_context(|| format!("Failed to collect rows from table '{}'", table))?;

    tracing::info!("Read {} rows from table '{}'", rows.len(), table);

    Ok(rows)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_db() -> (tempfile::TempDir, std::path::PathBuf) {
        let temp_dir = tempfile::tempdir().unwrap();
        let db_path = temp_dir.path().join("test.db");

        let conn = Connection::open(&db_path).unwrap();

        // Create test tables
        conn.execute(
            "CREATE TABLE users (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                email TEXT,
                age INTEGER
            )",
            [],
        )
        .unwrap();

        conn.execute(
            "CREATE TABLE posts (
                id INTEGER PRIMARY KEY,
                user_id INTEGER,
                title TEXT NOT NULL,
                content TEXT
            )",
            [],
        )
        .unwrap();

        // Insert test data
        conn.execute(
            "INSERT INTO users (id, name, email, age) VALUES (1, 'Alice', 'alice@example.com', 30)",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO users (id, name, email, age) VALUES (2, 'Bob', 'bob@example.com', 25)",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO users (id, name, email) VALUES (3, 'Charlie', 'charlie@example.com')",
            [],
        )
        .unwrap();

        conn.execute(
            "INSERT INTO posts (id, user_id, title, content) VALUES (1, 1, 'First Post', 'Hello')",
            [],
        )
        .unwrap();

        (temp_dir, db_path)
    }

    #[test]
    fn test_list_tables() {
        let (_temp_dir, db_path) = create_test_db();
        let conn = Connection::open(db_path).unwrap();

        let tables = list_tables(&conn).unwrap();

        assert_eq!(tables.len(), 2);
        assert!(tables.contains(&"users".to_string()));
        assert!(tables.contains(&"posts".to_string()));
        assert_eq!(tables, vec!["posts", "users"]); // Should be sorted
    }

    #[test]
    fn test_list_tables_excludes_system_tables() {
        let (_temp_dir, db_path) = create_test_db();
        let conn = Connection::open(&db_path).unwrap();

        // Create a sequence (creates sqlite_sequence table)
        conn.execute(
            "CREATE TABLE test_autoincrement (id INTEGER PRIMARY KEY AUTOINCREMENT)",
            [],
        )
        .unwrap();

        let tables = list_tables(&conn).unwrap();

        // Should not include sqlite_sequence
        assert!(!tables.iter().any(|t| t.starts_with("sqlite_")));
    }

    #[test]
    fn test_get_table_row_count() {
        let (_temp_dir, db_path) = create_test_db();
        let conn = Connection::open(db_path).unwrap();

        let users_count = get_table_row_count(&conn, "users").unwrap();
        assert_eq!(users_count, 3);

        let posts_count = get_table_row_count(&conn, "posts").unwrap();
        assert_eq!(posts_count, 1);
    }

    #[test]
    fn test_get_table_row_count_invalid_table() {
        let (_temp_dir, db_path) = create_test_db();
        let conn = Connection::open(db_path).unwrap();

        // SQL injection attempt
        let result = get_table_row_count(&conn, "users; DROP TABLE users;");
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Invalid table name"));
    }

    #[test]
    fn test_read_table_data() {
        let (_temp_dir, db_path) = create_test_db();
        let conn = Connection::open(db_path).unwrap();

        let rows = read_table_data(&conn, "users").unwrap();

        assert_eq!(rows.len(), 3);

        // Check first row
        let first_row = &rows[0];
        assert!(first_row.contains_key("id"));
        assert!(first_row.contains_key("name"));
        assert!(first_row.contains_key("email"));
        assert!(first_row.contains_key("age"));

        // Check data types
        match &first_row["id"] {
            rusqlite::types::Value::Integer(_) => (),
            _ => panic!("id should be INTEGER"),
        }

        match &first_row["name"] {
            rusqlite::types::Value::Text(_) => (),
            _ => panic!("name should be TEXT"),
        }
    }

    #[test]
    fn test_read_table_data_handles_null() {
        let (_temp_dir, db_path) = create_test_db();
        let conn = Connection::open(db_path).unwrap();

        let rows = read_table_data(&conn, "users").unwrap();

        // Find Charlie (row with NULL age)
        let charlie = rows.iter().find(|r| match &r["name"] {
            rusqlite::types::Value::Text(s) => s == "Charlie",
            _ => false,
        });

        assert!(charlie.is_some());
        let charlie = charlie.unwrap();

        // Age should be NULL
        match &charlie["age"] {
            rusqlite::types::Value::Null => (),
            _ => panic!("Charlie's age should be NULL"),
        }
    }

    #[test]
    fn test_read_table_data_invalid_table() {
        let (_temp_dir, db_path) = create_test_db();
        let conn = Connection::open(db_path).unwrap();

        // SQL injection attempt
        let result = read_table_data(&conn, "users'; DROP TABLE users; --");
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Invalid table name"));
    }

    #[test]
    fn test_batched_table_reader_creation() {
        let (_temp_dir, db_path) = create_test_db();
        let conn = Connection::open(db_path).unwrap();

        let reader = BatchedTableReader::new(&conn, "users", 100).unwrap();

        assert_eq!(reader.table, "users");
        assert_eq!(reader.batch_size, 100);
        assert_eq!(reader.last_rowid, 0);
        assert!(!reader.exhausted);
        assert_eq!(reader.columns.len(), 4); // id, name, email, age
    }

    #[test]
    fn test_batched_table_reader_invalid_table() {
        let (_temp_dir, db_path) = create_test_db();
        let conn = Connection::open(db_path).unwrap();

        let result = BatchedTableReader::new(&conn, "users; DROP TABLE users;", 100);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Invalid table name"));
    }

    #[test]
    fn test_read_table_batch_single_batch() {
        let (_temp_dir, db_path) = create_test_db();
        let conn = Connection::open(db_path).unwrap();

        // Use batch size larger than row count - should get all rows in one batch
        let mut reader = BatchedTableReader::new(&conn, "users", 100).unwrap();

        // First batch should return all 3 rows
        let batch1 = read_table_batch(&conn, &mut reader).unwrap();
        assert!(batch1.is_some());
        let rows = batch1.unwrap();
        assert_eq!(rows.len(), 3);

        // Second call should return None (exhausted)
        let batch2 = read_table_batch(&conn, &mut reader).unwrap();
        assert!(batch2.is_none());
        assert!(reader.exhausted);
    }

    #[test]
    fn test_read_table_batch_multiple_batches() {
        let (_temp_dir, db_path) = create_test_db();
        let conn = Connection::open(db_path).unwrap();

        // Use batch size of 1 - should need multiple batches
        let mut reader = BatchedTableReader::new(&conn, "users", 1).unwrap();

        // Collect all batches
        let mut all_rows = Vec::new();
        while let Some(batch) = read_table_batch(&conn, &mut reader).unwrap() {
            assert_eq!(batch.len(), 1); // Each batch should have 1 row
            all_rows.extend(batch);
        }

        assert_eq!(all_rows.len(), 3);
        assert!(reader.exhausted);
    }

    #[test]
    fn test_read_table_batch_preserves_data() {
        let (_temp_dir, db_path) = create_test_db();
        let conn = Connection::open(db_path).unwrap();

        let mut reader = BatchedTableReader::new(&conn, "users", 100).unwrap();
        let batch = read_table_batch(&conn, &mut reader).unwrap().unwrap();

        // Verify row contents (sorted by rowid)
        let first_row = &batch[0];
        assert!(first_row.contains_key("id"));
        assert!(first_row.contains_key("name"));
        assert!(first_row.contains_key("email"));
        assert!(first_row.contains_key("age"));

        // First row should be Alice (id=1)
        match &first_row["name"] {
            rusqlite::types::Value::Text(s) => assert_eq!(s, "Alice"),
            _ => panic!("name should be TEXT"),
        }
    }

    #[test]
    fn test_read_table_batch_empty_table() {
        let temp_dir = tempfile::tempdir().unwrap();
        let db_path = temp_dir.path().join("empty.db");
        let conn = Connection::open(&db_path).unwrap();

        conn.execute(
            "CREATE TABLE empty_table (id INTEGER PRIMARY KEY, name TEXT)",
            [],
        )
        .unwrap();

        let mut reader = BatchedTableReader::new(&conn, "empty_table", 100).unwrap();

        // Should return None immediately for empty table
        let batch = read_table_batch(&conn, &mut reader).unwrap();
        assert!(batch.is_none());
        assert!(reader.exhausted);
    }

    #[test]
    fn test_read_table_batch_large_table() {
        let temp_dir = tempfile::tempdir().unwrap();
        let db_path = temp_dir.path().join("large.db");
        let conn = Connection::open(&db_path).unwrap();

        conn.execute(
            "CREATE TABLE large_table (id INTEGER PRIMARY KEY, value TEXT)",
            [],
        )
        .unwrap();

        // Insert 250 rows
        for i in 1..=250 {
            conn.execute(
                "INSERT INTO large_table (id, value) VALUES (?, ?)",
                rusqlite::params![i, format!("value_{}", i)],
            )
            .unwrap();
        }

        // Read with batch size of 100
        let mut reader = BatchedTableReader::new(&conn, "large_table", 100).unwrap();

        let mut batch_count = 0;
        let mut total_rows = 0;

        while let Some(batch) = read_table_batch(&conn, &mut reader).unwrap() {
            batch_count += 1;
            total_rows += batch.len();

            // Each batch should be at most 100 rows
            assert!(batch.len() <= 100);
        }

        assert_eq!(total_rows, 250);
        assert_eq!(batch_count, 3); // 100 + 100 + 50
    }
}
