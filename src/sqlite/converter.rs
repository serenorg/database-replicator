// ABOUTME: SQLite to JSONB type conversion for PostgreSQL storage
// ABOUTME: Handles all SQLite types with lossless conversion and BLOB base64 encoding

use anyhow::{Context, Result};
use rusqlite::Connection;
use serde_json::Value as JsonValue;
use std::collections::HashMap;

/// Convert a single SQLite value to JSON
///
/// Maps SQLite types to JSON types:
/// - INTEGER → number (i64)
/// - REAL → number (f64)
/// - TEXT → string (UTF-8)
/// - BLOB → object with base64-encoded data
/// - NULL → null
///
/// # Arguments
///
/// * `value` - SQLite value from rusqlite
///
/// # Returns
///
/// JSON value suitable for JSONB storage
///
/// # Examples
///
/// ```no_run
/// # use database_replicator::sqlite::converter::sqlite_value_to_json;
/// # use rusqlite::types::Value;
/// let sqlite_int = Value::Integer(42);
/// let json = sqlite_value_to_json(&sqlite_int).unwrap();
/// assert_eq!(json, serde_json::json!(42));
/// ```
pub fn sqlite_value_to_json(value: &rusqlite::types::Value) -> Result<JsonValue> {
    match value {
        rusqlite::types::Value::Null => Ok(JsonValue::Null),

        rusqlite::types::Value::Integer(i) => Ok(JsonValue::Number((*i).into())),

        rusqlite::types::Value::Real(f) => {
            // Convert f64 to JSON number
            // Note: JSON can't represent NaN or Infinity, handle edge cases
            if f.is_finite() {
                serde_json::Number::from_f64(*f)
                    .map(JsonValue::Number)
                    .ok_or_else(|| anyhow::anyhow!("Failed to convert float {} to JSON number", f))
            } else {
                // Store non-finite numbers as strings for safety
                Ok(JsonValue::String(f.to_string()))
            }
        }

        rusqlite::types::Value::Text(s) => Ok(JsonValue::String(s.clone())),

        rusqlite::types::Value::Blob(b) => {
            // Encode BLOB as base64 in a JSON object
            // Format: {"_type": "blob", "data": "base64..."}
            // This allows distinguishing BLOBs from regular strings
            let encoded = base64::Engine::encode(&base64::engine::general_purpose::STANDARD, b);
            Ok(serde_json::json!({
                "_type": "blob",
                "data": encoded
            }))
        }
    }
}

/// Convert a SQLite row (HashMap) to JSON object
///
/// Converts all column values to JSON and returns a JSON object
/// with column names as keys.
///
/// # Arguments
///
/// * `row` - HashMap of column_name → SQLite value
///
/// # Returns
///
/// JSON object ready for JSONB storage
///
/// # Examples
///
/// ```no_run
/// # use database_replicator::sqlite::converter::sqlite_row_to_json;
/// # use std::collections::HashMap;
/// # use rusqlite::types::Value;
/// let mut row = HashMap::new();
/// row.insert("id".to_string(), Value::Integer(1));
/// row.insert("name".to_string(), Value::Text("Alice".to_string()));
/// let json = sqlite_row_to_json(row).unwrap();
/// assert_eq!(json["id"], 1);
/// assert_eq!(json["name"], "Alice");
/// ```
pub fn sqlite_row_to_json(row: HashMap<String, rusqlite::types::Value>) -> Result<JsonValue> {
    let mut json_obj = serde_json::Map::new();

    for (col_name, value) in row {
        let json_value = sqlite_value_to_json(&value)
            .with_context(|| format!("Failed to convert column '{}' to JSON", col_name))?;
        json_obj.insert(col_name, json_value);
    }

    Ok(JsonValue::Object(json_obj))
}

/// Convert an entire SQLite table to JSONB format
///
/// Reads all rows from a SQLite table and converts them to JSONB.
/// Returns a vector of (id, json_data) tuples ready for insertion.
///
/// # ID Generation Strategy
///
/// - If table has a column named "id", "rowid", or "_id", use that as the ID
/// - Otherwise, use SQLite's rowid (every table has one)
/// - IDs are converted to strings for consistency
///
/// # Arguments
///
/// * `conn` - SQLite database connection
/// * `table` - Table name (must be validated)
///
/// # Returns
///
/// Vector of (id_string, json_data) tuples for batch insert
///
/// # Security
///
/// Table name should be validated before calling this function.
///
/// # Examples
///
/// ```no_run
/// # use database_replicator::sqlite::{open_sqlite, converter::convert_table_to_jsonb};
/// # use database_replicator::jsonb::validate_table_name;
/// # fn example() -> anyhow::Result<()> {
/// let conn = open_sqlite("database.db")?;
/// let table = "users";
/// validate_table_name(table)?;
/// let rows = convert_table_to_jsonb(&conn, table)?;
/// println!("Converted {} rows to JSONB", rows.len());
/// # Ok(())
/// # }
/// ```
pub fn convert_table_to_jsonb(conn: &Connection, table: &str) -> Result<Vec<(String, JsonValue)>> {
    // Validate table name
    crate::jsonb::validate_table_name(table).context("Invalid table name for JSONB conversion")?;

    tracing::info!("Converting SQLite table '{}' to JSONB", table);

    // Read all rows using our reader
    let rows = crate::sqlite::reader::read_table_data(conn, table)
        .with_context(|| format!("Failed to read data from table '{}'", table))?;

    // Detect ID column
    let id_column = detect_id_column(conn, table)?;

    let mut result = Vec::with_capacity(rows.len());

    for (row_num, row) in rows.into_iter().enumerate() {
        // Extract or generate ID
        let id = if let Some(ref id_col) = id_column {
            // Use the specified ID column
            match row.get(id_col) {
                Some(rusqlite::types::Value::Integer(i)) => i.to_string(),
                Some(rusqlite::types::Value::Text(s)) => s.clone(),
                Some(rusqlite::types::Value::Real(f)) => f.to_string(),
                _ => {
                    // Fallback to row number if ID is NULL or unsupported type
                    tracing::warn!(
                        "Row {} in table '{}' has invalid ID type, using row number",
                        row_num + 1,
                        table
                    );
                    (row_num + 1).to_string()
                }
            }
        } else {
            // No ID column found, use row number
            // SQLite rowid is 1-indexed, so we add 1
            (row_num + 1).to_string()
        };

        // Convert row to JSON
        let json_data = sqlite_row_to_json(row).with_context(|| {
            format!(
                "Failed to convert row {} in table '{}' to JSON",
                row_num + 1,
                table
            )
        })?;

        result.push((id, json_data));
    }

    tracing::info!(
        "Converted {} rows from table '{}' to JSONB",
        result.len(),
        table
    );

    Ok(result)
}

/// Detect the ID column for a table
///
/// Checks for common ID column names: "id", "rowid", "_id" (case-insensitive).
/// If found, returns the column name. Otherwise returns None.
fn detect_id_column(conn: &Connection, table: &str) -> Result<Option<String>> {
    // Get column names for the table
    let query = format!("PRAGMA table_info(\"{}\")", table);
    let mut stmt = conn
        .prepare(&query)
        .with_context(|| format!("Failed to get table info for '{}'", table))?;

    let columns: Vec<String> = stmt
        .query_map([], |row| row.get::<_, String>(1))
        .context("Failed to query table columns")?
        .collect::<Result<Vec<_>, _>>()
        .context("Failed to collect column names")?;

    // Check for common ID column names (case-insensitive)
    let id_candidates = ["id", "rowid", "_id"];
    for candidate in &id_candidates {
        if let Some(col) = columns.iter().find(|c| c.to_lowercase() == *candidate) {
            tracing::debug!("Using column '{}' as ID for table '{}'", col, table);
            return Ok(Some(col.clone()));
        }
    }

    tracing::debug!(
        "No ID column found for table '{}', will use row number",
        table
    );
    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::types::Value;

    #[test]
    fn test_convert_integer() {
        let value = Value::Integer(42);
        let json = sqlite_value_to_json(&value).unwrap();
        assert_eq!(json, serde_json::json!(42));
    }

    #[test]
    fn test_convert_real() {
        let value = Value::Real(42.75);
        let json = sqlite_value_to_json(&value).unwrap();
        assert_eq!(json, serde_json::json!(42.75));
    }

    #[test]
    fn test_convert_text() {
        let value = Value::Text("Hello, World!".to_string());
        let json = sqlite_value_to_json(&value).unwrap();
        assert_eq!(json, serde_json::json!("Hello, World!"));
    }

    #[test]
    fn test_convert_null() {
        let value = Value::Null;
        let json = sqlite_value_to_json(&value).unwrap();
        assert_eq!(json, JsonValue::Null);
    }

    #[test]
    fn test_convert_blob() {
        let blob_data = vec![0x48, 0x65, 0x6c, 0x6c, 0x6f]; // "Hello" in bytes
        let value = Value::Blob(blob_data.clone());
        let json = sqlite_value_to_json(&value).unwrap();

        // Should be wrapped in an object with _type and data fields
        assert!(json.is_object());
        assert_eq!(json["_type"], "blob");

        // Decode and verify
        let encoded = json["data"].as_str().unwrap();
        let decoded =
            base64::Engine::decode(&base64::engine::general_purpose::STANDARD, encoded).unwrap();
        assert_eq!(decoded, blob_data);
    }

    #[test]
    fn test_convert_non_finite_float() {
        let nan_value = Value::Real(f64::NAN);
        let json = sqlite_value_to_json(&nan_value).unwrap();
        // NaN should be converted to string
        assert!(json.is_string());

        let inf_value = Value::Real(f64::INFINITY);
        let json = sqlite_value_to_json(&inf_value).unwrap();
        // Infinity should be converted to string
        assert!(json.is_string());
    }

    #[test]
    fn test_sqlite_row_to_json() {
        let mut row = HashMap::new();
        row.insert("id".to_string(), Value::Integer(1));
        row.insert("name".to_string(), Value::Text("Alice".to_string()));
        row.insert("age".to_string(), Value::Integer(30));
        row.insert("balance".to_string(), Value::Real(100.50));
        row.insert("notes".to_string(), Value::Null);

        let json = sqlite_row_to_json(row).unwrap();

        assert_eq!(json["id"], 1);
        assert_eq!(json["name"], "Alice");
        assert_eq!(json["age"], 30);
        assert_eq!(json["balance"], 100.50);
        assert_eq!(json["notes"], JsonValue::Null);
    }

    #[test]
    fn test_convert_table_to_jsonb() {
        // Create a test database
        let conn = Connection::open_in_memory().unwrap();

        // Create test table with ID column
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

        // Convert to JSONB
        let result = convert_table_to_jsonb(&conn, "users").unwrap();

        assert_eq!(result.len(), 2);

        // Check first row
        let (id1, json1) = &result[0];
        assert_eq!(id1, "1");
        assert_eq!(json1["name"], "Alice");
        assert_eq!(json1["email"], "alice@example.com");
        assert_eq!(json1["age"], 30);

        // Check second row
        let (id2, json2) = &result[1];
        assert_eq!(id2, "2");
        assert_eq!(json2["name"], "Bob");
    }

    #[test]
    fn test_convert_table_without_id_column() {
        // Create a test database
        let conn = Connection::open_in_memory().unwrap();

        // Create table WITHOUT explicit ID column
        conn.execute(
            "CREATE TABLE logs (
                timestamp INTEGER,
                message TEXT
            )",
            [],
        )
        .unwrap();

        // Insert test data
        conn.execute(
            "INSERT INTO logs (timestamp, message) VALUES (12345, 'Test message')",
            [],
        )
        .unwrap();

        // Convert to JSONB
        let result = convert_table_to_jsonb(&conn, "logs").unwrap();

        assert_eq!(result.len(), 1);

        // Should use row number as ID (1-indexed)
        let (id, json) = &result[0];
        assert_eq!(id, "1");
        assert_eq!(json["message"], "Test message");
    }

    #[test]
    fn test_convert_table_handles_null_values() {
        let conn = Connection::open_in_memory().unwrap();

        conn.execute(
            "CREATE TABLE users (
                id INTEGER PRIMARY KEY,
                name TEXT,
                email TEXT
            )",
            [],
        )
        .unwrap();

        // Insert row with NULL values
        conn.execute(
            "INSERT INTO users (id, name, email) VALUES (1, 'Alice', NULL)",
            [],
        )
        .unwrap();

        let result = convert_table_to_jsonb(&conn, "users").unwrap();

        assert_eq!(result.len(), 1);
        let (_, json) = &result[0];
        assert_eq!(json["name"], "Alice");
        assert_eq!(json["email"], JsonValue::Null);
    }

    #[test]
    fn test_convert_table_with_blob() {
        let conn = Connection::open_in_memory().unwrap();

        conn.execute(
            "CREATE TABLE files (
                id INTEGER PRIMARY KEY,
                name TEXT,
                data BLOB
            )",
            [],
        )
        .unwrap();

        // Insert row with BLOB (must be Vec<u8>, not Vec<i32>)
        let blob_data: Vec<u8> = vec![0x01, 0x02, 0x03, 0x04];
        conn.execute(
            "INSERT INTO files (id, name, data) VALUES (?1, ?2, ?3)",
            rusqlite::params![1, "test.bin", &blob_data],
        )
        .unwrap();

        let result = convert_table_to_jsonb(&conn, "files").unwrap();

        assert_eq!(result.len(), 1);
        let (_, json) = &result[0];
        assert_eq!(json["name"], "test.bin");

        // BLOB should be base64-encoded
        assert!(json["data"].is_object());
        assert_eq!(json["data"]["_type"], "blob");
        assert!(json["data"]["data"].is_string());
    }

    #[test]
    fn test_detect_id_column_case_insensitive() {
        let conn = Connection::open_in_memory().unwrap();

        // Create table with uppercase ID column
        conn.execute("CREATE TABLE test (ID INTEGER PRIMARY KEY, value TEXT)", [])
            .unwrap();

        let id_col = detect_id_column(&conn, "test").unwrap();
        assert!(id_col.is_some());
        assert_eq!(id_col.unwrap().to_lowercase(), "id");
    }

    #[test]
    fn test_convert_empty_table() {
        let conn = Connection::open_in_memory().unwrap();

        conn.execute("CREATE TABLE empty (id INTEGER PRIMARY KEY)", [])
            .unwrap();

        let result = convert_table_to_jsonb(&conn, "empty").unwrap();
        assert_eq!(result.len(), 0);
    }
}
