// ABOUTME: MySQL to JSONB type conversion with lossless data preservation
// ABOUTME: Handles all MySQL data types including dates, decimals, and binary data

use anyhow::{Context, Result};
use mysql_async::{prelude::*, Row, Value};
use serde_json::Value as JsonValue;

/// Convert a MySQL Value to JSON Value
///
/// Handles all MySQL data types with lossless conversion:
/// - Integers → JSON numbers
/// - Floats/Doubles → JSON numbers (non-finite as strings)
/// - Decimals → Strings (to preserve precision)
/// - Strings → JSON strings
/// - Dates/Times → ISO 8601 strings in special object
/// - Binary data → Base64 encoded in special object
/// - NULL → JSON null
///
/// # Arguments
///
/// * `value` - MySQL Value to convert
///
/// # Returns
///
/// JSON Value representing the MySQL value
///
/// # Examples
///
/// ```
/// # use mysql_async::Value;
/// # use database_replicator::mysql::converter::mysql_value_to_json;
/// let mysql_val = Value::Int(42);
/// let json_val = mysql_value_to_json(&mysql_val).unwrap();
/// assert_eq!(json_val, serde_json::json!(42));
/// ```
pub fn mysql_value_to_json(value: &Value) -> Result<JsonValue> {
    match value {
        Value::NULL => Ok(JsonValue::Null),

        Value::Int(i) => Ok(JsonValue::Number((*i).into())),
        Value::UInt(u) => Ok(JsonValue::Number((*u).into())),

        Value::Float(f) => {
            if f.is_finite() {
                serde_json::Number::from_f64(*f as f64)
                    .map(JsonValue::Number)
                    .ok_or_else(|| anyhow::anyhow!("Failed to convert float {} to JSON number", f))
            } else {
                // Store non-finite as strings
                Ok(JsonValue::String(f.to_string()))
            }
        }

        Value::Double(d) => {
            if d.is_finite() {
                serde_json::Number::from_f64(*d)
                    .map(JsonValue::Number)
                    .ok_or_else(|| anyhow::anyhow!("Failed to convert double {} to JSON number", d))
            } else {
                // Store non-finite as strings
                Ok(JsonValue::String(d.to_string()))
            }
        }

        Value::Bytes(b) => {
            // Try to interpret as UTF-8 string first
            if let Ok(s) = String::from_utf8(b.clone()) {
                Ok(JsonValue::String(s))
            } else {
                // If not valid UTF-8, encode as base64
                let encoded = base64::Engine::encode(&base64::engine::general_purpose::STANDARD, b);
                Ok(serde_json::json!({
                    "_type": "binary",
                    "data": encoded
                }))
            }
        }

        Value::Date(year, month, day, hour, minute, second, micro) => {
            // Format as ISO 8601 datetime string
            let datetime_str = format!(
                "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{:06}Z",
                year, month, day, hour, minute, second, micro
            );
            Ok(serde_json::json!({
                "_type": "datetime",
                "value": datetime_str
            }))
        }

        Value::Time(is_negative, days, hours, minutes, seconds, microseconds) => {
            // Format as time duration
            let sign = if *is_negative { "-" } else { "" };
            let time_str = format!(
                "{}{}d {:02}:{:02}:{:02}.{:06}",
                sign, days, hours, minutes, seconds, microseconds
            );
            Ok(serde_json::json!({
                "_type": "time",
                "value": time_str
            }))
        }
    }
}

/// Convert a MySQL Row to a JSONB-compatible JSON object
///
/// Converts all columns in the row to a JSON object with column names as keys.
///
/// # Arguments
///
/// * `row` - MySQL Row to convert
/// * `column_names` - Names of columns in the row (from table schema)
///
/// # Returns
///
/// JSON object with all column values
///
/// # Examples
///
/// ```no_run
/// # use mysql_async::Row;
/// # use database_replicator::mysql::converter::mysql_row_to_json;
/// # async fn example(row: Row) -> anyhow::Result<()> {
/// let column_names = vec!["id".to_string(), "name".to_string()];
/// let json_obj = mysql_row_to_json(&row, &column_names)?;
/// # Ok(())
/// # }
/// ```
pub fn mysql_row_to_json(row: &Row, column_names: &[String]) -> Result<JsonValue> {
    let mut obj = serde_json::Map::new();

    for (idx, col_name) in column_names.iter().enumerate() {
        // Get value at index
        let value: Value = row
            .get(idx)
            .ok_or_else(|| anyhow::anyhow!("Failed to get column {} at index {}", col_name, idx))?;

        // Convert to JSON
        let json_val = mysql_value_to_json(&value)
            .with_context(|| format!("Failed to convert column '{}' to JSON", col_name))?;

        obj.insert(col_name.clone(), json_val);
    }

    Ok(JsonValue::Object(obj))
}

/// Get column names for a MySQL table
///
/// Queries INFORMATION_SCHEMA to get all column names for a table.
///
/// # Arguments
///
/// * `conn` - MySQL connection
/// * `db_name` - Database name
/// * `table_name` - Table name
///
/// # Returns
///
/// Vector of column names in order
///
/// # Examples
///
/// ```no_run
/// # use database_replicator::mysql::{connect_mysql, converter::get_column_names};
/// # async fn example() -> anyhow::Result<()> {
/// let mut conn = connect_mysql("mysql://localhost:3306/mydb").await?;
/// let columns = get_column_names(&mut conn, "mydb", "users").await?;
/// # Ok(())
/// # }
/// ```
pub async fn get_column_names(
    conn: &mut mysql_async::Conn,
    db_name: &str,
    table_name: &str,
) -> Result<Vec<String>> {
    // Validate table name
    crate::jsonb::validate_table_name(table_name).context("Invalid table name for column query")?;

    let query = r#"
        SELECT COLUMN_NAME
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = ?
        AND TABLE_NAME = ?
        ORDER BY ORDINAL_POSITION
    "#;

    let columns: Vec<String> =
        conn.exec(query, (db_name, table_name))
            .await
            .with_context(|| {
                format!(
                    "Failed to get column names for table '{}.{}'",
                    db_name, table_name
                )
            })?;

    Ok(columns)
}

/// Convert an entire MySQL table to JSONB format
///
/// Reads all rows from the table and converts them to (id, jsonb_data) tuples.
/// The ID is extracted from a primary key or auto-generated.
///
/// # Arguments
///
/// * `conn` - MySQL connection
/// * `db_name` - Database name
/// * `table_name` - Table name
///
/// # Returns
///
/// Vector of (id, json_data) tuples ready for PostgreSQL JSONB storage
///
/// # Examples
///
/// ```no_run
/// # use database_replicator::mysql::{connect_mysql, converter::convert_table_to_jsonb};
/// # async fn example() -> anyhow::Result<()> {
/// let mut conn = connect_mysql("mysql://localhost:3306/mydb").await?;
/// let rows = convert_table_to_jsonb(&mut conn, "mydb", "users").await?;
/// println!("Converted {} rows", rows.len());
/// # Ok(())
/// # }
/// ```
pub async fn convert_table_to_jsonb(
    conn: &mut mysql_async::Conn,
    db_name: &str,
    table_name: &str,
) -> Result<Vec<(String, JsonValue)>> {
    // Validate table name
    crate::jsonb::validate_table_name(table_name)
        .context("Invalid table name for JSONB conversion")?;

    tracing::info!(
        "Converting MySQL table '{}.{}' to JSONB",
        db_name,
        table_name
    );

    // Get column names
    let column_names = get_column_names(conn, db_name, table_name).await?;

    if column_names.is_empty() {
        tracing::warn!("Table '{}.{}' has no columns", db_name, table_name);
        return Ok(vec![]);
    }

    // Read all rows
    let rows = crate::mysql::reader::read_table_data(conn, db_name, table_name).await?;

    let mut result = Vec::with_capacity(rows.len());
    let mut id_counter = 1u64;

    for row in rows {
        // Convert row to JSON
        let json_data = mysql_row_to_json(&row, &column_names)
            .with_context(|| format!("Failed to convert row in table '{}'", table_name))?;

        // Try to extract ID from common ID column names
        let id = if let Some(id_val) = json_data.get("id") {
            // Use 'id' column if exists
            id_val.to_string().trim_matches('"').to_string()
        } else if let Some(id_val) = json_data.get("Id") {
            // Case insensitive check
            id_val.to_string().trim_matches('"').to_string()
        } else if let Some(id_val) = json_data.get("ID") {
            id_val.to_string().trim_matches('"').to_string()
        } else {
            // Generate sequential ID
            let generated_id = format!("generated_{}", id_counter);
            id_counter += 1;
            generated_id
        };

        result.push((id, json_data));
    }

    tracing::info!(
        "Converted {} rows from table '{}.{}'",
        result.len(),
        db_name,
        table_name
    );

    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_convert_null() {
        let value = Value::NULL;
        let json = mysql_value_to_json(&value).unwrap();
        assert_eq!(json, JsonValue::Null);
    }

    #[test]
    fn test_convert_int() {
        let value = Value::Int(42);
        let json = mysql_value_to_json(&value).unwrap();
        assert_eq!(json, serde_json::json!(42));
    }

    #[test]
    fn test_convert_uint() {
        let value = Value::UInt(42);
        let json = mysql_value_to_json(&value).unwrap();
        assert_eq!(json, serde_json::json!(42));
    }

    #[test]
    fn test_convert_double() {
        let value = Value::Double(123.456);
        let json = mysql_value_to_json(&value).unwrap();
        assert_eq!(json, serde_json::json!(123.456));
    }

    #[test]
    fn test_convert_string_bytes() {
        let value = Value::Bytes(b"Hello World".to_vec());
        let json = mysql_value_to_json(&value).unwrap();
        assert_eq!(json, JsonValue::String("Hello World".to_string()));
    }

    #[test]
    fn test_convert_binary_bytes() {
        let value = Value::Bytes(vec![0xFF, 0xFE, 0xFD]);
        let json = mysql_value_to_json(&value).unwrap();
        assert!(json.is_object());
        assert_eq!(json["_type"], "binary");
    }

    #[test]
    fn test_convert_datetime() {
        let value = Value::Date(2024, 1, 15, 10, 30, 45, 123456);
        let json = mysql_value_to_json(&value).unwrap();
        assert!(json.is_object());
        assert_eq!(json["_type"], "datetime");
        assert_eq!(json["value"], "2024-01-15T10:30:45.123456Z");
    }

    #[test]
    fn test_convert_time() {
        let value = Value::Time(false, 1, 10, 30, 45, 123456);
        let json = mysql_value_to_json(&value).unwrap();
        assert!(json.is_object());
        assert_eq!(json["_type"], "time");
        assert!(json["value"].as_str().unwrap().contains("1d 10:30:45"));
    }

    #[test]
    fn test_convert_non_finite_double() {
        let value = Value::Double(f64::NAN);
        let json = mysql_value_to_json(&value).unwrap();
        assert_eq!(json, JsonValue::String("NaN".to_string()));
    }
}
