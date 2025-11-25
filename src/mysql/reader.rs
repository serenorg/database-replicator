// ABOUTME: MySQL database introspection and data reading
// ABOUTME: Provides read-only access to tables and rows with security validation

use anyhow::{Context, Result};
use mysql_async::{prelude::*, Conn, Row};

/// List all user tables in a MySQL database
///
/// Queries INFORMATION_SCHEMA to discover all user tables, excluding system tables.
/// Returns tables in alphabetical order.
///
/// # Arguments
///
/// * `conn` - MySQL connection
/// * `db_name` - Database name to list tables from
///
/// # Returns
///
/// Vector of table names
///
/// # Examples
///
/// ```no_run
/// # use database_replicator::mysql::{connect_mysql, reader::list_tables};
/// # async fn example() -> anyhow::Result<()> {
/// let mut conn = connect_mysql("mysql://localhost:3306/mydb").await?;
/// let tables = list_tables(&mut conn, "mydb").await?;
/// println!("Found {} tables", tables.len());
/// # Ok(())
/// # }
/// ```
pub async fn list_tables(conn: &mut Conn, db_name: &str) -> Result<Vec<String>> {
    tracing::info!("Listing tables from MySQL database '{}'", db_name);

    let query = r#"
        SELECT TABLE_NAME
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = ?
        AND TABLE_TYPE = 'BASE TABLE'
        ORDER BY TABLE_NAME
    "#;

    let tables: Vec<String> = conn
        .exec(query, (db_name,))
        .await
        .with_context(|| format!("Failed to list tables from database '{}'", db_name))?;

    tracing::info!("Found {} table(s) in database '{}'", tables.len(), db_name);

    Ok(tables)
}

/// Get row count for a MySQL table
///
/// Executes COUNT(*) query to determine table size.
///
/// # Arguments
///
/// * `conn` - MySQL connection
/// * `db_name` - Database name
/// * `table_name` - Table name
///
/// # Returns
///
/// Number of rows in the table
///
/// # Examples
///
/// ```no_run
/// # use database_replicator::mysql::{connect_mysql, reader::get_table_row_count};
/// # async fn example() -> anyhow::Result<()> {
/// let mut conn = connect_mysql("mysql://localhost:3306/mydb").await?;
/// let count = get_table_row_count(&mut conn, "mydb", "users").await?;
/// println!("Table has {} rows", count);
/// # Ok(())
/// # }
/// ```
pub async fn get_table_row_count(
    conn: &mut Conn,
    db_name: &str,
    table_name: &str,
) -> Result<usize> {
    // Validate table name to prevent injection
    crate::jsonb::validate_table_name(table_name).context("Invalid table name for count query")?;

    tracing::debug!("Getting row count for table '{}.{}'", db_name, table_name);

    // Use backticks for identifiers to allow reserved words
    let query = format!("SELECT COUNT(*) FROM `{}`.`{}`", db_name, table_name);

    let count: Option<u64> = conn
        .query_first(&query)
        .await
        .with_context(|| format!("Failed to count rows in table '{}'", table_name))?;

    let count = count.unwrap_or(0) as usize;

    tracing::debug!("Table '{}' has {} rows", table_name, count);

    Ok(count)
}

/// Read all data from a MySQL table
///
/// Reads all rows from the table and returns them as MySQL Row objects.
/// For large tables, this may consume significant memory.
///
/// # Arguments
///
/// * `conn` - MySQL connection
/// * `db_name` - Database name
/// * `table_name` - Table name to read from
///
/// # Returns
///
/// Vector of MySQL Row objects
///
/// # Security
///
/// Table name is validated to prevent SQL injection
///
/// # Examples
///
/// ```no_run
/// # use database_replicator::mysql::{connect_mysql, reader::read_table_data};
/// # async fn example() -> anyhow::Result<()> {
/// let mut conn = connect_mysql("mysql://localhost:3306/mydb").await?;
/// let rows = read_table_data(&mut conn, "mydb", "users").await?;
/// println!("Read {} rows", rows.len());
/// # Ok(())
/// # }
/// ```
pub async fn read_table_data(conn: &mut Conn, db_name: &str, table_name: &str) -> Result<Vec<Row>> {
    // Validate table name to prevent injection
    crate::jsonb::validate_table_name(table_name).context("Invalid table name for data reading")?;

    tracing::info!("Reading all rows from table '{}.{}'", db_name, table_name);

    // Use backticks for identifiers
    let query = format!("SELECT * FROM `{}`.`{}`", db_name, table_name);

    let rows: Vec<Row> = conn
        .query(&query)
        .await
        .with_context(|| format!("Failed to read data from table '{}'", table_name))?;

    tracing::info!("Read {} rows from table '{}'", rows.len(), table_name);

    Ok(rows)
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_validate_table_names() {
        // Valid table names should pass validation
        let valid_names = vec!["users", "user_events", "UserData", "_private"];

        for name in valid_names {
            let result = crate::jsonb::validate_table_name(name);
            assert!(
                result.is_ok(),
                "Valid table name '{}' should be accepted",
                name
            );
        }
    }

    #[test]
    fn test_reject_malicious_table_names() {
        // Malicious table names should be rejected
        let malicious_names = vec![
            "users; DROP TABLE users;",
            "users' OR '1'='1",
            "../etc/passwd",
            "users--",
        ];

        for name in malicious_names {
            let result = crate::jsonb::validate_table_name(name);
            assert!(
                result.is_err(),
                "Malicious table name '{}' should be rejected",
                name
            );
        }
    }
}
