// ABOUTME: Data validation utilities using checksums
// ABOUTME: Computes and compares table checksums for data integrity verification

use anyhow::{Context, Result};
use tokio_postgres::Client;

/// Result of a checksum comparison between source and target tables
#[derive(Debug, Clone, PartialEq)]
pub struct ChecksumResult {
    pub schema: String,
    pub table: String,
    pub source_checksum: String,
    pub target_checksum: String,
    pub source_row_count: i64,
    pub target_row_count: i64,
    pub matches: bool,
}

impl ChecksumResult {
    /// Returns true if both checksums and row counts match
    pub fn is_valid(&self) -> bool {
        self.matches && self.source_row_count == self.target_row_count
    }
}

/// Compute checksum for a table
///
/// This generates an MD5 checksum of all data in the table by:
/// 1. Querying all columns in the table
/// 2. Concatenating all column values for each row
/// 3. Ordering by all columns for deterministic results
/// 4. Computing MD5 hash of the aggregated data
pub async fn compute_table_checksum(
    client: &Client,
    schema: &str,
    table: &str,
) -> Result<(String, i64)> {
    tracing::debug!("Computing checksum for {}.{}", schema, table);

    // Get all columns for the table
    let column_query = "
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = $1 AND table_name = $2
        ORDER BY ordinal_position
    ";

    let column_rows = client
        .query(column_query, &[&schema, &table])
        .await
        .context(format!("Failed to get columns for {}.{}", schema, table))?;

    if column_rows.is_empty() {
        anyhow::bail!("Table {}.{} has no columns", schema, table);
    }

    let columns: Vec<String> = column_rows
        .iter()
        .map(|row| row.get::<_, String>(0))
        .collect();

    // Build COALESCE expressions to handle NULLs
    let coalesce_exprs: Vec<String> = columns
        .iter()
        .map(|col| format!("COALESCE(\"{}\"::text, '')", col))
        .collect();

    let concat_expr = coalesce_exprs.join(" || '|' || ");

    // Build ORDER BY clause using all columns
    let order_by: Vec<String> = columns.iter().map(|col| format!("\"{}\"", col)).collect();
    let order_by_clause = order_by.join(", ");

    // Compute checksum: MD5 of all concatenated rows, ordered deterministically
    let checksum_query = format!(
        "SELECT
            md5(string_agg(row_data, '' ORDER BY row_num)) as checksum,
            COUNT(*) as row_count
        FROM (
            SELECT
                {} as row_data,
                ROW_NUMBER() OVER (ORDER BY {}) as row_num
            FROM \"{}\".\"{}\"
        ) t",
        concat_expr, order_by_clause, schema, table
    );

    let result = client
        .query_one(&checksum_query, &[])
        .await
        .context(format!(
            "Failed to compute checksum for {}.{}",
            schema, table
        ))?;

    let checksum: Option<String> = result.get(0);
    let row_count: i64 = result.get(1);

    // If table is empty, checksum will be NULL
    let checksum = checksum.unwrap_or_else(|| "empty".to_string());

    tracing::debug!(
        "Checksum for {}.{}: {} ({} rows)",
        schema,
        table,
        checksum,
        row_count
    );

    Ok((checksum, row_count))
}

/// Compare a table between source and target databases
pub async fn compare_tables(
    source_client: &Client,
    target_client: &Client,
    schema: &str,
    table: &str,
) -> Result<ChecksumResult> {
    tracing::info!("Comparing table {}.{}", schema, table);

    // Compute checksums in parallel
    let source_future = compute_table_checksum(source_client, schema, table);
    let target_future = compute_table_checksum(target_client, schema, table);

    let (source_result, target_result) = tokio::try_join!(source_future, target_future)?;

    let (source_checksum, source_row_count) = source_result;
    let (target_checksum, target_row_count) = target_result;

    let matches = source_checksum == target_checksum;

    Ok(ChecksumResult {
        schema: schema.to_string(),
        table: table.to_string(),
        source_checksum,
        target_checksum,
        source_row_count,
        target_row_count,
        matches,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::postgres::connect;

    #[tokio::test]
    #[ignore]
    async fn test_compute_table_checksum() {
        let url = std::env::var("TEST_SOURCE_URL").unwrap();
        let client = connect(&url).await.unwrap();

        // Try to compute checksum for a system table
        let result = compute_table_checksum(&client, "pg_catalog", "pg_database").await;

        match &result {
            Ok((checksum, row_count)) => {
                println!("✓ Checksum computed: {} ({} rows)", checksum, row_count);
                assert!(!checksum.is_empty());
                assert!(*row_count > 0);
            }
            Err(e) => {
                println!("Error computing checksum: {:?}", e);
                panic!("Failed to compute checksum: {:?}", e);
            }
        }
    }

    #[tokio::test]
    #[ignore]
    async fn test_compute_empty_table_checksum() {
        let url = std::env::var("TEST_SOURCE_URL").unwrap();
        let client = connect(&url).await.unwrap();

        // Create a temporary empty table
        client
            .execute("CREATE TEMP TABLE test_empty (id INT, name TEXT)", &[])
            .await
            .unwrap();

        let result = compute_table_checksum(&client, "pg_temp", "test_empty").await;

        match &result {
            Ok((checksum, row_count)) => {
                println!("✓ Empty table checksum: {} ({} rows)", checksum, row_count);
                assert_eq!(checksum, "empty");
                assert_eq!(*row_count, 0);
            }
            Err(e) => {
                println!("Error computing empty table checksum: {:?}", e);
                panic!("Failed: {:?}", e);
            }
        }
    }

    #[tokio::test]
    #[ignore]
    async fn test_compare_tables() {
        // This test requires both source and target databases
        let source_url = std::env::var("TEST_SOURCE_URL").unwrap();
        let target_url = std::env::var("TEST_TARGET_URL").unwrap();

        let source_client = connect(&source_url).await.unwrap();
        let target_client = connect(&target_url).await.unwrap();

        // Compare a system table that should exist on both
        let result =
            compare_tables(&source_client, &target_client, "pg_catalog", "pg_database").await;

        match &result {
            Ok(comparison) => {
                println!("✓ Table comparison completed");
                println!("  Schema: {}", comparison.schema);
                println!("  Table: {}", comparison.table);
                println!("  Source checksum: {}", comparison.source_checksum);
                println!("  Target checksum: {}", comparison.target_checksum);
                println!("  Source rows: {}", comparison.source_row_count);
                println!("  Target rows: {}", comparison.target_row_count);
                println!("  Matches: {}", comparison.matches);
            }
            Err(e) => {
                println!("Error comparing tables: {:?}", e);
                panic!("Failed to compare tables: {:?}", e);
            }
        }

        assert!(result.is_ok());
    }

    #[tokio::test]
    #[ignore]
    async fn test_checksum_deterministic() {
        let url = std::env::var("TEST_SOURCE_URL").unwrap();
        let client = connect(&url).await.unwrap();

        // Compute checksum twice for the same table
        let (checksum1, rows1) = compute_table_checksum(&client, "pg_catalog", "pg_database")
            .await
            .unwrap();

        let (checksum2, rows2) = compute_table_checksum(&client, "pg_catalog", "pg_database")
            .await
            .unwrap();

        // Checksums should be identical (deterministic)
        assert_eq!(checksum1, checksum2);
        assert_eq!(rows1, rows2);
        println!("✓ Checksum is deterministic: {}", checksum1);
    }
}
