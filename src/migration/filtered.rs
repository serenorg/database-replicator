// ABOUTME: Handles filtered table replication using COPY streaming
// ABOUTME: Applies table-level predicates and time filters during init snapshots

use crate::postgres;
use anyhow::{bail, Context, Result};
use futures::{pin_mut, SinkExt, StreamExt};
use std::collections::BTreeSet;
use tokio_postgres::Client;

/// Parse schema-qualified table name into (schema, table)
/// Expects format: "schema"."table"
fn parse_schema_table(qualified: &str) -> Result<(String, String)> {
    // Remove quotes and split on .
    let parts: Vec<&str> = qualified.split('.').map(|s| s.trim_matches('"')).collect();

    if parts.len() == 2 {
        Ok((parts[0].to_string(), parts[1].to_string()))
    } else {
        bail!(
            "Expected schema-qualified table name (\"schema\".\"table\"), got: {}",
            qualified
        );
    }
}

/// Query tables that would be affected by TRUNCATE CASCADE on the given table
/// Returns a list of (schema, table) pairs that reference the target table via FK
async fn get_cascade_targets(
    client: &Client,
    schema: &str,
    table: &str,
) -> Result<Vec<(String, String)>> {
    let query = r#"
        WITH RECURSIVE fk_tree AS (
            -- Start with the table being truncated
            SELECT n.nspname as schema_name, c.relname as table_name, 0 as depth
            FROM pg_class c
            JOIN pg_namespace n ON c.relnamespace = n.oid
            WHERE n.nspname = $1 AND c.relname = $2

            UNION ALL

            -- Find tables that reference this table via FK
            SELECT n2.nspname, c2.relname, fk_tree.depth + 1
            FROM fk_tree
            JOIN pg_constraint con ON con.confrelid = (
                SELECT c.oid FROM pg_class c
                JOIN pg_namespace n ON c.relnamespace = n.oid
                WHERE n.nspname = fk_tree.schema_name
                AND c.relname = fk_tree.table_name
            )
            JOIN pg_class c2 ON con.conrelid = c2.oid
            JOIN pg_namespace n2 ON c2.relnamespace = n2.oid
            WHERE con.contype = 'f'
        )
        SELECT DISTINCT schema_name, table_name
        FROM fk_tree
        WHERE depth > 0
        ORDER BY schema_name, table_name
    "#;

    let rows = client.query(query, &[&schema, &table]).await?;
    Ok(rows.iter().map(|row| (row.get(0), row.get(1))).collect())
}

pub async fn copy_filtered_tables(
    source_url: &str,
    target_url: &str,
    tables: &[(String, String)],
) -> Result<()> {
    if tables.is_empty() {
        return Ok(());
    }

    let source_client = postgres::connect_with_retry(source_url)
        .await
        .context("Failed to connect to source database for filtered copy")?;
    let target_client = postgres::connect_with_retry(target_url)
        .await
        .context("Failed to connect to target database for filtered copy")?;

    // Step 1: Query CASCADE targets for all tables before truncating
    tracing::info!(
        "Checking FK dependencies for {} filtered tables",
        tables.len()
    );

    let mut all_cascade_targets = BTreeSet::new();
    let table_names: BTreeSet<String> = tables.iter().map(|(t, _)| t.clone()).collect();

    for (table, _) in tables {
        let (schema, table_name) = parse_schema_table(table)?;
        let targets = get_cascade_targets(&target_client, &schema, &table_name).await?;

        for (target_schema, target_table) in targets {
            let qualified = format!("\"{}\".\"{}\"", target_schema, target_table);
            all_cascade_targets.insert((target_schema, target_table, qualified));
        }
    }

    // Step 2: Show blast radius if CASCADE will affect other tables
    if !all_cascade_targets.is_empty() {
        tracing::warn!(
            "⚠ TRUNCATE CASCADE will affect {} additional tables:",
            all_cascade_targets.len()
        );
        for (schema, table, _) in &all_cascade_targets {
            tracing::warn!("  - {}.{}", schema, table);
        }

        // Step 3: Safety check - ensure all CASCADE targets are being copied
        for (schema, table, qualified) in &all_cascade_targets {
            if !table_names.contains(qualified) {
                bail!(
                    "FK-related table {}.{} will be truncated by CASCADE but is NOT being copied.\n\
                     This would result in data loss.\n\
                     \n\
                     Solution: Include this table in your replication scope or remove the FK constraint.",
                    schema, table
                );
            }
        }

        tracing::info!("✓ All CASCADE targets are included in replication scope");
    }

    // Step 4: Proceed with TRUNCATE CASCADE and filtered copy
    for (table, predicate) in tables {
        tracing::info!(
            "  Applying filtered copy for table '{}' with predicate: {}",
            table,
            predicate
        );

        // Table is already schema-qualified and quoted (e.g., "public"."table")
        let quoted_table = table;

        // Use TRUNCATE CASCADE to handle FK dependencies
        let truncate_sql = format!("TRUNCATE TABLE {} CASCADE", quoted_table);
        target_client
            .execute(&truncate_sql, &[])
            .await
            .with_context(|| format!("Failed to truncate target table '{}'", table))?;

        let copy_out_sql = format!(
            "COPY (SELECT * FROM {} WHERE {}) TO STDOUT BINARY",
            quoted_table, predicate
        );
        let reader = source_client
            .copy_out(&copy_out_sql)
            .await
            .with_context(|| format!("Failed to copy data from source table '{}'", table))?;

        let copy_in_sql = format!("COPY {} FROM STDIN BINARY", quoted_table);
        let writer = target_client
            .copy_in(&copy_in_sql)
            .await
            .with_context(|| format!("Failed to copy data into target table '{}'", table))?;

        pin_mut!(reader);
        pin_mut!(writer);

        while let Some(chunk) = reader.next().await {
            let data = chunk?;
            writer.as_mut().send(data).await?;
        }

        writer.finish().await?;
        tracing::info!("  ✓ Filtered copy complete for '{}'", table);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_schema_table_valid() {
        let result = parse_schema_table("\"public\".\"users\"").unwrap();
        assert_eq!(result, ("public".to_string(), "users".to_string()));

        let result = parse_schema_table("\"analytics\".\"orders\"").unwrap();
        assert_eq!(result, ("analytics".to_string(), "orders".to_string()));
    }

    #[test]
    fn test_parse_schema_table_invalid() {
        // Missing schema
        let result = parse_schema_table("\"users\"");
        assert!(result.is_err());

        // Too many parts
        let result = parse_schema_table("\"db\".\"schema\".\"table\"");
        assert!(result.is_err());

        // No quotes
        let result = parse_schema_table("public.users");
        assert_eq!(result.unwrap(), ("public".to_string(), "users".to_string()));
    }

    #[tokio::test]
    #[ignore]
    async fn test_cascade_targets_detected() {
        // This test requires a real database connection
        let url = std::env::var("TEST_TARGET_URL")
            .expect("TEST_TARGET_URL must be set for integration tests");

        let client = postgres::connect(&url).await.unwrap();

        // Create test tables with FK relationship
        client
            .execute(
                "CREATE TABLE IF NOT EXISTS users (id SERIAL PRIMARY KEY, name TEXT)",
                &[],
            )
            .await
            .unwrap();

        client
            .execute(
                "CREATE TABLE IF NOT EXISTS orders (
                    id SERIAL PRIMARY KEY,
                    user_id INTEGER REFERENCES users(id),
                    amount DECIMAL
                )",
                &[],
            )
            .await
            .unwrap();

        // Query cascade targets for users table
        let targets = get_cascade_targets(&client, "public", "users")
            .await
            .unwrap();

        // Should find orders table as it references users
        assert!(
            targets.contains(&("public".to_string(), "orders".to_string())),
            "Expected to find orders table as FK cascade target"
        );

        // Cleanup
        client
            .execute("DROP TABLE IF EXISTS orders CASCADE", &[])
            .await
            .unwrap();
        client
            .execute("DROP TABLE IF EXISTS users CASCADE", &[])
            .await
            .unwrap();
    }

    #[tokio::test]
    #[ignore]
    async fn test_cascade_safety_check_fails() {
        // This test verifies that we catch the case where a CASCADE target is not being copied
        let source_url = std::env::var("TEST_SOURCE_URL")
            .expect("TEST_SOURCE_URL must be set for integration tests");
        let target_url = std::env::var("TEST_TARGET_URL")
            .expect("TEST_TARGET_URL must be set for integration tests");

        let source_client = postgres::connect(&source_url).await.unwrap();
        let target_client = postgres::connect(&target_url).await.unwrap();

        // Create test tables with FK relationship on both source and target
        for client in &[&source_client, &target_client] {
            client
                .execute(
                    "CREATE TABLE IF NOT EXISTS users (id SERIAL PRIMARY KEY, name TEXT)",
                    &[],
                )
                .await
                .unwrap();

            client
                .execute(
                    "CREATE TABLE IF NOT EXISTS orders (
                        id SERIAL PRIMARY KEY,
                        user_id INTEGER REFERENCES users(id),
                        amount DECIMAL
                    )",
                    &[],
                )
                .await
                .unwrap();
        }

        // Try to replicate only orders (FK to users), without including users
        // This should fail the safety check
        let tables = vec![(
            "\"public\".\"orders\"".to_string(),
            "amount > 0".to_string(),
        )];

        let result = copy_filtered_tables(&source_url, &target_url, &tables).await;

        // Should fail with safety check error
        assert!(result.is_err());
        let err_msg = format!("{:?}", result.unwrap_err());
        assert!(
            err_msg.contains("FK-related") || err_msg.contains("data loss"),
            "Expected FK safety error, got: {}",
            err_msg
        );

        // Cleanup
        for client in &[&source_client, &target_client] {
            client
                .execute("DROP TABLE IF EXISTS orders CASCADE", &[])
                .await
                .unwrap();
            client
                .execute("DROP TABLE IF EXISTS users CASCADE", &[])
                .await
                .unwrap();
        }
    }

    #[tokio::test]
    #[ignore]
    async fn test_cascade_with_all_tables() {
        // This test verifies that filtered snapshot succeeds when all FK-related tables are included
        let source_url = std::env::var("TEST_SOURCE_URL")
            .expect("TEST_SOURCE_URL must be set for integration tests");
        let target_url = std::env::var("TEST_TARGET_URL")
            .expect("TEST_TARGET_URL must be set for integration tests");

        let source_client = postgres::connect(&source_url).await.unwrap();
        let target_client = postgres::connect(&target_url).await.unwrap();

        // Create test tables with FK relationship and insert test data
        for client in &[&source_client, &target_client] {
            client
                .execute("DROP TABLE IF EXISTS orders CASCADE", &[])
                .await
                .unwrap();
            client
                .execute("DROP TABLE IF EXISTS users CASCADE", &[])
                .await
                .unwrap();

            client
                .execute("CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT)", &[])
                .await
                .unwrap();

            client
                .execute(
                    "CREATE TABLE orders (
                        id SERIAL PRIMARY KEY,
                        user_id INTEGER REFERENCES users(id),
                        amount DECIMAL
                    )",
                    &[],
                )
                .await
                .unwrap();
        }

        // Insert test data in source
        source_client
            .execute("INSERT INTO users (name) VALUES ('Alice'), ('Bob')", &[])
            .await
            .unwrap();
        source_client
            .execute(
                "INSERT INTO orders (user_id, amount) VALUES (1, 100), (1, 200), (2, 50)",
                &[],
            )
            .await
            .unwrap();

        // Replicate both tables with filtering (only orders with amount > 75)
        let tables = vec![
            (
                "\"public\".\"users\"".to_string(),
                "id IN (SELECT user_id FROM orders WHERE amount > 75)".to_string(),
            ),
            (
                "\"public\".\"orders\"".to_string(),
                "amount > 75".to_string(),
            ),
        ];

        let result = copy_filtered_tables(&source_url, &target_url, &tables).await;

        // Should succeed since all FK-related tables are included
        assert!(
            result.is_ok(),
            "Filtered snapshot should succeed when all FK tables included: {:?}",
            result
        );

        // Verify filtered data
        let rows = target_client
            .query("SELECT COUNT(*) FROM orders", &[])
            .await
            .unwrap();
        let count: i64 = rows[0].get(0);
        assert_eq!(count, 2, "Should have 2 orders (amount > 75)");

        // Cleanup
        for client in &[&source_client, &target_client] {
            client
                .execute("DROP TABLE IF EXISTS orders CASCADE", &[])
                .await
                .unwrap();
            client
                .execute("DROP TABLE IF EXISTS users CASCADE", &[])
                .await
                .unwrap();
        }
    }
}
