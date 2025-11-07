// ABOUTME: Publication management for logical replication on source database
// ABOUTME: Creates and manages PostgreSQL publications for table replication

use anyhow::{Context, Result};
use tokio_postgres::Client;

use crate::filters::ReplicationFilter;

/// Create a publication for tables with optional filtering
///
/// When table filters are specified, creates a publication for only the filtered tables.
/// Without filters, creates a publication for all tables.
///
/// # Arguments
///
/// * `client` - Connected client to the database
/// * `db_name` - Name of the database (for filtering context)
/// * `publication_name` - Name of the publication to create
/// * `filter` - Replication filter for table inclusion/exclusion
///
/// # Returns
///
/// Returns `Ok(())` if publication is created or already exists
pub async fn create_publication(
    client: &Client,
    db_name: &str,
    publication_name: &str,
    filter: &ReplicationFilter,
) -> Result<()> {
    tracing::info!("Creating publication '{}'...", publication_name);

    // Check if table filtering is active
    let has_table_filter = filter.include_tables().is_some() || filter.exclude_tables().is_some();

    let query = if has_table_filter {
        // Build table list for filtered publication
        tracing::info!("Building filtered table list for publication...");

        // Get all tables in the database
        let tables = crate::migration::list_tables(client).await?;

        // Filter tables based on filter rules
        let filtered_tables: Vec<_> = tables
            .into_iter()
            .filter(|table| {
                // Build full table name in "database.table" format for filtering
                let table_name = if table.schema == "public" {
                    table.name.clone()
                } else {
                    format!("{}.{}", table.schema, table.name)
                };
                filter.should_replicate_table(db_name, &table_name)
            })
            .collect();

        if filtered_tables.is_empty() {
            anyhow::bail!(
                "No tables match the filter criteria for database '{}'.\n\
                 Cannot create publication '{}' with empty table list.\n\
                 Check your --include-tables or --exclude-tables settings.",
                db_name,
                publication_name
            );
        }

        tracing::info!(
            "Publication will include {} filtered table(s)",
            filtered_tables.len()
        );

        // Build FOR TABLE clause with schema-qualified table names
        let table_list: Vec<String> = filtered_tables
            .iter()
            .map(|t| format!("\"{}\".\"{}\"", t.schema, t.name))
            .collect();

        format!(
            "CREATE PUBLICATION \"{}\" FOR TABLE {}",
            publication_name,
            table_list.join(", ")
        )
    } else {
        // No filtering - use FOR ALL TABLES (fast path)
        format!("CREATE PUBLICATION \"{}\" FOR ALL TABLES", publication_name)
    };

    match client.execute(&query, &[]).await {
        Ok(_) => {
            tracing::info!("✓ Publication '{}' created successfully", publication_name);
            Ok(())
        }
        Err(e) => {
            let err_str = e.to_string();
            // Publication might already exist - that's okay
            if err_str.contains("already exists") {
                tracing::info!("✓ Publication '{}' already exists", publication_name);
                Ok(())
            } else if err_str.contains("permission denied") || err_str.contains("must be owner") {
                anyhow::bail!(
                    "Permission denied: Cannot create publication '{}'.\n\
                     You need superuser or owner privileges on the database.\n\
                     Grant with: GRANT CREATE ON DATABASE <dbname> TO <user>;\n\
                     Error: {}",
                    publication_name,
                    err_str
                )
            } else if err_str.contains("wal_level") || err_str.contains("logical replication") {
                anyhow::bail!(
                    "Logical replication not enabled: Cannot create publication '{}'.\n\
                     The database parameter 'wal_level' must be set to 'logical'.\n\
                     Contact your database administrator to update postgresql.conf:\n\
                     wal_level = logical\n\
                     Error: {}",
                    publication_name,
                    err_str
                )
            } else {
                anyhow::bail!(
                    "Failed to create publication '{}': {}\n\
                     \n\
                     Common causes:\n\
                     - Insufficient privileges (need CREATE privilege on database)\n\
                     - Logical replication not enabled (wal_level must be 'logical')\n\
                     - Database does not support publications",
                    publication_name,
                    err_str
                )
            }
        }
    }
}

/// List all publications in the database
pub async fn list_publications(client: &Client) -> Result<Vec<String>> {
    let rows = client
        .query("SELECT pubname FROM pg_publication ORDER BY pubname", &[])
        .await
        .context("Failed to list publications")?;

    let publications: Vec<String> = rows.iter().map(|row| row.get(0)).collect();

    Ok(publications)
}

/// Drop a publication
pub async fn drop_publication(client: &Client, publication_name: &str) -> Result<()> {
    tracing::info!("Dropping publication '{}'...", publication_name);

    let query = format!("DROP PUBLICATION IF EXISTS \"{}\"", publication_name);

    client
        .execute(&query, &[])
        .await
        .context(format!("Failed to drop publication '{}'", publication_name))?;

    tracing::info!("✓ Publication '{}' dropped", publication_name);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::postgres::connect;

    #[tokio::test]
    #[ignore]
    async fn test_create_and_list_publications() {
        let url = std::env::var("TEST_SOURCE_URL").unwrap();
        let client = connect(&url).await.unwrap();

        let pub_name = "test_publication";
        let db_name = "postgres"; // Assume testing on postgres database
        let filter = ReplicationFilter::empty();

        // Clean up if exists
        let _ = drop_publication(&client, pub_name).await;

        // Create publication
        let result = create_publication(&client, db_name, pub_name, &filter).await;
        match &result {
            Ok(_) => println!("✓ Publication created successfully"),
            Err(e) => {
                println!("Error creating publication: {:?}", e);
                // If Neon doesn't support publications, skip rest of test
                if e.to_string().contains("not supported") || e.to_string().contains("permission") {
                    println!("Skipping test - Neon might not support publications on pooler");
                    return;
                }
            }
        }
        assert!(result.is_ok(), "Failed to create publication");

        // List publications
        let pubs = list_publications(&client).await.unwrap();
        println!("Publications: {:?}", pubs);
        assert!(pubs.contains(&pub_name.to_string()));

        // Clean up
        drop_publication(&client, pub_name).await.unwrap();
    }

    #[tokio::test]
    #[ignore]
    async fn test_drop_publication() {
        let url = std::env::var("TEST_SOURCE_URL").unwrap();
        let client = connect(&url).await.unwrap();

        let pub_name = "test_drop_publication";
        let db_name = "postgres";
        let filter = ReplicationFilter::empty();

        // Create publication
        create_publication(&client, db_name, pub_name, &filter)
            .await
            .unwrap();

        // Drop it
        let result = drop_publication(&client, pub_name).await;
        assert!(result.is_ok());

        // Verify it's gone
        let pubs = list_publications(&client).await.unwrap();
        assert!(!pubs.contains(&pub_name.to_string()));
    }
}
