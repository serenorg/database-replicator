// ABOUTME: Publication management for logical replication on source database
// ABOUTME: Creates and manages PostgreSQL publications for table replication

use anyhow::{bail, Context, Result};
use tokio_postgres::Client;

use crate::filters::ReplicationFilter;
use crate::table_rules::TableRuleKind;

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
    // Validate publication name to prevent SQL injection
    crate::utils::validate_postgres_identifier(publication_name).with_context(|| {
        format!(
            "Invalid publication name '{}': must be a valid PostgreSQL identifier",
            publication_name
        )
    })?;

    tracing::info!("Creating publication '{}'...", publication_name);

    if filter.is_empty() {
        let query = format!("CREATE PUBLICATION \"{}\" FOR ALL TABLES", publication_name);
        return execute_publication_query(client, publication_name, &query).await;
    }

    let tables = crate::migration::list_tables(client).await?;

    let mut plain_tables = Vec::new();
    let mut predicate_tables = Vec::new();

    for table in tables {
        // Build "schema.table" identifier for include/exclude logic
        let table_identifier = if table.schema == "public" {
            table.name.clone()
        } else {
            format!("{}.{}", table.schema, table.name)
        };

        if !filter.should_replicate_table(db_name, &table_identifier) {
            continue;
        }

        // Validate schema/table names
        crate::utils::validate_postgres_identifier(&table.schema).with_context(|| {
            format!(
                "Invalid schema name '{}' for table '{}': must be a valid PostgreSQL identifier",
                table.schema, table.name
            )
        })?;
        crate::utils::validate_postgres_identifier(&table.name).with_context(|| {
            format!(
                "Invalid table name '{}' in schema '{}': must be a valid PostgreSQL identifier",
                table.name, table.schema
            )
        })?;

        let fq_table = format!("\"{}\".\"{}\"", table.schema, table.name);

        match filter
            .table_rules()
            .rule_for_table(db_name, &table.schema, &table.name)
        {
            Some(TableRuleKind::SchemaOnly) => {
                tracing::debug!(
                    "Excluding table '{}' from publication (schema-only)",
                    table_identifier
                );
            }
            Some(TableRuleKind::Predicate(pred)) => {
                predicate_tables.push((fq_table, pred));
            }
            None => {
                plain_tables.push(fq_table);
            }
        }
    }

    if plain_tables.is_empty() && predicate_tables.is_empty() {
        bail!(
            "No tables available for publication '{}' after applying filters and schema-only rules",
            publication_name
        );
    }

    let has_predicates = !predicate_tables.is_empty();
    let server_version = get_server_version(client).await?;
    if has_predicates && server_version < 150000 {
        bail!(
            "Table-level predicates require PostgreSQL 15+. Detected server version {}.\n\
             Upgrade the source database or remove --table-filter/--time-filter for logical replication.",
            server_version
        );
    }

    let mut clauses = Vec::new();
    clauses.extend(plain_tables);
    clauses.extend(
        predicate_tables
            .iter()
            .map(|(table, predicate)| format!("{} WHERE ({})", table, predicate)),
    );

    let query = format!(
        "CREATE PUBLICATION \"{}\" FOR TABLE {}",
        publication_name,
        clauses.join(", ")
    );

    execute_publication_query(client, publication_name, &query).await
}

async fn execute_publication_query(
    client: &Client,
    publication_name: &str,
    query: &str,
) -> Result<()> {
    match client.execute(query, &[]).await {
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

async fn get_server_version(client: &Client) -> Result<i32> {
    let row = client
        .query_one("SHOW server_version_num", &[])
        .await
        .context("Failed to query server version")?;
    let version_str: String = row.get(0);
    version_str.parse::<i32>().with_context(|| {
        format!(
            "Invalid server_version_num '{}'. Expected integer.",
            version_str
        )
    })
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
    // Validate publication name to prevent SQL injection
    crate::utils::validate_postgres_identifier(publication_name).with_context(|| {
        format!(
            "Invalid publication name '{}': must be a valid PostgreSQL identifier",
            publication_name
        )
    })?;

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
