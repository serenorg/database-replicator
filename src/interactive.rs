// ABOUTME: Interactive terminal UI for database and table selection
// ABOUTME: Provides multi-select interface for selective replication and table rules

use crate::{
    filters::ReplicationFilter,
    migration, postgres,
    table_rules::{QualifiedTable, TableRules},
};
use anyhow::{Context, Result};
use dialoguer::{theme::ColorfulTheme, Confirm, Input, MultiSelect, Select};

/// Interactive database and table selection with advanced filtering
///
/// Presents a terminal UI for selecting:
/// 1. Which databases to replicate (multi-select)
/// 2. For each selected database:
///    - Which tables to exclude entirely
///    - Which tables to replicate schema-only (no data)
///    - Which tables to apply time-based filters
/// 3. Summary and confirmation
///
/// Returns a tuple of `(ReplicationFilter, TableRules)` representing the user's selections.
///
/// # Arguments
///
/// * `source_url` - PostgreSQL connection string for source database
///
/// # Returns
///
/// Returns `Ok((ReplicationFilter, TableRules))` with the user's selections or an error if:
/// - Cannot connect to source database
/// - Cannot discover databases or tables
/// - User cancels the operation
///
/// # Examples
///
/// ```no_run
/// # use anyhow::Result;
/// # use database_replicator::interactive::select_databases_and_tables;
/// # async fn example() -> Result<()> {
/// let (filter, rules) = select_databases_and_tables(
///     "postgresql://user:pass@source.example.com/postgres"
/// ).await?;
/// # Ok(())
/// # }
/// ```
pub async fn select_databases_and_tables(
    source_url: &str,
) -> Result<(ReplicationFilter, TableRules)> {
    tracing::info!("Starting interactive database and table selection...");
    tracing::info!("");

    // Connect to source database
    tracing::info!("Connecting to source database...");
    let source_client = postgres::connect_with_retry(source_url)
        .await
        .context("Failed to connect to source database")?;
    tracing::info!("✓ Connected to source");
    tracing::info!("");

    // Discover databases
    tracing::info!("Discovering databases on source...");
    let all_databases = migration::list_databases(&source_client)
        .await
        .context("Failed to list databases on source")?;

    if all_databases.is_empty() {
        tracing::warn!("⚠ No user databases found on source");
        tracing::warn!("  Source appears to contain only template databases");
        return Ok((ReplicationFilter::empty(), TableRules::default()));
    }

    tracing::info!("✓ Found {} database(s)", all_databases.len());
    tracing::info!("");

    // Step 1: Select databases to replicate
    println!("Select databases to replicate:");
    println!("(Use arrow keys to navigate, Space to select, Enter to confirm)");
    println!();

    let db_names: Vec<String> = all_databases.iter().map(|db| db.name.clone()).collect();

    let db_selections = MultiSelect::with_theme(&ColorfulTheme::default())
        .items(&db_names)
        .interact()
        .context("Failed to get database selection")?;

    if db_selections.is_empty() {
        tracing::warn!("⚠ No databases selected");
        tracing::info!("  Cancelling interactive selection");
        return Ok((ReplicationFilter::empty(), TableRules::default()));
    }

    let selected_databases: Vec<String> = db_selections
        .iter()
        .map(|&idx| db_names[idx].clone())
        .collect();

    tracing::info!("");
    tracing::info!("✓ Selected {} database(s):", selected_databases.len());
    for db in &selected_databases {
        tracing::info!("  - {}", db);
    }
    tracing::info!("");

    // Step 2: For each selected database, configure table-level rules
    let mut excluded_tables: Vec<String> = Vec::new();
    let mut table_rules = TableRules::default();

    for db_name in &selected_databases {
        // Build database-specific connection URL
        let db_url = replace_database_in_url(source_url, db_name)
            .context(format!("Failed to build URL for database '{}'", db_name))?;

        // Connect to the specific database
        tracing::info!("Discovering tables in database '{}'...", db_name);
        let db_client = postgres::connect_with_retry(&db_url)
            .await
            .context(format!("Failed to connect to database '{}'", db_name))?;

        let all_tables = migration::list_tables(&db_client)
            .await
            .context(format!("Failed to list tables from database '{}'", db_name))?;

        if all_tables.is_empty() {
            tracing::info!("  No tables found in database '{}'", db_name);
            tracing::info!("");
            continue;
        }

        tracing::info!("✓ Found {} table(s) in '{}'", all_tables.len(), db_name);
        tracing::info!("");

        // Format table names for display
        let table_display_names: Vec<String> = all_tables
            .iter()
            .map(|t| {
                if t.schema == "public" {
                    t.name.clone()
                } else {
                    format!("{}.{}", t.schema, t.name)
                }
            })
            .collect();

        println!(
            "Select tables to EXCLUDE from '{}' (or press Enter to include all):",
            db_name
        );
        println!("(Use arrow keys to navigate, Space to select, Enter to confirm)");
        println!();

        let table_exclusions = MultiSelect::with_theme(&ColorfulTheme::default())
            .items(&table_display_names)
            .interact()
            .context(format!(
                "Failed to get table exclusion selection for database '{}'",
                db_name
            ))?;

        // Track which tables are excluded
        let excluded_indices: std::collections::HashSet<usize> =
            table_exclusions.iter().copied().collect();

        if !table_exclusions.is_empty() {
            let excluded_in_db: Vec<String> = table_exclusions
                .iter()
                .map(|&idx| {
                    // Build full table name in "database.table" format
                    format!("{}.{}", db_name, table_display_names[idx])
                })
                .collect();

            tracing::info!("");
            tracing::info!(
                "✓ Excluding {} table(s) from '{}':",
                excluded_in_db.len(),
                db_name
            );
            for table in &excluded_in_db {
                tracing::info!("  - {}", table);
            }

            excluded_tables.extend(excluded_in_db);
        } else {
            tracing::info!("");
            tracing::info!("✓ Including all tables from '{}'", db_name);
        }

        tracing::info!("");

        // Step 2a: Select tables for schema-only replication (from non-excluded tables)
        let remaining_tables: Vec<(usize, String)> = table_display_names
            .iter()
            .enumerate()
            .filter(|(idx, _)| !excluded_indices.contains(idx))
            .map(|(idx, name)| (idx, name.clone()))
            .collect();

        if !remaining_tables.is_empty() {
            let remaining_names: Vec<String> = remaining_tables
                .iter()
                .map(|(_, name)| name.clone())
                .collect();

            println!(
                "Select tables to replicate SCHEMA-ONLY (no data) from '{}' (or press Enter to skip):",
                db_name
            );
            println!("(Use arrow keys to navigate, Space to select, Enter to confirm)");
            println!();

            let schema_only_selections = MultiSelect::with_theme(&ColorfulTheme::default())
                .items(&remaining_names)
                .interact()
                .context(format!(
                    "Failed to get schema-only selection for database '{}'",
                    db_name
                ))?;

            if !schema_only_selections.is_empty() {
                tracing::info!("");
                tracing::info!(
                    "✓ Schema-only replication for {} table(s) from '{}':",
                    schema_only_selections.len(),
                    db_name
                );

                for &selection_idx in &schema_only_selections {
                    let (original_idx, display_name) = &remaining_tables[selection_idx];
                    let table_info = &all_tables[*original_idx];

                    tracing::info!("  - {}", display_name);

                    // Add to table rules
                    let qualified = QualifiedTable::new(
                        Some(db_name.clone()),
                        table_info.schema.clone(),
                        table_info.name.clone(),
                    );
                    table_rules.add_schema_only_table(qualified)?;
                }
            }

            tracing::info!("");

            // Step 2b: Configure time filters for remaining tables (not excluded, not schema-only)
            let schema_only_indices: std::collections::HashSet<usize> = schema_only_selections
                .iter()
                .map(|&sel_idx| remaining_tables[sel_idx].0)
                .collect();

            let tables_for_time_filter: Vec<(usize, String)> = remaining_tables
                .iter()
                .filter(|(idx, _)| !schema_only_indices.contains(idx))
                .cloned()
                .collect();

            if !tables_for_time_filter.is_empty() {
                let confirm_time_filters = Confirm::with_theme(&ColorfulTheme::default())
                    .with_prompt(format!(
                        "Configure time-based filters for tables in '{}'?",
                        db_name
                    ))
                    .default(false)
                    .interact()
                    .context("Failed to get time filter confirmation")?;

                if confirm_time_filters {
                    tracing::info!("");
                    tracing::info!("Configuring time filters for '{}'...", db_name);

                    for (original_idx, display_name) in &tables_for_time_filter {
                        let table_info = &all_tables[*original_idx];

                        let apply_filter = Confirm::with_theme(&ColorfulTheme::default())
                            .with_prompt(format!("Apply time filter to '{}'?", display_name))
                            .default(false)
                            .interact()
                            .context("Failed to get time filter confirmation")?;

                        if apply_filter {
                            // Query table columns to show actual timestamp columns
                            let db_client = postgres::connect_with_retry(&db_url)
                                .await
                                .with_context(|| {
                                    format!(
                                        "Failed to connect to database '{}' for column query",
                                        db_name
                                    )
                                })?;

                            let columns = migration::get_table_columns(
                                &db_client,
                                &table_info.schema,
                                &table_info.name,
                            )
                            .await?;

                            // Filter for timestamp columns and build display strings
                            let timestamp_columns: Vec<(String, String)> = columns
                                .iter()
                                .filter(|col| col.is_timestamp)
                                .map(|col| {
                                    let display = format!("{} ({})", col.name, col.data_type);
                                    (col.name.clone(), display)
                                })
                                .collect();

                            // Prompt for column selection
                            let column = if timestamp_columns.is_empty() {
                                // No timestamp columns found - fall back to manual entry
                                tracing::warn!(
                                    "  ⚠ No timestamp columns found in table '{}'. Please enter column name manually.",
                                    display_name
                                );
                                Input::with_theme(&ColorfulTheme::default())
                                    .with_prompt("  Column name")
                                    .default("created_at".to_string())
                                    .interact_text()
                                    .context("Failed to get column name")?
                            } else {
                                // Show timestamp columns in a selection list
                                let display_options: Vec<String> = timestamp_columns
                                    .iter()
                                    .map(|(_, display)| display.clone())
                                    .chain(std::iter::once(
                                        "[Enter custom column name]".to_string(),
                                    ))
                                    .collect();

                                let selection = Select::with_theme(&ColorfulTheme::default())
                                    .with_prompt("  Select timestamp column for filtering")
                                    .items(&display_options)
                                    .default(0)
                                    .interact()
                                    .context("Failed to select timestamp column")?;

                                if selection < timestamp_columns.len() {
                                    // User selected a timestamp column
                                    timestamp_columns[selection].0.clone()
                                } else {
                                    // User chose "custom" option
                                    Input::with_theme(&ColorfulTheme::default())
                                        .with_prompt("  Column name")
                                        .interact_text()
                                        .context("Failed to get custom column name")?
                                }
                            };

                            // Prompt for time window
                            let window: String = Input::with_theme(&ColorfulTheme::default())
                                .with_prompt(
                                    "  How far back to replicate data (e.g., '2 months', '90 days', '1 year')",
                                )
                                .default("2 months".to_string())
                                .interact_text()
                                .context("Failed to get time window")?;

                            tracing::info!(
                                "  ✓ Time filter for '{}': {} >= NOW() - INTERVAL '{}'",
                                display_name,
                                column,
                                window
                            );

                            // Add to table rules
                            let qualified = QualifiedTable::new(
                                Some(db_name.clone()),
                                table_info.schema.clone(),
                                table_info.name.clone(),
                            );
                            table_rules.add_time_filter(qualified, column, window)?;
                        }
                    }
                }
            }
        }

        tracing::info!("");
    }

    // Step 3: Show summary and confirm
    println!();
    println!("========================================");
    println!("Replication Configuration Summary");
    println!("========================================");
    println!();
    println!("Databases to replicate: {}", selected_databases.len());
    for db in &selected_databases {
        println!("  ✓ {}", db);
    }
    println!();

    if !excluded_tables.is_empty() {
        println!("Tables to exclude: {}", excluded_tables.len());
        for table in &excluded_tables {
            println!("  ✗ {}", table);
        }
        println!();
    }

    // Show schema-only tables
    let mut schema_only_count = 0;
    for db in &selected_databases {
        schema_only_count += table_rules.schema_only_tables(db).len();
    }
    if schema_only_count > 0 {
        println!(
            "Schema-only tables (DDL only, no data): {}",
            schema_only_count
        );
        for db in &selected_databases {
            let schema_only = table_rules.schema_only_tables(db);
            if !schema_only.is_empty() {
                for table in schema_only {
                    println!("  📋 {}.{}", db, table);
                }
            }
        }
        println!();
    }

    // Show time filters
    let mut time_filter_count = 0;
    for db in &selected_databases {
        time_filter_count += table_rules.predicate_tables(db).len();
    }
    if time_filter_count > 0 {
        println!("Tables with time-based filters: {}", time_filter_count);
        for db in &selected_databases {
            let predicate_tables = table_rules.predicate_tables(db);
            if !predicate_tables.is_empty() {
                for (table, predicate) in predicate_tables {
                    println!("  🕒 {}.{} [{}]", db, table, predicate);
                }
            }
        }
        println!();
    }

    println!("========================================");
    println!();

    let confirmed = Confirm::with_theme(&ColorfulTheme::default())
        .with_prompt("Proceed with this configuration?")
        .default(true)
        .interact()
        .context("Failed to get confirmation")?;

    if !confirmed {
        tracing::warn!("⚠ User cancelled operation");
        anyhow::bail!("Interactive selection cancelled by user");
    }

    tracing::info!("");
    tracing::info!("✓ Configuration confirmed");
    tracing::info!("");

    // Step 4: Convert selections to ReplicationFilter
    let filter = if excluded_tables.is_empty() {
        // No table exclusions - just filter by databases
        ReplicationFilter::new(Some(selected_databases), None, None, None)?
    } else {
        // Include selected databases and exclude specific tables
        ReplicationFilter::new(Some(selected_databases), None, None, Some(excluded_tables))?
    };

    Ok((filter, table_rules))
}

/// Replace the database name in a PostgreSQL connection URL
///
/// # Arguments
///
/// * `url` - PostgreSQL connection URL
/// * `new_db_name` - New database name to use
///
/// # Returns
///
/// URL with the database name replaced
fn replace_database_in_url(url: &str, new_db_name: &str) -> Result<String> {
    // Split into base URL and query parameters
    let parts: Vec<&str> = url.splitn(2, '?').collect();
    let base_url = parts[0];
    let query_params = parts.get(1);

    // Split base URL by '/' to replace the database name
    let url_parts: Vec<&str> = base_url.rsplitn(2, '/').collect();

    if url_parts.len() != 2 {
        anyhow::bail!("Invalid connection URL format: cannot replace database name");
    }

    // Rebuild URL with new database name
    let new_url = if let Some(params) = query_params {
        format!("{}/{}?{}", url_parts[1], new_db_name, params)
    } else {
        format!("{}/{}", url_parts[1], new_db_name)
    };

    Ok(new_url)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_replace_database_in_url() {
        // Basic URL
        let url = "postgresql://user:pass@localhost:5432/olddb";
        let new_url = replace_database_in_url(url, "newdb").unwrap();
        assert_eq!(new_url, "postgresql://user:pass@localhost:5432/newdb");

        // URL with query parameters
        let url = "postgresql://user:pass@localhost:5432/olddb?sslmode=require";
        let new_url = replace_database_in_url(url, "newdb").unwrap();
        assert_eq!(
            new_url,
            "postgresql://user:pass@localhost:5432/newdb?sslmode=require"
        );

        // URL without port
        let url = "postgresql://user:pass@localhost/olddb";
        let new_url = replace_database_in_url(url, "newdb").unwrap();
        assert_eq!(new_url, "postgresql://user:pass@localhost/newdb");
    }

    #[tokio::test]
    #[ignore]
    async fn test_interactive_selection() {
        // This test requires a real source database and manual interaction
        let source_url = std::env::var("TEST_SOURCE_URL").unwrap();

        let result = select_databases_and_tables(&source_url).await;

        // This will only work with manual interaction
        match &result {
            Ok((filter, rules)) => {
                println!("✓ Interactive selection completed");
                println!("Filter: {:?}", filter);
                println!("Rules: {:?}", rules);
            }
            Err(e) => {
                println!("Interactive selection error: {:?}", e);
            }
        }
    }
}
