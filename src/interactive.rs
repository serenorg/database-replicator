// ABOUTME: Interactive terminal UI for database and table selection
// ABOUTME: Provides multi-step wizard with back navigation using inquire crate

use crate::{
    filters::ReplicationFilter,
    migration, postgres,
    table_rules::{QualifiedTable, TableRules},
};
use anyhow::{Context, Result};
use inquire::{Confirm, MultiSelect, Select, Text};

/// Wizard step state machine
enum WizardStep {
    SelectDatabases,
    SelectTablesForDb(usize), // index of current database in selected_dbs
    SelectSchemaOnlyForDb(usize), // schema-only tables selection
    ConfigureTimeFiltersForDb(usize), // time filter configuration
    Review,
}

/// Cached table info for a database (to avoid repeated queries)
struct CachedDbTables {
    all_tables: Vec<migration::TableInfo>,
    table_display_names: Vec<String>,
}

/// Interactive database and table selection with back navigation
///
/// Presents a terminal UI for selecting:
/// 1. Which databases to replicate (multi-select)
/// 2. For each selected database: tables to exclude
/// 3. For each selected database: tables to replicate schema-only (no data)
/// 4. For each selected database: time-based filters
/// 5. Summary and confirmation
///
/// Supports back navigation:
/// - Cancel/Esc from any step → go back to previous step
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
    println!();

    // Connect to source database
    tracing::info!("Connecting to source database...");
    let source_client = postgres::connect_with_retry(source_url)
        .await
        .context("Failed to connect to source database")?;
    tracing::info!("✓ Connected to source");
    println!();

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
    println!();

    let db_names: Vec<String> = all_databases.iter().map(|db| db.name.clone()).collect();

    // State for wizard
    let mut selected_db_indices: Vec<usize> = Vec::new();
    let mut current_step = WizardStep::SelectDatabases;

    // Track selections per database for back navigation
    let mut excluded_tables_by_db: std::collections::HashMap<String, Vec<String>> =
        std::collections::HashMap::new();
    let mut schema_only_by_db: std::collections::HashMap<String, Vec<(String, String)>> =
        std::collections::HashMap::new(); // (schema, table)
    let mut time_filters_by_db: std::collections::HashMap<
        String,
        Vec<(String, String, String, String)>,
    > = std::collections::HashMap::new(); // (schema, table, column, window)

    // Cache table info per database to avoid repeated queries
    let mut table_cache: std::collections::HashMap<String, CachedDbTables> =
        std::collections::HashMap::new();

    loop {
        match current_step {
            WizardStep::SelectDatabases => {
                print_header("Step 1 of 5: Select Databases");
                println!("Navigation: Space to toggle, Enter to confirm, Esc to cancel");
                println!();

                let defaults: Vec<usize> = selected_db_indices.clone();

                let selections =
                    MultiSelect::new("Select databases to replicate:", db_names.clone())
                        .with_default(&defaults)
                        .with_help_message("↑↓ navigate, Space toggle, Enter confirm")
                        .prompt();

                match selections {
                    Ok(selected) => {
                        // Convert selected names back to indices
                        selected_db_indices = selected
                            .iter()
                            .filter_map(|name| db_names.iter().position(|n| n == name))
                            .collect();

                        if selected_db_indices.is_empty() {
                            println!();
                            println!("⚠ Please select at least one database");
                            continue;
                        }

                        // Clear previous selections when re-selecting databases
                        excluded_tables_by_db.clear();
                        schema_only_by_db.clear();
                        time_filters_by_db.clear();
                        table_cache.clear();

                        current_step = WizardStep::SelectTablesForDb(0);
                    }
                    Err(inquire::InquireError::OperationCanceled) => {
                        anyhow::bail!("Operation cancelled by user");
                    }
                    Err(inquire::InquireError::OperationInterrupted) => {
                        anyhow::bail!("Operation interrupted");
                    }
                    Err(e) => return Err(e.into()),
                }
            }

            WizardStep::SelectTablesForDb(db_idx) => {
                let db_name = &db_names[selected_db_indices[db_idx]].clone();
                print_header(&format!(
                    "Step 2 of 5: Select Tables to Exclude ({}/{})",
                    db_idx + 1,
                    selected_db_indices.len()
                ));
                println!("Database: {}", db_name);
                println!("Navigation: Space to toggle, Enter to continue, Esc to go back");
                println!();

                // Get or cache tables for this database
                let cached = get_or_cache_tables(&mut table_cache, source_url, db_name).await?;

                if cached.all_tables.is_empty() {
                    println!("  No tables found in database '{}'", db_name);
                    // Skip to next database or next step
                    if db_idx + 1 < selected_db_indices.len() {
                        current_step = WizardStep::SelectTablesForDb(db_idx + 1);
                    } else {
                        current_step = WizardStep::SelectSchemaOnlyForDb(0);
                    }
                    continue;
                }

                // Get previously excluded tables for this database (for back navigation)
                let previous_exclusions: Vec<usize> = excluded_tables_by_db
                    .get(db_name)
                    .map(|excluded| {
                        excluded
                            .iter()
                            .filter_map(|t| {
                                // Strip db name prefix to match display names
                                let stripped =
                                    t.strip_prefix(&format!("{}.", db_name)).unwrap_or(t);
                                cached
                                    .table_display_names
                                    .iter()
                                    .position(|n| n == stripped)
                            })
                            .collect()
                    })
                    .unwrap_or_default();

                let selections = MultiSelect::new(
                    "Select tables to EXCLUDE (or press Enter to include all):",
                    cached.table_display_names.clone(),
                )
                .with_default(&previous_exclusions)
                .with_help_message("Space toggle, Enter confirm, Esc go back")
                .prompt();

                match selections {
                    Ok(selected_exclusions) => {
                        // Build exclusion list for this database
                        let db_exclusions: Vec<String> = selected_exclusions
                            .iter()
                            .map(|table_name| format!("{}.{}", db_name, table_name))
                            .collect();

                        // Store for back navigation
                        excluded_tables_by_db.insert(db_name.clone(), db_exclusions);

                        // Move to next database or schema-only step
                        if db_idx + 1 < selected_db_indices.len() {
                            current_step = WizardStep::SelectTablesForDb(db_idx + 1);
                        } else {
                            current_step = WizardStep::SelectSchemaOnlyForDb(0);
                        }
                    }
                    Err(inquire::InquireError::OperationCanceled) => {
                        // Go back to previous step
                        if db_idx > 0 {
                            current_step = WizardStep::SelectTablesForDb(db_idx - 1);
                        } else {
                            current_step = WizardStep::SelectDatabases;
                        }
                    }
                    Err(inquire::InquireError::OperationInterrupted) => {
                        anyhow::bail!("Operation interrupted");
                    }
                    Err(e) => return Err(e.into()),
                }
            }

            WizardStep::SelectSchemaOnlyForDb(db_idx) => {
                let db_name = &db_names[selected_db_indices[db_idx]].clone();
                print_header(&format!(
                    "Step 3 of 5: Schema-Only Tables ({}/{})",
                    db_idx + 1,
                    selected_db_indices.len()
                ));
                println!("Database: {}", db_name);
                println!("Schema-only tables replicate structure but NO data.");
                println!("Navigation: Space to toggle, Enter to continue, Esc to go back");
                println!();

                let cached = get_or_cache_tables(&mut table_cache, source_url, db_name).await?;

                if cached.all_tables.is_empty() {
                    // Skip to next database or time filters
                    if db_idx + 1 < selected_db_indices.len() {
                        current_step = WizardStep::SelectSchemaOnlyForDb(db_idx + 1);
                    } else {
                        current_step = WizardStep::ConfigureTimeFiltersForDb(0);
                    }
                    continue;
                }

                // Filter out excluded tables
                let excluded = excluded_tables_by_db.get(db_name);
                let available_tables: Vec<(usize, String)> = cached
                    .table_display_names
                    .iter()
                    .enumerate()
                    .filter(|(_, name)| {
                        let full_name = format!("{}.{}", db_name, name);
                        !excluded.is_some_and(|ex| ex.contains(&full_name))
                    })
                    .map(|(idx, name)| (idx, name.clone()))
                    .collect();

                if available_tables.is_empty() {
                    println!("  All tables excluded from '{}'", db_name);
                    if db_idx + 1 < selected_db_indices.len() {
                        current_step = WizardStep::SelectSchemaOnlyForDb(db_idx + 1);
                    } else {
                        current_step = WizardStep::ConfigureTimeFiltersForDb(0);
                    }
                    continue;
                }

                let available_names: Vec<String> =
                    available_tables.iter().map(|(_, n)| n.clone()).collect();

                // Get previous schema-only selections
                let previous_schema_only: Vec<usize> = schema_only_by_db
                    .get(db_name)
                    .map(|selected| {
                        selected
                            .iter()
                            .filter_map(|(schema, table)| {
                                let display = if schema == "public" {
                                    table.clone()
                                } else {
                                    format!("{}.{}", schema, table)
                                };
                                available_names.iter().position(|n| n == &display)
                            })
                            .collect()
                    })
                    .unwrap_or_default();

                let selections = MultiSelect::new(
                    "Select tables to replicate SCHEMA-ONLY (no data):",
                    available_names.clone(),
                )
                .with_default(&previous_schema_only)
                .with_help_message("Space toggle, Enter confirm, Esc go back")
                .prompt();

                match selections {
                    Ok(selected_schema_only) => {
                        // Convert to (schema, table) pairs
                        let schema_only_tables: Vec<(String, String)> = selected_schema_only
                            .iter()
                            .filter_map(|display_name| {
                                available_tables
                                    .iter()
                                    .find(|(_, n)| n == display_name)
                                    .map(|(idx, _)| {
                                        let t = &cached.all_tables[*idx];
                                        (t.schema.clone(), t.name.clone())
                                    })
                            })
                            .collect();

                        schema_only_by_db.insert(db_name.clone(), schema_only_tables);

                        if db_idx + 1 < selected_db_indices.len() {
                            current_step = WizardStep::SelectSchemaOnlyForDb(db_idx + 1);
                        } else {
                            current_step = WizardStep::ConfigureTimeFiltersForDb(0);
                        }
                    }
                    Err(inquire::InquireError::OperationCanceled) => {
                        // Go back
                        if db_idx > 0 {
                            current_step = WizardStep::SelectSchemaOnlyForDb(db_idx - 1);
                        } else {
                            let last_db = selected_db_indices.len().saturating_sub(1);
                            current_step = WizardStep::SelectTablesForDb(last_db);
                        }
                    }
                    Err(inquire::InquireError::OperationInterrupted) => {
                        anyhow::bail!("Operation interrupted");
                    }
                    Err(e) => return Err(e.into()),
                }
            }

            WizardStep::ConfigureTimeFiltersForDb(db_idx) => {
                let db_name = &db_names[selected_db_indices[db_idx]].clone();
                print_header(&format!(
                    "Step 4 of 5: Time Filters ({}/{})",
                    db_idx + 1,
                    selected_db_indices.len()
                ));
                println!("Database: {}", db_name);
                println!("Time filters limit data to recent records (e.g., last 90 days).");
                println!();

                let cached = get_or_cache_tables(&mut table_cache, source_url, db_name).await?;

                if cached.all_tables.is_empty() {
                    if db_idx + 1 < selected_db_indices.len() {
                        current_step = WizardStep::ConfigureTimeFiltersForDb(db_idx + 1);
                    } else {
                        current_step = WizardStep::Review;
                    }
                    continue;
                }

                // Filter out excluded and schema-only tables
                let excluded = excluded_tables_by_db.get(db_name);
                let schema_only = schema_only_by_db.get(db_name);
                let available_tables: Vec<(usize, String)> = cached
                    .table_display_names
                    .iter()
                    .enumerate()
                    .filter(|(idx, name)| {
                        let full_name = format!("{}.{}", db_name, name);
                        let is_excluded = excluded.is_some_and(|ex| ex.contains(&full_name));
                        let t = &cached.all_tables[*idx];
                        let is_schema_only = schema_only.is_some_and(|so| {
                            so.iter().any(|(s, n)| s == &t.schema && n == &t.name)
                        });
                        !is_excluded && !is_schema_only
                    })
                    .map(|(idx, name)| (idx, name.clone()))
                    .collect();

                if available_tables.is_empty() {
                    println!("  No tables available for time filtering in '{}'", db_name);
                    if db_idx + 1 < selected_db_indices.len() {
                        current_step = WizardStep::ConfigureTimeFiltersForDb(db_idx + 1);
                    } else {
                        current_step = WizardStep::Review;
                    }
                    continue;
                }

                // Ask if user wants to configure time filters
                let configure = Confirm::new("Configure time-based filters for this database?")
                    .with_default(false)
                    .with_help_message("Enter to confirm, Esc to go back")
                    .prompt();

                match configure {
                    Ok(true) => {
                        // Let user select tables to filter
                        let available_names: Vec<String> =
                            available_tables.iter().map(|(_, n)| n.clone()).collect();

                        let table_selections = MultiSelect::new(
                            "Select tables to apply time filter:",
                            available_names.clone(),
                        )
                        .with_help_message("Space toggle, Enter confirm")
                        .prompt();

                        match table_selections {
                            Ok(selected_tables) => {
                                let mut time_filters: Vec<(String, String, String, String)> =
                                    Vec::new();

                                for display_name in &selected_tables {
                                    if let Some((idx, _)) =
                                        available_tables.iter().find(|(_, n)| n == display_name)
                                    {
                                        let t = &cached.all_tables[*idx];
                                        let db_url = replace_database_in_url(source_url, db_name)?;
                                        let db_client = postgres::connect_with_retry(&db_url)
                                            .await
                                            .context("Failed to connect for column query")?;

                                        // Get timestamp columns
                                        let columns = migration::get_table_columns(
                                            &db_client, &t.schema, &t.name,
                                        )
                                        .await?;

                                        let timestamp_columns: Vec<String> = columns
                                            .iter()
                                            .filter(|c| c.is_timestamp)
                                            .map(|c| format!("{} ({})", c.name, c.data_type))
                                            .collect();

                                        println!();
                                        println!("Configure time filter for '{}':", display_name);

                                        let column = if timestamp_columns.is_empty() {
                                            println!(
                                                "  ⚠ No timestamp columns found. Enter column name manually."
                                            );
                                            Text::new("  Column name:")
                                                .with_default("created_at")
                                                .prompt()
                                                .context("Failed to get column name")?
                                        } else {
                                            let mut options = timestamp_columns.clone();
                                            options.push("[Enter custom column name]".to_string());

                                            let selection =
                                                Select::new("  Select timestamp column:", options)
                                                    .prompt()
                                                    .context("Failed to select column")?;

                                            if selection == "[Enter custom column name]" {
                                                Text::new("  Column name:")
                                                    .prompt()
                                                    .context("Failed to get column name")?
                                            } else {
                                                // Extract column name from "name (type)" format
                                                selection
                                                    .split(" (")
                                                    .next()
                                                    .unwrap_or(&selection)
                                                    .to_string()
                                            }
                                        };

                                        let window = Text::new(
                                            "  Time window (e.g., '90 days', '6 months', '1 year'):",
                                        )
                                        .with_default("90 days")
                                        .prompt()
                                        .context("Failed to get time window")?;

                                        time_filters.push((
                                            t.schema.clone(),
                                            t.name.clone(),
                                            column,
                                            window,
                                        ));
                                    }
                                }

                                time_filters_by_db.insert(db_name.clone(), time_filters);
                            }
                            Err(inquire::InquireError::OperationCanceled) => {
                                // Stay on this step
                                continue;
                            }
                            Err(inquire::InquireError::OperationInterrupted) => {
                                anyhow::bail!("Operation interrupted");
                            }
                            Err(e) => return Err(e.into()),
                        }

                        if db_idx + 1 < selected_db_indices.len() {
                            current_step = WizardStep::ConfigureTimeFiltersForDb(db_idx + 1);
                        } else {
                            current_step = WizardStep::Review;
                        }
                    }
                    Ok(false) => {
                        // Skip time filters for this database
                        if db_idx + 1 < selected_db_indices.len() {
                            current_step = WizardStep::ConfigureTimeFiltersForDb(db_idx + 1);
                        } else {
                            current_step = WizardStep::Review;
                        }
                    }
                    Err(inquire::InquireError::OperationCanceled) => {
                        // Go back
                        if db_idx > 0 {
                            current_step = WizardStep::ConfigureTimeFiltersForDb(db_idx - 1);
                        } else {
                            let last_db = selected_db_indices.len().saturating_sub(1);
                            current_step = WizardStep::SelectSchemaOnlyForDb(last_db);
                        }
                    }
                    Err(inquire::InquireError::OperationInterrupted) => {
                        anyhow::bail!("Operation interrupted");
                    }
                    Err(e) => return Err(e.into()),
                }
            }

            WizardStep::Review => {
                print_header("Step 5 of 5: Review Configuration");

                // Collect all exclusions
                let excluded_tables: Vec<String> =
                    excluded_tables_by_db.values().flatten().cloned().collect();

                let selected_databases: Vec<String> = selected_db_indices
                    .iter()
                    .map(|&i| db_names[i].clone())
                    .collect();

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
                } else {
                    println!("Tables to exclude: none");
                    println!();
                }

                // Show schema-only tables
                let schema_only_count: usize = schema_only_by_db.values().map(|v| v.len()).sum();
                if schema_only_count > 0 {
                    println!("Schema-only tables (no data): {}", schema_only_count);
                    for (db, tables) in &schema_only_by_db {
                        for (schema, table) in tables {
                            let display = if schema == "public" {
                                format!("{}.{}", db, table)
                            } else {
                                format!("{}.{}.{}", db, schema, table)
                            };
                            println!("  ◇ {}", display);
                        }
                    }
                    println!();
                } else {
                    println!("Schema-only tables: none");
                    println!();
                }

                // Show time filters
                let time_filter_count: usize = time_filters_by_db.values().map(|v| v.len()).sum();
                if time_filter_count > 0 {
                    println!("Time-filtered tables: {}", time_filter_count);
                    for (db, filters) in &time_filters_by_db {
                        for (schema, table, column, window) in filters {
                            let display = if schema == "public" {
                                format!("{}.{}", db, table)
                            } else {
                                format!("{}.{}.{}", db, schema, table)
                            };
                            println!("  ⏱ {} ({} >= last {})", display, column, window);
                        }
                    }
                    println!();
                } else {
                    println!("Time filters: none");
                    println!();
                }

                println!("───────────────────────────────────────────────────────────────");
                println!();

                let confirmed = Confirm::new("Proceed with this configuration?")
                    .with_default(true)
                    .with_help_message("Enter confirm, Esc go back")
                    .prompt();

                match confirmed {
                    Ok(true) => break, // Exit loop, proceed with replication
                    Ok(false) | Err(inquire::InquireError::OperationCanceled) => {
                        // Go back to time filters
                        let last_db = selected_db_indices.len().saturating_sub(1);
                        current_step = WizardStep::ConfigureTimeFiltersForDb(last_db);
                    }
                    Err(inquire::InquireError::OperationInterrupted) => {
                        anyhow::bail!("Operation interrupted");
                    }
                    Err(e) => return Err(e.into()),
                }
            }
        }
    }

    // Build final filter from selections
    let selected_databases: Vec<String> = selected_db_indices
        .iter()
        .map(|&i| db_names[i].clone())
        .collect();

    let excluded_tables: Vec<String> = excluded_tables_by_db.values().flatten().cloned().collect();

    tracing::info!("");
    tracing::info!("✓ Configuration confirmed");
    tracing::info!("");

    let filter = if excluded_tables.is_empty() {
        ReplicationFilter::new(Some(selected_databases), None, None, None)?
    } else {
        ReplicationFilter::new(Some(selected_databases), None, None, Some(excluded_tables))?
    };

    // Build TableRules from selections
    let mut table_rules = TableRules::default();

    // Add schema-only tables
    for (db, tables) in &schema_only_by_db {
        for (schema, table) in tables {
            let qualified = QualifiedTable::new(Some(db.clone()), schema.clone(), table.clone());
            table_rules.add_schema_only_table(qualified)?;
        }
    }

    // Add time filters
    for (db, filters) in &time_filters_by_db {
        for (schema, table, column, window) in filters {
            let qualified = QualifiedTable::new(Some(db.clone()), schema.clone(), table.clone());
            table_rules.add_time_filter(qualified, column.clone(), window.clone())?;
        }
    }

    Ok((filter, table_rules))
}

/// Get or cache table info for a database
async fn get_or_cache_tables<'a>(
    cache: &'a mut std::collections::HashMap<String, CachedDbTables>,
    source_url: &str,
    db_name: &str,
) -> Result<&'a CachedDbTables> {
    if !cache.contains_key(db_name) {
        let db_url = replace_database_in_url(source_url, db_name)?;
        let db_client = postgres::connect_with_retry(&db_url)
            .await
            .context(format!("Failed to connect to database '{}'", db_name))?;

        let all_tables = migration::list_tables(&db_client)
            .await
            .context(format!("Failed to list tables from database '{}'", db_name))?;

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

        cache.insert(
            db_name.to_string(),
            CachedDbTables {
                all_tables,
                table_display_names,
            },
        );
    }

    Ok(cache.get(db_name).unwrap())
}

/// Print a formatted header for wizard steps
fn print_header(title: &str) {
    println!();
    println!("╔{}╗", "═".repeat(62));
    println!("║  {:<60}║", title);
    println!("╚{}╝", "═".repeat(62));
    println!();
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
