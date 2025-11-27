// ABOUTME: Interactive terminal UI for database and table selection
// ABOUTME: Provides multi-step wizard with back navigation using inquire crate

use crate::{filters::ReplicationFilter, migration, postgres, table_rules::TableRules};
use anyhow::{Context, Result};
use inquire::{Confirm, MultiSelect};

/// Wizard step state machine
enum WizardStep {
    SelectDatabases,
    SelectTablesForDb(usize), // index of current database in selected_dbs
    Review,
}

/// Interactive database and table selection with back navigation
///
/// Presents a terminal UI for selecting:
/// 1. Which databases to replicate (multi-select)
/// 2. For each selected database: tables to exclude
/// 3. Summary and confirmation
///
/// Supports back navigation:
/// - Cancel/Esc from table selection → go back to database selection
/// - Cancel/Esc from review → go back to last database's table selection
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
    let mut excluded_tables: Vec<String> = Vec::new();
    let mut current_step = WizardStep::SelectDatabases;
    // Track excluded tables per database for back navigation
    let mut excluded_tables_by_db: std::collections::HashMap<String, Vec<String>> =
        std::collections::HashMap::new();

    loop {
        match current_step {
            WizardStep::SelectDatabases => {
                print_header("Step 1 of 3: Select Databases");
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

                        // Clear previous exclusions when re-selecting databases
                        excluded_tables.clear();
                        excluded_tables_by_db.clear();

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
                let db_name = &db_names[selected_db_indices[db_idx]];
                print_header(&format!(
                    "Step 2 of 3: Select Tables to Exclude ({}/{})",
                    db_idx + 1,
                    selected_db_indices.len()
                ));
                println!("Database: {}", db_name);
                println!("Navigation: Space to toggle, Enter to continue, Esc to go back");
                println!();

                // Get tables for this database
                let db_url = replace_database_in_url(source_url, db_name)?;
                let db_client = postgres::connect_with_retry(&db_url)
                    .await
                    .context(format!("Failed to connect to database '{}'", db_name))?;

                let all_tables = migration::list_tables(&db_client)
                    .await
                    .context(format!("Failed to list tables from database '{}'", db_name))?;

                if all_tables.is_empty() {
                    println!("  No tables found in database '{}'", db_name);
                    // Skip to next database or review
                    if db_idx + 1 < selected_db_indices.len() {
                        current_step = WizardStep::SelectTablesForDb(db_idx + 1);
                    } else {
                        current_step = WizardStep::Review;
                    }
                    continue;
                }

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
                                table_display_names.iter().position(|n| n == stripped)
                            })
                            .collect()
                    })
                    .unwrap_or_default();

                let selections = MultiSelect::new(
                    "Select tables to EXCLUDE (or press Enter to include all):",
                    table_display_names.clone(),
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
                        excluded_tables_by_db.insert(db_name.clone(), db_exclusions.clone());

                        // Move to next database or review
                        if db_idx + 1 < selected_db_indices.len() {
                            current_step = WizardStep::SelectTablesForDb(db_idx + 1);
                        } else {
                            current_step = WizardStep::Review;
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

            WizardStep::Review => {
                print_header("Step 3 of 3: Review Configuration");

                // Collect all exclusions
                excluded_tables = excluded_tables_by_db.values().flatten().cloned().collect();

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
                    println!("Tables to exclude: none (all tables will be replicated)");
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
                        // Go back to last database's table selection
                        let last_db = selected_db_indices.len().saturating_sub(1);
                        current_step = WizardStep::SelectTablesForDb(last_db);
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

    tracing::info!("");
    tracing::info!("✓ Configuration confirmed");
    tracing::info!("");

    let filter = if excluded_tables.is_empty() {
        ReplicationFilter::new(Some(selected_databases), None, None, None)?
    } else {
        ReplicationFilter::new(Some(selected_databases), None, None, Some(excluded_tables))?
    };

    Ok((filter, TableRules::default()))
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
