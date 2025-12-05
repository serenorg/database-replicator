// ABOUTME: Interactive terminal UI for selecting SerenDB projects and databases
// ABOUTME: Uses dialoguer for consistent UX with existing interactive flows

use crate::serendb::{Branch, ConsoleClient, Project};
use anyhow::{Context, Result};
use dialoguer::{theme::ColorfulTheme, Select};

/// Result of the interactive project/database selection
#[derive(Debug, Clone)]
pub struct TargetSelection {
    pub project: Project,
    pub branch: Branch,
    pub databases: Vec<String>,
}

/// Run interactive SerenDB target selection.
/// Returns the selected project, branch, and database names to mirror the source.
pub async fn select_target(
    client: &ConsoleClient,
    source_databases: &[String],
) -> Result<TargetSelection> {
    println!("\n==================================================");
    println!("SerenDB Target Selection");
    println!("==================================================\n");

    let projects = client.list_projects().await?;

    if projects.is_empty() {
        anyhow::bail!(
            "No SerenDB projects found for this API key.\n\
             Create a project at: https://console.serendb.com"
        );
    }

    let project_labels: Vec<String> = projects
        .iter()
        .map(|p| {
            let short_id: String = p.id.chars().take(8).collect();
            format!("{} ({})", p.name, short_id)
        })
        .collect();

    let project_idx = Select::with_theme(&ColorfulTheme::default())
        .with_prompt("Select target project")
        .items(&project_labels)
        .default(0)
        .interact()
        .context("Project selection cancelled")?;

    let project = projects[project_idx].clone();
    println!("  Selected project: {}\n", project.name);

    let branch = client.get_default_branch(&project.id).await?;
    println!("  Using branch: {}\n", branch.name);

    let existing = client.list_databases(&project.id, &branch.id).await?;
    let existing_names: Vec<String> = existing.iter().map(|d| d.name.clone()).collect();

    println!("Source databases to replicate: {:?}", source_databases);
    println!("Existing target databases: {:?}\n", existing_names);

    let mut target_databases = Vec::new();
    for source_db in source_databases {
        if existing_names.contains(source_db) {
            println!("  \u{2713} {}", source_db);
        } else {
            println!("  + {} (will be created)", source_db);
        }
        target_databases.push(source_db.clone());
    }

    println!();

    Ok(TargetSelection {
        project,
        branch,
        databases: target_databases,
    })
}

/// Ensure target branch contains all databases required for replication.
pub async fn create_missing_databases(
    client: &ConsoleClient,
    project_id: &str,
    branch_id: &str,
    databases: &[String],
) -> Result<()> {
    let existing = client.list_databases(project_id, branch_id).await?;
    let existing_names: Vec<String> = existing.iter().map(|d| d.name.clone()).collect();

    for db_name in databases {
        if !existing_names.contains(db_name) {
            println!("  Creating database '{}'...", db_name);
            client
                .create_database(project_id, branch_id, db_name)
                .await?;
            println!("  \u{2713} Created '{}'", db_name);
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    // Interactive picker relies on network + terminal input, so unit tests are not practical here.
}
