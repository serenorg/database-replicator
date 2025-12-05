// ABOUTME: Integration tests for SerenDB Console API
// ABOUTME: Tests require SEREN_API_KEY and optionally TEST_SERENDB_PROJECT_ID

//! Integration tests for SerenDB Console API
//!
//! These tests require:
//! - SEREN_API_KEY environment variable
//! - TEST_SERENDB_PROJECT_ID environment variable (for project-specific tests)
//!
//! Run with: cargo test --test serendb_api_test -- --ignored --nocapture

use database_replicator::serendb::ConsoleClient;

fn get_test_client() -> Option<ConsoleClient> {
    let api_key = std::env::var("SEREN_API_KEY").ok()?;
    Some(ConsoleClient::new(None, api_key))
}

fn get_test_project_id() -> Option<String> {
    std::env::var("TEST_SERENDB_PROJECT_ID").ok()
}

#[tokio::test]
#[ignore]
async fn test_list_projects() {
    let client = get_test_client().expect("SEREN_API_KEY required");

    let projects = client.list_projects().await.unwrap();

    assert!(!projects.is_empty(), "Should have at least one project");
    println!("Found {} projects:", projects.len());
    for project in &projects {
        println!(
            "  - {} (id: {}, logical_replication: {})",
            project.name, project.id, project.enable_logical_replication
        );
    }
}

#[tokio::test]
#[ignore]
async fn test_get_project() {
    let client = get_test_client().expect("SEREN_API_KEY required");
    let project_id = get_test_project_id().expect("TEST_SERENDB_PROJECT_ID required");

    let project = client.get_project(&project_id).await.unwrap();

    assert_eq!(project.id, project_id);
    println!("Project: {} ({})", project.name, project.id);
    println!(
        "  Logical replication: {}",
        project.enable_logical_replication
    );
}

#[tokio::test]
#[ignore]
async fn test_list_branches() {
    let client = get_test_client().expect("SEREN_API_KEY required");
    let project_id = get_test_project_id().expect("TEST_SERENDB_PROJECT_ID required");

    let branches = client.list_branches(&project_id).await.unwrap();

    assert!(!branches.is_empty(), "Should have at least one branch");
    println!("Found {} branches:", branches.len());
    for branch in &branches {
        let default_marker = if branch.is_default { " (default)" } else { "" };
        println!("  - {}{} (id: {})", branch.name, default_marker, branch.id);
    }
}

#[tokio::test]
#[ignore]
async fn test_get_default_branch() {
    let client = get_test_client().expect("SEREN_API_KEY required");
    let project_id = get_test_project_id().expect("TEST_SERENDB_PROJECT_ID required");

    let branch = client.get_default_branch(&project_id).await.unwrap();

    println!("Default branch: {} (id: {})", branch.name, branch.id);
    assert!(!branch.id.is_empty());
    assert!(!branch.name.is_empty());
}

#[tokio::test]
#[ignore]
async fn test_list_databases() {
    let client = get_test_client().expect("SEREN_API_KEY required");
    let project_id = get_test_project_id().expect("TEST_SERENDB_PROJECT_ID required");

    let branch = client.get_default_branch(&project_id).await.unwrap();
    let databases = client
        .list_databases(&project_id, &branch.id)
        .await
        .unwrap();

    println!(
        "Found {} databases in branch {}:",
        databases.len(),
        branch.name
    );
    for db in &databases {
        println!("  - {} (id: {})", db.name, db.id);
    }
}

#[tokio::test]
#[ignore]
async fn test_get_connection_string() {
    let client = get_test_client().expect("SEREN_API_KEY required");
    let project_id = get_test_project_id().expect("TEST_SERENDB_PROJECT_ID required");

    let branch = client.get_default_branch(&project_id).await.unwrap();
    let conn_str = client
        .get_connection_string(&project_id, &branch.id, "serendb", false)
        .await
        .unwrap();

    assert!(
        conn_str.starts_with("postgresql://"),
        "Should be a PostgreSQL connection string"
    );
    assert!(
        conn_str.contains("serendb.com") || conn_str.contains("localhost"),
        "Should contain SerenDB hostname"
    );
    println!("Connection string retrieved successfully (credentials redacted)");
}

#[tokio::test]
#[ignore]
async fn test_is_logical_replication_enabled() {
    let client = get_test_client().expect("SEREN_API_KEY required");
    let project_id = get_test_project_id().expect("TEST_SERENDB_PROJECT_ID required");

    let enabled = client
        .is_logical_replication_enabled(&project_id)
        .await
        .unwrap();

    println!("Logical replication enabled: {}", enabled);
}

#[tokio::test]
#[ignore]
async fn test_invalid_api_key_returns_error() {
    let client = ConsoleClient::new(None, "invalid_key".to_string());

    let result = client.list_projects().await;

    assert!(result.is_err(), "Should fail with invalid API key");
    let error = result.unwrap_err().to_string();
    assert!(
        error.contains("invalid") || error.contains("expired") || error.contains("401"),
        "Error should indicate authentication failure: {}",
        error
    );
    println!("Correctly rejected invalid API key");
}

#[tokio::test]
#[ignore]
async fn test_nonexistent_project_returns_error() {
    let client = get_test_client().expect("SEREN_API_KEY required");

    let result = client
        .get_project("00000000-0000-0000-0000-000000000000")
        .await;

    assert!(result.is_err(), "Should fail for nonexistent project");
    let error = result.unwrap_err().to_string();
    assert!(
        error.contains("not found") || error.contains("404"),
        "Error should indicate not found: {}",
        error
    );
    println!("Correctly returned not found for nonexistent project");
}
