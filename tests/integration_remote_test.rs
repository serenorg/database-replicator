// ABOUTME: Integration tests for remote execution functionality
// ABOUTME: Tests job submission and status polling with the remote API

use seren_replicator::remote::{JobSpec, RemoteClient};
use std::collections::HashMap;
use std::env;

/// Helper to get remote API URL from environment
/// Set SEREN_REMOTE_API to run these tests
fn get_remote_api_url() -> Option<String> {
    env::var("SEREN_REMOTE_API").ok()
}

/// Helper to get test database URLs from environment
fn get_test_urls() -> Option<(String, String)> {
    let source = env::var("TEST_SOURCE_URL").ok()?;
    let target = env::var("TEST_TARGET_URL").ok()?;
    Some((source, target))
}

#[tokio::test]
#[ignore]
async fn test_remote_job_submission() {
    let api_url =
        get_remote_api_url().expect("SEREN_REMOTE_API must be set for remote execution tests");
    let (source_url, target_url) =
        get_test_urls().expect("TEST_SOURCE_URL and TEST_TARGET_URL must be set");

    println!("Testing remote job submission...");
    println!("API URL: {}", api_url);

    // Create remote client
    let api_key = std::env::var("SEREN_API_KEY").ok();
    let client = RemoteClient::new(api_url, api_key).expect("Failed to create remote client");

    // Create a job spec for validation (safe, read-only)
    let job_spec = JobSpec {
        version: "1.0".to_string(),
        command: "validate".to_string(),
        source_url,
        target_url,
        filter: None,
        options: HashMap::new(),
    };

    // Submit the job
    let result = client.submit_job(&job_spec).await;

    match &result {
        Ok(job_response) => {
            println!("✓ Job submitted successfully");
            println!("  Job ID: {}", job_response.job_id);
            println!("  Status: {}", job_response.status);

            // Verify response structure
            assert!(
                !job_response.job_id.is_empty(),
                "Job ID should not be empty"
            );
            assert!(
                !job_response.status.is_empty(),
                "Status should not be empty"
            );
        }
        Err(e) => {
            println!("✗ Job submission failed: {:?}", e);
            panic!("Job submission should succeed: {:?}", e);
        }
    }
}

#[tokio::test]
#[ignore]
async fn test_remote_job_polling() {
    let api_url =
        get_remote_api_url().expect("SEREN_REMOTE_API must be set for remote execution tests");
    let (source_url, target_url) =
        get_test_urls().expect("TEST_SOURCE_URL and TEST_TARGET_URL must be set");

    println!("Testing remote job polling...");
    println!("API URL: {}", api_url);

    // Create remote client
    let api_key = std::env::var("SEREN_API_KEY").ok();
    let client = RemoteClient::new(api_url, api_key).expect("Failed to create remote client");

    // Create and submit a job spec for validation (safe, read-only)
    let job_spec = JobSpec {
        version: "1.0".to_string(),
        command: "validate".to_string(),
        source_url,
        target_url,
        filter: None,
        options: HashMap::new(),
    };

    // Submit the job
    let job_response = client
        .submit_job(&job_spec)
        .await
        .expect("Failed to submit job");

    println!("✓ Job submitted");
    println!("  Job ID: {}", job_response.job_id);

    // Poll for initial status
    println!("Polling for job status...");
    let status = client
        .get_job_status(&job_response.job_id)
        .await
        .expect("Failed to get job status");

    println!("✓ Status retrieved");
    println!("  Status: {}", status.status);
    println!("  Job ID: {}", status.job_id);

    // Verify status structure
    assert_eq!(status.job_id, job_response.job_id, "Job ID should match");
    assert!(!status.status.is_empty(), "Status should not be empty");

    // Valid status values
    let valid_statuses = ["provisioning", "running", "completed", "failed"];
    assert!(
        valid_statuses.contains(&status.status.as_str()),
        "Status should be one of: {:?}, got: {}",
        valid_statuses,
        status.status
    );

    // If job is still running, poll a few more times
    if status.status == "provisioning" || status.status == "running" {
        println!("Job is {}. Polling a few more times...", status.status);

        for i in 1..=3 {
            tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;

            let updated_status = client
                .get_job_status(&job_response.job_id)
                .await
                .expect("Failed to get job status");

            println!("  Poll {}: Status = {}", i, updated_status.status);

            // Check if job completed
            if updated_status.status == "completed" {
                println!("✓ Job completed successfully");
                break;
            } else if updated_status.status == "failed" {
                println!("✗ Job failed");
                if let Some(error) = &updated_status.error {
                    println!("  Error: {}", error);
                }
                break;
            }
        }
    } else if status.status == "completed" {
        println!("✓ Job completed successfully");
    } else if status.status == "failed" {
        println!("✗ Job failed");
        if let Some(error) = &status.error {
            println!("  Error: {}", error);
        }
    }

    println!("✓ Job polling test completed");
}

#[tokio::test]
#[ignore]
async fn test_remote_job_poll_until_complete() {
    let api_url =
        get_remote_api_url().expect("SEREN_REMOTE_API must be set for remote execution tests");
    let (source_url, target_url) =
        get_test_urls().expect("TEST_SOURCE_URL and TEST_TARGET_URL must be set");

    println!("Testing remote job poll_until_complete...");
    println!("API URL: {}", api_url);

    // Create remote client
    let api_key = std::env::var("SEREN_API_KEY").ok();
    let client = RemoteClient::new(api_url, api_key).expect("Failed to create remote client");

    // Create and submit a job spec for validation (safe, read-only)
    let job_spec = JobSpec {
        version: "1.0".to_string(),
        command: "validate".to_string(),
        source_url,
        target_url,
        filter: None,
        options: HashMap::new(),
    };

    // Submit the job
    let job_response = client
        .submit_job(&job_spec)
        .await
        .expect("Failed to submit job");

    println!("✓ Job submitted");
    println!("  Job ID: {}", job_response.job_id);
    println!("Polling until complete...");

    // Poll until completion with a callback
    let final_status = client
        .poll_until_complete(&job_response.job_id, |status| {
            println!("  Status update: {}", status.status);
            if let Some(progress) = &status.progress {
                println!(
                    "    Progress: {}/{}",
                    progress.databases_completed, progress.databases_total
                );
                if let Some(db) = &progress.current_database {
                    println!("    Current database: {}", db);
                }
            }
        })
        .await
        .expect("Failed to poll job");

    println!("✓ Job completed polling");
    println!("  Final status: {}", final_status.status);

    // Verify final status
    assert!(
        final_status.status == "completed" || final_status.status == "failed",
        "Final status should be completed or failed, got: {}",
        final_status.status
    );

    if final_status.status == "failed" {
        if let Some(error) = &final_status.error {
            println!("  Error: {}", error);
        }
    }

    println!("✓ Poll until complete test finished");
}

#[tokio::test]
#[ignore]
async fn test_remote_client_creation() {
    println!("Testing remote client creation...");

    let api_url = "https://79cfok1msj.execute-api.us-east-1.amazonaws.com".to_string();
    let api_key = std::env::var("SEREN_API_KEY").ok();
    let result = RemoteClient::new(api_url, api_key);

    assert!(result.is_ok(), "Client creation should succeed");
    println!("✓ Remote client created successfully");
}

#[tokio::test]
#[ignore]
async fn test_remote_job_submission_with_filters() {
    let api_url =
        get_remote_api_url().expect("SEREN_REMOTE_API must be set for remote execution tests");
    let (source_url, target_url) =
        get_test_urls().expect("TEST_SOURCE_URL and TEST_TARGET_URL must be set");

    println!("Testing remote job submission with filters...");

    // Create remote client
    let api_key = std::env::var("SEREN_API_KEY").ok();
    let client = RemoteClient::new(api_url, api_key).expect("Failed to create remote client");

    // Create a job spec with database filters
    let filter = seren_replicator::remote::FilterSpec {
        include_databases: Some(vec!["postgres".to_string()]),
        exclude_tables: None,
    };

    let job_spec = JobSpec {
        version: "1.0".to_string(),
        command: "validate".to_string(),
        source_url,
        target_url,
        filter: Some(filter),
        options: HashMap::new(),
    };

    // Submit the job
    let result = client.submit_job(&job_spec).await;

    match &result {
        Ok(job_response) => {
            println!("✓ Job with filters submitted successfully");
            println!("  Job ID: {}", job_response.job_id);
            assert!(!job_response.job_id.is_empty());
        }
        Err(e) => {
            println!("✗ Job submission with filters failed: {:?}", e);
            panic!("Job submission should succeed: {:?}", e);
        }
    }
}

#[tokio::test]
#[ignore]
async fn test_remote_job_submission_with_options() {
    let api_url =
        get_remote_api_url().expect("SEREN_REMOTE_API must be set for remote execution tests");
    let (source_url, target_url) =
        get_test_urls().expect("TEST_SOURCE_URL and TEST_TARGET_URL must be set");

    println!("Testing remote job submission with options...");

    // Create remote client
    let api_key = std::env::var("SEREN_API_KEY").ok();
    let client = RemoteClient::new(api_url, api_key).expect("Failed to create remote client");

    // Create a job spec with options
    let mut options = HashMap::new();
    options.insert("drop_existing".to_string(), serde_json::Value::Bool(false));
    options.insert("enable_sync".to_string(), serde_json::Value::Bool(true));
    options.insert(
        "estimated_size_bytes".to_string(),
        serde_json::Value::Number(serde_json::Number::from(1073741824)), // 1GB
    );

    let job_spec = JobSpec {
        version: "1.0".to_string(),
        command: "validate".to_string(),
        source_url,
        target_url,
        filter: None,
        options,
    };

    // Submit the job
    let result = client.submit_job(&job_spec).await;

    match &result {
        Ok(job_response) => {
            println!("✓ Job with options submitted successfully");
            println!("  Job ID: {}", job_response.job_id);
            assert!(!job_response.job_id.is_empty());
        }
        Err(e) => {
            println!("✗ Job submission with options failed: {:?}", e);
            panic!("Job submission should succeed: {:?}", e);
        }
    }
}
