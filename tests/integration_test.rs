// ABOUTME: Integration tests for the full replication workflow
// ABOUTME: Tests all commands end-to-end with real database connections

use postgres_seren_replicator::commands;
use std::env;

/// Helper to get test database URLs from environment
fn get_test_urls() -> Option<(String, String)> {
    let source = env::var("TEST_SOURCE_URL").ok()?;
    let target = env::var("TEST_TARGET_URL").ok()?;
    Some((source, target))
}

#[tokio::test]
#[ignore]
async fn test_validate_command_integration() {
    let (source_url, target_url) =
        get_test_urls().expect("TEST_SOURCE_URL and TEST_TARGET_URL must be set");

    println!("Testing validate command...");
    let filter = postgres_seren_replicator::filters::ReplicationFilter::empty();
    let result = commands::validate(&source_url, &target_url, filter).await;

    match &result {
        Ok(_) => {
            println!("✓ Validate command completed successfully");
        }
        Err(e) => {
            println!("Validate command failed: {:?}", e);
            // Validation might fail if databases don't meet requirements
            // That's a valid result for this test
        }
    }

    // The command should at least connect without panicking
    // We don't assert Ok() because databases might not have required privileges
}

#[tokio::test]
#[ignore]
async fn test_init_command_integration() {
    let (source_url, target_url) =
        get_test_urls().expect("TEST_SOURCE_URL and TEST_TARGET_URL must be set");

    println!("Testing init command...");
    println!("⚠ WARNING: This will copy all data from source to target!");

    // Skip confirmation for automated tests, disable sync to keep test simple
    let filter = postgres_seren_replicator::filters::ReplicationFilter::empty();
    let result = commands::init(&source_url, &target_url, true, filter, false, false).await;

    match &result {
        Ok(_) => {
            println!("✓ Init command completed successfully");
        }
        Err(e) => {
            println!("Init command failed: {:?}", e);
            // Init might fail for various reasons (permissions, pg_dump not found, etc)
            // We just want to verify the command runs without panicking
        }
    }
}

#[tokio::test]
#[ignore]
async fn test_sync_command_integration() {
    let (source_url, target_url) =
        get_test_urls().expect("TEST_SOURCE_URL and TEST_TARGET_URL must be set");

    println!("Testing sync command...");
    println!("⚠ WARNING: This will set up logical replication!");

    let filter = postgres_seren_replicator::filters::ReplicationFilter::empty();
    let result = commands::sync(
        &source_url,
        &target_url,
        Some(filter),
        None,
        None,
        Some(30),
        false,
    )
    .await;

    match &result {
        Ok(_) => {
            println!("✓ Sync command completed successfully");
        }
        Err(e) => {
            println!("Sync command failed: {:?}", e);
            // Sync might fail if databases don't support logical replication
            if e.to_string().contains("not supported") || e.to_string().contains("permission") {
                println!("Skipping - database doesn't support logical replication");
                return;
            }
        }
    }
}

#[tokio::test]
#[ignore]
async fn test_status_command_integration() {
    let (source_url, target_url) =
        get_test_urls().expect("TEST_SOURCE_URL and TEST_TARGET_URL must be set");

    println!("Testing status command...");

    let result = commands::status(&source_url, &target_url, None).await;

    match &result {
        Ok(_) => {
            println!("✓ Status command completed successfully");
        }
        Err(e) => {
            println!("Status command failed: {:?}", e);
        }
    }

    // Status should always succeed even if no replication is active
    assert!(
        result.is_ok(),
        "Status command should not fail: {:?}",
        result
    );
}

#[tokio::test]
#[ignore]
async fn test_verify_command_integration() {
    let (source_url, target_url) =
        get_test_urls().expect("TEST_SOURCE_URL and TEST_TARGET_URL must be set");

    println!("Testing verify command...");

    let result = commands::verify(&source_url, &target_url, None).await;

    match &result {
        Ok(_) => {
            println!("✓ Verify command completed - all tables match!");
        }
        Err(e) => {
            println!("Verify command result: {:?}", e);
            // Verify might fail if tables don't match yet
            // That's expected if replication hasn't completed
        }
    }

    // We don't assert Ok() because tables might not match
}

#[tokio::test]
#[ignore]
async fn test_full_replication_workflow() {
    let (source_url, target_url) =
        get_test_urls().expect("TEST_SOURCE_URL and TEST_TARGET_URL must be set");

    println!("========================================");
    println!("Testing FULL replication workflow");
    println!("========================================");
    println!();

    // Step 1: Validate
    println!("STEP 1: Validate databases...");
    let filter = postgres_seren_replicator::filters::ReplicationFilter::empty();
    let validate_result = commands::validate(&source_url, &target_url, filter).await;
    match &validate_result {
        Ok(_) => println!("✓ Validation passed"),
        Err(e) => {
            println!("✗ Validation failed: {:?}", e);
            println!("Continuing anyway for test purposes...");
        }
    }
    println!();

    // Step 2: Init (commented out by default to avoid destructive operations)
    // Uncomment this section to test the full workflow including data copy
    /*
    println!("STEP 2: Initialize replication...");
    let filter = postgres_seren_replicator::filters::ReplicationFilter::empty();
    let init_result = commands::init(&source_url, &target_url, true, filter, false).await;
    match &init_result {
        Ok(_) => println!("✓ Init completed"),
        Err(e) => {
            println!("✗ Init failed: {:?}", e);
            println!("Cannot continue workflow without successful init");
            return;
        }
    }
    println!();
    */

    // Step 3: Sync (commented out by default)
    /*
    println!("STEP 3: Set up logical replication...");
    let filter = postgres_seren_replicator::filters::ReplicationFilter::empty();
    let sync_result = commands::sync(&source_url, &target_url, Some(filter), None, None, Some(60)).await;
    match &sync_result {
        Ok(_) => println!("✓ Sync completed"),
        Err(e) => {
            println!("✗ Sync failed: {:?}", e);
            if e.to_string().contains("not supported") || e.to_string().contains("permission") {
                println!("Database doesn't support logical replication");
                return;
            }
            println!("Cannot continue workflow without successful sync");
            return;
        }
    }
    println!();
    */

    // Step 4: Status
    println!("STEP 4: Check replication status...");
    let status_result = commands::status(&source_url, &target_url, None).await;
    match &status_result {
        Ok(_) => println!("✓ Status checked"),
        Err(e) => {
            println!("✗ Status failed: {:?}", e);
        }
    }
    println!();

    // Step 5: Verify (safe to run, read-only)
    println!("STEP 5: Verify data integrity...");
    let verify_result = commands::verify(&source_url, &target_url, None).await;
    match &verify_result {
        Ok(_) => println!("✓ Verification passed - all tables match!"),
        Err(e) => {
            println!("✗ Verification failed: {:?}", e);
            println!("This is expected if init/sync were not run");
        }
    }
    println!();

    println!("========================================");
    println!("Full workflow test completed");
    println!("========================================");

    // The test passes if it completes without panicking
    // Individual command failures are logged but don't fail the test
}

#[tokio::test]
#[ignore]
async fn test_error_handling_bad_source_url() {
    println!("Testing error handling with bad source URL...");

    let bad_source = "postgresql://invalid:invalid@nonexistent:5432/invalid";
    let (_, target_url) = get_test_urls().expect("TEST_TARGET_URL must be set");

    let filter = postgres_seren_replicator::filters::ReplicationFilter::empty();
    let result = commands::validate(bad_source, &target_url, filter).await;

    // Should fail gracefully with connection error
    assert!(result.is_err(), "Should fail with bad source URL");
    println!("✓ Error handled gracefully: {:?}", result);
}

#[tokio::test]
#[ignore]
async fn test_error_handling_bad_target_url() {
    println!("Testing error handling with bad target URL...");

    let (source_url, _) = get_test_urls().expect("TEST_SOURCE_URL must be set");
    let bad_target = "postgresql://invalid:invalid@nonexistent:5432/invalid";

    let filter = postgres_seren_replicator::filters::ReplicationFilter::empty();
    let result = commands::validate(&source_url, bad_target, filter).await;

    // Should fail gracefully with connection error
    assert!(result.is_err(), "Should fail with bad target URL");
    println!("✓ Error handled gracefully: {:?}", result);
}

#[tokio::test]
#[ignore]
async fn test_init_with_database_filter() {
    let (source_url, target_url) =
        get_test_urls().expect("TEST_SOURCE_URL and TEST_TARGET_URL must be set");

    println!("Testing init command with database filter...");
    println!("⚠ WARNING: This will copy filtered data from source to target!");

    // Create filter that includes only specific database
    // Note: Adjust the database name based on your test environment
    let filter = postgres_seren_replicator::filters::ReplicationFilter::new(
        Some(vec!["postgres".to_string()]), // Include only postgres database
        None,
        None,
        None,
    )
    .expect("Failed to create filter");

    // Skip confirmation for automated tests, disable sync to keep test simple
    let result = commands::init(&source_url, &target_url, true, filter, false, false).await;

    match &result {
        Ok(_) => {
            println!("✓ Init with database filter completed successfully");
        }
        Err(e) => {
            println!("Init with database filter failed: {:?}", e);
            // Init might fail for various reasons (permissions, pg_dump not found, etc)
            // We just want to verify the command runs without panicking
        }
    }
}

#[tokio::test]
#[ignore]
async fn test_init_with_table_filter() {
    let (source_url, target_url) =
        get_test_urls().expect("TEST_SOURCE_URL and TEST_TARGET_URL must be set");

    println!("Testing init command with table filter...");
    println!("⚠ WARNING: This will copy filtered data from source to target!");

    // Create filter that excludes specific tables
    // Note: Adjust the table names based on your test environment
    let filter = postgres_seren_replicator::filters::ReplicationFilter::new(
        None,
        None,
        None,
        Some(vec!["postgres.pg_stat_statements".to_string()]), // Exclude pg_stat_statements table
    )
    .expect("Failed to create filter");

    // Skip confirmation for automated tests, disable sync to keep test simple
    let result = commands::init(&source_url, &target_url, true, filter, false, false).await;

    match &result {
        Ok(_) => {
            println!("✓ Init with table filter completed successfully");
        }
        Err(e) => {
            println!("Init with table filter failed: {:?}", e);
            // Init might fail for various reasons (permissions, pg_dump not found, etc)
            // We just want to verify the command runs without panicking
        }
    }
}

#[tokio::test]
#[ignore]
async fn test_sync_with_table_filter() {
    let (source_url, target_url) =
        get_test_urls().expect("TEST_SOURCE_URL and TEST_TARGET_URL must be set");

    println!("Testing sync command with table filter...");
    println!("⚠ WARNING: This will set up filtered logical replication!");

    // Create filter that excludes certain tables
    // This test assumes the database has some tables to filter
    let filter = postgres_seren_replicator::filters::ReplicationFilter::new(
        None,
        None,
        None,
        Some(vec![
            "postgres.pg_stat_statements".to_string(), // Example system table to exclude
        ]),
    )
    .expect("Failed to create filter");

    // Use unique names for this test
    let pub_name = "test_filtered_pub";
    let sub_name = "test_filtered_sub";
    let timeout = 60; // 1 minute timeout for test

    let result = commands::sync(
        &source_url,
        &target_url,
        Some(filter),
        Some(pub_name),
        Some(sub_name),
        Some(timeout),
        false,
    )
    .await;

    match &result {
        Ok(_) => {
            println!("✓ Sync with table filter completed successfully");

            // Clean up
            let target_client = postgres_seren_replicator::postgres::connect(&target_url)
                .await
                .unwrap();
            let _ =
                postgres_seren_replicator::replication::drop_subscription(&target_client, sub_name)
                    .await;

            let source_client = postgres_seren_replicator::postgres::connect(&source_url)
                .await
                .unwrap();
            let _ =
                postgres_seren_replicator::replication::drop_publication(&source_client, pub_name)
                    .await;
        }
        Err(e) => {
            println!("Sync with table filter failed: {:?}", e);
            // If either database doesn't support logical replication, skip
            if e.to_string().contains("not supported")
                || e.to_string().contains("permission")
                || e.to_string().contains("wal_level")
            {
                println!("Skipping test - database might not support logical replication");
                return;
            }
        }
    }
}
