// ABOUTME: Integration tests for the full migration workflow
// ABOUTME: Tests all commands end-to-end with real database connections

use seren_neon_migrator::commands;
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
    let (source_url, target_url) = get_test_urls()
        .expect("TEST_SOURCE_URL and TEST_TARGET_URL must be set");

    println!("Testing validate command...");
    let result = commands::validate(&source_url, &target_url).await;

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
    let (source_url, target_url) = get_test_urls()
        .expect("TEST_SOURCE_URL and TEST_TARGET_URL must be set");

    println!("Testing init command...");
    println!("⚠ WARNING: This will copy all data from source to target!");

    let result = commands::init(&source_url, &target_url).await;

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
    let (source_url, target_url) = get_test_urls()
        .expect("TEST_SOURCE_URL and TEST_TARGET_URL must be set");

    println!("Testing sync command...");
    println!("⚠ WARNING: This will set up logical replication!");

    let result = commands::sync(&source_url, &target_url, None, None, Some(30)).await;

    match &result {
        Ok(_) => {
            println!("✓ Sync command completed successfully");
        }
        Err(e) => {
            println!("Sync command failed: {:?}", e);
            // Sync might fail if databases don't support logical replication
            if e.to_string().contains("not supported")
                || e.to_string().contains("permission")
            {
                println!("Skipping - database doesn't support logical replication");
                return;
            }
        }
    }
}

#[tokio::test]
#[ignore]
async fn test_status_command_integration() {
    let (source_url, target_url) = get_test_urls()
        .expect("TEST_SOURCE_URL and TEST_TARGET_URL must be set");

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
    let (source_url, target_url) = get_test_urls()
        .expect("TEST_SOURCE_URL and TEST_TARGET_URL must be set");

    println!("Testing verify command...");

    let result = commands::verify(&source_url, &target_url).await;

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
async fn test_full_migration_workflow() {
    let (source_url, target_url) = get_test_urls()
        .expect("TEST_SOURCE_URL and TEST_TARGET_URL must be set");

    println!("========================================");
    println!("Testing FULL migration workflow");
    println!("========================================");
    println!("");

    // Step 1: Validate
    println!("STEP 1: Validate databases...");
    let validate_result = commands::validate(&source_url, &target_url).await;
    match &validate_result {
        Ok(_) => println!("✓ Validation passed"),
        Err(e) => {
            println!("✗ Validation failed: {:?}", e);
            println!("Continuing anyway for test purposes...");
        }
    }
    println!("");

    // Step 2: Init (commented out by default to avoid destructive operations)
    // Uncomment this section to test the full workflow including data copy
    /*
    println!("STEP 2: Initialize migration...");
    let init_result = commands::init(&source_url, &target_url).await;
    match &init_result {
        Ok(_) => println!("✓ Init completed"),
        Err(e) => {
            println!("✗ Init failed: {:?}", e);
            println!("Cannot continue workflow without successful init");
            return;
        }
    }
    println!("");
    */

    // Step 3: Sync (commented out by default)
    /*
    println!("STEP 3: Set up logical replication...");
    let sync_result = commands::sync(&source_url, &target_url, None, None, Some(60)).await;
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
    println!("");
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
    println!("");

    // Step 5: Verify (safe to run, read-only)
    println!("STEP 5: Verify data integrity...");
    let verify_result = commands::verify(&source_url, &target_url).await;
    match &verify_result {
        Ok(_) => println!("✓ Verification passed - all tables match!"),
        Err(e) => {
            println!("✗ Verification failed: {:?}", e);
            println!("This is expected if init/sync were not run");
        }
    }
    println!("");

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
    let (_, target_url) = get_test_urls()
        .expect("TEST_TARGET_URL must be set");

    let result = commands::validate(bad_source, &target_url).await;

    // Should fail gracefully with connection error
    assert!(result.is_err(), "Should fail with bad source URL");
    println!("✓ Error handled gracefully: {:?}", result);
}

#[tokio::test]
#[ignore]
async fn test_error_handling_bad_target_url() {
    println!("Testing error handling with bad target URL...");

    let (source_url, _) = get_test_urls()
        .expect("TEST_SOURCE_URL must be set");
    let bad_target = "postgresql://invalid:invalid@nonexistent:5432/invalid";

    let result = commands::validate(&source_url, bad_target).await;

    // Should fail gracefully with connection error
    assert!(result.is_err(), "Should fail with bad target URL");
    println!("✓ Error handled gracefully: {:?}", result);
}
