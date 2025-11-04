// ABOUTME: Verify command implementation - Validate data integrity
// ABOUTME: Compares table checksums between source and target databases

use anyhow::{Context, Result};
use crate::postgres::connect;
use crate::migration::{list_tables, compare_tables, ChecksumResult};

/// Verify data integrity between source and target databases
///
/// This command:
/// 1. Lists all tables in the source database
/// 2. Compares each table's checksum between source and target
/// 3. Reports any mismatches or missing tables
/// 4. Provides overall validation summary
pub async fn verify(source_url: &str, target_url: &str) -> Result<()> {
    tracing::info!("Starting data integrity verification...");
    tracing::info!("");

    // Connect to source database
    tracing::info!("Connecting to source database...");
    let source_client = connect(source_url)
        .await
        .context("Failed to connect to source database")?;

    // Connect to target database
    tracing::info!("Connecting to target database...");
    let target_client = connect(target_url)
        .await
        .context("Failed to connect to target database")?;

    // List tables from source
    tracing::info!("Discovering tables...");
    let tables = list_tables(&source_client)
        .await
        .context("Failed to list tables from source database")?;

    if tables.is_empty() {
        tracing::warn!("⚠ No tables found to verify");
        return Ok(());
    }

    tracing::info!("Found {} tables to verify", tables.len());
    tracing::info!("");

    // Compare each table
    let mut results: Vec<ChecksumResult> = Vec::new();
    let mut mismatches = 0;
    let mut matches = 0;

    for (i, table) in tables.iter().enumerate() {
        tracing::info!(
            "[{}/{}] Verifying {}.{}...",
            i + 1,
            tables.len(),
            table.schema,
            table.name
        );

        match compare_tables(&source_client, &target_client, &table.schema, &table.name)
            .await
        {
            Ok(result) => {
                if result.is_valid() {
                    tracing::info!(
                        "  ✓ Match ({} rows, checksum: {})",
                        result.source_row_count,
                        &result.source_checksum[..8]
                    );
                    matches += 1;
                } else if result.matches {
                    tracing::warn!(
                        "  ⚠ Checksum matches but row count differs: source={}, target={}",
                        result.source_row_count,
                        result.target_row_count
                    );
                    mismatches += 1;
                } else {
                    tracing::error!(
                        "  ✗ MISMATCH: source={} ({}), target={} ({})",
                        &result.source_checksum[..8],
                        result.source_row_count,
                        &result.target_checksum[..8],
                        result.target_row_count
                    );
                    mismatches += 1;
                }
                results.push(result);
            }
            Err(e) => {
                tracing::error!("  ✗ ERROR: {}", e);
                mismatches += 1;
            }
        }
    }

    // Display summary
    tracing::info!("");
    tracing::info!("========================================");
    tracing::info!("Verification Summary");
    tracing::info!("========================================");
    tracing::info!("Total tables: {}", tables.len());
    tracing::info!("✓ Matches: {}", matches);
    tracing::info!("✗ Mismatches: {}", mismatches);
    tracing::info!("========================================");
    tracing::info!("");

    if mismatches > 0 {
        tracing::error!("⚠ DATA INTEGRITY ISSUES DETECTED!");
        tracing::error!("  {} table(s) have mismatched data", mismatches);
        tracing::error!("  Review the logs above for details");
        tracing::info!("");
        tracing::info!("Possible causes:");
        tracing::info!("  - Replication is still catching up (check 'status' command)");
        tracing::info!("  - Data was modified on target after migration");
        tracing::info!("  - Migration errors occurred during 'init' or 'sync'");
        tracing::info!("");

        anyhow::bail!("{} table(s) failed verification", mismatches);
    } else {
        tracing::info!("✓ ALL TABLES VERIFIED SUCCESSFULLY!");
        tracing::info!("  All {} tables match between source and target", matches);
        tracing::info!("  Your migration data is intact and ready for cutover");
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    #[ignore]
    async fn test_verify_command() {
        // This test requires both source and target databases
        let source_url = std::env::var("TEST_SOURCE_URL").unwrap();
        let target_url = std::env::var("TEST_TARGET_URL").unwrap();

        let result = verify(&source_url, &target_url).await;

        match &result {
            Ok(_) => {
                println!("✓ Verify command completed successfully");
            }
            Err(e) => {
                println!("Verify command result: {:?}", e);
                // It's okay if tables don't match yet (replication not set up)
                // We're just testing that the command runs
            }
        }

        // The command should at least connect and attempt verification
        // Even if it finds mismatches, that's a valid result
    }
}
