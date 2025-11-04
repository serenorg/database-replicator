// ABOUTME: Pre-flight validation command for migration readiness
// ABOUTME: Checks connectivity, privileges, and version compatibility

use crate::{postgres, utils};
use anyhow::{bail, Context, Result};

pub async fn validate(source_url: &str, target_url: &str) -> Result<()> {
    tracing::info!("Starting validation...");

    // Step 0: Validate connection strings
    tracing::info!("Validating connection strings...");
    utils::validate_connection_string(source_url).context("Invalid source connection string")?;
    utils::validate_connection_string(target_url).context("Invalid target connection string")?;
    tracing::info!("✓ Connection strings are valid");

    // Step 1: Connect to source
    tracing::info!("Connecting to source database...");
    let source_client = postgres::connect(source_url)
        .await
        .context("Failed to connect to source database")?;
    tracing::info!("✓ Connected to source");

    // Step 2: Connect to target
    tracing::info!("Connecting to target database...");
    let target_client = postgres::connect(target_url)
        .await
        .context("Failed to connect to target database")?;
    tracing::info!("✓ Connected to target");

    // Step 3: Check source privileges
    tracing::info!("Checking source privileges...");
    let source_privs = postgres::check_source_privileges(&source_client).await?;
    if !source_privs.has_replication && !source_privs.is_superuser {
        bail!("Source user lacks REPLICATION privilege. Grant with: ALTER USER <user> WITH REPLICATION;");
    }
    tracing::info!("✓ Source has replication privileges");

    // Step 4: Check target privileges
    tracing::info!("Checking target privileges...");
    let target_privs = postgres::check_target_privileges(&target_client).await?;
    if !target_privs.has_create_db && !target_privs.is_superuser {
        bail!(
            "Target user lacks CREATE DATABASE privilege. Grant with: ALTER USER <user> CREATEDB;"
        );
    }
    if !target_privs.has_create_role && !target_privs.is_superuser {
        tracing::warn!("⚠ Target user lacks CREATE ROLE privilege. Role migration may fail.");
    }
    tracing::info!("✓ Target has sufficient privileges");

    // Step 5: Check PostgreSQL versions
    tracing::info!("Checking PostgreSQL versions...");
    let source_version = get_pg_version(&source_client).await?;
    let target_version = get_pg_version(&target_client).await?;

    if source_version.major != target_version.major {
        bail!(
            "PostgreSQL major version mismatch: source={}.{}, target={}.{}. Logical replication requires same major version.",
            source_version.major, source_version.minor,
            target_version.major, target_version.minor
        );
    }
    tracing::info!(
        "✓ Version compatibility confirmed (both {}.{})",
        source_version.major,
        source_version.minor
    );

    tracing::info!("✅ Validation complete - ready for migration");
    Ok(())
}

struct PgVersion {
    major: u32,
    minor: u32,
}

async fn get_pg_version(client: &tokio_postgres::Client) -> Result<PgVersion> {
    let row = client
        .query_one("SHOW server_version", &[])
        .await
        .context("Failed to get PostgreSQL version")?;

    let version_str: String = row.get(0);

    // Parse version string like "16.2 (Debian 16.2-1.pgdg120+1)"
    let parts: Vec<&str> = version_str
        .split_whitespace()
        .next()
        .unwrap_or("0.0")
        .split('.')
        .collect();

    let major = parts.first().and_then(|s| s.parse().ok()).unwrap_or(0);
    let minor = parts.get(1).and_then(|s| s.parse().ok()).unwrap_or(0);

    Ok(PgVersion { major, minor })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    #[ignore]
    async fn test_validate_with_valid_databases_succeeds() {
        let source = std::env::var("TEST_SOURCE_URL").unwrap();
        let target = std::env::var("TEST_TARGET_URL").unwrap();

        let result = validate(&source, &target).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_validate_with_invalid_source_fails() {
        let result = validate("invalid-url", "postgresql://localhost/db").await;
        assert!(result.is_err());
    }
}
