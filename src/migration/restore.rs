// ABOUTME: Wrapper for psql and pg_restore to import database objects
// ABOUTME: Restores global objects, schema, and data to target

use anyhow::{Context, Result};
use std::process::{Command, Stdio};
use std::time::Duration;
use tokio::process::Command as TokioCommand;

/// Restore global objects using psql
pub async fn restore_globals(target_url: &str, input_path: &str) -> Result<()> {
    tracing::info!("Restoring global objects from {}", input_path);

    // Parse URL and create .pgpass file for secure authentication
    let parts = crate::utils::parse_postgres_url(target_url)
        .with_context(|| format!("Failed to parse target URL: {}", target_url))?;
    let pgpass = crate::utils::PgPassFile::new(&parts)
        .context("Failed to create .pgpass file for authentication")?;

    let env_vars = parts.to_pg_env_vars();

    let mut cmd = TokioCommand::new("psql");
    cmd.arg("--host")
        .arg(&parts.host)
        .arg("--port")
        .arg(parts.port.to_string())
        .arg("--dbname")
        .arg(&parts.database)
        .arg(format!("--file={}", input_path))
        .arg("--quiet")
        .arg("-v")
        .arg("ON_ERROR_STOP=1") // Stop on first error for better visibility
        .env("PGPASSFILE", pgpass.path())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());

    // Add username if specified
    if let Some(user) = &parts.user {
        cmd.arg("--username").arg(user);
    }

    // Apply query parameters as environment variables (SSL, channel_binding, etc.)
    for (env_var, value) in &env_vars {
        cmd.env(env_var, value);
    }

    // Apply TCP keepalive parameters to prevent idle connection timeouts
    for (env_var, value) in crate::utils::get_keepalive_env_vars() {
        cmd.env(env_var, value);
    }

    // Mitigate hangs on serverless DBs with strict connection limits
    cmd.env("PGCONNECT_TIMEOUT", "30");

    let output = cmd.output().await.context(
        "Failed to execute psql. Is PostgreSQL client installed?\n\
         Install with:\n\
         - Ubuntu/Debian: sudo apt-get install postgresql-client\n\
         - macOS: brew install postgresql\n\
         - RHEL/CentOS: sudo yum install postgresql",
    )?;

    if output.status.success() {
        tracing::info!("✓ Global objects restored");
        // Even on success, there might be NOTICES or WARNINGS on stderr
        let stderr = String::from_utf8_lossy(&output.stderr);
        if !stderr.trim().is_empty() {
            tracing::debug!("psql stderr output:\n{}", stderr);
        }
        Ok(())
    } else {
        let stderr = String::from_utf8_lossy(&output.stderr);
        let stdout = String::from_utf8_lossy(&output.stdout);

        // Check for common, non-fatal warnings
        if stderr.contains("already exists") && stderr.contains("skipping") {
            tracing::warn!(
                "⚠ Some global objects already exist, which is usually safe. Full output:\n{}",
                stderr
            );
            Ok(()) // Treat as success
        } else {
            // A real error occurred
            anyhow::bail!(
                "Failed to restore global objects.\n\
                 Exit Code: {}\n\
                 Stderr:\n{}\n\
                 Stdout:\n{}",
                output.status,
                stderr,
                stdout
            );
        }
    }
}

/// Restore schema using psql
pub async fn restore_schema(target_url: &str, input_path: &str) -> Result<()> {
    tracing::info!("Restoring schema from {}", input_path);

    // Parse URL and create .pgpass file for secure authentication
    let parts = crate::utils::parse_postgres_url(target_url)
        .with_context(|| format!("Failed to parse target URL: {}", target_url))?;
    let pgpass = crate::utils::PgPassFile::new(&parts)
        .context("Failed to create .pgpass file for authentication")?;

    let env_vars = parts.to_pg_env_vars();
    let input_path_owned = input_path.to_string();

    // Wrap subprocess execution with retry logic
    crate::utils::retry_subprocess_with_backoff(
        || {
            let mut cmd = Command::new("psql");
            cmd.arg("--host")
                .arg(&parts.host)
                .arg("--port")
                .arg(parts.port.to_string())
                .arg("--dbname")
                .arg(&parts.database)
                .arg(format!("--file={}", input_path_owned))
                .arg("--quiet")
                .arg("-v")
                .arg("ON_ERROR_STOP=1") // Stop on first error for better visibility
                .env("PGPASSFILE", pgpass.path())
                .stdout(Stdio::inherit())
                .stderr(Stdio::inherit());

            // Add username if specified
            if let Some(user) = &parts.user {
                cmd.arg("--username").arg(user);
            }

            // Apply query parameters as environment variables (SSL, channel_binding, etc.)
            for (env_var, value) in &env_vars {
                cmd.env(env_var, value);
            }

            // Apply TCP keepalive parameters to prevent idle connection timeouts
            for (env_var, value) in crate::utils::get_keepalive_env_vars() {
                cmd.env(env_var, value);
            }

            // Mitigate hangs on serverless DBs with strict connection limits
            cmd.env("PGCONNECT_TIMEOUT", "30");

            cmd.status().context(
                "Failed to execute psql. Is PostgreSQL client installed?\n\
                 Install with:\n\
                 - Ubuntu/Debian: sudo apt-get install postgresql-client\n\
                 - macOS: brew install postgresql\n\
                 - RHEL/CentOS: sudo yum install postgresql",
            )
        },
        3,                      // Max 3 retries
        Duration::from_secs(1), // Start with 1 second delay
        "psql (restore schema)",
    )
    .await
    .context(
        "Schema restoration failed.\n\
         \n\
         Common causes:\n\
         - Target database does not exist\n\
         - User lacks CREATE privileges on target\n\
         - Schema objects already exist (try dropping them first)\n\
         - Version incompatibility between source and target\n\
         - Syntax errors in dump file\n\
         - Connection timeout or network issues",
    )?;

    tracing::info!("✓ Schema restored successfully");
    Ok(())
}

/// Restore data using pg_restore with parallel jobs
///
/// Uses PostgreSQL directory format restore with:
/// - Parallel restore for faster performance
/// - Automatic decompression of compressed dump files
/// - Optimized for directory format dumps created by dump_data()
///
/// The number of parallel jobs is automatically determined based on available CPU cores.
pub async fn restore_data(target_url: &str, input_path: &str) -> Result<()> {
    // Determine optimal number of parallel jobs (number of CPUs, capped at 8)
    let num_cpus = std::thread::available_parallelism()
        .map(|n| n.get().min(8))
        .unwrap_or(4);

    tracing::info!(
        "Restoring data from {} (parallel={}, format=directory)",
        input_path,
        num_cpus
    );

    // Parse URL and create .pgpass file for secure authentication
    let parts = crate::utils::parse_postgres_url(target_url)
        .with_context(|| format!("Failed to parse target URL: {}", target_url))?;
    let pgpass = crate::utils::PgPassFile::new(&parts)
        .context("Failed to create .pgpass file for authentication")?;

    let env_vars = parts.to_pg_env_vars();
    let input_path_owned = input_path.to_string();

    // Wrap subprocess execution with retry logic
    crate::utils::retry_subprocess_with_backoff(
        || {
            let mut cmd = Command::new("pg_restore");
            cmd.arg("--data-only")
                .arg("--no-owner")
                .arg(format!("--jobs={}", num_cpus)) // Parallel restore jobs
                .arg("--host")
                .arg(&parts.host)
                .arg("--port")
                .arg(parts.port.to_string())
                .arg("--dbname")
                .arg(&parts.database)
                .arg("--format=directory") // Directory format
                .arg("--verbose") // Show progress
                .arg(&input_path_owned)
                .env("PGPASSFILE", pgpass.path())
                .stdout(Stdio::inherit())
                .stderr(Stdio::inherit());

            // Add username if specified
            if let Some(user) = &parts.user {
                cmd.arg("--username").arg(user);
            }

            // Apply query parameters as environment variables (SSL, channel_binding, etc.)
            for (env_var, value) in &env_vars {
                cmd.env(env_var, value);
            }

            // Apply TCP keepalive parameters to prevent idle connection timeouts
            for (env_var, value) in crate::utils::get_keepalive_env_vars() {
                cmd.env(env_var, value);
            }

            // Mitigate hangs on serverless DBs with strict connection limits
            cmd.env("PGCONNECT_TIMEOUT", "30");

            cmd.status().context(
                "Failed to execute pg_restore. Is PostgreSQL client installed?\n\
                 Install with:\n\
                 - Ubuntu/Debian: sudo apt-get install postgresql-client\n\
                 - macOS: brew install postgresql\n\
                 - RHEL/CentOS: sudo yum install postgresql",
            )
        },
        3,                      // Max 3 retries
        Duration::from_secs(1), // Start with 1 second delay
        "pg_restore (restore data)",
    )
    .await
    .context(
        "Data restoration failed.\n\
         \n\
         Common causes:\n\
         - Foreign key constraint violations\n\
         - Unique constraint violations (data already exists)\n\
         - User lacks INSERT privileges on target tables\n\
         - Disk space issues on target\n\
         - Data type mismatches\n\
         - Input directory is not a valid pg_dump directory format\n\
         - Connection timeout or network issues",
    )?;

    tracing::info!(
        "✓ Data restored successfully using {} parallel jobs",
        num_cpus
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::migration::dump;
    use tempfile::tempdir;

    #[tokio::test]
    #[ignore]
    async fn test_restore_globals() {
        let source_url = std::env::var("TEST_SOURCE_URL").unwrap();
        let target_url = std::env::var("TEST_TARGET_URL").unwrap();

        let dir = tempdir().unwrap();
        let dump_file = dir.path().join("globals.sql");

        // Dump from source
        dump::dump_globals(&source_url, dump_file.to_str().unwrap())
            .await
            .unwrap();

        // Restore to target
        let result = restore_globals(&target_url, dump_file.to_str().unwrap()).await;
        assert!(result.is_ok());
    }
}
