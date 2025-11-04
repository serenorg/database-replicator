// ABOUTME: Wrapper for pg_dump command to export database objects
// ABOUTME: Handles global objects, schema, and data export

use anyhow::{bail, Context, Result};
use std::process::Command;

/// Dump global objects (roles, tablespaces) using pg_dumpall
pub async fn dump_globals(source_url: &str, output_path: &str) -> Result<()> {
    tracing::info!("Dumping global objects to {}", output_path);

    let output = Command::new("pg_dumpall")
        .arg("--globals-only")
        .arg("--no-role-passwords") // Don't dump passwords
        .arg(format!("--dbname={}", source_url))
        .arg(format!("--file={}", output_path))
        .output()
        .context(
            "Failed to execute pg_dumpall. Is PostgreSQL client installed?\n\
             Install with:\n\
             - Ubuntu/Debian: sudo apt-get install postgresql-client\n\
             - macOS: brew install postgresql\n\
             - RHEL/CentOS: sudo yum install postgresql",
        )?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!(
            "pg_dumpall failed to dump global objects.\n\
             Error: {}\n\
             \n\
             Common causes:\n\
             - Connection authentication failed\n\
             - User lacks sufficient privileges (need SUPERUSER or pg_read_all_settings role)\n\
             - Network connectivity issues\n\
             - Invalid connection string",
            stderr
        );
    }

    tracing::info!("✓ Global objects dumped successfully");
    Ok(())
}

/// Dump schema (DDL) for a specific database
pub async fn dump_schema(source_url: &str, database: &str, output_path: &str) -> Result<()> {
    tracing::info!(
        "Dumping schema for database '{}' to {}",
        database,
        output_path
    );

    let output = Command::new("pg_dump")
        .arg("--schema-only")
        .arg("--no-owner") // Don't include ownership commands
        .arg("--no-privileges") // We'll handle privileges separately
        .arg(format!("--dbname={}", source_url))
        .arg(format!("--file={}", output_path))
        .output()
        .context(
            "Failed to execute pg_dump. Is PostgreSQL client installed?\n\
             Install with:\n\
             - Ubuntu/Debian: sudo apt-get install postgresql-client\n\
             - macOS: brew install postgresql\n\
             - RHEL/CentOS: sudo yum install postgresql",
        )?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!(
            "pg_dump failed to dump schema for database '{}'.\n\
             Error: {}\n\
             \n\
             Common causes:\n\
             - Database does not exist\n\
             - Connection authentication failed\n\
             - User lacks privileges to read database schema\n\
             - Network connectivity issues",
            database,
            stderr
        );
    }

    tracing::info!("✓ Schema dumped successfully");
    Ok(())
}

/// Dump data for a specific database
pub async fn dump_data(source_url: &str, database: &str, output_path: &str) -> Result<()> {
    tracing::info!(
        "Dumping data for database '{}' to {}",
        database,
        output_path
    );

    let output = Command::new("pg_dump")
        .arg("--data-only")
        .arg("--no-owner")
        .arg(format!("--dbname={}", source_url))
        .arg(format!("--file={}", output_path))
        .output()
        .context(
            "Failed to execute pg_dump. Is PostgreSQL client installed?\n\
             Install with:\n\
             - Ubuntu/Debian: sudo apt-get install postgresql-client\n\
             - macOS: brew install postgresql\n\
             - RHEL/CentOS: sudo yum install postgresql",
        )?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!(
            "pg_dump failed to dump data for database '{}'.\n\
             Error: {}\n\
             \n\
             Common causes:\n\
             - Database does not exist\n\
             - Connection authentication failed\n\
             - User lacks privileges to read table data\n\
             - Network connectivity issues\n\
             - Insufficient disk space for dump file",
            database,
            stderr
        );
    }

    tracing::info!("✓ Data dumped successfully");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    #[ignore]
    async fn test_dump_globals() {
        let url = std::env::var("TEST_SOURCE_URL").unwrap();
        let dir = tempdir().unwrap();
        let output = dir.path().join("globals.sql");

        let result = dump_globals(&url, output.to_str().unwrap()).await;

        assert!(result.is_ok());
        assert!(output.exists());

        // Verify file contains SQL
        let content = std::fs::read_to_string(&output).unwrap();
        assert!(content.contains("CREATE ROLE") || !content.is_empty());
    }

    #[tokio::test]
    #[ignore]
    async fn test_dump_schema() {
        let url = std::env::var("TEST_SOURCE_URL").unwrap();
        let dir = tempdir().unwrap();
        let output = dir.path().join("schema.sql");

        // Extract database name from URL
        let db = url.split('/').next_back().unwrap_or("postgres");

        let result = dump_schema(&url, db, output.to_str().unwrap()).await;

        assert!(result.is_ok());
        assert!(output.exists());
    }
}
