// ABOUTME: Wrapper for pg_dump command to export database objects
// ABOUTME: Handles global objects, schema, and data export

use crate::filters::ReplicationFilter;
use anyhow::{Context, Result};
use std::collections::BTreeSet;
use std::process::{Command, Stdio};
use std::time::Duration;

/// Dump global objects (roles, tablespaces) using pg_dumpall
pub async fn dump_globals(source_url: &str, output_path: &str) -> Result<()> {
    tracing::info!("Dumping global objects to {}", output_path);

    // Parse URL and create .pgpass file for secure authentication
    let parts = crate::utils::parse_postgres_url(source_url)
        .with_context(|| format!("Failed to parse source URL: {}", source_url))?;
    let pgpass = crate::utils::PgPassFile::new(&parts)
        .context("Failed to create .pgpass file for authentication")?;

    let env_vars = parts.to_pg_env_vars();
    let output_path_owned = output_path.to_string();

    // Wrap subprocess execution with retry logic
    crate::utils::retry_subprocess_with_backoff(
        || {
            let mut cmd = Command::new("pg_dumpall");
            cmd.arg("--globals-only")
                .arg("--no-role-passwords") // Don't dump passwords
                .arg("--verbose") // Show progress
                .arg("--host")
                .arg(&parts.host)
                .arg("--port")
                .arg(parts.port.to_string())
                .arg("--database")
                .arg(&parts.database)
                .arg(format!("--file={}", output_path_owned))
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

            cmd.status().context(
                "Failed to execute pg_dumpall. Is PostgreSQL client installed?\n\
                 Install with:\n\
                 - Ubuntu/Debian: sudo apt-get install postgresql-client\n\
                 - macOS: brew install postgresql\n\
                 - RHEL/CentOS: sudo yum install postgresql",
            )
        },
        3,                      // Max 3 retries
        Duration::from_secs(1), // Start with 1 second delay
        "pg_dumpall (dump globals)",
    )
    .context(
        "pg_dumpall failed to dump global objects.\n\
         \n\
         Common causes:\n\
         - Connection authentication failed\n\
         - User lacks sufficient privileges (need SUPERUSER or pg_read_all_settings role)\n\
         - Network connectivity issues\n\
         - Invalid connection string\n\
         - Connection timeout or network issues",
    )?;

    tracing::info!("✓ Global objects dumped successfully");
    Ok(())
}

/// Dump schema (DDL) for a specific database
pub async fn dump_schema(
    source_url: &str,
    database: &str,
    output_path: &str,
    filter: &ReplicationFilter,
) -> Result<()> {
    tracing::info!(
        "Dumping schema for database '{}' to {}",
        database,
        output_path
    );

    // Parse URL and create .pgpass file for secure authentication
    let parts = crate::utils::parse_postgres_url(source_url)
        .with_context(|| format!("Failed to parse source URL: {}", source_url))?;
    let pgpass = crate::utils::PgPassFile::new(&parts)
        .context("Failed to create .pgpass file for authentication")?;

    let env_vars = parts.to_pg_env_vars();
    let output_path_owned = output_path.to_string();

    // Collect filter options
    let exclude_tables = get_schema_excluded_tables_for_db(filter, database);
    let include_tables = get_included_tables_for_db(filter, database);

    // Wrap subprocess execution with retry logic
    crate::utils::retry_subprocess_with_backoff(
        || {
            let mut cmd = Command::new("pg_dump");
            cmd.arg("--schema-only")
                .arg("--no-owner") // Don't include ownership commands
                .arg("--no-privileges") // We'll handle privileges separately
                .arg("--verbose"); // Show progress

            // Add table filtering if specified
            // Only exclude explicit exclude_tables from schema dump (NOT schema_only or predicate tables)
            if let Some(ref exclude) = exclude_tables {
                if !exclude.is_empty() {
                    for table in exclude {
                        cmd.arg("--exclude-table").arg(table);
                    }
                }
            }

            // If include_tables is specified, only dump those tables
            if let Some(ref include) = include_tables {
                if !include.is_empty() {
                    for table in include {
                        cmd.arg("--table").arg(table);
                    }
                }
            }

            cmd.arg("--host")
                .arg(&parts.host)
                .arg("--port")
                .arg(parts.port.to_string())
                .arg("--dbname")
                .arg(&parts.database)
                .arg(format!("--file={}", output_path_owned))
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

            cmd.status().context(
                "Failed to execute pg_dump. Is PostgreSQL client installed?\n\
                 Install with:\n\
                 - Ubuntu/Debian: sudo apt-get install postgresql-client\n\
                 - macOS: brew install postgresql\n\
                 - RHEL/CentOS: sudo yum install postgresql",
            )
        },
        3,                      // Max 3 retries
        Duration::from_secs(1), // Start with 1 second delay
        "pg_dump (dump schema)",
    )
    .with_context(|| {
        format!(
            "pg_dump failed to dump schema for database '{}'.\n\
             \n\
             Common causes:\n\
             - Database does not exist\n\
             - Connection authentication failed\n\
             - User lacks privileges to read database schema\n\
             - Network connectivity issues\n\
             - Connection timeout or network issues",
            database
        )
    })?;

    tracing::info!("✓ Schema dumped successfully");
    Ok(())
}

/// Dump data for a specific database using optimized directory format
///
/// Uses PostgreSQL directory format dump with:
/// - Parallel dumps for faster performance
/// - Maximum compression (level 9)
/// - Large object (blob) support
/// - Directory output for efficient parallel restore
///
/// The number of parallel jobs is automatically determined based on available CPU cores.
pub async fn dump_data(
    source_url: &str,
    database: &str,
    output_path: &str,
    filter: &ReplicationFilter,
) -> Result<()> {
    // Determine optimal number of parallel jobs (number of CPUs, capped at 8)
    let num_cpus = std::thread::available_parallelism()
        .map(|n| n.get().min(8))
        .unwrap_or(4);

    tracing::info!(
        "Dumping data for database '{}' to {} (parallel={}, compression=9, format=directory)",
        database,
        output_path,
        num_cpus
    );

    // Parse URL and create .pgpass file for secure authentication
    let parts = crate::utils::parse_postgres_url(source_url)
        .with_context(|| format!("Failed to parse source URL: {}", source_url))?;
    let pgpass = crate::utils::PgPassFile::new(&parts)
        .context("Failed to create .pgpass file for authentication")?;

    let env_vars = parts.to_pg_env_vars();
    let output_path_owned = output_path.to_string();

    // Collect filter options
    let exclude_tables = get_data_excluded_tables_for_db(filter, database);
    let include_tables = get_included_tables_for_db(filter, database);

    // Wrap subprocess execution with retry logic
    crate::utils::retry_subprocess_with_backoff(
        || {
            let mut cmd = Command::new("pg_dump");
            cmd.arg("--data-only")
                .arg("--no-owner")
                .arg("--format=directory") // Directory format enables parallel operations
                .arg("--blobs") // Include large objects (blobs)
                .arg("--compress=9") // Maximum compression for smaller dump size
                .arg(format!("--jobs={}", num_cpus)) // Parallel dump jobs
                .arg("--verbose"); // Show progress

            // Add table filtering if specified
            // Exclude explicit excludes, schema_only tables, and predicate tables from data dump
            if let Some(ref exclude) = exclude_tables {
                if !exclude.is_empty() {
                    for table in exclude {
                        cmd.arg("--exclude-table-data").arg(table);
                    }
                }
            }

            // If include_tables is specified, only dump data for those tables
            if let Some(ref include) = include_tables {
                if !include.is_empty() {
                    for table in include {
                        cmd.arg("--table").arg(table);
                    }
                }
            }

            cmd.arg("--host")
                .arg(&parts.host)
                .arg("--port")
                .arg(parts.port.to_string())
                .arg("--dbname")
                .arg(&parts.database)
                .arg(format!("--file={}", output_path_owned))
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

            cmd.status().context(
                "Failed to execute pg_dump. Is PostgreSQL client installed?\n\
                 Install with:\n\
                 - Ubuntu/Debian: sudo apt-get install postgresql-client\n\
                 - macOS: brew install postgresql\n\
                 - RHEL/CentOS: sudo yum install postgresql",
            )
        },
        3,                      // Max 3 retries
        Duration::from_secs(1), // Start with 1 second delay
        "pg_dump (dump data)",
    )
    .with_context(|| {
        format!(
            "pg_dump failed to dump data for database '{}'.\n\
             \n\
             Common causes:\n\
             - Database does not exist\n\
             - Connection authentication failed\n\
             - User lacks privileges to read table data\n\
             - Network connectivity issues\n\
             - Insufficient disk space for dump directory\n\
             - Output directory already exists (pg_dump requires non-existent path)\n\
             - Connection timeout or network issues",
            database
        )
    })?;

    tracing::info!(
        "✓ Data dumped successfully using {} parallel jobs",
        num_cpus
    );
    Ok(())
}

/// Extract table names to exclude from SCHEMA dumps (--exclude-table flag)
/// Only excludes explicit exclude_tables - NOT schema_only or predicate tables
/// (those need their schema created, just not bulk data copied)
/// Returns schema-qualified names in format: "schema"."table"
fn get_schema_excluded_tables_for_db(
    filter: &ReplicationFilter,
    db_name: &str,
) -> Option<Vec<String>> {
    let mut tables = BTreeSet::new();

    // Handle explicit exclude_tables (format: "database.table")
    // These tables are completely excluded (no schema, no data)
    if let Some(explicit) = filter.exclude_tables() {
        for full_name in explicit {
            let parts: Vec<&str> = full_name.split('.').collect();
            if parts.len() == 2 && parts[0] == db_name {
                // Format as "public"."table" for consistency
                tables.insert(format!("\"public\".\"{}\"", parts[1]));
            }
        }
    }

    if tables.is_empty() {
        None
    } else {
        Some(tables.into_iter().collect())
    }
}

/// Extract table names to exclude from DATA dumps (--exclude-table-data flag)
/// Excludes explicit excludes, schema_only tables, and predicate tables
/// (predicate tables will be copied separately with filtering)
/// Returns schema-qualified names in format: "schema"."table"
fn get_data_excluded_tables_for_db(
    filter: &ReplicationFilter,
    db_name: &str,
) -> Option<Vec<String>> {
    let mut tables = BTreeSet::new();

    // Handle explicit exclude_tables (format: "database.table")
    // Default to public schema for backward compatibility
    if let Some(explicit) = filter.exclude_tables() {
        for full_name in explicit {
            let parts: Vec<&str> = full_name.split('.').collect();
            if parts.len() == 2 && parts[0] == db_name {
                // Format as "public"."table" for consistency
                tables.insert(format!("\"public\".\"{}\"", parts[1]));
            }
        }
    }

    // schema_only_tables and predicate_tables already return schema-qualified names
    for table in filter.schema_only_tables(db_name) {
        tables.insert(table);
    }

    for (table, _) in filter.predicate_tables(db_name) {
        tables.insert(table);
    }

    if tables.is_empty() {
        None
    } else {
        Some(tables.into_iter().collect())
    }
}

/// Extract table names for a specific database from include_tables filter
/// Returns schema-qualified names in format: "schema"."table"
fn get_included_tables_for_db(filter: &ReplicationFilter, db_name: &str) -> Option<Vec<String>> {
    filter.include_tables().map(|tables| {
        tables
            .iter()
            .filter_map(|full_name| {
                let parts: Vec<&str> = full_name.split('.').collect();
                if parts.len() == 2 && parts[0] == db_name {
                    // Format as "public"."table" for consistency
                    Some(format!("\"public\".\"{}\"", parts[1]))
                } else {
                    None
                }
            })
            .collect()
    })
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

        let filter = crate::filters::ReplicationFilter::empty();
        let result = dump_schema(&url, db, output.to_str().unwrap(), &filter).await;

        assert!(result.is_ok());
        assert!(output.exists());
    }

    #[test]
    fn test_get_schema_excluded_tables_for_db() {
        let filter = crate::filters::ReplicationFilter::new(
            None,
            None,
            None,
            Some(vec![
                "db1.table1".to_string(),
                "db1.table2".to_string(),
                "db2.table3".to_string(),
            ]),
        )
        .unwrap();

        // Schema exclusion only includes explicit exclude_tables
        let tables = get_schema_excluded_tables_for_db(&filter, "db1").unwrap();
        // Should return schema-qualified names
        assert_eq!(
            tables,
            vec!["\"public\".\"table1\"", "\"public\".\"table2\""]
        );

        let tables = get_schema_excluded_tables_for_db(&filter, "db2").unwrap();
        assert_eq!(tables, vec!["\"public\".\"table3\""]);

        let tables = get_schema_excluded_tables_for_db(&filter, "db3");
        assert!(tables.is_none() || tables.unwrap().is_empty());
    }

    #[test]
    fn test_get_data_excluded_tables_for_db() {
        let filter = crate::filters::ReplicationFilter::new(
            None,
            None,
            None,
            Some(vec![
                "db1.table1".to_string(),
                "db1.table2".to_string(),
                "db2.table3".to_string(),
            ]),
        )
        .unwrap();

        // Data exclusion includes explicit exclude_tables, schema_only, and predicate tables
        let tables = get_data_excluded_tables_for_db(&filter, "db1").unwrap();
        // Should return schema-qualified names
        assert_eq!(
            tables,
            vec!["\"public\".\"table1\"", "\"public\".\"table2\""]
        );

        let tables = get_data_excluded_tables_for_db(&filter, "db2").unwrap();
        assert_eq!(tables, vec!["\"public\".\"table3\""]);

        let tables = get_data_excluded_tables_for_db(&filter, "db3");
        assert!(tables.is_none() || tables.unwrap().is_empty());
    }

    #[test]
    fn test_get_included_tables_for_db() {
        let filter = crate::filters::ReplicationFilter::new(
            None,
            None,
            Some(vec![
                "db1.users".to_string(),
                "db1.orders".to_string(),
                "db2.products".to_string(),
            ]),
            None,
        )
        .unwrap();

        let tables = get_included_tables_for_db(&filter, "db1").unwrap();
        // Should return schema-qualified names in original order
        assert_eq!(
            tables,
            vec!["\"public\".\"users\"", "\"public\".\"orders\""]
        );

        let tables = get_included_tables_for_db(&filter, "db2").unwrap();
        assert_eq!(tables, vec!["\"public\".\"products\""]);

        let tables = get_included_tables_for_db(&filter, "db3");
        assert!(tables.is_none() || tables.unwrap().is_empty());
    }

    #[test]
    fn test_get_schema_excluded_tables_for_db_with_empty_filter() {
        let filter = crate::filters::ReplicationFilter::empty();
        let tables = get_schema_excluded_tables_for_db(&filter, "db1");
        assert!(tables.is_none());
    }

    #[test]
    fn test_get_data_excluded_tables_for_db_with_empty_filter() {
        let filter = crate::filters::ReplicationFilter::empty();
        let tables = get_data_excluded_tables_for_db(&filter, "db1");
        assert!(tables.is_none());
    }

    #[test]
    fn test_get_included_tables_for_db_with_empty_filter() {
        let filter = crate::filters::ReplicationFilter::empty();
        let tables = get_included_tables_for_db(&filter, "db1");
        assert!(tables.is_none());
    }
}
