// ABOUTME: Extension compatibility checking for PostgreSQL databases
// ABOUTME: Validates that target has all required extensions and they are properly configured

use anyhow::{Context, Result};
use tokio_postgres::Client;

#[derive(Debug, Clone)]
pub struct Extension {
    pub name: String,
    pub version: String,
}

#[derive(Debug, Clone)]
pub struct AvailableExtension {
    pub name: String,
    pub default_version: Option<String>,
    pub installed_version: Option<String>,
}

/// Get list of installed extensions on a database
pub async fn get_installed_extensions(client: &Client) -> Result<Vec<Extension>> {
    let rows = client
        .query(
            "SELECT extname, extversion FROM pg_extension WHERE extname != 'plpgsql' ORDER BY extname",
            &[],
        )
        .await
        .context("Failed to query installed extensions")?;

    let extensions = rows
        .iter()
        .map(|row| Extension {
            name: row.get(0),
            version: row.get(1),
        })
        .collect();

    Ok(extensions)
}

/// Get list of available extensions on a database
pub async fn get_available_extensions(client: &Client) -> Result<Vec<AvailableExtension>> {
    let rows = client
        .query(
            "SELECT name, default_version, installed_version FROM pg_available_extensions ORDER BY name",
            &[],
        )
        .await
        .context("Failed to query available extensions")?;

    let extensions = rows
        .iter()
        .map(|row| AvailableExtension {
            name: row.get(0),
            default_version: row.get(1),
            installed_version: row.get(2),
        })
        .collect();

    Ok(extensions)
}

/// Get list of preloaded libraries from shared_preload_libraries setting
pub async fn get_preloaded_libraries(client: &Client) -> Result<Vec<String>> {
    let row = client
        .query_one("SHOW shared_preload_libraries", &[])
        .await
        .context("Failed to query shared_preload_libraries")?;

    let libs_str: String = row.get(0);

    // Parse comma-separated list, handling spaces and empty strings
    let libraries = libs_str
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();

    Ok(libraries)
}

/// Extensions that require preloading via shared_preload_libraries
const PRELOAD_REQUIRED_EXTENSIONS: &[&str] = &[
    "timescaledb",
    "citus",
    "pg_stat_statements",
    "pg_cron",
    "auto_explain",
    "pg_partman_bgw",
];

/// Check if an extension requires preloading
pub fn requires_preload(extension_name: &str) -> bool {
    PRELOAD_REQUIRED_EXTENSIONS.contains(&extension_name)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_requires_preload() {
        assert!(requires_preload("timescaledb"));
        assert!(requires_preload("citus"));
        assert!(requires_preload("pg_stat_statements"));
        assert!(!requires_preload("pg_trgm"));
        assert!(!requires_preload("uuid-ossp"));
    }
}
