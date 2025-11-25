// ABOUTME: Database size estimation and replication time prediction
// ABOUTME: Helps users understand resource requirements before replication

use anyhow::{Context, Result};
use std::time::Duration;
use tokio_postgres::Client;

use super::schema::DatabaseInfo;
use crate::filters::ReplicationFilter;

/// Information about a database's size and estimated replication time
#[derive(Debug, Clone)]
pub struct DatabaseSizeInfo {
    /// Database name
    pub name: String,
    /// Size in bytes
    pub size_bytes: i64,
    /// Human-readable size (e.g., "15.3 GB")
    pub size_human: String,
    /// Estimated replication duration
    pub estimated_duration: Duration,
}

/// Estimate database sizes and replication times with filtering support
///
/// Queries PostgreSQL for database sizes and calculates estimated replication times
/// based on typical dump/restore speeds. Uses a conservative estimate of 20 GB/hour
/// for total replication time (dump + restore).
///
/// When table filters are specified, connects to each database to compute the exact
/// size of filtered tables rather than using the entire database size.
///
/// # Arguments
///
/// * `source_url` - Connection URL for the source database cluster
/// * `source_client` - Connected PostgreSQL client to source database
/// * `databases` - List of databases to estimate
/// * `filter` - Replication filter for table inclusion/exclusion
///
/// # Returns
///
/// Returns a vector of `DatabaseSizeInfo` with size and time estimates for each database.
///
/// # Errors
///
/// This function will return an error if:
/// - Cannot query database sizes
/// - Database connection fails
/// - Cannot connect to individual databases for table filtering
///
/// # Examples
///
/// ```no_run
/// # use anyhow::Result;
/// # use database_replicator::postgres::connect;
/// # use database_replicator::migration::{list_databases, estimate_database_sizes};
/// # use database_replicator::filters::ReplicationFilter;
/// # async fn example() -> Result<()> {
/// let url = "postgresql://user:pass@localhost:5432/postgres";
/// let client = connect(url).await?;
/// let databases = list_databases(&client).await?;
/// let filter = ReplicationFilter::empty();
/// let estimates = estimate_database_sizes(url, &client, &databases, &filter).await?;
///
/// for estimate in estimates {
///     println!("{}: {} (~{:?})", estimate.name, estimate.size_human, estimate.estimated_duration);
/// }
/// # Ok(())
/// # }
/// ```
pub async fn estimate_database_sizes(
    source_url: &str,
    source_client: &Client,
    databases: &[DatabaseInfo],
    filter: &ReplicationFilter,
) -> Result<Vec<DatabaseSizeInfo>> {
    let mut sizes = Vec::new();

    // Check if table filtering is active
    let has_table_filter = filter.include_tables().is_some() || filter.exclude_tables().is_some();

    for db in databases {
        let size_bytes = if has_table_filter {
            // With table filtering, we need to connect to each database
            // and sum up only the filtered tables
            estimate_filtered_database_size(source_url, &db.name, filter).await?
        } else {
            // Without table filtering, use the faster pg_database_size()
            let row = source_client
                .query_one("SELECT pg_database_size($1::text)", &[&db.name])
                .await
                .context(format!("Failed to query size for database '{}'", db.name))?;
            row.get(0)
        };

        // Estimate replication time based on typical speeds
        // Conservative estimates:
        // - Dump: 25-35 GB/hour
        // - Restore: 15-25 GB/hour
        // Combined conservative estimate: 20 GB/hour total
        let estimated_duration = estimate_replication_duration(size_bytes);

        sizes.push(DatabaseSizeInfo {
            name: db.name.clone(),
            size_bytes,
            size_human: format_bytes(size_bytes),
            estimated_duration,
        });
    }

    Ok(sizes)
}

/// Estimate database size considering table filters
///
/// Connects to the specific database, gets all tables, filters them,
/// and sums up the sizes of only the filtered tables.
///
/// # Arguments
///
/// * `source_url` - Connection URL for the source database cluster
/// * `db_name` - Name of the database to estimate
/// * `filter` - Replication filter for table inclusion/exclusion
///
/// # Returns
///
/// Total size in bytes of all filtered tables in the database
async fn estimate_filtered_database_size(
    source_url: &str,
    db_name: &str,
    filter: &ReplicationFilter,
) -> Result<i64> {
    // Build connection URL for this specific database
    let db_url = replace_database_in_url(source_url, db_name)?;
    let client = crate::postgres::connect(&db_url).await?;

    // Get all tables in the database
    let tables = super::schema::list_tables(&client).await?;

    // Filter tables based on filter rules
    let filtered_tables: Vec<_> = tables
        .into_iter()
        .filter(|table| {
            // Build full table name in "database.table" format for filtering
            let table_name = if table.schema == "public" {
                table.name.clone()
            } else {
                format!("{}.{}", table.schema, table.name)
            };
            filter.should_replicate_table(db_name, &table_name)
        })
        .collect();

    // Sum up sizes of filtered tables
    let mut total_size: i64 = 0;
    for table in filtered_tables {
        // Use pg_total_relation_size to include indexes, TOAST, etc.
        let query = format!(
            "SELECT pg_total_relation_size('{}.{}'::regclass)",
            table.schema, table.name
        );

        let row = client.query_one(&query, &[]).await.context(format!(
            "Failed to query size for table '{}.{}'",
            table.schema, table.name
        ))?;

        let table_size: i64 = row.get(0);
        total_size += table_size;
    }

    Ok(total_size)
}

/// Replace the database name in a connection URL
///
/// # Arguments
///
/// * `url` - Original connection URL
/// * `new_database` - New database name to use
///
/// # Returns
///
/// Updated connection URL with new database name
fn replace_database_in_url(url: &str, new_database: &str) -> Result<String> {
    // Parse URL to find database name
    // Format: postgresql://user:pass@host:port/database?params

    // Split by '?' to separate params
    let parts: Vec<&str> = url.split('?').collect();
    let base_url = parts[0];
    let params = if parts.len() > 1 {
        Some(parts[1])
    } else {
        None
    };

    // Split base by '/' to get everything before database name
    let url_parts: Vec<&str> = base_url.rsplitn(2, '/').collect();
    if url_parts.len() != 2 {
        anyhow::bail!("Invalid connection URL format");
    }

    // Reconstruct URL with new database name
    let mut new_url = format!("{}/{}", url_parts[1], new_database);
    if let Some(p) = params {
        new_url = format!("{}?{}", new_url, p);
    }

    Ok(new_url)
}

/// Estimate replication duration based on database size
///
/// Uses a conservative estimate of 20 GB/hour for total replication time,
/// which accounts for both dump and restore operations.
///
/// # Arguments
///
/// * `size_bytes` - Database size in bytes
///
/// # Returns
///
/// Estimated duration for complete replication (dump + restore)
fn estimate_replication_duration(size_bytes: i64) -> Duration {
    // Conservative estimate: 20 GB/hour total (dump + restore)
    const BYTES_PER_HOUR: f64 = 20.0 * 1024.0 * 1024.0 * 1024.0; // 20 GB

    let hours = size_bytes as f64 / BYTES_PER_HOUR;
    Duration::from_secs_f64(hours * 3600.0)
}

/// Format bytes into human-readable string
///
/// Converts byte count into appropriate units (B, KB, MB, GB, TB)
/// with one decimal place of precision.
///
/// # Arguments
///
/// * `bytes` - Number of bytes to format
///
/// # Returns
///
/// Human-readable string (e.g., "15.3 GB", "2.1 MB")
///
/// # Examples
///
/// ```
/// # use database_replicator::migration::format_bytes;
/// assert_eq!(format_bytes(1024), "1.0 KB");
/// assert_eq!(format_bytes(1536), "1.5 KB");
/// assert_eq!(format_bytes(1073741824), "1.0 GB");
/// assert_eq!(format_bytes(16106127360), "15.0 GB");
/// ```
pub fn format_bytes(bytes: i64) -> String {
    const UNITS: &[&str] = &["B", "KB", "MB", "GB", "TB"];
    let mut size = bytes as f64;
    let mut unit_idx = 0;

    while size >= 1024.0 && unit_idx < UNITS.len() - 1 {
        size /= 1024.0;
        unit_idx += 1;
    }

    format!("{:.1} {}", size, UNITS[unit_idx])
}

/// Format duration into human-readable string
///
/// Converts duration into appropriate units (seconds, minutes, hours, days)
/// with reasonable precision.
///
/// # Arguments
///
/// * `duration` - Duration to format
///
/// # Returns
///
/// Human-readable string (e.g., "~2.5 hours", "~45 minutes", "~3 days")
///
/// # Examples
///
/// ```
/// # use std::time::Duration;
/// # use database_replicator::migration::format_duration;
/// assert_eq!(format_duration(Duration::from_secs(45)), "~45 seconds");
/// assert_eq!(format_duration(Duration::from_secs(120)), "~2.0 minutes");
/// assert_eq!(format_duration(Duration::from_secs(3600)), "~1.0 hours");
/// assert_eq!(format_duration(Duration::from_secs(7200)), "~2.0 hours");
/// ```
pub fn format_duration(duration: Duration) -> String {
    let secs = duration.as_secs();

    if secs < 60 {
        format!("~{} seconds", secs)
    } else if secs < 3600 {
        let minutes = secs as f64 / 60.0;
        format!("~{:.1} minutes", minutes)
    } else if secs < 86400 {
        let hours = secs as f64 / 3600.0;
        format!("~{:.1} hours", hours)
    } else {
        let days = secs as f64 / 86400.0;
        format!("~{:.1} days", days)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_bytes() {
        assert_eq!(format_bytes(0), "0.0 B");
        assert_eq!(format_bytes(500), "500.0 B");
        assert_eq!(format_bytes(1024), "1.0 KB");
        assert_eq!(format_bytes(1536), "1.5 KB");
        assert_eq!(format_bytes(1048576), "1.0 MB");
        assert_eq!(format_bytes(1073741824), "1.0 GB");
        assert_eq!(format_bytes(16106127360), "15.0 GB");
        assert_eq!(format_bytes(1099511627776), "1.0 TB");
    }

    #[test]
    fn test_format_duration() {
        assert_eq!(format_duration(Duration::from_secs(30)), "~30 seconds");
        assert_eq!(format_duration(Duration::from_secs(59)), "~59 seconds");
        assert_eq!(format_duration(Duration::from_secs(60)), "~1.0 minutes");
        assert_eq!(format_duration(Duration::from_secs(120)), "~2.0 minutes");
        assert_eq!(format_duration(Duration::from_secs(3599)), "~60.0 minutes");
        assert_eq!(format_duration(Duration::from_secs(3600)), "~1.0 hours");
        assert_eq!(format_duration(Duration::from_secs(7200)), "~2.0 hours");
        assert_eq!(format_duration(Duration::from_secs(86400)), "~1.0 days");
        assert_eq!(format_duration(Duration::from_secs(172800)), "~2.0 days");
    }

    #[test]
    fn test_estimate_replication_duration() {
        // 1 GB should take ~3 minutes (20 GB/hour = 0.05 hours for 1 GB)
        let duration = estimate_replication_duration(1073741824);
        assert!(duration.as_secs() >= 170 && duration.as_secs() <= 190);

        // 20 GB should take ~1 hour
        let duration = estimate_replication_duration(21474836480);
        assert!(duration.as_secs() >= 3500 && duration.as_secs() <= 3700);
    }
}
