// ABOUTME: Privilege checking utilities for migration prerequisites
// ABOUTME: Validates source and target databases have required permissions

use anyhow::{Context, Result};
use tokio_postgres::Client;

/// Result of privilege check for a PostgreSQL user
///
/// Contains information about the user's permissions required for migration.
pub struct PrivilegeCheck {
    /// User has REPLICATION privilege (required for source database)
    pub has_replication: bool,
    /// User has CREATEDB privilege (required for target database)
    pub has_create_db: bool,
    /// User has CREATEROLE privilege (optional, for role migration)
    pub has_create_role: bool,
    /// User is a superuser (bypasses other privilege requirements)
    pub is_superuser: bool,
}

/// Check if connected user has replication privileges (needed for source)
///
/// Queries `pg_roles` to determine the privileges of the currently connected user.
/// For source databases, the user must have REPLICATION privilege (or be a superuser)
/// to enable logical replication.
///
/// # Arguments
///
/// * `client` - Connected PostgreSQL client
///
/// # Returns
///
/// Returns a `PrivilegeCheck` containing the user's privileges.
///
/// # Errors
///
/// This function will return an error if the database query fails.
///
/// # Examples
///
/// ```no_run
/// # use anyhow::Result;
/// # use database_replicator::postgres::{connect, check_source_privileges};
/// # async fn example() -> Result<()> {
/// let client = connect("postgresql://user:pass@localhost:5432/mydb").await?;
/// let privs = check_source_privileges(&client).await?;
/// assert!(privs.has_replication || privs.is_superuser);
/// # Ok(())
/// # }
/// ```
pub async fn check_source_privileges(client: &Client) -> Result<PrivilegeCheck> {
    let row = client
        .query_one(
            "SELECT rolreplication, rolcreatedb, rolcreaterole, rolsuper
             FROM pg_roles
             WHERE rolname = current_user",
            &[],
        )
        .await
        .context("Failed to query user privileges")?;

    Ok(PrivilegeCheck {
        has_replication: row.get(0),
        has_create_db: row.get(1),
        has_create_role: row.get(2),
        is_superuser: row.get(3),
    })
}

/// Check if connected user has sufficient privileges for target database
///
/// Queries `pg_roles` to determine the privileges of the currently connected user.
/// For target databases, the user must have CREATEDB privilege (or be a superuser)
/// to create new databases during migration.
///
/// # Arguments
///
/// * `client` - Connected PostgreSQL client
///
/// # Returns
///
/// Returns a `PrivilegeCheck` containing the user's privileges.
///
/// # Errors
///
/// This function will return an error if the database query fails.
///
/// # Examples
///
/// ```no_run
/// # use anyhow::Result;
/// # use database_replicator::postgres::{connect, check_target_privileges};
/// # async fn example() -> Result<()> {
/// let client = connect("postgresql://user:pass@localhost:5432/mydb").await?;
/// let privs = check_target_privileges(&client).await?;
/// assert!(privs.has_create_db || privs.is_superuser);
/// # Ok(())
/// # }
/// ```
pub async fn check_target_privileges(client: &Client) -> Result<PrivilegeCheck> {
    // Same query as source
    check_source_privileges(client).await
}

/// Check the wal_level setting on the target database
///
/// Queries the current `wal_level` configuration parameter.
/// For logical replication (subscriptions), `wal_level` must be set to `logical`.
///
/// # Arguments
///
/// * `client` - Connected PostgreSQL client
///
/// # Returns
///
/// Returns the current `wal_level` setting as a String (e.g., "replica", "logical").
///
/// # Errors
///
/// This function will return an error if the database query fails.
///
/// # Examples
///
/// ```no_run
/// # use anyhow::Result;
/// # use database_replicator::postgres::{connect, check_wal_level};
/// # async fn example() -> Result<()> {
/// let client = connect("postgresql://user:pass@localhost:5432/mydb").await?;
/// let wal_level = check_wal_level(&client).await?;
/// assert_eq!(wal_level, "logical");
/// # Ok(())
/// # }
/// ```
pub async fn check_wal_level(client: &Client) -> Result<String> {
    let row = client
        .query_one("SHOW wal_level", &[])
        .await
        .context("Failed to query wal_level setting")?;

    let wal_level: String = row.get(0);
    Ok(wal_level)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::postgres::connect;

    #[tokio::test]
    #[ignore]
    async fn test_check_source_privileges() {
        let url = std::env::var("TEST_SOURCE_URL").unwrap();
        let client = connect(&url).await.unwrap();

        let privileges = check_source_privileges(&client).await.unwrap();

        // Should have at least one privilege
        assert!(
            privileges.has_replication || privileges.is_superuser,
            "Source user should have REPLICATION privilege or be superuser"
        );
    }

    #[tokio::test]
    #[ignore]
    async fn test_check_target_privileges() {
        let url = std::env::var("TEST_TARGET_URL").unwrap();
        let client = connect(&url).await.unwrap();

        let privileges = check_target_privileges(&client).await.unwrap();

        // Should have create privileges or be superuser
        assert!(
            privileges.has_create_db || privileges.is_superuser,
            "Target user should have CREATE DATABASE privilege or be superuser"
        );
    }
}
