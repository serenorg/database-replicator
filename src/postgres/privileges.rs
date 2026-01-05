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
    /// User has AWS RDS rds_replication role (RDS-specific alternative to REPLICATION)
    pub has_rds_replication: bool,
}

impl PrivilegeCheck {
    /// Returns true if user can perform replication (any method)
    ///
    /// Checks for standard PostgreSQL REPLICATION privilege, superuser status,
    /// or AWS RDS rds_replication role membership.
    pub fn can_replicate(&self) -> bool {
        self.has_replication || self.is_superuser || self.has_rds_replication
    }
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

    // Check for AWS RDS rds_replication role membership
    // This role exists only on AWS RDS and provides replication capability
    let has_rds_replication = client
        .query_opt(
            "SELECT 1 FROM pg_roles WHERE rolname = 'rds_replication'
             AND pg_has_role(current_user, 'rds_replication', 'MEMBER')",
            &[],
        )
        .await
        .unwrap_or(None)
        .is_some();

    Ok(PrivilegeCheck {
        has_replication: row.get(0),
        has_create_db: row.get(1),
        has_create_role: row.get(2),
        is_superuser: row.get(3),
        has_rds_replication,
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

/// Result of table-level permission check
#[derive(Debug, Clone)]
pub struct TablePermissionCheck {
    /// Tables the user CAN read (has SELECT privilege)
    pub accessible_tables: Vec<String>,
    /// Tables the user CANNOT read (missing SELECT privilege)
    pub inaccessible_tables: Vec<String>,
}

impl TablePermissionCheck {
    /// Returns true if user has SELECT on all tables
    pub fn all_accessible(&self) -> bool {
        self.inaccessible_tables.is_empty()
    }

    /// Count of inaccessible tables
    pub fn inaccessible_count(&self) -> usize {
        self.inaccessible_tables.len()
    }
}

/// Check SELECT permission on user tables in a database
///
/// Queries pg_tables to find user tables and checks if current user has SELECT privilege.
/// If `filtered_tables` is provided, only those specific tables are checked.
/// Otherwise, all user tables (excluding pg_catalog and information_schema) are checked.
///
/// # Arguments
///
/// * `client` - Connected PostgreSQL client (must be connected to the target database)
/// * `filtered_tables` - Optional list of specific tables to check (format: "schema.table")
///
/// # Returns
///
/// Returns `TablePermissionCheck` with lists of accessible and inaccessible tables.
///
/// # Errors
///
/// Returns an error if the permission query fails.
///
/// # Examples
///
/// ```no_run
/// # use anyhow::Result;
/// # use database_replicator::postgres::{connect, check_table_select_permissions};
/// # async fn example() -> Result<()> {
/// let client = connect("postgresql://user:pass@localhost:5432/mydb").await?;
/// // Check all tables
/// let perms = check_table_select_permissions(&client, None).await?;
/// // Or check specific tables
/// let specific = vec!["public.users".to_string(), "public.orders".to_string()];
/// let perms = check_table_select_permissions(&client, Some(&specific)).await?;
/// if !perms.all_accessible() {
///     println!("Cannot read {} tables", perms.inaccessible_count());
/// }
/// # Ok(())
/// # }
/// ```
pub async fn check_table_select_permissions(
    client: &Client,
    filtered_tables: Option<&[String]>,
) -> Result<TablePermissionCheck> {
    let mut accessible = Vec::new();
    let mut inaccessible = Vec::new();

    if let Some(tables) = filtered_tables {
        // Check only the specified tables
        for full_name in tables {
            let parts: Vec<&str> = full_name.splitn(2, '.').collect();
            let (schema, table) = if parts.len() == 2 {
                (parts[0], parts[1])
            } else {
                ("public", parts[0])
            };

            let query = "SELECT has_table_privilege(current_user, $1, 'SELECT')";
            let qualified_name = format!("{}.{}", schema, table);

            match client.query_one(query, &[&qualified_name]).await {
                Ok(row) => {
                    let has_select: bool = row.get(0);
                    if has_select {
                        accessible.push(full_name.clone());
                    } else {
                        inaccessible.push(full_name.clone());
                    }
                }
                Err(_) => {
                    // Table might not exist or other error - treat as inaccessible
                    inaccessible.push(full_name.clone());
                }
            }
        }
    } else {
        // Query all user tables and check SELECT permission
        let query = r#"
            SELECT
                schemaname,
                tablename,
                has_table_privilege(current_user, quote_ident(schemaname) || '.' || quote_ident(tablename), 'SELECT') as has_select
            FROM pg_tables
            WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
            ORDER BY schemaname, tablename
        "#;

        let rows = client
            .query(query, &[])
            .await
            .context("Failed to query table permissions")?;

        for row in rows {
            let schema: String = row.get(0);
            let table: String = row.get(1);
            let has_select: bool = row.get(2);

            let full_name = format!("{}.{}", schema, table);

            if has_select {
                accessible.push(full_name);
            } else {
                inaccessible.push(full_name);
            }
        }
    }

    Ok(TablePermissionCheck {
        accessible_tables: accessible,
        inaccessible_tables: inaccessible,
    })
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

        // Should have at least one replication method
        assert!(
            privileges.can_replicate(),
            "Source user should have REPLICATION privilege, rds_replication role, or be superuser"
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

    #[tokio::test]
    #[ignore] // Requires database connection
    async fn test_check_table_select_permissions() {
        let url = std::env::var("TEST_SOURCE_URL").expect("TEST_SOURCE_URL not set");
        let client = connect(&url).await.unwrap();

        // Test checking all tables (no filter)
        let result = check_table_select_permissions(&client, None).await.unwrap();

        // Just verify the function runs without error
        // In a real database, results depend on actual permissions
        println!("Accessible tables: {}", result.accessible_tables.len());
        println!("Inaccessible tables: {}", result.inaccessible_tables.len());
    }

    #[test]
    fn test_table_permission_check_struct() {
        let check = TablePermissionCheck {
            accessible_tables: vec!["public.users".to_string()],
            inaccessible_tables: vec![],
        };
        assert!(check.all_accessible());
        assert_eq!(check.inaccessible_count(), 0);

        let check_with_issues = TablePermissionCheck {
            accessible_tables: vec!["public.users".to_string()],
            inaccessible_tables: vec!["public.secrets".to_string()],
        };
        assert!(!check_with_issues.all_accessible());
        assert_eq!(check_with_issues.inaccessible_count(), 1);
    }
}
