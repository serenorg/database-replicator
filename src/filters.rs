// ABOUTME: Central filtering logic for selective replication
// ABOUTME: Handles database and table include/exclude patterns

use anyhow::{bail, Result};
use tokio_postgres::Client;

/// Represents replication filtering rules
#[derive(Debug, Clone, Default)]
pub struct ReplicationFilter {
    include_databases: Option<Vec<String>>,
    exclude_databases: Option<Vec<String>>,
    include_tables: Option<Vec<String>>, // Format: "db.table"
    exclude_tables: Option<Vec<String>>, // Format: "db.table"
}

impl ReplicationFilter {
    /// Creates a filter from CLI arguments
    pub fn new(
        include_databases: Option<Vec<String>>,
        exclude_databases: Option<Vec<String>>,
        include_tables: Option<Vec<String>>,
        exclude_tables: Option<Vec<String>>,
    ) -> Result<Self> {
        // Validate mutually exclusive flags
        if include_databases.is_some() && exclude_databases.is_some() {
            bail!("Cannot use both --include-databases and --exclude-databases");
        }
        if include_tables.is_some() && exclude_tables.is_some() {
            bail!("Cannot use both --include-tables and --exclude-tables");
        }

        // Validate table format (must be "database.table")
        if let Some(ref tables) = include_tables {
            for table in tables {
                if !table.contains('.') {
                    bail!(
                        "Table must be specified as 'database.table', got '{}'",
                        table
                    );
                }
            }
        }
        if let Some(ref tables) = exclude_tables {
            for table in tables {
                if !table.contains('.') {
                    bail!(
                        "Table must be specified as 'database.table', got '{}'",
                        table
                    );
                }
            }
        }

        Ok(Self {
            include_databases,
            exclude_databases,
            include_tables,
            exclude_tables,
        })
    }

    /// Creates an empty filter (replicate everything)
    pub fn empty() -> Self {
        Self::default()
    }

    /// Checks if any filters are active
    pub fn is_empty(&self) -> bool {
        self.include_databases.is_none()
            && self.exclude_databases.is_none()
            && self.include_tables.is_none()
            && self.exclude_tables.is_none()
    }

    /// Determines if a database should be replicated
    pub fn should_replicate_database(&self, db_name: &str) -> bool {
        // If include list exists, database must be in it
        if let Some(ref include) = self.include_databases {
            if !include.contains(&db_name.to_string()) {
                return false;
            }
        }

        // If exclude list exists, database must not be in it
        if let Some(ref exclude) = self.exclude_databases {
            if exclude.contains(&db_name.to_string()) {
                return false;
            }
        }

        true
    }

    /// Determines if a table should be replicated
    pub fn should_replicate_table(&self, db_name: &str, table_name: &str) -> bool {
        let full_name = format!("{}.{}", db_name, table_name);

        // If include list exists, table must be in it
        if let Some(ref include) = self.include_tables {
            if !include.contains(&full_name) {
                return false;
            }
        }

        // If exclude list exists, table must not be in it
        if let Some(ref exclude) = self.exclude_tables {
            if exclude.contains(&full_name) {
                return false;
            }
        }

        true
    }

    /// Gets list of databases to replicate (queries source if needed)
    pub async fn get_databases_to_replicate(&self, source_conn: &Client) -> Result<Vec<String>> {
        // Get all databases from source
        let all_databases = crate::migration::schema::list_databases(source_conn).await?;

        // Filter based on rules
        let filtered: Vec<String> = all_databases
            .into_iter()
            .filter(|db| self.should_replicate_database(&db.name))
            .map(|db| db.name)
            .collect();

        if filtered.is_empty() {
            bail!("No databases selected for replication. Check your filters.");
        }

        Ok(filtered)
    }

    /// Gets list of tables to replicate for a given database
    pub async fn get_tables_to_replicate(
        &self,
        source_conn: &Client,
        db_name: &str,
    ) -> Result<Vec<String>> {
        // Get all tables from the database
        let all_tables = crate::migration::schema::list_tables(source_conn).await?;

        // Filter based on rules
        let filtered: Vec<String> = all_tables
            .into_iter()
            .filter(|table| self.should_replicate_table(db_name, &table.name))
            .map(|table| table.name)
            .collect();

        Ok(filtered)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_validates_mutually_exclusive_database_flags() {
        let result = ReplicationFilter::new(
            Some(vec!["db1".to_string()]),
            Some(vec!["db2".to_string()]),
            None,
            None,
        );
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Cannot use both --include-databases and --exclude-databases"));
    }

    #[test]
    fn test_new_validates_mutually_exclusive_table_flags() {
        let result = ReplicationFilter::new(
            None,
            None,
            Some(vec!["db1.table1".to_string()]),
            Some(vec!["db2.table2".to_string()]),
        );
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Cannot use both --include-tables and --exclude-tables"));
    }

    #[test]
    fn test_new_validates_table_format_for_include() {
        let result =
            ReplicationFilter::new(None, None, Some(vec!["invalid_table".to_string()]), None);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Table must be specified as 'database.table'"));
    }

    #[test]
    fn test_new_validates_table_format_for_exclude() {
        let result =
            ReplicationFilter::new(None, None, None, Some(vec!["invalid_table".to_string()]));
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Table must be specified as 'database.table'"));
    }

    #[test]
    fn test_should_replicate_database_with_include_list() {
        let filter = ReplicationFilter::new(
            Some(vec!["db1".to_string(), "db2".to_string()]),
            None,
            None,
            None,
        )
        .unwrap();

        assert!(filter.should_replicate_database("db1"));
        assert!(filter.should_replicate_database("db2"));
        assert!(!filter.should_replicate_database("db3"));
    }

    #[test]
    fn test_should_replicate_database_with_exclude_list() {
        let filter = ReplicationFilter::new(
            None,
            Some(vec!["test".to_string(), "dev".to_string()]),
            None,
            None,
        )
        .unwrap();

        assert!(filter.should_replicate_database("production"));
        assert!(!filter.should_replicate_database("test"));
        assert!(!filter.should_replicate_database("dev"));
    }

    #[test]
    fn test_should_replicate_table_with_include_list() {
        let filter = ReplicationFilter::new(
            None,
            None,
            Some(vec!["db1.users".to_string(), "db1.orders".to_string()]),
            None,
        )
        .unwrap();

        assert!(filter.should_replicate_table("db1", "users"));
        assert!(filter.should_replicate_table("db1", "orders"));
        assert!(!filter.should_replicate_table("db1", "logs"));
    }

    #[test]
    fn test_should_replicate_table_with_exclude_list() {
        let filter = ReplicationFilter::new(
            None,
            None,
            None,
            Some(vec![
                "db1.audit_logs".to_string(),
                "db1.temp_data".to_string(),
            ]),
        )
        .unwrap();

        assert!(filter.should_replicate_table("db1", "users"));
        assert!(!filter.should_replicate_table("db1", "audit_logs"));
        assert!(!filter.should_replicate_table("db1", "temp_data"));
    }

    #[test]
    fn test_empty_filter_replicates_everything() {
        let filter = ReplicationFilter::empty();

        assert!(filter.is_empty());
        assert!(filter.should_replicate_database("any_db"));
        assert!(filter.should_replicate_table("any_db", "any_table"));
    }

    #[test]
    fn test_is_empty_returns_false_when_include_databases_set() {
        let filter =
            ReplicationFilter::new(Some(vec!["db1".to_string()]), None, None, None).unwrap();
        assert!(!filter.is_empty());
    }

    #[test]
    fn test_is_empty_returns_false_when_exclude_databases_set() {
        let filter =
            ReplicationFilter::new(None, Some(vec!["db1".to_string()]), None, None).unwrap();
        assert!(!filter.is_empty());
    }

    #[test]
    fn test_is_empty_returns_false_when_include_tables_set() {
        let filter =
            ReplicationFilter::new(None, None, Some(vec!["db1.table1".to_string()]), None).unwrap();
        assert!(!filter.is_empty());
    }

    #[test]
    fn test_is_empty_returns_false_when_exclude_tables_set() {
        let filter =
            ReplicationFilter::new(None, None, None, Some(vec!["db1.table1".to_string()])).unwrap();
        assert!(!filter.is_empty());
    }
}
