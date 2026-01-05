// ABOUTME: Central filtering logic for selective replication
// ABOUTME: Handles database and table include/exclude patterns

use crate::table_rules::TableRules;
use anyhow::{bail, Result};
use sha2::{Digest, Sha256};
use tokio_postgres::Client;

/// Represents replication filtering rules
#[derive(Debug, Clone, Default)]
pub struct ReplicationFilter {
    include_databases: Option<Vec<String>>,
    exclude_databases: Option<Vec<String>>,
    include_tables: Option<Vec<String>>, // Format: "db.table"
    exclude_tables: Option<Vec<String>>, // Format: "db.table"
    table_rules: TableRules,
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
            table_rules: TableRules::default(),
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
            && self.table_rules.is_empty()
    }

    /// Returns a stable fingerprint for the filter configuration
    pub fn fingerprint(&self) -> String {
        fn hash_option_list(hasher: &mut Sha256, values: &Option<Vec<String>>) {
            match values {
                Some(items) => {
                    let mut sorted = items.clone();
                    sorted.sort();
                    for item in sorted {
                        hasher.update(item.as_bytes());
                        hasher.update(b"|");
                    }
                }
                None => hasher.update(b"<none>"),
            }
        }

        let mut hasher = Sha256::new();
        hash_option_list(&mut hasher, &self.include_databases);
        hasher.update(b"#");
        hash_option_list(&mut hasher, &self.exclude_databases);
        hasher.update(b"#");
        hash_option_list(&mut hasher, &self.include_tables);
        hasher.update(b"#");
        hash_option_list(&mut hasher, &self.exclude_tables);
        hasher.update(b"#");
        hasher.update(self.table_rules.fingerprint().as_bytes());

        format!("{:x}", hasher.finalize())
    }

    pub fn table_rules(&self) -> &TableRules {
        &self.table_rules
    }

    pub fn with_table_rules(mut self, rules: TableRules) -> Self {
        self.table_rules = rules;
        self
    }

    pub fn schema_only_tables(&self, database: &str) -> Vec<String> {
        self.table_rules.schema_only_tables(database)
    }

    pub fn predicate_tables(&self, database: &str) -> Vec<(String, String)> {
        self.table_rules.predicate_tables(database)
    }

    /// Gets the list of databases to include
    pub fn include_databases(&self) -> Option<&Vec<String>> {
        self.include_databases.as_ref()
    }

    /// Gets the list of databases to exclude
    pub fn exclude_databases(&self) -> Option<&Vec<String>> {
        self.exclude_databases.as_ref()
    }

    /// Gets the list of tables to include
    pub fn include_tables(&self) -> Option<&Vec<String>> {
        self.include_tables.as_ref()
    }

    /// Gets the list of tables to exclude
    pub fn exclude_tables(&self) -> Option<&Vec<String>> {
        self.exclude_tables.as_ref()
    }

    /// Gets the explicit list of databases to check/replicate
    ///
    /// Returns databases from:
    /// 1. include_databases if specified, OR
    /// 2. database names extracted from include_tables if specified
    ///
    /// Returns None if no explicit database list can be determined
    /// (meaning all databases should be enumerated).
    pub fn databases_to_check(&self) -> Option<Vec<String>> {
        if let Some(ref include) = self.include_databases {
            return Some(include.clone());
        }

        if let Some(ref include_tables) = self.include_tables {
            // Extract unique database names from "database.table" format
            let mut databases: Vec<String> = include_tables
                .iter()
                .filter_map(|table| table.split('.').next().map(String::from))
                .collect();
            databases.sort();
            databases.dedup();
            if !databases.is_empty() {
                return Some(databases);
            }
        }

        None
    }

    /// Determines if a database should be replicated
    ///
    /// A database is replicated if:
    /// 1. It's in the include_databases list (if specified), OR
    /// 2. It's referenced in include_tables (if specified and no include_databases), OR
    /// 3. No include filters are specified (replicate all)
    ///
    /// AND it's not in the exclude_databases list.
    pub fn should_replicate_database(&self, db_name: &str) -> bool {
        // If include_databases list exists, database must be in it
        if let Some(ref include) = self.include_databases {
            if !include.contains(&db_name.to_string()) {
                return false;
            }
        } else if let Some(ref include_tables) = self.include_tables {
            // If include_tables is specified but include_databases is not,
            // only replicate databases referenced in include_tables
            let db_referenced = include_tables
                .iter()
                .any(|table| table.split('.').next() == Some(db_name));
            if !db_referenced {
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

    /// Gets the table names to check for a specific database (synchronous, no DB query)
    ///
    /// Extracts table names from include_tables filter for the given database.
    /// Returns None if no include_tables filter is set (meaning check all tables).
    /// Table names are returned in "schema.table" format (defaulting to "public" schema).
    ///
    /// # Arguments
    ///
    /// * `db_name` - Database name to extract tables for
    ///
    /// # Returns
    ///
    /// Some(Vec<String>) with table names if include_tables is set, None otherwise
    pub fn tables_for_database(&self, db_name: &str) -> Option<Vec<String>> {
        let include_tables = self.include_tables.as_ref()?;

        let tables: Vec<String> = include_tables
            .iter()
            .filter_map(|full_name| {
                // Format: "database.table" or "database.schema.table"
                let parts: Vec<&str> = full_name.splitn(2, '.').collect();
                if parts.len() == 2 && parts[0] == db_name {
                    let table_part = parts[1];
                    // If table_part contains a dot, it's "schema.table"
                    // Otherwise, assume "public" schema
                    if table_part.contains('.') {
                        Some(table_part.to_string())
                    } else {
                        Some(format!("public.{}", table_part))
                    }
                } else {
                    None
                }
            })
            .collect();

        if tables.is_empty() {
            None
        } else {
            Some(tables)
        }
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

    #[test]
    fn test_fingerprint_is_order_insensitive() {
        let filter_a = ReplicationFilter::new(
            Some(vec!["db1".to_string(), "db2".to_string()]),
            None,
            None,
            None,
        )
        .unwrap();
        let filter_b = ReplicationFilter::new(
            Some(vec!["db2".to_string(), "db1".to_string()]),
            None,
            None,
            None,
        )
        .unwrap();

        assert_eq!(filter_a.fingerprint(), filter_b.fingerprint());
    }

    #[test]
    fn test_fingerprint_differs_for_different_filters() {
        let filter_a =
            ReplicationFilter::new(None, Some(vec!["db1".to_string()]), None, None).unwrap();
        let filter_b =
            ReplicationFilter::new(None, Some(vec!["db2".to_string()]), None, None).unwrap();

        assert_ne!(filter_a.fingerprint(), filter_b.fingerprint());
    }

    #[test]
    fn test_fingerprint_includes_table_rules_schema() {
        use crate::table_rules::TableRules;

        // Create two filters with different table rule schemas
        let mut table_rules_a = TableRules::default();
        table_rules_a
            .apply_schema_only_cli(&["public.orders".to_string()])
            .unwrap();

        let mut table_rules_b = TableRules::default();
        table_rules_b
            .apply_schema_only_cli(&["analytics.orders".to_string()])
            .unwrap();

        let filter_a = ReplicationFilter::empty().with_table_rules(table_rules_a);
        let filter_b = ReplicationFilter::empty().with_table_rules(table_rules_b);

        assert_ne!(
            filter_a.fingerprint(),
            filter_b.fingerprint(),
            "Filters with different table rule schemas should produce different fingerprints"
        );
    }

    #[test]
    fn test_tables_for_database_extracts_tables() {
        let filter = ReplicationFilter::new(
            None,
            None,
            Some(vec![
                "mydb.users".to_string(),
                "mydb.orders".to_string(),
                "otherdb.products".to_string(),
            ]),
            None,
        )
        .unwrap();

        let tables = filter.tables_for_database("mydb");
        assert!(tables.is_some());
        let tables = tables.unwrap();
        assert_eq!(tables.len(), 2);
        assert!(tables.contains(&"public.users".to_string()));
        assert!(tables.contains(&"public.orders".to_string()));
    }

    #[test]
    fn test_tables_for_database_returns_none_without_filter() {
        let filter = ReplicationFilter::empty();
        let tables = filter.tables_for_database("mydb");
        assert!(tables.is_none());
    }

    #[test]
    fn test_tables_for_database_returns_none_for_unmatched_db() {
        let filter = ReplicationFilter::new(
            None,
            None,
            Some(vec!["otherdb.users".to_string()]),
            None,
        )
        .unwrap();

        let tables = filter.tables_for_database("mydb");
        assert!(tables.is_none());
    }

    #[test]
    fn test_tables_for_database_preserves_schema() {
        let filter = ReplicationFilter::new(
            None,
            None,
            Some(vec!["mydb.analytics.events".to_string()]),
            None,
        )
        .unwrap();

        let tables = filter.tables_for_database("mydb");
        assert!(tables.is_some());
        let tables = tables.unwrap();
        assert_eq!(tables.len(), 1);
        assert!(tables.contains(&"analytics.events".to_string()));
    }
}
