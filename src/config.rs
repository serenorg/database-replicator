// ABOUTME: Parses replication configuration files for table-level rules
// ABOUTME: Converts TOML format into TableRules structures

use crate::table_rules::{QualifiedTable, TableRules};
use anyhow::{Context, Result};
use serde::Deserialize;
use std::collections::HashMap;
use std::fs;

#[derive(Debug, Deserialize)]
struct ReplicationConfig {
    #[serde(default)]
    databases: HashMap<String, DatabaseConfig>,
}

#[derive(Debug, Deserialize, Default)]
struct DatabaseConfig {
    #[serde(default)]
    schema_only: Vec<String>,
    #[serde(default)]
    table_filters: Vec<TableFilterConfig>,
    #[serde(default)]
    time_filters: Vec<TimeFilterConfig>,
}

#[derive(Debug, Deserialize)]
struct TableFilterConfig {
    table: String,
    #[serde(default)]
    schema: Option<String>,
    #[serde(rename = "where")]
    predicate: String,
}

#[derive(Debug, Deserialize)]
struct TimeFilterConfig {
    table: String,
    #[serde(default)]
    schema: Option<String>,
    column: String,
    last: String,
}

pub fn load_table_rules_from_file(path: &str) -> Result<TableRules> {
    let raw = fs::read_to_string(path)
        .with_context(|| format!("Failed to read config file at {}", path))?;
    let parsed: ReplicationConfig =
        toml::from_str(&raw).with_context(|| format!("Failed to parse TOML config at {}", path))?;

    let mut rules = TableRules::default();
    for (db_name, db) in parsed.databases {
        for table in db.schema_only {
            let qualified = QualifiedTable::parse(&table)?.with_database(Some(db_name.clone()));
            rules.add_schema_only_table(qualified)?;
        }
        for filter in db.table_filters {
            // If explicit schema field is provided, use it; otherwise parse from table name
            let qualified = if let Some(schema) = filter.schema {
                QualifiedTable::new(Some(db_name.clone()), schema, filter.table)
            } else {
                // Parse table name which might be "schema.table" or just "table"
                QualifiedTable::parse(&filter.table)?.with_database(Some(db_name.clone()))
            };
            rules.add_table_filter(qualified, filter.predicate)?;
        }
        for filter in db.time_filters {
            // If explicit schema field is provided, use it; otherwise parse from table name
            let qualified = if let Some(schema) = filter.schema {
                QualifiedTable::new(Some(db_name.clone()), schema, filter.table)
            } else {
                // Parse table name which might be "schema.table" or just "table"
                QualifiedTable::parse(&filter.table)?.with_database(Some(db_name.clone()))
            };
            rules.add_time_filter(qualified, filter.column, filter.last)?;
        }
    }

    Ok(rules)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    #[test]
    fn parse_sample_config() {
        let mut tmp = NamedTempFile::new().unwrap();
        let contents = r#"
            [databases.kong]
            schema_only = ["evmlog_strides", "price"]

            [[databases.kong.table_filters]]
            table = "output"
            where = "series_time >= NOW() - INTERVAL '6 months'"

            [[databases.kong.time_filters]]
            table = "metrics"
            column = "created_at"
            last = "1 year"
        "#;
        use std::io::Write;
        write!(tmp, "{}", contents).unwrap();

        let rules = load_table_rules_from_file(tmp.path().to_str().unwrap()).unwrap();
        assert_eq!(
            rules.schema_only_tables("kong"),
            vec!["\"public\".\"evmlog_strides\"", "\"public\".\"price\""]
        );
        assert!(rules.table_filter("kong", "public", "output").is_some());
        assert!(rules.time_filter("kong", "public", "metrics").is_some());
    }

    #[test]
    fn test_toml_with_explicit_schema() {
        let mut tmp = NamedTempFile::new().unwrap();
        let contents = r#"
            [databases.db1]

            [[databases.db1.table_filters]]
            table = "orders"
            schema = "analytics"
            where = "created_at > NOW() - INTERVAL '1 day'"

            [[databases.db1.time_filters]]
            table = "metrics"
            schema = "reporting"
            column = "timestamp"
            last = "6 months"
        "#;
        use std::io::Write;
        write!(tmp, "{}", contents).unwrap();

        let rules = load_table_rules_from_file(tmp.path().to_str().unwrap()).unwrap();
        // Should use explicit schema field
        assert!(rules.table_filter("db1", "analytics", "orders").is_some());
        assert!(rules.time_filter("db1", "reporting", "metrics").is_some());
    }

    #[test]
    fn test_toml_backward_compatibility() {
        let mut tmp = NamedTempFile::new().unwrap();
        let contents = r#"
            [databases.db1]

            [[databases.db1.table_filters]]
            table = "orders"
            where = "created_at > NOW() - INTERVAL '1 day'"
        "#;
        use std::io::Write;
        write!(tmp, "{}", contents).unwrap();

        let rules = load_table_rules_from_file(tmp.path().to_str().unwrap()).unwrap();
        // Should default to public schema when schema field not provided
        assert!(rules.table_filter("db1", "public", "orders").is_some());
    }

    #[test]
    fn test_toml_mixed_notation() {
        let mut tmp = NamedTempFile::new().unwrap();
        let contents = r#"
            [databases.db1]
            schema_only = ["analytics.large_table", "public.temp"]

            [[databases.db1.table_filters]]
            table = "events"
            schema = "analytics"
            where = "created_at > NOW() - INTERVAL '90 days'"

            [[databases.db1.table_filters]]
            table = "logs"
            where = "timestamp > NOW() - INTERVAL '7 days'"
        "#;
        use std::io::Write;
        write!(tmp, "{}", contents).unwrap();

        let rules = load_table_rules_from_file(tmp.path().to_str().unwrap()).unwrap();

        // Check schema_only with dot notation
        let schema_only = rules.schema_only_tables("db1");
        assert!(schema_only.contains(&"\"analytics\".\"large_table\"".to_string()));
        assert!(schema_only.contains(&"\"public\".\"temp\"".to_string()));

        // Check explicit schema field
        assert!(rules.table_filter("db1", "analytics", "events").is_some());

        // Check default to public when no schema
        assert!(rules.table_filter("db1", "public", "logs").is_some());
    }
}
