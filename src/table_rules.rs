// ABOUTME: Table-level replication rules for schema-only and filtered copies
// ABOUTME: Supports CLI/config inputs and deterministic fingerprints

use crate::utils;
use crate::utils::quote_ident;
use anyhow::{anyhow, bail, Context, Result};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};

/// Represents a fully-qualified table identifier with optional database and schema
/// Supports parsing from: `database.schema.table`, `schema.table`, or `table`
/// Defaults to `public` schema for backward compatibility
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct QualifiedTable {
    pub database: Option<String>,
    pub schema: String,
    pub table: String,
}

impl QualifiedTable {
    /// Parse a table specification from CLI or config
    /// Formats: `database.schema.table`, `schema.table`, or `table`
    /// Defaults to `public` schema if not specified
    pub fn parse(spec: &str) -> Result<Self> {
        let trimmed = spec.trim();
        if trimmed.is_empty() {
            bail!("Table specification cannot be empty");
        }

        let parts: Vec<&str> = trimmed.split('.').collect();
        match parts.len() {
            1 => {
                // Just table name: defaults to public schema, no database
                let table = non_empty(parts[0], "table")?;
                utils::validate_postgres_identifier(&table)?;
                Ok(QualifiedTable {
                    database: None,
                    schema: "public".to_string(),
                    table,
                })
            }
            2 => {
                // schema.table OR database.table
                // We treat as schema.table for consistency
                // Can be disambiguated later if database is provided separately
                let first = non_empty(parts[0], "schema")?;
                let second = non_empty(parts[1], "table")?;
                utils::validate_postgres_identifier(&first)?;
                utils::validate_postgres_identifier(&second)?;
                Ok(QualifiedTable {
                    database: None,
                    schema: first,
                    table: second,
                })
            }
            3 => {
                // database.schema.table
                let database = non_empty(parts[0], "database")?;
                let schema = non_empty(parts[1], "schema")?;
                let table = non_empty(parts[2], "table")?;
                utils::validate_postgres_identifier(&database)?;
                utils::validate_postgres_identifier(&schema)?;
                utils::validate_postgres_identifier(&table)?;
                Ok(QualifiedTable {
                    database: Some(database),
                    schema,
                    table,
                })
            }
            _ => bail!(
                "Invalid table specification '{}': must be 'table', 'schema.table', or 'database.schema.table'",
                spec
            ),
        }
    }

    /// Create from explicit database, schema, and table names
    pub fn new(database: Option<String>, schema: String, table: String) -> Self {
        QualifiedTable {
            database,
            schema,
            table,
        }
    }

    /// Set the database if not already set (for resolving ambiguous 2-part names)
    pub fn with_database(mut self, database: Option<String>) -> Self {
        if self.database.is_none() {
            self.database = database;
        }
        self
    }

    /// Get the schema-qualified table name (schema.table)
    pub fn schema_qualified(&self) -> String {
        format!("{}.{}", quote_ident(&self.schema), quote_ident(&self.table))
    }

    /// Get the fully-qualified name if database is present
    pub fn fully_qualified(&self) -> String {
        match &self.database {
            Some(db) => format!(
                "{}.{}.{}",
                quote_ident(db),
                quote_ident(&self.schema),
                quote_ident(&self.table)
            ),
            None => self.schema_qualified(),
        }
    }

    /// Check if this matches a given database
    pub fn matches_database(&self, database: &str) -> bool {
        match &self.database {
            Some(db) => db == database,
            None => true, // No database specified means it applies to all databases
        }
    }
}

/// Internal key for storing table rules with schema information
/// Used to distinguish tables with the same name in different schemas
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct SchemaTableKey {
    schema: String,
    table: String,
}

impl SchemaTableKey {
    /// Create from a QualifiedTable
    fn from_qualified(qualified: &QualifiedTable) -> Self {
        SchemaTableKey {
            schema: qualified.schema.clone(),
            table: qualified.table.clone(),
        }
    }

    /// Create from parts, defaulting to 'public' schema if not specified
    fn from_parts(schema: Option<&str>, table: &str) -> Self {
        SchemaTableKey {
            schema: schema.unwrap_or("public").to_string(),
            table: table.to_string(),
        }
    }

    /// Get the schema-qualified table name (schema.table)
    fn schema_qualified(&self) -> String {
        format!("{}.{}", quote_ident(&self.schema), quote_ident(&self.table))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TimeFilterRule {
    pub column: String,
    pub interval: String,
}

impl TimeFilterRule {
    fn predicate(&self) -> String {
        format!(
            "{} >= NOW() - INTERVAL '{}'",
            quote_ident(&self.column),
            self.interval
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TableRuleKind {
    SchemaOnly,
    Predicate(String),
}

#[derive(Debug, Clone, Default)]
pub struct TableRules {
    schema_only: ScopedTableSet,
    table_filters: ScopedTableMap<String>,
    time_filters: ScopedTableMap<TimeFilterRule>,
}

type ScopedTableSet = BTreeMap<ScopeKey, BTreeSet<SchemaTableKey>>;
type ScopedTableMap<V> = BTreeMap<ScopeKey, BTreeMap<SchemaTableKey, V>>;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum ScopeKey {
    Global,
    Database(String),
}

impl ScopeKey {
    fn from_option(value: Option<String>) -> Self {
        match value {
            Some(db) => ScopeKey::Database(db),
            None => ScopeKey::Global,
        }
    }

    fn database(database: &str) -> Self {
        ScopeKey::Database(database.to_string())
    }
}

impl TableRules {
    pub fn add_schema_only_table(&mut self, qualified: QualifiedTable) -> Result<()> {
        let scope = ScopeKey::from_option(qualified.database.clone());
        let key = SchemaTableKey::from_qualified(&qualified);
        self.schema_only.entry(scope).or_default().insert(key);
        Ok(())
    }

    pub fn add_table_filter(&mut self, qualified: QualifiedTable, predicate: String) -> Result<()> {
        if predicate.trim().is_empty() {
            bail!(
                "Table filter predicate cannot be empty for '{}'",
                qualified.schema_qualified()
            );
        }
        let scope = ScopeKey::from_option(qualified.database.clone());
        let key = SchemaTableKey::from_qualified(&qualified);
        ensure_schema_only_free(&self.schema_only, &qualified, "table filter")?;
        self.table_filters
            .entry(scope)
            .or_default()
            .insert(key, predicate);
        Ok(())
    }

    pub fn add_time_filter(
        &mut self,
        qualified: QualifiedTable,
        column: String,
        window: String,
    ) -> Result<()> {
        utils::validate_postgres_identifier(&column)?;
        let interval = normalize_time_window(&window)?;
        let scope = ScopeKey::from_option(qualified.database.clone());
        let key = SchemaTableKey::from_qualified(&qualified);
        ensure_schema_only_free(&self.schema_only, &qualified, "time filter")?;
        if self
            .table_filters
            .get(&scope)
            .and_then(|inner| inner.get(&key))
            .is_some()
        {
            bail!(
                "Cannot apply time filter to table '{}' because a table filter already exists",
                qualified.schema_qualified()
            );
        }
        self.time_filters
            .entry(scope)
            .or_default()
            .insert(key, TimeFilterRule { column, interval });
        Ok(())
    }

    pub fn apply_schema_only_cli(&mut self, specs: &[String]) -> Result<()> {
        for spec in specs {
            let qualified = QualifiedTable::parse(spec)?;
            self.add_schema_only_table(qualified)?;
        }
        Ok(())
    }

    pub fn apply_table_filter_cli(&mut self, specs: &[String]) -> Result<()> {
        for spec in specs {
            let (table_part, predicate) = spec
                .split_once(':')
                .with_context(|| format!("Table filter '{}' missing ':' separator", spec))?;
            if predicate.trim().is_empty() {
                bail!("Table filter '{}' must include a predicate after ':'", spec);
            }
            let qualified = QualifiedTable::parse(table_part)?;
            self.add_table_filter(qualified, predicate.trim().to_string())?;
        }
        Ok(())
    }

    pub fn apply_time_filter_cli(&mut self, specs: &[String]) -> Result<()> {
        for spec in specs {
            let (table_part, rest) = spec
                .split_once(':')
                .with_context(|| format!("Time filter '{}' missing second ':'", spec))?;
            let (column, window) = rest
                .split_once(':')
                .with_context(|| format!("Time filter '{}' must be table:column:window", spec))?;
            if column.trim().is_empty() || window.trim().is_empty() {
                bail!(
                    "Time filter '{}' must include non-empty column and window",
                    spec
                );
            }
            let qualified = QualifiedTable::parse(table_part)?;
            self.add_time_filter(
                qualified,
                column.trim().to_string(),
                window.trim().to_string(),
            )?;
        }
        Ok(())
    }

    pub fn schema_only_tables(&self, database: &str) -> Vec<String> {
        collect_tables(&self.schema_only, database)
    }

    pub fn table_filter(&self, database: &str, schema: &str, table: &str) -> Option<&String> {
        lookup_scoped(&self.table_filters, database, schema, table)
    }

    pub fn time_filter(
        &self,
        database: &str,
        schema: &str,
        table: &str,
    ) -> Option<&TimeFilterRule> {
        lookup_scoped(&self.time_filters, database, schema, table)
    }

    pub fn predicate_tables(&self, database: &str) -> Vec<(String, String)> {
        let schema_only: BTreeSet<String> = self.schema_only_tables(database).into_iter().collect();
        let mut combined = BTreeMap::new();

        for (table, predicate) in scoped_map_values(&self.table_filters, database) {
            if schema_only.contains(&table) {
                continue;
            }
            combined.insert(table, predicate);
        }

        for (table, rule) in scoped_map_values(&self.time_filters, database) {
            if schema_only.contains(&table) || combined.contains_key(&table) {
                continue;
            }
            combined.insert(table.clone(), rule.predicate());
        }

        combined.into_iter().collect()
    }

    pub fn rule_for_table(
        &self,
        database: &str,
        schema: &str,
        table: &str,
    ) -> Option<TableRuleKind> {
        if has_schema_only_rule(&self.schema_only, database, schema, table) {
            return Some(TableRuleKind::SchemaOnly);
        }
        if let Some(predicate) = self.table_filter(database, schema, table) {
            return Some(TableRuleKind::Predicate(predicate.clone()));
        }
        if let Some(rule) = self.time_filter(database, schema, table) {
            return Some(TableRuleKind::Predicate(rule.predicate()));
        }
        None
    }

    pub fn merge(&mut self, other: TableRules) {
        merge_sets(&mut self.schema_only, other.schema_only);
        merge_maps(&mut self.table_filters, other.table_filters);
        merge_maps(&mut self.time_filters, other.time_filters);
    }

    pub fn fingerprint(&self) -> String {
        let mut hasher = Sha256::new();
        hash_scoped_set(&mut hasher, &self.schema_only);
        hash_scoped_map(&mut hasher, &self.table_filters, |value| value.clone());
        hash_scoped_map(&mut hasher, &self.time_filters, |value| {
            format!("{}|{}", value.column, value.interval)
        });
        format!("{:x}", hasher.finalize())
    }

    pub fn is_empty(&self) -> bool {
        self.schema_only.is_empty() && self.table_filters.is_empty() && self.time_filters.is_empty()
    }
}

fn non_empty(value: &str, label: &str) -> Result<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        bail!("{} name cannot be empty", label);
    }
    Ok(trimmed.to_string())
}

fn collect_tables(map: &ScopedTableSet, database: &str) -> Vec<String> {
    let mut tables = BTreeSet::new();
    if let Some(global) = map.get(&ScopeKey::Global) {
        for key in global {
            tables.insert(key.schema_qualified());
        }
    }
    let scoped = ScopeKey::database(database);
    if let Some(specific) = map.get(&scoped) {
        for key in specific {
            tables.insert(key.schema_qualified());
        }
    }
    tables.into_iter().collect()
}

fn lookup_scoped<'a, V>(
    map: &'a ScopedTableMap<V>,
    database: &str,
    schema: &str,
    table: &str,
) -> Option<&'a V> {
    let key = SchemaTableKey::from_parts(Some(schema), table);
    let scoped = ScopeKey::database(database);
    map.get(&scoped)
        .and_then(|inner| inner.get(&key))
        .or_else(|| map.get(&ScopeKey::Global).and_then(|inner| inner.get(&key)))
}

fn scoped_map_values<V: Clone>(map: &ScopedTableMap<V>, database: &str) -> BTreeMap<String, V> {
    let mut values = BTreeMap::new();
    if let Some(global) = map.get(&ScopeKey::Global) {
        for (key, value) in global {
            values.insert(key.schema_qualified(), value.clone());
        }
    }
    if let Some(specific) = map.get(&ScopeKey::database(database)) {
        for (key, value) in specific {
            values.insert(key.schema_qualified(), value.clone());
        }
    }
    values
}

fn has_schema_only_rule(
    schema_only: &ScopedTableSet,
    database: &str,
    schema: &str,
    table: &str,
) -> bool {
    let key = SchemaTableKey::from_parts(Some(schema), table);
    schema_only
        .get(&ScopeKey::Global)
        .is_some_and(|set| set.contains(&key))
        || schema_only
            .get(&ScopeKey::database(database))
            .is_some_and(|set| set.contains(&key))
}

fn ensure_schema_only_free(
    schema_only: &ScopedTableSet,
    qualified: &QualifiedTable,
    rule_name: &str,
) -> Result<()> {
    let key = SchemaTableKey::from_qualified(qualified);
    if schema_only
        .get(&ScopeKey::Global)
        .is_some_and(|set| set.contains(&key))
    {
        bail!(
            "Cannot apply {} to table '{}' because it is marked schema-only globally",
            rule_name,
            qualified.schema_qualified()
        );
    }
    if let Some(db) = &qualified.database {
        if schema_only
            .get(&ScopeKey::database(db))
            .is_some_and(|set| set.contains(&key))
        {
            bail!(
                "Cannot apply {} to table '{}' in database '{}' because it is schema-only",
                rule_name,
                qualified.schema_qualified(),
                db
            );
        }
    }
    Ok(())
}

fn normalize_time_window(window: &str) -> Result<String> {
    let trimmed = window.trim();
    let mut parts = trimmed.split_whitespace();
    let amount_str = parts
        .next()
        .ok_or_else(|| anyhow!("Time filter window '{}' missing amount", window))?;
    let unit_str = parts
        .next()
        .ok_or_else(|| anyhow!("Time filter window '{}' missing unit", window))?;
    if parts.next().is_some() {
        bail!("Time filter window '{}' must be '<amount> <unit>'", window);
    }

    let amount: i64 = amount_str.parse().with_context(|| {
        format!(
            "Invalid time window amount '{}': must be integer",
            amount_str
        )
    })?;
    if amount <= 0 {
        bail!("Time window amount must be positive, got {}", amount);
    }

    let unit = match unit_str.to_lowercase().as_str() {
        "second" | "seconds" | "sec" | "secs" => "second",
        "minute" | "minutes" | "min" | "mins" => "minute",
        "hour" | "hours" | "hr" | "hrs" => "hour",
        "day" | "days" => "day",
        "week" | "weeks" => "week",
        "month" | "months" | "mon" | "mons" => "month",
        "year" | "years" | "yr" | "yrs" => "year",
        other => bail!(
            "Unsupported time window unit '{}'. Use seconds/minutes/hours/days/weeks/months/years",
            other
        ),
    };

    Ok(format!("{} {}", amount, unit))
}

fn merge_sets(target: &mut ScopedTableSet, source: ScopedTableSet) {
    for (scope, tables) in source {
        target.entry(scope).or_default().extend(tables);
    }
}

fn merge_maps<V: Clone>(target: &mut ScopedTableMap<V>, source: ScopedTableMap<V>) {
    for (scope, tables) in source {
        let entry = target.entry(scope).or_default();
        for (table, value) in tables {
            entry.insert(table, value);
        }
    }
}

fn hash_scoped_set(hasher: &mut Sha256, data: &ScopedTableSet) {
    for (scope, tables) in data {
        hash_scope_label(hasher, scope);
        for key in tables {
            hasher.update(key.schema.as_bytes());
            hasher.update(b".");
            hasher.update(key.table.as_bytes());
            hasher.update(b"|");
        }
    }
}

fn hash_scoped_map<V, F>(hasher: &mut Sha256, data: &ScopedTableMap<V>, mut encode: F)
where
    F: FnMut(&V) -> String,
{
    for (scope, tables) in data {
        hash_scope_label(hasher, scope);
        for (key, value) in tables {
            hasher.update(key.schema.as_bytes());
            hasher.update(b".");
            hasher.update(key.table.as_bytes());
            hasher.update(b"=");
            hasher.update(encode(value).as_bytes());
            hasher.update(b"|");
        }
    }
}

fn hash_scope_label(hasher: &mut Sha256, scope: &ScopeKey) {
    match scope {
        ScopeKey::Database(db) => {
            hasher.update(b"db:");
            hasher.update(db.as_bytes());
        }
        ScopeKey::Global => {
            hasher.update(b"global");
        }
    }
    hasher.update(b"#");
}

#[cfg(test)]
mod tests {
    use super::*;

    // QualifiedTable tests
    #[test]
    fn test_qualified_table_parse_single_part() {
        // Single part defaults to public schema
        let t = QualifiedTable::parse("users").unwrap();
        assert_eq!(t.database, None);
        assert_eq!(t.schema, "public");
        assert_eq!(t.table, "users");
    }

    #[test]
    fn test_qualified_table_parse_two_parts() {
        // Two parts is schema.table
        let t = QualifiedTable::parse("analytics.orders").unwrap();
        assert_eq!(t.database, None);
        assert_eq!(t.schema, "analytics");
        assert_eq!(t.table, "orders");
    }

    #[test]
    fn test_qualified_table_parse_three_parts() {
        // Three parts is database.schema.table
        let t = QualifiedTable::parse("db1.public.users").unwrap();
        assert_eq!(t.database, Some("db1".to_string()));
        assert_eq!(t.schema, "public");
        assert_eq!(t.table, "users");
    }

    #[test]
    fn test_qualified_table_parse_empty() {
        let result = QualifiedTable::parse("");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("cannot be empty"));
    }

    #[test]
    fn test_qualified_table_parse_whitespace() {
        let result = QualifiedTable::parse("   ");
        assert!(result.is_err());
    }

    #[test]
    fn test_qualified_table_parse_too_many_parts() {
        let result = QualifiedTable::parse("a.b.c.d");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("must be"));
    }

    #[test]
    fn test_qualified_table_schema_qualified() {
        let t = QualifiedTable::new(None, "analytics".into(), "orders".into());
        assert_eq!(t.schema_qualified(), "\"analytics\".\"orders\"");
    }

    #[test]
    fn test_qualified_table_fully_qualified_with_database() {
        let t = QualifiedTable::new(Some("db1".into()), "public".into(), "users".into());
        assert_eq!(t.fully_qualified(), "\"db1\".\"public\".\"users\"");
    }

    #[test]
    fn test_qualified_table_fully_qualified_without_database() {
        let t = QualifiedTable::new(None, "analytics".into(), "orders".into());
        assert_eq!(t.fully_qualified(), "\"analytics\".\"orders\"");
    }

    #[test]
    fn test_qualified_table_with_database() {
        let t = QualifiedTable::parse("analytics.orders")
            .unwrap()
            .with_database(Some("db1".to_string()));
        assert_eq!(t.database, Some("db1".to_string()));
        assert_eq!(t.schema, "analytics");
        assert_eq!(t.table, "orders");
    }

    #[test]
    fn test_qualified_table_with_database_no_override() {
        let t = QualifiedTable::parse("db1.analytics.orders")
            .unwrap()
            .with_database(Some("db2".to_string()));
        // Should not override existing database
        assert_eq!(t.database, Some("db1".to_string()));
    }

    #[test]
    fn test_qualified_table_matches_database() {
        let t = QualifiedTable::new(Some("db1".into()), "public".into(), "users".into());
        assert!(t.matches_database("db1"));
        assert!(!t.matches_database("db2"));
    }

    #[test]
    fn test_qualified_table_matches_database_no_database() {
        let t = QualifiedTable::new(None, "public".into(), "users".into());
        // No database specified means matches all
        assert!(t.matches_database("db1"));
        assert!(t.matches_database("db2"));
        assert!(t.matches_database("any_db"));
    }

    #[test]
    fn test_qualified_table_new() {
        let t = QualifiedTable::new(Some("db1".into()), "analytics".into(), "metrics".into());
        assert_eq!(t.database, Some("db1".to_string()));
        assert_eq!(t.schema, "analytics");
        assert_eq!(t.table, "metrics");
    }

    #[test]
    fn test_qualified_table_ordering() {
        let t1 = QualifiedTable::new(None, "analytics".into(), "orders".into());
        let t2 = QualifiedTable::new(None, "public".into(), "users".into());
        let t3 = QualifiedTable::new(None, "analytics".into(), "metrics".into());

        // Should be comparable and orderable
        assert!(t1 < t2); // analytics < public
        assert!(t3 < t1); // metrics < orders
    }

    #[test]
    fn cli_schema_only_parsing() {
        let mut rules = TableRules::default();
        // "db1.orders" is parsed as schema=db1, table=orders (not database.table!)
        // "invoices" is parsed as schema=public, table=invoices
        rules
            .apply_schema_only_cli(&["analytics.orders".to_string(), "invoices".to_string()])
            .unwrap();
        let tables = rules.schema_only_tables("anydb");
        // Both are global scope (no database specified), so they apply to all databases
        assert!(tables.contains(&"\"analytics\".\"orders\"".to_string()));
        assert!(tables.contains(&"\"public\".\"invoices\"".to_string()));
    }

    #[test]
    fn cli_table_filter_parsing() {
        let mut rules = TableRules::default();
        // "analytics.logs" is parsed as schema=analytics, table=logs
        rules
            .apply_table_filter_cli(
                &["analytics.logs:created_at > NOW() - INTERVAL '1 day'".into()],
            )
            .unwrap();
        assert!(rules
            .table_filter("anydb", "analytics", "logs")
            .unwrap()
            .contains("created_at"));
    }

    #[test]
    fn cli_time_filter_parsing() {
        let mut rules = TableRules::default();
        rules
            .apply_time_filter_cli(&["metrics:created_at:6 months".into()])
            .unwrap();
        let tf = rules.time_filter("any", "public", "metrics").unwrap();
        assert_eq!(tf.column, "created_at");
        assert_eq!(tf.interval, "6 month");
    }

    #[test]
    fn fingerprint_changes_with_rules() {
        let mut rules_a = TableRules::default();
        rules_a
            .apply_schema_only_cli(&["db.table".to_string()])
            .unwrap();
        let mut rules_b = TableRules::default();
        assert_ne!(rules_a.fingerprint(), rules_b.fingerprint());
        rules_b
            .apply_schema_only_cli(&["db.table".to_string()])
            .unwrap();
        assert_eq!(rules_a.fingerprint(), rules_b.fingerprint());
    }

    #[test]
    fn schema_only_conflicts_with_filters() {
        let mut rules = TableRules::default();
        rules
            .apply_schema_only_cli(&["db1.audit".to_string()])
            .unwrap();
        assert!(rules
            .apply_table_filter_cli(&["db1.audit:1=1".to_string()])
            .is_err());
    }

    #[test]
    fn predicate_tables_include_time_filters() {
        let mut rules = TableRules::default();
        rules
            .apply_time_filter_cli(&["db1.metrics:created_at:6 months".into()])
            .unwrap();
        let predicates = rules.predicate_tables("db1");
        assert_eq!(predicates.len(), 1);
        assert!(predicates[0].1.contains("INTERVAL '6 month'"));
    }

    #[test]
    fn test_fingerprint_changes_with_schema() {
        // Different schemas should produce different fingerprints
        let mut rules_a = TableRules::default();
        rules_a
            .apply_schema_only_cli(&["public.orders".to_string()])
            .unwrap();

        let mut rules_b = TableRules::default();
        rules_b
            .apply_schema_only_cli(&["analytics.orders".to_string()])
            .unwrap();

        assert_ne!(
            rules_a.fingerprint(),
            rules_b.fingerprint(),
            "Different schemas should produce different fingerprints"
        );
    }

    #[test]
    fn test_fingerprint_stable_with_order() {
        // Same tables, different order -> same fingerprint (BTreeSet sorts)
        let mut rules_a = TableRules::default();
        rules_a
            .apply_schema_only_cli(&["public.users".to_string(), "public.orders".to_string()])
            .unwrap();

        let mut rules_b = TableRules::default();
        rules_b
            .apply_schema_only_cli(&[
                "public.orders".to_string(), // Different order
                "public.users".to_string(),
            ])
            .unwrap();

        assert_eq!(
            rules_a.fingerprint(),
            rules_b.fingerprint(),
            "Same tables in different order should produce same fingerprint"
        );
    }

    #[test]
    fn test_fingerprint_includes_table_filter_schema() {
        // Table filters with different schemas should produce different fingerprints
        let mut rules_a = TableRules::default();
        rules_a
            .apply_table_filter_cli(&["public.logs:created_at > NOW()".to_string()])
            .unwrap();

        let mut rules_b = TableRules::default();
        rules_b
            .apply_table_filter_cli(&["analytics.logs:created_at > NOW()".to_string()])
            .unwrap();

        assert_ne!(
            rules_a.fingerprint(),
            rules_b.fingerprint(),
            "Table filters with different schemas should produce different fingerprints"
        );
    }

    #[test]
    fn test_fingerprint_includes_time_filter_schema() {
        // Time filters with different schemas should produce different fingerprints
        let mut rules_a = TableRules::default();
        rules_a
            .apply_time_filter_cli(&["public.metrics:timestamp:1 year".to_string()])
            .unwrap();

        let mut rules_b = TableRules::default();
        rules_b
            .apply_time_filter_cli(&["reporting.metrics:timestamp:1 year".to_string()])
            .unwrap();

        assert_ne!(
            rules_a.fingerprint(),
            rules_b.fingerprint(),
            "Time filters with different schemas should produce different fingerprints"
        );
    }
}
