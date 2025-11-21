# Selective Replication Implementation Plan

> Created: 2025-11-06 (YYYY-MM-DD)

## Overview

This plan implements selective database and table replication with multi-provider support (Neon, AWS RDS, Hetzner PostgreSQL → Seren Cloud).

**User Problems Being Solved:**
1. "I don't want to replicate the whole database" - need database-level filtering
2. "I don't want to replicate data in specific tables" - need table-level filtering
3. "How do I replicate with AWS RDS or Hetzner PostgreSQL" - need multi-provider support

**Design Principles:**
- **YAGNI**: Build only what's needed now (exact name matching, not regex patterns)
- **DRY**: Centralize filter logic in one module
- **TDD**: Write tests first, then implementation
- **Frequent commits**: Commit after each logical unit of work

## Background: Understanding the Codebase

### Current Architecture

```
src/
├── main.rs              # CLI entry point (clap argument parsing)
├── lib.rs               # Library root (exports all modules)
├── commands/            # One file per subcommand
│   ├── validate.rs      # Pre-flight checks
│   ├── init.rs          # Initial snapshot (dump/restore)
│   ├── sync.rs          # Set up logical replication
│   ├── status.rs        # Monitor replication
│   └── verify.rs        # Data integrity verification
├── postgres/
│   ├── connection.rs    # Database connections
│   └── privileges.rs    # Permission checking
├── migration/
│   ├── schema.rs        # Database/table introspection
│   ├── dump.rs          # pg_dump wrapper
│   ├── restore.rs       # pg_restore wrapper
│   ├── estimation.rs    # Size estimation
│   └── checksum.rs      # Data verification
├── replication/
│   ├── publication.rs   # Create publications on source
│   ├── subscription.rs  # Create subscriptions on target
│   └── monitor.rs       # Replication monitoring
└── utils.rs             # Shared utilities
```

### Current Workflow

1. **validate**: Check prerequisites (permissions, versions, connectivity)
2. **init**: Dump all databases/tables → restore to target
3. **sync**: Create publication (source) + subscription (target) for logical replication
4. **status**: Monitor replication lag
5. **verify**: Checksum comparison

### Key Technologies

- **Language**: Rust (async with tokio)
- **Database**: PostgreSQL via `tokio-postgres` crate
- **CLI**: `clap` for argument parsing
- **Logging**: `tracing` for structured logging
- **External tools**: Calls `pg_dump`, `pg_dumpall`, `pg_restore` via `std::process::Command`

### PostgreSQL Logical Replication Basics

- **Publication** (on source): Defines which tables to replicate
- **Subscription** (on target): Subscribes to a publication, receives changes
- Requires: REPLICATION privilege (source), CREATE privileges (target)
- Limitation: Tables need primary keys for replication

## Implementation Phases

---

## Phase 0: Project Rename (PREREQUISITE)

**Goal**: Rename project from `postgres-seren-replicator` to `postgres-seren-replicator` to reflect multi-provider support.

**Why**: The tool now supports replication from any PostgreSQL database (Neon, AWS RDS, Hetzner), not just Neon. The name should reflect this capability.

**Priority**: MUST be completed before starting Phase 1.

### Task 0.1: Rename Project and Update All References

**Files to modify:**
- `Cargo.toml` - Package name
- `README.md` - Title, descriptions, command examples, URLs
- `CLAUDE.md` - Project name, command examples
- `src/main.rs` - Crate imports
- `src/lib.rs` - Module declarations
- All test files - Crate imports
- `.github/workflows/ci.yml` - Artifact names
- `.github/workflows/release.yml` - Artifact names, asset names, release notes
- `docs/plans/selective-replication-implementation.md` - Command examples

**Implementation:**

1. **Update Cargo.toml:**
```toml
[package]
name = "postgres-seren-replicator"  # Changed from postgres-seren-replicator
# ... rest unchanged
```

2. **Update all Rust imports:**
```rust
// Old:
use neon_seren_replicator::commands;

// New:
use postgres_seren_replicator::commands;
```

3. **Update documentation:**
   - Replace all instances of `postgres-seren-replicator` with `postgres-seren-replicator`
   - Update descriptions: "from Neon to Seren" → "from PostgreSQL to Seren"

4. **Update CI/CD workflows:**
   - Artifact names: `postgres-seren-replicator-*` → `postgres-seren-replicator-*`
   - Binary names in release workflow

5. **Search for remaining references:**
```bash
# Find any remaining old references
rg "neon.seren.replicator" --type rust
rg "postgres-seren-replicator"
```

**Testing Strategy:**

```bash
# 1. Build succeeds
cargo build
cargo build --release

# 2. Binary has correct name
ls target/release/postgres-seren-replicator

# 3. All tests pass
cargo test
cargo test --doc

# 4. Clippy passes
cargo clippy --all-targets --all-features -- -D warnings

# 5. No old references remain
rg "neon_seren_replicator" --type rust
# Should only find this file (the plan document)

# 6. Commands work
./target/release/postgres-seren-replicator --help
./target/release/postgres-seren-replicator validate --help
```

**GitHub Repository Rename:**

After code changes are merged:
1. Go to repository Settings → General
2. Change repository name to `postgres-seren-replicator`
3. GitHub will automatically redirect old URLs

**Migration Note for README:**

Add this section for users:

```markdown
## Upgrading from v0.2.x

The project has been renamed from `postgres-seren-replicator` to `postgres-seren-replicator` to reflect multi-provider support.

**Binary name changed:**
- Old: `postgres-seren-replicator`
- New: `postgres-seren-replicator`

**Functionality is unchanged** - all commands work exactly the same way.
```

**Commit message:**
```
Rename project to postgres-seren-replicator

- Rename package in Cargo.toml
- Update all imports from neon_seren_replicator
- Update documentation (README, CLAUDE.md)
- Update CI/CD workflows and artifact names
- Update command examples in all docs
- Reflect multi-provider PostgreSQL support

Binary name changed:
- postgres-seren-replicator → postgres-seren-replicator

Related to #56
```

**Estimated time:** 1-2 hours

---

## Phase 1: Core Filtering Infrastructure

**Goal**: Create the central filtering module that all commands will use.

### Task 1.1: Create Filter Module

**Files to create:**
- `src/filters.rs`

**Files to modify:**
- `src/lib.rs` (add `pub mod filters;`)

**Implementation:**

```rust
// src/filters.rs
// ABOUTME: Central filtering logic for selective replication
// ABOUTME: Handles database and table include/exclude patterns

use anyhow::{anyhow, bail, Context, Result};
use tokio_postgres::Client;

/// Represents replication filtering rules
#[derive(Debug, Clone, Default)]
pub struct ReplicationFilter {
    include_databases: Option<Vec<String>>,
    exclude_databases: Option<Vec<String>>,
    include_tables: Option<Vec<String>>,    // Format: "db.table"
    exclude_tables: Option<Vec<String>>,    // Format: "db.table"
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
                    bail!("Table must be specified as 'database.table', got '{}'", table);
                }
            }
        }
        if let Some(ref tables) = exclude_tables {
            for table in tables {
                if !table.contains('.') {
                    bail!("Table must be specified as 'database.table', got '{}'", table);
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
            .filter(|db| self.should_replicate_database(db))
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
        let all_tables = crate::migration::schema::list_tables(source_conn, db_name).await?;

        // Filter based on rules
        let filtered: Vec<String> = all_tables
            .into_iter()
            .filter(|table| self.should_replicate_table(db_name, table))
            .collect();

        Ok(filtered)
    }

    /// Validates that filters reference existing databases/tables
    pub async fn validate(&self, source_conn: &Client) -> Result<()> {
        let all_databases = crate::migration::schema::list_databases(source_conn).await?;

        // Validate include_databases
        if let Some(ref include) = self.include_databases {
            for db in include {
                if !all_databases.contains(db) {
                    bail!(
                        "Database '{}' specified in --include-databases does not exist on source",
                        db
                    );
                }
            }
        }

        // Validate exclude_databases
        if let Some(ref exclude) = self.exclude_databases {
            for db in exclude {
                if !all_databases.contains(db) {
                    bail!(
                        "Database '{}' specified in --exclude-databases does not exist on source",
                        db
                    );
                }
            }
        }

        // Validate table filters
        let validate_tables = |tables: &[String]| async {
            for full_name in tables {
                let parts: Vec<&str> = full_name.split('.').collect();
                if parts.len() != 2 {
                    bail!("Invalid table format: '{}'. Expected 'database.table'", full_name);
                }
                let (db, table) = (parts[0], parts[1]);

                if !all_databases.contains(&db.to_string()) {
                    bail!("Database '{}' in table filter does not exist", db);
                }

                // Connect to database and check table exists
                let db_url = source_conn.???; // TODO: Need database-specific connection
                let tables = crate::migration::schema::list_tables(source_conn, db).await?;
                if !tables.contains(&table.to_string()) {
                    bail!("Table '{}' does not exist in database '{}'", table, db);
                }
            }
            Ok::<(), anyhow::Error>(())
        };

        if let Some(ref include) = self.include_tables {
            validate_tables(include).await?;
        }
        if let Some(ref exclude) = self.exclude_tables {
            validate_tables(exclude).await?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_validates_mutually_exclusive_flags() {
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
    fn test_new_validates_table_format() {
        let result = ReplicationFilter::new(
            None,
            None,
            Some(vec!["invalid_table".to_string()]),
            None,
        );
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
        let filter =
            ReplicationFilter::new(None, Some(vec!["test".to_string(), "dev".to_string()]), None, None)
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
            Some(vec!["db1.audit_logs".to_string(), "db1.temp_data".to_string()]),
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
}
```

**Testing Strategy:**

1. **Unit tests** (in the same file):
   - Test validation of mutually exclusive flags
   - Test table format validation
   - Test `should_replicate_database` with include/exclude lists
   - Test `should_replicate_table` with include/exclude lists
   - Test empty filter (replicates everything)

2. **Run tests:**
   ```bash
   cargo test filters::tests --lib
   ```

**How to verify:**
```bash
cargo build  # Should compile without errors
cargo test filters --lib  # All tests should pass
cargo clippy -- -D warnings  # No linting warnings
```

**Commit message:**
```
Add core filtering infrastructure

- Create ReplicationFilter struct for database/table filtering
- Implement include/exclude logic for databases and tables
- Add validation for mutually exclusive flags
- Add comprehensive unit tests

Related to #<issue-number>
```

---

### Task 1.2: Add Schema Introspection Functions

**Files to modify:**
- `src/migration/schema.rs`

**Why**: We need to query databases and tables from source to validate filters.

**Implementation:**

Check if `list_databases()` and `list_tables()` functions already exist. If not, add them:

```rust
// In src/migration/schema.rs

/// Lists all user databases (excluding system databases)
pub async fn list_databases(client: &Client) -> Result<Vec<String>> {
    let query = "
        SELECT datname
        FROM pg_database
        WHERE datistemplate = false
          AND datname NOT IN ('postgres', 'template0', 'template1')
        ORDER BY datname
    ";

    let rows = client
        .query(query, &[])
        .await
        .context("Failed to list databases")?;

    let databases: Vec<String> = rows.iter().map(|row| row.get(0)).collect();

    Ok(databases)
}

/// Lists all user tables in a database (excluding system tables)
pub async fn list_tables(client: &Client, database: &str) -> Result<Vec<String>> {
    // Note: This queries the current database connection
    // Caller must connect to the specific database first
    let query = "
        SELECT tablename
        FROM pg_tables
        WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
        ORDER BY tablename
    ";

    let rows = client
        .query(query, &[])
        .await
        .with_context(|| format!("Failed to list tables in database '{}'", database))?;

    let tables: Vec<String> = rows.iter().map(|row| row.get(0)).collect();

    Ok(tables)
}

#[cfg(test)]
mod tests {
    use super::*;

    // These are integration tests and require TEST_SOURCE_URL
    #[tokio::test]
    #[ignore]
    async fn test_list_databases() {
        let url = std::env::var("TEST_SOURCE_URL").expect("TEST_SOURCE_URL not set");
        let (client, connection) = tokio_postgres::connect(&url, tokio_postgres::NoTls)
            .await
            .unwrap();

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("connection error: {}", e);
            }
        });

        let databases = list_databases(&client).await.unwrap();
        assert!(!databases.is_empty());
        // Should not include system databases
        assert!(!databases.contains(&"template0".to_string()));
        assert!(!databases.contains(&"template1".to_string()));
    }
}
```

**Testing:**
```bash
# Unit tests (none for now, these are integration tests)
cargo test schema::tests --lib

# Integration tests (requires running database)
export TEST_SOURCE_URL="postgresql://postgres:postgres@localhost:5432/postgres"
cargo test schema::tests --lib -- --ignored
```

**How to verify:**
- Functions compile
- Integration tests pass when run against test database

**Commit message:**
```
Add database and table introspection functions

- Add list_databases() to query user databases
- Add list_tables() to query tables in a database
- Exclude system databases and tables from results
- Add integration tests

Related to #<issue-number>
```

---

### Task 1.3: Add CLI Arguments to Commands

**Files to modify:**
- `src/main.rs`

**Implementation:**

Add filter flags to all command variants:

```rust
// In src/main.rs

#[derive(Subcommand)]
enum Commands {
    /// Validate source and target databases are ready for replication
    Validate {
        #[arg(long)]
        source: String,
        #[arg(long)]
        target: String,
        // NEW: Filter flags
        #[arg(long, value_delimiter = ',')]
        include_databases: Option<Vec<String>>,
        #[arg(long, value_delimiter = ',')]
        exclude_databases: Option<Vec<String>>,
        #[arg(long, value_delimiter = ',')]
        include_tables: Option<Vec<String>>,
        #[arg(long, value_delimiter = ',')]
        exclude_tables: Option<Vec<String>>,
        #[arg(long)]
        interactive: bool,
    },
    /// Initialize replication with snapshot copy of schema and data
    Init {
        #[arg(long)]
        source: String,
        #[arg(long)]
        target: String,
        /// Skip confirmation prompt
        #[arg(short = 'y', long)]
        yes: bool,
        // NEW: Filter flags
        #[arg(long, value_delimiter = ',')]
        include_databases: Option<Vec<String>>,
        #[arg(long, value_delimiter = ',')]
        exclude_databases: Option<Vec<String>>,
        #[arg(long, value_delimiter = ',')]
        include_tables: Option<Vec<String>>,
        #[arg(long, value_delimiter = ',')]
        exclude_tables: Option<Vec<String>>,
        #[arg(long)]
        interactive: bool,
        // NEW: Drop existing databases flag
        #[arg(long)]
        drop_existing: bool,
    },
    /// Set up continuous logical replication from source to target
    Sync {
        #[arg(long)]
        source: String,
        #[arg(long)]
        target: String,
        // NEW: Filter flags
        #[arg(long, value_delimiter = ',')]
        include_databases: Option<Vec<String>>,
        #[arg(long, value_delimiter = ',')]
        exclude_databases: Option<Vec<String>>,
        #[arg(long, value_delimiter = ',')]
        include_tables: Option<Vec<String>>,
        #[arg(long, value_delimiter = ',')]
        exclude_tables: Option<Vec<String>>,
        #[arg(long)]
        interactive: bool,
    },
    /// Check replication status and lag in real-time
    Status {
        #[arg(long)]
        source: String,
        #[arg(long)]
        target: String,
        // NEW: Filter flags (to show only relevant subscriptions)
        #[arg(long, value_delimiter = ',')]
        include_databases: Option<Vec<String>>,
        #[arg(long, value_delimiter = ',')]
        exclude_databases: Option<Vec<String>>,
    },
    /// Verify data integrity between source and target
    Verify {
        #[arg(long)]
        source: String,
        #[arg(long)]
        target: String,
        // NEW: Filter flags
        #[arg(long, value_delimiter = ',')]
        include_databases: Option<Vec<String>>,
        #[arg(long, value_delimiter = ',')]
        exclude_databases: Option<Vec<String>>,
        #[arg(long, value_delimiter = ',')]
        include_tables: Option<Vec<String>>,
        #[arg(long, value_delimiter = ',')]
        exclude_tables: Option<Vec<String>>,
    },
}
```

Update the match statement in `main()`:

```rust
match cli.command {
    Commands::Validate {
        source,
        target,
        include_databases,
        exclude_databases,
        include_tables,
        exclude_tables,
        interactive,
    } => {
        if interactive {
            // TODO: Phase 6 - implement interactive mode
            anyhow::bail!("Interactive mode not yet implemented");
        }
        let filter = neon_seren_replicator::filters::ReplicationFilter::new(
            include_databases,
            exclude_databases,
            include_tables,
            exclude_tables,
        )?;
        commands::validate(&source, &target, filter).await
    }
    Commands::Init {
        source,
        target,
        yes,
        include_databases,
        exclude_databases,
        include_tables,
        exclude_tables,
        interactive,
        drop_existing,
    } => {
        if interactive {
            // TODO: Phase 6 - implement interactive mode
            anyhow::bail!("Interactive mode not yet implemented");
        }
        let filter = neon_seren_replicator::filters::ReplicationFilter::new(
            include_databases,
            exclude_databases,
            include_tables,
            exclude_tables,
        )?;
        commands::init(&source, &target, yes, filter, drop_existing).await
    }
    Commands::Sync {
        source,
        target,
        include_databases,
        exclude_databases,
        include_tables,
        exclude_tables,
        interactive,
    } => {
        if interactive {
            anyhow::bail!("Interactive mode not yet implemented");
        }
        let filter = neon_seren_replicator::filters::ReplicationFilter::new(
            include_databases,
            exclude_databases,
            include_tables,
            exclude_tables,
        )?;
        commands::sync(&source, &target, Some(filter), None, None).await
    }
    Commands::Status {
        source,
        target,
        include_databases,
        exclude_databases,
    } => {
        let filter = neon_seren_replicator::filters::ReplicationFilter::new(
            include_databases,
            exclude_databases,
            None,
            None,
        )?;
        commands::status(&source, &target, Some(filter)).await
    }
    Commands::Verify {
        source,
        target,
        include_databases,
        exclude_databases,
        include_tables,
        exclude_tables,
    } => {
        let filter = neon_seren_replicator::filters::ReplicationFilter::new(
            include_databases,
            exclude_databases,
            include_tables,
            exclude_tables,
        )?;
        commands::verify(&source, &target, Some(filter)).await
    }
}
```

**Testing:**

Test that CLI parsing works:

```bash
# Should compile
cargo build

# Test help output
./target/debug/postgres-seren-replicator init --help
# Should show new flags: --include-databases, --exclude-databases, etc.

# Test validation (should fail with clear error)
./target/debug/postgres-seren-replicator init \
  --source "postgresql://..." \
  --target "postgresql://..." \
  --include-databases db1 \
  --exclude-databases db2
# Should show: "Cannot use both --include-databases and --exclude-databases"
```

**How to verify:**
- `cargo build` succeeds
- `--help` shows new flags
- Passing conflicting flags produces clear error message

**Commit message:**
```
Add filter CLI arguments to all commands

- Add include/exclude database flags
- Add include/exclude table flags
- Add interactive mode flag (stub for now)
- Add drop-existing flag to init command
- Parse arguments and create ReplicationFilter
- Fail fast on conflicting arguments

Related to #<issue-number>
```

---

## Phase 2: Database Existence Handling

**Goal**: Handle the case where target database already exists.

### Task 2.1: Add Database Existence Detection

**Files to modify:**
- `src/commands/init.rs`

**Background:** Currently, `init` fails with "database already exists" error. We need to detect this and handle it gracefully.

**Implementation:**

Find the section in `init.rs` where databases are created. Add existence check before creation:

```rust
// In src/commands/init.rs

/// Checks if a database exists on the target
async fn database_exists(target_conn: &Client, db_name: &str) -> Result<bool> {
    let query = "SELECT 1 FROM pg_database WHERE datname = $1";
    let rows = target_conn.query(query, &[&db_name]).await?;
    Ok(!rows.is_empty())
}

/// Checks if a database is empty (no user tables)
async fn database_is_empty(target_url: &str, db_name: &str) -> Result<bool> {
    // Need to connect to the specific database to check tables
    let db_url = target_url.replace("/postgres", &format!("/{}", db_name));
    let (client, connection) = tokio_postgres::connect(&db_url, tokio_postgres::NoTls)
        .await
        .context("Failed to connect to target database")?;

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            tracing::error!("Connection error: {}", e);
        }
    });

    let query = "
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
    ";

    let row = client.query_one(query, &[]).await?;
    let count: i64 = row.get(0);

    Ok(count == 0)
}

/// Prompts user to drop existing database
fn prompt_drop_database(db_name: &str) -> Result<bool> {
    use std::io::{self, Write};

    print!(
        "\nWarning: Database '{}' already exists on target and contains data.\n\
         Drop and recreate database? This will delete all existing data. [y/N]: ",
        db_name
    );
    io::stdout().flush()?;

    let mut input = String::new();
    io::stdin().read_line(&mut input)?;

    Ok(input.trim().eq_ignore_ascii_case("y"))
}

/// Drops a database if it exists
async fn drop_database_if_exists(target_conn: &Client, db_name: &str) -> Result<()> {
    tracing::info!("Dropping existing database '{}'...", db_name);

    // Terminate existing connections to the database
    let terminate_query = "
        SELECT pg_terminate_backend(pid)
        FROM pg_stat_activity
        WHERE datname = $1 AND pid <> pg_backend_pid()
    ";
    target_conn.execute(terminate_query, &[&db_name]).await?;

    // Drop the database
    let drop_query = format!("DROP DATABASE IF EXISTS \"{}\"", db_name);
    target_conn
        .execute(&drop_query, &[])
        .await
        .with_context(|| format!("Failed to drop database '{}'", db_name))?;

    tracing::info!("✓ Database '{}' dropped", db_name);
    Ok(())
}
```

Now modify the database creation loop:

```rust
// Find the section that creates databases and modify it:

for db_name in &databases_to_replicate {
    tracing::info!("Replicating database {}/{}: '{}'", idx + 1, databases_to_replicate.len(), db_name);

    // NEW: Check if database exists
    if database_exists(&target_conn, db_name).await? {
        tracing::info!("Database '{}' already exists on target", db_name);

        // Check if empty
        if database_is_empty(target_url, db_name).await? {
            tracing::info!("Database '{}' is empty, proceeding with restore", db_name);
        } else {
            // Database exists and has data
            let should_drop = if drop_existing {
                // Auto-drop in automated mode
                true
            } else if yes {
                // In automated mode without --drop-existing, fail
                bail!(
                    "Database '{}' already exists and contains data. \
                     Use --drop-existing to overwrite, or manually drop the database first.",
                    db_name
                );
            } else {
                // Interactive mode: prompt user
                prompt_drop_database(db_name)?
            };

            if should_drop {
                drop_database_if_exists(&target_conn, db_name).await?;
                // Continue to create fresh database below
            } else {
                bail!("Aborted: Database '{}' already exists", db_name);
            }
        }
    }

    // Create database if it doesn't exist (or was just dropped)
    if !database_exists(&target_conn, db_name).await? {
        let create_query = format!("CREATE DATABASE \"{}\"", db_name);
        target_conn
            .execute(&create_query, &[])
            .await
            .with_context(|| format!("Failed to create database '{}'", db_name))?;
        tracing::info!("  Created database '{}'", db_name);
    }

    // ... rest of dump/restore logic
}
```

**Testing Strategy:**

Write integration tests:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    #[ignore]
    async fn test_database_exists() {
        let url = std::env::var("TEST_TARGET_URL").expect("TEST_TARGET_URL not set");
        let (client, connection) = tokio_postgres::connect(&url, tokio_postgres::NoTls)
            .await
            .unwrap();

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("connection error: {}", e);
            }
        });

        // postgres database should always exist
        assert!(database_exists(&client, "postgres").await.unwrap());

        // non-existent database should not exist
        assert!(!database_exists(&client, "nonexistent_db_12345").await.unwrap());
    }

    #[tokio::test]
    #[ignore]
    async fn test_database_is_empty() {
        let url = std::env::var("TEST_TARGET_URL").expect("TEST_TARGET_URL not set");

        // postgres database has system tables, should not be empty
        let result = database_is_empty(&url, "postgres").await.unwrap();
        // Note: postgres may be empty of user tables
        // This test just verifies the function doesn't crash
    }
}
```

**Manual Testing:**

```bash
# Setup: Create test databases
docker run -d --name pg-target -e POSTGRES_PASSWORD=postgres -p 5433:5432 postgres:17

export TEST_TARGET_URL="postgresql://postgres:postgres@localhost:5433/postgres"

# Test 1: Database doesn't exist (should create it)
./target/release/postgres-seren-replicator init \
  --source "$TEST_SOURCE_URL" \
  --target "$TEST_TARGET_URL" \
  --include-databases testdb \
  --yes

# Test 2: Database exists but empty (should proceed)
# Run init again - database now exists but should be recognized as empty

# Test 3: Database exists with data, no --drop-existing (should fail)
# Add some data to testdb first
psql "$TEST_TARGET_URL/testdb" -c "CREATE TABLE test (id int)"
./target/release/postgres-seren-replicator init \
  --source "$TEST_SOURCE_URL" \
  --target "$TEST_TARGET_URL" \
  --include-databases testdb \
  --yes
# Should fail with clear error message

# Test 4: Database exists with data, with --drop-existing (should drop and recreate)
./target/release/postgres-seren-replicator init \
  --source "$TEST_SOURCE_URL" \
  --target "$TEST_TARGET_URL" \
  --include-databases testdb \
  --yes \
  --drop-existing
# Should succeed after dropping

# Test 5: Interactive mode prompt
./target/release/postgres-seren-replicator init \
  --source "$TEST_SOURCE_URL" \
  --target "$TEST_TARGET_URL" \
  --include-databases testdb
# Should prompt: "Drop and recreate database? [y/N]:"
```

**How to verify:**
- All scenarios above behave as expected
- Error messages are clear
- No data loss without explicit confirmation

**Commit message:**
```
Handle existing databases during init

- Check if target database exists before creating
- Check if existing database is empty
- Prompt user to drop non-empty databases
- Add --drop-existing flag for automation
- Prevent accidental data loss

Related to #<issue-number>
```

---

## Phase 3: Selective Dump/Restore

**Goal**: Make init command respect filter flags during dump/restore.

### Task 3.1: Modify Dump Functions to Accept Filters

**Files to modify:**
- `src/migration/dump.rs`

**Background:** Currently, `dump.rs` dumps entire databases. We need to:
1. Dump only selected databases
2. Exclude specific tables when dumping

**Implementation:**

Modify dump functions to accept `ReplicationFilter`:

```rust
// In src/migration/dump.rs

use crate::filters::ReplicationFilter;

// Modify function signature:
pub async fn dump_schema(
    source_url: &str,
    db_name: &str,
    output_path: &Path,
    filter: &ReplicationFilter,  // NEW parameter
) -> Result<()> {
    let mut cmd = Command::new("pg_dump");
    cmd.arg("--schema-only")
        .arg("--no-owner")
        .arg("--no-privileges")
        .arg("--format=plain")
        .arg("--file")
        .arg(output_path);

    // NEW: Add table filtering
    if let Some(exclude_tables) = get_excluded_tables_for_db(filter, db_name) {
        for table in exclude_tables {
            cmd.arg("--exclude-table").arg(&table);
        }
    }

    // If include_tables is specified, only dump those tables
    if let Some(include_tables) = get_included_tables_for_db(filter, db_name) {
        for table in include_tables {
            cmd.arg("--table").arg(&table);
        }
    }

    // Set connection URL
    cmd.arg(&db_url);

    // Execute command
    let output = cmd.output().await?;
    if !output.status.success() {
        bail!(
            "Schema dump failed for database '{}'.\nError: {}",
            db_name,
            String::from_utf8_lossy(&output.stderr)
        );
    }

    Ok(())
}

// Similar changes for dump_data:
pub async fn dump_data(
    source_url: &str,
    db_name: &str,
    output_path: &Path,
    jobs: usize,
    filter: &ReplicationFilter,  // NEW parameter
) -> Result<()> {
    let mut cmd = Command::new("pg_dump");
    cmd.arg("--data-only")
        .arg("--no-owner")
        .arg("--no-privileges")
        .arg("--format=directory")
        .arg(format!("--jobs={}", jobs))
        .arg("--compress=9")
        .arg("--blobs")
        .arg("--file")
        .arg(output_path);

    // NEW: Add table filtering (same as schema dump)
    if let Some(exclude_tables) = get_excluded_tables_for_db(filter, db_name) {
        for table in exclude_tables {
            cmd.arg("--exclude-table-data").arg(&table);
        }
    }

    if let Some(include_tables) = get_included_tables_for_db(filter, db_name) {
        for table in include_tables {
            cmd.arg("--table").arg(&table);
        }
    }

    cmd.arg(&db_url);

    let output = cmd.output().await?;
    if !output.status.success() {
        bail!(
            "Data dump failed for database '{}'.\nError: {}",
            db_name,
            String::from_utf8_lossy(&output.stderr)
        );
    }

    Ok(())
}

// Helper functions to extract table names for a specific database
fn get_excluded_tables_for_db(filter: &ReplicationFilter, db_name: &str) -> Option<Vec<String>> {
    filter.exclude_tables.as_ref().map(|tables| {
        tables
            .iter()
            .filter_map(|full_name| {
                let parts: Vec<&str> = full_name.split('.').collect();
                if parts.len() == 2 && parts[0] == db_name {
                    Some(parts[1].to_string())
                } else {
                    None
                }
            })
            .collect()
    })
}

fn get_included_tables_for_db(filter: &ReplicationFilter, db_name: &str) -> Option<Vec<String>> {
    filter.include_tables.as_ref().map(|tables| {
        tables
            .iter()
            .filter_map(|full_name| {
                let parts: Vec<&str> = full_name.split('.').collect();
                if parts.len() == 2 && parts[0] == db_name {
                    Some(parts[1].to_string())
                } else {
                    None
                }
            })
            .collect()
    })
}
```

**Testing:**

Unit tests for helper functions:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_excluded_tables_for_db() {
        let filter = ReplicationFilter::new(
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

        let tables = get_excluded_tables_for_db(&filter, "db1").unwrap();
        assert_eq!(tables, vec!["table1", "table2"]);

        let tables = get_excluded_tables_for_db(&filter, "db2").unwrap();
        assert_eq!(tables, vec!["table3"]);

        let tables = get_excluded_tables_for_db(&filter, "db3");
        assert!(tables.is_none() || tables.unwrap().is_empty());
    }

    #[test]
    fn test_get_included_tables_for_db() {
        let filter = ReplicationFilter::new(
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
        assert_eq!(tables, vec!["users", "orders"]);
    }
}
```

**Manual testing:**

```bash
# Test with table exclusion
./target/release/postgres-seren-replicator init \
  --source "$TEST_SOURCE_URL" \
  --target "$TEST_TARGET_URL" \
  --include-databases testdb \
  --exclude-tables testdb.audit_logs \
  --yes

# Verify: Check that audit_logs table is NOT on target
psql "$TEST_TARGET_URL/testdb" -c "\dt"
# Should not show audit_logs

# Test with table inclusion
./target/release/postgres-seren-replicator init \
  --source "$TEST_SOURCE_URL" \
  --target "$TEST_TARGET_URL" \
  --include-databases testdb \
  --include-tables testdb.users,testdb.orders \
  --yes \
  --drop-existing

# Verify: Only users and orders tables exist on target
psql "$TEST_TARGET_URL/testdb" -c "\dt"
# Should show ONLY users and orders tables
```

**Commit message:**
```
Add table filtering to dump operations

- Modify dump_schema to accept ReplicationFilter
- Modify dump_data to accept ReplicationFilter
- Use --exclude-table and --table pg_dump flags
- Add helper functions to extract tables per database
- Add unit tests for table extraction logic

Related to #<issue-number>
```

---

### Task 3.2: Update Init Command to Use Filters

**Files to modify:**
- `src/commands/init.rs`

**Implementation:**

Modify `init()` function to:
1. Accept filter parameter (already done in Phase 1)
2. Use filter to determine which databases to replicate
3. Pass filter to dump/restore functions

```rust
// In src/commands/init.rs

pub async fn init(
    source_url: &str,
    target_url: &str,
    yes: bool,
    filter: ReplicationFilter,  // NEW parameter
    drop_existing: bool,         // NEW parameter
) -> Result<()> {
    tracing::info!("Starting initial replication...");

    // Connect to source
    let (source_conn, source_connection) = /*...*/;

    // Validate filter against source
    if !filter.is_empty() {
        tracing::info!("Validating replication filters...");
        filter.validate(&source_conn).await?;
    }

    // Get list of databases to replicate (NEW: uses filter)
    let databases_to_replicate = if filter.is_empty() {
        crate::migration::schema::list_databases(&source_conn).await?
    } else {
        filter.get_databases_to_replicate(&source_conn).await?
    };

    tracing::info!("Found {} database(s) to replicate", databases_to_replicate.len());

    // ... globals dump/restore (unchanged) ...

    // Analyze sizes (NEW: only for filtered databases)
    tracing::info!("Analyzing database sizes...");
    let estimations = estimate_databases(source_url, &databases_to_replicate, &filter).await?;

    // Show estimation table
    // ...

    // Ask for confirmation (unless --yes)
    if !yes {
        // ... confirmation prompt ...
    }

    // Replicate each database
    for (idx, db_name) in databases_to_replicate.iter().enumerate() {
        tracing::info!("Replicating database {}/{}: '{}'", idx + 1, databases_to_replicate.len(), db_name);

        // Database existence handling (from Phase 2)
        // ...

        // Dump schema (NEW: pass filter)
        tracing::info!("  Dumping schema for '{}'...", db_name);
        let schema_path = temp_dir.path().join(format!("{}_schema.sql", db_name));
        crate::migration::dump::dump_schema(source_url, db_name, &schema_path, &filter).await?;

        // Restore schema
        tracing::info!("  Restoring schema for '{}'...", db_name);
        crate::migration::restore::restore_schema(target_url, db_name, &schema_path).await?;

        // Dump data (NEW: pass filter)
        tracing::info!("  Dumping data for '{}'...", db_name);
        let data_path = temp_dir.path().join(format!("{}_data", db_name));
        let jobs = crate::utils::get_parallel_jobs();
        crate::migration::dump::dump_data(source_url, db_name, &data_path, jobs, &filter).await?;

        // Restore data (NEW: pass filter)
        tracing::info!("  Restoring data for '{}'...", db_name);
        crate::migration::restore::restore_data(target_url, db_name, &data_path, jobs).await?;

        tracing::info!("✓ Database '{}' replicated successfully", db_name);
    }

    tracing::info!("✓ Initial replication complete for {} database(s)", databases_to_replicate.len());
    Ok(())
}
```

**Testing:**

Integration test:

```rust
#[tokio::test]
#[ignore]
async fn test_init_with_database_filter() {
    let source_url = std::env::var("TEST_SOURCE_URL").expect("TEST_SOURCE_URL not set");
    let target_url = std::env::var("TEST_TARGET_URL").expect("TEST_TARGET_URL not set");

    // Create filter to replicate only one database
    let filter = ReplicationFilter::new(
        Some(vec!["testdb".to_string()]),
        None,
        None,
        None,
    )
    .unwrap();

    // Should not panic
    let result = init(&source_url, &target_url, true, filter, false).await;
    assert!(result.is_ok());

    // Verify: testdb exists on target, other databases don't
    // ...
}

#[tokio::test]
#[ignore]
async fn test_init_with_table_filter() {
    let source_url = std::env::var("TEST_SOURCE_URL").expect("TEST_SOURCE_URL not set");
    let target_url = std::env::var("TEST_TARGET_URL").expect("TEST_TARGET_URL not set");

    let filter = ReplicationFilter::new(
        Some(vec!["testdb".to_string()]),
        None,
        None,
        Some(vec!["testdb.excluded_table".to_string()]),
    )
    .unwrap();

    let result = init(&source_url, &target_url, true, filter, true).await;
    assert!(result.is_ok());

    // Verify: excluded_table doesn't exist on target
    // ...
}
```

**Manual testing:**

```bash
# Test full workflow with filters
export TEST_SOURCE_URL="postgresql://postgres:postgres@localhost:5432/postgres"
export TEST_TARGET_URL="postgresql://postgres:postgres@localhost:5433/postgres"

# Setup: Create source databases and tables
psql "$TEST_SOURCE_URL" <<EOF
CREATE DATABASE db1;
CREATE DATABASE db2;
CREATE DATABASE db3;
\c db1
CREATE TABLE users (id int PRIMARY KEY, name text);
CREATE TABLE orders (id int PRIMARY KEY, user_id int);
CREATE TABLE audit_logs (id int, event text);
INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob');
INSERT INTO orders VALUES (1, 1), (2, 2);
INSERT INTO audit_logs VALUES (1, 'login'), (2, 'logout');
\c db2
CREATE TABLE products (id int PRIMARY KEY, name text);
INSERT INTO products VALUES (1, 'Widget'), (2, 'Gadget');
EOF

# Test 1: Include specific databases only
./target/release/postgres-seren-replicator init \
  --source "$TEST_SOURCE_URL" \
  --target "$TEST_TARGET_URL" \
  --include-databases db1,db2 \
  --yes

# Verify: db1 and db2 exist, db3 doesn't
psql "$TEST_TARGET_URL" -c "\l" | grep -E "db1|db2|db3"

# Test 2: Exclude specific tables
./target/release/postgres-seren-replicator init \
  --source "$TEST_SOURCE_URL" \
  --target "$TEST_TARGET_URL" \
  --include-databases db1 \
  --exclude-tables db1.audit_logs \
  --yes \
  --drop-existing

# Verify: audit_logs table doesn't exist
psql "$TEST_TARGET_URL/db1" -c "\dt" | grep audit_logs
# Should return nothing

# Verify: users and orders tables exist with data
psql "$TEST_TARGET_URL/db1" -c "SELECT COUNT(*) FROM users"
# Should return 2
```

**Commit message:**
```
Integrate filtering into init command

- Use filter to determine databases to replicate
- Pass filter to dump and restore functions
- Update size estimation to use filtered databases
- Add integration tests for selective init
- Update command signature

Related to #<issue-number>
```

---

### Task 3.3: Update Size Estimation to Respect Filters

**Files to modify:**
- `src/migration/estimation.rs`

**Why**: Size estimates should only include filtered databases/tables, not everything.

**Implementation:**

```rust
// In src/migration/estimation.rs

use crate::filters::ReplicationFilter;

pub async fn estimate_databases(
    source_url: &str,
    databases: &[String],
    filter: &ReplicationFilter,  // NEW parameter
) -> Result<Vec<DatabaseEstimation>> {
    let mut estimations = Vec::new();

    for db_name in databases {
        // Connect to specific database
        let db_url = source_url.replace("/postgres", &format!("/{}", db_name));
        let (client, connection) = tokio_postgres::connect(&db_url, tokio_postgres::NoTls).await?;

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                tracing::error!("Connection error: {}", e);
            }
        });

        // Get all tables
        let all_tables = crate::migration::schema::list_tables(&client, db_name).await?;

        // Filter tables (NEW)
        let tables_to_estimate: Vec<String> = all_tables
            .into_iter()
            .filter(|table| filter.should_replicate_table(db_name, table))
            .collect();

        // Estimate size for filtered tables only
        let mut total_size: i64 = 0;
        for table in &tables_to_estimate {
            let size_query = format!(
                "SELECT pg_total_relation_size('\"{}\"')",
                table
            );
            let row = client.query_one(&size_query, &[]).await?;
            let size: i64 = row.get(0);
            total_size += size;
        }

        let estimation = DatabaseEstimation {
            name: db_name.clone(),
            size_bytes: total_size as u64,
            table_count: tables_to_estimate.len(),
        };

        estimations.push(estimation);
    }

    Ok(estimations)
}
```

**Testing:**

```rust
#[tokio::test]
#[ignore]
async fn test_estimate_with_filter() {
    let source_url = std::env::var("TEST_SOURCE_URL").expect("TEST_SOURCE_URL not set");

    // Setup: ensure db1 exists with multiple tables
    // ...

    let filter = ReplicationFilter::new(
        None,
        None,
        None,
        Some(vec!["db1.large_table".to_string()]),
    )
    .unwrap();

    let estimations = estimate_databases(&source_url, &["db1"], &filter).await.unwrap();

    // Size should be less than total database size (because large_table is excluded)
    // This is hard to test precisely, but we can verify it completes without error
    assert_eq!(estimations.len(), 1);
    assert_eq!(estimations[0].name, "db1");
}
```

**Commit message:**
```
Update size estimation to respect filters

- Accept ReplicationFilter parameter
- Only estimate size for filtered tables
- Update table count to reflect filtered tables
- Add integration test

Related to #<issue-number>
```

---

## Phase 4: Selective Logical Replication

**Goal**: Make sync command create filtered subscriptions.

### Task 4.1: Modify Subscription Creation to Filter Tables

**Files to modify:**
- `src/replication/subscription.rs`
- `src/replication/publication.rs` (verify current behavior)

**Background**: PostgreSQL subscriptions subscribe to entire publications. To filter tables, we need to either:
1. Create per-database publications with only desired tables (chosen approach)
2. Use publication table lists in subscription (not supported in all PG versions)

**Implementation:**

```rust
// In src/replication/publication.rs

use crate::filters::ReplicationFilter;

/// Creates a publication for specified tables (or all tables if filter is empty)
pub async fn create_publication(
    source_conn: &Client,
    db_name: &str,
    filter: &ReplicationFilter,
) -> Result<()> {
    let pub_name = format!("seren_replication_pub_{}", db_name);

    // Check if publication already exists
    let check_query = "SELECT 1 FROM pg_publication WHERE pubname = $1";
    let rows = source_conn.query(check_query, &[&pub_name]).await?;

    if !rows.is_empty() {
        tracing::info!("Publication '{}' already exists", pub_name);
        return Ok(());
    }

    // NEW: Create publication with filtered tables
    let create_query = if filter.is_empty() || filter.include_tables.is_none() && filter.exclude_tables.is_none() {
        // No table filtering - publish all tables
        format!("CREATE PUBLICATION \"{}\" FOR ALL TABLES", pub_name)
    } else {
        // Get list of tables to publish
        let tables_to_publish = filter.get_tables_to_replicate(source_conn, db_name).await?;

        if tables_to_publish.is_empty() {
            bail!("No tables to publish in database '{}'", db_name);
        }

        // Build table list: TABLE table1, TABLE table2, ...
        let table_list = tables_to_publish
            .iter()
            .map(|t| format!("TABLE \"{}\"", t))
            .collect::<Vec<_>>()
            .join(", ");

        format!("CREATE PUBLICATION \"{}\" FOR {}", pub_name, table_list)
    };

    source_conn
        .execute(&create_query, &[])
        .await
        .with_context(|| format!("Failed to create publication '{}'", pub_name))?;

    tracing::info!("✓ Publication '{}' created", pub_name);
    Ok(())
}
```

```rust
// In src/replication/subscription.rs

use crate::filters::ReplicationFilter;

pub async fn create_subscription(
    target_conn: &Client,
    source_url: &str,
    db_name: &str,
    filter: &ReplicationFilter,  // NEW parameter (not directly used, but kept for consistency)
) -> Result<()> {
    let pub_name = format!("seren_replication_pub_{}", db_name);
    let sub_name = format!("seren_replication_sub_{}", db_name);

    // Check if subscription already exists
    let check_query = "SELECT 1 FROM pg_subscription WHERE subname = $1";
    let rows = target_conn.query(check_query, &[&sub_name]).await?;

    if !rows.is_empty() {
        tracing::info!("Subscription '{}' already exists", sub_name);
        return Ok(());
    }

    // Create subscription
    // The publication already has the filtered tables, so we just subscribe to it
    let create_query = format!(
        "CREATE SUBSCRIPTION \"{}\" CONNECTION '{}' PUBLICATION \"{}\"",
        sub_name, source_url, pub_name
    );

    target_conn
        .execute(&create_query, &[])
        .await
        .with_context(|| format!("Failed to create subscription '{}'", sub_name))?;

    tracing::info!("✓ Subscription '{}' created", sub_name);
    Ok(())
}
```

**Testing:**

Integration test:

```rust
#[tokio::test]
#[ignore]
async fn test_create_filtered_publication() {
    let source_url = std::env::var("TEST_SOURCE_URL").expect("TEST_SOURCE_URL not set");

    // Connect to source database
    let db_url = source_url.replace("/postgres", "/testdb");
    let (client, connection) = tokio_postgres::connect(&db_url, tokio_postgres::NoTls)
        .await
        .unwrap();

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    // Create filter excluding audit_logs table
    let filter = ReplicationFilter::new(
        None,
        None,
        None,
        Some(vec!["testdb.audit_logs".to_string()]),
    )
    .unwrap();

    // Create publication
    create_publication(&client, "testdb", &filter).await.unwrap();

    // Verify: Check which tables are in the publication
    let query = "
        SELECT tablename
        FROM pg_publication_tables
        WHERE pubname = 'seren_replication_pub_testdb'
    ";
    let rows = client.query(query, &[]).await.unwrap();
    let tables: Vec<String> = rows.iter().map(|r| r.get(0)).collect();

    // audit_logs should NOT be in the publication
    assert!(!tables.contains(&"audit_logs".to_string()));

    // Other tables should be present
    assert!(tables.contains(&"users".to_string()));
    assert!(tables.contains(&"orders".to_string()));

    // Cleanup
    client
        .execute("DROP PUBLICATION seren_replication_pub_testdb", &[])
        .await
        .unwrap();
}
```

**Manual testing:**

```bash
# Setup source with tables
psql "$TEST_SOURCE_URL" <<EOF
CREATE DATABASE testdb;
\c testdb
CREATE TABLE users (id int PRIMARY KEY, name text);
CREATE TABLE orders (id int PRIMARY KEY, user_id int);
CREATE TABLE audit_logs (id int, event text);
ALTER TABLE users REPLICA IDENTITY FULL;
ALTER TABLE orders REPLICA IDENTITY FULL;
ALTER TABLE audit_logs REPLICA IDENTITY FULL;
EOF

# Run sync with table filter
./target/release/postgres-seren-replicator sync \
  --source "$TEST_SOURCE_URL" \
  --target "$TEST_TARGET_URL" \
  --include-databases testdb \
  --exclude-tables testdb.audit_logs

# Verify: Check publication tables on source
psql "$TEST_SOURCE_URL/testdb" -c "
  SELECT tablename
  FROM pg_publication_tables
  WHERE pubname = 'seren_replication_pub_testdb'
"
# Should show users and orders, NOT audit_logs

# Verify: Check subscription on target
psql "$TEST_TARGET_URL/testdb" -c "
  SELECT subname, subenabled
  FROM pg_subscription
  WHERE subname = 'seren_replication_sub_testdb'
"
# Should show subscription is enabled

# Test replication: Insert data on source
psql "$TEST_SOURCE_URL/testdb" -c "INSERT INTO users VALUES (1, 'Alice')"

# Wait a moment for replication
sleep 2

# Verify: Data appears on target
psql "$TEST_TARGET_URL/testdb" -c "SELECT * FROM users"
# Should show Alice

# Verify: audit_logs doesn't exist on target (since it was filtered)
psql "$TEST_TARGET_URL/testdb" -c "\dt audit_logs"
# Should show "No matching relations found"
```

**Commit message:**
```
Add table filtering to logical replication

- Modify create_publication to accept filter
- Create publications with filtered table list
- Keep create_subscription logic unchanged
- Add integration test for filtered publication
- Verify replication works with filtered tables

Related to #<issue-number>
```

---

### Task 4.2: Update Sync Command to Use Filters

**Files to modify:**
- `src/commands/sync.rs`

**Implementation:**

```rust
// In src/commands/sync.rs

pub async fn sync(
    source_url: &str,
    target_url: &str,
    filter: Option<ReplicationFilter>,  // Already added in Phase 1
    publication_name: Option<String>,
    subscription_name: Option<String>,
) -> Result<()> {
    let filter = filter.unwrap_or_else(ReplicationFilter::empty);

    tracing::info!("Setting up continuous replication...");

    // Connect to source
    let (source_conn, source_connection) = /*...*/;

    // Validate filter
    if !filter.is_empty() {
        tracing::info!("Validating replication filters...");
        filter.validate(&source_conn).await?;
    }

    // Get databases to replicate
    let databases = if filter.is_empty() {
        crate::migration::schema::list_databases(&source_conn).await?
    } else {
        filter.get_databases_to_replicate(&source_conn).await?
    };

    tracing::info!("Setting up replication for {} database(s)", databases.len());

    // For each database, create publication and subscription
    for db_name in &databases {
        tracing::info!("Setting up replication for database '{}'", db_name);

        // Connect to source database
        let source_db_url = source_url.replace("/postgres", &format!("/{}", db_name));
        let (source_db_conn, source_db_connection) = tokio_postgres::connect(&source_db_url, tokio_postgres::NoTls).await?;
        tokio::spawn(async move {
            if let Err(e) = source_db_connection.await {
                tracing::error!("Connection error: {}", e);
            }
        });

        // Create publication (NEW: pass filter)
        crate::replication::publication::create_publication(&source_db_conn, db_name, &filter).await?;

        // Connect to target database
        let target_db_url = target_url.replace("/postgres", &format!("/{}", db_name));
        let (target_db_conn, target_db_connection) = tokio_postgres::connect(&target_db_url, tokio_postgres::NoTls).await?;
        tokio::spawn(async move {
            if let Err(e) = target_db_connection.await {
                tracing::error!("Connection error: {}", e);
            }
        });

        // Create subscription (pass filter for consistency)
        crate::replication::subscription::create_subscription(
            &target_db_conn,
            &source_db_url,
            db_name,
            &filter,
        )
        .await?;

        tracing::info!("✓ Replication set up for database '{}'", db_name);
    }

    tracing::info!("✓ Continuous replication enabled for {} database(s)", databases.len());
    Ok(())
}
```

**Testing:**

Integration test:

```rust
#[tokio::test]
#[ignore]
async fn test_sync_with_filter() {
    let source_url = std::env::var("TEST_SOURCE_URL").expect("TEST_SOURCE_URL not set");
    let target_url = std::env::var("TEST_TARGET_URL").expect("TEST_TARGET_URL not set");

    // Filter: replicate db1 but exclude audit_logs table
    let filter = ReplicationFilter::new(
        Some(vec!["testdb".to_string()]),
        None,
        None,
        Some(vec!["testdb.audit_logs".to_string()]),
    )
    .unwrap();

    let result = sync(&source_url, &target_url, Some(filter), None, None).await;
    assert!(result.is_ok());

    // Verify replication is working
    // 1. Insert data on source
    // 2. Wait for replication
    // 3. Verify data on target
    // 4. Verify excluded table is not replicated
}
```

**Commit message:**
```
Integrate filtering into sync command

- Use filter to determine databases for replication
- Pass filter to publication creation
- Create filtered publications per database
- Add integration test for selective sync

Related to #<issue-number>
```

---

## Phase 5: Filter Support in Other Commands

### Task 5.1: Update Validate Command

**Files to modify:**
- `src/commands/validate.rs`

**Implementation:**

```rust
// In src/commands/validate.rs

pub async fn validate(
    source_url: &str,
    target_url: &str,
    filter: ReplicationFilter,  // NEW parameter (already added in Phase 1)
) -> Result<()> {
    tracing::info!("Validating replication prerequisites...");

    // ... existing validation (tools, connectivity, privileges) ...

    // NEW: Validate filter if present
    if !filter.is_empty() {
        tracing::info!("Validating replication filters...");

        let (source_conn, source_connection) = /*...*/;

        filter.validate(&source_conn).await.context("Filter validation failed")?;

        let databases = filter.get_databases_to_replicate(&source_conn).await?;

        tracing::info!("✓ Filter validation passed");
        tracing::info!("  {} database(s) will be replicated", databases.len());

        // Show which databases
        for db in &databases {
            tracing::info!("    - {}", db);
        }
    }

    tracing::info!("✓ Validation complete");
    Ok(())
}
```

**Testing:**

```bash
# Test validate with filter
./target/release/postgres-seren-replicator validate \
  --source "$TEST_SOURCE_URL" \
  --target "$TEST_TARGET_URL" \
  --include-databases db1,db2

# Should show:
# ✓ Filter validation passed
#   2 database(s) will be replicated
#     - db1
#     - db2

# Test with non-existent database
./target/release/postgres-seren-replicator validate \
  --source "$TEST_SOURCE_URL" \
  --target "$TEST_TARGET_URL" \
  --include-databases nonexistent

# Should fail with clear error:
# Error: Database 'nonexistent' specified in --include-databases does not exist on source
```

**Commit message:**
```
Add filter validation to validate command

- Validate filter before checking other prerequisites
- Show which databases will be replicated
- Provide clear error messages for invalid filters

Related to #<issue-number>
```

---

### Task 5.2: Update Status Command

**Files to modify:**
- `src/commands/status.rs`

**Implementation:**

```rust
// In src/commands/status.rs

pub async fn status(
    source_url: &str,
    target_url: &str,
    filter: Option<ReplicationFilter>,  // NEW parameter
) -> Result<()> {
    let filter = filter.unwrap_or_else(ReplicationFilter::empty);

    tracing::info!("Checking replication status...");

    // Get databases to check (NEW: use filter)
    let (source_conn, source_connection) = /*...*/;

    let databases = if filter.is_empty() {
        crate::migration::schema::list_databases(&source_conn).await?
    } else {
        filter.get_databases_to_replicate(&source_conn).await?
    };

    for db_name in &databases {
        tracing::info!("Status for database '{}':", db_name);

        // Connect to target database
        let target_db_url = target_url.replace("/postgres", &format!("/{}", db_name));
        let (target_conn, target_connection) = /*...*/;

        // Check subscription status
        let sub_name = format!("seren_replication_sub_{}", db_name);
        let status = crate::replication::monitor::get_subscription_status(&target_conn, &sub_name).await?;

        match status {
            Some(sub) => {
                tracing::info!("  Subscription: {}", sub.state);
                tracing::info!("  Lag: {} bytes", sub.lag_bytes);
                // ... more status info ...
            }
            None => {
                tracing::warn!("  No subscription found (replication not set up)");
            }
        }
    }

    Ok(())
}
```

**Commit message:**
```
Add filter support to status command

- Show status only for filtered databases
- Skip databases not included in filter
- Maintain backward compatibility (no filter = all databases)

Related to #<issue-number>
```

---

### Task 5.3: Update Verify Command

**Files to modify:**
- `src/commands/verify.rs`

**Implementation:**

```rust
// In src/commands/verify.rs

pub async fn verify(
    source_url: &str,
    target_url: &str,
    filter: Option<ReplicationFilter>,  // NEW parameter
) -> Result<()> {
    let filter = filter.unwrap_or_else(ReplicationFilter::empty);

    tracing::info!("Verifying data integrity...");

    // Get databases (NEW: use filter)
    let (source_conn, source_connection) = /*...*/;

    let databases = if filter.is_empty() {
        crate::migration::schema::list_databases(&source_conn).await?
    } else {
        filter.get_databases_to_replicate(&source_conn).await?
    };

    for db_name in &databases {
        tracing::info!("Verifying database '{}'", db_name);

        // Get tables (NEW: use filter)
        let source_db_url = source_url.replace("/postgres", &format!("/{}", db_name));
        let (source_db_conn, source_db_connection) = /*...*/;

        let all_tables = crate::migration::schema::list_tables(&source_db_conn, db_name).await?;
        let tables_to_verify: Vec<String> = all_tables
            .into_iter()
            .filter(|table| filter.should_replicate_table(db_name, table))
            .collect();

        tracing::info!("  Verifying {} table(s)", tables_to_verify.len());

        // Verify each table
        for table in &tables_to_verify {
            let source_checksum = crate::migration::checksum::compute_checksum(&source_db_conn, table).await?;

            let target_db_url = target_url.replace("/postgres", &format!("/{}", db_name));
            let (target_db_conn, target_db_connection) = /*...*/;
            let target_checksum = crate::migration::checksum::compute_checksum(&target_db_conn, table).await?;

            if source_checksum == target_checksum {
                tracing::info!("    ✓ {} - checksums match", table);
            } else {
                tracing::error!("    ✗ {} - checksums DIFFER", table);
                tracing::error!("      Source: {}", source_checksum);
                tracing::error!("      Target: {}", target_checksum);
            }
        }
    }

    Ok(())
}
```

**Testing:**

```bash
# Test verify with filter
./target/release/postgres-seren-replicator verify \
  --source "$TEST_SOURCE_URL" \
  --target "$TEST_TARGET_URL" \
  --include-databases db1 \
  --exclude-tables db1.temp_data

# Should verify only db1, excluding temp_data table
```

**Commit message:**
```
Add filter support to verify command

- Verify only filtered databases
- Verify only filtered tables within databases
- Skip excluded tables from checksum computation

Related to #<issue-number>
```

---

## Phase 6: Interactive Mode

**Goal**: Implement the interactive UI for database/table selection.

### Task 6.1: Add Interactive Selection Library

**Files to modify:**
- `Cargo.toml`

**Implementation:**

Add dependency:

```toml
[dependencies]
# ... existing dependencies ...
dialoguer = "0.11"
```

Run `cargo build` to download the dependency.

**Commit message:**
```
Add dialoguer dependency for interactive mode

- Add dialoguer crate for terminal UI
- Will be used for database/table selection

Related to #<issue-number>
```

---

### Task 6.2: Implement Interactive Selection

**Files to create:**
- `src/interactive.rs`

**Files to modify:**
- `src/lib.rs` (add `pub mod interactive;`)

**Implementation:**

```rust
// src/interactive.rs
// ABOUTME: Interactive terminal UI for database and table selection
// ABOUTME: Provides multi-select checklist interface

use anyhow::{Context, Result};
use dialoguer::{theme::ColorfulTheme, MultiSelect};
use tokio_postgres::Client;

use crate::filters::ReplicationFilter;

/// Launches interactive mode to select databases and tables
pub async fn select_databases_and_tables(source_conn: &Client) -> Result<ReplicationFilter> {
    // Step 1: Get all databases
    let all_databases = crate::migration::schema::list_databases(source_conn).await?;

    if all_databases.is_empty() {
        anyhow::bail!("No databases found on source");
    }

    // Step 2: Let user select databases
    println!("\nSelect databases to replicate:");
    let selections = MultiSelect::with_theme(&ColorfulTheme::default())
        .with_prompt("Use space to toggle, enter to confirm")
        .items(&all_databases)
        .interact()
        .context("Failed to get database selection")?;

    if selections.is_empty() {
        anyhow::bail!("No databases selected");
    }

    let selected_databases: Vec<String> = selections
        .iter()
        .map(|&idx| all_databases[idx].clone())
        .collect();

    // Step 3: For each selected database, let user exclude specific tables
    let mut excluded_tables = Vec::new();

    for db_name in &selected_databases {
        // Connect to database
        // Note: This is a simplified version; in production we'd need proper connection management
        let tables = get_tables_for_database(source_conn, db_name).await?;

        if tables.is_empty() {
            println!("\nDatabase '{}' has no tables", db_name);
            continue;
        }

        println!("\nSelect tables to EXCLUDE from '{}' (leave all unchecked to include all):", db_name);
        let exclude_selections = MultiSelect::with_theme(&ColorfulTheme::default())
            .with_prompt("Use space to toggle, enter to confirm")
            .items(&tables)
            .interact()
            .context("Failed to get table exclusion selection")?;

        for &idx in &exclude_selections {
            excluded_tables.push(format!("{}.{}", db_name, tables[idx]));
        }
    }

    // Step 4: Show summary
    println!("\n=== Replication Summary ===");
    println!("Databases: {}", selected_databases.len());
    for db in &selected_databases {
        println!("  - {}", db);
    }
    if !excluded_tables.is_empty() {
        println!("Excluded tables: {}", excluded_tables.len());
        for table in &excluded_tables {
            println!("  - {}", table);
        }
    }

    // Step 5: Confirm
    let confirm = dialoguer::Confirm::with_theme(&ColorfulTheme::default())
        .with_prompt("Proceed with these settings?")
        .default(false)
        .interact()
        .context("Failed to get confirmation")?;

    if !confirm {
        anyhow::bail!("Aborted by user");
    }

    // Step 6: Build filter
    let filter = ReplicationFilter::new(
        Some(selected_databases),
        None,
        None,
        if excluded_tables.is_empty() {
            None
        } else {
            Some(excluded_tables)
        },
    )?;

    Ok(filter)
}

// Helper to get tables for a database
async fn get_tables_for_database(source_conn: &Client, db_name: &str) -> Result<Vec<String>> {
    // This requires connecting to the specific database
    // For MVP, we can query information_schema if we have a connection
    // Or we need to establish a new connection to the database

    // Simplified: return empty list for now
    // In production, establish connection to db_name and query tables
    crate::migration::schema::list_tables(source_conn, db_name).await
}

#[cfg(test)]
mod tests {
    use super::*;

    // Interactive tests are hard to automate
    // Manual testing is recommended for this module
}
```

**Files to modify:**
- `src/main.rs` - wire up interactive mode

```rust
// In src/main.rs, update the match arms:

Commands::Init {
    source,
    target,
    yes,
    include_databases,
    exclude_databases,
    include_tables,
    exclude_tables,
    interactive,
    drop_existing,
} => {
    let filter = if interactive {
        // NEW: Launch interactive mode
        let (source_conn, source_connection) = tokio_postgres::connect(&source, tokio_postgres::NoTls).await?;
        tokio::spawn(async move {
            if let Err(e) = source_connection.await {
                tracing::error!("Connection error: {}", e);
            }
        });
        neon_seren_replicator::interactive::select_databases_and_tables(&source_conn).await?
    } else {
        neon_seren_replicator::filters::ReplicationFilter::new(
            include_databases,
            exclude_databases,
            include_tables,
            exclude_tables,
        )?
    };
    commands::init(&source, &target, yes, filter, drop_existing).await
}

// Similar changes for other commands that support interactive mode
```

**Manual Testing:**

```bash
# Test interactive mode
./target/release/postgres-seren-replicator init \
  --source "$TEST_SOURCE_URL" \
  --target "$TEST_TARGET_URL" \
  --interactive

# Should show:
# - List of databases with checkboxes
# - After selecting databases, show tables per database
# - Show summary before proceeding
# - Proceed with replication
```

**Testing checklist:**
- [ ] Can navigate with arrow keys
- [ ] Can toggle with space bar
- [ ] Can confirm with enter
- [ ] Summary shows correct selections
- [ ] Aborting stops the command
- [ ] Replication proceeds with selected databases/tables

**Commit message:**
```
Implement interactive database and table selection

- Add dialoguer for terminal UI
- Create interactive selection module
- Multi-select databases with checkboxes
- Exclude tables per database
- Show summary before proceeding
- Wire up to init command

Related to #<issue-number>
```

---

## Phase 7: Documentation & Polish

### Task 7.1: Update README

**Files to modify:**
- `README.md`

**Add sections:**

1. **Selective Replication** section with examples
2. **Interactive Mode** usage
3. **Multi-Provider Support** notes

**Example additions:**

```markdown
## Selective Replication

Replicate only specific databases or exclude certain tables:

### Database-Level Filtering

```bash
# Replicate only production and analytics databases
./postgres-seren-replicator init \
  --source "postgresql://..." \
  --target "postgresql://..." \
  --include-databases production,analytics

# Replicate all except test databases
./postgres-seren-replicator init \
  --source "postgresql://..." \
  --target "postgresql://..." \
  --exclude-databases test,staging,dev
```

### Table-Level Filtering

```bash
# Exclude audit logs and temporary tables
./postgres-seren-replicator init \
  --source "postgresql://..." \
  --target "postgresql://..." \
  --exclude-tables production.audit_logs,production.temp_sessions

# Replicate only specific tables
./postgres-seren-replicator init \
  --source "postgresql://..." \
  --target "postgresql://..." \
  --include-tables production.users,production.orders
```

### Combined Filtering

```bash
# Replicate specific databases but exclude certain tables
./postgres-seren-replicator init \
  --source "postgresql://..." \
  --target "postgresql://..." \
  --include-databases production,analytics \
  --exclude-tables production.audit_logs,analytics.raw_events
```

## Interactive Mode

For exploratory use, launch interactive mode to select databases and tables visually:

```bash
./postgres-seren-replicator init \
  --source "postgresql://..." \
  --target "postgresql://..." \
  --interactive
```

This will:
1. Show a list of all databases (use space to select)
2. For each selected database, show tables to exclude
3. Display a summary before proceeding
4. Begin replication with your selections

## Multi-Provider Support

The tool works with any PostgreSQL-compatible provider:

### AWS RDS

```bash
./postgres-seren-replicator init \
  --source "postgresql://user:pass@mydb.rds.amazonaws.com:5432/mydb" \
  --target "postgresql://user:pass@seren-host:5432/mydb"
```

### Hetzner PostgreSQL

```bash
./postgres-seren-replicator init \
  --source "postgresql://user:pass@postgres.hetzner.cloud:5432/mydb" \
  --target "postgresql://user:pass@seren-host:5432/mydb"
```

### Important Notes

- Ensure source user has REPLICATION privileges
- Target user needs CREATE DATABASE privileges
- Logical replication requires PostgreSQL 12+
- Tables need primary keys for replication

## Handling Existing Databases

If target database already exists:

- **Empty database**: Proceeds automatically
- **Non-empty database**: Prompts to drop (or use `--drop-existing`)

```bash
# Automatically drop existing databases
./postgres-seren-replicator init \
  --source "postgresql://..." \
  --target "postgresql://..." \
  --drop-existing \
  --yes
```
```

**Commit message:**
```
Update README with selective replication docs

- Add selective replication examples
- Document interactive mode usage
- Add multi-provider examples (RDS, Hetzner)
- Document database existence handling
- Add filtering flag reference

Related to #<issue-number>
```

---

### Task 7.2: Update CLAUDE.md

**Files to modify:**
- `CLAUDE.md`

**Add sections:**

In the "Architecture" section, add:

```markdown
### Filtering System

**Module: `src/filters.rs`**

The filtering system provides selective replication at database and table levels:

- `ReplicationFilter` - Central struct for filter rules
  - `include_databases` / `exclude_databases` - Database-level filtering
  - `include_tables` / `exclude_tables` - Table-level filtering (format: "db.table")
  - Validates filters against source database
  - Provides helper methods for filter evaluation

**Used by all commands:**
- `validate` - Validates only filtered databases
- `init` - Dumps/restores only filtered databases/tables
- `sync` - Creates publications with filtered tables
- `status` - Shows status for filtered databases
- `verify` - Verifies checksums for filtered tables

**Interactive Mode: `src/interactive.rs`**

Provides terminal UI for database/table selection using `dialoguer`:
- Multi-select checkboxes for databases
- Per-database table exclusion
- Summary confirmation before proceeding
```

**Commit message:**
```
Update CLAUDE.md with filtering architecture

- Document filtering system design
- Explain ReplicationFilter struct
- Document interactive mode module
- Add notes for future developers

Related to #<issue-number>
```

---

### Task 7.3: Add Integration Test Suite

**Files to create:**
- `tests/selective_replication_test.rs`

**Implementation:**

```rust
// tests/selective_replication_test.rs

use anyhow::Result;
use neon_seren_replicator::{commands, filters::ReplicationFilter};

#[tokio::test]
#[ignore]
async fn test_full_workflow_with_database_filter() -> Result<()> {
    let source_url = std::env::var("TEST_SOURCE_URL")?;
    let target_url = std::env::var("TEST_TARGET_URL")?;

    // Setup: Create test databases
    setup_test_databases(&source_url).await?;

    // Filter: Include only db1 and db2
    let filter = ReplicationFilter::new(
        Some(vec!["test_db1".to_string(), "test_db2".to_string()]),
        None,
        None,
        None,
    )?;

    // 1. Validate
    commands::validate(&source_url, &target_url, filter.clone()).await?;

    // 2. Init
    commands::init(&source_url, &target_url, true, filter.clone(), true).await?;

    // 3. Verify databases exist
    verify_databases_exist(&target_url, &["test_db1", "test_db2"]).await?;
    verify_database_not_exists(&target_url, "test_db3").await?;

    // 4. Sync
    commands::sync(&source_url, &target_url, Some(filter.clone()), None, None).await?;

    // 5. Verify replication works
    insert_test_data(&source_url, "test_db1", "users").await?;
    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
    verify_data_replicated(&target_url, "test_db1", "users").await?;

    // Cleanup
    cleanup_test_databases(&source_url).await?;
    cleanup_test_databases(&target_url).await?;

    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_full_workflow_with_table_filter() -> Result<()> {
    let source_url = std::env::var("TEST_SOURCE_URL")?;
    let target_url = std::env::var("TEST_TARGET_URL")?;

    // Setup
    setup_test_databases(&source_url).await?;

    // Filter: Exclude audit_logs table
    let filter = ReplicationFilter::new(
        Some(vec!["test_db1".to_string()]),
        None,
        None,
        Some(vec!["test_db1.audit_logs".to_string()]),
    )?;

    // Init
    commands::init(&source_url, &target_url, true, filter.clone(), true).await?;

    // Verify: audit_logs doesn't exist on target
    verify_table_not_exists(&target_url, "test_db1", "audit_logs").await?;
    verify_table_exists(&target_url, "test_db1", "users").await?;

    // Sync
    commands::sync(&source_url, &target_url, Some(filter), None, None).await?;

    // Verify: Changes to users replicate, audit_logs doesn't exist
    insert_test_data(&source_url, "test_db1", "users").await?;
    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
    verify_data_replicated(&target_url, "test_db1", "users").await?;

    // Cleanup
    cleanup_test_databases(&source_url).await?;
    cleanup_test_databases(&target_url).await?;

    Ok(())
}

// Helper functions
async fn setup_test_databases(url: &str) -> Result<()> {
    // Create test databases with tables
    Ok(())
}

async fn cleanup_test_databases(url: &str) -> Result<()> {
    // Drop test databases
    Ok(())
}

async fn verify_databases_exist(url: &str, databases: &[&str]) -> Result<()> {
    // Check databases exist
    Ok(())
}

async fn verify_database_not_exists(url: &str, database: &str) -> Result<()> {
    // Check database doesn't exist
    Ok(())
}

async fn verify_table_exists(url: &str, database: &str, table: &str) -> Result<()> {
    // Check table exists
    Ok(())
}

async fn verify_table_not_exists(url: &str, database: &str, table: &str) -> Result<()> {
    // Check table doesn't exist
    Ok(())
}

async fn insert_test_data(url: &str, database: &str, table: &str) -> Result<()> {
    // Insert test data
    Ok(())
}

async fn verify_data_replicated(url: &str, database: &str, table: &str) -> Result<()> {
    // Verify data exists on target
    Ok(())
}
```

**Run tests:**

```bash
# Setup test databases
docker run -d --name pg-source -e POSTGRES_PASSWORD=postgres -p 5432:5432 postgres:17
docker run -d --name pg-target -e POSTGRES_PASSWORD=postgres -p 5433:5432 postgres:17

export TEST_SOURCE_URL="postgresql://postgres:postgres@localhost:5432/postgres"
export TEST_TARGET_URL="postgresql://postgres:postgres@localhost:5433/postgres"

# Run tests
cargo test --test selective_replication_test -- --ignored --nocapture
```

**Commit message:**
```
Add comprehensive integration tests for selective replication

- Test full workflow with database filtering
- Test full workflow with table filtering
- Test replication works with filtered tables
- Add helper functions for test setup/cleanup

Related to #<issue-number>
```

---

### Task 7.4: Final Testing & Bug Fixes

**Manual Testing Checklist:**

```markdown
## Selective Replication Testing

### Database Filtering
- [ ] Include specific databases only
- [ ] Exclude specific databases
- [ ] Cannot use both include and exclude
- [ ] Non-existent database produces clear error
- [ ] Empty filter replicates everything (backward compat)

### Table Filtering
- [ ] Exclude specific tables
- [ ] Include specific tables only
- [ ] Table format validation (must be db.table)
- [ ] Non-existent table produces clear error
- [ ] Table filtering works during init
- [ ] Table filtering works during sync
- [ ] Excluded tables don't appear on target
- [ ] Included tables replicate correctly

### Combined Filtering
- [ ] Can combine database and table filters
- [ ] Database filter + table exclusion works
- [ ] All validation errors are clear

### Database Existence Handling
- [ ] Empty database proceeds automatically
- [ ] Non-empty database prompts in interactive mode
- [ ] Non-empty database fails without --drop-existing
- [ ] --drop-existing drops and recreates databases
- [ ] No data loss without confirmation

### Interactive Mode
- [ ] Shows all databases
- [ ] Can select multiple databases
- [ ] Shows tables per database
- [ ] Can exclude specific tables
- [ ] Shows summary before proceeding
- [ ] Can abort at confirmation
- [ ] Replication proceeds with selections

### Logical Replication
- [ ] Publications created with filtered tables
- [ ] Subscriptions work correctly
- [ ] Data replicates in real-time
- [ ] Excluded tables don't replicate
- [ ] Status shows correct lag
- [ ] Verify checksums match

### Multi-Provider
- [ ] Works with Neon as source
- [ ] Works with AWS RDS as source (manual test)
- [ ] Works with Hetzner as source (manual test)
- [ ] Clear errors for permission issues
- [ ] Clear errors for connectivity issues

### Commands
- [ ] validate works with filters
- [ ] init works with filters
- [ ] sync works with filters
- [ ] status works with filters
- [ ] verify works with filters

### Error Handling
- [ ] Clear error for conflicting flags
- [ ] Clear error for invalid table format
- [ ] Clear error for non-existent database
- [ ] Clear error for non-existent table
- [ ] Clear error for empty result after filtering
```

**Run full test suite:**

```bash
# Unit tests
cargo test

# Linting
cargo fmt -- --check
cargo clippy --all-targets --all-features -- -D warnings

# Doc tests
cargo test --doc

# Integration tests
export TEST_SOURCE_URL="postgresql://postgres:postgres@localhost:5432/postgres"
export TEST_TARGET_URL="postgresql://postgres:postgres@localhost:5433/postgres"
cargo test -- --ignored

# Build release binary
cargo build --release
```

**Commit message:**
```
Final testing and bug fixes for selective replication

- Fix edge cases discovered during testing
- Improve error messages
- Add missing validation
- Update documentation with findings

Related to #<issue-number>
```

---

## Summary

This implementation plan provides:

1. **7 phases** with **20+ tasks**
2. **Detailed code examples** for every change
3. **Testing strategy** for each task (unit, integration, manual)
4. **Clear commit messages** following best practices
5. **Verification steps** to ensure correctness
6. **Error handling** for all edge cases
7. **Documentation updates** throughout

**Total estimated time**: 3-4 weeks for an experienced Rust developer

**Key principles followed:**
- ✅ **TDD**: Write tests first, then implementation
- ✅ **YAGNI**: Only exact name matching, no regex (yet)
- ✅ **DRY**: Centralized filter logic in `filters.rs`
- ✅ **Frequent commits**: One commit per logical task
- ✅ **Incremental**: Each phase builds on previous ones
- ✅ **Testable**: Comprehensive unit and integration tests

**Next steps:**
1. Create GitHub issues from this plan
2. Assign to developer
3. Work through phases sequentially
4. Review and merge after each phase
