# Changelog

All notable changes to this project will be documented in this file.

## [7.0.8] - 2025-12-09

### Fixed

- **Foreign key constraint violations during parallel restore**: Fixed bug where `pg_restore --jobs=N` could fail with FK constraint violations when restoring tables with foreign key relationships. Tables were being restored in parallel without respecting FK dependency order. Added `--disable-triggers` flag to temporarily disable FK constraints during data restore, then re-enable them after completion.

## [7.0.7] - 2025-12-09

### Fixed

- **Large row JSONB batch insert failures**: Fixed connection failures when replicating tables with large JSONB payloads (SQLite, MongoDB, MySQL sources). The batch insert now uses adaptive sizing that automatically calculates optimal batch sizes based on row payload sizes, targeting ~10MB per batch. On connection failures, automatically falls back to row-by-row insertion for the affected batch.

- **Duplicate key errors during data restoration**: Fixed bug where `pg_restore` data restoration could cause duplicate key constraint violations when a transient failure occurred during restore. The retry logic was incorrectly applied to `pg_restore --data-only`, which is not idempotent (partially inserted data would be re-inserted on retry). Data restoration now fails immediately on error with clear guidance to re-run with `--drop-existing` for a clean database.

### Changed

- **Progress logging for large datasets**: Added progress logging (every 50,000 rows) when inserting large tables to provide visibility during long-running migrations.

## [7.0.6] - 2025-12-09

### Fixed

- **Non-PostgreSQL sources incorrectly trigger remote execution**: Fixed bug where `init` command with SQLite, MongoDB, or MySQL sources would attempt SerenAI cloud execution (which requires the source to be accessible remotely). The tool now automatically uses local execution for non-PostgreSQL sources since these files/databases are only accessible from the local machine.

## [7.0.5] - 2025-12-09

### Fixed

- **Interactive mode fails for non-PostgreSQL sources**: Fixed bug where `init`, `validate`, and `sync` commands would fail for SQLite, MongoDB, and MySQL sources because interactive mode attempted to connect to the source as PostgreSQL. The tool now detects the source type before entering interactive mode and automatically skips it for non-PostgreSQL sources.

### Changed

- **README-SQLite.md**: Updated all examples to include `-y` flag and added notes explaining that interactive mode only works with PostgreSQL sources.

## [7.0.4] - 2025-12-09

### Fixed

- **`--include-tables` ignored in xmin sync**: Fixed bug where `--include-tables` and other CLI filter flags were ignored by the `sync` command when using xmin-based sync. The sync command now correctly respects table filters when CLI flags are provided, skipping interactive mode and passing filtered tables to xmin sync.

- **CTRL+C not responding during sync**: Fixed bug where CTRL+C would not interrupt a running sync cycle. The daemon now checks for shutdown signals during sync and reconciliation cycles, allowing graceful termination at any point.

## [7.0.3] - 2025-12-08

### Fixed

- **Windows build failure**: Removed unused tracing code that caused E0282 type inference error in Windows-only daemon initialization code.

- **PostgreSQL array type handling in xmin sync**: Fixed "cannot convert between String and _text" errors by properly handling PostgreSQL array types (`text[]`, `integer[]`, `bigint[]`, etc.). Added support for 15+ array types including `_text`, `_int4`, `_int8`, `_float8`, `_bool`, `_uuid`, `_numeric`, `_jsonb`, `_timestamp`, and more.

- **Accurate PostgreSQL type detection**: Changed from using `data_type` (which returns generic "ARRAY" for all arrays) to `udt_name` in `information_schema.columns`, which returns specific types like `_text`, `_int4`, enabling correct array element type handling.

- **Numeric type handling**: Fixed "value too large to fit in target type" errors by using `rust_decimal` for PostgreSQL `numeric`/`decimal` columns instead of f64.

- **Large batch handling**: Fixed batch size issues that could cause sync failures on tables with many columns.

- **SQLite/MongoDB/MySQL init idempotency**: Fixed "relation already exists" errors when re-running `init` for JSONB-based sources (SQLite, MongoDB, MySQL). Now truncates existing tables instead of failing, making init idempotent.

### Changed

- **Default sync intervals**: Changed default sync interval from 60 seconds to 1 hour (3600s), and default reconciliation interval from 1 hour to 1 day (86400s) for more production-appropriate defaults.

## [7.0.1] - 2025-12-08

### Fixed

- **xmin sync reconciliation crash on missing target tables**: Fixed bug where the reconciliation cycle would crash with "Failed to get target primary keys" if a table existed in the source but not in the target (e.g., tables that were filtered during init, or failed to create). Reconciliation now checks if each table exists on the target before attempting to query it.

## [7.0.0] - 2025-12-08

### Added

- **Automatic xmin-based sync fallback**: The `sync` command now automatically detects your source database's `wal_level` and chooses the optimal sync method. When `wal_level=logical` is not available (the default for most managed PostgreSQL services like Neon, AWS RDS, Heroku), the tool automatically falls back to xmin-based incremental sync - **no source database configuration required**.

- **xmin sync module** (`src/xmin/`): New module implementing PostgreSQL xmin-based change detection:
  - `XminReader`: Reads changed rows using PostgreSQL's `xmin` system column
  - `ChangeWriter`: Applies changes to target via efficient batched UPSERTs
  - `Reconciler`: Detects deleted rows via primary key reconciliation
  - `SyncDaemon`: Continuous background sync with configurable intervals
  - `SyncState`: Persistent state for resume after interruption

- **xmin wraparound handling**: Automatic detection of PostgreSQL transaction ID wraparound (32-bit limit ~4 billion) with full table resync when detected - prevents silent data loss.

- **Comprehensive integration tests** for xmin sync lifecycle (`tests/xmin_integration_test.rs`)

### Changed

- **Sync command UX**: Zero configuration required - just run `database-replicator sync` and it works regardless of source database `wal_level` setting

### Documentation

- Updated README.md and README-PostgreSQL.md with:
  - Automatic sync method detection tables
  - New "xmin-Based Sync" section with full technical details
  - FAQ entries explaining xmin sync trade-offs vs logical replication

## [6.0.7] - 2025-12-08

### Fixed

- **Auto-discover SerenDB project from target URL**: Fixed bug where `sync` command couldn't auto-enable logical replication when using explicit `--target` SerenDB URL without saved state. The tool now discovers the project by matching the target hostname against SerenDB project connection strings. Also prompts for API key interactively if not provided. (Fixes #54)

- **Verify wal_level after enabling logical replication**: After enabling logical replication via SerenDB API, the tool now polls the database to verify `wal_level=logical` is actually applied (up to 60 seconds), with helpful instructions if the endpoint needs manual restart.

## [6.0.6] - 2025-12-08

### Fixed

- **Sync command now uses saved target state**: Fixed bug where `sync` required manual `--project-id` even after `init` captured it interactively. Now `init` saves TargetState to `.seren-replicator/target.json`, and `sync` automatically loads the `project_id` from saved state for auto-enabling logical replication. (Fixes #53)

## [6.0.5] - 2025-12-07

### Fixed

- **SerenDB interactive target selection**: Fixed multiple issues with the interactive project/database selector:
  - Fixed API response parsing for connection strings (wrapped in `data` object)
  - Fixed target state passing to remote execution (no longer fails with "Missing required field: target_url")
  - Added branch selection when multiple branches exist in a project
  - Cached API key after first prompt to avoid duplicate prompts

## [6.0.4] - 2025-12-07

### Fixed

- **Allow replication to empty SerenDB projects**: The interactive target selector now allows specifying a new database name when the selected project has no databases. Previously it would error with "has no databases in its default branch". Now it prompts for a database name (default: `serendb`) that will be created during replication.

## [6.0.3] - 2025-12-07

### Fixed

- **SerenDB API base URL**: Fixed the API base URL from `console.serendb.com` to `api.serendb.com`. This resolves the "Resource not found" error when using the interactive project selector with a SerenDB API key.

## [6.0.2] - 2025-12-07

### Fixed

- **Windows release build fix**: Fixed the automated release workflow to properly build Windows binaries by adding Cargo.toml patching for rusqlite bundled feature and home crate constraints.

## [6.0.0] - 2025-12-07

### Added

- **Windows x64 binary support**: Pre-built Windows binaries are now included in releases. The build uses bundled SQLite to avoid external dependencies. ([#51](https://github.com/serenorg/database-replicator/issues/51))

- **Docker support**: Official Docker images are now available for containerized deployments. Includes multi-stage build for minimal image size and support for running replication jobs in containers. ([#49](https://github.com/serenorg/database-replicator/issues/49), [#50](https://github.com/serenorg/database-replicator/issues/50), [#52](https://github.com/serenorg/database-replicator/issues/52))

- **SerenDB API key authentication**: Added `--api-key` flag for authenticating with SerenDB Console API when enabling logical replication via `--project-id`. ([#48](https://github.com/serenorg/database-replicator/issues/48))

### Fixed

- **Interactive selection defaults**: Fixed interactive database/table selection to default to all items selected instead of none, improving the user experience for common use cases.

## [5.3.20] - 2025-12-05

### Added

- **SerenDB Console API integration for `sync` command**: When using `--project-id` flag with a SerenDB target, the tool now automatically checks if logical replication is enabled on the project. If not enabled, it prompts the user to enable it via the SerenDB Console API before proceeding. This prevents the common error where `sync` fails because `wal_level` is set to `replica` instead of `logical`. ([#27](https://github.com/serenorg/database-replicator/issues/27))

## [5.3.19] - 2025-12-04

### Fixed

- **Fix `database_is_empty()` checking wrong database**: When checking if an existing target database is empty, the function now connects to the specific target database (target_db_url) instead of the default connection database (target_url). Previously, this bug caused the check to always return true because it was querying tables in the wrong database, potentially leading to data loss when the target was not actually empty.

- **Add 30-second timeout to `database_is_empty()` query**: Prevents indefinite hangs on stale serverless connections (SerenDB, Neon) by wrapping the information_schema query in a tokio timeout. If the query doesn't complete within 30 seconds, it fails with a clear timeout error instead of hanging forever.

## [5.3.18] - 2025-12-04

### Changed

- Internal version bump for CI testing (no functional changes).

## [5.3.17] - 2025-12-03

### Fixed

- **Connection hangs on serverless databases**: The replication process no longer hangs when connecting to serverless PostgreSQL providers like Neon or SerenDB. The fix involves two changes:
  1.  **Short-lived connections**: Pre-flight checks now use short-lived connections that are immediately closed, preventing connection pool exhaustion.
  2.  **Connection timeout**: A 30-second connection timeout has been added to the `psql` restore command to prevent indefinite hangs. ([70b4395](https://github.com/serenorg/database-replicator/commit/70b439585908b9e5f015e19d6554200013cb0053))

## [5.3.16] - 2025-12-03

### Fixed

- **Silent failure during globals restore**: The `restore_globals` function now captures `stderr` from `psql` to provide clear error messages when the restore process fails. Previously, `stderr` was inherited, causing the application to hang silently without displaying the underlying error. Non-fatal notices (e.g., "role already exists") are now logged as warnings, and the process continues. ([706e81d](https://github.com/serenorg/database-replicator/commit/706e81df2215c7e090a21051512a5241e3d748f5))

## [5.3.15] - 2025-12-03

### Fixed

- **Fix connection hang during preflight checks for SerenDB/Neon targets**: Preflight now opens at most one source and one target connection, reusing them across all connectivity and permission checks. Dropping those clients before running `pg_dump*` eliminates the connection pool exhaustion that previously caused `pg_dumpall` to hang indefinitely on serverless PostgreSQL providers with strict connection limits.
- **Mitigate connection pool exhaustion hangs with timeout**: All `pg_dump` and `pg_dumpall` commands now execute with a 30-second connection timeout (`PGCONNECT_TIMEOUT=30`). This prevents indefinite hangs if the connection pool is exhausted and makes failures detectable instead of causing silent hangs.

## [5.3.14] - 2025-12-03

### Fixed

- **Fix connection hang in `database_is_empty()` for SerenDB/Neon targets**: The `database_is_empty()` function now reuses the existing database connection instead of creating a new one. This prevents indefinite hangs when connecting to serverless PostgreSQL providers (SerenDB, Neon) that have strict connection pool limits, where attempting to create a second connection while the first is active would exhaust the pool and cause `tokio_postgres::connect` to hang forever (no built-in timeout).

## [5.3.13] - 2025-12-03

### Fixed

- **Clear error message when SUPERUSER connections block DROP DATABASE**: Before attempting to drop a database, the tool now checks if any connections remain after terminating regular user sessions. If SUPERUSER connections cannot be terminated (common on AWS RDS and SerenDB), the tool now fails early with a clear error message explaining the issue and providing resolution steps, instead of letting `DROP DATABASE` fail with an obscure "database is being accessed by other users" error.

## [5.3.12] - 2025-12-02

### Fixed

- **Skip SUPERUSER connections when dropping databases**: When dropping an existing database with `--drop-existing`, the tool now skips terminating connections owned by SUPERUSER roles. This prevents "permission denied to terminate process" errors on managed PostgreSQL services like AWS RDS and SerenDB where regular users cannot terminate superuser sessions.

## [5.3.11] - 2025-12-02

### Fixed

- **Skip AWS RDS internal database during discovery**: The `rdsadmin` database (AWS RDS's internal administration database) is now automatically excluded from database discovery, preventing "pg_hba.conf rejects connection" errors when replicating from AWS RDS sources.

- **Infer database list from `--include-tables`**: When using `--include-tables` without `--include-databases`, the tool now automatically extracts database names from the table specifications (e.g., `--include-tables "mydb.table1,mydb.table2"` will only replicate the `mydb` database). Previously, all databases were enumerated even when only specific tables were requested.

## [5.3.10] - 2025-12-02

### Improved

- **Better connection error diagnostics**: When database connections fail with generic "db error" messages, the tool now extracts detailed error information including PostgreSQL error codes, underlying causes, and debug representations. This helps diagnose connection issues more quickly, especially with AWS RDS and other managed PostgreSQL services.

## [5.3.9] - 2025-12-02

### Fixed

- **Handle unquoted RDS tablespace references**: Extended tablespace filtering to also catch unquoted references like `SECURITY LABEL ON TABLESPACE rds_temp_tablespace` and `GRANT ON TABLESPACE rds_temp_tablespace`. Previously only quoted forms (`'rds_*'` and `"rds_*"`) were filtered.

## [5.3.8] - 2025-12-02

### Fixed

- **Skip all RDS tablespace references during globals restore**: Any statement referencing AWS RDS-specific tablespaces (`rds_*`) is now automatically commented out. This catches `ALTER ROLE ... SET default_tablespace = 'rds_temp_tablespace'` and similar statements that fail on non-RDS targets.

## [5.3.7] - 2025-12-02

### Fixed

- **Skip CREATE TABLESPACE statements during globals restore**: `CREATE TABLESPACE` statements are now automatically commented out when restoring to managed PostgreSQL targets like SerenDB that do not support custom tablespaces.

## [5.3.6] - 2025-12-02

### Fixed

- **Skip GRANT statements with restricted GRANTED BY clauses**: `GRANT` statements that include `GRANTED BY rdsadmin`, `GRANTED BY rds_superuser`, or similar RDS admin roles are now automatically commented out during globals restore, preventing "permission denied to grant privileges as role" errors on AWS RDS targets.

## [5.3.5] - 2025-12-02

### Fixed

- **Extended restricted role grant handling**: Expanded the list of restricted PostgreSQL roles that are automatically skipped during globals restore to include `pg_checkpoint`, `pg_read_all_data`, `pg_write_all_data`, `pg_read_all_settings`, `pg_read_all_stats`, `pg_stat_scan_tables`, `pg_monitor`, `pg_signal_backend`, `pg_read_server_files`, `pg_write_server_files`, `pg_execute_server_program`, `pg_create_subscription`, `pg_maintain`, and `pg_use_reserved_connections`. Also fixed quote handling so quoted role names (e.g., `"pg_checkpoint"`) are properly matched.

## [5.3.4] - 2025-12-02

### Fixed

- **PostgreSQL globals restores no longer fail on `GRANT pg_checkpoint`**: `GRANT` statements for the `pg_checkpoint` role are now commented out, preventing permission denied errors on managed PostgreSQL services like AWS RDS.

## [5.3.3] - 2025-12-02

### Fixed

- **PostgreSQL globals restores no longer fail due to GUC case sensitivity**: `ALTER ROLE ... SET` commands in `pg_dumpall` output are now sanitized with a case-insensitive check, preventing replication failures on managed PostgreSQL services that restrict GUC changes.

## [5.3.2] - 2025-12-02

### Fixed

- **PostgreSQL globals restores no longer fail on `auto_explain.log_min_duration`**: globals sanitization now comments out `ALTER ROLE ... SET auto_explain.log_min_duration` (and similar privileged parameters) so `database-replicator init` can rerun cleanly against managed Postgres targets that restrict GUC changes.

## [5.3.1] - 2025-12-01

### Fixed

- **PostgreSQL globals restores no longer fail on `log_min_messages`**: globals sanitization now comments out `ALTER ROLE ... SET log_min_messages` (and similar privileged parameters) so `database-replicator init` can rerun cleanly against managed Postgres targets that restrict GUC changes.

## [5.3.0] - 2025-11-29

### Added

- **CLI log level control**: a new global `--log` flag allows users to set the log level (error, warn, info, debug, trace) for both local and remote executions, providing more detailed output for debugging.

## [5.2.5] - 2025-11-29

### Fixed

- **PostgreSQL globals restores no longer fail on `log_min_error_statement`**: globals sanitization now comments out `ALTER ROLE ... SET log_min_error_statement` (and similar privileged parameters) so `database-replicator init` can rerun cleanly against managed Postgres targets that restrict GUC changes.

## [5.2.4] - 2025-11-29

### Fixed

- **AWS RDS globals restores no longer fail on `log_statement`**: globals sanitization now comments out `ALTER ROLE ... SET log_statement` (and similar privileged parameters) so `database-replicator init` can rerun cleanly against managed Postgres targets that restrict GUC changes.

## [5.2.3] - 2025-11-29

### Fixed

- **Globals restore SUPERUSER errors**: replicate `pg_dumpall` globals now have any `ALTER ROLE ... SUPERUSER` statements commented out, preventing AWS RDS and other managed targets from failing during `database-replicator init`.

## [3.0.1] - 2025-11-23

### Fixed

#### Security

- **Upgraded mongodb crate from 2.8.2 to 3.4.1** to resolve security vulnerabilities
  - Fixed RUSTSEC-2024-0421: `idna 0.2.3` vulnerability (Punycode domain label handling)
  - Removed unmaintained `derivative 2.2.0` dependency (RUSTSEC-2024-0388)
  - Updated MongoDB API calls for 3.x compatibility
  - All security tests passing (43/43)
  - `cargo audit` clean with no vulnerabilities

#### Critical Bug Fixes

- **Fixed broken remote API endpoint** that prevented remote execution from working
  - Changed from non-existent `https://api.seren.cloud/replication` to actual deployed endpoint
  - Remote execution now uses real AWS API Gateway infrastructure
  - Updated all documentation and integration tests with correct endpoint
  - Infrastructure redeployed via Terraform

### Changed

- MongoDB connection API updated to remove deprecated parameters:
  - `run_command()` now takes 1 argument instead of 2
  - `find()` now takes `Document::new()` instead of `(None, None)`
  - `list_collection_names()` now takes 0 arguments
  - `estimated_document_count()` now takes 0 arguments

## [3.0.0] - 2025-11-22

### Added - Major Features

#### SQLite Support (Phase 1)

- **One-time migration** of SQLite databases to PostgreSQL with JSONB storage
- **Automatic type conversion**: INTEGER, REAL, TEXT, BLOB, NULL → JSONB
- **File-based migration** (local execution only, no remote support)
- **Path validation** with directory traversal prevention
- **Comprehensive security testing**: 14 SQLite-specific tests
- **Documentation**: [README-SQLite.md](README-SQLite.md) with usage examples
- **Integration tests**: Full workflow testing with real SQLite files

#### MongoDB Support (Phase 2)

- **One-time migration** of MongoDB databases to PostgreSQL with JSONB storage
- **Periodic refresh support**: 24-hour default (configurable)
- **Remote execution support**: Run migrations on SerenAI cloud infrastructure
- **BSON type conversion**: ObjectId, DateTime, Binary, Regex, Embedded Documents, Arrays → JSONB
- **Scheduler infrastructure**: Cron-like periodic refresh system
- **Comprehensive security testing**: 11 MongoDB-specific tests
- **Documentation**: [README-MongoDB.md](README-MongoDB.md) with periodic refresh guide
- **Integration tests**: Full workflow testing with real MongoDB connections

#### MySQL/MariaDB Support (Phase 3)

- **One-time migration** of MySQL/MariaDB databases to PostgreSQL with JSONB storage
- **Periodic refresh support**: 24-hour default (configurable)
- **Remote execution support**: Run migrations on SerenAI cloud infrastructure
- **MySQL type conversion**: INT, VARCHAR, DATETIME, BLOB, DECIMAL, ENUM, SET, JSON → JSONB
- **Full MariaDB compatibility**: Works with both MySQL and MariaDB
- **Comprehensive security testing**: 18 MySQL-specific tests
- **Documentation**: [README-MySQL.md](README-MySQL.md) with MariaDB examples
- **Integration tests**: Full workflow testing with real MySQL connections

#### Shared Infrastructure (All Phases)

- **JSONB utilities module** (`src/jsonb/`): Shared conversion, writing, and schema utilities
- **Source type auto-detection**: Automatic detection from connection strings (SQLite path, mongodb://, mysql://, postgresql://)
- **Enhanced remote execution**: MongoDB and MySQL now support remote execution
- **Periodic refresh scheduler** (`src/scheduler/`): Background job system for periodic migrations
- **Security audit framework**: 43 total security tests (25 SQLite + 11 MongoDB + 18 MySQL + existing PostgreSQL)
- **Performance testing framework**: 13 benchmarks across all database types

### Changed

- **Main README.md** rewritten as universal landing page
  - Clear tagline: "Universal database-to-PostgreSQL replication for AI agents"
  - Supported databases comparison table (4 database types)
  - Quick start examples for each database type
  - Prominent links to database-specific guides
  - Reduced from ~1000 to ~550 lines
- **PostgreSQL documentation** extracted to dedicated guide ([README-PostgreSQL.md](README-PostgreSQL.md))
  - 1,000+ line comprehensive guide
  - All PostgreSQL-specific features documented
  - Main README now links to this guide
- **CLI auto-detects source database type** from connection string
  - SQLite: Local file path detection
  - MongoDB: `mongodb://` protocol detection
  - MySQL: `mysql://` protocol detection
  - PostgreSQL: `postgresql://` or `postgres://` protocol detection
- **`init` command** now supports all 4 database types with automatic routing
- **`sync` command** supports periodic refresh for MongoDB and MySQL
- **`validate` command** supports validation for all database types

### Security (Phase 4.3)

- **Comprehensive security audit** completed and signed off ([docs/security-audit-report.md](docs/security-audit-report.md))
- **Connection string validation** for all database types with injection prevention
- **Credential redaction** in logs and error messages for all database types
- **Path traversal prevention** for SQLite file paths
- **SQL/NoSQL injection prevention**: Parameterized queries for all databases
- **Command injection prevention**: No shell commands with user input
- **KMS encryption** for MongoDB and MySQL credentials in remote execution
- **43 security tests** covering all attack vectors across all database types
- **Dependency audit**: All dependencies scanned, 1 low-risk finding documented

### Performance (Phase 4.4)

- **Performance test framework** implemented ([tests/performance_test.rs](tests/performance_test.rs))
  - 13 benchmarks across SQLite, MongoDB, MySQL
  - Performance targets defined for all database sizes
  - Automated test database generation scripts
- **Batch JSONB inserts** optimized for high throughput
- **Performance report template** created ([docs/performance-report.md](docs/performance-report.md))
  - Hardware/environment documentation
  - Baseline metrics for regression testing
  - Performance tuning recommendations

### Documentation (Phase 4.1 & 4.2)

- **[README.md](README.md)** - Universal landing page with multi-database support
- **[README-PostgreSQL.md](README-PostgreSQL.md)** - Comprehensive PostgreSQL replication guide (1,000+ lines)
- **[README-SQLite.md](README-SQLite.md)** - Complete SQLite migration guide
- **[README-MongoDB.md](README-MongoDB.md)** - Complete MongoDB migration guide with periodic refresh
- **[README-MySQL.md](README-MySQL.md)** - Complete MySQL/MariaDB migration guide
- **[docs/plans/multi-database-support.md](docs/plans/multi-database-support.md)** - Implementation plan and architecture
- **[docs/security-audit-report.md](docs/security-audit-report.md)** - Comprehensive security audit report
- **[docs/performance-report.md](docs/performance-report.md)** - Performance testing framework and results

### Fixed

- MongoDB connection URL validation now properly handles injection attempts
- MySQL backtick quoting prevents SQL injection in table names
- SQLite path validation prevents directory traversal attacks
- Error messages sanitize credentials across all database types

### Breaking Changes

⚠️ **Version 3.0.0 introduces breaking changes:**

- **Repository renamed**: `postgres-seren-replicator` → `seren-replicator`
  - GitHub repository: `serenorg/postgres-seren-replicator` → `serenorg/seren-replicator`
  - Binary name: `postgres-seren-replicator` → `seren-replicator`
  - Package name: `postgres-seren-replicator` → `seren-replicator`
  - Old URLs automatically redirect to new repository
- **Main README.md structure changed**: Now a landing page with links to database-specific guides
- **CLI output format may differ**: Source type detection added to output messages
- **Remote execution job spec**: Added `source_type` field for multi-database support
- **Documentation structure**: PostgreSQL-specific content moved to README-PostgreSQL.md

**Compatibility Note**: Existing PostgreSQL-to-PostgreSQL workflows are **NOT affected** and work exactly as before. All breaking changes affect only documentation structure and CLI output format.

### Migration Guide (for users upgrading from 2.x)

**No action required for PostgreSQL users**. Your existing workflows continue to work without changes.

**New capabilities available**:

- Use SQLite as a source: `seren-replicator init --source database.db --target "postgresql://..."`
- Use MongoDB as a source: `seren-replicator init --source "mongodb://..." --target "postgresql://..."`
- Use MySQL as a source: `seren-replicator init --source "mysql://..." --target "postgresql://..."`

**Documentation updates**:

- Main README is now a landing page - visit database-specific guides for detailed docs
- PostgreSQL documentation: See [README-PostgreSQL.md](README-PostgreSQL.md)

## [2.5.0] - 2025-11-20

### Added

- **Remote Execution (AWS)**: SerenAI-managed cloud service for running replication jobs
  - Remote-by-default execution mode with `--local` flag for local fallback
  - Job submission API with encrypted credentials via AWS KMS
  - Status polling and real-time progress monitoring
  - Automatic EC2 instance provisioning and termination
  - Integration tests for remote execution functionality
- **Job Spec Validation**: Comprehensive API validation framework
  - Schema versioning (current: v1.0) with backward compatibility
  - PostgreSQL URL security validation with injection prevention
  - Required field validation, command whitelist, and size limits (15KB max)
  - Test suite with 18 validation tests
  - API schema documentation at `docs/api-schema.md`
- **Observability**: Built-in monitoring and tracing
  - Trace ID generation for request tracking across systems
  - CloudWatch Logs integration with structured logging
  - CloudWatch Metrics for job lifecycle events
  - Log URLs returned in API responses for troubleshooting
- **CI/CD Improvements**: Enhanced testing and deployment
  - Smoke tests for AWS infrastructure validation
  - Environment-specific configurations (dev, staging, prod)
  - Comprehensive CI/CD documentation at `docs/cicd.md`
  - Automated release workflows
- **Security Features**: Enterprise-grade security controls
  - KMS encryption for database credentials at rest in DynamoDB
  - Credential redaction in all logs and outputs
  - IAM role-based access with least-privilege policies
  - API key authentication via SSM Parameter Store
  - Security model documentation at `docs/aws-setup.md`
- **Reliability Controls**: Production-ready resilience
  - Job timeout controls (default: 8 hours, configurable)
  - Maximum instance runtime limits to prevent runaway costs
  - Graceful error handling with detailed error messages
  - Connection retry with exponential backoff
- **Documentation**: Comprehensive user and developer guides
  - Remote execution guide in README with usage examples
  - AWS setup guide (23KB) for infrastructure deployment
  - API schema specification with migration guidance
  - CI/CD pipeline documentation
  - SerenDB signup instructions (optional target database)

### Changed

- `init` command now uses remote execution by default (use `--local` to run locally)
- Job specifications now require `schema_version` field (current: "1.0")
- API endpoint uses SerenDB's managed infrastructure

### Improved

- Error messages now include trace IDs for support ticket correlation
- Job submission failures provide clear fallback instructions (`--local` flag)
- CloudWatch integration enables post-mortem debugging of failed jobs
- Cost management with automatic instance termination after completion

## [1.2.0] - 2025-11-07
### Added
- Table-level replication rules with new CLI flags (`--schema-only-tables`, `--table-filter`, `--time-filter`) and TOML config support (`--config`).
- Filtered snapshot pipeline that streams predicate-matching rows and skips schema-only tables during `init`.
- Predicate-aware publications for `sync`, enabling logical replication that respects table filters on PostgreSQL 15+.
- TimescaleDB-focused documentation at `docs/replication-config.md` plus expanded README guidance.

### Improved
- Multi-database `init` now checkpoints per-table progress and resumes from the last successful database.

## [1.1.1] - 2025-11-07
- Previous improvements and fixes bundled with v1.1.1.
