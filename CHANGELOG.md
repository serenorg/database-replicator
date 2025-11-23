# Changelog

All notable changes to this project will be documented in this file.

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

- **Main README.md structure changed**: Now a landing page with links to database-specific guides
- **CLI output format may differ**: Source type detection added to output messages
- **Remote execution job spec**: Added `source_type` field for multi-database support
- **Documentation structure**: PostgreSQL-specific content moved to README-PostgreSQL.md

**Compatibility Note**: Existing PostgreSQL-to-PostgreSQL workflows are **NOT affected** and work exactly as before. All breaking changes affect only documentation structure and CLI output format.

### Migration Guide (for users upgrading from 2.x)

**No action required for PostgreSQL users**. Your existing workflows continue to work without changes.

**New capabilities available**:

- Use SQLite as a source: `postgres-seren-replicator init --source database.db --target "postgresql://..."`
- Use MongoDB as a source: `postgres-seren-replicator init --source "mongodb://..." --target "postgresql://..."`
- Use MySQL as a source: `postgres-seren-replicator init --source "mysql://..." --target "postgresql://..."`

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
- API endpoint standardized to `https://api.seren.cloud/replication`

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
