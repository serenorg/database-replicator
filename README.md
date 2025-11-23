# postgres-seren-replicator

[![CI](https://github.com/serenorg/postgres-seren-replicator/actions/workflows/ci.yml/badge.svg)](https://github.com/serenorg/postgres-seren-replicator/actions/workflows/ci.yml)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Rust Version](https://img.shields.io/badge/rust-1.70%2B-blue.svg)](https://www.rust-lang.org)
[![Latest Release](https://img.shields.io/github/v/release/serenorg/postgres-seren-replicator)](https://github.com/serenorg/postgres-seren-replicator/releases)

## Universal database-to-PostgreSQL replication for AI agents

Migrate any database to PostgreSQL with zero downtime. Supports PostgreSQL, SQLite, MongoDB, and MySQL/MariaDB.

---

## Overview

`postgres-seren-replicator` is a command-line tool that replicates databases from multiple sources to PostgreSQL (including Seren Cloud). It automatically detects your source database type and handles the migration accordingly:

- **PostgreSQL**: Zero-downtime replication with continuous sync via logical replication
- **SQLite**: One-time migration using JSONB storage
- **MongoDB**: One-time migration with JSONB storage and periodic refresh support
- **MySQL/MariaDB**: One-time migration with JSONB storage and periodic refresh support

### Why This Tool?

- **Multi-database support**: Single tool for all your database migrations
- **AI-friendly storage**: Non-PostgreSQL sources use JSONB for flexible querying
- **Zero downtime**: PostgreSQL-to-PostgreSQL replication with continuous sync
- **Remote execution**: Run migrations on SerenAI cloud infrastructure
- **Production-ready**: Data integrity verification, checkpointing, and error handling

---

## Supported Databases

| Source Database | Migration Type | Continuous Sync | Periodic Refresh | Remote Execution |
|----------------|----------------|-----------------|------------------|------------------|
| **PostgreSQL** | Native replication | ✅ Logical replication | N/A | ✅ Yes |
| **SQLite** | JSONB storage | ❌ One-time | ❌ No | ❌ Local only |
| **MongoDB** | JSONB storage | ❌ One-time | ✅ 24hr default | ✅ Yes |
| **MySQL/MariaDB** | JSONB storage | ❌ One-time | ✅ 24hr default | ✅ Yes |

---

## Quick Start

Choose your source database to get started:

### PostgreSQL → PostgreSQL

Zero-downtime replication with continuous sync:

```bash
postgres-seren-replicator init \
  --source "postgresql://user:pass@source-host:5432/db" \
  --target "postgresql://user:pass@target-host:5432/db"
```

**[📖 Full PostgreSQL Guide →](README-PostgreSQL.md)**

---

### SQLite → PostgreSQL

One-time migration to JSONB storage:

```bash
postgres-seren-replicator init \
  --source /path/to/database.db \
  --target "postgresql://user:pass@host:5432/db"
```

**[📖 Full SQLite Guide →](README-SQLite.md)**

---

### MongoDB → PostgreSQL

One-time migration with periodic refresh support:

```bash
postgres-seren-replicator init \
  --source "mongodb://user:pass@host:27017/db" \
  --target "postgresql://user:pass@host:5432/db"
```

**[📖 Full MongoDB Guide →](README-MongoDB.md)**

---

### MySQL/MariaDB → PostgreSQL

One-time migration with periodic refresh support:

```bash
postgres-seren-replicator init \
  --source "mysql://user:pass@host:3306/db" \
  --target "postgresql://user:pass@host:5432/db"
```

**[📖 Full MySQL Guide →](README-MySQL.md)**

---

## Features

### PostgreSQL-to-PostgreSQL

- **Zero-downtime replication** using PostgreSQL logical replication
- **Continuous sync** keeps databases in sync in real-time
- **Selective replication** with database and table-level filtering
- **Interactive mode** for selecting databases and tables
- **Remote execution** on SerenAI cloud infrastructure
- **Data integrity verification** with checksums

### Non-PostgreSQL Sources (SQLite, MongoDB, MySQL)

- **JSONB storage** preserves data fidelity for querying in PostgreSQL
- **Type preservation** with special encoding for complex types
- **One-time migration** for initial data transfer
- **Periodic refresh** (MongoDB, MySQL) for keeping data up to date
- **Schema-aware filtering** for precise table targeting
- **Remote execution** (MongoDB, MySQL) on cloud infrastructure

### Universal Features

- **Multi-provider support**: Works with any PostgreSQL provider (Neon, AWS RDS, Hetzner, self-hosted)
- **Size estimation**: Analyze database sizes before migration
- **High performance**: Parallel operations with automatic CPU detection
- **Checkpointing**: Resume interrupted migrations automatically
- **Security**: Credentials passed via `.pgpass` files, never in command output

---

## Installation

### Download Pre-built Binaries

Download the latest release for your platform from [GitHub Releases](https://github.com/serenorg/postgres-seren-replicator/releases/latest):

- **Linux (x64)**: `postgres-seren-replicator-linux-x64-binary`
- **macOS (Intel)**: `postgres-seren-replicator-macos-x64-binary`
- **macOS (Apple Silicon)**: `postgres-seren-replicator-macos-arm64-binary`

Make the binary executable:

```bash
chmod +x postgres-seren-replicator-*-binary
./postgres-seren-replicator-*-binary --help
```

### Build from Source

Requires Rust 1.70 or later:

```bash
git clone https://github.com/serenorg/postgres-seren-replicator.git
cd postgres-seren-replicator
cargo build --release
```

The binary will be available at `target/release/postgres-seren-replicator`.

### Prerequisites

- **PostgreSQL client tools** (pg_dump, pg_dumpall, psql) - Required for all database types
- **Source database access**: Connection credentials and appropriate permissions
- **Target database access**: PostgreSQL connection with write permissions

---

## Documentation

### Database-Specific Guides

- **[PostgreSQL to PostgreSQL](README-PostgreSQL.md)** - Zero-downtime replication with logical replication
- **[SQLite to PostgreSQL](README-SQLite.md)** - One-time migration using JSONB storage
- **[MongoDB to PostgreSQL](README-MongoDB.md)** - One-time migration with periodic refresh support
- **[MySQL/MariaDB to PostgreSQL](README-MySQL.md)** - One-time migration with periodic refresh support

### Additional Documentation

- **[Replication Configuration Guide](docs/replication-config.md)** - Advanced filtering with TOML config files
- **[AWS Setup Guide](docs/aws-setup.md)** - Remote execution infrastructure details
- **[CI/CD Guide](docs/cicd.md)** - Automated testing and deployment

---

## PostgreSQL-to-PostgreSQL Replication

For comprehensive PostgreSQL replication documentation, see **[README-PostgreSQL.md](README-PostgreSQL.md)**.

### Quick Overview

PostgreSQL-to-PostgreSQL replication uses logical replication for zero-downtime migration:

1. **Validate** - Check prerequisites and permissions
2. **Init** - Perform initial snapshot (schema + data)
3. **Sync** - Set up continuous logical replication
4. **Status** - Monitor replication lag and health
5. **Verify** - Validate data integrity with checksums

**Example:**

```bash
# Validate prerequisites
postgres-seren-replicator validate \
  --source "postgresql://user:pass@source:5432/db" \
  --target "postgresql://user:pass@target:5432/db"

# Initial snapshot
postgres-seren-replicator init \
  --source "postgresql://user:pass@source:5432/db" \
  --target "postgresql://user:pass@target:5432/db"

# Continuous sync
postgres-seren-replicator sync \
  --source "postgresql://user:pass@source:5432/db" \
  --target "postgresql://user:pass@target:5432/db"
```

**See [README-PostgreSQL.md](README-PostgreSQL.md) for:**

- Prerequisites and permission setup
- Detailed command documentation
- Selective replication (filtering databases/tables)
- Interactive mode
- Remote execution on cloud infrastructure
- Multi-provider support (Neon, AWS RDS, Hetzner, etc.)
- Schema-aware filtering
- Performance optimizations
- Troubleshooting guide
- Complete examples and FAQ

---

## Remote Execution (AWS)

By default, the `init` command uses **SerenAI's managed cloud service** to execute replication jobs. This means your replication runs on AWS infrastructure managed by SerenAI, with no AWS account or setup required on your part.

### Benefits of Remote Execution

- **No network interruptions**: Your replication continues even if your laptop loses connectivity
- **No laptop sleep**: Your computer can sleep or shut down without affecting the job
- **Faster performance**: Replication runs on dedicated cloud infrastructure closer to your databases
- **No local resource usage**: Your machine's CPU, memory, and disk are not consumed
- **Automatic monitoring**: Built-in observability with CloudWatch logs and metrics
- **Cost-free**: SerenAI covers all AWS infrastructure costs

### How It Works

When you run `init` without the `--local` flag, the tool:

1. **Submits your job** to SerenAI's API with encrypted credentials
2. **Provisions an EC2 worker** sized appropriately for your database
3. **Executes replication** on the cloud worker
4. **Monitors progress** and shows you real-time status updates
5. **Self-terminates** when complete to minimize costs

Your database credentials are encrypted with AWS KMS and never logged or stored in plaintext.

### Usage Example

Remote execution is the default - just run `init` as normal:

```bash
# Runs on SerenAI's cloud infrastructure (default)
./postgres-seren-replicator init \
  --source "postgresql://user:pass@source-host:5432/db" \
  --target "postgresql://user:pass@seren-host:5432/db"
```

The tool will:

- Submit the job to <https://api.seren.cloud/replication>
- Show you the job ID and trace ID for monitoring
- Poll for status updates and display progress
- Report success or failure when complete

Example output:

```text
Submitting replication job...
✓ Job submitted
Job ID: 550e8400-e29b-41d4-a716-446655440000
Trace ID: 660e8400-e29b-41d4-a716-446655440000

Polling for status...
Status: provisioning EC2 instance...
Status: running (1/2): myapp
Status: running (2/2): analytics

✓ Replication completed successfully
```

### Local Execution (Fallback)

If you prefer to run replication on your local machine, use the `--local` flag:

```bash
# Runs on your local machine
./postgres-seren-replicator init \
  --source "postgresql://user:pass@source-host:5432/db" \
  --target "postgresql://user:pass@seren-host:5432/db" \
  --local
```

Local execution is useful when:

- You're testing or developing
- Your databases are not accessible from the internet
- You need full control over the execution environment
- You're okay with keeping your machine running during the entire operation

### Advanced Configuration

#### Custom API endpoint (for testing or development)

```bash
export SEREN_REMOTE_API="https://dev.api.seren.cloud/replication"
./postgres-seren-replicator init \
  --source "..." \
  --target "..."
```

#### Job timeout (default: 8 hours)

```bash
# Set 12-hour timeout for very large databases
./postgres-seren-replicator init \
  --source "..." \
  --target "..." \
  --job-timeout 43200
```

### Remote Execution Troubleshooting

#### "Failed to submit job to remote service"

- Check your internet connection
- Verify you can reach <https://api.seren.cloud>
- Try with `--local` as a fallback

#### Job stuck in "provisioning" state

- AWS may be experiencing capacity issues in the region
- Wait a few minutes and check status again
- Contact SerenAI support if it persists for > 10 minutes

#### Job failed with error

- Check the error message in the status response
- Verify your source and target database credentials
- Ensure databases are accessible from the internet
- Try running with `--local` to validate locally first

For more details on the AWS infrastructure and architecture, see the [AWS Setup Guide](docs/aws-setup.md).

---

## PostgreSQL-Specific Features

The following features are available for PostgreSQL-to-PostgreSQL replication. For complete documentation, see **[README-PostgreSQL.md](README-PostgreSQL.md)**:

- **Selective Replication**: Filter databases and tables with include/exclude lists
- **Interactive Mode**: Terminal UI for selecting what to replicate
- **Schema-Aware Filtering**: Target specific schema.table combinations
- **Partial Data Replication**: Use WHERE clauses and time filters
- **Configuration Files**: Complex filtering rules in TOML format
- **Multi-Provider Support**: Works with Neon, AWS RDS, Hetzner, self-hosted PostgreSQL
- **Secure Credentials**: `.pgpass` files for safe credential handling
- **Subscription Security**: Guidelines for protecting connection strings in pg_subscription

## Testing

### Unit Tests

Run unit tests:

```bash
cargo test
```

### Integration Tests

Integration tests require real database connections. Set environment variables:

```bash
export TEST_SOURCE_URL="postgresql://user:pass@source-host:5432/db"
export TEST_TARGET_URL="postgresql://user:pass@target-host:5432/db"
```

Run integration tests:

```bash
# Run all integration tests
cargo test --test integration_test -- --ignored

# Run specific integration test
cargo test --test integration_test test_validate_command_integration -- --ignored

# Run full workflow test (read-only by default)
cargo test --test integration_test test_full_migration_workflow -- --ignored
```

**Note**: Some integration tests (init, sync) are commented out by default because they perform destructive operations. Uncomment them in `tests/integration_test.rs` to test the full workflow.

### Test Environment Setup

For local testing, you can use Docker to run PostgreSQL instances:

```bash
# Source database
docker run -d --name pg-source \
  -e POSTGRES_PASSWORD=postgres \
  -p 5432:5432 \
  postgres:17

# Target database
docker run -d --name pg-target \
  -e POSTGRES_PASSWORD=postgres \
  -p 5433:5432 \
  postgres:17

# Set test environment variables
export TEST_SOURCE_URL="postgresql://postgres:postgres@localhost:5432/postgres"
export TEST_TARGET_URL="postgresql://postgres:postgres@localhost:5433/postgres"
```

## Requirements

### Source Database

- PostgreSQL 12 or later (for PostgreSQL sources)
- SQLite 3.x (for SQLite sources)
- MongoDB 4.0+ (for MongoDB sources)
- MySQL 5.7+ or MariaDB 10.2+ (for MySQL/MariaDB sources)
- Appropriate privileges for source database type

### Target Database

- PostgreSQL 12 or later
- Database owner or superuser privileges
- Ability to create tables and schemas
- Network connectivity to source database (for continuous replication)

## Performance Optimizations

The tool uses several optimizations for fast, efficient database replication:

### Parallel Operations

- **Auto-detected parallelism**: Automatically uses up to 8 parallel workers based on CPU cores
- **Parallel dump**: pg_dump with `--jobs` flag for concurrent table exports
- **Parallel restore**: pg_restore with `--jobs` flag for concurrent table imports
- **Directory format**: Uses PostgreSQL directory format to enable parallel operations

### Compression

- **Maximum compression**: Level 9 compression for smaller dump sizes
- **Faster transfers**: Reduced network bandwidth and storage requirements
- **Per-file compression**: Each table compressed independently for parallel efficiency

### Large Objects

- **Blob support**: Includes large objects (BLOBs) with `--blobs` flag
- **Binary data**: Handles images, documents, and other binary data efficiently

These optimizations can significantly reduce replication time, especially for large databases with many tables.

## Architecture

- **src/commands/** - CLI command implementations
- **src/postgres/** - PostgreSQL connection and utilities
- **src/migration/** - Schema introspection, dump/restore, checksums
- **src/replication/** - Logical replication management
- **src/sqlite/** - SQLite reader and JSONB conversion
- **src/mongodb/** - MongoDB reader and BSON to JSONB conversion
- **src/mysql/** - MySQL reader and JSONB conversion
- **tests/** - Integration tests

## Troubleshooting

### "Permission denied" errors

Ensure your user has the required privileges:

```sql
-- On source (PostgreSQL)
ALTER USER myuser WITH REPLICATION;

-- On target (PostgreSQL)
ALTER USER myuser WITH SUPERUSER;
```

### "Publication already exists"

The tool handles existing publications gracefully. If you need to start over:

```sql
-- On target
DROP SUBSCRIPTION IF EXISTS seren_replication_sub;

-- On source
DROP PUBLICATION IF EXISTS seren_replication_pub;
```

### Replication lag

Check status frequently during replication:

```bash
# Monitor until lag < 1 second
watch -n 5 './postgres-seren-replicator status --source "$SOURCE" --target "$TARGET"'
```

### "FK-related table will be truncated but is NOT being copied"

When using filtered snapshots (table-level WHERE clauses or time filters), tables with foreign key relationships are truncated using `TRUNCATE CASCADE` to handle dependencies. This error means a dependent table would lose data because it's not included in your replication scope.

**Problem:** You're replicating a filtered table that has foreign key relationships, but some of the FK-related tables are not being copied. TRUNCATE CASCADE would delete data from those tables.

**Solution:** Include all FK-related tables in your replication scope:

```bash
# If you're filtering orders, also include users table
postgres-seren-replicator init \
  --source "$SOURCE" \
  --target "$TARGET" \
  --config replication.toml  # Include all FK-related tables
```

Example config with FK-related tables:

```toml
[databases.mydb]

[[databases.mydb.table_filters]]
table = "orders"
where = "created_at > NOW() - INTERVAL '90 days'"

# Must also include users since orders references users(id)
[[databases.mydb.table_filters]]
table = "users"
where = "id IN (SELECT user_id FROM orders WHERE created_at > NOW() - INTERVAL '90 days')"
```

**Alternative:** If you don't want to replicate the related tables, remove the foreign key constraint before replication.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

### Development

See [CLAUDE.md](CLAUDE.md) for development guidelines and practices.

### Reporting Issues

Please report bugs and feature requests on the [GitHub Issues](https://github.com/serenorg/postgres-seren-replicator/issues) page.

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.
