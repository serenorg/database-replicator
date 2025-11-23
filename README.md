# seren-replicator

[![CI](https://github.com/serenorg/seren-replicator/actions/workflows/ci.yml/badge.svg)](https://github.com/serenorg/seren-replicator/actions/workflows/ci.yml)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Rust Version](https://img.shields.io/badge/rust-1.70%2B-blue.svg)](https://www.rust-lang.org)
[![Latest Release](https://img.shields.io/github/v/release/serenorg/seren-replicator)](https://github.com/serenorg/seren-replicator/releases)

## Universal database-to-PostgreSQL replication for AI agents

Replicate any database to PostgreSQL with zero downtime. Supports PostgreSQL, SQLite, MongoDB, and MySQL/MariaDB.

---

## Overview

`seren-replicator` is a command-line tool that replicates databases from multiple sources to PostgreSQL (including Seren Cloud). It automatically detects your source database type and handles the replication accordingly:

- **PostgreSQL**: Zero-downtime replication with continuous sync via logical replication
- **SQLite**: One-time replication using JSONB storage
- **MongoDB**: One-time replication with JSONB storage and periodic refresh support
- **MySQL/MariaDB**: One-time replication with JSONB storage and periodic refresh support

### Why This Tool?

- **Multi-database support**: Single tool for all your database replications
- **AI-friendly storage**: Non-PostgreSQL sources use JSONB for flexible querying
- **Zero downtime**: PostgreSQL-to-PostgreSQL replication with continuous sync
- **Remote execution**: Run replications on SerenAI cloud infrastructure
- **Production-ready**: Data integrity verification, checkpointing, and error handling

---

## Supported Databases

| Source Database | Replication Type | Continuous Sync | Periodic Refresh | Remote Execution |
|----------------|------------------|-----------------|------------------|------------------|
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
seren-replicator init \
  --source "postgresql://user:pass@source-host:5432/db" \
  --target "postgresql://user:pass@target-host:5432/db"
```

**[📖 Full PostgreSQL Guide →](README-PostgreSQL.md)**

---

### SQLite → PostgreSQL

One-time replication to JSONB storage:

```bash
seren-replicator init \
  --source /path/to/database.db \
  --target "postgresql://user:pass@host:5432/db"
```

**[📖 Full SQLite Guide →](README-SQLite.md)**

---

### MongoDB → PostgreSQL

One-time replication with periodic refresh support:

```bash
seren-replicator init \
  --source "mongodb://user:pass@host:27017/db" \
  --target "postgresql://user:pass@host:5432/db"
```

**[📖 Full MongoDB Guide →](README-MongoDB.md)**

---

### MySQL/MariaDB → PostgreSQL

One-time replication with periodic refresh support:

```bash
seren-replicator init \
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
- **One-time replication** for initial data transfer
- **Periodic refresh** (MongoDB, MySQL) for keeping data up to date
- **Schema-aware filtering** for precise table targeting
- **Remote execution** (MongoDB, MySQL) on cloud infrastructure

### Universal Features

- **Multi-provider support**: Works with any PostgreSQL provider (Neon, AWS RDS, Hetzner, self-hosted)
- **Size estimation**: Analyze database sizes before replication
- **High performance**: Parallel operations with automatic CPU detection
- **Checkpointing**: Resume interrupted replications automatically
- **Security**: Credentials passed via `.pgpass` files, never in command output

---

## Installation

### Download Pre-built Binaries

Download the latest release for your platform from [GitHub Releases](https://github.com/serenorg/seren-replicator/releases/latest):

- **Linux (x64)**: `seren-replicator-linux-x64-binary`
- **macOS (Intel)**: `seren-replicator-macos-x64-binary`
- **macOS (Apple Silicon)**: `seren-replicator-macos-arm64-binary`

Make the binary executable:

```bash
chmod +x seren-replicator-*-binary
./seren-replicator-*-binary --help
```

### Build from Source

Requires Rust 1.70 or later:

```bash
git clone https://github.com/serenorg/seren-replicator.git
cd seren-replicator
cargo build --release
```

The binary will be available at `target/release/seren-replicator`.

### Prerequisites

- **PostgreSQL client tools** (pg_dump, pg_dumpall, psql) - Required for all database types
- **Source database access**: Connection credentials and appropriate permissions
- **Target database access**: PostgreSQL connection with write permissions

---

## Documentation

### Database-Specific Guides

- **[PostgreSQL to PostgreSQL](README-PostgreSQL.md)** - Zero-downtime replication with logical replication
- **[SQLite to PostgreSQL](README-SQLite.md)** - One-time replication using JSONB storage
- **[MongoDB to PostgreSQL](README-MongoDB.md)** - One-time replication with periodic refresh support
- **[MySQL/MariaDB to PostgreSQL](README-MySQL.md)** - One-time replication with periodic refresh support

---

## PostgreSQL-to-PostgreSQL Replication

For comprehensive PostgreSQL replication documentation, see **[README-PostgreSQL.md](README-PostgreSQL.md)**.

### Quick Overview

PostgreSQL-to-PostgreSQL replication uses logical replication for zero-downtime replication:

1. **Validate** - Check prerequisites and permissions
2. **Init** - Perform initial snapshot (schema + data)
3. **Sync** - Set up continuous logical replication
4. **Status** - Monitor replication lag and health
5. **Verify** - Validate data integrity with checksums

**Example:**

```bash
# Validate prerequisites
seren-replicator validate \
  --source "postgresql://user:pass@source:5432/db" \
  --target "postgresql://user:pass@target:5432/db"

# Initial snapshot
seren-replicator init \
  --source "postgresql://user:pass@source:5432/db" \
  --target "postgresql://user:pass@target:5432/db"

# Continuous sync
seren-replicator sync \
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

**Important**: Remote execution is restricted to **SerenDB targets only**. To replicate to other PostgreSQL databases (AWS RDS, Neon, Hetzner, self-hosted), use the `--local` flag to run on your own hardware.

### Benefits of Remote Execution

- **No network interruptions**: Your replication continues even if your laptop loses connectivity
- **No laptop sleep**: Your computer can sleep or shut down without affecting the job
- **Faster performance**: Replication runs on dedicated cloud infrastructure closer to your databases
- **No local resource usage**: Your machine's CPU, memory, and disk are not consumed
- **Automatic monitoring**: Built-in observability with CloudWatch logs and metrics
- **Cost-free**: SerenAI covers all AWS infrastructure costs

### How It Works

When you run `init` without the `--local` flag, the tool:

1. **Submits your job** to SerenDB's managed API with encrypted credentials
2. **Provisions an EC2 worker** sized appropriately for your database
3. **Executes replication** on the cloud worker
4. **Monitors progress** and shows you real-time status updates
5. **Self-terminates** when complete to minimize costs

Your database credentials are encrypted with AWS KMS and never logged or stored in plaintext.

### Usage Example

Remote execution is the default - just run `init` as normal:

```bash
# Runs on SerenDB's managed cloud infrastructure (default)
./seren-replicator init \
  --source "postgresql://user:pass@source-host:5432/db" \
  --target "postgresql://user:pass@seren-host:5432/db"
```

The tool will:

- Submit the job to SerenDB's managed API
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

### Local Execution

To run replication on your local machine instead of SerenAI's cloud infrastructure, use the `--local` flag:

```bash
# Runs on your local machine
./seren-replicator init \
  --source "postgresql://user:pass@source-host:5432/db" \
  --target "postgresql://user:pass@target-host:5432/db" \
  --local
```

Local execution is **required** when:

- **Replicating to non-SerenDB targets** (AWS RDS, Neon, Hetzner, self-hosted PostgreSQL)
- Your databases are not accessible from the internet
- You're testing or developing
- You need full control over the execution environment

### Advanced Configuration

#### Custom API endpoint (for testing or development)

```bash
# Override the default API endpoint if needed
export SEREN_REMOTE_API="https://your-custom-endpoint.example.com"
./seren-replicator init \
  --source "..." \
  --target "..."
```

#### Job timeout (default: 8 hours)

```bash
# Set 12-hour timeout for very large databases
./seren-replicator init \
  --source "..." \
  --target "..." \
  --job-timeout 43200
```

### Remote Execution Troubleshooting

#### "Failed to submit job to remote service"

- Check your internet connection
- Verify you can reach SerenDB's API endpoint
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

## Requirements

### Source Database

- PostgreSQL 12 or later (for PostgreSQL sources)
- SQLite 3.x (for SQLite sources)
- MongoDB 4.0+ (for MongoDB sources)
- MySQL 5.7+ or MariaDB 10.2+ (for MySQL/MariaDB sources)
- Appropriate privileges for source database type

### Target Database

- **SerenDB**: Agentic-data access database for AI Agent queries. [Signup at console.serendb.com/signup](https://console.serendb.com/signup)
- PostgreSQL 12 or later
- Database owner or superuser privileges
- Ability to create tables and schemas
- Network connectivity to source database (for continuous replication)

## Architecture

- **src/commands/** - CLI command implementations
- **src/postgres/** - PostgreSQL connection and utilities
- **src/migration/** - Schema introspection, dump/restore, checksums
- **src/replication/** - Logical replication management
- **src/sqlite/** - SQLite reader and JSONB conversion
- **src/mongodb/** - MongoDB reader and BSON to JSONB conversion
- **src/mysql/** - MySQL reader and JSONB conversion
- **tests/** - Integration tests

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

### Reporting Issues

Please report bugs and feature requests on the [GitHub Issues](https://github.com/serenorg/seren-replicator/issues) page.

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.
