# database-replicator

[![CI](https://github.com/serenorg/database-replicator/actions/workflows/ci.yml/badge.svg)](https://github.com/serenorg/database-replicator/actions/workflows/ci.yml)
[![Crates.io](https://img.shields.io/crates/v/database-replicator.svg)](https://crates.io/crates/database-replicator)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Rust Version](https://img.shields.io/badge/rust-1.70%2B-blue.svg)](https://www.rust-lang.org)
[![Latest Release](https://img.shields.io/github/v/release/serenorg/database-replicator)](https://github.com/serenorg/database-replicator/releases)
[![SPONSORED BY DAYTONA STARTUP GRID](https://img.shields.io/badge/SPONSORED%20BY-DAYTONA%20STARTUP%20GRID-2ECC71?style=for-the-badge)](https://daytona.io/startups?utm_source=serendb.com)

## Universal database-to-PostgreSQL replication for AI agents

Replicate any database to PostgreSQL with zero downtime. Supports PostgreSQL, SQLite, MongoDB, and MySQL/MariaDB.

---

## SerenAI Cloud Replication

**New to SerenAI?** Sign up at [console.serendb.com](https://console.serendb.com) to get started with managed cloud replication.

SerenAI provides managed PostgreSQL databases optimized for AI workloads. When replicating to SerenDB targets, this tool can run your replication jobs on SerenAI's cloud infrastructure - no local resources required.

**Benefits of SerenAI Cloud Execution:**

- No local compute resources needed
- Automatic retry and error handling
- Job monitoring and logging
- Optimized for large database transfers

### Option 1: Interactive Project Selection (Recommended)

With just your API key set, the tool will interactively guide you through selecting your target project and database:

```bash
export SEREN_API_KEY="your-api-key"  # Get from console.serendb.com

database-replicator init \
  --source "postgresql://user:pass@source:5432/db"
```

The tool will:

1. Show a picker to select your SerenDB project
2. Automatically enable logical replication if needed
3. Create missing databases on the target
4. Save your selection for future `sync` commands

### Option 2: Explicit Connection String

If you already have your connection string, you can provide it directly:

```bash
export SEREN_API_KEY="your-api-key"
database-replicator init \
  --source "postgresql://user:pass@source:5432/db" \
  --target "postgresql://user:pass@your-db.serendb.com:5432/db"
```

For local execution (non-SerenDB targets), use the `--local` flag. See [Remote Execution](#remote-execution-aws) for details.

---

## Overview

`database-replicator` is a command-line tool that replicates databases from multiple sources to PostgreSQL (including Seren Cloud). It automatically detects your source database type and handles the replication accordingly:

- **PostgreSQL**: Zero-downtime replication with continuous sync (automatic fallback to xmin-based sync when logical replication isn't available)
- **AWS RDS for PostgreSQL**: Managed Postgres replication with automatic xmin fallback for databases without `wal_level=logical`
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

| Source Database | Replication Type | Continuous Sync | Remote Execution |
|----------------|------------------|-----------------|------------------|
| **PostgreSQL** | Native replication | ✅ Auto-detects: logical replication or xmin-based sync | ✅ Yes |
| **AWS RDS (PostgreSQL)** | Native replication | ✅ Auto-fallback to xmin when wal_level isn't logical | ✅ Yes |
| **SQLite** | JSONB storage | ❌ One-time | ❌ Local only |
| **MongoDB** | JSONB storage | ✅ Periodic refresh (24hr default) | ✅ Yes |
| **MySQL/MariaDB** | JSONB storage | ✅ Periodic refresh (24hr default) | ✅ Yes |

**PostgreSQL sync methods:**

- **Logical replication** (when `wal_level=logical`): Sub-second latency, real-time delete detection
- **xmin-based sync** (automatic fallback): Works with any PostgreSQL, no source configuration needed

---

## Quick Start

Choose your source database to get started:

### PostgreSQL → PostgreSQL

Zero-downtime replication with continuous sync:

```bash
database-replicator init \
  --source "postgresql://user:pass@source-host:5432/db" \
  --target "postgresql://user:pass@target-host:5432/db"
```

**[📖 Full PostgreSQL Guide →](README-PostgreSQL.md)**

---

### AWS RDS (PostgreSQL) → PostgreSQL/SerenDB

Managed PostgreSQL instances on AWS RDS require the `rds_replication` role (or `rds_superuser`) plus a parameter group with `rds.logical_replication=1`. Once those prerequisites are met, run:

```bash
database-replicator init \
  --source "postgresql://replicator@your-rds-instance.abc123.us-east-1.rds.amazonaws.com:5432/db" \
  --target "postgresql://user:pass@target-host:5432/db"
```

The CLI will surface missing RDS privileges and suggest the AWS commands needed to grant `rds_replication` so reruns stay idempotent.

**[📖 Full PostgreSQL Guide →](README-PostgreSQL.md#aws-rds)**

---

### SQLite → PostgreSQL

One-time replication to JSONB storage:

```bash
database-replicator init \
  --source /path/to/database.db \
  --target "postgresql://user:pass@host:5432/db"
```

**[📖 Full SQLite Guide →](sqlite-watcher-docs/README-SQLite.md)**

---

### MongoDB → PostgreSQL

One-time replication with periodic refresh support:

```bash
database-replicator init \
  --source "mongodb://user:pass@host:27017/db" \
  --target "postgresql://user:pass@host:5432/db"
```

**[📖 Full MongoDB Guide →](README-MongoDB.md)**

---

### MySQL/MariaDB → PostgreSQL

One-time replication with periodic refresh support:

```bash
database-replicator init \
  --source "mysql://user:pass@host:3306/db" \
  --target "postgresql://user:pass@host:5432/db"
```

**[📖 Full MySQL Guide →](README-MySQL.md)**

---

## Features

### PostgreSQL-to-PostgreSQL

- **Zero-downtime replication** using PostgreSQL logical replication
- **Automatic sync method detection** - uses logical replication when available, falls back to xmin-based sync otherwise
- **Works without source configuration** - xmin-based sync requires no `wal_level` changes
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

Choose whichever approach best fits your environment.

### Option 1: Download a Pre-built Binary

1. Visit the [latest GitHub Release](https://github.com/serenorg/database-replicator/releases/latest).
2. Download the asset that matches your operating system and CPU:

| OS | Architectures |
| --- | --- |
| Linux | x86_64, arm64 |
| macOS | Apple Silicon (arm64), Intel (x86_64) |
| Windows | x86_64 |

3. Extract the archive if needed and optionally rename the binary to `database-replicator`.
4. On Linux/macOS, make it executable and move it somewhere on your `PATH`:

```bash
chmod +x database-replicator*
sudo mv database-replicator* /usr/local/bin/database-replicator
database-replicator --help
```

On Windows, run the `.exe` directly or place it in a directory referenced by the `PATH` environment variable.

### Option 2: Build from Source

Requires Rust 1.70 or later.

**Install via crates.io:**

```bash
cargo install database-replicator
database-replicator --help
```

**Build from this repository:**

```bash
git clone https://github.com/serenorg/database-replicator.git
cd database-replicator
cargo build --release
./target/release/database-replicator --help
```

This approach is useful if you want to pin to a specific commit, apply local patches, or build for custom targets.

### Docker Image (Optional)

You can pull the published Docker image from Docker Hub (`palomachain/database-replicator`) or build your own from the release assets.

**Pull a prebuilt image:**

```bash
docker pull palomachain/database-replicator:v6.0.5
docker tag palomachain/database-replicator:v6.0.5 palomachain/database-replicator:latest
```

**Build from GitHub release assets:**

```bash
# latest release asset
docker build -t palomachain/database-replicator:latest .

# specific version
docker build --build-arg VERSION=v6.0.5 -t palomachain/database-replicator:v6.0.5 .
```

Run the CLI inside the container (pass connection strings via arguments or environment variables):

```bash
docker run --rm -it palomachain/database-replicator:latest \
  validate --source "postgresql://user:pass@source/db" \
           --target "postgresql://user:pass@target/db"
```

Mount local config files if needed:

```bash
docker run --rm -it \
  -v "$PWD:/work" \
  palomachain/database-replicator:latest \
  init --source "$(cat /work/source.txt)" --target "$(cat /work/target.txt)"
```

### Prerequisites

- **PostgreSQL client tools** (pg_dump, pg_dumpall, psql) - Required for all database types
- **Source database access**: Connection credentials and appropriate permissions
- **Target database access**: PostgreSQL connection with write permissions

---

## Documentation

### Database-Specific Guides

- **[PostgreSQL to PostgreSQL](README-PostgreSQL.md)** - Zero-downtime replication with logical replication
- **[SQLite to PostgreSQL](sqlite-watcher-docs/README-SQLite.md)** - One-time replication using JSONB storage
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
database-replicator validate \
  --source "postgresql://user:pass@source:5432/db" \
  --target "postgresql://user:pass@target:5432/db"

# Initial snapshot
database-replicator init \
  --source "postgresql://user:pass@source:5432/db" \
  --target "postgresql://user:pass@target:5432/db"

# Continuous sync
database-replicator sync \
  --source "postgresql://user:pass@source:5432/db" \
  --target "postgresql://user:pass@target:5432/db"
```

**See [README-PostgreSQL.md](README-PostgreSQL.md) for:**

- Prerequisites and permission setup
- Detailed command documentation
- Selective replication (filtering databases/tables)
- Interactive mode
- **Sync timing controls** (`--sync-interval`, `--reconcile-interval`, `--once`)
- **Daemon mode** (`--daemon`, `--stop`, `--daemon-status`)
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

### Authentication

Remote execution requires a SerenDB API key for authentication. The tool obtains the API key in one of two ways:

#### Option 1: Environment Variable (Recommended for scripts)

```bash
export SEREN_API_KEY="your-api-key-here"
./database-replicator init --source "..." --target "..."
```

#### Option 2: Interactive Prompt

If `SEREN_API_KEY` is not set, the tool will prompt you to enter your API key:

```text
Remote execution requires a SerenDB API key for authentication.

You can generate an API key at:
  https://console.serendb.com/api-keys

Enter your SerenDB API key: [input]
```

**Getting Your API Key:**

1. Sign up for SerenDB at [console.serendb.com/signup](https://console.serendb.com/signup)
2. Navigate to [console.serendb.com/api-keys](https://console.serendb.com/api-keys)
3. Generate a new API key
4. Copy and save it securely (you won't be able to see it again)

**Security Note:** Never commit API keys to version control. Use environment variables or secure credential management.

### Usage Example

Remote execution is the default - just run `init` as normal:

```bash
# Runs on SerenDB's managed cloud infrastructure (default)
./database-replicator init \
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
./database-replicator init \
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
./database-replicator init \
  --source "..." \
  --target "..."
```

#### Job timeout (default: 8 hours)

```bash
# Set 12-hour timeout for very large databases
./database-replicator init \
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
  - **API Key Required**: Generate an API key at [console.serendb.com/api-keys](https://console.serendb.com/api-keys) for remote execution
- PostgreSQL 12 or later
- Database owner or superuser privileges
- Ability to create tables and schemas
- Network connectivity to source database (for continuous replication)

## Architecture

- **src/commands/** - CLI command implementations
- **src/postgres/** - PostgreSQL connection and utilities
- **src/migration/** - Schema introspection, dump/restore, checksums
- **src/replication/** - Logical replication management
- **src/xmin/** - xmin-based incremental sync (automatic fallback when logical replication unavailable)
- **src/sqlite/** - SQLite reader and JSONB conversion
- **src/mongodb/** - MongoDB reader and BSON to JSONB conversion
- **src/mysql/** - MySQL reader and JSONB conversion
- **tests/** - Integration tests

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

### Reporting Issues

Please report bugs and feature requests on the [GitHub Issues](https://github.com/serenorg/database-replicator/issues) page.

## About SerenAI

SerenAI is building infrastructure for AI agent data access. Agents are hungry for data and they will pay to access the data in your database. We're creating the layer that powers secure, compliant enterprise data commerce and data delivery for AI agents. SerenAI includes agent identity verification, persistent memory via SerenDB, data access control, tiered data-access pricing, SOC2-ready compliance systems, as well as micropayments and settlement.

Our team brings decades of experience building enterprise databases and security systems. We believe AI agents need to pay to access your data.

**Get in touch:** [hello@serendb.com](mailto:hello@serendb.com) | [serendb.com](https://serendb.com)

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.
