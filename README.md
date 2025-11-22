# postgres-seren-replicator

[![CI](https://github.com/serenorg/postgres-seren-replicator/actions/workflows/ci.yml/badge.svg)](https://github.com/serenorg/postgres-seren-replicator/actions/workflows/ci.yml)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Rust Version](https://img.shields.io/badge/rust-1.70%2B-blue.svg)](https://www.rust-lang.org)
[![Latest Release](https://img.shields.io/github/v/release/serenorg/postgres-seren-replicator)](https://github.com/serenorg/postgres-seren-replicator/releases)

Zero-downtime PostgreSQL replication tool from PostgreSQL to Seren with continuous sync and real-time monitoring.

## Overview

This tool enables safe, zero-downtime replication of PostgreSQL databases from any PostgreSQL provider (Neon, AWS RDS, Hetzner, self-hosted, etc.) to Seren Cloud. It uses PostgreSQL's logical replication for continuous data synchronization with real-time monitoring and supports selective replication for fine-grained control over what gets replicated.

## Features

- **Zero Downtime**: Uses logical replication to keep databases continuously in sync
- **Selective Replication**: Choose specific databases and tables to replicate
- **Interactive Mode**: User-friendly terminal UI for selecting what to replicate
- **Multi-Provider Support**: Works with any PostgreSQL provider (Neon, AWS RDS, Hetzner, self-hosted, etc.)
- **Size Estimation**: Analyze database sizes and view estimated replication times before starting
- **High Performance**: Parallel dump/restore with automatic CPU core detection
- **Optimized Compression**: Maximum compression (level 9) for faster transfers
- **Large Object Support**: Handles BLOBs and large binary objects efficiently
- **Complete Replication**: Replicates schema, data, roles, and permissions
- **Data Validation**: Checksum-based verification of data integrity
- **Real-time Monitoring**: Track replication lag and status continuously
- **Safe & Fail-fast**: Validates prerequisites before starting replication

## Replication Workflow

The replication process follows 5 phases:

1. **Validate** - Check source and target databases meet replication requirements
2. **Init** - Perform initial snapshot replication (schema + data) using pg_dump/restore
3. **Sync** - Set up continuous logical replication between databases
4. **Status** - Monitor replication lag and health in real-time
5. **Verify** - Validate data integrity with checksums

## Installation

### Prerequisites

- PostgreSQL client tools (pg_dump, pg_dumpall, psql)
- Access to both source and target databases with appropriate permissions

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

### Target Database Setup (Optional)

**SerenDB Account**: If you want to replicate to SerenDB, you can create a free account at <https://console.serendb.com/signup>. This will provide you with a managed PostgreSQL database optimized for your workload.

**Note**: SerenDB is completely optional. This tool works with any PostgreSQL-compatible database as your target, including:

- SerenDB (managed PostgreSQL)
- AWS RDS PostgreSQL
- Self-hosted PostgreSQL
- Neon, Supabase, or any other PostgreSQL provider

You only need a SerenDB account if you specifically want to use SerenDB as your replication target.

## Usage

### 1. Validate Databases

Check that both databases meet replication requirements:

```bash
./postgres-seren-replicator validate \
  --source "postgresql://user:pass@source-host:5432/db" \
  --target "postgresql://user:pass@seren-host:5432/db"
```

### 2. Initialize Replication

Perform initial snapshot replication. The tool will first analyze database sizes and show estimated replication times:

```bash
./postgres-seren-replicator init \
  --source "postgresql://user:pass@source-host:5432/db" \
  --target "postgresql://user:pass@seren-host:5432/db"
```

Example output:
```
Analyzing database sizes...

Database             Size         Est. Time
──────────────────────────────────────────────────
myapp               15.0 GB      ~45.0 minutes
analytics           250.0 GB     ~12.5 hours
staging             2.0 GB       ~6.0 minutes
──────────────────────────────────────────────────
Total: 267.0 GB (estimated ~13.3 hours)

Proceed with replication? [y/N]:
```

For automated scripts, skip the confirmation prompt with `--yes` or `-y`:

```bash
./postgres-seren-replicator init \
  --source "postgresql://user:pass@source-host:5432/db" \
  --target "postgresql://user:pass@seren-host:5432/db" \
  --yes
```

If the target database already exists, you can drop and recreate it with `--drop-existing`:

```bash
./postgres-seren-replicator init \
  --source "postgresql://user:pass@source-host:5432/db" \
  --target "postgresql://user:pass@seren-host:5432/db" \
  --drop-existing
```

The init command automatically checkpoints after each database finishes so you can rerun it if a later step fails. The next run will skip completed databases and continue with the remaining ones. Pass `--no-resume` if you want to discard any previous checkpoint and start from scratch (a new checkpoint is created for the fresh run).

### 3. Set Up Continuous Replication

Enable logical replication for ongoing change synchronization:

```bash
./postgres-seren-replicator sync \
  --source "postgresql://user:pass@source-host:5432/db" \
  --target "postgresql://user:pass@seren-host:5432/db"
```

> **Note:** Table-level predicates (`--table-filter`, `--time-filter`, or config file rules) require PostgreSQL 15+ on the source so publications can use `WHERE` clauses. Schema-only tables work on all supported versions.

### 4. Monitor Replication Status

Check replication health and lag in real-time:

```bash
./postgres-seren-replicator status \
  --source "postgresql://user:pass@source-host:5432/db" \
  --target "postgresql://user:pass@seren-host:5432/db"
```

### 5. Verify Data Integrity

Validate that all tables match:

```bash
./postgres-seren-replicator verify \
  --source "postgresql://user:pass@source-host:5432/db" \
  --target "postgresql://user:pass@seren-host:5432/db"
```

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

```
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

### Troubleshooting

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

## Selective Replication

Selective replication allows you to choose exactly which databases and tables to replicate, giving you fine-grained control over your migration.

### Database-Level Filtering

Replicate only specific databases:

```bash
# Include only specific databases
./postgres-seren-replicator init \
  --source "postgresql://user:pass@source-host:5432/postgres" \
  --target "postgresql://user:pass@seren-host:5432/postgres" \
  --include-databases "myapp,analytics"

# Exclude specific databases
./postgres-seren-replicator init \
  --source "postgresql://user:pass@source-host:5432/postgres" \
  --target "postgresql://user:pass@seren-host:5432/postgres" \
  --exclude-databases "test,staging"
```

### Table-Level Filtering

Replicate only specific tables or exclude certain tables:

```bash
# Include only specific tables (format: database.table)
./postgres-seren-replicator init \
  --source "postgresql://user:pass@source-host:5432/postgres" \
  --target "postgresql://user:pass@seren-host:5432/postgres" \
  --include-tables "myapp.users,myapp.orders,analytics.events"

# Exclude specific tables
./postgres-seren-replicator init \
  --source "postgresql://user:pass@source-host:5432/postgres" \
  --target "postgresql://user:pass@seren-host:5432/postgres" \
  --exclude-tables "myapp.logs,myapp.cache,analytics.temp_data"
```

### Schema-Only Tables (Structure Only)

Skip data for heavy archives while keeping the schema in sync:

```bash
./postgres-seren-replicator init \
  --source "$SRC" \
  --target "$TGT" \
  --schema-only-tables "myapp.audit_logs,analytics.evmlog_strides"
```

Schema-only tables are recreated with full DDL but no rows, which dramatically reduces dump/restore time for historical partitions or archived hypertables.

### Partial Data with WHERE Clauses

Filter tables down to the rows you actually need:

```bash
./postgres-seren-replicator init \
  --source "$SRC" \
  --target "$TGT" \
  --table-filter "output:series_time >= NOW() - INTERVAL '6 months'" \
  --table-filter "transactions:status IN ('active','pending')"
```

Each `--table-filter` takes `[db.]table:SQL predicate`. During `init`, data is streamed with `COPY (SELECT ... WHERE predicate)`; during `sync`, we create PostgreSQL publications that emit only rows matching those predicates (requires PostgreSQL 15+ on the source).

### Time-Based Filters (Shorthand)

For time-series tables (e.g., TimescaleDB hypertables) use the shorthand `table:column:window`:

```bash
./postgres-seren-replicator init \
  --source "$SRC" \
  --target "$TGT" \
  --time-filter "metrics:created_at:6 months" \
  --time-filter "billing_events:event_time:1 year"
```

Supported window units: seconds, minutes, hours, days, weeks, months, and years. The shorthand expands to `column >= NOW() - INTERVAL 'window'`.

### Combined Filtering

Combine database, table, and predicate filtering for precise control:

```bash
./postgres-seren-replicator init \
  --source "postgresql://user:pass@source-host:5432/postgres" \
  --target "postgresql://user:pass@seren-host:5432/postgres" \
  --include-databases "myapp,analytics" \
  --exclude-tables "myapp.logs" \
  --schema-only-tables "analytics.evmlog_strides" \
  --time-filter "analytics.metrics:created_at:6 months"
```

### Configuration File (Complex Rules)

Large migrations often need different rules per database. Describe them in TOML and pass `--config` to both `init` and `sync`:

```bash
./postgres-seren-replicator init \
  --source "$SRC" \
  --target "$TGT" \
  --config replication-config.toml
```

See [docs/replication-config.md](docs/replication-config.md) for the full schema (schema-only lists, table filters, time filters, and TimescaleDB tips). CLI flags merge on top of the file so you can override a single table without editing the config.

### Schema-Aware Filtering

PostgreSQL databases can have multiple schemas (namespaces) with identically-named tables. For example, both `public.orders` and `analytics.orders` can exist in the same database. Schema-aware filtering lets you target specific schema.table combinations to avoid ambiguity.

#### Using Schema Notation

**CLI with dot notation:**

```bash
# Include tables from specific schemas
./postgres-seren-replicator init \
  --source "$SRC" \
  --target "$TGT" \
  --schema-only-tables "analytics.large_table,public.temp"

# Filter tables in non-public schemas
./postgres-seren-replicator init \
  --source "$SRC" \
  --target "$TGT" \
  --table-filter "analytics.events:created_at > NOW() - INTERVAL '90 days'" \
  --table-filter "reporting.metrics:status = 'active'"

# Time filters with schema qualification
./postgres-seren-replicator init \
  --source "$SRC" \
  --target "$TGT" \
  --time-filter "analytics.metrics:timestamp:6 months"
```

**TOML config with explicit schema field:**

```toml
[databases.mydb]

# Schema-only tables (structure but no data)
schema_only = [
  "analytics.evmlog_strides",  # Dot notation
  "reporting.archive"
]

# Table filters with explicit schema field
[[databases.mydb.table_filters]]
table = "events"
schema = "analytics"
where = "created_at > NOW() - INTERVAL '90 days'"

# Alternatively, use dot notation in table name
[[databases.mydb.table_filters]]
table = "reporting.metrics"
where = "status = 'active'"

# Time filters with schema
[[databases.mydb.time_filters]]
table = "metrics"
schema = "analytics"
column = "timestamp"
last = "6 months"
```

#### Backward Compatibility

For convenience, table names without a schema qualifier default to the `public` schema:

```bash
# These are equivalent:
--schema-only-tables "users"
--schema-only-tables "public.users"

# TOML equivalent:
schema_only = ["users"]              # Defaults to public schema
schema_only = ["public.users"]       # Explicit public schema
```

This means existing configurations continue to work without modification.

#### Why Schema Awareness Matters

Without schema qualification, filtering `"orders"` is ambiguous if you have both `public.orders` and `analytics.orders`. Schema-aware filtering ensures:

- **Precise targeting**: Replicate `analytics.orders` while excluding `public.orders`
- **No collisions**: Different schemas can have identically-named tables
- **FK safety**: Cascading truncates handle schema-qualified FK relationships correctly
- **Resume correctness**: Checkpoints detect schema scope changes and invalidate when the replication scope shifts

### Filtering with Other Commands

Filtering works with all commands that support it:

```bash
# Validate with filtering
./postgres-seren-replicator validate \
  --source "postgresql://user:pass@source-host:5432/postgres" \
  --target "postgresql://user:pass@seren-host:5432/postgres" \
  --include-databases "myapp"

# Sync with filtering
./postgres-seren-replicator sync \
  --source "postgresql://user:pass@source-host:5432/postgres" \
  --target "postgresql://user:pass@seren-host:5432/postgres" \
  --include-databases "myapp,analytics" \
  --exclude-tables "myapp.logs"

# Status with filtering
./postgres-seren-replicator status \
  --source "postgresql://user:pass@source-host:5432/postgres" \
  --target "postgresql://user:pass@seren-host:5432/postgres" \
  --include-databases "myapp"

# Verify with filtering
./postgres-seren-replicator verify \
  --source "postgresql://user:pass@source-host:5432/postgres" \
  --target "postgresql://user:pass@seren-host:5432/postgres" \
  --include-databases "myapp" \
  --exclude-tables "myapp.logs"
```

## Interactive Mode

Interactive mode provides a user-friendly terminal UI for selecting databases and tables to replicate. This is ideal for exploratory migrations or when you're not sure exactly what you want to replicate.

**Interactive mode is the default** for `init`, `validate`, and `sync` commands. Simply run the command without any filter flags:

```bash
./postgres-seren-replicator init \
  --source "postgresql://user:pass@source-host:5432/postgres" \
  --target "postgresql://user:pass@seren-host:5432/postgres"
```

### Disabling Interactive Mode

To use CLI filter flags instead of interactive mode, add the `--no-interactive` flag:

```bash
./postgres-seren-replicator init \
  --source "postgresql://user:pass@source-host:5432/postgres" \
  --target "postgresql://user:pass@seren-host:5432/postgres" \
  --no-interactive \
  --include-databases "myapp,analytics"
```

**Note**: The `--yes` flag (for `init` command) automatically disables interactive mode since it's meant for automation.

### Interactive Workflow

1. **Select Databases**: A multi-select checklist shows all available databases. Use arrow keys to navigate, space to select, and enter to confirm.

2. **Select Tables to Exclude** (optional): For each selected database, you can optionally exclude specific tables. If you don't want to exclude any tables, just press enter.

3. **Review Configuration**: The tool shows a summary of what will be replicated, including:
   - Databases to replicate
   - Tables to exclude (if any)

4. **Confirm**: You'll be asked to confirm before proceeding.

### Example Interactive Session

```
Connecting to source database...
✓ Connected to source

Discovering databases on source...
✓ Found 4 database(s)

Select databases to replicate:
(Use arrow keys to navigate, Space to select, Enter to confirm)

> [x] myapp
  [x] analytics
  [ ] staging
  [ ] test

✓ Selected 2 database(s):
  - myapp
  - analytics

Discovering tables in database 'myapp'...
✓ Found 15 table(s) in 'myapp'

Select tables to EXCLUDE from 'myapp' (or press Enter to include all):
(Use arrow keys to navigate, Space to select, Enter to confirm)

  [ ] users
  [ ] orders
  [x] logs
  [x] cache
  [ ] products

✓ Excluding 2 table(s) from 'myapp':
  - myapp.logs
  - myapp.cache

========================================
Replication Configuration Summary
========================================

Databases to replicate: 2
  ✓ myapp
  ✓ analytics

Tables to exclude: 2
  ✗ myapp.logs
  ✗ myapp.cache

========================================

Proceed with this configuration? [Y/n]:
```

## Multi-Provider Support

The tool works seamlessly with any PostgreSQL-compatible database provider. Here are examples for common providers:

### Neon

```bash
./postgres-seren-replicator init \
  --source "postgresql://user:pass@ep-cool-name-123456.us-east-2.aws.neon.tech/mydb" \
  --target "postgresql://user:pass@seren-host:5432/mydb"
```

### AWS RDS

```bash
./postgres-seren-replicator init \
  --source "postgresql://user:pass@mydb.abc123.us-east-1.rds.amazonaws.com:5432/mydb" \
  --target "postgresql://user:pass@seren-host:5432/mydb"
```

### Hetzner Cloud

```bash
./postgres-seren-replicator init \
  --source "postgresql://user:pass@postgres-server.hetzner-cloud.de:5432/mydb" \
  --target "postgresql://user:pass@seren-host:5432/mydb"
```

### Self-Hosted PostgreSQL

```bash
./postgres-seren-replicator init \
  --source "postgresql://user:pass@192.168.1.100:5432/mydb" \
  --target "postgresql://user:pass@seren-host:5432/mydb"
```

### Provider-Specific Considerations

#### Connection Parameters

All providers support standard PostgreSQL connection strings. Add SSL/TLS parameters as needed:

```bash
# With SSL mode
--source "postgresql://user:pass@host:5432/db?sslmode=require"

# With SSL and certificate verification
--source "postgresql://user:pass@host:5432/db?sslmode=verify-full&sslrootcert=/path/to/ca.crt"
```

#### Privileges

Ensure your source database user has the required privileges:

```sql
-- On source (works for all providers)
ALTER USER myuser WITH REPLICATION;
GRANT USAGE ON SCHEMA public TO myuser;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO myuser;
```

#### Provider Limitations

- **AWS RDS**: Requires `rds_replication` role for logical replication
- **Neon**: Full support for logical replication out of the box
- **Hetzner**: Standard PostgreSQL, full support
- **Self-hosted**: Full control, ensure `wal_level = logical` in postgresql.conf

## Security

### Secure Credential Handling

The tool implements secure credential handling to prevent command injection vulnerabilities and credential exposure:

- **`.pgpass` Authentication**: Database credentials are passed to PostgreSQL tools (pg_dump, pg_dumpall, psql, pg_restore) via temporary `.pgpass` files instead of command-line arguments. This prevents credentials from appearing in process listings (`ps` output) or shell history.

- **Automatic Cleanup**: Temporary `.pgpass` files are automatically removed when operations complete, even if the process crashes or is interrupted. This is implemented using Rust's RAII pattern (Drop trait) to ensure cleanup happens reliably.

- **Secure Permissions**: On Unix systems, `.pgpass` files are created with `0600` permissions (owner read/write only) as required by PostgreSQL. This prevents other users on the system from reading credentials.

- **No Command Injection**: By using separate connection parameters (`--host`, `--port`, `--dbname`, `--username`) instead of embedding credentials in connection URLs passed to external commands, the tool eliminates command injection attack vectors.

**Connection String Format**: While you provide connection URLs to the tool (e.g., `postgresql://user:pass@host:5432/db`), these URLs are parsed internally and credentials are extracted securely. They are never passed as-is to external PostgreSQL commands.

### Subscription Connection Strings

**Important Security Consideration**: PostgreSQL logical replication subscriptions store connection strings in the `pg_subscription` system catalog table. This is a PostgreSQL design limitation - subscription connection strings (including passwords if provided) are visible to users who can query system catalogs.

**Security Implications**:

- Connection strings with passwords are stored in `pg_subscription.subconninfo`
- Users with `pg_read_all_settings` role or `SELECT` on `pg_subscription` can view these passwords
- This information persists until the subscription is dropped

**Recommended Mitigation** - Configure `.pgpass` on Target Server:

To avoid storing passwords in the subscription catalog, configure a `.pgpass` file on your target PostgreSQL server:

1. **Create `.pgpass` file** in the PostgreSQL server user's home directory (typically `/var/lib/postgresql/.pgpass`):

   ```text
   source-host:5432:dbname:username:password
   ```

2. **Set secure permissions**:

   ```bash
   chmod 0600 /var/lib/postgresql/.pgpass
   chown postgres:postgres /var/lib/postgresql/.pgpass
   ```

3. **Use password-less connection string** when running `sync`:

   ```bash
   # Omit password from source URL
   postgres-seren-replicator sync \
     --source "postgresql://user@source-host:5432/db" \
     --target "postgresql://user:pass@target-host:5432/db"
   ```

With this configuration, subscriptions will authenticate using the `.pgpass` file on the target server, and no password will be stored in `pg_subscription`.

**Note**: The tool displays a warning when creating subscriptions to remind you of this security consideration.

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

- PostgreSQL 12 or later
- Replication privilege (`REPLICATION` role attribute)
- Ability to create publications

### Target Database (Seren)

- PostgreSQL 12 or later
- Superuser or database owner privileges
- Ability to create subscriptions
- Network connectivity to source database

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
- **tests/** - Integration tests

## Troubleshooting

### "Permission denied" errors

Ensure your user has the required privileges:

```sql
-- On source (Neon)
ALTER USER myuser WITH REPLICATION;

-- On target (Seren)
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

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.
