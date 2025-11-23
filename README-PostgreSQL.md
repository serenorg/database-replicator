# PostgreSQL-to-PostgreSQL Replication Guide

Zero-downtime database replication using PostgreSQL logical replication with continuous sync.

---

## Overview

This guide covers replicating PostgreSQL databases from any PostgreSQL provider (Neon, AWS RDS, Hetzner, self-hosted, etc.) to another PostgreSQL database (including Seren Cloud). The tool uses PostgreSQL's native logical replication for zero-downtime migration with continuous sync.

### Why This Tool?

- **Zero downtime**: Your source database stays online during migration
- **Continuous sync**: Changes replicate in real-time after initial snapshot
- **Multi-provider**: Works with any PostgreSQL-compatible provider
- **Selective replication**: Choose specific databases and tables
- **Interactive mode**: User-friendly terminal UI for selecting what to replicate
- **Remote execution**: Run migrations on SerenAI cloud infrastructure
- **Production-ready**: Data integrity verification, checkpointing, error handling

### How It Works

The tool uses PostgreSQL's logical replication (publications and subscriptions) to keep databases synchronized:

1. **Initial snapshot**: Copies schema and data using pg_dump/restore
2. **Continuous replication**: Creates publication on source and subscription on target
3. **Real-time sync**: PostgreSQL streams changes from source to target automatically

---

## Prerequisites

### Source Database

- PostgreSQL 12 or later
- `REPLICATION` privilege (can create publications)
- Read access to all tables you want to replicate
- `wal_level = logical` configured (check with `SHOW wal_level;`)

Grant required privileges:

```sql
-- On source database
ALTER USER myuser WITH REPLICATION;
GRANT USAGE ON SCHEMA public TO myuser;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO myuser;
```

### Target Database

- PostgreSQL 12 or later
- Superuser or database owner privileges
- Ability to create subscriptions
- Network connectivity to source database (for continuous sync)

Grant required privileges:

```sql
-- On target database
ALTER USER myuser WITH SUPERUSER;
-- Or for non-superuser setup:
ALTER USER myuser WITH CREATEDB;
GRANT ALL PRIVILEGES ON DATABASE targetdb TO myuser;
```

### Network Requirements

- Target must be able to connect to source database
- For AWS RDS source: Enable `rds_replication` role
- For cloud databases: Ensure firewall rules allow connections
- For remote execution: Both databases must be accessible from the internet

---

## Replication Workflow

The PostgreSQL replication process follows 5 phases:

1. **Validate** - Check source and target databases meet replication requirements
2. **Init** - Perform initial snapshot replication (schema + data) using pg_dump/restore
3. **Sync** - Set up continuous logical replication between databases
4. **Status** - Monitor replication lag and health in real-time
5. **Verify** - Validate data integrity with checksums

---

## Commands

### 1. Validate

Check that both databases meet replication requirements:

```bash
postgres-seren-replicator validate \
  --source "postgresql://user:pass@source-host:5432/db" \
  --target "postgresql://user:pass@target-host:5432/db"
```

The validate command checks:

- PostgreSQL version (12+)
- Required privileges (REPLICATION, superuser)
- `wal_level = logical` on source
- Network connectivity between databases
- Target database exists or can be created

**With filtering:**

```bash
postgres-seren-replicator validate \
  --source "postgresql://user:pass@source-host:5432/postgres" \
  --target "postgresql://user:pass@target-host:5432/postgres" \
  --include-databases "myapp,analytics"
```

---

### 2. Initialize (Init)

Perform initial snapshot replication with schema and data:

```bash
postgres-seren-replicator init \
  --source "postgresql://user:pass@source-host:5432/db" \
  --target "postgresql://user:pass@target-host:5432/db"
```

**What happens during init:**

1. **Size estimation**: Analyzes database sizes and shows estimated replication times
2. **User confirmation**: Prompts to proceed (skip with `--yes`)
3. **Globals dump**: Replicates roles and permissions with `pg_dumpall --globals-only`
4. **Schema dump**: Replicates table structures with `pg_dump --schema-only`
5. **Data dump**: Replicates data with `pg_dump --data-only` (parallel, compressed)
6. **Restore**: Restores globals, schema, and data to target (parallel operations)

**Example output:**

```text
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

**Common options:**

```bash
# Skip confirmation prompt (for scripts)
postgres-seren-replicator init \
  --source "..." \
  --target "..." \
  --yes

# Drop existing target database and recreate
postgres-seren-replicator init \
  --source "..." \
  --target "..." \
  --drop-existing

# Run locally instead of on cloud infrastructure
postgres-seren-replicator init \
  --source "..." \
  --target "..." \
  --local

# Disable checkpoint resume (start fresh)
postgres-seren-replicator init \
  --source "..." \
  --target "..." \
  --no-resume
```

**Checkpointing:**

The init command automatically checkpoints after each database finishes. If replication is interrupted, you can rerun the same command and it will skip completed databases and continue with remaining ones.

To discard the checkpoint and start fresh, use `--no-resume` (a new checkpoint will be created for the fresh run).

---

### 3. Sync

Set up continuous logical replication for ongoing change synchronization:

```bash
postgres-seren-replicator sync \
  --source "postgresql://user:pass@source-host:5432/db" \
  --target "postgresql://user:pass@target-host:5432/db"
```

**What happens during sync:**

1. **Create publication**: Creates publication on source database with all tables
2. **Create subscription**: Creates subscription on target that connects to source
3. **Initial sync**: PostgreSQL performs initial table synchronization
4. **Continuous replication**: Changes stream automatically from source to target

**With filtering:**

```bash
# Sync only specific databases
postgres-seren-replicator sync \
  --source "postgresql://user:pass@source-host:5432/postgres" \
  --target "postgresql://user:pass@target-host:5432/postgres" \
  --include-databases "myapp,analytics"

# Sync with table exclusions
postgres-seren-replicator sync \
  --source "..." \
  --target "..." \
  --exclude-tables "myapp.logs,myapp.cache"
```

> **Note:** Table-level predicates (`--table-filter`, `--time-filter`, or config file rules) require PostgreSQL 15+ on the source so publications can use `WHERE` clauses. Schema-only tables work on all supported versions.

**Important Security Note:**

PostgreSQL subscriptions store connection strings (including passwords) in the `pg_subscription` system catalog. To avoid storing passwords in the catalog, configure a `.pgpass` file on your target PostgreSQL server:

1. Create `/var/lib/postgresql/.pgpass` with `source-host:5432:dbname:username:password`
2. Set permissions: `chmod 0600 /var/lib/postgresql/.pgpass`
3. Omit password from source URL when running `sync`

See [Security](#security) section for details.

---

### 4. Status

Monitor replication health and lag in real-time:

```bash
postgres-seren-replicator status \
  --source "postgresql://user:pass@source-host:5432/db" \
  --target "postgresql://user:pass@target-host:5432/db"
```

**Output includes:**

- Subscription state (streaming, syncing, stopped, etc.)
- Replication lag in bytes and time
- Last received LSN (Log Sequence Number)
- Statistics from both source and target

**With filtering:**

```bash
postgres-seren-replicator status \
  --source "..." \
  --target "..." \
  --include-databases "myapp"
```

**Monitor continuously:**

```bash
# Check status every 5 seconds
watch -n 5 'postgres-seren-replicator status --source "$SOURCE" --target "$TARGET"'
```

---

### 5. Verify

Validate data integrity by comparing checksums between source and target:

```bash
postgres-seren-replicator verify \
  --source "postgresql://user:pass@source-host:5432/db" \
  --target "postgresql://user:pass@target-host:5432/db"
```

**What happens during verify:**

1. **Compute checksums**: Calculates checksums for all tables on both sides
2. **Compare**: Compares checksums to detect any discrepancies
3. **Report**: Shows detailed results per table

**With filtering:**

```bash
postgres-seren-replicator verify \
  --source "..." \
  --target "..." \
  --include-databases "myapp" \
  --exclude-tables "myapp.logs"
```

---

## Selective Replication

Selective replication allows you to choose exactly which databases and tables to replicate, giving you fine-grained control over your migration.

### Database-Level Filtering

Replicate only specific databases:

```bash
# Include only specific databases
postgres-seren-replicator init \
  --source "postgresql://user:pass@source-host:5432/postgres" \
  --target "postgresql://user:pass@target-host:5432/postgres" \
  --include-databases "myapp,analytics"

# Exclude specific databases
postgres-seren-replicator init \
  --source "postgresql://user:pass@source-host:5432/postgres" \
  --target "postgresql://user:pass@target-host:5432/postgres" \
  --exclude-databases "test,staging"
```

**Note:** Database filters are mutually exclusive - you cannot use both `--include-databases` and `--exclude-databases` at the same time.

### Table-Level Filtering

Replicate only specific tables or exclude certain tables:

```bash
# Include only specific tables (format: database.table)
postgres-seren-replicator init \
  --source "postgresql://user:pass@source-host:5432/postgres" \
  --target "postgresql://user:pass@target-host:5432/postgres" \
  --include-tables "myapp.users,myapp.orders,analytics.events"

# Exclude specific tables
postgres-seren-replicator init \
  --source "postgresql://user:pass@source-host:5432/postgres" \
  --target "postgresql://user:pass@target-host:5432/postgres" \
  --exclude-tables "myapp.logs,myapp.cache,analytics.temp_data"
```

**Note:** Table filters are mutually exclusive - you cannot use both `--include-tables` and `--exclude-tables` at the same time.

### Schema-Only Tables (Structure Only)

Skip data for heavy archives while keeping the schema in sync:

```bash
postgres-seren-replicator init \
  --source "$SRC" \
  --target "$TGT" \
  --schema-only-tables "myapp.audit_logs,analytics.evmlog_strides"
```

Schema-only tables are recreated with full DDL but no rows, which dramatically reduces dump/restore time for historical partitions or archived hypertables.

### Partial Data with WHERE Clauses

Filter tables down to the rows you actually need:

```bash
postgres-seren-replicator init \
  --source "$SRC" \
  --target "$TGT" \
  --table-filter "output:series_time >= NOW() - INTERVAL '6 months'" \
  --table-filter "transactions:status IN ('active','pending')"
```

Each `--table-filter` takes `[db.]table:SQL predicate`. During `init`, data is streamed with `COPY (SELECT ... WHERE predicate)`; during `sync`, we create PostgreSQL publications that emit only rows matching those predicates (requires PostgreSQL 15+ on the source).

### Time-Based Filters (Shorthand)

For time-series tables (e.g., TimescaleDB hypertables) use the shorthand `table:column:window`:

```bash
postgres-seren-replicator init \
  --source "$SRC" \
  --target "$TGT" \
  --time-filter "metrics:created_at:6 months" \
  --time-filter "billing_events:event_time:1 year"
```

Supported window units: seconds, minutes, hours, days, weeks, months, and years. The shorthand expands to `column >= NOW() - INTERVAL 'window'`.

### Combined Filtering

Combine database, table, and predicate filtering for precise control:

```bash
postgres-seren-replicator init \
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
postgres-seren-replicator init \
  --source "$SRC" \
  --target "$TGT" \
  --config replication-config.toml
```

**Example config file:**

```toml
[databases.mydb]

# Schema-only tables (structure but no data)
schema_only = [
  "analytics.evmlog_strides",
  "reporting.archive"
]

# Table filters with WHERE clauses
[[databases.mydb.table_filters]]
table = "events"
schema = "analytics"
where = "created_at > NOW() - INTERVAL '90 days'"

[[databases.mydb.table_filters]]
table = "transactions"
where = "status IN ('active', 'pending')"

# Time filters (shorthand)
[[databases.mydb.time_filters]]
table = "metrics"
schema = "analytics"
column = "timestamp"
last = "6 months"
```

See [docs/replication-config.md](docs/replication-config.md) for the full schema. CLI flags merge on top of the file so you can override a single table without editing the config.

### Schema-Aware Filtering

PostgreSQL databases can have multiple schemas (namespaces) with identically-named tables. For example, both `public.orders` and `analytics.orders` can exist in the same database. Schema-aware filtering lets you target specific schema.table combinations to avoid ambiguity.

#### Using Schema Notation

**CLI with dot notation:**

```bash
# Include tables from specific schemas
postgres-seren-replicator init \
  --source "$SRC" \
  --target "$TGT" \
  --schema-only-tables "analytics.large_table,public.temp"

# Filter tables in non-public schemas
postgres-seren-replicator init \
  --source "$SRC" \
  --target "$TGT" \
  --table-filter "analytics.events:created_at > NOW() - INTERVAL '90 days'" \
  --table-filter "reporting.metrics:status = 'active'"

# Time filters with schema qualification
postgres-seren-replicator init \
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

---

## Interactive Mode

Interactive mode provides a user-friendly terminal UI for selecting databases and tables to replicate. This is ideal for exploratory migrations or when you're not sure exactly what you want to replicate.

**Interactive mode is the default** for `init`, `validate`, and `sync` commands. Simply run the command without any filter flags:

```bash
postgres-seren-replicator init \
  --source "postgresql://user:pass@source-host:5432/postgres" \
  --target "postgresql://user:pass@target-host:5432/postgres"
```

### Workflow

1. **Select Databases**: A multi-select checklist shows all available databases. Use arrow keys to navigate, space to select, and enter to confirm.

2. **Select Tables to Exclude** (optional): For each selected database, you can optionally exclude specific tables. If you don't want to exclude any tables, just press enter.

3. **Review Configuration**: The tool shows a summary of what will be replicated, including:
   - Databases to replicate
   - Tables to exclude (if any)

4. **Confirm**: You'll be asked to confirm before proceeding.

### Example Session

```text
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

### Disabling Interactive Mode

To use CLI filter flags instead of interactive mode, add the `--no-interactive` flag:

```bash
postgres-seren-replicator init \
  --source "postgresql://user:pass@source-host:5432/postgres" \
  --target "postgresql://user:pass@seren-host:5432/postgres" \
  --no-interactive \
  --include-databases "myapp,analytics"
```

**Note**: The `--yes` flag (for `init` command) automatically disables interactive mode since it's meant for automation.

---

## Remote Execution

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
postgres-seren-replicator init \
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
postgres-seren-replicator init \
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
postgres-seren-replicator init \
  --source "..." \
  --target "..."
```

#### Job timeout (default: 8 hours)

```bash
# Set 12-hour timeout for very large databases
postgres-seren-replicator init \
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

## Multi-Provider Support

The tool works seamlessly with any PostgreSQL-compatible database provider. Here are examples for common providers:

### Neon

```bash
postgres-seren-replicator init \
  --source "postgresql://user:pass@ep-cool-name-123456.us-east-2.aws.neon.tech/mydb" \
  --target "postgresql://user:pass@seren-host:5432/mydb"
```

### AWS RDS

```bash
postgres-seren-replicator init \
  --source "postgresql://user:pass@mydb.abc123.us-east-1.rds.amazonaws.com:5432/mydb" \
  --target "postgresql://user:pass@seren-host:5432/mydb"
```

**Note**: AWS RDS requires the `rds_replication` role for logical replication:

```sql
GRANT rds_replication TO myuser;
```

### Hetzner Cloud

```bash
postgres-seren-replicator init \
  --source "postgresql://user:pass@postgres-server.hetzner-cloud.de:5432/mydb" \
  --target "postgresql://user:pass@seren-host:5432/mydb"
```

### Self-Hosted PostgreSQL

```bash
postgres-seren-replicator init \
  --source "postgresql://user:pass@192.168.1.100:5432/mydb" \
  --target "postgresql://user:pass@seren-host:5432/mydb"
```

**Note**: Ensure `wal_level = logical` in postgresql.conf and restart PostgreSQL.

### Connection Parameters

All providers support standard PostgreSQL connection strings. Add SSL/TLS parameters as needed:

```bash
# With SSL mode
--source "postgresql://user:pass@host:5432/db?sslmode=require"

# With SSL and certificate verification
--source "postgresql://user:pass@host:5432/db?sslmode=verify-full&sslrootcert=/path/to/ca.crt"
```

---

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

---

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

### TCP Keepalive

To prevent connection timeouts when connecting through load balancers (like AWS ELB), the tool automatically configures TCP keepalive:

- **Environment variables**: Automatically sets `PGKEEPALIVES=1`, `PGKEEPALIVESIDLE=60`, `PGKEEPALIVESINTERVAL=10` for all PostgreSQL client tools
- **Connection strings**: Adds keepalive parameters to connection URLs for direct connections

No manual configuration needed - the tool handles this automatically.

These optimizations can significantly reduce replication time, especially for large databases with many tables.

---

## Troubleshooting

### "Permission denied" errors

Ensure your user has the required privileges:

```sql
-- On source
ALTER USER myuser WITH REPLICATION;
GRANT USAGE ON SCHEMA public TO myuser;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO myuser;

-- On target
ALTER USER myuser WITH SUPERUSER;
```

**Provider-specific:**

- **AWS RDS**: `GRANT rds_replication TO myuser;`
- **Neon**: Full support for logical replication out of the box
- **Self-hosted**: Ensure `wal_level = logical` in postgresql.conf

---

### "Publication already exists"

The tool handles existing publications gracefully. If you need to start over:

```sql
-- On target
DROP SUBSCRIPTION IF EXISTS seren_replication_sub;

-- On source
DROP PUBLICATION IF EXISTS seren_replication_pub;
```

---

### Replication lag

Check status frequently during replication:

```bash
# Monitor until lag < 1 second
watch -n 5 'postgres-seren-replicator status --source "$SOURCE" --target "$TARGET"'
```

If lag is high:

- Check network bandwidth between source and target
- Verify target database has sufficient resources (CPU, memory, disk I/O)
- Consider scaling target database instance
- Check for long-running queries on target blocking replication

---

### Connection timeouts during long operations

**Symptom:** Operations fail after 20-30 minutes with "connection closed" errors during `init` filtered copy.

**Root Cause:** When the target database is behind an AWS Elastic Load Balancer (ELB), the load balancer enforces idle connection timeouts (typically 60 seconds to 10 minutes). During long-running COPY operations, if data isn't flowing continuously, the ELB sees the connection as idle and closes it.

**Solution:** Increase the ELB idle timeout:

```bash
# Using AWS CLI
aws elbv2 modify-load-balancer-attributes \
  --region us-east-1 \
  --load-balancer-arn <ARN> \
  --attributes Key=idle_timeout.timeout_seconds,Value=1800

# Or via Kubernetes service annotation
kubectl annotate service <postgres-service> \
  service.beta.kubernetes.io/aws-load-balancer-connection-idle-timeout=1800
```

**Diagnosis Steps:**

1. Check if target is behind a load balancer (hostname contains `elb.amazonaws.com`)
2. Test basic connectivity: `timeout 10 psql <target-url> -c "SELECT version();"`
3. Check PostgreSQL timeout settings (should be `statement_timeout = 0`)
4. Check how much data is being copied to estimate operation duration
5. If target is responsive but operations timeout after predictable intervals, it's likely an ELB/proxy timeout

**Alternative:** The tool automatically configures TCP keepalive to mitigate this issue, but extremely idle connections may still timeout.

---

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

---

### Database hangs or degradation

**Symptom:** Connections succeed but queries hang indefinitely. Even simple queries like `SELECT version()` don't respond.

**Diagnosis:**

```bash
# Test with timeout
timeout 10 psql <target-url> -c "SELECT version();"

# If that hangs, check pod/container status
kubectl get pods -l app=postgres
kubectl logs <postgres-pod> --tail=100

# Check for locked queries (if you can connect)
psql <url> -c "SELECT pid, state, query FROM pg_stat_activity WHERE state != 'idle';"
```

**Solution:** Restart the PostgreSQL instance or container. Check resource usage (CPU, memory, disk).

---

## Examples

### Full Database Replication

Replicate entire database with all tables:

```bash
postgres-seren-replicator init \
  --source "postgresql://user:pass@source-host:5432/mydb" \
  --target "postgresql://user:pass@target-host:5432/mydb" \
  --yes

postgres-seren-replicator sync \
  --source "postgresql://user:pass@source-host:5432/mydb" \
  --target "postgresql://user:pass@target-host:5432/mydb"
```

---

### Selective Database Replication

Replicate only specific databases:

```bash
postgres-seren-replicator init \
  --source "postgresql://user:pass@source-host:5432/postgres" \
  --target "postgresql://user:pass@target-host:5432/postgres" \
  --include-databases "production,analytics" \
  --yes

postgres-seren-replicator sync \
  --source "postgresql://user:pass@source-host:5432/postgres" \
  --target "postgresql://user:pass@target-host:5432/postgres" \
  --include-databases "production,analytics"
```

---

### Filtered Replication with Predicates

Replicate only recent data:

```bash
postgres-seren-replicator init \
  --source "postgresql://user:pass@source-host:5432/mydb" \
  --target "postgresql://user:pass@target-host:5432/mydb" \
  --time-filter "events:created_at:6 months" \
  --time-filter "metrics:timestamp:1 year" \
  --schema-only-tables "audit_logs,archive_table" \
  --yes

postgres-seren-replicator sync \
  --source "postgresql://user:pass@source-host:5432/mydb" \
  --target "postgresql://user:pass@target-host:5432/mydb" \
  --time-filter "events:created_at:6 months" \
  --time-filter "metrics:timestamp:1 year"
```

---

### Complex Filtering with Config File

Create `replication.toml`:

```toml
[databases.production]

# Schema-only tables (no data)
schema_only = [
  "audit_logs",
  "archive_data"
]

# Filter events to last 90 days
[[databases.production.table_filters]]
table = "events"
where = "created_at > NOW() - INTERVAL '90 days'"

# Filter metrics to last 6 months
[[databases.production.time_filters]]
table = "metrics"
column = "timestamp"
last = "6 months"
```

Run replication:

```bash
postgres-seren-replicator init \
  --source "postgresql://user:pass@source-host:5432/postgres" \
  --target "postgresql://user:pass@target-host:5432/postgres" \
  --config replication.toml \
  --yes

postgres-seren-replicator sync \
  --source "postgresql://user:pass@source-host:5432/postgres" \
  --target "postgresql://user:pass@target-host:5432/postgres" \
  --config replication.toml
```

---

### Local Execution (No Cloud)

Run replication locally instead of on cloud infrastructure:

```bash
postgres-seren-replicator init \
  --source "postgresql://user:pass@source-host:5432/mydb" \
  --target "postgresql://user:pass@target-host:5432/mydb" \
  --local \
  --yes
```

---

## FAQ

### Can I replicate from PostgreSQL 13 to PostgreSQL 12?

No, logical replication requires the target to be the same or newer version than the source. You can replicate from PostgreSQL 12 → 13, but not 13 → 12.

---

### Does logical replication affect source database performance?

Yes, but minimally. Logical replication adds some overhead:

- WAL generation (already happens for crash recovery)
- Replication slot maintains WAL files until consumed
- Network bandwidth for streaming changes

For most workloads, the impact is negligible. Monitor disk usage on source to ensure WAL files don't accumulate.

---

### Can I replicate across different schemas?

Yes, the tool supports schema-aware filtering. You can replicate `schema1.table` from source to `schema2.table` on target by using schema-qualified table names.

---

### What happens if replication falls behind?

PostgreSQL will continue to stream changes as fast as possible. If lag grows too large:

- Check target database resources (CPU, memory, disk I/O)
- Verify network bandwidth between source and target
- Consider scaling target database instance

Use the `status` command to monitor lag in real-time.

---

### Can I pause and resume replication?

Yes, you can temporarily disable the subscription on the target:

```sql
ALTER SUBSCRIPTION seren_replication_sub DISABLE;
```

To resume:

```sql
ALTER SUBSCRIPTION seren_replication_sub ENABLE;
```

---

### How do I stop replication permanently?

Drop the subscription on the target, then the publication on the source:

```sql
-- On target
DROP SUBSCRIPTION IF EXISTS seren_replication_sub;

-- On source
DROP PUBLICATION IF EXISTS seren_replication_pub;
```

---

### Does this work with TimescaleDB?

Yes, the tool works with TimescaleDB. Use time-based filters for hypertables:

```bash
--time-filter "hypertable_name:time_column:6 months"
```

This replicates only recent data from hypertables, reducing migration time significantly.

---

### Can I replicate to multiple targets?

Yes, create multiple subscriptions on different target databases, all pointing to the same publication on the source.

---

### What's the difference between init and sync?

- **init**: One-time snapshot replication (schema + data)
- **sync**: Continuous replication (streams changes in real-time)

Run `init` first to copy existing data, then `sync` to keep databases synchronized.

---

### Do I need to run init before sync?

Yes, `sync` only streams changes - it doesn't copy existing data. Run `init` first to perform the initial snapshot, then `sync` to set up continuous replication.

---

## Additional Documentation

- **[Main README](README.md)** - Multi-database support overview
- **[Replication Configuration Guide](docs/replication-config.md)** - Advanced filtering with TOML config files
- **[AWS Setup Guide](docs/aws-setup.md)** - Remote execution infrastructure details
- **[CI/CD Guide](docs/cicd.md)** - Automated testing and deployment
- **[CLAUDE.md](CLAUDE.md)** - Development guidelines and technical details

---

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.
