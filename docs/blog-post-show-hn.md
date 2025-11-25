# Show HN: Database-replicator – Zero-downtime database migrations to PostgreSQL

**TL;DR:** We built an open-source CLI tool that replicates databases from PostgreSQL, MySQL, MongoDB, and SQLite to PostgreSQL with zero downtime. It's written in Rust, supports continuous sync via logical replication, and can optionally run on managed cloud infrastructure. [GitHub](https://github.com/serenorg/database-replicator) | [Crates.io](https://crates.io/crates/database-replicator)

---

## The Problem We Were Solving

At SerenAI, we're building PostgreSQL databases optimized for AI workloads. Our users kept asking the same question: "How do I migrate my existing database to SerenDB without downtime?"

The existing options weren't great:

**pg_dump/pg_restore** works, but requires downtime. For a 100GB database, you're looking at hours of your application being offline. Not acceptable for production systems.

**AWS DMS and similar tools** are powerful but complex. They require provisioning infrastructure, configuring replication instances, and managing IAM roles. Overkill for most migrations.

**Logical replication setup** is the right approach, but it's tedious. You need to configure `wal_level`, create publications, set up subscriptions, handle the initial snapshot, monitor lag, and verify data integrity. Each step has gotchas.

We wanted something simpler: a single command that handles everything.

## What We Built

`database-replicator` is a Rust CLI that automates the entire migration process:

```bash
# Validate both databases meet prerequisites
database-replicator validate --source $SOURCE --target $TARGET

# Initial snapshot + set up continuous sync
database-replicator init --source $SOURCE --target $TARGET --enable-sync

# Monitor replication lag
database-replicator status --source $SOURCE --target $TARGET

# Verify data integrity with checksums
database-replicator verify --source $SOURCE --target $TARGET
```

### How It Works (PostgreSQL to PostgreSQL)

For PostgreSQL sources, we use native logical replication, which is the gold standard for zero-downtime migrations:

1. **Validate** - Check that the source has `wal_level = logical`, user has REPLICATION privilege, and target can create subscriptions.

2. **Initial Snapshot** - Use `pg_dump` in directory format with parallel workers (auto-detected based on CPU cores). We dump globals, schema, and data separately for proper dependency ordering.

3. **Create Publication** - Set up a publication on the source for all tables (or filtered tables if you're doing selective replication).

4. **Create Subscription** - The target subscribes to the source. PostgreSQL handles the rest—every INSERT, UPDATE, and DELETE is streamed in real-time.

5. **Monitor** - Track replication lag in bytes and time. When lag hits zero, you're ready to cut over.

6. **Verify** - Compute MD5 checksums across all tables on both sides. Any discrepancy is flagged immediately.

The entire flow handles edge cases: TCP keepalives for long-running operations behind load balancers, retry with exponential backoff for transient failures, and proper credential handling via `.pgpass` files (never exposed in process arguments).

### Multi-Source Support

Not everyone is migrating from PostgreSQL. We added support for:

- **MySQL/MariaDB → PostgreSQL**: Schema translation, type mapping, one-time snapshot with optional periodic refresh
- **MongoDB → PostgreSQL**: Documents stored as JSONB, preserving the flexible schema while gaining SQL queryability
- **SQLite → PostgreSQL**: Perfect for graduating from a local SQLite database to a production PostgreSQL instance

For non-PostgreSQL sources, we can't use logical replication (it's a PostgreSQL-specific feature), so these are snapshot-based migrations with optional scheduled refreshes.

## Selective Replication

Real migrations are rarely "replicate everything." You might want to:

- Exclude large analytics tables that you'll backfill separately
- Filter old data (only replicate orders from the last 90 days)
- Skip tables with PII for a staging environment

We support this through CLI flags or a TOML config file:

```toml
[databases.mydb]
schema_only = ["large_analytics_table"]

[[databases.mydb.table_filters]]
table = "orders"
where = "created_at > NOW() - INTERVAL '90 days'"

[[databases.mydb.time_filters]]
table = "events"
column = "timestamp"
last = "6 months"
```

The filter predicates are pushed down to `pg_dump` and used in publication WHERE clauses (PostgreSQL 15+), so you're not transferring data you don't need.

## The Cloud Execution Feature

Here's where it gets interesting. Running a migration locally has drawbacks:

- Your laptop needs to stay connected for hours (or days for large databases)
- Network bandwidth is limited by your ISP upload speed
- If your connection drops, you need to restart or resume from checkpoints

For users migrating to SerenDB, we built a remote execution feature. Instead of running locally, the replication job runs on our AWS infrastructure:

```bash
export SEREN_API_KEY="your-api-key"  # from console.serendb.com
database-replicator init \
  --source "postgresql://user:pass@your-rds.amazonaws.com:5432/db" \
  --target "postgresql://user:pass@your-db.serendb.com:5432/db"
```

When the target is a SerenDB database, we automatically provision an EC2 worker, run the migration there, stream progress back to your terminal, and clean up when done. Your laptop can disconnect—the job continues.

The architecture:
- API Gateway + Lambda for job coordination
- SQS for reliable job queuing
- EC2 workers with the replicator binary pre-installed
- KMS encryption for credentials (never stored in plaintext)
- DynamoDB for job state tracking

For non-SerenDB targets, add `--local` and it runs on your machine like any other CLI tool.

## Technical Decisions

**Why Rust?** Performance and reliability. Database migrations are long-running operations where memory safety matters. We also get easy cross-compilation for Linux, macOS Intel, and macOS ARM.

**Why shell out to pg_dump?** We considered implementing dump logic directly, but `pg_dump` is battle-tested across millions of databases. It handles edge cases we'd never think of. We wrap it with proper error handling, progress tracking, and retry logic.

**Why logical replication over CDC tools?** PostgreSQL's built-in logical replication is robust and doesn't require additional infrastructure. It's what PostgreSQL itself uses for read replicas. For PostgreSQL-to-PostgreSQL migrations, there's no reason to add complexity.

**Checkpoint system** - Long migrations need resume support. We write checkpoints to `.seren-replicator/` after each database completes. If interrupted, the next run picks up where it left off. Checkpoints include a fingerprint of the filter configuration—if you change filters, it starts fresh to prevent data inconsistency.

## Limitations (Being Honest)

- **PostgreSQL-only targets**: We replicate *to* PostgreSQL, not from it to other databases
- **Logical replication requires PostgreSQL 10+** on the source (we recommend 12+)
- **Large objects (BLOBs)** aren't replicated via logical replication—use `pg_dump` for those
- **DDL changes** during replication need manual handling (logical replication doesn't capture schema changes)
- **Sequences** need to be manually advanced after cutover

## Getting Started

Install from crates.io:
```bash
cargo install database-replicator
```

Or download binaries from the [GitHub releases](https://github.com/serenorg/database-replicator/releases).

Basic migration:
```bash
# Check prerequisites
database-replicator validate \
  --source "postgresql://user:pass@source:5432/mydb" \
  --target "postgresql://user:pass@target:5432/mydb"

# Run migration with continuous sync
database-replicator init \
  --source "postgresql://user:pass@source:5432/mydb" \
  --target "postgresql://user:pass@target:5432/mydb" \
  --enable-sync

# Monitor until lag is zero
database-replicator status \
  --source "postgresql://user:pass@source:5432/mydb" \
  --target "postgresql://user:pass@target:5432/mydb"

# Verify checksums match
database-replicator verify \
  --source "postgresql://user:pass@source:5432/mydb" \
  --target "postgresql://user:pass@target:5432/mydb"

# Cut over your application to the new database
# Then drop the subscription on target
```

## What's Next

We're working on:
- **CDC for non-PostgreSQL sources** - Real-time sync from MySQL using binlog
- **Progress percentage** - Better estimation of completion time
- **Web dashboard** - Visual monitoring for remote jobs

The tool is Apache 2.0 licensed. Contributions welcome—especially around MySQL CDC and additional source database support.

---

[GitHub](https://github.com/serenorg/database-replicator) | [Documentation](https://github.com/serenorg/database-replicator#readme) | [SerenAI Console](https://console.serendb.com)

Happy to answer questions about the architecture, PostgreSQL logical replication gotchas, or anything else.
