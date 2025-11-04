# seren-neon-migrator

Zero-downtime database migration tool from Neon to Seren using PostgreSQL logical replication.

## Overview

This tool enables safe, zero-downtime migration of PostgreSQL databases from Neon Cloud to Seren Cloud. It uses PostgreSQL's logical replication to keep databases in sync during the migration process.

## Features

- **Zero Downtime**: Uses logical replication to keep databases in sync
- **Complete Migration**: Migrates schema, data, roles, and permissions
- **Data Validation**: Checksum-based verification of data integrity
- **Real-time Monitoring**: Track replication lag and status
- **Safe & Fail-fast**: Validates prerequisites before starting migration

## Migration Workflow

The migration process follows 5 phases:

1. **Validate** - Check source and target databases meet requirements
2. **Init** - Copy initial schema and data using pg_dump/restore
3. **Sync** - Set up logical replication between databases
4. **Status** - Monitor replication lag and health
5. **Verify** - Validate data integrity with checksums

## Installation

### Prerequisites

- Rust 1.70 or later
- PostgreSQL client tools (pg_dump, pg_dumpall, psql)
- Access to both Neon and Seren databases with appropriate permissions

### Build from Source

```bash
git clone https://github.com/serenorg/seren-neon-migrator.git
cd seren-neon-migrator
cargo build --release
```

The binary will be available at `target/release/seren-neon-migrator`.

## Usage

### 1. Validate Databases

Check that both databases meet migration requirements:

```bash
./seren-neon-migrator validate \
  --source "postgresql://user:pass@neon-host:5432/db" \
  --target "postgresql://user:pass@seren-host:5432/db"
```

### 2. Initialize Migration

Copy initial schema and data:

```bash
./seren-neon-migrator init \
  --source "postgresql://user:pass@neon-host:5432/db" \
  --target "postgresql://user:pass@seren-host:5432/db"
```

### 3. Set Up Replication

Enable logical replication to sync ongoing changes:

```bash
./seren-neon-migrator sync \
  --source "postgresql://user:pass@neon-host:5432/db" \
  --target "postgresql://user:pass@seren-host:5432/db"
```

### 4. Monitor Status

Check replication health and lag:

```bash
./seren-neon-migrator status \
  --source "postgresql://user:pass@neon-host:5432/db" \
  --target "postgresql://user:pass@seren-host:5432/db"
```

### 5. Verify Data Integrity

Validate that all tables match:

```bash
./seren-neon-migrator verify \
  --source "postgresql://user:pass@neon-host:5432/db" \
  --target "postgresql://user:pass@seren-host:5432/db"
```

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

### Source Database (Neon)

- PostgreSQL 12 or later
- Replication privilege (`REPLICATION` role attribute)
- Ability to create publications

### Target Database (Seren)

- PostgreSQL 12 or later
- Superuser or database owner privileges
- Ability to create subscriptions
- Network connectivity to source database

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
DROP SUBSCRIPTION IF EXISTS seren_migration_sub;

-- On source
DROP PUBLICATION IF EXISTS seren_migration_pub;
```

### Replication lag

Check status frequently during migration:

```bash
# Monitor until lag < 1 second
watch -n 5 './seren-neon-migrator status --source "$SOURCE" --target "$TARGET"'
```

## License

MIT
