# Integration Testing Guide

This guide explains how to run integration tests for the seren-replicator project.

## Overview

Integration tests validate the tool against real PostgreSQL databases. They are marked with `#[ignore]` by default because they require:
- Two running PostgreSQL instances (source and target)
- Test database credentials
- Network connectivity

## Test Categories

### Unit Tests (Always Run)
```bash
cargo test
```

Runs fast, isolated tests that don't require external dependencies.

### Integration Tests (Manual)
```bash
cargo test --test integration_test -- --ignored
```

Tests that validate commands against real databases.

## Setting Up Test Databases

### Option 1: Docker (Recommended)

The fastest way to set up test databases:

```bash
# Start source database on port 5432
docker run -d --name pg-source \
  -e POSTGRES_PASSWORD=postgres \
  -p 5432:5432 \
  postgres:17

# Start target database on port 5433
docker run -d --name pg-target \
  -e POSTGRES_PASSWORD=postgres \
  -p 5433:5432 \
  postgres:17

# Wait for databases to be ready
sleep 5

# Set environment variables
export TEST_SOURCE_URL="postgresql://postgres:postgres@localhost:5432/postgres"
export TEST_TARGET_URL="postgresql://postgres:postgres@localhost:5433/postgres"

# Run integration tests
cargo test --test integration_test -- --ignored

# Cleanup
docker stop pg-source pg-target
docker rm pg-source pg-target
```

### Option 2: Cloud Databases

For testing with real cloud providers (Neon, AWS RDS, etc.):

```bash
export TEST_SOURCE_URL="postgresql://user:pass@source.cloud.provider.com:5432/mydb"
export TEST_TARGET_URL="postgresql://user:pass@target.cloud.provider.com:5432/mydb"

cargo test --test integration_test -- --ignored
```

**Warning:** Some tests perform destructive operations (init, sync). Use dedicated test databases!

## Available Integration Tests

### test_validate_command_integration
Tests the `validate` command against source and target databases.
- **Safe:** Read-only operation
- **Duration:** ~2 seconds
- **Validates:** Connection, privileges, PostgreSQL version

### test_init_command_integration
Tests the `init` command for initial snapshot replication.
- **Destructive:** Drops and recreates target database
- **Duration:** Depends on database size (~30 seconds for empty DB)
- **Validates:** Full init workflow (globals, schema, data)

### test_sync_command_integration
Tests the `sync` command for setting up logical replication.
- **Modifies:** Creates publications and subscriptions
- **Duration:** ~10 seconds
- **Validates:** Publication/subscription setup

### test_status_command_integration
Tests the `status` command for monitoring replication.
- **Safe:** Read-only operation
- **Duration:** ~2 seconds
- **Validates:** Replication status queries

### test_verify_command_integration
Tests the `verify` command for data integrity checking.
- **Safe:** Read-only operation
- **Duration:** Depends on database size
- **Validates:** Checksum calculation and comparison

## Running Specific Tests

```bash
# Run only validate test
cargo test --test integration_test test_validate_command_integration -- --ignored

# Run only non-destructive tests
cargo test --test integration_test test_validate_command_integration test_status_command_integration test_verify_command_integration -- --ignored

# Run with output
cargo test --test integration_test -- --ignored --nocapture
```

## CI/CD Integration

Integration tests are **not** run in CI by default because:
1. They require real database instances (cost and complexity)
2. Some tests are destructive
3. Test duration varies significantly

### Option A: Manual CI Runs

Add a manual workflow trigger:

```yaml
name: Integration Tests
on: workflow_dispatch

jobs:
  integration:
    runs-on: ubuntu-latest
    services:
      postgres-source:
        image: postgres:17
        env:
          POSTGRES_PASSWORD: postgres
        ports:
          - 5432:5432
      postgres-target:
        image: postgres:17
        env:
          POSTGRES_PASSWORD: postgres
        ports:
          - 5433:5432
    steps:
      - uses: actions/checkout@v4
      - run: cargo test --test integration_test -- --ignored
        env:
          TEST_SOURCE_URL: postgresql://postgres:postgres@localhost:5432/postgres
          TEST_TARGET_URL: postgresql://postgres:postgres@localhost:5433/postgres
```

### Option B: Scheduled Nightly Tests

Run integration tests on a schedule:

```yaml
name: Nightly Integration Tests
on:
  schedule:
    - cron: '0 0 * * *'  # Midnight UTC
```

### Option C: Conditional Running

Run only on specific PR labels:

```yaml
name: Conditional Integration Tests
on:
  pull_request:
    types: [labeled]

jobs:
  integration:
    if: contains(github.event.pull_request.labels.*.name, 'integration-test')
    # ... rest of job
```

## Test Database Requirements

**Source Database:**
- PostgreSQL 12+ (17 recommended)
- `REPLICATION` privilege or superuser
- Can create publications
- Logical replication enabled (`wal_level = logical`)

**Target Database:**
- PostgreSQL 12+ (17 recommended)
- Superuser or database owner privileges
- Can create subscriptions
- Accessible from worker (network/firewall rules)

## Troubleshooting

### Connection Refused

```
Error: Connection refused (os error 61)
```

**Fix:** Ensure PostgreSQL is running and listening on the specified port:
```bash
docker ps  # Check containers are running
psql $TEST_SOURCE_URL -c "SELECT version();"  # Test connection
```

### Permission Denied

```
Error: Permission denied to create publication
```

**Fix:** Grant required privileges:
```sql
ALTER USER your_user WITH REPLICATION;
```

### Logical Replication Not Enabled

```
Error: logical decoding requires wal_level >= logical
```

**Fix:** Configure PostgreSQL for logical replication:
```sql
ALTER SYSTEM SET wal_level = logical;
-- Restart PostgreSQL
```

For Docker: Use custom config or postgres:17-alpine with proper settings.

## Writing New Integration Tests

Template for a new integration test:

```rust
#[test]
#[ignore]  // Mark as integration test
fn test_my_command_integration() -> Result<()> {
    // Get test database URLs
    let source_url = env::var("TEST_SOURCE_URL")
        .expect("TEST_SOURCE_URL must be set for integration tests");
    let target_url = env::var("TEST_TARGET_URL")
        .expect("TEST_TARGET_URL must be set for integration tests");

    // Run the command
    let result = my_command(&source_url, &target_url);

    // Assert expected behavior
    assert!(result.is_ok(), "Command should succeed");

    Ok(())
}
```

## Best Practices

1. **Isolate Tests:** Use separate test databases, not production
2. **Clean State:** Reset databases between test runs if needed
3. **Document Destruction:** Clearly mark which tests modify data
4. **Fast Feedback:** Run unit tests frequently, integration tests less often
5. **CI Strategy:** Use manual triggers or schedules for integration tests
6. **Timeouts:** Set reasonable timeouts for long-running operations

## Related Documentation

- [Testing](../CLAUDE.md#testing) - TDD requirements and test strategy
- [CI/CD Pipeline](../.github/workflows/ci.yml) - Current CI configuration
- [Deployment](../aws/README.md) - Deployment guide
