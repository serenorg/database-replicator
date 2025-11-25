# Performance Testing Report

**Date**: November 22, 2025
**Version**: 2.5.0
**Status**: Performance test framework implemented

---

## Executive Summary

This document describes the performance testing framework for `seren-replicator` across all supported database types (PostgreSQL, SQLite, MongoDB, MySQL). The test suite includes:

- **13 performance benchmarks** covering all database types
- **Automated test database generation** for consistent testing
- **Performance targets** for each database size category
- **Comprehensive coverage** of migration scenarios

**Framework Status**: ✅ Complete and ready for execution
**Test Execution**: Requires database connection URLs (TEST_TARGET_URL, TEST_MONGODB_URL, TEST_MYSQL_URL)

---

## Test Environment

### Hardware Specifications

To run performance tests, document your hardware:

- **CPU**: [e.g., Apple M1 Pro, Intel Core i7-10700K]
- **RAM**: [e.g., 16 GB]
- **Storage**: [e.g., SSD, NVMe]
- **OS**: [e.g., macOS 14.0, Ubuntu 22.04]

### Database Configurations

Document database versions and configurations:

- **PostgreSQL**: [e.g., PostgreSQL 17, default configuration]
- **SQLite**: [e.g., SQLite 3.43.0, libsqlite3-0]
- **MongoDB**: [e.g., MongoDB 7.0, default configuration]
- **MySQL**: [e.g., MySQL 8.0 / MariaDB 10.11, default configuration]

### Network Conditions

- **Target Database Location**: [e.g., Local, AWS us-east-1, etc.]
- **Network Latency**: [e.g., < 1ms local, ~20ms AWS, etc.]
- **Bandwidth**: [e.g., 1 Gbps local, 100 Mbps internet]

---

## Test Suite Overview

### Available Benchmarks

The performance test suite includes 13 benchmarks across 4 categories:

#### 1. SQLite Performance Tests (5 tests)

| Test Name | Database Size | Row Count | Performance Target |
|-----------|--------------|-----------|-------------------|
| `benchmark_sqlite_small_migration` | ~1 MB | 1,000 | < 10 seconds |
| `benchmark_sqlite_medium_migration` | ~10 MB | 30,000 | < 60 seconds |
| `benchmark_sqlite_large_migration` | ~100 MB | 100,000 | < 10 minutes |
| `benchmark_jsonb_batch_insert` | ~1 MB | 1,000 | < 5 seconds |
| `benchmark_many_small_tables` | ~1 MB | 1,000 (10 tables) | < 30 seconds |

**Notes:**
- SQLite databases are created programmatically by the test suite
- No external setup required
- Tests measure full migration including JSONB conversion

#### 2. MongoDB Performance Tests (2 tests)

| Test Name | Collection Size | Document Count | Performance Target |
|-----------|----------------|---------------|-------------------|
| `benchmark_mongodb_small_collection` | Small | < 10,000 | < 30 seconds |
| `benchmark_mongodb_medium_collection` | Medium | 10,000 - 100,000 | < 5 minutes |

**Setup Required:**
```bash
export TEST_MONGODB_URL="mongodb://user:pass@host:27017/dbname"
./scripts/create_perf_dbs.sh
```

#### 3. MySQL Performance Tests (2 tests)

| Test Name | Table Size | Row Count | Performance Target |
|-----------|-----------|-----------|-------------------|
| `benchmark_mysql_small_table` | Small | < 10,000 | < 30 seconds |
| `benchmark_mysql_medium_table` | Medium | 10,000 - 100,000 | < 5 minutes |

**Setup Required:**
```bash
export TEST_MYSQL_URL="mysql://user:pass@host:3306/dbname"
./scripts/create_perf_dbs.sh
```

#### 4. Infrastructure Tests (1 test)

| Test Name | Purpose | Performance Target |
|-----------|---------|-------------------|
| `benchmark_connection_overhead` | Measure connection establishment time | < 1 second |

---

## Running Performance Tests

### Prerequisites

1. **Set environment variables** for database connections:
   ```bash
   # Required for all tests
   export TEST_TARGET_URL="postgresql://user:pass@host:5432/db"

   # Optional (for MongoDB/MySQL tests)
   export TEST_MONGODB_URL="mongodb://user:pass@host:27017/db"
   export TEST_MYSQL_URL="mysql://user:pass@host:3306/db"
   ```

2. **Create test databases** (for MongoDB/MySQL):
   ```bash
   ./scripts/create_perf_dbs.sh
   ```

### Running Tests

```bash
# Run all performance tests
cargo test --release --test performance_test -- --ignored --nocapture

# Run specific test
cargo test --release --test performance_test benchmark_sqlite_small_migration -- --ignored --nocapture

# Run only SQLite tests
cargo test --release --test performance_test benchmark_sqlite -- --ignored --nocapture

# Run only MongoDB tests
cargo test --release --test performance_test benchmark_mongodb -- --ignored --nocapture

# Run only MySQL tests
cargo test --release --test performance_test benchmark_mysql -- --ignored --nocapture
```

**Important**: Use `--release` flag for accurate performance measurements. Debug builds are significantly slower.

---

## Performance Targets

### General Targets by Database Size

| Database Size | Expected Migration Time | Notes |
|--------------|------------------------|-------|
| < 100 MB | < 1 minute | Small datasets, single table |
| 100 MB - 1 GB | < 10 minutes | Medium datasets, multiple tables |
| 1 GB - 10 GB | < 1 hour | Large datasets, requires sustained performance |
| > 10 GB | Proportional | Scales linearly with size |

### Performance Metrics

For each test, measure and record:

1. **Total Migration Time**: End-to-end time from start to completion
2. **Throughput**: Rows per second or MB per second
3. **Peak Memory Usage**: Maximum RAM consumed during migration
4. **CPU Utilization**: Average CPU usage during migration
5. **Network I/O**: Data transferred over network (if remote database)

---

## Performance Test Results

### SQLite Migration Performance

#### Small Database (~1 MB, 1,000 rows)

| Metric | Target | Actual | Status |
|--------|--------|--------|--------|
| Migration Time | < 10 seconds | _[To be measured]_ | ⏳ Pending |
| Throughput | - | _[rows/sec]_ | ⏳ Pending |
| Peak Memory | - | _[MB]_ | ⏳ Pending |
| CPU Usage | - | _[%]_ | ⏳ Pending |

**Notes**: [Add observations here after running tests]

#### Medium Database (~10 MB, 30,000 rows)

| Metric | Target | Actual | Status |
|--------|--------|--------|--------|
| Migration Time | < 60 seconds | _[To be measured]_ | ⏳ Pending |
| Throughput | - | _[rows/sec]_ | ⏳ Pending |
| Peak Memory | - | _[MB]_ | ⏳ Pending |
| CPU Usage | - | _[%]_ | ⏳ Pending |

**Notes**: [Add observations here after running tests]

#### Large Database (~100 MB, 100,000 rows)

| Metric | Target | Actual | Status |
|--------|--------|--------|--------|
| Migration Time | < 10 minutes | _[To be measured]_ | ⏳ Pending |
| Throughput | - | _[rows/sec]_ | ⏳ Pending |
| Peak Memory | - | _[MB]_ | ⏳ Pending |
| CPU Usage | - | _[%]_ | ⏳ Pending |

**Notes**: [Add observations here after running tests]

---

### MongoDB Migration Performance

#### Small Collection (< 10,000 documents)

| Metric | Target | Actual | Status |
|--------|--------|--------|--------|
| Migration Time | < 30 seconds | _[To be measured]_ | ⏳ Pending |
| Throughput | - | _[docs/sec]_ | ⏳ Pending |
| Peak Memory | - | _[MB]_ | ⏳ Pending |
| CPU Usage | - | _[%]_ | ⏳ Pending |

**Notes**: [Add observations here after running tests]

#### Medium Collection (10,000 - 100,000 documents)

| Metric | Target | Actual | Status |
|--------|--------|--------|--------|
| Migration Time | < 5 minutes | _[To be measured]_ | ⏳ Pending |
| Throughput | - | _[docs/sec]_ | ⏳ Pending |
| Peak Memory | - | _[MB]_ | ⏳ Pending |
| CPU Usage | - | _[%]_ | ⏳ Pending |

**Notes**: [Add observations here after running tests]

---

### MySQL Migration Performance

#### Small Table (< 10,000 rows)

| Metric | Target | Actual | Status |
|--------|--------|--------|--------|
| Migration Time | < 30 seconds | _[To be measured]_ | ⏳ Pending |
| Throughput | - | _[rows/sec]_ | ⏳ Pending |
| Peak Memory | - | _[MB]_ | ⏳ Pending |
| CPU Usage | - | _[%]_ | ⏳ Pending |

**Notes**: [Add observations here after running tests]

#### Medium Table (10,000 - 100,000 rows)

| Metric | Target | Actual | Status |
|--------|--------|--------|--------|
| Migration Time | < 5 minutes | _[To be measured]_ | ⏳ Pending |
| Throughput | - | _[rows/sec]_ | ⏳ Pending |
| Peak Memory | - | _[MB]_ | ⏳ Pending |
| CPU Usage | - | _[%]_ | ⏳ Pending |

**Notes**: [Add observations here after running tests]

---

### JSONB Performance

#### Batch Insert (1,000 rows)

| Metric | Target | Actual | Status |
|--------|--------|--------|--------|
| Insert Time | < 5 seconds | _[To be measured]_ | ⏳ Pending |
| Throughput | - | _[rows/sec]_ | ⏳ Pending |
| Peak Memory | - | _[MB]_ | ⏳ Pending |

**Notes**: [Add observations here after running tests]

---

### Infrastructure Performance

#### Connection Overhead

| Metric | Target | Actual | Status |
|--------|--------|--------|--------|
| Connection Time | < 1 second | _[To be measured]_ | ⏳ Pending |

**Notes**: [Add observations here after running tests]

---

## Performance Bottlenecks

### Identified Bottlenecks

_[Document bottlenecks identified during testing]_

Examples:
- [ ] Slow JSONB conversion for large documents
- [ ] Network latency for remote databases
- [ ] Memory pressure with large result sets
- [ ] Single-threaded processing bottleneck

### Optimization Opportunities

_[Document potential optimizations]_

Examples:
- [ ] Implement batch insert for JSONB
- [ ] Add connection pooling
- [ ] Parallelize table processing
- [ ] Stream large result sets instead of loading into memory
- [ ] Add pagination for large collections

---

## Optimizations Applied

_[Document any optimizations implemented during or after testing]_

### Example Format:

**Optimization**: [Name of optimization]
**Problem**: [What performance issue was addressed]
**Solution**: [What changes were made]
**Impact**: [Before/after metrics]
**Commit**: [Commit hash]

---

## Performance Regression Testing

### Baseline Metrics

To detect performance regressions in future releases, record baseline metrics:

| Test | Baseline Time | Baseline Throughput | Version |
|------|--------------|---------------------|---------|
| SQLite Small | _[To be measured]_ | _[rows/sec]_ | 2.5.0 |
| SQLite Medium | _[To be measured]_ | _[rows/sec]_ | 2.5.0 |
| SQLite Large | _[To be measured]_ | _[rows/sec]_ | 2.5.0 |
| MongoDB Small | _[To be measured]_ | _[docs/sec]_ | 2.5.0 |
| MongoDB Medium | _[To be measured]_ | _[docs/sec]_ | 2.5.0 |
| MySQL Small | _[To be measured]_ | _[rows/sec]_ | 2.5.0 |
| MySQL Medium | _[To be measured]_ | _[rows/sec]_ | 2.5.0 |

### Regression Criteria

A performance regression is defined as:
- **Major Regression**: > 50% slower than baseline
- **Minor Regression**: 25-50% slower than baseline
- **Acceptable Variation**: < 25% difference (within normal variance)

---

## Comparison with Alternatives

_[Optional: Compare performance with alternative tools]_

### Example:

| Tool | Database Size | Migration Time | Notes |
|------|--------------|----------------|-------|
| seren-replicator | 100 MB | _[time]_ | This tool |
| pgloader | 100 MB | _[time]_ | Alternative |
| Custom scripts | 100 MB | _[time]_ | Manual approach |

---

## Recommendations

### For Production Use

Based on performance testing results:

1. **Small Databases (< 100 MB)**:
   - [Recommendation based on test results]
   - Expected time: [time]
   - Resource requirements: [specs]

2. **Medium Databases (100 MB - 1 GB)**:
   - [Recommendation based on test results]
   - Expected time: [time]
   - Resource requirements: [specs]

3. **Large Databases (> 1 GB)**:
   - [Recommendation based on test results]
   - Expected time: [time]
   - Resource requirements: [specs]
   - Consider: Remote execution for unattended migration

### Performance Tuning Tips

- **Local execution**: Faster for small databases, avoids network overhead
- **Remote execution**: Better for large databases, continues if laptop disconnects
- **Batch size tuning**: [Recommendations based on testing]
- **Connection pooling**: [Recommendations based on testing]
- **Parallel processing**: Already optimized with automatic CPU detection

---

## Appendix

### Test Database Schemas

#### SQLite Small Database
```sql
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    email TEXT NOT NULL,
    age INTEGER,
    balance REAL,
    bio TEXT,
    avatar BLOB
);
-- 1,000 rows
```

#### SQLite Medium Database
```sql
CREATE TABLE users (...);  -- 10,000 rows
CREATE TABLE posts (...);  -- 20,000 rows
```

#### SQLite Large Database
```sql
CREATE TABLE events (...);  -- 100,000 rows
```

#### MongoDB Test Databases
- `small_test.users`: 1,000 documents
- `medium_test.events`: 50,000 documents

#### MySQL Test Databases
- `small_test.users`: 1,000 rows
- `medium_test.events`: 50,000 rows

### Running the Complete Test Suite

```bash
# 1. Set all environment variables
export TEST_TARGET_URL="postgresql://postgres:postgres@localhost:5432/postgres"
export TEST_MONGODB_URL="mongodb://localhost:27017/test"
export TEST_MYSQL_URL="mysql://root:password@localhost:3306/test"

# 2. Create MongoDB and MySQL test databases
./scripts/create_perf_dbs.sh

# 3. Run all performance tests with release optimizations
cargo test --release --test performance_test -- --ignored --nocapture 2>&1 | tee performance-results.log

# 4. Review results and update this report with actual measurements
```

---

## Version History

| Version | Date | Changes | Tested By |
|---------|------|---------|-----------|
| 2.5.0 | 2025-11-22 | Initial performance test framework | _Pending_ |

---

**Status**: Performance test framework complete and ready for execution. Actual test results pending database connection availability.
