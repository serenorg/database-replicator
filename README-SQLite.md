# SQLite Migration Guide

This guide explains how to migrate SQLite databases to PostgreSQL using postgres-seren-replicator's JSONB storage approach.

## Overview

The tool automatically detects SQLite database files and migrates them to PostgreSQL using a JSONB storage model. All SQLite data is preserved with full type fidelity, including BLOBs, NULLs, and special float values.

**Key Features:**
- **Zero Configuration**: Automatic source type detection from file extension
- **Type Preservation**: Lossless conversion of all SQLite types to JSONB
- **Security First**: Read-only connections, path validation, SQL injection prevention
- **BLOB Support**: Binary data encoded as base64 in JSONB
- **Metadata Tracking**: Each row includes source type and migration timestamp

## Quick Start

### Basic Migration

Migrate an entire SQLite database to PostgreSQL:

```bash
postgres-seren-replicator init \
  --source /path/to/database.db \
  --target "postgresql://user:pass@target-host:5432/db"
```

The tool automatically:
1. Detects that the source is SQLite (from `.db` extension)
2. Validates the file path for security
3. Opens the database in read-only mode
4. Lists all tables
5. Creates corresponding JSONB tables in PostgreSQL
6. Converts and inserts all data

### Supported File Extensions

The tool recognizes these SQLite file extensions:
- `.db`
- `.sqlite`
- `.sqlite3`

## Data Type Mapping

SQLite types are mapped to JSONB as follows:

| SQLite Type | JSON Type | Example Input | JSON Output |
|-------------|-----------|---------------|-------------|
| INTEGER | number | `42` | `42` |
| REAL | number | `3.14` | `3.14` |
| TEXT | string | `"Hello"` | `"Hello"` |
| NULL | null | `NULL` | `null` |
| BLOB | object | `X'48656C6C6F'` | `{"_type": "blob", "data": "SGVsbG8="}` |

### Special Cases

**Non-Finite Floats:**
- `NaN`, `Infinity`, and `-Infinity` are converted to strings for JSON compatibility
- Example: `NaN` → `"NaN"`

**BLOB Data:**
- Encoded as base64 in a structured object
- Format: `{"_type": "blob", "data": "<base64>"}`
- Allows distinguishing BLOBs from regular strings

## PostgreSQL Table Structure

Each SQLite table is converted to a PostgreSQL table with this schema:

```sql
CREATE TABLE IF NOT EXISTS "table_name" (
    id TEXT PRIMARY KEY,
    data JSONB NOT NULL,
    _source_type TEXT NOT NULL DEFAULT 'sqlite',
    _migrated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Performance indexes
CREATE INDEX IF NOT EXISTS "idx_table_name_data" ON "table_name" USING GIN (data);
CREATE INDEX IF NOT EXISTS "idx_table_name_source" ON "table_name" (_source_type);
```

**Field Descriptions:**
- `id`: String ID from SQLite's ID column or generated row number
- `data`: JSONB containing all column data from the SQLite row
- `_source_type`: Always `'sqlite'` for SQLite migrations
- `_migrated_at`: Timestamp of when the row was migrated

## Querying Migrated Data

### Basic Queries

Query JSONB data using PostgreSQL's JSONB operators:

```sql
-- Get all users
SELECT id, data FROM users;

-- Get specific fields
SELECT
    id,
    data->>'name' AS name,
    data->>'email' AS email,
    (data->>'age')::int AS age
FROM users;

-- Filter by JSONB fields
SELECT * FROM users WHERE data->>'name' = 'Alice';

-- Range queries
SELECT * FROM users WHERE (data->>'age')::int > 25;

-- Check for NULL values
SELECT * FROM users WHERE data->'age' IS NULL;
```

### Advanced Queries

```sql
-- Search in nested JSONB
SELECT * FROM events WHERE data->'metadata'->>'category' = 'login';

-- Array operations
SELECT * FROM products WHERE data->'tags' ? 'electronics';

-- Full-text search
SELECT * FROM articles
WHERE to_tsvector('english', data->>'content') @@ to_tsquery('postgresql');

-- Aggregations
SELECT
    data->>'status' AS status,
    COUNT(*) AS count,
    AVG((data->>'amount')::numeric) AS avg_amount
FROM orders
GROUP BY data->>'status';
```

### Working with BLOBs

Decode base64-encoded BLOBs:

```sql
-- Check if a field is a BLOB
SELECT id, data->'avatar'->>'_type' AS type
FROM users
WHERE data->'avatar'->>'_type' = 'blob';

-- Decode BLOB data (PostgreSQL 14+)
SELECT
    id,
    data->>'name' AS name,
    decode(data->'avatar'->>'data', 'base64') AS avatar_bytes
FROM users
WHERE data->'avatar'->>'_type' = 'blob';
```

## Security

### Path Validation

The tool validates SQLite file paths to prevent attacks:

**Protected Against:**
- Path traversal: `../../../etc/passwd` ❌
- Absolute paths to system files: `/etc/shadow` ❌
- Shell metacharacters: `file.db; rm -rf /` ❌
- Symlink attacks outside allowed directories ❌

**Requirements:**
- File must exist and be readable
- Must be a regular file (not directory)
- Must have `.db`, `.sqlite`, or `.sqlite3` extension

### Read-Only Access

All SQLite connections are opened with `SQLITE_OPEN_READ_ONLY` flag:
- No modifications to source database possible
- INSERT, UPDATE, DELETE, CREATE, DROP all fail
- Safe for production database migration

### SQL Injection Prevention

Table names are validated before use in SQL:
- Only alphanumeric characters and underscores allowed
- Reserved SQL keywords (SELECT, DROP, etc.) rejected
- All queries use validated identifiers or parameterized queries

## Examples

### Example 1: Migrate Local Database

```bash
# Download SQLite sample database
wget https://example.com/sample.db

# Migrate to PostgreSQL
postgres-seren-replicator init \
  --source ./sample.db \
  --target "postgresql://localhost:5432/mydb"
```

### Example 2: Query Migrated Data

After migration, query the data:

```sql
-- Connect to PostgreSQL
psql "postgresql://localhost:5432/mydb"

-- List migrated tables
\dt

-- View sample data
SELECT id, data FROM products LIMIT 5;

-- Query specific fields
SELECT
    id,
    data->>'name' AS product_name,
    (data->>'price')::numeric AS price,
    data->>'category' AS category
FROM products
WHERE (data->>'price')::numeric < 50
ORDER BY (data->>'price')::numeric DESC;
```

### Example 3: Handling Special Data Types

```sql
-- Find rows with NULL values
SELECT id, data->>'name' AS name
FROM users
WHERE data->'age' IS NULL;

-- Find rows with BLOB data
SELECT id, data->>'name' AS name
FROM users
WHERE data->'avatar'->>'_type' = 'blob';

-- Extract BLOB size
SELECT
    id,
    data->>'name' AS name,
    length(decode(data->'avatar'->>'data', 'base64')) AS avatar_size_bytes
FROM users
WHERE data->'avatar'->>'_type' = 'blob';
```

## Performance Considerations

### Batch Insert Performance

The tool inserts data in batches of 1000 rows for optimal performance:
- Large databases migrate faster than single-row inserts
- PostgreSQL parameters per batch: 3000 (well under 65535 limit)
- Progress tracking shows estimated completion time

### Index Usage

GIN indexes on JSONB columns enable fast queries:

```sql
-- Uses GIN index
EXPLAIN SELECT * FROM users WHERE data @> '{"status": "active"}';

-- Uses GIN index
EXPLAIN SELECT * FROM users WHERE data->>'email' = 'user@example.com';
```

For frequently queried fields, consider adding expression indexes:

```sql
-- Index for email queries
CREATE INDEX idx_users_email ON users ((data->>'email'));

-- Index for age range queries
CREATE INDEX idx_users_age ON users (((data->>'age')::int));
```

### Large Database Tips

For very large SQLite databases:

1. **Monitor Progress**: The tool shows real-time progress with ETA
2. **Check Disk Space**: Ensure sufficient space on target PostgreSQL server
3. **Connection Timeouts**: Use connection strings with keepalive parameters
4. **Network Stability**: For remote PostgreSQL, ensure stable network connection

## Limitations

### No Continuous Sync

SQLite migrations are **snapshot-only**:
- Data migrated at a point in time
- No continuous replication like PostgreSQL logical replication
- Re-running `init` will recreate tables (drop and recreate)

**Workaround for Ongoing Sync:**
- Use SQLite's backup API to create periodic snapshots
- Re-run migration with updated snapshot
- Consider PostgreSQL FDW for live SQLite querying

### No Schema Preservation

SQLite table schemas are not preserved:
- All data stored as JSONB regardless of original column types
- Column constraints (UNIQUE, CHECK, etc.) not enforced
- Foreign key relationships not maintained

**Workaround:**
- Create views to expose typed columns
- Add constraints manually after migration
- Use triggers to enforce business rules

### ID Column Detection

ID columns are detected by name:
- Checks for columns named `id`, `rowid`, or `_id` (case-insensitive)
- Uses SQLite's implicit `rowid` if no ID column found
- Always converted to string in PostgreSQL `id` field

**Custom IDs:**
If your SQLite table uses a different ID column (e.g., `user_id`), the tool will use row numbers as IDs. Access the original ID via JSONB:

```sql
SELECT
    id AS generated_id,
    data->>'user_id' AS original_id,
    data->>'name' AS name
FROM users;
```

## Troubleshooting

### Error: "Failed to resolve SQLite file path"

**Cause**: File doesn't exist or isn't readable

**Solutions:**
- Check file path spelling and location
- Verify file permissions: `ls -l /path/to/database.db`
- Use absolute paths to avoid confusion: `/full/path/to/database.db`

### Error: "Invalid SQLite file extension"

**Cause**: File doesn't have `.db`, `.sqlite`, or `.sqlite3` extension

**Solutions:**
- Rename file: `mv database.sqlite3.bak database.sqlite3`
- Copy with correct extension: `cp original.bak database.db`

### Error: "Path is not a regular file"

**Cause**: Path points to a directory, not a file

**Solutions:**
- Check if you specified a directory instead of file
- List directory contents: `ls /path/to/directory/`
- Use full path to database file

### Error: "Invalid table name"

**Cause**: SQLite table name contains invalid characters or is a reserved keyword

**Solutions:**
- SQLite tables with special characters may fail validation
- Reserved keywords (select, insert, etc.) are rejected for security
- Rename tables in SQLite before migration if possible

### Migration is Slow

**Causes and Solutions:**

1. **Network Latency**: Target PostgreSQL is remote
   - Use local migration then replicate PostgreSQL-to-PostgreSQL
   - Check network bandwidth with `iperf`

2. **Large BLOBs**: Tables contain many large binary objects
   - Base64 encoding increases size by ~33%
   - Consider storing BLOBs separately (e.g., S3) with references

3. **PostgreSQL Performance**: Target server under load
   - Check with `pg_stat_activity`
   - Increase `work_mem` and `maintenance_work_mem` temporarily
   - Disable autovacuum during migration: `ALTER TABLE foo SET (autovacuum_enabled = false);`

## Best Practices

### Before Migration

1. **Backup SQLite Database**
   ```bash
   cp database.db database.db.backup
   ```

2. **Test with Sample Data**
   ```bash
   # Create test database with sample data
   sqlite3 test.db < test_data.sql

   # Test migration
   postgres-seren-replicator init --source test.db --target "postgresql://localhost/test"
   ```

3. **Verify PostgreSQL Space**
   ```sql
   SELECT pg_size_pretty(pg_database_size('mydb'));
   ```

### During Migration

1. **Monitor Progress**: The tool shows real-time progress
2. **Don't Interrupt**: Let migration complete to avoid partial data
3. **Check Logs**: Watch for warnings about NULL values or type conversions

### After Migration

1. **Verify Row Counts**
   ```sql
   -- PostgreSQL
   SELECT 'users' AS table, COUNT(*) AS rows FROM users
   UNION ALL
   SELECT 'products', COUNT(*) FROM products;

   -- SQLite
   SELECT 'users' AS table, COUNT(*) AS rows FROM users
   UNION ALL
   SELECT 'products', COUNT(*) FROM products;
   ```

2. **Spot Check Data**
   ```sql
   -- Compare specific rows
   SELECT * FROM users WHERE id = '1';
   ```

3. **Create Application Views**
   ```sql
   -- Create view with typed columns for application use
   CREATE VIEW users_typed AS
   SELECT
       id,
       data->>'name' AS name,
       data->>'email' AS email,
       (data->>'age')::int AS age,
       (data->>'created_at')::timestamp AS created_at
   FROM users;
   ```

4. **Add Performance Indexes**
   ```sql
   -- Add indexes for common queries
   CREATE INDEX idx_users_email ON users ((data->>'email'));
   CREATE INDEX idx_products_category ON products ((data->>'category'));
   ```

## FAQ

### Why JSONB instead of preserving schema?

**Benefits:**
- **Zero Configuration**: No schema mapping required
- **Type Safety**: All SQLite types preserved losslessly
- **Flexibility**: Easy to query without rigid schema
- **AI-Friendly**: LLMs can query JSONB without knowing schema upfront

**Trade-offs:**
- No column-level constraints enforcement
- Requires type casting in queries
- Slightly larger storage footprint

### Can I convert JSONB back to columns?

Yes! Create a regular table and populate from JSONB:

```sql
-- Create typed table
CREATE TABLE users_typed (
    id SERIAL PRIMARY KEY,
    original_id TEXT,
    name TEXT NOT NULL,
    email TEXT,
    age INT,
    created_at TIMESTAMP
);

-- Populate from JSONB
INSERT INTO users_typed (original_id, name, email, age, created_at)
SELECT
    id,
    data->>'name',
    data->>'email',
    (data->>'age')::int,
    (data->>'created_at')::timestamp
FROM users;
```

### Does this modify my SQLite database?

No. The tool opens SQLite databases in **read-only mode**. It's impossible to modify the source database.

### Can I migrate the same database twice?

Re-running `init` will **drop and recreate** tables. All data will be replaced with fresh data from SQLite. This is useful for:
- Correcting errors in the first migration
- Refreshing data from an updated SQLite snapshot

### How do I handle incremental updates?

SQLite migrations are snapshot-only. For incremental updates:

1. **Option 1**: Periodic full re-migration
   - Create SQLite backup/snapshot
   - Re-run `init` with `--drop-existing` flag (implied in init)

2. **Option 2**: Track changes in application
   - Maintain updated_at timestamps
   - Query for changed records: `SELECT * FROM users WHERE updated_at > ?`
   - Upsert changed records manually

3. **Option 3**: Migrate to PostgreSQL entirely
   - After initial migration, move application to PostgreSQL
   - Retire SQLite database

### Is the source database locked during migration?

No. The tool uses `SQLITE_OPEN_READ_ONLY` which allows concurrent readers. Other processes can read the database during migration.

## Additional Resources

- [Main README](README.md) - PostgreSQL-to-PostgreSQL migration
- [CHANGELOG](CHANGELOG.md) - Version history
- [GitHub Issues](https://github.com/serenorg/postgres-seren-replicator/issues) - Report bugs or request features

## Support

For issues or questions:
- **GitHub Issues**: https://github.com/serenorg/postgres-seren-replicator/issues
- **Email**: support@seren.ai
