# SQLite Replication Guide

This guide explains how to replicate SQLite databases to PostgreSQL using database-replicator's JSONB storage approach.

---

## SerenAI Cloud Replication

**New to SerenAI?** Sign up at [console.serendb.com](https://console.serendb.com) to get a managed PostgreSQL database optimized for AI workloads.

**Note:** SQLite replication requires local execution (the `--local` flag) since the source file must be accessible from your machine.

```bash
database-replicator init -y \
  --source /path/to/database.db \
  --target "postgresql://user:pass@your-db.serendb.com:5432/db" \
  --local
```

**Important:** The `-y` flag (or `--no-interactive`) is required for SQLite sources to skip the interactive database selection prompt, which only works with PostgreSQL sources.

---

## Installation

Get the CLI onto your machine before following the SQLite workflow.

### Option 1: Download a Pre-built Binary

1. Visit the [latest GitHub Release](https://github.com/serenorg/database-replicator/releases/latest).
2. Download the asset for your operating system and CPU (Linux x86_64/arm64, macOS Intel/Apple Silicon, or Windows x86_64).
3. Extract the archive, then on Linux/macOS run:

```bash
chmod +x database-replicator*
sudo mv database-replicator* /usr/local/bin/database-replicator
database-replicator --help
```

4. On Windows, run the `.exe` directly or add it to your `PATH`.

### Option 2: Build from Source

Requires Rust 1.70 or later.

```bash
# Install from crates.io
cargo install database-replicator

# Or build from the repository
git clone https://github.com/serenorg/database-replicator.git
cd database-replicator
cargo build --release
./target/release/database-replicator --help
```

Source builds are ideal when you need to pin to a specific commit or run on hosts without internet access.

---

## Overview

The tool automatically detects SQLite database files and replicates them to PostgreSQL using a JSONB storage model. All SQLite data is preserved with full type fidelity, including BLOBs, NULLs, and special float values.

**Key Features:**
- **Zero Configuration**: Automatic source type detection from file extension
- **Type Preservation**: Lossless conversion of all SQLite types to JSONB
- **Security First**: Read-only connections, path validation, SQL injection prevention
- **BLOB Support**: Binary data encoded as base64 in JSONB
- **Metadata Tracking**: Each row includes source type and replication timestamp

## Quick Start

### Basic Replication

Replicate an entire SQLite database to PostgreSQL:

```bash
database-replicator init -y \
  --source /path/to/database.db \
  --target "postgresql://user:pass@target-host:5432/db"
```

> **Note:** The `-y` flag skips interactive mode, which is required for SQLite sources.

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
- `_source_type`: Always `'sqlite'` for SQLite replications
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
- Safe for production database replication

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
database-replicator init -y \
  --source ./sample.db \
  --target "postgresql://localhost:5432/mydb"
```

### Example 2: Query Migrated Data

After replication, query the data:

```sql
-- Connect to PostgreSQL
psql "postgresql://localhost:5432/mydb"

-- List replicated tables
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
- Large databases replicate faster than single-row inserts
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

SQLite replications are **snapshot-only**:
- Data replicated at a point in time
- No continuous replication like PostgreSQL logical replication
- Re-running `init` will recreate tables (drop and recreate)

**Workaround for Ongoing Sync:**
- Use SQLite's backup API to create periodic snapshots
- Re-run replication with updated snapshot
- Consider PostgreSQL FDW for live SQLite querying

### No Schema Preservation

SQLite table schemas are not preserved:
- All data stored as JSONB regardless of original column types
- Column constraints (UNIQUE, CHECK, etc.) not enforced
- Foreign key relationships not maintained

**Workaround:**
- Create views to expose typed columns
- Add constraints manually after replication
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
- Rename tables in SQLite before replication if possible

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
   - Disable autovacuum during replication: `ALTER TABLE foo SET (autovacuum_enabled = false);`

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

   # Test replication
   database-replicator init -y --source test.db --target "postgresql://localhost/test"
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

### Can I replicate the same database twice?

Re-running `init` will **clear** previously migrated tables (via truncate by default or a full drop when `--drop-existing` is supplied). All data will be replaced with fresh data from SQLite. This is useful for:
- Correcting errors in the first migration
- Refreshing data from an updated SQLite snapshot

### How do I handle incremental updates?

SQLite replications are snapshot-only. For incremental updates:

1. **Option 1**: Periodic full re-migration
   - Create SQLite backup/snapshot
   - Re-run `init` (add `--drop-existing` to drop/recreate tables instead of truncate)

2. **Option 2**: Track changes in application
   - Maintain updated_at timestamps
   - Query for changed records: `SELECT * FROM users WHERE updated_at > ?`
   - Upsert changed records manually

3. **Option 3**: Migrate to PostgreSQL entirely
   - After initial migration, move application to PostgreSQL
   - Retire SQLite database

### Is the source database locked during replication?

No. The tool uses `SQLITE_OPEN_READ_ONLY` which allows concurrent readers. Other processes can read the database during replication.

## Additional Resources

- [Main README](README.md) - PostgreSQL-to-PostgreSQL migration
- [CHANGELOG](CHANGELOG.md) - Version history
- [GitHub Issues](https://github.com/serenorg/database-replicator/issues) - Report bugs or request features

## Support

For issues or questions:
- **GitHub Issues**: https://github.com/serenorg/database-replicator/issues
- **Email**: support@seren.ai
<<<<<<< HEAD:README-SQLite.md
## Delta replication with sqlite-watcher

Once you have completed the initial snapshot (`database-replicator init --source sqlite ...`), you can switch to incremental change capture:

1. Install `sqlite-watcher` (see [sqlite-watcher-docs/installers.md](installers.md) for Linux systemd units, macOS launchd plists, and Windows service guidance).
2. Start the watcher beside your `.sqlite` file (example for Linux/macOS):

   ```bash
   sqlite-watcher serve \
     --queue-db ~/.seren/sqlite-watcher/changes.db \
     --listen unix:/tmp/sqlite-watcher.sock \
     --token-file ~/.seren/sqlite-watcher/token
   ```

3. Consume the change feed with the new command:

   ```bash
   database-replicator sync-sqlite \
     --target "postgresql://user:pass@your-serendb.serendb.com:5432/app" \
     --watcher-endpoint unix:/tmp/sqlite-watcher.sock \
     --token-file ~/.seren/sqlite-watcher/token \
     --incremental-mode append
   ```

   Use `--incremental-mode append_deduped` to maintain `_latest` tables (one row per primary key) in addition to the append-only history.

4. Verify the smoke test if you have Docker available:

   ```bash
   scripts/test-sqlite-delta.sh
   ```

   The script spins up a temporary Postgres container, runs `sqlite-watcher`, and executes `database-replicator sync-sqlite` against the watcher feed. Windows users can follow the same steps manually (the script prints the equivalent commands at the end).
