# MySQL/MariaDB Replication Guide

This guide explains how to replicate MySQL and MariaDB databases to PostgreSQL using postgres-seren-replicator's JSONB storage approach.

## Overview

The tool automatically detects MySQL/MariaDB connection strings and replicates data to PostgreSQL using a JSONB storage model. All MySQL data is preserved with full type fidelity, including DECIMAL precision, DATETIME microseconds, and BLOB data.

**Key Features:**
- **Zero Configuration**: Automatic source type detection from connection string
- **Type Preservation**: Lossless conversion of all MySQL types to JSONB
- **Security First**: Connection validation, SQL injection prevention, read-only access
- **DECIMAL Precision**: Stored as strings to preserve exact decimal values
- **DATETIME Microseconds**: Full microsecond precision preserved
- **MariaDB Compatible**: Works with both MySQL and MariaDB databases

## Why Migrate MySQL to PostgreSQL?

- **Modern SQL Features**: PostgreSQL offers advanced features like CTEs, window functions, and better JSON support
- **Better Performance**: Superior query optimization and indexing capabilities
- **Open Source**: Truly open-source without commercial pressures
- **Cloud Migration**: Easy path to cloud-native PostgreSQL services like Seren Cloud
- **Data Analysis**: Better integration with analytics tools and BI platforms

## Prerequisites

- **MySQL/MariaDB Database**: Version 5.7+ (MySQL) or 10.2+ (MariaDB)
- **Connection Access**: Valid MySQL connection string with read permissions
- **PostgreSQL Target**: Version 12+ with credentials for data writing
- **Tool Installation**: postgres-seren-replicator binary installed

## Quick Start

### One-Time Replication

Replicate an entire MySQL database to PostgreSQL:

```bash
postgres-seren-replicator init \
  --source "mysql://user:password@mysql-host:3306/mydb" \
  --target "postgresql://user:password@pg-host:5432/db"
```

The tool automatically:
1. Detects that the source is MySQL (from `mysql://` prefix)
2. Validates the connection string for security
3. Connects to MySQL database
4. Lists all tables
5. Creates corresponding JSONB tables in PostgreSQL
6. Converts and inserts all data

### MariaDB Databases

MariaDB works identically - just use the same connection string format:

```bash
postgres-seren-replicator init \
  --source "mysql://user:password@mariadb-host:3306/mydb" \
  --target "postgresql://user:password@pg-host:5432/db"
```

## Step-by-Step Guide

### 1. Get Your MySQL Connection String

Format: `mysql://username:password@hostname:port/database`

Example:
```
mysql://admin:secretpass@db.example.com:3306/production
```

**Common Scenarios:**

| Setup | Connection String |
|-------|-------------------|
| Local MySQL | `mysql://root:password@localhost:3306/mydb` |
| AWS RDS MySQL | `mysql://admin:pass@mydb.abc123.us-east-1.rds.amazonaws.com:3306/db` |
| Azure MySQL | `mysql://admin@server:pass@server.mysql.database.azure.com:3306/db` |
| GCP Cloud SQL | `mysql://root:pass@34.123.45.67:3306/mydb` |
| MariaDB | `mysql://user:pass@mariadb-host:3306/db` |

### 2. Get Your PostgreSQL Connection String

Format: `postgresql://username:password@hostname:port/database`

Example:
```
postgresql://user:pass@seren.cloud:5432/target_db
```

### 3. Run the Replication

```bash
postgres-seren-replicator init \
  --source "mysql://SOURCE_CONNECTION" \
  --target "postgresql://TARGET_CONNECTION"
```

### 4. Verify the Data

Connect to PostgreSQL and query the replicated data:

```sql
-- List replicated tables
\dt

-- View data from a table
SELECT * FROM users LIMIT 5;

-- Query specific fields from JSONB
SELECT
  id,
  data->>'name' as name,
  data->>'email' as email
FROM users;
```

## Data Format

### JSONB Storage Model

Each MySQL table is converted to a PostgreSQL table with this structure:

```sql
CREATE TABLE IF NOT EXISTS "table_name" (
    id TEXT PRIMARY KEY,
    data JSONB NOT NULL,
    _source_type TEXT NOT NULL DEFAULT 'mysql',
    _replicated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Performance indexes
CREATE INDEX IF NOT EXISTS "idx_table_name_data" ON "table_name" USING GIN (data);
CREATE INDEX IF NOT EXISTS "idx_table_name_source" ON "table_name" (_source_type);
```

**Field Descriptions:**
- `id`: String ID from MySQL's primary key or auto-generated row number
- `data`: JSONB containing all column data from the MySQL row
- `_source_type`: Always `'mysql'` for MySQL replications
- `_replicated_at`: Timestamp of when the row was replicated

### Example Data

**Original MySQL Table:**
```sql
CREATE TABLE users (
    id INT PRIMARY KEY,
    name VARCHAR(255),
    email VARCHAR(255),
    age INT,
    balance DECIMAL(10, 2),
    created_at DATETIME(6)
);

INSERT INTO users VALUES
    (1, 'Alice', 'alice@example.com', 30, 100.50, '2024-01-15 10:30:45.123456');
```

**Replicated PostgreSQL Data:**
```json
{
  "id": "1",
  "data": {
    "id": 1,
    "name": "Alice",
    "email": "alice@example.com",
    "age": 30,
    "balance": "100.50",
    "created_at": {
      "_type": "datetime",
      "value": "2024-01-15T10:30:45.123456Z"
    }
  },
  "_source_type": "mysql",
  "_replicated_at": "2024-01-15T10:35:00.000000+00:00"
}
```

## MySQL Type Conversions

All MySQL types are converted to JSONB with full fidelity:

| MySQL Type | JSON Type | Example Input | JSON Output |
|------------|-----------|---------------|-------------|
| INT, BIGINT | number | `42` | `42` |
| DECIMAL(10,2) | string | `123.45` | `"123.45"` |
| FLOAT, DOUBLE | number | `3.14` | `3.14` |
| VARCHAR, TEXT | string | `"Hello"` | `"Hello"` |
| DATETIME | object | `2024-01-15 10:30:45` | `{"_type":"datetime","value":"2024-01-15T10:30:45.000000Z"}` |
| DATE | object | `2024-01-15` | `{"_type":"datetime","value":"2024-01-15T00:00:00.000000Z"}` |
| TIME | object | `10:30:45` | `{"_type":"time","value":"0d 10:30:45.000000"}` |
| BLOB, BINARY | object | Binary data | `{"_type":"binary","data":"<base64>"}` |
| NULL | null | `NULL` | `null` |
| TINYINT(1) | number | `1` | `1` |
| ENUM | string | `'active'` | `"active"` |
| SET | string | `'read,write'` | `"read,write"` |

### Special Cases

**DECIMAL Precision:**
- Stored as strings to preserve exact decimal values
- Prevents floating-point precision loss
- Convert to numeric when querying: `(data->>'balance')::numeric`

**DATETIME Microseconds:**
- Full microsecond precision preserved
- Stored in ISO 8601 format with timezone
- Example: `2024-01-15T10:30:45.123456Z`

**BLOB and Binary Data:**
- Encoded as base64 in a structured object
- Format: `{"_type": "binary", "data": "<base64>"}`
- Allows distinguishing binary data from text

**Non-Finite Floats:**
- `NaN`, `Infinity`, and `-Infinity` are converted to strings
- Example: `NaN` → `"NaN"`

**TIME with Negative Values:**
- MySQL TIME can be negative (for time intervals)
- Preserved in format: `-1d 10:30:45.000000`

## Querying JSONB Data

### Basic Queries

Access replicated MySQL data using PostgreSQL's JSONB operators:

```sql
-- Get all users
SELECT id, data FROM users;

-- Get specific fields
SELECT
  id,
  data->>'name' as name,
  data->>'email' as email,
  data->>'age' as age
FROM users;

-- Cast to appropriate types
SELECT
  data->>'name' as name,
  (data->>'age')::int as age,
  (data->>'balance')::numeric as balance
FROM users;
```

### Filtering Data

```sql
-- Filter by string field
SELECT * FROM users
WHERE data->>'name' = 'Alice';

-- Filter by numeric field
SELECT * FROM users
WHERE (data->>'age')::int > 25;

-- Filter by DECIMAL
SELECT * FROM users
WHERE (data->>'balance')::numeric > 100.00;

-- NULL checks
SELECT * FROM users
WHERE data->>'email' IS NOT NULL;

-- LIKE queries
SELECT * FROM users
WHERE data->>'email' LIKE '%@example.com';
```

### Datetime Queries

DATETIME fields require extracting the `value` from the nested object:

```sql
-- Extract datetime value
SELECT
  data->>'name' as name,
  (data->'created_at'->>'value')::timestamp as created_at
FROM users;

-- Filter by datetime
SELECT * FROM users
WHERE (data->'created_at'->>'value')::timestamp > NOW() - INTERVAL '7 days';

-- Date arithmetic
SELECT
  data->>'name' as name,
  AGE(NOW(), (data->'created_at'->>'value')::timestamp) as account_age
FROM users;
```

### Working with Binary Data

```sql
-- Check if field contains binary data
SELECT
  id,
  data->>'name' as name,
  CASE
    WHEN data->'avatar'->>'_type' = 'binary' THEN 'Has avatar'
    ELSE 'No avatar'
  END as avatar_status
FROM users;

-- Decode base64 binary data (if needed)
SELECT
  id,
  decode(data->'avatar'->>'data', 'base64') as avatar_binary
FROM users
WHERE data->'avatar'->>'_type' = 'binary';
```

### Performance Indexes

Create indexes on frequently queried fields:

```sql
-- Index on email field
CREATE INDEX idx_users_email ON users ((data->>'email'));

-- Index on age (cast to int)
CREATE INDEX idx_users_age ON users (((data->>'age')::int));

-- Index on balance (cast to numeric)
CREATE INDEX idx_users_balance ON users (((data->>'balance')::numeric));

-- Index on datetime (extract and cast)
CREATE INDEX idx_users_created ON users (((data->'created_at'->>'value')::timestamp));

-- Partial index (only active users)
CREATE INDEX idx_active_users ON users ((data->>'status'))
WHERE data->>'status' = 'active';
```

### Aggregations

```sql
-- Count users
SELECT COUNT(*) FROM users;

-- Average age
SELECT AVG((data->>'age')::int) as avg_age FROM users;

-- Sum of balances (handle DECIMAL)
SELECT SUM((data->>'balance')::numeric) as total_balance FROM users;

-- Group by status
SELECT
  data->>'status' as status,
  COUNT(*) as count
FROM users
GROUP BY data->>'status';

-- Top users by balance
SELECT
  data->>'name' as name,
  (data->>'balance')::numeric as balance
FROM users
ORDER BY (data->>'balance')::numeric DESC
LIMIT 10;
```

## Remote Execution

Run replication jobs on SerenAI-managed cloud infrastructure:

```bash
postgres-seren-replicator init \
  --source "mysql://SOURCE_CONNECTION" \
  --target "postgresql://TARGET_CONNECTION" \
  --remote
```

**Benefits:**
- No local resources consumed
- Automatic retry and error handling
- Logs stored in CloudWatch
- Job monitoring via API
- Managed security and credentials

**Monitoring Jobs:**

```bash
# Check job status
postgres-seren-replicator status \
  --job-id <job-id>

# View job logs
postgres-seren-replicator logs \
  --job-id <job-id>
```

## Common Issues

### Connection Refused

**Symptom:** `Error: Failed to connect to MySQL: Connection refused`

**Solutions:**
1. Verify MySQL is running: `mysql -h hostname -u username -p`
2. Check firewall rules allow connections from your IP
3. Verify hostname and port are correct
4. Check MySQL is listening on the correct interface:
   ```sql
   SHOW VARIABLES LIKE 'bind_address';
   ```

### Authentication Failures

**Symptom:** `Error: Access denied for user 'username'@'host'`

**Solutions:**
1. Verify username and password are correct
2. Check user has required permissions:
   ```sql
   SHOW GRANTS FOR 'username'@'%';
   ```
3. For caching_sha2_password plugin issues, use mysql_native_password:
   ```sql
   ALTER USER 'username'@'%' IDENTIFIED WITH mysql_native_password BY 'password';
   FLUSH PRIVILEGES;
   ```

### Timeout Errors

**Symptom:** `Error: Connection timeout` or operations hang

**Solutions:**
1. Increase `connect_timeout` in MySQL configuration
2. Check network latency between source and tool
3. For AWS RDS, verify security group allows connections
4. Increase ELB idle timeout if using load balancer

### Large Tables

**Symptom:** Replication is slow or runs out of memory

**Solutions:**
1. Monitor progress - the tool reports per-table progress
2. Large tables may take hours - this is normal
3. Consider increasing target database resources
4. Split very large tables by filtering (requires manual SQL)

### Character Encoding Issues

**Symptom:** Special characters appear corrupted

**Solutions:**
1. Verify MySQL charset: `SHOW VARIABLES LIKE 'character_set%';`
2. Ensure connection uses UTF-8:
   ```bash
   mysql --default-character-set=utf8mb4 ...
   ```
3. PostgreSQL target should use UTF8 encoding
4. Check collation settings match data expectations

### Time Zone Differences

**Symptom:** Datetime values are offset by hours

**Solutions:**
1. MySQL DATETIME is timezone-naive, stored as-is
2. Tool converts to ISO 8601 with 'Z' (UTC) suffix
3. If your MySQL data has implicit timezone, note it during queries:
   ```sql
   -- Interpret as specific timezone
   SELECT (data->'created_at'->>'value')::timestamp AT TIME ZONE 'America/New_York';
   ```

### MariaDB-Specific Issues

**Symptom:** Connection fails with MariaDB

**Solutions:**
1. Use `mysql://` prefix (not `mariadb://`)
2. Verify MariaDB version is 10.2+
3. Check authentication plugin compatibility
4. MariaDB-specific data types convert to MySQL equivalents

## Limitations

**Current Limitations:**
- **One-Time Replication Only**: No continuous sync (unlike PostgreSQL→PostgreSQL)
- **No Incremental Updates**: Full table refresh on each run
- **Schema-Only Migration Not Supported**: Data is always replicated to JSONB format
- **Foreign Keys Not Preserved**: Relationships must be reconstructed in PostgreSQL
- **Indexes Not Migrated**: Create PostgreSQL indexes manually after replication
- **Triggers and Stored Procedures**: Not migrated (PostgreSQL equivalents needed)
- **Views**: Not replicated (recreate using JSONB queries)
- **Auto-Increment State**: Not preserved (PostgreSQL sequences start from 1)

**Design Tradeoffs:**
- **JSONB Storage**: Flexible but requires JSONB operators for querying
- **No Schema Mapping**: Simpler tool, but schema must be manually designed for production use
- **Type Safety**: Cast JSONB fields to appropriate types when querying

## FAQ

**Q: Can I replicate the same MySQL database multiple times?**
A: Yes, data is replaced on each run. You can re-run replication to refresh data.

**Q: Does this work with MariaDB?**
A: Yes, MariaDB is fully compatible. Use the same `mysql://` connection string format.

**Q: Are MySQL indexes migrated?**
A: No, indexes are not preserved. Create PostgreSQL indexes on JSONB fields after replication for performance.

**Q: What about very large tables (100+ million rows)?**
A: The tool handles large tables, but replication time is proportional to data size. Monitor progress through logged messages.

**Q: Can I replicate specific tables only?**
A: Not currently supported. The tool replicates all tables in the specified database.

**Q: How do I handle DECIMAL precision in queries?**
A: DECIMAL values are stored as strings. Cast to numeric when querying:
```sql
SELECT (data->>'balance')::numeric as balance FROM users;
```

**Q: Are MySQL stored procedures and triggers replicated?**
A: No, only table data is replicated. Procedures and triggers must be rewritten for PostgreSQL.

**Q: What happens if replication fails midway?**
A: Tables are created and partially populated. Re-running the command will replace data in existing tables.

**Q: Can I run this in production without downtime?**
A: Yes, the tool uses read-only connections and doesn't lock MySQL tables. Applications can continue running during replication.

**Q: How do I convert this to a proper PostgreSQL schema after replication?**
A: Once data is in JSONB format, write SQL to extract fields into typed columns:
```sql
CREATE TABLE users_normalized AS
SELECT
  id,
  data->>'name' as name,
  (data->>'age')::int as age,
  (data->>'balance')::numeric as balance,
  (data->'created_at'->>'value')::timestamp as created_at
FROM users;
```

**Q: What about MySQL-specific features like spatial types?**
A: Spatial types (GEOMETRY, POINT, etc.) are converted to their string representations. Spatial functionality must be recreated in PostgreSQL using PostGIS.

**Q: Does this support MySQL replication (master-slave)?**
A: No, this tool reads directly from the specified MySQL server. It doesn't integrate with MySQL's native replication system.

## Examples

### Example 1: Local MySQL to Cloud PostgreSQL

```bash
postgres-seren-replicator init \
  --source "mysql://root:password@localhost:3306/ecommerce" \
  --target "postgresql://user:pass@seren.cloud:5432/ecommerce"
```

### Example 2: AWS RDS MySQL to PostgreSQL

```bash
postgres-seren-replicator init \
  --source "mysql://admin:pass@mydb.abc123.us-east-1.rds.amazonaws.com:3306/production" \
  --target "postgresql://user:pass@target-host:5432/production"
```

### Example 3: MariaDB Migration

```bash
postgres-seren-replicator init \
  --source "mysql://maria_user:pass@mariadb.example.com:3306/mydb" \
  --target "postgresql://user:pass@pg-host:5432/mydb"
```

### Example 4: Remote Execution

```bash
export SEREN_API_KEY="your-api-key"

postgres-seren-replicator init \
  --source "mysql://user:pass@mysql-host:3306/db" \
  --target "postgresql://user:pass@pg-host:5432/db" \
  --remote
```

### Example 5: Querying After Replication

```sql
-- Connect to PostgreSQL
psql "postgresql://user:pass@pg-host:5432/db"

-- List replicated tables
\dt

-- View sample data
SELECT * FROM users LIMIT 5;

-- Query with type casting
SELECT
  data->>'name' as name,
  (data->>'age')::int as age,
  (data->>'balance')::numeric as balance,
  (data->'created_at'->>'value')::timestamp as created_at
FROM users
WHERE (data->>'age')::int > 25
ORDER BY (data->>'balance')::numeric DESC;

-- Create index for performance
CREATE INDEX idx_users_email ON users ((data->>'email'));

-- Test index usage
EXPLAIN ANALYZE
SELECT * FROM users WHERE data->>'email' = 'alice@example.com';
```

## Next Steps

After replicating your MySQL data:

1. **Verify Data**: Query tables to ensure data was replicated correctly
2. **Create Indexes**: Add indexes on frequently queried JSONB fields
3. **Design Schema**: If needed, create normalized tables from JSONB data
4. **Update Applications**: Modify application code to query PostgreSQL
5. **Test Performance**: Benchmark queries and add indexes as needed
6. **Monitor Usage**: Track query performance and optimize JSONB access patterns

## Support

For issues, questions, or feature requests:
- **GitHub Issues**: [postgres-seren-replicator/issues](https://github.com/serenorg/postgres-seren-replicator/issues)
- **Documentation**: See main [README.md](README.md) for general tool documentation
- **Security Issues**: Report privately to security@seren.ai

## License

This tool is part of the postgres-seren-replicator project. See [LICENSE](LICENSE) for details.
