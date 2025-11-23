# MongoDB Replication Guide

This guide explains how to replicate MongoDB databases to PostgreSQL using postgres-seren-replicator's JSONB storage approach.

## Overview

The tool automatically detects MongoDB connection strings and replicates collections to PostgreSQL using a JSONB storage model. All BSON types are preserved with full type fidelity, including ObjectIds, DateTimes, Binary data, and MongoDB-specific types.

**Key Features:**
- **Zero Configuration**: Automatic source type detection from connection URL
- **Type Preservation**: Lossless conversion of all BSON types to JSONB
- **Security First**: Read-only connections, URL validation, NoSQL injection prevention
- **MongoDB Types**: Full support for ObjectId, DateTime, Binary, Decimal128, and more
- **Metadata Tracking**: Each document includes source type and replication timestamp

## Quick Start

### Basic Replication

Replicate an entire MongoDB database to PostgreSQL:

```bash
postgres-seren-replicator init \
  --source "mongodb://localhost:27017/mydb" \
  --target "postgresql://user:pass@target-host:5432/db"
```

The tool automatically:
1. Detects that the source is MongoDB (from `mongodb://` URL)
2. Validates the connection string for security
3. Connects to MongoDB and verifies connection
4. Extracts the database name from the URL
5. Lists all collections (excluding system collections)
6. Creates corresponding JSONB tables in PostgreSQL
7. Converts and inserts all documents

### Supported Connection Strings

The tool recognizes these MongoDB URL formats:
- `mongodb://localhost:27017/mydb` - Standard MongoDB connection
- `mongodb://user:pass@localhost:27017/mydb` - With authentication
- `mongodb://host1:27017,host2:27017/mydb?replicaSet=rs0` - Replica sets
- `mongodb+srv://cluster.mongodb.net/mydb` - MongoDB Atlas (SRV)

**Important**: The database name must be included in the connection URL (e.g., `/mydb`).

## Data Type Mapping

MongoDB BSON types are mapped to JSONB as follows:

### Primitive Types

| BSON Type | JSON Type | Example Input | JSON Output |
|-----------|-----------|---------------|-------------|
| String | string | `"Hello"` | `"Hello"` |
| Int32 | number | `42` | `42` |
| Int64 | number | `9223372036854775807` | `9223372036854775807` |
| Double | number | `3.14159` | `3.14159` |
| Boolean | boolean | `true` | `true` |
| Null | null | `null` | `null` |
| Undefined | null | `undefined` | `null` |

### MongoDB-Specific Types

| BSON Type | JSON Representation | Example |
|-----------|---------------------|---------|
| ObjectId | Object with `_type` and `$oid` | `{"_type": "objectid", "$oid": "507f1f77bcf86cd799439011"}` |
| DateTime | Object with `_type` and `$date` (milliseconds) | `{"_type": "datetime", "$date": 1678886400000}` |
| Binary | Object with `_type`, `subtype`, and base64 `data` | `{"_type": "binary", "subtype": 0, "data": "SGVsbG8="}` |
| Decimal128 | String (preserves precision) | `"123.456789012345"` |
| RegularExpression | Object with `_type`, `pattern`, and `options` | `{"_type": "regex", "pattern": "^test", "options": "i"}` |
| Timestamp | Object with `_type`, `t` (time), and `i` (increment) | `{"_type": "timestamp", "t": 1234567890, "i": 1}` |
| MaxKey | Object with `_type` | `{"_type": "maxkey"}` |
| MinKey | Object with `_type` | `{"_type": "minkey"}` |

### Special Cases

**Non-Finite Doubles:**
- `NaN`, `Infinity`, and `-Infinity` are converted to strings for JSON compatibility
- Example: `NaN` → `"NaN"`

**Arrays:**
- MongoDB arrays are preserved as JSON arrays
- Nested arrays are fully supported
- Example: `[1, 2, [3, 4]]` → `[1, 2, [3, 4]]`

**Embedded Documents:**
- Nested documents are preserved as JSON objects
- Full depth nesting is supported
- Example: `{user: {name: "Alice"}}` → `{"user": {"name": "Alice"}}`

**ObjectId Handling:**
- Always converted to hex string in `$oid` field
- Preserves exact ObjectId value for round-trip compatibility
- Example: `ObjectId("507f1f77bcf86cd799439011")` → `{"_type": "objectid", "$oid": "507f1f77bcf86cd799439011"}`

## PostgreSQL Table Structure

Each MongoDB collection is converted to a PostgreSQL table with this schema:

```sql
CREATE TABLE IF NOT EXISTS "collection_name" (
    id TEXT PRIMARY KEY,
    data JSONB NOT NULL,
    _source_type TEXT NOT NULL DEFAULT 'mongodb',
    _migrated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Performance indexes
CREATE INDEX IF NOT EXISTS "idx_collection_name_data" ON "collection_name" USING GIN (data);
CREATE INDEX IF NOT EXISTS "idx_collection_name_source" ON "collection_name" (_source_type);
```

**Field Descriptions:**
- `id`: String ID from MongoDB's `_id` field (ObjectId converted to hex, or string/number as-is)
- `data`: JSONB containing the complete MongoDB document
- `_source_type`: Always `'mongodb'` for MongoDB replications
- `_migrated_at`: Timestamp of when the document was replicated

## Querying Replicated Data

### Basic Queries

Query JSONB data using PostgreSQL's JSONB operators:

```sql
-- Get all documents
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

### Working with MongoDB ObjectIds

```sql
-- Extract ObjectId as hex string
SELECT
    id,
    data->'_id'->>'$oid' AS objectid_hex,
    data->>'name' AS name
FROM users;

-- Find by ObjectId
SELECT * FROM users
WHERE data->'_id'->>'$oid' = '507f1f77bcf86cd799439011';

-- Check for ObjectId type
SELECT * FROM users
WHERE data->'_id'->>'_type' = 'objectid';
```

### Working with Dates

```sql
-- Extract DateTime as timestamp
SELECT
    id,
    to_timestamp((data->'created_at'->>'$date')::bigint / 1000) AS created_at,
    data->>'name' AS name
FROM users;

-- Filter by date range
SELECT * FROM events
WHERE to_timestamp((data->'timestamp'->>'$date')::bigint / 1000)
    BETWEEN '2024-01-01' AND '2024-12-31';

-- Group by date
SELECT
    DATE_TRUNC('day', to_timestamp((data->'created_at'->>'$date')::bigint / 1000)) AS date,
    COUNT(*) AS count
FROM events
GROUP BY date
ORDER BY date;
```

### Working with Binary Data

```sql
-- Check if a field is Binary
SELECT id, data->'avatar'->>'_type' AS type
FROM users
WHERE data->'avatar'->>'_type' = 'binary';

-- Decode Binary data
SELECT
    id,
    data->>'name' AS name,
    decode(data->'avatar'->>'data', 'base64') AS avatar_bytes
FROM users
WHERE data->'avatar'->>'_type' = 'binary';

-- Get Binary subtype
SELECT
    id,
    (data->'avatar'->>'subtype')::int AS binary_subtype
FROM users
WHERE data->'avatar'->>'_type' = 'binary';
```

### Working with Nested Documents

```sql
-- Query nested fields
SELECT * FROM users
WHERE data->'address'->>'city' = 'New York';

-- Extract nested data
SELECT
    id,
    data->>'name' AS name,
    data->'address'->>'street' AS street,
    data->'address'->>'city' AS city,
    data->'address'->>'zip' AS zip
FROM users;

-- Query deeply nested fields
SELECT * FROM orders
WHERE data->'payment'->'card'->>'type' = 'visa';
```

### Working with Arrays

```sql
-- Check if array contains value
SELECT * FROM products
WHERE data->'tags' ? 'electronics';

-- Check if array contains any of these values
SELECT * FROM products
WHERE data->'tags' ?| array['electronics', 'computers'];

-- Check if array contains all of these values
SELECT * FROM products
WHERE data->'tags' ?& array['electronics', 'sale'];

-- Get array length
SELECT
    id,
    data->>'name' AS name,
    jsonb_array_length(data->'tags') AS tag_count
FROM products;

-- Expand array to rows
SELECT
    id,
    data->>'name' AS product_name,
    tag
FROM products,
    jsonb_array_elements_text(data->'tags') AS tag;
```

### Advanced Queries

```sql
-- Full-text search
SELECT * FROM articles
WHERE to_tsvector('english', data->>'content') @@ to_tsquery('mongodb & postgresql');

-- Aggregations with nested fields
SELECT
    data->'metadata'->>'category' AS category,
    COUNT(*) AS count,
    AVG((data->>'price')::numeric) AS avg_price,
    SUM((data->>'quantity')::int) AS total_quantity
FROM products
GROUP BY data->'metadata'->>'category';

-- Complex filtering
SELECT * FROM users
WHERE (data->>'age')::int BETWEEN 25 AND 35
    AND data->'address'->>'country' = 'USA'
    AND data->'tags' ? 'premium';

-- Join across collections
SELECT
    u.data->>'name' AS user_name,
    o.data->>'total' AS order_total,
    to_timestamp((o.data->'created_at'->>'$date')::bigint / 1000) AS order_date
FROM users u
JOIN orders o ON u.id = (o.data->>'user_id');
```

## Security Features

### Connection String Validation

The tool validates MongoDB connection strings before use:

**Checks Performed:**
- URL must start with `mongodb://` or `mongodb+srv://`
- Connection string cannot be empty
- Format is validated by MongoDB driver
- Ping test confirms server accessibility

**Rejected Patterns:**
- Wrong protocols (postgresql://, mysql://, http://)
- Missing protocol prefix
- Malformed URLs

### Collection Name Validation

Collection names are validated to prevent NoSQL injection:

**Allowed:**
- Alphanumeric characters (a-z, A-Z, 0-9)
- Underscores (_)
- Names starting with letters or underscores

**Rejected:**
- SQL keywords (SELECT, DROP, etc.)
- Special characters ($, ., ;, etc.)
- Shell metacharacters
- System collection names (system.*)

### Read-Only Operations

**Guarantees:**
- All MongoDB operations are read-only queries
- No insert, update, or delete operations are performed
- No administrative commands are executed
- Collections are listed and read only

### Credential Protection

**Security Measures:**
- Credentials in URLs are never logged
- Error messages sanitize connection strings
- Passwords are not exposed in stack traces
- Connection validation doesn't leak credentials

## Performance Considerations

### Index Strategy

The tool automatically creates these indexes on each replicated table:

```sql
-- GIN index for efficient JSONB queries
CREATE INDEX IF NOT EXISTS "idx_collection_name_data" ON "collection_name" USING GIN (data);

-- Source type filter index
CREATE INDEX IF NOT EXISTS "idx_collection_name_source" ON "collection_name" (_source_type);
```

For optimal query performance, create additional indexes based on your access patterns:

```sql
-- Index on frequently queried fields
CREATE INDEX idx_users_email ON users ((data->>'email'));
CREATE INDEX idx_users_age ON users (((data->>'age')::int));
CREATE INDEX idx_orders_user_id ON orders ((data->>'user_id'));

-- Index on nested fields
CREATE INDEX idx_users_city ON users ((data->'address'->>'city'));

-- Index for date range queries
CREATE INDEX idx_events_timestamp ON events (to_timestamp((data->'timestamp'->>'$date')::bigint / 1000));
```

### Batch Insert Performance

**Default Behavior:**
- Documents are inserted in batches of 1000
- Each batch is a single transaction
- Progress is logged per collection

**Tips for Large Collections:**
- Replication time scales linearly with document count
- Network bandwidth is typically the bottleneck
- Target PostgreSQL should have sufficient disk space

### Query Optimization

**Best Practices:**
- Always cast JSONB values to appropriate types for comparisons
- Use `->` for intermediate navigation, `->>` only for final text extraction
- Create indexes on frequently queried fields
- Use GIN indexes for containment queries (`?`, `?|`, `?&`)
- Consider materialized views for complex aggregations

**Example Optimization:**

```sql
-- Slow: No index, string comparison
SELECT * FROM users WHERE data->>'age' = '25';

-- Better: Cast to int, but still no index
SELECT * FROM users WHERE (data->>'age')::int = 25;

-- Best: Create index, then query
CREATE INDEX idx_users_age ON users (((data->>'age')::int));
SELECT * FROM users WHERE (data->>'age')::int = 25;
```

## Troubleshooting

### Connection Issues

**Error**: `Failed to connect to MongoDB server`

**Causes:**
- MongoDB server is not running
- Network connectivity issues
- Incorrect hostname or port
- Firewall blocking connection

**Solutions:**
```bash
# Test connectivity
mongo "mongodb://localhost:27017/mydb" --eval "db.version()"

# Check MongoDB is running
systemctl status mongod  # Linux
brew services list | grep mongodb  # macOS

# Verify connection string format
# Must include database name: mongodb://host:port/dbname
```

### Database Name Missing

**Error**: `MongoDB URL must include database name`

**Cause**: Connection URL doesn't specify which database to replicate

**Solution**: Add database name to URL:
```bash
# Wrong
mongodb://localhost:27017

# Correct
mongodb://localhost:27017/mydb
```

### Empty Collection Results

**Symptom**: Replication succeeds but some collections appear empty

**Causes:**
- Collection was actually empty in MongoDB
- System collections were filtered out
- Connection timeout during large collection read

**Verification:**
```javascript
// In mongo shell
use mydb
db.collectionName.countDocuments()
```

### System Collections Not Replicated

**Behavior**: Collections starting with `system.` are not replicated

**Reason**: System collections are internal MongoDB metadata and are intentionally excluded

**Collections Filtered:**
- `system.indexes`
- `system.users`
- `system.profile`
- All other `system.*` collections

### Type Conversion Warnings

**Warning**: `Document X has unsupported _id type, using doc number`

**Cause**: Document has an `_id` field with a type that's not ObjectId, String, Int32, or Int64

**Impact**: Document will get a generated ID instead of using the original `_id`

**Resolution**: This is safe - the original `_id` is preserved in the `data` JSONB field

## Best Practices

### Before Replication

**Planning:**
1. **Analyze Source Data**:
   ```javascript
   // In mongo shell
   db.stats()  // Database statistics
   db.collectionName.stats()  // Per-collection stats
   db.collectionName.find().limit(10)  // Sample data
   ```

2. **Estimate Target Size**:
   - JSONB storage is ~1.5-2x larger than BSON
   - Plan for additional space for indexes
   - Account for metadata fields (_source_type, _migrated_at)

3. **Check Disk Space**:
   ```sql
   -- On target PostgreSQL
   SELECT pg_size_pretty(pg_database_size('your_db'));
   ```

4. **Review Connection Credentials**:
   - Ensure MongoDB user has read access
   - Verify PostgreSQL user can create tables
   - Test connections before full replication

### During Replication

**Monitoring:**
1. **Watch Progress**: Replication logs show collection-by-collection progress
2. **Monitor Resources**: Check CPU, memory, and network on both systems
3. **Verify Data**: Spot-check replicated documents

**For Large Databases:**
- Run during off-peak hours if possible
- Monitor MongoDB server load
- Ensure stable network connection
- Consider replicating collections individually if needed

### After Replication

**Verification:**
```sql
-- Check row counts
SELECT COUNT(*) FROM collection_name;

-- Verify recent replications
SELECT MAX(_migrated_at) FROM collection_name;

-- Check for data integrity
SELECT id, data FROM collection_name LIMIT 10;

-- Verify all expected collections replicated
SELECT table_name FROM information_schema.tables
WHERE table_schema = 'public';
```

**Optimization:**
```sql
-- Analyze tables for query planning
ANALYZE collection_name;

-- Create application-specific indexes
CREATE INDEX idx_custom ON collection_name ((data->>'field_name'));

-- Consider vacuum for space reclamation
VACUUM ANALYZE collection_name;
```

**Backup:**
```bash
# Backup replicated data
pg_dump -h target-host -U user -Fc db > mongodb_replication_backup.dump
```

## FAQ

### Can I replicate multiple databases?

No, each invocation replicates one database. To replicate multiple databases:

```bash
# Replicate database 1
postgres-seren-replicator init \
  --source "mongodb://localhost:27017/db1" \
  --target "postgresql://user:pass@target:5432/db"

# Replicate database 2
postgres-seren-replicator init \
  --source "mongodb://localhost:27017/db2" \
  --target "postgresql://user:pass@target:5432/db"
```

### Is this a one-time replication or continuous sync?

**One-time replication only**. MongoDB to PostgreSQL replications do not support continuous replication.

**Reasons:**
- MongoDB doesn't have built-in logical replication to PostgreSQL
- Change streams would require additional infrastructure
- JSONB storage model is optimized for snapshot replications

**For continuous sync**, consider:
- MongoDB Change Streams with custom sync application
- Third-party replication tools (Debezium, etc.)
- Periodic re-replication for batch updates

### What happens to indexes?

**MongoDB indexes are not replicated**. Only data is converted to JSONB.

**Recommendation**: Create PostgreSQL indexes based on your query patterns:
```sql
-- Replace MongoDB index {email: 1}
CREATE INDEX idx_users_email ON users ((data->>'email'));

-- Replace MongoDB compound index {status: 1, created_at: -1}
CREATE INDEX idx_orders_status_date ON orders (
    (data->>'status'),
    to_timestamp((data->'created_at'->>'$date')::bigint / 1000) DESC
);
```

### Can I query replicated data like a MongoDB database?

**Partially**. PostgreSQL's JSONB supports many MongoDB-like operations, but not all:

**✅ Supported:**
- Field access: `data->>'field'`
- Nested field access: `data->'nested'->>'field'`
- Array containment: `data->'tags' ? 'value'`
- Existence checks: `data ? 'field'`

**❌ Not Supported Natively:**
- MongoDB query syntax (no `$gt`, `$in`, `$regex` operators)
- Aggregation pipeline
- Map-reduce operations

**Solution**: Use PostgreSQL-native SQL with JSONB operators (see Querying Replicated Data section).

### How do I handle schema changes?

JSONB storage is schema-less, so documents can have different fields:

```sql
-- Find documents with a specific field
SELECT * FROM users WHERE data ? 'premium_features';

-- Handle optional fields safely
SELECT
    id,
    data->>'name' AS name,
    COALESCE(data->>'email', 'no-email@example.com') AS email
FROM users;
```

### What about MongoDB transactions?

**Not preserved**. Each document is replicated independently.

**Implications:**
- Referential integrity is not enforced during replication
- Related documents may be replicated in different batches
- No ACID guarantees across collections during replication

**Recommendation**: Verify data relationships after replication if critical.

### Can I reverse the replication (PostgreSQL back to MongoDB)?

**Yes, but with manual work**. The JSONB data can be exported and reimported to MongoDB:

```javascript
// Example: Export from PostgreSQL and import to MongoDB
// 1. Export as JSON
\copy (SELECT jsonb_build_object('_id', id, 'data', data) FROM users) TO 'users.json'

// 2. Import to MongoDB
mongoimport --db mydb --collection users --file users.json
```

However, MongoDB-specific types (ObjectId, DateTime) would need manual conversion back to BSON types.

### How do I replicate only specific collections?

Currently, the tool replicates all collections in a database. To replicate specific collections:

**Option 1: Create a temporary database with only desired collections**
```javascript
// In mongo shell
use temp_db
db.users.insertMany(db.getSiblingDB('mydb').users.find().toArray())
db.orders.insertMany(db.getSiblingDB('mydb').orders.find().toArray())
// Then replicate temp_db
```

**Option 2: Drop unwanted tables after replication**
```sql
DROP TABLE IF EXISTS unwanted_collection;
```

Future versions may support collection-level filtering.

### What's the replication speed?

**Typical rates** (depends on network, hardware, document complexity):
- Small documents (<1KB): 5,000-10,000 docs/sec
- Medium documents (1-10KB): 1,000-5,000 docs/sec
- Large documents (>10KB): 100-1,000 docs/sec

**Example**: A 1 million document collection with 2KB average document size:
- Estimated time: 3-10 minutes

**Factors affecting speed:**
- Network latency between MongoDB and PostgreSQL
- Document size and complexity
- Number of nested fields/arrays
- Source and target server resources

### Is the replication safe?

**Yes**. The tool uses read-only connections to MongoDB and validates all inputs.

**Safety features:**
- MongoDB connection is read-only (no write operations)
- Collection names are validated for SQL injection
- Connection strings are validated before use
- All operations are logged for audit trail
- Target data is transactional (rollback on error)

**Best practice**: Always test replications on non-production data first.
