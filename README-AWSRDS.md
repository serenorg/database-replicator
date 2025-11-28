# AWS RDS PostgreSQL Replication Guide

Zero-downtime database replication from Amazon AWS RDS PostgreSQL to any PostgreSQL target.

---

## Quick Start

```bash
database-replicator init \
  --allow-self-signed-certs \
  --source "postgresql://user:pass@mydb.abc123.us-east-1.rds.amazonaws.com:5432/mydb?sslmode=require" \
  --target "postgresql://user:pass@target-host:5432/mydb" \
  --local
```

**Important Flags for AWS RDS:**

- `--allow-self-signed-certs`: Required when connecting to AWS RDS (accepts Amazon's CA certificates)
- `?sslmode=require`: Add to your source URL for encrypted connections

---

## SerenAI Cloud Replication

**New to SerenAI?** Sign up at [console.serendb.com](https://console.serendb.com) to get started with managed cloud replication.

When replicating to SerenDB targets, this tool runs your replication jobs on SerenAI's cloud infrastructure automatically:

```bash
export SEREN_API_KEY="your-api-key"  # Get from console.serendb.com
database-replicator init \
  --allow-self-signed-certs \
  --source "postgresql://user:pass@mydb.abc123.us-east-1.rds.amazonaws.com:5432/mydb?sslmode=require" \
  --target "postgresql://user:pass@your-db.serendb.com:5432/db"
```

For non-SerenDB targets, use the `--local` flag to run replication locally.

---

## Overview

This guide covers replicating PostgreSQL databases from **Amazon AWS RDS** to another PostgreSQL database (including Seren Cloud). The tool uses PostgreSQL's native logical replication for zero-downtime migration with continuous sync.

### Why This Tool?

- **Zero downtime**: Your RDS database stays online during replication
- **Continuous sync**: Changes replicate in real-time after initial snapshot
- **AWS-native**: Works seamlessly with RDS security groups and IAM
- **Selective replication**: Choose specific databases and tables
- **Interactive mode**: User-friendly terminal UI for selecting what to replicate
- **Production-ready**: Data integrity verification, checkpointing, error handling

### How It Works

The tool uses PostgreSQL's logical replication (publications and subscriptions) to keep databases synchronized:

1. **Initial snapshot**: Copies schema and data using pg_dump/restore
2. **Continuous replication**: Creates publication on source and subscription on target
3. **Real-time sync**: PostgreSQL streams changes from source to target automatically

---

## AWS RDS Prerequisites

### 1. Enable Logical Replication

AWS RDS requires a custom parameter group with logical replication enabled:

```bash
# Create a new parameter group (if you don't have one)
aws rds create-db-parameter-group \
  --db-parameter-group-name my-postgres-params \
  --db-parameter-group-family postgres17 \
  --description "PostgreSQL with logical replication"

# Enable logical replication
aws rds modify-db-parameter-group \
  --db-parameter-group-name my-postgres-params \
  --parameters "ParameterName=rds.logical_replication,ParameterValue=1,ApplyMethod=pending-reboot"

# Associate with your RDS instance and reboot
aws rds modify-db-instance \
  --db-instance-identifier mydb \
  --db-parameter-group-name my-postgres-params

aws rds reboot-db-instance \
  --db-instance-identifier mydb
```

**Verify logical replication is enabled:**

```sql
SHOW rds.logical_replication;  -- Should return '1' or 'on'
SHOW wal_level;                 -- Should return 'logical'
```

### 2. Grant Required Permissions

AWS RDS uses a special `rds_replication` role instead of the standard PostgreSQL `REPLICATION` privilege:

```sql
-- Grant replication role to your user
GRANT rds_replication TO myuser;

-- Grant read access to tables
GRANT USAGE ON SCHEMA public TO myuser;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO myuser;

-- For future tables
ALTER DEFAULT PRIVILEGES IN SCHEMA public
GRANT SELECT ON TABLES TO myuser;
```

### 3. Configure Security Groups

Ensure your RDS security group allows inbound PostgreSQL connections:

**For local execution:**
- Allow your IP address on port 5432

**For remote execution (SerenAI Cloud):**
- Allow SerenAI's IP ranges (contact support for current ranges)
- Or temporarily allow 0.0.0.0/0 during migration (not recommended for production)

```bash
# Example: Allow specific IP
aws ec2 authorize-security-group-ingress \
  --group-id sg-xxx \
  --protocol tcp \
  --port 5432 \
  --cidr YOUR_IP/32
```

### 4. Public Accessibility (Optional)

For remote execution, RDS must be publicly accessible:

```bash
aws rds modify-db-instance \
  --db-instance-identifier mydb \
  --publicly-accessible
```

**Note:** For production databases, consider using VPC peering or AWS PrivateLink instead.

---

## TLS/SSL Configuration

### The Self-Signed Certificate Issue

AWS RDS uses certificates signed by Amazon's Root CA. Many systems don't have Amazon's CA in their trust store, causing TLS errors:

```
Error: certificate verify failed: self-signed certificate in certificate chain
```

### Solution: Use --allow-self-signed-certs

Add the `--allow-self-signed-certs` flag to accept AWS RDS certificates:

```bash
database-replicator init \
  --allow-self-signed-certs \
  --source "postgresql://user:pass@mydb.abc123.us-east-1.rds.amazonaws.com:5432/mydb?sslmode=require" \
  --target "postgresql://user:pass@target-host:5432/mydb" \
  --local
```

### Alternative: Install Amazon Root CA

For production environments, install Amazon's root CA certificate:

```bash
# Download Amazon RDS CA bundle
wget https://truststore.pki.rds.amazonaws.com/global/global-bundle.pem

# Use verify-full mode with the CA bundle
database-replicator init \
  --source "postgresql://user:pass@mydb.abc123.us-east-1.rds.amazonaws.com:5432/mydb?sslmode=verify-full&sslrootcert=/path/to/global-bundle.pem" \
  --target "postgresql://user:pass@target-host:5432/mydb" \
  --local
```

### SSL Modes Explained

| Mode | Encryption | Certificate Verification | Use Case |
|------|------------|-------------------------|----------|
| `disable` | No | No | Testing only (not recommended) |
| `require` | Yes | No | Standard encrypted connection |
| `verify-ca` | Yes | CA only | Higher security |
| `verify-full` | Yes | CA + hostname | Maximum security |

For AWS RDS, use `sslmode=require` with `--allow-self-signed-certs` for simplicity, or `sslmode=verify-full` with the RDS CA bundle for maximum security.

---

## Connection String Format

AWS RDS connection strings follow this format:

```
postgresql://username:password@endpoint:port/database?sslmode=require
```

**Example:**

```bash
# Find your RDS endpoint
aws rds describe-db-instances \
  --db-instance-identifier mydb \
  --query 'DBInstances[0].Endpoint.Address' \
  --output text

# Full connection string
postgresql://myuser:mypassword@mydb.abc123.us-east-1.rds.amazonaws.com:5432/mydb?sslmode=require
```

**URL Encoding:**

If your password contains special characters, URL-encode them:

| Character | Encoded |
|-----------|---------|
| `@` | `%40` |
| `:` | `%3A` |
| `/` | `%2F` |
| `#` | `%23` |
| `?` | `%3F` |
| `%` | `%25` |

Example: `p@ssw:rd` → `p%40ssw%3Ard`

---

## Replication Workflow

### 1. Validate

Check that RDS meets replication requirements:

```bash
database-replicator validate \
  --allow-self-signed-certs \
  --source "postgresql://user:pass@mydb.abc123.us-east-1.rds.amazonaws.com:5432/mydb?sslmode=require" \
  --target "postgresql://user:pass@target-host:5432/mydb"
```

The validate command checks:

- PostgreSQL version (12+)
- `rds_replication` role granted
- `rds.logical_replication` enabled
- Network connectivity
- Target database permissions

---

### 2. Initialize (Init)

Perform initial snapshot replication:

```bash
database-replicator init \
  --allow-self-signed-certs \
  --source "postgresql://user:pass@mydb.abc123.us-east-1.rds.amazonaws.com:5432/mydb?sslmode=require" \
  --target "postgresql://user:pass@target-host:5432/mydb" \
  --yes \
  --local
```

**What happens during init:**

1. **Size estimation**: Analyzes database sizes and shows estimated replication times
2. **Globals dump**: Replicates roles and permissions
3. **Schema dump**: Replicates table structures
4. **Data dump**: Replicates data with parallel operations
5. **Restore**: Restores to target

**Example output:**

```text
Analyzing database sizes...

Database             Size         Est. Time
──────────────────────────────────────────────────
myapp               15.0 GB      ~45.0 minutes
──────────────────────────────────────────────────
Total: 15.0 GB (estimated ~45.0 minutes)

Proceed with replication? [y/N]:
```

---

### 3. Sync

Set up continuous logical replication:

```bash
database-replicator sync \
  --allow-self-signed-certs \
  --source "postgresql://user:pass@mydb.abc123.us-east-1.rds.amazonaws.com:5432/mydb?sslmode=require" \
  --target "postgresql://user:pass@target-host:5432/mydb"
```

**Note:** The target must be able to connect BACK to the RDS source for subscription to work. Ensure:
- RDS security group allows target IP
- RDS is publicly accessible (or use VPC peering)

---

### 4. Status

Monitor replication health:

```bash
database-replicator status \
  --allow-self-signed-certs \
  --source "postgresql://user:pass@mydb.abc123.us-east-1.rds.amazonaws.com:5432/mydb?sslmode=require" \
  --target "postgresql://user:pass@target-host:5432/mydb"
```

---

### 5. Verify

Validate data integrity:

```bash
database-replicator verify \
  --allow-self-signed-certs \
  --source "postgresql://user:pass@mydb.abc123.us-east-1.rds.amazonaws.com:5432/mydb?sslmode=require" \
  --target "postgresql://user:pass@target-host:5432/mydb"
```

---

## Selective Replication

### Include Specific Tables

```bash
database-replicator init \
  --allow-self-signed-certs \
  --source "postgresql://user:pass@mydb.rds.amazonaws.com:5432/mydb?sslmode=require" \
  --target "postgresql://user:pass@target-host:5432/mydb" \
  --include-tables "mydb.users,mydb.orders,mydb.products" \
  --local
```

### Exclude Large Tables

```bash
database-replicator init \
  --allow-self-signed-certs \
  --source "postgresql://user:pass@mydb.rds.amazonaws.com:5432/mydb?sslmode=require" \
  --target "postgresql://user:pass@target-host:5432/mydb" \
  --exclude-tables "mydb.logs,mydb.audit_trail" \
  --local
```

### Time-Based Filters

Replicate only recent data (great for large historical tables):

```bash
database-replicator init \
  --allow-self-signed-certs \
  --source "postgresql://user:pass@mydb.rds.amazonaws.com:5432/mydb?sslmode=require" \
  --target "postgresql://user:pass@target-host:5432/mydb" \
  --time-filter "events:created_at:6 months" \
  --schema-only-tables "audit_logs" \
  --local
```

---

## Troubleshooting

### TLS Certificate Errors

**Error:**
```
certificate verify failed: self-signed certificate in certificate chain
```

**Solution:**
```bash
# Add --allow-self-signed-certs flag
database-replicator init \
  --allow-self-signed-certs \
  --source "postgresql://...?sslmode=require" \
  ...
```

---

### Permission Denied

**Error:**
```
permission denied for table
```

**Solution:**
```sql
-- On RDS source
GRANT rds_replication TO myuser;
GRANT USAGE ON SCHEMA public TO myuser;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO myuser;
```

---

### Logical Replication Not Enabled

**Error:**
```
logical replication is not enabled
```

**Solution:**

1. Create parameter group with `rds.logical_replication=1`
2. Apply to RDS instance
3. Reboot instance
4. Verify: `SHOW rds.logical_replication;`

---

### Connection Timeout

**Error:**
```
connection timed out
```

**Solution:**

1. Check RDS security group allows your IP
2. Verify RDS is publicly accessible (if needed)
3. Check VPC routing and NAT gateway configuration

```bash
# Test connectivity
nc -zv mydb.abc123.us-east-1.rds.amazonaws.com 5432
```

---

### Subscription Cannot Connect Back

**Error:**
```
could not connect to the publisher
```

**Cause:** Target cannot reach RDS source for subscription.

**Solution:**

1. Ensure RDS allows inbound from target IP
2. Use `.pgpass` on target server for credentials
3. Verify network path between target and RDS

---

### WAL Files Accumulating

**Symptom:** RDS storage increasing rapidly during replication.

**Cause:** Replication slot holding WAL files.

**Solution:**

```sql
-- Check replication slots
SELECT * FROM pg_replication_slots;

-- Monitor slot lag
SELECT slot_name, pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn) AS lag_bytes
FROM pg_replication_slots;
```

If slot is orphaned:
```sql
SELECT pg_drop_replication_slot('slot_name');
```

---

## AWS RDS-Specific Limitations

1. **No Superuser**: RDS doesn't allow true superuser access. Use `rds_superuser` role for admin tasks.

2. **Parameter Group Changes**: Some parameters require reboot to take effect.

3. **Multi-AZ Failover**: During failover, replication slots may be lost. Monitor and recreate if needed.

4. **Read Replicas**: Cannot create publications on read replicas (use primary instance).

5. **Aurora**: Aurora PostgreSQL has different replication mechanics. This guide applies to standard RDS PostgreSQL.

---

## Best Practices

### Security

- Use IAM database authentication when possible
- Rotate credentials regularly
- Use VPC peering instead of public accessibility for production
- Enable RDS encryption at rest
- Use `sslmode=verify-full` with CA bundle for maximum security

### Performance

- Schedule large migrations during low-traffic periods
- Use `--time-filter` to reduce initial sync time
- Monitor RDS CloudWatch metrics during replication
- Consider upgrading RDS instance class temporarily for faster migration

### Monitoring

```bash
# Watch replication status
watch -n 5 'database-replicator status \
  --allow-self-signed-certs \
  --source "postgresql://...?sslmode=require" \
  --target "postgresql://..."'
```

### Cleanup After Migration

After successful migration, clean up replication artifacts:

```sql
-- On target
DROP SUBSCRIPTION IF EXISTS seren_replication_sub;

-- On RDS source
DROP PUBLICATION IF EXISTS seren_replication_pub;

-- Check for orphaned slots
SELECT pg_drop_replication_slot(slot_name)
FROM pg_replication_slots
WHERE active = false;
```

---

## Complete Example: RDS to SerenDB

```bash
# Step 1: Validate connectivity
database-replicator validate \
  --allow-self-signed-certs \
  --source "postgresql://admin:MyP%40ssword@mydb.abc123.us-east-1.rds.amazonaws.com:5432/production?sslmode=require" \
  --target "postgresql://serendb_owner:password@mydb.serendb.com:5432/production?sslmode=require"

# Step 2: Initial snapshot (local execution)
database-replicator init \
  --allow-self-signed-certs \
  --source "postgresql://admin:MyP%40ssword@mydb.abc123.us-east-1.rds.amazonaws.com:5432/production?sslmode=require" \
  --target "postgresql://serendb_owner:password@mydb.serendb.com:5432/production?sslmode=require" \
  --include-tables "production.users,production.orders,production.products" \
  --time-filter "production.events:created_at:6 months" \
  --yes \
  --local

# Step 3: Set up continuous sync
database-replicator sync \
  --allow-self-signed-certs \
  --source "postgresql://admin:MyP%40ssword@mydb.abc123.us-east-1.rds.amazonaws.com:5432/production?sslmode=require" \
  --target "postgresql://serendb_owner:password@mydb.serendb.com:5432/production?sslmode=require"

# Step 4: Monitor replication
database-replicator status \
  --allow-self-signed-certs \
  --source "postgresql://admin:MyP%40ssword@mydb.abc123.us-east-1.rds.amazonaws.com:5432/production?sslmode=require" \
  --target "postgresql://serendb_owner:password@mydb.serendb.com:5432/production?sslmode=require"

# Step 5: Verify data integrity
database-replicator verify \
  --allow-self-signed-certs \
  --source "postgresql://admin:MyP%40ssword@mydb.abc123.us-east-1.rds.amazonaws.com:5432/production?sslmode=require" \
  --target "postgresql://serendb_owner:password@mydb.serendb.com:5432/production?sslmode=require"
```

---

## Additional Documentation

- **[Main README](README.md)** - Multi-database support overview
- **[PostgreSQL Guide](README-PostgreSQL.md)** - General PostgreSQL replication
- **[Replication Configuration Guide](docs/replication-config.md)** - Advanced filtering with TOML config files
- **[AWS Setup Guide](docs/aws-setup.md)** - Remote execution infrastructure details

---

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.
