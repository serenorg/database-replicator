# AWS RDS PostgreSQL Replication Guide

Zero-downtime database replication from AWS RDS PostgreSQL to SerenDB using logical replication.

---

## SerenAI Cloud Replication

**New to SerenAI?** Sign up at [console.serendb.com](https://console.serendb.com) to get started with managed cloud replication.

When replicating to SerenDB targets, this tool runs your replication jobs on SerenAI's cloud infrastructure automatically. Just set your API key and run:

```bash
export SEREN_API_KEY="your-api-key"  # Get from console.serendb.com
database-replicator init \
  --source "postgresql://user:pass@your-rds.region.rds.amazonaws.com:5432/db" \
  --target "postgresql://user:pass@your-db.serendb.com:5432/db"
```

For non-SerenDB targets, use the `--local` flag to run replication locally.

---

## Installation

Install the CLI before running the AWS-specific workflow below.

### Option 1: Download a Pre-built Binary

1. Visit the [latest GitHub Release](https://github.com/serenorg/database-replicator/releases/latest).
2. Download the asset for your operating system and CPU (Linux x86_64/arm64, macOS Intel/Apple Silicon, or Windows x86_64).
3. Extract the archive. On Linux/macOS run:

```bash
chmod +x database-replicator*
sudo mv database-replicator* /usr/local/bin/database-replicator
database-replicator --help
```

4. On Windows, run the `.exe` directly or add it to the `PATH`.

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

Building locally lets you pin to a specific commit or customize the binary for regulated environments.

---

## AWS RDS-Specific Prerequisites

### 1. Enable Logical Replication

AWS RDS requires a custom parameter group to enable logical replication:

```bash
# Create a new parameter group (if not already done)
aws rds create-db-parameter-group \
  --db-parameter-group-name pg-logical-replication \
  --db-parameter-group-family postgres17 \
  --description "PostgreSQL with logical replication enabled"

# Set wal_level to logical
aws rds modify-db-parameter-group \
  --db-parameter-group-name pg-logical-replication \
  --parameters "ParameterName=rds.logical_replication,ParameterValue=1,ApplyMethod=pending-reboot"

# Apply the parameter group to your RDS instance
aws rds modify-db-instance \
  --db-instance-identifier your-instance-id \
  --db-parameter-group-name pg-logical-replication \
  --apply-immediately

# IMPORTANT: Reboot is required for wal_level change
aws rds reboot-db-instance --db-instance-identifier your-instance-id
```

After reboot, verify the setting:

```sql
SHOW wal_level;  -- Should return 'logical'
```

### 2. Grant Replication Privileges

AWS RDS uses a special role `rds_replication` instead of the standard PostgreSQL `REPLICATION` privilege:

```sql
-- Grant the rds_replication role to your user
GRANT rds_replication TO your_username;

-- Grant read access to tables
GRANT USAGE ON SCHEMA public TO your_username;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO your_username;
```

**Note:** On AWS RDS, you cannot use `ALTER USER ... WITH REPLICATION;` - you must use `GRANT rds_replication TO username;` instead.

### 3. Configure Security Group

Ensure your RDS security group allows inbound connections:

- **Port:** 5432 (PostgreSQL default)
- **Source:** IP address or CIDR range of your replication client
- For SerenAI Cloud execution, allow connections from SerenAI infrastructure

### 4. TLS/SSL Configuration

AWS RDS requires SSL connections. Add `sslmode=require` to your connection string:

```bash
# Connection string format for AWS RDS
postgresql://user:password@your-rds.region.rds.amazonaws.com:5432/database?sslmode=require
```

If you encounter TLS certificate verification errors, use the `--allow-self-signed-certs` flag:

```bash
database-replicator validate \
  --source "postgresql://user:pass@your-rds.region.rds.amazonaws.com:5432/db?sslmode=require" \
  --target "postgresql://user:pass@your-db.serendb.com:5432/db" \
  --allow-self-signed-certs
```

---

## Quick Start

### Step 1: Validate Connection and Prerequisites

```bash
database-replicator validate \
  --source "postgresql://user:pass@your-rds.region.rds.amazonaws.com:5432/db?sslmode=require" \
  --target "postgresql://user:pass@your-db.serendb.com:5432/db" \
  --allow-self-signed-certs
```

Expected output:

```
Pre-flight Checks
═════════════════════════════════════════════════════════════

Local Environment:
  ✓ pg_dump found
  ✓ pg_dumpall found
  ✓ pg_restore found
  ✓ psql found

Network Connectivity:
  ✓ Source database reachable
  ✓ Target database reachable

Source Permissions:
  ✓ Has rds_replication role (AWS RDS)
  ✓ Has SELECT on all 42 tables

Target Permissions:
  ✓ Can create databases
  ✓ Can create subscriptions

═════════════════════════════════════════════════════════════
PASSED: All pre-flight checks successful
```

### Step 2: Initial Snapshot Replication

```bash
database-replicator init \
  --source "postgresql://user:pass@your-rds.region.rds.amazonaws.com:5432/db?sslmode=require" \
  --target "postgresql://user:pass@your-db.serendb.com:5432/db" \
  --allow-self-signed-certs \
  --yes
```

### Step 3: Set Up Continuous Sync

```bash
database-replicator sync \
  --source "postgresql://user:pass@your-rds.region.rds.amazonaws.com:5432/db?sslmode=require" \
  --target "postgresql://user:pass@your-db.serendb.com:5432/db" \
  --allow-self-signed-certs
```

### Step 4: Monitor Replication

```bash
database-replicator status \
  --source "postgresql://user:pass@your-rds.region.rds.amazonaws.com:5432/db?sslmode=require" \
  --target "postgresql://user:pass@your-db.serendb.com:5432/db" \
  --allow-self-signed-certs
```

### Step 5: Verify Data Integrity

```bash
database-replicator verify \
  --source "postgresql://user:pass@your-rds.region.rds.amazonaws.com:5432/db?sslmode=require" \
  --target "postgresql://user:pass@your-db.serendb.com:5432/db" \
  --allow-self-signed-certs
```

---

## Troubleshooting

### Error: "Missing REPLICATION privilege"

**Cause:** AWS RDS uses `rds_replication` role instead of the standard `REPLICATION` privilege.

**Solution:**

```sql
-- Connect to your RDS instance as the master user
GRANT rds_replication TO your_username;
```

### Error: "certificate verify failed: self-signed certificate in certificate chain"

**Cause:** AWS RDS uses certificates that may not be in your local trust store.

**Solution:** Add `--allow-self-signed-certs` flag and ensure `sslmode=require` in your connection string:

```bash
database-replicator validate \
  --source "postgresql://...?sslmode=require" \
  --target "postgresql://..." \
  --allow-self-signed-certs
```

### Error: "wal_level must be logical"

**Cause:** Logical replication is not enabled on your RDS instance.

**Solution:**

1. Create or modify a parameter group with `rds.logical_replication = 1`
2. Apply the parameter group to your instance
3. Reboot the instance (required for this change)

```bash
aws rds modify-db-parameter-group \
  --db-parameter-group-name your-param-group \
  --parameters "ParameterName=rds.logical_replication,ParameterValue=1,ApplyMethod=pending-reboot"

aws rds reboot-db-instance --db-instance-identifier your-instance-id
```

### Error: "Connection refused" or timeout

**Cause:** Security group or network configuration issue.

**Solution:**

1. Check your RDS security group allows inbound connections on port 5432
2. Ensure the RDS instance is publicly accessible (if connecting from outside VPC)
3. Check VPC routing if connecting from within AWS

```bash
# Test connectivity
psql "postgresql://user:pass@your-rds.region.rds.amazonaws.com:5432/db?sslmode=require"
```

---

## AWS RDS Limitations

1. **No superuser access:** AWS RDS doesn't provide superuser, so some operations may require workarounds
2. **rds_replication role:** Must use `GRANT rds_replication TO user` instead of `ALTER USER ... REPLICATION`
3. **Parameter group changes:** Changing `wal_level` requires instance reboot
4. **SSL required:** Most RDS instances require SSL connections by default

---

## Additional Resources

- [AWS RDS PostgreSQL Documentation](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/CHAP_PostgreSQL.html)
- [PostgreSQL Logical Replication](https://www.postgresql.org/docs/current/logical-replication.html)
- [SerenDB Documentation](https://console.serendb.com/docs)
