# Show HN: Publisher-Subscriber PostgreSQL Replication with xmin and Rust

**TL;DR:** We built an open-source Rust CLI that replicates PostgreSQL databases without requiring `wal_level=logical`. It uses PostgreSQL's `xmin` system column to detect changes, enabling CDC-style replication from any managed PostgreSQL service—no configuration changes needed.

**GitHub:** https://github.com/serenorg/database-replicator

---

## The Problem

PostgreSQL's logical replication is powerful but has a frustrating prerequisite: `wal_level=logical`. Most managed PostgreSQL services (Neon, Heroku, many AWS RDS configurations) default to `wal_level=replica`, and changing it often requires a database restart or isn't available at all.

We needed replication for SerenDB that worked with *any* PostgreSQL source—regardless of how it was configured. The solution: leverage PostgreSQL's transaction visibility system instead of the WAL.

---

## How xmin-Based Replication Works

Every PostgreSQL row has a hidden system column called `xmin`—the transaction ID that created or last modified that row. By tracking `xmin` values, we can identify which rows changed since our last sync without requiring any special database configuration.

Here's the core algorithm:

```
1. Record current max(xmin) as high_water_mark
2. SELECT * FROM table WHERE xmin > last_sync_xmin
3. UPSERT changed rows to target (ON CONFLICT DO UPDATE)
4. Store high_water_mark for next cycle
5. Periodically reconcile deletes via primary key comparison
```

The beauty is that `xmin` is always available—it's part of PostgreSQL's MVCC implementation. No extensions, no configuration, no special privileges beyond SELECT.

---

## Five Technical Decisions That Made It Work

### 1. Rust for Reliability and Performance

We chose Rust for zero-cost abstractions and memory safety in a long-running daemon. The `tokio-postgres` crate provides async database access, and `rust_decimal` handles PostgreSQL's `numeric` type without precision loss. Type safety caught numerous edge cases at compile time that would have been runtime bugs in other languages.

```rust
// Type-safe handling of 15+ PostgreSQL array types
match data_type.as_str() {
    "_text" | "_varchar" => {
        let val: Option<Vec<String>> = row.get(idx);
        Box::new(val)
    }
    "_int8" => {
        let val: Option<Vec<i64>> = row.get(idx);
        Box::new(val)
    }
    // ... handles _numeric, _jsonb, _timestamp, etc.
}
```

### 2. Using `udt_name` Instead of `data_type`

A subtle but critical detail: PostgreSQL's `information_schema.columns.data_type` returns "ARRAY" for all array types. To get the actual element type (`_text`, `_int4`, `_jsonb`), you need `udt_name`. This single change fixed a class of serialization errors that had been causing sync failures.

### 3. Batched Upserts with Composite Primary Keys

Rather than individual INSERTs, we batch changes into multi-row upserts:

```sql
INSERT INTO table (pk1, pk2, col1, col2)
VALUES ($1, $2, $3, $4), ($5, $6, $7, $8), ...
ON CONFLICT (pk1, pk2) DO UPDATE SET
  col1 = EXCLUDED.col1,
  col2 = EXCLUDED.col2
```

This reduces round-trips and handles both inserts and updates in one operation. Composite primary keys are fully supported.

### 4. Transaction ID Wraparound Detection

PostgreSQL's `xmin` is a 32-bit transaction ID that wraps around after ~4 billion transactions. We detect wraparound by checking if the current `xmin` is significantly lower than our stored high-water mark, triggering a full table resync when detected. Silent data loss from missed wraparound was a risk we couldn't accept.

### 5. Reconciliation for Delete Detection

xmin only tracks row modifications—it can't tell us about deletes. We solve this with periodic reconciliation: compare primary keys between source and target, delete any target rows missing from source. This runs on a configurable interval (default: daily) to balance consistency with performance.

---

## Using It

```bash
# Install
cargo install database-replicator

# Initial copy
database-replicator init \
  --source "postgresql://source-host/mydb" \
  --target "postgresql://target-host/mydb"

# Start continuous sync (auto-detects best method)
database-replicator sync \
  --source "postgresql://source-host/mydb" \
  --target "postgresql://target-host/mydb"
```

If your source has `wal_level=logical`, it uses native logical replication. If not, it automatically falls back to xmin-based polling. Zero configuration required.

---

## xmin vs WAL: Trade-offs and Costs

Choosing between xmin-based sync and native logical replication involves trade-offs in latency, resource usage, and operational complexity.

### Performance Comparison

| Metric | WAL (Logical Replication) | xmin Polling |
|--------|---------------------------|--------------|
| **Latency** | Sub-second (streaming) | Configurable (default: 1 hour) |
| **Source CPU** | Minimal (reads WAL) | Higher (full table scans) |
| **Source I/O** | WAL only | Data pages + indexes |
| **Delete detection** | Immediate | Requires reconciliation |
| **Configuration** | `wal_level=logical` + restart | None |

### When to Use Each

**Use WAL replication when:**

- You control the source database configuration
- You need real-time or near-real-time sync
- The source has `wal_level=logical` already enabled
- You're replicating high-volume OLTP workloads

**Use xmin polling when:**

- Source is a managed service without logical replication
- Hourly/daily sync is acceptable
- You can't restart the source database
- You need zero-config replication from any PostgreSQL

### Compute Costs (Continuous Sync)

For xmin-based sync running 24/7 on cloud infrastructure:

| Instance | Use Case | Monthly Cost |
|----------|----------|--------------|
| t3.micro | Light sync (small DB) | ~$8 |
| t3.small | Medium workloads | ~$15 |
| t3.medium | Heavy sync (many tables) | ~$30 |

WAL replication has no dedicated compute cost—it runs within your existing PostgreSQL process. However, it increases WAL volume and may require larger storage.

### Our Recommendation

Start with xmin polling (it works everywhere). If you need lower latency and can enable `wal_level=logical`, the tool automatically upgrades to native replication—no code changes required.

---

## Fork It

The entire codebase is Apache 2.0 licensed. Key extension points:

- **`src/xmin/reader.rs`** - Change detection logic
- **`src/xmin/writer.rs`** - Type conversion and batched writes
- **`src/xmin/daemon.rs`** - Sync orchestration and scheduling
- **`src/xmin/reconciler.rs`** - Delete detection

We'd love contributions for: additional source databases, smarter batching strategies, or webhook notifications on sync events.

---

**Links:**
- GitHub: https://github.com/serenorg/database-replicator
- Crates.io: https://crates.io/crates/database-replicator
- SerenDB: https://serendb.com
