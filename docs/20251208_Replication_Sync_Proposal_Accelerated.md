

### Network Traffic Comparison

| Operation | WAL Streaming | xmin Polling |
|-----------|---------------|--------------|
| Single row UPDATE | ~50 bytes (delta only) | ~500 bytes (entire row) |
| 10,000 changes/day | ~500 KB | ~5 MB |
| Delete detection | Built-in | +8 MB per 1M PKs reconciled |

**Why xmin uses more bandwidth:**

- WAL streams only the **changed columns** as compact binary records
- xmin queries return **entire rows** even if only one column changed
- Delete reconciliation requires fetching all PKs from both source and target

For a 1M row table with 1% daily churn, expect ~10x more network traffic than logical replication. This is acceptable because:

1. SerenDB absorbs the compute/bandwidth cost
2. Customer source DB sees only read traffic
3. Modern networks handle this easily

---

## Problem Statement

The existing `sync` command requires `wal_level=logical` on the source database. Most customers can't or won't change this setting. Without it, they're limited to periodic `init` runs (full table copies).

---

## Solution Overview

### Core Approach: xmin + Full Reconciliation

PostgreSQL has a hidden system column called `xmin` on every row. It contains the transaction ID that last modified that row. We can use this to detect changes without any source configuration.

```sql
-- Get all rows changed since last sync
SELECT * FROM table WHERE xmin::text::bigint > $last_synced_xmin;
```

**However**, `xmin` cannot detect deletes (deleted rows are gone). So we combine it with **full PK reconciliation**:

```sql
-- Find rows in target that no longer exist in source
SELECT target.pk FROM target_table target
LEFT JOIN source_table source ON target.pk = source.pk
WHERE source.pk IS NULL;

-- Delete orphaned rows
DELETE FROM target_table WHERE pk IN (orphaned_pks);
```

### Automatic Mode Detection

The `sync` command automatically detects source capabilities and chooses the best sync method:

| Source wal_level | Sync Method | Delete Detection | Latency |
|------------------|-------------|------------------|---------|
| `logical` | Logical replication | Real-time | Sub-second |
| `replica` (default) | xmin polling | Full reconciliation | Seconds |

**No flags required.** The tool handles everything automatically.

### Customer Experience

```bash
# Customer runs a single command - no mode flags needed
database-replicator sync --source "postgresql://..." --target "postgresql://..."
```

**What happens internally:**

```
1. Connect to source database
2. Check: SELECT current_setting('wal_level')
3. If 'logical' → use logical replication (faster)
   If 'replica' → auto-fallback to xmin (no prompt, no error)
4. Log which method was selected
5. Start continuous sync
```

**Customer sees:**
```
INFO: Checking source database capabilities...
INFO: Source has wal_level=replica (logical replication not available)
INFO: Using xmin-based sync (no source changes required)
INFO: Starting sync cycle 1...
```

Zero customer decisions. Zero flags. Just works.

---

## Architecture

### System Components

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        SerenDB Infrastructure                            │
│                                                                          │
│  ┌──────────────────────────────────────────────────────────────────┐   │
│  │                     Sync Daemon Service                           │   │
│  │                                                                    │   │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────────┐   │   │
│  │  │   Scheduler │  │ Sync Worker │  │   State Store           │   │   │
│  │  │             │──│             │──│   (PostgreSQL)          │   │   │
│  │  │ Cron/Timer  │  │ xmin query  │  │   - last_xmin per table │   │   │
│  │  │             │  │ PK reconcile│  │   - sync_status         │   │   │
│  │  │             │  │ UPSERT/DEL  │  │   - error_log           │   │   │
│  │  └─────────────┘  └─────────────┘  └─────────────────────────┘   │   │
│  │                           │                                        │   │
│  └───────────────────────────│────────────────────────────────────────┘   │
│                              │                                            │
└──────────────────────────────│────────────────────────────────────────────┘
                               │
          ┌────────────────────┴────────────────────┐
          │                                         │
          ▼                                         ▼
┌──────────────────────┐                 ┌──────────────────────┐
│   Customer Source    │                 │   SerenDB Target     │
│   (Neon/RDS/etc)     │                 │                      │
│                      │                 │                      │
│  - Read-only access  │                 │  - Full access       │
│  - No config changes │                 │  - UPSERT/DELETE     │
│                      │                 │                      │
└──────────────────────┘                 └──────────────────────┘
```

### Module Structure

```
src/
├── xmin/                      # NEW: xmin-based sync module
│   ├── mod.rs                 # Module exports
│   ├── reader.rs              # Read changed rows via xmin
│   ├── reconciler.rs          # PK comparison for delete detection
│   ├── writer.rs              # UPSERT changes to target
│   └── state.rs               # Sync state persistence
├── commands/
│   └── sync.rs                # MODIFY: Add auto-detection logic
├── daemon/                    # NEW: Background service mode
│   ├── mod.rs
│   ├── scheduler.rs           # Periodic sync scheduling
│   └── runner.rs              # Sync execution loop
└── ...
```

---

## Data Flow

### Sync Cycle (Every N Seconds)

```
┌─────────────────────────────────────────────────────────────────────────┐
│                           SYNC CYCLE                                     │
└─────────────────────────────────────────────────────────────────────────┘

Step 1: Load State
━━━━━━━━━━━━━━━━━━
┌─────────────────┐
│ State Store     │──▶ last_xmin = 12345678
│                 │    tables = [users, orders, events]
└─────────────────┘

Step 2: Query Changed Rows (per table)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
┌─────────────────┐    SELECT * FROM users
│ Source DB       │◀── WHERE xmin::text::bigint > 12345678
│                 │───▶ [row1, row2, row3...]
└─────────────────┘

Step 3: UPSERT to Target
━━━━━━━━━━━━━━━━━━━━━━━━━
┌─────────────────┐    INSERT INTO users (...) VALUES (...)
│ Target DB       │◀── ON CONFLICT (pk) DO UPDATE SET ...
│                 │
└─────────────────┘

Step 4: Delete Detection (periodic, e.g., every 10th cycle)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
┌─────────────────┐    SELECT id FROM users
│ Source DB       │───▶ source_pks = {1, 2, 3, 5, 6}
└─────────────────┘

┌─────────────────┐    SELECT id FROM users
│ Target DB       │───▶ target_pks = {1, 2, 3, 4, 5, 6}
└─────────────────┘

                       orphaned = target_pks - source_pks = {4}

┌─────────────────┐    DELETE FROM users WHERE id IN (4)
│ Target DB       │◀──
└─────────────────┘

Step 5: Update State
━━━━━━━━━━━━━━━━━━━━
┌─────────────────┐
│ State Store     │◀── last_xmin = 12345999 (max xmin seen)
│                 │    last_reconcile = NOW()
└─────────────────┘
```

### xmin Wraparound Handling

PostgreSQL transaction IDs are 32-bit and wrap around. We must detect this:

```rust
// If new_xmin < old_xmin by a large margin, wraparound occurred
fn detect_wraparound(old_xmin: i64, new_xmin: i64) -> bool {
    // Transaction IDs are unsigned 32-bit, max ~4 billion
    // If new < old by more than 2 billion, it wrapped
    old_xmin > new_xmin && (old_xmin - new_xmin) > 2_000_000_000
}

// On wraparound, do a full table sync
if detect_wraparound(state.last_xmin, current_xmin) {
    warn!("xmin wraparound detected, performing full sync");
    perform_full_table_sync(table);
}
```

---

## Security Considerations

**Credentials:** Encrypted storage, `.pgpass` temp files (0600 permissions), never logged. Use existing `strip_password_from_url` pattern.

**Access Control:**

```sql
-- Source: SELECT only
GRANT SELECT ON ALL TABLES IN SCHEMA public TO sync_user;
-- Target: INSERT, UPDATE, DELETE
GRANT INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO sync_user;
```

**Rate Limiting:** Min 5s sync interval, connection pooling, exponential backoff on errors.

---

## Design Patterns (Adapted from Christian's Example)

The following patterns are borrowed from Christian's watermark-based sync example and adapted for our xmin approach:

### 1. Fetch-Upsert-Sleep Loop

```rust
loop {
    // 1. Get current watermark (xmin instead of updated_at)
    let last_xmin = get_watermark_from_state(&state, schema, table);

    // 2. Fetch changed rows with LIMIT for batching
    let changed_rows = fetch_changes_batched(source, table, last_xmin, BATCH_SIZE).await?;

    if changed_rows.is_empty() {
        // No changes - sleep and retry
        tokio::time::sleep(Duration::from_secs(interval)).await;
        continue;
    }

    // 3. Upsert in transaction for atomicity
    upsert_batch_in_transaction(target, table, &changed_rows).await?;

    // 4. Update watermark
    update_state_watermark(&mut state, schema, table, max_xmin);

    // 5. Immediate continue if batch was full (more data likely waiting)
    if changed_rows.len() == BATCH_SIZE {
        continue;  // Don't sleep - catch up first
    }
}
```

### 2. Batched Data Retrieval with LIMIT

Prevents memory exhaustion on large tables:

```rust
const BATCH_SIZE: i64 = 1000;

async fn fetch_changes_batched(
    client: &Client,
    table: &str,
    last_xmin: i64,
    limit: i64,
) -> Result<Vec<Row>> {
    let query = format!(
        r#"
        SELECT *, xmin::text::bigint as __xmin
        FROM "{}"
        WHERE xmin::text::bigint > $1
        ORDER BY xmin::text::bigint ASC
        LIMIT $2
        "#,
        table
    );

    client.query(&query, &[&last_xmin, &limit]).await
}
```

### 3. Transaction-Wrapped Batch Upserts

Ensures atomicity - if network fails mid-batch, transaction rolls back:

```rust
async fn upsert_batch_in_transaction(
    client: &Client,
    table: &str,
    rows: &[Row],
    pk_columns: &[String],
) -> Result<u64> {
    let tx = client.transaction().await?;

    let mut count = 0u64;
    for row in rows {
        // Build and execute UPSERT
        tx.execute(&upsert_query, &params).await?;
        count += 1;
    }

    tx.commit().await?;
    Ok(count)
}
```

### 4. Automatic State Recovery

If the daemon crashes, it recovers by reading the last saved watermark:

```rust
// On startup - auto-recover from last known state
let state = load_sync_state()?
    .filter(|s| s.urls_match(source, target))  // Validate same source/target
    .unwrap_or_else(|| SyncState::new(source, target));  // Fresh start if mismatch

// State is saved after each successful batch
// Crash recovery = restart from last committed watermark
```

### 5. Key Differences from Christian's Approach

| Aspect | Christian (updated_at) | Our Approach (xmin) |
|--------|------------------------|---------------------|
| Watermark source | `MAX(updated_at)` column | `xmin` system column |
| Schema changes | Requires `updated_at` column | None required |
| Delete detection | Not supported | Full PK reconciliation |
| App dependency | App must set `updated_at` | None - PostgreSQL manages xmin |
| Type safety | Timestamp comparison | Integer comparison (faster) |

---

## Implementation Tasks

This project is planned for a single, intensive day of development, with tasks split between Gemini and Claude to maximize parallel progress.

### 1-Day Accelerated Implementation Plan

| Time Slot | Gemini (Engineer 1) | Claude (Engineer 2) |
|---|---|---|
| **Hour 1-2** | **Task: Core Reader & State** <br> Implement `XminReader` for fetching changed rows. <br> Implement `SyncState` for persistence. <br> **Files:** `xmin/reader.rs`, `xmin/state.rs` | **Task: Change Writer** <br> Implement `ChangeWriter` for UPSERTing data to the target. <br> Focus on efficient, batched writes. <br> **Files:** `xmin/writer.rs` |
| **Hour 3-4** | **Task: Delete Reconciliation** <br> Implement `Reconciler` to detect and delete orphaned rows using PK comparison. <br> **Files:** `xmin/reconciler.rs` | **Task: Refinement & Utilities** <br> Refine `ChangeWriter` with robust type handling. <br> Create utility functions for fetching PK/column metadata. <br> **Files:** `xmin/writer.rs`, `utils.rs` |
| **Hour 5-6** | **Task: Daemon Mode** <br> Implement the `SyncDaemon` for continuous, background operation. <br> Add graceful shutdown logic. <br> **Files:** `daemon/runner.rs` | **Task: Command Integration** <br> Integrate all `xmin` modules into the `sync` command. <br> Add auto-detection logic for `wal_level`. <br> **Files:** `commands/sync.rs` |
| **Hour 7-8** | **Task: Integration Testing** <br> Write end-to-end integration tests covering inserts, updates, deletes, and performance for the xmin sync workflow. <br> **Files:** `tests/xmin_integration_test.rs` | **Task: Documentation** <br> Update `README.md` and other relevant documents with details on the new xmin sync mode, its benefits, and configuration. <br> **Files:** `README.md` |

---

## Testing Strategy

**Unit tests:** Happy path, edge cases, error cases, SQL injection prevention.

**Integration tests:** Insert/update/delete detection, large tables (100k+), non-public schemas, composite PKs, type preservation, error recovery.

**Security:** No production data, clear test tables after each test, don't commit connection strings.

---

## Appendix

### A. PostgreSQL xmin Explained

`xmin` is a system column present on every PostgreSQL row. It contains the
transaction ID (XID) of the transaction that inserted or last updated the row.

```sql
-- View xmin values
SELECT xmin, * FROM my_table LIMIT 5;

-- xmin is unsigned 32-bit, but we cast to bigint for comparison
SELECT * FROM my_table WHERE xmin::text::bigint > 12345678;
```

**Important Notes:**
- xmin values are not globally unique across tables
- xmin values can wrap around (32-bit limit ~4 billion)
- Deleted rows have no xmin (they're gone)
- VACUUM can freeze old xmin values

### B. References

- [Airbyte PostgreSQL Source](https://docs.airbyte.com/integrations/sources/postgres)
- [PostgreSQL System Columns](https://www.postgresql.org/docs/current/ddl-system-columns.html)
- [PostgreSQL Logical Replication](https://www.postgresql.org/docs/current/logical-replication.html)
