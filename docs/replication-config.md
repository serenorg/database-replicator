# Advanced Replication Configuration

The `--config` flag lets you describe complex per-database replication rules in TOML. This is especially useful for TimescaleDB or financial workloads with huge historical tables where only recent slices are required.

## File Structure

```toml
# replication-config.toml

[databases.analytics]
# Tables copied as schema-only (no data)
schema_only = ["archive_logs", "historical_prices"]

# Arbitrary SQL WHERE predicates
[[databases.analytics.table_filters]]
table = "events"
where = "event_time >= NOW() - INTERVAL '6 months'"

[[databases.analytics.table_filters]]
table = "orders"
where = "status IN ('active', 'pending')"

# Time-based shorthand; converted to WHERE column >= NOW() - INTERVAL 'x'
[[databases.analytics.time_filters]]
table = "user_activity"
column = "created_at"
last = "1 year"
```

- `schema_only`: Accepts table names local to the database. These tables get their structure copied but skip bulk data dumps (ideal for archive tables).
- `table_filters`: Provide full SQL predicates. Use this when you need arbitrary filters or multi-column conditions.
- `time_filters`: Easier syntax for the common `column >= NOW() - INTERVAL '...'` pattern. Units can be seconds, minutes, hours, days, weeks, months, or years.

You can mix and match sections. CLI flags (`--schema-only-tables`, `--table-filter`, `--time-filter`) merge on top of the config file, so one-off overrides do not require committing a new file.

## Usage

```bash
./seren-replicator init \
  --source "$SRC" \
  --target "$TGT" \
  --config replication-config.toml
```

The same config file can be passed to `sync` to ensure logical replication uses the identical rule set.

## Logical Replication Requirements

Table-level predicates in publications (generated for table/time filters) require PostgreSQL 15 or newer on the source. If the server is older, the tool will abort before creating the publication so you can remove the filters or upgrade the cluster.

Schema-only tables and filtered snapshots work on all supported PostgreSQL versions because they rely on pg_dump and streaming COPY operations during `init`.

## TimescaleDB Tips

- Mark hypertables storing many years of history as schema-only, then separately filter the recent chunks you need (e.g., `events:bucketed_at:6 months`).
- Exclude Timescale’s `_timescaledb_internal` or `timescaledb_information` schemas unless you explicitly need them; schema-only + time filters reduce restore time significantly.
- After the initial snapshot, the same predicates determine which tuples enter logical replication, so older data aging past the filter window remains untouched on the target.
