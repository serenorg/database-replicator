# sqlite-watcher

`sqlite-watcher` tails SQLite WAL files and exposes change streams that `database-replicator` can consume for incremental syncs. The current milestone focuses on a CLI skeleton plus a WAL-growth watcher loop so we can exercise configuration, logging, and packaging before wiring in the change queue + gRPC service described in `docs/plans/sqlite-watcher-plan.md`.

## Building

```bash
cargo build -p sqlite-watcher
```

This crate participates in the main workspace, so `cargo build --workspace` or `cargo test --workspace` will also compile it.

## CLI usage

```bash
sqlite-watcher \
  --db /path/to/database.db \
  --listen unix:/tmp/sqlite-watcher.sock \
  --token-file ~/.seren/sqlite-watcher/token \
  --log-level info \
  --queue-db ~/.seren/sqlite-watcher/changes.db \
  --poll-interval-ms 250 \
  --min-event-bytes 4096
```

Flag summary:

- `--db` (required): SQLite file to monitor; must exist and be accessible in WAL mode.
- `--listen`: Listener endpoint; accepts `unix:/path`, `tcp:<port>`, or `pipe:<name>`.
- `--token-file`: Shared-secret used to authenticate gRPC clients (defaults to `~/.seren/sqlite-watcher/token`).
- `--queue-db`: SQLite file used to persist change events + checkpoints (defaults to `~/.seren/sqlite-watcher/changes.db`).
- `--log-level`: Tracing filter (also settable via `SQLITE_WATCHER_LOG`).
- `--poll-interval-ms`: How often to check the WAL file for growth (default 500 ms). Lower values react faster but cost more syscalls.
- `--min-event-bytes`: Minimum WAL byte growth before emitting an event. Use larger values to avoid spam when very small transactions occur.
- `--listen` + `--token-file` now control the embedded gRPC server. Clients must send `Authorization: Bearer <token>` metadata when calling the `Watcher` service (see `proto/watcher.proto`). TCP (`tcp:50051`) and Unix sockets (`unix:/tmp/sqlite-watcher.sock`) are available today; Windows named pipes currently fall back to TCP until native support lands.

## Cross-platform notes

- **Linux/macOS**: Default listener is a Unix domain socket at `/tmp/sqlite-watcher.sock`. The watcher cleans up stale socket files on startup; point `--listen unix:/path` elsewhere if needed.
- **Windows**: Unix sockets are disabled; pass `--listen tcp:50051` or `--listen pipe:SerenWatcher`. Named pipes allow local service accounts without opening TCP ports.
- All platforms expect the token file to live under `~/.seren/sqlite-watcher/token` by default; create the directory with `0700` permissions so the watcher refuses to start if the secret is world-readable.
- The current WAL watcher polls the `*.sqlite-wal` file for byte growth. To keep WAL history available, configure your writers with `PRAGMA journal_mode=WAL;` and raise `wal_autocheckpoint` (or disable it) so the SQLite engine does not aggressively truncate the log.
- Change queue data is stored under `~/.seren/sqlite-watcher/changes.db`. The binary enforces owner-only permissions on that directory to keep tokens + change data private.

### Row change format

Each WAL commit triggers a snapshot diff across user tables. sqlite-watcher emits `RowChange` structs for inserts, updates, and deletes using declared primary keys (falling back to `rowid` when none exists). Payloads contain the latest row image so downstream consumers can apply upserts or tombstones without touching customer schemas.

Additional design constraints and follow-up work items live in `docs/plans/sqlite-watcher-plan.md` and `docs/plans/sqlite-watcher-tickets.md`.
