# sqlite-watcher (alpha)

This crate currently ships the shared queue + gRPC server used by `database-replicator sync-sqlite`. The `sqlite-watcher` binary includes:

- `serve`: start the queue-backed gRPC API so clients can pull change batches.
- `enqueue`: helper for tests/smoke scripts to add sample changes to the queue database.

> **Note:** WAL tailing is still under active development; use the binary today to test queue + sync flows.

See `docs/installers.md` for per-OS service guidance and `scripts/test-sqlite-delta.sh` for the end-to-end smoke test.
