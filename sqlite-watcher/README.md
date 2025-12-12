# sqlite-watcher

This crate provides the building blocks for an upcoming sqlite-watcher binary. Issue #82 adds a durable queue plus a tonic-based gRPC server so other components can stream captured SQLite changes.

## Components

- `queue.rs`: stores change rows and per-table checkpoints in `~/.seren/sqlite-watcher/changes.db`.
- `proto/watcher.proto`: RPC definitions (`HealthCheck`, `ListChanges`, `AckChanges`, `GetState`, `SetState`).
- `server.rs`: tonic server wrappers exposing the queue over TCP or Unix sockets with shared-secret authentication.

## Building & Testing

```bash
cargo test -p sqlite-watcher
```

The tests cover queue durability/state behavior. Server tests will be added once the consumer wiring lands.
