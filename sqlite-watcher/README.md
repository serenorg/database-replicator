# sqlite-watcher

Work-in-progress tooling for monitoring SQLite databases. This issue adds the durable change queue used by the watcher service. The queue stores row-level changes plus per-table checkpoints in `~/.seren/sqlite-watcher/changes.db` so restarts can resume from the last acknowledged WAL frame.

Run `cargo test -p sqlite-watcher` to execute the queue integration tests.
