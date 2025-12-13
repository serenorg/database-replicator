## Summary
- fix sqlite sync change-state handling by treating watcher wal_frame/cursor fields as optional strings and cleaning up unused code
- implement FromStr for sqlite ChangeOperation and resolve needless borrow lints in queue/server modules
- keep clippy happy by applying the suggested clamp change and ensuring proto tests build

## Testing
- cargo clippy
- cargo test
