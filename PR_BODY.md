## Summary
- vendor `protoc` via `protoc-bin-vendored` so sqlite-watcher can build on runners without system protobuf
- update build script to set `PROTOC` before invoking `tonic_build`
- refresh Cargo.lock to capture the new dependencies

## Testing
- cargo clippy
- cargo test
