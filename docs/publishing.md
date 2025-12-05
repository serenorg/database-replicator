# Publishing to crates.io

1. **Prepare a clean release checkout**
   ```bash
   git fetch origin --tags
   git checkout vX.Y.Z   # replace with the release tag
   cargo clean
   ```

2. **Verify metadata and package contents**
   ```bash
   cargo package
   ```
   Inspect the generated tarball under `target/package/` to confirm the right files are included.

3. **Authenticate with crates.io**
   ```bash
   cargo login $CARGO_REGISTRY_TOKEN
   ```

4. **Publish**
   ```bash
   cargo publish
   ```
   Publishing may take a few minutes before the crate appears in search results.

5. **Tag/Release**
   Ensure the git tag matches the published version and update release notes.
