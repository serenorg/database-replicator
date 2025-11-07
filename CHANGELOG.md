# Changelog

All notable changes to this project will be documented in this file.

## [1.2.0] - 2025-11-07
### Added
- Table-level replication rules with new CLI flags (`--schema-only-tables`, `--table-filter`, `--time-filter`) and TOML config support (`--config`).
- Filtered snapshot pipeline that streams predicate-matching rows and skips schema-only tables during `init`.
- Predicate-aware publications for `sync`, enabling logical replication that respects table filters on PostgreSQL 15+.
- TimescaleDB-focused documentation at `docs/replication-config.md` plus expanded README guidance.

### Improved
- Multi-database `init` now checkpoints per-table progress and resumes from the last successful database.

## [1.1.1] - 2025-11-07
- Previous improvements and fixes bundled with v1.1.1.
