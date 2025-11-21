# Schema-Aware Filters & Filtered Snapshot Safety Plan

> Created: 2025-11-09 (YYYY-MM-DD)

## Problem Statement
v1.2.0 introduces table-level replication rules, but the current implementation strips schema context and cannot handle tables with identical names across schemas. Filtered snapshots also fail when tables participate in foreign-key chains because we truncate without cascading. We must implement schema-aware filtering/parsing/storage and switch filtered-copy truncation to `TRUNCATE ... CASCADE` with guardrails before documenting the release.

## Goals
- Accept and persist `[database.]schema.table` identifiers (while defaulting schema to `public` for backward compatibility).
- Ensure every filtering surface (CLI flags, TOML config, publications, pg_dump include/exclude, size estimator, checkpoints) understands schema-aware rules.
- Keep checkpoint fingerprints stable but schema-sensitive so resumed runs do not replay stale instructions.
- Make filtered snapshots safe for FK-linked tables by cascading truncates and making the blast radius explicit.

## Non-Goals
- Changing replication behavior for non-filtered tables.
- Supporting mixed-case identifiers without quoting (we will continue to normalize unless quoted input demands exact casing).

## Design Outline
1. **Identifier Parsing + Normalization**
   - Introduce a shared `QualifiedTable` struct `{ database: Option<String>, schema: String, table: String }` with helpers to parse from CLI strings (`db.schema.table`, `schema.table`, `table`).
   - Update `TableRuleArgs` parsing and `config::load_table_rules_from_file` so TOML entries can specify `schema = "analytics"` explicitly; keep support for legacy `table = "orders"` by assuming `public`.
   - Store schema names in lower case unless the input was double-quoted; reuse `quote_ident` for round-tripping.
2. **Storage & Fingerprinting**
   - Refactor `TableRules` maps to key by `(ScopeKey, schema, table)` instead of just `table`.
   - Update `ReplicationFilter`’s include/exclude vectors to preserve schema-qualified strings and use them in `should_replicate_table`.
   - Include schema in checkpoint fingerprints and `filter_hash` so resuming with different schema scope forces a fresh run.
3. **Filter Application Points**
   - `migration::dump::{get_included_tables_for_db,get_excluded_tables_for_db}`: emit schema-qualified `"schema"."table"` names for pg_dump.
   - `migration::filtered::copy_filtered_tables`: parse schema from qualified table names and query FK CASCADE targets.
   - `replication::publication`: ensure publication creation uses schema-qualified table names.
   - Size estimator: query table sizes with schema-qualified names.

4. **FK-Safe CASCADE Truncation**
   - Add `parse_schema_table()` to extract (schema, table) from qualified names.
   - Add `get_cascade_targets()` to recursively find all FK-related tables that would be affected by TRUNCATE CASCADE.
   - Show blast radius to user before truncating.
   - Verify all CASCADE targets are included in replication scope to prevent data loss.

5. **Testing & Documentation**
   - Unit tests for QualifiedTable parsing and SchemaTableKey storage.
   - Integration tests for FK CASCADE detection and filtered copy.
   - Fingerprint tests to verify schema changes invalidate checkpoints.
   - README documentation with schema-aware examples.
   - CLAUDE.md technical documentation for contributors.

## Implementation Phases

### Phase 1: QualifiedTable struct (Issue #102) ✅ COMPLETED
- Created `QualifiedTable` struct with `parse()`, `with_database()`, `qualified_name()` methods
- Added `SchemaTableKey` type alias `(String, String)` for internal storage
- Implemented backward compatibility: defaults to `public` schema when not specified

### Phase 2: TableRules storage refactoring (Issue #103) ✅ COMPLETED
- Refactored `TableRules` to use `SchemaTableKey` instead of plain table names
- Updated all storage maps to use `(schema, table)` tuples
- Added schema-aware query methods: `schema_only_tables()`, `table_filter()`, `time_filter()`

### Phase 3: TOML config schema support (Issue #104) ✅ COMPLETED
- Added optional `schema` field to `TableFilterConfig` and `TimeFilterConfig`
- Updated parser to handle both explicit schema field and dot notation
- Maintained backward compatibility for configs without schema field

### Phase 4: Filter application (Issue #105) ✅ COMPLETED
- Updated `migration::dump` to emit schema-qualified table names for pg_dump
- Updated size estimation to use schema-qualified queries
- Updated publication creation to use schema-qualified table lists

### Phase 5: FK-safe CASCADE truncation (Issue #106) ✅ COMPLETED
- Implemented `parse_schema_table()` helper function
- Implemented `get_cascade_targets()` with recursive FK query
- Added blast radius display and safety checks
- Added comprehensive integration tests for FK detection

### Phase 6: Checkpoint fingerprints (Issue #107) ✅ COMPLETED
- Verified existing implementation includes schema in fingerprints via SchemaTableKey
- Added comprehensive tests to validate fingerprint behavior
- Confirmed schema changes invalidate checkpoints as expected

### Phase 7: Tests and documentation (Issue #108) ✅ COMPLETED
- All unit tests passing with comprehensive schema-aware coverage
- Integration tests added for FK CASCADE scenarios
- README updated with schema-aware filtering section and examples
- CLAUDE.md updated with technical implementation details
- Troubleshooting section added for FK CASCADE errors

## Status: ✅ COMPLETED

All phases of schema-aware filtering implementation have been completed successfully. The tool now:
- Supports schema-qualified table identifiers (`schema.table`)
- Maintains backward compatibility (defaults to `public` schema)
- Handles FK relationships safely with CASCADE truncation
- Validates checkpoint fingerprints include schema information
- Provides comprehensive documentation for users and contributors

**Completed:** 2025-11-09
**Issues Closed:** #102, #103, #104, #105, #106, #107, #108
