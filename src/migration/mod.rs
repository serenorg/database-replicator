// ABOUTME: Migration utilities module
// ABOUTME: Handles schema introspection, dump/restore, and data migration

pub mod checksum;
pub mod dump;
pub mod estimation;
pub mod filtered;
pub mod restore;
pub mod schema;

pub use checksum::{compare_tables, compute_table_checksum, ChecksumResult};
pub use dump::{dump_data, dump_globals, dump_schema};
pub use estimation::{estimate_database_sizes, format_bytes, format_duration, DatabaseSizeInfo};
pub use filtered::copy_filtered_tables;
pub use restore::{restore_data, restore_globals, restore_schema};
pub use schema::{
    get_table_columns, list_databases, list_tables, ColumnInfo, DatabaseInfo, TableInfo,
};
