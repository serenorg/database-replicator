// ABOUTME: xmin-based sync module for incremental PostgreSQL replication
// ABOUTME: Provides change detection using PostgreSQL's xmin system column

pub mod daemon;
pub mod reader;
pub mod reconciler;
pub mod state;
pub mod writer;

pub use daemon::{DaemonConfig, SyncDaemon, SyncStats};
pub use reader::{detect_wraparound, BatchReader, ColumnInfo, WraparoundCheck, XminReader};
pub use reconciler::{ReconcileConfig, ReconcileResult, Reconciler};
pub use state::{SyncState, TableSyncState};
pub use writer::{get_primary_key_columns, get_table_columns, row_to_values, ChangeWriter};
