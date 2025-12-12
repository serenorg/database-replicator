pub mod change;
pub mod queue;
pub mod server;
pub mod wal;

pub mod watcher_proto {
    tonic::include_proto!("sqlitewatcher");
}
