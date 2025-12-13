use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::Sender;
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::Duration;

use anyhow::{Context, Result};
use tracing::{debug, warn};

#[derive(Debug, Clone, Copy)]
pub struct WalWatcherConfig {
    pub poll_interval: Duration,
    pub min_event_bytes: u64,
}

impl Default for WalWatcherConfig {
    fn default() -> Self {
        Self {
            poll_interval: Duration::from_millis(500),
            min_event_bytes: 0,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalEvent {
    pub bytes_added: u64,
    pub current_size: u64,
}

pub struct WalWatcherHandle {
    stop: Arc<AtomicBool>,
    thread: Option<JoinHandle<()>>,
}

impl Drop for WalWatcherHandle {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        if let Some(handle) = self.thread.take() {
            let _ = handle.join();
        }
    }
}

pub fn start_wal_watcher<P: AsRef<Path>>(
    db_path: P,
    options: WalWatcherConfig,
    sender: Sender<WalEvent>,
) -> Result<WalWatcherHandle> {
    let db_path = db_path.as_ref().canonicalize().with_context(|| {
        format!(
            "failed to canonicalize database path {}",
            db_path.as_ref().display()
        )
    })?;
    if !db_path.is_file() {
        anyhow::bail!("database path {} is not a file", db_path.display());
    }

    let wal_path = wal_file_path(&db_path);
    let poll_interval = options.poll_interval;
    let min_event_bytes = options.min_event_bytes;
    let stop_flag = Arc::new(AtomicBool::new(false));
    let thread_stop = Arc::clone(&stop_flag);

    let handle = thread::spawn(move || {
        let mut last_len = wal_file_size(&wal_path).unwrap_or(0);
        debug!(
            wal = %wal_path.display(),
            last_len,
            "wal watcher started"
        );
        while !thread_stop.load(Ordering::SeqCst) {
            match wal_file_size(&wal_path) {
                Ok(len) => {
                    if len < last_len {
                        debug!(
                            wal = %wal_path.display(),
                            prev = last_len,
                            current = len,
                            "wal truncated; resetting baseline"
                        );
                        last_len = len;
                    } else if len > last_len {
                        let delta = len - last_len;
                        last_len = len;
                        if delta >= min_event_bytes {
                            let event = WalEvent {
                                bytes_added: delta,
                                current_size: len,
                            };
                            if sender.send(event).is_err() {
                                debug!("wal watcher stopping because receiver closed");
                                break;
                            }
                        }
                    }
                }
                Err(err) => {
                    if err.kind() == std::io::ErrorKind::NotFound {
                        last_len = 0;
                    } else {
                        warn!(
                            wal = %wal_path.display(),
                            error = %err,
                            "failed to read wal metadata"
                        );
                    }
                }
            }

            thread::sleep(poll_interval);
        }

        debug!("wal watcher exiting");
    });

    Ok(WalWatcherHandle {
        stop: stop_flag,
        thread: Some(handle),
    })
}

fn wal_file_path(db_path: &Path) -> PathBuf {
    let mut os_string = OsString::from(db_path.as_os_str());
    os_string.push("-wal");
    PathBuf::from(os_string)
}

fn wal_file_size(path: &Path) -> std::io::Result<u64> {
    std::fs::metadata(path).map(|m| m.len())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::Connection;
    use std::sync::mpsc::channel;
    use std::time::{Duration, Instant};
    use tempfile::tempdir;

    #[test]
    fn emits_event_when_wal_grows() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("watch.sqlite");
        let writer = Connection::open(&db_path).unwrap();
        writer.pragma_update(None, "journal_mode", &"wal").unwrap();
        writer
            .pragma_update(None, "wal_autocheckpoint", &0i64)
            .unwrap();
        writer
            .execute(
                "CREATE TABLE changes(id INTEGER PRIMARY KEY, value TEXT)",
                [],
            )
            .unwrap();

        let (tx, rx) = channel();
        let handle = start_wal_watcher(
            &db_path,
            WalWatcherConfig {
                poll_interval: Duration::from_millis(50),
                min_event_bytes: 1,
            },
            tx,
        )
        .unwrap();

        for i in 0..50 {
            writer
                .execute(
                    "INSERT INTO changes(value) VALUES (?1)",
                    [format!("value-{i}")],
                )
                .unwrap();
        }

        let event = rx.recv_timeout(Duration::from_secs(5)).unwrap();
        assert!(event.bytes_added > 0);
        assert!(event.current_size >= event.bytes_added);

        drop(handle);
    }

    #[test]
    fn handles_wal_truncation() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("truncate.sqlite");
        let writer = Connection::open(&db_path).unwrap();
        writer.pragma_update(None, "journal_mode", &"wal").unwrap();
        writer
            .pragma_update(None, "wal_autocheckpoint", &0i64)
            .unwrap();
        writer
            .execute("CREATE TABLE stuff(id INTEGER PRIMARY KEY, value TEXT)", [])
            .unwrap();

        let (tx, rx) = channel();
        let handle = start_wal_watcher(
            &db_path,
            WalWatcherConfig {
                poll_interval: Duration::from_millis(25),
                min_event_bytes: 1,
            },
            tx,
        )
        .unwrap();

        for i in 0..10 {
            writer
                .execute("INSERT INTO stuff(value) VALUES (?1)", [format!("row-{i}")])
                .unwrap();
        }

        rx.recv_timeout(Duration::from_secs(5)).unwrap();

        writer
            .execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
            .unwrap();

        // Ensure watcher does not send negative deltas (would panic or overflow)
        let start = Instant::now();
        loop {
            if rx.recv_timeout(Duration::from_millis(100)).is_ok() {
                break;
            }
            if start.elapsed() > Duration::from_millis(500) {
                break;
            }
        }

        drop(handle);
    }
}
