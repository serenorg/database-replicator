use std::sync::mpsc::channel;
use std::time::Duration;

use rusqlite::Connection;
use sqlite_watcher::queue::ChangeOperation;
use sqlite_watcher::wal::{start_wal_watcher, WalWatcherConfig};
use tempfile::tempdir;

#[test]
fn integration_watcher_emits_changes() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("tailer.db");
    let writer = Connection::open(&db_path).unwrap();
    writer
        .execute_batch(
            r#"
            PRAGMA journal_mode=WAL;
            CREATE TABLE posts(id INTEGER PRIMARY KEY, title TEXT);
        "#,
        )
        .unwrap();

    let (tx, rx) = channel();
    let _handle = start_wal_watcher(
        &db_path,
        WalWatcherConfig {
            poll_interval: Duration::from_millis(100),
            min_event_bytes: 0,
        },
        tx,
    )
    .unwrap();

    std::thread::sleep(Duration::from_millis(200));

    writer
        .execute("INSERT INTO posts(title) VALUES ('hello')", [])
        .unwrap();
    let insert = rx.recv_timeout(Duration::from_secs(3)).unwrap();
    assert_eq!(insert.table_name, "posts");
    assert_eq!(insert.operation, ChangeOperation::Insert);

    writer
        .execute("UPDATE posts SET title='hi' WHERE id=1", [])
        .unwrap();
    let update = rx.recv_timeout(Duration::from_secs(3)).unwrap();
    assert_eq!(update.operation, ChangeOperation::Update);

    writer.execute("DELETE FROM posts WHERE id=1", []).unwrap();
    let delete = rx.recv_timeout(Duration::from_secs(3)).unwrap();
    assert_eq!(delete.operation, ChangeOperation::Delete);
}
