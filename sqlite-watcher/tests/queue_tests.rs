use sqlite_watcher::queue::{ChangeOperation, ChangeQueue, NewChange, QueueState};
use tempfile::tempdir;

fn new_change(table: &str, id: &str, op: ChangeOperation) -> NewChange {
    NewChange {
        table_name: table.to_string(),
        operation: op,
        primary_key: id.to_string(),
        payload: Some(format!("{{\"id\":\"{id}\"}}").into_bytes()),
        wal_frame: Some("frame1".to_string()),
        cursor: None,
    }
}

#[test]
fn durable_enqueue_and_ack_flow() {
    let dir = tempdir().unwrap();
    let queue_path = dir.path().join("changes.db");
    let queue = ChangeQueue::open(&queue_path).unwrap();

    let mut ids = Vec::new();
    for i in 0..3 {
        let change = new_change("vaults", &format!("pk-{i}"), ChangeOperation::Insert);
        ids.push(queue.enqueue(&change).unwrap());
    }

    let batch = queue.fetch_batch(10).unwrap();
    assert_eq!(batch.len(), 3);

    queue.ack_up_to(ids[1]).unwrap();
    queue.purge_acked().unwrap();

    drop(queue);

    let reopened = ChangeQueue::open(&queue_path).unwrap();
    let remaining = reopened.fetch_batch(10).unwrap();
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0].change_id, ids[2]);
}

#[test]
fn state_round_trip() {
    let dir = tempdir().unwrap();
    let queue_path = dir.path().join("state.db");
    let queue = ChangeQueue::open(&queue_path).unwrap();

    assert!(queue.get_state("prices").unwrap().is_none());

    let state = QueueState {
        table_name: "prices".into(),
        last_change_id: 42,
        last_wal_frame: Some("frame-42".into()),
        cursor: Some("cursor-data".into()),
    };
    queue.set_state(&state).unwrap();

    let fetched = queue.get_state("prices").unwrap().unwrap();
    assert_eq!(fetched, state);
}
