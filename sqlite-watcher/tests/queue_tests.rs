use sqlite_watcher::queue::{ChangeOperation, ChangeQueue, NewChange, QueueState};
use tempfile::tempdir;

fn new_change(table: &str, pk: &str, op: ChangeOperation) -> NewChange {
    NewChange {
        table_name: table.to_string(),
        operation: op,
        primary_key: pk.to_string(),
        payload: Some(format!("payload-{pk}").into_bytes()),
        wal_frame: Some("0001".to_string()),
        cursor: Some("cursor-1".to_string()),
    }
}

#[test]
fn queue_persists_changes_and_ack_flow() {
    let dir = tempdir().unwrap();
    let queue_path = dir.path().join("changes.db");
    let queue = ChangeQueue::open(&queue_path).unwrap();

    let mut ids = Vec::new();
    for idx in 0..3 {
        let change = new_change("vaults", &format!("pk-{idx}"), ChangeOperation::Insert);
        ids.push(queue.enqueue(&change).unwrap());
    }

    let batch = queue.fetch_batch(10).unwrap();
    assert_eq!(batch.len(), 3);
    assert_eq!(batch[0].change_id, ids[0]);
    assert_eq!(batch[1].primary_key, "pk-1");

    queue.ack_up_to(ids[0]).unwrap();
    let batch = queue.fetch_batch(10).unwrap();
    assert_eq!(batch.len(), 2);
    assert_eq!(batch[0].change_id, ids[1]);

    let removed = queue.vacuum_acknowledged().unwrap();
    assert_eq!(removed, 1);
    drop(queue);

    // Reopen to ensure durability.
    let queue = ChangeQueue::open(&queue_path).unwrap();
    let batch = queue.fetch_batch(10).unwrap();
    assert_eq!(batch.len(), 2);
    assert_eq!(batch[0].change_id, ids[1]);
}

#[test]
fn queue_state_round_trip() {
    let dir = tempdir().unwrap();
    let queue_path = dir.path().join("state.db");
    let queue = ChangeQueue::open(&queue_path).unwrap();

    assert!(queue.get_state("prices").unwrap().is_none());
    let state = QueueState {
        table_name: "prices".to_string(),
        last_change_id: 42,
        last_wal_frame: Some("abcdef".to_string()),
        cursor: Some(r#"{"timestamp":"2024-01-01T00:00:00Z"}"#.to_string()),
    };
    queue.set_state(&state).unwrap();
    let fetched = queue.get_state("prices").unwrap().unwrap();
    assert_eq!(fetched, state);

    let updated = QueueState {
        last_change_id: 55,
        ..state
    };
    queue.set_state(&updated).unwrap();
    let fetched = queue.get_state("prices").unwrap().unwrap();
    assert_eq!(fetched.last_change_id, 55);
}
