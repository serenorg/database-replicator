use std::net::SocketAddr;
use std::time::Duration;

use sqlite_watcher::queue::{ChangeOperation, ChangeQueue, NewChange};
use sqlite_watcher::server::spawn_tcp;
use sqlite_watcher::watcher_proto::watcher_client::WatcherClient;
use sqlite_watcher::watcher_proto::{AckChangesRequest, HealthCheckRequest, ListChangesRequest};
use tempfile::tempdir;
use tokio::time::sleep;
use tonic::metadata::MetadataValue;

fn seed_queue(path: &str) {
    let queue = ChangeQueue::open(path).unwrap();
    for i in 0..2 {
        let change = NewChange {
            table_name: "examples".into(),
            operation: ChangeOperation::Insert,
            primary_key: format!("row-{i}"),
            payload: None,
            wal_frame: None,
            cursor: None,
        };
        queue.enqueue(&change).unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tcp_server_handles_health_and_list() {
    let dir = tempdir().unwrap();
    let queue_path = dir.path().join("queue.db");
    seed_queue(queue_path.to_str().unwrap());

    let addr: SocketAddr = "127.0.0.1:56060".parse().unwrap();
    let token = "secret".to_string();
    let _handle = spawn_tcp(addr, queue_path, token.clone()).unwrap();
    sleep(Duration::from_millis(200)).await;

    let channel = tonic::transport::Channel::from_shared(format!("http://{}", addr))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut client = WatcherClient::new(channel);

    let mut health_req = tonic::Request::new(HealthCheckRequest {});
    let header = MetadataValue::try_from(format!("Bearer {}", token)).unwrap();
    health_req
        .metadata_mut()
        .insert("authorization", header.clone());
    client.health_check(health_req).await.unwrap();

    let mut list_req = tonic::Request::new(ListChangesRequest { limit: 10 });
    list_req.metadata_mut().insert("authorization", header);
    let resp = client.list_changes(list_req).await.unwrap();
    assert_eq!(resp.into_inner().changes.len(), 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unauthenticated_requests_fail() {
    let dir = tempdir().unwrap();
    let queue_path = dir.path().join("queue.db");
    let addr: SocketAddr = "127.0.0.1:56061".parse().unwrap();
    let token = "secret".to_string();
    let _handle = spawn_tcp(addr, queue_path, token).unwrap();
    sleep(Duration::from_millis(200)).await;

    let channel = tonic::transport::Channel::from_shared(format!("http://{}", addr))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut client = WatcherClient::new(channel);

    let request = tonic::Request::new(ListChangesRequest { limit: 1 });
    let err = client.list_changes(request).await.unwrap_err();
    assert_eq!(err.code(), tonic::Code::Unauthenticated);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ack_changes_advances_queue() {
    let dir = tempdir().unwrap();
    let queue_path = dir.path().join("queue.db");
    seed_queue(queue_path.to_str().unwrap());
    let addr: SocketAddr = "127.0.0.1:56062".parse().unwrap();
    let token = "secret".to_string();
    let _handle = spawn_tcp(addr, queue_path, token.clone()).unwrap();
    sleep(Duration::from_millis(200)).await;

    let channel = tonic::transport::Channel::from_shared(format!("http://{}", addr))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut client = WatcherClient::new(channel);
    let header = MetadataValue::try_from(format!("Bearer {}", token)).unwrap();

    let mut req = tonic::Request::new(ListChangesRequest { limit: 10 });
    req.metadata_mut().insert("authorization", header.clone());
    let resp = client.list_changes(req).await.unwrap().into_inner();
    assert_eq!(resp.changes.len(), 2);
    let highest = resp.changes.last().unwrap().change_id;

    let mut ack_req = tonic::Request::new(AckChangesRequest {
        up_to_change_id: highest,
    });
    ack_req.metadata_mut().insert("authorization", header);
    client.ack_changes(ack_req).await.unwrap();
}
