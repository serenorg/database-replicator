use std::net::SocketAddr;
use std::time::Duration;

use sqlite_watcher::server::TcpServerHandle;
use sqlite_watcher::watcher_proto::watcher_client::WatcherClient;
use sqlite_watcher::watcher_proto::HealthCheckRequest;
use tempfile::tempdir;
use tokio::time::sleep;
use tonic::metadata::MetadataValue;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn health_check_responds_ok() {
    let dir = tempdir().unwrap();
    let queue_path = dir.path().join("queue.db");
    let addr: SocketAddr = "127.0.0.1:55051".parse().unwrap();
    let token = "secret-token".to_string();

    let _handle = TcpServerHandle::spawn(addr, queue_path, token.clone()).unwrap();
    sleep(Duration::from_millis(200)).await;

    let channel = tonic::transport::Channel::from_shared(format!("http://{}", addr))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut client = WatcherClient::new(channel);
    let mut req = tonic::Request::new(HealthCheckRequest {});
    let header = MetadataValue::try_from(format!("Bearer {}", token)).unwrap();
    req.metadata_mut().insert("authorization", header);
    let resp = client.health_check(req).await.unwrap();
    assert_eq!(resp.into_inner().status, "ok");
}
