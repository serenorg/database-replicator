use std::net::SocketAddr;
use std::time::Duration;

use sqlite_watcher::server::spawn_tcp_server;
#[cfg(unix)]
use sqlite_watcher::server::spawn_unix_server;
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

    let _handle = spawn_tcp_server(addr, queue_path, token.clone()).unwrap();
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
#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn health_check_over_unix_socket() {
    use tokio::net::UnixStream;
    use tonic::transport::Endpoint;
    use tower::service_fn;

    let dir = tempdir().unwrap();
    let queue_path = dir.path().join("queue.db");
    let socket_path = dir.path().join("watcher.sock");
    let token = "secret-token".to_string();

    let _handle = spawn_unix_server(&socket_path, queue_path, token.clone()).unwrap();
    sleep(Duration::from_millis(200)).await;

    let endpoint = Endpoint::try_from("http://[::]:50051").unwrap();
    let channel = endpoint
        .connect_with_connector(service_fn(move |_: tonic::transport::Uri| {
            let path = socket_path.clone();
            async move { UnixStream::connect(path).await }
        }))
        .await
        .unwrap();
    let mut client = WatcherClient::new(channel);
    let mut req = tonic::Request::new(HealthCheckRequest {});
    let header = MetadataValue::try_from(format!("Bearer {}", token)).unwrap();
    req.metadata_mut().insert("authorization", header);
    let resp = client.health_check(req).await.unwrap();
    assert_eq!(resp.into_inner().status, "ok");
}
