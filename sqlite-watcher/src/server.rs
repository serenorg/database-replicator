use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::thread::{self, JoinHandle};

use anyhow::{Context, Result};
use tokio::runtime::Builder;
use tokio::sync::oneshot;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::service::Interceptor;
use tonic::{transport::Server, Request, Response, Status};

use crate::queue::{ChangeQueue, QueueState};
use crate::watcher_proto::watcher_server::{Watcher, WatcherServer};
use crate::watcher_proto::{
    AckChangesRequest, AckChangesResponse, Change, GetStateRequest, GetStateResponse,
    HealthCheckRequest, HealthCheckResponse, ListChangesRequest, ListChangesResponse,
    SetStateRequest, SetStateResponse,
};

pub struct TcpServerHandle {
    shutdown: Option<oneshot::Sender<()>>,
    thread: Option<JoinHandle<Result<()>>>,
}

impl Drop for TcpServerHandle {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
        if let Some(handle) = self.thread.take() {
            let _ = handle.join();
        }
    }
}

impl TcpServerHandle {
    pub fn spawn(addr: SocketAddr, queue_path: PathBuf, token: String) -> Result<Self> {
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let thread = thread::spawn(move || -> Result<()> {
            let runtime = Builder::new_multi_thread()
                .enable_all()
                .build()
                .context("failed to build tokio runtime")?;
            runtime.block_on(async move {
                let listener = tokio::net::TcpListener::bind(addr)
                    .await
                    .context("failed to bind tcp listener")?;
                let queue_path = Arc::new(queue_path);
                let svc = WatcherService::new(queue_path.clone());
                let interceptor = AuthInterceptor {
                    token: Arc::new(token),
                };
                Server::builder()
                    .add_service(WatcherServer::with_interceptor(svc, interceptor))
                    .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async move {
                        let _ = shutdown_rx.await;
                    })
                    .await
                    .context("grpc server exited with error")?;
                Ok(())
            })
        });

        Ok(Self {
            shutdown: Some(shutdown_tx),
            thread: Some(thread),
        })
    }
}

struct WatcherService {
    queue_path: Arc<PathBuf>,
}

impl WatcherService {
    fn new(queue_path: Arc<PathBuf>) -> Self {
        Self { queue_path }
    }

    fn open_queue(&self) -> Result<ChangeQueue> {
        ChangeQueue::open(&*self.queue_path)
    }
}

#[tonic::async_trait]
impl Watcher for WatcherService {
    async fn health_check(
        &self,
        _: Request<HealthCheckRequest>,
    ) -> Result<Response<HealthCheckResponse>, Status> {
        Ok(Response::new(HealthCheckResponse {
            status: "ok".to_string(),
        }))
    }

    async fn list_changes(
        &self,
        request: Request<ListChangesRequest>,
    ) -> Result<Response<ListChangesResponse>, Status> {
        let limit = request.get_ref().limit.max(1).min(10_000) as usize;
        let queue = self
            .open_queue()
            .map_err(|err| Status::internal(err.to_string()))?;
        let rows = queue
            .fetch_batch(limit)
            .map_err(|err| Status::internal(err.to_string()))?;
        let changes = rows.into_iter().map(change_to_proto).collect();
        Ok(Response::new(ListChangesResponse { changes }))
    }

    async fn ack_changes(
        &self,
        request: Request<AckChangesRequest>,
    ) -> Result<Response<AckChangesResponse>, Status> {
        let upto = request.get_ref().up_to_change_id;
        let queue = self
            .open_queue()
            .map_err(|err| Status::internal(err.to_string()))?;
        let count = queue
            .ack_up_to(upto)
            .map_err(|err| Status::internal(err.to_string()))?;
        Ok(Response::new(AckChangesResponse {
            acknowledged: count,
        }))
    }

    async fn get_state(
        &self,
        request: Request<GetStateRequest>,
    ) -> Result<Response<GetStateResponse>, Status> {
        let name = request.get_ref().table_name.clone();
        let queue = self
            .open_queue()
            .map_err(|err| Status::internal(err.to_string()))?;
        let state = queue
            .get_state(&name)
            .map_err(|err| Status::internal(err.to_string()))?;
        let resp = match state {
            Some(state) => GetStateResponse {
                exists: true,
                last_change_id: state.last_change_id,
                last_wal_frame: state.last_wal_frame.unwrap_or_default(),
                cursor: state.cursor.unwrap_or_default(),
            },
            None => GetStateResponse {
                exists: false,
                last_change_id: 0,
                last_wal_frame: String::new(),
                cursor: String::new(),
            },
        };
        Ok(Response::new(resp))
    }

    async fn set_state(
        &self,
        request: Request<SetStateRequest>,
    ) -> Result<Response<SetStateResponse>, Status> {
        let payload = request.into_inner();
        if payload.table_name.is_empty() {
            return Err(Status::invalid_argument("table_name is required"));
        }
        let queue = self
            .open_queue()
            .map_err(|err| Status::internal(err.to_string()))?;
        let state = QueueState {
            table_name: payload.table_name,
            last_change_id: payload.last_change_id,
            last_wal_frame: if payload.last_wal_frame.is_empty() {
                None
            } else {
                Some(payload.last_wal_frame)
            },
            cursor: if payload.cursor.is_empty() {
                None
            } else {
                Some(payload.cursor)
            },
        };
        queue
            .set_state(&state)
            .map_err(|err| Status::internal(err.to_string()))?;
        Ok(Response::new(SetStateResponse {}))
    }
}

fn change_to_proto(row: crate::queue::ChangeRecord) -> Change {
    Change {
        change_id: row.change_id,
        table_name: row.table_name,
        op: row.operation.as_str().to_string(),
        primary_key: row.primary_key,
        payload: row.payload.unwrap_or_default(),
        wal_frame: row.wal_frame.unwrap_or_default(),
        cursor: row.cursor.unwrap_or_default(),
    }
}

#[derive(Clone)]
struct AuthInterceptor {
    token: Arc<String>,
}

impl Interceptor for AuthInterceptor {
    fn call(&mut self, request: Request<()>) -> Result<Request<()>, Status> {
        let header = request
            .metadata()
            .get("authorization")
            .ok_or_else(|| Status::unauthenticated("missing authorization header"))?;
        let expected = format!("Bearer {}", self.token.as_ref());
        if header
            .to_str()
            .map(|value| value == expected)
            .unwrap_or(false)
        {
            Ok(request)
        } else {
            Err(Status::unauthenticated("invalid authorization header"))
        }
    }
}
