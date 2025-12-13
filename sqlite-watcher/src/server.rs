use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::thread::{self, JoinHandle};

use anyhow::{Context, Result};
use tokio::runtime::Builder;
use tokio::sync::oneshot;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::service::Interceptor;
use tonic::{transport::Server, Request, Response, Status};

#[cfg(unix)]
use tokio::net::UnixListener;
#[cfg(unix)]
use tokio_stream::wrappers::UnixListenerStream;

use crate::queue::{ChangeQueue, QueueState};
use crate::watcher_proto::watcher_server::{Watcher, WatcherServer};
use crate::watcher_proto::{
    AckChangesRequest, AckChangesResponse, Change, GetStateRequest, GetStateResponse,
    HealthCheckRequest, HealthCheckResponse, ListChangesRequest, ListChangesResponse,
    SetStateRequest, SetStateResponse,
};

pub enum ServerHandle {
    Tcp {
        shutdown: Option<oneshot::Sender<()>>,
        thread: Option<JoinHandle<Result<()>>>,
    },
    #[cfg(unix)]
    Unix {
        shutdown: Option<oneshot::Sender<()>>,
        thread: Option<JoinHandle<Result<()>>>,
        path: PathBuf,
    },
}

impl Drop for ServerHandle {
    fn drop(&mut self) {
        match self {
            ServerHandle::Tcp { shutdown, thread } => {
                if let Some(tx) = shutdown.take() {
                    let _ = tx.send(());
                }
                if let Some(handle) = thread.take() {
                    let _ = handle.join();
                }
            }
            #[cfg(unix)]
            ServerHandle::Unix {
                shutdown,
                thread,
                path,
            } => {
                if let Some(tx) = shutdown.take() {
                    let _ = tx.send(());
                }
                if let Some(handle) = thread.take() {
                    let _ = handle.join();
                }
                let _ = std::fs::remove_file(path);
            }
        }
    }
}

pub fn spawn_tcp(addr: SocketAddr, queue_path: PathBuf, token: String) -> Result<ServerHandle> {
    let (tx, rx) = oneshot::channel();
    let thread = thread::spawn(move || -> Result<()> {
        let rt = Builder::new_multi_thread().enable_all().build()?;
        rt.block_on(async move {
            let listener = tokio::net::TcpListener::bind(addr)
                .await
                .context("failed to bind tcp listener")?;
            let service = WatcherService::new(queue_path);
            let interceptor = AuthInterceptor::new(token);
            Server::builder()
                .add_service(WatcherServer::with_interceptor(service, interceptor))
                .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async move {
                    let _ = rx.await;
                })
                .await
                .context("grpc server exited")
        })
    });
    Ok(ServerHandle::Tcp {
        shutdown: Some(tx),
        thread: Some(thread),
    })
}

#[cfg(unix)]
pub fn spawn_unix(path: &Path, queue_path: PathBuf, token: String) -> Result<ServerHandle> {
    if path.exists() {
        std::fs::remove_file(path)
            .with_context(|| format!("failed to remove stale socket {}", path.display()))?;
    }
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("failed to create socket dir {}", parent.display()))?;
    }
    let socket_path = path.to_path_buf();
    let (tx, rx) = oneshot::channel();
    let path_clone = socket_path.clone();
    let thread = thread::spawn(move || -> Result<()> {
        let rt = Builder::new_multi_thread().enable_all().build()?;
        rt.block_on(async move {
            let listener = UnixListener::bind(&path_clone).context("failed to bind unix socket")?;
            let service = WatcherService::new(queue_path);
            let interceptor = AuthInterceptor::new(token);
            Server::builder()
                .add_service(WatcherServer::with_interceptor(service, interceptor))
                .serve_with_incoming_shutdown(UnixListenerStream::new(listener), async move {
                    let _ = rx.await;
                })
                .await
                .context("grpc server exited")
        })
    });
    Ok(ServerHandle::Unix {
        shutdown: Some(tx),
        thread: Some(thread),
        path: socket_path,
    })
}

#[derive(Clone)]
struct WatcherService {
    queue_path: Arc<PathBuf>,
}

impl WatcherService {
    fn new(queue_path: PathBuf) -> Self {
        Self {
            queue_path: Arc::new(queue_path),
        }
    }

    fn queue(&self) -> Result<ChangeQueue> {
        ChangeQueue::open(&*self.queue_path)
    }
}

#[derive(Clone)]
struct AuthInterceptor {
    token: Arc<String>,
}

impl AuthInterceptor {
    fn new(token: String) -> Self {
        Self {
            token: Arc::new(token),
        }
    }
}

impl Interceptor for AuthInterceptor {
    fn call(&mut self, request: Request<()>) -> Result<Request<()>, Status> {
        let provided = request
            .metadata()
            .get("authorization")
            .ok_or_else(|| Status::unauthenticated("missing authorization header"))?;
        let expected = format!("Bearer {}", self.token.as_str());
        if provided == expected.as_str() {
            Ok(request)
        } else {
            Err(Status::unauthenticated("invalid authorization header"))
        }
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
        let limit = request.get_ref().limit.clamp(1, 10_000) as usize;
        let queue = self.queue().map_err(internal_err)?;
        let rows = queue.fetch_batch(limit).map_err(internal_err)?;
        let changes = rows.into_iter().map(change_to_proto).collect();
        Ok(Response::new(ListChangesResponse { changes }))
    }

    async fn ack_changes(
        &self,
        request: Request<AckChangesRequest>,
    ) -> Result<Response<AckChangesResponse>, Status> {
        let upto = request.get_ref().up_to_change_id;
        let queue = self.queue().map_err(internal_err)?;
        let count = queue.ack_up_to(upto).map_err(internal_err)?;
        Ok(Response::new(AckChangesResponse {
            acknowledged: count,
        }))
    }

    async fn get_state(
        &self,
        request: Request<GetStateRequest>,
    ) -> Result<Response<GetStateResponse>, Status> {
        let queue = self.queue().map_err(internal_err)?;
        let state = queue
            .get_state(&request.get_ref().table_name)
            .map_err(internal_err)?;
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
        let queue = self.queue().map_err(internal_err)?;
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
        queue.set_state(&state).map_err(internal_err)?;
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

fn internal_err(err: anyhow::Error) -> Status {
    Status::internal(err.to_string())
}
