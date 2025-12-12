use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use clap::ValueEnum;
use sqlite_watcher::watcher_proto::watcher_client::WatcherClient;
use sqlite_watcher::watcher_proto::{
    AckChangesRequest, GetStateRequest, HealthCheckRequest, ListChangesRequest, SetStateRequest,
};
use tokio_postgres::Client;
use tonic::codegen::InterceptedService;
use tonic::service::Interceptor;
use tonic::transport::{Channel, Endpoint};
use tonic::{Request, Status};
use tower::service_fn;

use crate::jsonb::writer::{delete_jsonb_rows, insert_jsonb_batch, upsert_jsonb_rows};

const DEFAULT_BATCH_LIMIT: u32 = 500;
const GLOBAL_STATE_KEY: &str = "_global";

#[derive(Debug, Clone, Copy, ValueEnum, PartialEq, Eq)]
pub enum IncrementalMode {
    Append,
    AppendDeduped,
}

pub struct SyncSqliteOptions {
    pub target: String,
    pub watcher_endpoint: String,
    pub token_file: Option<PathBuf>,
    pub incremental_mode: IncrementalMode,
    pub batch_size: u32,
}

pub async fn run(opts: SyncSqliteOptions) -> Result<()> {
    let token = load_token(opts.token_file.as_deref())?;
    let endpoint = WatcherEndpoint::parse(&opts.watcher_endpoint)?;
    let mut watcher = connect_watcher(endpoint, token.clone()).await?;

    let client = crate::postgres::connect(&opts.target)
        .await
        .context("failed to connect to target PostgreSQL")?;
    ensure_state_table(&client).await?;
    ensure_baseline_exists(&client).await?;

    tracing::info!("Connecting to sqlite-watcher...");
    watcher
        .health_check(Request::new(HealthCheckRequest {}))
        .await
        .context("watcher health check failed")?;
    let _ = watcher
        .get_state(Request::new(GetStateRequest {
            table_name: GLOBAL_STATE_KEY.to_string(),
        }))
        .await?;

    tracing::info!(
        "Starting incremental sync (mode: {:?})",
        opts.incremental_mode
    );
    let mut processed_any = false;

    loop {
        let mut req = Request::new(ListChangesRequest {
            limit: opts.batch_size.max(1),
        });
        let changes = watcher
            .list_changes(req)
            .await
            .context("failed to list changes from watcher")?
            .into_inner()
            .changes;

        if changes.is_empty() {
            if !processed_any {
                tracing::info!("No pending sqlite-watcher changes");
            }
            break;
        }

        apply_changes(&client, &changes, opts.incremental_mode).await?;
        processed_any = true;

        let max_id = changes
            .iter()
            .map(|c| c.change_id)
            .max()
            .unwrap_or_default();
        watcher
            .ack_changes(Request::new(AckChangesRequest {
                up_to_change_id: max_id,
            }))
            .await
            .context("failed to ack changes")?;

        let last_change = changes.last().unwrap();
        watcher
            .set_state(Request::new(SetStateRequest {
                table_name: GLOBAL_STATE_KEY.to_string(),
                last_change_id: max_id,
                last_wal_frame: last_change.wal_frame.clone(),
                cursor: last_change.cursor.clone(),
            }))
            .await
            .context("failed to update watcher state")?;

        if changes.len() < opts.batch_size as usize {
            break;
        }
    }

    tracing::info!("sqlite-watcher sync completed");
    Ok(())
}

struct TableBatch {
    upserts: Vec<(String, serde_json::Value)>,
    deletes: Vec<String>,
}

impl TableBatch {
    fn new() -> Self {
        Self {
            upserts: Vec::new(),
            deletes: Vec::new(),
        }
    }
}

async fn apply_changes(
    client: &Client,
    changes: &[sqlite_watcher::watcher_proto::Change],
    mode: IncrementalMode,
) -> Result<()> {
    let mut per_table: HashMap<String, TableBatch> = HashMap::new();
    let mut table_state: HashMap<String, TableState> = HashMap::new();

    for change in changes {
        let entry = per_table
            .entry(change.table_name.clone())
            .or_insert_with(TableBatch::new);
        match change.op.as_str() {
            "insert" | "update" => {
                let payload = if change.payload.is_empty() {
                    serde_json::Value::Null
                } else {
                    serde_json::from_slice(&change.payload)
                        .context("failed to parse change payload")?
                };
                entry.upserts.push((change.primary_key.clone(), payload));
            }
            "delete" => {
                entry.deletes.push(change.primary_key.clone());
            }
            other => bail!("unknown change operation '{other}'"),
        }
        table_state.insert(
            change.table_name.clone(),
            TableState {
                last_change_id: change.change_id,
                wal_frame: change.wal_frame.clone(),
                cursor: change.cursor.clone(),
            },
        );
    }

    for (table, batch) in per_table.iter() {
        if !batch.upserts.is_empty() {
            insert_jsonb_batch(client, table, batch.upserts.clone(), "sqlite").await?;
            if mode == IncrementalMode::AppendDeduped {
                let latest_table = format!("{}_latest", table);
                ensure_latest_table(client, table, &latest_table).await?;
                upsert_jsonb_rows(client, &latest_table, &batch.upserts, "sqlite").await?;
            }
        }
        if !batch.deletes.is_empty() {
            delete_jsonb_rows(client, table, &batch.deletes).await?;
            if mode == IncrementalMode::AppendDeduped {
                let latest_table = format!("{}_latest", table);
                ensure_latest_table(client, table, &latest_table).await?;
                delete_jsonb_rows(client, &latest_table, &batch.deletes).await?;
            }
        }
    }

    persist_state(client, &table_state, mode).await?;
    Ok(())
}

async fn ensure_latest_table(
    client: &Client,
    source_table: &str,
    latest_table: &str,
) -> Result<()> {
    crate::jsonb::validate_table_name(source_table)?;
    crate::jsonb::validate_table_name(latest_table)?;
    let sql = format!(
        r#"CREATE TABLE IF NOT EXISTS "{}" (LIKE "{}" INCLUDING ALL)"#,
        latest_table, source_table
    );
    client.execute(&sql, &[]).await?;
    Ok(())
}

struct TableState {
    last_change_id: i64,
    wal_frame: Option<String>,
    cursor: Option<String>,
}

async fn persist_state(
    client: &Client,
    updates: &HashMap<String, TableState>,
    mode: IncrementalMode,
) -> Result<()> {
    for (table, state) in updates.iter() {
        client
            .execute(
                "INSERT INTO sqlite_sync_state(table_name, last_change_id, last_wal_frame, cursor, snapshot_completed, incremental_mode)
                 VALUES ($1, $2, $3, $4, TRUE, $5)
                 ON CONFLICT(table_name) DO UPDATE SET last_change_id = EXCLUDED.last_change_id, last_wal_frame = EXCLUDED.last_wal_frame, cursor = EXCLUDED.cursor, incremental_mode = EXCLUDED.incremental_mode",
                &[&table, &state.last_change_id, &state.wal_frame, &state.cursor, &mode_string(mode)],
            )
            .await?;
    }
    Ok(())
}

fn mode_string(mode: IncrementalMode) -> &'static str {
    match mode {
        IncrementalMode::Append => "append",
        IncrementalMode::AppendDeduped => "append_deduped",
    }
}

fn load_token(path: Option<&Path>) -> Result<String> {
    let token_path = path
        .map(|p| p.to_path_buf())
        .unwrap_or(default_token_path()?);
    let contents = std::fs::read_to_string(&token_path)
        .with_context(|| format!("failed to read token file {}", token_path.display()))?;
    let token = contents.trim().to_string();
    if token.is_empty() {
        bail!("token file {} is empty", token_path.display());
    }
    Ok(token)
}

fn default_token_path() -> Result<PathBuf> {
    let home = dirs::home_dir().ok_or_else(|| anyhow!("Could not determine home directory"))?;
    Ok(home.join(".seren/sqlite-watcher/token"))
}

async fn ensure_state_table(client: &Client) -> Result<()> {
    client
        .execute(
            r#"CREATE TABLE IF NOT EXISTS sqlite_sync_state (
                table_name TEXT PRIMARY KEY,
                last_change_id BIGINT NOT NULL DEFAULT 0,
                last_wal_frame TEXT,
                cursor TEXT,
                snapshot_completed BOOLEAN NOT NULL DEFAULT FALSE,
                incremental_mode TEXT NOT NULL DEFAULT 'append',
                baseline_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )"#,
            &[],
        )
        .await?;
    Ok(())
}

async fn ensure_baseline_exists(client: &Client) -> Result<()> {
    let row = client
        .query_one(
            "SELECT COUNT(*) FROM sqlite_sync_state WHERE snapshot_completed",
            &[],
        )
        .await?;
    let completed: i64 = row.get(0);
    if completed == 0 {
        bail!(
            "No completed sqlite baseline found. Run 'database-replicator init --source sqlite://...' first"
        );
    }
    Ok(())
}

enum WatcherEndpoint {
    Tcp { host: String, port: u16 },
    Unix(PathBuf),
    Pipe(String),
}

impl WatcherEndpoint {
    fn parse(value: &str) -> Result<Self> {
        if let Some(rest) = value.strip_prefix("unix:") {
            #[cfg(unix)]
            {
                if rest.is_empty() {
                    bail!("unix endpoint requires a path");
                }
                return Ok(WatcherEndpoint::Unix(PathBuf::from(rest)));
            }
            #[cfg(not(unix))]
            bail!("unix sockets are not supported on this platform")
        }
        if let Some(rest) = value.strip_prefix("tcp:") {
            let mut parts = rest.split(':');
            let host = parts
                .next()
                .ok_or_else(|| anyhow!("tcp endpoint must include host:port"))?;
            let port = parts
                .next()
                .ok_or_else(|| anyhow!("tcp endpoint must include port"))?
                .parse::<u16>()
                .context("invalid tcp port")?;
            return Ok(WatcherEndpoint::Tcp {
                host: host.to_string(),
                port,
            });
        }
        if let Some(rest) = value.strip_prefix("pipe:") {
            return Ok(WatcherEndpoint::Pipe(rest.to_string()));
        }
        bail!("unsupported watcher endpoint: {value}");
    }
}

#[derive(Clone)]
struct TokenInterceptor {
    header: tonic::metadata::MetadataValue<tonic::metadata::Ascii>,
}

impl TokenInterceptor {
    fn new(token: String) -> Result<Self> {
        let value = tonic::metadata::MetadataValue::try_from(format!("Bearer {token}"))
            .context("invalid watcher token")?;
        Ok(Self { header: value })
    }
}

impl Interceptor for TokenInterceptor {
    fn call(&mut self, mut req: Request<()>) -> Result<Request<()>, Status> {
        req.metadata_mut()
            .insert("authorization", self.header.clone());
        Ok(req)
    }
}

type WatcherClientWithAuth = WatcherClient<InterceptedService<Channel, TokenInterceptor>>;

async fn connect_watcher(
    endpoint: WatcherEndpoint,
    token: String,
) -> Result<WatcherClientWithAuth> {
    let interceptor = TokenInterceptor::new(token)?;
    let channel = match endpoint {
        WatcherEndpoint::Tcp { host, port } => {
            let uri = format!("http://{}:{}", host, port);
            Endpoint::try_from(uri)?.connect().await?
        }
        WatcherEndpoint::Unix(path) => {
            #[cfg(unix)]
            {
                let path_buf = path.clone();
                Endpoint::try_from("http://[::]:50051")?
                    .connect_with_connector(service_fn(move |_| {
                        let path = path_buf.clone();
                        async move { tokio::net::UnixStream::connect(path).await }
                    }))
                    .await?
            }
            #[cfg(not(unix))]
            {
                bail!("unix sockets are not supported on this platform")
            }
        }
        WatcherEndpoint::Pipe(name) => {
            bail!("named pipe endpoints are not supported yet: {name}")
        }
    };

    Ok(WatcherClient::with_interceptor(channel, interceptor))
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlite_watcher::watcher_proto::Change;

    #[test]
    fn group_changes_by_table() {
        let changes = vec![
            Change {
                change_id: 1,
                table_name: "foo".into(),
                op: "insert".into(),
                primary_key: "1".into(),
                payload: serde_json::to_vec(&serde_json::json!({"a":1})).unwrap(),
                wal_frame: None,
                cursor: None,
            },
            Change {
                change_id: 2,
                table_name: "foo".into(),
                op: "delete".into(),
                primary_key: "2".into(),
                payload: Vec::new(),
                wal_frame: None,
                cursor: None,
            },
        ];
        let mut per_table: HashMap<String, TableBatch> = HashMap::new();
        for change in changes {
            let entry = per_table
                .entry(change.table_name.clone())
                .or_insert_with(TableBatch::new);
            match change.op.as_str() {
                "insert" | "update" => {
                    entry
                        .upserts
                        .push((change.primary_key.clone(), serde_json::Value::Null));
                }
                "delete" => entry.deletes.push(change.primary_key.clone()),
                _ => {}
            }
        }
        let foo = per_table.get("foo").unwrap();
        assert_eq!(foo.upserts.len(), 1);
        assert_eq!(foo.deletes.len(), 1);
    }
}
