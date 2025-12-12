use std::path::PathBuf;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use clap::{Parser, Subcommand, ValueEnum};
use sqlite_watcher::queue::{ChangeOperation, ChangeQueue, NewChange};
use sqlite_watcher::server::{spawn_tcp, spawn_unix};
use tokio::signal;

#[derive(Parser)]
#[command(name = "sqlite-watcher")]
#[command(about = "sqlite watcher utility (alpha)")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Run the gRPC server exposing the change queue
    Serve {
        /// SQLite queue database path
        #[arg(long = "queue-db")]
        queue_db: Option<PathBuf>,
        /// gRPC listener (unix:/path or tcp:host:port)
        #[arg(long = "listen", default_value = "unix:/tmp/sqlite-watcher.sock")]
        listen: String,
        /// Shared-secret token file (defaults to ~/.seren/sqlite-watcher/token)
        #[arg(long = "token-file")]
        token_file: Option<PathBuf>,
    },
    /// Enqueue a test change into the queue database
    Enqueue {
        #[arg(long = "queue-db")]
        queue_db: Option<PathBuf>,
        #[arg(long = "table", default_value = "demo")]
        table: String,
        #[arg(long = "id", default_value = "smoke-test")]
        id: String,
        #[arg(long = "payload", default_value = r#"{""message"":""hello""}"#)]
        payload: String,
        #[arg(long = "op", value_enum, default_value = "insert")]
        op: ChangeOp,
    },
}

#[derive(Clone, Copy, ValueEnum)]
enum ChangeOp {
    Insert,
    Update,
    Delete,
}

impl From<ChangeOp> for ChangeOperation {
    fn from(value: ChangeOp) -> Self {
        match value {
            ChangeOp::Insert => ChangeOperation::Insert,
            ChangeOp::Update => ChangeOperation::Update,
            ChangeOp::Delete => ChangeOperation::Delete,
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Command::Serve {
            queue_db,
            listen,
            token_file,
        } => serve(queue_db, &listen, token_file).await,
        Command::Enqueue {
            queue_db,
            table,
            id,
            payload,
            op,
        } => enqueue(queue_db, &table, &id, &payload, op.into()),
    }
}

async fn serve(queue_db: Option<PathBuf>, listen: &str, token_file: Option<PathBuf>) -> Result<()> {
    let queue_path = resolve_queue_path(queue_db)?;
    let token_path = resolve_token_path(token_file)?;
    let token = std::fs::read_to_string(&token_path)
        .with_context(|| format!("failed to read token file {}", token_path.display()))?;
    let queue = ChangeQueue::open(&queue_path)?;
    let endpoint = WatcherEndpoint::parse(listen)?;
    println!(
        "sqlite-watcher serving {listen} using queue {}",
        queue.path().display()
    );
    let handle = match endpoint {
        WatcherEndpoint::Tcp { host, port } => {
            let addr = format!("{}:{}", host, port)
                .parse()
                .context("invalid tcp address")?;
            spawn_tcp(addr, queue.path().to_path_buf(), token)?
        }
        WatcherEndpoint::Unix(path) => spawn_unix(&path, queue.path().to_path_buf(), token)?,
        WatcherEndpoint::Pipe(name) => bail!("named pipes are not yet supported ({name})"),
    };
    println!("Press Ctrl+C to stop sqlite-watcher");
    let ctrl_c = signal::ctrl_c();
    tokio::pin!(ctrl_c);
    let _ = tokio::time::timeout(Duration::MAX, &mut ctrl_c).await;
    drop(handle);
    Ok(())
}

fn enqueue(
    queue_db: Option<PathBuf>,
    table: &str,
    id: &str,
    payload: &str,
    op: ChangeOperation,
) -> Result<()> {
    let queue_path = resolve_queue_path(queue_db)?;
    let queue = ChangeQueue::open(&queue_path)?;
    let bytes = payload.as_bytes().to_vec();
    queue.enqueue(&NewChange {
        table_name: table.to_string(),
        operation: op,
        primary_key: id.to_string(),
        payload: Some(bytes),
        wal_frame: None,
        cursor: None,
    })?;
    println!(
        "Enqueued row id '{}' for table '{}' into {}",
        id,
        table,
        queue.path().display()
    );
    Ok(())
}

fn resolve_queue_path(path: Option<PathBuf>) -> Result<PathBuf> {
    match path {
        Some(p) => Ok(expand_path(p)?),
        None => {
            let mut default = dirs::home_dir()
                .ok_or_else(|| anyhow::anyhow!("Unable to resolve home directory"))?;
            default.push(".seren/sqlite-watcher/changes.db");
            Ok(default)
        }
    }
}

fn resolve_token_path(path: Option<PathBuf>) -> Result<PathBuf> {
    match path {
        Some(p) => Ok(expand_path(p)?),
        None => {
            let mut default = dirs::home_dir()
                .ok_or_else(|| anyhow::anyhow!("Unable to resolve home directory"))?;
            default.push(".seren/sqlite-watcher/token");
            Ok(default)
        }
    }
}

fn expand_path(p: PathBuf) -> Result<PathBuf> {
    let s = p.to_string_lossy();
    if let Some(rest) = s.strip_prefix("~/") {
        let mut home =
            dirs::home_dir().ok_or_else(|| anyhow::anyhow!("Unable to resolve home directory"))?;
        home.push(rest);
        Ok(home)
    } else {
        Ok(p)
    }
}

enum WatcherEndpoint {
    Tcp { host: String, port: u16 },
    Unix(PathBuf),
    Pipe(String),
}

impl WatcherEndpoint {
    fn parse(value: &str) -> Result<Self> {
        if let Some(rest) = value.strip_prefix("unix:") {
            if rest.is_empty() {
                bail!("unix endpoint requires a path");
            }
            return Ok(WatcherEndpoint::Unix(PathBuf::from(rest)));
        }
        if let Some(rest) = value.strip_prefix("tcp:") {
            let mut parts = rest.split(':');
            let host = parts
                .next()
                .ok_or_else(|| anyhow::anyhow!("tcp endpoint missing host"))?;
            let port = parts
                .next()
                .ok_or_else(|| anyhow::anyhow!("tcp endpoint missing port"))?
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
        bail!("unsupported listener endpoint: {value}");
    }
}
