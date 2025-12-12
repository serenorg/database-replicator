use std::fmt;
use std::fs;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::mpsc;
use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use clap::Parser;
use sqlite_watcher::decoder::WalGrowthDecoder;
use sqlite_watcher::queue::ChangeQueue;
#[cfg(unix)]
use sqlite_watcher::server::spawn_unix_server;
use sqlite_watcher::server::{spawn_tcp_server, ServerHandle};
use sqlite_watcher::wal::{start_wal_watcher, WalWatcherConfig as TailConfig};
use tracing_subscriber::EnvFilter;

#[cfg(unix)]
const DEFAULT_LISTEN: &str = "unix:/tmp/sqlite-watcher.sock";
#[cfg(not(unix))]
const DEFAULT_LISTEN: &str = "tcp:50051";

/// Command-line interface definition for sqlite-watcher.
#[derive(Debug, Clone, Parser)]
#[command(
    name = "sqlite-watcher",
    version,
    about = "Tails SQLite WAL files and exposes change streams.",
    long_about = None
)]
struct Cli {
    /// Path to the SQLite database the watcher should monitor.
    #[arg(long = "db", value_name = "PATH")]
    db_path: PathBuf,

    /// Listener binding. Accepts unix:/path, tcp:<port>, or pipe:<name>.
    #[arg(long, value_name = "ENDPOINT", default_value = DEFAULT_LISTEN)]
    listen: String,

    /// Shared-secret token file used to authenticate RPC clients.
    #[arg(long = "token-file", value_name = "PATH")]
    token_file: Option<PathBuf>,

    /// Path to the durable change queue database.
    #[arg(long = "queue-db", value_name = "PATH")]
    queue_db: Option<PathBuf>,

    /// Tracing filter (info,warn,debug,trace). Can also be provided via SQLITE_WATCHER_LOG.
    #[arg(
        long = "log-level",
        value_name = "FILTER",
        default_value = "info",
        env = "SQLITE_WATCHER_LOG"
    )]
    log_filter: String,

    /// Interval in milliseconds between WAL file polls.
    #[arg(
        long = "poll-interval-ms",
        default_value_t = 500,
        value_parser = clap::value_parser!(u64).range(50..=60_000)
    )]
    poll_interval_ms: u64,

    /// Minimum WAL byte growth required before emitting an event.
    #[arg(
        long = "min-event-bytes",
        default_value_t = 1,
        value_parser = clap::value_parser!(u64).range(1..=10_000_000)
    )]
    min_event_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ListenAddress {
    Unix(PathBuf),
    Tcp { host: String, port: u16 },
    Pipe(String),
}

impl fmt::Display for ListenAddress {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ListenAddress::Unix(path) => write!(f, "unix:{}", path.display()),
            ListenAddress::Tcp { host, port } => write!(f, "tcp:{}:{}", host, port),
            ListenAddress::Pipe(name) => write!(f, "pipe:{}", name),
        }
    }
}

impl FromStr for ListenAddress {
    type Err = anyhow::Error;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if let Some(path) = value.strip_prefix("unix:") {
            if cfg!(windows) {
                bail!("unix sockets are not supported on Windows");
            }
            if path.is_empty() {
                bail!("unix listen path cannot be empty");
            }
            return Ok(ListenAddress::Unix(PathBuf::from(path)));
        }

        if let Some(port) = value.strip_prefix("tcp:") {
            let port: u16 = port
                .parse()
                .map_err(|_| anyhow!("tcp listener must specify a numeric port"))?;
            return Ok(ListenAddress::Tcp {
                host: "127.0.0.1".to_string(),
                port,
            });
        }

        if let Some(name) = value.strip_prefix("pipe:") {
            if cfg!(not(windows)) {
                bail!("named pipes are only valid on Windows");
            }
            if name.is_empty() {
                bail!("pipe name cannot be empty");
            }
            return Ok(ListenAddress::Pipe(name.to_string()));
        }

        bail!("listen endpoint must start with unix:/, tcp:, or pipe:");
    }
}

#[derive(Debug, Clone)]
struct WatcherConfig {
    database_path: PathBuf,
    listen: ListenAddress,
    token_file: PathBuf,
    queue_path: PathBuf,
    poll_interval: Duration,
    min_event_bytes: u64,
}

impl TryFrom<Cli> for WatcherConfig {
    type Error = anyhow::Error;

    fn try_from(args: Cli) -> Result<Self> {
        let database_path = ensure_sqlite_file(&args.db_path)?;
        let listen = ListenAddress::from_str(args.listen.trim())?;
        let token_file = match args.token_file {
            Some(path) => expand_home(path)?,
            None => default_token_path()?,
        };
        let queue_path = match args.queue_db {
            Some(path) => expand_home(path)?,
            None => default_queue_path()?,
        };

        Ok(Self {
            database_path,
            listen,
            token_file,
            queue_path,
            poll_interval: Duration::from_millis(args.poll_interval_ms),
            min_event_bytes: args.min_event_bytes,
        })
    }
}

fn ensure_sqlite_file(path: &Path) -> Result<PathBuf> {
    if !path.exists() {
        bail!("database path {} does not exist", path.display());
    }
    if !path.is_file() {
        bail!("database path {} is not a file", path.display());
    }
    Ok(path
        .canonicalize()
        .with_context(|| format!("failed to canonicalize {}", path.display()))?)
}

fn default_token_path() -> Result<PathBuf> {
    let home = dirs::home_dir().ok_or_else(|| anyhow!("unable to determine home directory"))?;
    Ok(home.join(".seren/sqlite-watcher/token"))
}

fn default_queue_path() -> Result<PathBuf> {
    let home = dirs::home_dir().ok_or_else(|| anyhow!("unable to determine home directory"))?;
    Ok(home.join(".seren/sqlite-watcher/changes.db"))
}

fn expand_home(path: PathBuf) -> Result<PathBuf> {
    let as_str = path.to_string_lossy();
    if let Some(stripped) = as_str.strip_prefix("~/") {
        let home = dirs::home_dir().ok_or_else(|| anyhow!("unable to determine home directory"))?;
        return Ok(home.join(stripped));
    }
    if as_str == "~" {
        let home = dirs::home_dir().ok_or_else(|| anyhow!("unable to determine home directory"))?;
        return Ok(home);
    }
    Ok(path)
}

fn init_tracing(filter: &str) -> Result<()> {
    let env_filter = EnvFilter::try_new(filter).or_else(|_| EnvFilter::try_new("info"))?;
    tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .with_target(false)
        .try_init()
        .map_err(|err| anyhow!("failed to init tracing subscriber: {err}"))
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    init_tracing(&cli.log_filter)?;
    let config = WatcherConfig::try_from(cli)?;
    let auth_token = read_token_file(&config.token_file)?;

    tracing::info!(
        db = %config.database_path.display(),
        listen = %config.listen,
        token = %config.token_file.display(),
        queue = %config.queue_path.display(),
        poll_ms = config.poll_interval.as_millis(),
        min_event_bytes = config.min_event_bytes,
        "sqlite-watcher starting"
    );

    let queue = ChangeQueue::open(&config.queue_path)?;
    let decoder = WalGrowthDecoder::default();
    let server_handle = start_grpc_server(&config.listen, &config.queue_path, &auth_token)?;
    let (event_tx, event_rx) = mpsc::channel();
    let _wal_handle = start_wal_watcher(
        &config.database_path,
        TailConfig {
            poll_interval: config.poll_interval,
            min_event_bytes: config.min_event_bytes,
        },
        event_tx,
    )?;

    for event in event_rx {
        match process_wal_event(&decoder, &queue, &event) {
            Ok(change_ids) if !change_ids.is_empty() => {
                tracing::info!(
                    bytes_added = event.bytes_added,
                    wal_size = event.current_size,
                    change_count = change_ids.len(),
                    "queued wal growth event"
                );
            }
            Err(err) => {
                tracing::warn!(error = %err, "failed to persist wal event to queue");
            }
            _ => {}
        }
    }

    drop(server_handle);
    Ok(())
}

fn process_wal_event(
    decoder: &WalGrowthDecoder,
    queue: &ChangeQueue,
    event: &sqlite_watcher::wal::WalEvent,
) -> Result<Vec<i64>> {
    let mut ids = Vec::new();
    for row in decoder.decode(event) {
        ids.push(queue.enqueue(&row.into_new_change())?);
    }
    Ok(ids)
}

fn read_token_file(path: &Path) -> Result<String> {
    let contents = fs::read_to_string(path)
        .with_context(|| format!("failed to read token file {}", path.display()))?;
    let token = contents.trim().to_string();
    if token.is_empty() {
        bail!("token file {} is empty", path.display());
    }
    Ok(token)
}

fn start_grpc_server(
    listen: &ListenAddress,
    queue_path: &Path,
    token: &str,
) -> Result<Option<ServerHandle>> {
    match listen {
        ListenAddress::Tcp { host, port } => {
            let addr: SocketAddr = format!("{}:{}", host, port)
                .parse()
                .with_context(|| format!("invalid tcp listen address {host}:{port}"))?;
            let handle = spawn_tcp_server(addr, queue_path.to_path_buf(), token.to_string())?;
            Ok(Some(handle))
        }
        ListenAddress::Unix(path) => {
            #[cfg(unix)]
            {
                let handle = spawn_unix_server(path, queue_path.to_path_buf(), token.to_string())?;
                Ok(Some(handle))
            }
            #[cfg(not(unix))]
            {
                bail!("unix sockets are not supported on this platform")
            }
        }
        ListenAddress::Pipe(name) => {
            tracing::warn!(pipe = name, "named pipe transport is not yet implemented");
            Ok(None)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;
    use sqlite_watcher::queue::ChangeQueue;
    use tempfile::{tempdir, NamedTempFile};

    #[test]
    fn parses_tcp_listener() {
        let tmp = NamedTempFile::new().unwrap();
        let cli = Cli::try_parse_from([
            "sqlite-watcher",
            "--db",
            tmp.path().to_str().unwrap(),
            "--listen",
            "tcp:6000",
            "--token-file",
            "./token",
            "--log-level",
            "debug",
        ])
        .expect("cli parsing failed");

        let config = WatcherConfig::try_from(cli).expect("config conversion failed");
        assert!(matches!(
            config.listen,
            ListenAddress::Tcp { host, port } if host == "127.0.0.1" && port == 6000
        ));
        assert!(config.token_file.ends_with("token"));
        assert!(config.queue_path.ends_with("changes.db"));
    }

    #[test]
    #[cfg(unix)]
    fn parses_unix_listener_default() {
        let tmp = NamedTempFile::new().unwrap();
        let cli =
            Cli::try_parse_from(["sqlite-watcher", "--db", tmp.path().to_str().unwrap()]).unwrap();
        let config = WatcherConfig::try_from(cli).unwrap();
        assert!(matches!(config.listen, ListenAddress::Unix(_)));
    }

    #[test]
    fn persist_wal_events_into_queue() {
        let dir = tempdir().unwrap();
        let queue_path = dir.path().join("queue.db");
        let queue = ChangeQueue::open(&queue_path).unwrap();
        let decoder = WalGrowthDecoder::default();

        let event = sqlite_watcher::wal::WalEvent {
            bytes_added: 2048,
            current_size: 4096,
        };
        let change_ids = process_wal_event(&decoder, &queue, &event).unwrap();
        let batch = queue.fetch_batch(10).unwrap();
        assert_eq!(batch.len(), change_ids.len());
        assert_eq!(batch[0].table_name, "__wal__");
    }
}
