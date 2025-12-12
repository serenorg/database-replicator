use std::collections::HashMap;
use std::ffi::c_void;
use std::fs;
use std::os::raw::{c_char, c_int};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, RecvTimeoutError, Sender};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use base64::{engine::general_purpose, Engine as _};
use rusqlite::types::ValueRef;
use rusqlite::{Connection, OpenFlags};
use serde_json::{Map, Value};
use tracing::{debug, warn};

use crate::change::RowChange;
use crate::queue::ChangeOperation;

#[derive(Debug, Clone, Copy)]
pub struct WalWatcherConfig {
    pub poll_interval: Duration,
    pub min_event_bytes: u64,
}

impl Default for WalWatcherConfig {
    fn default() -> Self {
        Self {
            poll_interval: Duration::from_millis(500),
            min_event_bytes: 0,
        }
    }
}

pub struct WalWatcherHandle {
    stop: Arc<AtomicBool>,
    thread: Option<JoinHandle<Result<()>>>,
}

impl Drop for WalWatcherHandle {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        if let Some(handle) = self.thread.take() {
            if let Err(err) = handle.join() {
                warn!(error = ?err, "wal watcher thread panicked");
            }
        }
    }
}

pub fn start_wal_watcher<P: AsRef<Path>>(
    db_path: P,
    options: WalWatcherConfig,
    sender: Sender<RowChange>,
) -> Result<WalWatcherHandle> {
    let db_path = db_path.as_ref().canonicalize().with_context(|| {
        format!(
            "failed to canonicalize database path {}",
            db_path.as_ref().display()
        )
    })?;
    if !db_path.is_file() {
        bail!("database path {} is not a file", db_path.display());
    }

    let stop_flag = Arc::new(AtomicBool::new(false));
    let thread_stop = stop_flag.clone();
    let poll_interval = options.poll_interval;

    let thread = thread::spawn(move || -> Result<()> {
        let conn = Connection::open_with_flags(
            &db_path,
            OpenFlags::SQLITE_OPEN_READ_WRITE
                | OpenFlags::SQLITE_OPEN_URI
                | OpenFlags::SQLITE_OPEN_NO_MUTEX
                | OpenFlags::SQLITE_OPEN_SHARED_CACHE,
        )
        .with_context(|| format!("failed to open sqlite database {}", db_path.display()))?;

        ensure_wal_mode(&conn)?;
        conn.busy_timeout(Duration::from_secs(5))
            .context("failed to configure busy timeout")?;
        conn.pragma_update(None, "query_only", &1i64)
            .context("failed to enable query_only pragma")?;

        let mut tracker = TableTracker::new(&conn)?;
        let (notify_tx, notify_rx) = mpsc::channel();
        let _hook = unsafe { WalHook::register(conn.handle(), notify_tx.clone()) }?;
        let wal_path = wal_file_path(&db_path);
        let mut last_wal_len = wal_file_size(&wal_path).unwrap_or(0);

        debug!(db = %db_path.display(), "wal watcher thread initialized");

        while !thread_stop.load(Ordering::SeqCst) {
            let mut should_scan = match notify_rx.recv_timeout(poll_interval) {
                Ok(_) => true,
                Err(RecvTimeoutError::Timeout) => true,
                Err(RecvTimeoutError::Disconnected) => break,
            };

            match wal_file_size(&wal_path) {
                Ok(len) => {
                    if len != last_wal_len {
                        should_scan = true;
                        last_wal_len = len;
                    }
                }
                Err(err) => {
                    if err.kind() != std::io::ErrorKind::NotFound {
                        warn!(
                            wal = %wal_path.display(),
                            error = %err,
                            "failed to read wal metadata"
                        );
                    }
                    last_wal_len = 0;
                }
            }

            if should_scan {
                if let Err(err) = tracker.emit_changes(&conn, &sender) {
                    warn!(error = %err, "failed to emit wal changes");
                }
            }
        }

        Ok(())
    });

    Ok(WalWatcherHandle {
        stop: stop_flag,
        thread: Some(thread),
    })
}

fn ensure_wal_mode(conn: &Connection) -> Result<()> {
    conn.pragma_update(None, "journal_mode", &"wal")
        .context("failed to enable WAL journal mode")?;
    let mode: String = conn
        .pragma_query_value(None, "journal_mode", |row| row.get(0))
        .context("failed to query journal_mode")?;
    if mode.to_lowercase() != "wal" {
        bail!("sqlite-watcher requires WAL journal mode, found '{}'", mode);
    }
    Ok(())
}

struct WalHook {
    db: *mut rusqlite::ffi::sqlite3,
    user_data: *mut HookUserData,
}

struct HookUserData {
    tx: Sender<()>,
}

impl WalHook {
    unsafe fn register(db: *mut rusqlite::ffi::sqlite3, tx: Sender<()>) -> Result<Self> {
        let user = Box::new(HookUserData { tx });
        let user_ptr = Box::into_raw(user);
        rusqlite::ffi::sqlite3_wal_hook(db, Some(wal_hook_trampoline), user_ptr as *mut c_void);
        Ok(Self {
            db,
            user_data: user_ptr,
        })
    }
}

impl Drop for WalHook {
    fn drop(&mut self) {
        unsafe {
            rusqlite::ffi::sqlite3_wal_hook(self.db, None, std::ptr::null_mut());
            drop(Box::from_raw(self.user_data));
        }
    }
}

unsafe extern "C" fn wal_hook_trampoline(
    user_data: *mut c_void,
    _db: *mut rusqlite::ffi::sqlite3,
    _db_name: *const c_char,
    _pages: c_int,
) -> c_int {
    if !user_data.is_null() {
        let hook = &*(user_data as *mut HookUserData);
        let _ = hook.tx.send(());
    }
    0
}

struct TableTracker {
    schemas: HashMap<String, TableSchema>,
    snapshots: HashMap<String, HashMap<RowKey, Value>>,
}

impl TableTracker {
    fn new(conn: &Connection) -> Result<Self> {
        let mut tracker = TableTracker {
            schemas: HashMap::new(),
            snapshots: HashMap::new(),
        };
        tracker.refresh_tables(conn)?;
        Ok(tracker)
    }

    fn refresh_tables(&mut self, conn: &Connection) -> Result<()> {
        for name in fetch_table_names(conn)? {
            if self.schemas.contains_key(&name) {
                continue;
            }
            let schema = TableSchema::load(conn, &name)?;
            let snapshot = schema.capture(conn)?;
            self.snapshots.insert(name.clone(), snapshot);
            self.schemas.insert(name, schema);
        }
        Ok(())
    }

    fn emit_changes(&mut self, conn: &Connection, sender: &Sender<RowChange>) -> Result<()> {
        self.refresh_tables(conn)?;
        for (name, schema) in self.schemas.iter() {
            let mut previous = self.snapshots.remove(name).unwrap_or_default();
            let current = schema.capture(conn)?;
            diff_table(name, &current, &mut previous, sender)?;
            self.snapshots.insert(name.clone(), current);
        }
        Ok(())
    }
}

fn diff_table(
    table_name: &str,
    current: &HashMap<RowKey, Value>,
    previous: &mut HashMap<RowKey, Value>,
    sender: &Sender<RowChange>,
) -> Result<()> {
    for (key, value) in current.iter() {
        match previous.remove(key) {
            Some(old) => {
                if old != *value {
                    send_change(
                        sender,
                        table_name,
                        ChangeOperation::Update,
                        key,
                        Some(value.clone()),
                    )?;
                }
            }
            None => {
                send_change(
                    sender,
                    table_name,
                    ChangeOperation::Insert,
                    key,
                    Some(value.clone()),
                )?;
            }
        }
    }

    for (key, old_value) in previous.iter() {
        send_change(
            sender,
            table_name,
            ChangeOperation::Delete,
            key,
            Some(old_value.clone()),
        )?;
    }

    Ok(())
}

fn send_change(
    sender: &Sender<RowChange>,
    table: &str,
    op: ChangeOperation,
    key: &RowKey,
    payload: Option<Value>,
) -> Result<()> {
    let change = RowChange {
        table_name: table.to_string(),
        operation: op,
        primary_key: key.0.clone(),
        payload,
        wal_frame: None,
        cursor: None,
    };
    sender
        .send(change)
        .map_err(|_| anyhow!("row change receiver dropped"))
}

struct TableSchema {
    name: String,
    primary_key: Vec<String>,
    select_sql: String,
    include_rowid: bool,
}

impl TableSchema {
    fn load(conn: &Connection, name: &str) -> Result<Self> {
        let mut stmt = conn.prepare(&format!("PRAGMA table_info({})", quote_identifier(name)))?;
        let mut columns = Vec::new();
        let mut pk_cols: Vec<(i32, String)> = Vec::new();
        let mut rows = stmt.query([])?;
        while let Some(row) = rows.next()? {
            let col_name: String = row.get("name")?;
            columns.push(col_name.clone());
            let pk_pos: i32 = row.get("pk")?;
            if pk_pos > 0 {
                pk_cols.push((pk_pos, col_name));
            }
        }
        pk_cols.sort_by_key(|(pos, _)| *pos);
        let primary_key: Vec<String> = pk_cols.into_iter().map(|(_, name)| name).collect();
        let include_rowid = primary_key.is_empty();
        let select_sql = if include_rowid {
            format!(
                "SELECT rowid as __seren_rowid__, * FROM {}",
                quote_identifier(name)
            )
        } else {
            format!("SELECT * FROM {}", quote_identifier(name))
        };

        Ok(TableSchema {
            name: name.to_string(),
            primary_key,
            select_sql,
            include_rowid,
        })
    }

    fn capture(&self, conn: &Connection) -> Result<HashMap<RowKey, Value>> {
        let mut stmt = conn.prepare(&self.select_sql)?;
        let column_names: Vec<String> = stmt.column_names().iter().map(|c| c.to_string()).collect();
        let mut rows = stmt.query([])?;
        let mut map = HashMap::new();
        while let Some(row) = rows.next()? {
            let mut payload = Map::new();
            let mut rowid_value = Value::Null;
            for (idx, name) in column_names.iter().enumerate() {
                let value = sqlite_value_to_json(row.get_ref(idx)?);
                if self.include_rowid && idx == 0 && name == "__seren_rowid__" {
                    rowid_value = value;
                } else {
                    payload.insert(name.clone(), value);
                }
            }
            let key = self.make_row_key(&rowid_value, &payload)?;
            map.insert(key, Value::Object(payload));
        }
        Ok(map)
    }

    fn make_row_key(&self, rowid: &Value, row: &Map<String, Value>) -> Result<RowKey> {
        if self.primary_key.is_empty() {
            match rowid {
                Value::Number(num) => Ok(RowKey(format!("rowid:{}", num))),
                _ => bail!("rowid not available for table {}", self.name),
            }
        } else {
            let mut parts = Vec::with_capacity(self.primary_key.len());
            for pk in &self.primary_key {
                let value = row.get(pk).cloned().unwrap_or(Value::Null);
                parts.push(value_to_key_fragment(&value));
            }
            Ok(RowKey(parts.join("|")))
        }
    }
}

fn fetch_table_names(conn: &Connection) -> Result<Vec<String>> {
    let mut stmt = conn.prepare(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name",
    )?;
    let table_names = stmt
        .query_map([], |row| row.get(0))?
        .collect::<Result<Vec<String>, _>>()?;
    Ok(table_names)
}

fn sqlite_value_to_json(value: ValueRef<'_>) -> Value {
    match value {
        ValueRef::Null => Value::Null,
        ValueRef::Integer(i) => Value::Number(i.into()),
        ValueRef::Real(f) => {
            Value::Number(serde_json::Number::from_f64(f).unwrap_or_else(|| 0.into()))
        }
        ValueRef::Text(t) => Value::String(String::from_utf8_lossy(t).into_owned()),
        ValueRef::Blob(b) => Value::String(general_purpose::STANDARD.encode(b)),
    }
}

fn value_to_key_fragment(value: &Value) -> String {
    match value {
        Value::Null => "null".to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Number(num) => num.to_string(),
        Value::String(s) => format!("\"{}\"", s),
        other => serde_json::to_string(other).unwrap_or_else(|_| "null".to_string()),
    }
}

#[derive(Hash, Eq, PartialEq, Clone)]
struct RowKey(String);

fn quote_identifier(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

fn wal_file_path(db_path: &Path) -> PathBuf {
    let mut os_string = db_path.as_os_str().to_owned();
    os_string.push("-wal");
    PathBuf::from(os_string)
}

fn wal_file_size(path: &Path) -> std::io::Result<u64> {
    fs::metadata(path).map(|m| m.len())
}
