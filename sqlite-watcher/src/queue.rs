use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{anyhow, Context, Result};
use rusqlite::{params, Connection, OptionalExtension};

const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS changes (
    change_id     INTEGER PRIMARY KEY AUTOINCREMENT,
    table_name    TEXT NOT NULL,
    op            TEXT NOT NULL,
    id            TEXT NOT NULL,
    payload       BLOB,
    wal_frame     TEXT,
    cursor        TEXT,
    created_at    TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    acked         INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS state (
    table_name      TEXT PRIMARY KEY,
    last_change_id  INTEGER NOT NULL DEFAULT 0,
    last_wal_frame  TEXT,
    cursor          TEXT,
    updated_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
"#;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChangeOperation {
    Insert,
    Update,
    Delete,
}

impl ChangeOperation {
    pub fn as_str(&self) -> &'static str {
        match self {
            ChangeOperation::Insert => "insert",
            ChangeOperation::Update => "update",
            ChangeOperation::Delete => "delete",
        }
    }

    fn from_str(value: &str) -> Result<Self> {
        match value {
            "insert" => Ok(ChangeOperation::Insert),
            "update" => Ok(ChangeOperation::Update),
            "delete" => Ok(ChangeOperation::Delete),
            other => Err(anyhow!("unknown change op: {other}")),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewChange {
    pub table_name: String,
    pub operation: ChangeOperation,
    pub primary_key: String,
    pub payload: Option<Vec<u8>>,
    pub wal_frame: Option<String>,
    pub cursor: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChangeRecord {
    pub change_id: i64,
    pub table_name: String,
    pub operation: ChangeOperation,
    pub primary_key: String,
    pub payload: Option<Vec<u8>>,
    pub wal_frame: Option<String>,
    pub cursor: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueueState {
    pub table_name: String,
    pub last_change_id: i64,
    pub last_wal_frame: Option<String>,
    pub cursor: Option<String>,
}

pub struct ChangeQueue {
    path: PathBuf,
    conn: Connection,
}

impl ChangeQueue {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).with_context(|| {
                format!("failed to create queue directory {}", parent.display())
            })?;
            #[cfg(unix)]
            set_owner_perms(parent)?;
        }
        let conn = Connection::open(path)
            .with_context(|| format!("failed to open queue database {}", path.display()))?;
        conn.pragma_update(None, "journal_mode", &"wal")
            .context("failed to enable WAL for change queue")?;
        conn.pragma_update(None, "synchronous", &"normal").ok();
        conn.execute_batch(SCHEMA)
            .context("failed to initialize queue schema")?;
        Ok(Self {
            path: path.to_path_buf(),
            conn,
        })
    }

    pub fn enqueue(&self, change: &NewChange) -> Result<i64> {
        self.conn.execute(
            "INSERT INTO changes(table_name, op, id, payload, wal_frame, cursor) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                change.table_name,
                change.operation.as_str(),
                change.primary_key,
                change.payload,
                change.wal_frame,
                change.cursor,
            ],
        )?;
        Ok(self.conn.last_insert_rowid())
    }

    pub fn fetch_batch(&self, limit: usize) -> Result<Vec<ChangeRecord>> {
        let mut stmt = self.conn.prepare(
            "SELECT change_id, table_name, op, id, payload, wal_frame, cursor
             FROM changes
             WHERE acked = 0
             ORDER BY change_id ASC
             LIMIT ?1",
        )?;
        let mut rows = stmt.query([limit as i64])?;
        let mut out = Vec::new();
        while let Some(row) = rows.next()? {
            let op_str: String = row.get(2)?;
            out.push(ChangeRecord {
                change_id: row.get(0)?,
                table_name: row.get(1)?,
                operation: ChangeOperation::from_str(&op_str)?,
                primary_key: row.get(3)?,
                payload: row.get(4)?,
                wal_frame: row.get(5)?,
                cursor: row.get(6)?,
            });
        }
        Ok(out)
    }

    pub fn ack_up_to(&self, change_id: i64) -> Result<u64> {
        let updated = self.conn.execute(
            "UPDATE changes SET acked = 1 WHERE change_id <= ?1",
            [change_id],
        )?;
        Ok(updated as u64)
    }

    pub fn vacuum_acknowledged(&self) -> Result<u64> {
        let deleted = self
            .conn
            .execute("DELETE FROM changes WHERE acked = 1", [])?;
        Ok(deleted as u64)
    }

    pub fn get_state(&self, table_name: &str) -> Result<Option<QueueState>> {
        self.conn
            .prepare(
                "SELECT table_name, last_change_id, last_wal_frame, cursor
                 FROM state WHERE table_name = ?1",
            )?
            .query_row([table_name], |row| {
                Ok(QueueState {
                    table_name: row.get(0)?,
                    last_change_id: row.get(1)?,
                    last_wal_frame: row.get(2)?,
                    cursor: row.get(3)?,
                })
            })
            .optional()
            .map_err(Into::into)
    }

    pub fn set_state(&self, state: &QueueState) -> Result<()> {
        self.conn.execute(
            "INSERT INTO state(table_name, last_change_id, last_wal_frame, cursor, updated_at)
             VALUES (?1, ?2, ?3, ?4, CURRENT_TIMESTAMP)
             ON CONFLICT(table_name) DO UPDATE SET
                last_change_id = excluded.last_change_id,
                last_wal_frame = excluded.last_wal_frame,
                cursor = excluded.cursor,
                updated_at = CURRENT_TIMESTAMP",
            params![
                state.table_name,
                state.last_change_id,
                state.last_wal_frame,
                state.cursor,
            ],
        )?;
        Ok(())
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

#[cfg(unix)]
fn set_owner_perms(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    let metadata = fs::metadata(path)?;
    let mut perms = metadata.permissions();
    perms.set_mode(0o700);
    fs::set_permissions(path, perms)?;
    Ok(())
}

#[cfg(not(unix))]
fn set_owner_perms(_path: &Path) -> Result<()> {
    Ok(())
}
