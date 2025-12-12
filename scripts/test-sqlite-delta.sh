#!/usr/bin/env bash
# Smoke test for sqlite-watcher + database-replicator incremental sync
# Requires: docker, sqlite-watcher, database-replicator, sqlite3

set -euo pipefail

if ! command -v docker >/dev/null; then
  echo "[smoke] docker is required" >&2
  exit 1
fi
if ! command -v sqlite-watcher >/dev/null; then
  echo "[smoke] sqlite-watcher binary not found in PATH" >&2
  exit 1
fi
if ! command -v database-replicator >/dev/null; then
  echo "[smoke] database-replicator binary not found in PATH" >&2
  exit 1
fi

TMPDIR=$(mktemp -d)
QUEUE_DB="$TMPDIR/queue.db"
SOCK="$TMPDIR/watcher.sock"
TOKEN_FILE="$TMPDIR/token"
POSTGRES_PORT=55432
CONTAINER_NAME=sqlite-delta-smoke

cleanup() {
  set +e
  if [[ -n "${WATCHER_PID:-}" ]]; then
    kill "$WATCHER_PID" >/dev/null 2>&1 || true
  fi
  docker rm -f "$CONTAINER_NAME" >/dev/null 2>&1 || true
  rm -rf "$TMPDIR"
}
trap cleanup EXIT

echo "[smoke] preparing token + queue"
mkdir -p "$(dirname "$TOKEN_FILE")"
printf 'smoke-%s' "$RANDOM" > "$TOKEN_FILE"
chmod 600 "$TOKEN_FILE"

sqlite-watcher enqueue --queue-db "$QUEUE_DB" --table demo --id smoke --payload '{"message":"hello-from-watcher"}'

sqlite-watcher serve --queue-db "$QUEUE_DB" --listen "unix:$SOCK" --token-file "$TOKEN_FILE" >/dev/null 2>&1 &
WATCHER_PID=$!
sleep 1

echo "[smoke] starting postgres container"
docker run -d --rm \
  --name "$CONTAINER_NAME" \
  -e POSTGRES_PASSWORD=postgres \
  -p "$POSTGRES_PORT":5432 \
  postgres:15 >/dev/null

until docker exec "$CONTAINER_NAME" pg_isready -U postgres >/dev/null 2>&1; do
  sleep 1
done

docker exec "$CONTAINER_NAME" psql -U postgres <<'SQL'
CREATE TABLE IF NOT EXISTS demo (
  id TEXT PRIMARY KEY,
  data JSONB NOT NULL,
  _source_type TEXT NOT NULL DEFAULT 'sqlite',
  _migrated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE TABLE IF NOT EXISTS sqlite_sync_state (
  table_name TEXT PRIMARY KEY,
  last_change_id BIGINT NOT NULL DEFAULT 0,
  last_wal_frame TEXT,
  cursor TEXT,
  snapshot_completed BOOLEAN NOT NULL DEFAULT FALSE,
  incremental_mode TEXT NOT NULL DEFAULT 'append',
  baseline_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
INSERT INTO sqlite_sync_state(table_name, snapshot_completed, incremental_mode)
VALUES ('demo', TRUE, 'append')
ON CONFLICT(table_name) DO UPDATE SET snapshot_completed = EXCLUDED.snapshot_completed,
  incremental_mode = EXCLUDED.incremental_mode;
SQL

echo "[smoke] running sync-sqlite"
DATABASE_URL="postgresql://postgres:postgres@localhost:$POSTGRES_PORT/postgres"
database-replicator sync-sqlite \
  --target "$DATABASE_URL" \
  --watcher-endpoint "unix:$SOCK" \
  --token-file "$TOKEN_FILE" \
  --batch-size 50 \
  --incremental-mode append >/dev/null

docker exec "$CONTAINER_NAME" psql -U postgres -tAc "SELECT count(*) FROM demo WHERE id = 'smoke'" | grep -q '^ 1'

echo "[smoke] success! sqlite-watcher + sync-sqlite end-to-end"
echo "[windows] Manual steps: run sqlite-watcher serve with tcp listener, start a Postgres instance (Docker Desktop works), then run database-replicator sync-sqlite with the TCP watcher endpoint."
