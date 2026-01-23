from __future__ import annotations

import os
import sqlite3
from datetime import datetime, timezone
from pathlib import Path


def get_db_path() -> Path:
    raw = os.environ.get("FIM_DB_PATH", "").strip()
    if raw:
        return Path(raw).expanduser()
    return Path("data/fim.sqlite3")


def connect() -> sqlite3.Connection:
    db_path = get_db_path()
    db_path.parent.mkdir(parents=True, exist_ok=True)
    busy_timeout_ms = int(os.environ.get("FIM_DB_BUSY_TIMEOUT_MS", "5000"))
    conn = sqlite3.connect(str(db_path), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA busy_timeout = ?;", (busy_timeout_ms,))
    return conn


def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS auth_token (
          machine_id INTEGER PRIMARY KEY AUTOINCREMENT,
          machine_name TEXT UNIQUE,
          token TEXT NOT NULL UNIQUE,
          created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS file_record (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          machine_name TEXT,
          machine_id INTEGER,
          mac TEXT,
          file_name TEXT,
          file_path TEXT,
          size_bytes INTEGER,
          sha256 TEXT,
          tag TEXT,
          host_name TEXT,
          client_ip TEXT,
          scan_ts TEXT,
          urn TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_file_record_sha256 ON file_record(sha256);
        CREATE INDEX IF NOT EXISTS idx_file_record_machine_time ON file_record(machine_name, scan_ts);
        CREATE INDEX IF NOT EXISTS idx_file_record_machine_path_time ON file_record(machine_name, file_path, scan_ts);
        """
    )
    conn.commit()


def now_iso_text() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="minutes")
