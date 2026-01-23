from __future__ import annotations

import sqlite3
import uuid

from .db import now_iso_text


def extract_bearer_token(authorization: str | None) -> str | None:
    if not authorization:
        return None
    if not authorization.lower().startswith("bearer "):
        return None
    token = authorization[7:].strip()
    return token or None


def machine_identity_for_token(
    conn: sqlite3.Connection, token: str
) -> tuple[int, str | None] | None:
    row = conn.execute(
        "SELECT machine_id, machine_name FROM auth_token WHERE token = ?", (token,)
    ).fetchone()
    if row is None:
        return None
    return int(row["machine_id"]), row["machine_name"]


def create_or_rotate_token(conn: sqlite3.Connection, machine_name: str) -> str:
    token = str(uuid.uuid4())
    ts = now_iso_text()
    conn.execute(
        """
        INSERT INTO auth_token(machine_name, token, created_at, updated_at)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(machine_name) DO UPDATE SET token=excluded.token, updated_at=excluded.updated_at
        """,
        (machine_name, token, ts, ts),
    )
    conn.commit()
    return token


def delete_token(conn: sqlite3.Connection, machine_name: str) -> None:
    conn.execute("DELETE FROM auth_token WHERE machine_name = ?", (machine_name,))
    conn.commit()


def list_tokens(conn: sqlite3.Connection) -> list[dict]:
    rows = conn.execute(
        """
        SELECT machine_id, machine_name, token, created_at, updated_at
        FROM auth_token
        ORDER BY machine_name
        """
    ).fetchall()
    return [dict(r) for r in rows]
