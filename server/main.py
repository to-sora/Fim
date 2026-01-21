from __future__ import annotations

import sqlite3
from typing import Any

from fastapi import Depends, FastAPI, Header, HTTPException, Request
from fastapi.responses import PlainTextResponse

from .auth import extract_bearer_token, machine_name_for_token
from .db import connect, init_db
from .ingest_buffer import BufferFullError, IngestBuffer
from .models import IngestRequest


app = FastAPI(title="FimSystem", version="0.1.0")


async def get_db() -> sqlite3.Connection:
    conn = connect()
    try:
        yield conn
    finally:
        conn.close()


@app.on_event("startup")
async def _startup() -> None:
    conn = connect()
    try:
        init_db(conn)
    finally:
        conn.close()
    app.state.ingest_buffer = IngestBuffer()
    await app.state.ingest_buffer.start()


@app.on_event("shutdown")
async def _shutdown() -> None:
    buf = getattr(app.state, "ingest_buffer", None)
    if buf is not None:
        await buf.stop()
        delattr(app.state, "ingest_buffer")


def _client_ip(request: Request) -> str:
    xff = request.headers.get("x-forwarded-for")
    if xff:
        return xff.split(",")[0].strip()
    if request.client:
        return request.client.host
    return "unknown"


def _require_machine_name(
    conn: sqlite3.Connection, authorization: str | None
) -> str:
    token = extract_bearer_token(authorization)
    if not token:
        raise HTTPException(status_code=401, detail="missing bearer token")
    machine_name = machine_name_for_token(conn, token)
    if not machine_name:
        raise HTTPException(status_code=401, detail="invalid token")
    return machine_name


def _latest_sha_by_path(
    conn: sqlite3.Connection, *, machine_name: str, file_paths: list[str]
) -> dict[str, str]:
    if not file_paths:
        return {}
    placeholders = ",".join(["?"] * len(file_paths))
    sql = f"""
        SELECT fr.file_path, fr.sha256
        FROM file_record fr
        JOIN (
          SELECT file_path, MAX(id) AS max_id
          FROM file_record
          WHERE machine_name = ? AND file_path IN ({placeholders})
          GROUP BY file_path
        ) latest ON fr.id = latest.max_id
    """
    params: list[Any] = [machine_name, *file_paths]
    rows = conn.execute(sql, params).fetchall()
    return {str(r["file_path"]): str(r["sha256"]) for r in rows}


@app.get("/healthz")
async def healthz() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/hello", response_class=PlainTextResponse)
async def hello() -> str:
    return "Hello"


@app.post("/ingest")
async def ingest(
    request: Request,
    payload: IngestRequest,
    authorization: str | None = Header(default=None),
    conn: sqlite3.Connection = Depends(get_db),
) -> dict[str, Any]:
    buf: IngestBuffer | None = getattr(request.app.state, "ingest_buffer", None)
    if buf is None:
        raise HTTPException(status_code=503, detail="ingest buffer not initialized")
    machine_name = _require_machine_name(conn, authorization)
    ip = _client_ip(request)

    file_paths = [r.file_path for r in payload.records]
    cached_prev_by_path = await buf.cached_latest_sha_by_path(machine_name=machine_name, file_paths=file_paths)
    missing_paths = [p for p in file_paths if p not in cached_prev_by_path]
    db_prev_by_path = _latest_sha_by_path(conn, machine_name=machine_name, file_paths=missing_paths)
    await buf.prime_latest_sha_by_path(machine_name=machine_name, latest_sha_by_path=db_prev_by_path)
    prev_by_path = {**db_prev_by_path, **cached_prev_by_path}

    changed: list[dict[str, str]] = []
    for r in payload.records:
        prev = prev_by_path.get(r.file_path)
        if prev and prev != r.sha256:
            changed.append(
                {
                    "file_path": r.file_path,
                    "previous_sha256": prev,
                    "new_sha256": r.sha256,
                }
            )

    rows = [
        (
            machine_name,
            payload.machine_id,
            payload.mac,
            rec.file_name,
            rec.file_path,
            rec.size_bytes,
            rec.sha256,
            payload.tag,
            payload.host_name,
            ip,
            rec.scan_ts,
            rec.urn,
        )
        for rec in payload.records
    ]
    latest_updates = {rec.file_path: rec.sha256 for rec in payload.records}
    try:
        await buf.enqueue(
            machine_name=machine_name,
            rows=rows,
            latest_sha_updates=latest_updates,
        )
    except BufferFullError as e:
        raise HTTPException(status_code=503, detail=str(e)) from e

    sha_set = sorted({r.sha256 for r in payload.records})
    duplicates: list[dict[str, Any]] = []
    if sha_set:
        placeholders = ",".join(["?"] * len(sha_set))
        name_set: dict[str, set[str]] = {sha: set() for sha in sha_set}
        path_set: dict[str, set[str]] = {sha: set() for sha in sha_set}
        for rec in payload.records:
            name_set[rec.sha256].add(rec.file_name)
            path_set[rec.sha256].add(rec.file_path)

        db_rows = conn.execute(
            f"""
            SELECT sha256, file_name, file_path
            FROM file_record
            WHERE sha256 IN ({placeholders})
            """,
            sha_set,
        ).fetchall()
        for r in db_rows:
            sha = str(r["sha256"])
            if sha not in name_set:
                continue
            file_name = r["file_name"]
            file_path = r["file_path"]
            if isinstance(file_name, str) and file_name:
                name_set[sha].add(file_name)
            if isinstance(file_path, str) and file_path:
                path_set[sha].add(file_path)

        duplicates = []
        for sha in sha_set:
            distinct_file_names = len(name_set[sha])
            distinct_file_paths = len(path_set[sha])
            if distinct_file_names > 1 or distinct_file_paths > 1:
                duplicates.append(
                    {
                        "sha256": sha,
                        "distinct_file_names": distinct_file_names,
                        "distinct_file_paths": distinct_file_paths,
                    }
                )

    return {"inserted": len(payload.records), "changed": changed, "duplicates": duplicates}


@app.get("/file/{sha256}")
async def file_by_sha256(
    sha256: str,
    limit: int = 100,
    conn: sqlite3.Connection = Depends(get_db),
) -> dict[str, Any]:
    if len(sha256) != 64:
        raise HTTPException(status_code=400, detail="sha256 must be 64 hex chars")
    try:
        buf = getattr(app.state, "ingest_buffer", None)
        if buf is not None:
            await buf.flush()
    except sqlite3.Error:
        # Best-effort: fall through and serve whatever is already persisted.
        pass
    limit = max(1, min(int(limit), 1000))
    rows = conn.execute(
        """
        SELECT machine_name, file_path, file_name, size_bytes, sha256, tag, host_name, client_ip, scan_ts, urn
        FROM file_record
        WHERE sha256 = ?
        ORDER BY scan_ts DESC, id DESC
        LIMIT ?
        """,
        (sha256, limit),
    ).fetchall()
    return {"sha256": sha256, "records": [dict(r) for r in rows]}


@app.get("/machine/{machine_name}")
async def machine_records(
    machine_name: str,
    limit: int = 200,
    sha256: str | None = None,
    conn: sqlite3.Connection = Depends(get_db),
) -> dict[str, Any]:
    try:
        buf = getattr(app.state, "ingest_buffer", None)
        if buf is not None:
            await buf.flush()
    except sqlite3.Error:
        pass
    limit = max(1, min(int(limit), 5000))
    if sha256 is None:
        rows = conn.execute(
            """
            SELECT file_path, file_name, size_bytes, sha256, tag, host_name, client_ip, scan_ts, urn
            FROM file_record
            WHERE machine_name = ?
            ORDER BY scan_ts DESC, id DESC
            LIMIT ?
            """,
            (machine_name, limit),
        ).fetchall()
    else:
        rows = conn.execute(
            """
            SELECT file_path, file_name, size_bytes, sha256, tag, host_name, client_ip, scan_ts, urn
            FROM file_record
            WHERE machine_name = ? AND sha256 = ?
            ORDER BY scan_ts DESC, id DESC
            LIMIT ?
            """,
            (machine_name, sha256, limit),
        ).fetchall()
    return {"machine_name": machine_name, "records": [dict(r) for r in rows]}
