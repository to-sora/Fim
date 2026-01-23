from __future__ import annotations

import sqlite3
from typing import Any

from fastapi import Depends, FastAPI, Header, HTTPException, Request
from fastapi.responses import PlainTextResponse

from .auth import extract_bearer_token, machine_identity_for_token
from .db import connect, init_db
from .ingest_buffer import BufferFullError, IngestBuffer
from .logger import get_logger
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
    if request.client:
        return request.client.host
    return "unknown"


def _require_machine_identity(
    conn: sqlite3.Connection, authorization: str | None
) -> tuple[int, str | None]:
    token = extract_bearer_token(authorization)
    if not token:
        raise HTTPException(status_code=401, detail="missing bearer token")
    identity = machine_identity_for_token(conn, token)
    if not identity:
        raise HTTPException(status_code=401, detail="invalid token")
    return identity


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
    machine_id, machine_name = _require_machine_identity(conn, authorization)
    ip = _client_ip(request)

    rows = [
        (
            machine_name,
            machine_id,
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
    buf = getattr(app.state, "ingest_buffer", None)
    if buf is None:
        get_logger().error("ingest failed: buffer not available")
        raise HTTPException(status_code=503, detail="ingest buffer not available")
    try:
        await buf.enqueue(
            machine_name=machine_name,
            rows=rows,
        )
    except BufferFullError as e:
        get_logger().warning("ingest rejected: buffer full")
        raise HTTPException(status_code=503, detail=str(e)) from e

    return {"received": len(payload.records)}
