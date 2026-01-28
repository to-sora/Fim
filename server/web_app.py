from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse, PlainTextResponse
from fastapi.staticfiles import StaticFiles

from .db import connect, init_db
from .graph import (
    fetch_segments_for_sha256,
    render_ascii_chain,
    render_dot,
    render_mermaid_flowchart,
)


WEB_ROOT = Path(__file__).resolve().parent / "webui"

app = FastAPI(title="FimWebUI", version="0.1.0")


def _format_bytes(size_bytes: int) -> str:
    if size_bytes <= 0:
        return "0 B"
    size = float(size_bytes)
    units = ["B", "KB", "MB", "GB", "TB", "PB"]
    unit_index = 0
    while size >= 1024 and unit_index < len(units) - 1:
        size /= 1024
        unit_index += 1
    if size >= 10 or unit_index == 0:
        return f"{size:.0f} {units[unit_index]}"
    return f"{size:.1f} {units[unit_index]}"


def _dedupe_records(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: set[tuple[str, str]] = set()
    out: list[dict[str, Any]] = []
    for r in records:
        key = (str(r.get("file_path", "")), str(r.get("file_name", "")))
        if key in seen:
            continue
        seen.add(key)
        out.append(r)
    return out


def _apply_size_human(records: list[dict[str, Any]]) -> None:
    for r in records:
        size = r.get("size_bytes")
        if isinstance(size, int):
            r["size_human"] = _format_bytes(size)


def _limit_value(limit: int | None, *, max_limit: int) -> int | None:
    if limit is None:
        return None
    limit_val = int(limit)
    if limit_val <= 0:
        return None
    return min(limit_val, max_limit)


@app.on_event("startup")
async def _startup() -> None:
    conn = connect()
    try:
        init_db(conn)
    finally:
        conn.close()


@app.get("/", response_class=FileResponse)
def index() -> FileResponse:
    return FileResponse(WEB_ROOT / "index.html")


@app.get("/healthz")
def healthz() -> dict[str, str]:
    return {"status": "ok"}


app.mount("/static", StaticFiles(directory=WEB_ROOT), name="static")


@app.get("/api/machines")
def list_machines() -> dict[str, list[str]]:
    conn = connect()
    try:
        init_db(conn)
        rows = conn.execute(
            "SELECT DISTINCT machine_name FROM file_record WHERE machine_name IS NOT NULL ORDER BY machine_name ASC"
        ).fetchall()
    finally:
        conn.close()
    return {"machines": [str(r["machine_name"]) for r in rows if r["machine_name"]]}


@app.get("/api/query/file")
def query_file(
    sha256: str = Query(..., min_length=64, max_length=64),
    limit: int | None = 100,
    dedupe: bool = True,
) -> dict[str, Any]:
    if len(sha256) != 64:
        raise HTTPException(status_code=400, detail="sha256 must be 64 hex chars")
    conn = connect()
    try:
        init_db(conn)
        limit_val = _limit_value(limit, max_limit=20000)
        if limit_val is None:
            rows = conn.execute(
                """
                SELECT machine_name, file_path, file_name, size_bytes, sha256, scan_ts, ingested_at, urn
                FROM file_record
                WHERE sha256 = ?
                ORDER BY scan_ts DESC, id DESC
                """,
                (sha256,),
            ).fetchall()
        else:
            rows = conn.execute(
                """
                SELECT machine_name, file_path, file_name, size_bytes, sha256, scan_ts, ingested_at, urn
                FROM file_record
                WHERE sha256 = ?
                ORDER BY scan_ts DESC, id DESC
                LIMIT ?
                """,
                (sha256, limit_val),
            ).fetchall()
        sha_count = conn.execute(
            "SELECT COUNT(*) FROM file_record WHERE sha256 = ?",
            (sha256,),
        ).fetchone()[0]
    finally:
        conn.close()

    records = [dict(r) for r in rows]
    for r in records:
        r["sha256_count"] = sha_count
    _apply_size_human(records)
    if dedupe:
        records = _dedupe_records(records)
    return {"sha256": sha256, "records": records, "sha256_count": sha_count}


@app.get("/api/query/machine")
def query_machine(
    machine_name: str = Query(..., min_length=1),
    sha256: str | None = None,
    limit: int | None = 0,
    dedupe: bool = True,
) -> dict[str, Any]:
    if sha256 is not None and len(sha256) != 64:
        raise HTTPException(status_code=400, detail="sha256 must be 64 hex chars")
    conn = connect()
    try:
        init_db(conn)
        limit_val = _limit_value(limit, max_limit=50000)
        if sha256 is None:
            if limit_val is None:
                rows = conn.execute(
                    """
                    SELECT machine_name, file_path, file_name, size_bytes, sha256, scan_ts, ingested_at, urn
                    FROM file_record
                    WHERE machine_name = ?
                    ORDER BY scan_ts DESC, id DESC
                    """,
                    (machine_name,),
                ).fetchall()
            else:
                rows = conn.execute(
                    """
                    SELECT machine_name, file_path, file_name, size_bytes, sha256, scan_ts, ingested_at, urn
                    FROM file_record
                    WHERE machine_name = ?
                    ORDER BY scan_ts DESC, id DESC
                    LIMIT ?
                    """,
                    (machine_name, limit_val),
                ).fetchall()
        else:
            if limit_val is None:
                rows = conn.execute(
                    """
                    SELECT machine_name, file_path, file_name, size_bytes, sha256, scan_ts, ingested_at, urn
                    FROM file_record
                    WHERE machine_name = ? AND sha256 = ?
                    ORDER BY scan_ts DESC, id DESC
                    """,
                    (machine_name, sha256),
                ).fetchall()
            else:
                rows = conn.execute(
                    """
                    SELECT machine_name, file_path, file_name, size_bytes, sha256, scan_ts, ingested_at, urn
                    FROM file_record
                    WHERE machine_name = ? AND sha256 = ?
                    ORDER BY scan_ts DESC, id DESC
                    LIMIT ?
                    """,
                    (machine_name, sha256, limit_val),
                ).fetchall()

        records = [dict(r) for r in rows]
        if sha256:
            sha_count = conn.execute(
                "SELECT COUNT(*) FROM file_record WHERE machine_name = ? AND sha256 = ?",
                (machine_name, sha256),
            ).fetchone()[0]
            for r in records:
                r["sha256_count"] = sha_count
        else:
            sha_values = sorted({str(r.get("sha256", "")) for r in records if r.get("sha256")})
            if sha_values:
                placeholders = ",".join("?" for _ in sha_values)
                rows = conn.execute(
                    f"""
                    SELECT sha256, COUNT(*) AS c
                    FROM file_record
                    WHERE machine_name = ?
                      AND sha256 IN ({placeholders})
                    GROUP BY sha256
                    """,
                    (machine_name, *sha_values),
                ).fetchall()
                sha_counts = {str(r["sha256"]): int(r["c"]) for r in rows}
                for r in records:
                    r["sha256_count"] = sha_counts.get(str(r.get("sha256", "")), 0)
    finally:
        conn.close()

    _apply_size_human(records)
    if dedupe:
        records = _dedupe_records(records)
    payload: dict[str, Any] = {"machine_name": machine_name, "records": records}
    if sha256:
        payload["sha256"] = sha256
    return payload


@app.get("/api/query/name")
def query_name(
    substring: str = Query(..., min_length=1),
    machine_name: str | None = None,
    limit: int | None = 0,
) -> dict[str, Any]:
    pattern = f"%{substring}%"
    conn = connect()
    try:
        init_db(conn)
        limit_val = _limit_value(limit, max_limit=50000)
        where = "WHERE file_name LIKE ?"
        params: list[object] = [pattern]
        if machine_name:
            where += " AND machine_name = ?"
            params.append(machine_name)
        if limit_val is None:
            rows = conn.execute(
                f"""
                SELECT file_name, sha256, scan_ts, ingested_at
                FROM file_record
                {where}
                ORDER BY file_name ASC, scan_ts DESC, id DESC
                """,
                tuple(params),
            ).fetchall()
        else:
            rows = conn.execute(
                f"""
                SELECT file_name, sha256, scan_ts, ingested_at
                FROM file_record
                {where}
                ORDER BY file_name ASC, scan_ts DESC, id DESC
                LIMIT ?
                """,
                tuple(params + [limit_val]),
            ).fetchall()
    finally:
        conn.close()

    records = [dict(r) for r in rows]
    payload: dict[str, Any] = {"records": records}
    if machine_name:
        payload["machine_name"] = machine_name
    return payload


@app.get("/api/graph/sha256", response_model=None)
def graph_sha256(
    sha256: str = Query(..., min_length=64, max_length=64),
    fmt: str = Query("ascii", pattern="^(ascii|dot|mermaid|json)$"),
    limit: int | None = 20000,
) -> Any:
    conn = connect()
    try:
        init_db(conn)
        segments = fetch_segments_for_sha256(
            conn, sha256=sha256, limit=_limit_value(limit, max_limit=200000) or 20000
        )
    finally:
        conn.close()

    if fmt == "dot":
        return PlainTextResponse(render_dot(segments))
    if fmt == "mermaid":
        return PlainTextResponse(render_mermaid_flowchart(segments))
    if fmt == "json":
        return {"sha256": sha256, "segments": [s.__dict__ for s in segments]}
    return PlainTextResponse(render_ascii_chain(segments))
