from __future__ import annotations

import asyncio
import json
import os
import tempfile
import unittest
from typing import Any

from server.db import connect, init_db
from server.web_app import app


class LifespanManager:
    def __init__(self, asgi_app: Any):
        self._app = asgi_app
        self._receive_queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
        self._send_queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
        self._task: asyncio.Task[None] | None = None

    async def __aenter__(self) -> "LifespanManager":
        async def run() -> None:
            scope = {"type": "lifespan"}

            async def receive() -> dict[str, Any]:
                return await self._receive_queue.get()

            async def send(message: dict[str, Any]) -> None:
                await self._send_queue.put(message)

            await self._app(scope, receive, send)

        self._task = asyncio.create_task(run())
        await self._receive_queue.put({"type": "lifespan.startup"})

        message = await self._send_queue.get()
        if message["type"] != "lifespan.startup.complete":
            raise RuntimeError(f"unexpected lifespan message: {message!r}")

        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:  # type: ignore[no-untyped-def]
        await self._receive_queue.put({"type": "lifespan.shutdown"})
        message = await self._send_queue.get()
        if message["type"] != "lifespan.shutdown.complete":
            raise RuntimeError(f"unexpected lifespan message: {message!r}")
        assert self._task is not None
        await self._task


async def asgi_get(asgi_app: Any, path: str) -> tuple[int, dict[str, str], bytes]:
    scope = {
        "type": "http",
        "asgi": {"version": "3.0", "spec_version": "2.3"},
        "http_version": "1.1",
        "method": "GET",
        "scheme": "http",
        "path": path,
        "raw_path": path.encode("utf-8"),
        "query_string": b"",
        "headers": [(b"content-length", b"0")],
        "client": ("testclient", 123),
        "server": ("testserver", 80),
    }

    recv_queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
    await recv_queue.put({"type": "http.request", "body": b"", "more_body": False})
    await recv_queue.put({"type": "http.disconnect"})

    async def receive() -> dict[str, Any]:
        return await recv_queue.get()

    status_code: int | None = None
    response_headers: list[tuple[bytes, bytes]] = []
    body_parts: list[bytes] = []

    async def send(message: dict[str, Any]) -> None:
        nonlocal status_code, response_headers
        if message["type"] == "http.response.start":
            status_code = int(message["status"])
            response_headers = list(message.get("headers", []))
        elif message["type"] == "http.response.body":
            body_parts.append(message.get("body", b""))

    await asgi_app(scope, receive, send)
    assert status_code is not None
    headers: dict[str, str] = {}
    for k, v in response_headers:
        headers[k.decode("latin-1").lower()] = v.decode("latin-1")
    return status_code, headers, b"".join(body_parts)


class WebUiTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self._tmp.name, "fim_test.sqlite3")
        os.environ["FIM_DB_PATH"] = self.db_path
        conn = connect()
        try:
            init_db(conn)
            conn.execute(
                """
                INSERT INTO file_record (
                    machine_name, file_path, file_name, size_bytes, sha256, scan_ts, ingested_at, urn
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "MachineA",
                    "/tmp/a.bin",
                    "a.bin",
                    1234,
                    "a" * 64,
                    "2026-01-01T00:00:00+00:00",
                    "2026-01-01T00:01:00+00:00",
                    "MachineA:a.bin:bin:1:2026-01-01",
                ),
            )
            conn.commit()
        finally:
            conn.close()

        self._lifespan = LifespanManager(app)
        await self._lifespan.__aenter__()

    async def asyncTearDown(self) -> None:
        await self._lifespan.__aexit__(None, None, None)
        os.environ.pop("FIM_DB_PATH", None)
        self._tmp.cleanup()

    async def test_query_file(self) -> None:
        status, _, body = await asgi_get(app, "/api/query/file?sha256=" + ("a" * 64))
        self.assertEqual(status, 200)
        payload = json.loads(body)
        self.assertEqual(payload["sha256"], "a" * 64)
        self.assertEqual(len(payload["records"]), 1)

    async def test_query_name(self) -> None:
        status, _, body = await asgi_get(app, "/api/query/name?substring=a.bin")
        self.assertEqual(status, 200)
        payload = json.loads(body)
        self.assertEqual(len(payload["records"]), 1)

    async def test_machines_list(self) -> None:
        status, _, body = await asgi_get(app, "/api/machines")
        self.assertEqual(status, 200)
        payload = json.loads(body)
        self.assertIn("MachineA", payload["machines"])


if __name__ == "__main__":
    unittest.main()
