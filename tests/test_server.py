from __future__ import annotations

import asyncio
import json
import os
import tempfile
import unittest
from dataclasses import dataclass
from typing import Any

from server.auth import create_or_rotate_token
from server.db import connect, init_db
from server.main import app


@dataclass(frozen=True, slots=True)
class ASGIResponse:
    status_code: int
    headers: dict[str, str]
    body: bytes

    @property
    def text(self) -> str:
        return self.body.decode("utf-8", errors="replace")

    def json(self) -> Any:
        return json.loads(self.body)


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
        if message["type"] == "lifespan.startup.failed":
            raise RuntimeError(f"lifespan startup failed: {message!r}")
        if message["type"] != "lifespan.startup.complete":
            raise RuntimeError(f"unexpected lifespan message: {message!r}")

        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:  # type: ignore[no-untyped-def]
        await self._receive_queue.put({"type": "lifespan.shutdown"})

        message = await self._send_queue.get()
        if message["type"] == "lifespan.shutdown.failed":
            raise RuntimeError(f"lifespan shutdown failed: {message!r}")
        if message["type"] != "lifespan.shutdown.complete":
            raise RuntimeError(f"unexpected lifespan message: {message!r}")

        assert self._task is not None
        await self._task


async def asgi_request(
    asgi_app: Any,
    *,
    method: str,
    path: str,
    headers: dict[str, str] | None = None,
    json_body: Any | None = None,
) -> ASGIResponse:
    body = b""
    request_headers = dict(headers or {})
    if json_body is not None:
        body = json.dumps(json_body).encode("utf-8")
        request_headers.setdefault("content-type", "application/json")

    raw_headers: list[tuple[bytes, bytes]] = []
    for k, v in request_headers.items():
        raw_headers.append((k.lower().encode("latin-1"), v.encode("latin-1")))
    raw_headers.append((b"content-length", str(len(body)).encode("latin-1")))

    scope = {
        "type": "http",
        "asgi": {"version": "3.0", "spec_version": "2.3"},
        "http_version": "1.1",
        "method": method.upper(),
        "scheme": "http",
        "path": path,
        "raw_path": path.encode("utf-8"),
        "query_string": b"",
        "headers": raw_headers,
        "client": ("testclient", 123),
        "server": ("testserver", 80),
    }

    recv_queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
    await recv_queue.put({"type": "http.request", "body": body, "more_body": False})
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
        else:
            raise AssertionError(f"unexpected ASGI message: {message!r}")

    await asgi_app(scope, receive, send)

    if status_code is None:
        raise AssertionError("app did not send http.response.start")

    normalized_headers: dict[str, str] = {}
    for k, v in response_headers:
        normalized_headers[k.decode("latin-1").lower()] = v.decode("latin-1")

    return ASGIResponse(status_code=status_code, headers=normalized_headers, body=b"".join(body_parts))


class ServerTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self._tmp.name, "fim_test.sqlite3")
        os.environ["FIM_DB_PATH"] = self.db_path
        conn = connect()
        try:
            init_db(conn)
            self.machine_name = "MachineNameA"
            self.token = create_or_rotate_token(conn, self.machine_name)
        finally:
            conn.close()

        self._lifespan = LifespanManager(app)
        await self._lifespan.__aenter__()

    async def asyncTearDown(self) -> None:
        os.environ.pop("FIM_DB_PATH", None)
        await self._lifespan.__aexit__(None, None, None)
        self._tmp.cleanup()

    async def test_hello(self) -> None:
        resp = await asgi_request(app, method="GET", path="/hello")
        self.assertEqual(resp.status_code, 200, resp.text)
        self.assertEqual(resp.text.strip(), "Hello")

    async def test_ingest_requires_auth(self) -> None:
        resp = await asgi_request(app, method="POST", path="/ingest", json_body={"records": []})
        self.assertEqual(resp.status_code, 401)

    async def test_ingest_invalid_token(self) -> None:
        resp = await asgi_request(
            app,
            method="POST",
            path="/ingest",
            headers={"Authorization": "Bearer not-a-real-token"},
            json_body={"records": []},
        )
        self.assertEqual(resp.status_code, 401)

    async def test_ingest_change_detection(self) -> None:
        base = {
            "machine_id": "id1",
            "mac": "00:11:22:33:44:55",
            "host_name": "host1",
            "tag": "test",
        }
        rec1 = {
            "file_path": "/tmp/a.txt",
            "file_name": "a.txt",
            "extension": "txt",
            "size_bytes": 10,
            "sha256": "0" * 64,
            "scan_ts": 1,
            "urn": f"{self.machine_name}:a.txt:txt:1:2026-01-21",
        }
        resp1 = await asgi_request(
            app,
            method="POST",
            path="/ingest",
            headers={"Authorization": f"Bearer {self.token}"},
            json_body={**base, "records": [rec1]},
        )
        self.assertEqual(resp1.status_code, 200, resp1.text)
        body1 = resp1.json()
        self.assertEqual(body1["inserted"], 1)
        self.assertEqual(body1["changed"], [])

        rec2 = dict(rec1)
        rec2["sha256"] = "1" * 64
        rec2["scan_ts"] = 2
        resp2 = await asgi_request(
            app,
            method="POST",
            path="/ingest",
            headers={"Authorization": f"Bearer {self.token}"},
            json_body={**base, "records": [rec2]},
        )
        self.assertEqual(resp2.status_code, 200, resp2.text)
        body2 = resp2.json()
        self.assertEqual(body2["inserted"], 1)
        self.assertEqual(
            body2["changed"],
            [
                {
                    "file_path": "/tmp/a.txt",
                    "previous_sha256": "0" * 64,
                    "new_sha256": "1" * 64,
                }
            ],
        )

    async def test_duplicates_reporting(self) -> None:
        base = {"machine_id": "id1", "mac": "", "host_name": "host1", "tag": "test"}
        sha = "2" * 64
        recs = [
            {
                "file_path": "/tmp/a.bin",
                "file_name": "a.bin",
                "extension": "bin",
                "size_bytes": 10,
                "sha256": sha,
                "scan_ts": 1,
                "urn": f"{self.machine_name}:a.bin:bin:1:2026-01-21",
            },
            {
                "file_path": "/tmp/b.bin",
                "file_name": "b.bin",
                "extension": "bin",
                "size_bytes": 10,
                "sha256": sha,
                "scan_ts": 2,
                "urn": f"{self.machine_name}:b.bin:bin:1:2026-01-21",
            },
        ]
        resp = await asgi_request(
            app,
            method="POST",
            path="/ingest",
            headers={"Authorization": f"Bearer {self.token}"},
            json_body={**base, "records": recs},
        )
        self.assertEqual(resp.status_code, 200, resp.text)
        body = resp.json()
        self.assertEqual(body["inserted"], 2)
        self.assertTrue(body["duplicates"])

        file_resp = await asgi_request(app, method="GET", path=f"/file/{sha}")
        self.assertEqual(file_resp.status_code, 200, file_resp.text)
        file_body = file_resp.json()
        self.assertEqual(file_body["sha256"], sha)
        self.assertGreaterEqual(len(file_body["records"]), 2)


if __name__ == "__main__":
    unittest.main()
