from __future__ import annotations

import asyncio
import sqlite3
from typing import Any

from .db import connect


INSERT_SQL = """
INSERT INTO file_record(
  machine_name, machine_id, mac, file_name, file_path, size_bytes, sha256,
  tag, host_name, client_ip, scan_ts, urn
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""


class BufferFullError(RuntimeError):
    pass


class IngestBuffer:
    def __init__(
        self,
        *,
        flush_interval_sec: float = 0.5,
        flush_max_rows: int = 1000,
        max_pending_rows: int = 50_000,
    ) -> None:
        self._flush_interval_sec = float(flush_interval_sec)
        self._flush_max_rows = int(flush_max_rows)
        self._max_pending_rows = int(max_pending_rows)

        self._pending_rows: list[tuple[Any, ...]] = []
        self._latest_sha_by_machine_path: dict[tuple[str, str], str] = {}

        self._lock = asyncio.Lock()
        self._wakeup = asyncio.Event()
        self._stop_requested = asyncio.Event()
        self._task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        if self._task is not None:
            return
        self._task = asyncio.create_task(self._run(), name="fim_ingest_buffer")

    async def stop(self) -> None:
        self._stop_requested.set()
        self._wakeup.set()
        task = self._task
        if task is None:
            return
        await task
        self._task = None

    async def pending_count(self) -> int:
        async with self._lock:
            return len(self._pending_rows)

    async def cached_latest_sha_by_path(
        self, *, machine_name: str, file_paths: list[str]
    ) -> dict[str, str]:
        async with self._lock:
            out: dict[str, str] = {}
            for p in file_paths:
                v = self._latest_sha_by_machine_path.get((machine_name, p))
                if v is not None:
                    out[p] = v
            return out

    async def prime_latest_sha_by_path(
        self, *, machine_name: str, latest_sha_by_path: dict[str, str]
    ) -> None:
        if not latest_sha_by_path:
            return
        async with self._lock:
            for p, sha in latest_sha_by_path.items():
                self._latest_sha_by_machine_path.setdefault((machine_name, p), sha)

    async def enqueue(
        self,
        *,
        machine_name: str,
        rows: list[tuple[Any, ...]],
        latest_sha_updates: dict[str, str],
    ) -> None:
        if not rows:
            return
        async with self._lock:
            if len(self._pending_rows) + len(rows) > self._max_pending_rows:
                raise BufferFullError("server ingest buffer is full; try again")
            self._pending_rows.extend(rows)
            for p, sha in latest_sha_updates.items():
                self._latest_sha_by_machine_path[(machine_name, p)] = sha
        self._wakeup.set()

    async def flush(self, *, max_rows: int | None = None) -> int:
        if max_rows is None:
            max_rows = 1_000_000_000
        max_rows = max(1, int(max_rows))

        async with self._lock:
            if not self._pending_rows:
                return 0
            n = min(len(self._pending_rows), max_rows)
            batch = self._pending_rows[:n]
            del self._pending_rows[:n]

        try:
            conn = connect()
            try:
                conn.executemany(INSERT_SQL, batch)
                conn.commit()
            finally:
                conn.close()
        except sqlite3.Error:
            async with self._lock:
                # Prepend so order is preserved as best as possible.
                self._pending_rows = batch + self._pending_rows
            raise

        return len(batch)

    async def _run(self) -> None:
        while True:
            try:
                await asyncio.wait_for(self._wakeup.wait(), timeout=self._flush_interval_sec)
            except TimeoutError:
                pass
            self._wakeup.clear()

            try:
                while True:
                    flushed = await self.flush(max_rows=self._flush_max_rows)
                    if flushed == 0:
                        break
            except sqlite3.Error:
                # Best-effort buffering: keep data in memory and retry later.
                await asyncio.sleep(min(self._flush_interval_sec, 2.0))

            if self._stop_requested.is_set():
                try:
                    while True:
                        flushed = await self.flush(max_rows=self._flush_max_rows)
                        if flushed == 0:
                            break
                except sqlite3.Error:
                    # Give up on shutdown flush if DB is unavailable.
                    pass
                return

