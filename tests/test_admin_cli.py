from __future__ import annotations

import io
import json
import os
import tempfile
import unittest
from contextlib import redirect_stdout
from datetime import datetime
from types import SimpleNamespace

from server.admin_cli import _cmd_query_machine, _cmd_query_name
from server.db import connect, init_db


def _insert_record(
    conn,
    *,
    machine_name: str,
    file_name: str,
    sha256: str,
    scan_ts: str,
    ingested_at: str,
    file_path: str | None = None,
    machine_id: int = 1,
    size_bytes: int = 1,
    extension: str = "bin",
    mac: str = "",
    tag: str = "",
    host_name: str = "",
    client_ip: str = "",
    urn: str | None = None,
) -> None:
    if file_path is None:
        file_path = f"/tmp/{file_name}"
    if urn is None:
        urn = f"{machine_name}:{file_name}:{extension}:1:2026-01-21"
    conn.execute(
        """
        INSERT INTO file_record(
          machine_name, machine_id, mac, file_name, file_path, size_bytes, sha256,
          tag, host_name, client_ip, scan_ts, urn, ingested_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            machine_name,
            machine_id,
            mac,
            file_name,
            file_path,
            size_bytes,
            sha256,
            tag,
            host_name,
            client_ip,
            scan_ts,
            urn,
            ingested_at,
        ),
    )


def _capture_output(func, args) -> str:
    buf = io.StringIO()
    with redirect_stdout(buf):
        func(args)
    return buf.getvalue()


class AdminCliTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self._tmp.name, "fim_test.sqlite3")
        os.environ["FIM_DB_PATH"] = self.db_path
        conn = connect()
        try:
            init_db(conn)
        finally:
            conn.close()

    def tearDown(self) -> None:
        os.environ.pop("FIM_DB_PATH", None)
        self._tmp.cleanup()

    def test_query_name_global_and_machine_filter(self) -> None:
        conn = connect()
        try:
            init_db(conn)
            _insert_record(
                conn,
                machine_name="M1",
                file_name="abc.txt",
                sha256="a" * 64,
                scan_ts="2026-01-21T00:00:00+00:00",
                ingested_at="2026-01-21T00:01+00:00",
            )
            _insert_record(
                conn,
                machine_name="M2",
                file_name="xabcx.bin",
                sha256="b" * 64,
                scan_ts="2026-01-21T00:00:10+00:00",
                ingested_at="2026-01-21T00:02+00:00",
            )
            _insert_record(
                conn,
                machine_name="M2",
                file_name="nomatch.bin",
                sha256="c" * 64,
                scan_ts="2026-01-21T00:00:20+00:00",
                ingested_at="2026-01-21T00:03+00:00",
            )
            conn.commit()
        finally:
            conn.close()

        out = _capture_output(
            _cmd_query_name,
            SimpleNamespace(substring="abc", machine_name=None, limit=0, table=False),
        )
        data = json.loads(out)
        names = {r["file_name"] for r in data["records"]}
        self.assertEqual(names, {"abc.txt", "xabcx.bin"})

        out = _capture_output(
            _cmd_query_name,
            SimpleNamespace(substring="abc", machine_name="M1", limit=0, table=False),
        )
        data = json.loads(out)
        self.assertEqual(len(data["records"]), 1)
        self.assertEqual(data["records"][0]["file_name"], "abc.txt")

    def test_query_machine_no_limit(self) -> None:
        conn = connect()
        try:
            init_db(conn)
            for i in range(250):
                _insert_record(
                    conn,
                    machine_name="M1",
                    file_name=f"f{i}.bin",
                    sha256=format(i, "064x"),
                    scan_ts="2026-01-21T00:00:00+00:00",
                    ingested_at="2026-01-21T00:01+00:00",
                )
            conn.commit()
        finally:
            conn.close()

        out = _capture_output(
            _cmd_query_machine,
            SimpleNamespace(machine_name="M1", limit=0, sha256=None, table=False, human=False),
        )
        data = json.loads(out)
        self.assertEqual(len(data["records"]), 250)

    def test_query_machine_table_headers(self) -> None:
        conn = connect()
        try:
            init_db(conn)
            _insert_record(
                conn,
                machine_name="M1",
                file_name="one.bin",
                sha256="d" * 64,
                scan_ts="2026-01-21T00:00:00+00:00",
                ingested_at="2026-01-21T00:01+00:00",
            )
            conn.commit()
        finally:
            conn.close()

        out = _capture_output(
            _cmd_query_machine,
            SimpleNamespace(machine_name="M1", limit=0, sha256=None, table=True, human=False),
        )
        header = out.splitlines()[0]
        self.assertIn("PATH", header)
        self.assertIn("SCAN_TS", header)
        self.assertIn("INGESTED_AT", header)
        self.assertIn("URN", header)
        # Validate timestamps are parseable in data rows.
        data_line = out.splitlines()[2]
        parts = [p.strip() for p in data_line.split("|")]
        scan_ts = parts[4]
        ingested_at = parts[5]
        datetime.fromisoformat(scan_ts)
        datetime.fromisoformat(ingested_at)

    def test_query_machine_table_sha256_count_populated(self) -> None:
        conn = connect()
        try:
            init_db(conn)
            sha = "f" * 64
            _insert_record(
                conn,
                machine_name="M1",
                file_name="c1.bin",
                sha256=sha,
                scan_ts="2026-01-21T00:00:00+00:00",
                ingested_at="2026-01-21T00:01+00:00",
            )
            _insert_record(
                conn,
                machine_name="M1",
                file_name="c2.bin",
                sha256=sha,
                scan_ts="2026-01-21T00:00:10+00:00",
                ingested_at="2026-01-21T00:02+00:00",
            )
            conn.commit()
        finally:
            conn.close()

        out = _capture_output(
            _cmd_query_machine,
            SimpleNamespace(machine_name="M1", limit=0, sha256=None, table=True, human=False),
        )
        data_line = out.splitlines()[2]
        parts = [p.strip() for p in data_line.split("|")]
        # Column order: MACHINE, PATH, FILE, SIZE, SHA256_COUNT, SCAN_TS, INGESTED_AT, URN
        self.assertEqual(parts[4], "2")

    def test_query_machine_table_counts_sha256(self) -> None:
        conn = connect()
        try:
            init_db(conn)
            sha = "e" * 64
            _insert_record(
                conn,
                machine_name="M1",
                file_name="dup1.bin",
                sha256=sha,
                scan_ts="2026-01-21T00:00:00+00:00",
                ingested_at="2026-01-21T00:01+00:00",
            )
            _insert_record(
                conn,
                machine_name="M1",
                file_name="dup2.bin",
                sha256=sha,
                scan_ts="2026-01-21T00:00:10+00:00",
                ingested_at="2026-01-21T00:02+00:00",
            )
            conn.commit()
        finally:
            conn.close()

        out = _capture_output(
            _cmd_query_machine,
            SimpleNamespace(machine_name="M1", limit=0, sha256=sha, table=True, human=False),
        )
        self.assertIn("records: 2", out)
        header = out.splitlines()[0]
        self.assertIn("SHA256_COUNT", header)


if __name__ == "__main__":
    unittest.main()
