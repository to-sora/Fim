from __future__ import annotations

import os
import tempfile
import unittest

from server.db import connect, init_db
from server.graph import fetch_segments_for_sha256


class GraphTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self._tmp.name, "fim_test.sqlite3")
        os.environ["FIM_DB_PATH"] = self.db_path
        self.conn = connect()
        init_db(self.conn)

    def tearDown(self) -> None:
        self.conn.close()
        os.environ.pop("FIM_DB_PATH", None)
        self._tmp.cleanup()

    def _insert_record(
        self,
        *,
        machine_name: str,
        file_path: str,
        file_name: str,
        sha256: str,
        scan_ts: str,
    ) -> None:
        self.conn.execute(
            """
            INSERT INTO file_record (
                machine_name,
                file_path,
                file_name,
                size_bytes,
                sha256,
                scan_ts,
                urn,
                ingested_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                machine_name,
                file_path,
                file_name,
                1,
                sha256,
                scan_ts,
                "",
                scan_ts,
            ),
        )
        self.conn.commit()

    def test_segments_group_by_machine_path_name(self) -> None:
        sha = "a" * 64
        self._insert_record(
            machine_name="M1",
            file_path="/a/file.bin",
            file_name="file.bin",
            sha256=sha,
            scan_ts="2026-01-01T00:00:00+00:00",
        )
        self._insert_record(
            machine_name="M1",
            file_path="/a/file.bin",
            file_name="file.bin",
            sha256=sha,
            scan_ts="2026-01-02T00:00:00+00:00",
        )
        self._insert_record(
            machine_name="M1",
            file_path="/b/file.bin",
            file_name="file.bin",
            sha256=sha,
            scan_ts="2026-01-03T00:00:00+00:00",
        )
        self._insert_record(
            machine_name="M1",
            file_path="/b/file2.bin",
            file_name="file2.bin",
            sha256=sha,
            scan_ts="2026-01-04T00:00:00+00:00",
        )
        self._insert_record(
            machine_name="M2",
            file_path="/a/file.bin",
            file_name="file.bin",
            sha256=sha,
            scan_ts="2026-01-05T00:00:00+00:00",
        )

        segments = fetch_segments_for_sha256(self.conn, sha256=sha, limit=20000)
        seg_map = {
            (s.machine_name, s.file_path, s.file_name): (s.start_date, s.end_date)
            for s in segments
        }

        self.assertEqual(len(segments), 4)
        self.assertEqual(len(seg_map), 4)
        self.assertEqual(
            seg_map[("M1", "/a/file.bin", "file.bin")],
            ("2026-01-01", "2026-01-02"),
        )
        self.assertEqual(
            seg_map[("M1", "/b/file.bin", "file.bin")],
            ("2026-01-03", "2026-01-03"),
        )
        self.assertEqual(
            seg_map[("M1", "/b/file2.bin", "file2.bin")],
            ("2026-01-04", "2026-01-04"),
        )
        self.assertEqual(
            seg_map[("M2", "/a/file.bin", "file.bin")],
            ("2026-01-05", "2026-01-05"),
        )


if __name__ == "__main__":
    unittest.main()
