from __future__ import annotations

import os
import tempfile
import unittest

from fastapi.testclient import TestClient

from server.auth import create_or_rotate_token
from server.db import connect, init_db
from server.main import app


class ServerTests(unittest.TestCase):
    def setUp(self) -> None:
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

    def tearDown(self) -> None:
        os.environ.pop("FIM_DB_PATH", None)
        self._tmp.cleanup()

    def _client(self) -> TestClient:
        return TestClient(app)

    def test_ingest_requires_auth(self) -> None:
        with self._client() as client:
            resp = client.post("/ingest", json={"records": []})
        self.assertEqual(resp.status_code, 401)

    def test_ingest_invalid_token(self) -> None:
        with self._client() as client:
            resp = client.post(
                "/ingest",
                headers={"Authorization": "Bearer not-a-real-token"},
                json={"records": []},
            )
        self.assertEqual(resp.status_code, 401)

    def test_ingest_change_detection(self) -> None:
        with self._client() as client:
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
            resp1 = client.post(
                "/ingest",
                headers={"Authorization": f"Bearer {self.token}"},
                json={**base, "records": [rec1]},
            )
            self.assertEqual(resp1.status_code, 200, resp1.text)
            body1 = resp1.json()
            self.assertEqual(body1["inserted"], 1)
            self.assertEqual(body1["changed"], [])

            rec2 = dict(rec1)
            rec2["sha256"] = "1" * 64
            rec2["scan_ts"] = 2
            resp2 = client.post(
                "/ingest",
                headers={"Authorization": f"Bearer {self.token}"},
                json={**base, "records": [rec2]},
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

    def test_duplicates_reporting(self) -> None:
        with self._client() as client:
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
            resp = client.post(
                "/ingest",
                headers={"Authorization": f"Bearer {self.token}"},
                json={**base, "records": recs},
            )
            self.assertEqual(resp.status_code, 200, resp.text)
            body = resp.json()
            self.assertEqual(body["inserted"], 2)
            self.assertTrue(body["duplicates"])

            file_resp = client.get(f"/file/{sha}")
            self.assertEqual(file_resp.status_code, 200, file_resp.text)
            file_body = file_resp.json()
            self.assertEqual(file_body["sha256"], sha)
            self.assertGreaterEqual(len(file_body["records"]), 2)


if __name__ == "__main__":
    unittest.main()

