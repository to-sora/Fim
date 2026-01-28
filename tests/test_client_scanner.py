from __future__ import annotations

import os
import tempfile
import unittest

from client.scanner import scan_files, select_files_for_run
from client.state import ClientState


class ClientScannerTests(unittest.TestCase):
    def _write_file(self, root: str, name: str, size: int = 1) -> str:
        path = os.path.join(root, name)
        with open(path, "wb") as f:
            f.write(b"x" * size)
        return path

    def test_select_files_orders_by_last_scan(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path_a = self._write_file(tmp, "a.txt")
            path_b = self._write_file(tmp, "b.txt")
            path_c = self._write_file(tmp, "c.txt")

            class StubConfig:
                scan_paths = [tmp]
                exclude_subdirs: list[str] = []
                exclude_extensions: list[str] = []
                size_threshold_kb_by_ext: dict = {}

            config = StubConfig()
            state = ClientState(
                machine_id="m1",
                files={
                    path_a: "2026-01-01T00:00:00+00:00",
                    path_c: "2026-01-05T00:00:00+00:00",
                },
                schedule_last_run={},
            )

            ordered = select_files_for_run(config, state)
            ordered_names = [os.path.basename(entry.path) for entry in ordered]
            self.assertEqual(ordered_names, ["b.txt"])

            state.files[path_b] = "2026-01-03T00:00:00+00:00"
            ordered = select_files_for_run(config, state)
            ordered_names = [os.path.basename(entry.path) for entry in ordered]
            self.assertEqual(ordered_names, ["a.txt", "b.txt", "c.txt"])

    def test_round_robin_progresses_with_large_pool(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            for i in range(10_000):
                self._write_file(tmp, f"file_{i:05d}.bin")

            class StubConfig:
                scan_paths = [tmp]
                exclude_subdirs: list[str] = []
                exclude_extensions: list[str] = []
                size_threshold_kb_by_ext: dict = {}

            config = StubConfig()
            state = ClientState(machine_id="m2", files={}, schedule_last_run={})

            seen: set[str] = set()
            for _ in range(200):
                records, _ = scan_files(config=config, state=state, quota_gb=0)
                self.assertEqual(len(records), 1)
                rec = records[0]
                self.assertNotIn(rec.file_path, seen)
                seen.add(rec.file_path)
                state.files[rec.file_path] = rec.scan_ts
