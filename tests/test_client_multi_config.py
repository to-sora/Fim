from __future__ import annotations

import json
import os
import tempfile
import unittest
from pathlib import Path

from client.multi_config import discover_config_paths, verify_config_schedules


def _write_config(path: Path, schedule: dict[str, int], tag: str = "") -> None:
    data = {
        "tag": tag,
        "schedule_quota_gb": schedule,
    }
    path.write_text(json.dumps(data), encoding="utf-8")


class MultiConfigTests(unittest.TestCase):
    def test_conflict_detected(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            cfg1 = Path(tmpdir) / "FIM_config_01.json"
            cfg2 = Path(tmpdir) / "FIM_config_02.json"
            _write_config(cfg1, {"Mon0900": 10}, tag="a")
            _write_config(cfg2, {"Mon0903": 10}, tag="b")

            paths = discover_config_paths(tmpdir, "FIM_config_[0-9]*.json")
            report = verify_config_schedules(paths, min_gap_min=5)
            self.assertEqual(report["status"], "conflict")
            self.assertEqual(len(report["conflicts"]), 1)

    def test_no_conflict_ok(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            cfg1 = Path(tmpdir) / "FIM_config_01.json"
            cfg2 = Path(tmpdir) / "FIM_config_02.json"
            _write_config(cfg1, {"Mon0900": 10}, tag="a")
            _write_config(cfg2, {"Mon0910": 10}, tag="b")

            paths = discover_config_paths(tmpdir, "FIM_config_[0-9]*.json")
            report = verify_config_schedules(paths, min_gap_min=5)
            self.assertEqual(report["status"], "ok")
            self.assertEqual(report["conflicts"], [])

    def test_invalid_key_reported(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            cfg1 = Path(tmpdir) / "FIM_config_01.json"
            _write_config(cfg1, {"Bad0900": 10}, tag="a")

            paths = discover_config_paths(tmpdir, "FIM_config_[0-9]*.json")
            report = verify_config_schedules(paths, min_gap_min=5)
            self.assertEqual(report["status"], "conflict")
            self.assertEqual(len(report["invalid_keys"]), 1)
