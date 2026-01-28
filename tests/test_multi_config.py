from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from client.config import ClientConfig
from client.multi_config import (
    derive_paths_from_tag,
    derive_tag_from_config,
    discover_configs,
    parse_schedule_key_to_minutes,
    validate_scan_path_disjoint,
    validate_schedule_spacing,
)


class TestDiscoverConfigs(unittest.TestCase):
    def test_discover_configs_empty_dir(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            configs = discover_configs(Path(tmpdir))
            self.assertEqual(configs, [])

    def test_discover_configs_finds_matching_files(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir)
            (path / "FIM_config_01.json").write_text("{}")
            (path / "FIM_config_02.json").write_text("{}")
            (path / "other.json").write_text("{}")
            (path / "FIM_config_abc.json").write_text("{}")

            configs = discover_configs(path)
            self.assertEqual(len(configs), 2)
            self.assertEqual(configs[0][0], "01")
            self.assertEqual(configs[1][0], "02")

    def test_discover_configs_sorted_by_id(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir)
            (path / "FIM_config_10.json").write_text("{}")
            (path / "FIM_config_02.json").write_text("{}")
            (path / "FIM_config_1.json").write_text("{}")

            configs = discover_configs(path)
            ids = [c[0] for c in configs]
            self.assertEqual(ids, ["1", "02", "10"])

    def test_discover_configs_nonexistent_dir(self) -> None:
        configs = discover_configs(Path("/nonexistent/path"))
        self.assertEqual(configs, [])


class TestDeriveTagFromConfig(unittest.TestCase):
    def test_uses_config_tag_if_present(self) -> None:
        config = ClientConfig(tag="my_tag")
        tag = derive_tag_from_config(Path("FIM_config_01.json"), config)
        self.assertEqual(tag, "my_tag")

    def test_derives_from_filename_if_no_tag(self) -> None:
        config = ClientConfig(tag="")
        tag = derive_tag_from_config(Path("FIM_config_01.json"), config)
        self.assertEqual(tag, "config_01")

    def test_strips_whitespace_from_tag(self) -> None:
        config = ClientConfig(tag="  my_tag  ")
        tag = derive_tag_from_config(Path("FIM_config_01.json"), config)
        self.assertEqual(tag, "my_tag")

    def test_uses_stem_for_non_standard_name(self) -> None:
        config = ClientConfig(tag="")
        tag = derive_tag_from_config(Path("custom_config.json"), config)
        self.assertEqual(tag, "custom_config")


class TestDerivePathsFromTag(unittest.TestCase):
    def test_derives_correct_paths(self) -> None:
        paths = derive_paths_from_tag(Path("/app"), "group1")
        self.assertEqual(paths["state_path"], Path("/app/data/state/group1.json"))
        self.assertEqual(paths["lock_path"], Path("/app/data/locks/group1.lock"))
        self.assertEqual(paths["log_path"], Path("/app/log/client_group1.log"))


class TestParseScheduleKeyToMinutes(unittest.TestCase):
    def test_parse_mon_0000(self) -> None:
        result = parse_schedule_key_to_minutes("Mon0000")
        self.assertEqual(result, 0)

    def test_parse_mon_0910(self) -> None:
        result = parse_schedule_key_to_minutes("Mon0910")
        self.assertEqual(result, 9 * 60 + 10)

    def test_parse_tue_1230(self) -> None:
        result = parse_schedule_key_to_minutes("Tue1230")
        self.assertEqual(result, 1 * 24 * 60 + 12 * 60 + 30)

    def test_parse_sun_2359(self) -> None:
        result = parse_schedule_key_to_minutes("Sun2359")
        self.assertEqual(result, 6 * 24 * 60 + 23 * 60 + 59)

    def test_invalid_day(self) -> None:
        result = parse_schedule_key_to_minutes("Xyz0910")
        self.assertIsNone(result)

    def test_invalid_time_format(self) -> None:
        result = parse_schedule_key_to_minutes("Mon91")
        self.assertIsNone(result)

    def test_invalid_hour(self) -> None:
        result = parse_schedule_key_to_minutes("Mon2500")
        self.assertIsNone(result)

    def test_invalid_minute(self) -> None:
        result = parse_schedule_key_to_minutes("Mon0960")
        self.assertIsNone(result)

    def test_too_short(self) -> None:
        result = parse_schedule_key_to_minutes("Mon09")
        self.assertIsNone(result)


class TestValidateScanPathDisjoint(unittest.TestCase):
    def test_no_overlap(self) -> None:
        configs = [
            ("tag1", ClientConfig(scan_paths=["/tmp/a"])),
            ("tag2", ClientConfig(scan_paths=["/tmp/b"])),
        ]
        warnings = validate_scan_path_disjoint(configs)
        self.assertEqual(warnings, [])

    def test_detect_overlap_parent_child(self) -> None:
        configs = [
            ("tag1", ClientConfig(scan_paths=["/tmp"])),
            ("tag2", ClientConfig(scan_paths=["/tmp/subdir"])),
        ]
        warnings = validate_scan_path_disjoint(configs)
        self.assertEqual(len(warnings), 1)
        self.assertIn("Overlap", warnings[0])
        self.assertIn("tag1", warnings[0])
        self.assertIn("tag2", warnings[0])

    def test_detect_exact_same_path(self) -> None:
        configs = [
            ("tag1", ClientConfig(scan_paths=["/tmp/same"])),
            ("tag2", ClientConfig(scan_paths=["/tmp/same"])),
        ]
        warnings = validate_scan_path_disjoint(configs)
        self.assertEqual(len(warnings), 1)

    def test_multiple_overlaps(self) -> None:
        configs = [
            ("tag1", ClientConfig(scan_paths=["/a", "/b"])),
            ("tag2", ClientConfig(scan_paths=["/a/sub", "/c"])),
            ("tag3", ClientConfig(scan_paths=["/b/sub"])),
        ]
        warnings = validate_scan_path_disjoint(configs)
        self.assertEqual(len(warnings), 2)


class TestValidateScheduleSpacing(unittest.TestCase):
    def test_no_conflict_different_times(self) -> None:
        configs = [
            ("tag1", ClientConfig(schedule_quota_gb={"Mon0900": 1})),
            ("tag2", ClientConfig(schedule_quota_gb={"Mon0920": 1})),
        ]
        warnings = validate_schedule_spacing(configs, min_gap_minutes=5)
        self.assertEqual(warnings, [])

    def test_conflict_close_times(self) -> None:
        configs = [
            ("tag1", ClientConfig(schedule_quota_gb={"Mon0900": 1})),
            ("tag2", ClientConfig(schedule_quota_gb={"Mon0903": 1})),
        ]
        warnings = validate_schedule_spacing(configs, min_gap_minutes=5)
        self.assertEqual(len(warnings), 1)
        self.assertIn("3 minutes", warnings[0])

    def test_same_config_different_times_no_warning(self) -> None:
        configs = [
            ("tag1", ClientConfig(schedule_quota_gb={"Mon0900": 1, "Mon0901": 1})),
        ]
        warnings = validate_schedule_spacing(configs, min_gap_minutes=5)
        self.assertEqual(warnings, [])

    def test_wrap_around_week(self) -> None:
        configs = [
            ("tag1", ClientConfig(schedule_quota_gb={"Sun2358": 1})),
            ("tag2", ClientConfig(schedule_quota_gb={"Mon0001": 1})),
        ]
        warnings = validate_schedule_spacing(configs, min_gap_minutes=5)
        self.assertEqual(len(warnings), 1)

    def test_no_conflicts_empty_schedules(self) -> None:
        configs = [
            ("tag1", ClientConfig(schedule_quota_gb={})),
            ("tag2", ClientConfig(schedule_quota_gb={})),
        ]
        warnings = validate_schedule_spacing(configs, min_gap_minutes=5)
        self.assertEqual(warnings, [])


if __name__ == "__main__":
    unittest.main()
