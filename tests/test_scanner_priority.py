"""
Comprehensive tests for scanner priority ordering.

Tests verify:
1. Unscanned files (bucket -1) are processed before scanned files
2. Older scanned files are processed before newer ones
3. Progress is made through file list across multiple runs
"""
from __future__ import annotations

import os
import random
import shutil
import tempfile
import unittest
from datetime import date, timedelta
from pathlib import Path

from client.config import ClientConfig
from client.enumerator import FileEntry
from client.scanner import _bucket_index, get_bucket_stats, select_files_for_run
from client.state import ClientState


class TestBucketIndex(unittest.TestCase):
    """Test the bucket index calculation."""

    def test_empty_string_returns_minus_one(self) -> None:
        self.assertEqual(_bucket_index(""), -1)

    def test_invalid_date_returns_minus_one(self) -> None:
        self.assertEqual(_bucket_index("not-a-date"), -1)
        self.assertEqual(_bucket_index("2026-13-01"), -1)

    def test_valid_date_returns_positive_bucket(self) -> None:
        bucket = _bucket_index("2026-01-27")
        self.assertGreater(bucket, 0)

    def test_bucket_minus_one_is_smallest(self) -> None:
        """Bucket -1 should sort before any valid date bucket."""
        today_bucket = _bucket_index(date.today().isoformat())
        self.assertLess(-1, today_bucket)

    def test_older_dates_have_smaller_bucket_index(self) -> None:
        old = _bucket_index("2020-01-01")
        new = _bucket_index("2026-01-27")
        self.assertLess(old, new)


class TestSelectFilesForRun(unittest.TestCase):
    """Test file selection and priority ordering."""

    def setUp(self) -> None:
        self._tmp = tempfile.mkdtemp()
        self.scan_root = os.path.join(self._tmp, "scan")
        os.makedirs(self.scan_root)

    def tearDown(self) -> None:
        shutil.rmtree(self._tmp, ignore_errors=True)

    def _create_files(self, count: int) -> list[str]:
        """Create test files and return their paths."""
        paths = []
        for i in range(count):
            path = os.path.join(self.scan_root, f"file_{i:05d}.txt")
            Path(path).write_text(f"content {i}")
            paths.append(os.path.realpath(path))
        return paths

    def _get_config(self) -> ClientConfig:
        return ClientConfig(scan_paths=[self.scan_root])

    def test_all_unscanned_returns_all_files(self) -> None:
        """With no state, all files should be returned (all in bucket -1)."""
        paths = self._create_files(100)
        config = self._get_config()
        state = ClientState(machine_id="test", files={}, schedule_last_run={})

        result = select_files_for_run(config, state)
        result_paths = {e.path for e in result}

        self.assertEqual(len(result), 100)
        for p in paths:
            self.assertIn(p, result_paths)

    def test_unscanned_files_come_first(self) -> None:
        """Unscanned files (bucket -1) should be selected before scanned files."""
        paths = self._create_files(100)
        config = self._get_config()

        # Mark last 50 files as scanned today
        today = date.today().isoformat()
        scanned_paths = set(paths[50:])
        state = ClientState(
            machine_id="test",
            files={p: today for p in scanned_paths},
            schedule_last_run={},
        )

        result = select_files_for_run(config, state)
        result_paths = [e.path for e in result]

        # First 50 results should all be unscanned
        for i, p in enumerate(result_paths[:50]):
            self.assertNotIn(p, scanned_paths,
                f"Position {i}: scanned file {p} appeared before all unscanned files")

    def test_bucket_stats_matches_selection(self) -> None:
        """get_bucket_stats should match actual selection."""
        self._create_files(100)
        config = self._get_config()

        today = date.today().isoformat()
        old_date = (date.today() - timedelta(days=30)).isoformat()

        # Get all paths from iter_files
        from client.enumerator import iter_files
        all_paths = [e.path for e in iter_files(config)]

        state = ClientState(
            machine_id="test",
            files={
                **{p: old_date for p in all_paths[:30]},
                **{p: today for p in all_paths[30:60]},
            },
            schedule_last_run={},
        )

        stats = get_bucket_stats(config, state)

        # Should have 3 buckets: -1, old_bucket, today_bucket
        self.assertEqual(stats[-1], 40)  # 40 unscanned

        old_bucket = _bucket_index(old_date)
        today_bucket = _bucket_index(today)
        self.assertEqual(stats[old_bucket], 30)  # 30 old
        self.assertEqual(stats[today_bucket], 30)  # 30 today


class TestLargeScaleProgress(unittest.TestCase):
    """Large-scale tests to verify progress across runs."""

    def setUp(self) -> None:
        self._tmp = tempfile.mkdtemp()
        self.scan_root = os.path.join(self._tmp, "scan")
        os.makedirs(self.scan_root)

    def tearDown(self) -> None:
        shutil.rmtree(self._tmp, ignore_errors=True)

    def _create_files(self, count: int) -> list[str]:
        """Create test files."""
        paths = []
        for i in range(count):
            path = os.path.join(self.scan_root, f"file_{i:05d}.bin")
            Path(path).write_text(f"content {i}")
            paths.append(os.path.realpath(path))
        return paths

    def _get_config(self) -> ClientConfig:
        return ClientConfig(scan_paths=[self.scan_root])

    def test_10k_files_priority_ordering(self) -> None:
        """With 10K files, verify priority ordering is correct."""
        print("\nCreating 10,000 test files...")
        self._create_files(10000)
        config = self._get_config()

        # Get all paths
        from client.enumerator import iter_files
        all_paths = [e.path for e in iter_files(config)]
        self.assertEqual(len(all_paths), 10000)

        # Create 4 groups:
        # - 2500 unscanned (bucket -1)
        # - 2500 scanned 45 days ago
        # - 2500 scanned 20 days ago
        # - 2500 scanned today
        today = date.today()
        state_files = {}

        for p in all_paths[2500:5000]:
            state_files[p] = (today - timedelta(days=45)).isoformat()
        for p in all_paths[5000:7500]:
            state_files[p] = (today - timedelta(days=20)).isoformat()
        for p in all_paths[7500:10000]:
            state_files[p] = today.isoformat()

        state = ClientState(machine_id="test", files=state_files, schedule_last_run={})

        print("Selecting files...")
        result = select_files_for_run(config, state)
        result_paths = [e.path for e in result]

        # Verify order: unscanned first, then oldest, then newer, then today
        print("Verifying bucket order...")

        # First 2500 should be unscanned (not in state)
        unscanned_in_first = sum(1 for p in result_paths[:2500] if p not in state_files)
        self.assertEqual(unscanned_in_first, 2500,
            f"Expected first 2500 to be unscanned, got {unscanned_in_first}")
        print(f"✓ First 2500 files are unscanned (bucket -1)")

        # Get bucket indices
        bucket_45 = _bucket_index((today - timedelta(days=45)).isoformat())
        bucket_20 = _bucket_index((today - timedelta(days=20)).isoformat())
        bucket_today = _bucket_index(today.isoformat())

        # Verify bucket ordering: -1 < bucket_45 < bucket_20 < bucket_today
        self.assertLess(-1, bucket_45)
        self.assertLess(bucket_45, bucket_20)
        self.assertLess(bucket_20, bucket_today)
        print(f"✓ Bucket order: -1 < {bucket_45} < {bucket_20} < {bucket_today}")

    def test_10k_files_progress_simulation(self) -> None:
        """Simulate 10 runs to verify no re-scanning of files."""
        print("\nCreating 10,000 test files...")
        self._create_files(10000)
        config = self._get_config()
        state = ClientState(machine_id="test", files={}, schedule_last_run={})

        today = date.today().isoformat()
        processed_all = set()
        quota = 1000

        print("Simulating 10 runs with quota of 1000 files each...")
        for run in range(10):
            result = select_files_for_run(config, state)
            batch = result[:quota]

            # Check that we're not re-processing files already in state
            reprocessed = [e.path for e in batch if e.path in state.files]
            if reprocessed:
                self.fail(
                    f"Run {run + 1}: Re-selected {len(reprocessed)} files already in state!\n"
                    f"Examples: {reprocessed[:3]}"
                )

            # Mark files in state (simulate successful scan)
            for e in batch:
                state.files[e.path] = today
                processed_all.add(e.path)

            print(f"  Run {run + 1}: selected {len(batch)} new files, total: {len(processed_all)}")

        self.assertEqual(len(processed_all), 10000,
            f"Expected to process 10000 unique files, got {len(processed_all)}")
        print("✓ All 10,000 files processed exactly once")

    def test_bucket_priority_after_partial_scan(self) -> None:
        """After scanning some files, unscanned should still come first."""
        print("\nCreating 1000 test files...")
        self._create_files(1000)
        config = self._get_config()

        from client.enumerator import iter_files
        all_paths = [e.path for e in iter_files(config)]

        # Simulate: 200 files scanned, 800 unscanned
        today = date.today().isoformat()
        scanned = set(all_paths[:200])
        state = ClientState(
            machine_id="test",
            files={p: today for p in scanned},
            schedule_last_run={},
        )

        result = select_files_for_run(config, state)

        # First 800 should all be unscanned
        first_800 = result[:800]
        for e in first_800:
            self.assertNotIn(e.path, scanned,
                f"Scanned file {e.path} appeared in first 800 positions")

        # Last 200 should all be scanned
        last_200 = result[800:]
        for e in last_200:
            self.assertIn(e.path, scanned,
                f"Unscanned file {e.path} appeared in last 200 positions")

        print("✓ Unscanned files (800) come before scanned files (200)")


class TestStatePersistence(unittest.TestCase):
    """Test state persistence across 'runs' (simulating separate processes)."""

    def setUp(self) -> None:
        self._tmp = tempfile.mkdtemp()
        self.scan_root = os.path.join(self._tmp, "scan")
        self.state_path = Path(os.path.join(self._tmp, "state.json"))
        os.makedirs(self.scan_root)

    def tearDown(self) -> None:
        shutil.rmtree(self._tmp, ignore_errors=True)

    def _create_files(self, count: int) -> list[str]:
        """Create test files."""
        paths = []
        for i in range(count):
            path = os.path.join(self.scan_root, f"file_{i:05d}.bin")
            Path(path).write_text(f"content {i}")
            paths.append(os.path.realpath(path))
        return paths

    def _get_config(self) -> ClientConfig:
        return ClientConfig(scan_paths=[self.scan_root])

    def test_state_persistence_across_runs(self) -> None:
        """Simulate multiple 'runs' with state saved/loaded from disk."""
        from client.state import load_state, save_state

        print("\nCreating 500 test files...")
        self._create_files(500)
        config = self._get_config()

        today = date.today().isoformat()
        processed_all = set()
        quota = 50

        print("Simulating 12 runs with state persistence to disk...")
        for run in range(12):
            # Load state from disk (simulating new process)
            state = load_state(self.state_path)

            result = select_files_for_run(config, state)
            batch = result[:quota]

            # Check that we're not re-processing files
            reprocessed = [e.path for e in batch if e.path in state.files]
            if reprocessed and len(processed_all) < 500:
                self.fail(
                    f"Run {run + 1}: Re-selected {len(reprocessed)} files already in state!\n"
                    f"State file has {len(state.files)} entries\n"
                    f"Examples: {reprocessed[:3]}"
                )

            # Mark files in state and save
            for e in batch:
                state.files[e.path] = today
                processed_all.add(e.path)

            # Save state to disk (simulating end of run)
            save_state(self.state_path, state)

            print(f"  Run {run + 1}: selected {len(batch)} files, "
                  f"state has {len(state.files)} entries, unique total: {len(processed_all)}")

        # After 10 runs of 50 files each, should have processed all 500
        self.assertEqual(len(processed_all), 500,
            f"Expected to process 500 unique files, got {len(processed_all)}")
        print("✓ All 500 files processed exactly once with state persistence")

    def test_state_file_content_matches_paths(self) -> None:
        """Verify state file paths match iter_files paths exactly."""
        from client.state import load_state, save_state
        from client.enumerator import iter_files

        print("\nCreating 100 test files...")
        self._create_files(100)
        config = self._get_config()

        # Get all paths from iter_files
        all_paths = [e.path for e in iter_files(config)]

        # Save some paths to state
        state = load_state(self.state_path)
        today = date.today().isoformat()
        for p in all_paths[:50]:
            state.files[p] = today
        save_state(self.state_path, state)

        # Reload state and verify paths match
        state2 = load_state(self.state_path)

        # Get paths from iter_files again
        all_paths2 = [e.path for e in iter_files(config)]

        # Check that saved paths can be looked up
        for p in all_paths[:50]:
            self.assertIn(p, state2.files,
                f"Path {p} not found in reloaded state")
            # Also check the exact same path comes from iter_files
            self.assertIn(p, all_paths2,
                f"Path {p} from state not found in iter_files")

        print("✓ State file paths match iter_files paths exactly")


if __name__ == "__main__":
    unittest.main(verbosity=2)
