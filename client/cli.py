from __future__ import annotations

import argparse
import json
import time
from datetime import datetime, timezone
from pathlib import Path

from .config import load_config
from .enumerator import iter_files
from .scanner import get_bucket_stats, scan_files, select_files_for_run
from .state import SingleInstance, load_state, save_state
from .uploader import ensure_server_hello, upload_records
from .utils import format_bytes, get_host_name, get_mac_address, iso_now, normalize_path, setup_logger


def _cmd_dry_run(args: argparse.Namespace) -> int:
    config = load_config(args.config)
    total_files = 0
    total_bytes = 0
    for entry in iter_files(config):
        total_files += 1
        total_bytes += entry.size_bytes
        if args.list:
            payload = {"path": normalize_path(entry.path), "size_bytes": entry.size_bytes}
            if args.human:
                payload["size_human"] = format_bytes(entry.size_bytes)
            print(json.dumps(payload))
    summary = {
        "total_files": total_files,
        "total_bytes": total_bytes,
        "total_gb": round(total_bytes / (1024**3), 3),
    }
    if args.human:
        summary["total_size"] = format_bytes(total_bytes)
    print(
        json.dumps(
            summary
        )
    )
    return 0


def _print_ingest_summary(resp: dict) -> None:
    inserted = resp.get("inserted")
    changed = resp.get("changed") or []
    duplicates = resp.get("duplicates") or []
    print(json.dumps({"inserted": inserted, "changed": changed, "duplicates": duplicates}, indent=2))


def _cmd_run(args: argparse.Namespace) -> int:
    config = load_config(args.config)
    state_path = Path(args.state_path)
    logger = setup_logger(Path(args.log_path))

    lock_path = state_path.with_suffix(state_path.suffix + ".lock")
    with SingleInstance(lock_path):
        # Load state INSIDE the lock to avoid race conditions
        state = load_state(state_path)

        # Log bucket stats for debugging
        bucket_stats = get_bucket_stats(config, state)
        unscanned_count = bucket_stats.get(-1, 0)
        scanned_count = sum(v for k, v in bucket_stats.items() if k != -1)
        logger.info(json.dumps({
            "event": "bucket_stats",
            "total_files": sum(bucket_stats.values()),
            "state_entries": len(state.files),
            "unscanned_count": unscanned_count,
            "scanned_count": scanned_count,
            "ts": iso_now(),
        }))
        # Print summary to stdout for visibility
        print(json.dumps({
            "status": "run_start",
            "unscanned": unscanned_count,
            "scanned": scanned_count,
            "state_entries": len(state.files),
        }))

        if not config.server_url:
            print(json.dumps({"status": "config_error", "error": "server_url is empty in config"}))
            return 2
        if not config.auth_token:
            print(json.dumps({"status": "config_error", "error": "auth_token is empty in config"}))
            return 2
        try:
            ensure_server_hello(config=config)
        except Exception as e:
            print(json.dumps({"status": "server_unavailable", "error": str(e)}))
            return 2

        quota_gb = args.quota_gb
        logger.info(json.dumps({"event": "scan_start", "quota_gb": quota_gb, "ts": iso_now()}))
        records, scanned_bytes = scan_files(config=config, state=state, quota_gb=quota_gb)
        if not records:
            print(json.dumps({"scanned_files": 0, "scanned_bytes": 0}))
            return 0

        mac = get_mac_address()
        host = get_host_name()

        batch_size = min(30, max(1, int(config.max_batch_records)))
        logger.info(
            json.dumps(
                {
                    "event": "upload_request",
                    "record_count": len(records),
                    "batch_size": batch_size,
                    "scanned_bytes": scanned_bytes,
                    "scanned_size": format_bytes(scanned_bytes),
                    "ts": iso_now(),
                }
            )
        )
        upload_errors: list[str] = []
        for i in range(0, len(records), batch_size):
            batch = records[i : i + batch_size]
            try:
                resp = upload_records(
                    config=config,
                    mac=mac,
                    host_name=host,
                    records=batch,
                )
            except Exception as e:
                upload_errors.append(str(e))
                print(json.dumps({"status": "upload_error", "batch_index": i, "error": str(e)}))
                break
            else:
                _print_ingest_summary(resp)
                today = datetime.now(timezone.utc).date().isoformat()
                for r in batch:
                    state.files[r.file_path] = today
                save_state(state_path, state)

        print(
            json.dumps(
                {
                    "scanned_files": len(records),
                    "scanned_bytes": scanned_bytes,
                    "scanned_gb": round(scanned_bytes / (1024**3), 3),
                    **({"scanned_size": format_bytes(scanned_bytes)} if args.human else {}),
                }
            )
        )
        return 1 if upload_errors else 0


def _now_schedule_key() -> str:
    now = datetime.now()
    day = now.strftime("%a")
    hm = now.strftime("%H%M")
    return f"{day}{hm}"


def _cmd_daemon(args: argparse.Namespace) -> int:
    config = load_config(args.config)
    state_path = Path(args.state_path)
    state = load_state(state_path)
    lock_path = state_path.with_suffix(state_path.suffix + ".lock")
    logger = setup_logger(Path(args.log_path))

    with SingleInstance(lock_path):
        if not config.server_url:
            print(json.dumps({"status": "config_error", "error": "server_url is empty in config"}))
            return 2
        if not config.auth_token:
            print(json.dumps({"status": "config_error", "error": "auth_token is empty in config"}))
            return 2
        logger.info(json.dumps({"event": "scheduler_start", "ts": iso_now()}))
        print(json.dumps({"status": "daemon_started", "ts": iso_now()}))
        while True:
            key = _now_schedule_key()
            quota = config.schedule_quota_gb.get(key)
            if quota is not None and quota > 0:
                today = datetime.now(timezone.utc).date().isoformat()
                if state.schedule_last_run.get(key) != today:
                    print(json.dumps({"status": "schedule_trigger", "key": key, "quota_gb": quota}))
                    try:
                        ensure_server_hello(config=config)
                        logger.info(
                            json.dumps(
                                {
                                    "event": "schedule_scan_start",
                                    "key": key,
                                    "quota_gb": quota,
                                    "ts": iso_now(),
                                }
                            )
                        )
                        records, scanned_bytes = scan_files(
                            config=config, state=state, quota_gb=quota
                        )
                        if records:
                            mac = get_mac_address()
                            host = get_host_name()
                            batch_size = min(30, max(1, int(config.max_batch_records)))
                            logger.info(
                                json.dumps(
                                    {
                                        "event": "schedule_upload_request",
                                        "key": key,
                                        "record_count": len(records),
                                        "batch_size": batch_size,
                                        "scanned_bytes": scanned_bytes,
                                        "scanned_size": format_bytes(scanned_bytes),
                                        "ts": iso_now(),
                                    }
                                )
                            )
                            for i in range(0, len(records), batch_size):
                                batch = records[i : i + batch_size]
                                resp = upload_records(
                                    config=config,
                                    mac=mac,
                                    host_name=host,
                                    records=batch,
                                )
                                _print_ingest_summary(resp)
                                for r in batch:
                                    state.files[r.file_path] = today
                                save_state(state_path, state)
                            state.schedule_last_run[key] = today
                            save_state(state_path, state)
                            print(
                                json.dumps(
                                    {
                                        "status": "schedule_done",
                                        "key": key,
                                        "scanned_files": len(records),
                                        "scanned_bytes": scanned_bytes,
                                        **(
                                            {"scanned_size": format_bytes(scanned_bytes)}
                                            if args.human
                                            else {}
                                        ),
                                    }
                                )
                            )
                        else:
                            state.schedule_last_run[key] = today
                            save_state(state_path, state)
                            print(json.dumps({"status": "schedule_done", "key": key, "scanned_files": 0}))
                    except Exception as e:
                        print(json.dumps({"status": "schedule_error", "key": key, "error": str(e)}))
            time.sleep(float(args.poll_sec))


def _cmd_validate_config(args: argparse.Namespace) -> int:
    config = load_config(args.config)
    print(json.dumps(config.model_dump(), indent=2))
    return 0


def _cmd_debug_buckets(args: argparse.Namespace) -> int:
    """Show bucket distribution for debugging priority issues."""
    config = load_config(args.config)
    state_path = Path(args.state_path)
    state = load_state(state_path)

    # Get bucket statistics
    bucket_stats = get_bucket_stats(config, state)

    # Get first N files from each bucket
    files = select_files_for_run(config, state)

    # Group files by bucket for display
    bucket_files: dict[int, list[str]] = {}
    for entry in files:
        last_scan = state.files.get(entry.path, "")
        from .scanner import _bucket_index
        bucket = _bucket_index(last_scan)
        bucket_files.setdefault(bucket, []).append(entry.path)

    # Calculate total
    total_files = sum(bucket_stats.values())

    # Print summary
    print(json.dumps({
        "total_files_in_scan_paths": total_files,
        "total_files_in_state": len(state.files),
        "bucket_distribution": {
            str(k): v for k, v in sorted(bucket_stats.items())
        },
    }, indent=2))

    # Print first few files from each bucket
    if args.verbose:
        print("\n--- First 5 files from each bucket (in priority order) ---")
        for bucket in sorted(bucket_files.keys()):
            bucket_name = "unscanned (bucket -1)" if bucket == -1 else f"bucket {bucket}"
            print(f"\n{bucket_name}: {bucket_stats.get(bucket, 0)} files")
            for path in bucket_files[bucket][:5]:
                last_scan = state.files.get(path, "(never)")
                print(f"  {path}")
                print(f"    last_scan: {last_scan}")

    # Show state file entries that don't match any file in scan paths
    if args.verbose:
        scanned_paths = {e.path for e in files}
        orphaned = [p for p in state.files if p not in scanned_paths]
        if orphaned:
            print(f"\n--- Orphaned state entries (in state but not in scan paths): {len(orphaned)} ---")
            for p in orphaned[:10]:
                print(f"  {p}: {state.files[p]}")
            if len(orphaned) > 10:
                print(f"  ... and {len(orphaned) - 10} more")

    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="fimclient")
    p.add_argument("--config", default="client/config.json", help="Path to client config JSON")
    p.add_argument(
        "-H",
        "--human",
        action="store_true",
        help="Display file sizes in human-readable form",
    )
    sub = p.add_subparsers(dest="cmd", required=True)

    dry = sub.add_parser("dry-run", help="List eligible files and totals (no hashing)")
    dry.add_argument("--list", action="store_true", help="Print each eligible file as JSONL")
    dry.set_defaults(func=_cmd_dry_run)

    run = sub.add_parser("run", help="Scan files and upload to server once")
    run.add_argument("--state-path", required=True, help="Path to state JSON file")
    run.add_argument("--log-path", required=True, help="Path to log file")
    run.add_argument("--quota-gb", type=int, default=None, help="Max GB per run (can exceed by 1 file)")
    run.set_defaults(func=_cmd_run)

    daemon = sub.add_parser("daemon", help="Run scheduler loop from config.schedule_quota_gb")
    daemon.add_argument("--state-path", required=True, help="Path to state JSON file")
    daemon.add_argument("--log-path", required=True, help="Path to log file")
    daemon.add_argument("--poll-sec", type=float, default=20.0, help="Polling interval seconds")
    daemon.set_defaults(func=_cmd_daemon)

    val = sub.add_parser("validate-config", help="Validate config and print normalized JSON")
    val.set_defaults(func=_cmd_validate_config)

    dbg = sub.add_parser("debug-buckets", help="Show bucket distribution for debugging")
    dbg.add_argument("--state-path", required=True, help="Path to state JSON file")
    dbg.add_argument("-v", "--verbose", action="store_true", help="Show sample files from each bucket")
    dbg.set_defaults(func=_cmd_debug_buckets)

    return p


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
