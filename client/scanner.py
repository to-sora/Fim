from __future__ import annotations

import hashlib
import os
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path

from .config import ClientConfig
from .enumerator import FileEntry, iter_files
from .state import ClientState
from .utils import ceil_gb, iso_now


@dataclass(frozen=True)
class ScanRecord:
    file_path: str
    file_name: str
    extension: str
    size_bytes: int
    sha256: str
    scan_ts: str
    urn: str


def sha256_file(path: str, chunk_size: int = 4 * 1024 * 1024) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            b = f.read(chunk_size)
            if not b:
                break
            h.update(b)
    return h.hexdigest()


def _is_sha256_hex(value: str) -> bool:
    if len(value) != 64:
        return False
    for ch in value:
        if ch not in "0123456789abcdef":
            return False
    return True


def make_urn(machine_name: str, file_name: str, extension: str, size_bytes: int) -> str:
    scan_date = datetime.now(timezone.utc).date().isoformat()
    size_gb = ceil_gb(size_bytes)
    return f"{machine_name}:{file_name}:{extension}:{size_gb}:{scan_date}"


def _bucket_index(last_scan: str) -> int:
    if not last_scan:
        return -1
    try:
        return date.fromisoformat(last_scan).toordinal() // 15
    except ValueError:
        return -1


def select_files_for_run(config: ClientConfig, state: ClientState) -> list[FileEntry]:
    buckets: dict[int, list[FileEntry]] = {}
    for entry in iter_files(config):
        last_scan = state.files.get(entry.path, "")
        bucket = _bucket_index(last_scan)
        buckets.setdefault(bucket, []).append(entry)
    ordered: list[FileEntry] = []
    for bucket in sorted(buckets.keys()):
        ordered.extend(buckets[bucket])
    return ordered


def scan_files(
    *,
    config: ClientConfig,
    state: ClientState,
    quota_gb: int | None,
    skip_paths: set[str] | None = None,
) -> tuple[list[ScanRecord], int]:
    now_ts = iso_now()
    files = select_files_for_run(config, state)
    quota_bytes = None if quota_gb is None else int(quota_gb) * (1024**3)
    skip_paths = skip_paths or set()

    out: list[ScanRecord] = []
    scanned_bytes = 0
    scanned_count = 0

    for entry in files:
        if quota_bytes is not None and scanned_count > 0 and scanned_bytes >= quota_bytes:
            break

        file_path = entry.path
        if file_path in skip_paths:
            continue
        file_name = Path(file_path).name
        ext = Path(file_name).suffix.lower().lstrip(".")
        try:
            if not os.path.isfile(file_path):
                continue
        except OSError:
            continue

        try:
            digest = sha256_file(file_path)
        except OSError:
            # File was deleted/edited mid-scan (or unreadable). Skip and try next run.
            continue
        if not _is_sha256_hex(digest):
            continue
        urn = make_urn(config.machine_name, file_name, ext, entry.size_bytes)
        out.append(
            ScanRecord(
                file_path=file_path,
                file_name=file_name,
                extension=ext,
                size_bytes=entry.size_bytes,
                sha256=digest,
                scan_ts=now_ts,
                urn=urn,
            )
        )
        scanned_bytes += entry.size_bytes
        scanned_count += 1

    return out, scanned_bytes
