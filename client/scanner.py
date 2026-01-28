from __future__ import annotations

import hashlib
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from .enumerator import FileEntry, iter_files
from .state import ClientState
from .utils import iso_now
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .config import ClientConfig


@dataclass(frozen=True)
class ScanRecord:
    file_path: str
    file_name: str
    extension: str
    size_bytes: int
    sha256: str
    scan_ts: str


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


_MIN_SCAN_TS = datetime.min.replace(tzinfo=timezone.utc)


def _parse_last_scan(value: str) -> datetime | None:
    if not value:
        return None
    raw = value.strip()
    if not raw:
        return None
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def select_files_for_run(config: ClientConfig, state: ClientState) -> list[FileEntry]:
    entries = list(iter_files(config))
    unscanned: list[FileEntry] = []
    scanned: list[tuple[datetime, str, FileEntry]] = []
    for entry in entries:
        last_scan = state.files.get(entry.path, "")
        parsed = _parse_last_scan(last_scan)
        if parsed is None:
            unscanned.append(entry)
        else:
            scanned.append((parsed, entry.path, entry))
    if unscanned:
        return unscanned
    scanned.sort(key=lambda item: (item[0], item[1]))
    return [entry for _, _, entry in scanned]


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
        out.append(
            ScanRecord(
                file_path=file_path,
                file_name=file_name,
                extension=ext,
                size_bytes=entry.size_bytes,
                sha256=digest,
                scan_ts=now_ts,
            )
        )
        scanned_bytes += entry.size_bytes
        scanned_count += 1

    return out, scanned_bytes
