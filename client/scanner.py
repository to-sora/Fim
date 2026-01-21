from __future__ import annotations

import hashlib
import os
import time
from dataclasses import dataclass
from datetime import date
from pathlib import Path

from .config import ClientConfig
from .enumerator import FileEntry, iter_files
from .state import ClientState
from .utils import ceil_gb, normalize_path


@dataclass(frozen=True)
class ScanRecord:
    file_path: str
    file_name: str
    extension: str
    size_bytes: int
    sha256: str
    scan_ts: int
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


def make_urn(machine_name: str, file_name: str, extension: str, size_bytes: int) -> str:
    scan_date = date.today().isoformat()
    size_gb = ceil_gb(size_bytes)
    return f"{machine_name}:{file_name}:{extension}:{size_gb}:{scan_date}"


def _last_scan_key(state: ClientState, path: str) -> tuple[str, str]:
    last = state.files.get(path, "")
    # Empty string sorts first.
    return (last, path)


def select_files_for_run(config: ClientConfig, state: ClientState) -> list[FileEntry]:
    entries = list(iter_files(config))
    entries.sort(key=lambda e: _last_scan_key(state, normalize_path(e.path)))
    return entries


def scan_files(
    *,
    config: ClientConfig,
    state: ClientState,
    quota_gb: int | None,
) -> tuple[list[ScanRecord], int]:
    now_ts = int(time.time())
    files = select_files_for_run(config, state)
    quota_bytes = None if quota_gb is None else int(quota_gb) * (1024**3)

    out: list[ScanRecord] = []
    scanned_bytes = 0
    scanned_count = 0

    for entry in files:
        if quota_bytes is not None and scanned_count > 0 and scanned_bytes >= quota_bytes:
            break

        file_path = normalize_path(entry.path)
        file_name = Path(file_path).name
        ext = Path(file_name).suffix.lower().lstrip(".")
        try:
            if not os.path.isfile(file_path):
                continue
        except OSError:
            continue

        digest = sha256_file(file_path)
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

