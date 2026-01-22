from __future__ import annotations

import os
import stat
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from .config import ClientConfig
from .utils import is_subpath, normalize_path


@dataclass(frozen=True)
class FileEntry:
    path: str
    size_bytes: int


def _prepare_excludes(config: ClientConfig) -> tuple[set[str], list[str]]:
    exclude_dir_names: set[str] = set()
    exclude_dir_path_entries: list[str] = []
    for item in config.exclude_subdirs:
        if not isinstance(item, str):
            continue
        raw = item.strip()
        if not raw:
            continue
        if raw.startswith("~"):
            raw = str(Path(raw).expanduser())
        # Heuristic: treat entries with a path separator as paths; otherwise as names.
        if os.sep in raw or "/" in raw or "\\" in raw:
            exclude_dir_path_entries.append(raw)
        else:
            exclude_dir_names.add(raw)
    return exclude_dir_names, exclude_dir_path_entries


def iter_files(config: ClientConfig) -> Iterable[FileEntry]:
    exclude_dir_names, exclude_dir_path_entries = _prepare_excludes(config)
    exclude_exts = set(config.exclude_extensions)
    thresholds = config.size_threshold_kb_by_ext

    for scan_root in config.scan_paths:
        root = normalize_path(scan_root)
        if not os.path.exists(root):
            continue
        exclude_dir_paths: list[str] = []
        for raw in exclude_dir_path_entries:
            candidate = Path(raw).expanduser()
            if candidate.is_absolute():
                exclude_dir_paths.append(normalize_path(candidate))
            else:
                exclude_dir_paths.append(normalize_path(os.path.join(root, raw)))
        for dirpath, dirnames, filenames in os.walk(root, topdown=True, followlinks=False):
            dirpath_norm = normalize_path(dirpath)
            if any(is_subpath(dirpath_norm, ex) for ex in exclude_dir_paths):
                dirnames[:] = []
                continue
            kept_dirs: list[str] = []
            for d in dirnames:
                if d in exclude_dir_names:
                    continue
                full = normalize_path(os.path.join(dirpath, d))
                if any(is_subpath(full, ex) for ex in exclude_dir_paths):
                    continue
                try:
                    dir_stat = os.lstat(full)
                except FileNotFoundError:
                    continue
                if stat.S_ISLNK(dir_stat.st_mode):
                    continue
                kept_dirs.append(d)
            dirnames[:] = kept_dirs

            for name in filenames:
                full_path = normalize_path(os.path.join(dirpath, name))
                ext = Path(name).suffix.lower()
                if ext in exclude_exts:
                    continue
                try:
                    file_stat = os.lstat(full_path)
                except FileNotFoundError:
                    continue
                if stat.S_ISLNK(file_stat.st_mode):
                    continue
                if not stat.S_ISREG(file_stat.st_mode):
                    continue
                if file_stat.st_nlink > 1:
                    continue
                size_bytes = int(file_stat.st_size)
                if ext in thresholds:
                    rule = thresholds[ext]
                    size_kb = size_bytes / 1024
                    if size_kb < rule.lowtherehold or size_kb > rule.uppertherehold:
                        continue
                yield FileEntry(path=full_path, size_bytes=size_bytes)
