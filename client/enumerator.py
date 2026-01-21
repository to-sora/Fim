from __future__ import annotations

import os
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
    exclude_dir_paths: list[str] = []
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
            exclude_dir_paths.append(normalize_path(raw))
        else:
            exclude_dir_names.add(raw)
    return exclude_dir_names, exclude_dir_paths


def iter_files(config: ClientConfig) -> Iterable[FileEntry]:
    exclude_dir_names, exclude_dir_paths = _prepare_excludes(config)
    exclude_exts = set(config.exclude_extensions)
    thresholds = config.size_threshold_kb_by_ext

    for scan_root in config.scan_paths:
        root = normalize_path(scan_root)
        if not os.path.exists(root):
            continue
        for dirpath, dirnames, filenames in os.walk(
            root, topdown=True, followlinks=config.follow_symlinks
        ):
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
                kept_dirs.append(d)
            dirnames[:] = kept_dirs

            for name in filenames:
                full_path = normalize_path(os.path.join(dirpath, name))
                ext = Path(name).suffix.lower()
                if ext in exclude_exts:
                    continue
                try:
                    stat = os.stat(full_path, follow_symlinks=config.follow_symlinks)
                except FileNotFoundError:
                    continue
                size_bytes = int(stat.st_size)
                if ext in thresholds:
                    rule = thresholds[ext]
                    size_kb = size_bytes / 1024
                    if size_kb < rule.lowtherehold or size_kb > rule.uppertherehold:
                        continue
                yield FileEntry(path=full_path, size_bytes=size_bytes)

