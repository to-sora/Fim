from __future__ import annotations

import math
import os
import platform
import uuid
from pathlib import Path


def normalize_path(path: str | Path) -> str:
    return str(Path(path).expanduser().resolve())


def get_host_name() -> str:
    return platform.node() or "unknown"


def get_mac_address() -> str:
    node = uuid.getnode()
    mac_hex = f"{node:012x}"
    return ":".join(mac_hex[i : i + 2] for i in range(0, 12, 2))


def ceil_gb(size_bytes: int) -> int:
    if size_bytes <= 0:
        return 0
    return int(math.ceil(size_bytes / (1024**3)))


def is_subpath(path: str, maybe_parent: str) -> bool:
    try:
        return os.path.commonpath([path, maybe_parent]) == os.path.normpath(maybe_parent)
    except ValueError:
        return False

