from __future__ import annotations

import logging
import math
import os
import platform
import uuid
from datetime import datetime, timezone
from pathlib import Path


def normalize_path(path: str | Path) -> str:
    return str(Path(path).expanduser().resolve())


def get_host_name() -> str:
    return platform.node() or "unknown"


def get_mac_address() -> str:
    node = uuid.getnode()
    if (node >> 40) & 0x01:
        return "ff:ff:ff:ff:ff:ff"
    mac_hex = f"{node:012x}"
    return ":".join(mac_hex[i : i + 2] for i in range(0, 12, 2))


def ceil_gb(size_bytes: int) -> int:
    if size_bytes <= 0:
        return 0
    return int(math.ceil(size_bytes / (1024**3)))


def format_bytes(size_bytes: int) -> str:
    if size_bytes <= 0:
        return "0 B"
    units = ["B", "KB", "MB", "GB", "TB", "PB"]
    size = float(size_bytes)
    unit_index = 0
    while size >= 1024 and unit_index < len(units) - 1:
        size /= 1024
        unit_index += 1
    if size >= 10 or unit_index == 0:
        return f"{size:.0f} {units[unit_index]}"
    return f"{size:.1f} {units[unit_index]}"


def setup_logger(path: str | Path) -> logging.Logger:
    log_path = Path(path)
    logger = logging.getLogger("fimclient")
    logger.setLevel(logging.INFO)
    for handler in list(logger.handlers):
        if isinstance(handler, logging.FileHandler):
            handler_path = Path(handler.baseFilename)
            if handler_path == log_path:
                return logger
        logger.removeHandler(handler)
        handler.close()
    handler = logging.FileHandler(log_path)
    handler.setFormatter(logging.Formatter("%(message)s"))
    logger.addHandler(handler)
    return logger


def is_subpath(path: str, maybe_parent: str) -> bool:
    try:
        return os.path.commonpath([path, maybe_parent]) == os.path.normpath(maybe_parent)
    except ValueError:
        return False


def iso_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds")
