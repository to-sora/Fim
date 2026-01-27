from __future__ import annotations

import atexit
import json
import os
import signal
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .utils import iso_now

@dataclass
class ClientState:
    machine_id: str
    files: dict[str, str]  # abs_path -> ISO 8601 timestamp (or date for backward compat)
    schedule_last_run: dict[str, str]  # Mon0910 -> YYYY-MM-DD


def _atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    os.replace(tmp, path)


def load_state(path: Path) -> ClientState:
    if not path.exists():
        return ClientState(machine_id=str(uuid.uuid4()), files={}, schedule_last_run={})
    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise TypeError("state file must be a JSON object")
    machine_id = raw.get("machine_id") or str(uuid.uuid4())
    files = raw.get("files") or {}
    schedule_last_run = raw.get("schedule_last_run") or {}
    if not isinstance(files, dict) or not isinstance(schedule_last_run, dict):
        raise TypeError("invalid state file format")
    return ClientState(
        machine_id=str(machine_id),
        files={str(k): str(v) for k, v in files.items()},
        schedule_last_run={str(k): str(v) for k, v in schedule_last_run.items()},
    )


def save_state(path: Path, state: ClientState) -> None:
    payload: dict[str, Any] = {
        "version": 1,
        "machine_id": state.machine_id,
        "files": state.files,
        "schedule_last_run": state.schedule_last_run,
        "saved_at": iso_now(),
    }
    _atomic_write_json(path, payload)


class SingleInstance:
    def __init__(self, lock_path: Path) -> None:
        self._lock_path = lock_path
        self._fd: int | None = None
        self._prev_sigterm: signal.Handlers | None = None
        self._prev_sigint: signal.Handlers | None = None

    def _signal_handler(self, signum: int, frame: Any) -> None:
        self.release()
        # Re-raise the signal with the original handler
        if signum == signal.SIGTERM and self._prev_sigterm not in (signal.SIG_IGN, signal.SIG_DFL, None):
            self._prev_sigterm(signum, frame)  # type: ignore[misc]
        elif signum == signal.SIGINT and self._prev_sigint not in (signal.SIG_IGN, signal.SIG_DFL, None):
            self._prev_sigint(signum, frame)  # type: ignore[misc]
        else:
            raise SystemExit(128 + signum)

    def acquire(self) -> bool:
        flags = os.O_CREAT | os.O_EXCL | os.O_WRONLY
        try:
            self._fd = os.open(str(self._lock_path), flags, 0o644)
        except FileExistsError:
            return False
        os.write(self._fd, f"{os.getpid()}\n".encode("utf-8"))
        # Register cleanup handlers
        atexit.register(self.release)
        self._prev_sigterm = signal.signal(signal.SIGTERM, self._signal_handler)
        self._prev_sigint = signal.signal(signal.SIGINT, self._signal_handler)
        return True

    def release(self) -> None:
        if self._fd is None:
            return
        # Unregister cleanup handlers
        try:
            atexit.unregister(self.release)
        except Exception:
            pass
        if self._prev_sigterm is not None:
            try:
                signal.signal(signal.SIGTERM, self._prev_sigterm)
            except Exception:
                pass
            self._prev_sigterm = None
        if self._prev_sigint is not None:
            try:
                signal.signal(signal.SIGINT, self._prev_sigint)
            except Exception:
                pass
            self._prev_sigint = None
        try:
            os.close(self._fd)
        finally:
            self._fd = None
            try:
                os.unlink(self._lock_path)
            except FileNotFoundError:
                pass

    def __enter__(self) -> "SingleInstance":
        if not self.acquire():
            raise RuntimeError(f"lock exists: {self._lock_path}")
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.release()
