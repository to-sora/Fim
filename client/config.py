from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field, field_validator


class SizeThresholdKB(BaseModel):
    lowtherehold: int = Field(alias="lowtherehold")
    uppertherehold: int = Field(alias="uppertherehold")

    @field_validator("lowtherehold", "uppertherehold")
    @classmethod
    def _non_negative(cls, value: int) -> int:
        if value < 0:
            raise ValueError("threshold must be >= 0")
        return value


class ClientConfig(BaseModel):
    machine_name: str
    server_url: str = ""
    auth_token: str = ""

    scan_paths: list[str] = Field(default_factory=lambda: ["."])
    exclude_subdirs: list[str] = Field(default_factory=list)
    exclude_extensions: list[str] = Field(default_factory=list)
    size_threshold_kb_by_ext: dict[str, SizeThresholdKB] = Field(default_factory=dict)

    schedule_quota_gb: dict[str, int] = Field(default_factory=dict)

    state_path: str = ".fim_state.json"
    tag: str = ""
    follow_symlinks: bool = False
    max_batch_records: int = 500
    http_timeout_sec: float = 30.0

    @field_validator("exclude_extensions", mode="before")
    @classmethod
    def _normalize_extensions(cls, value: Any) -> list[str]:
        if value is None:
            return []
        if not isinstance(value, list):
            raise TypeError("exclude_extensions must be a list")
        normalized: list[str] = []
        for ext in value:
            if not isinstance(ext, str):
                raise TypeError("exclude_extensions entries must be strings")
            ext = ext.strip().lower()
            if not ext:
                continue
            if not ext.startswith("."):
                ext = f".{ext}"
            normalized.append(ext)
        return normalized

    @field_validator("size_threshold_kb_by_ext", mode="before")
    @classmethod
    def _normalize_threshold_keys(cls, value: Any) -> Any:
        if value is None:
            return {}
        if not isinstance(value, dict):
            raise TypeError("size_threshold_kb_by_ext must be an object/dict")
        normalized: dict[str, Any] = {}
        for raw_ext, thresholds in value.items():
            if not isinstance(raw_ext, str):
                raise TypeError("extension keys must be strings")
            ext = raw_ext.strip().lower()
            if not ext:
                continue
            if not ext.startswith("."):
                ext = f".{ext}"
            # Support common misspellings from the prompt.
            if isinstance(thresholds, dict):
                thresholds = dict(thresholds)
                if "lowthreshold" in thresholds and "lowtherehold" not in thresholds:
                    thresholds["lowtherehold"] = thresholds["lowthreshold"]
                if "upperthreshold" in thresholds and "uppertherehold" not in thresholds:
                    thresholds["uppertherehold"] = thresholds["upperthreshold"]
            normalized[ext] = thresholds
        return normalized

    @field_validator("schedule_quota_gb", mode="before")
    @classmethod
    def _normalize_schedule_quota(cls, value: Any) -> dict[str, int]:
        if value is None:
            return {}
        if not isinstance(value, dict):
            raise TypeError("schedule_quota_gb must be an object/dict")
        out: dict[str, int] = {}
        for k, v in value.items():
            if not isinstance(k, str):
                raise TypeError("schedule keys must be strings like Mon0910")
            if isinstance(v, str):
                v = v.strip()
                if not v:
                    continue
                v = int(v)
            if not isinstance(v, int):
                raise TypeError("schedule values must be integers or numeric strings (GB)")
            if v < 0:
                raise ValueError("schedule quota must be >= 0")
            out[k] = v
        return out

    def state_file(self) -> Path:
        return Path(self.state_path)


def load_config(path: str | Path) -> ClientConfig:
    config_path = Path(path)
    raw = json.loads(config_path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise TypeError("config JSON must be an object")
    return ClientConfig.model_validate(raw)

