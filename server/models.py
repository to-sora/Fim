from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, Field, field_validator


class RecordIn(BaseModel):
    file_path: str
    file_name: str
    extension: str = ""
    size_bytes: int
    sha256: str = Field(min_length=64, max_length=64)
    scan_ts: str
    urn: str

    @field_validator("size_bytes")
    @classmethod
    def _non_negative_int(cls, value: int) -> int:
        if value < 0:
            raise ValueError("must be >= 0")
        return value

    @field_validator("scan_ts")
    @classmethod
    def _valid_scan_ts(cls, value: str) -> str:
        if not isinstance(value, str) or not value.strip():
            raise ValueError("scan_ts must be a non-empty ISO 8601 timestamp")
        try:
            parsed = datetime.fromisoformat(value)
        except ValueError as exc:
            raise ValueError("scan_ts must be ISO 8601 format") from exc
        if parsed.tzinfo is None:
            raise ValueError("scan_ts must include a timezone offset")
        return value


class IngestRequest(BaseModel):
    mac: str = ""
    host_name: str = ""
    tag: str = ""
    records: list[RecordIn]
