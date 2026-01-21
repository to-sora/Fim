from __future__ import annotations

from pydantic import BaseModel, Field, field_validator


class RecordIn(BaseModel):
    file_path: str
    file_name: str
    extension: str = ""
    size_bytes: int
    sha256: str = Field(min_length=64, max_length=64)
    scan_ts: int
    urn: str

    @field_validator("size_bytes", "scan_ts")
    @classmethod
    def _non_negative_int(cls, value: int) -> int:
        if value < 0:
            raise ValueError("must be >= 0")
        return value


class IngestRequest(BaseModel):
    machine_id: str = ""
    mac: str = ""
    host_name: str = ""
    tag: str = ""
    records: list[RecordIn]

