from __future__ import annotations

from dataclasses import asdict

import time
import httpx

from .config import ClientConfig
from .scanner import ScanRecord


def _retryable_status(status_code: int) -> bool:
    return status_code in {408, 425, 429} or status_code >= 500


def ensure_server_hello(*, config: ClientConfig) -> None:
    if not config.server_url:
        raise RuntimeError("server_url is empty in config")
    url = config.server_url.rstrip("/") + "/hello"
    timeout = httpx.Timeout(config.http_timeout_sec)
    retries = max(1, int(config.http_retries))
    backoff_sec = 0.5

    with httpx.Client(timeout=timeout) as client:
        last_exc: Exception | None = None
        for attempt in range(1, retries + 1):
            try:
                resp = client.get(url, headers={"accept": "text/plain"})
                if resp.status_code == 200 and resp.text.strip() == "Hello":
                    return
                if _retryable_status(resp.status_code) and attempt < retries:
                    time.sleep(backoff_sec)
                    backoff_sec = min(backoff_sec * 2, 8.0)
                    continue
                raise RuntimeError(
                    f"server hello check failed: status={resp.status_code} body={resp.text[:200]!r}"
                )
            except (httpx.TimeoutException, httpx.TransportError) as e:
                last_exc = e
                if attempt >= retries:
                    raise RuntimeError("server hello check failed") from e
                time.sleep(backoff_sec)
                backoff_sec = min(backoff_sec * 2, 8.0)

        raise RuntimeError("server hello check failed") from last_exc


def _validate_records(records: list[ScanRecord]) -> None:
    if not records:
        raise ValueError("records is empty")
    if len(records) > 30:
        raise ValueError("records exceeds max batch size (30)")
    for r in records:
        if not r.file_path:
            raise ValueError("record.file_path is empty")
        if not r.file_name:
            raise ValueError("record.file_name is empty")
        if r.size_bytes < 0:
            raise ValueError("record.size_bytes must be >= 0")
        if r.scan_ts < 0:
            raise ValueError("record.scan_ts must be >= 0")
        if len(r.sha256) != 64:
            raise ValueError("record.sha256 must be 64 hex chars")
        sha = r.sha256.lower()
        if any(ch not in "0123456789abcdef" for ch in sha):
            raise ValueError("record.sha256 must be hex")
        if not r.urn:
            raise ValueError("record.urn is empty")


def upload_records(
    *,
    config: ClientConfig,
    machine_id: str,
    mac: str,
    host_name: str,
    records: list[ScanRecord],
) -> dict:
    _validate_records(records)
    if not config.server_url:
        raise RuntimeError("server_url is empty in config")
    if not config.auth_token:
        raise RuntimeError("auth_token is empty in config")
    url = config.server_url.rstrip("/") + "/ingest"
    payload = {
        "machine_id": machine_id,
        "mac": mac,
        "host_name": host_name,
        "tag": config.tag,
        "records": [asdict(r) for r in records],
    }
    headers = {"Authorization": f"Bearer {config.auth_token}"}
    timeout = httpx.Timeout(config.http_timeout_sec)
    retries = max(1, int(config.http_retries))
    backoff_sec = 0.5
    with httpx.Client(timeout=timeout) as client:
        last_exc: Exception | None = None
        for attempt in range(1, retries + 1):
            try:
                resp = client.post(url, json=payload, headers=headers)
                if _retryable_status(resp.status_code) and attempt < retries:
                    time.sleep(backoff_sec)
                    backoff_sec = min(backoff_sec * 2, 8.0)
                    continue
                resp.raise_for_status()
                body = resp.json()
                if not isinstance(body, dict):
                    raise RuntimeError("invalid server response")
                return body
            except (httpx.TimeoutException, httpx.TransportError) as e:
                last_exc = e
                if attempt >= retries:
                    raise
                time.sleep(backoff_sec)
                backoff_sec = min(backoff_sec * 2, 8.0)

        raise RuntimeError("upload failed") from last_exc
