from __future__ import annotations

from dataclasses import asdict

import httpx

from .config import ClientConfig
from .scanner import ScanRecord


def upload_records(
    *,
    config: ClientConfig,
    machine_id: str,
    mac: str,
    host_name: str,
    records: list[ScanRecord],
) -> dict:
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
    with httpx.Client(timeout=timeout) as client:
        resp = client.post(url, json=payload, headers=headers)
        resp.raise_for_status()
        body = resp.json()
        if not isinstance(body, dict):
            raise RuntimeError("invalid server response")
        return body

