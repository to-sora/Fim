from __future__ import annotations

import argparse
import hashlib
import json
import math
import platform
import time
from datetime import date
from pathlib import Path

import httpx


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        while True:
            b = f.read(4 * 1024 * 1024)
            if not b:
                break
            h.update(b)
    return h.hexdigest()


def _ceil_gb(size_bytes: int) -> int:
    if size_bytes <= 0:
        return 0
    return int(math.ceil(size_bytes / (1024**3)))


def main() -> int:
    p = argparse.ArgumentParser(description="Simple /ingest test sender")
    p.add_argument("--server-url", required=True)
    p.add_argument("--token", required=True)
    p.add_argument("--machine-name", required=True)
    p.add_argument("--machine-id", default="test-machine-id")
    p.add_argument("--mac", default="")
    p.add_argument("--host-name", default="")
    p.add_argument("--tag", default="test")
    p.add_argument("file", nargs="+")
    args = p.parse_args()

    records = []
    for f in args.file:
        path = Path(f).expanduser().resolve()
        st = path.stat()
        digest = _sha256_file(path)
        file_name = path.name
        ext = path.suffix.lower().lstrip(".")
        urn = f"{args.machine_name}:{file_name}:{ext}:{_ceil_gb(st.st_size)}:{date.today().isoformat()}"
        records.append(
            {
                "file_path": str(path),
                "file_name": file_name,
                "extension": ext,
                "size_bytes": int(st.st_size),
                "sha256": digest,
                "scan_ts": int(time.time()),
                "urn": urn,
            }
        )

    payload = {
        "machine_id": args.machine_id,
        "mac": args.mac,
        "host_name": args.host_name or platform.node() or "unknown",
        "tag": args.tag,
        "records": records,
    }

    url = args.server_url.rstrip("/") + "/ingest"
    headers = {"Authorization": f"Bearer {args.token}"}
    resp = httpx.post(url, json=payload, headers=headers, timeout=30.0)
    resp.raise_for_status()
    print(json.dumps(resp.json(), indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
