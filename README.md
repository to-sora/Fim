## FimSystem (SHA256 inventory / integrity)

Monorepo:

- `client/` Python client agent (scan + SHA256 + upload)
- `server/` FastAPI server (append-only SQLite + admin CLI queries)

### Setup

```bash
python3 -m venv venv
pip install -r server/requirements.txt
pip install -r client/requirements.txt
```

### Server (FastAPI on port `19991`)

Create/rotate a token for a `MachineName` (group):

```bash
python -m server.admin_cli token create MachineNameA
```

List or delete tokens:

```bash
python -m server.admin_cli token list
python -m server.admin_cli token delete MachineNameA
```

Start server:

```bash
./start_server.sh
```

Environment:

- `FIM_DB_PATH` (optional): SQLite path (default `data/fim.sqlite3`)
- `FIM_DB_BUSY_TIMEOUT_MS` (optional): SQLite busy timeout in ms (default `5000`)
- `scan_ts` is client-supplied ISO 8601; `ingested_at` is server time in UTC (minute precision).

Endpoints:

- `GET /healthz` (JSON `{"status":"ok"}`)
- `GET /hello` (plain text `Hello`)
- `POST /ingest` (Bearer token; JSON body with `mac`, `host_name`, optional `tag`, and `records`)

Ingest behavior:

- Records are queued in an in-memory buffer and flushed to SQLite in the background.
- If the buffer is full, the server returns `503` with `server ingest buffer is full; try again`.

### Client

Edit `client/.config.json.env` and paste your token. Save the file in path client/config.json

Note: scanning + SHA256 hashing can be CPU/disk intensive. For best results, schedule daemon runs during periods when the machine is relatively idle.

If you're connecting through Tailscale, a reverse proxy, or a LAN interface with a mismatched CA/domain, set `allow_insecure_ssl` to `true` in `client/.config.json.env` to skip TLS verification (use only for trusted networks).

Config highlights:

- `server_url` (required)
- `auth_token` (required; bearer token from `server.admin_cli`)
- `scan_paths` list of roots to walk (overlapping subpaths are de-duplicated)
- `exclude_subdirs` supports directory names or relative/absolute paths
- `exclude_extensions` uses lowercase extensions (add a leading `.` if omitted)
- `size_threshold_kb_by_ext` applies per-extension size limits (KB) with optional `lowtherehold`/`uppertherehold`
- `schedule_quota_gb` uses keys like `Mon0910` (weekday + 24h time)
- `state_path` stores scan history and scheduler state (provided via `--state-path`)

Scanner behavior:

- Skips symlinks, hardlinks, and non-regular files.
- Applies per-extension size thresholds before hashing.

Example threshold config (lower-only / upper-only):

```json
{
  "size_threshold_kb_by_ext": {
    ".log": {"uppertherehold": 1024},
    ".bin": {"lowtherehold": 512}
  }
}
```

Config highlights:

- `server_url` (required)
- `auth_token` (required; bearer token from `server.admin_cli`)
- `scan_paths` list of roots to walk
- `exclude_subdirs` supports directory names or relative/absolute paths
- `exclude_extensions` uses lowercase extensions (add a leading `.` if omitted)
- `size_threshold_kb_by_ext` applies per-extension size limits (KB) with `lowtherehold`/`uppertherehold`
- `schedule_quota_gb` uses keys like `Mon0910` (weekday + 24h time)
- `state_path` stores scan history and scheduler state (provided via `--state-path`)

Scanner behavior:

- Skips symlinks, hardlinks, and non-regular files.
- Applies per-extension size thresholds before hashing.

URN structure (computed on the server for each record):

```
<machine_name>:<file_name>:<extension>:<size_gb>:<scan_date>
```

`scan_date` is an ISO 8601 date (`YYYY-MM-DD`), `extension` is the file suffix without a dot, and `size_gb` is rounded up.

Dry-run (list eligible files and totals):

```bash
python -m client.cli dry-run --list
```

Run once (quota in GB; may exceed by 1 file):

```bash
python -m client.cli run --state-path .fim_state.json --log-path .fim.log --quota-gb 10
```

First-time/full run (no quota limit):

```bash
python -m client.cli run --state-path .fim_state.json --log-path .fim.log
```

Daemon scheduler (uses `schedule_quota_gb` in config):

```bash
python -m client.cli daemon --state-path .fim_state.json --log-path .fim.log
```

Systemd examples:

- Server unit: `scripts/fimserver.service`
- Client unit: `scripts/fimclient.service`
- Web UI unit: `scripts/fimwebui.service`

Copy the unit files to `/etc/systemd/system/`, adjust paths/users, then run:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now fimserver.service
sudo systemctl enable --now fimclient.service
sudo systemctl enable --now fimwebui.service
```

Validate config (prints normalized JSON):

```bash
python -m client.cli validate-config
```

Shortcut script (`daemon` is default when no args are provided):

```bash
./start_client.sh
```

### Web UI (no auth)

The web UI is a separate service and runs on its own port. It reads directly from the same
SQLite DB and does not require auth.

Defaults:

- `FIM_WEB_HOST` = `0.0.0.0`
- `FIM_WEB_PORT` = `19992`
- API server defaults to `19991` (set `FIM_HOST` / `FIM_PORT` if needed).

Run it locally:

```bash
./start_webui.sh
```

### Graph (CLI)

ASCII chain:

```bash
python -m server.admin_cli graph sha256 <SHA256> --format ascii
```

Mermaid flowchart:

```bash
python -m server.admin_cli graph sha256 <SHA256> --format mermaid
```

Graphviz DOT:

```bash
python -m server.admin_cli graph sha256 <SHA256> --format dot
```

Raw JSON:

```bash
python -m server.admin_cli graph sha256 <SHA256> --format json
```

### Query (CLI)

Lookup records by SHA256:

```bash
python -m server.admin_cli query file <SHA256>
```

Query records for a machine (optional SHA256 filter):

```bash
python -m server.admin_cli query machine MachineNameA
python -m server.admin_cli query machine MachineNameA --sha256 <SHA256>
```

Query records by filename substring (returns file name, scan time, server ingest time, sha256):

```bash
python -m server.admin_cli query name "substring"
python -m server.admin_cli query name "substring" --machine-name MachineNameA
```

### Testing

```bash
python -m unittest discover -s tests -v
```
