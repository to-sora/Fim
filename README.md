## FimSystem (SHA256 inventory / integrity)

Monorepo:

- `client/` Python client agent (scan + SHA256 + upload)
- `server/` FastAPI server (append-only SQLite + admin CLI queries)

### Setup

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r server/requirements.txt
pip install -r client/requirements.txt
```

### Server (FastAPI on port `19991`)

Create/rotate a token for a `MachineName` (group):

```bash
source venv/bin/activate
python -m server.admin_cli token create MachineNameA
```

List or delete tokens:

```bash
source venv/bin/activate
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
- Timestamps are stored as ISO 8601 text in UTC with minute precision (format: `YYYY-MM-DDTHH:MM+00:00`).

Endpoints:

- `GET /healthz` (JSON `{"status":"ok"}`)
- `GET /hello` (plain text `Hello`)
- `POST /ingest` (Bearer token; JSON body with `mac`, `host_name`, optional `tag`, and `records`)

Ingest behavior:

- Records are queued in an in-memory buffer and flushed to SQLite in the background.
- If the buffer is full, the server returns `503` with `server ingest buffer is full; try again`.

### Client

Edit `client/.config.json.env` and paste your token.

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
- `state_path` stores scan history and scheduler state

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

URN structure (added to each record):

```
<machine_name>:<file_name>:<extension>:<size_gb>:<scan_date>
```

`scan_date` is an ISO 8601 date (`YYYY-MM-DD`), `extension` is the file suffix without a dot, and `size_gb` is rounded up.

Dry-run (list eligible files and totals):

```bash
source venv/bin/activate
python -m client.cli dry-run --list
```

Run once (quota in GB; may exceed by 1 file):

```bash
source venv/bin/activate
python -m client.cli run --quota-gb 10
```

First-time/full run (no quota limit):

```bash
source venv/bin/activate
python -m client.cli run
```

Daemon scheduler (uses `schedule_quota_gb` in config):

```bash
source venv/bin/activate
python -m client.cli daemon
```

Validate config (prints normalized JSON):

```bash
source venv/bin/activate
python -m client.cli validate-config
```

Shortcut script (`daemon` is default when no args are provided):

```bash
./start_client.sh
```

### Graph (CLI)

ASCII chain:

```bash
source venv/bin/activate
python -m server.admin_cli graph sha256 <SHA256> --format ascii
```

Mermaid flowchart:

```bash
source venv/bin/activate
python -m server.admin_cli graph sha256 <SHA256> --format mermaid
```

Graphviz DOT:

```bash
source venv/bin/activate
python -m server.admin_cli graph sha256 <SHA256> --format dot
```

Raw JSON:

```bash
source venv/bin/activate
python -m server.admin_cli graph sha256 <SHA256> --format json
```

### Query (CLI)

Lookup records by SHA256:

```bash
source venv/bin/activate
python -m server.admin_cli query file <SHA256>
```

Query records for a machine (optional SHA256 filter):

```bash
source venv/bin/activate
python -m server.admin_cli query machine MachineNameA
python -m server.admin_cli query machine MachineNameA --sha256 <SHA256>
```

### Testing

```bash
source venv/bin/activate
python -m unittest discover -s tests -v
```
