## FimSystem (SHA256 inventory / integrity)

Monorepo:

- `client/` Python client agent (scan + SHA256 + upload)
- `server/` FastAPI server (append-only SQLite + queries + admin CLI)

### Setup

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### Server (FastAPI on port `19991`)

Create/rotate a token for a `MachineName` (group):

```bash
source venv/bin/activate
python -m server.admin_cli token create MachineNameA
```

Start server:

```bash
./start_server.sh
```

Environment:

- `FIM_DB_PATH` (optional): SQLite path (default `data/fim.sqlite3`)

Endpoints:

- `GET /hello` (plain text `Hello`)
- `POST /ingest` (Bearer token)
- `GET /file/{sha256}`
- `GET /machine/{machine_name}`

### Client

Edit `client/config.json` and paste your token.

Note: scanning + SHA256 hashing can be CPU/disk intensive. For best results, schedule daemon runs during periods when the machine is relatively idle.

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

### Testing

```bash
source venv/bin/activate
python -m unittest discover -s tests -v
```

### Recent changes (2026-01-21)

- Client uploads are capped at 30 records/request, with HTTP retries (default 5) and exponential backoff.
- Client checks `GET /hello` before starting scans/uploads (fails fast if server unavailable).
- Client skips files that fail hashing (deleted/edited mid-scan) and does not count them toward quota/state.
- Server buffers `/ingest` writes in memory and flushes to SQLite in the background (availability > immediate consistency); read endpoints best-effort flush.
