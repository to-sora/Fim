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

- `POST /ingest` (Bearer token)
- `GET /file/{sha256}`
- `GET /machine/{machine_name}`

### Client

Edit `client/config.json` and paste your token.

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
