#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT"

if [[ ! -d venv ]]; then
  python3 -m venv venv
fi

source venv/bin/activate
python -m pip install -r requirements.txt >/dev/null

export FIM_DB_PATH="${FIM_DB_PATH:-$ROOT/data/fim.sqlite3}"

exec uvicorn server.main:app --host 0.0.0.0 --port 19991
