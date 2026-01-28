#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT"

if [[ ! -d venv ]]; then
  python3 -m venv venv
fi

source venv/bin/activate
python -m pip install -r server/requirements.txt >/dev/null

export FIM_DB_PATH="${FIM_DB_PATH:-$ROOT/data/fim.sqlite3}"
export FIM_WEB_HOST="${FIM_WEB_HOST:-0.0.0.0}"
export FIM_WEB_PORT="${FIM_WEB_PORT:-19992}"

exec uvicorn server.web_app:app --host "${FIM_WEB_HOST}" --port "${FIM_WEB_PORT}"
