#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT"

if [[ ! -d venv ]]; then
  python3 -m venv venv
fi

source venv/bin/activate
python -m pip install -r client/requirements.txt >/dev/null

if [[ $# -eq 0 ]]; then
  exec python -m client.cli daemon
fi

exec python -m client.cli "$@"
