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
  echo "usage: $0 <client.cli args> (include --state-path and --log-path for run/daemon)" >&2
  exit 1
fi

exec python -m client.cli "$@"
