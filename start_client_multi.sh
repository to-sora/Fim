#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT"

if [[ ! -d venv ]]; then
  python3 -m venv venv
fi

source venv/bin/activate
python -m pip install -r client/requirements.txt >/dev/null

# Create required directories
mkdir -p data/state data/locks log

# Run validation first
echo "Validating all FIM_config_*.json files..."
if ! python -m client.cli validate-all --config-dir client; then
  echo "Validation failed. Please fix config errors before starting." >&2
  exit 1
fi

# Find and start daemons for each config
CONFIG_COUNT=0
for config_file in client/FIM_config_*.json; do
  [[ -e "$config_file" ]] || continue

  # Extract config ID from filename (e.g., FIM_config_01.json -> 01)
  config_id=$(basename "$config_file" | sed 's/FIM_config_\([0-9]*\)\.json/\1/')

  # Get tag from config or derive from filename
  tag=$(python -c "
import json
from pathlib import Path
cfg = json.loads(Path('$config_file').read_text())
tag = cfg.get('tag', '').strip()
if not tag:
    tag = 'config_$config_id'
print(tag)
")

  state_path="data/state/${tag}.json"
  lock_path="data/locks/${tag}.lock"
  log_path="log/client_${tag}.log"

  echo "Starting daemon for config $config_id (tag: $tag)..."
  echo "  Config: $config_file"
  echo "  State:  $state_path"
  echo "  Lock:   $lock_path"
  echo "  Log:    $log_path"

  # Start daemon in background
  nohup python -m client.cli --config "$config_file" daemon \
    --state-path "$state_path" \
    --log-path "$log_path" \
    >> "$log_path" 2>&1 &

  echo "  PID: $!"
  CONFIG_COUNT=$((CONFIG_COUNT + 1))
done

if [[ $CONFIG_COUNT -eq 0 ]]; then
  echo "No FIM_config_*.json files found in client/" >&2
  exit 1
fi

echo "Started $CONFIG_COUNT daemon(s)."
