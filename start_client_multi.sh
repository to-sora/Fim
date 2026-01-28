#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT"

if [[ ! -d venv ]]; then
  python3 -m venv venv
fi

source venv/bin/activate
python -m pip install -r client/requirements.txt >/dev/null

CONFIG_DIR="${FIM_CONFIG_DIR:-$ROOT/client}"
PATTERN="${FIM_CONFIG_PATTERN:-FIM_config_[0-9]*.json}"
STATE_DIR="${FIM_STATE_DIR:-$ROOT/data/state}"
LOG_DIR="${FIM_LOG_DIR:-$ROOT/log}"
POLL_SEC="${FIM_POLL_SEC:-20}"
MIN_GAP_MIN="${FIM_MIN_GAP_MIN:-5}"
MODE="${FIM_MODE:-daemon}"

mkdir -p "$STATE_DIR" "$LOG_DIR"

python -m client.cli verify-configs --config-dir "$CONFIG_DIR" --pattern "$PATTERN" --min-gap-min "$MIN_GAP_MIN"

shopt -s nullglob
configs=("$CONFIG_DIR"/$PATTERN)
shopt -u nullglob

if [[ ${#configs[@]} -eq 0 ]]; then
  echo "no configs matched $PATTERN in $CONFIG_DIR" >&2
  exit 2
fi

pids=()
for cfg in "${configs[@]}"; do
  base="$(basename "$cfg" .json)"
  state_path="$STATE_DIR/$base.json"
  log_path="$LOG_DIR/$base.log"
  if [[ "$MODE" == "daemon" ]]; then
    python -m client.cli daemon \
      --config "$cfg" \
      --state-path "$state_path" \
      --log-path "$log_path" \
      --poll-sec "$POLL_SEC" &
  else
    python -m client.cli run \
      --config "$cfg" \
      --state-path "$state_path" \
      --log-path "$log_path" &
  fi
  pids+=("$!")
done

trap 'for pid in "${pids[@]}"; do kill "$pid" 2>/dev/null || true; done' INT TERM

wait
