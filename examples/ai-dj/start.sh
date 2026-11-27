#!/usr/bin/env bash
# Start every AI DJ service, wait until each one answers, then open the web UI.
#
#   ./start.sh
#
# Logs go to data/logs/<service>.log. Press Ctrl+C to stop everything this
# script started. Services that were already running are left alone.
set -euo pipefail
cd "$(dirname "$0")"

if [ ! -f config/navidrome.toml ] || [ ! -f data/track_clamp3.parquet ]; then
  echo "Run ./setup.sh first."
  exit 1
fi

mkdir -p data/logs
pids=""
stop_all() {
  [ -n "$pids" ] && kill $pids 2>/dev/null || true
  wait 2>/dev/null || true
}
trap stop_all EXIT
trap 'echo; echo "Stopping..."; exit 0' INT TERM

up() { [ "$(curl -s -o /dev/null -w '%{http_code}' --max-time 2 "$1")" != "000" ]; }

# start <name> <url that answers once it is up> <command...>
start() {
  local name=$1 url=$2 pid
  shift 2
  if up "$url"; then
    printf '  %-10s already running\n' "$name"
    return
  fi
  "$@" >"data/logs/$name.log" 2>&1 &
  pid=$!
  pids="$pids $pid"
  printf '  %-10s starting' "$name"
  for _ in $(seq 1 300); do
    if up "$url"; then
      echo " ok"
      return
    fi
    if ! kill -0 "$pid" 2>/dev/null; then
      echo " failed. Last lines of data/logs/$name.log:"
      tail -20 "data/logs/$name.log"
      exit 1
    fi
    sleep 1
    printf .
  done
  echo " timed out, see data/logs/$name.log"
  exit 1
}

echo "Starting the AI DJ:"
start ollama http://127.0.0.1:11434/api/tags ollama serve
start clamp3 http://127.0.0.1:6580/health sh -c 'cd clamp3 && exec .venv/bin/python server.py'
start feast http://127.0.0.1:6566/health uv run python -m dj.server
start mcp http://127.0.0.1:6590/mcp uv run feast mcp --config feast_mcp.yaml
start bridge http://127.0.0.1:6570/docs uv run python -m dj.bridge
start navidrome http://127.0.0.1:4533/ping navidrome --configfile config/navidrome.toml
uv run python scripts/init_navidrome.py
start ui http://127.0.0.1:6600/api/status uv run python -m dj.ui

UI=http://127.0.0.1:6600
printf '\nAI DJ is up: %s   (Navidrome: http://127.0.0.1:4533)\n' "$UI"
printf 'Logs: data/logs/. Press Ctrl+C to stop.\n'
command -v open >/dev/null && open "$UI"
wait
