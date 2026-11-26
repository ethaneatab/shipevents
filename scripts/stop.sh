#!/usr/bin/env bash
# Stops what demo.sh started, by PID file only. --infra also stops this project's containers (data kept).
cd "$(dirname "$0")/.."
for f in .runtime/*.pid; do
  [ -e "$f" ] || continue
  pid=$(cat "$f")
  # make sure the PID still belongs to one of our processes before signalling it
  if ps -p "$pid" -o command= 2>/dev/null | grep -qE "src/(processor|api|downstream)/main.ts|vite/bin/vite.js"; then
    kill "$pid" 2>/dev/null && echo "stopped $(basename "$f" .pid) ($pid)"
  fi
  rm -f "$f"
done
if [ "${1:-}" = "--infra" ]; then
  ${COMPOSE:-/opt/homebrew/bin/docker-compose} -p shipevents --profile obs stop
fi
