#!/usr/bin/env bash
# One command to bring up everything: infra in docker (compose project "shipevents"),
# the three Node services and the Vite frontend on the host. Works from an empty volume.
set -euo pipefail
cd "$(dirname "$0")/.."
ROOT=$(pwd)
COMPOSE=${COMPOSE:-/opt/homebrew/bin/docker-compose}
RT=.runtime
mkdir -p "$RT/logs"

# Only ever stop processes this script started (by PID file), never by name.
./scripts/stop.sh >/dev/null 2>&1 || true

echo "== infra (kafka 19210, postgres 54310, prometheus 9210, grafana 3210)"
$COMPOSE -p shipevents --profile obs up -d
for i in $(seq 1 90); do
  k=$(docker inspect -f '{{.State.Health.Status}}' shipevents-kafka-1 2>/dev/null || echo none)
  p=$(docker inspect -f '{{.State.Health.Status}}' shipevents-postgres-1 2>/dev/null || echo none)
  [ "$k" = healthy ] && [ "$p" = healthy ] && break
  sleep 1
done
[ "$k" = healthy ] && [ "$p" = healthy ] || { echo "infra not healthy (kafka=$k postgres=$p)"; exit 1; }

[ -d node_modules ] || npm ci --no-audit --no-fund
[ -d frontend/node_modules ] || (cd frontend && npm ci --no-audit --no-fund)
node scripts/learn_anchors.mjs >/dev/null

start() { # name, then command...
  local name=$1; shift
  "$@" >"$RT/logs/$name.log" 2>&1 &
  echo $! >"$RT/$name.pid"
}
wait_http() { # url, seconds
  for i in $(seq 1 "$2"); do curl -sf "$1" >/dev/null 2>&1 && return 0; sleep 1; done
  echo "timeout waiting for $1"; return 1
}

echo "== services"
start downstream node src/downstream/main.ts
wait_http http://127.0.0.1:8902/healthz 30
# The processor migrates the schema; the API waits for it so they never race on an empty database.
start processor env LEARN_MODE=1 node src/processor/main.ts
wait_http http://127.0.0.1:8900/healthz 60
start api node src/api/main.ts
wait_http http://127.0.0.1:8901/healthz 30
# @learn:frontend-start
# exec, so the PID file holds vite itself; redirect the subshell so it never holds our stdout open.
(cd frontend && exec env VITE_ADMIN_TOKEN="${ADMIN_TOKEN:-dev-admin-token-change-me}" node node_modules/vite/bin/vite.js --port 5190 --strictPort) >"$RT/logs/frontend.log" 2>&1 &
echo $! >"$RT/frontend.pid"
# @learn:end frontend-start
wait_http http://127.0.0.1:5190/ 60

echo "== seed (idempotent: each demo shipment is only sent if it is not there yet)"
TOKEN=${ADMIN_TOKEN:-dev-admin-token-change-me}
has() { curl -sf "http://127.0.0.1:8901/v1/shipments/$1" >/dev/null; }
has_dlq() { curl -sf -H "authorization: Bearer $TOKEN" "http://127.0.0.1:8900/admin/dlq?shipmentId=$1" | grep -q '"id"'; }
has SHP-DEMO0001 || node src/sim/cli.ts journey --shipment SHP-DEMO0001 >/dev/null
has SHP-DEMO0002 || node src/sim/cli.ts out-of-order --shipment SHP-DEMO0002 >/dev/null
has SHP-DEMO0003 || node src/sim/cli.ts illegal --shipment SHP-DEMO0003 >/dev/null
has_dlq SHP-DEMO0004 || node src/sim/cli.ts malformed --shipment SHP-DEMO0004 >/dev/null
for i in $(seq 1 30); do
  has SHP-DEMO0002 && has SHP-DEMO0003 && has_dlq SHP-DEMO0004 && break
  sleep 1
done
has SHP-DEMO0002 && has_dlq SHP-DEMO0004 || { echo "seed did not land"; exit 1; }

cat <<MSG
== up
  frontend     http://127.0.0.1:5190/track?id=SHP-DEMO0001   (/ops, /learn)
  customer API http://127.0.0.1:8901/v1/shipments/SHP-DEMO0001
  processor    http://127.0.0.1:8900/metrics   (admin: Bearer token from ADMIN_TOKEN)
  fake ETA     http://127.0.0.1:8902/faults
  grafana      http://127.0.0.1:3210   prometheus http://127.0.0.1:9210
  stop: ./scripts/stop.sh   (add --infra to also stop the containers)
MSG
