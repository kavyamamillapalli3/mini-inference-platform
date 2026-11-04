#!/usr/bin/env bash
# Start N fake replicas (ports 8011..), the router (8002) and the gateway (8000).
# Extra settings pass through as env vars, e.g.:
#   POLICY=fifo ROUTING=least_loaded ./scripts/start.sh 4
set -euo pipefail
cd "$(dirname "$0")/.."
N="${1:-2}"
mkdir -p logs
: > logs/pids

replicas=""
for i in $(seq 1 "$N"); do
  port=$((8010 + i))
  python -m uvicorn fake_replica.server:app --port "$port" --log-level warning \
    > "logs/replica_$port.log" 2>&1 &
  echo $! >> logs/pids
  replicas="${replicas:+$replicas,}http://localhost:$port"
done

REPLICAS="$replicas" python -m uvicorn router.server:app --port 8002 --log-level warning \
  > logs/router.log 2>&1 &
echo $! >> logs/pids

UPSTREAM=http://localhost:8002 python -m uvicorn gateway.server:app --port 8000 --log-level warning \
  > logs/gateway.log 2>&1 &
echo $! >> logs/pids

sleep 2
echo "replicas: $replicas"
echo "router:   http://localhost:8002  (POLICY=${POLICY:-priority} ROUTING=${ROUTING:-affinity})"
echo "gateway:  http://localhost:8000"
echo "stop with ./scripts/stop.sh"
