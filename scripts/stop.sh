#!/usr/bin/env bash
# Stop everything started by scripts/start.sh
cd "$(dirname "$0")/.."
if [ -f logs/pids ]; then
  while read -r pid; do kill "$pid" 2>/dev/null || true; done < logs/pids
  rm -f logs/pids
fi
echo "stopped"
