#!/bin/bash
set -e
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

mkdir -p "$DIR/logs" "$DIR/bin"

echo "Building..."
cd "$DIR/node"
CGO_ENABLED=0 go build -o "$DIR/bin/dbnode" .
cd "$DIR"
echo "OK"

PEERS_1='["localhost:8002","localhost:8003"]'
PEERS_2='["localhost:8001","localhost:8003"]'
PEERS_3='["localhost:8001","localhost:8002"]'

PORT=8001 NODE_ID="alpha" PEERS="$PEERS_1" "$DIR/bin/dbnode" >> "$DIR/logs/node1.log" 2>&1 & echo $! > "$DIR/logs/node1.pid"
PORT=8002 NODE_ID="beta"  PEERS="$PEERS_2" "$DIR/bin/dbnode" >> "$DIR/logs/node2.log" 2>&1 & echo $! > "$DIR/logs/node2.pid"
PORT=8003 NODE_ID="gamma" PEERS="$PEERS_3" "$DIR/bin/dbnode" >> "$DIR/logs/node3.log" 2>&1 & echo $! > "$DIR/logs/node3.pid"

python3 "$DIR/server.py" >> "$DIR/logs/dashboard.log" 2>&1 & echo $! > "$DIR/logs/dashboard.pid"

echo "alpha     -> http://localhost:8001"
echo "beta      -> http://localhost:8002"
echo "gamma     -> http://localhost:8003"
echo "dashboard -> http://localhost:8080/dashboard"
echo "To stop: ./stop.sh"
