#!/bin/bash
set -e
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
echo "Building..."
cd "$DIR/node" && go build -o ../bin/dbnode . && cd "$DIR"
echo "OK"

PEERS_1='["localhost:8002","localhost:8003"]'
PEERS_2='["localhost:8001","localhost:8003"]'
PEERS_3='["localhost:8001","localhost:8002"]'

PORT=8001 NODE_ID="alpha" PEERS="$PEERS_1" ./bin/dbnode >> logs/node1.log 2>&1 & echo $! > logs/node1.pid
PORT=8002 NODE_ID="beta"  PEERS="$PEERS_2" ./bin/dbnode >> logs/node2.log 2>&1 & echo $! > logs/node2.pid
PORT=8003 NODE_ID="gamma" PEERS="$PEERS_3" ./bin/dbnode >> logs/node3.log 2>&1 & echo $! > logs/node3.pid

echo "alpha  -> http://localhost:8001"
echo "beta   -> http://localhost:8002"
echo "gamma  -> http://localhost:8003"
echo "Dashboard: open dashboard/index.html"
echo "To stop: ./stop.sh"

trap "./stop.sh; exit 0" SIGINT SIGTERM
wait
