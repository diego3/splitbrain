#!/bin/bash
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG="$DIR/logs"
BIN="$DIR/bin/dbnode"
PORTS=([1]=8001 [2]=8002 [3]=8003)
NAMES=([1]="alpha" [2]="beta" [3]="gamma")
PEERS=(
  [1]='["localhost:8002","localhost:8003"]'
  [2]='["localhost:8001","localhost:8003"]'
  [3]='["localhost:8001","localhost:8002"]'
)
cmd=$1; n=$2
case "$cmd" in
  kill|stop)
    f="$LOG/node${n}.pid"
    [ ! -f "$f" ] && echo "no PID file" && exit 1
    PID=$(cat "$f")
    kill "$PID" 2>/dev/null && echo "${NAMES[$n]} (${PORTS[$n]}) killed" && rm "$f" || echo "already stopped"
    ;;
  start|up)
    PORT=${PORTS[$n]} NODE_ID=${NAMES[$n]} PEERS=${PEERS[$n]} \
    "$BIN" >> "$LOG/node${n}.log" 2>&1 &
    echo $! > "$LOG/node${n}.pid"
    echo "${NAMES[$n]} (${PORTS[$n]}) started"
    ;;
  status)
    for i in 1 2 3; do
      f="$LOG/node${i}.pid"
      if [ -f "$f" ] && kill -0 "$(cat $f)" 2>/dev/null; then
        R=$(curl -s --max-time 1 "http://localhost:${PORTS[$i]}/status" 2>/dev/null)
        S=$(echo "$R" | python3 -c "import sys,json;d=json.load(sys.stdin);print(d['state'],d['term'],d['log_size'])" 2>/dev/null)
        echo "  UP   ${NAMES[$i]} :${PORTS[$i]} | $S"
      else
        echo "  DOWN ${NAMES[$i]} :${PORTS[$i]} | OFFLINE"
      fi
    done
    f="$LOG/dashboard.pid"
    if [ -f "$f" ] && kill -0 "$(cat $f)" 2>/dev/null; then
      echo "  UP   dashboard  :8080  | http://localhost:8080/dash"
    else
      echo "  DOWN dashboard  :8080  | OFFLINE"
    fi
    ;;
  logs) tail -f "$LOG/node${n}.log" ;;
  *) echo "usage: $0 {kill|start|status|logs} [1|2|3]" ;;
esac
