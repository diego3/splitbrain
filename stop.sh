#!/bin/bash
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/logs"
for i in 1 2 3; do
  f="$DIR/node${i}.pid"
  [ -f "$f" ] && PID=$(cat "$f") && kill "$PID" 2>/dev/null && echo "node $i (PID $PID) stopped" && rm "$f"
done
