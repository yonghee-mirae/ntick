#!/usr/bin/env bash
# Perf measurement helpers (Sprint 5). Usage:
#   scripts/bench.sh ingest SYMBOLS TOTAL_TICKS [mockfeed flags...]   # mockfeed -> ntick e2e, prints one result line
#   scripts/bench.sh feedonly SYMBOLS TOTAL_TICKS [mockfeed flags...] # mockfeed -> nc >/dev/null (generator limit)
# Env: BIN (binaries dir), WORK (scratch dir), PORT, NTICK_FLAGS (extra ntick flags), NTICK_BIN
set -euo pipefail
ROOT=$(cd "$(dirname "$0")/.." && pwd)
WORK=${WORK:-${TMPDIR:-/tmp}/ntick-bench}
BIN=${BIN:-$WORK/bin}
PORT=${PORT:-19100}
NTICK_BIN=${NTICK_BIN:-$BIN/ntick}
if [[ ! -x $BIN/ntick || ! -x $BIN/mockfeed ]]; then mkdir -p "$BIN"; for c in ntick mockfeed; do (cd "$ROOT" && go build -o "$BIN/$c" "./cmd/$c"); done; fi
mode=$1 syms=$2 total=$3; shift 3
now() { date +%s.%N; }

if [[ $mode == feedonly ]]; then
  "$BIN/mockfeed" -addr 127.0.0.1:$PORT -symbols "$syms" -days 1 -perday "$total" -rate 0 -truth /dev/null "$@" 2>/dev/null &
  pid=$!; sleep 0.3
  t0=$(now); nc -d 127.0.0.1 $PORT >/dev/null; t1=$(now)
  kill $pid 2>/dev/null || true
  awk -v t0=$t0 -v t1=$t1 -v n=$total -v s=$syms 'BEGIN{printf "feedonly syms=%d ticks=%d secs=%.2f rate=%.0f/s\n", s, n, t1-t0, n/(t1-t0)}'
  exit 0
fi

D=$WORK/run.$$; mkdir -p $D/data
"$BIN/mockfeed" -addr 127.0.0.1:$PORT -symbols "$syms" -days 1 -perday "$total" -rate 0 -truth $D/truth.csv "$@" 2>$D/mf.log &
mf=$!; sleep 0.3
t0=$(now)
/usr/bin/time -v -o $D/time.txt "$NTICK_BIN" -feed 127.0.0.1:$PORT -data $D/data -close "" ${NTICK_FLAGS:-} 2>$D/ntick.log &
nt=$!
# wait for the feed to end (ntick logs the disconnect)
while ! grep -q "feed disconnected" $D/ntick.log 2>/dev/null; do sleep 0.02; done
t1=$(now)
pkill -TERM -P $nt || true  # nt is /usr/bin/time; its child is ntick
wait $nt 2>/dev/null || true
t2=$(now)
kill $mf 2>/dev/null || true; wait $mf 2>/dev/null || true
recv=$(grep -o 'after [0-9]* messages' $D/ntick.log | head -1 | awk '{print $2}')
user=$(awk -F: '/User time/{print $2}' $D/time.txt); sys=$(awk -F: '/System time/{print $2}' $D/time.txt)
rss=$(awk -F: '/Maximum resident/{print $2}' $D/time.txt)
files=$(find $D/data -name '*.db' | wc -l); bytes=$(du -sb $D/data | cut -f1)
awk -v s=$syms -v n=$recv -v t0=$t0 -v t1=$t1 -v t2=$t2 -v u="$user" -v sy="$sys" -v r="$rss" -v f=$files -v b=$bytes -v fl="$*" 'BEGIN{
 printf "ingest syms=%d msgs=%d feed_s=%.2f drain_s=%.2f total_s=%.2f rate_feed=%.0f/s rate_total=%.0f/s cpu_u=%.1f cpu_s=%.1f cpu_pct=%.0f rss_mb=%.0f files=%d bytes=%d flags=[%s]\n", s, n, t1-t0, t2-t1, t2-t0, n/(t1-t0), n/(t2-t0), u, sy, (u+sy)/(t2-t0)*100, r/1024, f, b, fl}'
[[ ${KEEP:-0} == 1 ]] || rm -rf $D
