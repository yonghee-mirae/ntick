#!/usr/bin/env bash
# End-to-end test: mockfeed -> ntick -> SQLite files -> tclient.
# Usage: scripts/e2e.sh [--kill9]
# Env overrides: ADDR SYMBOLS DAYS PERDAY RATE SEED FEED_FLAGS
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
ADDR=${ADDR:-127.0.0.1:19000}
SYMBOLS=${SYMBOLS:-5}
DAYS=${DAYS:-3}
PERDAY=${PERDAY:-2000}
RATE=${RATE:-1000}
SEED=${SEED:-1}
FEED_FLAGS=${FEED_FLAGS:--anomaly 0.05 -reversal 0.05 -dup 0.05}
PORT=${PORT:-$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1])')}
KILL9=0
[[ ${1:-} == --kill9 ]] && KILL9=1

WORK=$(mktemp -d)
BIN=$WORK/bin DATA=$WORK/data TRUTH=$WORK/truth.csv
mkdir -p "$BIN" "$DATA"
PIDS=()
cleanup() { for p in "${PIDS[@]}"; do kill "$p" 2>/dev/null || true; done; }
trap cleanup EXIT

cd "$ROOT"
for c in mockfeed ntick ntickctl tclient; do go build -o "$BIN/$c" "./cmd/$c"; done

start_feed() {
  # shellcheck disable=SC2086
  "$BIN/mockfeed" -addr "$ADDR" -seed "$SEED" -symbols "$SYMBOLS" -days "$DAYS" -perday "$PERDAY" \
    -rate "$RATE" $FEED_FLAGS -truth "$TRUTH" 2>"$WORK/mockfeed.log" &
  PIDS+=($!)
  sleep 0.5
}

start_ntick() {
  "$BIN/ntick" -feed "$ADDR" -data "$DATA" -http "127.0.0.1:$PORT" >>"$WORK/ntick.log" 2>&1 &
  NTICK=$!
  PIDS+=($NTICK)
}

if ((KILL9)); then
  start_feed
  start_ntick
  sleep 2
  kill -9 "$NTICK"
  wait "$NTICK" 2>/dev/null || true
  echo "ntick killed with SIGKILL, restarting"
  start_ntick
else
  # Stream subscribers must be live before the first tick, so ntick starts before the feed exists.
  start_ntick
  SYMLIST=$(seq -s, 100000 $((100000 + SYMBOLS - 1)) | sed 's/\(^\|,\)/\1S/g')
  "$BIN/tclient" verify-stream -truth "$TRUTH" -base "ws://127.0.0.1:$PORT" -symbols "$SYMLIST" \
    -ready "$WORK/stream.ready" >"$WORK/stream.out" 2>&1 &
  STREAM=$!
  PIDS+=($STREAM)
  for _ in $(seq 100); do [[ -e $WORK/stream.ready ]] && break; sleep 0.1; done
  [[ -e $WORK/stream.ready ]] || { cat "$WORK/stream.out"; echo "stream clients not ready"; exit 1; }
  start_feed
fi

# The feed is done when the truth file stops growing (3 stable seconds).
prev=-1 stable=0
while ((stable < 3)); do
  sleep 1
  cur=$(stat -c %s "$TRUTH")
  if ((cur == prev && cur > 0)); then stable=$((stable + 1)); else stable=0; fi
  prev=$cur
done
sleep 1
# The feed is done once the truth stops growing; ntick may still be draining its socket buffer.
want=$(wc -l <"$TRUTH") stored=0 prev=-1 stable=0
for _ in $(seq 240); do
  stored=$("$BIN/tclient" count-ticks -data "$DATA")
  ((stored == want)) && break
  if ((stored == prev)); then stable=$((stable + 1)); else stable=0; fi
  ((stable >= 3)) && break
  prev=$stored; sleep 1
done
echo "drained: stored $stored of $want truth lines"
rc=0
# verify-api needs the server, so it runs before ntick is stopped.
((KILL9)) || "$BIN/tclient" verify-api -truth "$TRUTH" -base "http://127.0.0.1:$PORT" || rc=1
kill -TERM "$NTICK"
wait "$NTICK" 2>/dev/null || true
if ((!KILL9)); then
  wait "$STREAM" || rc=1
  cat "$WORK/stream.out"
fi

# Tamper a past-date file, then closeout must report drift and rejudge must repair it.
rejudge_recovery() {
  local f date ctl="$BIN/ntickctl" ok=0
  f=$(ls "$DATA"/2*/*.db | head -1)
  date=$(basename "$(dirname "$f")")
  python3 - "$f" <<'PY' || return 1
import sqlite3, sys
c = sqlite3.connect(sys.argv[1])
n = lambda q: c.execute(q).fetchone()[0]
assert n("SELECT count(*) FROM ticks WHERE valid = 0") >= 1, "need an invalid tick to flip"
c.execute("UPDATE ticks SET valid = 0 WHERE raw_seq % 13 = 0 AND valid = 1")
c.execute("UPDATE ticks SET valid = 1 WHERE raw_seq IN (SELECT raw_seq FROM ticks WHERE valid = 0 LIMIT 3)")
c.execute("UPDATE day_stats SET valid_count = valid_count + 7")
c.execute("UPDATE candle_1m SET volume = volume + 1, tick_count = tick_count + 3 WHERE minute = (SELECT min(minute) FROM candle_1m)")
c.commit()
PY
  if "$ctl" closeout -data "$DATA" -date "$date" >"$WORK/closeout1.out" 2>&1; then
    echo "FAIL: closeout did not report drift on tampered $f"; ok=1
  fi
  if "$ctl" rejudge -data "$DATA" -date "$(TZ=Asia/Seoul date +%Y%m%d)" >/dev/null 2>&1; then
    echo "FAIL: rejudge accepted today's date"; ok=1
  fi
  if "$ctl" rejudge -data "$DATA" -date 29991231 >/dev/null 2>&1; then
    echo "FAIL: rejudge accepted a future date"; ok=1
  fi
  "$ctl" rejudge -data "$DATA" -date "$date" >"$WORK/rejudge.out" 2>&1 || { echo "FAIL: rejudge"; cat "$WORK/rejudge.out"; ok=1; }
  for d in "$DATA"/2*/; do
    "$ctl" closeout -data "$DATA" -date "$(basename "$d")" >"$WORK/closeout2.out" 2>&1 || { echo "FAIL: closeout after rejudge"; cat "$WORK/closeout2.out"; ok=1; }
  done
  python3 - "$DATA" <<'PY' || ok=1
import sqlite3, sys, glob, os
meta = dict(((s, d), v) for s, d, v in sqlite3.connect(sys.argv[1] + "/meta.db").execute("SELECT symbol, date, valid_count FROM day_index"))
bad = 0
for f in glob.glob(sys.argv[1] + "/2*/*.db"):
    k = (os.path.basename(f)[:-3], os.path.basename(os.path.dirname(f)))
    v = sqlite3.connect(f).execute("SELECT valid_count FROM day_stats").fetchone()[0]
    if meta.get(k) != v:
        print("day_index mismatch", k, meta.get(k), v); bad += 1
print("day_index vs day_stats: %d files, %d mismatches" % (len(meta), bad))
sys.exit(bad > 0)
PY
  "$BIN/tclient" verify -data "$DATA" -truth "$TRUTH" || ok=1
  "$BIN/tclient" verify-ntick -data "$DATA" -truth "$TRUTH" || ok=1
  "$BIN/tclient" verify-time -data "$DATA" -truth "$TRUTH" || ok=1
  ((ok == 0)) && echo "rejudge recovery: PASS ($date)"
  return $ok
}

echo "truth lines: $(wc -l <"$TRUTH"), work dir: $WORK"
"$BIN/tclient" check-integrity -data "$DATA" || rc=1
if ((KILL9)); then
  echo "kill9 mode: verify skipped (in-flight messages are lost by design)"
else
  "$BIN/tclient" verify -data "$DATA" -truth "$TRUTH" || rc=1
  "$BIN/tclient" verify-ntick -data "$DATA" -truth "$TRUTH" || rc=1
  "$BIN/tclient" verify-time -data "$DATA" -truth "$TRUTH" || rc=1
  rejudge_recovery || rc=1
fi
((rc == 0)) && echo "E2E PASS" || echo "E2E FAIL (logs in $WORK)"
exit $rc
