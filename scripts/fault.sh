#!/usr/bin/env bash
# Fault-injection scenarios for ntick. Usage: scripts/fault.sh close|kill9|disk|fd|storm|stress|newday|malformed|all
# Each scenario runs in its own directory under a temp work dir and prints "RESULT <name> PASS|FAIL".
set -uo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
WORK=${WORK:-$(mktemp -d)}
BIN=$WORK/bin
SEOUL="Asia/Seoul"
PIDS=()
mkdir -p "$BIN"
cd "$ROOT"
for c in mockfeed ntick ntickctl tclient; do go build -o "$BIN/$c" "./cmd/$c" || exit 1; done

cleanup() { for p in "${PIDS[@]:-}"; do [[ -n $p ]] && kill -9 "$p" 2>/dev/null; done; PIDS=(); }
trap cleanup EXIT

port() { python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1])'; }

# Per-scenario setup: sets D (dir), DATA, TRUTH, ADDR, HPORT.
setup() {
  cleanup
  D=$WORK/$1; rm -rf "$D"; mkdir -p "$D"
  DATA=$D/data; TRUTH=$D/truth.csv
  ADDR=127.0.0.1:$(port); HPORT=$(port)
  mkdir -p "$DATA"
}

# start_feed [mockfeed flags...]: sets FEED pid.
start_feed() {
  "$BIN/mockfeed" -addr "$ADDR" -truth "$TRUTH" "$@" 2>"$D/mockfeed.log" &
  FEED=$!; PIDS+=($FEED); FEED_T0=$(date +%s.%N); sleep 0.5
}

# start_ntick [ntick flags...]: sets NTICK pid. Extra env via NTICK_PREFIX (e.g. "prlimit --fsize=N").
start_ntick() {
  # shellcheck disable=SC2086
  ${NTICK_PREFIX:-} "$BIN/ntick" -feed "$ADDR" -data "$DATA" -close "" "$@" >>"$D/ntick.log" 2>&1 &
  NTICK=$!; PIDS+=($NTICK)
}

# wait_stable FILE: until FILE stops growing for 3 s (and exists non-empty). Gives up after 240 s.
wait_stable() {
  local prev=-1 stable=0 cur t=0
  while ((stable < 3 && t < 240)); do
    sleep 1; t=$((t + 1))
    cur=$(stat -c %s "$1" 2>/dev/null || echo 0)
    if ((cur == prev && cur > 0)); then stable=$((stable + 1)); else stable=0; fi
    prev=$cur
  done
  ((stable >= 3))
}

# wait_drained: after the feed is done, wait until ntick has stored every emitted message
# (stored rows == truth lines), or, if that never holds (expected loss), until the stored count has not
# grown for 3 s. Fails after 240 s. Sets DRAIN_RATE (rows/s since the feed started).
wait_drained() {
  local want stored prev=-1 stable=0 t=0
  want=$(wc -l <"$TRUTH")
  while ((t < 240)); do
    if ! stored=$(T count-ticks -data "$DATA" 2>/dev/null); then stored=-2; sleep 1; t=$((t + 1)); continue; fi
    if ((stored == want)); then break; fi
    if ((stored == prev)); then stable=$((stable + 1)); else stable=0; fi
    ((stable >= 3)) && break
    prev=$stored; sleep 1; t=$((t + 1))
  done
  if ((t >= 240)); then echo "FAIL: ntick did not drain within 240 s (stored $stored of $want)"; return 1; fi
  DRAIN_RATE=$(python3 -c "import time;print(int($stored/max(time.time()-$FEED_T0,0.001)))")
  echo "drained: stored $stored of $want truth lines, ~$DRAIN_RATE ticks/s since feed start"
}
wait_done() { wait_stable "$TRUTH" && wait_drained; }

stop_ntick() { kill -TERM "$NTICK" 2>/dev/null; wait "$NTICK" 2>/dev/null; return 0; }

# result NAME OK DETAIL
result() { local r=PASS; (($2 != 0)) && r=FAIL; echo "RESULT $1 $r $3 (logs: $D)"; return "$2"; }

T() { "$BIN/tclient" "$@"; }

# meta_check DATADIR [DATE]: day_index must hold a row equal to day_stats.valid_count for every file (of DATE).
meta_check() {
  python3 - "$1" "${2:-}" <<'PY'
import sqlite3, sys, glob, os
data, date = sys.argv[1], sys.argv[2]
try:
    meta = {(s, d): v for s, d, v in sqlite3.connect(data + "/meta.db").execute("SELECT symbol, date, valid_count FROM day_index")}
except sqlite3.Error:
    meta = {}
n = bad = 0
for f in glob.glob(data + "/2*/*.db"):
    d = os.path.basename(os.path.dirname(f))
    if date and d != date:
        continue
    k = (os.path.basename(f)[:-3], d)
    v = sqlite3.connect(f).execute("SELECT valid_count FROM day_stats").fetchone()[0]
    n += 1
    if meta.get(k) != v:
        bad += 1
        print("day_index mismatch", k, meta.get(k), v)
print("day_index: %d files checked, %d mismatches" % (n, bad))
sys.exit(1 if bad or n == 0 else 0)
PY
}

scenario_close() {
  setup close; local ok=0 today compact sec add close_at now_hm target
  today=$(TZ=$SEOUL date +%Y-%m-%d); compact=${today//-/}
  sec=$(TZ=$SEOUL date +%S); add=1; ((10#$sec >= 45)) && add=2
  close_at=$(TZ=$SEOUL date -d "+$add minute" +%H:%M); now_hm=$(TZ=$SEOUL date +%H:%M)
  if [[ $close_at < $now_hm ]]; then result close 1 "close time wraps past midnight, rerun"; return; fi
  start_feed -start-date "$today" -days 1 -perday 2000 -symbols 5 -rate 0 -anomaly 0.05 -dup 0.05
  start_ntick -close "$close_at"
  wait_done || ok=1
  target=$(TZ=$SEOUL date -d "$today $close_at:05" +%s)
  while (($(date +%s) < target)); do sleep 1; done
  grep -q "closeout: 5 files checked, 0 with drift" "$D/ntick.log" || { echo "closeout log line missing"; ok=1; }
  meta_check "$DATA" "$compact" || ok=1
  "$BIN/ntickctl" closeout -data "$DATA" -date "$compact" >"$D/closeout.out" 2>&1 || { echo "ntickctl closeout disagrees"; cat "$D/closeout.out"; ok=1; }
  kill -0 "$NTICK" 2>/dev/null || { echo "ntick died"; ok=1; }
  stop_ntick
  local r1=$ok
  # CatchUp: past dates, no scheduler; day_index is empty until the next start.
  setup catchup; ok=0
  start_feed -days 3 -perday 2000 -symbols 5 -rate 0
  start_ntick; wait_done || ok=1; stop_ntick
  if [[ -e $DATA/meta.db ]] && [[ $(python3 -c "import sqlite3;print(sqlite3.connect('$DATA/meta.db').execute('select count(*) from day_index').fetchone()[0])") != 0 ]]; then
    echo "day_index not empty before restart"; ok=1
  fi
  start_ntick; sleep 3; stop_ntick
  meta_check "$DATA" || ok=1
  result close $((r1 | ok)) "scheduler close=$close_at today=$compact; catch-up of 15 past files"
}

scenario_kill9() {
  setup kill9; local ok=0 kills=0 want=12 i
  start_feed -days 3 -perday 20000 -symbols 5 -rate 2000 -anomaly 0.05 -reversal 0.05 -dup 0.05
  start_ntick
  for ((i = 0; i < want; i++)); do
    sleep "0.$((RANDOM % 9 + 3))"
    kill -0 "$FEED" 2>/dev/null || break
    kill -9 "$NTICK"; wait "$NTICK" 2>/dev/null; kills=$((kills + 1))
    T check-integrity -data "$DATA" >"$D/ci.$i.out" 2>&1 || { echo "integrity FAIL after kill $kills"; tail -3 "$D/ci.$i.out"; ok=1; }
    start_ntick
  done
  ((kills >= 10)) || { echo "only $kills kills"; ok=1; }
  wait_done || ok=1; sleep 1; stop_ntick
  T check-integrity -data "$DATA" | tail -1 || ok=1
  T verify-subsequence -data "$DATA" -truth "$TRUTH" | tee "$D/subseq.out" || ok=1
  result kill9 $ok "$kills kills; $(tail -1 "$D/subseq.out")"
}

scenario_disk() {
  setup disk; local ok=0 lim=${FSIZE:-1000000} code=0 i
  start_feed -days 3 -perday 4000 -symbols 5 -rate 500 -anomaly 0.05
  NTICK_PREFIX="prlimit --fsize=$lim" start_ntick
  for i in $(seq 120); do kill -0 "$NTICK" 2>/dev/null || break; sleep 0.5; done
  if kill -0 "$NTICK" 2>/dev/null; then echo "ntick still running: fsize limit never hit"; ok=1; kill -9 "$NTICK"; fi
  wait "$NTICK" 2>/dev/null; code=$?
  ((code != 0)) || { echo "ntick exit code 0"; ok=1; }
  grep -q "retrying once" "$D/ntick.log" || { echo "no retry log line"; ok=1; }
  echo "ntick exit code under fsize limit: $code; last log lines:"; tail -3 "$D/ntick.log"
  T check-integrity -data "$DATA" | tail -1 || ok=1
  start_ntick
  wait_done || ok=1; sleep 1; stop_ntick
  T check-integrity -data "$DATA" | tail -1 || ok=1
  T verify-subsequence -data "$DATA" -truth "$TRUTH" | tee "$D/subseq.out" || ok=1
  result disk $ok "exit code $code under fsize=$lim; $(tail -1 "$D/subseq.out")"
}

scenario_fd() {
  setup fd; local ok=0
  start_feed -days 2 -perday 30000 -symbols 300 -rate 0
  ( ulimit -n 256; exec "$BIN/ntick" -feed "$ADDR" -data "$DATA" -close "" >>"$D/ntick.log" 2>&1 ) &
  NTICK=$!; PIDS+=($NTICK)
  wait_done || ok=1; sleep 2
  kill -0 "$NTICK" 2>/dev/null || { echo "ntick died"; ok=1; }
  grep -i "emfile\|too many open files" "$D/ntick.log" && ok=1
  grep "file pool" "$D/ntick.log" | head -1
  stop_ntick
  T check-integrity -data "$DATA" | tail -1 || ok=1
  T verify -data "$DATA" -truth "$TRUTH" | tee "$D/verify.out" | tail -1 || ok=1
  result fd $ok "ulimit -n 256, 300 symbols, ${DRAIN_RATE:-?} ticks/s; $(tail -1 "$D/verify.out"); $(grep -m1 'file pool' "$D/ntick.log")"
}

scenario_storm() {
  setup storm; local ok=0 conns
  start_feed -days 3 -perday 2000 -symbols 5 -rate 2000 -disconnect 50 -anomaly 0.05 -reversal 0.05 -dup 0.05
  start_ntick
  wait_done || ok=1; sleep 1; stop_ntick
  conns=$(grep -c "disconnected after [1-9][0-9]* messages" "$D/ntick.log")
  T check-integrity -data "$DATA" | tail -1 || ok=1
  T verify-subsequence -data "$DATA" -truth "$TRUTH" | tee "$D/subseq.out" || ok=1
  result storm $ok "$conns data-carrying connections (reconnects); $(tail -1 "$D/subseq.out")"
}

# stress_run NAME START-TIME PERDAY: "stress" keeps the live feed inside one local day; "newday" lets the
# feed cross midnight so ingest creates new day files while readers are querying.
stress_run() {
  local name=$1 start=$2 perday=$3 rate=${4:-0}
  setup "$name"; local ok=0 today rj=0 rjbad=0 sl i
  # Phase 1: past data (3 days) so rejudge has targets.
  start_feed -days 3 -perday 3000 -symbols 5 -rate 0
  start_ntick; wait_done || ok=1; stop_ntick; kill "$FEED" 2>/dev/null
  # Phase 2: heavy live feed for today while readers and rejudge run.
  today=$(TZ=$SEOUL date +%Y-%m-%d)
  ADDR=127.0.0.1:$(port); TRUTH=$D/truth2.csv
  start_feed -start-date "$today" -start-time "$start" -days 1 -perday "$perday" -symbols 5 -rate "$rate" -anomaly 0.05
  start_ntick -http "127.0.0.1:$HPORT"
  sleep 2
  T stress-read -base "http://127.0.0.1:$HPORT" -symbols S100000,S100001,S100002 -duration "${DUR:-20s}" -clients 16 -ws 4 >"$D/stress.out" 2>&1 &
  sl=$!
  sleep 3
  while kill -0 "$sl" 2>/dev/null; do
    "$BIN/ntickctl" rejudge -data "$DATA" -date 20260105 -symbol S100000 >"$D/rejudge.out" 2>&1 || { rjbad=$((rjbad + 1)); cat "$D/rejudge.out"; }
    grep -q "to_invalid=0 to_valid=0" "$D/rejudge.out" || { rjbad=$((rjbad + 1)); echo "rejudge changed rows: $(cat "$D/rejudge.out")"; }
    rj=$((rj + 1)); sleep 1
  done
  wait "$sl" || ok=1
  cat "$D/stress.out"
  # mockfeed never exits, so "still ingesting" is judged from the stored row count.
  local stored; stored=$(T count-ticks -data "$DATA")
  ((stored < 9000 + perday)) || { echo "feed finished before the stress ended (not concurrent): $stored rows"; ok=1; }
  ((rjbad == 0)) || ok=1
  kill "$FEED" 2>/dev/null; sleep 1; stop_ntick
  T check-integrity -data "$DATA" | tail -1 || ok=1
  ! grep -q "panic\|fatal" "$D/ntick.log" || { echo "ntick log shows panic/fatal"; ok=1; }
  result "$name" $ok "$rj rejudge runs ($rjbad bad); $(grep -h 'stress-read:' "$D/stress.out")"
}

scenario_stress() { stress_run stress 00:00 800000 ${STRESS_RATE:-20000}; }
scenario_newday() { stress_run newday 23:55 400000 ${NEWDAY_RATE:-5000}; }

scenario_malformed() {
  setup malformed; local ok=0 before after good
  DATA=$D/a/b/data; mkdir -p "$DATA"
  before=$(cd "$D/a" && find . -mindepth 1 -not -path './b/data/*' | sort)
  python3 - "${ADDR##*:}" "$D/good.count" >"$D/feed.log" 2>&1 <<'PY' &
import socket, struct, sys, time
port, out = int(sys.argv[1]), sys.argv[2]
base = 1767571200000  # 2026-01-05 09:00 KST
def frame(sym, ts, price, qty, typ=1, ver=1):
    return struct.pack(">BB12sqqq", typ, ver, sym, ts, price, qty)
def pad(s): return s.ljust(12, b" ")
junk = [
    frame(pad(b"../../x"), base, 100, 1), frame(pad(b""), base, 100, 1), frame(b"A\x00B".ljust(12, b"\x00"), base, 100, 1),
    frame(pad(b"a/b"), base, 100, 1), frame(pad(b"A\x07"), base, 100, 1),
    frame(pad(b"GOOD1"), 0, 100, 1), frame(pad(b"GOOD1"), -1, 100, 1), frame(pad(b"GOOD1"), 2**63 - 1, 100, 1),
    frame(pad(b"GOOD1"), 946684799999, 100, 1), frame(pad(b"GOOD1"), 4102444800000, 100, 1),
    frame(pad(b"GOOD1"), base, 100, 1, typ=2), frame(pad(b"GOOD1"), base, 100, 1, ver=2),
    frame(pad(b"GOOD1"), base, 100, 1, typ=0, ver=0),
]
srv = socket.socket(); srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
srv.bind(("127.0.0.1", port)); srv.listen(1)
c, _ = srv.accept()
good = 0
for i in range(60):
    sym = b"GOOD1" if i % 2 == 0 else b"GOOD2"
    price, qty = (100 + i, 1)
    if i == 7: price = 0     # stored as invalid
    if i == 9: qty = -1      # stored as invalid
    c.sendall(frame(pad(sym), base + i * 1000, price, qty)); good += 1
    for j in junk[i % len(junk):] + junk[:i % len(junk)]:
        c.sendall(j)
open(out, "w").write(str(good))
time.sleep(3)
c.close()
PY
  PIDS+=($!)
  sleep 0.5
  start_ntick
  for _ in $(seq 100); do [[ -s $D/good.count ]] && break; sleep 0.2; done
  for _ in $(seq 120); do
    [[ $(T count-ticks -data "$DATA") == "$(cat "$D/good.count")" ]] && break; sleep 0.5
  done
  sleep 1 # a stray extra row would show up in the final count
  kill -0 "$NTICK" 2>/dev/null || { echo "ntick died"; ok=1; }
  stop_ntick
  good=$(cat "$D/good.count")
  python3 - "$DATA" "$good" <<'PY' || ok=1
import sqlite3, sys, glob, os
data, good = sys.argv[1], int(sys.argv[2])
n = sum(sqlite3.connect(f).execute("select count(*) from ticks").fetchone()[0] for f in glob.glob(data + "/*/*.db"))
dirs = sorted(d for d in os.listdir(data) if os.path.isdir(os.path.join(data, d)))
files = sorted(os.path.basename(f) for f in glob.glob(data + "/*/*"))
print("stored rows %d (expected %d), dirs %s, files %s" % (n, good, dirs, [f for f in files if not f.endswith(('-wal', '-shm'))]))
bad = n != good or dirs != ["20260105"] or [f for f in files if not f.endswith(('-wal', '-shm'))] != ["GOOD1.db", "GOOD2.db"]
sys.exit(1 if bad else 0)
PY
  after=$(cd "$D/a" && find . -mindepth 1 -not -path './b/data/*' | sort)
  [[ $before == "$after" ]] || { echo "stray files outside data:"; diff <(echo "$before") <(echo "$after"); ok=1; }
  grep -m2 "rejected" "$D/ntick.log"
  result malformed $ok "$good good frames among $((good * 13)) junk frames"
}

RC=0
run() { "scenario_$1"; local r=$?; ((r == 0)) || RC=1; cleanup; }
case ${1:-} in
  close | kill9 | disk | fd | storm | stress | newday | malformed) run "$1" ;;
  all) for s in close kill9 disk fd storm stress newday malformed; do run $s; done ;;
  *) echo "usage: $0 close|kill9|disk|fd|storm|stress|newday|malformed|all"; exit 2 ;;
esac
exit $RC
