#!/usr/bin/env bash
# Write-path comparison of three storage layouts (docs/perf-storage-modes.md).
# Usage: scripts/bench_modes.sh MATRIX   (matrix = a | b | c | skew | read | all)
# Env: WORK (scratch dir, never inside the repo), REPS (default 3), TICKS (default 1000000)
# Output: one "LABEL RESULT ..." line per run on stdout. Aggregate with scripts/bench_modes_sum.py.
set -uo pipefail
ROOT=$(cd "$(dirname "$0")/.." && pwd)
WORK=${WORK:-${TMPDIR:-/tmp}/ntick-modes}
REPS=${REPS:-3}; TICKS=${TICKS:-1000000}
mkdir -p "$WORK"
BIN=$WORK/tclient_modes
(cd "$ROOT" && go build -o "$BIN" ./cmd/tclient) || exit 1
trap 'pkill -P $$ -f tclient_modes 2>/dev/null; pkill -f "$BIN modes-child" 2>/dev/null' EXIT
mem_ok() { awk '/MemAvailable/{exit ($2 < 1400*1024)}' /proc/meminfo; }
run() { # label args...
  local label=$1; shift
  for ((i = 1; i <= REPS; i++)); do
    mem_ok || { echo "$label SKIP low memory"; continue; }
    echo "$label $(timeout 600 "$BIN" modes-run -dir "$WORK/d" "$@" 2>&1 | grep -E '^(RESULT|ABORT|ERR)' | head -1)"
  done
  rm -rf "$WORK/d"
}
A_TUNED="-cache 256 -batch 2000 -pool 0"
a() { # phase dist syms ticks
  local ph=$1 di=$2 s=$3 t=$4
  [[ $s -le 500 ]] && run "A-prod" -mode A-prod -syms $s -ticks $t -phase $ph -dist $di
  run "A-cur" -mode A-cur -syms $s -ticks $t -phase $ph -dist $di
  run "A-tuned" -mode A-tuned -syms $s -ticks $t -phase $ph -dist $di $A_TUNED
}
b() {
  local ph=$1 di=$2 s=$3 t=$4
  for bt in 500 2000 5000; do
    local tt=$t; [[ $s -eq 2000 && $bt -eq 500 ]] && tt=400000 # budget: ~4k tick/s here, 1M ticks would take 4 min per run
    run "B-b$bt" -mode B -syms $s -ticks $tt -phase $ph -dist $di -batch $bt
  done
}
case ${1:-all} in
  a) for s in 5 100 500 2000; do t=$TICKS; [[ $s -eq 2000 ]] && t=100000; for ph in cold steady; do a $ph uniform $s $t; done; done
     for ph in cold steady; do run "A-tuned" -mode A-tuned -syms 2000 -ticks $TICKS -phase $ph $A_TUNED; done ;;
  b) for s in 5 100 500 2000; do for ph in cold steady; do b $ph uniform $s $TICKS; done; done
     run "B-b2000-cache64M" -mode B -syms 100 -ticks $TICKS -phase steady -batch 2000 -cache 65536 ;;
  skew) for s in 100 500; do a steady zipf $s $TICKS; b steady zipf $s $TICKS; done ;;
  c) for s in 5 50 100 200; do for ph in cold steady; do
       run "C-a" -mode C-a -syms $s -ticks $TICKS -phase $ph
       run "C-b" -mode C-b -syms $s -ticks $TICKS -phase $ph
     done; done
     for s in 100 200; do run "C-b-tuned" -mode C-b -syms $s -ticks $TICKS -phase steady -cache 256 -batch 2000
       run "C-router(discard)" -mode C-router -syms $s -ticks $TICKS; done
     for ph in steady; do run "C-a-zipf" -mode C-a -syms 100 -ticks $TICKS -phase $ph -dist zipf
       run "C-b-zipf" -mode C-b -syms 100 -ticks $TICKS -phase $ph -dist zipf; done
     REPS=1 run "C-a" -mode C-a -syms 300 -ticks $TICKS -phase steady
     REPS=1 run "C-b" -mode C-b -syms 300 -ticks $TICKS -phase steady ;;
  read) # same data (100 symbols, 5M ticks): A layout vs B layout, one symbol, warm
     rm -rf "$WORK/dsA" "$WORK/dsB"
     "$BIN" modes-run -mode A-tuned -syms 100 -ticks 5000000 -dir "$WORK/dsA" -keep $A_TUNED | head -1
     "$BIN" modes-run -mode B -syms 100 -ticks 5000000 -dir "$WORK/dsB" -keep -batch 2000 | head -1
     for i in 1 2 3; do for nm in "1000 200" "100 200"; do set -- $nm
       for l in A B; do d=$WORK/ds$l; echo "read-$l $("$BIN" modes-read -layout $l -dir "$d" -sym 7 -n $1 -m $2)"; done; done; done ;;
esac
