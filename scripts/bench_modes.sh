#!/usr/bin/env bash
# Write-path comparison of three storage layouts (docs/perf-storage-modes.md).
# Usage: scripts/bench_modes.sh MATRIX   (matrix = a | b | c | skew | ready | paced | sat | read | all)
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
  ready) # hash-fixed vs ready-queue dispatch (docs/perf-ready-queue.md)
     for di in uniform zipf; do for s in 100 500; do
       run "A-tuned-$di-$s" -mode A-tuned -syms $s -ticks $TICKS -phase steady -dist $di $A_TUNED
       run "A-ready-$di-$s" -mode A-ready -syms $s -ticks $TICKS -phase steady -dist $di $A_TUNED
       run "A-ready-mb200-$di-$s" -mode A-ready -minbatch 200 -syms $s -ticks $TICKS -phase steady -dist $di $A_TUNED
     done; done ;;
  paced) # paced input (ticks/s), Zipf: dispatch policy vs CPU, commit size and end-to-end latency
     for cell in "100 110000" "100 225000" "100 360000" "500 50000" "500 100000" "500 170000"; do set -- $cell
       s=$1 r=$2; t=$((r * 6)); c="-syms $s -ticks $t -rate $r -phase steady -dist zipf $A_TUNED"
       run "H-f50-$s-$r" -mode A-tuned $c -flush 50
       run "H-f10-$s-$r" -mode A-tuned $c -flush 10
       run "R1-$s-$r" -mode A-ready $c -minbatch 1
       run "R200-T10-$s-$r" -mode A-ready $c -minbatch 200 -flush 10
       run "R200-T50-$s-$r" -mode A-ready $c -minbatch 200 -flush 50
       run "R50-T10-$s-$r" -mode A-ready $c -minbatch 50 -flush 10
     done ;;
  sat) # saturation anomaly: uniform 100, minbatch/sweep interval
     for i in 1 2; do
       run "sat-R1" -mode A-ready -syms 100 -ticks $TICKS -phase steady -dist uniform $A_TUNED -minbatch 1
       for f in 10 20 50; do run "sat-R200-T$f" -mode A-ready -syms 100 -ticks $TICKS -phase steady -dist uniform $A_TUNED -minbatch 200 -flush $f; done
     done ;;
  read) # same data (100 symbols, 5M ticks): A layout vs B layout, one symbol, warm
     rm -rf "$WORK/dsA" "$WORK/dsB"
     "$BIN" modes-run -mode A-tuned -syms 100 -ticks 5000000 -dir "$WORK/dsA" -keep $A_TUNED | head -1
     "$BIN" modes-run -mode B -syms 100 -ticks 5000000 -dir "$WORK/dsB" -keep -batch 2000 | head -1
     for i in 1 2 3; do for nm in "1000 200" "100 200"; do set -- $nm
       for l in A B; do d=$WORK/ds$l; echo "read-$l $("$BIN" modes-read -layout $l -dir "$d" -sym 7 -n $1 -m $2)"; done; done; done ;;
esac
