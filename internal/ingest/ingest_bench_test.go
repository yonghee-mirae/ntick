//go:build perf

// Perf measurements (Sprint 5). Run with: go test -tags perf -run TestPerf -v ./internal/ingest
// Env: PERF_DIR (data dir, required), PERF_SYMS, PERF_TICKS, PERF_BAD (fraction of price<=0 ticks),
// PERF_MODE=ingest|gen|churn. gen also reads PERF_SYMBOL, PERF_DAYS, PERF_PERDAY, PERF_START (YYYYMMDD).
package ingest

import (
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"syscall"
	"testing"
	"time"

	"ntick/internal/tick"
)

func envInt(k string, def int) int {
	if v, err := strconv.Atoi(os.Getenv(k)); err == nil {
		return v
	}
	return def
}

func envF(k string) float64 { v, _ := strconv.ParseFloat(os.Getenv(k), 64); return v }

func cpu() time.Duration {
	var ru syscall.Rusage
	syscall.Getrusage(syscall.RUSAGE_SELF, &ru)
	return time.Duration(ru.Utime.Nano() + ru.Stime.Nano())
}

func run(t *testing.T, label string, ticks []tick.Tick) {
	dir := os.Getenv("PERF_DIR")
	if dir == "" {
		t.Skip("PERF_DIR not set")
	}
	in := New(dir, seoul)
	c0, t0 := cpu(), time.Now()
	for _, tk := range ticks {
		in.Put(tk)
	}
	tPut := time.Since(t0)
	in.Close()
	el := time.Since(t0)
	fmt.Printf("PERF %s ticks=%d put_s=%.3f total_s=%.3f rate=%.0f/s cpu_s=%.2f cpu_pct=%.0f\n", label, len(ticks),
		tPut.Seconds(), el.Seconds(), float64(len(ticks))/el.Seconds(), (cpu() - c0).Seconds(), float64(cpu()-c0)/float64(el)*100)
}

// TestPerfIngest mimics mockfeed: uniform random symbols, ts += rand(200) ms, price random walk, qty 1..1000.
func TestPerfIngest(t *testing.T) {
	if os.Getenv("PERF_MODE") != "ingest" {
		t.Skip()
	}
	nsym, n, bad := envInt("PERF_SYMS", 5), envInt("PERF_TICKS", 1000000), envF("PERF_BAD")
	r := rand.New(rand.NewSource(1))
	px := make([]int64, nsym)
	for i := range px {
		px[i] = 10000 + int64(r.Intn(90000))
	}
	clock := time.Date(2026, 1, 5, 9, 0, 0, 0, seoul).UnixMilli()
	ticks := make([]tick.Tick, n)
	for i := range ticks {
		clock += int64(r.Intn(200))
		s := r.Intn(nsym)
		px[s] = max(1, px[s]+int64(r.Intn(201))-100)
		tk := tick.Tick{Symbol: "S" + strconv.Itoa(100000+s), TS: clock, Price: px[s], Qty: 1 + int64(r.Intn(1000))}
		if r.Float64() < bad {
			tk.Price = 0
		}
		ticks[i] = tk
	}
	run(t, fmt.Sprintf("ingest syms=%d bad=%.2f", nsym, bad), ticks)
}

// TestPerfGen builds a query dataset: one symbol, PERF_DAYS consecutive days, PERF_PERDAY ticks per day
// spread evenly over 6 h from 09:00 (qty 1..1000, price random walk, PERF_BAD fraction invalid).
func TestPerfGen(t *testing.T) {
	if os.Getenv("PERF_MODE") != "gen" {
		t.Skip()
	}
	sym := os.Getenv("PERF_SYMBOL")
	days, per, bad := envInt("PERF_DAYS", 30), envInt("PERF_PERDAY", 200000), envF("PERF_BAD")
	start, err := time.ParseInLocation("20060102", os.Getenv("PERF_START"), seoul)
	if err != nil {
		t.Fatal(err)
	}
	r := rand.New(rand.NewSource(2))
	px := int64(50000)
	var ticks []tick.Tick
	for d := 0; d < days; d++ {
		base := start.AddDate(0, 0, d).Add(9 * time.Hour).UnixMilli()
		for i := 0; i < per; i++ {
			px = max(1, px+int64(r.Intn(21))-10)
			tk := tick.Tick{Symbol: sym, TS: base + int64(i)*21600000/int64(per), Price: px, Qty: 1 + int64(r.Intn(1000))}
			if r.Float64() < bad {
				tk.Price = 0
			}
			ticks = append(ticks, tk)
		}
	}
	run(t, fmt.Sprintf("gen %s days=%d perday=%d bad=%.2f", sym, days, per, bad), ticks)
}

// TestPerfChurn makes every tick open a new day file: tick i goes to symbol i%PERF_SYMS on day i/PERF_SYMS.
func TestPerfChurn(t *testing.T) {
	if os.Getenv("PERF_MODE") != "churn" {
		t.Skip()
	}
	nsym, n := envInt("PERF_SYMS", 100), envInt("PERF_TICKS", 20000)
	base := time.Date(2030, 1, 1, 9, 0, 0, 0, seoul).UnixMilli()
	ticks := make([]tick.Tick, n)
	for i := range ticks {
		ticks[i] = tick.Tick{Symbol: "S" + strconv.Itoa(100000+i%nsym), TS: base + int64(i/nsym)*86400000, Price: 100, Qty: 1}
	}
	run(t, fmt.Sprintf("churn syms=%d (every tick = new file)", nsym), ticks)
}
