//go:build perf

// Perf measurements (Sprint 5). Run with: go test -tags perf -run TestPerf -v ./internal/query
// Env: PERF_DIR (data dir), PERF_SYMBOL, PERF_ITERS (default 30), PERF_COLD=1 (fadvise DONTNEED the day files
// before every call, so the page cache is cold for them), PERF_START (YYYYMMDD of the oldest day, time test).
package query

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

func cpuNow() time.Duration {
	var ru syscall.Rusage
	syscall.Getrusage(syscall.RUSAGE_SELF, &ru)
	return time.Duration(ru.Utime.Nano() + ru.Stime.Nano())
}

func hwmKB() string {
	b, _ := os.ReadFile("/proc/self/status")
	for _, l := range strings.Split(string(b), "\n") {
		if strings.HasPrefix(l, "VmHWM") {
			return strings.Fields(l)[1]
		}
	}
	return "?"
}

// drop evicts the clean page cache pages of every day file (and its -wal/-shm) of the symbol.
func drop(dir, sym string) {
	files, _ := filepath.Glob(filepath.Join(dir, "*", sym+".db*"))
	for _, f := range files {
		if fd, err := syscall.Open(f, syscall.O_RDONLY, 0); err == nil {
			syscall.Syscall6(syscall.SYS_FADVISE64, uintptr(fd), 0, 0, 4, 0, 0)
			syscall.Close(fd)
		}
	}
}

func stats(label string, d []time.Duration, extra string) {
	sort.Slice(d, func(i, j int) bool { return d[i] < d[j] })
	p := func(q float64) float64 { return float64(d[min(len(d)-1, int(float64(len(d))*q))]) / 1e6 }
	fmt.Printf("PERF %s iters=%d min=%.1fms p50=%.1fms p95=%.1fms p99=%.1fms max=%.1fms %s\n", label, len(d), p(0), p(.5), p(.95), p(.99), p(1), extra)
}

func TestPerfNTick(t *testing.T) {
	dir, sym := os.Getenv("PERF_DIR"), os.Getenv("PERF_SYMBOL")
	if dir == "" || sym == "" {
		t.Skip("PERF_DIR/PERF_SYMBOL not set")
	}
	iters, cold := 30, os.Getenv("PERF_COLD") == "1"
	if v, err := strconv.Atoi(os.Getenv("PERF_ITERS")); err == nil {
		iters = v
	}
	cells := [][2]int{{100, 200}, {1000, 200}, {1000, 1000}, {10000, 100}, {10000, 1000}}
	if v := os.Getenv("PERF_CELLS"); v != "" { // "n:m,n:m"
		cells = nil
		for _, c := range strings.Split(v, ",") {
			var n, m int
			fmt.Sscanf(c, "%d:%d", &n, &m)
			cells = append(cells, [2]int{n, m})
		}
	}
	for _, c := range cells {
		n, m := c[0], c[1]
		if !cold {
			NTick(context.Background(), dir, sym, n, m) // warm page cache
		}
		var ds []time.Duration
		r0, c0 := rowsRead.Load(), cpuNow()
		var got int
		for i := 0; i < iters; i++ {
			if cold {
				drop(dir, sym)
			}
			t0 := time.Now()
			cs, err := NTick(context.Background(), dir, sym, n, m)
			ds = append(ds, time.Since(t0))
			if err != nil {
				t.Fatal(err)
			}
			got = len(cs)
		}
		rows := (rowsRead.Load() - r0) / int64(iters)
		stats(fmt.Sprintf("ntick n=%d m=%d cold=%v", n, m, cold), ds,
			fmt.Sprintf("candles=%d rows=%d cpu_ms_per_call=%.1f ns_per_row=%.0f hwm_kb=%s", got, rows, float64(cpuNow()-c0)/1e6/float64(iters),
				float64(ds[len(ds)/2])/float64(max(rows, 1)), hwmKB()))
	}
}

func TestPerfTime(t *testing.T) {
	dir, sym := os.Getenv("PERF_DIR"), os.Getenv("PERF_SYMBOL")
	start, err := time.ParseInLocation("20060102", os.Getenv("PERF_START"), time.FixedZone("KST", 9*3600))
	if dir == "" || sym == "" || err != nil {
		t.Skip("PERF_DIR/PERF_SYMBOL/PERF_START not set")
	}
	loc := start.Location()
	iters := 30
	for _, span := range []int{1, 30} {
		for _, iv := range []string{"1m", "5m", "1h", "1d"} {
			imin, _ := ParseInterval(iv)
			// newest `span` days of the dataset: the dataset has 30 days starting at `start`
			from := start.AddDate(0, 0, 30-span).UnixMilli()
			to := start.AddDate(0, 0, 30).UnixMilli()
			Time(context.Background(), dir, sym, loc, imin, from, to)
			var ds []time.Duration
			c0 := cpuNow()
			var got int
			for i := 0; i < iters; i++ {
				t0 := time.Now()
				cs, err := Time(context.Background(), dir, sym, loc, imin, from, to)
				ds = append(ds, time.Since(t0))
				if err != nil {
					t.Fatal(err)
				}
				got = len(cs)
			}
			stats(fmt.Sprintf("time iv=%s days=%d", iv, span), ds, fmt.Sprintf("candles=%d cpu_ms_per_call=%.1f", got, float64(cpuNow()-c0)/1e6/float64(iters)))
		}
	}
}

// TestPerfScale: G goroutines call NTick(n, m) in a loop for PERF_SECS seconds; reports calls/s and CPU.
func TestPerfScale(t *testing.T) {
	dir, sym := os.Getenv("PERF_DIR"), os.Getenv("PERF_SYMBOL")
	if dir == "" || sym == "" {
		t.Skip("PERF_DIR/PERF_SYMBOL not set")
	}
	n, m, secs := 100, 200, 5
	if v, err := strconv.Atoi(os.Getenv("PERF_N")); err == nil {
		n = v
	}
	if v, err := strconv.Atoi(os.Getenv("PERF_M")); err == nil {
		m = v
	}
	gs := []int{1, 2, 4, 8, 12, 32}
	if v, err := strconv.Atoi(os.Getenv("PERF_G")); err == nil {
		gs = []int{v}
	}
	for _, g := range gs {
		var calls atomic.Int64
		stop := time.Now().Add(time.Duration(secs) * time.Second)
		c0 := cpuNow()
		var wg sync.WaitGroup
		for i := 0; i < g; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for time.Now().Before(stop) {
					if _, err := NTick(context.Background(), dir, sym, n, m); err != nil {
						t.Error(err)
						return
					}
					calls.Add(1)
				}
			}()
		}
		wg.Wait()
		fmt.Printf("PERF scale n=%d m=%d goroutines=%d calls/s=%.1f cpu_pct=%.0f\n", n, m, g, float64(calls.Load())/float64(secs), float64(cpuNow()-c0)/float64(secs)/1e7)
	}
}
