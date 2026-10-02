//go:build perf

// Commit -> WS receipt latency (Sprint 5). Run: go test -tags perf -run TestPerfLatency -v ./internal/stream
// Env: PERF_DIR (empty data dir), PERF_RATES (default 1000,10000,100000), PERF_SECS (default 10).
// One symbol, one subscriber (time 1d candle, so candle.tick_count == day valid count T of the commit event).
package stream

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"

	"ntick/internal/ingest"
	"ntick/internal/tick"
)

func TestPerfLatency(t *testing.T) {
	root := os.Getenv("PERF_DIR")
	if root == "" {
		t.Skip("PERF_DIR not set")
	}
	loc, _ := time.LoadLocation("Asia/Seoul")
	secs := 10
	if v, err := strconv.Atoi(os.Getenv("PERF_SECS")); err == nil {
		secs = v
	}
	rates := os.Getenv("PERF_RATES")
	if rates == "" {
		rates = "1000,10000,100000"
	}
	for ri, rs := range strings.Split(rates, ",") {
		rate, _ := strconv.Atoi(rs)
		total := rate * secs
		dir := fmt.Sprintf("%s/lat%d", root, ri)
		commitAt := make([]int64, total+2) // index = T of the commit event, unix ns
		b := NewBroker()
		in := ingest.New(dir, loc)
		in.OnCommit(func(ev ingest.Event) {
			if int(ev.T) < len(commitAt) {
				atomic.StoreInt64(&commitAt[ev.T], time.Now().UnixNano())
			}
			b.Publish(ev)
		})
		srv := httptest.NewServer(Handler(dir, b, loc))
		ctx, cancel := context.WithCancel(context.Background())
		c, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(srv.URL, "http")+"/candles/stream?symbol=LAT&type=time&interval=1d", nil)
		if err != nil {
			t.Fatal(err)
		}
		var lat []time.Duration
		var events int
		done := make(chan struct{})
		go func() {
			defer close(done)
			for {
				_, msg, err := c.Read(ctx)
				if err != nil {
					return
				}
				recv := time.Now().UnixNano()
				var m struct {
					Type   string
					Candle *struct {
						Tick_count int
					}
				}
				if json.Unmarshal(msg, &m) != nil || m.Type != "update" || m.Candle == nil {
					continue
				}
				if at := atomic.LoadInt64(&commitAt[m.Candle.Tick_count]); at != 0 {
					lat = append(lat, time.Duration(recv-at))
					events++
				}
			}
		}()
		base := time.Date(2026, 9, 1, 9, 0, 0, 0, loc).UnixMilli()
		start := time.Now()
		sent := 0
		for sent < total {
			target := min(total, int(float64(time.Since(start))/float64(time.Second)*float64(rate)))
			for ; sent < target; sent++ {
				in.Put(tick.Tick{Symbol: "LAT", TS: base + int64(sent/100), Price: 1000, Qty: 1})
			}
			time.Sleep(500 * time.Microsecond)
		}
		sendSecs := time.Since(start).Seconds()
		in.Close()
		time.Sleep(300 * time.Millisecond)
		cancel()
		<-done
		srv.Close()
		b.Close()
		sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
		p := func(q float64) float64 { return float64(lat[min(len(lat)-1, int(float64(len(lat))*q))]) / 1e6 }
		fmt.Printf("PERF latency rate=%d/s ticks=%d send_s=%.2f events_seen=%d (ticks per event %.0f) commit->recv p50=%.3fms p95=%.3fms p99=%.3fms max=%.3fms\n",
			rate, total, sendSecs, len(lat), float64(total)/float64(len(lat)), p(.5), p(.95), p(.99), p(1))
	}
}
