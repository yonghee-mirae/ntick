package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"

	"ntick/internal/oracle"
)

// verifySubsequence checks that every stored ticks table is an ordered subsequence of the truth
// rows of its (symbol, day): rows lost in flight are fine, duplicated, altered or reordered rows are not.
func verifySubsequence(dir, truthPath, tz string, maxN int) (bool, error) {
	loc, err := time.LoadLocation(tz)
	if err != nil {
		return false, err
	}
	f, err := os.Open(truthPath)
	if err != nil {
		return false, err
	}
	defer f.Close()
	rows, err := oracle.ParseTruth(f)
	if err != nil {
		return false, err
	}
	want := oracle.Expected(rows, loc)
	fm, err := files(dir)
	if err != nil {
		return false, err
	}
	mm := &mismatches{max: maxN}
	var stored, truthTotal int
	for _, d := range want {
		truthTotal += len(d.Ticks)
	}
	for _, k := range sortedKeys(fm) {
		d, ok := want[k]
		if !ok {
			mm.add("%s/%s: file has no truth rows", k.Date, k.Symbol)
			continue
		}
		n, err := subsequence(fm[k], k, d, mm)
		if err != nil {
			return false, err
		}
		stored += n
	}
	fmt.Printf("verify-subsequence: %d files, stored %d of %d truth rows (lost in flight: %d), %d mismatches\n",
		len(fm), stored, truthTotal, truthTotal-stored, mm.total)
	return mm.total == 0, nil
}

func subsequence(path string, k oracle.Key, d *oracle.Day, mm *mismatches) (int, error) {
	db, err := open(path)
	if err != nil {
		return 0, err
	}
	defer db.Close()
	rs, err := db.Query(`SELECT raw_seq, ts, price, qty, valid FROM ticks ORDER BY raw_seq`)
	if err != nil {
		return 0, err
	}
	defer rs.Close()
	pos, n := 0, 0
	for rs.Next() {
		var seq, ts, p, q, v int64
		if err := rs.Scan(&seq, &ts, &p, &q, &v); err != nil {
			return n, err
		}
		n++
		got := oracle.Tick{TS: ts, Price: p, Qty: q, Valid: v == 1}
		for pos < len(d.Ticks) && d.Ticks[pos] != got {
			pos++
		}
		if pos == len(d.Ticks) {
			mm.add("%s/%s: stored row raw_seq=%d %+v is not an ordered continuation of the truth", k.Date, k.Symbol, seq, got)
			return n, rs.Err()
		}
		pos++
	}
	return n, rs.Err()
}

// stressRead hammers REST (and a few WS subscribers) for the given duration and checks response invariants.
func stressRead(base, symbols string, dur time.Duration, clients, wsClients int) (bool, error) {
	syms := strings.Split(symbols, ",")
	var reqs, c5xx, c4xx, bad, wsMsgs atomic.Int64
	var mu sync.Mutex
	var firstBad []string
	violate := func(format string, a ...any) {
		bad.Add(1)
		mu.Lock()
		if len(firstBad) < 10 {
			firstBad = append(firstBad, fmt.Sprintf(format, a...))
		}
		mu.Unlock()
	}
	ctx, cancel := context.WithTimeout(context.Background(), dur)
	defer cancel()
	hc := &http.Client{Timeout: 30 * time.Second}
	get := func(q url.Values) ([]byte, bool) {
		reqs.Add(1)
		resp, err := hc.Get(base + "/candles?" + q.Encode())
		if err != nil {
			if ctx.Err() == nil {
				violate("request error: %v", err)
			}
			return nil, false
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		switch {
		case resp.StatusCode >= 500:
			c5xx.Add(1)
			violate("HTTP %d for %s: %s", resp.StatusCode, q.Encode(), body)
		case resp.StatusCode != 200:
			c4xx.Add(1)
			violate("HTTP %d for %s: %s", resp.StatusCode, q.Encode(), body)
		default:
			return body, true
		}
		return nil, false
	}
	var wg sync.WaitGroup
	for i := 0; i < clients; i++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			r := rand.New(rand.NewSource(seed))
			for ctx.Err() == nil {
				sym := syms[r.Intn(len(syms))]
				if r.Intn(2) == 0 {
					n := []int{1, 7, 50, 500}[r.Intn(4)]
					m := []int{1, 5, 50, 200}[r.Intn(4)]
					body, ok := get(url.Values{"symbol": {sym}, "type": {"tick"}, "n": {fmt.Sprint(n)}, "m": {fmt.Sprint(m)}})
					if ok {
						checkTickBody(body, sym, n, violate)
					}
				} else {
					iv := []string{"1m", "5m", "1h"}[r.Intn(3)]
					now := time.Now().UnixMilli()
					body, ok := get(url.Values{"symbol": {sym}, "type": {"time"}, "interval": {iv},
						"from": {fmt.Sprint(now - 72*3600000)}, "to": {fmt.Sprint(now + 48*3600000)}})
					if ok {
						checkTimeBody(body, sym, iv, violate)
					}
				}
			}
		}(int64(i))
	}
	wsURL := "ws" + strings.TrimPrefix(base, "http")
	for i := 0; i < wsClients; i++ {
		wg.Add(1)
		go func(sym string) {
			defer wg.Done()
			c, _, err := websocket.Dial(ctx, wsURL+"/candles/stream?symbol="+sym+"&type=tick&n=7", nil)
			if err != nil {
				violate("ws dial %s: %v", sym, err)
				return
			}
			defer c.CloseNow()
			lastDate, lastIdx := "", int64(-1)
			for {
				_, data, err := c.Read(ctx)
				if err != nil {
					if ctx.Err() == nil {
						violate("ws %s ended: %v", sym, err)
					}
					return
				}
				wsMsgs.Add(1)
				var m struct {
					Type   string      `json:"type"`
					Candle *streamTick `json:"candle"`
				}
				if json.Unmarshal(data, &m) != nil || m.Type != "complete" || m.Candle == nil {
					continue
				}
				c := m.Candle
				switch {
				case lastIdx < 0: // first complete after the snapshot
				case c.Date == lastDate && c.Index == lastIdx+1:
				case c.Date > lastDate && c.Index == 0:
				default:
					violate("ws %s: complete %s/%d after %s/%d (gap or duplicate)", sym, c.Date, c.Index, lastDate, lastIdx)
				}
				lastDate, lastIdx = c.Date, c.Index
			}
		}(syms[i%len(syms)])
	}
	wg.Wait()
	fmt.Printf("stress-read: %d requests, %d 5xx, %d other non-200, %d ws messages, %d violations\n",
		reqs.Load(), c5xx.Load(), c4xx.Load(), wsMsgs.Load(), bad.Load())
	for _, s := range firstBad {
		fmt.Println("VIOLATION", s)
	}
	return bad.Load() == 0 && reqs.Load() > 0, nil
}

func checkTickBody(body []byte, sym string, n int, violate func(string, ...any)) {
	var r struct {
		Candles []tickC `json:"candles"`
	}
	if err := json.Unmarshal(body, &r); err != nil {
		violate("tick %s: bad json: %v", sym, err)
		return
	}
	lastDate := ""
	for i, c := range r.Candles {
		newestOfDay := i == 0 || r.Candles[i-1].Date != c.Date
		if i > 0 && c.Date > r.Candles[i-1].Date {
			violate("tick %s n=%d: dates not newest-first at #%d", sym, n, i)
		}
		if newestOfDay && c.Date == lastDate {
			violate("tick %s n=%d: date %s appears in two groups", sym, n, c.Date)
		}
		lastDate = c.Date
		if !newestOfDay && c.TickCount != n {
			violate("tick %s n=%d: inner candle #%d has tick_count %d", sym, n, i, c.TickCount)
		}
		if c.TickCount < 1 || c.TickCount > n || c.Partial != (c.TickCount < n) {
			violate("tick %s n=%d: candle #%d tick_count=%d partial=%v", sym, n, i, c.TickCount, c.Partial)
		}
	}
}

func checkTimeBody(body []byte, sym, iv string, violate func(string, ...any)) {
	var r struct {
		Candles []timeC `json:"candles"`
	}
	if err := json.Unmarshal(body, &r); err != nil {
		violate("time %s: bad json: %v", sym, err)
		return
	}
	for i := 1; i < len(r.Candles); i++ {
		if r.Candles[i].Start <= r.Candles[i-1].Start {
			violate("time %s %s: starts not strictly ascending at #%d", sym, iv, i)
			return
		}
	}
}

// countTicks prints the total number of stored tick rows (valid and invalid) under dir.
func countTicks(dir string) (int64, error) {
	fm, err := files(dir)
	if err != nil {
		return 0, err
	}
	var total int64
	for _, p := range fm {
		db, err := open(p)
		if err != nil {
			return 0, err
		}
		var n int64
		err = db.QueryRow(`SELECT count(*) FROM ticks`).Scan(&n)
		db.Close()
		if err != nil {
			return 0, err
		}
		total += n
	}
	return total, nil
}
