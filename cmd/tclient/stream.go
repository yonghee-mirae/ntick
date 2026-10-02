package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"

	"ntick/internal/oracle"
)

// streamTick is a stream tick candle: the REST fields plus index.
type streamTick struct {
	Date      string `json:"date"`
	Index     int64  `json:"index"`
	Open      int64  `json:"open"`
	High      int64  `json:"high"`
	Low       int64  `json:"low"`
	Close     int64  `json:"close"`
	Volume    int64  `json:"volume"`
	TickCount int    `json:"tick_count"`
	Partial   bool   `json:"partial"`
}

type wsMsg struct {
	Type   string          `json:"type"`
	Start  int64           `json:"start"`
	Candle json.RawMessage `json:"candle"`
}

// subRes is everything one subscription received.
type subRes struct {
	name    string // symbol and parameters
	isTime  bool
	n, iv   int // tick n or interval minutes
	mid     bool
	snapT   *streamTick
	snapM   *timeC
	curT    *streamTick // in-progress candle after the last message
	curM    *timeC
	doneT   []streamTick
	doneM   []timeC
	invalid map[int64]bool
	errs    []string
}

func (r *subRes) errf(format string, a ...any) { r.errs = append(r.errs, fmt.Sprintf(format, a...)) }

// runSub subscribes, reads the snapshot (so the subscription is live when it returns) and then
// consumes messages in the background until the server closes; done is called at the end.
func runSub(ctx context.Context, url string, r *subRes, done func()) error {
	c, _, err := websocket.Dial(ctx, url, nil)
	if err != nil {
		return fmt.Errorf("%s: dial: %w", r.name, err)
	}
	r.invalid = map[int64]bool{}
	first := true
	read := func() (bool, error) {
		_, data, err := c.Read(ctx)
		if err != nil {
			return false, err
		}
		var m wsMsg
		if err := json.Unmarshal(data, &m); err != nil {
			return false, fmt.Errorf("bad json %q: %w", data, err)
		}
		if first != (m.Type == "snapshot") {
			r.errf("message type %q at wrong position (first=%v)", m.Type, first)
		}
		first = false
		r.handle(m)
		return true, nil
	}
	if _, err := read(); err != nil {
		c.CloseNow()
		return fmt.Errorf("%s: first message: %w", r.name, err)
	}
	go func() {
		defer done()
		defer c.CloseNow()
		for {
			if _, err := read(); err != nil {
				if websocket.CloseStatus(err) != websocket.StatusTryAgainLater || !strings.Contains(err.Error(), "shutting down") {
					r.errf("connection ended unexpectedly: %v", err)
				}
				return
			}
		}
	}()
	return nil
}

func (r *subRes) handle(m wsMsg) {
	null := string(m.Candle) == "null" || len(m.Candle) == 0
	if m.Type == "invalidate" {
		if !r.isTime {
			r.errf("invalidate on a tick stream")
		}
		r.invalid[m.Start] = true
		return
	}
	var t *streamTick
	var tm *timeC
	if !null {
		var err error
		if r.isTime {
			tm = new(timeC)
			err = json.Unmarshal(m.Candle, tm)
		} else {
			t = new(streamTick)
			err = json.Unmarshal(m.Candle, t)
		}
		if err != nil {
			r.errf("bad candle %s: %v", m.Candle, err)
			return
		}
	}
	switch m.Type {
	case "snapshot":
		r.snapT, r.snapM, r.curT, r.curM = t, tm, t, tm
	case "update":
		if null {
			r.errf("update without candle")
		}
		r.curT, r.curM = t, tm
	case "complete":
		if null {
			r.errf("complete without candle")
			return
		}
		if r.isTime {
			r.doneM = append(r.doneM, *tm)
		} else {
			r.doneT = append(r.doneT, *t)
		}
		r.curT, r.curM = nil, nil
	default:
		r.errf("unknown message type %q", m.Type)
	}
}

// verifyStream subscribes to every symbol before the feed starts and once more mid-stream, waits for
// the server to close the streams, and compares what was received with the oracle.
func verifyStream(truthPath, tz, base, symbols, ready string, midDelay, timeout time.Duration, maxN int) (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	var subs []*subRes
	var wg sync.WaitGroup
	var mu sync.Mutex
	var setupErrs []error
	subscribe := func(mid bool) {
		var rs []*subRes
		for _, sym := range strings.Split(symbols, ",") {
			for _, n := range []int{1, 7, 100} {
				rs = append(rs, &subRes{name: fmt.Sprintf("%s tick n=%d mid=%v", sym, n, mid), n: n, mid: mid})
				rs[len(rs)-1].name += "|" + sym
			}
			for _, iv := range []string{"1m", "5m", "1h"} {
				m, _ := parseMinutes(iv)
				rs = append(rs, &subRes{name: fmt.Sprintf("%s time %s mid=%v", sym, iv, mid), isTime: true, iv: m, mid: mid})
				rs[len(rs)-1].name += "|" + sym + "|" + iv
			}
		}
		for _, r := range rs {
			parts := strings.Split(r.name, "|")
			url := base + "/candles/stream?symbol=" + parts[1]
			if r.isTime {
				url += "&type=time&interval=" + parts[2]
			} else {
				url += fmt.Sprintf("&type=tick&n=%d", r.n)
			}
			r.name = parts[0]
			wg.Add(1)
			if err := runSub(ctx, url, r, wg.Done); err != nil {
				wg.Done()
				mu.Lock()
				setupErrs = append(setupErrs, err)
				mu.Unlock()
			}
		}
		mu.Lock()
		subs = append(subs, rs...)
		mu.Unlock()
	}
	subscribe(false)
	if len(setupErrs) > 0 {
		return false, setupErrs[0]
	}
	if ready != "" {
		if err := os.WriteFile(ready, nil, 0o644); err != nil {
			return false, err
		}
	}
	midDone := make(chan struct{})
	go func() {
		defer close(midDone)
		select {
		case <-time.After(midDelay):
			subscribe(true)
		case <-ctx.Done():
		}
	}()
	wg.Wait()
	<-midDone
	// A mid-stream subscriber may start before wg.Wait sees it; wait again for late registrations.
	wg.Wait()
	if len(setupErrs) > 0 {
		return false, setupErrs[0]
	}
	if ctx.Err() != nil {
		return false, fmt.Errorf("timeout waiting for the streams to end")
	}

	syms, loc, err := loadSymbols(truthPath, tz)
	if err != nil {
		return false, err
	}
	bySym := map[string]*symData{}
	for _, s := range syms {
		bySym[s.name] = s
	}
	mm := &mismatches{max: maxN}
	nMid := 0
	for _, r := range subs {
		sym := strings.Fields(r.name)[0]
		s := bySym[sym]
		if s == nil {
			mm.add("%s: symbol absent from truth", r.name)
			continue
		}
		for _, e := range r.errs {
			mm.add("%s: %s", r.name, e)
		}
		if r.isTime {
			checkTimeStream(mm, r, s, loc)
		} else {
			checkTickStream(mm, r, s)
		}
		if r.mid && (r.snapT != nil || r.snapM != nil) {
			nMid++
		}
	}
	// Mid-stream subscribers that joined before any data would not test the snapshot path.
	if nMid == 0 {
		mm.add("no mid-stream subscriber received a non-empty snapshot (joined too early/late?)")
	}
	fmt.Printf("verify-stream: %d subscriptions (%d mid-stream with snapshot), %d mismatches\n", len(subs), nMid, mm.total)
	return mm.total == 0, nil
}

func parseMinutes(iv string) (int, error) {
	var v int
	var u byte
	if _, err := fmt.Sscanf(iv, "%d%c", &v, &u); err != nil {
		return 0, err
	}
	if u == 'h' {
		v *= 60
	}
	return v, nil
}

// expectedTick lists the complete messages a full-history subscriber should see (oldest first)
// and the in-progress candle left at the end (nil if the last candle is full).
func expectedTick(s *symData, n int) (done []streamTick, cur *streamTick) {
	for i, day := range s.perDay {
		cs := oracle.NTick([][]oracle.Tick{day}, n, math.MaxInt32)
		slices.Reverse(cs)
		for j, c := range cs {
			t := streamTick{s.dates[i], int64(j), c.Open, c.High, c.Low, c.Close, c.Volume, int(c.Count), c.Count < int64(n)}
			if i == len(s.perDay)-1 && j == len(cs)-1 && c.Count < int64(n) {
				cur = &t
				break
			}
			done = append(done, t)
		}
	}
	return done, cur
}

func checkTickStream(mm *mismatches, r *subRes, s *symData) {
	done, cur := expectedTick(s, r.n)
	got := r.doneT
	if r.mid {
		p := 0
		if len(got) > 0 {
			p = slices.IndexFunc(done, func(t streamTick) bool { return t.Date == got[0].Date && t.Index == got[0].Index })
			if p < 0 {
				mm.add("%s: first complete %+v is not an expected candle", r.name, got[0])
				return
			}
			if r.snapT != nil && (r.snapT.Date != got[0].Date || r.snapT.Index != got[0].Index ||
				r.snapT.Open != got[0].Open || r.snapT.TickCount > got[0].TickCount) {
				mm.add("%s: snapshot %+v does not lead into first complete %+v", r.name, *r.snapT, got[0])
			}
		}
		done = done[p:]
	}
	if !slices.Equal(got, done) {
		for i := 0; i < max(len(got), len(done)); i++ {
			if i >= len(got) || i >= len(done) || got[i] != done[i] {
				mm.add("%s: complete #%d: got %d completes want %d; first diff got=%v want=%v", r.name, i, len(got), len(done), at(got, i), at(done, i))
				break
			}
		}
	}
	if (r.curT == nil) != (cur == nil) || (cur != nil && *r.curT != *cur) {
		mm.add("%s: final in-progress candle got %v want %v", r.name, ptr(r.curT), ptr(cur))
	}
}

func at[T any](s []T, i int) any {
	if i < len(s) {
		return s[i]
	}
	return nil
}

func ptr[T any](p *T) any {
	if p == nil {
		return nil
	}
	return *p
}

func checkTimeStream(mm *mismatches, r *subRes, s *symData, loc *time.Location) {
	all := wantTime(s, loc, fmt.Sprintf("%dm", r.iv), rng{from: math.MinInt64, to: math.MaxInt64})
	last := all[len(all)-1]
	last.Partial = true // the stream always flags the in-progress candle as partial
	low := int64(math.MinInt64)
	if r.mid && r.snapM != nil {
		low = r.snapM.Start
	}
	exp := map[int64]timeC{}
	for _, c := range all[:len(all)-1] {
		if c.Start >= low && !r.invalid[c.Start] {
			exp[c.Start] = c
		}
	}
	prev := int64(math.MinInt64)
	seen := map[int64]bool{}
	for _, c := range r.doneM {
		if c.Start <= prev {
			mm.add("%s: complete starts not ascending at %d", r.name, c.Start)
		}
		prev = c.Start
		if c.Start < low {
			mm.add("%s: complete %d before snapshot start %d", r.name, c.Start, low)
		}
		if r.invalid[c.Start] {
			continue // client would re-query this bucket; DB state is covered by verify-time
		}
		seen[c.Start] = true
		if e, ok := exp[c.Start]; !ok {
			mm.add("%s: unexpected complete %+v", r.name, c)
		} else if c != e {
			mm.add("%s: complete bucket %d: got %+v want %+v", r.name, c.Start, c, e)
		}
	}
	for st := range exp {
		if !seen[st] {
			mm.add("%s: missing complete for bucket %d (no invalidate seen)", r.name, st)
		}
	}
	if r.curM == nil || *r.curM != last {
		mm.add("%s: final in-progress candle got %v want %+v", r.name, ptr(r.curM), last)
	}
}
