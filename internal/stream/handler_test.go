package stream

import (
	"context"
	"encoding/json"
	"math"
	"math/rand"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"

	"ntick/internal/ingest"
	"ntick/internal/oracle"
	"ntick/internal/tick"
)

func TestBrokerSlowSubscriber(t *testing.T) {
	b := NewBroker()
	slow, fast := b.Subscribe("A"), b.Subscribe("A")
	other := b.Subscribe("B")
	done := make(chan struct{})
	go func() { // fast drains; the publisher must never wait for slow
		for range fast.C {
		}
		b.Unsubscribe(fast) // a handler does this when its channel closes
		close(done)
	}()
	finished := make(chan struct{})
	go func() {
		for i := 0; i < subBuffer*3; i++ {
			b.Publish(ingest.Event{Symbol: "A", T: int64(i)})
			time.Sleep(10 * time.Microsecond)
		}
		close(finished)
	}()
	select {
	case <-finished:
	case <-time.After(5 * time.Second):
		t.Fatal("Publish blocked on a slow subscriber")
	}
	n := 0
	for range slow.C { // buffered events drain, then the channel is closed
		n++
	}
	if n != subBuffer || !strings.Contains(slow.Reason(), "slow subscriber") {
		t.Fatalf("slow got %d events, reason %q", n, slow.Reason())
	}
	if len(other.C) != 0 {
		t.Fatal("event leaked to another symbol")
	}
	b.Unsubscribe(slow)
	b.Unsubscribe(other)
	b.Close() // drops fast, waits for its release
	<-done
	if !strings.Contains(fast.Reason(), "shutting down") {
		t.Fatalf("fast reason %q", fast.Reason())
	}
}

func TestBadParams(t *testing.T) {
	srv := httptest.NewServer(Handler(t.TempDir(), NewBroker(), seoul))
	defer srv.Close()
	base := "ws" + strings.TrimPrefix(srv.URL, "http") + "/candles/stream?"
	for _, q := range []string{
		"", "symbol=A", "type=tick&n=5", "symbol=../x&type=tick&n=5", "symbol=A&type=bogus",
		"symbol=A&type=tick", "symbol=A&type=tick&n=0", "symbol=A&type=tick&n=10001", "symbol=A&type=tick&n=x",
		"symbol=A&type=time", "symbol=A&type=time&interval=0m", "symbol=A&type=time&interval=2d", "symbol=A&type=time&interval=5x",
	} {
		_, resp, err := websocket.Dial(context.Background(), base+q, nil)
		if err == nil || resp == nil || resp.StatusCode != 400 {
			t.Errorf("%q: want 400 before upgrade, got resp=%v err=%v", q, resp, err)
		}
	}
	for _, q := range []string{"symbol=A&type=tick&n=10000", "symbol=A&type=time&interval=1d"} {
		c, _, err := websocket.Dial(context.Background(), base+q, nil)
		if err != nil {
			t.Errorf("%q: %v", q, err)
			continue
		}
		_, data, err := c.Read(context.Background())
		if err != nil || strings.TrimSpace(string(data)) != `{"type":"snapshot","candle":null}` {
			t.Errorf("%q: first message %q err %v", q, data, err)
		}
		c.CloseNow()
	}
}

type wsMsg struct {
	Type   string `json:"type"`
	Start  int64  `json:"start"`
	Candle *struct {
		Date      string `json:"date"`
		Index     int64  `json:"index"`
		Start     int64  `json:"start"`
		Open      int64  `json:"open"`
		High      int64  `json:"high"`
		Low       int64  `json:"low"`
		Close     int64  `json:"close"`
		Volume    int64  `json:"volume"`
		TickCount int64  `json:"tick_count"`
	} `json:"candle"`
}

// collect reads until the server closes the connection.
func collect(t *testing.T, url string) func() []wsMsg {
	c, _, err := websocket.Dial(context.Background(), url, nil)
	if err != nil {
		t.Fatal(err)
	}
	var msgs []wsMsg
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			_, data, err := c.Read(context.Background())
			if err != nil {
				if websocket.CloseStatus(err) != websocket.StatusTryAgainLater {
					t.Errorf("unexpected end: %v", err)
				}
				return
			}
			var m wsMsg
			if err := json.Unmarshal(data, &m); err != nil {
				t.Errorf("bad json %q: %v", data, err)
			}
			msgs = append(msgs, m)
		}
	}()
	return func() []wsMsg { <-done; return msgs }
}

// TestIntegration runs a real ingester writing day files, subscribes before and
// during the stream, and checks that snapshot + updates have no gap or duplicate.
func TestIntegration(t *testing.T) {
	const n, iv, total = 7, 5, 4000
	dir := t.TempDir()
	b := NewBroker()
	in := ingest.New(dir, seoul)
	in.OnCommit(b.Publish)
	srv := httptest.NewServer(Handler(dir, b, seoul))
	defer srv.Close()
	base := "ws" + strings.TrimPrefix(srv.URL, "http") + "/candles/stream?symbol=AAA&"
	tickURL, timeURL := base+"type=tick&n=7", base+"type=time&interval=5m"

	r := rand.New(rand.NewSource(1))
	ts := time.Date(2026, 1, 5, 23, 50, 0, 0, seoul).UnixMilli()
	var truth []rawTick
	for i := 0; i < total; i++ {
		if r.Intn(5) != 0 { // else duplicate ts
			ts += 1000 + r.Int63n(3000)
		}
		tk := rawTick{ts, 1 + r.Int63n(100), 1 + r.Int63n(9), true}
		if r.Intn(8) == 0 {
			tk.valid, tk.price = false, 0
		}
		truth = append(truth, tk)
	}

	type sub struct {
		join int // join after this many ticks were put
		get  func() []wsMsg
	}
	var tickSubs, timeSubs []*sub
	for _, j := range []int{0, 1000, 2500} {
		tickSubs = append(tickSubs, &sub{join: j})
		timeSubs = append(timeSubs, &sub{join: j})
	}
	join := func(i int) {
		for k := range tickSubs {
			if tickSubs[k].join == i {
				tickSubs[k].get = collect(t, tickURL)
				timeSubs[k].get = collect(t, timeURL)
			}
		}
	}
	for i, tk := range truth {
		join(i)
		in.Put(tickOf(tk))
		if i%10 == 9 {
			time.Sleep(time.Millisecond)
		}
	}
	in.Close()
	b.Close()

	// Expected values from the oracle.
	days := map[string][]oracle.Tick{}
	var valid []oracle.Tick
	for _, tk := range truth {
		if tk.valid {
			d, _ := oracle.DateOf(tk.ts, seoul)
			days[d] = append(days[d], oracle.Tick{TS: tk.ts, Price: tk.price, Qty: tk.qty, Valid: true})
			valid = append(valid, days[d][len(days[d])-1])
		}
	}
	var dates []string
	for d := range days {
		dates = append(dates, d)
	}
	slices.Sort(dates)
	var flat []flatCandle
	for _, d := range dates {
		cs := oracle.NTick([][]oracle.Tick{days[d]}, n, math.MaxInt32)
		slices.Reverse(cs)
		for i, c := range cs {
			flat = append(flat, flatCandle{d, int64(i), c})
		}
	}
	pos := func(date string, idx int64) int {
		return slices.IndexFunc(flat, func(f flatCandle) bool { return f.date == date && f.index == idx })
	}

	for k, s := range tickSubs {
		msgs := s.get()
		if msgs[0].Type != "snapshot" {
			t.Fatalf("tick sub %d: first message %q", k, msgs[0].Type)
		}
		next, lastCur := -1, -1
		for i, m := range msgs {
			if m.Candle == nil {
				if i != 0 || m.Type != "snapshot" {
					t.Fatalf("tick sub %d: null candle in %q", k, m.Type)
				}
				continue
			}
			c := m.Candle
			p := pos(c.Date, c.Index)
			if p < 0 {
				t.Fatalf("tick sub %d: unknown candle %+v", k, c)
			}
			// The candle must equal the oracle's first TickCount ticks of that candle.
			day := days[c.Date]
			part := oracle.NTick([][]oracle.Tick{day[int(c.Index)*n : int(c.Index)*n+int(c.TickCount)]}, n, 1)[0]
			if part.Open != c.Open || part.High != c.High || part.Low != c.Low || part.Close != c.Close || part.Volume != c.Volume {
				t.Fatalf("tick sub %d msg %d (%s): %+v != oracle prefix %+v", k, i, m.Type, c, part)
			}
			if m.Type == "complete" {
				if c.TickCount != flat[p].c.Count {
					t.Fatalf("tick sub %d: complete %+v has %d ticks, want %d", k, c, c.TickCount, flat[p].c.Count)
				}
				if next >= 0 && p != next {
					t.Fatalf("tick sub %d: complete at %d, want %d (gap or duplicate)", k, p, next)
				}
				if next < 0 && lastCur >= 0 && p != lastCur {
					t.Fatalf("tick sub %d: first complete %d but snapshot candle %d", k, p, lastCur)
				}
				if next < 0 && lastCur < 0 && s.join == 0 && p != 0 {
					t.Fatalf("tick sub %d: first complete %d, want 0", k, p)
				}
				next, lastCur = p+1, -1
			} else {
				lastCur = p
			}
		}
		last := flat[len(flat)-1]
		wantNext := len(flat)
		if last.c.Count < n {
			wantNext-- // the final partial candle stays in progress
		}
		if next != wantNext {
			t.Errorf("tick sub %d: completes end before %d, want %d", k, next, wantNext)
		}
		if last.c.Count < n && lastCur != len(flat)-1 {
			t.Errorf("tick sub %d: final in-progress candle missing", k)
		}
	}

	want := oracle.Time(valid, seoul, iv, 0, math.MaxInt64)
	for k, s := range timeSubs {
		msgs := s.get()
		var cur *wsMsg
		next := -1
		for i := range msgs {
			m := msgs[i]
			if m.Candle == nil {
				continue
			}
			p := slices.IndexFunc(want, func(w oracle.TimeCandle) bool { return w.Start == m.Candle.Start })
			if p < 0 || m.Candle.TickCount > want[p].Count {
				t.Fatalf("time sub %d: bad candle %+v", k, m.Candle)
			}
			if m.Type == "complete" {
				w, c := want[p], m.Candle
				if c.Open != w.Open || c.High != w.High || c.Low != w.Low || c.Close != w.Close || c.Volume != w.Volume || c.TickCount != w.Count {
					t.Fatalf("time sub %d: complete %+v want %+v", k, c, w)
				}
				if next >= 0 && p != next {
					t.Fatalf("time sub %d: complete at %d, want %d", k, p, next)
				}
				next, cur = p+1, nil
			} else {
				cur = &msgs[i]
			}
		}
		if next != len(want)-1 || cur == nil || cur.Candle.Start != want[len(want)-1].Start || cur.Candle.TickCount != want[len(want)-1].Count {
			t.Errorf("time sub %d: ends at %d (want %d) in-progress %+v", k, next, len(want)-1, cur)
		}
	}
}

func tickOf(t rawTick) tick.Tick {
	return tick.Tick{Symbol: "AAA", TS: t.ts, Price: t.price, Qty: t.qty}
}

// Events already contained in the snapshot (raw_seq <= its last_raw_seq) must not be applied twice.
func TestSnapshotDedup(t *testing.T) {
	dir := t.TempDir()
	in := ingest.New(dir, seoul)
	ts := time.Date(2026, 1, 5, 10, 0, 0, 0, seoul).UnixMilli()
	for i := int64(1); i <= 5; i++ {
		in.Put(tick.Tick{Symbol: "AAA", TS: ts + i, Price: 100 * i, Qty: 1})
	}
	in.Close()
	b := NewBroker()
	srv := httptest.NewServer(Handler(dir, b, seoul))
	defer srv.Close()
	c, _, err := websocket.Dial(context.Background(), "ws"+strings.TrimPrefix(srv.URL, "http")+"/candles/stream?symbol=AAA&type=tick&n=1000", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer c.CloseNow()
	read := func() wsMsg {
		_, data, err := c.Read(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		var m wsMsg
		json.Unmarshal(data, &m)
		return m
	}
	if m := read(); m.Type != "snapshot" || m.Candle.TickCount != 5 || m.Candle.Volume != 5 || m.Candle.Close != 500 {
		t.Fatalf("snapshot %+v", m.Candle)
	}
	ct := func(seq int64) ingest.CommitTick {
		return ingest.CommitTick{Seq: seq, TS: ts + seq, Price: 100 * seq, Qty: 1}
	}
	b.Publish(ingest.Event{Symbol: "AAA", Date: "20260105", Ticks: []ingest.CommitTick{ct(4), ct(5)}, T: 5, LastSeq: 5})
	b.Publish(ingest.Event{Symbol: "AAA", Date: "20260105", Ticks: []ingest.CommitTick{ct(5), ct(6)}, T: 6, LastSeq: 6})
	if m := read(); m.Type != "update" || m.Candle.TickCount != 6 || m.Candle.Volume != 6 || m.Candle.Close != 600 || m.Candle.Index != 0 {
		t.Fatalf("update %+v", m.Candle)
	}
}
