package stream

import (
	"math"
	"math/rand"
	"slices"
	"testing"
	"time"

	"ntick/internal/ingest"
	"ntick/internal/oracle"
)

var seoul = func() *time.Location {
	l, err := time.LoadLocation("Asia/Seoul")
	if err != nil {
		panic(err)
	}
	return l
}()

type rawTick struct {
	ts, price, qty int64
	valid          bool
}

// genTicks makes a random stream around local midnight with invalid ticks,
// duplicated ts and ts reversals (some reaching back into the previous day).
func genTicks(r *rand.Rand, count int) []rawTick {
	ts := time.Date(2026, 1, 5, 23, 55, 0, 0, seoul).UnixMilli()
	out := make([]rawTick, count)
	for i := range out {
		switch x := r.Intn(10); {
		case x == 0: // duplicate ts
		case x == 1: // reversal
			ts -= r.Int63n(400_000)
		default:
			ts += r.Int63n(60_000)
		}
		t := rawTick{ts, 1 + r.Int63n(100), 1 + r.Int63n(9), true}
		if r.Intn(8) == 0 {
			t.valid, t.price = false, 0
		}
		out[i] = t
	}
	return out
}

// mkEvents emulates the writer: random batches, one event per day file in
// first-appearance order, raw_seq counting invalid ticks too.
func mkEvents(r *rand.Rand, ticks []rawTick) []ingest.Event {
	seq, valid := map[string]int64{}, map[string]int64{}
	var evs []ingest.Event
	for len(ticks) > 0 {
		b := ticks[:min(len(ticks), 1+r.Intn(20))]
		ticks = ticks[len(b):]
		var order []string
		byDate := map[string]*ingest.Event{}
		for _, t := range b {
			d, _ := oracle.DateOf(t.ts, seoul)
			seq[d]++
			e := byDate[d]
			if e == nil {
				e = &ingest.Event{Symbol: "A", Date: d}
				byDate[d] = e
				order = append(order, d)
			}
			if t.valid {
				valid[d]++
				e.Ticks = append(e.Ticks, ingest.CommitTick{Seq: seq[d], TS: t.ts, Price: t.price, Qty: t.qty})
			}
			e.T, e.LastSeq = valid[d], seq[d]
		}
		for _, d := range order {
			if len(byDate[d].Ticks) > 0 {
				evs = append(evs, *byDate[d])
			}
		}
	}
	return evs
}

type flatCandle struct {
	date  string
	index int64
	c     oracle.Candle
}

func TestTickMachineMatchesOracle(t *testing.T) {
	for seed := int64(0); seed < 40; seed++ {
		for _, n := range []int{1, 2, 7, 50, 100000} {
			r := rand.New(rand.NewSource(seed))
			evs := mkEvents(r, genTicks(r, 50+r.Intn(1500)))
			m := &tickMachine{n: n}
			var got []tickCandle
			var last any
			for _, ev := range evs {
				for _, msg := range m.apply(ev) {
					cm := msg.(candleMsg)
					switch cm.Type {
					case "complete":
						got = append(got, cm.Candle.(tickCandle))
					case "update":
						last = cm.Candle
					}
				}
			}
			if cur := m.current(); cur != nil {
				got = append(got, cur.(tickCandle))
				if last != cur {
					t.Fatalf("seed %d n %d: last update %v != current %v", seed, n, last, cur)
				}
			}
			// Expected: valid ticks per day; ticks of a day older than the newest
			// day seen so far are ignored by design.
			days := map[string][]oracle.Tick{}
			maxDate := ""
			for _, ev := range evs {
				if ev.Date < maxDate {
					continue
				}
				maxDate = ev.Date
				for _, k := range ev.Ticks {
					days[ev.Date] = append(days[ev.Date], oracle.Tick{TS: k.TS, Price: k.Price, Qty: k.Qty, Valid: true})
				}
			}
			var dates []string
			for d := range days {
				dates = append(dates, d)
			}
			slices.Sort(dates)
			var want []flatCandle
			for _, d := range dates {
				cs := oracle.NTick([][]oracle.Tick{days[d]}, n, math.MaxInt32)
				slices.Reverse(cs)
				for i, c := range cs {
					want = append(want, flatCandle{d, int64(i), c})
				}
			}
			if len(got) != len(want) {
				t.Fatalf("seed %d n %d: %d candles, want %d", seed, n, len(got), len(want))
			}
			for i, g := range got {
				w := want[i]
				if g.Date != w.date || g.Index != w.index || g.Open != w.c.Open || g.High != w.c.High || g.Low != w.c.Low ||
					g.Close != w.c.Close || g.Volume != w.c.Volume || int64(g.TickCount) != w.c.Count || g.Partial != (g.TickCount < n) {
					t.Fatalf("seed %d n %d candle %d: got %+v want %+v", seed, n, i, g, w)
				}
			}
		}
	}
}

func TestTimeMachineMatchesOracle(t *testing.T) {
	for seed := int64(0); seed < 40; seed++ {
		for _, iv := range []int{1, 5, 60, 1440} {
			r := rand.New(rand.NewSource(seed))
			evs := mkEvents(r, genTicks(r, 50+r.Intn(1500)))
			m := &timeMachine{loc: seoul, step: int64(iv) * 60000}
			completes := map[int64]timeCandle{}
			dirty := map[int64]bool{}
			var all []oracle.Tick
			for _, ev := range evs {
				for _, k := range ev.Ticks {
					all = append(all, oracle.Tick{TS: k.TS, Price: k.Price, Qty: k.Qty, Valid: true})
				}
				for _, msg := range m.apply(ev) {
					switch x := msg.(type) {
					case candleMsg:
						if x.Type == "complete" {
							c := x.Candle.(timeCandle)
							if _, dup := completes[c.Start]; dup && !dirty[c.Start] {
								t.Fatalf("seed %d iv %d: bucket %d completed twice", seed, iv, c.Start)
							}
							completes[c.Start] = c
						}
					case invalidateMsg:
						dirty[x.Start] = true
					}
				}
			}
			want := oracle.Time(all, seoul, iv, 0, math.MaxInt64)
			cur := m.current().(timeCandle)
			if last := want[len(want)-1]; cur.Start != last.Start {
				t.Fatalf("seed %d iv %d: in-progress start %d, want %d", seed, iv, cur.Start, last.Start)
			}
			for _, w := range want {
				same := func(c timeCandle) bool {
					return c.Start == w.Start && c.Open == w.Open && c.High == w.High && c.Low == w.Low &&
						c.Close == w.Close && c.Volume == w.Volume && int64(c.TickCount) == w.Count
				}
				if w.Start == cur.Start {
					if !same(cur) {
						t.Fatalf("seed %d iv %d: in-progress %+v want %+v", seed, iv, cur, w)
					}
					continue
				}
				c, ok := completes[w.Start]
				switch {
				case dirty[w.Start]: // client re-queries; nothing to compare
				case !ok:
					t.Fatalf("seed %d iv %d: bucket %d never completed or invalidated", seed, iv, w.Start)
				case !same(c) || c.Partial:
					t.Fatalf("seed %d iv %d: complete %+v want %+v", seed, iv, c, w)
				}
			}
		}
	}
}

// A late tick must invalidate exactly its own bucket, once per event.
func TestTimeInvalidate(t *testing.T) {
	m := &timeMachine{loc: seoul, step: 60000}
	base := time.Date(2026, 1, 5, 10, 0, 0, 0, seoul).UnixMilli()
	m.apply(ingest.Event{Date: "20260105", Ticks: []ingest.CommitTick{{Seq: 1, TS: base, Price: 10, Qty: 1}, {Seq: 2, TS: base + 120_000, Price: 11, Qty: 1}}})
	out := m.apply(ingest.Event{Date: "20260105", Ticks: []ingest.CommitTick{{Seq: 3, TS: base + 1000, Price: 9, Qty: 1}, {Seq: 4, TS: base + 2000, Price: 9, Qty: 1}}})
	if len(out) != 1 || out[0] != (invalidateMsg{"invalidate", base}) {
		t.Fatalf("got %+v", out)
	}
}
