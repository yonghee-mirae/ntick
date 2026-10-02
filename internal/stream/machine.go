package stream

import (
	"time"

	"ntick/internal/ingest"
)

// agg accumulates OHLCV. Ordering is by ts with arrival order breaking ties
// (open replaced only when strictly earlier, close when equal or later), the
// same rule as the candle_1m upsert. Tick candles pass raw_seq as ts, which
// reduces it to plain arrival order.
type agg struct {
	open, high, low, close, volume int64
	count                          int
	openTS, closeTS                int64
}

func (a *agg) add(price, qty, ts int64) {
	if a.count == 0 {
		*a = agg{price, price, price, price, qty, 1, ts, ts}
		return
	}
	if ts < a.openTS {
		a.open, a.openTS = price, ts
	}
	if ts >= a.closeTS {
		a.close, a.closeTS = price, ts
	}
	a.high, a.low = max(a.high, price), min(a.low, price)
	a.volume += qty
	a.count++
}

// Wire candles: same field names as REST. tickCandle adds Index, the 0-based
// ordinal of the candle within its day (stream messages only).
type tickCandle struct {
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

type timeCandle struct {
	Start     int64 `json:"start"`
	Open      int64 `json:"open"`
	High      int64 `json:"high"`
	Low       int64 `json:"low"`
	Close     int64 `json:"close"`
	Volume    int64 `json:"volume"`
	TickCount int   `json:"tick_count"`
	Partial   bool  `json:"partial"`
}

type candleMsg struct {
	Type   string `json:"type"`
	Candle any    `json:"candle"`
}

type invalidateMsg struct {
	Type  string `json:"type"`
	Start int64  `json:"start"`
}

// machine derives the watched in-progress candle from commit events only.
type machine interface {
	apply(ev ingest.Event) []any // messages caused by ev
	current() any                // in-progress candle or nil
}

// tickMachine tracks the n-tick candle of the newest day. A candle completes
// the moment the day's valid count T reaches a multiple of n; a tick of a newer
// day completes the old day's last (partial) candle. Ticks of an older day than
// the current one cannot touch the in-progress candle and are ignored.
type tickMachine struct {
	n    int
	date string
	t    int64 // valid ticks of date
	cur  agg
}

func (m *tickMachine) candle() tickCandle {
	a := m.cur
	return tickCandle{m.date, (m.t - 1) / int64(m.n), a.open, a.high, a.low, a.close, a.volume, a.count, a.count < m.n}
}

func (m *tickMachine) current() any {
	if m.cur.count == 0 {
		return nil
	}
	return m.candle()
}

func (m *tickMachine) apply(ev ingest.Event) []any {
	if ev.Date < m.date {
		return nil
	}
	var out []any
	if ev.Date > m.date {
		if m.cur.count > 0 {
			out = append(out, candleMsg{"complete", m.candle()})
		}
		m.date, m.t, m.cur = ev.Date, 0, agg{}
	}
	changed := false
	for _, k := range ev.Ticks {
		m.cur.add(k.Price, k.Qty, k.Seq)
		m.t++
		changed = true
		if m.cur.count == m.n {
			out = append(out, candleMsg{"complete", m.candle()})
			m.cur, changed = agg{}, false
		}
	}
	if changed {
		out = append(out, candleMsg{"update", m.candle()})
	}
	return out
}

// timeMachine tracks the newest time bucket (by ts). A tick in a later bucket
// completes the current one; a tick in an earlier bucket is a late tick and
// yields an invalidate for that bucket.
type timeMachine struct {
	loc   *time.Location
	step  int64 // bucket length in ms
	start int64 // current bucket start; valid when cur.count > 0
	cur   agg
}

func (m *timeMachine) candle() timeCandle {
	a := m.cur
	return timeCandle{m.start, a.open, a.high, a.low, a.close, a.volume, a.count, true}
}

func (m *timeMachine) current() any {
	if m.cur.count == 0 {
		return nil
	}
	return m.candle()
}

func (m *timeMachine) apply(ev ingest.Event) []any {
	d, err := time.ParseInLocation("20060102", ev.Date, m.loc)
	if err != nil {
		return nil // ingest only emits valid dates
	}
	mid := d.UnixMilli()
	var out []any
	var late []int64
	changed := false
	for _, k := range ev.Ticks {
		start := mid + (k.TS-mid)/m.step*m.step
		switch {
		case m.cur.count == 0 || start > m.start:
			if m.cur.count > 0 {
				c := m.candle()
				c.Partial = false
				out = append(out, candleMsg{"complete", c})
			}
			m.cur, m.start = agg{}, start
			m.cur.add(k.Price, k.Qty, k.TS)
			changed = true
		case start == m.start:
			m.cur.add(k.Price, k.Qty, k.TS)
			changed = true
		default:
			late = append(late, start)
		}
	}
	if changed {
		out = append(out, candleMsg{"update", m.candle()})
	}
	seen := map[int64]bool{}
	for _, s := range late {
		if !seen[s] {
			seen[s] = true
			out = append(out, invalidateMsg{"invalidate", s})
		}
	}
	return out
}
