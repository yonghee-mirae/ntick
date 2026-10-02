// Package oracle derives expected DB contents from the mockfeed truth CSV,
// independently of the implementation under test.
package oracle

import (
	"encoding/csv"
	"fmt"
	"io"
	"sort"
	"strconv"
	"time"
)

// Row is one truth line: idx,symbol,ts,price,qty,label.
type Row struct {
	Idx, TS, Price, Qty int64
	Symbol, Label       string
}

// Tick is an expected ticks-table row (raw_seq is the 1-based position in the file).
type Tick struct {
	TS, Price, Qty int64
	Valid          bool
}

// Candle is an OHLCV bucket. OpenTS/CloseTS are only set for 1m candles.
type Candle struct {
	Open, High, Low, Close int64
	OpenTS, CloseTS        int64
	Volume, Count          int64
}

// Key identifies one DB file.
type Key struct{ Symbol, Date string } // Date = YYYYMMDD local

// Day is the expectation for one DB file.
type Day struct {
	Ticks      []Tick
	ValidCount int64
	Candles    map[int64]Candle // by minute
}

// ParseTruth reads the truth CSV.
func ParseTruth(r io.Reader) ([]Row, error) {
	recs, err := csv.NewReader(r).ReadAll()
	if err != nil {
		return nil, err
	}
	rows := make([]Row, 0, len(recs))
	for i, f := range recs {
		if len(f) != 6 {
			return nil, fmt.Errorf("line %d: want 6 fields, got %d", i+1, len(f))
		}
		var n [4]int64
		for j, s := range []string{f[0], f[2], f[3], f[4]} {
			if n[j], err = strconv.ParseInt(s, 10, 64); err != nil {
				return nil, fmt.Errorf("line %d: %w", i+1, err)
			}
		}
		rows = append(rows, Row{Idx: n[0], Symbol: f[1], TS: n[1], Price: n[2], Qty: n[3], Label: f[5]})
	}
	return rows, nil
}

// IsValid applies the PRD rule to a truth label. Duplicates and ts reversals are valid.
func IsValid(label string) bool { return label != "price_le0" && label != "qty_le0" }

// DateOf returns the local YYYYMMDD of ts and the epoch ms of that local midnight.
func DateOf(ts int64, loc *time.Location) (string, int64) {
	t := time.UnixMilli(ts).In(loc)
	mid := time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, loc)
	return t.Format("20060102"), mid.UnixMilli()
}

// Expected builds the per-file expectation from truth rows in idx order.
func Expected(rows []Row, loc *time.Location) map[Key]*Day {
	out := map[Key]*Day{}
	for _, r := range rows {
		date, mid := DateOf(r.TS, loc)
		k := Key{r.Symbol, date}
		d := out[k]
		if d == nil {
			d = &Day{Candles: map[int64]Candle{}}
			out[k] = d
		}
		v := IsValid(r.Label)
		d.Ticks = append(d.Ticks, Tick{r.TS, r.Price, r.Qty, v})
		if !v {
			continue
		}
		d.ValidCount++
		m := (r.TS - mid) / 60000
		c, ok := d.Candles[m]
		if !ok {
			d.Candles[m] = Candle{r.Price, r.Price, r.Price, r.Price, r.TS, r.TS, r.Qty, 1}
			continue
		}
		// Arrival order breaks ts ties: a later arrival replaces open only if strictly earlier,
		// and close if equal or later.
		if r.TS < c.OpenTS {
			c.Open, c.OpenTS = r.Price, r.TS
		}
		if r.TS >= c.CloseTS {
			c.Close, c.CloseTS = r.Price, r.TS
		}
		c.High, c.Low = max(c.High, r.Price), min(c.Low, r.Price)
		c.Volume += r.Qty
		c.Count++
		d.Candles[m] = c
	}
	return out
}

// NTick returns the expected n-tick candles for a query (n, m). days holds the valid ticks of each
// day in arrival order, oldest day first. Boundaries are fixed per day from its first valid tick;
// the last candle of a day may be partial and is never merged with the next day. The result is
// ordered newest first and holds at most m candles.
func NTick(days [][]Tick, n, m int) []Candle {
	var out []Candle
	for i := len(days) - 1; i >= 0 && len(out) < m; i-- {
		t := days[i]
		var cs []Candle
		for s := 0; s < len(t); s += n {
			cs = append(cs, bucket(t[s:min(s+n, len(t))]))
		}
		for j := len(cs) - 1; j >= 0 && len(out) < m; j-- {
			out = append(out, cs[j])
		}
	}
	return out
}

func bucket(t []Tick) Candle {
	c := Candle{Open: t[0].Price, High: t[0].Price, Low: t[0].Price, Close: t[len(t)-1].Price}
	for _, x := range t {
		c.High, c.Low = max(c.High, x.Price), min(c.Low, x.Price)
		c.Volume += x.Qty
		c.Count++
	}
	return c
}

// TimeCandle is an expected time candle. Start is the bucket start in epoch ms.
type TimeCandle struct {
	Start                  int64
	Open, High, Low, Close int64
	Volume, Count          int64
	Partial                bool
}

// Time returns the expected time candles of one symbol, computed from its valid ticks in arrival
// order (all days together; day files are implied by each ts). Buckets are aligned to local midnight
// and never cross it; only buckets with Start in [from, to) are returned, ascending, empty ones
// omitted. Open/close follow ts, ties broken by arrival order. A bucket is Partial when its end,
// min(start+interval, local day end), is later than the latest ts among all given ticks.
func Time(ticks []Tick, loc *time.Location, intervalMin int, from, to int64) []TimeCandle {
	step := int64(intervalMin) * 60000
	type acc struct {
		TimeCandle
		openTS, closeTS int64
	}
	byStart := map[int64]*acc{}
	latest, end0 := int64(-1<<63), map[int64]int64{}
	for _, t := range ticks {
		latest = max(latest, t.TS)
		_, mid := DateOf(t.TS, loc)
		start := mid + (t.TS-mid)/step*step
		a := byStart[start]
		if a == nil {
			tm := time.UnixMilli(mid).In(loc)
			dayEnd := time.Date(tm.Year(), tm.Month(), tm.Day()+1, 0, 0, 0, 0, loc).UnixMilli()
			end0[start] = min(start+step, dayEnd)
			byStart[start] = &acc{TimeCandle{Start: start, Open: t.Price, High: t.Price, Low: t.Price, Close: t.Price,
				Volume: t.Qty, Count: 1}, t.TS, t.TS}
			continue
		}
		if t.TS < a.openTS {
			a.Open, a.openTS = t.Price, t.TS
		}
		if t.TS >= a.closeTS {
			a.Close, a.closeTS = t.Price, t.TS
		}
		a.High, a.Low = max(a.High, t.Price), min(a.Low, t.Price)
		a.Volume += t.Qty
		a.Count++
	}
	var out []TimeCandle
	for s, a := range byStart {
		if s >= from && s < to {
			a.Partial = end0[s] > latest
			out = append(out, a.TimeCandle)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Start < out[j].Start })
	return out
}
