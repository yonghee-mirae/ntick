package main

import (
	"math/rand"
	"strconv"
	"time"

	"ntick/internal/wire"
)

// Truth labels written to the ground-truth file.
const (
	LabelOK       = "ok"
	LabelPriceLE0 = "price_le0"
	LabelQtyLE0   = "qty_le0"
	LabelReversal = "ts_reversal"
	LabelDup      = "dup" // exact copy of the preceding message of the same symbol
)

type genConfig struct {
	Seed      int64
	Symbols   int
	Days      int
	PerDay    int // base ticks per day (duplicates are extra)
	Loc       *time.Location
	Start     time.Time // local date and time of the first tick of day 1; zero = 2026-01-05 09:00
	PAnomaly  float64
	PReversal float64
	PDup      float64
}

type gen struct {
	c       genConfig
	rnd     *rand.Rand
	day     int
	inDay   int
	clock   int64 // current epoch ms within the day
	price   []int64
	lastTs  []int64
	pending []wire.Message // queued duplicates
}

func newGen(c genConfig) *gen {
	g := &gen{c: c, rnd: rand.New(rand.NewSource(c.Seed)), price: make([]int64, c.Symbols), lastTs: make([]int64, c.Symbols)}
	for i := range g.price {
		g.price[i] = 10000 + int64(g.rnd.Intn(90000))
	}
	g.startDay()
	return g
}

func symName(i int) string { return "S" + strconv.Itoa(100000+i) }

// startDay places the clock at the start time on consecutive local calendar days,
// so ts crosses local midnight between days.
func (g *gen) startDay() {
	st := g.c.Start
	if st.IsZero() {
		st = time.Date(2026, 1, 5, 9, 0, 0, 0, g.c.Loc)
	}
	y, mo, dd := st.Date()
	d := time.Date(y, mo, dd+g.day, st.Hour(), st.Minute(), 0, 0, g.c.Loc)
	g.clock = d.UnixMilli()
	g.inDay = 0
}

// next returns the next message and its label; ok=false when all days are done.
func (g *gen) next() (wire.Message, string, bool) {
	if len(g.pending) > 0 {
		m := g.pending[0]
		g.pending = g.pending[1:]
		return m, LabelDup, true
	}
	if g.inDay >= g.c.PerDay {
		g.day++
		if g.day >= g.c.Days {
			return wire.Message{}, "", false
		}
		g.startDay()
	}
	g.inDay++
	g.clock += int64(g.rnd.Intn(200)) // 0 ms steps allow same-ms ticks
	s := g.rnd.Intn(g.c.Symbols)
	g.price[s] += int64(g.rnd.Intn(201)) - 100
	if g.price[s] < 1 {
		g.price[s] = 1
	}
	m := wire.Message{Symbol: symName(s), Ts: g.clock, Price: g.price[s], Qty: 1 + int64(g.rnd.Intn(1000))}
	label := LabelOK
	switch r := g.rnd.Float64(); {
	case r < g.c.PAnomaly:
		if g.rnd.Intn(2) == 0 {
			m.Price, label = -int64(g.rnd.Intn(2)), LabelPriceLE0 // 0 or -1
		} else {
			m.Qty, label = -int64(g.rnd.Intn(2)), LabelQtyLE0
		}
	case r < g.c.PAnomaly+g.c.PReversal && g.lastTs[s] > 0:
		m.Ts, label = g.lastTs[s]-1-int64(g.rnd.Intn(1000)), LabelReversal
	case r < g.c.PAnomaly+g.c.PReversal+g.c.PDup:
		g.pending = append(g.pending, m)
	}
	g.lastTs[s] = m.Ts
	return m, label, true
}
