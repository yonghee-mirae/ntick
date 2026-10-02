package oracle

import (
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"
)

var seoul = func() *time.Location { l, _ := time.LoadLocation("Asia/Seoul"); return l }()

func ms(s string) int64 {
	t, err := time.ParseInLocation("2006-01-02 15:04:05", s, seoul)
	if err != nil {
		panic(err)
	}
	return t.UnixMilli()
}

func TestExpected(t *testing.T) {
	csv := strings.Join([]string{
		"0,A,%d,100,5,ok",
		"1,A,%d,0,5,price_le0",
		"2,A,%d,110,1,ts_reversal", // same minute, earlier ts: becomes open
		"3,A,%d,110,1,dup",         // same ts as close, later arrival: becomes close
		"4,A,%d,90,0,qty_le0",
		"5,A,%d,120,2,ok", // next local day
		"",
	}, "\n")
	ts := []any{ms("2026-01-05 09:00:30"), ms("2026-01-05 09:00:40"), ms("2026-01-05 09:00:10"),
		ms("2026-01-05 09:00:30"), ms("2026-01-05 09:00:50"), ms("2026-01-06 00:00:00")}
	rows, err := ParseTruth(strings.NewReader(sprintf(csv, ts...)))
	if err != nil {
		t.Fatal(err)
	}
	got := Expected(rows, seoul)
	d := got[Key{"A", "20260105"}]
	if d == nil || len(d.Ticks) != 5 || d.ValidCount != 3 {
		t.Fatalf("day1: %+v", d)
	}
	if d.Ticks[1].Valid || d.Ticks[4].Valid || !d.Ticks[3].Valid {
		t.Errorf("valid flags: %+v", d.Ticks)
	}
	want := Candle{Open: 110, High: 110, Low: 100, Close: 110, OpenTS: ms("2026-01-05 09:00:10"),
		CloseTS: ms("2026-01-05 09:00:30"), Volume: 7, Count: 3}
	if c := d.Candles[540]; c != want || len(d.Candles) != 1 {
		t.Errorf("candle: got %+v want %+v", c, want)
	}
	d2 := got[Key{"A", "20260106"}]
	if d2 == nil || d2.Candles[0].Open != 120 { // midnight tick belongs to minute 0 of the new day
		t.Errorf("day2: %+v", d2)
	}
}

func sprintf(f string, a ...any) string { return strings.TrimSpace(fmt.Sprintf(f, a...)) + "\n" }

func ticks(prices ...int64) []Tick {
	out := make([]Tick, len(prices))
	for i, p := range prices {
		out[i] = Tick{TS: int64(i), Price: p, Qty: 1, Valid: true}
	}
	return out
}

func TestNTick(t *testing.T) {
	days := [][]Tick{ticks(1, 2, 3, 4, 5), ticks(10, 20, 15)} // oldest first
	got := NTick(days, 2, 4)
	want := []Candle{
		{Open: 10, High: 20, Low: 10, Close: 20, Volume: 2, Count: 2}, // newest day, full
		{Open: 15, High: 15, Low: 15, Close: 15, Volume: 1, Count: 1}, // newest day's partial last candle comes first
		{Open: 5, High: 5, Low: 5, Close: 5, Volume: 1, Count: 1},
		{Open: 3, High: 4, Low: 3, Close: 4, Volume: 2, Count: 2},
	}
	// newest first: partial candle (15) is the most recent
	want[0], want[1] = want[1], want[0]
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %+v\nwant %+v", got, want)
	}
	if got := NTick(days, 2, 100); len(got) != 5 {
		t.Errorf("m beyond data: got %d candles, want 5", len(got))
	}
	if got := NTick([][]Tick{nil, ticks(1)}, 3, 5); len(got) != 1 {
		t.Errorf("empty day must be skipped: %d", len(got))
	}
}

func TestTime(t *testing.T) {
	tk := func(s string, p, q int64) Tick { return Tick{TS: ms(s), Price: p, Qty: q, Valid: true} }
	in := []Tick{
		tk("2026-01-05 09:01:30", 100, 1),
		tk("2026-01-05 09:00:10", 90, 2),  // arrives later, earlier ts: becomes open of 09:00 5m
		tk("2026-01-05 09:01:30", 110, 3), // same ts as close, later arrival: becomes close
		tk("2026-01-05 09:07:00", 50, 4),
		tk("2026-01-05 23:59:59", 70, 5),
		tk("2026-01-06 00:00:00", 80, 6),
	}
	d5, d6 := ms("2026-01-05 00:00:00"), ms("2026-01-06 00:00:00")
	got := Time(in, seoul, 5, d5, d6+1)
	want := []TimeCandle{
		{Start: ms("2026-01-05 09:00:00"), Open: 90, High: 110, Low: 90, Close: 110, Volume: 6, Count: 3},
		{Start: ms("2026-01-05 09:05:00"), Open: 50, High: 50, Low: 50, Close: 50, Volume: 4, Count: 1},
		{Start: ms("2026-01-05 23:55:00"), Open: 70, High: 70, Low: 70, Close: 70, Volume: 5, Count: 1},
		{Start: d6, Open: 80, High: 80, Low: 80, Close: 80, Volume: 6, Count: 1, Partial: true},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("5m:\n got %+v\nwant %+v", got, want)
	}
	// Half-open range: `to` excludes a bucket starting exactly at to.
	if g := Time(in, seoul, 5, ms("2026-01-05 09:05:00"), ms("2026-01-05 23:55:00")); len(g) != 1 || g[0].Count != 1 {
		t.Errorf("range: %+v", g)
	}
	// 1d buckets never cross midnight; the day-1 bucket is complete, the last day is partial.
	g := Time(in, seoul, 1440, d5, d6+1)
	if len(g) != 2 || g[0].Start != d5 || g[0].Count != 5 || g[0].Partial || !g[1].Partial {
		t.Errorf("1d: %+v", g)
	}
	// A 7m bucket is cut at midnight: 23:55 start (1435 = 205*7) ends at 00:00, not 00:02.
	g = Time(in, seoul, 7, ms("2026-01-05 23:50:00"), d6)
	if len(g) != 1 || g[0].Start != d5+205*7*60000 || g[0].Partial {
		t.Errorf("7m: %+v", g)
	}
}
