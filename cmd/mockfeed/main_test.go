package main

import (
	"bufio"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"ntick/internal/wire"
)

func cfg() genConfig {
	loc, _ := time.LoadLocation("Asia/Seoul")
	return genConfig{Seed: 7, Symbols: 3, Days: 2, PerDay: 300, Loc: loc, PAnomaly: 0.05, PReversal: 0.05, PDup: 0.05}
}

func TestGenDeterministicAndLabels(t *testing.T) {
	run := func() (out []wire.Message, labels map[string]int) {
		g, labels := newGen(cfg()), map[string]int{}
		for {
			m, l, ok := g.next()
			if !ok {
				return
			}
			out = append(out, m)
			labels[l]++
		}
	}
	a, la := run()
	b, _ := run()
	if len(a) != len(b) {
		t.Fatal("not deterministic")
	}
	for i := range a {
		if a[i] != b[i] {
			t.Fatalf("diff at %d", i)
		}
	}
	for _, l := range []string{LabelOK, LabelPriceLE0, LabelQtyLE0, LabelReversal, LabelDup} {
		if la[l] == 0 {
			t.Errorf("no %s emitted", l)
		}
	}
	// Two days: ts must cross a local midnight.
	loc := cfg().Loc
	if time.UnixMilli(a[0].Ts).In(loc).YearDay() == time.UnixMilli(a[len(a)-1].Ts).In(loc).YearDay() {
		t.Error("days did not advance")
	}
}

func TestServeDisconnectAndTruth(t *testing.T) {
	f, _ := os.CreateTemp(t.TempDir(), "truth")
	s := &server{g: newGen(cfg()), truth: bufio.NewWriter(f), disconnect: 100}
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	defer ln.Close()
	go s.serve(ln)

	var got []wire.Message
	for conn := 0; conn < 2; conn++ {
		c, err := net.Dial("tcp", ln.Addr().String())
		if err != nil {
			t.Fatal(err)
		}
		r, n := wire.NewReader(c), 0
		for {
			m, err := r.Next()
			if err == io.EOF {
				break
			} else if err != nil {
				t.Fatal(err)
			}
			got, n = append(got, m), n+1
		}
		c.Close()
		if n != 100 {
			t.Fatalf("conn %d got %d msgs, want 100", conn, n)
		}
	}
	time.Sleep(50 * time.Millisecond) // let handler flush truth
	data, _ := os.ReadFile(f.Name())
	if lines := strings.Count(string(data), "\n"); lines != len(got) {
		t.Fatalf("truth lines %d != messages %d", lines, len(got))
	}
}

func TestStartDateTime(t *testing.T) {
	loc, _ := time.LoadLocation("Asia/Seoul")
	st, err := parseStart("2026-03-10", "08:30", loc)
	if err != nil {
		t.Fatal(err)
	}
	c := cfg()
	c.Start = st
	g := newGen(c)
	first, _, _ := g.next()
	if got := time.UnixMilli(first.Ts).In(loc); got.Format("2006-01-02") != "2026-03-10" || got.Hour() != 8 || got.Minute() != 30 {
		t.Fatalf("first tick at %v", got)
	}
	var last wire.Message
	for m, _, ok := g.next(); ok; m, _, ok = g.next() {
		last = m
	}
	if got := time.UnixMilli(last.Ts).In(loc).Format("2006-01-02"); got != "2026-03-11" {
		t.Fatalf("last day %s", got)
	}
	if _, err := parseStart("bad", "09:00", loc); err == nil {
		t.Fatal("want parse error")
	}
}

func BenchmarkGen(b *testing.B) {
	for _, n := range []int{5, 500, 2000} {
		b.Run(strconv.Itoa(n), func(b *testing.B) {
			c := cfg()
			c.Symbols, c.Days, c.PerDay = n, 1, b.N+1
			g := newGen(c)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				g.next()
			}
		})
	}
}
