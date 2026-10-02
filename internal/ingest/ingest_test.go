package ingest

import (
	"context"
	"database/sql"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"ntick/internal/store"
	"ntick/internal/tick"
	"ntick/internal/wire"
)

var seoul = func() *time.Location {
	l, err := time.LoadLocation("Asia/Seoul")
	if err != nil {
		panic(err)
	}
	return l
}()

func ms(y int, mo time.Month, d, h, mi, s, milli int) int64 {
	return time.Date(y, mo, d, h, mi, s, milli*1e6, seoul).UnixMilli()
}

func TestSplitByDate(t *testing.T) {
	b := []tick.Tick{
		{Symbol: "A", TS: ms(2026, 1, 5, 23, 59, 59, 999), Price: 10, Qty: 1},
		{Symbol: "A", TS: ms(2026, 1, 6, 0, 0, 0, 0), Price: 11, Qty: 1},
		{Symbol: "A", TS: ms(2026, 1, 5, 23, 59, 59, 999), Price: 12, Qty: 1}, // reversal back into day 1
		{Symbol: "B", TS: ms(2026, 1, 6, 0, 1, 0, 0), Price: 0, Qty: 1},       // invalid
	}
	g := split(b, tick.DefaultFilter(), seoul)
	if len(g) != 3 {
		t.Fatalf("groups = %d, want 3", len(g))
	}
	if g[0].key != (fileKey{"20260105", "A"}) || len(g[0].recs) != 2 || g[0].recs[0].Price != 10 || g[0].recs[1].Price != 12 {
		t.Errorf("group 0 = %+v", g[0])
	}
	if g[0].recs[0].minute != 1439 || g[1].recs[0].minute != 0 {
		t.Errorf("minutes = %d, %d", g[0].recs[0].minute, g[1].recs[0].minute)
	}
	if g[1].key != (fileKey{"20260106", "A"}) || g[2].key != (fileKey{"20260106", "B"}) || g[2].recs[0].valid {
		t.Errorf("groups 1,2 = %+v %+v", g[1], g[2])
	}
}

func openDB(t *testing.T, path string) *sql.DB {
	db, err := sql.Open(store.Driver, path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

// Candle open/close follow ts, ties by arrival order, also across batches.
func TestCandleOpenClose(t *testing.T) {
	dir := t.TempDir()
	w := newWorker(dir, seoul)
	base := ms(2026, 1, 5, 9, 0, 0, 0)
	k := fileKey{"20260105", "A"}
	mk := func(off, price, qty int64) rec {
		return rec{Tick: tick.Tick{Symbol: "A", TS: base + off, Price: price, Qty: qty}, valid: true, date: k.date, minute: 540}
	}
	// batch 1: reversal (ts 5 arrives after ts 20) and an invalid tick
	bad := mk(1, 999, 1)
	bad.valid = false
	if err := w.write(k, []rec{mk(20, 100, 1), mk(5, 90, 1), bad, mk(20, 105, 2)}); err != nil {
		t.Fatal(err)
	}
	// batch 2: same ts as stored open (5) and stored close (20), later arrival
	if err := w.write(k, []rec{mk(5, 91, 1), mk(20, 106, 1), mk(10, 120, 1)}); err != nil {
		t.Fatal(err)
	}
	w.closeAll()

	var o, h, l, c, ot, ct, v, n int64
	err := openDB(t, filepath.Join(dir, k.date, "A.db")).QueryRow(
		`SELECT open, high, low, close, open_ts, close_ts, volume, tick_count FROM candle_1m WHERE minute = 540`).
		Scan(&o, &h, &l, &c, &ot, &ct, &v, &n)
	if err != nil {
		t.Fatal(err)
	}
	// open: first arrival among ts=5 (90); close: last arrival among ts=20 (106)
	if o != 90 || c != 106 || h != 120 || l != 90 || ot != base+5 || ct != base+20 || v != 7 || n != 6 {
		t.Errorf("candle = %d %d %d %d %d %d %d %d", o, h, l, c, ot, ct, v, n)
	}
}

func count(t *testing.T, db *sql.DB, q string) (n int64) {
	if err := db.QueryRow(q).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return
}

func TestIngestOverTCP(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	d1 := func(h, mi, s, milli int) int64 { return ms(2026, 1, 5, h, mi, s, milli) }
	msgs := []wire.Message{
		{Symbol: "A", Ts: d1(9, 0, 20, 0), Price: 100, Qty: 1},
		{Symbol: "A", Ts: d1(9, 0, 30, 0), Price: 110, Qty: 2},
		{Symbol: "A", Ts: d1(9, 0, 31, 0), Price: 0, Qty: 5},   // invalid price
		{Symbol: "A", Ts: d1(9, 0, 10, 0), Price: 90, Qty: 1},  // ts reversal, valid
		{Symbol: "A", Ts: d1(9, 0, 30, 0), Price: 110, Qty: 2}, // same-ms duplicate, kept
		{Symbol: "A", Ts: d1(23, 59, 59, 999), Price: 120, Qty: 1},
		{Symbol: "A", Ts: ms(2026, 1, 6, 0, 0, 0, 0), Price: 130, Qty: 1}, // next local day
		{Symbol: "B", Ts: d1(9, 0, 0, 0), Price: 50, Qty: 0},              // invalid qty
	}
	go func() {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		defer c.Close()
		for _, m := range msgs {
			b, _ := wire.Encode(m)
			c.Write(b)
		}
	}()

	dir := t.TempDir()
	in := New(dir, seoul)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { in.Serve(ctx, ln.Addr().String()); close(done) }()

	// Wait until the last message of each symbol is committed, then shut down.
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if committed(dir, "20260106", "A") && committed(dir, "20260105", "B") {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	cancel()
	<-done
	in.Close()

	a5 := openDB(t, filepath.Join(dir, "20260105", "A.db"))
	if n := count(t, a5, "SELECT count(*) FROM ticks"); n != 6 {
		t.Errorf("A 0105 ticks = %d, want 6", n)
	}
	if n := count(t, a5, "SELECT count(*) FROM ticks WHERE valid = 0"); n != 1 {
		t.Errorf("A 0105 invalid = %d, want 1", n)
	}
	var vc, last int64
	a5.QueryRow("SELECT valid_count, last_raw_seq FROM day_stats").Scan(&vc, &last)
	if vc != 5 || last != 6 {
		t.Errorf("A 0105 day_stats = %d, %d, want 5, 6", vc, last)
	}
	var o, c, tc int64
	a5.QueryRow("SELECT open, close, tick_count FROM candle_1m WHERE minute = 540").Scan(&o, &c, &tc)
	if o != 90 || c != 110 || tc != 4 {
		t.Errorf("A 0105 candle 540 = %d %d %d, want 90 110 4", o, c, tc)
	}
	if n := count(t, a5, "SELECT tick_count FROM candle_1m WHERE minute = 1439"); n != 1 {
		t.Errorf("A 0105 candle 1439 count = %d", n)
	}
	a6 := openDB(t, filepath.Join(dir, "20260106", "A.db"))
	if n := count(t, a6, "SELECT count(*) FROM ticks"); n != 1 {
		t.Errorf("A 0106 ticks = %d, want 1", n)
	}
	if n := count(t, a6, "SELECT tick_count FROM candle_1m WHERE minute = 0"); n != 1 {
		t.Errorf("A 0106 candle 0 count = %d", n)
	}
	b5 := openDB(t, filepath.Join(dir, "20260105", "B.db"))
	if n := count(t, b5, "SELECT valid_count FROM day_stats"); n != 0 {
		t.Errorf("B valid_count = %d, want 0", n)
	}
	if n := count(t, b5, "SELECT count(*) FROM candle_1m"); n != 0 {
		t.Errorf("B candles = %d, want 0", n)
	}
}

// committed reports whether the file has at least one tick row.
func committed(dir, date, sym string) bool {
	db, err := sql.Open(store.Driver, filepath.Join(dir, date, sym+".db"))
	if err != nil {
		return false
	}
	defer db.Close()
	var n int
	return db.QueryRow("SELECT count(*) FROM ticks").Scan(&n) == nil && n > 0
}

// A batch that cannot be committed is retried once, then fatal is called.
func TestCommitFailureRetriesThenFatal(t *testing.T) {
	blocker := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(blocker, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	w := newWorker(blocker, seoul) // data dir is a regular file, so MkdirAll always fails
	var fatals int
	w.fatal = func(error) { fatals++ }
	w.commit([]tick.Tick{{Symbol: "A", TS: ms(2026, 1, 5, 9, 0, 0, 0), Price: 1, Qty: 1}})
	if fatals != 1 {
		t.Errorf("fatal calls = %d, want 1", fatals)
	}
}
