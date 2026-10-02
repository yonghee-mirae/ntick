package ingest

import (
	"context"
	"fmt"
	"math"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"ntick/internal/store"
	"ntick/internal/tick"
	"ntick/internal/wire"
)

// Junk symbols and timestamps are dropped before any file or directory is created.
func TestPutRejectsJunk(t *testing.T) {
	root := t.TempDir()
	data := filepath.Join(root, "data")
	if err := os.Mkdir(data, 0o755); err != nil {
		t.Fatal(err)
	}
	in := New(data, seoul)
	ok := ms(2026, 1, 5, 9, 0, 0, 0)
	for _, tk := range []tick.Tick{
		{Symbol: "../../x", TS: ok, Price: 1, Qty: 1},
		{Symbol: "", TS: ok, Price: 1, Qty: 1},
		{Symbol: "A\x00", TS: ok, Price: 1, Qty: 1},
		{Symbol: "a/b", TS: ok, Price: 1, Qty: 1},
		{Symbol: "A", TS: 0, Price: 1, Qty: 1},
		{Symbol: "A", TS: -1, Price: 1, Qty: 1},
		{Symbol: "A", TS: math.MaxInt64, Price: 1, Qty: 1},
		{Symbol: "A", TS: minTS - 1, Price: 1, Qty: 1},
		{Symbol: "A", TS: maxTS, Price: 1, Qty: 1},
	} {
		in.Put(tk)
	}
	in.Close()
	if in.bad.Load() != 9 {
		t.Errorf("rejected = %d, want 9", in.bad.Load())
	}
	if ents, _ := os.ReadDir(root); len(ents) != 1 {
		t.Errorf("entries outside data dir: %v", ents)
	}
	if ents, _ := os.ReadDir(data); len(ents) != 0 {
		t.Errorf("entries in data dir: %v", ents)
	}
}

// Bad frames on the wire are skipped, the stream stays aligned, good ticks land.
func TestBadFramesSkipped(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	good, _ := wire.Encode(wire.Message{Symbol: "G", Ts: ms(2026, 1, 5, 9, 0, 0, 0), Price: 5, Qty: 1})
	traversal := append([]byte(nil), good...)
	copy(traversal[2:14], "../../x     ")
	badTS, _ := wire.Encode(wire.Message{Symbol: "G", Ts: 0, Price: 5, Qty: 1})
	go func() {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		defer c.Close()
		c.Write(traversal)
		c.Write(badTS)
		c.Write(good)
	}()
	root := t.TempDir()
	data := filepath.Join(root, "data")
	os.Mkdir(data, 0o755)
	in := New(data, seoul)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { in.Serve(ctx, ln.Addr().String()); close(done) }()
	for end := time.Now().Add(10 * time.Second); time.Now().Before(end) && !committed(data, "20260105", "G"); {
		time.Sleep(20 * time.Millisecond)
	}
	cancel()
	<-done
	in.Close()
	if !committed(data, "20260105", "G") || in.bad.Load() != 2 {
		t.Errorf("good tick missing or rejected = %d, want 2", in.bad.Load())
	}
	if ents, _ := os.ReadDir(root); len(ents) != 1 {
		t.Errorf("entries outside data dir: %v", ents)
	}
	if ents, _ := os.ReadDir(data); len(ents) != 1 {
		t.Errorf("data dir entries: %v", ents)
	}
}

func TestSizePool(t *testing.T) {
	for _, c := range []struct {
		workers int
		soft    uint64
		want    int
	}{{4, 1 << 20, maxOpenFiles}, {16, 1024, 10}, {64, 1024, minOpenFiles}} {
		if got := sizePool(c.workers, c.soft); got != c.want {
			t.Errorf("sizePool(%d, %d) = %d, want %d", c.workers, c.soft, got, c.want)
		}
	}
}

// More symbols than pool slots: nothing is lost and counters stay consistent.
func TestPoolEviction(t *testing.T) {
	dir := t.TempDir()
	w := newWorker(dir, seoul)
	w.maxFiles = 2
	const syms, rounds = 10, 3
	base := ms(2026, 1, 5, 9, 0, 0, 0)
	for r := 0; r < rounds; r++ {
		var batch []tick.Tick
		for s := 0; s < syms; s++ {
			for k := 0; k <= s; k++ { // symbol s gets s+1 ticks per round
				batch = append(batch, tick.Tick{Symbol: fmt.Sprintf("S%d", s), TS: base + int64(r*1000+k), Price: 10, Qty: 1})
			}
		}
		w.commit(batch)
		if w.lru.Len() > 2 || len(w.pool) > 2 {
			t.Fatalf("pool size %d/%d exceeds 2", w.lru.Len(), len(w.pool))
		}
	}
	w.closeAll()
	for s := 0; s < syms; s++ {
		db := openDB(t, filepath.Join(dir, "20260105", fmt.Sprintf("S%d.db", s)))
		want := int64(rounds * (s + 1))
		var n, vc, last, cs int64
		db.QueryRow("SELECT count(*) FROM ticks").Scan(&n)
		db.QueryRow("SELECT valid_count, last_raw_seq FROM day_stats").Scan(&vc, &last)
		db.QueryRow("SELECT sum(tick_count) FROM candle_1m").Scan(&cs)
		if n != want || vc != want || last != want || cs != want {
			t.Errorf("S%d: ticks=%d valid_count=%d last=%d candles=%d, want %d", s, n, vc, last, cs, want)
		}
	}
}

// A failure in the middle of the transaction (after the tick inserts) leaves
// nothing behind; the batch is retried once and then fatal is called.
func TestMidTransactionFailureIsAtomic(t *testing.T) {
	dir := t.TempDir()
	w := newWorker(dir, seoul)
	var fatals int
	w.fatal = func(error) { fatals++ }
	base := ms(2026, 1, 5, 9, 0, 0, 0)
	k := fileKey{"20260105", "A"}
	w.commit([]tick.Tick{{Symbol: "A", TS: base, Price: 10, Qty: 1}})
	db, err := w.open(k)
	if err != nil {
		t.Fatal(err)
	}
	snap := func() (s string) {
		for _, q := range []string{
			"SELECT count(*) FROM ticks", "SELECT valid_count || '/' || last_raw_seq FROM day_stats",
			"SELECT count(*) || '/' || sum(tick_count) FROM candle_1m",
		} {
			var v string
			if err := db.QueryRow(q).Scan(&v); err != nil {
				t.Fatal(err)
			}
			s += v + " "
		}
		return
	}
	before := snap()
	// Fails on the candle upsert, after ticks and day_stats were already written in the tx.
	if _, err := db.Exec(`CREATE TRIGGER boom BEFORE INSERT ON candle_1m BEGIN SELECT RAISE(ABORT, 'boom'); END`); err != nil {
		t.Fatal(err)
	}
	w.commit([]tick.Tick{
		{Symbol: "A", TS: base + 120000, Price: 11, Qty: 1},
		{Symbol: "A", TS: base + 121000, Price: 12, Qty: 1},
	})
	if fatals != 1 {
		t.Errorf("fatal calls = %d, want 1", fatals)
	}
	if after := snap(); after != before {
		t.Errorf("state changed by failed batch: %q -> %q", before, after)
	}
	// the connection is usable and not left inside a transaction
	if _, err := db.Exec(`DROP TRIGGER boom`); err != nil {
		t.Fatal(err)
	}
	w.commit([]tick.Tick{{Symbol: "A", TS: base + 120000, Price: 11, Qty: 1}})
	if snap() == before || fatals != 1 {
		t.Errorf("worker did not recover: %q fatals=%d", snap(), fatals)
	}
	w.closeAll()
}

// A create failure before the rename leaves no file; a later ingest of the same
// symbol and day succeeds.
func TestCreateFailureThenRetry(t *testing.T) {
	dir := t.TempDir()
	w := newWorker(dir, seoul)
	var fatals int
	w.fatal = func(error) { fatals++ }
	b := []tick.Tick{{Symbol: "A", TS: ms(2026, 1, 5, 9, 0, 0, 0), Price: 1, Qty: 1}}
	store.BeforeRename = func() error { return fmt.Errorf("injected") }
	w.commit(b)
	store.BeforeRename = nil
	if fatals != 1 {
		t.Errorf("fatal calls = %d, want 1", fatals)
	}
	if _, err := os.Stat(filepath.Join(dir, "20260105", "A.db")); err == nil {
		t.Error("final file exists after failed create")
	}
	w.commit(b)
	w.closeAll()
	if n := count(t, openDB(t, filepath.Join(dir, "20260105", "A.db")), "SELECT count(*) FROM ticks"); n != 1 {
		t.Errorf("ticks = %d, want 1", n)
	}
}
