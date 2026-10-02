package query

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"ntick/internal/oracle"
	"ntick/internal/store"
)

func TestPlan(t *testing.T) {
	for _, c := range []struct{ t, n, rem, take, r, l int }{
		{10, 5, 100, 2, 5, 10}, // r == n
		{3, 5, 100, 1, 3, 3},   // T < n
		{12, 5, 100, 3, 2, 12},
		{12, 5, 2, 2, 2, 7}, // m limits take
		{12, 5, 1, 1, 2, 2},
		{0, 5, 3, 0, 0, 0}, // empty day
		{4, 1, 3, 3, 1, 3}, // n = 1
	} {
		take, r, l := plan(c.t, c.n, c.rem)
		if take != c.take || r != c.r || l != c.l {
			t.Errorf("%+v: got %d %d %d", c, take, r, l)
		}
	}
}

func openDay(t testing.TB, dir, date, sym string) *sql.DB {
	t.Helper()
	if err := os.MkdirAll(filepath.Join(dir, date), 0o755); err != nil {
		t.Fatal(err)
	}
	db, err := store.Create(store.Driver, filepath.Join(dir, date, sym+".db"))
	if err != nil {
		t.Fatal(err)
	}
	return db
}

// insert is the PRD 6.1 transaction without the candle upsert (not used by queries).
func insert(db *sql.DB, ticks []oracle.Tick) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	valid := 0
	for _, k := range ticks {
		v := 0
		if k.Valid {
			v, valid = 1, valid+1
		}
		if _, err := tx.Exec(`INSERT INTO ticks (ts, price, qty, valid) VALUES (?,?,?,?)`, k.TS, k.Price, k.Qty, v); err != nil {
			return err
		}
	}
	if _, err := tx.Exec(`INSERT INTO day_stats (id, valid_count, last_raw_seq) VALUES (1, ?, last_insert_rowid())
ON CONFLICT (id) DO UPDATE SET valid_count = valid_count + excluded.valid_count, last_raw_seq = excluded.last_raw_seq`, valid); err != nil {
		return err
	}
	return tx.Commit()
}

func randTicks(rng *rand.Rand, cnt int) []oracle.Tick {
	ts := make([]oracle.Tick, cnt)
	for i := range ts {
		ts[i] = oracle.Tick{TS: int64(i), Price: 1 + rng.Int63n(1000), Qty: 1 + rng.Int63n(50), Valid: rng.Intn(4) != 0}
	}
	return ts
}

func validOnly(ts []oracle.Tick) []oracle.Tick {
	var v []oracle.Tick
	for _, x := range ts {
		if x.Valid {
			v = append(v, x)
		}
	}
	return v
}

func TestAgainstOracle(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	dir := t.TempDir()
	dates := []string{"20260101", "20260102", "20260103", "20260105"} // 0103: no valid ticks
	var days [][]oracle.Tick
	for i, d := range dates {
		cnt := 1 + rng.Intn(80)
		ts := randTicks(rng, cnt)
		if i == 2 {
			for j := range ts {
				ts[j].Valid = false
			}
		}
		db := openDay(t, dir, d, "A")
		if err := insert(db, ts); err != nil {
			t.Fatal(err)
		}
		db.Close()
		days = append(days, validOnly(ts))
	}
	openDay(t, dir, "20260104", "B").Close() // other symbol only; must be ignored
	for i := 0; i < 300; i++ {
		n, m := 1+rng.Intn(30), 1+rng.Intn(40)
		got, err := NTick(context.Background(), dir, "A", n, m)
		if err != nil {
			t.Fatal(err)
		}
		want := oracle.NTick(days, n, m)
		if len(got) != len(want) {
			t.Fatalf("n=%d m=%d: %d candles, want %d", n, m, len(got), len(want))
		}
		for j, w := range want {
			g := got[j]
			if g.Open != w.Open || g.High != w.High || g.Low != w.Low || g.Close != w.Close ||
				g.Volume != w.Volume || int64(g.TickCount) != w.Count || g.Partial != (w.Count < int64(n)) {
				t.Fatalf("n=%d m=%d #%d: got %+v want %+v", n, m, j, g, w)
			}
		}
	}
}

func TestRowsReadBound(t *testing.T) {
	rng := rand.New(rand.NewSource(2))
	dir := t.TempDir()
	for _, d := range []string{"20260101", "20260102", "20260103"} {
		db := openDay(t, dir, d, "A")
		if err := insert(db, randTicks(rng, 500)); err != nil {
			t.Fatal(err)
		}
		db.Close()
	}
	for i := 0; i < 100; i++ {
		n, m := 1+rng.Intn(60), 1+rng.Intn(40)
		rowsRead.Store(0)
		got, err := NTick(context.Background(), dir, "A", n, m)
		if err != nil {
			t.Fatal(err)
		}
		if r := rowsRead.Load(); r > int64(m*n) {
			t.Fatalf("n=%d m=%d: read %d rows > %d", n, m, r, m*n)
		}
		if len(got) > m {
			t.Fatalf("got %d > m=%d", len(got), m)
		}
	}
}

func TestLimits(t *testing.T) {
	for _, c := range [][2]int{{0, 1}, {1, 0}, {MaxN + 1, 1}, {1, MaxM + 1}} {
		if _, err := NTick(context.Background(), t.TempDir(), "A", c[0], c[1]); err == nil {
			t.Errorf("n=%d m=%d: want error", c[0], c[1])
		}
	}
}

func TestMaxDays(t *testing.T) {
	dir := t.TempDir()
	for i := 1; i <= MaxDays+5; i++ {
		db := openDay(t, dir, fmt.Sprintf("202601%02d", i), "A")
		if err := insert(db, []oracle.Tick{{TS: 1, Price: 1, Qty: 1, Valid: true}}); err != nil {
			t.Fatal(err)
		}
		db.Close()
	}
	got, err := NTick(context.Background(), dir, "A", 1, MaxM)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != MaxDays || got[0].Date != "20260135" {
		t.Fatalf("got %d candles", len(got))
	}
}

func TestConcurrentWriter(t *testing.T) {
	dir := t.TempDir()
	const n = 7
	oldDB := openDay(t, dir, "20260101", "A")
	if err := insert(oldDB, randTicks(rand.New(rand.NewSource(3)), 50)); err != nil {
		t.Fatal(err)
	}
	oldDB.Close()
	db := openDay(t, dir, "20260102", "A")
	defer db.Close()
	if err := insert(db, []oracle.Tick{{TS: 1, Price: 1, Qty: 1, Valid: true}}); err != nil {
		t.Fatal(err)
	}

	stop := make(chan struct{})
	var wg sync.WaitGroup
	var werr error
	wg.Add(1)
	go func() {
		defer wg.Done()
		rng := rand.New(rand.NewSource(4))
		for {
			select {
			case <-stop:
				return
			default:
			}
			if err := insert(db, randTicks(rng, 1+rng.Intn(10))); err != nil {
				werr = err
				return
			}
		}
	}()
	for i := 0; i < 40; i++ {
		got, err := NTick(context.Background(), dir, "A", n, 1000)
		if err != nil {
			t.Fatal(err)
		}
		// Within a day only the newest candle of a day may be partial; ticks are never double counted.
		seen := map[string]int{}
		for j, c := range got {
			if c.Partial && j > 0 && got[j-1].Date == c.Date {
				t.Fatalf("partial candle #%d not last of its day", j)
			}
			if !c.Partial && c.TickCount != n {
				t.Fatalf("candle #%d has %d ticks", j, c.TickCount)
			}
			seen[c.Date] += c.TickCount
		}
		if seen["20260101"] == 0 && len(got) < 1000 {
			t.Fatal("lost previous day")
		}
	}
	close(stop)
	wg.Wait()
	if werr != nil {
		t.Fatal(werr)
	}
}

func TestNTickCancel(t *testing.T) {
	dir := t.TempDir()
	db := openDay(t, dir, "20260101", "A")
	if _, err := db.Exec(`WITH RECURSIVE s(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM s WHERE i < 200000)
INSERT INTO ticks (ts, price, qty, valid) SELECT i, i%100+1, 1, 1 FROM s`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO day_stats (id, valid_count, last_raw_seq) VALUES (1, 200000, 200000)`); err != nil {
		t.Fatal(err)
	}
	db.Close()

	pre, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := NTick(pre, dir, "A", 10000, 100); !errors.Is(err, context.Canceled) {
		t.Fatalf("pre-cancelled: %v", err)
	}

	// Cancel mid-scan; the full scan of 200k rows takes far longer than the cancel delay.
	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(5*time.Millisecond, cancel)
	start := time.Now()
	_, err := NTick(ctx, dir, "A", 10000, 100)
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("returned after %v", d)
	}
	if err == nil {
		t.Log("scan finished before the cancel took effect")
	} else if !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v", err)
	}
}
