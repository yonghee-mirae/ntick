package dayindex

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand"
	"path/filepath"
	"testing"
	"time"

	"ntick/internal/ingest"
	"ntick/internal/query"
	"ntick/internal/store"
	"ntick/internal/tick"
)

var seoul, _ = time.LoadLocation("Asia/Seoul")

// ingestDay writes a deterministic mixed stream (invalid, reversals, dups) for 2026-01-05.
func ingestDay(t *testing.T, dir string) {
	t.Helper()
	base := time.Date(2026, 1, 5, 9, 0, 0, 0, seoul).UnixMilli()
	rng := rand.New(rand.NewSource(1))
	in := ingest.New(dir, seoul)
	for i := 0; i < 200; i++ {
		tk := tick.Tick{Symbol: []string{"A", "B"}[i%2], TS: base + int64(rng.Intn(180000)), Price: int64(90 + rng.Intn(20)), Qty: int64(1 + rng.Intn(5))}
		if i%17 == 0 {
			tk.Price = 0
		}
		in.Put(tk)
	}
	in.Close()
}

func indexRows(t *testing.T, dir string) map[string]int64 {
	db, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	rows, err := db.Query(`SELECT symbol || '/' || date, valid_count FROM day_index`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	m := map[string]int64{}
	for rows.Next() {
		var k string
		var v int64
		rows.Scan(&k, &v)
		m[k] = v
	}
	return m
}

func TestCloseoutDriftAndIdempotent(t *testing.T) {
	dir := t.TempDir()
	ingestDay(t, dir)
	reps, err := Closeout(dir, "20260105")
	if err != nil || len(reps) != 2 {
		t.Fatalf("reps=%d err=%v", len(reps), err)
	}
	for _, r := range reps {
		if len(r.Drift) != 0 {
			t.Errorf("unexpected drift %+v", r)
		}
	}
	first := indexRows(t, dir)
	if len(first) != 2 || first["A/20260105"] == 0 {
		t.Fatalf("index = %v", first)
	}
	// second run is a no-op
	Closeout(dir, "20260105")
	if got := indexRows(t, dir); len(got) != 2 || got["A/20260105"] != first["A/20260105"] {
		t.Errorf("not idempotent: %v vs %v", got, first)
	}

	// tamper: counter, last_raw_seq, candle count
	path := filepath.Join(dir, "20260105", "A.db")
	db, _ := sql.Open(store.Driver, path)
	db.Exec(`UPDATE day_stats SET valid_count = valid_count + 3, last_raw_seq = last_raw_seq + 1`)
	db.Close()
	reps, _ = Closeout(dir, "20260105")
	for _, r := range reps {
		if (r.Symbol == "A") != (len(r.Drift) == 3) || (r.Symbol == "B" && len(r.Drift) != 0) {
			t.Errorf("%s drift = %v", r.Symbol, r.Drift)
		}
	}
	if got := indexRows(t, dir)["A/20260105"]; got != first["A/20260105"]+3 {
		t.Errorf("index must follow day_stats absolutely, got %d", got)
	}
}

func TestCatchUp(t *testing.T) {
	dir := t.TempDir()
	ingestDay(t, dir)
	today := time.Date(2026, 1, 5, 12, 0, 0, 0, seoul)
	if reps, err := CatchUp(dir, seoul, today); err != nil || len(reps) != 0 {
		t.Errorf("today must not be finalized: %d %v", len(reps), err)
	}
	tomorrow := today.AddDate(0, 0, 1)
	if reps, err := CatchUp(dir, seoul, tomorrow); err != nil || len(reps) != 2 {
		t.Errorf("catch-up: %d %v", len(reps), err)
	}
	if reps, _ := CatchUp(dir, seoul, tomorrow); len(reps) != 0 {
		t.Errorf("already indexed files must be skipped, got %d", len(reps))
	}
}

func TestNextClose(t *testing.T) {
	at := func(h, m int) time.Time { return time.Date(2026, 1, 5, h, m, 0, 0, seoul) }
	if n, _ := NextClose(at(19, 59), seoul, "20:00"); !n.Equal(at(20, 0)) {
		t.Errorf("before close: %v", n)
	}
	if n, _ := NextClose(at(20, 0), seoul, "20:00"); !n.Equal(at(20, 0).AddDate(0, 0, 1)) {
		t.Errorf("at close: %v", n)
	}
	if _, err := NextClose(at(1, 0), seoul, "bad"); err == nil {
		t.Error("bad time accepted")
	}
}

// Readers listing the directory while new day files are being created must
// never see a file without schema.
func TestReadersDuringFileCreation(t *testing.T) {
	dir := t.TempDir()
	stop := make(chan struct{})
	errc := make(chan error, 1)
	report := func(err error) {
		select {
		case errc <- err:
		default:
		}
	}
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		for {
			select {
			case <-stop:
				return
			default:
			}
			if _, err := query.NTick(context.Background(), dir, "S0", 5, 3); err != nil {
				report(fmt.Errorf("NTick: %w", err))
			}
			if _, err := query.Time(context.Background(), dir, "S0", seoul, 1, 0, 1<<60); err != nil {
				report(fmt.Errorf("Time: %w", err))
			}
			for d := 5; d < 25; d++ {
				if _, err := Closeout(dir, fmt.Sprintf("202601%02d", d)); err != nil {
					report(fmt.Errorf("Closeout: %w", err))
				}
			}
		}
	}()
	in := ingest.New(dir, seoul)
	for day := 5; day < 25; day++ {
		base := time.Date(2026, 1, day, 9, 0, 0, 0, seoul).UnixMilli()
		for s := 0; s < 20; s++ {
			in.Put(tick.Tick{Symbol: fmt.Sprintf("S%d", s), TS: base, Price: 10, Qty: 1})
		}
	}
	in.Close()
	close(stop)
	<-finished
	select {
	case err := <-errc:
		t.Fatal(err)
	default:
	}
}
