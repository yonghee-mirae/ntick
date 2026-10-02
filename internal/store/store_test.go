package store

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	_ "modernc.org/sqlite"
)

var drivers = []string{"sqlite"} // modernc.org/sqlite

type tk struct {
	ts, price, qty int64
	valid          int
}

// writeBatch is the PRD 6.1 transaction; the candle upsert is applied per valid tick.
func writeBatch(db *sql.DB, batch []tk, fail bool) error {
	tx, err := db.Begin() // deferred BEGIN is fine: single connection, single writer
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var ph []string
	var args []any
	valid := 0
	for _, t := range batch {
		ph = append(ph, "(?,?,?,?)")
		args = append(args, t.ts, t.price, t.qty, t.valid)
		valid += t.valid
	}
	if _, err := tx.Exec(`INSERT INTO ticks (ts, price, qty, valid) VALUES `+strings.Join(ph, ","), args...); err != nil {
		return err
	}
	if _, err := tx.Exec(`INSERT INTO day_stats (id, valid_count, last_raw_seq)
VALUES (1, ?, last_insert_rowid())
ON CONFLICT (id) DO UPDATE
SET valid_count = valid_count + excluded.valid_count, last_raw_seq = excluded.last_raw_seq`, valid); err != nil {
		return err
	}
	for _, t := range batch {
		if t.valid == 0 {
			continue
		}
		// A new tick always has the largest raw_seq so far, so ts ties keep the stored
		// open (strict <) and move the close (>=); no raw_seq column is needed.
		if _, err := tx.Exec(`INSERT INTO candle_1m (minute, open, high, low, close, open_ts, close_ts, volume, tick_count)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1)
ON CONFLICT (minute) DO UPDATE SET
  open = CASE WHEN excluded.open_ts < open_ts THEN excluded.open ELSE open END,
  open_ts = MIN(open_ts, excluded.open_ts),
  close = CASE WHEN excluded.close_ts >= close_ts THEN excluded.close ELSE close END,
  close_ts = MAX(close_ts, excluded.close_ts),
  high = MAX(high, excluded.high), low = MIN(low, excluded.low),
  volume = volume + excluded.volume, tick_count = tick_count + 1`,
			t.ts/60000%1440, t.price, t.price, t.price, t.price, t.ts, t.ts, t.qty); err != nil {
			return err
		}
	}
	if fail {
		return fmt.Errorf("injected failure")
	}
	return tx.Commit()
}

func one[T any](t *testing.T, db *sql.DB, q string, args ...any) T {
	t.Helper()
	var v T
	if err := db.QueryRow(q, args...).Scan(&v); err != nil {
		t.Fatal(q, err)
	}
	return v
}

func TestSQLite(t *testing.T) {
	for _, drv := range drivers {
		t.Run(drv, func(t *testing.T) {
			db, err := Create(drv, filepath.Join(t.TempDir(), "x.db"))
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()

			ver := one[string](t, db, `SELECT sqlite_version()`)
			t.Logf("%s sqlite_version=%s", drv, ver)
			var maj, min int
			fmt.Sscanf(ver, "%d.%d", &maj, &min)
			if maj < 3 || (maj == 3 && min < 25) {
				t.Fatalf("sqlite %s < 3.25", ver)
			}
			if m := one[string](t, db, `PRAGMA journal_mode`); m != "wal" {
				t.Fatalf("journal_mode=%s", m)
			}

			// minute 0: ts 1000 (p100), 500 (p90, earlier), 1000 (p110, tie, arrives later), 2000 (p120)
			// minute 1: ts 60000 (p50 invalid) is excluded from the candle.
			b1 := []tk{{1000, 100, 1, 1}, {500, 90, 2, 1}, {60000, 50, 1, 0}}
			b2 := []tk{{1000, 110, 3, 1}, {2000, 120, 4, 1}}
			if err := writeBatch(db, b1, false); err != nil {
				t.Fatal(err)
			}
			if err := writeBatch(db, b2, false); err != nil {
				t.Fatal(err)
			}
			if vc, last := one[int](t, db, `SELECT valid_count FROM day_stats`), one[int](t, db, `SELECT last_raw_seq FROM day_stats`); vc != 4 || last != 5 {
				t.Fatalf("day_stats valid_count=%d last_raw_seq=%d", vc, last)
			}
			var o, h, l, c, ots, cts, vol, n int
			if err := db.QueryRow(`SELECT open,high,low,close,open_ts,close_ts,volume,tick_count FROM candle_1m WHERE minute=0`).
				Scan(&o, &h, &l, &c, &ots, &cts, &vol, &n); err != nil {
				t.Fatal(err)
			}
			if o != 90 || ots != 500 || c != 120 || cts != 2000 || h != 120 || l != 90 || vol != 10 || n != 4 {
				t.Fatalf("candle o=%d h=%d l=%d c=%d ots=%d cts=%d v=%d n=%d", o, h, l, c, ots, cts, vol, n)
			}
			if one[int](t, db, `SELECT COUNT(*) FROM candle_1m`) != 1 {
				t.Fatal("invalid tick created a candle")
			}

			// Rollback leaves everything untouched; next insert continues at max(rowid)+1.
			if err := writeBatch(db, []tk{{3000, 130, 1, 1}, {3001, 131, 1, 1}}, true); err == nil {
				t.Fatal("expected failure")
			}
			if one[int](t, db, `SELECT COUNT(*) FROM ticks`) != 5 || one[int](t, db, `SELECT valid_count FROM day_stats`) != 4 ||
				one[int](t, db, `SELECT tick_count FROM candle_1m WHERE minute=0`) != 4 {
				t.Fatal("rollback leaked state")
			}
			if err := writeBatch(db, []tk{{3000, 130, 1, 1}}, false); err != nil {
				t.Fatal(err)
			}
			if got := one[int](t, db, `SELECT MAX(raw_seq) FROM ticks`); got != 6 {
				t.Fatalf("raw_seq after rollback=%d, want 6", got)
			}

			// PRD 5.2 query must use the partial index.
			rows, err := db.Query(`EXPLAIN QUERY PLAN SELECT price, qty FROM ticks WHERE valid = 1 AND raw_seq <= ? ORDER BY raw_seq DESC LIMIT ?`, 6, 3)
			if err != nil {
				t.Fatal(err)
			}
			defer rows.Close()
			var plan []string
			for rows.Next() {
				var a, b, c int
				var d string
				if err := rows.Scan(&a, &b, &c, &d); err != nil {
					t.Fatal(err)
				}
				plan = append(plan, d)
			}
			t.Log("plan:", plan)
			if !strings.Contains(strings.Join(plan, ";"), "ix_ticks_valid") {
				t.Fatalf("index not used: %v", plan)
			}
		})
	}
}

func BenchmarkBatch(b *testing.B) {
	for _, drv := range drivers {
		b.Run(drv, func(b *testing.B) {
			db, err := Create(drv, filepath.Join(b.TempDir(), "x.db"))
			if err != nil {
				b.Fatal(err)
			}
			defer db.Close()
			batch := make([]tk, 200)
			for i := range batch {
				batch[i] = tk{int64(i * 100), 100 + int64(i%7), 1, 1}
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := writeBatch(db, batch, false); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(float64(b.N*200)/b.Elapsed().Seconds(), "ticks/s")
		})
	}
}

// Pragmas must hold on every connection of the pool, not only the first.
func TestPragmasOnEveryConnection(t *testing.T) {
	db, err := Create(Driver, filepath.Join(t.TempDir(), "x.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(2)
	ctx := context.Background()
	c1, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer c1.Close()
	c2, err := db.Conn(ctx) // a second, distinct connection
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Close()
	for i, c := range []*sql.Conn{c1, c2} {
		var bt, sync int
		var jm string
		c.QueryRowContext(ctx, "PRAGMA busy_timeout").Scan(&bt)
		c.QueryRowContext(ctx, "PRAGMA synchronous").Scan(&sync)
		c.QueryRowContext(ctx, "PRAGMA journal_mode").Scan(&jm)
		if bt != 5000 || sync != 1 || jm != "wal" {
			t.Errorf("conn %d: busy_timeout=%d synchronous=%d journal_mode=%s", i, bt, sync, jm)
		}
	}
}

// A failure between building the temp file and the rename leaves no final file,
// and a retry succeeds.
func TestCreateAtomic(t *testing.T) {
	path := filepath.Join(t.TempDir(), "x.db")
	BeforeRename = func() error { return fmt.Errorf("injected") }
	if _, err := Create(Driver, path); err == nil {
		t.Fatal("create succeeded despite injected failure")
	}
	BeforeRename = nil
	for _, p := range []string{path, path + ".tmp", path + ".tmp-journal"} {
		if _, err := os.Stat(p); err == nil {
			t.Errorf("%s exists after failed create", p)
		}
	}
	os.WriteFile(path+".tmp", []byte("stale"), 0o644)
	db, err := Create(Driver, path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var n int
	if err := db.QueryRow(`SELECT count(*) FROM day_stats`).Scan(&n); err != nil {
		t.Errorf("schema missing: %v", err)
	}
}
