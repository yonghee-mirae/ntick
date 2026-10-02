// Package rejudge re-applies the current receive filter to the ticks of past
// day files (PRD 6.3).
package rejudge

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"ntick/internal/dayindex"
	"ntick/internal/store"
	"ntick/internal/tick"
)

// Summary describes one file. Valid* count valid ticks before/after.
type Summary struct {
	Date, Symbol            string
	ToInvalid, ToValid      int
	ValidBefore, ValidAfter int64
}

// Rejudge re-judges dataDir/{date}/{symbol}.db (all files if symbol is "") with
// filter() as a fresh filter per file, in raw_seq order. It refuses dates that
// are not before today in loc. Each file is one transaction; with dryRun it is
// rolled back and day_index is untouched.
//
// day_index lives in another file, so it is updated after the file commit.
// If that fails the file stays authoritative; dayindex.Closeout/CatchUp
// rewrites the index from day_stats.
func Rejudge(dataDir string, loc *time.Location, date, symbol string, now time.Time, dryRun bool, filter func() *tick.Filter) ([]Summary, error) {
	if _, err := time.ParseInLocation("20060102", date, loc); err != nil {
		return nil, fmt.Errorf("bad date %q: %w", date, err)
	}
	if date >= now.In(loc).Format("20060102") {
		return nil, fmt.Errorf("refusing to re-judge %s: only dates before today are allowed", date)
	}
	if strings.ContainsAny(symbol, `/\`) {
		return nil, fmt.Errorf("bad symbol %q", symbol)
	}
	pattern := symbol
	if pattern == "" {
		pattern = "*"
	}
	files, err := filepath.Glob(filepath.Join(dataDir, date, pattern+".db"))
	if err != nil {
		return nil, err
	}
	meta, err := dayindex.Open(dataDir)
	if err != nil {
		return nil, err
	}
	defer meta.Close()
	var out []Summary
	var errs []error
	for _, f := range files {
		sym := strings.TrimSuffix(filepath.Base(f), ".db")
		s, err := rejudgeFile(f, date, sym, loc, dryRun, filter())
		if err == nil && !dryRun {
			if err = dayindex.Set(meta, sym, date, s.ValidAfter); err != nil {
				err = fmt.Errorf("file committed but day_index stale (run closeout): %w", err)
			}
		}
		if err != nil {
			errs = append(errs, fmt.Errorf("%s/%s: %w", date, sym, err))
		}
		out = append(out, s)
	}
	return out, errors.Join(errs...)
}

type flip struct {
	seq   int64
	valid int
}

func rejudgeFile(path, date, symbol string, loc *time.Location, dryRun bool, f *tick.Filter) (s Summary, err error) {
	s.Date, s.Symbol = date, symbol
	midnight, _ := time.ParseInLocation("20060102", date, loc)
	base := midnight.UnixMilli()
	db, err := store.Create(store.Driver, path)
	if err != nil {
		return s, err
	}
	defer db.Close()
	ctx := context.Background()
	conn, err := db.Conn(ctx)
	if err != nil {
		return s, err
	}
	defer conn.Close()
	if _, err = conn.ExecContext(ctx, "BEGIN IMMEDIATE"); err != nil {
		return s, err
	}
	defer func() {
		if err != nil || dryRun {
			conn.ExecContext(ctx, "ROLLBACK")
		}
	}()

	rows, err := conn.QueryContext(ctx, `SELECT raw_seq, ts, price, qty, valid FROM ticks ORDER BY raw_seq`)
	if err != nil {
		return s, err
	}
	var flips []flip
	candles := map[int64]*store.Candle{}
	for rows.Next() {
		var seq, ts, price, qty int64
		var old int
		if err = rows.Scan(&seq, &ts, &price, &qty, &old); err != nil {
			rows.Close()
			return s, err
		}
		valid := 0
		if f.Valid(tick.Tick{Symbol: symbol, TS: ts, Price: price, Qty: qty}) {
			valid = 1
		}
		s.ValidBefore += int64(old)
		s.ValidAfter += int64(valid)
		if valid != old {
			flips = append(flips, flip{seq, valid})
			if valid == 1 {
				s.ToValid++
			} else {
				s.ToInvalid++
			}
		}
		if valid == 0 {
			continue
		}
		m := (ts - base) / 60000
		c := candles[m]
		if c == nil {
			c = &store.Candle{}
			candles[m] = c
		}
		c.Add(ts, price, qty)
	}
	if err = rows.Err(); err != nil {
		rows.Close()
		return s, err
	}
	rows.Close()

	for _, fl := range flips {
		if _, err = conn.ExecContext(ctx, `UPDATE ticks SET valid = ? WHERE raw_seq = ?`, fl.valid, fl.seq); err != nil {
			return s, err
		}
	}
	res, err := conn.ExecContext(ctx, `UPDATE day_stats SET valid_count = ?`, s.ValidAfter)
	if err != nil {
		return s, err
	}
	if n, _ := res.RowsAffected(); n != 1 {
		err = errors.New("day_stats has no row")
		return s, err
	}
	if _, err = conn.ExecContext(ctx, `DELETE FROM candle_1m`); err != nil {
		return s, err
	}
	for m, c := range candles {
		if err = store.UpsertCandle(ctx, conn, m, c); err != nil {
			return s, err
		}
	}
	if dryRun {
		return s, nil
	}
	_, err = conn.ExecContext(ctx, "COMMIT")
	return s, err
}
