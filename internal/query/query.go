// Package query implements the PRD 5.2 n-tick candle query over per-day SQLite files.
package query

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"sync/atomic"

	"ntick/internal/store"
)

const (
	MaxM    = 1000
	MaxN    = 10000
	MaxDays = 30
)

// Candle is one n-tick candle. Date is the local YYYYMMDD of its day file.
type Candle struct {
	Date                   string
	Open, High, Low, Close int64
	Volume                 int64
	TickCount              int
	Partial                bool // TickCount < n
}

// rowsRead counts ticks scanned; tests use it to verify the m*n bound.
var rowsRead atomic.Int64

var dayDir = regexp.MustCompile(`^\d{8}$`)

// plan returns the number of candles to take from a day, the size r of its
// newest (possibly partial) candle, and the number of ticks L to read.
func plan(t, n, remaining int) (take, r, l int) {
	if t <= 0 {
		return 0, 0, 0
	}
	c := (t + n - 1) / n
	take = min(remaining, c)
	r = t % n
	if r == 0 {
		r = n
	}
	return take, r, r + (take-1)*n
}

// NTick returns up to m candles of n valid ticks, newest first. Day boundaries are
// fixed per day; empty days are skipped; at most MaxDays day files are scanned.
func NTick(ctx context.Context, dataDir, symbol string, n, m int) ([]Candle, error) {
	if n < 1 || n > MaxN || m < 1 || m > MaxM {
		return nil, fmt.Errorf("query: n must be 1..%d and m 1..%d (got n=%d m=%d)", MaxN, MaxM, n, m)
	}
	days, _, err := listDays(dataDir, symbol)
	if err != nil {
		return nil, err
	}
	var out []Candle
	for _, d := range days {
		cs, err := readDay(ctx, filepath.Join(dataDir, d, symbol+".db"), d, n, m-len(out))
		if err != nil {
			return nil, fmt.Errorf("query: day %s: %w", d, err)
		}
		out = append(out, cs...)
		if len(out) >= m {
			break
		}
	}
	return out, nil
}

// listDays returns up to MaxDays YYYYMMDD dirs holding symbol.db, newest first,
// and whether older day files were cut off. A missing data dir is empty.
func listDays(dataDir, symbol string) (days []string, older bool, err error) {
	ents, err := os.ReadDir(dataDir)
	if errors.Is(err, os.ErrNotExist) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	for _, e := range ents {
		if !e.IsDir() || !dayDir.MatchString(e.Name()) {
			continue
		}
		if _, err := os.Stat(filepath.Join(dataDir, e.Name(), symbol+".db")); err == nil {
			days = append(days, e.Name())
		}
	}
	sort.Sort(sort.Reverse(sort.StringSlice(days)))
	return days[:min(len(days), MaxDays)], len(days) > MaxDays, nil
}

// readTx runs f in one read-only transaction on a read-only connection (PRD 6.2).
func readTx(ctx context.Context, path string, f func(tx *sql.Tx) error) error {
	db, err := store.OpenRO(path)
	if err != nil {
		return err
	}
	defer db.Close()
	tx, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return err
	}
	defer tx.Rollback()
	return f(tx)
}

// readDay reads day_stats and the ticks in one read transaction (PRD 6.2).
func readDay(ctx context.Context, path, date string, n, remaining int) (out []Candle, err error) {
	err = readTx(ctx, path, func(tx *sql.Tx) error {
		out, err = readDayTx(ctx, tx, date, n, remaining)
		return err
	})
	return out, err
}

func readDayTx(ctx context.Context, tx *sql.Tx, date string, n, remaining int) ([]Candle, error) {
	var total, last int
	err := tx.QueryRowContext(ctx, `SELECT valid_count, last_raw_seq FROM day_stats WHERE id = 1`).Scan(&total, &last)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil // no ticks yet
	}
	if err != nil {
		return nil, err
	}
	take, r, l := plan(total, n, remaining)
	if take == 0 {
		return nil, nil
	}
	rows, err := tx.QueryContext(ctx, `SELECT price, qty FROM ticks WHERE valid = 1 AND raw_seq <= ? ORDER BY raw_seq DESC LIMIT ?`, last, l)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]Candle, 0, take)
	size := r // size of the candle being filled; only the first (newest) is short
	var c Candle
	for rows.Next() {
		var p, q int64
		if err := rows.Scan(&p, &q); err != nil {
			return nil, err
		}
		rowsRead.Add(1)
		if c.TickCount == 0 { // newest tick of the candle is its close
			c = Candle{Date: date, Close: p, High: p, Low: p}
		}
		c.Open = p // rows are newest first, so the last assignment is the open
		c.High, c.Low = max(c.High, p), min(c.Low, p)
		c.Volume += q
		c.TickCount++
		if c.TickCount == size {
			c.Partial = c.TickCount < n
			out = append(out, c)
			c, size = Candle{}, n
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if c.TickCount != 0 || len(out) != take {
		return nil, fmt.Errorf("short read: %d candles, want %d", len(out), take)
	}
	return out, nil
}
