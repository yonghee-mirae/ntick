package stream

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	"ntick/internal/store"
)

var dayDir = regexp.MustCompile(`^\d{8}$`)

// snapshot builds the machine from the newest day file that holds valid ticks.
// Everything is read in ONE read transaction bounded by that file's
// last_raw_seq, which is returned with the date so the caller can skip events
// the snapshot already contains. With no data it returns a fresh machine.
func snapshot(ctx context.Context, dataDir, symbol string, load func(tx *sql.Tx, date string, last int64) error) (date string, last int64, err error) {
	ents, err := os.ReadDir(dataDir)
	if err != nil && !os.IsNotExist(err) {
		return "", 0, err
	}
	var days []string
	for _, e := range ents {
		if e.IsDir() && dayDir.MatchString(e.Name()) {
			if _, err := os.Stat(filepath.Join(dataDir, e.Name(), symbol+".db")); err == nil {
				days = append(days, e.Name())
			}
		}
	}
	sort.Sort(sort.Reverse(sort.StringSlice(days)))
	for _, d := range days {
		found, l, err := readDay(ctx, filepath.Join(dataDir, d, symbol+".db"), d, load)
		if err != nil {
			return "", 0, err
		}
		if found {
			return d, l, nil
		}
	}
	return "", 0, nil
}

func readDay(ctx context.Context, path, date string, load func(tx *sql.Tx, date string, last int64) error) (found bool, last int64, err error) {
	db, err := store.OpenRO(path)
	if err != nil {
		return false, 0, err
	}
	defer db.Close()
	tx, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return false, 0, err
	}
	defer tx.Rollback()
	var total int64
	err = tx.QueryRowContext(ctx, `SELECT valid_count, last_raw_seq FROM day_stats WHERE id = 1`).Scan(&total, &last)
	// "no such table": the writer has created the file but not its schema yet.
	if err == sql.ErrNoRows || (err == nil && total == 0) || (err != nil && strings.Contains(err.Error(), "no such table")) {
		return false, 0, nil // no valid ticks in this file
	}
	if err != nil {
		return false, 0, err
	}
	return true, last, load(tx, date, last)
}

// loadTick fills m with the last T mod n valid ticks (raw_seq <= last).
func (m *tickMachine) load(tx *sql.Tx, date string, last int64) error {
	var total int64
	if err := tx.QueryRow(`SELECT valid_count FROM day_stats WHERE id = 1`).Scan(&total); err != nil {
		return err
	}
	m.date, m.t = date, total
	r := total % int64(m.n)
	if r == 0 {
		return nil // candle just completed; the next tick starts a new one
	}
	rows, err := tx.Query(`SELECT price, qty, raw_seq FROM (
		SELECT price, qty, raw_seq FROM ticks WHERE valid = 1 AND raw_seq <= ? ORDER BY raw_seq DESC LIMIT ?
	) ORDER BY raw_seq`, last, r)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var p, q, seq int64
		if err := rows.Scan(&p, &q, &seq); err != nil {
			return err
		}
		m.cur.add(p, q, seq)
	}
	return rows.Err()
}

// load fills m with the newest bucket rolled up from candle_1m.
func (m *timeMachine) load(tx *sql.Tx, date string, _ int64) error {
	d, err := time.ParseInLocation("20060102", date, m.loc)
	if err != nil {
		return err
	}
	iv := m.step / 60000
	var maxMin int64
	if err := tx.QueryRow(`SELECT max(minute) FROM candle_1m WHERE tick_count > 0`).Scan(&maxMin); err != nil {
		return err
	}
	lo := maxMin / iv * iv
	m.start = d.UnixMilli() + lo*60000
	rows, err := tx.Query(`SELECT open, high, low, close, open_ts, close_ts, volume, tick_count
		FROM candle_1m WHERE minute >= ? AND tick_count > 0 ORDER BY minute`, lo)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var o, h, l, c, ots, cts, v int64
		var n int
		if err := rows.Scan(&o, &h, &l, &c, &ots, &cts, &v, &n); err != nil {
			return err
		}
		if m.cur.count == 0 {
			m.cur = agg{open: o, high: h, low: l, openTS: ots}
		}
		m.cur.high, m.cur.low = max(m.cur.high, h), min(m.cur.low, l)
		m.cur.close, m.cur.closeTS = c, cts
		m.cur.volume += v
		m.cur.count += n
	}
	return rows.Err()
}
