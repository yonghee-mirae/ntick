// Package dayindex maintains meta.db day_index (PRD 4.1) and the end-of-day
// consistency check (PRD 6.4). The day files are authoritative: day_index is a
// cache that is always overwritten with the absolute day_stats.valid_count.
package dayindex

import (
	"database/sql"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"ntick/internal/store"
)

const metaSchema = `CREATE TABLE IF NOT EXISTS day_index (
  symbol      TEXT NOT NULL,
  date        TEXT NOT NULL,   -- local YYYYMMDD
  valid_count INTEGER NOT NULL,
  PRIMARY KEY (symbol, date)
)`

var dateRe = regexp.MustCompile(`^\d{8}$`)

// Open opens or creates dataDir/meta.db.
func Open(dataDir string) (*sql.DB, error) {
	db, err := sql.Open(store.Driver, store.DSN(filepath.Join(dataDir, "meta.db")))
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	if _, err := db.Exec(metaSchema); err != nil {
		db.Close()
		return nil, err
	}
	return db, nil
}

// Set overwrites the index row with an absolute count (idempotent, never a delta).
func Set(db *sql.DB, symbol, date string, validCount int64) error {
	_, err := db.Exec(`INSERT INTO day_index (symbol, date, valid_count) VALUES (?, ?, ?)
		ON CONFLICT (symbol, date) DO UPDATE SET valid_count = excluded.valid_count`, symbol, date, validCount)
	return err
}

// Report is the check result of one day file.
type Report struct {
	Date, Symbol string
	ValidCount   int64    // day_stats.valid_count (the value written to day_index)
	Drift        []string // empty = consistent
}

// Closeout checks every dataDir/{date}/{symbol}.db and writes day_index from
// day_stats.valid_count, also when drift is found (the file is the source of truth).
func Closeout(dataDir, date string) ([]Report, error) {
	return closeout(dataDir, date, false)
}

// CatchUp closes out every date before today (in loc) whose files are missing
// from day_index. Meant to run at startup.
func CatchUp(dataDir string, loc *time.Location, now time.Time) ([]Report, error) {
	entries, err := os.ReadDir(dataDir)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil // first start, nothing to close out
	}
	if err != nil {
		return nil, err
	}
	today := now.In(loc).Format("20060102")
	var all []Report
	for _, e := range entries {
		if e.IsDir() && dateRe.MatchString(e.Name()) && e.Name() < today {
			r, err := closeout(dataDir, e.Name(), true)
			all = append(all, r...)
			if err != nil {
				return all, err
			}
		}
	}
	return all, nil
}

func closeout(dataDir, date string, onlyMissing bool) ([]Report, error) {
	files, err := filepath.Glob(filepath.Join(dataDir, date, "*.db"))
	if err != nil || len(files) == 0 {
		return nil, err
	}
	meta, err := Open(dataDir)
	if err != nil {
		return nil, err
	}
	defer meta.Close()
	var out []Report
	var errs []error
	for _, f := range files {
		sym := strings.TrimSuffix(filepath.Base(f), ".db")
		if onlyMissing {
			var one int
			if err := meta.QueryRow(`SELECT 1 FROM day_index WHERE symbol = ? AND date = ?`, sym, date).Scan(&one); err == nil {
				continue
			}
		}
		r, err := checkFile(f)
		r.Date, r.Symbol = date, sym
		if err == nil && r.ValidCount >= 0 {
			err = Set(meta, sym, date, r.ValidCount)
		}
		if err != nil {
			errs = append(errs, fmt.Errorf("%s/%s: %w", date, sym, err))
			continue
		}
		out = append(out, r)
	}
	return out, errors.Join(errs...)
}

// checkFile runs the PRD 6.4 checks in one read snapshot. ValidCount is -1
// when day_stats has no row (nothing to index).
func checkFile(path string) (r Report, err error) {
	db, err := sql.Open(store.Driver, "file:"+path+"?mode=ro&_pragma=busy_timeout(5000)")
	if err != nil {
		return r, err
	}
	defer db.Close()
	tx, err := db.Begin()
	if err != nil {
		return r, err
	}
	defer tx.Rollback()
	var last, countValid, maxRaw, candleSum int64
	err = tx.QueryRow(`SELECT valid_count, last_raw_seq FROM day_stats`).Scan(&r.ValidCount, &last)
	if errors.Is(err, sql.ErrNoRows) {
		r.ValidCount = -1
		r.Drift = []string{"day_stats has no row"}
		return r, nil
	}
	if err != nil {
		return r, err
	}
	if err = tx.QueryRow(`SELECT count(CASE WHEN valid = 1 THEN 1 END), coalesce(max(raw_seq), 0) FROM ticks`).Scan(&countValid, &maxRaw); err != nil {
		return r, err
	}
	if err = tx.QueryRow(`SELECT coalesce(sum(tick_count), 0) FROM candle_1m`).Scan(&candleSum); err != nil {
		return r, err
	}
	if countValid != r.ValidCount {
		r.Drift = append(r.Drift, fmt.Sprintf("COUNT(valid=1)=%d != valid_count=%d", countValid, r.ValidCount))
	}
	if maxRaw != last {
		r.Drift = append(r.Drift, fmt.Sprintf("MAX(raw_seq)=%d != last_raw_seq=%d", maxRaw, last))
	}
	if candleSum != r.ValidCount {
		r.Drift = append(r.Drift, fmt.Sprintf("SUM(candle_1m.tick_count)=%d != valid_count=%d", candleSum, r.ValidCount))
	}
	return r, nil
}

// NextClose returns the first HH:MM wall-clock instant in loc strictly after now.
func NextClose(now time.Time, loc *time.Location, hhmm string) (time.Time, error) {
	c, err := time.ParseInLocation("15:04", hhmm, loc)
	if err != nil {
		return time.Time{}, fmt.Errorf("close time %q: %w", hhmm, err)
	}
	now = now.In(loc)
	t := time.Date(now.Year(), now.Month(), now.Day(), c.Hour(), c.Minute(), 0, 0, loc)
	if !t.After(now) {
		t = t.AddDate(0, 0, 1)
	}
	return t, nil
}

// Schedule blocks until stop is closed, running Closeout for the local date at
// each close time. Reports with drift are logged.
func Schedule(dataDir string, loc *time.Location, hhmm string, stop <-chan struct{}) error {
	for {
		next, err := NextClose(time.Now(), loc, hhmm)
		if err != nil {
			return err
		}
		select {
		case <-stop:
			return nil
		case <-time.After(time.Until(next)):
		}
		date := next.Format("20060102")
		reps, err := Closeout(dataDir, date)
		LogReports(reps)
		if err != nil {
			log.Printf("closeout %s: %v", date, err)
		}
	}
}

// LogReports logs one line per file with drift and a summary line.
func LogReports(reps []Report) {
	drift := 0
	for _, r := range reps {
		if len(r.Drift) > 0 {
			drift++
			log.Printf("DRIFT %s/%s: %s", r.Date, r.Symbol, strings.Join(r.Drift, "; "))
		}
	}
	log.Printf("closeout: %d files checked, %d with drift", len(reps), drift)
}
