package query

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"regexp"
	"strconv"
	"time"
	_ "time/tzdata" // time zones must resolve on hosts without a zoneinfo database
)

// ErrRangeTooLarge means from lies before the oldest of the MaxDays scanned day files
// while older day files exist.
var ErrRangeTooLarge = errors.New("query: range reaches beyond the newest 30 day files")

// TimeCandle is one rolled-up time candle. Start is the bucket start in epoch ms.
type TimeCandle struct {
	Start                  int64
	Open, High, Low, Close int64
	Volume                 int64
	TickCount              int
	Partial                bool // bucket end is later than the latest ts held for the symbol
}

var intervalRe = regexp.MustCompile(`^(\d+)([mhd])$`)

// ParseInterval converts "5m", "2h", "1d" to minutes (1..1440).
func ParseInterval(s string) (int, error) {
	g := intervalRe.FindStringSubmatch(s)
	if g == nil {
		return 0, fmt.Errorf("query: bad interval %q", s)
	}
	v, err := strconv.Atoi(g[1])
	if err != nil || v < 1 || v > 1440 {
		return 0, fmt.Errorf("query: interval %q out of range", s)
	}
	v *= map[string]int{"m": 1, "h": 60, "d": 1440}[g[2]]
	if v > 1440 {
		return 0, fmt.Errorf("query: interval %q out of range", s)
	}
	return v, nil
}

type minuteRow struct {
	minute                 int
	open, high, low, close int64
	volume                 int64
	count                  int
}

// Time returns candles whose bucket Start lies in [from, to) (epoch ms), ascending.
// Buckets are aligned to the local midnight in loc of each day file and never cross it.
func Time(ctx context.Context, dataDir, symbol string, loc *time.Location, intervalMin int, from, to int64) ([]TimeCandle, error) {
	if intervalMin < 1 || intervalMin > 1440 {
		return nil, fmt.Errorf("query: interval must be 1..1440 minutes (got %d)", intervalMin)
	}
	days, older, err := listDays(dataDir, symbol) // newest first
	if err != nil {
		return nil, err
	}
	if older {
		oldest, err := time.ParseInLocation("20060102", days[len(days)-1], loc)
		if err != nil {
			return nil, err
		}
		if from < oldest.UnixMilli() {
			return nil, ErrRangeTooLarge
		}
	}
	var latest int64 // latest close_ts of the newest day file that has any candle_1m row
	var perDay [][]TimeCandle
	var ends [][]int64
	for _, d := range days {
		t, err := time.ParseInLocation("20060102", d, loc)
		if err != nil {
			return nil, err
		}
		mid, dayEnd := t.UnixMilli(), t.AddDate(0, 0, 1).UnixMilli()
		overlap := mid < to && dayEnd > from
		if !overlap && latest != 0 {
			continue
		}
		var rows []minuteRow
		var maxClose int64
		path := filepath.Join(dataDir, d, symbol+".db")
		err = readTx(ctx, path, func(tx *sql.Tx) error {
			rows, maxClose, err = readMinutes(ctx, tx)
			return err
		})
		if err != nil {
			return nil, fmt.Errorf("query: day %s: %w", d, err)
		}
		if latest == 0 {
			latest = maxClose // newest-first, so the first non-empty file holds the latest ts
		}
		if !overlap {
			continue
		}
		cs, es := rollup(rows, mid, dayEnd, intervalMin, from, to)
		perDay = append(perDay, cs)
		ends = append(ends, es)
	}
	var out []TimeCandle
	for i := len(perDay) - 1; i >= 0; i-- { // oldest day first
		for j, c := range perDay[i] {
			c.Partial = ends[i][j] > latest
			out = append(out, c)
		}
	}
	return out, nil
}

// readMinutes reads the non-empty 1m candles and the latest close_ts in one transaction.
func readMinutes(ctx context.Context, tx *sql.Tx) ([]minuteRow, int64, error) {
	rs, err := tx.QueryContext(ctx, `SELECT minute, open, high, low, close, close_ts, volume, tick_count FROM candle_1m WHERE tick_count > 0 ORDER BY minute`)
	if err != nil {
		return nil, 0, err
	}
	defer rs.Close()
	var rows []minuteRow
	var maxClose int64
	for rs.Next() {
		var r minuteRow
		var cts int64
		if err := rs.Scan(&r.minute, &r.open, &r.high, &r.low, &r.close, &cts, &r.volume, &r.count); err != nil {
			return nil, 0, err
		}
		maxClose = max(maxClose, cts)
		rows = append(rows, r)
	}
	return rows, maxClose, rs.Err()
}

// rollup merges ascending minute rows into buckets of iv minutes starting at mid, keeping
// buckets with Start in [from, to). It also returns each bucket's end (capped at dayEnd).
func rollup(rows []minuteRow, mid, dayEnd int64, iv int, from, to int64) ([]TimeCandle, []int64) {
	var cs []TimeCandle
	var ends []int64
	last := -1
	for _, r := range rows {
		b := r.minute / iv
		start := mid + int64(b*iv)*60000
		if b != last {
			last = b
			if start < from || start >= to {
				continue
			}
			cs = append(cs, TimeCandle{Start: start, Open: r.open, High: r.high, Low: r.low})
			ends = append(ends, min(start+int64(iv)*60000, dayEnd))
		} else if len(cs) == 0 || cs[len(cs)-1].Start != start {
			continue // bucket filtered out
		}
		c := &cs[len(cs)-1]
		c.High, c.Low = max(c.High, r.high), min(c.Low, r.low)
		c.Close = r.close
		c.Volume += r.volume
		c.TickCount += r.count
	}
	return cs, ends
}
