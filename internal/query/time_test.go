package query

import (
	"context"
	"database/sql"
	"errors"
	"math/rand"
	"sort"
	"testing"
	"time"

	"ntick/internal/oracle"
)

func TestParseInterval(t *testing.T) {
	for s, want := range map[string]int{"1m": 1, "5m": 5, "2h": 120, "24h": 1440, "1d": 1440, "1440m": 1440} {
		if got, err := ParseInterval(s); err != nil || got != want {
			t.Errorf("%s: got %d, %v", s, got, err)
		}
	}
	for _, s := range []string{"", "0m", "-1m", "5x", "m", "1441m", "25h", "2d", "99999999999999999999m", "1.5h", " 5m"} {
		if _, err := ParseInterval(s); err == nil {
			t.Errorf("%q: want error", s)
		}
	}
}

func TestRollup(t *testing.T) {
	rows := []minuteRow{
		{0, 10, 12, 9, 11, 5, 2},
		{4, 11, 20, 8, 15, 7, 3},
		{5, 15, 16, 14, 14, 1, 1},
		{1439, 1, 1, 1, 1, 1, 1},
	}
	const mid, end = int64(1000), int64(1000 + 1440*60000)
	cs, es := rollup(rows, mid, end, 5, mid, end)
	want := []TimeCandle{
		{Start: mid, Open: 10, High: 20, Low: 8, Close: 15, Volume: 12, TickCount: 5},
		{Start: mid + 5*60000, Open: 15, High: 16, Low: 14, Close: 14, Volume: 1, TickCount: 1},
		{Start: mid + 1435*60000, Open: 1, High: 1, Low: 1, Close: 1, Volume: 1, TickCount: 1},
	}
	if len(cs) != 3 || cs[0] != want[0] || cs[1] != want[1] || cs[2] != want[2] {
		t.Fatalf("got %+v", cs)
	}
	if es[2] != end { // last bucket of the day is capped at midnight (1439 -> bucket 1435..1440 fits; check 7m too)
		t.Fatalf("end %d", es[2])
	}
	cs, es = rollup(rows, mid, end, 7, mid, end)
	if last := len(cs) - 1; es[last] != end || cs[last].Start != mid+1435*60000-1435%7*60000 {
		t.Fatalf("7m last: %+v end %d", cs[last], es[last])
	}
	// Start filter [from, to)
	cs, _ = rollup(rows, mid, end, 5, mid+5*60000, mid+1435*60000)
	if len(cs) != 1 || cs[0].Start != mid+5*60000 {
		t.Fatalf("filter: %+v", cs)
	}
}

// writeTS is the PRD 6.1 transaction, including the candle_1m upsert.
func writeTS(db *sql.DB, mid int64, ticks []oracle.Tick) error {
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
	for _, k := range ticks {
		if !k.Valid {
			continue
		}
		if _, err := tx.Exec(`INSERT INTO candle_1m (minute, open, high, low, close, open_ts, close_ts, volume, tick_count)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1)
ON CONFLICT (minute) DO UPDATE SET
  open = CASE WHEN excluded.open_ts < open_ts THEN excluded.open ELSE open END,
  open_ts = MIN(open_ts, excluded.open_ts),
  close = CASE WHEN excluded.close_ts >= close_ts THEN excluded.close ELSE close END,
  close_ts = MAX(close_ts, excluded.close_ts),
  high = MAX(high, excluded.high), low = MIN(low, excluded.low),
  volume = volume + excluded.volume, tick_count = tick_count + 1`,
			(k.TS-mid)/60000, k.Price, k.Price, k.Price, k.Price, k.TS, k.TS, k.Qty); err != nil {
			return err
		}
	}
	return tx.Commit()
}

type arrTick struct {
	oracle.Tick
	seq int
}

// bruteTime rolls up valid ticks directly: open = min (ts, arrival), close = max (ts, arrival).
func bruteTime(all []arrTick, loc *time.Location, iv int, from, to, latest int64) []TimeCandle {
	type key struct{ mid, b int64 }
	groups := map[key][]arrTick{}
	for _, k := range all {
		if !k.Valid {
			continue
		}
		_, mid := oracle.DateOf(k.TS, loc)
		kk := key{mid, (k.TS - mid) / 60000 / int64(iv)}
		groups[kk] = append(groups[kk], k)
	}
	var out []TimeCandle
	for kk, g := range groups {
		start := kk.mid + kk.b*int64(iv)*60000
		if start < from || start >= to {
			continue
		}
		sort.Slice(g, func(i, j int) bool {
			if g[i].TS != g[j].TS {
				return g[i].TS < g[j].TS
			}
			return g[i].seq < g[j].seq
		})
		c := TimeCandle{Start: start, Open: g[0].Price, Close: g[len(g)-1].Price, High: g[0].Price, Low: g[0].Price}
		for _, x := range g {
			c.High, c.Low = max(c.High, x.Price), min(c.Low, x.Price)
			c.Volume += x.Qty
			c.TickCount++
		}
		end := min(start+int64(iv)*60000, time.UnixMilli(kk.mid).In(loc).AddDate(0, 0, 1).UnixMilli())
		c.Partial = end > latest
		out = append(out, c)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Start < out[j].Start })
	return out
}

func TestTimeAgainstBruteForce(t *testing.T) {
	for _, name := range []string{"Asia/Seoul", "Asia/Tokyo", "America/New_York"} {
		t.Run(name, func(t *testing.T) {
			loc, err := time.LoadLocation(name)
			if err != nil {
				t.Fatal(err)
			}
			timeAgainstBruteForce(t, loc)
		})
	}
}

func timeAgainstBruteForce(t *testing.T, loc *time.Location) {
	rng := rand.New(rand.NewSource(5))
	dir := t.TempDir()
	d0 := time.Date(2026, 1, 1, 0, 0, 0, 0, loc)
	mid := func(day int) int64 { return d0.AddDate(0, 0, day).UnixMilli() }
	// Days 0..3 hold ticks, day 3 has only invalid ticks (skipped as "latest"); day 2 midnight crossing with day 1.
	var all []arrTick
	byDay := map[int][]oracle.Tick{}
	for i := 0; i < 900; i++ {
		var ts int64
		if i%3 == 0 { // 1s granularity around the day 0/1 and 1/2 midnights, so ties and crossings occur
			ts = mid(1+rng.Intn(2)) + (rng.Int63n(3600)-1800)*1000
		} else {
			ts = mid(0) + rng.Int63n(3*86400000)
		}
		k := oracle.Tick{TS: ts, Price: 1 + rng.Int63n(500), Qty: 1 + rng.Int63n(20), Valid: rng.Intn(5) != 0}
		date, _ := oracle.DateOf(ts, loc)
		day := int((ts - mid(0)) / 86400000)
		_ = date
		byDay[day] = append(byDay[day], k)
		all = append(all, arrTick{k, i})
	}
	for i := 0; i < 20; i++ { // invalid-only newest day
		k := oracle.Tick{TS: mid(3) + int64(i)*1000, Price: 5, Qty: 5, Valid: false}
		byDay[3] = append(byDay[3], k)
		all = append(all, arrTick{k, 1000 + i})
	}
	// write in arrival order per day file
	for day := 0; day < 4; day++ {
		date, m := oracle.DateOf(mid(day), loc)
		db := openDay(t, dir, date, "A")
		ts := byDay[day]
		for s := 0; s < len(ts); s += 25 {
			if err := writeTS(db, m, ts[s:min(s+25, len(ts))]); err != nil {
				t.Fatal(err)
			}
		}
		db.Close()
	}
	var latest int64
	for _, k := range all {
		if k.Valid && k.TS < mid(3) {
			latest = max(latest, k.TS)
		}
	}
	ivs := []int{1, 2, 3, 5, 7, 10, 60, 90, 120, 1440}
	for i := 0; i < 300; i++ {
		iv := ivs[rng.Intn(len(ivs))]
		if i%4 == 0 {
			iv = 1 + rng.Intn(1440)
		}
		from := mid(0) + rng.Int63n(4*86400000) - 3600000
		to := from + rng.Int63n(2*86400000)
		got, err := Time(context.Background(), dir, "A", loc, iv, from, to)
		if err != nil {
			t.Fatal(err)
		}
		want := bruteTime(all, loc, iv, from, to, latest)
		if len(got) != len(want) {
			t.Fatalf("iv=%d [%d,%d): %d candles, want %d", iv, from, to, len(got), len(want))
		}
		for j := range want {
			if got[j] != want[j] {
				t.Fatalf("iv=%d #%d: got %+v want %+v", iv, j, got[j], want[j])
			}
		}
	}
}

func TestRangeTooLarge(t *testing.T) {
	loc, _ := time.LoadLocation("Asia/Seoul")
	dir := t.TempDir()
	d0 := time.Date(2026, 1, 1, 0, 0, 0, 0, loc)
	for i := 0; i <= MaxDays; i++ { // MaxDays+1 files; the oldest (day 0) is cut off
		db := openDay(t, dir, d0.AddDate(0, 0, i).Format("20060102"), "A")
		db.Close()
	}
	oldest := d0.AddDate(0, 0, 1).UnixMilli() // start of the oldest scanned file
	to := d0.AddDate(0, 0, 40).UnixMilli()
	if _, err := Time(context.Background(), dir, "A", loc, 1, oldest-1, to); !errors.Is(err, ErrRangeTooLarge) {
		t.Errorf("from before oldest scanned: %v", err)
	}
	if _, err := Time(context.Background(), dir, "A", loc, 1, oldest, to); err != nil {
		t.Errorf("from at oldest scanned: %v", err)
	}
	// Exactly MaxDays files: nothing is cut, no error.
	if _, err := Time(context.Background(), t.TempDir(), "A", loc, 1, 0, to); err != nil {
		t.Errorf("empty dir: %v", err)
	}
}
