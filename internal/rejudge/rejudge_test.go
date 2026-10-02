package rejudge

import (
	"database/sql"
	"fmt"
	"math/rand"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"ntick/internal/dayindex"
	"ntick/internal/ingest"
	"ntick/internal/store"
	"ntick/internal/tick"
)

var seoul, _ = time.LoadLocation("Asia/Seoul")

var now = time.Date(2026, 1, 6, 10, 0, 0, 0, seoul)

func testTicks() []tick.Tick {
	base := time.Date(2026, 1, 5, 9, 0, 0, 0, seoul).UnixMilli()
	rng := rand.New(rand.NewSource(7))
	var out []tick.Tick
	for i := 0; i < 300; i++ {
		tk := tick.Tick{Symbol: "A", TS: base + int64(rng.Intn(240000)), Price: int64(90 + rng.Intn(20)), Qty: int64(1 + rng.Intn(5))}
		if i%13 == 0 {
			tk.Price = 0
		}
		if i%29 == 0 {
			tk.Qty = 0
		}
		out = append(out, tk)
		if i%11 == 0 {
			out = append(out, tk) // same-ms duplicate
		}
	}
	return out
}

func ingestTo(t *testing.T, dir string, ticks []tick.Tick) string {
	in := ingest.New(dir, seoul)
	for _, tk := range ticks {
		in.Put(tk)
	}
	in.Close()
	return filepath.Join(dir, "20260105", "A.db")
}

func dump(t *testing.T, path string) string {
	db, err := sql.Open(store.Driver, path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var sb strings.Builder
	for _, q := range []string{
		`SELECT raw_seq, ts, price, qty, valid FROM ticks ORDER BY raw_seq`,
		`SELECT * FROM day_stats`,
		`SELECT * FROM candle_1m ORDER BY minute`,
	} {
		rows, err := db.Query(q)
		if err != nil {
			t.Fatal(err)
		}
		cols, _ := rows.Columns()
		v := make([]any, len(cols))
		p := make([]any, len(cols))
		for i := range v {
			p[i] = &v[i]
		}
		for rows.Next() {
			rows.Scan(p...)
			fmt.Fprintln(&sb, v...)
		}
		rows.Close()
		sb.WriteString("--\n")
	}
	return sb.String()
}

func exec(t *testing.T, path string, q string) {
	db, _ := sql.Open(store.Driver, path)
	defer db.Close()
	if _, err := db.Exec(q); err != nil {
		t.Fatal(err)
	}
}

func TestRejudgeRestoresFreshIngest(t *testing.T) {
	ticks := testTicks()
	want := dump(t, ingestTo(t, t.TempDir(), ticks))

	dir := t.TempDir()
	path := ingestTo(t, dir, ticks)
	// tamper in both directions and corrupt the derived data
	exec(t, path, `UPDATE ticks SET valid = CASE WHEN price <= 0 OR qty <= 0 THEN 1 WHEN raw_seq % 7 = 0 THEN 0 ELSE valid END`)
	exec(t, path, `UPDATE day_stats SET valid_count = 5`)
	exec(t, path, `UPDATE candle_1m SET open = 1, high = 1, tick_count = 1`)
	if dump(t, path) == want {
		t.Fatal("tampering had no effect")
	}

	// dry run changes nothing
	tampered := dump(t, path)
	sums, err := Rejudge(dir, seoul, "20260105", "", now, true, tick.DefaultFilter)
	if err != nil || len(sums) != 1 || sums[0].ToValid == 0 || sums[0].ToInvalid == 0 {
		t.Fatalf("dry run: %+v %v", sums, err)
	}
	if dump(t, path) != tampered {
		t.Error("dry run modified the file")
	}

	sums, err = Rejudge(dir, seoul, "20260105", "A", now, false, tick.DefaultFilter)
	if err != nil {
		t.Fatal(err)
	}
	if got := dump(t, path); got != want {
		t.Errorf("rejudged file differs from fresh ingest\ngot:\n%s\nwant:\n%s", got, want)
	}
	meta, _ := dayindex.Open(dir)
	defer meta.Close()
	var vc int64
	meta.QueryRow(`SELECT valid_count FROM day_index WHERE symbol = 'A' AND date = '20260105'`).Scan(&vc)
	if vc != sums[0].ValidAfter {
		t.Errorf("day_index = %d, want %d", vc, sums[0].ValidAfter)
	}
	reps, _ := dayindex.Closeout(dir, "20260105")
	if len(reps) != 1 || len(reps[0].Drift) != 0 {
		t.Errorf("drift after rejudge: %+v", reps)
	}
}

func TestRejudgeRefusesTodayAndFuture(t *testing.T) {
	dir := t.TempDir()
	ingestTo(t, dir, testTicks())
	for _, d := range []string{"20260105", "20260106", "20260107"} {
		n := time.Date(2026, 1, 5, 23, 0, 0, 0, seoul) // "today" is 20260105
		if _, err := Rejudge(dir, seoul, d, "", n, false, tick.DefaultFilter); err == nil {
			t.Errorf("date %s accepted", d)
		}
	}
	if _, err := Rejudge(dir, seoul, "20260105", "../x", now, false, tick.DefaultFilter); err == nil {
		t.Error("path symbol accepted")
	}
}

// A stricter rule flips exactly the ticks a brute-force recomputation expects.
func TestRejudgeStricterRule(t *testing.T) {
	ticks := testTicks()
	dir := t.TempDir()
	path := ingestTo(t, dir, ticks)
	strict := func() *tick.Filter {
		return tick.NewFilter(func(string) tick.Rule {
			return tick.RuleFunc(func(x tick.Tick) bool { return x.Price >= 100 && x.Qty > 0 })
		})
	}
	sums, err := Rejudge(dir, seoul, "20260105", "A", now, false, strict)
	if err != nil {
		t.Fatal(err)
	}

	// expected, from the raw ticks in arrival order
	type tk struct {
		i int
		tick.Tick
	}
	var valid []tk
	wantVal := map[int]int{}
	for i, x := range ticks {
		if x.Price >= 100 && x.Qty > 0 {
			valid = append(valid, tk{i, x})
			wantVal[i+1] = 1 // raw_seq is arrival index + 1 (single batch order)
		}
	}
	if sums[0].ValidAfter != int64(len(valid)) || sums[0].ToValid != 0 {
		t.Errorf("summary %+v, want valid %d and no ToValid", sums[0], len(valid))
	}
	db, _ := sql.Open(store.Driver, path)
	defer db.Close()
	rows, _ := db.Query(`SELECT raw_seq, valid FROM ticks`)
	for rows.Next() {
		var seq, v int
		rows.Scan(&seq, &v)
		if v != wantVal[seq] {
			t.Errorf("raw_seq %d valid=%d want %d", seq, v, wantVal[seq])
		}
	}
	rows.Close()
	base := time.Date(2026, 1, 5, 0, 0, 0, 0, seoul).UnixMilli()
	byMin := map[int64][]tk{}
	for _, x := range valid {
		byMin[(x.TS-base)/60000] = append(byMin[(x.TS-base)/60000], x)
	}
	for m, g := range byMin {
		sort.SliceStable(g, func(a, b int) bool { return g[a].TS < g[b].TS }) // ties keep arrival order
		var o, c, h, l, vol int64
		h, l = g[0].Price, g[0].Price
		for _, x := range g {
			h, l, vol = max(h, x.Price), min(l, x.Price), vol+x.Qty
		}
		o, c = g[0].Price, g[len(g)-1].Price
		var go_, gc, gh, gl, gv, gn int64
		if err := db.QueryRow(`SELECT open, close, high, low, volume, tick_count FROM candle_1m WHERE minute = ?`, m).Scan(&go_, &gc, &gh, &gl, &gv, &gn); err != nil {
			t.Fatalf("minute %d: %v", m, err)
		}
		if go_ != o || gc != c || gh != h || gl != l || gv != vol || gn != int64(len(g)) {
			t.Errorf("minute %d: got %d %d %d %d %d %d want %d %d %d %d %d %d", m, go_, gc, gh, gl, gv, gn, o, c, h, l, vol, len(g))
		}
	}
	var n int
	db.QueryRow(`SELECT count(*) FROM candle_1m`).Scan(&n)
	if n != len(byMin) {
		t.Errorf("candle rows %d, want %d", n, len(byMin))
	}
}
