// Command tclient is the ntick test client: DB integrity checks and oracle verification.
//
//	tclient check-integrity -data DIR
//	tclient verify-time -data DIR -truth truth.csv
//	tclient verify-stream -truth truth.csv -base ws://HOST:PORT -symbols A,B [-ready FILE]
//	tclient count-ticks -data DIR
//	tclient verify-subsequence -data DIR -truth truth.csv
//	tclient stress-read -base URL -symbols A,B [-duration 20s -clients 16 -ws 4]
//	tclient verify-api -truth truth.csv -base URL
//	tclient verify-ntick -data DIR -truth truth.csv
//	tclient verify -data DIR -truth truth.csv [-tz Asia/Seoul] [-max 20]
package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"ntick/internal/oracle"
	"ntick/internal/query"
	"ntick/internal/store"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: tclient check-integrity|verify [flags]")
		os.Exit(2)
	}
	fs := flag.NewFlagSet(os.Args[1], flag.ExitOnError)
	data := fs.String("data", "data", "data directory")
	truth := fs.String("truth", "truth.csv", "mockfeed truth CSV (verify)")
	tz := fs.String("tz", "Asia/Seoul", "exchange time zone (verify)")
	maxN := fs.Int("max", 20, "max mismatches to print (verify)")
	base := fs.String("base", "http://127.0.0.1:8080", "ntick HTTP base URL (verify-api)")
	symbols := fs.String("symbols", "", "comma-separated symbols to subscribe (verify-stream)")
	ready := fs.String("ready", "", "file created once all pre-feed subscriptions are live (verify-stream)")
	midDelay := fs.Duration("mid-delay", 3*time.Second, "delay before the mid-stream subscribers join (verify-stream)")
	timeout := fs.Duration("timeout", 3*time.Minute, "give up waiting for the streams to end (verify-stream)")
	duration := fs.Duration("duration", 20*time.Second, "how long to run (stress-read)")
	clients := fs.Int("clients", 16, "concurrent REST clients (stress-read)")
	wsClients := fs.Int("ws", 4, "WS tick subscribers (stress-read)")
	fs.Parse(os.Args[2:])

	var ok bool
	var err error
	switch os.Args[1] {
	case "check-integrity":
		ok, err = checkIntegrity(*data)
	case "verify":
		ok, err = verify(*data, *truth, *tz, *maxN)
	case "verify-ntick":
		ok, err = verifyNTick(*data, *truth, *tz, *maxN)
	case "verify-time":
		ok, err = verifyTime(*data, *truth, *tz, *maxN)
	case "verify-stream":
		ok, err = verifyStream(*truth, *tz, *base, *symbols, *ready, *midDelay, *timeout, *maxN)
	case "count-ticks":
		var n int64
		if n, err = countTicks(*data); err == nil {
			ok = true
			fmt.Println(n)
		}
	case "verify-subsequence":
		ok, err = verifySubsequence(*data, *truth, *tz, *maxN)
	case "stress-read":
		ok, err = stressRead(*base, *symbols, *duration, *clients, *wsClients)
	case "verify-api":
		ok, err = verifyAPI(*truth, *tz, *base, *maxN)
	default:
		err = fmt.Errorf("unknown subcommand %q", os.Args[1])
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(2)
	}
	if !ok {
		os.Exit(1)
	}
}

// files returns data/{date}/{symbol}.db paths keyed by (symbol, date).
func files(dir string) (map[oracle.Key]string, error) {
	paths, err := filepath.Glob(filepath.Join(dir, "*", "*.db"))
	if err != nil {
		return nil, err
	}
	out := map[oracle.Key]string{}
	for _, p := range paths {
		out[oracle.Key{Symbol: strings.TrimSuffix(filepath.Base(p), ".db"), Date: filepath.Base(filepath.Dir(p))}] = p
	}
	return out, nil
}

func sortedKeys[V any](m map[oracle.Key]V) []oracle.Key {
	ks := make([]oracle.Key, 0, len(m))
	for k := range m {
		ks = append(ks, k)
	}
	sort.Slice(ks, func(i, j int) bool {
		if ks[i].Date != ks[j].Date {
			return ks[i].Date < ks[j].Date
		}
		return ks[i].Symbol < ks[j].Symbol
	})
	return ks
}

func open(path string) (*sql.DB, error) {
	db, err := sql.Open(store.Driver, "file:"+path+"?_pragma=busy_timeout(5000)")
	if err == nil {
		db.SetMaxOpenConns(1)
	}
	return db, err
}

func checkIntegrity(dir string) (bool, error) {
	fm, err := files(dir)
	if err != nil {
		return false, err
	}
	if len(fm) == 0 {
		return false, fmt.Errorf("no db files under %s", dir)
	}
	all := true
	for _, k := range sortedKeys(fm) {
		msg, err := integrity(fm[k])
		if err != nil {
			msg = err.Error()
		}
		if msg == "" {
			fmt.Printf("PASS %s/%s\n", k.Date, k.Symbol)
		} else {
			all = false
			fmt.Printf("FAIL %s/%s: %s\n", k.Date, k.Symbol, msg)
		}
	}
	fmt.Printf("%d files, ok=%v\n", len(fm), all)
	return all, nil
}

// integrity returns "" when the three invariants hold, else a description of the violations.
func integrity(path string) (string, error) {
	db, err := open(path)
	if err != nil {
		return "", err
	}
	defer db.Close()
	var valid, maxSeq, candleSum int64
	var statValid, statSeq sql.NullInt64
	if err := db.QueryRow(`SELECT COALESCE(SUM(valid),0), COALESCE(MAX(raw_seq),0) FROM ticks`).Scan(&valid, &maxSeq); err != nil {
		return "", err
	}
	if err := db.QueryRow(`SELECT COALESCE(SUM(tick_count),0) FROM candle_1m`).Scan(&candleSum); err != nil {
		return "", err
	}
	if err := db.QueryRow(`SELECT valid_count, last_raw_seq FROM day_stats WHERE id = 1`).Scan(&statValid, &statSeq); err != nil && err != sql.ErrNoRows {
		return "", err
	}
	var bad []string
	if !statValid.Valid {
		bad = append(bad, "day_stats row missing")
	} else {
		if statValid.Int64 != valid {
			bad = append(bad, fmt.Sprintf("valid_count %d != COUNT(valid=1) %d", statValid.Int64, valid))
		}
		if statSeq.Int64 != maxSeq {
			bad = append(bad, fmt.Sprintf("last_raw_seq %d != MAX(raw_seq) %d", statSeq.Int64, maxSeq))
		}
		if candleSum != statValid.Int64 {
			bad = append(bad, fmt.Sprintf("SUM(tick_count) %d != valid_count %d", candleSum, statValid.Int64))
		}
	}
	return strings.Join(bad, "; "), nil
}

// mismatches collects up to max messages and counts the rest.
type mismatches struct {
	max, total int
}

func (m *mismatches) add(format string, a ...any) {
	if m.total < m.max {
		fmt.Printf("MISMATCH "+format+"\n", a...)
	}
	m.total++
}

func verify(dir, truthPath, tz string, maxN int) (bool, error) {
	loc, err := time.LoadLocation(tz)
	if err != nil {
		return false, err
	}
	f, err := os.Open(truthPath)
	if err != nil {
		return false, err
	}
	defer f.Close()
	rows, err := oracle.ParseTruth(f)
	if err != nil {
		return false, err
	}
	want := oracle.Expected(rows, loc)
	fm, err := files(dir)
	if err != nil {
		return false, err
	}
	mm := &mismatches{max: maxN}
	for k := range want {
		if _, ok := fm[k]; !ok {
			mm.add("%s/%s: file missing", k.Date, k.Symbol)
		}
	}
	for _, k := range sortedKeys(fm) {
		d, ok := want[k]
		if !ok {
			mm.add("%s/%s: unexpected file", k.Date, k.Symbol)
			continue
		}
		if err := verifyFile(fm[k], k, d, mm); err != nil {
			return false, err
		}
	}
	fmt.Printf("%d truth rows, %d expected files, %d actual files, %d mismatches\n", len(rows), len(want), len(fm), mm.total)
	return mm.total == 0, nil
}

func verifyFile(path string, k oracle.Key, d *oracle.Day, mm *mismatches) error {
	db, err := open(path)
	if err != nil {
		return err
	}
	defer db.Close()
	id := k.Date + "/" + k.Symbol

	rs, err := db.Query(`SELECT raw_seq, ts, price, qty, valid FROM ticks ORDER BY raw_seq`)
	if err != nil {
		return err
	}
	defer rs.Close()
	i := 0
	for rs.Next() {
		var seq, ts, p, q, v int64
		if err := rs.Scan(&seq, &ts, &p, &q, &v); err != nil {
			return err
		}
		if i < len(d.Ticks) {
			e := d.Ticks[i]
			if got := (oracle.Tick{TS: ts, Price: p, Qty: q, Valid: v == 1}); got != e {
				mm.add("%s: tick #%d (raw_seq %d): got %+v want %+v", id, i+1, seq, got, e)
			}
		}
		i++
	}
	if err := rs.Err(); err != nil {
		return err
	}
	if i != len(d.Ticks) {
		mm.add("%s: ticks count got %d want %d", id, i, len(d.Ticks))
	}

	cs, err := db.Query(`SELECT minute, open, high, low, close, open_ts, close_ts, volume, tick_count FROM candle_1m`)
	if err != nil {
		return err
	}
	defer cs.Close()
	seen := map[int64]bool{}
	for cs.Next() {
		var m int64
		var c oracle.Candle
		if err := cs.Scan(&m, &c.Open, &c.High, &c.Low, &c.Close, &c.OpenTS, &c.CloseTS, &c.Volume, &c.Count); err != nil {
			return err
		}
		seen[m] = true
		if e, ok := d.Candles[m]; !ok {
			mm.add("%s: unexpected candle minute %d", id, m)
		} else if c != e {
			mm.add("%s: candle minute %d: got %+v want %+v", id, m, c, e)
		}
	}
	if err := cs.Err(); err != nil {
		return err
	}
	for m := range d.Candles {
		if !seen[m] {
			mm.add("%s: candle minute %d missing", id, m)
		}
	}
	return nil
}

// symData is the oracle input of one symbol.
type symData struct {
	name        string
	dates       []string        // ascending, days with valid ticks
	perDay      [][]oracle.Tick // valid ticks per day, arrival order
	flat        []oracle.Tick   // all valid ticks, day order then arrival order
	first, last int64           // min and max valid ts
}

func loadSymbols(truthPath, tz string) ([]*symData, *time.Location, error) {
	loc, err := time.LoadLocation(tz)
	if err != nil {
		return nil, nil, err
	}
	f, err := os.Open(truthPath)
	if err != nil {
		return nil, nil, err
	}
	defer f.Close()
	rows, err := oracle.ParseTruth(f)
	if err != nil {
		return nil, nil, err
	}
	bySym := map[string]*symData{}
	exp := oracle.Expected(rows, loc)
	for _, k := range sortedKeys(exp) { // date ascending
		var v []oracle.Tick
		for _, t := range exp[k].Ticks {
			if t.Valid {
				v = append(v, t)
			}
		}
		if len(v) == 0 {
			continue
		}
		s := bySym[k.Symbol]
		if s == nil {
			s = &symData{name: k.Symbol, first: v[0].TS, last: v[0].TS}
			bySym[k.Symbol] = s
		}
		s.dates = append(s.dates, k.Date)
		s.perDay = append(s.perDay, v)
		s.flat = append(s.flat, v...)
		for _, t := range v {
			s.first, s.last = min(s.first, t.TS), max(s.last, t.TS)
		}
	}
	var out []*symData
	for _, s := range bySym {
		out = append(out, s)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].name < out[j].name })
	return out, loc, nil
}

type timeC struct {
	Start     int64 `json:"start"`
	Open      int64 `json:"open"`
	High      int64 `json:"high"`
	Low       int64 `json:"low"`
	Close     int64 `json:"close"`
	Volume    int64 `json:"volume"`
	TickCount int   `json:"tick_count"`
	Partial   bool  `json:"partial"`
}

type tickC struct {
	Date      string `json:"date"`
	Open      int64  `json:"open"`
	High      int64  `json:"high"`
	Low       int64  `json:"low"`
	Close     int64  `json:"close"`
	Volume    int64  `json:"volume"`
	TickCount int    `json:"tick_count"`
	Partial   bool   `json:"partial"`
}

func fromQueryTime(cs []query.TimeCandle) []timeC {
	out := []timeC{}
	for _, c := range cs {
		out = append(out, timeC(c))
	}
	return out
}

func fromQueryTick(cs []query.Candle) []tickC {
	out := []tickC{}
	for _, c := range cs {
		out = append(out, tickC(c))
	}
	return out
}

var (
	timeIntervals = []string{"1m", "3m", "5m", "7m", "15m", "1h", "1d"}
	tickNs        = []int{1, 2, 3, 7, 50, 500, 999, 10000}
	tickMs        = []int{1, 2, 3, 5, 50, 999, 1000}
)

type rng struct {
	name     string
	from, to int64
}

// ranges returns the non-empty query ranges of the grid for one symbol.
func ranges(s *symData, loc *time.Location) []rng {
	_, mid0 := oracle.DateOf(s.first, loc)
	_, midN := oracle.DateOf(s.last, loc)
	cross := midN // midnight that starts the last day
	if len(s.dates) > 1 {
		_, cross = oracle.DateOf(s.flat[len(s.perDay[0])].TS, loc)
	}
	minute := s.flat[len(s.flat)/2].TS / 60000 * 60000
	return []rng{
		{"span", mid0, midN + 24*3600000},
		{"mid-day", s.first + 600000, s.last - 600000},
		{"cross-midnight", cross - 3*3600000, cross + 12*3600000},
		{"one-minute", minute, minute + 60000},
	}
}

func compareTime(mm *mismatches, id string, got, want []timeC) {
	if len(got) != len(want) {
		mm.add("%s: got %d candles want %d", id, len(got), len(want))
		return
	}
	for i := range want {
		if got[i] != want[i] {
			mm.add("%s candle #%d: got %+v want %+v", id, i, got[i], want[i])
			return
		}
	}
}

func compareTick(mm *mismatches, id string, got, want []tickC) {
	if len(got) != len(want) {
		mm.add("%s: got %d candles want %d", id, len(got), len(want))
		return
	}
	for i := range want {
		if got[i] != want[i] {
			mm.add("%s candle #%d: got %+v want %+v", id, i, got[i], want[i])
			return
		}
	}
}

func wantTime(s *symData, loc *time.Location, iv string, r rng) []timeC {
	mins, _ := query.ParseInterval(iv)
	out := []timeC{}
	for _, c := range oracle.Time(s.flat, loc, mins, r.from, r.to) {
		out = append(out, timeC{c.Start, c.Open, c.High, c.Low, c.Close, c.Volume, int(c.Count), c.Partial})
	}
	return out
}

func wantTick(s *symData, n, m int) []tickC {
	cs := oracle.NTick(s.perDay, n, m)
	ds := nTickDates(s.dates, s.perDay, n, len(cs))
	out := []tickC{}
	for i, c := range cs {
		out = append(out, tickC{ds[i], c.Open, c.High, c.Low, c.Close, c.Volume, int(c.Count), c.Count < int64(n)})
	}
	return out
}

// timeGrid and tickGrid run the shared (interval, range) and (n, m) grids against a fetcher.
func timeGrid(syms []*symData, loc *time.Location, mm *mismatches, fetch func(sym, iv string, from, to int64) ([]timeC, error)) int {
	cases := 0
	for _, s := range syms {
		for _, iv := range timeIntervals {
			for _, r := range ranges(s, loc) {
				cases++
				id := fmt.Sprintf("%s iv=%s %s", s.name, iv, r.name)
				got, err := fetch(s.name, iv, r.from, r.to)
				if err != nil {
					mm.add("%s: %v", id, err)
					continue
				}
				compareTime(mm, id, got, wantTime(s, loc, iv, r))
			}
		}
	}
	return cases
}

func tickGrid(syms []*symData, mm *mismatches, fetch func(sym string, n, m int) ([]tickC, error)) int {
	cases := 0
	for _, s := range syms {
		for _, n := range tickNs {
			for _, m := range tickMs {
				cases++
				id := fmt.Sprintf("%s n=%d m=%d", s.name, n, m)
				got, err := fetch(s.name, n, m)
				if err != nil {
					mm.add("%s: %v", id, err)
					continue
				}
				compareTick(mm, id, got, wantTick(s, n, m))
			}
		}
	}
	return cases
}

func summary(name string, syms, cases int, mm *mismatches) bool {
	fmt.Printf("%s: %d symbols, %d cases, %d mismatches\n", name, syms, cases, mm.total)
	return syms > 0 && mm.total == 0
}

// verifyNTick compares query.NTick with oracle.NTick and checks that out-of-limit requests are rejected.
func verifyNTick(dir, truthPath, tz string, maxN int) (bool, error) {
	syms, _, err := loadSymbols(truthPath, tz)
	if err != nil {
		return false, err
	}
	mm := &mismatches{max: maxN}
	cases := tickGrid(syms, mm, func(sym string, n, m int) ([]tickC, error) {
		cs, err := query.NTick(context.Background(), dir, sym, n, m)
		return fromQueryTick(cs), err
	})
	for _, s := range syms {
		for _, c := range [][2]int{{10001, 1}, {1, 1001}, {0, 1}, {1, 0}} {
			cases++
			if _, err := query.NTick(context.Background(), dir, s.name, c[0], c[1]); err == nil {
				mm.add("%s n=%d m=%d: expected an error", s.name, c[0], c[1])
			}
		}
	}
	return summary("verify-ntick", len(syms), cases, mm), nil
}

// verifyTime compares query.Time with oracle.Time.
func verifyTime(dir, truthPath, tz string, maxN int) (bool, error) {
	syms, loc, err := loadSymbols(truthPath, tz)
	if err != nil {
		return false, err
	}
	mm := &mismatches{max: maxN}
	cases := timeGrid(syms, loc, mm, func(sym, iv string, from, to int64) ([]timeC, error) {
		m, err := query.ParseInterval(iv)
		if err != nil {
			return nil, err
		}
		cs, err := query.Time(context.Background(), dir, sym, loc, m, from, to)
		return fromQueryTime(cs), err
	})
	for _, s := range syms { // empty range
		cases++
		if cs, err := query.Time(context.Background(), dir, s.name, loc, 1, s.first, s.first); err != nil || len(cs) != 0 {
			mm.add("%s: from>=to: got %d candles, err %v", s.name, len(cs), err)
		}
	}
	for _, bad := range []string{"0m", "-5m", "5x", "1441m", "2d", ""} {
		cases++
		if _, err := query.ParseInterval(bad); err == nil {
			mm.add("ParseInterval(%q): expected an error", bad)
		}
	}
	return summary("verify-time", len(syms), cases, mm), nil
}

type apiResp struct {
	Symbol  string          `json:"symbol"`
	Type    string          `json:"type"`
	Candles json.RawMessage `json:"candles"`
	Error   string          `json:"error"`
}

func apiGet(base string, params url.Values) (int, apiResp, error) {
	var r apiResp
	resp, err := http.Get(base + "/candles?" + params.Encode())
	if err != nil {
		return 0, r, err
	}
	defer resp.Body.Close()
	err = json.NewDecoder(resp.Body).Decode(&r)
	return resp.StatusCode, r, err
}

// apiCandles fetches a 200 response and decodes its candles into out.
func apiCandles(base string, params url.Values, out any) error {
	code, r, err := apiGet(base, params)
	if err != nil {
		return err
	}
	if code != 200 {
		return fmt.Errorf("HTTP %d: %s", code, r.Error)
	}
	if r.Symbol != params.Get("symbol") || r.Type != params.Get("type") {
		return fmt.Errorf("wrapper symbol=%q type=%q", r.Symbol, r.Type)
	}
	return json.Unmarshal(r.Candles, out)
}

// verifyAPI compares GET /candles responses with the oracle and checks error handling.
func verifyAPI(truthPath, tz, base string, maxN int) (bool, error) {
	syms, loc, err := loadSymbols(truthPath, tz)
	if err != nil {
		return false, err
	}
	mm := &mismatches{max: maxN}
	i64 := func(v int64) string { return fmt.Sprint(v) }
	cases := timeGrid(syms, loc, mm, func(sym, iv string, from, to int64) ([]timeC, error) {
		var out []timeC
		err := apiCandles(base, url.Values{"symbol": {sym}, "type": {"time"}, "interval": {iv},
			"from": {i64(from)}, "to": {i64(to)}}, &out)
		if out == nil {
			out = []timeC{} // a JSON null would also be a contract violation worth seeing
		}
		return out, err
	})
	cases += tickGrid(syms, mm, func(sym string, n, m int) ([]tickC, error) {
		var out []tickC
		err := apiCandles(base, url.Values{"symbol": {sym}, "type": {"tick"}, "n": {fmt.Sprint(n)}, "m": {fmt.Sprint(m)}}, &out)
		if out == nil {
			out = []tickC{}
		}
		return out, err
	})
	sym := "UNKNOWN"
	if len(syms) > 0 {
		sym = syms[0].name
	}
	tm := func(kv ...string) url.Values {
		v := url.Values{"symbol": {sym}, "type": {"time"}, "interval": {"1m"}, "from": {"1"}, "to": {"2"}}
		for i := 0; i < len(kv); i += 2 {
			if kv[i+1] == "" {
				v.Del(kv[i])
			} else {
				v.Set(kv[i], kv[i+1])
			}
		}
		return v
	}
	tk := func(kv ...string) url.Values {
		v := url.Values{"symbol": {sym}, "type": {"tick"}, "n": {"1"}, "m": {"1"}}
		for i := 0; i < len(kv); i += 2 {
			v.Set(kv[i], kv[i+1])
		}
		return v
	}
	bad := map[string]url.Values{
		"missing symbol":    tm("symbol", ""),
		"symbol ../x":       tm("symbol", "../x"),
		"missing type":      tm("type", ""),
		"unknown type":      tm("type", "foo"),
		"n=10001":           tk("n", "10001"),
		"m=1001":            tk("m", "1001"),
		"n=0":               tk("n", "0"),
		"interval=0m":       tm("interval", "0m"),
		"interval=2d":       tm("interval", "2d"),
		"missing interval":  tm("interval", ""),
		"from>=to":          tm("from", "5", "to", "5"),
		"from>to":           tm("from", "9", "to", "5"),
		"from not a number": tm("from", "abc"),
	}
	for name, p := range bad {
		cases++
		code, r, err := apiGet(base, p)
		if err != nil || code != 400 || r.Error == "" {
			mm.add("bad param %q: want 400 + error body, got code=%d error=%q err=%v", name, code, r.Error, err)
		}
	}
	return summary("verify-api", len(syms), cases, mm), nil
}

// nTickDates returns the date of each of the first k candles, newest first.
func nTickDates(ds []string, perDay [][]oracle.Tick, n, k int) []string {
	var out []string
	for i := len(ds) - 1; i >= 0 && len(out) < k; i-- {
		for c := (len(perDay[i]) + n - 1) / n; c > 0 && len(out) < k; c-- {
			out = append(out, ds[i])
		}
	}
	return out
}
