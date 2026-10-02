package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"ntick/internal/query"
	"ntick/internal/store"
)

var loc, _ = time.LoadLocation("Asia/Seoul")

// mkData writes one day file with valid and invalid ticks and matching day_stats/candle_1m.
func mkData(t *testing.T) (dir string, mid int64) {
	t.Helper()
	dir = t.TempDir()
	loc, _ := time.LoadLocation("Asia/Seoul")
	day := time.Date(2026, 1, 5, 0, 0, 0, 0, loc)
	mid = day.UnixMilli()
	if err := os.MkdirAll(filepath.Join(dir, "20260105"), 0o755); err != nil {
		t.Fatal(err)
	}
	db, err := store.Create(store.Driver, filepath.Join(dir, "20260105", "005930.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	type tk struct {
		min        int
		price, qty int64
		valid      int
	}
	ticks := []tk{{540, 100, 1, 1}, {540, 0, 5, 0}, {540, 110, 2, 1}, {541, 90, 3, 1}, {541, 95, -1, 0},
		{541, 105, 4, 1}, {542, 100, 1, 1}, {545, 120, 7, 1}, {545, 80, 2, 1}}
	valid := 0
	for _, k := range ticks {
		if _, err := db.Exec(`INSERT INTO ticks (ts, price, qty, valid) VALUES (?,?,?,?)`, mid+int64(k.min)*60000, k.price, k.qty, k.valid); err != nil {
			t.Fatal(err)
		}
		valid += k.valid
	}
	if _, err := db.Exec(`INSERT INTO day_stats VALUES (1, ?, (SELECT max(raw_seq) FROM ticks))`, valid); err != nil {
		t.Fatal(err)
	}
	for _, c := range [][]int64{{540, 100, 110, 100, 110, 3}, {541, 90, 105, 90, 105, 7}, {542, 100, 100, 100, 100, 1}, {545, 120, 120, 80, 80, 9}} {
		cnt := 2
		if c[0] == 542 {
			cnt = 1
		}
		if _, err := db.Exec(`INSERT INTO candle_1m VALUES (?,?,?,?,?,?,?,?,?)`,
			c[0], c[1], c[2], c[3], c[4], mid+c[0]*60000, mid+c[0]*60000, c[5], cnt); err != nil {
			t.Fatal(err)
		}
	}
	return dir, mid
}

func get(h http.Handler, method, url string) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(method, url, nil))
	return w
}

func TestTick(t *testing.T) {
	dir, _ := mkData(t)
	w := get(Handler(dir, loc), "GET", "/candles?symbol=005930&type=tick&n=2&m=10")
	if w.Code != 200 {
		t.Fatal(w.Code, w.Body)
	}
	var got struct {
		Symbol, Type string
		Candles      []tickCandle
	}
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	want, err := query.NTick(context.Background(), dir, "005930", 2, 10)
	if err != nil {
		t.Fatal(err)
	}
	if got.Symbol != "005930" || got.Type != "tick" || len(got.Candles) != len(want) || len(want) != 4 {
		t.Fatalf("got %+v want %+v", got, want)
	}
	for i, c := range want {
		if got.Candles[i] != tickCandle(c) {
			t.Errorf("%d: %+v vs %+v", i, got.Candles[i], c)
		}
	}
}

func TestTime(t *testing.T) {
	dir, mid := mkData(t)
	url := func(iv string, from, to int64) string {
		return "/candles?symbol=005930&type=time&interval=" + iv + "&from=" + itoa(from) + "&to=" + itoa(to)
	}
	for _, iv := range []string{"1m", "5m"} {
		w := get(Handler(dir, loc), "GET", url(iv, mid, mid+86400000))
		if w.Code != 200 {
			t.Fatal(w.Code, w.Body)
		}
		var got struct{ Candles []timeCandle }
		if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
			t.Fatal(err)
		}
		n, _ := query.ParseInterval(iv)
		want, err := query.Time(context.Background(), dir, "005930", loc, n, mid, mid+86400000)
		if err != nil || len(want) == 0 || len(got.Candles) != len(want) {
			t.Fatalf("%s: %v %+v %+v", iv, err, got.Candles, want)
		}
		for i, c := range want {
			if !reflect.DeepEqual(got.Candles[i], timeCandle(c)) {
				t.Errorf("%s %d: %+v vs %+v", iv, i, got.Candles[i], c)
			}
		}
	}
	// A range without data returns an empty array, not null.
	w := get(Handler(dir, loc), "GET", url("1m", mid-1000000, mid-500000))
	if w.Code != 200 || !contains(w.Body.String(), `"candles":[]`) {
		t.Errorf("empty: %d %s", w.Code, w.Body)
	}
}

func TestBadRequests(t *testing.T) {
	dir, mid := mkData(t)
	h := Handler(dir, loc)
	ts := itoa(mid)
	for _, u := range []string{
		"/candles?type=tick&n=1&m=1",
		"/candles?symbol=&type=tick&n=1&m=1",
		"/candles?symbol=005930",
		"/candles?symbol=005930&type=foo",
		"/candles?symbol=005930&type=tick&n=0&m=1",
		"/candles?symbol=005930&type=tick&n=10001&m=1",
		"/candles?symbol=005930&type=tick&n=1&m=1001",
		"/candles?symbol=005930&type=tick&n=x&m=1",
		"/candles?symbol=005930&type=tick&n=1",
		"/candles?symbol=005930&type=time&interval=7x&from=1&to=2",
		"/candles?symbol=005930&type=time&interval=2d&from=1&to=2",
		"/candles?symbol=005930&type=time&from=1&to=2",
		"/candles?symbol=005930&type=time&interval=1m&from=" + ts + "&to=" + ts,
		"/candles?symbol=005930&type=time&interval=1m&from=5&to=2",
		"/candles?symbol=005930&type=time&interval=1m&from=a&to=2",
		"/candles?symbol=005930&type=time&interval=1m&to=2",
		"/candles?symbol=0123456789012&type=tick&n=1&m=1",
		"/candles?symbol=..&type=tick&n=1&m=1",
		"/candles?symbol=..%2F..%2Fetc&type=tick&n=1&m=1",
		"/candles?symbol=a%2Fb&type=tick&n=1&m=1",
		"/candles?symbol=a%5Cb&type=tick&n=1&m=1",
		"/candles?symbol=a%00b&type=tick&n=1&m=1",
		"/candles?symbol=a.b&type=time&interval=1m&from=1&to=2",
	} {
		w := get(h, "GET", u)
		var e map[string]string
		if w.Code != 400 || json.Unmarshal(w.Body.Bytes(), &e) != nil || e["error"] == "" {
			t.Errorf("%s: %d %s", u, w.Code, w.Body)
		}
	}
}

func TestMethodNotAllowed(t *testing.T) {
	h := Handler(t.TempDir(), loc)
	for _, m := range []string{"POST", "PUT", "DELETE", "PATCH"} {
		if w := get(h, m, "/candles?symbol=A&type=tick&n=1&m=1"); w.Code != http.StatusMethodNotAllowed {
			t.Errorf("%s: %d", m, w.Code)
		} else if !contains(w.Body.String(), `"error":"method not allowed"`) {
			t.Errorf("%s body: %s", m, w.Body)
		}
	}
}

func TestReadFailure500(t *testing.T) {
	dir, mid := mkData(t)
	if err := os.WriteFile(filepath.Join(dir, "20260105", "005930.db"), []byte("not a database"), 0o644); err != nil {
		t.Fatal(err)
	}
	for _, u := range []string{
		"/candles?symbol=005930&type=tick&n=1&m=1",
		"/candles?symbol=005930&type=time&interval=1m&from=" + itoa(mid) + "&to=" + itoa(mid+1000),
	} {
		if w := get(Handler(dir, loc), "GET", u); w.Code != 500 || !contains(w.Body.String(), `"error"`) {
			t.Errorf("%s: %d %s", u, w.Code, w.Body)
		}
	}
}

func itoa(v int64) string { b, _ := json.Marshal(v); return string(b) }

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

func TestNoData200(t *testing.T) {
	h := Handler(filepath.Join(t.TempDir(), "missing"), loc)
	dir, mid := mkData(t)
	for _, c := range []struct {
		h http.Handler
		u string
	}{
		{h, "/candles?symbol=A&type=tick&n=1&m=1"},
		{h, "/candles?symbol=A&type=time&interval=1m&from=1&to=2"},
		{Handler(dir, loc), "/candles?symbol=NOPE&type=tick&n=1&m=1"},
		{Handler(dir, loc), "/candles?symbol=NOPE&type=time&interval=1m&from=" + itoa(mid) + "&to=" + itoa(mid+1000)},
	} {
		if w := get(c.h, "GET", c.u); w.Code != 200 || !contains(w.Body.String(), `"candles":[]`) {
			t.Errorf("%s: %d %s", c.u, w.Code, w.Body)
		}
	}
}

func TestRangeTooLarge400(t *testing.T) {
	dir := t.TempDir()
	for i := 0; i <= query.MaxDays; i++ { // MaxDays+1 day files
		d := filepath.Join(dir, time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC).AddDate(0, 0, i).Format("20060102"))
		os.MkdirAll(d, 0o755)
		db, err := store.Create(store.Driver, filepath.Join(d, "A.db"))
		if err != nil {
			t.Fatal(err)
		}
		db.Close()
	}
	if w := get(Handler(dir, loc), "GET", "/candles?symbol=A&type=time&interval=1m&from=0&to=9999999999999"); w.Code != 400 || !contains(w.Body.String(), "30 day files") {
		t.Errorf("%d %s", w.Code, w.Body)
	}
}
