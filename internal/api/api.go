// Package api serves the REST candle endpoint (PRD 5.1) over the query package.
package api

import (
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"regexp"
	"strconv"
	"time"

	"ntick/internal/query"
)

// symbolRe is strict because the symbol becomes part of a file path.
var symbolRe = regexp.MustCompile(`^[A-Za-z0-9_-]{1,12}$`)

type timeCandle struct {
	Start     int64 `json:"start"`
	Open      int64 `json:"open"`
	High      int64 `json:"high"`
	Low       int64 `json:"low"`
	Close     int64 `json:"close"`
	Volume    int64 `json:"volume"`
	TickCount int   `json:"tick_count"`
	Partial   bool  `json:"partial"`
}

type tickCandle struct {
	Date      string `json:"date"`
	Open      int64  `json:"open"`
	High      int64  `json:"high"`
	Low       int64  `json:"low"`
	Close     int64  `json:"close"`
	Volume    int64  `json:"volume"`
	TickCount int    `json:"tick_count"`
	Partial   bool   `json:"partial"`
}

// Handler returns the HTTP handler serving GET /candles from dataDir.
func Handler(dataDir string, loc *time.Location) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /candles", func(w http.ResponseWriter, r *http.Request) {
		candles(dataDir, loc, w, r)
	})
	// Non-GET requests on /candles get a JSON 405 like other errors.
	mux.HandleFunc("/candles", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Allow", "GET")
		fail(w, http.StatusMethodNotAllowed, "method not allowed")
	})
	return mux
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		log.Printf("api: write response: %v", err)
	}
}

func fail(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}

func intParam(q map[string][]string, name string) (int64, bool) {
	v, err := strconv.ParseInt(first(q[name]), 10, 64)
	return v, err == nil
}

func first(s []string) string {
	if len(s) == 0 {
		return ""
	}
	return s[0]
}

func candles(dataDir string, loc *time.Location, w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	symbol := q.Get("symbol")
	if !symbolRe.MatchString(symbol) {
		fail(w, http.StatusBadRequest, "symbol must match [A-Za-z0-9_-]{1,12}")
		return
	}
	typ := q.Get("type")
	switch typ {
	case "tick":
		n, ok1 := intParam(q, "n")
		m, ok2 := intParam(q, "m")
		if !ok1 || !ok2 || n < 1 || n > query.MaxN || m < 1 || m > query.MaxM {
			fail(w, http.StatusBadRequest, "n must be 1..10000 and m 1..1000")
			return
		}
		cs, err := query.NTick(r.Context(), dataDir, symbol, int(n), int(m))
		if err != nil {
			log.Printf("api: tick query: %v", err)
			fail(w, http.StatusInternalServerError, "query failed")
			return
		}
		out := make([]tickCandle, len(cs))
		for i, c := range cs {
			out[i] = tickCandle(c)
		}
		writeJSON(w, http.StatusOK, map[string]any{"symbol": symbol, "type": typ, "candles": out})
	case "time":
		iv, err := query.ParseInterval(q.Get("interval"))
		if err != nil {
			fail(w, http.StatusBadRequest, "interval must be like 1m, 5m, 1h, 1d (max 1d)")
			return
		}
		from, ok1 := intParam(q, "from")
		to, ok2 := intParam(q, "to")
		if !ok1 || !ok2 || from >= to {
			fail(w, http.StatusBadRequest, "from and to must be epoch ms integers with from < to")
			return
		}
		cs, err := query.Time(r.Context(), dataDir, symbol, loc, iv, from, to)
		if errors.Is(err, query.ErrRangeTooLarge) {
			fail(w, http.StatusBadRequest, "range reaches beyond the newest 30 day files; raise from")
			return
		}
		if err != nil {
			log.Printf("api: time query: %v", err)
			fail(w, http.StatusInternalServerError, "query failed")
			return
		}
		out := make([]timeCandle, len(cs))
		for i, c := range cs {
			out[i] = timeCandle(c)
		}
		writeJSON(w, http.StatusOK, map[string]any{"symbol": symbol, "type": typ, "candles": out})
	default:
		fail(w, http.StatusBadRequest, "type must be time or tick")
	}
}
