// Package stream pushes in-progress candle updates over WebSocket (PRD 5.5).
// The ingest commit hook feeds a Broker; each connection derives its candle
// from a consistent snapshot plus the events after it, with no per-event DB reads.
package stream

import (
	"context"
	"database/sql"
	"encoding/json"
	"log"
	"net/http"
	"regexp"
	"sort"
	"strconv"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"

	"ntick/internal/ingest"
	"ntick/internal/query"
)

// writeTimeout bounds one message write so a stalled client cannot pin a handler.
const writeTimeout = 10 * time.Second

// symbolRe is strict because the symbol becomes part of a file path (same rule as REST).
var symbolRe = regexp.MustCompile(`^[A-Za-z0-9_-]{1,12}$`)

type handler struct {
	dataDir string
	b       *Broker
	loc     *time.Location
}

// Handler serves WS /candles/stream. Mount it at "/candles/stream".
func Handler(dataDir string, b *Broker, loc *time.Location) http.Handler {
	return &handler{dataDir, b, loc}
}

func fail(w http.ResponseWriter, code int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(map[string]string{"error": msg})
}

// newMachine validates the query like REST does and returns the empty machine and its loader.
func (h *handler) newMachine(q map[string][]string) (machine, func(*sql.Tx, string, int64) error, string) {
	get := func(k string) string {
		if v := q[k]; len(v) > 0 {
			return v[0]
		}
		return ""
	}
	if !symbolRe.MatchString(get("symbol")) {
		return nil, nil, "symbol must match [A-Za-z0-9_-]{1,12}"
	}
	switch get("type") {
	case "tick":
		n, err := strconv.Atoi(get("n"))
		if err != nil || n < 1 || n > query.MaxN {
			return nil, nil, "n must be 1..10000"
		}
		m := &tickMachine{n: n}
		return m, m.load, ""
	case "time":
		iv, err := query.ParseInterval(get("interval"))
		if err != nil {
			return nil, nil, "interval must be like 1m, 5m, 1h, 1d (max 1d)"
		}
		m := &timeMachine{loc: h.loc, step: int64(iv) * 60000}
		return m, m.load, ""
	}
	return nil, nil, "type must be time or tick"
}

func (h *handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	m, load, msg := h.newMachine(q)
	if msg != "" {
		fail(w, http.StatusBadRequest, msg)
		return
	}
	symbol := q.Get("symbol")
	// Subscribe before the snapshot so no commit can fall between them.
	sub := h.b.Subscribe(symbol)
	if sub == nil {
		fail(w, http.StatusServiceUnavailable, "server shutting down")
		return
	}
	defer h.b.Unsubscribe(sub)
	snapDate, snapSeq, err := snapshot(r.Context(), h.dataDir, symbol, load)
	if err != nil {
		log.Printf("stream: snapshot %s: %v", symbol, err)
		fail(w, http.StatusInternalServerError, "snapshot failed")
		return
	}
	c, err := websocket.Accept(w, r, nil)
	if err != nil {
		return // Accept already replied
	}
	defer c.CloseNow()
	ctx := c.CloseRead(r.Context())
	send := func(v any) error {
		wctx, cancel := context.WithTimeout(ctx, writeTimeout)
		defer cancel()
		return wsjson.Write(wctx, c, v)
	}
	if send(candleMsg{"snapshot", m.current()}) != nil {
		return
	}
	for {
		select {
		case <-ctx.Done():
			return
		case ev, ok := <-sub.C:
			if !ok {
				c.Close(websocket.StatusTryAgainLater, sub.Reason())
				return
			}
			if ev.Date == snapDate { // drop what the snapshot already contains
				i := sort.Search(len(ev.Ticks), func(i int) bool { return ev.Ticks[i].Seq > snapSeq })
				ev = ingest.Event{Symbol: ev.Symbol, Date: ev.Date, Ticks: ev.Ticks[i:], T: ev.T, LastSeq: ev.LastSeq}
			}
			for _, msg := range m.apply(ev) {
				if send(msg) != nil {
					return
				}
			}
		}
	}
}
