// Command mockfeed streams MOCK fixed-length trade messages over TCP (see docs/mock-feed-spec.md).
// The stream is one global deterministic sequence: a reconnecting client continues
// where the previous one stopped; nothing is resent. Intended for one client at a time.
package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"sync"
	"time"

	"ntick/internal/wire"
)

type server struct {
	mu         sync.Mutex
	g          *gen
	idx        int64
	truth      *bufio.Writer
	rate       int
	disconnect int
}

// emit generates and writes one message; it returns false when the stream is done or the write failed.
func (s *server) emit(conn net.Conn) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	m, label, ok := s.g.next()
	if !ok {
		return false
	}
	b, err := wire.Encode(m)
	if err != nil {
		log.Fatal(err)
	}
	if _, err := conn.Write(b); err != nil {
		return false // ponytail: message lost in flight is still counted as consumed
	}
	fmt.Fprintf(s.truth, "%d,%s,%d,%d,%d,%s\n", s.idx, m.Symbol, m.Ts, m.Price, m.Qty, label)
	s.idx++
	return true
}

func (s *server) handle(conn net.Conn) {
	defer conn.Close()
	defer func() { s.mu.Lock(); s.truth.Flush(); s.mu.Unlock() }()
	for n := 0; s.disconnect == 0 || n < s.disconnect; n++ {
		if !s.emit(conn) {
			return
		}
		if s.rate > 0 {
			time.Sleep(time.Second / time.Duration(s.rate))
		}
	}
}

func (s *server) serve(ln net.Listener) {
	for {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		go s.handle(c)
	}
}

func main() {
	addr := flag.String("addr", "127.0.0.1:9000", "listen address")
	seed := flag.Int64("seed", 1, "random seed")
	symbols := flag.Int("symbols", 5, "number of symbols")
	rate := flag.Int("rate", 100, "messages per second per client (0 = unthrottled)")
	days := flag.Int("days", 1, "number of simulated trading days")
	perDay := flag.Int("perday", 1000, "base ticks per day")
	tz := flag.String("tz", "Asia/Seoul", "exchange time zone")
	startDate := flag.String("start-date", "", "first simulated local date YYYY-MM-DD (default 2026-01-05)")
	startTime := flag.String("start-time", "09:00", "first tick time of each day, HH:MM local")
	anomaly := flag.Float64("anomaly", 0, "probability of price<=0 or qty<=0")
	reversal := flag.Float64("reversal", 0, "probability of ts earlier than the previous tick of the symbol")
	dup := flag.Float64("dup", 0, "probability of an exact duplicate (same ms, price, qty)")
	disc := flag.Int("disconnect", 0, "close each connection after N messages (0 = never)")
	truthPath := flag.String("truth", "truth.csv", "ground-truth file: idx,symbol,ts,price,qty,label")
	flag.Parse()

	loc, err := time.LoadLocation(*tz)
	if err != nil {
		log.Fatal(err)
	}
	start, err := parseStart(*startDate, *startTime, loc)
	if err != nil {
		log.Fatal(err)
	}
	f, err := os.Create(*truthPath)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("listening on %s", ln.Addr())
	s := &server{
		g: newGen(genConfig{Seed: *seed, Symbols: *symbols, Days: *days, PerDay: *perDay, Loc: loc, Start: start,
			PAnomaly: *anomaly, PReversal: *reversal, PDup: *dup}),
		truth: bufio.NewWriter(f), rate: *rate, disconnect: *disc,
	}
	s.serve(ln)
}

// parseStart returns the zero time (generator default date) when date is empty.
func parseStart(date, hhmm string, loc *time.Location) (time.Time, error) {
	if date == "" {
		date = "2026-01-05"
	}
	return time.ParseInLocation("2006-01-02 15:04", date+" "+hhmm, loc)
}
