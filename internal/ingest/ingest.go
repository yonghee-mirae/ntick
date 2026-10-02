// Package ingest reads the feed, filters ticks, and writes them to per-day,
// per-symbol SQLite files with one transaction per micro-batch (PRD 6.1).
package ingest

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"ntick/internal/tick"
	"ntick/internal/wire"
)

const (
	maxBatch      = 500                   // flush when this many ticks are pending
	flushInterval = 50 * time.Millisecond // flush pending ticks at least this often
	queueSize     = 4096                  // per-worker channel capacity
	maxBackoff    = 5 * time.Second
)

// Ingester routes ticks to workers by symbol hash. A symbol always maps to the
// same worker, so each symbol file has exactly one writer and per-symbol order
// is preserved.
type Ingester struct {
	workers []*worker
	wg      sync.WaitGroup
	bad     atomic.Int64 // rejected messages so far
	badLog  atomic.Int64 // unix nano of the last reject log line
}

// Sanity bounds for ts (epoch ms): [2000-01-01, 2100-01-01) UTC. Anything else
// would create junk day directories.
var (
	minTS = time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC).UnixMilli()
	maxTS = time.Date(2100, 1, 1, 0, 0, 0, 0, time.UTC).UnixMilli()
)

// reject counts a bad message and logs at most one line per second.
func (in *Ingester) reject(why string) {
	n := in.bad.Add(1)
	now := time.Now().UnixNano()
	if last := in.badLog.Load(); now-last >= int64(time.Second) && in.badLog.CompareAndSwap(last, now) {
		log.Printf("rejected %d bad messages so far, latest: %s", n, why)
	}
}

// New starts the workers. Day files live under dir/{YYYYMMDD}/{symbol}.db and
// the day and minute are computed in loc.
func New(dir string, loc *time.Location) *Ingester {
	in := &Ingester{}
	n := runtime.NumCPU()
	pool := poolSize(n)
	for i := 0; i < n; i++ {
		w := newWorker(dir, loc)
		w.maxFiles = pool
		in.workers = append(in.workers, w)
		in.wg.Add(1)
		go func() { defer in.wg.Done(); w.run() }()
	}
	return in
}

// Event describes one committed per-file transaction that added valid ticks.
type Event struct {
	Symbol, Date string       // Date is the local YYYYMMDD of the day file
	Ticks        []CommitTick // the new valid ticks, in raw_seq order
	T            int64        // day valid_count after the commit
	LastSeq      int64        // day last_raw_seq after the commit
}

// CommitTick is a valid tick with its raw_seq in the day file.
type CommitTick struct{ Seq, TS, Price, Qty int64 }

// OnCommit sets fn to be called after each committed file transaction that
// added valid ticks. fn runs on the writer goroutine and must not block. Call
// it before the first Put.
func (in *Ingester) OnCommit(fn func(Event)) {
	for _, w := range in.workers {
		w.onCommit = fn
	}
}

// Put queues one tick. Ticks with an invalid symbol or an out-of-range ts are
// dropped and counted before anything touches the file system. It blocks when
// the worker queue is full and must not be called after Close.
func (in *Ingester) Put(t tick.Tick) {
	if !wire.ValidSymbol(t.Symbol) {
		in.reject(fmt.Sprintf("bad symbol %q", t.Symbol))
		return
	}
	if t.TS < minTS || t.TS >= maxTS {
		in.reject(fmt.Sprintf("ts %d out of range (symbol %s)", t.TS, t.Symbol))
		return
	}
	h := fnv.New32a()
	h.Write([]byte(t.Symbol))
	in.workers[int(h.Sum32()%uint32(len(in.workers)))].ch <- t
}

// Close flushes all pending batches and closes the files.
func (in *Ingester) Close() {
	for _, w := range in.workers {
		close(w.ch)
	}
	in.wg.Wait()
}

// Serve reads the feed at addr until ctx is cancelled, reconnecting with
// backoff. Nothing is replayed or deduplicated after a reconnect; the gap is
// only logged.
func (in *Ingester) Serve(ctx context.Context, addr string) {
	backoff := 100 * time.Millisecond
	var d net.Dialer
	for ctx.Err() == nil {
		conn, err := d.DialContext(ctx, "tcp", addr)
		if err == nil {
			backoff = 100 * time.Millisecond
			log.Printf("feed connected: %s", addr)
			n, err := in.read(ctx, conn)
			log.Printf("feed disconnected after %d messages (%v); messages sent while down are lost", n, err)
		} else if ctx.Err() == nil {
			log.Printf("feed dial failed: %v", err)
		}
		select {
		case <-ctx.Done():
		case <-time.After(backoff):
		}
		if backoff *= 2; backoff > maxBackoff {
			backoff = maxBackoff
		}
	}
}

// read feeds ticks from conn until the stream ends or ctx is cancelled.
func (in *Ingester) read(ctx context.Context, conn net.Conn) (n int, err error) {
	defer conn.Close()
	stop := context.AfterFunc(ctx, func() { conn.Close() })
	defer stop()
	r := wire.NewReader(bufio.NewReaderSize(conn, 64<<10))
	for {
		m, err := r.Next()
		if errors.Is(err, wire.ErrType) || errors.Is(err, wire.ErrVersion) || errors.Is(err, wire.ErrSymbol) {
			in.reject(err.Error()) // framing is fixed-length, so the stream stays aligned
			continue
		}
		if err != nil {
			if err == io.EOF || ctx.Err() != nil {
				err = nil
			}
			return n, err
		}
		in.Put(tick.Tick{Symbol: m.Symbol, TS: m.Ts, Price: m.Price, Qty: m.Qty})
		n++
	}
}
