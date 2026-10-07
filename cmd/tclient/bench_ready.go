package main

// A-ready: per-symbol queues plus a ready queue. An idle writer claims one ready symbol, drains up to
// -batch ticks, commits, and only then releases the symbol, so a symbol is never held by two writers
// and per-symbol order is preserved. Same transaction code (fwriter.write) as A-tuned; only dispatch differs.

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// gBusy/gExtra carry per-run writer stats to the RESULT line (set by the A-* run closures).
var (
	gBusy  []time.Duration
	gE2E   []int32
	gExtra string

	gSkipUntil atomic.Int64
)

// stampE2E records arrival-to-commit latency for ticks that carry an arrival stamp (paced runs).
func (w *fwriter) stampE2E(recs []mtick) {
	now := time.Now().UnixNano()
	for _, r := range recs {
		if r.at != 0 && r.at >= gSkipUntil.Load() {
			w.e2e = append(w.e2e, int32((now-r.at)/1000))
		}
	}
}

// busyStr reports min/max writer busy ratio (time inside write / wall time): the imbalance measure.
func busyStr(secs float64) string {
	if len(gBusy) == 0 {
		return ""
	}
	b := append([]time.Duration(nil), gBusy...)
	sort.Slice(b, func(i, j int) bool { return b[i] < b[j] })
	e := ""
	if len(gE2E) > 0 {
		e = fmt.Sprintf(" e2e_p50_ms=%.2f e2e_p99_ms=%.2f e2e_max_ms=%.2f", pctl(append([]int32(nil), gE2E...), .5), pctl(append([]int32(nil), gE2E...), .99), pctl(gE2E, 1))
	}
	return fmt.Sprintf(" wbusy_min=%.2f wbusy_max=%.2f%s%s", b[0].Seconds()/secs, b[len(b)-1].Seconds()/secs, e, gExtra)
}

type dbEnt struct {
	once sync.Once
	db   *sql.DB
	err  error
}

// dbCache shares one handle per symbol file across all writers (safe: a symbol has one holder at a time).
type dbCache struct {
	mu sync.Mutex
	m  map[int32]*dbEnt
}

func (c *dbCache) get(cfg wcfg, sym int32) (*sql.DB, error) {
	c.mu.Lock()
	e := c.m[sym]
	if e == nil {
		e = &dbEnt{}
		c.m[sym] = e
	}
	c.mu.Unlock()
	e.once.Do(func() {
		path := filepath.Join(cfg.dir, mDate, cfg.names[sym]+".db")
		os.MkdirAll(filepath.Dir(path), 0o755)
		e.db, e.err = createTuned(path, cfg.cacheKB)
	})
	return e.db, e.err
}

// closeAll closes in parallel: Close checkpoints the WAL, and A-tuned writers do it concurrently too.
func (c *dbCache) closeAll(par int) {
	ch := make(chan *sql.DB, len(c.m))
	for _, e := range c.m {
		if e.db != nil {
			ch <- e.db
		}
	}
	close(ch)
	var wg sync.WaitGroup
	for i := 0; i < par; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for db := range ch {
				db.Close()
			}
		}()
	}
	wg.Wait()
}

type symQ struct {
	mu    sync.Mutex
	q     []mtick
	sched bool // queued in ready or held by a writer
	max   int
	last  int64 // last committed ts; touched only by the holder
}

type readySched struct {
	qs      []symQ
	ready   chan int32 // cap = #symbols; sched flag keeps at most one entry per symbol, so sends never block
	pending atomic.Int64
	viol    atomic.Int64
	minB    int
}

func (r *readySched) put(t mtick) {
	r.pending.Add(1)
	q := &r.qs[t.sym]
	q.mu.Lock()
	q.q = append(q.q, t)
	if len(q.q) > q.max {
		q.max = len(q.q)
	}
	push := !q.sched && len(q.q) >= r.minB
	if push {
		q.sched = true
	}
	q.mu.Unlock()
	if push {
		r.ready <- t.sym
	}
}

// sweep schedules symbols whose data waits below minB, so nothing lingers longer than one interval.
func (r *readySched) sweep(every time.Duration, stop <-chan struct{}) {
	tk := time.NewTicker(every)
	defer tk.Stop()
	for {
		select {
		case <-stop:
			return
		case <-tk.C:
		}
		for i := range r.qs {
			q := &r.qs[i]
			q.mu.Lock()
			push := !q.sched && len(q.q) > 0
			if push {
				q.sched = true
			}
			q.mu.Unlock()
			if push {
				r.ready <- int32(i)
			}
		}
	}
}

func (r *readySched) work(w *fwriter, k int) {
	buf := make([]mtick, 0, k)
	for sym := range r.ready {
		q := &r.qs[sym]
		q.mu.Lock()
		n := min(len(q.q), k)
		buf = append(buf[:0], q.q[:n]...)
		if n == len(q.q) {
			q.q = q.q[:0]
		} else {
			q.q = q.q[n:]
		}
		q.mu.Unlock()
		for _, t := range buf {
			if t.ts < q.last {
				r.viol.Add(1)
			}
			q.last = t.ts
		}
		t0 := time.Now()
		if err := w.write(sym, buf); err != nil && w.err == nil {
			w.err = err
		}
		d := time.Since(t0)
		w.lat = append(w.lat, int32(d.Microseconds()))
		w.sizes = append(w.sizes, int32(n))
		w.busy += d
		w.stampE2E(buf)
		q.mu.Lock() // release only after the commit; same lock as put, so no lost wakeup
		if len(q.q) > 0 {
			q.mu.Unlock()
			r.ready <- sym
		} else {
			q.sched = false
			q.mu.Unlock()
		}
		r.pending.Add(-int64(n))
	}
}

func runReady(c runCfg, cfg wcfg, feed func(put func(mtick))) ([]int32, []int32, int, error) {
	cfg.shared = &dbCache{m: map[int32]*dbEnt{}}
	r := &readySched{qs: make([]symQ, c.syms), ready: make(chan int32, c.syms), minB: c.minBatch}
	ws := make([]*fwriter, c.workers)
	var wg sync.WaitGroup
	for i := range ws {
		ws[i] = newFWriter(cfg)
		wg.Add(1)
		go func(w *fwriter) { defer wg.Done(); r.work(w, c.batch) }(ws[i])
	}
	stop := make(chan struct{})
	if c.minBatch > 1 {
		go r.sweep(time.Duration(c.flushMS)*time.Millisecond, stop)
	}
	feed(r.put)
	for r.pending.Load() > 0 {
		time.Sleep(time.Millisecond)
	}
	close(stop)
	close(r.ready)
	wg.Wait()
	cfg.shared.closeAll(c.workers)
	var lat, sizes []int32
	var err error
	gBusy, gE2E = gBusy[:0], gE2E[:0]
	for _, w := range ws {
		lat, sizes = append(lat, w.lat...), append(sizes, w.sizes...)
		gBusy, gE2E = append(gBusy, w.busy), append(gE2E, w.e2e...)
		if w.err != nil {
			err = w.err
		}
	}
	maxQ := 0
	for i := range r.qs {
		maxQ = max(maxQ, r.qs[i].max)
	}
	gExtra = fmt.Sprintf(" order_viol=%d max_symq=%d", r.viol.Load(), maxQ)
	return lat, sizes, 0, err
}

// runAcc is the hash-pinned alternative: per-symbol buffers inside the writer, a symbol is committed
// when it holds minB ticks or at the next flush tick (so a tick waits at most one flush interval).
func (w *fwriter) runAcc() {
	bufs := map[int32][]mtick{}
	tk := time.NewTicker(w.cfg.flush)
	defer tk.Stop()
	defer w.closeAll()
	commitSym := func(s int32) {
		b := bufs[s]
		for len(b) > 0 {
			n := min(len(b), w.cfg.batch)
			t0 := time.Now()
			if err := w.write(s, b[:n]); err != nil && w.err == nil {
				w.err = err
			}
			d := time.Since(t0)
			w.lat = append(w.lat, int32(d.Microseconds()))
			w.sizes = append(w.sizes, int32(n))
			w.busy += d
			w.stampE2E(b[:n])
			b = b[n:]
		}
		bufs[s] = bufs[s][:0]
	}
	flushAll := func() {
		for s, b := range bufs {
			if len(b) > 0 {
				commitSym(s)
			}
		}
	}
	for {
		select {
		case t, ok := <-w.ch:
			if !ok {
				flushAll()
				return
			}
			bufs[t.sym] = append(bufs[t.sym], t)
			if len(bufs[t.sym]) >= w.cfg.minB {
				commitSym(t.sym)
			}
		case <-tk.C:
			flushAll()
		}
	}
}
