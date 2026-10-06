package main

// Write-path harness for comparing three storage architectures (see docs/perf-storage-modes.md).
// It is dispatched from init() so main.go stays untouched and uses its own transaction code
// (a copy of the product's per-batch transaction) so tuning variants need no product change.
//
//	tclient modes-run  -mode A-prod|A-cur|A-tuned|B|C-a|C-b|C-router -syms N -ticks N -dir D [...]
//	tclient modes-read -layout A|B -dir D -syms N -n N -m N          (read-side sanity)
//	tclient modes-child ...                                          (internal: one C writer process)

import (
	"bufio"
	"container/list"
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"hash/fnv"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"ntick/internal/ingest"
	"ntick/internal/query"
	"ntick/internal/store"
	"ntick/internal/tick"
	"ntick/internal/wire"
)

func init() {
	if len(os.Args) < 2 {
		return
	}
	switch os.Args[1] {
	case "modes-run":
		modesRun(os.Args[2:])
	case "modes-child":
		modesChild(os.Args[2:])
	case "modes-read":
		modesRead(os.Args[2:])
	default:
		return
	}
	os.Exit(0)
}

const (
	mDate      = "20260901"
	minAvailKB = 1200 * 1024 // abort a run when MemAvailable drops below 1.2 GiB
)

var (
	mKST      = time.FixedZone("KST", 9*3600)
	mMidnight = time.Date(2026, 9, 1, 0, 0, 0, 0, mKST).UnixMilli()
	mBase     = mMidnight + 9*3600*1000
)

type mtick struct {
	sym            int32
	ts, price, qty int64
}

func symName(i int) string { return "S" + strconv.Itoa(100000+i) }

// genTicks builds the deterministic input: ts spreads evenly over 6 h of one day, all ticks valid.
// uniform = round-robin symbols; zipf = Zipf(s=1) hot-symbol distribution sampled with a fixed seed.
func genTicks(syms, n int, dist string) []mtick {
	out := make([]mtick, n)
	var cdf []float64
	if dist == "zipf" {
		cdf = make([]float64, syms)
		sum := 0.0
		for i := range cdf {
			sum += 1 / float64(i+1)
			cdf[i] = sum
		}
		for i := range cdf {
			cdf[i] /= sum
		}
	}
	rnd := rand.New(rand.NewSource(1))
	for i := range out {
		s := i % syms
		if cdf != nil {
			s = sort.SearchFloat64s(cdf, rnd.Float64())
			if s >= syms {
				s = syms - 1
			}
		}
		out[i] = mtick{int32(s), mBase + int64(i)*21600000/int64(n), 10000 + int64((i*7+s*13)%5000), 1 + int64(i%997)}
	}
	return out
}

func memAvailKB() int64 {
	b, _ := os.ReadFile("/proc/meminfo")
	for _, l := range strings.Split(string(b), "\n") {
		if strings.HasPrefix(l, "MemAvailable:") {
			v, _ := strconv.ParseInt(strings.Fields(l)[1], 10, 64)
			return v
		}
	}
	return 1 << 40
}

func procStatusKB(field string) int64 {
	b, _ := os.ReadFile("/proc/self/status")
	for _, l := range strings.Split(string(b), "\n") {
		if strings.HasPrefix(l, field+":") {
			v, _ := strconv.ParseInt(strings.Fields(l)[1], 10, 64)
			return v
		}
	}
	return 0
}

// ioWriteBytes is the number of bytes this process caused to be sent to storage (/proc/self/io write_bytes).
func ioWriteBytes() int64 {
	b, _ := os.ReadFile("/proc/self/io")
	for _, l := range strings.Split(string(b), "\n") {
		if strings.HasPrefix(l, "write_bytes:") {
			v, _ := strconv.ParseInt(strings.Fields(l)[1], 10, 64)
			return v
		}
	}
	return 0
}

func meanMS(v []int32) float64 {
	if len(v) == 0 {
		return 0
	}
	var t float64
	for _, x := range v {
		t += float64(x)
	}
	return t / float64(len(v)) / 1000
}

func selfCPU() time.Duration {
	var ru syscall.Rusage
	syscall.Getrusage(syscall.RUSAGE_SELF, &ru)
	return time.Duration(ru.Utime.Nano() + ru.Stime.Nano())
}

func childCPU() time.Duration {
	var ru syscall.Rusage
	syscall.Getrusage(syscall.RUSAGE_CHILDREN, &ru)
	return time.Duration(ru.Utime.Nano() + ru.Stime.Nano())
}

func dirBytes(dir string) int64 {
	var n int64
	filepath.Walk(dir, func(_ string, fi os.FileInfo, err error) error {
		if err == nil && !fi.IsDir() {
			n += fi.Size()
		}
		return nil
	})
	return n
}

// sampler tracks the maximum fd and thread count of this process and enforces the memory floor.
type sampler struct {
	maxFD, maxThr atomic.Int64
	stop          chan struct{}
	onLowMem      func()
}

func startSampler(onLowMem func()) *sampler {
	s := &sampler{stop: make(chan struct{}), onLowMem: onLowMem}
	go func() {
		for {
			if ents, err := os.ReadDir("/proc/self/fd"); err == nil && int64(len(ents)) > s.maxFD.Load() {
				s.maxFD.Store(int64(len(ents)))
			}
			if t := procStatusKB("Threads"); t > s.maxThr.Load() {
				s.maxThr.Store(t)
			}
			if memAvailKB() < minAvailKB {
				fmt.Println("ABORT low memory")
				s.onLowMem()
				os.Exit(3)
			}
			select {
			case <-s.stop:
				return
			case <-time.After(150 * time.Millisecond):
			}
		}
	}()
	return s
}

func pctl(v []int32, p float64) float64 {
	if len(v) == 0 {
		return 0
	}
	sort.Slice(v, func(i, j int) bool { return v[i] < v[j] })
	return float64(v[min(len(v)-1, int(float64(len(v))*p))]) / 1000
}

// ---- per-symbol-file writer (modes A and C), a copy of the product transaction ----

const upsertStats = `
INSERT INTO day_stats (id, valid_count, last_raw_seq) VALUES (1, ?, last_insert_rowid())
ON CONFLICT (id) DO UPDATE SET
  valid_count  = valid_count + excluded.valid_count,
  last_raw_seq = excluded.last_raw_seq`

var bg = context.Background()

type wcfg struct {
	batch   int
	flush   time.Duration
	pool    int // max open files per writer
	tuned   bool
	cacheKB int // 0 = SQLite default
	dir     string
	names   []string // symbol names by index
	shared  *dbCache // A-ready: DB handles shared by all writers (nil = per-writer pool)
}

func createTuned(path string, cacheKB int) (*sql.DB, error) {
	if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
		tmp := path + ".tmp"
		db, err := sql.Open(store.Driver, "file:"+tmp+"?_pragma=journal_mode(DELETE)")
		if err != nil {
			return nil, err
		}
		db.SetMaxOpenConns(1)
		_, err = db.Exec("BEGIN;\n" + store.Schema + "\nCOMMIT;")
		db.Close()
		if err != nil {
			return nil, err
		}
		if err := os.Rename(tmp, path); err != nil {
			return nil, err
		}
	}
	dsn := store.DSN(path)
	if cacheKB > 0 {
		dsn += "&_pragma=cache_size(-" + strconv.Itoa(cacheKB) + ")"
	}
	db, err := sql.Open(store.Driver, dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	_, err = db.Exec(store.Schema)
	return db, err
}

type fwriter struct {
	cfg      wcfg
	ch       chan mtick
	pool     map[int32]*list.Element
	lru      *list.List
	lat      []int32 // commit latency in us
	sizes    []int32 // ticks per commit
	err      error
	wg       sync.WaitGroup
	opens    int
	openTime time.Duration
	busy     time.Duration // time spent inside write
}

type pent struct {
	sym int32
	db  *sql.DB
}

func newFWriter(cfg wcfg) *fwriter {
	return &fwriter{cfg: cfg, ch: make(chan mtick, 4096), pool: map[int32]*list.Element{}, lru: list.New()}
}

func (w *fwriter) start() {
	w.wg.Add(1)
	go func() { defer w.wg.Done(); w.run() }()
}

func (w *fwriter) run() {
	var batch []mtick
	tk := time.NewTicker(w.cfg.flush)
	defer tk.Stop()
	defer w.closeAll()
	flush := func() {
		if len(batch) > 0 {
			w.commit(batch)
			batch = batch[:0]
		}
	}
	for {
		select {
		case t, ok := <-w.ch:
			if !ok {
				flush()
				return
			}
			batch = append(batch, t)
			if len(batch) >= w.cfg.batch {
				flush()
			}
		case <-tk.C:
			flush()
		}
	}
}

func (w *fwriter) commit(batch []mtick) {
	groups := map[int32][]mtick{}
	var order []int32
	for _, t := range batch {
		if _, ok := groups[t.sym]; !ok {
			order = append(order, t.sym)
		}
		groups[t.sym] = append(groups[t.sym], t)
	}
	for _, s := range order {
		t0 := time.Now()
		if err := w.write(s, groups[s]); err != nil && w.err == nil {
			w.err = err
		}
		w.lat = append(w.lat, int32(time.Since(t0).Microseconds()))
		w.busy += time.Since(t0)
		w.sizes = append(w.sizes, int32(len(groups[s])))
	}
}

func (w *fwriter) open(sym int32) (*sql.DB, error) {
	if w.cfg.shared != nil {
		return w.cfg.shared.get(w.cfg, sym)
	}
	if e, ok := w.pool[sym]; ok {
		w.lru.MoveToFront(e)
		return e.Value.(*pent).db, nil
	}
	path := filepath.Join(w.cfg.dir, mDate, w.cfg.names[sym]+".db")
	os.MkdirAll(filepath.Dir(path), 0o755)
	t0 := time.Now()
	var db *sql.DB
	var err error
	if w.cfg.tuned {
		db, err = createTuned(path, w.cfg.cacheKB)
	} else {
		db, err = store.Create(store.Driver, path)
	}
	w.opens++
	w.openTime += time.Since(t0)
	if err != nil {
		return nil, err
	}
	w.pool[sym] = w.lru.PushFront(&pent{sym, db})
	if w.lru.Len() > w.cfg.pool {
		e := w.lru.Remove(w.lru.Back()).(*pent)
		delete(w.pool, e.sym)
		e.db.Close()
	}
	return db, nil
}

func (w *fwriter) write(sym int32, recs []mtick) (err error) {
	db, err := w.open(sym)
	if err != nil {
		return err
	}
	conn, err := db.Conn(bg)
	if err != nil {
		return err
	}
	defer conn.Close()
	if _, err = conn.ExecContext(bg, "BEGIN IMMEDIATE"); err != nil {
		return err
	}
	defer func() {
		if err != nil {
			conn.ExecContext(bg, "ROLLBACK")
		}
	}()
	ins, err := conn.PrepareContext(bg, "INSERT INTO ticks (ts, price, qty, valid) VALUES (?, ?, ?, 1)")
	if err != nil {
		return err
	}
	defer ins.Close()
	candles := map[int64]*store.Candle{}
	for _, r := range recs {
		m := (r.ts - mMidnight) / 60000
		c := candles[m]
		if c == nil {
			c = &store.Candle{}
			candles[m] = c
		}
		c.Add(r.ts, r.price, r.qty)
		if _, err = ins.ExecContext(bg, r.ts, r.price, r.qty); err != nil {
			return err
		}
	}
	if _, err = conn.ExecContext(bg, upsertStats, int64(len(recs))); err != nil {
		return err
	}
	for m, c := range candles {
		if err = store.UpsertCandle(bg, conn, m, c); err != nil {
			return err
		}
	}
	_, err = conn.ExecContext(bg, "COMMIT")
	return err
}

func (w *fwriter) closeAll() {
	for e := w.lru.Front(); e != nil; e = e.Next() {
		e.Value.(*pent).db.Close()
	}
}

// ---- single-file writer (mode B) ----

const bSchema = `
CREATE TABLE IF NOT EXISTS symbols (symbol_id INTEGER PRIMARY KEY, name TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS ticks (
  raw_seq INTEGER PRIMARY KEY, symbol_id INTEGER NOT NULL,
  ts INTEGER NOT NULL, price INTEGER NOT NULL, qty INTEGER NOT NULL, valid INTEGER NOT NULL);
CREATE INDEX IF NOT EXISTS ix_ticks_sym ON ticks(symbol_id, raw_seq) WHERE valid = 1;
CREATE TABLE IF NOT EXISTS day_stats (
  symbol_id INTEGER PRIMARY KEY, valid_count INTEGER NOT NULL, last_raw_seq INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS candle_1m (
  symbol_id INTEGER NOT NULL, minute INTEGER NOT NULL,
  open INTEGER, high INTEGER, low INTEGER, close INTEGER, open_ts INTEGER, close_ts INTEGER,
  volume INTEGER, tick_count INTEGER, PRIMARY KEY (symbol_id, minute)) WITHOUT ROWID;`

const bUpsertStats = `
INSERT INTO day_stats (symbol_id, valid_count, last_raw_seq) VALUES (?, ?, ?)
ON CONFLICT (symbol_id) DO UPDATE SET
  valid_count  = valid_count + excluded.valid_count,
  last_raw_seq = excluded.last_raw_seq`

const bUpsertCandle = `
INSERT INTO candle_1m (symbol_id, minute, open, high, low, close, open_ts, close_ts, volume, tick_count)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT (symbol_id, minute) DO UPDATE SET
  open     = CASE WHEN excluded.open_ts < open_ts THEN excluded.open ELSE open END,
  open_ts  = min(open_ts, excluded.open_ts),
  close    = CASE WHEN excluded.close_ts >= close_ts THEN excluded.close ELSE close END,
  close_ts = max(close_ts, excluded.close_ts),
  high     = max(high, excluded.high),
  low      = min(low, excluded.low),
  volume   = volume + excluded.volume,
  tick_count = tick_count + excluded.tick_count`

type bwriter struct {
	conn               *sql.Conn // one dedicated connection, statements prepared once
	ins, stats, candle *sql.Stmt
	db                 *sql.DB
	cfg                wcfg
	ch                 chan mtick
	lat                []int32
	sizes              []int32
	err                error
	seen               map[int32]bool
	wg                 sync.WaitGroup
}

func openB(path string, cacheKB int) (*sql.DB, error) {
	os.MkdirAll(filepath.Dir(path), 0o755)
	dsn := store.DSN(path)
	if cacheKB > 0 {
		dsn += "&_pragma=cache_size(-" + strconv.Itoa(cacheKB) + ")"
	}
	db, err := sql.Open(store.Driver, dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	_, err = db.Exec("BEGIN;\n" + bSchema + "\nCOMMIT;")
	return db, err
}

func newBWriter(db *sql.DB, cfg wcfg) *bwriter {
	return &bwriter{db: db, cfg: cfg, ch: make(chan mtick, 4096), seen: map[int32]bool{}}
}

func (w *bwriter) start() {
	var err error
	if w.conn, err = w.db.Conn(bg); err == nil {
		w.ins, err = w.conn.PrepareContext(bg, "INSERT INTO ticks (symbol_id, ts, price, qty, valid) VALUES (?, ?, ?, ?, 1)")
	}
	if err == nil {
		w.stats, err = w.conn.PrepareContext(bg, bUpsertStats)
	}
	if err == nil {
		w.candle, err = w.conn.PrepareContext(bg, bUpsertCandle)
	}
	if err != nil {
		panic(err)
	}
	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		defer func() { // release the only connection so a later writer (steady-state warm pass) can use it
			w.ins.Close()
			w.stats.Close()
			w.candle.Close()
			w.conn.Close()
		}()
		var batch []mtick
		tk := time.NewTicker(w.cfg.flush)
		defer tk.Stop()
		flush := func() {
			if len(batch) > 0 {
				t0 := time.Now()
				if err := w.write(batch); err != nil && w.err == nil {
					w.err = err
				}
				w.lat = append(w.lat, int32(time.Since(t0).Microseconds()))
				w.sizes = append(w.sizes, int32(len(batch)))
				batch = batch[:0]
			}
		}
		for {
			select {
			case t, ok := <-w.ch:
				if !ok {
					flush()
					return
				}
				batch = append(batch, t)
				if len(batch) >= w.cfg.batch {
					flush()
				}
			case <-tk.C:
				flush()
			}
		}
	}()
}

type bkey struct {
	sym    int32
	minute int64
}

func (w *bwriter) write(recs []mtick) (err error) {
	conn := w.conn
	if _, err = conn.ExecContext(bg, "BEGIN IMMEDIATE"); err != nil {
		return err
	}
	defer func() {
		if err != nil {
			conn.ExecContext(bg, "ROLLBACK")
		}
	}()
	ins := w.ins
	candles := map[bkey]*store.Candle{}
	counts := map[int32]int64{}
	last := map[int32]int64{}
	for _, r := range recs {
		if !w.seen[r.sym] {
			w.seen[r.sym] = true
			if _, err = conn.ExecContext(bg, "INSERT OR IGNORE INTO symbols (symbol_id, name) VALUES (?, ?)", r.sym, w.cfg.names[r.sym]); err != nil {
				return err
			}
		}
		k := bkey{r.sym, (r.ts - mMidnight) / 60000}
		c := candles[k]
		if c == nil {
			c = &store.Candle{}
			candles[k] = c
		}
		c.Add(r.ts, r.price, r.qty)
		res, err := ins.ExecContext(bg, r.sym, r.ts, r.price, r.qty)
		if err != nil {
			return err
		}
		id, _ := res.LastInsertId()
		counts[r.sym]++
		last[r.sym] = id
	}
	for s, n := range counts {
		if _, err = w.stats.ExecContext(bg, s, n, last[s]); err != nil {
			return err
		}
	}
	for k, c := range candles {
		if _, err = w.candle.ExecContext(bg, k.sym, k.minute, c.Open, c.High, c.Low, c.Close, c.OpenTS, c.CloseTS, c.Volume, c.Count); err != nil {
			return err
		}
	}
	_, err = conn.ExecContext(bg, "COMMIT")
	return err
}

// ---- runner ----

type runCfg struct {
	mode, dist, phase, dir string
	syms, ticks, batch     int
	flushMS, cacheKB       int
	workers, pool          int
	minBatch               int
	keep                   bool
}

func modesRun(args []string) {
	fs := flag.NewFlagSet("modes-run", flag.ExitOnError)
	var c runCfg
	fs.StringVar(&c.mode, "mode", "A-cur", "A-prod|A-cur|A-tuned|B|C-a|C-b|C-router")
	fs.IntVar(&c.syms, "syms", 100, "symbols")
	fs.IntVar(&c.ticks, "ticks", 1000000, "ticks")
	fs.StringVar(&c.dist, "dist", "uniform", "uniform|zipf")
	fs.StringVar(&c.phase, "phase", "cold", "cold|steady (steady: files created by a 1-tick-per-symbol warm pass)")
	fs.StringVar(&c.dir, "dir", "", "data dir (created, must be empty)")
	fs.IntVar(&c.batch, "batch", 500, "max batch")
	fs.IntVar(&c.flushMS, "flush", 50, "flush ms")
	fs.IntVar(&c.cacheKB, "cache", 0, "cache_size KB (0 = default)")
	fs.IntVar(&c.workers, "workers", 12, "workers (A)")
	fs.IntVar(&c.minBatch, "minbatch", 1, "min queued ticks before a symbol becomes ready (A-ready); a sweeper still schedules older data every -flush ms")
	fs.IntVar(&c.pool, "pool", 64, "per-worker pool (A); 0 = unlimited")
	fs.BoolVar(&c.keep, "keep", false, "keep data dir")
	fs.Parse(args)
	if c.dir == "" {
		fmt.Println("need -dir")
		os.Exit(2)
	}
	os.RemoveAll(c.dir)
	os.MkdirAll(c.dir, 0o755)
	if !c.keep {
		defer os.RemoveAll(c.dir)
	}
	names := make([]string, c.syms)
	for i := range names {
		names[i] = symName(i)
	}
	if strings.HasPrefix(c.mode, "C-") {
		runC(c, names)
		return
	}
	ticks := genTicks(c.syms, c.ticks, c.dist)
	wcfg := wcfg{batch: c.batch, flush: time.Duration(c.flushMS) * time.Millisecond, pool: c.pool, tuned: c.mode == "A-tuned" || c.mode == "A-ready", cacheKB: c.cacheKB, dir: c.dir, names: names}
	if wcfg.pool == 0 {
		wcfg.pool = 1 << 30
	}
	// warm pass for steady: one tick per symbol, slightly earlier than the measured input
	warm := func(put func(mtick)) {
		for s := 0; s < c.syms; s++ {
			put(mtick{int32(s), mBase - 1000, 10000, 1})
		}
	}
	rss0 := procStatusKB("VmRSS")
	smp := startSampler(func() {})
	defer close(smp.stop)
	var peakDir atomic.Int64
	go func() { // peak on-disk size (db + wal + shm) while running
		for {
			if b := dirBytes(c.dir); b > peakDir.Load() {
				peakDir.Store(b)
			}
			select {
			case <-smp.stop:
				return
			case <-time.After(time.Second):
			}
		}
	}()

	var run func(feed func(put func(mtick))) (lat, sizes []int32, opens int, err error)
	switch {
	case c.mode == "A-prod":
		loc := mKST
		run = func(feed func(put func(mtick))) ([]int32, []int32, int, error) {
			in := ingest.New(c.dir, loc)
			t0 := time.Now()
			feed(func(t mtick) {
				in.Put(tick.Tick{Symbol: names[t.sym], TS: t.ts, Price: t.price, Qty: t.qty})
			})
			_ = t0
			in.Close()
			return nil, nil, 0, nil
		}
	case c.mode == "A-ready":
		run = func(feed func(put func(mtick))) ([]int32, []int32, int, error) {
			return runReady(c, wcfg, feed)
		}
	case strings.HasPrefix(c.mode, "A-"):
		run = func(feed func(put func(mtick))) ([]int32, []int32, int, error) {
			ws := make([]*fwriter, c.workers)
			route := make([]int, c.syms)
			for i := range ws {
				ws[i] = newFWriter(wcfg)
				ws[i].start()
			}
			for s := range route {
				h := fnv.New32a()
				h.Write([]byte(names[s]))
				route[s] = int(h.Sum32() % uint32(c.workers))
			}
			feed(func(t mtick) { ws[route[t.sym]].ch <- t })
			var lat, sizes []int32
			var opens int
			var err error
			for _, w := range ws {
				close(w.ch)
			}
			gBusy = gBusy[:0]
			for _, w := range ws {
				w.wg.Wait()
				gBusy = append(gBusy, w.busy)
				lat, sizes, opens = append(lat, w.lat...), append(sizes, w.sizes...), opens+w.opens
				if w.err != nil {
					err = w.err
				}
			}
			return lat, sizes, opens, err
		}
	case c.mode == "B":
		var db *sql.DB
		db, err := openB(filepath.Join(c.dir, mDate, "ALL.db"), c.cacheKB)
		if err != nil {
			fmt.Println("ERR", err)
			os.Exit(1)
		}
		run = func(feed func(put func(mtick))) ([]int32, []int32, int, error) {
			w := newBWriter(db, wcfg)
			w.start()
			feed(func(t mtick) { w.ch <- t })
			close(w.ch)
			w.wg.Wait()
			return w.lat, w.sizes, 0, w.err
		}
		defer db.Close()
	default:
		fmt.Println("bad mode")
		os.Exit(2)
	}

	var warmSecs float64
	if c.phase == "steady" {
		t0 := time.Now()
		if _, _, _, err := run(warm); err != nil {
			fmt.Println("ERR warm", err)
			os.Exit(1)
		}
		warmSecs = time.Since(t0).Seconds()
		if c.mode == "B" { // B's writer closes nothing; reopen is unnecessary
		}
	}
	cpu0, io0 := selfCPU(), ioWriteBytes()
	t0 := time.Now()
	lat, sizes, opens, err := run(func(put func(mtick)) {
		for _, t := range ticks {
			put(t)
		}
	})
	secs := time.Since(t0).Seconds()
	cpu := (selfCPU() - cpu0).Seconds()
	wbytes := ioWriteBytes() - io0
	if err != nil {
		fmt.Println("ERR", err)
		os.Exit(1)
	}
	var avg float64
	for _, s := range sizes {
		avg += float64(s)
	}
	if len(sizes) > 0 {
		avg /= float64(len(sizes))
	}
	wal := dirBytes(c.dir) // after Close: WAL folded into db (peak WAL measured separately via ls)
	fmt.Printf("RESULT mode=%s syms=%d dist=%s phase=%s ticks=%d batch=%d secs=%.3f rate=%.0f cpu_pct=%.0f rss_net_mb=%.0f fd_max=%d thr_max=%d commits=%d avg_ticks_per_commit=%.1f lat_p50_ms=%.2f lat_p99_ms=%.2f lat_mean_ms=%.2f opens=%d disk_bytes_per_tick=%.1f peak_disk_mb=%.0f io_write_bytes_per_tick=%.1f warm_s=%.1f%s\n",
		c.mode, c.syms, c.dist, c.phase, c.ticks, c.batch, secs, float64(c.ticks)/secs, cpu/secs*100,
		float64(procStatusKB("VmHWM")-rss0)/1024, smp.maxFD.Load(), smp.maxThr.Load(), len(sizes), avg,
		pctl(append([]int32(nil), lat...), .5), pctl(lat, .99), meanMS(lat), opens, float64(wal)/float64(c.ticks), float64(peakDir.Load())/1e6, float64(wbytes)/float64(c.ticks), warmSecs, busyStr(secs))
}

// ---- mode C: one writer process per symbol ----

type childRes struct {
	line string
	kv   map[string]float64
}

func parseKV(line string) map[string]float64 {
	m := map[string]float64{}
	for _, f := range strings.Fields(line) {
		if k, v, ok := strings.Cut(f, "="); ok {
			if x, err := strconv.ParseFloat(v, 64); err == nil {
				m[k] = x
			}
		}
	}
	return m
}

func runC(c runCfg, names []string) {
	self, _ := os.Executable()
	router := c.mode == "C-b" || c.mode == "C-router"
	// per-symbol tick counts for C-a (self-generated input)
	counts := make([]int, c.syms)
	if c.dist == "zipf" {
		sum := 0.0
		for i := range counts {
			sum += 1 / float64(i+1)
		}
		rem := c.ticks
		for i := range counts {
			counts[i] = int(float64(c.ticks) / float64(i+1) / sum)
			rem -= counts[i]
		}
		counts[0] += rem
	} else {
		for i := range counts {
			counts[i] = c.ticks / c.syms
		}
		counts[0] += c.ticks - c.ticks/c.syms*c.syms
	}
	type child struct {
		cmd   *exec.Cmd
		stdin *os.File
		out   *bufio.Reader
		pw    *os.File // data pipe write end (router)
		res   chan childRes
	}
	var kids []*child
	killAll := func() {
		for _, k := range kids {
			k.cmd.Process.Kill()
		}
	}
	defer killAll() // never leave stray processes
	smp := startSampler(killAll)
	defer close(smp.stop)
	for i := 0; i < c.syms; i++ {
		if memAvailKB() < minAvailKB+200*1024 {
			fmt.Printf("ABORT low memory while spawning child %d\n", i)
			return
		}
		args := []string{"modes-child", "-name", names[i], "-dir", c.dir, "-batch", strconv.Itoa(c.batch), "-flush", strconv.Itoa(c.flushMS), "-cache", strconv.Itoa(c.cacheKB)}
		if router {
			args = append(args, "-n", "0")
		} else {
			args = append(args, "-n", strconv.Itoa(counts[i]), "-total", strconv.Itoa(c.ticks))
		}
		if c.mode == "C-router" {
			args = append(args, "-discard")
		}
		if c.cacheKB > 0 {
			args = append(args, "-tuned")
		}
		cmd := exec.Command(self, args...)
		in, _ := cmd.StdinPipe()
		out, _ := cmd.StdoutPipe()
		cmd.Stderr = os.Stderr
		k := &child{cmd: cmd, res: make(chan childRes, 1)}
		if router {
			pr, pw, _ := os.Pipe()
			cmd.ExtraFiles = []*os.File{pr}
			k.pw = pw
			defer pr.Close()
		}
		if err := cmd.Start(); err != nil {
			fmt.Println("ERR spawn", err)
			return
		}
		k.stdin, k.out = in.(*os.File), bufio.NewReader(out)
		kids = append(kids, k)
		if l, _ := k.out.ReadString('\n'); !strings.HasPrefix(l, "READY") {
			fmt.Println("ERR child not ready:", l)
			return
		}
	}
	var warmRes []childRes
	_ = warmRes
	// all children ready: steady phase = children create their file during READY? no: they create it at GO.
	rss0 := procStatusKB("VmRSS")
	memBefore := memAvailKB()
	var data []byte
	var global []mtick
	if router {
		global = genTicks(c.syms, c.ticks, c.dist)
		data = make([]byte, 0, c.ticks*wire.Size)
		for _, t := range global {
			b, _ := wire.Encode(wire.Message{Symbol: names[t.sym], Ts: t.ts, Price: t.price, Qty: t.qty})
			data = append(data, b...)
		}
		global = nil
	}
	if c.phase == "steady" {
		for _, k := range kids {
			fmt.Fprintln(k.stdin, "PREP") // child creates its db file and reports
		}
		for _, k := range kids {
			k.out.ReadString('\n')
		}
	}
	cpu0, ccpu0 := selfCPU(), childCPU()
	t0 := time.Now()
	for _, k := range kids {
		fmt.Fprintln(k.stdin, "GO")
	}
	for _, k := range kids {
		k := k
		go func() {
			l, _ := k.out.ReadString('\n')
			k.res <- childRes{l, parseKV(l)}
		}()
	}
	if router {
		// router: decode, map symbol -> owner, buffer, hand 4 KB chunks to one sender goroutine per child
		idx := map[string]int{}
		for i, n := range names {
			idx[n] = i
		}
		chans := make([]chan []byte, len(kids))
		var swg sync.WaitGroup
		for i, k := range kids {
			chans[i] = make(chan []byte, 64)
			swg.Add(1)
			go func(ch chan []byte, f *os.File) {
				defer swg.Done()
				for b := range ch {
					f.Write(b)
				}
				f.Close()
			}(chans[i], k.pw)
		}
		bufs := make([][]byte, len(kids))
		last := time.Now()
		sendAll := func() {
			for i, b := range bufs {
				if len(b) > 0 {
					chans[i] <- b
					bufs[i] = nil
				}
			}
		}
		for i := 0; i < c.ticks; i++ {
			m, err := wire.Decode(data[i*wire.Size : (i+1)*wire.Size])
			if err != nil {
				continue
			}
			o := idx[m.Symbol]
			bufs[o] = append(bufs[o], data[i*wire.Size:(i+1)*wire.Size]...)
			if len(bufs[o]) >= 4096 {
				chans[o] <- bufs[o]
				bufs[o] = nil
			}
			if i&255 == 0 && time.Since(last) > 2*time.Millisecond {
				sendAll()
				last = time.Now()
			}
		}
		sendAll()
		for _, ch := range chans {
			close(ch)
		}
		swg.Wait()
	}
	routerSecs := time.Since(t0).Seconds()
	routerCPU := (selfCPU() - cpu0).Seconds()
	var agg = map[string]float64{}
	var p50s, p99s []float64
	ok := 0
	var rssSum float64
	for _, k := range kids {
		r := <-k.res
		if r.kv["ticks"] == 0 && c.mode != "C-router" {
			fmt.Println("ERR child result:", r.line)
			continue
		}
		ok++
		for _, f := range []string{"ticks", "cpu_s", "commits", "fd_max", "thr_max", "bytes", "wbytes"} {
			agg[f] += r.kv[f]
		}
		rssSum += r.kv["hwm_mb"]
		p50s = append(p50s, r.kv["lat_p50_ms"])
		p99s = append(p99s, r.kv["lat_p99_ms"])
	}
	secs := time.Since(t0).Seconds()
	for _, k := range kids {
		k.cmd.Wait()
	}
	_ = ccpu0
	sort.Float64s(p50s)
	sort.Float64s(p99s)
	if len(p99s) == 0 {
		p99s = []float64{0}
	}
	med := func(v []float64) float64 {
		if len(v) == 0 {
			return 0
		}
		return v[len(v)/2]
	}
	wal := dirBytes(c.dir)
	hold := float64(memBefore-memAvailKB()) / 1024
	_ = hold
	avg := 0.0
	if agg["commits"] > 0 {
		avg = agg["ticks"] / agg["commits"]
	}
	fmt.Printf("RESULT mode=%s syms=%d dist=%s phase=%s ticks=%d batch=%d secs=%.3f rate=%.0f cpu_pct=%.0f rss_net_mb=%.0f rss_per_child_mb=%.1f fd_max=%.0f thr_max=%.0f commits=%.0f avg_ticks_per_commit=%.1f lat_p50_ms=%.2f lat_p99_ms=%.2f io_write_bytes_per_tick=%.1f children_ok=%d router_cpu_pct=%.0f router_secs=%.3f disk_bytes_per_tick=%.1f\n",
		c.mode, c.syms, c.dist, c.phase, c.ticks, c.batch, secs, agg["ticks"]/secs,
		(agg["cpu_s"]+routerCPU)/secs*100, rssSum+float64(procStatusKB("VmHWM")-rss0)/1024, rssSum/float64(max(ok, 1)),
		agg["fd_max"]+float64(smp.maxFD.Load()), agg["thr_max"]+float64(smp.maxThr.Load()), agg["commits"], avg, med(p50s), p99s[max(len(p99s)-1, 0):][0], agg["wbytes"]/float64(c.ticks),
		ok, routerCPU/routerSecs*100, routerSecs, float64(wal)/float64(c.ticks))
}

func modesChild(args []string) {
	fs := flag.NewFlagSet("modes-child", flag.ExitOnError)
	name := fs.String("name", "S100000", "symbol")
	dir := fs.String("dir", "", "")
	n := fs.Int("n", 0, "self-generated ticks (0 = read wire stream from fd 3)")
	total := fs.Int("total", 0, "total ticks of the whole run (for ts spacing)")
	batch := fs.Int("batch", 500, "")
	flushMS := fs.Int("flush", 50, "")
	cache := fs.Int("cache", 0, "")
	tuned := fs.Bool("tuned", false, "")
	discard := fs.Bool("discard", false, "")
	fs.Parse(args)
	_ = total
	cfg := wcfg{batch: *batch, flush: time.Duration(*flushMS) * time.Millisecond, pool: 1, tuned: *tuned, cacheKB: *cache, dir: *dir, names: []string{*name}}
	var own []mtick
	for k := 0; k < *n; k++ { // this symbol's own ticks, spread evenly over the same 6 h
		own = append(own, mtick{0, mBase + int64(k)*21600000/int64(max(*n, 1)), 10000 + int64((k*7)%5000), 1 + int64(k%997)})
	}
	in := bufio.NewReader(os.Stdin)
	fmt.Println("READY")
	prep := func() *fwriter {
		w := newFWriter(cfg)
		return w
	}
	w := prep()
	for {
		l, err := in.ReadString('\n')
		if err != nil {
			os.Exit(1)
		}
		if strings.HasPrefix(l, "PREP") {
			w.open(0)
			fmt.Println("PREPPED")
			continue
		}
		break // GO
	}
	cpu0, io0 := selfCPU(), ioWriteBytes()
	t0 := time.Now()
	smp := startSampler(func() {})
	var cnt int
	if !*discard {
		w.start()
	}
	if *n > 0 {
		for _, t := range own {
			w.ch <- t
		}
		cnt = *n
	} else {
		r := wire.NewReader(bufio.NewReaderSize(os.NewFile(3, "pipe"), 64<<10))
		for {
			m, err := r.Next()
			if err != nil {
				break
			}
			cnt++
			if !*discard {
				w.ch <- mtick{0, m.Ts, m.Price, m.Qty}
			}
		}
	}
	if !*discard {
		close(w.ch)
		w.wg.Wait()
	}
	secs := time.Since(t0).Seconds()
	close(smp.stop)
	size := dirBytes(*dir)
	fmt.Printf("RESULT ticks=%d secs=%.3f cpu_s=%.3f hwm_mb=%.1f fd_max=%d thr_max=%d commits=%d lat_p50_ms=%.2f lat_p99_ms=%.2f lat_mean_ms=%.2f bytes=%d wbytes=%d\n",
		cnt, secs, (selfCPU() - cpu0).Seconds(), float64(procStatusKB("VmHWM"))/1024, smp.maxFD.Load(), smp.maxThr.Load(), len(w.sizes),
		pctl(append([]int32(nil), w.lat...), .5), pctl(w.lat, .99), meanMS(w.lat), size, ioWriteBytes()-io0)
}

// ---- read-side sanity ----

func modesRead(args []string) {
	fs := flag.NewFlagSet("modes-read", flag.ExitOnError)
	layout := fs.String("layout", "A", "A (per-symbol files) or B (one file)")
	dir := fs.String("dir", "", "")
	sym := fs.Int("sym", 7, "symbol index")
	n := fs.Int("n", 1000, "")
	m := fs.Int("m", 200, "")
	iters := fs.Int("iters", 30, "")
	fs.Parse(args)
	name := symName(*sym)
	var one func() (int, int, error)
	if *layout == "A" {
		one = func() (int, int, error) {
			cs, err := query.NTick(bg, *dir, name, *n, *m)
			rows := 0
			for _, c := range cs {
				rows += c.TickCount
			}
			return len(cs), rows, err
		}
	} else {
		one = func() (int, int, error) { return bNTick(*dir, int32(*sym), *n, *m) }
	}
	for i := 0; i < 3; i++ {
		one()
	}
	var ds []int32
	var cs, rows int
	for i := 0; i < *iters; i++ {
		t0 := time.Now()
		var err error
		if cs, rows, err = one(); err != nil {
			fmt.Println("ERR", err)
			os.Exit(1)
		}
		ds = append(ds, int32(time.Since(t0).Microseconds()))
	}
	fmt.Printf("READ layout=%s n=%d m=%d candles=%d rows=%d p50_ms=%.2f p99_ms=%.2f\n", *layout, *n, *m, cs, rows, pctl(append([]int32(nil), ds...), .5), pctl(ds, .99))
}

func bNTick(dir string, sym int32, n, m int) (int, int, error) {
	db, err := sql.Open(store.Driver, "file:"+filepath.Join(dir, mDate, "ALL.db")+"?mode=ro&_pragma=busy_timeout(5000)")
	if err != nil {
		return 0, 0, err
	}
	defer db.Close()
	tx, err := db.BeginTx(bg, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return 0, 0, err
	}
	defer tx.Rollback()
	var total, last int
	if err := tx.QueryRowContext(bg, "SELECT valid_count, last_raw_seq FROM day_stats WHERE symbol_id = ?", sym).Scan(&total, &last); err != nil {
		return 0, 0, err
	}
	c := (total + n - 1) / n
	take := min(m, c)
	r := total % n
	if r == 0 {
		r = n
	}
	l := r + (take-1)*n
	rs, err := tx.QueryContext(bg, "SELECT price, qty FROM ticks WHERE symbol_id = ? AND valid = 1 AND raw_seq <= ? ORDER BY raw_seq DESC LIMIT ?", sym, last, l)
	if err != nil {
		return 0, 0, err
	}
	defer rs.Close()
	size, cnt, candles, rows := r, 0, 0, 0
	var hi, lo, vol int64
	for rs.Next() {
		var p, q int64
		if err := rs.Scan(&p, &q); err != nil {
			return 0, 0, err
		}
		if cnt == 0 {
			hi, lo = p, p
		}
		hi, lo, vol = max(hi, p), min(lo, p), vol+q
		cnt++
		rows++
		if cnt == size {
			candles++
			cnt, size = 0, n
		}
	}
	return candles, rows, rs.Err()
}
