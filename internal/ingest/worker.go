package ingest

import (
	"container/list"
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"syscall"
	"time"

	"ntick/internal/store"
	"ntick/internal/tick"
)

// Per-worker LRU pool size bounds. Each open file costs 3 file descriptors
// (db, -wal, -shm), so total FDs <= workers*pool*3. Files are only ever opened
// by their one worker, so pools are not shared. Too small a pool thrashes
// (close checkpoints the WAL), too large exhausts RLIMIT_NOFILE (EMFILE).
const (
	maxOpenFiles = 64
	minOpenFiles = 4
)

// poolSize budgets at most half of the soft RLIMIT_NOFILE across workers.
func poolSize(workers int) int {
	var rl syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rl); err != nil {
		log.Printf("getrlimit failed (%v); using pool size %d", err, maxOpenFiles)
		return maxOpenFiles
	}
	return sizePool(workers, rl.Cur)
}

func sizePool(workers int, soft uint64) int {
	pool := int(soft / 2 / uint64(3*workers))
	pool = min(max(pool, minOpenFiles), maxOpenFiles)
	log.Printf("file pool: %d open files per worker x %d workers (RLIMIT_NOFILE soft=%d)", pool, workers, soft)
	if need := uint64(workers * minOpenFiles * 3); need > soft/2 {
		log.Printf("WARNING: RLIMIT_NOFILE %d is below what %d workers need (%d fds at minimum pool); raise ulimit -n", soft, workers, need*2)
	}
	return pool
}

// rec is a received tick with its validity and its day-file coordinates.
type rec struct {
	tick.Tick
	valid  bool
	date   string // local YYYYMMDD
	minute int64
}

// fileKey identifies one day file.
type fileKey struct{ date, symbol string }

type worker struct {
	ch       chan tick.Tick
	dir      string
	loc      *time.Location
	filter   *tick.Filter
	pool     map[string]*list.Element // path -> element holding *poolEntry
	lru      *list.List               // front = most recently used
	maxFiles int                      // pool capacity
	fatal    func(error)              // called when a batch cannot be committed; must not return normally

	onCommit func(Event) // optional; called on this worker's goroutine after each committed file transaction
}

type poolEntry struct {
	path string
	db   *sql.DB
}

func newWorker(dir string, loc *time.Location) *worker {
	return &worker{
		ch: make(chan tick.Tick, queueSize), dir: dir, loc: loc,
		filter: tick.DefaultFilter(), pool: map[string]*list.Element{}, lru: list.New(), maxFiles: maxOpenFiles,
		fatal: func(err error) { log.Fatal(err) }, // exit 1 instead of silently dropping the batch
	}
}

func (w *worker) run() {
	var batch []tick.Tick
	timer := time.NewTicker(flushInterval)
	defer timer.Stop()
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
			if len(batch) >= maxBatch {
				flush()
			}
		case <-timer.C:
			flush()
		}
	}
}

// commit judges validity in arrival order, splits by day file and commits one
// transaction per file.
func (w *worker) commit(batch []tick.Tick) {
	for _, g := range split(batch, w.filter, w.loc) {
		err := w.write(g.key, g.recs)
		if err != nil {
			log.Printf("commit %s/%s failed, retrying once: %v", g.key.date, g.key.symbol, err)
			err = w.write(g.key, g.recs)
		}
		if err != nil {
			w.fatal(fmt.Errorf("commit %s/%s failed twice: %w", g.key.date, g.key.symbol, err))
		}
	}
}

type group struct {
	key  fileKey
	recs []rec
}

// split judges validity in arrival order and groups ticks per day file,
// keeping arrival order inside each group.
func split(batch []tick.Tick, f *tick.Filter, loc *time.Location) []group {
	var groups []group
	idx := map[fileKey]int{}
	for _, t := range batch {
		lt := time.UnixMilli(t.TS).In(loc)
		y, m, d := lt.Date()
		midnight := time.Date(y, m, d, 0, 0, 0, 0, loc).UnixMilli()
		r := rec{Tick: t, valid: f.Valid(t), date: lt.Format("20060102"), minute: (t.TS - midnight) / 60000}
		k := fileKey{r.date, t.Symbol}
		i, ok := idx[k]
		if !ok {
			i = len(groups)
			idx[k] = i
			groups = append(groups, group{key: k})
		}
		groups[i].recs = append(groups[i].recs, r)
	}
	return groups
}

const upsertStats = `
INSERT INTO day_stats (id, valid_count, last_raw_seq) VALUES (1, ?, last_insert_rowid())
ON CONFLICT (id) DO UPDATE SET
  valid_count  = valid_count + excluded.valid_count,
  last_raw_seq = excluded.last_raw_seq`

// write commits recs to one day file in a single transaction (PRD 6.1).
func (w *worker) write(k fileKey, recs []rec) (err error) {
	db, err := w.open(k)
	if err != nil {
		return err
	}
	// BEGIN IMMEDIATE so the write lock is taken up front.
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
	ins, err := conn.PrepareContext(bg, "INSERT INTO ticks (ts, price, qty, valid) VALUES (?, ?, ?, ?)")
	if err != nil {
		return err
	}
	defer ins.Close()
	var nValid int64
	var evTicks []CommitTick
	candles := map[int64]*store.Candle{}
	for _, r := range recs {
		v := 0
		if r.valid {
			v = 1
			nValid++
			c := candles[r.minute]
			if c == nil {
				c = &store.Candle{}
				candles[r.minute] = c
			}
			c.Add(r.TS, r.Price, r.Qty)
		}
		res, err := ins.ExecContext(bg, r.TS, r.Price, r.Qty, v)
		if err != nil {
			return err
		}
		if r.valid && w.onCommit != nil {
			seq, _ := res.LastInsertId() // rowid of this insert = raw_seq
			evTicks = append(evTicks, CommitTick{Seq: seq, TS: r.TS, Price: r.Price, Qty: r.Qty})
		}
	}
	// last_insert_rowid() must be read directly after the ticks insert.
	if _, err = conn.ExecContext(bg, upsertStats, nValid); err != nil {
		return err
	}
	for m, c := range candles {
		if err = store.UpsertCandle(bg, conn, m, c); err != nil {
			return err
		}
	}
	var total, last int64
	if w.onCommit != nil && len(evTicks) > 0 {
		if err = conn.QueryRowContext(bg, "SELECT valid_count, last_raw_seq FROM day_stats WHERE id = 1").Scan(&total, &last); err != nil {
			return err
		}
	}
	if _, err = conn.ExecContext(bg, "COMMIT"); err != nil {
		return err
	}
	if w.onCommit != nil && len(evTicks) > 0 {
		w.onCommit(Event{Symbol: k.symbol, Date: k.date, Ticks: evTicks, T: total, LastSeq: last})
	}
	return nil
}

// open returns the pooled connection for k, creating the file on demand and
// evicting the least recently used one when the pool is full.
func (w *worker) open(k fileKey) (*sql.DB, error) {
	path := filepath.Join(w.dir, k.date, k.symbol+".db")
	if e, ok := w.pool[path]; ok {
		w.lru.MoveToFront(e)
		return e.Value.(*poolEntry).db, nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	db, err := store.Create(store.Driver, path)
	if err != nil {
		return nil, err
	}
	w.pool[path] = w.lru.PushFront(&poolEntry{path, db})
	if w.lru.Len() > w.maxFiles {
		old := w.lru.Back()
		e := w.lru.Remove(old).(*poolEntry)
		delete(w.pool, e.path)
		e.db.Close()
	}
	return db, nil
}

func (w *worker) closeAll() {
	for e := w.lru.Front(); e != nil; e = e.Next() {
		e.Value.(*poolEntry).db.Close()
	}
}

var bg = context.Background()
