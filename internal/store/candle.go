package store

import (
	"context"
	"database/sql"
)

// Candle accumulates the valid ticks of one minute. It is the single
// implementation of the candle_1m rules, shared by ingest and rejudge.
type Candle struct {
	Open, High, Low, Close int64
	OpenTS, CloseTS        int64
	Volume, Count          int64
}

// Add applies a valid tick. Ticks must be added in raw_seq order: on equal ts
// the first tick keeps open (strict <) and the later one takes close (>=).
func (c *Candle) Add(ts, price, qty int64) {
	if c.Count == 0 {
		*c = Candle{price, price, price, price, ts, ts, qty, 1}
		return
	}
	if ts < c.OpenTS {
		c.Open, c.OpenTS = price, ts
	}
	if ts >= c.CloseTS {
		c.Close, c.CloseTS = price, ts
	}
	c.High, c.Low = max(c.High, price), min(c.Low, price)
	c.Volume += qty
	c.Count++
}

// Across batches a stored candle has lower raw_seq than the new one, so on
// equal ts the stored open stays (strict <) and the new close wins (>=).
const upsertCandle = `
INSERT INTO candle_1m (minute, open, high, low, close, open_ts, close_ts, volume, tick_count)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT (minute) DO UPDATE SET
  open     = CASE WHEN excluded.open_ts < open_ts THEN excluded.open ELSE open END,
  open_ts  = min(open_ts, excluded.open_ts),
  close    = CASE WHEN excluded.close_ts >= close_ts THEN excluded.close ELSE close END,
  close_ts = max(close_ts, excluded.close_ts),
  high     = max(high, excluded.high),
  low      = min(low, excluded.low),
  volume   = volume + excluded.volume,
  tick_count = tick_count + excluded.tick_count`

// Execer is satisfied by *sql.Conn, *sql.Tx and *sql.DB.
type Execer interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

// UpsertCandle merges c, whose ticks are later in raw_seq than any stored
// ones, into the candle of the given minute. On an empty table it inserts, so
// a rebuild is DELETE FROM candle_1m followed by one UpsertCandle per minute.
func UpsertCandle(ctx context.Context, e Execer, minute int64, c *Candle) error {
	_, err := e.ExecContext(ctx, upsertCandle, minute, c.Open, c.High, c.Low, c.Close, c.OpenTS, c.CloseTS, c.Volume, c.Count)
	return err
}
