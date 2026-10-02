-- data/{YYYYMMDD}/{symbol}.db (PRD 4.1)
CREATE TABLE IF NOT EXISTS ticks (
  raw_seq INTEGER PRIMARY KEY,   -- rowid, assigned in arrival order
  ts      INTEGER NOT NULL,      -- exchange time field of the message, epoch ms
  price   INTEGER NOT NULL,      -- integer in the smallest price unit
  qty     INTEGER NOT NULL,
  valid   INTEGER NOT NULL       -- 0/1
);

CREATE INDEX IF NOT EXISTS ix_ticks_valid ON ticks(raw_seq) WHERE valid = 1;

CREATE TABLE IF NOT EXISTS day_stats (
  id           INTEGER PRIMARY KEY CHECK (id = 1),
  valid_count  INTEGER NOT NULL,
  last_raw_seq INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS candle_1m (
  minute INTEGER PRIMARY KEY,          -- minute offset from 00:00 of the day (0~1439)
  open INTEGER, high INTEGER, low INTEGER, close INTEGER,
  open_ts INTEGER, close_ts INTEGER,   -- ts of the ticks behind open/close
  volume INTEGER, tick_count INTEGER
);
