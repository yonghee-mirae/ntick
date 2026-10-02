// Package store holds the per-day, per-symbol SQLite schema.
package store

import (
	"database/sql"
	_ "embed"
	"errors"
	"os"
	"strings"

	_ "modernc.org/sqlite"
)

//go:embed schema.sql
var Schema string

// Pragmas are the PRD 7 operating settings; they are per connection.
const Pragmas = `PRAGMA journal_mode = WAL; PRAGMA synchronous = NORMAL; PRAGMA busy_timeout = 5000;`

// Driver is the registered database/sql driver name (modernc.org/sqlite).
const Driver = "sqlite"

// DSN returns the SQLite URI for path with Pragmas as _pragma parameters, so
// they apply to every connection the pool opens, not only the first one.
func DSN(path string) string {
	var q []string
	for _, st := range strings.Split(Pragmas, ";") {
		name, val, ok := strings.Cut(strings.TrimPrefix(strings.TrimSpace(st), "PRAGMA "), "=")
		if ok {
			q = append(q, "_pragma="+strings.TrimSpace(name)+"("+strings.TrimSpace(val)+")")
		}
	}
	return "file:" + path + "?" + strings.Join(q, "&")
}

// BeforeRename is a test hook called after the temporary file is complete and
// before it is renamed into place; a non-nil error aborts the creation.
var BeforeRename func() error

// Create opens an existing-or-new DB file with a single connection (one writer
// per file). A new file is built under path+".tmp" with the full schema in
// rollback-journal mode and renamed into place, so a reader listing the
// directory never sees path without its schema. Only exact "*.db" names are
// data files; "*.db.tmp" and its journal never match those listings. The final
// path is then opened with Pragmas from the DSN (WAL for the writer).
func Create(driver, path string) (*sql.DB, error) {
	if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
		if err := build(driver, path); err != nil {
			return nil, err
		}
	}
	db, err := sql.Open(driver, DSN(path))
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	// IF NOT EXISTS: a no-op on complete files, and heals a schema-less file left by an older version.
	if _, err := db.Exec(Schema); err != nil {
		db.Close()
		return nil, err
	}
	return db, nil
}

func build(driver, path string) (err error) {
	tmp := path + ".tmp"
	for _, sfx := range []string{"", "-journal", "-wal", "-shm"} {
		os.Remove(tmp + sfx) // stale leftovers of a crashed create
	}
	defer func() {
		if err != nil {
			for _, sfx := range []string{"", "-journal"} {
				os.Remove(tmp + sfx)
			}
		}
	}()
	db, err := sql.Open(driver, "file:"+tmp+"?_pragma=journal_mode(DELETE)")
	if err != nil {
		return err
	}
	db.SetMaxOpenConns(1)
	_, err = db.Exec(Schema)
	if cerr := db.Close(); err == nil {
		err = cerr
	}
	if err == nil && BeforeRename != nil {
		err = BeforeRename()
	}
	if err != nil {
		return err
	}
	return os.Rename(tmp, path)
}
