package store

import "database/sql"

// OpenRO opens a day file read-only for a short-lived read transaction (PRD 6.2).
// The caller closes it as soon as the transaction ends.
func OpenRO(path string) (*sql.DB, error) {
	return sql.Open(Driver, "file:"+path+"?mode=ro&_pragma=busy_timeout(5000)")
}
