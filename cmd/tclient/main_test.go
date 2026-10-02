package main

import (
	"os"
	"path/filepath"
	"testing"

	"ntick/internal/store"
)

func TestIntegrityAndVerify(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "20260105"), 0o755); err != nil {
		t.Fatal(err)
	}
	db, err := store.Create(store.Driver, filepath.Join(dir, "20260105", "A.db"))
	if err != nil {
		t.Fatal(err)
	}
	for _, q := range []string{
		`INSERT INTO ticks VALUES (1, 1767571200000, 100, 5, 1), (2, 1767571210000, 0, 5, 0)`,
		`INSERT INTO day_stats VALUES (1, 1, 2)`,
		`INSERT INTO candle_1m VALUES (540, 100, 100, 100, 100, 1767571200000, 1767571200000, 5, 1)`,
	} {
		if _, err := db.Exec(q); err != nil {
			t.Fatal(err)
		}
	}
	truth := filepath.Join(dir, "truth.csv")
	csv := "0,A,1767571200000,100,5,ok\n1,A,1767571210000,0,5,price_le0\n"
	if err := os.WriteFile(truth, []byte(csv), 0o644); err != nil {
		t.Fatal(err)
	}
	if ok, err := checkIntegrity(dir); !ok || err != nil {
		t.Fatalf("integrity: %v %v", ok, err)
	}
	if ok, err := verify(dir, truth, "Asia/Seoul", 5); !ok || err != nil {
		t.Fatalf("verify: %v %v", ok, err)
	}
	// Corrupt: wrong counter and a wrong candle.
	db.Exec(`UPDATE day_stats SET valid_count = 2`)
	db.Exec(`UPDATE candle_1m SET close = 99`)
	db.Close()
	if ok, _ := checkIntegrity(dir); ok {
		t.Error("integrity should fail")
	}
	if ok, _ := verify(dir, truth, "Asia/Seoul", 5); ok {
		t.Error("verify should fail")
	}
}
