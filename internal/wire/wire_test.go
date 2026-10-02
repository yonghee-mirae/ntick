package wire

import (
	"bytes"
	"errors"
	"io"
	"testing"
	"testing/iotest"
)

func TestRoundTrip(t *testing.T) {
	in := Message{Symbol: "005930", Ts: 1767567600123, Price: 70500, Qty: 12}
	b, err := Encode(in)
	if err != nil || len(b) != Size {
		t.Fatalf("encode: %v len=%d", err, len(b))
	}
	out, err := Decode(b)
	if err != nil || out != in {
		t.Fatalf("got %+v err=%v", out, err)
	}
	neg := Message{Symbol: "X", Ts: -1, Price: -5, Qty: 0}
	b, _ = Encode(neg)
	if out, _ := Decode(b); out != neg {
		t.Fatalf("negatives: %+v", out)
	}
}

func TestBadInput(t *testing.T) {
	if _, err := Decode(make([]byte, Size-1)); !errors.Is(err, ErrLength) {
		t.Fatalf("short: %v", err)
	}
	if _, err := Decode(make([]byte, Size+1)); !errors.Is(err, ErrLength) {
		t.Fatalf("long: %v", err)
	}
	b, _ := Encode(Message{Symbol: "A"})
	b[1] = 9
	if _, err := Decode(b); !errors.Is(err, ErrVersion) {
		t.Fatalf("version: %v", err)
	}
	b[1], b[0] = Version, 9
	if _, err := Decode(b); !errors.Is(err, ErrType) {
		t.Fatalf("type: %v", err)
	}
	if _, err := Encode(Message{Symbol: "TOOLONGSYMBOL1"}); !errors.Is(err, ErrSymbol) {
		t.Fatalf("symbol: %v", err)
	}
}

func TestReaderPartial(t *testing.T) {
	var buf bytes.Buffer
	want := []Message{{"A", 1, 10, 1}, {"BB", 2, 20, 2}, {"CCC", 3, 30, 3}}
	for _, m := range want {
		b, _ := Encode(m)
		buf.Write(b)
	}
	full := buf.Bytes()
	r := NewReader(iotest.OneByteReader(bytes.NewReader(full)))
	for i, w := range want {
		if m, err := r.Next(); err != nil || m != w {
			t.Fatalf("%d: %+v %v", i, m, err)
		}
	}
	if _, err := r.Next(); err != io.EOF {
		t.Fatalf("want EOF, got %v", err)
	}
	r = NewReader(bytes.NewReader(full[:Size+5]))
	if _, err := r.Next(); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Next(); err != io.ErrUnexpectedEOF {
		t.Fatalf("want ErrUnexpectedEOF, got %v", err)
	}
}

func TestValidSymbolAndDecode(t *testing.T) {
	good := []string{"A", "005930", "S100000", "a_b-C", "ABCDEFGHIJKL"}
	bad := []string{"", "../../x", "a/b", `a\b`, "a.b", "a b", "a\x00b", "ABCDEFGHIJKLM", "한글"}
	for _, s := range good {
		if !ValidSymbol(s) {
			t.Errorf("%q rejected", s)
		}
	}
	for _, s := range bad {
		if ValidSymbol(s) {
			t.Errorf("%q accepted", s)
		}
	}
	b, _ := Encode(Message{Symbol: "OK", Ts: 1})
	for _, raw := range []string{"../../x     ", "            ", "AB\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00", "A B         "} {
		c := append([]byte(nil), b...)
		copy(c[2:14], raw)
		if _, err := Decode(c); !errors.Is(err, ErrSymbol) {
			t.Errorf("Decode(%q) err = %v, want ErrSymbol", raw, err)
		}
	}
}
