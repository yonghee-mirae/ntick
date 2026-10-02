// Package wire defines the MOCK fixed-length binary trade message.
// The real exchange format is unknown; see docs/mock-feed-spec.md.
package wire

import (
	"encoding/binary"
	"errors"
	"io"
	"strings"
)

const (
	Size      = 38 // total message length in bytes
	SymbolLen = 12
	Version   = 1
	TypeTrade = 1
)

var (
	ErrLength  = errors.New("wire: bad message length")
	ErrVersion = errors.New("wire: unsupported version")
	ErrType    = errors.New("wire: unknown message type")
	ErrSymbol  = errors.New("wire: symbol too long or not printable ASCII")
)

// Message is one trade. Type and version are implied by the layout constants.
type Message struct {
	Symbol string // up to SymbolLen printable ASCII chars
	Ts     int64  // epoch ms
	Price  int64  // smallest price unit
	Qty    int64
}

// ValidSymbol reports whether s matches ^[A-Za-z0-9_-]{1,12}$. Symbols become
// file names, so anything else (path separators, dots, NUL, spaces) is refused.
func ValidSymbol(s string) bool {
	if len(s) == 0 || len(s) > SymbolLen {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if !(c >= 'A' && c <= 'Z' || c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '_' || c == '-') {
			return false
		}
	}
	return true
}

// Encode returns the Size-byte big-endian encoding of m.
func Encode(m Message) ([]byte, error) {
	if !ValidSymbol(m.Symbol) {
		return nil, ErrSymbol
	}
	b := make([]byte, Size)
	b[0], b[1] = TypeTrade, Version
	copy(b[2:2+SymbolLen], m.Symbol+strings.Repeat(" ", SymbolLen-len(m.Symbol)))
	binary.BigEndian.PutUint64(b[14:], uint64(m.Ts))
	binary.BigEndian.PutUint64(b[22:], uint64(m.Price))
	binary.BigEndian.PutUint64(b[30:], uint64(m.Qty))
	return b, nil
}

// Decode parses exactly one message. It checks length, type, version and
// symbol; it does not judge ts, price or qty.
func Decode(b []byte) (Message, error) {
	if len(b) != Size {
		return Message{}, ErrLength
	}
	if b[0] != TypeTrade {
		return Message{}, ErrType
	}
	if b[1] != Version {
		return Message{}, ErrVersion
	}
	sym := strings.TrimRight(string(b[2:2+SymbolLen]), " ")
	if !ValidSymbol(sym) {
		return Message{}, ErrSymbol
	}
	return Message{
		Symbol: sym,
		Ts:     int64(binary.BigEndian.Uint64(b[14:])),
		Price:  int64(binary.BigEndian.Uint64(b[22:])),
		Qty:    int64(binary.BigEndian.Uint64(b[30:])),
	}, nil
}

// Reader yields messages from a byte stream, tolerating partial reads.
type Reader struct {
	r   io.Reader
	buf [Size]byte
}

func NewReader(r io.Reader) *Reader { return &Reader{r: r} }

// Next returns io.EOF at a clean message boundary and io.ErrUnexpectedEOF
// if the stream ends mid-message.
func (r *Reader) Next() (Message, error) {
	if _, err := io.ReadFull(r.r, r.buf[:]); err != nil {
		return Message{}, err
	}
	return Decode(r.buf[:])
}
