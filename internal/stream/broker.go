package stream

import (
	"sync"

	"ntick/internal/ingest"
)

// subBuffer is the number of commit events a subscriber may lag behind.
const subBuffer = 256

// Broker fans commit events out to subscribers by symbol. Publish never
// blocks: a subscriber whose buffer is full is closed with a reason instead of
// slowing the writer down (the client reconnects and gets a fresh snapshot).
type Broker struct {
	mu     sync.Mutex
	subs   map[string]map[*Sub]struct{}
	closed bool
	wg     sync.WaitGroup // live subscribers
}

// Sub is one subscription. C is closed when the subscriber is dropped or the
// broker closes; Reason then says why. Events already buffered are still
// delivered before the close is seen.
type Sub struct {
	C      chan ingest.Event
	symbol string
	reason string // set before C is closed
}

// Reason is valid once C is closed.
func (s *Sub) Reason() string { return s.reason }

func NewBroker() *Broker { return &Broker{subs: map[string]map[*Sub]struct{}{}} }

// Subscribe returns nil after Close. Every subscription must be released with Unsubscribe.
func (b *Broker) Subscribe(symbol string) *Sub {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return nil
	}
	s := &Sub{C: make(chan ingest.Event, subBuffer), symbol: symbol}
	if b.subs[symbol] == nil {
		b.subs[symbol] = map[*Sub]struct{}{}
	}
	b.subs[symbol][s] = struct{}{}
	b.wg.Add(1)
	return s
}

// Unsubscribe releases s; it is safe after the broker already dropped s.
func (b *Broker) Unsubscribe(s *Sub) {
	b.mu.Lock()
	b.drop(s, "")
	b.mu.Unlock()
	b.wg.Done()
}

// drop removes s and closes its channel once. Caller holds b.mu.
func (b *Broker) drop(s *Sub, reason string) {
	m := b.subs[s.symbol]
	if _, ok := m[s]; !ok {
		return
	}
	delete(m, s)
	if len(m) == 0 {
		delete(b.subs, s.symbol)
	}
	s.reason = reason
	close(s.C)
}

// Publish is the ingest OnCommit callback. It never blocks.
func (b *Broker) Publish(ev ingest.Event) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for s := range b.subs[ev.Symbol] {
		select {
		case s.C <- ev:
		default:
			b.drop(s, "slow subscriber: event buffer full, reconnect for a new snapshot")
		}
	}
}

// Close drops all subscribers with a shutdown reason and waits until their
// handlers have released them. Further Subscribe calls return nil.
func (b *Broker) Close() {
	b.mu.Lock()
	b.closed = true
	for _, m := range b.subs {
		for s := range m {
			b.drop(s, "server shutting down")
		}
	}
	b.mu.Unlock()
	b.wg.Wait()
}
