// Package tick defines the tick type and the receive-time validity filter.
package tick

// Tick is one trade. TS is the exchange time in epoch ms; Price is an integer
// in the smallest price unit.
type Tick struct {
	Symbol string
	TS     int64
	Price  int64
	Qty    int64
}

// Rule judges one tick. It may keep state (e.g. the previous valid price) and
// is called once per received tick, in arrival order, by the symbol's single
// writer, so it needs no locking. Valid reports whether the tick counts.
type Rule interface {
	Valid(t Tick) bool
}

// RuleFunc adapts a stateless function to Rule.
type RuleFunc func(Tick) bool

func (f RuleFunc) Valid(t Tick) bool { return f(t) }

// Filter holds per-symbol rules with a default for unlisted symbols.
// Stateful rules must not be shared between symbols, so the factory is called
// once per symbol.
type Filter struct {
	newRule func(symbol string) Rule
	perSym  map[string]Rule
}

// NewFilter returns a Filter that creates each symbol's rule with newRule on
// first use. Swapping rules per symbol is done inside newRule.
func NewFilter(newRule func(symbol string) Rule) *Filter {
	return &Filter{newRule: newRule, perSym: map[string]Rule{}}
}

// DefaultFilter applies DefaultRule to every symbol.
func DefaultFilter() *Filter {
	return NewFilter(func(string) Rule { return DefaultRule })
}

// Valid is not safe for concurrent use on the same symbol; the per-symbol
// writer is the only caller for that symbol, but the map is not guarded, so
// give each worker its own Filter.
func (f *Filter) Valid(t Tick) bool {
	r, ok := f.perSym[t.Symbol]
	if !ok {
		r = f.newRule(t.Symbol)
		f.perSym[t.Symbol] = r
	}
	return r.Valid(t)
}

// DefaultRule: price > 0 and qty > 0. No further thresholds by decision.
var DefaultRule Rule = RuleFunc(func(t Tick) bool { return t.Price > 0 && t.Qty > 0 })
