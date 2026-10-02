package tick

import "testing"

func TestDefaultRule(t *testing.T) {
	f := DefaultFilter()
	for _, c := range []struct {
		p, q int64
		want bool
	}{{100, 1, true}, {0, 1, false}, {-1, 1, false}, {100, 0, false}, {100, -5, false}} {
		if got := f.Valid(Tick{Symbol: "A", Price: c.p, Qty: c.q}); got != c.want {
			t.Errorf("price=%d qty=%d: got %v", c.p, c.q, got)
		}
	}
}

// prevRule is a stateful rule: it rejects a price that moves >2x from the last valid one.
type prevRule struct{ last int64 }

func (r *prevRule) Valid(t Tick) bool {
	if !DefaultRule.Valid(t) || (r.last != 0 && (t.Price > 2*r.last || t.Price*2 < r.last)) {
		return false
	}
	r.last = t.Price
	return true
}

func TestPerSymbolStatefulRules(t *testing.T) {
	f := NewFilter(func(sym string) Rule {
		if sym == "S" {
			return &prevRule{}
		}
		return DefaultRule
	})
	seq := []struct {
		sym string
		p   int64
		ok  bool
	}{{"S", 100, true}, {"S", 500, false}, {"S", 110, true}, {"O", 500, true}, {"O", 1, true}}
	for i, c := range seq {
		if got := f.Valid(Tick{Symbol: c.sym, Price: c.p, Qty: 1}); got != c.ok {
			t.Errorf("#%d %s %d: got %v", i, c.sym, c.p, got)
		}
	}
}
