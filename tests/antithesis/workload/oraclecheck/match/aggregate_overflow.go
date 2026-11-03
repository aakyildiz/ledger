package match

import (
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/tests/oracle"
	"github.com/holiman/uint256"
)

// AggregateOverflows reports whether the server's aggregation of ls under
// filter and opts must answer AGGREGATE_OVERFLOW (internal/query/aggregate.go):
// a per-bucket sum, a color collapse, a precision merge or a rescale to the
// base's highest precision exceeds 256 bits. With GroupByPrefixes, each group
// is folded on its own and any group overflowing is enough.
//
// ModelAggregate wraps silently; validators consult this before treating an
// AGGREGATE_OVERFLOW answer as unexplained (task 99: a checkpoint aggregate
// with useMaxPrecision over task 08's USD/<1..255> probe cells overflowed on
// the stack).
func AggregateOverflows(ls oracle.LedgerState, filter *commonpb.QueryFilter, opts AggOptions) bool {
	if len(opts.GroupByPrefixes) == 0 {
		return aggOverflow(ls, filter, opts, nil)
	}
	for _, prefix := range opts.GroupByPrefixes {
		if aggOverflow(ls, filter, opts, func(addr string) bool {
			claimed, ok := opts.PrefixOf(addr)

			return ok && claimed == prefix
		}) {
			return true
		}
	}

	return false
}

type aggOverflowKey struct {
	base      string
	precision uint8
	color     string
}

func aggOverflow(ls oracle.LedgerState, filter *commonpb.QueryFilter, opts AggOptions, keep func(string) bool) bool {
	byAsset := map[aggOverflowKey]*AggPair{}
	for k, vp := range ls.Volumes().All() {
		if filter != nil && !MatchAccountFilter(ls, filter, k.Address) {
			continue
		}
		if keep != nil && !keep(k.Address) {
			continue
		}
		base, precision := SplitAsset(k.Asset)
		key := aggOverflowKey{base: base, precision: precision, color: k.Color}
		p := byAsset[key]
		if p == nil {
			p = &AggPair{}
			byAsset[key] = p
		}
		if _, o := p.In.AddOverflow(&p.In, &vp.Input); o {
			return true
		}
		if _, o := p.Out.AddOverflow(&p.Out, &vp.Output); o {
			return true
		}
	}

	buckets := byAsset
	if opts.UseMaxPrecision {
		maxPrec := map[string]uint8{}
		for key := range byAsset {
			maxPrec[key.base] = max(maxPrec[key.base], key.precision)
		}
		merged := map[aggOverflowKey]*AggPair{}
		for key, p := range byAsset {
			target := maxPrec[key.base]
			mk := aggOverflowKey{base: key.base, precision: target, color: key.color}
			m := merged[mk]
			if m == nil {
				m = &AggPair{}
				merged[mk] = m
			}
			in, out := p.In, p.Out
			if key.precision != target {
				if in.IsZero() && out.IsZero() {
					continue
				}
				factor, o := aggPow10(target - key.precision)
				if o {
					return true
				}
				if _, o := in.MulOverflow(&in, factor); o {
					return true
				}
				if _, o := out.MulOverflow(&out, factor); o {
					return true
				}
			}
			if _, o := m.In.AddOverflow(&m.In, &in); o {
				return true
			}
			if _, o := m.Out.AddOverflow(&m.Out, &out); o {
				return true
			}
		}
		buckets = merged
	}

	if opts.CollapseColors {
		collapsed := map[aggOverflowKey]*AggPair{}
		for key, p := range buckets {
			ck := aggOverflowKey{base: key.base, precision: key.precision}
			c := collapsed[ck]
			if c == nil {
				c = &AggPair{}
				collapsed[ck] = c
			}
			if _, o := c.In.AddOverflow(&c.In, &p.In); o {
				return true
			}
			if _, o := c.Out.AddOverflow(&c.Out, &p.Out); o {
				return true
			}
		}
	}

	return false
}

// aggPow10 is the server's pow10: 10^exp, or overflow past 256 bits.
func aggPow10(exp uint8) (*uint256.Int, bool) {
	result := uint256.NewInt(1)
	ten := uint256.NewInt(10)
	for range exp {
		if _, o := result.MulOverflow(result, ten); o {
			return nil, true
		}
	}

	return result, false
}
