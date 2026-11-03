package match

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/tests/oracle"
	"github.com/holiman/uint256"
)

// AggOptions are the result-stage options an aggregate request may set. A
// non-empty groupByPrefixes moves every total into per-prefix groups, in the
// order the request listed them.
type AggOptions struct {
	CollapseColors  bool
	UseMaxPrecision bool
	GroupByPrefixes []string
}

// prefixOf assigns an account to the first prefix that matches it, mirroring
// groupedAggregator.matchPrefix. An account matching none is left out of the
// result entirely.
func (o AggOptions) PrefixOf(addr string) (string, bool) {
	for _, prefix := range o.GroupByPrefixes {
		if strings.HasPrefix(addr, prefix) {
			return prefix, true
		}
	}

	return "", false
}

// AggPair is one bucket's summed volumes.
type AggPair struct{ In, Out uint256.Int }

func addAgg(sums map[AssetColor]*AggPair, key AssetColor, in, out *uint256.Int) {
	p := sums[key]
	if p == nil {
		p = &AggPair{}
		sums[key] = p
	}

	p.In.Add(&p.In, in)
	p.Out.Add(&p.Out, out)
}

// ModelAggregate folds the volume cells of every filter-matching account into
// per-(asset, color) sums the way the server's result stage does: precisions
// of one base merged under the highest seen with lower amounts rescaled, then
// colors summed into the uncolored bucket, each when asked. Stored zero cells
// contribute buckets too: zero postings create normal volume cells, while
// oracle.purgeZeroBalance removes purged cells from Volumes entirely.
func ModelAggregate(ls oracle.LedgerState, filter *commonpb.QueryFilter, opts AggOptions) map[AssetColor]*AggPair {
	return AggregateFold(ls, filter, opts, nil)
}

// ModelAggregateGroups is the per-prefix prediction: one entry per requested
// prefix, in request order, each folding the accounts that prefix claims. A
// prefix no account matches still gets its (empty) entry.
func ModelAggregateGroups(ls oracle.LedgerState, filter *commonpb.QueryFilter, opts AggOptions) []AggGroup {
	groups := make([]AggGroup, 0, len(opts.GroupByPrefixes))

	for _, prefix := range opts.GroupByPrefixes {
		sums := AggregateFold(ls, filter, opts, func(addr string) bool {
			claimed, ok := opts.PrefixOf(addr)

			return ok && claimed == prefix
		})
		groups = append(groups, AggGroup{Prefix: prefix, Sums: sums})
	}

	return groups
}

// AggGroup is one prefix's totals.
type AggGroup struct {
	Prefix string
	Sums   map[AssetColor]*AggPair
}

// AggregateFold sums the volume cells of the accounts filter selects and keep
// admits. A nil keep admits every matching account.
func AggregateFold(ls oracle.LedgerState, filter *commonpb.QueryFilter, opts AggOptions, keep func(addr string) bool) map[AssetColor]*AggPair {
	sums := map[AssetColor]*AggPair{}

	for k, vp := range ls.Volumes().All() {
		if filter != nil && !MatchAccountFilter(ls, filter, k.Address) {
			continue
		}

		if keep != nil && !keep(k.Address) {
			continue
		}

		addAgg(sums, AssetColor{Asset: k.Asset, Color: k.Color}, &vp.Input, &vp.Output)
	}

	if opts.UseMaxPrecision {
		sums = MergePrecisions(sums)
	}

	if opts.CollapseColors {
		collapsed := map[AssetColor]*AggPair{}
		for key, p := range sums {
			addAgg(collapsed, AssetColor{Asset: key.Asset}, &p.In, &p.Out)
		}

		sums = collapsed
	}

	return sums
}

// MergePrecisions re-keys every bucket of a base under the base's highest
// precision, scaling lower-precision amounts by the power of ten between.
func MergePrecisions(sums map[AssetColor]*AggPair) map[AssetColor]*AggPair {
	maxPrecision := map[string]uint8{}
	for key := range sums {
		base, precision := SplitAsset(key.Asset)
		maxPrecision[base] = max(maxPrecision[base], precision)
	}

	merged := map[AssetColor]*AggPair{}
	for key, p := range sums {
		base, precision := SplitAsset(key.Asset)
		target := maxPrecision[base]
		factor := uint256.NewInt(1)
		for range target - precision {
			factor.Mul(factor, uint256.NewInt(10))
		}

		var in, out uint256.Int
		in.Mul(&p.In, factor)
		out.Mul(&p.Out, factor)
		addAgg(merged, AssetColor{Asset: FormatAsset(base, target), Color: key.Color}, &in, &out)
	}

	return merged
}

// SplitAsset reads the server's asset convention: "BASE/N" carries precision
// N, a bare base has precision 0.
func SplitAsset(asset string) (base string, precision uint8) {
	if before, after, ok := strings.CutLast(asset, "/"); ok {
		n, err := strconv.ParseUint(after, 10, 8)
		if err == nil {
			return before, uint8(n)
		}
	}

	return asset, 0
}

func FormatAsset(base string, precision uint8) string {
	if precision == 0 {
		return base
	}

	return base + "/" + strconv.FormatUint(uint64(precision), 10)
}

// ServerAggregate decodes an AggregateResult into per-(asset, color) sums. Every
// bucket the server sent is kept, zero totals included: a bucket it invented is
// a divergence even when its amounts are zero. ok is false when a
// bucket repeats, which the result's one-entry-per-bucket contract forbids.
func ServerAggregate(res *commonpb.AggregateResult) (out map[AssetColor]*AggPair, ok bool) {
	out = map[AssetColor]*AggPair{}

	for _, av := range res.GetVolumes() {
		key := AssetColor{Asset: av.GetAsset(), Color: av.GetColor()}
		if _, dup := out[key]; dup {
			return nil, false
		}

		p := &AggPair{}
		av.GetInput().IntoUint256(&p.In)
		av.GetOutput().IntoUint256(&p.Out)
		out[key] = p
	}

	return out, true
}

// ServerAggregateGroups decodes the grouped arm of a result, preserving the
// order the server listed the prefixes in. ok is false when a group repeats a
// bucket.
func ServerAggregateGroups(res *commonpb.AggregateResult) (out []AggGroup, ok bool) {
	for _, g := range res.GetGroups() {
		sums, decoded := ServerAggregate(&commonpb.AggregateResult{Volumes: g.GetVolumes()})
		if !decoded {
			return nil, false
		}

		out = append(out, AggGroup{Prefix: g.GetPrefix(), Sums: sums})
	}

	return out, true
}

// AggGroupsEqual compares two grouped folds prefix for prefix, in order: the
// server emits one group per requested prefix, in request order.
func AggGroupsEqual(a, b []AggGroup) bool {
	if len(a) != len(b) {
		return false
	}

	for i := range a {
		if a[i].Prefix != b[i].Prefix || !AggEqual(a[i].Sums, b[i].Sums) {
			return false
		}
	}

	return true
}

func RenderAggGroups(groups []AggGroup) string {
	parts := make([]string, 0, len(groups))
	for _, g := range groups {
		parts = append(parts, g.Prefix+"{"+RenderAgg(g.Sums)+"}")
	}

	return strings.Join(parts, " ")
}

func AggEqual(a, b map[AssetColor]*AggPair) bool {
	if len(a) != len(b) {
		return false
	}

	for key, pa := range a {
		pb, ok := b[key]
		if !ok || pa.In.Cmp(&pb.In) != 0 || pa.Out.Cmp(&pb.Out) != 0 {
			return false
		}
	}

	return true
}

func RenderAgg(m map[AssetColor]*AggPair) string {
	keys := make([]AssetColor, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}

	sort.Slice(keys, func(i, j int) bool { return keys[i].String() < keys[j].String() })

	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, fmt.Sprintf("%s=in:%s,out:%s", k.String(), m[k].In.Dec(), m[k].Out.Dec()))
	}

	return strings.Join(parts, " ")
}

// AggregateResultMatches is the per-base verdict on a successful
// AggregateVolumes result under opts: grouped requests (GroupByPrefixes set)
// must answer only groups and flat ones only volumes, no bucket repeats, and
// the fold equals ls's (ModelAggregate / ModelAggregateGroups). why is "" when
// ok.
//
// Tracker-bound part (error classification through IndexedQueryOutcomeLegal,
// candidate walk, coverage): task 05.
func AggregateResultMatches(ls oracle.LedgerState, filter *commonpb.QueryFilter, opts AggOptions, res *commonpb.AggregateResult) (ok bool, why string) {
	grouped := len(opts.GroupByPrefixes) > 0
	if grouped && len(res.GetVolumes()) != 0 || !grouped && len(res.GetGroups()) != 0 {
		return false, "aggregate result mixes grouped and flat volumes"
	}

	if grouped {
		got, decoded := ServerAggregateGroups(res)
		if !decoded {
			return false, "aggregate result repeats a bucket"
		}

		want := ModelAggregateGroups(ls, filter, opts)
		if !AggGroupsEqual(want, got) {
			return false, "groups " + RenderAggGroups(got) + ", model " + RenderAggGroups(want)
		}

		return true, ""
	}

	got, decoded := ServerAggregate(res)
	if !decoded {
		return false, "aggregate result repeats a bucket"
	}

	want := ModelAggregate(ls, filter, opts)
	if !AggEqual(want, got) {
		return false, "volumes " + RenderAgg(got) + ", model " + RenderAgg(want)
	}

	return true, ""
}
