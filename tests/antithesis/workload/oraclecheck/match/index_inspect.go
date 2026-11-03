package match

import (
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/tests/oracle"
)

// InspectPageViolation reports the first way a served inspection breaks its own
// contract: a page longer than asked for, a value served twice, a resume cursor
// that disagrees with has_more, a facet counting nothing, or a summary claiming
// more distinct values than entities carrying the key.
func InspectPageViolation(res *servicepb.InspectIndexResponse, mode servicepb.InspectIndexMode, pageSize int) string {
	switch mode {
	case servicepb.InspectIndexMode_INSPECT_INDEX_MODE_DISTINCT_VALUES:
		page := res.GetDistinctValues()
		if page == nil {
			return "distinct-values mode answered another arm"
		}

		if len(page.GetValues()) > pageSize {
			return "page longer than requested"
		}

		if seen := map[string]bool{}; !DistinctValues(page.GetValues(), seen) {
			return "value served twice"
		}

		return CursorPairing(page.GetHasMore(), page.GetNextCursor())
	case servicepb.InspectIndexMode_INSPECT_INDEX_MODE_FACETS:
		page := res.GetFacets()
		if page == nil {
			return "facets mode answered another arm"
		}

		if len(page.GetFacets()) > pageSize {
			return "page longer than requested"
		}

		seen := map[string]bool{}
		for _, f := range page.GetFacets() {
			if f.GetCount() == 0 {
				return "facet counts no entity"
			}

			if seen[f.GetValue().String()] {
				return "value served twice"
			}

			seen[f.GetValue().String()] = true
		}

		return CursorPairing(page.GetHasMore(), page.GetNextCursor())
	default:
		summary := res.GetSummary()
		if summary == nil {
			return "summary mode answered another arm"
		}

		return ""
	}
}

// CursorPairing reports the inspection's resume contract: a token is published
// exactly when the scan has more to serve.
func CursorPairing(hasMore bool, next string) string {
	if hasMore == (next != "") {
		return ""
	}

	if hasMore {
		return "more to serve but no resume cursor"
	}

	return "resume cursor on an exhausted scan"
}

func DistinctValues(values []*commonpb.MetadataValue, seen map[string]bool) bool {
	for _, v := range values {
		if seen[v.String()] {
			return false
		}

		seen[v.String()] = true
	}

	return true
}

// InspectCounts is what a summary inspection reports about one (target, key).
type InspectCounts struct {
	Cardinality      uint64
	EntitiesWithKey  uint64
	EntitiesWithNull uint64
}

// ModelInspectCounts folds a base's metadata for one (target, key) the way the
// index writer keys it: every entity carrying the key contributes exactly one
// encoded value, coerced to the declared type first, and the distinct encodings
// are the value index's groups.
//
// A value that does not survive the coercion becomes a null encoding, and the
// encoder carries the text it failed to convert into the key
// (readstore.EncodeNull), so two entities holding different unconvertible text
// are two groups, not one. Those entities are counted by entities_with_null;
// entities_with_key counts only the rest. That split is why cardinality does
// not bound entities_with_key on its own — a key every entity holds
// unconvertibly reports entities_with_key 0 with a non-zero cardinality.
func ModelInspectCounts(ls oracle.LedgerState, target commonpb.QueryTarget, key string, declared commonpb.MetadataType) InspectCounts {
	var counts InspectCounts

	groups := map[string]struct{}{}

	fold := func(stored *commonpb.MetadataValue) {
		coerced := stored
		if !commonpb.TypeMatches(stored, declared) {
			coerced = commonpb.ConvertMetadataValue(stored, declared)
		}

		groups[oracle.MetaValueString(coerced)] = struct{}{}

		if _, isNull := coerced.GetType().(*commonpb.MetadataValue_NullValue); isNull {
			counts.EntitiesWithNull++

			return
		}

		counts.EntitiesWithKey++
	}

	if target == commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS {
		txs := ls.Txs()
		for i := range txs.Len() {
			if v, ok := txs.Get(i).Metadata()[key]; ok {
				fold(v)
			}
		}
	} else {
		for k, v := range ls.Metadata().All() {
			if k.Key == key {
				fold(v)
			}
		}
	}

	counts.Cardinality = uint64(len(groups))

	return counts
}
