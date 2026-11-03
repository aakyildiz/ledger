package querycheck

import (
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	oc "github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck/match"
)

// Data-driven coverage probes (M/coverage.go): one per workload index, one
// per entity target for metadata-field indexes, and one for the retype
// window. Each is a must-hit Sometimes registered with RegisterCoverage and
// emitted as a Raw event on every validated index-backed query, true only for
// a model-verified page with rows whose filter needed that index.

const coveragePrefix = "oracle: coverage "

// CoverageIndexID names the per-index probe.
func CoverageIndexID(canonical string) oc.EventID {
	return oc.EventID(coveragePrefix + "index " + canonical + " served a model-verified page")
}

// CoverageMetadataID names the per-target metadata-index probe.
func CoverageMetadataID(target commonpb.QueryTarget) oc.EventID {
	name := "accounts"
	if target == commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS {
		name = "transactions"
	}

	return oc.EventID(coveragePrefix + "a metadata-field index served a model-verified page on " + name)
}

// CoverageRetypeID names the retype-window probe.
func CoverageRetypeID() oc.EventID {
	return oc.EventID(coveragePrefix + "a query was served while a retype window was open")
}

var coverageIndexes = match.WorkloadIndexes()

// CoverageIDs lists every probe, for RegisterCoverage.
func CoverageIDs() []oc.EventID {
	out := make([]oc.EventID, 0, len(coverageIndexes)+3)
	for _, wi := range coverageIndexes {
		out = append(out, CoverageIndexID(wi.Canonical))
	}

	return append(out,
		CoverageMetadataID(commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS),
		CoverageMetadataID(commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS),
		CoverageRetypeID(),
	)
}

// coverageEvents evaluates every probe for one validated query outcome
// (M/coverage.go noteQueryCoverage + coverageHits). The retype window is read
// on the drained model, like the singleton model read its committed state.
func coverageEvents(c *oc.ReadCtx, ledger string, target commonpb.QueryTarget, filter *commonpb.QueryFilter, needed map[string]struct{}, verified bool, rows int) []oc.Event {
	served := verified && rows > 0
	d := map[string]any{"ledger": ledger}
	evs := make([]oc.Event, 0, len(coverageIndexes)+2)
	for _, wi := range coverageIndexes {
		_, want := needed[wi.Canonical]
		evs = append(evs, oc.Raw(CoverageIndexID(wi.Canonical), served && want, d))
	}
	if target != commonpb.QueryTarget_QUERY_TARGET_LOGS {
		evs = append(evs, oc.Raw(CoverageMetadataID(target), served && filterNeedsMetadataIndex(filter), d))
	}
	retypeOpen := false
	if served {
		ls := c.T.Model().Ledger(ledger)
		for canon := range needed {
			if _, open := ls.RetypeWindow(canon); open {
				retypeOpen = true

				break
			}
		}
	}

	return append(evs, oc.Raw(CoverageRetypeID(), served && retypeOpen, d))
}

func filterNeedsMetadataIndex(f *commonpb.QueryFilter) bool {
	return match.AnyLeaf(f, func(leaf *commonpb.QueryFilter) bool {
		_, ok := leaf.GetFilter().(*commonpb.QueryFilter_Field)

		return ok
	})
}
