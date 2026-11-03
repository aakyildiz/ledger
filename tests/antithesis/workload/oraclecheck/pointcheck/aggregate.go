package pointcheck

import (
	"strings"

	"google.golang.org/grpc/codes"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	oc "github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck/match"
	"github.com/formancehq/ledger/v3/tests/oracle"
)

const accounts = commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS

// ValidateAggregateVolumes checks one linearizable AggregateVolumes
// (M/aggregates.go runAggregateQuery / validateAggregate):
//
//   - NotFound on an absent ledger is coverage; otherwise it must be explained
//     by a base where the ledger does not exist or is deleted;
//   - a served result on an absent ledger is a finding, and so is one on a
//     ledger deleted in every candidate (the liveness guard model-reads §4.8
//     flags as missing in the singleton model); each candidate that explains a
//     served result must also hold the ledger live;
//   - grouped and flat arms are exclusive and buckets never repeat;
//   - otherwise IndexedQueryOutcomeLegal over the candidates with the fold
//     (bucket set and sums) equal to the server's, or, when an index the filter
//     reads is not ready on that base, the refusal.
func ValidateAggregateVolumes(c *oc.ReadCtx) []oc.Event {
	var req servicepb.AggregateVolumesRequest
	if err := decode(c.Request, &req); err != nil {
		return []oc.Event{oc.Violation(EvAggOutsideModel, map[string]any{"error": "undecodable request: " + err.Error()})}
	}
	ledger, filter := req.GetLedger(), req.GetFilter()
	opts := match.AggOptions{
		CollapseColors:  req.GetCollapseColors(),
		UseMaxPrecision: req.GetUseMaxPrecision(),
		GroupByPrefixes: req.GetGroupByPrefixes(),
	}
	absent := absentEverywhere(c, ledger)
	base := map[string]any{"ledger": ledger, "filter": match.DescribeFilter(filter)}

	if c.Outcome.IsNotFound() {
		if absent {
			return []oc.Event{oc.Reached(EvAggAbsentNotFound, map[string]any{"ledger": ledger})}
		}

		return c.LedgerNotFoundExplained(ledger)
	}

	needed := map[string]struct{}{}
	match.NeededIndexCanonicals(filter, accounts, needed)
	rerr := rpcError(c.Outcome)

	// A filter the compiler must refuse whatever the state (a has-asset
	// precision past the one-byte index cell, a leaf invalid on accounts):
	// FILTER_COMPILATION_ERROR is its answer (M/aggregates.go ran
	// classifyRejectedFilter first). Task 06's generators, which this read
	// draws since task 99, produce these shapes.
	if rerr.Code == codes.InvalidArgument && rerr.HasReason("FILTER_COMPILATION_ERROR") &&
		(match.HasAssetPrecisionOverflow(filter) || match.ClassifyRejectedFilter(filter, accounts).Rejected) {
		return nil
	}

	if !c.Outcome.OK && len(needed) == 0 {
		d := outcomeDetails(c.Outcome)
		d["ledger"], d["absent"], d["filter"] = ledger, absent, match.DescribeFilter(filter)

		return []oc.Event{oc.Violation(EvAggregateVolumesUnexpected, d)}
	}

	errKind, classified := match.ClassifyIndexedQueryError(rerr)
	if !classified {
		d := outcomeDetails(c.Outcome)
		d["ledger"], d["filter"] = ledger, match.DescribeFilter(filter)

		return []oc.Event{oc.Violation(EvAggUnexpectedError, d)}
	}

	grouped := len(opts.GroupByPrefixes) > 0
	var (
		res          *commonpb.AggregateResult
		serverFlat   map[match.AssetColor]*match.AggPair
		serverGroups []match.AggGroup
	)
	if errKind == match.IndexedErrNone {
		if absent {
			return []oc.Event{oc.Violation(EvAggServedOutsideFleet, base)}
		}
		if !liveSomewhere(c, ledger) {
			return []oc.Event{oc.Violation(EvAggServedDeletedEverywhere, base)}
		}
		res = &commonpb.AggregateResult{}
		if err := decode(c.Outcome.Response, res); err != nil {
			base["error"] = "undecodable response: " + err.Error()

			return []oc.Event{oc.Violation(EvAggOutsideModel, base)}
		}
		if grouped && len(res.GetVolumes()) != 0 || !grouped && len(res.GetGroups()) != 0 {
			return []oc.Event{oc.Violation(EvAggMixed, map[string]any{
				"ledger": ledger, "prefixes": strings.Join(opts.GroupByPrefixes, ","),
				"volumes": len(res.GetVolumes()), "groups": len(res.GetGroups()),
			})}
		}
		var decoded bool
		if grouped {
			serverGroups, decoded = match.ServerAggregateGroups(res)
		} else {
			serverFlat, decoded = match.ServerAggregate(res)
		}
		if !decoded {
			return []oc.Event{oc.Violation(EvAggRepeatsBucket, base)}
		}
	}

	rejectedIndex := match.RejectedIndexLabel(rerr)
	if c.Matches(func(b oracle.GlobalState) bool {
		if errKind == match.IndexedErrNone && !match.LedgerIsLive(b, ledger) {
			return false
		}

		return match.IndexedQueryOutcomeLegal(b.Ledger(ledger), accounts, filter, needed, errKind, rejectedIndex, func(ls oracle.LedgerState) bool {
			if grouped {
				return match.AggGroupsEqual(match.ModelAggregateGroups(ls, filter, opts), serverGroups)
			}

			return match.AggEqual(match.ModelAggregate(ls, filter, opts), serverFlat)
		})
	}) {
		switch {
		case errKind != match.IndexedErrNone:
			return []oc.Event{oc.Reached(EvAggGatedOnMissingIndex, map[string]any{"ledger": ledger})}
		case grouped:
			return []oc.Event{oc.Reached(EvAggGroupedValidated, map[string]any{"ledger": ledger})}
		default:
			return []oc.Event{oc.Reached(EvAggValidated, map[string]any{"ledger": ledger})}
		}
	}

	committed := c.T.Model().Ledger(ledger)
	base["collapseColors"], base["useMaxPrecision"], base["prefixes"] = opts.CollapseColors, opts.UseMaxPrecision, strings.Join(opts.GroupByPrefixes, ",")
	base["maxTicket"] = c.MaxTicket
	if !c.Outcome.OK {
		base["code"], base["reason"] = c.Outcome.Code, c.Outcome.Reason
	}
	if grouped {
		base["server"], base["model"] = match.RenderAggGroups(serverGroups), match.RenderAggGroups(match.ModelAggregateGroups(committed, filter, opts))
	} else {
		base["server"], base["model"] = match.RenderAgg(serverFlat), match.RenderAgg(match.ModelAggregate(committed, filter, opts))
	}

	return []oc.Event{oc.Violation(EvAggOutsideModel, base)}
}
