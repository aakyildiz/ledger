package querycheck

import (
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	oc "github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck/match"
	"github.com/formancehq/ledger/v3/tests/oracle"
)

const targetAccounts = commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS

// ValidateListAccounts is the ListAccounts read kind (M/queries.go
// runAccountQuery, error routing order of M/queries.go:170-211; transient
// outcomes were already skipped by the service).
func ValidateListAccounts(c *oc.ReadCtx) []oc.Event {
	req, err := DecodeListRequest(c.Request)
	if err != nil {
		return []oc.Event{oc.Violation(EvAccountQueryOutsideModel, map[string]any{"error": "undecodable request: " + err.Error()})}
	}
	evs := pageSizeEvents(req)
	ledger, filter := req.Ledger, req.Filter
	cursor := CursorKey(req.Cursor)
	needed := map[string]struct{}{}
	match.NeededIndexCanonicals(filter, targetAccounts, needed)
	_, _, bareAsset := match.HasAssetTarget(filter)
	invalidTarget := match.BareLeaf(filter) && match.FilterInvalidForTarget(filter, targetAccounts)
	refused := match.ClassifyRejectedFilter(filter, targetAccounts)
	o := c.Outcome

	if !o.OK {
		rerr := rpcError(o)
		if o.IsNotFound() {
			return append(evs, c.LedgerNotFoundExplained(ledger)...)
		}
		if e, done := handleInvalidTargetError(invalidTarget, targetAccounts, ledger, filter, o); done {
			return append(evs, e...)
		}
		if match.HasAssetPrecisionOverflow(filter) && rerr.Code == codesInvalidArgument && rerr.HasReason("FILTER_COMPILATION_ERROR") {
			return append(evs, oc.Reached(EvHasAssetPrecisionOverflowRejct, map[string]any{"ledger": ledger}))
		}
		if e, done := handleRejectedFilterError(c, refused, targetAccounts, ledger, filter, o); done {
			return append(evs, e...)
		}
		if bareAsset {
			return append(evs, validateAssetAccountQuery(c, req, cursor, nil)...)
		}
		if len(needed) > 0 {
			return append(evs, validateIndexedAccountQuery(c, req, cursor, needed, nil)...)
		}

		return append(evs, oc.Violation(EvListAccountsUnexpectedError, map[string]any{
			"ledger": ledger, "filter": match.DescribeFilter(filter), "error": errText(o),
		}))
	}

	accts, ok := decodeItems[commonpb.Account](o.Items)
	if !ok {
		return append(evs, oc.Violation(EvAccountQueryOutsideModel, map[string]any{"ledger": ledger, "error": "undecodable page"}))
	}
	if invalidTarget {
		return append(evs, oc.Violation(EvTargetInvalidAccountResults, map[string]any{
			"ledger": ledger, "filter": match.DescribeFilter(filter), "rows": len(accts),
		}))
	}
	if e, done := refusedFilterServed(refused, targetAccounts, ledger, filter, len(accts)); done {
		return append(evs, e...)
	}
	if bareAsset {
		return append(evs, validateAssetAccountQuery(c, req, cursor, accts)...)
	}
	if len(needed) > 0 {
		return append(evs, validateIndexedAccountQuery(c, req, cursor, needed, accts)...)
	}

	return append(evs, validateAccountQuery(c, req, cursor, accts)...)
}

func accountAddrs(accts []*commonpb.Account) []string {
	out := make([]string, len(accts))
	for i, a := range accts {
		out[i] = a.GetAddress()
	}

	return out
}

// validateAccountQuery (M/queries.go): an index-free filter's page must be some
// candidate base's ordered window, with the token that window implies.
func validateAccountQuery(c *oc.ReadCtx, req ListRequest, cursor string, accts []*commonpb.Account) []oc.Event {
	next := match.NextCursorOf(c.Outcome.NextCursor)
	if c.Matches(func(base oracle.GlobalState) bool {
		ls, live := match.LiveLedgerState(base, req.Ledger)

		return live && match.AccountPageMatches(ls, req.Filter, cursor, req.PageSize, req.Reverse, accts, next)
	}) {
		return nil
	}

	return []oc.Event{oc.Violation(EvAccountQueryOutsideModel, map[string]any{
		"ledger": req.Ledger, "filter": match.DescribeFilter(req.Filter), "cursor": cursor, "nextCursor": next,
		"pageSize": req.PageSize, "reverse": req.Reverse, "rows": len(accts),
		"serverAddrs": joinStrings(accountAddrs(accts)),
		"modelAddrs":  joinStrings(match.AccountWindow(c.T.Model().Ledger(req.Ledger), req.Filter, cursor, req.PageSize, req.Reverse)),
		"maxTicket":   c.MaxTicket,
	})}
}

// validateAssetAccountQuery (M/indexes.go): a bare has-asset filter; rows need
// the asset index present and the ever-touched window, a not-ready refusal
// needs the index not active on some base.
func validateAssetAccountQuery(c *oc.ReadCtx, req ListRequest, cursor string, accts []*commonpb.Account) []oc.Event {
	o := c.Outcome
	rerr := rpcError(o)
	ledger, filter := req.Ledger, req.Filter
	if !o.OK && !match.IsIndexNotFound(rerr) && !match.IsIndexNotReady(rerr) {
		return []oc.Event{oc.Violation(EvAssetIndexUnexpectedError, map[string]any{
			"ledger": ledger, "filter": match.DescribeFilter(filter), "error": errText(o),
		})}
	}
	base, precision, ok := match.HasAssetTarget(filter)
	if !ok {
		return []oc.Event{oc.Violation(EvAssetIndexFilterNotBare, map[string]any{"ledger": ledger, "filter": match.DescribeFilter(filter)})}
	}
	gotResults := o.OK
	next := match.NextCursorOf(o.NextCursor)
	pageSize := req.PageSize

	matched := c.Matches(func(cand oracle.GlobalState) bool {
		ls, live := match.LiveLedgerState(cand, ledger)
		if !live {
			return false
		}
		exists, active := ls.IndexState(match.AssetIndexCanonical)
		if !gotResults {
			return !exists || !active
		}
		if !exists {
			return false
		}
		want := match.AssetWindow(ls, base, precision, cursor, pageSize+1, req.Reverse)
		more := match.CursorForbidden
		if len(want) > pageSize {
			want = want[:pageSize]
			more = match.CursorRequired
		}
		if len(want) != len(accts) {
			return false
		}
		for i, addr := range want {
			if accts[i].GetAddress() != addr || !match.AccountMatches(ls, addr, accts[i]) {
				return false
			}
		}

		return match.NextCursorLegal(next, more, match.LastAccountKey(accts), len(accts), pageSize)
	})

	needed := map[string]struct{}{match.AssetIndexCanonical: {}}
	evs := coverageEvents(c, ledger, targetAccounts, filter, needed, matched && gotResults, len(accts))
	if matched {
		if gotResults {
			return append(evs, oc.Reached(EvAssetIndexAccountQueryServed, map[string]any{"ledger": ledger}))
		}

		return append(evs, oc.Reached(EvAssetIndexAccountQueryGated, map[string]any{"ledger": ledger}))
	}

	modelLS := c.T.Model().Ledger(ledger)
	d := map[string]any{
		"ledger": ledger, "filter": match.DescribeFilter(filter), "cursor": cursor, "pageSize": pageSize,
		"reverse": req.Reverse, "modelIdx": match.IndexStateLabelFull(modelLS, match.AssetIndexCanonical),
		"modelAddrs": joinStrings(match.AssetWindow(modelLS, base, precision, cursor, pageSize, req.Reverse)),
		"maxTicket":  c.MaxTicket,
	}
	if gotResults {
		d["rows"] = len(accts)
		d["serverAddrs"] = joinStrings(accountAddrs(accts))
	} else {
		d["error"] = errText(o)
	}

	return append(evs, oc.Violation(EvAssetIndexAccountQueryOutside, d))
}

// validateIndexedAccountQuery (M/indexes.go): the needed-set lifecycle gating
// with the ordered account window as the result check.
func validateIndexedAccountQuery(c *oc.ReadCtx, req ListRequest, cursor string, needed map[string]struct{}, accts []*commonpb.Account) []oc.Event {
	o := c.Outcome
	rerr := rpcError(o)
	ledger, filter := req.Ledger, req.Filter
	errKind, ok := match.ClassifyIndexedQueryError(rerr)
	if !ok {
		return []oc.Event{oc.Violation(EvIndexedAccountUnexpectedError, map[string]any{
			"ledger": ledger, "filter": match.DescribeFilter(filter), "error": errText(o),
		})}
	}
	next := match.NextCursorOf(o.NextCursor)
	rejectedIndex := match.RejectedIndexLabel(rerr)
	matched := c.Matches(func(cand oracle.GlobalState) bool {
		ls, live := match.LiveLedgerState(cand, ledger)

		return live && match.IndexedQueryOutcomeLegal(ls, targetAccounts, filter, needed, errKind, rejectedIndex, func(view oracle.LedgerState) bool {
			return match.AccountPageMatches(view, filter, cursor, req.PageSize, req.Reverse, accts, next)
		})
	})

	evs := coverageEvents(c, ledger, targetAccounts, filter, needed, matched && errKind == match.IndexedErrNone, len(accts))
	if matched {
		d := map[string]any{"ledger": ledger}
		switch errKind {
		case match.IndexedErrNone:
			evs = append(evs, oc.Reached(EvIndexedAccountQueryServed, d))
			if match.AnyLeaf(filter, func(l *commonpb.QueryFilter) bool { return l.GetAccountHasAsset() != nil }) {
				evs = append(evs, oc.Reached(EvHasAssetCombinatorServed, d))
			}
		case match.IndexedErrCompilation:
			evs = append(evs, oc.Reached(EvKindMismatchRejectedAccounts, d))
		default:
			evs = append(evs, oc.Reached(EvIndexedAccountQueryGated, d))
		}

		return evs
	}

	modelLS := c.T.Model().Ledger(ledger)
	d := map[string]any{
		"ledger": ledger, "filter": match.DescribeFilter(filter), "cursor": cursor, "pageSize": req.PageSize,
		"reverse": req.Reverse, "modelIdx": indexStates(modelLS, needed),
		"modelAddrs": joinStrings(match.AccountWindow(modelLS, filter, cursor, req.PageSize, req.Reverse)),
		"maxTicket":  c.MaxTicket,
	}
	if o.OK {
		d["rows"] = len(accts)
		d["serverAddrs"] = joinStrings(accountAddrs(accts))
	} else {
		d["error"] = errText(o)
	}

	return append(evs, oc.Violation(EvIndexedAccountQueryOutside, d))
}
