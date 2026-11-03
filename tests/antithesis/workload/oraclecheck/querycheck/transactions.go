package querycheck

import (
	"strconv"
	"strings"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	oc "github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck/match"
	"github.com/formancehq/ledger/v3/tests/oracle"
)

const targetTransactions = commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS

// ValidateListTransactions is the ListTransactions read kind (M/queries.go
// runTransactionQuery and its error routing order). The endpoint inverts the
// reverse flag (reverse=false is newest first); the matchers handle it.
func ValidateListTransactions(c *oc.ReadCtx) []oc.Event {
	req, err := DecodeListRequest(c.Request)
	if err != nil {
		return []oc.Event{oc.Violation(EvTransactionQueryOutsideModel, map[string]any{"error": "undecodable request: " + err.Error()})}
	}
	evs := pageSizeEvents(req)
	ledger, filter := req.Ledger, req.Filter
	afterID, malformed := UintCursor(req.Cursor)
	unsupported := IsUnsupportedBuiltinProbe(filter)
	needed := map[string]struct{}{}
	match.NeededIndexCanonicals(filter, targetTransactions, needed)
	invalidTarget := match.BareLeaf(filter) && match.FilterInvalidForTarget(filter, targetTransactions)
	refused := match.ClassifyRejectedFilter(filter, targetTransactions)
	o := c.Outcome

	if !o.OK {
		if e, done := handleMalformedCursorError(malformed, targetTransactions, req.Cursor, o); done {
			return append(evs, e...)
		}
		if o.IsNotFound() {
			return append(evs, c.LedgerNotFoundExplained(ledger)...)
		}
		if unsupported {
			if rpcError(o).Code == codesInvalidArgument {
				return append(evs, oc.Reached(EvUnsupportedBuiltinRejected, map[string]any{"ledger": ledger}))
			}

			return append(evs, oc.Violation(EvUnsupportedBuiltinUnexpectedErr, map[string]any{
				"ledger": ledger, "filter": match.DescribeFilter(filter), "error": errText(o),
			}))
		}
		if e, done := handleInvalidTargetError(invalidTarget, targetTransactions, ledger, filter, o); done {
			return append(evs, e...)
		}
		if e, done := handleRejectedFilterError(c, refused, targetTransactions, ledger, filter, o); done {
			return append(evs, e...)
		}
		if len(needed) > 0 {
			return append(evs, validateIndexedTransactionQuery(c, req, afterID, needed, nil)...)
		}

		return append(evs, oc.Violation(EvListTransactionsUnexpectedError, map[string]any{
			"ledger": ledger, "filter": match.DescribeFilter(filter), "error": errText(o),
		}))
	}

	txs, ok := decodeItems[commonpb.Transaction](o.Items)
	if !ok {
		return append(evs, oc.Violation(EvTransactionQueryOutsideModel, map[string]any{"ledger": ledger, "error": "undecodable page"}))
	}
	switch {
	case malformed:
		return append(evs, oc.Violation(EvMalformedTxCursorResults, map[string]any{"cursor": req.Cursor, "rows": len(txs)}))
	case unsupported:
		return append(evs, oc.Violation(EvUnsupportedBuiltinResults, map[string]any{
			"ledger": ledger, "filter": match.DescribeFilter(filter), "rows": len(txs),
		}))
	case invalidTarget:
		return append(evs, oc.Violation(EvTargetInvalidTxResults, map[string]any{
			"ledger": ledger, "filter": match.DescribeFilter(filter), "rows": len(txs),
		}))
	}
	if e, done := refusedFilterServed(refused, targetTransactions, ledger, filter, len(txs)); done {
		return append(evs, e...)
	}
	if len(needed) > 0 {
		return append(evs, validateIndexedTransactionQuery(c, req, afterID, needed, txs)...)
	}

	return append(evs, validateTransactionQuery(c, req, afterID, txs)...)
}

func txIDs(txs []*commonpb.Transaction) string {
	parts := make([]string, len(txs))
	for i, t := range txs {
		parts[i] = strconv.FormatUint(t.GetId(), 10)
	}

	return strings.Join(parts, ",")
}

func joinUint64(ids []uint64) string {
	parts := make([]string, len(ids))
	for i, id := range ids {
		parts[i] = strconv.FormatUint(id, 10)
	}

	return strings.Join(parts, ",")
}

// validateTransactionQuery (M/queries.go): an index-free filter's page must be
// some candidate base's window (optional rows for unlearned stamps).
func validateTransactionQuery(c *oc.ReadCtx, req ListRequest, afterID uint64, txs []*commonpb.Transaction) []oc.Event {
	next := match.NextCursorOf(c.Outcome.NextCursor)
	if c.Matches(func(base oracle.GlobalState) bool {
		ls, live := match.LiveLedgerState(base, req.Ledger)

		return live && match.TxWindowMatches(ls, req.Filter, afterID, req.PageSize, req.Reverse, txs, next)
	}) {
		return nil
	}

	return []oc.Event{oc.Violation(EvTransactionQueryOutsideModel, map[string]any{
		"ledger": req.Ledger, "filter": match.DescribeFilter(req.Filter), "nextCursor": next, "afterId": afterID,
		"pageSize": req.PageSize, "reverse": req.Reverse, "rows": len(txs), "serverIds": txIDs(txs),
		"modelIds":  joinUint64(match.TransactionWindow(c.T.Model().Ledger(req.Ledger), req.Filter, afterID, req.PageSize, req.Reverse)),
		"maxTicket": c.MaxTicket,
	})}
}

// validateIndexedTransactionQuery (M/indexes.go).
func validateIndexedTransactionQuery(c *oc.ReadCtx, req ListRequest, afterID uint64, needed map[string]struct{}, txs []*commonpb.Transaction) []oc.Event {
	o := c.Outcome
	rerr := rpcError(o)
	ledger, filter := req.Ledger, req.Filter
	errKind, ok := match.ClassifyIndexedQueryError(rerr)
	if !ok {
		return []oc.Event{oc.Violation(EvIndexedTxUnexpectedError, map[string]any{
			"ledger": ledger, "filter": match.DescribeFilter(filter), "error": errText(o),
		})}
	}
	next := match.NextCursorOf(o.NextCursor)
	rejectedIndex := match.RejectedIndexLabel(rerr)
	matched := c.Matches(func(cand oracle.GlobalState) bool {
		ls, live := match.LiveLedgerState(cand, ledger)

		return live && match.IndexedQueryOutcomeLegal(ls, targetTransactions, filter, needed, errKind, rejectedIndex, func(view oracle.LedgerState) bool {
			return match.TxWindowMatches(view, filter, afterID, req.PageSize, req.Reverse, txs, next)
		})
	})

	evs := coverageEvents(c, ledger, targetTransactions, filter, needed, matched && errKind == match.IndexedErrNone, len(txs))
	if matched {
		d := map[string]any{"ledger": ledger}
		switch errKind {
		case match.IndexedErrNone:
			return append(evs, oc.Reached(EvIndexedTxQueryServed, d))
		case match.IndexedErrCompilation:
			return append(evs, oc.Reached(EvKindMismatchRejectedTxs, d))
		default:
			return append(evs, oc.Reached(EvIndexedTxQueryGated, d))
		}
	}

	modelLS := c.T.Model().Ledger(ledger)
	d := map[string]any{
		"ledger": ledger, "filter": match.DescribeFilter(filter), "afterId": afterID, "pageSize": req.PageSize,
		"reverse": req.Reverse, "modelIdx": indexStates(modelLS, needed),
		"modelIds":  joinUint64(match.TransactionWindow(modelLS, filter, afterID, req.PageSize, req.Reverse)),
		"maxTicket": c.MaxTicket,
	}
	if o.OK {
		d["rows"] = len(txs)
		d["serverIds"] = txIDs(txs)
	} else {
		d["error"] = errText(o)
	}

	return append(evs, oc.Violation(EvIndexedTransactionQueryOutside, d))
}
