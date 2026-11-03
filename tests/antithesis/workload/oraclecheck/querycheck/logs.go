package querycheck

import (
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	oc "github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck/match"
	"github.com/formancehq/ledger/v3/tests/oracle"
)

const targetLogs = commonpb.QueryTarget_QUERY_TARGET_LOGS

// ValidateListLogs is the ListLogs read kind (M/queries_logs.go runLogQuery,
// routing of M/queries_logs.go:641-774; transient outcomes other than
// INDEX_BUILDING were already skipped by the service).
func ValidateListLogs(c *oc.ReadCtx) []oc.Event {
	req, err := DecodeListRequest(c.Request)
	if err != nil {
		return []oc.Event{oc.Violation(EvLogQueryOutsideModel, map[string]any{"error": "undecodable request: " + err.Error()})}
	}
	evs := pageSizeEvents(req)
	ledger, filter := req.Ledger, req.Filter
	afterSeq, malformed := UintCursor(req.Cursor)
	refused := match.ClassifyRejectedFilter(filter, targetLogs)
	o := c.Outcome
	rerr := rpcError(o)

	if o.IsNotFound() {
		return append(evs, c.LedgerNotFoundExplained(ledger)...)
	}
	if !o.OK && malformed && rerr.Code == codesInvalidArgument {
		// A refused cursor may be reported ahead of a refused filter.
		refused = match.RejectedFilter{}
	}
	if !o.OK {
		if e, done := handleRejectedFilterError(c, refused, targetLogs, ledger, filter, o); done {
			return append(evs, e...)
		}
	}

	errKind, gated := match.ClassifyLogQueryError(rerr)
	if !gated {
		if e, done := handleMalformedCursorError(malformed, targetLogs, req.Cursor, o); done {
			return append(evs, e...)
		}

		return append(evs, oc.Violation(EvListLogsUnexpectedError, map[string]any{
			"ledger": ledger, "filter": match.DescribeFilter(filter), "error": errText(o),
		}))
	}
	if malformed && !o.OK {
		// The handler decodes the cursor before the filter compiles, so an
		// index gate cannot answer ahead of the cursor's refusal.
		e, _ := handleMalformedCursorError(malformed, targetLogs, req.Cursor, o)

		return append(evs, e...)
	}

	var logs []*commonpb.Log
	if o.OK {
		var ok bool
		if logs, ok = decodeItems[commonpb.Log](o.Items); !ok {
			return append(evs, oc.Violation(EvLogQueryOutsideModel, map[string]any{"ledger": ledger, "error": "undecodable page"}))
		}
	}
	if malformed {
		return append(evs, oc.Violation(EvMalformedLogCursorResults, map[string]any{"cursor": req.Cursor, "rows": len(logs)}))
	}
	if o.OK {
		if e, done := refusedFilterServed(refused, targetLogs, ledger, filter, len(logs)); done {
			return append(evs, e...)
		}
	}

	return append(evs, validateLogQuery(c, req, afterSeq, logs, match.NeededLogIndexes(filter), errKind)...)
}

// LogPageOrderViolation is the self-structure check of a ListLogs page
// (M/queries_logs.go validateLogQuery): ordered in its direction (ascending,
// or descending when reverse), inside the exclusive cursor, and no longer
// than the page size. True means the page violates it.
func LogPageOrderViolation(ids []uint64, afterSeq uint64, reverse bool, pageSize int) bool {
	if len(ids) > pageSize {
		return true
	}
	for i, id := range ids {
		outsideCursor := afterSeq > 0 && (!reverse && id <= afterSeq || reverse && id >= afterSeq)
		outOfOrder := i > 0 && (!reverse && id <= ids[i-1] || reverse && id >= ids[i-1])
		if outsideCursor || outOfOrder {
			return true
		}
	}

	return false
}

// signedRowOfUnsignedBulk returns the first served row carrying a response
// signature whose committing bulk the oracle knows and knows to be unsigned
// (task 16 signs only some bulks, with the root key). Rows whose bulk the
// oracle cannot attribute yet are not judged.
func signedRowOfUnsignedBulk(t *oc.Tracker, page []match.ServerLogRow) (match.ServerLogRow, string, bool) {
	var bulks map[uint64]oc.CommittedBulk
	for _, row := range page {
		if !row.Signed || row.Sequence == 0 {
			continue
		}
		if bulks == nil {
			bulks = t.CommittedBulks()
		}
		for _, b := range bulks {
			if row.Sequence < b.MinSeq || row.Sequence > b.MaxSeq || b.Wid == "" {
				continue
			}
			if rec, ok := t.Record(b.Wid); ok && !rec.Signed {
				return row, b.Wid, true
			}
		}
	}

	return match.ServerLogRow{}, "", false
}

// validateLogQuery (M/queries_logs.go): self-structure first, then the shared
// index-lifecycle legality with the base's ordered log window.
func validateLogQuery(c *oc.ReadCtx, req ListRequest, afterSeq uint64, logs []*commonpb.Log, needed map[string]struct{}, errKind match.IndexedErrKind) []oc.Event {
	ledger, filter := req.Ledger, req.Filter
	page := match.ServerLogRows(logs)
	ids := match.ServerLogIDs(logs)

	if LogPageOrderViolation(ids, afterSeq, req.Reverse, req.PageSize) {
		return []oc.Event{oc.Violation(EvLogPageViolatesOrdering, map[string]any{
			"ledger": ledger, "filter": match.DescribeFilter(filter), "afterSeq": afterSeq,
			"reverse": req.Reverse, "pageSize": req.PageSize, "serverIds": joinUint64(ids),
		})}
	}
	if row, wid, bad := signedRowOfUnsignedBulk(c.T, page); bad {
		return []oc.Event{oc.Violation(EvUnsignedLogCarriesSignature, map[string]any{
			"ledger": ledger, "logId": row.ID, "sequence": row.Sequence, "wid": wid,
		})}
	}

	next := match.NextCursorOf(c.Outcome.NextCursor)
	matched := c.Matches(func(base oracle.GlobalState) bool {
		ls, live := match.LiveLedgerState(base, ledger)

		return live && match.LogOutcomeLegal(ls, ledger, filter, needed, errKind, page, afterSeq, req.Reverse, req.PageSize, next)
	})

	evs := coverageEvents(c, ledger, targetLogs, filter, needed, matched && errKind == match.IndexedErrNone, len(page))
	if matched {
		d := map[string]any{"ledger": ledger}
		if errKind == match.IndexedErrNotReady {
			return append(evs, oc.Reached(EvLogQueryGated, d))
		}
		evs = append(evs, oc.Reached(EvLogQueryServed, d))
		if req.Reverse && len(page) > 0 {
			evs = append(evs, oc.Reached(EvReverseLogQueryServed, d))
		}

		return evs
	}

	modelLS := c.T.Model().Ledger(ledger)
	d := map[string]any{
		"ledger": ledger, "filter": match.DescribeFilter(filter), "nextCursor": next, "afterSeq": afterSeq,
		"pageSize": req.PageSize, "reverse": req.Reverse, "rows": len(ids), "serverIds": joinUint64(ids),
		"serverRows": match.DescribeServerLogRows(page),
		"modelIds":   joinUint64(match.LogWindow(modelLS, ledger, filter, afterSeq, req.Reverse, req.PageSize)),
		"modelIdx":   indexStates(modelLS, needed),
		"modelKinds": joinStrings(modelLS.LogKinds()),
		"maxTicket":  c.MaxTicket,
	}
	if !c.Outcome.OK {
		d["error"] = errText(c.Outcome)
	}

	return append(evs, oc.Violation(EvLogQueryOutsideModel, d))
}
