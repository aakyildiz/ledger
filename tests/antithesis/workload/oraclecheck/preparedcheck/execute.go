package preparedcheck

import (
	"encoding/hex"
	"encoding/json"

	"google.golang.org/grpc/codes"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	oc "github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck/match"
	"github.com/formancehq/ledger/v3/tests/oracle"
)

// pageTableCap bounds the remembered first pages (oldest evicted).
const pageTableCap = 1024

// PageRecord is a validated LIST page whose server cursor advertised more:
// what a follow-up page needs (the model-side after-key and the direction the
// page was read in). Keyed by the page's read ticket.
type PageRecord struct {
	Ledger, Name string
	Reverse      bool
	PageSize     uint32
	LastKey      []byte
	Next         string
}

type pageTable struct {
	byTicket map[uint64]PageRecord
	order    []uint64
}

func pages(t *oc.Tracker) *pageTable {
	return t.Ext(extKey, func() any { return &pageTable{byTicket: map[uint64]PageRecord{}} }).(*pageTable)
}

func (p *pageTable) put(ticket uint64, r PageRecord) {
	if _, ok := p.byTicket[ticket]; !ok {
		p.order = append(p.order, ticket)
	}
	p.byTicket[ticket] = r
	for len(p.order) > pageTableCap {
		delete(p.byTicket, p.order[0])
		p.order = p.order[1:]
	}
}

// Page returns the remembered page of a read ticket (tests and diagnostics).
func Page(t *oc.Tracker, ticket uint64) (PageRecord, bool) {
	r, ok := pages(t).byTicket[ticket]

	return r, ok
}

// ExecParams are the kind-specific /read/end params: a follow-up page names
// the read ticket of the page whose cursor it resumes.
type ExecParams struct {
	PrevTicket uint64 `json:"prevTicket,omitempty"`
}

// ValidateExecutePreparedQuery checks one ExecutePreparedQuery
// (M/prepared_queries_read.go runExecutePreparedQuery, runExecuteNextPage,
// runAggregateReverseMisuse, runAggregateTargetMisuse, validateExecuteList,
// validateExecuteAggregate). Generator intent (mode, reverse, parameters,
// page size, resumed page) comes from the request and the params; the stored
// definition is re-read from each candidate base.
func ValidateExecutePreparedQuery(c *oc.ReadCtx) []oc.Event {
	var req servicepb.ExecutePreparedQueryRequest
	if err := decode(c.Request, &req); err != nil {
		return []oc.Event{oc.Violation(EvPageOutsideModel, map[string]any{"error": "undecodable request: " + err.Error()})}
	}
	errv := rpcError(c.Outcome)

	// Misuse probe: AGGREGATE with reverse is refused before any read, so
	// InvalidArgument is the only legal answer on every base.
	if req.GetReverse() && req.GetMode() != commonpb.QueryMode_QUERY_MODE_LIST {
		d := map[string]any{"ledger": req.GetLedger(), "query": req.GetQueryName(), "mode": req.GetMode().String()}
		if errv.Code == codes.InvalidArgument {
			return []oc.Event{oc.Reached(EvReverseAggregateRejected, d)}
		}
		for k, v := range outcomeDetails(c.Outcome) {
			d[k] = v
		}
		d["ok"] = c.Outcome.OK

		return []oc.Event{oc.Violation(EvReverseAggregateNotRejected, d)}
	}

	call := match.PreparedCall{
		Ledger:   req.GetLedger(),
		Name:     req.GetQueryName(),
		Params:   match.PreparedParams(req.GetParameters()),
		PageSize: match.EffectivePageSize(int(req.GetPageSize())),
		Reverse:  req.GetReverse(),
		ErrKind:  match.ClassifyPreparedExecError(errv),
		Err:      errv,
	}
	if call.Params == nil {
		call.Params = match.PreparedParams{}
	}
	call.Complete = paramsComplete(c.Snapshot, call)

	var resp servicepb.ExecutePreparedQueryResponse
	if c.Outcome.OK {
		if err := decode(c.Outcome.Response, &resp); err != nil {
			return []oc.Event{oc.Violation(EvPageOutsideModel, map[string]any{"ledger": call.Ledger, "query": call.Name, "error": "undecodable response: " + err.Error()})}
		}
	}

	switch req.GetMode() {
	case commonpb.QueryMode_QUERY_MODE_AGGREGATE_VOLUMES:
		return validateAggregate(c, call, &resp)
	case commonpb.QueryMode_QUERY_MODE_LIST:
		return validateList(c, &req, call, &resp)
	default:
		// The driver never sends another mode.
		return nil
	}
}

// paramsComplete reports (diagnostics only) whether the parameters resolve the
// filter stored on the read's snapshot.
func paramsComplete(snap oracle.GlobalState, call match.PreparedCall) bool {
	q, ok := snap.Ledger(call.Ledger).PreparedQuery(call.Name)
	if !ok {
		return true
	}
	_, resolved := match.SubstituteParams(q.GetFilter(), call.Params)

	return resolved
}

func validateAggregate(c *oc.ReadCtx, call match.PreparedCall, resp *servicepb.ExecutePreparedQueryResponse) []oc.Event {
	_, isAgg := resp.GetResult().(*servicepb.ExecutePreparedQueryResponse_Aggregate)
	call.WrongResult = c.Outcome.OK && !isAgg
	agg := resp.GetAggregate()
	if c.Matches(func(base oracle.GlobalState) bool {
		if handled, legal := match.PreparedLedgerOutcomeLegal(base, call); handled {
			return legal
		}

		return match.PreparedAggregateOutcomeLegal(base.Ledger(call.Ledger), call, agg)
	}) {
		d := map[string]any{"ledger": call.Ledger, "query": call.Name}
		switch {
		case c.Outcome.OK:
			return []oc.Event{oc.Reached(EvAggregateValidated, d)}
		case call.ErrKind == match.PqErrAggregateTarget:
			return []oc.Event{oc.Reached(EvAggregateTargetRejected, d)}
		}

		return nil
	}

	return []oc.Event{oc.Violation(EvAggregateOutsideModel, map[string]any{
		"ledger":        call.Ledger,
		"query":         call.Name,
		"params":        describeParams(call.Params),
		"paramsFull":    call.Complete,
		"errKind":       int(call.ErrKind),
		"error":         outcomeDetails(c.Outcome),
		"serverVolumes": match.DescribeAggregate(agg),
		"modelQueries":  modelPreparedQueries(c.T.Model(), call.Ledger),
		"maxTicket":     c.MaxTicket,
	})}
}

func validateList(c *oc.ReadCtx, req *servicepb.ExecutePreparedQueryRequest, call match.PreparedCall, resp *servicepb.ExecutePreparedQueryResponse) []oc.Event {
	// A follow-up page resumes a remembered page: the model-side after-key is
	// that page's last row, so the opaque cursor is never decoded.
	var (
		after   []byte
		flipped bool
	)
	if req.GetCursor() != "" {
		var p ExecParams
		if len(c.Params) > 0 {
			_ = json.Unmarshal(c.Params, &p) // malformed params: treated as no previous page
		}
		prev, ok := pages(c.T).byTicket[p.PrevTicket]
		if !ok || prev.Next != req.GetCursor() || prev.Ledger != call.Ledger || prev.Name != call.Name {
			// Unknown or evicted previous page: nothing to validate against.
			return nil
		}
		after = prev.LastKey
		flipped = prev.Reverse != call.Reverse
	}

	if call.ErrKind == match.PqErrOther || call.ErrKind == match.PqErrAggregateTarget {
		d := outcomeDetails(c.Outcome)
		d["ledger"], d["query"], d["params"] = call.Ledger, call.Name, describeParams(call.Params)

		return []oc.Event{oc.Violation(EvExecUnexpectedError, d)}
	}
	cur := resp.GetCursor()
	if c.Outcome.OK && cur.GetPageSize() != uint32(call.PageSize) {
		return []oc.Event{oc.Violation(EvCursorPageSizeMismatch, map[string]any{
			"ledger": call.Ledger, "query": call.Name,
			"pageSize": cur.GetPageSize(), "expectedPageSize": call.PageSize, "requested": req.GetPageSize(),
		})}
	}
	_, isCursor := resp.GetResult().(*servicepb.ExecutePreparedQueryResponse_Cursor)
	call.WrongResult = c.Outcome.OK && !isCursor

	if c.Matches(func(base oracle.GlobalState) bool {
		if handled, legal := match.PreparedLedgerOutcomeLegal(base, call); handled {
			return legal
		}

		return match.PreparedListOutcomeLegal(base.Ledger(call.Ledger), call, after, cur)
	}) {
		if !c.Outcome.OK {
			return nil
		}
		d := map[string]any{"ledger": call.Ledger, "query": call.Name, "reverse": call.Reverse, "followUp": after != nil}
		evs := []oc.Event{oc.Reached(EvPageValidated, d)}
		if flipped {
			evs = append(evs, oc.Reached(EvPageResumedOpposite, map[string]any{"ledger": call.Ledger, "reverse": call.Reverse}))
		}
		if last := match.LastPageKey(cur); cur.GetNext() != "" && len(last) > 0 {
			pages(c.T).put(c.Ticket, PageRecord{
				Ledger: call.Ledger, Name: call.Name, Reverse: call.Reverse,
				PageSize: req.GetPageSize(), LastKey: last, Next: cur.GetNext(),
			})
		}

		return evs
	}

	return []oc.Event{oc.Violation(EvPageOutsideModel, map[string]any{
		"ledger":       call.Ledger,
		"query":        call.Name,
		"params":       describeParams(call.Params),
		"paramsFull":   call.Complete,
		"after":        hex.EncodeToString(after),
		"pageSize":     call.PageSize,
		"reverse":      call.Reverse,
		"errKind":      int(call.ErrKind),
		"error":        outcomeDetails(c.Outcome),
		"hasMore":      cur.GetHasMore(),
		"serverRows":   match.PreparedServerRows(cur),
		"modelQueries": modelPreparedQueries(c.T.Model(), call.Ledger),
		"response":     rawOrNull(c.Outcome.Response),
		"maxTicket":    c.MaxTicket,
	})}
}
