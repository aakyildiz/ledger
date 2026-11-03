package auditcheck

import (
	"encoding/json"
	"fmt"
	"math"
	"strings"

	"github.com/formancehq/ledger/v3/internal/proto/auditpb"
	oc "github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck/match"
)

// Read kind names (DESIGN §5.3).
const (
	KindList = "ListAuditEntries"
	KindGet  = "GetAuditEntry"
	// ViewSamples is the family's aiming view.
	ViewSamples = "auditSamples"
)

func errText(o oc.Outcome) string { return fmt.Sprintf("%s/%s: %s", o.Code, o.Reason, o.Message) }

func isCode(o oc.Outcome, code string) bool {
	return !o.OK && o.Unknown == "" && strings.EqualFold(o.Code, code)
}

func decodeEntry(raw json.RawMessage) (match.AuditEntry, error) {
	e := &auditpb.AuditEntry{}
	if err := unmarshalOpts.Unmarshal(raw, e); err != nil {
		return match.AuditEntry{}, err
	}

	return match.AuditEntryOf(e), nil
}

func probeEvent(p Probe, filter string) oc.Event {
	d := map[string]any{"filter": filter}
	switch p {
	case ProbeSeqInOr:
		return oc.Reached(EvAuditSeqInOrRejected, d)
	case ProbeUnspecifiedField:
		return oc.Reached(EvAuditUnspecifiedRejected, d)
	case ProbeTypeMismatch:
		return oc.Reached(EvAuditTypeMismatchRejected, d)
	case ProbeKeyParam:
		return oc.Reached(EvAuditKeyParamRejected, d)
	case ProbeNulPrefix:
		return oc.Reached(EvAuditNulPrefixRejected, d)
	default:
		return oc.Reached(EvAuditNotRejected, d)
	}
}

func findingEvent(f *Finding, d map[string]any) oc.Event {
	d["entry"], d["why"] = f.Entry, f.Why
	if f.Missing != 0 {
		d["missingSeq"] = f.Missing
	}

	return oc.Violation(f.ID, d)
}

// ValidateListAuditEntries is the ListAuditEntries read kind (M/audit.go
// runAuditQuery). Transient outcomes were already skipped by the service.
func ValidateListAuditEntries(c *oc.ReadCtx) []oc.Event {
	req, err := DecodeListRequest(c.Request)
	if err != nil {
		return []oc.Event{oc.Violation(EvAuditPageContract, map[string]any{"violation": "undecodable request: " + err.Error()})}
	}
	filter := match.DescribeFilter(req.Filter)
	probe := ClassifyProbe(req.Filter)
	afterSeq, malformed := UintCursor(req.Cursor)
	o := c.Outcome

	if !o.OK {
		switch {
		case malformed && isCode(o, "InvalidArgument"):
			return []oc.Event{oc.Reached(EvAuditMalformedCursorRejected, map[string]any{"cursor": req.Cursor})}
		case probe != ProbeNone && isCode(o, "InvalidArgument"):
			return []oc.Event{probeEvent(probe, filter)}
		case probe == ProbeNone && NeedsIndex(req.Filter) && isCode(o, "Unavailable") && o.Reason == "INDEX_BUILDING":
			// The audit projection is rebuilding or a checkpoint is frozen
			// (ctrl ListAuditEntries readiness gate): not a verdict.
			return nil
		}

		return []oc.Event{oc.Violation(EvAuditListUnexpectedError, map[string]any{
			"filter": filter, "cursor": req.Cursor, "malformed": malformed, "error": errText(o),
		})}
	}
	entries := make([]match.AuditEntry, 0, len(o.Items))
	for i, raw := range o.Items {
		e, err := decodeEntry(raw)
		if err != nil {
			return []oc.Event{oc.Violation(EvAuditPageContract, map[string]any{"violation": fmt.Sprintf("item %d does not decode: %v", i, err)})}
		}
		entries = append(entries, e)
	}
	if malformed {
		return []oc.Event{oc.Violation(EvAuditMalformedCursorResults, map[string]any{"cursor": req.Cursor, "rows": len(entries)})}
	}
	if probe != ProbeNone {
		return []oc.Event{oc.Violation(EvAuditUnsupportedFilterPage, map[string]any{"filter": filter, "rows": len(entries)})}
	}
	if !o.Complete {
		// The client stopped at its item cap: the page is not whole, so the
		// page contract and the model checks have nothing to judge. The
		// refusals above hold for any OK answer, whole or not.
		return nil
	}

	m := ModelOf(c.T, c.MaxTicket)
	st := stateOf(c.T)
	st.noteSamples(entries, m.Logs)

	next := match.NextCursorOf(o.NextCursor)
	d := map[string]any{
		"filter": filter, "pageSize": req.PageSize, "requested": req.RequestedPageSize, "reverse": req.Reverse,
		"cursor": req.Cursor, "nextCursor": next, "rows": len(entries), "seqs": match.DescribeAuditSeqs(entries),
		"maxTicket": c.MaxTicket,
	}
	if why := match.AuditPageViolation(entries, req.PageSize, req.Reverse, req.Filter, afterSeq, next); why != "" {
		d["violation"] = why

		return []oc.Event{oc.Violation(EvAuditPageContract, d)}
	}

	known := st.knownBefore(c.Ticket)
	v := ValidatePage(m, known, Page{Entries: entries, Filter: req.Filter, Reverse: req.Reverse, PageSize: req.PageSize, AfterSeq: afterSeq, Next: next, ReadTicket: c.Ticket})
	if v.Finding != nil {
		return []oc.Event{findingEvent(v.Finding, d)}
	}
	if f := DuplicateProposalViolation(m, known, entries); f != nil {
		return []oc.Event{findingEvent(f, d)}
	}
	var evs []oc.Event
	f, checked := BatchKeyViolation(m, entries)
	if f != nil {
		return []oc.Event{findingEvent(f, d)}
	}
	if checked > 0 {
		evs = append(evs, oc.Reached(EvAuditBatchKeyMatched, map[string]any{"checked": checked}))
	}

	st.rememberKnown(entries, c.T.ReadFrontier())

	evs = append(evs, oc.Reached(EvAuditPageValidated, map[string]any{"filter": filter, "rows": len(entries)}))
	if v.Rejections > 0 {
		evs = append(evs, oc.Reached(EvAuditFailureMatchedRejection, map[string]any{"rejections": v.Rejections}))
	}
	if v.Unjudged > 0 {
		evs = append(evs, oc.Reached(EvAuditRetryableFailureUnjudged, map[string]any{"unjudged": v.Unjudged}))
	}
	if v.FailureOwedChecked {
		evs = append(evs, oc.Reached(EvAuditFailureOwedChecked, map[string]any{"filter": filter}))
	}
	if v.KnownCertified > 0 {
		evs = append(evs, oc.Reached(EvAuditJudgedAgainstKnown, map[string]any{"certified": v.KnownCertified}))
	}
	if v.CursorCorroborated {
		evs = append(evs, oc.Reached(EvAuditCursorCorroborated, map[string]any{}))
	}

	return evs
}

// ValidateGetAuditEntry is the GetAuditEntry read kind (M/audit.go
// runAuditEntryRead): a sequence past anything the trail holds (MaxUint64)
// must resolve NotFound; a sampled one must be served as itself and hold the
// per-entry rules. It is the only path that carries items.
func ValidateGetAuditEntry(c *oc.ReadCtx) []oc.Event {
	seq, err := DecodeGetRequest(c.Request)
	if err != nil {
		return []oc.Event{oc.Violation(EvAuditGetUnexpectedError, map[string]any{"error": "undecodable request: " + err.Error()})}
	}
	absent := seq == math.MaxUint64
	o := c.Outcome
	if !o.OK {
		if absent && o.IsNotFound() {
			return []oc.Event{oc.Reached(EvAuditUnassignedNotFound, map[string]any{})}
		}

		return []oc.Event{oc.Violation(EvAuditGetUnexpectedError, map[string]any{"sequence": seq, "absent": absent, "error": errText(o)})}
	}
	if absent {
		return []oc.Event{oc.Violation(EvAuditEntryServedUnassigned, map[string]any{"sequence": seq})}
	}
	e, err := decodeEntry(o.Response)
	if err != nil {
		return []oc.Event{oc.Violation(EvAuditGetUnexpectedError, map[string]any{"sequence": seq, "error": "undecodable entry: " + err.Error()})}
	}
	if e.Seq != seq {
		return []oc.Event{oc.Violation(EvAuditEntryReadOtherSeq, map[string]any{"asked": seq, "served": e.Seq})}
	}

	m := ModelOf(c.T, c.MaxTicket)
	d := map[string]any{"sequence": seq, "maxTicket": c.MaxTicket}
	f, ev := checkEntry(m, e, nil)
	if f != nil {
		return []oc.Event{findingEvent(f, d)}
	}
	st := stateOf(c.T)
	if f := DuplicateProposalViolation(m, st.knownBefore(c.Ticket), []match.AuditEntry{e}); f != nil {
		return []oc.Event{findingEvent(f, d)}
	}
	st.rememberKnown([]match.AuditEntry{e}, c.T.ReadFrontier())

	evs := []oc.Event{oc.Reached(EvAuditEntryReadValidated, map[string]any{})}
	if ev == entryUnjudged {
		evs = append(evs, oc.Reached(EvAuditRetryableFailureUnjudged, map[string]any{"unjudged": 1}))
	}
	if len(e.ItemSeqs) > 0 {
		evs = append(evs, oc.Reached(EvAuditEntryCarriedItems, map[string]any{"orders": len(e.ItemSeqs)}))
	}

	return evs
}
