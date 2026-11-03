package ledgercheck

import (
	"slices"
	"strings"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/pkg/pagecursor"
	oc "github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck/match"
	"github.com/formancehq/ledger/v3/tests/oracle"
)

// ValidateListLedgers checks one linearizable ListLedgers page (M/ledgers_list.go
// runLedgersList), foreign-tolerant through match.LedgerListingMatches (DESIGN
// §3.2): the listing is shared with ledgers the oracle does not track, so only
// the tracked rows are compared with the model, while the structural checks
// (repeated name, malformed row, reused id) apply to every row.
func ValidateListLedgers(c *oc.ReadCtx) []oc.Event {
	var req servicepb.ListLedgersRequest
	if err := decode(c.Request, &req); err != nil {
		return []oc.Event{oc.Violation(EvLcListingUnexpected, map[string]any{"error": "undecodable request: " + err.Error()})}
	}
	opts := req.GetOptions()

	// The endpoint supports no filter (ListOptionsSupport): any filter must be
	// refused with InvalidArgument, never served as an unfiltered page.
	if opts.GetFilter() != nil {
		if !c.Outcome.OK && strings.EqualFold(c.Outcome.Code, "InvalidArgument") {
			return []oc.Event{oc.Reached(EvLcListingRejectedFilter, map[string]any{})}
		}

		return []oc.Event{oc.Violation(EvLcListingAcceptedFilter, map[string]any{
			"ok": c.Outcome.OK, "code": c.Outcome.Code, "reason": c.Outcome.Reason, "rows": len(c.Outcome.Items),
		})}
	}

	if !c.Outcome.OK {
		return []oc.Event{oc.Violation(EvLcListingUnexpected, map[string]any{
			"code": c.Outcome.Code, "reason": c.Outcome.Reason, "message": c.Outcome.Message,
		})}
	}
	if !c.Outcome.Complete {
		// The client cut the stream short (more rows than it reads): not a
		// whole page, nothing to judge.
		return nil
	}

	infos := make([]*commonpb.LedgerInfo, 0, len(c.Outcome.Items))
	for i, raw := range c.Outcome.Items {
		info := &commonpb.LedgerInfo{}
		if err := decode(raw, info); err != nil {
			return []oc.Event{oc.Violation(EvLcListingMalformed, map[string]any{"row": i, "violation": "undecodable row: " + err.Error()})}
		}
		infos = append(infos, info)
	}

	served := make(map[string]bool, len(infos))
	ids := make(map[uint32]string, len(infos))
	names := make([]string, 0, len(infos))
	for _, info := range infos {
		name := info.GetName()
		if served[name] {
			return []oc.Event{oc.Violation(EvLcListingRepeated, map[string]any{"ledger": name})}
		}
		if v := match.LedgerInfoStructureViolation(info); v != "" {
			return []oc.Event{oc.Violation(EvLcListingMalformed, map[string]any{"ledger": name, "violation": v})}
		}
		if owner, taken := ids[info.GetId()]; taken {
			return []oc.Event{oc.Violation(EvLcListingReusedID, map[string]any{"id": info.GetId(), "ledger": name, "other": owner})}
		}
		ids[info.GetId()] = name
		served[name] = true
		names = append(names, name)
	}

	prefix := c.T.Config().TrackedPrefix
	lreq := match.ListLedgersReq{Cursor: opts.GetCursor(), PageSize: opts.GetPageSize(), Reverse: opts.GetReverse()}
	next := c.Outcome.NextCursor

	// One base must explain the window and every tracked row at once; the
	// window-only search below only picks which finding to report.
	pageMatches := c.Matches(func(base oracle.GlobalState) bool {
		ok, _ := match.LedgerListingMatches(base, prefix, lreq, infos, next)

		return ok
	})
	if !pageMatches {
		windowMatches := c.Matches(func(base oracle.GlobalState) bool {
			return listingWindowMatches(base, prefix, lreq, names, next)
		})
		model := c.T.Model()
		_, why := match.LedgerListingMatches(model, prefix, lreq, infos, next)
		details := map[string]any{
			"served":     strings.Join(names, ","),
			"cursor":     lreq.Cursor,
			"nextCursor": next,
			"pageSize":   lreq.PageSize,
			"reverse":    lreq.Reverse,
			"maxTicket":  c.MaxTicket,
			"drainedWhy": why,
		}
		if !windowMatches {
			key := cursorKey(lreq.Cursor)
			details["want"] = strings.Join(match.TrackedLedgerWindow(model, prefix, key, lreq.Reverse), ",")

			return []oc.Event{oc.Violation(EvLcListingNotWindow, details)}
		}
		for _, info := range infos {
			if strings.HasPrefix(info.GetName(), prefix) {
				details["model:"+info.GetName()] = modelLedgerDump(model, info.GetName())
			}
		}

		return []oc.Event{oc.Violation(EvLcListingOutsideModel, details)}
	}

	if diverged, info := identityViolation(c.T, prefix, infos); diverged != "" {
		return []oc.Event{oc.Violation(EvLcListingIdentity, map[string]any{
			"ledger":    diverged,
			"servedId":  info.GetId(),
			"createdAt": info.GetCreatedAt().String(),
		})}
	}

	evs := []oc.Event{oc.Reached(EvLcListingValidated, map[string]any{"count": len(infos)})}
	if next != "" {
		evs = append(evs, oc.Reached(EvLcListingTruncated, map[string]any{"count": len(infos)}))
	}
	if key := cursorKey(lreq.Cursor); key != "" && strings.HasPrefix(key, prefix) {
		if lc, ok := c.Snapshot.Lifecycle(key); ok && lc.Deleted {
			evs = append(evs, oc.Reached(EvLcListingResumedAtDeleted, map[string]any{"cursor": key}))
		}
	}

	return evs
}

func cursorKey(token string) string {
	cur, err := pagecursor.Decode(token)
	if err != nil {
		return ""
	}

	return cur.Key
}

// listingWindowMatches is LedgerListingMatches without the per-row ledger
// info comparison: the tracked names served are a prefix of the base's tracked
// window, foreign rows lie outside the tracked range, and the page length and
// next cursor agree with rules 4–5 of DESIGN §3.2. It only chooses between the
// "not the fleet's window" and "outside model" findings.
func listingWindowMatches(base oracle.GlobalState, prefix string, req match.ListLedgersReq, names []string, next string) bool {
	cur, err := pagecursor.Decode(req.Cursor)
	if err != nil || cur.Back {
		return false
	}
	want := match.TrackedLedgerWindow(base, prefix, cur.Key, req.Reverse)
	beyond := func(name string) bool {
		if req.Reverse {
			return name < prefix
		}

		return name > prefix
	}
	var tracked []string
	foreignAfter, sawTracked := false, false
	for _, name := range names {
		if strings.HasPrefix(name, prefix) {
			if foreignAfter {
				return false
			}
			tracked = append(tracked, name)
			sawTracked = true

			continue
		}
		if sawTracked && !beyond(name) {
			return false
		}
		if sawTracked || beyond(name) {
			foreignAfter = true
		}
	}
	if len(tracked) > len(want) || !slices.Equal(tracked, want[:len(tracked)]) {
		return false
	}
	exhausted := len(tracked) == len(want)
	if foreignAfter && !exhausted {
		return false
	}
	pageSize := match.EffectivePageSize(int(req.PageSize))
	switch {
	case len(names) > pageSize:
		return false
	case len(names) < pageSize:
		return exhausted && next == ""
	default:
		return next != "" || exhausted
	}
}

// identityViolation names the first tracked row whose (id, created_at) is not
// what its creation log reported (M/ledger_info.go ledgerIdentityViolation). A
// ledger whose create is still in flight or pending is skipped: its identity
// is not learned yet (ledgerCreateUnsettled).
func identityViolation(t *oc.Tracker, prefix string, infos []*commonpb.LedgerInfo) (string, *commonpb.LedgerInfo) {
	identities := t.LedgerIdentities()
	for _, info := range infos {
		name := info.GetName()
		if !strings.HasPrefix(name, prefix) {
			continue
		}
		identity, known := identities[name]
		if !known || createUnsettled(t, name) {
			continue
		}
		if !identity.MatchIdentity().Matches(info) {
			return name, info
		}
	}

	return "", nil
}

// createUnsettled reports whether a create of name is dispatched but
// unanswered, or answered but undrained.
func createUnsettled(t *oc.Tracker, name string) bool {
	for _, rec := range t.InflightBulks() {
		if bulkCreates(rec, name) {
			return true
		}
	}
	for _, rec := range t.PendingBulks() {
		if bulkCreates(rec, name) {
			return true
		}
	}

	return false
}

func bulkCreates(rec oc.WriteRecord, name string) bool {
	for _, r := range rec.Requests {
		if r.GetCreateLedger().GetName() == name {
			return true
		}
	}

	return false
}
