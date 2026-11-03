package indexcheck

import (
	"encoding/json"
	"slices"
	"strings"

	"github.com/formancehq/ledger/v3/internal/domain/indexes"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	oc "github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck/match"
	"github.com/formancehq/ledger/v3/tests/oracle"
)

// The registry reads (M/index_introspect.go). The registry is an FSM
// projection of the CreateIndex / DropIndex / RemoveMetadataFieldType log
// stream, exactly what the model's index map tracks, so membership is checked
// for equality. build_status, created_at, cursor and the versions are not
// checked (M/index_introspect.go:24-36).

// isAbsentProbe reports a ledger name the driver drew as an absent probe
// (DESIGN §3: o-<run>-absent-<draw>, never created).
func isAbsentProbe(c *oc.ReadCtx, ledger string) bool {
	return strings.HasPrefix(ledger, c.T.Config().TrackedPrefix+"absent-")
}

func errDetails(c *oc.ReadCtx, d map[string]any) map[string]any {
	d["code"], d["reason"], d["message"] = c.Outcome.Code, c.Outcome.Reason, c.Outcome.Message

	return d
}

func modelIndexList(g oracle.GlobalState, ledger string) string {
	var out []string
	for canonical := range g.Ledger(ledger).Indexes().All() {
		out = append(out, canonical)
	}
	slices.Sort(out)

	return strings.Join(out, ",")
}

func decodeEntries(items []json.RawMessage) ([]*commonpb.Index, error) {
	out := make([]*commonpb.Index, 0, len(items))
	for _, raw := range items {
		idx := &commonpb.Index{}
		if err := unmarshal.Unmarshal(raw, idx); err != nil {
			return nil, err
		}
		out = append(out, idx)
	}

	return out, nil
}

// ValidateListIndexes validates a ListIndexes stream in one of its three
// scopes (M/index_introspect.go runIndexList / runIndexListFleet). SCOPE_ALL
// is restricted to tracked ledgers (DESIGN §3.2) with the mayVanish
// tolerance kept.
func ValidateListIndexes(c *oc.ReadCtx) []oc.Event {
	var req servicepb.ListIndexesRequest
	if err := unmarshal.Unmarshal(c.Request, &req); err != nil {
		return nil
	}
	if req.GetScope() == servicepb.ListIndexesRequest_SCOPE_LEDGER {
		return validateLedgerListing(c, req.GetLedger())
	}

	return validateFleetListing(c, req.GetScope())
}

func validateLedgerListing(c *oc.ReadCtx, ledger string) []oc.Event {
	absent := isAbsentProbe(c, ledger)
	if !c.Outcome.OK {
		if c.Outcome.IsNotFound() {
			if absent {
				return []oc.Event{oc.Reached(EvListingAbsentNotFound, map[string]any{"ledger": ledger})}
			}

			return c.LedgerNotFoundExplained(ledger)
		}

		return []oc.Event{oc.Violation(EvListIndexesError, errDetails(c, map[string]any{"ledger": ledger, "absent": absent}))}
	}
	if !c.Outcome.Complete {
		return nil // capped stream: not a whole listing
	}
	if absent {
		return []oc.Event{oc.Violation(EvListingOutsideFleet, map[string]any{"ledger": ledger, "count": len(c.Outcome.Items)})}
	}
	entries, err := decodeEntries(c.Outcome.Items)
	if err != nil {
		return []oc.Event{oc.Violation(EvListingScope, map[string]any{"ledger": ledger, "error": "undecodable entry: " + err.Error()})}
	}
	served := make(map[string]bool, len(entries))
	for _, idx := range entries {
		if idx.GetId() == nil || idx.GetLedger() != ledger {
			return []oc.Event{oc.Violation(EvListingScope, map[string]any{"ledger": ledger, "entryLedger": idx.GetLedger(), "hasID": idx.GetId() != nil})}
		}
		canonical := indexes.Canonical(idx.GetId())
		if served[canonical] {
			return []oc.Event{oc.Violation(EvListingRepeated, map[string]any{"ledger": ledger, "index": canonical})}
		}
		served[canonical] = true
	}
	if c.Matches(func(base oracle.GlobalState) bool { return match.IndexSetEqual(base.Ledger(ledger), served) }) {
		return []oc.Event{oc.Reached(EvListingValidated, map[string]any{"ledger": ledger, "count": len(served)})}
	}

	return []oc.Event{oc.Violation(EvListingOutsideModel, map[string]any{"ledger": ledger,
		"serverIdx": match.JoinSortedKeys(served), "modelIndexes": modelIndexList(c.T.Model(), ledger), "maxTicket": c.MaxTicket})}
}

func validateFleetListing(c *oc.ReadCtx, scope servicepb.ListIndexesRequest_Scope) []oc.Event {
	bucketScope := scope == servicepb.ListIndexesRequest_SCOPE_BUCKET
	if !c.Outcome.OK {
		return []oc.Event{oc.Violation(EvBucketListIndexesError, errDetails(c, map[string]any{"scope": scope.String()}))}
	}
	if !c.Outcome.Complete {
		return nil
	}
	entries, err := decodeEntries(c.Outcome.Items)
	if err != nil {
		return []oc.Event{oc.Violation(EvBucketListingNoID, map[string]any{"scope": scope.String(), "error": "undecodable entry: " + err.Error()})}
	}
	byLedger := map[string]map[string]bool{}
	for _, idx := range entries {
		if idx.GetId() == nil {
			return []oc.Event{oc.Violation(EvBucketListingNoID, map[string]any{"scope": scope.String(), "ledger": idx.GetLedger()})}
		}
		owner := idx.GetLedger()
		canonical := indexes.Canonical(idx.GetId())
		if bucketScope && owner != "" {
			return []oc.Event{oc.Violation(EvBucketScopedListingLedger, map[string]any{"ledger": owner, "index": canonical})}
		}
		if byLedger[owner] == nil {
			byLedger[owner] = map[string]bool{}
		}
		if byLedger[owner][canonical] {
			return []oc.Event{oc.Violation(EvBucketListingRepeated, map[string]any{"scope": scope.String(), "ledger": owner, "index": canonical})}
		}
		byLedger[owner][canonical] = true
	}
	if bucketScope {
		return []oc.Event{oc.Reached(EvBucketListingValid, map[string]any{"count": len(entries)})}
	}

	prefix := c.T.Config().TrackedPrefix
	mayVanish := match.LedgersDeletedInSomeBase(func(visit func(oracle.GlobalState) bool) {
		c.T.WalkCandidates(c.MaxTicket, visit, nil)
	})
	if c.Matches(func(base oracle.GlobalState) bool {
		return match.TrackedIndexListingMatches(base, prefix, byLedger, mayVanish)
	}) {
		return []oc.Event{oc.Reached(EvFleetListingValidated, map[string]any{"entries": len(entries)})}
	}

	return []oc.Event{oc.Violation(EvBucketListingOutsideModel, map[string]any{"ledgers": len(byLedger), "entries": len(entries),
		"served": match.DescribeIndexOwners(byLedger), "maxTicket": c.MaxTicket})}
}

// lookupClass turns a registry point read's outcome into "the server says
// the entry is there" (M/index_introspect.go classifyIndexLookup); ok=false
// means the outcome was fully handled (coverage or finding in evs).
func lookupClass(c *oc.ReadCtx, rpc, ledger, canonical string, absent bool) (found, ok bool, evs []oc.Event) {
	if c.Outcome.OK {
		if absent {
			return false, false, []oc.Event{oc.Violation(EvLookupOutsideFleet, map[string]any{"rpc": rpc, "ledger": ledger, "index": canonical})}
		}

		return true, true, nil
	}
	if !c.Outcome.IsNotFound() {
		return false, false, []oc.Event{oc.Violation(EvLookupError, errDetails(c, map[string]any{"rpc": rpc, "ledger": ledger, "index": canonical, "absent": absent}))}
	}
	if absent {
		return false, false, []oc.Event{oc.Reached(EvLookupAbsentNotFound, map[string]any{"rpc": rpc, "ledger": ledger})}
	}

	return false, true, nil
}

// presence checks a registry point read against the candidate bases. A
// NotFound is also explained by a base where the ledger is not live (the
// wire NotFound does not say whether the ledger or the index is missing).
func presence(c *oc.ReadCtx, rpc, ledger, canonical string, found bool, reached oc.EventID) []oc.Event {
	if c.Matches(func(base oracle.GlobalState) bool {
		_, live := match.LiveLedgerState(base, ledger)
		if !found && !live {
			return true
		}

		return live && match.IndexPresenceMatches(base, ledger, canonical, found)
	}) {
		return []oc.Event{oc.Reached(reached, map[string]any{"ledger": ledger, "index": canonical, "found": found})}
	}

	return []oc.Event{oc.Violation(EvPresenceOutsideModel, map[string]any{"rpc": rpc, "ledger": ledger, "index": canonical,
		"servedFound": found, "modelIndexes": modelIndexList(c.T.Model(), ledger), "maxTicket": c.MaxTicket})}
}

// ValidateGetIndex validates one GetIndex point read; an empty ledger
// addresses the bucket-scoped slot (M/index_introspect.go runIndexGet,
// runBucketIndexGet).
func ValidateGetIndex(c *oc.ReadCtx) []oc.Event {
	var req servicepb.GetIndexRequest
	if err := unmarshal.Unmarshal(c.Request, &req); err != nil {
		return nil
	}
	ledger, canonical := req.GetLedger(), indexes.Canonical(req.GetId())
	served := &commonpb.Index{}
	if c.Outcome.OK {
		if err := unmarshal.Unmarshal(c.Outcome.Response, served); err != nil {
			return []oc.Event{oc.Violation(EvLookupAnotherEntry, map[string]any{"rpc": "GetIndex", "ledger": ledger, "error": err.Error()})}
		}
	}

	if ledger == "" {
		switch {
		case c.Outcome.OK && served.GetLedger() != "":
			return []oc.Event{oc.Violation(EvBucketLookupLedgerEntry, map[string]any{"index": canonical, "servedLedger": served.GetLedger()})}
		case c.Outcome.OK:
			return nil
		case c.Outcome.IsNotFound():
			return []oc.Event{oc.Reached(EvBucketLookupNotFound, map[string]any{"index": canonical})}
		default:
			return []oc.Event{oc.Violation(EvBucketLookupError, errDetails(c, map[string]any{"index": canonical}))}
		}
	}

	found, ok, evs := lookupClass(c, "GetIndex", ledger, canonical, isAbsentProbe(c, ledger))
	if !ok {
		return evs
	}
	if found && !match.IndexLookupAnswersRequest(served.GetLedger(), served.GetId(), ledger, canonical) {
		return []oc.Event{oc.Violation(EvLookupAnotherEntry, map[string]any{"rpc": "GetIndex", "ledger": ledger, "askedIndex": canonical,
			"servedLedger": served.GetLedger(), "servedIndex": canonicalOrEmpty(served.GetId())})}
	}

	return presence(c, "GetIndex", ledger, canonical, found, EvLookupValidated)
}

func canonicalOrEmpty(id *commonpb.IndexID) string {
	if id == nil {
		return ""
	}

	return indexes.Canonical(id)
}

// ValidateGetIndexStatus validates one index's status view, the
// GetIndexEntryStatus RPC (M/index_introspect.go runIndexEntryStatus): only
// the registry entry it carries is checked.
func ValidateGetIndexStatus(c *oc.ReadCtx) []oc.Event {
	var req servicepb.GetIndexEntryStatusRequest
	if err := unmarshal.Unmarshal(c.Request, &req); err != nil {
		return nil
	}
	ledger, canonical := req.GetLedger(), indexes.Canonical(req.GetId())
	entry := &servicepb.IndexEntry{}
	if c.Outcome.OK {
		if err := unmarshal.Unmarshal(c.Outcome.Response, entry); err != nil {
			return []oc.Event{oc.Violation(EvLookupAnotherEntry, map[string]any{"rpc": "GetIndexEntryStatus", "ledger": ledger, "error": err.Error()})}
		}
	}
	found, ok, evs := lookupClass(c, "GetIndexEntryStatus", ledger, canonical, isAbsentProbe(c, ledger))
	if !ok {
		return evs
	}
	if found && !match.IndexLookupAnswersRequest(entry.GetLedger(), entry.GetIndex().GetId(), ledger, canonical) {
		return []oc.Event{oc.Violation(EvLookupAnotherEntry, map[string]any{"rpc": "GetIndexEntryStatus", "ledger": ledger, "askedIndex": canonical,
			"servedLedger": entry.GetLedger(), "servedIndex": canonicalOrEmpty(entry.GetIndex().GetId())})}
	}

	return presence(c, "GetIndexEntryStatus", ledger, canonical, found, EvStatusValidated)
}
