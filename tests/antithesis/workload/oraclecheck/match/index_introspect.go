package match

import (
	"fmt"
	"slices"
	"sort"
	"strings"

	"github.com/formancehq/ledger/v3/internal/domain/indexes"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/tests/oracle"
)

// IndexSetEqual reports whether a base's declared index set is exactly served.
func IndexSetEqual(ls oracle.LedgerState, served map[string]bool) bool {
	declared := 0

	for canonical := range ls.Indexes().All() {
		if !served[canonical] {
			return false
		}

		declared++
	}

	return declared == len(served)
}

// DescribeIndexOwners renders a served bucket-wide listing as sorted
// "ledger=count" entries for a finding.
func DescribeIndexOwners(byLedger map[string]map[string]bool) string {
	parts := make([]string, 0, len(byLedger))
	for owner, set := range byLedger {
		parts = append(parts, fmt.Sprintf("%s=%d", owner, len(set)))
	}
	slices.Sort(parts)

	return "[" + strings.Join(parts, ",") + "]"
}

// JoinSortedKeys renders a canonical set in a stable order.
func JoinSortedKeys(set map[string]bool) string {
	out := make([]string, 0, len(set))
	for k := range set {
		out = append(out, k)
	}

	sort.Strings(out)

	return strings.Join(out, ",")
}

// IndexListingMatches reports whether base explains a fleet-wide listing. The
// handler streams the registry from an iterator opened at the start but checks
// each owning ledger's liveness later, against the live store: a ledger live in
// base may have been deleted by the time its rows were checked, so a ledger in
// mayVanish may contribute nothing instead of base's whole set.
//
// TODO: this tolerates a server torn read. Once ListIndexes reads the registry
// and ledger liveness from one snapshot, drop mayVanish so a listing must match
// a single base.
func IndexListingMatches(base oracle.GlobalState, byLedger map[string]map[string]bool, mayVanish map[string]bool) bool {
	live := base.LiveLedgers()
	for _, ledger := range live {
		if len(byLedger[ledger]) == 0 && mayVanish[ledger] {
			continue
		}

		if !IndexSetEqual(base.Ledger(ledger), byLedger[ledger]) {
			return false
		}
	}

	// A ledger the base does not hold live must contribute nothing.
	for owner := range byLedger {
		if owner != "" && !slices.Contains(live, owner) {
			return false
		}
	}

	return true
}

// TrackedIndexListingMatches is IndexListingMatches restricted to the tracked
// partition (DESIGN §3.2: index SCOPE_ALL listings are restricted to tracked
// ledgers). Only ledgers with the tracked prefix are compared with base; served
// entries of foreign ledgers and of the bucket-scoped slot ("") are ignored.
func TrackedIndexListingMatches(base oracle.GlobalState, prefix string, byLedger map[string]map[string]bool, mayVanish map[string]bool) bool {
	tracked := map[string]map[string]bool{}
	for owner, set := range byLedger {
		if strings.HasPrefix(owner, prefix) {
			tracked[owner] = set
		}
	}

	live := map[string]bool{}
	for _, ledger := range base.LiveLedgers() {
		if !strings.HasPrefix(ledger, prefix) {
			continue
		}

		live[ledger] = true

		if len(tracked[ledger]) == 0 && mayVanish[ledger] {
			continue
		}

		if !IndexSetEqual(base.Ledger(ledger), tracked[ledger]) {
			return false
		}
	}

	for owner := range tracked {
		if !live[owner] {
			return false
		}
	}

	return true
}

// IndexListingScopeViolation checks a ListIndexes page against its scope and
// partitions it by owning ledger ("" is the bucket-scoped slot). SCOPE_LEDGER
// entries must all belong to ledger; SCOPE_BUCKET entries must be ledger-less;
// every entry carries an id and no (ledger, canonical) pair repeats. why is ""
// when the page respects its scope.
func IndexListingScopeViolation(entries []*commonpb.Index, scope servicepb.ListIndexesRequest_Scope, ledger string) (byLedger map[string]map[string]bool, why string) {
	byLedger = map[string]map[string]bool{}

	for _, idx := range entries {
		if idx.GetId() == nil {
			return nil, fmt.Sprintf("entry of ledger %q has no id", idx.GetLedger())
		}

		owner := idx.GetLedger()
		canonical := indexes.Canonical(idx.GetId())

		switch scope {
		case servicepb.ListIndexesRequest_SCOPE_LEDGER:
			if owner != ledger {
				return nil, fmt.Sprintf("SCOPE_LEDGER %q served an entry of ledger %q", ledger, owner)
			}
		case servicepb.ListIndexesRequest_SCOPE_BUCKET:
			if owner != "" {
				return nil, fmt.Sprintf("SCOPE_BUCKET served ledger %q's entry %s", owner, canonical)
			}
		}

		if byLedger[owner] == nil {
			byLedger[owner] = map[string]bool{}
		}

		if byLedger[owner][canonical] {
			return nil, fmt.Sprintf("entry %s of ledger %q repeated", canonical, owner)
		}

		byLedger[owner][canonical] = true
	}

	return byLedger, ""
}

// IndexPresenceMatches is the per-base verdict of a registry point read
// (GetIndex, GetIndexEntryStatus): both directions are findings — an entry
// served for an index base does not declare, or a NotFound for one it does.
//
// Tracker-bound part (candidate walk, NotFound classification, reporting):
// task 07.
func IndexPresenceMatches(base oracle.GlobalState, ledger, canonical string, found bool) bool {
	exists, _ := base.Ledger(ledger).IndexState(canonical)

	return exists == found
}

// IndexLookupAnswersRequest reports whether a served registry entry is the one
// asked for (same ledger, same canonical id).
func IndexLookupAnswersRequest(servedLedger string, servedID *commonpb.IndexID, ledger, canonical string) bool {
	return servedLedger == ledger && servedID != nil && indexes.Canonical(servedID) == canonical
}

// LedgersDeletedInSomeBase names the ledgers live in some candidate base and
// not in another. walk visits every candidate base (stop when visit returns
// true), e.g. a closure over oraclecheck.Tracker.WalkCandidates.
func LedgersDeletedInSomeBase(walk func(visit func(oracle.GlobalState) bool)) map[string]bool {
	bases := 0
	liveIn := map[string]int{}
	walk(func(base oracle.GlobalState) bool {
		bases++
		for _, ledger := range base.LiveLedgers() {
			liveIn[ledger]++
		}

		return false
	})

	out := map[string]bool{}
	for ledger, n := range liveIn {
		if n < bases {
			out[ledger] = true
		}
	}

	return out
}
