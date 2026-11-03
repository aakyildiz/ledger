package preparedcheck

import (
	"sort"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	oc "github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck"
	"github.com/formancehq/ledger/v3/tests/oracle"
)

// PQRef names one prepared query.
type PQRef struct{ Ledger, Name string }

// pqTouch is what one bulk does to prepared queries and ledger lifecycles.
type pqTouch struct {
	all           map[PQRef]bool // every (ledger, name) a create, update or delete names
	creates       map[PQRef]bool
	modifies      map[PQRef]bool // updates and deletes
	pqDeletes     map[PQRef]bool
	updateLedgers map[string]bool // ledgers of the bulk's updates
	ledgerDeletes map[string]bool // ledgers the bulk deletes
	updates       bool
	nonPQ         bool // the bulk carries an order that is not a prepared-query order
}

// Touches returns the prepared queries a bulk's requests create, update or
// delete.
func Touches(reqs []*servicepb.Request) map[PQRef]bool { return touchesOf(reqs).all }

func touchesOf(reqs []*servicepb.Request) pqTouch {
	t := pqTouch{
		all: map[PQRef]bool{}, creates: map[PQRef]bool{}, modifies: map[PQRef]bool{},
		pqDeletes: map[PQRef]bool{}, updateLedgers: map[string]bool{}, ledgerDeletes: map[string]bool{},
	}
	for _, r := range reqs {
		switch x := r.GetType().(type) {
		case *servicepb.Request_CreatePreparedQuery:
			k := PQRef{x.CreatePreparedQuery.GetLedger(), x.CreatePreparedQuery.GetQuery().GetName()}
			t.all[k], t.creates[k] = true, true
		case *servicepb.Request_UpdatePreparedQuery:
			k := PQRef{x.UpdatePreparedQuery.GetLedger(), x.UpdatePreparedQuery.GetName()}
			t.all[k], t.modifies[k] = true, true
			t.updateLedgers[k.Ledger], t.updates = true, true
		case *servicepb.Request_DeletePreparedQuery:
			k := PQRef{x.DeletePreparedQuery.GetLedger(), x.DeletePreparedQuery.GetName()}
			t.all[k], t.modifies[k], t.pqDeletes[k] = true, true, true
		case *servicepb.Request_DeleteLedger:
			t.ledgerDeletes[x.DeleteLedger.GetName()], t.nonPQ = true, true
		default:
			t.nonPQ = true
		}
	}

	return t
}

// blockTable remembers the prepared queries of a bulk (in practice a single
// UpdatePreparedQuery) whose ticket ended with a definitive-unfrozen
// (FSM-stage) failure: another attempt of that key can still reach the server
// after the ticket ended (a delayed request gets its whole relative deadline
// when it arrives) and is evaluated again. A prepared-query-only bulk's
// outcome depends only on whether each named query exists, its stored target
// and whether its ledger is live; an update changes none of these, so only a
// create or delete of a named query, or a delete of its ledger, can change
// the late attempt's outcome. Those are refused.
// The entry holds until the next quiet period begins: faults are stopped and
// every client answered a barrier before /quiet/begin, so no attempt is still
// on its way.
type blockTable struct {
	since map[PQRef]uint64 // QuietBegins() when the ticket ended
}

const blockExtKey = "prepared-blocks"

func blocks(t *oc.Tracker) *blockTable {
	return t.Ext(blockExtKey, func() any { return &blockTable{since: map[PQRef]uint64{}} }).(*blockTable)
}

// live returns the blocked prepared queries, dropping the expired ones.
func (b *blockTable) live(t *oc.Tracker) []PQRef {
	q := t.QuietBegins()
	for k, at := range b.since {
		if q > at {
			delete(b.since, k)
		}
	}
	out := make(map[PQRef]bool, len(b.since))
	for k := range b.since {
		out[k] = true
	}

	return sortedRefs(out)
}

// Blocked reports the prepared queries a create or delete is refused on
// (tests and diagnostics).
func Blocked(t *oc.Tracker) []PQRef { return blocks(t).live(t) }

// BeginSerialize is the begin hook prepared-serialize (DESIGN §4.5). The
// FSM-stage FILTER_COMPILATION_ERROR of an UpdatePreparedQuery is a
// domain.UncommittedFailure: never frozen or audited, re-evaluated on every
// attempt. Its outcome is a function of the bulk's prepared queries and the
// liveness of their ledgers only if the bulk carries nothing else, and those
// must stay fixed for as long as an attempt can run. So the hook refuses
// (`409 serialized`)
//   - a bulk that updates a prepared query and carries any order that is not
//     a prepared-query order (its outcome would depend on balances, ...);
//   - a bulk that updates or deletes a name it also creates;
//   - a bulk naming a (ledger, name) another in-flight or pending bulk names;
//   - a bulk updating a prepared query of a ledger another outstanding bulk
//     deletes, and a bulk deleting a ledger an outstanding bulk updates a
//     prepared query of;
//   - after an update ticket ended with an FSM-stage failure, until the next
//     quiet period begins: a create or delete of a prepared query it names,
//     or a delete of its ledger (blockTable).
func BeginSerialize(t *oc.Tracker, rec oc.WriteRecord) (map[string]any, *oc.Refusal) {
	mine := touchesOf(rec.Requests)
	if len(mine.all) == 0 && len(mine.ledgerDeletes) == 0 {
		return nil, nil
	}
	if mine.updates && mine.nonPQ {
		return nil, oc.Serialized("a prepared-query update shares a bulk with other orders", map[string]any{"wid": rec.Wid})
	}
	for _, k := range sortedRefs(mine.modifies) {
		if mine.creates[k] {
			return nil, oc.Serialized("a prepared query is created and modified in one bulk",
				map[string]any{"ledger": k.Ledger, "name": k.Name})
		}
	}
	for _, k := range blocks(t).live(t) {
		if mine.pqDeletes[k] || mine.creates[k] || mine.ledgerDeletes[k.Ledger] {
			return nil, oc.Serialized("a prepared-query update that failed uncommitted may still run",
				map[string]any{"ledger": k.Ledger, "name": k.Name})
		}
	}
	inflight, pending := t.InflightBulks(), t.PendingBulks()
	others := make([]oc.WriteRecord, 0, len(inflight)+len(pending))
	others = append(append(others, inflight...), pending...)
	for _, o := range others {
		if o.Wid == rec.Wid {
			continue
		}
		theirs := touchesOf(o.Requests)
		for _, k := range sortedRefs(mine.all) {
			if theirs.all[k] {
				return nil, oc.Serialized("an outstanding bulk touches the same prepared query",
					map[string]any{"ledger": k.Ledger, "name": k.Name, "outstanding": o.Wid, "ticket": o.Ticket})
			}
		}
		for _, l := range sortedKeys(mine.updateLedgers) {
			if theirs.ledgerDeletes[l] {
				return nil, oc.Serialized("an outstanding bulk deletes the ledger of a prepared-query update",
					map[string]any{"ledger": l, "outstanding": o.Wid, "ticket": o.Ticket})
			}
		}
		for _, l := range sortedKeys(mine.ledgerDeletes) {
			if theirs.updateLedgers[l] {
				return nil, oc.Serialized("an outstanding bulk updates a prepared query of the deleted ledger",
					map[string]any{"ledger": l, "outstanding": o.Wid, "ticket": o.Ticket})
			}
		}
	}

	return nil, nil
}

func sortedKeys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)

	return out
}

func sortedRefs(m map[PQRef]bool) []PQRef {
	out := make([]PQRef, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	for i := 1; i < len(out); i++ {
		for j := i; j > 0 && less(out[j], out[j-1]); j-- {
			out[j], out[j-1] = out[j-1], out[j]
		}
	}

	return out
}

func less(a, b PQRef) bool {
	if a.Ledger != b.Ledger {
		return a.Ledger < b.Ledger
	}

	return a.Name < b.Name
}

// FailureCoverage is the failure hook prepared-failures, run after the
// failure was explained by a candidate:
//   - "malformed prepared-query filter refused at save" (M/validate.go:563):
//     an admission-stage FILTER_COMPILATION_ERROR of a bulk that saves a
//     filter (create or update);
//   - "an update with a filter invalid for the stored target is refused":
//     the FSM-stage (uncommitted) FILTER_COMPILATION_ERROR of an update;
//   - an FSM-stage failure with an uncommitted reason blocks creates and
//     deletes of the bulk's prepared queries, and deletes of their ledgers,
//     until the next quiet period (blockTable).
func FailureCoverage(t *oc.Tracker, rec oc.WriteRecord, res oracle.ApplyResult, reason string) []oc.Event {
	admission := oc.IsAdmissionStage(res)
	mine := touchesOf(rec.Requests)
	if !admission && oracle.UncommittedReasons[reason] {
		b, q := blocks(t), t.QuietBegins()
		for k := range mine.all {
			b.since[k] = q
		}
	}
	if reason != domain.ErrReasonFilterCompilation || (len(mine.creates) == 0 && !mine.updates) {
		return nil
	}
	if admission {
		return []oc.Event{oc.Reached(EvMalformedFilterRefused, map[string]any{"wid": rec.Wid})}
	}
	if mine.updates {
		return []oc.Event{oc.Reached(EvUpdateInvalidForStoredTarget, map[string]any{"wid": rec.Wid})}
	}

	return nil
}
