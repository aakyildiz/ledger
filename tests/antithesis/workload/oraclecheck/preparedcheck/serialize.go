package preparedcheck

import (
	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	oc "github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck"
	"github.com/formancehq/ledger/v3/tests/oracle"
)

// PQRef names one prepared query.
type PQRef struct{ Ledger, Name string }

// pqTouch is what one bulk does to prepared queries.
type pqTouch struct {
	all      map[PQRef]bool // every (ledger, name) a create, update or delete names
	creates  map[PQRef]bool
	modifies map[PQRef]bool // updates and deletes
}

// Touches returns the prepared queries a bulk's requests create, update or
// delete.
func Touches(reqs []*servicepb.Request) map[PQRef]bool { return touchesOf(reqs).all }

func touchesOf(reqs []*servicepb.Request) pqTouch {
	t := pqTouch{all: map[PQRef]bool{}, creates: map[PQRef]bool{}, modifies: map[PQRef]bool{}}
	for _, r := range reqs {
		switch x := r.GetType().(type) {
		case *servicepb.Request_CreatePreparedQuery:
			k := PQRef{x.CreatePreparedQuery.GetLedger(), x.CreatePreparedQuery.GetQuery().GetName()}
			t.all[k], t.creates[k] = true, true
		case *servicepb.Request_UpdatePreparedQuery:
			k := PQRef{x.UpdatePreparedQuery.GetLedger(), x.UpdatePreparedQuery.GetName()}
			t.all[k], t.modifies[k] = true, true
		case *servicepb.Request_DeletePreparedQuery:
			k := PQRef{x.DeletePreparedQuery.GetLedger(), x.DeletePreparedQuery.GetName()}
			t.all[k], t.modifies[k] = true, true
		}
	}

	return t
}

// BeginSerialize is the begin hook prepared-serialize (DESIGN §4.5): a bulk
// containing any prepared-query order on (ledger, name) is refused with
// `409 serialized` while another in-flight or pending bulk touches the same
// (ledger, name). The stored target therefore stays fixed for the life of an
// UpdatePreparedQuery ticket, so its FSM-stage FILTER_COMPILATION_ERROR (a
// domain.UncommittedFailure, never frozen or audited) repeats on every
// attempt and is final. A bulk that updates or deletes a name it also creates
// is refused too, which keeps that argument about one order per name.
func BeginSerialize(t *oc.Tracker, rec oc.WriteRecord) (map[string]any, *oc.Refusal) {
	mine := touchesOf(rec.Requests)
	if len(mine.all) == 0 {
		return nil, nil
	}
	for _, k := range sortedRefs(mine.modifies) {
		if mine.creates[k] {
			return nil, oc.Serialized("a prepared query is created and modified in one bulk",
				map[string]any{"ledger": k.Ledger, "name": k.Name})
		}
	}
	others := append(t.InflightBulks(), t.PendingBulks()...)
	for _, o := range others {
		if o.Wid == rec.Wid {
			continue
		}
		theirs := Touches(o.Requests)
		for _, k := range sortedRefs(mine.all) {
			if theirs[k] {
				return nil, oc.Serialized("an outstanding bulk touches the same prepared query",
					map[string]any{"ledger": k.Ledger, "name": k.Name, "outstanding": o.Wid, "ticket": o.Ticket})
			}
		}
	}

	return nil, nil
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

// FailureCoverage is the failure hook prepared-failures: coverage of the two
// save-time rejections of a prepared-query filter, after the failure was
// explained by a candidate.
//   - "malformed prepared-query filter refused at save" (M/validate.go:563):
//     any explained FILTER_COMPILATION_ERROR of a bulk that saves a filter
//     (a create at admission, an update at admission or in the FSM);
//   - "an update with a filter invalid for the stored target is refused":
//     the FSM-stage (uncommitted) FILTER_COMPILATION_ERROR of an update.
func FailureCoverage(_ *oc.Tracker, rec oc.WriteRecord, res oracle.ApplyResult, reason string) []oc.Event {
	if reason != domain.ErrReasonFilterCompilation {
		return nil
	}
	creates, updates := false, false
	for _, r := range rec.Requests {
		switch r.GetType().(type) {
		case *servicepb.Request_CreatePreparedQuery:
			creates = true
		case *servicepb.Request_UpdatePreparedQuery:
			updates = true
		}
	}
	if !creates && !updates {
		return nil
	}
	admission := oc.IsAdmissionStage(res)
	d := map[string]any{"wid": rec.Wid, "admission": admission}
	evs := []oc.Event{oc.Reached(EvMalformedFilterRefused, d)}
	if updates && !admission {
		evs = append(evs, oc.Reached(EvUpdateInvalidForStoredTarget, map[string]any{"wid": rec.Wid}))
	}

	return evs
}
