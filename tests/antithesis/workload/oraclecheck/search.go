package oraclecheck

import (
	"slices"

	"github.com/formancehq/ledger/v3/tests/oracle"
)

// Lifted from M/search.go. The one change: in-flight bulks are folded in
// ticket order (M/ ranged over a Go map), so the visit order — and therefore
// which candidate explains an observation first — is deterministic.

// maxCandidateInflight bounds the in-flight bulks one search may fold; the
// search is exponential in them. The service refuses a 9th write (`busy`).
const maxCandidateInflight = 8

// checkpointCreationMatches checks a served predicted checkpoint against the
// state immediately before the candidate create transition that assigned id.
func (t *Tracker) checkpointCreationMatches(maxTicket, id uint64, match func(oracle.GlobalState) bool) bool {
	if snapshot, ok := t.checkpoints[id]; ok && match(snapshot.state) {
		return true
	}
	if snapshot, ok := t.deletedCheckpointSnapshots[id]; ok && match(snapshot.state) {
		return true
	}

	matched := false
	t.walkCandidateStates(maxTicket, func(oracle.GlobalState) bool { return matched }, func(before oracle.GlobalState, bulk oracle.Bulk, result oracle.ApplyResult) bool {
		for i, request := range bulk.Requests {
			if request.GetCreateQueryCheckpoint() != nil && i < len(result.Orders) && result.Orders[i].CheckpointID == id {
				matched = match(before)

				return matched
			}
		}

		return false
	})

	return matched
}

// CheckpointCreationMatches exposes checkpointCreationMatches (task 09).
func (t *Tracker) CheckpointCreationMatches(maxTicket, id uint64, match func(oracle.GlobalState) bool) bool {
	return t.checkpointCreationMatches(maxTicket, id, match)
}

// walkCandidateStates enumerates the distinct committed states the server
// could be in relative to an observation made at maxTicket: the drained state
// folded with an ordered prefix of pending (known commit order) and any
// ordered subset of the in-flight bulks dispatched no later than maxTicket.
// visit returning true stops; transition (may be nil) observes every
// successful fold and stops the walk by returning true.
func (t *Tracker) walkCandidateStates(maxTicket uint64, visit func(oracle.GlobalState) bool, transition func(oracle.GlobalState, oracle.Bulk, oracle.ApplyResult) bool) {
	type candidateBulk struct {
		ticket uint64
		bulk   oracle.Bulk
	}

	pending := make([]candidateBulk, 0, len(t.pending))
	for _, pe := range t.pending {
		// pending is minSeq-ordered, so an entry dispatched after the
		// observation committed after it — and so did every later entry.
		if pe.ticket > maxTicket {
			break
		}
		pending = append(pending, candidateBulk{ticket: pe.ticket, bulk: t.records[pe.wid].Bulk})
	}

	tickets := make([]uint64, 0, len(t.inflight))
	for tk := range t.inflight {
		if tk <= maxTicket {
			tickets = append(tickets, tk)
		}
	}
	slices.Sort(tickets)
	inflight := make([]candidateBulk, 0, len(tickets))
	for _, tk := range tickets {
		inflight = append(inflight, candidateBulk{ticket: tk, bulk: t.records[t.inflight[tk]].Bulk})
	}
	if len(inflight) > maxCandidateInflight {
		panic("candidate search exceeded its bounded in-flight set")
	}

	allRem := make([]byte, (len(inflight)+7)/8)
	for idx := range inflight {
		allRem[idx/8] |= 1 << (idx % 8)
	}

	type dedupKey struct {
		state oracle.Digest
		pIdx  int
		rem   string
	}
	seen := map[dedupKey]bool{}

	var rec func(base oracle.GlobalState, pIdx int, rem []byte) bool

	rec = func(base oracle.GlobalState, pIdx int, rem []byte) bool {
		k := dedupKey{state: base.Fingerprint(), pIdx: pIdx, rem: string(rem)}
		if seen[k] {
			return false
		}
		seen[k] = true

		if visit(base) {
			return true
		}

		// Advance the pending prefix by one, in minSeq order.
		if pIdx < len(pending) {
			candidate := pending[pIdx]
			if res := base.Apply(candidate.bulk); res.OK {
				if transition != nil && transition(base, candidate.bulk, res) {
					return true
				}
				if rec(res.State, pIdx+1, rem) {
					return true
				}
			}
		}

		// Fold in any one of the remaining in-flight bulks (unknown position).
		for idx := range inflight {
			byteIdx, mask := idx/8, byte(1<<(idx%8))
			if rem[byteIdx]&mask == 0 {
				continue
			}

			candidate := inflight[idx]

			res := base.Apply(candidate.bulk)
			if !res.OK {
				continue
			}
			if transition != nil && transition(base, candidate.bulk, res) {
				return true
			}

			rem[byteIdx] &^= mask
			matched := rec(res.State, pIdx, rem)
			rem[byteIdx] |= mask
			if matched {
				return true
			}
		}

		return false
	}

	rec(t.modelState, 0, allRem)
}
