package oraclecheck

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/tests/oracle"
	"github.com/formancehq/ledger/v3/tests/oracle/oracletest"
)

// Port of M/search_test.go onto the Tracker. Ledger "L" with an empty tracked
// prefix, as the oracletest builders use.

func searchTracker(maxInflight int) *Tracker {
	return NewTracker(Config{TrackedPrefix: "", MaxInflight: maxInflight})
}

// addInflight registers a bulk as in flight under exactly ticket.
func addInflight(t *Tracker, ticket uint64, reqs ...*servicepb.Request) {
	wid := fmt.Sprintf("w%d", ticket)
	t.records[wid] = &WriteRecord{Wid: wid, Key: fmt.Sprintf("k%d", ticket), Ticket: ticket, Requests: reqs, Bulk: oracle.Bulk{Requests: reqs}, Status: WInflight}
	t.ticketWid[ticket] = wid
	t.inflight[ticket] = wid
	t.ticketSeq = max(t.ticketSeq, ticket)
}

// addPending appends a pending success (already minSeq-ordered).
func addPending(t *Tracker, ticket, minSeq uint64, reqs ...*servicepb.Request) {
	wid := fmt.Sprintf("w%d", ticket)
	t.records[wid] = &WriteRecord{Wid: wid, Ticket: ticket, Requests: reqs, Bulk: oracle.Bulk{Requests: reqs}, Status: WPending}
	t.ticketWid[ticket] = wid
	t.pending = append(t.pending, &pendingEntry{minSeq: minSeq, ticket: ticket, wid: wid})
}

func collectBases(t *Tracker, maxTicket uint64) []oracle.GlobalState {
	var out []oracle.GlobalState
	t.walkCandidateStates(maxTicket, func(b oracle.GlobalState) bool {
		out = append(out, b)

		return false
	}, nil)

	return out
}

func TestCandidateBases_BoundsInflightByMaxTicket(t *testing.T) {
	t.Parallel()

	tr := searchTracker(0)
	addInflight(tr, 5, oracletest.AddTypeReq("T"))

	require.Len(t, collectBases(tr, 4), 1)
	require.Len(t, collectBases(tr, 5), 2)
}

func TestCandidateBases_TruncatesPendingByMaxTicket(t *testing.T) {
	t.Parallel()

	tr := searchTracker(0)
	addPending(tr, 2, 1, oracletest.AddTypeReq("A"))
	addPending(tr, 6, 2, oracletest.AddTypeReq("B"))

	require.Len(t, collectBases(tr, 4), 2)
}

func TestCandidateBases_FoldsInflight(t *testing.T) {
	t.Parallel()

	tr := searchTracker(0)
	addInflight(tr, 1, oracletest.AddTypeReq("T"))
	require.Len(t, collectBases(tr, ^uint64(0)), 2)
}

func TestCandidateBases_PinsPendingOrder(t *testing.T) {
	t.Parallel()

	tr := searchTracker(0)
	addPending(tr, 1, 1, oracletest.AddTypeReqP("a", commonpb.AccountTypePersistence_ACCOUNT_TYPE_EPHEMERAL))
	addPending(tr, 2, 3, oracletest.TxReq("world", "a:1", "USD", 5), oracletest.TxReq("a:1", "world", "USD", 5))

	for _, b := range collectBases(tr, ^uint64(0)) {
		_, present := b.Ledger("L").Volumes().Get(oracle.VolumeKey{Address: "a:1", Asset: "USD"})
		require.False(t, present, "a:1 must be absent in every candidate base")
	}
}

func TestModelFailure_NoSelfExplanation(t *testing.T) {
	t.Parallel()

	explained := func(tr *Tracker) bool {
		for _, b := range collectBases(tr, ^uint64(0)) {
			res := b.Apply(oracle.Bulk{Requests: []*servicepb.Request{oracletest.AddTypeReq("T")}})
			if !res.OK && res.Reason == domain.ErrReasonAccountTypeAlreadyExists {
				return true
			}
		}

		return false
	}

	require.False(t, explained(searchTracker(0)))
	withAdd := searchTracker(0)
	addInflight(withAdd, 1, oracletest.AddTypeReq("T"))
	require.True(t, explained(withAdd))
}

func TestCandidateBasesRejectsIntractableInflightSet(t *testing.T) {
	t.Parallel()

	tr := searchTracker(maxCandidateInflight + 1)
	for ticket := uint64(1); ticket <= maxCandidateInflight+1; ticket++ {
		addInflight(tr, ticket, oracletest.AddTypeReq(string(rune('A'+ticket))))
	}

	require.PanicsWithValue(t, "candidate search exceeded its bounded in-flight set", func() {
		collectBases(tr, maxCandidateInflight+1)
	})
}

func TestCandidateBasesCompletesAtInflightBound(t *testing.T) {
	t.Parallel()

	tr := searchTracker(0)
	for ticket := uint64(1); ticket <= maxCandidateInflight; ticket++ {
		addInflight(tr, ticket, oracletest.AddTypeReq(string(rune('A'+ticket))))
	}
	require.Len(t, collectBases(tr, maxCandidateInflight), 109601)
}

// The same in-flight set registered in different map insertion orders visits
// the same sequence of bases, and a failure is explained by the same
// candidate.
func TestCandidateBases_DeterministicVisitOrder(t *testing.T) {
	t.Parallel()

	bulks := map[uint64][]*servicepb.Request{
		1: {oracletest.TxReq("world", "a:1", "USD", 5)},
		2: {oracletest.AddTypeReq("a")},
		3: {oracletest.TxReq("a:1", "b:1", "USD", 3)},
		4: {oracletest.AddTypeReq("b")},
		5: {oracletest.TxReq("world", "b:2", "USD", 7)},
	}
	orders := [][]uint64{{1, 2, 3, 4, 5}, {5, 4, 3, 2, 1}, {3, 1, 5, 2, 4}, {2, 5, 1, 4, 3}}

	var first []oracle.Digest
	for i, order := range orders {
		for rep := range 4 {
			tr := searchTracker(0)
			for _, tk := range order {
				addInflight(tr, tk, bulks[tk]...)
			}
			var seq []oracle.Digest
			for _, b := range collectBases(tr, ^uint64(0)) {
				seq = append(seq, b.Fingerprint())
			}
			if i == 0 && rep == 0 {
				first = seq

				continue
			}
			require.Equal(t, first, seq, "order %v rep %d", order, rep)
		}
	}
}
