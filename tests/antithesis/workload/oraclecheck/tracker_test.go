package oraclecheck_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	oc "github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck/testutil"
	"github.com/formancehq/ledger/v3/tests/oracle"
)

// newLedgerSession returns a session with ledger l1 created and drained.
func newLedgerSession(t *testing.T) *testutil.ScriptedSession {
	t.Helper()
	s := testutil.NewSession("")
	require.Empty(t, testutil.Findings(s.Write(false, testutil.CreateLedgerReq(s.Ledger(1)))))
	require.EqualValues(t, 1, s.T.Version())

	return s
}

func tx(s *testutil.ScriptedSession, ref string, amount uint64) *servicepb.Request {
	return testutil.TxReq(s.Ledger(1), ref, "world", "t-1:1", "COIN", amount)
}

func TestDrain_HeadWaitsForEarlierWrite(t *testing.T) {
	t.Parallel()
	s := newLedgerSession(t)
	a := s.Begin(false, tx(s, "", 1))
	b := s.Begin(false, tx(s, "", 2))
	status, evs := s.End(b)
	require.Equal(t, "pending", status)
	require.Empty(t, testutil.Findings(evs))
	require.EqualValues(t, 1, s.T.Version(), "b must wait for the earlier-ticketed a")
	_, evs = s.End(a)
	require.Empty(t, testutil.Findings(evs))
	require.EqualValues(t, 3, s.T.Version())
	_, _, pending := s.T.Outstanding()
	require.Zero(t, pending)
}

func TestDrain_HeadWaitsForEarlierRead(t *testing.T) {
	t.Parallel()
	s := newLedgerSession(t)
	r := s.Read("Smoke")
	b := s.Begin(false, tx(s, "", 2))
	s.End(b)
	require.EqualValues(t, 1, s.T.Version(), "b must wait for the earlier-ticketed read")
	require.Empty(t, testutil.Findings(s.T.FinishRead(r)))
	require.EqualValues(t, 2, s.T.Version())
}

func TestDrain_DrainsUnderLaterRead(t *testing.T) {
	t.Parallel()
	s := newLedgerSession(t)
	a := s.Begin(false, tx(s, "", 1))
	b := s.Begin(false, tx(s, "", 2))
	outB := s.Execute(b) // b commits first on the server
	outA := s.Execute(a)
	_, _, ref := s.T.EndWrite(b, outB)
	require.Nil(t, ref)
	r := s.Read("Smoke")
	_, evs, ref := s.T.EndWrite(a, outA)
	require.Nil(t, ref)
	require.Empty(t, testutil.Findings(evs))
	// b (observed before r) drained; a (observed after r began) waits.
	require.EqualValues(t, 2, s.T.Version())
	require.True(t, s.T.ReadOpen(r))
	s.T.FinishRead(r)
	require.EqualValues(t, 3, s.T.Version())
}

func TestDrain_AbandonReleasesGate(t *testing.T) {
	t.Parallel()
	s := newLedgerSession(t)
	a := s.Begin(false, tx(s, "", 1))
	b := s.Begin(false, tx(s, "", 2))
	s.End(b)
	require.EqualValues(t, 1, s.T.Version())
	_, ref := s.T.AbandonWrite(a, 1)
	require.NotNil(t, ref)
	require.Equal(t, oc.CodeProtocol, ref.Code)
	evs, ref := s.T.AbandonWrite(a, 0)
	require.Nil(t, ref)
	require.Empty(t, testutil.Findings(evs))
	require.EqualValues(t, 2, s.T.Version())
}

func TestEndWrite_UnknownTicket(t *testing.T) {
	t.Parallel()
	s := newLedgerSession(t)
	_, _, ref := s.T.EndWrite(999, testutil.Fail("FailedPrecondition", "INSUFFICIENT_FUNDS"))
	require.NotNil(t, ref)
	require.Equal(t, 404, ref.HTTP)
	require.Equal(t, oc.CodeUnknownTicket, ref.Code)
}

func TestEndWrite_NonDefinitive(t *testing.T) {
	t.Parallel()
	s := newLedgerSession(t)
	a := s.Begin(false, tx(s, "", 1))
	_, evs, ref := s.T.EndWrite(a, testutil.Fail("Unavailable", "STALE_PROPOSAL"))
	require.NotNil(t, ref)
	require.Equal(t, oc.CodeProtocol, ref.Code)
	require.Equal(t, []oc.EventID{oc.EvProtocolNonDefinitive}, testutil.IDs(evs))
	in, _, _ := s.T.Outstanding()
	require.Equal(t, 1, in, "a refused end leaves the ticket in flight")
}

func TestBeginWrite_BusyAndBacklog(t *testing.T) {
	t.Parallel()
	s := testutil.NewSession("")
	s.T = oc.NewTracker(oc.Config{TrackedPrefix: s.Prefix, PendingCap: 1})
	s.Write(false, testutil.CreateLedgerReq(s.Ledger(1)))

	hold := s.Begin(false, tx(s, "", 1))
	for range 2 {
		s.End(s.Begin(false, tx(s, "", 1)))
	}
	_, _, pending := s.T.Outstanding()
	require.Equal(t, 2, pending)
	_, _, _, ref := s.T.BeginWrite(s.Record(false, tx(s, "", 1)))
	require.NotNil(t, ref)
	require.Equal(t, oc.CodeBacklog, ref.Code)
	s.End(hold)

	for range 8 {
		s.Begin(false, tx(s, "", 1))
	}
	_, _, _, ref = s.T.BeginWrite(s.Record(false, tx(s, "", 1)))
	require.NotNil(t, ref)
	require.Equal(t, oc.CodeBusy, ref.Code)
	require.Equal(t, 409, ref.HTTP)
}

func TestBeginWrite_Partition(t *testing.T) {
	t.Parallel()
	s := newLedgerSession(t)

	rec := s.Record(false, testutil.TxReq("r-ns-x-1", "", "world", "a:1", "COIN", 1))
	_, _, _, ref := s.T.BeginWrite(rec)
	require.Equal(t, oc.CodeUnmodeled, ref.Code)

	rec = s.Record(false, tx(s, "", 1))
	rec.Key = "r-1-k1"
	_, _, _, ref = s.T.BeginWrite(rec)
	require.Equal(t, oc.CodeUnmodeled, ref.Code)

	reqs := make([]*servicepb.Request, 65)
	for i := range reqs {
		reqs[i] = tx(s, "", 1)
	}
	_, _, _, ref = s.T.BeginWrite(s.Record(false, reqs...))
	require.Equal(t, oc.CodeUnmodeled, ref.Code)

	_, _, _, ref = s.T.BeginWrite(s.Record(false, &servicepb.Request{Type: &servicepb.Request_SaveNumscript{SaveNumscript: &servicepb.SaveNumscriptRequest{}}}))
	require.Equal(t, oc.CodeUnmodeled, ref.Code)
	require.Equal(t, "saveNumscript", ref.Details["arm"])

	// Cluster-scoped tracked orders are allowed.
	_, _, _, ref = s.T.BeginWrite(s.Record(false, testutil.CreateCheckpointReq()))
	require.Nil(t, ref)
}

func TestBeginWrite_ConflictRules(t *testing.T) {
	t.Parallel()
	s := newLedgerSession(t)
	orig := s.Record(true, tx(s, "", 1))
	_, _, _, ref := s.T.BeginWrite(orig)
	require.Nil(t, ref)

	conflict := s.Record(true, tx(s, "", 2))
	conflict.Role, conflict.Of, conflict.Key = oc.RoleConflict, orig.Wid, orig.Key
	untracked := conflict
	untracked.TrackKey = false
	_, _, _, ref = s.T.BeginWrite(untracked)
	require.Equal(t, oc.CodeProtocol, ref.Code)

	equal := conflict
	equal.Requests = orig.Requests
	_, _, _, ref = s.T.BeginWrite(equal)
	require.Equal(t, oc.CodeProtocol, ref.Code)

	_, _, _, ref = s.T.BeginWrite(conflict)
	require.Nil(t, ref)
}

func TestSettleHooksRunAfterEveryKindOfCall(t *testing.T) {
	calls := 0
	oc.RegisterSettleHook("zz-test-settle", func(*oc.Tracker) []oc.Event {
		calls++

		return nil
	})
	t.Cleanup(func() { oc.TestingUnregister("zz-test-settle") })

	s := newLedgerSession(t) // one EndWrite + one drain step
	require.Equal(t, 2, calls)

	a := s.Begin(false, tx(s, "", 1))
	b := s.Begin(false, tx(s, "", 2))
	calls = 0
	s.End(b) // EndWrite (no drain: a blocks)
	require.Equal(t, 1, calls)
	calls = 0
	_, _ = s.T.AbandonWrite(a, 0) // drain step + abandon
	require.Equal(t, 2, calls)

	calls = 0
	_, ref := s.T.LateWrite(b, s.Execute(b))
	require.Nil(t, ref)
	require.Equal(t, 1, calls)

	r := s.Read("Smoke")
	calls = 0
	s.T.FinishRead(r)
	require.Equal(t, 1, calls)
}

func TestMutateModel_PanicsOnBusinessChange(t *testing.T) {
	t.Parallel()
	s := newLedgerSession(t)
	require.NotPanics(t, func() { s.T.MutateModel("noop", func(*oracle.GlobalState) {}) })
	require.PanicsWithValue(t, "oraclecheck: MutateModel(balance) changed business state", func() {
		s.T.MutateModel("balance", func(gs *oracle.GlobalState) {
			*gs = gs.Apply(oracle.Bulk{Requests: []*servicepb.Request{tx(s, "", 5)}}).State
		})
	})
}

func TestLateWrite_ComparesOutcomes(t *testing.T) {
	t.Parallel()
	s := newLedgerSession(t)
	a := s.Begin(true, tx(s, "", 1))
	out := s.Execute(a)
	_, _, ref := s.T.EndWrite(a, out)
	require.Nil(t, ref)

	evs, ref := s.T.LateWrite(a, out)
	require.Nil(t, ref)
	require.Equal(t, []oc.EventID{oc.EvTwoDefinitiveOutcomes}, testutil.IDs(evs))
	require.Empty(t, testutil.Findings(evs))

	evs, ref = s.T.LateWrite(a, testutil.Fail("FailedPrecondition", "INSUFFICIENT_FUNDS"))
	require.Nil(t, ref)
	require.Equal(t, []oc.EventID{oc.EvTwoDefinitiveOutcomes, oc.EvOneKeyOneOutcome}, testutil.IDs(evs))

	// An identical repeat of /write/end answers the first status with no event;
	// a different definitive outcome is routed to the late comparison.
	status, evs, ref := s.T.EndWrite(a, out)
	require.Nil(t, ref)
	require.Equal(t, "pending", status)
	require.Empty(t, evs)
	_, evs, _ = s.T.EndWrite(a, testutil.Fail("FailedPrecondition", "INSUFFICIENT_FUNDS"))
	require.Contains(t, testutil.IDs(evs), oc.EvOneKeyOneOutcome)
}

func TestCommittedResponses_ReleaseAndCap(t *testing.T) {
	t.Parallel()
	s := newLedgerSession(t)
	s.Write(false, tx(s, "", 1))
	held := s.T.HeldResponses()
	require.Len(t, held, 2)
	_, ok := s.T.CommittedResponse(held[1])
	require.True(t, ok)
	require.Equal(t, 1, s.T.ReleaseCommittedResponses(held[0]))
	_, ok = s.T.CommittedResponse(held[0])
	require.False(t, ok)
	require.False(t, s.T.ResyncNeeded())
}
