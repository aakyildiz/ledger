package oraclecheck_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	oc "github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck/testutil"
	"github.com/formancehq/ledger/v3/tests/oracle"
	"github.com/formancehq/ledger/v3/tests/oracle/oracletest"
)

const insufficient = "INSUFFICIENT_FUNDS"

// Two concurrent drains of a:1: the second fails, explained only because the
// first is in flight.
func TestFailure_ExplainedOnlyByInflightPredecessor(t *testing.T) {
	t.Parallel()
	s := bareLedgerSession(t)
	s.Write(false, oracletest.TxReq("world", "a:1", "USD", 5))

	first := s.Begin(false, oracletest.TxReq("a:1", "b:1", "USD", 5))
	second := s.Begin(true, oracletest.TxReq("a:1", "b:1", "USD", 5))
	outFirst := s.Execute(first)
	outSecond := s.Execute(second)
	require.True(t, outFirst.OK)
	require.Equal(t, insufficient, outSecond.Reason)

	status, evs, ref := s.T.EndWrite(second, outSecond)
	require.Nil(t, ref)
	require.Equal(t, "validated", status)
	require.Empty(t, testutil.Findings(evs))
	require.Contains(t, testutil.IDs(evs), oc.EvModelOutcomeVerified)
	rec, _ := s.T.RecordByTicket(second)
	require.NotNil(t, rec.Result)
	require.Equal(t, insufficient, rec.Result.Reason)
	require.Equal(t, oc.WFailed, rec.Status)
	_, ok := s.T.Rejections()[oc.RejectedBulk{Ledgers: "L", Orders: 1, Reason: insufficient}]
	require.True(t, ok)

	// The same failure with no predecessor in flight is unexplained.
	_, evs = s.End(first)
	require.Empty(t, testutil.Findings(evs))
	lone := s.Begin(false, oracletest.TxReq("world", "b:1", "USD", 1))
	_, evs, ref = s.T.EndWrite(lone, testutil.Fail("FailedPrecondition", insufficient))
	require.Nil(t, ref)
	findings := testutil.Findings(evs)
	require.Equal(t, []oc.EventID{oc.EvFailureUnexplained}, testutil.IDs(findings))
	require.Equal(t, "OK", findings[0].Details["modelReason"])
}

func TestFailure_UnexplainedReasonReportsModelReason(t *testing.T) {
	t.Parallel()
	s := bareLedgerSession(t)
	tk := s.Begin(false, oracletest.TxReq("a:9", "b:1", "USD", 5))
	_, evs, ref := s.T.EndWrite(tk, testutil.Fail("FailedPrecondition", "TRANSACTION_REFERENCE_CONFLICT"))
	require.Nil(t, ref)
	findings := testutil.Findings(evs)
	require.Len(t, findings, 1)
	require.Equal(t, oc.EvFailureUnexplained, findings[0].ID)
	require.Equal(t, insufficient, findings[0].Details["modelReason"])
}

func TestFailure_HooksSeeExplainingResult(t *testing.T) {
	var seen []string
	oc.RegisterFailureHook("zz-test-failure", func(_ *oc.Tracker, rec oc.WriteRecord, res oracle.ApplyResult, reason string) []oc.Event {
		seen = append(seen, rec.Wid+":"+res.Reason+":"+reason)

		return nil
	})
	t.Cleanup(func() { oc.TestingUnregister("zz-test-failure") })

	s := bareLedgerSession(t)
	tk := s.Begin(false, oracletest.TxReq("a:9", "b:1", "USD", 5))
	rec, _ := s.T.RecordByTicket(tk)
	s.End(tk)
	require.Equal(t, []string{rec.Wid + ":" + insufficient + ":" + insufficient}, seen)

	// An unexplained failure runs no failure hook.
	seen = nil
	tk = s.Begin(false, oracletest.TxReq("world", "b:1", "USD", 5))
	_, _, _ = s.T.EndWrite(tk, testutil.Fail("FailedPrecondition", insufficient))
	require.Empty(t, seen)
}

func TestFailure_FrozenFailuresAreReplayable(t *testing.T) {
	t.Parallel()
	s := bareLedgerSession(t)

	frozen := s.Begin(true, oracletest.TxReq("a:9", "b:1", "USD", 5))
	s.End(frozen)
	notFlagged := s.Begin(false, oracletest.TxReq("a:9", "b:1", "USD", 6))
	s.End(notFlagged)

	malformed := &servicepb.Request{Type: &servicepb.Request_CreatePreparedQuery{CreatePreparedQuery: &servicepb.CreatePreparedQueryRequest{
		Ledger: "L",
		Query: &commonpb.PreparedQuery{Name: "q", Target: commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS,
			Filter: &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Address{Address: &commonpb.AddressMatch{}}}},
	}}}
	unfrozen := s.Begin(true, malformed)
	out := s.Execute(unfrozen)
	require.Equal(t, "FILTER_COMPILATION_ERROR", out.Reason)
	_, evs, ref := s.T.EndWrite(unfrozen, out)
	require.Nil(t, ref)
	require.Empty(t, testutil.Findings(evs))

	rec, _ := s.T.RecordByTicket(frozen)
	require.Equal(t, []oc.ReplayEntry{{Wid: rec.Wid, Key: rec.Key, Status: "failed", Reason: insufficient, Stage: "fsm"}}, s.T.Replayable())
}

func TestFailure_ReservedBulksAreNotReplayable(t *testing.T) {
	t.Parallel()
	s := bareLedgerSession(t)
	s.Write(true, oracletest.TxReq("world", "t-0:own-x", "USD", 5))
	s.Write(true, oracletest.TxReq("t-0:own-y", "b:1", "USD", 5)) // frozen failure
	require.Empty(t, s.T.Replayable())
}
