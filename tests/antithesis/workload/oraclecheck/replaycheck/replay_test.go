package replaycheck_test

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	oc "github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck/replaycheck"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck/testutil"
	"github.com/formancehq/ledger/v3/tests/oracle"
)

func session(t *testing.T) *testutil.ScriptedSession {
	t.Helper()
	s := testutil.NewSession("")
	require.Empty(t, testutil.Findings(s.Write(false, testutil.CreateLedgerReq(s.Ledger(1)))))

	return s
}

func applyJSON(t *testing.T, key string, reqs ...*servicepb.Request) json.RawMessage {
	t.Helper()
	b, err := protojson.Marshal(servicepb.UnsignedApplyRequest(key, reqs...))
	require.NoError(t, err)

	return b
}

// replay validates a re-send of the original of ticket tk with outcome out.
func replay(t *testing.T, s *testutil.ScriptedSession, wid string, out oc.Outcome, params string) []oc.Event {
	t.Helper()
	rec, ok := s.T.Record(wid)
	require.True(t, ok)
	rt := s.Read(replaycheck.ReadKindReplay)
	c := &oc.ReadCtx{T: s.T, Ticket: rt, MaxTicket: s.T.ReadFrontier(), Kind: replaycheck.ReadKindReplay,
		Request: applyJSON(t, rec.Key, rec.Requests...), Outcome: out}
	if params != "" {
		c.Params = json.RawMessage(params)
	}
	evs := replaycheck.ValidateReplay(c)
	s.T.FinishRead(rt)

	return evs
}

// committedOriginal commits a flagged create and returns its wid and outcome.
func committedOriginal(t *testing.T, s *testutil.ScriptedSession) (string, oc.Outcome) {
	t.Helper()
	tk := s.Begin(true, testutil.TxReq(s.Ledger(1), "", "world", "users:1", "COIN", 5))
	out := s.Execute(tk)
	_, evs, ref := s.T.EndWrite(tk, out)
	require.Nil(t, ref)
	require.Empty(t, testutil.Findings(evs))
	rec, _ := s.T.RecordByTicket(tk)

	return rec.Wid, out
}

// frozenOriginal reverts a transaction twice; the flagged second revert
// fails TRANSACTION_ALREADY_REVERTED at the FSM stage and is frozen.
func frozenOriginal(t *testing.T, s *testutil.ScriptedSession) string {
	t.Helper()
	require.Empty(t, testutil.Findings(s.Write(false, testutil.TxReq(s.Ledger(1), "", "world", "users:2", "COIN", 3))))
	require.Empty(t, testutil.Findings(s.Write(false, testutil.RevertReq(s.Ledger(1), 1))))
	tk := s.Begin(true, testutil.RevertReq(s.Ledger(1), 1))
	out := s.Execute(tk)
	require.Equal(t, "TRANSACTION_ALREADY_REVERTED", out.Reason)
	_, evs, ref := s.T.EndWrite(tk, out)
	require.Nil(t, ref)
	require.Empty(t, testutil.Findings(evs))
	rec, _ := s.T.RecordByTicket(tk)
	var listed bool
	for _, e := range s.T.Replayable() {
		listed = listed || (e.Wid == rec.Wid && e.Status == "failed")
	}
	require.True(t, listed, "a frozen failure is replayable")

	return rec.Wid
}

func TestReplay_CommittedIdenticalSequences(t *testing.T) {
	t.Parallel()
	s := session(t)
	wid, out := committedOriginal(t, s)
	evs := replay(t, s, wid, out, `{"wid":"`+wid+`"}`)
	require.Empty(t, testutil.Findings(evs))
	require.Equal(t, []oc.EventID{replaycheck.EvReplayExercised}, testutil.IDs(evs))
}

func TestReplay_OriginalFromKeyWithoutWid(t *testing.T) {
	t.Parallel()
	s := session(t)
	wid, out := committedOriginal(t, s)
	evs := replay(t, s, wid, out, "")
	require.Equal(t, []oc.EventID{replaycheck.EvReplayExercised}, testutil.IDs(evs))
	evs = replay(t, s, wid, out, `{}`)
	require.Equal(t, []oc.EventID{replaycheck.EvReplayExercised}, testutil.IDs(evs))
}

func TestReplay_CommittedNewSequencesViolate(t *testing.T) {
	t.Parallel()
	s := session(t)
	wid, _ := committedOriginal(t, s)
	rec, _ := s.T.Record(wid)
	// A re-execution: same body, fresh higher sequences.
	resp := testutil.SimulateCommit(s.Server, oracle.Bulk{Requests: rec.Requests}, 100, 1)
	evs := replay(t, s, wid, testutil.OK(resp), "")
	require.Equal(t, []oc.EventID{replaycheck.EvReplayNewSequences}, testutil.IDs(evs))
}

func TestReplay_CommittedOutsideModelAndUnexpectedError(t *testing.T) {
	t.Parallel()
	s := session(t)
	wid, out := committedOriginal(t, s)
	resp, err := oc.DecodeApplyResponse(out.Response)
	require.NoError(t, err)
	// Same sequence, different transaction id.
	resp.GetLogs()[0].GetPayload().GetApply().GetLog().GetData().GetCreatedTransaction().GetTransaction().Id = 42
	evs := replay(t, s, wid, testutil.OK(resp), "")
	require.Equal(t, []oc.EventID{replaycheck.EvReplayOutsideModel}, testutil.IDs(evs))

	evs = replay(t, s, wid, testutil.Fail("FailedPrecondition", "IDEMPOTENCY_KEY_CONFLICT"), "")
	require.Equal(t, []oc.EventID{replaycheck.EvReplayUnexpectedError}, testutil.IDs(evs))
}

func TestReplay_FrozenFailure(t *testing.T) {
	t.Parallel()
	s := session(t)
	wid := frozenOriginal(t, s)
	evs := replay(t, s, wid, testutil.Fail("FailedPrecondition", "TRANSACTION_ALREADY_REVERTED"), "")
	require.Empty(t, testutil.Findings(evs))
	require.Equal(t, []oc.EventID{replaycheck.EvReplayExercised, replaycheck.EvReplayFrozenReturned}, testutil.IDs(evs))

	evs = replay(t, s, wid, testutil.Fail("FailedPrecondition", "INSUFFICIENT_FUNDS"), "")
	require.Equal(t, []oc.EventID{replaycheck.EvReplayFrozenMismatch}, testutil.IDs(evs))

	// A success (any committed response) for a frozen key.
	resp := testutil.SimulateCommit(s.Server, oracle.Bulk{Requests: []*servicepb.Request{testutil.TxReq(s.Ledger(1), "", "world", "users:3", "COIN", 1)}}, 50, 1)
	evs = replay(t, s, wid, testutil.OK(resp), "")
	require.Equal(t, []oc.EventID{replaycheck.EvReplayRejectedCommitted}, testutil.IDs(evs))
}

func TestReplay_NonDefinitiveAndTransientAreSkipped(t *testing.T) {
	t.Parallel()
	s := session(t)
	wid, _ := committedOriginal(t, s)
	for _, out := range []oc.Outcome{
		testutil.Unknown("timeout"),
		testutil.Fail("Unavailable", "MAINTENANCE_MODE"),
		testutil.Fail("Unauthenticated", ""),
		testutil.Fail("PermissionDenied", "SIGNATURE_REQUIRED"),
		testutil.Fail("Internal", ""),
		testutil.Fail("Unavailable", "STALE_PROPOSAL"),
	} {
		require.True(t, out.Transient(true) || !out.Definitive())
		require.Empty(t, replay(t, s, wid, out, ""), "%+v", out)
	}
}

func TestReplay_WrongOriginal(t *testing.T) {
	t.Parallel()
	s := session(t)
	wid, out := committedOriginal(t, s)
	evs := replay(t, s, wid, out, `{"wid":"w999"}`)
	require.Equal(t, []oc.EventID{replaycheck.EvReplayWrongOriginal}, testutil.IDs(evs))

	// A key with no original.
	rt := s.Read(replaycheck.ReadKindReplay)
	c := &oc.ReadCtx{T: s.T, Ticket: rt, MaxTicket: s.T.ReadFrontier(), Request: applyJSON(t, "o-test-k999", testutil.CreateLedgerReq("x")), Outcome: out}
	require.Equal(t, []oc.EventID{replaycheck.EvReplayWrongOriginal}, testutil.IDs(replaycheck.ValidateReplay(c)))
	s.T.FinishRead(rt)

	// The original's key with another body (a conflict sent as a replay).
	rec, _ := s.T.Record(wid)
	rt = s.Read(replaycheck.ReadKindReplay)
	c = &oc.ReadCtx{T: s.T, Ticket: rt, MaxTicket: s.T.ReadFrontier(), Request: applyJSON(t, rec.Key, testutil.TxReq(s.Ledger(1), "", "world", "users:9", "COIN", 5)), Outcome: out}
	require.Equal(t, []oc.EventID{replaycheck.EvReplayWrongOriginal}, testutil.IDs(replaycheck.ValidateReplay(c)))
	s.T.FinishRead(rt)
}

func TestReplay_UnflaggedOrAdmissionOriginalsAreSkipped(t *testing.T) {
	t.Parallel()
	s := session(t)
	// Unflagged commit.
	tk := s.Begin(false, testutil.TxReq(s.Ledger(1), "", "world", "users:1", "COIN", 5))
	out := s.Execute(tk)
	_, _, ref := s.T.EndWrite(tk, out)
	require.Nil(t, ref)
	rec, _ := s.T.RecordByTicket(tk)
	require.Empty(t, replay(t, s, rec.Wid, out, ""))

	// Admission-stage rejection (empty create): not frozen, never replayable.
	tk = s.Begin(true, testutil.EmptyTxReq(s.Ledger(1)))
	fail := s.Execute(tk)
	require.Equal(t, "VALIDATION", fail.Reason)
	_, _, ref = s.T.EndWrite(tk, fail)
	require.Nil(t, ref)
	rec, _ = s.T.RecordByTicket(tk)
	require.Empty(t, replay(t, s, rec.Wid, fail, ""))
}

func TestConflictHookAndSkipCoverage(t *testing.T) {
	t.Parallel()
	s := session(t)
	wid, _ := committedOriginal(t, s)
	orig, _ := s.T.Record(wid)

	// A conflict under the committed key is explained and covered.
	conflict := oc.WriteRecord{Wid: "wc", Key: orig.Key, Role: oc.RoleConflict, Of: wid, Family: "test", TrackKey: true,
		Requests: []*servicepb.Request{testutil.TxReq(s.Ledger(1), "", "world", "users:1", "COIN", 6)}}
	tk, _, _, ref := s.T.BeginWrite(conflict)
	require.Nil(t, ref)
	_, evs, ref := s.T.EndWrite(tk, testutil.Fail("FailedPrecondition", "IDEMPOTENCY_KEY_CONFLICT"))
	require.Nil(t, ref)
	require.Empty(t, testutil.Findings(evs))
	require.Contains(t, testutil.IDs(evs), replaycheck.EvConflictRejection)

	// Skip-and-continue: a reverted transaction's revert opted in, then a
	// committed order.
	require.Empty(t, testutil.Findings(s.Write(false, testutil.RevertReq(s.Ledger(1), 1))))
	skipRevert := testutil.RevertReq(s.Ledger(1), 1)
	skipRevert.GetApply().SkippableReasons = []commonpb.ErrorReason{commonpb.ErrorReason_ERROR_REASON_TRANSACTION_ALREADY_REVERTED}
	skipMeta := &servicepb.Request{Type: &servicepb.Request_Apply{Apply: &servicepb.LedgerApplyRequest{
		Ledger:           s.Ledger(1),
		SkippableReasons: []commonpb.ErrorReason{commonpb.ErrorReason_ERROR_REASON_METADATA_NOT_FOUND},
		Action: &servicepb.LedgerAction{Data: &servicepb.LedgerAction_DeleteMetadata{DeleteMetadata: &commonpb.DeleteMetadataCommand{
			Target: &commonpb.Target{Target: &commonpb.Target_Account{Account: &commonpb.TargetAccount{Addr: "users:1"}}}, Key: "absent-k1",
		}}},
	}}}
	// A skip followed by another skip does not continue; each is followed by
	// a committed order here.
	evs = s.Write(false, skipRevert, skipMeta, testutil.AccountMetaReq(s.Ledger(1), "users:1", "k0", "v"))
	require.Empty(t, testutil.Findings(evs))
	require.NotContains(t, testutil.IDs(evs), replaycheck.CovSkipAlreadyReverted)
	evs = s.Write(false, skipRevert, testutil.AccountMetaReq(s.Ledger(1), "users:1", "k1", "v"), skipMeta, testutil.AccountMetaReq(s.Ledger(1), "users:1", "k0", "v"))
	require.Empty(t, testutil.Findings(evs))
	ids := testutil.IDs(evs)
	require.Contains(t, ids, replaycheck.CovSkipAlreadyReverted)
	require.Contains(t, ids, replaycheck.CovSkipMetadataNotFound)
	for _, e := range evs {
		if e.ID == replaycheck.CovSkipAlreadyReverted {
			require.Equal(t, oc.EvRaw, e.Kind)
			require.True(t, e.Cond)
		}
	}

	// A trailing skip (nothing committed after it) is not a hit.
	evs = s.Write(false, testutil.AccountMetaReq(s.Ledger(1), "users:1", "k0", "w"), skipRevert)
	require.Empty(t, testutil.Findings(evs))
	require.NotContains(t, testutil.IDs(evs), replaycheck.CovSkipAlreadyReverted)
	require.True(t, oc.IsCoverageID(replaycheck.CovSkipMetadataNotFound))
}

func TestRevertSeekView(t *testing.T) {
	t.Parallel()
	s := session(t)
	l := s.Ledger(1)
	require.Empty(t, testutil.Findings(s.Write(false, testutil.TxReq(l, "", "world", "users:1", "COIN", 5))))
	require.Empty(t, testutil.Findings(s.Write(false, testutil.TxReq(l, "", "world", "users:2", "COIN", 5))))
	require.Empty(t, testutil.Findings(s.Write(false, testutil.RevertReq(l, 1)))) // tx 3 reverts 1
	// A reserved transaction and its revert stay hidden.
	require.Empty(t, testutil.Findings(s.Write(false, testutil.TxReq(l, "", "world", "users:own-x", "COIN", 5)))) // tx 4
	require.Empty(t, testutil.Findings(s.Write(false, testutil.RevertReq(l, 4))))                                 // tx 5

	view := func(probe string, owned bool) map[string]any {
		raw, err := json.Marshal(map[string]any{"name": replaycheck.ViewRevertSeek, "ledger": l, "probe": probe, "owned": owned})
		require.NoError(t, err)
		out, err := s.T.ComputeViews(s.T.Model(), []json.RawMessage{raw})
		require.NoError(t, err)
		var m map[string]any
		require.NoError(t, json.Unmarshal(out[0], &m))

		return m
	}
	for _, probe := range []string{"1", "3", "4", "99", "x"} {
		m := view(probe, false)
		require.Equal(t, true, m["found"], probe)
		entry := m["entry"].(map[string]any)
		require.Equal(t, "3", entry["id"], probe)
		require.Equal(t, "1", entry["revertsTransaction"])
	}
	require.Equal(t, "5", view("4", true)["entry"].(map[string]any)["id"])
	require.Equal(t, l, view("1", false)["ledger"])
}
