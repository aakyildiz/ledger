package checkpointcheck_test

import (
	"encoding/json"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/formancehq/ledger/v3/internal/proto/clusterpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	oc "github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck"
	cc "github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck/checkpointcheck"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck/match"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck/match/matchtest"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck/testutil"
	"github.com/formancehq/ledger/v3/tests/oracle"
)

func pj(t *testing.T, m proto.Message) json.RawMessage {
	t.Helper()
	b, err := protojson.Marshal(m)
	require.NoError(t, err)

	return b
}

func okOut(t *testing.T, m proto.Message) oc.Outcome { return oc.Outcome{OK: true, Response: pj(t, m)} }

func pageOut(t *testing.T, next string, rows ...proto.Message) oc.Outcome {
	out := oc.Outcome{OK: true, Complete: true, NextCursor: next}
	for _, r := range rows {
		out.Items = append(out.Items, pj(t, r))
	}

	return out
}

var notFound = oc.Outcome{Code: "NotFound", Message: "not found"}

func write(t *testing.T, s *testutil.ScriptedSession, reqs ...*servicepb.Request) []oc.Event {
	t.Helper()
	evs := s.Write(false, reqs...)
	require.Empty(t, testutil.Findings(evs))

	return evs
}

func deleteCheckpoint(id uint64) *servicepb.Request {
	return &servicepb.Request{Type: &servicepb.Request_DeleteQueryCheckpoint{DeleteQueryCheckpoint: &servicepb.DeleteQueryCheckpointRequest{CheckpointId: id}}}
}

func setSchedule(cron string) *servicepb.Request {
	return &servicepb.Request{Type: &servicepb.Request_SetQueryCheckpointSchedule{SetQueryCheckpointSchedule: &servicepb.SetQueryCheckpointScheduleRequest{Cron: cron}}}
}

func rawHit(evs []oc.Event, id oc.EventID) bool {
	for _, e := range evs {
		if e.ID == id && e.Kind == oc.EvRaw && e.Cond {
			return true
		}
	}

	return false
}

func reached(evs []oc.Event, id oc.EventID) bool {
	for _, e := range evs {
		if e.ID == id && e.Kind == oc.EvReachable {
			return true
		}
	}

	return false
}

// validate runs kind's validator on a read ticket taken now.
func validate(t *testing.T, s *testutil.ScriptedSession, kind string, req proto.Message, out oc.Outcome, params map[string]any) []oc.Event {
	t.Helper()
	tk := s.Read(kind)
	defer s.T.FinishRead(tk)

	return validateOn(t, s, tk, s.T.Model(), kind, req, out, params)
}

func validateOn(t *testing.T, s *testutil.ScriptedSession, tk uint64, snap oracle.GlobalState, kind string, req proto.Message, out oc.Outcome, params map[string]any) []oc.Event {
	t.Helper()
	k, ok := oc.LookupReadKind(kind)
	require.True(t, ok)
	pb, _ := json.Marshal(params)
	c := &oc.ReadCtx{T: s.T, Ticket: tk, MaxTicket: s.T.ReadFrontier(), Snapshot: snap, Kind: kind, Request: pj(t, req), Outcome: out, Params: pb}

	return k.Validate(c)
}

// fixture: l1 with history, checkpoint 1 frozen after it, then l1 moves on
// and l2 is created after the checkpoint.
func fixture(t *testing.T) (*testutil.ScriptedSession, oracle.GlobalState) {
	t.Helper()
	s := testutil.NewSession("")
	l1 := s.Ledger(1)
	write(t, s, testutil.CreateLedgerReq(l1))
	write(t, s, testutil.TxReq(l1, "", "world", "t-0:1", "USD/2", 10))
	write(t, s, testutil.TxReq(l1, "", "t-0:1", "t-0:2", "USD/2", 3))
	evs := write(t, s, testutil.CreateCheckpointReq())
	require.True(t, rawHit(evs, cc.CovCreate), "creation coverage at the drain")
	frozen, ok := s.T.FrozenCheckpoint(1)
	require.True(t, ok)
	write(t, s, testutil.TxReq(l1, "", "world", "t-0:1", "USD/2", 5))
	write(t, s, testutil.CreateLedgerReq(s.Ledger(2)))

	return s, frozen
}

func TestBeginHook_SerializesCreatesAndPredictsTheID(t *testing.T) {
	t.Parallel()
	s := testutil.NewSession("")
	write(t, s, testutil.CreateLedgerReq(s.Ledger(1)))

	tk, _, predicted, ref := s.T.BeginWrite(s.Record(false, testutil.CreateCheckpointReq()))
	require.Nil(t, ref)
	require.Equal(t, "1", predicted["checkpointId"])

	// In flight: a second create is refused, other writes are not.
	_, _, _, ref = s.T.BeginWrite(s.Record(false, testutil.CreateCheckpointReq()))
	require.NotNil(t, ref)
	require.Equal(t, oc.CodeSerialized, ref.Code)
	require.Equal(t, 409, ref.HTTP)
	_, _, p2, ref := s.T.BeginWrite(s.Record(false, setSchedule("@every 876000h")))
	require.Nil(t, ref)
	require.NotContains(t, p2, "checkpointId")

	// Pending (committed, not drained behind an earlier open ticket): refused.
	s2 := testutil.NewSession("")
	write(t, s2, testutil.CreateLedgerReq(s2.Ledger(1)))
	early := s2.Begin(false, testutil.TxReq(s2.Ledger(1), "", "world", "t-0:1", "COIN", 1))
	create := s2.Begin(false, testutil.CreateCheckpointReq())
	status, evs := s2.End(create)
	require.Equal(t, "pending", status)
	require.Empty(t, testutil.Findings(evs))
	_, _, _, ref = s2.T.BeginWrite(s2.Record(false, testutil.CreateCheckpointReq()))
	require.NotNil(t, ref, "a pending create still serializes")
	_, evs = s2.End(early)
	require.Empty(t, testutil.Findings(evs))
	_, _, predicted, ref = s2.T.BeginWrite(s2.Record(false, testutil.CreateCheckpointReq()))
	require.Nil(t, ref)
	require.Equal(t, "2", predicted["checkpointId"], "the drained model's next id")

	// The first session's create, once ended, frees the next one.
	_, evs = s.End(tk)
	require.Empty(t, testutil.Findings(evs))
}

func TestCheckpointRead_FrozenAccountAndTransaction(t *testing.T) {
	t.Parallel()
	s, frozen := fixture(t)
	l1 := s.Ledger(1)
	params := map[string]any{"arm": cc.ArmGetAccount, "id": "1"}
	req := &servicepb.GetAccountRequest{Ledger: l1, Address: "t-0:1", CheckpointId: 1}

	evs := validate(t, s, cc.KindCheckpointRead, req, okOut(t, matchtest.RenderAccount(frozen, l1, "t-0:1", false)), params)
	require.Empty(t, testutil.Findings(evs))
	require.True(t, rawHit(evs, cc.CovFrozenRead))

	// Live data never substitutes for the frozen snapshot.
	evs = validate(t, s, cc.KindCheckpointRead, req, okOut(t, matchtest.RenderAccount(s.T.Model(), l1, "t-0:1", false)), params)
	require.Equal(t, []oc.EventID{cc.EvReadOutsideFrozen}, testutil.IDs(testutil.Findings(evs)))

	// NotFound of an account the live checkpoint knows is a finding.
	evs = validate(t, s, cc.KindCheckpointRead, req, notFound, params)
	require.Equal(t, []oc.EventID{cc.EvReadOutsideFrozen}, testutil.IDs(testutil.Findings(evs)))

	// An account the frozen state does not know: NotFound is the frozen answer.
	absent := &servicepb.GetAccountRequest{Ledger: l1, Address: "t-9:9", CheckpointId: 1}
	require.Empty(t, testutil.Findings(validate(t, s, cc.KindCheckpointRead, absent, notFound, params)))

	// Transactions: tx 3 exists live, not in the frozen state.
	tparams := map[string]any{"arm": cc.ArmGetTransaction, "id": "1"}
	tx2 := &servicepb.GetTransactionRequest{Ledger: l1, TransactionId: 2, CheckpointId: 1}
	evs = validate(t, s, cc.KindCheckpointRead, tx2, okOut(t, &servicepb.GetTransactionResponse{Transaction: matchtest.RenderTx(frozen, l1, 2)}), tparams)
	require.Empty(t, testutil.Findings(evs))
	tx3 := &servicepb.GetTransactionRequest{Ledger: l1, TransactionId: 3, CheckpointId: 1}
	require.Empty(t, testutil.Findings(validate(t, s, cc.KindCheckpointRead, tx3, notFound, tparams)))
	evs = validate(t, s, cc.KindCheckpointRead, tx3, okOut(t, &servicepb.GetTransactionResponse{Transaction: matchtest.RenderTx(s.T.Model(), l1, 3)}), tparams)
	require.Equal(t, []oc.EventID{cc.EvReadOutsideFrozen}, testutil.IDs(testutil.Findings(evs)))
}

func TestCheckpointRead_StatsListsAggregateLog(t *testing.T) {
	t.Parallel()
	s, frozen := fixture(t)
	l1 := s.Ledger(1)

	stats := &servicepb.GetLedgerStatsRequest{Ledger: l1, CheckpointId: 1}
	sp := map[string]any{"arm": cc.ArmGetLedgerStats}
	require.Empty(t, testutil.Findings(validate(t, s, cc.KindCheckpointRead, stats, okOut(t, matchtest.RenderStats(frozen, l1)), sp)))
	evs := validate(t, s, cc.KindCheckpointRead, stats, okOut(t, matchtest.RenderStats(s.T.Model(), l1)), sp)
	require.Equal(t, []oc.EventID{cc.EvReadOutsideFrozen}, testutil.IDs(testutil.Findings(evs)))

	agg := &servicepb.AggregateVolumesRequest{Ledger: l1, CheckpointId: 1, GroupByPrefixes: []string{"t-"}}
	opts := match.AggOptions{GroupByPrefixes: []string{"t-"}}
	ap := map[string]any{"arm": cc.ArmAggregateVolumes}
	require.Empty(t, testutil.Findings(validate(t, s, cc.KindCheckpointRead, agg, okOut(t, matchtest.RenderAggregate(frozen, l1, opts)), ap)))
	evs = validate(t, s, cc.KindCheckpointRead, agg, okOut(t, matchtest.RenderAggregate(s.T.Model(), l1, opts)), ap)
	require.Equal(t, []oc.EventID{cc.EvReadOutsideFrozen}, testutil.IDs(testutil.Findings(evs)))
	// AGGREGATE_OVERFLOW is explained only when the frozen fold overflows
	// (task 99); the fixture's small amounts never do.
	overflow := oc.Outcome{Code: "FailedPrecondition", Reason: "AGGREGATE_OVERFLOW"}
	maxp := &servicepb.AggregateVolumesRequest{Ledger: l1, CheckpointId: 1, UseMaxPrecision: true}
	require.False(t, match.AggregateOverflows(frozen.Ledger(l1), nil, match.AggOptions{UseMaxPrecision: true}))
	evs = validate(t, s, cc.KindCheckpointRead, maxp, overflow, ap)
	require.Equal(t, []oc.EventID{cc.EvReadOutsideFrozen}, testutil.IDs(testutil.Findings(evs)))

	la := &servicepb.ListAccountsRequest{Ledger: l1, Options: &commonpb.ListOptions{Read: &commonpb.ReadOptions{CheckpointId: 1}, PageSize: 1}}
	page, next := matchtest.RenderAccountPage(frozen, l1, matchtest.AccountPageReq{PageSize: 1})
	lap := map[string]any{"arm": cc.ArmListAccounts}
	rows := make([]proto.Message, len(page))
	for i, r := range page {
		rows[i] = r
	}
	evs = validate(t, s, cc.KindCheckpointRead, la, pageOut(t, next, rows...), lap)
	require.Empty(t, testutil.Findings(evs))
	require.True(t, rawHit(evs, cc.CovFrozenRead))
	evs = validate(t, s, cc.KindCheckpointRead, la, pageOut(t, "", rows...), lap)
	require.NotEmpty(t, testutil.Findings(evs), "a full page with a waiting row needs a cursor")

	lt := &servicepb.ListTransactionsRequest{Ledger: l1, Options: &commonpb.ListOptions{Read: &commonpb.ReadOptions{CheckpointId: 1}, PageSize: 5}}
	tpage, tnext := matchtest.RenderTxPage(frozen, l1, matchtest.TxPageReq{PageSize: 5})
	trows := make([]proto.Message, len(tpage))
	for i, r := range tpage {
		trows[i] = r
	}
	ltp := map[string]any{"arm": cc.ArmListTransactions}
	require.Empty(t, testutil.Findings(validate(t, s, cc.KindCheckpointRead, lt, pageOut(t, tnext, trows...), ltp)))
	live, _ := matchtest.RenderTxPage(s.T.Model(), l1, matchtest.TxPageReq{PageSize: 5})
	lrows := make([]proto.Message, len(live))
	for i, r := range live {
		lrows[i] = r
	}
	require.NotEmpty(t, testutil.Findings(validate(t, s, cc.KindCheckpointRead, lt, pageOut(t, "", lrows...), ltp)))

	// GetLog: a frozen sequence matches its frozen log; a far sequence must be NotFound.
	var seqs []uint64
	for seq := range frozen.Logs() {
		seqs = append(seqs, seq)
	}
	slices.Sort(seqs)
	require.NotEmpty(t, seqs)
	entry, _ := frozen.Log(seqs[1])
	log, ok := matchtest.RenderLog(frozen, entry.Ledger, entry.ID)
	require.True(t, ok)
	gp := map[string]any{"arm": cc.ArmGetLog}
	evs = validate(t, s, cc.KindCheckpointRead, &servicepb.GetLogRequest{Sequence: seqs[1], CheckpointId: 1}, okOut(t, log), gp)
	require.Empty(t, testutil.Findings(evs))
	require.True(t, rawHit(evs, cc.CovFrozenRead))
	maxSeq, _ := s.T.FrozenCheckpointMaxSequence(1)
	far := &servicepb.GetLogRequest{Sequence: maxSeq + 1<<40, CheckpointId: 1}
	require.Empty(t, testutil.Findings(validate(t, s, cc.KindCheckpointRead, far, notFound, gp)))
	require.NotEmpty(t, testutil.Findings(validate(t, s, cc.KindCheckpointRead, far, okOut(t, log), gp)))
}

func TestCheckpointRead_ListLedgersForeignOnBothSides(t *testing.T) {
	t.Parallel()
	s, frozen := fixture(t)
	prefix := s.Prefix
	foreign := []string{"billing", "default", "r-burst-x", "treasury"}
	lp := map[string]any{"arm": cc.ArmListLedgers}
	for _, reverse := range []bool{false, true} {
		for _, size := range []uint32{1, 2, 3, 10} {
			lreq := match.ListLedgersReq{PageSize: size, Reverse: reverse}
			page, next := matchtest.RenderLedgerPage(frozen, prefix, foreign, lreq)
			rows := make([]proto.Message, len(page))
			for i, r := range page {
				rows[i] = r
			}
			req := &servicepb.ListLedgersRequest{Options: &commonpb.ListOptions{Read: &commonpb.ReadOptions{CheckpointId: 1}, PageSize: size, Reverse: reverse}}
			evs := validate(t, s, cc.KindCheckpointRead, req, pageOut(t, next, rows...), lp)
			require.Empty(t, testutil.Findings(evs), "size %d reverse %v", size, reverse)
			require.True(t, reached(evs, cc.EvFrozenFleetListing))

			// The live fleet (l2 was created after the checkpoint) is not the frozen one.
			if size == 10 {
				lpage, lnext := matchtest.RenderLedgerPage(s.T.Model(), prefix, foreign, lreq)
				lrows := make([]proto.Message, len(lpage))
				for i, r := range lpage {
					lrows[i] = r
				}
				evs = validate(t, s, cc.KindCheckpointRead, req, pageOut(t, lnext, lrows...), lp)
				require.Equal(t, []oc.EventID{cc.EvReadOutsideFrozen}, testutil.IDs(testutil.Findings(evs)))
			}
		}
	}
}

func TestCheckpointRead_DeletedCheckpointNotFound(t *testing.T) {
	t.Parallel()
	s, frozen := fixture(t)
	l1 := s.Ledger(1)
	params := map[string]any{"arm": cc.ArmGetAccount}
	req := &servicepb.GetAccountRequest{Ledger: l1, Address: "t-0:1", CheckpointId: 1}

	// While live, NotFound is a finding (above); a read ticket opened before
	// the delete commits sees both states.
	tk := s.Read(cc.KindCheckpointRead)
	snap := s.T.Model()
	status, evs := s.End(s.Begin(false, deleteCheckpoint(1)))
	require.Equal(t, "pending", status, "the open read holds the drain")
	require.Empty(t, testutil.Findings(evs))
	evs = validateOn(t, s, tk, snap, cc.KindCheckpointRead, req, notFound, params)
	require.Empty(t, testutil.Findings(evs))
	evs = s.T.FinishRead(tk)
	require.True(t, rawHit(evs, cc.CovDelete), "deletion coverage at the drain")
	require.True(t, s.T.CheckpointDeleted(1))

	evs = validate(t, s, cc.KindCheckpointRead, req, notFound, params)
	require.Empty(t, testutil.Findings(evs))
	require.True(t, rawHit(evs, cc.CovDeletedRead))
	// A lagging replica may still serve the frozen snapshot.
	evs = validate(t, s, cc.KindCheckpointRead, req, okOut(t, matchtest.RenderAccount(frozen, l1, "t-0:1", false)), params)
	require.Empty(t, testutil.Findings(evs))
	require.False(t, rawHit(evs, cc.CovDeletedRead))
	// …but never live data.
	evs = validate(t, s, cc.KindCheckpointRead, req, okOut(t, matchtest.RenderAccount(s.T.Model(), l1, "t-0:1", false)), params)
	require.Equal(t, []oc.EventID{cc.EvReadOutsideFrozen}, testutil.IDs(testutil.Findings(evs)))
	// Any other error is a finding.
	evs = validate(t, s, cc.KindCheckpointRead, req, oc.Outcome{Code: "Internal"}, params)
	require.Equal(t, []oc.EventID{cc.EvReadOutsideFrozen}, testutil.IDs(testutil.Findings(evs)))
}

func TestPredictedRead_BeforeAndAfterTheCreate(t *testing.T) {
	t.Parallel()
	s := testutil.NewSession("")
	l1 := s.Ledger(1)
	write(t, s, testutil.CreateLedgerReq(l1))
	write(t, s, testutil.TxReq(l1, "", "world", "t-0:1", "COIN", 4))
	before := s.T.Model()
	req := &servicepb.GetLedgerRequest{Ledger: l1, Read: &commonpb.ReadOptions{CheckpointId: 1}}
	good := okOut(t, matchtest.RenderLedgerInfo(before, l1))
	wrongModel := before.Apply(oracle.Bulk{Requests: []*servicepb.Request{testutil.AddTypeReq(l1, "users", "users:{id}")}}).State
	wrong := okOut(t, matchtest.RenderLedgerInfo(wrongModel, l1))

	create := s.Begin(false, testutil.CreateCheckpointReq())
	// Before the transition: NotFound legal; the pre-create state legal.
	require.Empty(t, testutil.Findings(validate(t, s, cc.KindPredictedCheckpointRead, req, notFound, nil)))
	require.Empty(t, testutil.Findings(validate(t, s, cc.KindPredictedCheckpointRead, req, good, nil)))
	evs := validate(t, s, cc.KindPredictedCheckpointRead, req, wrong, nil)
	require.Equal(t, []oc.EventID{cc.EvPredictedOutsideCreation}, testutil.IDs(testutil.Findings(evs)))
	evs = validate(t, s, cc.KindPredictedCheckpointRead, req, oc.Outcome{Code: "FailedPrecondition", Reason: "X"}, nil)
	require.Equal(t, []oc.EventID{cc.EvPredictedUnexpectedError}, testutil.IDs(testutil.Findings(evs)))

	_, evs = s.End(create)
	require.Empty(t, testutil.Findings(evs))
	// After the transition drained: the frozen state, never NotFound.
	require.Empty(t, testutil.Findings(validate(t, s, cc.KindPredictedCheckpointRead, req, good, nil)))
	evs = validate(t, s, cc.KindPredictedCheckpointRead, req, notFound, nil)
	require.Equal(t, []oc.EventID{cc.EvPredictedUnexpectedError}, testutil.IDs(testutil.Findings(evs)))
	// A later write does not change what the checkpoint froze.
	write(t, s, testutil.AccountMetaReq(l1, "t-0:1", "k0", "v"))
	require.Empty(t, testutil.Findings(validate(t, s, cc.KindPredictedCheckpointRead, req, good, nil)))

	// A ledger absent from the frozen state explains NotFound.
	gone := &servicepb.GetLedgerRequest{Ledger: s.Ledger(7), Read: &commonpb.ReadOptions{CheckpointId: 1}}
	require.Empty(t, testutil.Findings(validate(t, s, cc.KindPredictedCheckpointRead, gone, notFound, nil)))

	// A DeleteLedger committed after the create: no frozen checkpoint keeps
	// serving the deleted ledger, so NotFound is legal (M/ reported it).
	write(t, s, testutil.DeleteLedgerReq(l1))
	require.Empty(t, testutil.Findings(validate(t, s, cc.KindPredictedCheckpointRead, req, notFound, nil)))
	// …but other errors are still findings.
	evs = validate(t, s, cc.KindPredictedCheckpointRead, req, oc.Outcome{Code: "Internal"}, nil)
	require.Equal(t, []oc.EventID{cc.EvPredictedUnexpectedError}, testutil.IDs(testutil.Findings(evs)))
}

func TestMalformedRequestsAreDriverProtocolErrors(t *testing.T) {
	t.Parallel()
	s, _ := fixture(t)
	// An unknown arm, and a request naming no checkpoint, are driver bugs.
	evs := validate(t, s, cc.KindCheckpointRead, &servicepb.GetAccountRequest{Ledger: s.Ledger(1), Address: "t-0:1", CheckpointId: 1}, notFound, map[string]any{"arm": "Bogus"})
	require.Equal(t, []oc.EventID{cc.EvProtocolMalformedRead}, testutil.IDs(testutil.Findings(evs)))
	evs = validate(t, s, cc.KindCheckpointRead, &servicepb.GetAccountRequest{Ledger: s.Ledger(1), Address: "t-0:1"}, notFound, map[string]any{"arm": cc.ArmGetAccount})
	require.Equal(t, []oc.EventID{cc.EvProtocolMalformedRead}, testutil.IDs(testutil.Findings(evs)))
	evs = validate(t, s, cc.KindPredictedCheckpointRead, &servicepb.GetLedgerRequest{Ledger: s.Ledger(1)}, notFound, nil)
	require.Equal(t, []oc.EventID{cc.EvProtocolMalformedRead}, testutil.IDs(testutil.Findings(evs)))
	evs = validate(t, s, cc.KindCheckpointInfo, &clusterpb.GetQueryCheckpointInfoRequest{}, notFound, nil)
	require.Equal(t, []oc.EventID{cc.EvProtocolMalformedRead}, testutil.IDs(testutil.Findings(evs)))
}

func TestCheckpointInfo(t *testing.T) {
	t.Parallel()
	s, _ := fixture(t)
	maxSeq, ok := s.T.FrozenCheckpointMaxSequence(1)
	require.True(t, ok)
	req := &clusterpb.GetQueryCheckpointInfoRequest{CheckpointId: 1}
	info := func(id, seq uint64) oc.Outcome {
		return okOut(t, &clusterpb.QueryCheckpointInfo{CheckpointId: id, MaxSequence: seq})
	}

	evs := validate(t, s, cc.KindCheckpointInfo, req, info(1, maxSeq), nil)
	require.Empty(t, testutil.Findings(evs))
	require.True(t, reached(evs, cc.EvInfoMatched))
	// Wrong id, wrong max sequence, NotFound on a live id: outside the model.
	for _, out := range []oc.Outcome{info(2, maxSeq), info(1, maxSeq+1), notFound} {
		evs = validate(t, s, cc.KindCheckpointInfo, req, out, nil)
		require.Equal(t, []oc.EventID{cc.EvInfoOutsideModel}, testutil.IDs(testutil.Findings(evs)), "%+v", out)
	}
	evs = validate(t, s, cc.KindCheckpointInfo, req, oc.Outcome{Code: "Internal"}, nil)
	require.Equal(t, []oc.EventID{cc.EvInfoUnexpectedError}, testutil.IDs(testutil.Findings(evs)))

	// An in-flight delete: both answers are legal.
	del := s.Begin(false, deleteCheckpoint(1))
	require.Empty(t, testutil.Findings(validate(t, s, cc.KindCheckpointInfo, req, notFound, nil)))
	require.Empty(t, testutil.Findings(validate(t, s, cc.KindCheckpointInfo, req, info(1, maxSeq), nil)))
	_, evs = s.End(del)
	require.Empty(t, testutil.Findings(evs))
	// Deleted: NotFound only.
	require.Empty(t, testutil.Findings(validate(t, s, cc.KindCheckpointInfo, req, notFound, nil)))
	evs = validate(t, s, cc.KindCheckpointInfo, req, info(1, maxSeq), nil)
	require.Equal(t, []oc.EventID{cc.EvInfoOutsideModel}, testutil.IDs(testutil.Findings(evs)))
}

func TestRegistryAndSchedule(t *testing.T) {
	t.Parallel()
	s, _ := fixture(t)
	maxSeq, _ := s.T.FrozenCheckpointMaxSequence(1)
	list := func(cps ...*clusterpb.QueryCheckpointInfo) oc.Outcome {
		return okOut(t, &clusterpb.ListQueryCheckpointsResponse{Checkpoints: cps})
	}
	req := &clusterpb.ListQueryCheckpointsRequest{}

	evs := validate(t, s, cc.KindCheckpointRegistry, req, list(&clusterpb.QueryCheckpointInfo{CheckpointId: 1, MaxSequence: maxSeq}), nil)
	require.Empty(t, testutil.Findings(evs))
	require.True(t, rawHit(evs, cc.CovListing))

	evs = validate(t, s, cc.KindCheckpointRegistry, req, list(&clusterpb.QueryCheckpointInfo{CheckpointId: 1, MaxSequence: maxSeq + 1}), nil)
	require.Equal(t, []oc.EventID{cc.EvListSequenceOutside}, testutil.IDs(testutil.Findings(evs)))
	evs = validate(t, s, cc.KindCheckpointRegistry, req, list(), nil)
	require.Equal(t, []oc.EventID{cc.EvListOutsideModel}, testutil.IDs(testutil.Findings(evs)))
	evs = validate(t, s, cc.KindCheckpointRegistry, req, list(&clusterpb.QueryCheckpointInfo{CheckpointId: 1, MaxSequence: maxSeq}, &clusterpb.QueryCheckpointInfo{CheckpointId: 2}), nil)
	require.Equal(t, []oc.EventID{cc.EvListOutsideModel}, testutil.IDs(testutil.Findings(evs)))
	evs = validate(t, s, cc.KindCheckpointRegistry, req, oc.Outcome{Code: "Internal"}, nil)
	require.Equal(t, []oc.EventID{cc.EvListUnexpectedError}, testutil.IDs(testutil.Findings(evs)))

	// An in-flight delete: either registry is legal.
	del := s.Begin(false, deleteCheckpoint(1))
	require.Empty(t, testutil.Findings(validate(t, s, cc.KindCheckpointRegistry, req, list(), nil)))
	_, evs = s.End(del)
	require.Empty(t, testutil.Findings(evs))

	sreq := &clusterpb.GetQueryCheckpointScheduleRequest{}
	sched := func(cron string) oc.Outcome {
		return okOut(t, &clusterpb.GetQueryCheckpointScheduleResponse{Cron: cron})
	}
	evs = validate(t, s, cc.KindCheckpointSchedule, sreq, sched(""), nil)
	require.Empty(t, testutil.Findings(evs))
	require.True(t, rawHit(evs, cc.CovScheduleRead))
	evs = write(t, s, setSchedule("@every 876000h"))
	require.True(t, rawHit(evs, cc.CovScheduleSet))
	require.Empty(t, testutil.Findings(validate(t, s, cc.KindCheckpointSchedule, sreq, sched("@every 876000h"), nil)))
	evs = validate(t, s, cc.KindCheckpointSchedule, sreq, sched(""), nil)
	require.Equal(t, []oc.EventID{cc.EvScheduleOutsideModel}, testutil.IDs(testutil.Findings(evs)))
	evs = validate(t, s, cc.KindCheckpointSchedule, sreq, oc.Outcome{Code: "FailedPrecondition"}, nil)
	require.Equal(t, []oc.EventID{cc.EvScheduleUnexpectedError}, testutil.IDs(testutil.Findings(evs)))
}

func TestLimitCoverageAndView(t *testing.T) {
	t.Parallel()
	s := testutil.NewSession("")
	write(t, s, testutil.CreateLedgerReq(s.Ledger(1)))
	for range 10 {
		write(t, s, testutil.CreateCheckpointReq())
	}
	tk := s.Begin(false, testutil.CreateCheckpointReq())
	_, evs, ref := s.T.EndWrite(tk, testutil.Fail("FailedPrecondition", "CHECKPOINT_LIMIT_REACHED"))
	require.Nil(t, ref)
	require.Empty(t, testutil.Findings(evs))
	require.True(t, rawHit(evs, cc.CovLimit))

	write(t, s, deleteCheckpoint(3))
	pick := uint64(19) // 9 live + 1 deleted: 19 mod 10 = 9, the deleted one
	req, _ := json.Marshal(map[string]any{"name": cc.ViewCheckpoints, "pick": pick, "cpLedgerPick": 0, "table": "volumes", "probe": "t-0:0", "seenPick": 0})
	views, err := s.T.ComputeViews(s.T.Model(), []json.RawMessage{req})
	require.NoError(t, err)
	var v struct {
		Live, Deleted []string
		NextID        string `json:"nextId"`
		Limit         uint64
		Target        struct {
			ID      string
			Deleted bool
			Ledger  *string
			Seen    string `json:"seenLedger"`
		}
	}
	require.NoError(t, json.Unmarshal(views[0], &v))
	require.Len(t, v.Live, 9)
	require.Equal(t, []string{"3"}, v.Deleted)
	require.Equal(t, "11", v.NextID)
	require.EqualValues(t, 10, v.Limit)
	require.Equal(t, "3", v.Target.ID)
	require.True(t, v.Target.Deleted)
	require.NotNil(t, v.Target.Ledger)
	require.Equal(t, s.Ledger(1), *v.Target.Ledger)
	require.Equal(t, s.Ledger(1), v.Target.Seen, "the ListLedgers cursor pool: every tracked name seen")
}
