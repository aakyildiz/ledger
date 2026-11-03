package applysurfacecheck_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	oc "github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck"
	txcheck "github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck/applysurface"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck/testutil"
	"github.com/formancehq/ledger/v3/tests/oracle"
)

const refConflict = commonpb.ErrorReason_ERROR_REASON_TRANSACTION_REFERENCE_CONFLICT

func withSkip(r *servicepb.Request) *servicepb.Request {
	r.GetApply().SkippableReasons = []commonpb.ErrorReason{refConflict}

	return r
}

// A state with ledger L holding transaction 1 under reference "r".
func seeded(t *testing.T) oracle.GlobalState {
	t.Helper()
	g := oracle.NewGlobalState().WithQueryCheckpointLimit(10)
	for _, r := range []*servicepb.Request{testutil.CreateLedgerReq("L"), testutil.TxReq("L", "r", "world", "users:1", "COIN", 5)} {
		res := g.Apply(oracle.Bulk{Requests: []*servicepb.Request{r}})
		require.True(t, res.OK, res.Reason)
		g = res.State
	}

	return g
}

func drain(t *testing.T, before oracle.GlobalState, reqs ...*servicepb.Request) []oc.Event {
	t.Helper()
	rec := oc.WriteRecord{Wid: "w1", Key: "k1", Family: "tx", Requests: reqs, Bulk: oracle.Bulk{Requests: reqs, IdempotencyKey: "k1"}}
	resp := testutil.SimulateCommit(before, rec.Bulk, 10, 1_700_000_000_000_000)
	res := before.Apply(rec.Bulk)
	require.True(t, res.OK, res.Reason)

	return txcheck.ApplySurfaceDrain(nil, rec, resp, before, res.State)
}

func TestApplySurface_SkipThenSuccessorHits(t *testing.T) {
	t.Parallel()
	g := seeded(t)
	evs := drain(t, g, withSkip(testutil.TxReq("L", "r", "world", "users:2", "COIN", 1)), testutil.AccountMetaReq("L", "t-1:3", "k0", "v"))
	require.Len(t, evs, 1)
	require.Equal(t, txcheck.EvSkipRefConflictContinued, evs[0].ID)
	require.Equal(t, oc.EvRaw, evs[0].Kind)
	require.True(t, evs[0].Cond)
	require.True(t, oc.IsCoverageID(txcheck.EvSkipRefConflictContinued))
}

func TestApplySurface_TrailingSkipDoesNotHit(t *testing.T) {
	t.Parallel()
	g := seeded(t)
	evs := drain(t, g, withSkip(testutil.TxReq("L", "r", "world", "users:2", "COIN", 1)))
	require.Len(t, evs, 1)
	require.False(t, evs[0].Cond, "a skip without a successor proves nothing about continuing")
}

func TestApplySurface_FreshReferenceOptInDoesNotHit(t *testing.T) {
	t.Parallel()
	g := seeded(t)
	// The opt-in on a fresh reference never fires: no skip at all.
	evs := drain(t, g, withSkip(testutil.TxReq("L", "fresh", "world", "users:2", "COIN", 1)), testutil.AccountMetaReq("L", "t-1:3", "k0", "v"))
	require.Len(t, evs, 1)
	require.False(t, evs[0].Cond)
}

func TestApplySurface_NoOptInNoEvent(t *testing.T) {
	t.Parallel()
	g := seeded(t)
	require.Empty(t, drain(t, g, testutil.TxReq("L", "fresh", "world", "users:2", "COIN", 1)))
}

func TestGeneratedSkippableReason(t *testing.T) {
	t.Parallel()
	r, ok := txcheck.GeneratedSkippableReason(testutil.TxReq("L", "", "world", "a:1", "COIN", 1))
	require.True(t, ok)
	require.Equal(t, refConflict, r)
	r, ok = txcheck.GeneratedSkippableReason(testutil.RevertReq("L", 1))
	require.True(t, ok)
	require.Equal(t, commonpb.ErrorReason_ERROR_REASON_TRANSACTION_ALREADY_REVERTED, r)
	mode := &servicepb.Request{Type: &servicepb.Request_Apply{Apply: &servicepb.LedgerApplyRequest{Ledger: "L", Action: &servicepb.LedgerAction{
		Data: &servicepb.LedgerAction_SetDefaultEnforcementMode{SetDefaultEnforcementMode: &servicepb.SetDefaultEnforcementModeRequest{}},
	}}}}
	r, ok = txcheck.GeneratedSkippableReason(mode)
	require.True(t, ok, "mode setters get a deliberately disallowed reason")
	require.Equal(t, commonpb.ErrorReason_ERROR_REASON_ACCOUNT_TYPE_NOT_FOUND, r)
	_, ok = txcheck.AllowedSkippableReason(mode)
	require.False(t, ok)
	_, ok = txcheck.GeneratedSkippableReason(testutil.CreateLedgerReq("L"))
	require.False(t, ok)
}

func TestRejectionCoverage_OneReachablePerReason(t *testing.T) {
	t.Parallel()
	want := map[string]oc.EventID{
		"INSUFFICIENT_FUNDS":             txcheck.EvInsufficientFundsExercised,
		"TRANSACTION_REFERENCE_CONFLICT": txcheck.EvReferenceConflictExercised,
		"VOLUME_OVERFLOW":                txcheck.EvVolumeOverflowExercised,
		"VALIDATION":                     txcheck.EvValidationExercised,
		"TRANSIENT_ACCOUNT_NON_ZERO":     txcheck.EvTransientNonZeroExercised,
	}
	for reason, id := range want {
		evs := txcheck.RejectionCoverage(nil, oc.WriteRecord{Wid: "w"}, oracle.ApplyResult{}, reason)
		require.Len(t, evs, 1, reason)
		require.Equal(t, id, evs[0].ID)
		require.Equal(t, oc.EvReachable, evs[0].Kind)
	}
	require.Empty(t, txcheck.RejectionCoverage(nil, oc.WriteRecord{}, oracle.ApplyResult{}, "LEDGER_NOT_FOUND"))
}

// End to end through a scripted Tracker session: an explained reference
// conflict runs the failure hook; a skip + successor commit runs the drain hook.
func TestHooksRunThroughTheTracker(t *testing.T) {
	t.Parallel()
	s := testutil.NewSession("")
	l := s.Ledger(1)
	require.Empty(t, testutil.Findings(s.Write(false, testutil.CreateLedgerReq(l))))
	require.Empty(t, testutil.Findings(s.Write(false, testutil.TxReq(l, "o-test-ref1", "world", "t-0:1", "COIN", 3))))

	dup := s.Write(true, testutil.TxReq(l, "o-test-ref1", "world", "t-0:2", "COIN", 1))
	require.Empty(t, testutil.Findings(dup))
	require.Contains(t, testutil.IDs(dup), txcheck.EvReferenceConflictExercised)

	skip := s.Write(false, withSkip(testutil.TxReq(l, "o-test-ref1", "world", "t-0:2", "COIN", 1)), testutil.AccountMetaReq(l, "t-0:3", "k0", "v"))
	require.Empty(t, testutil.Findings(skip))
	var raw []oc.Event
	for _, e := range skip {
		if e.ID == txcheck.EvSkipRefConflictContinued {
			raw = append(raw, e)
		}
	}
	require.Len(t, raw, 1)
	require.True(t, raw[0].Cond)
}
