package quietcheck_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/domain/indexes"
	"github.com/formancehq/ledger/v3/internal/proto/auditpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	oc "github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck"
	qc "github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck/quietcheck"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck/testutil"
	"github.com/formancehq/ledger/v3/tests/oracle"
	"github.com/formancehq/ledger/v3/tests/oracle/oracletest"
)

func pqCreate(ledger, name string, target commonpb.QueryTarget) *servicepb.Request {
	return &servicepb.Request{Type: &servicepb.Request_CreatePreparedQuery{CreatePreparedQuery: &servicepb.CreatePreparedQueryRequest{
		Ledger: ledger, Query: &commonpb.PreparedQuery{Name: name, Target: target},
	}}}
}

func pqUpdateAddress(ledger, name, prefix string) *servicepb.Request {
	return &servicepb.Request{Type: &servicepb.Request_UpdatePreparedQuery{UpdatePreparedQuery: &servicepb.UpdatePreparedQueryRequest{
		Ledger: ledger, Name: name,
		Filter: &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Address{Address: &commonpb.AddressMatch{
			Match: &commonpb.AddressMatch_HardcodedPrefix{HardcodedPrefix: prefix},
		}}},
	}}}
}

// session is the scripted session of the replay tests.
type session struct {
	create, fund, move, insufficient, admission, uncommitted oc.WriteRecord
	conflicts                                                []oc.WriteRecord
}

func script(t *testing.T, h *harness) session {
	t.Helper()
	l := h.ledger(1)
	var s session
	s.create = h.write(false, testutil.CreateLedgerReq(l))
	s.fund = h.write(true, testutil.TxReq(l, "", "world", "t-1:a", "COIN", 100))
	s.move = h.write(false, testutil.TxReq(l, "", "t-1:a", "t-1:b", "COIN", 30))
	s.insufficient = h.write(true, testutil.TxReq(l, "", "t-1:b", "t-1:c", "COIN", 1000))
	require.Equal(t, oc.WFailed, s.insufficient.Status)
	require.Equal(t, oracle.StageFSM, s.insufficient.Result.Stage)
	s.admission = h.write(true, testutil.EmptyTxReq(l))
	require.Equal(t, oracle.StageAdmission, s.admission.Result.Stage)
	h.write(false, pqCreate(l, "q", commonpb.QueryTarget_QUERY_TARGET_LOGS))
	s.uncommitted = h.write(false, pqUpdateAddress(l, "q", "a:"))
	require.Equal(t, domain.ErrReasonFilterCompilation, s.uncommitted.Outcome.Reason)
	for n := 1; n <= 3; n++ {
		s.conflicts = append(s.conflicts, h.conflict(s.fund, n, testutil.TxReq(l, "", "world", "t-1:a", "COIN", uint64(100+n))))
		require.Equal(t, "IDEMPOTENCY_KEY_CONFLICT", s.conflicts[n-1].Outcome.Reason)
	}
	// Foreign proposals interleave.
	h.appendEntry(&auditpb.AuditEntry{OrderCount: 1, Ledgers: []string{"r-ns-x-1"}, Idempotency: &commonpb.Idempotency{Key: "r-x-k1"},
		Outcome: &auditpb.AuditEntry_Success{Success: &auditpb.AuditSuccess{MinLogSequence: 9000, MaxLogSequence: 9000}}})
	h.appendEntry(&auditpb.AuditEntry{OrderCount: 1, Ledgers: []string{"default"},
		Outcome: &auditpb.AuditEntry_Success{Success: &auditpb.AuditSuccess{MinLogSequence: 9001, MaxLogSequence: 9001}}})
	h.write(true, testutil.AccountMetaReq(l, "t-1:a", "k0", "v"))

	return s
}

func TestAuditReplay_CleanSessionVerifies(t *testing.T) {
	t.Parallel()
	for _, chunk := range []int{1, 3, 500} {
		h := newHarness(t)
		script(t, h)
		evs, next := h.quiet(chunk)
		require.Empty(t, ids(evs), "chunk %d", chunk)
		require.True(t, reached(evs, qc.EvAuditReplayVerified))
		require.False(t, next.Resync)
		require.Equal(t, h.s.T.AuditCursor(), next.Cursor)
		require.EqualValues(t, len(h.trail), mustUint(t, next.Cursor))
		require.Empty(t, h.s.T.HeldResponses(), "every decided commit's response is released")
	}
}

func mustJSON(t *testing.T, e *auditpb.AuditEntry) []byte {
	t.Helper()
	b, err := protojson.Marshal(e)
	require.NoError(t, err)

	return b
}

func mustUint(t *testing.T, s string) uint64 {
	t.Helper()
	var v uint64
	for _, c := range s {
		require.True(t, c >= '0' && c <= '9', s)
		v = v*10 + uint64(c-'0')
	}

	return v
}

// entryIndex finds the trail entry of a key (the first one).
func entryIndex(t *testing.T, h *harness, key string) int {
	t.Helper()
	for i, e := range h.trail {
		if e.GetIdempotency().GetKey() == key {
			return i
		}
	}
	t.Fatalf("no entry for %s", key)

	return -1
}

func renumber(trail []*auditpb.AuditEntry) {
	for i, e := range trail {
		e.Sequence = uint64(i + 1)
	}
}

func TestAuditReplay_Mutations(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name   string
		mutate func(t *testing.T, h *harness, s session)
		want   oc.EventID
	}{
		{"drop an acknowledged success", func(t *testing.T, h *harness, s session) {
			i := entryIndex(t, h, s.move.Key)
			h.trail = append(h.trail[:i], h.trail[i+1:]...)
		}, qc.EvAuditMissingSuccess},
		{"drop a frozen failure", func(t *testing.T, h *harness, s session) {
			i := entryIndex(t, h, s.insufficient.Key)
			h.trail = append(h.trail[:i], h.trail[i+1:]...)
		}, qc.EvAuditMissingFailure},
		{"fewer conflict entries than conflicts", func(t *testing.T, h *harness, s session) {
			// Three conflicts sent 1+2+3 attempts; keep only two entries.
			var kept []*auditpb.AuditEntry
			seen := 0
			for _, e := range h.trail {
				if e.GetIdempotency().GetKey() == s.fund.Key && e.GetFailure() != nil {
					if seen++; seen > 2 {
						continue
					}
				}
				kept = append(kept, e)
			}
			h.trail = kept
		}, qc.EvAuditMissingFailure},
		{"swap two dependent entries", func(t *testing.T, h *harness, s session) {
			i, j := entryIndex(t, h, s.fund.Key), entryIndex(t, h, s.move.Key)
			h.trail[i], h.trail[j] = h.trail[j], h.trail[i]
		}, qc.EvAuditUnexplainedEntry},
		{"change a failure reason", func(t *testing.T, h *harness, s session) {
			i := entryIndex(t, h, s.insufficient.Key)
			h.trail[i].Outcome = &auditpb.AuditEntry_Failure{Failure: &auditpb.AuditFailure{Details: failureDetails(t, domain.ErrReasonAccountTypeNotFound)}}
		}, qc.EvAuditUnexplainedEntry},
		{"change a success range", func(t *testing.T, h *harness, s session) {
			i := entryIndex(t, h, s.move.Key)
			h.trail[i].GetSuccess().MaxLogSequence++
		}, qc.EvAuditUnexplainedEntry},
		{"audit an admission rejection", func(t *testing.T, h *harness, s session) {
			h.failureEntry(s.admission.Key, s.admission.Outcome.Reason, s.admission.Requests)
		}, qc.EvAuditAdmissionEntry},
		{"a foreign proposal on a tracked ledger", func(t *testing.T, h *harness, s session) {
			h.appendEntry(&auditpb.AuditEntry{OrderCount: 1, Ledgers: []string{h.ledger(1)},
				Outcome: &auditpb.AuditEntry_Success{Success: &auditpb.AuditSuccess{MinLogSequence: 9100, MaxLogSequence: 9100}}})
		}, qc.EvAuditForeignOnTracked},
		{"an unknown tracked key", func(t *testing.T, h *harness, s session) {
			h.failureEntry(h.s.Prefix+"k9999", domain.ErrReasonInsufficientFunds, s.move.Requests)
		}, qc.EvAuditUnexplainedEntry},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			s := script(t, h)
			c.mutate(t, h, s)
			renumber(h.trail)
			evs, _ := h.quiet(4)
			require.Contains(t, ids(evs), c.want)
			require.False(t, reached(evs, qc.EvAuditReplayVerified))
		})
	}
}

// The uncommitted reasons the model can return (FILTER_COMPILATION_ERROR,
// CLUSTER_POLICY_INVALID, STALE_CLUSTER_POLICY) have no FailureDetails arm:
// the server cannot even encode an audit failure entry with one of them. The
// EvAuditUncommittedEntry check is kept as a defence; this test pins the
// representability argument so a new arm makes it testable (and tested).
func TestAuditReplay_UncommittedReasonsHaveNoAuditArm(t *testing.T) {
	t.Parallel()
	d := &commonpb.FailureDetails{}
	oneof := d.ProtoReflect().Descriptor().Oneofs().ByName("kind")
	arms := map[commonpb.ErrorReason]bool{}
	for i := range oneof.Fields().Len() {
		f := oneof.Fields().Get(i)
		c := &commonpb.FailureDetails{}
		c.ProtoReflect().Set(f, protoreflect.ValueOfMessage(c.ProtoReflect().NewField(f).Message()))
		arms[commonpb.FailureDetailsReason(c)] = true
	}
	for _, r := range []string{domain.ErrReasonFilterCompilation, domain.ErrReasonClusterPolicyInvalid, domain.ErrReasonStaleClusterPolicy} {
		require.True(t, oracle.UncommittedReasons[r])
		require.False(t, arms[domain.ReasonCode(r)], "%s became auditable: add an uncommitted-entry mutation test", r)
	}
	// Every auditable reason is definitive, so no audited failure is of a
	// non-freezable kind on the wire either.
	for r := range arms {
		require.True(t, oc.IsDefinitiveReason(strings.TrimPrefix(r.String(), "ERROR_REASON_")), r.String())
	}
}

func TestAuditReplay_StateDiffersFromModel(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	s := script(t, h)
	// The trail lacks a commit the online model drained: the replay ends in
	// another business state.
	i := entryIndex(t, h, s.move.Key)
	h.trail = append(h.trail[:i], h.trail[i+1:]...)
	renumber(h.trail)
	evs, _ := h.quiet(500)
	require.Contains(t, ids(evs), qc.EvAuditReplayStateDiffers)
	// Reported once: the next quiet period continues from the online model.
	evs, _ = h.quiet(500)
	require.Empty(t, ids(evs))
	require.True(t, reached(evs, qc.EvAuditReplayVerified))
}

// Observation state learned online — an index promoted, a retype window
// closed, stamps learned from responses — is not replayed, and the business
// fingerprints still agree.
func TestAuditReplay_ObservationStateIgnored(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	l := h.ledger(1)
	h.write(false, testutil.CreateLedgerReq(l))
	h.write(true, onLedger(oracletest.SetFieldTypeReq(commonpb.TargetType_TARGET_TYPE_ACCOUNT, "k", commonpb.MetadataType_METADATA_TYPE_STRING), l))
	id := indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_ACCOUNT, "k")
	canonical := indexes.Canonical(id)
	h.write(true, onLedger(oracletest.CreateIndexReq(id), l))
	h.write(false, testutil.TxReq(l, "", "world", "t-1:a", "COIN", 5))
	h.s.T.MutateModel("index", func(g *oracle.GlobalState) { g.SetIndexActive(l, canonical) })
	_, active := h.s.T.Model().Ledger(l).IndexState(canonical)
	require.True(t, active)
	h.write(false, onLedger(oracletest.SetFieldTypeReq(commonpb.TargetType_TARGET_TYPE_ACCOUNT, "k", commonpb.MetadataType_METADATA_TYPE_INT64), l))
	_, open := h.s.T.Model().Ledger(l).RetypeWindow(canonical)
	require.True(t, open)
	h.s.T.MutateModel("retype", func(g *oracle.GlobalState) { g.CloseRetypeWindow(l, canonical) })
	require.NotEqual(t, h.s.T.Model().Fingerprint(), h.s.Server.Fingerprint(), "the online model learned observation state")

	evs, _ := h.quiet(2)
	require.Empty(t, ids(evs))
	require.True(t, reached(evs, qc.EvAuditReplayVerified))
}

func TestAuditReplay_IncrementalAcrossEpochs(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	script(t, h)
	evs, next := h.quiet(500)
	require.Empty(t, ids(evs))
	first := mustUint(t, next.Cursor)
	require.EqualValues(t, len(h.trail), first)

	l := h.ledger(1)
	h.write(true, testutil.TxReq(l, "", "world", "t-1:e", "COIN", 3))
	h.write(false, testutil.TxReq(l, "", "t-1:e", "t-1:f", "COIN", 2))
	require.Len(t, h.s.T.HeldResponses(), 2)
	evs, next = h.quiet(1)
	require.Empty(t, ids(evs))
	require.True(t, reached(evs, qc.EvAuditReplayVerified))
	require.EqualValues(t, first+2, mustUint(t, next.Cursor))
	require.Empty(t, h.s.T.HeldResponses())

}

func TestAuditReplay_PartialChunkThenComplete(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	script(t, h)
	h.epoch++
	_, ref := h.s.T.BeginQuiet(h.epoch)
	require.Nil(t, ref)
	// Only the first half, not complete: no completeness verdict.
	body := qc.AuditBody{Head: "0"}
	for _, e := range h.trail[:len(h.trail)/2] {
		body.Entries = append(body.Entries, mustJSON(t, e))
	}
	evs, next := h.section(qc.SectionAudit, 0, body)
	require.Empty(t, ids(evs))
	require.False(t, reached(evs, qc.EvAuditReplayVerified))
	require.EqualValues(t, len(h.trail)/2, mustUint(t, next.(qc.AuditNext).Cursor))
	h.s.T.EndQuiet(h.epoch)
	// The next period resumes from the cursor.
	evs, _ = h.quiet(500)
	require.Empty(t, ids(evs))
	require.True(t, reached(evs, qc.EvAuditReplayVerified))
}

// The FSM numbers audit entries without gaps: a listing that skips one —
// first entry after the cursor, inside a chunk, or across chunks — is a
// finding, and the epoch is not verified.
func TestAuditReplay_SequenceGap(t *testing.T) {
	t.Parallel()
	for name, drop := range map[string]func(h *harness) int{
		"the first entry":  func(*harness) int { return 0 },
		"a foreign entry":  func(h *harness) int { return entryIndex(t, h, "r-x-k1") },
		"the last but one": func(h *harness) int { return len(h.trail) - 2 },
	} {
		for _, chunk := range []int{1, 3, 500} {
			h := newHarness(t)
			script(t, h)
			i := drop(h)
			h.trail = append(h.trail[:i], h.trail[i+1:]...) // not renumbered
			evs, _ := h.quiet(chunk)
			require.Contains(t, ids(evs), qc.EvAuditSequenceGap, "%s, chunk %d", name, chunk)
			require.False(t, reached(evs, qc.EvAuditReplayVerified))
		}
	}
}

// A tracked commit missing from the trail is reported once and its response
// released with the rest: it must not pin every later response.
func TestAuditReplay_MissingEntryDoesNotPinResponses(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	s := script(t, h)
	i := entryIndex(t, h, s.move.Key)
	h.trail = append(h.trail[:i], h.trail[i+1:]...)
	renumber(h.trail)
	evs, _ := h.quiet(500)
	require.Contains(t, ids(evs), qc.EvAuditMissingSuccess)
	require.Empty(t, h.s.T.HeldResponses(), "settled records release their responses, reported or not")

	l := h.ledger(1)
	h.write(true, testutil.TxReq(l, "", "world", "t-1:g", "COIN", 3))
	require.Len(t, h.s.T.HeldResponses(), 1)
	evs, _ = h.quiet(500)
	require.Empty(t, ids(evs))
	require.True(t, reached(evs, qc.EvAuditReplayVerified))
	require.Empty(t, h.s.T.HeldResponses())
}

// Surplus conflict entries (several attempts of one conflict) do not carry
// over to a later quiet period, where they would cover a later conflict on
// the same key whose entry is missing.
func TestAuditReplay_ConflictSurplusDoesNotCarryOver(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	l := h.ledger(1)
	h.write(false, testutil.CreateLedgerReq(l))
	orig := h.write(true, testutil.TxReq(l, "", "world", "t-1:a", "COIN", 100))
	h.conflict(orig, 3, testutil.TxReq(l, "", "world", "t-1:a", "COIN", 101))
	evs, _ := h.quiet(500)
	require.Empty(t, ids(evs))
	require.True(t, reached(evs, qc.EvAuditReplayVerified))

	h.conflict(orig, 1, testutil.TxReq(l, "", "world", "t-1:a", "COIN", 102))
	h.trail = h.trail[:len(h.trail)-1] // its only entry is lost
	evs, _ = h.quiet(500)
	require.Contains(t, ids(evs), qc.EvAuditMissingFailure)
}

// The per-epoch state is keyed by the tracker's quiet-period count, not only
// the driver's epoch number: a reused epoch number (a restarted driver
// starting again at 1) still gets a verdict.
func TestAuditReplay_ReusedEpochNumber(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	script(t, h)
	evs, _ := h.quiet(500)
	require.True(t, reached(evs, qc.EvAuditReplayVerified))

	h.write(true, testutil.TxReq(h.ledger(1), "", "world", "t-1:h", "COIN", 3))
	h.epoch = 0 // the next quiet period is epoch 1 again
	evs, _ = h.quiet(500)
	require.Empty(t, ids(evs))
	require.True(t, reached(evs, qc.EvAuditReplayVerified))
	require.Empty(t, h.s.T.HeldResponses())
}
