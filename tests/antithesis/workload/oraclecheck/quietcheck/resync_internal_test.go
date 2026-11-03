package quietcheck

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/formancehq/ledger/v3/internal/proto/auditpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	oc "github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck/testutil"
)

func successEntry(t *testing.T, tr *oc.Tracker, rec oc.WriteRecord, seq uint64) json.RawMessage {
	t.Helper()
	var r oc.CommittedBulk
	for _, b := range tr.CommittedBulks() {
		if b.Wid == rec.Wid {
			r = b
		}
	}
	require.NotZero(t, r.MinSeq)
	e := &auditpb.AuditEntry{Sequence: seq, OrderCount: uint32(len(rec.Requests)), Ledgers: []string{"o-test-l1"},
		Idempotency: &commonpb.Idempotency{Key: rec.Key},
		Outcome:     &auditpb.AuditEntry_Success{Success: &auditpb.AuditSuccess{MinLogSequence: r.MinSeq, MaxLogSequence: r.MaxSeq}}}
	b, err := protojson.Marshal(e)
	require.NoError(t, err)

	return b
}

func post(t *testing.T, tr *oc.Tracker, epoch, seq uint64, body AuditBody) (AuditNext, []oc.Event) {
	t.Helper()
	raw, err := json.Marshal(body)
	require.NoError(t, err)
	next, evs, ref := auditSection(tr, epoch, seq, raw)
	require.Nil(t, ref)

	return next.(AuditNext), evs
}

// Resync (DESIGN §5.5): the first chunk re-seeds the replay from the model at
// the head the driver saw, answers resync, and judges nothing this epoch; the
// next epoch replays from the head.
func TestAuditReplay_Resync(t *testing.T) {
	t.Parallel()
	s := testutil.NewSession("")
	l := s.Ledger(1)
	require.Empty(t, testutil.Findings(s.Write(false, testutil.CreateLedgerReq(l))))
	require.Empty(t, testutil.Findings(s.Write(true, testutil.TxReq(l, "", "world", "t-1:a", "COIN", 5))))
	require.NotEmpty(t, s.T.HeldResponses())

	_, ref := s.T.BeginQuiet(1)
	require.Nil(t, ref)
	load(s.T, 1).forceResync = true
	// The trail the driver pages holds entries the replay never sees (the
	// skipped interval), with head 40.
	next, evs := post(t, s.T, 1, 0, AuditBody{Head: "40", Entries: []json.RawMessage{json.RawMessage(`{"sequence":"1"}`)}})
	require.True(t, next.Resync)
	require.Equal(t, "40", next.Cursor)
	require.Equal(t, &SkippedRange{From: "0", To: "40"}, next.Skipped, "the skipped interval is reported")
	require.Empty(t, evs, "a resync is not a finding")
	require.Equal(t, "40", s.T.AuditCursor())
	require.Empty(t, s.T.HeldResponses())
	// Later chunks of the epoch are ignored.
	next, evs = post(t, s.T, 1, 1, AuditBody{Complete: true})
	require.True(t, next.Resync)
	require.Empty(t, evs)
	s.T.EndQuiet(1)

	// The next epoch: only entries after the head, and completeness covers
	// only what ended after the resync.
	tk := s.Begin(true, testutil.TxReq(l, "", "world", "t-1:b", "COIN", 6))
	s.End(tk)
	rec, _ := s.T.RecordByTicket(tk)
	_, ref = s.T.BeginQuiet(2)
	require.Nil(t, ref)
	next, evs = post(t, s.T, 2, 0, AuditBody{Head: "41", Complete: true, Entries: []json.RawMessage{successEntry(t, s.T, rec, 41)}})
	require.False(t, next.Resync)
	require.Equal(t, "41", next.Cursor)
	require.Empty(t, testutil.Findings(evs))
	require.Contains(t, testutil.IDs(evs), EvAuditReplayVerified)
	s.T.EndQuiet(2)

	// Without the entry, the acknowledged write would be missing.
	tk = s.Begin(true, testutil.TxReq(l, "", "world", "t-1:c", "COIN", 7))
	s.End(tk)
	_, ref = s.T.BeginQuiet(3)
	require.Nil(t, ref)
	_, evs = post(t, s.T, 3, 0, AuditBody{Head: "41", Complete: true})
	require.Contains(t, testutil.IDs(testutil.Findings(evs)), EvAuditMissingSuccess)
}
