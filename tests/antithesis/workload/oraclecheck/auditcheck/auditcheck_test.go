package auditcheck

import (
	"encoding/json"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"

	"github.com/formancehq/ledger/v3/internal/proto/auditpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/pkg/pagecursor"
	oc "github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck/match"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck/match/matchtest"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck/testutil"
)

// ---- pure-model helpers (the shapes of M/audit_test.go) ----

const (
	tPrefix = "o-test-"
	tLedger = "o-test-l1"
	tKey    = "o-test-k"
)

// metadataLogs is one account-metadata save per sequence on the tracked
// ledger.
func metadataLogs(sequences ...uint64) map[uint64]match.CommittedLog {
	out := make(map[uint64]match.CommittedLog, len(sequences))
	for i, seq := range sequences {
		out[seq] = match.CommittedLog{Ledger: tLedger, ID: uint64(i + 1), Kind: "saved_metadata"}
	}

	return out
}

func singleOrderBulks(sequences ...uint64) map[uint64]match.CommittedBulk {
	out := make(map[uint64]match.CommittedBulk, len(sequences))
	for _, seq := range sequences {
		out[seq] = match.CommittedBulk{MinSeq: seq, MaxSeq: seq, Orders: 1}
	}

	return out
}

func testModel(logSeqs []uint64, bulkSeqs []uint64) Model {
	logs := metadataLogs(logSeqs...)
	var committedMax uint64
	for s := range logs {
		committedMax = max(committedMax, s)
	}

	return Model{
		LedgerPrefix: tPrefix, KeyPrefix: tKey, Logs: logs, CommittedMax: committedMax,
		Bulks: singleOrderBulks(bulkSeqs...), Rejections: map[oc.RejectedBulk]struct{}{},
		KeyByFirstSeq: map[uint64]string{}, Outstanding: func(string) bool { return false },
		FrozenRejected: func(string) bool { return false },
	}
}

// ok is a tracked success entry at audit sequence seq naming log logSeq.
func ok(seq, logSeq uint64) match.AuditEntry {
	return match.AuditEntry{Seq: seq, IdemKey: tKey + u(seq), Ledgers: []string{tLedger}, OrderCount: 1, MinLog: logSeq, MaxLog: logSeq}
}

func page(entries []match.AuditEntry, filter *commonpb.QueryFilter, reverse bool, size int, after uint64, next string) Page {
	return Page{Entries: entries, Filter: filter, Reverse: reverse, PageSize: size, AfterSeq: after, Next: next}
}

func findingID(v Verdict) oc.EventID {
	if v.Finding == nil {
		return ""
	}

	return v.Finding.ID
}

// ---- ported M/audit_test.go cases ----

func TestRejectionExplainsMatchesLedgersOrdersAndReason(t *testing.T) {
	t.Parallel()
	m := testModel(nil, nil)
	m.Rejections = map[oc.RejectedBulk]struct{}{{Ledgers: "a,b", Orders: 2, Reason: "INSUFFICIENT_FUNDS"}: {}}

	require.True(t, m.rejectionExplains(match.AuditEntry{Failed: true, Ledgers: []string{"a", "b"}, OrderCount: 2, Reason: "INSUFFICIENT_FUNDS"}))
	require.True(t, m.rejectionExplains(match.AuditEntry{Failed: true, Ledgers: []string{"a", "b"}, OrderCount: 2, Reason: "UNSPECIFIED"}), "a reason the wire does not name still matches on ledgers and orders")
	require.False(t, m.rejectionExplains(match.AuditEntry{Failed: true, Ledgers: []string{"a"}, OrderCount: 2, Reason: "INSUFFICIENT_FUNDS"}))
	require.False(t, m.rejectionExplains(match.AuditEntry{Failed: true, Ledgers: []string{"a", "b"}, OrderCount: 1, Reason: "INSUFFICIENT_FUNDS"}))
	require.False(t, m.rejectionExplains(match.AuditEntry{Failed: true, Ledgers: []string{"a", "b"}, OrderCount: 2, Reason: "VALIDATION"}))
}

func TestValidatePageKnownEntriesAreStillOwed(t *testing.T) {
	t.Parallel()
	m := testModel([]uint64{42, 43, 44}, []uint64{42, 43, 44})
	e42, e43, e44 := ok(3, 42), ok(4, 43), ok(5, 44)
	st := &state{known: map[uint64]knownEntry{}}

	// Nothing remembered yet: every shape of page is admissible.
	require.Empty(t, findingID(ValidatePage(m, st.knownBefore(10), page(nil, nil, false, 5, 0, ""))))

	st.rememberKnown([]match.AuditEntry{e42, e43, e44}, 10)

	// A read that began before the entries were learned may have been served
	// from a snapshot older than them.
	require.Empty(t, findingID(ValidatePage(m, st.knownBefore(10), page(nil, nil, false, 5, 0, ""))))

	known := st.knownBefore(11)
	require.Equal(t, EvAuditStoppedShortOfKnown, findingID(ValidatePage(m, known, page(nil, nil, false, 5, 0, ""))), "an empty page cannot be the answer")
	require.Equal(t, EvAuditOmitsKnown, findingID(ValidatePage(m, known, page([]match.AuditEntry{e42, e44}, nil, false, 5, 0, ""))), "4 sits inside the page's own range")
	require.Equal(t, EvAuditStoppedShortOfKnown, findingID(ValidatePage(m, known, page([]match.AuditEntry{e42, e43}, nil, false, 5, 0, ""))), "the page had rows left")

	// A full page owes a cursor naming where to resume, not the rows themselves.
	require.Equal(t, EvAuditDroppedResumeCursor, findingID(ValidatePage(m, known, page([]match.AuditEntry{e42, e43}, nil, false, 2, 0, ""))))
	full := ValidatePage(m, known, page([]match.AuditEntry{e42, e43}, nil, false, 2, 0, "4"))
	require.Nil(t, full.Finding)
	require.True(t, full.CursorCorroborated, "entry 5 is what proved the cursor was owed")
	require.Equal(t, 3, full.KnownCertified)

	// Entries at or before the cursor are another page's business.
	require.Nil(t, ValidatePage(m, known, page([]match.AuditEntry{e44}, nil, false, 5, 4, "")).Finding)
	require.Nil(t, ValidatePage(m, known, page([]match.AuditEntry{e43, e42}, nil, true, 5, 5, "")).Finding)
	require.Equal(t, EvAuditStoppedShortOfKnown, findingID(ValidatePage(m, known, page([]match.AuditEntry{e44, e43}, nil, true, 5, 0, ""))), "3 is still below a descending page")
}

func TestValidatePageKnownEntriesMustCertainlyMatch(t *testing.T) {
	t.Parallel()
	m := testModel([]uint64{42}, []uint64{42})
	st := &state{known: map[uint64]knownEntry{}}
	st.rememberKnown([]match.AuditEntry{ok(3, 42)}, 1)
	known := st.knownBefore(2)

	addMeta := match.FilterAuditString(commonpb.AuditField_AUDIT_FIELD_ORDER_TYPE, "add_metadata")
	createTx := match.FilterAuditString(commonpb.AuditField_AUDIT_FIELD_ORDER_TYPE, "create_transaction")
	require.Equal(t, EvAuditStoppedShortOfKnown, findingID(ValidatePage(m, known, page(nil, addMeta, false, 5, 0, ""))))
	require.Nil(t, ValidatePage(m, known, page(nil, createTx, false, 5, 0, "")).Finding)
	require.Nil(t, ValidatePage(m, known, page(nil, match.FilterAuditString(commonpb.AuditField_AUDIT_FIELD_LEDGER, "other"), false, 5, 0, "")).Finding)

	// A system-scoped (or foreign) entry names no logs the model knows, so no
	// order type can be derived.
	st.rememberKnown([]match.AuditEntry{{Seq: 9, OrderCount: 1}}, 1)
	require.Nil(t, ValidatePage(m, st.knownBefore(2), page(nil, createTx, false, 5, 0, "")).Finding)
}

func TestValidatePageOrderTypesAndLogCoverage(t *testing.T) {
	t.Parallel()
	m := testModel([]uint64{42, 44}, []uint64{42, 43, 44})
	e42, e44 := ok(3, 42), ok(5, 44)

	addMeta := match.FilterAuditString(commonpb.AuditField_AUDIT_FIELD_ORDER_TYPE, "add_metadata")
	createTx := match.FilterAuditString(commonpb.AuditField_AUDIT_FIELD_ORDER_TYPE, "create_transaction")
	require.Nil(t, ValidatePage(m, nil, page([]match.AuditEntry{e42}, addMeta, false, 50, 0, "")).Finding)
	require.Equal(t, EvAuditOutsideOrderType, findingID(ValidatePage(m, nil, page([]match.AuditEntry{e42}, createTx, false, 50, 0, ""))))

	m.Logs[43] = match.CommittedLog{Ledger: tLedger, Kind: "saved_ledger_metadata"}
	e43 := ok(4, 43)
	require.Nil(t, ValidatePage(m, nil, page([]match.AuditEntry{e43}, match.FilterAuditString(commonpb.AuditField_AUDIT_FIELD_ORDER_TYPE, "save_ledger_metadata"), false, 50, 0, "")).Finding)
	require.Equal(t, EvAuditOutsideOrderType, findingID(ValidatePage(m, nil, page([]match.AuditEntry{e43}, match.FilterAuditString(commonpb.AuditField_AUDIT_FIELD_ORDER_TYPE, "delete_ledger_metadata"), false, 50, 0, ""))))

	lo, hi := uint64(40), uint64(50)
	logRange := match.FilterAuditUint(commonpb.AuditField_AUDIT_FIELD_LOG_SEQUENCE, &commonpb.UintCondition{Min: &lo, Max: &hi})
	require.Nil(t, ValidatePage(m, nil, page([]match.AuditEntry{e42, e43, e44}, logRange, false, 50, 0, "")).Finding)
	require.Equal(t, EvAuditMissesCommittedLog, findingID(ValidatePage(m, nil, page([]match.AuditEntry{e42, e44}, logRange, false, 50, 0, ""))), "43 is committed and in range")
	require.Nil(t, ValidatePage(m, nil, page([]match.AuditEntry{e42, e44}, logRange, false, 2, 0, "")).Finding, "a full page may have been cut before 43")

	// Foreign logs interleave the sequences: only learned tracked ones are owed.
	foreign := match.AuditEntry{Seq: 6, IdemKey: "r-ns-x-k1", Ledgers: []string{"r-ns-x-1"}, OrderCount: 1, MinLog: 45, MaxLog: 45}
	require.Nil(t, ValidatePage(m, nil, page([]match.AuditEntry{e42, e43, e44, foreign}, logRange, false, 50, 0, "")).Finding)
}

// The singleton's "audit tail misses the newest committed bulk" is not ported:
// a newer foreign entry or system order can be the real tail. The rest of
// TestValidateAuditPageReverseTail still holds.
func TestValidatePageReverseTailIsNotJudged(t *testing.T) {
	t.Parallel()
	m := testModel([]uint64{42, 44}, []uint64{42, 43, 44})
	m.Rejections = map[oc.RejectedBulk]struct{}{{Ledgers: tLedger, Orders: 1, Reason: "VALIDATION"}: {}}
	newest, older, hole := ok(5, 44), ok(3, 42), ok(4, 43)
	rejected := match.AuditEntry{Seq: 6, IdemKey: tKey + "6", Ledgers: []string{tLedger}, OrderCount: 1, Failed: true, Reason: "VALIDATION"}
	system := match.AuditEntry{Seq: 7}

	require.Nil(t, ValidatePage(m, nil, page([]match.AuditEntry{newest, older}, nil, true, 50, 0, "")).Finding)
	require.Equal(t, EvAuditOutsideModel, findingID(ValidatePage(m, nil, page([]match.AuditEntry{hole}, nil, true, 50, 0, ""))), "43 is no committed log")
	res := ValidatePage(m, nil, page([]match.AuditEntry{rejected, newest}, nil, true, 50, 0, ""))
	require.Nil(t, res.Finding)
	require.Equal(t, 1, res.Rejections)
	require.Nil(t, ValidatePage(m, nil, page([]match.AuditEntry{system, older}, nil, true, 50, 0, "")).Finding, "dropped: tail check")
}

func TestValidatePageLedgerLevelLogsAndOutstanding(t *testing.T) {
	t.Parallel()
	m := testModel([]uint64{42}, []uint64{42, 43})
	ledgerLevel := ok(8, 43)
	require.Equal(t, EvAuditNamesUncommittedLogs, findingID(ValidatePage(m, nil, page([]match.AuditEntry{ledgerLevel}, nil, false, 50, 0, ""))))
	got, _ := checkEntry(m, ledgerLevel, nil)
	require.Equal(t, EvAuditNamesUncommittedLogs, got.ID)

	// An undrained bulk under the entry's key may have committed it.
	m.Outstanding = func(key string) bool { return key == ledgerLevel.IdemKey }
	require.Nil(t, ValidatePage(m, nil, page([]match.AuditEntry{ledgerLevel}, nil, false, 50, 0, "")).Finding)

	m.Outstanding = func(string) bool { return false }
	m.Logs[43] = match.CommittedLog{Ledger: tLedger, Kind: "saved_ledger_metadata"}
	m.CommittedMax = 43
	require.Nil(t, ValidatePage(m, nil, page([]match.AuditEntry{ledgerLevel}, nil, false, 50, 0, "")).Finding, "a learned ledger-level sequence is a committed log")
}

// ---- foreign tolerance ----

func TestForeignEntriesInterleaveOnDenseZoneScans(t *testing.T) {
	t.Parallel()
	m := testModel([]uint64{42, 47}, []uint64{42, 47})
	foreignOK := match.AuditEntry{Seq: 4, IdemKey: "r-ns-1a-k3", Ledgers: []string{"r-ns-1a-1"}, OrderCount: 2, MinLog: 43, MaxLog: 44}
	foreignFail := match.AuditEntry{Seq: 5, Ledgers: []string{"marketplace"}, OrderCount: 1, Failed: true, Reason: "INSUFFICIENT_FUNDS"}
	technical := match.AuditEntry{Seq: 6, OrderCount: 1, MinLog: 45, MaxLog: 46}
	entries := []match.AuditEntry{ok(3, 42), foreignOK, foreignFail, technical, ok(7, 47)}

	require.Empty(t, match.AuditPageViolation(entries, 5, false, nil, 2, ""), "dense from after+1, foreign included")
	v := ValidatePage(m, nil, page(entries, nil, false, 5, 2, ""))
	require.Nil(t, v.Finding)
	require.Zero(t, v.Rejections, "foreign failures are not judged")

	// Reverse, with a foreign entry newer than every tracked bulk.
	newest := match.AuditEntry{Seq: 8, IdemKey: "r-wit-1a-k9", Ledgers: []string{"r-wit-1a-2"}, OrderCount: 1, MinLog: 99, MaxLog: 99}
	rev := []match.AuditEntry{newest, ok(7, 47)}
	require.Empty(t, match.AuditPageViolation(rev, 50, true, nil, 0, ""))
	require.Nil(t, ValidatePage(m, nil, page(rev, nil, true, 50, 0, "")).Finding)

	// A tracked failure nothing explains, with nothing outstanding, is a finding.
	trackedFail := match.AuditEntry{Seq: 9, IdemKey: tKey + "9", Ledgers: []string{tLedger}, OrderCount: 1, Failed: true, Reason: "INSUFFICIENT_FUNDS"}
	require.Equal(t, EvAuditFailureUnexplained, findingID(ValidatePage(m, nil, page([]match.AuditEntry{trackedFail}, nil, false, 50, 8, ""))))
	m.Outstanding = func(key string) bool { return key == trackedFail.IdemKey }
	require.Nil(t, ValidatePage(m, nil, page([]match.AuditEntry{trackedFail}, nil, false, 50, 8, "")).Finding, "its bulk is still in flight")
}

func TestForeignEntryNamingTrackedLedgerIsAViolation(t *testing.T) {
	t.Parallel()
	m := testModel([]uint64{42}, []uint64{42})
	for _, e := range []match.AuditEntry{
		{Seq: 4, IdemKey: "r-ns-1a-k3", Ledgers: []string{"default", tLedger}, OrderCount: 2, MinLog: 43, MaxLog: 44},
		{Seq: 4, Ledgers: []string{tLedger}, OrderCount: 1, Failed: true, Reason: "VALIDATION"},
		{Seq: 4, IdemKey: "o-test-ref1", Ledgers: []string{"o-test-l9"}, OrderCount: 1, MinLog: 43, MaxLog: 43},
	} {
		require.Equal(t, EvAuditForeignTouchedTracked, findingID(ValidatePage(m, nil, page([]match.AuditEntry{e}, nil, false, 50, 0, ""))), match.DescribeAuditEntry(e))
	}
}

func TestTrackedFailureWithAnUncommittedReasonIsUnexplained(t *testing.T) {
	t.Parallel()
	m := testModel(nil, nil)
	m.Rejections = map[oc.RejectedBulk]struct{}{{Ledgers: tLedger, Orders: 1, Reason: "FILTER_COMPILATION_ERROR"}: {}}
	m.Outstanding = func(string) bool { return true }
	e := match.AuditEntry{Seq: 4, IdemKey: tKey + "1", Ledgers: []string{tLedger}, OrderCount: 1, Failed: true, Reason: "FILTER_COMPILATION_ERROR"}
	require.True(t, oc.UncommittedReason("FILTER_COMPILATION_ERROR"))
	require.Equal(t, EvAuditFailureUnexplained, findingID(ValidatePage(m, nil, page([]match.AuditEntry{e}, nil, false, 50, 0, ""))))
}

func TestBatchKeyViolation(t *testing.T) {
	t.Parallel()
	m := testModel([]uint64{42}, []uint64{42})
	m.KeyByFirstSeq = map[uint64]string{42: tKey + "3"}
	f, checked := BatchKeyViolation(m, []match.AuditEntry{ok(3, 42)})
	require.Nil(t, f)
	require.Equal(t, 1, checked)
	bad := ok(3, 42)
	bad.IdemKey = "r-x-k1"
	f, _ = BatchKeyViolation(m, []match.AuditEntry{bad})
	require.Equal(t, EvAuditWrongBatchKey, f.ID)
}

func TestDuplicateProposalIDsUnderARejectedKey(t *testing.T) {
	t.Parallel()
	m := testModel(nil, nil)
	key := tKey + "7"
	m.FrozenRejected = func(k string) bool { return k == key }
	a := match.AuditEntry{Seq: 4, ProposalID: 11, IdemKey: key, Ledgers: []string{tLedger}, OrderCount: 1, Failed: true, Reason: "INSUFFICIENT_FUNDS"}
	b := a
	b.Seq = 9
	require.Nil(t, DuplicateProposalViolation(m, nil, []match.AuditEntry{a}))
	require.Nil(t, DuplicateProposalViolation(m, []match.AuditEntry{a}, []match.AuditEntry{a}), "the same entry seen twice")
	require.Equal(t, EvAuditFailuresUniquePerPropsl, DuplicateProposalViolation(m, []match.AuditEntry{a}, []match.AuditEntry{b}).ID)
	c := b
	c.ProposalID = 12
	require.Nil(t, DuplicateProposalViolation(m, []match.AuditEntry{a}, []match.AuditEntry{c}))
	m.FrozenRejected = func(string) bool { return false }
	require.Nil(t, DuplicateProposalViolation(m, []match.AuditEntry{a}, []match.AuditEntry{b}), "only keys rejected for good")
}

// ---- request intent ----

func uptr(v uint64) *uint64 { return &v }

func TestClassifyProbeMirrorsTheAuditCompiler(t *testing.T) {
	t.Parallel()
	seq := func(lo, hi uint64) *commonpb.QueryFilter {
		return match.FilterAuditUint(commonpb.AuditField_AUDIT_FIELD_SEQUENCE, &commonpb.UintCondition{Min: &lo, Max: &hi})
	}
	ledger := match.FilterAuditString(commonpb.AuditField_AUDIT_FIELD_LEDGER, "x")
	or := func(fs ...*commonpb.QueryFilter) *commonpb.QueryFilter {
		return &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Or{Or: &commonpb.OrFilter{Filters: fs}}}
	}
	and := func(fs ...*commonpb.QueryFilter) *commonpb.QueryFilter {
		return &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_And{And: &commonpb.AndFilter{Filters: fs}}}
	}
	keyParam := &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Audit{Audit: &commonpb.AuditCondition{Field: commonpb.AuditField_AUDIT_FIELD_IDEMPOTENCY_KEY,
		Condition: &commonpb.AuditCondition_StringCond{StringCond: &commonpb.StringCondition{Value: &commonpb.StringCondition_Param{Param: "p0"}}}}}}
	keyPrefix := func(p string) *commonpb.QueryFilter {
		return &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Audit{Audit: &commonpb.AuditCondition{Field: commonpb.AuditField_AUDIT_FIELD_IDEMPOTENCY_KEY,
			Condition: &commonpb.AuditCondition_StringPrefix{StringPrefix: p}}}}
	}
	cases := []struct {
		name string
		f    *commonpb.QueryFilter
		want Probe
	}{
		{"nil", nil, ProbeNone},
		{"seq", seq(1, 5), ProbeNone},
		{"and of seq and ledger", and(seq(1, 5), ledger), ProbeNone},
		{"empty and", and(), ProbeNone},
		{"or of indexed", or(ledger, match.FilterAuditString(commonpb.AuditField_AUDIT_FIELD_OUTCOME, "success")), ProbeNone},
		{"or with empty seq range", or(seq(5, 1), ledger), ProbeNone},
		{"seq in or", or(seq(1, 5), ledger), ProbeSeqInOr},
		{"and of seq in or", or(ledger, and(seq(1, 5))), ProbeSeqInOr},
		{"not", &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Not{Not: &commonpb.NotFilter{Filter: ledger}}}, ProbeNot},
		{"unspecified", match.FilterAuditString(commonpb.AuditField_AUDIT_FIELD_UNSPECIFIED, "anything"), ProbeUnspecifiedField},
		{"string on seq", match.FilterAuditString(commonpb.AuditField_AUDIT_FIELD_SEQUENCE, "x"), ProbeTypeMismatch},
		{"uint on ledger", match.FilterAuditUint(commonpb.AuditField_AUDIT_FIELD_LEDGER, &commonpb.UintCondition{Min: uptr(1)}), ProbeTypeMismatch},
		{"key param", keyParam, ProbeKeyParam},
		{"key nul prefix", keyPrefix("o-\x00bad"), ProbeNulPrefix},
		{"key prefix", keyPrefix("o-"), ProbeNone},
		{"subject empty", match.FilterAuditString(commonpb.AuditField_AUDIT_FIELD_CALLER_SUBJECT, ""), ProbeNone},
	}
	for _, c := range cases {
		require.Equal(t, c.want, ClassifyProbe(c.f), c.name)
	}
}

func TestUintCursor(t *testing.T) {
	t.Parallel()
	for _, bad := range []string{"not-a-cursor", "-1", "0x10", " 12", "12 ", "1.0", "18446744073709551616"} {
		_, malformed := UintCursor(bad)
		require.True(t, malformed, bad)
		_, malformed = UintCursor(pagecursor.Cursor{Key: bad}.Encode())
		require.True(t, malformed, "wrapped "+bad)
	}
	after, malformed := UintCursor(match.PageToken("17"))
	require.False(t, malformed)
	require.Equal(t, uint64(17), after)
}

// ---- through the read kinds, on a scripted tracker ----

func entryJSON(t *testing.T, e *auditpb.AuditEntry) json.RawMessage {
	t.Helper()
	b, err := protojson.Marshal(e)
	require.NoError(t, err)

	return b
}

func listReq(filter *commonpb.QueryFilter, pageSize int, cursor string, reverse bool) json.RawMessage {
	opts := &commonpb.ListOptions{PageSize: uint32(pageSize), Cursor: cursor, Reverse: reverse, Filter: filter}
	b, _ := protojson.Marshal(&servicepb.ListAuditEntriesRequest{Options: opts})

	return b
}

func readList(t *testing.T, s *testutil.ScriptedSession, req json.RawMessage, out oc.Outcome) []oc.Event {
	t.Helper()
	tk := s.Read(KindList)
	evs := ValidateListAuditEntries(&oc.ReadCtx{T: s.T, Ticket: tk, MaxTicket: s.T.ReadFrontier(), Kind: KindList, Request: req, Outcome: out})
	s.T.FinishRead(tk)

	return evs
}

func readGet(t *testing.T, s *testutil.ScriptedSession, seq uint64, out oc.Outcome) []oc.Event {
	t.Helper()
	tk := s.Read(KindGet)
	b, _ := protojson.Marshal(&servicepb.GetAuditEntryRequest{Sequence: seq})
	evs := ValidateGetAuditEntry(&oc.ReadCtx{T: s.T, Ticket: tk, MaxTicket: s.T.ReadFrontier(), Kind: KindGet, Request: b, Outcome: out})
	s.T.FinishRead(tk)

	return evs
}

func hasEvent(evs []oc.Event, id oc.EventID) bool {
	for _, e := range evs {
		if e.ID == id {
			return true
		}
	}

	return false
}

// auditOf renders the audit entry of the committed bulk with this key.
func auditOf(t *testing.T, s *testutil.ScriptedSession, key string, auditSeq uint64) *auditpb.AuditEntry {
	t.Helper()
	recs := s.T.WritesByKey(key)
	require.NotEmpty(t, recs)
	for _, b := range s.T.MatchCommittedBulks() {
		if b.Wid == recs[0].Wid {
			// The merged table: a creation log names its ledger only there.
			return matchtest.RenderAuditEntry(b, ModelOf(s.T, 0).Logs, auditSeq, key)
		}
	}
	t.Fatalf("no committed bulk for %s", key)

	return nil
}

func TestListThroughTheTracker(t *testing.T) {
	t.Parallel()
	s := testutil.NewSession("")
	l := s.Ledger(1)
	require.Empty(t, testutil.Findings(s.Write(false, testutil.CreateLedgerReq(l))))
	require.Empty(t, testutil.Findings(s.Write(true, testutil.TxReq(l, "", "world", "users:1", "COIN", 5))))
	k1, k2 := s.Prefix+"k1", s.Prefix+"k2"

	e1 := auditOf(t, s, k1, 2)
	e2 := auditOf(t, s, k2, 4)
	foreign := &auditpb.AuditEntry{Sequence: 3, OrderCount: 1, Ledgers: []string{"default"},
		Outcome: &auditpb.AuditEntry_Success{Success: &auditpb.AuditSuccess{MinLogSequence: 3, MaxLogSequence: 3}}}
	// A listing carries no items.
	for _, e := range []*auditpb.AuditEntry{e1, e2} {
		e.Items = nil
	}
	items := []json.RawMessage{entryJSON(t, e1), entryJSON(t, foreign), entryJSON(t, e2)}

	evs := readList(t, s, listReq(nil, 10, match.PageToken("1"), false), oc.Outcome{OK: true, Complete: true, Items: items})
	require.Empty(t, testutil.Findings(evs), "%v", evs)
	require.True(t, hasEvent(evs, EvAuditPageValidated))
	require.True(t, hasEvent(evs, EvAuditBatchKeyMatched), "k2 was flagged: replayable")

	// The samples view serves what the page showed.
	v, err := SamplesView(s.T.Model(), s.T, nil)
	require.NoError(t, err)
	require.Len(t, v.(map[string]any)["samples"], 3)

	// The next read is held to the remembered entries.
	evs = readList(t, s, listReq(nil, 10, match.PageToken("1"), false), oc.Outcome{OK: true, Complete: true, Items: items[:2]})
	require.Equal(t, []oc.EventID{EvAuditStoppedShortOfKnown}, testutil.IDs(testutil.Findings(evs)))

	// GetAuditEntry carries items.
	full := auditOf(t, s, k2, 4)
	evs = readGet(t, s, 4, oc.Outcome{OK: true, Response: entryJSON(t, full)})
	require.Empty(t, testutil.Findings(evs))
	require.True(t, hasEvent(evs, EvAuditEntryReadValidated))
	require.True(t, hasEvent(evs, EvAuditEntryCarriedItems))
	require.Equal(t, []oc.EventID{EvAuditEntryReadOtherSeq}, testutil.IDs(testutil.Findings(readGet(t, s, 5, oc.Outcome{OK: true, Response: entryJSON(t, full)}))))
	require.True(t, hasEvent(readGet(t, s, math.MaxUint64, oc.Outcome{Code: "NotFound", Reason: "AUDIT_ENTRY_NOT_FOUND"}), EvAuditUnassignedNotFound))
	require.Equal(t, []oc.EventID{EvAuditEntryServedUnassigned}, testutil.IDs(readGet(t, s, math.MaxUint64, oc.Outcome{OK: true, Response: entryJSON(t, full)})))
	require.Equal(t, []oc.EventID{EvAuditGetUnexpectedError}, testutil.IDs(readGet(t, s, 4, oc.Outcome{Code: "NotFound"})))

	// A mutated range is outside the model.
	bad := auditOf(t, s, k2, 4)
	bad.GetSuccess().MaxLogSequence++
	bad.OrderCount++
	bad.Items = nil
	require.Equal(t, []oc.EventID{EvAuditOutsideModel}, testutil.IDs(testutil.Findings(readGet(t, s, 4, oc.Outcome{OK: true, Response: entryJSON(t, bad)}))))
}

func TestListRefusalsAndErrors(t *testing.T) {
	t.Parallel()
	s := testutil.NewSession("")
	not := &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Not{Not: &commonpb.NotFilter{Filter: match.FilterAuditString(commonpb.AuditField_AUDIT_FIELD_OUTCOME, "success")}}}
	invalid := oc.Outcome{Code: "InvalidArgument", Message: "refused"}

	require.Equal(t, []oc.EventID{EvAuditNotRejected}, testutil.IDs(readList(t, s, listReq(not, 5, "", false), invalid)))
	require.Equal(t, []oc.EventID{EvAuditUnsupportedFilterPage}, testutil.IDs(readList(t, s, listReq(not, 5, "", false), oc.Outcome{OK: true, Complete: true})))
	require.Equal(t, []oc.EventID{EvAuditMalformedCursorRejected}, testutil.IDs(readList(t, s, listReq(nil, 5, "not-a-cursor", false), invalid)))
	require.Equal(t, []oc.EventID{EvAuditMalformedCursorResults}, testutil.IDs(readList(t, s, listReq(nil, 5, "1.0", false), oc.Outcome{OK: true, Complete: true})))
	require.Equal(t, []oc.EventID{EvAuditListUnexpectedError}, testutil.IDs(readList(t, s, listReq(nil, 5, "", false), invalid)))

	building := oc.Outcome{Code: "Unavailable", Reason: "INDEX_BUILDING"}
	require.Empty(t, readList(t, s, listReq(match.FilterAuditString(commonpb.AuditField_AUDIT_FIELD_OUTCOME, "failure"), 5, "", false), building), "projection rebuilding")
	require.Equal(t, []oc.EventID{EvAuditListUnexpectedError}, testutil.IDs(readList(t, s, listReq(nil, 5, "", false), building)), "a zone scan never waits on the projection")

	// A page longer than requested breaks the contract.
	e := &auditpb.AuditEntry{Sequence: 1, OrderCount: 1, Outcome: &auditpb.AuditEntry_Success{Success: &auditpb.AuditSuccess{MinLogSequence: 1, MaxLogSequence: 1}}}
	e2 := &auditpb.AuditEntry{Sequence: 2, OrderCount: 1, Outcome: &auditpb.AuditEntry_Success{Success: &auditpb.AuditSuccess{MinLogSequence: 2, MaxLogSequence: 2}}}
	require.Equal(t, []oc.EventID{EvAuditPageContract}, testutil.IDs(readList(t, s, listReq(nil, 1, "", false), oc.Outcome{OK: true, Complete: true, Items: []json.RawMessage{entryJSON(t, e), entryJSON(t, e2)}})))
	// An incomplete page is not judged.
	require.Empty(t, readList(t, s, listReq(nil, 1, "", false), oc.Outcome{OK: true, Items: []json.RawMessage{entryJSON(t, e), entryJSON(t, e2)}}))
	// ...but an OK answer to a malformed cursor or a refused probe is reported
	// even when the client stopped at its item cap.
	require.Equal(t, []oc.EventID{EvAuditMalformedCursorResults}, testutil.IDs(readList(t, s, listReq(nil, 1, "1.0", false), oc.Outcome{OK: true, Items: []json.RawMessage{entryJSON(t, e)}})))
	require.Equal(t, []oc.EventID{EvAuditUnsupportedFilterPage}, testutil.IDs(readList(t, s, listReq(not, 1, "", false), oc.Outcome{OK: true, Items: []json.RawMessage{entryJSON(t, e)}})))
}

// failureEntry renders the audit entry the server writes for a rejected bulk.
func failureEntry(seq, proposal uint64, key string, ledgers []string, orders uint32, reason string) *auditpb.AuditEntry {
	return &auditpb.AuditEntry{Sequence: seq, ProposalId: proposal, Ledgers: ledgers, OrderCount: orders,
		Idempotency: &commonpb.Idempotency{Key: key},
		Outcome:     &auditpb.AuditEntry_Failure{Failure: &auditpb.AuditFailure{Details: detailsFor(reason)}}}
}

// detailsFor builds failure details whose variant is annotated with reason.
func detailsFor(reason string) *commonpb.FailureDetails {
	want := commonpb.ErrorReason(commonpb.ErrorReason_value["ERROR_REASON_"+reason])
	d := &commonpb.FailureDetails{}
	m := d.ProtoReflect()
	fields := m.Descriptor().Oneofs().ByName("kind").Fields()
	for i := range fields.Len() {
		f := fields.Get(i)
		opts, ok := f.Options().(*descriptorpb.FieldOptions)
		if ok && proto.HasExtension(opts, commonpb.E_FailureReason) && proto.GetExtension(opts, commonpb.E_FailureReason).(commonpb.ErrorReason) == want {
			m.Set(f, m.NewField(f))

			return d
		}
	}
	panic("no failure detail for " + reason)
}

func TestBulkAtomicityPerKey(t *testing.T) {
	t.Parallel()
	s := testutil.NewSession("")
	l := s.Ledger(1)
	require.Empty(t, testutil.Findings(s.Write(false, testutil.CreateLedgerReq(l))))

	// A frozen FSM rejection: an overdraft from an unfunded account.
	doomed := s.Begin(true, testutil.TxReq(l, "", "t-0:1", "users:1", "COIN", 5))
	_, evs := s.End(doomed)
	require.Empty(t, testutil.Findings(evs))
	rec, _ := s.T.RecordByTicket(doomed)
	require.Equal(t, oc.WFailed, rec.Status)
	require.True(t, frozenRejected(s.T.WritesByKey(rec.Key)), "reason %s", rec.Outcome.Reason)

	fail := failureEntry(2, 7, rec.Key, []string{l}, 1, rec.Outcome.Reason)
	keyFilter := match.FilterAuditString(commonpb.AuditField_AUDIT_FIELD_IDEMPOTENCY_KEY, rec.Key)
	evs = readList(t, s, listReq(keyFilter, 10, "", false), oc.Outcome{OK: true, Complete: true, Items: []json.RawMessage{entryJSON(t, fail)}})
	require.Empty(t, testutil.Findings(evs), "%v", evs)
	require.True(t, hasEvent(evs, EvAuditFailureMatchedRejection))
	require.True(t, hasEvent(evs, EvAuditFailureOwedChecked))

	// A whole page of the rejected key's filter with no failure entry.
	evs = readList(t, s, listReq(keyFilter, 10, "", false), oc.Outcome{OK: true, Complete: true})
	require.Equal(t, []oc.EventID{EvAuditRejectedBulkMissesFailure}, testutil.IDs(testutil.Findings(evs)))

	// A success entry under the rejected key is a violation.
	succ := &auditpb.AuditEntry{Sequence: 3, ProposalId: 8, Ledgers: []string{l}, OrderCount: 1, Idempotency: &commonpb.Idempotency{Key: rec.Key},
		Outcome: &auditpb.AuditEntry_Success{Success: &auditpb.AuditSuccess{MinLogSequence: 2, MaxLogSequence: 2}}}
	evs = readList(t, s, listReq(keyFilter, 10, "", false), oc.Outcome{OK: true, Complete: true, Items: []json.RawMessage{entryJSON(t, fail), entryJSON(t, succ)}})
	require.Equal(t, []oc.EventID{EvAuditRejectedBulkHasSuccess}, testutil.IDs(testutil.Findings(evs)))

	// A second failure entry of the same proposal is a violation.
	dup := failureEntry(4, 7, rec.Key, []string{l}, 1, rec.Outcome.Reason)
	evs = readList(t, s, listReq(keyFilter, 10, "", false), oc.Outcome{OK: true, Complete: true, Items: []json.RawMessage{entryJSON(t, fail), entryJSON(t, dup)}})
	require.Equal(t, []oc.EventID{EvAuditFailuresUniquePerPropsl}, testutil.IDs(testutil.Findings(evs)))

	// An original that committed plus two conflict attempts under its key: the
	// conflicts are audited as failures while the original's success stays.
	origTk := s.Begin(true, testutil.TxReq(l, "", "world", "users:2", "COIN", 3))
	_, evs = s.End(origTk)
	require.Empty(t, testutil.Findings(evs))
	orig, _ := s.T.RecordByTicket(origTk)
	for i := range 2 {
		conflict := oc.WriteRecord{Wid: "wc" + u(uint64(i)), Key: orig.Key, Role: oc.RoleConflict, Of: orig.Wid, Family: "test", TrackKey: true,
			Requests: []*servicepb.Request{testutil.TxReq(l, "", "world", "users:3", "COIN", uint64(4+i))}}
		tk, _, _, ref := s.T.BeginWrite(conflict)
		require.Nil(t, ref)
		_, evs = s.End(tk)
		require.Empty(t, testutil.Findings(evs))
		crec, _ := s.T.RecordByTicket(tk)
		require.Equal(t, "IDEMPOTENCY_KEY_CONFLICT", crec.Outcome.Reason)
	}
	require.False(t, frozenRejected(s.T.WritesByKey(orig.Key)))
	origEntry := auditOf(t, s, orig.Key, 5)
	origEntry.Items = nil
	c1 := failureEntry(6, 20, orig.Key, []string{l}, 1, "IDEMPOTENCY_KEY_CONFLICT")
	c2 := failureEntry(7, 21, orig.Key, []string{l}, 1, "IDEMPOTENCY_KEY_CONFLICT")
	evs = readList(t, s, listReq(match.FilterAuditString(commonpb.AuditField_AUDIT_FIELD_IDEMPOTENCY_KEY, orig.Key), 10, "", false),
		oc.Outcome{OK: true, Complete: true, Items: []json.RawMessage{entryJSON(t, origEntry), entryJSON(t, c1), entryJSON(t, c2)}})
	require.Empty(t, testutil.Findings(evs), "%v", evs)
}

// A retryable FSM-stage failure whose error is not an UncommittedFailure is
// audited but not frozen (machine.go recordIdempotencyFailure); the driver
// retries the key, which then commits. The audited failure has no recorded
// rejection, and once the key settles it must be left unjudged rather than
// reported unexplained. (BALANCE_NOT_PRELOADED and the other
// UncommittedReasons are never audited and stay findings.)
func TestRetryableFailureThenSuccessUnderOneKey(t *testing.T) {
	t.Parallel()
	m := testModel([]uint64{42}, []uint64{42})
	key := tKey + "9"
	m.Registered = func(k string) bool { return k == key }
	retry := match.AuditEntry{Seq: 3, ProposalID: 5, IdemKey: key, Ledgers: []string{tLedger}, OrderCount: 1, Failed: true, Reason: "INDEX_INCONSISTENT"}
	require.False(t, oc.IsDefinitiveReason(retry.Reason))
	require.False(t, oc.UncommittedReason(retry.Reason))
	success := ok(4, 42)
	success.IdemKey = key
	v := ValidatePage(m, nil, page([]match.AuditEntry{retry, success}, nil, false, 50, 0, ""))
	require.Nil(t, v.Finding, "%+v", v.Finding)
	require.Equal(t, 1, v.Unjudged)
	require.Zero(t, v.Rejections)

	// A definitive reason with no recorded rejection is still unexplained.
	def := retry
	def.Reason = "INSUFFICIENT_FUNDS"
	require.True(t, oc.IsDefinitiveReason(def.Reason))
	require.Equal(t, EvAuditFailureUnexplained, findingID(ValidatePage(m, nil, page([]match.AuditEntry{def, success}, nil, false, 50, 0, ""))))

	// A reason the server never audits stays a finding, registered or not.
	never := retry
	never.Reason = "BALANCE_NOT_PRELOADED"
	require.Equal(t, EvAuditFailureUnexplained, findingID(ValidatePage(m, nil, page([]match.AuditEntry{never, success}, nil, false, 50, 0, ""))))

	// A retryable reason under a key the oracle never registered is judged.
	m.Registered = func(string) bool { return false }
	require.Equal(t, EvAuditFailureUnexplained, findingID(ValidatePage(m, nil, page([]match.AuditEntry{retry}, nil, false, 50, 0, ""))))
}

// The failure half of the old bulk-atomicity driver: a whole first page of an
// exact key filter, read after the key's frozen rejection was observed, holds
// a failure entry under the key.
func TestRejectedKeyOwesAFailureEntry(t *testing.T) {
	t.Parallel()
	m := testModel(nil, nil)
	key := tKey + "7"
	m.FrozenRejected = func(k string) bool { return k == key }
	m.FrozenObservedAt = func(k string) uint64 {
		if k == key {
			return 10
		}

		return 0
	}
	filter := match.FilterAuditString(commonpb.AuditField_AUDIT_FIELD_IDEMPOTENCY_KEY, key)
	fail := match.AuditEntry{Seq: 4, ProposalID: 11, IdemKey: key, Ledgers: []string{tLedger}, OrderCount: 1, Failed: true, Reason: "INSUFFICIENT_FUNDS"}
	m.Rejections = map[oc.RejectedBulk]struct{}{{Ledgers: tLedger, Orders: 1, Reason: "INSUFFICIENT_FUNDS"}: {}}
	withTicket := func(p Page, tk uint64) Page { p.ReadTicket = tk; return p }

	v := ValidatePage(m, nil, withTicket(page([]match.AuditEntry{fail}, filter, false, 10, 0, ""), 11))
	require.Nil(t, v.Finding)
	require.True(t, v.FailureOwedChecked)

	v = ValidatePage(m, nil, withTicket(page(nil, filter, false, 10, 0, ""), 11))
	require.Equal(t, EvAuditRejectedBulkMissesFailure, findingID(v))

	// Not judged: a read that began before the observation, a resumed page,
	// a full page, a prefix filter, a key not rejected for good.
	for name, p := range map[string]Page{
		"read before":  withTicket(page(nil, filter, false, 10, 0, ""), 10),
		"no ticket":    page(nil, filter, false, 10, 0, ""),
		"resumed page": withTicket(page(nil, filter, false, 10, 3, ""), 11),
		"prefix":       withTicket(page(nil, &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Audit{Audit: &commonpb.AuditCondition{Field: commonpb.AuditField_AUDIT_FIELD_IDEMPOTENCY_KEY, Condition: &commonpb.AuditCondition_StringPrefix{StringPrefix: key}}}}, false, 10, 0, ""), 11),
		"other key":    withTicket(page(nil, match.FilterAuditString(commonpb.AuditField_AUDIT_FIELD_IDEMPOTENCY_KEY, tKey+"8"), false, 10, 0, ""), 11),
	} {
		v := ValidatePage(m, nil, p)
		require.Nil(t, v.Finding, name)
		require.False(t, v.FailureOwedChecked, name)
	}
	full := withTicket(page([]match.AuditEntry{ok(5, 0)}, filter, false, 1, 0, ""), 11)
	full.Entries[0].IdemKey = "r-foreign"
	full.Entries[0].Ledgers = []string{"default"}
	require.False(t, ValidatePage(m, nil, full).FailureOwedChecked, "a full page")
}

// At most SamplesPerPage entries of a page become samples, spread over it.
func TestSamplesSpreadOverThePage(t *testing.T) {
	t.Parallel()
	s := &state{known: map[uint64]knownEntry{}}
	var entries []match.AuditEntry
	for i := range uint64(20) {
		entries = append(entries, match.AuditEntry{Seq: i + 1})
	}
	s.noteSamples(entries, nil)
	require.Len(t, s.samples, SamplesPerPage)
	require.Equal(t, uint64(1), s.samples[0].Seq)
	require.Equal(t, uint64(20), s.samples[SamplesPerPage-1].Seq)
	s.noteSamples(entries[:2], nil)
	require.Len(t, s.samples, SamplesPerPage+2)
}
