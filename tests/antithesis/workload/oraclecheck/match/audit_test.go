package match

import (
	"math"
	"strconv"
	"testing"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/tests/oracle"
	"github.com/stretchr/testify/require"
)

// committedStateWithSequences is a one-ledger committed state holding one
// account-metadata log per sequence given, each learned at that sequence.
func committedStateWithSequences(t *testing.T, sequences ...uint64) oracle.GlobalState {
	t.Helper()

	reqs := make([]*servicepb.Request, 0, len(sequences))
	for i := range sequences {
		reqs = append(reqs, saveAccountMetaReqL("L", "acc:1", "k"+strconv.Itoa(i), "v"))
	}

	res := oracle.NewGlobalState().Apply(oracle.Bulk{Requests: reqs})
	require.True(t, res.OK, "setup bulk rejected: %s", res.Reason)

	for i, seq := range sequences {
		res.State.LearnLogSequence("L", uint64(i+1), seq)
	}

	return res.State
}

// metadataLogs is the committed-log record committedStateWithSequences builds:
// one account-metadata save per sequence, in order, on ledger L.
func metadataLogs(sequences ...uint64) map[uint64]CommittedLog {
	out := make(map[uint64]CommittedLog, len(sequences))
	for i, seq := range sequences {
		out[seq] = CommittedLog{Ledger: "L", ID: uint64(i + 1), Kind: "saved_metadata"}
	}

	return out
}

// singleOrderBulks is the boundary record for a run of one-order bulks, each
// committing at one sequence — the shape committedStateWithSequences builds.
func singleOrderBulks(sequences ...uint64) map[uint64]CommittedBulk {
	out := make(map[uint64]CommittedBulk, len(sequences))
	for _, seq := range sequences {
		out[seq] = CommittedBulk{MinSeq: seq, MaxSeq: seq, Orders: 1}
	}

	return out
}

func TestAuditSuccessMismatchPinsOneBulk(t *testing.T) {
	t.Parallel()

	logs := map[uint64]CommittedLog{
		10: {Ledger: "a", ID: 1}, 11: {Ledger: "b", ID: 1}, 12: {Ledger: "a", ID: 2},
	}
	entry := AuditEntry{Seq: 7, Ledgers: []string{"a", "b"}, OrderCount: 3, MinLog: 10, MaxLog: 12, ItemSeqs: []uint64{10, 11, 12}}
	bulks := map[uint64]CommittedBulk{10: {MinSeq: 10, MaxSeq: 12, Orders: 3}}

	why, _ := AuditSuccessMismatch(entry, logs, bulks, 12)
	require.Empty(t, why)

	archived := entry
	archived.ItemSeqs = nil
	why, _ = AuditSuccessMismatch(archived, logs, bulks, 12)
	require.Empty(t, why, "purged items are not a mismatch")

	e := entry
	e.Ledgers = []string{"a"}
	why, _ = AuditSuccessMismatch(e, logs, bulks, 12)
	require.Contains(t, why, "entry names a")

	e = entry
	e.OrderCount = 2
	why, _ = AuditSuccessMismatch(e, logs, bulks, 12)
	require.Contains(t, why, "order count")

	e = entry
	e.ItemSeqs = []uint64{10, 10, 12}
	why, _ = AuditSuccessMismatch(e, logs, bulks, 12)
	require.Contains(t, why, "repeated", "a repeated item leaves log 11 without one")

	e = entry
	e.MaxLog = 13
	why, _ = AuditSuccessMismatch(e, logs, bulks, 12)
	require.Contains(t, why, "straddles")

	e = entry
	e.ItemSeqs = []uint64{10, 11, 99}
	why, _ = AuditSuccessMismatch(e, logs, bulks, 12)
	require.Contains(t, why, "outside the entry")

	// One entry per bulk: a contiguous run of committed logs is not enough when
	// the bulks behind it were separate.
	split := map[uint64]CommittedBulk{10: {MinSeq: 10, MaxSeq: 11, Orders: 2}, 12: {MinSeq: 12, MaxSeq: 12, Orders: 1}}
	why, _ = AuditSuccessMismatch(entry, logs, split, 12)
	require.Contains(t, why, "ends at 11")

	merged := AuditEntry{Seq: 7, Ledgers: []string{"b"}, OrderCount: 1, MinLog: 11, MaxLog: 11}
	why, missing := AuditSuccessMismatch(merged, logs, bulks, 12)
	require.Contains(t, why, "no committed bulk begins at 11")
	require.EqualValues(t, 11, missing)

	delete(logs, 11)
	why, _ = AuditSuccessMismatch(entry, logs, bulks, 12)
	require.Contains(t, why, "no committed log")
}

func TestAuditPageViolationIsAnExtraOverTheModel(t *testing.T) {
	t.Parallel()

	ledgerA := FilterAuditString(commonpb.AuditField_AUDIT_FIELD_LEDGER, "a")
	page := []AuditEntry{{Seq: 3, Ledgers: []string{"a"}}, {Seq: 2, Ledgers: []string{"a"}}}
	require.Empty(t, AuditPageViolation(page, 2, true, ledgerA, 0, ""))
	require.Equal(t, "page longer than requested", AuditPageViolation(page, 1, true, ledgerA, 0, ""))
	require.Equal(t, "ascending order violated", AuditPageViolation(page, 2, false, ledgerA, 0, ""))
	require.Equal(t, "entry outside the filter", AuditPageViolation(page, 2, true, FilterAuditString(commonpb.AuditField_AUDIT_FIELD_LEDGER, "b"), 0, ""))
	require.Equal(t, "entry outside the filter", AuditPageViolation(page, 2, true, FilterAuditString(commonpb.AuditField_AUDIT_FIELD_OUTCOME, "failure"), 0, ""))
}

// A zone scan — no filter, a sequence bound, or an And of those — serves dense
// sequences from its lower bound; an indexed page may skip.
func TestAuditPageViolationZoneScanIsDense(t *testing.T) {
	t.Parallel()

	page := func(seqs ...uint64) []AuditEntry {
		out := make([]AuditEntry, 0, len(seqs))
		for _, s := range seqs {
			out = append(out, AuditEntry{Seq: s, Ledgers: []string{"a"}})
		}

		return out
	}

	require.Empty(t, AuditPageViolation(page(1, 2, 3), 10, false, nil, 0, ""))
	require.Equal(t, "gap in a zone-scan page", AuditPageViolation(page(1, 3), 10, false, nil, 0, ""))
	require.Equal(t, "zone-scan page does not start at its lower bound", AuditPageViolation(page(2, 3), 10, false, nil, 0, ""))

	lo := uint64(2)
	from2 := FilterAuditUint(commonpb.AuditField_AUDIT_FIELD_SEQUENCE, &commonpb.UintCondition{Min: &lo})
	require.Empty(t, AuditPageViolation(page(2, 3), 10, false, from2, 0, ""))
	require.Empty(t, AuditPageViolation(page(3, 4), 10, false, FilterAuditUint(commonpb.AuditField_AUDIT_FIELD_SEQUENCE, &commonpb.UintCondition{Min: &lo, MinExclusive: true}), 0, ""), "an exclusive bound starts one past it")
	require.Equal(t, "zone-scan page does not start at its lower bound", AuditPageViolation(page(1, 2), 10, false, from2, 0, ""))
	require.Empty(t, AuditPageViolation(page(5, 4, 3), 10, true, nil, 0, ""), "a reverse scan starts anywhere but stays dense")
	require.Equal(t, "gap in a zone-scan page", AuditPageViolation(page(5, 3), 10, true, nil, 0, ""))

	indexed := FilterAnd(from2, FilterAuditString(commonpb.AuditField_AUDIT_FIELD_LEDGER, "a"))
	require.Empty(t, AuditPageViolation(page(2, 5, 9), 10, false, indexed, 0, ""), "an indexed page selects sparsely")
}

// A resumed page starts past its cursor, in the iteration's own direction, and
// the token it hands back names the last entry it served.
func TestAuditPageViolationHonoursTheCursor(t *testing.T) {
	t.Parallel()

	page := func(seqs ...uint64) []AuditEntry {
		out := make([]AuditEntry, 0, len(seqs))
		for _, s := range seqs {
			out = append(out, AuditEntry{Seq: s, Ledgers: []string{"a"}})
		}

		return out
	}

	require.Empty(t, AuditPageViolation(page(4, 5), 10, false, nil, 3, ""))
	require.Equal(t, "entry outside the cursor", AuditPageViolation(page(3, 4), 10, false, nil, 3, ""), "resume is exclusive")
	require.Empty(t, AuditPageViolation(page(2, 1), 10, true, nil, 3, ""))
	require.Equal(t, "entry outside the cursor", AuditPageViolation(page(3, 2), 10, true, nil, 3, ""))

	// The zone is dense past the cursor too, so a resumed scan starts on the
	// very next sequence.
	require.Equal(t, "zone-scan page does not start at its lower bound", AuditPageViolation(page(5, 6), 10, false, nil, 3, ""))

	lo := uint64(8)
	from8 := FilterAuditUint(commonpb.AuditField_AUDIT_FIELD_SEQUENCE, &commonpb.UintCondition{Min: &lo})
	require.Empty(t, AuditPageViolation(page(8, 9), 10, false, from8, 3, ""), "the filter's bound outranks a lower cursor")

	// The trail's universe is wider than the model's, so whether a token was
	// owed is never judged — but its value is, and only a full page can carry
	// one at all.
	require.Empty(t, AuditPageViolation(page(4, 5), 2, false, nil, 3, "5"))
	require.Empty(t, AuditPageViolation(page(4, 5), 2, false, nil, 3, ""), "a full page may equally have been the last one")
	require.Equal(t, "resume token does not match the page it rode with", AuditPageViolation(page(4, 5), 2, false, nil, 3, "6"))
	require.Equal(t, "resume token does not match the page it rode with", AuditPageViolation(page(4, 5), 10, false, nil, 3, "5"), "the page size did not cut this page, so the peek never fired")
	require.Equal(t, "resume token does not match the page it rode with", AuditPageViolation(nil, 10, false, nil, 3, "5"))
}

// Every audit leaf is judged on the served entry's own fields.
func TestAuditEntrySatisfiesEveryLeaf(t *testing.T) {
	t.Parallel()

	ok := AuditEntry{Seq: 10, ProposalID: 77, Timestamp: 5_000_000, Subject: "alice", Ledgers: []string{"a", "b"}, MinLog: 40, MaxLog: 42}
	failed := AuditEntry{Seq: 11, Ledgers: []string{"a"}, Failed: true, Reason: "VALIDATION"}
	u := func(lo, hi uint64) *commonpb.UintCondition { return &commonpb.UintCondition{Min: &lo, Max: &hi} }
	leaf := FilterAuditUint
	str := FilterAuditString

	require.True(t, AuditEntrySatisfies(nil, ok))
	require.True(t, AuditEntrySatisfies(leaf(commonpb.AuditField_AUDIT_FIELD_SEQUENCE, u(10, 10)), ok))
	require.False(t, AuditEntrySatisfies(leaf(commonpb.AuditField_AUDIT_FIELD_SEQUENCE, u(11, 20)), ok))
	require.True(t, AuditEntrySatisfies(leaf(commonpb.AuditField_AUDIT_FIELD_PROPOSAL_ID, u(70, 80)), ok))
	require.True(t, AuditEntrySatisfies(leaf(commonpb.AuditField_AUDIT_FIELD_TIMESTAMP, u(4_000_000, 6_000_000)), ok))
	require.True(t, AuditEntrySatisfies(leaf(commonpb.AuditField_AUDIT_FIELD_LOG_SEQUENCE, u(42, 50)), ok), "match-any over the entry's logs")
	require.False(t, AuditEntrySatisfies(leaf(commonpb.AuditField_AUDIT_FIELD_LOG_SEQUENCE, u(43, 50)), ok))
	require.False(t, AuditEntrySatisfies(leaf(commonpb.AuditField_AUDIT_FIELD_LOG_SEQUENCE, u(0, 100)), failed), "a rejection has no logs")
	require.True(t, AuditEntrySatisfies(str(commonpb.AuditField_AUDIT_FIELD_OUTCOME, "success"), ok))
	require.True(t, AuditEntrySatisfies(str(commonpb.AuditField_AUDIT_FIELD_OUTCOME, "failure"), failed))
	require.True(t, AuditEntrySatisfies(str(commonpb.AuditField_AUDIT_FIELD_LEDGER, "b"), ok))
	require.True(t, AuditEntrySatisfies(str(commonpb.AuditField_AUDIT_FIELD_CALLER_SUBJECT, "alice"), ok))
	require.False(t, AuditEntrySatisfies(str(commonpb.AuditField_AUDIT_FIELD_CALLER_SUBJECT, ""), failed), "an empty subject is never indexed")
	require.True(t, AuditEntrySatisfies(str(commonpb.AuditField_AUDIT_FIELD_ORDER_TYPE, "anything"), ok), "order types are judged by the model")
	require.True(t, AuditEntrySatisfies(FilterAnd(str(commonpb.AuditField_AUDIT_FIELD_LEDGER, "a"), str(commonpb.AuditField_AUDIT_FIELD_OUTCOME, "success")), ok))
	require.False(t, AuditEntrySatisfies(FilterOr(str(commonpb.AuditField_AUDIT_FIELD_LEDGER, "c"), str(commonpb.AuditField_AUDIT_FIELD_OUTCOME, "failure")), ok))

	// The exclusive extrema keep an empty range empty.
	zero := uint64(0)
	top := uint64(math.MaxUint64)
	require.False(t, BoundsMeetRange(&commonpb.UintCondition{Max: &zero, MaxExclusive: true}, 0, 100))
	require.False(t, BoundsMeetRange(&commonpb.UintCondition{Min: &top, MinExclusive: true}, 0, math.MaxUint64))
	require.True(t, BoundsMeetRange(&commonpb.UintCondition{Max: &zero}, 0, 100))
}

// The kind-to-token table is the indexer's own derivation.
func TestAuditOrderTypeTableMatchesTheDomain(t *testing.T) {
	t.Parallel()

	ledgerScoped := func(o *raftcmdpb.LedgerScopedOrder) *raftcmdpb.Order {
		return &raftcmdpb.Order{Type: &raftcmdpb.Order_LedgerScoped{LedgerScoped: o}}
	}
	apply := func(o *raftcmdpb.LedgerApplyOrder) *raftcmdpb.Order {
		return ledgerScoped(&raftcmdpb.LedgerScopedOrder{Payload: &raftcmdpb.LedgerScopedOrder_Apply{Apply: o}})
	}
	orders := map[string]*raftcmdpb.Order{
		"created_transaction":              apply(&raftcmdpb.LedgerApplyOrder{Data: &raftcmdpb.LedgerApplyOrder_CreateTransaction{}}),
		"reverted_transaction":             apply(&raftcmdpb.LedgerApplyOrder{Data: &raftcmdpb.LedgerApplyOrder_RevertTransaction{}}),
		"saved_metadata":                   apply(&raftcmdpb.LedgerApplyOrder{Data: &raftcmdpb.LedgerApplyOrder_AddMetadata{}}),
		"deleted_metadata":                 apply(&raftcmdpb.LedgerApplyOrder{Data: &raftcmdpb.LedgerApplyOrder_DeleteMetadata{}}),
		"set_metadata_field_type":          apply(&raftcmdpb.LedgerApplyOrder{Data: &raftcmdpb.LedgerApplyOrder_SetMetadataFieldType{}}),
		"removed_metadata_field_type":      apply(&raftcmdpb.LedgerApplyOrder{Data: &raftcmdpb.LedgerApplyOrder_RemoveMetadataFieldType{}}),
		"create_index":                     apply(&raftcmdpb.LedgerApplyOrder{Data: &raftcmdpb.LedgerApplyOrder_CreateIndex{}}),
		"drop_index":                       apply(&raftcmdpb.LedgerApplyOrder{Data: &raftcmdpb.LedgerApplyOrder_DropIndex{}}),
		"added_account_type":               apply(&raftcmdpb.LedgerApplyOrder{Data: &raftcmdpb.LedgerApplyOrder_AddAccountType{}}),
		"removed_account_type":             apply(&raftcmdpb.LedgerApplyOrder{Data: &raftcmdpb.LedgerApplyOrder_RemoveAccountType{}}),
		"updated_default_enforcement_mode": apply(&raftcmdpb.LedgerApplyOrder{Data: &raftcmdpb.LedgerApplyOrder_UpdateDefaultEnforcementMode{}}),
		"saved_ledger_metadata":            ledgerScoped(&raftcmdpb.LedgerScopedOrder{Payload: &raftcmdpb.LedgerScopedOrder_SaveLedgerMetadata{}}),
		"deleted_ledger_metadata":          ledgerScoped(&raftcmdpb.LedgerScopedOrder{Payload: &raftcmdpb.LedgerScopedOrder_DeleteLedgerMetadata{}}),
	}
	require.Len(t, orders, len(AuditOrderTypeOfKind))
	for kind, order := range orders {
		require.Equal(t, domain.AuditOrderType(order), AuditOrderTypeOfKind[kind], kind)
	}

	for _, token := range AuditOrderTypeOfKind {
		require.Contains(t, AuditOrderTypes, token)
	}
}

func saveLedgerMetaReqL(ledger string) *servicepb.Request {
	return &servicepb.Request{Type: &servicepb.Request_SaveLedgerMetadata{
		SaveLedgerMetadata: &servicepb.SaveLedgerMetadataRequest{Ledger: ledger, Metadata: map[string]*commonpb.MetadataValue{"k": {Type: &commonpb.MetadataValue_StringValue{StringValue: "v"}}}},
	}}
}

// requestLogKind names the accepted order, so every kind the token table maps
// has a request shape that produces it — otherwise an order-type filter judges
// the entry against an empty set and rejects a page the server served right.
func TestRequestLogKindCoversEveryAuditToken(t *testing.T) {
	t.Parallel()

	apply := func(action *servicepb.LedgerAction) *servicepb.Request {
		return &servicepb.Request{Type: &servicepb.Request_Apply{
			Apply: &servicepb.LedgerApplyRequest{Action: action},
		}}
	}

	reqs := []*servicepb.Request{
		apply(&servicepb.LedgerAction{Data: &servicepb.LedgerAction_CreateTransaction{}}),
		apply(&servicepb.LedgerAction{Data: &servicepb.LedgerAction_RevertTransaction{}}),
		apply(&servicepb.LedgerAction{Data: &servicepb.LedgerAction_AddMetadata{}}),
		apply(&servicepb.LedgerAction{Data: &servicepb.LedgerAction_DeleteMetadata{}}),
		apply(&servicepb.LedgerAction{Data: &servicepb.LedgerAction_AddAccountType{}}),
		apply(&servicepb.LedgerAction{Data: &servicepb.LedgerAction_RemoveAccountType{}}),
		apply(&servicepb.LedgerAction{Data: &servicepb.LedgerAction_SetDefaultEnforcementMode{}}),
		{Type: &servicepb.Request_SetMetadataFieldType{}},
		{Type: &servicepb.Request_RemoveMetadataFieldType{}},
		{Type: &servicepb.Request_CreateIndex{}},
		{Type: &servicepb.Request_DropIndex{}},
		{Type: &servicepb.Request_AddAccountType{}},
		{Type: &servicepb.Request_RemoveAccountType{}},
		{Type: &servicepb.Request_SetDefaultEnforcementMode{}},
		{Type: &servicepb.Request_SaveLedgerMetadata{}},
		{Type: &servicepb.Request_DeleteLedgerMetadata{}},
	}

	produced := map[string]bool{}
	for _, req := range reqs {
		kind := RequestLogKind(req)
		require.NotEmpty(t, kind, "%T names no kind", req.GetType())
		produced[AuditOrderTypeOfKind[kind]] = true
	}

	for kind, token := range AuditOrderTypeOfKind {
		require.True(t, produced[token], "no request produces kind %q", kind)
	}
}

// With the committed order types known, a filter mixing an order-type leaf
// with another field is decided in one pass: an Or holds only if one of its
// arms does.
func TestAuditEntryMatchesMixedOrInOnePass(t *testing.T) {
	t.Parallel()

	ledgerB := AuditEntry{Seq: 10, Ledgers: []string{"b"}, OrderCount: 1, MinLog: 40, MaxLog: 40}
	metadataOnly := map[string]bool{"add_metadata": true}
	mixed := FilterOr(
		FilterAuditString(commonpb.AuditField_AUDIT_FIELD_LEDGER, "a"),
		FilterAuditString(commonpb.AuditField_AUDIT_FIELD_ORDER_TYPE, "create_transaction"),
	)

	require.False(t, AuditEntryMatches(mixed, ledgerB, metadataOnly), "neither arm holds")
	require.True(t, AuditEntryMatches(mixed, ledgerB, map[string]bool{"create_transaction": true}), "the order-type arm holds")

	ledgerA := ledgerB
	ledgerA.Ledgers = []string{"a"}
	require.True(t, AuditEntryMatches(mixed, ledgerA, metadataOnly), "the ledger arm holds")
	require.True(t, AuditEntryMatches(mixed, ledgerB, nil), "unknown orders leave the order-type arm holding")
}
