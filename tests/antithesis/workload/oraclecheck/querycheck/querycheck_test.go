package querycheck

import (
	"encoding/json"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/formancehq/ledger/v3/internal/domain/indexes"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/internal/proto/signaturepb"
	"github.com/formancehq/ledger/v3/pkg/pagecursor"
	oc "github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck/match"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck/match/matchtest"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck/testutil"
	"github.com/formancehq/ledger/v3/tests/oracle"
	"github.com/formancehq/ledger/v3/tests/oracle/oracletest"
)

const L = "L"

// session is a bare scripted session holding ledger "L" with a few committed
// transactions (ids 1..4, one with a reference).
func session(t *testing.T) *testutil.ScriptedSession {
	t.Helper()
	s := testutil.NewBareSession()
	require.Empty(t, testutil.Findings(s.Write(false, testutil.CreateLedgerReq(L))))
	require.Empty(t, testutil.Findings(s.Write(false,
		oracletest.TxReq("world", "t-0:1", "USD/2", 10),
		oracletest.TxReqRefL(L, "r1", "world", "t-0:2", "EUR/2", 7),
	)))
	require.Empty(t, testutil.Findings(s.Write(false, oracletest.TxReq("t-0:1", "t-0:3", "USD/2", 3))))
	require.Empty(t, testutil.Findings(s.Write(false, oracletest.AddAccountMetaReq("t-0:1", "k0", matchtest.Str("v0")))))
	require.Empty(t, testutil.Findings(s.Write(false, oracletest.TxReq("world", "t-1:5", "COIN", 1))))

	return s
}

func listReq(t *testing.T, ledger string, filter *commonpb.QueryFilter, pageSize uint32, cursor string, reverse bool) json.RawMessage {
	t.Helper()
	b, err := protojson.Marshal(&servicepb.ListTransactionsRequest{Ledger: ledger, Options: &commonpb.ListOptions{
		PageSize: pageSize, Cursor: cursor, Reverse: reverse, Filter: filter,
	}})
	require.NoError(t, err)

	return b
}

func page[M proto.Message](t *testing.T, items []M, next string) oc.Outcome {
	t.Helper()
	out := oc.Outcome{OK: true, Complete: true, NextCursor: next}
	for _, m := range items {
		b, err := protojson.Marshal(m)
		require.NoError(t, err)
		out.Items = append(out.Items, b)
	}

	return out
}

func fail(code, reason string, md map[string]string) oc.Outcome {
	return oc.Outcome{Code: code, Reason: reason, Message: reason, Metadata: md}
}

func ctx(s *testutil.ScriptedSession, kind string, req json.RawMessage, out oc.Outcome) *oc.ReadCtx {
	tk := s.Read(kind)

	return &oc.ReadCtx{T: s.T, Ticket: tk, MaxTicket: s.T.ReadFrontier(), Kind: kind, Request: req, Outcome: out, Snapshot: s.T.Model()}
}

func ids(evs []oc.Event) []oc.EventID { return testutil.IDs(evs) }

func findings(evs []oc.Event) []oc.EventID { return testutil.IDs(testutil.Findings(evs)) }

func token(key string) string { return pagecursor.Cursor{Key: key}.Encode() }

// ---- ListTransactions ----

func TestListTransactions_ServedWindowAndInvertedReverse(t *testing.T) {
	t.Parallel()
	s := session(t)
	for _, reverse := range []bool{false, true} {
		for _, ps := range []uint32{1, 2, 10} {
			txs, next := matchtest.RenderTxPage(s.T.Model(), L, matchtest.TxPageReq{PageSize: ps, Reverse: reverse})
			evs := ValidateListTransactions(ctx(s, "ListTransactions", listReq(t, L, nil, ps, "", reverse), page(t, txs, next)))
			require.Empty(t, findings(evs), "reverse=%v ps=%d", reverse, ps)
		}
	}

	// reverse=false is newest first: an ascending page is outside the model.
	asc, _ := matchtest.RenderTxPage(s.T.Model(), L, matchtest.TxPageReq{PageSize: 10, Reverse: true})
	require.Greater(t, asc[0].GetId(), uint64(0))
	require.Less(t, asc[0].GetId(), asc[len(asc)-1].GetId(), "reverse=true is ascending")
	evs := ValidateListTransactions(ctx(s, "ListTransactions", listReq(t, L, nil, 10, "", false), page(t, asc, "")))
	require.Equal(t, []oc.EventID{EvTransactionQueryOutsideModel}, findings(evs))
}

func TestListTransactions_CursorLegality(t *testing.T) {
	t.Parallel()
	s := session(t)
	txs, next := matchtest.RenderTxPage(s.T.Model(), L, matchtest.TxPageReq{PageSize: 2, AfterID: 4, Reverse: false})
	require.NotEmpty(t, next)
	req := listReq(t, L, nil, 2, token("4"), false)
	require.Empty(t, findings(ValidateListTransactions(ctx(s, "ListTransactions", req, page(t, txs, next)))))
	// A full page whose further rows the base requires must carry a token.
	require.Equal(t, []oc.EventID{EvTransactionQueryOutsideModel},
		findings(ValidateListTransactions(ctx(s, "ListTransactions", req, page(t, txs, "")))))
	// A token naming another row than the last one sent.
	require.Equal(t, []oc.EventID{EvTransactionQueryOutsideModel},
		findings(ValidateListTransactions(ctx(s, "ListTransactions", req, page(t, txs, token("1"))))))
	// A dropped row.
	require.Equal(t, []oc.EventID{EvTransactionQueryOutsideModel},
		findings(ValidateListTransactions(ctx(s, "ListTransactions", req, page(t, txs[:1], "")))))
}

func TestListTransactions_OptionalRowsForUnlearnedStamps(t *testing.T) {
	t.Parallel()
	s := session(t)
	tsIdx := indexes.TxBuiltinID(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_TIMESTAMP)
	require.Empty(t, testutil.Findings(s.Write(false, oracletest.CreateIndexReq(tsIdx))))
	before := s.T.Model()

	// A transaction in flight: folded into a candidate base without a learned
	// timestamp, so a date filter may or may not show it.
	inflight := s.Begin(false, oracletest.TxReq("world", "t-2:1", "USD/2", 1))
	_ = inflight
	lo, hi := uint64(0), uint64(1<<62)
	filter := match.FilterDateRange(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_TIMESTAMP, lo, hi)
	req := listReq(t, L, filter, 50, "", true)

	without, _ := matchtest.RenderTxPage(before, L, matchtest.TxPageReq{Filter: filter, PageSize: 50, Reverse: true})
	require.Len(t, without, 4)
	evs := ValidateListTransactions(ctx(s, "ListTransactions", req, page(t, without, "")))
	require.Empty(t, findings(evs))
	require.Contains(t, ids(evs), EvIndexedTxQueryServed)

	server := s.Server.Apply(oraclecheckBulk(s, inflight))
	require.True(t, server.OK)
	with := append(slices.Clone(without), matchtest.RenderTx(server.State, L, 5))
	require.Empty(t, findings(ValidateListTransactions(ctx(s, "ListTransactions", req, page(t, with, "")))))

	// Coverage: the timestamp probe is true on a verified page with rows.
	var hit bool
	for _, e := range evs {
		if e.Kind == oc.EvRaw && e.ID == CoverageIndexID(match.TxBuiltinCanonical(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_TIMESTAMP)) {
			hit = e.Cond
		}
	}
	require.True(t, hit)
}

func oraclecheckBulk(s *testutil.ScriptedSession, ticket uint64) oracle.Bulk {
	rec, _ := s.T.RecordByTicket(ticket)

	return oracle.Bulk{Requests: rec.Requests}
}

func TestListTransactions_IndexGating(t *testing.T) {
	t.Parallel()
	s := session(t)
	filter := match.FilterReference("r1")
	req := listReq(t, L, filter, 10, "", false)
	md := map[string]string{"index": "reference"}
	evs := ValidateListTransactions(ctx(s, "ListTransactions", req, fail("FailedPrecondition", "INDEX_NOT_FOUND", md)))
	require.Empty(t, findings(evs))
	require.Contains(t, ids(evs), EvIndexedTxQueryGated)
	// Rows served without the index.
	tx := matchtest.RenderTx(s.T.Model(), L, 2)
	require.Equal(t, []oc.EventID{EvIndexedTransactionQueryOutside},
		findings(ValidateListTransactions(ctx(s, "ListTransactions", req, page(t, []*commonpb.Transaction{tx}, "")))))
	// An undocumented error class.
	require.Equal(t, []oc.EventID{EvIndexedTxUnexpectedError},
		findings(ValidateListTransactions(ctx(s, "ListTransactions", req, fail("FailedPrecondition", "", nil)))))
	// Once the index exists, the page is the model's.
	require.Empty(t, testutil.Findings(s.Write(false, oracletest.CreateIndexReq(indexes.TxBuiltinID(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE)))))
	evs = ValidateListTransactions(ctx(s, "ListTransactions", req, page(t, []*commonpb.Transaction{tx}, "")))
	require.Empty(t, findings(evs))
	require.Contains(t, ids(evs), EvIndexedTxQueryServed)
}

func TestListTransactions_MalformedCursorClass(t *testing.T) {
	t.Parallel()
	s := session(t)
	for _, bad := range []string{"not-a-cursor", "-1", "0x10", " 12", "12 ", "1.0", "18446744073709551616"} {
		for _, cur := range []string{bad, token(bad)} {
			_, malformed := UintCursor(cur)
			require.True(t, malformed, "%q", cur)
			req := listReq(t, L, nil, 5, cur, false)
			evs := ValidateListTransactions(ctx(s, "ListTransactions", req, fail("InvalidArgument", "", nil)))
			require.Equal(t, []oc.EventID{EvMalformedTxCursorRejected}, ids(evs))
			require.Equal(t, []oc.EventID{EvMalformedCursorUnexpectedError},
				findings(ValidateListTransactions(ctx(s, "ListTransactions", req, fail("Internal", "", nil)))))
			require.Equal(t, []oc.EventID{EvMalformedTxCursorResults},
				findings(ValidateListTransactions(ctx(s, "ListTransactions", req, page[*commonpb.Transaction](t, nil, "")))))
		}
	}
	id, malformed := UintCursor(token("12"))
	require.False(t, malformed)
	require.Equal(t, uint64(12), id)
}

func TestListTransactions_ProbesThatMustBeRefused(t *testing.T) {
	t.Parallel()
	s := session(t)
	lo, hi := uint64(1), uint64(1_000_000)
	unsupported := &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_BuiltinUint{BuiltinUint: &commonpb.BuiltinUintCondition{
		Field: commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_SOURCE_ADDRESS, Cond: &commonpb.UintCondition{Min: &lo, Max: &hi},
	}}}
	req := listReq(t, L, unsupported, 5, "", false)
	require.Equal(t, []oc.EventID{EvUnsupportedBuiltinRejected}, ids(ValidateListTransactions(ctx(s, "ListTransactions", req, fail("InvalidArgument", "FILTER_COMPILATION_ERROR", nil)))))
	require.Equal(t, []oc.EventID{EvUnsupportedBuiltinResults}, findings(ValidateListTransactions(ctx(s, "ListTransactions", req, page[*commonpb.Transaction](t, nil, "")))))
	require.Equal(t, []oc.EventID{EvUnsupportedBuiltinUnexpectedErr}, findings(ValidateListTransactions(ctx(s, "ListTransactions", req, fail("Internal", "", nil)))))

	hasAsset := match.FilterHasAsset("USD", 2)
	req = listReq(t, L, hasAsset, 5, "", false)
	require.Equal(t, []oc.EventID{EvTargetInvalidRejectedTxs}, ids(ValidateListTransactions(ctx(s, "ListTransactions", req, fail("InvalidArgument", "", nil)))))
	require.Equal(t, []oc.EventID{EvTargetInvalidTxResults}, findings(ValidateListTransactions(ctx(s, "ListTransactions", req, page[*commonpb.Transaction](t, nil, "")))))
	require.Equal(t, []oc.EventID{EvTargetInvalidUnexpectedError}, findings(ValidateListTransactions(ctx(s, "ListTransactions", req, fail("NotImplemented", "", nil)))))
}

func TestListTransactions_PurgedAccountNotReachedByAddress(t *testing.T) {
	t.Parallel()
	s := testutil.NewBareSession()
	require.Empty(t, testutil.Findings(s.Write(false, testutil.CreateLedgerReq(L))))
	require.Empty(t, testutil.Findings(s.Write(false,
		oracletest.AddTypeReqP("t-1", commonpb.AccountTypePersistence_ACCOUNT_TYPE_EPHEMERAL),
		oracletest.AddTypeReq("world"),
		oracletest.CreateIndexReq(indexes.TxBuiltinID(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_ADDRESS)),
	)))
	require.Empty(t, testutil.Findings(s.Write(false, oracletest.TxReq("world", "t-1:1", "USD/2", 5))))
	require.Empty(t, testutil.Findings(s.Write(false, oracletest.TxReq("t-1:1", "world", "USD/2", 5))))
	ls := s.T.Model().Ledger(L)
	require.False(t, ls.HasAccount("t-1:1"), "the drained ephemeral account is purged")

	filter := match.FilterAddrExact("t-1:1")
	req := listReq(t, L, filter, 10, "", false)
	evs := ValidateListTransactions(ctx(s, "ListTransactions", req, page[*commonpb.Transaction](t, nil, "")))
	require.Empty(t, findings(evs))
	both := []*commonpb.Transaction{matchtest.RenderTx(s.T.Model(), L, 2), matchtest.RenderTx(s.T.Model(), L, 1)}
	require.Equal(t, []oc.EventID{EvIndexedTransactionQueryOutside},
		findings(ValidateListTransactions(ctx(s, "ListTransactions", req, page(t, both, "")))))
}

func TestListTransactions_LedgerNotFound(t *testing.T) {
	t.Parallel()
	s := session(t)
	req := listReq(t, "absent", nil, 5, "", false)
	require.Empty(t, findings(ValidateListTransactions(ctx(s, "ListTransactions", req, fail("NotFound", "LEDGER_NOT_FOUND", nil)))))
	req = listReq(t, L, nil, 5, "", false)
	require.Equal(t, []oc.EventID{oc.EvLedgerNotFoundUnexplained},
		findings(ValidateListTransactions(ctx(s, "ListTransactions", req, fail("NotFound", "LEDGER_NOT_FOUND", nil)))))
	require.Equal(t, []oc.EventID{EvListTransactionsUnexpectedError},
		findings(ValidateListTransactions(ctx(s, "ListTransactions", req, fail("Internal", "", nil)))))
}

func TestPageSizeCoverage(t *testing.T) {
	t.Parallel()
	s := session(t)
	txs, next := matchtest.RenderTxPage(s.T.Model(), L, matchtest.TxPageReq{PageSize: 0})
	evs := ValidateListTransactions(ctx(s, "ListTransactions", listReq(t, L, nil, 0, "", false), page(t, txs, next)))
	require.Empty(t, findings(evs))
	require.Contains(t, ids(evs), EvPageSizeServerDefault)
	txs, next = matchtest.RenderTxPage(s.T.Model(), L, matchtest.TxPageReq{PageSize: 4000})
	evs = ValidateListTransactions(ctx(s, "ListTransactions", listReq(t, L, nil, 4000, "", false), page(t, txs, next)))
	require.Empty(t, findings(evs))
	require.Contains(t, ids(evs), EvPageSizeClamped)
}

// ---- ListAccounts ----

func acctReq(t *testing.T, filter *commonpb.QueryFilter, pageSize uint32, cursor string, reverse bool) json.RawMessage {
	t.Helper()
	b, err := protojson.Marshal(&servicepb.ListAccountsRequest{Ledger: L, Options: &commonpb.ListOptions{
		PageSize: pageSize, Cursor: cursor, Reverse: reverse, Filter: filter,
	}})
	require.NoError(t, err)

	return b
}

func TestListAccounts_IndexFreeWindow(t *testing.T) {
	t.Parallel()
	s := session(t)
	for _, f := range []*commonpb.QueryFilter{nil, match.FilterAddrPrefix("t-0:"), match.FilterNot(match.FilterAddrExact("world"))} {
		for _, reverse := range []bool{false, true} {
			for _, cur := range []string{"", "t-0:1"} {
				accts, next := matchtest.RenderAccountPage(s.T.Model(), L, matchtest.AccountPageReq{Filter: f, Cursor: cur, PageSize: 2, Reverse: reverse})
				req := acctReq(t, f, 2, match.PageToken(cur), reverse)
				require.Empty(t, findings(ValidateListAccounts(ctx(s, "ListAccounts", req, page(t, accts, next)))))
				if len(accts) > 0 {
					bad := proto.Clone(accts[0]).(*commonpb.Account)
					bad.Address = "t-9:9"
					rows := append([]*commonpb.Account{bad}, accts[1:]...)
					require.Equal(t, []oc.EventID{EvAccountQueryOutsideModel}, findings(ValidateListAccounts(ctx(s, "ListAccounts", req, page(t, rows, next)))))
				}
			}
		}
	}
}

func TestListAccounts_TargetInvalidAndRefused(t *testing.T) {
	t.Parallel()
	s := session(t)
	req := acctReq(t, match.FilterReverted(true), 5, "", false)
	require.Equal(t, []oc.EventID{EvTargetInvalidRejectedAccounts}, ids(ValidateListAccounts(ctx(s, "ListAccounts", req, fail("InvalidArgument", "", nil)))))
	require.Equal(t, []oc.EventID{EvTargetInvalidAccountResults}, findings(ValidateListAccounts(ctx(s, "ListAccounts", req, page[*commonpb.Account](t, nil, "")))))

	// A refused filter whose earlier leaf needs an index the base lacks: the
	// not-ready answer naming that index is legal, any other is not.
	refused := match.FilterAnd(match.FilterMetaExists("k0"), &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Audit{Audit: &commonpb.AuditCondition{}}})
	require.True(t, match.ClassifyRejectedFilter(refused, targetAccounts).Rejected)
	req = acctReq(t, refused, 5, "", false)
	require.Equal(t, []oc.EventID{EvRefusedAccountFilterRejected}, ids(ValidateListAccounts(ctx(s, "ListAccounts", req, fail("InvalidArgument", "", nil)))))
	label := map[string]string{"index": `metadata["k0"] on accounts`}
	require.Empty(t, ValidateListAccounts(ctx(s, "ListAccounts", req, fail("FailedPrecondition", "INDEX_NOT_FOUND", label))))
	require.Equal(t, []oc.EventID{EvRefusedFilterUnexpectedError}, findings(ValidateListAccounts(ctx(s, "ListAccounts", req,
		fail("FailedPrecondition", "INDEX_NOT_FOUND", map[string]string{"index": `metadata["k3"] on accounts`})))))
	require.Equal(t, []oc.EventID{EvRefusedFilterResults}, findings(ValidateListAccounts(ctx(s, "ListAccounts", req, page[*commonpb.Account](t, nil, "")))))
}

func TestListAccounts_IndexedAndAssetGating(t *testing.T) {
	t.Parallel()
	s := session(t)
	req := acctReq(t, match.FilterMetaExists("undeclared-k1"), 5, "", false)
	evs := ValidateListAccounts(ctx(s, "ListAccounts", req, fail("FailedPrecondition", "INDEX_NOT_FOUND", map[string]string{"index": `metadata["undeclared-k1"] on accounts`})))
	require.Empty(t, findings(evs))
	require.Contains(t, ids(evs), EvIndexedAccountQueryGated)

	asset := match.FilterHasAsset("USD", 2)
	req = acctReq(t, asset, 5, "", false)
	evs = ValidateListAccounts(ctx(s, "ListAccounts", req, fail("FailedPrecondition", "INDEX_NOT_FOUND", map[string]string{"index": "has asset"})))
	require.Contains(t, ids(evs), EvAssetIndexAccountQueryGated)
	require.Empty(t, findings(evs))
	window := match.AssetWindow(s.T.Model().Ledger(L), "USD", 2, "", 5, false)
	require.NotEmpty(t, window)
	var accts []*commonpb.Account
	for _, a := range window {
		accts = append(accts, matchtest.RenderAccount(s.T.Model(), L, a, false))
	}
	require.Equal(t, []oc.EventID{EvAssetIndexAccountQueryOutside}, findings(ValidateListAccounts(ctx(s, "ListAccounts", req, page(t, accts, "")))))
	require.Equal(t, []oc.EventID{EvAssetIndexUnexpectedError}, findings(ValidateListAccounts(ctx(s, "ListAccounts", req, fail("Internal", "", nil)))))

	require.Empty(t, testutil.Findings(s.Write(false, oracletest.CreateIndexReq(match.AssetIndexID()))))
	evs = ValidateListAccounts(ctx(s, "ListAccounts", req, page(t, accts, "")))
	require.Empty(t, findings(evs))
	require.Contains(t, ids(evs), EvAssetIndexAccountQueryServed)
	require.Contains(t, ids(evs), oc.EventID(CoverageIndexID(match.AssetIndexCanonical)))
}

// ---- ListLogs ----

func logReq(t *testing.T, ledger string, filter *commonpb.QueryFilter, pageSize uint32, cursor string, reverse bool) json.RawMessage {
	t.Helper()
	b, err := protojson.Marshal(&servicepb.ListLogsRequest{Ledger: ledger, Options: &commonpb.ListOptions{
		PageSize: pageSize, Cursor: cursor, Reverse: reverse, Filter: filter,
	}})
	require.NoError(t, err)

	return b
}

func TestListLogs_ServedAndReverse(t *testing.T) {
	t.Parallel()
	s := session(t)
	for _, reverse := range []bool{false, true} {
		logs, next, ok := matchtest.RenderLogPage(s.T.Model(), L, matchtest.LogPageReq{PageSize: 3, Reverse: reverse})
		require.True(t, ok)
		evs := ValidateListLogs(ctx(s, "ListLogs", logReq(t, L, nil, 3, "", reverse), page(t, logs, next)))
		require.Empty(t, findings(evs), "reverse=%v", reverse)
		require.Contains(t, ids(evs), EvLogQueryServed)
		if reverse {
			require.Contains(t, ids(evs), EvReverseLogQueryServed)
		}
	}
}

func TestListLogs_SelfStructure(t *testing.T) {
	t.Parallel()
	s := session(t)
	logs, _, ok := matchtest.RenderLogPage(s.T.Model(), L, matchtest.LogPageReq{PageSize: 3})
	require.True(t, ok)
	reversed := slices.Clone(logs)
	slices.Reverse(reversed)
	require.Equal(t, []oc.EventID{EvLogPageViolatesOrdering},
		findings(ValidateListLogs(ctx(s, "ListLogs", logReq(t, L, nil, 3, "", false), page(t, reversed, "")))))
	require.Equal(t, []oc.EventID{EvLogPageViolatesOrdering},
		findings(ValidateListLogs(ctx(s, "ListLogs", logReq(t, L, nil, 2, "", false), page(t, logs, "")))))
	// Inside the cursor.
	require.Equal(t, []oc.EventID{EvLogPageViolatesOrdering},
		findings(ValidateListLogs(ctx(s, "ListLogs", logReq(t, L, nil, 3, token("2"), false), page(t, logs, "")))))

	// A signature on a log of an unsigned tracked bulk.
	signed := proto.Clone(logs[0]).(*commonpb.Log)
	signed.ResponseSignature = &signaturepb.SignedLog{}
	signed.Sequence = 1
	rows := append([]*commonpb.Log{signed}, logs[1:]...)
	require.Equal(t, []oc.EventID{EvUnsignedLogCarriesSignature},
		findings(ValidateListLogs(ctx(s, "ListLogs", logReq(t, L, nil, 3, "", false), page(t, rows, "")))))
}

func TestListLogs_GatingMalformedRefused(t *testing.T) {
	t.Parallel()
	s := session(t)
	dateLeaf := &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_LogBuiltinUint{LogBuiltinUint: &commonpb.LogBuiltinUintCondition{
		Field: commonpb.LogBuiltinIndex_LOG_BUILTIN_INDEX_DATE, Cond: &commonpb.UintCondition{},
	}}}
	req := logReq(t, L, dateLeaf, 5, "", false)
	evs := ValidateListLogs(ctx(s, "ListLogs", req, fail("FailedPrecondition", "INDEX_NOT_FOUND", map[string]string{"index": "log date"})))
	require.Empty(t, findings(evs))
	require.Contains(t, ids(evs), EvLogQueryGated)
	logs, _, _ := matchtest.RenderLogPage(s.T.Model(), L, matchtest.LogPageReq{PageSize: 5})
	require.Equal(t, []oc.EventID{EvLogQueryOutsideModel}, findings(ValidateListLogs(ctx(s, "ListLogs", req, page(t, logs, token(""))))))

	// An index-free filter may not be gated.
	idLeaf := &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_LogId{LogId: &commonpb.LogIdCondition{Cond: &commonpb.UintCondition{}}}}
	require.Equal(t, []oc.EventID{EvLogQueryOutsideModel}, findings(ValidateListLogs(ctx(s, "ListLogs", logReq(t, L, idLeaf, 5, "", false),
		fail("FailedPrecondition", "INDEX_NOT_FOUND", map[string]string{"index": "log date"})))))

	req = logReq(t, L, nil, 5, "not-a-cursor", false)
	require.Equal(t, []oc.EventID{EvMalformedLogCursorRejected}, ids(ValidateListLogs(ctx(s, "ListLogs", req, fail("InvalidArgument", "", nil)))))
	require.Equal(t, []oc.EventID{EvMalformedLogCursorResults}, findings(ValidateListLogs(ctx(s, "ListLogs", req, page[*commonpb.Log](t, nil, "")))))
	require.Equal(t, []oc.EventID{EvMalformedCursorUnexpectedError}, findings(ValidateListLogs(ctx(s, "ListLogs", req, fail("Internal", "", nil)))))
	// A gate cannot answer ahead of the cursor refusal.
	require.Equal(t, []oc.EventID{EvMalformedCursorUnexpectedError}, findings(ValidateListLogs(ctx(s, "ListLogs", logReq(t, L, dateLeaf, 5, "not-a-cursor", false),
		fail("FailedPrecondition", "INDEX_NOT_FOUND", map[string]string{"index": "log date"})))))

	refused := &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Reverted{Reverted: &commonpb.RevertedCondition{}}}
	req = logReq(t, L, refused, 5, "", false)
	require.Equal(t, []oc.EventID{EvRefusedLogFilterRejected}, ids(ValidateListLogs(ctx(s, "ListLogs", req, fail("InvalidArgument", "", nil)))))
	require.Equal(t, []oc.EventID{EvRefusedFilterResults}, findings(ValidateListLogs(ctx(s, "ListLogs", req, page[*commonpb.Log](t, nil, "")))))
	// Malformed cursor and refused filter together: the cursor refusal may win.
	req = logReq(t, L, refused, 5, "-1", false)
	require.Equal(t, []oc.EventID{EvMalformedLogCursorRejected}, ids(ValidateListLogs(ctx(s, "ListLogs", req, fail("InvalidArgument", "", nil)))))

	require.Empty(t, findings(ValidateListLogs(ctx(s, "ListLogs", logReq(t, "nope", nil, 5, "", false), fail("NotFound", "", nil)))))
	require.Equal(t, []oc.EventID{EvListLogsUnexpectedError}, findings(ValidateListLogs(ctx(s, "ListLogs", logReq(t, L, nil, 5, "", false), fail("Internal", "", nil)))))
}

func TestCoverageIDsAreDistinctAndPrefixed(t *testing.T) {
	t.Parallel()
	seen := map[oc.EventID]bool{}
	for _, id := range CoverageIDs() {
		require.False(t, seen[id], "%s", id)
		seen[id] = true
		require.Contains(t, string(id), "oracle: coverage ")
	}
	require.Len(t, seen, len(match.WorkloadIndexes())+3)
}
