package match

import (
	"context"
	"fmt"
	"testing"

	"github.com/formancehq/ledger/v3/internal/domain/indexes"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
	"github.com/formancehq/ledger/v3/tests/oracle"
	"github.com/formancehq/ledger/v3/tests/oracle/oracletest"
	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestIndexNeeds(t *testing.T) {
	t.Parallel()

	assertNeeds := func(asset, other bool, gotAsset, gotOther bool) {
		t.Helper()
		require.Equal(t, asset, gotAsset, "asset need")
		require.Equal(t, other, gotOther, "other need")
	}

	// nil / universe: no index.
	a, o := IndexNeeds(nil, accounts)
	assertNeeds(false, false, a, o)

	// has-asset → asset index only.
	a, o = IndexNeeds(FilterHasAsset("USD", 2), accounts)
	assertNeeds(true, false, a, o)

	// metadata field → a non-asset index on either target.
	a, o = IndexNeeds(FilterMetaExists("k"), accounts)
	assertNeeds(false, true, a, o)
	a, o = IndexNeeds(FilterMetaExists("k"), txns)
	assertNeeds(false, true, a, o)

	// index-free leaves.
	a, o = IndexNeeds(FilterReverted(true), txns)
	assertNeeds(false, false, a, o)
	a, o = IndexNeeds(FilterTxIDRange(1, 5), txns)
	assertNeeds(false, false, a, o)
	a, o = IndexNeeds(FilterAddrPrefix("acc:"), accounts)
	assertNeeds(false, false, a, o)

	// address is index-backed (non-asset) on transactions.
	a, o = IndexNeeds(FilterAddrPrefix("acc:"), txns)
	assertNeeds(false, true, a, o)

	// Combinators OR their children's needs.
	a, o = IndexNeeds(FilterAnd(FilterHasAsset("USD", 2), FilterAddrPrefix("acc:")), accounts)
	assertNeeds(true, false, a, o) // asset-only: the other operand is index-free
	a, o = IndexNeeds(FilterAnd(FilterHasAsset("USD", 2), FilterMetaExists("k")), accounts)
	assertNeeds(true, true, a, o) // needs both → not asset-only
	a, o = IndexNeeds(FilterNot(FilterHasAsset("USD", 2)), accounts)
	assertNeeds(true, false, a, o)
}

func TestAccountHasAsset(t *testing.T) {
	t.Parallel()

	// world → acc:1 in USD/2 and world → acc:2 in COIN (precision 0).
	ls := buildLedger(t,
		oracletest.TxReq("world", "acc:1", "USD/2", 5),
		oracletest.TxReq("world", "acc:2", "COIN", 5),
	)

	require.True(t, AccountHasAsset(ls, "acc:1", "USD", 2))
	require.False(t, AccountHasAsset(ls, "acc:1", "USD", 4)) // precision must match
	require.False(t, AccountHasAsset(ls, "acc:1", "EUR", 2))
	require.True(t, AccountHasAsset(ls, "acc:2", "COIN", 0))
	require.False(t, AccountHasAsset(ls, "acc:2", "COIN", 2))
	// A source is credited on the output side, so it holds the asset too.
	require.True(t, AccountHasAsset(ls, "world", "USD", 2))
	require.True(t, AccountHasAsset(ls, "world", "COIN", 0))
	require.False(t, AccountHasAsset(ls, "acc:absent", "USD", 2))
}

// TestAssetWindow checks the ever-touched account set and the has-asset page
// window: every account a USD/2 posting touched (both sides, including the
// source world) appears, ordered by address, with the exclusive cursor and
// reverse applied — and an account that only touched a different asset does not.
func TestAssetWindow(t *testing.T) {
	t.Parallel()

	ls := buildLedger(t,
		oracletest.TxReq("world", "acc:1", "USD/2", 5),
		oracletest.TxReq("world", "acc:2", "USD/2", 5),
		oracletest.TxReq("world", "acc:3", "EUR/2", 5),
	)

	// Ever-touched USD/2: acc:1, acc:2, world (source of both). acc:3 only EUR.
	require.Equal(t, []string{"acc:1", "acc:2", "world"}, ls.EverAssetAccounts("USD", 2))
	require.Equal(t, []string{"acc:3", "world"}, ls.EverAssetAccounts("EUR", 2))
	require.Empty(t, ls.EverAssetAccounts("USD", 4)) // precision must match

	// Full window, forward.
	require.Equal(t, []string{"acc:1", "acc:2", "world"},
		AssetWindow(ls, "USD", 2, "", 10, false))

	// pageSize cap.
	require.Equal(t, []string{"acc:1", "acc:2"},
		AssetWindow(ls, "USD", 2, "", 2, false))

	// Forward cursor is exclusive (drops <= cursor).
	require.Equal(t, []string{"acc:2", "world"},
		AssetWindow(ls, "USD", 2, "acc:1", 10, false))

	// Reverse order + exclusive cursor (drops >= cursor).
	require.Equal(t, []string{"acc:2", "acc:1"},
		AssetWindow(ls, "USD", 2, "world", 10, true))
}

// A multi-leaf filter can carry both an absent needed index and a
// kind-mismatched leaf; the surfaced rejection follows the compiler's walk
// order, so both honest error classes must be legal — but never results, and
// never a rejection class the filter cannot produce.
func TestIndexedQueryOutcomeLegal_MismatchAndAbsentCoexist(t *testing.T) {
	t.Parallel()

	acct := commonpb.TargetType_TARGET_TYPE_ACCOUNT

	// k0 declared UINT64 with an active index; k3 never declared, no index.
	gs := buildGlobal(t,
		oracletest.SetFieldTypeReq(acct, "k0", commonpb.MetadataType_METADATA_TYPE_UINT64),
		oracletest.CreateIndexReq(indexes.MetadataID(acct, "k0")),
	)
	gs.SetIndexActive("L", indexes.Canonical(indexes.MetadataID(acct, "k0")))
	ls := gs.Ledger("L")

	neg := int64(-5)
	mismatched := FilterFieldInt("k0", &neg, nil) // negative bound on unsigned: kind mismatch
	absent := FilterFieldExists("k3", false)

	both := FilterOr(mismatched, absent)
	needed := map[string]struct{}{}
	NeededIndexCanonicals(both, commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS, needed)

	noWindow := func(oracle.LedgerState) bool { return false }

	require.True(t, IndexedQueryOutcomeLegal(ls, commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS, both, needed, IndexedErrCompilation, "", noWindow),
		"the mismatched leaf makes a compilation rejection legal even with an absent sibling index")
	require.True(t, IndexedQueryOutcomeLegal(ls, commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS, both, needed, IndexedErrNotReady, "", noWindow),
		"the absent index keeps a not-ready rejection legal too")
	require.False(t, IndexedQueryOutcomeLegal(ls, commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS, both, needed, IndexedErrNone, "", noWindow),
		"results are never legal while a leaf mismatches")

	// Mismatch alone (all needed indexes active): compilation legal, not-ready illegal.
	alone := FilterOr(mismatched, mismatched)
	neededAlone := map[string]struct{}{}
	NeededIndexCanonicals(alone, commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS, neededAlone)

	require.True(t, IndexedQueryOutcomeLegal(ls, commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS, alone, neededAlone, IndexedErrCompilation, "", noWindow))
	require.False(t, IndexedQueryOutcomeLegal(ls, commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS, alone, neededAlone, IndexedErrNotReady, "", noWindow),
		"with every needed index active, a not-ready rejection is unexplained")
}

// fakeStatusClient serves a canned GetIndexStatus response, standing in for one
// node of PerNodeConns. Every other client method panics via the embedded nil
// interface — the poller must not call anything else.
type fakeStatusClient struct {
	servicepb.BucketServiceClient

	resp *servicepb.GetIndexStatusResponse
}

func (f fakeStatusClient) GetIndexStatus(context.Context, *servicepb.GetIndexStatusRequest, ...grpc.CallOption) (*servicepb.GetIndexStatusResponse, error) {
	return f.resp, nil
}

// TestIndexBuildingErrorReachesValidators pins the not-ready plumbing for
// ErrIndexBuilding, which rides codes.Unavailable and therefore sits inside
// internal.IsTransient: the query paths must peel it off the generic transient
// bail (isIndexNotReady) and the classifier must bucket it as not-ready —
// otherwise a server wrongly rejecting an active index is silently skipped and
// the lifecycle coverage never fires.
func TestIndexBuildingErrorReachesValidators(t *testing.T) {
	t.Parallel()

	st, err := status.New(codes.Unavailable, "index is building").WithDetails(&errdetails.ErrorInfo{
		Domain: "ledger",
		Reason: "INDEX_BUILDING",
	})
	require.NoError(t, err)

	building := st.Err()

	require.True(t, internal.IsTransient(building),
		"the trap this test guards: INDEX_BUILDING is inside the transient set")
	require.True(t, IsIndexNotReady(RPCErrorOf(building)))

	kind, ok := ClassifyIndexedQueryError(RPCErrorOf(building))
	require.True(t, ok)
	require.Equal(t, IndexedErrNotReady, kind)

	// A plain Unavailable (node down) stays a skippable transient.
	plain := status.Error(codes.Unavailable, "connection refused")
	require.True(t, internal.IsTransient(plain))
	require.False(t, IsIndexNotReady(RPCErrorOf(plain)))
}

// TestIndexNotFoundMatchedByReason pins the reason requirement on the
// index-absent rejection: a FailedPrecondition carrying an unrelated reason
// (or none) must stay a finding, never be explained away as a missing index.
func TestIndexNotFoundMatchedByReason(t *testing.T) {
	t.Parallel()

	st, err := status.New(codes.FailedPrecondition, "index not found").WithDetails(&errdetails.ErrorInfo{
		Domain: "ledger",
		Reason: "INDEX_NOT_FOUND",
	})
	require.NoError(t, err)

	kind, ok := ClassifyIndexedQueryError(RPCErrorOf(st.Err()))
	require.True(t, ok)
	require.Equal(t, IndexedErrNotReady, kind)
	require.True(t, IsIndexNotFound(RPCErrorOf(st.Err())))

	// Bare FailedPrecondition: not part of the indexed-query surface.
	_, ok = ClassifyIndexedQueryError(RPCErrorOf(status.Error(codes.FailedPrecondition, "boom")))
	require.False(t, ok, "a reasonless precondition error must stay a finding")

	// Unrelated reason on the same code: also a finding.
	other, err := status.New(codes.FailedPrecondition, "transient account non-zero").WithDetails(&errdetails.ErrorInfo{
		Domain: "ledger",
		Reason: "TRANSIENT_ACCOUNT_NON_ZERO",
	})
	require.NoError(t, err)
	_, ok = ClassifyIndexedQueryError(RPCErrorOf(other.Err()))
	require.False(t, ok)
	require.False(t, IsIndexNotFound(RPCErrorOf(other.Err())))
}

// TestIndexNotReadyRequiresDocumentedCode pins isIndexNotReady to the
// documented wire shape: the INDEX_BUILDING reason on any other status code is
// an API error-code regression and must surface as a finding, not be accepted
// as the expected not-ready response.
func TestIndexNotReadyRequiresDocumentedCode(t *testing.T) {
	t.Parallel()

	wrongCode, err := status.New(codes.FailedPrecondition, "index is building").WithDetails(&errdetails.ErrorInfo{
		Domain: "ledger",
		Reason: "INDEX_BUILDING",
	})
	require.NoError(t, err)

	require.False(t, IsIndexNotReady(RPCErrorOf(wrongCode.Err())),
		"INDEX_BUILDING on a non-Unavailable code is a regression, not a not-ready response")
	require.False(t, IsIndexNotFound(RPCErrorOf(wrongCode.Err())))

	_, ok := ClassifyIndexedQueryError(RPCErrorOf(wrongCode.Err()))
	require.False(t, ok, "the wrong-code shape must stay a finding")
}

// An open window for one filter leaf cannot explain a refusal attributed to
// its active sibling. Both codes carry the server's index identity, so keep
// that identity through classification and candidate validation.
func TestIndexedQueryOutcomeLegal_RetypeRefusalAttribution(t *testing.T) {
	t.Parallel()

	for _, target := range []struct {
		name   string
		typeID commonpb.TargetType
		query  commonpb.QueryTarget
	}{
		{"accounts", commonpb.TargetType_TARGET_TYPE_ACCOUNT, accounts},
		{"transactions", commonpb.TargetType_TARGET_TYPE_TRANSACTION, txns},
	} {
		t.Run(target.name, func(t *testing.T) {
			t.Parallel()
			gs := buildGlobal(t,
				oracletest.SetFieldTypeReq(target.typeID, "changing", commonpb.MetadataType_METADATA_TYPE_STRING),
				oracletest.CreateIndexReq(indexes.MetadataID(target.typeID, "changing")),
				oracletest.SetFieldTypeReq(target.typeID, "sibling", commonpb.MetadataType_METADATA_TYPE_STRING),
				oracletest.CreateIndexReq(indexes.MetadataID(target.typeID, "sibling")),
				oracletest.SetFieldTypeReq(target.typeID, "changing", commonpb.MetadataType_METADATA_TYPE_INT64),
			)
			for _, key := range []string{"changing", "sibling"} {
				gs.SetIndexActive("L", indexes.Canonical(indexes.MetadataID(target.typeID, key)))
			}
			filter := FilterAnd(FilterMetaExists("changing"), FilterMetaExists("sibling"))
			needed := map[string]struct{}{}
			NeededIndexCanonicals(filter, target.query, needed)
			_, open := gs.Ledger("L").RetypeWindow(MetadataCanonical(target.query, "changing"))
			require.True(t, open, "the setup must actually exercise an open window")

			for _, refusal := range []struct {
				code   codes.Code
				reason string
			}{
				{codes.Unavailable, "INDEX_BUILDING"},
				{codes.FailedPrecondition, "INDEX_NOT_FOUND"},
			} {
				for _, key := range []string{"changing", "sibling", "unreferenced", ""} {
					label := ""
					if key != "" {
						label = fmt.Sprintf("metadata[%q] on %s", key, target.name)
					}
					st, err := status.New(refusal.code, "index refusal").WithDetails(&errdetails.ErrorInfo{
						Domain: "ledger", Reason: refusal.reason, Metadata: map[string]string{"index": label},
					})
					require.NoError(t, err)
					errKind, ok := ClassifyIndexedQueryError(RPCErrorOf(st.Err()))
					require.True(t, ok)
					legal := IndexedQueryOutcomeLegal(gs.Ledger("L"), target.query, filter, needed, errKind, RejectedIndexLabel(RPCErrorOf(st.Err())), func(oracle.LedgerState) bool {
						t.Fatal("a refusal must not use the result-window branch")

						return false
					})
					require.Equal(t, key == "changing", legal, "%s attributed to %q", refusal.reason, label)
				}
			}
			gs.CloseRetypeWindow("L", MetadataCanonical(target.query, "changing"))
			require.False(t, IndexedQueryOutcomeLegal(gs.Ledger("L"), target.query, filter, needed, IndexedErrNotReady,
				fmt.Sprintf("metadata[%q] on %s", "changing", target.name), func(oracle.LedgerState) bool { return false }),
				"once the window closes, its active index must serve results again")
		})
	}
}
