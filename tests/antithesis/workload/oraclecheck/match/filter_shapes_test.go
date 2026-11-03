package match

import (
	"testing"

	"github.com/formancehq/ledger/v3/internal/domain/indexes"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/tests/oracle"
	"github.com/formancehq/ledger/v3/tests/oracle/oracletest"
	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestClassifyRejectedFilter(t *testing.T) {
	t.Parallel()

	accounts := commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS
	k0 := indexes.Canonical(indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_ACCOUNT, "k0"))
	broken := &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Address{Address: &commonpb.AddressMatch{}}}
	param := &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Address{Address: &commonpb.AddressMatch{
		Match: &commonpb.AddressMatch_ParamPrefix{ParamPrefix: "p0"},
	}}}

	require.False(t, ClassifyRejectedFilter(FilterAnd(FilterMetaExists("k0"), FilterAddrPrefix("a:")), accounts).Rejected)

	rf := ClassifyRejectedFilter(FilterAnd(FilterMetaExists("k0"), broken), accounts)
	require.True(t, rf.Rejected)
	require.Contains(t, rf.Prior, k0, "an index compiled before the refused leaf may answer not-ready first")

	rf = ClassifyRejectedFilter(FilterAnd(broken, FilterMetaExists("k0")), accounts)
	require.True(t, rf.Rejected)
	require.Empty(t, rf.Prior, "nothing is compiled before the refused leaf")

	require.True(t, ClassifyRejectedFilter(param, accounts).Rejected, "a list query never binds a parameter")
	require.True(t, ClassifyRejectedFilter(&commonpb.QueryFilter{}, accounts).Rejected)
	require.True(t, ClassifyRejectedFilter(FilterHasAsset("USD", 256), accounts).Rejected)
	require.True(t, ClassifyRejectedFilter(FilterNot(FilterReverted(true)), accounts).Rejected)
}

// A has-asset leaf selects accounts whose volume rows are gone; every other arm,
// and the complement a Not takes, selects only current accounts.
func TestHasAssetComposesOverCurrentAccounts(t *testing.T) {
	t.Parallel()

	ls := buildLedger(t, oracletest.TxReq("world", "acc:1", "USD/2", 1))

	require.True(t, MatchAccountFilterIn(ls, FilterHasAsset("USD", 2), "gone:1", false) == ls.HasEverAsset("gone:1", "USD", 2))
	require.False(t, MatchAccountFilterIn(ls, FilterNot(FilterHasAsset("EUR", 2)), "gone:1", false))
	require.False(t, MatchAccountFilterIn(ls, FilterAddrPrefix("gone:"), "gone:1", false))
	require.False(t, MatchAccountFilterIn(ls, FilterAnd(), "gone:1", false))
	require.True(t, MatchAccountFilterIn(ls, FilterAnd(), "acc:1", true))
	require.Equal(t, []string{"acc:1"}, AccountWindow(ls, FilterAnd(FilterHasAsset("USD", 2), FilterAddrPrefix("acc:")), "", 10, false))
	require.Equal(t, []string{"world"}, AccountWindow(ls, FilterAnd(FilterHasAsset("USD", 2), FilterNot(FilterAddrPrefix("acc:"))), "", 10, false))
}

// A leaf compiled before the refused one may answer not-ready while its index
// is not active, or while its key's retype window is open even though its index
// is active — but only when the refusal names that index.
func TestRefusedFilterAcceptsAWindowedEarlierLeaf(t *testing.T) {
	t.Parallel()

	accounts := commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS
	id := indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_ACCOUNT, "k0")
	canon := indexes.Canonical(id)

	gs := oracle.NewGlobalState()
	for _, req := range []*servicepb.Request{
		oracletest.SetFieldTypeReq(commonpb.TargetType_TARGET_TYPE_ACCOUNT, "k0", commonpb.MetadataType_METADATA_TYPE_STRING),
		oracletest.CreateIndexReq(id),
	} {
		res := gs.Apply(oracle.Bulk{Requests: []*servicepb.Request{req}})
		require.True(t, res.OK, res.Reason)
		gs = res.State
	}

	broken := &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Address{Address: &commonpb.AddressMatch{}}}
	rf := ClassifyRejectedFilter(FilterAnd(FilterMetaExists("k0"), broken), accounts)
	require.Equal(t, PriorIndex{Label: `metadata["k0"] on accounts`, Metadata: true}, rf.Prior[canon])

	building := func(index string) error {
		st, err := status.New(codes.Unavailable, "index is building").WithDetails(&errdetails.ErrorInfo{
			Domain: "ledger", Reason: "INDEX_BUILDING", Metadata: map[string]string{"index": index},
		})
		require.NoError(t, err)

		return st.Err()
	}
	own := building(`metadata["k0"] on accounts`)
	sibling := building(`metadata["k9"] on accounts`)

	require.True(t, RefusedFilterNotReadyLegal(gs.Ledger("L"), rf, accounts, RPCErrorOf(own)), "the not-yet-active index may refuse")
	require.False(t, RefusedFilterNotReadyLegal(gs.Ledger("L"), rf, accounts, RPCErrorOf(sibling)), "a not-active prior does not excuse a refusal naming another index")
	require.False(t, RefusedFilterNotReadyLegal(gs.Ledger("L"), rf, accounts, RPCErrorOf(building("has asset"))), "nor one naming a builtin")

	gs.SetIndexActive("L", canon)
	require.False(t, RefusedFilterNotReadyLegal(gs.Ledger("L"), rf, accounts, RPCErrorOf(own)), "active and no window: the refusal is illegal")

	res := gs.Apply(oracle.Bulk{Requests: []*servicepb.Request{
		oracletest.SetFieldTypeReq(commonpb.TargetType_TARGET_TYPE_ACCOUNT, "k0", commonpb.MetadataType_METADATA_TYPE_INT64),
	}})
	require.True(t, res.OK, res.Reason)
	windowed := res.State.Ledger("L")
	_, open := windowed.RetypeWindow(canon)
	require.True(t, open)

	require.True(t, RefusedFilterNotReadyLegal(windowed, rf, accounts, RPCErrorOf(own)), "the windowed index may refuse")
	require.False(t, RefusedFilterNotReadyLegal(windowed, rf, accounts, RPCErrorOf(sibling)), "a refusal naming another index stays illegal")
}
