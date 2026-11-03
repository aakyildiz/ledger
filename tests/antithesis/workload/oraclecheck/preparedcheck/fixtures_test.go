package preparedcheck_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck/match"
)

const fixtureDir = "../../../hegel/protocol-fixtures/08"

func readFixture(t *testing.T, name string) []byte {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(fixtureDir, name))
	require.NoError(t, err)

	return b
}

// TestRustProtocolFixtures decodes the driver's prepared-query request shapes
// (written by `cargo test prepared::tests::protocol_fixtures`) with strict
// protojson, as the Go client does, and checks the parameter values and
// filters mean what the driver intends.
func TestRustProtocolFixtures(t *testing.T) {
	t.Parallel()
	strict := protojson.UnmarshalOptions{}

	var list servicepb.ExecutePreparedQueryRequest
	require.NoError(t, strict.Unmarshal(readFixture(t, "execute-list-params.json"), &list))
	require.Equal(t, commonpb.QueryMode_QUERY_MODE_LIST, list.GetMode())
	require.True(t, list.GetReverse())
	require.EqualValues(t, 5, list.GetPageSize())
	p := list.GetParameters()
	require.Equal(t, "users:", p["p0"].GetStringValue())
	require.EqualValues(t, 42, p["p1"].GetUint64Value())
	require.EqualValues(t, -3, p["p2"].GetInt64Value())
	require.True(t, p["p3"].GetBoolValue())

	var follow servicepb.ExecutePreparedQueryRequest
	require.NoError(t, strict.Unmarshal(readFixture(t, "execute-follow-up.json"), &follow))
	require.Equal(t, "opaque", follow.GetCursor())
	require.False(t, follow.GetReverse(), "flipped back")

	var agg servicepb.ExecutePreparedQueryRequest
	require.NoError(t, strict.Unmarshal(readFixture(t, "execute-aggregate.json"), &agg))
	require.Equal(t, commonpb.QueryMode_QUERY_MODE_AGGREGATE_VOLUMES, agg.GetMode())
	require.False(t, agg.GetReverse())
	require.NoError(t, strict.Unmarshal(readFixture(t, "execute-aggregate-reverse.json"), &agg))
	require.True(t, agg.GetReverse())

	var mis servicepb.ExecutePreparedQueryRequest
	require.NoError(t, strict.Unmarshal(readFixture(t, "execute-mistyped.json"), &mis))
	f := &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_And{And: &commonpb.AndFilter{Filters: []*commonpb.QueryFilter{
		paramPrefix("p0"),
		{Filter: &commonpb.QueryFilter_BuiltinUint{BuiltinUint: &commonpb.BuiltinUintCondition{
			Field: commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_ID, Cond: &commonpb.UintCondition{ParamMin: "p1"},
		}}},
	}}}}
	_, resolved := match.SubstituteParams(f, match.PreparedParams(mis.GetParameters()))
	require.False(t, resolved, "mistyped values never resolve")

	var bulk struct {
		Requests []json.RawMessage `json:"requests"`
	}
	require.NoError(t, json.Unmarshal(readFixture(t, "create-param-filters.json"), &bulk))
	require.NotEmpty(t, bulk.Requests)
	kinds := map[string]match.ParamKind{}
	for _, raw := range bulk.Requests {
		var r servicepb.Request
		require.NoError(t, strict.Unmarshal(raw, &r), "%s", raw)
		var filter *commonpb.QueryFilter
		switch x := r.GetType().(type) {
		case *servicepb.Request_CreatePreparedQuery:
			q := x.CreatePreparedQuery.GetQuery()
			require.NoError(t, domain.ValidatePreparedQueryName(q.GetName()))
			if err := domain.ValidateFilterForTarget(q.GetFilter(), q.GetTarget()); err != nil {
				t.Fatalf("%s: %v", raw, err)
			}
			filter = q.GetFilter()
		case *servicepb.Request_UpdatePreparedQuery:
			filter = x.UpdatePreparedQuery.GetFilter()
		}
		if err := domain.ValidateFilterAdmissionShape(filter); err != nil {
			t.Fatalf("%s: %v", raw, err)
		}
		for _, n := range match.CollectParams(filter) {
			kinds[n.Name] = n.Kind
		}
	}
	require.Equal(t, match.ParamKindAddress, kinds["p0"])
	require.Equal(t, match.ParamKindString, kinds["p1"])
}

// TestRustMalformedFilterFixtures decodes every malformed filter shape the
// driver's malformed_filter sends (strict protojson, as the Go client does)
// and requires the server's validators to refuse each: a create on its
// target, an update at admission or against both stored targets. The stage
// and reason against a real server are in testdata/rejections/08-*.json.
func TestRustMalformedFilterFixtures(t *testing.T) {
	t.Parallel()
	strict := protojson.UnmarshalOptions{}
	var bulk struct {
		Requests []json.RawMessage `json:"requests"`
	}
	require.NoError(t, json.Unmarshal(readFixture(t, "malformed-filters.json"), &bulk))
	require.Len(t, bulk.Requests, 6)
	for _, raw := range bulk.Requests {
		var r servicepb.Request
		require.NoError(t, strict.Unmarshal(raw, &r), "%s", raw)
		switch x := r.GetType().(type) {
		case *servicepb.Request_CreatePreparedQuery:
			q := x.CreatePreparedQuery.GetQuery()
			require.NotNil(t, q.GetFilter().GetFilter(), "%s decoded to an empty filter", raw)
			require.Error(t, domain.ValidateFilterForTarget(q.GetFilter(), q.GetTarget()), "%s", raw)
		case *servicepb.Request_UpdatePreparedQuery:
			f := x.UpdatePreparedQuery.GetFilter()
			require.NotNil(t, f.GetFilter(), "%s decoded to an empty filter", raw)
			for _, target := range []commonpb.QueryTarget{commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS, commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS} {
				require.Error(t, domain.ValidateFilterForTarget(f, target), "%s on %s", raw, target)
			}
		default:
			t.Fatalf("unexpected request %s", raw)
		}
	}
}
