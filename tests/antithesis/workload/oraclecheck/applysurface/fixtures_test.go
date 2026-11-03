package applysurfacecheck_test

import (
	"encoding/json"
	"math/big"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck/testutil"
	"github.com/formancehq/ledger/v3/tests/oracle"
)

// Go-produced protocol fixtures for task 02 (HG/protocol-fixtures/02/): the
// request shapes the Rust order generators emit, rendered by protojson from
// the generated types, and an Apply response carrying an OrderSkipped log.
// The Rust driver checks its generated requests against these schemas and
// parses the skip facts out of the response fixture.
//
// Regenerate with UPDATE_FIXTURES=1 go test ./oraclecheck/applysurface -run TestProtocolFixtures02.

const fixtureDir = "../../../hegel/protocol-fixtures/02"

func colored(src, dst, asset, color string, amount *big.Int) *commonpb.Posting {
	return commonpb.NewColoredPosting(src, dst, asset, color, amount)
}

func fixtureMessages(t *testing.T) map[string]proto.Message {
	t.Helper()
	half := new(big.Int).Lsh(big.NewInt(1), 255)
	maxU := new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 256), big.NewInt(1))
	full := &servicepb.Request{Type: &servicepb.Request_Apply{Apply: &servicepb.LedgerApplyRequest{
		Ledger:           "o-1a2b-l2",
		SkippableReasons: []commonpb.ErrorReason{refConflict},
		Action: &servicepb.LedgerAction{Data: &servicepb.LedgerAction_CreateTransaction{CreateTransaction: &servicepb.CreateTransactionPayload{
			Postings: []*commonpb.Posting{
				colored("world", "t-3:42", "USD/2", "AB", big.NewInt(0)),
				colored("t-3:42", "t-3:42", "COIN", "", maxU),
				colored("world", "t-4:1", "EUR/2", "Z", half),
			},
			Reference: "o-1a2b-ref7",
			Force:     true,
			Timestamp: &commonpb.Timestamp{Data: 1_499_999_999_999_999},
			Metadata: map[string]*commonpb.MetadataValue{
				"k0": commonpb.NewIntValue(-200),
				"k1": commonpb.NewUintValue(5000000000),
				"k2": commonpb.NewBoolValue(true),
				"k3": commonpb.NewStringValue("v0"),
				"k4": commonpb.NewDatetimeValue(1700000000000000),
				"k5": commonpb.NewNullValue("030"),
				"k6": commonpb.NewNullValue(""),
			},
			AccountMetadata: map[string]*commonpb.MetadataMap{
				"t-4:1": {Values: map[string]*commonpb.MetadataValue{"k1": commonpb.NewStringValue("7")}},
			},
		}}},
	}}}

	// An Apply response: tx 1 under reference "o-1a2b-ref1", then a skipped
	// duplicate followed by a committed account-metadata write.
	g := oracle.NewGlobalState().WithQueryCheckpointLimit(10)
	for _, r := range []*servicepb.Request{testutil.CreateLedgerReq("o-1a2b-l2"), testutil.TxReq("o-1a2b-l2", "o-1a2b-ref1", "world", "t-0:1", "COIN", 5)} {
		res := g.Apply(oracle.Bulk{Requests: []*servicepb.Request{r}})
		require.True(t, res.OK, res.Reason)
		g = res.State
	}
	bulk := oracle.Bulk{Requests: []*servicepb.Request{
		withSkip(testutil.TxReq("o-1a2b-l2", "o-1a2b-ref1", "world", "fees", "COIN", 6)),
		testutil.AccountMetaReq("o-1a2b-l2", "t-1:3", "k0", "v"),
	}}
	skipped := testutil.SimulateCommit(g, bulk, 40, 1_793_836_800_000_000)
	created := testutil.SimulateCommit(g, oracle.Bulk{Requests: []*servicepb.Request{testutil.TxReq("o-1a2b-l2", "o-1a2b-ref2", "world", "t-0:1", "COIN", 9)}}, 40, 1_793_836_800_000_000)

	return map[string]proto.Message{
		"request.create-full.json":          full,
		"request.create-empty.json":         testutil.EmptyTxReq("o-1a2b-l1"),
		"request.add-account-metadata.json": testutil.AccountMetaReq("o-1a2b-l1", "t-0:9", "k2", "x"),
		"response.order-skipped.json":       skipped,
		"response.created-transaction.json": created,
	}
}

func canonical(t *testing.T, m proto.Message) []byte {
	t.Helper()
	b, err := protojson.MarshalOptions{UseProtoNames: false}.Marshal(m)
	require.NoError(t, err)
	var v any
	require.NoError(t, json.Unmarshal(b, &v))
	out, err := json.MarshalIndent(v, "", "  ")
	require.NoError(t, err)

	return append(out, '\n')
}

func TestProtocolFixtures02(t *testing.T) {
	t.Parallel()
	update := os.Getenv("UPDATE_FIXTURES") == "1"
	if update {
		require.NoError(t, os.MkdirAll(fixtureDir, 0o755))
	}
	for name, m := range fixtureMessages(t) {
		want := canonical(t, m)
		path := filepath.Join(fixtureDir, name)
		if update {
			require.NoError(t, os.WriteFile(path, want, 0o644))

			continue
		}
		got, err := os.ReadFile(path)
		require.NoError(t, err, "missing fixture %s (UPDATE_FIXTURES=1 regenerates)", name)
		require.JSONEq(t, string(want), string(got), name)

		// Every fixture decodes back into its Go type.
		fresh := m.ProtoReflect().New().Interface()
		require.NoError(t, protojson.Unmarshal(got, fresh), name)
		require.True(t, proto.Equal(m, fresh), name)
	}
}
