package match

import (
	"testing"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/tests/oracle"
	"github.com/stretchr/testify/require"
)

func saveAccountMetaReqL(ledger, addr, key, val string) *servicepb.Request {
	return &servicepb.Request{
		Type: &servicepb.Request_Apply{
			Apply: &servicepb.LedgerApplyRequest{
				Ledger: ledger,
				Action: &servicepb.LedgerAction{
					Data: &servicepb.LedgerAction_AddMetadata{
						AddMetadata: &commonpb.SaveMetadataCommand{
							Target: accountTarget(addr),
							Metadata: map[string]*commonpb.MetadataValue{
								key: {Type: &commonpb.MetadataValue_StringValue{StringValue: val}},
							},
						},
					},
				},
			},
		},
	}
}
func checkpointMetadata(value string) map[string]*commonpb.MetadataValue {
	return map[string]*commonpb.MetadataValue{"phase": {Type: &commonpb.MetadataValue_StringValue{StringValue: value}}}
}

// accountTarget builds a metadata Target pointing at an account address.
func accountTarget(addr string) *commonpb.Target {
	return &commonpb.Target{
		Target: &commonpb.Target_Account{
			Account: &commonpb.TargetAccount{Addr: addr},
		},
	}
}

func createPreparedQueryReq(ledger string, q *commonpb.PreparedQuery) *servicepb.Request {
	return &servicepb.Request{Type: &servicepb.Request_CreatePreparedQuery{
		CreatePreparedQuery: &servicepb.CreatePreparedQueryRequest{Ledger: ledger, Query: q},
	}}
}
func bulkOf(reqs ...*servicepb.Request) oracle.Bulk { return oracle.Bulk{Requests: reqs} }

// newModel is the singleton model's NewChecker initial state without the
// Checker: a GlobalState with the given ledgers created.
func newModel(t *testing.T, ledgers ...string) oracle.GlobalState {
	t.Helper()

	gs := oracle.NewGlobalState()
	for _, ledger := range ledgers {
		created := gs.Apply(oracle.Bulk{Requests: []*servicepb.Request{{
			Type: &servicepb.Request_CreateLedger{CreateLedger: &servicepb.CreateLedgerRequest{Name: ledger}},
		}}})
		require.True(t, created.OK, created.Reason)
		gs = created.State
	}

	return gs
}

// candidatesOf is a CandidateMatcher over a fixed list of bases (the
// Tracker's MatchesModel in production).
func candidatesOf(bases ...oracle.GlobalState) CandidateMatcher {
	return func(m func(oracle.GlobalState) bool) bool {
		for _, b := range bases {
			if m(b) {
				return true
			}
		}

		return false
	}
}
