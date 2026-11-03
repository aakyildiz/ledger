package oraclecheck

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck/match"
)

// The conversions must stay field-complete: a field added on either side
// without the other breaks this test.
func TestMatchConversionsFieldComplete(t *testing.T) {
	t.Parallel()
	pairs := []struct{ a, b any }{
		{CommittedLog{}, match.CommittedLog{}},
		{CommittedBulk{}, match.CommittedBulk{}},
		{LedgerIdentity{}, match.LedgerIdentity{}},
	}
	for _, p := range pairs {
		ta, tb := reflect.TypeOf(p.a), reflect.TypeOf(p.b)
		require.Equal(t, ta.NumField(), tb.NumField(), "%s vs %s", ta, tb)
		for i := 0; i < ta.NumField(); i++ {
			f, ok := tb.FieldByName(ta.Field(i).Name)
			require.True(t, ok, "%s.%s missing in %s", ta, ta.Field(i).Name, tb)
			require.Equal(t, ta.Field(i).Type, f.Type)
		}
	}
}

func TestMatchConversions(t *testing.T) {
	t.Parallel()
	ts := &commonpb.Timestamp{Data: 5}
	require.Equal(t, map[uint64]match.CommittedLog{3: {Ledger: "l", ID: 7, Kind: "created_transaction"}},
		MatchLogs(map[uint64]CommittedLog{3: {Ledger: "l", ID: 7, Kind: "created_transaction"}}))
	require.Equal(t, map[uint64]match.CommittedBulk{3: {MinSeq: 3, MaxSeq: 5, Orders: 3, Wid: "w1"}},
		MatchBulks(map[uint64]CommittedBulk{3: {Wid: "w1", MinSeq: 3, MaxSeq: 5, Orders: 3}}))
	got := MatchIdentities(map[string]LedgerIdentity{"l": {ID: 2, CreatedAt: ts}})
	require.Equal(t, uint32(2), got["l"].ID)
	require.Same(t, ts, got["l"].CreatedAt)
}
