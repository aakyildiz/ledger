package main

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/tests/antithesis/workload/bin/oracle/servicetest"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck/testutil"
)

// The usage provider reports exact model counters for every live tracked
// ledger, and nothing for a deleted one.
func TestUsageProvider_ExactCountsAtQuietBegin(t *testing.T) {
	t.Parallel()
	_, c := setupWithLedger(t)
	l1, l2 := c.Ledger(1), c.Ledger(2)

	for _, m := range []map[string]any{
		c.Write(true, testutil.TxReq(l1, c.Prefix+"ref1", "world", "t-1:1", "COIN", 100)),
		c.Write(false, testutil.TxReq(l1, "", "world", "t-1:2", "COIN", 7), testutil.TxReq(l1, "", "t-1:1", "t-1:2", "COIN", 3)),
		c.Write(false, testutil.RevertReq(l1, 3)),
		c.Write(false, testutil.AccountMetaReq(l1, "t-1:1", "k0", "v")),
		c.Write(false, testutil.CreateLedgerReq(l2)),
		c.Write(false, testutil.TxReq(l2, "", "world", "t-1:1", "COIN", 1)),
		c.Write(false, testutil.DeleteLedgerReq(l2)),
	} {
		okv, viol := servicetest.Verdict(m)
		require.True(t, okv, "%v", viol)
		require.Equal(t, "pending", m["status"], "every write commits")
	}

	begin := c.OK("POST", "/quiet/begin", map[string]any{"epoch": 1})
	raw, err := json.Marshal(begin["providers"].(map[string]any)["usage"])
	require.NoError(t, err)
	var got usageProvided
	require.NoError(t, json.Unmarshal(raw, &got))

	require.Equal(t, []string{l1}, keys(got.Ledgers), "the deleted ledger is not listed")
	u := got.Ledgers[l1]
	require.EqualValues(t, 4, u.TxCount, "3 creates + 1 revert")
	require.EqualValues(t, 1+2+1, u.PostingCount, "postings of creates and of the revert transaction")
	require.EqualValues(t, 1, u.ReferenceCount)
	require.EqualValues(t, 1, u.RevertCount)
	require.EqualValues(t, 5, u.LogCount, "4 transaction logs + 1 metadata log")
	// The provider agrees with the model it reads.
	ls := c.Server.Ledger(l1)
	require.EqualValues(t, ls.Txs().Len(), u.TxCount)
	require.EqualValues(t, len(ls.LogRows()), u.LogCount)
}

func keys[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}

	return out
}
