package match

import (
	"testing"

	"github.com/formancehq/ledger/v3/tests/oracle/oracletest"
	"github.com/stretchr/testify/require"
)

// Adapted from M/reads_test.go: the Checker only supplied the initial state.
func TestModelKnowsAccount(t *testing.T) {
	t.Parallel()

	gs := newModel(t, "L").Apply(bulkOf(oracletest.TxReq("world", "t-0:5", "USD/2", 10))).State
	ls := gs.Ledger("L")

	require.True(t, ModelKnowsAccount(ls, "t-0:5"))
	require.True(t, ModelKnowsAccount(ls, "world"))
	require.False(t, ModelKnowsAccount(ls, "t-0:6"))
	require.False(t, ModelKnowsAccount(ls, "t-99:1"))
}
