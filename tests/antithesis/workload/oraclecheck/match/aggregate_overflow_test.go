package match

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/tests/oracle/oracletest"
)

func TestAggregateOverflows_MaxPrecisionRescale(t *testing.T) {
	t.Parallel()

	// USD/2 holds 10^18 and USD/255 one unit: merging under USD/255 needs
	// 10^253 * 10^18, past 256 bits (the server's max-precision-rescale).
	ls := buildLedger(t,
		oracletest.TxReq("world", "t-1:1", "USD/2", 1_000_000_000_000_000_000),
		oracletest.TxReq("world", "t-1:2", "USD/255", 1),
	)
	require.False(t, AggregateOverflows(ls, nil, AggOptions{}), "no rescale without useMaxPrecision")
	require.False(t, AggregateOverflows(ls, nil, AggOptions{CollapseColors: true}))
	require.True(t, AggregateOverflows(ls, nil, AggOptions{UseMaxPrecision: true}))
	// Grouped: only the group that holds both cells overflows.
	require.True(t, AggregateOverflows(ls, nil, AggOptions{UseMaxPrecision: true, GroupByPrefixes: []string{"t-1:", "world"}}))
	require.False(t, AggregateOverflows(ls, nil, AggOptions{UseMaxPrecision: true, GroupByPrefixes: []string{"t-1:1"}}))

	// Close precisions rescale without overflow.
	small := buildLedger(t,
		oracletest.TxReq("world", "t-1:1", "USD/2", 5),
		oracletest.TxReq("world", "t-1:2", "USD/4", 1),
	)
	require.False(t, AggregateOverflows(small, nil, AggOptions{UseMaxPrecision: true, CollapseColors: true}))
}
