package match

import (
	"maps"
	"testing"

	"github.com/formancehq/ledger/v3/tests/oracle"
	"github.com/formancehq/ledger/v3/tests/oracle/oracletest"
	"github.com/stretchr/testify/require"
)

// Adapted from M/validate_test.go: the Checker only supplied the initial state.
func TestAccountVolumesMatch(t *testing.T) {
	t.Parallel()

	gs := newModel(t, "L").Apply(bulkOf(
		oracletest.TxReq("world", "t-0:5", "USD/2", 10),
		oracletest.TxReq("world", "t-0:5", "COIN", 3),
		oracletest.TxReqColoredL("L", "world", "t-0:5", "USD/2", "GRANTS", 4),
	)).State
	ls := gs.Ledger("L")

	exact := map[AssetColor]oracle.VolumePair{}
	for k, vp := range ls.Volumes().All() {
		if k.Address == "t-0:5" {
			exact[AssetColor{Asset: k.Asset, Color: k.Color}] = vp
		}
	}
	require.Len(t, exact, 3)
	require.True(t, AccountVolumesMatch(ls, "t-0:5", exact, false))

	ghost := maps.Clone(exact)
	var g oracle.VolumePair
	g.Input.SetUint64(7)
	g.Output.SetUint64(7)
	ghost[AssetColor{Asset: "EUR/2"}] = g
	require.False(t, AccountVolumesMatch(ls, "t-0:5", ghost, false), "ghost cell under an unprobed asset must mismatch")

	omitted := maps.Clone(exact)
	delete(omitted, AssetColor{Asset: "USD/2"})
	require.False(t, AccountVolumesMatch(ls, "t-0:5", omitted, false), "omitted cell must mismatch")

	divergent := maps.Clone(exact)
	d := divergent[AssetColor{Asset: "USD/2"}]
	d.Input.AddUint64(&d.Input, 1)
	divergent[AssetColor{Asset: "USD/2"}] = d
	require.False(t, AccountVolumesMatch(ls, "t-0:5", divergent, false))

	recoloured := maps.Clone(exact)
	grants := AssetColor{Asset: "USD/2", Color: "GRANTS"}
	recoloured[AssetColor{Asset: "USD/2", Color: "GOLD"}] = recoloured[grants]
	delete(recoloured, grants)
	require.False(t, AccountVolumesMatch(ls, "t-0:5", recoloured, false), "the right amounts in the wrong bucket must mismatch")

	// An account the base doesn't hold: only the empty reading matches.
	require.True(t, AccountVolumesMatch(ls, "t-9:9", nil, false))
	require.False(t, AccountVolumesMatch(ls, "t-9:9", map[AssetColor]oracle.VolumePair{{Asset: "USD/2"}: {}}, false))
}
