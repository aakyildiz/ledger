package quietcheck

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck/testutil"
)

// The global stream check proper, on served rows the per-ledger check would
// already reject or on side tables the online path could not have built
// consistently: a sequence served twice, a learned log not served at its
// sequence, a bulk whose range has a hole, a per-ledger sequence that goes
// back.
func TestGlobalStream(t *testing.T) {
	t.Parallel()
	s := testutil.NewSession("")
	l1, l2 := s.Ledger(1), s.Ledger(2)
	require.Empty(t, testutil.Findings(s.Write(false, testutil.CreateLedgerReq(l1))))
	require.Empty(t, testutil.Findings(s.Write(false, testutil.CreateLedgerReq(l2))))
	require.Empty(t, testutil.Findings(s.Write(false, testutil.TxReq(l1, "", "world", "t-1:a", "COIN", 5), testutil.TxReq(l1, "", "world", "t-1:a", "COIN", 6))))
	require.Empty(t, testutil.Findings(s.Write(false, testutil.TxReq(l2, "", "world", "t-1:a", "COIN", 5))))
	model := s.T.Model()
	refs := func(l string) []rowRef {
		var out []rowRef
		for i, r := range model.Ledger(l).LogRows() {
			out = append(out, rowRef{ledger: l, id: r.ID, seq: r.Sequence, date: r.Date.GetData(), hasDate: r.Date != nil, position: i})
		}

		return out
	}
	good := map[string][]rowRef{l1: refs(l1), l2: refs(l2)}
	require.Len(t, good[l1], 2)
	require.Empty(t, globalStream(good, model, s.T.CommittedBulks(), s.T.CommittedLogs()))

	for name, mutate := range map[string]func(m map[string][]rowRef){
		"a sequence served twice": func(m map[string][]rowRef) { m[l2][0].seq = m[l1][0].seq },
		"a learned log missing":   func(m map[string][]rowRef) { m[l1] = m[l1][:1] },
		"sequences go back":       func(m map[string][]rowRef) { m[l1][0].seq, m[l1][1].seq = m[l1][1].seq, m[l1][0].seq },
	} {
		m := map[string][]rowRef{l1: append([]rowRef(nil), good[l1]...), l2: append([]rowRef(nil), good[l2]...)}
		mutate(m)
		evs := globalStream(m, model, s.T.CommittedBulks(), s.T.CommittedLogs())
		require.Equal(t, EvQuietGlobalStreamDiffers, evs[0].ID, name)
	}

	// A bulk's range with a hole (its logs were not contiguous).
	bulks := s.T.CommittedBulks()
	for k, b := range bulks {
		if b.MaxSeq > b.MinSeq {
			b.MaxSeq += 5
			bulks[k] = b
		}
	}
	evs := globalStream(good, model, bulks, s.T.CommittedLogs())
	require.Equal(t, EvQuietGlobalStreamDiffers, evs[0].ID)
}
