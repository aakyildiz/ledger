package oraclecheck_test

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	oc "github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck/testutil"
)

func viewSession(t *testing.T) *testutil.ScriptedSession {
	t.Helper()
	s := testutil.NewSession("")
	for _, n := range []string{s.Ledger(1), s.Ledger(2), s.Ledger(3), s.Prefix + "own1"} {
		require.Empty(t, testutil.Findings(s.Write(false, testutil.CreateLedgerReq(n))))
	}
	l1 := s.Ledger(1)
	require.Empty(t, testutil.Findings(s.Write(false,
		testutil.TxReq(l1, s.Prefix+"ref1", "world", "t-0:1", "COIN", 3),
		testutil.TxReq(l1, s.Prefix+"own-r", "world", "t-0:5", "COIN", 3),
		testutil.TxReq(l1, "", "world", "t-0:own-a", "COIN", 3),
		testutil.AccountMetaReq(l1, "t-0:1", "own-k", "v"),
		testutil.AccountMetaReq(l1, "t-0:5", "k", "v"),
	)))

	return s
}

func views(t *testing.T, s *testutil.ScriptedSession, reqs ...string) []map[string]any {
	t.Helper()
	raw := make([]json.RawMessage, len(reqs))
	for i, r := range reqs {
		raw[i] = json.RawMessage(r)
	}
	out, err := s.T.ComputeViews(s.T.Model(), raw)
	require.NoError(t, err)
	res := make([]map[string]any, len(out))
	for i, o := range out {
		require.NoError(t, json.Unmarshal(o, &res[i]))
	}

	return res
}

func TestViews_LedgersHidesReserved(t *testing.T) {
	t.Parallel()
	s := viewSession(t)
	v := views(t, s, `{"name":"ledgers"}`, `{"name":"ledgers","owned":true}`)
	require.Equal(t, []any{s.Ledger(1), s.Ledger(2), s.Ledger(3)}, v[0]["live"])
	require.Equal(t, []any{s.Ledger(1), s.Ledger(2), s.Ledger(3), s.Prefix + "own1"}, v[1]["live"])
	require.EqualValues(t, 4, v[0]["nextLedger"])
	require.Equal(t, false, v[0]["maintenance"])
}

func TestViews_LedgerPickResolvesOnSnapshot(t *testing.T) {
	t.Parallel()
	s := viewSession(t)
	v := views(t, s,
		`{"name":"ledger","ledgerPick":4}`,
		`{"name":"seek","ledgerPick":4,"table":"volumes","probe":"t-0:0"}`,
		`{"name":"stamps","ledgerPick":4}`,
		`{"name":"ledger","ledgerPick":3}`,
		`{"name":"ledger","ledgerPick":3,"owned":true}`,
	)
	for _, x := range v[:3] {
		require.Equal(t, s.Ledger(2), x["ledger"], "live[4 mod 3]")
	}
	require.Equal(t, s.Ledger(1), v[3]["ledger"])
	require.Equal(t, s.Prefix+"own1", v[4]["ledger"])

	// Deleted ledgers join the list only with includeDeleted.
	s.Write(false, testutil.DeleteLedgerReq(s.Ledger(3)))
	v = views(t, s, `{"name":"ledger","ledgerPick":2}`, `{"name":"ledger","ledgerPick":2,"includeDeleted":true}`)
	require.Equal(t, s.Ledger(1), v[0]["ledger"])
	require.Equal(t, s.Ledger(3), v[1]["ledger"])
	require.Equal(t, false, v[1]["live"])
}

func TestViews_EmptyLiveAnswersNull(t *testing.T) {
	t.Parallel()
	s := testutil.NewSession("")
	out, err := s.T.ComputeViews(s.T.Model(), []json.RawMessage{json.RawMessage(`{"name":"seek","ledgerPick":7,"table":"volumes","probe":"x"}`)})
	require.NoError(t, err)
	require.JSONEq(t, `{"ledger":null}`, string(out[0]))
}

func TestViews_SeekWrapsAndHidesReserved(t *testing.T) {
	t.Parallel()
	s := viewSession(t)
	l1 := fmt.Sprintf(`"ledger":%q`, s.Ledger(1))
	v := views(t, s,
		`{"name":"seek",`+l1+`,"table":"volumes","probe":"t-0:2"}`,
		`{"name":"seek",`+l1+`,"table":"volumes","probe":"t-0:6"}`,
		`{"name":"seek",`+l1+`,"table":"volumes","probe":"t-0:6","owned":true}`,
		`{"name":"seek",`+l1+`,"table":"volumes","probe":"zzz"}`,
		`{"name":"seek",`+l1+`,"table":"accountMeta","probe":"t-0:0"}`,
		`{"name":"seek",`+l1+`,"table":"txByRef","probe":""}`,
		`{"name":"seek",`+l1+`,"table":"txs","probe":"2"}`,
		`{"name":"seek",`+l1+`,"table":"txs","probe":"99"}`,
		`{"name":"seek",`+l1+`,"table":"volumes","probe":"t-0:0","filter":"transient"}`,
	)
	entry := func(i int) map[string]any { return v[i]["entry"].(map[string]any) }
	require.Equal(t, "t-0:5", entry(0)["address"])
	require.Equal(t, "world", entry(1)["address"], "t-0:own-a is reserved")
	require.Equal(t, "t-0:own-a", entry(2)["address"])
	require.Equal(t, true, v[3]["wrapped"])
	require.Equal(t, "t-0:1", entry(3)["address"])
	require.Equal(t, "t-0:5", entry(4)["address"], "own- keys are reserved")
	require.Equal(t, s.Prefix+"ref1", entry(5)["reference"])
	require.Equal(t, "1", entry(6)["id"], "txs 2 and 3 touch reserved entities")
	require.Equal(t, true, v[6]["wrapped"])
	require.Equal(t, true, v[7]["wrapped"])
	require.Equal(t, "1", entry(7)["id"])
	require.Equal(t, false, v[8]["found"])
	require.Nil(t, v[8]["entry"])
}

func TestViews_CapsAndDeterminism(t *testing.T) {
	t.Parallel()
	build := func() *testutil.ScriptedSession {
		s := testutil.NewSession("")
		s.Write(false, testutil.CreateLedgerReq(s.Ledger(1)))
		for batch := range 2 {
			var reqs []*servicepb.Request
			for i := range 40 {
				reqs = append(reqs, testutil.AddTypeReq(s.Ledger(1), fmt.Sprintf("ty%d-%02d", batch, i), fmt.Sprintf("ty%d-%02d:{id}", batch, i)))
			}
			require.Empty(t, testutil.Findings(s.Write(false, reqs...)))
		}

		return s
	}
	a, b := build(), build()
	req := []json.RawMessage{json.RawMessage(fmt.Sprintf(`{"name":"ledger","ledger":%q}`, a.Ledger(1))), json.RawMessage(`{"name":"ledgers"}`)}
	va, err := a.T.ComputeViews(a.T.Model(), req)
	require.NoError(t, err)
	vb, err := b.T.ComputeViews(b.T.Model(), req)
	require.NoError(t, err)
	require.Equal(t, va, vb)
	var ledger map[string]any
	require.NoError(t, json.Unmarshal(va[0], &ledger))
	require.Len(t, ledger["types"], oc.ViewCap)
}

func TestViews_ReplayableSample(t *testing.T) {
	t.Parallel()
	var all []oc.ReplayEntry
	for i := range 100 {
		all = append(all, oc.ReplayEntry{Wid: fmt.Sprintf("w%d", i)})
	}
	got := oc.ReplayableView(all)
	require.LessOrEqual(t, len(got), 64)
	require.Equal(t, "w0", got[0].Wid)
	require.Equal(t, all[68:], got[len(got)-32:])
	require.Equal(t, got, oc.ReplayableView(all))
	require.Len(t, oc.ReplayableView(all[:10]), 10)
}

func TestViews_UnknownViewIsAnError(t *testing.T) {
	t.Parallel()
	s := testutil.NewSession("")
	_, err := s.T.ComputeViews(s.T.Model(), []json.RawMessage{json.RawMessage(`{"name":"nope"}`)})
	require.ErrorIs(t, err, oc.ErrViewRequest)
}
