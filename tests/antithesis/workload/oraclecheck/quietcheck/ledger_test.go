package quietcheck_test

import (
	"encoding/json"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	oc "github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck/match"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck/match/matchtest"
	qc "github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck/quietcheck"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck/testutil"
	"github.com/formancehq/ledger/v3/tests/oracle/oracletest"
)

// served is a model-consistent rendering of every part of one ledger.
type served struct {
	logs  []*commonpb.Log
	txs   []*commonpb.Transaction
	accts []*commonpb.Account
	stats *commonpb.LedgerStats
	info  *commonpb.LedgerInfo
}

func render(t *testing.T, h *harness, l string) *served {
	t.Helper()
	model := h.s.T.Model()
	ls := model.Ledger(l)
	out := &served{stats: matchtest.RenderStats(model, l),
		info: matchtest.RenderLedgerInfoWithIdentity(model, l, h.s.T.MatchLedgerIdentities()[l])}
	for _, row := range ls.LogRows() {
		lg, ok := matchtest.RenderLog(model, l, row.ID)
		require.True(t, ok)
		out.logs = append(out.logs, lg)
	}
	for id := 1; id <= ls.Txs().Len(); id++ {
		out.txs = append(out.txs, matchtest.RenderTx(model, l, uint64(id)))
	}
	for _, a := range match.AccountUniverse(ls) {
		out.accts = append(out.accts, matchtest.RenderAccount(model, l, a, false))
	}

	return out
}

func items[M proto.Message](t *testing.T, ms ...M) []json.RawMessage {
	t.Helper()
	out := []json.RawMessage{}
	for _, m := range ms {
		b, err := protojson.Marshal(m)
		require.NoError(t, err)
		out = append(out, b)
	}

	return out
}

func (s *served) part(t *testing.T, part string) []json.RawMessage {
	t.Helper()
	switch part {
	case qc.PartLogs:
		return items(t, s.logs...)
	case qc.PartTransactions:
		return items(t, s.txs...)
	case qc.PartAccounts:
		return items(t, s.accts...)
	case qc.PartStats:
		return items(t, s.stats)
	default:
		return items(t, s.info)
	}
}

// postLedger posts every part of scope for ledger l, in chunks of 2 rows,
// and returns the events.
func (h *harness) postLedger(t *testing.T, l, scope string, s *served, seq *uint64) []oc.Event {
	t.Helper()
	parts := qc.LinearizableParts
	if scope != qc.ScopeLinearizable {
		parts = qc.NodeParts
	}
	var evs []oc.Event
	for _, part := range parts {
		rows := s.part(t, part)
		for {
			n := min(2, len(rows))
			e, _ := h.section(qc.SectionLedger, *seq, qc.LedgerBody{Ledger: l, Scope: scope, Part: part, Items: rows[:n], Final: n == len(rows)})
			*seq++
			evs = append(evs, e...)
			rows = rows[n:]
			if len(rows) == 0 {
				break
			}
		}
	}

	return evs
}

// ledgerSession: two ledgers with interleaved commits, metadata, a revert,
// an ephemeral type.
func ledgerSession(t *testing.T) *harness {
	t.Helper()
	h := newHarness(t)
	l1, l2 := h.ledger(1), h.ledger(2)
	h.write(false, testutil.CreateLedgerReq(l1))
	h.write(false, testutil.CreateLedgerReq(l2))
	h.write(false, testutil.TxReq(l1, "r1", "world", "t-1:a", "COIN", 50))
	h.write(true, testutil.TxReq(l2, "", "world", "t-2:a", "USD/2", 7))
	h.write(false, testutil.TxReq(l1, "", "t-1:a", "t-1:b", "COIN", 20), testutil.AccountMetaReq(l1, "t-1:b", "k1", "x"))
	h.write(false, testutil.RevertReq(l1, 2))
	h.write(false, onLedger(oracletest.AddTypeReqP("eph", commonpb.AccountTypePersistence_ACCOUNT_TYPE_EPHEMERAL), l2))
	h.write(false, testutil.TxReq(l2, "", "world", "eph:1", "USD/2", 3), testutil.TxReq(l2, "", "eph:1", "t-2:b", "USD/2", 3))

	return h
}

func (h *harness) openQuiet(t *testing.T) {
	t.Helper()
	h.epoch++
	_, ref := h.s.T.BeginQuiet(h.epoch)
	require.Nil(t, ref)
}

func TestExactState_ModelConsistentPartsVerify(t *testing.T) {
	t.Parallel()
	h := ledgerSession(t)
	h.openQuiet(t)
	var seq uint64
	var evs []oc.Event
	for _, l := range []string{h.ledger(1), h.ledger(2)} {
		evs = append(evs, h.postLedger(t, l, qc.ScopeLinearizable, render(t, h, l), &seq)...)
		evs = append(evs, h.postLedger(t, l, "node:ledger-1:8888", render(t, h, l), &seq)...)
	}
	require.Empty(t, ids(evs))
	count := map[oc.EventID]int{}
	for _, e := range evs {
		count[e.ID]++
	}
	require.Equal(t, 2, count[qc.EvQuietExactStateVerified])
	require.Equal(t, 2, count[qc.EvQuietNodeStateVerified])
	require.True(t, reached(evs, qc.EvQuietStatsUsageCaughtUp))
}

func TestExactState_OneFieldMutations(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name         string
		mutate       func(s *served)
		linear, node oc.EventID
	}{
		{"log id", func(s *served) { s.logs[0].GetPayload().GetApply().GetLog().Id += 100 }, qc.EvQuietLogsDiffer, qc.EvNodeLogsDiffer},
		{"log sequence", func(s *served) { s.logs[1].Sequence += 1000 }, qc.EvQuietLogsDiffer, qc.EvNodeLogsDiffer},
		{"a missing log", func(s *served) { s.logs = s.logs[:len(s.logs)-1] }, qc.EvQuietLogsDiffer, qc.EvNodeLogsDiffer},
		{"tx reverted flag", func(s *served) { s.txs[1].Reverted = !s.txs[1].Reverted }, qc.EvQuietTxsDiffer, qc.EvNodeTxsDiffer},
		{"tx reference", func(s *served) { s.txs[0].Reference = "other" }, qc.EvQuietTxsDiffer, qc.EvNodeTxsDiffer},
		{"a missing tx", func(s *served) { s.txs = s.txs[1:] }, qc.EvQuietTxsDiffer, qc.EvNodeTxsDiffer},
		{"a duplicated tx", func(s *served) { s.txs = append(s.txs, s.txs[0]) }, qc.EvQuietTxsDiffer, qc.EvNodeTxsDiffer},
		{"account metadata", func(s *served) {
			for _, a := range s.accts {
				if len(a.Metadata) > 0 {
					a.Metadata["k1"] = commonpb.NewStringValue("y")
				}
			}
		}, qc.EvQuietAcctsDiffer, qc.EvNodeAcctsDiffer},
		{"account volume", func(s *served) {
			s.accts[0].Volumes[0].Volumes.Input = commonpb.NewBigUintFromUint256(uint256.NewInt(123456))
		}, qc.EvQuietAcctsDiffer, qc.EvNodeAcctsDiffer},
		{"a missing account", func(s *served) { s.accts = s.accts[1:] }, qc.EvQuietAcctsDiffer, qc.EvNodeAcctsDiffer},
		{"a ghost account", func(s *served) {
			g := proto.Clone(s.accts[0]).(*commonpb.Account)
			g.Address = "t-9:ghost"
			s.accts = append(s.accts, g)
		}, qc.EvQuietAcctsDiffer, qc.EvNodeAcctsDiffer},
		{"stats boundary", func(s *served) { s.stats.TransactionCount++ }, qc.EvQuietStatsDiffer, ""},
		{"stats usage above the model", func(s *served) { s.stats.PostingCount++ }, qc.EvQuietStatsUsageAboveModel, ""},
		{"ledger info metadata", func(s *served) {
			s.info.Metadata = map[string]*commonpb.MetadataValue{"x": commonpb.NewStringValue("y")}
		}, qc.EvQuietInfoDiffers, qc.EvNodeInfoDiffers},
		{"ledger identity", func(s *served) { s.info.Id++ }, qc.EvQuietInfoDiffers, qc.EvNodeInfoDiffers},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			h := ledgerSession(t)
			l := h.ledger(1)
			h.openQuiet(t)
			var seq uint64
			s := render(t, h, l)
			c.mutate(s)
			evs := h.postLedger(t, l, qc.ScopeLinearizable, s, &seq)
			require.Equal(t, []oc.EventID{c.linear}, ids(evs))
			require.False(t, reached(evs, qc.EvQuietExactStateVerified))
			if c.node == "" {
				return
			}
			evs = h.postLedger(t, l, "node:ledger-0:8888", s, &seq)
			require.Equal(t, []oc.EventID{c.node}, ids(evs))
			require.False(t, reached(evs, qc.EvQuietNodeStateVerified))
		})
	}
}

// Usage counters are asynchronous: below the model is legal (the Sometimes
// is false), above it is a finding.
func TestExactState_UsageBelowTheModelIsLegal(t *testing.T) {
	t.Parallel()
	h := ledgerSession(t)
	l := h.ledger(1)
	h.openQuiet(t)
	s := render(t, h, l)
	s.stats.PostingCount--
	var seq uint64
	evs := h.postLedger(t, l, qc.ScopeLinearizable, s, &seq)
	require.Empty(t, ids(evs))
	require.True(t, reached(evs, qc.EvQuietExactStateVerified))
	for _, e := range evs {
		if e.ID == qc.EvQuietStatsUsageCaughtUp {
			require.False(t, e.Cond)
		}
	}
}

// A zero-balance cell of an address that is ephemeral by the current chart
// may already be purged by the server: its absence (and the account's, when
// that was its only state) is tolerated; any other absence is not.
func TestExactState_PurgedZeroCellsTolerated(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	l := h.ledger(1)
	h.write(false, testutil.CreateLedgerReq(l))
	h.write(false, testutil.TxReq(l, "", "world", "z:1", "COIN", 5))
	h.write(false, testutil.TxReq(l, "", "z:1", "t-1:a", "COIN", 5))
	// z:1 now holds a zero-balance cell; typing z as ephemeral afterwards
	// leaves the model's cell in place, while a server may purge it.
	typed := h.write(false, onLedger(oracletest.AddTypeReqP("z", commonpb.AccountTypePersistence_ACCOUNT_TYPE_EPHEMERAL), l))
	if typed.Status != oc.WCommitted {
		t.Skipf("the model refuses the late ephemeral type: %v", typed.Outcome)
	}
	require.Contains(t, match.AccountUniverse(h.s.T.Model().Ledger(l)), "z:1")
	h.openQuiet(t)
	s := render(t, h, l)
	var kept []*commonpb.Account
	for _, a := range s.accts {
		if a.GetAddress() != "z:1" {
			kept = append(kept, a)
		}
	}
	s.accts = kept
	var seq uint64
	evs := h.postLedger(t, l, qc.ScopeLinearizable, s, &seq)
	require.Empty(t, ids(evs))

	// world is not ephemeral: its absence is a finding.
	s = render(t, h, l)
	require.Equal(t, "t-1:a", s.accts[0].GetAddress())
	s.accts = s.accts[1:]
	evs = h.postLedger(t, l, qc.ScopeLinearizable, s, &seq)
	require.Contains(t, ids(evs), qc.EvQuietAcctsDiffer)
}

// Learned log dates that go back in time along the global sequence (the
// server stamped a later proposal earlier) are a finding of the global
// stream check, which runs once every live ledger's logs matched.
func TestExactState_LogDatesDecreaseAlongTheSequence(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	l1, l2 := h.ledger(1), h.ledger(2)
	h.write(false, testutil.CreateLedgerReq(l1))
	h.write(false, testutil.CreateLedgerReq(l2))
	h.write(false, testutil.TxReq(l1, "", "world", "t-1:a", "COIN", 5))
	h.s.Stamps -= 1_000_000
	h.write(false, testutil.TxReq(l2, "", "world", "t-1:a", "COIN", 5))
	h.openQuiet(t)
	var seq uint64
	evs := h.postLedger(t, l1, qc.ScopeLinearizable, render(t, h, l1), &seq)
	require.Empty(t, ids(evs), "the global check waits for every live ledger")
	evs = h.postLedger(t, l2, qc.ScopeLinearizable, render(t, h, l2), &seq)
	require.Equal(t, []oc.EventID{qc.EvQuietLogDatesDecrease}, ids(evs))
}

func TestExactState_Refusals(t *testing.T) {
	t.Parallel()
	h := ledgerSession(t)
	h.openQuiet(t)
	fn, _ := oc.LookupQuietSection(qc.SectionLedger)
	for _, body := range []qc.LedgerBody{
		{Ledger: h.ledger(1), Scope: "eventual", Part: qc.PartLogs, Final: true},
		{Ledger: h.ledger(1), Scope: "node:", Part: qc.PartLogs, Final: true},
		{Ledger: h.ledger(1), Scope: "node:ledger-0:8888", Part: qc.PartStats, Final: true},
		{Ledger: h.ledger(1), Scope: qc.ScopeLinearizable, Part: "aggregates", Final: true},
		{Ledger: h.ledger(9), Scope: qc.ScopeLinearizable, Part: qc.PartLogs, Final: true},
		{Ledger: "r-ns-x", Scope: qc.ScopeLinearizable, Part: qc.PartLogs, Final: true},
	} {
		raw, err := json.Marshal(body)
		require.NoError(t, err)
		_, _, ref := fn(h.s.T, h.epoch, 0, raw)
		require.NotNil(t, ref, "%+v", body)
		require.Equal(t, oc.CodeProtocol, ref.Code)
	}
}
