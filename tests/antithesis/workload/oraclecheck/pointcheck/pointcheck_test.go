package pointcheck_test

import (
	"encoding/json"
	"slices"
	"strconv"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	oc "github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck/match"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck/match/matchtest"
	pc "github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck/pointcheck"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck/testutil"
	"github.com/formancehq/ledger/v3/tests/oracle"
	"github.com/formancehq/ledger/v3/tests/oracle/oracletest"
)

const absentLedger = "o-test-absent-ff"

// fixture is a session with one live ledger l1 carrying coloured and
// uncoloured cells over two assets, a referenced transaction, account and
// transaction metadata and a revert, plus a deleted ledger l2 with history.
func fixture(t *testing.T) *testutil.ScriptedSession {
	s, _ := fixtureWithPreDelete(t)

	return s
}

// fixtureWithPreDelete also returns the drained model just before l2's
// deletion (the model drops a deleted ledger's tables; its global logs stay).
func fixtureWithPreDelete(t *testing.T) (*testutil.ScriptedSession, oracle.GlobalState) {
	t.Helper()
	s := testutil.NewSession("")
	l1, l2 := s.Ledger(1), s.Ledger(2)
	write := func(reqs ...*servicepb.Request) {
		t.Helper()
		require.Empty(t, testutil.Findings(s.Write(false, reqs...)))
	}
	write(testutil.CreateLedgerReq(l1), testutil.CreateLedgerReq(l2))
	write(testutil.TxReq(l1, "", "world", "t-0:1", "USD/2", 10))
	write(oracletest.TxReqColoredL(l1, "world", "t-0:1", "USD/2", "GOLD", 4))
	write(testutil.TxReq(l1, "o-test-ref1", "world", "t-0:2", "COIN", 7))
	write(testutil.TxReq(l1, "", "t-0:1", "t-0:3", "USD/2", 3), testutil.AccountMetaReq(l1, "t-0:1", "k0", "v0"))
	write(testutil.RevertReq(l1, 3))
	write(testutil.TxReq(l2, "", "world", "t-1:1", "COIN", 2))
	pre := s.T.Model()
	write(testutil.DeleteLedgerReq(l2))

	return s, pre
}

func pj(t *testing.T, m proto.Message) json.RawMessage {
	t.Helper()
	b, err := protojson.Marshal(m)
	require.NoError(t, err)

	return b
}

func okOut(t *testing.T, m proto.Message) oc.Outcome {
	return oc.Outcome{OK: true, Response: pj(t, m)}
}

var notFound = oc.Outcome{Code: "NotFound", Message: "not found"}

// validate runs kind's validator on one read ticket taken now.
func validate(t *testing.T, s *testutil.ScriptedSession, kind string, req proto.Message, out oc.Outcome) []oc.Event {
	t.Helper()
	k, ok := oc.LookupReadKind(kind)
	require.True(t, ok)
	tk := s.Read(kind)
	defer s.T.FinishRead(tk)
	c := &oc.ReadCtx{T: s.T, Ticket: tk, MaxTicket: s.T.ReadFrontier(), Snapshot: s.T.Model(), Kind: kind, Request: pj(t, req), Outcome: out}

	return k.Validate(c)
}

func findingIDs(evs []oc.Event) []oc.EventID { return testutil.IDs(testutil.Findings(evs)) }

func requireClean(t *testing.T, evs []oc.Event) {
	t.Helper()
	require.Empty(t, findingIDs(evs), "%+v", evs)
}

func requireFinding(t *testing.T, evs []oc.Event, id oc.EventID) {
	t.Helper()
	require.Equal(t, []oc.EventID{id}, findingIDs(evs), "%+v", evs)
}

func requireReached(t *testing.T, evs []oc.Event, id oc.EventID) {
	t.Helper()
	require.True(t, slices.ContainsFunc(evs, func(e oc.Event) bool { return e.ID == id && e.Kind == oc.EvReachable }), "%s not in %+v", id, evs)
}

func requireNotReached(t *testing.T, evs []oc.Event, id oc.EventID) {
	t.Helper()
	require.False(t, slices.ContainsFunc(evs, func(e oc.Event) bool { return e.ID == id }), "%s in %+v", id, evs)
}

// ---- GetAccount ----

func TestGetAccount(t *testing.T) {
	t.Parallel()
	s := fixture(t)
	l1 := s.Ledger(1)
	req := func(addr string, collapse bool) *servicepb.GetAccountRequest {
		return &servicepb.GetAccountRequest{Ledger: l1, Address: addr, CollapseColors: collapse}
	}
	render := func(addr string, collapse bool) *commonpb.Account {
		return proto.Clone(matchtest.RenderAccount(s.T.Model(), l1, addr, collapse)).(*commonpb.Account)
	}

	t.Run("model-consistent passes", func(t *testing.T) {
		evs := validate(t, s, pc.KindGetAccount, req("t-0:1", false), okOut(t, render("t-0:1", false)))
		requireClean(t, evs)
		requireNotReached(t, evs, pc.EvCollapsedAccountValidated)
		requireNotReached(t, evs, pc.EvAccountModelAbsentProbed)
	})
	t.Run("collapsed passes and is covered", func(t *testing.T) {
		acct := render("t-0:1", true)
		require.Len(t, acct.GetVolumes(), 1, "GOLD folded into USD/2")
		evs := validate(t, s, pc.KindGetAccount, req("t-0:1", true), okOut(t, acct))
		requireClean(t, evs)
		requireReached(t, evs, pc.EvCollapsedAccountValidated)
	})
	t.Run("uncollapsed answer to a collapsed request fails", func(t *testing.T) {
		requireFinding(t, validate(t, s, pc.KindGetAccount, req("t-0:1", true), okOut(t, render("t-0:1", false))), pc.EvAccountReadOutsideModel)
	})

	mutations := map[string]func(a *commonpb.Account){
		"balance": func(a *commonpb.Account) {
			a.Volumes[0].Volumes.Input = commonpb.NewBigUintFromUint256(uint256.NewInt(999))
		},
		"colour bucket": func(a *commonpb.Account) {
			for _, v := range a.Volumes {
				if v.Color == "GOLD" {
					v.Color = "SILVER"
				}
			}
		},
		"ghost bucket": func(a *commonpb.Account) {
			a.Volumes = append(a.Volumes, &commonpb.AccountVolume{Asset: "EUR/2", Volumes: &commonpb.VolumesWithBalance{
				Input: commonpb.NewBigUintFromUint256(uint256.NewInt(0)), Output: commonpb.NewBigUintFromUint256(uint256.NewInt(0)),
			}})
		},
		"omitted bucket": func(a *commonpb.Account) { a.Volumes = a.Volumes[1:] },
		"repeated bucket": func(a *commonpb.Account) {
			a.Volumes = append(a.Volumes, a.Volumes[0])
		},
		"metadata value": func(a *commonpb.Account) { a.Metadata["k0"] = commonpb.NewStringValue("other") },
		"extra metadata": func(a *commonpb.Account) { a.Metadata["k9"] = commonpb.NewStringValue("x") },
	}
	for name, mutate := range mutations {
		t.Run("mutation "+name, func(t *testing.T) {
			acct := render("t-0:1", false)
			mutate(acct)
			requireFinding(t, validate(t, s, pc.KindGetAccount, req("t-0:1", false), okOut(t, acct)), pc.EvAccountReadOutsideModel)
		})
	}

	t.Run("NotFound for a known account fails", func(t *testing.T) {
		requireFinding(t, validate(t, s, pc.KindGetAccount, req("t-0:1", false), notFound), pc.EvAccountReadOutsideModel)
	})
	t.Run("model-absent account: NotFound and empty pass, covered", func(t *testing.T) {
		evs := validate(t, s, pc.KindGetAccount, req("t-7:7", false), notFound)
		requireClean(t, evs)
		requireReached(t, evs, pc.EvAccountModelAbsentProbed)
		requireClean(t, validate(t, s, pc.KindGetAccount, req("t-7:7", false), okOut(t, &commonpb.Account{Address: "t-7:7"})))
	})
	t.Run("absent ledger: NotFound passes and is covered; a served cell fails", func(t *testing.T) {
		r := &servicepb.GetAccountRequest{Ledger: absentLedger, Address: "t-0:1"}
		evs := validate(t, s, pc.KindGetAccount, r, notFound)
		requireClean(t, evs)
		requireReached(t, evs, pc.EvAccountAbsentLedgerProbed)
		evs = validate(t, s, pc.KindGetAccount, r, okOut(t, render("t-0:1", false)))
		requireFinding(t, evs, pc.EvAccountReadOutsideModel)
		requireReached(t, evs, pc.EvAccountAbsentLedgerProbed)
	})
	t.Run("deleted ledger: NotFound passes", func(t *testing.T) {
		evs := validate(t, s, pc.KindGetAccount, &servicepb.GetAccountRequest{Ledger: s.Ledger(2), Address: "t-1:1"}, notFound)
		requireClean(t, evs)
		requireNotReached(t, evs, pc.EvAccountAbsentLedgerProbed)
	})
	t.Run("unexpected error", func(t *testing.T) {
		requireFinding(t, validate(t, s, pc.KindGetAccount, req("t-0:1", false), oc.Outcome{Code: "Internal", Message: "boom"}), pc.EvGetAccountUnexpectedError)
	})
}

func TestGetAccountInflightWriteBothValuesLegal(t *testing.T) {
	t.Parallel()
	s := fixture(t)
	l1 := s.Ledger(1)
	tx := testutil.TxReq(l1, "", "world", "t-0:1", "COIN", 5)
	tk := s.Begin(false, tx)
	before := s.T.Model()
	res := before.Apply(oracle.Bulk{Requests: []*servicepb.Request{tx}})
	require.True(t, res.OK)
	after := res.State

	r := &servicepb.GetAccountRequest{Ledger: l1, Address: "t-0:1"}
	requireClean(t, validate(t, s, pc.KindGetAccount, r, okOut(t, matchtest.RenderAccount(before, l1, "t-0:1", false))))
	requireClean(t, validate(t, s, pc.KindGetAccount, r, okOut(t, matchtest.RenderAccount(after, l1, "t-0:1", false))))
	_, evs := s.End(tk)
	require.Empty(t, testutil.Findings(evs))
	// Once the write drained, the "before" value is no longer legal.
	requireFinding(t, validate(t, s, pc.KindGetAccount, r, okOut(t, matchtest.RenderAccount(before, l1, "t-0:1", false))), pc.EvAccountReadOutsideModel)
}

// ---- GetTransaction ----

func TestGetTransaction(t *testing.T) {
	t.Parallel()
	s := fixture(t)
	l1 := s.Ledger(1)
	req := func(id uint64) *servicepb.GetTransactionRequest {
		return &servicepb.GetTransactionRequest{Ledger: l1, TransactionId: id}
	}
	render := func(id uint64) *servicepb.GetTransactionResponse {
		// Clone: the renderer shares postings and stamps with the model.
		return &servicepb.GetTransactionResponse{Transaction: proto.Clone(matchtest.RenderTx(s.T.Model(), l1, id)).(*commonpb.Transaction)}
	}
	n := uint64(s.T.Model().Ledger(l1).Txs().Len())
	require.EqualValues(t, 5, n, "4 creates + 1 revert")
	require.NotNil(t, s.T.Model().Ledger(l1).Txs().Get(0).Timestamp(), "stamps learned at drain")

	for id := uint64(1); id <= n; id++ {
		requireClean(t, validate(t, s, pc.KindGetTransaction, req(id), okOut(t, render(id))))
	}

	mutations := map[string]struct {
		id     uint64
		mutate func(tx *commonpb.Transaction)
	}{
		"post-commit volume cell": {2, func(tx *commonpb.Transaction) {
			for _, byAsset := range tx.GetPostCommitVolumes().GetVolumesByAccount() {
				byAsset.Volumes[0].Volumes.Input = commonpb.NewBigUintFromUint256(uint256.NewInt(12345))

				break
			}
		}},
		"timestamp (learned)": {2, func(tx *commonpb.Transaction) {
			tx.Timestamp = &commonpb.Timestamp{Data: tx.GetTimestamp().GetData() + 1}
		}},
		"reverted flag": {3, func(tx *commonpb.Transaction) { tx.Reverted = false }},
		"reference":     {3, func(tx *commonpb.Transaction) { tx.Reference = "o-test-ref2" }},
		"metadata value": {3, func(tx *commonpb.Transaction) {
			tx.Metadata = map[string]*commonpb.MetadataValue{"x": commonpb.NewStringValue("y")}
		}},
		"posting amount": {2, func(tx *commonpb.Transaction) { tx.Postings[0].Amount = commonpb.NewUint256FromUint64(5) }},
	}
	for name, m := range mutations {
		t.Run("mutation "+name, func(t *testing.T) {
			resp := render(m.id)
			m.mutate(resp.Transaction)
			requireFinding(t, validate(t, s, pc.KindGetTransaction, req(m.id), okOut(t, resp)), pc.EvTxReadOutsideModel)
		})
	}

	t.Run("NotFound beyond the frontier passes; on a committed id fails", func(t *testing.T) {
		requireClean(t, validate(t, s, pc.KindGetTransaction, req(n+3), notFound))
		requireClean(t, validate(t, s, pc.KindGetTransaction, req(0), notFound))
		requireFinding(t, validate(t, s, pc.KindGetTransaction, req(n), notFound), pc.EvTxReadOutsideModel)
		requireFinding(t, validate(t, s, pc.KindGetTransaction, req(n+1), okOut(t, render(n))), pc.EvTxReadOutsideModel)
	})
	t.Run("absent ledger", func(t *testing.T) {
		r := &servicepb.GetTransactionRequest{Ledger: absentLedger, TransactionId: 1}
		evs := validate(t, s, pc.KindGetTransaction, r, notFound)
		requireClean(t, evs)
		requireReached(t, evs, pc.EvTxAbsentLedgerProbed)
		requireFinding(t, validate(t, s, pc.KindGetTransaction, r, okOut(t, render(1))), pc.EvTxReadOutsideModel)
	})
	t.Run("deleted ledger: NotFound passes", func(t *testing.T) {
		requireClean(t, validate(t, s, pc.KindGetTransaction, &servicepb.GetTransactionRequest{Ledger: s.Ledger(2), TransactionId: 1}, notFound))
	})
	t.Run("unexpected error", func(t *testing.T) {
		requireFinding(t, validate(t, s, pc.KindGetTransaction, req(1), oc.Outcome{Code: "FailedPrecondition", Reason: "X"}), pc.EvGetTransactionUnexpectedErr)
	})
	t.Run("in-flight create: found and NotFound both legal", func(t *testing.T) {
		tx := testutil.TxReq(l1, "", "world", "t-0:5", "COIN", 1)
		tk := s.Begin(false, tx)
		res := s.T.Model().Apply(oracle.Bulk{Requests: []*servicepb.Request{tx}})
		require.True(t, res.OK)
		next := n + 1
		requireClean(t, validate(t, s, pc.KindGetTransaction, req(next), notFound))
		requireClean(t, validate(t, s, pc.KindGetTransaction, req(next), okOut(t, &servicepb.GetTransactionResponse{Transaction: matchtest.RenderTx(res.State, l1, next)})))
		_, evs := s.End(tk)
		require.Empty(t, testutil.Findings(evs))
	})
}

// ---- AggregateVolumes ----

func TestAggregateVolumes(t *testing.T) {
	t.Parallel()
	s := fixture(t)
	l1 := s.Ledger(1)
	req := func(ledger string, filter *commonpb.QueryFilter, opts match.AggOptions) *servicepb.AggregateVolumesRequest {
		return &servicepb.AggregateVolumesRequest{Ledger: ledger, Filter: filter, CollapseColors: opts.CollapseColors,
			UseMaxPrecision: opts.UseMaxPrecision, GroupByPrefixes: opts.GroupByPrefixes}
	}
	render := func(filter *commonpb.QueryFilter, opts match.AggOptions) *commonpb.AggregateResult {
		return matchtest.RenderAggregateFiltered(s.T.Model(), l1, filter, opts)
	}
	free := match.FilterOr(match.FilterAddrPrefix("t-0:"), match.FilterNot(match.FilterAddrExact("t-0:2")))
	grouped := match.AggOptions{GroupByPrefixes: []string{"t-0:", "no-such-prefix:", "world"}}

	for name, tc := range map[string]struct {
		filter *commonpb.QueryFilter
		opts   match.AggOptions
		cover  oc.EventID
	}{
		"flat":           {nil, match.AggOptions{}, pc.EvAggValidated},
		"flat collapsed": {nil, match.AggOptions{CollapseColors: true, UseMaxPrecision: true}, pc.EvAggValidated},
		"free filter":    {free, match.AggOptions{}, pc.EvAggValidated},
		"grouped":        {nil, grouped, pc.EvAggGroupedValidated},
		"grouped+filter": {free, grouped, pc.EvAggGroupedValidated},
	} {
		t.Run(name+" passes", func(t *testing.T) {
			evs := validate(t, s, pc.KindAggregateVolumes, req(l1, tc.filter, tc.opts), okOut(t, render(tc.filter, tc.opts)))
			requireClean(t, evs)
			requireReached(t, evs, tc.cover)
		})
	}

	t.Run("mutated sum fails", func(t *testing.T) {
		res := render(nil, match.AggOptions{})
		res.Volumes[0].Input = commonpb.NewUint256FromUint64(1)
		requireFinding(t, validate(t, s, pc.KindAggregateVolumes, req(l1, nil, match.AggOptions{}), okOut(t, res)), pc.EvAggOutsideModel)
	})
	t.Run("uncollapsed answer to a collapsed request fails", func(t *testing.T) {
		requireFinding(t, validate(t, s, pc.KindAggregateVolumes, req(l1, nil, match.AggOptions{CollapseColors: true}), okOut(t, render(nil, match.AggOptions{}))), pc.EvAggOutsideModel)
	})
	t.Run("mixed arms fail", func(t *testing.T) {
		res := render(nil, grouped)
		res.Volumes = render(nil, match.AggOptions{}).Volumes
		requireFinding(t, validate(t, s, pc.KindAggregateVolumes, req(l1, nil, grouped), okOut(t, res)), pc.EvAggMixed)
	})
	t.Run("repeated bucket fails", func(t *testing.T) {
		res := render(nil, match.AggOptions{})
		res.Volumes = append(res.Volumes, res.Volumes[0])
		requireFinding(t, validate(t, s, pc.KindAggregateVolumes, req(l1, nil, match.AggOptions{}), okOut(t, res)), pc.EvAggRepeatsBucket)
	})
	t.Run("absent ledger", func(t *testing.T) {
		evs := validate(t, s, pc.KindAggregateVolumes, req(absentLedger, nil, match.AggOptions{}), notFound)
		requireClean(t, evs)
		requireReached(t, evs, pc.EvAggAbsentNotFound)
		requireFinding(t, validate(t, s, pc.KindAggregateVolumes, req(absentLedger, nil, match.AggOptions{}), okOut(t, &commonpb.AggregateResult{})), pc.EvAggServedOutsideFleet)
	})
	t.Run("deleted ledger: NotFound explained, a served fold is the liveness finding", func(t *testing.T) {
		l2 := s.Ledger(2)
		requireClean(t, validate(t, s, pc.KindAggregateVolumes, req(l2, nil, match.AggOptions{}), notFound))
		// The model drops a deleted ledger's tables, so an empty fold would
		// match its state: only the liveness guard catches it.
		requireFinding(t, validate(t, s, pc.KindAggregateVolumes, req(l2, nil, match.AggOptions{}), okOut(t, &commonpb.AggregateResult{})), pc.EvAggServedDeletedEverywhere)
	})
	t.Run("live ledger NotFound is unexplained", func(t *testing.T) {
		requireFinding(t, validate(t, s, pc.KindAggregateVolumes, req(l1, nil, match.AggOptions{}), notFound), oc.EvLedgerNotFoundUnexplained)
	})
	t.Run("indexed filter on a missing index: refusal is gated coverage, results fail", func(t *testing.T) {
		f := match.FilterAnd(match.FilterMetaExists("k0"), match.FilterAddrPrefix("t-0:"))
		refusal := oc.Outcome{Code: "FailedPrecondition", Reason: "INDEX_NOT_FOUND", Metadata: map[string]string{"index": "metadata[\"k0\"] on accounts"}}
		evs := validate(t, s, pc.KindAggregateVolumes, req(l1, f, match.AggOptions{}), refusal)
		requireClean(t, evs)
		requireReached(t, evs, pc.EvAggGatedOnMissingIndex)
		requireFinding(t, validate(t, s, pc.KindAggregateVolumes, req(l1, f, match.AggOptions{}), okOut(t, render(f, match.AggOptions{}))), pc.EvAggOutsideModel)
		requireFinding(t, validate(t, s, pc.KindAggregateVolumes, req(l1, f, match.AggOptions{}), oc.Outcome{Code: "Internal"}), pc.EvAggUnexpectedError)
	})
	t.Run("index-free filter refused", func(t *testing.T) {
		refusal := oc.Outcome{Code: "FailedPrecondition", Reason: "INDEX_NOT_FOUND"}
		requireFinding(t, validate(t, s, pc.KindAggregateVolumes, req(l1, free, match.AggOptions{}), refusal), pc.EvAggregateVolumesUnexpected)
	})
	t.Run("in-flight write: before and after folds both legal", func(t *testing.T) {
		tx := testutil.TxReq(l1, "", "world", "t-0:9", "EUR/2", 3)
		tk := s.Begin(false, tx)
		res := s.T.Model().Apply(oracle.Bulk{Requests: []*servicepb.Request{tx}})
		require.True(t, res.OK)
		r := req(l1, nil, match.AggOptions{})
		requireClean(t, validate(t, s, pc.KindAggregateVolumes, r, okOut(t, render(nil, match.AggOptions{}))))
		requireClean(t, validate(t, s, pc.KindAggregateVolumes, r, okOut(t, matchtest.RenderAggregate(res.State, l1, match.AggOptions{}))))
		_, evs := s.End(tk)
		require.Empty(t, testutil.Findings(evs))
	})
}

// ---- GetLedgerStats ----

func TestGetLedgerStats(t *testing.T) {
	t.Parallel()
	s := fixture(t)
	l1 := s.Ledger(1)
	req := &servicepb.GetLedgerStatsRequest{Ledger: l1}
	render := func() *commonpb.LedgerStats { return matchtest.RenderStats(s.T.Model(), l1) }

	evs := validate(t, s, pc.KindGetLedgerStats, req, okOut(t, render()))
	requireClean(t, evs)
	requireReached(t, evs, pc.EvStatsUsageCaughtUp)
	requireReached(t, evs, pc.EvStatsValidated)

	lag := render()
	lag.PostingCount--
	evs = validate(t, s, pc.KindGetLedgerStats, req, okOut(t, lag))
	requireClean(t, evs)
	requireNotReached(t, evs, pc.EvStatsUsageCaughtUp)

	for name, m := range map[string]struct {
		mutate func(st *commonpb.LedgerStats)
		id     oc.EventID
	}{
		"tx count off by one":      {func(st *commonpb.LedgerStats) { st.TransactionCount++ }, pc.EvStatsBoundsOutsideModel},
		"log count off by one":     {func(st *commonpb.LedgerStats) { st.LogCount-- }, pc.EvStatsBoundsOutsideModel},
		"postings above the model": {func(st *commonpb.LedgerStats) { st.PostingCount++ }, pc.EvStatsCounterAboveModel},
		"reverts above the model":  {func(st *commonpb.LedgerStats) { st.RevertCount++ }, pc.EvStatsCounterAboveModel},
		"numscript counted":        {func(st *commonpb.LedgerStats) { st.NumscriptExecutionCount = 1 }, pc.EvStatsCountedNumscript},
	} {
		t.Run(name, func(t *testing.T) {
			st := render()
			m.mutate(st)
			requireFinding(t, validate(t, s, pc.KindGetLedgerStats, req, okOut(t, st)), m.id)
		})
	}

	t.Run("absent ledger", func(t *testing.T) {
		r := &servicepb.GetLedgerStatsRequest{Ledger: absentLedger}
		evs := validate(t, s, pc.KindGetLedgerStats, r, notFound)
		requireClean(t, evs)
		requireReached(t, evs, pc.EvStatsAbsentNotFound)
		requireFinding(t, validate(t, s, pc.KindGetLedgerStats, r, okOut(t, &commonpb.LedgerStats{})), pc.EvStatsServedOutsideFleet)
	})
	t.Run("deleted ledger NotFound explained; live NotFound not", func(t *testing.T) {
		requireClean(t, validate(t, s, pc.KindGetLedgerStats, &servicepb.GetLedgerStatsRequest{Ledger: s.Ledger(2)}, notFound))
		requireFinding(t, validate(t, s, pc.KindGetLedgerStats, req, notFound), oc.EvLedgerNotFoundUnexplained)
	})
	t.Run("unexpected error", func(t *testing.T) {
		requireFinding(t, validate(t, s, pc.KindGetLedgerStats, req, oc.Outcome{Code: "Internal"}), pc.EvGetLedgerStatsUnexpectedErr)
	})
	t.Run("in-flight write: either boundary pair legal", func(t *testing.T) {
		tx := testutil.TxReq(l1, "", "world", "t-0:8", "COIN", 1)
		tk := s.Begin(false, tx)
		res := s.T.Model().Apply(oracle.Bulk{Requests: []*servicepb.Request{tx}})
		require.True(t, res.OK)
		requireClean(t, validate(t, s, pc.KindGetLedgerStats, req, okOut(t, render())))
		requireClean(t, validate(t, s, pc.KindGetLedgerStats, req, okOut(t, matchtest.RenderStats(res.State, l1))))
		_, evs := s.End(tk)
		require.Empty(t, testutil.Findings(evs))
	})
}

// ---- GetLog ----

func TestGetLog(t *testing.T) {
	t.Parallel()
	s, pre := fixtureWithPreDelete(t)
	model := s.T.Model()
	var seqs []uint64
	for seq := range model.Logs() {
		seqs = append(seqs, seq)
	}
	require.NotEmpty(t, seqs)
	maxSeq := seqs[len(seqs)-1]
	tryRender := func(seq uint64) (*commonpb.Log, bool) {
		e, ok := model.Log(seq)
		require.True(t, ok)
		src := model
		if e.Ledger == s.Ledger(2) {
			src = pre
		}
		log, ok := matchtest.RenderLog(src, e.Ledger, e.ID)
		if ok {
			require.Equal(t, seq, log.GetSequence())
		}

		return log, ok
	}
	render := func(seq uint64) *commonpb.Log {
		log, ok := tryRender(seq)
		require.True(t, ok, "seq %d", seq)

		return log
	}
	req := func(seq uint64) *servicepb.GetLogRequest { return &servicepb.GetLogRequest{Sequence: seq} }

	validated := 0
	for _, seq := range seqs {
		log, ok := tryRender(seq)
		if !ok {
			continue // a payload the renderer cannot rebuild
		}
		validated++
		evs := validate(t, s, pc.KindGetLog, req(seq), okOut(t, log))
		requireClean(t, evs)
		requireReached(t, evs, pc.EvGetLogValidated)
	}
	require.Greater(t, validated, 5)

	t.Run("a deleted ledger's log still resolves", func(t *testing.T) {
		for _, seq := range seqs {
			if e, _ := model.Log(seq); e.Ledger == s.Ledger(2) {
				requireClean(t, validate(t, s, pc.KindGetLog, req(seq), okOut(t, render(seq))))
				requireFinding(t, validate(t, s, pc.KindGetLog, req(seq), notFound), pc.EvGetLogLostCommitted)

				return
			}
		}
		t.Fatal("no log of the deleted ledger")
	})
	t.Run("payload of another log fails", func(t *testing.T) {
		other := render(seqs[1])
		other.Sequence = seqs[2]
		requireFinding(t, validate(t, s, pc.KindGetLog, req(seqs[2]), okOut(t, other)), pc.EvGetLogAnotherLog)
	})
	t.Run("NotFound on a learned sequence is a lost log", func(t *testing.T) {
		requireFinding(t, validate(t, s, pc.KindGetLog, req(seqs[0]), notFound), pc.EvGetLogLostCommitted)
	})
	t.Run("unassigned sequence", func(t *testing.T) {
		un := maxSeq + pc.UnassignedSeqSlack
		evs := validate(t, s, pc.KindGetLog, req(un), notFound)
		requireClean(t, evs)
		requireReached(t, evs, pc.EvGetLogUnassignedNotFound)
		requireFinding(t, validate(t, s, pc.KindGetLog, req(un), okOut(t, render(seqs[0]))), pc.EvGetLogServedUnassigned)
		// Below the slack: not judged either way (a foreign writer may own it).
		requireClean(t, validate(t, s, pc.KindGetLog, req(maxSeq+1), okOut(t, render(seqs[0]))))
	})
	t.Run("unexpected error", func(t *testing.T) {
		requireFinding(t, validate(t, s, pc.KindGetLog, req(seqs[0]), oc.Outcome{Code: "Internal"}), pc.EvGetLogUnexpectedError)
	})
	t.Run("the begin logSeqs view supplies the learned maximum", func(t *testing.T) {
		k, ok := oc.LookupReadKind(pc.KindGetLog)
		require.True(t, ok)
		withViews := func(seq uint64, out oc.Outcome, views []json.RawMessage) []oc.Event {
			tk := s.Read(pc.KindGetLog)
			defer s.T.FinishRead(tk)

			return k.Validate(&oc.ReadCtx{T: s.T, Ticket: tk, MaxTicket: s.T.ReadFrontier(), Snapshot: s.T.Model(),
				Views: views, Kind: pc.KindGetLog, Request: pj(t, req(seq)), Outcome: out})
		}
		begun, err := s.T.ComputeViews(s.T.Model(), []json.RawMessage{
			json.RawMessage(`{"name":"ledger","ledgerPick":0}`), json.RawMessage(`{"name":"logSeqs"}`),
		})
		require.NoError(t, err)
		un := maxSeq + pc.UnassignedSeqSlack
		// The view computed on the read's snapshot agrees with the walk.
		requireReached(t, withViews(un, notFound, begun), pc.EvGetLogUnassignedNotFound)
		requireFinding(t, withViews(un, okOut(t, render(seqs[0])), begun), pc.EvGetLogServedUnassigned)
		requireClean(t, withViews(un-1, okOut(t, render(seqs[0])), begun))
		// It is read rather than recomputed: a view naming a higher maximum
		// moves the boundary (only the oracle builds views, so this cannot
		// happen in a run).
		fake := []json.RawMessage{json.RawMessage(`{"seqs":[],"maxLearnedSeq":"` + strconv.FormatUint(maxSeq+1, 10) + `"}`)}
		requireNotReached(t, withViews(un, notFound, fake), pc.EvGetLogUnassignedNotFound)
		requireReached(t, withViews(un+1, notFound, fake), pc.EvGetLogUnassignedNotFound)
	})
}

func TestLogSeqsView(t *testing.T) {
	t.Parallel()
	s := fixture(t)
	views, err := s.T.ComputeViews(s.T.Model(), []json.RawMessage{json.RawMessage(`{"name":"logSeqs"}`)})
	require.NoError(t, err)
	var v struct {
		Seqs          []string `json:"seqs"`
		MaxLearnedSeq string   `json:"maxLearnedSeq"`
	}
	require.NoError(t, json.Unmarshal(views[0], &v))
	require.NotEmpty(t, v.Seqs)
	require.Equal(t, v.Seqs[len(v.Seqs)-1], v.MaxLearnedSeq)

	all := make([]uint64, 1000)
	for i := range all {
		all[i] = uint64(i + 1)
	}
	sample := pc.SampleSeqs(all)
	require.LessOrEqual(t, len(sample), oc.ViewCap)
	require.Equal(t, uint64(1), sample[0])
	require.Equal(t, all[len(all)-pc.LogSeqsNewest:], sample[len(sample)-pc.LogSeqsNewest:])
	require.True(t, slices.IsSorted(sample))
	require.Equal(t, sample, pc.SampleSeqs(all), "deterministic")
	require.Equal(t, []uint64{3, 4}, pc.SampleSeqs([]uint64{3, 4}))
}
