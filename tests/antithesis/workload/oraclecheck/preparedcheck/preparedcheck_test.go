package preparedcheck_test

import (
	"encoding/json"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	oc "github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck/match"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck/match/matchtest"
	pc "github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck/preparedcheck"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck/testutil"
	"github.com/formancehq/ledger/v3/tests/oracle"
)

// ---- builders ----

func createPQ(ledger, name string, target commonpb.QueryTarget, f *commonpb.QueryFilter) *servicepb.Request {
	return &servicepb.Request{Type: &servicepb.Request_CreatePreparedQuery{CreatePreparedQuery: &servicepb.CreatePreparedQueryRequest{
		Ledger: ledger, Query: &commonpb.PreparedQuery{Name: name, Target: target, Filter: f},
	}}}
}

func updatePQ(ledger, name string, f *commonpb.QueryFilter) *servicepb.Request {
	return &servicepb.Request{Type: &servicepb.Request_UpdatePreparedQuery{UpdatePreparedQuery: &servicepb.UpdatePreparedQueryRequest{Ledger: ledger, Name: name, Filter: f}}}
}

func deletePQ(ledger, name string) *servicepb.Request {
	return &servicepb.Request{Type: &servicepb.Request_DeletePreparedQuery{DeletePreparedQuery: &servicepb.DeletePreparedQueryRequest{Ledger: ledger, Name: name}}}
}

func prefix(p string) *commonpb.QueryFilter {
	return &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Address{Address: &commonpb.AddressMatch{Match: &commonpb.AddressMatch_HardcodedPrefix{HardcodedPrefix: p}}}}
}

func paramPrefix(name string) *commonpb.QueryFilter {
	return &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Address{Address: &commonpb.AddressMatch{Match: &commonpb.AddressMatch_ParamPrefix{ParamPrefix: name}}}}
}

func hasAsset(base string, precision uint32) *commonpb.QueryFilter {
	return &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_AccountHasAsset{AccountHasAsset: &commonpb.AccountHasAssetCondition{AssetBase: base, Precision: precision}}}
}

func strParam(v string) *commonpb.ParameterValue {
	return &commonpb.ParameterValue{Value: &commonpb.ParameterValue_StringValue{StringValue: v}}
}

func uintParam(v uint64) *commonpb.ParameterValue {
	return &commonpb.ParameterValue{Value: &commonpb.ParameterValue_Uint64Value{Uint64Value: v}}
}

const (
	accts = commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS
	txs   = commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS
	list  = commonpb.QueryMode_QUERY_MODE_LIST
	aggr  = commonpb.QueryMode_QUERY_MODE_AGGREGATE_VOLUMES
)

// fixture: l1 with five users and two t- accounts, a param-prefix ACCOUNTS
// query q-1, a TRANSACTIONS query q-2 and a match-all ACCOUNTS query q-3.
func fixture(t *testing.T) *testutil.ScriptedSession {
	t.Helper()
	s := testutil.NewSession("")
	l1 := s.Ledger(1)
	write := func(reqs ...*servicepb.Request) {
		t.Helper()
		require.Empty(t, testutil.Findings(s.Write(false, reqs...)))
	}
	write(testutil.CreateLedgerReq(l1))
	for _, a := range []string{"users:1", "users:2", "users:3", "users:4", "users:5"} {
		write(testutil.TxReq(l1, "", "world", a, "USD/2", 10))
	}
	write(testutil.TxReq(l1, "", "world", "t-0:1", "COIN", 3), testutil.TxReq(l1, "", "world", "t-0:2", "EUR/2", 4))
	write(createPQ(l1, "q-1", accts, paramPrefix("p0")))
	write(createPQ(l1, "q-2", txs, prefix("users:")))
	write(createPQ(l1, "q-3", accts, nil))

	return s
}

func pj(t *testing.T, m proto.Message) json.RawMessage {
	t.Helper()
	b, err := protojson.Marshal(m)
	require.NoError(t, err)

	return b
}

func okOut(t *testing.T, m proto.Message) oc.Outcome { return oc.Outcome{OK: true, Response: pj(t, m)} }

func errOut(code, reason string) oc.Outcome {
	return oc.Outcome{Code: code, Reason: reason, Message: "x"}
}

type readResult struct {
	ticket uint64
	evs    []oc.Event
}

// validateAt runs kind's validator on a read ticket taken by `tk` (or a fresh
// one when tk == 0), with params.
func validateAt(t *testing.T, s *testutil.ScriptedSession, kind string, req proto.Message, out oc.Outcome, params any) readResult {
	t.Helper()
	k, ok := oc.LookupReadKind(kind)
	require.True(t, ok)
	tk := s.Read(kind)
	defer s.T.FinishRead(tk)
	var raw json.RawMessage
	if params != nil {
		raw, _ = json.Marshal(params)
	}
	c := &oc.ReadCtx{T: s.T, Ticket: tk, MaxTicket: s.T.ReadFrontier(), Snapshot: s.T.Model(), Kind: kind, Request: pj(t, req), Outcome: out, Params: raw}

	return readResult{ticket: tk, evs: k.Validate(c)}
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

func reached(evs []oc.Event, id oc.EventID) bool {
	return slices.ContainsFunc(evs, func(e oc.Event) bool { return e.ID == id && e.Kind == oc.EvReachable })
}

// accountPage renders the ACCOUNTS page a correct server returns for the
// bound filter on gs.
func accountPage(gs oracle.GlobalState, ledger string, bound *commonpb.QueryFilter, after string, size int, reverse bool) *servicepb.ExecutePreparedQueryResponse {
	probe := match.PreparedAccountProbe(gs.Ledger(ledger), bound, after, size+1, reverse)
	rows := probe
	if len(rows) > size {
		rows = rows[:size]
	}
	cur := &commonpb.PreparedQueryCursor{PageSize: uint32(size)}
	for _, a := range rows {
		cur.AccountData = append(cur.AccountData, matchtest.RenderAccount(gs, ledger, a, false))
	}
	if len(probe) > size {
		cur.HasMore, cur.Next = true, "cursor-"+rows[len(rows)-1]
	}

	return &servicepb.ExecutePreparedQueryResponse{Result: &servicepb.ExecutePreparedQueryResponse_Cursor{Cursor: cur}}
}

func execReq(ledger, name string, params map[string]*commonpb.ParameterValue, size uint32, mode commonpb.QueryMode, reverse bool) *servicepb.ExecutePreparedQueryRequest {
	return &servicepb.ExecutePreparedQueryRequest{Ledger: ledger, QueryName: name, Parameters: params, PageSize: size, Mode: mode, Reverse: reverse}
}

// ---- begin hook ----

func TestBeginSerialize(t *testing.T) {
	t.Parallel()
	s := testutil.NewSession("")
	l1 := s.Ledger(1)
	require.Empty(t, testutil.Findings(s.Write(false, testutil.CreateLedgerReq(l1))))

	begin := func(reqs ...*servicepb.Request) (uint64, *oc.Refusal) {
		tk, _, _, ref := s.T.BeginWrite(s.Record(false, reqs...))

		return tk, ref
	}

	first, ref := begin(createPQ(l1, "q-1", accts, prefix("users:")))
	require.Nil(t, ref)

	// In flight: a second bulk on the same name is serialized, whatever the order kind.
	for _, r := range []*servicepb.Request{createPQ(l1, "q-1", txs, nil), updatePQ(l1, "q-1", nil), deletePQ(l1, "q-1")} {
		_, ref = begin(testutil.TxReq(l1, "", "world", "users:1", "COIN", 1), r)
		require.NotNil(t, ref)
		require.Equal(t, oc.CodeSerialized, ref.Code)
		require.Equal(t, 409, ref.HTTP)
	}
	// Different names, different ledgers and bulks without a prepared-query order never are.
	other, ref := begin(createPQ(l1, "q-2", accts, nil))
	require.Nil(t, ref)
	_, ref = begin(createPQ(s.Ledger(2), "q-1", accts, nil))
	require.Nil(t, ref)
	_, ref = begin(testutil.TxReq(l1, "", "world", "users:1", "COIN", 1))
	require.Nil(t, ref)

	// Pending (committed, not drained because `other` is still in flight):
	// still serialized.
	out := s.Execute(first)
	st, evs, _ := s.T.EndWrite(first, out)
	require.Empty(t, testutil.Findings(evs))
	require.Equal(t, "pending", st)
	require.Len(t, s.T.PendingTickets(), 1)
	_, ref = begin(updatePQ(l1, "q-1", prefix("t-")))
	require.NotNil(t, ref, "pending bulk on the name")

	// After drain: accepted.
	for _, tk := range s.T.InflightTickets() {
		_, evs, _ := s.T.EndWrite(tk, s.Execute(tk))
		require.Empty(t, testutil.Findings(evs))
	}
	require.NotZero(t, other)
	require.Empty(t, s.T.InflightTickets())
	require.Empty(t, s.T.PendingTickets())
	_, ref = begin(updatePQ(l1, "q-1", prefix("t-")))
	require.Nil(t, ref)
}

func TestBeginSerializeSameBulkCreateAndModify(t *testing.T) {
	t.Parallel()
	s := testutil.NewSession("")
	l1 := s.Ledger(1)
	for _, r := range []*servicepb.Request{updatePQ(l1, "q-1", nil), deletePQ(l1, "q-1")} {
		_, _, _, ref := s.T.BeginWrite(s.Record(false, createPQ(l1, "q-1", accts, nil), r))
		require.NotNil(t, ref)
		require.Equal(t, oc.CodeSerialized, ref.Code)
	}
	// Two different names in one bulk are fine.
	_, _, _, ref := s.T.BeginWrite(s.Record(false, createPQ(l1, "q-1", accts, nil), deletePQ(l1, "q-2")))
	require.Nil(t, ref)
}

// ---- view ----

func TestPreparedQueriesView(t *testing.T) {
	t.Parallel()
	s := fixture(t)
	l1 := s.Ledger(1)
	require.Empty(t, testutil.Findings(s.Write(false, createPQ(l1, "own-pq-asset1", accts, hasAsset("USD", 2)))))
	views, err := s.T.ComputeViews(s.T.Model(), []json.RawMessage{
		json.RawMessage(`{"name":"preparedQueries","ledger":"` + l1 + `"}`),
		json.RawMessage(`{"name":"preparedQueries","ledger":"` + l1 + `","owned":true}`),
		json.RawMessage(`{"name":"preparedQueries","ledgerPick":7}`),
	})
	require.NoError(t, err)
	var v struct {
		Ledger  string         `json:"ledger"`
		Queries []pc.ViewQuery `json:"queries"`
	}
	require.NoError(t, json.Unmarshal(views[0], &v))
	require.Equal(t, l1, v.Ledger)
	require.Len(t, v.Queries, 3)
	require.Equal(t, pc.ViewQuery{Name: "q-1", Target: "QUERY_TARGET_ACCOUNTS", Params: []pc.ViewParam{{Name: "p0", Kind: "address"}}}, v.Queries[0])
	require.Equal(t, "QUERY_TARGET_TRANSACTIONS", v.Queries[1].Target)
	require.Empty(t, v.Queries[2].Params)
	require.NoError(t, json.Unmarshal(views[1], &v))
	require.Len(t, v.Queries, 4, "owned shows own- names")
	require.NoError(t, json.Unmarshal(views[2], &v))
	require.Equal(t, l1, v.Ledger)
}

// ---- ListPreparedQueries ----

func TestListPreparedQueries(t *testing.T) {
	t.Parallel()
	s := fixture(t)
	l1 := s.Ledger(1)
	ls := s.T.Model().Ledger(l1)
	var served []*commonpb.PreparedQuery
	for _, n := range ls.PreparedQueryNames() {
		q, _ := ls.PreparedQuery(n)
		served = append(served, q)
	}
	req := &servicepb.ListPreparedQueriesRequest{Ledger: l1}

	slices.Reverse(served) // no ordering contract
	requireClean(t, validateAt(t, s, pc.KindListPreparedQueries, req, okOut(t, &servicepb.ListPreparedQueriesResponse{Queries: served}), nil).evs)

	mutated := proto.Clone(served[0]).(*commonpb.PreparedQuery)
	mutated.Filter = prefix("zzz")
	bad := append([]*commonpb.PreparedQuery{mutated}, served[1:]...)
	requireFinding(t, validateAt(t, s, pc.KindListPreparedQueries, req, okOut(t, &servicepb.ListPreparedQueriesResponse{Queries: bad}), nil).evs, pc.EvListingOutsideModel)
	requireFinding(t, validateAt(t, s, pc.KindListPreparedQueries, req, okOut(t, &servicepb.ListPreparedQueriesResponse{Queries: served[1:]}), nil).evs, pc.EvListingOutsideModel)

	// A concurrent delete makes both listings legal.
	tk := s.Begin(false, deletePQ(l1, "q-3"))
	requireClean(t, validateAt(t, s, pc.KindListPreparedQueries, req, okOut(t, &servicepb.ListPreparedQueriesResponse{Queries: served}), nil).evs)
	var without []*commonpb.PreparedQuery
	for _, q := range served {
		if q.GetName() != "q-3" {
			without = append(without, q)
		}
	}
	requireClean(t, validateAt(t, s, pc.KindListPreparedQueries, req, okOut(t, &servicepb.ListPreparedQueriesResponse{Queries: without}), nil).evs)
	_, evs := s.End(tk)
	require.Empty(t, testutil.Findings(evs))

	requireFinding(t, validateAt(t, s, pc.KindListPreparedQueries, req, errOut("Internal", ""), nil).evs, pc.EvListUnexpectedError)
	requireFinding(t, validateAt(t, s, pc.KindListPreparedQueries, req, errOut("NotFound", "LEDGER_NOT_FOUND"), nil).evs, oc.EvLedgerNotFoundUnexplained)
	requireClean(t, validateAt(t, s, pc.KindListPreparedQueries, &servicepb.ListPreparedQueriesRequest{Ledger: "o-test-absent-1"}, errOut("NotFound", "LEDGER_NOT_FOUND"), nil).evs)
}

// ---- ExecutePreparedQuery: parameters ----

func TestExecuteParameterSubstitution(t *testing.T) {
	t.Parallel()
	s := fixture(t)
	l1 := s.Ledger(1)
	gs := s.T.Model()
	params := map[string]*commonpb.ParameterValue{"p0": strParam("users:")}
	page := accountPage(gs, l1, prefix("users:"), "", 3, false)
	r := validateAt(t, s, pc.KindExecutePreparedQuery, execReq(l1, "q-1", params, 3, list, false), okOut(t, page), nil)
	requireClean(t, r.evs)
	require.True(t, reached(r.evs, pc.EvPageValidated))

	// Missing or mistyped parameter: only a compilation (or index) error is legal.
	for _, p := range []map[string]*commonpb.ParameterValue{nil, {"p0": uintParam(1)}} {
		req := execReq(l1, "q-1", p, 3, list, false)
		requireClean(t, validateAt(t, s, pc.KindExecutePreparedQuery, req, errOut("InvalidArgument", "FILTER_COMPILATION_ERROR"), nil).evs)
		requireFinding(t, validateAt(t, s, pc.KindExecutePreparedQuery, req, okOut(t, page), nil).evs, pc.EvPageOutsideModel)
		requireFinding(t, validateAt(t, s, pc.KindExecutePreparedQuery, req, errOut("NotFound", "PREPARED_QUERY_NOT_FOUND"), nil).evs, pc.EvPageOutsideModel)
	}
	// Resolved parameters never explain a compilation error.
	requireFinding(t, validateAt(t, s, pc.KindExecutePreparedQuery, execReq(l1, "q-1", params, 3, list, false), errOut("InvalidArgument", "FILTER_COMPILATION_ERROR"), nil).evs, pc.EvPageOutsideModel)
	// An absent name explains only NOT_FOUND.
	requireClean(t, validateAt(t, s, pc.KindExecutePreparedQuery, execReq(l1, "q-99", nil, 3, list, false), errOut("NotFound", "PREPARED_QUERY_NOT_FOUND"), nil).evs)
	// Wrong rows, wrong page size echo, unexpected errors.
	wrong := accountPage(gs, l1, prefix("t-"), "", 3, false)
	requireFinding(t, validateAt(t, s, pc.KindExecutePreparedQuery, execReq(l1, "q-1", params, 3, list, false), okOut(t, wrong), nil).evs, pc.EvPageOutsideModel)
	requireFinding(t, validateAt(t, s, pc.KindExecutePreparedQuery, execReq(l1, "q-1", params, 2, list, false), okOut(t, page), nil).evs, pc.EvCursorPageSizeMismatch)
	requireFinding(t, validateAt(t, s, pc.KindExecutePreparedQuery, execReq(l1, "q-1", params, 3, list, false), errOut("Internal", ""), nil).evs, pc.EvExecUnexpectedError)
	// A reversed page in the right order.
	rev := accountPage(gs, l1, prefix("users:"), "", 3, true)
	requireClean(t, validateAt(t, s, pc.KindExecutePreparedQuery, execReq(l1, "q-1", params, 3, list, true), okOut(t, rev), nil).evs)
	requireFinding(t, validateAt(t, s, pc.KindExecutePreparedQuery, execReq(l1, "q-1", params, 3, list, true), okOut(t, page), nil).evs, pc.EvPageOutsideModel)
}

// ---- ExecutePreparedQuery: stored definition per base ----

func TestExecuteStoredDefinitionPerBase(t *testing.T) {
	t.Parallel()
	s := fixture(t)
	l1 := s.Ledger(1)
	before := s.T.Model()
	tk := s.Begin(false, updatePQ(l1, "q-3", prefix("t-")))
	after := before.Apply(oracle.Bulk{Requests: []*servicepb.Request{updatePQ(l1, "q-3", prefix("t-"))}}).State
	req := execReq(l1, "q-3", nil, 10, list, false)

	oldPage := accountPage(before, l1, nil, "", 10, false)
	newPage := accountPage(after, l1, prefix("t-"), "", 10, false)
	requireClean(t, validateAt(t, s, pc.KindExecutePreparedQuery, req, okOut(t, oldPage), nil).evs)
	requireClean(t, validateAt(t, s, pc.KindExecutePreparedQuery, req, okOut(t, newPage), nil).evs)
	mixed := accountPage(after, l1, prefix("users:"), "", 10, false)
	requireFinding(t, validateAt(t, s, pc.KindExecutePreparedQuery, req, okOut(t, mixed), nil).evs, pc.EvPageOutsideModel)

	_, evs := s.End(tk)
	require.Empty(t, testutil.Findings(evs))
	// Drained: only the new definition.
	requireFinding(t, validateAt(t, s, pc.KindExecutePreparedQuery, req, okOut(t, oldPage), nil).evs, pc.EvPageOutsideModel)
	requireClean(t, validateAt(t, s, pc.KindExecutePreparedQuery, req, okOut(t, newPage), nil).evs)
}

// ---- ExecutePreparedQuery: follow-up page through the cursor ----

func TestExecuteResumeToken(t *testing.T) {
	t.Parallel()
	s := fixture(t)
	l1 := s.Ledger(1)
	gs := s.T.Model()
	params := map[string]*commonpb.ParameterValue{"p0": strParam("users:")}
	first := accountPage(gs, l1, prefix("users:"), "", 2, false)
	r1 := validateAt(t, s, pc.KindExecutePreparedQuery, execReq(l1, "q-1", params, 2, list, false), okOut(t, first), nil)
	requireClean(t, r1.evs)
	rec, ok := pc.Page(s.T, r1.ticket)
	require.True(t, ok)
	require.Equal(t, "users:2", string(rec.LastKey))
	next := first.GetCursor().GetNext()

	follow := func(reverse bool) *servicepb.ExecutePreparedQueryRequest {
		req := execReq(l1, "q-1", params, 2, list, reverse)
		req.Cursor = next

		return req
	}
	// Same direction: users:3, users:4.
	second := accountPage(gs, l1, prefix("users:"), "users:2", 2, false)
	r2 := validateAt(t, s, pc.KindExecutePreparedQuery, follow(false), okOut(t, second), pc.ExecParams{PrevTicket: r1.ticket})
	requireClean(t, r2.evs)
	require.True(t, reached(r2.evs, pc.EvPageValidated))
	require.False(t, reached(r2.evs, pc.EvPageResumedOpposite))
	// Re-using the first page's page as the second is a finding.
	requireFinding(t, validateAt(t, s, pc.KindExecutePreparedQuery, follow(false), okOut(t, first), pc.ExecParams{PrevTicket: r1.ticket}).evs, pc.EvPageOutsideModel)
	// Opposite direction: back from users:2 → users:1.
	back := accountPage(gs, l1, prefix("users:"), "users:2", 2, true)
	require.Len(t, back.GetCursor().GetAccountData(), 1)
	r3 := validateAt(t, s, pc.KindExecutePreparedQuery, follow(true), okOut(t, back), pc.ExecParams{PrevTicket: r1.ticket})
	requireClean(t, r3.evs)
	require.True(t, reached(r3.evs, pc.EvPageResumedOpposite))
	requireFinding(t, validateAt(t, s, pc.KindExecutePreparedQuery, follow(true), okOut(t, second), pc.ExecParams{PrevTicket: r1.ticket}).evs, pc.EvPageOutsideModel)
	// Unknown previous page or a cursor that is not the remembered one: not judged.
	require.Empty(t, validateAt(t, s, pc.KindExecutePreparedQuery, follow(false), okOut(t, first), pc.ExecParams{PrevTicket: 99999}).evs)
	other := follow(false)
	other.Cursor = "something-else"
	require.Empty(t, validateAt(t, s, pc.KindExecutePreparedQuery, other, okOut(t, first), pc.ExecParams{PrevTicket: r1.ticket}).evs)
}

// ---- ExecutePreparedQuery: aggregate ----

func TestExecuteAggregate(t *testing.T) {
	t.Parallel()
	s := fixture(t)
	l1 := s.Ledger(1)
	gs := s.T.Model()
	params := map[string]*commonpb.ParameterValue{"p0": strParam("users:")}
	agg := matchtest.RenderAggregateFiltered(gs, l1, prefix("users:"), match.AggOptions{})
	resp := func(a *commonpb.AggregateResult) *servicepb.ExecutePreparedQueryResponse {
		return &servicepb.ExecutePreparedQueryResponse{Result: &servicepb.ExecutePreparedQueryResponse_Aggregate{Aggregate: a}}
	}
	r := validateAt(t, s, pc.KindExecutePreparedQuery, execReq(l1, "q-1", params, 0, aggr, false), okOut(t, resp(agg)), nil)
	requireClean(t, r.evs)
	require.True(t, reached(r.evs, pc.EvAggregateValidated))

	// Exact bucket set: an extra zero bucket, a dropped bucket or a wrong
	// total is a finding.
	all := matchtest.RenderAggregateFiltered(gs, l1, nil, match.AggOptions{})
	require.Greater(t, len(all.GetVolumes()), len(agg.GetVolumes()))
	requireFinding(t, validateAt(t, s, pc.KindExecutePreparedQuery, execReq(l1, "q-1", params, 0, aggr, false), okOut(t, resp(all)), nil).evs, pc.EvAggregateOutsideModel)
	extra := proto.Clone(agg).(*commonpb.AggregateResult)
	extra.Volumes = append(extra.Volumes, &commonpb.AggregatedVolume{Asset: "ZZZ", Input: commonpb.NewUint256FromUint64(0), Output: commonpb.NewUint256FromUint64(0)})
	requireFinding(t, validateAt(t, s, pc.KindExecutePreparedQuery, execReq(l1, "q-1", params, 0, aggr, false), okOut(t, resp(extra)), nil).evs, pc.EvAggregateOutsideModel)
	requireFinding(t, validateAt(t, s, pc.KindExecutePreparedQuery, execReq(l1, "q-1", params, 0, aggr, false), okOut(t, resp(&commonpb.AggregateResult{})), nil).evs, pc.EvAggregateOutsideModel)
	// Match-all query: the whole ledger.
	requireClean(t, validateAt(t, s, pc.KindExecutePreparedQuery, execReq(l1, "q-3", nil, 0, aggr, false), okOut(t, resp(all)), nil).evs)
	// A list result in aggregate mode is wrong.
	requireFinding(t, validateAt(t, s, pc.KindExecutePreparedQuery, execReq(l1, "q-3", nil, 0, aggr, false), okOut(t, accountPage(gs, l1, nil, "", 3, false)), nil).evs, pc.EvAggregateOutsideModel)

	// Misuse probes.
	r = validateAt(t, s, pc.KindExecutePreparedQuery, execReq(l1, "q-2", nil, 0, aggr, false), errOut("InvalidArgument", ""), nil)
	requireClean(t, r.evs)
	require.True(t, reached(r.evs, pc.EvAggregateTargetRejected))
	requireFinding(t, validateAt(t, s, pc.KindExecutePreparedQuery, execReq(l1, "q-2", nil, 0, aggr, false), okOut(t, resp(all)), nil).evs, pc.EvAggregateOutsideModel)
	r = validateAt(t, s, pc.KindExecutePreparedQuery, execReq(l1, "q-1", params, 0, aggr, true), errOut("InvalidArgument", ""), nil)
	requireClean(t, r.evs)
	require.True(t, reached(r.evs, pc.EvReverseAggregateRejected))
	requireFinding(t, validateAt(t, s, pc.KindExecutePreparedQuery, execReq(l1, "q-1", params, 0, aggr, true), okOut(t, resp(agg)), nil).evs, pc.EvReverseAggregateNotRejected)
	// Even on an absent ledger, reverse aggregate is refused first.
	r = validateAt(t, s, pc.KindExecutePreparedQuery, execReq("o-test-absent-1", "q-1", nil, 0, aggr, true), errOut("InvalidArgument", ""), nil)
	require.True(t, reached(r.evs, pc.EvReverseAggregateRejected))
}

// ---- failure hook ----

func TestFailureCoverage(t *testing.T) {
	t.Parallel()
	s := fixture(t)
	l1 := s.Ledger(1)
	bad := &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Address{Address: &commonpb.AddressMatch{}}}

	// Malformed at create: admission stage.
	evs := s.Write(true, createPQ(l1, "q-9", accts, bad))
	require.Empty(t, testutil.Findings(evs), "%+v", evs)
	require.True(t, reached(evs, pc.EvMalformedFilterRefused))
	require.False(t, reached(evs, pc.EvUpdateInvalidForStoredTarget))

	// Update invalid for the stored (TRANSACTIONS) target: FSM stage, uncommitted.
	evs = s.Write(true, updatePQ(l1, "q-2", hasAsset("USD", 2)))
	require.Empty(t, testutil.Findings(evs), "%+v", evs)
	require.True(t, reached(evs, pc.EvUpdateInvalidForStoredTarget))
	require.True(t, reached(evs, pc.EvMalformedFilterRefused))
	// Never frozen: not replayable.
	for _, e := range s.T.Replayable() {
		require.NotEqual(t, "FILTER_COMPILATION_ERROR", e.Reason)
	}
	// Other rejections: no coverage.
	evs = s.Write(false, updatePQ(l1, "q-77", prefix("users:")))
	require.False(t, reached(evs, pc.EvMalformedFilterRefused))
}
