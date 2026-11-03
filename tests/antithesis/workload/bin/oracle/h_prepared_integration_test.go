package main

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/bin/oracle/servicetest"
	oc "github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck/preparedcheck"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck/testutil"
)

func pqAddr(prefix bool, v string) *commonpb.QueryFilter {
	am := &commonpb.AddressMatch{}
	if prefix {
		am.Match = &commonpb.AddressMatch_HardcodedPrefix{HardcodedPrefix: v}
	} else {
		am.Match = &commonpb.AddressMatch_ParamPrefix{ParamPrefix: v}
	}

	return &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Address{Address: am}}
}

func pqCreate(ledger, name string, target commonpb.QueryTarget, f *commonpb.QueryFilter) *servicepb.Request {
	return &servicepb.Request{Type: &servicepb.Request_CreatePreparedQuery{CreatePreparedQuery: &servicepb.CreatePreparedQueryRequest{
		Ledger: ledger, Query: &commonpb.PreparedQuery{Name: name, Target: target, Filter: f},
	}}}
}

func pqUpdate(ledger, name string, f *commonpb.QueryFilter) *servicepb.Request {
	return &servicepb.Request{Type: &servicepb.Request_UpdatePreparedQuery{UpdatePreparedQuery: &servicepb.UpdatePreparedQueryRequest{Ledger: ledger, Name: name, Filter: f}}}
}

func pqOutcome(m proto.Message, err error) oc.Outcome {
	if err != nil {
		return outcomeOf(nil, err)
	}
	b, mErr := protojson.Marshal(m)
	if mErr != nil {
		panic(mErr)
	}

	return oc.Outcome{OK: true, Response: b}
}

// TestIntegration_PreparedQueries feeds real prepared-query answers of an
// in-process single-node server to the oracle service: saves (including the
// rejected shapes, with a retry of the FSM-stage update failure), listings,
// LIST pages with a follow-up page in both directions, aggregates and the two
// misuse probes. Every verdict must be clean.
func TestIntegration_PreparedQueries(t *testing.T) {
	if testing.Short() {
		t.Skip("in-process server")
	}
	ctx, client := singleNodeServer(t, "oracle-prepared")
	_, c := newTestServer(t)
	c.Setup("prepared")
	l1 := c.Ledger(1)

	write := func(trackKey bool, reqs ...*servicepb.Request) oc.Outcome {
		t.Helper()
		body := c.BeginWriteBody(trackKey, reqs...)
		m := c.OK("POST", "/write/begin", body)
		key := body["idempotencyKey"].(string)
		out, err := applyUntilDefinitive(ctx, client, key, reqs)
		require.NoError(t, err)
		tk := uint64(m["ticket"].(float64))
		ok, viol := servicetest.Verdict(c.EndWrite(tk, out))
		require.True(t, ok, "%v (outcome %+v)", viol, out)
		if !out.OK {
			// Another attempt of the same key must yield the same outcome
			// (frozen, or for the uncommitted update failure, re-evaluated on
			// an unchanged target).
			again, err := applyUntilDefinitive(ctx, client, key, reqs)
			require.NoError(t, err)
			require.Equal(t, out.Reason, again.Reason)
		}

		return out
	}
	require.True(t, write(false, testutil.CreateLedgerReq(l1)).OK)
	for i := 1; i <= 6; i++ {
		require.True(t, write(false, testutil.TxReq(l1, "", "world", fmt.Sprintf("users:%d", i), "USD/2", uint64(i))).OK)
	}
	require.True(t, write(false, testutil.TxReq(l1, "", "world", "t-0:1", "COIN", 3)).OK)
	require.True(t, write(true, pqCreate(l1, "q-1", commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS, pqAddr(false, "p0"))).OK)
	require.True(t, write(true, pqCreate(l1, "q-2", commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS, pqAddr(true, "users:"))).OK)
	require.True(t, write(false, pqCreate(l1, "q-3", commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS, nil)).OK)
	// A parameterised TRANSACTIONS query: AGGREGATE on it, with or without
	// its parameter, is the aggregate-target refusal (the server checks the
	// target before compiling the filter).
	require.True(t, write(true, pqCreate(l1, "q-4", commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS, pqAddr(false, "p0"))).OK)
	// Rejections: an existing name, a malformed filter at create, an update
	// invalid for the stored TRANSACTIONS target (FSM, uncommitted), a missing name.
	require.False(t, write(true, pqCreate(l1, "q-1", commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS, nil)).OK)
	require.False(t, write(true, pqCreate(l1, "q-9", commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS,
		&commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Address{Address: &commonpb.AddressMatch{}}})).OK)
	hasAsset := &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_AccountHasAsset{AccountHasAsset: &commonpb.AccountHasAssetCondition{AssetBase: "USD", Precision: 2}}}
	require.Equal(t, "FILTER_COMPILATION_ERROR", write(true, pqUpdate(l1, "q-2", hasAsset)).Reason)
	require.Equal(t, "PREPARED_QUERY_NOT_FOUND", write(true, pqUpdate(l1, "q-77", nil)).Reason)
	// The driver's other malformed shapes (rules/prepared.rs malformed_filter):
	// a metadata condition without a field reference, and `reverted` on an
	// ACCOUNTS query, at create and at update.
	noRef := &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Field{Field: &commonpb.FieldCondition{
		Condition: &commonpb.FieldCondition_StringCond{StringCond: &commonpb.StringCondition{Value: &commonpb.StringCondition_Hardcoded{Hardcoded: "v0"}}},
	}}}
	reverted := &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Reverted{Reverted: &commonpb.RevertedCondition{Value: true}}}
	for _, r := range []*servicepb.Request{
		pqCreate(l1, "q-10", commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS, noRef),
		pqCreate(l1, "q-11", commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS, reverted),
		pqUpdate(l1, "q-1", noRef),
		pqUpdate(l1, "q-1", reverted),
	} {
		require.Equal(t, "FILTER_COMPILATION_ERROR", write(true, r).Reason)
	}

	coverage := map[string]int{}
	read := func(kind string, params map[string]any, req proto.Message, out oc.Outcome) uint64 {
		t.Helper()
		rb := c.BeginRead(kind)
		tk := uint64(rb["ticket"].(float64))
		reqJSON, err := protojson.Marshal(req)
		require.NoError(t, err)
		if params == nil {
			params = map[string]any{}
		}
		m := c.OK("POST", "/read/end", map[string]any{"ticket": tk, "kind": kind, "request": json.RawMessage(reqJSON), "outcome": out,
			"route": map[string]any{"client": 0, "route": "lib", "node": "", "consistency": "linearizable"}, "hint": map[string]any{}, "params": params})
		ok, viol := servicetest.Verdict(m)
		require.True(t, ok, "%s %s: %v (outcome %+v)", kind, reqJSON, viol, out)
		cov, _ := m["coverage"].([]any)
		for _, x := range cov {
			coverage[fmt.Sprint(x)]++
		}

		return tk
	}

	lr := &servicepb.ListPreparedQueriesRequest{Ledger: l1}
	lresp, err := client.ListPreparedQueries(ctx, lr)
	read(preparedcheck.KindListPreparedQueries, nil, lr, pqOutcome(lresp, err))

	str := func(v string) *commonpb.ParameterValue {
		return &commonpb.ParameterValue{Value: &commonpb.ParameterValue_StringValue{StringValue: v}}
	}
	exec := func(req *servicepb.ExecutePreparedQueryRequest, params map[string]any) (uint64, *servicepb.ExecutePreparedQueryResponse) {
		t.Helper()
		resp, err := client.ExecutePreparedQuery(ctx, req)

		return read(preparedcheck.KindExecutePreparedQuery, params, req, pqOutcome(resp, err)), resp
	}
	list := commonpb.QueryMode_QUERY_MODE_LIST
	aggr := commonpb.QueryMode_QUERY_MODE_AGGREGATE_VOLUMES
	for _, name := range []string{"q-1", "q-2", "q-3", "q-4", "q-99"} {
		for _, params := range []map[string]*commonpb.ParameterValue{
			nil,
			{"p0": str("users:")},
			{"p0": {Value: &commonpb.ParameterValue_Uint64Value{Uint64Value: 1}}},
		} {
			for _, reverse := range []bool{false, true} {
				for _, flip := range []bool{false, true} {
					req := &servicepb.ExecutePreparedQueryRequest{Ledger: l1, QueryName: name, Parameters: params, PageSize: 2, Mode: list, Reverse: reverse}
					tk, resp := exec(req, nil)
					if next := resp.GetCursor().GetNext(); next != "" {
						f := proto.Clone(req).(*servicepb.ExecutePreparedQueryRequest)
						f.Cursor, f.Reverse = next, reverse != flip
						exec(f, map[string]any{"prevTicket": tk})
					}
				}
			}
			exec(&servicepb.ExecutePreparedQueryRequest{Ledger: l1, QueryName: name, Parameters: params, Mode: aggr}, nil)
			exec(&servicepb.ExecutePreparedQueryRequest{Ledger: l1, QueryName: name, Parameters: params, Mode: aggr, Reverse: true}, nil)
		}
	}
	exec(&servicepb.ExecutePreparedQueryRequest{Ledger: c.Prefix + "absent-1", QueryName: "q-1", PageSize: 5, Mode: list}, nil)

	t.Logf("coverage: %v", coverage)
	for _, id := range []oc.EventID{
		preparedcheck.EvPageValidated, preparedcheck.EvAggregateValidated, preparedcheck.EvPageResumedOpposite,
		preparedcheck.EvReverseAggregateRejected, preparedcheck.EvAggregateTargetRejected,
	} {
		require.Positive(t, coverage[string(id)], "%s not reached", id)
	}
}
