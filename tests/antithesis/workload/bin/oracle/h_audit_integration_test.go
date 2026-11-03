package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/bin/oracle/servicetest"
	oc "github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck"
	ac "github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck/auditcheck"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck/match"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck/testutil"
)

// TestIntegration_AuditReads feeds real audit answers of an in-process
// single-node server, with tracked writes through the oracle and foreign
// writes beside them, to the oracle service: every read must validate.
func TestIntegration_AuditReads(t *testing.T) {
	if testing.Short() {
		t.Skip("in-process server")
	}
	ctx, client := singleNodeServer(t, "oracle-audit-reads")
	_, c := newTestServer(t)
	c.Setup("audit")
	l1, l2 := c.Ledger(1), c.Ledger(2)

	write := func(trackKey bool, reqs ...*servicepb.Request) oc.Outcome {
		t.Helper()
		body := c.BeginWriteBody(trackKey, reqs...)
		m := c.OK("POST", "/write/begin", body)
		out, err := applyUntilDefinitive(ctx, client, body["idempotencyKey"].(string), reqs)
		require.NoError(t, err)
		ok, viol := servicetest.Verdict(c.EndWrite(uint64(m["ticket"].(float64)), out))
		require.True(t, ok, "%v (outcome %+v)", viol, out)

		return out
	}
	foreign := func(key string, reqs ...*servicepb.Request) {
		t.Helper()
		_, err := applyUntilDefinitive(ctx, client, key, reqs)
		require.NoError(t, err)
	}

	write(false, testutil.CreateLedgerReq(l1), testutil.CreateLedgerReq(l2))
	foreign("", testutil.CreateLedgerReq("r-ns-audit-1"))
	write(true, testutil.TxReq(l1, "", "world", "t-0:1", "USD/2", 10))
	foreign("r-audit-k1", testutil.TxReq("r-ns-audit-1", "", "world", "a:1", "COIN", 3))
	doomed := write(true, testutil.TxReq(l1, "", "t-9:9", "t-0:2", "USD/2", 5))
	require.False(t, doomed.OK, "an overdraft from an unfunded account is rejected")
	foreign("", testutil.TxReq("r-ns-audit-1", "", "a:9", "a:2", "COIN", 3)) // foreign rejection
	write(true, testutil.TxReq(l1, c.Prefix+"ref1", "world", "t-0:2", "COIN", 7), testutil.AccountMetaReq(l1, "t-0:1", "k0", "v0"))
	write(false, testutil.TxReq(l2, "", "world", "t-1:1", "COIN", 2))
	write(true, testutil.TxReq(l1, "", "t-0:1", "t-0:3", "USD/2", 3))

	coverage := map[string]int{}
	read := func(kind string, req proto.Message, out oc.Outcome) {
		t.Helper()
		tk := uint64(c.BeginRead(kind)["ticket"].(float64))
		reqJSON, err := protojson.Marshal(req)
		require.NoError(t, err)
		m := c.EndRead(tk, kind, json.RawMessage(reqJSON), out)
		ok, viol := servicetest.Verdict(m)
		require.True(t, ok, "%s %s: %v (outcome %+v)", kind, reqJSON, viol, out)
		cov, _ := m["coverage"].([]any)
		for _, x := range cov {
			coverage[fmt.Sprint(x)]++
		}
	}
	list := func(opts *commonpb.ListOptions) []uint64 {
		t.Helper()
		req := &servicepb.ListAuditEntriesRequest{Options: opts}
		stream, err := client.ListAuditEntries(ctx, req)
		out := oc.Outcome{}
		var seqs []uint64
		if err == nil {
			out = oc.Outcome{OK: true, Complete: true}
			for {
				e, rerr := stream.Recv()
				if errors.Is(rerr, io.EOF) {
					if next := stream.Trailer().Get("x-next-cursor"); len(next) > 0 {
						out.NextCursor = next[0]
					}

					break
				}
				if rerr != nil {
					out = outcomeOf(nil, rerr)

					break
				}
				b, mErr := protojson.Marshal(e)
				require.NoError(t, mErr)
				out.Items = append(out.Items, b)
				seqs = append(seqs, e.GetSequence())
			}
		} else {
			out = outcomeOf(nil, err)
		}
		read(ac.KindList, req, out)

		return seqs
	}

	all := list(&commonpb.ListOptions{PageSize: 100})
	require.GreaterOrEqual(t, len(all), 8)
	keyOf := c.Prefix + "k3"
	lo, hi := uint64(2), uint64(6)
	logLo, logHi := uint64(1), uint64(40)
	filters := []*commonpb.QueryFilter{
		nil,
		match.FilterAuditString(commonpb.AuditField_AUDIT_FIELD_OUTCOME, "failure"),
		match.FilterAuditString(commonpb.AuditField_AUDIT_FIELD_LEDGER, l1),
		match.FilterAuditString(commonpb.AuditField_AUDIT_FIELD_IDEMPOTENCY_KEY, keyOf),
		{Filter: &commonpb.QueryFilter_Audit{Audit: &commonpb.AuditCondition{Field: commonpb.AuditField_AUDIT_FIELD_IDEMPOTENCY_KEY,
			Condition: &commonpb.AuditCondition_StringPrefix{StringPrefix: c.Prefix + "k"}}}},
		match.FilterAuditUint(commonpb.AuditField_AUDIT_FIELD_SEQUENCE, &commonpb.UintCondition{Min: &lo, Max: &hi}),
		match.FilterAuditUint(commonpb.AuditField_AUDIT_FIELD_LOG_SEQUENCE, &commonpb.UintCondition{Min: &logLo, Max: &logHi}),
		match.FilterAuditString(commonpb.AuditField_AUDIT_FIELD_ORDER_TYPE, "create_transaction"),
		match.FilterAuditString(commonpb.AuditField_AUDIT_FIELD_ORDER_TYPE, "add_metadata"),
		{Filter: &commonpb.QueryFilter_Or{Or: &commonpb.OrFilter{Filters: []*commonpb.QueryFilter{
			match.FilterAuditString(commonpb.AuditField_AUDIT_FIELD_LEDGER, l2),
			match.FilterAuditString(commonpb.AuditField_AUDIT_FIELD_OUTCOME, "failure"),
		}}}},
		{Filter: &commonpb.QueryFilter_And{And: &commonpb.AndFilter{Filters: []*commonpb.QueryFilter{
			match.FilterAuditUint(commonpb.AuditField_AUDIT_FIELD_SEQUENCE, &commonpb.UintCondition{Min: &lo}),
			match.FilterAuditString(commonpb.AuditField_AUDIT_FIELD_OUTCOME, "success"),
		}}}},
	}
	for _, f := range filters {
		for _, size := range []uint32{0, 1, 2, 5} {
			for _, reverse := range []bool{false, true} {
				for _, cursor := range []string{"", match.PageToken("3")} {
					list(&commonpb.ListOptions{PageSize: size, Reverse: reverse, Cursor: cursor, Filter: f})
				}
			}
		}
	}

	// Refused probes and malformed cursors.
	nul := &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Audit{Audit: &commonpb.AuditCondition{Field: commonpb.AuditField_AUDIT_FIELD_IDEMPOTENCY_KEY,
		Condition: &commonpb.AuditCondition_StringPrefix{StringPrefix: "o-\x00bad"}}}}
	param := &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Audit{Audit: &commonpb.AuditCondition{Field: commonpb.AuditField_AUDIT_FIELD_IDEMPOTENCY_KEY,
		Condition: &commonpb.AuditCondition_StringCond{StringCond: &commonpb.StringCondition{Value: &commonpb.StringCondition_Param{Param: "p0"}}}}}}
	for _, f := range []*commonpb.QueryFilter{
		{Filter: &commonpb.QueryFilter_Or{Or: &commonpb.OrFilter{Filters: []*commonpb.QueryFilter{
			match.FilterAuditUint(commonpb.AuditField_AUDIT_FIELD_SEQUENCE, &commonpb.UintCondition{Min: &lo, Max: &hi}),
			match.FilterAuditString(commonpb.AuditField_AUDIT_FIELD_LEDGER, l1),
		}}}},
		{Filter: &commonpb.QueryFilter_Not{Not: &commonpb.NotFilter{Filter: match.FilterAuditString(commonpb.AuditField_AUDIT_FIELD_LEDGER, l1)}}},
		match.FilterAuditString(commonpb.AuditField_AUDIT_FIELD_UNSPECIFIED, "anything"),
		match.FilterAuditString(commonpb.AuditField_AUDIT_FIELD_SEQUENCE, "not-a-number"),
		match.FilterAuditUint(commonpb.AuditField_AUDIT_FIELD_LEDGER, &commonpb.UintCondition{Min: &lo}),
		param, nul,
	} {
		list(&commonpb.ListOptions{PageSize: 5, Filter: f})
	}
	for _, bad := range []string{"not-a-cursor", "1.0", match.PageToken("-1"), match.PageToken("18446744073709551616")} {
		list(&commonpb.ListOptions{PageSize: 5, Cursor: bad})
	}

	for _, seq := range append(all, math.MaxUint64) {
		req := &servicepb.GetAuditEntryRequest{Sequence: seq}
		e, err := client.GetAuditEntry(ctx, req)
		read(ac.KindGet, req, pointOutcomeOf(e, err))
	}

	t.Logf("coverage: %v", coverage)
	for _, id := range []oc.EventID{
		ac.EvAuditPageValidated, ac.EvAuditEntryReadValidated, ac.EvAuditEntryCarriedItems, ac.EvAuditUnassignedNotFound,
		ac.EvAuditFailureMatchedRejection, ac.EvAuditBatchKeyMatched, ac.EvAuditJudgedAgainstKnown, ac.EvAuditCursorCorroborated,
		ac.EvAuditSeqInOrRejected, ac.EvAuditUnspecifiedRejected, ac.EvAuditTypeMismatchRejected, ac.EvAuditKeyParamRejected,
		ac.EvAuditNulPrefixRejected, ac.EvAuditNotRejected, ac.EvAuditMalformedCursorRejected,
	} {
		require.Positive(t, coverage[string(id)], "%s not reached", id)
	}
}

func pointOutcomeOf(m proto.Message, err error) oc.Outcome {
	if err != nil {
		return outcomeOf(nil, err)
	}
	b, mErr := protojson.Marshal(m)
	if mErr != nil {
		panic(mErr)
	}

	return oc.Outcome{OK: true, Response: b}
}
