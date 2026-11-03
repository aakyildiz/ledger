package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/bin/oracle/servicetest"
	oc "github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck/match"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck/pointcheck"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck/testutil"
	"github.com/formancehq/ledger/v3/tests/oracle/oracletest"
)

func pointOutcome(m proto.Message, err error) oc.Outcome {
	if err != nil {
		return outcomeOf(nil, err)
	}
	b, mErr := protojson.Marshal(m)
	if mErr != nil {
		panic(mErr)
	}

	return oc.Outcome{OK: true, Response: b}
}

// TestIntegration_PointReads feeds real point-read answers of an in-process
// single-node server to the oracle service: every read must validate.
func TestIntegration_PointReads(t *testing.T) {
	if testing.Short() {
		t.Skip("in-process server")
	}
	ctx, client := singleNodeServer(t, "oracle-point-reads")
	_, c := newTestServer(t)
	c.Setup("point")
	l1, l2 := c.Ledger(1), c.Ledger(2)

	write := func(reqs ...*servicepb.Request) {
		t.Helper()
		body := c.BeginWriteBody(false, reqs...)
		m := c.OK("POST", "/write/begin", body)
		out, err := applyUntilDefinitive(ctx, client, body["idempotencyKey"].(string), reqs)
		require.NoError(t, err)
		ok, viol := servicetest.Verdict(c.EndWrite(uint64(m["ticket"].(float64)), out))
		require.True(t, ok, "%v (outcome %+v)", viol, out)
	}
	write(testutil.CreateLedgerReq(l1), testutil.CreateLedgerReq(l2))
	write(testutil.TxReq(l1, "", "world", "t-0:1", "USD/2", 10))
	write(oracletest.TxReqColoredL(l1, "world", "t-0:1", "USD/2", "GOLD", 4))
	write(testutil.TxReq(l1, c.Prefix+"ref1", "world", "t-0:2", "COIN", 7), testutil.AccountMetaReq(l1, "t-0:1", "k0", "v0"))
	write(testutil.TxReq(l1, "", "t-0:1", "t-0:3", "USD/2", 3))
	write(testutil.RevertReq(l1, 3))
	write(testutil.TxReq(l2, "", "world", "t-1:1", "COIN", 2))
	write(testutil.DeleteLedgerReq(l2))

	coverage := map[string]int{}
	read := func(kind string, views []map[string]any, call func(views []json.RawMessage) (proto.Message, oc.Outcome)) {
		t.Helper()
		rb := c.BeginRead(kind, views...)
		raw, _ := json.Marshal(rb["views"])
		var vs []json.RawMessage
		require.NoError(t, json.Unmarshal(raw, &vs))
		req, out := call(vs)
		reqJSON, err := protojson.Marshal(req)
		require.NoError(t, err)
		m := c.EndRead(uint64(rb["ticket"].(float64)), kind, json.RawMessage(reqJSON), out)
		ok, viol := servicetest.Verdict(m)
		require.True(t, ok, "%s %s: %v (outcome %+v)", kind, reqJSON, viol, out)
		cov, _ := m["coverage"].([]any)
		for _, x := range cov {
			coverage[fmt.Sprint(x)]++
		}
	}
	absent := c.Prefix + "absent-0123"

	for _, ledger := range []string{l1, l2, absent} {
		for _, addr := range []string{"t-0:1", "t-0:2", "t-0:3", "world", "t-9:9", "t-1:1"} {
			for _, collapse := range []bool{false, true} {
				read(pointcheck.KindGetAccount, nil, func([]json.RawMessage) (proto.Message, oc.Outcome) {
					r := &servicepb.GetAccountRequest{Ledger: ledger, Address: addr, CollapseColors: collapse}
					a, err := client.GetAccount(ctx, r)

					return r, pointOutcome(a, err)
				})
			}
		}
		for id := uint64(1); id <= 8; id++ {
			read(pointcheck.KindGetTransaction, nil, func([]json.RawMessage) (proto.Message, oc.Outcome) {
				r := &servicepb.GetTransactionRequest{Ledger: ledger, TransactionId: id}
				tx, err := client.GetTransaction(ctx, r)

				return r, pointOutcome(tx, err)
			})
		}
		read(pointcheck.KindGetLedgerStats, nil, func([]json.RawMessage) (proto.Message, oc.Outcome) {
			r := &servicepb.GetLedgerStatsRequest{Ledger: ledger}
			st, err := client.GetLedgerStats(ctx, r)

			return r, pointOutcome(st, err)
		})
		for _, f := range []*commonpb.QueryFilter{
			nil,
			match.FilterOr(match.FilterAddrPrefix("t-0:"), match.FilterNot(match.FilterAddrExact("t-0:2"))),
			match.FilterAnd(match.FilterMetaExists("k0"), match.FilterAddrPrefix("t-0:")),
		} {
			for _, groups := range [][]string{nil, {"t-0:", "no-such-prefix:", "world"}} {
				for _, flags := range []bool{false, true} {
					read(pointcheck.KindAggregateVolumes, nil, func([]json.RawMessage) (proto.Message, oc.Outcome) {
						r := &servicepb.AggregateVolumesRequest{Ledger: ledger, Filter: f, GroupByPrefixes: groups, CollapseColors: flags, UseMaxPrecision: flags}
						res, err := client.AggregateVolumes(ctx, r)

						return r, pointOutcome(res, err)
					})
				}
			}
		}
	}

	// GetLog: every sampled learned sequence, and the unassigned probe.
	var seqs []string
	var maxSeq uint64
	read(pointcheck.KindGetLog, []map[string]any{{"name": pointcheck.ViewLogSeqs}}, func(vs []json.RawMessage) (proto.Message, oc.Outcome) {
		var v struct {
			Seqs []string `json:"seqs"`
			Max  string   `json:"maxLearnedSeq"`
		}
		require.NoError(t, json.Unmarshal(vs[0], &v))
		seqs = v.Seqs
		var err error
		maxSeq, err = strconv.ParseUint(v.Max, 10, 64)
		require.NoError(t, err)
		r := &servicepb.GetLogRequest{Sequence: maxSeq + pointcheck.UnassignedSeqSlack}
		l, err := client.GetLog(ctx, r)

		return r, pointOutcome(l, err)
	})
	require.NotEmpty(t, seqs)
	for _, s := range seqs {
		seq, err := strconv.ParseUint(s, 10, 64)
		require.NoError(t, err)
		read(pointcheck.KindGetLog, nil, func([]json.RawMessage) (proto.Message, oc.Outcome) {
			r := &servicepb.GetLogRequest{Sequence: seq}
			l, err := client.GetLog(context.WithoutCancel(ctx), r)

			return r, pointOutcome(l, err)
		})
	}

	t.Logf("coverage: %v", coverage)
	for _, id := range []oc.EventID{
		pointcheck.EvAccountAbsentLedgerProbed, pointcheck.EvAccountModelAbsentProbed, pointcheck.EvCollapsedAccountValidated,
		pointcheck.EvTxAbsentLedgerProbed, pointcheck.EvAggValidated, pointcheck.EvAggGroupedValidated,
		pointcheck.EvAggAbsentNotFound, pointcheck.EvAggGatedOnMissingIndex, pointcheck.EvStatsUsageCaughtUp,
		pointcheck.EvStatsValidated, pointcheck.EvStatsAbsentNotFound, pointcheck.EvGetLogValidated, pointcheck.EvGetLogUnassignedNotFound,
	} {
		require.Positive(t, coverage[string(id)], "%s not reached", id)
	}
}
