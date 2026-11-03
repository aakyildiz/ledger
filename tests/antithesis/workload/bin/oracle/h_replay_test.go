package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/bin/oracle/servicetest"
	oc "github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck/replaycheck"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck/testutil"
	"github.com/formancehq/ledger/v3/tests/oracle"
)

func replayApplyJSON(t *testing.T, key string, reqs ...*servicepb.Request) json.RawMessage {
	t.Helper()
	b, err := protojson.Marshal(servicepb.UnsignedApplyRequest(key, reqs...))
	require.NoError(t, err)

	return b
}

// replayEnd ends a Replay read with params.wid.
func replayEnd(c *servicetest.Client, ticket uint64, request json.RawMessage, out oc.Outcome, wid string) map[string]any {
	return c.OK("POST", "/read/end", map[string]any{"ticket": ticket, "kind": replaycheck.ReadKindReplay, "request": request, "outcome": out,
		"route": map[string]any{"client": 0, "route": "lib", "node": "", "consistency": "linearizable"}, "hint": map[string]any{},
		"params": map[string]any{"wid": wid}})
}

func coverageList(m map[string]any) []string {
	var out []string
	for _, x := range m["coverage"].([]any) {
		out = append(out, x.(string))
	}

	return out
}

func TestReplayService_ViewEndAndSkips(t *testing.T) {
	t.Parallel()
	_, c := setupWithLedger(t)
	reqs := []*servicepb.Request{testutil.TxReq(c.Ledger(1), "", "world", "users:1", "COIN", 5)}
	body := c.BeginWriteBody(true, reqs...)
	wid, key := body["wid"].(string), body["idempotencyKey"].(string)
	m := c.OK("POST", "/write/begin", body)
	tk := uint64(m["ticket"].(float64))
	// The fake server answers the commit (c.Execute only knows c.BeginWrite tickets).
	committed := testutil.OK(testutil.SimulateCommit(c.Server, oracle.Bulk{Requests: reqs, IdempotencyKey: key}, c.NextSeq, 1_700_000_000_000_000))
	okv, viol := servicetest.Verdict(c.EndWrite(tk, committed))
	require.True(t, okv, "%v", viol)

	request := replayApplyJSON(t, key, reqs...)
	rb := c.BeginRead(replaycheck.ReadKindReplay, map[string]any{"name": "replayable"})
	views := rb["views"].([]any)
	entries := views[0].(map[string]any)["entries"].([]any)
	require.Len(t, entries, 1)
	require.Equal(t, wid, entries[0].(map[string]any)["wid"])
	require.Equal(t, "committed", entries[0].(map[string]any)["status"])

	m = replayEnd(c, uint64(rb["ticket"].(float64)), request, committed, wid)
	okv, viol = servicetest.Verdict(m)
	require.True(t, okv, "%v", viol)
	require.Contains(t, coverageList(m), string(replaycheck.EvReplayExercised))

	// Auth errors and maintenance are transient for a kind that sends an Apply.
	for _, o := range []oc.Outcome{testutil.Fail("Unauthenticated", ""), testutil.Fail("PermissionDenied", ""), testutil.Fail("Unavailable", "MAINTENANCE_MODE")} {
		rb = c.BeginRead(replaycheck.ReadKindReplay)
		m = replayEnd(c, uint64(rb["ticket"].(float64)), request, o, wid)
		require.True(t, m["verdict"].(map[string]any)["skipped"].(bool), "%+v", o)
	}

	// A definitive error for a committed original is a finding.
	rb = c.BeginRead(replaycheck.ReadKindReplay)
	m = replayEnd(c, uint64(rb["ticket"].(float64)), request, testutil.Fail("FailedPrecondition", "INSUFFICIENT_FUNDS"), wid)
	okv, viol = servicetest.Verdict(m)
	require.False(t, okv)
	require.Equal(t, []string{string(replaycheck.EvReplayUnexpectedError)}, viol)
}

// TestReplayIntegration_RealServer drives the family's shapes against a real
// in-process single-node server: a committed replay, a frozen failure and its
// replay, a conflict on a committed key, and the skip-and-continue bulks.
func TestReplayIntegration_RealServer(t *testing.T) {
	if testing.Short() {
		t.Skip("in-process server")
	}
	ctx, client := singleNodeServer(t, "oracle-replay")
	srv, c := newTestServer(t)
	c.Setup("replay")
	l := c.Ledger(1)

	type sent struct {
		wid, key string
		reqs     []*servicepb.Request
		out      oc.Outcome
		end      map[string]any
	}
	write := func(trackKey bool, role, of, key string, reqs ...*servicepb.Request) sent {
		body := c.BeginWriteBody(trackKey, reqs...)
		if key != "" {
			body["idempotencyKey"] = key
		}
		if role != "" {
			body["role"] = role
			body["of"] = of
		}
		m := c.OK("POST", "/write/begin", body)
		tk := uint64(m["ticket"].(float64))
		out, err := applyUntilDefinitive(ctx, client, body["idempotencyKey"].(string), reqs)
		require.NoError(t, err)
		end := c.EndWrite(tk, out)
		okv, viol := servicetest.Verdict(end)
		require.True(t, okv, "%v (outcome %+v)", viol, out)

		return sent{wid: body["wid"].(string), key: body["idempotencyKey"].(string), reqs: reqs, out: out, end: end}
	}
	replay := func(s sent) map[string]any {
		rb := c.BeginRead(replaycheck.ReadKindReplay)
		out := outcomeOf(client.Apply(ctx, servicepb.UnsignedApplyRequest(s.key, s.reqs...)))
		m := replayEnd(c, uint64(rb["ticket"].(float64)), replayApplyJSON(t, s.key, s.reqs...), out, s.wid)
		okv, viol := servicetest.Verdict(m)
		require.True(t, okv, "%v (outcome %+v)", viol, out)

		return m
	}

	write(false, "", "", "", testutil.CreateLedgerReq(l))
	create := write(true, "", "", "", testutil.TxReq(l, "", "world", "users:1", "COIN", 5)) // tx 1
	require.True(t, create.out.OK)
	require.Contains(t, coverageList(replay(create)), string(replaycheck.EvReplayExercised))

	// Revert tx 1, then a flagged second revert: frozen at the FSM stage.
	write(false, "", "", "", testutil.RevertReq(l, 1))
	again := write(true, "", "", "", testutil.RevertReq(l, 1))
	require.Equal(t, "TRANSACTION_ALREADY_REVERTED", again.out.Reason)
	cov := coverageList(replay(again))
	require.Contains(t, cov, string(replaycheck.EvReplayFrozenReturned))

	// A different body under the committed key.
	conflict := write(true, oc.RoleConflict, create.wid, create.key, testutil.TxReq(l, "", "world", "users:1", "COIN", 6))
	require.Equal(t, "IDEMPOTENCY_KEY_CONFLICT", conflict.out.Reason)

	// Skip-and-continue: the reverted transaction and an absent key, opted in.
	skipRevert := testutil.RevertReq(l, 1)
	skipRevert.GetApply().SkippableReasons = []commonpb.ErrorReason{commonpb.ErrorReason_ERROR_REASON_TRANSACTION_ALREADY_REVERTED}
	skipMeta := &servicepb.Request{Type: &servicepb.Request_Apply{Apply: &servicepb.LedgerApplyRequest{
		Ledger:           l,
		SkippableReasons: []commonpb.ErrorReason{commonpb.ErrorReason_ERROR_REASON_METADATA_NOT_FOUND},
		Action: &servicepb.LedgerAction{Data: &servicepb.LedgerAction_DeleteMetadata{DeleteMetadata: &commonpb.DeleteMetadataCommand{
			Target: &commonpb.Target{Target: &commonpb.Target_TransactionId{TransactionId: 1}}, Key: "absent-k2",
		}}},
	}}}
	// /write/end reports only the verdict, so this bulk is ended on the
	// tracker directly (under the service lock) to see its coverage events.
	skipReqs := []*servicepb.Request{skipRevert, testutil.AccountMetaReq(l, "users:1", "k0", "v"), skipMeta, testutil.AccountMetaReq(l, "users:1", "k1", "w")}
	body := c.BeginWriteBody(true, skipReqs...)
	tk := uint64(c.OK("POST", "/write/begin", body)["ticket"].(float64))
	bulk := sent{wid: body["wid"].(string), key: body["idempotencyKey"].(string), reqs: skipReqs}
	var err error
	bulk.out, err = applyUntilDefinitive(ctx, client, bulk.key, skipReqs)
	require.NoError(t, err)
	require.True(t, bulk.out.OK, "%+v", bulk.out)
	srv.mu.Lock()
	_, evs, ref := srv.tracker.EndWrite(tk, bulk.out)
	srv.mu.Unlock()
	require.Nil(t, ref)
	require.Empty(t, testutil.Findings(evs))
	ids := testutil.IDs(evs)
	require.Contains(t, ids, replaycheck.CovSkipAlreadyReverted, "the real server's skip-and-continue reaches the probe")
	require.Contains(t, ids, replaycheck.CovSkipMetadataNotFound, "the real server's skip-and-continue reaches the probe")
	replay(bulk)

	st := c.OK("GET", "/quiet/status", nil)
	require.EqualValues(t, 0, st["pending"])
}

// Task 03's protocol fixtures decode strictly into the service's structs, and
// the Replay request carries the original's key.
func TestReplayFixtures_Decode(t *testing.T) {
	t.Parallel()
	paths, err := filepath.Glob("../../../hegel/protocol-fixtures/03/*.json")
	require.NoError(t, err)
	require.NotEmpty(t, paths)
	for _, p := range paths {
		b, err := os.ReadFile(p)
		require.NoError(t, err)
		var f struct {
			Endpoint string          `json:"endpoint"`
			Request  json.RawMessage `json:"request"`
		}
		require.NoError(t, json.Unmarshal(b, &f), p)
		switch f.Endpoint {
		case "POST /read/begin":
			var r readBeginReq
			require.NoError(t, decodeStrict(f.Request, &r), p)
			require.Equal(t, replaycheck.ReadKindReplay, r.Kind)
		case "POST /read/end":
			var r readEndReq
			require.NoError(t, decodeStrict(f.Request, &r), p)
			var ar servicepb.ApplyRequest
			require.NoError(t, protojson.Unmarshal(r.Request, &ar), p)
			require.NotEmpty(t, ar.GetUnsigned().GetIdempotencyKey(), p)
		case "POST /view":
			var r viewReq
			require.NoError(t, decodeStrict(f.Request, &r), p)
		case "POST /write/begin":
			var r writeBeginReq
			require.NoError(t, decodeStrict(f.Request, &r), p)
		default:
			t.Fatalf("%s: unexpected endpoint %s", p, f.Endpoint)
		}
	}
}
