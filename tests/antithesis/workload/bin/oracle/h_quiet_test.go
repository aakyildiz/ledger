package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/formancehq/ledger/v3/internal/proto/auditpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/pkg/actions"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/bin/oracle/servicetest"
	oc "github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck/match"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck/match/matchtest"
	qc "github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck/quietcheck"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck/testutil"
	"github.com/formancehq/ledger/v3/tests/oracle"
)

// quietWrite begins, executes and ends a write, and returns the audit
// success entry a correct server writes for it.
func quietWrite(t *testing.T, c *servicetest.Client, seq uint64, reqs ...*servicepb.Request) json.RawMessage {
	t.Helper()
	body := c.BeginWriteBody(false, reqs...)
	m := c.OK("POST", "/write/begin", body)
	tk := uint64(m["ticket"].(float64))
	key := body["idempotencyKey"].(string)
	first := c.NextSeq
	out := quietExecute(t, c, key, reqs)
	okv, viol := servicetest.Verdict(c.EndWrite(tk, out))
	require.True(t, okv, "%v", viol)
	var ledgers []string
	for _, r := range reqs {
		if l := oracle.LedgerOf(r); l != "" && (len(ledgers) == 0 || ledgers[len(ledgers)-1] != l) {
			ledgers = append(ledgers, l)
		}
	}
	e := &auditpb.AuditEntry{Sequence: seq, OrderCount: uint32(len(reqs)), Ledgers: ledgers,
		Idempotency: &commonpb.Idempotency{Key: key},
		Outcome:     &auditpb.AuditEntry_Success{Success: &auditpb.AuditSuccess{MinLogSequence: first, MaxLogSequence: c.NextSeq - 1}}}
	b, err := protojson.Marshal(e)
	require.NoError(t, err)

	return b
}

func quietExecute(t *testing.T, c *servicetest.Client, key string, reqs []*servicepb.Request) oc.Outcome {
	t.Helper()
	bulk := oracle.Bulk{Requests: reqs, IdempotencyKey: key}
	res := c.Server.Apply(bulk)
	require.True(t, res.OK, res.Reason)
	resp := testutil.SimulateCommit(c.Server, bulk, c.NextSeq, 1_700_000_000_000_000)
	c.NextSeq += uint64(len(resp.GetLogs()))
	c.Server = res.State

	return testutil.OK(resp)
}

func protoItems[M proto.Message](t *testing.T, ms ...M) []json.RawMessage {
	t.Helper()
	out := make([]json.RawMessage, 0, len(ms))
	for _, m := range ms {
		b, err := protojson.Marshal(m)
		require.NoError(t, err)
		out = append(out, b)
	}

	return out
}

// The service end to end: the audit section replays the trail, the ledger
// section checks every part, the must-hit events are emitted, and a mutated
// part is reported with its own literal.
func TestQuietService_AuditAndLedgerSections(t *testing.T) {
	t.Parallel()
	s, c := newTestServer(t)
	c.Setup("run1")
	l := c.Ledger(1)
	var trail []json.RawMessage
	trail = append(trail, quietWrite(t, c, 1, testutil.CreateLedgerReq(l)))
	trail = append(trail, quietWrite(t, c, 2, testutil.TxReq(l, "r1", "world", "t-1:a", "COIN", 9)))
	trail = append(trail, quietWrite(t, c, 3, testutil.TxReq(l, "", "t-1:a", "t-1:b", "COIN", 4), testutil.AccountMetaReq(l, "t-1:b", "k0", "v")))

	c.OK("POST", "/quiet/begin", map[string]any{"epoch": 1})
	m := c.OK("POST", "/quiet/section", map[string]any{"epoch": 1, "section": "audit", "seq": 0,
		"body": qc.AuditBody{Entries: trail, Complete: true, Head: "3"}})
	okv, viol := servicetest.Verdict(m)
	require.True(t, okv, "%v", viol)
	require.Equal(t, map[string]any{"cursor": "3", "resync": false}, m["next"])

	model := s.tracker.Model()
	ls := model.Ledger(l)
	var logs []*commonpb.Log
	for _, row := range ls.LogRows() {
		lg, ok := matchtest.RenderLog(model, l, row.ID)
		require.True(t, ok)
		logs = append(logs, lg)
	}
	var txs []*commonpb.Transaction
	for id := ls.Txs().Len(); id >= 1; id-- { // served newest first: order-insensitive
		txs = append(txs, matchtest.RenderTx(model, l, uint64(id)))
	}
	var accts []*commonpb.Account
	for _, a := range match.AccountUniverse(ls) {
		accts = append(accts, matchtest.RenderAccount(model, l, a, false))
	}
	info := matchtest.RenderLedgerInfoWithIdentity(model, l, s.tracker.MatchLedgerIdentities()[l])
	parts := map[string][]json.RawMessage{
		"logs": protoItems(t, logs...), "transactions": protoItems(t, txs...), "accounts": protoItems(t, accts...),
		"stats": protoItems(t, matchtest.RenderStats(model, l)), "info": protoItems(t, info),
	}
	seq := 0
	post := func(scope, part string, items []json.RawMessage, final bool) map[string]any {
		seq++

		return c.OK("POST", "/quiet/section", map[string]any{"epoch": 1, "section": "ledger", "seq": seq,
			"body": qc.LedgerBody{Ledger: l, Scope: scope, Part: part, Items: items, Final: final}})
	}
	for _, part := range qc.LinearizableParts {
		items := parts[part]
		// The logs come in two chunks.
		if part == "logs" && len(items) > 1 {
			_ = post("linearizable", part, items[:1], false)
			items = items[1:]
		}
		okv, viol := servicetest.Verdict(post("linearizable", part, items, true))
		require.True(t, okv, "%s: %v", part, viol)
	}
	// A node serving a different ledger info.
	bad := proto.Clone(info).(*commonpb.LedgerInfo)
	bad.DefaultEnforcementMode = commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_STRICT
	if bad.DefaultEnforcementMode == info.DefaultEnforcementMode {
		bad.DefaultEnforcementMode = commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_AUDIT
	}
	_, viol = servicetest.Verdict(post("node:ledger-1:8888", "info", protoItems(t, bad), true))
	require.Equal(t, []string{string(qc.EvNodeInfoDiffers)}, viol)
	// Stats are not a per-node part.
	status, _ := c.Do("POST", "/quiet/section", map[string]any{"epoch": 1, "section": "ledger", "seq": 99,
		"body": qc.LedgerBody{Ledger: l, Scope: "node:ledger-1:8888", Part: "stats", Final: true}})
	require.Equal(t, 400, status)
	c.OK("POST", "/quiet/end", map[string]any{"epoch": 1})
	require.Equal(t, "3", s.tracker.AuditCursor())
}

// The task-11 fixtures decode into the section bodies and answers.
func TestQuietFixtures(t *testing.T) {
	t.Parallel()
	paths, err := filepath.Glob("../../../hegel/protocol-fixtures/11/*.json")
	require.NoError(t, err)
	require.NotEmpty(t, paths)
	for _, p := range paths {
		raw, err := os.ReadFile(p)
		require.NoError(t, err)
		var f struct {
			Endpoint string `json:"endpoint"`
			Request  struct {
				Epoch   uint64          `json:"epoch"`
				Section string          `json:"section"`
				Seq     uint64          `json:"seq"`
				Body    json.RawMessage `json:"body"`
			} `json:"request"`
			Status   int `json:"status"`
			Response struct {
				BootID  string          `json:"bootId"`
				Verdict json.RawMessage `json:"verdict"`
				Next    json.RawMessage `json:"next"`
			} `json:"response"`
		}
		require.NoError(t, json.Unmarshal(raw, &f), p)
		require.Equal(t, "POST /quiet/section", f.Endpoint)
		strict := func(b []byte, v any) {
			d := json.NewDecoder(bytes.NewReader(b))
			d.DisallowUnknownFields()
			require.NoError(t, d.Decode(v), p)
		}
		switch f.Request.Section {
		case qc.SectionAudit:
			var b qc.AuditBody
			strict(f.Request.Body, &b)
			for _, e := range b.Entries {
				require.NoError(t, protojson.Unmarshal(e, &auditpb.AuditEntry{}), p)
			}
			_, err := strconv.ParseUint(b.Head, 10, 64)
			require.NoError(t, err)
			var n qc.AuditNext
			strict(f.Response.Next, &n)
		case qc.SectionLedger:
			var b qc.LedgerBody
			strict(f.Request.Body, &b)
		default:
			t.Fatalf("%s: unknown section %s", p, f.Request.Section)
		}
	}
}

// The audit replay and the exact state check against a real in-process
// server (M/skippable_integration_test.go:187-214 pattern), so the matching
// does not rest on the oracle model playing the server: the entries' ledgers
// (order.GetLedgerScoped()), order counts, success ranges, failure and
// conflict entries all come from the server. Two quiet periods: the second
// pages from the cursor with the driver's filter.
func TestQuietIntegration_RealServer(t *testing.T) {
	if testing.Short() {
		t.Skip("in-process server")
	}
	ctx, client := singleNodeServer(t, "oracle-quiet")
	s, c := newTestServer(t)
	c.Setup("qint")
	l1, l2, l3 := c.Ledger(1), c.Ledger(2), c.Ledger(3)

	end := func(tk uint64, out oc.Outcome, attempts int) {
		t.Helper()
		m := c.OK("POST", "/write/end", map[string]any{"ticket": tk, "outcome": out, "attempts": attempts,
			"via": map[string]any{"opId": tk, "client": 0, "route": "lib", "late": false}})
		okv, viol := servicetest.Verdict(m)
		require.True(t, okv, "%v (outcome %+v)", viol, out)
	}
	write := func(trackKey bool, reqs ...*servicepb.Request) (map[string]any, oc.Outcome) {
		t.Helper()
		body := c.BeginWriteBody(trackKey, reqs...)
		tk := uint64(c.OK("POST", "/write/begin", body)["ticket"].(float64))
		out, err := applyUntilDefinitive(ctx, client, body["idempotencyKey"].(string), reqs)
		require.NoError(t, err)
		end(tk, out, 1)

		return body, out
	}
	// conflict sends a different body under orig's key attempts times.
	conflict := func(orig map[string]any, attempts int, reqs ...*servicepb.Request) {
		t.Helper()
		body := c.BeginWriteBody(true, reqs...)
		body["role"], body["of"], body["idempotencyKey"] = "conflict", orig["wid"], orig["idempotencyKey"]
		tk := uint64(c.OK("POST", "/write/begin", body)["ticket"].(float64))
		var out oc.Outcome
		for range attempts {
			var err error
			out, err = applyUntilDefinitive(ctx, client, orig["idempotencyKey"].(string), reqs)
			require.NoError(t, err)
			require.Equal(t, "IDEMPOTENCY_KEY_CONFLICT", out.Reason)
		}
		end(tk, out, attempts)
	}

	var cursor uint64
	epoch := uint64(0)
	quiet := func(lastKey string) {
		t.Helper()
		epoch++
		c.OK("POST", "/quiet/begin", map[string]any{"epoch": epoch})
		defer c.OK("POST", "/quiet/end", map[string]any{"epoch": epoch})

		// The driver's listing: AUDIT_FIELD_SEQUENCE >= cursor+1, linearizable,
		// waiting until the last write's entry is listed.
		minSeq := cursor + 1
		req := &servicepb.ListAuditEntriesRequest{Options: &commonpb.ListOptions{Filter: &commonpb.QueryFilter{
			Filter: &commonpb.QueryFilter_Audit{Audit: &commonpb.AuditCondition{Field: commonpb.AuditField_AUDIT_FIELD_SEQUENCE,
				Condition: &commonpb.AuditCondition_UintCond{UintCond: &commonpb.UintCondition{Min: &minSeq}}}}}}}
		var entries []*auditpb.AuditEntry
		require.Eventually(t, func() bool {
			var err error
			entries, err = actions.ListAuditEntriesWithRequest(ctx, client, req)
			if err != nil {
				return false
			}
			for _, e := range entries {
				if e.GetIdempotency().GetKey() == lastKey {
					return true
				}
			}

			return false
		}, 20*time.Second, 20*time.Millisecond)
		require.NotEmpty(t, entries)
		require.EqualValues(t, minSeq, entries[0].GetSequence())

		section, _ := oc.LookupQuietSection(qc.SectionAudit)
		var evs []oc.Event
		head := strconv.FormatUint(entries[len(entries)-1].GetSequence(), 10)
		for i, seq := 0, uint64(0); i < len(entries); seq++ {
			n := min(4, len(entries)-i) // small chunks: entries cross chunk boundaries
			body := qc.AuditBody{Complete: i+n == len(entries), Head: head}
			for _, e := range entries[i : i+n] {
				b, err := protojson.Marshal(e)
				require.NoError(t, err)
				body.Entries = append(body.Entries, b)
			}
			i += n
			raw, err := json.Marshal(body)
			require.NoError(t, err)
			s.mu.Lock()
			next, e, ref := section(s.tracker, epoch, seq, raw)
			s.mu.Unlock()
			require.Nil(t, ref)
			evs = append(evs, e...)
			require.False(t, next.(qc.AuditNext).Resync)
		}
		for _, e := range entries {
			t.Logf("audit %d key=%q ledgers=%v orders=%d success=%v failure=%v", e.GetSequence(), e.GetIdempotency().GetKey(),
				e.GetLedgers(), e.GetOrderCount(), e.GetSuccess() != nil, e.GetFailure().GetReason())
		}
		require.Empty(t, testutil.Findings(evs), "audit replay against the real trail")
		require.Contains(t, testutil.IDs(evs), qc.EvAuditReplayVerified)
		cursor = entries[len(entries)-1].GetSequence()
		require.Empty(t, s.tracker.HeldResponses())

		// The exact state of every live tracked ledger, read from the server.
		ledgerSection, _ := oc.LookupQuietSection(qc.SectionLedger)
		var lseq uint64
		post := func(l, part string, items []json.RawMessage) []oc.Event {
			raw, err := json.Marshal(qc.LedgerBody{Ledger: l, Scope: qc.ScopeLinearizable, Part: part, Items: items, Final: true})
			require.NoError(t, err)
			s.mu.Lock()
			defer s.mu.Unlock()
			_, e, ref := ledgerSection(s.tracker, epoch, lseq, raw)
			lseq++
			require.Nil(t, ref, "%s %s", l, part)

			return e
		}
		evs = nil
		for _, l := range s.tracker.Model().LiveLedgers() {
			if !strings.HasPrefix(l, c.Prefix) {
				continue
			}
			logs, err := actions.ListAllLogs(ctx, client, l)
			require.NoError(t, err)
			txs, err := actions.ListAllTransactions(ctx, client, l)
			require.NoError(t, err)
			accts, err := actions.ListAllAccounts(ctx, client, l)
			require.NoError(t, err)
			stats, err := actions.GetLedgerStats(ctx, client, l)
			require.NoError(t, err)
			info, err := actions.GetLedger(ctx, client, l)
			require.NoError(t, err)
			evs = append(evs, post(l, qc.PartLogs, protoItems(t, logs...))...)
			evs = append(evs, post(l, qc.PartTransactions, protoItems(t, txs...))...)
			evs = append(evs, post(l, qc.PartAccounts, protoItems(t, accts...))...)
			evs = append(evs, post(l, qc.PartStats, protoItems(t, stats))...)
			evs = append(evs, post(l, qc.PartInfo, protoItems(t, info))...)
			require.Contains(t, testutil.IDs(evs), qc.EvQuietExactStateVerified, l)
		}
		require.Empty(t, testutil.Findings(evs), "exact state against the real server")
	}

	// Epoch 1: ledger creates, single- and multi-ledger commits, a chart
	// order, FSM failures (keyed and tracked, keyed only), a conflict with
	// two attempts, a cluster-scoped query checkpoint create.
	write(false, testutil.CreateLedgerReq(l1))
	write(true, testutil.CreateLedgerReq(l2), testutil.CreateLedgerReq(l3))
	write(false, testutil.TxReq(l1, "", "world", "t-1:a", "COIN", 50))
	orig, _ := write(true, testutil.TxReq(l1, "", "world", "t-1:b", "COIN", 10), testutil.TxReq(l2, "", "world", "t-1:b", "COIN", 7),
		testutil.AccountMetaReq(l2, "t-1:b", "k0", "v0"))
	write(true, testutil.AddTypeReq(l2, "users", "users:{id}"))
	_, out := write(true, testutil.TxReq(l1, "", "t-1:z", "t-1:a", "COIN", 5))
	require.False(t, out.OK)
	_, out = write(false, testutil.TxReq(l2, "", "t-1:z", "t-1:a", "COIN", 5))
	require.False(t, out.OK)
	conflict(orig, 2, testutil.TxReq(l1, "", "world", "t-1:b", "COIN", 11))
	write(false, testutil.CreateCheckpointReq())
	last, _ := write(true, testutil.TxReq(l1, "", "t-1:a", "t-1:b", "COIN", 20))
	quiet(last["idempotencyKey"].(string))

	// Epoch 2: from the cursor, with a delete and a second conflict.
	write(true, testutil.TxReq(l3, "", "world", "t-1:c", "COIN", 3))
	write(false, testutil.DeleteLedgerReq(l3))
	conflict(orig, 1, testutil.TxReq(l1, "", "world", "t-1:b", "COIN", 12))
	last, _ = write(false, testutil.TxReq(l1, "", "t-1:b", "t-1:a", "COIN", 4))
	quiet(last["idempotencyKey"].(string))
}
