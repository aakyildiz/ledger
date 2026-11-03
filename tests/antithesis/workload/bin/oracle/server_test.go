package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/bin/oracle/servicetest"
	oc "github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck/pointcheck"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck/testutil"
	"github.com/formancehq/ledger/v3/tests/oracle"
)

const testBoot = "1793836800123456789"

func newTestServer(t testing.TB) (*server, *servicetest.Client) {
	t.Helper()
	s, err := newServer(options{BootID: testBoot, QueryCheckpointLimit: "10", Out: &bytes.Buffer{}})
	require.NoError(t, err)

	return s, servicetest.New(t, s)
}

// setupWithLedger runs /setup and creates tracked ledger l1.
func setupWithLedger(t testing.TB) (*server, *servicetest.Client) {
	t.Helper()
	s, c := newTestServer(t)
	c.Setup("run1")
	m := c.Write(false, testutil.CreateLedgerReq(c.Ledger(1)))
	okv, viol := servicetest.Verdict(m)
	require.True(t, okv, "%v", viol)

	return s, c
}

func errCode(m map[string]any) string {
	e, _ := m["error"].(map[string]any)
	code, _ := e["code"].(string)

	return code
}

// renderAccount renders the GetAccount answer of a correct server from the
// fake server state.
func renderAccount(c *servicetest.Client, ledger, addr string) json.RawMessage {
	ls := c.Server.Ledger(ledger)
	acct := &commonpb.Account{Address: addr, Metadata: ls.AccountMetadata(addr)}
	for k, vp := range ls.Volumes().From(oracle.VolumeKey{Address: addr}) {
		if k.Address != addr {
			break
		}
		acct.Volumes = append(acct.Volumes, &commonpb.AccountVolume{Asset: k.Asset, Color: k.Color, Volumes: &commonpb.VolumesWithBalance{
			Input: commonpb.NewBigUintFromUint256(&vp.Input), Output: commonpb.NewBigUintFromUint256(&vp.Output),
		}})
	}
	b, err := protojson.Marshal(acct)
	if err != nil {
		panic(err)
	}

	return b
}

func smokeRequest(ledger, addr string) map[string]any {
	return map[string]any{"ledger": ledger, "address": addr}
}

func TestServer_FullFlows(t *testing.T) {
	t.Parallel()
	_, c := setupWithLedger(t)
	l1 := c.Ledger(1)

	m := c.Write(true, testutil.TxReq(l1, c.Prefix+"ref1", "world", "t-1:1", "COIN", 100))
	require.Equal(t, "pending", m["status"])
	require.Equal(t, testBoot, m["bootId"])

	// A failure explained by the model.
	m = c.Write(true, testutil.TxReq(l1, "", "t-1:2", "t-1:1", "COIN", 5))
	require.Equal(t, "validated", m["status"])
	okv, viol := servicetest.Verdict(m)
	require.True(t, okv, "%v", viol)

	// A GetAccount read that matches the model, then one that does not.
	rb := c.BeginRead("GetAccount", map[string]any{"name": "seek", "ledgerPick": 0, "table": "volumes", "probe": "t-1:0"})
	views := rb["views"].([]any)
	require.Equal(t, l1, views[0].(map[string]any)["ledger"])
	tk := uint64(rb["ticket"].(float64))
	m = c.EndRead(tk, "GetAccount", smokeRequest(l1, "t-1:1"), oc.Outcome{OK: true, Response: renderAccount(c, l1, "t-1:1")})
	okv, viol = servicetest.Verdict(m)
	require.True(t, okv, "%v", viol)
	// (task 05's GetAccount kind reports coverage only for collapsed or
	// absent-probe reads; the smoke kind it replaces reported "model outcome
	// verified", which drained writes still report.)

	rb = c.BeginRead("GetAccount")
	tk = uint64(rb["ticket"].(float64))
	m = c.EndRead(tk, "GetAccount", smokeRequest(l1, "t-1:1"), oc.Outcome{OK: true, Response: renderAccount(c, l1, "t-1:2")})
	okv, viol = servicetest.Verdict(m)
	require.False(t, okv)
	require.Equal(t, []string{string(pointcheck.EvAccountReadOutsideModel)}, viol)

	// A transient read is skipped.
	rb = c.BeginRead("GetAccount")
	m = c.EndRead(uint64(rb["ticket"].(float64)), "GetAccount", smokeRequest(l1, "t-1:1"), oc.Outcome{Unknown: "timeout"})
	require.Equal(t, true, m["verdict"].(map[string]any)["skipped"])

	// The replayable view lists the committed flagged bulk.
	v := c.OK("POST", "/view", map[string]any{"views": []any{map[string]any{"name": "replayable"}}})
	entries := v["views"].([]any)[0].(map[string]any)["entries"].([]any)
	require.Len(t, entries, 2) // the commit and the frozen failure
	require.Equal(t, "committed", entries[0].(map[string]any)["status"])
	require.Equal(t, "failed", entries[1].(map[string]any)["status"])

	st := c.OK("GET", "/quiet/status", nil)
	require.EqualValues(t, 0, st["inflight"])
	d := c.OK("GET", "/dump", nil)
	require.EqualValues(t, 2, d["version"])
	require.Len(t, d["fingerprint"], 32)
}

func TestServer_ErrorCodes(t *testing.T) {
	t.Parallel()
	_, c := setupWithLedger(t)
	l1 := c.Ledger(1)

	// setup
	status, m := c.Do("POST", "/setup", map[string]any{"runId": "other", "trackedPrefix": "o-other-", "queryCheckpointLimit": "10", "liveTarget": 4})
	require.Equal(t, http.StatusConflict, status)
	require.Equal(t, oc.CodeSetup, errCode(m))
	require.Equal(t, testBoot, m["bootId"])

	// unmodeled
	body := c.BeginWriteBody(false, &servicepb.Request{Type: &servicepb.Request_SaveNumscript{SaveNumscript: &servicepb.SaveNumscriptRequest{}}})
	status, m = c.Do("POST", "/write/begin", body)
	require.Equal(t, http.StatusBadRequest, status)
	require.Equal(t, oc.CodeUnmodeled, errCode(m))

	// unknown_ticket
	status, m = c.Do("POST", "/write/end", map[string]any{"ticket": 9999, "outcome": map[string]any{"ok": false, "code": "NotFound", "reason": "LEDGER_NOT_FOUND"}, "attempts": 1, "via": map[string]any{}})
	require.Equal(t, http.StatusNotFound, status)
	require.Equal(t, oc.CodeUnknownTicket, errCode(m))
	status, m = c.Do("POST", "/read/end", map[string]any{"ticket": 9999, "kind": "GetAccount", "outcome": map[string]any{"unknown": "timeout"}})
	require.Equal(t, http.StatusNotFound, status)
	require.Equal(t, oc.CodeUnknownTicket, errCode(m))

	// protocol: non-definitive end, abandon after attempts, unknown kind
	tk := c.BeginWrite(false, testutil.TxReq(l1, "", "world", "t-1:1", "COIN", 1))
	status, m = c.Do("POST", "/write/end", map[string]any{"ticket": tk, "outcome": map[string]any{"ok": false, "code": "Unavailable", "reason": "STALE_PROPOSAL"}, "attempts": 1, "via": map[string]any{}})
	require.Equal(t, http.StatusBadRequest, status)
	require.Equal(t, oc.CodeProtocol, errCode(m))
	status, m = c.Do("POST", "/write/abandon", map[string]any{"ticket": tk, "attempts": 2})
	require.Equal(t, http.StatusBadRequest, status)
	require.Equal(t, oc.CodeProtocol, errCode(m))
	c.OK("POST", "/write/abandon", map[string]any{"ticket": tk, "attempts": 0})
	status, m = c.Do("POST", "/read/begin", map[string]any{"rid": "rx", "kind": "NoSuchKind", "views": []any{}})
	require.Equal(t, http.StatusBadRequest, status)
	require.Equal(t, oc.CodeProtocol, errCode(m))

	// protocol: conflict with trackKey false, conflict equal to the original
	orig := c.BeginWriteBody(true, testutil.TxReq(l1, "", "world", "t-1:1", "COIN", 1))
	c.OK("POST", "/write/begin", orig)
	conflict := c.BeginWriteBody(false, testutil.TxReq(l1, "", "world", "t-1:1", "COIN", 2))
	conflict["role"], conflict["of"], conflict["idempotencyKey"] = "conflict", orig["wid"], orig["idempotencyKey"]
	status, m = c.Do("POST", "/write/begin", conflict)
	require.Equal(t, http.StatusBadRequest, status)
	require.Equal(t, oc.CodeProtocol, errCode(m))
	conflict["trackKey"] = true
	conflict["requests"] = orig["requests"]
	status, m = c.Do("POST", "/write/begin", conflict)
	require.Equal(t, http.StatusBadRequest, status)
	require.Equal(t, oc.CodeProtocol, errCode(m))

	// busy: 8 in flight (orig + 7)
	for range 7 {
		c.BeginWrite(false, testutil.TxReq(l1, "", "world", "t-1:1", "COIN", 1))
	}
	status, m = c.Do("POST", "/write/begin", c.BeginWriteBody(false, testutil.TxReq(l1, "", "world", "t-1:1", "COIN", 1)))
	require.Equal(t, http.StatusConflict, status)
	require.Equal(t, oc.CodeBusy, errCode(m))

	// not_quiet with tickets outstanding
	status, m = c.Do("POST", "/quiet/begin", map[string]any{"epoch": 1})
	require.Equal(t, http.StatusConflict, status)
	require.Equal(t, oc.CodeNotQuiet, errCode(m))
}

func TestServer_Backlog(t *testing.T) {
	t.Parallel()
	s, c := setupWithLedger(t)
	s.tracker = oc.NewTracker(oc.Config{TrackedPrefix: c.Prefix, PendingCap: 1})
	c.Server = oracle.NewGlobalState().WithQueryCheckpointLimit(10)
	c.Write(false, testutil.CreateLedgerReq(c.Ledger(1)))
	l1 := c.Ledger(1)
	hold := c.BeginWrite(false, testutil.TxReq(l1, "", "world", "t-1:1", "COIN", 1))
	for range 2 {
		c.Write(false, testutil.TxReq(l1, "", "world", "t-1:1", "COIN", 1))
	}
	status, m := c.Do("POST", "/write/begin", c.BeginWriteBody(false, testutil.TxReq(l1, "", "world", "t-1:1", "COIN", 1)))
	require.Equal(t, http.StatusConflict, status)
	require.Equal(t, oc.CodeBacklog, errCode(m))
	c.EndWrite(hold, c.Execute(hold))
}

func TestServer_SerializedAndPanic(t *testing.T) {
	oc.RegisterBeginHook("zz-test-serialize", func(_ *oc.Tracker, rec oc.WriteRecord) (map[string]any, *oc.Refusal) {
		if rec.Family == "refuse" {
			return nil, oc.Serialized("refused by a test hook", map[string]any{"hook": "zz-test-serialize"})
		}

		return map[string]any{"checkpointId": "7"}, nil
	})
	oc.RegisterReadKind(oc.ReadKind{Name: "zz-test-panic", Validate: func(*oc.ReadCtx) []oc.Event { panic("boom") }})
	t.Cleanup(func() { oc.TestingUnregister("zz-test-serialize", "zz-test-panic") })

	_, c := setupWithLedger(t)
	body := c.BeginWriteBody(false, testutil.CreateCheckpointReq())
	m := c.OK("POST", "/write/begin", body)
	require.Equal(t, "7", m["predicted"].(map[string]any)["checkpointId"])

	body = c.BeginWriteBody(false, testutil.CreateCheckpointReq())
	body["family"] = "refuse"
	status, m := c.Do("POST", "/write/begin", body)
	require.Equal(t, http.StatusConflict, status)
	require.Equal(t, oc.CodeSerialized, errCode(m))

	rb := c.BeginRead("zz-test-panic")
	status, m = c.Do("POST", "/read/end", map[string]any{"ticket": rb["ticket"], "kind": "zz-test-panic", "request": map[string]any{}, "outcome": map[string]any{"ok": true}})
	require.Equal(t, http.StatusInternalServerError, status)
	require.Equal(t, oc.CodePanic, errCode(m))
	// The service keeps serving.
	c.OK("GET", "/health", nil)
}

func TestServer_IdempotentRepeats(t *testing.T) {
	t.Parallel()
	_, c := setupWithLedger(t)
	l1 := c.Ledger(1)

	body := c.BeginWriteBody(true, testutil.TxReq(l1, "", "world", "t-1:1", "COIN", 1))
	s1, r1 := c.DoRaw("POST", "/write/begin", body)
	s2, r2 := c.DoRaw("POST", "/write/begin", body)
	require.Equal(t, http.StatusOK, s1)
	require.Equal(t, s1, s2)
	require.Equal(t, r1, r2)
	_, inflight := c.Do("GET", "/tickets", nil)
	require.Len(t, inflight["inflight"], 1, "a repeated wid registers once")

	var begin map[string]any
	require.NoError(t, json.Unmarshal(r1, &begin))
	tk := uint64(begin["ticket"].(float64))
	bulk := oracle.Bulk{Requests: []*servicepb.Request{testutil.TxReq(l1, "", "world", "t-1:1", "COIN", 1)}, IdempotencyKey: body["idempotencyKey"].(string)}
	resp := testutil.SimulateCommit(c.Server, bulk, 100, 0)
	end := map[string]any{"ticket": tk, "outcome": testutil.OK(resp), "attempts": 2, "via": map[string]any{"opId": 1, "client": 0, "route": "lib", "late": false}}
	_, e1 := c.DoRaw("POST", "/write/end", end)
	_, e2 := c.DoRaw("POST", "/write/end", end)
	require.Equal(t, e1, e2)

	rid := map[string]any{"rid": "r77", "kind": "GetAccount", "views": []any{map[string]any{"name": "ledgers"}}, "params": map[string]any{}}
	_, b1 := c.DoRaw("POST", "/read/begin", rid)
	_, b2 := c.DoRaw("POST", "/read/begin", rid)
	require.Equal(t, b1, b2)
	var rb map[string]any
	require.NoError(t, json.Unmarshal(b1, &rb))
	ab := map[string]any{"ticket": rb["ticket"]}
	_, a1 := c.DoRaw("POST", "/read/abandon", ab)
	_, a2 := c.DoRaw("POST", "/read/abandon", ab)
	require.Equal(t, a1, a2)
}

func TestServer_BootIDOnEveryAnswer(t *testing.T) {
	t.Parallel()
	_, c := newTestServer(t)
	for _, ep := range [][2]string{{"GET", "/health"}, {"GET", "/tickets"}, {"POST", "/view"}} {
		_, m := c.Do(ep[0], ep[1], map[string]any{})
		require.Equal(t, testBoot, m["bootId"], ep)
	}
	c.Setup("run1")
	for _, ep := range [][2]string{{"GET", "/tickets"}, {"GET", "/dump"}, {"GET", "/journal?from=0&limit=5"}, {"GET", "/quiet/status"}, {"POST", "/nope"}} {
		_, m := c.Do(ep[0], ep[1], nil)
		require.Equal(t, testBoot, m["bootId"], ep)
	}
}

func TestServer_QuietPeriods(t *testing.T) {
	t.Parallel()
	_, c := setupWithLedger(t)

	// Maintenance on in the drained model refuses quiet.
	c.Write(false, testutil.MaintenanceReq(true))
	status, m := c.Do("POST", "/quiet/begin", map[string]any{"epoch": 1})
	require.Equal(t, http.StatusConflict, status)
	require.Equal(t, oc.CodeNotQuiet, errCode(m))
	require.Equal(t, "maintenance", m["error"].(map[string]any)["details"].(map[string]any)["reason"])
	c.Write(false, testutil.MaintenanceReq(false))

	qb := c.OK("POST", "/quiet/begin", map[string]any{"epoch": 1})
	require.Equal(t, "0", qb["auditCursor"])
	require.Equal(t, []any{c.Ledger(1)}, qb["ledgers"].(map[string]any)["live"])
	// Writes are refused during quiet.
	status, m = c.Do("POST", "/write/begin", c.BeginWriteBody(false, testutil.TxReq(c.Ledger(1), "", "world", "a:1", "COIN", 1)))
	require.Equal(t, http.StatusBadRequest, status)
	require.Equal(t, oc.CodeProtocol, errCode(m))
	// Unknown section.
	status, m = c.Do("POST", "/quiet/section", map[string]any{"epoch": 1, "section": "nope", "seq": 0, "body": map[string]any{}})
	require.Equal(t, http.StatusBadRequest, status)
	require.Equal(t, oc.CodeProtocol, errCode(m))

	// A new epoch implicitly ends the stale one.
	c.OK("POST", "/quiet/begin", map[string]any{"epoch": 2})
	_, open := c.Do("GET", "/quiet/status", nil)
	require.Equal(t, true, open["quiet"])
	end := c.OK("POST", "/quiet/end", map[string]any{"epoch": 2})
	require.EqualValues(t, 0, end["summary"].(map[string]any)["violations"])
	_, st := c.Do("GET", "/quiet/status", nil)
	require.Equal(t, false, st["quiet"])
	c.BeginWrite(false, testutil.TxReq(c.Ledger(1), "", "world", "a:1", "COIN", 1))
}

func TestServer_SetupLimitMismatch(t *testing.T) {
	t.Parallel()
	s, err := newServer(options{BootID: testBoot, QueryCheckpointLimit: "12", Out: &bytes.Buffer{}})
	require.NoError(t, err)
	c := servicetest.New(t, s)
	status, m := c.Do("POST", "/setup", map[string]any{"runId": "r", "trackedPrefix": "o-r-", "queryCheckpointLimit": "10", "liveTarget": 4})
	require.Equal(t, http.StatusConflict, status)
	require.Equal(t, oc.CodeSetup, errCode(m))
}

func TestServer_JournalDumpOnFirstFinding(t *testing.T) {
	t.Parallel()
	out := &bytes.Buffer{}
	s, err := newServer(options{BootID: testBoot, Out: out, DumpBatches: true})
	require.NoError(t, err)
	c := servicetest.New(t, s)
	c.Setup("run1")
	c.Write(false, testutil.CreateLedgerReq(c.Ledger(1)))
	require.Contains(t, out.String(), "[batch-dump] ticket=1 seq=1 outcome=OK b64=")
	require.NotContains(t, out.String(), "[journal]")
	tk := c.BeginWrite(false, testutil.TxReq(c.Ledger(1), "", "world", "a:1", "COIN", 1))
	m := c.EndWrite(tk, testutil.Fail("FailedPrecondition", "INSUFFICIENT_FUNDS"))
	okv, _ := servicetest.Verdict(m)
	require.False(t, okv)
	require.Contains(t, out.String(), "[journal] ")
	require.Contains(t, out.String(), "outcome=INSUFFICIENT_FUNDS")

	j := c.OK("GET", "/journal?from=1&limit=2", nil)
	require.Len(t, j["entries"], 2)
	require.EqualValues(t, 3, j["next"])
}
