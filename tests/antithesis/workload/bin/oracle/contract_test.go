package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/bin/oracle/servicetest"
	oc "github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck/testutil"
)

// Contract cross-check (00d step 3): a scripted session through the real
// service, shaped like the driver's calls, records every request and response
// in the wave-0 fixture format ({"endpoint","request","status","response"}).
// The Rust test `oracle::contract` deserializes the same files with the
// driver's structs:
//
//	ORACLE_CONTRACT_DIR=<dir> cargo test --locked contract -- --nocapture
//
// With ORACLE_CONTRACT_DIR set, the Go test writes there; otherwise into a
// fresh temporary directory whose path (and the command) it prints.

const contractDirEnv = "ORACLE_CONTRACT_DIR"

type recorder struct {
	h   http.Handler
	dir string
	n   int
	t   testing.TB
}

func (r *recorder) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	body, err := io.ReadAll(req.Body)
	require.NoError(r.t, err)
	req.Body = io.NopCloser(bytes.NewReader(body))
	rec := httptest.NewRecorder()
	r.h.ServeHTTP(rec, req)

	endpoint := req.Method + " " + req.URL.Path
	if req.URL.RawQuery != "" {
		endpoint += "?" + req.URL.RawQuery
	}
	entry := map[string]any{"endpoint": endpoint, "status": rec.Code, "response": json.RawMessage(rec.Body.Bytes())}
	if len(bytes.TrimSpace(body)) > 0 {
		entry["request"] = json.RawMessage(body)
	} else {
		entry["request"] = nil
	}
	b, err := json.MarshalIndent(entry, "", "  ")
	require.NoError(r.t, err)
	r.n++
	require.NoError(r.t, os.WriteFile(filepath.Join(r.dir, fmt.Sprintf("%04d.json", r.n)), b, 0o644))

	for k, v := range rec.Header() {
		w.Header()[k] = v
	}
	w.WriteHeader(rec.Code)
	_, _ = w.Write(rec.Body.Bytes())
}

func contractDir(t *testing.T) string {
	t.Helper()
	if d := os.Getenv(contractDirEnv); d != "" {
		require.NoError(t, os.MkdirAll(d, 0o755))
		old, err := filepath.Glob(filepath.Join(d, "*.json"))
		require.NoError(t, err)
		for _, p := range old {
			require.NoError(t, os.Remove(p))
		}

		return d
	}
	d, err := os.MkdirTemp("", "oracle-contract-")
	require.NoError(t, err)

	return d
}

// via is what the driver sends with every write end.
func ctrVia(op uint64, late bool) map[string]any {
	return map[string]any{"opId": op, "client": 1, "route": "plain", "late": late}
}

func readRoute() map[string]any {
	return map[string]any{"client": 2, "route": "lib", "node": "", "consistency": "linearizable"}
}

func TestContract_DriverStructs(t *testing.T) {
	dir := contractDir(t)
	s, err := newServer(options{BootID: testBoot, QueryCheckpointLimit: "10", Out: &bytes.Buffer{}})
	require.NoError(t, err)
	rec := &recorder{h: s, dir: dir, t: t}
	c := servicetest.New(t, rec)

	status, _ := c.Do("GET", "/health", nil)
	require.Equal(t, http.StatusOK, status)
	c.Setup("ctr")
	c.Setup("ctr") // idempotent repeat
	c.OK("GET", "/health", nil)
	l1, l2 := c.Ledger(1), c.Ledger(2)

	endWrite := func(tk uint64, out oc.Outcome, attempts int) map[string]any {
		return c.OK("POST", "/write/end", map[string]any{"ticket": tk, "outcome": out, "attempts": attempts, "via": ctrVia(tk, false)})
	}
	write := func(trackKey bool, reqs ...*servicepb.Request) (uint64, oc.Outcome, map[string]any) {
		tk := c.BeginWrite(trackKey, reqs...)
		out := c.Execute(tk)

		return tk, out, endWrite(tk, out, 1)
	}

	// Ledgers, as setup creates them.
	write(false, testutil.CreateLedgerReq(l1))
	write(false, testutil.CreateLedgerReq(l2))

	// A flagged success (pending), a flagged definitive failure (validated),
	// a mixed bulk, a revert, metadata.
	tkA, outA, m := write(true, testutil.TxReq(l1, c.Prefix+"ref1", "world", "t-1:1", "COIN", 100))
	require.Equal(t, "pending", m["status"])
	_, _, m = write(true, testutil.TxReq(l1, "", "t-1:2", "t-1:1", "COIN", 5))
	require.Equal(t, "validated", m["status"])
	write(false, testutil.TxReq(l2, "", "world", "users:1", "USD/2", 7), testutil.AccountMetaReq(l2, "users:1", "k0", "v"))
	write(true, testutil.RevertReq(l1, 1))

	// A conflict under the original's key (role/of).
	orig := c.BeginWriteBody(true, testutil.TxReq(l2, "", "world", "t-2:1", "COIN", 1))
	om := c.OK("POST", "/write/begin", orig)
	conflict := c.BeginWriteBody(true, testutil.TxReq(l2, "", "world", "t-2:1", "COIN", 2))
	conflict["role"], conflict["of"], conflict["idempotencyKey"] = "conflict", orig["wid"], orig["idempotencyKey"]
	cm := c.OK("POST", "/write/begin", conflict)
	c.OK("POST", "/write/abandon", map[string]any{"ticket": cm["ticket"], "attempts": 0})
	c.OK("POST", "/write/abandon", map[string]any{"ticket": om["ticket"], "attempts": 0})

	// A late outcome for an ended ticket, and an idempotent repeat of an end.
	c.OK("POST", "/write/late", map[string]any{"ticket": tkA, "outcome": outA, "via": ctrVia(tkA+1000, true)})
	endWrite(tkA, outA, 1)

	// Views: every core view, with ledgerPick and ledger.
	views := []map[string]any{
		{"name": "ledgers"},
		{"name": "ledgers", "owned": true},
		{"name": "ledger", "ledgerPick": 3141592},
		{"name": "ledger", "ledger": l1},
		{"name": "seek", "ledgerPick": 1, "table": "volumes", "probe": "t-1:0"},
		{"name": "seek", "ledger": l1, "table": "accountMeta", "probe": "users:0"},
		{"name": "seek", "ledger": l2, "table": "accountMeta", "probe": "users:0"},
		{"name": "seek", "ledger": l1, "table": "txByRef", "probe": "o-"},
		{"name": "seek", "ledger": l1, "table": "txMeta", "probe": "0"},
		{"name": "seek", "ledger": l1, "table": "txs", "probe": "0"},
		{"name": "seek", "ledger": l1, "table": "volumes", "probe": "t-1:0", "filter": "has-metadata"},
		{"name": "replayable"},
		{"name": "stamps", "ledgerPick": 0},
	}
	anyViews := make([]any, 0, len(views))
	for _, v := range views {
		anyViews = append(anyViews, v)
	}
	c.OK("POST", "/view", map[string]any{"views": anyViews})

	// Reads: validated, transient (skipped), abandoned, repeated begin.
	rb := c.OK("POST", "/read/begin", map[string]any{"rid": "r1", "kind": "Smoke", "views": anyViews, "params": map[string]any{}})
	c.OK("POST", "/read/begin", map[string]any{"rid": "r1", "kind": "Smoke", "views": anyViews, "params": map[string]any{}})
	readEnd := func(tk any, out oc.Outcome) map[string]any {
		return c.OK("POST", "/read/end", map[string]any{"ticket": tk, "kind": "Smoke", "request": smokeRequest(l1, "t-1:1"),
			"outcome": out, "route": readRoute(), "hint": map[string]any{"collapsed": false}, "params": map[string]any{}})
	}
	m = readEnd(rb["ticket"], oc.Outcome{OK: true, Response: renderAccount(c, l1, "t-1:1"), Peer: "ledger-0:8888", ElapsedMs: 3})
	okv, viol := servicetest.Verdict(m)
	require.True(t, okv, "%v", viol)
	rb = c.OK("POST", "/read/begin", map[string]any{"rid": "r2", "kind": "Smoke", "views": []any{}, "params": map[string]any{}})
	readEnd(rb["ticket"], oc.Outcome{Unknown: "timeout"})
	rb = c.OK("POST", "/read/begin", map[string]any{"rid": "r3", "kind": "Smoke", "views": []any{map[string]any{"name": "ledger", "ledgerPick": 0}}, "params": map[string]any{}})
	c.OK("GET", "/tickets", nil)
	c.OK("GET", "/quiet/status", nil)
	c.OK("POST", "/read/abandon", map[string]any{"ticket": rb["ticket"]})

	// Error codes the driver maps.
	expectErr := func(method, path string, body any, wantStatus int, wantCode string) {
		status, m := c.Do(method, path, body)
		require.Equal(t, wantStatus, status, "%s %s: %v", method, path, m)
		require.Equal(t, wantCode, errCode(m))
	}
	expectErr("POST", "/setup", map[string]any{"runId": "other", "trackedPrefix": "o-other-", "queryCheckpointLimit": "10", "liveTarget": 4},
		http.StatusConflict, oc.CodeSetup)
	expectErr("POST", "/write/begin", c.BeginWriteBody(false, testutil.EmptyTxReq("r-ns-x-1")), http.StatusBadRequest, oc.CodeUnmodeled)
	expectErr("POST", "/write/end", map[string]any{"ticket": 9999, "outcome": testutil.Fail("NotFound", "LEDGER_NOT_FOUND"), "attempts": 1, "via": ctrVia(1, false)},
		http.StatusNotFound, oc.CodeUnknownTicket)
	held := c.BeginWrite(false, testutil.TxReq(l1, "", "world", "t-1:3", "COIN", 1))
	expectErr("POST", "/write/end", map[string]any{"ticket": held, "outcome": testutil.Fail("Unavailable", "STALE_PROPOSAL"), "attempts": 1, "via": ctrVia(held, false)},
		http.StatusBadRequest, oc.CodeProtocol)
	expectErr("POST", "/write/abandon", map[string]any{"ticket": held, "attempts": 1}, http.StatusBadRequest, oc.CodeProtocol)
	expectErr("POST", "/quiet/begin", map[string]any{"epoch": 1}, http.StatusConflict, oc.CodeNotQuiet)
	busy := []uint64{held}
	for range 7 {
		busy = append(busy, c.BeginWrite(false, testutil.TxReq(l1, "", "world", "t-1:3", "COIN", 1)))
	}
	expectErr("POST", "/write/begin", c.BeginWriteBody(false, testutil.TxReq(l1, "", "world", "t-1:3", "COIN", 1)), http.StatusConflict, oc.CodeBusy)
	for _, tk := range busy {
		c.OK("POST", "/write/abandon", map[string]any{"ticket": tk, "attempts": 0})
	}

	// A quiet period: begin, an unknown section (refused), end; begin during
	// quiet is a protocol error.
	c.OK("POST", "/quiet/begin", map[string]any{"epoch": 1})
	c.OK("POST", "/quiet/begin", map[string]any{"epoch": 1})
	c.Do("POST", "/quiet/section", map[string]any{"epoch": 1, "section": "no-such-section", "seq": 0, "body": map[string]any{}})
	expectErr("POST", "/write/begin", c.BeginWriteBody(false, testutil.TxReq(l1, "", "world", "t-1:3", "COIN", 1)), http.StatusBadRequest, oc.CodeProtocol)
	c.OK("POST", "/quiet/end", map[string]any{"epoch": 1})
	c.OK("POST", "/quiet/begin", map[string]any{"epoch": 2})
	c.OK("POST", "/quiet/end", map[string]any{"epoch": 2})

	c.OK("GET", "/dump", nil)
	c.OK("GET", "/journal?from=1&limit=5", nil)

	require.Greater(t, rec.n, 50)
	t.Logf("wrote %d request/response files to %s; cross-check with:\n  cd tests/antithesis/hegel/driver && %s=%s cargo test --locked contract -- --nocapture",
		rec.n, dir, contractDirEnv, dir)
}
