// Package servicetest is a scripted protocol client for the oracle service,
// for family service tests in package main (h_<fam>_test.go). The service
// itself lives in package main, so tests construct it there and hand its
// http.Handler to New:
//
//	c := servicetest.New(t, newTestHandler(t))
//	c.Setup("run1")
//	tk := c.BeginWrite("w1", c.Key(1), true, reqs...)
//	c.EndWrite(tk, outcome)
package servicetest

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	oc "github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck/testutil"
	"github.com/formancehq/ledger/v3/tests/oracle"
)

// Client drives one in-process service.
type Client struct {
	T      testing.TB
	H      http.Handler
	RunID  string
	Prefix string

	// Server is a fake, model-faithful server state used by Execute.
	Server  oracle.GlobalState
	NextSeq uint64

	reqs     map[uint64][]*servicepb.Request
	keys     map[uint64]string
	wids     int
	rids     int
	keyCount int
}

// New returns a client for h.
func New(t testing.TB, h http.Handler) *Client {
	return &Client{T: t, H: h, Server: oracle.NewGlobalState().WithQueryCheckpointLimit(10), NextSeq: 1,
		reqs: map[uint64][]*servicepb.Request{}, keys: map[uint64]string{}}
}

// Do sends one request and decodes the answer.
func (c *Client) Do(method, path string, body any) (int, map[string]any) {
	c.T.Helper()
	status, raw := c.DoRaw(method, path, body)
	var m map[string]any
	require.NoError(c.T, json.Unmarshal(raw, &m), "response: %s", raw)

	return status, m
}

// DoRaw sends one request and returns the raw answer.
func (c *Client) DoRaw(method, path string, body any) (int, []byte) {
	c.T.Helper()
	var rd *bytes.Reader
	switch b := body.(type) {
	case nil:
		rd = bytes.NewReader(nil)
	case []byte:
		rd = bytes.NewReader(b)
	case string:
		rd = bytes.NewReader([]byte(b))
	default:
		enc, err := json.Marshal(b)
		require.NoError(c.T, err)
		rd = bytes.NewReader(enc)
	}
	req := httptest.NewRequest(method, path, rd)
	w := httptest.NewRecorder()
	c.H.ServeHTTP(w, req)

	return w.Code, w.Body.Bytes()
}

// OK sends a request that must answer 200.
func (c *Client) OK(method, path string, body any) map[string]any {
	c.T.Helper()
	status, m := c.Do(method, path, body)
	require.Equal(c.T, http.StatusOK, status, "%s %s: %v", method, path, m)

	return m
}

// Setup runs /setup with limit 10.
func (c *Client) Setup(runID string) map[string]any {
	c.T.Helper()
	c.RunID = runID
	c.Prefix = "o-" + runID + "-"

	return c.OK("POST", "/setup", map[string]any{"runId": runID, "trackedPrefix": c.Prefix, "queryCheckpointLimit": "10", "liveTarget": 4})
}

// Ledger names tracked ledger n.
func (c *Client) Ledger(n int) string { return fmt.Sprintf("%sl%d", c.Prefix, n) }

// Key returns a fresh tracked idempotency key.
func (c *Client) Key() string {
	c.keyCount++

	return fmt.Sprintf("%sk%d", c.Prefix, c.keyCount)
}

// RequestsJSON renders requests as protojson values.
func RequestsJSON(reqs ...*servicepb.Request) []json.RawMessage {
	out := make([]json.RawMessage, 0, len(reqs))
	for _, r := range reqs {
		b, err := protojson.Marshal(r)
		if err != nil {
			panic(err)
		}
		out = append(out, b)
	}

	return out
}

// BeginWriteBody builds a /write/begin body with a fresh wid and key.
func (c *Client) BeginWriteBody(trackKey bool, reqs ...*servicepb.Request) map[string]any {
	c.wids++

	return map[string]any{"wid": fmt.Sprintf("w%d", c.wids), "idempotencyKey": c.Key(), "requests": RequestsJSON(reqs...),
		"trackKey": trackKey, "role": "original", "family": "test", "signed": false, "hint": map[string]any{}}
}

// BeginWrite begins a write and returns its ticket (must succeed).
func (c *Client) BeginWrite(trackKey bool, reqs ...*servicepb.Request) uint64 {
	c.T.Helper()
	body := c.BeginWriteBody(trackKey, reqs...)
	m := c.OK("POST", "/write/begin", body)
	tk := uint64(m["ticket"].(float64))
	c.reqs[tk] = reqs
	c.keys[tk] = body["idempotencyKey"].(string)

	return tk
}

// Execute runs a begun write on the fake server and returns the outcome a
// correct server answers.
func (c *Client) Execute(ticket uint64) oc.Outcome {
	bulk := oracle.Bulk{Requests: c.reqs[ticket], IdempotencyKey: c.keys[ticket]}
	res := c.Server.Apply(bulk)
	if !res.OK {
		return testutil.Fail("FailedPrecondition", res.Reason)
	}
	resp := testutil.SimulateCommit(c.Server, bulk, c.NextSeq, 1_700_000_000_000_000)
	c.NextSeq += uint64(len(resp.GetLogs()))
	c.Server = res.State

	return testutil.OK(resp)
}

// EndWrite posts an outcome and returns the answer.
func (c *Client) EndWrite(ticket uint64, out oc.Outcome) map[string]any {
	c.T.Helper()

	return c.OK("POST", "/write/end", map[string]any{"ticket": ticket, "outcome": out, "attempts": 1,
		"via": map[string]any{"opId": ticket, "client": 0, "route": "lib", "late": false}})
}

// Write begins, executes and ends a write.
func (c *Client) Write(trackKey bool, reqs ...*servicepb.Request) map[string]any {
	c.T.Helper()
	tk := c.BeginWrite(trackKey, reqs...)

	return c.EndWrite(tk, c.Execute(tk))
}

// BeginRead begins a read with views.
func (c *Client) BeginRead(kind string, views ...map[string]any) map[string]any {
	c.T.Helper()
	c.rids++
	if views == nil {
		views = []map[string]any{}
	}

	return c.OK("POST", "/read/begin", map[string]any{"rid": fmt.Sprintf("r%d", c.rids), "kind": kind, "views": views, "params": map[string]any{}})
}

// EndRead ends a read.
func (c *Client) EndRead(ticket uint64, kind string, request any, out oc.Outcome) map[string]any {
	c.T.Helper()

	return c.OK("POST", "/read/end", map[string]any{"ticket": ticket, "kind": kind, "request": request, "outcome": out,
		"route": map[string]any{"client": 0, "route": "lib", "node": "", "consistency": "linearizable"}, "hint": map[string]any{}, "params": map[string]any{}})
}

// Verdict extracts the verdict of an answer.
func Verdict(m map[string]any) (ok bool, violations []string) {
	v, _ := m["verdict"].(map[string]any)
	ok, _ = v["ok"].(bool)
	list, _ := v["violations"].([]any)
	for _, x := range list {
		violations = append(violations, x.(map[string]any)["id"].(string))
	}

	return ok, violations
}
