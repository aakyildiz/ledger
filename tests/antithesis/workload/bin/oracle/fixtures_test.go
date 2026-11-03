package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/bin/oracle/servicetest"
	oc "github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck/testutil"
	"github.com/formancehq/ledger/v3/tests/oracle"
)

// Golden protocol fixtures of task 00 (DESIGN §4.1, §10).

const fixtureDir = "../../../hegel/protocol-fixtures/core"

type fixture struct {
	Name       string          `json:"-"`
	Endpoint   string          `json:"endpoint"`
	Request    json.RawMessage `json:"request"`
	Status     int             `json:"status"`
	Response   json.RawMessage `json:"response"`
	Definitive *bool           `json:"definitive"`
	Transient  *bool           `json:"transient"`
}

func loadFixtures(t *testing.T) []fixture {
	t.Helper()
	paths, err := filepath.Glob(filepath.Join(fixtureDir, "*.json"))
	require.NoError(t, err)
	require.NotEmpty(t, paths)
	var out []fixture
	for _, p := range paths {
		b, err := os.ReadFile(p)
		require.NoError(t, err)
		var f fixture
		require.NoError(t, json.Unmarshal(b, &f), p)
		f.Name = strings.TrimSuffix(filepath.Base(p), ".json")
		out = append(out, f)
	}

	return out
}

func canonical(t *testing.T, b []byte) string {
	t.Helper()
	d := json.NewDecoder(bytes.NewReader(b))
	d.UseNumber()
	var v any
	require.NoError(t, d.Decode(&v))
	out, err := json.Marshal(v)
	require.NoError(t, err)

	return string(out)
}

func requestStruct(endpoint string) any {
	path, _, _ := strings.Cut(endpoint, "?")
	switch path {
	case "POST /setup":
		return &setupReq{}
	case "POST /view":
		return &viewReq{}
	case "POST /write/begin":
		return &writeBeginReq{}
	case "POST /write/end":
		return &writeEndReq{}
	case "POST /write/late":
		return &writeLateReq{}
	case "POST /write/abandon":
		return &writeAbandonReq{}
	case "POST /read/begin":
		return &readBeginReq{}
	case "POST /read/end":
		return &readEndReq{}
	case "POST /read/abandon":
		return &readAbandonReq{}
	case "POST /quiet/begin":
		return &quietBeginReq{}
	case "POST /quiet/section":
		return &quietSectionReq{}
	case "POST /quiet/end":
		return &quietEndReq{}
	}

	return nil
}

func TestFixtures_RequestsRoundTrip(t *testing.T) {
	t.Parallel()
	for _, f := range loadFixtures(t) {
		if f.Endpoint == "outcome" {
			var o oc.Outcome
			require.NoError(t, json.Unmarshal(f.Response, &o), f.Name)
			b, err := json.Marshal(o)
			require.NoError(t, err)
			require.Equal(t, canonical(t, f.Response), canonical(t, b), f.Name)
			require.Equal(t, *f.Definitive, o.Definitive(), f.Name)
			require.Equal(t, *f.Transient, o.Transient(false), f.Name)

			continue
		}
		if string(f.Request) == "null" {
			require.True(t, strings.HasPrefix(f.Endpoint, "GET "), f.Name)

			continue
		}
		v := requestStruct(f.Endpoint)
		require.NotNil(t, v, "%s: no request struct for %s", f.Name, f.Endpoint)
		dec := json.NewDecoder(bytes.NewReader(f.Request))
		dec.DisallowUnknownFields()
		require.NoError(t, dec.Decode(v), f.Name)
		b, err := json.Marshal(v)
		require.NoError(t, err)
		require.Equal(t, canonical(t, f.Request), canonical(t, b), f.Name)
		if wb, ok := v.(*writeBeginReq); ok {
			_, err := decodeRequests(wb.Requests)
			require.NoError(t, err, f.Name)
		}
	}
}

// mapPaths hold objects whose keys are data (ledger names, metadata keys,
// section- or provider-defined content, informative details): only the
// values present on both sides are compared.
var mapPaths = map[string]bool{
	"error.details": true, "expect": true, "providers": true, "next": true,
	"views[].fieldTypes.account": true, "views[].fieldTypes.transaction": true, "views[].fieldTypes.ledger": true,
	"views[].indexes[].id": true, "entries[].req": true, "entries[].resp": true,
	"verdict.violations[].details": true,
}

func jsonType(v any) string {
	switch v.(type) {
	case nil:
		return "null"
	case bool:
		return "bool"
	case json.Number:
		return "number"
	case string:
		return "string"
	case []any:
		return "array"
	case map[string]any:
		return "object"
	}

	return fmt.Sprintf("%T", v)
}

// shapeMatch compares JSON shapes: same keys and JSON types; arrays compare
// their first elements when both are non-empty; null matches an object
// (optional entries, `{"ledger": null}`).
func shapeMatch(path string, want, got any) error {
	wt, gt := jsonType(want), jsonType(got)
	if wt != gt {
		if (wt == "null" && gt == "object") || (wt == "object" && gt == "null") {
			return nil
		}

		return fmt.Errorf("%s: type %s, want %s", path, gt, wt)
	}
	switch w := want.(type) {
	case map[string]any:
		g := got.(map[string]any)
		if !mapPaths[path] {
			var wk, gk []string
			for k := range w {
				wk = append(wk, k)
			}
			for k := range g {
				gk = append(gk, k)
			}
			sort.Strings(wk)
			sort.Strings(gk)
			if strings.Join(wk, ",") != strings.Join(gk, ",") {
				return fmt.Errorf("%s: keys %v, want %v", path, gk, wk)
			}
		}
		for k, wv := range w {
			gv, ok := g[k]
			if !ok {
				continue
			}
			p := k
			if path != "" {
				p = path + "." + k
			}
			if mapPaths[path] {
				p = path + ".*"
			}
			if err := shapeMatch(p, wv, gv); err != nil {
				return err
			}
		}
	case []any:
		g := got.([]any)
		if len(w) > 0 && len(g) > 0 {
			return shapeMatch(path+"[]", w[0], g[0])
		}
	}

	return nil
}

func decodeAny(t *testing.T, b []byte) any {
	t.Helper()
	d := json.NewDecoder(bytes.NewReader(b))
	d.UseNumber()
	var v any
	require.NoError(t, d.Decode(&v))

	return v
}

// fixtureRun is the scripted state a fixture is replayed in.
type fixtureRun struct {
	t *testing.T
	s *server
	c *servicetest.Client
}

const fixtureRunID = "18f6c2a1b3d4e"

func (r *fixtureRun) ledger(n int) string { return fmt.Sprintf("o-%s-l%d", fixtureRunID, n) }

// newFixtureRun sets up the fixtures' run id with ledgers l1..l3 live, l4
// deleted, some data on l3.
func newFixtureRun(t *testing.T, ledgers bool) *fixtureRun {
	t.Helper()
	s, c := newTestServer(t)
	c.Setup(fixtureRunID)
	r := &fixtureRun{t: t, s: s, c: c}
	if !ledgers {
		return r
	}
	for n := 1; n <= 4; n++ {
		c.Write(false, testutil.CreateLedgerReq(r.ledger(n)))
	}
	c.Write(false, testutil.DeleteLedgerReq(r.ledger(4)))
	l3 := r.ledger(3)
	c.Write(true,
		testutil.AddTypeReq(l3, "t-3", "t-3:{id}"),
		testutil.TxReq(l3, "o-"+fixtureRunID+"-ref2", "world", "t-3:42", "COIN", 100),
		testutil.AccountMetaReq(l3, "t-3:42", "tier", "gold"),
	)

	return r
}

// send replays the fixture request with overrides.
func (r *fixtureRun) send(f fixture, override map[string]any) (int, []byte) {
	r.t.Helper()
	method, path, _ := strings.Cut(f.Endpoint, " ")
	if string(f.Request) == "null" {
		return r.c.DoRaw(method, path, nil)
	}
	var m map[string]any
	require.NoError(r.t, json.Unmarshal(f.Request, &m))
	for k, v := range override {
		m[k] = v
	}

	return r.c.DoRaw(method, path, m)
}

func fixtureByName(t *testing.T, name string) fixture {
	t.Helper()
	for _, f := range loadFixtures(t) {
		if f.Name == name {
			return f
		}
	}
	t.Fatalf("no fixture %s", name)

	return fixture{}
}

// beginFixtureWrite begins the plain write-begin fixture (w123, key k77) and
// returns its ticket.
func (r *fixtureRun) beginFixtureWrite() uint64 {
	r.t.Helper()
	status, b := r.send(fixtureByName(r.t, "write-begin"), nil)
	require.Equal(r.t, 200, status, string(b))
	var m map[string]any
	require.NoError(r.t, json.Unmarshal(b, &m))

	return uint64(m["ticket"].(float64))
}

func (r *fixtureRun) execute(f fixture, ticket uint64) oc.Outcome {
	r.t.Helper()
	var req writeBeginReq
	require.NoError(r.t, json.Unmarshal(f.Request, &req))
	reqs, err := decodeRequests(req.Requests)
	require.NoError(r.t, err)
	bulk := oracle.Bulk{Requests: reqs, IdempotencyKey: req.IdempotencyKey}
	res := r.c.Server.Apply(bulk)
	if !res.OK {
		return testutil.Fail("FailedPrecondition", res.Reason)
	}
	resp := testutil.SimulateCommit(r.c.Server, bulk, r.c.NextSeq, 1_700_000_000_000_000)
	r.c.NextSeq += uint64(len(resp.GetLogs()))
	r.c.Server = res.State

	return testutil.OK(resp)
}

func TestFixtures_ResponseShapes(t *testing.T) {
	isCheckpoint := func(rec oc.WriteRecord) bool {
		return len(rec.Requests) == 1 && rec.Requests[0].GetCreateQueryCheckpoint() != nil
	}
	oc.RegisterBeginHook("zz-fixture-checkpoint", func(tr *oc.Tracker, rec oc.WriteRecord) (map[string]any, *oc.Refusal) {
		if !isCheckpoint(rec) {
			return nil, nil
		}
		for _, in := range tr.InflightBulks() {
			if isCheckpoint(in) {
				return nil, oc.Serialized("a query checkpoint create is still undrained", map[string]any{"hook": "checkpoint", "blockingTicket": in.Ticket})
			}
		}

		return map[string]any{"checkpointId": fmt.Sprint(tr.Model().NextQueryCheckpointID())}, nil
	})
	oc.RegisterQuietSection("zz-fixture-section", func(*oc.Tracker, uint64, uint64, json.RawMessage) (any, []oc.Event, *oc.Refusal) {
		return map[string]any{"from": "18233"}, nil, nil
	})
	oc.RegisterReadKind(oc.ReadKind{Name: "zz-fixture-panic", Validate: func(*oc.ReadCtx) []oc.Event { panic("index out of range") }})
	t.Cleanup(func() { oc.TestingUnregister("zz-fixture-checkpoint", "zz-fixture-section", "zz-fixture-panic") })

	smoke := map[string]any{"kind": "Smoke"}
	for _, f := range loadFixtures(t) {
		if f.Endpoint == "outcome" {
			continue
		}
		t.Run(f.Name, func(t *testing.T) {
			var status int
			var body []byte
			switch f.Name {
			case "setup":
				r := newFixtureRun(t, false)
				status, body = r.send(f, nil)
			case "setup.conflict":
				r := newFixtureRun(t, false)
				status, body = r.send(f, nil)
			case "read-begin.empty-live":
				r := newFixtureRun(t, false)
				status, body = r.send(f, map[string]any{"kind": "Smoke"})
			default:
				r := newFixtureRun(t, true)
				status, body = r.script(f, smoke)
			}
			require.Equal(t, f.Status, status, "%s", body)
			if f.Status != 200 {
				var w, g map[string]any
				require.NoError(t, json.Unmarshal(f.Response, &w))
				require.NoError(t, json.Unmarshal(body, &g))
				require.Equal(t, w["error"].(map[string]any)["code"], g["error"].(map[string]any)["code"])
			}
			require.NoError(t, shapeMatch("", decodeAny(t, f.Response), decodeAny(t, body)), "%s", body)
		})
	}
}

func (r *fixtureRun) script(f fixture, smoke map[string]any) (int, []byte) {
	t, c := r.t, r.c
	switch f.Name {
	case "health", "tickets", "dump", "quiet-status", "view", "read-begin",
		"write-begin", "write-begin.unmodeled", "write-begin.too-many-requests", "write-end.unknown-ticket":
		if f.Name == "tickets" || f.Name == "dump" {
			r.beginFixtureWrite()
			c.BeginRead("Smoke")
		}
		if strings.HasPrefix(f.Name, "read-begin") {
			return r.send(f, smoke)
		}

		return r.send(f, nil)
	case "journal":
		return r.send(f, nil)
	case "write-begin.checkpoint":
		return r.send(f, nil)
	case "write-begin.serialized":
		c.BeginWrite(false, testutil.CreateCheckpointReq())

		return r.send(f, nil)
	case "write-begin.conflict", "write-begin.protocol-conflict-equal", "write-begin.protocol-conflict-untracked":
		r.beginFixtureWrite()

		return r.send(f, nil)
	case "write-begin.quiet":
		c.OK("POST", "/quiet/begin", map[string]any{"epoch": 7})

		return r.send(f, nil)
	case "write-begin.busy":
		for range 8 {
			c.BeginWrite(false, testutil.TxReq(r.ledger(1), "", "world", "t-0:1", "COIN", 1))
		}

		return r.send(f, nil)
	case "write-begin.backlog":
		r.s.tracker = oc.NewTracker(oc.Config{TrackedPrefix: "o-" + fixtureRunID + "-", PendingCap: 1})
		c.Server = oracle.NewGlobalState()
		c.Write(false, testutil.CreateLedgerReq(r.ledger(3)))
		hold := c.BeginWrite(false, testutil.TxReq(r.ledger(3), "", "world", "t-0:1", "COIN", 1))
		for range 2 {
			c.Write(false, testutil.TxReq(r.ledger(3), "", "world", "t-0:1", "COIN", 1))
		}
		_ = hold

		return r.send(f, nil)
	case "write-end.pending":
		tk := r.beginFixtureWrite()
		r.beginFixtureWriteOther()

		return r.send(f, map[string]any{"ticket": tk, "outcome": r.execute(fixtureByName(t, "write-begin"), tk)})
	case "write-end.protocol-nondefinitive":
		tk := r.beginFixtureWrite()

		return r.send(f, map[string]any{"ticket": tk})
	case "write-end.validated-failure":
		tk := c.BeginWrite(false, testutil.TxReq(r.ledger(3), "", "t-3:7", "t-3:42", "COIN", 5))

		return r.send(f, map[string]any{"ticket": tk})
	case "write-late":
		tk := r.beginFixtureWrite()
		out := r.execute(fixtureByName(t, "write-begin"), tk)
		c.EndWrite(tk, out)

		return r.send(f, map[string]any{"ticket": tk, "outcome": out})
	case "write-abandon", "write-abandon.protocol-attempts":
		tk := r.beginFixtureWrite()

		return r.send(f, map[string]any{"ticket": tk})
	case "read-end", "read-end.skipped", "read-end.violation", "read-abandon", "panic":
		kind := "Smoke"
		if f.Name == "panic" {
			kind = "zz-fixture-panic"
		}
		rb := c.BeginRead(kind)
		tk := rb["ticket"]
		if f.Name == "read-abandon" {
			return r.send(f, map[string]any{"ticket": tk})
		}
		over := map[string]any{"ticket": tk, "kind": kind}
		if f.Name == "read-end" {
			over["outcome"] = oc.Outcome{OK: true, Response: renderAccount(c, r.ledger(3), "t-3:42")}
		}

		return r.send(f, over)
	case "quiet-begin", "quiet-end":
		if f.Name == "quiet-end" {
			c.OK("POST", "/quiet/begin", map[string]any{"epoch": 7})
		}

		return r.send(f, nil)
	case "quiet-begin.not-quiet":
		r.beginFixtureWrite()

		return r.send(f, nil)
	case "quiet-begin.maintenance":
		c.Write(false, testutil.MaintenanceReq(true))

		return r.send(f, nil)
	case "quiet-section":
		c.OK("POST", "/quiet/begin", map[string]any{"epoch": 7})

		return r.send(f, map[string]any{"section": "zz-fixture-section"})
	}
	t.Fatalf("no script for fixture %s", f.Name)

	return 0, nil
}

// beginFixtureWriteOther keeps a later ticket in flight so a success stays
// pending... it is not needed for the gate (a later ticket does not block),
// so it only exercises the path where other writes are outstanding.
func (r *fixtureRun) beginFixtureWriteOther() {
	r.c.BeginWrite(false, &servicepb.Request{Type: &servicepb.Request_CreateLedger{CreateLedger: &servicepb.CreateLedgerRequest{Name: r.ledger(9)}}})
}

func TestShapeMatch_DetectsDifferences(t *testing.T) {
	t.Parallel()
	d := func(s string) any { return decodeAny(t, []byte(s)) }
	require.NoError(t, shapeMatch("", d(`{"a":1,"b":[{"c":"x"}]}`), d(`{"a":2,"b":[{"c":"y"}]}`)))
	require.Error(t, shapeMatch("", d(`{"a":1}`), d(`{"a":"1"}`)))
	require.Error(t, shapeMatch("", d(`{"a":1}`), d(`{"a":1,"b":2}`)))
	require.Error(t, shapeMatch("", d(`{"b":[{"c":"x"}]}`), d(`{"b":[{"d":"x"}]}`)))
	require.NoError(t, shapeMatch("", d(`{"expect":{"l1":{"n":1}}}`), d(`{"expect":{"l9":{"n":1}}}`)))
}
