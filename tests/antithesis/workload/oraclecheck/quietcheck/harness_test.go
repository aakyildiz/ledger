package quietcheck_test

import (
	"encoding/json"
	"fmt"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/auditpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	oc "github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck/quietcheck"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck/testutil"
	"github.com/formancehq/ledger/v3/tests/oracle"
)

// harness is a scripted session against a fake, model-faithful server that
// also writes the server's audit trail: one success entry per commit, one
// failure entry per FSM-stage failure that is neither replayed nor
// uncommitted (freezing it like recordIdempotencyFailure), nothing for an
// admission rejection.
type harness struct {
	t     *testing.T
	s     *testutil.ScriptedSession
	trail []*auditpb.AuditEntry
	epoch uint64
}

func newHarness(t *testing.T) *harness {
	t.Helper()

	return &harness{t: t, s: testutil.NewSession("")}
}

func (h *harness) ledger(n int) string { return h.s.Ledger(n) }

func distinctLedgers(reqs []*servicepb.Request) []string {
	var out []string
	for _, r := range reqs {
		if l := oracle.LedgerOf(r); l != "" && !slices.Contains(out, l) {
			out = append(out, l)
		}
	}
	slices.Sort(out)

	return out
}

// failureDetails builds FailureDetails whose reason is reason.
func failureDetails(t *testing.T, reason string) *commonpb.FailureDetails {
	t.Helper()
	want := domain.ReasonCode(reason)
	d := &commonpb.FailureDetails{}
	oneof := d.ProtoReflect().Descriptor().Oneofs().ByName("kind")
	for i := range oneof.Fields().Len() {
		f := oneof.Fields().Get(i)
		c := &commonpb.FailureDetails{}
		c.ProtoReflect().Set(f, protoreflect.ValueOfMessage(c.ProtoReflect().NewField(f).Message()))
		if commonpb.FailureDetailsReason(c) == want {
			return c
		}
	}
	t.Fatalf("no failure details arm for %s", reason)

	return nil
}

func (h *harness) appendEntry(e *auditpb.AuditEntry) {
	e.Sequence = uint64(len(h.trail) + 1)
	h.trail = append(h.trail, e)
}

// failureEntry appends a failure entry for key.
func (h *harness) failureEntry(key, reason string, reqs []*servicepb.Request) {
	e := &auditpb.AuditEntry{OrderCount: uint32(len(reqs)), Ledgers: distinctLedgers(reqs),
		Outcome: &auditpb.AuditEntry_Failure{Failure: &auditpb.AuditFailure{Details: failureDetails(h.t, reason)}}}
	if key != "" {
		e.Idempotency = &commonpb.Idempotency{Key: key}
	}
	h.appendEntry(e)
}

// exec runs one attempt of ticket's bulk on the fake server.
func (h *harness) exec(ticket uint64) oc.Outcome {
	rec, ok := h.s.T.RecordByTicket(ticket)
	require.True(h.t, ok)
	bulk := oracle.Bulk{Requests: rec.Requests, IdempotencyKey: rec.Key}
	res := h.s.Server.Apply(bulk)
	if !res.OK {
		if res.Stage == oracle.StageFSM && !res.Replayed && !oracle.UncommittedReasons[res.Reason] {
			h.failureEntry(rec.Key, res.Reason, rec.Requests)
			h.s.Server = h.s.Server.RecordFailure(bulk, res)
		}

		return testutil.Fail("FailedPrecondition", res.Reason)
	}
	require.False(h.t, res.Replayed, "the harness does not replay successes")
	resp := testutil.SimulateCommit(h.s.Server, bulk, h.s.NextSeq, h.s.Stamps)
	first := h.s.NextSeq
	h.s.NextSeq += uint64(len(resp.GetLogs()))
	h.s.Server = res.State
	h.appendEntry(&auditpb.AuditEntry{
		OrderCount: uint32(len(rec.Requests)), Ledgers: distinctLedgers(rec.Requests),
		Idempotency: &commonpb.Idempotency{Key: rec.Key},
		Outcome:     &auditpb.AuditEntry_Success{Success: &auditpb.AuditSuccess{MinLogSequence: first, MaxLogSequence: h.s.NextSeq - 1}},
	})

	return testutil.OK(resp)
}

func (h *harness) end(ticket uint64, out oc.Outcome) {
	_, evs, ref := h.s.T.EndWrite(ticket, out)
	require.Nil(h.t, ref)
	require.Empty(h.t, testutil.Findings(evs), "online validation")
}

// write begins, executes once and ends a bulk; it returns the record.
func (h *harness) write(trackKey bool, reqs ...*servicepb.Request) oc.WriteRecord {
	tk := h.s.Begin(trackKey, reqs...)
	h.end(tk, h.exec(tk))
	rec, _ := h.s.T.RecordByTicket(tk)

	return rec
}

// conflict sends a different body under orig's key attempts times.
func (h *harness) conflict(orig oc.WriteRecord, attempts int, reqs ...*servicepb.Request) oc.WriteRecord {
	rec := h.s.Record(true, reqs...)
	rec.Key, rec.Role, rec.Of = orig.Key, oc.RoleConflict, orig.Wid
	tk, _, _, ref := h.s.T.BeginWrite(rec)
	require.Nil(h.t, ref)
	var out oc.Outcome
	for range attempts {
		out = h.exec(tk)
	}
	h.end(tk, out)
	r, _ := h.s.T.RecordByTicket(tk)

	return r
}

// quiet runs one quiet period's audit replay over the trail from the
// tracker's cursor, in chunks of chunk entries; it returns every event.
func (h *harness) quiet(chunk int) ([]oc.Event, quietcheck.AuditNext) {
	h.t.Helper()
	h.epoch++
	_, ref := h.s.T.BeginQuiet(h.epoch)
	require.Nil(h.t, ref)
	defer h.s.T.EndQuiet(h.epoch)

	return h.replayChunks(h.trail, chunk)
}

func (h *harness) replayChunks(trail []*auditpb.AuditEntry, chunk int) ([]oc.Event, quietcheck.AuditNext) {
	h.t.Helper()
	var cursor uint64
	_, err := fmt.Sscan(h.s.T.AuditCursor(), &cursor)
	require.NoError(h.t, err)
	var rest []*auditpb.AuditEntry
	for _, e := range trail {
		if e.GetSequence() > cursor {
			rest = append(rest, e)
		}
	}
	var evs []oc.Event
	var next quietcheck.AuditNext
	seq := uint64(0)
	for {
		n := min(chunk, len(rest))
		body := quietcheck.AuditBody{Complete: n == len(rest), Head: fmt.Sprint(len(trail))}
		for _, e := range rest[:n] {
			b, err := protojson.Marshal(e)
			require.NoError(h.t, err)
			body.Entries = append(body.Entries, b)
		}
		rest = rest[n:]
		e, nx := h.section(quietcheck.SectionAudit, seq, body)
		seq++
		evs = append(evs, e...)
		next = nx.(quietcheck.AuditNext)
		if body.Complete || next.Resync {
			return evs, next
		}
	}
}

func (h *harness) section(name string, seq uint64, body any) ([]oc.Event, any) {
	h.t.Helper()
	fn, ok := oc.LookupQuietSection(name)
	require.True(h.t, ok)
	raw, err := json.Marshal(body)
	require.NoError(h.t, err)
	next, evs, ref := fn(h.s.T, h.epoch, seq, raw)
	require.Nil(h.t, ref, "section refused")

	return evs, next
}

func ids(evs []oc.Event) []oc.EventID {
	var out []oc.EventID
	for _, e := range testutil.Findings(evs) {
		out = append(out, e.ID)
	}

	return out
}

func reached(evs []oc.Event, id oc.EventID) bool {
	for _, e := range evs {
		if e.ID == id && (e.Kind == oc.EvReachable || e.Cond) {
			return true
		}
	}

	return false
}

// onLedger sets the ledger field of the request's arm (the oracletest
// builders target ledger "L").
func onLedger(req *servicepb.Request, ledger string) *servicepb.Request {
	req = proto.Clone(req).(*servicepb.Request)
	m := req.ProtoReflect()
	arm := m.WhichOneof(m.Descriptor().Oneofs().ByName("type"))
	inner := m.Get(arm).Message()
	for _, name := range []protoreflect.Name{"ledger", "name"} {
		if f := inner.Descriptor().Fields().ByName(name); f != nil && f.Kind() == protoreflect.StringKind {
			inner.Set(f, protoreflect.ValueOfString(ledger))

			return req
		}
	}
	panic("request has no ledger field")
}
