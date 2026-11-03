package testutil

import (
	"fmt"

	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck"
	"github.com/formancehq/ledger/v3/tests/oracle"
)

// DefaultPrefix is the tracked prefix scripted sessions use.
const DefaultPrefix = "o-test-"

// ScriptedSession drives a bare Tracker with begin/end/read sequences against
// a fake, model-faithful server: Server is the state the fake server
// committed, in the order the script commits.
type ScriptedSession struct {
	T       *oraclecheck.Tracker
	Server  oracle.GlobalState
	NextSeq uint64
	Stamps  uint64
	Prefix  string

	wids, keys int
}

// NewSession returns a session on a fresh Tracker with the given prefix
// ("" = DefaultPrefix).
func NewSession(prefix string) *ScriptedSession {
	if prefix == "" {
		prefix = DefaultPrefix
	}

	return newSession(prefix)
}

// NewBareSession returns a session with an empty tracked prefix, so the
// tests/oracle/oracletest builders (ledger "L") are in the partition. Keys are
// "k<N>".
func NewBareSession() *ScriptedSession { return newSession("") }

func newSession(prefix string) *ScriptedSession {
	t := oraclecheck.NewTracker(oraclecheck.Config{RunID: "test", TrackedPrefix: prefix, QueryCheckpointLimit: 10})

	return &ScriptedSession{T: t, Server: oracle.NewGlobalState().WithQueryCheckpointLimit(10), NextSeq: 1, Stamps: 1_700_000_000_000_000, Prefix: prefix}
}

// Ledger names a tracked ledger of the session.
func (s *ScriptedSession) Ledger(n int) string { return fmt.Sprintf("%sl%d", s.Prefix, n) }

// NextKey returns a fresh tracked idempotency key.
func (s *ScriptedSession) NextKey() string {
	s.keys++

	return fmt.Sprintf("%sk%d", s.Prefix, s.keys)
}

// Record builds a WriteRecord with a fresh wid and key.
func (s *ScriptedSession) Record(trackKey bool, reqs ...*servicepb.Request) oraclecheck.WriteRecord {
	s.wids++

	return oraclecheck.WriteRecord{Wid: fmt.Sprintf("w%d", s.wids), Key: s.NextKey(), Role: oraclecheck.RoleOriginal,
		Family: "test", TrackKey: trackKey, Requests: reqs}
}

// Begin registers a write; it panics on a refusal.
func (s *ScriptedSession) Begin(trackKey bool, reqs ...*servicepb.Request) uint64 {
	tk, _, _, ref := s.T.BeginWrite(s.Record(trackKey, reqs...))
	if ref != nil {
		panic(fmt.Sprintf("BeginWrite refused: %s %s", ref.Code, ref.Message))
	}

	return tk
}

// Execute runs ticket's bulk on the fake server now and returns the outcome
// a correct server answers.
func (s *ScriptedSession) Execute(ticket uint64) oraclecheck.Outcome {
	rec, ok := s.T.RecordByTicket(ticket)
	if !ok {
		panic(fmt.Sprintf("unknown ticket %d", ticket))
	}
	bulk := oracle.Bulk{Requests: rec.Requests, IdempotencyKey: rec.Key}
	res := s.Server.Apply(bulk)
	if !res.OK {
		return Fail("FailedPrecondition", res.Reason)
	}
	resp := SimulateCommit(s.Server, bulk, s.NextSeq, s.Stamps)
	s.NextSeq += uint64(len(resp.GetLogs()))
	s.Server = res.State

	return OK(resp)
}

// End executes ticket on the fake server and posts the outcome.
func (s *ScriptedSession) End(ticket uint64) (string, []oraclecheck.Event) {
	status, evs, ref := s.T.EndWrite(ticket, s.Execute(ticket))
	if ref != nil {
		panic(fmt.Sprintf("EndWrite refused: %s %s", ref.Code, ref.Message))
	}

	return status, evs
}

// Write begins and ends a bulk at once.
func (s *ScriptedSession) Write(trackKey bool, reqs ...*servicepb.Request) []oraclecheck.Event {
	_, evs := s.End(s.Begin(trackKey, reqs...))

	return evs
}

// Read begins a read ticket.
func (s *ScriptedSession) Read(kind string) uint64 {
	tk, _, _ := s.T.BeginRead(kind, fmt.Sprintf("r%d", s.T.ReadFrontier()+1))

	return tk
}

// Findings filters the findings out of evs.
func Findings(evs []oraclecheck.Event) []oraclecheck.Event {
	var out []oraclecheck.Event
	for _, e := range evs {
		if e.IsFinding() {
			out = append(out, e)
		}
	}

	return out
}

// IDs lists event ids.
func IDs(evs []oraclecheck.Event) []oraclecheck.EventID {
	out := make([]oraclecheck.EventID, 0, len(evs))
	for _, e := range evs {
		out = append(out, e.ID)
	}

	return out
}
