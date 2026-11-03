package auditcheck

import (
	"encoding/json"
	"slices"

	"github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck/match"
	"github.com/formancehq/ledger/v3/tests/oracle"

	oc "github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck"
)

// extKey is this family's side table key in Tracker.Ext.
const extKey = "audit"

// KnownCap bounds the remembered entries (M/audit.go knownAuditCap).
// Eviction only weakens the lower bound.
const KnownCap = 4096

// SampleCap bounds the aiming samples the auditSamples view serves.
const SampleCap = 32

// knownEntry is a remembered audit entry and the ticket frontier when the
// read that served it ended: a read whose ticket is greater began after it
// and, being linearizable, must see it (the singleton's auditLearnSeq).
type knownEntry struct {
	entry     match.AuditEntry
	learnedAt uint64
}

// Sample is one served entry's indexed fields, kept so later filters can be
// aimed at values the trail holds (M/audit.go auditSample). Server values are
// decimal strings (DESIGN §4.1).
type Sample struct {
	Seq        uint64   `json:"seq,string"`
	ProposalID uint64   `json:"proposalId,string"`
	Timestamp  uint64   `json:"timestamp,string"`
	Subject    string   `json:"subject"`
	Key        string   `json:"key"`
	Ledgers    []string `json:"ledgers"`
	Outcome    string   `json:"outcome"` // success | failure
	MinLog     uint64   `json:"minLog,string"`
	MaxLog     uint64   `json:"maxLog,string"`
	OrderTypes []string `json:"orderTypes"`
}

// state is the family's side table (Tracker.Ext("audit")): remembered
// entries (knownAudit) and aiming samples (auditSamples). Every mutation is
// deterministic: FIFO eviction, ring replacement.
type state struct {
	known      map[uint64]knownEntry
	knownOrder []uint64
	samples    []Sample
	sampleNext int
}

func stateOf(t *oc.Tracker) *state {
	return t.Ext(extKey, func() any { return &state{known: map[uint64]knownEntry{}} }).(*state)
}

// rememberKnown remembers a validated page or entry (noteKnownAuditEntries).
func (s *state) rememberKnown(entries []match.AuditEntry, learnedAt uint64) {
	for _, e := range entries {
		if _, held := s.known[e.Seq]; held {
			continue
		}
		if len(s.known) >= KnownCap {
			oldest := s.knownOrder[0]
			s.knownOrder = s.knownOrder[1:]
			delete(s.known, oldest)
		}
		s.known[e.Seq] = knownEntry{entry: e, learnedAt: learnedAt}
		s.knownOrder = append(s.knownOrder, e.Seq)
	}
}

// knownBefore lists the remembered entries learned before the read with
// ticket readTicket began, by ascending sequence.
func (s *state) knownBefore(readTicket uint64) []match.AuditEntry {
	seqs := make([]uint64, 0, len(s.known))
	for seq, k := range s.known {
		if k.learnedAt < readTicket {
			seqs = append(seqs, seq)
		}
	}
	slices.Sort(seqs)
	out := make([]match.AuditEntry, 0, len(seqs))
	for _, seq := range seqs {
		out = append(out, s.known[seq].entry)
	}

	return out
}

// noteSamples remembers the indexed fields of a served page, replacing the
// oldest sample once the ring is full.
func (s *state) noteSamples(entries []match.AuditEntry, logs map[uint64]match.CommittedLog) {
	for _, e := range entries {
		smp := Sample{
			Seq: e.Seq, ProposalID: e.ProposalID, Timestamp: e.Timestamp, Subject: e.Subject, Key: e.IdemKey,
			Ledgers: slices.Clone(e.Ledgers), Outcome: "success", MinLog: e.MinLog, MaxLog: e.MaxLog,
		}
		if smp.Ledgers == nil {
			smp.Ledgers = []string{}
		}
		if e.Failed {
			smp.Outcome = "failure"
		}
		types := match.CommittedOrderTypes(e, logs)
		smp.OrderTypes = make([]string, 0, len(types))
		for t := range types {
			smp.OrderTypes = append(smp.OrderTypes, t)
		}
		slices.Sort(smp.OrderTypes)
		if len(s.samples) < SampleCap {
			s.samples = append(s.samples, smp)

			continue
		}
		s.samples[s.sampleNext] = smp
		s.sampleNext = (s.sampleNext + 1) % SampleCap
	}
}

// SamplesView is the auditSamples view: the sampled served entries, ordered
// by sequence, and the tracked key prefix.
func SamplesView(_ oracle.GlobalState, t *oc.Tracker, _ json.RawMessage) (any, error) {
	s := stateOf(t)
	out := slices.Clone(s.samples)
	slices.SortFunc(out, func(a, b Sample) int {
		switch {
		case a.Seq < b.Seq:
			return -1
		case a.Seq > b.Seq:
			return 1
		}

		return 0
	})
	if out == nil {
		out = []Sample{}
	}

	return map[string]any{
		"samples":   out,
		"keyPrefix": t.Config().TrackedPrefix + "k",
		"known":     len(s.known),
	}, nil
}
