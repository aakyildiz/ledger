package auditcheck

import (
	"slices"
	"strconv"
	"strings"

	oc "github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck/match"
	"github.com/formancehq/ledger/v3/tests/oracle"
)

// Model is what the audit checks read from the tracker, gathered once per
// read. Tests build it directly.
type Model struct {
	// LedgerPrefix is the tracked ledger prefix ("o-<run>-"); KeyPrefix the
	// tracked idempotency-key prefix ("o-<run>-k").
	LedgerPrefix, KeyPrefix string
	// Logs is every committed tracked log a sequence was learned for — the
	// oracle's rows and the ledger-level logs (M/audit.go
	// committedLogsBySequence) — and CommittedMax the highest of them.
	Logs         map[uint64]match.CommittedLog
	CommittedMax uint64
	// Bulks are the committed tracked bulks' boundaries, keyed by min sequence.
	Bulks map[uint64]match.CommittedBulk
	// Rejections are the model-explained rejection shapes.
	Rejections map[oc.RejectedBulk]struct{}
	// KeyByFirstSeq maps a replayable commit's first sequence to its key.
	KeyByFirstSeq map[uint64]string
	// Outstanding reports whether an entry under a tracked key may belong to
	// a bulk the model has not settled yet (in flight or not drained).
	Outstanding func(key string) bool
	// FrozenRejected reports a key whose only write (no conflict body, no
	// other writer) ended in a definitive-frozen rejection at the FSM stage.
	FrozenRejected func(key string) bool
}

// Tracked reports whether e is the record of a tracked bulk: its key carries
// the tracked key prefix.
func (m Model) Tracked(e match.AuditEntry) bool {
	return m.KeyPrefix != "" && strings.HasPrefix(e.IdemKey, m.KeyPrefix)
}

// trackedLedger returns the first ledger of e in the tracked partition.
func (m Model) trackedLedger(e match.AuditEntry) (string, bool) {
	if m.LedgerPrefix == "" {
		return "", false
	}
	for _, l := range e.Ledgers {
		if strings.HasPrefix(l, m.LedgerPrefix) {
			return l, true
		}
	}

	return "", false
}

// rejectionExplains reports whether a recorded, model-explained rejection has
// this entry's ledgers, order count and reason (M/audit.go rejectionExplains).
func (m Model) rejectionExplains(e match.AuditEntry) bool {
	shape := oc.RejectedBulk{Ledgers: strings.Join(e.Ledgers, ","), Orders: e.OrderCount, Reason: e.Reason}
	if _, ok := m.Rejections[shape]; ok {
		return true
	}
	// A reason the wire enum does not name reaches the trail as UNSPECIFIED.
	if e.Reason != "UNSPECIFIED" {
		return false
	}
	for r := range m.Rejections {
		if r.Ledgers == shape.Ledgers && r.Orders == shape.Orders {
			return true
		}
	}

	return false
}

// ModelOf gathers the model from the tracker for a read whose frontier is
// maxTicket.
func ModelOf(t *oc.Tracker, maxTicket uint64) Model {
	prefix := t.Config().TrackedPrefix
	logs := t.MatchCommittedLogs()
	// The ledger-level record carries the order that produced the log, which
	// the served payload does not name; it overrides the row table as in
	// M/audit.go committedLogsBySequence.
	for seq, rec := range t.LedgerLogSeqs() {
		logs[seq] = match.CommittedLog{Ledger: rec.Ledger, Kind: rec.Kind}
	}
	var committedMax uint64
	for seq := range logs {
		committedMax = max(committedMax, seq)
	}
	keyByFirst := map[uint64]string{}
	for _, r := range t.Replayable() {
		if first := firstNonZero(r.LogSeqs); first != 0 {
			keyByFirst[first] = r.Key
		}
	}

	globalOutstanding := false
	for _, tk := range t.InflightTickets() {
		globalOutstanding = globalOutstanding || tk <= maxTicket
	}
	for _, tk := range t.PendingTickets() {
		globalOutstanding = globalOutstanding || tk <= maxTicket
	}

	return Model{
		LedgerPrefix:  prefix,
		KeyPrefix:     prefix + "k",
		Logs:          logs,
		CommittedMax:  committedMax,
		Bulks:         t.MatchCommittedBulks(),
		Rejections:    t.Rejections(),
		KeyByFirstSeq: keyByFirst,
		Outstanding: func(key string) bool {
			recs := t.WritesByKey(key)
			if len(recs) == 0 {
				// A key the oracle never registered: fall back to the
				// singleton's global rule.
				return globalOutstanding
			}
			for _, r := range recs {
				if r.Status == oc.WInflight || r.Status == oc.WPending {
					return true
				}
			}

			return false
		},
		FrozenRejected: func(key string) bool { return frozenRejected(t.WritesByKey(key)) },
	}
}

// frozenRejected is the atomicity precondition: exactly one write under the
// key, ended in a definitive-frozen failure explained at the FSM stage. Keys
// with a conflict record are excluded: a conflict shares the key of a
// committed original, whose success entry stays legal.
func frozenRejected(recs []oc.WriteRecord) bool {
	if len(recs) != 1 {
		return false
	}
	r := recs[0]

	return r.Status == oc.WFailed && r.Outcome != nil && r.Result != nil &&
		r.Result.Stage == oracle.StageFSM && !r.Result.Replayed &&
		r.Outcome.DefinitiveFrozen() && !oc.UncommittedReason(r.Outcome.Reason)
}

// firstNonZero is the smallest non-zero sequence a committed bulk was
// assigned, where its audit entry's log range starts.
func firstNonZero(seqs []uint64) uint64 {
	var first uint64
	for _, seq := range seqs {
		if seq != 0 && (first == 0 || seq < first) {
			first = seq
		}
	}

	return first
}

func sortedLogSeqs(logs map[uint64]match.CommittedLog) []uint64 {
	out := make([]uint64, 0, len(logs))
	for seq := range logs {
		out = append(out, seq)
	}
	slices.Sort(out)

	return out
}

func u(v uint64) string { return strconv.FormatUint(v, 10) }
