package quietcheck

import (
	"encoding/json"

	oc "github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck"
	"github.com/formancehq/ledger/v3/tests/oracle"
)

// Section names (DESIGN §4.10, §5.3).
const (
	SectionAudit  = "audit"
	SectionLedger = "ledger"
)

// extKey names the family side table in Tracker.Ext.
const extKey = "quiet"

// obligationKind says which audit entry a tracked record is owed.
type obligationKind uint8

const (
	oweSuccess  obligationKind = iota + 1 // an acknowledged success: one AuditSuccess
	oweFailure                            // a definitive-frozen FSM failure: one AuditFailure
	oweConflict                           // a conflict body answered IDEMPOTENCY_KEY_CONFLICT: at least one entry
)

type obligation struct {
	wid  string
	kind obligationKind
}

// partKey identifies one accumulated part of the ledger section.
type partKey struct{ ledger, scope, part string }

// scopeKey identifies the parts verified for one ledger in one scope.
type scopeKey struct{ ledger, scope string }

// state is the task-11 side table (Tracker.Ext("quiet")). The replay fields
// persist across quiet periods (the audit replay is incremental); the epoch
// fields are reset when a new epoch posts its first section.
type state struct {
	// ---- incremental audit replay ----
	replay oracle.GlobalState
	// cursor is the last audit sequence consumed.
	cursor uint64
	// decided: records an audit entry was matched to whose ticket is above
	// scanned. A record at or below scanned is settled (matched, reported
	// missing, or owed nothing) and counts as decided: finish prunes it from
	// the map, so the map holds only the current interval's matches.
	decided map[string]bool
	// conflictSeen counts the IDEMPOTENCY_KEY_CONFLICT entries per key not yet
	// consumed by an owed conflict record (any number of attempts may reach
	// the FSM; each owed record needs at least one).
	conflictSeen map[string]int
	// obligations are the records still owed an entry, in ticket order;
	// scanned is the highest ticket classified into them.
	obligations []obligation
	scanned     uint64
	// forceResync makes the next epoch's first audit chunk resynchronise as
	// if the tracker had dropped held responses (tests only: the tracker's
	// cap is 100 000 responses).
	forceResync bool

	// ---- current epoch ----
	// epoch and begins identify the quiet period the fields below belong to:
	// the driver's epoch number and the tracker's QuietBegins count (a reused
	// epoch number, e.g. after a driver restart, still starts afresh).
	epoch  uint64
	begins uint64
	// ranges is the wid → committed log range index, built once per quiet
	// period (the tracker's table only grows; quiet periods freeze it).
	ranges        map[string]oc.CommittedBulk
	auditChunks   int
	auditDone     bool
	resynced      bool
	auditFindings int
	parts         map[partKey][]json.RawMessage
	verified      map[scopeKey]map[string]bool
	reported      map[scopeKey]bool
	servedLogs    map[string][]rowRef
	globalDone    bool
}

func newState(t *oc.Tracker) *state {
	return &state{
		replay:       oracle.NewGlobalState().WithQueryCheckpointLimit(t.Config().QueryCheckpointLimit),
		decided:      map[string]bool{},
		conflictSeen: map[string]int{},
	}
}

// load returns the tracker's quiet side table, reset for epoch when epoch
// differs from the last one seen.
func load(t *oc.Tracker, epoch uint64) *state {
	st, _ := t.Ext(extKey, func() any { return newState(t) }).(*state)
	if st.parts == nil || st.epoch != epoch || st.begins != t.QuietBegins() {
		st.epoch = epoch
		st.begins = t.QuietBegins()
		st.ranges = nil
		st.auditChunks = 0
		st.auditDone = false
		st.resynced = false
		st.auditFindings = 0
		st.parts = map[partKey][]json.RawMessage{}
		st.verified = map[scopeKey]map[string]bool{}
		st.reported = map[scopeKey]bool{}
		st.servedLogs = map[string][]rowRef{}
		st.globalDone = false
	}

	return st
}

// rangesByWid returns the committed bulk ranges indexed by wid, built once
// per quiet period.
func (st *state) rangesByWid(t *oc.Tracker) map[string]oc.CommittedBulk {
	if st.ranges == nil {
		bulks := t.CommittedBulks()
		st.ranges = make(map[string]oc.CommittedBulk, len(bulks))
		for _, b := range bulks {
			st.ranges[b.Wid] = b
		}
	}

	return st.ranges
}

// isDecided reports whether rec was matched to an audit entry, or is settled
// (at or below the scanned ticket).
func (st *state) isDecided(rec oc.WriteRecord) bool {
	return rec.Ticket <= st.scanned || st.decided[rec.Wid]
}

func init() {
	oc.RegisterQuietSection(SectionAudit, auditSection)
	oc.RegisterQuietSection(SectionLedger, ledgerSection)
}
