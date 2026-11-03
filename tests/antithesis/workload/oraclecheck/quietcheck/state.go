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
	// decided: records an audit entry was matched to.
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
	epoch         uint64
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
	if st.parts == nil || st.epoch != epoch {
		st.epoch = epoch
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

func init() {
	oc.RegisterQuietSection(SectionAudit, auditSection)
	oc.RegisterQuietSection(SectionLedger, ledgerSection)
}
