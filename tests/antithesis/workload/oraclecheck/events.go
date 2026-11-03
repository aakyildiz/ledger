package oraclecheck

// EventID names one property. Its value is the literal assertion message the
// service asserts for it (DESIGN §9.1).
type EventID string

// EventKind says how the service asserts an event.
type EventKind uint8

const (
	// EvViolation is a finding; the asserter calls assert.Unreachable (or
	// AlwaysOrUnreachable(false)).
	EvViolation EventKind = iota
	// EvAlways is a property with a condition; Cond=false is a finding.
	EvAlways
	// EvSometimes is must-hit coverage with a condition.
	EvSometimes
	// EvReachable is must-hit coverage; Cond is ignored.
	EvReachable
	// EvRaw is a data-driven probe registered with RegisterCoverage and
	// emitted with assert.AssertRaw.
	EvRaw
)

func (k EventKind) String() string {
	switch k {
	case EvViolation:
		return "violation"
	case EvAlways:
		return "always"
	case EvSometimes:
		return "sometimes"
	case EvReachable:
		return "reachable"
	case EvRaw:
		return "raw"
	default:
		return "unknown"
	}
}

// Event is the only channel from the library to the service.
type Event struct {
	ID      EventID
	Kind    EventKind
	Cond    bool
	Details map[string]any
}

// Violation reports a finding.
func Violation(id EventID, d map[string]any) Event {
	return Event{ID: id, Kind: EvViolation, Details: d}
}

// Always reports an evaluation of an always-property.
func Always(id EventID, cond bool, d map[string]any) Event {
	return Event{ID: id, Kind: EvAlways, Cond: cond, Details: d}
}

// Sometimes reports an evaluation of a must-hit sometimes-property.
func Sometimes(id EventID, cond bool, d map[string]any) Event {
	return Event{ID: id, Kind: EvSometimes, Cond: cond, Details: d}
}

// Reached reports that a must-hit point was reached.
func Reached(id EventID, d map[string]any) Event {
	return Event{ID: id, Kind: EvReachable, Cond: true, Details: d}
}

// Raw reports an evaluation of a data-driven coverage probe.
func Raw(id EventID, cond bool, d map[string]any) Event {
	return Event{ID: id, Kind: EvRaw, Cond: cond, Details: d}
}

// IsFinding reports whether the event is a finding: a violation, or an
// always-property evaluated false. Only findings make a verdict not ok and
// trigger the journal dump.
func (e Event) IsFinding() bool {
	return e.Kind == EvViolation || (e.Kind == EvAlways && !e.Cond)
}

// Core event ids (00a). Their values are the literal messages the service
// asserts (DESIGN §9.1: "oracle: " + the singleton model's suffix).
const (
	// Commit cross-check (M/validate.go crossCheckCommit).
	EvModelRejectsCommittedBulk      EventID = "oracle: model rejects a server-committed bulk"
	EvCheckpointCommitOutsideModel   EventID = "oracle: checkpoint commit outside model"
	EvCommittedLogCountMismatch      EventID = "oracle: committed log count mismatch"
	EvCommittedLogIdentityMismatch   EventID = "oracle: committed ledger log identity mismatch"
	EvSkippedOrderMismatch           EventID = "oracle: skipped-order response mismatch"
	EvEnforcementModeMismatch        EventID = "oracle: enforcement-mode response mismatch"
	EvLifecycleResponseMismatch      EventID = "oracle: lifecycle response mismatch"
	EvPostCommitVolumeMismatch       EventID = "oracle: model post-commit volume mismatch"
	EvResponseMetadataMismatch       EventID = "oracle: response metadata value mismatch"
	EvRevertTxIDMismatch             EventID = "oracle: revert transaction id mismatch"
	EvRevertedTxIDMismatch           EventID = "oracle: reverted transaction id mismatch"
	EvRevertPostingsMismatch         EventID = "oracle: revert postings mismatch"
	EvTxIDMismatch                   EventID = "oracle: transaction id mismatch"
	EvTxReferenceMismatch            EventID = "oracle: transaction reference mismatch"
	EvTxPostingsMismatch             EventID = "oracle: transaction postings mismatch"
	EvTxAccountMetadataMismatch      EventID = "oracle: transaction account-metadata mismatch"
	EvDeleteMetadataMismatch         EventID = "oracle: delete-metadata response mismatch"
	EvChartResponseMismatch          EventID = "oracle: chart response mismatch"
	EvSetFieldTypeMismatch           EventID = "oracle: set-field-type response mismatch"
	EvRemoveFieldTypeMismatch        EventID = "oracle: remove-field-type response mismatch"
	EvRemoveFieldTypeDroppedMismatch EventID = "oracle: remove-field-type dropped-index mismatch"
	EvRemoveFieldTypeDroppedWrong    EventID = "oracle: remove-field-type dropped wrong index"

	// Failure and empty-commit checks.
	EvFailureUnexplained EventID = "oracle: bulk failure not explained by any serialization"
	EvEmptyCommit        EventID = "oracle: successful bulk returned no committed log"

	// Late outcomes of an ended ticket (DESIGN §4.6).
	EvOneKeyOneOutcome      EventID = "oracle: one idempotency key yields one outcome across attempts"
	EvTwoDefinitiveOutcomes EventID = "oracle: two attempts of one key both returned definitive outcomes"

	// Reads.
	EvLedgerNotFoundUnexplained EventID = "oracle: ledger-scoped read returned unexplained NotFound"

	// Protocol misuse by the driver (asserted through the same mapping).
	EvProtocolNonDefinitive EventID = "oracle protocol: driver posted a non-definitive write outcome"

	// Must-hit: a definitive server outcome reached a model validation path.
	EvModelOutcomeVerified EventID = "oracle: model outcome verified"
)

// CommitEventIDs lists the crossCheckCommit ids, in check order.
var CommitEventIDs = []EventID{
	EvModelRejectsCommittedBulk, EvCheckpointCommitOutsideModel, EvCommittedLogCountMismatch,
	EvCommittedLogIdentityMismatch, EvSkippedOrderMismatch, EvEnforcementModeMismatch,
	EvLifecycleResponseMismatch, EvPostCommitVolumeMismatch, EvResponseMetadataMismatch,
	EvRevertTxIDMismatch, EvRevertedTxIDMismatch, EvRevertPostingsMismatch, EvTxIDMismatch,
	EvTxReferenceMismatch, EvTxPostingsMismatch, EvTxAccountMetadataMismatch,
	EvDeleteMetadataMismatch, EvChartResponseMismatch, EvSetFieldTypeMismatch,
	EvRemoveFieldTypeMismatch, EvRemoveFieldTypeDroppedMismatch, EvRemoveFieldTypeDroppedWrong,
}

// HasFinding reports whether any event is a finding.
func HasFinding(evs []Event) bool {
	for _, e := range evs {
		if e.IsFinding() {
			return true
		}
	}

	return false
}
