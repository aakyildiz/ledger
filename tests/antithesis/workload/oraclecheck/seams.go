package oraclecheck

import "github.com/formancehq/ledger/v3/tests/oracle"

// Seams with task 01 (failure freezing in tests/oracle), closed by task 00d.

// recordFailure freezes an explained failure of a keyed bulk.
func recordFailure(gs oracle.GlobalState, b oracle.Bulk, res oracle.ApplyResult) oracle.GlobalState {
	return gs.RecordFailure(b, res)
}

// businessFingerprint is the fingerprint without observation state.
func businessFingerprint(gs oracle.GlobalState) oracle.Digest { return gs.BusinessFingerprint() }

// uncommittedReasons are freezable reasons the server neither audits nor
// freezes (DESIGN §4.5).
var uncommittedReasons = oracle.UncommittedReasons

// isAdmissionStage reports an admission-stage rejection (never frozen).
func isAdmissionStage(res oracle.ApplyResult) bool { return res.Stage == oracle.StageAdmission }

// StageSeamWired reports whether the seams above are the real task-01
// functions; the stage/freeze conformance test is strict only then.
const StageSeamWired = true

// UncommittedReason reports whether a reason is one the server never freezes.
func UncommittedReason(reason string) bool { return uncommittedReasons[reason] }

// IsAdmissionStage exposes the stage seam to the service's conformance test.
func IsAdmissionStage(res oracle.ApplyResult) bool { return isAdmissionStage(res) }

// BusinessFingerprint exposes the fingerprint seam.
func BusinessFingerprint(gs oracle.GlobalState) oracle.Digest { return businessFingerprint(gs) }

// RecordFailure exposes the freezing seam (used by the conformance test).
func RecordFailure(gs oracle.GlobalState, b oracle.Bulk, res oracle.ApplyResult) oracle.GlobalState {
	return recordFailure(gs, b, res)
}
