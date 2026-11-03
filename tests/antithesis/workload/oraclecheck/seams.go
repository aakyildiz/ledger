package oraclecheck

import "github.com/formancehq/ledger/v3/tests/oracle"

// Seams with task 01 (failure freezing in tests/oracle). Task 00d replaces the
// bodies with the real functions once 01 is merged.

// recordFailure freezes an explained failure of a keyed bulk.
// TODO(00d): return gs.RecordFailure(b, res)
func recordFailure(gs oracle.GlobalState, b oracle.Bulk, res oracle.ApplyResult) oracle.GlobalState {
	return gs
}

// businessFingerprint is the fingerprint without observation state.
// TODO(00d): return gs.BusinessFingerprint()
func businessFingerprint(gs oracle.GlobalState) oracle.Digest { return gs.Fingerprint() }

// uncommittedReasons are freezable reasons the server neither audits nor
// freezes (DESIGN §4.5).
// TODO(00d): oracle.UncommittedReasons
var uncommittedReasons = map[string]bool{"FILTER_COMPILATION_ERROR": true, "CLUSTER_POLICY_INVALID": true, "STALE_CLUSTER_POLICY": true}

// isAdmissionStage reports an admission-stage rejection (never frozen).
// TODO(00d): res.Stage == oracle.StageAdmission
func isAdmissionStage(res oracle.ApplyResult) bool { return false }

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
