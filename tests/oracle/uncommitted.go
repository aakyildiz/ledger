package oracle

import "github.com/formancehq/ledger/v3/internal/domain"

// UncommittedReasons is the set of reason strings the server raises only through
// domain.UncommittedFailure errors: never audited, never frozen, re-evaluated per attempt.
//
// The FSM returns a domain.UncommittedFailure before it writes the failure
// audit entry or calls recordIdempotencyFailure (internal/infra/state/
// machine.go, the two errors.AsType[domain.UncommittedFailure] returns), so a
// keyed bulk failing with one of these leaves its key free: the next attempt
// is evaluated again and may commit. RecordFailure never freezes them.
//
// The list is explicit, one entry per typed error that implements
// UncommittedFailure() (internal/domain/errors.go and
// internal/infra/state/scope.go). uncommitted_test.go builds one value of every
// such type, derives its reason with Describable.Reason() — the value the gRPC
// adapter puts into ErrorInfo.reason (internal/adapter/grpc/errors.go
// describableToGRPCStatus) — and requires that the freezable-kind reasons are
// exactly the freezable-kind members of this set. Non-freezable members are
// listed for completeness; RecordFailure skips them anyway by kind.
//
// Membership is by reason string, so it is conservative where one reason is
// shared by a committed and an uncommitted error:
//   - NUMSCRIPT_COMPILE_ERROR: *ErrNumscriptCompile is committed (audited,
//     frozen); the unclassified compile error is uncommitted. The model does
//     not evaluate Numscript, so it never returns this reason.
//   - CLUSTER_POLICY_INVALID: *ErrClusterPolicyInvalid and the
//     unconfigured-metadata-limits error are both uncommitted.
//
// Two FSM rejections bypass the audit and the freeze without the marker, and
// are listed too (both non-freezable): PRELOAD_UNAVAILABLE (the
// preload-unavailable check after the dedup gate) and COVERAGE_MISS
// (*state.ErrCoverageMiss, which does carry the marker but lives outside
// internal/domain).
var UncommittedReasons = map[string]bool{
	// Freezable kinds (Validation / Conflict): the reasons that matter.
	domain.ErrReasonFilterCompilation:     true, // *ErrFilterCompilation (Validation)
	domain.ErrReasonClusterPolicyInvalid:  true, // *ErrClusterPolicyInvalid, errMetadataLimitsUnconfigured (Validation)
	domain.ErrReasonStaleClusterPolicy:    true, // *ErrStaleClusterPolicy (Conflict)
	domain.ErrReasonNumscriptCompileError: true, // unclassifiedNumscriptCompile (Validation)

	// Non-freezable kinds (Unavailable / Internal / ResourceExhausted).
	domain.ErrReasonBalanceNotPreloaded:           true,
	domain.ErrReasonInvalidApplyType:              true,
	domain.ErrReasonInvalidExecutionPlan:          true,
	domain.ErrReasonInvalidOrderType:              true,
	domain.ErrReasonMirrorV2LogIDGap:              true,
	domain.ErrReasonMirrorV2LogIDInvalid:          true,
	domain.ErrReasonNumscriptRuntime:              true,
	domain.ErrReasonSequenceExhausted:             true,
	domain.ErrReasonStaleInputsResolution:         true,
	domain.ErrReasonStaleProposal:                 true,
	domain.ErrReasonStorageOperation:              true,
	domain.ErrReasonTransactionStateInconsistent:  true,
	domain.ErrReasonVolumeNotMaterialized:         true,
	domain.ErrReasonClusterPolicyRevisionConflict: true,
	domain.ErrReasonCoverageMiss:                  true,
	domain.ErrReasonPreloadUnavailable:            true,
}
