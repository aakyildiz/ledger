// Package applysurfacecheck is task 02's oracle library: the apply-surface
// coverage of transaction orders (skippable opt-ins, lifted from
// M/apply_surface.go) and the rejection coverage of the transaction failure
// branches (M/validate.go:549-564). It is assertion-free: every probe is a
// returned oraclecheck.Event; bin/oracle/h_tx.go asserts them.
package applysurfacecheck

import (
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	oc "github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck"
	"github.com/formancehq/ledger/v3/tests/oracle"
)

// Event ids. The Raw probe is registered with RegisterCoverage (no asserter);
// every other id has exactly one asserter in bin/oracle/h_tx.go.
const (
	// EvSkipRefConflictContinued: an order opted into
	// TRANSACTION_REFERENCE_CONFLICT produced an OrderSkipped log and the next
	// order of the same bulk committed (M/apply_surface.go applyCoverageHits).
	EvSkipRefConflictContinued oc.EventID = "oracle: coverage Apply skipped TRANSACTION_REFERENCE_CONFLICT and continued"

	EvInsufficientFundsExercised oc.EventID = "oracle: insufficient-funds rejection exercised"
	EvReferenceConflictExercised oc.EventID = "oracle: reference-conflict rejection exercised"
	EvVolumeOverflowExercised    oc.EventID = "oracle: volume-overflow rejection exercised"
	EvValidationExercised        oc.EventID = "oracle: validation rejection exercised"
	EvTransientNonZeroExercised  oc.EventID = "oracle: transient non-zero rejection exercised"
)

// Hook names (hooks run in sorted name order).
const (
	DrainHookName   = "02-tx-apply-surface"
	FailureHookName = "02-tx-rejection-coverage"
)

func init() {
	oc.RegisterCoverage(EvSkipRefConflictContinued)
	oc.RegisterDrainHook(DrainHookName, ApplySurfaceDrain)
	oc.RegisterFailureHook(FailureHookName, RejectionCoverage)
}

// AllowedSkippableReason is the model's independent copy of the public
// per-action whitelist (M/apply_surface.go allowedSkippableReason).
func AllowedSkippableReason(req *servicepb.Request) (commonpb.ErrorReason, bool) {
	switch req.GetApply().GetAction().GetData().(type) {
	case *servicepb.LedgerAction_CreateTransaction:
		return commonpb.ErrorReason_ERROR_REASON_TRANSACTION_REFERENCE_CONFLICT, true
	case *servicepb.LedgerAction_RevertTransaction:
		return commonpb.ErrorReason_ERROR_REASON_TRANSACTION_ALREADY_REVERTED, true
	case *servicepb.LedgerAction_DeleteMetadata:
		return commonpb.ErrorReason_ERROR_REASON_METADATA_NOT_FOUND, true
	case *servicepb.LedgerAction_AddAccountType:
		return commonpb.ErrorReason_ERROR_REASON_ACCOUNT_TYPE_ALREADY_EXISTS, true
	case *servicepb.LedgerAction_RemoveAccountType:
		return commonpb.ErrorReason_ERROR_REASON_ACCOUNT_TYPE_NOT_FOUND, true
	default:
		return commonpb.ErrorReason_ERROR_REASON_UNSPECIFIED, false
	}
}

// GeneratedSkippableReason maps an Apply action to the skip reason a
// generator attaches (M/apply_surface.go generatedSkippableReason): the
// whitelisted reason, or, for mode setters, a deliberately disallowed one
// (task 04 generates those).
func GeneratedSkippableReason(req *servicepb.Request) (commonpb.ErrorReason, bool) {
	if reason, ok := AllowedSkippableReason(req); ok {
		return reason, true
	}
	if req.GetApply().GetAction().GetSetDefaultEnforcementMode() != nil {
		return commonpb.ErrorReason_ERROR_REASON_ACCOUNT_TYPE_NOT_FOUND, true
	}

	return commonpb.ErrorReason_ERROR_REASON_UNSPECIFIED, false
}

// OptsInto reports whether req carries reason among its skippable reasons.
func OptsInto(req *servicepb.Request, reason commonpb.ErrorReason) bool {
	for _, r := range req.GetApply().GetSkippableReasons() {
		if r == reason {
			return true
		}
	}

	return false
}

// SkippedAndContinued decides the probe for one validated commit: some order
// opted into reason was skipped (OrderSkipped with that reason) and the next
// order of the bulk committed (not skipped). Request-based: the opt-in is
// read from the requests, the outcome from the model result that the commit
// check already agreed with.
func SkippedAndContinued(bulk oracle.Bulk, res oracle.ApplyResult, reason commonpb.ErrorReason) bool {
	for i, order := range res.Orders {
		if i >= len(bulk.Requests) || order.Skipped == nil || !OptsInto(bulk.Requests[i], reason) {
			continue
		}
		if skippedReason(order.Skipped) != reason {
			continue
		}
		if i+1 < len(res.Orders) && res.Orders[i+1].Skipped == nil {
			return true
		}
	}

	return false
}

func skippedReason(s *commonpb.OrderSkippedLog) commonpb.ErrorReason {
	if s.GetDetails().GetTransactionReferenceConflict() != nil {
		return commonpb.ErrorReason_ERROR_REASON_TRANSACTION_REFERENCE_CONFLICT
	}

	return commonpb.ErrorReason_ERROR_REASON_UNSPECIFIED
}

func bulkOptsInto(bulk oracle.Bulk, reason commonpb.ErrorReason) bool {
	for _, r := range bulk.Requests {
		if OptsInto(r, reason) {
			return true
		}
	}

	return false
}

// ApplySurfaceDrain runs after a commit passed every check (DESIGN §5.2
// DrainHook). It evaluates the REFERENCE_CONFLICT "and continued" probe on
// every committed bulk that carries that opt-in (selection alone never proves
// the skipped outcome or the execution of the following order).
func ApplySurfaceDrain(_ *oc.Tracker, rec oc.WriteRecord, _ *servicepb.ApplyResponse, before, _ oracle.GlobalState) []oc.Event {
	reason := commonpb.ErrorReason_ERROR_REASON_TRANSACTION_REFERENCE_CONFLICT
	if !bulkOptsInto(rec.Bulk, reason) {
		return nil
	}
	res := before.Apply(rec.Bulk)
	if !res.OK || res.Replayed {
		return nil
	}
	hit := SkippedAndContinued(rec.Bulk, res, reason)

	return []oc.Event{oc.Raw(EvSkipRefConflictContinued, hit, map[string]any{"wid": rec.Wid, "orders": len(rec.Bulk.Requests)})}
}

// RejectionCoverage fires one Reachable per deliberately-triggered rejection
// branch once a failure was explained (M/validate.go:549-564; the transient
// branch is new). If one stops firing, the generator stopped emitting the
// shape.
func RejectionCoverage(_ *oc.Tracker, rec oc.WriteRecord, _ oracle.ApplyResult, reason string) []oc.Event {
	d := map[string]any{"wid": rec.Wid, "family": rec.Family}
	switch reason {
	case "INSUFFICIENT_FUNDS":
		return []oc.Event{oc.Reached(EvInsufficientFundsExercised, d)}
	case "TRANSACTION_REFERENCE_CONFLICT":
		return []oc.Event{oc.Reached(EvReferenceConflictExercised, d)}
	case "VOLUME_OVERFLOW":
		return []oc.Event{oc.Reached(EvVolumeOverflowExercised, d)}
	case "VALIDATION":
		return []oc.Event{oc.Reached(EvValidationExercised, d)}
	case "TRANSIENT_ACCOUNT_NON_ZERO":
		return []oc.Event{oc.Reached(EvTransientNonZeroExercised, d)}
	}

	return nil
}
