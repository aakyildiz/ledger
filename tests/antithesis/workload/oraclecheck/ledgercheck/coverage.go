package ledgercheck

import (
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	oc "github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck"
	"github.com/formancehq/ledger/v3/tests/oracle"
)

// extKey is the family's Tracker side table key.
const extKey = "ledgers"

// maxPendingDeleted caps the deleted names the deletion coverage probe
// remembers (the side table must not grow for the whole run).
const maxPendingDeleted = 256

// sideTable is the observation state the coverage hooks keep
// (M/checker.go pendingDeleted / pendingPromoted).
type sideTable struct {
	pendingDeleted  map[string]struct{}
	pendingPromoted map[string]struct{}
}

func table(t *oc.Tracker) *sideTable {
	return t.Ext(extKey, func() any {
		return &sideTable{pendingDeleted: map[string]struct{}{}, pendingPromoted: map[string]struct{}{}}
	}).(*sideTable)
}

// DrainHook runs after every agreed commit (M/validate.go validateBulkSuccess
// lifecycle bookkeeping + M/apply_surface.go noteApplyCoverage): it remembers
// deleted and promoted ledgers, reports the promotion probe when a promoted
// ledger commits an ordinary business write, and reports the apply-surface
// probes this family's orders reach. Only satisfied probes are emitted; the
// service catalogues every probe unhit at startup.
func DrainHook(t *oc.Tracker, rec oc.WriteRecord, resp *servicepb.ApplyResponse, _, _ oracle.GlobalState) []oc.Event {
	st := table(t)
	logs := resp.GetLogs()
	var evs []oc.Event
	for i, req := range rec.Requests {
		var log *commonpb.Log
		if i < len(logs) {
			log = logs[i]
		}
		switch {
		case req.GetDeleteLedger() != nil:
			// Bounded: the probe needs one recreate of a remembered name, and
			// the rule-owned deletion probes delete a reserved ledger per run
			// that nothing ever recreates.
			if len(st.pendingDeleted) < maxPendingDeleted {
				st.pendingDeleted[req.GetDeleteLedger().GetName()] = struct{}{}
			}
		case req.GetPromoteLedger() != nil:
			st.pendingPromoted[req.GetPromoteLedger().GetLedger()] = struct{}{}
		case req.GetApply() != nil && log != nil && isSuccessfulBusinessWrite(req, log):
			ledger := req.GetApply().GetLedger()
			if _, pending := st.pendingPromoted[ledger]; pending {
				evs = append(evs, oc.Raw(EvLcCovPromotion, true, map[string]any{"ledger": ledger}))
				delete(st.pendingPromoted, ledger)
			}
		}
	}

	return append(evs, applyCoverage(rec.Requests, logs)...)
}

// isSuccessfulBusinessWrite reports whether a committed Apply proved that a
// promoted ledger accepts ordinary business traffic (M/validate.go). Mirror-safe
// configuration actions and skipped orders do not.
func isSuccessfulBusinessWrite(req *servicepb.Request, log *commonpb.Log) bool {
	action := req.GetApply().GetAction()
	if action == nil || log.GetPayload().GetApply().GetLog().GetData().GetOrderSkipped() != nil {
		return false
	}
	switch action.GetData().(type) {
	case *servicepb.LedgerAction_CreateTransaction,
		*servicepb.LedgerAction_RevertTransaction,
		*servicepb.LedgerAction_AddMetadata,
		*servicepb.LedgerAction_DeleteMetadata:
		return true
	default:
		return false
	}
}

// applyCoverage decides the apply-surface probes of one validated commit
// (M/apply_surface.go applyCoverageHits, restricted to the chart and
// enforcement orders): a skipped account-type order followed by an executed
// order, and each form × mode of a default-enforcement setter. The response
// log at index i pairs with request i.
func applyCoverage(reqs []*servicepb.Request, logs []*commonpb.Log) []oc.Event {
	skipped := func(i int) *commonpb.OrderSkippedLog {
		if i >= len(logs) {
			return nil
		}

		return logs[i].GetPayload().GetApply().GetLog().GetData().GetOrderSkipped()
	}
	hits := map[oc.EventID]bool{}
	for i, req := range reqs {
		if s := skipped(i); s != nil && i+1 < len(reqs) && i+1 < len(logs) && skipped(i+1) == nil {
			switch s.GetReason() {
			case commonpb.ErrorReason_ERROR_REASON_ACCOUNT_TYPE_ALREADY_EXISTS:
				hits[EvLcCovSkipTypeExists] = true
			case commonpb.ErrorReason_ERROR_REASON_ACCOUNT_TYPE_NOT_FOUND:
				hits[EvLcCovSkipTypeNotFound] = true
			}
		}
		if mode := requestedEnforcementMode(req); mode != nil {
			nested := req.GetApply() != nil
			strict := *mode == commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_STRICT
			switch {
			case !nested && strict:
				hits[EvLcCovTopLevelStrict] = true
			case !nested:
				hits[EvLcCovTopLevelAudit] = true
			case strict:
				hits[EvLcCovLedgerActionStrict] = true
			default:
				hits[EvLcCovLedgerActionAudit] = true
			}
		}
	}
	var evs []oc.Event
	for _, id := range applyCoverageIDs {
		if hits[id] {
			evs = append(evs, oc.Raw(id, true, map[string]any{}))
		}
	}

	return evs
}

// applyCoverageIDs fixes the emission order.
var applyCoverageIDs = []oc.EventID{
	EvLcCovSkipTypeExists, EvLcCovSkipTypeNotFound,
	EvLcCovTopLevelStrict, EvLcCovTopLevelAudit, EvLcCovLedgerActionStrict, EvLcCovLedgerActionAudit,
}

// FailureHook runs after an explained definitive failure (M/validate.go
// validateFailure coverage): LEDGER_DELETED on a create of a name whose
// deletion committed proves the reserved-name rejection; VALIDATION on a bulk
// carrying a skippable reason its action does not allow proves the admission
// refusal of an invalid opt-in.
func FailureHook(t *oc.Tracker, rec oc.WriteRecord, _ oracle.ApplyResult, reason string) []oc.Event {
	var evs []oc.Event
	if reason == "LEDGER_DELETED" {
		st := table(t)
		for _, req := range rec.Requests {
			name := req.GetCreateLedger().GetName()
			if name == "" {
				continue
			}
			if _, pending := st.pendingDeleted[name]; pending {
				evs = append(evs, oc.Raw(EvLcCovDeletion, true, map[string]any{"ledger": name}))
				delete(st.pendingDeleted, name)
			}
		}
	}
	if reason == "VALIDATION" && hasInvalidSkippableReason(rec.Requests) {
		evs = append(evs, oc.Raw(EvLcCovInvalidSkip, true, map[string]any{"wid": rec.Wid}))
	}

	return evs
}

// hasInvalidSkippableReason reports a request whose skippable reasons are not
// the one its action allows (M/apply_surface.go bulkHasInvalidSkippableReason).
func hasInvalidSkippableReason(reqs []*servicepb.Request) bool {
	for _, req := range reqs {
		reasons := req.GetApply().GetSkippableReasons()
		if len(reasons) == 0 {
			continue
		}
		allowed, ok := allowedSkippableReason(req)
		for _, r := range reasons {
			if !ok || r != allowed {
				return true
			}
		}
	}

	return false
}

// allowedSkippableReason is the model's independent copy of the public
// per-action whitelist (M/apply_surface.go).
func allowedSkippableReason(req *servicepb.Request) (commonpb.ErrorReason, bool) {
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

// requestedEnforcementMode returns the mode a request sets through either
// request form, or nil (M/apply_surface.go).
func requestedEnforcementMode(req *servicepb.Request) *commonpb.ChartEnforcementMode {
	if setter := req.GetSetDefaultEnforcementMode(); setter != nil {
		mode := setter.GetEnforcementMode()

		return &mode
	}
	if setter := req.GetApply().GetAction().GetSetDefaultEnforcementMode(); setter != nil {
		mode := setter.GetEnforcementMode()

		return &mode
	}

	return nil
}
