package oraclecheck

import (
	"github.com/formancehq/ledger/v3/tests/oracle"
)

// validateFailure accepts the observed failure of failedBulk iff some
// candidate base reproduces the observed reason when failedBulk is applied to
// it (lifted from M/validate.go). It returns the explaining candidate's
// ApplyResult — the first in the search's deterministic ticket order — and
// whether one explained it. Unexplained, it returns the finding with the
// model's own reason on the drained state.
func (t *Tracker) validateFailure(maxTicket uint64, failedBulk oracle.Bulk, reason string) (oracle.ApplyResult, bool, []Event) {
	var explaining oracle.ApplyResult

	matched := false
	t.walkCandidateStates(maxTicket, func(base oracle.GlobalState) bool {
		res := base.Apply(failedBulk)
		if !res.OK && res.Reason == reason {
			explaining = res
			matched = true

			return true
		}

		return false
	}, nil)

	if matched {
		return explaining, true, nil
	}

	mres := t.modelState.Apply(failedBulk)
	modelReason := mres.Reason
	if mres.OK {
		modelReason = "OK"
	}

	postings, modelTypes, buffered := t.failureDiag(failedBulk, maxTicket)

	return oracle.ApplyResult{}, false, []Event{Violation(EvFailureUnexplained, map[string]any{
		"ledgers":     bulkLedgers(failedBulk),
		"error":       reason,
		"kinds":       requestKinds(failedBulk),
		"postings":    postings,
		"modelReason": modelReason,
		"modelTypes":  modelTypes,
		"buffered":    buffered,
		"maxTicket":   maxTicket,
	})}
}
