package pointcheck

import (
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	oc "github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck/match"
	"github.com/formancehq/ledger/v3/tests/oracle"
)

// ValidateGetLedgerStats checks one linearizable GetLedgerStats (M/stats.go):
// no numscript execution counted (tracked ledgers never run scripts), the
// boundary pair (transactionCount, logCount) exactly some base's, and the
// usage counters bounded above by some (possibly different) base's fully
// folded counters — the usage store folds asynchronously with no barrier.
func ValidateGetLedgerStats(c *oc.ReadCtx) []oc.Event {
	var req servicepb.GetLedgerStatsRequest
	if err := decode(c.Request, &req); err != nil {
		return []oc.Event{oc.Violation(EvStatsBoundsOutsideModel, map[string]any{"error": "undecodable request: " + err.Error()})}
	}
	ledger := req.GetLedger()
	absent := absentEverywhere(c, ledger)

	switch {
	case c.Outcome.IsNotFound():
		if absent {
			return []oc.Event{oc.Reached(EvStatsAbsentNotFound, map[string]any{"ledger": ledger})}
		}

		return c.LedgerNotFoundExplained(ledger)
	case !c.Outcome.OK:
		d := outcomeDetails(c.Outcome)
		d["ledger"], d["absent"] = ledger, absent

		return []oc.Event{oc.Violation(EvGetLedgerStatsUnexpectedErr, d)}
	}

	if absent {
		return []oc.Event{oc.Violation(EvStatsServedOutsideFleet, map[string]any{"ledger": ledger})}
	}

	stats := &commonpb.LedgerStats{}
	if err := decode(c.Outcome.Response, stats); err != nil {
		return []oc.Event{oc.Violation(EvStatsBoundsOutsideModel, map[string]any{"ledger": ledger, "error": "undecodable response: " + err.Error()})}
	}

	return StatsEvents(c, ledger, stats)
}

// StatsEvents is the served-stats half of ValidateGetLedgerStats.
func StatsEvents(c *oc.ReadCtx, ledger string, stats *commonpb.LedgerStats) []oc.Event {
	if stats.GetNumscriptExecutionCount() != 0 {
		return []oc.Event{oc.Violation(EvStatsCountedNumscript, map[string]any{
			"ledger": ledger, "count": stats.GetNumscriptExecutionCount(),
		})}
	}

	committed := c.T.Model().Ledger(ledger)
	if !c.Matches(func(base oracle.GlobalState) bool {
		return match.StatsBoundsMatch(base.Ledger(ledger), stats)
	}) {
		return []oc.Event{oc.Violation(EvStatsBoundsOutsideModel, map[string]any{
			"ledger":         ledger,
			"serverTxCount":  stats.GetTransactionCount(),
			"serverLogCount": stats.GetLogCount(),
			"modelTxCount":   committed.Txs().Len(),
			"modelLogCount":  len(committed.LogDates()),
			"maxTicket":      c.MaxTicket,
		})}
	}

	served := match.ServedStatsUsage(stats)
	modelMin := match.ModelUsage(committed)
	if !c.Matches(func(base oracle.GlobalState) bool {
		return match.StatsUsageWithin(base.Ledger(ledger), served)
	}) {
		return []oc.Event{oc.Violation(EvStatsCounterAboveModel, map[string]any{
			"ledger":           ledger,
			"serverPostings":   served.Postings,
			"serverReferences": served.References,
			"serverReverts":    served.Reverts,
			"modelPostings":    modelMin.Postings,
			"modelReferences":  modelMin.References,
			"modelReverts":     modelMin.Reverts,
			"maxTicket":        c.MaxTicket,
		})}
	}

	var evs []oc.Event
	// The usage builder folded at least everything the drained model holds.
	if served == modelMin {
		evs = append(evs, oc.Reached(EvStatsUsageCaughtUp, map[string]any{"ledger": ledger}))
	}

	return append(evs, oc.Reached(EvStatsValidated, map[string]any{
		"ledger": ledger, "txCount": stats.GetTransactionCount(), "logCount": stats.GetLogCount(),
	}))
}
