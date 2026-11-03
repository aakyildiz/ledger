package preparedcheck

import (
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	oc "github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck/match"
	"github.com/formancehq/ledger/v3/tests/oracle"
)

// ValidateListPreparedQueries checks one linearizable ListPreparedQueries
// (M/prepared_queries_read.go runListPreparedQueries): legal iff some
// candidate base holds the ledger live with exactly these names, each with
// the same target and the byte-identical stored filter (no ordering
// contract). NotFound is legal iff some serialization has the ledger absent
// or deleted; any other error is a finding.
func ValidateListPreparedQueries(c *oc.ReadCtx) []oc.Event {
	var req servicepb.ListPreparedQueriesRequest
	if err := decode(c.Request, &req); err != nil {
		return []oc.Event{oc.Violation(EvListingOutsideModel, map[string]any{"error": "undecodable request: " + err.Error()})}
	}
	ledger := req.GetLedger()

	switch {
	case c.Outcome.OK:
	case c.Outcome.IsNotFound():
		return c.LedgerNotFoundExplained(ledger)
	default:
		d := outcomeDetails(c.Outcome)
		d["ledger"] = ledger

		return []oc.Event{oc.Violation(EvListUnexpectedError, d)}
	}

	var resp servicepb.ListPreparedQueriesResponse
	if err := decode(c.Outcome.Response, &resp); err != nil {
		return []oc.Event{oc.Violation(EvListingOutsideModel, map[string]any{"ledger": ledger, "error": "undecodable response: " + err.Error()})}
	}
	served := resp.GetQueries()
	if c.Matches(func(base oracle.GlobalState) bool {
		ls, live := match.LiveLedgerState(base, ledger)

		return live && match.RegistryMatches(ls, served)
	}) {
		return nil
	}

	return []oc.Event{oc.Violation(EvListingOutsideModel, map[string]any{
		"ledger":       ledger,
		"rows":         len(served),
		"serverQuery":  describePreparedQueries(served),
		"modelQueries": modelPreparedQueries(c.T.Model(), ledger),
		"maxTicket":    c.MaxTicket,
	})}
}
