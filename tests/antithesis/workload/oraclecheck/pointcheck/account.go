package pointcheck

import (
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	oc "github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck/match"
	"github.com/formancehq/ledger/v3/tests/oracle"
)

// ValidateGetAccount checks one linearizable GetAccount (M/reads.go runRead,
// M/validate.go validateAccountRead): the read is legal iff some candidate
// base holds exactly the served volume set (colours summed when collapsed)
// and exactly the served metadata for the address, both on the SAME base. A
// base where the ledger is not live explains only NotFound; NotFound is an
// empty account. A repeated bucket or an unparseable amount fails outright.
func ValidateGetAccount(c *oc.ReadCtx) []oc.Event {
	var req servicepb.GetAccountRequest
	if err := decode(c.Request, &req); err != nil {
		return []oc.Event{oc.Violation(EvAccountReadOutsideModel, map[string]any{"error": "undecodable request: " + err.Error()})}
	}
	ledger, addr, collapsed := req.GetLedger(), req.GetAddress(), req.GetCollapseColors()

	var (
		served     map[match.AssetColor]oracle.VolumePair
		meta       map[string]*commonpb.MetadataValue
		found      = true
		wellFormed = true
	)
	switch {
	case c.Outcome.OK:
		acct := &commonpb.Account{}
		if err := decode(c.Outcome.Response, acct); err != nil {
			wellFormed = false
		} else {
			served, wellFormed = match.AccountVolumeSet(acct)
			meta = acct.GetMetadata()
		}
	case c.Outcome.IsNotFound():
		found = false
	default:
		d := outcomeDetails(c.Outcome)
		d["ledger"], d["address"] = ledger, addr

		return []oc.Event{oc.Violation(EvGetAccountUnexpectedError, d)}
	}

	var evs []oc.Event
	// Coverage of the generator's negative-space probes, recomputed here.
	if absentEverywhere(c, ledger) {
		evs = append(evs, oc.Reached(EvAccountAbsentLedgerProbed, map[string]any{"ledger": ledger}))
	} else if match.LedgerIsLive(c.Snapshot, ledger) && !match.ModelKnowsAccount(c.Snapshot.Ledger(ledger), addr) {
		evs = append(evs, oc.Reached(EvAccountModelAbsentProbed, map[string]any{"ledger": ledger}))
	}

	if wellFormed && c.Matches(func(base oracle.GlobalState) bool {
		return AccountReadMatches(base, ledger, addr, served, meta, found, collapsed)
	}) {
		if collapsed && len(served) > 0 {
			evs = append(evs, oc.Reached(EvCollapsedAccountValidated, map[string]any{"ledger": ledger}))
		}

		return evs
	}

	return append(evs, oc.Violation(EvAccountReadOutsideModel, map[string]any{
		"ledger":     ledger,
		"address":    addr,
		"found":      found,
		"collapsed":  collapsed,
		"wellFormed": wellFormed,
		"maxTicket":  c.MaxTicket,
		"response":   rawOrNull(c.Outcome.Response),
	}))
}

// AccountReadMatches is the per-base verdict of ValidateGetAccount.
func AccountReadMatches(base oracle.GlobalState, ledger, addr string, served map[match.AssetColor]oracle.VolumePair,
	meta map[string]*commonpb.MetadataValue, found, collapsed bool,
) bool {
	ls, live := match.LiveLedgerState(base, ledger)
	if !live {
		return !found
	}

	return match.AccountVolumesMatch(ls, addr, served, collapsed) && match.MetadataMatches(ls, addr, meta)
}
