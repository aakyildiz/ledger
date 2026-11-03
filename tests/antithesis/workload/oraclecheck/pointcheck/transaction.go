package pointcheck

import (
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	oc "github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck/match"
	"github.com/formancehq/ledger/v3/tests/oracle"
)

// ValidateGetTransaction checks one linearizable GetTransaction
// (M/reads.go runTransactionRead, M/validate.go validateTransactionRead):
// legal iff some candidate base agrees at that id — a non-live ledger or an id
// beyond the base's transaction count explains only NotFound; otherwise the
// read must be found and match the base's record in full (TxRecordMatches:
// id, reference, revert relations, postings, metadata, learned stamps and the
// post-commit volumes snapshot — the sentinel's strength, W/internal/sentinel.go).
func ValidateGetTransaction(c *oc.ReadCtx) []oc.Event {
	var req servicepb.GetTransactionRequest
	if err := decode(c.Request, &req); err != nil {
		return []oc.Event{oc.Violation(EvTxReadOutsideModel, map[string]any{"error": "undecodable request: " + err.Error()})}
	}
	ledger, id := req.GetLedger(), req.GetTransactionId()

	var (
		serverTx *commonpb.Transaction
		found    = true
	)
	switch {
	case c.Outcome.OK:
		resp := &servicepb.GetTransactionResponse{}
		if err := decode(c.Outcome.Response, resp); err != nil {
			return []oc.Event{oc.Violation(EvTxReadOutsideModel, map[string]any{
				"ledger": ledger, "id": id, "error": "undecodable response: " + err.Error(),
			})}
		}
		serverTx = resp.GetTransaction()
	case c.Outcome.IsNotFound():
		found = false
	default:
		d := outcomeDetails(c.Outcome)
		d["ledger"], d["id"] = ledger, id

		return []oc.Event{oc.Violation(EvGetTransactionUnexpectedErr, d)}
	}

	var evs []oc.Event
	if absentEverywhere(c, ledger) {
		evs = append(evs, oc.Reached(EvTxAbsentLedgerProbed, map[string]any{"ledger": ledger}))
	}

	if c.Matches(func(base oracle.GlobalState) bool {
		return TransactionReadMatches(base, ledger, id, serverTx, found)
	}) {
		return evs
	}

	return append(evs, oc.Violation(EvTxReadOutsideModel, map[string]any{
		"ledger":        ledger,
		"id":            id,
		"found":         found,
		"maxTicket":     c.MaxTicket,
		"serverRef":     serverTx.GetReference(),
		"serverRev":     serverTx.GetReverted(),
		"serverRevBy":   serverTx.GetRevertedByTransaction(),
		"serverReverts": serverTx.GetRevertsTransaction(),
		"response":      rawOrNull(c.Outcome.Response),
	}))
}

// TransactionReadMatches is the per-base verdict of ValidateGetTransaction.
func TransactionReadMatches(base oracle.GlobalState, ledger string, id uint64, serverTx *commonpb.Transaction, found bool) bool {
	ls, live := match.LiveLedgerState(base, ledger)
	if !live {
		return !found
	}
	txs := ls.Txs()
	if id == 0 || id > uint64(txs.Len()) {
		return !found
	}
	if !found || serverTx == nil {
		return false
	}

	return match.TxRecordMatches(txs.Get(int(id-1)), serverTx)
}
