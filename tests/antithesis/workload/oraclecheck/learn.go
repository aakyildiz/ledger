package oraclecheck

import (
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/tests/oracle"
)

// Lifted verbatim from M/validate.go.

// learnTxStamps folds the server-stamped transaction dates from a committed
// bulk's response logs into the just-advanced model state (LearnTxStamps): the
// created/revert transaction's timestamp and inserted_at, and the reverted
// original's reverted_at (the compensating transaction's timestamp). Runs in
// the same critical section that advanced the state — the safety condition
// LearnTxStamps documents.
func learnTxStamps(gs *oracle.GlobalState, bulk oracle.Bulk, logs []*commonpb.Log) {
	for i, req := range bulk.Requests {
		if i >= len(logs) {
			break
		}

		ledger := oracle.LedgerOf(req)
		entry := logs[i].GetPayload().GetApply().GetLog()
		data := entry.GetData()

		// The log's date is the one part of the stream the model cannot
		// derive; its id is derived, so it is checked against the server
		// rather than copied from it.
		gs.LearnLogDate(ledger, entry.GetId(), entry.GetDate())

		switch {
		case data.GetCreatedTransaction() != nil:
			tx := data.GetCreatedTransaction().GetTransaction()
			gs.LearnTxStamps(ledger, tx.GetId(), tx.GetTimestamp(), tx.GetInsertedAt(), nil)
		case data.GetRevertedTransaction() != nil:
			rt := data.GetRevertedTransaction()
			revTx := rt.GetRevertTransaction()
			gs.LearnTxStamps(ledger, revTx.GetId(), revTx.GetTimestamp(), revTx.GetInsertedAt(), nil)
			gs.LearnTxStamps(ledger, rt.GetRevertedTransactionId(), nil, nil, revTx.GetTimestamp())
		}

		// The global stream freezes the transaction record it announces, so its
		// stamps are learned first.
		gs.LearnLogSequence(ledger, entry.GetId(), logs[i].GetSequence())
	}
}
