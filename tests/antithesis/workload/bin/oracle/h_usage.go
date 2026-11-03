package main

import (
	oc "github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck/match"
)

// Quiet provider "usage" (task 15). /quiet/begin carries, for every live
// tracked ledger, the exact usage counters the model derives
// (match.ModelUsage) plus its log and transaction counts. The oracle is
// quiescent at /quiet/begin (no in-flight write, pending drained), so the
// drained model is the cluster's state for these ledgers. The driver's
// stats_convergence hook compares every member's stale GetLedgerStats with
// these counts once a witness certifies that the member's usage fold has
// passed the source horizon. The provider makes no verdict and emits no event.
//
//	"usage": {"<name>": {"id", "logCount", "txCount", "postingCount",
//	          "referenceCount", "revertCount"}}
//
// (keyed by ledger name, as HG/protocol-fixtures/core/quiet-begin.json
// shows; counts are JSON numbers). "id" is the learned ledger id (0 if the
// commit that created the ledger was never observed with its id). Entries
// are maps so the JSON keys are sorted, as the journal replay re-encodes
// them.

func init() {
	oc.RegisterQuietProvider("usage", usageProvider)
}

func usageProvider(t *oc.Tracker) any {
	snap := t.Model()
	ids := t.LedgerIdentities()
	out := map[string]any{}
	for _, name := range t.LedgerNames() {
		lc, exists := snap.Lifecycle(name)
		if !exists || lc.Deleted {
			continue
		}
		ls := snap.Ledger(name)
		u := match.ModelUsage(ls)
		out[name] = map[string]any{
			"id":             ids[name].ID,
			"logCount":       uint64(len(ls.LogRows())),
			"txCount":        uint64(ls.Txs().Len()),
			"postingCount":   u.Postings,
			"referenceCount": u.References,
			"revertCount":    u.Reverts,
		}
	}

	return out
}
