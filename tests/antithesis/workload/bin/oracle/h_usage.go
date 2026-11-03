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
//	"usage": {"ledgers": {"<name>": {"id", "logCount", "txCount",
//	          "postingCount", "referenceCount", "revertCount"}}}
//
// "id" is the learned ledger id (0 if the commit that created the ledger was
// never observed with its id).

// usageLedger is one ledger's entry of the usage provider.
type usageLedger struct {
	ID             uint32 `json:"id"`
	LogCount       uint64 `json:"logCount"`
	TxCount        uint64 `json:"txCount"`
	PostingCount   uint64 `json:"postingCount"`
	ReferenceCount uint64 `json:"referenceCount"`
	RevertCount    uint64 `json:"revertCount"`
}

// usageProvided is the provider's answer.
type usageProvided struct {
	Ledgers map[string]usageLedger `json:"ledgers"`
}

func init() {
	oc.RegisterQuietProvider("usage", usageProvider)
}

func usageProvider(t *oc.Tracker) any {
	snap := t.Model()
	ids := t.LedgerIdentities()
	out := usageProvided{Ledgers: map[string]usageLedger{}}
	for _, name := range t.LedgerNames() {
		lc, exists := snap.Lifecycle(name)
		if !exists || lc.Deleted {
			continue
		}
		ls := snap.Ledger(name)
		u := match.ModelUsage(ls)
		out.Ledgers[name] = usageLedger{
			ID:             ids[name].ID,
			LogCount:       uint64(len(ls.LogRows())),
			TxCount:        uint64(ls.Txs().Len()),
			PostingCount:   u.Postings,
			ReferenceCount: u.References,
			RevertCount:    u.Reverts,
		}
	}

	return out
}
