package match

import (
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/tests/oracle"
)

// StatsUsage is the subset of the usagestore counters the model derives exactly.
// Each only ever grows along the audit chain, so the fully-folded value bounds a
// served one above however far the projection lags.
type StatsUsage struct {
	Postings   uint64
	References uint64
	Reverts    uint64
}

// ModelUsage counts what the usagebuilder counts (usagestore/keys.go): postings
// summed over every created and revert transaction, creates carrying a
// reference, and revert orders.
func ModelUsage(ls oracle.LedgerState) StatsUsage {
	var u StatsUsage

	txs := ls.Txs()
	for i := range txs.Len() {
		rec := txs.Get(i)

		u.Postings += uint64(len(rec.Postings()))

		if rec.Reference() != "" {
			u.References++
		}

		if rec.RevertsTransaction() != 0 {
			u.Reverts++
		}
	}

	return u
}

// ServedStatsUsage reads the usage counters of a served LedgerStats.
func ServedStatsUsage(stats *commonpb.LedgerStats) StatsUsage {
	return StatsUsage{
		Postings:   stats.GetPostingCount(),
		References: stats.GetReferenceCount(),
		Reverts:    stats.GetRevertCount(),
	}
}

// StatsBoundsMatch reports whether a served LedgerStats' boundaries are
// exactly ls's: TransactionCount and LogCount are next_id-1 off one boundaries
// read, so the pair is exactly some base's.
func StatsBoundsMatch(ls oracle.LedgerState, stats *commonpb.LedgerStats) bool {
	return stats.GetTransactionCount() == uint64(ls.Txs().Len()) &&
		stats.GetLogCount() == uint64(len(ls.LogDates()))
}

// StatsUsageWithin reports whether served is bounded above by ls's fully
// folded counters. The usagestore snapshot is taken after the boundary handle
// and its builder folds with no barrier, so which prefix it holds is unknown;
// every counter only grows along the chain, so an over-count (a double-applied
// bulk) exceeds every base whatever the lag.
func StatsUsageWithin(ls oracle.LedgerState, served StatsUsage) bool {
	m := ModelUsage(ls)

	return served.Postings <= m.Postings &&
		served.References <= m.References &&
		served.Reverts <= m.Reverts
}

// StatsMatches is the per-base verdict on a GetLedgerStats answer: no
// numscript execution counted (the tracked workload sends no script), exact
// boundaries, and usage counters bounded by the model.
//
// Tracker-bound part (candidate walk, coverage on catching up): task 05.
func StatsMatches(ls oracle.LedgerState, stats *commonpb.LedgerStats) bool {
	return stats.GetNumscriptExecutionCount() == 0 &&
		StatsBoundsMatch(ls, stats) &&
		StatsUsageWithin(ls, ServedStatsUsage(stats))
}
