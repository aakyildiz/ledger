package match

import (
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/tests/oracle"
	"google.golang.org/grpc/codes"
)

// Checkpoint data is compared only with the drained state captured at creation.
// Live candidate states may explain deletion, never different business data.
func CheckpointAccountReadMatches(state oracle.GlobalState, ledger, address string, account *commonpb.Account, found, collapsed bool) bool {
	ls := state.Ledger(ledger)
	if !found {
		return !ModelKnowsAccount(ls, address)
	}

	return account != nil && account.GetAddress() == address && AccountMatchesCollapsed(ls, address, account, collapsed)
}

func CheckpointTransactionReadMatches(state oracle.GlobalState, ledger string, id uint64, transaction *commonpb.Transaction, found bool) bool {
	txs := state.Ledger(ledger).Txs()
	if id == 0 || id > uint64(txs.Len()) {
		return !found
	}

	return found && transaction != nil && TxRecordMatches(txs.Get(int(id-1)), transaction)
}

func PredictedCheckpointLedgerMatches(state oracle.GlobalState, ledger string, info *commonpb.LedgerInfo) bool {
	return info.GetName() == ledger && LedgerInfoMatches(state, info)
}

// CheckpointAccountWindow is the unfiltered accounts page a frozen checkpoint
// must serve, with the verdict on whether a further row is waiting. The frozen
// state cannot move, so both are exact.
func CheckpointAccountWindow(ls oracle.LedgerState, cursor string, pageSize int, reverse bool) ([]string, CursorMore) {
	want := AccountWindow(ls, nil, cursor, pageSize+1, reverse)
	if len(want) > pageSize {
		return want[:pageSize], CursorRequired
	}

	return want, CursorForbidden
}

// CheckpointAggregateMatches compares an aggregate served from a frozen
// checkpoint against that checkpoint's own state. The frozen state cannot move,
// so the fold is exact; concrete reports whether the answer carried any total,
// which is what makes the read coverage-worthy.
func CheckpointAggregateMatches(ls oracle.LedgerState, opts AggOptions, res *commonpb.AggregateResult, served bool) (matches, concrete bool) {
	if !served {
		return false, false
	}

	if len(opts.GroupByPrefixes) > 0 {
		if len(res.GetVolumes()) != 0 {
			return false, false
		}

		groups, ok := ServerAggregateGroups(res)
		if !ok {
			return false, false
		}

		for _, g := range groups {
			if len(g.Sums) > 0 {
				concrete = true
			}
		}

		return AggGroupsEqual(ModelAggregateGroups(ls, nil, opts), groups), concrete
	}

	if len(res.GetGroups()) != 0 {
		return false, false
	}

	sums, ok := ServerAggregate(res)
	if !ok {
		return false, false
	}

	return AggEqual(ModelAggregate(ls, nil, opts), sums), len(sums) > 0
}

// DescribeCheckpointAggregate renders whichever arm the request asked for.
func DescribeCheckpointAggregate(opts AggOptions, res *commonpb.AggregateResult) string {
	if len(opts.GroupByPrefixes) > 0 {
		groups, _ := ServerAggregateGroups(res)

		return RenderAggGroups(groups)
	}

	sums, _ := ServerAggregate(res)

	return RenderAgg(sums)
}

// CheckpointNotFound reports whether err is the server's NotFound for a
// checkpoint read. The server maps commonpb.NotFoundError to a typed gRPC
// status without ErrorInfo details (server.go), so the code alone decides.
func CheckpointNotFound(err RPCError) bool { return err.Code == codes.NotFound }

// CandidateMatcher reports whether some candidate base of the read satisfies
// the predicate — oraclecheck.ReadCtx.Matches (MatchesModel at the read's
// frontier).
type CandidateMatcher func(func(oracle.GlobalState) bool) bool

// CheckpointReadOutcomeMatches never substitutes current business data for the
// frozen result, including expected entity absence. frozenMatches is the
// caller's verdict on the served data against the frozen snapshot of
// checkpoint id; candidate states can explain only a missing checkpoint.
//
// Tracker-bound part (finding the frozen snapshot, reporting): task 09.
func CheckpointReadOutcomeMatches(id uint64, frozenMatches bool, err RPCError, candidates CandidateMatcher) bool {
	if err.OK() {
		return frozenMatches
	}
	if !CheckpointNotFound(err) {
		return false
	}
	if frozenMatches {
		return true
	}

	return candidates(func(base oracle.GlobalState) bool {
		return !base.QueryCheckpointExists(id)
	})
}

// CheckpointLedgerReadOutcomeMatches is CheckpointReadOutcomeMatches for a
// ledger-scoped read: a NotFound is equally explained by the ledger being
// deleted, which no frozen checkpoint keeps serving.
//
// Tracker-bound part: task 09.
func CheckpointLedgerReadOutcomeMatches(id uint64, ledger string, frozenMatches bool, err RPCError, candidates CandidateMatcher) bool {
	if CheckpointReadOutcomeMatches(id, frozenMatches, err, candidates) {
		return true
	}

	if ledger == "" || !CheckpointNotFound(err) {
		return false
	}

	return candidates(func(base oracle.GlobalState) bool {
		return !LedgerIsLive(base, ledger)
	})
}
