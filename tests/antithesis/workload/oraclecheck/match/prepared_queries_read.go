package match

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"slices"
	"sort"
	"strings"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/tests/oracle"
	"github.com/holiman/uint256"
	"google.golang.org/grpc/codes"
)

// RegistryMatches compares a served listing against one base's registry. The
// listing is unordered by contract, so it is compared as a keyed set; the
// filter comparison is exact (EqualVT), which is what makes a stale or
// half-applied update visible.
func RegistryMatches(ls oracle.LedgerState, served []*commonpb.PreparedQuery) bool {
	if len(served) != ls.PreparedQueries().Len() {
		return false
	}

	seen := make(map[string]struct{}, len(served))
	for _, q := range served {
		if _, duplicate := seen[q.GetName()]; duplicate {
			return false
		}
		seen[q.GetName()] = struct{}{}

		stored, ok := ls.PreparedQuery(q.GetName())
		if !ok || stored.GetTarget() != q.GetTarget() || !stored.GetFilter().EqualVT(q.GetFilter()) {
			return false
		}
	}

	return true
}

// PqErrKind buckets an ExecutePreparedQuery failure. Everything but PqErrOther
// is a documented outcome the model can predict; PqErrOther is a finding unless
// the call was the deliberate target-misuse probe.
type PqErrKind int

const (
	PqErrNone PqErrKind = iota
	PqErrNotFound
	PqErrIndex
	PqErrCompilation
	PqErrAggregateTarget
	PqErrLedgerNotFound
	PqErrOther
)

// ClassifyPreparedExecError buckets err by stable wire signals, never by
// message text. Query-mode validation has no ErrorInfo reason and is identified
// by its InvalidArgument status code.
func ClassifyPreparedExecError(err RPCError) PqErrKind {
	switch {
	case err.OK():
		return PqErrNone
	case err.HasReason("PREPARED_QUERY_NOT_FOUND"):
		return PqErrNotFound
	case IsIndexNotFound(err), IsIndexNotReady(err):
		return PqErrIndex
	case err.HasReason("FILTER_COMPILATION_ERROR"):
		return PqErrCompilation
	case err.Code == codes.InvalidArgument:
		return PqErrAggregateTarget
	case err.HasReason("LEDGER_NOT_FOUND"):
		return PqErrLedgerNotFound
	default:
		return PqErrOther
	}
}

// AsIndexedErrKind projects a prepared-query failure onto the indexed-query
// vocabulary, so IndexedQueryOutcomeLegal — which already encodes the index
// lifecycle and retype-window rules — can be reused verbatim.
func AsIndexedErrKind(kind PqErrKind) IndexedErrKind {
	switch kind {
	case PqErrIndex:
		return IndexedErrNotReady
	case PqErrCompilation:
		return IndexedErrCompilation
	default:
		return IndexedErrNone
	}
}

// PreparedCall carries one ExecutePreparedQuery invocation's inputs and outcome
// through validation, keeping the validators' signatures readable.
type PreparedCall struct {
	Ledger      string
	Name        string
	Params      PreparedParams
	Complete    bool
	PageSize    int
	Reverse     bool
	ErrKind     PqErrKind
	Err         RPCError
	WrongResult bool
}

// LastPageKey renders the model-side after-key of a page: the last account
// address, transaction id, or log id it returned. Empty when the page is empty
// — there is nothing to page past.
func LastPageKey(cur *commonpb.PreparedQueryCursor) []byte {
	if accts := cur.GetAccountData(); len(accts) > 0 {
		return []byte(accts[len(accts)-1].GetAddress())
	}

	if txs := cur.GetTransactionData(); len(txs) > 0 {
		return Uint64EntityKey(txs[len(txs)-1].GetId())
	}

	if ids := ServerLogIDs(cur.GetLogData()); len(ids) > 0 {
		return Uint64EntityKey(ids[len(ids)-1])
	}

	return nil
}

func Uint64EntityKey(v uint64) []byte {
	key := make([]byte, 8)
	binary.BigEndian.PutUint64(key, v)

	return key
}

// PreparedLedgerOutcomeLegal handles lifecycle outcomes before validators read
// the ledger's retained model state. Deletion keeps that state for recreation,
// but ledger-scoped reads must return NotFound while the lifecycle is absent or
// deleted and must never serve the retained snapshot.
func PreparedLedgerOutcomeLegal(base oracle.GlobalState, call PreparedCall) (handled, legal bool) {
	lc, exists := base.Lifecycle(call.Ledger)
	live := exists && !lc.Deleted
	if call.ErrKind == PqErrLedgerNotFound {
		return true, !live
	}
	if !live {
		return true, false
	}

	return false, false
}

// PreparedListOutcomeLegal is the whole per-base verdict for a LIST page.
func PreparedListOutcomeLegal(ls oracle.LedgerState, call PreparedCall, after []byte, cur *commonpb.PreparedQueryCursor) bool {
	if call.WrongResult {
		return false
	}
	stored, exists := ls.PreparedQuery(call.Name)
	if !exists {
		// The only outcome an absent query can produce.
		return call.ErrKind == PqErrNotFound
	}

	if call.ErrKind == PqErrNotFound {
		return false
	}
	if call.ErrKind == PqErrAggregateTarget {
		return false
	}

	bound, resolved := SubstituteParams(stored.GetFilter(), call.Params)
	if !resolved {
		return UnresolvedParamRejectionLegal(ls, bound, stored.GetTarget(), call.ErrKind)
	}

	return IndexedQueryOutcomeLegal(ls, stored.GetTarget(), bound, PreparedNeededIndexes(bound, stored.GetTarget()),
		AsIndexedErrKind(call.ErrKind), RejectedIndexLabel(call.Err),
		func(view oracle.LedgerState) bool {
			return PreparedWindowMatches(view, call, stored.GetTarget(), bound, after, cur)
		})
}

// UnresolvedParamRejectionLegal is the verdict for a base whose stored filter
// references a parameter the call did not supply, or supplied with the wrong
// value type. Such a base can only have produced a rejection — but not
// necessarily the parameter's own one: the compiler resolves an index before it
// resolves that leaf's parameter, so a filter that ALSO needs an index the base
// lacks legitimately fails on the index first. Both rejections are therefore
// legal; results never are.
func UnresolvedParamRejectionLegal(
	ls oracle.LedgerState,
	bound *commonpb.QueryFilter,
	target commonpb.QueryTarget,
	errKind PqErrKind,
) bool {
	switch errKind {
	case PqErrCompilation:
		return true
	case PqErrIndex:
		for canon := range PreparedNeededIndexes(bound, target) {
			if exists, active := ls.IndexState(canon); !exists || !active {
				return true
			}
		}

		return false
	default:
		return false
	}
}

// PreparedNeededIndexes names the indexes a bound filter needs on its target.
// Log leaves use the same stream and opt-in date index as ordinary log queries.
func PreparedNeededIndexes(bound *commonpb.QueryFilter, target commonpb.QueryTarget) map[string]struct{} {
	if target == commonpb.QueryTarget_QUERY_TARGET_LOGS {
		return NeededLogIndexes(bound)
	}

	needed := map[string]struct{}{}
	NeededIndexCanonicals(bound, target, needed)

	return needed
}

// PreparedWindowMatches compares one page against the window the base predicts.
// Prepared execution walks the entity keyspace ascending, or descending with
// call.reverse, and the after-key is exclusive in both directions.
func PreparedWindowMatches(
	ls oracle.LedgerState,
	call PreparedCall,
	target commonpb.QueryTarget,
	bound *commonpb.QueryFilter,
	after []byte,
	cur *commonpb.PreparedQueryCursor,
) bool {
	if cur.GetHasMore() != (cur.GetNext() != "") {
		return false
	}

	switch target {
	case commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS:
		return PreparedAccountPageMatches(ls, call, bound, string(after), cur)
	case commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS:
		return len(cur.GetAccountData()) == 0 && len(cur.GetLogData()) == 0 &&
			PreparedTransactionPageMatches(ls, call, bound, after, cur)
	case commonpb.QueryTarget_QUERY_TARGET_LOGS:
		return len(cur.GetAccountData()) == 0 && len(cur.GetTransactionData()) == 0 &&
			PreparedLogPageMatches(ls, call, bound, after, cur)
	default:
		return false
	}
}

// PreparedAccountPageMatches checks the rows, their content, and has_more. The
// account window is exact, so has_more is checkable: it is set iff a further
// row exists past the page.
func PreparedAccountPageMatches(
	ls oracle.LedgerState,
	call PreparedCall,
	bound *commonpb.QueryFilter,
	after string,
	cur *commonpb.PreparedQueryCursor,
) bool {
	if len(cur.GetTransactionData()) > 0 || len(cur.GetLogData()) > 0 {
		return false
	}

	serverAccts := cur.GetAccountData()

	// One row past the page tells us whether the server was right to advertise
	// more, without a second window computation.
	probe := PreparedAccountProbe(ls, bound, after, call.PageSize+1, call.Reverse)

	want := probe
	if len(want) > call.PageSize {
		want = want[:call.PageSize]
	}

	if len(want) != len(serverAccts) || cur.GetHasMore() != (len(probe) > call.PageSize) {
		return false
	}

	for i, addr := range want {
		if serverAccts[i].GetAddress() != addr || !AccountMatches(ls, addr, serverAccts[i]) {
			return false
		}
	}

	return true
}

// PreparedLogPageMatches checks the same row facts as ordinary log queries,
// plus the prepared cursor's has_more flag.
func PreparedLogPageMatches(
	ls oracle.LedgerState,
	call PreparedCall,
	bound *commonpb.QueryFilter,
	after []byte,
	cur *commonpb.PreparedQueryCursor,
) bool {
	return PreparedLogWindowMatches(ls, call, bound, after, ServerLogRows(cur.GetLogData()), cur.GetHasMore())
}

// PreparedLogWindowMatches permits unknown-date rows to match or not, while
// requiring every known match. The suffix must explain has_more on that same
// page: a required row forces it, and any possible row can justify it.
func PreparedLogWindowMatches(ls oracle.LedgerState, call PreparedCall, bound *commonpb.QueryFilter, after []byte, page []ServerLogRow, hasMore bool) bool {
	rows := PreparedLogWindowRows(ls, call.Ledger, bound, after, call.Reverse)
	if !LogRowsMatch(call.Ledger, rows, call.PageSize, page) {
		return false
	}

	if hasMore && len(page) != call.PageSize {
		return false
	}

	if len(page) > 0 {
		after = Uint64EntityKey(page[len(page)-1].ID)
	}

	remaining := PreparedLogWindowRows(ls, call.Ledger, bound, after, call.Reverse)
	if hasMore {
		return len(remaining) > 0
	}

	for _, row := range remaining {
		if row.Required {
			return false
		}
	}

	return true
}

func PreparedTransactionPageMatches(ls oracle.LedgerState, call PreparedCall, bound *commonpb.QueryFilter, after []byte, cur *commonpb.PreparedQueryCursor) bool {
	page := cur.GetTransactionData()
	rows := PreparedTransactionWindowRows(ls, bound, after, call.Reverse)
	if !TxRowsMatch(ls, rows, call.PageSize, page) {
		return false
	}
	if cur.GetHasMore() && len(page) != call.PageSize {
		return false
	}
	if len(page) > 0 {
		after = Uint64EntityKey(page[len(page)-1].GetId())
	}
	remaining := PreparedTransactionWindowRows(ls, bound, after, call.Reverse)
	if cur.GetHasMore() {
		return len(remaining) > 0
	}
	for _, row := range remaining {
		if row.Required {
			return false
		}
	}

	return true
}

// PreparedTransactionWindowRows orders by transaction id. TransactionWindowRows
// takes the ad-hoc ListTransactions flag, whose default is newest-first, so the
// prepared ascending order is its reverse=true.
func PreparedTransactionWindowRows(ls oracle.LedgerState, bound *commonpb.QueryFilter, after []byte, reverse bool) []TxWindowRow {
	rows := TransactionWindowRows(ls, bound, 0, !reverse)

	return FilterPreparedRows(rows, after, reverse, func(row TxWindowRow) []byte { return Uint64EntityKey(row.ID) })
}

func PreparedLogWindowRows(ls oracle.LedgerState, ledger string, bound *commonpb.QueryFilter, after []byte, reverse bool) []LogWindowRow {
	rows := LogWindowRows(ls, ledger, bound, 0, false)
	if reverse {
		slices.Reverse(rows)
	}

	return FilterPreparedRows(rows, after, reverse, func(row LogWindowRow) []byte { return Uint64EntityKey(row.ID) })
}

// FilterPreparedRows drops the rows at or before the raw after-key in the
// walk's direction. The comparison is on raw entity bytes, as the server's is.
func FilterPreparedRows[T any](rows []T, after []byte, reverse bool, key func(T) []byte) []T {
	if after == nil {
		return rows
	}
	kept := rows[:0]
	for _, row := range rows {
		cmp := bytes.Compare(key(row), after)
		if reverse && cmp < 0 || !reverse && cmp > 0 {
			kept = append(kept, row)
		}
	}

	return kept
}

func PreparedAggregateOutcomeLegal(ls oracle.LedgerState, call PreparedCall, agg *commonpb.AggregateResult) bool {
	if call.WrongResult {
		return false
	}
	stored, exists := ls.PreparedQuery(call.Name)
	if !exists {
		return call.ErrKind == PqErrNotFound
	}

	if call.ErrKind == PqErrNotFound {
		return false
	}

	// The call was shaped for an ACCOUNTS-target query; a base holding another
	// target explains only the rejection, which runAggregateTargetMisuse owns.
	if stored.GetTarget() != commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS {
		return call.ErrKind == PqErrAggregateTarget
	}
	if call.ErrKind == PqErrOther || call.ErrKind == PqErrAggregateTarget {
		return false
	}

	bound, resolved := SubstituteParams(stored.GetFilter(), call.Params)
	if !resolved {
		return UnresolvedParamRejectionLegal(ls, bound, stored.GetTarget(), call.ErrKind)
	}

	return IndexedQueryOutcomeLegal(ls, stored.GetTarget(), bound, PreparedNeededIndexes(bound, stored.GetTarget()),
		AsIndexedErrKind(call.ErrKind), RejectedIndexLabel(call.Err),
		func(view oracle.LedgerState) bool {
			return AggregateMatches(view, bound, agg)
		})
}

// AggregateMatches folds the base's volume cells over the accounts the filter
// selects and compares the bucket set and every total exactly. A prepared query
// carries no result-stage options, so the fold runs with none.
func AggregateMatches(ls oracle.LedgerState, bound *commonpb.QueryFilter, agg *commonpb.AggregateResult) bool {
	if len(agg.GetGroups()) != 0 {
		return false
	}

	got, ok := ServerAggregate(agg)
	if !ok {
		// One entry per bucket: a repeated bucket is a server-side fold bug that
		// summing into a map would silently absorb.
		return false
	}

	return AggEqual(ModelAggregate(ls, bound, AggOptions{}), got)
}

// DescribeAggregate renders an aggregate result for a finding's details,
// bucket-sorted so model and server renderings line up.
func DescribeAggregate(agg *commonpb.AggregateResult) string {
	parts := make([]string, 0, len(agg.GetVolumes()))
	for _, v := range agg.GetVolumes() {
		var in, out uint256.Int
		v.GetInput().IntoUint256(&in)
		v.GetOutput().IntoUint256(&out)

		parts = append(parts, fmt.Sprintf("%s/%s=%s:%s", v.GetAsset(), v.GetColor(), in.Dec(), out.Dec()))
	}

	sort.Strings(parts)

	return strings.Join(parts, " ")
}

// PreparedServerRows renders a page's keys, whichever target it carries.
func PreparedServerRows(cur *commonpb.PreparedQueryCursor) string {
	if accts := cur.GetAccountData(); len(accts) > 0 {
		addrs := make([]string, len(accts))
		for i, a := range accts {
			addrs[i] = a.GetAddress()
		}

		return strings.Join(addrs, ",")
	}

	if txs := cur.GetTransactionData(); len(txs) > 0 {
		ids := make([]uint64, len(txs))
		for i, tx := range txs {
			ids[i] = tx.GetId()
		}

		return joinUint64(ids)
	}

	if ids := ServerLogIDs(cur.GetLogData()); len(ids) > 0 {
		return joinUint64(ids)
	}

	return ""
}

// PreparedAccountProbe is the ordered account window for a bound ACCOUNTS
// filter, one row longer than the page so has_more is checkable, descending
// when reverse.
//
// A bare has-asset leaf takes its own universe. The has-asset index serves the
// EVER-touched projection — an account drained to zero and purged from the
// volume table stays in it — while AccountUniverse holds only accounts with a
// live volume or metadata cell. Paging a has-asset query over AccountUniverse
// silently drops every purged account the server correctly returns.
// genAccountAssetFilter never composes the leaf with anything else for the same
// reason, and requireBareHasAsset holds the stored filters to that.
func PreparedAccountProbe(ls oracle.LedgerState, bound *commonpb.QueryFilter, after string, limit int, reverse bool) []string {
	if base, precision, bare := HasAssetTarget(bound); bare {
		return AssetWindow(ls, base, precision, after, limit, reverse)
	}

	return AccountWindow(ls, bound, after, limit, reverse)
}
