package match

import (
	"slices"
	"sort"
	"strconv"
	"strings"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/pkg/pagecursor"
	"github.com/formancehq/ledger/v3/tests/oracle"
	"github.com/holiman/uint256"
	"google.golang.org/grpc/codes"
)

// ServerDefaultPageSize and ServerMaxPageSize mirror ctrl.DefaultPageSize and
// ctrl.MaxPageSize, the bounds the server's own ClampPageSize applies. Mirrored
// rather than imported so the driver binary does not link the controller;
// TestEffectivePageSize_MirrorsTheServerClamp pins the two together.
const (
	ServerDefaultPageSize = 100
	ServerMaxPageSize     = 1000
)

// EffectivePageSize is the size the server serves a request of this size with,
// mirroring ctrl.ClampPageSize. The model must size its window with this, never
// with what the driver asked for: a page the server capped at its own maximum is
// not a short page, and one it defaulted is not an empty request.
func EffectivePageSize(requested int) int {
	if requested == 0 {
		return ServerDefaultPageSize
	}

	if requested > ServerMaxPageSize {
		return ServerMaxPageSize
	}

	return requested
}

// BareLeaf reports whether f is a single non-empty leaf, the only shape the
// target-invalid probes take.
func BareLeaf(f *commonpb.QueryFilter) bool {
	switch f.GetFilter().(type) {
	case nil, *commonpb.QueryFilter_And, *commonpb.QueryFilter_Or, *commonpb.QueryFilter_Not:
		return false
	default:
		return true
	}
}

// PageToken renders the model's exclusive resume key as a forward page
// token; the empty key is the first page.
func PageToken(key string) string {
	if key == "" {
		return ""
	}

	return pagecursor.Cursor{Key: key}.Encode()
}

// AccountPageMatches reports whether the served page is ls's ordered window and
// the resume token published with it is the one that window implies. The window
// is computed one row long so the model sees the same peek the handler did: a
// row past the page is what makes a token mandatory.
func AccountPageMatches(ls oracle.LedgerState, filter *commonpb.QueryFilter, cursor string, pageSize int, reverse bool, serverAccts []*commonpb.Account, next string) bool {
	want := AccountWindow(ls, filter, cursor, pageSize+1, reverse)

	more := CursorForbidden
	if len(want) > pageSize {
		want = want[:pageSize]
		more = CursorRequired
	}

	if len(want) != len(serverAccts) {
		return false
	}

	for i, addr := range want {
		if serverAccts[i].GetAddress() != addr || !AccountMatches(ls, addr, serverAccts[i]) {
			return false
		}
	}

	return NextCursorLegal(next, more, LastAccountKey(serverAccts), len(serverAccts), pageSize)
}

// LastAccountKey is the cursor an accounts page implies: the address of its last
// row (accountCursorOf), empty for a page that showed none.
func LastAccountKey(accounts []*commonpb.Account) string {
	if len(accounts) == 0 {
		return ""
	}

	return accounts[len(accounts)-1].GetAddress()
}

// AccountWindow is the model's prediction of a ListAccounts page: the ledger's
// accounts matching filter, in address order (reversed when reverse), with the
// exclusive cursor applied and capped at pageSize. Because the list is sorted,
// dropping every key past the cursor equals the server's contiguous prefix skip.
//
// The candidates are the current accounts plus, when the filter reads the
// account-by-asset index, every account that index holds: a has-asset leaf can
// select an account whose volume rows are gone, which no other leaf sees.
func AccountWindow(ls oracle.LedgerState, filter *commonpb.QueryFilter, cursor string, pageSize int, reverse bool) []string {
	universe := AccountUniverse(ls)
	candidates := universe
	if AnyLeaf(filter, func(leaf *commonpb.QueryFilter) bool { return leaf.GetAccountHasAsset() != nil }) {
		candidates = mergeSorted(universe, ls.EverAssetAddresses())
	}

	var window []string
	for _, addr := range candidates {
		_, inUniverse := slices.BinarySearch(universe, addr)
		if MatchAccountFilterIn(ls, filter, addr, inUniverse) {
			window = append(window, addr)
		}
	}

	if reverse {
		reverseStrings(window)
	}

	if cursor != "" {
		kept := window[:0]
		for _, addr := range window {
			if reverse && addr >= cursor || !reverse && addr <= cursor {
				continue
			}

			kept = append(kept, addr)
		}
		window = kept
	}

	if len(window) > pageSize {
		window = window[:pageSize]
	}

	return window
}

// TransactionWindow is the required-row window — the ids a page must show when
// every filter verdict is known — capped at pageSize. Used for a finding's
// diagnostics; validation goes through TxWindowMatches, which also handles
// optional rows.
func TransactionWindow(ls oracle.LedgerState, filter *commonpb.QueryFilter, afterID uint64, pageSize int, reverse bool) []uint64 {
	var window []uint64
	for _, row := range TransactionWindowRows(ls, filter, afterID, reverse) {
		if row.Required {
			window = append(window, row.ID)
		}
	}

	if len(window) > pageSize {
		window = window[:pageSize]
	}

	return window
}

// TxWindowRow is one ordered candidate row of a predicted transaction page:
// required rows must appear in the server page, optional ones (a filter verdict
// hinging on a not-yet-learned server stamp) may.
type TxWindowRow struct {
	ID       uint64
	Required bool
}

// TransactionWindowRows is the ordered, cursor-filtered, UNTRUNCATED row
// sequence a ListTransactions page draws from. Truncation is the matcher's job
// (TxWindowMatches) — optional rows may or may not consume page slots, so a
// fixed prefix cut would be wrong.
//
// The transactions endpoint INVERTS the API reverse flag (controller
// ListTransactionsFrom passes reverse=!reverse to listEntities): reverse=false
// yields newest-first (descending id), reverse=true yields oldest-first
// (ascending). This is the opposite of accounts, which follow reverse literally.
// Descending pages drop ids >= afterID (older-than cursor); ascending pages drop
// ids <= afterID (newer-than cursor) — mirroring PaginateReverse / PaginateForward.
func TransactionWindowRows(ls oracle.LedgerState, filter *commonpb.QueryFilter, afterID uint64, reverse bool) []TxWindowRow {
	descending := !reverse

	var rows []TxWindowRow
	for _, rec := range ls.Txs().All() {
		match, known := MatchTxFilter(ls, filter, rec)
		if known && !match {
			continue
		}

		rows = append(rows, TxWindowRow{ID: rec.Id(), Required: known})
	}

	if descending {
		for i, j := 0, len(rows)-1; i < j; i, j = i+1, j-1 {
			rows[i], rows[j] = rows[j], rows[i]
		}
	}

	if afterID != 0 {
		kept := rows[:0]
		for _, row := range rows {
			if descending && row.ID >= afterID || !descending && row.ID <= afterID {
				continue
			}

			kept = append(kept, row)
		}
		rows = kept
	}

	return rows
}

// TxWindowMatches reports whether the server page is exactly a legal window
// over the candidate's row sequence: required rows appear in order (each
// content-matching its model record), optional rows may, nothing else does, a
// required row may only be missing past a full (truncated) page, and the resume
// token published with the page is the one the leftover rows imply.
func TxWindowMatches(ls oracle.LedgerState, filter *commonpb.QueryFilter, afterID uint64, pageSize int, reverse bool, serverTxs []*commonpb.Transaction, next string) bool {
	rows := TransactionWindowRows(ls, filter, afterID, reverse)
	if !TxRowsMatch(ls, rows, pageSize, serverTxs) {
		return false
	}

	return NextCursorLegal(next, RowsLeftoverMore(LeftoverTxRows(rows, serverTxs)), LastTransactionKey(serverTxs), len(serverTxs), pageSize)
}

func TxRowsMatch(ls oracle.LedgerState, rows []TxWindowRow, pageSize int, serverTxs []*commonpb.Transaction) bool {
	if len(serverTxs) > pageSize {
		return false
	}

	txs := ls.Txs()
	j := 0

	for _, row := range rows {
		if j == len(serverTxs) {
			if len(serverTxs) == pageSize {
				return true // full page — the remaining rows were truncated
			}
			if row.Required {
				return false // page had room, yet a required row is missing
			}

			continue
		}

		if serverTxs[j].GetId() == row.ID {
			if !TxRecordMatches(txs.Get(int(row.ID-1)), serverTxs[j]) {
				return false
			}

			j++
		} else if row.Required {
			return false
		}
	}

	return j == len(serverTxs)
}

// LeftoverTxRows is the tail of rows the page did not consume.
func LeftoverTxRows(rows []TxWindowRow, serverTxs []*commonpb.Transaction) []TxWindowRow {
	j, i := 0, 0
	for ; i < len(rows) && j < len(serverTxs); i++ {
		if serverTxs[j].GetId() == rows[i].ID {
			j++
		}
	}

	return rows[i:]
}

// RowsLeftoverMore turns the rows a page left behind into the verdict on
// whether a resume cursor was owed: a required row certainly follows, rows whose
// filter verdict hinges on a stamp the model has not learned may or may not.
func RowsLeftoverMore(leftover []TxWindowRow) CursorMore {
	more := CursorForbidden
	for _, row := range leftover {
		if row.Required {
			return CursorRequired
		}

		more = CursorEither
	}

	return more
}

// LastTransactionKey is the cursor a transactions page implies: the id of its
// last row in decimal (txCursorOf), empty for a page that showed none.
func LastTransactionKey(txs []*commonpb.Transaction) string {
	if len(txs) == 0 {
		return ""
	}

	return strconv.FormatUint(txs[len(txs)-1].GetId(), 10)
}

// AccountUniverse returns ls's account addresses — every address carrying a
// volume cell or a metadata entry — in ascending byte order, matching the
// server's merged V+M attribute scan (readstore.NewPebbleAccountIterator).
func AccountUniverse(ls oracle.LedgerState) []string {
	seen := map[string]struct{}{}
	for k := range ls.Volumes().All() {
		seen[k.Address] = struct{}{}
	}
	for k := range ls.Metadata().All() {
		seen[k.Address] = struct{}{}
	}

	out := make([]string, 0, len(seen))
	for addr := range seen {
		out = append(out, addr)
	}

	sort.Strings(out)

	return out
}

// AccountMatches reports whether the server account is exactly what the model
// holds for addr: the same volume cells (per asset and color, input and output)
// and the same metadata. MetadataMatches is shared with the single GetAccount
// read.
func AccountMatches(ls oracle.LedgerState, addr string, serverAcct *commonpb.Account) bool {
	return AccountMatchesCollapsed(ls, addr, serverAcct, false)
}

// AccountMatchesCollapsed is AccountMatches for a read that asked the server to
// collapse colours: every colour of an asset is then summed into the uncolored
// bucket, so the model folds the same way before comparing.
func AccountMatchesCollapsed(ls oracle.LedgerState, addr string, serverAcct *commonpb.Account, collapsed bool) bool {
	if !MetadataMatches(ls, addr, serverAcct.GetMetadata()) {
		return false
	}

	// assembleAccount fills address, metadata and volumes and nothing else, so
	// these three carry no value the model could be held to. Their absence is
	// a settled decision, not a gap awaiting a fix (ledger#2025, documented in
	// docs/technical/contributing/api-comparison.md under Missing Features):
	// the replacement is typed `datetime` account metadata, which the oracle
	// already predicts through MetadataMatches. Pinned as absent so a
	// regression that starts stamping them fails here rather than being waved
	// through.
	if serverAcct.GetFirstUsage() != nil || serverAcct.GetInsertionDate() != nil ||
		serverAcct.GetUpdatedAt() != nil {
		return false
	}

	server := map[AssetColor]struct{ in, out uint256.Int }{}
	for _, av := range serverAcct.GetVolumes() {
		var in, out uint256.Int
		vols := av.GetVolumes()
		if vols == nil || vols.GetInput() == nil || vols.GetOutput() == nil {
			return false
		}
		if err := in.SetFromDecimal(vols.GetInput().DecimalString()); err != nil {
			return false
		}
		if err := out.SetFromDecimal(vols.GetOutput().DecimalString()); err != nil {
			return false
		}

		key := AssetColor{Asset: av.GetAsset(), Color: av.GetColor()}
		if _, dup := server[key]; dup {
			return false
		}

		server[key] = struct{ in, out uint256.Int }{in, out}
	}

	got := make(map[AssetColor]oracle.VolumePair, len(server))
	for key, v := range server {
		got[key] = oracle.VolumePair{Input: v.in, Output: v.out}
	}

	return AccountVolumesMatch(ls, addr, got, collapsed)
}

// TxRecordView is the subset of oracle's (unexported) transaction record that
// the query and single-read validators compare against. Declaring the interface
// here lets both share TxRecordMatches without oracle exporting the concrete type.
type TxRecordView interface {
	Id() uint64
	Reference() string
	Postings() []*commonpb.Posting
	Metadata() map[string]*commonpb.MetadataValue
	Reverted() bool
	Timestamp() *commonpb.Timestamp
	InsertedAt() *commonpb.Timestamp
	RevertedBy() uint64
	RevertedAt() *commonpb.Timestamp
	RevertsTransaction() uint64
	IndexedAddrs() map[string]uint8
	PostCommitVolumes() map[oracle.VolumeKey]oracle.VolumePair
}

// TxRecordMatches reports whether the model record rec is consistent with the
// server transaction: same id, reference, revert relationships, postings, and
// metadata. Timestamps follow the model's nil-means-server-dated convention — a
// nil model timestamp is unpredictable, so it is not checked; inserted_at and
// reverted_at are the same, and only a reverted record may carry the latter at
// all.
func TxRecordMatches(rec TxRecordView, serverTx *commonpb.Transaction) bool {
	tsOK := rec.Timestamp() == nil || rec.Timestamp().GetData() == serverTx.GetTimestamp().GetData()

	// inserted_at follows the same convention: server-assigned, so it is
	// checked only once learned — and once learned it also drives inserted_at
	// filter evaluation, so a row serving a different value must not pass.
	insOK := rec.InsertedAt() == nil || rec.InsertedAt().GetData() == serverTx.GetInsertedAt().GetData()

	var raOK bool
	switch {
	case rec.RevertedAt() != nil:
		raOK = serverTx.GetRevertedAt() != nil && rec.RevertedAt().GetData() == serverTx.GetRevertedAt().GetData()
	case rec.Reverted():
		raOK = true
	default:
		raOK = serverTx.GetRevertedAt() == nil
	}

	// updated_at is stamped once, from the same proposal date as inserted_at
	// (processor_transaction.go, processor_revert_transaction.go), and nothing
	// bumps it afterwards — not a metadata save, not being reverted. So the
	// two must agree on every served row, whether or not the model learned the
	// value.
	if serverTx.GetUpdatedAt().GetData() != serverTx.GetInsertedAt().GetData() ||
		(serverTx.GetUpdatedAt() == nil) != (serverTx.GetInsertedAt() == nil) {
		return false
	}

	if !PCVSnapshotMatches(rec.PostCommitVolumes(), serverTx.GetPostCommitVolumes()) {
		return false
	}

	return rec.Id() == serverTx.GetId() &&
		rec.Reference() == serverTx.GetReference() &&
		rec.Reverted() == serverTx.GetReverted() &&
		rec.RevertedBy() == serverTx.GetRevertedByTransaction() &&
		rec.RevertsTransaction() == serverTx.GetRevertsTransaction() &&
		tsOK && insOK && raOK &&
		postingsEqual(rec.Postings(), serverTx.GetPostings()) &&
		metaMapEqual(rec.Metadata(), serverTx.GetMetadata())
}

// PCVSnapshotMatches compares a served post-commit snapshot against the
// model's cell for cell, in both directions: an absent cell and a fabricated
// one are equally wrong, and so is a repeated one — only the first copy of a
// cell is ever read, so a duplicate would hide whatever the second carries.
func PCVSnapshotMatches(model map[oracle.VolumeKey]oracle.VolumePair, server *commonpb.PostCommitVolumes) bool {
	served := map[oracle.VolumeKey]struct{}{}

	for account, byAssets := range server.GetVolumesByAccount() {
		for _, entry := range byAssets.GetVolumes() {
			key := oracle.VolumeKey{Address: account, Asset: entry.GetAsset(), Color: entry.GetColor()}
			if _, dup := served[key]; dup {
				return false
			}

			served[key] = struct{}{}
		}
	}

	if len(served) != len(model) {
		return false
	}

	for key, vp := range model {
		gotIn, gotOut, ok := postCommitVolume(server, key)
		if !ok || vp.Input.Cmp(&gotIn) != 0 || vp.Output.Cmp(&gotOut) != 0 {
			return false
		}
	}

	return true
}

// IsIndexNotReady matches the server's index-building rejection. The reason
// rides codes.Unavailable (SDK retry predicates retry it), which puts it
// inside the transient class (oraclecheck.Outcome.Transient exempts it) — so the generic transient bail must peel it
// off first: an index the model holds active answering INDEX_BUILDING is the
// exact finding the lifecycle coverage exists to catch, not a skippable blip.
func IsIndexNotReady(err RPCError) bool {
	return err.Code == codes.Unavailable && err.HasReason("INDEX_BUILDING")
}

// IsIndexNotFound matches the server's index-absent rejection by its reason,
// not by the bare FailedPrecondition code — an unrelated precondition error
// must stay a finding instead of being explained away as a missing index.
func IsIndexNotFound(err RPCError) bool {
	return err.Code == codes.FailedPrecondition && err.HasReason("INDEX_NOT_FOUND")
}

func FilterAddrPrefix(prefix string) *commonpb.QueryFilter {
	return FilterAddrPrefixRole(prefix, commonpb.AddressRole_ADDRESS_ROLE_ANY)
}

func FilterAddrPrefixRole(prefix string, role commonpb.AddressRole) *commonpb.QueryFilter {
	return &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Address{Address: &commonpb.AddressMatch{
		Match: &commonpb.AddressMatch_HardcodedPrefix{HardcodedPrefix: prefix},
		Role:  role,
	}}}
}

func FilterAddrExact(addr string) *commonpb.QueryFilter {
	return FilterAddrExactRole(addr, commonpb.AddressRole_ADDRESS_ROLE_ANY)
}

func FilterAddrExactRole(addr string, role commonpb.AddressRole) *commonpb.QueryFilter {
	return &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Address{Address: &commonpb.AddressMatch{
		Match: &commonpb.AddressMatch_HardcodedExact{HardcodedExact: addr},
		Role:  role,
	}}}
}

func FilterReference(value string) *commonpb.QueryFilter {
	return &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Reference{Reference: &commonpb.ReferenceCondition{
		Cond: &commonpb.StringCondition{Value: &commonpb.StringCondition_Hardcoded{Hardcoded: value}},
	}}}
}

func FilterReverted(value bool) *commonpb.QueryFilter {
	return &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Reverted{
		Reverted: &commonpb.RevertedCondition{Value: value},
	}}
}

// FilterTxIDRange matches transactions with lo <= id <= hi. Both bounds are
// inclusive (no exclusive flags), mirroring resolveUintBounds' [min, max+1).
func FilterTxIDRange(lo, hi uint64) *commonpb.QueryFilter {
	return &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_BuiltinUint{BuiltinUint: &commonpb.BuiltinUintCondition{
		Field: commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_ID,
		Cond:  &commonpb.UintCondition{Min: &lo, Max: &hi},
	}}}
}

// FilterDateRange matches transactions whose `field` date lies in [lo, hi],
// both bounds inclusive (like FilterTxIDRange).
func FilterDateRange(field commonpb.TransactionBuiltinIndex, lo, hi uint64) *commonpb.QueryFilter {
	return &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_BuiltinUint{BuiltinUint: &commonpb.BuiltinUintCondition{
		Field: field,
		Cond:  &commonpb.UintCondition{Min: &lo, Max: &hi},
	}}}
}

func FilterAnd(children ...*commonpb.QueryFilter) *commonpb.QueryFilter {
	return &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_And{And: &commonpb.AndFilter{Filters: children}}}
}

func FilterOr(children ...*commonpb.QueryFilter) *commonpb.QueryFilter {
	return &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Or{Or: &commonpb.OrFilter{Filters: children}}}
}

func FilterNot(child *commonpb.QueryFilter) *commonpb.QueryFilter {
	return &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Not{Not: &commonpb.NotFilter{Filter: child}}}
}

// FilterMetaExists matches entities that carry metadata key — an existence
// condition, index-backed on both targets. On a declared, indexed key it is a
// result-returning filter; on an undeclared key it is the index-not-found
// probe.
func FilterMetaExists(key string) *commonpb.QueryFilter {
	return &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Field{Field: &commonpb.FieldCondition{
		Field:     &commonpb.FieldRef{Metadata: key},
		Condition: &commonpb.FieldCondition_ExistsCond{ExistsCond: &commonpb.ExistsCondition{}},
	}}}
}

// FilterHasAsset matches accounts holding (assetBase, precision). Valid only on
// the ACCOUNTS target, so on transactions it is the target-invalid probe
// (rejected with InvalidArgument before any index check). The values are
// immaterial — rejection happens on target validity, before evaluation.
func FilterHasAsset(assetBase string, precision uint32) *commonpb.QueryFilter {
	return &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_AccountHasAsset{
		AccountHasAsset: &commonpb.AccountHasAssetCondition{AssetBase: assetBase, Precision: precision},
	}}
}

// FilterNeedsIndex reports whether f contains any condition the server's
// compiler serves from a created index (query.Compile → requireIndexReady): the
// account-by-asset builtin, or any other index-backed leaf (metadata Field,
// Reference, timestamp builtins, address-on-transactions). The index-free
// conditions are universe (nil), address on accounts, reverted, and the tx-id
// builtin. See IndexNeeds for the asset/other split the account-query lifecycle
// path keys on.
func FilterNeedsIndex(f *commonpb.QueryFilter, target commonpb.QueryTarget) bool {
	asset, other := IndexNeeds(f, target)

	return asset || other
}

// NeededIndexCanonicals collects the canonical ids of every index the filter
// needs on target — the builtin transaction indexes, the account-by-asset index,
// per-(target, key) metadata indexes — plus a sentinel for any index-backed
// leaf whose index the workload never creates. The sentinel is never in the
// model's index set, so those filters always predict rejection. The set drives
// the per-index lifecycle validation (validateIndexedTransactionQuery /
// validateIndexedAccountQuery); empty means the filter is index-free and
// validates exactly. Target-invalid leaves classify like their home target —
// callers consult FilterInvalidForTarget first, mirroring the compiler's
// check order.
func NeededIndexCanonicals(f *commonpb.QueryFilter, target commonpb.QueryTarget, out map[string]struct{}) {
	VisitLeaves(f, func(leaf *commonpb.QueryFilter) {
		switch x := leaf.GetFilter().(type) {
		case nil:
			// No condition, no index.
		case *commonpb.QueryFilter_Field:
			out[MetadataCanonical(target, x.Field.GetField().GetMetadata())] = struct{}{}
		case *commonpb.QueryFilter_AccountHasAsset:
			out[AssetIndexCanonical] = struct{}{}
		case *commonpb.QueryFilter_Reference:
			out[TxBuiltinCanonical(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE)] = struct{}{}
		case *commonpb.QueryFilter_Address:
			// Index-free on accounts (existence scan); the account→tx mapping
			// index on transactions.
			if target == commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS {
				out[TxBuiltinCanonical(AddressRoleBuiltin(x.Address.GetRole()))] = struct{}{}
			}
		case *commonpb.QueryFilter_BuiltinUint:
			if x.BuiltinUint.GetField() != commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_ID {
				out[TxBuiltinCanonical(x.BuiltinUint.GetField())] = struct{}{}
			}
		case *commonpb.QueryFilter_Reverted:
			// index-free
		default:
			// Log conditions and future leaves: index-backed, never built here.
			out[NeverBuiltIndexCanonical] = struct{}{}
		}
	})
}

// NeverBuiltIndexCanonical is the sentinel canonical for index-backed leaves
// the workload never builds an index for. It never appears in the model's index
// set, so IndexState reports it absent — a rejection is always legal, results
// never are.
const NeverBuiltIndexCanonical = "workload:never-built"

// IndexNeeds classifies f's index requirements into two buckets: `asset` is set
// if any leaf needs the account-by-asset builtin (an AccountHasAsset condition),
// `other` if any leaf needs a different index (metadata Field, Reference, a
// timestamp builtin, address-on-transactions). A filter is asset-only — its
// outcome governed by the account-by-asset lifecycle — iff asset && !other; the
// driver creates only that index, so any `other` need means a missing index and
// a rejected compile regardless. Combinators OR their children's needs; the
// compiler fails the whole query on the first missing index, so a single
// index-backed leaf anywhere sets the whole tree's need.
func IndexNeeds(f *commonpb.QueryFilter, target commonpb.QueryTarget) (asset, other bool) {
	n := FoldFilter(f, FilterFold[indexNeed]{
		And: unionNeeds,
		Or:  unionNeeds,
		Not: identity[indexNeed],
		Leaf: func(leaf *commonpb.QueryFilter) indexNeed {
			switch x := leaf.GetFilter().(type) {
			case nil:
				return indexNeed{}
			case *commonpb.QueryFilter_AccountHasAsset:
				return indexNeed{asset: true}
			case *commonpb.QueryFilter_Reverted:
				return indexNeed{}
			case *commonpb.QueryFilter_Address:
				// Index-free on accounts; on transactions it needs the account→tx index.
				return indexNeed{other: target == commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS}
			case *commonpb.QueryFilter_BuiltinUint:
				// Only the id builtin scans the always-present Pebble tx keyspace; the
				// timestamp/inserted_at/reverted_at builtins need an index.
				return indexNeed{other: x.BuiltinUint.GetField() != commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_ID}
			default:
				// Field, Reference, log conditions — index-backed, non-asset.
				return indexNeed{other: true}
			}
		},
	})

	return n.asset, n.other
}

type indexNeed struct {
	asset, other bool
}

func unionNeeds(children []indexNeed) indexNeed {
	var n indexNeed
	for _, c := range children {
		n.asset, n.other = n.asset || c.asset, n.other || c.other
	}

	return n
}

// FilterInvalidForTarget reports whether f carries any condition the server's
// per-target validity table rejects (rejectInvalidCondition → InvalidArgument) —
// e.g. reverted / tx-id on accounts, or account-has-asset on transactions. It
// reuses the exact commonpb functions the compiler consults, so the model's
// verdict cannot drift from the server's; every node is checked, combinators
// included (mirroring the compiler's per-node check). The server checks
// validity before index availability, so callers must consult this before
// FilterNeedsIndex.
func FilterInvalidForTarget(f *commonpb.QueryFilter, target commonpb.QueryTarget) bool {
	invalidKind := func(kind commonpb.ConditionKind) bool {
		return !commonpb.ConditionValidForTarget(target, kind)
	}

	return FoldFilter(f, FilterFold[bool]{
		And: func(children []bool) bool { return invalidKind(commonpb.ConditionKindAnd) || anyOf(children) },
		Or:  func(children []bool) bool { return invalidKind(commonpb.ConditionKindOr) || anyOf(children) },
		Not: func(child bool) bool { return invalidKind(commonpb.ConditionKindNot) || child },
		Leaf: func(leaf *commonpb.QueryFilter) bool {
			return leaf.GetFilter() != nil && invalidKind(commonpb.ConditionKindOf(leaf))
		},
	})
}

// MatchAccountFilter evaluates an accounts filter against one current account
// of ls (see MatchAccountFilterIn).
func MatchAccountFilter(ls oracle.LedgerState, f *commonpb.QueryFilter, addr string) bool {
	return MatchAccountFilterIn(ls, f, addr, true)
}

// MatchAccountFilterIn evaluates an accounts filter against one address.
// inUniverse says whether addr is a current account — one the server's account
// scan yields. Every arm but has-asset selects from that scan: the universe
// (a nil node, an empty And), an address leaf, a metadata leaf, and the
// complement a Not takes. Has-asset reads the account-by-asset index, which
// holds accounts whose volume rows are gone. An empty Or matches nothing.
func MatchAccountFilterIn(ls oracle.LedgerState, f *commonpb.QueryFilter, addr string, inUniverse bool) bool {
	return FoldFilter(f, FilterFold[bool]{
		And: func(children []bool) bool { return (len(children) > 0 || inUniverse) && allOf(children) },
		Or:  anyOf,
		Not: func(child bool) bool { return inUniverse && !child },
		Leaf: func(leaf *commonpb.QueryFilter) bool {
			switch x := leaf.GetFilter().(type) {
			case nil:
				return inUniverse
			case *commonpb.QueryFilter_Address:
				switch m := x.Address.GetMatch().(type) {
				case *commonpb.AddressMatch_HardcodedPrefix:
					return inUniverse && strings.HasPrefix(addr, m.HardcodedPrefix)
				case *commonpb.AddressMatch_HardcodedExact:
					return inUniverse && addr == m.HardcodedExact
				}

				return false
			case *commonpb.QueryFilter_AccountHasAsset:
				return AccountHasAsset(ls, addr, x.AccountHasAsset.GetAssetBase(), x.AccountHasAsset.GetPrecision())
			case *commonpb.QueryFilter_Field:
				return inUniverse && MatchFieldCondition(ls.AccountFieldTypes(), func(key string) (*commonpb.MetadataValue, bool) {
					v, ok := ls.Metadata().Get(oracle.MetaKey{Address: addr, Key: key})

					return v, ok
				}, x.Field)
			default:
				return false
			}
		},
	})
}

// mergeSorted unions two ascending, duplicate-free lists into one.
func mergeSorted(a, b []string) []string {
	out := make([]string, 0, len(a)+len(b))
	i, j := 0, 0
	for i < len(a) && j < len(b) {
		switch {
		case a[i] < b[j]:
			out = append(out, a[i])
			i++
		case a[i] > b[j]:
			out = append(out, b[j])
			j++
		default:
			out = append(out, a[i])
			i++
			j++
		}
	}

	out = append(out, a[i:]...)

	return append(out, b[j:]...)
}

// MatchTxFilter evaluates f on rec three-valued: known=false means the leaf
// verdict hinges on a server stamp the model has not learned yet (a date field
// of a bulk still in flight — see oracle.LearnTxStamps). Booleans propagate
// unknowns Kleene-style: a decided AND/OR short-circuits, an undecided one
// stays unknown. Window construction turns unknown rows into optional ones.
func MatchTxFilter(ls oracle.LedgerState, f *commonpb.QueryFilter, rec TxRecordView) (match, known bool) {
	v := FoldFilter(f, FilterFold[Kleene]{
		And: KleeneAnd,
		Or:  KleeneOr,
		Not: KleeneNot,
		Leaf: func(leaf *commonpb.QueryFilter) Kleene {
			switch x := leaf.GetFilter().(type) {
			case nil:
				return Kleene{Match: true, Known: true}
			case *commonpb.QueryFilter_Reverted:
				return Kleene{Match: rec.Reverted() == x.Reverted.GetValue(), Known: true}
			case *commonpb.QueryFilter_Reference:
				// Exact match on the null-terminated txref key; empty references are
				// never written to the index, so they can never match.
				return Kleene{Match: rec.Reference() != "" && rec.Reference() == x.Reference.GetCond().GetHardcoded(), Known: true}
			case *commonpb.QueryFilter_Address:
				return Kleene{Match: MatchTxAddress(ls, x.Address, rec), Known: true}
			case *commonpb.QueryFilter_Field:
				return Kleene{Match: MatchFieldCondition(ls.TransactionFieldTypes(), func(key string) (*commonpb.MetadataValue, bool) {
					v, ok := rec.Metadata()[key]

					return v, ok
				}, x.Field), Known: true}
			case *commonpb.QueryFilter_BuiltinUint:
				m, k := MatchTxBuiltinUint(x.BuiltinUint, rec)

				return Kleene{Match: m, Known: k}
			default:
				return Kleene{Match: false, Known: true}
			}
		},
	})

	return v.Match, v.Known
}

// MatchTxAddress evaluates an address leaf on the TRANSACTIONS target: the
// transaction matches iff some account in its account→tx index membership
// (IndexedAddrs, role-filtered) matches the prefix/exact pattern AND is still
// in the merged V+M account universe — the server resolves matching accounts
// through the attributes zone (pebbleAccountExists / the account prefix
// iterator), so a purged account with no metadata stops reaching its
// transactions even though the index rows remain.
func MatchTxAddress(ls oracle.LedgerState, am *commonpb.AddressMatch, rec TxRecordView) bool {
	var roleMask uint8
	switch am.GetRole() {
	case commonpb.AddressRole_ADDRESS_ROLE_SOURCE:
		roleMask = oracle.AddrIndexedSource
	case commonpb.AddressRole_ADDRESS_ROLE_DESTINATION:
		roleMask = oracle.AddrIndexedDestination
	default:
		roleMask = oracle.AddrIndexedSource | oracle.AddrIndexedDestination
	}

	for addr, bits := range rec.IndexedAddrs() {
		if bits&roleMask == 0 {
			continue
		}

		switch m := am.GetMatch().(type) {
		case *commonpb.AddressMatch_HardcodedPrefix:
			if !strings.HasPrefix(addr, m.HardcodedPrefix) {
				continue
			}
		case *commonpb.AddressMatch_HardcodedExact:
			if addr != m.HardcodedExact {
				continue
			}
		default:
			continue // param matches are not generated
		}

		if ls.HasAccount(addr) {
			return true
		}
	}

	return false
}

// MatchTxBuiltinUint evaluates a transaction builtin-uint leaf. The date
// builtins read the record's (possibly learned) server stamps; a still-unknown
// stamp is an unknown verdict, not a miss — the record may well be in the
// server's index.
func MatchTxBuiltinUint(cond *commonpb.BuiltinUintCondition, rec TxRecordView) (match, known bool) {
	switch cond.GetField() {
	case commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_ID:
		return MatchUintBounds(cond.GetCond(), rec.Id()), true
	case commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_TIMESTAMP:
		if rec.Timestamp() == nil {
			return false, false
		}

		return MatchUintBounds(cond.GetCond(), rec.Timestamp().GetData()), true
	case commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_INSERTED_AT:
		if rec.InsertedAt() == nil {
			return false, false
		}

		return MatchUintBounds(cond.GetCond(), rec.InsertedAt().GetData()), true
	case commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REVERTED_AT:
		// Only reverted originals carry an rvat row. An un-reverted record is a
		// known miss; a reverted one with an unlearned revert stamp is unknown.
		if !rec.Reverted() {
			return false, true
		}
		if rec.RevertedAt() == nil {
			return false, false
		}

		return MatchUintBounds(cond.GetCond(), rec.RevertedAt().GetData()), true
	default:
		return false, true
	}
}

func reverseStrings(s []string) {
	for i, j := 0, len(s)-1; i < j; i, j = i+1, j-1 {
		s[i], s[j] = s[j], s[i]
	}
}

func joinUint64(s []uint64) string {
	parts := make([]string, len(s))
	for i, v := range s {
		parts[i] = strconv.FormatUint(v, 10)
	}

	return strings.Join(parts, ",")
}

// DescribeFilter renders a filter as a compact prefix expression for assertion
// details and debug logs.
func DescribeFilter(f *commonpb.QueryFilter) string {
	return FoldFilter(f, FilterFold[string]{
		And: func(children []string) string { return "and(" + strings.Join(children, ",") + ")" },
		Or:  func(children []string) string { return "or(" + strings.Join(children, ",") + ")" },
		Not: func(child string) string { return "not(" + child + ")" },
		Leaf: func(leaf *commonpb.QueryFilter) string {
			switch x := leaf.GetFilter().(type) {
			case nil:
				return "*"
			case *commonpb.QueryFilter_Address:
				role := ""
				switch x.Address.GetRole() {
				case commonpb.AddressRole_ADDRESS_ROLE_SOURCE:
					role = "/src"
				case commonpb.AddressRole_ADDRESS_ROLE_DESTINATION:
					role = "/dst"
				}

				switch m := x.Address.GetMatch().(type) {
				case *commonpb.AddressMatch_HardcodedPrefix:
					return "addr^" + m.HardcodedPrefix + role
				case *commonpb.AddressMatch_HardcodedExact:
					return "addr=" + m.HardcodedExact + role
				}

				return "addr?"
			case *commonpb.QueryFilter_Reverted:
				return "reverted=" + strconv.FormatBool(x.Reverted.GetValue())
			case *commonpb.QueryFilter_Reference:
				return "ref=" + x.Reference.GetCond().GetHardcoded()
			case *commonpb.QueryFilter_BuiltinUint:
				return DescribeBuiltinUint(x.BuiltinUint)
			case *commonpb.QueryFilter_Field:
				return "field:" + x.Field.GetField().GetMetadata() + DescribeFieldCondition(x.Field)
			case *commonpb.QueryFilter_AccountHasAsset:
				return "hasAsset:" + x.AccountHasAsset.GetAssetBase() + "/" + strconv.FormatUint(uint64(x.AccountHasAsset.GetPrecision()), 10)
			case *commonpb.QueryFilter_Ledger:
				return "ledger=" + x.Ledger.GetCond().GetHardcoded()
			case *commonpb.QueryFilter_LogId:
				return "logId" + DescribeUintBounds(x.LogId.GetCond())
			case *commonpb.QueryFilter_LogBuiltinUint:
				return "logDate" + DescribeUintBounds(x.LogBuiltinUint.GetCond())
			case *commonpb.QueryFilter_Audit:
				field := "audit:" + strings.TrimPrefix(x.Audit.GetField().String(), "AUDIT_FIELD_")
				if uc := x.Audit.GetUintCond(); uc != nil {
					return field + DescribeUintBounds(uc)
				}

				if p, isPrefix := x.Audit.GetCondition().(*commonpb.AuditCondition_StringPrefix); isPrefix {
					return field + "^=" + p.StringPrefix
				}

				if param := x.Audit.GetStringCond().GetParam(); param != "" {
					return field + "=$" + param
				}

				return field + "=" + x.Audit.GetStringCond().GetHardcoded()
			default:
				return "?"
			}
		},
	})
}

// DescribeBuiltinUint renders a builtin-uint leaf: the field's short name and
// its bounds, exclusive bounds parenthesized, absent ones as "_".
func DescribeBuiltinUint(c *commonpb.BuiltinUintCondition) string {
	name := map[commonpb.TransactionBuiltinIndex]string{
		commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_ID:          "id",
		commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_TIMESTAMP:   "ts",
		commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_INSERTED_AT: "iat",
		commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REVERTED_AT: "rvat",
	}[c.GetField()]
	if name == "" {
		name = c.GetField().String()
	}

	return name + DescribeUintBounds(c.GetCond())
}

// DescribeUintBounds renders a uint range, exclusive bounds parenthesized and
// absent ones as "_". A finding must be diagnosable from its details alone, so
// every generated leaf renders its bounds.
func DescribeUintBounds(cond *commonpb.UintCondition) string {
	if cond == nil {
		return "[nil]"
	}

	lo, hi := "_", "_"
	if cond.Min != nil {
		lo = strconv.FormatUint(cond.GetMin(), 10)
		if cond.GetMinExclusive() {
			lo = "(" + lo
		}
	}

	if cond.Max != nil {
		hi = strconv.FormatUint(cond.GetMax(), 10)
		if cond.GetMaxExclusive() {
			hi += ")"
		}
	}

	return "[" + lo + "," + hi + "]"
}

// DescribeFieldCondition renders a Field leaf's condition — the bounds are
// what a retype-window finding needs to be diagnosable from its details alone
// (which coercions the serving types apply to them decides the verdict).
func DescribeFieldCondition(fc *commonpb.FieldCondition) string {
	switch c := fc.GetCondition().(type) {
	case *commonpb.FieldCondition_ExistsCond:
		if c.ExistsCond.GetIncludeNull() {
			return "?exists+null"
		}

		return "?exists"
	case *commonpb.FieldCondition_StringCond:
		return "=" + strconv.Quote(c.StringCond.GetHardcoded())
	case *commonpb.FieldCondition_BoolCond:
		return "=" + strconv.FormatBool(c.BoolCond.GetHardcoded())
	case *commonpb.FieldCondition_IntCond:
		ic := c.IntCond

		// min/max are proto3 optional: RenderBound needs the pointer to tell an
		// unset bound ("_") from a zero one.
		//nolint:protogetter
		return "[" + RenderBound(ic.Min, ic.GetMinExclusive(), false) + "," + RenderBound(ic.Max, ic.GetMaxExclusive(), true) + "]"
	case *commonpb.FieldCondition_UintCond:
		uc := c.UintCond

		//nolint:protogetter // see DescribeFieldCondition's IntCond case
		return "u[" + RenderBound(uc.Min, uc.GetMinExclusive(), false) + "," + RenderBound(uc.Max, uc.GetMaxExclusive(), true) + "]"
	default:
		return "?"
	}
}
