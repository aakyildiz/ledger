package match

import (
	"fmt"
	"math"
	"strings"

	"github.com/formancehq/ledger/v3/internal/domain/indexes"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/tests/oracle"
	"google.golang.org/grpc/codes"
)

// AssetIndexID is the account-by-asset builtin index the has-asset condition is
// served from. AssetIndexCanonical is its stable map key.
func AssetIndexID() *commonpb.IndexID {
	return indexes.AccountBuiltinID(commonpb.AccountBuiltinIndex_ACCT_BUILTIN_INDEX_ASSET)
}

var AssetIndexCanonical = indexes.Canonical(AssetIndexID())

// WorkloadTxBuiltins are the transaction builtin indexes the generator churns:
// the index-backed leaves of the transactions filter grammar (reference, the
// three date fields, and the three address-role account→tx mappings).
var WorkloadTxBuiltins = []commonpb.TransactionBuiltinIndex{
	commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE,
	commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_TIMESTAMP,
	commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_INSERTED_AT,
	commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REVERTED_AT,
	commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_ADDRESS,
	commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_SOURCE_ADDRESS,
	commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_DESTINATION_ADDRESS,
}

// AddressRoleBuiltin maps an address role to the tx builtin index serving it —
// the model's copy of the compiler's txAddressIndexID.
func AddressRoleBuiltin(role commonpb.AddressRole) commonpb.TransactionBuiltinIndex {
	switch role {
	case commonpb.AddressRole_ADDRESS_ROLE_SOURCE:
		return commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_SOURCE_ADDRESS
	case commonpb.AddressRole_ADDRESS_ROLE_DESTINATION:
		return commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_DESTINATION_ADDRESS
	default:
		return commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_ADDRESS
	}
}

// TxBuiltinCanonical returns the canonical IndexID string of a tx builtin — the
// model's index-map key the filter classifier and the lifecycle validation
// share.
func TxBuiltinCanonical(field commonpb.TransactionBuiltinIndex) string {
	return indexes.Canonical(indexes.TxBuiltinID(field))
}

// WorkloadIndex is one index the generator churns: its wire ID and canonical key.
type WorkloadIndex struct {
	ID        *commonpb.IndexID
	Canonical string
}

// WorkloadIndexes is every index the workload creates and drops: the
// account-by-asset builtin, the tx builtins, and the log-date builtin the LOGS
// date filters are served from.
func WorkloadIndexes() []WorkloadIndex {
	out := []WorkloadIndex{
		{AssetIndexID(), AssetIndexCanonical},
		{LogDateIndexID(), LogDateIndexCanonical},
	}
	for _, field := range WorkloadTxBuiltins {
		out = append(out, WorkloadIndex{indexes.TxBuiltinID(field), TxBuiltinCanonical(field)})
	}

	return out
}

// IndexStateLabel renders an index's model lifecycle state for finding details.
// IndexStateLabelFull renders one canonical's model state, retype window
// included, for finding diagnostics.
func IndexStateLabelFull(ls oracle.LedgerState, canon string) string {
	exists, active := ls.IndexState(canon)
	label := IndexStateLabel(exists, active)
	if oldTypes, open := ls.RetypeWindow(canon); open {
		parts := make([]string, len(oldTypes))
		for i, t := range oldTypes {
			parts[i] = fmt.Sprintf("%d", t)
		}
		label += fmt.Sprintf("+window(old=%s)", strings.Join(parts, ","))
	}

	return label
}

func IndexStateLabel(exists, active bool) string {
	switch {
	case !exists:
		return "absent"
	case active:
		return "active"
	default:
		return "ambiguous"
	}
}

// WorkloadAsset is a (base, precision) pair matching one of the workload's asset
// strings (config.go assets), so a has-asset filter built from it can match the
// accounts those postings touched.
type WorkloadAsset struct {
	Base      string
	Precision uint32
}

// WorkloadAssets mirrors config.go assets {"USD/2","EUR/2","COIN"} split into
// (base, precision) via the server's asset-string convention (no "/N" suffix
// means precision 0).
var WorkloadAssets = []WorkloadAsset{{"USD", 2}, {"EUR", 2}, {"COIN", 0}}

// AccountHasAsset reports whether addr is in the account-by-asset index for
// (assetBase, precision): whether it has EVER touched that asset via a committed,
// non-excluded posting. This is a monotonic history the oracle tracks (see
// LedgerState.everAsset / recordAssetTouches), NOT the current volume set — an
// account drained to zero and purged from the volume table still matches.
func AccountHasAsset(ls oracle.LedgerState, addr, assetBase string, precision uint32) bool {
	return ls.HasEverAsset(addr, assetBase, precision)
}

// AssetWindow is the model's prediction of a bare has-asset ListAccounts page:
// the accounts that ever touched (base, precision), in address order (reversed
// when reverse), with the exclusive cursor applied and capped at pageSize —
// mirroring AccountWindow's pagination over the ever-touched set instead of the
// current-volume universe.
func AssetWindow(ls oracle.LedgerState, base string, precision uint32, cursor string, pageSize int, reverse bool) []string {
	window := ls.EverAssetAccounts(base, precision)

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

// HasAssetTarget extracts the (base, precision) of a bare AccountHasAsset filter.
func HasAssetTarget(f *commonpb.QueryFilter) (base string, precision uint32, ok bool) {
	if ha, isHA := f.GetFilter().(*commonpb.QueryFilter_AccountHasAsset); isHA {
		return ha.AccountHasAsset.GetAssetBase(), ha.AccountHasAsset.GetPrecision(), true
	}

	return "", 0, false
}

// HasAssetPrecisionOverflow reports whether f is a bare has-asset leaf whose
// precision exceeds the index cell.
func HasAssetPrecisionOverflow(f *commonpb.QueryFilter) bool {
	return f.GetAccountHasAsset() != nil && f.GetAccountHasAsset().GetPrecision() > math.MaxUint8
}

// DescribeAccountContentDiff renders the first content divergence between a
// base's view of addr and the server's enriched row — the diagnostic for a
// finding whose page ADDRESSES match a candidate base while a row's content
// does not. Empty when the row matches.
func DescribeAccountContentDiff(ls oracle.LedgerState, addr string, serverAcct *commonpb.Account) string {
	modelMeta := ls.AccountMetadata(addr)
	serverMeta := serverAcct.GetMetadata()

	for k, v := range modelMeta {
		sv, ok := serverMeta[k]
		if !ok {
			return fmt.Sprintf("%s meta[%s] model=%s server=<absent>", addr, k, oracle.MetaValueString(v))
		}

		if oracle.MetaValueString(sv) != oracle.MetaValueString(v) {
			return fmt.Sprintf("%s meta[%s] model=%s server=%s", addr, k, oracle.MetaValueString(v), oracle.MetaValueString(sv))
		}
	}

	for k, sv := range serverMeta {
		if _, ok := modelMeta[k]; !ok {
			return fmt.Sprintf("%s meta[%s] model=<absent> server=%s", addr, k, oracle.MetaValueString(sv))
		}
	}

	model := map[AssetColor]oracle.VolumePair{}
	for k, vp := range ls.Volumes().All() {
		if k.Address == addr {
			model[AssetColor{Asset: k.Asset, Color: k.Color}] = vp
		}
	}

	seen := map[AssetColor]bool{}
	for _, av := range serverAcct.GetVolumes() {
		key := AssetColor{Asset: av.GetAsset(), Color: av.GetColor()}
		seen[key] = true

		vp, ok := model[key]
		if !ok {
			return fmt.Sprintf("%s volumes[%s] model=<absent> server=in:%s,out:%s", addr, key, av.GetVolumes().GetInput().DecimalString(), av.GetVolumes().GetOutput().DecimalString())
		}

		if vp.Input.Dec() != av.GetVolumes().GetInput().DecimalString() || vp.Output.Dec() != av.GetVolumes().GetOutput().DecimalString() {
			return fmt.Sprintf("%s volumes[%s] model=in:%s,out:%s server=in:%s,out:%s", addr, key, vp.Input.Dec(), vp.Output.Dec(), av.GetVolumes().GetInput().DecimalString(), av.GetVolumes().GetOutput().DecimalString())
		}
	}

	for key, vp := range model {
		if !seen[key] {
			return fmt.Sprintf("%s volumes[%s] model=in:%s,out:%s server=<absent>", addr, key, vp.Input.Dec(), vp.Output.Dec())
		}
	}

	return ""
}

// LedgerIndexPool is every index the workload churns on one ledger: the
// builtins plus one metadata index per declared field of both indexable
// targets, so metadata index lifecycles churn alongside the builtins.
func LedgerIndexPool(ls oracle.LedgerState) []WorkloadIndex {
	all := WorkloadIndexes()
	for _, target := range []commonpb.TargetType{
		commonpb.TargetType_TARGET_TYPE_ACCOUNT,
		commonpb.TargetType_TARGET_TYPE_TRANSACTION,
	} {
		for key := range ls.FieldTypesFor(target).All() {
			id := indexes.MetadataID(target, key)
			all = append(all, WorkloadIndex{id, indexes.Canonical(id)})
		}
	}

	return all
}

// AbsentIndexes is the subset of pool the ledger does not hold.
func AbsentIndexes(ls oracle.LedgerState, pool []WorkloadIndex) []WorkloadIndex {
	var absent []WorkloadIndex
	for _, candidate := range pool {
		if exists, _ := ls.IndexState(candidate.Canonical); !exists {
			absent = append(absent, candidate)
		}
	}

	return absent
}

// HasIndexGap reports whether the ledger is missing any index the workload
// churns. generateIndexOp names the gap it fills; this only decides whether one
// is worth looking for, so it stops at the first.
func HasIndexGap(ls oracle.LedgerState) bool {
	for _, candidate := range LedgerIndexPool(ls) {
		if exists, _ := ls.IndexState(candidate.Canonical); !exists {
			return true
		}
	}

	return false
}

// IndexStatusViolation names what is internally inconsistent about an index
// status, or "" when nothing is. lag is derived from the other two counters in
// the same response (controller GetIndexStatus), so it is an exact identity —
// the only relation between them that holds, since the indexed sequence and the
// log sequence are read from two different snapshots and either may lead.
func IndexStatusViolation(resp *servicepb.GetIndexStatusResponse) string {
	lastIndexed, lastLog := resp.GetLastIndexedSequence(), resp.GetLastLogSequence()

	want := uint64(0)
	if lastLog > lastIndexed {
		want = lastLog - lastIndexed
	}

	if resp.GetLag() != want {
		return "lag is not the distance between the log and the index"
	}

	return ""
}

// RejectedIndexLabel preserves the index identity carried by a documented
// readiness refusal. The reason match prevents an unrelated detail from
// granting the retype-window exception.
func RejectedIndexLabel(err RPCError) string {
	if !IsIndexNotFound(err) && !IsIndexNotReady(err) {
		return ""
	}

	return err.Metadata["index"]
}

// MetadataCanonical is the canonical IndexID of the per-(target, key) metadata
// index serving Field conditions on the given query target.
func MetadataCanonical(target commonpb.QueryTarget, key string) string {
	tt := commonpb.TargetType_TARGET_TYPE_ACCOUNT
	if target == commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS {
		tt = commonpb.TargetType_TARGET_TYPE_TRANSACTION
	}

	return indexes.Canonical(indexes.MetadataID(tt, key))
}

// IndexedQueryOutcomeLegal is the shared per-candidate verdict for a query
// whose filter needs indexes. Per Field leaf the compiler checks
// schema → requireIndexReady → kind coercion, but across a multi-leaf tree
// the surfaced error follows the compiler's walk order, which the model does
// not pin down. So:
//
//   - a FailedPrecondition (index not found / not ready) is legal iff some
//     needed index is not active on the base;
//   - a FILTER_COMPILATION rejection (InvalidArgument) is legal iff the filter
//     carries a kind-mismatched Field leaf under the base's declared types —
//     regardless of the other leaves' index states: when a mismatched leaf and
//     an absent index coexist (e.g. or(field:absent, field:mismatched)),
//     whichever leaf the walk reaches first decides which honest rejection
//     surfaces, and both are legal;
//   - results are legal iff there is no kind mismatch, every needed index
//     exists, and the window matches (windowMatches).
//
// A retype of an indexed key opens a serving window (EN-1724): the schema
// flips at commit, but each replica keeps serving the index under its current
// version's bound type until that rewrite's atomic switch — and a CHAIN of
// retypes walks the replica through every intermediate binding, so the window
// accumulates a SET of possible serving types. A query during the window is
// legal under the current declared type or any accumulated one — each as a
// WHOLE window, since one query is served from one snapshot of one replica —
// and the flip is per-index, so a filter touching several windowed keys may
// see any combination. The verdict enumerates those assignments over the
// existing single-view legality and accepts any.
func IndexedQueryOutcomeLegal(
	ls oracle.LedgerState,
	target commonpb.QueryTarget,
	filter *commonpb.QueryFilter,
	needed map[string]struct{},
	errKind IndexedErrKind,
	rejectedIndex string,
	windowMatches func(oracle.LedgerState) bool,
) bool {
	refs := WindowedFieldRefs(ls, target, filter)

	// While any referenced key's window is open, a switch may just have
	// landed above the query's pin, in which case the server honestly
	// refuses the read (pin below the promoted version's activation
	// sequence) instead of serving it — the same not-ready class a building
	// index produces.
	// Attribute the refusal to the windowed index through the server's
	// ErrorInfo metadata. A sibling index being active must still make its
	// own refusal a finding, even when this filter also touches a retype.
	if errKind == IndexedErrNotReady {
		targetName := commonpb.TargetHumanName(target)
		for _, ref := range refs {
			if rejectedIndex == fmt.Sprintf("metadata[%q] on %s", ref.Key, targetName) {
				return true
			}
		}
	}

	tt := commonpb.TargetType_TARGET_TYPE_ACCOUNT
	if target == commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS {
		tt = commonpb.TargetType_TARGET_TYPE_TRANSACTION
	}

	// Cross-product over the per-key choices: the current declared type
	// (choice 0) or any type the window accumulated. Capped: a filter
	// referencing this many windowed keys and chained types is beyond anything
	// the generator produces; enumerating further would only burn the checker.
	const maxViews = 256

	views := 0

	var enumerate func(view oracle.LedgerState, i int) bool
	enumerate = func(view oracle.LedgerState, i int) bool {
		if i == len(refs) {
			views++

			return IndexedQueryOutcomeLegalUnder(view, target, filter, needed, errKind, windowMatches)
		}

		if enumerate(view, i+1) {
			return true
		}

		for _, t := range refs[i].Types {
			if views >= maxViews {
				return false
			}

			if enumerate(view.WithDeclaredType(tt, refs[i].Key, t), i+1) {
				return true
			}
		}

		return false
	}

	return enumerate(ls, 0)
}

// WindowedFieldRefs collects the filter's distinct metadata keys whose index
// has an open retype window on this base, with the accumulated types each
// window may still serve.
func WindowedFieldRefs(ls oracle.LedgerState, target commonpb.QueryTarget, f *commonpb.QueryFilter) []WindowedRef {
	seen := map[string]bool{}

	var out []WindowedRef

	VisitLeaves(f, func(leaf *commonpb.QueryFilter) {
		x, ok := leaf.GetFilter().(*commonpb.QueryFilter_Field)
		if !ok {
			return
		}

		key := x.Field.GetField().GetMetadata()
		if seen[key] {
			return
		}

		seen[key] = true
		if oldTypes, open := ls.RetypeWindow(MetadataCanonical(target, key)); open {
			out = append(out, WindowedRef{Key: key, Types: oldTypes})
		}
	})

	return out
}

type WindowedRef struct {
	Key   string
	Types []commonpb.MetadataType
}

func IndexedQueryOutcomeLegalUnder(
	ls oracle.LedgerState,
	target commonpb.QueryTarget,
	filter *commonpb.QueryFilter,
	needed map[string]struct{},
	errKind IndexedErrKind,
	windowMatches func(oracle.LedgerState) bool,
) bool {
	switch errKind {
	case IndexedErrNotReady:
		for canon := range needed {
			if exists, active := ls.IndexState(canon); !exists || !active {
				return true
			}
		}

		return false
	case IndexedErrCompilation:
		return FieldKindMismatch(ls, filter, target)
	default: // results
		if FieldKindMismatch(ls, filter, target) {
			return false
		}
		for canon := range needed {
			if exists, _ := ls.IndexState(canon); !exists {
				return false
			}
		}

		return windowMatches(ls)
	}
}

// IndexedErrKind classifies the observed outcome of an index-backed query.
type IndexedErrKind int

const (
	IndexedErrNone IndexedErrKind = iota
	IndexedErrNotReady
	IndexedErrCompilation
)

// ClassifyIndexedQueryError buckets err for IndexedQueryOutcomeLegal; ok=false
// means the code is not part of the indexed-query surface at all (a finding).
func ClassifyIndexedQueryError(err RPCError) (IndexedErrKind, bool) {
	switch {
	case err.OK():
		return IndexedErrNone, true
	case IsIndexNotFound(err):
		// ErrIndexNotFound: the filter needs an index no replica holds. The
		// reason is required — an unrelated FailedPrecondition explained away
		// as a missing index would hide a server regression.
		return IndexedErrNotReady, true
	case IsIndexNotReady(err):
		// ErrIndexBuilding rides codes.Unavailable so SDK retry predicates
		// retry it; the reason is what identifies it as the not-ready class.
		return IndexedErrNotReady, true
	case err.Code == codes.InvalidArgument && err.HasReason("FILTER_COMPILATION_ERROR"):
		return IndexedErrCompilation, true
	default:
		return IndexedErrNone, false
	}
}
