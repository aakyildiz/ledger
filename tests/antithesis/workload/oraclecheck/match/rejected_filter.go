package match

import (
	"fmt"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/tests/oracle"
)

// RejectedFilter describes a filter the server must refuse whatever the
// ledger's state: some node fails the per-target validity or shape checks the
// compiler runs at dispatch, the tree is too deep, or a leaf references a
// parameter, which a plain list query never binds.
type RejectedFilter struct {
	Rejected bool
	// prior maps each index the leaves compiled before the first refused one
	// need to the label the server names it by. The compiler walks the tree in
	// order and checks each leaf's index as it compiles it, so a not-ready
	// rejection from one of these may come first.
	Prior map[string]PriorIndex
}

type PriorIndex struct {
	// label is the server's ErrorInfo "index" label; empty when unknown.
	Label string
	// metadata is set for a metadata index: under an open retype window it may
	// answer not-ready even while active.
	Metadata bool
}

// ClassifyRejectedFilter decides whether f must be refused on target, and which
// indexes may answer not-ready before the refusal is reached.
func ClassifyRejectedFilter(f *commonpb.QueryFilter, target commonpb.QueryTarget) RejectedFilter {
	if domain.ValidateFilterForTarget(f, target) == nil && len(CollectParams(f)) == 0 {
		return RejectedFilter{}
	}

	prior := map[string]PriorIndex{}
	stopped := false
	VisitLeaves(f, func(leaf *commonpb.QueryFilter) {
		if stopped {
			return
		}
		if leaf != nil && (!commonpb.ConditionValidForTarget(target, commonpb.ConditionKindOf(leaf)) || domain.ValidateFilterLeaf(leaf) != nil) {
			stopped = true

			return
		}

		// A leaf checks its index before it resolves its parameters.
		needed := map[string]struct{}{}
		if target == commonpb.QueryTarget_QUERY_TARGET_LOGS {
			needed = NeededLogIndexes(leaf)
		} else {
			NeededIndexCanonicals(leaf, target, needed)
		}
		for canon := range needed {
			prior[canon] = PriorIndex{
				Label:    ServerIndexLabel(leaf, target),
				Metadata: leaf.GetField().GetField().GetMetadata() != "",
			}
		}
		stopped = len(CollectParams(leaf)) > 0
	})

	return RejectedFilter{Rejected: true, Prior: prior}
}

// ServerIndexLabel is the label the compiler names leaf's index by in an
// INDEX_NOT_FOUND / INDEX_BUILDING ErrorInfo (internal/query/compile.go).
func ServerIndexLabel(leaf *commonpb.QueryFilter, target commonpb.QueryTarget) string {
	if target == commonpb.QueryTarget_QUERY_TARGET_LOGS {
		return "log date"
	}

	switch x := leaf.GetFilter().(type) {
	case *commonpb.QueryFilter_Field:
		return fmt.Sprintf("metadata[%q] on %s", x.Field.GetField().GetMetadata(), commonpb.TargetHumanName(target))
	case *commonpb.QueryFilter_AccountHasAsset:
		return "has asset"
	case *commonpb.QueryFilter_Reference:
		return "reference"
	case *commonpb.QueryFilter_Address:
		switch x.Address.GetRole() {
		case commonpb.AddressRole_ADDRESS_ROLE_SOURCE:
			return "source"
		case commonpb.AddressRole_ADDRESS_ROLE_DESTINATION:
			return "destination"
		default:
			return "address"
		}
	case *commonpb.QueryFilter_BuiltinUint:
		switch x.BuiltinUint.GetField() {
		case commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_TIMESTAMP:
			return "timestamp"
		case commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_INSERTED_AT:
			return "inserted_at"
		case commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REVERTED_AT:
			return "reverted_at"
		}
	}

	return ""
}

// RefusedFilterNotReadyLegal reports whether ls explains a not-ready answer to
// a refused filter: the refusal names an index a leaf compiled before the
// refused one needs, and that index is absent or not active, or is a metadata
// index under an open retype window. A refusal naming any other index stays
// illegal.
func RefusedFilterNotReadyLegal(ls oracle.LedgerState, rf RejectedFilter, target commonpb.QueryTarget, err RPCError) bool {
	rejectedIndex := RejectedIndexLabel(err)
	for canon, p := range rf.Prior {
		if p.Label != "" && p.Label != rejectedIndex {
			continue
		}

		if exists, active := ls.IndexState(canon); !exists || !active {
			return true
		}

		if _, open := ls.RetypeWindow(canon); p.Metadata && open && IsIndexNotReady(err) {
			return true
		}
	}

	return false
}
