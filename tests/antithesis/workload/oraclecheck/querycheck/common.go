package querycheck

import (
	"encoding/json"
	"fmt"
	"slices"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/pkg/pagecursor"
	oc "github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck/match"
	"github.com/formancehq/ledger/v3/tests/oracle"
)

// ListRequest is what a list validator reads off the request the driver
// sent: ListAccountsRequest, ListTransactionsRequest and ListLogsRequest share
// the {ledger, options} shape.
type ListRequest struct {
	Ledger            string
	Filter            *commonpb.QueryFilter
	RequestedPageSize int
	PageSize          int // match.EffectivePageSize(RequestedPageSize)
	Cursor            string
	Reverse           bool
}

// listRequestWire is the protojson shape of the three requests.
type listRequestWire struct {
	Ledger  string          `json:"ledger"`
	Options json.RawMessage `json:"options"`
}

var unmarshalOpts = protojson.UnmarshalOptions{DiscardUnknown: true}

// DecodeListRequest decodes the protojson request of a list read.
func DecodeListRequest(raw json.RawMessage) (ListRequest, error) {
	var w listRequestWire
	if err := json.Unmarshal(raw, &w); err != nil {
		return ListRequest{}, err
	}
	opts := &commonpb.ListOptions{}
	if len(w.Options) > 0 && string(w.Options) != "null" {
		if err := unmarshalOpts.Unmarshal(w.Options, opts); err != nil {
			return ListRequest{}, err
		}
	}
	requested := int(opts.GetPageSize())

	return ListRequest{
		Ledger:            w.Ledger,
		Filter:            opts.GetFilter(),
		RequestedPageSize: requested,
		PageSize:          match.EffectivePageSize(requested),
		Cursor:            opts.GetCursor(),
		Reverse:           opts.GetReverse(),
	}, nil
}

// CursorKey is the forward key of a page token ("" for the first page or a
// token that does not decode).
func CursorKey(token string) string {
	c, err := pagecursor.Decode(token)
	if err != nil || c.Back {
		return ""
	}

	return c.Key
}

// UintCursor reads the token of an endpoint keyed by a decimal id
// (transactions, logs). malformed is set for a token the server must refuse:
// one that does not decode, a back token, or a key that is not a uint64 —
// the malformed-cursor menu of M/cursors.go, raw or wrapped.
func UintCursor(token string) (afterID uint64, malformed bool) {
	if token == "" {
		return 0, false
	}
	c, err := pagecursor.Decode(token)
	if err != nil || c.Back {
		return 0, true
	}
	v, err := c.Uint64()
	if err != nil {
		return 0, true
	}

	return v, false
}

// pageSizeEvents reports the reads whose page size the server chose (M/queries.go
// noteClampedPageSize): one literal per substitution.
func pageSizeEvents(req ListRequest) []oc.Event {
	switch {
	case req.RequestedPageSize == 0:
		return []oc.Event{oc.Reached(EvPageSizeServerDefault, map[string]any{"effective": req.PageSize})}
	case req.RequestedPageSize > req.PageSize:
		return []oc.Event{oc.Reached(EvPageSizeClamped, map[string]any{"requested": req.RequestedPageSize})}
	}

	return nil
}

// rpcError is the outcome's error in the matchers' form (NoError on success).
func rpcError(o oc.Outcome) match.RPCError {
	if o.OK {
		return match.NoError
	}

	return match.RPCErrorFrom(o.Code, o.Reason, o.Metadata)
}

func errText(o oc.Outcome) string {
	return fmt.Sprintf("%s/%s: %s", o.Code, o.Reason, o.Message)
}

// decodeItems decodes a stream page into protos. ok is false when an item
// does not decode (reported as a divergence by the caller).
func decodeItems[T any, P interface {
	*T
	proto.Message
}](items []json.RawMessage) ([]P, bool) {
	out := make([]P, 0, len(items))
	for _, raw := range items {
		m := P(new(T))
		if err := unmarshalOpts.Unmarshal(raw, m); err != nil {
			return nil, false
		}
		out = append(out, m)
	}

	return out, true
}

// handleInvalidTargetError (M/queries.go) validates the error a target-invalid
// bare leaf drew: InvalidArgument. handled=false when the filter is not one.
func handleInvalidTargetError(invalidTarget bool, target commonpb.QueryTarget, ledger string, filter *commonpb.QueryFilter, o oc.Outcome) ([]oc.Event, bool) {
	if !invalidTarget {
		return nil, false
	}
	if rpcError(o).Code == codes.InvalidArgument {
		if target == commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS {
			return []oc.Event{oc.Reached(EvTargetInvalidRejectedAccounts, map[string]any{"ledger": ledger})}, true
		}

		return []oc.Event{oc.Reached(EvTargetInvalidRejectedTxs, map[string]any{"ledger": ledger})}, true
	}

	return []oc.Event{oc.Violation(EvTargetInvalidUnexpectedError, map[string]any{
		"target": target.String(), "ledger": ledger, "filter": match.DescribeFilter(filter), "error": errText(o),
	})}, true
}

// handleMalformedCursorError (M/cursors.go) validates a token the server
// cannot decode: InvalidArgument, and no page.
func handleMalformedCursorError(malformed bool, target commonpb.QueryTarget, cursor string, o oc.Outcome) ([]oc.Event, bool) {
	if !malformed {
		return nil, false
	}
	if rpcError(o).Code == codes.InvalidArgument {
		if target == commonpb.QueryTarget_QUERY_TARGET_LOGS {
			return []oc.Event{oc.Reached(EvMalformedLogCursorRejected, map[string]any{"cursor": cursor})}, true
		}

		return []oc.Event{oc.Reached(EvMalformedTxCursorRejected, map[string]any{"cursor": cursor})}, true
	}

	return []oc.Event{oc.Violation(EvMalformedCursorUnexpectedError, map[string]any{
		"target": target.String(), "cursor": cursor, "error": errText(o),
	})}, true
}

// handleRejectedFilterError (M/rejected_filter.go): a refused filter must draw
// InvalidArgument, or a not-ready error from an index an earlier leaf needs
// while some candidate base does not hold it active.
func handleRejectedFilterError(c *oc.ReadCtx, rf match.RejectedFilter, target commonpb.QueryTarget, ledger string, filter *commonpb.QueryFilter, o oc.Outcome) ([]oc.Event, bool) {
	if !rf.Rejected {
		return nil, false
	}
	err := rpcError(o)
	if err.Code == codes.InvalidArgument {
		var id oc.EventID
		switch target {
		case commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS:
			id = EvRefusedAccountFilterRejected
		case commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS:
			id = EvRefusedTxFilterRejected
		default:
			id = EvRefusedLogFilterRejected
		}

		return []oc.Event{oc.Reached(id, map[string]any{"ledger": ledger})}, true
	}
	if (match.IsIndexNotReady(err) || match.IsIndexNotFound(err)) && c.Matches(func(base oracle.GlobalState) bool {
		ls, live := match.LiveLedgerState(base, ledger)

		return live && match.RefusedFilterNotReadyLegal(ls, rf, target, err)
	}) {
		return nil, true
	}

	return []oc.Event{oc.Violation(EvRefusedFilterUnexpectedError, map[string]any{
		"target": target.String(), "ledger": ledger, "filter": match.DescribeFilter(filter), "error": errText(o),
	})}, true
}

func refusedFilterServed(rf match.RejectedFilter, target commonpb.QueryTarget, ledger string, filter *commonpb.QueryFilter, rows int) ([]oc.Event, bool) {
	if !rf.Rejected {
		return nil, false
	}

	return []oc.Event{oc.Violation(EvRefusedFilterResults, map[string]any{
		"target": target.String(), "ledger": ledger, "filter": match.DescribeFilter(filter), "rows": rows,
	})}, true
}

// unsupportedBuiltinFields name a real tx builtin index without a uint64
// order: a BuiltinUintCondition over one must be refused (M/queries.go).
var unsupportedBuiltinFields = map[commonpb.TransactionBuiltinIndex]bool{
	commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE:           true,
	commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_ADDRESS:             true,
	commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_SOURCE_ADDRESS:      true,
	commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_DESTINATION_ADDRESS: true,
}

// IsUnsupportedBuiltinProbe recognises the unsupported-builtin probe from the
// request: a bare BuiltinUint leaf over a field with no uint64 order.
func IsUnsupportedBuiltinProbe(f *commonpb.QueryFilter) bool {
	b := f.GetBuiltinUint()

	return b != nil && unsupportedBuiltinFields[b.GetField()]
}

func joinStrings(s []string) string { return strings.Join(s, ",") }

const codesInvalidArgument = codes.InvalidArgument

// indexStates renders the model's lifecycle state of each needed index
// (sorted), for a finding's diagnostics.
func indexStates(ls oracle.LedgerState, needed map[string]struct{}) string {
	states := make([]string, 0, len(needed))
	for canon := range needed {
		states = append(states, canon+"="+match.IndexStateLabelFull(ls, canon))
	}
	slices.Sort(states)

	return strings.Join(states, " ")
}
