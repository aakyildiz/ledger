package auditcheck

import (
	"encoding/json"
	"math"
	"strings"

	"google.golang.org/protobuf/encoding/protojson"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/pkg/pagecursor"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck/match"
)

var unmarshalOpts = protojson.UnmarshalOptions{DiscardUnknown: true}

// ListRequest is what the validator reads off a ListAuditEntriesRequest.
type ListRequest struct {
	Filter            *commonpb.QueryFilter
	RequestedPageSize int
	PageSize          int // match.EffectivePageSize(RequestedPageSize)
	Cursor            string
	Reverse           bool
}

// DecodeListRequest decodes the protojson ListAuditEntriesRequest.
func DecodeListRequest(raw json.RawMessage) (ListRequest, error) {
	var w struct {
		Options json.RawMessage `json:"options"`
	}
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &w); err != nil {
			return ListRequest{}, err
		}
	}
	opts := &commonpb.ListOptions{}
	if len(w.Options) > 0 && string(w.Options) != "null" {
		if err := unmarshalOpts.Unmarshal(w.Options, opts); err != nil {
			return ListRequest{}, err
		}
	}
	requested := int(opts.GetPageSize())

	return ListRequest{
		Filter:            opts.GetFilter(),
		RequestedPageSize: requested,
		PageSize:          match.EffectivePageSize(requested),
		Cursor:            opts.GetCursor(),
		Reverse:           opts.GetReverse(),
	}, nil
}

// DecodeGetRequest decodes the protojson GetAuditEntryRequest's sequence.
func DecodeGetRequest(raw json.RawMessage) (uint64, error) {
	req := &servicepb.GetAuditEntryRequest{}
	if err := unmarshalOpts.Unmarshal(raw, req); err != nil {
		return 0, err
	}

	return req.GetSequence(), nil
}

// UintCursor reads an audit page token (keyed by a decimal audit sequence).
// malformed is set for a token the server must refuse: one that does not
// decode, a back token, or a key that is not a uint64 — the malformed-cursor
// menu of M/cursors.go, raw or wrapped.
func UintCursor(token string) (afterSeq uint64, malformed bool) {
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

// Probe names the refusal an audit filter draws (M/audit.go auditProbe). The
// validator derives it from the filter the driver sent, mirroring the
// server's audit compiler (internal/query/audit_filter.go compileAuditNode) in
// compile order, so the first refusal it meets is the one the server reports.
type Probe int

const (
	ProbeNone Probe = iota
	// A non-narrowed disjunct (a satisfiable sequence bound) inside Or.
	ProbeSeqInOr
	// Not, or any condition kind the audit target does not admit.
	ProbeNot
	// The unspecified (or an unknown) audit field.
	ProbeUnspecifiedField
	// A string condition on a uint field, the reverse, or an invalid value.
	ProbeTypeMismatch
	// A parameterised idempotency-key condition.
	ProbeKeyParam
	// A NUL in an idempotency-key prefix.
	ProbeNulPrefix
)

// maxFilterDepth mirrors domain.MaxFilterDepth through query.MaxFilterDepth.
const maxFilterDepth = 100

// ClassifyProbe reports the refusal the server answers filter with, or
// ProbeNone for a filter it serves.
func ClassifyProbe(filter *commonpb.QueryFilter) Probe {
	if filter == nil {
		return ProbeNone
	}
	_, p := compileNode(filter, 0)

	return p
}

// compileNode returns whether the node is index-narrowed and the first
// refusal under it.
func compileNode(f *commonpb.QueryFilter, depth int) (narrowed bool, p Probe) {
	if depth >= maxFilterDepth {
		return false, ProbeTypeMismatch
	}
	switch n := f.GetFilter().(type) {
	case *commonpb.QueryFilter_Audit:
		return compileLeaf(n.Audit)
	case *commonpb.QueryFilter_And:
		narrowed := false
		for _, c := range n.And.GetFilters() {
			cn, cp := compileNode(c, depth+1)
			if cp != ProbeNone {
				return false, cp
			}
			narrowed = narrowed || cn
		}

		return narrowed, ProbeNone
	case *commonpb.QueryFilter_Or:
		for _, c := range n.Or.GetFilters() {
			cn, cp := compileNode(c, depth+1)
			if cp != ProbeNone {
				return false, cp
			}
			if !cn {
				return false, ProbeSeqInOr
			}
		}

		return true, ProbeNone
	default:
		// Not, an empty filter and every non-audit kind fail the per-target
		// validity gate.
		return false, ProbeNot
	}
}

func compileLeaf(a *commonpb.AuditCondition) (bool, Probe) {
	switch a.GetField() {
	case commonpb.AuditField_AUDIT_FIELD_SEQUENCE:
		uc := a.GetUintCond()
		if uc == nil {
			return false, ProbeTypeMismatch
		}
		if uc.GetParamMin() != "" || uc.GetParamMax() != "" {
			return false, ProbeTypeMismatch
		}

		return uintRangeEmpty(uc), ProbeNone
	case commonpb.AuditField_AUDIT_FIELD_OUTCOME:
		v := a.GetStringCond().GetHardcoded()
		if a.GetStringCond() == nil || (v != "success" && v != "failure") {
			return false, ProbeTypeMismatch
		}

		return true, ProbeNone
	case commonpb.AuditField_AUDIT_FIELD_LEDGER, commonpb.AuditField_AUDIT_FIELD_CALLER_SUBJECT,
		commonpb.AuditField_AUDIT_FIELD_ORDER_TYPE:
		sc := a.GetStringCond()
		if sc == nil || sc.GetParam() != "" {
			return false, ProbeTypeMismatch
		}

		return true, ProbeNone
	case commonpb.AuditField_AUDIT_FIELD_IDEMPOTENCY_KEY:
		switch c := a.GetCondition().(type) {
		case *commonpb.AuditCondition_StringCond:
			if c.StringCond.GetParam() != "" {
				return false, ProbeKeyParam
			}

			return true, ProbeNone
		case *commonpb.AuditCondition_StringPrefix:
			if strings.ContainsRune(c.StringPrefix, '\x00') {
				return false, ProbeNulPrefix
			}

			return true, ProbeNone
		default:
			return false, ProbeTypeMismatch
		}
	case commonpb.AuditField_AUDIT_FIELD_PROPOSAL_ID, commonpb.AuditField_AUDIT_FIELD_TIMESTAMP,
		commonpb.AuditField_AUDIT_FIELD_LOG_SEQUENCE:
		uc := a.GetUintCond()
		if uc == nil || uc.GetParamMin() != "" || uc.GetParamMax() != "" {
			return false, ProbeTypeMismatch
		}

		return true, ProbeNone
	default:
		return false, ProbeUnspecifiedField
	}
}

// uintRangeEmpty reports whether a uint condition holds no value (the server
// compiles such a sequence bound to an empty narrowed set).
func uintRangeEmpty(uc *commonpb.UintCondition) bool {
	lo, hi := uint64(0), uint64(math.MaxUint64)
	if uc.Min != nil {
		lo = uc.GetMin()
		if uc.GetMinExclusive() {
			if lo == math.MaxUint64 {
				return true
			}
			lo++
		}
	}
	if uc.Max != nil {
		hi = uc.GetMax()
		if uc.GetMaxExclusive() {
			if hi == 0 {
				return true
			}
			hi--
		}
	}

	return lo > hi
}

// NeedsIndex mirrors query.AuditFilterNeedsIndex: whether the server may
// answer the filter through the audit projection (and so may report it
// rebuilding with INDEX_BUILDING).
func NeedsIndex(filter *commonpb.QueryFilter) bool {
	return !match.AuditFilterIndexFree(filter)
}
