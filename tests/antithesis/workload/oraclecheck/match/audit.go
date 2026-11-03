package match

import (
	"maps"
	"math"
	"slices"
	"strconv"
	"strings"

	"github.com/formancehq/ledger/v3/internal/proto/auditpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

// AuditEntry is one served entry reduced to what the model can pin.
type AuditEntry struct {
	Seq        uint64
	ProposalID uint64
	Timestamp  uint64   // unix microseconds
	Subject    string   // caller subject; empty when unauthenticated or system-initiated
	IdemKey    string   // batch dedup key, as the entry carries it
	Ledgers    []string // as served, ascending
	OrderCount uint32
	Failed     bool
	Reason     string   // failure only, in the domain's vocabulary
	MinLog     uint64   // success only
	MaxLog     uint64   // success only
	ItemSeqs   []uint64 // success only, one per order; empty once archived
}

func AuditEntryOf(e *auditpb.AuditEntry) AuditEntry {
	out := AuditEntry{
		Seq:        e.GetSequence(),
		ProposalID: e.GetProposalId(),
		Timestamp:  e.GetTimestamp().GetData(),
		Subject:    e.GetCallerSnapshot().GetAuthenticated().GetIdentity().GetSubject(),
		IdemKey:    e.GetIdempotency().GetKey(),
		Ledgers:    slices.Sorted(slices.Values(e.GetLedgers())),
		OrderCount: e.GetOrderCount(),
	}

	if f := e.GetFailure(); f != nil {
		out.Failed = true
		out.Reason = strings.TrimPrefix(f.GetReason().String(), "ERROR_REASON_")

		return out
	}

	out.MinLog = e.GetSuccess().GetMinLogSequence()
	out.MaxLog = e.GetSuccess().GetMaxLogSequence()

	for _, item := range e.GetItems() {
		out.ItemSeqs = append(out.ItemSeqs, item.GetLogSequence())
	}

	return out
}

// AuditEntryCertainlyMatches reports whether the filter certainly selects e.
// Every leaf but order-type is judged exactly off the entry's own fields; an
// order-type leaf holds only for the kinds the model committed under the entry's
// logs, so an entry whose logs the model does not know — a rejection, a
// system-scoped proposal — certifies nothing. A Not certifies nothing either:
// the audit grammar refuses it, so no served page carries one, and inverting a
// conservative verdict would not stay conservative.
func AuditEntryCertainlyMatches(filter *commonpb.QueryFilter, e AuditEntry, logs map[uint64]CommittedLog) bool {
	types := CommittedOrderTypes(e, logs)

	return FoldFilter(filter, FilterFold[bool]{
		And: allOf,
		Or:  anyOf,
		Not: func(bool) bool { return false },
		Leaf: func(leaf *commonpb.QueryFilter) bool {
			if a := leaf.GetAudit(); a.GetField() == commonpb.AuditField_AUDIT_FIELD_ORDER_TYPE {
				return types[a.GetStringCond().GetHardcoded()]
			}

			return AuditEntrySatisfies(leaf, e)
		},
	})
}

// CommittedLog is one committed log a sequence was learned for; id is zero for
// a ledger-level log, which the oracle keeps no row for.
type CommittedLog struct {
	Ledger string
	ID     uint64
	Kind   string // the oracle's log kind; empty for a ledger-level log
}

// AuditSuccessMismatch explains why a successful entry within the committed
// frontier is not one bulk of the model, or "" when it is: its sequence range
// is contiguous, one order per log, every sequence is a committed log, and the
// entry's ledgers are exactly those logs' ledgers.
func AuditSuccessMismatch(e AuditEntry, logs map[uint64]CommittedLog, bulks map[uint64]CommittedBulk, committedMax uint64) (why string, missingSeq uint64) {
	if e.MinLog == 0 || e.MaxLog < e.MinLog {
		return "empty or inverted log range", 0
	}

	if e.MaxLog > committedMax {
		return "log range straddles the committed frontier", 0
	}

	span := e.MaxLog - e.MinLog + 1
	if uint64(e.OrderCount) != span {
		return "order count " + strconv.FormatUint(uint64(e.OrderCount), 10) + " for " + strconv.FormatUint(span, 10) + " logs", 0
	}

	// Items are purged from archived entries; a present list is one per order.
	if len(e.ItemSeqs) != 0 && uint64(len(e.ItemSeqs)) != span {
		return strconv.Itoa(len(e.ItemSeqs)) + " items for " + strconv.FormatUint(span, 10) + " logs", 0
	}

	seen := map[string]bool{}
	for seq := e.MinLog; seq <= e.MaxLog; seq++ {
		l, ok := logs[seq]
		if !ok {
			return "sequence " + strconv.FormatUint(seq, 10) + " is no committed log", seq
		}

		seen[l.Ledger] = true
	}

	items := map[uint64]bool{}
	for _, seq := range e.ItemSeqs {
		if seq < e.MinLog || seq > e.MaxLog {
			return "item sequence " + strconv.FormatUint(seq, 10) + " outside the entry's range", 0
		}

		// One item per order, one log per order: with the count already equal
		// to the span, a repeat means some log of the range has no item.
		if items[seq] {
			return "item sequence " + strconv.FormatUint(seq, 10) + " repeated", 0
		}
		items[seq] = true
	}

	ledgers := slices.Sorted(maps.Keys(seen))
	if !slices.Equal(ledgers, e.Ledgers) {
		return "logs belong to " + strings.Join(ledgers, ",") + ", entry names " + strings.Join(e.Ledgers, ","), 0
	}

	// One bulk is one entry, so the range must be some bulk's own boundaries: a
	// contiguous run of committed logs is not enough, or an entry merging two
	// adjacent bulks — or naming part of one — passes every check above.
	bulk, recorded := bulks[e.MinLog]
	if !recorded {
		return "no committed bulk begins at " + strconv.FormatUint(e.MinLog, 10), e.MinLog
	}

	if bulk.MaxSeq != e.MaxLog {
		return "the bulk at " + strconv.FormatUint(e.MinLog, 10) + " ends at " + strconv.FormatUint(bulk.MaxSeq, 10) + ", entry names " + strconv.FormatUint(e.MaxLog, 10), 0
	}

	return "", 0
}

// AuditPageViolation reports the first way the page breaks its own contract —
// length, order, or filter scope — or "" when it holds. These are extras over
// the model check, never the verdict on their own.
func AuditPageViolation(entries []AuditEntry, pageSize int, reverse bool, filter *commonpb.QueryFilter, afterSeq uint64, next string) string {
	if len(entries) > pageSize {
		return "page longer than requested"
	}

	// The trail's universe is wider than the model's — system-scoped and
	// setup-era entries are not tracked — so no model verdict on whether another
	// entry is waiting is available here. What still holds is structural: the
	// token names the last entry served, and only a full page can carry one.
	if !NextCursorLegal(next, CursorEither, LastAuditKey(entries), len(entries), pageSize) {
		return "resume token does not match the page it rode with"
	}

	// Without an indexed leaf the page is a zone scan, and the zone is dense:
	// every proposal, accepted or rejected, takes the next sequence.
	dense := AuditFilterIndexFree(filter)

	// Resume is exclusive and runs in the iteration's own direction.
	for _, e := range entries {
		if afterSeq != 0 && (!reverse && e.Seq <= afterSeq || reverse && e.Seq >= afterSeq) {
			return "entry outside the cursor"
		}
	}

	for i, e := range entries {
		if i > 0 {
			prev := entries[i-1].Seq
			if !reverse && e.Seq <= prev {
				return "ascending order violated"
			}

			if reverse && e.Seq >= prev {
				return "descending order violated"
			}

			if dense && !reverse && e.Seq != prev+1 {
				return "gap in a zone-scan page"
			}

			if dense && reverse && e.Seq != prev-1 {
				return "gap in a zone-scan page"
			}
		}

		if i == 0 && dense && !reverse && e.Seq != max(AuditZoneStart(filter), afterSeq+1) {
			return "zone-scan page does not start at its lower bound"
		}

		if !AuditEntrySatisfies(filter, e) {
			return "entry outside the filter"
		}
	}

	return ""
}

// AuditEntrySatisfies reports whether a served entry meets the filter it was
// selected by, on the entry's own fields. An order-type leaf holds here: the
// list entry does not carry its orders.
func AuditEntrySatisfies(filter *commonpb.QueryFilter, e AuditEntry) bool {
	return AuditEntryMatches(filter, e, nil)
}

// AuditEntryMatches evaluates filter on e in one pass, with an order-type leaf
// read from orderTypes — the kinds the model committed under e's logs. A nil
// orderTypes leaves that leaf holding, for when the orders are not known.
func AuditEntryMatches(filter *commonpb.QueryFilter, e AuditEntry, orderTypes map[string]bool) bool {
	return FoldFilter(filter, FilterFold[bool]{
		And: allOf,
		Or:  anyOf,
		Not: negate,
		Leaf: func(leaf *commonpb.QueryFilter) bool {
			a := leaf.GetAudit()
			if a == nil {
				return leaf.GetFilter() == nil
			}

			value := a.GetStringCond().GetHardcoded()
			switch a.GetField() {
			case commonpb.AuditField_AUDIT_FIELD_SEQUENCE:
				return MatchUintBounds(a.GetUintCond(), e.Seq)
			case commonpb.AuditField_AUDIT_FIELD_PROPOSAL_ID:
				return MatchUintBounds(a.GetUintCond(), e.ProposalID)
			case commonpb.AuditField_AUDIT_FIELD_TIMESTAMP:
				return MatchUintBounds(a.GetUintCond(), e.Timestamp)
			case commonpb.AuditField_AUDIT_FIELD_LOG_SEQUENCE:
				// Match-any over the entry's items, whose sequences are the
				// contiguous range a success entry names; a rejection has none.
				return !e.Failed && BoundsMeetRange(a.GetUintCond(), e.MinLog, e.MaxLog)
			case commonpb.AuditField_AUDIT_FIELD_OUTCOME:
				return (value == "failure") == e.Failed
			case commonpb.AuditField_AUDIT_FIELD_LEDGER:
				return slices.Contains(e.Ledgers, value)
			case commonpb.AuditField_AUDIT_FIELD_CALLER_SUBJECT:
				// An empty subject is never indexed.
				return e.Subject != "" && e.Subject == value
			case commonpb.AuditField_AUDIT_FIELD_IDEMPOTENCY_KEY:
				// The only audit field with a prefix form. An entry carrying no
				// key is absent from the index, so neither form selects it.
				if e.IdemKey == "" {
					return false
				}

				if p, isPrefix := a.GetCondition().(*commonpb.AuditCondition_StringPrefix); isPrefix {
					return strings.HasPrefix(e.IdemKey, p.StringPrefix)
				}

				return e.IdemKey == value
			case commonpb.AuditField_AUDIT_FIELD_ORDER_TYPE:
				return orderTypes == nil || orderTypes[value]
			default:
				return true
			}
		},
	})
}

// CommittedOrderTypes maps the logs a success entry names to the audit
// indexer's order-type tokens. A ledger-level log has no oracle row, so both
// ledger-metadata tokens are admitted for it.
func CommittedOrderTypes(e AuditEntry, logs map[uint64]CommittedLog) map[string]bool {
	types := map[string]bool{}
	for seq := e.MinLog; seq <= e.MaxLog && seq >= e.MinLog; seq++ {
		l, ok := logs[seq]
		if !ok {
			continue
		}

		if token, known := AuditOrderTypeOfKind[l.Kind]; known {
			types[token] = true
		}
	}

	return types
}

// AuditOrderTypeOfKind maps the oracle's log kinds to domain.AuditOrderType's
// tokens for the orders this workload sends.
var AuditOrderTypeOfKind = map[string]string{
	"created_transaction":              "create_transaction",
	"reverted_transaction":             "revert_transaction",
	"saved_metadata":                   "add_metadata",
	"deleted_metadata":                 "delete_metadata",
	"set_metadata_field_type":          "set_metadata_field_type",
	"removed_metadata_field_type":      "remove_metadata_field_type",
	"create_index":                     "create_index",
	"drop_index":                       "drop_index",
	"added_account_type":               "add_account_type",
	"removed_account_type":             "remove_account_type",
	"updated_default_enforcement_mode": "update_default_enforcement_mode",
	"saved_ledger_metadata":            "save_ledger_metadata",
	"deleted_ledger_metadata":          "delete_ledger_metadata",
}

// AuditFiltersOrderType reports whether filter carries an order-type leaf.
func AuditFiltersOrderType(filter *commonpb.QueryFilter) bool {
	return AnyLeaf(filter, func(leaf *commonpb.QueryFilter) bool {
		return leaf.GetAudit().GetField() == commonpb.AuditField_AUDIT_FIELD_ORDER_TYPE
	})
}

// AuditFilterIndexFree mirrors the compiler's dispatch: nil, a sequence bound,
// or an And of those scans the zone; anything else goes through the index.
func AuditFilterIndexFree(filter *commonpb.QueryFilter) bool {
	return FoldFilter(filter, FilterFold[bool]{
		And: allOf,
		Or:  func([]bool) bool { return false },
		Not: func(bool) bool { return false },
		Leaf: func(leaf *commonpb.QueryFilter) bool {
			return leaf.GetFilter() == nil || leaf.GetAudit().GetField() == commonpb.AuditField_AUDIT_FIELD_SEQUENCE
		},
	})
}

// LastAuditKey is the cursor an audit page implies: the sequence of its last
// entry in decimal, empty for a page that showed none.
func LastAuditKey(entries []AuditEntry) string {
	if len(entries) == 0 {
		return ""
	}

	return strconv.FormatUint(entries[len(entries)-1].Seq, 10)
}

// AuditZoneStart is the first sequence a forward zone scan serves: the zone
// starts at 1 and every sequence bound of the filter raises it.
func AuditZoneStart(filter *commonpb.QueryFilter) uint64 {
	start := uint64(1)
	FoldFilter(filter, FilterFold[struct{}]{
		And: func([]struct{}) struct{} { return struct{}{} },
		Or:  func([]struct{}) struct{} { return struct{}{} },
		Not: identity[struct{}],
		Leaf: func(leaf *commonpb.QueryFilter) struct{} {
			if a := leaf.GetAudit(); a.GetField() == commonpb.AuditField_AUDIT_FIELD_SEQUENCE && a.GetUintCond().Min != nil {
				lo := a.GetUintCond().GetMin()
				if a.GetUintCond().GetMinExclusive() {
					lo++
				}

				start = max(start, lo)
			}

			return struct{}{}
		},
	})

	return start
}

// BareAuditLogSeqRange returns the condition when filter is exactly one
// log-sequence leaf.
func BareAuditLogSeqRange(filter *commonpb.QueryFilter) *commonpb.UintCondition {
	if a := filter.GetAudit(); a != nil && a.GetField() == commonpb.AuditField_AUDIT_FIELD_LOG_SEQUENCE {
		return a.GetUintCond()
	}

	return nil
}

// BoundsMeetRange reports whether some value of [lo, hi] satisfies cond.
func BoundsMeetRange(cond *commonpb.UintCondition, lo, hi uint64) bool {
	if cond.Min != nil {
		m := cond.GetMin()
		if cond.GetMinExclusive() {
			if m == math.MaxUint64 {
				return false
			}

			m++
		}

		lo = max(lo, m)
	}

	if cond.Max != nil {
		m := cond.GetMax()
		if cond.GetMaxExclusive() {
			if m == 0 {
				return false
			}

			m--
		}

		hi = min(hi, m)
	}

	return lo <= hi
}

func FilterAuditUint(field commonpb.AuditField, cond *commonpb.UintCondition) *commonpb.QueryFilter {
	return &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Audit{Audit: &commonpb.AuditCondition{
		Field:     field,
		Condition: &commonpb.AuditCondition_UintCond{UintCond: cond},
	}}}
}

func DescribeAuditEntry(e AuditEntry) string {
	outcome := "ok[" + strconv.FormatUint(e.MinLog, 10) + ".." + strconv.FormatUint(e.MaxLog, 10) + "]"
	if e.Failed {
		outcome = "failed:" + e.Reason
	}

	return "seq" + strconv.FormatUint(e.Seq, 10) + " " + outcome + " orders=" + strconv.FormatUint(uint64(e.OrderCount), 10) + " ledgers=" + strings.Join(e.Ledgers, ",")
}

func DescribeAuditSeqs(entries []AuditEntry) string {
	seqs := make([]string, 0, len(entries))
	for _, e := range entries {
		seqs = append(seqs, strconv.FormatUint(e.Seq, 10))
	}

	return strings.Join(seqs, ",")
}

// FilterAuditString builds an audit leaf on a string-typed audit field.
func FilterAuditString(field commonpb.AuditField, value string) *commonpb.QueryFilter {
	return &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Audit{Audit: &commonpb.AuditCondition{
		Field: field,
		Condition: &commonpb.AuditCondition_StringCond{StringCond: &commonpb.StringCondition{
			Value: &commonpb.StringCondition_Hardcoded{Hardcoded: value},
		}},
	}}}
}

// auditOrderTypes are the tokens a filter may ask for: every kind this
// workload commits plus the two ledger-metadata orders.
var AuditOrderTypes = []string{
	"create_transaction", "revert_transaction", "add_metadata", "delete_metadata",
	"set_metadata_field_type", "remove_metadata_field_type", "create_index", "drop_index",
	"add_account_type", "remove_account_type", "update_default_enforcement_mode",
	"save_ledger_metadata", "delete_ledger_metadata",
}
