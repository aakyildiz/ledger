package match

import (
	"slices"
	"strconv"
	"strings"

	"github.com/formancehq/ledger/v3/internal/domain/indexes"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/tests/oracle"
)

// HasDateLeaf reports whether the filter reads the log date, which is served
// only when the log-date builtin index exists.
func HasDateLeaf(f *commonpb.QueryFilter) bool {
	return AnyLeaf(f, func(leaf *commonpb.QueryFilter) bool {
		_, ok := leaf.GetFilter().(*commonpb.QueryFilter_LogBuiltinUint)

		return ok
	})
}

// LogDateIndexID is the opt-in builtin index the log date is served from.
// LogDateIndexCanonical is its stable map key in the model's index set.
func LogDateIndexID() *commonpb.IndexID {
	return indexes.LogBuiltinID(commonpb.LogBuiltinIndex_LOG_BUILTIN_INDEX_DATE)
}

var LogDateIndexCanonical = indexes.Canonical(LogDateIndexID())

// NeededLogIndexes returns the canonical IDs of the indexes a LOGS filter
// needs the compiler to find ready. The model owns whether each one exists:
// the outcome the server owes — a page or a not-ready refusal — follows from
// the index's lifecycle in every candidate base, exactly as on the accounts
// and transactions paths.
func NeededLogIndexes(f *commonpb.QueryFilter) map[string]struct{} {
	if !HasDateLeaf(f) {
		return nil
	}

	return map[string]struct{}{LogDateIndexCanonical: {}}
}

// MatchLogFilter evaluates a LOGS filter against one modelled log, three-valued
// (see Kleene). date is the server-assigned value learned at commit; it is nil
// for a log whose response this base has not folded yet — a candidate base that
// applied an observed-but-undrained bulk holds exactly such logs — so a date
// leaf reading it is undecided, and the row becomes optional in the predicted
// window rather than absent from it.
func MatchLogFilter(ledger string, id uint64, date *commonpb.Timestamp, f *commonpb.QueryFilter) (match, known bool) {
	v := FoldFilter(f, FilterFold[Kleene]{
		And: KleeneAnd,
		Or:  KleeneOr,
		Not: KleeneNot,
		Leaf: func(leaf *commonpb.QueryFilter) Kleene {
			switch t := leaf.GetFilter().(type) {
			case nil:
				return Kleene{Match: true, Known: true}
			case *commonpb.QueryFilter_Ledger:
				return Kleene{Match: ledger == t.Ledger.GetCond().GetHardcoded(), Known: true}
			case *commonpb.QueryFilter_LogId:
				return Kleene{Match: MatchUintBounds(t.LogId.GetCond(), id), Known: true}
			case *commonpb.QueryFilter_LogBuiltinUint:
				if date == nil {
					return Kleene{Known: false}
				}

				return Kleene{Match: MatchUintBounds(t.LogBuiltinUint.GetCond(), date.GetData()), Known: true}
			default:
				// Every LOGS-valid condition is handled above; anything else means
				// the generator and the matcher have drifted apart.
				panic("model: unmatched LOGS condition")
			}
		},
	})

	return v.Match, v.Known
}

// LogWindowRow is one row a ListLogs page may draw, with the fields the model
// pins on it. required=false marks a log whose date this base has not learned,
// so a date leaf cannot be decided for it: the page may or may not carry it.
type LogWindowRow struct {
	ID        uint64
	Kind      string
	Payload   string
	Tx        TxRecordView
	RevertsID uint64
	Date      *commonpb.Timestamp
	Sequence  uint64
	Purged    string
	NewKept   string
	Ephemeral string
	Required  bool
}

// ServerLogRow is one log of a page, reduced to what the model can pin: the
// ledger it belongs to, its per-ledger id, its payload kind, the date the
// server assigned it, its end-of-bulk volume annotations, and whether it
// arrived signed.
type ServerLogRow struct {
	Ledger    string
	ID        uint64
	Kind      string
	Payload   string
	Tx        *commonpb.Transaction
	RevertsID uint64
	Date      uint64
	HasDate   bool
	Sequence  uint64
	Purged    string
	NewKept   string
	Ephemeral string
	Signed    bool
}

// ServerLogRows reads a page into comparable rows.
func ServerLogRows(logs []*commonpb.Log) []ServerLogRow {
	out := make([]ServerLogRow, 0, len(logs))

	for _, l := range logs {
		entry := l.GetPayload().GetApply().GetLog()

		tx, revertsID := ServedLogTransaction(entry.GetData())

		out = append(out, ServerLogRow{
			Ledger:    l.GetPayload().GetApply().GetLedgerName(),
			ID:        entry.GetId(),
			Kind:      ServerLogKind(l),
			Payload:   oracle.CanonicalServedLogPayload(entry.GetData()),
			Tx:        tx,
			RevertsID: revertsID,
			Date:      entry.GetDate().GetData(),
			HasDate:   entry.GetDate() != nil,
			Sequence:  l.GetSequence(),
			Purged:    RenderServedVolumes(entry.GetPurgedVolumes()),
			NewKept:   RenderServedVolumes(entry.GetNewKeptVolumes()),
			Ephemeral: RenderServedVolumes(entry.GetEphemeralVolumes()),
			Signed:    l.GetResponseSignature() != nil,
		})
	}

	return out
}

// ServedLogTransaction pulls the transaction a log announces off its payload,
// with the id of the transaction a revert compensates. Nil for the kinds that
// announce none.
func ServedLogTransaction(data *commonpb.LedgerLogPayload) (*commonpb.Transaction, uint64) {
	switch {
	case data.GetCreatedTransaction() != nil:
		return data.GetCreatedTransaction().GetTransaction(), 0
	case data.GetRevertedTransaction() != nil:
		rt := data.GetRevertedTransaction()

		return rt.GetRevertTransaction(), rt.GetRevertedTransactionId()
	default:
		return nil, 0
	}
}

// RenderServedVolumes names a served annotation list the way the oracle names
// its own (renderTouchedVolumes): "account:asset:color" entries joined by
// commas, verbatim in the order the server sent them, so a mis-sorted or
// duplicated list is a mismatch. The colour segment is always present, empty
// for the uncolored bucket.
func RenderServedVolumes(vols []*commonpb.TouchedVolume) string {
	if len(vols) == 0 {
		return ""
	}

	parts := make([]string, 0, len(vols))
	for _, v := range vols {
		parts = append(parts, v.GetAccount()+":"+v.GetAsset()+":"+v.GetColor())
	}

	return strings.Join(parts, ",")
}

// ServerLogKind names a log's payload the way the model names it from the
// request that produced it (oracle logKindOf), so the two are comparable.
func ServerLogKind(l *commonpb.Log) string {
	switch d := l.GetPayload().GetApply().GetLog().GetData(); {
	case d.GetOrderSkipped() != nil:
		return "order_skipped"
	case d.GetUpdatedDefaultEnforcementMode() != nil:
		return "updated_default_enforcement_mode"
	case d.GetCreatedTransaction() != nil:
		return "created_transaction"
	case d.GetRevertedTransaction() != nil:
		return "reverted_transaction"
	case d.GetSavedMetadata() != nil:
		return "saved_metadata"
	case d.GetDeletedMetadata() != nil:
		return "deleted_metadata"
	case d.GetSetMetadataFieldType() != nil:
		return "set_metadata_field_type"
	case d.GetRemovedMetadataFieldType() != nil:
		return "removed_metadata_field_type"
	case d.GetCreateIndex() != nil:
		return "create_index"
	case d.GetDropIndex() != nil:
		return "drop_index"
	case d.GetAddedAccountType() != nil:
		return "added_account_type"
	case d.GetRemovedAccountType() != nil:
		return "removed_account_type"
	default:
		return "other"
	}
}

// LogWindowRows is the ordered, cursor-filtered, UNTRUNCATED row sequence a
// ListLogs page draws from: ascending by id with the cursor as an exclusive
// lower bound, or descending with it as an exclusive upper bound when reverse.
// Truncation is LogWindowMatches' job: optional rows may or may not consume
// page slots, so a fixed prefix cut would be wrong.
func LogWindowRows(ls oracle.LedgerState, ledger string, filter *commonpb.QueryFilter, afterSeq uint64, reverse bool) []LogWindowRow {
	var rows []LogWindowRow

	all := ls.LogRows()
	if reverse {
		all = slices.Clone(all)
		slices.Reverse(all)
	}

	for _, row := range all {
		if afterSeq > 0 && (!reverse && row.ID <= afterSeq || reverse && row.ID >= afterSeq) {
			continue
		}

		match, known := MatchLogFilter(ledger, row.ID, row.Date, filter)
		if known && !match {
			continue
		}

		rows = append(rows, LogWindowRowOf(ls, row, known))
	}

	return rows
}

// LogWindowRowOf is one committed log as a comparable row of its base.
func LogWindowRowOf(ls oracle.LedgerState, row oracle.LogRow, required bool) LogWindowRow {
	return LogWindowRow{
		ID: row.ID, Kind: row.Kind, Payload: row.Payload,
		Tx: ModelTxForLog(ls, row.TxID), RevertsID: RevertedIDForLog(ls, row),
		Date: row.Date, Sequence: row.Sequence,
		Purged: row.PurgedVolumes, NewKept: row.NewKeptVolumes, Ephemeral: row.EphemeralVolumes,
		Required: required,
	}
}

// ModelTxForLog resolves the transaction a log announces, nil when the log
// announces none or the id is past this base's frontier.
func ModelTxForLog(ls oracle.LedgerState, txID uint64) TxRecordView {
	if txID == 0 || txID > uint64(ls.Txs().Len()) {
		return nil
	}

	return ls.Txs().Get(int(txID - 1))
}

// RevertedIDForLog names the transaction a revert log compensates, which the
// model holds on the revert's own record.
func RevertedIDForLog(ls oracle.LedgerState, row oracle.LogRow) uint64 {
	tx := ModelTxForLog(ls, row.TxID)
	if tx == nil {
		return 0
	}

	return tx.RevertsTransaction()
}

// LogWindowMatches reports whether the page is exactly a legal window over the
// candidate's row sequence: required rows appear in order, optional rows may,
// nothing else does, and a required row may only be missing past a full
// (truncated) page.
func LogWindowMatches(ls oracle.LedgerState, ledger string, filter *commonpb.QueryFilter, afterSeq uint64, reverse bool, pageSize int, page []ServerLogRow, next string) bool {
	rows := LogWindowRows(ls, ledger, filter, afterSeq, reverse)
	if !LogRowsMatch(ledger, rows, pageSize, page) {
		return false
	}

	return NextCursorLegal(next, LogRowsLeftoverMore(rows, page), LastLogKey(page), len(page), pageSize)
}

// LogRowsLeftoverMore turns the rows a page left behind into the verdict on
// whether a resume cursor was owed: a required row certainly follows, one whose
// date the model has not learned may or may not.
func LogRowsLeftoverMore(rows []LogWindowRow, page []ServerLogRow) CursorMore {
	j, i := 0, 0
	for ; i < len(rows) && j < len(page); i++ {
		if page[j].ID == rows[i].ID {
			j++
		}
	}

	more := CursorForbidden
	for _, row := range rows[i:] {
		if row.Required {
			return CursorRequired
		}

		more = CursorEither
	}

	return more
}

func LogRowsMatch(ledger string, rows []LogWindowRow, pageSize int, page []ServerLogRow) bool {
	if len(page) > pageSize {
		return false
	}

	j := 0

	for _, row := range rows {
		if j == len(page) {
			if len(page) == pageSize {
				return true // full page — the remaining rows were truncated
			}

			if row.Required {
				return false // page had room, yet a required row is missing
			}

			continue
		}

		if page[j].ID == row.ID {
			if !LogRowMatches(ledger, row, page[j]) {
				return false
			}

			j++
		} else if row.Required {
			return false
		}
	}

	return j == len(page)
}

// LastLogKey is the cursor a logs page implies: the ledger-local id of its last
// row in decimal, empty for a page that showed none. The handler emits the empty
// token for a row carrying no apply payload, which this workload never produces
// — a token missing from a page of apply logs is a finding here.
func LastLogKey(page []ServerLogRow) string {
	if len(page) == 0 {
		return ""
	}

	return strconv.FormatUint(page[len(page)-1].ID, 10)
}

// LogRowMatches compares a served log against the model's record of it, field
// by field. The date is compared once the model has learned it from the commit
// response; before that the row is optional and its date says nothing. Every
// other field the model derives itself, so a mismatch is the server's.
func LogRowMatches(ledger string, row LogWindowRow, got ServerLogRow) bool {
	if got.Ledger != ledger || got.Kind != row.Kind {
		return false
	}

	if row.Payload != "" && got.Payload != row.Payload {
		return false
	}

	// A transaction log carries the whole transaction, and it is the log's own
	// copy: ListTransactions agreeing with the model says nothing about what
	// this payload holds.
	if row.Tx != nil {
		if got.Tx == nil || !LogTxMatches(row.Tx, got.Tx) || got.RevertsID != row.RevertsID {
			return false
		}
	} else if got.Tx != nil {
		return false
	}

	// The global sequence counts every ledger's logs and the technical entries
	// between them, so the model holds it only for a bulk whose response it
	// folded; before that it says nothing.
	if row.Sequence != 0 && got.Sequence != row.Sequence {
		return false
	}

	// The three volume annotations are derived, never learned: the model runs
	// the same end-of-bulk partition, so all three are pinned exactly — an
	// empty list included.
	if got.Purged != row.Purged || got.NewKept != row.NewKept || got.Ephemeral != row.Ephemeral {
		return false
	}

	if row.Date == nil {
		return true
	}

	return got.HasDate && got.Date == row.Date.GetData()
}

// LogTxMatches compares the transaction embedded in a log against the model's
// record of it, over the fields the payload freezes at creation.
//
// The record keeps moving after its log is written — a later revert sets
// reverted, reverted_by and reverted_at, and a metadata save rewrites the
// metadata map — while the log keeps the values it was written with. Only the
// fields that cannot move are compared here; the mutable ones are the
// transaction table's to answer for, through TxRecordMatches.
func LogTxMatches(rec TxRecordView, got *commonpb.Transaction) bool {
	if rec.Id() != got.GetId() || rec.Reference() != got.GetReference() ||
		rec.RevertsTransaction() != got.GetRevertsTransaction() {
		return false
	}

	if rec.Timestamp() != nil && rec.Timestamp().GetData() != got.GetTimestamp().GetData() {
		return false
	}

	if rec.InsertedAt() != nil && rec.InsertedAt().GetData() != got.GetInsertedAt().GetData() {
		return false
	}

	if got.GetUpdatedAt().GetData() != got.GetInsertedAt().GetData() ||
		(got.GetUpdatedAt() == nil) != (got.GetInsertedAt() == nil) {
		return false
	}

	return postingsEqual(rec.Postings(), got.GetPostings()) &&
		PCVSnapshotMatches(rec.PostCommitVolumes(), got.GetPostCommitVolumes())
}

// LogWindow is the page the model predicts when every row is decided: the
// required rows, truncated to pageSize. Optional rows are left out, so it is a
// diagnostic rendering only — legality is LogWindowMatches, which admits them.
func LogWindow(ls oracle.LedgerState, ledger string, filter *commonpb.QueryFilter, afterSeq uint64, reverse bool, pageSize int) []uint64 {
	var window []uint64

	for _, row := range LogWindowRows(ls, ledger, filter, afterSeq, reverse) {
		if !row.Required {
			continue
		}

		window = append(window, row.ID)
		if len(window) == pageSize {
			break
		}
	}

	return window
}

// ClassifyLogQueryError maps a ListLogs outcome to its class. ok=false means
// the error is not one the index gate produces, so the caller decides whether
// it is environmental. The not-ready rejection rides codes.Unavailable, which
// sits inside the transient class, so classifying must happen before any
// transient bail: an index gate refusing a filter the model can serve — or
// refusing one that needs no index — is a finding, not a blip.
func ClassifyLogQueryError(err RPCError) (IndexedErrKind, bool) {
	switch {
	case err.OK():
		return IndexedErrNone, true
	case IsIndexNotFound(err), IsIndexNotReady(err):
		return IndexedErrNotReady, true
	default:
		return IndexedErrNone, false
	}
}

// ServerLogIDs pulls the ledger-local ids out of a page.
func ServerLogIDs(logs []*commonpb.Log) []uint64 {
	out := make([]uint64, 0, len(logs))
	for _, l := range logs {
		out = append(out, l.GetPayload().GetApply().GetLog().GetId())
	}

	return out
}

// DescribeServerLogRows renders every pinned field of a served page, the shape
// a field mismatch is read from: id:kind@date/seq[payload]{volume annotations}.
func DescribeServerLogRows(page []ServerLogRow) string {
	parts := make([]string, 0, len(page))

	for _, row := range page {
		date := "?"
		if row.HasDate {
			date = strconv.FormatUint(row.Date, 10)
		}

		parts = append(parts, strconv.FormatUint(row.ID, 10)+":"+row.Kind+"@"+date+
			"/seq"+strconv.FormatUint(row.Sequence, 10)+"["+row.Payload+"]"+
			"{purged="+row.Purged+";newKept="+row.NewKept+";ephemeral="+row.Ephemeral+"}")
	}

	return strings.Join(parts, ",")
}

// LogOutcomeLegal is the per-candidate verdict for one ListLogs outcome: the
// shared index-lifecycle legality, with the base's ordered log window as the
// result check.
func LogOutcomeLegal(ls oracle.LedgerState, ledger string, filter *commonpb.QueryFilter, needed map[string]struct{}, errKind IndexedErrKind, page []ServerLogRow, afterSeq uint64, reverse bool, pageSize int, next string) bool {
	return IndexedQueryOutcomeLegal(ls, commonpb.QueryTarget_QUERY_TARGET_LOGS, filter, needed, errKind, "", func(view oracle.LedgerState) bool {
		return LogWindowMatches(view, ledger, filter, afterSeq, reverse, pageSize, page, next)
	})
}

// LogDateSample lists the dates ls's logs carry, the pool log-date bounds are
// drawn from.
func LogDateSample(ls oracle.LedgerState) []uint64 {
	rows := ls.LogDates()

	out := make([]uint64, 0, len(rows))

	for _, row := range rows {
		if row.Date != nil {
			out = append(out, row.Date.GetData())
		}
	}

	return out
}
