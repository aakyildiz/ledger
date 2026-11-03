package quietcheck

import (
	"encoding/json"
	"fmt"
	"log"
	"slices"
	"strconv"
	"strings"

	"google.golang.org/protobuf/encoding/protojson"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/auditpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	oc "github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck"
	"github.com/formancehq/ledger/v3/tests/oracle"
)

// AuditBody is the body of an "audit" section chunk.
type AuditBody struct {
	// Entries are AuditEntry protojson, ascending by sequence, from cursor+1.
	Entries []json.RawMessage `json:"entries"`
	// Complete marks the last chunk: the listing reached the end of the trail.
	Complete bool `json:"complete"`
	// Head is the newest audit sequence the driver observed (decimal; first
	// chunk; used on resync).
	Head string `json:"head,omitempty"`
}

// AuditNext is the "next" answer of an "audit" chunk.
type AuditNext struct {
	Cursor string `json:"cursor"`
	Resync bool   `json:"resync"`
	// Skipped is the audit interval a resync skipped, (from, to] (only on
	// the chunk that resynchronised).
	Skipped *SkippedRange `json:"skipped,omitempty"`
}

// SkippedRange is an audit interval (From, To] the replay did not judge.
type SkippedRange struct {
	From string `json:"from"`
	To   string `json:"to"`
}

const reasonIdempotencyConflict = "IDEMPOTENCY_KEY_CONFLICT"

// entryReason is a failure entry's reason in the domain's vocabulary.
func entryReason(e *auditpb.AuditEntry) string {
	return strings.TrimPrefix(e.GetFailure().GetReason().String(), "ERROR_REASON_")
}

// auditSection replays one chunk of the server's audit trail (task 11, "Audit
// replay (incremental)").
func auditSection(t *oc.Tracker, epoch, _ uint64, raw json.RawMessage) (any, []oc.Event, *oc.Refusal) {
	var body AuditBody
	if err := json.Unmarshal(raw, &body); err != nil {
		return nil, nil, oc.Protocol("cannot decode the audit section body", map[string]any{"error": err.Error()})
	}
	entries := make([]*auditpb.AuditEntry, 0, len(body.Entries))
	for i, r := range body.Entries {
		e := &auditpb.AuditEntry{}
		if err := (protojson.UnmarshalOptions{DiscardUnknown: true}).Unmarshal(r, e); err != nil {
			return nil, nil, oc.Protocol("cannot decode an audit entry", map[string]any{"index": i, "error": err.Error()})
		}
		if i > 0 && e.GetSequence() <= entries[i-1].GetSequence() {
			return nil, nil, oc.Protocol("audit entries must ascend by sequence", map[string]any{"index": i, "sequence": e.GetSequence()})
		}
		entries = append(entries, e)
	}

	st := load(t, epoch)
	next := func() AuditNext {
		return AuditNext{Cursor: strconv.FormatUint(st.cursor, 10), Resync: st.resynced}
	}
	if st.auditDone {
		return next(), nil, nil // a chunk after the epoch's replay ended (complete or resynced)
	}

	if st.auditChunks == 0 && (t.ResyncNeeded() || st.forceResync) {
		head, err := strconv.ParseUint(body.Head, 10, 64)
		if err != nil {
			return nil, nil, oc.Protocol("a resync needs the audit head in the first chunk", map[string]any{"head": body.Head})
		}
		from := st.cursor
		st.resync(t, head)
		log.Printf("oracle: quiet audit replay resynchronised in epoch %d: audit sequences (%d, %d] skipped (held responses were dropped)", epoch, from, head)
		n := next()
		n.Skipped = &SkippedRange{From: strconv.FormatUint(from, 10), To: strconv.FormatUint(head, 10)}

		return n, nil, nil
	}
	st.auditChunks++

	ranges := st.rangesByWid(t)

	var evs []oc.Event
	gapReported := false
	for _, e := range entries {
		if e.GetSequence() <= st.cursor {
			continue // overlap with what an earlier chunk consumed
		}
		if e.GetSequence() != st.cursor+1 && !gapReported {
			// The FSM numbers entries without gaps (NextAuditSequenceID), so
			// a gap is a listing (pagination or filter) defect; the skipped
			// entries can never be replayed.
			gapReported = true
			evs = append(evs, oc.Violation(EvAuditSequenceGap, map[string]any{
				"cursor": st.cursor, "sequence": e.GetSequence(), "epoch": epoch,
			}))
		}
		evs = append(evs, st.replayEntry(t, e, ranges)...)
		st.cursor = e.GetSequence()
	}
	t.SetAuditCursor(strconv.FormatUint(st.cursor, 10))

	if body.Complete {
		evs = append(evs, st.finish(t)...)
		st.auditDone = true
	}
	for _, e := range evs {
		if e.IsFinding() {
			st.auditFindings++
		}
	}
	if body.Complete && st.auditFindings == 0 {
		evs = append(evs, oc.Reached(EvAuditReplayVerified, map[string]any{"cursor": st.cursor, "epoch": epoch}))
	}

	return next(), evs, nil
}

// resync re-seeds the replay from the drained model at the head the driver
// observed (DESIGN §5.5): the interval the dropped responses covered is
// skipped, which is not a finding.
func (st *state) resync(t *oc.Tracker, head uint64) {
	st.replay = t.Model()
	st.cursor = head
	t.SetAuditCursor(strconv.FormatUint(head, 10))
	t.ClearResync()
	st.forceResync = false
	st.obligations = nil
	st.scanned = t.ReadFrontier()
	clear(st.decided)
	clear(st.conflictSeen)
	if held := t.HeldResponses(); len(held) > 0 {
		t.ReleaseCommittedResponses(held[len(held)-1])
	}
	st.auditDone = true
	st.resynced = true
}

func (st *state) isTrackedKey(t *oc.Tracker, key string) bool {
	return strings.HasPrefix(key, t.Config().TrackedPrefix+"k")
}

// replayEntry consumes one audit entry.
func (st *state) replayEntry(t *oc.Tracker, e *auditpb.AuditEntry, ranges map[string]oc.CommittedBulk) []oc.Event {
	key := e.GetIdempotency().GetKey()
	prefix := t.Config().TrackedPrefix
	if !st.isTrackedKey(t, key) {
		var tracked []string
		for _, l := range e.GetLedgers() {
			if strings.HasPrefix(l, prefix) {
				tracked = append(tracked, l)
			}
		}
		if len(tracked) > 0 {
			return []oc.Event{oc.Violation(EvAuditForeignOnTracked, map[string]any{
				"sequence": e.GetSequence(), "key": key, "ledgers": e.GetLedgers(), "tracked": tracked,
			})}
		}

		return nil
	}

	recs := t.WritesByKey(key)
	failed := e.GetFailure() != nil
	reason := entryReason(e)

	// A failure of a kind the model does not predict (an FSM maintenance
	// gate, a stale proposal…) is audited per attempt; it decides nothing.
	if failed && len(recs) > 0 && e.GetFailure().GetReason() != commonpb.ErrorReason_ERROR_REASON_UNSPECIFIED &&
		!oc.IsDefinitiveReason(reason) {
		return nil
	}

	var tried []map[string]any
	for _, b := range recs {
		if st.isDecided(b) {
			continue
		}
		res := st.replay.Apply(b.Bulk)
		tried = append(tried, map[string]any{"wid": b.Wid, "role": b.Role, "ok": res.OK, "reason": res.Reason, "stage": int(res.Stage), "replayed": res.Replayed})
		if !failed {
			if !res.OK || res.Replayed || e.GetOrderCount() != uint32(len(b.Requests)) || !ledgersEqual(e.GetLedgers(), b.Bulk) {
				continue
			}
			r, learned := ranges[b.Wid]
			if !learned {
				continue // the online path never learned a commit for b
			}
			s := e.GetSuccess()
			if s.GetMinLogSequence() != r.MinSeq || s.GetMaxLogSequence() != r.MaxSeq {
				continue
			}
			st.replay = res.State
			st.decided[b.Wid] = true

			return nil
		}
		if res.OK || res.Replayed || res.Stage != oracle.StageFSM || oracle.UncommittedReasons[res.Reason] ||
			domain.ReasonCode(res.Reason) != e.GetFailure().GetReason() {
			continue
		}
		st.replay = st.replay.RecordFailure(b.Bulk, res)
		if b.Role == oc.RoleConflict && res.Reason == reasonIdempotencyConflict {
			// Conflicts are audited per attempt that reached the FSM and never
			// frozen (machine.go): any number of them.
			st.conflictSeen[key]++
		} else {
			st.decided[b.Wid] = true
		}

		return nil
	}

	d := map[string]any{
		"sequence": e.GetSequence(), "key": key, "failed": failed, "reason": reason, "orderCount": e.GetOrderCount(),
		"ledgers": e.GetLedgers(), "records": len(recs), "candidates": tried,
	}
	if s := e.GetSuccess(); s != nil {
		d["minLogSequence"], d["maxLogSequence"] = s.GetMinLogSequence(), s.GetMaxLogSequence()
	}
	if failed {
		for _, b := range recs {
			if st.decided[b.Wid] || b.Status != oc.WFailed || b.Outcome == nil || b.Outcome.Reason != reason {
				continue
			}
			d["wid"] = b.Wid
			if b.Result != nil && b.Result.Stage == oracle.StageAdmission {
				return []oc.Event{oc.Violation(EvAuditAdmissionEntry, d)}
			}
			if oracle.UncommittedReasons[reason] {
				return []oc.Event{oc.Violation(EvAuditUncommittedEntry, d)}
			}
		}
	}

	return []oc.Event{oc.Violation(EvAuditUnexplainedEntry, d)}
}

// ledgersEqual compares an entry's ledgers with the distinct ledgers the bulk's
// orders target.
func ledgersEqual(served []string, b oracle.Bulk) bool {
	want := map[string]bool{}
	for _, r := range b.Requests {
		if l := oracle.LedgerOf(r); l != "" {
			want[l] = true
		}
	}
	got := map[string]bool{}
	for _, l := range served {
		got[l] = true
	}
	if len(got) != len(served) || len(got) != len(want) {
		return false
	}
	for l := range want {
		if !got[l] {
			return false
		}
	}

	return true
}

// classify says which entry a tracked record is owed, if any.
func classify(rec oc.WriteRecord, ranges map[string]oc.CommittedBulk) (obligationKind, bool) {
	switch rec.Status {
	case oc.WCommitted:
		if _, ok := ranges[rec.Wid]; ok {
			return oweSuccess, true
		}
		// A success without sequenced logs is already a finding
		// (validateEmptyCommit) and never reached the model.
	case oc.WFailed:
		res := rec.Result
		if res == nil || res.Replayed || res.Stage != oracle.StageFSM || oracle.UncommittedReasons[res.Reason] {
			return 0, false // unexplained (already a finding), replayed, admission or uncommitted: no entry
		}
		if rec.Role == oc.RoleConflict && res.Reason == reasonIdempotencyConflict {
			return oweConflict, true
		}

		return oweFailure, true
	}

	return 0, false
}

// finish runs at the end of the trail: completeness, fingerprint, release.
func (st *state) finish(t *oc.Tracker) []oc.Event {
	ranges := st.rangesByWid(t)
	epochCount := t.QuietBegins()
	for tk := st.scanned + 1; tk <= t.ReadFrontier(); tk++ {
		rec, ok := t.RecordByTicket(tk)
		if !ok {
			st.scanned = tk // a read ticket
			continue
		}
		if rec.Status == oc.WInflight || rec.Status == oc.WPending || rec.EndedBeforeEpoch >= epochCount {
			break // not settled before this quiet period: next time
		}
		if kind, owed := classify(rec, ranges); owed {
			st.obligations = append(st.obligations, obligation{wid: rec.Wid, kind: kind})
		}
		st.scanned = tk
	}

	var evs []oc.Event
	for _, o := range st.obligations {
		rec, _ := t.Record(o.wid)
		d := map[string]any{"wid": o.wid, "key": rec.Key, "role": rec.Role, "ticket": rec.Ticket, "cursor": st.cursor}
		switch o.kind {
		case oweSuccess:
			if !st.decided[o.wid] {
				r := ranges[o.wid]
				d["minLogSequence"], d["maxLogSequence"] = r.MinSeq, r.MaxSeq
				evs = append(evs, oc.Violation(EvAuditMissingSuccess, d))
			}
		case oweFailure:
			if !st.decided[o.wid] {
				d["reason"] = rec.Result.Reason
				evs = append(evs, oc.Violation(EvAuditMissingFailure, d))
			}
		case oweConflict:
			// Entries of conflicts under one key cannot be told apart: each
			// owed conflict consumes one of the key's entries.
			switch {
			case st.decided[o.wid]:
			case st.conflictSeen[rec.Key] > 0:
				st.conflictSeen[rec.Key]--
			default:
				d["reason"] = rec.Result.Reason
				evs = append(evs, oc.Violation(EvAuditMissingFailure, d))
			}
		}
	}
	st.obligations = nil // every obligation was settled or reported once

	model := t.Model()
	if st.replay.BusinessFingerprint() != model.BusinessFingerprint() {
		evs = append(evs, oc.Violation(EvAuditReplayStateDiffers, map[string]any{
			"cursor": st.cursor, "diff": stateDiff(st.replay, model),
		}))
		// Reported once: continue from the online model so the next quiet
		// period checks the next interval instead of repeating this finding.
		st.replay = model
	}

	// Release every held response whose record is settled (matched to an
	// entry, or reported missing above): a reported record must not pin the
	// responses after it until the tracker's cap forces a resync.
	held := t.HeldResponses()
	n := 0
	for n < len(held) {
		rec, ok := t.Record(held[n])
		if !ok || !st.isDecided(rec) {
			break
		}
		n++
	}
	if n > 0 {
		t.ReleaseCommittedResponses(held[n-1])
	}

	// Prune what settled records no longer need: their matches (isDecided
	// covers them through scanned) and the surplus conflict entries of keys
	// whose conflict records all settled — a surplus must not cover a later
	// conflict whose entry is missing.
	for wid := range st.decided {
		if rec, ok := t.Record(wid); !ok || rec.Ticket <= st.scanned {
			delete(st.decided, wid)
		}
	}
	for key := range st.conflictSeen {
		settled := true
		for _, rec := range t.WritesByKey(key) {
			if rec.Role == oc.RoleConflict && rec.Ticket > st.scanned {
				settled = false

				break
			}
		}
		if settled {
			delete(st.conflictSeen, key)
		}
	}

	return evs
}

// stateDiff describes, per ledger, where two states differ (business view).
func stateDiff(replay, model oracle.GlobalState) map[string]any {
	out := map[string]any{}
	rl, ml := replay.Ledgers(), model.Ledgers()
	names := map[string]bool{}
	for n := range rl {
		names[n] = true
	}
	for n := range ml {
		names[n] = true
	}
	sorted := make([]string, 0, len(names))
	for n := range names {
		sorted = append(sorted, n)
	}
	slices.Sort(sorted)
	for _, n := range sorted {
		r, m := rl[n], ml[n]
		var diffs []string
		cmp := func(what string, a, b any) {
			if fmt.Sprint(a) != fmt.Sprint(b) {
				diffs = append(diffs, fmt.Sprintf("%s: replay %v, model %v", what, a, b))
			}
		}
		cmp("txs", r.Txs().Len(), m.Txs().Len())
		cmp("logs", len(r.LogRows()), len(m.LogRows()))
		cmp("volumes", r.Volumes().Fingerprint(), m.Volumes().Fingerprint())
		cmp("accountMetadata", r.Metadata().Fingerprint(), m.Metadata().Fingerprint())
		cmp("ledgerMetadata", r.LedgerMeta().Fingerprint(), m.LedgerMeta().Fingerprint())
		cmp("chart", r.Types().Fingerprint(), m.Types().Fingerprint())
		cmp("defaultEnforcement", r.DefaultEnforcementMode(), m.DefaultEnforcementMode())
		cmp("accountFieldTypes", r.AccountFieldTypes().Fingerprint(), m.AccountFieldTypes().Fingerprint())
		cmp("transactionFieldTypes", r.TransactionFieldTypes().Fingerprint(), m.TransactionFieldTypes().Fingerprint())
		cmp("ledgerFieldTypes", r.LedgerFieldTypes().Fingerprint(), m.LedgerFieldTypes().Fingerprint())
		cmp("indexes", slices.Sorted(keys(r.Indexes())), slices.Sorted(keys(m.Indexes())))
		cmp("preparedQueries", r.PreparedQueryNames(), m.PreparedQueryNames())
		rlc, rok := replay.Lifecycle(n)
		mlc, mok := model.Lifecycle(n)
		cmp("lifecycle", fmt.Sprint(rok, rlc.Deleted, rlc.Mode), fmt.Sprint(mok, mlc.Deleted, mlc.Mode))
		if len(diffs) > 0 {
			out[n] = diffs
		}
	}
	var global []string
	if a, b := replay.QueryCheckpointIDs(), model.QueryCheckpointIDs(); !slices.Equal(a, b) {
		global = append(global, fmt.Sprintf("checkpoints: replay %v, model %v", a, b))
	}
	if a, b := replay.NextQueryCheckpointID(), model.NextQueryCheckpointID(); a != b {
		global = append(global, fmt.Sprintf("nextCheckpointId: replay %d, model %d", a, b))
	}
	if a, b := replay.MaintenanceMode(), model.MaintenanceMode(); a != b {
		global = append(global, fmt.Sprintf("maintenance: replay %v, model %v", a, b))
	}
	if len(out) == 0 && len(global) == 0 {
		// Nothing above differs: the difference is in what has no accessor —
		// the frozen idempotency keys, or a field the projection above omits.
		global = append(global, "frozen idempotency keys (or an unlisted business field) differ")
	}
	if len(global) > 0 {
		out["*global"] = global
	}

	return out
}

func keys[V any](m oracle.Map[string, V]) func(func(string) bool) {
	return func(yield func(string) bool) {
		for k := range m.All() {
			if !yield(k) {
				return
			}
		}
	}
}
