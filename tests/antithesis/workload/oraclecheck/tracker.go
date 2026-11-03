package oraclecheck

import (
	"fmt"
	"slices"
	"strings"

	"google.golang.org/protobuf/encoding/protojson"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/tests/oracle"
)

// WriteStatus is the life stage of a tracked write.
type WriteStatus uint8

const (
	WInflight WriteStatus = iota
	WCommitted
	WFailed
	WAbandoned
	// WPending is a definitive success observed but not drained yet.
	WPending
)

func (s WriteStatus) String() string {
	switch s {
	case WInflight:
		return "inflight"
	case WCommitted:
		return "committed"
	case WFailed:
		return "failed"
	case WAbandoned:
		return "abandoned"
	case WPending:
		return "pending"
	default:
		return "unknown"
	}
}

// Write roles.
const (
	RoleOriginal = "original"
	RoleConflict = "conflict"
)

// WriteRecord is everything the oracle knows about one tracked write.
type WriteRecord struct {
	Wid, Key, Role, Of, Family string
	Ticket                     uint64
	TrackKey                   bool
	Signed                     bool
	Requests                   []*servicepb.Request
	Bulk                       oracle.Bulk // Bulk.IdempotencyKey = Key iff TrackKey
	Status                     WriteStatus
	Outcome                    *Outcome            // set at EndWrite
	Result                     *oracle.ApplyResult // failures: the explaining candidate's result
	EndedBeforeEpoch           uint64              // number of quiet periods begun when EndWrite arrived
	// ObserveTicket is the ticket counter when EndWrite arrived.
	ObserveTicket uint64
	// EndStatus is what EndWrite answered ("pending" | "validated").
	EndStatus string
}

type pendingEntry struct {
	minSeq        uint64
	ticket        uint64
	observeTicket uint64
	wid           string
	resp          *servicepb.ApplyResponse
}

type readRec struct {
	kind, rid string
}

// Tracker owns tickets, the in-flight / pending / read sets, the drained model
// and the side tables. All methods require the caller to hold the service
// mutex.
type Tracker struct {
	cfg Config

	// ticketSeq hands out one monotonic ticket per write or read.
	ticketSeq uint64
	// inflight: tickets of begun writes without a definitive outcome yet.
	inflight map[uint64]string
	// pending: observed successes not yet drained, sorted by minSeq.
	pending []*pendingEntry
	// reads: tickets of outstanding reads.
	reads map[uint64]readRec

	// modelState is the drained state: the exact predecessor of pending[0].
	modelState oracle.GlobalState
	version    uint64

	records   map[string]*WriteRecord
	ticketWid map[uint64]string
	byKey     map[string][]string

	quietBegins uint64
	quietEpoch  uint64
	quietOpen   bool

	// ledgerNames is every tracked ledger name a drained bulk created.
	ledgerNames map[string]struct{}
	// forcedTxs marks committed transactions created with force.
	forcedTxs map[string]map[uint64]bool

	sideTables

	replayable []ReplayEntry

	checkpoints                map[uint64]checkpointSnapshot
	deletedCheckpoints         []uint64
	deletedCheckpointSnapshots map[uint64]checkpointSnapshot

	responses     map[string]*servicepb.ApplyResponse
	responseOrder []string
	resync        bool

	ext map[string]any

	auditCursor string
}

// NewTracker returns a Tracker on an empty model configured with the query
// checkpoint limit.
func NewTracker(cfg Config) *Tracker {
	cfg = cfg.withDefaults()
	t := &Tracker{
		cfg:                        cfg,
		inflight:                   map[uint64]string{},
		reads:                      map[uint64]readRec{},
		modelState:                 oracle.NewGlobalState().WithQueryCheckpointLimit(cfg.QueryCheckpointLimit),
		records:                    map[string]*WriteRecord{},
		ticketWid:                  map[uint64]string{},
		byKey:                      map[string][]string{},
		ledgerNames:                map[string]struct{}{},
		forcedTxs:                  map[string]map[uint64]bool{},
		sideTables:                 newSideTables(),
		checkpoints:                map[uint64]checkpointSnapshot{},
		deletedCheckpointSnapshots: map[uint64]checkpointSnapshot{},
		responses:                  map[string]*servicepb.ApplyResponse{},
		ext:                        map[string]any{},
	}

	return t
}

// Config returns the tracker's configuration.
func (t *Tracker) Config() Config { return t.cfg }

// ---- writes ----

// arm names an unmodeled request arm, or "" when O/ models it.
func unmodeledArm(r *servicepb.Request) string {
	switch ty := r.GetType().(type) {
	case nil:
		return "empty"
	case *servicepb.Request_Apply:
		a := ty.Apply.GetAction()
		if ct := a.GetCreateTransaction(); ct != nil {
			if ct.GetScript() != nil {
				return "apply.createTransaction.script"
			}
			if ct.GetScriptReference() != nil {
				return "apply.createTransaction.scriptReference"
			}
		}
		if a.GetData() == nil {
			return "apply.empty"
		}
	case *servicepb.Request_SaveNumscript:
		return "saveNumscript"
	case *servicepb.Request_RegisterSigningKey:
		return "registerSigningKey"
	case *servicepb.Request_RevokeSigningKey:
		return "revokeSigningKey"
	case *servicepb.Request_SetSigningConfig:
		return "setSigningConfig"
	case *servicepb.Request_AddEventsSink:
		return "addEventsSink"
	case *servicepb.Request_RemoveEventsSink:
		return "removeEventsSink"
	}
	if func() (bad bool) {
		defer func() {
			if recover() != nil {
				bad = true
			}
		}()
		oracle.LedgerOf(r)

		return false
	}() {
		return fmt.Sprintf("%T", r.GetType())
	}

	return ""
}

func unmodeled(msg string, d map[string]any) *Refusal { return refuse(400, CodeUnmodeled, msg, d) }

// checkPartition enforces DESIGN §3.2: tracked ledgers and key, request count,
// modeled arms.
func (t *Tracker) checkPartition(rec WriteRecord) *Refusal {
	prefix := t.cfg.TrackedPrefix
	if len(rec.Requests) > t.cfg.MaxTrackedRequests {
		return unmodeled(fmt.Sprintf("%d requests exceed the tracked bulk limit", len(rec.Requests)),
			map[string]any{"wid": rec.Wid, "requests": len(rec.Requests), "max": t.cfg.MaxTrackedRequests})
	}
	if !strings.HasPrefix(rec.Key, prefix+"k") {
		return unmodeled("the idempotency key is outside the tracked partition", map[string]any{"wid": rec.Wid, "key": rec.Key})
	}
	for i, r := range rec.Requests {
		if arm := unmodeledArm(r); arm != "" {
			return unmodeled(fmt.Sprintf("request %d: arm %s is not modeled", i, arm),
				map[string]any{"wid": rec.Wid, "index": i, "arm": arm})
		}
		ledger := oracle.LedgerOf(r)
		if ledger == "" {
			continue // the cluster-scoped orders LedgerOf knows are the allowed ones
		}
		if !strings.HasPrefix(ledger, prefix) {
			return unmodeled(fmt.Sprintf("request %d: ledger %s is outside the tracked partition", i, ledger),
				map[string]any{"wid": rec.Wid, "index": i, "ledger": ledger})
		}
	}

	return nil
}

// BeginWrite registers a write (DESIGN §4.6 service steps, in order).
func (t *Tracker) BeginWrite(rec WriteRecord) (ticket uint64, evs []Event, predicted map[string]any, ref *Refusal) {
	if t.quietOpen {
		return 0, nil, nil, Protocol("write during a quiet period", map[string]any{"epoch": t.quietEpoch})
	}
	if ref := t.checkPartition(rec); ref != nil {
		return 0, nil, nil, ref
	}
	if rec.Role == "" {
		rec.Role = RoleOriginal
	}
	switch rec.Role {
	case RoleOriginal:
	case RoleConflict:
		d := map[string]any{"wid": rec.Wid, "of": rec.Of}
		if !rec.TrackKey {
			return 0, nil, nil, Protocol("a conflict bulk requires trackKey", d)
		}
		orig, ok := t.records[rec.Of]
		if !ok {
			return 0, nil, nil, Protocol("the conflict's original is unknown", d)
		}
		if orig.Key != rec.Key {
			return 0, nil, nil, Protocol("a conflict must reuse the original's key", d)
		}
		if oracle.RequestsEqual(orig.Requests, rec.Requests) {
			return 0, nil, nil, Protocol("conflict body equals the original's", d)
		}
	default:
		return 0, nil, nil, Protocol("unknown role", map[string]any{"wid": rec.Wid, "role": rec.Role})
	}
	if _, dup := t.records[rec.Wid]; dup {
		return 0, nil, nil, Protocol("wid already registered with another body", map[string]any{"wid": rec.Wid})
	}
	if len(t.inflight) >= t.cfg.MaxInflight {
		return 0, nil, nil, refuse(409, CodeBusy, fmt.Sprintf("%d tickets in flight", len(t.inflight)),
			map[string]any{"inflight": len(t.inflight), "max": t.cfg.MaxInflight})
	}
	if len(t.pending) > t.cfg.PendingCap {
		return 0, nil, nil, refuse(409, CodeBacklog, "pending backlog above cap",
			map[string]any{"pending": len(t.pending), "cap": t.cfg.PendingCap})
	}

	rec.Bulk = oracle.Bulk{Requests: rec.Requests}
	if rec.TrackKey {
		rec.Bulk.IdempotencyKey = rec.Key
	}

	res := t.modelState.Apply(rec.Bulk)
	predicted = map[string]any{"ok": res.OK, "reason": res.Reason}
	if ref := t.runBeginHooks(rec, predicted); ref != nil {
		return 0, nil, nil, ref
	}

	t.ticketSeq++
	ticket = t.ticketSeq
	rec.Ticket = ticket
	rec.Status = WInflight
	rec.Outcome, rec.Result = nil, nil
	stored := rec
	t.records[rec.Wid] = &stored
	t.ticketWid[ticket] = rec.Wid
	t.byKey[rec.Key] = append(t.byKey[rec.Key], rec.Wid)
	t.inflight[ticket] = rec.Wid

	return ticket, nil, predicted, nil
}

func (t *Tracker) recordByTicket(ticket uint64) (*WriteRecord, bool) {
	wid, ok := t.ticketWid[ticket]
	if !ok {
		return nil, false
	}

	return t.records[wid], true
}

func nonDefinitive(ticket uint64, out Outcome) ([]Event, *Refusal) {
	d := map[string]any{"ticket": ticket, "code": out.Code, "reason": out.Reason}
	if out.Unknown != "" {
		d["unknown"] = out.Unknown
	}
	if out.Local {
		d["local"] = true
	}

	return []Event{Violation(EvProtocolNonDefinitive, d)}, Protocol("the outcome is not definitive", d)
}

// DecodeApplyResponse parses a successful write outcome's response.
func DecodeApplyResponse(raw []byte) (*servicepb.ApplyResponse, error) {
	resp := &servicepb.ApplyResponse{}
	if len(raw) == 0 {
		return resp, nil
	}
	if err := (protojson.UnmarshalOptions{DiscardUnknown: true}).Unmarshal(raw, resp); err != nil {
		return nil, err
	}

	return resp, nil
}

// EndWrite records the definitive outcome of a write. A success with logs
// joins pending ("pending"); every other outcome is validated now
// ("validated"). An already ended ticket answers its first status; a different
// definitive outcome for it is routed to the LateWrite comparison.
func (t *Tracker) EndWrite(ticket uint64, out Outcome) (status string, evs []Event, ref *Refusal) {
	rec, ok := t.recordByTicket(ticket)
	if !ok {
		return "", nil, UnknownTicket(ticket)
	}
	if !out.Definitive() {
		evs, ref := nonDefinitive(ticket, out)

		return "", evs, ref
	}
	switch rec.Status {
	case WAbandoned:
		return "", nil, Protocol("the ticket was abandoned", map[string]any{"ticket": ticket})
	case WInflight:
	default:
		if outcomesEquivalent(*rec.Outcome, out) {
			return rec.EndStatus, nil, nil
		}
		evs, ref := t.LateWrite(ticket, out)

		return rec.EndStatus, evs, ref
	}

	var resp *servicepb.ApplyResponse
	if out.OK {
		var err error
		if resp, err = DecodeApplyResponse(out.Response); err != nil {
			return "", nil, Protocol("cannot decode the success response", map[string]any{"ticket": ticket, "error": err.Error()})
		}
	}

	observe := t.ticketSeq
	delete(t.inflight, ticket)
	o := out
	rec.Outcome = &o
	rec.ObserveTicket = observe
	rec.EndedBeforeEpoch = t.quietBegins

	switch {
	case out.OK && minLogSequence(resp.GetLogs()) == 0:
		rec.Status = WCommitted
		rec.EndStatus = "validated"
		evs = append(evs, validateEmptyCommit(rec.Bulk)...)
		evs = append(evs, Reached(EvModelOutcomeVerified, map[string]any{}))
	case out.OK:
		rec.Status = WPending
		rec.EndStatus = "pending"
		t.insertPending(&pendingEntry{minSeq: minLogSequence(resp.GetLogs()), ticket: ticket, observeTicket: observe, wid: rec.Wid, resp: resp})
	default:
		rec.Status = WFailed
		rec.EndStatus = "validated"
		evs = append(evs, t.endFailure(rec, observe, out.Reason)...)
		evs = append(evs, Reached(EvModelOutcomeVerified, map[string]any{}))
	}

	evs = append(evs, t.tryDrain()...)
	evs = append(evs, t.runSettle()...)

	return rec.EndStatus, evs, nil
}

// endFailure validates a definitive failure and records what follows from it.
func (t *Tracker) endFailure(rec *WriteRecord, observe uint64, reason string) []Event {
	res, explained, evs := t.validateFailure(observe, rec.Bulk, reason)
	if !explained {
		return evs
	}
	rec.Result = &res
	t.recordRejection(rec.Bulk, reason)
	if rec.TrackKey {
		t.modelState = recordFailure(t.modelState, rec.Bulk, res)
		if rec.Role != RoleConflict && reason != "IDEMPOTENCY_KEY_CONFLICT" && !isAdmissionStage(res) && !uncommittedReasons[reason] {
			t.rememberReplayable(*rec, ReplayEntry{Wid: rec.Wid, Key: rec.Key, Status: "failed", Reason: reason, Stage: "fsm"})
		}
	}

	return append(evs, t.runFailureHooks(*rec, res, reason)...)
}

func outcomesEquivalent(a, b Outcome) bool {
	if a.OK != b.OK {
		return false
	}
	if !a.OK {
		return a.Reason == b.Reason && a.Code == b.Code
	}
	ra, errA := DecodeApplyResponse(a.Response)
	rb, errB := DecodeApplyResponse(b.Response)
	if errA != nil || errB != nil {
		return false
	}

	return ra.EqualVT(rb)
}

// LateWrite compares a definitive outcome of another attempt of an ended
// ticket with the recorded one (DESIGN §4.6).
func (t *Tracker) LateWrite(ticket uint64, out Outcome) ([]Event, *Refusal) {
	rec, ok := t.recordByTicket(ticket)
	if !ok {
		return nil, UnknownTicket(ticket)
	}
	if !out.Definitive() {
		return nonDefinitive(ticket, out)
	}
	switch rec.Status {
	case WInflight:
		return nil, Protocol("the ticket has not ended", map[string]any{"ticket": ticket})
	case WAbandoned:
		return nil, Protocol("the ticket was abandoned", map[string]any{"ticket": ticket})
	}

	first := *rec.Outcome
	d := map[string]any{"ticket": ticket, "wid": rec.Wid, "key": rec.Key, "firstOk": first.OK, "lateOk": out.OK,
		"firstReason": first.Reason, "lateReason": out.Reason}
	same := false
	switch {
	case first.OK && out.OK:
		a, errA := DecodeApplyResponse(first.Response)
		b, errB := DecodeApplyResponse(out.Response)
		if errA != nil || errB != nil {
			return nil, Protocol("cannot decode the success response", map[string]any{"ticket": ticket})
		}
		d["firstSeqs"] = logSeqs(a.GetLogs())
		d["lateSeqs"] = logSeqs(b.GetLogs())
		same = sequencesEqual(logSequences(a.GetLogs()), logSequences(b.GetLogs()))
	case !first.OK && !out.OK:
		same = first.Reason == out.Reason
	}

	evs := []Event{Sometimes(EvTwoDefinitiveOutcomes, true, map[string]any{"ticket": ticket, "same": same})}
	if !same {
		evs = append(evs, Violation(EvOneKeyOneOutcome, d))
	}

	return append(evs, t.runSettle()...), nil
}

// AbandonWrite drops a ticket whose rule never sent an attempt.
func (t *Tracker) AbandonWrite(ticket uint64, attempts int) ([]Event, *Refusal) {
	rec, ok := t.recordByTicket(ticket)
	if !ok {
		return nil, UnknownTicket(ticket)
	}
	if attempts > 0 {
		return nil, Protocol("a ticket with attempts must be handed to a resolver", map[string]any{"ticket": ticket, "attempts": attempts})
	}
	switch rec.Status {
	case WAbandoned:
		return nil, nil
	case WInflight:
	default:
		return nil, Protocol("the ticket already ended", map[string]any{"ticket": ticket})
	}
	delete(t.inflight, ticket)
	rec.Status = WAbandoned
	evs := t.tryDrain()

	return append(evs, t.runSettle()...), nil
}

// ---- reads ----

// BeginRead allocates a read ticket and snapshots the drained model in the
// same step.
func (t *Tracker) BeginRead(kind, rid string) (ticket uint64, snap oracle.GlobalState, version uint64) {
	t.ticketSeq++
	t.reads[t.ticketSeq] = readRec{kind: kind, rid: rid}

	return t.ticketSeq, t.modelState, t.version
}

// ReadFrontier is the ticket counter now (maxTicket of a read ending now).
func (t *Tracker) ReadFrontier() (maxTicket uint64) { return t.ticketSeq }

// ReadOpen reports whether ticket is an outstanding read.
func (t *Tracker) ReadOpen(ticket uint64) bool {
	_, ok := t.reads[ticket]

	return ok
}

// ReadKindOf returns the kind an outstanding read was begun with.
func (t *Tracker) ReadKindOf(ticket uint64) (string, bool) {
	r, ok := t.reads[ticket]

	return r.kind, ok
}

// FinishRead drops the read, drains what it held back and runs settle hooks.
func (t *Tracker) FinishRead(ticket uint64) []Event {
	if _, ok := t.reads[ticket]; !ok {
		return nil
	}
	delete(t.reads, ticket)
	evs := t.tryDrain()

	return append(evs, t.runSettle()...)
}

// ---- drain ----

// earliestOutstanding is the smallest ticket over in-flight writes and reads;
// empty=true when there are none.
func (t *Tracker) earliestOutstanding() (uint64, bool) {
	var earliest uint64
	found := false
	consider := func(tk uint64) {
		if !found || tk < earliest {
			earliest = tk
			found = true
		}
	}
	for tk := range t.inflight {
		consider(tk)
	}
	for tk := range t.reads {
		consider(tk)
	}

	return earliest, !found
}

// EarliestOutstanding is the smallest ticket over in-flight writes and reads.
func (t *Tracker) EarliestOutstanding() (ticket uint64, ok bool) {
	tk, empty := t.earliestOutstanding()

	return tk, !empty
}

// tryDrain drains buffered successes in log-sequence order while safe: the
// head drains only once every outstanding operation has a ticket greater than
// the head's observe ticket (M/processor.go tryDrain).
func (t *Tracker) tryDrain() []Event {
	var evs []Event
	for len(t.pending) > 0 {
		head := t.pending[0]
		minTicket, empty := t.earliestOutstanding()
		if !empty && minTicket <= head.observeTicket {
			break
		}

		t.pending = t.pending[1:]
		rec := t.records[head.wid]
		evs = append(evs, t.validateBulkSuccess(rec, head.resp)...)
		rec.Status = WCommitted
		t.version++
		evs = append(evs, Reached(EvModelOutcomeVerified, map[string]any{}))
		evs = append(evs, t.runSettle()...)
	}

	return evs
}

// insertPending keeps pending sorted ascending by minSeq.
func (t *Tracker) insertPending(entry *pendingEntry) {
	i := 0
	for i < len(t.pending) && t.pending[i].minSeq < entry.minSeq {
		i++
	}

	t.pending = append(t.pending, nil)
	copy(t.pending[i+1:], t.pending[i:])
	t.pending[i] = entry
}

// minLogSequence is the smallest non-zero Log.Sequence in logs, or 0.
func minLogSequence(logs []*commonpb.Log) uint64 {
	var lowest uint64
	for _, l := range logs {
		s := l.GetSequence()
		if s == 0 {
			continue
		}
		if lowest == 0 || s < lowest {
			lowest = s
		}
	}

	return lowest
}

// maxLogSequence is the largest Log.Sequence in logs, or 0.
func maxLogSequence(logs []*commonpb.Log) uint64 {
	var highest uint64
	for _, l := range logs {
		if s := l.GetSequence(); s > highest {
			highest = s
		}
	}

	return highest
}

// MinLogSequence is exported for the batch dump and families.
func MinLogSequence(logs []*commonpb.Log) uint64 { return minLogSequence(logs) }

// MaxLogSequence is exported for families.
func MaxLogSequence(logs []*commonpb.Log) uint64 { return maxLogSequence(logs) }

// ---- quiet periods ----

// BeginQuiet opens a quiet epoch (DESIGN §4.10). A new epoch while an older
// one is open ends the older one first; endedOld reports it.
func (t *Tracker) BeginQuiet(epoch uint64) (endedOld uint64, ref *Refusal) {
	if t.quietOpen && t.quietEpoch == epoch {
		return 0, nil
	}
	if len(t.inflight) > 0 || len(t.reads) > 0 {
		return 0, refuse(409, CodeNotQuiet, "tickets are outstanding", map[string]any{"inflight": len(t.inflight), "reads": len(t.reads)})
	}
	if t.modelState.MaintenanceMode() {
		return 0, refuse(409, CodeNotQuiet, "the drained model has maintenance mode on", map[string]any{"reason": "maintenance"})
	}
	if t.quietOpen {
		endedOld = t.quietEpoch
	}
	t.quietOpen = true
	t.quietEpoch = epoch
	t.quietBegins++

	return endedOld, nil
}

// EndQuiet closes the quiet epoch; ending another or a closed epoch is a
// no-op.
func (t *Tracker) EndQuiet(epoch uint64) bool {
	if !t.quietOpen || t.quietEpoch != epoch {
		return false
	}
	t.quietOpen = false

	return true
}

// QuietEpoch returns the last epoch begun and whether it is open.
func (t *Tracker) QuietEpoch() (epoch uint64, open bool) { return t.quietEpoch, t.quietOpen }

// QuietBegins is the number of quiet periods begun so far.
func (t *Tracker) QuietBegins() uint64 { return t.quietBegins }

// ---- model access ----

// MatchesModel reports whether some candidate base satisfies m.
func (t *Tracker) MatchesModel(maxTicket uint64, m func(oracle.GlobalState) bool) bool {
	matched := false
	t.walkCandidateStates(maxTicket, func(base oracle.GlobalState) bool {
		matched = m(base)

		return matched
	}, nil)

	return matched
}

// WalkCandidates enumerates candidate bases (visit) and successful
// transitions (transition, may be nil); returning true from either stops.
func (t *Tracker) WalkCandidates(maxTicket uint64, visit func(oracle.GlobalState) bool,
	transition func(before oracle.GlobalState, b oracle.Bulk, r oracle.ApplyResult) bool,
) {
	t.walkCandidateStates(maxTicket, visit, transition)
}

// Model returns the drained state (exact predecessor of pending[0]).
func (t *Tracker) Model() oracle.GlobalState { return t.modelState }

// MutateModel applies fn to the drained state in place. Only for observation
// state; panics if fn changed the business fingerprint.
func (t *Tracker) MutateModel(tag string, fn func(*oracle.GlobalState)) {
	before := businessFingerprint(t.modelState)
	fn(&t.modelState)
	if businessFingerprint(t.modelState) != before {
		panic(fmt.Sprintf("oraclecheck: MutateModel(%s) changed business state", tag))
	}
}

// Version is the number of bulks drained so far.
func (t *Tracker) Version() uint64 { return t.version }

// Outstanding counts in-flight writes, open reads and pending successes.
func (t *Tracker) Outstanding() (inflight, reads, pending int) {
	return len(t.inflight), len(t.reads), len(t.pending)
}

// InflightTickets returns the in-flight tickets, sorted.
func (t *Tracker) InflightTickets() []uint64 {
	out := make([]uint64, 0, len(t.inflight))
	for tk := range t.inflight {
		out = append(out, tk)
	}
	slices.Sort(out)

	return out
}

// PendingTickets returns the pending tickets in drain order.
func (t *Tracker) PendingTickets() []uint64 {
	out := make([]uint64, 0, len(t.pending))
	for _, p := range t.pending {
		out = append(out, p.ticket)
	}

	return out
}

// InflightBulks returns the in-flight writes, sorted by ticket.
func (t *Tracker) InflightBulks() []WriteRecord {
	out := make([]WriteRecord, 0, len(t.inflight))
	for _, tk := range t.InflightTickets() {
		out = append(out, *t.records[t.inflight[tk]])
	}

	return out
}

// PendingBulks returns the pending successes in drain order.
func (t *Tracker) PendingBulks() []WriteRecord {
	out := make([]WriteRecord, 0, len(t.pending))
	for _, p := range t.pending {
		out = append(out, *t.records[p.wid])
	}

	return out
}

// OpenReads returns the open read tickets, sorted.
func (t *Tracker) OpenReads() []uint64 {
	out := make([]uint64, 0, len(t.reads))
	for tk := range t.reads {
		out = append(out, tk)
	}
	slices.Sort(out)

	return out
}

// Record returns a write by wid.
func (t *Tracker) Record(wid string) (WriteRecord, bool) {
	r, ok := t.records[wid]
	if !ok {
		return WriteRecord{}, false
	}

	return *r, true
}

// RecordByTicket returns a write by ticket.
func (t *Tracker) RecordByTicket(ticket uint64) (WriteRecord, bool) {
	r, ok := t.recordByTicket(ticket)
	if !ok {
		return WriteRecord{}, false
	}

	return *r, true
}

// WritesByKey returns every write registered under an idempotency key, in
// wid registration order.
func (t *Tracker) WritesByKey(key string) []WriteRecord {
	wids := t.byKey[key]
	out := make([]WriteRecord, 0, len(wids))
	for _, w := range wids {
		out = append(out, *t.records[w])
	}

	return out
}

// Ext returns the family side table stored under key, creating it with init.
func (t *Tracker) Ext(key string, init func() any) any {
	v, ok := t.ext[key]
	if !ok {
		v = init()
		t.ext[key] = v
	}

	return v
}

// LedgerNames returns every tracked ledger name a drained bulk created,
// sorted.
func (t *Tracker) LedgerNames() []string {
	out := make([]string, 0, len(t.ledgerNames))
	for n := range t.ledgerNames {
		out = append(out, n)
	}
	slices.Sort(out)

	return out
}

// TxForced reports whether a committed transaction was created with force.
func (t *Tracker) TxForced(ledger string, id uint64) bool { return t.forcedTxs[ledger][id] }

// ---- committed responses (DESIGN §5.5) ----

func (t *Tracker) keepResponse(wid string, resp *servicepb.ApplyResponse) {
	if _, ok := t.responses[wid]; !ok {
		t.responseOrder = append(t.responseOrder, wid)
	}
	t.responses[wid] = resp
	for len(t.responseOrder) > CommittedResponseCap {
		delete(t.responses, t.responseOrder[0])
		t.responseOrder = t.responseOrder[1:]
		t.resync = true
	}
}

// CommittedResponse returns a drained bulk's response until released.
func (t *Tracker) CommittedResponse(wid string) (*servicepb.ApplyResponse, bool) {
	r, ok := t.responses[wid]

	return r, ok
}

// ReleaseCommittedResponses drops every held response up to and including
// upToWid, in drain order. Unknown wid: nothing is released.
func (t *Tracker) ReleaseCommittedResponses(upToWid string) int {
	idx := slices.Index(t.responseOrder, upToWid)
	if idx < 0 {
		return 0
	}
	for _, w := range t.responseOrder[:idx+1] {
		delete(t.responses, w)
	}
	t.responseOrder = append([]string(nil), t.responseOrder[idx+1:]...)

	return idx + 1
}

// HeldResponses returns the wids of the held responses, in drain order.
func (t *Tracker) HeldResponses() []string { return append([]string(nil), t.responseOrder...) }

// ResyncNeeded reports that the response cap dropped unreleased responses.
func (t *Tracker) ResyncNeeded() bool { return t.resync }

// ClearResync acknowledges a resync.
func (t *Tracker) ClearResync() { t.resync = false }

// AuditCursor is the last audit sequence the incremental replay consumed
// ("0" before any), reported by /quiet/begin. Task 11's audit section
// advances it with SetAuditCursor.
func (t *Tracker) AuditCursor() string {
	if t.auditCursor == "" {
		return "0"
	}

	return t.auditCursor
}

// SetAuditCursor records the audit replay's cursor.
func (t *Tracker) SetAuditCursor(c string) { t.auditCursor = c }
