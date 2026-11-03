package oraclecheck

import (
	"maps"
	"slices"
	"strings"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/tests/oracle"
)

// Side tables lifted from M/checker.go: what the model cannot derive and the
// audit and log validators of the families need. Read-only for families.

// CommittedLog is what the model knows about one committed log sequence.
type CommittedLog struct {
	Ledger string
	ID     uint64
	Kind   string
}

// CommittedBulk is the log range one committed bulk produced (one audit
// entry).
type CommittedBulk struct {
	Wid            string
	MinSeq, MaxSeq uint64
	Orders         uint32
}

// LedgerIdentity is the id and creation timestamp a ledger's creation log
// reported.
type LedgerIdentity struct {
	ID        uint32
	CreatedAt *commonpb.Timestamp
}

// RejectedBulk is the shape of one model-explained rejection as the audit
// trail records it.
type RejectedBulk struct {
	Ledgers string // distinct ledgers, ascending, comma-joined
	Orders  uint32
	Reason  string
}

// LedgerLogRecord is a committed ledger-metadata log: its ledger and kind.
type LedgerLogRecord struct {
	Ledger string
	Kind   string
}

type sideTables struct {
	ledgerLogSeqs    map[uint64]LedgerLogRecord
	committedLogs    map[uint64]CommittedLog
	committedBulks   map[uint64]CommittedBulk
	setupMaxSeq      uint64
	ledgerIdentities map[string]LedgerIdentity
	rejections       map[RejectedBulk]struct{}
}

func newSideTables() sideTables {
	return sideTables{
		ledgerLogSeqs:    map[uint64]LedgerLogRecord{},
		committedLogs:    map[uint64]CommittedLog{},
		committedBulks:   map[uint64]CommittedBulk{},
		ledgerIdentities: map[string]LedgerIdentity{},
		rejections:       map[RejectedBulk]struct{}{},
	}
}

// CommittedLogs returns a copy of the committed log table.
func (t *Tracker) CommittedLogs() map[uint64]CommittedLog { return maps.Clone(t.committedLogs) }

// CommittedBulks returns a copy of the committed bulk table, keyed by min
// sequence.
func (t *Tracker) CommittedBulks() map[uint64]CommittedBulk { return maps.Clone(t.committedBulks) }

// LedgerIdentities returns a copy of the ledger identity table.
func (t *Tracker) LedgerIdentities() map[string]LedgerIdentity { return maps.Clone(t.ledgerIdentities) }

// Rejections returns a copy of the explained rejection shapes.
func (t *Tracker) Rejections() map[RejectedBulk]struct{} { return maps.Clone(t.rejections) }

// LedgerLogSeqs returns a copy of the ledger-metadata log table.
func (t *Tracker) LedgerLogSeqs() map[uint64]LedgerLogRecord { return maps.Clone(t.ledgerLogSeqs) }

// SetupMaxSeq is the last sequence before tracked writes started (0 when the
// oracle saw every log of its ledgers).
func (t *Tracker) SetupMaxSeq() uint64 { return t.setupMaxSeq }

// SetSetupMaxSeq records the setup horizon.
func (t *Tracker) SetSetupMaxSeq(seq uint64) { t.setupMaxSeq = seq }

// recordRejection remembers a rejection the model explained.
func (t *Tracker) recordRejection(bulk oracle.Bulk, reason string) {
	t.rejections[RejectedBulk{
		Ledgers: strings.Join(distinctLedgers(bulk), ","),
		Orders:  uint32(len(bulk.Requests)),
		Reason:  reason,
	}] = struct{}{}
}

// distinctLedgers lists the ledgers a bulk's requests name, ascending.
func distinctLedgers(bulk oracle.Bulk) []string {
	seen := map[string]bool{}
	var out []string
	for _, r := range bulk.Requests {
		if l := oracle.LedgerOf(r); l != "" && !seen[l] {
			seen[l] = true
			out = append(out, l)
		}
	}
	slices.Sort(out)

	return out
}

// recordCommittedBulk remembers the boundaries a bulk committed at.
func (t *Tracker) recordCommittedBulk(bulk oracle.Bulk, wid string, logs []*commonpb.Log) {
	var minSeq, maxSeq uint64

	for _, l := range logs {
		seq := l.GetSequence()
		if seq == 0 {
			continue
		}
		if minSeq == 0 || seq < minSeq {
			minSeq = seq
		}
		maxSeq = max(maxSeq, seq)
	}

	if minSeq == 0 {
		return
	}

	t.committedBulks[minSeq] = CommittedBulk{Wid: wid, MinSeq: minSeq, MaxSeq: maxSeq, Orders: uint32(len(bulk.Requests))}

	for i, req := range bulk.Requests {
		if i >= len(logs) {
			break
		}

		seq := logs[i].GetSequence()
		if seq == 0 {
			continue
		}

		apply := logs[i].GetPayload().GetApply()
		t.committedLogs[seq] = CommittedLog{
			Ledger: apply.GetLedgerName(),
			ID:     apply.GetLog().GetId(),
			Kind:   requestLogKind(req),
		}

		if created := logs[i].GetPayload().GetCreateLedger(); created != nil {
			t.ledgerIdentities[created.GetName()] = LedgerIdentity{
				ID:        created.GetId(),
				CreatedAt: created.GetCreatedAt(),
			}
		}
	}
}

// requestLogKind names the order a request carries, the way the audit index
// keys it.
func requestLogKind(req *servicepb.Request) string {
	switch r := req.GetType().(type) {
	case *servicepb.Request_Apply:
		switch r.Apply.GetAction().GetData().(type) {
		case *servicepb.LedgerAction_CreateTransaction:
			return "created_transaction"
		case *servicepb.LedgerAction_RevertTransaction:
			return "reverted_transaction"
		case *servicepb.LedgerAction_AddMetadata:
			return "saved_metadata"
		case *servicepb.LedgerAction_DeleteMetadata:
			return "deleted_metadata"
		case *servicepb.LedgerAction_AddAccountType:
			return "added_account_type"
		case *servicepb.LedgerAction_RemoveAccountType:
			return "removed_account_type"
		case *servicepb.LedgerAction_SetDefaultEnforcementMode:
			return "updated_default_enforcement_mode"
		default:
			return ""
		}
	case *servicepb.Request_SetMetadataFieldType:
		return "set_metadata_field_type"
	case *servicepb.Request_RemoveMetadataFieldType:
		return "removed_metadata_field_type"
	case *servicepb.Request_CreateIndex:
		return "create_index"
	case *servicepb.Request_DropIndex:
		return "drop_index"
	case *servicepb.Request_AddAccountType:
		return "added_account_type"
	case *servicepb.Request_RemoveAccountType:
		return "removed_account_type"
	case *servicepb.Request_SetDefaultEnforcementMode:
		return "updated_default_enforcement_mode"
	default:
		return ledgerLogKindOf(req)
	}
}

// RequestLogKind exposes requestLogKind.
func RequestLogKind(req *servicepb.Request) string { return requestLogKind(req) }

// learnLedgerLogSequences records the sequences of a committed bulk's logs
// that carry no ledger-local id.
func (t *Tracker) learnLedgerLogSequences(bulk oracle.Bulk, logs []*commonpb.Log) {
	for i, req := range bulk.Requests {
		if i >= len(logs) {
			break
		}

		seq := logs[i].GetSequence()
		if seq == 0 || logs[i].GetPayload().GetApply().GetLog().GetId() != 0 {
			continue
		}

		t.ledgerLogSeqs[seq] = LedgerLogRecord{Ledger: oracle.LedgerOf(req), Kind: ledgerLogKindOf(req)}
	}
}

// ledgerLogKindOf names the order behind a ledger-metadata log.
func ledgerLogKindOf(req *servicepb.Request) string {
	switch req.GetType().(type) {
	case *servicepb.Request_SaveLedgerMetadata:
		return "saved_ledger_metadata"
	case *servicepb.Request_DeleteLedgerMetadata:
		return "deleted_ledger_metadata"
	default:
		return ""
	}
}

// ---- replayable registry (M/replay.go rememberReplayable, DESIGN §4.7) ----

// ReplayEntry is a flagged (trackKey) bulk the driver may re-send: every
// commit, and every frozen failure.
type ReplayEntry struct {
	Wid     string   `json:"wid"`
	Key     string   `json:"key"`
	Status  string   `json:"status"` // committed | failed
	Reason  string   `json:"reason"`
	Stage   string   `json:"stage"` // "" (committed) | admission | fsm
	LogSeqs []uint64 `json:"-"`     // committed: the sequences the commit was assigned
}

// rememberReplayable records an entry, excluding bulks that touch reserved
// entities and keys already listed; the registry keeps the newest
// ReplayRegistryCap entries.
func (t *Tracker) rememberReplayable(rec WriteRecord, e ReplayEntry) {
	if rec.Key == "" || !rec.TrackKey || TouchesReserved(t.cfg.TrackedPrefix, rec.Bulk) {
		return
	}
	for _, have := range t.replayable {
		if have.Key == e.Key {
			return
		}
	}
	t.replayable = append(t.replayable, e)
	if len(t.replayable) > ReplayRegistryCap {
		t.replayable = append([]ReplayEntry(nil), t.replayable[len(t.replayable)-ReplayRegistryCap:]...)
	}
}

// Replayable returns the registry, oldest first.
func (t *Tracker) Replayable() []ReplayEntry {
	return slices.Clone(t.replayable)
}

// ---- checkpoints (M/checkpoints.go recordCheckpoints) ----

type checkpointSnapshot struct {
	state       oracle.GlobalState
	maxSequence uint64
}

// recordCheckpoints runs only after a validated commit advanced the model.
// Creates are singleton bulks, so the post-state has precisely the business
// contents at max_sequence.
func (t *Tracker) recordCheckpoints(logs []*commonpb.Log) {
	for _, log := range logs {
		payload := log.GetPayload()
		switch {
		case payload.GetCreatedQueryCheckpoint() != nil:
			cp := payload.GetCreatedQueryCheckpoint()
			t.checkpoints[cp.GetCheckpointId()] = checkpointSnapshot{state: t.modelState, maxSequence: cp.GetMaxSequence()}
		case payload.GetDeletedQueryCheckpoint() != nil:
			id := payload.GetDeletedQueryCheckpoint().GetCheckpointId()
			snapshot, known := t.checkpoints[id]
			delete(t.checkpoints, id)
			if !known {
				continue
			}
			t.deletedCheckpointSnapshots[id] = snapshot
			t.deletedCheckpoints = append(t.deletedCheckpoints, id)
			if len(t.deletedCheckpoints) > deletedCheckpointHistoryCap {
				delete(t.deletedCheckpointSnapshots, t.deletedCheckpoints[0])
				t.deletedCheckpoints = t.deletedCheckpoints[1:]
			}
		}
	}
}

// FrozenCheckpoint returns the business state frozen by a checkpoint's
// creation (live, or deleted within the bounded history).
func (t *Tracker) FrozenCheckpoint(id uint64) (oracle.GlobalState, bool) {
	if s, ok := t.checkpoints[id]; ok {
		return s.state, true
	}
	if s, ok := t.deletedCheckpointSnapshots[id]; ok {
		return s.state, true
	}

	return oracle.GlobalState{}, false
}

// FrozenCheckpointMaxSequence returns a checkpoint's max_sequence.
func (t *Tracker) FrozenCheckpointMaxSequence(id uint64) (uint64, bool) {
	if s, ok := t.checkpoints[id]; ok {
		return s.maxSequence, true
	}
	if s, ok := t.deletedCheckpointSnapshots[id]; ok {
		return s.maxSequence, true
	}

	return 0, false
}

// CheckpointDeleted reports whether a known checkpoint was deleted (within the
// bounded history).
func (t *Tracker) CheckpointDeleted(id uint64) bool {
	_, ok := t.deletedCheckpointSnapshots[id]

	return ok
}

// ---- exported pure helpers for families (copies live in match/) ----

// ReplayOrdersMatch reports whether a replay's logs echo the frozen orders.
func ReplayOrdersMatch(bulk oracle.Bulk, orders []oracle.OrderResult, logs []*commonpb.Log) bool {
	return replayOrdersMatch(bulk, orders, logs)
}

// LogSequences extracts the log sequences from a response, in order.
func LogSequences(logs []*commonpb.Log) []uint64 { return logSequences(logs) }

// SequencesEqual reports whether two sequence slices are identical.
func SequencesEqual(a, b []uint64) bool { return sequencesEqual(a, b) }
