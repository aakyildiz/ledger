package checkpointcheck

import (
	"encoding/json"
	"slices"
	"strconv"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	oc "github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck"
	"github.com/formancehq/ledger/v3/tests/oracle"
)

// Registration names.
const (
	KindCheckpointRead          = "CheckpointRead"
	KindPredictedCheckpointRead = "PredictedCheckpointRead"
	KindCheckpointRegistry      = "CheckpointRegistry"
	KindCheckpointSchedule      = "CheckpointSchedule"
	KindCheckpointInfo          = "CheckpointInfo"
	ViewCheckpoints             = "checkpoints"
	BeginHookSerialize          = "checkpoint-serialize"
	DrainHookCoverage           = "checkpoint-coverage"
	FailureHookCoverage         = "checkpoint-limit"
	extKey                      = "checkpointcheck"
)

func init() {
	oc.RegisterReadKind(oc.ReadKind{Name: KindCheckpointRead, Validate: ValidateCheckpointRead})
	oc.RegisterReadKind(oc.ReadKind{Name: KindPredictedCheckpointRead, Validate: ValidatePredictedCheckpointRead})
	oc.RegisterReadKind(oc.ReadKind{Name: KindCheckpointRegistry, Validate: ValidateCheckpointRegistry})
	oc.RegisterReadKind(oc.ReadKind{Name: KindCheckpointSchedule, Validate: ValidateCheckpointSchedule})
	oc.RegisterReadKind(oc.ReadKind{Name: KindCheckpointInfo, Validate: ValidateCheckpointInfo})
	oc.RegisterView(ViewCheckpoints, viewCheckpoints)
	oc.RegisterBeginHook(BeginHookSerialize, SerializeCreates)
	oc.RegisterDrainHook(DrainHookCoverage, drainCoverage)
	oc.RegisterFailureHook(FailureHookCoverage, failureCoverage)
	oc.RegisterCoverage(CovCreate, CovDelete, CovFrozenRead, CovDeletedRead, CovListing, CovLimit,
		CovScheduleSet, CovScheduleDelete, CovScheduleRead)
}

// createsCheckpoint reports whether a bulk carries a createQueryCheckpoint.
func createsCheckpoint(reqs []*servicepb.Request) bool {
	for _, r := range reqs {
		if r.GetCreateQueryCheckpoint() != nil {
			return true
		}
	}

	return false
}

// SerializeCreates is the begin hook `checkpoint-serialize` (replaces
// checkpointCreateMu, M/main.go:344-380): a bulk creating a query checkpoint
// is refused (`serialized`) while another create is in flight or committed
// but not drained; otherwise the drained model's next id is exactly the id
// the create will get, published as predicted.checkpointId.
func SerializeCreates(t *oc.Tracker, rec oc.WriteRecord) (map[string]any, *oc.Refusal) {
	if !createsCheckpoint(rec.Requests) {
		return nil, nil
	}
	for _, list := range [][]oc.WriteRecord{t.InflightBulks(), t.PendingBulks()} {
		for _, other := range list {
			if createsCheckpoint(other.Requests) {
				return nil, oc.Serialized("a query checkpoint create is still undrained",
					map[string]any{"wid": rec.Wid, "outstanding": other.Wid, "ticket": other.Ticket})
			}
		}
	}

	return map[string]any{"checkpointId": strconv.FormatUint(t.Model().NextQueryCheckpointID(), 10)}, nil
}

// side is the family side table: deleted checkpoint ids in deletion order (a
// superset of the Tracker's bounded retained history, trimmed lazily).
type side struct {
	deleted []uint64
}

func sideOf(t *oc.Tracker) *side {
	return t.Ext(extKey, func() any { return &side{} }).(*side)
}

// RetainedDeleted lists the deleted checkpoint ids whose frozen snapshot the
// Tracker still holds, in deletion order.
func RetainedDeleted(t *oc.Tracker) []uint64 {
	s := sideOf(t)
	kept := s.deleted[:0]
	for _, id := range s.deleted {
		if t.CheckpointDeleted(id) {
			kept = append(kept, id)
		}
	}
	s.deleted = kept

	return slices.Clone(kept)
}

// drainCoverage runs after a commit's cross-check agreed (the coverage of
// M/checkpoints.go recordCheckpoints) and records deletions for the view.
func drainCoverage(t *oc.Tracker, _ oc.WriteRecord, resp *servicepb.ApplyResponse, _, _ oracle.GlobalState) []oc.Event {
	var evs []oc.Event
	for _, log := range resp.GetLogs() {
		p := log.GetPayload()
		switch {
		case p.GetCreatedQueryCheckpoint() != nil:
			evs = append(evs, coverage(CovCreate, map[string]any{"checkpoint": p.GetCreatedQueryCheckpoint().GetCheckpointId()})...)
		case p.GetDeletedQueryCheckpoint() != nil:
			id := p.GetDeletedQueryCheckpoint().GetCheckpointId()
			s := sideOf(t)
			if !slices.Contains(s.deleted, id) {
				s.deleted = append(s.deleted, id)
			}
			evs = append(evs, coverage(CovDelete, map[string]any{"checkpoint": id})...)
		case p.GetSetQueryCheckpointSchedule() != nil:
			evs = append(evs, coverage(CovScheduleSet, map[string]any{"cron": p.GetSetQueryCheckpointSchedule().GetCron()})...)
		case p.GetDeleteQueryCheckpointSchedule() != nil:
			evs = append(evs, coverage(CovScheduleDelete, nil)...)
		}
	}

	return evs
}

// failureCoverage credits an explained CHECKPOINT_LIMIT_REACHED.
func failureCoverage(_ *oc.Tracker, rec oc.WriteRecord, _ oracle.ApplyResult, reason string) []oc.Event {
	if reason != domain.ErrReasonCheckpointLimitReached || !createsCheckpoint(rec.Requests) {
		return nil
	}

	return coverage(CovLimit, map[string]any{"wid": rec.Wid})
}

// viewParams are the `checkpoints` view's optional target fields. They are
// deliberately not named ledgerPick: the core would resolve that on the live
// snapshot, while the target ledger must be live in the FROZEN one.
type viewParams struct {
	Pick         *uint64 `json:"pick,omitempty"`
	CpLedgerPick uint64  `json:"cpLedgerPick,omitempty"`
	Table        string  `json:"table,omitempty"`
	Probe        string  `json:"probe,omitempty"`
	LogPick      uint64  `json:"logPick,omitempty"`
	SeenPick     uint64  `json:"seenPick,omitempty"`
}

func idStrings(ids []uint64) []string {
	out := make([]string, len(ids))
	for i, id := range ids {
		out[i] = strconv.FormatUint(id, 10)
	}

	return out
}

// viewCheckpoints answers {"live","deleted","nextId","limit","schedule"} on
// the snapshot, and with "pick" a "target": the checkpoint
// (live ++ retained deleted)[pick mod n], whether it is deleted, a ledger
// live in its frozen state ([cpLedgerPick mod n], null if none), a seek on
// that frozen ledger when "table" is set, a frozen log sequence
// ([logPick mod n] of the frozen global stream), its max sequence, and a
// "seenLedger": [seenPick mod n] of every non-reserved tracked ledger name the
// oracle has seen, created after the checkpoint or deleted included (the
// ListLedgers cursor pool of M/checkpoint_reads.go:195-197).
func viewCheckpoints(snap oracle.GlobalState, t *oc.Tracker, params json.RawMessage) (any, error) {
	var p viewParams
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, err
	}
	var live []uint64
	for _, id := range snap.QueryCheckpointIDs() {
		if _, ok := t.FrozenCheckpoint(id); ok {
			live = append(live, id)
		}
	}
	var deleted []uint64
	for _, id := range RetainedDeleted(t) {
		if !snap.QueryCheckpointExists(id) {
			deleted = append(deleted, id)
		}
	}
	out := map[string]any{
		"live":     idStrings(live),
		"deleted":  idStrings(deleted),
		"nextId":   strconv.FormatUint(snap.NextQueryCheckpointID(), 10),
		"limit":    snap.QueryCheckpointLimit(),
		"schedule": snap.QueryCheckpointSchedule(),
	}
	if p.Pick == nil {
		return out, nil
	}
	all := append(slices.Clone(live), deleted...)
	if len(all) == 0 {
		out["target"] = nil

		return out, nil
	}
	id := all[*p.Pick%uint64(len(all))]
	frozen, _ := t.FrozenCheckpoint(id)
	maxSeq, _ := t.FrozenCheckpointMaxSequence(id)
	target := map[string]any{
		"id":          strconv.FormatUint(id, 10),
		"deleted":     !snap.QueryCheckpointExists(id),
		"maxSequence": strconv.FormatUint(maxSeq, 10),
		"ledger":      nil,
	}
	var seen []string
	for _, name := range t.LedgerNames() {
		if !oc.IsReservedLedger(t.Config().TrackedPrefix, name) {
			seen = append(seen, name)
		}
	}
	if len(seen) > 0 {
		target["seenLedger"] = seen[p.SeenPick%uint64(len(seen))]
	}
	if seqs := frozenLogSequences(frozen); len(seqs) > 0 {
		target["logSeq"] = strconv.FormatUint(seqs[p.LogPick%uint64(len(seqs))], 10)
	}
	if ledgers := t.LiveLedgers(frozen, false); len(ledgers) > 0 {
		ledger := ledgers[p.CpLedgerPick%uint64(len(ledgers))]
		target["ledger"] = ledger
		if p.Table != "" {
			req, _ := json.Marshal(map[string]any{"name": "seek", "ledger": ledger, "table": p.Table, "probe": p.Probe})
			res, err := t.ComputeViews(frozen, []json.RawMessage{req})
			if err != nil {
				return nil, err
			}
			target["seek"] = res[0]
		}
	}
	out["target"] = target

	return out, nil
}

// frozenLogSequences lists the global log sequences a frozen state holds,
// ascending.
func frozenLogSequences(gs oracle.GlobalState) []uint64 {
	var seqs []uint64
	for seq := range gs.Logs() {
		seqs = append(seqs, seq)
	}
	slices.Sort(seqs)

	return seqs
}
