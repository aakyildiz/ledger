package replaycheck

import (
	"encoding/json"
	"strconv"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	oc "github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck"
	"github.com/formancehq/ledger/v3/tests/oracle"
)

func init() {
	oc.RegisterFailureHook("replaycheck-conflict", conflictHook)
	oc.RegisterCoverage(CovSkipAlreadyReverted, CovSkipMetadataNotFound)
	oc.RegisterDrainHook("replaycheck-skip-coverage", skipCoverageHook)
	oc.RegisterView(ViewRevertSeek, viewRevertSeek)
}

// conflictHook reports that an explained IDEMPOTENCY_KEY_CONFLICT rejection
// reached the oracle (M/validate.go:561 coverage switch arm).
func conflictHook(_ *oc.Tracker, rec oc.WriteRecord, _ oracle.ApplyResult, reason string) []oc.Event {
	if reason != "IDEMPOTENCY_KEY_CONFLICT" {
		return nil
	}

	return []oc.Event{oc.Reached(EvConflictRejection, map[string]any{"wid": rec.Wid, "role": rec.Role, "of": rec.Of})}
}

// skipCoverageHook evaluates, after a commit passed every check, whether an
// opted-in revert of a reverted transaction or delete of an absent metadata
// key produced an OrderSkipped log followed by a committed (not skipped)
// order in the same bulk (M/apply_surface.go applyCoverageHits). Only hits
// are emitted: the service catalogues every probe unhit at startup.
func skipCoverageHook(_ *oc.Tracker, rec oc.WriteRecord, _ *servicepb.ApplyResponse, before, _ oracle.GlobalState) []oc.Event {
	if !optsIntoFamilySkip(rec.Bulk) {
		return nil
	}
	res := before.Apply(rec.Bulk)
	if !res.OK {
		return nil
	}
	var evs []oc.Event
	for _, hit := range SkipHits(res.Orders) {
		evs = append(evs, oc.Raw(hit, true, map[string]any{"wid": rec.Wid}))
	}

	return evs
}

// optsIntoFamilySkip reports whether some order opts into one of this
// family's skippable reasons (a cheap filter before re-applying the bulk).
func optsIntoFamilySkip(b oracle.Bulk) bool {
	for _, r := range b.Requests {
		for _, reason := range r.GetApply().GetSkippableReasons() {
			if reason == commonpb.ErrorReason_ERROR_REASON_TRANSACTION_ALREADY_REVERTED ||
				reason == commonpb.ErrorReason_ERROR_REASON_METADATA_NOT_FOUND {
				return true
			}
		}
	}

	return false
}

// SkipHits lists this family's probes a validated commit's orders reach, each
// at most once.
func SkipHits(orders []oracle.OrderResult) []oc.EventID {
	var out []oc.EventID
	seen := map[oc.EventID]bool{}
	for i, order := range orders {
		if order.Skipped == nil || i+1 >= len(orders) || orders[i+1].Skipped != nil {
			continue
		}
		var id oc.EventID
		switch order.Skipped.GetReason() {
		case commonpb.ErrorReason_ERROR_REASON_TRANSACTION_ALREADY_REVERTED:
			id = CovSkipAlreadyReverted
		case commonpb.ErrorReason_ERROR_REASON_METADATA_NOT_FOUND:
			id = CovSkipMetadataNotFound
		default:
			continue
		}
		if !seen[id] {
			seen[id] = true
			out = append(out, id)
		}
	}

	return out
}

type revertSeekParams struct {
	Ledger string `json:"ledger"`
	Probe  string `json:"probe"`
	Owned  bool   `json:"owned"`
}

// revertSeekScanCap bounds the transactions one revertSeek inspects.
const revertSeekScanCap = 1 << 16

// viewRevertSeek answers the first revert transaction (one whose
// revertsTransaction is set) with id at or after probe, wrapping, like the
// core `seek` over `txs`; reserved transactions are skipped unless owned.
// Entry: {id, revertsTransaction, reverted}. Aiming only.
func viewRevertSeek(snap oracle.GlobalState, t *oc.Tracker, params json.RawMessage) (any, error) {
	var p revertSeekParams
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, err
	}
	miss := map[string]any{"found": false, "wrapped": false, "entry": nil}
	txs := snap.Ledger(p.Ledger).Txs()
	n := txs.Len()
	if n == 0 {
		return miss, nil
	}
	probe, err := strconv.ParseUint(p.Probe, 10, 64)
	if err != nil {
		probe = 0
	}
	start := int(min(probe, uint64(n+1)))
	if start < 1 {
		start = 1
	}
	prefix := t.Config().TrackedPrefix
	for step := range min(n, revertSeekScanCap) {
		id := start + step
		wrapped := false
		if id > n {
			id -= n
			wrapped = true
		}
		tx := txs.Get(id - 1)
		if tx.RevertsTransaction() == 0 {
			continue
		}
		if !p.Owned && oc.IsReservedTx(prefix, tx.Reference(), tx.Postings()) {
			continue
		}

		return map[string]any{"found": true, "wrapped": wrapped, "entry": map[string]any{
			"id": strconv.Itoa(id), "revertsTransaction": strconv.FormatUint(tx.RevertsTransaction(), 10), "reverted": tx.Reverted(),
		}}, nil
	}

	return miss, nil
}
