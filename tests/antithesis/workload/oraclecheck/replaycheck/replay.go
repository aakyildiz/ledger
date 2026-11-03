package replaycheck

import (
	"encoding/json"

	"google.golang.org/protobuf/encoding/protojson"

	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	oc "github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck"
	"github.com/formancehq/ledger/v3/tests/oracle"
)

func init() {
	oc.RegisterReadKind(oc.ReadKind{Name: ReadKindReplay, SendsWrites: true, Validate: ValidateReplay})
}

// replayParams are the kind-specific params of a Replay read
// (`/read/end` params; `wid` is a cross-check only).
type replayParams struct {
	Wid string `json:"wid"`
}

// Original returns the single original write registered under key (conflict
// bulks under the same key are not originals).
func Original(t *oc.Tracker, key string) (oc.WriteRecord, bool) {
	for _, rec := range t.WritesByKey(key) {
		if rec.Role == oc.RoleOriginal || rec.Role == "" {
			return rec, true
		}
	}

	return oc.WriteRecord{}, false
}

// originalKind classifies what a replay of rec must return.
type originalKind uint8

const (
	origSkip      originalKind = iota // not replayable: in flight, abandoned, admission, unfrozen, unexplained
	origCommitted                     // a definitive success
	origFrozen                        // an explained FSM-stage failure the model froze
)

// classify mirrors the conditions under which the tracker lists a write in
// `replayable` (commit.go / tracker.go endFailure), computed from the record
// so that eviction from the bounded registry does not matter.
func classify(rec oc.WriteRecord) originalKind {
	if !rec.TrackKey || rec.Outcome == nil {
		return origSkip
	}
	switch rec.Status {
	case oc.WCommitted, oc.WPending:
		if rec.Outcome.OK {
			return origCommitted
		}
	case oc.WFailed:
		reason := rec.Outcome.Reason
		if rec.Result == nil || oc.IsAdmissionStage(*rec.Result) || oc.UncommittedReason(reason) ||
			reason == "IDEMPOTENCY_KEY_CONFLICT" || !oc.IsDefinitiveReason(reason) {
			return origSkip
		}

		return origFrozen
	}

	return origSkip
}

// ValidateReplay judges a re-send of a flagged original under its key
// (lifted from M/replay.go runReplay/validateReplay, extended to frozen
// failures). The original is found by the request's key; params.wid is a
// cross-check. Transient outcomes never reach here (SendsWrites kind); other
// non-definitive outcomes are skipped.
func ValidateReplay(c *oc.ReadCtx) []oc.Event {
	var req servicepb.ApplyRequest
	if err := (protojson.UnmarshalOptions{DiscardUnknown: true}).Unmarshal(c.Request, &req); err != nil {
		return []oc.Event{oc.Violation(EvReplayWrongOriginal, map[string]any{"error": "undecodable request: " + err.Error()})}
	}
	batch := req.GetUnsigned()
	key := batch.GetIdempotencyKey()
	var p replayParams
	if len(c.Params) > 0 {
		_ = json.Unmarshal(c.Params, &p) // params is a validated object; a missing wid is legal
	}

	orig, found := Original(c.T, key)
	d := map[string]any{"key": key, "wid": p.Wid}
	switch {
	case key == "" || !found:
		d["problem"] = "no original under the key"

		return []oc.Event{oc.Violation(EvReplayWrongOriginal, d)}
	case p.Wid != "" && p.Wid != orig.Wid:
		d["problem"] = "params.wid differs from the key's original"
		d["original"] = orig.Wid

		return []oc.Event{oc.Violation(EvReplayWrongOriginal, d)}
	case !oracle.RequestsEqual(orig.Requests, batch.GetRequests()):
		d["problem"] = "the replayed body differs from the original's"
		d["original"] = orig.Wid

		return []oc.Event{oc.Violation(EvReplayWrongOriginal, d)}
	}

	out := c.Outcome
	if out.Transient(true) || !out.Definitive() {
		return nil
	}
	d["original"] = orig.Wid
	d["ledgers"] = bulkLedgers(orig.Bulk)

	switch classify(orig) {
	case origCommitted:
		return validateCommittedReplay(c, orig, out, d)
	case origFrozen:
		frozen := orig.Outcome.Reason
		d["frozenReason"] = frozen
		if out.OK {
			return []oc.Event{oc.Violation(EvReplayRejectedCommitted, d)}
		}
		if out.Reason != frozen {
			d["replayReason"] = out.Reason
			d["replayCode"] = out.Code

			return []oc.Event{oc.Violation(EvReplayFrozenMismatch, d)}
		}

		return []oc.Event{
			oc.Reached(EvReplayExercised, map[string]any{"status": "failed"}),
			oc.Reached(EvReplayFrozenReturned, map[string]any{"reason": frozen}),
		}
	default:
		return nil
	}
}

func validateCommittedReplay(c *oc.ReadCtx, orig oc.WriteRecord, out oc.Outcome, d map[string]any) []oc.Event {
	if !out.OK {
		d["code"] = out.Code
		d["reason"] = out.Reason
		d["message"] = out.Message

		return []oc.Event{oc.Violation(EvReplayUnexpectedError, d)}
	}
	first, errA := oc.DecodeApplyResponse(orig.Outcome.Response)
	resp, errB := oc.DecodeApplyResponse(out.Response)
	if errA != nil || errB != nil {
		d["problem"] = "undecodable response"

		return []oc.Event{oc.Violation(EvReplayOutsideModel, d)}
	}
	originalSeqs := oc.LogSequences(first.GetLogs())
	replayed := oc.LogSequences(resp.GetLogs())
	if !oc.SequencesEqual(originalSeqs, replayed) {
		d["originalSeqs"] = originalSeqs
		d["replayedSeqs"] = replayed

		return []oc.Event{oc.Violation(EvReplayNewSequences, d)}
	}
	if c.Matches(func(base oracle.GlobalState) bool {
		res := base.Apply(orig.Bulk)

		return res.OK && oc.ReplayOrdersMatch(orig.Bulk, res.Orders, resp.GetLogs())
	}) {
		return []oc.Event{oc.Reached(EvReplayExercised, map[string]any{"status": "committed"})}
	}
	d["logSeqs"] = replayed
	d["maxTicket"] = c.MaxTicket

	return []oc.Event{oc.Violation(EvReplayOutsideModel, d)}
}

// bulkLedgers lists the ledgers a bulk names, in request order, deduplicated.
func bulkLedgers(b oracle.Bulk) []string {
	seen := map[string]bool{}
	out := []string{}
	for _, r := range b.Requests {
		l := safeLedgerOf(r)
		if l == "" || seen[l] {
			continue
		}
		seen[l] = true
		out = append(out, l)
	}

	return out
}

func safeLedgerOf(r *servicepb.Request) (l string) {
	defer func() {
		if recover() != nil {
			l = ""
		}
	}()

	return oracle.LedgerOf(r)
}
