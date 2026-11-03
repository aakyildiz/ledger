package checkpointcheck

import (
	"slices"

	"github.com/formancehq/ledger/v3/internal/proto/clusterpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	oc "github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck/match"
	"github.com/formancehq/ledger/v3/tests/oracle"
)

// ValidatePredictedCheckpointRead checks the GetLedger{read:{checkpointId}}
// that races the create assigned that id (M/checkpoint_reads.go
// runPredictedCheckpointRead). NotFound is legal while some candidate has not
// committed the checkpoint; a success must be the ledger info of the state
// immediately before the create transition in some legal serialization
// (Tracker.CheckpointCreationMatches).
//
// Loosened against M/: a NotFound is also legal when the ledger was not live
// in the state the create froze (a concurrent DeleteLedger serialized before
// the create), or when some candidate base has deleted the ledger since (no
// frozen checkpoint keeps serving a deleted ledger,
// match.CheckpointLedgerReadOutcomeMatches); M/ reported both.
func ValidatePredictedCheckpointRead(c *oc.ReadCtx) []oc.Event {
	var req servicepb.GetLedgerRequest
	if err := decode(c.Request, &req); err != nil || req.GetRead().GetCheckpointId() == 0 {
		return []oc.Event{oc.Violation(EvProtocolMalformedRead, map[string]any{"kind": KindPredictedCheckpointRead, "request": rawOrNull(c.Request)})}
	}
	id, ledger := req.GetRead().GetCheckpointId(), req.GetLedger()
	d := map[string]any{"checkpoint": id, "ledger": ledger, "maxTicket": c.MaxTicket}
	if !c.Outcome.OK {
		err := rpcError(c.Outcome)
		if match.CheckpointNotFound(err) {
			if match.CheckpointLedgerReadOutcomeMatches(id, ledger, false, err, c.Matches) {
				return nil
			}
			if c.T.CheckpointCreationMatches(c.MaxTicket, id, func(before oracle.GlobalState) bool {
				return !match.LedgerIsLive(before, ledger)
			}) {
				return nil
			}
		}
		d["code"], d["reason"], d["message"] = c.Outcome.Code, c.Outcome.Reason, c.Outcome.Message

		return []oc.Event{oc.Violation(EvPredictedUnexpectedError, d)}
	}
	info := &commonpb.LedgerInfo{}
	if err := decode(c.Outcome.Response, info); err == nil && c.T.CheckpointCreationMatches(c.MaxTicket, id, func(before oracle.GlobalState) bool {
		return match.PredictedCheckpointLedgerMatches(before, ledger, info)
	}) {
		return nil
	}
	d["returned"] = rawOrNull(c.Outcome.Response)

	return []oc.Event{oc.Violation(EvPredictedOutsideCreation, d)}
}

// ValidateCheckpointRegistry checks a fenced ListQueryCheckpoints on a pinned
// node (M/checkpoint_reads.go runCheckpointListRead): every id whose creation
// the oracle captured reports the captured max_sequence, and the sorted id
// list equals some candidate base's live registry. The fence itself ran in
// the driver; its record in the hint is coverage only.
func ValidateCheckpointRegistry(c *oc.ReadCtx) []oc.Event {
	if !c.Outcome.OK {
		return []oc.Event{oc.Violation(EvListUnexpectedError, map[string]any{"code": c.Outcome.Code, "reason": c.Outcome.Reason, "message": c.Outcome.Message, "node": c.Route.Node})}
	}
	resp := &clusterpb.ListQueryCheckpointsResponse{}
	if err := decode(c.Outcome.Response, resp); err != nil {
		return []oc.Event{oc.Violation(EvListOutsideModel, map[string]any{"error": err.Error(), "returned": rawOrNull(c.Outcome.Response)})}
	}
	ids := make([]uint64, 0, len(resp.GetCheckpoints()))
	for _, cp := range resp.GetCheckpoints() {
		id := cp.GetCheckpointId()
		ids = append(ids, id)
		if seq, known := c.T.FrozenCheckpointMaxSequence(id); known && cp.GetMaxSequence() != seq {
			return []oc.Event{oc.Violation(EvListSequenceOutside, map[string]any{"checkpoint": id, "expected": seq, "actual": cp.GetMaxSequence(), "node": c.Route.Node})}
		}
	}
	slices.Sort(ids)
	if !c.Matches(func(base oracle.GlobalState) bool { return slices.Equal(ids, base.QueryCheckpointIDs()) }) {
		return []oc.Event{oc.Violation(EvListOutsideModel, map[string]any{"ids": ids, "model": c.T.Model().QueryCheckpointIDs(), "node": c.Route.Node, "maxTicket": c.MaxTicket})}
	}

	return coverage(CovListing, map[string]any{"ids": len(ids)})
}

// ValidateCheckpointSchedule checks a fenced GetQueryCheckpointSchedule
// (M/checkpoint_reads.go runCheckpointScheduleRead): the cron equals some
// candidate base's schedule.
func ValidateCheckpointSchedule(c *oc.ReadCtx) []oc.Event {
	if !c.Outcome.OK {
		return []oc.Event{oc.Violation(EvScheduleUnexpectedError, map[string]any{"code": c.Outcome.Code, "reason": c.Outcome.Reason, "message": c.Outcome.Message, "node": c.Route.Node})}
	}
	resp := &clusterpb.GetQueryCheckpointScheduleResponse{}
	if err := decode(c.Outcome.Response, resp); err != nil {
		return []oc.Event{oc.Violation(EvScheduleOutsideModel, map[string]any{"error": err.Error(), "returned": rawOrNull(c.Outcome.Response)})}
	}
	if !c.Matches(func(base oracle.GlobalState) bool { return resp.GetCron() == base.QueryCheckpointSchedule() }) {
		return []oc.Event{oc.Violation(EvScheduleOutsideModel, map[string]any{"cron": resp.GetCron(), "model": c.T.Model().QueryCheckpointSchedule(), "node": c.Route.Node, "maxTicket": c.MaxTicket})}
	}

	return coverage(CovScheduleRead, map[string]any{"cron": resp.GetCron()})
}

// ValidateCheckpointInfo checks a fenced GetQueryCheckpointInfo on a pinned
// node (MD/parallel_driver_query_checkpoints/main.go:127-137, now against the
// model): a success names the requested id, reports the max sequence the
// oracle captured at its creation, and the id is live in some candidate base;
// NotFound is legal only when some candidate base lacks the id.
func ValidateCheckpointInfo(c *oc.ReadCtx) []oc.Event {
	var req clusterpb.GetQueryCheckpointInfoRequest
	if err := decode(c.Request, &req); err != nil || req.GetCheckpointId() == 0 {
		return []oc.Event{oc.Violation(EvProtocolMalformedRead, map[string]any{"kind": KindCheckpointInfo, "request": rawOrNull(c.Request)})}
	}
	id := req.GetCheckpointId()
	d := map[string]any{"checkpoint": id, "node": c.Route.Node, "maxTicket": c.MaxTicket, "modelIds": c.T.Model().QueryCheckpointIDs()}
	if !c.Outcome.OK {
		err := rpcError(c.Outcome)
		if match.CheckpointReadOutcomeMatches(id, false, err, c.Matches) {
			return nil
		}
		d["code"], d["reason"], d["message"] = c.Outcome.Code, c.Outcome.Reason, c.Outcome.Message
		if match.CheckpointNotFound(err) {
			return []oc.Event{oc.Violation(EvInfoOutsideModel, d)}
		}

		return []oc.Event{oc.Violation(EvInfoUnexpectedError, d)}
	}
	info := &clusterpb.QueryCheckpointInfo{}
	if err := decode(c.Outcome.Response, info); err != nil {
		d["error"], d["returned"] = err.Error(), rawOrNull(c.Outcome.Response)

		return []oc.Event{oc.Violation(EvInfoOutsideModel, d)}
	}
	seq, known := c.T.FrozenCheckpointMaxSequence(id)
	d["returnedId"], d["returnedMaxSequence"], d["expectedMaxSequence"] = info.GetCheckpointId(), info.GetMaxSequence(), seq
	switch {
	case info.GetCheckpointId() != id:
		d["why"] = "info names another checkpoint"
	case known && info.GetMaxSequence() != seq:
		d["why"] = "max sequence differs from the captured creation"
	case !c.Matches(func(base oracle.GlobalState) bool { return base.QueryCheckpointExists(id) }):
		d["why"] = "no candidate base holds the checkpoint"
	default:
		return []oc.Event{oc.Reached(EvInfoMatched, map[string]any{"checkpoint": id, "maxSequence": seq})}
	}

	return []oc.Event{oc.Violation(EvInfoOutsideModel, d)}
}
