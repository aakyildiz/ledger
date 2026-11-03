package indexcheck

import (
	"fmt"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	oc "github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck/match"
	"github.com/formancehq/ledger/v3/tests/oracle"
)

// ValidateInspectIndex validates one InspectIndex page (M/index_inspect.go).
// A request carrying a cursor is the one resume through the opaque cursor the
// driver takes on a second ticket; it is held to the same page contract under
// the "resumed" literals. Every mode is judged on its page contract; SUMMARY
// on the live index (no checkpoint, no cursor) is held to the model's fold of
// the key under each base's declared type, and a base with the key's retype
// window open accepts any summary.
func ValidateInspectIndex(c *oc.ReadCtx) []oc.Event {
	var req servicepb.InspectIndexRequest
	if err := unmarshal.Unmarshal(c.Request, &req); err != nil {
		return nil
	}
	ledger, key, mode := req.GetLedger(), req.GetMetadataKey(), req.GetMode()
	resume := req.GetCursor() != ""
	checkpoint := req.GetCheckpointId()
	absent := isAbsentProbe(c, ledger)
	target := commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS
	if req.GetTargetType() == commonpb.TargetType_TARGET_TYPE_TRANSACTION {
		target = commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS
	}
	pageSize := match.EffectivePageSize(int(req.GetPageSize()))
	details := map[string]any{"ledger": ledger, "key": key, "target": req.GetTargetType().String(), "mode": mode.String(),
		"pageSize": pageSize, "checkpoint": checkpoint, "resume": resume}

	if !c.Outcome.OK {
		rpcErr := match.RPCErrorFrom(c.Outcome.Code, c.Outcome.Reason, c.Outcome.Metadata)
		gated := match.IsIndexNotReady(rpcErr) || match.IsIndexNotFound(rpcErr)
		if resume {
			if gated || c.Outcome.IsNotFound() {
				return nil
			}

			return []oc.Event{oc.Violation(EvResumeError, errDetails(c, details))}
		}
		switch {
		case checkpoint != 0 && c.Outcome.IsNotFound():
			return nil // a checkpoint the fleet dropped cannot answer
		case c.Outcome.IsNotFound() && absent:
			return []oc.Event{oc.Reached(EvInspectAbsentNotFound, map[string]any{"ledger": ledger})}
		case c.Outcome.IsNotFound():
			return c.LedgerNotFoundExplained(ledger)
		case gated:
			return []oc.Event{oc.Reached(EvInspectGated, map[string]any{"ledger": ledger, "reason": c.Outcome.Reason})}
		default:
			return []oc.Event{oc.Violation(EvInspectError, errDetails(c, details))}
		}
	}
	if absent {
		return []oc.Event{oc.Violation(EvInspectOutsideFleet, map[string]any{"ledger": ledger})}
	}
	res := &servicepb.InspectIndexResponse{}
	if err := unmarshal.Unmarshal(c.Outcome.Response, res); err != nil {
		details["why"] = "undecodable response: " + err.Error()
		if resume {
			return []oc.Event{oc.Violation(EvResumeContract, details)}
		}

		return []oc.Event{oc.Violation(EvInspectContract, details)}
	}
	if mode == servicepb.InspectIndexMode_INSPECT_INDEX_MODE_SUMMARY && !resume && res.GetSummary() == nil {
		// The catalogued literal for this arm, ahead of the generic page
		// contract (which would otherwise report it first).
		return []oc.Event{oc.Violation(EvSummaryOtherArm, details)}
	}
	if why := match.InspectPageViolation(res, mode, pageSize); why != "" {
		details["why"] = why
		if resume {
			return []oc.Event{oc.Violation(EvResumeContract, details)}
		}

		return []oc.Event{oc.Violation(EvInspectContract, details)}
	}
	if resume {
		return []oc.Event{oc.Reached(EvInspectResumed, map[string]any{"mode": mode.String()})}
	}

	var evs []oc.Event
	if mode == servicepb.InspectIndexMode_INSPECT_INDEX_MODE_SUMMARY && checkpoint == 0 {
		sevs, ok := validateSummary(c, ledger, target, key, res.GetSummary(), details)
		evs = append(evs, sevs...)
		if !ok {
			return evs
		}
	}
	evs = append(evs, oc.Reached(EvInspectValidated, map[string]any{"mode": mode.String()}))
	if checkpoint != 0 {
		evs = append(evs, oc.Reached(EvInspectCheckpoint, map[string]any{"checkpoint": checkpoint}))
	}

	return evs
}

// validateSummary holds a summary's three counters to the model
// (M/index_inspect.go validateInspectSummary). The fallback declared type,
// for a base that does not declare the key, is the one the read's snapshot
// declared (the generator's view when it picked the key).
func validateSummary(c *oc.ReadCtx, ledger string, target commonpb.QueryTarget, key string, summary *servicepb.InspectSummary, details map[string]any) ([]oc.Event, bool) {
	if summary == nil {
		return []oc.Event{oc.Violation(EvSummaryOtherArm, details)}, false
	}
	served := match.InspectCounts{
		Cardinality:      summary.GetCardinality(),
		EntitiesWithKey:  summary.GetEntitiesWithKey(),
		EntitiesWithNull: summary.GetEntitiesWithNull(),
	}
	declared := commonpb.MetadataType_METADATA_TYPE_STRING
	if t, ok := match.DeclaredFieldTypes(c.Snapshot.Ledger(ledger), target).Get(key); ok {
		declared = t
	}
	canonical := match.MetadataCanonical(target, key)

	compared := false
	if c.Matches(func(base oracle.GlobalState) bool {
		ls, live := match.LiveLedgerState(base, ledger)
		if !live {
			return false
		}
		if _, open := ls.RetypeWindow(canonical); open {
			compared = false

			return true
		}
		baseDeclared := declared
		if t, ok := match.DeclaredFieldTypes(ls, target).Get(key); ok {
			baseDeclared = t
		}
		compared = match.ModelInspectCounts(ls, target, key, baseDeclared) == served

		return compared
	}) {
		if !compared {
			return nil, true
		}
		evs := []oc.Event{oc.Reached(EvSummaryMatched, map[string]any{"cardinality": served.Cardinality})}
		if served.Cardinality > 0 {
			evs = append(evs, oc.Reached(EvSummaryPopulated, map[string]any{"cardinality": served.Cardinality,
				"withKey": served.EntitiesWithKey, "withNull": served.EntitiesWithNull}))
		}
		if served.EntitiesWithNull > 0 {
			evs = append(evs, oc.Reached(EvSummaryUnconvertible, map[string]any{"withNull": served.EntitiesWithNull}))
		}

		return evs, true
	}
	model := match.ModelInspectCounts(c.T.Model().Ledger(ledger), target, key, declared)
	details["servedCardinality"] = served.Cardinality
	details["servedWithKey"] = served.EntitiesWithKey
	details["servedWithNull"] = served.EntitiesWithNull
	details["modelCounts"] = fmt.Sprintf("cardinality=%d withKey=%d withNull=%d", model.Cardinality, model.EntitiesWithKey, model.EntitiesWithNull)
	details["maxTicket"] = c.MaxTicket

	return []oc.Event{oc.Violation(EvInspectSummaryOutsideModel, details)}, false
}
