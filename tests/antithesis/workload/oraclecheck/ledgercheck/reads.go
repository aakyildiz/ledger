package ledgercheck

import (
	"encoding/json"
	"strings"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	oc "github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck/match"
	"github.com/formancehq/ledger/v3/tests/oracle"
)

var unmarshal = protojson.UnmarshalOptions{DiscardUnknown: true}

func decode(raw json.RawMessage, m proto.Message) error {
	if len(raw) == 0 {
		raw = json.RawMessage("{}")
	}

	return unmarshal.Unmarshal(raw, m)
}

// AbsentPrefix is the name prefix of the never-created ledgers the driver
// probes (DESIGN §3: o-<run>-absent-<draw>).
func AbsentPrefix(trackedPrefix string) string { return trackedPrefix + "absent-" }

// isAbsent recomputes the generator's intent from the request (DESIGN §4.7):
// an absent probe is named under the absent prefix and was never created.
func isAbsent(c *oc.ReadCtx, ledger string) bool {
	prefix := c.T.Config().TrackedPrefix

	return prefix != "" && strings.HasPrefix(ledger, AbsentPrefix(prefix))
}

func rawOrNull(b json.RawMessage) json.RawMessage {
	if len(b) == 0 {
		return json.RawMessage("null")
	}

	return b
}

// ---- GetLedger ----

// ValidateGetLedger checks one linearizable GetLedger answer (M/reads.go
// runLedgerRead + M/validate.go validateLedgerRead): a served snapshot must
// be the chart, ledger metadata and default enforcement mode of one candidate
// base where the ledger is live; an absent probe must answer NotFound; a
// NotFound on a tracked name must be explained by some serialization.
func ValidateGetLedger(c *oc.ReadCtx) []oc.Event {
	var req servicepb.GetLedgerRequest
	if err := decode(c.Request, &req); err != nil {
		return []oc.Event{oc.Violation(EvLcGetLedgerUnexpected, map[string]any{"error": "undecodable request: " + err.Error()})}
	}
	ledger := req.GetLedger()
	absent := isAbsent(c, ledger)

	switch {
	case c.Outcome.OK:
		if absent {
			return []oc.Event{oc.Violation(EvLcGetLedgerServedAbsent, map[string]any{"ledger": ledger})}
		}
		info := &commonpb.LedgerInfo{}
		if err := decode(c.Outcome.Response, info); err != nil {
			return []oc.Event{oc.Violation(EvLcLedgerReadOutsideModel, map[string]any{"ledger": ledger, "error": "undecodable response: " + err.Error()})}
		}
		if c.Matches(func(base oracle.GlobalState) bool {
			lc, exists := base.Lifecycle(ledger)
			if !exists || lc.Deleted {
				return false
			}
			ls := base.Ledger(ledger)

			return match.ChartMatches(ls, info.GetAccountTypes()) &&
				match.LedgerMetaMatches(ls, info.GetMetadata()) &&
				ls.DefaultEnforcementMode() == info.GetDefaultEnforcementMode()
		}) {
			return nil
		}

		return []oc.Event{oc.Violation(EvLcLedgerReadOutsideModel, map[string]any{
			"ledger":      ledger,
			"maxTicket":   c.MaxTicket,
			"serverTypes": len(info.GetAccountTypes()),
			"serverMode":  info.GetDefaultEnforcementMode().String(),
			"response":    rawOrNull(c.Outcome.Response),
			"modelLedger": modelLedgerDump(c.T.Model(), ledger),
		})}
	case c.Outcome.IsNotFound():
		var evs []oc.Event
		if absent {
			evs = append(evs, oc.Reached(EvLcGetLedgerAbsentProbed, map[string]any{"ledger": ledger}))
		}

		return append(evs, c.LedgerNotFoundExplained(ledger)...)
	default:
		return []oc.Event{oc.Violation(EvLcGetLedgerUnexpected, map[string]any{
			"ledger": ledger, "absent": absent, "code": c.Outcome.Code, "reason": c.Outcome.Reason, "message": c.Outcome.Message,
		})}
	}
}

// ---- GetMetadataSchemaStatus ----

// ValidateSchemaStatus checks one linearizable GetMetadataSchemaStatus
// answer (M/reads.go runSchemaRead + M/validate.go validateSchemaRead): the
// declared field types of every target must equal one candidate base's.
func ValidateSchemaStatus(c *oc.ReadCtx) []oc.Event {
	var req servicepb.GetMetadataSchemaStatusRequest
	if err := decode(c.Request, &req); err != nil {
		return []oc.Event{oc.Violation(EvLcSchemaUnexpected, map[string]any{"error": "undecodable request: " + err.Error()})}
	}
	ledger := req.GetLedger()
	absent := isAbsent(c, ledger)

	switch {
	case c.Outcome.OK:
		if absent {
			return []oc.Event{oc.Violation(EvLcSchemaServedAbsent, map[string]any{"ledger": ledger})}
		}
		resp := &servicepb.GetMetadataSchemaStatusResponse{}
		if err := decode(c.Outcome.Response, resp); err != nil {
			return []oc.Event{oc.Violation(EvLcSchemaReadOutsideModel, map[string]any{"ledger": ledger, "error": "undecodable response: " + err.Error()})}
		}
		if c.Matches(func(base oracle.GlobalState) bool {
			lc, exists := base.Lifecycle(ledger)
			if !exists || lc.Deleted {
				return false
			}
			ls := base.Ledger(ledger)

			return fieldTypesMatch(ls.AccountFieldTypes(), resp.GetAccountFields()) &&
				fieldTypesMatch(ls.TransactionFieldTypes(), resp.GetTransactionFields()) &&
				fieldTypesMatch(ls.LedgerFieldTypes(), resp.GetLedgerFields())
		}) {
			return nil
		}

		return []oc.Event{oc.Violation(EvLcSchemaReadOutsideModel, map[string]any{
			"ledger":       ledger,
			"maxTicket":    c.MaxTicket,
			"serverAcct":   len(resp.GetAccountFields()),
			"serverTxn":    len(resp.GetTransactionFields()),
			"serverLedger": len(resp.GetLedgerFields()),
			"response":     rawOrNull(c.Outcome.Response),
			"modelLedger":  modelLedgerDump(c.T.Model(), ledger),
		})}
	case c.Outcome.IsNotFound():
		var evs []oc.Event
		if absent {
			evs = append(evs, oc.Reached(EvLcSchemaAbsentProbed, map[string]any{"ledger": ledger}))
		}

		return append(evs, c.LedgerNotFoundExplained(ledger)...)
	default:
		return []oc.Event{oc.Violation(EvLcSchemaUnexpected, map[string]any{
			"ledger": ledger, "absent": absent, "code": c.Outcome.Code, "reason": c.Outcome.Reason, "message": c.Outcome.Message,
		})}
	}
}

// fieldTypesMatch reports whether the model's declared field types equal the
// server's for one target — same keys, same declared type (M/validate.go).
func fieldTypesMatch(model oracle.Map[string, commonpb.MetadataType], server map[string]*servicepb.MetadataFieldStatus) bool {
	if model.Len() != len(server) {
		return false
	}
	for k, t := range model.All() {
		st, ok := server[k]
		if !ok || st.GetDeclaredType() != t {
			return false
		}
	}

	return true
}

// modelLedgerDump renders the drained model's view of a ledger for a
// finding's diagnostics.
func modelLedgerDump(gs oracle.GlobalState, ledger string) map[string]any {
	lc, exists := gs.Lifecycle(ledger)
	if !exists {
		return map[string]any{"exists": false}
	}
	ls := gs.Ledger(ledger)
	types := map[string]string{}
	for name, t := range ls.Types().All() {
		types[name] = t.Pattern + "/" + t.Persistence.String()
	}
	meta := map[string]string{}
	for k, v := range ls.LedgerMeta().All() {
		meta[k] = oracle.MetaValueString(v)
	}
	schema := map[string]string{}
	for k, t := range ls.AccountFieldTypes().All() {
		schema["account:"+k] = t.String()
	}
	for k, t := range ls.TransactionFieldTypes().All() {
		schema["transaction:"+k] = t.String()
	}
	for k, t := range ls.LedgerFieldTypes().All() {
		schema["ledger:"+k] = t.String()
	}

	return map[string]any{
		"exists": true, "deleted": lc.Deleted, "mode": lc.Mode.String(),
		"enforcement": ls.DefaultEnforcementMode().String(),
		"types":       types, "meta": meta, "schema": schema,
	}
}
