package pointcheck

import (
	"encoding/json"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	oc "github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck/match"
	"github.com/formancehq/ledger/v3/tests/oracle"
)

// Read kind names (DESIGN §5.3).
const (
	KindGetAccount       = "GetAccount"
	KindGetTransaction   = "GetTransaction"
	KindAggregateVolumes = "AggregateVolumes"
	KindGetLedgerStats   = "GetLedgerStats"
	KindGetLog           = "GetLog"
	ViewLogSeqs          = "logSeqs"
)

func init() {
	oc.RegisterReadKind(oc.ReadKind{Name: KindGetAccount, Validate: ValidateGetAccount})
	oc.RegisterReadKind(oc.ReadKind{Name: KindGetTransaction, Validate: ValidateGetTransaction})
	oc.RegisterReadKind(oc.ReadKind{Name: KindAggregateVolumes, Validate: ValidateAggregateVolumes})
	oc.RegisterReadKind(oc.ReadKind{Name: KindGetLedgerStats, Validate: ValidateGetLedgerStats})
	oc.RegisterReadKind(oc.ReadKind{Name: KindGetLog, Validate: ValidateGetLog})
	oc.RegisterView(ViewLogSeqs, viewLogSeqs)
}

var unmarshal = protojson.UnmarshalOptions{DiscardUnknown: true}

// decode reads protojson b into m; an empty body is the zero message.
func decode(b []byte, m proto.Message) error {
	if len(b) == 0 {
		return nil
	}

	return unmarshal.Unmarshal(b, m)
}

// rawOrNull keeps a response for diagnostics.
func rawOrNull(b []byte) json.RawMessage {
	if len(b) == 0 {
		return json.RawMessage("null")
	}

	return json.RawMessage(b)
}

// absentEverywhere reports whether no candidate base knows ledger at all —
// the absent-ledger probe, recomputed from the snapshot and the candidates
// rather than read from the hint. A ledger created by an in-flight bulk is
// known to the candidate that folds it, so it is not absent.
func absentEverywhere(c *oc.ReadCtx, ledger string) bool {
	return !c.Matches(func(base oracle.GlobalState) bool {
		_, exists := base.Lifecycle(ledger)

		return exists
	})
}

// liveSomewhere reports whether some candidate base holds ledger live.
func liveSomewhere(c *oc.ReadCtx, ledger string) bool {
	return c.Matches(func(base oracle.GlobalState) bool {
		return match.LedgerIsLive(base, ledger)
	})
}

// rpcError is the classification view of a non-OK outcome.
func rpcError(o oc.Outcome) match.RPCError {
	if o.OK {
		return match.NoError
	}

	return match.RPCErrorFrom(o.Code, o.Reason, o.Metadata)
}

func outcomeDetails(o oc.Outcome) map[string]any {
	return map[string]any{"code": o.Code, "reason": o.Reason, "message": o.Message}
}
