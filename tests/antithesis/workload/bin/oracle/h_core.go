package main

import (
	"encoding/json"

	"github.com/antithesishq/antithesis-sdk-go/assert"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	oc "github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck"
	"github.com/formancehq/ledger/v3/tests/oracle"

	"google.golang.org/protobuf/encoding/protojson"
)

// Core asserters (00a). One literal call site per event id (asserter_test.go).
// Every model-divergence finding is Unreachable; the two must-hit properties
// are the Reachable "model outcome verified" (reached by the smoke rule) and
// the Sometimes on /write/late (needs faults).

// EvSmokeReadOutsideModel is the Smoke read kind's finding (task 99 deletes
// the kind).
const EvSmokeReadOutsideModel oc.EventID = "oracle: smoke account read outside model"

func init() {
	registerAsserter(oc.EvModelRejectsCommittedBulk, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: model rejects a server-committed bulk", d)
	})
	registerAsserter(oc.EvCheckpointCommitOutsideModel, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: checkpoint commit outside model", d)
	})
	registerAsserter(oc.EvCommittedLogCountMismatch, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: committed log count mismatch", d)
	})
	registerAsserter(oc.EvCommittedLogIdentityMismatch, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: committed ledger log identity mismatch", d)
	})
	registerAsserter(oc.EvSkippedOrderMismatch, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: skipped-order response mismatch", d)
	})
	registerAsserter(oc.EvEnforcementModeMismatch, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: enforcement-mode response mismatch", d)
	})
	registerAsserter(oc.EvLifecycleResponseMismatch, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: lifecycle response mismatch", d)
	})
	registerAsserter(oc.EvPostCommitVolumeMismatch, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: model post-commit volume mismatch", d)
	})
	registerAsserter(oc.EvResponseMetadataMismatch, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: response metadata value mismatch", d)
	})
	registerAsserter(oc.EvRevertTxIDMismatch, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: revert transaction id mismatch", d)
	})
	registerAsserter(oc.EvRevertedTxIDMismatch, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: reverted transaction id mismatch", d)
	})
	registerAsserter(oc.EvRevertPostingsMismatch, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: revert postings mismatch", d)
	})
	registerAsserter(oc.EvTxIDMismatch, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: transaction id mismatch", d)
	})
	registerAsserter(oc.EvTxReferenceMismatch, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: transaction reference mismatch", d)
	})
	registerAsserter(oc.EvTxPostingsMismatch, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: transaction postings mismatch", d)
	})
	registerAsserter(oc.EvTxAccountMetadataMismatch, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: transaction account-metadata mismatch", d)
	})
	registerAsserter(oc.EvDeleteMetadataMismatch, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: delete-metadata response mismatch", d)
	})
	registerAsserter(oc.EvChartResponseMismatch, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: chart response mismatch", d)
	})
	registerAsserter(oc.EvSetFieldTypeMismatch, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: set-field-type response mismatch", d)
	})
	registerAsserter(oc.EvRemoveFieldTypeMismatch, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: remove-field-type response mismatch", d)
	})
	registerAsserter(oc.EvRemoveFieldTypeDroppedMismatch, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: remove-field-type dropped-index mismatch", d)
	})
	registerAsserter(oc.EvRemoveFieldTypeDroppedWrong, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: remove-field-type dropped wrong index", d)
	})
	registerAsserter(oc.EvFailureUnexplained, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: bulk failure not explained by any serialization", d)
	})
	registerAsserter(oc.EvEmptyCommit, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: successful bulk returned no committed log", d)
	})
	registerAsserter(oc.EvOneKeyOneOutcome, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: one idempotency key yields one outcome across attempts", d)
	})
	registerAsserter(oc.EvLedgerNotFoundUnexplained, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: ledger-scoped read returned unexplained NotFound", d)
	})
	registerAsserter(oc.EvProtocolNonDefinitive, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle protocol: driver posted a non-definitive write outcome", d)
	})
	registerAsserter(oc.EvModelOutcomeVerified, func(_ bool, d map[string]any) {
		assert.Reachable("oracle: model outcome verified", d)
	})
	registerAsserter(oc.EvTwoDefinitiveOutcomes, func(cond bool, d map[string]any) {
		assert.Sometimes(cond, "oracle: two attempts of one key both returned definitive outcomes", d)
	})
	registerAsserter(EvSmokeReadOutsideModel, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: smoke account read outside model", d)
	})

	oc.RegisterReadKind(oc.ReadKind{Name: "Smoke", Validate: coreSmokeValidate})
}

// coreAssetColor is one (asset, color) bucket of a served account.
type coreAssetColor struct{ asset, color string }

// coreSmokeValidate checks a GetAccount answer: some candidate base must hold
// exactly the served volume set and metadata for the address (lifted from
// M/validate.go validateAccountRead / accountVolumesMatch / metadataMatches).
func coreSmokeValidate(c *oc.ReadCtx) []oc.Event {
	var req servicepb.GetAccountRequest
	if err := (protojson.UnmarshalOptions{DiscardUnknown: true}).Unmarshal(c.Request, &req); err != nil {
		return []oc.Event{oc.Violation(EvSmokeReadOutsideModel, map[string]any{"error": "undecodable request: " + err.Error()})}
	}
	ledger, addr, collapsed := req.GetLedger(), req.GetAddress(), req.GetCollapseColors()

	var served map[coreAssetColor]oracle.VolumePair
	var meta map[string]*commonpb.MetadataValue
	found, wellFormed := true, true
	switch {
	case c.Outcome.OK:
		acct := &commonpb.Account{}
		if err := (protojson.UnmarshalOptions{DiscardUnknown: true}).Unmarshal(c.Outcome.Response, acct); err != nil {
			wellFormed = false
		} else {
			served, wellFormed = coreAccountVolumeSet(acct)
			meta = acct.GetMetadata()
		}
	case c.Outcome.IsNotFound():
		found = false
	default:
		return nil // other errors are not judged by the smoke kind
	}

	if wellFormed && c.Matches(func(base oracle.GlobalState) bool {
		lc, exists := base.Lifecycle(ledger)
		if !exists || lc.Deleted {
			return !found
		}
		ls := base.Ledger(ledger)

		return coreAccountVolumesMatch(ls, addr, served, collapsed) && coreMetadataMatches(ls, addr, meta)
	}) {
		return []oc.Event{oc.Reached(oc.EvModelOutcomeVerified, map[string]any{})}
	}

	return []oc.Event{oc.Violation(EvSmokeReadOutsideModel, map[string]any{
		"ledger":     ledger,
		"address":    addr,
		"found":      found,
		"wellFormed": wellFormed,
		"maxTicket":  c.MaxTicket,
		"response":   json.RawMessage(nonEmptyJSON(c.Outcome.Response)),
	})}
}

func nonEmptyJSON(b []byte) []byte {
	if len(b) == 0 {
		return []byte("null")
	}

	return b
}

func coreAccountVolumeSet(acct *commonpb.Account) (map[coreAssetColor]oracle.VolumePair, bool) {
	out := make(map[coreAssetColor]oracle.VolumePair, len(acct.GetVolumes()))
	for _, entry := range acct.GetVolumes() {
		key := coreAssetColor{asset: entry.GetAsset(), color: entry.GetColor()}
		if _, dup := out[key]; dup {
			return nil, false
		}
		var vp oracle.VolumePair
		vols := entry.GetVolumes()
		if vols == nil || vols.GetInput() == nil || vols.GetOutput() == nil {
			return nil, false
		}
		if vp.Input.SetFromDecimal(vols.GetInput().DecimalString()) != nil ||
			vp.Output.SetFromDecimal(vols.GetOutput().DecimalString()) != nil {
			return nil, false
		}
		out[key] = vp
	}

	return out, true
}

func coreAccountVolumesMatch(ls oracle.LedgerState, addr string, got map[coreAssetColor]oracle.VolumePair, collapsed bool) bool {
	want := map[coreAssetColor]oracle.VolumePair{}
	for k, vp := range ls.Volumes().From(oracle.VolumeKey{Address: addr}) {
		if k.Address != addr {
			break
		}
		key := coreAssetColor{asset: k.Asset, color: k.Color}
		if collapsed {
			key = coreAssetColor{asset: k.Asset}
		}
		acc := want[key]
		acc.Input.Add(&acc.Input, &vp.Input)
		acc.Output.Add(&acc.Output, &vp.Output)
		want[key] = acc
	}
	if len(want) != len(got) {
		return false
	}
	for key, vp := range want {
		g, ok := got[key]
		if !ok || g.Input.Cmp(&vp.Input) != 0 || g.Output.Cmp(&vp.Output) != 0 {
			return false
		}
	}

	return true
}

func coreMetadataMatches(ls oracle.LedgerState, addr string, served map[string]*commonpb.MetadataValue) bool {
	model := ls.AccountMetadata(addr)
	if len(model) != len(served) {
		return false
	}
	for k, v := range model {
		sv, ok := served[k]
		if !ok || oracle.MetaValueString(sv) != oracle.MetaValueString(v) {
			return false
		}
	}

	return true
}
