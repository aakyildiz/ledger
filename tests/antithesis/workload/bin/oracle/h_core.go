package main

import (
	"github.com/antithesishq/antithesis-sdk-go/assert"

	oc "github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck"
)

// Core asserters (00a). One literal call site per event id (asserter_test.go).
// Every model-divergence finding is Unreachable; the two must-hit properties
// are the Reachable "model outcome verified" (reached by every drained or validated write) and
// the Sometimes on /write/late (needs faults).

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
}
