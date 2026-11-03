package main

import (
	"github.com/antithesishq/antithesis-sdk-go/assert"

	txcheck "github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck/applysurface"
)

// Task 02 (transaction writes) asserters. The rejection-coverage Reachables
// are reached by tx_bulk / tx_local_bulk / tx_boundary / tx_transient /
// tx_drain_probe / tx_reference_reuse; the apply-surface "and continued"
// probe is a Raw coverage id (registered by the library, no asserter).

func init() {
	registerAsserter(txcheck.EvInsufficientFundsExercised, func(_ bool, d map[string]any) {
		assert.Reachable("oracle: insufficient-funds rejection exercised", d)
	})
	registerAsserter(txcheck.EvReferenceConflictExercised, func(_ bool, d map[string]any) {
		assert.Reachable("oracle: reference-conflict rejection exercised", d)
	})
	registerAsserter(txcheck.EvVolumeOverflowExercised, func(_ bool, d map[string]any) {
		assert.Reachable("oracle: volume-overflow rejection exercised", d)
	})
	registerAsserter(txcheck.EvValidationExercised, func(_ bool, d map[string]any) {
		assert.Reachable("oracle: validation rejection exercised", d)
	})
	registerAsserter(txcheck.EvTransientNonZeroExercised, func(_ bool, d map[string]any) {
		assert.Reachable("oracle: transient non-zero rejection exercised", d)
	})
}
