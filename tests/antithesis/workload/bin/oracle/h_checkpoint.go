package main

import (
	"github.com/antithesishq/antithesis-sdk-go/assert"

	"github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck/checkpointcheck"
)

// Task 09 asserters (query checkpoints). One literal call site per event id
// (asserter_test.go). Model divergences are Unreachable; the frozen-fleet
// listing coverage is Reachable and reached by checkpoint_read's ListLedgers
// arm, the info coverage by checkpoint_registry's info arm. A malformed
// checkpoint read request is a driver protocol error, not a ledger finding. The nine "oracle: coverage checkpoint …" probes are Raw events
// (oraclecheck.RegisterCoverage), emitted by the registry without asserters
// (tests/antithesis/hegel/porting/09-query-checkpoints.md).
func init() {
	registerAsserter(checkpointcheck.EvReadOutsideFrozen, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: checkpoint read outside frozen model", d)
	})
	registerAsserter(checkpointcheck.EvPredictedOutsideCreation, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: predicted checkpoint read outside creation model", d)
	})
	registerAsserter(checkpointcheck.EvPredictedUnexpectedError, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: predicted checkpoint read returned unexpected error", d)
	})
	registerAsserter(checkpointcheck.EvListOutsideModel, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: checkpoint list outside model", d)
	})
	registerAsserter(checkpointcheck.EvListSequenceOutside, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: checkpoint list sequence outside captured model", d)
	})
	registerAsserter(checkpointcheck.EvListUnexpectedError, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: checkpoint list returned unexpected error", d)
	})
	registerAsserter(checkpointcheck.EvScheduleOutsideModel, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: checkpoint schedule outside model", d)
	})
	registerAsserter(checkpointcheck.EvScheduleUnexpectedError, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: checkpoint schedule returned unexpected error", d)
	})
	registerAsserter(checkpointcheck.EvFrozenFleetListing, func(_ bool, d map[string]any) {
		assert.Reachable("oracle: coverage a checkpoint served the frozen fleet listing", d)
	})
	registerAsserter(checkpointcheck.EvInfoOutsideModel, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: checkpoint info outside model", d)
	})
	registerAsserter(checkpointcheck.EvInfoUnexpectedError, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: checkpoint info returned unexpected error", d)
	})
	registerAsserter(checkpointcheck.EvInfoMatched, func(_ bool, d map[string]any) {
		assert.Reachable("oracle: coverage checkpoint info matched the oracle", d)
	})
	registerAsserter(checkpointcheck.EvProtocolMalformedRead, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle protocol: checkpoint read request is malformed", d)
	})
}
