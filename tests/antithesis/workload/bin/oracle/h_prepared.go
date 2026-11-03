package main

import (
	"github.com/antithesishq/antithesis-sdk-go/assert"

	"github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck/preparedcheck"
)

// Task 08 asserters (prepared queries). One literal call site per event id
// (asserter_test.go). Model divergences are Unreachable; the coverage events
// are Reachable and reached by the prepared_write, prepared_execute and
// projection_probe rules (tests/antithesis/hegel/porting/08-prepared-queries.md).
func init() {
	registerAsserter(preparedcheck.EvListingOutsideModel, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: prepared query listing outside model", d)
	})
	registerAsserter(preparedcheck.EvPageOutsideModel, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: prepared query page outside model", d)
	})
	registerAsserter(preparedcheck.EvAggregateOutsideModel, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: prepared query aggregate outside model", d)
	})
	registerAsserter(preparedcheck.EvExecUnexpectedError, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: prepared query execution returned unexpected error", d)
	})
	registerAsserter(preparedcheck.EvCursorPageSizeMismatch, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: prepared query cursor page size mismatch", d)
	})
	registerAsserter(preparedcheck.EvReverseAggregateNotRejected, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: reverse aggregate on a prepared query not rejected", d)
	})
	registerAsserter(preparedcheck.EvListUnexpectedError, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: ListPreparedQueries returned unexpected error", d)
	})
	registerAsserter(preparedcheck.EvReverseAggregateRejected, func(_ bool, d map[string]any) {
		assert.Reachable("oracle: reverse aggregate on a prepared query rejected", d)
	})
	registerAsserter(preparedcheck.EvAggregateTargetRejected, func(_ bool, d map[string]any) {
		assert.Reachable("oracle: aggregate on a non-accounts prepared query rejected", d)
	})
	registerAsserter(preparedcheck.EvPageResumedOpposite, func(_ bool, d map[string]any) {
		assert.Reachable("oracle: prepared query page resumed in the opposite direction", d)
	})
	registerAsserter(preparedcheck.EvPageValidated, func(_ bool, d map[string]any) {
		assert.Reachable("oracle: prepared query page validated", d)
	})
	registerAsserter(preparedcheck.EvAggregateValidated, func(_ bool, d map[string]any) {
		assert.Reachable("oracle: prepared query aggregate validated", d)
	})
	registerAsserter(preparedcheck.EvMalformedFilterRefused, func(_ bool, d map[string]any) {
		assert.Reachable("oracle: malformed prepared-query filter refused at save", d)
	})
	registerAsserter(preparedcheck.EvUpdateInvalidForStoredTarget, func(_ bool, d map[string]any) {
		assert.Reachable("oracle: an update with a filter invalid for the stored target is refused", d)
	})
}
