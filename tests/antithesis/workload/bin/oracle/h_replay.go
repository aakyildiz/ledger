package main

import (
	"github.com/antithesishq/antithesis-sdk-go/assert"

	"github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck/replaycheck"
)

// Task 03 asserters (reverts, metadata, idempotency replays and conflicts).
// The replaycheck package registers the Replay read kind, the conflict
// failure hook, the skip coverage probes (Raw, no asserter) and the
// revertSeek view from its init().
func init() {
	registerAsserter(replaycheck.EvReplayNewSequences, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: idempotency replay produced new log sequences", d)
	})
	registerAsserter(replaycheck.EvReplayOutsideModel, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: idempotency replay outside model", d)
	})
	registerAsserter(replaycheck.EvReplayUnexpectedError, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: idempotency replay returned unexpected error", d)
	})
	registerAsserter(replaycheck.EvReplayExercised, func(_ bool, d map[string]any) {
		assert.Reachable("oracle: idempotency replay exercised", d)
	})
	registerAsserter(replaycheck.EvReplayFrozenMismatch, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: a replayed rejected key returns its frozen failure", d)
	})
	registerAsserter(replaycheck.EvReplayRejectedCommitted, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: a rejected write stays rejected when its idempotency key is replayed", d)
	})
	registerAsserter(replaycheck.EvReplayFrozenReturned, func(_ bool, d map[string]any) {
		assert.Reachable("oracle: a replay returned a frozen failure", d)
	})
	registerAsserter(replaycheck.EvReplayWrongOriginal, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle protocol: replay names a different original", d)
	})
	registerAsserter(replaycheck.EvConflictRejection, func(_ bool, d map[string]any) {
		assert.Reachable("oracle: idempotency-conflict rejection exercised", d)
	})
}
