// Package replaycheck is task 03's oracle family: the `Replay` read kind
// (idempotency replays of flagged originals, committed or frozen), the
// idempotency-conflict failure hook, the skip-and-continue coverage probes of
// reverts and metadata deletes, and the `revertSeek` aiming view.
//
// Like every oraclecheck family it is assertion-free: validators and hooks
// return oraclecheck events; bin/oracle/h_replay.go asserts their literals.
package replaycheck

import oc "github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck"

// Event ids (DESIGN §9.1). Values are the literal assertion messages.
const (
	// Ported from M/replay.go validateReplay (original committed).
	EvReplayNewSequences    oc.EventID = "oracle: idempotency replay produced new log sequences"
	EvReplayOutsideModel    oc.EventID = "oracle: idempotency replay outside model"
	EvReplayUnexpectedError oc.EventID = "oracle: idempotency replay returned unexpected error"
	// Must-hit (Reachable), reached by idem_replay.
	EvReplayExercised oc.EventID = "oracle: idempotency replay exercised"

	// New (task 01 freezing): replays of frozen failures.
	EvReplayFrozenMismatch    oc.EventID = "oracle: a replayed rejected key returns its frozen failure"
	EvReplayRejectedCommitted oc.EventID = "oracle: a rejected write stays rejected when its idempotency key is replayed"
	// Must-hit (Reachable), reached by idem_replay over meta_bulk's frozen failures.
	EvReplayFrozenReturned oc.EventID = "oracle: a replay returned a frozen failure"

	// Driver bug: the replay's key and its params.wid disagree.
	EvReplayWrongOriginal oc.EventID = "oracle protocol: replay names a different original"

	// Failure hook (M/validate.go:561), must-hit, reached by idem_conflict.
	EvConflictRejection oc.EventID = "oracle: idempotency-conflict rejection exercised"
)

// Data-driven coverage probes (RegisterCoverage, emitted as Raw by the
// service), evaluated by the drain hook; reached by meta_bulk.
const (
	CovSkipAlreadyReverted  oc.EventID = "oracle: coverage Apply skipped TRANSACTION_ALREADY_REVERTED and continued"
	CovSkipMetadataNotFound oc.EventID = "oracle: coverage Apply skipped METADATA_NOT_FOUND and continued"
)

// ReadKindReplay is the read kind name the driver's idem_replay uses.
const ReadKindReplay = "Replay"

// ViewRevertSeek is the aiming view revert generators use to find a revert
// transaction (revert of a revert).
const ViewRevertSeek = "revertSeek"
