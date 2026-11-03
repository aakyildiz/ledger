// Package checkpointcheck validates the query-checkpoint family of task 09
// (DESIGN §3 cluster-scoped orders, §4.6 begin hooks, §5.3): the begin hook
// that serializes checkpoint creates and predicts the id a create will get,
// frozen reads served by a checkpoint (read kind CheckpointRead), the
// predicted read that races a create (PredictedCheckpointRead), the fenced
// registry and schedule reads (CheckpointRegistry, CheckpointSchedule), the
// `checkpoints` aiming view, and the nine checkpoint coverage probes of the
// singleton model (M/coverage.go:212-220).
//
// The checks are lifted from M/checkpoint_reads.go and M/checkpoints.go;
// every assertion became a returned oraclecheck.Event whose literal lives in
// the service's h_checkpoint.go asserter (coverage probes are Raw events).
// Generator intent (which arm, which checkpoint) is read from the request and
// the read params, never from the hint.
package checkpointcheck

import oc "github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck"

// Findings (Unreachable). Ported literals: "oracle: " + the M/ suffix.
const (
	EvReadOutsideFrozen        oc.EventID = "oracle: checkpoint read outside frozen model"
	EvPredictedOutsideCreation oc.EventID = "oracle: predicted checkpoint read outside creation model"
	EvPredictedUnexpectedError oc.EventID = "oracle: predicted checkpoint read returned unexpected error"
	EvListOutsideModel         oc.EventID = "oracle: checkpoint list outside model"
	EvListSequenceOutside      oc.EventID = "oracle: checkpoint list sequence outside captured model"
	EvListUnexpectedError      oc.EventID = "oracle: checkpoint list returned unexpected error"
	EvScheduleOutsideModel     oc.EventID = "oracle: checkpoint schedule outside model"
	EvScheduleUnexpectedError  oc.EventID = "oracle: checkpoint schedule returned unexpected error"
	// New (MD/parallel_driver_query_checkpoints/main.go:127-137 checked
	// GetQueryCheckpointInfo once per invocation; read kind CheckpointInfo
	// checks it against the model on a fenced node).
	EvInfoOutsideModel    oc.EventID = "oracle: checkpoint info outside model"
	EvInfoUnexpectedError oc.EventID = "oracle: checkpoint info returned unexpected error"
)

// Driver protocol misuse (a request the driver never builds: an unknown arm,
// an undecodable body, no checkpoint id). Not a ledger finding.
const EvProtocolMalformedRead oc.EventID = "oracle protocol: checkpoint read request is malformed"

// Reachable coverage of read kind CheckpointInfo, reached by
// checkpoint_registry's info arm once a checkpoint exists.
const EvInfoMatched oc.EventID = "oracle: coverage checkpoint info matched the oracle"

// Reachable coverage (M/checkpoint_reads.go:241), reached by checkpoint_read's
// ListLedgers arm.
const EvFrozenFleetListing oc.EventID = "oracle: coverage a checkpoint served the frozen fleet listing"

// Data-driven coverage probes (M/coverage.go:212-220), registered with
// oraclecheck.RegisterCoverage and emitted as Raw (Sometimes) events: every
// emission reports all nine, the one that happened true, so each probe gets
// false evaluations to steer on, as noteCheckpointCoverage did.
const (
	CovCreate         oc.EventID = "oracle: coverage checkpoint creation matched the oracle"
	CovDelete         oc.EventID = "oracle: coverage checkpoint deletion matched the oracle"
	CovFrozenRead     oc.EventID = "oracle: coverage checkpoint served a frozen model-verified read"
	CovDeletedRead    oc.EventID = "oracle: coverage deleted checkpoint read returned typed NOT_FOUND"
	CovListing        oc.EventID = "oracle: coverage checkpoint listing matched the oracle"
	CovLimit          oc.EventID = "oracle: coverage checkpoint limit returned typed CHECKPOINT_LIMIT_REACHED"
	CovScheduleSet    oc.EventID = "oracle: coverage checkpoint schedule set matched the oracle"
	CovScheduleDelete oc.EventID = "oracle: coverage checkpoint schedule deletion matched the oracle"
	CovScheduleRead   oc.EventID = "oracle: coverage checkpoint schedule read matched the oracle"
)

// CoverageProbes lists the nine Raw probes in M/coverage.go order.
var CoverageProbes = []oc.EventID{
	CovCreate, CovDelete, CovFrozenRead, CovDeletedRead, CovListing, CovLimit,
	CovScheduleSet, CovScheduleDelete, CovScheduleRead,
}

// coverage reports hit as true and every other probe as false
// (M/coverage.go noteCheckpointCoverage).
func coverage(hit oc.EventID, d map[string]any) []oc.Event {
	out := make([]oc.Event, 0, len(CoverageProbes))
	for _, id := range CoverageProbes {
		var details map[string]any
		if id == hit {
			details = d
		}
		out = append(out, oc.Raw(id, id == hit, details))
	}

	return out
}
