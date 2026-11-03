// Package preparedcheck is task 08's oracle family: prepared queries
// (DESIGN §3.1, §4.5, §4.7, §5.3).
//
// It registers
//   - the read kinds ListPreparedQueries and ExecutePreparedQuery, whose
//     validators are lifted from the singleton model
//     (M/prepared_queries_read.go) on top of the 00b matchers
//     (match.RegistryMatches, match.PreparedListOutcomeLegal,
//     match.PreparedAggregateOutcomeLegal, match.SubstituteParams,
//     match.PreparedWindowMatches, match.AggregateMatches,
//     match.PreparedAccountProbe);
//   - the view preparedQueries (per ledger, the stored names, targets and the
//     parameter references of each stored filter; `own-` names hidden unless
//     the request carries "owned": true);
//   - the begin hook prepared-serialize, which refuses (`409 serialized`) a
//     bulk touching a prepared query (ledger, name) that an outstanding bulk
//     already touches, so the FSM-stage FILTER_COMPILATION_ERROR of an update
//     (definitive-unfrozen, DESIGN §4.5) repeats on every attempt;
//   - the failure hook prepared-failures (coverage of the save-time
//     rejections).
//
// The stored definition is always re-read from each candidate base, never
// from a driver-side copy: a concurrent update makes the stored filter
// base-dependent, and the window follows it. Every assertion is a returned
// oraclecheck.Event; the literals live in bin/oracle/h_prepared.go.
package preparedcheck

import oc "github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck"

// Findings (Unreachable). Ported literals: "oracle: " + the M/ suffix.
const (
	EvListingOutsideModel         oc.EventID = "oracle: prepared query listing outside model"
	EvPageOutsideModel            oc.EventID = "oracle: prepared query page outside model"
	EvAggregateOutsideModel       oc.EventID = "oracle: prepared query aggregate outside model"
	EvExecUnexpectedError         oc.EventID = "oracle: prepared query execution returned unexpected error"
	EvCursorPageSizeMismatch      oc.EventID = "oracle: prepared query cursor page size mismatch"
	EvReverseAggregateNotRejected oc.EventID = "oracle: reverse aggregate on a prepared query not rejected"
	EvListUnexpectedError         oc.EventID = "oracle: ListPreparedQueries returned unexpected error"
)

// Coverage (Reachable), reached by task 08's rules.
const (
	EvReverseAggregateRejected oc.EventID = "oracle: reverse aggregate on a prepared query rejected"
	EvAggregateTargetRejected  oc.EventID = "oracle: aggregate on a non-accounts prepared query rejected"
	EvPageResumedOpposite      oc.EventID = "oracle: prepared query page resumed in the opposite direction"
	EvPageValidated            oc.EventID = "oracle: prepared query page validated"
	EvAggregateValidated       oc.EventID = "oracle: prepared query aggregate validated"

	// Failure-hook coverage.
	EvMalformedFilterRefused       oc.EventID = "oracle: malformed prepared-query filter refused at save"
	EvUpdateInvalidForStoredTarget oc.EventID = "oracle: an update with a filter invalid for the stored target is refused"
)
