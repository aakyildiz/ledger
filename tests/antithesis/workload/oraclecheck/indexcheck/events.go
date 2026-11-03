// Package indexcheck is task 07's oracle library: index create frontiers,
// driver-polled readiness (promotion, demotion), retype-window closure, the
// index registry reads (ListIndexes, GetIndex, GetIndexStatus), InspectIndex,
// the indexGaps view and the index rejection coverage. It is assertion-free:
// every check returns oraclecheck events whose ids are the literal messages
// bin/oracle/h_index.go asserts.
package indexcheck

import oc "github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck"

// Findings (Unreachable). Values are "oracle: " + the singleton model's
// suffix (M/indexes.go, M/index_introspect.go, M/index_inspect.go).
const (
	EvListingOutsideModel         oc.EventID = "oracle: index listing outside model"
	EvListingRepeated             oc.EventID = "oracle: index listing repeated an entry"
	EvListingOutsideFleet         oc.EventID = "oracle: index listing served a ledger outside the fleet"
	EvListingScope                oc.EventID = "oracle: index listing violated its scope"
	EvBucketListingOutsideModel   oc.EventID = "oracle: bucket-wide index listing outside model"
	EvBucketListingRepeated       oc.EventID = "oracle: bucket-wide index listing repeated an entry"
	EvBucketListingNoID           oc.EventID = "oracle: bucket-wide index listing served an entry with no id"
	EvBucketListIndexesError      oc.EventID = "oracle: bucket-wide ListIndexes returned unexpected error"
	EvBucketScopedListingLedger   oc.EventID = "oracle: bucket-scoped index listing served a ledger's entry"
	EvBucketLookupLedgerEntry     oc.EventID = "oracle: bucket-scoped index lookup answered with a ledger's entry"
	EvBucketLookupError           oc.EventID = "oracle: bucket-scoped index lookup returned unexpected error"
	EvLookupAnotherEntry          oc.EventID = "oracle: index lookup answered with another entry"
	EvLookupError                 oc.EventID = "oracle: index lookup returned unexpected error"
	EvLookupOutsideFleet          oc.EventID = "oracle: index lookup served a ledger outside the fleet"
	EvPresenceOutsideModel        oc.EventID = "oracle: index presence outside model"
	EvStatusNotSelfConsistent     oc.EventID = "oracle: index status is not self-consistent"
	EvListIndexesError            oc.EventID = "oracle: ListIndexes returned unexpected error"
	EvInspectOutsideFleet         oc.EventID = "oracle: index inspection served a ledger outside the fleet"
	EvInspectSummaryOutsideModel  oc.EventID = "oracle: index inspection summary outside model"
	EvInspectContract             oc.EventID = "oracle: index inspection violates its own contract"
	EvResumeError                 oc.EventID = "oracle: resumed index inspection returned unexpected error"
	EvResumeContract              oc.EventID = "oracle: resumed index inspection violates its own contract"
	EvInspectError                oc.EventID = "oracle: InspectIndex returned unexpected error"
	EvSummaryOtherArm             oc.EventID = "oracle: summary mode answered another arm"
)

// Must-hit coverage (Reachable).
const (
	EvPromoted              oc.EventID = "oracle: index promoted to active"
	EvDemoted               oc.EventID = "oracle: index demoted to ambiguous"
	EvPromotionDeferred     oc.EventID = "oracle: index promotion deferred behind an earlier operation"
	EvRetypeClosed          oc.EventID = "oracle: retype window closed"
	EvStatusValidated       oc.EventID = "oracle: index status validated"
	EvStatusFile            oc.EventID = "oracle: index status reported a materialised index file"
	EvListingValidated      oc.EventID = "oracle: index listing validated"
	EvFleetListingValidated oc.EventID = "oracle: fleet-wide index listing validated"
	EvBucketListingValid    oc.EventID = "oracle: bucket-scoped index listing validated"
	EvBucketLookupNotFound  oc.EventID = "oracle: bucket-scoped index lookup returned NotFound"
	EvLookupValidated       oc.EventID = "oracle: index lookup validated"
	EvListingAbsentNotFound oc.EventID = "oracle: index listing on an absent ledger returned NotFound"
	EvLookupAbsentNotFound  oc.EventID = "oracle: index lookup on an absent ledger returned NotFound"
	EvInspectValidated      oc.EventID = "oracle: index inspection validated"
	EvInspectGated          oc.EventID = "oracle: index inspection gated on a missing index"
	EvInspectAbsentNotFound oc.EventID = "oracle: index inspection on an absent ledger returned NotFound"
	EvInspectResumed        oc.EventID = "oracle: index inspection resumed from its cursor"
	EvSummaryMatched        oc.EventID = "oracle: index inspection summary matched the model"
	EvSummaryPopulated      oc.EventID = "oracle: index inspection summary matched a populated index"
	EvSummaryUnconvertible  oc.EventID = "oracle: index inspection summary counted unconvertible values"
	EvInspectCheckpoint     oc.EventID = "oracle: index inspection served from a checkpoint"
	// EvFieldNotInSchema is new (failure hook): a CreateIndex on an
	// undeclared metadata field was refused and explained by the model.
	EvFieldNotInSchema oc.EventID = "oracle: metadata field not in schema rejection exercised"
)
