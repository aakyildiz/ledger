package main

import (
	"github.com/antithesishq/antithesis-sdk-go/assert"

	ic "github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck/indexcheck"
)

// Task 07 asserters: one literal call site per indexcheck event id. Every
// model-divergence finding is Unreachable; coverage points are Reachable.

func init() {
	registerAsserter(ic.EvListingOutsideModel, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: index listing outside model", d)
	})
	registerAsserter(ic.EvListingRepeated, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: index listing repeated an entry", d)
	})
	registerAsserter(ic.EvListingOutsideFleet, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: index listing served a ledger outside the fleet", d)
	})
	registerAsserter(ic.EvListingScope, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: index listing violated its scope", d)
	})
	registerAsserter(ic.EvBucketListingOutsideModel, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: bucket-wide index listing outside model", d)
	})
	registerAsserter(ic.EvBucketListingRepeated, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: bucket-wide index listing repeated an entry", d)
	})
	registerAsserter(ic.EvBucketListingNoID, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: bucket-wide index listing served an entry with no id", d)
	})
	registerAsserter(ic.EvBucketListIndexesError, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: bucket-wide ListIndexes returned unexpected error", d)
	})
	registerAsserter(ic.EvBucketScopedListingLedger, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: bucket-scoped index listing served a ledger's entry", d)
	})
	registerAsserter(ic.EvBucketLookupLedgerEntry, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: bucket-scoped index lookup answered with a ledger's entry", d)
	})
	registerAsserter(ic.EvBucketLookupError, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: bucket-scoped index lookup returned unexpected error", d)
	})
	registerAsserter(ic.EvLookupAnotherEntry, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: index lookup answered with another entry", d)
	})
	registerAsserter(ic.EvLookupError, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: index lookup returned unexpected error", d)
	})
	registerAsserter(ic.EvLookupOutsideFleet, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: index lookup served a ledger outside the fleet", d)
	})
	registerAsserter(ic.EvPresenceOutsideModel, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: index presence outside model", d)
	})
	registerAsserter(ic.EvStatusNotSelfConsistent, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: index status is not self-consistent", d)
	})
	registerAsserter(ic.EvListIndexesError, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: ListIndexes returned unexpected error", d)
	})
	registerAsserter(ic.EvInspectOutsideFleet, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: index inspection served a ledger outside the fleet", d)
	})
	registerAsserter(ic.EvInspectSummaryOutsideModel, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: index inspection summary outside model", d)
	})
	registerAsserter(ic.EvInspectContract, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: index inspection violates its own contract", d)
	})
	registerAsserter(ic.EvResumeError, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: resumed index inspection returned unexpected error", d)
	})
	registerAsserter(ic.EvResumeContract, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: resumed index inspection violates its own contract", d)
	})
	registerAsserter(ic.EvInspectError, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: InspectIndex returned unexpected error", d)
	})
	registerAsserter(ic.EvSummaryOtherArm, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: summary mode answered another arm", d)
	})
	registerAsserter(ic.EvPromoted, func(_ bool, d map[string]any) {
		assert.Reachable("oracle: index promoted to active", d)
	})
	registerAsserter(ic.EvDemoted, func(_ bool, d map[string]any) {
		assert.Reachable("oracle: index demoted to ambiguous", d)
	})
	registerAsserter(ic.EvPromotionDeferred, func(_ bool, d map[string]any) {
		assert.Reachable("oracle: index promotion deferred behind an earlier operation", d)
	})
	registerAsserter(ic.EvRetypeClosed, func(_ bool, d map[string]any) {
		assert.Reachable("oracle: retype window closed", d)
	})
	registerAsserter(ic.EvStatusValidated, func(_ bool, d map[string]any) {
		assert.Reachable("oracle: index status validated", d)
	})
	registerAsserter(ic.EvStatusFile, func(_ bool, d map[string]any) {
		assert.Reachable("oracle: index status reported a materialised index file", d)
	})
	registerAsserter(ic.EvListingValidated, func(_ bool, d map[string]any) {
		assert.Reachable("oracle: index listing validated", d)
	})
	registerAsserter(ic.EvFleetListingValidated, func(_ bool, d map[string]any) {
		assert.Reachable("oracle: fleet-wide index listing validated", d)
	})
	registerAsserter(ic.EvBucketListingValid, func(_ bool, d map[string]any) {
		assert.Reachable("oracle: bucket-scoped index listing validated", d)
	})
	registerAsserter(ic.EvBucketLookupNotFound, func(_ bool, d map[string]any) {
		assert.Reachable("oracle: bucket-scoped index lookup returned NotFound", d)
	})
	registerAsserter(ic.EvLookupValidated, func(_ bool, d map[string]any) {
		assert.Reachable("oracle: index lookup validated", d)
	})
	registerAsserter(ic.EvListingAbsentNotFound, func(_ bool, d map[string]any) {
		assert.Reachable("oracle: index listing on an absent ledger returned NotFound", d)
	})
	registerAsserter(ic.EvLookupAbsentNotFound, func(_ bool, d map[string]any) {
		assert.Reachable("oracle: index lookup on an absent ledger returned NotFound", d)
	})
	registerAsserter(ic.EvInspectValidated, func(_ bool, d map[string]any) {
		assert.Reachable("oracle: index inspection validated", d)
	})
	registerAsserter(ic.EvInspectGated, func(_ bool, d map[string]any) {
		assert.Reachable("oracle: index inspection gated on a missing index", d)
	})
	registerAsserter(ic.EvInspectAbsentNotFound, func(_ bool, d map[string]any) {
		assert.Reachable("oracle: index inspection on an absent ledger returned NotFound", d)
	})
	registerAsserter(ic.EvInspectResumed, func(_ bool, d map[string]any) {
		assert.Reachable("oracle: index inspection resumed from its cursor", d)
	})
	registerAsserter(ic.EvSummaryMatched, func(_ bool, d map[string]any) {
		assert.Reachable("oracle: index inspection summary matched the model", d)
	})
	registerAsserter(ic.EvSummaryPopulated, func(_ bool, d map[string]any) {
		assert.Reachable("oracle: index inspection summary matched a populated index", d)
	})
	registerAsserter(ic.EvSummaryUnconvertible, func(_ bool, d map[string]any) {
		assert.Reachable("oracle: index inspection summary counted unconvertible values", d)
	})
	registerAsserter(ic.EvInspectCheckpoint, func(_ bool, d map[string]any) {
		assert.Reachable("oracle: index inspection served from a checkpoint", d)
	})
	registerAsserter(ic.EvFieldNotInSchema, func(_ bool, d map[string]any) {
		assert.Reachable("oracle: metadata field not in schema rejection exercised", d)
	})
}
