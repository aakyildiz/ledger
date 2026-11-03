package main

import (
	"github.com/antithesishq/antithesis-sdk-go/assert"

	lc "github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck/ledgercheck"
)

// Task 04 asserters (ledgers, chart, maintenance). One literal call site per
// event id (asserter_test.go). The nine coverage probes of ledgercheck are
// RegisterCoverage ids, emitted with AssertRaw by the core.

func init() {
	registerAsserter(lc.EvLcLedgerReadOutsideModel, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: ledger read outside model", d)
	})
	registerAsserter(lc.EvLcGetLedgerServedAbsent, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: GetLedger served a ledger outside the fleet", d)
	})
	registerAsserter(lc.EvLcGetLedgerUnexpected, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: GetLedger returned unexpected error", d)
	})
	registerAsserter(lc.EvLcGetLedgerAbsentProbed, func(_ bool, d map[string]any) {
		assert.Reachable("oracle: GetLedger on an absent ledger returned NotFound", d)
	})
	registerAsserter(lc.EvLcSchemaReadOutsideModel, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: metadata schema read outside model", d)
	})
	registerAsserter(lc.EvLcSchemaServedAbsent, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: GetMetadataSchemaStatus served a ledger outside the fleet", d)
	})
	registerAsserter(lc.EvLcSchemaUnexpected, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: GetMetadataSchemaStatus returned unexpected error", d)
	})
	registerAsserter(lc.EvLcSchemaAbsentProbed, func(_ bool, d map[string]any) {
		assert.Reachable("oracle: GetMetadataSchemaStatus on an absent ledger returned NotFound", d)
	})
	registerAsserter(lc.EvLcListingNotWindow, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: ledger listing is not the fleet's window", d)
	})
	registerAsserter(lc.EvLcListingOutsideModel, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: ledger listing outside model", d)
	})
	registerAsserter(lc.EvLcListingIdentity, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: listed ledger identity is not the one creation reported", d)
	})
	registerAsserter(lc.EvLcListingRepeated, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: ledger listing repeated a ledger", d)
	})
	registerAsserter(lc.EvLcListingMalformed, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: listed ledger is malformed", d)
	})
	registerAsserter(lc.EvLcListingReusedID, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: ledger listing reused an id", d)
	})
	registerAsserter(lc.EvLcListingAcceptedFilter, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: ledger listing accepted an unsupported filter", d)
	})
	registerAsserter(lc.EvLcListingUnexpected, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: ListLedgers returned unexpected error", d)
	})
	registerAsserter(lc.EvLcListingValidated, func(_ bool, d map[string]any) {
		assert.Reachable("oracle: ledger listing validated", d)
	})
	registerAsserter(lc.EvLcListingTruncated, func(_ bool, d map[string]any) {
		assert.Reachable("oracle: ledger listing page truncated", d)
	})
	registerAsserter(lc.EvLcListingResumedAtDeleted, func(_ bool, d map[string]any) {
		assert.Reachable("oracle: ledger listing resumed at a deleted ledger", d)
	})
	registerAsserter(lc.EvLcListingRejectedFilter, func(_ bool, d map[string]any) {
		assert.Reachable("oracle: ledger listing rejected an unsupported filter", d)
	})
}
