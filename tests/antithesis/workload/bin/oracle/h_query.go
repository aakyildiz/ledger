package main

import (
	"github.com/antithesishq/antithesis-sdk-go/assert"

	oc "github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck"
	qc "github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck/querycheck"
)

// Task 06 (list queries): read kinds ListAccounts, ListTransactions and
// ListLogs, their asserters (one literal call site per event id) and the
// data-driven index coverage probes (M/coverage.go). Every model divergence
// is Unreachable; the Reachables are must-hit coverage.

func init() {
	oc.RegisterReadKind(oc.ReadKind{Name: "ListAccounts", Validate: qc.ValidateListAccounts})
	oc.RegisterReadKind(oc.ReadKind{Name: "ListTransactions", Validate: qc.ValidateListTransactions})
	oc.RegisterReadKind(oc.ReadKind{Name: "ListLogs", Validate: qc.ValidateListLogs})

	// One probe per workload index, the two metadata-index probes and the
	// retype-window probe. They are true only on model-verified served pages
	// whose filter needed the index; the builtin and asset indexes exist only
	// once task 07's rules create them (task 99 checks reachability).
	oc.RegisterCoverage(qc.CoverageIDs()...)

	// Model divergences and contract violations.
	registerAsserter(qc.EvAccountQueryOutsideModel, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: account query outside model", d)
	})
	registerAsserter(qc.EvTransactionQueryOutsideModel, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: transaction query outside model", d)
	})
	registerAsserter(qc.EvIndexedAccountQueryOutside, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: indexed account query outside model", d)
	})
	registerAsserter(qc.EvIndexedTransactionQueryOutside, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: indexed transaction query outside model", d)
	})
	registerAsserter(qc.EvAssetIndexAccountQueryOutside, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: asset-index account query outside model", d)
	})
	registerAsserter(qc.EvLogQueryOutsideModel, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: log query outside model", d)
	})
	registerAsserter(qc.EvLogPageViolatesOrdering, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: log page violates its own ordering", d)
	})
	registerAsserter(qc.EvLogPageCarriesSignature, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: log page carries a response signature", d)
	})
	registerAsserter(qc.EvListAccountsUnexpectedError, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: ListAccounts returned unexpected error", d)
	})
	registerAsserter(qc.EvListTransactionsUnexpectedError, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: ListTransactions returned unexpected error", d)
	})
	registerAsserter(qc.EvListLogsUnexpectedError, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: ListLogs returned unexpected error", d)
	})
	registerAsserter(qc.EvIndexedAccountUnexpectedError, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: indexed account query returned unexpected error", d)
	})
	registerAsserter(qc.EvIndexedTxUnexpectedError, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: indexed transaction query returned unexpected error", d)
	})
	registerAsserter(qc.EvAssetIndexUnexpectedError, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: asset-index account query returned unexpected error", d)
	})
	registerAsserter(qc.EvAssetIndexFilterNotBare, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: asset-index query filter is not a bare has-asset leaf", d)
	})
	registerAsserter(qc.EvMalformedCursorUnexpectedError, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: malformed cursor returned unexpected error", d)
	})
	registerAsserter(qc.EvMalformedTxCursorResults, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: malformed transaction cursor returned results", d)
	})
	registerAsserter(qc.EvMalformedLogCursorResults, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: malformed log cursor returned results", d)
	})
	registerAsserter(qc.EvRefusedFilterUnexpectedError, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: refused filter returned unexpected error", d)
	})
	registerAsserter(qc.EvRefusedFilterResults, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: refused filter returned results", d)
	})
	registerAsserter(qc.EvTargetInvalidUnexpectedError, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: target-invalid query returned unexpected error", d)
	})
	registerAsserter(qc.EvTargetInvalidAccountResults, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: target-invalid account query returned results", d)
	})
	registerAsserter(qc.EvTargetInvalidTxResults, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: target-invalid transaction query returned results", d)
	})
	registerAsserter(qc.EvUnsupportedBuiltinUnexpectedErr, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: unsupported builtin uint field returned unexpected error", d)
	})
	registerAsserter(qc.EvUnsupportedBuiltinResults, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: unsupported builtin uint field returned results", d)
	})

	// Must-hit coverage reached by task 06's rules without faults.
	registerAsserter(qc.EvMalformedTxCursorRejected, func(_ bool, d map[string]any) {
		assert.Reachable("oracle: malformed transaction cursor rejected", d)
	})
	registerAsserter(qc.EvMalformedLogCursorRejected, func(_ bool, d map[string]any) {
		assert.Reachable("oracle: malformed log cursor rejected", d)
	})
	registerAsserter(qc.EvRefusedAccountFilterRejected, func(_ bool, d map[string]any) {
		assert.Reachable("oracle: refused account filter rejected", d)
	})
	registerAsserter(qc.EvRefusedTxFilterRejected, func(_ bool, d map[string]any) {
		assert.Reachable("oracle: refused transaction filter rejected", d)
	})
	registerAsserter(qc.EvRefusedLogFilterRejected, func(_ bool, d map[string]any) {
		assert.Reachable("oracle: refused log filter rejected", d)
	})
	registerAsserter(qc.EvTargetInvalidRejectedAccounts, func(_ bool, d map[string]any) {
		assert.Reachable("oracle: target-invalid query rejected on accounts", d)
	})
	registerAsserter(qc.EvTargetInvalidRejectedTxs, func(_ bool, d map[string]any) {
		assert.Reachable("oracle: target-invalid query rejected on transactions", d)
	})
	registerAsserter(qc.EvIndexedAccountQueryGated, func(_ bool, d map[string]any) {
		assert.Reachable("oracle: indexed account query gated", d)
	})
	registerAsserter(qc.EvIndexedTxQueryGated, func(_ bool, d map[string]any) {
		assert.Reachable("oracle: indexed transaction query gated", d)
	})
	registerAsserter(qc.EvLogQueryGated, func(_ bool, d map[string]any) {
		assert.Reachable("oracle: log query gated on a missing index", d)
	})
	registerAsserter(qc.EvLogQueryServed, func(_ bool, d map[string]any) {
		assert.Reachable("oracle: log query served results", d)
	})
	registerAsserter(qc.EvReverseLogQueryServed, func(_ bool, d map[string]any) {
		assert.Reachable("oracle: reverse log query served results", d)
	})
	registerAsserter(qc.EvPageSizeServerDefault, func(_ bool, d map[string]any) {
		assert.Reachable("oracle: page size left to the server default", d)
	})
	registerAsserter(qc.EvPageSizeClamped, func(_ bool, d map[string]any) {
		assert.Reachable("oracle: page size clamped to the server maximum", d)
	})
	registerAsserter(qc.EvUnsupportedBuiltinRejected, func(_ bool, d map[string]any) {
		assert.Reachable("oracle: unsupported builtin uint field rejected", d)
	})
	// Reached without indexes: the oracle classifies the overflow as refused
	// only after this check, and the server answers FILTER_COMPILATION_ERROR.
	registerAsserter(qc.EvHasAssetPrecisionOverflowRejct, func(_ bool, d map[string]any) {
		assert.Reachable("oracle: has-asset precision overflow rejected", d)
	})

	// reachable only with task 07 merged (indexes created, kind checks need a
	// ready index); listed under "Requests to 99" in porting/06-list-queries.md.
	registerAsserter(qc.EvKindMismatchRejectedAccounts, func(_ bool, d map[string]any) {
		assert.Reachable("oracle: kind-mismatched field query rejected on accounts", d)
	})
	registerAsserter(qc.EvKindMismatchRejectedTxs, func(_ bool, d map[string]any) {
		assert.Reachable("oracle: kind-mismatched field query rejected on transactions", d)
	})
	registerAsserter(qc.EvIndexedAccountQueryServed, func(_ bool, d map[string]any) {
		assert.Reachable("oracle: indexed account query served results", d)
	})
	registerAsserter(qc.EvIndexedTxQueryServed, func(_ bool, d map[string]any) {
		assert.Reachable("oracle: indexed transaction query served results", d)
	})
	registerAsserter(qc.EvHasAssetCombinatorServed, func(_ bool, d map[string]any) {
		assert.Reachable("oracle: has-asset under a combinator served results", d)
	})
	registerAsserter(qc.EvAssetIndexAccountQueryServed, func(_ bool, d map[string]any) {
		assert.Reachable("oracle: asset-index account query served results", d)
	})
	registerAsserter(qc.EvAssetIndexAccountQueryGated, func(_ bool, d map[string]any) {
		assert.Reachable("oracle: asset-index account query gated", d)
	})
}
