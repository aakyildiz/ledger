// Package querycheck validates the list reads of task 06 — ListAccounts,
// ListTransactions and ListLogs — against the oracle's candidate states. It
// is the tracker-bound half of the singleton model's query validators
// (M/queries.go, M/queries_logs.go, M/indexes.go, M/rejected_filter.go,
// M/cursors.go, M/coverage.go); the pure windows, folds and legality checks
// live in oraclecheck/match (task 00b).
//
// Validators read the generator's intent from the request itself (filter,
// cursor, page size, reverse flag), never from the driver's hint (DESIGN
// §4.7). They return events; bin/oracle/h_query.go asserts them.
package querycheck

import oc "github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck"

// Event ids: "oracle: " + the singleton model's literal suffix (DESIGN §9.1).
const (
	// Model divergences (Unreachable).
	EvAccountQueryOutsideModel        oc.EventID = "oracle: account query outside model"
	EvTransactionQueryOutsideModel    oc.EventID = "oracle: transaction query outside model"
	EvIndexedAccountQueryOutside      oc.EventID = "oracle: indexed account query outside model"
	EvIndexedTransactionQueryOutside  oc.EventID = "oracle: indexed transaction query outside model"
	EvAssetIndexAccountQueryOutside   oc.EventID = "oracle: asset-index account query outside model"
	EvLogQueryOutsideModel            oc.EventID = "oracle: log query outside model"
	EvLogPageViolatesOrdering         oc.EventID = "oracle: log page violates its own ordering"
	EvLogPageCarriesSignature         oc.EventID = "oracle: log page carries a response signature"
	EvListAccountsUnexpectedError     oc.EventID = "oracle: ListAccounts returned unexpected error"
	EvListTransactionsUnexpectedError oc.EventID = "oracle: ListTransactions returned unexpected error"
	EvListLogsUnexpectedError         oc.EventID = "oracle: ListLogs returned unexpected error"
	EvIndexedAccountUnexpectedError   oc.EventID = "oracle: indexed account query returned unexpected error"
	EvIndexedTxUnexpectedError        oc.EventID = "oracle: indexed transaction query returned unexpected error"
	EvAssetIndexUnexpectedError       oc.EventID = "oracle: asset-index account query returned unexpected error"
	EvAssetIndexFilterNotBare         oc.EventID = "oracle: asset-index query filter is not a bare has-asset leaf"
	EvMalformedCursorUnexpectedError  oc.EventID = "oracle: malformed cursor returned unexpected error"
	EvMalformedTxCursorResults        oc.EventID = "oracle: malformed transaction cursor returned results"
	EvMalformedLogCursorResults       oc.EventID = "oracle: malformed log cursor returned results"
	EvRefusedFilterUnexpectedError    oc.EventID = "oracle: refused filter returned unexpected error"
	EvRefusedFilterResults            oc.EventID = "oracle: refused filter returned results"
	EvTargetInvalidUnexpectedError    oc.EventID = "oracle: target-invalid query returned unexpected error"
	EvTargetInvalidAccountResults     oc.EventID = "oracle: target-invalid account query returned results"
	EvTargetInvalidTxResults          oc.EventID = "oracle: target-invalid transaction query returned results"
	EvUnsupportedBuiltinUnexpectedErr oc.EventID = "oracle: unsupported builtin uint field returned unexpected error"
	EvUnsupportedBuiltinResults       oc.EventID = "oracle: unsupported builtin uint field returned results"

	// Must-hit coverage reached by task 06's own rules without faults.
	EvMalformedTxCursorRejected      oc.EventID = "oracle: malformed transaction cursor rejected"
	EvMalformedLogCursorRejected     oc.EventID = "oracle: malformed log cursor rejected"
	EvRefusedAccountFilterRejected   oc.EventID = "oracle: refused account filter rejected"
	EvRefusedTxFilterRejected        oc.EventID = "oracle: refused transaction filter rejected"
	EvRefusedLogFilterRejected       oc.EventID = "oracle: refused log filter rejected"
	EvTargetInvalidRejectedAccounts  oc.EventID = "oracle: target-invalid query rejected on accounts"
	EvTargetInvalidRejectedTxs       oc.EventID = "oracle: target-invalid query rejected on transactions"
	EvIndexedAccountQueryGated       oc.EventID = "oracle: indexed account query gated"
	EvIndexedTxQueryGated            oc.EventID = "oracle: indexed transaction query gated"
	EvLogQueryGated                  oc.EventID = "oracle: log query gated on a missing index"
	EvLogQueryServed                 oc.EventID = "oracle: log query served results"
	EvReverseLogQueryServed          oc.EventID = "oracle: reverse log query served results"
	EvPageSizeServerDefault          oc.EventID = "oracle: page size left to the server default"
	EvPageSizeClamped                oc.EventID = "oracle: page size clamped to the server maximum"
	EvUnsupportedBuiltinRejected     oc.EventID = "oracle: unsupported builtin uint field rejected"
	EvKindMismatchRejectedAccounts   oc.EventID = "oracle: kind-mismatched field query rejected on accounts"
	EvKindMismatchRejectedTxs        oc.EventID = "oracle: kind-mismatched field query rejected on transactions"
	EvIndexedAccountQueryServed      oc.EventID = "oracle: indexed account query served results"
	EvIndexedTxQueryServed           oc.EventID = "oracle: indexed transaction query served results"
	EvHasAssetCombinatorServed       oc.EventID = "oracle: has-asset under a combinator served results"
	EvAssetIndexAccountQueryServed   oc.EventID = "oracle: asset-index account query served results"
	EvAssetIndexAccountQueryGated    oc.EventID = "oracle: asset-index account query gated"
	EvHasAssetPrecisionOverflowRejct oc.EventID = "oracle: has-asset precision overflow rejected"
)
