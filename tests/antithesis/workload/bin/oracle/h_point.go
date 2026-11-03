package main

import (
	"github.com/antithesishq/antithesis-sdk-go/assert"

	"github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck/pointcheck"
)

// Task 05 asserters (point reads). One literal call site per event id
// (asserter_test.go). Model divergences are Unreachable; the coverage events
// are Reachable and reached by the get_account, get_transaction, aggregate,
// ledger_stats and get_log rules (tests/antithesis/hegel/porting/05-point-reads.md).
func init() {
	registerAsserter(pointcheck.EvAccountReadOutsideModel, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: account read outside model", d)
	})
	registerAsserter(pointcheck.EvGetAccountUnexpectedError, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: GetAccount returned unexpected error", d)
	})
	registerAsserter(pointcheck.EvTxReadOutsideModel, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: transaction read outside model", d)
	})
	registerAsserter(pointcheck.EvGetTransactionUnexpectedErr, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: GetTransaction returned unexpected error", d)
	})
	registerAsserter(pointcheck.EvAggOutsideModel, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: aggregate volumes outside model", d)
	})
	registerAsserter(pointcheck.EvAggMixed, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: aggregate result mixes grouped and flat volumes", d)
	})
	registerAsserter(pointcheck.EvAggRepeatsBucket, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: aggregate result repeats a bucket", d)
	})
	registerAsserter(pointcheck.EvAggServedOutsideFleet, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: aggregate served a ledger outside the fleet", d)
	})
	registerAsserter(pointcheck.EvAggUnexpectedError, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: aggregate returned unexpected error", d)
	})
	registerAsserter(pointcheck.EvAggregateVolumesUnexpected, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: AggregateVolumes returned unexpected error", d)
	})
	registerAsserter(pointcheck.EvAggServedDeletedEverywhere, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: aggregate served a ledger deleted in every candidate", d)
	})
	registerAsserter(pointcheck.EvStatsBoundsOutsideModel, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: ledger stats boundaries outside model", d)
	})
	registerAsserter(pointcheck.EvStatsCounterAboveModel, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: ledger stats counter above the model", d)
	})
	registerAsserter(pointcheck.EvStatsCountedNumscript, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: ledger stats counted a numscript execution", d)
	})
	registerAsserter(pointcheck.EvStatsServedOutsideFleet, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: ledger stats served a ledger outside the fleet", d)
	})
	registerAsserter(pointcheck.EvGetLedgerStatsUnexpectedErr, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: GetLedgerStats returned unexpected error", d)
	})
	registerAsserter(pointcheck.EvGetLogAnotherLog, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: GetLog answered with another log", d)
	})
	registerAsserter(pointcheck.EvGetLogLostCommitted, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: GetLog lost a committed log", d)
	})
	registerAsserter(pointcheck.EvGetLogServedUnassigned, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: GetLog served an unassigned sequence", d)
	})
	registerAsserter(pointcheck.EvGetLogUnexpectedError, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: GetLog returned unexpected error", d)
	})
	registerAsserter(pointcheck.EvAccountAbsentLedgerProbed, func(_ bool, d map[string]any) {
		assert.Reachable("oracle: GetAccount probing an absent ledger", d)
	})
	registerAsserter(pointcheck.EvAccountModelAbsentProbed, func(_ bool, d map[string]any) {
		assert.Reachable("oracle: GetAccount probing a model-absent account", d)
	})
	registerAsserter(pointcheck.EvCollapsedAccountValidated, func(_ bool, d map[string]any) {
		assert.Reachable("oracle: colour-collapsed account read validated", d)
	})
	registerAsserter(pointcheck.EvTxAbsentLedgerProbed, func(_ bool, d map[string]any) {
		assert.Reachable("oracle: GetTransaction probing an absent ledger", d)
	})
	registerAsserter(pointcheck.EvAggValidated, func(_ bool, d map[string]any) {
		assert.Reachable("oracle: aggregate volumes validated", d)
	})
	registerAsserter(pointcheck.EvAggGroupedValidated, func(_ bool, d map[string]any) {
		assert.Reachable("oracle: grouped aggregate volumes validated", d)
	})
	registerAsserter(pointcheck.EvAggAbsentNotFound, func(_ bool, d map[string]any) {
		assert.Reachable("oracle: aggregate on an absent ledger returned NotFound", d)
	})
	registerAsserter(pointcheck.EvAggGatedOnMissingIndex, func(_ bool, d map[string]any) {
		assert.Reachable("oracle: aggregate gated on a missing index", d)
	})
	registerAsserter(pointcheck.EvStatsUsageCaughtUp, func(_ bool, d map[string]any) {
		assert.Reachable("oracle: ledger stats usage caught up with the model", d)
	})
	registerAsserter(pointcheck.EvStatsValidated, func(_ bool, d map[string]any) {
		assert.Reachable("oracle: ledger stats validated", d)
	})
	registerAsserter(pointcheck.EvStatsAbsentNotFound, func(_ bool, d map[string]any) {
		assert.Reachable("oracle: ledger stats on an absent ledger returned NotFound", d)
	})
	registerAsserter(pointcheck.EvGetLogValidated, func(_ bool, d map[string]any) {
		assert.Reachable("oracle: GetLog validated", d)
	})
	registerAsserter(pointcheck.EvGetLogUnassignedNotFound, func(_ bool, d map[string]any) {
		assert.Reachable("oracle: GetLog on an unassigned sequence returned NotFound", d)
	})
}
