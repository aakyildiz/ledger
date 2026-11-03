package main

import (
	"github.com/antithesishq/antithesis-sdk-go/assert"

	qc "github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck/quietcheck"
)

// Task 11 asserters: the quiet-period exact checks (incremental audit replay,
// exact per-ledger state linearizably and per node, global log stream). The
// quietcheck package registers the "audit" and "ledger" quiet sections in its
// own init(); importing it here links them into the service. Every finding is
// Unreachable; the must-hits are reached by the exact quiet hook
// (rules/quiet_exact.rs).
func init() {
	registerAsserter(qc.EvAuditUnexplainedEntry, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: audit replay found an entry no tracked bulk explains", d)
	})
	registerAsserter(qc.EvAuditForeignOnTracked, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: audit replay met a foreign proposal on a tracked ledger", d)
	})
	registerAsserter(qc.EvAuditMissingSuccess, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: an acknowledged tracked bulk is missing from the audit trail", d)
	})
	registerAsserter(qc.EvAuditMissingFailure, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: a definitively rejected tracked bulk is missing from the audit trail", d)
	})
	registerAsserter(qc.EvAuditAdmissionEntry, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: an admission rejection reached the audit trail", d)
	})
	registerAsserter(qc.EvAuditUncommittedEntry, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: an uncommitted failure reached the audit trail", d)
	})
	registerAsserter(qc.EvAuditReplayStateDiffers, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: audit replay state differs from the online model", d)
	})
	registerAsserter(qc.EvAuditReplayVerified, func(_ bool, d map[string]any) {
		assert.Reachable("oracle: quiet audit replay verified", d)
	})
	registerAsserter(qc.EvQuietGlobalStreamDiffers, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: quiet global log stream differs from the learned sequences", d)
	})
	registerAsserter(qc.EvQuietLogDatesDecrease, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: log dates decrease along the global sequence", d)
	})
	registerAsserter(qc.EvQuietExactStateVerified, func(_ bool, d map[string]any) {
		assert.Reachable("oracle: quiet exact state verified", d)
	})
	registerAsserter(qc.EvQuietNodeStateVerified, func(_ bool, d map[string]any) {
		assert.Reachable("oracle: a node's state matched the model at the horizon", d)
	})
	registerAsserter(qc.EvQuietStatsUsageCaughtUp, func(cond bool, d map[string]any) {
		assert.Sometimes(cond, "oracle: quiet ledger stats usage caught up with the model", d)
	})
	registerAsserter(qc.EvQuietStatsUsageAboveModel, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: quiet ledger stats usage exceeds the model", d)
	})
	registerAsserter(qc.EvQuietLogsDiffer, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: quiet log stream differs from the model", d)
	})
	registerAsserter(qc.EvQuietTxsDiffer, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: quiet transactions differ from the model", d)
	})
	registerAsserter(qc.EvQuietAcctsDiffer, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: quiet accounts differ from the model", d)
	})
	registerAsserter(qc.EvQuietStatsDiffer, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: quiet ledger stats boundaries differ from the model", d)
	})
	registerAsserter(qc.EvQuietInfoDiffers, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: quiet ledger info differs from the model", d)
	})
	registerAsserter(qc.EvNodeLogsDiffer, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: a node's log stream differs from the model at the horizon", d)
	})
	registerAsserter(qc.EvNodeTxsDiffer, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: a node's transactions differ from the model at the horizon", d)
	})
	registerAsserter(qc.EvNodeAcctsDiffer, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: a node's accounts differ from the model at the horizon", d)
	})
	registerAsserter(qc.EvNodeInfoDiffers, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: a node's ledger info differs from the model at the horizon", d)
	})
}
