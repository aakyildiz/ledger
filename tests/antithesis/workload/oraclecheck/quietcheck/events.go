// Package quietcheck holds the quiet-period exact checks of task 11 (DESIGN
// §4.10, oracle.md §4.6 option C): the incremental audit replay (the server's
// total order of successes and failures replayed on a separate GlobalState),
// the exact per-ledger state check (linearizable and per node, after the node
// reached the barrier horizon) and the global log stream check. Both run as
// the quiet sections "audit" and "ledger", registered in init().
//
// Like every oraclecheck family package it is assertion-free: it returns
// oraclecheck.Events, which the service asserts at its own call sites
// (bin/oracle/h_quiet.go).
package quietcheck

import oc "github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck"

// Audit replay findings (Unreachable).
const (
	EvAuditUnexplainedEntry     oc.EventID = "oracle: audit replay found an entry no tracked bulk explains"
	EvAuditForeignOnTracked     oc.EventID = "oracle: audit replay met a foreign proposal on a tracked ledger"
	EvAuditMissingSuccess       oc.EventID = "oracle: an acknowledged tracked bulk is missing from the audit trail"
	EvAuditMissingFailure       oc.EventID = "oracle: a definitively rejected tracked bulk is missing from the audit trail"
	EvAuditAdmissionEntry       oc.EventID = "oracle: an admission rejection reached the audit trail"
	EvAuditUncommittedEntry     oc.EventID = "oracle: an uncommitted failure reached the audit trail"
	EvAuditReplayStateDiffers   oc.EventID = "oracle: audit replay state differs from the online model"
	EvAuditReplayVerified       oc.EventID = "oracle: quiet audit replay verified" // Reachable
	EvQuietGlobalStreamDiffers  oc.EventID = "oracle: quiet global log stream differs from the learned sequences"
	EvQuietLogDatesDecrease     oc.EventID = "oracle: log dates decrease along the global sequence"
	EvQuietExactStateVerified   oc.EventID = "oracle: quiet exact state verified"                        // Reachable
	EvQuietNodeStateVerified    oc.EventID = "oracle: a node's state matched the model at the horizon"   // Reachable
	EvQuietStatsUsageCaughtUp   oc.EventID = "oracle: quiet ledger stats usage caught up with the model" // Sometimes
	EvQuietStatsUsageAboveModel oc.EventID = "oracle: quiet ledger stats usage exceeds the model"
)

// Exact state findings, linearizable scope (Unreachable).
const (
	EvQuietLogsDiffer  oc.EventID = "oracle: quiet log stream differs from the model"
	EvQuietTxsDiffer   oc.EventID = "oracle: quiet transactions differ from the model"
	EvQuietAcctsDiffer oc.EventID = "oracle: quiet accounts differ from the model"
	EvQuietStatsDiffer oc.EventID = "oracle: quiet ledger stats boundaries differ from the model"
	EvQuietInfoDiffers oc.EventID = "oracle: quiet ledger info differs from the model"
	EvNodeLogsDiffer   oc.EventID = "oracle: a node's log stream differs from the model at the horizon"
	EvNodeTxsDiffer    oc.EventID = "oracle: a node's transactions differ from the model at the horizon"
	EvNodeAcctsDiffer  oc.EventID = "oracle: a node's accounts differ from the model at the horizon"
	EvNodeInfoDiffers  oc.EventID = "oracle: a node's ledger info differs from the model at the horizon"
)
