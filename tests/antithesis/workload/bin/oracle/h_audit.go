package main

import (
	"github.com/antithesishq/antithesis-sdk-go/assert"

	oc "github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck"
	ac "github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck/auditcheck"
)

// Task 10 (audit reads): read kinds ListAuditEntries and GetAuditEntry, the
// auditSamples view and their asserters (one literal call site per event id;
// the singleton's dynamic "singleton_driver_model: "+finding messages,
// M/audit.go:168,1321, become one fixed id per finding). Every model
// divergence is Unreachable; the Reachables are must-hit coverage.

func init() {
	oc.RegisterReadKind(oc.ReadKind{Name: ac.KindList, Validate: ac.ValidateListAuditEntries})
	oc.RegisterReadKind(oc.ReadKind{Name: ac.KindGet, Validate: ac.ValidateGetAuditEntry})
	oc.RegisterView(ac.ViewSamples, ac.SamplesView)

	// Model divergences, contract violations and the new foreign-tolerance and
	// bulk-atomicity properties.
	registerAsserter(ac.EvAuditFailureUnexplained, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: audit failure entry unexplained by the model", d)
	})
	registerAsserter(ac.EvAuditNamesUncommittedLogs, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: audit entry names logs the model never committed", d)
	})
	registerAsserter(ac.EvAuditOutsideModel, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: audit entry outside model", d)
	})
	registerAsserter(ac.EvAuditOutsideOrderType, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: audit entry outside the order-type scope", d)
	})
	registerAsserter(ac.EvAuditMissesCommittedLog, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: audit trail misses a committed log", d)
	})
	registerAsserter(ac.EvAuditOmitsKnown, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: audit page omits an entry the trail served before", d)
	})
	registerAsserter(ac.EvAuditStoppedShortOfKnown, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: audit page stopped short of an entry the trail served before", d)
	})
	registerAsserter(ac.EvAuditDroppedResumeCursor, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: audit page dropped its resume cursor", d)
	})
	registerAsserter(ac.EvAuditWrongBatchKey, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: audit entry names the wrong batch key", d)
	})
	registerAsserter(ac.EvAuditPageContract, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: audit page violates its own contract", d)
	})
	registerAsserter(ac.EvAuditEntryReadOtherSeq, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: audit entry read served a different sequence", d)
	})
	registerAsserter(ac.EvAuditEntryServedUnassigned, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: audit entry served an unassigned sequence", d)
	})
	registerAsserter(ac.EvAuditMalformedCursorResults, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: malformed audit cursor returned results", d)
	})
	registerAsserter(ac.EvAuditUnsupportedFilterPage, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: unsupported audit filter returned a page", d)
	})
	registerAsserter(ac.EvAuditListUnexpectedError, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: ListAuditEntries returned unexpected error", d)
	})
	registerAsserter(ac.EvAuditGetUnexpectedError, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: GetAuditEntry returned unexpected error", d)
	})
	registerAsserter(ac.EvAuditForeignTouchedTracked, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: a foreign proposal touched a tracked ledger", d)
	})
	registerAsserter(ac.EvAuditRejectedBulkHasSuccess, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: a rejected keyed bulk has no success audit entry", d)
	})
	registerAsserter(ac.EvAuditFailuresUniquePerPropsl, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: audit failure entries are unique per proposal", d)
	})
	registerAsserter(ac.EvAuditRejectedBulkMissesFailure, func(_ bool, d map[string]any) {
		assert.Unreachable("oracle: a rejected keyed bulk has a failure audit entry", d)
	})

	// Must-hit coverage reached by task 10's rules (see HG/porting/10-audit.md).
	registerAsserter(ac.EvAuditPageValidated, func(_ bool, d map[string]any) {
		assert.Reachable("oracle: audit page validated", d)
	})
	registerAsserter(ac.EvAuditEntryReadValidated, func(_ bool, d map[string]any) {
		assert.Reachable("oracle: audit entry read validated", d)
	})
	registerAsserter(ac.EvAuditEntryCarriedItems, func(_ bool, d map[string]any) {
		assert.Reachable("oracle: audit entry carried its per-order items", d)
	})
	registerAsserter(ac.EvAuditUnassignedNotFound, func(_ bool, d map[string]any) {
		assert.Reachable("oracle: audit entry read on an unassigned sequence returned NotFound", d)
	})
	registerAsserter(ac.EvAuditBatchKeyMatched, func(_ bool, d map[string]any) {
		assert.Reachable("oracle: audit entry batch key matched the model", d)
	})
	registerAsserter(ac.EvAuditJudgedAgainstKnown, func(_ bool, d map[string]any) {
		assert.Reachable("oracle: audit page judged against remembered entries", d)
	})
	registerAsserter(ac.EvAuditCursorCorroborated, func(_ bool, d map[string]any) {
		assert.Reachable("oracle: audit resume cursor corroborated by a remembered entry", d)
	})
	registerAsserter(ac.EvAuditSeqInOrRejected, func(_ bool, d map[string]any) {
		assert.Reachable("oracle: audit sequence bound inside or rejected", d)
	})
	registerAsserter(ac.EvAuditUnspecifiedRejected, func(_ bool, d map[string]any) {
		assert.Reachable("oracle: unspecified audit field rejected", d)
	})
	registerAsserter(ac.EvAuditTypeMismatchRejected, func(_ bool, d map[string]any) {
		assert.Reachable("oracle: audit condition of the wrong type rejected", d)
	})
	registerAsserter(ac.EvAuditKeyParamRejected, func(_ bool, d map[string]any) {
		assert.Reachable("oracle: parameterised idempotency-key condition rejected", d)
	})
	registerAsserter(ac.EvAuditNulPrefixRejected, func(_ bool, d map[string]any) {
		assert.Reachable("oracle: idempotency-key prefix containing NUL rejected", d)
	})
	registerAsserter(ac.EvAuditNotRejected, func(_ bool, d map[string]any) {
		assert.Reachable("oracle: audit not filter rejected", d)
	})
	registerAsserter(ac.EvAuditMalformedCursorRejected, func(_ bool, d map[string]any) {
		assert.Reachable("oracle: malformed audit cursor rejected", d)
	})

	// Must-hit coverage task 10 cannot reach alone (Requests to 99 in
	// HG/porting/10-audit.md): tracked rejections come from tasks 02/03, and a
	// retryable FSM-stage failure needs faults.
	registerAsserter(ac.EvAuditFailureMatchedRejection, func(_ bool, d map[string]any) {
		assert.Reachable("oracle: audit failure entry matched a model rejection", d)
	})
	// Not must-hit (task 99): on the current server every named retryable FSM
	// reason implements UncommittedFailure and is never audited, so the guard
	// can fire only for an unclassified (UNSPECIFIED) or future reason. It
	// guards against a false alarm; a cataloged Reachable that cannot be hit
	// would be a failing property. AlwaysOrUnreachable(true) records it when it
	// does happen and never fails.
	registerAsserter(ac.EvAuditRetryableFailureUnjudged, func(_ bool, d map[string]any) {
		assert.AlwaysOrUnreachable(true, "oracle: audit failure entry with a retryable reason left unjudged", d)
	})
	registerAsserter(ac.EvAuditFailureOwedChecked, func(_ bool, d map[string]any) {
		assert.Reachable("oracle: rejected keyed bulk checked for its failure audit entry", d)
	})
}
