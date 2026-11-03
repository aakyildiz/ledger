// Package auditcheck validates the audit reads of task 10 — ListAuditEntries
// and GetAuditEntry — against the oracle's side tables. It is the
// tracker-bound half of the singleton model's audit validators (M/audit.go);
// the pure page contract, entry matchers and filter folds live in
// oraclecheck/match (task 00b).
//
// Unlike the singleton, the oracle does not own the audit trail: Rust-checked
// families, scenario blocks and witnesses write concurrently (DESIGN §3).
// Every served entry is classified by its idempotency key: an entry whose key
// starts with "<trackedPrefix>k" is tracked, everything else (other keys, the
// empty key, technical proposals) is foreign. Structural checks hold for every
// entry; model checks hold for tracked entries only; a foreign entry must never
// name a tracked ledger.
//
// Validators read the generator's intent (filter, cursor, page size, direction,
// probed sequence) from the request itself, never from the driver's hint
// (DESIGN §4.7). They return events; bin/oracle/h_audit.go asserts them, one
// literal call site per id.
package auditcheck

import oc "github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck"

// Event ids: "oracle: " + the singleton model's literal suffix for ported
// checks (DESIGN §9.1), "oracle: <sentence>" for the new ones.
const (
	// Model divergences (Unreachable). One id per finding string of
	// M/audit.go validateAuditPage / validateAuditEntry, which asserted them
	// through a dynamic message (M/audit.go:168,1321).
	EvAuditFailureUnexplained   oc.EventID = "oracle: audit failure entry unexplained by the model"
	EvAuditNamesUncommittedLogs oc.EventID = "oracle: audit entry names logs the model never committed"
	EvAuditOutsideModel         oc.EventID = "oracle: audit entry outside model"
	EvAuditOutsideOrderType     oc.EventID = "oracle: audit entry outside the order-type scope"
	EvAuditMissesCommittedLog   oc.EventID = "oracle: audit trail misses a committed log"
	EvAuditOmitsKnown           oc.EventID = "oracle: audit page omits an entry the trail served before"
	EvAuditStoppedShortOfKnown  oc.EventID = "oracle: audit page stopped short of an entry the trail served before"
	EvAuditDroppedResumeCursor  oc.EventID = "oracle: audit page dropped its resume cursor"

	// Structural and contract violations (Unreachable).
	EvAuditWrongBatchKey          oc.EventID = "oracle: audit entry names the wrong batch key"
	EvAuditPageContract           oc.EventID = "oracle: audit page violates its own contract"
	EvAuditEntryReadOtherSeq      oc.EventID = "oracle: audit entry read served a different sequence"
	EvAuditEntryServedUnassigned  oc.EventID = "oracle: audit entry served an unassigned sequence"
	EvAuditMalformedCursorResults oc.EventID = "oracle: malformed audit cursor returned results"
	EvAuditUnsupportedFilterPage  oc.EventID = "oracle: unsupported audit filter returned a page"
	EvAuditListUnexpectedError    oc.EventID = "oracle: ListAuditEntries returned unexpected error"
	EvAuditGetUnexpectedError     oc.EventID = "oracle: GetAuditEntry returned unexpected error"

	// New properties (Unreachable): foreign tolerance and the audit half of
	// MD/parallel_driver_bulk_atomicity.
	EvAuditForeignTouchedTracked   oc.EventID = "oracle: a foreign proposal touched a tracked ledger"
	EvAuditRejectedBulkHasSuccess  oc.EventID = "oracle: a rejected keyed bulk has no success audit entry"
	EvAuditFailuresUniquePerPropsl oc.EventID = "oracle: audit failure entries are unique per proposal"
	// The failure half of the old driver's "failure audit entry observed for
	// failed atomic bulk", restricted to keys rejected for good at the FSM
	// stage and observed before the read began.
	EvAuditRejectedBulkMissesFailure oc.EventID = "oracle: a rejected keyed bulk has a failure audit entry"

	// Must-hit coverage (Reachable), reached by task 10's rules without faults
	// (EvAuditFailureMatchedRejection needs tracked FSM rejections from tasks
	// 02/03 and is listed under "Requests to 99" in HG/porting/10-audit.md).
	EvAuditPageValidated           oc.EventID = "oracle: audit page validated"
	EvAuditEntryReadValidated      oc.EventID = "oracle: audit entry read validated"
	EvAuditEntryCarriedItems       oc.EventID = "oracle: audit entry carried its per-order items"
	EvAuditUnassignedNotFound      oc.EventID = "oracle: audit entry read on an unassigned sequence returned NotFound"
	EvAuditFailureMatchedRejection oc.EventID = "oracle: audit failure entry matched a model rejection"
	EvAuditBatchKeyMatched         oc.EventID = "oracle: audit entry batch key matched the model"
	EvAuditJudgedAgainstKnown      oc.EventID = "oracle: audit page judged against remembered entries"
	EvAuditCursorCorroborated      oc.EventID = "oracle: audit resume cursor corroborated by a remembered entry"
	EvAuditSeqInOrRejected         oc.EventID = "oracle: audit sequence bound inside or rejected"
	EvAuditUnspecifiedRejected     oc.EventID = "oracle: unspecified audit field rejected"
	EvAuditTypeMismatchRejected    oc.EventID = "oracle: audit condition of the wrong type rejected"
	EvAuditKeyParamRejected        oc.EventID = "oracle: parameterised idempotency-key condition rejected"
	EvAuditNulPrefixRejected       oc.EventID = "oracle: idempotency-key prefix containing NUL rejected"
	EvAuditNotRejected             oc.EventID = "oracle: audit not filter rejected"
	EvAuditMalformedCursorRejected oc.EventID = "oracle: malformed audit cursor rejected"

	// Coverage that needs faults (a retryable FSM-stage failure audited under
	// a tracked key) or task 02/03's rejection generators; listed under
	// "Requests to 99" in HG/porting/10-audit.md and excluded from task 10's
	// acceptance.
	EvAuditRetryableFailureUnjudged oc.EventID = "oracle: audit failure entry with a retryable reason left unjudged"
	EvAuditFailureOwedChecked       oc.EventID = "oracle: rejected keyed bulk checked for its failure audit entry"
)
