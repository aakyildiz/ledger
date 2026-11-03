// Package pointcheck validates the point reads of task 05 (DESIGN §4.7,
// §5.3): GetAccount, GetTransaction, AggregateVolumes, GetLedgerStats and
// GetLog by global sequence, and serves the logSeqs view the GetLog rule aims
// with. The checks are lifted from the singleton model (M/reads.go,
// M/validate.go, M/aggregates.go, M/stats.go, M/getlog.go); every assertion
// became a returned oraclecheck.Event whose literal lives in the service's
// h_point.go asserter.
//
// Generator intent (absent ledger, colour collapse, model-absent account,
// learned or unassigned sequence) is recomputed from the request and the
// read's snapshot, never read from the hint.
package pointcheck

import oc "github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck"

// Findings (Unreachable). Ported literals: "oracle: " + the M/ suffix.
const (
	EvAccountReadOutsideModel     oc.EventID = "oracle: account read outside model"
	EvGetAccountUnexpectedError   oc.EventID = "oracle: GetAccount returned unexpected error"
	EvTxReadOutsideModel          oc.EventID = "oracle: transaction read outside model"
	EvGetTransactionUnexpectedErr oc.EventID = "oracle: GetTransaction returned unexpected error"

	EvAggOutsideModel             oc.EventID = "oracle: aggregate volumes outside model"
	EvAggMixed                    oc.EventID = "oracle: aggregate result mixes grouped and flat volumes"
	EvAggRepeatsBucket            oc.EventID = "oracle: aggregate result repeats a bucket"
	EvAggServedOutsideFleet       oc.EventID = "oracle: aggregate served a ledger outside the fleet"
	EvAggUnexpectedError          oc.EventID = "oracle: aggregate returned unexpected error"
	EvAggregateVolumesUnexpected  oc.EventID = "oracle: AggregateVolumes returned unexpected error"
	EvAggServedDeletedEverywhere  oc.EventID = "oracle: aggregate served a ledger deleted in every candidate"
	EvStatsBoundsOutsideModel     oc.EventID = "oracle: ledger stats boundaries outside model"
	EvStatsCounterAboveModel      oc.EventID = "oracle: ledger stats counter above the model"
	EvStatsCountedNumscript       oc.EventID = "oracle: ledger stats counted a numscript execution"
	EvStatsServedOutsideFleet     oc.EventID = "oracle: ledger stats served a ledger outside the fleet"
	EvGetLedgerStatsUnexpectedErr oc.EventID = "oracle: GetLedgerStats returned unexpected error"
	EvGetLogAnotherLog            oc.EventID = "oracle: GetLog answered with another log"
	EvGetLogLostCommitted         oc.EventID = "oracle: GetLog lost a committed log"
	EvGetLogServedUnassigned      oc.EventID = "oracle: GetLog served an unassigned sequence"
	EvGetLogUnexpectedError       oc.EventID = "oracle: GetLog returned unexpected error"
)

// Coverage (Reachable), reached by task 05's rules.
const (
	EvAccountAbsentLedgerProbed oc.EventID = "oracle: GetAccount probing an absent ledger"
	EvAccountModelAbsentProbed  oc.EventID = "oracle: GetAccount probing a model-absent account"
	EvCollapsedAccountValidated oc.EventID = "oracle: colour-collapsed account read validated"
	EvTxAbsentLedgerProbed      oc.EventID = "oracle: GetTransaction probing an absent ledger"
	EvAggValidated              oc.EventID = "oracle: aggregate volumes validated"
	EvAggGroupedValidated       oc.EventID = "oracle: grouped aggregate volumes validated"
	EvAggAbsentNotFound         oc.EventID = "oracle: aggregate on an absent ledger returned NotFound"
	EvAggGatedOnMissingIndex    oc.EventID = "oracle: aggregate gated on a missing index"
	EvStatsUsageCaughtUp        oc.EventID = "oracle: ledger stats usage caught up with the model"
	EvStatsValidated            oc.EventID = "oracle: ledger stats validated"
	EvStatsAbsentNotFound       oc.EventID = "oracle: ledger stats on an absent ledger returned NotFound"
	EvGetLogValidated           oc.EventID = "oracle: GetLog validated"
	EvGetLogUnassignedNotFound  oc.EventID = "oracle: GetLog on an unassigned sequence returned NotFound"
)
