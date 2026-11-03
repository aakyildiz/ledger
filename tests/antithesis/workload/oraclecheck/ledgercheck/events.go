// Package ledgercheck is task 04's oracle family: the ledger read kinds
// (GetLedger, GetMetadataSchemaStatus, ListLedgers) and the lifecycle and
// apply-surface coverage hooks, lifted from the singleton model
// (bin/cmds/model/singleton_driver_model reads.go, validate.go,
// ledgers_list.go, ledger_info.go, apply_surface.go). It never asserts: every
// check returns oraclecheck Events, asserted by bin/oracle/h_ledgers.go.
package ledgercheck

import oc "github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck"

// Read-kind names (DESIGN §5.3).
const (
	KindGetLedger    = "GetLedger"
	KindSchemaStatus = "GetMetadataSchemaStatus"
	KindListLedgers  = "ListLedgers"
)

// GetLedger (M/reads.go runLedgerRead, M/validate.go validateLedgerRead).
const (
	EvLcLedgerReadOutsideModel oc.EventID = "oracle: ledger read outside model"
	EvLcGetLedgerServedAbsent  oc.EventID = "oracle: GetLedger served a ledger outside the fleet"
	EvLcGetLedgerUnexpected    oc.EventID = "oracle: GetLedger returned unexpected error"
	EvLcGetLedgerAbsentProbed  oc.EventID = "oracle: GetLedger on an absent ledger returned NotFound"
)

// GetMetadataSchemaStatus (M/reads.go runSchemaRead, M/validate.go validateSchemaRead).
const (
	EvLcSchemaReadOutsideModel oc.EventID = "oracle: metadata schema read outside model"
	EvLcSchemaServedAbsent     oc.EventID = "oracle: GetMetadataSchemaStatus served a ledger outside the fleet"
	EvLcSchemaUnexpected       oc.EventID = "oracle: GetMetadataSchemaStatus returned unexpected error"
	EvLcSchemaAbsentProbed     oc.EventID = "oracle: GetMetadataSchemaStatus on an absent ledger returned NotFound"
)

// ListLedgers (M/ledgers_list.go runLedgersList, M/ledger_info.go).
const (
	EvLcListingNotWindow        oc.EventID = "oracle: ledger listing is not the fleet's window"
	EvLcListingOutsideModel     oc.EventID = "oracle: ledger listing outside model"
	EvLcListingIdentity         oc.EventID = "oracle: listed ledger identity is not the one creation reported"
	EvLcListingRepeated         oc.EventID = "oracle: ledger listing repeated a ledger"
	EvLcListingMalformed        oc.EventID = "oracle: listed ledger is malformed"
	EvLcListingReusedID         oc.EventID = "oracle: ledger listing reused an id"
	EvLcListingAcceptedFilter   oc.EventID = "oracle: ledger listing accepted an unsupported filter"
	EvLcListingUnexpected       oc.EventID = "oracle: ListLedgers returned unexpected error"
	EvLcListingValidated        oc.EventID = "oracle: ledger listing validated"
	EvLcListingTruncated        oc.EventID = "oracle: ledger listing page truncated"
	EvLcListingResumedAtDeleted oc.EventID = "oracle: ledger listing resumed at a deleted ledger"
	EvLcListingRejectedFilter   oc.EventID = "oracle: ledger listing rejected an unsupported filter"
)

// Data-driven coverage probes (RegisterCoverage, emitted with AssertRaw):
// M/coverage.go coverageDeletionMessage / coveragePromotionMessage and
// M/apply_surface.go applyCoverageMessages for the orders this family
// generates. The reason and mode names are the short forms of the task spec.
const (
	EvLcCovDeletion           oc.EventID = "oracle: coverage ledger deletion and reserved-name rejection verified"
	EvLcCovPromotion          oc.EventID = "oracle: coverage mirror promotion and write recovery verified"
	EvLcCovTopLevelStrict     oc.EventID = "oracle: coverage Apply top-level set STRICT"
	EvLcCovTopLevelAudit      oc.EventID = "oracle: coverage Apply top-level set AUDIT"
	EvLcCovLedgerActionStrict oc.EventID = "oracle: coverage Apply ledger-action set STRICT"
	EvLcCovLedgerActionAudit  oc.EventID = "oracle: coverage Apply ledger-action set AUDIT"
	EvLcCovSkipTypeExists     oc.EventID = "oracle: coverage Apply skipped ACCOUNT_TYPE_ALREADY_EXISTS and continued"
	EvLcCovSkipTypeNotFound   oc.EventID = "oracle: coverage Apply skipped ACCOUNT_TYPE_NOT_FOUND and continued"
	EvLcCovInvalidSkip        oc.EventID = "oracle: coverage Apply rejected invalid skip opt-in"
)
