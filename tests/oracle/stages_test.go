package oracle

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/big"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/domain/indexes"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/tests/oracle/oracletest"
)

// stageShape is one row of STAGES.md: the smallest bulk that makes the model
// return reason, with the setup it needs, where the server raises the reason
// and the stage the model reports.
type stageShape struct {
	name   string
	reason string
	stage  Stage
	server string // server location
	model  string // model location
	// setup is applied first, each request as its own unkeyed bulk; every
	// fixture shape also yields the same reason when the setup is applied as
	// one bulk (TestStageShapes checks both).
	setup    []*servicepb.Request
	requests []*servicepb.Request
	// base replaces NewGlobalState for shapes that need model configuration
	// the fixture format cannot carry; such shapes are not fixtures.
	base func() GlobalState
	// noFixture explains why a shape is not exported as a rejection fixture
	// (empty = exported).
	noFixture string
}

func stageName(s Stage) string {
	switch s {
	case StageAdmission:
		return "admission"
	case StageFSM:
		return "fsm"
	default:
		return fmt.Sprintf("stage(%d)", s)
	}
}

const fixtureLedger = "L"

func createLedgerReq(mode commonpb.ChartEnforcementMode) *servicepb.Request {
	return &servicepb.Request{Type: &servicepb.Request_CreateLedger{CreateLedger: &servicepb.CreateLedgerRequest{Name: fixtureLedger, DefaultEnforcementMode: mode}}}
}

func createLedgerWith(cl *servicepb.CreateLedgerRequest) *servicepb.Request {
	cl.Name = fixtureLedger

	return &servicepb.Request{Type: &servicepb.Request_CreateLedger{CreateLedger: cl}}
}

func maintenanceReq(on bool) *servicepb.Request {
	return &servicepb.Request{Type: &servicepb.Request_SetMaintenanceMode{SetMaintenanceMode: &servicepb.SetMaintenanceModeRequest{Enabled: on}}}
}

func applyAction(action *servicepb.LedgerAction) *servicepb.Request {
	return &servicepb.Request{Type: &servicepb.Request_Apply{Apply: &servicepb.LedgerApplyRequest{Ledger: fixtureLedger, Action: action}}}
}

func emptyCreateReq() *servicepb.Request {
	return applyAction(&servicepb.LedgerAction{Data: &servicepb.LedgerAction_CreateTransaction{CreateTransaction: &servicepb.CreateTransactionPayload{}}})
}

func stringMeta(v string) *commonpb.MetadataValue {
	return &commonpb.MetadataValue{Type: &commonpb.MetadataValue_StringValue{StringValue: v}}
}

func deleteMetaReq(target *commonpb.Target, key string) *servicepb.Request {
	return applyAction(&servicepb.LedgerAction{Data: &servicepb.LedgerAction_DeleteMetadata{DeleteMetadata: &commonpb.DeleteMetadataCommand{Target: target, Key: key}}})
}

func accountTarget(addr string) *commonpb.Target {
	return &commonpb.Target{Target: &commonpb.Target_Account{Account: &commonpb.TargetAccount{Addr: addr}}}
}

func txTarget(id uint64) *commonpb.Target {
	return &commonpb.Target{Target: &commonpb.Target_TransactionId{TransactionId: id}}
}

func pqCreateRaw(q *commonpb.PreparedQuery) *servicepb.Request {
	return &servicepb.Request{Type: &servicepb.Request_CreatePreparedQuery{CreatePreparedQuery: &servicepb.CreatePreparedQueryRequest{Ledger: fixtureLedger, Query: q}}}
}

func logIDWithoutCond() *commonpb.QueryFilter {
	return &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_LogId{LogId: &commonpb.LogIdCondition{}}}
}

func maxAmountTx(dest string) *servicepb.Request {
	maxU256 := new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 256), big.NewInt(1))

	return applyAction(&servicepb.LedgerAction{Data: &servicepb.LedgerAction_CreateTransaction{CreateTransaction: &servicepb.CreateTransactionPayload{
		Postings: []*commonpb.Posting{commonpb.NewColoredPosting("world", dest, "USD", "", maxU256)},
	}}})
}

func revertAtL(id uint64) *servicepb.Request { return oracletest.RevertReqL(fixtureLedger, id, true) }

// stageShapes is the source of STAGES.md's table and fixture list: one row per
// (reason, check) the model can return.
func stageShapes() []stageShape {
	strict := createLedgerReq(commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_STRICT)
	audit := createLedgerReq(commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_AUDIT)
	accountsK := indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_ACCOUNT, "k")
	validTx := oracletest.TxReq("world", "users:1", "USD", 1)

	return []stageShape{
		// ---- admission ----
		{
			name: "maintenance-gate", reason: domain.ErrReasonMaintenanceMode, stage: StageAdmission,
			server: "admission.go prepareAdmission (MaintenanceMode gate)", model: "model.go Apply (maintenance gate)",
			setup: []*servicepb.Request{audit, maintenanceReq(true)}, requests: []*servicepb.Request{validTx},
			noFixture: "MAINTENANCE_MODE is Unavailable (non-definitive), and the harness's different-body step would need the maintenance lifted; 00a ships its own core maintenance fixture",
		},
		{
			name: "invalid-skippable-reason", reason: domain.ErrReasonValidation, stage: StageAdmission,
			server: "skippable.go extractSkippableReasonsFromApply (ErrInvalidSkippableReason)", model: "model.go admissionReason → skippable.go validSkippableReasons",
			setup:    []*servicepb.Request{audit},
			requests: []*servicepb.Request{skipRequest(oracletest.TxReq("world", "users:1", "USD", 1), commonpb.ErrorReason_ERROR_REASON_INSUFFICIENT_FUNDS)},
		},
		{
			name: "revert-without-target", reason: domain.ErrReasonValidation, stage: StageAdmission,
			server: "admission.go resolveRevertTarget (ErrTransactionTargetMissing)", model: "model.go admissionReason",
			setup: []*servicepb.Request{audit}, requests: []*servicepb.Request{revertAtL(0)},
		},
		{
			name: "empty-create", reason: domain.ErrReasonValidation, stage: StageAdmission,
			server: "validate_order.go validateOrderContent (ErrEmptyTransaction)", model: "model.go admissionReason",
			setup: []*servicepb.Request{audit}, requests: []*servicepb.Request{emptyCreateReq()},
		},
		{
			name: "pq-create-query-required", reason: domain.ErrReasonValidation, stage: StageAdmission,
			server: "validate_order.go validateOrderPreparedQuery (ErrPreparedQueryRequired)", model: "model.go preparedQueryAdmissionReason",
			setup: []*servicepb.Request{audit}, requests: []*servicepb.Request{pqCreateRaw(nil)},
		},
		{
			name: "pq-create-name-required", reason: domain.ErrReasonValidation, stage: StageAdmission,
			server: "validate_order.go validateOrderPreparedQuery (ValidatePreparedQueryName)", model: "model.go preparedQueryAdmissionReason",
			setup:    []*servicepb.Request{audit},
			requests: []*servicepb.Request{pqCreateRaw(&commonpb.PreparedQuery{Target: commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS})},
		},
		{
			name: "pq-create-target-unsupported", reason: domain.ErrReasonValidation, stage: StageAdmission,
			server: "validate_order.go validateOrderPreparedQuery (ErrPreparedQueryTargetUnsupported)", model: "model.go preparedQueryAdmissionReason",
			setup:    []*servicepb.Request{audit},
			requests: []*servicepb.Request{pqCreateRaw(&commonpb.PreparedQuery{Name: "q", Target: commonpb.QueryTarget_QUERY_TARGET_AUDIT})},
		},
		{
			name: "pq-create-filter-invalid-for-target", reason: domain.ErrReasonFilterCompilation, stage: StageAdmission,
			server: "validate_order.go validateOrderPreparedQuery (ValidateFilterForTarget)", model: "model.go preparedQueryAdmissionReason",
			setup:    []*servicepb.Request{audit},
			requests: []*servicepb.Request{pqReq("q", commonpb.QueryTarget_QUERY_TARGET_LOGS, pqFilter("a:"))},
		},
		{
			name: "pq-create-filter-shape", reason: domain.ErrReasonFilterCompilation, stage: StageAdmission,
			server: "validate_order.go validateOrderPreparedQuery (ValidateFilterAdmissionShape)", model: "model.go preparedQueryAdmissionReason",
			setup:    []*servicepb.Request{audit},
			requests: []*servicepb.Request{pqReq("q", commonpb.QueryTarget_QUERY_TARGET_LOGS, logIDWithoutCond())},
		},
		{
			name: "pq-update-name-required", reason: domain.ErrReasonValidation, stage: StageAdmission,
			server: "validate_order.go validateOrderPreparedQuery (ValidatePreparedQueryName)", model: "model.go preparedQueryAdmissionReason",
			setup: []*servicepb.Request{audit}, requests: []*servicepb.Request{pqUpdateReq("", nil)},
		},
		{
			name: "pq-update-filter-shape", reason: domain.ErrReasonFilterCompilation, stage: StageAdmission,
			server: "validate_order.go validateOrderPreparedQuery (ValidateFilterAdmissionShape)", model: "model.go preparedQueryAdmissionReason",
			setup:    []*servicepb.Request{audit, pqReq("q", commonpb.QueryTarget_QUERY_TARGET_LOGS, nil)},
			requests: []*servicepb.Request{pqUpdateReq("q", logIDWithoutCond())},
		},
		{
			name: "pq-delete-name-required", reason: domain.ErrReasonValidation, stage: StageAdmission,
			server: "validate_order.go validateOrderPreparedQuery (ValidatePreparedQueryName)", model: "model.go preparedQueryAdmissionReason",
			setup: []*servicepb.Request{audit}, requests: []*servicepb.Request{pqDeleteReq("")},
		},
		{
			name: "create-ledger-nameless-account-type", reason: domain.ErrReasonInvalidPattern, stage: StageAdmission,
			server: "validate_order.go validateOrderAccountType", model: "model.go admissionReason",
			requests: []*servicepb.Request{createLedgerWith(&servicepb.CreateLedgerRequest{AccountTypes: map[string]*commonpb.AccountType{"": {Pattern: "x:{id}"}}})},
		},

		// ---- FSM: dedup gate ----
		{
			name: "idempotency-key-conflict", reason: domain.ErrReasonIdempotencyKeyConflict, stage: StageFSM,
			server: "machine.go dedup gate (ErrIdempotencyKeyConflict; audited, never frozen: live entry)", model: "model.go applyFSM (dedup gate)",
			noFixture: "needs a prior outcome under the same key; the harness's different-body step covers it",
		},

		// ---- FSM: ledger lifecycle ----
		{
			name: "ledger-already-exists", reason: domain.ErrReasonLedgerAlreadyExists, stage: StageFSM,
			server: "processor_ledger.go processCreateLedger", model: "model.go applyLifecycle (CreateLedger)",
			setup: []*servicepb.Request{audit}, requests: []*servicepb.Request{audit},
		},
		{
			name: "create-ledger-deleted", reason: domain.ErrReasonLedgerDeleted, stage: StageFSM,
			server: "processor_ledger.go processCreateLedger", model: "model.go applyLifecycle (CreateLedger)",
			setup: []*servicepb.Request{audit, {Type: &servicepb.Request_DeleteLedger{DeleteLedger: &servicepb.DeleteLedgerRequest{Name: fixtureLedger}}}}, requests: []*servicepb.Request{audit},
		},
		{
			name: "create-ledger-invalid-pattern", reason: domain.ErrReasonInvalidPattern, stage: StageFSM,
			server: "processor_ledger.go processCreateLedger (accounttype.ValidatePattern)", model: "model.go applyLifecycle (CreateLedger)",
			requests: []*servicepb.Request{createLedgerWith(&servicepb.CreateLedgerRequest{AccountTypes: map[string]*commonpb.AccountType{"t": {Pattern: "{"}}})},
		},
		{
			name: "apply-on-deleted-ledger", reason: domain.ErrReasonLedgerDeleted, stage: StageFSM,
			server: "processor_apply.go processApply", model: "model.go applyFSM (lifecycle Deleted)",
			setup: []*servicepb.Request{audit, {Type: &servicepb.Request_DeleteLedger{DeleteLedger: &servicepb.DeleteLedgerRequest{Name: fixtureLedger}}}}, requests: []*servicepb.Request{validTx},
		},
		{
			name: "apply-on-mirror-ledger", reason: domain.ErrReasonLedgerInMirrorMode, stage: StageFSM,
			server: "processor_apply.go processApply", model: "model.go applyFSM (mirrorSafeRequest)",
			setup:     []*servicepb.Request{createLedgerWith(&servicepb.CreateLedgerRequest{Mode: commonpb.LedgerMode_LEDGER_MODE_MIRROR})},
			requests:  []*servicepb.Request{validTx},
			noFixture: "a mirror ledger needs a valid mirrorSource (admission validateOrderMirrorSource) and starts a mirror worker",
		},
		{
			name: "promote-unknown-ledger", reason: domain.ErrReasonLedgerNotFound, stage: StageFSM,
			server: "processor_mirror.go processPromoteLedger → scope_helpers.go loadLedger", model: "model.go applyLifecycle (PromoteLedger)",
			requests: []*servicepb.Request{{Type: &servicepb.Request_PromoteLedger{PromoteLedger: &servicepb.PromoteLedgerRequest{Ledger: fixtureLedger}}}},
		},
		{
			name: "promote-normal-ledger", reason: domain.ErrReasonLedgerNotInMirrorMode, stage: StageFSM,
			server: "processor_mirror.go processPromoteLedger", model: "model.go applyLifecycle (PromoteLedger)",
			setup: []*servicepb.Request{audit}, requests: []*servicepb.Request{{Type: &servicepb.Request_PromoteLedger{PromoteLedger: &servicepb.PromoteLedgerRequest{Ledger: fixtureLedger}}}},
		},
		{
			name: "delete-unknown-ledger", reason: domain.ErrReasonLedgerNotFound, stage: StageFSM,
			server: "processor_ledger.go processDeleteLedger → scope_helpers.go loadLedger", model: "model.go applyLifecycle (DeleteLedger)",
			requests: []*servicepb.Request{{Type: &servicepb.Request_DeleteLedger{DeleteLedger: &servicepb.DeleteLedgerRequest{Name: fixtureLedger}}}},
		},

		// ---- FSM: query checkpoints ----
		{
			name: "checkpoint-limit-reached", reason: domain.ErrReasonCheckpointLimitReached, stage: StageFSM,
			server: "processor_query_checkpoint.go processCreateQueryCheckpoint", model: "model.go applyCheckpoint",
			base:      func() GlobalState { return NewGlobalState().WithQueryCheckpointLimit(1) },
			setup:     []*servicepb.Request{createCheckpointRequest()},
			requests:  []*servicepb.Request{createCheckpointRequest()},
			noFixture: "depends on the server's QUERY_CHECKPOINT_LIMIT policy, which the fixture format does not carry",
		},
		{
			name: "checkpoint-id-required", reason: domain.ErrReasonCheckpointIDRequired, stage: StageFSM,
			server: "processor_query_checkpoint.go processDeleteQueryCheckpoint", model: "model.go applyCheckpoint",
			requests: []*servicepb.Request{deleteCheckpointRequest(0)},
		},
		{
			name: "checkpoint-not-found", reason: domain.ErrReasonCheckpointNotFound, stage: StageFSM,
			server: "processor_query_checkpoint.go processDeleteQueryCheckpoint", model: "model.go applyCheckpoint",
			requests: []*servicepb.Request{deleteCheckpointRequest(999)},
		},
		{
			name: "invalid-cron-expression", reason: domain.ErrReasonInvalidCronExpression, stage: StageFSM,
			server: "processor_query_checkpoint_schedule.go processSetQueryCheckpointSchedule", model: "model.go applyCheckpoint",
			requests: []*servicepb.Request{scheduleCheckpointRequest("not a cron")},
		},

		// ---- FSM: chart ----
		{
			name: "account-type-already-exists", reason: domain.ErrReasonAccountTypeAlreadyExists, stage: StageFSM,
			server: "processor_account_type.go processAddAccountType", model: "model.go applyOne (AddAccountType)",
			setup: []*servicepb.Request{audit, oracletest.AddTypeReq("users")}, requests: []*servicepb.Request{oracletest.AddTypeReq("users")},
		},
		{
			name: "account-type-not-found", reason: domain.ErrReasonAccountTypeNotFound, stage: StageFSM,
			server: "processor_account_type.go processRemoveAccountType", model: "model.go applyOne (RemoveAccountType)",
			setup: []*servicepb.Request{audit}, requests: []*servicepb.Request{oracletest.RemoveTypeReq("users")},
		},

		// ---- FSM: transactions ----
		{
			name: "reference-conflict", reason: domain.ErrReasonTransactionReferenceConflict, stage: StageFSM,
			server: "processor_transaction.go processCreateTransaction", model: "model.go applyTransaction",
			setup:    []*servicepb.Request{audit, oracletest.TxReqRefL(fixtureLedger, "r", "world", "users:1", "USD", 1)},
			requests: []*servicepb.Request{oracletest.TxReqRefL(fixtureLedger, "r", "world", "users:1", "USD", 1)},
		},
		{
			name: "insufficient-funds", reason: domain.ErrReasonInsufficientFunds, stage: StageFSM,
			server: "processor_posting.go applyPosting", model: "model.go applyPostings",
			setup: []*servicepb.Request{audit}, requests: []*servicepb.Request{oracletest.TxReq("users:1", "users:2", "USD", 1)},
		},
		{
			name: "volume-overflow", reason: domain.ErrReasonVolumeOverflow, stage: StageFSM,
			server: "processor_posting.go applyPosting", model: "model.go applyPostings",
			setup: []*servicepb.Request{audit, maxAmountTx("users:1")}, requests: []*servicepb.Request{oracletest.TxReq("world", "users:1", "USD", 1)},
		},
		{
			name: "tx-account-not-matching-type", reason: domain.ErrReasonAccountNotMatchingType, stage: StageFSM,
			server: "processor_account_type.go validateAccountAgainstAccountTypes", model: "model.go applyTransaction (chartRejects)",
			setup: []*servicepb.Request{strict, oracletest.AddTypeReq("users")}, requests: []*servicepb.Request{oracletest.TxReq("world", "other:1", "USD", 1)},
		},
		{
			name: "account-metadata-not-matching-type", reason: domain.ErrReasonAccountNotMatchingType, stage: StageFSM,
			server: "processor_account_type.go validateAccountAgainstAccountTypes", model: "model.go applyAddAccountMetadata",
			setup:    []*servicepb.Request{strict, oracletest.AddTypeReq("users")},
			requests: []*servicepb.Request{oracletest.AddAccountMetaReq("other:1", "k", stringMeta("v"))},
		},
		{
			name: "revert-unknown-transaction", reason: domain.ErrReasonTransactionNotFound, stage: StageFSM,
			server: "processor_revert_transaction.go processRevertTransaction", model: "model.go applyRevert",
			setup: []*servicepb.Request{audit}, requests: []*servicepb.Request{revertAtL(7)},
		},
		{
			name: "revert-already-reverted", reason: domain.ErrReasonTransactionAlreadyReverted, stage: StageFSM,
			server: "processor_revert_transaction.go processRevertTransaction", model: "model.go applyRevert",
			setup:     []*servicepb.Request{audit, oracletest.TxReq("world", "users:1", "USD", 1), revertAtL(1)},
			requests:  []*servicepb.Request{revertAtL(1)},
			noFixture: "its setup must be applied as separate bulks (a revert of a transaction created in the same bulk is REVERT_TARGET_CREATED_IN_BATCH)",
		},
		{
			name: "revert-target-created-in-batch", reason: domain.ErrReasonRevertTargetCreatedInBatch, stage: StageFSM,
			server: "processor_revert_transaction.go checkRevertTargetObservation", model: "model.go applyRevert",
			setup: []*servicepb.Request{audit}, requests: []*servicepb.Request{oracletest.TxReq("world", "users:1", "USD", 1), revertAtL(1)},
		},
		{
			name: "transient-account-non-zero", reason: domain.ErrReasonTransientAccountNonZero, stage: StageFSM,
			server: "write_set.go ValidateTransientVolumes (machine.go end-of-bulk check)", model: "model.go transientViolation",
			setup:    []*servicepb.Request{audit, oracletest.AddTypeReqP("users", commonpb.AccountTypePersistence_ACCOUNT_TYPE_TRANSIENT)},
			requests: []*servicepb.Request{oracletest.TxReq("world", "users:1", "USD", 1)},
		},

		// ---- FSM: metadata ----
		{
			name: "tx-metadata-without-target", reason: domain.ErrReasonValidation, stage: StageFSM,
			server: "processor_metadata.go validateTransactionTarget (ErrTransactionTargetMissing)", model: "model.go applyAddTxMetadata",
			setup: []*servicepb.Request{audit}, requests: []*servicepb.Request{oracletest.AddTxMetaReq(0, map[string]*commonpb.MetadataValue{"k": stringMeta("v")})},
		},
		{
			name: "tx-metadata-unknown-transaction", reason: domain.ErrReasonTransactionNotFound, stage: StageFSM,
			server: "processor_metadata.go validateTransactionTarget", model: "model.go applyAddTxMetadata",
			setup: []*servicepb.Request{audit}, requests: []*servicepb.Request{oracletest.AddTxMetaReq(7, map[string]*commonpb.MetadataValue{"k": stringMeta("v")})},
		},
		{
			name: "delete-tx-metadata-without-target", reason: domain.ErrReasonValidation, stage: StageFSM,
			server: "processor_metadata.go validateTransactionTarget (ErrTransactionTargetMissing)", model: "model.go applyDeleteMetadata",
			setup: []*servicepb.Request{audit}, requests: []*servicepb.Request{deleteMetaReq(txTarget(0), "k")},
		},
		{
			name: "delete-tx-metadata-unknown-transaction", reason: domain.ErrReasonTransactionNotFound, stage: StageFSM,
			server: "processor_metadata.go validateTransactionTarget", model: "model.go applyDeleteMetadata",
			setup: []*servicepb.Request{audit}, requests: []*servicepb.Request{deleteMetaReq(txTarget(7), "k")},
		},
		{
			name: "delete-account-metadata-missing", reason: domain.ErrReasonMetadataNotFound, stage: StageFSM,
			server: "processor_metadata.go processDeleteMetadata", model: "model.go applyDeleteMetadata",
			setup: []*servicepb.Request{audit}, requests: []*servicepb.Request{deleteMetaReq(accountTarget("users:1"), "k")},
		},
		{
			name: "delete-tx-metadata-missing", reason: domain.ErrReasonMetadataNotFound, stage: StageFSM,
			server: "processor_metadata.go processDeleteMetadata", model: "model.go applyDeleteMetadata",
			setup: []*servicepb.Request{audit, oracletest.TxReq("world", "users:1", "USD", 1)}, requests: []*servicepb.Request{deleteMetaReq(txTarget(1), "k")},
		},
		{
			name: "delete-ledger-metadata-missing", reason: domain.ErrReasonMetadataNotFound, stage: StageFSM,
			server: "processor_ledger_metadata.go processDeleteLedgerMetadata", model: "model.go applyDeleteLedgerMetadata",
			setup:    []*servicepb.Request{audit},
			requests: []*servicepb.Request{{Type: &servicepb.Request_DeleteLedgerMetadata{DeleteLedgerMetadata: &servicepb.DeleteLedgerMetadataRequest{Ledger: fixtureLedger, Key: "k"}}}},
		},

		// ---- FSM: indexes ----
		{
			name: "index-already-exists", reason: domain.ErrReasonIndexAlreadyExists, stage: StageFSM,
			server: "processor_index.go processCreateIndex", model: "model.go applyCreateIndex",
			setup: []*servicepb.Request{
				audit,
				oracletest.SetFieldTypeReq(commonpb.TargetType_TARGET_TYPE_ACCOUNT, "k", commonpb.MetadataType_METADATA_TYPE_STRING),
				oracletest.CreateIndexReq(accountsK),
			},
			requests: []*servicepb.Request{oracletest.CreateIndexReq(accountsK)},
		},
		{
			name: "index-field-not-in-schema", reason: domain.ErrReasonMetadataFieldNotInSchema, stage: StageFSM,
			server: "processor_index.go validateIndexTarget", model: "model.go applyCreateIndex",
			setup: []*servicepb.Request{audit}, requests: []*servicepb.Request{oracletest.CreateIndexReq(accountsK)},
		},

		// ---- FSM: prepared queries ----
		{
			name: "pq-already-exists", reason: domain.ErrReasonPreparedQueryAlreadyExists, stage: StageFSM,
			server: "processor_prepared_query.go processCreatePreparedQuery", model: "model.go applyCreatePreparedQuery",
			setup:    []*servicepb.Request{audit, pqReq("q", commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS, nil)},
			requests: []*servicepb.Request{pqReq("q", commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS, nil)},
		},
		{
			name: "pq-update-not-found", reason: domain.ErrReasonPreparedQueryNotFound, stage: StageFSM,
			server: "processor_prepared_query.go processUpdatePreparedQuery", model: "model.go applyUpdatePreparedQuery",
			setup: []*servicepb.Request{audit}, requests: []*servicepb.Request{pqUpdateReq("q", nil)},
		},
		{
			name: "pq-update-filter-invalid-for-stored-target", reason: domain.ErrReasonFilterCompilation, stage: StageFSM,
			server:   "processor_prepared_query.go processUpdatePreparedQuery:129 (ValidateFilterForTarget; *ErrFilterCompilation is an UncommittedFailure: not audited, not frozen)",
			model:    "model.go applyUpdatePreparedQuery",
			setup:    []*servicepb.Request{audit, pqReq("q", commonpb.QueryTarget_QUERY_TARGET_LOGS, nil)},
			requests: []*servicepb.Request{pqUpdateReq("q", pqFilter("a:"))},
		},
		{
			name: "pq-delete-not-found", reason: domain.ErrReasonPreparedQueryNotFound, stage: StageFSM,
			server: "processor_prepared_query.go processDeletePreparedQuery", model: "model.go applyDeletePreparedQuery",
			setup: []*servicepb.Request{audit}, requests: []*servicepb.Request{pqDeleteReq("q")},
		},
	}
}

func (s stageShape) baseState() GlobalState {
	if s.base != nil {
		return s.base()
	}

	return NewGlobalState()
}

// TestStageShapes builds every row's bulk and requires the documented reason
// and stage, and the freeze decision that follows from them.
func TestStageShapes(t *testing.T) {
	t.Parallel()

	seen := map[string]bool{}
	for _, shape := range stageShapes() {
		require.False(t, seen[shape.name], "duplicate shape %s", shape.name)
		seen[shape.name] = true

		t.Run(shape.name, func(t *testing.T) {
			t.Parallel()

			g := shape.baseState()
			for _, req := range shape.setup {
				g = applyOK(t, g, bulkOf(req))
			}

			bulk := keyedBulk("k1", shape.requests...)
			if shape.name == "idempotency-key-conflict" {
				g = applyOK(t, g, keyedBulk("k1", oracletest.TxReq("world", "users:1", "USD", 1)))
				bulk = keyedBulk("k1", oracletest.TxReq("world", "users:1", "USD", 2))
			}

			res := g.Apply(bulk)
			require.False(t, res.OK)
			require.Equal(t, shape.reason, res.Reason)
			require.Equal(t, stageName(shape.stage), stageName(res.Stage))
			require.False(t, res.Replayed)
			require.Equal(t, g.Fingerprint(), res.State.Fingerprint())

			frozen := g.RecordFailure(bulk, res)
			wantFrozen := shape.stage == StageFSM && !UncommittedReasons[shape.reason] &&
				isFreezableReason(shape.reason) && shape.reason != domain.ErrReasonIdempotencyKeyConflict
			require.Equal(t, wantFrozen, frozen.Fingerprint() != g.Fingerprint(), "freeze decision")
			if wantFrozen {
				requireOutcome(t, outcome{reason: shape.reason, stage: StageFSM, replayed: true}, frozen.Apply(bulk))
			}

			if shape.noFixture != "" || len(shape.setup) == 0 {
				return
			}
			// The fixture harness may apply the setup as one bulk: it must
			// lead to the same rejection.
			one := applyOK(t, shape.baseState(), bulkOf(shape.setup...))
			again := one.Apply(bulk)
			require.Equal(t, shape.reason, again.Reason, "setup applied as one bulk")
			require.Equal(t, shape.stage, again.Stage)
		})
	}
}

// TestStagesDocument requires STAGES.md's generated sections to match
// stageShapes, so the table, the fixture list and this test cannot drift.
// Regenerate with UPDATE_STAGES=1 go test -run TestStagesDocument.
func TestStagesDocument(t *testing.T) {
	t.Parallel()

	doc, err := os.ReadFile("STAGES.md")
	require.NoError(t, err)

	updated := string(doc)
	updated = replaceSection(t, updated, "table", renderStageTable())
	updated = replaceSection(t, updated, "fixtures", "```json\n"+renderFixtures(t)+"```\n")

	if os.Getenv("UPDATE_STAGES") != "" {
		require.NoError(t, os.WriteFile("STAGES.md", []byte(updated), 0o600))

		return
	}
	require.Equal(t, updated, string(doc), "STAGES.md is stale; rerun with UPDATE_STAGES=1")
}

func replaceSection(t *testing.T, doc, name, body string) string {
	t.Helper()
	begin := "<!-- " + name + ":begin -->\n"
	end := "<!-- " + name + ":end -->"
	i := strings.Index(doc, begin)
	j := strings.Index(doc, end)
	require.True(t, i >= 0 && j > i, "STAGES.md lacks the %s markers", name)

	return doc[:i+len(begin)] + body + doc[j:]
}

func renderStageTable() string {
	var b strings.Builder
	b.WriteString("| Shape | Reason | Server location | Stage | Model location | Fixture |\n")
	b.WriteString("|---|---|---|---|---|---|\n")
	for _, s := range stageShapes() {
		fixture := "`01-" + s.name + "`"
		if s.noFixture != "" {
			fixture = "no: " + s.noFixture
		}
		fmt.Fprintf(&b, "| %s | `%s` | %s | %s | %s | %s |\n", s.name, s.reason, s.server, stageName(s.stage), s.model, fixture)
	}

	return b.String()
}

// renderFixtures renders the fixture-ready shapes in the
// W/oraclecheck/testdata/rejections format: {"name","setup","requests",
// "expectReason"}, requests as servicepb.Request protojson. protojson output is
// deliberately unstable, so it is normalised through encoding/json.
func renderFixtures(t *testing.T) string {
	t.Helper()

	toJSON := func(reqs []*servicepb.Request) []any {
		out := make([]any, 0, len(reqs))
		for _, req := range reqs {
			raw, err := protojson.Marshal(req)
			require.NoError(t, err)
			var v any
			require.NoError(t, json.Unmarshal(raw, &v))
			out = append(out, v)
		}

		return out
	}

	type fixture struct {
		Name         string `json:"name"`
		Setup        []any  `json:"setup"`
		Requests     []any  `json:"requests"`
		ExpectReason string `json:"expectReason"`
	}
	var fixtures []fixture
	for _, s := range stageShapes() {
		if s.noFixture != "" {
			continue
		}
		fixtures = append(fixtures, fixture{Name: "01-" + s.name, Setup: toJSON(s.setup), Requests: toJSON(s.requests), ExpectReason: s.reason})
	}

	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetIndent("", "  ")
	require.NoError(t, enc.Encode(fixtures))

	return buf.String()
}

// TestStageShapesRoundTrip decodes the fixture list back through protojson
// and replays it, so the JSON 00d copies is what the shapes test checked.
func TestStageShapesRoundTrip(t *testing.T) {
	t.Parallel()

	var fixtures []struct {
		Name         string            `json:"name"`
		Setup        []json.RawMessage `json:"setup"`
		Requests     []json.RawMessage `json:"requests"`
		ExpectReason string            `json:"expectReason"`
	}
	require.NoError(t, json.Unmarshal([]byte(renderFixtures(t)), &fixtures))
	require.NotEmpty(t, fixtures)

	decode := func(raws []json.RawMessage) []*servicepb.Request {
		out := make([]*servicepb.Request, 0, len(raws))
		for _, raw := range raws {
			req := &servicepb.Request{}
			require.NoError(t, protojson.Unmarshal(raw, req))
			out = append(out, req)
		}

		return out
	}

	for _, f := range fixtures {
		g := NewGlobalState()
		for _, req := range decode(f.Setup) {
			g = applyOK(t, g, bulkOf(req))
		}
		res := g.Apply(keyedBulk("k1", decode(f.Requests)...))
		require.Equal(t, f.ExpectReason, res.Reason, f.Name)
	}
}
