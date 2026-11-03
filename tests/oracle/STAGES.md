# Rejection stages of the model (`tests/oracle`)

Every reason `GlobalState.Apply` can return, where the server raises it, and the
`ApplyResult.Stage` the model reports. The stage decides freezing
(`GlobalState.RecordFailure`, mirroring `recordIdempotencyFailure` in
`internal/infra/state/machine.go`):

- **admission** — rejected before raft by `internal/application/admission`
  (`admission.go` maintenance gate, `requestToOrder`/`applyRequestToOrder`,
  `skippable.go`, `validate_order.go`). No audit entry, nothing frozen; a later
  valid body under the same key applies normally.
- **fsm** — produced by the apply path. A failure is audited and frozen when its
  kind is freezable (`domain.IsFreezableFailure(domain.KindForReason(...))`:
  Validation, NotFound, AlreadyExists, Conflict, Precondition), it is not a
  replay, no live outcome holds the key (so `IDEMPOTENCY_KEY_CONFLICT` is audited
  but never frozen), and its reason is not in `oracle.UncommittedReasons`
  (a `domain.UncommittedFailure`: neither audited nor frozen, re-evaluated per
  attempt; here only `FILTER_COMPILATION_ERROR` from an update).

How the list was built: every `Reason:` assignment and `domain.ErrReason*` /
`.Reason()` use in `model.go` (plus the reasons `skippable.go` and the
prepared-query validators return through `err.Reason()`), each traced to the
server function that raises it for the same input. A reason raised in both
places (`VALIDATION`, `FILTER_COMPILATION_ERROR`, `INVALID_PATTERN`,
`LEDGER_DELETED`, `TRANSACTION_NOT_FOUND`) has one row per check, with the stage
of the check the model performs.

The table and the fixture list below are generated from `stageShapes()` in
`stages_test.go` (`UPDATE_STAGES=1 go test -run TestStagesDocument
./tests/oracle/`); `TestStagesDocument` fails if they are stale and
`TestStageShapes` applies every row's bulk to the model and checks reason, stage
and freeze decision. The server half of the check is 00a's `TestRejectionStages`
over the fixtures.

## Model fixes made while auditing

- A revert with `transactionId: 0` is rejected by admission
  (`resolveRevertTarget`, `ErrTransactionTargetMissing`, `VALIDATION`); the
  model returned `TRANSACTION_NOT_FOUND` from the FSM.
- Transaction-target metadata writes and deletes with id 0 fail in the FSM with
  `VALIDATION` (`processor_metadata.go validateTransactionTarget`); the model
  returned `TRANSACTION_NOT_FOUND`.
- A `CreateLedger` account type with an empty name is an admission
  `INVALID_PATTERN` (`validateOrderAccountType`); the model accepted it.

## Known gaps (not reasons the model returns)

These server rejections are not modelled; a generator must not produce them on
tracked ledgers (an occurrence is reported as unexplained):

- admission: metadata shape and size limits (`ValidateOrderMetadataShape`,
  `validateCommandMetadata` → `METADATA_LIMIT_EXCEEDED`, `VALIDATION`), ledger
  name validation, account address validation, reference/name/pattern length
  limits, posting colors, numscript checks, `INDEX_TARGET_UNSUPPORTED`
  (`VALIDATION`), mirror-source checks, `ErrCheckpointOrderNotLast` (no reason),
  `INDEX_BUILDING` and `AUDIT_DISABLED` from the audit-projection gate;
- FSM: `INVALID_PATTERN` / `ACCOUNT_TYPE_CONFLICT` on `AddAccountType`,
  `ACCOUNT_TYPE_HAS_ACCOUNTS`, `LEDGER_NOT_FOUND` for ledger-scoped orders on a
  never-created ledger (the model creates ledgers implicitly), metadata limits at
  apply, numscript outcomes;
- `NUMSCRIPT_COMPILE_ERROR` is in `UncommittedReasons` because the unclassified
  compile error is uncommitted, although the classified one is frozen; the model
  never returns it.

## Table

<!-- table:begin -->
| Shape | Reason | Server location | Stage | Model location | Fixture |
|---|---|---|---|---|---|
| maintenance-gate | `MAINTENANCE_MODE` | admission.go prepareAdmission (MaintenanceMode gate) | admission | model.go Apply (maintenance gate) | no: MAINTENANCE_MODE is Unavailable (non-definitive), and the harness's different-body step would need the maintenance lifted; 00a ships its own core maintenance fixture |
| invalid-skippable-reason | `VALIDATION` | skippable.go extractSkippableReasonsFromApply (ErrInvalidSkippableReason) | admission | model.go admissionReason → skippable.go validSkippableReasons | `01-invalid-skippable-reason` |
| revert-without-target | `VALIDATION` | admission.go resolveRevertTarget (ErrTransactionTargetMissing) | admission | model.go admissionReason | `01-revert-without-target` |
| empty-create | `VALIDATION` | validate_order.go validateOrderContent (ErrEmptyTransaction) | admission | model.go admissionReason | `01-empty-create` |
| pq-create-query-required | `VALIDATION` | validate_order.go validateOrderPreparedQuery (ErrPreparedQueryRequired) | admission | model.go preparedQueryAdmissionReason | `01-pq-create-query-required` |
| pq-create-name-required | `VALIDATION` | validate_order.go validateOrderPreparedQuery (ValidatePreparedQueryName) | admission | model.go preparedQueryAdmissionReason | `01-pq-create-name-required` |
| pq-create-target-unsupported | `VALIDATION` | validate_order.go validateOrderPreparedQuery (ErrPreparedQueryTargetUnsupported) | admission | model.go preparedQueryAdmissionReason | `01-pq-create-target-unsupported` |
| pq-create-filter-invalid-for-target | `FILTER_COMPILATION_ERROR` | validate_order.go validateOrderPreparedQuery (ValidateFilterForTarget) | admission | model.go preparedQueryAdmissionReason | `01-pq-create-filter-invalid-for-target` |
| pq-create-filter-shape | `FILTER_COMPILATION_ERROR` | validate_order.go validateOrderPreparedQuery (ValidateFilterAdmissionShape) | admission | model.go preparedQueryAdmissionReason | `01-pq-create-filter-shape` |
| pq-update-name-required | `VALIDATION` | validate_order.go validateOrderPreparedQuery (ValidatePreparedQueryName) | admission | model.go preparedQueryAdmissionReason | `01-pq-update-name-required` |
| pq-update-filter-shape | `FILTER_COMPILATION_ERROR` | validate_order.go validateOrderPreparedQuery (ValidateFilterAdmissionShape) | admission | model.go preparedQueryAdmissionReason | `01-pq-update-filter-shape` |
| pq-delete-name-required | `VALIDATION` | validate_order.go validateOrderPreparedQuery (ValidatePreparedQueryName) | admission | model.go preparedQueryAdmissionReason | `01-pq-delete-name-required` |
| create-ledger-nameless-account-type | `INVALID_PATTERN` | validate_order.go validateOrderAccountType | admission | model.go admissionReason | `01-create-ledger-nameless-account-type` |
| idempotency-key-conflict | `IDEMPOTENCY_KEY_CONFLICT` | machine.go dedup gate (ErrIdempotencyKeyConflict; audited, never frozen: live entry) | fsm | model.go applyFSM (dedup gate) | no: needs a prior outcome under the same key; the harness's different-body step covers it |
| ledger-already-exists | `LEDGER_ALREADY_EXISTS` | processor_ledger.go processCreateLedger | fsm | model.go applyLifecycle (CreateLedger) | `01-ledger-already-exists` |
| create-ledger-deleted | `LEDGER_DELETED` | processor_ledger.go processCreateLedger | fsm | model.go applyLifecycle (CreateLedger) | `01-create-ledger-deleted` |
| create-ledger-invalid-pattern | `INVALID_PATTERN` | processor_ledger.go processCreateLedger (accounttype.ValidatePattern) | fsm | model.go applyLifecycle (CreateLedger) | `01-create-ledger-invalid-pattern` |
| apply-on-deleted-ledger | `LEDGER_DELETED` | processor_apply.go processApply | fsm | model.go applyFSM (lifecycle Deleted) | `01-apply-on-deleted-ledger` |
| apply-on-mirror-ledger | `LEDGER_IN_MIRROR_MODE` | processor_apply.go processApply | fsm | model.go applyFSM (mirrorSafeRequest) | no: a mirror ledger needs a valid mirrorSource (admission validateOrderMirrorSource) and starts a mirror worker |
| promote-unknown-ledger | `LEDGER_NOT_FOUND` | processor_mirror.go processPromoteLedger → scope_helpers.go loadLedger | fsm | model.go applyLifecycle (PromoteLedger) | `01-promote-unknown-ledger` |
| promote-normal-ledger | `LEDGER_NOT_IN_MIRROR_MODE` | processor_mirror.go processPromoteLedger | fsm | model.go applyLifecycle (PromoteLedger) | `01-promote-normal-ledger` |
| delete-unknown-ledger | `LEDGER_NOT_FOUND` | processor_ledger.go processDeleteLedger → scope_helpers.go loadLedger | fsm | model.go applyLifecycle (DeleteLedger) | `01-delete-unknown-ledger` |
| checkpoint-limit-reached | `CHECKPOINT_LIMIT_REACHED` | processor_query_checkpoint.go processCreateQueryCheckpoint | fsm | model.go applyCheckpoint | no: depends on the server's QUERY_CHECKPOINT_LIMIT policy, which the fixture format does not carry |
| checkpoint-id-required | `CHECKPOINT_ID_REQUIRED` | processor_query_checkpoint.go processDeleteQueryCheckpoint | fsm | model.go applyCheckpoint | `01-checkpoint-id-required` |
| checkpoint-not-found | `CHECKPOINT_NOT_FOUND` | processor_query_checkpoint.go processDeleteQueryCheckpoint | fsm | model.go applyCheckpoint | `01-checkpoint-not-found` |
| invalid-cron-expression | `INVALID_CRON_EXPRESSION` | processor_query_checkpoint_schedule.go processSetQueryCheckpointSchedule | fsm | model.go applyCheckpoint | `01-invalid-cron-expression` |
| account-type-already-exists | `ACCOUNT_TYPE_ALREADY_EXISTS` | processor_account_type.go processAddAccountType | fsm | model.go applyOne (AddAccountType) | `01-account-type-already-exists` |
| account-type-not-found | `ACCOUNT_TYPE_NOT_FOUND` | processor_account_type.go processRemoveAccountType | fsm | model.go applyOne (RemoveAccountType) | `01-account-type-not-found` |
| reference-conflict | `TRANSACTION_REFERENCE_CONFLICT` | processor_transaction.go processCreateTransaction | fsm | model.go applyTransaction | `01-reference-conflict` |
| insufficient-funds | `INSUFFICIENT_FUNDS` | processor_posting.go applyPosting | fsm | model.go applyPostings | `01-insufficient-funds` |
| volume-overflow | `VOLUME_OVERFLOW` | processor_posting.go applyPosting | fsm | model.go applyPostings | `01-volume-overflow` |
| tx-account-not-matching-type | `ACCOUNT_NOT_MATCHING_TYPE` | processor_account_type.go validateAccountAgainstAccountTypes | fsm | model.go applyTransaction (chartRejects) | `01-tx-account-not-matching-type` |
| account-metadata-not-matching-type | `ACCOUNT_NOT_MATCHING_TYPE` | processor_account_type.go validateAccountAgainstAccountTypes | fsm | model.go applyAddAccountMetadata | `01-account-metadata-not-matching-type` |
| revert-unknown-transaction | `TRANSACTION_NOT_FOUND` | processor_revert_transaction.go processRevertTransaction | fsm | model.go applyRevert | `01-revert-unknown-transaction` |
| revert-already-reverted | `TRANSACTION_ALREADY_REVERTED` | processor_revert_transaction.go processRevertTransaction | fsm | model.go applyRevert | no: its setup must be applied as separate bulks (a revert of a transaction created in the same bulk is REVERT_TARGET_CREATED_IN_BATCH) |
| revert-target-created-in-batch | `REVERT_TARGET_CREATED_IN_BATCH` | processor_revert_transaction.go checkRevertTargetObservation | fsm | model.go applyRevert | `01-revert-target-created-in-batch` |
| transient-account-non-zero | `TRANSIENT_ACCOUNT_NON_ZERO` | write_set.go ValidateTransientVolumes (machine.go end-of-bulk check) | fsm | model.go transientViolation | `01-transient-account-non-zero` |
| tx-metadata-without-target | `VALIDATION` | processor_metadata.go validateTransactionTarget (ErrTransactionTargetMissing) | fsm | model.go applyAddTxMetadata | `01-tx-metadata-without-target` |
| tx-metadata-unknown-transaction | `TRANSACTION_NOT_FOUND` | processor_metadata.go validateTransactionTarget | fsm | model.go applyAddTxMetadata | `01-tx-metadata-unknown-transaction` |
| delete-tx-metadata-without-target | `VALIDATION` | processor_metadata.go validateTransactionTarget (ErrTransactionTargetMissing) | fsm | model.go applyDeleteMetadata | `01-delete-tx-metadata-without-target` |
| delete-tx-metadata-unknown-transaction | `TRANSACTION_NOT_FOUND` | processor_metadata.go validateTransactionTarget | fsm | model.go applyDeleteMetadata | `01-delete-tx-metadata-unknown-transaction` |
| delete-account-metadata-missing | `METADATA_NOT_FOUND` | processor_metadata.go processDeleteMetadata | fsm | model.go applyDeleteMetadata | `01-delete-account-metadata-missing` |
| delete-tx-metadata-missing | `METADATA_NOT_FOUND` | processor_metadata.go processDeleteMetadata | fsm | model.go applyDeleteMetadata | `01-delete-tx-metadata-missing` |
| delete-ledger-metadata-missing | `METADATA_NOT_FOUND` | processor_ledger_metadata.go processDeleteLedgerMetadata | fsm | model.go applyDeleteLedgerMetadata | `01-delete-ledger-metadata-missing` |
| index-already-exists | `INDEX_ALREADY_EXISTS` | processor_index.go processCreateIndex | fsm | model.go applyCreateIndex | `01-index-already-exists` |
| index-field-not-in-schema | `METADATA_FIELD_NOT_IN_SCHEMA` | processor_index.go validateIndexTarget | fsm | model.go applyCreateIndex | `01-index-field-not-in-schema` |
| pq-already-exists | `PREPARED_QUERY_ALREADY_EXISTS` | processor_prepared_query.go processCreatePreparedQuery | fsm | model.go applyCreatePreparedQuery | `01-pq-already-exists` |
| pq-update-not-found | `PREPARED_QUERY_NOT_FOUND` | processor_prepared_query.go processUpdatePreparedQuery | fsm | model.go applyUpdatePreparedQuery | `01-pq-update-not-found` |
| pq-update-filter-invalid-for-stored-target | `FILTER_COMPILATION_ERROR` | processor_prepared_query.go processUpdatePreparedQuery:129 (ValidateFilterForTarget; *ErrFilterCompilation is an UncommittedFailure: not audited, not frozen) | fsm | model.go applyUpdatePreparedQuery | `01-pq-update-filter-invalid-for-stored-target` |
| pq-delete-not-found | `PREPARED_QUERY_NOT_FOUND` | processor_prepared_query.go processDeletePreparedQuery | fsm | model.go applyDeletePreparedQuery | `01-pq-delete-not-found` |
<!-- table:end -->

## Fixture-ready shapes

For `W/oraclecheck/testdata/rejections/` (00d copies them, one file per entry,
named after `name`). Requests are `servicepb.Request` protojson. Every `setup`
works both applied as one unkeyed bulk and as one bulk per request. All shapes
use ledger `L`.

<!-- fixtures:begin -->
```json
[
  {
    "name": "01-invalid-skippable-reason",
    "setup": [
      {
        "createLedger": {
          "defaultEnforcementMode": "CHART_ENFORCEMENT_AUDIT",
          "name": "L"
        }
      }
    ],
    "requests": [
      {
        "apply": {
          "action": {
            "createTransaction": {
              "postings": [
                {
                  "amount": {
                    "v0": "1"
                  },
                  "asset": "USD",
                  "destination": "users:1",
                  "source": "world"
                }
              ]
            }
          },
          "ledger": "L",
          "skippableReasons": [
            "ERROR_REASON_INSUFFICIENT_FUNDS"
          ]
        }
      }
    ],
    "expectReason": "VALIDATION"
  },
  {
    "name": "01-revert-without-target",
    "setup": [
      {
        "createLedger": {
          "defaultEnforcementMode": "CHART_ENFORCEMENT_AUDIT",
          "name": "L"
        }
      }
    ],
    "requests": [
      {
        "apply": {
          "action": {
            "revertTransaction": {
              "force": true
            }
          },
          "ledger": "L"
        }
      }
    ],
    "expectReason": "VALIDATION"
  },
  {
    "name": "01-empty-create",
    "setup": [
      {
        "createLedger": {
          "defaultEnforcementMode": "CHART_ENFORCEMENT_AUDIT",
          "name": "L"
        }
      }
    ],
    "requests": [
      {
        "apply": {
          "action": {
            "createTransaction": {}
          },
          "ledger": "L"
        }
      }
    ],
    "expectReason": "VALIDATION"
  },
  {
    "name": "01-pq-create-query-required",
    "setup": [
      {
        "createLedger": {
          "defaultEnforcementMode": "CHART_ENFORCEMENT_AUDIT",
          "name": "L"
        }
      }
    ],
    "requests": [
      {
        "createPreparedQuery": {
          "ledger": "L"
        }
      }
    ],
    "expectReason": "VALIDATION"
  },
  {
    "name": "01-pq-create-name-required",
    "setup": [
      {
        "createLedger": {
          "defaultEnforcementMode": "CHART_ENFORCEMENT_AUDIT",
          "name": "L"
        }
      }
    ],
    "requests": [
      {
        "createPreparedQuery": {
          "ledger": "L",
          "query": {}
        }
      }
    ],
    "expectReason": "VALIDATION"
  },
  {
    "name": "01-pq-create-target-unsupported",
    "setup": [
      {
        "createLedger": {
          "defaultEnforcementMode": "CHART_ENFORCEMENT_AUDIT",
          "name": "L"
        }
      }
    ],
    "requests": [
      {
        "createPreparedQuery": {
          "ledger": "L",
          "query": {
            "name": "q",
            "target": "QUERY_TARGET_AUDIT"
          }
        }
      }
    ],
    "expectReason": "VALIDATION"
  },
  {
    "name": "01-pq-create-filter-invalid-for-target",
    "setup": [
      {
        "createLedger": {
          "defaultEnforcementMode": "CHART_ENFORCEMENT_AUDIT",
          "name": "L"
        }
      }
    ],
    "requests": [
      {
        "createPreparedQuery": {
          "ledger": "L",
          "query": {
            "filter": {
              "address": {
                "hardcodedPrefix": "a:"
              }
            },
            "name": "q",
            "target": "QUERY_TARGET_LOGS"
          }
        }
      }
    ],
    "expectReason": "FILTER_COMPILATION_ERROR"
  },
  {
    "name": "01-pq-create-filter-shape",
    "setup": [
      {
        "createLedger": {
          "defaultEnforcementMode": "CHART_ENFORCEMENT_AUDIT",
          "name": "L"
        }
      }
    ],
    "requests": [
      {
        "createPreparedQuery": {
          "ledger": "L",
          "query": {
            "filter": {
              "logId": {}
            },
            "name": "q",
            "target": "QUERY_TARGET_LOGS"
          }
        }
      }
    ],
    "expectReason": "FILTER_COMPILATION_ERROR"
  },
  {
    "name": "01-pq-update-name-required",
    "setup": [
      {
        "createLedger": {
          "defaultEnforcementMode": "CHART_ENFORCEMENT_AUDIT",
          "name": "L"
        }
      }
    ],
    "requests": [
      {
        "updatePreparedQuery": {
          "ledger": "L"
        }
      }
    ],
    "expectReason": "VALIDATION"
  },
  {
    "name": "01-pq-update-filter-shape",
    "setup": [
      {
        "createLedger": {
          "defaultEnforcementMode": "CHART_ENFORCEMENT_AUDIT",
          "name": "L"
        }
      },
      {
        "createPreparedQuery": {
          "ledger": "L",
          "query": {
            "name": "q",
            "target": "QUERY_TARGET_LOGS"
          }
        }
      }
    ],
    "requests": [
      {
        "updatePreparedQuery": {
          "filter": {
            "logId": {}
          },
          "ledger": "L",
          "name": "q"
        }
      }
    ],
    "expectReason": "FILTER_COMPILATION_ERROR"
  },
  {
    "name": "01-pq-delete-name-required",
    "setup": [
      {
        "createLedger": {
          "defaultEnforcementMode": "CHART_ENFORCEMENT_AUDIT",
          "name": "L"
        }
      }
    ],
    "requests": [
      {
        "deletePreparedQuery": {
          "ledger": "L"
        }
      }
    ],
    "expectReason": "VALIDATION"
  },
  {
    "name": "01-create-ledger-nameless-account-type",
    "setup": [],
    "requests": [
      {
        "createLedger": {
          "accountTypes": {
            "": {
              "pattern": "x:{id}"
            }
          },
          "name": "L"
        }
      }
    ],
    "expectReason": "INVALID_PATTERN"
  },
  {
    "name": "01-ledger-already-exists",
    "setup": [
      {
        "createLedger": {
          "defaultEnforcementMode": "CHART_ENFORCEMENT_AUDIT",
          "name": "L"
        }
      }
    ],
    "requests": [
      {
        "createLedger": {
          "defaultEnforcementMode": "CHART_ENFORCEMENT_AUDIT",
          "name": "L"
        }
      }
    ],
    "expectReason": "LEDGER_ALREADY_EXISTS"
  },
  {
    "name": "01-create-ledger-deleted",
    "setup": [
      {
        "createLedger": {
          "defaultEnforcementMode": "CHART_ENFORCEMENT_AUDIT",
          "name": "L"
        }
      },
      {
        "deleteLedger": {
          "name": "L"
        }
      }
    ],
    "requests": [
      {
        "createLedger": {
          "defaultEnforcementMode": "CHART_ENFORCEMENT_AUDIT",
          "name": "L"
        }
      }
    ],
    "expectReason": "LEDGER_DELETED"
  },
  {
    "name": "01-create-ledger-invalid-pattern",
    "setup": [],
    "requests": [
      {
        "createLedger": {
          "accountTypes": {
            "t": {
              "pattern": "{"
            }
          },
          "name": "L"
        }
      }
    ],
    "expectReason": "INVALID_PATTERN"
  },
  {
    "name": "01-apply-on-deleted-ledger",
    "setup": [
      {
        "createLedger": {
          "defaultEnforcementMode": "CHART_ENFORCEMENT_AUDIT",
          "name": "L"
        }
      },
      {
        "deleteLedger": {
          "name": "L"
        }
      }
    ],
    "requests": [
      {
        "apply": {
          "action": {
            "createTransaction": {
              "postings": [
                {
                  "amount": {
                    "v0": "1"
                  },
                  "asset": "USD",
                  "destination": "users:1",
                  "source": "world"
                }
              ]
            }
          },
          "ledger": "L"
        }
      }
    ],
    "expectReason": "LEDGER_DELETED"
  },
  {
    "name": "01-promote-unknown-ledger",
    "setup": [],
    "requests": [
      {
        "promoteLedger": {
          "ledger": "L"
        }
      }
    ],
    "expectReason": "LEDGER_NOT_FOUND"
  },
  {
    "name": "01-promote-normal-ledger",
    "setup": [
      {
        "createLedger": {
          "defaultEnforcementMode": "CHART_ENFORCEMENT_AUDIT",
          "name": "L"
        }
      }
    ],
    "requests": [
      {
        "promoteLedger": {
          "ledger": "L"
        }
      }
    ],
    "expectReason": "LEDGER_NOT_IN_MIRROR_MODE"
  },
  {
    "name": "01-delete-unknown-ledger",
    "setup": [],
    "requests": [
      {
        "deleteLedger": {
          "name": "L"
        }
      }
    ],
    "expectReason": "LEDGER_NOT_FOUND"
  },
  {
    "name": "01-checkpoint-id-required",
    "setup": [],
    "requests": [
      {
        "deleteQueryCheckpoint": {}
      }
    ],
    "expectReason": "CHECKPOINT_ID_REQUIRED"
  },
  {
    "name": "01-checkpoint-not-found",
    "setup": [],
    "requests": [
      {
        "deleteQueryCheckpoint": {
          "checkpointId": "999"
        }
      }
    ],
    "expectReason": "CHECKPOINT_NOT_FOUND"
  },
  {
    "name": "01-invalid-cron-expression",
    "setup": [],
    "requests": [
      {
        "setQueryCheckpointSchedule": {
          "cron": "not a cron"
        }
      }
    ],
    "expectReason": "INVALID_CRON_EXPRESSION"
  },
  {
    "name": "01-account-type-already-exists",
    "setup": [
      {
        "createLedger": {
          "defaultEnforcementMode": "CHART_ENFORCEMENT_AUDIT",
          "name": "L"
        }
      },
      {
        "addAccountType": {
          "accountType": {
            "name": "users",
            "pattern": "users:{id}"
          },
          "ledger": "L"
        }
      }
    ],
    "requests": [
      {
        "addAccountType": {
          "accountType": {
            "name": "users",
            "pattern": "users:{id}"
          },
          "ledger": "L"
        }
      }
    ],
    "expectReason": "ACCOUNT_TYPE_ALREADY_EXISTS"
  },
  {
    "name": "01-account-type-not-found",
    "setup": [
      {
        "createLedger": {
          "defaultEnforcementMode": "CHART_ENFORCEMENT_AUDIT",
          "name": "L"
        }
      }
    ],
    "requests": [
      {
        "removeAccountType": {
          "ledger": "L",
          "name": "users"
        }
      }
    ],
    "expectReason": "ACCOUNT_TYPE_NOT_FOUND"
  },
  {
    "name": "01-reference-conflict",
    "setup": [
      {
        "createLedger": {
          "defaultEnforcementMode": "CHART_ENFORCEMENT_AUDIT",
          "name": "L"
        }
      },
      {
        "apply": {
          "action": {
            "createTransaction": {
              "postings": [
                {
                  "amount": {
                    "v0": "1"
                  },
                  "asset": "USD",
                  "destination": "users:1",
                  "source": "world"
                }
              ],
              "reference": "r"
            }
          },
          "ledger": "L"
        }
      }
    ],
    "requests": [
      {
        "apply": {
          "action": {
            "createTransaction": {
              "postings": [
                {
                  "amount": {
                    "v0": "1"
                  },
                  "asset": "USD",
                  "destination": "users:1",
                  "source": "world"
                }
              ],
              "reference": "r"
            }
          },
          "ledger": "L"
        }
      }
    ],
    "expectReason": "TRANSACTION_REFERENCE_CONFLICT"
  },
  {
    "name": "01-insufficient-funds",
    "setup": [
      {
        "createLedger": {
          "defaultEnforcementMode": "CHART_ENFORCEMENT_AUDIT",
          "name": "L"
        }
      }
    ],
    "requests": [
      {
        "apply": {
          "action": {
            "createTransaction": {
              "postings": [
                {
                  "amount": {
                    "v0": "1"
                  },
                  "asset": "USD",
                  "destination": "users:2",
                  "source": "users:1"
                }
              ]
            }
          },
          "ledger": "L"
        }
      }
    ],
    "expectReason": "INSUFFICIENT_FUNDS"
  },
  {
    "name": "01-volume-overflow",
    "setup": [
      {
        "createLedger": {
          "defaultEnforcementMode": "CHART_ENFORCEMENT_AUDIT",
          "name": "L"
        }
      },
      {
        "apply": {
          "action": {
            "createTransaction": {
              "postings": [
                {
                  "amount": {
                    "v0": "18446744073709551615",
                    "v1": "18446744073709551615",
                    "v2": "18446744073709551615",
                    "v3": "18446744073709551615"
                  },
                  "asset": "USD",
                  "destination": "users:1",
                  "source": "world"
                }
              ]
            }
          },
          "ledger": "L"
        }
      }
    ],
    "requests": [
      {
        "apply": {
          "action": {
            "createTransaction": {
              "postings": [
                {
                  "amount": {
                    "v0": "1"
                  },
                  "asset": "USD",
                  "destination": "users:1",
                  "source": "world"
                }
              ]
            }
          },
          "ledger": "L"
        }
      }
    ],
    "expectReason": "VOLUME_OVERFLOW"
  },
  {
    "name": "01-tx-account-not-matching-type",
    "setup": [
      {
        "createLedger": {
          "name": "L"
        }
      },
      {
        "addAccountType": {
          "accountType": {
            "name": "users",
            "pattern": "users:{id}"
          },
          "ledger": "L"
        }
      }
    ],
    "requests": [
      {
        "apply": {
          "action": {
            "createTransaction": {
              "postings": [
                {
                  "amount": {
                    "v0": "1"
                  },
                  "asset": "USD",
                  "destination": "other:1",
                  "source": "world"
                }
              ]
            }
          },
          "ledger": "L"
        }
      }
    ],
    "expectReason": "ACCOUNT_NOT_MATCHING_TYPE"
  },
  {
    "name": "01-account-metadata-not-matching-type",
    "setup": [
      {
        "createLedger": {
          "name": "L"
        }
      },
      {
        "addAccountType": {
          "accountType": {
            "name": "users",
            "pattern": "users:{id}"
          },
          "ledger": "L"
        }
      }
    ],
    "requests": [
      {
        "apply": {
          "action": {
            "addMetadata": {
              "metadata": {
                "k": {
                  "stringValue": "v"
                }
              },
              "target": {
                "account": {
                  "addr": "other:1"
                }
              }
            }
          },
          "ledger": "L"
        }
      }
    ],
    "expectReason": "ACCOUNT_NOT_MATCHING_TYPE"
  },
  {
    "name": "01-revert-unknown-transaction",
    "setup": [
      {
        "createLedger": {
          "defaultEnforcementMode": "CHART_ENFORCEMENT_AUDIT",
          "name": "L"
        }
      }
    ],
    "requests": [
      {
        "apply": {
          "action": {
            "revertTransaction": {
              "force": true,
              "transactionId": "7"
            }
          },
          "ledger": "L"
        }
      }
    ],
    "expectReason": "TRANSACTION_NOT_FOUND"
  },
  {
    "name": "01-revert-target-created-in-batch",
    "setup": [
      {
        "createLedger": {
          "defaultEnforcementMode": "CHART_ENFORCEMENT_AUDIT",
          "name": "L"
        }
      }
    ],
    "requests": [
      {
        "apply": {
          "action": {
            "createTransaction": {
              "postings": [
                {
                  "amount": {
                    "v0": "1"
                  },
                  "asset": "USD",
                  "destination": "users:1",
                  "source": "world"
                }
              ]
            }
          },
          "ledger": "L"
        }
      },
      {
        "apply": {
          "action": {
            "revertTransaction": {
              "force": true,
              "transactionId": "1"
            }
          },
          "ledger": "L"
        }
      }
    ],
    "expectReason": "REVERT_TARGET_CREATED_IN_BATCH"
  },
  {
    "name": "01-transient-account-non-zero",
    "setup": [
      {
        "createLedger": {
          "defaultEnforcementMode": "CHART_ENFORCEMENT_AUDIT",
          "name": "L"
        }
      },
      {
        "addAccountType": {
          "accountType": {
            "name": "users",
            "pattern": "users:{id}",
            "persistence": "ACCOUNT_TYPE_TRANSIENT"
          },
          "ledger": "L"
        }
      }
    ],
    "requests": [
      {
        "apply": {
          "action": {
            "createTransaction": {
              "postings": [
                {
                  "amount": {
                    "v0": "1"
                  },
                  "asset": "USD",
                  "destination": "users:1",
                  "source": "world"
                }
              ]
            }
          },
          "ledger": "L"
        }
      }
    ],
    "expectReason": "TRANSIENT_ACCOUNT_NON_ZERO"
  },
  {
    "name": "01-tx-metadata-without-target",
    "setup": [
      {
        "createLedger": {
          "defaultEnforcementMode": "CHART_ENFORCEMENT_AUDIT",
          "name": "L"
        }
      }
    ],
    "requests": [
      {
        "apply": {
          "action": {
            "addMetadata": {
              "metadata": {
                "k": {
                  "stringValue": "v"
                }
              },
              "target": {
                "transactionId": "0"
              }
            }
          },
          "ledger": "L"
        }
      }
    ],
    "expectReason": "VALIDATION"
  },
  {
    "name": "01-tx-metadata-unknown-transaction",
    "setup": [
      {
        "createLedger": {
          "defaultEnforcementMode": "CHART_ENFORCEMENT_AUDIT",
          "name": "L"
        }
      }
    ],
    "requests": [
      {
        "apply": {
          "action": {
            "addMetadata": {
              "metadata": {
                "k": {
                  "stringValue": "v"
                }
              },
              "target": {
                "transactionId": "7"
              }
            }
          },
          "ledger": "L"
        }
      }
    ],
    "expectReason": "TRANSACTION_NOT_FOUND"
  },
  {
    "name": "01-delete-tx-metadata-without-target",
    "setup": [
      {
        "createLedger": {
          "defaultEnforcementMode": "CHART_ENFORCEMENT_AUDIT",
          "name": "L"
        }
      }
    ],
    "requests": [
      {
        "apply": {
          "action": {
            "deleteMetadata": {
              "key": "k",
              "target": {
                "transactionId": "0"
              }
            }
          },
          "ledger": "L"
        }
      }
    ],
    "expectReason": "VALIDATION"
  },
  {
    "name": "01-delete-tx-metadata-unknown-transaction",
    "setup": [
      {
        "createLedger": {
          "defaultEnforcementMode": "CHART_ENFORCEMENT_AUDIT",
          "name": "L"
        }
      }
    ],
    "requests": [
      {
        "apply": {
          "action": {
            "deleteMetadata": {
              "key": "k",
              "target": {
                "transactionId": "7"
              }
            }
          },
          "ledger": "L"
        }
      }
    ],
    "expectReason": "TRANSACTION_NOT_FOUND"
  },
  {
    "name": "01-delete-account-metadata-missing",
    "setup": [
      {
        "createLedger": {
          "defaultEnforcementMode": "CHART_ENFORCEMENT_AUDIT",
          "name": "L"
        }
      }
    ],
    "requests": [
      {
        "apply": {
          "action": {
            "deleteMetadata": {
              "key": "k",
              "target": {
                "account": {
                  "addr": "users:1"
                }
              }
            }
          },
          "ledger": "L"
        }
      }
    ],
    "expectReason": "METADATA_NOT_FOUND"
  },
  {
    "name": "01-delete-tx-metadata-missing",
    "setup": [
      {
        "createLedger": {
          "defaultEnforcementMode": "CHART_ENFORCEMENT_AUDIT",
          "name": "L"
        }
      },
      {
        "apply": {
          "action": {
            "createTransaction": {
              "postings": [
                {
                  "amount": {
                    "v0": "1"
                  },
                  "asset": "USD",
                  "destination": "users:1",
                  "source": "world"
                }
              ]
            }
          },
          "ledger": "L"
        }
      }
    ],
    "requests": [
      {
        "apply": {
          "action": {
            "deleteMetadata": {
              "key": "k",
              "target": {
                "transactionId": "1"
              }
            }
          },
          "ledger": "L"
        }
      }
    ],
    "expectReason": "METADATA_NOT_FOUND"
  },
  {
    "name": "01-delete-ledger-metadata-missing",
    "setup": [
      {
        "createLedger": {
          "defaultEnforcementMode": "CHART_ENFORCEMENT_AUDIT",
          "name": "L"
        }
      }
    ],
    "requests": [
      {
        "deleteLedgerMetadata": {
          "key": "k",
          "ledger": "L"
        }
      }
    ],
    "expectReason": "METADATA_NOT_FOUND"
  },
  {
    "name": "01-index-already-exists",
    "setup": [
      {
        "createLedger": {
          "defaultEnforcementMode": "CHART_ENFORCEMENT_AUDIT",
          "name": "L"
        }
      },
      {
        "setMetadataFieldType": {
          "key": "k",
          "ledger": "L"
        }
      },
      {
        "createIndex": {
          "id": {
            "metadata": {
              "key": "k"
            }
          },
          "ledger": "L"
        }
      }
    ],
    "requests": [
      {
        "createIndex": {
          "id": {
            "metadata": {
              "key": "k"
            }
          },
          "ledger": "L"
        }
      }
    ],
    "expectReason": "INDEX_ALREADY_EXISTS"
  },
  {
    "name": "01-index-field-not-in-schema",
    "setup": [
      {
        "createLedger": {
          "defaultEnforcementMode": "CHART_ENFORCEMENT_AUDIT",
          "name": "L"
        }
      }
    ],
    "requests": [
      {
        "createIndex": {
          "id": {
            "metadata": {
              "key": "k"
            }
          },
          "ledger": "L"
        }
      }
    ],
    "expectReason": "METADATA_FIELD_NOT_IN_SCHEMA"
  },
  {
    "name": "01-pq-already-exists",
    "setup": [
      {
        "createLedger": {
          "defaultEnforcementMode": "CHART_ENFORCEMENT_AUDIT",
          "name": "L"
        }
      },
      {
        "createPreparedQuery": {
          "ledger": "L",
          "query": {
            "name": "q"
          }
        }
      }
    ],
    "requests": [
      {
        "createPreparedQuery": {
          "ledger": "L",
          "query": {
            "name": "q"
          }
        }
      }
    ],
    "expectReason": "PREPARED_QUERY_ALREADY_EXISTS"
  },
  {
    "name": "01-pq-update-not-found",
    "setup": [
      {
        "createLedger": {
          "defaultEnforcementMode": "CHART_ENFORCEMENT_AUDIT",
          "name": "L"
        }
      }
    ],
    "requests": [
      {
        "updatePreparedQuery": {
          "ledger": "L",
          "name": "q"
        }
      }
    ],
    "expectReason": "PREPARED_QUERY_NOT_FOUND"
  },
  {
    "name": "01-pq-update-filter-invalid-for-stored-target",
    "setup": [
      {
        "createLedger": {
          "defaultEnforcementMode": "CHART_ENFORCEMENT_AUDIT",
          "name": "L"
        }
      },
      {
        "createPreparedQuery": {
          "ledger": "L",
          "query": {
            "name": "q",
            "target": "QUERY_TARGET_LOGS"
          }
        }
      }
    ],
    "requests": [
      {
        "updatePreparedQuery": {
          "filter": {
            "address": {
              "hardcodedPrefix": "a:"
            }
          },
          "ledger": "L",
          "name": "q"
        }
      }
    ],
    "expectReason": "FILTER_COMPILATION_ERROR"
  },
  {
    "name": "01-pq-delete-not-found",
    "setup": [
      {
        "createLedger": {
          "defaultEnforcementMode": "CHART_ENFORCEMENT_AUDIT",
          "name": "L"
        }
      }
    ],
    "requests": [
      {
        "deletePreparedQuery": {
          "ledger": "L",
          "name": "q"
        }
      }
    ],
    "expectReason": "PREPARED_QUERY_NOT_FOUND"
  }
]
```
<!-- fixtures:end -->
