# Porting record — 00a Go oracle core

Task 00a lifts the singleton model driver's checker (`M/` =
`tests/antithesis/workload/bin/cmds/model/singleton_driver_model/`) into the
assertion-free library `W/oraclecheck` and the oracle service `W/bin/oracle`.
Every ported check keeps its `M/` suffix behind the `oracle: ` prefix (DESIGN
§9.1) and is asserted at exactly one literal call site in `bin/oracle`
(checked by `bin/oracle/asserter_test.go`).

| Old source | Old assertion (kind: literal) | New | Where |
|---|---|---|---|
| M/processor.go:181 | Reachable: singleton_driver_model: model outcome verified | oracle: model outcome verified | bin/oracle/h_core.go (event in oraclecheck/commit.go, failure.go, tracker.go, registry.go) |
| M/validate.go:124 | Unreachable: singleton_driver_model: model rejects a server-committed bulk | oracle: model rejects a server-committed bulk | bin/oracle/h_core.go (event in oraclecheck/commit.go, failure.go, tracker.go, registry.go) |
| M/validate.go:136 | Unreachable: singleton_driver_model: checkpoint commit outside model | oracle: checkpoint commit outside model | bin/oracle/h_core.go (event in oraclecheck/commit.go, failure.go, tracker.go, registry.go) |
| M/validate.go:141 | Unreachable: singleton_driver_model: committed log count mismatch | oracle: committed log count mismatch | bin/oracle/h_core.go (event in oraclecheck/commit.go, failure.go, tracker.go, registry.go) |
| M/validate.go:148 | Unreachable: singleton_driver_model: committed ledger log identity mismatch | oracle: committed ledger log identity mismatch | bin/oracle/h_core.go (event in oraclecheck/commit.go, failure.go, tracker.go, registry.go) |
| M/validate.go:155 | Unreachable: singleton_driver_model: skipped-order response mismatch | oracle: skipped-order response mismatch | bin/oracle/h_core.go (event in oraclecheck/commit.go, failure.go, tracker.go, registry.go) |
| M/validate.go:161 | Unreachable: singleton_driver_model: enforcement-mode response mismatch | oracle: enforcement-mode response mismatch | bin/oracle/h_core.go (event in oraclecheck/commit.go, failure.go, tracker.go, registry.go) |
| M/validate.go:173 | Unreachable: singleton_driver_model: lifecycle response mismatch | oracle: lifecycle response mismatch | bin/oracle/h_core.go (event in oraclecheck/commit.go, failure.go, tracker.go, registry.go) |
| M/validate.go:206 | Unreachable: singleton_driver_model: model post-commit volume mismatch | oracle: model post-commit volume mismatch | bin/oracle/h_core.go (event in oraclecheck/commit.go, failure.go, tracker.go, registry.go) |
| M/validate.go:229 | Unreachable: singleton_driver_model: response metadata value mismatch | oracle: response metadata value mismatch | bin/oracle/h_core.go (event in oraclecheck/commit.go, failure.go, tracker.go, registry.go) |
| M/validate.go:271 | Unreachable: singleton_driver_model: revert transaction id mismatch | oracle: revert transaction id mismatch | bin/oracle/h_core.go (event in oraclecheck/commit.go, failure.go, tracker.go, registry.go) |
| M/validate.go:281 | Unreachable: singleton_driver_model: reverted transaction id mismatch | oracle: reverted transaction id mismatch | bin/oracle/h_core.go (event in oraclecheck/commit.go, failure.go, tracker.go, registry.go) |
| M/validate.go:291 | Unreachable: singleton_driver_model: revert postings mismatch | oracle: revert postings mismatch | bin/oracle/h_core.go (event in oraclecheck/commit.go, failure.go, tracker.go, registry.go) |
| M/validate.go:306 | Unreachable: singleton_driver_model: transaction id mismatch | oracle: transaction id mismatch | bin/oracle/h_core.go (event in oraclecheck/commit.go, failure.go, tracker.go, registry.go) |
| M/validate.go:316 | Unreachable: singleton_driver_model: transaction reference mismatch | oracle: transaction reference mismatch | bin/oracle/h_core.go (event in oraclecheck/commit.go, failure.go, tracker.go, registry.go) |
| M/validate.go:326 | Unreachable: singleton_driver_model: transaction postings mismatch | oracle: transaction postings mismatch | bin/oracle/h_core.go (event in oraclecheck/commit.go, failure.go, tracker.go, registry.go) |
| M/validate.go:339 | Unreachable: singleton_driver_model: transaction account-metadata mismatch | oracle: transaction account-metadata mismatch | bin/oracle/h_core.go (event in oraclecheck/commit.go, failure.go, tracker.go, registry.go) |
| M/validate.go:354 | Unreachable: singleton_driver_model: delete-metadata response mismatch | oracle: delete-metadata response mismatch | bin/oracle/h_core.go (event in oraclecheck/commit.go, failure.go, tracker.go, registry.go) |
| M/validate.go:365 | Unreachable: singleton_driver_model: chart response mismatch | oracle: chart response mismatch | bin/oracle/h_core.go (event in oraclecheck/commit.go, failure.go, tracker.go, registry.go) |
| M/validate.go:373 | Unreachable: singleton_driver_model: set-field-type response mismatch | oracle: set-field-type response mismatch | bin/oracle/h_core.go (event in oraclecheck/commit.go, failure.go, tracker.go, registry.go) |
| M/validate.go:385 | Unreachable: singleton_driver_model: remove-field-type response mismatch | oracle: remove-field-type response mismatch | bin/oracle/h_core.go (event in oraclecheck/commit.go, failure.go, tracker.go, registry.go) |
| M/validate.go:408 | Unreachable: singleton_driver_model: remove-field-type dropped-index mismatch | oracle: remove-field-type dropped-index mismatch | bin/oracle/h_core.go (event in oraclecheck/commit.go, failure.go, tracker.go, registry.go) |
| M/validate.go:423 | Unreachable: singleton_driver_model: remove-field-type dropped wrong index | oracle: remove-field-type dropped wrong index | bin/oracle/h_core.go (event in oraclecheck/commit.go, failure.go, tracker.go, registry.go) |
| M/validate.go:583 | Unreachable: singleton_driver_model: bulk failure not explained by any serialization | oracle: bulk failure not explained by any serialization | bin/oracle/h_core.go (event in oraclecheck/commit.go, failure.go, tracker.go, registry.go) |
| M/validate.go:605 | Unreachable: singleton_driver_model: successful bulk returned no committed log | oracle: successful bulk returned no committed log | bin/oracle/h_core.go (event in oraclecheck/commit.go, failure.go, tracker.go, registry.go) |
| M/validate.go:782 | Unreachable: singleton_driver_model: ledger-scoped read returned unexplained NotFound | oracle: ledger-scoped read returned unexplained NotFound | bin/oracle/h_core.go (event in oraclecheck/commit.go, failure.go, tracker.go, registry.go) |
| M/validate.go:553 | Reachable: singleton_driver_model: insufficient-funds rejection exercised | moved: rejection coverage is a failure hook of the family that generates the shape (task 02) | – |
| M/validate.go:555 | Reachable: singleton_driver_model: reference-conflict rejection exercised | moved: failure hook (task 02) | – |
| M/validate.go:557 | Reachable: singleton_driver_model: volume-overflow rejection exercised | moved: failure hook (task 02) | – |
| M/validate.go:559 | Reachable: singleton_driver_model: validation rejection exercised | moved: failure hook (task 02) | – |
| M/validate.go:561 | Reachable: singleton_driver_model: idempotency-conflict rejection exercised | moved: failure hook (task 03) | – |
| M/validate.go:563 | Reachable: singleton_driver_model: malformed prepared-query filter refused at save | moved: failure hook (task 08) | – |
| M/validate.go:538 | Raw: singleton_driver_model: coverage ledger deletion and reserved-name rejection verified | moved: pendingDeleted failure hook (task 04) | – |
| M/validate.go:58 | Raw: singleton_driver_model: coverage mirror promotion and write recovery verified | moved: drain hook (task 04) | – |
| M/validate.go:544 | Raw: singleton_driver_model: invalid skippable-reason coverage | moved: failure hook (task 02) | – |
| M/checkpoints.go:105-127 | Raw: singleton_driver_model: checkpoint create/delete/schedule/limit coverage | moved: drain/failure hooks (task 09) | – |
| M/validate.go:663 | Unreachable: singleton_driver_model: account read outside model | task 05 (GetAccount kind); 00a keeps a minimal copy as the Smoke kind's `oracle: smoke account read outside model` (deleted by 99) | – |
| M/validate.go:757 | Unreachable: singleton_driver_model: ledger read outside model | task 04 (GetLedger kind) | – |
| M/validate.go:853 | Unreachable: singleton_driver_model: transaction read outside model | task 05 (GetTransaction kind) | – |
| M/validate.go:886 | Unreachable: singleton_driver_model: metadata schema read outside model | task 04 (GetMetadataSchemaStatus kind) | – |

## New properties

| Literal | Kind | Where | Condition |
|---|---|---|---|
| `oracle: one idempotency key yields one outcome across attempts` | Unreachable | `bin/oracle/h_core.go` (`Tracker.LateWrite`) | a late definitive outcome of an ended ticket differs from the recorded one (success vs success: different log sequences; failure vs failure: different reason; mixed) |
| `oracle: two attempts of one key both returned definitive outcomes` | Sometimes (true on every `/write/late`) | `bin/oracle/h_core.go` | coverage that the property above is exercised |
| `oracle: smoke account read outside model` | Unreachable | `bin/oracle/h_core.go` (Smoke kind) | a GetAccount answer no candidate base explains (Smoke kind only; task 99 deletes it) |
| `oracle protocol: driver posted a non-definitive write outcome` | Unreachable | `bin/oracle/h_core.go` (`Tracker.EndWrite`, `LateWrite`) | `/write/end` or `/write/late` with an outcome outside D |
| `oracle protocol: checker panicked` | Unreachable | `bin/oracle/server.go` | a handler panicked (answered `500 panic`) |
| `oracle protocol: unmapped event` | Unreachable | `bin/oracle/registry.go` | an event id with no registered asserter |
| `oracle protocol: the driver and the oracle agree on the query checkpoint limit` | Unreachable | `bin/oracle/server.go` | `/setup` limit differs from env `QUERY_CHECKPOINT_LIMIT` |

## Reachability of must-hit assertions

| Literal | Kind | Reached by | Reachability |
|---|---|---|---|
| `oracle: model outcome verified` | Reachable | every drained success, every validated failure and empty commit, every Smoke read that matches; 00c's `smoke_tx` / `smoke_get_account` rules | no faults |
| `oracle: two attempts of one key both returned definitive outcomes` | Sometimes | `/write/late` (late UDP outcome or resolver race), 00c's write path | faults (needs an attempt that times out on the driver and completes on the server) |

## Requests to 99

- None for dependencies.
- `go list -deps ./oraclecheck/...` necessarily contains `antithesis-sdk-go/assert`:
  the root module's `internal/storage/dal` imports it and is reached through
  `tests/oracle` → `internal/domain`. `bin/oracle/deps_test.go` therefore checks that
  no package **of the workload module** in the closure imports the SDK, and that
  `workload/internal` is absent (DESIGN §5.1 asks for the stricter form).
