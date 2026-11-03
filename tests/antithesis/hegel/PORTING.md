# Porting record of the hegel workload

This file is generated from `porting/*.md` by `porting/gen_porting.py` (one file per task; edit those, not this concatenation). Each family file maps every old assertion it inherits to `verbatim | renamed | oracle (literal) | new | dropped (why)`, lists its new properties, its must-hit reachability and its requests to task 99. The integration record (catalog audit, dispositions of the requests, integration fixes, server findings) is the last section, [99 — Integration](#99-integration).

## Summary

- **59 main drivers** (`workload/bin/cmds/main/`): 53 in scope, 6 out of scope (k8s operator, DESIGN §1 item 7).
- **The model template** (`workload/bin/cmds/model/singleton_driver_model/`): lifted into the oracle service (`workload/oraclecheck/**`, `workload/bin/oracle/`).
- **26 scenario blocks** (`singleton_driver_scenario_blocks`): `driver/src/rules/blocks.rs` through the client's `/block` (task 17).

### Main drivers

| Old driver | New home (porting file) |
|---|---|
| `eventually_correct` | `14-rust-probes` |
| `eventually_cross_node_identity` | `15-convergence` |
| `eventually_quiesce_read` | `14-rust-probes` |
| `eventually_stats_consistency` | `15-convergence` |
| `first_default_ledger` | 00c `setup.rs` (creates `default` at setup, untracked) |
| `parallel_driver_account_type_violations` | `04-ledgers-chart-maintenance` |
| `parallel_driver_account_types` | `04-ledgers-chart-maintenance` |
| `parallel_driver_audit` | `10-audit` |
| `parallel_driver_barrier_monotonic` | `12-cluster-ops-signals` |
| `parallel_driver_bulk` | `02-tx-writes` |
| `parallel_driver_bulk_atomicity` | `02-tx-writes`, `10-audit` |
| `parallel_driver_check_store` | `12-cluster-ops-signals` |
| `parallel_driver_compaction` | `12-cluster-ops-signals` |
| `parallel_driver_concurrent_ledger_delete` | `04-ledgers-chart-maintenance` |
| `parallel_driver_definitive_errors` | `02-tx-writes`, `14-rust-probes` |
| `parallel_driver_delete_ledger` | `04-ledgers-chart-maintenance` |
| `parallel_driver_event_sinks` | `12-cluster-ops-signals` |
| `parallel_driver_idempotency` | `03-reverts-metadata-idempotency` |
| `parallel_driver_idempotency_conflict` | `03-reverts-metadata-idempotency` |
| `parallel_driver_indexes` | `07-indexes-typed-metadata` |
| `parallel_driver_insufficient_funds` | `02-tx-writes` |
| `parallel_driver_invariants` | `05-point-reads`, `14-rust-probes` |
| `parallel_driver_ledger_create` | `04-ledgers-chart-maintenance` |
| `parallel_driver_ledger_recreate` | `04-ledgers-chart-maintenance` |
| `parallel_driver_list_completeness` | `06-list-queries` |
| `parallel_driver_list_transactions` | `06-list-queries` |
| `parallel_driver_logs` | `05-point-reads` |
| `parallel_driver_maintenance` | `04-ledgers-chart-maintenance` |
| `parallel_driver_metadata` | `03-reverts-metadata-idempotency` |
| `parallel_driver_numscript_lifecycle` | `13-numscript` |
| `parallel_driver_numscripts` | `13-numscript` |
| `parallel_driver_projection_alignment` | `07-indexes-typed-metadata`, `08-prepared-queries` |
| `parallel_driver_queries` | `08-prepared-queries` |
| `parallel_driver_query_checkpoints` | `09-query-checkpoints` |
| `parallel_driver_reference_conflict` | `02-tx-writes` |
| `parallel_driver_reference_conflict_skip` | `02-tx-writes` |
| `parallel_driver_reference_race` | `02-tx-writes` |
| `parallel_driver_revert_concurrent` | `03-reverts-metadata-idempotency` |
| `parallel_driver_revert_idempotent` | `03-reverts-metadata-idempotency` |
| `parallel_driver_revert_sequential` | `03-reverts-metadata-idempotency` |
| `parallel_driver_stale_reads` | `14-rust-probes` |
| `parallel_driver_stats_consistency` | `05-point-reads`, `14-rust-probes` |
| `parallel_driver_timestamp_order` | `02-tx-writes`, `06-list-queries`, `11-quiet-exact` |
| `parallel_driver_transactions` | `02-tx-writes`, `13-numscript` |
| `parallel_driver_transfer_leadership` | `12-cluster-ops-signals` |
| `parallel_driver_transient_accounts` | `02-tx-writes` |
| `parallel_driver_tx_metadata` | `03-reverts-metadata-idempotency` |
| `parallel_driver_typed_metadata` | `07-indexes-typed-metadata` |
| `parallel_driver_update_query` | `08-prepared-queries` |
| `singleton_driver_backup` | `12-cluster-ops-signals` |
| `singleton_driver_config_change` | out of scope: k8s operator (DESIGN §1 item 7); the non-operator part is ported in `15-convergence` |
| `singleton_driver_incremental_backup` | `12-cluster-ops-signals`, `18-restore` |
| `singleton_driver_quorum_recovery` | out of scope: k8s operator (DESIGN §1 item 7) |
| `singleton_driver_rolling_restart` | out of scope: k8s operator (DESIGN §1 item 7) |
| `singleton_driver_scaling` | out of scope: k8s operator (DESIGN §1 item 7) |
| `singleton_driver_scaling_chaos` | out of scope: k8s operator (DESIGN §1 item 7) |
| `singleton_driver_scaling_structured` | out of scope: k8s operator (DESIGN §1 item 7) |
| `singleton_driver_scenario_blocks` | `17-scenario-blocks` |
| `singleton_driver_signing_keys` | `16-signing` |

### Model template (`M/` files)

| `M/` file | New home (porting file) |
|---|---|
| `actions.go` | `02-tx-writes`, `04-ledgers-chart-maintenance`, `07-indexes-typed-metadata` |
| `aggregates.go` | `05-point-reads` |
| `apply_surface.go` | `02-tx-writes`, `03-reverts-metadata-idempotency`, `04-ledgers-chart-maintenance` |
| `audit.go` | `10-audit` |
| `checker.go` | `04-ledgers-chart-maintenance`, `07-indexes-typed-metadata` |
| `checkpoint_metadata.go` | `09-query-checkpoints` |
| `checkpoint_reads.go` | `09-query-checkpoints` |
| `checkpoint_setup.go` | `09-query-checkpoints` |
| `checkpoints.go` | `00a-oracle-core`, `09-query-checkpoints` |
| `config.go` | helper code without assertions (lifted as code where a family needed it) |
| `coverage.go` | `06-list-queries`, `09-query-checkpoints` |
| `coverage_stats.go` | helper code without assertions (lifted as code where a family needed it) |
| `cursors.go` | `06-list-queries`, `10-audit` |
| `debug.go` | helper code without assertions (lifted as code where a family needed it) |
| `enforcement.go` | helper code without assertions (lifted as code where a family needed it) |
| `filter_fold.go` | helper code without assertions (lifted as code where a family needed it) |
| `filter_shapes.go` | helper code without assertions (lifted as code where a family needed it) |
| `generation.go` | dropped: its one assertion (`generation engine failed`) guards the old random engine, which hegel draws replace |
| `getlog.go` | `05-point-reads` |
| `index_inspect.go` | `07-indexes-typed-metadata` |
| `index_introspect.go` | `07-indexes-typed-metadata` |
| `indexes.go` | `06-list-queries`, `07-indexes-typed-metadata` |
| `ledger_info.go` | helper code without assertions (lifted as code where a family needed it) |
| `ledgers_list.go` | `04-ledgers-chart-maintenance` |
| `lifecycle.go` | helper code without assertions (lifted as code where a family needed it) |
| `main.go` | `04-ledgers-chart-maintenance`, `09-query-checkpoints` |
| `metadata_filters.go` | helper code without assertions (lifted as code where a family needed it) |
| `prepared_queries.go` | `08-prepared-queries` |
| `prepared_queries_read.go` | `08-prepared-queries` |
| `processor.go` | `00a-oracle-core` |
| `queries.go` | `06-list-queries` |
| `queries_logs.go` | `06-list-queries` |
| `reads.go` | `04-ledgers-chart-maintenance`, `05-point-reads` |
| `rejected_filter.go` | `06-list-queries` |
| `replay.go` | `03-reverts-metadata-idempotency` |
| `restore.go` | `18-restore` |
| `search.go` | helper code without assertions (lifted as code where a family needed it) |
| `stats.go` | `05-point-reads` |
| `validate.go` | `00a-oracle-core`, `02-tx-writes`, `03-reverts-metadata-idempotency`, `04-ledgers-chart-maintenance`, `05-point-reads`, `07-indexes-typed-metadata`, `08-prepared-queries` |

### Scenario blocks

26 blocks, each with `block <name> succeeded` / `block <name> failed` in `rules/blocks.rs`: `marketplace/deposit`, `marketplace/purchase`, `marketplace/revert`, `marketplace/payout`, `marketplace/metadata`, `gaming/topup`, `gaming/buy_item`, `gaming/p2p_trade`, `gaming/promotion`, `gaming/clawback`, `lending/fund_pool`, `lending/disburse`, `lending/repay`, `lending/provision`, `lending/write_off`, `multicurrency/fund`, `multicurrency/fx`, `multicurrency/vendor_pay`, `payroll/fund_clearing`, `payroll/distribute`, `payroll/pay_salary`, `payroll/cost_alloc`, `subscription/fund`, `subscription/charge`, `subscription/recognize`, `subscription/credit`.

---

<a id="00-foundation"></a>

## 00 — Foundation (Rust core, task 00c)

00c restructured `HG/driver/src/` into the module layout of DESIGN §6.1, deleted
`H/model.rs` and every old rule body of `H/main.rs` (DESIGN §6.9), and kept the assertions
of `H/logs.rs` and `H/clients.rs`. Old line numbers refer to `H/` at tag `hegel-fixtures`.
"Inherited by" names the family that ports the old check (DESIGN §12, task specs); the
family's own porting file records the final mapping.

## Old `H/` assertions

| Old source | Old assertion (kind: literal) | New | Where |
|---|---|---|---|
| H/logs.rs:51 | Always: no data race is reported by any process | verbatim | logs.rs `NodeStates::handle` |
| H/logs.rs:53 | Always: no process panics | verbatim | logs.rs `NodeStates::handle` |
| H/logs.rs:61 | Reachable: a ledger node restarts | verbatim | logs.rs |
| H/logs.rs:73 | Reachable: the driver reads leadership changes from node logs | verbatim | logs.rs |
| H/logs.rs:87 | Sometimes: a node gains leadership while the workload runs | verbatim | logs.rs |
| H/logs.rs:88 | Sometimes: a node loses leadership while the workload runs | verbatim | logs.rs |
| H/logs.rs:91 | Sometimes: two nodes believe they are leader at once | verbatim | logs.rs |
| H/logs.rs:95 | Sometimes (`assert_sometimes_greater_than`): a node changes leadership role many times in one boot | verbatim literal, now `check_sometimes!(max > 4, …)` with `{left, right}` details (the macros have no numeric-guidance form) | logs.rs |
| H/logs.rs:118 | Reachable: the driver receives ledger node logs | verbatim | logs.rs `handle_datagram` |
| H/logs.rs:119 | Reachable: the driver receives client logs | verbatim | logs.rs `handle_datagram` |
| H/clients.rs:170 | Reachable: the driver notices a client restart | verbatim | clients.rs `observe_boot` |
| H/clients.rs:211 | Unreachable: the client program accepts every request the driver builds | verbatim | clients.rs `call_with_op` |
| H/main.rs:1037 | Reachable: a quiet period starts | verbatim | quiet.rs `run_quiet` (Q0) |
| H/main.rs:1051 | Sometimes: the system recovers after faults stop | verbatim | quiet.rs (Q1) |
| H/main.rs:1057 | Sometimes: the full state check completes in a quiet period | deleted here; inherited by 11 (exact hook) | – |
| H/main.rs:1078 | Reachable: a late completion event resolves an operation | renamed: `a late completion event ends a tracked write` (the late outcome now ends a ticket) | write.rs `finish` |
| H/main.rs:1353 | Reachable: the workload starts | verbatim | setup.rs `run` |
| H/main.rs:194, 197, 201, 206, 233, 241, 258, 266, 267, 364, 365 | create_transaction / record_created / resolve_tx (`an acknowledged transaction has exactly the postings that were sent`, `an acknowledged transaction carries its reference`, `every answer for one transaction gives the same transaction id`, `transaction ids are never reused within a ledger`, `a successful single-request Apply returns exactly one log`, `a fresh-reference transaction is never skipped`, `a forced transaction is never rejected for insufficient funds`, `a transfer is rejected for insufficient funds`, `a write gets an ambiguous outcome`, `a forced transaction commits`, `an unforced transfer between accounts commits`) | deleted (Rust model retired); inherited by 02 (oracle commit check) | – |
| H/main.rs:400, 404, 406, 409, 413, 414, 415 | duplicate_reference (`a reference never maps to two transactions`, `only a skip-tolerant order is skipped`, `a skipped duplicate reference names the existing transaction`, `a duplicate reference is skipped`, `a skip-tolerant duplicate reference is skipped, not rejected`, `a duplicate reference is rejected as a reference conflict`, `a duplicate reference is rejected`) | deleted; inherited by 02 | – |
| H/main.rs:455, 462, 472, 475, 476, 482 | idempotent_replay (`reusing an idempotency key with another payload never creates a second transaction`, `a changed payload under a used idempotency key is refused`, `a rejected write stays rejected when its idempotency key is replayed`, `a replay resolves an ambiguous write`, `a replay returns an acknowledged transaction`, `replaying an acknowledged write under its idempotency key never fails`) | deleted; inherited by 03 | – |
| H/main.rs:550, 563, 575, 582, 585 | revert (`a revert reverts the requested transaction`, `a revert moves exactly the original postings back`, `a second revert is refused as already reverted`, `a transaction is reverted at most once`, `one of several concurrent reverts wins`) | deleted; inherited by 03 | – |
| H/main.rs:614, 615 | metadata (`deleting a missing metadata key is refused`, `a metadata key is deleted`) | deleted; inherited by 03 | – |
| H/main.rs:635, 641, 644, 649 | read_account (`an account's balance equals its input minus its output`, `account metadata holds only values the workload wrote`, `a linearizable read returns the latest metadata value`, `a metadata read overlaps a write to the same key`) | deleted; inherited by 05 | – |
| H/main.rs:677, 682, 685, 689 | read_transaction (`a linearizable read finds every acknowledged transaction`, `a transaction reads back as it was acknowledged`, `a linearizable read of a reverted transaction shows it reverted`, `a stale read misses a recent transaction`) | deleted; inherited by 05 (linearizable) and 14 (stale) | – |
| H/main.rs:322, 715, 718, 723, 727 | list_all / list_pages (`a list page never exceeds the requested page size` ×2, `paging never returns an item twice`, `list results are strictly ordered across pages`, `a listing spans several pages`) | deleted; inherited by 06 | – |
| H/main.rs:745 | Always: double entry: an asset's total input equals its total output | deleted; inherited by 05/14 | – |
| H/main.rs:776, 777, 779, 788, 794 | batch (`a batch with a failing transaction never succeeds`, `a successful batch returns one log per request`, `a batch's logs have increasing sequences`, `an atomic batch commits`, `a doomed batch is rejected as a whole`) | deleted; inherited by 02 | – |
| H/main.rs:816 | Sometimes: a numscript transaction commits | deleted; inherited by 13 | – |
| H/main.rs:829, 830 | barrier_monotonic (`a barrier's commit index is positive`, `the barrier commit index never goes back`) | deleted; inherited by 12 | – |
| H/main.rs:863, 869, 878, 881, 887, 894 | ledger_lifecycle (`a write to an ephemeral ledger commits`, `a ledger is deleted`, `a deleted ledger accepts no writes`, `a write to a deleted ledger is refused as deleted`, `a deleted ledger's name is never reused`, `a deleted ledger is not listed as live`) | deleted; inherited by 04 | – |
| H/main.rs:916, 920, 924, 928, 934, 936 | admin (`leadership is transferred on request`, `the primary store is compacted`, `the secondary store is compacted`, `a checkpoint is created`, `a successful backup contains files`, `a backup completes`) | deleted; inherited by 12 | – |
| H/main.rs:950 | Sometimes: a write during maintenance is refused | deleted; inherited by 04 | – |
| H/main.rs:966, 969 | admin sinks (`an added events sink is listed`, `an events sink is removed`) | deleted; inherited by 12 | – |
| H/main.rs:998, 1001, 1002, 1007, 1008 | account_types (`a balanced transient transaction commits`, `a transient account never keeps a balance`, `an unbalanced transient transaction is refused as non-zero`, `strict enforcement refuses addresses outside the chart`, `an address outside the chart is refused as not matching a type`) | deleted; inherited by 02 (transient) and 04 (chart) | – |
| H/main.rs:1022, 1023 | check_store (`the store integrity check finds no error`, `a store integrity check completes`) | deleted; inherited by 12 | – |
| H/main.rs:1107, 1117, 1121, 1123, 1127, 1130, 1134, 1137, 1145, 1149, 1153, 1156, 1186, 1190, 1200, 1201 | check_state (listing, references, reverts, rejected-never-takes-effect, survival, balances, stats) | deleted (Rust model retired); oracle-checked, inherited by 11 | – |
| H/main.rs:1223, 1242 | check_nodes_converge (`every node catches up after faults stop`, `every node holds the same account state at a common index`) | deleted; `nodes.rs` fixes the leader-index bug (DESIGN §6.10); inherited by 15 (and 11 for 1223) | – |

## New properties (00c)

| Literal | Kind | Where |
|---|---|---|
| the workload starts | Reachable | setup.rs (kept) |
| the fault pause command exists in Antithesis | Always | setup.rs (only under `ANTITHESIS_OUTPUT_DIR`) |
| a quiet period starts | Reachable | quiet.rs Q0 (kept) |
| the system recovers after faults stop | Sometimes | quiet.rs Q1 (kept) |
| every handed-off write resolves during a quiet period | Sometimes | quiet.rs Q3 |
| every oracle ticket has an owner in the driver | Always | quiet.rs ticket audit (Q3 start and end) |
| the drawn quiet check completes within its budget | Sometimes | quiet.rs Q5 (true when no hook is registered) |
| the fault pause command succeeds | Always | quiet.rs `pause_faults` (only under `ANTITHESIS_OUTPUT_DIR`) |
| a tracked write needs more than one attempt and then commits | Sometimes | write.rs `finish` |
| a late completion event ends a tracked write | Reachable | write.rs `finish` |
| a tracked write is handed off to a resolver | Sometimes | write.rs `handoff` |
| a resolver settles a handed-off write | Sometimes | resolver.rs `resolve_once` |
| a write attempt is refused as a stale proposal | Sometimes | write.rs `explore_attempt` |
| a write attempt hits a preload miss | Sometimes | write.rs `explore_attempt` |
| a write attempt is blocked by clock skew | Sometimes | write.rs `explore_attempt` |
| a driver rule panics | Unreachable | machine.rs `guarded` |
| a driver helper thread panics | Unreachable | machine.rs `helper_iteration` (resolvers, poster, jobs, log hooks) |
| the oracle service stays reachable | Unreachable | oracle.rs |
| the oracle service never restarts | Unreachable | oracle.rs (boot change, `unknown_ticket`) |
| the oracle accepts every request the driver sends | Unreachable | oracle.rs (`unmodeled`, `protocol`) |
| the driver never exceeds the oracle's in-flight cap | Unreachable | oracle.rs (`busy`) |
| a tracked write reaches a definitive outcome once faults stop | Unreachable | resolver.rs (3 Q3 timeouts survived) |
| a tracked write outcome fits the client's message limit | Unreachable | write.rs `attempt` |
| a tracked write plan has between 1 and 64 requests | Unreachable | write.rs `begin_write` (driver bug: oversized or empty plan, not sent) |
| the oracle answers one view per view request | AlwaysOrUnreachable | read.rs `view`, `read_begin` |

## Reachability of the must-hit assertions

| Literal | Reached by | Needs |
|---|---|---|
| the workload starts | setup | no faults |
| a quiet period starts | quiet invariant | no faults (`QUIET_PROBABILITY > 0` locally) |
| the system recovers after faults stop | quiet Q1 | no faults (`QUIET_PROBABILITY > 0`) |
| every handed-off write resolves during a quiet period | quiet Q3 | no faults (`QUIET_PROBABILITY > 0`) |
| every oracle ticket has an owner in the driver | quiet Q3 audit | no faults (`QUIET_PROBABILITY > 0`) |
| the drawn quiet check completes within its budget | quiet Q5 | no faults (`QUIET_PROBABILITY > 0`) |
| the fault pause command exists in Antithesis | setup | Antithesis only (whitelisted by `run_native.sh`) |
| the fault pause command succeeds | quiet Q0/Q5 | Antithesis only (whitelisted by `run_native.sh`) |
| a tracked write needs more than one attempt and then commits | smoke_tx / any tracked write | faults |
| a late completion event ends a tracked write | smoke_tx / any tracked write | faults |
| a tracked write is handed off to a resolver | smoke_tx / any tracked write | faults |
| a resolver settles a handed-off write | resolvers | faults |
| a write attempt is refused as a stale proposal | any tracked write | faults (+ heterogeneous cache knobs, DESIGN §8) |
| a write attempt hits a preload miss | any tracked write | faults |
| a write attempt is blocked by clock skew | any tracked write | faults (clock jitter) |
| H/logs.rs leadership/restart/log-receipt literals | log receiver | faults for restarts and leadership changes; log receipt needs the compose log forwarders (not `run_native.sh`) |
| the driver notices a client restart | any call after a client restart | faults |

Unit tests cover the paths of every new assertion (`cargo test`): dispatch (retry, late,
truncated, handoff, resolver, abandon, follow-up, message-size), slots and back-pressure,
quiet Q1–Q6 (full, Q3 timeout, leaked ticket, unwind), the oracle client (retry/degraded,
boot change, error codes), `guarded`, jobs, nodes, reads, wire, draws, fixtures, markers.

## API notes for families

- `check_*!(cond, "literal", details)`: `details` is a `serde_json::Value` or `&Value`
  (optional). The sink is thread-local; helper threads spawned by the driver inherit it.
- `draw::owned_address(&Shared, account_type, tag)` gives `<type>:own-<tag><N>` from
  `Shared::next_own` (DESIGN §6.8 lists it as `owned_address(tag)`; the type prefix and the
  counter source are explicit arguments). `Shared::{next_own_ledger, next_own_ref}` give the
  other reserved names.
- `draw::route(tc, &Shared)`, `draw::schedule(tc, &Shared, n)`, `draw::backoffs(tc, n)`:
  routes need the client count and node list, so they take `&Shared`.
- `WritePlan::new(family, requests)` fills defaults; `WritePlan::drawn(…, d, tc)` also
  draws `track_key` and an 8-entry schedule and backoff list. A `Role::Conflict { of }`
  plan takes the key of `Shared::flagged_plan(of)`; it is refused locally (not sent) when
  `of` is not among the newest 2048 flagged originals.
- `OracleRefusal { kind: OracleError, status, message, details }`; `OracleError` adds
  `Degraded` (nothing sent), `Restarted` and `Other` to the DESIGN §4.1 codes.
- `Clients::watch(op)` before a raw `Driver::call` keeps that op's late UDP outcome for
  `Clients::wait_late`; events of unwatched, un-ended ops are dropped (bounded memory).
- `QuietCtx::section` returns the whole `/quiet/section` response (`verdict`, `next`).
- `testkit::{driver, FakeTransport, FakeReply, FakeOracle, run_rule, with_tc_n, assertions,
  reset, fired, wait_until, StopTestLike}`; `Shared::for_tests(ft, fo)` runs with 3 clients,
  3 nodes, run id `test`, `QUIET_PROBABILITY = EXCLUSIVE_PROBABILITY = 1` and short deadlines.

## Requests to 99

- The 00c write-path exploration assertions are fault-only; never delete them for being
  unhit in a fault-free local run.
- `run_native.sh` must whitelist `the fault pause command exists in Antithesis` and
  `the fault pause command succeeds` (00d) and the "faults" rows above.

---

<a id="00a-oracle-core"></a>

## Porting record — 00a Go oracle core

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

---

<a id="00e-client-compose"></a>

## 00e — Go client program and compose files

Client program `tests/antithesis/workload/bin/client` (DESIGN §7 C1–C7) and compose/env
(DESIGN §2.3, §2.4, §8).

## Ported classification (C3)

The old workload asserted, in a gRPC interceptor on every connection, that every RPC error
is classified. The client now asserts the same property after every RPC it sends (`/op`,
every `/digest` RPC, and extensions through `assertClassified`), on the final error of the
call, including mid-stream `Recv` errors (the old interceptor only saw stream creation).

| Old source | Old assertion (kind: literal) | New | Where |
|---|---|---|---|
| `W/internal/client.go:212-232` (`classifyUnaryInterceptor`) | Always: every RPC error must be classified (workload predicate set complete) | Always: `client: every RPC error the client returns is classified`, details `{method, code, reason, node}` | `CL/classify.go` `assertClassified`, called from `CL/main.go` `callRoute` |
| `W/internal/client.go:234-259` (`classifyStreamInterceptor`) | same literal | same new literal; covers stream creation **and** mid-stream errors | `CL/main.go` `call` (stream path), `CL/digest.go` `digester.rpc` |

Predicate mapping (`W/internal/client.go` → `CL/classify.go` `classified`):

| Old predicate | Codes / reasons | New |
|---|---|---|
| `err == nil` | – | `out.OK` |
| `IsUnavailable` (`:297`) | Unavailable | kept |
| `IsDeadlineExceeded` (`:311`) | DeadlineExceeded | kept |
| `IsExternalServiceError` (`:437`) | reason `EXTERNAL_SERVICE_ERROR` under any code | kept |
| `IsWritesBlockedDiskFull` (`:380`) | ResourceExhausted + `WRITES_BLOCKED_DISK_FULL` | kept (exact code + reason tuple) |
| `IsCanceled` (`:343`) | Canceled | kept |
| `isBusinessError` (`:532`) | NotFound, AlreadyExists, InvalidArgument, FailedPrecondition | kept |
| – (new, DESIGN §7 C3) | Unauthenticated, PermissionDenied | classified: signing is exercised (task 16, main-drivers §3.36) |
| – (new) | Unimplemented | classified only when `node` starts with `restore-` (RestoreService on a restore-agent child not in restore mode) |
| – (new) | `local: true` outcomes (dial failure, marshalling, client-side message-size limit) | classified: no server produced them |
| not classified (old and new) | Aborted, Internal, Unknown, OutOfRange, DataLoss, other ResourceExhausted, Unimplemented elsewhere | Always fails |

## New properties

- `client: every RPC error the client returns is classified` (Always; reached by every RPC
  of every rule; no faults needed for the true branch).

## Reachability

| Assertion | Kind | Reached by |
|---|---|---|
| `client: every RPC error the client returns is classified` | Always | every `/op` (no faults) |

## Client changes (DESIGN §7)

- C1 `CL/routes.go`: `route` `""`/`lib` (round-robin + `actions.GRPCRetryPolicy`), `plain`
  (round-robin, no retry policy), `node` (direct, no retries). Every connection dials with
  `WithDefaultCallOptions(MaxCallRecvMsgSize(64<<20), MaxCallSendMsgSize(64<<20))`. Unknown
  route → HTTP 400.
- C2 `CL/main.go`: `local`, `peer`, `elapsedMs` (omitted when zero/false, as the fixture
  README says). The connections dial through a dialer that reports the dialed address as
  the remote address, so `peer` reads `ledger-1:8888` (the name the driver uses) rather than
  an IP. A ResourceExhausted whose message is one of grpc-go's own size-limit messages and
  that carries no ErrorInfo is `local` (the server's limits are 64 MiB too, so a server
  never produces one for a request the client let through).
- C4 `CL/sign.go`: `sign: {keyId, seed}` on bucket `Apply`; `POST /pubkey`.
- C5: `services["restore"]`; a restore call without `node` → HTTP 400.
- C6 `CL/digest.go`: `POST /digest` (format documented in the file header).
- C7 `CL/ext.go`: `registerExt(pattern, h)`; handlers reach the client through
  `extServer()`.
- Shutdown `CL/main.go` `serve`: on SIGTERM/SIGINT the client stops accepting requests,
  waits up to 8 s (`shutdownGrace`, under Docker's 10 s stop timeout) for the open HTTP
  requests and for every in-flight RPC (including those whose HTTP request already gave
  up) to finish and report, and only then closes its gRPC connections. An RPC cut off by
  that close fails with grpc-go's own "grpc: the client connection is closing"
  (Canceled, or Unavailable carrying it). That outcome is not a server answer: it is
  never reported (no UDP event, and the HTTP connection is dropped through
  `http.ErrAbortHandler`), so the driver sees no answer. The check needs both that error
  shape and the client's own `closing` flag, so a server answering Canceled with the same
  message is still reported as a remote Canceled. After close, a new node connection is a
  local `dial:` error (no answer for the driver). `shutdown_test.go`.

### Deviations

- **Signing payload (C4).** `signing.Sign` serializes with `MarshalVT`, whose map
  iteration (request metadata, account types) is unordered, so two signings of one batch
  can produce different payload bytes, contradicting "deterministic, so retries resend
  identical bytes". `signBatch` builds the same `SignedApplyBatch` envelope (`keyId`,
  `ed25519.Sign(key, payload)`, `payload`) over `proto.MarshalOptions{Deterministic:true}`
  bytes. `sign_test.go` verifies it with `signing.Verify` and `signing.ExtractBatch` and
  checks byte-identical payloads and signatures over repeated signings of a 20-key batch.
- **Digest extras (C6).** Additive fields beyond DESIGN's shape: request `route` and
  `pageSize` (default 100); response `boot`, `opId`, per-ledger breakdown
  `sections.<name>.ledgers.<ledger> = {count, sha256, lastKey}` for per-ledger sections
  (restore.md §5.2: per-ledger hashing localises a failure), and `outcome` (the failing
  RPC's outcome) next to `error`. Per-ledger section hashes include the framed ledger name
  before each ledger's items. Empty `sections` means all sections. Without `ledgers`, deleted
  ledgers (`deletedAt` set) are skipped by the per-ledger sections. `indexes` uses
  `ListIndexes{SCOPE_ALL}` (filtered to `ledgers` when given); `sinks` digests the sink
  configurations only; `checkpoints` uses `ClusterService.ListQueryCheckpoints`.

## Compose (DESIGN §2.3, §8)

- `x-qcl: &qcl "10"` used by ledger `QUERY_CHECKPOINT_LIMIT`, oracle and driver.
- Ledger env: `DEBUG`, `SNAPSHOT_THRESHOLD` removed; `LOG_LEVEL=debug`,
  `RAFT_COMPACTION_MARGIN=100`, `MAINTENANCE_INTERVAL=5s`, `SPOOL_SEGMENT_MAX_BYTES=4096`,
  `RAFT_REPLAY_BATCH_SIZE=32`, `SNAPSHOT_PARALLELISM=1`, `PEBBLE_MAX_CHECKPOINTS=3`,
  `IDEMPOTENCY_TTL=0s`; per node `CACHE_ROTATION_THRESHOLD` 50/64/80 and
  `BLOOM_VOLUMES_EXPECTED_KEYS` 1000000/1000001/1000002.
- `oracle` service (image `ledger-hegel-oracle`, `HG/oracle/Dockerfile` from 00a),
  entrypoint `/usr/local/bin/oracle`, `ORACLE_ADDR=:8090`, no `forward-logs`, no
  `ORACLE_DUMP_BATCHES`.
- Driver env `ORACLE_URL`, `QUERY_CHECKPOINT_LIMIT`.
- Empty `# ==== 18-restore` marker pairs at the end of `services:` and of `volumes:`.
- `docker-compose.local.yaml`: driver `QUIET_PROBABILITY`, `EXCLUSIVE_PROBABILITY`,
  `ANTITHESIS_SDK_LOCAL_OUTPUT`; oracle `ORACLE_JOURNAL_FILE=/journal/oracle.jsonl`,
  `ORACLE_DUMP_BATCHES=1`, `ANTITHESIS_SDK_LOCAL_OUTPUT=/journal/sdk.jsonl`, named volume
  `journal`.

## Requests to 99

- The client's classification assertion literal changed from the old workload literal; the
  Antithesis report shows it under the new name.
- `HG/client/Dockerfile` unchanged: the new imports (`restorepb`, `signaturepb`) live in the
  root module directories it already copies.

---

<a id="02-tx-writes"></a>

## 02 — Transaction writes: porting record

Rules (`HG/driver/src/rules/tx.rs`, tests in `rules/tx/tests.rs`): `tx_bulk` 14,
`tx_local_bulk` 2, `tx_transient` 1.5, `tx_reference_race` 1.0, `tx_reference_reuse` 0.8,
`tx_drain_probe` 0.8, `tx_boundary` 0.6, `tx_stress` 0.4. Order generators: `tx_chaos` 10,
`tx_drain_ephemeral` 2.5, `tx_dup_ref_skip` 0.6, `tx_rejected` 0.6, `tx_skip_fresh` 0.3.
Setup hook `tx_setup` (creates `r-stress-<run>`). Oracle: `W/oraclecheck/applysurface`
(package `applysurfacecheck`: drain hook, failure hook, Raw probe) and `W/bin/oracle/h_tx.go`
(asserters). Rejection fixtures `W/oraclecheck/testdata/rejections/02-*.json`. Go-produced
protocol fixtures `HG/protocol-fixtures/02/` (regenerate with
`UPDATE_FIXTURES=1 go test ./oraclecheck/applysurface -run TestProtocolFixtures02`).

## Assertion mapping

| Old source | Old assertion (kind: literal) | New | Where |
|---|---|---|---|
| M/validate.go:551 | Reachable: singleton_driver_model: insufficient-funds rejection exercised | oracle: insufficient-funds rejection exercised | h_tx.go (failure hook) |
| M/validate.go:553 | Reachable: singleton_driver_model: reference-conflict rejection exercised | oracle: reference-conflict rejection exercised | h_tx.go |
| M/validate.go:555 | Reachable: singleton_driver_model: volume-overflow rejection exercised | oracle: volume-overflow rejection exercised | h_tx.go |
| M/validate.go:557 | Reachable: singleton_driver_model: validation rejection exercised | oracle: validation rejection exercised | h_tx.go |
| – | – | new: oracle: transient non-zero rejection exercised (Reachable) | h_tx.go |
| M/validate.go:559,561,547 | Reachable idempotency-conflict / filter refused / checkpoint limit | not here (03, 08, 09) | – |
| M/apply_surface.go applyCoverageHits | Raw: coverage Apply skipped ERROR_REASON_TRANSACTION_REFERENCE_CONFLICT and continued | oracle: coverage Apply skipped TRANSACTION_REFERENCE_CONFLICT and continued (Raw, RegisterCoverage) | applysurface drain hook |
| M/apply_surface.go | Raw: skipped ALREADY_REVERTED / METADATA_NOT_FOUND / ACCOUNT_TYPE_* and continued; mode-setter probes; invalid skip opt-in | not here (03, 04 generate those reasons) | – |
| MD/parallel_driver_transactions/main.go:92 | Sometimes: should be able to create a transaction | oracle (every tracked create is validated; `a multi-order bulk commits`, `oracle: model outcome verified`) | – |
| MD/parallel_driver_transactions/main.go:117 | Sometimes: should be able to create bulk transactions | verbatim (success or EXECUTION_PLAN_TOO_LARGE) | tx.rs tx_stress job |
| MD/parallel_driver_transactions/main.go:126 | AlwaysOrUnreachable: bulk Apply should return one log per request | verbatim | tx.rs tx_stress job |
| MD/parallel_driver_transactions/main.go:157 | AlwaysOrUnreachable: should always be able to read committed transaction | oracle (point reads, task 05) | – |
| MD/parallel_driver_bulk/main.go:85 | Sometimes: bulk Apply should succeed | renamed: a multi-order bulk commits (Sometimes) | tx.rs tx_bulk / tx_local_bulk |
| MD/parallel_driver_bulk/main.go:92 | AlwaysOrUnreachable: bulk Apply should produce at least 3 logs | oracle: committed log count mismatch (00a) | – |
| MD/parallel_driver_bulk/main.go:116 | AlwaysOrUnreachable: bulk metadata should be readable after write | oracle (account reads, task 05; quiet exact state, task 11) | – |
| MD/parallel_driver_bulk_atomicity/main.go:155 | Sometimes: mid-failing atomic bulk definitively rejected | verbatim | tx.rs tx_bulk / tx_local_bulk (doomed) |
| MD/parallel_driver_bulk_atomicity/main.go:277,281,285 | Always: failed atomic bulk yields no success audit entry…; audit failure entries are unique per proposal; failure audit entry observed… | oracle (audit, task 10) | – |
| MD/parallel_driver_bulk_atomicity/main.go:295,302 | Always: failed atomic bulk leaves no partial transaction effects / account activity | oracle: bulk failure not explained by any serialization (00a) + quiet exact state (11) | – |
| MD/parallel_driver_insufficient_funds/main.go:62 | Unreachable: funding should not fail | verbatim | tx.rs tx_drain_probe |
| MD/parallel_driver_insufficient_funds/main.go:90 | Unreachable: exact balance transfer should succeed | verbatim | tx.rs tx_drain_probe |
| MD/parallel_driver_insufficient_funds/main.go:102,109,115 | Unreachable: empty-account precondition read failed / drained account has no readable balance / drain did not leave an empty account | oracle (the drain's success is validated against the model; account reads task 05) | – |
| MD/parallel_driver_insufficient_funds/main.go:140 | Unreachable: overdraft without Force should fail | verbatim | tx.rs tx_drain_probe |
| MD/parallel_driver_insufficient_funds/main.go:150 | AlwaysOrUnreachable: overdraft should return INSUFFICIENT_FUNDS | verbatim | tx.rs tx_drain_probe |
| MD/parallel_driver_insufficient_funds/main.go:157 | Reachable: insufficient funds path exercised | verbatim | tx.rs tx_drain_probe |
| MD/parallel_driver_insufficient_funds/main.go:173 | Unreachable: overdraft with Force should succeed | verbatim | tx.rs tx_drain_probe |
| MD/parallel_driver_reference_conflict/main.go:38 | Sometimes: should be able to create a transaction with reference | oracle (tx_chaos creates always carry a reference) | – |
| MD/parallel_driver_reference_conflict/main.go:72 | Unreachable: duplicate reference should be rejected | verbatim | tx.rs tx_reference_reuse |
| MD/parallel_driver_reference_conflict/main.go:82 | AlwaysOrUnreachable: duplicate reference should return TRANSACTION_REFERENCE_CONFLICT | verbatim | tx.rs tx_reference_reuse |
| MD/parallel_driver_reference_conflict/main.go:86 | Reachable: reference conflict path exercised | verbatim | tx.rs tx_reference_reuse |
| MD/parallel_driver_reference_conflict_skip/main.go:66 | Sometimes: skip-tolerant first-claim should be able to create a transaction with a reference | oracle (the original is any acked l2 reference) | – |
| MD/parallel_driver_reference_conflict_skip/main.go:107 | Unreachable: skip-tolerant duplicate reference must not return a business error | verbatim | tx.rs tx_reference_reuse |
| MD/parallel_driver_reference_conflict_skip/main.go:117,125 | Unreachable: …must return exactly one log / …must return an Apply log | folded into the next row (a missing log or Apply log fails it) | tx.rs tx_reference_reuse |
| MD/parallel_driver_reference_conflict_skip/main.go:132 | AlwaysOrUnreachable: skip-tolerant duplicate reference must land as an OrderSkipped log | verbatim | tx.rs tx_reference_reuse |
| MD/parallel_driver_reference_conflict_skip/main.go:140 | AlwaysOrUnreachable: OrderSkipped reason must be TRANSACTION_REFERENCE_CONFLICT | verbatim (reason = the `details` arm) | tx.rs tx_reference_reuse |
| MD/parallel_driver_reference_conflict_skip/main.go:145 | AlwaysOrUnreachable: OrderSkipped.context.reference must match the requested reference | verbatim, exact compare | tx.rs tx_reference_reuse |
| MD/parallel_driver_reference_conflict_skip/main.go:150 | AlwaysOrUnreachable: OrderSkipped.context.ledger must match the target ledger | verbatim, exact compare | tx.rs tx_reference_reuse |
| MD/parallel_driver_reference_conflict_skip/main.go:156 | AlwaysOrUnreachable: OrderSkipped.context.existingTransactionId must point to the first tx | verbatim, exact compare (fixes main-drivers §1.3 #2) | tx.rs tx_reference_reuse |
| MD/parallel_driver_reference_conflict_skip/main.go:160 | Reachable: reference-conflict skip path exercised | verbatim | tx.rs tx_reference_reuse |
| MD/parallel_driver_reference_conflict_skip/main.go:190,198,207,218 | fresh-reference first claim with the opt-in (Sometimes/Reachable/Unreachable ×2) | oracle: `tx_skip_fresh` and `tx_chaos` opt-ins on fresh references are predicted by the model (a fired skip there is `oracle: skipped-order response mismatch`) | – |
| MD/parallel_driver_reference_race/main.go:204 | Always: at most one concurrent same-reference create is acked | verbatim | tx.rs tx_reference_race |
| MD/parallel_driver_reference_race/main.go:211 | Sometimes: concurrent same-reference race produced a winner and a conflict loser | verbatim (only when every racer was definitive) | tx.rs tx_reference_race |
| MD/parallel_driver_reference_race/main.go:134,139 | Always: an acknowledged reference create remains visible / a reference never maps to more than one committed transaction | oracle (reads 05/06; quiet exact state 11) | – |
| MD/parallel_driver_reference_race/main.go:260,264 | Unreachable/AlwaysOrUnreachable: aged reference reuse … | covered by tx_reference_reuse on any acked (old) l2 reference + oracle | – |
| MD/parallel_driver_transient_accounts/main.go:47 | Unreachable: should be able to add transient account type | dropped: l2's transient type `t-2` is created at setup (00c) | – |
| MD/parallel_driver_transient_accounts/main.go:99 | Sometimes: balanced transient batch should succeed | verbatim | tx.rs tx_transient |
| MD/parallel_driver_transient_accounts/main.go:104 | Reachable: balanced transient batch succeeded | verbatim | tx.rs tx_transient |
| MD/parallel_driver_transient_accounts/main.go:132 | Unreachable: unbalanced transient batch should fail | verbatim | tx.rs tx_transient |
| MD/parallel_driver_transient_accounts/main.go:142 | AlwaysOrUnreachable: unbalanced transient batch should return TRANSIENT_ACCOUNT_NON_ZERO | verbatim | tx.rs tx_transient |
| MD/parallel_driver_transient_accounts/main.go:146 | Reachable: transient account non-zero path exercised | verbatim | tx.rs tx_transient |
| MD/parallel_driver_definitive_errors/main.go:152 | Sometimes: definitive write rejection recorded for absence check | renamed: a 256-bit boundary amount is definitively rejected (new literal; the amount half only) | tx.rs tx_boundary |
| MD/parallel_driver_definitive_errors/main.go:206 | Always: definitively rejected write never appears in the ledger | oracle: bulk failure not explained by any serialization + quiet exact state (11) | – |
| MD/parallel_driver_definitive_errors (invalid asset) | InvalidArgument class | not here (task 14, untracked) | – |
| MD/parallel_driver_timestamp_order/main.go:117,220,223 | timestamp order write set / ordering Always | not here (list ordering is task 06); only the backdated `timestamp` field of `tx_chaos` is ported | – |
| H/main.rs:364 | Sometimes: a forced transaction commits | oracle (forced creates validated); dropped as a Rust literal | – |
| H/main.rs:365 | Sometimes: an unforced transfer between accounts commits | oracle; dropped as a Rust literal | – |
| H/main.rs:400 | Always: a reference never maps to two transactions | oracle (commit check; quiet exact state 11) | – |
| H/main.rs:404 | Always: only a skip-tolerant order is skipped | oracle: skipped-order response mismatch (00a) | – |
| H/main.rs:406 | Always: a skipped duplicate reference names the existing transaction | superseded by the exact-field AlwaysOrUnreachables above | tx.rs tx_reference_reuse |
| H/main.rs:409 | Reachable: a duplicate reference is skipped | superseded by `reference-conflict skip path exercised` + the oracle Raw probe | – |
| H/main.rs:413,414,415 | Always/Sometimes: duplicate reference skipped not rejected / rejected as conflict / is rejected | superseded by the verbatim MD literals above | tx.rs tx_reference_reuse |
| H/main.rs:776 | Always: a batch with a failing transaction never succeeds | oracle: model rejects a server-committed bulk (00a) | – |
| H/main.rs:777 | Always: a successful batch returns one log per request | oracle: committed log count mismatch (00a) | – |
| H/main.rs:779 | Always: a batch's logs have increasing sequences | oracle (commit check / drain order, 00a) | – |
| H/main.rs:788 | Reachable: an atomic batch commits | superseded by `a multi-order bulk commits` | tx.rs |
| H/main.rs:794 | Sometimes: a doomed batch is rejected as a whole | superseded by `mid-failing atomic bulk definitively rejected` | tx.rs |
| H/main.rs:998 | Sometimes: a balanced transient transaction commits | superseded by `balanced transient batch should succeed` | tx.rs tx_transient |
| H/main.rs:1001,1002 | Always: a transient account never keeps a balance / refused as non-zero | superseded by the verbatim transient literals + oracle | tx.rs tx_transient |
| H/main.rs:1007,1008 | Always: strict enforcement … / not matching a type | not here (task 04, enforcement) | – |

## New properties

- `a multi-order bulk commits` (Sometimes, Rust).
- `a 256-bit boundary amount is definitively rejected` (Sometimes, Rust).
- `oracle: transient non-zero rejection exercised` (Reachable, oracle).
- The REFERENCE_CONFLICT "and continued" probe is new under the `oracle: ` name
  (spec literal uses the short reason name).

## Reachability (must-hit)

| Assertion | Reached by | Needs |
|---|---|---|
| a multi-order bulk commits | tx_bulk, tx_local_bulk | no faults |
| at most one concurrent same-reference create is acked (Always) | tx_reference_race | no faults |
| concurrent same-reference race produced a winner and a conflict loser | tx_reference_race | no faults |
| reference conflict path exercised; reference-conflict skip path exercised | tx_reference_reuse (needs an acked chaos reference on l2) | no faults |
| insufficient funds path exercised | tx_drain_probe | no faults |
| balanced transient batch should succeed / succeeded; transient account non-zero path exercised | tx_transient | no faults |
| mid-failing atomic bulk definitively rejected | tx_bulk, tx_local_bulk (doomed 1/10) | no faults |
| a 256-bit boundary amount is definitively rejected | tx_boundary | no faults |
| should be able to create bulk transactions | tx_stress (job) | no faults |
| oracle: insufficient-funds / reference-conflict / volume-overflow / validation / transient non-zero rejection exercised | tx_drain_probe, tx_boundary, tx_bulk (tx_rejected, chaos on l2 transient types), tx_transient, tx_reference_reuse | no faults |
| oracle: coverage Apply skipped TRANSACTION_REFERENCE_CONFLICT and continued (Raw) | tx_local_bulk / tx_bulk: tx_dup_ref_skip or a rebinding tx_chaos opt-in followed by an order (a later generated order or the trailing successor) | no faults |

## Deviations and decisions

- `tx_chaos` source: `world` 1/2, else `draw::pool_address` (as `M/actions.go sourceAddress`);
  destination `draw::pool_address`. `draw::address` would also yield `users:` and `world`
  destinations, which the singleton never generated.
- `tx_chaos` offers the opt-in (`has_opt_in`): with it, REFERENCE_CONFLICT is attached and half
  rebind the reference to an existing one (`M/apply_surface.go maybeAddSkippableReason`).
  `tx_dup_ref_skip` and `tx_skip_fresh` always carry the opt-in.
- `tx_drain_probe`: the 00c API releases a slot unit when its ticket ends, so each of the four
  steps takes a fresh unit (at most one held at a time); a step without a free unit ends the
  chain. The chain budget is 12 s (400 ms in unit tests).
- `tx_reference_reuse`: `seek(txByRef)` gives the reference and the first id, not the body.
  The original postings come from a bounded book (`RefBook`) of l2 creates acked by this
  family's bulks; the conflict body mutates them (destination `fees` ↔ other, or amount + 1
  wrapping at 2^256 − 1). It always also carries the reserved metadata key `own-conflict`,
  which guarantees different business bytes when the original is not in the book (no
  generator writes `own-` keys). The server's reference check does not depend on the body.
- `tx_reference_race` runs on l2 (never deleted) with destinations `users:<n>` (untyped; l2's
  enforcement is AUDIT), so no racer fails for funds or for transient balances.
- `tx_stress`: at most 64 posting templates are drawn and cycled across the bulk (each order
  keeps a unique reference `r-<run>-k<N>-ref-<i>`), so a 5000-order bulk does not cost ~30 000
  hegel draws. The ledger is `r-stress-<run>` as the spec names it.
- `tx_stress` admission (`StressGate`): at most one stress job queued or running (the rule
  returns before drawing otherwise; a concurrent winner is decided by a compare-and-swap after
  the draws), and at most `STRESS_ORDER_CAP = 100 000` stress orders per run. A stress job holds
  a job thread for up to 180 s, and the two job threads also serve other families'
  quiet-blocking jobs (04's maintenance disable, 12/18), which must not wait behind a stress
  backlog; the cap bounds `r-stress-<run>`, which is never deleted.
- `tx_setup` retries for 20 s (plus one send budget), not a minute, to leave the shared setup
  budget to other hooks; a stress job that gets `LEDGER_NOT_FOUND` sends the idempotent
  `createLedger` itself, so the next stress bulk finds the ledger.
- Taper on l2: `tx_transient`, `tx_reference_race` and `tx_drain_probe` commit on l2 (never
  deleted), so each reads l2's `txCount` first and applies the DESIGN §3.2 bands (≥ 4000
  never, 2000–4000 one in eight, 500–2000 half). `tx_reference_reuse` and `tx_boundary`
  commit no transaction (a conflict, an OrderSkipped log, or a rejection) and are not tapered.
- Multi-ledger bulks: the bulk size is at least the number of chosen ledgers and every chosen
  ledger gets at least one order, so a 2–3-ledger pick really spans its ledgers.
- `OrderSkipped.context.existingTransactionId` is compared exactly: the proto field is
  `optional`, so the server always renders it; an absent field fails the check (it is never
  read as 0).
- The "and continued" Raw probe is true when **any later** order of the bulk committed after
  the skip (spec wording); `M/apply_surface.go` required the immediately next order.
- The Raw probe is emitted only for committed bulks that carry a REFERENCE_CONFLICT opt-in
  (the singleton emitted every probe on every commit).

## Requests to 99

- No new dependency.
- Acceptance and swarm selection: hegel's swarm testing enables only a subset of the allowed
  rules per test case, so a single `run_native.sh` with all eight rules may never run a given
  rule (the first acceptance run never ran `tx_stress`). `tx_stress` was therefore also run
  alone (`RULES=tx_stress,tx_local_bulk`). 99 should check every 02 must-hit over the union of
  runs, not per run.

---

<a id="03-reverts-metadata-idempotency"></a>

## 03 — Reverts, metadata, idempotency replays and conflicts

Rust: `HG/driver/src/rules/{reverts,metadata,idempotency}.rs`. Go:
`W/oraclecheck/replaycheck/` (package `replaycheck`) and `W/bin/oracle/h_replay.go`.

One file outside the spec's ownership list: `W/bin/oracle/h_replay_test.go` (new; the
service-level and real-server tests of the Replay kind, which need package `main`'s test
helpers). It touches no other task's file, so it cannot conflict at merge.

| Rule | Weight | File |
|---|---|---|
| `meta_bulk` | 2 | `rules/metadata.rs` (compose_bulk over `revert`, `meta_account`, `meta_tx`, `meta_ledger`) |
| `revert_concurrent` | 2 | `rules/reverts.rs` |
| `revert_twice` | 0.6 | `rules/reverts.rs` |
| `idem_replay` | 2.5 | `rules/idempotency.rs` |
| `idem_conflict` | 1.5 | `rules/idempotency.rs` |
| `meta_read_after_write` | 0.8 | `rules/metadata.rs` |

Order gens (registered in `order_gens()` for task 02's `tx_bulk` too): `revert` 3 (opt-in
`TRANSACTION_ALREADY_REVERTED`), `meta_account` 3, `meta_tx` 1.5, `meta_ledger` 1.

## Old assertions

| Old source | Old assertion (kind: literal) | New | Where |
|---|---|---|---|
| MD/parallel_driver_revert_concurrent/main.go:36 | Sometimes: should be able to create a transaction for concurrent revert | dropped: the target is an existing committed transaction picked from `seek(txs)`; creation success is the oracle's (`oracle: model outcome verified`) | – |
| MD/parallel_driver_revert_concurrent/main.go:96 | AlwaysOrUnreachable: successful concurrent revert contains exactly the expected reversal | verbatim (in-place posting order, `revertsTransaction == target`, `id != target`: fixes main-drivers §1.3 #5) | rules/reverts.rs revert_concurrent |
| MD/parallel_driver_revert_concurrent/main.go:123 | Unreachable: concurrent revert returned unexpected error | oracle: bulk failure not explained by any serialization | (00a) |
| MD/parallel_driver_revert_concurrent/main.go:130 | AlwaysOrUnreachable: at most one concurrent revert should succeed | verbatim (definitive outcomes only; distinct keys, so a retried attempt replays instead of reporting already-reverted) | rules/reverts.rs revert_concurrent |
| MD/parallel_driver_revert_concurrent/main.go:140 | Reachable: concurrent revert path exercised | verbatim (k ≥ 2, at least one definitive outcome) | rules/reverts.rs revert_concurrent |
| MD/parallel_driver_revert_idempotent/main.go:34 | Sometimes: should be able to create a transaction for double revert | dropped: the rule's own forced create on l2; its outcome is the oracle's | – |
| MD/parallel_driver_revert_idempotent/main.go:75, MD/parallel_driver_revert_sequential/main.go:66 (`internal.CheckPostCommitVolumes`, `internal/checks.go:66-78`) | Always: post-commit volume entry has missing required fields; Always: post-commit volume input is invalid; Always: post-commit volume output is invalid | oracle: model post-commit volume mismatch (00a `crossCheckCommit` → `pcvMatches`: every model cell must be present on the revert's `postCommitVolumes` with equal, decodable input and output; a missing entry, input or output fails the match) | (00a) |
| MD/parallel_driver_revert_idempotent/main.go:64 | Sometimes: first revert should succeed | verbatim (when definitive; LEDGER_* reasons inconclusive) | rules/reverts.rs revert_twice |
| MD/parallel_driver_revert_idempotent/main.go:90 | Unreachable: GetTransaction should not fail after revert | dropped: no GetTransaction in the chain; GetTransaction answers are task 05's read kind | – |
| MD/parallel_driver_revert_idempotent/main.go:96 | AlwaysOrUnreachable: transaction should be marked as reverted after first revert | oracle (task 05 `GetTransaction` read validation: the model marks the original reverted) | – |
| MD/parallel_driver_revert_idempotent/main.go:115 | Unreachable: double revert should fail | verbatim | rules/reverts.rs revert_twice |
| MD/parallel_driver_revert_idempotent/main.go:126 | AlwaysOrUnreachable: double revert should return TRANSACTION_ALREADY_REVERTED | verbatim | rules/reverts.rs revert_twice |
| MD/parallel_driver_revert_idempotent/main.go:130 | Reachable: double revert path exercised | verbatim | rules/reverts.rs revert_twice |
| MD/parallel_driver_revert_sequential/main.go:30 | Sometimes: should be able to create a transaction for revert | dropped (as above) | – |
| MD/parallel_driver_revert_sequential/main.go:57 | Sometimes: should be able to revert a transaction | dropped: revert successes are judged by the oracle (`oracle: model outcome verified`, revert id/postings checks of crossCheckCommit) | – |
| MD/parallel_driver_revert_sequential/main.go:81 | AlwaysOrUnreachable: reverted transaction should be marked as reverted | oracle (task 05 GetTransaction read validation) | – |
| MD/parallel_driver_metadata/main.go:49 | Sometimes: should be able to save account metadata | dropped: write outcome judged by the oracle | – |
| MD/parallel_driver_metadata/main.go:68 | Always: metadata read-after-write should return saved value | verbatim (reserved account `t-0:own-meta<N>`, reserved key `own-k<N>`, after a definitive add; read is a linearizable GetAccount through a client, not posted to the oracle) | rules/metadata.rs meta_read_after_write |
| MD/parallel_driver_metadata/main.go:92 | Sometimes: should be able to delete account metadata | dropped (as :49) | – |
| MD/parallel_driver_metadata/main.go:110 | Always: deleted metadata key should be absent | renamed: `deleted metadata key is absent on a linearizable read` (Always; per the task spec) | rules/metadata.rs meta_read_after_write |
| MD/parallel_driver_tx_metadata/main.go:38 | Sometimes: should be able to create tx for metadata test | dropped (rule's own forced create) | – |
| MD/parallel_driver_tx_metadata/main.go:68 | Sometimes: should be able to save transaction metadata | dropped | – |
| MD/parallel_driver_tx_metadata/main.go:84 | AlwaysOrUnreachable: tx metadata read-after-write should return saved value | verbatim (rule-created tx with reference `o-<run>-own-ref<M>`) | rules/metadata.rs meta_read_after_write |
| MD/parallel_driver_tx_metadata/main.go:107 | Sometimes: should be able to delete transaction metadata | dropped | – |
| MD/parallel_driver_tx_metadata/main.go:123 | AlwaysOrUnreachable: deleted tx metadata key should be absent | verbatim (deterministic on the rule-owned tx and key) | rules/metadata.rs meta_read_after_write |
| MD/parallel_driver_idempotency/main.go:37,48 | Sometimes: should be able to create idempotent transaction (first)/(second) | dropped: the original is any flagged write; the replay outcome is the oracle's | – |
| MD/parallel_driver_idempotency/main.go:58 | Always: idempotent transactions should return the same ID | oracle: idempotency replay produced new log sequences / oracle: idempotency replay outside model (ids are part of the replayed orders) | replaycheck (h_replay.go) |
| MD/parallel_driver_idempotency_conflict/main.go:54 | Sometimes: should be able to create idempotent transaction | dropped (original from `replayable`) | – |
| MD/parallel_driver_idempotency_conflict/main.go:85 | Unreachable: reusing idempotency key with different payload should fail | verbatim (original committed) | rules/idempotency.rs idem_conflict |
| MD/parallel_driver_idempotency_conflict/main.go:95 | AlwaysOrUnreachable: mismatched idempotency key should return IDEMPOTENCY_KEY_CONFLICT | verbatim (original committed) | rules/idempotency.rs idem_conflict |
| MD/parallel_driver_idempotency_conflict/main.go:99 | Reachable: idempotency conflict path exercised | verbatim | rules/idempotency.rs idem_conflict |
| M/replay.go:97 | Unreachable: idempotency replay returned unexpected error | oracle: idempotency replay returned unexpected error | replaycheck, h_replay.go |
| M/replay.go:116 | Unreachable: idempotency replay produced new log sequences | oracle: idempotency replay produced new log sequences | replaycheck, h_replay.go |
| M/replay.go:133 | Reachable: idempotency replay exercised | oracle: idempotency replay exercised (also on a frozen-failure replay) | replaycheck, h_replay.go |
| M/replay.go:138 | Unreachable: idempotency replay outside model | oracle: idempotency replay outside model | replaycheck, h_replay.go |
| M/replay.go stampIdempotency | (conflict injection with `!oracle.RequestsEqual`) | idem_conflict constructors (guaranteed different, checked) + the oracle's `/write/begin` conflict check (00a) | rules/idempotency.rs |
| M/validate.go:561 | Reachable: idempotency-conflict rejection exercised | oracle: idempotency-conflict rejection exercised (failure hook) | replaycheck, h_replay.go |
| M/apply_surface.go (coverage) | Raw: coverage Apply skipped TRANSACTION_ALREADY_REVERTED and continued | oracle: coverage Apply skipped TRANSACTION_ALREADY_REVERTED and continued (drain hook; only hits emitted, catalogued unhit at startup) | replaycheck |
| M/apply_surface.go (coverage) | Raw: coverage Apply skipped METADATA_NOT_FOUND and continued | oracle: coverage Apply skipped METADATA_NOT_FOUND and continued | replaycheck |
| H/main.rs:457 | AlwaysOrUnreachable: reusing an idempotency key with another payload never creates a second transaction | oracle (a success of a conflict bulk is unexplained: `oracle: model rejects a server-committed bulk`) + verbatim `reusing idempotency key with different payload should fail` | – |
| H/main.rs:462 | Sometimes: a changed payload under a used idempotency key is refused | oracle: idempotency-conflict rejection exercised | replaycheck |
| H/main.rs:472 | Always: a rejected write stays rejected when its idempotency key is replayed | oracle: a rejected write stays rejected when its idempotency key is replayed (Unreachable) | replaycheck |
| H/main.rs:475 | Sometimes: a replay resolves an ambiguous write | dropped: an ambiguous write is resolved by the resolver retrying the same key (00c write path) | – |
| H/main.rs:476 | Sometimes: a replay returns an acknowledged transaction | oracle: idempotency replay exercised | replaycheck |
| H/main.rs:484 | Always: replaying an acknowledged write under its idempotency key never fails | oracle: idempotency replay returned unexpected error | replaycheck |
| H/main.rs:550 | Always: a revert reverts the requested transaction | oracle: reverted transaction id mismatch (00a crossCheckCommit) + verbatim reversal check | rules/reverts.rs |
| H/main.rs:563 | Always: a revert moves exactly the original postings back | oracle: revert postings mismatch (00a) + `successful concurrent revert contains exactly the expected reversal` | rules/reverts.rs |
| H/main.rs:575 | Sometimes: a second revert is refused as already reverted | `double revert path exercised` (revert_twice) | rules/reverts.rs |
| H/main.rs:582 | Always: a transaction is reverted at most once | `at most one concurrent revert should succeed` + oracle (a second successful revert is unexplained by the model) | rules/reverts.rs |
| H/main.rs:585 | Sometimes: one of several concurrent reverts wins | `concurrent revert path exercised` | rules/reverts.rs |
| H/main.rs:614 | Sometimes: deleting a missing metadata key is refused | oracle: a replay returned a frozen failure (METADATA_NOT_FOUND) / failure validation (00a) | – |
| H/main.rs:615 | Sometimes: a metadata key is deleted | dropped: covered by `oracle: model outcome verified` on meta_bulk's deletes | – |

## New properties

- `oracle: a replayed rejected key returns its frozen failure` (Unreachable): a replay of a
  frozen failure returns another reason.
- `oracle: a rejected write stays rejected when its idempotency key is replayed`
  (Unreachable; was the Rust `Always` of H/main.rs:472): a replay of a frozen failure succeeds.
- `oracle: a replay returned a frozen failure` (Reachable, must-hit).
- `oracle protocol: replay names a different original` (Unreachable): the Replay's key has
  no original, its `params.wid` names another write, or its body differs from the original's.
- `deleted metadata key is absent on a linearizable read` (Always, Rust; renamed per spec).
- View `revertSeek` (aiming only): the first revert transaction at or after a probe id,
  used by the `revert` gen's 1/8 revert-of-a-revert branch (the core `seek(txs)` does not
  say whether a transaction is a revert).

## Reachability (must-hit)

| Assertion | Rule | Needs |
|---|---|---|
| `first revert should succeed` (Sometimes) | revert_twice | no faults |
| `double revert path exercised` | revert_twice | no faults |
| `concurrent revert path exercised` | revert_concurrent (k ≥ 2, a free grant of ≥ 2 units) | no faults; needs non-reserved transactions (`smoke_tx` / task 02) |
| `idempotency conflict path exercised` | idem_conflict on a committed original | no faults |
| `metadata read-after-write should return saved value` (Always) | meta_read_after_write (account branch) | no faults |
| `deleted metadata key is absent on a linearizable read` (Always) | meta_read_after_write (account branch) | no faults |
| `oracle: idempotency replay exercised` | idem_replay | no faults |
| `oracle: idempotency-conflict rejection exercised` | idem_conflict | no faults |
| `oracle: a replay returned a frozen failure` | idem_replay over a flagged meta_bulk that failed at the FSM stage (revert of a reverted tx or delete of an absent key, without opt-in) | no faults |
| `oracle: coverage Apply skipped TRANSACTION_ALREADY_REVERTED and continued` | meta_bulk (revert opt-in on a reverted tx, followed by a committed order) | no faults; needs non-reserved transactions (`smoke_tx` / task 02) |
| `oracle: coverage Apply skipped METADATA_NOT_FOUND and continued` | meta_bulk (absent-key delete with opt-in, followed by a committed order) | no faults |

## Rejection fixtures

`W/oraclecheck/testdata/rejections/03-*.json` (all pass `TestRejectionStages`): revert of a
reverted tx, revert of an already reverted revert transaction, unforced revert
`INSUFFICIENT_FUNDS`, delete of an absent account / tx / ledger metadata key, and a mixed
add + absent delete bulk.

The conflict shape (`IDEMPOTENCY_KEY_CONFLICT`) cannot be written in the fixture format:
`setup` is sent unkeyed and the fixture's `requests` must fail on their first send under a
fresh key. The stage/freeze test already sends a different body under every frozen fixture's
key and requires `IDEMPOTENCY_KEY_CONFLICT`; the committed-original case is covered by
`TestReplayIntegration_RealServer` (`W/bin/oracle/h_replay_test.go`), which commits a flagged
create on a real in-process server, sends a different body under its key through the
service with `role: conflict`, and requires the explained conflict.

## Deviations

- `revert_twice` and `meta_read_after_write` take a fresh slot unit for each write of the
  chain (`dispatch` consumes the unit and releases it at `/write/end`); a chain whose next
  unit is not free ends without assertions. At most one unit is held at a time, as specified.
- `meta_read_after_write` draws one branch per run (account key 1/2, transaction key 1/2)
  instead of doing both, so a chain stays at 2–3 writes inside its 12 s budget.
- `revert_concurrent` reads the target's original postings with a linearizable
  `GetTransaction` through a client (not posted to the oracle) so the reversal check has
  the in-place expectation; reverts are forced, as in the old driver.
- `revert` gen: the DESIGN §3.2 taper is split: `applies` (no draw) cuts off at 4000
  transactions, and the gen itself draws the 500–2000 (1/2) and 2000–4000 (1/8) bands first
  and returns `None` when the draw fails.
- Rule-owned transactions on `o-<run>-l2` (never deleted): `revert_twice` and the transaction
  branch of `meta_read_after_write` read l2's `ledger` view and apply the same taper bands
  before creating anything (skip at ≥ 4000), so l2 stays bounded for the quiet full scans.
- Chain budgets: the 12 s budget of `revert_twice` and `meta_read_after_write` covers the
  writes and the client reads. A write starts only with at least one attempt plus the
  transport margin (5.5 s) left and gets the rest minus that reserve as its retry budget, so
  its last attempt ends by the deadline; a read gets `min(left − 1.5 s, 3 s)` and is not sent
  below 1 s. A step that does not fit ends the rule without asserting. `revert_concurrent`
  caps its `GetTransaction` at 3 s; its fan-out keeps the core's 10 s in-rule budget.
- `meta_bulk` composes slot by slot: it draws the gen with `fair_weighted` (integer units
  drawn with `draw::pick`) among the gens whose `applies` holds, then calls `compose_bulk`
  over that single gen (which makes the opt-in draw and the call). `draw::weighted` (00c)
  picks the first positive weight about 96% of the time (measured: 1914/2000 for weights
  3/3/1.5/1), so plain `compose_bulk` over the four gens produced reverts only and the
  skip probes were never reached (see Requests to 99).
- The `Replay` validator also skips non-transient non-definitive outcomes (Internal,
  Aborted, `STALE_PROPOSAL`, …): the rule sends a single attempt, and only definitive
  outcomes carry a verdict. The verdict reports such a read as `ok`, not `skipped`: the
  service sets `skipped` from `Outcome.Transient` alone (`W/bin/oracle/server.go`
  `handleReadEnd`, 00a) and a read kind's `Validate` has no skip return. No event is
  emitted, so no coverage is claimed (see Requests to 99).

## Requests to 99

- No new dependency. `order_gens()` now contains this family's four gens, so task 02's
  `tx_bulk` mixes reverts and metadata orders once both are merged.
- **`draw::weighted` is biased** (00c, `HG/driver/src/draw.rs`): it draws
  `int_in(tc, 0, 2^32 - 1)`, which hegel biases towards 0, so the first positive weight
  wins about 96% of the draws. Every `compose_bulk` caller (task 02's `tx_bulk` over
  `order_gens()`, other families' `<fam>_bulk`) and every weighted choice (quiet hooks,
  exclusive actions) is affected. A fix along the lines of this family's `fair_weighted`
  (`draw::pick` over integer units, uniform in practice) belongs in `draw.rs`; once it
  lands, `meta_bulk` can go back to a plain `compose_bulk` over `family_gens()`.
- The revert-side must-hits (`concurrent revert path exercised`, `oracle: coverage Apply
  skipped TRANSACTION_ALREADY_REVERTED and continued`, the revert gen itself) need
  non-reserved transactions on tracked ledgers, which this family does not create: run
  with `smoke_tx` (COMMON's `RULES=<your rules,smoke_tx>`) or task 02's rules. Without
  them, `seek(txs)` only sees reserved transactions (hidden) and those paths are idle.
- Local runs on this machine: the disk was above 90%, so the ledger's disk-full guard
  (`--health-{wal,data}-threshold`, default 0.8) blocked every write; the native runs set
  `HEALTH_WAL_THRESHOLD=0.995 HEALTH_DATA_THRESHOLD=0.995` in the environment (inherited
  by `run_native.sh`'s `env`).

- **Read-kind skip path** (00a): a `Validate` func cannot mark its read skipped, so the
  Replay kind's non-definitive, non-transient outcomes show as `ok` verdicts. A
  `ReadKind.Skip func(Outcome) bool` (or a skip return) consulted by `handleReadEnd` would
  make the verdict exact.

## Native run

`RULES=meta_bulk,revert_concurrent,revert_twice,idem_replay,idem_conflict,meta_read_after_write,smoke_tx run_native.sh 180`:
PASS, 0 findings, every must-hit of the table above hit (≈127 tracked ops/s). Without
`smoke_tx` the run also passes with the two oracle replay must-hits hit, but the two
revert-side must-hits stay idle (see Requests to 99).

---

<a id="04-ledgers-chart-maintenance"></a>

## 04 — ledgers, chart and enforcement, maintenance, ledger reads

Branch `hegel/04-ledgers-chart-maintenance`. Rust: `rules/ledgers.rs`, `rules/chart.rs`,
`rules/maintenance.rs`. Go: `oraclecheck/ledgercheck/`, `bin/oracle/h_ledgers.go`. Fixtures:
`oraclecheck/testdata/rejections/04-*.json` (all nine pass `TestRejectionStages`).

## Old assertions

| Old source | Old assertion (kind: literal) | New | Where |
|---|---|---|---|
| M/reads.go:311 | Reachable: GetLedger on an absent ledger returned NotFound | oracle: GetLedger on an absent ledger returned NotFound | ledgercheck `ValidateGetLedger` ← `get_ledger` (2 % absent) |
| M/reads.go:316 | Unreachable: GetLedger returned unexpected error | oracle: GetLedger returned unexpected error | ledgercheck |
| M/reads.go:327 | Unreachable: GetLedger served a ledger outside the fleet | oracle: GetLedger served a ledger outside the fleet | ledgercheck |
| M/validate.go:762 | Unreachable: ledger read outside model | oracle: ledger read outside model (chart + ledger metadata + default enforcement on one live base, as in M/) | ledgercheck |
| M/validate.go:779 | Unreachable: ledger-scoped read returned unexplained NotFound | oracle: ledger-scoped read returned unexplained NotFound (00a's `LedgerNotFoundExplained`) | (00a) |
| M/reads.go:435 | Reachable: GetMetadataSchemaStatus on an absent ledger returned NotFound | oracle: GetMetadataSchemaStatus on an absent ledger returned NotFound | ledgercheck ← `schema_status` (3 % absent) |
| M/reads.go:440 | Unreachable: GetMetadataSchemaStatus returned unexpected error | oracle: GetMetadataSchemaStatus returned unexpected error | ledgercheck |
| M/reads.go:451 | Unreachable: GetMetadataSchemaStatus served a ledger outside the fleet | oracle: GetMetadataSchemaStatus served a ledger outside the fleet | ledgercheck |
| M/validate.go:899 | Unreachable: metadata schema read outside model | oracle: metadata schema read outside model | ledgercheck |
| M/ledgers_list.go:101 | Unreachable: ListLedgers returned unexpected error | oracle: ListLedgers returned unexpected error | ledgercheck `ValidateListLedgers` ← `list_ledgers` |
| M/ledgers_list.go:115 | Unreachable: ledger listing repeated a ledger | oracle: ledger listing repeated a ledger (every row, foreign included) | ledgercheck |
| M/ledgers_list.go:121 | Unreachable: listed ledger is malformed | oracle: listed ledger is malformed (every row) | ledgercheck |
| M/ledgers_list.go:130 | Unreachable: ledger listing reused an id | oracle: ledger listing reused an id (every row) | ledgercheck |
| M/ledgers_list.go:160 | Unreachable: ledger listing is not the fleet's window | oracle: ledger listing is not the fleet's window (tracked window per DESIGN §3.2, foreign-tolerant) | ledgercheck |
| M/ledgers_list.go:189 | Unreachable: ledger listing outside model | oracle: ledger listing outside model (`match.LedgerListingMatches` over every candidate base) | ledgercheck |
| M/ledgers_list.go:195 | Unreachable: listed ledger identity is not the one creation reported | oracle: listed ledger identity is not the one creation reported (tracked rows; in-flight/pending creates skipped) | ledgercheck |
| M/ledgers_list.go:206 | Reachable: ledger listing validated | oracle: ledger listing validated | ledgercheck |
| M/ledgers_list.go:210 | Reachable: ledger listing page truncated | oracle: ledger listing page truncated | ledgercheck |
| M/ledgers_list.go:215 | Reachable: ledger listing resumed at a deleted ledger | oracle: ledger listing resumed at a deleted ledger (cursor key is a tracked name deleted in the begin snapshot) | ledgercheck |
| M/ledgers_list.go:241 | Reachable: ledger listing rejected an unsupported filter | oracle: ledger listing rejected an unsupported filter | ledgercheck |
| M/ledgers_list.go:246 | Unreachable: ledger listing accepted an unsupported filter | oracle: ledger listing accepted an unsupported filter | ledgercheck |
| M/validate.go:533-536 | Raw: coverage ledger deletion and reserved-name rejection verified | oracle: coverage ledger deletion and reserved-name rejection verified | ledgercheck `FailureHook` |
| M/validate.go:56 | Raw: coverage mirror promotion and write recovery verified | oracle: coverage mirror promotion and write recovery verified | ledgercheck `DrainHook` |
| M/validate.go:542 | Raw: coverage Apply rejected invalid skip opt-in | oracle: coverage Apply rejected invalid skip opt-in | ledgercheck `FailureHook` |
| M/apply_surface.go:150 | Raw: coverage Apply {top-level,ledger-action} set {CHART_ENFORCEMENT_STRICT,…AUDIT} | oracle: coverage Apply {top-level\|ledger-action} set {STRICT\|AUDIT} (renamed: short mode names, as the task spec writes them) | ledgercheck `DrainHook` |
| M/apply_surface.go:141 | Raw: coverage Apply skipped ERROR_REASON_ACCOUNT_TYPE_{ALREADY_EXISTS,NOT_FOUND} and continued | oracle: coverage Apply skipped ACCOUNT_TYPE_{ALREADY_EXISTS\|NOT_FOUND} and continued (renamed: short reason names, as the task spec and tasks 02/03 write them) | ledgercheck `DrainHook` |
| M/apply_surface.go (other skip reasons) | Raw: … TRANSACTION_REFERENCE_CONFLICT / TRANSACTION_ALREADY_REVERTED / METADATA_NOT_FOUND … | – | tasks 02 / 03 |
| M/checker.go:287-312 | (no assertion: create reservations) | soft reservation counters `LedgersExt` | rules/ledgers.rs |
| M/main.go:440-555 | (no assertion: maintenance recovery) | the enable follow-up / disable on the reserved slot | rules/maintenance.rs |
| MD/parallel_driver_concurrent_ledger_delete:108 | Unreachable: delete ledger should not fail unexpectedly | verbatim (definitive failure whose reason is not `LEDGER_*`) | rules/ledgers.rs `ledger_delete_race` |
| MD/parallel_driver_concurrent_ledger_delete:118 | Reachable: concurrent ledger delete: delete failed transiently | dropped: a transient delete is retried and handed off by the write path; nothing to cover | – |
| MD/parallel_driver_concurrent_ledger_delete:144 | Unreachable: write after delete should fail | verbatim (a write begun after the delete's definitive success commits) | `ledger_delete_race` |
| MD/parallel_driver_concurrent_ledger_delete:154 | Reachable: concurrent ledger delete: post-delete write inconclusive (transient) | dropped: the post-delete write retries until definitive or is handed off | – |
| MD/parallel_driver_concurrent_ledger_delete:162 | AlwaysOrUnreachable: write to deleted ledger should return LEDGER_DELETED or LEDGER_NOT_FOUND | verbatim | `ledger_delete_race` |
| MD/parallel_driver_concurrent_ledger_delete:171 | Reachable: concurrent ledger delete: post-delete write rejected | verbatim | `ledger_delete_race` |
| MD/parallel_driver_ledger_recreate:48 | Always: ledger deletion scenario has no unexpected operation errors | oracle (every write of the probe is tracked: an unexpected failure is `oracle: bulk failure not explained by any serialization`); reads: transient outcomes retried ≤ 3 times then skipped | (00a) |
| MD/parallel_driver_ledger_recreate:56 | Always: ledger deletion acknowledged transaction includes its created log | oracle: committed log count mismatch / transaction id mismatch (commit check) | (00a) |
| MD/parallel_driver_ledger_recreate:67, :256 | Always: deleted ledger name remains permanently reserved | verbatim (code FailedPrecondition + reason LEDGER_DELETED on the oracle-tracked recreate), checked after the delete and again after a successful reference reuse, as the old `recreate-before`/`recreate-after` | rules/ledgers.rs `ledger_recreate_probe` |
| MD/parallel_driver_ledger_recreate:111 | Sometimes: ledger deletion predecessor write set recorded | dropped: the seed bulk is one tracked write; the oracle validates it. As the old driver (lines 111–114), the probe stops (after deleting the reserved ledger) when the seeds were not acknowledged | – |
| MD/parallel_driver_ledger_recreate:129 | Always: deleted ledger is hidden from ledger reads | verbatim | `ledger_recreate_probe` |
| MD/parallel_driver_ledger_recreate:138 | Always: deleted ledger hides predecessor transaction point reads | verbatim | `ledger_recreate_probe` |
| MD/parallel_driver_ledger_recreate:147 | Always: deleted ledger hides predecessor account point reads | verbatim | `ledger_recreate_probe` |
| MD/parallel_driver_ledger_recreate:161 | Always: deleted ledger exposes no predecessor transactions | verbatim | `ledger_recreate_probe` |
| MD/parallel_driver_ledger_recreate:173 | Always: deleted ledger exposes no predecessor accounts | verbatim | `ledger_recreate_probe` |
| MD/parallel_driver_ledger_recreate:183 | Always: deleted ledger rejects new transaction writes | verbatim (FailedPrecondition + LEDGER_DELETED) | `ledger_recreate_probe` |
| MD/parallel_driver_ledger_recreate:223 | Always: other ledger never exposes predecessor transactions | verbatim (first page of 100, evaluated only when the listing is complete) | `ledger_recreate_probe` |
| MD/parallel_driver_ledger_recreate:237 | Always: other ledger never exposes predecessor account activity | verbatim (first page of 100, evaluated only when both listings are complete) | `ledger_recreate_probe` |
| MD/parallel_driver_ledger_recreate:249 | Sometimes: predecessor reference accepted by another ledger | verbatim | `ledger_recreate_probe` |
| MD/parallel_driver_ledger_recreate:250 | Always: predecessor references are reusable in another ledger | verbatim | `ledger_recreate_probe` |
| MD/parallel_driver_delete_ledger:34 | Unreachable: Hegel driver generation failed | dropped: no Go generator | – |
| MD/parallel_driver_delete_ledger:65 | Sometimes: should be able to seed ephemeral ledger before delete | oracle (tracked writes) | – |
| MD/parallel_driver_delete_ledger:80 | Sometimes: should be able to delete ledger | oracle (`ledger_lifecycle` delete, validated commit) | – |
| MD/parallel_driver_delete_ledger:113 | Always: deleted ledger should not appear in ListLedgers | oracle: ledger listing is not the fleet's window | ledgercheck |
| W/internal/ledger.go:150 (MD/parallel_driver_ledger_create) | Sometimes: should be able to create ledger | oracle (`ledger_lifecycle` creates, `oracle: model outcome verified` at drain) | – |
| W/internal/ledger.go:157 | Sometimes: should always be able to get created ledger | oracle: ledger read outside model / unexplained NotFound (`get_ledger`) | ledgercheck |
| MD/parallel_driver_maintenance:36 | Sometimes: should be able to enable maintenance mode | verbatim (definitive success of the tracked enable) | rules/maintenance.rs `maintenance_window` |
| MD/parallel_driver_maintenance:60 | AlwaysOrUnreachable: write during maintenance should be rejected as Unavailable | verbatim (raw probe to `r-maint-<run>`, evaluated when the reason is MAINTENANCE_MODE) | enable follow-up |
| MD/parallel_driver_maintenance:75 | Sometimes: should be able to disable maintenance mode | verbatim (disable follow-up) | disable follow-up |
| MD/parallel_driver_maintenance:97 | Sometimes: write after disabling maintenance should succeed | verbatim (keyed probe, 30 s budget) | disable follow-up |
| MD/parallel_driver_account_types:46 | Sometimes: should be able to add account type | oracle: chart response mismatch / model outcome verified (chart_bulk adds) | (00a) |
| MD/parallel_driver_account_types:69 | AlwaysOrUnreachable: added account type should appear in ledger info | oracle: ledger read outside model / ledger listing outside model | ledgercheck |
| MD/parallel_driver_account_types:92 | Sometimes: should be able to create tx with typed account address | oracle (tx writes on typed pool addresses, task 02) | – |
| MD/parallel_driver_account_types:109 | Sometimes: should be able to set enforcement mode | oracle: enforcement-mode response mismatch + `oracle: coverage Apply … set …` | ledgercheck |
| MD/parallel_driver_account_types:122 | Sometimes: should be able to remove account type | oracle: chart response mismatch (chart_bulk removes) | (00a) |
| MD/parallel_driver_account_type_violations:48 | Unreachable: should be able to add account type | oracle: bulk failure not explained by any serialization | (00a) |
| MD/parallel_driver_account_type_violations:88 | Sometimes: transaction with valid typed address should succeed | oracle (task 02 tx writes) | – |
| MD/parallel_driver_account_type_violations:125 | Sometimes: invalid address should trigger ACCOUNT_NOT_MATCHING_TYPE in strict mode | oracle (failure explained; fixture `04-strict-violation`) | – |
| MD/parallel_driver_account_type_violations:130 | Reachable: account type violation path exercised | oracle rejection coverage (task 02's ACCOUNT_NOT_MATCHING_TYPE Reachable) | task 02 |
| MD/parallel_driver_account_type_violations:165 | Sometimes: invalid address should succeed in AUDIT mode | oracle (AUDIT-mode commits validated) | – |
| H/main.rs:860 | Sometimes: a write to an ephemeral ledger commits | dropped: tracked writes on lifecycle-created ledgers are oracle-validated | – |
| H/main.rs:867 | Sometimes: a ledger is deleted | oracle (lifecycle delete commit validated; `oracle: coverage ledger deletion …`) | – |
| H/main.rs:876 | Always: a deleted ledger accepts no writes | `deleted ledger rejects new transaction writes` (verbatim MD literal) + oracle | `ledger_recreate_probe` |
| H/main.rs:879 | Always: a write to a deleted ledger is refused as deleted | `write to deleted ledger should return LEDGER_DELETED or LEDGER_NOT_FOUND` | `ledger_delete_race` |
| H/main.rs:885 | Always: a deleted ledger's name is never reused | `deleted ledger name remains permanently reserved` + oracle (lifecycle recreate branch) | `ledger_recreate_probe`, `ledger_lifecycle` |
| H/main.rs:892 | Always: a deleted ledger is not listed as live | oracle: ledger listing is not the fleet's window | ledgercheck |
| H/main.rs:950 | Sometimes: a write during maintenance is refused | `a write attempt hits maintenance mode` (new literal, attempt log of the concurrent writes) | `maintenance_window` |
| H/main.rs:977-1012 | Sometimes/Always: balanced transient commits, transient keeps no balance, strict refuses outside-chart addresses (5 literals) | oracle (chart orders + tx writes validated by the model; strict refusals explained); transient bulks are task 02's | task 02 |

## New properties

- `a write attempt hits maintenance mode` (Rust Sometimes): one of the concurrent tracked
  writes of `maintenance_window` had an attempt refused with reason MAINTENANCE_MODE.
- Foreign tolerance of every ListLedgers check (DESIGN §3.2): structural row checks apply
  to foreign rows; the tracked window and row contents are compared with the model.

## Deviations from the spec

- **Promotion below the target.** The `ledgers` view lists a promoted mirror under `live`, so a
  promotion at `live == LIVE_TARGET` (the spec's "else … 1/4 promote") would grow live normal
  ledgers to 5, 6, …. `choose_lifecycle` promotes (1/4, when a mirror exists) in the branch that
  would otherwise create a normal ledger, i.e. only when `live + pending creates < LIVE_TARGET`,
  and counts an in-flight promotion as a pending create. Mirror creates have their own pending
  counter, checked against `MAX_MIRRORS`. The old model (`M/actions.go:286-338`) had no bound.
- **Chart generator weights.** `draw::weighted` (00c) almost always returns the first positive
  weight (see Requests to 99), so `compose_chart` picks each slot's generator with a `draw::chance`
  chain over the spec weights (1.5/1.5/0.5, i.e. 3/7, 3/7, 1/7) and hands `compose_bulk` that one
  generator. An earlier version rotated the list by a uniform draw (≈ 1/3 each).
- **Nested chart forms.** `chart_add`/`chart_remove` nest exactly when `compose_bulk` asks for the
  opt-in variant (1/4 per slot), always with the opt-in, as `M/apply_surface.go`
  maybeAddSkippableReason does; there is no nested form without the opt-in. The enforcement gen
  keeps its nested-without-opt-in form (1/2), which the four mode × form coverage probes need.
- **Recreate-probe reads.** Retried (≤ 3 times, 200 ms apart) and then skipped on outcomes that
  say nothing about the ledger: the old driver's `internal.IsTolerated` set (no answer,
  Unavailable, DeadlineExceeded, Canceled, `EXTERNAL_SERVICE_ERROR`, `WRITES_BLOCKED_DISK_FULL`)
  plus client-local errors, answers without a code, and Unknown / Aborted / ResourceExhausted
  (node restarts, overload). Internal remains an answer, as in the old driver. The reads share the
  probe's 12 s chain deadline (each RPC timeout is capped by what is left), so the rule never
  blocks for more than the chain budget plus a 2 s cleanup grace.
- **Probe cleanup.** Both deletion probes delete their reserved ledger on every early return after
  the create (inconclusive pre-writes, no slots, budget spent, unacknowledged seeds), with a 2 s
  grace past the chain budget. `ledger_delete_race` stops when its pre-writes are inconclusive.
- **Maintenance window lifecycle.** The window claim is an `Arc` held by every piece of the window
  (the enable's follow-up, the disable and its follow-up, a disable retry) and released when the
  last one goes: a definitively failed or dropped enable (rule or resolver) releases it, a
  `/write/end` error after a committed enable does not, and the claim outlives the post-disable
  probe. A disable that fails definitively (rule or resolver) is sent again by a quiet-blocking
  retry job (up to 30 times, 1 s apart). Oracle refusals of the disable's begin: back-pressure is
  retried; anything else is retried 5 times with backoff and then the driver enters degraded mode
  explicitly before disabling untracked, so no untracked `setMaintenanceMode` is sent while the
  oracle is healthy (DESIGN §3).
- **Ledger-scoped reads** (`get_ledger`, `schema_status`) also target live mirrors, as the old
  model's ledger pool did.
- **Deletion coverage side table** (`ledgercheck` `pendingDeleted`) is capped at 256 names, since
  nothing recreates the rule-owned probe ledgers.

## Reachability (must-hit)

| Assertion | Kind | Reached by | Needs |
|---|---|---|---|
| oracle: GetLedger on an absent ledger returned NotFound | Reachable | `get_ledger` (2 % absent) | no faults |
| oracle: GetMetadataSchemaStatus on an absent ledger returned NotFound | Reachable | `schema_status` (3 % absent) | no faults |
| oracle: ledger listing validated | Reachable | `list_ledgers` | no faults |
| oracle: ledger listing page truncated | Reachable | `list_ledgers` (page size ≤ live tracked ledgers) | no faults |
| oracle: ledger listing resumed at a deleted ledger | Reachable | `list_ledgers` (cursor = deleted name; deletions by `ledger_lifecycle`) | no faults |
| oracle: ledger listing rejected an unsupported filter | Reachable | `list_ledgers` (1/4 filtered) | no faults |
| oracle: coverage ledger deletion and reserved-name rejection verified | Raw | `ledger_lifecycle` recreate branch, `ledger_recreate_probe` recreate | no faults |
| oracle: coverage Apply {top-level\|ledger-action} set {STRICT\|AUDIT} | Raw | `chart_bulk` (enforcement gen) | no faults |
| oracle: coverage Apply skipped ACCOUNT_TYPE_{ALREADY_EXISTS\|NOT_FOUND} and continued | Raw | `chart_bulk` (opt-in skip followed by another order, bulks of 2–3) | no faults |
| oracle: coverage Apply rejected invalid skip opt-in | Raw | `chart_bulk` (opt-in on the enforcement setter) | no faults |
| oracle: coverage mirror promotion and write recovery verified | Raw | needs a business write (create/revert/metadata) on a promoted mirror | needs task 02 |
| should be able to enable maintenance mode | Sometimes | `maintenance_window` | no faults |
| should be able to disable maintenance mode | Sometimes | disable follow-up | no faults |
| write after disabling maintenance should succeed | Sometimes | disable follow-up | no faults |
| a write attempt hits maintenance mode | Sometimes | `maintenance_window` with k > 0 | no faults (observed natively, see below) |
| concurrent ledger delete: post-delete write rejected | Reachable | `ledger_delete_race` | no faults |
| predecessor reference accepted by another ledger | Sometimes | `ledger_recreate_probe` | no faults |
| the ten `ledger_recreate_probe` Always literals | Always | `ledger_recreate_probe` | no faults |

`M/validate.go:53-60` counts only `createTransaction`, `revertTransaction`, `addMetadata` and
`deleteMetadata` Apply orders as business writes; chart orders are mirror-safe configuration.
`chart_bulk` therefore never satisfies the promotion probe: it needs task 02's `tx_bulk` (or 03's
metadata rules) on a promoted `o-<run>-m<N>` ledger.

## Native smoke

`RULES=ledger_lifecycle,chart_bulk,ledger_delete_race,ledger_recreate_probe,maintenance_window,get_ledger,list_ledgers,schema_status,smoke_tx run_native.sh 180`:
PASS, 0 findings, ~2800 tracked writes; every must-hit of this task hit (maintenance enable,
disable, both probes, `a write attempt hits maintenance mode`, the recreate-probe literals, all
apply-surface probes) except `oracle: coverage mirror promotion and write recovery verified`
(needs task 02; an earlier run hit it through `smoke_tx` on a promoted mirror). The run was made
inside `unshare -rn` (private loopback, so the fixed UDP ports 7777/7778 do not collide with
other native runs on the machine) and with `HEALTH_WAL_THRESHOLD=HEALTH_DATA_THRESHOLD=0.995`
exported, because the shared disk was above the server's 80 % write-block threshold.

Re-run after the review fixes (machine load average 10–40 from parallel agents; same
`unshare -rn` and threshold setup): all nine rules for 180 s, PASS, 0 findings; maintenance
enable/disable, both probe Sometimes, `a write attempt hits maintenance mode`, the delete-race
literals hit; the cluster sat in maintenance most of the run (one window right after another),
so only ~270 tracked writes went through and the recreate probe never got past its create. Without
`maintenance_window` (150 s): PASS, 0 findings, ~2000 writes, every recreate-probe literal hit, but
`chart_bulk` (last in the rule list) never ran. `chart_bulk,ledger_lifecycle,smoke_tx` for 90 s:
PASS, 0 findings, ~9600 writes, all apply-surface probes and the promotion probe hit; chart orders
were add 2774 / remove 2837 / enforcement 1013 (≈ 3/7, 3/7, 1/7), with the opt-in nested form on a
quarter of adds and removes.

## Requests to 99

- `oracle: coverage mirror promotion and write recovery verified` needs task 02/03 writes on a
  promoted mirror (excluded from this task's acceptance).
- `a write attempt hits maintenance mode` is reached without faults whenever `k > 0`; keep it
  even if a local run misses it (the window is short).
- The ten `ledger_recreate_probe` Always literals check `code == NotFound` as the old driver did
  (`MD/parallel_driver_ledger_recreate`); the task spec's "without ErrorInfo" would strengthen
  the old literal, so it is not added.
- `draw::weighted` (00c) draws a 32-bit integer that hegel biases toward small values, so the
  first positive weight wins almost every draw (first native run: 0 enforcement orders out of
  ~150 chart bulks). `chart_bulk` works around it with a `draw::chance` chain over the spec
  weights per slot (`chart::pick_chart_gen`); `compose_bulk` users in other families (task 02's
  `tx_bulk`) are affected the same way. Suggest `weighted` use a small-range draw (e.g. a
  cumulative pick over an integer scale of 1000) or `chance` chains.
- `oracle: coverage Apply rejected invalid skip opt-in` is registered by `ledgercheck`
  (this task's spec assigns it here). Its failure hook fires for any family's bulk carrying an
  invalid skip reason; if another family registers the same literal, `RegisterCoverage` collides
  at merge, and the other registration should be dropped.
- Rule selection does not look proportional to the rule weights: in the 150 s run above
  `list_ledgers` (1.0) ran 561 reads, `get_ledger` (1.5) 264, and `chart_bulk` (1.0, last in the
  list) never ran. Rule order may bias hegel's rule draw, as `draw::weighted` is biased.
- `maintenance_window` (0.4) starts a new window as soon as the last one closes, so the cluster
  can stay in maintenance most of the time on a slow machine, which starves the 12 s probe chains.
  Consider a cool-down between windows if the Docker runs show the same.
- Rule-owned ledgers accumulate as tombstones in the model (each `ledger_delete_race` /
  `ledger_recreate_probe` run leaves one or two deleted `o-<run>-own<N>`); at weight 0.4 each this
  is several hundred per hour. Watch `LiveLedgers`/candidate-search cost in long runs.

---

<a id="05-point-reads"></a>

## Porting record — 05 point reads

Task 05 ports the singleton model's point reads (GetAccount, GetTransaction,
AggregateVolumes, GetLedgerStats, GetLog by global sequence) into the oracle
(`W/oraclecheck/pointcheck`, asserters in `W/bin/oracle/h_point.go`) and adds
five Rust rules in `HG/driver/src/rules/point_reads.rs`, every read through a
read ticket, linearizable, on a drawn route:

| Rule | Weight | Kind | Views |
|---|---|---|---|
| `get_account` | 4 | `GetAccount` | `seek(volumes \| accountMeta)`; the model-absent probe seeks 8 pool candidates in both tables |
| `get_transaction` | 3 | `GetTransaction` | `ledger` |
| `aggregate` | 1.5 | `AggregateVolumes` | `ledger` (`includeDeleted`) |
| `ledger_stats` | 1 | `GetLedgerStats` | `ledger` (`includeDeleted`) |
| `get_log` | 1 | `GetLog` | `logSeqs` (registered here) |

Generator intent (absent ledger, colour collapse, model-absent account,
learned/unassigned sequence) is recomputed by the validators from the request
and the read's snapshot; the hint is coverage-only.

Deviations from the spec's rule table:

- `aggregate` requests no `stamps` view: accounts filters have no date
  leaves, so it would go unread. 06's leaf builder adds it back if needed.
- `get_account`'s model-absent probe pre-draws 8 pool addresses and seeks
  each in `volumes` and `accountMeta`; after `/read/begin` it reads the first
  one neither seek lands on (M/reads.go pickAbsentAccount/modelKnowsAccount),
  else the first candidate. A single blind pool draw would turn into an
  ordinary known-account read once a long-lived ledger has touched it.
- `get_transaction` on an absent ledger probes ids `1..=64`, as M/reads.go
  pickTransactionID did (the ledger view of an absent name has no txCount).
- `ValidateGetLog` looks up the learned maximum only for an unlearned
  sequence at or past `2^40`, and takes it from the `logSeqs` view the oracle
  computed at `/read/begin` on the same snapshot (walking the stream only when
  that view is absent).

| Old source | Old assertion (kind: literal) | New | Where |
|---|---|---|---|
| M/validate.go:663 | Unreachable: account read outside model | oracle: account read outside model | pointcheck/account.go, h_point.go |
| M/reads.go:80 | Unreachable: GetAccount returned unexpected error | oracle: GetAccount returned unexpected error | pointcheck/account.go |
| M/reads.go:45 | Reachable: GetAccount probing an absent ledger | oracle: GetAccount probing an absent ledger | pointcheck/account.go (rule get_account) |
| M/reads.go:49 | Reachable: GetAccount probing a model-absent account | oracle: GetAccount probing a model-absent account | pointcheck/account.go (rule get_account) |
| M/reads.go:95 | Reachable: colour-collapsed account read validated | oracle: colour-collapsed account read validated | pointcheck/account.go (rule get_account) |
| M/validate.go:853 | Unreachable: transaction read outside model | oracle: transaction read outside model | pointcheck/transaction.go |
| M/reads.go:400 | Unreachable: GetTransaction returned unexpected error | oracle: GetTransaction returned unexpected error | pointcheck/transaction.go |
| M/reads.go:380 | Reachable: GetTransaction probing an absent ledger | oracle: GetTransaction probing an absent ledger | pointcheck/transaction.go (rule get_transaction) |
| M/aggregates.go:496 | Unreachable: aggregate volumes outside model | oracle: aggregate volumes outside model | pointcheck/aggregate.go |
| M/aggregates.go:421 | Unreachable: aggregate result mixes grouped and flat volumes | oracle: aggregate result mixes grouped and flat volumes | pointcheck/aggregate.go |
| M/aggregates.go:439 | Unreachable: aggregate result repeats a bucket | oracle: aggregate result repeats a bucket | pointcheck/aggregate.go |
| M/aggregates.go:114 | Unreachable: aggregate served a ledger outside the fleet | oracle: aggregate served a ledger outside the fleet | pointcheck/aggregate.go |
| M/aggregates.go:402 | Unreachable: aggregate returned unexpected error | oracle: aggregate returned unexpected error | pointcheck/aggregate.go |
| M/aggregates.go:103 | Unreachable: AggregateVolumes returned unexpected error | oracle: AggregateVolumes returned unexpected error | pointcheck/aggregate.go |
| M/aggregates.go:83 | Reachable: aggregate on an absent ledger returned NotFound | oracle: aggregate on an absent ledger returned NotFound | pointcheck/aggregate.go (rule aggregate) |
| M/aggregates.go:463 | Reachable: aggregate gated on a missing index | oracle: aggregate gated on a missing index | pointcheck/aggregate.go (rule aggregate, indexed class) |
| M/aggregates.go:466 | Reachable: grouped aggregate volumes validated | oracle: grouped aggregate volumes validated | pointcheck/aggregate.go (rule aggregate) |
| M/aggregates.go:469 | Reachable: aggregate volumes validated | oracle: aggregate volumes validated | pointcheck/aggregate.go (rule aggregate) |
| M/aggregates.go:52-58 (rollFilterShape, classifyRejectedFilter) | Reachable/Unreachable: refused account filter … | not generated here: refused-filter shapes belong to task 06's filter generator | – |
| M/stats.go:147 | Unreachable: ledger stats boundaries outside model | oracle: ledger stats boundaries outside model | pointcheck/stats.go |
| M/stats.go:180 | Unreachable: ledger stats counter above the model | oracle: ledger stats counter above the model | pointcheck/stats.go |
| M/stats.go:128 | Unreachable: ledger stats counted a numscript execution | oracle: ledger stats counted a numscript execution | pointcheck/stats.go |
| M/stats.go:111 | Unreachable: ledger stats served a ledger outside the fleet | oracle: ledger stats served a ledger outside the fleet | pointcheck/stats.go |
| M/stats.go:99 | Unreachable: GetLedgerStats returned unexpected error | oracle: GetLedgerStats returned unexpected error | pointcheck/stats.go |
| M/stats.go:198 | Reachable: ledger stats usage caught up with the model | oracle: ledger stats usage caught up with the model | pointcheck/stats.go (rule ledger_stats) |
| M/stats.go:203 | Reachable: ledger stats validated | oracle: ledger stats validated | pointcheck/stats.go (rule ledger_stats) |
| M/stats.go:89 | Reachable: ledger stats on an absent ledger returned NotFound | oracle: ledger stats on an absent ledger returned NotFound | pointcheck/stats.go (rule ledger_stats) |
| M/getlog.go:137 | Unreachable: GetLog answered with another log | oracle: GetLog answered with another log | pointcheck/getlog.go |
| M/getlog.go:112 | Unreachable: GetLog lost a committed log | oracle: GetLog lost a committed log | pointcheck/getlog.go |
| M/getlog.go:122 | Unreachable: GetLog served an unassigned sequence | oracle: GetLog served an unassigned sequence | pointcheck/getlog.go (probe at `maxLearnedSeq + 2^40`, DESIGN §3.2, instead of `+ 1_000_000`) |
| M/getlog.go:96 | Unreachable: GetLog returned unexpected error | oracle: GetLog returned unexpected error | pointcheck/getlog.go |
| M/getlog.go:153 | Reachable: GetLog validated | oracle: GetLog validated | pointcheck/getlog.go (rule get_log) |
| M/getlog.go:107 | Reachable: GetLog on an unassigned sequence returned NotFound | oracle: GetLog on an unassigned sequence returned NotFound | pointcheck/getlog.go (rule get_log) |
| MD/parallel_driver_logs/main.go:114 | AlwaysOrUnreachable: GetLog should return the requested log entry | oracle: GetLog answered with another log (subsumes: same sequence, owner, id and full row) | pointcheck/getlog.go |
| MD/parallel_driver_logs/main.go:33,45,62,98 | Sometimes/Unreachable/Always: create tx / no committed log / ListLogs should not fail / ListLogs at least one entry | not this task: the write half is task 02 (`oracle: successful bulk returned no committed log`, 00a), the ListLogs half task 06 | – |
| MD/parallel_driver_stats_consistency/main.go:51 | Always: log count must be >= transaction count | oracle: ledger stats boundaries outside model (subsumes: both counts exactly some base's) | pointcheck/stats.go |
| MD/parallel_driver_stats_consistency/main.go:73 | Always: aggregate balance must be zero (double-entry) | oracle: aggregate volumes outside model (subsumes: the whole fold is exact; the model is double-entry) | pointcheck/aggregate.go |
| MD/parallel_driver_stats_consistency/main.go:85 | Reachable: stats consistency check passed | oracle: ledger stats validated | pointcheck/stats.go |
| MD/parallel_driver_invariants/main.go:23 | Unreachable: AggregateVolumes returned unexpected error | oracle: AggregateVolumes returned unexpected error | pointcheck/aggregate.go |
| MD/parallel_driver_invariants/main.go:35 | Always: aggregate double-entry: input must equal output | oracle: aggregate volumes outside model (subsumes) | pointcheck/aggregate.go |
| H/main.rs:635 | Always: an account's balance equals its input minus its output | verbatim (now exact over 256 bits, not i128) | rules/point_reads.rs get_account |
| H/main.rs:641 | Always: account metadata holds only values the workload wrote | oracle: account read outside model (exact metadata map) | – |
| H/main.rs:644 | Always: a linearizable read returns the latest metadata value | oracle: account read outside model (candidate bases) | – |
| H/main.rs:649 | Sometimes: a metadata read overlaps a write to the same key | dropped: the oracle's in-flight candidate walk covers overlap; the Rust history model is retired (DESIGN §6.9) | – |
| H/main.rs:677 | Always: a linearizable read finds every acknowledged transaction | oracle: transaction read outside model (NotFound on a committed id is a finding) | – |
| H/main.rs:682 | Always: a transaction reads back as it was acknowledged | oracle: transaction read outside model (TxRecordMatches: full record) | – |
| H/main.rs:685 | Always: a linearizable read of a reverted transaction shows it reverted | oracle: transaction read outside model (reverted, revertedBy, revertedAt) | – |
| H/main.rs:689 | Sometimes: a stale read misses a recent transaction | dropped here: stale reads are never oracle-validated; task 14 owns stale probes | – |
| H/main.rs:745 | Always: double entry: an asset's total input equals its total output | oracle: aggregate volumes outside model (subsumes) | – |
| W/internal/sentinel.go:110,127 | Always/Unreachable: committed sentinel transaction content survives / must survive operational events | oracle: transaction read outside model — TxRecordMatches compares id, reference, revert relations, postings, metadata, learned timestamp/insertedAt/revertedAt, updatedAt == insertedAt and post-commit volumes both ways (main-drivers §3.37 strength) | pointcheck/transaction.go |
| W/internal/sentinel.go:114,119 | Reachable: sentinel read-after-write succeeded / verify hit a transient error | dropped: no sentinel writer in the hegel workload; transient reads are `skipped` by the service | – |

## New properties

- `oracle: aggregate served a ledger deleted in every candidate` (Unreachable):
  the liveness guard model-reads §4.8 flags as missing. A served aggregate is
  also only explained by candidates that hold the ledger live.
- GetLog unassigned probe at `maxLearnedSeq + 2^40` (foreign writers share the
  global stream).

## Reachability of the must-hit assertions

| Assertion | Rule / draw | Needs |
|---|---|---|
| `oracle: GetAccount probing an absent ledger` | get_account, absent 2% | no faults |
| `oracle: GetAccount probing a model-absent account` | get_account, pool probe 5% (first of 8 candidates the model does not know), and empty-seek reads | no faults |
| `oracle: colour-collapsed account read validated` | get_account, collapse 1/3, any served volume (smoke_tx's writes suffice) | no faults |
| `oracle: GetTransaction probing an absent ledger` | get_transaction, absent 2% | no faults |
| `oracle: aggregate volumes validated` | aggregate, flat | no faults |
| `oracle: grouped aggregate volumes validated` | aggregate, groupByPrefixes 1/4 | no faults |
| `oracle: aggregate on an absent ledger returned NotFound` | aggregate, absent 2% | no faults |
| `oracle: aggregate gated on a missing index` | aggregate, indexed class (metadata exists on a key with no index) | no faults (until task 07 creates indexes, every such filter is refused) |
| `oracle: ledger stats usage caught up with the model` | ledger_stats | no faults (a quiet-ish moment) |
| `oracle: ledger stats validated` | ledger_stats | no faults |
| `oracle: ledger stats on an absent ledger returned NotFound` | ledger_stats, absent 2% | no faults (rare: weight 1 × 2%) |
| `oracle: GetLog validated` | get_log | no faults |
| `oracle: GetLog on an unassigned sequence returned NotFound` | get_log, unassigned 1/8 | no faults |
| `an account's balance equals its input minus its output` (Rust Always) | get_account on an account with volumes | no faults |

The in-process integration test `bin/oracle/h_point_integration_test.go`
(`TestIntegration_PointReads`, real single-node server) reaches every oracle
must-hit above with zero findings. It is also the only real-server evidence for
`oracle: aggregate gated on a missing index` and for the deleted-ledger and
liveness-guard paths (it deletes a ledger, then runs every read kind on it):
the 180 s native run used only the point reads and `smoke_tx`, so it created
no index and deleted no ledger, and none of them ran with a delete in flight.

## Rejection fixtures

None: the five rules only read; they produce no write rejection shape.

## Requests to 99

- `oracle: aggregate gated on a missing index`: reached locally because no
  index exists without task 07; re-check once 07 is merged (the indexed class
  then also produces served results on indexed keys).
- The local minimal accounts-filter builder in `point_reads.rs` (address
  prefix/exact, metadata exists, and/or/not) duplicates part of task 06's
  generator; 99 may replace it with 06's builders.
- Missing coverage until then: the indexed class only builds `metadata exists`
  leaves. M/aggregates.go genAccountFilterIndexed (sampleAccountFieldSeeds,
  model-reads §4.5) also drew typed comparisons on declared keys, 1/12
  kind-mismatched leaves and the 1/8 `undeclared-…` probe. So `aggregate` never
  reaches the `FILTER_COMPILATION_ERROR` arm of `IndexedQueryOutcomeLegal`, and
  served results only test presence, never value-encoding windows (retype,
  EN-1724). The aggregate path has old-equivalent strength only once 06's
  typed and kind-mismatch leaves feed it.
- The `logSeqs` view still walks the whole global log stream (O(n) in learned
  sequences, deleted ledgers included) once per GetLog read, under the
  service mutex: `oracle.GlobalState` exposes only an ascending `Logs()` over
  its persistent map. Ask for O(log n) accessors in `tests/oracle` (outside
  05's ownership), for example `LastLogSequence()` plus a `LogsFrom(seq)` seek,
  so the view can sample by seeking random sequences between the first and last.
- Re-check the deleted-ledger and liveness-guard paths of `aggregate` and
  `ledger_stats` with task 04 (lifecycle) and 07 (indexes) merged: so far only
  the single-node integration test exercises them, never with a delete in flight.
- `matchtest.Render*` (00b) return model-owned slices and pointers (postings,
  timestamps, metadata): a test that mutates a rendered response in place
  mutates the model. `pointcheck` tests clone before mutating; other read
  families should too.

---

<a id="06-list-queries"></a>

## 06 — List queries (ListAccounts, ListTransactions, ListLogs, filters, cursors)

Rust: `HG/driver/src/rules/lists.rs` (rules), `rules/filters.rs` (filter, shape, cursor and
page-token generation). Go: `W/oraclecheck/querycheck/` (validators), `W/bin/oracle/h_query.go`
(read kinds `ListAccounts`, `ListTransactions`, `ListLogs`, asserters, coverage probes).
Fixtures: `HG/protocol-fixtures/06/page_tokens.json` (written by the Go test
`TestPageTokenFixture`, checked by the Rust `page_token_matches_go_fixture`) and
`HG/protocol-fixtures/06/filters/{builders,generated}.json` (written by the Rust test
`filter_fixtures` with `UPDATE_FIXTURES=1`, unmarshalled by the Go test
`TestFilterFixtures_ClientAccepts` with the client's strict protojson options; the builder
cases also pin the oracle's refused-filter classification, not the server's). Without
`UPDATE_FIXTURES`, `filter_fixtures` fails when either file differs from the current
builders or from the fixed-seed (seed 6) generator sample, so the Go test cannot go stale.

Rules: `list_accounts` (2), `list_transactions` (3), `list_logs` (1.5), `list_traverse` (0.7).
Every draw happens before `/read/begin`: filters are pre-drawn plans whose view-dependent
leaves (declared field types from `ledger`, a reference from `seek(txByRef)`, transaction
stamps and log dates from `stamps`, the log head from `ledger.logCount`) carry pre-drawn picks
resolved against the views of the same ticket; there are no post-begin draws at all.

Index targeting (coverage fix, run 3d8b6ff1…-67-0): `FilterCtx` also reads the `ledger` view's
`indexes` and keeps, per target, the keys with an **active**, non-reserved metadata index. A
Field leaf picks among those keys with probability 3/4 when any is declared (else any declared
key, as before), so a metadata-field filter usually needs an index the model holds active and
the `oracle: coverage a metadata-field index served …` probes can be true. `list_accounts` and
`list_transactions` aim at `o-<run>-l1` 1/4 of the time (by name; never deleted), where task
07's `metadata_index_bootstrap` keeps `k0` (accounts, STRING) and `k1` (transactions, INT64)
indexed. `KeyListPlan` (lists.rs) is the one-leaf list on a given key that task 07's
`schema_bulk` retype probe sends right after a committed retype (hint `retype_probe`).

## Old assertions

| Old source | Old assertion (kind: literal) | New | Where |
|---|---|---|---|
| M/queries.go:101 | Reachable: page size left to the server default | `oracle: page size left to the server default` | h_query.go (querycheck `pageSizeEvents`) |
| M/queries.go:103 | Reachable: page size clamped to the server maximum | `oracle: page size clamped to the server maximum` | h_query.go |
| M/queries.go:184 | Reachable: has-asset precision overflow rejected | `oracle: has-asset precision overflow rejected` | h_query.go (accounts) |
| M/queries.go:205 | Unreachable: ListAccounts returned unexpected error | `oracle: ListAccounts returned unexpected error` | h_query.go |
| M/queries.go:218 | Unreachable: target-invalid account query returned results | `oracle: target-invalid account query returned results` | h_query.go |
| M/queries.go:377 | Unreachable: ListTransactions returned unexpected error | `oracle: ListTransactions returned unexpected error` | h_query.go |
| M/queries.go:387 | Unreachable: malformed transaction cursor returned results | `oracle: malformed transaction cursor returned results` | h_query.go |
| M/queries.go:396 | Unreachable: unsupported builtin uint field returned results | `oracle: unsupported builtin uint field returned results` | h_query.go |
| M/queries.go:405 | Unreachable: target-invalid transaction query returned results | `oracle: target-invalid transaction query returned results` | h_query.go |
| M/queries.go:441,443 | Reachable: target-invalid query rejected on accounts / on transactions | `oracle: target-invalid query rejected on accounts` / `… on transactions` | h_query.go |
| M/queries.go:449 | Unreachable: target-invalid query returned unexpected error | `oracle: target-invalid query returned unexpected error` | h_query.go |
| M/queries.go:499 | Reachable: unsupported builtin uint field rejected | `oracle: unsupported builtin uint field rejected` (not in the spec's list; reached by the 1/24 probe, so kept as a must-hit) | h_query.go |
| M/queries.go:504 | Unreachable: unsupported builtin uint field returned unexpected error | `oracle: unsupported builtin uint field returned unexpected error` | h_query.go |
| M/queries.go:550 | Unreachable: account query outside model | `oracle: account query outside model` | h_query.go |
| M/queries.go:594 | Unreachable: transaction query outside model | `oracle: transaction query outside model` | h_query.go |
| M/cursors.go:131 | Reachable: malformed transaction cursor rejected | `oracle: malformed transaction cursor rejected` | h_query.go |
| M/cursors.go:133 | Reachable: malformed log cursor rejected | `oracle: malformed log cursor rejected` | h_query.go |
| M/cursors.go:135 | Reachable: malformed audit cursor rejected | not ported here (audit cursor: task 10) | – |
| M/cursors.go:141 | Unreachable: malformed cursor returned unexpected error | `oracle: malformed cursor returned unexpected error` (transactions and logs; task 10 owns the audit arm under its own literal) | h_query.go |
| M/rejected_filter.go:125,127,129 | Reachable: refused account / transaction / log filter rejected | `oracle: refused account filter rejected` / `… transaction …` / `… log …` | h_query.go |
| M/rejected_filter.go:142 | Unreachable: refused filter returned unexpected error | `oracle: refused filter returned unexpected error` | h_query.go |
| M/rejected_filter.go:163 | Unreachable: refused filter returned results | `oracle: refused filter returned results` | h_query.go |
| M/indexes.go:253 | Unreachable: asset-index account query returned unexpected error | `oracle: asset-index account query returned unexpected error` | h_query.go |
| M/indexes.go:264 | Unreachable: asset-index query filter is not a bare has-asset leaf | `oracle: asset-index query filter is not a bare has-asset leaf` | h_query.go |
| M/indexes.go:310,312 | Reachable: asset-index account query served results / gated | `oracle: asset-index account query served results` / `… gated` | h_query.go (needs task 07) |
| M/indexes.go:356 | Unreachable: asset-index account query outside model | `oracle: asset-index account query outside model` | h_query.go |
| M/indexes.go:927 | Unreachable: indexed transaction query returned unexpected error | `oracle: indexed transaction query returned unexpected error` | h_query.go |
| M/indexes.go:953,955,957 | Reachable: indexed transaction query served results / kind-mismatched field query rejected on transactions / indexed transaction query gated | `oracle: …` same suffixes | h_query.go (served, kind-mismatched: need task 07) |
| M/indexes.go:1018 | Unreachable: indexed transaction query outside model | `oracle: indexed transaction query outside model` | h_query.go |
| M/indexes.go:1310 | Unreachable: indexed account query returned unexpected error | `oracle: indexed account query returned unexpected error` | h_query.go |
| M/indexes.go:1336,1339,1342,1344 | Reachable: indexed account query served results / has-asset under a combinator served results / kind-mismatched field query rejected on accounts / indexed account query gated | `oracle: …` same suffixes | h_query.go (all but gated need task 07) |
| M/indexes.go:1399 | Unreachable: indexed account query outside model | `oracle: indexed account query outside model` | h_query.go |
| M/queries_logs.go:686-693 | Unreachable: malformed log cursor returned results | `oracle: malformed log cursor returned results` | h_query.go |
| M/queries_logs.go:680 | Unreachable: ListLogs returned unexpected error | `oracle: ListLogs returned unexpected error` | h_query.go |
| M/queries_logs.go:741 | Unreachable: log page violates its own ordering | `oracle: log page violates its own ordering` | h_query.go |
| M/queries_logs.go:758 | Unreachable: log page carries a response signature | `oracle: log page carries a response signature` (verbatim, unconditional). The spec asked for a rename to "a log of an unsigned tracked bulk carries a response signature", but `Log.response_signature` is the server signing its own responses with `ledger run --response-signing-key` (docs/ops/signing.md "Response Signing"), not client request signing (task 16, which travels on the applied proposal). No compose node sets a response key, so any response signature stays a finding; see Requests to 99 | h_query.go (querycheck `signedRow`) |
| M/queries_logs.go:780,782,785 | Reachable: log query gated on a missing index / log query served results / reverse log query served results | `oracle: …` same suffixes | h_query.go |
| M/queries_logs.go:808 | Unreachable: log query outside model | `oracle: log query outside model`; the diagnostic re-reads (`recheckLogIDs`, `recheckLogKinds`) moved to Rust (`lists.rs recheck_logs`, printed after a `log query outside model` verdict) because the oracle never dials the cluster | h_query.go |
| M/coverage.go:110-178 | Raw Sometimes: `coverage index <canonical> served a model-verified page` (one per workload index), `coverage a metadata-field index served a model-verified page on accounts|transactions`, `coverage a query was served while a retype window was open` | `oracle: coverage …` same suffixes, registered with `RegisterCoverage` in h_query.go, evaluated on every index-backed validated outcome (needs task 07 to be true) | h_query.go, querycheck/coverage.go |
| M/coverage.go (checkpoint, deletion, promotion probes) | Raw Sometimes: checkpoint / ledger deletion / mirror promotion probes | not ported here (tasks 09 and 04) | – |
| MD/parallel_driver_list_completeness/main.go:98 | Sometimes: list completeness write set recorded | dropped: the traversal no longer owns a write set; tracked writes are judged by the oracle | – |
| MD/parallel_driver_list_completeness/main.go:218 | Always: clean-EOF list pagination respects the requested page size | verbatim; compares each page with `EffectivePageSize(requested)` (0 → 100, > 1000 → 1000), ListTransactions and ListLogs | lists.rs `check_traversal` |
| MD/parallel_driver_list_completeness/main.go:221 | Always: clean-EOF list pagination contains every acked transaction | oracle: subsumed by the per-page window validation (`oracle: transaction query outside model` on every page of the traversal) | h_query.go |
| MD/parallel_driver_list_completeness/main.go:224 | Always: clean-EOF list pagination yields no duplicate transaction IDs | verbatim | lists.rs `check_traversal` |
| MD/parallel_driver_list_completeness/main.go:227 | Always: clean-EOF list pagination yields strictly increasing transaction IDs | verbatim; evaluated only on ascending traversals (`reverse=true` on transactions) | lists.rs `check_traversal` |
| MD/parallel_driver_timestamp_order/main.go:117 | Sometimes: timestamp order write set recorded | dropped (no owned write set; backdated timestamps are task 02's) | – |
| MD/parallel_driver_timestamp_order/main.go:220 | Always: ListLogs pagination yields strictly increasing log sequences | **renamed** `ListLogs pagination yields strictly monotone log sequences in the requested direction` (the old literal assumed ascending; `reverse=true` on logs is descending) | lists.rs `check_traversal` |
| MD/parallel_driver_timestamp_order/main.go:223 | Always: effective timestamps are non-decreasing with log sequence | not ported here (task 11, global stream check) | – |
| MD/parallel_driver_list_transactions/main.go:36 | Sometimes: should be able to create tx for list test | dropped: tracked writes are task 02's and judged by the oracle | – |
| MD/parallel_driver_list_transactions/main.go:74,90,142 | Unreachable: ListTransactions should not fail / ListTransactions stream returned unexpected error / reverse ListTransactions stream returned unexpected error | oracle: `oracle: ListTransactions returned unexpected error` (transient classes are skipped as the old driver tolerated them) | h_query.go |
| MD/parallel_driver_list_transactions/main.go:112,113,153 | AlwaysOrUnreachable: ListTransactions should return at least one transaction / … should contain the just-created transaction / reverse ListTransactions should return results | oracle: the exact window check (`oracle: transaction query outside model`) requires every committed transaction a base holds | h_query.go |
| H/main.rs:322, 715 | Always: a list page never exceeds the requested page size | renamed into `clean-EOF list pagination respects the requested page size` (same check against the effective size) and the oracle's per-page check | lists.rs |
| H/main.rs:718 | Always: paging never returns an item twice | `clean-EOF list pagination yields no duplicate transaction IDs` (transactions) and `ListLogs pagination yields strictly monotone log sequences in the requested direction` (logs) | lists.rs |
| H/main.rs:723 | Always: list results are strictly ordered across pages | `clean-EOF list pagination yields strictly increasing transaction IDs` (ascending transactions) and the ListLogs monotone check; the oracle checks every page's order | lists.rs |
| H/main.rs:727 | Sometimes: a listing spans several pages | verbatim | lists.rs `check_traversal` |

## New properties

- `ListLogs pagination yields strictly monotone log sequences in the requested direction` (Always; renamed).

No other new literal. All oracle literals are `oracle: ` + the `M/` suffix.

## Reachability of must-hit assertions

| Literal | Rule / draw | Needs |
|---|---|---|
| `oracle: malformed transaction cursor rejected` | list_transactions, malformed cursor 1/16 | no faults |
| `oracle: malformed log cursor rejected` | list_logs, malformed cursor 1/16 | no faults |
| `oracle: refused account filter rejected` | list_accounts, rollFilterShape 1/8 | no faults |
| `oracle: refused transaction filter rejected` | list_transactions, rollFilterShape 1/8 | no faults |
| `oracle: refused log filter rejected` | list_logs, rollFilterShape 1/8 | no faults |
| `oracle: target-invalid query rejected on accounts` | list_accounts 1/16 | no faults |
| `oracle: target-invalid query rejected on transactions` | list_transactions 1/16 | no faults |
| `oracle: indexed account query gated` | list_accounts, `undeclared-` key 1/8 (INDEX_NOT_FOUND) | no faults |
| `oracle: indexed transaction query gated` | list_transactions, `undeclared-` key, reference/date/address leaves without indexes | no faults |
| `oracle: log query gated on a missing index` | list_logs, date leaf without the log-date index | no faults |
| `oracle: log query served results` | list_logs, index-free filters | no faults |
| `oracle: reverse log query served results` | list_logs, reverse 1/2 | no faults |
| `oracle: page size left to the server default` | every list rule, page size 1/16 | no faults |
| `oracle: page size clamped to the server maximum` | every list rule, page size 1/32 | no faults |
| `oracle: unsupported builtin uint field rejected` | list_transactions 1/24 | no faults |
| `clean-EOF list pagination respects the requested page size`, `… no duplicate transaction IDs`, `… strictly increasing transaction IDs`, `ListLogs pagination yields strictly monotone …` | list_traverse | no faults (Always) |
| `a listing spans several pages` | list_traverse, small page sizes | no faults |
| `oracle: kind-mismatched field query rejected on accounts` / `… on transactions` | list_accounts / list_transactions, 1/12 | needs task 07 |
| `oracle: indexed account query served results`, `oracle: indexed transaction query served results`, `oracle: has-asset under a combinator served results` | list rules | needs task 07 |
| `oracle: asset-index account query served results` | list_accounts has-asset 1/6 | needs task 07 |
| `oracle: asset-index account query gated` | list_accounts has-asset 1/6 (INDEX_NOT_FOUND) | no faults (hit in the native run), kept in the 07 block as the spec lists it |
| `oracle: has-asset precision overflow rejected` | list_accounts has-asset 1/16 × 1/6 | no faults (hit in the native run) |
| `oracle: coverage …` (Raw probes) | every index-backed read; the metadata-field probes need a leaf on an active metadata index (3/4 of leaves when one is declared, l1 aimed 1/4) and a non-empty page; the retype probe is task 07's `schema_bulk` retype + `KeyListPlan` | needs task 07 |

## Deviations from the task spec

- **kind-mismatched Reachables need task 07.** The server checks a Field leaf's index
  (`requireIndexReady`) before coercing the condition kind (`internal/query/compile.go`
  `resolveFieldMetadataCtx`), and nothing in this task creates a metadata index, so a
  kind-mismatched leaf answers INDEX_NOT_FOUND (counted as "indexed … gated") until task 07
  creates indexes. Both literals are in the "reachable only with task 07 merged" asserter block.
- **Two extra Reachables** ported from `M/` that the spec's list omits: `oracle: unsupported
  builtin uint field rejected` and `oracle: has-asset precision overflow rejected`; both were
  hit without faults in the native run.
- **`asset-index account query gated`** stays in the 07 block as the spec says, although an
  INDEX_NOT_FOUND on a bare has-asset leaf already reaches it without indexes (hit in the
  native run).
- **Fixture paths.** The spec names `HG/protocol-fixtures/filters/*.json`; COMMON.md lets a
  family add files only under `HG/protocol-fixtures/<NN>/`, so they are in
  `HG/protocol-fixtures/06/filters/`.
- **Field seeds have no committed sample values**: the core `ledger` view lists declared field
  types but no stored values, so string leaves aim at the generator's own metadata value pool
  (3/4) instead of a committed sample, and numeric centres are drawn in `0..1023`.
- **`list_traverse` is unfiltered** (as the old traversal drivers were), page size from
  `draw::page_size`, 1–8 pages, 2 s per page; no page starts with less than 250 ms of the
  8 s budget left, and the last page's timeout is cut to what is left, so the rule ends
  near 8 s plus one `/read/begin` and `/read/end`.
- **The clean-EOF literals run on shared tracked ledgers.** The old drivers owned their
  ledgers; here the traversal reads a tracked ledger other rules write to. The verbatim
  literals are evaluated only when every page was complete *and* the last page had an
  empty `nextCursor` (the clean EOF the old drivers reached), never on a prefix cut short
  by the page cap or the time budget. Concurrent writes do not break the properties: a
  page's size bound holds at any time, and id cursors exclude everything already served,
  so duplicates or out-of-order ids across pages remain findings. `a listing spans several
  pages` is still evaluated on clean prefixes.
- **Response signatures stay an unconditional finding** (spec asked for "a log of an
  unsigned tracked bulk"). `Log.response_signature` is the server signing its own
  responses (`ledger run --response-signing-key`, docs/ops/signing.md "Response Signing"),
  unrelated to request signing; no compose node sets that key, so the old literal and rule
  are kept verbatim.
- The client may stream `effective + 64` items, so a page that overshoots the page size still
  arrives `complete` and is judged (the old driver capped at `page + 1`, which hid overshoot).

## Requests to 99

- The spec's premise for renaming `log page carries a response signature` (task 16's request
  signing makes response signatures legal) does not hold: request signatures never reach
  `Log.response_signature`. If a later task does start a node with
  `--response-signing-key`, this check must change to comparing `KeyId` with the server key
  from Discovery, not to a per-bulk rule.
- The clean-EOF literals are kept verbatim on shared tracked ledgers, gated on reaching
  the end of the listing (see Deviations); confirm that meaning is acceptable, or rename.

- Every assertion of the "reachable only with task 07 merged" block of `h_query.go` and the
  `oracle: coverage …` probes: check reachability in the integrated build (DESIGN §9.2 item 4).
- `run_native.sh` whitelists never-hit must-hits only from `porting/00*.md`; the 07-dependent
  literals above show as "NEVER HIT" in a 06-only run (they do not fail it).
- Filter shapes the oracle does not classify as refused but the server might refuse (out-of-range
  `AddressRole` on accounts, `Not` without a child): inherited unchanged from `M/`; if the
  integrated run shows `ListAccounts returned unexpected error` with InvalidArgument for such a
  shape, the classification in `match.ClassifyRejectedFilter` (00b) needs the server's rule.

## Native run (acceptance)

`RULES=list_accounts,list_transactions,list_logs,list_traverse,smoke_tx run_native.sh 180`
(with `HEALTH_{WAL,DATA}_THRESHOLD=0.995` in the environment because the shared build machine's
disk was above the server's 80 % write gate): 26 576 list reads validated (12 522
ListTransactions, 8 374 ListLogs, 5 680 ListAccounts), zero findings; every must-hit of the
"no faults" rows above was hit; the only never-hit must-hits were the `oracle: coverage …`
probes (task 07).

Rerun after the review fixes (same command and environment): 21 727 `/read/end`, zero
findings, 73 assertions seen; the clean-EOF literals (now gated on reaching an empty
`nextCursor`) and `a listing spans several pages` were all hit; the only never-hit must-hits
were again the 12 `oracle: coverage …` probes (task 07).

---

<a id="07-indexes-typed-metadata"></a>

## 07 — Indexes, typed metadata, readiness, retype windows, index introspection

Go: `W/oraclecheck/indexcheck/` (package `indexcheck`), `W/bin/oracle/h_index.go`.
Rust: `HG/driver/src/rules/indexes.rs` (+ the 07 marker blocks of `rules/mod.rs`).
Fixtures: `W/oraclecheck/testdata/rejections/07-*.json`.

## What runs where

| Piece | Where |
|---|---|
| Create frontiers (`M/validate.go:90-116` `recordIndexCreates`) | drain hook `07-indexes` |
| Retype observations (`M/checker.go:369-440`, `M/validate.go:440-458`) | drain hook `07-indexes` (opens / re-arms), `IndexStatusPoll` (two-phase per node), settle hook (closure) |
| Readiness poller (`M/indexes.go:632-920`) | Rust `index_poll` (one read ticket per poll, stale node-pinned `GetIndexStatus` per node) + read kind `IndexStatusPoll` + settle hook `07-indexes` (deferred promotion) |
| Registry reads (`M/index_introspect.go`) | Rust `index_introspect` + read kinds `ListIndexes`, `GetIndex`, `GetIndexStatus` |
| InspectIndex (`M/index_inspect.go`) | Rust `inspect_index` + read kind `InspectIndex` (a resume is a second ticket whose request carries `cursor`) |
| Index op / schema gens (`M/actions.go:1076-1154`, `M/indexes.go:512-622`) | Rust `index_op`, order gens `schema_set` / `schema_remove`, local `schema_bulk` |
| `MD/parallel_driver_indexes` | `index_bootstrap` + `index_op` + `index_poll` (tracked, oracle-judged) |
| Metadata-index seeds on l1 (new, coverage) | `metadata_index_bootstrap`: `setMetadataFieldType` then `createIndex`, separate tracked writes, for `k0` on accounts (STRING) and `k1` on transactions (INT64) |
| `MD/parallel_driver_typed_metadata` | `typed_metadata_cycle` on reserved entities |
| `MD/parallel_driver_projection_alignment` | asset index from `index_bootstrap`/`index_op`; prepared-query half is task 08's |

## Old assertions

| Old source | Old assertion (kind: literal) | New | Where |
|---|---|---|---|
| M/indexes.go:676 | Reachable: index demoted to ambiguous | oracle: index demoted to ambiguous (broadened: fires on every not-ready observation of a tracked index; `wasActive` in details) | indexcheck/poll.go |
| M/indexes.go:698 | Reachable: index promotion deferred behind an earlier operation | oracle: index promotion deferred behind an earlier operation (once per pending promotion) | indexcheck/state.go settleHook |
| M/indexes.go:707 | Reachable: index promoted to active | oracle: index promoted to active | indexcheck/state.go settleHook |
| M/indexes.go:809 | Unreachable: index status is not self-consistent | oracle: index status is not self-consistent | indexcheck/poll.go |
| M/indexes.go:823 | Reachable: index status reported a materialised index file | oracle: index status reported a materialised index file | indexcheck/poll.go |
| M/indexes.go:901 | Reachable: retype window closed | oracle: retype window closed | indexcheck/state.go settleHook |
| M/indexes.go:253,264,316,318,356 | asset-index account query (Unreachable/Reachable) | not this task: index-served query pages are task 06's validators | (06) |
| M/indexes.go:928-1399 | indexed transaction/account query literals | not this task (task 06) | (06) |
| M/index_introspect.go:117 | Reachable: index listing on an absent ledger returned NotFound | oracle: … verbatim suffix | indexcheck/introspect.go |
| M/index_introspect.go:127 | Unreachable: ListIndexes returned unexpected error | oracle: ListIndexes returned unexpected error | introspect.go |
| M/index_introspect.go:137 | Unreachable: index listing served a ledger outside the fleet | oracle: … | introspect.go |
| M/index_introspect.go:149 | Unreachable: index listing violated its scope | oracle: … (also for an undecodable entry) | introspect.go |
| M/index_introspect.go:160 | Unreachable: index listing repeated an entry | oracle: … | introspect.go |
| M/index_introspect.go:175 | Reachable: index listing validated | oracle: … | introspect.go |
| M/index_introspect.go:183 | Unreachable: index listing outside model | oracle: … | introspect.go |
| M/index_introspect.go:243 | Unreachable: bucket-wide ListIndexes returned unexpected error | oracle: … | introspect.go |
| M/index_introspect.go:256 | Unreachable: bucket-wide index listing served an entry with no id | oracle: … | introspect.go |
| M/index_introspect.go:263 | Unreachable: bucket-scoped index listing served a ledger's entry | oracle: … | introspect.go |
| M/index_introspect.go:277 | Unreachable: bucket-wide index listing repeated an entry | oracle: … | introspect.go |
| M/index_introspect.go:291 | Reachable: bucket-scoped index listing validated | oracle: … | introspect.go |
| M/index_introspect.go:301 | Unreachable: bucket-wide index listing outside model | oracle: … (SCOPE_ALL restricted to tracked ledgers, `match.TrackedIndexListingMatches`, mayVanish kept) | introspect.go |
| M/index_introspect.go:312 | Reachable: fleet-wide index listing validated | oracle: … | introspect.go |
| M/index_introspect.go:360,448 | Unreachable: index lookup answered with another entry | oracle: … (one asserter for both RPCs; `rpc` in details) | introspect.go |
| M/index_introspect.go:373 | Reachable: index lookup validated | oracle: … | introspect.go |
| M/index_introspect.go:402 | Reachable: bucket-scoped index lookup returned NotFound | oracle: … | introspect.go |
| M/index_introspect.go:407 | Unreachable: bucket-scoped index lookup returned unexpected error | oracle: … | introspect.go |
| M/index_introspect.go:416 | Unreachable: bucket-scoped index lookup answered with a ledger's entry | oracle: … | introspect.go |
| M/index_introspect.go:461 | Reachable: index status validated | oracle: … | introspect.go |
| M/index_introspect.go:477 | Unreachable: index lookup served a ledger outside the fleet | oracle: … | introspect.go |
| M/index_introspect.go:494 | Unreachable: index lookup returned unexpected error | oracle: … | introspect.go |
| M/index_introspect.go:507 | Reachable: index lookup on an absent ledger returned NotFound | oracle: … | introspect.go |
| M/index_introspect.go:531 | Unreachable: index presence outside model | oracle: … (a NotFound is also explained by a base where the ledger is not live) | introspect.go |
| M/index_inspect.go:103 | Reachable: index inspection on an absent ledger returned NotFound | oracle: … | indexcheck/inspect.go |
| M/index_inspect.go:115 | Reachable: index inspection gated on a missing index | oracle: … | inspect.go |
| M/index_inspect.go:120 | Unreachable: InspectIndex returned unexpected error | oracle: … | inspect.go |
| M/index_inspect.go:132 | Unreachable: index inspection served a ledger outside the fleet | oracle: … | inspect.go |
| M/index_inspect.go:148 | Unreachable: index inspection violates its own contract | oracle: … | inspect.go |
| M/index_inspect.go:160 | Reachable: index inspection validated | oracle: … | inspect.go |
| M/index_inspect.go:164 | Reachable: index inspection served from a checkpoint | oracle: … | inspect.go |
| M/index_inspect.go:311 | Unreachable: resumed index inspection returned unexpected error | oracle: … | inspect.go |
| M/index_inspect.go:318 | Unreachable: resumed index inspection violates its own contract | oracle: … | inspect.go |
| M/index_inspect.go:324 | Reachable: index inspection resumed from its cursor | oracle: … | inspect.go |
| M/index_inspect.go:404 | Unreachable: summary mode answered another arm | oracle: … | inspect.go |
| M/index_inspect.go:447 | Reachable: index inspection summary matched the model | oracle: … | inspect.go |
| M/index_inspect.go:452 | Reachable: index inspection summary matched a populated index | oracle: … | inspect.go |
| M/index_inspect.go:462 | Reachable: index inspection summary counted unconvertible values | oracle: … | inspect.go |
| M/index_inspect.go:474 | Unreachable: index inspection summary outside model | oracle: … | inspect.go |
| MD/parallel_driver_indexes/main.go:42 | Unreachable: SetMetadataFieldType returned unexpected error | oracle (commit / failure checks of the tracked `schema_bulk`) | (00a) |
| MD/parallel_driver_indexes/main.go:71 | Unreachable: CreateIndex returned unexpected error | oracle (commit / failure checks: `INDEX_ALREADY_EXISTS`, `METADATA_FIELD_NOT_IN_SCHEMA` must be model-explained) | (00a) |
| MD/parallel_driver_indexes/main.go:79 | Unreachable: GetIndexStatus returned unexpected error | dropped: a stale per-node status error only counts as not ready (`IndexStatusPoll`); linearizable entry status errors are `oracle: index lookup returned unexpected error` | – |
| MD/parallel_driver_indexes/main.go:85 | AlwaysOrUnreachable: GetIndexStatus should return a response | dropped: a nil success cannot reach the driver (protojson outcome); `oracle: index status is not self-consistent` judges the response | – |
| MD/parallel_driver_indexes/main.go:100 | Unreachable: DropIndex returned unexpected error | oracle (commit / failure checks; a drop of an absent index is a model-predicted success) | (00a) |
| MD/parallel_driver_typed_metadata/main.go:140 | AlwaysOrUnreachable: typed metadata query should return a response | dropped here: the prepared query execution belongs to task 08 | (08) |
| MD/parallel_driver_typed_metadata/main.go:153 | AlwaysOrUnreachable: declared metadata key should appear in schema status | oracle (task 04 `GetMetadataSchemaStatus` read kind) | (04) |
| MD/parallel_driver_typed_metadata/main.go:156 | AlwaysOrUnreachable: declared type should be INT64 | oracle (task 04 `GetMetadataSchemaStatus`) | (04) |
| MD/parallel_driver_typed_metadata/main.go:181 | AlwaysOrUnreachable: changing metadata type should not crash | verbatim (definitive outcome is not Internal/Unknown) | rules/indexes.rs typed_metadata_cycle |
| MD/parallel_driver_typed_metadata/main.go:209 | AlwaysOrUnreachable: account should be readable after metadata type changes | oracle (task 05 GetAccount on the account; reads are model-checked) | (05) |
| MD/parallel_driver_typed_metadata/main.go:216 | AlwaysOrUnreachable: metadata key should survive type changes | oracle (task 05 GetAccount metadata equality: stored values are never rewritten) | (05) |
| MD/parallel_driver_typed_metadata/main.go:256 | AlwaysOrUnreachable: declaring an int64 type over an uncoercible value should not crash | verbatim | rules/indexes.rs typed_metadata_cycle |
| MD/parallel_driver_typed_metadata/main.go:275 | AlwaysOrUnreachable: declaring a bool type over a numeric value should not crash | verbatim | rules/indexes.rs typed_metadata_cycle |
| MD/parallel_driver_projection_alignment/main.go:84 | Unreachable: projection asset-index creation returned unexpected error | oracle (commit / failure checks of `index_bootstrap` / `index_op`) | (00a) |
| MD/parallel_driver_projection_alignment/main.go:92 | Unreachable: projection asset index did not become ready | dropped: readiness is observed, not awaited; never-promoted shows as `oracle: index promoted to active` missing | – |
| MD/parallel_driver_projection_alignment/main.go:124,152,159,172,178,182,196 | prepared-query / probe / projection-aligned read literals | not this task: has-asset reads are task 06 (ListAccounts) and task 08 (prepared queries), judged by the oracle's has-asset window | (06, 08) |

## New properties

- `oracle: metadata field not in schema rejection exercised` (Reachable, failure hook):
  a CreateIndex on an undeclared metadata field was rejected and explained by the model.
- `a write attempt is refused while an index builds` (Rust Sometimes, DESIGN §4.9): an arm
  of `write::explore_attempt`, so it sees every tracked attempt of every family. The only
  write-path `INDEX_BUILDING` is a `createQueryCheckpoint` (task 09) while the audit
  projection rebuilds after a restart (`admission.go` `checkQueryCheckpointProjectionReady`);
  index-family writes never get it. It used to be checked only on this family's attempt
  logs, so it could never be true (run 3d8b6ff1…-67-0).
- Promotion is sound across polls: only one poll covering every discovered node can
  promote (critic C9), verdicts are never aggregated across polls.

## Reachability

| Must-hit | Rule / path | Needs |
|---|---|---|
| oracle: index promoted to active | `index_bootstrap`/`index_op` create, then an all-nodes `index_poll` | no faults |
| oracle: index demoted to ambiguous | `index_poll` right after a create (node not built yet) | no faults (more under faults) |
| oracle: index promotion deferred behind an earlier operation | `index_poll` while a concurrent rule holds an older ticket | no faults (concurrency ≥ 2) |
| oracle: retype window closed | `schema_bulk` retypes a declared key that `index_op` / `metadata_index_bootstrap` indexed; two later `index_poll`s | no faults |
| oracle: coverage a metadata-field index served a model-verified page on accounts / transactions | `metadata_index_bootstrap` (k0 / k1 on l1) + `index_poll` promotion, then a list query whose Field leaf aims at the active index (task 06) | no faults |
| oracle: coverage a query was served while a retype window was open | `schema_bulk` retype probe (1/4): re-declare an actively indexed key on l1, then one list on that key right away | no faults |
| oracle: index status validated | `index_introspect` GetIndexStatus arm | no faults |
| oracle: index status reported a materialised index file | `index_poll` | no faults |
| oracle: index listing validated / fleet-wide … / bucket-scoped index listing validated | `index_introspect` list arms | no faults |
| oracle: bucket-scoped index lookup returned NotFound | `index_introspect` GetIndex 1/8 bucket slot | no faults |
| oracle: index lookup validated | `index_introspect` GetIndex | no faults |
| oracle: index listing / lookup on an absent ledger returned NotFound | `index_introspect` absent 2 % | no faults |
| oracle: index inspection validated / gated / resumed / on an absent ledger | `inspect_index` | no faults |
| oracle: index inspection summary matched the model / a populated index | `inspect_index` SUMMARY on an indexed key | no faults |
| oracle: index inspection summary counted unconvertible values | `typed_metadata_cycle` writes `own-bad = "not-a-number-<v>"` (declared INT64, indexed while the model lacks the index); `inspect_index` inspects l1 with the owned view 1/4 | no faults |
| oracle: index inspection served from a checkpoint | `inspect_index` with `HEGEL_HAVE_CHECKPOINTS=1` | needs task 09 |
| oracle: metadata field not in schema rejection exercised | `index_op` 1/16 probe | no faults |
| a write attempt is refused while an index builds | a `createQueryCheckpoint` attempt (task 09) answered `INDEX_BUILDING` inside the audit-projection rebuild after a restart | faults (rare) |
| changing metadata type should not crash (+ the two declaring literals) | `typed_metadata_cycle` | no faults (AlwaysOrUnreachable) |

## Deviations from the spec

- `indexGaps` answers `{"gaps": {ledger: [{"canonical", "id"}…]}}` instead of
  `{ledger: [canonical…]}`: the driver needs the IndexID protojson to send a create, and the
  view takes an optional `ledger`/`ledgerPick` (the core then echoes `"ledger"`).
- Read kind `GetIndexStatus` sends the `GetIndexEntryStatus` RPC (the registry entry
  status of one index, `M/index_introspect.go runIndexEntryStatus`, which is what
  `index status validated` covered); the ledger-wide `GetIndexStatus` RPC is the stale poll.
- `index demoted to ambiguous` fires on every not-ready observation (the old one only on an
  active→ambiguous flip), so it is reached right after creates without faults.
- `typed_metadata_cycle` declares INT64 over an uncoercible value on a fresh reserved key
  `own-bad<N>` per cycle (as the old driver's `bad-convert-<rand>`), so that check always
  declares a type over uncoercible data, then removes it. Separately, every cycle writes
  `"not-a-number-<v>"` under one shared reserved key `own-bad`, and, while the owned `ledger`
  view of l1 shows no `metadata:TARGET_TYPE_ACCOUNT:own-bad` index (the model decides, not a
  counter, so a cut-short cycle cannot lose it), declares it INT64 and creates its index as
  the cycle's last two steps (a racing duplicate create fails last and costs nothing).
  `own-bad` is never removed, so its index holds many distinct unconvertible values:
  `inspect_index` reaches `summary counted unconvertible values` and `resumed from its
  cursor` on it. `inspect_index` reads `o-<run>-l1` with the owned `ledger` view 1/4 of the
  time (reads cannot disturb rule-owned entities).
- Index growth on l1 is bounded: `RemoveMetadataFieldType` drops the key's metadata index in
  the same order (server: the `RemovedMetadataFieldType` log carries the `DroppedIndex`;
  model: `applyRemoveMetadataFieldType`), and every cycle ends by removing `own-score<N>`
  and `own-bad<N>`. A cycle cut short (handoff, budget, failed step, no free slot) leaves
  its declaration (and the `own-score<N>` index); the next cycle removes up to two such
  leftovers first, skipping the keys of cycles still in flight (`TypedMeta.inflight`).
- "Sequential on one slot unit": the steps run strictly one after another, but each takes
  its own unit, because `begin_write` consumes the unit and the ticket releases it when it
  ends (core `write.rs` API, not owned here). No free unit ends the cycle like a handoff.
- Retype closure phase two accepts a node's `pendingVersion == 0` only from a poll that began
  after the poll that armed the node ended (`Ticket > armedAt`, armedAt = that poll's
  MaxTicket) and that still shows `lastIndexed >= openSeq`: `index_poll` runs concurrently,
  unlike the old single poller goroutine, so "a later poll" must be later in time.
- The drain hook (re-)arms a retype observation only when the bulk opened the window or
  changed the declared type (walking the bulk in order, so A→B→A counts); a same-type
  re-declaration on an open window keeps the observation.
- `oracle: summary mode answered another arm` is checked before the generic page contract
  (which otherwise reported the same case as `index inspection violates its own contract`).
- `inspect_index` probes an absent ledger 1/20 (the spec lists the must-hit, not the rate).
- Rejection fixture "drop of an absent index": not a rejection. The server tombstones the
  key and logs a `DroppedIndexLog` (`processDropIndex`), the model predicts success
  (`applyDropIndex`); `TestRejectionStages` requires a rejection, so the shape is covered by
  `indexcheck.TestDropOfAbsentIndexIsANoOpSuccess` instead of a fixture.
- `index_op`: create with 1/3 (gap) or 1/8 (no gap), else a drop of a pool index (present
  or not); a create without a gap targets a pool index, so it is a duplicate
  (`INDEX_ALREADY_EXISTS`). A create picks a metadata gap 3/4 of the time when there is one
  (the builtins are bootstrapped; field-leaf queries need metadata indexes).
- `metadata_index_bootstrap` (new, coverage fix for run 3d8b6ff1…-67-0, where no transaction
  metadata index and only two account ones were ever promoted): reads the public `ledger`
  view of l1 and, per seed (`k0` accounts STRING, `k1` transactions INT64) whose index the
  model lacks, declares the type (unless already declared with it) and creates the index as
  **separate** tracked writes, so the create sees a committed declaration. Its own done flag
  (not `index_bootstrap`'s): `chance(1/2)` per run until done, then `chance(1/8)` to re-create
  a seed index that `index_op` dropped or `schema_remove` took with its declaration.
  `INDEX_ALREADY_EXISTS` on a create counts as done.
- `schema_bulk` retype probe (new): 1/4 of runs read l1's public view; if a declared,
  non-reserved key has an active metadata index, the bulk is one `setMetadataFieldType` on
  it (the current type 1/2, else one of STRING / INT64 / BOOL / UINT64; a same-type
  re-declaration opens a window in the model too), and after it commits one
  ListAccounts / ListTransactions page filtered on that key (`lists::KeyListPlan`, drawn
  before the write), so a page can be served while the window is open. Otherwise the run is
  the usual 1–2-order bulk on a drawn ledger.

## Requests to 99

- Set `HEGEL_HAVE_CHECKPOINTS=1` in the driver env once task 09 (view `checkpoints`) is
  merged; `oracle: index inspection served from a checkpoint` needs it. `inspect_index`
  reads `checkpoints.live` (an array of decimal id strings, checked against
  `hegel/09-query-checkpoints` `checkpointcheck/hooks.go viewCheckpoints`) and sends one as
  `checkpointId`; re-check if 09's view shape changes before merge.
- `a write attempt is refused while an index builds` needs faults and a checkpoint create
  inside the audit rebuild window; it is now asserted on every tracked attempt
  (`write::explore_attempt`), not only on this family's (which never get it).
- Task 06's indexed-query validators depend on `index promoted to active` happening; with
  `index_op` at weight 1.0 roughly half of the pool is present at any time.

---

<a id="08-prepared-queries"></a>

## 08 — Prepared queries: porting record

Sources: `M/prepared_queries.go`, `M/prepared_queries_read.go`, `M/validate.go:563`
(rejection coverage), `MD/parallel_driver_queries`, `MD/parallel_driver_update_query`,
`MD/parallel_driver_projection_alignment` (prepared half).

New code: `W/oraclecheck/preparedcheck/` (validators, view, begin hook, failure hook),
`W/bin/oracle/h_prepared.go` (asserters), `HG/driver/src/rules/prepared.rs` (rules and
order gens), `W/oraclecheck/testdata/rejections/08-*.json`, `HG/protocol-fixtures/08/`.

## Assertion map

| Old source | Old assertion (kind: literal) | New | Where |
|---|---|---|---|
| M/prepared_queries_read.go:67 | Unreachable: ListPreparedQueries returned unexpected error | oracle: ListPreparedQueries returned unexpected error | preparedcheck/list.go |
| M/prepared_queries_read.go:62 (validateLedgerNotFound) | Unreachable (core): ledger-scoped NotFound unexplained | core `ReadCtx.LedgerNotFoundExplained` | preparedcheck/list.go |
| M/prepared_queries_read.go:93 | Unreachable: prepared query listing outside model | oracle: prepared query listing outside model | preparedcheck/list.go |
| M/prepared_queries_read.go:365 | Reachable: prepared query page resumed in the opposite direction | oracle: prepared query page resumed in the opposite direction | preparedcheck/execute.go (follow-up page validated, direction differs from the remembered first page) |
| M/prepared_queries_read.go:448 | Reachable: aggregate on a non-accounts prepared query rejected | oracle: aggregate on a non-accounts prepared query rejected | preparedcheck/execute.go; now fired only when the rejection is also explained by a candidate (was: any error) |
| M/prepared_queries_read.go:489 | Reachable: reverse aggregate on a prepared query rejected | oracle: reverse aggregate on a prepared query rejected | preparedcheck/execute.go |
| M/prepared_queries_read.go:497 | Unreachable: reverse aggregate on a prepared query not rejected | oracle: reverse aggregate on a prepared query not rejected | preparedcheck/execute.go |
| M/prepared_queries_read.go:517 | Unreachable: prepared query execution returned unexpected error | oracle: prepared query execution returned unexpected error | preparedcheck/execute.go |
| M/prepared_queries_read.go:527 | Unreachable: prepared query cursor page size mismatch | oracle: prepared query cursor page size mismatch | preparedcheck/execute.go (expected = `match.EffectivePageSize(requested)`; the driver only requests sizes served unchanged) |
| M/prepared_queries_read.go:547 | Unreachable: prepared query page outside model | oracle: prepared query page outside model | preparedcheck/execute.go |
| M/prepared_queries_read.go:849 | Unreachable: prepared query aggregate outside model | oracle: prepared query aggregate outside model | preparedcheck/execute.go |
| M/prepared_queries_read.go:272 (`dbgf PQPARAM`) | debug trace | dropped: debug only; the spoiled-parameter path is covered by the window check | – |
| M/validate.go:563 | Reachable: malformed prepared-query filter refused at save | oracle: malformed prepared-query filter refused at save | preparedcheck/serialize.go `FailureCoverage` (an explained admission-stage FILTER_COMPILATION_ERROR of a bulk that saves a filter, create or update; the FSM-stage update refusal has its own literal below) |
| MD/parallel_driver_queries/main.go:52 | Unreachable: prepared query creation returned unexpected error | oracle (every definitive write outcome is explained by `validateFailure`, core `oracle: bulk failure not explained by any serialization`; non-definitive outcomes are retried) | (00a) |
| MD/parallel_driver_queries/main.go:69 | Sometimes: should be able to execute prepared query | oracle: prepared query page validated (new Reachable on a validated successful page) | preparedcheck/execute.go |
| MD/parallel_driver_queries/main.go:74 | AlwaysOrUnreachable: prepared query should return a response | oracle: prepared query page outside model (a success without a cursor result is `WrongResult`, outside every candidate) | preparedcheck/execute.go |
| MD/parallel_driver_queries/main.go:88 | Unreachable: prepared query deletion returned unexpected error | oracle (as :52) | (00a) |
| MD/parallel_driver_update_query/main.go:59 | Unreachable: prepared query creation returned unexpected error | oracle (as above) | (00a) |
| MD/parallel_driver_update_query/main.go:85 | Sometimes: should be able to update prepared query | oracle: model outcome verified (00a) for committed updates; not re-asserted: the shared pool makes the outcome non-deterministic | (00a) |
| MD/parallel_driver_update_query/main.go:104 | AlwaysOrUnreachable: updated query should return a response | oracle: prepared query page outside model (as :74) | preparedcheck/execute.go |
| MD/parallel_driver_projection_alignment/main.go:84 | Unreachable: projection asset-index creation returned unexpected error | dropped here: the asset index on `o-<run>-l1` is task 07's `index_bootstrap` (its outcome is oracle-validated there) | – |
| MD/parallel_driver_projection_alignment/main.go:92 | Unreachable: projection asset index did not become ready | dropped: readiness is task 07's poller; an execution while the index is absent or building is gated legally by the oracle (`IndexedQueryOutcomeLegal`) | – |
| MD/parallel_driver_projection_alignment/main.go:124 | Unreachable: projection prepared-query creation returned unexpected error | oracle (definitive outcome explained, 00a) | (00a) |
| MD/parallel_driver_projection_alignment/main.go:152 | Unreachable: projection probe apply returned unexpected error | oracle (as above) | (00a) |
| MD/parallel_driver_projection_alignment/main.go:159 | Unreachable: projection probe apply succeeded without an audit log | oracle: core empty-commit check (`validateEmptyCommit`) | (00a) |
| MD/parallel_driver_projection_alignment/main.go:172 | Unreachable: projection-aligned prepared query returned unexpected error | oracle: prepared query execution returned unexpected error / page outside model | preparedcheck/execute.go |
| MD/parallel_driver_projection_alignment/main.go:178 | Reachable: projection-aligned prepared query succeeded | verbatim | rules/prepared.rs projection_probe (successful execute after both writes succeeded) |
| MD/parallel_driver_projection_alignment/main.go:182 | Unreachable: projection-aligned prepared query returned no cursor result | oracle: prepared query page outside model (`WrongResult`) | preparedcheck/execute.go |
| MD/parallel_driver_projection_alignment/main.go:196 | Always: projection-aligned read includes the acknowledged write | oracle: prepared query page outside model (the exact window on every candidate is a superset check: the acknowledged write precedes the read's ticket, so it is in every candidate base; a missing row is outside the window). To keep the probe account inside a checked window on the shared `o-<run>-l1`, the probe reads ascending pages of 1000 and follows the server cursor (one ticket per page, `prevTicket`) until a page lists the account or an address after it (at most 4 pages); its drawn branch uses an asset no generator writes (precision 1..255 except 2), so the account is among few matches. Not re-asserted verbatim: on the shared ledger the outcome (index state, traversal length) is not deterministic | preparedcheck/execute.go, rules/prepared.rs projection_probe |

## New properties

- `oracle: prepared query page validated` (Reachable): a successful LIST page matched a
  candidate (first or follow-up page).
- `oracle: prepared query aggregate validated` (Reachable): a successful AGGREGATE matched
  a candidate (exact bucket set and totals).
- `oracle: an update with a filter invalid for the stored target is refused` (Reachable):
  failure hook on an explained FSM-stage FILTER_COMPILATION_ERROR of an update (the
  definitive-unfrozen class of DESIGN §4.5).
- Begin hook `prepared-serialize` (`409 serialized`, not an assertion). It keeps the
  outcome of an update's FSM-stage FILTER_COMPILATION_ERROR (uncommitted, re-evaluated on
  every attempt) the same for every attempt, including one that reaches the server after
  the ticket ended. A prepared-query-only bulk's outcome depends only on whether each named
  query exists, its stored target and whether its ledger is live. The hook refuses:
  - a bulk with an update and any order that is not a prepared-query order (its outcome
    would depend on balances and other state); `pq_update` also `applies` only to
    single-order bulks, so composed bulks (task 02's `tx_bulk`) never carry one;
  - a bulk that creates and updates/deletes the same name;
  - a bulk touching a prepared query `(ledger, name)` while another in-flight or pending
    bulk touches it;
  - a bulk updating a prepared query of a ledger an outstanding bulk deletes, and a
    DeleteLedger of a ledger an outstanding bulk updates a prepared query of;
  - after a ticket ended with an FSM-stage failure whose reason is in
    `oracle.UncommittedReasons`, until the next `/quiet/begin` (faults stopped, every
    client answered a barrier, nothing in flight): a create or delete of a prepared query
    it names, or a DeleteLedger of its ledger (`blockTable`; updates stay allowed, they
    never change a target). This closes a hole DESIGN §4.5 leaves open: the hook only
    serialized for the ticket's life, while a delayed attempt (relative `grpc-timeout`)
    can still be evaluated after the ticket ended; §4.5 should say so (request to 99).
- View `preparedQueries`: `{"queries":[{name,target,params:[{name,kind}]}]}`, `own-` names
  hidden unless `"owned": true`.

## Reachability

| Must-hit | Rule / path | Needs |
|---|---|---|
| oracle: prepared query page validated | prepared_execute (LIST, success) | no faults |
| oracle: prepared query aggregate validated | prepared_execute (AGGREGATE 1/3 on an ACCOUNTS query, success) | no faults |
| oracle: prepared query page resumed in the opposite direction | prepared_execute follow-up page, flip 1/4, page with `next` (page sizes 1–3 on ≥ 2 matching accounts) | no faults |
| oracle: reverse aggregate on a prepared query rejected | prepared_execute, AGGREGATE + reverse 1/8 | no faults |
| oracle: aggregate on a non-accounts prepared query rejected | prepared_execute, AGGREGATE target probe 1/8 on a TRANSACTIONS query | no faults |
| oracle: malformed prepared-query filter refused at save | prepared_write, malformed filter 1/16 (create: admission) | no faults |
| oracle: an update with a filter invalid for the stored target is refused | prepared_write `pq_update` with a malformed or target-specific filter on a stored query | no faults |
| projection-aligned prepared query succeeded | projection_probe, after both writes succeed; with the asset index absent the execute fails with an index error, so a success needs task 07's `index_bootstrap` | needs task 07 |

All of them are covered by Go unit tests (`preparedcheck_test.go`) and the in-process
integration test (`bin/oracle/h_prepared_integration_test.go`), and the Rust literal by
`rules::prepared::tests::projection_probe_creates_writes_and_executes`.

## Notes and deviations

- Protocol fixtures live in `HG/protocol-fixtures/08/` (the spec's unit-test bullet says
  `prepared/`; the ownership list and COMMON say `08/`). Written by
  `PROTOCOL_FIXTURES_WRITE=1 cargo test prepared::tests::protocol_fixtures`, checked by
  that test without the variable and decoded by `preparedcheck/fixtures_test.go`.
- A follow-up page carries `params: {"prevTicket": <ticket of the first page>}`; the
  oracle remembers validated pages whose cursor advertised more (last key, direction; 1024
  newest) and derives the model-side after-key itself, so the opaque cursor is never decoded
  and the driver supplies no model data. A follow-up whose previous page is unknown or whose
  cursor differs is not judged.
- Targets are ACCOUNTS and TRANSACTIONS only (spec), not LOGS as in `M/`.
- Draws after `/read/begin` (DESIGN §4.7): `prepared_execute` draws the fallback pool name,
  the parameter spoil mode and three values per parameter kind before begin; after begin it
  only draws indices (the query among the stored ones, the spoiled reference, a value per
  reference).
- Every malformed shape `malformed_filter` sends (`{"address":{}}`, a field condition
  without a field reference, `reverted` on ACCOUNTS, has-asset on TRANSACTIONS) is in the
  protocol fixture `malformed-filters.json` (strict decode + server validators) and in the
  rejection fixtures `08-pq-{create,update}-*` (stage and freeze against a real server),
  and goes through the integration test.
- `own-pq-asset<N>` queries stay on `o-<run>-l1` (one per probe, weight 0.3); they are
  listed by ListPreparedQueries and compared like any other stored query.

## Requests to 99

- DESIGN §4.5 "definitive-unfrozen": amend to say the serialization holds beyond the
  ticket's life (until the next quiet period) and also covers ledger deletion and
  non-prepared-query orders in the same bulk (see the begin hook above). In degraded mode
  (no `/quiet/begin`) blocked names stay blocked; the oracle is off anyway.

- `projection-aligned prepared query succeeded` needs task 07's `index_bootstrap` (asset
  index on `o-<run>-l1`); excluded from this task's acceptance.
- Execute pages on TRANSACTIONS queries and account metadata filters mostly return
  index errors without task 07's indexes; with 07 merged they become served pages.

---

<a id="09-query-checkpoints"></a>

## Porting record — 09 query checkpoints

Task 09 ports the singleton model's query-checkpoint machinery
(`M/checkpoints.go`, `M/checkpoint_setup.go`, `M/checkpoint_reads.go`,
`M/checkpoint_metadata.go`, the nine probes of `M/coverage.go:212-220`) and
`MD/parallel_driver_query_checkpoints` into the oracle
(`W/oraclecheck/checkpointcheck`, asserters in `W/bin/oracle/h_checkpoint.go`)
and five Rust rules plus one setup hook in `HG/driver/src/rules/checkpoints.rs`.

| Rule | Weight | What |
|---|---|---|
| `checkpoint_bootstrap` | 1.0 | until a create was acknowledged (`CheckpointExt.created`), `chance(1/2)` creates a probe checkpoint through the write protocol |
| `checkpoint_write` | 1.0 | one singleton lifecycle bulk: set non-firing schedule 1, delete schedule 1, invalid cron 1, delete id 0 1, delete next id 1, delete a live id 2 (6 and the oldest id once `live ≥ limit − 1`), create 3. A create takes `predicted.checkpointId` from `/write/begin` and races `ticket.run` against a pinned-node `GetLedger{read:{checkpointId}}` of the first live tracked ledger under read kind `PredictedCheckpointRead` |
| `checkpoint_read` | 1.0 | read kind `CheckpointRead`, pinned node, 5 s: GetAccount, GetTransaction, ListAccounts page, ListTransactions page, AggregateVolumes, GetLedgerStats, GetLog, ListLedgers against a live or retained-deleted checkpoint. ListLedgers cursors: none, the tracked prefix's edge, the frozen target ledger, or any tracked name the oracle has seen (created after the checkpoint or deleted included, `M/checkpoint_reads.go:195-197`) |
| `checkpoint_registry` | 0.5 | read kinds `CheckpointRegistry` / `CheckpointSchedule` / `CheckpointInfo` (1:1:1; info asks `GetQueryCheckpointInfo` for a live or retained-deleted id from view `checkpoints`); one read ticket covers the driver-run fence (`GetClusterState{}` → `Leader` + node id, `Barrier`, poll `GetClusterState{nodeId}` every 20–200 ms (drawn)) and the metadata read on the same node, all within 9 s of the rule's start (the fence by 7 s; every call's timeout is capped by the time left); a failed fence ends the ticket with a local `Unavailable` (transient) outcome; the fence record travels in `hint` |
| `checkpoint_burst` | 0.3 | draws 50–200 forced writes (bodies + routes), creates a checkpoint, and only if the create succeeded submits a quiet-blocking job that sends the burst unkeyed, single-attempt, `send_untracked`, from all clients concurrently, within 20 s (2 s per write; writes left at the deadline are dropped). Ledger `r-burst-<run>` for the first 16 bursts, then `r-burst-<run>-<gen>` (rotated every 16); at most 256 bursts per run |
| setup hook `checkpoint_setup` | – | verifies an empty registry and no schedule (logs otherwise), creates `r-burst-<run>` |

Oracle side: begin hook `checkpoint-serialize` (`predicted.checkpointId =
Model().NextQueryCheckpointID()`; a second create while one is in flight or
pending → `409 serialized`; replaces `checkpointCreateMu`, `M/main.go:344-380`);
view `checkpoints` (`live`, `deleted` = retained deleted ids, `nextId`, `limit`,
`schedule`, and with `pick` a `target` resolved **on the frozen snapshot**:
the id, `deleted`, `maxSequence`, a ledger live in the frozen state, an
optional seek on it, a frozen log sequence, and `seenLedger` — any
non-reserved tracked name the oracle has seen, for the ListLedgers cursor); drain hook `checkpoint-coverage`
and failure hook `checkpoint-limit` (coverage only; 00a's commit path keeps
`checkpointOrdersMatch` and the frozen snapshots).

## Assertions

| Old source | Old assertion (kind: literal) | New | Where |
|---|---|---|---|
| M/checkpoint_reads.go:330 | Unreachable: checkpoint read outside frozen model | oracle: checkpoint read outside frozen model | checkpointcheck/reads.go (checkpoint_read) |
| M/checkpoint_reads.go:189-245 | (ListLedgers arm inside the line above: exact name window) | renamed/loosened: foreign-tolerant `match.LedgerListingMatches` with the frozen snapshot as base (DESIGN §3.2, review-correctness 6); same literal | checkpointcheck/reads.go |
| M/checkpoint_reads.go:245 | Reachable: coverage a checkpoint served the frozen fleet listing | oracle: coverage a checkpoint served the frozen fleet listing | reads.go, h_checkpoint.go (checkpoint_read ListLedgers arm) |
| M/checkpoint_reads.go:380 | Unreachable: predicted checkpoint read returned unexpected error | oracle: predicted checkpoint read returned unexpected error; loosened: NotFound is also legal when the ledger was not live in the state the create froze, or some candidate base has deleted it since (`match.CheckpointLedgerReadOutcomeMatches`; no frozen checkpoint serves a deleted ledger) | checkpointcheck/metadata.go (checkpoint_write create) |
| M/checkpoint_reads.go:391 | Unreachable: predicted checkpoint read outside creation model | oracle: predicted checkpoint read outside creation model | checkpointcheck/metadata.go |
| M/checkpoint_reads.go:436 | Unreachable: checkpoint list returned unexpected error | oracle: checkpoint list returned unexpected error | checkpointcheck/metadata.go (checkpoint_registry) |
| M/checkpoint_reads.go:444 | Unreachable: checkpoint list sequence outside captured model | oracle: checkpoint list sequence outside captured model (also checks retained deleted ids' max sequence) | checkpointcheck/metadata.go |
| M/checkpoint_reads.go:451 | Unreachable: checkpoint list outside model | oracle: checkpoint list outside model | checkpointcheck/metadata.go |
| M/checkpoint_reads.go:466 | Unreachable: checkpoint schedule returned unexpected error | oracle: checkpoint schedule returned unexpected error | checkpointcheck/metadata.go |
| M/checkpoint_reads.go:471 | Unreachable: checkpoint schedule outside model | oracle: checkpoint schedule outside model | checkpointcheck/metadata.go |
| M/coverage.go:212 (checkpoints.go:105) | Raw Sometimes: coverage checkpoint creation matched the oracle | oracle: coverage checkpoint creation matched the oracle (Raw) | hooks.go drain hook |
| M/coverage.go:213 (checkpoints.go:113,123) | Raw: coverage checkpoint deletion matched the oracle | oracle: coverage checkpoint deletion matched the oracle (Raw) | hooks.go drain hook |
| M/coverage.go:214 (checkpoint_reads.go:340) | Raw: coverage checkpoint served a frozen model-verified read | oracle: coverage … (Raw) | reads.go |
| M/coverage.go:215 (checkpoint_reads.go:337) | Raw: coverage deleted checkpoint read returned typed NOT_FOUND | oracle: coverage … (Raw) | reads.go |
| M/coverage.go:216 (checkpoint_reads.go:455) | Raw: coverage checkpoint listing matched the oracle | oracle: coverage … (Raw) | metadata.go |
| M/coverage.go:217 (validate.go:551) | Raw: coverage checkpoint limit returned typed CHECKPOINT_LIMIT_REACHED | oracle: coverage … (Raw) | hooks.go failure hook |
| M/coverage.go:218 (checkpoints.go:125) | Raw: coverage checkpoint schedule set matched the oracle | oracle: coverage … (Raw) | hooks.go drain hook |
| M/coverage.go:219 (checkpoints.go:127) | Raw: coverage checkpoint schedule deletion matched the oracle | oracle: coverage … (Raw) | hooks.go drain hook |
| M/coverage.go:220 (checkpoint_reads.go:475) | Raw: coverage checkpoint schedule read matched the oracle | oracle: coverage … (Raw) | metadata.go |
| M/checkpoint_setup.go:95 | Unreachable: checkpoint setup failed | dropped: setup no longer reconstructs a baseline on a reused cluster (fresh cluster: verify, log otherwise); the first create is a rule whose commit is checked by the oracle | – |
| M/checkpoints.go:58-96 | (checkpointOrdersMatch, inside commit validation) | (00a commit path) | – |
| MD/parallel_driver_query_checkpoints/main.go:45 | Sometimes: query checkpoint capacity reached | oracle: coverage checkpoint limit returned typed CHECKPOINT_LIMIT_REACHED (subsumes: definitive and model-explained) | – |
| MD/…/main.go:60 | Unreachable: CreateQueryCheckpoint should not fail | oracle (commit check / `oracle: bulk failure not explained by any serialization`, 00a) | – |
| MD/…/main.go:79 | Unreachable: owned query checkpoint cleanup returned unexpected error | dropped: no per-invocation ownership; deletions are oracle-validated lifecycle orders | – |
| MD/…/main.go:85 | Reachable: query checkpoint created | oracle: coverage checkpoint creation matched the oracle | – |
| MD/…/main.go:94 | Unreachable: ListQueryCheckpoints should not fail | oracle: checkpoint list returned unexpected error | – |
| MD/…/main.go:109 | Sometimes: created checkpoint should appear in list | oracle: checkpoint list outside model (exact registry after a fence; strictly stronger, so mapped, not re-asserted) + coverage checkpoint listing matched the oracle | – |
| MD/…/main.go:127 | Unreachable: GetQueryCheckpointInfo should not fail | oracle: checkpoint info returned unexpected error (fenced; NotFound legal only when some candidate base lacks the id, else oracle: checkpoint info outside model) | checkpointcheck/metadata.go (checkpoint_registry info arm) |
| MD/…/main.go:133 | AlwaysOrUnreachable: checkpoint info should match requested ID | oracle: checkpoint info outside model | checkpointcheck/metadata.go |
| MD/…/main.go:137 | AlwaysOrUnreachable: checkpoint max sequence should be consistent | oracle: checkpoint info outside model (max sequence = the one captured at the create's drain); the listing's equivalent is oracle: checkpoint list sequence outside captured model | checkpointcheck/metadata.go |
| MD/…/main.go:154 | Unreachable: DeleteQueryCheckpoint should not fail | oracle (commit / failure check, 00a) | – |
| MD/…/main.go:177 | Sometimes: deleted checkpoint should not appear in list | oracle: checkpoint list outside model | – |
| MD/…/main.go:180 | Reachable: query checkpoint lifecycle completed | oracle: coverage checkpoint deletion matched the oracle | – |

## New properties

- `a write burst follows a query checkpoint create` (Rust Sometimes, burst
  job): at least one write of a post-create burst committed. The burst exists
  to reach the server's `spool replay after gated window applied at least one
  entry` and `spool pruned after resync` (server-signals §1.1, §1.2), which
  are task 12's log signals.
- `oracle: checkpoint info outside model` / `oracle: checkpoint info returned
  unexpected error` (Unreachable) and `oracle: coverage checkpoint info
  matched the oracle` (Reachable), read kind `CheckpointInfo`: a fenced
  `GetQueryCheckpointInfo` names the requested id, reports the captured max
  sequence, and the id is live in some candidate base; NotFound only when
  some candidate base lacks it.
- `oracle protocol: checkpoint read request is malformed` (Unreachable): a
  checkpoint read kind got a request the driver never builds (unknown arm,
  undecodable body, no checkpoint id) — a driver bug, kept apart from the
  ledger findings.
- The begin hook makes every create's id exact before it is sent
  (`predicted.checkpointId`), and the oracle's commit check requires the
  server to assign it.

## Deviations

- **Target aiming on the frozen snapshot.** The spec says "ledger targets via
  `ledgerPick` on the same begin". The core resolves `ledgerPick` on the live
  snapshot, which can name a ledger created after the checkpoint; the old
  model only read ledgers live in the frozen state, and a checkpoint read of
  a never-frozen ledger has no modelled answer. The `checkpoints` view
  therefore takes `pick` / `cpLedgerPick` / `logPick` (pre-drawn `u32`s, no
  post-begin draws) and resolves checkpoint, ledger, seek entry and log
  sequence on the frozen snapshot inside the same `/read/begin`.
- **Arm in params.** `CheckpointRead`'s arm is named in the `/read/end`
  params (`{"arm","id"}`); everything the validator judges is decoded from the
  request (checkpoint id, ledger, address, cursor, options).
- **GetLog arm.** A sequence the frozen state holds must match its frozen
  log; a sequence ≥ `maxSequence + 2^39` must be NotFound (the driver sends
  `+2^40`); any other sequence is not judged (the model retires deleted
  ledgers' streams).
- **Fence identity.** The node id comes from the leader topology's
  `serviceAddress` as in `M/`. Only a single-member topology falls back to
  the answer's `localNode` (native runs dial `localhost` while the node
  advertises `127.0.0.1`); that answer is the leader's, so with several
  members and no address match the fence fails (`unresolved node identity`,
  transient) as `M/checkpoint_metadata.go:126-137` did.
- **Burst job.** Quiet-blocking (its untracked writes would otherwise overlap
  a quiet check, and rules are parked then, so it never delays Q2 by more
  than its 20 s budget), bounded at 20 s, submitted only after a successful
  create, ledger rotated every 16 bursts, 256 bursts per run at most.
- **Limit rejection fixture.** `09-limit-reached` (setup 10 creates, then a
  create) is not shipped: `TestRejectionStages` builds its model with
  `oracle.NewGlobalState()` (no checkpoint limit), so the model accepts the
  11th create and the harness fails before comparing stages. The shape is
  checked in `checkpointcheck/integration_test.go` instead (real server,
  limit 10: same reason, same replay, freeze decision vs. the server's
  answer to a different body under the key). Shipped fixtures:
  `09-delete-id-zero`, `09-delete-next-id`, `09-invalid-cron`.
- **Integration test location.** The real-server test lives in
  `checkpointcheck/integration_test.go` (owned files) rather than in
  `bin/oracle`, and drives a bare `Tracker`.

## Reachability

| Must-hit | Reached by | Needs |
|---|---|---|
| oracle: coverage checkpoint creation matched the oracle | checkpoint_bootstrap, checkpoint_write (create), checkpoint_burst | no faults |
| oracle: coverage checkpoint deletion matched the oracle | checkpoint_write (delete live) | no faults |
| oracle: coverage checkpoint served a frozen model-verified read | checkpoint_read | no faults |
| oracle: coverage deleted checkpoint read returned typed NOT_FOUND | checkpoint_read on a `deleted` target (view `checkpoints.deleted`) | no faults |
| oracle: coverage checkpoint listing matched the oracle | checkpoint_registry | no faults |
| oracle: coverage checkpoint limit returned typed CHECKPOINT_LIMIT_REACHED | checkpoint_write / checkpoint_burst creates once the registry is full (the delete preference starts only at `limit − 1`, so creates still reach the limit) | no faults |
| oracle: coverage checkpoint schedule set matched the oracle | checkpoint_write | no faults |
| oracle: coverage checkpoint schedule deletion matched the oracle | checkpoint_write | no faults |
| oracle: coverage checkpoint schedule read matched the oracle | checkpoint_registry | no faults |
| oracle: coverage a checkpoint served the frozen fleet listing | checkpoint_read ListLedgers arm | no faults |
| oracle: coverage checkpoint info matched the oracle | checkpoint_registry info arm (once a checkpoint exists) | no faults |
| a write burst follows a query checkpoint create | checkpoint_burst job | no faults |

Local runs: hegel's swarm testing draws, at machine creation, a set of rules
disabled for the whole test case, and a driver process runs one test case.
A native run can therefore lose `checkpoint_write` entirely (observed in the
first 180 s run: no deletion or schedule coverage, every create from
`checkpoint_burst`); the deletion, schedule-set and schedule-deletion probes
and the deleted-read probe need `checkpoint_write`, so judge them over
several runs.

## Requests to 99

- `TestRejectionStages` should build its model with
  `WithQueryCheckpointLimit(10)` (the server default) so a
  `CHECKPOINT_LIMIT_REACHED` fixture can be added; until then the shape is
  covered by `checkpointcheck/integration_test.go`.
- `spool replay after gated window applied at least one entry` /
  `spool pruned after resync` are server-side assertions observed by task
  12; their reachability depends on bursts (this task) plus the
  compaction knobs and faults (multi-node only).

---

<a id="10-audit"></a>

## Porting record — 10 audit reads

Task 10 ports the singleton model's audit reads (`M/audit.go` =
`tests/antithesis/workload/bin/cmds/model/singleton_driver_model/audit.go`), the
main driver `MD/parallel_driver_audit` and the audit half of
`MD/parallel_driver_bulk_atomicity` onto two read kinds, `ListAuditEntries` and
`GetAuditEntry`, validated by `W/oraclecheck/auditcheck` and asserted in
`W/bin/oracle/h_audit.go` (one literal call site per id). The Rust rules are
`audit_list` (1.0) and `audit_entry` (0.5) in `HG/driver/src/rules/audit.rs`.

## Foreign tolerance

Every served entry is classified by its idempotency key: **tracked** when it
starts with `o-<run>-k`, **foreign** otherwise (other keys, the empty key,
technical proposals). The singleton's only foreign range was `setupMaxSeq`.

| Check (`M/audit.go`) | Decision | Where |
|---|---|---|
| page contract (`auditPageViolation`: length, cursor legality, ordering, zone-scan density, filter satisfaction) | kept for every entry (structural) | `readkinds.go` → `match.AuditPageViolation` |
| failed entry must match a recorded rejection | tracked failures only; foreign failures skipped. A tracked failure with a reason outside D (retryable) under a key the oracle registered is **left unjudged** (see below) | `validate.go` `checkEntry` |
| success past the committed frontier ("names logs the model never committed") | tracked only | `checkEntry` |
| `auditSuccessMismatch` (exactly one bulk, ranges, ledgers) | tracked only | `checkEntry` |
| order-type scope | tracked only | `checkEntry` |
| "audit trail misses a committed log" (bare log-seq ranges) | tracked logs only: iterates the learned tracked sequences | `ValidatePage` |
| "audit tail misses the newest committed bulk" | **dropped** (not ported): foreign writes can be newer than every tracked bulk. Task 11's quiet-period `oracle: an acknowledged tracked bulk is missing from the audit trail` covers the intent at quiet periods only | – |
| tracked failure with a reason in `oracle.UncommittedReasons` | cannot exist (the server neither audits nor freezes them): reported as `audit failure entry unexplained by the model` | `checkEntry` |
| known-entry lower bound (`auditKnownMatchViolation`) | kept for remembered entries, tracked or foreign (history is permanent); an entry counts only for reads whose ticket is greater than the frontier when it was learned | `knownMatchViolation`, `state.go` |
| batch-key check (`auditIdempotencyViolation`) | kept (tracked keys, via `replayable`) | `BatchKeyViolation` |
| new: foreign entry names a tracked ledger | `oracle: a foreign proposal touched a tracked ledger` | `checkEntry` |
| new: bulk atomicity per key | see "New properties" | `checkEntry`, `ValidatePage`, `DuplicateProposalViolation` |

**Retryable failure entries.** `machine.go` (`:1607-1626`, `:1659-1679`) audits
every FSM failure whose error is not an `UncommittedFailure`, but
`recordIdempotencyFailure` freezes only freezable kinds. A failure with a reason
outside D and outside `UncommittedReasons` (today only an unclassified error,
which reaches the trail as `UNSPECIFIED`, or a future reason; every currently
named retryable FSM reason such as `BALANCE_NOT_PRELOADED` or `STALE_PROPOSAL`
implements `UncommittedFailure` and is never audited) would be audited
unfrozen, the client would see it as non-definitive, and the driver would retry
the key, which may commit. The oracle records only definitive rejections, so
once the key settles such an entry has nothing to match. It is left unjudged
(coverage `oracle: audit failure entry with a retryable reason left unjudged`)
when its key was registered; a reason in `UncommittedReasons` stays a finding.

## Assertions

| Old source | Old assertion (kind: literal) | New | Where |
|---|---|---|---|
| M/audit.go:103 | Reachable: singleton_driver_model: audit sequence bound inside or rejected | oracle: audit sequence bound inside or rejected | bin/oracle/h_audit.go (readkinds.go probeEvent) |
| M/audit.go:105 | Reachable: singleton_driver_model: unspecified audit field rejected | oracle: unspecified audit field rejected | h_audit.go |
| M/audit.go:107 | Reachable: singleton_driver_model: audit condition of the wrong type rejected | oracle: audit condition of the wrong type rejected | h_audit.go |
| M/audit.go:109 | Reachable: singleton_driver_model: parameterised idempotency-key condition rejected | oracle: parameterised idempotency-key condition rejected | h_audit.go |
| M/audit.go:111 | Reachable: singleton_driver_model: idempotency-key prefix containing NUL rejected | oracle: idempotency-key prefix containing NUL rejected | h_audit.go |
| M/audit.go:113 | Reachable: singleton_driver_model: audit not filter rejected | oracle: audit not filter rejected | h_audit.go |
| M/audit.go:119 | Unreachable: singleton_driver_model: ListAuditEntries returned unexpected error | oracle: ListAuditEntries returned unexpected error | h_audit.go |
| M/audit.go:128 | Unreachable: singleton_driver_model: malformed audit cursor returned results | oracle: malformed audit cursor returned results (also on an incomplete page) | h_audit.go |
| M/audit.go:137 | Unreachable: singleton_driver_model: unsupported audit filter returned a page | oracle: unsupported audit filter returned a page (also on an incomplete page) | h_audit.go |
| M/audit.go:160 | Unreachable: singleton_driver_model: audit page violates its own contract | oracle: audit page violates its own contract | h_audit.go |
| M/audit.go:172, :1325 (dynamic `"singleton_driver_model: "+verdict.finding`) | Unreachable: audit failure entry unexplained by the model (finding at :325, :1384) | oracle: audit failure entry unexplained by the model | h_audit.go |
| M/audit.go:172, :1325 | Unreachable: audit entry names logs the model never committed (:333, :1392) | oracle: audit entry names logs the model never committed | h_audit.go |
| M/audit.go:172, :1325 | Unreachable: audit entry outside model (:337, :1396) | oracle: audit entry outside model | h_audit.go |
| M/audit.go:172 | Unreachable: audit entry outside the order-type scope (:345) | oracle: audit entry outside the order-type scope | h_audit.go |
| M/audit.go:172 | Unreachable: audit trail misses a committed log (:363) | oracle: audit trail misses a committed log (tracked logs only) | h_audit.go |
| M/audit.go:172 | Unreachable: audit tail misses the newest committed bulk (:374) | dropped: foreign writes can be newer; intent covered at quiet periods by task 11's `oracle: an acknowledged tracked bulk is missing from the audit trail` | – |
| M/audit.go:172 | Unreachable: audit page omits an entry the trail served before (:431) | oracle: audit page omits an entry the trail served before | h_audit.go |
| M/audit.go:172 | Unreachable: audit page stopped short of an entry the trail served before (:434) | oracle: audit page stopped short of an entry the trail served before | h_audit.go |
| M/audit.go:172 | Unreachable: audit page dropped its resume cursor (:436) | oracle: audit page dropped its resume cursor | h_audit.go |
| M/audit.go:179 | Unreachable: singleton_driver_model: audit entry names the wrong batch key | oracle: audit entry names the wrong batch key | h_audit.go |
| M/audit.go:185 | Reachable: singleton_driver_model: audit entry batch key matched the model | oracle: audit entry batch key matched the model | h_audit.go |
| M/audit.go:191 | Reachable: singleton_driver_model: audit page validated | oracle: audit page validated | h_audit.go |
| M/audit.go:195 | Reachable: singleton_driver_model: audit failure entry matched a model rejection | oracle: audit failure entry matched a model rejection (needs task 02/03; Requests to 99) | h_audit.go |
| M/audit.go:200 | Reachable: singleton_driver_model: audit page judged against remembered entries | oracle: audit page judged against remembered entries | h_audit.go |
| M/audit.go:205 | Reachable: singleton_driver_model: audit resume cursor corroborated by a remembered entry | oracle: audit resume cursor corroborated by a remembered entry | h_audit.go |
| M/audit.go:1294 | Reachable: singleton_driver_model: audit entry read on an unassigned sequence returned NotFound | oracle: audit entry read on an unassigned sequence returned NotFound | h_audit.go |
| M/audit.go:1299 | Unreachable: singleton_driver_model: GetAuditEntry returned unexpected error | oracle: GetAuditEntry returned unexpected error | h_audit.go |
| M/audit.go:1309 | Unreachable: singleton_driver_model: audit entry served an unassigned sequence | oracle: audit entry served an unassigned sequence | h_audit.go |
| M/audit.go:1316 | Unreachable: singleton_driver_model: audit entry read served a different sequence | oracle: audit entry read served a different sequence | h_audit.go |
| M/audit.go:1337 | Reachable: singleton_driver_model: audit entry read validated | oracle: audit entry read validated | h_audit.go |
| M/audit.go:1341 | Reachable: singleton_driver_model: audit entry carried its per-order items | oracle: audit entry carried its per-order items | h_audit.go |
| M/cursors.go:133 | Reachable: singleton_driver_model: malformed audit cursor rejected | oracle: malformed audit cursor rejected | h_audit.go |
| M/cursors.go:139 | Unreachable: singleton_driver_model: malformed cursor returned unexpected error (audit kind) | mapped: a malformed audit cursor answered with anything but InvalidArgument is `oracle: ListAuditEntries returned unexpected error` (details carry `malformed: true`); the transaction/log kinds belong to task 06 | h_audit.go |
| MD/parallel_driver_audit/main.go:41 | Sometimes: should be able to create tx for audit trail | dropped: the setup write is not this task's; tracked writes come from other rules (here `smoke_tx`) and their outcomes are the oracle's (`oracle: model outcome verified`, 00a) | – |
| MD/parallel_driver_audit/main.go:50 | Unreachable: Apply succeeded but returned no committed log | mapped: `oracle: successful bulk returned no committed log` (00a), which judges every tracked write | (00a) |
| MD/parallel_driver_audit/main.go:63 | Unreachable: ListAuditEntries returned unexpected error | mapped: `oracle: ListAuditEntries returned unexpected error` (transient outcomes are the service's skip class) | h_audit.go |
| MD/parallel_driver_audit/main.go:80 | Unreachable: ListAuditEntries stream returned unexpected error | mapped: the client reports a mid-stream error as the call's error outcome, judged as `oracle: ListAuditEntries returned unexpected error` | h_audit.go |
| MD/parallel_driver_audit/main.go:95 | AlwaysOrUnreachable: audit trail should contain entries | verbatim, on a successful complete unfiltered first page (setup wrote entries) | rules/audit.rs `check_trail_has_entries` |
| MD/parallel_driver_bulk_atomicity/main.go:155 | Sometimes: mid-failing atomic bulk definitively rejected | task 02 (verbatim there) | – |
| MD/parallel_driver_bulk_atomicity/main.go:277 | Always: failed atomic bulk yields no success audit entry beyond ledger creation | mapped (per key, not per ledger): `oracle: a rejected keyed bulk has no success audit entry` | h_audit.go |
| MD/parallel_driver_bulk_atomicity/main.go:281 | Always: audit failure entries are unique per proposal | mapped: `oracle: audit failure entries are unique per proposal` (keys rejected for good; page plus remembered entries) | h_audit.go |
| MD/parallel_driver_bulk_atomicity/main.go:285 | Always: failure audit entry observed for failed atomic bulk | mapped: `oracle: a rejected keyed bulk has a failure audit entry` (whole first page of an exact key filter, read after the rejection was observed) | h_audit.go |
| MD/parallel_driver_bulk_atomicity/main.go:295 | Always: failed atomic bulk leaves no partial transaction effects | not this task: the oracle's failure explanation plus task 11's quiet exact state (task 02 "no partial effects") | – |
| MD/parallel_driver_bulk_atomicity/main.go:302 | Always: failed atomic bulk leaves no partial account activity | not this task: as above (task 02 / task 11) | – |

## New properties

| Literal | Kind | Condition |
|---|---|---|
| `oracle: a foreign proposal touched a tracked ledger` | Unreachable | a foreign entry (key without the tracked prefix, including the empty key) names an `o-<run>-` ledger |
| `oracle: a rejected keyed bulk has no success audit entry` | Unreachable | a success entry under a key whose only write (`WritesByKey` holds one record, no conflict) ended definitive-frozen at the FSM stage |
| `oracle: audit failure entries are unique per proposal` | Unreachable | two failure entries (page or remembered) under such a key share a proposal id |
| `oracle: a rejected keyed bulk has a failure audit entry` | Unreachable | filter is exactly one hardcoded `AUDIT_FIELD_IDEMPOTENCY_KEY` leaf, first page (`AfterSeq == 0`), shorter than its page size, the key is rejected for good as above, and its `ObserveTicket` is below the read's ticket; the page holds no failure entry under the key |
| `oracle: audit failure entry with a retryable reason left unjudged` | Reachable (needs faults; Requests to 99) | a tracked failure entry with a reason outside D under a registered key was skipped |
| `oracle: rejected keyed bulk checked for its failure audit entry` | Reachable (needs task 02/03; Requests to 99) | the previous Unreachable's preconditions held on a page |

## Reachability

| Must-hit | Reached by | Needs |
|---|---|---|
| oracle: audit page validated | audit_list | no faults |
| oracle: audit entry read validated | audit_entry | no faults |
| oracle: audit entry carried its per-order items | audit_entry on a sample of a setup or `smoke_tx` entry | no faults |
| oracle: audit entry read on an unassigned sequence returned NotFound | audit_entry (1/4 `u64::MAX`) | no faults |
| oracle: audit entry batch key matched the model | audit_list over `smoke_tx` commits (replayable keys) | no faults |
| oracle: audit page judged against remembered entries | audit_list after an earlier page | no faults |
| oracle: audit resume cursor corroborated by a remembered entry | audit_list full page short of a remembered entry | no faults |
| oracle: audit sequence bound inside or rejected | audit_list (Or with a seq leaf) | no faults |
| oracle: unspecified audit field rejected | audit_list refused probe | no faults |
| oracle: audit condition of the wrong type rejected | audit_list refused probe | no faults |
| oracle: parameterised idempotency-key condition rejected | audit_list refused probe | no faults |
| oracle: idempotency-key prefix containing NUL rejected | audit_list refused probe | no faults |
| oracle: audit not filter rejected | audit_list Not probe | no faults |
| oracle: malformed audit cursor rejected | audit_list (1/16 malformed cursor) | no faults |
| oracle: audit failure entry matched a model rejection | audit_list over a tracked FSM rejection | needs task 02/03 (`smoke_tx` never fails) |
| oracle: rejected keyed bulk checked for its failure audit entry | audit_list key mode (1/8) on a frozen-rejected key | needs task 02/03 |
| oracle: audit failure entry with a retryable reason left unjudged | an audited unfrozen tracked failure | faults (and a server path that audits a retryable reason; may be unreachable on the current server) |

## Requests to 99

- Exclude from task 10's acceptance and check in the integrated build:
  `oracle: audit failure entry matched a model rejection` and
  `oracle: rejected keyed bulk checked for its failure audit entry` (need the
  tracked FSM rejections of tasks 02/03).
- `oracle: audit failure entry with a retryable reason left unjudged` needs
  faults and a server path that audits a failure whose reason is outside D and
  outside `UncommittedReasons`. On the current server every named retryable FSM
  reason implements `UncommittedFailure` and is not audited, so this may never
  fire; 99 decides whether to keep it as a must-hit or downgrade it to a
  non-must-hit (it guards against a false alarm, not a server property).
- The rejection fixtures are task 02/03's: task 10 generates no writes.

---

<a id="11-quiet-exact"></a>

## 11 — Quiet-period exact checks

The `exact` quiet-period flavour (DESIGN §4.10): once the oracle is quiescent, the driver
feeds it the audit trail (incremental replay in the server's total order), every live
tracked ledger's full state read linearizably, and the state of hegel-drawn nodes and
ledgers read stale after the node reached the barrier horizon. The oracle judges all of it
against `Model()`.

Files: `W/oraclecheck/quietcheck/` (sections `audit` and `ledger`, registered in `init()`),
`W/bin/oracle/h_quiet.go` (asserters), `H/rules/quiet_exact.rs` (the `QuietHook`),
`HG/protocol-fixtures/11/`.

## Porting table

`H/main.rs` is the driver before 00c (`git show 6c1aaa7f9:tests/antithesis/hegel/driver/src/main.rs`).

| Old source | Old assertion (kind: literal) | New | Where |
|---|---|---|---|
| H/main.rs:1058 | Sometimes: the full state check completes in a quiet period | verbatim (true when audit, linearizable and per-node phases all completed) | rules/quiet_exact.rs `run` |
| H/main.rs:1104 | Always: a listing has no duplicate transaction ids | oracle (`oracle: quiet transactions differ from the model`, "served twice") | quietcheck/ledger.go `txsDiffer` |
| H/main.rs:1116 | Always: every revert transaction was requested by the workload | oracle (`oracle: quiet transactions differ from the model`: every served tx is the model's record) | quietcheck/ledger.go `txsDiffer` |
| H/main.rs:1121 | Always: a reference maps to at most one committed transaction | oracle (exact quiet transactions; the model enforces reference uniqueness) | quietcheck/ledger.go `txsDiffer` |
| H/main.rs:1123 | Unreachable: every committed transaction was sent by the workload | oracle (`oracle: quiet transactions differ from the model`; `oracle: audit replay found an entry no tracked bulk explains`; `oracle: audit replay met a foreign proposal on a tracked ledger`) | quietcheck |
| H/main.rs:1127 | Always: a definitively rejected write never takes effect | oracle (`oracle: audit replay found an entry no tracked bulk explains`, `oracle: audit replay state differs from the online model`, exact state) | quietcheck/audit.go |
| H/main.rs:1130 | Always: a committed transaction has the postings that were sent | oracle (`oracle: quiet transactions differ from the model`, `TxRecordMatches`) | quietcheck/ledger.go |
| H/main.rs:1134 | Always: a committed transaction keeps its acknowledged id | oracle (online commit check, 00a; exact transactions) | quietcheck/ledger.go |
| H/main.rs:1137 | Sometimes: an ambiguous write turns out to have committed | dropped: the Rust model's "unknown" status is gone; writes retry until definitive (00c's `a tracked write needs more than one attempt and then commits`) | – |
| H/main.rs:1145 | Always: an acknowledged revert marks its transaction reverted | oracle (`oracle: quiet transactions differ from the model`: reverted, revertedBy, revertedAt) | quietcheck/ledger.go |
| H/main.rs:1149 | Always: a transaction is reverted at most once | oracle (model rejects a second revert; exact transactions) | quietcheck/ledger.go |
| H/main.rs:1153 | Always: an acknowledged transaction survives faults | oracle (`oracle: quiet transactions differ from the model` count/ids; `oracle: an acknowledged tracked bulk is missing from the audit trail`) | quietcheck |
| H/main.rs:1156 | Always: an acknowledged revert survives faults | oracle (same two) | quietcheck |
| H/main.rs:1185 | Always: an account's balance equals the sum of its committed postings | oracle (`oracle: quiet accounts differ from the model`, every cell's input/output) | quietcheck/ledger.go `accountsDiffer` |
| H/main.rs:1188 | Always: an account goes negative only through forced writes | oracle (the model rejects non-forced overdrafts; exact accounts) | quietcheck/ledger.go |
| H/main.rs:1199 | Always: ledger stats count every committed transaction | oracle (`oracle: quiet ledger stats boundaries differ from the model`, exact `transactionCount`/`logCount`) | quietcheck/ledger.go `statsDiffer` |
| H/main.rs:1200 | Always: a ledger has at least one log per transaction | oracle (same: exact boundaries) | quietcheck/ledger.go `statsDiffer` |
| H/main.rs:1223 | Sometimes: every node catches up after faults stop | verbatim (per drawn node; `wait_at_horizon` asks the node itself, DESIGN §6.10) | rules/quiet_exact.rs `node_phase` |
| H/main.rs:1243 | Always: every node holds the same account state at a common index | oracle, strengthened and renamed: each node's stale logs, transactions, accounts and ledger info equal the model at the horizon (`oracle: a node's … differs from the model at the horizon`) | quietcheck/ledger.go |
| MD/parallel_driver_timestamp_order/main.go:223 | Always: effective timestamps are non-decreasing with log sequence | new sibling, not a re-assertion: `oracle: log dates decrease along the global sequence` (log dates across every tracked ledger) | quietcheck/ledger.go `globalStream` |

## New properties

Unreachable (findings):

- `oracle: audit replay found an entry no tracked bulk explains`
- `oracle: audit replay met a foreign proposal on a tracked ledger`
- `oracle: an acknowledged tracked bulk is missing from the audit trail`
- `oracle: a definitively rejected tracked bulk is missing from the audit trail`
- `oracle: an admission rejection reached the audit trail`
- `oracle: an uncommitted failure reached the audit trail`
- `oracle: audit replay state differs from the online model`
- `oracle: the audit trail listing skips a sequence` (not in the spec's list: the spec requires
  the entries to be "contiguous from cursor+1", and the FSM numbers them without gaps
  (`fsmstate.go` `AppendAuditEntry`), so a gap is a listing defect that needs its own literal)
- `oracle: quiet log stream differs from the model`, `oracle: quiet transactions differ from the model`,
  `oracle: quiet accounts differ from the model`, `oracle: quiet ledger stats boundaries differ from the model`,
  `oracle: quiet ledger info differs from the model`
- `oracle: a node's log stream differs from the model at the horizon`, `… transactions …`, `… accounts …`, `… ledger info …`
- `oracle: quiet global log stream differs from the learned sequences`
- `oracle: log dates decrease along the global sequence`
- `oracle: quiet ledger stats usage exceeds the model` (not in the spec's list: the spec's usage rule
  "≤ the model" needs a literal for the violated case, and reusing the boundaries literal would
  give two properties one message)

Must-hit:

- `oracle: quiet audit replay verified` (Reachable)
- `oracle: quiet exact state verified` (Reachable)
- `oracle: a node's state matched the model at the horizon` (Reachable)
- `oracle: quiet ledger stats usage caught up with the model` (Sometimes)
- Rust: `exact completes within its quiet budget` (Sometimes), and the verbatim
  `the full state check completes in a quiet period` and `every node catches up after faults stop`.

## Reachability

| Assertion | Reached by | Needs |
|---|---|---|
| `oracle: quiet audit replay verified` | `exact` hook, audit phase to the end of the trail | no faults |
| `oracle: quiet exact state verified` | `exact` hook, linearizable phase, any live tracked ledger | no faults |
| `oracle: a node's state matched the model at the horizon` | `exact` hook, per-node phase | no faults |
| `oracle: quiet ledger stats usage caught up with the model` | `exact` hook, stats part (true once the usage store folded everything; false while it lags) | no faults (true); the false side needs load |
| `the full state check completes in a quiet period` | `exact` hook | no faults |
| `exact completes within its quiet budget` | `exact` hook (true on completion) | no faults (true); false needs a large state or slow nodes |
| `every node catches up after faults stop` | `exact` hook, per-node phase | no faults (true) |

## Design notes and deviations

- **Non-freezable audit failures.** The spec accepts a non-freezable failure entry (e.g. the
  FSM maintenance gate) without deciding the bulk. An audit failure carries its reason only
  through a `FailureDetails` arm, and every arm's reason is in D (definitive, freezable)
  — `TestAuditReplay_UncommittedReasonsHaveNoAuditArm` pins it — so such an entry cannot be
  encoded on the wire. The acceptance path is kept (it accepts a failure whose reason is not in
  D for any tracked key with records) but has no unit test with a real entry.
- **Uncommitted entries.** Likewise `FILTER_COMPILATION_ERROR`, `CLUSTER_POLICY_INVALID` and
  `STALE_CLUSTER_POLICY` have no `FailureDetails` arm, so `oracle: an uncommitted failure
  reached the audit trail` is a defence only; the mutation test the spec lists is replaced by
  the representability test above, which fails the day an arm appears.
- **Conflicts.** Conflict bodies under one key cannot be told apart in the trail: the replay
  counts `IDEMPOTENCY_KEY_CONFLICT` entries per key, and each conflict record owed an entry
  consumes one at completeness time. Once every conflict record of a key has settled, the
  key's surplus (extra attempts) is dropped, so it cannot cover a later conflict on the same
  key whose entry is missing.
- **Completeness bookkeeping.** Records are classified by scanning tickets
  (`RecordByTicket`) from the last scanned one; each owed record is checked once, at the
  first complete replay after it ended, and reported at most once. A record at or below the
  scanned ticket is settled and counts as decided, so the per-wid match table is pruned at each
  completion and holds only the current interval.
- **Response release.** At completion every held response whose record settled — matched to
  an entry *or* reported missing — is released (a prefix of the held list), so one missing
  entry cannot pin every later response until the tracker's cap forces a resync.
- **Per-period state** is reset when the driver's epoch number *or* the tracker's
  `QuietBegins` count changes (a reused epoch number still starts afresh). The wid → range
  index of the committed bulks is built once per quiet period, not per chunk.
- **After a fingerprint mismatch** the replay re-seeds from `Model()`, so the next quiet period
  checks the next interval instead of repeating one finding forever.
- **Frozen keys in the diff.** `GlobalState` exposes no accessor for the idempotency table,
  so the per-ledger diff of `oracle: audit replay state differs from the online model` names
  frozen keys only as the residual explanation when nothing else differs.
- **Purged zero cells: no tolerance (deviation from the spec text).** The spec allows purged
  zero cells to be absent. The FSM purges only the cells a bulk's volume updates touched
  (`write_set_ephemeral_purge.go` classifies `update`s), and the model purges exactly those
  (`purgeZeroBalance` over the bulk's touched cells), so a purged cell is already absent from
  the model. The exact state therefore uses `AccountMatches` and the listed set must equal
  `AccountUniverse`, like the old validators. A tolerance would hide a server dropping a zero
  cell it must keep, e.g. under a pattern that became ephemeral after the cell was drained
  (`TestExactState_PurgedZeroCellsExact`).
- **Per-node stats.** No per-node stats literal exists, so the node scope posts logs,
  transactions, accounts and info only; a node-scope stats part is refused (`protocol`).
- **Global stream** runs once per epoch, after every live tracked ledger's linearizable log
  listing matched the model; it checks served sequences against the learned global stream
  (`Model().Logs()`), per-ledger monotonicity, committed-bulk contiguity and log dates along
  the sequence. Contiguity: every sequence of a bulk's `[MinSeq, MaxSeq]` is a tracked log
  (`CommittedLogs`), and the bulk ranges sorted by `MinSeq` do not overlap; together they mean
  a range holds only its own bulk's logs (two interleaved tracked bulks overlap). The
  foundation's tables carry no per-bulk log count, so the `MaxSeq-MinSeq+1 == count` form is
  expressed as this equivalent pair.
- **Resync** test: the tracker's response cap (100 000) cannot be reached in a unit test, so
  the side table has a test-only `forceResync` flag (`resync_internal_test.go`). The resync
  logs the skipped interval in the oracle's log and returns it as `next.skipped`
  (`{"from","to"}`, the interval `(from, to]`).
- **Audit chunks** must continue the cursor: the first entry after the cursor is `cursor+1`
  and each next one `prev+1`, within and across chunks; a gap is reported once per chunk.
- **Real server test.** `TestQuietIntegration_RealServer` (`bin/oracle/h_quiet_test.go`,
  in-process one-node server, skipped with `-short`) runs two quiet periods of real writes —
  ledger creates (one multi-create bulk), single- and multi-ledger commits, a chart order, FSM
  failures with a tracked and an untracked key, conflicts of one and two attempts, a
  cluster-scoped checkpoint create (empty `ledgers`), a ledger delete — pages the real
  `ListAuditEntries` with the driver's filter into the audit section and the real ledger reads
  into the ledger section, and requires both verdicts with no finding.
- **Ledger reads** are paged by cursor with `pageSize` 500; the audit listing pages with the
  filter `AUDIT_FIELD_SEQUENCE ≥ cursor+1` and the cursor. The hook stops paging when the oracle
  answers `next.resync: true`, and treats a page the client truncated at `max_items`
  (`complete:false`) as a failed phase rather than the last page.

## Requests to 99

- None of the must-hits need another family. A run with `RULES=smoke_tx,smoke_get_account`
  reaches all of them (the native acceptance run of this task).
- Acceptance run (300 s, `QUIET_PROBABILITY=0.05 RULES=smoke_tx,smoke_get_account`): 77 quiet
  periods, every one with the audit, linearizable and per-node phases complete and no finding;
  every must-hit above was hit. The shared build host's disk was above the ledger's default
  disk-full write gate, so the run exported `HEALTH_WAL_THRESHOLD=0.999 HEALTH_DATA_THRESHOLD=0.999`
  (inherited by the native ledger process); nothing in `run_native.sh` changed.

---

<a id="12-cluster-ops-signals"></a>

## 12 — Cluster operations and server signals (Rust-only)

Rules in `HG/driver/src/rules/cluster_ops.rs`, log hooks in `rules/server_signals.rs`. No
oracle involvement: these RPCs never change tracked business state (oracle.md §1.5 item 8).
Sink and backup requests and the post-transfer witness write are untracked
(`send_untracked`, keys `r-<run>-k<N>`); the witness writes go to `r-ops-<run>`, created by
the setup hook `ops_setup`.

Error classes used below (`cluster_ops.rs`):

- **transient**: no answer (`unknown`), a client-local dial failure (`local`, message
  `dial:`), a remote `Unavailable`, `DeadlineExceeded` or `Canceled`, reason
  `EXTERNAL_SERVICE_ERROR`, or `ResourceExhausted` + `WRITES_BLOCKED_DISK_FULL` (the old
  `IsTolerated` set). Other client-local errors (request/response marshalling, the local
  message-size limit) are driver or client bugs and never transient.
- **backup transient**: transient without a remote `Canceled` (the backup drivers tolerated
  `Canceled` only as caller cancellation, `backup.go:36-45`; the driver's own deadline shows
  as no answer or `DeadlineExceeded`).
- **busy**: `FailedPrecondition` whose message ends with
  `backup: destination already has a running job` (exact suffix, `IsBackupInProgress`).
- **no full checkpoint**: `FailedPrecondition` whose message contains `full checkpoint`.
- **classified**: ok, transient, or `NotFound`/`AlreadyExists`/`InvalidArgument`/`FailedPrecondition`
  (the old `IsClassified`).

## Old assertions

| Old source | Old assertion (kind: literal) | New | Where |
|---|---|---|---|
| H/main.rs:829 | Always: a barrier's commit index is positive | verbatim | cluster_ops.rs `barrier_monotonic` |
| H/main.rs:830 | Always: the barrier commit index never goes back | verbatim (floor = driver-global high-water of completed barriers, read before the call) | `barrier_monotonic` |
| MD/parallel_driver_barrier_monotonic:104 | AlwaysGreaterThan: barrier commit index is positive on success | mapped to `a barrier's commit index is positive` (H literal kept, DESIGN §9.1) | `barrier_monotonic` |
| MD/parallel_driver_barrier_monotonic:107 | AlwaysGreaterThanOrEqualTo: barrier commit index is monotonic within a session | mapped to `the barrier commit index never goes back` (one driver-global session) | `barrier_monotonic` |
| MD/parallel_driver_barrier_monotonic:112 | AlwaysGreaterThanOrEqualTo: barrier commit index never regresses below the cross-invocation high-water mark | mapped to `the barrier commit index never goes back` | `barrier_monotonic` |
| MD/parallel_driver_barrier_monotonic:116 | Sometimes: barrier succeeded after an observed leadership change | verbatim; the change is a leadership transition seen in `NodeStates` (log-derived) since the previous successful barrier, instead of a `GetClusterState.leader` sample | `barrier_monotonic` |
| MD/parallel_driver_barrier_monotonic:87 | Unreachable: barrier monotonic driver got non-transient error | verbatim (non-transient = not in the transient class above) | `barrier_monotonic` |
| H/main.rs:916 | Sometimes: leadership is transferred on request | renamed to the MD literal `should be able to transfer leadership` | `transfer_leadership` |
| MD/parallel_driver_transfer_leadership:39 | Unreachable: GetClusterState should not fail | verbatim | `transfer_leadership` |
| MD/parallel_driver_transfer_leadership:94 | Sometimes: should be able to transfer leadership | verbatim (each transfer of a burst) | `transfer_leadership` |
| MD/parallel_driver_transfer_leadership:100 | AlwaysOrUnreachable: new leader should be the requested target | verbatim | `transfer_leadership` |
| MD/parallel_driver_transfer_leadership | Reachable: leadership transfer completed | verbatim | `transfer_leadership` |
| MD/parallel_driver_transfer_leadership:113 | Sometimes: business write commits after leadership transfer | verbatim; one keyed untracked write to `r-ops-<run>` | `transfer_leadership` |
| MD/parallel_driver_transfer_leadership:131 | Sometimes: cluster should have a leader after transfer | verbatim | `transfer_leadership` |
| MD/parallel_driver_transfer_leadership (`Sentinel.Verify` ×2) | sentinel read-back after transfer | dropped: the witness is untracked and Rust-checked only for commit; acknowledged-write survival is the oracle's job (tracked ledgers, task 11) | – |
| H/main.rs:920 | Sometimes: the primary store is compacted | renamed to `primary compaction should succeed or be transiently unavailable` (MD literal) | `compaction` |
| H/main.rs:924 | Sometimes: the secondary store is compacted | renamed to `secondary compaction should succeed or be transiently unavailable` | `compaction` |
| MD/parallel_driver_compaction:51 | Sometimes: primary compaction should succeed or be transiently unavailable | verbatim | `compaction` |
| MD/parallel_driver_compaction:74 | Sometimes: secondary compaction should succeed or be transiently unavailable | verbatim | `compaction` |
| MD/parallel_driver_compaction | Reachable: primary compaction completed / secondary compaction completed | verbatim | `compaction` |
| MD/parallel_driver_compaction:107 | Unreachable: GetLedgerStats should not fail after compaction | verbatim (on `default`) | `compaction` |
| MD/parallel_driver_compaction:118 | AlwaysOrUnreachable: world account must be readable after compaction | verbatim (`classified`, on `default`) | `compaction` |
| H/main.rs:928 | Sometimes: a checkpoint is created | verbatim | `cluster_checkpoint` |
| H/main.rs:934 | Always: a successful backup contains files | renamed to the MD literal `backup should produce files` (AlwaysOrUnreachable) | backup job |
| H/main.rs:936 | Sometimes: a backup completes | renamed to the MD literal `backup completed successfully` (Reachable) | backup job |
| MD/singleton_driver_backup:52 | Unreachable: Backup returned unexpected error | verbatim (not busy, not backup-transient), full-only chains | backup job `run_chain` |
| MD/singleton_driver_backup:63 | AlwaysOrUnreachable: backup should produce files | verbatim | `run_chain` |
| MD/singleton_driver_backup:65 | Reachable: backup completed successfully | verbatim | `run_chain` |
| W/internal/backup.go:81 | Sometimes: backup recovery after destination busy succeeds | verbatim; evaluated on every attempt after the first busy answer (as the old `RetryBackup`), retry ≤ 2 min from the first busy, drawn backoffs 500–5000 ms | `backup_call` |
| MD/singleton_driver_incremental_backup | Unreachable: Backup (pre-incremental) returned unexpected error | verbatim: the full backup of a chain that goes on with incrementals | `run_chain` |
| MD/singleton_driver_incremental_backup | Reachable: incremental backup completed | verbatim (the first incremental of a chain) | `run_chain` |
| MD/singleton_driver_incremental_backup | Unreachable: IncrementalBackup returned unexpected error | verbatim (busy, backup-transient, no full checkpoint tolerated) | `run_chain` |
| MD/singleton_driver_incremental_backup | Unreachable: second IncrementalBackup returned unexpected error | verbatim: an incremental chain runs full, incremental, incremental, as the old driver | `run_chain` |
| MD/singleton_driver_incremental_backup:130 | AlwaysOrUnreachable: incremental backup log sequence should not regress | verbatim: the second incremental against the first of the same chain (old shape). Not compared when a full backup of this driver (another chain of the same job) was in flight before the first, or started or ended before the second returned: a full backup restarts the export chain from its checkpoint (`manifest.go:100-114`), and the server only keeps one export chain monotonic (`manager.go:513-518`) | `run_chain` |
| MD/singleton_driver_incremental_backup:137 | AlwaysOrUnreachable: incremental backup audit sequence should not regress | verbatim, same scheme | `run_chain` |
| H/main.rs:1022 | Always: the store integrity check finds no error | renamed to the MD literal `CheckStore found integrity error` (AlwaysOrUnreachable) | check_store job |
| H/main.rs:1023 | Sometimes: a store integrity check completes | renamed to `CheckStore completed successfully` (Reachable) | check_store job |
| MD/parallel_driver_check_store | Unreachable: CheckStore returned unexpected error | verbatim | `run_check_store` |
| MD/parallel_driver_check_store | Unreachable: CheckStore stream error | merged into `CheckStore returned unexpected error`: the client returns one outcome for the whole stream, so an open error and a mid-stream error are indistinguishable | `run_check_store` |
| MD/parallel_driver_check_store | AlwaysOrUnreachable: CheckStore found integrity error | verbatim (one evaluation per stream: no `error` event; first five errors in the details) | `run_check_store` |
| MD/parallel_driver_check_store | Reachable: CheckStore completed successfully | verbatim (only when the stream completed within `maxItems` 100 000) | `run_check_store` |
| H/main.rs:966 | Always: an added events sink is listed | renamed to the MD literal `added sink should appear in GetEventsSinks`, now an exact `sinks[].name` compare instead of `to_string().contains` | `event_sinks` |
| H/main.rs:969 | Sometimes: an events sink is removed | renamed to `should be able to remove event sink` | `event_sinks` |
| MD/parallel_driver_event_sinks | Sometimes: should be able to add NATS event sink | verbatim (cond: ok or transient, as the old `IsTolerated`) | `event_sinks` |
| MD/parallel_driver_event_sinks:87 | AlwaysOrUnreachable: added sink should appear in GetEventsSinks | verbatim, exact name, linearizable read | `event_sinks` |
| MD/parallel_driver_event_sinks | Sometimes: should be able to remove event sink | verbatim | `event_sinks` |
| MD/parallel_driver_event_sinks:143 | AlwaysOrUnreachable: removed sink should not appear in GetEventsSinks | verbatim | `event_sinks` |
| MD/parallel_driver_event_sinks | Unreachable: Hegel driver generation failed | dropped: no Go-side generation any more | – |
| MD/parallel_driver_event_sinks (step 3) | best-effort transaction to generate events | dropped: other rules write continuously | – |

## New properties

| Kind | Literal | Where |
|---|---|---|
| Always | a full backup's applied index covers every barrier completed before it started | `run_chain`: `lastAppliedIndex ≥` the highest barrier commit index (`BarrierState.high`) recorded in the rule. Backup applies its Start order (`orchestrator.go:274`) before it takes the checkpoint, and a proposal resolves only after its Pebble commit (`applier.go` `submitAsyncCommit`), so the checkpoint is past every barrier that completed earlier. Replaces the spec's `lastLogSequence ≥ Shared.acked_seq_high_water` (see Deviations) |
| Sometimes | a follower reports an out-of-sync or snapshotting status | `cluster_state` |
| Sometimes | the leader reports a follower in snapshot progress | `cluster_state` |
| Reachable | the driver parses a structured server log record | server_signals.rs |
| Reachable | the driver receives ledger server logs | server_signals.rs |
| Reachable | the driver receives client logs while the workload runs | server_signals.rs (renamed from the spec's `the driver receives client logs`, which 00c already uses ungated in logs.rs; DESIGN §9.2 item 6) |
| Reachable | the driver receives restore-target logs | server_signals.rs |
| Reachable | a node takes a WAL snapshot | server_signals.rs (`Snapshot created`) |
| Reachable | a follower receives a raft snapshot | (`Applying snapshot sent by leader`) |
| Reachable | a follower restores a checkpoint from the leader | (`Checkpoint restored successfully`) |
| Reachable | a gated window completes with spool replay | (`Unspooling operation terminated, resuming...`, debug) |
| Reachable | a node creates a main store query checkpoint | (`Created main store query checkpoint`, debug) |
| Reachable | a cluster config change resets the cache epoch | (`Applying cluster config change: resetting cache and purging 0xFF`) |
| Reachable | a bloom config change triggers a rebuild | (`Bloom filter config changed: purging blocks and rebuilding`) |
| Reachable | a bloom populate races a rebuild | (`Bloom task skipped SetReady: Rebuild occurred`) |
| Unreachable | a server logs an invariant red flag | server_signals.rs: records whose `msg` starts with `SENTINEL DIAG:`, `CACHE COHERENCE:`, `POST-COMMIT VOLUME ASSERTION FAILED`, `AGGREGATED VOLUME BALANCE CHECK FAILED`, `PERSISTED VOLUME UPDATE at invariant violation`, `TRANSIENT VOLUME at invariant violation`, `Preload boundary mismatch`, `preload coverage miss: read of undeclared key`, `Pebble lags WAL snapshot`, `Leadership gained Ready has no entries`; ledger and restore-target lines, from boot. `Terminal Raft command failure; stopping node` is printed only (see Deviations) |

Log signals are counted per node (`SignalState`, first occurrence printed as
`driver: signal <name> first seen on <node>`), active or not; Reachables only while the
workload is active. Signal Reachables count only `ledger-*` lines (the main cluster's knobs).

## Reachability (must-hit assertions)

| Assertion | Rule / path | Needs |
|---|---|---|
| a barrier's commit index is positive | barrier_monotonic | no faults |
| the barrier commit index never goes back | barrier_monotonic | no faults |
| barrier succeeded after an observed leadership change | barrier_monotonic after transfer_leadership or a leader kill | no faults (≥ 2 voters, via transfer_leadership); faults otherwise |
| should be able to transfer leadership | transfer_leadership | no faults (≥ 2 voters; not in `run_native.sh`) |
| leadership transfer completed | transfer_leadership | no faults (≥ 2 voters) |
| business write commits after leadership transfer | transfer_leadership | no faults (≥ 2 voters) |
| cluster should have a leader after transfer | transfer_leadership | no faults (≥ 2 voters) |
| primary / secondary compaction should succeed or be transiently unavailable | compaction | no faults |
| primary compaction completed, secondary compaction completed | compaction | no faults |
| a checkpoint is created | cluster_checkpoint | no faults |
| backup completed successfully | backup job | no faults (needs minio) |
| a full backup's applied index covers every barrier completed before it started | backup job | no faults (needs minio) |
| backup recovery after destination busy succeeds | backup job, two concurrent chains (1/6) | no faults (needs minio) |
| incremental backup completed | backup job (1/3 of chains) | no faults (needs minio) |
| incremental backup log / audit sequence should not regress | backup job (1/3 of chains) | no faults (needs minio) |
| CheckStore completed successfully | check_store job | no faults |
| should be able to add NATS event sink, should be able to remove event sink | event_sinks | no faults |
| a follower reports an out-of-sync or snapshotting status | cluster_state | faults + knobs |
| the leader reports a follower in snapshot progress | cluster_state | faults + knobs |
| the driver parses a structured server log record | log hook | no faults |
| the driver receives ledger server logs | log hook | no faults |
| the driver receives client logs while the workload runs | log hook | no faults |
| the driver receives restore-target logs | log hook | needs task 18 |
| a node takes a WAL snapshot | log hook | knobs (`MAINTENANCE_INTERVAL`, `RAFT_COMPACTION_MARGIN`); faults make it likelier |
| a follower receives a raft snapshot | log hook | faults + knobs |
| a follower restores a checkpoint from the leader | log hook | faults + knobs |
| a gated window completes with spool replay | log hook | faults + knobs (`LOG_LEVEL=debug`) |
| a node creates a main store query checkpoint | log hook | knobs (`LOG_LEVEL=debug`) + task 09's `createQueryCheckpoint` orders |
| a cluster config change resets the cache epoch | log hook | knobs (per-node `CACHE_ROTATION_THRESHOLD`) + a leadership change (transfer_leadership or faults) |
| a bloom config change triggers a rebuild | log hook | knobs (per-node `BLOOM_VOLUMES_EXPECTED_KEYS`) + a leadership change |
| a bloom populate races a rebuild | log hook | faults + knobs (transfer bursts help) |

`run_native.sh` runs one node: transfers, snapshots, sync windows and config changes cannot
happen there, and minio/NATS are absent, so backups and sink adds fail there.

## Deviations from the spec

- **Full-backup coverage Always.** The spec asks for `lastLogSequence ≥
  Shared.acked_seq_high_water`. That floor is not sound: the server's own comment
  (`manager.go:513-518`) says a node's history-zone log sequence can be lower than one
  already recorded after a checkpoint restore, and `Clients::note_apply` raises the
  high-water on every successful Apply response, including late UDP events and (once task
  18 lands) other clusters. The new literal compares the checkpoint's `lastAppliedIndex`
  with the barrier high-water instead, which holds by construction.
- **Incremental non-regression.** The spec compares each incremental with the previous
  successful incremental of the process (driver-global). Every chain starts with a full
  backup that restarts the export chain, so that floor is stronger than the old literals
  and can fail on a correct server (COMMON: never strengthen an old check). Chains with
  incrementals run full + two incrementals, as the old driver did.
- **Red flag list.** `Terminal Raft command failure; stopping node` has one source, a failed
  conf-state persist after a force-remove (`node.go:3558`): an I/O error, so a correct
  fail-stop under disk faults. It is printed (`driver: <node> stopped on a terminal raft
  command failure`) but not asserted.
- **Time bounds.** Each rule body has one 10 s deadline (`Clock`): transfer (≤ 8 s), the
  burst transfer (only while ≥ 3.5 s are left), the witness write and the final
  `GetClusterState` are capped by it; compaction is 6 s + 2 s + 2 s; event sinks give the
  leftover cleanup 2 s and each step ≤ 3 s; `cluster_state` makes one 2 s discovery call
  (not `NodeMap::discover`) and stops querying nodes with less than 2 s left. `ops_setup`
  spends at most 20 s; when it gave up, `transfer_leadership` creates `r-ops-<run>` after a
  `LEDGER_NOT_FOUND` witness write.
- **Long jobs.** Backup and CheckStore share one in-flight slot (one long job queued or
  running), and are not submitted while a quiet-blocking job is queued or running, so one
  job thread always stays free. A backup job has a 180 s budget for all its calls, busy
  retries and waits; each call's timeout is capped by what is left.
- **Backups and quiet periods.** Backup Start/Complete are Raft proposals, which spoil task
  15's fenced checks. Before each Backup/IncrementalBackup call the job waits (within its
  budget) while a quiet period may be open. 00c's `Shared` has no quiet flag, so
  `QuietWatch` infers it: rules are parked during a quiet period, so each task-12 rule
  records `(next_epoch, now)`; a quiet period is taken as open when the epoch moved past
  that observation and `last_quiet_end` did not (or the epoch moved by two). A quiet period
  that starts while a backup call runs is not detected.
- **NodeCache.** A discovered member map is cached only when every member has a service
  address and it lists at least the configured number of nodes; members without an address
  are skipped.

## Requests to 99

- `the driver receives restore-target logs` needs task 18's restore targets forwarding logs
  (exclude from 12's acceptance).
- `a node creates a main store query checkpoint` needs task 09's `createQueryCheckpoint`
  orders.
- Backups use the server's default namespace (`bucketId` omitted, as the old drivers did);
  task 18 uses `hegel-restore`. The full-backup Always uses barrier commit indexes of the
  main cluster only (`barrier_monotonic`), so restore-target traffic cannot affect it.
- A `Shared` flag "quiet period open" (set at Q0, cleared by `EndGuard`) would replace
  `QuietWatch`'s inference in `cluster_ops.rs`.
- Long jobs: one task-12 job at a time. If other families' long jobs also share the two
  job threads, 99 may still want a dedicated thread for long Rust-only jobs (00c's
  `jobs.rs`).

---

<a id="13-numscript"></a>

## 13 — Numscript (Rust-checked `r-ns-*` ledgers)

Numscript never runs on oracle-tracked ledgers (DESIGN §3, critic Q7). Task 13 owns two
Rust-checked ledgers, `r-ns-<run>-1` and `r-ns-<run>-2`, created by the setup hook
`numscript_setup` (untracked, keyed `r-<run>-k<N>`, retried; no draws), which also saves
library script `transfer` version `1.0.0` on each:

```
vars { account $from  account $to  monetary $amount }
send $amount ( source = $from allowing unbounded overdraft  destination = $to )
```

Code: `HG/driver/src/rules/numscript.rs` (rules, setup hook, quiet hook),
`rules/numscript/model.rs` (library and transaction model, script texts, pure checks),
`rules/numscript/semver.rs` (ordering, selectors, bumps). Every write goes through
`send_untracked` with a key; nothing goes through the oracle.

| Rule | Weight | What |
|---|---|---|
| `ns_save` | 0.5 | a next version of `transfer` (fixed `2.0.0` branch while the greatest sent version is below it, else a drawn major/minor/patch bump by 1 or by a drawn amount of the uint32 room), a next version of an existing `s-<N>`, or the first version of a new `s-<N>` (`N.0.0`, N ∈ 1..=10, or three drawn uint32 components below `u32::MAX`); 1/8 re-saves an acked version with other content. Versions are allocated under the model lock above the greatest version ever sent, so concurrent saves never race on one version. `transfer` versions differ from 1.0.0 only in drawn whitespace, line endings and comments (`Layout`); `s-<N>` versions are drawn among three templates (`Transfer`, `Reversed`, `ViaHub`) |
| `ns_read` | 0.5 | GetNumscript (`""`, `latest`, exact, partial `a`, `a.b`), ListNumscripts (drawn page size, cursor paging ≤ 8 s), ListNumscriptVersions, on a name with an acked version; linearizable (server default), drawn route |
| `ns_exec` | 1.0 | CreateTransaction with `scriptReference{name, "latest" | pinned acked version, vars}` or inline `script{plain: transfer, vars}`; from = pool address or `world`, to = another pool address, amount `COIN`/`USD/2` from `amount_positive`; unique reference `r-<run>-ns<N>`; keyed; `send_untracked` |
| quiet `numscript_state` | 0.5 (30 s, `needs_oracle: false`) | per ledger (hegel-drawn order): ListTransactions (all pages), ListAccounts (all pages), GetLedgerStats; resolve Unknown records by reference; evaluate the Always properties only for fully read ledgers |

## Porting table

| Old source | Old assertion (kind: literal) | New | Where |
|---|---|---|---|
| MD/parallel_driver_numscript_lifecycle/main.go:71 | Sometimes: should be able to save numscript | verbatim (cond = success or non-definitive outcome, the old `IsTolerated`) on the first save of a name | rules/numscript.rs ns_save |
| MD/parallel_driver_numscripts/main.go:34 | Sometimes: should be able to save numscript | verbatim (same call site as above: one literal, one site) | rules/numscript.rs ns_save |
| MD/parallel_driver_numscript_lifecycle/main.go:80 | AlwaysOrUnreachable: ListNumscripts should not return unexpected error | verbatim (tolerated = success or non-definitive outcome) | rules/numscript.rs ns_read |
| MD/parallel_driver_numscript_lifecycle/main.go:83 | AlwaysOrUnreachable: saved numscript should appear in ListNumscripts | verbatim, generalised to concurrency: every name with a version acked before the first page was sent appears in a complete traversal | rules/numscript.rs ns_read |
| MD/parallel_driver_numscript_lifecycle/main.go:97 | Sometimes: should be able to save second numscript version | verbatim on saves of a name that already has an acked version | rules/numscript.rs ns_save |
| MD/parallel_driver_numscript_lifecycle/main.go:114 | AlwaysOrUnreachable: GetNumscript should return latest version | verbatim for selectors `""`/`latest`: the returned version may be stored (not definitively rejected) and is ≥ the greatest version acked before the read (equal to the old check when no save is concurrent) | rules/numscript.rs ns_read |
| MD/parallel_driver_numscript_lifecycle/main.go:130 | AlwaysOrUnreachable: latest pointer should be the greatest saved semver | verbatim: `latestVersion` ∈ versions that may be stored and ≥ max acked-before; also (no concurrency involved: one read handle, `query/numscript.go` ReadAllNumscriptVersions) the listing is non-empty and `latestVersion` equals its greatest version | rules/numscript.rs ns_read |
| MD/parallel_driver_numscripts/main.go:49 | AlwaysOrUnreachable: saved numscript should be readable | verbatim (returned name equals the requested name) | rules/numscript.rs ns_read |
| MD/parallel_driver_numscripts/main.go:75 | Sometimes: should be able to use saved numscript in transaction | verbatim on scriptReference transactions (cond = success or non-definitive) | rules/numscript.rs ns_exec |
| MD/parallel_driver_numscripts/main.go:77 (`internal.CheckCreatedTransaction`) | (helper assertions on the created transaction: `checks.go:66-78` post-commit volume entry missing fields / input / output invalid) | task 02 never sees `r-ns-*` transactions, so the post-commit volume part is re-checked here under a new literal (`a numscript transaction reports consistent post-commit volumes`, below), strengthened; the postings part is the new exact-postings Always; the helper's literals stay with task 02's port | rules/numscript.rs ns_exec |
| MD/parallel_driver_transactions/main.go:46-79 (Numscript branch of `randomTransactionRequest`) | (no literal of its own; the branch shares `should be able to create a transaction` / `should be able to create bulk transactions` / `bulk Apply should return one log per request` / `should always be able to read committed transaction` with the postings branch) | the Numscript branch moves here as ns_exec's inline mode; the shared literals stay with task 02 (postings branch) | rules/numscript.rs ns_exec |
| H/main.rs:800-817 (`numscript` rule on tracked hegel ledgers) | Sometimes: a numscript transaction commits | verbatim (existing hegel literal kept) on inline script transactions, now on `r-ns-*` ledgers | rules/numscript.rs ns_exec |
| H/main.rs:813-815 (`register_tx`/`resolve_tx` of the numscript rule) | Always: an acknowledged transaction has exactly the postings that were sent; an acknowledged transaction survives faults; a definitively rejected write never takes effect; an account's balance equals the sum of its committed postings (H/model.rs-backed, tracked ledgers) | retired with H/model.rs for tracked ledgers (oracle); for Numscript re-homed as the new properties below with their own literals | – |
| H/model.rs `Status`, `TxRec` | (model, no literal) | re-homed for `r-ns-*` ledgers in rules/numscript/model.rs (no metadata histories) | rules/numscript/model.rs |

## New properties

| Literal | Kind | Rule | Condition |
|---|---|---|---|
| `a saved numscript version is immutable` | AlwaysOrUnreachable | ns_save | a re-save of an acked version with other content never succeeds |
| `a re-save of a saved numscript version is refused as already existing` | AlwaysOrUnreachable | ns_save | a definitive refusal of that re-save has reason `NUMSCRIPT_VERSION_ALREADY_EXISTS` and code AlreadyExists (`processor_numscript_library.go:46-48`); a non-definitive outcome checks nothing |
| `a numscript read returns a saved version and its content` | AlwaysOrUnreachable | ns_read | GetNumscript (any selector) returns a legal resolution of the selector (stored possibly, matching, ≥ the greatest matching acked-before version) with the content the model saved for that version; ListNumscripts items are legal latest versions with their content; ListNumscriptVersions lists every acked-before version and only versions that may be stored; a server refusal of a selector that an acked version matches is a violation: a definitive reason, or the bare gRPC `NotFound` (no reason) GetNumscript answers a missing script with (`controller_default.go` GetNumscript, `adapter/grpc/server.go:662-664`) |
| `a numscript transaction commits exactly the postings its script implies` | Always | ns_exec | a definitive success returns one log whose created transaction's postings are one of the candidates (`transfer`/inline: `[{from,to,amount,asset}]`; `s-<N>` with `latest`: the template of every non-rejected version, as known when the answer arrives, at or above the greatest version acked when the transaction was sent; pinned: that version's template). Second call site in numscript_state: an Unknown/Pending record found committed by the quiet listing must be listed with one of its candidates (for `latest`, recomputed from the library at the listing, so a write applied after the driver gave up on it is judged against every version saved since its floor) |
| `a numscript transaction reports consistent post-commit volumes` | Always | ns_exec | on a definitive success whose created transaction carries `postCommitVolumes`: every posting's (account, asset) has an uncolored entry with present, decodable input and output (the server always sets both, `processor_volumes.go:60-80`), input ≥ the amounts the transaction sent to it and output ≥ the amounts it sent from it. An absent `postCommitVolumes` is tolerated, like the old helper |
| `an acknowledged numscript transaction survives faults` | Always | numscript_state | every Acked record is listed by reference with the postings acknowledged |
| `a definitively rejected numscript transaction never takes effect` | Always | numscript_state | no Rejected record is listed |
| `a numscript ledger's balances equal its committed postings` | Always | numscript_state | ListAccounts volumes (input, output, balance = input − output) of every account equal the sums over committed records (Acked + resolved); no unexpected non-zero volume; color always `""`. The two listings are separate snapshots and a write the driver gave up on can still land between them (a client-side retry outlives the Q1 barrier), so on a mismatch the hook re-reads accounts then transactions and judges again; only a mismatch on three reads is asserted |
| `numscript execution count caught up` | Sometimes | numscript_state | `GetLedgerStats.numscriptExecutionCount` equals the committed script transactions (records that stay unresolved are not committed as of the listing, so they do not gate it; the counter is an asynchronous usagebuilder projection, `controller_default.go:530-545`, so only Sometimes) |
| `numscript_state completes within its quiet budget` | Sometimes | numscript_state | both ledgers fully checked before the deadline |

Status machinery (H/model.rs): a record is Pending when sent, Acked on success, Rejected on a
definitive failure (`clients::definitive`), Unknown otherwise. The quiet hook resolves
Unknown/Pending records by reference against the complete listing: listed → checked against its
candidates, then Acked with the listed postings (balances use them); not listed → stays Unknown (it did not commit by now,
after the Q1 barrier; balances exclude it). Library versions keep Pending/Acked/Unknown/
Rejected; a read is judged with the window (acked before the send, possibly stored after the
answer).

## Reachability (must-hit assertions)

| Literal | Kind | Reached by | Needs |
|---|---|---|---|
| should be able to save numscript | Sometimes | ns_save (new `s-<N>`) | no faults |
| should be able to save second numscript version | Sometimes | ns_save (`transfer` next version) | no faults |
| should be able to use saved numscript in transaction | Sometimes | ns_exec (scriptReference modes) | no faults |
| a numscript transaction commits | Sometimes | ns_exec (inline mode) | no faults |
| a numscript transaction commits exactly the postings its script implies | Always | ns_exec success | no faults |
| an acknowledged numscript transaction survives faults | Always | numscript_state | no faults (quiet period) |
| a definitively rejected numscript transaction never takes effect | Always | numscript_state (evaluated on every fully read ledger, vacuously true without rejected records) | no faults |
| a numscript ledger's balances equal its committed postings | Always | numscript_state | no faults |
| numscript execution count caught up | Sometimes | numscript_state | no faults |
| a numscript transaction reports consistent post-commit volumes | Always | ns_exec success | no faults |
| numscript_state completes within its quiet budget | Sometimes | numscript_state | no faults |

AlwaysOrUnreachable literals (`a saved numscript version is immutable`, `a numscript read
returns a saved version and its content`, `a re-save of a saved numscript version is refused
as already existing`, the five verbatim reads) are free by policy.

## Deviations and decisions

- **Taper**: ns_exec stops writing to a ledger with 4000 records (`TX_CAP`), at most 16
  `s-<N>` names per ledger and 24 versions per name, so the quiet hook's full listing stays
  inside its 30 s budget on long timelines (DESIGN §3.2 tapers tracked ledgers; the same
  bound is applied here).
- **No self-transfers**: `to` is redrawn deterministically (`i+1 mod 100`) when it equals
  `from`; a `from == to` Numscript send has no single "exact postings" expectation worth
  pinning.
- **Setup steps are bounded** (40 s each); a ledger whose creation or `transfer@1.0.0` save
  did not succeed is marked not ready and every rule skips it.
- Content comparisons trim surrounding whitespace (the model compares what it sent with
  what the server returns).
- Paged reads stay inside the rule bound: ListNumscripts pages until its 8 s read budget is
  spent (no page-count cap: 17 names at page size 1 need 17 pages), each page with the
  remaining budget as its timeout; the quiet hook's listings back off 300/600/900 ms between
  failed pages, inside the deadline.
- `ListAccounts`/`ListTransactions` in the quiet hook page with `pageSize 1000` and
  `maxItems 1001` so each call reaches the stream's end (and its `x-next-cursor`).

## Requests to 99

- None for dependencies. The native acceptance run
  (`RULES=ns_save,ns_read,ns_exec QUIET_PROBABILITY=0.05 run_native.sh 180`) could not be
  run by task 13: the build host's disk was full at first, and on the re-run after review the
  ledger server refused the setup writes with `WRITES_BLOCKED_DISK_FULL` (host disk 96% used);
  task 99 should run it and confirm the Sometimes literals above are hit with zero failing
  assertions.

---

<a id="14-rust-probes"></a>

## 14 — Rust probes: porting record

Rules `stale_probe` (1.0) and `malformed_write` (0.5), setup hook `probes_setup`
(creates `r-probe-<run>` and `r-bad-<run>`, untracked, keyed), quiet hooks
`quiesce_and_absence` (45 s, weight 1) and `eventually_correct` (90 s, weight 1).
All code is in `driver/src/rules/probes.rs`. Nothing here uses the oracle: the
rules run in degraded mode and both hooks ignore `QuietCtx.begin`.

| Old source | Old assertion (kind: literal) | New | Where |
|---|---|---|---|
| MD/parallel_driver_stale_reads/main.go:114 | Unreachable: stale read GetAccount returned unexpected error | verbatim (NotFound and transient errors tolerated) | rules/probes.rs `check_stale_read` |
| MD/parallel_driver_stale_reads/main.go:137 | Unreachable: stale read returned volume with absent required field | verbatim | rules/probes.rs `check_stale_read` |
| MD/parallel_driver_stale_reads/main.go:150 | Unreachable: stale read returned invalid typed volume | verbatim | rules/probes.rs `check_stale_read` |
| MD/parallel_driver_stale_reads/main.go:166 | Always: stale read balance stays within attempted write bounds | verbatim (`0 ≤ balance ≤ attempted`; `attempted` counted before each send, read after the read returns) | rules/probes.rs `check_stale_read` |
| MD/parallel_driver_stale_reads/main.go:172 | Always: stale read probe volumes are single-direction consistent | verbatim (`input == balance && output == 0`) | rules/probes.rs `check_stale_read` |
| MD/parallel_driver_stale_reads/main.go:183 | Sometimes: stale read of probe account succeeded | verbatim (per round) | rules/probes.rs `run_stale_round` |
| MD/parallel_driver_stale_reads/main.go:186 | Sometimes: stale read observed lag behind acked writes | verbatim (`balance < acked`, `acked` snapshotted before the read) | rules/probes.rs `run_stale_round` |
| MD/parallel_driver_stale_reads (Lot F, deferred) | per-node stale-read monotonicity (proposed in main-drivers §3.28) | not adopted: node boots are learned from forwarded logs, which lag the first read after a restart, and a restarted node may serve an older persisted prefix while it replays (DESIGN Q8, Appendix A) | – |
| H/main.rs:654-693 (stale half of `read_transaction`) | Sometimes: a stale read misses a recent transaction | renamed: `stale read observed lag behind acked writes` (on the probe, where lag is judged against exact acked counts) | rules/probes.rs |
| H/main.rs:654-693 (linearizable half) | Always: a linearizable read finds every acknowledged transaction; a transaction reads back as it was acknowledged; … | oracle (task 05) | – |
| MD/eventually_quiesce_read/main.go:97 | Always: linearizable read completes on a quiescent cluster | verbatim (success, or an error the 30 s deadline did not cause; evaluated only after an acked write and the full idle window) | rules/probes.rs `run_quiesce` |
| MD/eventually_quiesce_read/main.go:100 | Reachable: quiescent linearizable read returned data | verbatim | rules/probes.rs `run_quiesce` |
| MD/parallel_driver_definitive_errors/main.go:152 | Sometimes: definitive write rejection recorded for absence check | verbatim (per malformed write) | rules/probes.rs `judge_malformed` |
| MD/parallel_driver_definitive_errors/main.go:206 | Always: definitively rejected write never appears in the ledger | verbatim, malformed half: quiet hook lists every `r-bad-<run>` transaction; any whose reference was definitively rejected fails it | rules/probes.rs `absence_check` |
| MD/parallel_driver_definitive_errors (overdraft half, boundary amounts) | same two literals for unforced overdrafts | oracle (tracked insufficient-funds failures, freezing in task 01; boundary amounts are task 02's domain) | – |
| MD/eventually_correct/main.go:42 | Unreachable: barrier returned unexpected error | verbatim | rules/probes.rs `barrier` |
| MD/eventually_correct/main.go:79 | Sometimes: barrier quiescence achieved | verbatim (per ledger fence: `after == before + 1`) | rules/probes.rs `check_ledger` |
| MD/eventually_correct/main.go:84 | Sometimes: should be able to list ledgers | dropped: a failed ListLedgers is reported through `eventually_correct completes within its quiet budget` (false) | – |
| MD/eventually_correct/main.go:135 | Unreachable: listAccounts returned unexpected error | verbatim | rules/probes.rs `observe_ledger` |
| MD/eventually_correct/main.go:154 | Always(false): double-entry: account volume balance field is absent | verbatim, as `check_always!(present, …)` | rules/probes.rs `sum_balances` |
| MD/eventually_correct/main.go:160 | Always(false): double-entry: account volume balance is invalid | verbatim, as `check_always!(valid, …)` | rules/probes.rs `sum_balances` |
| MD/eventually_correct/main.go:175 | Always: double-entry: sum of balances should be 0 | verbatim (per (asset, color), exact arbitrary-width signed sum; only under a quiescent fence) | rules/probes.rs `evaluate_ledger` |
| MD/eventually_correct/main.go:191-222 (`checkAccountBalances`, W/internal/checks.go:15-56) | Reachable: should be able to get account; Unreachable: GetAccount returned unexpected error; Always: reported balance and volumes should be consistent, account volume entry has missing required fields, account volume input/output/balance is invalid | `GetAccount returned unexpected error` verbatim (sampled cross-check reads); the per-user GetAccount volume checks are oracle on tracked ledgers (task 05 point reads); on every ledger the same properties are checked on the listed volumes (rows for main.go:258,266,272 below) | rules/probes.rs `observe_ledger`, `check_listed_volumes` |
| MD/eventually_correct/main.go:245 | Unreachable: listAccounts returned unexpected error in checkVolumesConsistent | merged into `listAccounts returned unexpected error` (one listing serves both checks) | rules/probes.rs `observe_ledger` |
| MD/eventually_correct/main.go:258,266 | Always(false): cross-check: account volume has missing required fields / has invalid typed amount | verbatim, every listed volume of every ledger, unfenced (a property of one response), as `check_always!(…)` | rules/probes.rs `check_listed_volumes` |
| MD/eventually_correct/main.go:272 (`internal.CheckVolume`) | Always: reported balance and volumes should be consistent | new literal `listed account volume balance equals input minus output` (same property, every listed volume of every ledger, exact at any width, unfenced). The old literal is shared by every `CheckVolume` caller (post-commit volumes, point reads) that other families map to the oracle, so it is not reused here | rules/probes.rs `check_listed_volumes` |
| MD/eventually_correct/main.go:280 | Unreachable: GetAccount returned unexpected error in cross-check | merged into `GetAccount returned unexpected error` (the sampled reads serve both checks) | rules/probes.rs `observe_ledger` |
| MD/eventually_correct/main.go:292 | Unreachable: should get requested volumes | new literal `list/get volume bucket missing on one side past quiescence` (Unreachable): under the fence, both directions, only for a bucket with non-zero input or output (an unused bucket may be absent). Not the old literal: the fence and the non-zero condition change the check | rules/probes.rs `evaluate_ledger` |
| MD/eventually_correct/main.go:302,308 | Always(false): cross-check: GetAccount volume balance is absent / invalid | verbatim, as `check_always!(…)` on every sampled GetAccount bucket | rules/probes.rs `evaluate_ledger` |
| MD/eventually_correct/main.go:331 | Unreachable: list/get balance divergence persisted past quiescence | verbatim (only under a quiescent fence; otherwise re-read up to 2 times, then no verdict) | rules/probes.rs `evaluate_ledger` |
| MD/eventually_correct/main.go:343 | Reachable: list/get balance pair verified matching | verbatim | rules/probes.rs `evaluate_ledger` |
| MD/eventually_correct/main.go:367 | Reachable: eventually_correct found no list/get balance pairs to compare | dropped: coverage note only; `can check all volumes for consistency` details carry `pairs` | – |
| MD/eventually_correct/main.go:372 | Reachable: can check all volumes for consistency | verbatim (per checked ledger) | rules/probes.rs `evaluate_ledger` |
| MD/eventually_correct/main.go:383-404 | AlwaysOrUnreachable: list metadata key should exist in getaccount; list metadata value should match getaccount; metadata key count should match between list and get | verbatim, under the fence, for up to 20 sampled accounts; the value literal compares `GetStringValue()` as the old `metadataToMap` did ("" for non-string values); typed equality is the new `list metadata typed value should match getaccount` | rules/probes.rs `evaluate_ledger` |
| MD/parallel_driver_stats_consistency/main.go:57 | Always: log count must be >= transaction count (logs include metadata, reverts, etc.) | verbatim, every listed ledger, in `eventually_correct` | rules/probes.rs `check_stats` |
| MD/parallel_driver_stats_consistency/main.go:80 | Always: aggregate balance must be zero (double-entry) | merged into `aggregate double-entry: input must equal output` (the same check) | rules/probes.rs `check_aggregates` |
| MD/parallel_driver_stats_consistency/main.go:91 | Reachable: stats consistency check passed | dropped: coverage only, `can check all volumes for consistency` | – |
| MD/parallel_driver_invariants/main.go:24 | Unreachable: AggregateVolumes returned unexpected error | verbatim | rules/probes.rs `stats_and_aggregates` |
| MD/parallel_driver_invariants/main.go:35 | Always: aggregate double-entry: input must equal output | verbatim, every listed ledger, exact U256; absent side = 0 (as `ToBigInt` on nil), an undecodable side fails (fixes H/main.rs:743-745, where `None == None` passed) | rules/probes.rs `check_aggregates` |
| H/main.rs:736-747 (`aggregate`) | Always: double entry: an asset's total input equals its total output | renamed to the MD literal above (same property, bug fixed) | rules/probes.rs |

## New properties

- `a malformed request is refused as invalid` (AlwaysOrUnreachable): a definitive outcome of a
  malformed write is `InvalidArgument` with the arm's reason.
- `a malformed request never commits` (Unreachable): on a successful malformed write, and on any
  transaction found in `r-bad-<run>` by the quiet absence listing.
- `quiesce_and_absence completes within its quiet budget` (Sometimes).
- `eventually_correct completes within its quiet budget` (Sometimes).
- `listed account volume balance equals input minus output` (Always): every listed volume of every
  ledger (replaces the old shared `CheckVolume` literal here, see the table).
- `list/get volume bucket missing on one side past quiescence` (Unreachable): a sampled account's
  used bucket (non-zero input or output) is listed but not returned by GetAccount, or the reverse.
- `list metadata typed value should match getaccount` (AlwaysOrUnreachable): the whole typed
  `MetadataValue` agrees between ListAccounts and GetAccount.

## Reachability

| Must-hit assertion | Rule / hook | Needs |
|---|---|---|
| stale read balance stays within attempted write bounds | stale_probe | no faults |
| stale read probe volumes are single-direction consistent | stale_probe | no faults |
| stale read of probe account succeeded | stale_probe | no faults |
| stale read observed lag behind acked writes | stale_probe | faults (or a lagging follower; on one native node only "succeeded" is expected) |
| definitive write rejection recorded for absence check | malformed_write | no faults |
| definitively rejected write never appears in the ledger | quiesce_and_absence | no faults (needs a malformed write first) |
| linearizable read completes on a quiescent cluster | quiesce_and_absence | no faults |
| quiescent linearizable read returned data | quiesce_and_absence | no faults |
| quiesce_and_absence completes within its quiet budget | quiesce_and_absence | no faults |
| barrier quiescence achieved | eventually_correct | no faults |
| double-entry: sum of balances should be 0 | eventually_correct | no faults |
| double-entry: account volume balance field is absent / is invalid | eventually_correct | no faults (any listed volume) |
| cross-check: GetAccount volume balance is absent / is invalid | eventually_correct | no faults (any sampled account with volumes) |
| cross-check: account volume has missing required fields / has invalid typed amount | eventually_correct | no faults (any listed volume) |
| listed account volume balance equals input minus output | eventually_correct | no faults (any listed volume) |
| list/get balance pair verified matching | eventually_correct | no faults (any ledger with accounts: `r-probe-<run>` always has some) |
| can check all volumes for consistency | eventually_correct | no faults |
| log count must be >= transaction count (…) | eventually_correct | no faults |
| aggregate double-entry: input must equal output | eventually_correct | no faults |
| eventually_correct completes within its quiet budget | eventually_correct | no faults |

## Deviations from the spec

- Metadata-key arm of `malformed_write`: the server rejects an over-long key with
  `InvalidArgument` / `METADATA_LIMIT_EXCEEDED` (`ErrMetadataKeyValidation` wrapping
  `ErrMetadataLimitExceeded`, kind Validation, `internal/domain/reason.go`), not `VALIDATION`.
  That arm expects `METADATA_LIMIT_EXCEEDED`; the other three expect `VALIDATION`.
- "Single attempt" sends use `send_untracked` with `attempt_timeout = 2 s` (8 s for the quiesce
  tries) and a **zero** retry budget instead of `budget = attempt_timeout`: with a budget equal to
  the timeout, a fast non-definitive failure made the loop wait out the budget and could send a
  second attempt when the backoff rounding left time. A zero budget gives exactly one attempt.
- `quiesce_and_absence` runs (b) the absence listing before (a) the write/idle/read, so a hanging
  probe read cannot starve the absence check; nothing is sent between the acked write and the
  probe read.
- Stale-read epochs: the spec fixes the probe account to `probe:main` with counters for the whole
  timeline. Every unknown outcome widens `attempted - commits` for good, so over a long run
  `balance <= attempted` would stop catching a duplicated increment. Increments go to the current
  epoch's account (`probe:main`, then `probe:main-<n>`), and the epoch moves on once 5 of its
  increments are lost (unknown or refused); each epoch keeps its own counters, and a read is judged
  against the counters of the account it read. The slack stays as small as with the old per-run
  ledger (5 or 15 increments), and accounts grow only with lost increments.
- Account samples: instead of 20 independent `u32`s modulo the listed count, `plan` draws a base
  and one jitter per sample; the sample takes one index in each of 20 disjoint stride windows
  (every account when 20 or fewer). hegel pushes wide integers toward 0, so independent draws
  collapsed onto the first account; the windows keep 20 distinct accounts spread over the listing.
- Ledger walk order: rotated by a drawn offset plus a cursor of ledgers walked by earlier runs
  (driver state, not a draw), so a walk cut short by the deadline resumes where the last one
  stopped, and the tail of `ListLedgers` (`r-*`, `treasury`) is reached.
- Quiesce write tries: 3 tries of 8 s cannot fit next to the idle window and the 30 s read in 45 s.
  The room left (deadline minus idle, read and margin) is shared among the remaining tries: each try
  gets `room / tries_left - 1.5 s`, within 2..8 s and never past the room. With an 8 s idle the room
  is about 5 s, so usually one 2 s try plus fast retries; with a 2 s idle, up to 3 tries of about 2 s.
  A slow absence listing (its 8 s maximum) with an 8 s idle leaves no room: no probe read that
  period, logged and reported through `quiesce_and_absence completes within its quiet budget`.
- Fences are per ledger (Barrier, read the ledger, Barrier), not one fence around the whole sweep,
  so a background proposal costs one ledger's re-read rather than the whole check.

## Requests to 99

- None for dependencies. `stale read observed lag behind acked writes` needs faults or a lagging
  follower: do not remove it for being unhit in a single-node native run.
- Flavour draw bias: in the native acceptance run (`RULES=stale_probe,malformed_write
  QUIET_PROBABILITY=0.05`, 180 s) all 23 quiet periods drew `quiesce_and_absence`, the first of the
  two equally weighted hooks, so `eventually_correct` never ran. hegel biases
  `draw::weighted`'s `int_in(0, 2^32-1)` toward 0. With the quiesce weight set to 0 for a local
  run only (not committed), 61 periods ran `eventually_correct` over 7 ledgers with every must-hit
  hit and no finding. 99 should check how hooks spread once all families are merged; the fix
  belongs in 00c's `draw::weighted`.

---

<a id="15-convergence"></a>

## 15 — Cross-node identity, stats convergence, cluster-config convergence

Three Rust quiet hooks (`HG/driver/src/rules/convergence.rs`, DESIGN §4.10) and one Go quiet
provider (`W/bin/oracle/h_usage.go`, `usage`). Every hook runs in its own quiet period (one
flavour per period: rules parked, resolvers drained, oracle quiescent), writes only to
`r-wit-<run>-<N>` and issues Barriers. `plan` draws the client that carries the pinned calls
(and, for `stats_convergence`, the seed of the foreign-ledger sample); `run` draws nothing.

| Hook | Budget | Weight | `needs_oracle` |
|---|---|---|---|
| `cross_node_identity` | 90 s | 1 | no |
| `stats_convergence` | 70 s | 1 | yes (`providers.usage`) |
| `cluster_config_convergence` | 20 s | 0.5 | no |

Liveness rule: verdicts and the "connects" / "obtains a qualified observation" Always
properties are evaluated only when the hook ran to completion. A deadline stop reports
`<hook> completes within its quiet budget` (Sometimes, false) and evaluates nothing else.
Attempts end `Late` (a fence moved: late write, election entry, membership change — never a
finding), `Other` or `Cut` (deadline). The qualified-observation Always is false only when
every attempt ended `Other`.

## Old assertions

| Old source | Old assertion (kind: literal) | New | Where |
|---|---|---|---|
| MD/eventually_cross_node_identity/main.go:24 | Always: cross-node identity oracle connects to replicas | verbatim (on completion; true when the quiet period's node discovery answered — the old per-node dial — or any candidate gave a non-local answer to a pinned Barrier) | convergence.rs `cross_node_identity` |
| MD/eventually_cross_node_identity/main.go:40 | Always: cross-node identity oracle obtains a qualified all-member observation | verbatim (on completion; false only when every attempt failed for a reason other than a late write or membership change) | convergence.rs `cross_node_identity` |
| MD/eventually_cross_node_identity/main.go:45 | Sometimes: cross-node oracle reaches quiescence | verbatim | convergence.rs `publish_cross` |
| MD/eventually_cross_node_identity/main.go:46 | Sometimes: at least two nodes converge to a common applied index | verbatim | `publish_cross` |
| MD/eventually_cross_node_identity/main.go:47 | Reachable: all current replicas reach the same durable identity horizon | verbatim | `publish_cross` |
| MD/eventually_cross_node_identity/main.go:49 | Always: complete audit listing and point tip agree at a common applied index | verbatim; the listing is the bounded window `[max(1, A − 5000), A]` (A = the audit head) instead of the whole chain (main-drivers §3.2 timing gotcha): the window digest must end at the tail's sequence, and the window's last entry (a forward one-item listing filtered to that sequence) must equal the tail (`pageSize:1, reverse:true`) as a whole entry (old `proto.Equal(lastListed, tail)`); the `GetAuditEntry` tip with items stripped must equal the tail as before | `publish_cross` |
| MD/eventually_cross_node_identity/main.go:65 | Always: account state is identical across nodes at a common applied index | verbatim; compares the `/digest` sections `ledgers` and `accounts` (all ledgers), reports the first differing ledger | `publish_cross` |
| MD/eventually_cross_node_identity/main.go:67 | Always: audit hash is identical across nodes at the same sequence | verbatim (same window digest, same tail, same last listed entry, same tip **items included** — the old `TipDigest` over the unstripped tip) | `publish_cross` |
| MD/eventually_cross_node_identity/main.go:68 | Always: audit hash version is identical across nodes at the same sequence | verbatim (tail `hashVersion`) | `publish_cross` |
| MD/eventually_cross_node_identity/main.go:72 | Reachable: cross-node complete account and audit identity verified | verbatim; **deviation**: also reached with a single member (the old driver returned before it with fewer than two observations). Task 15's acceptance requires it on the one-node native run; the two-member exercise is shown by `at least two nodes converge to a common applied index` | `publish_cross` |
| MD/eventually_stats_consistency/main.go:26 | Always: stats convergence oracle connects to replicas | verbatim (on completion; same rule as the cross-node one: discovery answered or a candidate answered non-locally) | convergence.rs `stats_convergence` |
| MD/eventually_stats_consistency/main.go:42 | Always: stats oracle obtains a qualified observation within its bounded retry budget | verbatim (on completion, same rule as above) | `stats_convergence` |
| MD/eventually_stats_consistency/main.go:48 | Always: usage counters converge exactly at a fixed source horizon | verbatim, **evaluated only for a member with a certified sweep** (witness `referenceCount == 1` before and after the sweep) or a definitive divergence (ledger incarnation differs, non-transient read failure past the horizon): true when a certified sweep matched, false when the window ended with the last certified sweep mismatching. A member that never certified (usage store still rebuilding after a restart) gets no usage verdict; it is reported by the new `the usage witness certifies on a member within the stats window` Sometimes (**deviation**: the old driver reported it as a usage violation, but it ran once at the end of the test, not right after faults stop). Tracked ledgers' expected counts come from the oracle (`providers.usage`) and are checked inside the capture fence against the source's synchronous `GetLedgerStats {transactionCount, logCount}` (`txCount`, `logCount`; a difference fails the attempt as Other, never a usage verdict); foreign ledgers' from the ListLogs fold (contiguous ids, `payload.apply.ledgerName == ledger`, fold log count == main-store `logCount`), every untracked ledger of the tracked partition (reserved `o-<run>-own<N>`) plus a plan-seeded sample of 8 other foreign ledgers, each with at most 5000 logs (**deviation**: the old driver folded every ledger; the full fold made the hook's budget run out on long timelines) | `publish_stats` |
| MD/eventually_stats_consistency/main.go:50 | Reachable: stats usage convergence verified | verbatim | `publish_stats` |
| H/main.rs:1223 | Sometimes: every node catches up after faults stop | dropped: superseded by `all current replicas reach the same durable identity horizon` (exact per-node horizon, DESIGN §6.10) — the old check read the leader's index for every node (main-drivers §1.3 #1) | – |
| H/main.rs:1242 | Always: every node holds the same account state at a common index | dropped: superseded by `account state is identical across nodes at a common applied index` (full account digests per member, not a fixed address sample read at the leader's index) | – |
| MD/singleton_driver_config_change/main.go:174 | Reachable: follower observed same ClusterConfig as leader | replaced by the new `cluster config convergence verified` / `every node's cluster config equals the leader's at a common horizon` (no K8s patch; compares every member, not one follower) | convergence.rs `publish_config` |
| MD/singleton_driver_config_change/main.go:134, 138, 153, 157, 160 | Sometimes: config patch should succeed; Reachable: config patch applied; Sometimes: StatefulSet should reach Ready after a config patch; Reachable: STS ready after config patch; Sometimes: ClusterConfig should converge to the patched value | dropped: K8s/operator out of scope (DESIGN §1 item 7) | – |

## New properties

| Literal | Kind | Hook |
|---|---|---|
| `cross_node_identity completes within its quiet budget` | Sometimes | cross_node_identity |
| `stats_convergence completes within its quiet budget` | Sometimes | stats_convergence |
| `cluster_config_convergence completes within its quiet budget` | Sometimes | cluster_config_convergence |
| `the usage witness certifies on a member within the stats window` | Sometimes (per member of a qualified stats observation; false when the member's usage store never certified the witness in the window) | stats_convergence |
| `every node's cluster config equals the leader's at a common horizon` | Always (on completion, per member; skipped when the leader's config is the unpersisted local fallback `{rotationThreshold}`, `internal/adapter/grpc/server_cluster.go:155-160`) | cluster_config_convergence |
| `cluster config convergence verified` | Reachable | cluster_config_convergence |
| `a cluster config change was observed between quiet periods` | Sometimes (the leader's whole `clusterConfig` differs from the previous completed run's; per-node `CACHE_ROTATION_THRESHOLD`/`BLOOM_*` make a new leader propose its own config, DESIGN §8) | cluster_config_convergence |

## Protocol details

- Source selection (every hook): a pinned Barrier on each candidate (discovery, then the
  configured nodes); while some answer is transient (no leader, restart), rounds are retried
  with backoff for up to 15 s or a quarter of the remaining budget.
- Cross-node attempt (up to 3): pinned source Barrier `before`; membership from the source's
  `GetClusterState{}` (unique non-zero ids and addresses, suffrage Voter/Learner); Barrier
  `horizon == before + 1`; every member (in parallel, one 60 s window) must report
  `localNode == id`, status normal and `lastPersistedIndex == horizon` (above it → `Late`);
  per member stale pinned tail, the forward listing at the tail's sequence, and the tip;
  `POST /digest` `{node, consistency:"stale", sections:[ledgers, accounts, audit], auditFrom}`
  (retried once on failure; a failure with less than 1 s of budget left is a cut, not Other);
  the member must still be exactly at the horizon; membership unchanged and closing Barrier
  `== horizon + 1`.
- Stats attempt (up to 3): pinned source; membership; witness `r-wit-<run>-<N>` created
  (keyed, `send_untracked` pinned to the source); Barrier `before`; source capture
  (ListLedgers; tracked rows from `providers.usage`, each checked against the source's
  `GetLedgerStats {transactionCount, logCount}`; untracked partition rows and a seeded sample
  of 8 foreign rows with ≤ 5000 logs folded; deleted rows and the witness skipped); Barrier
  `after == before + 1`; one forced witness tx with `reference = witness`; Barrier
  `== after + 2` (the horizon); per member (parallel, one window of
  `min(60 s, remaining − 3 s)`, so the window always ends before the closing fence's
  reserve; under 1 s is a cut): `lastPersistedIndex ≥ horizon`, then stale sweeps: witness
  incarnation, witness `referenceCount == 1`, every source ledger's incarnation and
  `{logCount, postingCount, revertCount}`, witness re-check (the usage store can rewind; only
  a sweep certified at both ends counts); closing Barrier `== horizon + 1`.
  `QuietCtx.begin == None` (or no `providers.usage`) returns quietly.
- Cluster-config attempt (up to 3): pinned source; membership and leader; Barrier; every
  member `lastPersistedIndex ≥ horizon` with its own `GetClusterState{nodeId}`; compare each
  member's `clusterConfig` with the leader's (only when the leader's config is persisted:
  a config with no field besides `rotationThreshold` is the per-node fallback); membership/leader unchanged and closing Barrier
  `== horizon + 1` (added fence: a config proposal between the reads would otherwise be a
  false difference).
- Go provider `usage`: `{name: {id, logCount, txCount, postingCount, referenceCount,
  revertCount}}` (keyed by ledger name, JSON numbers) for every live tracked ledger (`match.ModelUsage`, model log
  and transaction counts, learned ledger id).

## Reachability

| Must-hit | Reached by | Needs |
|---|---|---|
| `cross_node_identity completes within its quiet budget` | cross_node_identity | no faults |
| `stats_convergence completes within its quiet budget` | stats_convergence | no faults |
| `cluster_config_convergence completes within its quiet budget` | cluster_config_convergence | no faults |
| `cross-node identity oracle connects to replicas` | cross_node_identity | no faults |
| `cross-node identity oracle obtains a qualified all-member observation` | cross_node_identity | no faults |
| `cross-node oracle reaches quiescence` | cross_node_identity | no faults |
| `at least two nodes converge to a common applied index` | cross_node_identity | 3-node cluster (task 99; native run has one node, so only `false` there) |
| `all current replicas reach the same durable identity horizon` | cross_node_identity | no faults |
| `complete audit listing and point tip agree at a common applied index` | cross_node_identity | no faults |
| `account state is identical across nodes at a common applied index` | cross_node_identity | 3-node cluster (task 99) |
| `audit hash is identical across nodes at the same sequence` | cross_node_identity | 3-node cluster (task 99) |
| `audit hash version is identical across nodes at the same sequence` | cross_node_identity | 3-node cluster (task 99) |
| `cross-node complete account and audit identity verified` | cross_node_identity | no faults (one member suffices) |
| `stats convergence oracle connects to replicas` | stats_convergence | no faults |
| `stats oracle obtains a qualified observation within its bounded retry budget` | stats_convergence | no faults |
| `usage counters converge exactly at a fixed source horizon` | stats_convergence | no faults |
| `stats usage convergence verified` | stats_convergence | no faults |
| `the usage witness certifies on a member within the stats window` | stats_convergence | no faults |
| `every node's cluster config equals the leader's at a common horizon` | cluster_config_convergence | no faults |
| `cluster config convergence verified` | cluster_config_convergence | no faults |
| `a cluster config change was observed between quiet periods` | cluster_config_convergence | faults (a leadership change between two runs of the hook, 3-node cluster with per-node cache/bloom config, DESIGN §8) |

## Requests to 99

- The three-node Always properties (`account state is identical …`, `audit hash … identical`,
  `audit hash version …`) and `at least two nodes converge to a common applied index` are
  only meaningful on the compose stack; the native one-node run evaluates them vacuously or
  as `false` Sometimes. Check them on the local stack.
- `a cluster config change was observed between quiet periods` needs a leadership change
  between two `cluster_config_convergence` runs; mark it "needs faults".
- Witness ledgers `r-wit-<run>-<N>` accumulate (one per stats attempt, one transaction each)
  and are folded by later stats runs; they are never deleted. If a long timeline shows the
  ListLedgers/fold cost growing, cap or delete them.
- Foreign ledgers are sampled (8 per attempt, plan-seeded) and those with more than 5000
  logs are never folded, so `r-stress-*` bulks are not usage-checked by this hook. Task 99
  should record how often `stats_convergence completes within its quiet budget` and
  `the usage witness certifies on a member within the stats window` are false on the stack.
- **Fixture conflict (blocks `go test ./bin/oracle/...` until fixed).** 00a's
  `TestFixtures_ResponseShapes` compares `/quiet/begin`'s `providers` against
  `HG/protocol-fixtures/core/quiet-begin.json`, whose `usage` placeholder is
  `{"o-…-l3": {"transactions": "57", "accounts": "12"}}`. `mapPaths` exempts only the
  `providers` level, so once a real `usage` provider is registered the test demands that
  exact ledger key set and those two string fields. The provider's content is
  provider-defined (DESIGN §4.10, the `mapPaths` comment says so). Fix in 99 (owner of the
  core files): add `"providers.*": true, "providers.*.*": true` to `mapPaths` in
  `W/bin/oracle/fixtures_test.go` (verified: with that one-line change
  `go test -short ./bin/oracle/` passes on this branch; not committed here because COMMON
  forbids edits of 00a's files), or regenerate the fixture's `usage` example as
  `{"<ledger>": {"id": 3, "logCount": 3, "txCount": 1, "postingCount": 1,
  "referenceCount": 1, "revertCount": 0}}`. Every other oracle test passes.
- The native acceptance run (`QUIET_PROBABILITY=0.05 RULES=smoke_tx run_native.sh 180`) was
  not run: the machine's root disk was full (0–100 MB free for most of the task, shared by
  the parallel agents). Run it once in 99.

---

<a id="16-signing"></a>

## 16 — Signing keys, signed writes, the requireSignatures window

Code: `HG/driver/src/rules/signing.rs` (setup hook, two rules, one exclusive action, the
key-tree model, unit tests). Rust-checked, except signed **tracked** writes, which the
oracle validates like any other write (`/write/begin` carries `"signed": true`).

| Item | Weight | What |
|---|---|---|
| setup hook `signing_setup` | – | `POST /pubkey` for the fixed root seed; unsigned `registerSigningKey{keyId:"root-<run>"}` keyed `r-<run>-signing-root`, retried until definitive; an Unauthenticated answer is resolved by listing the keys (our root listed → an earlier attempt committed; not listed → keys of an earlier run exist on a reused volume, and the whole family stays idle); then `createLedger r-sign-<run>` keyed and retried |
| `signing_keys` | 0.4 | drawn among register (32 drawn seed bytes, id `child-<run>-<N>`, signed by the root or a usable child; weight 0 at 24 usable children or 256 keys in the model), revoke (a usable non-root key, signed by the root with probability 3/4, else by itself or a usable ancestor; `cascade` drawn; an acked cascade is followed by a list), list (drawn route and page size, cursor paging ≤ 4 s and ≤ 512 pages; a repeated cursor makes the traversal inconclusive), unsigned register probe (pinned). Management sends are bounded at 6 s in all (2 s attempts), and a rule body starts nothing 10 s after it began |
| `signed_write` | 0.6 | 1/3: a forced tx to `r-sign-<run>` signed by a certainly revoked key, pinned (falls through to the tracked write when no key is revoked); else a forced tx (`world` → drawn address, positive amount, `COIN`) on a `ledgerPick`-drawn tracked ledger with the DESIGN §3.2 taper, `WritePlan.sign` = root, dispatched through the oracle |
| exclusive `require_signatures_window` | 1 | plan: probe node and client, management schedule and backoffs, write payload. run: skipped while `signatures_required` or maintenance is on; Drop guard; `setSigningConfig{true}` signed by root (8 s in all, last attempt included); if acked, Barrier → horizon → horizon wait on the node, unsigned and root-signed writes to `r-sign-<run>` pinned there (probes stop 13 s after the window started); if the enable got no definitive answer, it is re-sent under the **same key** until definitive (up to the same 13 s), so no delayed attempt can commit after the restore; restore `false` (one key for every attempt) with the rest of the 20 s (≥ 2 s); restore not definitive, or enable still unsettled → `signatures_required = true` + quiet-blocking job `signing-restore`, which first settles the enable (same key, until definitive), then retries the restore until definitive and clears the flag |

Probes that expect Unauthenticated/PermissionDenied (`probe_call`) are raw `Driver::call`s,
re-sent at most 3 times on unknown/Unavailable/DeadlineExceeded, judging the first success,
auth-class or definitive answer; anything else is inconclusive (no assertion). Every pinned
probe runs after `Barrier` → horizon → `wait_at_horizon(node, horizon, exact=false)`
(safety rule 4); a node that does not reach the horizon in 4 s skips the probe. Every call of
a pinned probe (discovery, Barrier, horizon polls, sends) is capped at the time left before
the probe's deadline, and none starts with less than 150 ms left.

## Key-tree model (`KeyTree`)

Keys: parent (= signer, as the server records), seed, public key, registration phase
(`InFlight | Acked(tick) | Unknown`), `tainted`. Revokes: target, cascade, start tick,
phase. A logical clock orders acks against sends. Server semantics followed
(`processor_signing.go`, `write_set.go`): register is an upsert with parent = signer; a
cascade walks stored parent links of keys still present (a key orphaned by an earlier
non-cascading revoke of its parent is not reached); revoking an absent key succeeds.

- **certain live** for a list started at tick `s` (judged with everything recorded until
  it ended): registration acked before `s`, no revoke on the key and no cascading revoke on
  any ancestor in any state. Unknown registrations are never expected.
- **certain absent**: registration acked, and a revoke on itself acked before `s`, or an
  acked-before-`s` cascade on an ancestor `G` such that the key and every key between it
  and `G` were acked before the cascade started and are untainted, and no key between them
  had a revoke started before the cascade was acked.
- **tainted**: registration acked while a cascading revoke on the signer or one of its
  ancestors had started (a lagging node may have admitted it after the cascade, as an
  orphan).
- **usable** (signers, revoke candidates): root, or acked, untainted, untouched by any
  revoke on itself or cascade on an ancestor. The root is never revoked.
- **pruning** (after each revoke): a certainly absent key with no recorded children moves,
  leaves first, to a `pruned` list (id, seed, tick) capped at 64; settled revokes on keys no
  longer in the tree are dropped. No remaining key's parent chain passes through a pruned
  key, so the derivations above are unchanged. A pruned key counts as certainly absent for
  traversals started at or after its pruning tick, and can still sign the revoked-key
  probe. Revokes are indexed per key, so the checks cost about keys × depth × revokes per
  key, not keys × revokes².

## Porting table

| Old source | Old assertion (kind: literal) | New | Where |
|---|---|---|---|
| MD/singleton_driver_signing_keys/main.go:55 | Sometimes: should be able to register signing key | verbatim; cond = the old `IsTolerated` (success or a non-definitive answer other than Unauthenticated/PermissionDenied); the register is now signed by the root or a child (unsigned registration only works once, at bootstrap) | rules/signing.rs exec_keys Register |
| MD/singleton_driver_signing_keys/main.go:76 | AlwaysOrUnreachable: registered key should appear in ListSigningKeys | verbatim, generalised to concurrency: every certainly-live key (above) is listed by a complete paged traversal | rules/signing.rs list_and_check |
| MD/singleton_driver_signing_keys/main.go:100 | Sometimes: should be able to revoke signing key | verbatim (cond as for register); revoke signed by the key itself or an ancestor, `cascade` drawn (old: always true, self-signed) | rules/signing.rs exec_keys Revoke |
| MD/singleton_driver_signing_keys/main.go:119 | AlwaysOrUnreachable: revoked key should not appear in ListSigningKeys | verbatim: no certainly-absent key is listed (evaluated when at least one exists) | rules/signing.rs list_and_check |

## New properties

| Literal | Kind | Reached by | Condition |
|---|---|---|---|
| `an unsigned key registration after bootstrap is refused as unauthenticated` | AlwaysOrUnreachable | signing_keys (probe) | pinned probe answers Unauthenticated |
| `a cascade revoke removes exactly the subtree` | AlwaysOrUnreachable | signing_keys (list after an acked cascade) | no key of the cascade's certain subtree is listed and every certainly-live key is |
| `a signed tracked write commits` | Sometimes | signed_write | definitive success of the root-signed tracked write |
| `a write signed by a revoked key is refused` | AlwaysOrUnreachable | signed_write (1/3) | pinned probe answers PermissionDenied |
| `an unsigned write is refused while signatures are required` | AlwaysOrUnreachable | require_signatures_window | pinned probe answers Unauthenticated |
| `a signed write is accepted while signatures are required` | Sometimes | require_signatures_window | root-signed pinned write succeeds |
| `the require-signatures window closes` | Reachable | require_signatures_window / signing-restore job | the restore is definitive (one call site per path, same literal) |

## Reachability

| Must-hit | Needs |
|---|---|
| should be able to register signing key | no faults |
| should be able to revoke signing key | no faults |
| a signed tracked write commits | no faults |
| a signed write is accepted while signatures are required | no faults (exclusive action; native run with `EXCLUSIVE_PROBABILITY`) |
| the require-signatures window closes | no faults (exclusive action) |

## Deviations

- Safety rule 1 says children are revoked with cascade only when they are leaves; the rule
  table and `a cascade revoke removes exactly the subtree` need non-leaf cascades. Cascades
  are drawn on any usable non-root key: no cascade can reach the root (it is every key's
  ancestor, never a descendant), so the bootstrap window never reopens; only the root signs
  tracked writes, so a cascade never turns them into PermissionDenied; the model handles
  orphans and uncertain cascades conservatively.
- Register is an upsert and revoke of an absent key succeeds, so no AlreadyExists/NotFound
  exists for them; the bootstrap tolerates `*ALREADY_EXISTS` anyway, and resolves an
  Unauthenticated answer by listing (see above), instead of looping forever.
- The ledger is `r-sign-<run>` (task spec), not `r-sign-<run>-<N>`.

## Open issues

- Admission runs on the leader (`RoutedController.Apply` forwards every Apply,
  `controller_routed.go:176`; `admit()` waits `waitLeaderReady` first, `admission.go:573`),
  so safety rule 4's premise (node-local `requireSignatures`) does not hold: the
  Barrier → horizon → wait on the pinned node adds latency and makes probes inconclusive
  when the drawn node is slow or partitioned, without adding soundness. Kept as specified;
  reported to 99.
- The earlier concern that `lastPersistedIndex` might move before the key store or the
  flag is closed: both change in `flushGlobal` (`write_set.go:721-752`), before
  `publishApplied` moves `lastPersistedIndex`.
- The settle-before-restore fix relies on a late duplicate of a definitively answered
  enable replaying its idempotency result instead of re-applying (`machine.go:1495-1530`).
  Not done (optional): an unsigned write after the restore to detect a flag left on.

## Requests to 99

- Safety rule 4 / pinning: see the first open issue; 99 may drop the pinning (probes on any
  route) to raise reachability of the window and revoked-key assertions.
- Cross-family: while `Shared.signatures_required` is set (the `signing-restore` job is
  running, rules are running again), only tracked writes are held back. Unsigned
  `send_untracked` writes of other families (`r-ns-*`, `r-probe-*`, scenario blocks) get
  Unauthenticated (non-definitive) until the job finishes, and any family that expects
  success on a Rust-checked ledger then could fail falsely or burn its budgets. 99 should
  check every family, or have those families skip while `signatures_required()` holds.

---

<a id="17-scenario-blocks"></a>

## 17 — Scenario blocks

Ports `MD/singleton_driver_scenario_blocks` (the loop in `W/internal/block/block.go`) and the
six registered `pkg/scenario` block groups (main-drivers §3.35, HEGEL-INVENTORY "Registered
block checklist" 26/26). Scenario ledgers (`marketplace`, `gaming`, `lending`, `billing`,
`treasury`, `dept-*`, …) are outside the oracle's partition (DESIGN §3); blocks stay unkeyed and
go through the client's `lib` route (production retry policy), as in the old driver.

Code:

- `W/bin/client/ext_block.go`: `POST /block-setup {"opId","group","timeoutMs"}` and
  `POST /block {"opId","name","values":["<u64>",…],"timeoutMs"}`. The groups come straight
  from `pkg/scenario` (`MarketplaceBlocks()` … `MultiLedgerPayrollBlocks()`); `W/internal/block`
  is not imported. The random source is a value list (`valueRandom`): `Uint64` pops, `Intn`
  and `Int63n` apply `scenario.RandFunc`'s one-draw modulo to a popped value, an exhausted list
  returns 0 and sets `exhausted`. Every RPC a block sends goes through a classifying
  `grpc.ClientConnInterface` wrapper (`client: every RPC error the client returns is
  classified`), and every completion is reported over UDP like `/op`. A block panic is answered
  as a non-local `Unknown` error, so the driver reports `block <name> failed`.
- `HG/driver/src/rules/blocks.rs`: setup hook `blocks_setup`, rule `block_run` (weight 1.5),
  and `scenario_blocks!`, which declares the 26 names and expands to one literal
  `check_reachable!` and one literal `check_unreachable!` per block.

## Old assertions

| Old source | Old assertion (kind: literal) | New | Where |
|---|---|---|---|
| W/internal/block/block.go:106 | Always: should be able to setup scenario | verbatim: `true` when a group's setup succeeds, `false` only on a definitive (non-transient) answer, as the old loop, which retried Unavailable without limit. Transient answers are retried within the 120 s hook budget; a group still unanswered then is not asserted by the hook: `block_run` makes one setup attempt per run for it (and runs none of its blocks) until it is set up | rules/blocks.rs `blocks_setup`, `block_run` |
| W/internal/block/block.go:135 | Unreachable: scenario block generation failed | dropped: no hegel callback runs in the client any more; the driver draws the values itself, and a block panic is reported as `block <name> failed` | – |
| W/internal/block/block.go:189-193 (`emitBlockSucceeded`, AssertRaw, 26 names) | Reachable: block `<name>` succeeded | verbatim, 26 literal `check_reachable!` calls produced by `scenario_blocks!` | rules/blocks.rs `block_run` |
| W/internal/block/block.go:195-198 (`emitBlockFailed`, AssertRaw, 26 names) | Unreachable: block `<name>` failed | verbatim, 26 literal `check_unreachable!` calls; classification **loosened**: (a) DeadlineExceeded, Canceled and client-local errors are transient (nothing), because the harness imposes a per-call deadline (`timeoutMs` 8000) that the old loop did not (DESIGN Appendix A C11); (b) NotFound with reason `METADATA_NOT_FOUND` is expected, because `marketplace/metadata`'s unkeyed delete of `last_seen` can find the key gone: two concurrent blocks on one address (the old loop was sequential), or the `lib` retry policy resending a delete whose first attempt committed; (c) reason `EXTERNAL_SERVICE_ERROR` (any code) and ResourceExhausted with a `WRITES_BLOCKED_*` reason are transient, as `W/internal/client.go` `IsTransient` and DESIGN §4.5 treat them (tmpfs volumes can fill); (d) Unauthenticated and PermissionDenied are transient, and `block_run` sends nothing while `signatures_required` is set, because task 16's signature window refuses unsigned Applies and a lagging follower can still refuse them after the window closes | rules/blocks.rs `block_run` |
| W/internal/checks.go:66 (via `block.CheckPostCommitVolumes`) | Always: post-commit volume entry has missing required fields | renamed: `a block's post-commit volumes are well formed` (Always), evaluated once per succeeded block, false at the first malformed entry | rules/blocks.rs `block_run` |
| W/internal/checks.go:72 | Always: post-commit volume input is invalid | merged into `a block's post-commit volumes are well formed` | rules/blocks.rs |
| W/internal/checks.go:78 | Always: post-commit volume output is invalid | merged into `a block's post-commit volumes are well formed` | rules/blocks.rs |
| W/internal/checks.go:14 (`CheckVolume` from `CheckPostCommitVolumes`) | Always: reported balance and volumes should be consistent | dropped: on post-commit volumes the balance is computed as `input − output` by the check itself, so the comparison is a tautology there | – |
| W/internal/apply.go:30 (via `ExtractCreatedTransaction`) | Unreachable: CreatedTransaction must carry its transaction payload | merged into `a block's post-commit volumes are well formed`: a `createdTransaction` log without its `transaction` (or with a null one) makes it false | rules/blocks.rs `malformed_pcv` |

The PCV check now covers every created transaction of a block's Apply response, not only the
first log as `ExtractCreatedTransaction` did (a block Apply with two orders, e.g.
`lending/repay`, is fully checked). This is under the new literal, not the old ones.

Not ported: the `SCENARIO` environment filter of the old driver (empty / one / comma-separated
scenarios). Every group is always set up and every block is eligible; hegel's equal-weight
choice among the 26 replaces it.

## New properties

| Literal | Kind | Where |
|---|---|---|
| `a block runs within its pre-drawn values` | Sometimes (`exhausted == false`), every answered `/block` | rules/blocks.rs `block_run` |
| `a block never runs out of its pre-drawn values` | AlwaysOrUnreachable (`exhausted == false`), every answered `/block`; the Sometimes above cannot detect exhaustion (no block uses more than 5 values, `TestEveryBlockUsesAtMostFiveValues`) | rules/blocks.rs `block_run` |
| `a block's post-commit volumes are well formed` | Always | rules/blocks.rs `block_run` |
| `the client program accepts every block request` | Unreachable (the client answered `/block` with a non-200) | rules/blocks.rs `post_block` |

## Reachability

| Must-hit | Reached by | Needs |
|---|---|---|
| `should be able to setup scenario` | `blocks_setup` (setup hook, 6 evaluations) | no faults |
| `block <name> succeeded` × 26 | `block_run`: every block is drawn with equal weight; the 16 values make a success likely once the group's ledger has funds. Blocks that read balances (`purchase`, `payout`, `buy_item`, `p2p_trade`, `clawback`, `disburse`, `repay`, `provision`, `write_off`, `charge`, `recognize`, `fx`, `vendor_pay`, `distribute`, `pay_salary`, `cost_alloc`, `revert`) skip until a funding block of the same group succeeded; in a long run every funding block is drawn many times | no faults (long enough run) |
| `a block's post-commit volumes are well formed` | any succeeded block | no faults |
| `a block runs within its pre-drawn values` | any answered `/block` (the largest block uses 5 of the 16 values) | no faults |

Native runs (one node, no faults):

- `RULES=block_run run_native.sh 180` (first version): all six groups set up, 24 of the 26
  `block … succeeded` hit, no `block … failed`; `subscription/charge` and
  `subscription/recognize` not hit.
- `RULES=block_run run_native.sh 300` (after the review fixes; `HEALTH_*_THRESHOLD=0.995`
  because the host disk was 96 % full and the default threshold blocked writes): all six
  groups set up (`should be able to setup scenario` true), **all 26** `block … succeeded`
  hit, including `subscription/charge` and `subscription/recognize`, no `block … failed`, no
  finding; `a block's post-commit volumes are well formed`, `a block runs within its
  pre-drawn values` and `a block never runs out of its pre-drawn values` true.

## Requests to 99

- None for code. A short native run may not hit all 26 `block … succeeded` Reachables (some
  depend on a prior funding block of the same group); task 99 should judge them on the long
  local run, not delete them.

---

<a id="18-restore"></a>

## 18 — Backup → restore → verify on separate restore targets

Restore into `restore-0` (and the optional joiner `restore-1`) while the source cluster keeps
running (restore.md §3.2). Code: `HG/driver/src/rules/restore.rs` and `rules/restore/*.rs`
(cycle, cut hook, stage/serve/verify jobs, digest parity, admission probes),
`W/bin/restore-agent` (process supervisor), `HG/restore/Dockerfile`, the `18-restore`
compose block.

## Old assertions

| Old source | Old assertion (kind: literal) | New | Where |
|---|---|---|---|
| `M/restore.go:150` | Sometimes: `singleton_driver_model: restore cycle completed` | renamed: `a restore round trip verifies with a non-empty delta` (Sometimes); the model driver had no direct comparison, the hegel cycle compares digests | `rules/restore/stage.rs` `verify_job` |
| `M/restore.go:51-89` (pause/drain) | – (no assertion) | the cut runs inside a quiet period (rules parked, resolvers drained) and is fenced by a second incremental | `restore.rs` `take_cut` |
| `MD/singleton_driver_incremental_backup/main.go:60` | Unreachable: `Backup (pre-incremental) returned unexpected error` | dropped: an unexpected backup error is classified by the client's `client: every RPC error the client returns is classified`; the cycle treats any non-success as inconclusive | – |
| `MD/singleton_driver_incremental_backup/main.go:93` | Unreachable: `IncrementalBackup returned unexpected error` | dropped (same reason) | – |
| `MD/singleton_driver_incremental_backup/main.go:108` | Reachable: `incremental backup completed` | subsumed by `a restore cut has a non-empty delta` (Sometimes, evaluated after every successful cut incremental) | `restore.rs` `take_cut` |
| `MD/singleton_driver_incremental_backup/main.go:123` | Unreachable: `second IncrementalBackup returned unexpected error` | dropped (client classification) | – |
| `MD/singleton_driver_incremental_backup/main.go:130` | AlwaysOrUnreachable: `incremental backup log sequence should not regress` | new literal (task 12 keeps the old one, DESIGN §9.2 rule 6): `a restore cut's incremental backup log sequence does not regress` | `restore.rs` `take_cut` (fence) |
| `MD/singleton_driver_incremental_backup/main.go:137` | AlwaysOrUnreachable: `incremental backup audit sequence should not regress` | new literal: `a restore cut's incremental backup audit sequence does not regress` | `restore.rs` `take_cut` (fence) |
| `W/internal/backup.go` `RetryBackup` | Sometimes: `backup recovery after destination busy succeeds` | dropped here: task 12 owns the busy-retry coverage of the main drivers; the cycle ports the retry (2 min from the first busy, drawn backoffs) without the literal | `restore.rs` `Cx::backup` |
| `W/restore-orchestrator.sh:199-205` (commit proof), `:213-215`, `:246-259`, `:218-224` | – (shell checks, no SDK assertions) | `finalize succeeded without durable proof` (Unreachable), `the restore marker records the base backup's applied index` (Always), agent waits for the child's exit before a restart | `stage.rs` `finalize_proof`, `W/bin/restore-agent` |
| `tests/antithesis/run_model_test.sh:150-270` (offline `store bootstrap`, no `--validate`) | – | offline path with and without `--validate` | `stage.rs` `offline` |
| `tests/antithesis/restore_orchestrator_test.go` (EN-3029) | – (Go test) | lost finalize answer + marker ⇒ committed; no marker ⇒ reset (unit tests) | `rules/restore/tests.rs` |

## New properties

Always (evaluated in the jobs, only on definite outcomes):
`the staged restore validates clean`, `a restore preview reports the cut sequence`,
`a restore preview lists the source ledgers` (subset: PreviewRestore also lists soft-deleted
ledgers), `the restore marker records the base backup's applied index`, one per digest section:
`restored ledgers match the source at the cut`, `restored accounts match the source at the cut`,
`restored transactions match the source at the cut`, `restored logs match the source at the cut`,
`restored stats match the source at the cut` (transactionCount and logCount only),
`restored audit matches the source at the cut`, `restored numscripts match the source at the cut`,
`restored prepared queries match the source at the cut`, `restored indexes match the source at the cut`,
`restored signing keys match the source at the cut`, `restored sinks match the source at the cut`,
`restored metadata schemas match the source at the cut`;
`post-restore admission probes behave as predicted`, `the restored store check finds no error`,
`the restore marker is consumed on the first normal boot`.

AlwaysOrUnreachable: `finalize before validate is refused as a failed precondition`,
`a restore cut's incremental backup log sequence does not regress`,
`a restore cut's incremental backup audit sequence does not regress`,
`a restored joiner syncs from the restored genesis` (only with `RESTORE_JOINER=1`).

Unreachable: `restore download finds a corrupt backup` (exact fragments of
`internal/infra/backup/restore.go`, `manifest.go` `ErrCheckpointFileCorrupted` and
`server_restore_download.go` "invalid manifest entry"), `a restore-target ledger process exits
unexpectedly`, `finalize succeeded without durable proof`.

Sometimes / Reachable: `restore base backup completes`, `a restore cut has a non-empty delta`,
`a restore cut fence holds`, `restore_cut completes within its quiet budget`,
`an online restore download succeeds`, `a canceled restore download is retried`,
`a finalize before validate is refused`, `an offline bootstrap with validation succeeds`,
`an offline bootstrap without validation succeeds`, `a restored cluster serves`,
`a restore round trip verifies with a non-empty delta`, `a chained restore verifies`,
`a restore cycle survives a restart of restore-0`, `a restore cycle completes within its wall budget`.

## Reachability (must-hit)

| Property | Reached by | Needs |
|---|---|---|
| `restore base backup completes` | `restore_base` | no faults |
| `a restore cut has a non-empty delta`, `a restore cut fence holds`, `restore_cut completes within its quiet budget` | `restore_cut` (weight 6 in BaseTaken; the delta seed guarantees S > F) | no faults (needs quiet periods: a run of ≥ 10 min) |
| `an online restore download succeeds`, `a restored cluster serves`, `a restore round trip verifies with a non-empty delta`, `a restore cycle completes within its wall budget` | `restore_step` (online path, 3/4 of plans) | no faults |
| `a canceled restore download is retried` | `restore_plan` cancel perturbation (1/4) | no faults (needs a download that is still running at the first poll; tiny backups can finish first — then not hit locally) |
| `a finalize before validate is refused` | `restore_plan` finalize-early perturbation (1/4) | no faults |
| `an offline bootstrap with validation succeeds`, `an offline bootstrap without validation succeeds` | offline plans (1/4, validate 1/2) | no faults |
| `a chained restore verifies` | chained plans (1/4) | no faults |
| `a restore cycle survives a restart of restore-0` | any cycle during which restore-0's container restarts | faults |
| `a restored joiner syncs from the restored genesis` | joiner plans | `RESTORE_JOINER=1` (AlwaysOrUnreachable, not must-hit) |

## Deviations from the spec

1. **Budget** (review finding 5): the 10-minute wall budget runs from the cut. From the base
   until the cut the cycle waits at most `cut_wait` = 15 minutes (quiet periods come every
   1.5–4 minutes, so a single budget from the base would expire most cycles before their cut).
   Expiry resets the agents and returns to Idle with `a restore cycle completes within its wall
   budget` (Sometimes) false; it never evaluates an Always.
2. **Chained restore** (review finding 4): the chain uses the per-generation namespace
   `restore-g<gen>` (empty `bucketId` on the restored cluster's backups, restore.md §4.3 step
   6), which the agent wipes (`POST /wipe-namespace`, restore-g* only) right before the
   chained full backup and again at the end of every cycle, so no object of an earlier chain
   can be read. The chain is restored back into restore-0 (cluster id `restore-g<gen>c`)
   rather than restore-1, which idles unless `RESTORE_JOINER=1` (DESIGN §2.4: 50 MB idle).
   The chained comparison reuses the per-section literals (same property: restored ==
   source at its cut).
3. **Admission probes** work on a rule-owned Rust-checked seed ledger
   (`r-restore-<run>-<n>`, 5 transactions + 1 revert): T1/T2/T4 + the revert of T2 are
   written before the base backup (checkpoint), T3 after it (delta). The probes check
   idempotent replays and conflicts of a checkpointed and a delta key, a reference conflict,
   reverts (already reverted / once), the balance boundary, the next transaction id, the
   next log sequence (`S + 1`), the HLC (`insertedAt` after the restored maximum) and the
   next ledger id. Skip-on-duplicate references and the Go-oracle fork (restore.md §5.4)
   are not implemented.
4. **Joiner**: the AlwaysOrUnreachable is evaluated on a *linearizable* digest pinned to
   restore-1 (a follower serves it only after catching up), so read lag can never fail it;
   a stale digest of restore-1 is taken for coverage only.
5. **Not compared**: `aggregates` (derived from accounts) and `checkpoints` (rows are kept
   but marked restored: an expected difference).
6. A broken fence or an empty delta drops the cut but keeps the base (a later quiet period
   cuts again on the same chain) instead of dropping the whole cycle.
7. Each long step (Cut → Staged, Staged → Serving, Serving → Verified) is one job; the seed
   writes run synchronously in `restore_base`/`restore_step` (rules are parked during quiet
   periods, so the seed never overlaps another family's quiet check).

## Requests to 99

- Check on the stack that a restored cluster's first boot appends no audit entry and no log
  (the `restored audit matches` and the `S + 1` probe assume it; incremental-restore-contract
  says technical-only proposals do not advance the chain).
- Check that the Antithesis compose launcher accepts `extra_hosts` on restore-0/1.
- Measure restore-0's RSS and volume during a chained cycle (DESIGN §2.4: 700 MB + 500 MB).

---

<a id="99-integration"></a>

## 99 — Integration

## Catalog audit (DESIGN §9.2 item 4)

Union of the SDK outputs of the local runs: the native smokes (600 s with
constant quiet/exclusive probabilities, 540 s with the default trigger, a
20-minute run) and the 45-minute compose runs (driver, oracle and client
catalogs). 364 must-hit assertions are cataloged; 12 were never hit
(a Sometimes counts only when true). None is unreachable by construction, so
none was removed; each is justified here:

| Assertion | Kind | Why not hit locally | Class |
|---|---|---|---|
| `the fault pause command exists in Antithesis` | Always | evaluated only under `ANTITHESIS_OUTPUT_DIR` | Antithesis only |
| `the fault pause command succeeds` | Always | evaluated only under `ANTITHESIS_OUTPUT_DIR` | Antithesis only |
| `a write attempt hits a preload miss` | Sometimes | `BALANCE_NOT_PRELOADED` / `PRELOAD_UNAVAILABLE` need a leader change or a cache miss under faults | faults |
| `a write attempt is blocked by clock skew` | Sometimes | `WRITES_BLOCKED_CLOCK_SKEW` needs Antithesis clock faults | faults |
| `a write attempt is refused as a stale proposal` | Sometimes | `STALE_PROPOSAL` needs a leadership change between plan and apply (more with per-node cache thresholds, see below) | faults + knobs |
| `a write attempt is refused while an index builds` | Sometimes | the only write-path `INDEX_BUILDING` is a `createQueryCheckpoint` while the audit projection rebuilds after a restart (`admission.go` `checkQueryCheckpointProjectionReady`): reachable, kept. Since run 3d8b6ff1…-67-0 it is an arm of `write::explore_attempt` (every tracked attempt, so the checkpoint creates of task 09 too); before, only index-family attempts were checked, which never get it | faults |
| `the leader reports a follower in snapshot progress` | Sometimes | a follower more than `RAFT_COMPACTION_MARGIN` entries behind while the leader reports it (`a follower reports an out-of-sync or snapshotting status` was hit once under local faults) | faults + knobs |
| `a restore cycle survives a restart of restore-0` | Sometimes | needs restore-0 killed mid-cycle | faults |
| `a cluster config change resets the cache epoch` | Reachable | needs per-node `CACHE_ROTATION_THRESHOLD`, which the compose file equalises until the server divergence below is fixed | knobs (disabled) |
| `a cluster config change was observed between quiet periods` | Sometimes | a leadership change that changes the persisted cluster config between two `cluster_config_convergence` runs | faults + knobs |
| `a chained restore verifies` | Sometimes | a drawn plan variant (chained, about one cycle in four); a cycle takes 5–10 minutes, so 45 minutes give a handful of cycles | no faults (long runs) |
| `an offline bootstrap with validation succeeds` | Sometimes | a drawn plan variant (offline × validate) | no faults (long runs) |

Every other must-hit was hit in at least one local run. The oracle image's
catalog contains no `singleton_driver_model:` literal and its symbol file
lists no `workload/internal` path, so the service does not link
`W/internal`.

## Requests to 99: dispositions

| From | Request | Disposition |
|---|---|---|
| 02 | check must-hits over the union of runs (hegel swarms rules) | done (union above). Root cause found: one `#[hegel::main]` test case fixed the swarm subset for the whole run; the driver now runs bounded test cases (below) |
| 03, 04, 14 | `draw::weighted` biased to the first weight | fixed in `draw.rs` (integer units, small-range `pick`); the families' local workarounds stay (harmless) |
| 03 | `ReadKind.Skip` for the Replay kind | not done: a verdict-precision improvement inside 00a's protocol, no false finding; left as is |
| 04 | mirror-promotion coverage needs 02/03 writes | reached in the merged build |
| 04 | maintenance window cool-down | not needed: no rule starved in the stack runs (no slow rule logged) |
| 05 | dedupe the accounts-filter builder with 06's | done: AggregateVolumes draws `filters::gen_account_filter_{free,indexed}` (typed leaves, has-asset); the aggregate validator now explains `FILTER_COMPILATION_ERROR` for filters the compiler must refuse and `AGGREGATE_OVERFLOW` when the model's fold overflows |
| 05 | O(log n) log accessors in `tests/oracle` | not done: the oracle's p99 stayed under 20 ms per read in the stack runs (`us` in the journal) |
| 06 | clean-EOF literals on shared ledgers; index-dependent literals | kept as 06 wrote them; the index-served literals are hit in the merged build |
| 07 | `HEGEL_HAVE_CHECKPOINTS=1` | set in the compose driver env and `run_native.sh` |
| 07 | remove the index-building write Sometimes if unreachable | kept: reachable (see the audit) |
| 08 | DESIGN §4.5 amendment (serialization beyond the ticket's life) | recorded here; DESIGN is outside the repository |
| 08 | dedupe with 06 | `has_asset` comes from `filters.rs`; the prepared-query filter keeps its own parameterised shapes (they feed 08's rejection fixtures) |
| 09 | `TestRejectionStages` with the server's checkpoint limit | done, plus fixture `09-checkpoint-limit-reached.json` |
| 10 | `audit failure entry with a retryable reason left unjudged` | downgraded to `AlwaysOrUnreachable(true, …)` (not must-hit): on the current server every named retryable FSM reason is never audited |
| 12 | a real "quiet period open" flag | `Shared.quiet_open` (Q0 to the end of the period), used by `quiet_maybe_open` |
| 12 | restore-target logs, checkpoint creation | reached in the merged build |
| 15 | fixture shape test vs the real usage provider | `mapPaths` treats provider content as data |
| 16 | other families during `signatures_required` | `Driver::guarded` skips every rule while it is set |
| 16 | drop the probe pinning | not done (kept as specified) |
| 17 | judge the 26 blocks on long runs | all 26 `block … succeeded` hit once rules re-swarm per test case |
| 18 | `extra_hosts`, restore RSS/volume, restored first boot | `snouty validate` accepts the compose file; restore-0 peaked at 398 MB RSS with a restored copy, its volume is wiped at the end of every cycle; the restore cycle verified with a non-empty delta in every compose run |

## Integration fixes (beyond the requests)

- `preparedcheck` event constants renamed (`EvListingOutsideModel`,
  `EvListUnexpectedError` clashed with 07/09 in the asserter AST test).
- The smoke rules, the `Smoke` read kind and its asserter are gone.
- The driver runs an unbounded sequence of bounded hegel test cases
  (`HEGEL_STEPS_PER_CASE`, default 2000) instead of one `#[hegel::main]` case:
  hegel records every choice of a case, so memory grew without bound, and the
  rule subset never changed within a run. `MALLOC_ARENA_MAX=2` keeps glibc
  from spreading freed memory over dozens of arenas.
- Restore admission probes read the served `{"data": micros}` timestamps.
- Task 15's and 18's listings ask the client for one item past the page (a
  page of exactly `maxItems` rows is reported incomplete).
- The oracle exits normally on SIGTERM (coverage counters) and journals each
  request's service time (`us`).
- `TestBlockEndpoints` (17) and `TestMuxServesOpAndExtensions` (00e) no
  longer run in parallel: both set the process-wide extension server, which
  made the block test fail about one run in six.
- A frozen `AGGREGATE_OVERFLOW` is explained before task 09's outcome matcher
  (which accepts only success and NotFound).
- The client shuts down gracefully (00e "Shutdown"). It closed its gRPC
  connections as soon as `ListenAndServe` returned, with RPCs still in flight,
  and reported grpc-go's own "grpc: the client connection is closing" as a
  remote `Canceled`: a stop fault on a client fired task 12's `Backup returned
  unexpected error` (run 3d8b6ff117f4ff08f7b9b40db1b5e083-67-0). The client now
  waits (bounded) for in-flight RPCs and never reports an outcome caused by its
  own close; the driver's backup rule (a remote `Canceled` is not tolerated) is
  unchanged.
- `minio` runs under a signal wrapper like `nats` (keeping `restart:
  on-failure`): as PID 1, minio exits 2 silently on a SIGTERM in its first
  ~0.3 s, before its handler is installed, which an Antithesis stop fault right
  after a kill-fault restart hit (same run). Both wrappers remap only when the
  trap fired (a flag), so a process killed by a signal the wrapper never got
  keeps its status (137) instead of a second `wait`'s 127.
- Quiet-period timing for short Antithesis timelines (run
  `3d8b6ff117f4ff08f7b9b40db1b5e083-67-0`: 12 compute hours, timelines up to
  about 8 minutes, `exact` drawn once and never finished, so none of the
  oracle's quiet checks fired). The trigger gaps are 60 s/240 s instead of
  150 s/480 s (DESIGN §4.10), the first two quiet periods of a process use
  30 s/120 s while no `exact` check has completed, and a gap counts from the
  end of the previous period's fault pause when that is later. `exact` weighs
  50 until it completes once in the process (and again after 3 quiet periods
  without a completion), 10 otherwise, instead of 4 (50 after 3).

## Server findings reported by task 99 (not masked in the checks)

1. **Per-node `CACHE_ROTATION_THRESHOLD` makes joining nodes diverge.** With
   thresholds 50/64/80 (DESIGN §8), the two nodes that join the bootstrap node
   never apply ledger-scoped entries: right after setup, `GetLog` sequence 3 is
   `createLedger o-…-l1` on node 1 and a different (cluster-scoped) entry on
   nodes 2 and 3, and every follower read of a tracked ledger answers "ledger
   not found". Equal thresholds on every node do not diverge (bisected on the
   compose stack). Node-local configuration must not change how a committed
   entry is applied (CLAUDE.md invariant 2). The compose file uses one
   threshold for now; restore 50/64/80 once fixed.
2. **`Backup` answers gRPC `Unknown` when its upload is cancelled during a
   leadership change** ("failed to propose backup terminal order … raft
   proposal dropped", then "upload multipart failed … context canceled" mapped
   to `unknown server error`). Both `client: every RPC error the client returns
   is classified` and task 12's `Backup returned unexpected error` fire; this
   is the error classification the old workload checked.

---
