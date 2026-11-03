# 00 — Foundation (Rust core, task 00c)

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
| a write attempt is refused as a stale proposal | Sometimes | write.rs `explore_attempt`, run by clients.rs `call_with_op` on every `Apply` answer (tracked attempts, `send_untracked`, direct Applies) |
| a write attempt hits a preload miss | Sometimes | write.rs `explore_attempt` (every `Apply` answer, as above) |
| a write attempt is blocked by clock skew | Sometimes | write.rs `explore_attempt` (every `Apply` answer, as above) |
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
| a write attempt is refused as a stale proposal | any `Apply` the driver sends through `/op` | faults (+ heterogeneous cache knobs, DESIGN §8) |
| a write attempt hits a preload miss | 13's `ns_unresolved`: a keyed send whose dependency resolution fails after a mutable read gets `PRELOAD_UNAVAILABLE` (triage of run 076d255c…-67-0). `BALANCE_NOT_PRELOADED` cannot be produced from outside the server: a volume read the admission plan did not declare fails as `COVERAGE_MISS`, so the server raises it only when a store read returns neither a volume nor an error (`processor_posting.go` `readVolumeOrZero`) | no faults |
| a write attempt is blocked by clock skew | any `Apply` the driver sends through `/op` | faults (a clock offset between nodes; Antithesis clock faults that move every node together do not produce it) |
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
