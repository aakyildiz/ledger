# 14 — Rust probes: porting record

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
| MD/eventually_correct/main.go:191-222 (`checkAccountBalances`, W/internal/checks.go:15-56) | Reachable: should be able to get account; Unreachable: GetAccount returned unexpected error; Always: reported balance and volumes should be consistent, account volume entry has missing required fields, account volume input/output/balance is invalid | `GetAccount returned unexpected error` verbatim (sampled cross-check reads); the per-user volume checks are oracle (tracked ledgers, task 05 point reads); dropped for foreign ledgers | rules/probes.rs `observe_ledger` |
| MD/eventually_correct/main.go:258,266 | Always(false): cross-check: account volume has missing required fields / has invalid typed amount | dropped: listed balance presence/validity is covered by the two double-entry literals; input/output of tracked accounts are oracle-checked | – |
| MD/eventually_correct/main.go:292 | Unreachable: should get requested volumes | dropped (subsumed): a bucket missing on one side counts as balance 0 there and a difference fires `list/get balance divergence persisted past quiescence` | – |
| MD/eventually_correct/main.go:302,308 | Always(false): cross-check: GetAccount volume balance is absent / invalid | verbatim, as `check_always!(…)` on every sampled GetAccount bucket | rules/probes.rs `evaluate_ledger` |
| MD/eventually_correct/main.go:331 | Unreachable: list/get balance divergence persisted past quiescence | verbatim (only under a quiescent fence; otherwise re-read up to 2 times, then no verdict) | rules/probes.rs `evaluate_ledger` |
| MD/eventually_correct/main.go:343 | Reachable: list/get balance pair verified matching | verbatim | rules/probes.rs `evaluate_ledger` |
| MD/eventually_correct/main.go:367 | Reachable: eventually_correct found no list/get balance pairs to compare | dropped: coverage note only; `can check all volumes for consistency` details carry `pairs` | – |
| MD/eventually_correct/main.go:372 | Reachable: can check all volumes for consistency | verbatim (per checked ledger) | rules/probes.rs `evaluate_ledger` |
| MD/eventually_correct/main.go:383-404 | AlwaysOrUnreachable: list metadata key should exist in getaccount; list metadata value should match getaccount; metadata key count should match between list and get | verbatim, under the fence, for up to 20 sampled accounts; values compared as whole typed `MetadataValue`s (old: `stringValue` only, per the task spec's "whole metadata map") | rules/probes.rs `evaluate_ledger` |
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
