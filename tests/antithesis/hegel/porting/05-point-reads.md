# Porting record — 05 point reads

Task 05 ports the singleton model's point reads (GetAccount, GetTransaction,
AggregateVolumes, GetLedgerStats, GetLog by global sequence) into the oracle
(`W/oraclecheck/pointcheck`, asserters in `W/bin/oracle/h_point.go`) and adds
five Rust rules in `HG/driver/src/rules/point_reads.rs`, every read through a
read ticket, linearizable, on a drawn route:

| Rule | Weight | Kind | Views |
|---|---|---|---|
| `get_account` | 4 | `GetAccount` | `seek(volumes \| accountMeta)` |
| `get_transaction` | 3 | `GetTransaction` | `ledger` |
| `aggregate` | 1.5 | `AggregateVolumes` | `ledger`, `stamps` (both `includeDeleted`) |
| `ledger_stats` | 1 | `GetLedgerStats` | `ledger` (`includeDeleted`) |
| `get_log` | 1 | `GetLog` | `logSeqs` (registered here) |

Generator intent (absent ledger, colour collapse, model-absent account,
learned/unassigned sequence) is recomputed by the validators from the request
and the read's snapshot; the hint is coverage-only.

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
| `oracle: GetAccount probing a model-absent account` | get_account, pool probe 5% (and empty-seek reads) | no faults |
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
must-hit above with zero findings.

## Rejection fixtures

None: the five rules only read; they produce no write rejection shape.

## Requests to 99

- `oracle: aggregate gated on a missing index`: reached locally because no
  index exists without task 07; re-check once 07 is merged (the indexed class
  then also produces served results on indexed keys).
- The local minimal accounts-filter builder in `point_reads.rs` (address
  prefix/exact, metadata exists, and/or/not) duplicates part of task 06's
  generator; 99 may replace it with 06's builders.
- `matchtest.Render*` (00b) return model-owned slices and pointers (postings,
  timestamps, metadata): a test that mutates a rendered response in place
  mutates the model. `pointcheck` tests clone before mutating; other read
  families should too.
