# 11 — Quiet-period exact checks

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
  consumes one at completeness time.
- **Completeness bookkeeping.** Records are classified by scanning tickets
  (`RecordByTicket`) from the last scanned one; each owed record is checked once, at the
  first complete replay after it ended, and reported at most once.
- **After a fingerprint mismatch** the replay re-seeds from `Model()`, so the next quiet period
  checks the next interval instead of repeating one finding forever.
- **Frozen keys in the diff.** `GlobalState` exposes no accessor for the idempotency table,
  so the per-ledger diff of `oracle: audit replay state differs from the online model` names
  frozen keys only as the residual explanation when nothing else differs.
- **Purged zero cells.** A model cell may be absent from a served account only when its
  balance is zero and the address matches an ephemeral or transient type by the current chart
  (the server may have purged it; the model purges only cells a bulk touched); an account whose
  only state is such cells may be absent from the listing.
- **Per-node stats.** No per-node stats literal exists, so the node scope posts logs,
  transactions, accounts and info only; a node-scope stats part is refused (`protocol`).
- **Global stream** runs once per epoch, after every live tracked ledger's linearizable log
  listing matched the model; it checks served sequences against the learned global stream
  (`Model().Logs()`), per-ledger monotonicity, committed-bulk contiguity (`CommittedBulks` ×
  `CommittedLogs`) and log dates along the sequence.
- **Resync** test: the tracker's response cap (100 000) cannot be reached in a unit test, so
  the side table has a test-only `forceResync` flag (`resync_internal_test.go`).
- **Ledger reads** are paged by cursor with `pageSize` 500; the audit listing pages with the
  filter `AUDIT_FIELD_SEQUENCE ≥ cursor+1` and the cursor. The hook stops paging when the oracle
  answers `next.resync: true`.

## Requests to 99

- None of the must-hits need another family. A run with `RULES=smoke_tx,smoke_get_account`
  reaches all of them (the native acceptance run of this task).
- The disk of the shared build machine filled during this task; see the task report.
