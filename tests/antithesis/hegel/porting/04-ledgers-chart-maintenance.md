# 04 — ledgers, chart and enforcement, maintenance, ledger reads

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
| MD/parallel_driver_ledger_recreate:67 | Always: deleted ledger name remains permanently reserved | verbatim (code FailedPrecondition + reason LEDGER_DELETED on the oracle-tracked recreate) | rules/ledgers.rs `ledger_recreate_probe` |
| MD/parallel_driver_ledger_recreate:111 | Sometimes: ledger deletion predecessor write set recorded | dropped: the seed bulk is one tracked write; the oracle validates it | – |
| MD/parallel_driver_ledger_recreate:129 | Always: deleted ledger is hidden from ledger reads | verbatim | `ledger_recreate_probe` |
| MD/parallel_driver_ledger_recreate:138 | Always: deleted ledger hides predecessor transaction point reads | verbatim | `ledger_recreate_probe` |
| MD/parallel_driver_ledger_recreate:147 | Always: deleted ledger hides predecessor account point reads | verbatim | `ledger_recreate_probe` |
| MD/parallel_driver_ledger_recreate:161 | Always: deleted ledger exposes no predecessor transactions | verbatim | `ledger_recreate_probe` |
| MD/parallel_driver_ledger_recreate:173 | Always: deleted ledger exposes no predecessor accounts | verbatim | `ledger_recreate_probe` |
| MD/parallel_driver_ledger_recreate:183 | Always: deleted ledger rejects new transaction writes | verbatim (FailedPrecondition + LEDGER_DELETED) | `ledger_recreate_probe` |
| MD/parallel_driver_ledger_recreate:223 | Always: other ledger never exposes predecessor transactions | verbatim (first page of 100) | `ledger_recreate_probe` |
| MD/parallel_driver_ledger_recreate:237 | Always: other ledger never exposes predecessor account activity | verbatim (first page of 100) | `ledger_recreate_probe` |
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
  ~150 chart bulks). `chart_bulk` works around it by rotating its generator list by a drawn
  offset per slot (`chart::compose_chart`); `compose_bulk` users in other families (task 02's
  `tx_bulk`) are affected the same way. Suggest `weighted` use a small-range draw (e.g. a
  cumulative pick over an integer scale of 1000) or `chance` chains.
- Rule-owned ledgers accumulate as tombstones in the model (each `ledger_delete_race` /
  `ledger_recreate_probe` run leaves one or two deleted `o-<run>-own<N>`); at weight 0.4 each this
  is several hundred per hour. Watch `LiveLedgers`/candidate-search cost in long runs.
