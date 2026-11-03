# 18 — Backup → restore → verify on separate restore targets

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
| `a canceled restore download is retried` | `restore_plan` cancel perturbation (1/2 of online plans, after the first or second running poll; it fires only when the job then reports CANCELED and a later download succeeds, so a cancel that lands after the download finished does not count) | no faults (needs a download that is still running at the first poll; tiny backups can finish first — then not hit locally) |
| `a finalize before validate is refused` | `restore_plan` finalize-early perturbation (1/2 of online plans) | no faults |
| `an offline bootstrap with validation succeeds`, `an offline bootstrap without validation succeeds` | offline plans (1/4, validate 1/2) | no faults |
| `a chained restore verifies` | chained plans (1/4) | no faults |
| `a restore cycle survives a restart of restore-0` | any cycle during which restore-0's container restarts | faults |
| `a restored joiner syncs from the restored genesis` | joiner plans | `RESTORE_JOINER=1` (AlwaysOrUnreachable, not must-hit) |

## Deviations from the spec

1. **Budget** (review finding 5): the 10-minute wall budget runs from the cut. From the base
   until the cut the cycle waits at most `cut_wait` = 15 minutes (quiet periods come every
   3–8 minutes, so a single budget from the base would expire most cycles before their cut).
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
