# 12 — Cluster operations and server signals (Rust-only)

Rules in `HG/driver/src/rules/cluster_ops.rs`, log hooks in `rules/server_signals.rs`. No
oracle involvement: these RPCs never change tracked business state (oracle.md §1.5 item 8).
Sink and backup requests and the post-transfer witness write are untracked
(`send_untracked`, keys `r-<run>-k<N>`); the witness writes go to `r-ops-<run>`, created by
the setup hook `ops_setup`.

Error classes used below (`cluster_ops.rs`):

- **transient**: no answer (`unknown`), a client-local error, `Unavailable`,
  `DeadlineExceeded`, `Canceled`, reason `EXTERNAL_SERVICE_ERROR`, or `ResourceExhausted` +
  `WRITES_BLOCKED_DISK_FULL` (the old `IsTransient` set plus the caller-side codes).
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
| MD/singleton_driver_backup:52 | Unreachable: Backup returned unexpected error | verbatim (not busy, not transient, not `EXTERNAL_SERVICE_ERROR`) | backup job `run_chain` |
| MD/singleton_driver_backup:63 | AlwaysOrUnreachable: backup should produce files | verbatim | `run_chain` |
| MD/singleton_driver_backup:65 | Reachable: backup completed successfully | verbatim | `run_chain` |
| W/internal/backup.go:81 | Sometimes: backup recovery after destination busy succeeds | verbatim; evaluated on every attempt after the first busy answer (as the old `RetryBackup`), retry ≤ 2 min from the first busy, drawn backoffs 500–5000 ms | `backup_call` |
| MD/singleton_driver_incremental_backup | Unreachable: Backup (pre-incremental) returned unexpected error | merged into `Backup returned unexpected error` (the full backup of a chain is the same call with the same assertions) | `run_chain` |
| MD/singleton_driver_incremental_backup | Reachable: incremental backup completed | verbatim | `run_chain` |
| MD/singleton_driver_incremental_backup | Unreachable: IncrementalBackup returned unexpected error | verbatim (busy, transient, `EXTERNAL_SERVICE_ERROR`, no full checkpoint tolerated) | `run_chain` |
| MD/singleton_driver_incremental_backup | Unreachable: second IncrementalBackup returned unexpected error | merged into `IncrementalBackup returned unexpected error`: one incremental per chain; the "second" is the next chain's incremental | `run_chain` |
| MD/singleton_driver_incremental_backup:130 | AlwaysOrUnreachable: incremental backup log sequence should not regress | verbatim; compared with the highest `lastLogSequence` of the incrementals that completed before this one was sent (driver-global) | `run_chain` |
| MD/singleton_driver_incremental_backup:137 | AlwaysOrUnreachable: incremental backup audit sequence should not regress | verbatim, same floor scheme | `run_chain` |
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
| Always | a full backup's last log sequence covers every write acknowledged before it started | `run_chain` (`lastLogSequence ≥ Shared.acked_seq_high_water` recorded in the rule) |
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
| Unreachable | a server logs an invariant red flag | server_signals.rs: records whose `msg` starts with `SENTINEL DIAG:`, `CACHE COHERENCE:`, `POST-COMMIT VOLUME ASSERTION FAILED`, `AGGREGATED VOLUME BALANCE CHECK FAILED`, `PERSISTED VOLUME UPDATE at invariant violation`, `TRANSIENT VOLUME at invariant violation`, `Preload boundary mismatch`, `preload coverage miss: read of undeclared key`, `Pebble lags WAL snapshot`, `Terminal Raft command failure; stopping node`, `Leadership gained Ready has no entries`; ledger and restore-target lines, from boot |

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
| a full backup's last log sequence covers every write acknowledged before it started | backup job | no faults (needs minio) |
| backup recovery after destination busy succeeds | backup job, two concurrent chains (1/6) | no faults (needs minio) |
| incremental backup completed | backup job (1/3 of chains) | no faults (needs minio) |
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

## Requests to 99

- `the driver receives restore-target logs` needs task 18's restore targets forwarding logs
  (exclude from 12's acceptance).
- `a node creates a main store query checkpoint` needs task 09's `createQueryCheckpoint`
  orders.
- Backups use the server's default namespace (`bucketId` omitted, as the old drivers did);
  task 18 uses `hegel-restore`. If task 18 ever sends Applies to restore targets through
  `Driver::call`, those responses also raise `Shared.acked_seq_high_water`; the new
  full-backup Always would then compare against another cluster's sequences. 99 should
  check that restore-target Applies do not go through `note_apply`, or keep a separate
  high-water.
- Backup and CheckStore jobs are limited to one queued-or-running job each. Together they
  can still occupy both job threads for minutes (a backup chain with busy retries ≤ 2 min
  plus 90 s calls; CheckStore ≤ 60 s), delaying other families' jobs such as task 04's
  quiet-blocking maintenance disable. If 99 sees that, a dedicated job thread for
  long Rust-only jobs is the fix (00c's `jobs.rs`, not owned by 12).
