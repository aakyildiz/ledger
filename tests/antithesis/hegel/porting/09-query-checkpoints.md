# Porting record — 09 query checkpoints

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
