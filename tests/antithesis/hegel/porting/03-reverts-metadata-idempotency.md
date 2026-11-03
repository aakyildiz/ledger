# 03 — Reverts, metadata, idempotency replays and conflicts

Rust: `HG/driver/src/rules/{reverts,metadata,idempotency}.rs`. Go:
`W/oraclecheck/replaycheck/` (package `replaycheck`) and `W/bin/oracle/h_replay.go`.

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
- `revert` gen: no reverts on a ledger with 4000 transactions or more (the hard end of the
  DESIGN §3.2 taper; `applies` takes no draw, so the 1/2 and 1/8 bands are not applied).
- `meta_bulk` composes slot by slot: it draws the gen with `fair_weighted` (integer units
  drawn with `draw::pick`) among the gens whose `applies` holds, then calls `compose_bulk`
  over that single gen (which makes the opt-in draw and the call). `draw::weighted` (00c)
  picks the first positive weight about 96% of the time (measured: 1914/2000 for weights
  3/3/1.5/1), so plain `compose_bulk` over the four gens produced reverts only and the
  skip probes were never reached (see Requests to 99).
- The `Replay` validator also skips non-transient non-definitive outcomes (Internal,
  Aborted, `STALE_PROPOSAL`, …): the rule sends a single attempt, and only definitive
  outcomes carry a verdict.

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

## Native run

`RULES=meta_bulk,revert_concurrent,revert_twice,idem_replay,idem_conflict,meta_read_after_write,smoke_tx run_native.sh 180`:
PASS, 0 findings, every must-hit of the table above hit (≈127 tracked ops/s). Without
`smoke_tx` the run also passes with the two oracle replay must-hits hit, but the two
revert-side must-hits stay idle (see Requests to 99).
