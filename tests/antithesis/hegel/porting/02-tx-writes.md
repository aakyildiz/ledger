# 02 — Transaction writes: porting record

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
