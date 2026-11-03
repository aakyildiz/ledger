# 08 — Prepared queries: porting record

Sources: `M/prepared_queries.go`, `M/prepared_queries_read.go`, `M/validate.go:563`
(rejection coverage), `MD/parallel_driver_queries`, `MD/parallel_driver_update_query`,
`MD/parallel_driver_projection_alignment` (prepared half).

New code: `W/oraclecheck/preparedcheck/` (validators, view, begin hook, failure hook),
`W/bin/oracle/h_prepared.go` (asserters), `HG/driver/src/rules/prepared.rs` (rules and
order gens), `W/oraclecheck/testdata/rejections/08-*.json`, `HG/protocol-fixtures/08/`.

## Assertion map

| Old source | Old assertion (kind: literal) | New | Where |
|---|---|---|---|
| M/prepared_queries_read.go:67 | Unreachable: ListPreparedQueries returned unexpected error | oracle: ListPreparedQueries returned unexpected error | preparedcheck/list.go |
| M/prepared_queries_read.go:62 (validateLedgerNotFound) | Unreachable (core): ledger-scoped NotFound unexplained | core `ReadCtx.LedgerNotFoundExplained` | preparedcheck/list.go |
| M/prepared_queries_read.go:93 | Unreachable: prepared query listing outside model | oracle: prepared query listing outside model | preparedcheck/list.go |
| M/prepared_queries_read.go:365 | Reachable: prepared query page resumed in the opposite direction | oracle: prepared query page resumed in the opposite direction | preparedcheck/execute.go (follow-up page validated, direction differs from the remembered first page) |
| M/prepared_queries_read.go:448 | Reachable: aggregate on a non-accounts prepared query rejected | oracle: aggregate on a non-accounts prepared query rejected | preparedcheck/execute.go; now fired only when the rejection is also explained by a candidate (was: any error) |
| M/prepared_queries_read.go:489 | Reachable: reverse aggregate on a prepared query rejected | oracle: reverse aggregate on a prepared query rejected | preparedcheck/execute.go |
| M/prepared_queries_read.go:497 | Unreachable: reverse aggregate on a prepared query not rejected | oracle: reverse aggregate on a prepared query not rejected | preparedcheck/execute.go |
| M/prepared_queries_read.go:517 | Unreachable: prepared query execution returned unexpected error | oracle: prepared query execution returned unexpected error | preparedcheck/execute.go |
| M/prepared_queries_read.go:527 | Unreachable: prepared query cursor page size mismatch | oracle: prepared query cursor page size mismatch | preparedcheck/execute.go (expected = `match.EffectivePageSize(requested)`; the driver only requests sizes served unchanged) |
| M/prepared_queries_read.go:547 | Unreachable: prepared query page outside model | oracle: prepared query page outside model | preparedcheck/execute.go |
| M/prepared_queries_read.go:849 | Unreachable: prepared query aggregate outside model | oracle: prepared query aggregate outside model | preparedcheck/execute.go |
| M/prepared_queries_read.go:272 (`dbgf PQPARAM`) | debug trace | dropped: debug only; the spoiled-parameter path is covered by the window check | – |
| M/validate.go:563 | Reachable: malformed prepared-query filter refused at save | oracle: malformed prepared-query filter refused at save | preparedcheck/serialize.go `FailureCoverage` (any explained FILTER_COMPILATION_ERROR of a bulk that saves a filter) |
| MD/parallel_driver_queries/main.go:52 | Unreachable: prepared query creation returned unexpected error | oracle (every definitive write outcome is explained by `validateFailure`, core `oracle: bulk failure not explained by any serialization`; non-definitive outcomes are retried) | (00a) |
| MD/parallel_driver_queries/main.go:69 | Sometimes: should be able to execute prepared query | oracle: prepared query page validated (new Reachable on a validated successful page) | preparedcheck/execute.go |
| MD/parallel_driver_queries/main.go:74 | AlwaysOrUnreachable: prepared query should return a response | oracle: prepared query page outside model (a success without a cursor result is `WrongResult`, outside every candidate) | preparedcheck/execute.go |
| MD/parallel_driver_queries/main.go:88 | Unreachable: prepared query deletion returned unexpected error | oracle (as :52) | (00a) |
| MD/parallel_driver_update_query/main.go:59 | Unreachable: prepared query creation returned unexpected error | oracle (as above) | (00a) |
| MD/parallel_driver_update_query/main.go:85 | Sometimes: should be able to update prepared query | oracle: model outcome verified (00a) for committed updates; not re-asserted: the shared pool makes the outcome non-deterministic | (00a) |
| MD/parallel_driver_update_query/main.go:104 | AlwaysOrUnreachable: updated query should return a response | oracle: prepared query page outside model (as :74) | preparedcheck/execute.go |
| MD/parallel_driver_projection_alignment/main.go:84 | Unreachable: projection asset-index creation returned unexpected error | dropped here: the asset index on `o-<run>-l1` is task 07's `index_bootstrap` (its outcome is oracle-validated there) | – |
| MD/parallel_driver_projection_alignment/main.go:92 | Unreachable: projection asset index did not become ready | dropped: readiness is task 07's poller; an execution while the index is absent or building is gated legally by the oracle (`IndexedQueryOutcomeLegal`) | – |
| MD/parallel_driver_projection_alignment/main.go:124 | Unreachable: projection prepared-query creation returned unexpected error | oracle (definitive outcome explained, 00a) | (00a) |
| MD/parallel_driver_projection_alignment/main.go:152 | Unreachable: projection probe apply returned unexpected error | oracle (as above) | (00a) |
| MD/parallel_driver_projection_alignment/main.go:159 | Unreachable: projection probe apply succeeded without an audit log | oracle: core empty-commit check (`validateEmptyCommit`) | (00a) |
| MD/parallel_driver_projection_alignment/main.go:172 | Unreachable: projection-aligned prepared query returned unexpected error | oracle: prepared query execution returned unexpected error / page outside model | preparedcheck/execute.go |
| MD/parallel_driver_projection_alignment/main.go:178 | Reachable: projection-aligned prepared query succeeded | verbatim | rules/prepared.rs projection_probe (successful execute after both writes succeeded) |
| MD/parallel_driver_projection_alignment/main.go:182 | Unreachable: projection-aligned prepared query returned no cursor result | oracle: prepared query page outside model (`WrongResult`) | preparedcheck/execute.go |
| MD/parallel_driver_projection_alignment/main.go:196 | Always: projection-aligned read includes the acknowledged write | oracle: prepared query page outside model (the exact window on every candidate is a superset check: the acknowledged write precedes the read's ticket, so it is in every candidate base; a missing row is outside the window). Not re-asserted verbatim: on the shared `o-<run>-l1` the reserved account need not fall on the first page of 100, so the outcome is not deterministic | preparedcheck/execute.go |

## New properties

- `oracle: prepared query page validated` (Reachable): a successful LIST page matched a
  candidate (first or follow-up page).
- `oracle: prepared query aggregate validated` (Reachable): a successful AGGREGATE matched
  a candidate (exact bucket set and totals).
- `oracle: an update with a filter invalid for the stored target is refused` (Reachable):
  failure hook on an explained FSM-stage FILTER_COMPILATION_ERROR of an update (the
  definitive-unfrozen class of DESIGN §4.5).
- Begin hook `prepared-serialize` (`409 serialized`, not an assertion): a bulk touching a
  prepared query `(ledger, name)` while another in-flight or pending bulk touches it, or a
  bulk that creates and updates/deletes the same name.
- View `preparedQueries`: `{"queries":[{name,target,params:[{name,kind}]}]}`, `own-` names
  hidden unless `"owned": true`.

## Reachability

| Must-hit | Rule / path | Needs |
|---|---|---|
| oracle: prepared query page validated | prepared_execute (LIST, success) | no faults |
| oracle: prepared query aggregate validated | prepared_execute (AGGREGATE 1/3 on an ACCOUNTS query, success) | no faults |
| oracle: prepared query page resumed in the opposite direction | prepared_execute follow-up page, flip 1/4, page with `next` (page sizes 1–3 on ≥ 2 matching accounts) | no faults |
| oracle: reverse aggregate on a prepared query rejected | prepared_execute, AGGREGATE + reverse 1/8 | no faults |
| oracle: aggregate on a non-accounts prepared query rejected | prepared_execute, AGGREGATE target probe 1/8 on a TRANSACTIONS query | no faults |
| oracle: malformed prepared-query filter refused at save | prepared_write, malformed filter 1/16 (create: admission) | no faults |
| oracle: an update with a filter invalid for the stored target is refused | prepared_write `pq_update` with a malformed or target-specific filter on a stored query | no faults |
| projection-aligned prepared query succeeded | projection_probe, after both writes succeed; with the asset index absent the execute fails with an index error, so a success needs task 07's `index_bootstrap` | needs task 07 |

All of them are covered by Go unit tests (`preparedcheck_test.go`) and the in-process
integration test (`bin/oracle/h_prepared_integration_test.go`), and the Rust literal by
`rules::prepared::tests::projection_probe_creates_writes_and_executes`.

## Notes and deviations

- Protocol fixtures live in `HG/protocol-fixtures/08/` (the spec's unit-test bullet says
  `prepared/`; the ownership list and COMMON say `08/`). Written by
  `PROTOCOL_FIXTURES_WRITE=1 cargo test prepared::tests::protocol_fixtures`, checked by
  that test without the variable and decoded by `preparedcheck/fixtures_test.go`.
- A follow-up page carries `params: {"prevTicket": <ticket of the first page>}`; the
  oracle remembers validated pages whose cursor advertised more (last key, direction; 1024
  newest) and derives the model-side after-key itself, so the opaque cursor is never decoded
  and the driver supplies no model data. A follow-up whose previous page is unknown or whose
  cursor differs is not judged.
- Targets are ACCOUNTS and TRANSACTIONS only (spec), not LOGS as in `M/`.
- `own-pq-asset<N>` queries stay on `o-<run>-l1` (one per probe, weight 0.3); they are
  listed by ListPreparedQueries and compared like any other stored query.

## Requests to 99

- `projection-aligned prepared query succeeded` needs task 07's `index_bootstrap` (asset
  index on `o-<run>-l1`); excluded from this task's acceptance.
- Execute pages on TRANSACTIONS queries and account metadata filters mostly return
  index errors without task 07's indexes; with 07 merged they become served pages.
