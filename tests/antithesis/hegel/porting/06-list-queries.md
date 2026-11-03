# 06 — List queries (ListAccounts, ListTransactions, ListLogs, filters, cursors)

Rust: `HG/driver/src/rules/lists.rs` (rules), `rules/filters.rs` (filter, shape, cursor and
page-token generation). Go: `W/oraclecheck/querycheck/` (validators), `W/bin/oracle/h_query.go`
(read kinds `ListAccounts`, `ListTransactions`, `ListLogs`, asserters, coverage probes).
Fixtures: `HG/protocol-fixtures/06/page_tokens.json` (written by the Go test
`TestPageTokenFixture`, checked by the Rust `page_token_matches_go_fixture`) and
`HG/protocol-fixtures/06/filters/{builders,generated}.json` (written by the Rust test
`filter_fixtures` with `UPDATE_FIXTURES=1`, unmarshalled by the Go test
`TestFilterFixtures_ClientAccepts` with the client's strict protojson options; the builder
cases also pin the oracle's refused-filter classification).

Rules: `list_accounts` (2), `list_transactions` (3), `list_logs` (1.5), `list_traverse` (0.7).
Every draw happens before `/read/begin`: filters are pre-drawn plans whose view-dependent
leaves (declared field types from `ledger`, a reference from `seek(txByRef)`, transaction
stamps and log dates from `stamps`, the log head from `ledger.logCount`) carry pre-drawn picks
resolved against the views of the same ticket; there are no post-begin draws at all.

## Old assertions

| Old source | Old assertion (kind: literal) | New | Where |
|---|---|---|---|
| M/queries.go:101 | Reachable: page size left to the server default | `oracle: page size left to the server default` | h_query.go (querycheck `pageSizeEvents`) |
| M/queries.go:103 | Reachable: page size clamped to the server maximum | `oracle: page size clamped to the server maximum` | h_query.go |
| M/queries.go:184 | Reachable: has-asset precision overflow rejected | `oracle: has-asset precision overflow rejected` | h_query.go (accounts) |
| M/queries.go:205 | Unreachable: ListAccounts returned unexpected error | `oracle: ListAccounts returned unexpected error` | h_query.go |
| M/queries.go:218 | Unreachable: target-invalid account query returned results | `oracle: target-invalid account query returned results` | h_query.go |
| M/queries.go:377 | Unreachable: ListTransactions returned unexpected error | `oracle: ListTransactions returned unexpected error` | h_query.go |
| M/queries.go:387 | Unreachable: malformed transaction cursor returned results | `oracle: malformed transaction cursor returned results` | h_query.go |
| M/queries.go:396 | Unreachable: unsupported builtin uint field returned results | `oracle: unsupported builtin uint field returned results` | h_query.go |
| M/queries.go:405 | Unreachable: target-invalid transaction query returned results | `oracle: target-invalid transaction query returned results` | h_query.go |
| M/queries.go:441,443 | Reachable: target-invalid query rejected on accounts / on transactions | `oracle: target-invalid query rejected on accounts` / `… on transactions` | h_query.go |
| M/queries.go:449 | Unreachable: target-invalid query returned unexpected error | `oracle: target-invalid query returned unexpected error` | h_query.go |
| M/queries.go:499 | Reachable: unsupported builtin uint field rejected | `oracle: unsupported builtin uint field rejected` (not in the spec's list; reached by the 1/24 probe, so kept as a must-hit) | h_query.go |
| M/queries.go:504 | Unreachable: unsupported builtin uint field returned unexpected error | `oracle: unsupported builtin uint field returned unexpected error` | h_query.go |
| M/queries.go:550 | Unreachable: account query outside model | `oracle: account query outside model` | h_query.go |
| M/queries.go:594 | Unreachable: transaction query outside model | `oracle: transaction query outside model` | h_query.go |
| M/cursors.go:131 | Reachable: malformed transaction cursor rejected | `oracle: malformed transaction cursor rejected` | h_query.go |
| M/cursors.go:133 | Reachable: malformed log cursor rejected | `oracle: malformed log cursor rejected` | h_query.go |
| M/cursors.go:135 | Reachable: malformed audit cursor rejected | not ported here (audit cursor: task 10) | – |
| M/cursors.go:141 | Unreachable: malformed cursor returned unexpected error | `oracle: malformed cursor returned unexpected error` (transactions and logs; task 10 owns the audit arm under its own literal) | h_query.go |
| M/rejected_filter.go:125,127,129 | Reachable: refused account / transaction / log filter rejected | `oracle: refused account filter rejected` / `… transaction …` / `… log …` | h_query.go |
| M/rejected_filter.go:142 | Unreachable: refused filter returned unexpected error | `oracle: refused filter returned unexpected error` | h_query.go |
| M/rejected_filter.go:163 | Unreachable: refused filter returned results | `oracle: refused filter returned results` | h_query.go |
| M/indexes.go:253 | Unreachable: asset-index account query returned unexpected error | `oracle: asset-index account query returned unexpected error` | h_query.go |
| M/indexes.go:264 | Unreachable: asset-index query filter is not a bare has-asset leaf | `oracle: asset-index query filter is not a bare has-asset leaf` | h_query.go |
| M/indexes.go:310,312 | Reachable: asset-index account query served results / gated | `oracle: asset-index account query served results` / `… gated` | h_query.go (needs task 07) |
| M/indexes.go:356 | Unreachable: asset-index account query outside model | `oracle: asset-index account query outside model` | h_query.go |
| M/indexes.go:927 | Unreachable: indexed transaction query returned unexpected error | `oracle: indexed transaction query returned unexpected error` | h_query.go |
| M/indexes.go:953,955,957 | Reachable: indexed transaction query served results / kind-mismatched field query rejected on transactions / indexed transaction query gated | `oracle: …` same suffixes | h_query.go (served, kind-mismatched: need task 07) |
| M/indexes.go:1018 | Unreachable: indexed transaction query outside model | `oracle: indexed transaction query outside model` | h_query.go |
| M/indexes.go:1310 | Unreachable: indexed account query returned unexpected error | `oracle: indexed account query returned unexpected error` | h_query.go |
| M/indexes.go:1336,1339,1342,1344 | Reachable: indexed account query served results / has-asset under a combinator served results / kind-mismatched field query rejected on accounts / indexed account query gated | `oracle: …` same suffixes | h_query.go (all but gated need task 07) |
| M/indexes.go:1399 | Unreachable: indexed account query outside model | `oracle: indexed account query outside model` | h_query.go |
| M/queries_logs.go:686-693 | Unreachable: malformed log cursor returned results | `oracle: malformed log cursor returned results` | h_query.go |
| M/queries_logs.go:680 | Unreachable: ListLogs returned unexpected error | `oracle: ListLogs returned unexpected error` | h_query.go |
| M/queries_logs.go:741 | Unreachable: log page violates its own ordering | `oracle: log page violates its own ordering` | h_query.go |
| M/queries_logs.go:758 | Unreachable: log page carries a response signature | **renamed** `oracle: a log of an unsigned tracked bulk carries a response signature`: signing exists now (task 16 signs some tracked bulks with the root key), so a signature is a finding only on a log whose committing bulk the oracle knows (`CommittedBulks` → `Record(wid).Signed`) and knows to be unsigned | h_query.go (querycheck `signedRowOfUnsignedBulk`) |
| M/queries_logs.go:780,782,785 | Reachable: log query gated on a missing index / log query served results / reverse log query served results | `oracle: …` same suffixes | h_query.go |
| M/queries_logs.go:808 | Unreachable: log query outside model | `oracle: log query outside model`; the diagnostic re-reads (`recheckLogIDs`, `recheckLogKinds`) moved to Rust (`lists.rs recheck_logs`, printed after a `log query outside model` verdict) because the oracle never dials the cluster | h_query.go |
| M/coverage.go:110-178 | Raw Sometimes: `coverage index <canonical> served a model-verified page` (one per workload index), `coverage a metadata-field index served a model-verified page on accounts|transactions`, `coverage a query was served while a retype window was open` | `oracle: coverage …` same suffixes, registered with `RegisterCoverage` in h_query.go, evaluated on every index-backed validated outcome (needs task 07 to be true) | h_query.go, querycheck/coverage.go |
| M/coverage.go (checkpoint, deletion, promotion probes) | Raw Sometimes: checkpoint / ledger deletion / mirror promotion probes | not ported here (tasks 09 and 04) | – |
| MD/parallel_driver_list_completeness/main.go:98 | Sometimes: list completeness write set recorded | dropped: the traversal no longer owns a write set; tracked writes are judged by the oracle | – |
| MD/parallel_driver_list_completeness/main.go:218 | Always: clean-EOF list pagination respects the requested page size | verbatim; compares each page with `EffectivePageSize(requested)` (0 → 100, > 1000 → 1000), ListTransactions and ListLogs | lists.rs `check_traversal` |
| MD/parallel_driver_list_completeness/main.go:221 | Always: clean-EOF list pagination contains every acked transaction | oracle: subsumed by the per-page window validation (`oracle: transaction query outside model` on every page of the traversal) | h_query.go |
| MD/parallel_driver_list_completeness/main.go:224 | Always: clean-EOF list pagination yields no duplicate transaction IDs | verbatim | lists.rs `check_traversal` |
| MD/parallel_driver_list_completeness/main.go:227 | Always: clean-EOF list pagination yields strictly increasing transaction IDs | verbatim; evaluated only on ascending traversals (`reverse=true` on transactions) | lists.rs `check_traversal` |
| MD/parallel_driver_timestamp_order/main.go:117 | Sometimes: timestamp order write set recorded | dropped (no owned write set; backdated timestamps are task 02's) | – |
| MD/parallel_driver_timestamp_order/main.go:220 | Always: ListLogs pagination yields strictly increasing log sequences | **renamed** `ListLogs pagination yields strictly monotone log sequences in the requested direction` (the old literal assumed ascending; `reverse=true` on logs is descending) | lists.rs `check_traversal` |
| MD/parallel_driver_timestamp_order/main.go:223 | Always: effective timestamps are non-decreasing with log sequence | not ported here (task 11, global stream check) | – |
| MD/parallel_driver_list_transactions/main.go:36 | Sometimes: should be able to create tx for list test | dropped: tracked writes are task 02's and judged by the oracle | – |
| MD/parallel_driver_list_transactions/main.go:74,90,142 | Unreachable: ListTransactions should not fail / ListTransactions stream returned unexpected error / reverse ListTransactions stream returned unexpected error | oracle: `oracle: ListTransactions returned unexpected error` (transient classes are skipped as the old driver tolerated them) | h_query.go |
| MD/parallel_driver_list_transactions/main.go:112,113,153 | AlwaysOrUnreachable: ListTransactions should return at least one transaction / … should contain the just-created transaction / reverse ListTransactions should return results | oracle: the exact window check (`oracle: transaction query outside model`) requires every committed transaction a base holds | h_query.go |
| H/main.rs:322, 715 | Always: a list page never exceeds the requested page size | renamed into `clean-EOF list pagination respects the requested page size` (same check against the effective size) and the oracle's per-page check | lists.rs |
| H/main.rs:718 | Always: paging never returns an item twice | `clean-EOF list pagination yields no duplicate transaction IDs` (transactions) and `ListLogs pagination yields strictly monotone log sequences in the requested direction` (logs) | lists.rs |
| H/main.rs:723 | Always: list results are strictly ordered across pages | `clean-EOF list pagination yields strictly increasing transaction IDs` (ascending transactions) and the ListLogs monotone check; the oracle checks every page's order | lists.rs |
| H/main.rs:727 | Sometimes: a listing spans several pages | verbatim | lists.rs `check_traversal` |

## New properties

- `oracle: a log of an unsigned tracked bulk carries a response signature` (Unreachable; renamed, see above).
- `ListLogs pagination yields strictly monotone log sequences in the requested direction` (Always; renamed).

No other new literal. All oracle literals are `oracle: ` + the `M/` suffix.

## Reachability of must-hit assertions

| Literal | Rule / draw | Needs |
|---|---|---|
| `oracle: malformed transaction cursor rejected` | list_transactions, malformed cursor 1/16 | no faults |
| `oracle: malformed log cursor rejected` | list_logs, malformed cursor 1/16 | no faults |
| `oracle: refused account filter rejected` | list_accounts, rollFilterShape 1/8 | no faults |
| `oracle: refused transaction filter rejected` | list_transactions, rollFilterShape 1/8 | no faults |
| `oracle: refused log filter rejected` | list_logs, rollFilterShape 1/8 | no faults |
| `oracle: target-invalid query rejected on accounts` | list_accounts 1/16 | no faults |
| `oracle: target-invalid query rejected on transactions` | list_transactions 1/16 | no faults |
| `oracle: indexed account query gated` | list_accounts, `undeclared-` key 1/8 (INDEX_NOT_FOUND) | no faults |
| `oracle: indexed transaction query gated` | list_transactions, `undeclared-` key, reference/date/address leaves without indexes | no faults |
| `oracle: log query gated on a missing index` | list_logs, date leaf without the log-date index | no faults |
| `oracle: log query served results` | list_logs, index-free filters | no faults |
| `oracle: reverse log query served results` | list_logs, reverse 1/2 | no faults |
| `oracle: page size left to the server default` | every list rule, page size 1/16 | no faults |
| `oracle: page size clamped to the server maximum` | every list rule, page size 1/32 | no faults |
| `oracle: unsupported builtin uint field rejected` | list_transactions 1/24 | no faults |
| `clean-EOF list pagination respects the requested page size`, `… no duplicate transaction IDs`, `… strictly increasing transaction IDs`, `ListLogs pagination yields strictly monotone …` | list_traverse | no faults (Always) |
| `a listing spans several pages` | list_traverse, small page sizes | no faults |
| `oracle: kind-mismatched field query rejected on accounts` / `… on transactions` | list_accounts / list_transactions, 1/12 | needs task 07 |
| `oracle: indexed account query served results`, `oracle: indexed transaction query served results`, `oracle: has-asset under a combinator served results` | list rules | needs task 07 |
| `oracle: asset-index account query served results` | list_accounts has-asset 1/6 | needs task 07 |
| `oracle: asset-index account query gated` | list_accounts has-asset 1/6 (INDEX_NOT_FOUND) | no faults (hit in the native run), kept in the 07 block as the spec lists it |
| `oracle: has-asset precision overflow rejected` | list_accounts has-asset 1/16 × 1/6 | no faults (hit in the native run) |
| `oracle: coverage …` (Raw probes) | every index-backed read | needs task 07 |

## Deviations from the task spec

- **kind-mismatched Reachables need task 07.** The server checks a Field leaf's index
  (`requireIndexReady`) before coercing the condition kind (`internal/query/compile.go`
  `resolveFieldMetadataCtx`), and nothing in this task creates a metadata index, so a
  kind-mismatched leaf answers INDEX_NOT_FOUND (counted as "indexed … gated") until task 07
  creates indexes. Both literals are in the "reachable only with task 07 merged" asserter block.
- **Two extra Reachables** ported from `M/` that the spec's list omits: `oracle: unsupported
  builtin uint field rejected` and `oracle: has-asset precision overflow rejected`; both were
  hit without faults in the native run.
- **`asset-index account query gated`** stays in the 07 block as the spec says, although an
  INDEX_NOT_FOUND on a bare has-asset leaf already reaches it without indexes (hit in the
  native run).
- **Fixture paths.** The spec names `HG/protocol-fixtures/filters/*.json`; COMMON.md lets a
  family add files only under `HG/protocol-fixtures/<NN>/`, so they are in
  `HG/protocol-fixtures/06/filters/`.
- **Field seeds have no committed sample values**: the core `ledger` view lists declared field
  types but no stored values, so string leaves aim at the generator's own metadata value pool
  (3/4) instead of a committed sample, and numeric centres are drawn in `0..1023`.
- **`list_traverse` is unfiltered** (as the old traversal drivers were), page size from
  `draw::page_size`, 1–8 pages, 2 s per page, stops at 8 s.
- The client may stream `effective + 64` items, so a page that overshoots the page size still
  arrives `complete` and is judged (the old driver capped at `page + 1`, which hid overshoot).

## Requests to 99

- Every assertion of the "reachable only with task 07 merged" block of `h_query.go` and the
  `oracle: coverage …` probes: check reachability in the integrated build (DESIGN §9.2 item 4).
- `run_native.sh` whitelists never-hit must-hits only from `porting/00*.md`; the 07-dependent
  literals above show as "NEVER HIT" in a 06-only run (they do not fail it).
- Filter shapes the oracle does not classify as refused but the server might refuse (out-of-range
  `AddressRole` on accounts, `Not` without a child): inherited unchanged from `M/`; if the
  integrated run shows `ListAccounts returned unexpected error` with InvalidArgument for such a
  shape, the classification in `match.ClassifyRejectedFilter` (00b) needs the server's rule.

## Native run (acceptance)

`RULES=list_accounts,list_transactions,list_logs,list_traverse,smoke_tx run_native.sh 180`
(with `HEALTH_{WAL,DATA}_THRESHOLD=0.995` in the environment because the shared build machine's
disk was above the server's 80 % write gate): 26 576 list reads validated (12 522
ListTransactions, 8 374 ListLogs, 5 680 ListAccounts), zero findings; every must-hit of the
"no faults" rows above was hit; the only never-hit must-hits were the `oracle: coverage …`
probes (task 07).
