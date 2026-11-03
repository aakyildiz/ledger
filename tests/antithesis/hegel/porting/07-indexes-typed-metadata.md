# 07 — Indexes, typed metadata, readiness, retype windows, index introspection

Go: `W/oraclecheck/indexcheck/` (package `indexcheck`), `W/bin/oracle/h_index.go`.
Rust: `HG/driver/src/rules/indexes.rs` (+ the 07 marker blocks of `rules/mod.rs`).
Fixtures: `W/oraclecheck/testdata/rejections/07-*.json`.

## What runs where

| Piece | Where |
|---|---|
| Create frontiers (`M/validate.go:90-116` `recordIndexCreates`) | drain hook `07-indexes` |
| Retype observations (`M/checker.go:369-440`, `M/validate.go:440-458`) | drain hook `07-indexes` (opens / re-arms), `IndexStatusPoll` (two-phase per node), settle hook (closure) |
| Readiness poller (`M/indexes.go:632-920`) | Rust `index_poll` (one read ticket per poll, stale node-pinned `GetIndexStatus` per node) + read kind `IndexStatusPoll` + settle hook `07-indexes` (deferred promotion) |
| Registry reads (`M/index_introspect.go`) | Rust `index_introspect` + read kinds `ListIndexes`, `GetIndex`, `GetIndexStatus` |
| InspectIndex (`M/index_inspect.go`) | Rust `inspect_index` + read kind `InspectIndex` (a resume is a second ticket whose request carries `cursor`) |
| Index op / schema gens (`M/actions.go:1076-1154`, `M/indexes.go:512-622`) | Rust `index_op`, order gens `schema_set` / `schema_remove`, local `schema_bulk` |
| `MD/parallel_driver_indexes` | `index_bootstrap` + `index_op` + `index_poll` (tracked, oracle-judged) |
| `MD/parallel_driver_typed_metadata` | `typed_metadata_cycle` on reserved entities |
| `MD/parallel_driver_projection_alignment` | asset index from `index_bootstrap`/`index_op`; prepared-query half is task 08's |

## Old assertions

| Old source | Old assertion (kind: literal) | New | Where |
|---|---|---|---|
| M/indexes.go:676 | Reachable: index demoted to ambiguous | oracle: index demoted to ambiguous (broadened: fires on every not-ready observation of a tracked index; `wasActive` in details) | indexcheck/poll.go |
| M/indexes.go:698 | Reachable: index promotion deferred behind an earlier operation | oracle: index promotion deferred behind an earlier operation (once per pending promotion) | indexcheck/state.go settleHook |
| M/indexes.go:707 | Reachable: index promoted to active | oracle: index promoted to active | indexcheck/state.go settleHook |
| M/indexes.go:809 | Unreachable: index status is not self-consistent | oracle: index status is not self-consistent | indexcheck/poll.go |
| M/indexes.go:823 | Reachable: index status reported a materialised index file | oracle: index status reported a materialised index file | indexcheck/poll.go |
| M/indexes.go:901 | Reachable: retype window closed | oracle: retype window closed | indexcheck/state.go settleHook |
| M/indexes.go:253,264,316,318,356 | asset-index account query (Unreachable/Reachable) | not this task: index-served query pages are task 06's validators | (06) |
| M/indexes.go:928-1399 | indexed transaction/account query literals | not this task (task 06) | (06) |
| M/index_introspect.go:117 | Reachable: index listing on an absent ledger returned NotFound | oracle: … verbatim suffix | indexcheck/introspect.go |
| M/index_introspect.go:127 | Unreachable: ListIndexes returned unexpected error | oracle: ListIndexes returned unexpected error | introspect.go |
| M/index_introspect.go:137 | Unreachable: index listing served a ledger outside the fleet | oracle: … | introspect.go |
| M/index_introspect.go:149 | Unreachable: index listing violated its scope | oracle: … (also for an undecodable entry) | introspect.go |
| M/index_introspect.go:160 | Unreachable: index listing repeated an entry | oracle: … | introspect.go |
| M/index_introspect.go:175 | Reachable: index listing validated | oracle: … | introspect.go |
| M/index_introspect.go:183 | Unreachable: index listing outside model | oracle: … | introspect.go |
| M/index_introspect.go:243 | Unreachable: bucket-wide ListIndexes returned unexpected error | oracle: … | introspect.go |
| M/index_introspect.go:256 | Unreachable: bucket-wide index listing served an entry with no id | oracle: … | introspect.go |
| M/index_introspect.go:263 | Unreachable: bucket-scoped index listing served a ledger's entry | oracle: … | introspect.go |
| M/index_introspect.go:277 | Unreachable: bucket-wide index listing repeated an entry | oracle: … | introspect.go |
| M/index_introspect.go:291 | Reachable: bucket-scoped index listing validated | oracle: … | introspect.go |
| M/index_introspect.go:301 | Unreachable: bucket-wide index listing outside model | oracle: … (SCOPE_ALL restricted to tracked ledgers, `match.TrackedIndexListingMatches`, mayVanish kept) | introspect.go |
| M/index_introspect.go:312 | Reachable: fleet-wide index listing validated | oracle: … | introspect.go |
| M/index_introspect.go:360,448 | Unreachable: index lookup answered with another entry | oracle: … (one asserter for both RPCs; `rpc` in details) | introspect.go |
| M/index_introspect.go:373 | Reachable: index lookup validated | oracle: … | introspect.go |
| M/index_introspect.go:402 | Reachable: bucket-scoped index lookup returned NotFound | oracle: … | introspect.go |
| M/index_introspect.go:407 | Unreachable: bucket-scoped index lookup returned unexpected error | oracle: … | introspect.go |
| M/index_introspect.go:416 | Unreachable: bucket-scoped index lookup answered with a ledger's entry | oracle: … | introspect.go |
| M/index_introspect.go:461 | Reachable: index status validated | oracle: … | introspect.go |
| M/index_introspect.go:477 | Unreachable: index lookup served a ledger outside the fleet | oracle: … | introspect.go |
| M/index_introspect.go:494 | Unreachable: index lookup returned unexpected error | oracle: … | introspect.go |
| M/index_introspect.go:507 | Reachable: index lookup on an absent ledger returned NotFound | oracle: … | introspect.go |
| M/index_introspect.go:531 | Unreachable: index presence outside model | oracle: … (a NotFound is also explained by a base where the ledger is not live) | introspect.go |
| M/index_inspect.go:103 | Reachable: index inspection on an absent ledger returned NotFound | oracle: … | indexcheck/inspect.go |
| M/index_inspect.go:115 | Reachable: index inspection gated on a missing index | oracle: … | inspect.go |
| M/index_inspect.go:120 | Unreachable: InspectIndex returned unexpected error | oracle: … | inspect.go |
| M/index_inspect.go:132 | Unreachable: index inspection served a ledger outside the fleet | oracle: … | inspect.go |
| M/index_inspect.go:148 | Unreachable: index inspection violates its own contract | oracle: … | inspect.go |
| M/index_inspect.go:160 | Reachable: index inspection validated | oracle: … | inspect.go |
| M/index_inspect.go:164 | Reachable: index inspection served from a checkpoint | oracle: … | inspect.go |
| M/index_inspect.go:311 | Unreachable: resumed index inspection returned unexpected error | oracle: … | inspect.go |
| M/index_inspect.go:318 | Unreachable: resumed index inspection violates its own contract | oracle: … | inspect.go |
| M/index_inspect.go:324 | Reachable: index inspection resumed from its cursor | oracle: … | inspect.go |
| M/index_inspect.go:404 | Unreachable: summary mode answered another arm | oracle: … | inspect.go |
| M/index_inspect.go:447 | Reachable: index inspection summary matched the model | oracle: … | inspect.go |
| M/index_inspect.go:452 | Reachable: index inspection summary matched a populated index | oracle: … | inspect.go |
| M/index_inspect.go:462 | Reachable: index inspection summary counted unconvertible values | oracle: … | inspect.go |
| M/index_inspect.go:474 | Unreachable: index inspection summary outside model | oracle: … | inspect.go |
| MD/parallel_driver_indexes/main.go:42 | Unreachable: SetMetadataFieldType returned unexpected error | oracle (commit / failure checks of the tracked `schema_bulk`) | (00a) |
| MD/parallel_driver_indexes/main.go:71 | Unreachable: CreateIndex returned unexpected error | oracle (commit / failure checks: `INDEX_ALREADY_EXISTS`, `METADATA_FIELD_NOT_IN_SCHEMA` must be model-explained) | (00a) |
| MD/parallel_driver_indexes/main.go:79 | Unreachable: GetIndexStatus returned unexpected error | dropped: a stale per-node status error only counts as not ready (`IndexStatusPoll`); linearizable entry status errors are `oracle: index lookup returned unexpected error` | – |
| MD/parallel_driver_indexes/main.go:85 | AlwaysOrUnreachable: GetIndexStatus should return a response | dropped: a nil success cannot reach the driver (protojson outcome); `oracle: index status is not self-consistent` judges the response | – |
| MD/parallel_driver_indexes/main.go:100 | Unreachable: DropIndex returned unexpected error | oracle (commit / failure checks; a drop of an absent index is a model-predicted success) | (00a) |
| MD/parallel_driver_typed_metadata/main.go:140 | AlwaysOrUnreachable: typed metadata query should return a response | dropped here: the prepared query execution belongs to task 08 | (08) |
| MD/parallel_driver_typed_metadata/main.go:153 | AlwaysOrUnreachable: declared metadata key should appear in schema status | oracle (task 04 `GetMetadataSchemaStatus` read kind) | (04) |
| MD/parallel_driver_typed_metadata/main.go:156 | AlwaysOrUnreachable: declared type should be INT64 | oracle (task 04 `GetMetadataSchemaStatus`) | (04) |
| MD/parallel_driver_typed_metadata/main.go:181 | AlwaysOrUnreachable: changing metadata type should not crash | verbatim (definitive outcome is not Internal/Unknown) | rules/indexes.rs typed_metadata_cycle |
| MD/parallel_driver_typed_metadata/main.go:209 | AlwaysOrUnreachable: account should be readable after metadata type changes | oracle (task 05 GetAccount on the account; reads are model-checked) | (05) |
| MD/parallel_driver_typed_metadata/main.go:216 | AlwaysOrUnreachable: metadata key should survive type changes | oracle (task 05 GetAccount metadata equality: stored values are never rewritten) | (05) |
| MD/parallel_driver_typed_metadata/main.go:256 | AlwaysOrUnreachable: declaring an int64 type over an uncoercible value should not crash | verbatim | rules/indexes.rs typed_metadata_cycle |
| MD/parallel_driver_typed_metadata/main.go:275 | AlwaysOrUnreachable: declaring a bool type over a numeric value should not crash | verbatim | rules/indexes.rs typed_metadata_cycle |
| MD/parallel_driver_projection_alignment/main.go:84 | Unreachable: projection asset-index creation returned unexpected error | oracle (commit / failure checks of `index_bootstrap` / `index_op`) | (00a) |
| MD/parallel_driver_projection_alignment/main.go:92 | Unreachable: projection asset index did not become ready | dropped: readiness is observed, not awaited; never-promoted shows as `oracle: index promoted to active` missing | – |
| MD/parallel_driver_projection_alignment/main.go:124,152,159,172,178,182,196 | prepared-query / probe / projection-aligned read literals | not this task: has-asset reads are task 06 (ListAccounts) and task 08 (prepared queries), judged by the oracle's has-asset window | (06, 08) |

## New properties

- `oracle: metadata field not in schema rejection exercised` (Reachable, failure hook):
  a CreateIndex on an undeclared metadata field was rejected and explained by the model.
- `a write attempt is refused while an index builds` (Rust Sometimes, from the attempt logs
  of this family's writes; DESIGN §4.9).
- Promotion is sound across polls: only one poll covering every discovered node can
  promote (critic C9), verdicts are never aggregated across polls.

## Reachability

| Must-hit | Rule / path | Needs |
|---|---|---|
| oracle: index promoted to active | `index_bootstrap`/`index_op` create, then an all-nodes `index_poll` | no faults |
| oracle: index demoted to ambiguous | `index_poll` right after a create (node not built yet) | no faults (more under faults) |
| oracle: index promotion deferred behind an earlier operation | `index_poll` while a concurrent rule holds an older ticket | no faults (concurrency ≥ 2) |
| oracle: retype window closed | `schema_bulk` retypes a declared key that `index_op` indexed; two later `index_poll`s | no faults |
| oracle: index status validated | `index_introspect` GetIndexStatus arm | no faults |
| oracle: index status reported a materialised index file | `index_poll` | no faults |
| oracle: index listing validated / fleet-wide … / bucket-scoped index listing validated | `index_introspect` list arms | no faults |
| oracle: bucket-scoped index lookup returned NotFound | `index_introspect` GetIndex 1/8 bucket slot | no faults |
| oracle: index lookup validated | `index_introspect` GetIndex | no faults |
| oracle: index listing / lookup on an absent ledger returned NotFound | `index_introspect` absent 2 % | no faults |
| oracle: index inspection validated / gated / resumed / on an absent ledger | `inspect_index` | no faults |
| oracle: index inspection summary matched the model / a populated index | `inspect_index` SUMMARY on an indexed key | no faults |
| oracle: index inspection summary counted unconvertible values | `typed_metadata_cycle` writes `own-bad = "not-a-number-<v>"` (declared INT64, indexed by the first cycle); `inspect_index` inspects l1 with the owned view 1/4 | no faults |
| oracle: index inspection served from a checkpoint | `inspect_index` with `HEGEL_HAVE_CHECKPOINTS=1` | needs task 09 |
| oracle: metadata field not in schema rejection exercised | `index_op` 1/16 probe | no faults |
| a write attempt is refused while an index builds | any family write attempt answered `INDEX_BUILDING` | faults (99 removes it if the server never returns it on writes) |
| changing metadata type should not crash (+ the two declaring literals) | `typed_metadata_cycle` | no faults (AlwaysOrUnreachable) |

## Deviations from the spec

- `indexGaps` answers `{"gaps": {ledger: [{"canonical", "id"}…]}}` instead of
  `{ledger: [canonical…]}`: the driver needs the IndexID protojson to send a create, and the
  view takes an optional `ledger`/`ledgerPick` (the core then echoes `"ledger"`).
- Read kind `GetIndexStatus` sends the `GetIndexEntryStatus` RPC (the registry entry
  status of one index, `M/index_introspect.go runIndexEntryStatus`, which is what
  `index status validated` covered); the ledger-wide `GetIndexStatus` RPC is the stale poll.
- `index demoted to ambiguous` fires on every not-ready observation (the old one only on an
  active→ambiguous flip), so it is reached right after creates without faults.
- `typed_metadata_cycle` writes its uncoercible value under one shared reserved key
  `own-bad` as `"not-a-number-<v>"` (the old driver used a fresh key and the fixed text
  `"not-a-number"`); the first cycle also indexes `own-bad`, which is never removed. So the
  index holds many distinct unconvertible values: `inspect_index` reaches `summary counted
  unconvertible values` and `resumed from its cursor` on it. Each cycle ends by removing
  its score key's declaration (keeps l1's schema small). `inspect_index` reads
  `o-<run>-l1` with the owned `ledger` view 1/4 of the time (reads cannot disturb
  rule-owned entities).
- `inspect_index` probes an absent ledger 1/20 (the spec lists the must-hit, not the rate).
- Rejection fixture "drop of an absent index": not a rejection. The server tombstones the
  key and logs a `DroppedIndexLog` (`processDropIndex`), the model predicts success
  (`applyDropIndex`); `TestRejectionStages` requires a rejection, so the shape is covered by
  `indexcheck.TestDropOfAbsentIndexIsANoOpSuccess` instead of a fixture.
- `index_op`: create with 1/3 (gap) or 1/8 (no gap), else a drop of a pool index (present
  or not); a create without a gap targets a pool index, so it is a duplicate
  (`INDEX_ALREADY_EXISTS`).

## Requests to 99

- Set `HEGEL_HAVE_CHECKPOINTS=1` in the driver env once task 09 (view `checkpoints`) is
  merged; `oracle: index inspection served from a checkpoint` needs it.
- `a write attempt is refused while an index builds` needs faults; remove it if coverage
  shows the server never returns `INDEX_BUILDING` on writes.
- Task 06's indexed-query validators depend on `index promoted to active` happening; with
  `index_op` at weight 1.0 roughly half of the pool is present at any time.
