# 13 — Numscript (Rust-checked `r-ns-*` ledgers)

Numscript never runs on oracle-tracked ledgers (DESIGN §3, critic Q7). Task 13 owns two
Rust-checked ledgers, `r-ns-<run>-1` and `r-ns-<run>-2`, created by the setup hook
`numscript_setup` (untracked, keyed `r-<run>-k<N>`, retried; no draws), which also saves
library script `transfer` version `1.0.0` on each:

```
vars { account $from  account $to  monetary $amount }
send $amount ( source = $from allowing unbounded overdraft  destination = $to )
```

Code: `HG/driver/src/rules/numscript.rs` (rules, setup hook, quiet hook),
`rules/numscript/model.rs` (library and transaction model, script texts, pure checks),
`rules/numscript/semver.rs` (ordering, selectors, bumps). Every write goes through
`send_untracked` with a key; nothing goes through the oracle.

| Rule | Weight | What |
|---|---|---|
| `ns_save` | 0.5 | a next version of `transfer` (fixed `2.0.0` branch while the greatest sent version is below it, else a drawn major/minor/patch bump by 1 or by a drawn amount of the uint32 room), a next version of an existing `s-<N>`, or the first version of a new `s-<N>` (`N.0.0`, N ∈ 1..=10, or three drawn uint32 components below `u32::MAX`); 1/8 re-saves an acked version with other content. Versions are allocated under the model lock above the greatest version ever sent, so concurrent saves never race on one version. `transfer` versions differ from 1.0.0 only in drawn whitespace, line endings and comments (`Layout`); `s-<N>` versions are drawn among three templates (`Transfer`, `Reversed`, `ViaHub`) |
| `ns_read` | 0.5 | GetNumscript (`""`, `latest`, exact, partial `a`, `a.b`), ListNumscripts (drawn page size, cursor paging ≤ 8 s), ListNumscriptVersions, on a name with an acked version; linearizable (server default), drawn route |
| `ns_exec` | 1.0 | CreateTransaction with `scriptReference{name, "latest" | pinned acked version, vars}` or inline `script{plain: transfer, vars}`; from = pool address or `world`, to = another pool address, amount `COIN`/`USD/2` from `amount_positive`; unique reference `r-<run>-ns<N>`; keyed; `send_untracked` |
| quiet `numscript_state` | 0.5 (30 s, `needs_oracle: false`) | per ledger (hegel-drawn order): ListTransactions (all pages), ListAccounts (all pages), GetLedgerStats; resolve Unknown records by reference; evaluate the Always properties only for fully read ledgers |

## Porting table

| Old source | Old assertion (kind: literal) | New | Where |
|---|---|---|---|
| MD/parallel_driver_numscript_lifecycle/main.go:71 | Sometimes: should be able to save numscript | verbatim (cond = success or non-definitive outcome, the old `IsTolerated`) on the first save of a name | rules/numscript.rs ns_save |
| MD/parallel_driver_numscripts/main.go:34 | Sometimes: should be able to save numscript | verbatim (same call site as above: one literal, one site) | rules/numscript.rs ns_save |
| MD/parallel_driver_numscript_lifecycle/main.go:80 | AlwaysOrUnreachable: ListNumscripts should not return unexpected error | verbatim (tolerated = success or non-definitive outcome) | rules/numscript.rs ns_read |
| MD/parallel_driver_numscript_lifecycle/main.go:83 | AlwaysOrUnreachable: saved numscript should appear in ListNumscripts | verbatim, generalised to concurrency: every name with a version acked before the first page was sent appears in a complete traversal | rules/numscript.rs ns_read |
| MD/parallel_driver_numscript_lifecycle/main.go:97 | Sometimes: should be able to save second numscript version | verbatim on saves of a name that already has an acked version | rules/numscript.rs ns_save |
| MD/parallel_driver_numscript_lifecycle/main.go:114 | AlwaysOrUnreachable: GetNumscript should return latest version | verbatim for selectors `""`/`latest`: the returned version may be stored (not definitively rejected) and is ≥ the greatest version acked before the read (equal to the old check when no save is concurrent) | rules/numscript.rs ns_read |
| MD/parallel_driver_numscript_lifecycle/main.go:130 | AlwaysOrUnreachable: latest pointer should be the greatest saved semver | verbatim: `latestVersion` ∈ versions that may be stored and ≥ max acked-before; also (no concurrency involved: one read handle, `query/numscript.go` ReadAllNumscriptVersions) the listing is non-empty and `latestVersion` equals its greatest version | rules/numscript.rs ns_read |
| MD/parallel_driver_numscripts/main.go:49 | AlwaysOrUnreachable: saved numscript should be readable | verbatim (returned name equals the requested name) | rules/numscript.rs ns_read |
| MD/parallel_driver_numscripts/main.go:75 | Sometimes: should be able to use saved numscript in transaction | verbatim on scriptReference transactions (cond = success or non-definitive) | rules/numscript.rs ns_exec |
| MD/parallel_driver_numscripts/main.go:77 (`internal.CheckCreatedTransaction`) | (helper assertions on the created transaction: `checks.go:66-78` post-commit volume entry missing fields / input / output invalid) | task 02 never sees `r-ns-*` transactions, so the post-commit volume part is re-checked here under a new literal (`a numscript transaction reports consistent post-commit volumes`, below), strengthened; the postings part is the new exact-postings Always; the helper's literals stay with task 02's port | rules/numscript.rs ns_exec |
| MD/parallel_driver_transactions/main.go:46-79 (Numscript branch of `randomTransactionRequest`) | (no literal of its own; the branch shares `should be able to create a transaction` / `should be able to create bulk transactions` / `bulk Apply should return one log per request` / `should always be able to read committed transaction` with the postings branch) | the Numscript branch moves here as ns_exec's inline mode; the shared literals stay with task 02 (postings branch) | rules/numscript.rs ns_exec |
| H/main.rs:800-817 (`numscript` rule on tracked hegel ledgers) | Sometimes: a numscript transaction commits | verbatim (existing hegel literal kept) on inline script transactions, now on `r-ns-*` ledgers | rules/numscript.rs ns_exec |
| H/main.rs:813-815 (`register_tx`/`resolve_tx` of the numscript rule) | Always: an acknowledged transaction has exactly the postings that were sent; an acknowledged transaction survives faults; a definitively rejected write never takes effect; an account's balance equals the sum of its committed postings (H/model.rs-backed, tracked ledgers) | retired with H/model.rs for tracked ledgers (oracle); for Numscript re-homed as the new properties below with their own literals | – |
| H/model.rs `Status`, `TxRec` | (model, no literal) | re-homed for `r-ns-*` ledgers in rules/numscript/model.rs (no metadata histories) | rules/numscript/model.rs |

## New properties

| Literal | Kind | Rule | Condition |
|---|---|---|---|
| `a saved numscript version is immutable` | AlwaysOrUnreachable | ns_save | a re-save of an acked version with other content never succeeds |
| `a re-save of a saved numscript version is refused as already existing` | AlwaysOrUnreachable | ns_save | a definitive refusal of that re-save has reason `NUMSCRIPT_VERSION_ALREADY_EXISTS` and code AlreadyExists (`processor_numscript_library.go:46-48`); a non-definitive outcome checks nothing |
| `a numscript read returns a saved version and its content` | AlwaysOrUnreachable | ns_read | GetNumscript (any selector) returns a legal resolution of the selector (stored possibly, matching, ≥ the greatest matching acked-before version) with the content the model saved for that version; ListNumscripts items are legal latest versions with their content; ListNumscriptVersions lists every acked-before version and only versions that may be stored; a server refusal of a selector that an acked version matches is a violation: a definitive reason, or the bare gRPC `NotFound` (no reason) GetNumscript answers a missing script with (`controller_default.go` GetNumscript, `adapter/grpc/server.go:662-664`) |
| `a numscript transaction commits exactly the postings its script implies` | Always | ns_exec | a definitive success returns one log whose created transaction's postings are one of the candidates (`transfer`/inline: `[{from,to,amount,asset}]`; `s-<N>` with `latest`: the template of every non-rejected version, as known when the answer arrives, at or above the greatest version acked when the transaction was sent; pinned: that version's template). Second call site in numscript_state: an Unknown/Pending record found committed by the quiet listing must be listed with one of its candidates (for `latest`, recomputed from the library at the listing, so a write applied after the driver gave up on it is judged against every version saved since its floor) |
| `a numscript transaction reports consistent post-commit volumes` | Always | ns_exec | on a definitive success whose created transaction carries `postCommitVolumes`: every posting's (account, asset) has an uncolored entry with present, decodable input and output (the server always sets both, `processor_volumes.go:60-80`), input ≥ the amounts the transaction sent to it and output ≥ the amounts it sent from it. An absent `postCommitVolumes` is tolerated, like the old helper |
| `an acknowledged numscript transaction survives faults` | Always | numscript_state | every Acked record is listed by reference with the postings acknowledged |
| `a definitively rejected numscript transaction never takes effect` | Always | numscript_state | no Rejected record is listed |
| `a numscript ledger's balances equal its committed postings` | Always | numscript_state | ListAccounts volumes (input, output, balance = input − output) of every account equal the sums over committed records (Acked + resolved); no unexpected non-zero volume; color always `""`. The two listings are separate snapshots and a write the driver gave up on can still land between them (a client-side retry outlives the Q1 barrier), so on a mismatch the hook re-reads accounts then transactions and judges again; only a mismatch on three reads is asserted |
| `numscript execution count caught up` | Sometimes | numscript_state | `GetLedgerStats.numscriptExecutionCount` equals the committed script transactions (records that stay unresolved are not committed as of the listing, so they do not gate it; the counter is an asynchronous usagebuilder projection, `controller_default.go:530-545`, so only Sometimes) |
| `numscript_state completes within its quiet budget` | Sometimes | numscript_state | both ledgers fully checked before the deadline |

Status machinery (H/model.rs): a record is Pending when sent, Acked on success, Rejected on a
definitive failure (`clients::definitive`), Unknown otherwise. The quiet hook resolves
Unknown/Pending records by reference against the complete listing: listed → checked against its
candidates, then Acked with the listed postings (balances use them); not listed → stays Unknown (it did not commit by now,
after the Q1 barrier; balances exclude it). Library versions keep Pending/Acked/Unknown/
Rejected; a read is judged with the window (acked before the send, possibly stored after the
answer).

## Reachability (must-hit assertions)

| Literal | Kind | Reached by | Needs |
|---|---|---|---|
| should be able to save numscript | Sometimes | ns_save (new `s-<N>`) | no faults |
| should be able to save second numscript version | Sometimes | ns_save (`transfer` next version) | no faults |
| should be able to use saved numscript in transaction | Sometimes | ns_exec (scriptReference modes) | no faults |
| a numscript transaction commits | Sometimes | ns_exec (inline mode) | no faults |
| a numscript transaction commits exactly the postings its script implies | Always | ns_exec success | no faults |
| an acknowledged numscript transaction survives faults | Always | numscript_state | no faults (quiet period) |
| a definitively rejected numscript transaction never takes effect | Always | numscript_state (evaluated on every fully read ledger, vacuously true without rejected records) | no faults |
| a numscript ledger's balances equal its committed postings | Always | numscript_state | no faults |
| numscript execution count caught up | Sometimes | numscript_state | no faults |
| a numscript transaction reports consistent post-commit volumes | Always | ns_exec success | no faults |
| numscript_state completes within its quiet budget | Sometimes | numscript_state | no faults |

AlwaysOrUnreachable literals (`a saved numscript version is immutable`, `a numscript read
returns a saved version and its content`, `a re-save of a saved numscript version is refused
as already existing`, the five verbatim reads) are free by policy.

## Deviations and decisions

- **Taper**: ns_exec stops writing to a ledger with 4000 records (`TX_CAP`), at most 16
  `s-<N>` names per ledger and 24 versions per name, so the quiet hook's full listing stays
  inside its 30 s budget on long timelines (DESIGN §3.2 tapers tracked ledgers; the same
  bound is applied here).
- **No self-transfers**: `to` is redrawn deterministically (`i+1 mod 100`) when it equals
  `from`; a `from == to` Numscript send has no single "exact postings" expectation worth
  pinning.
- **Setup steps are bounded** (40 s each); a ledger whose creation or `transfer@1.0.0` save
  did not succeed is marked not ready and every rule skips it.
- Content comparisons trim surrounding whitespace (the model compares what it sent with
  what the server returns).
- Paged reads stay inside the rule bound: ListNumscripts pages until its 8 s read budget is
  spent (no page-count cap: 17 names at page size 1 need 17 pages), each page with the
  remaining budget as its timeout; the quiet hook's listings back off 300/600/900 ms between
  failed pages, inside the deadline.
- `ListAccounts`/`ListTransactions` in the quiet hook page with `pageSize 1000` and
  `maxItems 1001` so each call reaches the stream's end (and its `x-next-cursor`).

## Requests to 99

- None for dependencies. The native acceptance run
  (`RULES=ns_save,ns_read,ns_exec QUIET_PROBABILITY=0.05 run_native.sh 180`) could not be
  run by task 13: the build host's disk was full (see the task report); task 99 should run
  it and confirm the Sometimes literals above are hit with zero failing assertions.
