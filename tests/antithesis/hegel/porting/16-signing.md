# 16 — Signing keys, signed writes, the requireSignatures window

Code: `HG/driver/src/rules/signing.rs` (setup hook, two rules, one exclusive action, the
key-tree model, unit tests). Rust-checked, except signed **tracked** writes, which the
oracle validates like any other write (`/write/begin` carries `"signed": true`).

| Item | Weight | What |
|---|---|---|
| setup hook `signing_setup` | – | `POST /pubkey` for the fixed root seed; unsigned `registerSigningKey{keyId:"root-<run>"}` keyed `r-<run>-signing-root`, retried until definitive; an Unauthenticated answer is resolved by listing the keys (our root listed → an earlier attempt committed; not listed → keys of an earlier run exist on a reused volume, and the whole family stays idle); then `createLedger r-sign-<run>` keyed and retried |
| `signing_keys` | 0.4 | drawn among register (32 drawn seed bytes, id `child-<run>-<N>`, signed by the root or a usable child; weight 0 at 24 usable children), revoke (a usable non-root key, signed by itself or a usable ancestor, `cascade` drawn; an acked cascade is followed by a list), list (drawn route and page size, cursor paging ≤ 4 s), unsigned register probe (pinned) |
| `signed_write` | 0.6 | 1/3: a forced tx to `r-sign-<run>` signed by a certainly revoked key, pinned (falls through to the tracked write when no key is revoked); else a forced tx (`world` → drawn address, positive amount, `COIN`) on a `ledgerPick`-drawn tracked ledger with the DESIGN §3.2 taper, `WritePlan.sign` = root, dispatched through the oracle |
| exclusive `require_signatures_window` | 1 | plan: probe node and client, management schedule and backoffs, write payload. run: skipped while `signatures_required` or maintenance is on; Drop guard; `setSigningConfig{true}` signed by root (8 s); if acked, Barrier → horizon → `wait_at_horizon` on the node, unsigned and root-signed writes to `r-sign-<run>` pinned there (probes stop 13 s after the enable started); restore `false` (one key for every attempt) with the rest of the 20 s (≥ 2 s); not definitive → `signatures_required = true` + quiet-blocking job `signing-restore` retrying until definitive, which clears the flag |

Probes that expect Unauthenticated/PermissionDenied (`probe_call`) are raw `Driver::call`s,
re-sent at most 3 times on unknown/Unavailable/DeadlineExceeded, judging the first success,
auth-class or definitive answer; anything else is inconclusive (no assertion). Every pinned
probe runs after `Barrier` → horizon → `wait_at_horizon(node, horizon, exact=false)`
(safety rule 4); a node that does not reach the horizon in 4 s skips the probe.

## Key-tree model (`KeyTree`)

Keys: parent (= signer, as the server records), seed, public key, registration phase
(`InFlight | Acked(tick) | Unknown`), `tainted`. Revokes: target, cascade, start tick,
phase. A logical clock orders acks against sends. Server semantics followed
(`processor_signing.go`, `write_set.go`): register is an upsert with parent = signer; a
cascade walks stored parent links of keys still present (a key orphaned by an earlier
non-cascading revoke of its parent is not reached); revoking an absent key succeeds.

- **certain live** for a list started at tick `s` (judged with everything recorded until
  it ended): registration acked before `s`, no revoke on the key and no cascading revoke on
  any ancestor in any state. Unknown registrations are never expected.
- **certain absent**: registration acked, and a revoke on itself acked before `s`, or an
  acked-before-`s` cascade on an ancestor `G` such that the key and every key between it
  and `G` were acked before the cascade started and are untainted, and no key between them
  had a revoke started before the cascade was acked.
- **tainted**: registration acked while a cascading revoke on the signer or one of its
  ancestors had started (a lagging node may have admitted it after the cascade, as an
  orphan).
- **usable** (signers, revoke candidates): root, or acked, untainted, untouched by any
  revoke on itself or cascade on an ancestor. The root is never revoked.

## Porting table

| Old source | Old assertion (kind: literal) | New | Where |
|---|---|---|---|
| MD/singleton_driver_signing_keys/main.go:55 | Sometimes: should be able to register signing key | verbatim; cond = the old `IsTolerated` (success or a non-definitive answer other than Unauthenticated/PermissionDenied); the register is now signed by the root or a child (unsigned registration only works once, at bootstrap) | rules/signing.rs exec_keys Register |
| MD/singleton_driver_signing_keys/main.go:76 | AlwaysOrUnreachable: registered key should appear in ListSigningKeys | verbatim, generalised to concurrency: every certainly-live key (above) is listed by a complete paged traversal | rules/signing.rs list_and_check |
| MD/singleton_driver_signing_keys/main.go:100 | Sometimes: should be able to revoke signing key | verbatim (cond as for register); revoke signed by the key itself or an ancestor, `cascade` drawn (old: always true, self-signed) | rules/signing.rs exec_keys Revoke |
| MD/singleton_driver_signing_keys/main.go:119 | AlwaysOrUnreachable: revoked key should not appear in ListSigningKeys | verbatim: no certainly-absent key is listed (evaluated when at least one exists) | rules/signing.rs list_and_check |

## New properties

| Literal | Kind | Reached by | Condition |
|---|---|---|---|
| `an unsigned key registration after bootstrap is refused as unauthenticated` | AlwaysOrUnreachable | signing_keys (probe) | pinned probe answers Unauthenticated |
| `a cascade revoke removes exactly the subtree` | AlwaysOrUnreachable | signing_keys (list after an acked cascade) | no key of the cascade's certain subtree is listed and every certainly-live key is |
| `a signed tracked write commits` | Sometimes | signed_write | definitive success of the root-signed tracked write |
| `a write signed by a revoked key is refused` | AlwaysOrUnreachable | signed_write (1/3) | pinned probe answers PermissionDenied |
| `an unsigned write is refused while signatures are required` | AlwaysOrUnreachable | require_signatures_window | pinned probe answers Unauthenticated |
| `a signed write is accepted while signatures are required` | Sometimes | require_signatures_window | root-signed pinned write succeeds |
| `the require-signatures window closes` | Reachable | require_signatures_window / signing-restore job | the restore is definitive (one call site per path, same literal) |

## Reachability

| Must-hit | Needs |
|---|---|
| should be able to register signing key | no faults |
| should be able to revoke signing key | no faults |
| a signed tracked write commits | no faults |
| a signed write is accepted while signatures are required | no faults (exclusive action; native run with `EXCLUSIVE_PROBABILITY`) |
| the require-signatures window closes | no faults (exclusive action) |

## Deviations

- Safety rule 1 says children are revoked with cascade only when they are leaves; the rule
  table and `a cascade revoke removes exactly the subtree` need non-leaf cascades. Cascades
  are drawn on any usable non-root key: no cascade can reach the root (it is every key's
  ancestor, never a descendant), so the bootstrap window never reopens; only the root signs
  tracked writes, so a cascade never turns them into PermissionDenied; the model handles
  orphans and uncertain cascades conservatively.
- Register is an upsert and revoke of an absent key succeeds, so no AlreadyExists/NotFound
  exists for them; the bootstrap tolerates `*ALREADY_EXISTS` anyway, and resolves an
  Unauthenticated answer by listing (see above), instead of looping forever.
- The ledger is `r-sign-<run>` (task spec), not `r-sign-<run>-<N>`.

## Open issues

- An enable attempt still in flight inside the cluster after the restore committed could
  commit later and leave `requireSignatures` on (there is no read of the flag); tracked
  writes would then stay unresolved and the quiet periods would report it. Not mitigated.
- `wait_at_horizon` gates on `lastPersistedIndex`; admission reads `requireSignatures` and
  the key store from node-local state updated by the same apply. A gap between the two
  would show as an inconclusive-looking false finding on a lagging node.

## Requests to 99

None.
