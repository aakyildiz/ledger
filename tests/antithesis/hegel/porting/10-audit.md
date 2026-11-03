# Porting record — 10 audit reads

Task 10 ports the singleton model's audit reads (`M/audit.go` =
`tests/antithesis/workload/bin/cmds/model/singleton_driver_model/audit.go`), the
main driver `MD/parallel_driver_audit` and the audit half of
`MD/parallel_driver_bulk_atomicity` onto two read kinds, `ListAuditEntries` and
`GetAuditEntry`, validated by `W/oraclecheck/auditcheck` and asserted in
`W/bin/oracle/h_audit.go` (one literal call site per id). The Rust rules are
`audit_list` (1.0) and `audit_entry` (0.5) in `HG/driver/src/rules/audit.rs`.

## Foreign tolerance

Every served entry is classified by its idempotency key: **tracked** when it
starts with `o-<run>-k`, **foreign** otherwise (other keys, the empty key,
technical proposals). The singleton's only foreign range was `setupMaxSeq`.

| Check (`M/audit.go`) | Decision | Where |
|---|---|---|
| page contract (`auditPageViolation`: length, cursor legality, ordering, zone-scan density, filter satisfaction) | kept for every entry (structural) | `readkinds.go` → `match.AuditPageViolation` |
| failed entry must match a recorded rejection | tracked failures only; foreign failures skipped. A tracked failure with a reason outside D (retryable) under a key the oracle registered is **left unjudged** (see below) | `validate.go` `checkEntry` |
| success past the committed frontier ("names logs the model never committed") | tracked only | `checkEntry` |
| `auditSuccessMismatch` (exactly one bulk, ranges, ledgers) | tracked only | `checkEntry` |
| order-type scope | tracked only | `checkEntry` |
| "audit trail misses a committed log" (bare log-seq ranges) | tracked logs only: iterates the learned tracked sequences | `ValidatePage` |
| "audit tail misses the newest committed bulk" | **dropped** (not ported): foreign writes can be newer than every tracked bulk. Task 11's quiet-period `oracle: an acknowledged tracked bulk is missing from the audit trail` covers the intent at quiet periods only | – |
| tracked failure with a reason in `oracle.UncommittedReasons` | cannot exist (the server neither audits nor freezes them): reported as `audit failure entry unexplained by the model` | `checkEntry` |
| known-entry lower bound (`auditKnownMatchViolation`) | kept for remembered entries, tracked or foreign (history is permanent); an entry counts only for reads whose ticket is greater than the frontier when it was learned | `knownMatchViolation`, `state.go` |
| batch-key check (`auditIdempotencyViolation`) | kept (tracked keys, via `replayable`) | `BatchKeyViolation` |
| new: foreign entry names a tracked ledger | `oracle: a foreign proposal touched a tracked ledger` | `checkEntry` |
| new: bulk atomicity per key | see "New properties" | `checkEntry`, `ValidatePage`, `DuplicateProposalViolation` |

**Retryable failure entries.** `machine.go` (`:1607-1626`, `:1659-1679`) audits
every FSM failure whose error is not an `UncommittedFailure`, but
`recordIdempotencyFailure` freezes only freezable kinds. A failure with a reason
outside D and outside `UncommittedReasons` (today only an unclassified error,
which reaches the trail as `UNSPECIFIED`, or a future reason; every currently
named retryable FSM reason such as `BALANCE_NOT_PRELOADED` or `STALE_PROPOSAL`
implements `UncommittedFailure` and is never audited) would be audited
unfrozen, the client would see it as non-definitive, and the driver would retry
the key, which may commit. The oracle records only definitive rejections, so
once the key settles such an entry has nothing to match. It is left unjudged
(coverage `oracle: audit failure entry with a retryable reason left unjudged`)
when its key was registered; a reason in `UncommittedReasons` stays a finding.

## Assertions

| Old source | Old assertion (kind: literal) | New | Where |
|---|---|---|---|
| M/audit.go:103 | Reachable: singleton_driver_model: audit sequence bound inside or rejected | oracle: audit sequence bound inside or rejected | bin/oracle/h_audit.go (readkinds.go probeEvent) |
| M/audit.go:105 | Reachable: singleton_driver_model: unspecified audit field rejected | oracle: unspecified audit field rejected | h_audit.go |
| M/audit.go:107 | Reachable: singleton_driver_model: audit condition of the wrong type rejected | oracle: audit condition of the wrong type rejected | h_audit.go |
| M/audit.go:109 | Reachable: singleton_driver_model: parameterised idempotency-key condition rejected | oracle: parameterised idempotency-key condition rejected | h_audit.go |
| M/audit.go:111 | Reachable: singleton_driver_model: idempotency-key prefix containing NUL rejected | oracle: idempotency-key prefix containing NUL rejected | h_audit.go |
| M/audit.go:113 | Reachable: singleton_driver_model: audit not filter rejected | oracle: audit not filter rejected | h_audit.go |
| M/audit.go:119 | Unreachable: singleton_driver_model: ListAuditEntries returned unexpected error | oracle: ListAuditEntries returned unexpected error | h_audit.go |
| M/audit.go:128 | Unreachable: singleton_driver_model: malformed audit cursor returned results | oracle: malformed audit cursor returned results (also on an incomplete page) | h_audit.go |
| M/audit.go:137 | Unreachable: singleton_driver_model: unsupported audit filter returned a page | oracle: unsupported audit filter returned a page (also on an incomplete page) | h_audit.go |
| M/audit.go:160 | Unreachable: singleton_driver_model: audit page violates its own contract | oracle: audit page violates its own contract | h_audit.go |
| M/audit.go:172, :1325 (dynamic `"singleton_driver_model: "+verdict.finding`) | Unreachable: audit failure entry unexplained by the model (finding at :325, :1384) | oracle: audit failure entry unexplained by the model | h_audit.go |
| M/audit.go:172, :1325 | Unreachable: audit entry names logs the model never committed (:333, :1392) | oracle: audit entry names logs the model never committed | h_audit.go |
| M/audit.go:172, :1325 | Unreachable: audit entry outside model (:337, :1396) | oracle: audit entry outside model | h_audit.go |
| M/audit.go:172 | Unreachable: audit entry outside the order-type scope (:345) | oracle: audit entry outside the order-type scope | h_audit.go |
| M/audit.go:172 | Unreachable: audit trail misses a committed log (:363) | oracle: audit trail misses a committed log (tracked logs only) | h_audit.go |
| M/audit.go:172 | Unreachable: audit tail misses the newest committed bulk (:374) | dropped: foreign writes can be newer; intent covered at quiet periods by task 11's `oracle: an acknowledged tracked bulk is missing from the audit trail` | – |
| M/audit.go:172 | Unreachable: audit page omits an entry the trail served before (:431) | oracle: audit page omits an entry the trail served before | h_audit.go |
| M/audit.go:172 | Unreachable: audit page stopped short of an entry the trail served before (:434) | oracle: audit page stopped short of an entry the trail served before | h_audit.go |
| M/audit.go:172 | Unreachable: audit page dropped its resume cursor (:436) | oracle: audit page dropped its resume cursor | h_audit.go |
| M/audit.go:179 | Unreachable: singleton_driver_model: audit entry names the wrong batch key | oracle: audit entry names the wrong batch key | h_audit.go |
| M/audit.go:185 | Reachable: singleton_driver_model: audit entry batch key matched the model | oracle: audit entry batch key matched the model | h_audit.go |
| M/audit.go:191 | Reachable: singleton_driver_model: audit page validated | oracle: audit page validated | h_audit.go |
| M/audit.go:195 | Reachable: singleton_driver_model: audit failure entry matched a model rejection | oracle: audit failure entry matched a model rejection (needs task 02/03; Requests to 99) | h_audit.go |
| M/audit.go:200 | Reachable: singleton_driver_model: audit page judged against remembered entries | oracle: audit page judged against remembered entries | h_audit.go |
| M/audit.go:205 | Reachable: singleton_driver_model: audit resume cursor corroborated by a remembered entry | oracle: audit resume cursor corroborated by a remembered entry | h_audit.go |
| M/audit.go:1294 | Reachable: singleton_driver_model: audit entry read on an unassigned sequence returned NotFound | oracle: audit entry read on an unassigned sequence returned NotFound | h_audit.go |
| M/audit.go:1299 | Unreachable: singleton_driver_model: GetAuditEntry returned unexpected error | oracle: GetAuditEntry returned unexpected error | h_audit.go |
| M/audit.go:1309 | Unreachable: singleton_driver_model: audit entry served an unassigned sequence | oracle: audit entry served an unassigned sequence | h_audit.go |
| M/audit.go:1316 | Unreachable: singleton_driver_model: audit entry read served a different sequence | oracle: audit entry read served a different sequence | h_audit.go |
| M/audit.go:1337 | Reachable: singleton_driver_model: audit entry read validated | oracle: audit entry read validated | h_audit.go |
| M/audit.go:1341 | Reachable: singleton_driver_model: audit entry carried its per-order items | oracle: audit entry carried its per-order items | h_audit.go |
| M/cursors.go:133 | Reachable: singleton_driver_model: malformed audit cursor rejected | oracle: malformed audit cursor rejected | h_audit.go |
| M/cursors.go:139 | Unreachable: singleton_driver_model: malformed cursor returned unexpected error (audit kind) | mapped: a malformed audit cursor answered with anything but InvalidArgument is `oracle: ListAuditEntries returned unexpected error` (details carry `malformed: true`); the transaction/log kinds belong to task 06 | h_audit.go |
| MD/parallel_driver_audit/main.go:41 | Sometimes: should be able to create tx for audit trail | dropped: the setup write is not this task's; tracked writes come from other rules (here `smoke_tx`) and their outcomes are the oracle's (`oracle: model outcome verified`, 00a) | – |
| MD/parallel_driver_audit/main.go:50 | Unreachable: Apply succeeded but returned no committed log | mapped: `oracle: successful bulk returned no committed log` (00a), which judges every tracked write | (00a) |
| MD/parallel_driver_audit/main.go:63 | Unreachable: ListAuditEntries returned unexpected error | mapped: `oracle: ListAuditEntries returned unexpected error` (transient outcomes are the service's skip class) | h_audit.go |
| MD/parallel_driver_audit/main.go:80 | Unreachable: ListAuditEntries stream returned unexpected error | mapped: the client reports a mid-stream error as the call's error outcome, judged as `oracle: ListAuditEntries returned unexpected error` | h_audit.go |
| MD/parallel_driver_audit/main.go:95 | AlwaysOrUnreachable: audit trail should contain entries | verbatim, on a successful complete unfiltered first page (setup wrote entries) | rules/audit.rs `check_trail_has_entries` |
| MD/parallel_driver_bulk_atomicity/main.go:155 | Sometimes: mid-failing atomic bulk definitively rejected | task 02 (verbatim there) | – |
| MD/parallel_driver_bulk_atomicity/main.go:277 | Always: failed atomic bulk yields no success audit entry beyond ledger creation | mapped (per key, not per ledger): `oracle: a rejected keyed bulk has no success audit entry` | h_audit.go |
| MD/parallel_driver_bulk_atomicity/main.go:281 | Always: audit failure entries are unique per proposal | mapped: `oracle: audit failure entries are unique per proposal` (keys rejected for good; page plus remembered entries) | h_audit.go |
| MD/parallel_driver_bulk_atomicity/main.go:285 | Always: failure audit entry observed for failed atomic bulk | mapped: `oracle: a rejected keyed bulk has a failure audit entry` (whole first page of an exact key filter, read after the rejection was observed) | h_audit.go |
| MD/parallel_driver_bulk_atomicity/main.go:295 | Always: failed atomic bulk leaves no partial transaction effects | not this task: the oracle's failure explanation plus task 11's quiet exact state (task 02 "no partial effects") | – |
| MD/parallel_driver_bulk_atomicity/main.go:302 | Always: failed atomic bulk leaves no partial account activity | not this task: as above (task 02 / task 11) | – |

## New properties

| Literal | Kind | Condition |
|---|---|---|
| `oracle: a foreign proposal touched a tracked ledger` | Unreachable | a foreign entry (key without the tracked prefix, including the empty key) names an `o-<run>-` ledger |
| `oracle: a rejected keyed bulk has no success audit entry` | Unreachable | a success entry under a key whose only write (`WritesByKey` holds one record, no conflict) ended definitive-frozen at the FSM stage |
| `oracle: audit failure entries are unique per proposal` | Unreachable | two failure entries (page or remembered) under such a key share a proposal id |
| `oracle: a rejected keyed bulk has a failure audit entry` | Unreachable | filter is exactly one hardcoded `AUDIT_FIELD_IDEMPOTENCY_KEY` leaf, first page (`AfterSeq == 0`), shorter than its page size, the key is rejected for good as above, and its `ObserveTicket` is below the read's ticket; the page holds no failure entry under the key |
| `oracle: audit failure entry with a retryable reason left unjudged` | Reachable (needs faults; Requests to 99) | a tracked failure entry with a reason outside D under a registered key was skipped |
| `oracle: rejected keyed bulk checked for its failure audit entry` | Reachable (needs task 02/03; Requests to 99) | the previous Unreachable's preconditions held on a page |

## Reachability

| Must-hit | Reached by | Needs |
|---|---|---|
| oracle: audit page validated | audit_list | no faults |
| oracle: audit entry read validated | audit_entry | no faults |
| oracle: audit entry carried its per-order items | audit_entry on a sample of a setup or `smoke_tx` entry | no faults |
| oracle: audit entry read on an unassigned sequence returned NotFound | audit_entry (1/4 `u64::MAX`) | no faults |
| oracle: audit entry batch key matched the model | audit_list over `smoke_tx` commits (replayable keys) | no faults |
| oracle: audit page judged against remembered entries | audit_list after an earlier page | no faults |
| oracle: audit resume cursor corroborated by a remembered entry | audit_list full page short of a remembered entry | no faults |
| oracle: audit sequence bound inside or rejected | audit_list (Or with a seq leaf) | no faults |
| oracle: unspecified audit field rejected | audit_list refused probe | no faults |
| oracle: audit condition of the wrong type rejected | audit_list refused probe | no faults |
| oracle: parameterised idempotency-key condition rejected | audit_list refused probe | no faults |
| oracle: idempotency-key prefix containing NUL rejected | audit_list refused probe | no faults |
| oracle: audit not filter rejected | audit_list Not probe | no faults |
| oracle: malformed audit cursor rejected | audit_list (1/16 malformed cursor) | no faults |
| oracle: audit failure entry matched a model rejection | audit_list over a tracked FSM rejection | needs task 02/03 (`smoke_tx` never fails) |
| oracle: rejected keyed bulk checked for its failure audit entry | audit_list key mode (1/8) on a frozen-rejected key | needs task 02/03 |
| oracle: audit failure entry with a retryable reason left unjudged | an audited unfrozen tracked failure | faults (and a server path that audits a retryable reason; may be unreachable on the current server) |

## Requests to 99

- Exclude from task 10's acceptance and check in the integrated build:
  `oracle: audit failure entry matched a model rejection` and
  `oracle: rejected keyed bulk checked for its failure audit entry` (need the
  tracked FSM rejections of tasks 02/03).
- `oracle: audit failure entry with a retryable reason left unjudged` needs
  faults and a server path that audits a failure whose reason is outside D and
  outside `UncommittedReasons`. On the current server every named retryable FSM
  reason implements `UncommittedFailure` and is not audited, so this may never
  fire; 99 decides whether to keep it as a must-hit or downgrade it to a
  non-must-hit (it guards against a false alarm, not a server property).
- The rejection fixtures are task 02/03's: task 10 generates no writes.
