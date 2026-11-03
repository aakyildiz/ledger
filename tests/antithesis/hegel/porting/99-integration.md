# 99 — Integration

## Catalog audit (DESIGN §9.2 item 4)

Union of the SDK outputs of the local runs: the native smokes (600 s with
constant quiet/exclusive probabilities, 540 s with the default trigger, a
20-minute run) and the 45-minute compose runs (driver, oracle and client
catalogs). 364 must-hit assertions are cataloged; 12 were never hit
(a Sometimes counts only when true). None is unreachable by construction, so
none was removed; each is justified here:

| Assertion | Kind | Why not hit locally | Class |
|---|---|---|---|
| `the fault pause command exists in Antithesis` | Always | evaluated only under `ANTITHESIS_OUTPUT_DIR` | Antithesis only |
| `the fault pause command succeeds` | Always | evaluated only under `ANTITHESIS_OUTPUT_DIR` | Antithesis only |
| `a write attempt hits a preload miss` | Sometimes | `BALANCE_NOT_PRELOADED` / `PRELOAD_UNAVAILABLE` need a leader change or a cache miss under faults | faults |
| `a write attempt is blocked by clock skew` | Sometimes | `WRITES_BLOCKED_CLOCK_SKEW` needs Antithesis clock faults | faults |
| `a write attempt is refused as a stale proposal` | Sometimes | `STALE_PROPOSAL` needs a leadership change between plan and apply (more with per-node cache thresholds, see below) | faults + knobs |
| `a write attempt is refused while an index builds` | Sometimes | the only write-path `INDEX_BUILDING` is a `createQueryCheckpoint` while the audit projection rebuilds after a restart (`admission.go` `checkQueryCheckpointProjectionReady`): reachable, kept | faults |
| `the leader reports a follower in snapshot progress` | Sometimes | a follower more than `RAFT_COMPACTION_MARGIN` entries behind while the leader reports it (`a follower reports an out-of-sync or snapshotting status` was hit once under local faults) | faults + knobs |
| `a restore cycle survives a restart of restore-0` | Sometimes | needs restore-0 killed mid-cycle | faults |
| `a cluster config change resets the cache epoch` | Reachable | needs per-node `CACHE_ROTATION_THRESHOLD`, which the compose file equalises until the server divergence below is fixed | knobs (disabled) |
| `a cluster config change was observed between quiet periods` | Sometimes | a leadership change that changes the persisted cluster config between two `cluster_config_convergence` runs | faults + knobs |
| `a chained restore verifies` | Sometimes | a drawn plan variant (chained, about one cycle in four); a cycle takes 5–10 minutes, so 45 minutes give a handful of cycles | no faults (long runs) |
| `an offline bootstrap with validation succeeds` | Sometimes | a drawn plan variant (offline × validate) | no faults (long runs) |

Every other must-hit was hit in at least one local run. The oracle image's
catalog contains no `singleton_driver_model:` literal and its symbol file
lists no `workload/internal` path, so the service does not link
`W/internal`.

## Requests to 99: dispositions

| From | Request | Disposition |
|---|---|---|
| 02 | check must-hits over the union of runs (hegel swarms rules) | done (union above). Root cause found: one `#[hegel::main]` test case fixed the swarm subset for the whole run; the driver now runs bounded test cases (below) |
| 03, 04, 14 | `draw::weighted` biased to the first weight | fixed in `draw.rs` (integer units, small-range `pick`); the families' local workarounds stay (harmless) |
| 03 | `ReadKind.Skip` for the Replay kind | not done: a verdict-precision improvement inside 00a's protocol, no false finding; left as is |
| 04 | mirror-promotion coverage needs 02/03 writes | reached in the merged build |
| 04 | maintenance window cool-down | not needed: no rule starved in the stack runs (no slow rule logged) |
| 05 | dedupe the accounts-filter builder with 06's | done: AggregateVolumes draws `filters::gen_account_filter_{free,indexed}` (typed leaves, has-asset); the aggregate validator now explains `FILTER_COMPILATION_ERROR` for filters the compiler must refuse and `AGGREGATE_OVERFLOW` when the model's fold overflows |
| 05 | O(log n) log accessors in `tests/oracle` | not done: the oracle's p99 stayed under 20 ms per read in the stack runs (`us` in the journal) |
| 06 | clean-EOF literals on shared ledgers; index-dependent literals | kept as 06 wrote them; the index-served literals are hit in the merged build |
| 07 | `HEGEL_HAVE_CHECKPOINTS=1` | set in the compose driver env and `run_native.sh` |
| 07 | remove the index-building write Sometimes if unreachable | kept: reachable (see the audit) |
| 08 | DESIGN §4.5 amendment (serialization beyond the ticket's life) | recorded here; DESIGN is outside the repository |
| 08 | dedupe with 06 | `has_asset` comes from `filters.rs`; the prepared-query filter keeps its own parameterised shapes (they feed 08's rejection fixtures) |
| 09 | `TestRejectionStages` with the server's checkpoint limit | done, plus fixture `09-checkpoint-limit-reached.json` |
| 10 | `audit failure entry with a retryable reason left unjudged` | downgraded to `AlwaysOrUnreachable(true, …)` (not must-hit): on the current server every named retryable FSM reason is never audited |
| 12 | a real "quiet period open" flag | `Shared.quiet_open` (Q0 to the end of the period), used by `quiet_maybe_open` |
| 12 | restore-target logs, checkpoint creation | reached in the merged build |
| 15 | fixture shape test vs the real usage provider | `mapPaths` treats provider content as data |
| 16 | other families during `signatures_required` | `Driver::guarded` skips every rule while it is set |
| 16 | drop the probe pinning | not done (kept as specified) |
| 17 | judge the 26 blocks on long runs | all 26 `block … succeeded` hit once rules re-swarm per test case |
| 18 | `extra_hosts`, restore RSS/volume, restored first boot | `snouty validate` accepts the compose file; restore-0 peaked at 398 MB RSS with a restored copy, its volume is wiped at the end of every cycle; the restore cycle verified with a non-empty delta in every compose run |

## Integration fixes (beyond the requests)

- `preparedcheck` event constants renamed (`EvListingOutsideModel`,
  `EvListUnexpectedError` clashed with 07/09 in the asserter AST test).
- The smoke rules, the `Smoke` read kind and its asserter are gone.
- The driver runs an unbounded sequence of bounded hegel test cases
  (`HEGEL_STEPS_PER_CASE`, default 2000) instead of one `#[hegel::main]` case:
  hegel records every choice of a case, so memory grew without bound, and the
  rule subset never changed within a run. `MALLOC_ARENA_MAX=2` keeps glibc
  from spreading freed memory over dozens of arenas.
- Restore admission probes read the served `{"data": micros}` timestamps.
- Task 15's and 18's listings ask the client for one item past the page (a
  page of exactly `maxItems` rows is reported incomplete).
- The oracle exits normally on SIGTERM (coverage counters) and journals each
  request's service time (`us`).
- `TestBlockEndpoints` (17) and `TestMuxServesOpAndExtensions` (00e) no
  longer run in parallel: both set the process-wide extension server, which
  made the block test fail about one run in six.
- A frozen `AGGREGATE_OVERFLOW` is explained before task 09's outcome matcher
  (which accepts only success and NotFound).
- The client shuts down gracefully (00e "Shutdown"). It closed its gRPC
  connections as soon as `ListenAndServe` returned, with RPCs still in flight,
  and reported grpc-go's own "grpc: the client connection is closing" as a
  remote `Canceled`: a stop fault on a client fired task 12's `Backup returned
  unexpected error` (run 3d8b6ff117f4ff08f7b9b40db1b5e083-67-0). The client now
  waits (bounded) for in-flight RPCs and never reports an outcome caused by its
  own close; the driver's backup rule (a remote `Canceled` is not tolerated) is
  unchanged.
- `minio` runs under a signal wrapper like `nats` (keeping `restart:
  on-failure`): as PID 1, minio exits 2 silently on a SIGTERM in its first
  ~0.3 s, before its handler is installed, which an Antithesis stop fault right
  after a kill-fault restart hit (same run). Both wrappers remap only when the
  trap fired (a flag), so a process killed by a signal the wrapper never got
  keeps its status (137) instead of a second `wait`'s 127.
- Quiet-period timing for short Antithesis timelines (run
  `3d8b6ff117f4ff08f7b9b40db1b5e083-67-0`: 12 compute hours, timelines up to
  about 8 minutes, `exact` drawn once and never finished, so none of the
  oracle's quiet checks fired). The trigger gaps are 60 s/240 s instead of
  150 s/480 s (DESIGN §4.10), the first two quiet periods of a process use
  30 s/120 s while no `exact` check has completed, and a gap counts from the
  end of the previous period's fault pause when that is later. `exact` weighs
  50 until it completes once in the process (and again after 3 quiet periods
  without a completion), 10 otherwise, instead of 4 (50 after 3).

## Server findings reported by task 99 (not masked in the checks)

1. **Per-node `CACHE_ROTATION_THRESHOLD` makes joining nodes diverge.** With
   thresholds 50/64/80 (DESIGN §8), the two nodes that join the bootstrap node
   never apply ledger-scoped entries: right after setup, `GetLog` sequence 3 is
   `createLedger o-…-l1` on node 1 and a different (cluster-scoped) entry on
   nodes 2 and 3, and every follower read of a tracked ledger answers "ledger
   not found". Equal thresholds on every node do not diverge (bisected on the
   compose stack). Node-local configuration must not change how a committed
   entry is applied (CLAUDE.md invariant 2). The compose file uses one
   threshold for now; restore 50/64/80 once fixed.
2. **`Backup` answers gRPC `Unknown` when its upload is cancelled during a
   leadership change** ("failed to propose backup terminal order … raft
   proposal dropped", then "upload multipart failed … context canceled" mapped
   to `unknown server error`). Both `client: every RPC error the client returns
   is classified` and task 12's `Backup returned unexpected error` fire; this
   is the error classification the old workload checked.
