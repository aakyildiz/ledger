# 15 — Cross-node identity, stats convergence, cluster-config convergence

Three Rust quiet hooks (`HG/driver/src/rules/convergence.rs`, DESIGN §4.10) and one Go quiet
provider (`W/bin/oracle/h_usage.go`, `usage`). Every hook runs in its own quiet period (one
flavour per period: rules parked, resolvers drained, oracle quiescent), writes only to
`r-wit-<run>-<N>` and issues Barriers. `plan` draws the client that carries the pinned calls
(and, for `stats_convergence`, the seed of the foreign-ledger sample); `run` draws nothing.

| Hook | Budget | Weight | `needs_oracle` |
|---|---|---|---|
| `cross_node_identity` | 90 s | 1 | no |
| `stats_convergence` | 70 s | 1 | yes (`providers.usage`) |
| `cluster_config_convergence` | 20 s | 0.5 | no |

Liveness rule: verdicts and the "connects" / "obtains a qualified observation" Always
properties are evaluated only when the hook ran to completion. A deadline stop reports
`<hook> completes within its quiet budget` (Sometimes, false) and evaluates nothing else.
Attempts end `Late` (a fence moved: late write, election entry, membership change — never a
finding), `Other` or `Cut` (deadline). The qualified-observation Always is false only when
every attempt ended `Other`.

## Old assertions

| Old source | Old assertion (kind: literal) | New | Where |
|---|---|---|---|
| MD/eventually_cross_node_identity/main.go:24 | Always: cross-node identity oracle connects to replicas | verbatim (on completion; true when the quiet period's node discovery answered — the old per-node dial — or any candidate gave a non-local answer to a pinned Barrier) | convergence.rs `cross_node_identity` |
| MD/eventually_cross_node_identity/main.go:40 | Always: cross-node identity oracle obtains a qualified all-member observation | verbatim (on completion; false only when every attempt failed for a reason other than a late write or membership change) | convergence.rs `cross_node_identity` |
| MD/eventually_cross_node_identity/main.go:45 | Sometimes: cross-node oracle reaches quiescence | verbatim | convergence.rs `publish_cross` |
| MD/eventually_cross_node_identity/main.go:46 | Sometimes: at least two nodes converge to a common applied index | verbatim | `publish_cross` |
| MD/eventually_cross_node_identity/main.go:47 | Reachable: all current replicas reach the same durable identity horizon | verbatim | `publish_cross` |
| MD/eventually_cross_node_identity/main.go:49 | Always: complete audit listing and point tip agree at a common applied index | verbatim; the listing is the bounded window `[max(1, A − 5000), A]` (A = the audit head) instead of the whole chain (main-drivers §3.2 timing gotcha): the window digest must end at the tail's sequence, and the window's last entry (a forward one-item listing filtered to that sequence) must equal the tail (`pageSize:1, reverse:true`) as a whole entry (old `proto.Equal(lastListed, tail)`); the `GetAuditEntry` tip with items stripped must equal the tail as before | `publish_cross` |
| MD/eventually_cross_node_identity/main.go:65 | Always: account state is identical across nodes at a common applied index | verbatim; compares the `/digest` sections `ledgers` and `accounts` (all ledgers), reports the first differing ledger | `publish_cross` |
| MD/eventually_cross_node_identity/main.go:67 | Always: audit hash is identical across nodes at the same sequence | verbatim (same window digest, same tail, same last listed entry, same tip **items included** — the old `TipDigest` over the unstripped tip) | `publish_cross` |
| MD/eventually_cross_node_identity/main.go:68 | Always: audit hash version is identical across nodes at the same sequence | verbatim (tail `hashVersion`) | `publish_cross` |
| MD/eventually_cross_node_identity/main.go:72 | Reachable: cross-node complete account and audit identity verified | verbatim; **deviation**: also reached with a single member (the old driver returned before it with fewer than two observations). Task 15's acceptance requires it on the one-node native run; the two-member exercise is shown by `at least two nodes converge to a common applied index` | `publish_cross` |
| MD/eventually_stats_consistency/main.go:26 | Always: stats convergence oracle connects to replicas | verbatim (on completion; same rule as the cross-node one: discovery answered or a candidate answered non-locally) | convergence.rs `stats_convergence` |
| MD/eventually_stats_consistency/main.go:42 | Always: stats oracle obtains a qualified observation within its bounded retry budget | verbatim (on completion, same rule as above) | `stats_convergence` |
| MD/eventually_stats_consistency/main.go:48 | Always: usage counters converge exactly at a fixed source horizon | verbatim, **evaluated only for a member with a certified sweep** (witness `referenceCount == 1` before and after the sweep) or a definitive divergence (ledger incarnation differs, non-transient read failure past the horizon): true when a certified sweep matched, false when the window ended with the last certified sweep mismatching. A member that never certified (usage store still rebuilding after a restart) gets no usage verdict; it is reported by the new `the usage witness certifies on a member within the stats window` Sometimes (**deviation**: the old driver reported it as a usage violation, but it ran once at the end of the test, not right after faults stop). Tracked ledgers' expected counts come from the oracle (`providers.usage`) and are checked inside the capture fence against the source's synchronous `GetLedgerStats {transactionCount, logCount}` (`txCount`, `logCount`; a difference fails the attempt as Other, never a usage verdict); foreign ledgers' from the ListLogs fold (contiguous ids, `payload.apply.ledgerName == ledger`, fold log count == main-store `logCount`), every untracked ledger of the tracked partition (reserved `o-<run>-own<N>`) plus a plan-seeded sample of 8 other foreign ledgers, each with at most 5000 logs (**deviation**: the old driver folded every ledger; the full fold made the hook's budget run out on long timelines) | `publish_stats` |
| MD/eventually_stats_consistency/main.go:50 | Reachable: stats usage convergence verified | verbatim | `publish_stats` |
| H/main.rs:1223 | Sometimes: every node catches up after faults stop | dropped: superseded by `all current replicas reach the same durable identity horizon` (exact per-node horizon, DESIGN §6.10) — the old check read the leader's index for every node (main-drivers §1.3 #1) | – |
| H/main.rs:1242 | Always: every node holds the same account state at a common index | dropped: superseded by `account state is identical across nodes at a common applied index` (full account digests per member, not a fixed address sample read at the leader's index) | – |
| MD/singleton_driver_config_change/main.go:174 | Reachable: follower observed same ClusterConfig as leader | replaced by the new `cluster config convergence verified` / `every node's cluster config equals the leader's at a common horizon` (no K8s patch; compares every member, not one follower) | convergence.rs `publish_config` |
| MD/singleton_driver_config_change/main.go:134, 138, 153, 157, 160 | Sometimes: config patch should succeed; Reachable: config patch applied; Sometimes: StatefulSet should reach Ready after a config patch; Reachable: STS ready after config patch; Sometimes: ClusterConfig should converge to the patched value | dropped: K8s/operator out of scope (DESIGN §1 item 7) | – |

## New properties

| Literal | Kind | Hook |
|---|---|---|
| `cross_node_identity completes within its quiet budget` | Sometimes | cross_node_identity |
| `stats_convergence completes within its quiet budget` | Sometimes | stats_convergence |
| `cluster_config_convergence completes within its quiet budget` | Sometimes | cluster_config_convergence |
| `the usage witness certifies on a member within the stats window` | Sometimes (per member of a qualified stats observation; false when the member's usage store never certified the witness in the window) | stats_convergence |
| `every node's cluster config equals the leader's at a common horizon` | Always (on completion, per member; skipped when the leader's config is the unpersisted local fallback `{rotationThreshold}`, `internal/adapter/grpc/server_cluster.go:155-160`) | cluster_config_convergence |
| `cluster config convergence verified` | Reachable | cluster_config_convergence |
| `a cluster config change was observed between quiet periods` | Sometimes (the leader's whole `clusterConfig` differs from the previous completed run's; per-node `CACHE_ROTATION_THRESHOLD`/`BLOOM_*` make a new leader propose its own config, DESIGN §8) | cluster_config_convergence |

## Protocol details

- Source selection (every hook): a pinned Barrier on each candidate (discovery, then the
  configured nodes); while some answer is transient (no leader, restart), rounds are retried
  with backoff for up to 15 s or a quarter of the remaining budget.
- Cross-node attempt (up to 3): pinned source Barrier `before`; membership from the source's
  `GetClusterState{}` (unique non-zero ids and addresses, suffrage Voter/Learner); Barrier
  `horizon == before + 1`; every member (in parallel, one 60 s window) must report
  `localNode == id`, status normal and `lastPersistedIndex == horizon` (above it → `Late`);
  per member stale pinned tail, the forward listing at the tail's sequence, and the tip;
  `POST /digest` `{node, consistency:"stale", sections:[ledgers, accounts, audit], auditFrom}`
  (retried once on failure; a failure with less than 1 s of budget left is a cut, not Other);
  the member must still be exactly at the horizon; membership unchanged and closing Barrier
  `== horizon + 1`.
- Stats attempt (up to 3): pinned source; membership; witness `r-wit-<run>-<N>` created
  (keyed, `send_untracked` pinned to the source); Barrier `before`; source capture
  (ListLedgers; tracked rows from `providers.usage`, each checked against the source's
  `GetLedgerStats {transactionCount, logCount}`; untracked partition rows and a seeded sample
  of 8 foreign rows with ≤ 5000 logs folded; deleted rows and the witness skipped); Barrier
  `after == before + 1`; one forced witness tx with `reference = witness`; Barrier
  `== after + 2` (the horizon); per member (parallel, one window of
  `min(60 s, remaining − 3 s)`, so the window always ends before the closing fence's
  reserve; under 1 s is a cut): `lastPersistedIndex ≥ horizon`, then stale sweeps: witness
  incarnation, witness `referenceCount == 1`, every source ledger's incarnation and
  `{logCount, postingCount, revertCount}`, witness re-check (the usage store can rewind; only
  a sweep certified at both ends counts); closing Barrier `== horizon + 1`.
  `QuietCtx.begin == None` (or no `providers.usage`) returns quietly.
- Cluster-config attempt (up to 3): pinned source; membership and leader; Barrier; every
  member `lastPersistedIndex ≥ horizon` with its own `GetClusterState{nodeId}`; compare each
  member's `clusterConfig` with the leader's (only when the leader's config is persisted:
  a config with no field besides `rotationThreshold` is the per-node fallback); membership/leader unchanged and closing Barrier
  `== horizon + 1` (added fence: a config proposal between the reads would otherwise be a
  false difference).
- Go provider `usage`: `{name: {id, logCount, txCount, postingCount, referenceCount,
  revertCount}}` (keyed by ledger name, JSON numbers) for every live tracked ledger (`match.ModelUsage`, model log
  and transaction counts, learned ledger id).

## Reachability

| Must-hit | Reached by | Needs |
|---|---|---|
| `cross_node_identity completes within its quiet budget` | cross_node_identity | no faults |
| `stats_convergence completes within its quiet budget` | stats_convergence | no faults |
| `cluster_config_convergence completes within its quiet budget` | cluster_config_convergence | no faults |
| `cross-node identity oracle connects to replicas` | cross_node_identity | no faults |
| `cross-node identity oracle obtains a qualified all-member observation` | cross_node_identity | no faults |
| `cross-node oracle reaches quiescence` | cross_node_identity | no faults |
| `at least two nodes converge to a common applied index` | cross_node_identity | 3-node cluster (task 99; native run has one node, so only `false` there) |
| `all current replicas reach the same durable identity horizon` | cross_node_identity | no faults |
| `complete audit listing and point tip agree at a common applied index` | cross_node_identity | no faults |
| `account state is identical across nodes at a common applied index` | cross_node_identity | 3-node cluster (task 99) |
| `audit hash is identical across nodes at the same sequence` | cross_node_identity | 3-node cluster (task 99) |
| `audit hash version is identical across nodes at the same sequence` | cross_node_identity | 3-node cluster (task 99) |
| `cross-node complete account and audit identity verified` | cross_node_identity | no faults (one member suffices) |
| `stats convergence oracle connects to replicas` | stats_convergence | no faults |
| `stats oracle obtains a qualified observation within its bounded retry budget` | stats_convergence | no faults |
| `usage counters converge exactly at a fixed source horizon` | stats_convergence | no faults |
| `stats usage convergence verified` | stats_convergence | no faults |
| `the usage witness certifies on a member within the stats window` | stats_convergence | no faults |
| `every node's cluster config equals the leader's at a common horizon` | cluster_config_convergence | no faults |
| `cluster config convergence verified` | cluster_config_convergence | no faults |
| `a cluster config change was observed between quiet periods` | cluster_config_convergence | faults (a leadership change between two runs of the hook, 3-node cluster with per-node cache/bloom config, DESIGN §8) |

## Requests to 99

- The three-node Always properties (`account state is identical …`, `audit hash … identical`,
  `audit hash version …`) and `at least two nodes converge to a common applied index` are
  only meaningful on the compose stack; the native one-node run evaluates them vacuously or
  as `false` Sometimes. Check them on the local stack.
- `a cluster config change was observed between quiet periods` needs a leadership change
  between two `cluster_config_convergence` runs; mark it "needs faults".
- Witness ledgers `r-wit-<run>-<N>` accumulate (one per stats attempt, one transaction each)
  and are folded by later stats runs; they are never deleted. If a long timeline shows the
  ListLedgers/fold cost growing, cap or delete them.
- Foreign ledgers are sampled (8 per attempt, plan-seeded) and those with more than 5000
  logs are never folded, so `r-stress-*` bulks are not usage-checked by this hook. Task 99
  should record how often `stats_convergence completes within its quiet budget` and
  `the usage witness certifies on a member within the stats window` are false on the stack.
- **Fixture conflict (blocks `go test ./bin/oracle/...` until fixed).** 00a's
  `TestFixtures_ResponseShapes` compares `/quiet/begin`'s `providers` against
  `HG/protocol-fixtures/core/quiet-begin.json`, whose `usage` placeholder is
  `{"o-…-l3": {"transactions": "57", "accounts": "12"}}`. `mapPaths` exempts only the
  `providers` level, so once a real `usage` provider is registered the test demands that
  exact ledger key set and those two string fields. The provider's content is
  provider-defined (DESIGN §4.10, the `mapPaths` comment says so). Fix in 99 (owner of the
  core files): add `"providers.*": true, "providers.*.*": true` to `mapPaths` in
  `W/bin/oracle/fixtures_test.go` (verified: with that one-line change
  `go test -short ./bin/oracle/` passes on this branch; not committed here because COMMON
  forbids edits of 00a's files), or regenerate the fixture's `usage` example as
  `{"<ledger>": {"id": 3, "logCount": 3, "txCount": 1, "postingCount": 1,
  "referenceCount": 1, "revertCount": 0}}`. Every other oracle test passes.
- The native acceptance run (`QUIET_PROBABILITY=0.05 RULES=smoke_tx run_native.sh 180`) was
  not run: the machine's root disk was full (0–100 MB free for most of the task, shared by
  the parallel agents). Run it once in 99.
