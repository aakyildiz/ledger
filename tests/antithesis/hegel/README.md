# Antithesis hegel workload

This directory holds the hegel workload for the ledger: one Rust driver that
makes every random choice, a passive Go oracle service that checks every
tracked write and read against the forward model of `tests/oracle`, three Go
client containers that send every RPC, and the system under test (three
ledger nodes, MinIO, NATS, and two restore targets). The design is
`DESIGN.md` of the hegel workload effort; `PORTING.md` maps every assertion of
the old drivers to its new home.

## Layout

| Path | What it is |
|---|---|
| `docker-compose.yaml` | The deployment: `ledger-0..2` (3 voters), `minio` + `minio-init` (backups), `nats` (event sinks), `client1..3`, `oracle`, `driver`, `restore-0`/`restore-1`. Build context is the repository root. |
| `driver/` | The driver: a Rust [hegel](https://hegel.dev) concurrent state machine. It owns every draw (rules, shapes, targets, routes, backoffs, quiet and exclusive triggers) and every RPC, keeps the ticket protocol with the oracle, and holds the Rust-only checks (Numscript, stale-read probes, cluster operations, cross-node digests, restore parity, signing keys, scenario blocks, log signals). `driver/src/rules/` has one file (or folder) per family. |
| `oracle/Dockerfile` | The oracle service (`../workload/bin/oracle`, checks in `../workload/oraclecheck/**`, model in `../../oracle`). Passive and deterministic: no draws, no timers, no cluster access. It asserts every finding with a literal Antithesis assertion. Instrumented, built without `-race`. |
| `../workload/bin/client/`, `client/Dockerfile` | The client program (Go). One shared production-configured gRPC connection (round-robin + the library retry policy, route `lib`), a second round-robin connection without retries (`plain`), and direct per-node connections (`node`). It signs batches, computes deterministic digests, runs scenario blocks, classifies every RPC error, and reports late completions to the driver over UDP. |
| `ledger/Dockerfile` | The instrumented ledger server (`-race`), run under `forward-logs`. |
| `restore/Dockerfile`, `../workload/bin/restore-agent/` | The restore targets: `restore-agent` supervises one ledger-server child in restore, normal or join mode on the driver's commands (built without `-race`). `restore-1` (the joiner) idles unless the driver runs with `RESTORE_JOINER=1`. |
| `log-forward/`, `forward-logs` | Forward the ledgers', clients' and restore targets' logs to the driver (UDP 7777). The oracle's stdout is captured by Antithesis directly. |
| `porting/`, `PORTING.md` | One porting record per task (old assertion → new home, new properties, must-hit reachability, requests to integration); `PORTING.md` concatenates them with a summary table. |
| `protocol-fixtures/` | Golden request/response JSON of the driver ↔ oracle protocol, checked by both the Go and the Rust side. |
| `run_native.sh` | Native single-node smoke run (no Docker): builds everything, runs one node, the oracle, one client and the driver, fails on any failing assertion and lists never-hit must-hits with their reachability class. |
| `docker-compose.local.yaml` | Local runs only: SDK output to files, constant quiet/exclusive probabilities, the oracle's full journal and batch dumps, a raised disk-full gate for shared hosts. |
| `docker-compose.coverage.yaml` | Local runs only: `go build -cover` ledger nodes, clients, restore targets (child and `ledgerctl`) and oracle. |

## How a run goes

1. **Setup** (no draws, under 3 minutes; about 10–30 s locally). The driver
   waits for every client and node, posts `/setup` to the oracle, creates the
   four tracked ledgers `o-<run>-l1..l4` through the oracle, runs the families'
   setup hooks (the `default` ledger, the signing root key, the scenario
   groups, Numscript and probe ledgers), and reports setup complete.
   `first_start_workload` then lets the rules start.
2. **Rules.** All rules live in one hegel group; hegel draws one per step with
   the weights in `driver/src/rules/*.rs`. A write on a tracked ledger follows
   `/write/begin → RPC attempts → /write/end`: the same bytes and idempotency
   key are retried until the outcome is definitive (10 s in the rule, then a
   resolver thread takes over). A write slot semaphore keeps the oracle's
   in-flight set at most 8. A read follows `/read/begin` (ticket and views on
   one snapshot) `→ RPC → /read/end`, and the oracle validates the answer
   against every candidate serialization. Rust-checked writers use their own
   `r-<family>-<run>-*` ledgers; scenario blocks use the fixed scenario
   ledgers.
3. **Jobs.** Anything that would block a rule for long (backups, store checks,
   stress bulks, restore steps, maintenance disable, signature-window
   restore, delayed follow-ups) is drawn in a rule and run on a job thread.
   Jobs that send tracked writes hold quiet periods back.
4. **Quiet periods.** An `always_run` invariant draws a quiet period with a
   probability that rises with the time since the last one (1/5000 below
   150 s, up to 1/10 at 480 s): about one every 3–8 minutes of timeline. It
   stops faults with `$ANTITHESIS_STOP_FAULTS 100`, waits for every client
   (Q1), for maintenance to be closed, no signature window and no blocking job
   (Q2), for every resolver ticket to settle and the oracle to be idle, with a
   ticket audit before and after (Q3), opens the oracle's quiet epoch (Q4),
   re-issues the pause for the drawn check's budget and runs **one** drawn
   flavour (Q5), and closes the epoch (Q6). Flavours: `exact` (incremental
   audit replay, exact per-ledger state linearizable and per node; 150 s),
   `cross_node_identity` (90 s), `stats_convergence` (70 s),
   `cluster_config_convergence` (20 s), `eventually_correct` (90 s),
   `quiesce_and_absence` (45 s), `numscript_state` (30 s), `restore_cut`
   (60 s, only while a restore cycle waits for its cut).
5. **Exclusive windows.** A second invariant draws, on the same elapsed-time
   scheme, an action that must not overlap rules: task 16's
   `requireSignatures` window, which always restores the cluster setting
   within 20 s or hands the restore to a blocking job.
6. **Oracle failures.** A changed oracle boot id, an unreachable oracle for
   300 s, or a leaked ticket put the driver in degraded mode for the rest of
   the process: tracked families stop, Rust-only families continue.

Writes carry idempotency keys, so a definitive business rejection is final
(the oracle freezes it like the server). Timeouts, unavailability,
maintenance mode, stale proposals and similar errors are non-definitive and
retried with the same bytes.

## Running locally

Native smoke (no Docker, one node, about 2 CPUs):

```sh
QUIET_PROBABILITY=0.01 EXCLUSIVE_PROBABILITY=0.01 tests/antithesis/hegel/run_native.sh 600
```

On a host whose disk is above 80 %, export `HEALTH_WAL_THRESHOLD=0.995
HEALTH_DATA_THRESHOLD=0.995` first (the ledger's disk-full write gate).

Full stack:

```sh
cd tests/antithesis/hegel
export COMPOSE_FILE=docker-compose.yaml:docker-compose.local.yaml
docker compose build && docker compose up -d
docker compose exec -T driver /opt/antithesis/test/v1/main/first_start_workload
# kill, pause or restart ledger-N / clientN by hand to emulate faults,
# but not during a quiet period (no ANTITHESIS_STOP_FAULTS locally)
docker compose cp driver:/tmp/sdk.jsonl . ; docker compose cp oracle:/journal/oracle.jsonl .
```

A finding of the oracle is replayed offline, deterministically and with full
diagnostics, from its journal:

```sh
cd tests/antithesis/workload && go test ./bin/oracle -run TestReplayJournal -args -journal=/path/to/oracle.jsonl
```

Coverage: add `docker-compose.coverage.yaml` to `COMPOSE_FILE`, run, stop the
stack with `docker compose stop` (Go writes counters at a normal exit), copy
the `coverage` volume out and read it with `go tool covdata` (merge the
`ledger-*` and `restore-0` directories, then `go tool covdata textfmt` and
`go tool cover -func` from the repository root).

`docker compose build` can fail with `image "…" already exists` when several
services share one image (the three clients, the three ledgers, the two
restore targets) and the Docker engine uses the containerd image store; run it
again (the second pass is cached), or build one service per image
(`docker compose build driver client1 oracle ledger-0 restore-0`, as CI does).

## Launching

Validate (`snouty validate tests/antithesis/hegel`), then launch with the
`basic_test` launcher, faults excluded from the driver and the oracle (both
hold in-memory state the verdicts depend on). Iterate on 12-hour runs first;
use 24 hours for the final run:

```sh
snouty launch --launcher basic_test --config tests/antithesis/hegel \
  --fault-tolerance high_availability --fault-exclusions '^driver$|^oracle$' \
  --max-compute-hours 12
```

The first short run should be checked with `snouty runs events`: no kill,
stop, partition or CPU fault may target `driver` or `oracle`, and `the fault
pause command exists in Antithesis` / `the fault pause command succeeds` must
be hit.

**Triage note.** Ignore `tests/antithesis/workload/oraclecheck/**` and
`tests/antithesis/workload/bin/oracle/**` in `.results.coverage`: the oracle
is instrumented for its assertions, not for coverage.

### Coverage gaps (local 45-minute coverage run)

Measured by task 99 with `docker-compose.coverage.yaml` (ledger nodes, the
restore targets' server child and `ledgerctl`, clients, oracle), 45 minutes
with hand-made kills, pauses and restarts. Functions under the five audited
packages that never ran, by class; the Antithesis run checks them with
`snouty runs show … .results.coverage`:

| Package | Never run / functions | What is left |
|---|---|---|
| `internal/infra/node` | 52 / 240 | Membership changes (`AddLearner`, `PromoteLearner`, `RemoveNode`, `ForceRemoveNode`, drain, pending removals): operator-driven, out of scope. Snapshot catch-up (`TrySyncSnapshot`, `setOutOfSync`, checkpoint boundaries during replay): needs a follower more than `RAFT_COMPACTION_MARGIN` behind, i.e. longer partitions than local runs make (faults). Error/stop paths and the raft logger adapter's warning/fatal levels (faults). |
| `internal/infra/state` | 68 / 406 | Mirror sync (`applyMirrorSyncUpdate`, `QueueMirrorSync`, mirror status): needs an external mirror source, not in this deployment. Idempotency eviction: disabled on purpose (`IDEMPOTENCY_TTL=0`). Sentinel offense dumps and coverage-miss paths: only on a server bug. Event-sink cursors (`SetSinkCursor`): task 12 adds and removes NATS sinks within one rule, so no delivery advanced a cursor locally. Drain/marker-only applies (membership, out of scope). `Error()` methods of error types. |
| `internal/infra/backup` | 3 / 97 | Azure storage (disabled build), the rebuild's volume/metadata moves (restore of a backup with moved volumes; faults). |
| `internal/application/indexbuilder` | 9 / 174 | History-replay invariant failures, batch discard and backfill purge after a generation change (faults or a server bug). |
| `internal/application/admission` | 18 / 106 | Preload-unavailable classification (`markPreloadUnavailable`, `classifyResolutionFailure`: faults), TLS DSN checks, typed errors' accessors. |

The audited packages' statement coverage was 66.8 % (`node`), 70.4 %
(`state`), 73.2 % (`backup`), 71.2 % (`indexbuilder`) and 76.2 %
(`admission`). The remaining gaps are fault-only, out of scope, or disabled by
a knob on purpose.

## What was not ported

- The k8s operator drivers (`singleton_driver_scaling`, `_scaling_chaos`,
  `_scaling_structured`, `_rolling_restart`, `_quorum_recovery`,
  `_config_change`) need the operator. This deployment has none; Antithesis
  itself kills, pauses and partitions the nodes. The non-operator part of
  `_config_change` (every node reports the leader's cluster config) is task
  15's `cluster_config_convergence` flavour.
- Checks that assumed the old driver owned the whole audit trail (`audit tail
  misses the newest committed bulk`): other writers share the trail now.
- The per-node stale-read monotonicity idea (boot detection lags reads); the
  stale-read probe checks bounds on its own ledgers instead.
- The old random generation engine's own assertion (`generation engine
  failed`): hegel draws replace it.
- Everything else each family dropped, with its reason, is in `PORTING.md`
  ("dropped: …" rows).
