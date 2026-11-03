# 00e — Go client program and compose files

Client program `tests/antithesis/workload/bin/client` (DESIGN §7 C1–C7) and compose/env
(DESIGN §2.3, §2.4, §8).

## Ported classification (C3)

The old workload asserted, in a gRPC interceptor on every connection, that every RPC error
is classified. The client now asserts the same property after every RPC it sends (`/op`,
every `/digest` RPC, and extensions through `assertClassified`), on the final error of the
call, including mid-stream `Recv` errors (the old interceptor only saw stream creation).

| Old source | Old assertion (kind: literal) | New | Where |
|---|---|---|---|
| `W/internal/client.go:212-232` (`classifyUnaryInterceptor`) | Always: every RPC error must be classified (workload predicate set complete) | Always: `client: every RPC error the client returns is classified`, details `{method, code, reason, node}` | `CL/classify.go` `assertClassified`, called from `CL/main.go` `callRoute` |
| `W/internal/client.go:234-259` (`classifyStreamInterceptor`) | same literal | same new literal; covers stream creation **and** mid-stream errors | `CL/main.go` `call` (stream path), `CL/digest.go` `digester.rpc` |

Predicate mapping (`W/internal/client.go` → `CL/classify.go` `classified`):

| Old predicate | Codes / reasons | New |
|---|---|---|
| `err == nil` | – | `out.OK` |
| `IsUnavailable` (`:297`) | Unavailable | kept |
| `IsDeadlineExceeded` (`:311`) | DeadlineExceeded | kept |
| `IsExternalServiceError` (`:437`) | reason `EXTERNAL_SERVICE_ERROR` under any code | kept |
| `IsWritesBlockedDiskFull` (`:380`) | ResourceExhausted + `WRITES_BLOCKED_DISK_FULL` | kept (exact code + reason tuple) |
| `IsCanceled` (`:343`) | Canceled | kept |
| `isBusinessError` (`:532`) | NotFound, AlreadyExists, InvalidArgument, FailedPrecondition | kept |
| – (new, DESIGN §7 C3) | Unauthenticated, PermissionDenied | classified: signing is exercised (task 16, main-drivers §3.36) |
| – (new) | Unimplemented | classified only when `node` starts with `restore-` (RestoreService on a restore-agent child not in restore mode) |
| – (new) | `local: true` outcomes (dial failure, marshalling, client-side message-size limit) | classified: no server produced them |
| not classified (old and new) | Aborted, Internal, Unknown, OutOfRange, DataLoss, other ResourceExhausted, Unimplemented elsewhere | Always fails |

## New properties

- `client: every RPC error the client returns is classified` (Always; reached by every RPC
  of every rule; no faults needed for the true branch).

## Reachability

| Assertion | Kind | Reached by |
|---|---|---|
| `client: every RPC error the client returns is classified` | Always | every `/op` (no faults) |

## Client changes (DESIGN §7)

- C1 `CL/routes.go`: `route` `""`/`lib` (round-robin + `actions.GRPCRetryPolicy`), `plain`
  (round-robin, no retry policy), `node` (direct, no retries). Every connection dials with
  `WithDefaultCallOptions(MaxCallRecvMsgSize(64<<20), MaxCallSendMsgSize(64<<20))`. Unknown
  route → HTTP 400.
- C2 `CL/main.go`: `local`, `peer`, `elapsedMs` (omitted when zero/false, as the fixture
  README says). The connections dial through a dialer that reports the dialed address as
  the remote address, so `peer` reads `ledger-1:8888` (the name the driver uses) rather than
  an IP. A ResourceExhausted whose message is one of grpc-go's own size-limit messages and
  that carries no ErrorInfo is `local` (the server's limits are 64 MiB too, so a server
  never produces one for a request the client let through).
- C4 `CL/sign.go`: `sign: {keyId, seed}` on bucket `Apply`; `POST /pubkey`.
- C5: `services["restore"]`; a restore call without `node` → HTTP 400.
- C6 `CL/digest.go`: `POST /digest` (format documented in the file header).
- C7 `CL/ext.go`: `registerExt(pattern, h)`; handlers reach the client through
  `extServer()`.

### Deviations

- **Signing payload (C4).** `signing.Sign` serializes with `MarshalVT`, whose map
  iteration (request metadata, account types) is unordered, so two signings of one batch
  can produce different payload bytes, contradicting "deterministic, so retries resend
  identical bytes". `signBatch` builds the same `SignedApplyBatch` envelope (`keyId`,
  `ed25519.Sign(key, payload)`, `payload`) over `proto.MarshalOptions{Deterministic:true}`
  bytes. `sign_test.go` verifies it with `signing.Verify` and `signing.ExtractBatch` and
  checks byte-identical payloads and signatures over repeated signings of a 20-key batch.
- **Digest extras (C6).** Additive fields beyond DESIGN's shape: request `route` and
  `pageSize` (default 100); response `boot`, `opId`, per-ledger breakdown
  `sections.<name>.ledgers.<ledger> = {count, sha256, lastKey}` for per-ledger sections
  (restore.md §5.2: per-ledger hashing localises a failure), and `outcome` (the failing
  RPC's outcome) next to `error`. Per-ledger section hashes include the framed ledger name
  before each ledger's items. Empty `sections` means all sections. Without `ledgers`, deleted
  ledgers (`deletedAt` set) are skipped by the per-ledger sections. `indexes` uses
  `ListIndexes{SCOPE_ALL}` (filtered to `ledgers` when given); `sinks` digests the sink
  configurations only; `checkpoints` uses `ClusterService.ListQueryCheckpoints`.

## Compose (DESIGN §2.3, §8)

- `x-qcl: &qcl "10"` used by ledger `QUERY_CHECKPOINT_LIMIT`, oracle and driver.
- Ledger env: `DEBUG`, `SNAPSHOT_THRESHOLD` removed; `LOG_LEVEL=debug`,
  `RAFT_COMPACTION_MARGIN=100`, `MAINTENANCE_INTERVAL=5s`, `SPOOL_SEGMENT_MAX_BYTES=4096`,
  `RAFT_REPLAY_BATCH_SIZE=32`, `SNAPSHOT_PARALLELISM=1`, `PEBBLE_MAX_CHECKPOINTS=3`,
  `IDEMPOTENCY_TTL=0s`; per node `CACHE_ROTATION_THRESHOLD` 50/64/80 and
  `BLOOM_VOLUMES_EXPECTED_KEYS` 1000000/1000001/1000002.
- `oracle` service (image `ledger-hegel-oracle`, `HG/oracle/Dockerfile` from 00a),
  entrypoint `/usr/local/bin/oracle`, `ORACLE_ADDR=:8090`, no `forward-logs`, no
  `ORACLE_DUMP_BATCHES`.
- Driver env `ORACLE_URL`, `QUERY_CHECKPOINT_LIMIT`.
- Empty `# ==== 18-restore` marker pairs at the end of `services:` and of `volumes:`.
- `docker-compose.local.yaml`: driver `QUIET_PROBABILITY`, `EXCLUSIVE_PROBABILITY`,
  `ANTITHESIS_SDK_LOCAL_OUTPUT`; oracle `ORACLE_JOURNAL_FILE=/journal/oracle.jsonl`,
  `ORACLE_DUMP_BATCHES=1`, `ANTITHESIS_SDK_LOCAL_OUTPUT=/journal/sdk.jsonl`, named volume
  `journal`.

## Requests to 99

- The client's classification assertion literal changed from the old workload literal; the
  Antithesis report shows it under the new name.
- `HG/client/Dockerfile` unchanged: the new imports (`restorepb`, `signaturepb`) live in the
  root module directories it already copies.
