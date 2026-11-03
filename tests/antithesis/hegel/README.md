# Antithesis hegel workload

This directory holds a single-driver Antithesis workload for the ledger. It
ports the property drivers under `../workload/bin/cmds/` to one model-based
driver, and the deployment is a Docker Compose stack.

## Layout

| Path | What it is |
|---|---|
| `docker-compose.yaml` | The deployment: 3 ledger nodes, MinIO (backups), NATS (event sinks), 3 client containers and the driver. Build context is the repository root. |
| `driver/` | The driver: a Rust [hegel](https://hegel.dev) concurrent state machine. It is the only process that makes random choices, and it holds the model and every assertion. |
| `../workload/bin/client/` | The client program (Go). It holds one shared connection built by the workload's `internal.NewGRPCConn`, which round-robins over every node, retries with backoff and classifies errors. It runs any `BucketService` or `ClusterService` RPC that the driver sends it as protojson over HTTP. |
| `client/Dockerfile` | Builds the client program with Antithesis instrumentation. |
| `ledger/Dockerfile` | Builds the instrumented ledger server, as `Dockerfile.antithesis` does. |
| `log-forward/`, `forward-logs` | Forward every container's logs to the driver (UDP 7777). |
| `docker-compose.local.yaml` | Local runs only: SDK output to `/tmp/sdk.jsonl` and frequent quiet periods. |
| `docker-compose.coverage.yaml` | Local runs only: `go build -cover` ledger nodes. |

## How a run goes

1. The driver waits for every client and every node. It creates the
   `hegel-a` and `hegel-b` ledgers, waits until every client can read them,
   and then reports setup complete.
2. Antithesis runs `first_start_workload` in the driver container at the start
   of each timeline. The driver then starts its rules.
3. Each rule draws a client and sends one action through it. The rules cover:
   - transactions (postings, force, metadata, account metadata, skippable
     duplicate references), atomic batches including batches built to fail,
     and Numscript;
   - idempotency-key replays, both identical and with a changed payload;
   - reverts, sometimes concurrent;
   - account metadata set and delete;
   - linearizable and stale reads, paging through `ListTransactions` and
     `ListLogs`, `AggregateVolumes` and `Barrier`;
   - ephemeral ledger create and delete;
   - cluster admin: `TransferLeadership`, compaction, checkpoints, S3 backup,
     maintenance mode and NATS event sinks.
4. Now and then, a quiet period stops the faults. The driver waits for every
   client to serve again, then checks the full state against the model:
   - every acknowledged write survives;
   - no definitively rejected write committed;
   - each reference maps to one transaction, and each transaction is
     reverted at most once;
   - balances equal the committed postings, and only forced writes overdraw;
   - ledger stats agree with what is committed;
   - every node holds the same account state at a common index.

Writes carry idempotency keys, so a business rejection is definitive. A
timeout, an unavailable cluster, maintenance mode and similar errors leave
the outcome unknown. The model then accepts either result until a quiet
period or a replay settles it.

## Running locally

```sh
cd tests/antithesis/hegel
export COMPOSE_FILE=docker-compose.yaml:docker-compose.local.yaml
docker compose build && docker compose up -d
docker compose exec -T driver /opt/antithesis/test/v1/main/first_start_workload
docker compose cp driver:/tmp/sdk.jsonl .
```

## Launching

Build the images (`docker compose build`) and validate the config
(`snouty validate .`). Then launch with the `advanced_test` launcher, which
keeps faults off the driver:

```sh
snouty launch --launcher advanced_test --config . \
  --max-compute-hours 24 \
  --param 'custom.network_fault_exclusion_patterns=^driver$' \
  --param custom.container_status_fault_exclusions=driver \
  --param custom.container_cpu_fault_exclusions=driver
```

## What was not ported

- The k8s operator drivers (scaling, rolling restart, quorum recovery, config
  change) need the operator. This deployment has none. Antithesis itself
  kills, pauses and partitions the ledger nodes here.
- Incremental backup and restore.
- Signing keys, typed metadata schemas, indexes and prepared queries, query
  checkpoints, account types, and the cross-node audit-hash identity check.
  The rules are built so these can be added the same way.
- The `model` template's exhaustive serialization oracle (`tests/oracle`).
