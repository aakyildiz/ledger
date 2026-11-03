# Protocol fixtures (driver ↔ oracle)

Golden JSON for the protocol between the Rust hegel driver (`driver/`) and the Go oracle
service (`tests/antithesis/workload/bin/oracle`), normative in DESIGN §4. The Go service and
the Rust driver test against these same bytes independently: each request decodes into the
side's request struct and re-encodes to the same canonical JSON (sorted keys), and each
response the service produces for a fixture's scripted state has the fixture's shape (same
keys, same JSON types).

## Format

One file per case, one JSON object per file:

```json
{"endpoint": "POST /write/begin", "request": {…} | null, "status": 200, "response": {…}}
```

- `endpoint`: HTTP method and path. A `GET` with query parameters carries them in the path
  (`GET /journal?from=41&limit=2`).
- `request`: the request body, `null` for a `GET`.
- `status`: the HTTP status.
- `response`: the response body. Every response carries `"bootId"` (decimal nanoseconds
  at service start). An error is `{"bootId", "error": {"code", "message", "details"}}`
  with the codes of DESIGN §4.1. `message` and `details` are informative: tests compare
  `status` and `error.code` exactly, and `details` only by JSON type.

`outcome.<shape>.json` files are not endpoints. They hold one client outcome object
(DESIGN §4.5) in `response` (`endpoint` is `"outcome"`, `request` is `null`, `status` is
`200`, no `bootId`) plus the expected classification:
`"definitive"` (`Outcome.Definitive()`, a write outcome) and `"transient"`
(`Outcome.Transient(false)`, the skip class of a read).

### Values

- Number convention (DESIGN §4.1): harness counters are JSON numbers (`ticket`,
  `version`, `epoch`, `seq`, `attempts`, `opId`, `client`, counts such as `txCount`,
  `nextLedger`, `inflight`). Server values are decimal strings: log sequences
  (`maxLogSeq`), transaction ids, checkpoint ids, `auditCursor`, `queryCheckpointLimit`,
  timestamps (`Timestamp.data`), amounts in views. `bootId` is a decimal string.
- Protobuf payloads (`requests`, `request`, `outcome.response`, `outcome.items`,
  view `id` of an index) are protojson with camelCase names, enums by full name, 64-bit
  integers as strings, bytes in base64, and zero values omitted — exactly what
  `protojson.MarshalOptions{UseProtoNames: false}` writes. They were produced by
  round-tripping through the generated `servicepb`/`commonpb` types.
- Client outcomes omit empty and false fields, as `bin/client` writes them
  (`omitempty`): `local` appears only when true, `reason` only when set.
- Enum-valued harness fields use the proto enum's full name (`LEDGER_MODE_NORMAL`,
  `CHART_ENFORCEMENT_AUDIT`, `ACCOUNT_TYPE_NORMAL`, `METADATA_TYPE_INT64`).
- Every view that takes a ledger (`ledger`, `seek`, `stamps`) echoes the resolved
  `"ledger"` name in its response, whether the request used `ledger` or `ledgerPick`;
  with `ledgerPick` and no live ledger the whole view answer is `{"ledger": null}`. A
  `seek` that finds nothing answers `"found": false, "entry": null`.
- `verdict` is always `{"ok", "skipped", "violations": [{"id", "details"}]}`.

## Naming

`core/<endpoint>[.<case>].json`, where `<endpoint>` is the path with `/` turned into `-`
and the leading `/` dropped (`/write/begin` → `write-begin`), and `<case>` names the
scenario or the error (`write-begin.busy`, `setup.conflict`). The file without a case is
the plain success.

| File | Endpoint | Status / code |
|---|---|---|
| `health` | `GET /health` | 200 |
| `setup`, `setup.conflict` | `POST /setup` | 200; 409 `setup` |
| `view` | `POST /view` | 200 (every core view) |
| `write-begin`, `.checkpoint`, `.conflict` | `POST /write/begin` | 200 (plain; `predicted.checkpointId`; conflict role) |
| `write-begin.unmodeled`, `.too-many-requests` | `POST /write/begin` | 400 `unmodeled` |
| `write-begin.protocol-conflict-equal`, `.protocol-conflict-untracked`, `.quiet` | `POST /write/begin` | 400 `protocol` |
| `write-begin.busy`, `.backlog`, `.serialized` | `POST /write/begin` | 409 `busy`, `backlog`, `serialized` |
| `write-end.pending`, `.validated-failure` | `POST /write/end` | 200 |
| `write-end.protocol-nondefinitive` | `POST /write/end` | 400 `protocol` |
| `write-end.unknown-ticket` | `POST /write/end` | 404 `unknown_ticket` |
| `write-late` | `POST /write/late` | 200 |
| `write-abandon`, `.protocol-attempts` | `POST /write/abandon` | 200; 400 `protocol` |
| `read-begin`, `.empty-live` | `POST /read/begin` | 200 (`ledgerPick` + `seek` views; `{"ledger": null}`) |
| `read-end`, `.skipped`, `.violation` | `POST /read/end` | 200 (validated; skipped; one violation) |
| `read-abandon` | `POST /read/abandon` | 200 |
| `quiet-status` | `GET /quiet/status` | 200 |
| `quiet-begin`, `.not-quiet`, `.maintenance` | `POST /quiet/begin` | 200; 409 `not_quiet` (the second with `details.reason: "maintenance"`) |
| `quiet-section`, `quiet-end` | `POST /quiet/section`, `POST /quiet/end` | 200 |
| `tickets`, `journal`, `dump` | `GET /tickets`, `GET /journal`, `GET /dump` | 200 |
| `panic` | `POST /read/end` | 500 `panic` |
| `outcome.{ok-unary,ok-stream,err-definitive,err-nondefinitive,err-local,unknown}` | – | client outcome shapes of DESIGN §4.5 |

The values inside a file describe one plausible service state; fixtures are not a
scripted session and need not be consistent with each other.

## Definitive reasons

`core/definitive_reasons.txt` lists the definitive failure reasons of DESIGN §4.5, one per
line, sorted: every `commonpb.ErrorReason` name without the `ERROR_REASON_` prefix whose
`domain.KindForReason` is freezable (`domain.IsFreezableFailure`: Validation, NotFound,
AlreadyExists, Conflict, Precondition), minus `NON_DEFINITIVE_OVERRIDES = {AUDIT_DISABLED}`.
Regenerate and check it from the repository root:

```sh
go run ./tests/antithesis/hegel/protocol-fixtures/core/gen \
  | diff - tests/antithesis/hegel/protocol-fixtures/core/definitive_reasons.txt
```

`driver/src/definitive_reasons.txt` is a byte-identical copy, and the oracle's
`reasons_test.go` checks both.

## Rules for later tasks

`core/` belongs to task 00 and is frozen: later tasks never edit, rename or delete a file
in it. A task that adds endpoints, views, read kinds or quiet sections adds **new** files
under `protocol-fixtures/<NN>/` (its task number), with the same format and naming, and
tests its own code against them.
