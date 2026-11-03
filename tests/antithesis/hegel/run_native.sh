#!/usr/bin/env bash
# Native smoke run of the hegel workload (DESIGN §10, task 00d): no Docker.
#
# Builds the ledger server, the oracle, the client, log-forward and the driver
# natively; starts one bootstrapped ledger node, the oracle, one client and the
# driver on free local ports; lets the driver run for ${1:-120} seconds; then
# fails on any failing assertion in the SDK outputs and lists the must-hit
# assertions that were never hit (minus a whitelist).
#
#   tests/antithesis/hegel/run_native.sh [seconds]
#
# Environment:
#   RULES                  comma-separated allow-list of driver rule names
#   QUIET_PROBABILITY      constant quiet-period probability (default 0.02)
#   EXCLUSIVE_PROBABILITY  constant exclusive-window probability (forwarded)
#   LEDGER_NODES           overrides the node list given to the driver
#   KEEP_TMP=1             keep the run directory on success (always kept on failure)
#   CARGO_TARGET_DIR       where cargo builds (default: <driver>/target)
#
# The driver's UDP receivers use the fixed ports 7777 (logs) and 7778 (late
# outcomes); every other port is picked free at start, so a running Docker
# stack (which publishes nothing on the host) never collides. Two native runs
# at once do collide on the UDP ports.
set -euo pipefail

DURATION="${1:-120}"
HG="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HG/../../.." && pwd)"
W="$ROOT/tests/antithesis/workload"
TMP="$(mktemp -d "${TMPDIR:-/tmp}/hegel-native.XXXXXX")"
echo "run_native: run directory $TMP"

PIDS=()
cleanup() {
  local status=$?
  for pid in "${PIDS[@]}"; do kill -TERM "$pid" 2>/dev/null || true; done
  sleep 1
  for pid in "${PIDS[@]}"; do kill -KILL "$pid" 2>/dev/null || true; done
  # Only the started processes: a log-forward reader ends at EOF on its own.
  for pid in "${PIDS[@]}"; do wait "$pid" 2>/dev/null || true; done
  if [ "$status" -eq 0 ] && [ "${KEEP_TMP:-0}" != "1" ]; then
    rm -rf "$TMP"
  else
    echo "run_native: logs and SDK outputs kept in $TMP"
  fi
  exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT TERM

free_port() {
  python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1]); s.close()'
}

# ---------------------------------------------------------------- build
echo "run_native: building ledger server, oracle, client, log-forward, driver"
(cd "$ROOT" && nice go build -tags s3,nats -o "$TMP/ledger-server" ./)
# enable_antithesis_sdk: without it the Go SDK is a no-op and writes nothing.
(cd "$W" && nice go build -tags enable_antithesis_sdk -o "$TMP/oracle" ./bin/oracle)
(cd "$W" && nice go build -tags enable_antithesis_sdk -o "$TMP/client" ./bin/client)
CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$HG/driver/target}"
export CARGO_TARGET_DIR
(cd "$HG/driver" && nice cargo build --release --locked)
(cd "$HG/log-forward" && nice cargo build --release --locked)
DRIVER_BIN="$CARGO_TARGET_DIR/release/driver"
LOGFWD_BIN="$CARGO_TARGET_DIR/release/log-forward"

# ---------------------------------------------------------------- ports
RAFT_PORT="$(free_port)"
GRPC_PORT="$(free_port)"
HTTP_PORT="$(free_port)"
ORACLE_PORT="$(free_port)"
CLIENT_PORT="$(free_port)"
echo "run_native: raft $RAFT_PORT grpc $GRPC_PORT http $HTTP_PORT oracle $ORACLE_PORT client $CLIENT_PORT"

# Runs "$@" with output through log-forward (as the compose forward-logs
# wrapper does) into $log; HOSTNAME names the source for the driver.
forwarded() {
  local name="$1" log="$2"
  shift 2
  # Process substitution, so $! is the process itself (killing a pipeline's
  # subshell would leave the process running).
  "$@" > >(HOSTNAME="$name" LOG_FORWARD_ADDR=127.0.0.1:7777 "$LOGFWD_BIN" >"$log") 2>&1 &
  PIDS+=($!)
}

# ---------------------------------------------------------------- ledger node
mkdir -p "$TMP/node/wal" "$TMP/node/data"
forwarded ledger-0 "$TMP/ledger-0.log" env \
  CLUSTER_ID=default INSECURE=true LOG_LEVEL=debug JSON_FORMATTING_LOGGER=true \
  RAFT_COMPACTION_MARGIN=100 MAINTENANCE_INTERVAL=5s SPOOL_SEGMENT_MAX_BYTES=4096 \
  RAFT_REPLAY_BATCH_SIZE=32 SNAPSHOT_PARALLELISM=1 PEBBLE_MAX_CHECKPOINTS=3 \
  IDEMPOTENCY_TTL=0s QUERY_CHECKPOINT_LIMIT=10 CACHE_ROTATION_THRESHOLD=50 \
  SENTINEL_MODE=true PEBBLE_MEMTABLE_SIZE=32Mi READ_INDEX_MEMTABLE_SIZE=16Mi PEBBLE_CACHE_SIZE=128Mi \
  "$TMP/ledger-server" run --node-id 1 --bootstrap \
  --bind-addr "127.0.0.1:$RAFT_PORT" --advertise-addr "127.0.0.1:$RAFT_PORT" \
  --grpc-port "$GRPC_PORT" --http-port "$HTTP_PORT" \
  --wal-dir "$TMP/node/wal" --data-dir "$TMP/node/data"

# ---------------------------------------------------------------- oracle
ORACLE_ADDR="127.0.0.1:$ORACLE_PORT" QUERY_CHECKPOINT_LIMIT=10 \
  ORACLE_JOURNAL_FILE="$TMP/oracle.jsonl" ANTITHESIS_SDK_LOCAL_OUTPUT="$TMP/sdk-oracle.jsonl" \
  "$TMP/oracle" >"$TMP/oracle.log" 2>&1 &
PIDS+=($!)

# ---------------------------------------------------------------- client
forwarded client1 "$TMP/client1.log" env \
  CLIENT_ADDR="127.0.0.1:$CLIENT_PORT" LEDGER_GRPC_ADDR="localhost:$GRPC_PORT" \
  OUTCOME_ADDR=127.0.0.1:7778 ANTITHESIS_SDK_LOCAL_OUTPUT="$TMP/sdk-client.jsonl" \
  "$TMP/client"

# ---------------------------------------------------------------- driver
DRIVER_ENV=(
  CLIENT_URLS="http://127.0.0.1:$CLIENT_PORT"
  LEDGER_NODES="${LEDGER_NODES:-localhost:$GRPC_PORT}"
  ORACLE_URL="http://127.0.0.1:$ORACLE_PORT"
  QUERY_CHECKPOINT_LIMIT=10
  QUIET_PROBABILITY="${QUIET_PROBABILITY:-0.02}"
  ANTITHESIS_SDK_LOCAL_OUTPUT="$TMP/sdk.jsonl"
  WORKLOAD_ACTIVE_FILE="$TMP/workload-active"
  RUST_BACKTRACE=1
)
[ -n "${EXCLUSIVE_PROBABILITY:-}" ] && DRIVER_ENV+=(EXCLUSIVE_PROBABILITY="$EXCLUSIVE_PROBABILITY")
[ -n "${RULES:-}" ] && DRIVER_ENV+=(RULES="$RULES")
START=$(date +%s)
env "${DRIVER_ENV[@]}" "$DRIVER_BIN" >"$TMP/driver.log" 2>&1 &
DRIVER_PID=$!
PIDS+=("$DRIVER_PID")

# Touch the workload-active file once the driver reports setup complete.
SETUP_DEADLINE=$((START + 240))
until grep -q "setup: complete" "$TMP/driver.log" 2>/dev/null; do
  if ! kill -0 "$DRIVER_PID" 2>/dev/null; then
    echo "run_native: the driver exited during setup"; tail -50 "$TMP/driver.log"; exit 1
  fi
  if [ "$(date +%s)" -ge "$SETUP_DEADLINE" ]; then
    echo "run_native: setup did not complete within 240 s"; tail -50 "$TMP/driver.log"; exit 1
  fi
  sleep 0.5
done
SETUP_S=$(( $(date +%s) - START ))
touch "$TMP/workload-active"
echo "run_native: setup complete after ${SETUP_S}s; running for ${DURATION}s"

END=$(( $(date +%s) + DURATION ))
while [ "$(date +%s)" -lt "$END" ]; do
  if ! kill -0 "$DRIVER_PID" 2>/dev/null; then
    echo "run_native: the driver exited early"; tail -50 "$TMP/driver.log"; exit 1
  fi
  sleep 1
done
kill -TERM "$DRIVER_PID" 2>/dev/null || true
sleep 2
kill -KILL "$DRIVER_PID" 2>/dev/null || true

# ---------------------------------------------------------------- verdict
# Whitelisted never-hit must-hits: the two Antithesis-only pause literals and
# every literal the foundation porting files mark as needing faults.
python3 - "$TMP" "$HG/porting" "$DURATION" "$SETUP_S" <<'PY'
import glob, json, os, re, sys

tmp, porting, duration, setup_s = sys.argv[1], sys.argv[2], int(sys.argv[3]), int(sys.argv[4])

whitelist = {"the fault pause command exists in Antithesis", "the fault pause command succeeds"}
for path in sorted(glob.glob(os.path.join(porting, "00*.md"))):
    for line in open(path, encoding="utf-8"):
        cells = [c.strip() for c in line.strip().strip("|").split("|")]
        if len(cells) < 3 or not line.lstrip().startswith("|"):
            continue
        needs = cells[-1].lower()
        if needs.startswith("faults") or needs.startswith("antithesis only"):
            whitelist.add(cells[0].strip("`"))
# The "H/logs.rs leadership/restart/log-receipt literals" row of
# 00-foundation.md: leadership changes and restarts need faults.
whitelist |= {
    "a ledger node restarts",
    "the driver reads leadership changes from node logs",
    "a node gains leadership while the workload runs",
    "a node loses leadership while the workload runs",
    "a node changes leadership role many times in one boot",
}
# plus any other leadership literal of logs.rs (check_sometimes! may span lines)
src = open(os.path.join(porting, "..", "driver", "src", "logs.rs"), encoding="utf-8").read()
for m in re.finditer(r'check_sometimes!\(\s*[^"]*?"([^"]+)"', src):
    if "leader" in m.group(1):
        whitelist.add(m.group(1))

asserts = {}  # message -> dict
failures = []
for path in sorted(glob.glob(os.path.join(tmp, "sdk*.jsonl"))):
    src_name = os.path.basename(path)
    for raw in open(path, encoding="utf-8", errors="replace"):
        raw = raw.strip()
        if not raw:
            continue
        try:
            v = json.loads(raw)
        except ValueError:
            continue
        a = v.get("antithesis_assert")
        if not a:
            continue
        msg = a.get("message", "")
        disp = a.get("display_type", "")
        st = asserts.setdefault(msg, {"display": disp, "must_hit": bool(a.get("must_hit")), "hits": 0, "true": 0, "src": src_name})
        if not a.get("hit"):
            continue
        st["hits"] += 1
        if a.get("condition"):
            st["true"] += 1
        failing = (disp in ("Always", "AlwaysOrUnreachable") and not a.get("condition")) or disp == "Unreachable"
        if failing:
            failures.append((src_name, disp, msg, json.dumps(a.get("details"))[:600]))

def hit(st):
    if st["display"] == "Sometimes":
        return st["true"] > 0
    return st["hits"] > 0

never = sorted(m for m, st in asserts.items() if st["must_hit"] and not hit(st) and m not in whitelist)
whitelisted = sorted(m for m, st in asserts.items() if st["must_hit"] and not hit(st) and m in whitelist)

# Smoke summary from the driver log and the oracle journal.
drv = open(os.path.join(tmp, "driver.log"), encoding="utf-8", errors="replace").read()
quiet_begins = quiet_ends = 0
jpath = os.path.join(tmp, "oracle.jsonl")
eps = {}
if os.path.exists(jpath):
    for raw in open(jpath, encoding="utf-8", errors="replace"):
        try:
            e = json.loads(raw)
        except ValueError:
            continue
        ep = e.get("ep", "")
        eps[ep] = eps.get(ep, 0) + 1
        if ep == "POST /quiet/end" and e.get("status") == 200:
            quiet_ends += 1
        if ep == "POST /quiet/begin" and e.get("status") == 200:
            quiet_begins += 1
writes = eps.get("POST /write/end", 0)
reads = eps.get("POST /read/end", 0)
verified = asserts.get("oracle: model outcome verified", {})

print()
print("run_native: ---- summary ----")
print(f"  duration {duration}s, setup {setup_s}s")
print(f"  oracle: {writes} /write/end, {reads} /read/end ({(writes + reads) / max(duration, 1):.2f} tracked ops/s), "
      f"{eps.get('POST /write/begin', 0)} /write/begin, {eps.get('POST /view', 0)} /view")
print(f"  quiet periods: {quiet_begins} /quiet/begin, {quiet_ends} /quiet/end (Q6)")
print(f"  'oracle: model outcome verified' hits: {verified.get('hits', 0)}")
print(f"  hegel re-entries: {drv.count('hegel callback entered again')}; slow rules: {drv.count('driver: slow rule')}")
print(f"  assertions seen: {len(asserts)}; findings: {len(failures)}")
print(f"  journal: {jpath}")
for m in whitelisted:
    print(f"  never hit (whitelisted): {m}")
for m in never:
    print(f"  NEVER HIT: {m} [{asserts[m]['display']}, {asserts[m]['src']}]")
for f in failures[:50]:
    print(f"  FAILING [{f[0]}] {f[1]}: {f[2]} {f[3]}")
status = 0
if failures:
    print(f"run_native: FAIL: {len(failures)} failing assertion event(s)")
    status = 1
if never:
    print(f"run_native: {len(never)} must-hit assertion(s) never hit (not whitelisted)")
if status == 0:
    print("run_native: PASS (no failing assertion)")
sys.exit(status)
PY
