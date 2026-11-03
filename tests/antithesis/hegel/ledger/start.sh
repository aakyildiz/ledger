#!/bin/sh
# Starts one ledger node. Node 1 bootstraps the cluster on first boot, the
# others join it; once a node has state (checkpoints on disk) it restarts
# without either flag.
set -eu
# Coverage builds only: the directory go -cover writes to must exist.
[ -n "${GOCOVERDIR:-}" ] && mkdir -p "$GOCOVERDIR"
if [ -d /data/app/checkpoints ] && [ "$(ls -A /data/app/checkpoints 2>/dev/null)" ]; then
  FLAG=""
elif [ "$NODE_ID" = "1" ]; then
  FLAG="--bootstrap"
else
  FLAG="--join ledger-0:7777"
fi
exec /app/ledger-server run --node-id "$NODE_ID" --advertise-addr "$(hostname):7777" $FLAG
