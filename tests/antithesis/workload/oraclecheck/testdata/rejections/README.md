# Rejection fixtures (stage/freeze conformance, DESIGN §10)

Each `*.json` file is one rejection shape a generator can produce on a tracked
ledger: the smallest bulk that triggers it, with its setup.

```json
{
  "name": "insufficient funds",
  "setup": [ /* servicepb.Request protojson, applied one by one, unkeyed */ ],
  "requests": [ /* servicepb.Request protojson: the keyed bulk */ ],
  "expectReason": "INSUFFICIENT_FUNDS"
}
```

`bin/oracle/rejection_stages_test.go` (`TestRejectionStages`, skipped by
`-short`) runs every file against a fresh in-process single-node server and a
fresh `oracle.GlobalState`:

1. apply `setup` to both (each request alone, no key);
2. send `requests` under key `k1`; the server's reason, `expectReason` and the
   model's reason must be equal;
3. send the same body again under `k1` (replay), then a different body (the
   `requests` plus a trailing `addMetadata` on `users:own-x` of the last
   ledger) under `k1`;
4. read the audit trail for `k1`.

The server must be self-consistent: a failure audit entry for `k1` (frozen)
⇔ the different body answers `IDEMPOTENCY_KEY_CONFLICT`; otherwise the
different body executes as the model predicts on a fresh key; the replay
always returns the same failure. Once task 00d wires the task-01 seams, the
model must predict it: `Stage == StageFSM` and reason ∉
`oracle.UncommittedReasons` ⇔ the server froze the key.

Ledger names are free (each fixture gets its own server); use `o-rej-…`.
Task 00a owns `core-*.json`; families add new `NN-*.json` files only.
