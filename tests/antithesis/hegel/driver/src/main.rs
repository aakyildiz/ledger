//! Antithesis driver for the formance ledger v3: a hegel concurrent state
//! machine that sends every action through the client containers (the
//! ledger's Go gRPC client), records the outcomes in a model, and checks the
//! ledger's guarantees against it.
mod clients;
mod logs;
mod model;
mod wire;

use antithesis_sdk::prelude::*;
use clients::{CallOpts, Clients, Outcome};
use hegel::generators as gs;
use hegel::TestCase;
use model::{KeyHistory, Lifecycle, MetaWrite, Model, RevertRec, Status, TxRec};
use serde_json::{json, Value};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use wire::*;

const WORKLOAD_ACTIVE: &str = "/workload-active";
/// The business ledgers every rule writes to.
const LEDGERS: [&str; 2] = ["hegel-a", "hegel-b"];
/// A ledger with a chart of account types, in strict enforcement: `users:{id}`
/// is a normal type and `tmp:{id}` a transient one.
const TYPED_LEDGER: &str = "hegel-types";
const USERS: usize = 6;
/// users:00 and users:01: the only accounts forced writes may overdraw.
const FORCED_ACCOUNTS: usize = 2;
const ASSETS: [&str; 3] = ["USD/2", "EUR/2", "COIN"];
const META_KEYS: [&str; 3] = ["tier", "status", "owner"];
/// Chance per round of starting a quiet period: about one every few minutes
/// at the ~25 rounds/s measured locally. QUIET_PROBABILITY overrides it.
const P_QUIET: f64 = 1.0 / 4000.0;
const QUIET: Duration = Duration::from_secs(90);
const DEADLINE: Duration = Duration::from_secs(60);

fn account(i: usize) -> String {
    if i < USERS {
        format!("users:{i:02}")
    } else {
        "bank:main".to_string()
    }
}

fn all_accounts() -> Vec<String> {
    let mut v: Vec<String> = (0..=USERS).map(account).collect();
    v.push("world".to_string());
    v.push("fees".to_string());
    v
}

fn pick(tc: &TestCase, n: usize) -> usize {
    if n <= 1 {
        return 0;
    }
    tc.draw(gs::integers::<usize>().min_value(0).max_value(n - 1))
}

fn chance(tc: &TestCase, p: f64) -> bool {
    tc.draw(gs::weighted_booleans(p))
}

/// Retryable or ambiguous error codes: the write may or may not have landed.
fn ambiguous(out: &Outcome) -> bool {
    match out {
        Outcome::Unknown(_) => true,
        Outcome::Err { code, reason, .. } => {
            reason.is_empty()
                || reason == "MAINTENANCE_MODE"
                || reason == "STALE_PROPOSAL"
                || reason == "WRITES_BLOCKED_DISK_FULL"
                || reason == "WRITES_BLOCKED_CLOCK_SKEW"
                || matches!(code.as_str(), "Unavailable" | "DeadlineExceeded" | "Canceled" | "Aborted" | "Internal" | "Unknown" | "ResourceExhausted")
        }
        _ => false,
    }
}

struct Driver {
    clients: Arc<Clients>,
    nodes: Vec<String>,
    model: Mutex<Model>,
    node_states: Arc<Mutex<logs::NodeStates>>,
    run_id: String,
    seq: AtomicU64,
    p_quiet: f64,
}

impl Driver {
    fn uniq(&self, prefix: &str) -> String {
        format!("{prefix}-{}-{}", self.run_id, self.seq.fetch_add(1, Ordering::Relaxed))
    }

    fn draw_client(&self, tc: &TestCase) -> usize {
        pick(tc, self.clients.len())
    }

    fn draw_amount(&self, tc: &TestCase) -> u64 {
        if chance(tc, 0.1) {
            tc.draw(gs::integers::<u64>().min_value(1).max_value(1_000_000_000))
        } else {
            tc.draw(gs::integers::<u64>().min_value(1).max_value(500))
        }
    }

    /// Postings among the workload's accounts. Forced postings draw their
    /// sources only from FORCED_ACCOUNTS, so every other account must never go
    /// negative.
    fn draw_postings(&self, tc: &TestCase) -> Vec<Posting> {
        self.draw_postings_for(tc, false)
    }

    fn draw_postings_for(&self, tc: &TestCase, force: bool) -> Vec<Posting> {
        let n = 1 + pick(tc, 3);
        (0..n)
            .map(|_| {
                let from_world = chance(tc, 0.5);
                let source = if from_world {
                    "world".to_string()
                } else if force {
                    account(pick(tc, FORCED_ACCOUNTS))
                } else {
                    account(pick(tc, USERS + 1))
                };
                let mut destination = account(pick(tc, USERS + 1));
                if destination == source {
                    destination = "fees".to_string();
                }
                Posting { source, destination, amount: self.draw_amount(tc), asset: ASSETS[pick(tc, ASSETS.len())].to_string() }
            })
            .collect()
    }

    fn create_tx_request(ledger: &str, postings: &[Posting], reference: &str, force: bool, skip: bool, tx_meta: Option<&(String, String)>, acct_meta: Option<&(String, String, String)>) -> Value {
        let mut ct = json!({
            "postings": postings.iter().map(Posting::to_json).collect::<Vec<_>>(),
            "reference": reference,
        });
        if force {
            ct["force"] = json!(true);
        }
        if let Some((k, v)) = tx_meta {
            ct["metadata"] = json!({ k.as_str(): string_meta(v) });
        }
        if let Some((addr, k, v)) = acct_meta {
            ct["accountMetadata"] = json!({ addr.as_str(): { "values": { k.as_str(): string_meta(v) } } });
        }
        let mut apply = json!({ "ledger": ledger, "action": { "createTransaction": ct } });
        if skip {
            apply["skippableReasons"] = json!(["ERROR_REASON_TRANSACTION_REFERENCE_CONFLICT"]);
        }
        json!({ "apply": apply })
    }

    /// Records a new transaction in the model before it is sent.
    fn register_tx(&self, l: usize, postings: &[Posting], reference: &str, force: bool, key: Option<String>, doomed: bool) -> usize {
        let mut m = self.model.lock().unwrap();
        if force {
            for p in postings {
                m.ledgers[l].may_be_negative.insert(p.source.clone());
            }
        }
        let lm = &mut m.ledgers[l];
        lm.txs.push(TxRec {
            ops: vec![],
            reference: reference.to_string(),
            postings: postings.to_vec(),
            force,
            key,
            sent: Instant::now(),
            acked_at: None,
            status: Status::Pending,
            tx_id: None,
            reason: String::new(),
            doomed_batch: doomed,
        });
        let idx = lm.txs.len() - 1;
        lm.by_ref.insert(reference.to_string(), idx);
        idx
    }

    /// Checks a created-transaction log against what was sent, and records it.
    fn record_created(&self, l: usize, idx: usize, tx: &Value, at: Instant) {
        let id = u64_field(tx, "id");
        let mut m = self.model.lock().unwrap();
        let lm = &mut m.ledgers[l];
        let rec = &lm.txs[idx];
        let got = postings_of(tx);
        assert_always!(got.as_ref() == Some(&rec.postings), "an acknowledged transaction has exactly the postings that were sent", &json!({
            "ledger": lm.name, "reference": rec.reference, "sent": format!("{:?}", rec.postings), "got": tx.get("postings")
        }));
        assert_always!(str_field(tx, "reference") == rec.reference, "an acknowledged transaction carries its reference", &json!({
            "ledger": lm.name, "sent": rec.reference, "got": str_field(tx, "reference")
        }));
        if let Some(prev) = rec.tx_id {
            assert_always!(prev == id, "every answer for one transaction gives the same transaction id", &json!({
                "ledger": lm.name, "reference": rec.reference, "first": prev, "now": id
            }));
        }
        if let Some(&other) = lm.by_id.get(&id) {
            assert_always!(other == idx, "transaction ids are never reused within a ledger", &json!({
                "ledger": lm.name, "id": id, "first": lm.txs[other].reference, "second": lm.txs[idx].reference
            }));
        }
        lm.by_id.insert(id, idx);
        let rec = &mut lm.txs[idx];
        rec.tx_id = Some(id);
        rec.status = Status::Acked;
        rec.acked_at.get_or_insert(at);
    }

    /// Resolves the outcome of one CreateTransaction send.
    fn resolve_tx(&self, l: usize, idx: usize, op: u64, out: &Outcome) {
        self.resolve_tx_in(l, idx, op, out, false)
    }

    /// `in_batch`: the transaction shared its Apply with others, so a sibling's
    /// failure may be the reason it was rejected.
    fn resolve_tx_in(&self, l: usize, idx: usize, op: u64, out: &Outcome, in_batch: bool) {
        let at = Instant::now();
        {
            let mut m = self.model.lock().unwrap();
            m.ledgers[l].txs[idx].ops.push(op);
        }
        match out {
            Outcome::Ok(resp) => {
                let logs = resp.get("logs").and_then(Value::as_array).cloned().unwrap_or_default();
                assert_always!(logs.len() == 1, "a successful single-request Apply returns exactly one log", &json!({"logs": logs.len()}));
                let Some(log) = logs.first() else { return };
                if let Some(tx) = created_transaction(log) {
                    self.record_created(l, idx, tx, at);
                } else {
                    let skipped = order_skipped(log).is_some();
                    let mut m = self.model.lock().unwrap();
                    let rec = &mut m.ledgers[l].txs[idx];
                    assert_unreachable!("a fresh-reference transaction is never skipped", &json!({
                        "reference": rec.reference, "skipped": skipped, "log": log
                    }));
                }
            }
            Outcome::Items(..) => {}
            _ => {
                let mut m = self.model.lock().unwrap();
                let lm = &mut m.ledgers[l];
                let rec = &mut lm.txs[idx];
                if rec.status == Status::Acked {
                    return; // an earlier answer settled it
                }
                if !ambiguous(out) && rec.key.is_some() {
                    rec.status = Status::Rejected;
                    rec.reason = out.reason().to_string();
                    if out.reason() == "INSUFFICIENT_FUNDS" && !in_batch {
                        assert_always!(!rec.force, "a forced transaction is never rejected for insufficient funds", &json!({
                            "reference": rec.reference, "postings": format!("{:?}", rec.postings)
                        }));
                    }
                } else if rec.status == Status::Pending {
                    rec.status = Status::Unknown;
                }
                let r = out.reason();
                assert_sometimes!(r == "INSUFFICIENT_FUNDS", "a transfer is rejected for insufficient funds", &json!({}));
                assert_sometimes!(ambiguous(out), "a write gets an ambiguous outcome", &json!({"outcome": out.summary()}));
            }
        }
    }

    /// Records a metadata write in the model before it is sent.
    fn register_meta(&self, l: usize, addr: &str, key: &str, value: Option<String>, op: u64) {
        let mut m = self.model.lock().unwrap();
        let h = m.ledgers[l].meta.entry((addr.to_string(), key.to_string())).or_default();
        h.writes.push(MetaWrite { op, value, sent: Instant::now(), acked: None, status: Status::Pending });
    }

    fn resolve_meta(&self, l: usize, addr: &str, key: &str, op: u64, ok: bool, definitive_fail: bool) {
        let mut m = self.model.lock().unwrap();
        let h: &mut KeyHistory = m.ledgers[l].meta.entry((addr.to_string(), key.to_string())).or_default();
        if let Some(w) = h.writes.iter_mut().find(|w| w.op == op) {
            if ok {
                w.status = Status::Acked;
                w.acked = Some(Instant::now());
            } else if definitive_fail {
                w.status = Status::Rejected;
            } else {
                w.status = Status::Unknown;
            }
        }
        h.prune();
    }

    fn get_cluster_state(&self, client: usize, node: &str) -> Option<Value> {
        let opts = CallOpts { node: Some(node.to_string()), timeout: Some(Duration::from_secs(2)), ..Default::default() };
        match self.clients.call(client, "cluster", "GetClusterState", json!({}), &opts).1 {
            Outcome::Ok(v) => Some(v),
            _ => None,
        }
    }

    fn barrier(&self, client: usize) -> Option<u64> {
        match self.clients.call(client, "bucket", "Barrier", json!({}), &CallOpts::default()).1 {
            Outcome::Ok(v) => Some(u64_field(&v, "commitIndex")),
            _ => None,
        }
    }

    /// Pages through a stream RPC to its end. None if any page failed.
    fn list_all(&self, client: usize, method: &str, ledger: &str, page: u32, node: Option<&str>) -> Option<Vec<Value>> {
        let mut cursor = String::new();
        let mut all = vec![];
        for _ in 0..10_000 {
            let mut opts = json!({ "pageSize": page });
            if !cursor.is_empty() {
                opts["cursor"] = json!(cursor);
            }
            let call = CallOpts { node: node.map(str::to_string), max_items: page as usize + 1, ..Default::default() };
            let (_, out) = self.clients.call(client, "bucket", method, json!({ "ledger": ledger, "options": opts }), &call);
            let Outcome::Items(items, true, next) = out else { return None };
            assert_always!(items.len() <= page as usize, "a list page never exceeds the requested page size", &json!({
                "method": method, "page": page, "got": items.len()
            }));
            all.extend(items);
            if next.is_empty() {
                return Some(all);
            }
            cursor = next;
        }
        None
    }
}

#[hegel::concurrent_state_machine]
impl Driver {
    /// A single CreateTransaction, with every option drawn.
    #[rule(weight = 6.0)]
    fn create_transaction(&self, tc: TestCase) {
        let l = pick(&tc, LEDGERS.len());
        let client = self.draw_client(&tc);
        let force = chance(&tc, 0.2);
        let postings = self.draw_postings_for(&tc, force);
        let key = if chance(&tc, 0.9) { Some(self.uniq("k")) } else { None };
        let reference = self.uniq("r");
        let tx_meta = if chance(&tc, 0.3) { Some(("note".to_string(), self.uniq("n"))) } else { None };
        let acct_meta = if chance(&tc, 0.15) {
            Some((postings[0].destination.clone(), META_KEYS[pick(&tc, META_KEYS.len())].to_string(), self.uniq("v")))
        } else {
            None
        };
        let skip = chance(&tc, 0.1);
        let req = Self::create_tx_request(LEDGERS[l], &postings, &reference, force, skip, tx_meta.as_ref(), acct_meta.as_ref());
        let idx = self.register_tx(l, &postings, &reference, force, key.clone(), false);
        let op = self.clients.next_op_id();
        if let Some((addr, k, v)) = &acct_meta {
            self.register_meta(l, addr, k, Some(v.clone()), op);
        }
        let (rop, out) = self.clients.apply(client, vec![req], key.as_deref());
        self.resolve_tx(l, idx, rop, &out);
        if let Some((addr, k, _)) = &acct_meta {
            self.resolve_meta(l, addr, k, op, out.is_ok(), !ambiguous(&out) && key.is_some());
        }
        assert_sometimes!(out.is_ok() && force, "a forced transaction commits", &json!({}));
        assert_sometimes!(out.is_ok() && !force && postings.iter().any(|p| p.source != "world"), "an unforced transfer between accounts commits", &json!({}));
    }

    /// The same reference sent again, from several clients at once.
    #[rule(weight = 1.0)]
    fn duplicate_reference(&self, tc: TestCase) {
        let l = pick(&tc, LEDGERS.len());
        let target = {
            let m = self.model.lock().unwrap();
            let acked: Vec<&TxRec> = m.ledgers[l].txs.iter().filter(|t| t.status == Status::Acked).collect();
            if acked.is_empty() {
                None
            } else {
                Some(acked[pick(&tc, acked.len())].clone())
            }
        };
        let Some(target) = target else { return };
        let n = 1 + pick(&tc, 3);
        let plans: Vec<(usize, bool, Vec<Posting>)> = (0..n).map(|_| (self.draw_client(&tc), chance(&tc, 0.5), self.draw_postings(&tc))).collect();
        let outs: Vec<(bool, Outcome)> = std::thread::scope(|s| {
            let hs: Vec<_> = plans
                .iter()
                .map(|(c, skip, ps)| {
                    let req = Self::create_tx_request(LEDGERS[l], ps, &target.reference, false, *skip, None, None);
                    let key = self.uniq("k");
                    let skip = *skip;
                    s.spawn(move || (skip, self.clients.apply(*c, vec![req], Some(&key)).1))
                })
                .collect();
            hs.into_iter().map(|h| h.join().unwrap()).collect()
        });
        for (skip, out) in outs {
            match &out {
                Outcome::Ok(resp) => {
                    let log = resp.pointer("/logs/0").cloned().unwrap_or(Value::Null);
                    assert_always!(created_transaction(&log).is_none(), "a reference never maps to two transactions", &json!({
                        "ledger": LEDGERS[l], "reference": target.reference, "log": log
                    }));
                    if let Some(sk) = order_skipped(&log) {
                        assert_always!(skip, "only a skip-tolerant order is skipped", &json!({"log": log}));
                        let existing = sk.to_string();
                        assert_always!(existing.contains(&target.tx_id.unwrap_or(0).to_string()), "a skipped duplicate reference names the existing transaction", &json!({
                            "expected": target.tx_id, "skipped": sk
                        }));
                        assert_reachable!("a duplicate reference is skipped", &json!({}));
                    }
                }
                Outcome::Err { reason, .. } if !ambiguous(&out) => {
                    assert_always!(!skip || reason != "TRANSACTION_REFERENCE_CONFLICT", "a skip-tolerant duplicate reference is skipped, not rejected", &json!({"reason": reason}));
                    assert_always!(skip || reason == "TRANSACTION_REFERENCE_CONFLICT" || reason == "INSUFFICIENT_FUNDS", "a duplicate reference is rejected as a reference conflict", &json!({"reason": reason}));
                    assert_sometimes!(reason == "TRANSACTION_REFERENCE_CONFLICT", "a duplicate reference is rejected", &json!({}));
                }
                _ => {}
            }
        }
    }

    /// Replays an idempotency key: with the same payload it must answer the
    /// original outcome, with another payload it must be refused.
    #[rule(weight = 2.0)]
    fn idempotent_replay(&self, tc: TestCase) {
        let l = pick(&tc, LEDGERS.len());
        let found = {
            let m = self.model.lock().unwrap();
            let lm = &m.ledgers[l];
            let cands: Vec<usize> = (0..lm.txs.len()).filter(|&i| lm.txs[i].key.is_some() && !lm.txs[i].doomed_batch).collect();
            if cands.is_empty() {
                None
            } else {
                let i = cands[cands.len() - 1 - pick(&tc, cands.len().min(32))];
                Some((i, lm.txs[i].clone()))
            }
        };
        let Some((idx, rec)) = found else { return };
        let client = self.draw_client(&tc);
        let key = rec.key.clone().unwrap();
        // A changed payload is only meaningful once the key is known to be taken.
        let mutate = rec.status == Status::Acked && chance(&tc, 0.3);
        let mut postings = rec.postings.clone();
        if mutate {
            postings[0].amount = postings[0].amount.wrapping_add(1).max(1);
        }
        let req = Self::create_tx_request(LEDGERS[l], &postings, &rec.reference, rec.force, false, None, None);
        // The original carried metadata only sometimes; replay what defines the
        // batch: replays of a metadata-carrying original are payload changes.
        let (op, out) = self.clients.apply(client, vec![req], Some(&key));
        if mutate {
            if let Outcome::Ok(resp) = &out {
                let log = resp.pointer("/logs/0").cloned().unwrap_or(Value::Null);
                let new_tx = created_transaction(&log).map(|t| u64_field(t, "id"));
                assert_always_or_unreachable!(
                    new_tx.is_none() || new_tx == rec.tx_id,
                    "reusing an idempotency key with another payload never creates a second transaction",
                    &json!({"reference": rec.reference, "first": rec.tx_id, "second": new_tx})
                );
            }
            if !ambiguous(&out) && rec.status == Status::Acked {
                assert_sometimes!(out.reason() == "IDEMPOTENCY_KEY_CONFLICT", "a changed payload under a used idempotency key is refused", &json!({}));
            }
            return;
        }
        match &out {
            Outcome::Ok(resp) => {
                let log = resp.pointer("/logs/0").cloned().unwrap_or(Value::Null);
                if let Some(tx) = created_transaction(&log) {
                    let was = rec.status;
                    let meta_differs = tx.get("metadata").is_some();
                    if !meta_differs {
                        self.record_created(l, idx, tx, Instant::now());
                    }
                    assert_always!(was != Status::Rejected, "a rejected write stays rejected when its idempotency key is replayed", &json!({
                        "reference": rec.reference, "reason": rec.reason
                    }));
                    assert_sometimes!(was == Status::Unknown, "a replay resolves an ambiguous write", &json!({}));
                    assert_sometimes!(was == Status::Acked, "a replay returns an acknowledged transaction", &json!({}));
                }
            }
            _ if !ambiguous(&out) => {
                let m = self.model.lock().unwrap();
                let st = m.ledgers[l].txs[idx].status;
                // A replay of an acknowledged write may be refused only for a payload change
                // (a metadata-carrying original).
                assert_always!(
                    st != Status::Acked || out.reason() == "IDEMPOTENCY_KEY_CONFLICT",
                    "replaying an acknowledged write under its idempotency key never fails",
                    &json!({"reference": rec.reference, "outcome": out.summary(), "op": op})
                );
            }
            _ => {}
        }
    }

    /// Reverts an acknowledged transaction, sometimes from several clients at once.
    #[rule(weight = 2.0)]
    fn revert(&self, tc: TestCase) {
        let l = pick(&tc, LEDGERS.len());
        let target = {
            let m = self.model.lock().unwrap();
            let ids = m.ledgers[l].acked_ids();
            if ids.is_empty() {
                None
            } else {
                let mut ids = ids;
                ids.sort();
                Some(ids[pick(&tc, ids.len())])
            }
        };
        let Some(target) = target else { return };
        let n = if chance(&tc, 0.3) { 2 + pick(&tc, 2) } else { 1 };
        let force = chance(&tc, 0.3) && {
            let m = self.model.lock().unwrap();
            let lm = &m.ledgers[l];
            let forced: Vec<String> = (0..FORCED_ACCOUNTS).map(account).chain(["world".to_string(), "fees".to_string()]).collect();
            lm.txs[lm.by_id[&target]].postings.iter().all(|p| forced.contains(&p.destination))
        };
        let plans: Vec<(usize, Option<String>)> = (0..n).map(|_| (self.draw_client(&tc), Some(self.uniq("k")))).collect();
        {
            let mut m = self.model.lock().unwrap();
            if force {
                let idx = m.ledgers[l].by_id[&target];
                let dests: Vec<String> = m.ledgers[l].txs[idx].postings.iter().map(|p| p.destination.clone()).collect();
                m.ledgers[l].may_be_negative.extend(dests);
            }
        }
        let started = Instant::now();
        let outs: Vec<(u64, Option<String>, Outcome)> = std::thread::scope(|s| {
            let hs: Vec<_> = plans
                .iter()
                .map(|(c, key)| {
                    let req = json!({"apply": {"ledger": LEDGERS[l], "action": {"revertTransaction": {"transactionId": target.to_string(), "force": force}}}});
                    s.spawn(move || {
                        let (op, out) = self.clients.apply(*c, vec![req], key.as_deref());
                        (op, key.clone(), out)
                    })
                })
                .collect();
            hs.into_iter().map(|h| h.join().unwrap()).collect()
        });
        let mut m = self.model.lock().unwrap();
        let lm = &mut m.ledgers[l];
        let target_postings = lm.txs[lm.by_id[&target]].postings.clone();
        let mut successes = 0;
        for (op, key, out) in outs {
            let mut rec = RevertRec { op, key, sent: started, status: Status::Unknown, new_id: None, acked_at: None };
            match &out {
                Outcome::Ok(resp) => {
                    let log = resp.pointer("/logs/0").cloned().unwrap_or(Value::Null);
                    if let Some(rt) = reverted_transaction(&log) {
                        successes += 1;
                        let reverted = u64_field(rt, "revertedTransactionId");
                        assert_always!(reverted == target, "a revert reverts the requested transaction", &json!({"target": target, "got": reverted}));
                        let rtx = rt.get("revertTransaction").cloned().unwrap_or(Value::Null);
                        let expected: Vec<Posting> = target_postings.iter().rev().map(|p| Posting {
                            source: p.destination.clone(), destination: p.source.clone(), amount: p.amount, asset: p.asset.clone()
                        }).collect();
                        let got = postings_of(&rtx).unwrap_or_default();
                        let same = |a: &[Posting], b: &[Posting]| {
                            let mut a = a.to_vec();
                            let mut b = b.to_vec();
                            a.sort_by(|x, y| format!("{x:?}").cmp(&format!("{y:?}")));
                            b.sort_by(|x, y| format!("{x:?}").cmp(&format!("{y:?}")));
                            a == b
                        };
                        assert_always!(same(&got, &expected), "a revert moves exactly the original postings back", &json!({
                            "target": target, "expected": format!("{expected:?}"), "got": format!("{got:?}")
                        }));
                        let new_id = u64_field(&rtx, "id");
                        rec.status = Status::Acked;
                        rec.new_id = Some(new_id);
                        rec.acked_at = Some(Instant::now());
                        lm.revert_ids.insert(new_id, target);
                    }
                }
                Outcome::Err { reason, .. } if !ambiguous(&out) => {
                    rec.status = Status::Rejected;
                    assert_sometimes!(reason == "TRANSACTION_ALREADY_REVERTED", "a second revert is refused as already reverted", &json!({}));
                }
                _ => {}
            }
            lm.reverts.entry(target).or_default().push(rec);
        }
        let acked_new: HashSet<u64> = lm.reverts[&target].iter().filter_map(|r| r.new_id).collect();
        assert_always!(acked_new.len() <= 1, "a transaction is reverted at most once", &json!({
            "ledger": lm.name, "target": target, "revert_ids": acked_new.iter().collect::<Vec<_>>()
        }));
        assert_sometimes!(n > 1 && successes == 1, "one of several concurrent reverts wins", &json!({}));
    }

    /// Sets or deletes account and transaction metadata.
    #[rule(weight = 2.0)]
    fn metadata(&self, tc: TestCase) {
        let l = pick(&tc, LEDGERS.len());
        let client = self.draw_client(&tc);
        let addr = account(pick(&tc, USERS + 1));
        let key = META_KEYS[pick(&tc, META_KEYS.len())];
        let delete = chance(&tc, 0.3);
        let idem = if chance(&tc, 0.8) { Some(self.uniq("k")) } else { None };
        let value = if delete { None } else { Some(self.uniq("v")) };
        let action = if delete {
            json!({"deleteMetadata": {"target": {"account": {"addr": addr}}, "key": key}})
        } else {
            json!({"addMetadata": {"target": {"account": {"addr": addr}}, "metadata": {key: string_meta(value.as_ref().unwrap())}}})
        };
        let mut apply = json!({"ledger": LEDGERS[l], "action": action});
        let skip_missing = delete && chance(&tc, 0.5);
        if skip_missing {
            apply["skippableReasons"] = json!(["ERROR_REASON_METADATA_NOT_FOUND"]);
        }
        let op = self.clients.next_op_id();
        self.register_meta(l, &addr, key, value.clone(), op);
        let (_, out) = self.clients.apply(client, vec![json!({"apply": apply})], idem.as_deref());
        // A delete of a missing key that is refused has no effect either way.
        let refused_missing = out.reason() == "METADATA_NOT_FOUND";
        self.resolve_meta(l, &addr, key, op, out.is_ok() || refused_missing, !ambiguous(&out) && idem.is_some());
        assert_sometimes!(refused_missing, "deleting a missing metadata key is refused", &json!({}));
        assert_sometimes!(delete && out.is_ok(), "a metadata key is deleted", &json!({}));
    }

    /// Linearizable GetAccount: metadata must be a value some write could have
    /// left, and volumes must be self-consistent.
    #[rule(weight = 2.0)]
    fn read_account(&self, tc: TestCase) {
        let l = pick(&tc, LEDGERS.len());
        let client = self.draw_client(&tc);
        let addr = account(pick(&tc, USERS + 1));
        let start = Instant::now();
        let (_, out) = self.clients.call(client, "bucket", "GetAccount", json!({"ledger": LEDGERS[l], "address": addr}), &CallOpts::default());
        let end = Instant::now();
        let acct = match out {
            Outcome::Ok(v) => v,
            Outcome::Err { ref reason, .. } if reason == "ACCOUNT_NOT_FOUND" || out.code() == "NotFound" => json!({}),
            _ => return,
        };
        for vol in acct.get("volumes").and_then(Value::as_array).cloned().unwrap_or_default() {
            let v = volumes(&vol);
            assert_always!(v.is_some_and(|(i, o, b)| i - o == b), "an account's balance equals its input minus its output", &json!({"account": addr, "volume": vol}));
        }
        let m = self.model.lock().unwrap();
        for key in META_KEYS {
            let got = acct.pointer(&format!("/metadata/{key}/stringValue")).and_then(Value::as_str).map(str::to_string);
            let Some(h) = m.ledgers[l].meta.get(&(addr.clone(), key.to_string())) else {
                assert_always!(got.is_none(), "account metadata holds only values the workload wrote", &json!({"account": addr, "key": key, "got": got}));
                continue;
            };
            assert_always!(h.allows(&got, start, end), "a linearizable read returns the latest metadata value", &json!({
                "ledger": LEDGERS[l], "account": addr, "key": key, "got": got,
                "writes": h.writes.iter().map(|w| json!({"op": w.op, "value": w.value, "status": format!("{:?}", w.status),
                    "sent_ms_ago": w.sent.elapsed().as_millis() as u64, "acked_ms_ago": w.acked.map(|a| a.elapsed().as_millis() as u64)})).collect::<Vec<_>>()
            }));
            assert_sometimes!(h.has_overlap(start, end), "a metadata read overlaps a write to the same key", &json!({}));
        }
    }

    /// Reads an acknowledged transaction back.
    #[rule(weight = 2.0)]
    fn read_transaction(&self, tc: TestCase) {
        let l = pick(&tc, LEDGERS.len());
        let client = self.draw_client(&tc);
        let stale = chance(&tc, 0.2);
        let target = {
            let m = self.model.lock().unwrap();
            let lm = &m.ledgers[l];
            let mut ids = lm.acked_ids();
            if ids.is_empty() {
                return;
            }
            ids.sort();
            let id = ids[ids.len() - 1 - pick(&tc, ids.len().min(16))];
            let rec = lm.txs[lm.by_id[&id]].clone();
            let reverted_before = lm.reverts.get(&id).map_or(false, |rs| rs.iter().any(|r| r.status == Status::Acked));
            (id, rec, reverted_before)
        };
        let (id, rec, reverted_before) = target;
        let opts = CallOpts { stale, ..Default::default() };
        let (_, out) = self.clients.call(client, "bucket", "GetTransaction", json!({"ledger": LEDGERS[l], "transactionId": id.to_string()}), &opts);
        let not_found = out.code() == "NotFound";
        if out.is_ok() || not_found {
            assert_always!(stale || !not_found, "a linearizable read finds every acknowledged transaction", &json!({"ledger": LEDGERS[l], "id": id}));
        }
        match out {
            Outcome::Ok(v) => {
                let tx = v.get("transaction").cloned().unwrap_or(Value::Null);
                assert_always!(postings_of(&tx).as_ref() == Some(&rec.postings) && str_field(&tx, "reference") == rec.reference,
                    "a transaction reads back as it was acknowledged", &json!({"ledger": LEDGERS[l], "id": id, "got": tx, "sent": format!("{:?}", rec.postings)}));
                if !stale && reverted_before {
                    assert_always!(tx.get("reverted").and_then(Value::as_bool).unwrap_or(false), "a linearizable read of a reverted transaction shows it reverted", &json!({"id": id}));
                }
            }
            Outcome::Err { ref code, .. } if code == "NotFound" => {
                assert_sometimes!(stale, "a stale read misses a recent transaction", &json!({}));
            }
            _ => {}
        }
    }

    /// Lists transactions or logs with a small page size, checking order and paging.
    #[rule(weight = 1.0)]
    fn list_pages(&self, tc: TestCase) {
        let l = pick(&tc, LEDGERS.len());
        let client = self.draw_client(&tc);
        let page = 1 + pick(&tc, 25) as u32;
        let reverse = chance(&tc, 0.3);
        let logs = chance(&tc, 0.4);
        let method = if logs { "ListLogs" } else { "ListTransactions" };
        let field = if logs { "sequence" } else { "id" };
        let mut cursor = String::new();
        let mut prev: Option<u64> = None;
        let mut seen = HashSet::new();
        for _ in 0..(1 + pick(&tc, 4)) {
            let mut opts = json!({"pageSize": page, "reverse": reverse});
            if !cursor.is_empty() {
                opts["cursor"] = json!(cursor);
            }
            let (_, out) = self.clients.call(client, "bucket", method, json!({"ledger": LEDGERS[l], "options": opts}), &CallOpts { max_items: page as usize + 1, ..Default::default() });
            let Outcome::Items(items, true, next) = out else { return };
            assert_always!(items.len() <= page as usize, "a list page never exceeds the requested page size", &json!({"method": method, "page": page, "got": items.len()}));
            for it in &items {
                let k = u64_field(it, field);
                assert_always!(seen.insert(k), "paging never returns an item twice", &json!({"method": method, "key": k}));
                if let Some(p) = prev {
                    // Transactions list newest first, logs oldest first; reverse flips either.
                    let descending = logs == reverse;
                    let ordered = if descending { k < p } else { k > p };
                    assert_always!(ordered, "list results are strictly ordered across pages", &json!({"method": method, "prev": p, "next": k, "reverse": reverse}));
                }
                prev = Some(k);
            }
            assert_sometimes!(!next.is_empty(), "a listing spans several pages", &json!({}));
            if next.is_empty() {
                break;
            }
            cursor = next;
        }
    }

    /// Double entry: every asset's total input equals its total output.
    #[rule(weight = 1.0)]
    fn aggregate(&self, tc: TestCase) {
        let l = pick(&tc, LEDGERS.len());
        let client = self.draw_client(&tc);
        let (_, out) = self.clients.call(client, "bucket", "AggregateVolumes", json!({"ledger": LEDGERS[l], "useMaxPrecision": chance(&tc, 0.5)}), &CallOpts::default());
        let Outcome::Ok(v) = out else { return };
        for vol in v.get("volumes").and_then(Value::as_array).cloned().unwrap_or_default() {
            let i = vol.get("input").and_then(parse_uint256);
            let o = vol.get("output").and_then(parse_uint256);
            assert_always!(i == o, "double entry: an asset's total input equals its total output", &json!({"ledger": LEDGERS[l], "volume": vol}));
        }
    }

    /// Atomic batches: several transactions in one Apply, sometimes built so
    /// the last one must fail and take the others down with it.
    #[rule(weight = 1.0)]
    fn batch(&self, tc: TestCase) {
        let l = pick(&tc, LEDGERS.len());
        let client = self.draw_client(&tc);
        let n = 2 + pick(&tc, 3);
        let doomed = chance(&tc, 0.4);
        let key = self.uniq("k");
        let mut reqs = vec![];
        let mut idxs = vec![];
        for i in 0..n {
            let reference = self.uniq("r");
            let force = !doomed && chance(&tc, 0.2);
            let postings = if doomed && i == n - 1 {
                // A fresh account has nothing: an unforced transfer from it must fail.
                vec![Posting { source: format!("empty:{}", self.seq.fetch_add(1, Ordering::Relaxed)), destination: "fees".into(), amount: 1 + pick(&tc, 100) as u64, asset: "COIN".into() }]
            } else {
                self.draw_postings_for(&tc, force)
            };
            reqs.push(Self::create_tx_request(LEDGERS[l], &postings, &reference, force, false, None, None));
            idxs.push(self.register_tx(l, &postings, &reference, force, Some(key.clone()), doomed));
        }
        let (op, out) = self.clients.apply(client, reqs, Some(&key));
        match &out {
            Outcome::Ok(resp) => {
                let logs = resp.get("logs").and_then(Value::as_array).cloned().unwrap_or_default();
                assert_always!(!doomed, "a batch with a failing transaction never succeeds", &json!({"logs": logs.len()}));
                assert_always!(logs.len() == n, "a successful batch returns one log per request", &json!({"requests": n, "logs": logs.len()}));
                let seqs: Vec<u64> = logs.iter().map(|g| u64_field(g, "sequence")).collect();
                assert_always!(seqs.windows(2).all(|w| w[1] > w[0]), "a batch's logs have increasing sequences", &json!({"sequences": seqs}));
                for (log, &idx) in logs.iter().zip(&idxs) {
                    if let Some(tx) = created_transaction(log) {
                        {
                            self.model.lock().unwrap().ledgers[l].txs[idx].ops.push(op);
                        }
                        self.record_created(l, idx, tx, Instant::now());
                    }
                }
                assert_reachable!("an atomic batch commits", &json!({}));
            }
            _ => {
                for &idx in &idxs {
                    self.resolve_tx_in(l, idx, op, &out, true);
                }
                assert_sometimes!(doomed && out.reason() == "INSUFFICIENT_FUNDS", "a doomed batch is rejected as a whole", &json!({}));
            }
        }
    }

    /// A Numscript transaction; its postings are known in advance.
    #[rule(weight = 1.0)]
    fn numscript(&self, tc: TestCase) {
        let l = pick(&tc, LEDGERS.len());
        let client = self.draw_client(&tc);
        let dest = account(pick(&tc, USERS + 1));
        let amount = self.draw_amount(&tc);
        let reference = self.uniq("r");
        let key = self.uniq("k");
        let script = format!("vars {{ account $dest }}\nsend [COIN {amount}] (\n  source = @world\n  destination = $dest\n)\n");
        let postings = vec![Posting { source: "world".into(), destination: dest.clone(), amount, asset: "COIN".into() }];
        let idx = self.register_tx(l, &postings, &reference, false, Some(key.clone()), false);
        let req = json!({"apply": {"ledger": LEDGERS[l], "action": {"createTransaction": {
            "script": {"plain": script, "vars": {"dest": dest}}, "reference": reference
        }}}});
        let (op, out) = self.clients.apply(client, vec![req], Some(&key));
        self.resolve_tx(l, idx, op, &out);
        assert_sometimes!(out.is_ok(), "a numscript transaction commits", &json!({"outcome": out.summary()}));
    }

    /// The cluster's commit index, as Barrier reports it, never goes back.
    #[rule(weight = 1.0)]
    fn barrier_monotonic(&self, tc: TestCase) {
        let client = self.draw_client(&tc);
        let start = Instant::now();
        let floor = {
            let m = self.model.lock().unwrap();
            m.barrier_high.iter().map(|(_, i)| *i).max().unwrap_or(0)
        };
        let Some(idx) = self.barrier(client) else { return };
        assert_always!(idx > 0, "a barrier's commit index is positive", &json!({}));
        assert_always!(idx >= floor, "the barrier commit index never goes back", &json!({"floor": floor, "got": idx}));
        let mut m = self.model.lock().unwrap();
        m.barrier_high.push((start, idx));
        if m.barrier_high.len() > 16 {
            let max = m.barrier_high.iter().map(|(_, i)| *i).max().unwrap_or(0);
            m.barrier_high = vec![(start, max)];
        }
    }

    /// An ephemeral ledger's lifecycle: create, write, delete, and the
    /// guarantees after deletion.
    #[rule(weight = 0.5)]
    fn ledger_lifecycle(&self, tc: TestCase) {
        let client = self.draw_client(&tc);
        let existing: Option<(String, Lifecycle)> = {
            let m = self.model.lock().unwrap();
            let mut v: Vec<(String, Lifecycle)> = m.ephemeral.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
            v.sort_by(|a, b| a.0.cmp(&b.0));
            if v.is_empty() || chance(&tc, 0.4) { None } else { Some(v[pick(&tc, v.len())].clone()) }
        };
        match existing {
            None => {
                let name = self.uniq("eph");
                self.model.lock().unwrap().ephemeral.insert(name.clone(), Lifecycle::Creating);
                let (_, out) = self.clients.apply(client, vec![json!({"createLedger": {"name": name}})], Some(&self.uniq("k")));
                let st = if out.is_ok() { Lifecycle::Live } else { Lifecycle::MaybeDeleted };
                self.model.lock().unwrap().ephemeral.insert(name, st);
            }
            Some((name, Lifecycle::Live)) => {
                if chance(&tc, 0.5) {
                    let ps = self.draw_postings(&tc);
                    let req = Self::create_tx_request(&name, &ps, &self.uniq("r"), true, false, None, None);
                    let (_, out) = self.clients.apply(client, vec![req], Some(&self.uniq("k")));
                    assert_sometimes!(out.is_ok(), "a write to an ephemeral ledger commits", &json!({}));
                    return;
                }
                let (_, out) = self.clients.apply(client, vec![json!({"deleteLedger": {"name": name}})], Some(&self.uniq("k")));
                let st = if out.is_ok() { Lifecycle::Deleted(Instant::now()) } else { Lifecycle::MaybeDeleted };
                self.model.lock().unwrap().ephemeral.insert(name, st);
                assert_sometimes!(out.is_ok(), "a ledger is deleted", &json!({}));
            }
            Some((name, Lifecycle::Deleted(_))) => {
                match pick(&tc, 3) {
                    0 => {
                        let ps = self.draw_postings(&tc);
                        let req = Self::create_tx_request(&name, &ps, &self.uniq("r"), true, false, None, None);
                        let (_, out) = self.clients.apply(client, vec![req], Some(&self.uniq("k")));
                        if !matches!(out, Outcome::Unknown(_)) {
                            assert_always!(!out.is_ok(), "a deleted ledger accepts no writes", &json!({"ledger": name}));
                        }
                        if !ambiguous(&out) {
                            assert_always!(matches!(out.reason(), "LEDGER_DELETED" | "LEDGER_NOT_FOUND"), "a write to a deleted ledger is refused as deleted", &json!({"ledger": name, "outcome": out.summary()}));
                        }
                    }
                    1 => {
                        let (_, out) = self.clients.apply(client, vec![json!({"createLedger": {"name": name}})], Some(&self.uniq("k")));
                        if !matches!(out, Outcome::Unknown(_)) {
                            assert_always!(!out.is_ok(), "a deleted ledger's name is never reused", &json!({"ledger": name}));
                        }
                    }
                    _ => {
                        let (_, out) = self.clients.call(client, "bucket", "ListLedgers", json!({"options": {"pageSize": 1000}}), &CallOpts::default());
                        if let Outcome::Items(items, _, _) = out {
                            let listed = items.iter().any(|i| str_field(i, "name") == name && i.get("deletedAt").is_none());
                            assert_always!(!listed, "a deleted ledger is not listed as live", &json!({"ledger": name}));
                        }
                    }
                }
            }
            _ => {}
        }
    }

    /// Rare administrative actions on the cluster.
    #[rule(weight = 0.5)]
    fn admin(&self, tc: TestCase) {
        let client = self.draw_client(&tc);
        match pick(&tc, 7) {
            0 => {
                // Prefer a node the logs say is not leading.
                let leader = self.node_states.lock().unwrap().leader_hint();
                let mut target = 1 + pick(&tc, self.nodes.len()) as u64;
                if leader.as_deref() == Some(&format!("ledger-{}", target - 1)) {
                    target = target % self.nodes.len() as u64 + 1;
                }
                let (_, out) = self.clients.call(client, "cluster", "TransferLeadership", json!({"transferee": target}), &CallOpts::default());
                assert_sometimes!(out.is_ok(), "leadership is transferred on request", &json!({"outcome": out.summary()}));
            }
            1 => {
                let (_, out) = self.clients.call(client, "cluster", "CompactPrimary", json!({}), &CallOpts { timeout: Some(Duration::from_secs(20)), ..Default::default() });
                assert_sometimes!(out.is_ok(), "the primary store is compacted", &json!({"outcome": out.summary()}));
            }
            2 => {
                let (_, out) = self.clients.call(client, "cluster", "CompactSecondary", json!({}), &CallOpts { timeout: Some(Duration::from_secs(20)), ..Default::default() });
                assert_sometimes!(out.is_ok(), "the secondary store is compacted", &json!({"outcome": out.summary()}));
            }
            3 => {
                let (_, out) = self.clients.call(client, "cluster", "CreateCheckpoint", json!({}), &CallOpts { timeout: Some(Duration::from_secs(20)), ..Default::default() });
                assert_sometimes!(out.is_ok(), "a checkpoint is created", &json!({"outcome": out.summary()}));
            }
            4 => {
                let req = json!({"storage": {"s3": {"bucket": "backups", "region": "us-east-1", "endpoint": "http://minio:9000"}}});
                let (_, out) = self.clients.call(client, "cluster", "Backup", req, &CallOpts { timeout: Some(Duration::from_secs(30)), ..Default::default() });
                if let Outcome::Ok(v) = &out {
                    assert_always!(u64_field(v, "totalFiles") > 0, "a successful backup contains files", &json!({"response": v}));
                }
                assert_sometimes!(out.is_ok(), "a backup completes", &json!({"outcome": out.summary()}));
            }
            5 => {
                // Maintenance mode blocks writes; switch it straight back off.
                let (_, on) = self.clients.apply(client, vec![json!({"setMaintenanceMode": {"enabled": true}})], Some(&self.uniq("k")));
                if on.is_ok() {
                    let l = pick(&tc, LEDGERS.len());
                    let ps = vec![Posting { source: "world".into(), destination: "fees".into(), amount: 1, asset: "COIN".into() }];
                    let reference = self.uniq("r");
                    let key = self.uniq("k");
                    let idx = self.register_tx(l, &ps, &reference, false, Some(key.clone()), false);
                    let (op, out) = self.clients.apply(client, vec![Self::create_tx_request(LEDGERS[l], &ps, &reference, false, false, None, None)], Some(&key));
                    self.resolve_tx(l, idx, op, &out);
                    assert_sometimes!(out.reason() == "MAINTENANCE_MODE", "a write during maintenance is refused", &json!({}));
                }
                for _ in 0..20 {
                    let (_, off) = self.clients.apply(client, vec![json!({"setMaintenanceMode": {"enabled": false}})], Some(&self.uniq("k")));
                    if off.is_ok() {
                        break;
                    }
                }
            }
            _ => {
                let name = self.uniq("sink");
                let cfg = json!({"config": {"name": name, "nats": {"url": "nats://nats:4222", "topic": name}, "format": "json", "batchSize": 32}});
                let (_, out) = self.clients.apply(client, vec![json!({"addEventsSink": cfg})], Some(&self.uniq("k")));
                if out.is_ok() {
                    let (_, sinks) = self.clients.call(client, "bucket", "GetEventsSinks", json!({}), &CallOpts::default());
                    if let Outcome::Ok(v) = sinks {
                        assert_always!(v.to_string().contains(&name), "an added events sink is listed", &json!({"sink": name}));
                    }
                    let (_, rm) = self.clients.apply(client, vec![json!({"removeEventsSink": {"name": name}})], Some(&self.uniq("k")));
                    assert_sometimes!(rm.is_ok(), "an events sink is removed", &json!({}));
                }
            }
        }
    }

    /// The chart of accounts: transient accounts must net to zero within a
    /// transaction, and strict enforcement refuses addresses outside the chart.
    #[rule(weight = 1.0)]
    fn account_types(&self, tc: TestCase) {
        let client = self.draw_client(&tc);
        let id = pick(&tc, 4);
        let amount = self.draw_amount(&tc);
        let kind = pick(&tc, 3);
        let postings = match kind {
            // Balanced: tmp is credited and drained in the same transaction.
            0 => vec![
                Posting { source: "world".into(), destination: format!("tmp:{id}"), amount, asset: "COIN".into() },
                Posting { source: format!("tmp:{id}"), destination: format!("users:{id}"), amount, asset: "COIN".into() },
            ],
            // Unbalanced: tmp would keep a balance.
            1 => vec![Posting { source: "world".into(), destination: format!("tmp:{id}"), amount, asset: "COIN".into() }],
            // Outside the chart.
            _ => vec![Posting { source: "world".into(), destination: format!("stray:{id}"), amount, asset: "COIN".into() }],
        };
        let req = Self::create_tx_request(TYPED_LEDGER, &postings, &self.uniq("r"), false, false, None, None);
        let (_, out) = self.clients.apply(client, vec![req], Some(&self.uniq("k")));
        let definitive = !ambiguous(&out);
        match kind {
            0 => assert_sometimes!(out.is_ok(), "a balanced transient transaction commits", &json!({"outcome": out.summary()})),
            1 => {
                if definitive || out.is_ok() {
                    assert_always!(!out.is_ok(), "a transient account never keeps a balance", &json!({"postings": format!("{postings:?}")}));
                    assert_always!(out.reason() == "TRANSIENT_ACCOUNT_NON_ZERO", "an unbalanced transient transaction is refused as non-zero", &json!({"outcome": out.summary()}));
                }
            }
            _ => {
                if definitive || out.is_ok() {
                    assert_always!(!out.is_ok(), "strict enforcement refuses addresses outside the chart", &json!({"postings": format!("{postings:?}")}));
                    assert_always!(out.reason() == "ACCOUNT_NOT_MATCHING_TYPE", "an address outside the chart is refused as not matching a type", &json!({"outcome": out.summary()}));
                }
            }
        }
    }

    /// The store's own integrity check finds nothing.
    #[rule(weight = 0.3)]
    fn check_store(&self, tc: TestCase) {
        let client = self.draw_client(&tc);
        let opts = CallOpts { timeout: Some(Duration::from_secs(30)), max_items: 100_000, ..Default::default() };
        let (_, out) = self.clients.call(client, "bucket", "CheckStore", json!({}), &opts);
        if let Outcome::Items(events, complete, _) = &out {
            let errors: Vec<&Value> = events.iter().filter(|e| e.get("error").is_some()).collect();
            assert_always!(errors.is_empty(), "the store integrity check finds no error", &json!({"errors": errors.iter().take(5).collect::<Vec<_>>()}));
            assert_sometimes!(*complete, "a store integrity check completes", &json!({}));
        }
    }

    /// A quiet period: faults stop, the system must recover, and the full
    /// state is checked against the model.
    #[invariant(always_run)]
    fn quiet_period(&self, tc: TestCase) {
        if !chance(&tc, self.p_quiet) {
            return;
        }
        if let Some(bin) = std::env::var_os("ANTITHESIS_STOP_FAULTS") {
            let _ = std::process::Command::new(bin).arg(QUIET.as_secs().to_string()).status();
        }
        assert_reachable!("a quiet period starts", &json!({}));
        let deadline = Instant::now() + DEADLINE;
        // Every client serves a real request again.
        let mut healthy = vec![false; self.clients.len()];
        while Instant::now() < deadline && healthy.contains(&false) {
            for c in 0..self.clients.len() {
                if !healthy[c] {
                    healthy[c] = self.barrier(c).is_some();
                }
            }
            if healthy.contains(&false) {
                std::thread::sleep(Duration::from_millis(500));
            }
        }
        assert_sometimes!(!healthy.contains(&false), "the system recovers after faults stop", &json!({"healthy": healthy}));
        let Some(client) = healthy.iter().position(|h| *h) else { return };
        // Let late completions of earlier operations arrive.
        std::thread::sleep(Duration::from_secs(2));
        self.resolve_late();
        let ok = self.check_state(client);
        assert_sometimes!(ok, "the full state check completes in a quiet period", &json!({}));
        self.check_nodes_converge(client, deadline + Duration::from_secs(20));
    }
}

impl Driver {
    /// Applies completion events that arrived after the driver stopped waiting.
    fn resolve_late(&self) {
        let pending: Vec<(usize, usize, Vec<u64>)> = {
            let m = self.model.lock().unwrap();
            m.ledgers
                .iter()
                .enumerate()
                .flat_map(|(l, lm)| {
                    lm.txs.iter().enumerate().filter(|(_, t)| matches!(t.status, Status::Unknown | Status::Pending)).map(move |(i, t)| (l, i, t.ops.clone()))
                })
                .collect()
        };
        for (l, idx, ops) in pending {
            for op in ops {
                if let Some(out) = self.clients.late_outcome(op) {
                    assert_reachable!("a late completion event resolves an operation", &json!({}));
                    if let Outcome::Ok(resp) = &out {
                        if let Some(tx) = resp.pointer("/logs/0").and_then(created_transaction) {
                            let rec_ref = self.model.lock().unwrap().ledgers[l].txs[idx].reference.clone();
                            if str_field(tx, "reference") == rec_ref {
                                self.record_created(l, idx, tx, Instant::now());
                            }
                        }
                    }
                }
            }
        }
    }

    /// Checks every business ledger against the model. Returns false if the
    /// state could not be read consistently.
    fn check_state(&self, client: usize) -> bool {
        let mut complete = true;
        for (l, name) in LEDGERS.iter().enumerate() {
            let Some(_) = self.barrier(client) else { return false };
            let Some(txs) = self.list_all(client, "ListTransactions", name, 500, None) else { return false };
            let mut balances: BTreeMap<(String, String), i128> = BTreeMap::new();
            let mut ids = HashSet::new();
            let mut refs = HashSet::new();
            let mut m = self.model.lock().unwrap();
            let lm = &mut m.ledgers[l];
            let mut reverts_of: HashMap<u64, Vec<u64>> = HashMap::new();
            for tx in &txs {
                let id = u64_field(tx, "id");
                assert_always!(ids.insert(id), "a listing has no duplicate transaction ids", &json!({"ledger": name, "id": id}));
                let postings = postings_of(tx).unwrap_or_default();
                for p in &postings {
                    *balances.entry((p.source.clone(), p.asset.clone())).or_default() -= p.amount as i128;
                    *balances.entry((p.destination.clone(), p.asset.clone())).or_default() += p.amount as i128;
                }
                let reverts = u64_field(tx, "revertsTransaction");
                if reverts != 0 {
                    reverts_of.entry(reverts).or_default().push(id);
                    let requested = lm.reverts.get(&reverts).map_or(false, |rs| rs.iter().any(|r| r.status != Status::Rejected));
                    assert_always!(requested, "every revert transaction was requested by the workload", &json!({"ledger": name, "id": id, "reverts": reverts}));
                    continue;
                }
                let reference = str_field(tx, "reference").to_string();
                assert_always!(refs.insert(reference.clone()), "a reference maps to at most one committed transaction", &json!({"ledger": name, "reference": reference}));
                let Some(&idx) = lm.by_ref.get(&reference) else {
                    assert_unreachable!("every committed transaction was sent by the workload", &json!({"ledger": name, "tx": tx}));
                    continue;
                };
                let rec = &mut lm.txs[idx];
                assert_always!(rec.status != Status::Rejected, "a definitively rejected write never takes effect", &json!({
                    "ledger": name, "reference": reference, "reason": rec.reason, "doomed_batch": rec.doomed_batch
                }));
                assert_always!(postings == rec.postings, "a committed transaction has the postings that were sent", &json!({
                    "ledger": name, "reference": reference, "sent": format!("{:?}", rec.postings), "got": format!("{postings:?}")
                }));
                if let Some(acked) = rec.tx_id {
                    assert_always!(acked == id, "a committed transaction keeps its acknowledged id", &json!({"ledger": name, "reference": reference, "acked": acked, "listed": id}));
                }
                if rec.status != Status::Acked {
                    assert_sometimes!(true, "an ambiguous write turns out to have committed", &json!({}));
                    rec.status = Status::Acked;
                    rec.tx_id = Some(id);
                    lm.by_id.insert(id, idx);
                }
                let reverted = tx.get("reverted").and_then(Value::as_bool).unwrap_or(false);
                let revert_acked = lm.reverts.get(&id).map_or(false, |rs| rs.iter().any(|r| r.status == Status::Acked));
                if revert_acked {
                    assert_always!(reverted, "an acknowledged revert marks its transaction reverted", &json!({"ledger": name, "id": id}));
                }
            }
            for (target, rs) in &reverts_of {
                assert_always!(rs.len() == 1, "a transaction is reverted at most once", &json!({"ledger": name, "target": target, "reverts": rs}));
            }
            for rec in lm.txs.iter().filter(|t| t.status == Status::Acked) {
                let id = rec.tx_id.unwrap_or(0);
                assert_always!(ids.contains(&id), "an acknowledged transaction survives faults", &json!({"ledger": name, "id": id, "reference": rec.reference}));
            }
            for (&new_id, &target) in &lm.revert_ids {
                assert_always!(ids.contains(&new_id), "an acknowledged revert survives faults", &json!({"ledger": name, "revert": new_id, "target": target}));
            }
            let may_be_negative = lm.may_be_negative.clone();
            drop(m);

            // Account balances equal the committed postings, read linearizably.
            let mut accounts: HashSet<String> = all_accounts().into_iter().collect();
            accounts.extend(balances.keys().map(|(a, _)| a.clone()));
            let mut accounts: Vec<String> = accounts.into_iter().collect();
            accounts.sort();
            let mut read_all = true;
            for addr in accounts.iter().filter(|a| !a.starts_with("empty:")) {
                let (_, out) = self.clients.call(client, "bucket", "GetAccount", json!({"ledger": name, "address": addr}), &CallOpts::default());
                let acct = match out {
                    Outcome::Ok(v) => v,
                    Outcome::Err { ref code, .. } if code == "NotFound" => json!({}),
                    _ => {
                        read_all = false;
                        continue;
                    }
                };
                let mut got: HashMap<String, i128> = HashMap::new();
                for vol in acct.get("volumes").and_then(Value::as_array).cloned().unwrap_or_default() {
                    if let Some((_, _, b)) = volumes(&vol) {
                        *got.entry(str_field(&vol, "asset").to_string()).or_default() += b;
                    }
                }
                for asset in ASSETS {
                    let want = balances.get(&(addr.clone(), asset.to_string())).copied().unwrap_or(0);
                    let have = got.get(asset).copied().unwrap_or(0);
                    assert_always!(want == have, "an account's balance equals the sum of its committed postings", &json!({
                        "ledger": name, "account": addr, "asset": asset, "expected": want.to_string(), "got": have.to_string()
                    }));
                    if addr != "world" && !may_be_negative.contains(addr) {
                        assert_always!(have >= 0, "an account goes negative only through forced writes", &json!({"ledger": name, "account": addr, "asset": asset, "balance": have.to_string()}));
                    }
                }
            }
            complete &= read_all;

            // Ledger stats count what is committed.
            let (_, stats) = self.clients.call(client, "bucket", "GetLedgerStats", json!({"ledger": name}), &CallOpts::default());
            if let Outcome::Ok(s) = stats {
                let n = u64_field(&s, "transactionCount");
                assert_always!(n == txs.len() as u64, "ledger stats count every committed transaction", &json!({"ledger": name, "stats": n, "listed": txs.len()}));
                assert_always!(u64_field(&s, "logCount") >= n, "a ledger has at least one log per transaction", &json!({"ledger": name, "stats": s}));
            }
        }
        complete
    }

    /// After faults stop, every node reaches the leader's commit index and
    /// serves the same account state from its local store.
    fn check_nodes_converge(&self, client: usize, deadline: Instant) {
        let Some(index) = self.barrier(client) else { return };
        let mut reference: Option<String> = None;
        for node in &self.nodes {
            let mut persisted = 0;
            while Instant::now() < deadline {
                if let Some(cs) = self.get_cluster_state(client, node) {
                    persisted = cs.get("raftStatus").map(|r| u64_field(r, "lastPersistedIndex")).unwrap_or(0);
                    if persisted >= index {
                        break;
                    }
                }
                std::thread::sleep(Duration::from_millis(500));
            }
            assert_sometimes!(persisted >= index, "every node catches up after faults stop", &json!({"node": node, "persisted": persisted, "index": index}));
            if persisted < index {
                continue;
            }
            // Nothing is written in a quiet period, so the node's local state
            // at `index` is final.
            let mut snapshot = String::new();
            for name in LEDGERS {
                for addr in all_accounts() {
                    let opts = CallOpts { node: Some(node.clone()), stale: true, ..Default::default() };
                    match self.clients.call(client, "bucket", "GetAccount", json!({"ledger": name, "address": addr}), &opts).1 {
                        Outcome::Ok(v) => snapshot.push_str(&format!("{name}/{addr}:{}\n", v.get("volumes").cloned().unwrap_or(Value::Null))),
                        Outcome::Err { code, .. } if code == "NotFound" => snapshot.push_str(&format!("{name}/{addr}:none\n")),
                        _ => return,
                    }
                }
            }
            match &reference {
                None => reference = Some(snapshot),
                Some(r) => assert_always!(r == &snapshot, "every node holds the same account state at a common index", &json!({"node": node, "index": index})),
            }
        }
    }
}

fn wait_until_up(clients: &Clients, nodes: &[String]) {
    // Print each wait's status only when it changes.
    let mut last: HashMap<String, String> = HashMap::new();
    let say = |last: &mut HashMap<String, String>, who: &str, msg: String| {
        if last.get(who) != Some(&msg) {
            println!("setup: {who}: {msg}");
            last.insert(who.to_string(), msg);
        }
    };
    for c in 0..clients.len() {
        loop {
            match clients.health(c) {
                Ok(_) => break,
                Err(e) => say(&mut last, &clients.urls[c], format!("waiting: {e}")),
            }
            std::thread::sleep(Duration::from_secs(1));
        }
    }
    // Every node answers, and the cluster has a leader and every voter.
    loop {
        let mut ready = true;
        for node in nodes {
            let opts = CallOpts { node: Some(node.clone()), timeout: Some(Duration::from_secs(2)), ..Default::default() };
            match clients.call(0, "cluster", "GetClusterState", json!({}), &opts).1 {
                Outcome::Ok(cs) => {
                    let voters = cs.get("nodes").and_then(Value::as_array).map_or(0, |ns| ns.iter().filter(|n| str_field(n, "suffrage") == "Voter").count());
                    if u64_field(&cs, "leader") == 0 || voters < nodes.len() {
                        say(&mut last, node, format!("leader={} voters={voters}", u64_field(&cs, "leader")));
                        ready = false;
                    }
                }
                other => {
                    say(&mut last, node, format!("{}", other.summary()));
                    ready = false;
                }
            }
        }
        if ready {
            return;
        }
        std::thread::sleep(Duration::from_secs(1));
    }
}

fn admin_setup(clients: &Clients) {
    let typed = json!({"createLedger": {"name": TYPED_LEDGER, "accountTypes": {
        "users": {"name": "users", "pattern": "users:{id}"},
        "tmp": {"name": "tmp", "pattern": "tmp:{id}", "persistence": "ACCOUNT_TYPE_TRANSIENT"},
    }}});
    loop {
        let (_, out) = clients.apply(0, vec![typed.clone()], Some("setup-create-typed"));
        if out.is_ok() || out.reason() == "LEDGER_ALREADY_EXISTS" {
            break;
        }
        println!("setup: creating ledger {TYPED_LEDGER}: {}", out.summary());
        std::thread::sleep(Duration::from_secs(1));
    }
    for name in LEDGERS {
        loop {
            let (_, out) = clients.apply(0, vec![json!({"createLedger": {"name": name}})], Some(&format!("setup-create-{name}")));
            if out.is_ok() || out.reason() == "LEDGER_ALREADY_EXISTS" {
                break;
            }
            println!("setup: creating ledger {name}: {:?}", out.summary());
            std::thread::sleep(Duration::from_secs(1));
        }
    }
}

fn wait_until_serving(clients: &Clients) {
    for c in 0..clients.len() {
        for name in LEDGERS {
            loop {
                let (_, out) = clients.call(c, "bucket", "GetLedger", json!({"ledger": name}), &CallOpts::default());
                if out.is_ok() {
                    break;
                }
                println!("setup: client {c} reading ledger {name}: {:?}", out.summary());
                std::thread::sleep(Duration::from_secs(1));
            }
        }
    }
}

#[hegel::main(phases = [hegel::Phase::Generate])]
fn main(tc: TestCase) {
    antithesis_init();
    let active = Arc::new(AtomicBool::new(false));
    let node_states = Arc::new(Mutex::new(logs::NodeStates::new(active.clone())));
    logs::start(node_states.clone());
    let urls: Vec<String> = std::env::var("CLIENT_URLS").unwrap_or_default().split(',').filter(|s| !s.is_empty()).map(str::to_string).collect();
    let nodes: Vec<String> = std::env::var("LEDGER_NODES").unwrap_or_default().split(',').filter(|s| !s.is_empty()).map(str::to_string).collect();
    let clients = Arc::new(Clients::new(urls, active.clone()));
    clients.start_receiver();

    wait_until_up(&clients, &nodes);
    admin_setup(&clients);
    wait_until_serving(&clients);
    println!("setup: complete");
    antithesis_sdk::lifecycle::setup_complete(&json!({"nodes": nodes, "clients": clients.urls}));

    while !std::path::Path::new(WORKLOAD_ACTIVE).exists() {
        std::thread::sleep(Duration::from_millis(100));
    }
    active.store(true, Ordering::Relaxed);
    assert_reachable!("the workload starts", &json!({}));

    let run_id = format!("{:x}", SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_micros()).unwrap_or(0));
    let driver = Driver {
        clients,
        nodes,
        model: Mutex::new(Model::new(&LEDGERS)),
        node_states,
        run_id,
        seq: AtomicU64::new(0),
        p_quiet: std::env::var("QUIET_PROBABILITY").ok().and_then(|p| p.parse().ok()).unwrap_or(P_QUIET),
    };
    hegel::stateful::machine(driver).steps(i64::MAX).max_concurrency(4).run_concurrent(tc);
}
