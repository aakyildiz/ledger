//! Task 03 — metadata: the `meta_account`, `meta_tx` and `meta_ledger` order
//! generators, the family's local `meta_bulk` rule (composes this family's
//! four gens, revert included), and `meta_read_after_write` (rule-owned keys
//! on `o-<run>-l2`, read back linearizably through a client).
use crate::clients::Outcome;
use crate::draw;
use crate::machine::{rule, Driver};
use crate::read::{LedgerSel, ViewReq};
use crate::rules::reverts::{chain_read_opts, l2_taper, step_budget};
use crate::wire::{self, Posting};
use crate::write::{compose_bulk, BulkCtx, OrderGen, WritePlan, WriteResult};
use hegel::stateful::ConcurrentRule;
use hegel::TestCase;
use serde_json::{json, Map, Value};
use std::time::{Duration, Instant};

pub const FAMILY: &str = "metadata";
/// Chain budget of `meta_read_after_write`.
const CHAIN_BUDGET: Duration = Duration::from_secs(12);
const NOT_FOUND_OPT_IN: &str = "ERROR_REASON_METADATA_NOT_FOUND";

pub fn rules() -> Vec<ConcurrentRule<Driver>> {
    vec![rule("meta_bulk", 2.0, meta_bulk), rule("meta_read_after_write", 0.8, meta_read_after_write)]
}

pub fn order_gens() -> Vec<OrderGen> {
    vec![
        OrderGen { name: "meta_account", weight: 3.0, applies: always, has_opt_in: false, gen: gen_meta_account },
        OrderGen { name: "meta_tx", weight: 1.5, applies: has_txs, has_opt_in: false, gen: gen_meta_tx },
        OrderGen { name: "meta_ledger", weight: 1.0, applies: always, has_opt_in: false, gen: gen_meta_ledger },
    ]
}

/// This family's four order generators (revert + the three metadata gens).
pub fn family_gens() -> Vec<OrderGen> {
    let mut v = crate::rules::reverts::order_gens();
    v.extend(order_gens());
    v
}

fn always(_: &BulkCtx) -> bool {
    true
}

fn has_txs(ctx: &BulkCtx) -> bool {
    wire::u64_field(ctx.ledger_view, "txCount") > 0
}

/// A 1–2 key metadata map over `k0..k3` with typed values (`MetadataValue`
/// protojson).
pub fn meta_map(tc: &TestCase) -> Value {
    let n = draw::int_in(tc, 1, 2);
    let mut m = Map::new();
    for _ in 0..n {
        let k = draw::meta_key_model(tc);
        m.insert(k, draw::meta_value(tc));
    }
    Value::Object(m)
}

fn account_target(addr: &str) -> Value {
    json!({"account": {"addr": addr}})
}

fn tx_target(id: u64) -> Value {
    json!({"transactionId": id.to_string()})
}

pub fn add_metadata_request(ledger: &str, target: Value, metadata: Value) -> Value {
    json!({"apply": {"ledger": ledger, "action": {"addMetadata": {"target": target, "metadata": metadata}}}})
}

pub fn delete_metadata_request(ledger: &str, target: Value, key: &str, opt_in: bool) -> Value {
    let mut apply = json!({"ledger": ledger, "action": {"deleteMetadata": {"target": target, "key": key}}});
    if opt_in {
        apply["skippableReasons"] = json!([NOT_FOUND_OPT_IN]);
    }
    json!({"apply": apply})
}

/// An absent key `absent-k<N>` (never written by any generator).
fn absent_key(tc: &TestCase) -> String {
    format!("absent-{}", draw::meta_key_model(tc))
}

/// `meta_account` (weight 3): `addMetadata` on a pool address (1–2 keys), or
/// (1/3) `deleteMetadata` of an existing (address, key) from
/// `seek(accountMeta)`; half the deletes target an absent key and half of
/// those opt into `METADATA_NOT_FOUND`. A delete with nothing to delete
/// falls back to the add.
fn gen_meta_account(d: &Driver, tc: &TestCase, ctx: &BulkCtx, _opt_in: bool) -> Option<Value> {
    let addr = draw::pool_address(tc);
    let delete = draw::chance(tc, 1.0 / 3.0);
    let absent = draw::chance(tc, 0.5);
    let opt_in = draw::chance(tc, 0.5);
    let absent_k = absent_key(tc);
    let metadata = meta_map(tc);
    if delete {
        let got = d.view(&[ViewReq::seek(LedgerSel::Name(ctx.ledger.to_string()), "accountMeta", &addr, None)])?;
        if got[0]["found"] == json!(true) {
            let e = &got[0]["entry"];
            let address = wire::str_field(e, "address");
            return Some(if absent {
                delete_metadata_request(ctx.ledger, account_target(address), &absent_k, opt_in)
            } else {
                delete_metadata_request(ctx.ledger, account_target(address), wire::str_field(e, "key"), false)
            });
        }
    }
    Some(add_metadata_request(ctx.ledger, account_target(&addr), metadata))
}

/// `meta_tx` (weight 1.5): the same on `target: {transactionId}` —
/// `addMetadata` on a `seek(txs)` transaction, or (1/3) `deleteMetadata` of a
/// `seek(txMeta)` (id, key); 1/4 of the deletes use an absent key
/// `absent-k<N>`, half of those opting into `METADATA_NOT_FOUND`.
fn gen_meta_tx(d: &Driver, tc: &TestCase, ctx: &BulkCtx, _opt_in: bool) -> Option<Value> {
    let n = wire::u64_field(ctx.ledger_view, "txCount").max(1);
    let probe = draw::int_in(tc, 1, n).to_string();
    let delete = draw::chance(tc, 1.0 / 3.0);
    let absent = draw::chance(tc, 0.25);
    let opt_in = draw::chance(tc, 0.5);
    let absent_k = absent_key(tc);
    let metadata = meta_map(tc);
    let sel = LedgerSel::Name(ctx.ledger.to_string());
    let got = d.view(&[ViewReq::seek(sel.clone(), "txMeta", &probe, None), ViewReq::seek(sel, "txs", &probe, None)])?;
    if delete && got[0]["found"] == json!(true) {
        let e = &got[0]["entry"];
        let id = wire::u64_field(e, "id");
        if id > 0 {
            return Some(if absent {
                delete_metadata_request(ctx.ledger, tx_target(id), &absent_k, opt_in)
            } else {
                delete_metadata_request(ctx.ledger, tx_target(id), wire::str_field(e, "key"), false)
            });
        }
    }
    if got[1]["found"] != json!(true) {
        return None;
    }
    let id = wire::u64_field(&got[1]["entry"], "id");
    if id == 0 {
        return None;
    }
    Some(add_metadata_request(ctx.ledger, tx_target(id), metadata))
}

/// `meta_ledger` (weight 1): `saveLedgerMetadata` (1–2 keys) or (1/3)
/// `deleteLedgerMetadata` of a key of the ledger view (1/4 a random model
/// key, possibly absent).
fn gen_meta_ledger(_d: &Driver, tc: &TestCase, ctx: &BulkCtx, _opt_in: bool) -> Option<Value> {
    let delete = draw::chance(tc, 1.0 / 3.0);
    let random = draw::chance(tc, 0.25);
    let random_key = draw::meta_key_model(tc);
    let keys: Vec<String> = ctx.ledger_view.get("ledgerMetaKeys").and_then(Value::as_array).map(|a| a.iter().filter_map(|k| k.as_str().map(str::to_string)).collect()).unwrap_or_default();
    let idx = draw::pick(tc, keys.len());
    let metadata = meta_map(tc);
    if delete && (random || !keys.is_empty()) {
        let key = if random || keys.is_empty() { random_key } else { keys[idx].clone() };
        return Some(json!({"deleteLedgerMetadata": {"ledger": ctx.ledger, "key": key}}));
    }
    Some(json!({"saveLedgerMetadata": {"ledger": ctx.ledger, "metadata": metadata}}))
}

// ---------------------------------------------------------------- meta_bulk

/// `meta_bulk` (weight 2): `compose_bulk` over this family's four gens (1–3
/// orders) on one `ledgerPick`-drawn ledger.
pub fn meta_bulk(d: &Driver, tc: TestCase) {
    d.guarded("meta_bulk", &tc, |d, tc| {
        if !d.s.oracle_ok() {
            return None;
        }
        let pick = draw::ledger_pick(tc);
        let len = draw::int_in(tc, 1, 3) as usize;
        let lv = d.view(&[ViewReq::ledger(LedgerSel::Pick(pick))])?.pop()?;
        let ledger = lv.get("ledger").and_then(Value::as_str)?.to_string();
        let reqs = compose_family_bulk(d, tc, &ledger, &lv, len);
        if reqs.is_empty() {
            return None;
        }
        let mut plan = WritePlan::drawn(FAMILY, reqs, d, tc);
        plan.hint = json!({"gen": "meta_bulk"});
        let unit = d.s.slots.try_acquire_up_to(1)?.take_one()?;
        let _ = d.dispatch(plan, unit);
        Some(())
    });
}

/// A weighted index drawn with `draw::pick` over integer units (weights in
/// quarters), or `None` when no weight is positive. `draw::weighted` (00c)
/// draws a 32-bit integer that hegel biases strongly towards 0, so it picks
/// the first positive weight almost always; this keeps the gens' weights.
pub fn fair_weighted(tc: &TestCase, weights: &[f64]) -> Option<usize> {
    let units: Vec<usize> = weights.iter().map(|w| if w.is_finite() && *w > 0.0 { ((w * 4.0).round() as usize).max(1) } else { 0 }).collect();
    let total: usize = units.iter().sum();
    if total == 0 {
        return None;
    }
    let mut r = draw::pick(tc, total);
    for (i, u) in units.iter().enumerate() {
        if r < *u {
            return Some(i);
        }
        r -= u;
    }
    None
}

/// The family's bulk: per slot, one of the four gens by weight among those
/// whose `applies` holds (`fair_weighted`), then `compose_bulk` over that
/// single gen (its opt-in draw and call).
pub fn compose_family_bulk(d: &Driver, tc: &TestCase, ledger: &str, lv: &Value, len: usize) -> Vec<Value> {
    let gens = family_gens();
    let mut out = Vec::with_capacity(len);
    for i in 0..len {
        let ctx = BulkCtx { run: &d.s.run, ledger, ledger_view: lv, index_in_bulk: i, len };
        let weights: Vec<f64> = gens.iter().map(|g| if (g.applies)(&ctx) { g.weight } else { 0.0 }).collect();
        let Some(gi) = fair_weighted(tc, &weights) else { break };
        let g = &gens[gi];
        let one = [OrderGen { name: g.name, weight: g.weight, applies: g.applies, has_opt_in: g.has_opt_in, gen: g.gen }];
        out.extend(compose_bulk(d, tc, &one, ledger, lv, 1));
    }
    out
}

// ---------------------------------------------------------------- meta_read_after_write

fn chain_step(d: &Driver, mut plan: WritePlan, req: Value, step: &str, deadline: Instant) -> Option<Outcome> {
    plan.requests = vec![req];
    plan.hint = json!({"gen": "meta_read_after_write", "step": step});
    let budget = step_budget(deadline)?;
    let unit = d.s.slots.try_acquire_up_to(1)?.take_one()?;
    let t = d.begin_write(plan, unit).ok()?;
    match t.run(budget) {
        WriteResult::Definitive { outcome, .. } => Some(outcome),
        _ => None,
    }
}

/// The served string value of `key` in a metadata map (None if absent).
fn served_value(meta: Option<&Value>, key: &str) -> Option<String> {
    let v = meta?.get(key)?;
    Some(v.get("stringValue").and_then(Value::as_str).map(str::to_string).unwrap_or_else(|| v.to_string()))
}

/// A linearizable GetAccount's metadata map, within the chain budget.
/// `Some(None)` when the account does not exist; `None` when the read gave
/// no usable answer or the budget is spent.
fn read_account_meta(d: &Driver, route: &crate::clients::Route, ledger: &str, addr: &str, deadline: Instant) -> Option<Option<Value>> {
    let opts = chain_read_opts(deadline)?;
    let (_, out) = d.call(route, "bucket", "GetAccount", json!({"ledger": ledger, "address": addr}), &opts);
    if out.is_ok() {
        return Some(Some(out.response.as_ref()?.get("metadata").cloned().unwrap_or(json!({}))));
    }
    if !out.is_unknown() && out.code == "NotFound" {
        return Some(None);
    }
    None
}

fn read_tx_meta(d: &Driver, route: &crate::clients::Route, ledger: &str, id: u64, deadline: Instant) -> Option<Value> {
    let opts = chain_read_opts(deadline)?;
    let (_, out) = d.call(route, "bucket", "GetTransaction", json!({"ledger": ledger, "transactionId": id.to_string()}), &opts);
    if !out.is_ok() {
        return None;
    }
    Some(out.response.as_ref()?.pointer("/transaction/metadata").cloned().unwrap_or(json!({})))
}

/// `meta_read_after_write` (weight 0.8): a reserved key `own-k<N>` on the
/// reserved account `t-0:own-meta<N>` of `o-<run>-l2` (1/2) or on a
/// rule-created transaction with reference `o-<run>-own-ref<M>` (1/2):
/// tracked add, linearizable read back through a client (not posted to the
/// oracle), tracked delete, read back; 12 s chain budget covering the writes
/// and the reads (a step without enough budget left ends the rule without
/// asserting). The transaction branch follows the §3.2 taper on l2.
pub fn meta_read_after_write(d: &Driver, tc: TestCase) {
    d.guarded("meta_read_after_write", &tc, |d, tc| {
        if !d.s.oracle_ok() {
            return None;
        }
        let on_tx = draw::chance(tc, 0.5);
        let amount = draw::amount_positive(tc);
        let asset = draw::asset(tc);
        let value = format!("v{}", draw::int_in(tc, 0, 1 << 32));
        let routes = [draw::route(tc, &d.s), draw::route(tc, &d.s)];
        let mut plans: Vec<WritePlan> = (0..3).map(|_| WritePlan::drawn(FAMILY, vec![], d, tc)).collect::<Vec<_>>();
        let ledger = format!("o-{}-l2", d.s.run);
        let addr = draw::owned_address(&d.s, "t-0", "meta");
        let key = format!("own-k{}", d.s.next_own());
        let deadline = Instant::now() + CHAIN_BUDGET;
        let meta = json!({ key.clone(): {"stringValue": value} });

        if !on_tx {
            let target = account_target(&addr);
            let added = chain_step(d, plans.remove(0), add_metadata_request(&ledger, target.clone(), meta), "add", deadline)?;
            if !added.is_ok() {
                return None;
            }
            let got = read_account_meta(d, &routes[0], &ledger, &addr, deadline)?;
            let actual = served_value(got.as_ref(), &key);
            check_always!(
                actual.as_deref() == Some(value.as_str()),
                "metadata read-after-write should return saved value",
                json!({"ledger": ledger, "account": addr, "key": key, "expected": value, "actual": actual})
            );
            let deleted = chain_step(d, plans.remove(0), delete_metadata_request(&ledger, target, &key, false), "delete", deadline)?;
            if !deleted.is_ok() {
                return None;
            }
            let got = read_account_meta(d, &routes[1], &ledger, &addr, deadline)?;
            let actual = served_value(got.as_ref(), &key);
            check_always!(actual.is_none(), "deleted metadata key is absent on a linearizable read", json!({"ledger": ledger, "account": addr, "key": key, "actual": actual}));
            return Some(());
        }

        // l2 is never deleted: the creating branch follows the §3.2 taper.
        l2_taper(d, tc, &ledger)?;
        let reference = d.s.next_own_ref("ref");
        let mut create = wire::create_tx_request(&ledger, &[Posting::new("world", &addr, amount, asset)], Some(&reference));
        create["apply"]["action"]["createTransaction"]["force"] = json!(true);
        let created = chain_step(d, plans.remove(0), create, "create", deadline)?;
        if !created.is_ok() {
            return None;
        }
        let logs = wire::apply_logs(created.response.as_ref()?);
        let id = wire::u64_field(wire::created_transaction(logs.first()?)?, "id");
        if id == 0 {
            return None;
        }
        let target = tx_target(id);
        let added = chain_step(d, plans.remove(0), add_metadata_request(&ledger, target.clone(), meta), "add", deadline)?;
        if !added.is_ok() {
            return None;
        }
        let got = read_tx_meta(d, &routes[0], &ledger, id, deadline)?;
        let actual = served_value(Some(&got), &key);
        check_always_or_unreachable!(
            actual.as_deref() == Some(value.as_str()),
            "tx metadata read-after-write should return saved value",
            json!({"ledger": ledger, "txId": id, "key": key, "expected": value, "actual": actual})
        );
        let deleted = chain_step(d, plans.remove(0), delete_metadata_request(&ledger, target, &key, false), "delete", deadline)?;
        if !deleted.is_ok() {
            return None;
        }
        let got = read_tx_meta(d, &routes[1], &ledger, id, deadline)?;
        let actual = served_value(Some(&got), &key);
        check_always_or_unreachable!(actual.is_none(), "deleted tx metadata key should be absent", json!({"ledger": ledger, "txId": id, "key": key, "actual": actual}));
        Some(())
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testkit::{self, FakeReply};

    /// Whether a generated request names a reserved entity (DESIGN §3.1).
    pub fn names_reserved(v: &Value, run: &str) -> bool {
        match v {
            Value::String(s) => {
                s.split(':').skip(1).any(|seg| seg.starts_with("own-")) || s.starts_with("own-") || s.starts_with(&format!("o-{run}-own"))
            }
            Value::Array(a) => a.iter().any(|x| names_reserved(x, run)),
            Value::Object(o) => o.iter().any(|(k, x)| k.starts_with("own-") || names_reserved(x, run)),
            _ => false,
        }
    }

    #[test]
    fn no_gen_output_names_a_reserved_entity() {
        let (d, _ft, fo) = testkit::driver();
        let gens = family_gens();
        assert_eq!(gens.iter().map(|g| g.name).collect::<Vec<_>>(), vec!["revert", "meta_account", "meta_tx", "meta_ledger"]);
        let lv = json!({"ledger": "o-test-l1", "name": "o-test-l1", "txCount": 50, "ledgerMetaKeys": ["k0", "region"]});
        // Views answer non-reserved entries (the core hides reserved ones);
        // alternate found / not found.
        for found in [true, false] {
            fo.set_view("seek", json!({"found": found, "wrapped": false, "entry": {"id": "3", "key": "k1", "address": "t-3:4", "reverted": false}}));
            fo.set_view("revertSeek", json!({"found": found, "wrapped": false, "entry": {"id": "5", "revertsTransaction": "3", "reverted": false}}));
            let n = std::sync::atomic::AtomicUsize::new(0);
            testkit::with_tc_n(150, |tc| {
                let ctx = BulkCtx { run: "test", ledger: "o-test-l1", ledger_view: &lv, index_in_bulk: 0, len: 1 };
                for g in &gens {
                    for opt in [false, true] {
                        if let Some(v) = (g.gen)(&d, tc, &ctx, opt) {
                            n.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                            assert!(!names_reserved(&v, "test"), "{} produced {v}", g.name);
                            let s = v.to_string();
                            assert!(!s.contains("\\u0000"), "{s}");
                        }
                    }
                }
            });
            assert!(n.load(std::sync::atomic::Ordering::SeqCst) > 0);
        }
    }

    #[test]
    fn gens_cover_their_shapes() {
        let (d, _ft, fo) = testkit::driver();
        fo.set_view("seek", json!({"found": true, "wrapped": false, "entry": {"id": "3", "key": "k1", "address": "t-3:4", "reverted": true}}));
        fo.set_view("revertSeek", json!({"found": true, "wrapped": false, "entry": {"id": "5", "revertsTransaction": "3", "reverted": false}}));
        let lv = json!({"ledger": "L", "txCount": 9, "ledgerMetaKeys": ["k2"]});
        let seen = std::sync::Mutex::new(std::collections::BTreeSet::new());
        testkit::with_tc_n(300, |tc| {
            let ctx = BulkCtx { run: "test", ledger: "L", ledger_view: &lv, index_in_bulk: 0, len: 1 };
            for g in family_gens() {
                let opt = draw::chance(tc, 0.25);
                let Some(v) = (g.gen)(&d, tc, &ctx, opt) else { continue };
                let mut s = seen.lock().unwrap();
                let a = &v["apply"];
                if let Some(rt) = a.pointer("/action/revertTransaction") {
                    s.insert(format!("revert:{}", rt["transactionId"]));
                    if a.get("skippableReasons").is_some() {
                        s.insert("revert:opt-in".into());
                    }
                }
                if let Some(dm) = a.pointer("/action/deleteMetadata") {
                    let key = dm["key"].as_str().unwrap();
                    let tgt = if dm["target"].get("account").is_some() { "account" } else { "tx" };
                    s.insert(format!("delete:{tgt}:{}", if key.starts_with("absent-") { "absent" } else { "present" }));
                    if a.get("skippableReasons").is_some() {
                        assert!(key.starts_with("absent-"));
                        s.insert(format!("delete:{tgt}:opt-in"));
                    }
                }
                if a.pointer("/action/addMetadata").is_some() {
                    s.insert("add".into());
                }
                if v.get("deleteLedgerMetadata").is_some() {
                    s.insert("ledger:delete".into());
                }
                if v.get("saveLedgerMetadata").is_some() {
                    s.insert("ledger:save".into());
                }
            }
        });
        let s = seen.into_inner().unwrap();
        for want in ["revert:\"3\"", "revert:\"5\"", "revert:opt-in", "delete:account:absent", "delete:account:present", "delete:account:opt-in", "delete:tx:absent", "delete:tx:present", "add", "ledger:delete", "ledger:save"] {
            assert!(s.contains(want), "missing {want}: {s:?}");
        }
    }

    #[test]
    fn meta_bulk_dispatches_family_orders() {
        let (d, _ft, fo) = testkit::driver();
        fo.set_view("ledger", json!({"txCount": 5, "ledgerMetaKeys": []}));
        fo.set_view("seek", json!({"found": true, "wrapped": false, "entry": {"id": "2", "key": "k0", "address": "t-1:1", "reverted": false}}));
        for _ in 0..5 {
            testkit::run_rule(&d, meta_bulk);
        }
        let begins = fo.calls("/write/begin");
        assert!(!begins.is_empty());
        for b in &begins {
            let n = b["requests"].as_array().unwrap().len();
            assert!((1..=3).contains(&n));
            assert_eq!(b["family"], "metadata");
        }
    }

    #[test]
    fn meta_read_after_write_reads_back_through_a_client_only() {
        let (d, ft, fo) = testkit::driver();
        let created = Outcome::from_json(&json!({"ok": true, "response": {"logs": [{"sequence": "1", "payload": {"apply": {"log": {"data": {"createdTransaction": {"transaction": {"id": "7"}}}}}}}]}}));
        ft.set_default("bucket", "Apply", FakeReply::Outcome(created));
        // The reads serve no metadata at all: the saved value is missing.
        ft.set_default("bucket", "GetAccount", FakeReply::Outcome(Outcome::from_json(&json!({"ok": true, "response": {"address": "x", "metadata": {}}}))));
        ft.set_default("bucket", "GetTransaction", FakeReply::Outcome(Outcome::from_json(&json!({"ok": true, "response": {"transaction": {"id": "7"}}}))));
        for _ in 0..16 {
            testkit::run_rule(&d, meta_read_after_write);
        }
        let a = testkit::assertions();
        assert!(a.iter().any(|r| r.message == "metadata read-after-write should return saved value" && !r.cond), "{a:?}");
        assert!(a.iter().any(|r| r.message == "tx metadata read-after-write should return saved value" && !r.cond), "{a:?}");
        assert!(fo.calls("/read/begin").is_empty(), "never posted to the oracle");
        for b in fo.calls("/write/begin") {
            let r = &b["requests"][0];
            assert!(r["apply"]["ledger"] == "o-test-l2", "{r}");
            let s = r.to_string();
            assert!(s.contains("own-"), "rule-owned entities only: {s}");
        }
        assert!(d.s.registry.snapshot().is_empty());
    }

    #[test]
    fn fair_weighted_keeps_the_weights() {
        let counts = std::sync::Mutex::new([0usize; 4]);
        testkit::with_tc_n(2000, |tc| {
            counts.lock().unwrap()[fair_weighted(tc, &[3.0, 3.0, 1.5, 1.0]).unwrap()] += 1;
            assert_eq!(fair_weighted(tc, &[0.0, 2.0]), Some(1));
            assert_eq!(fair_weighted(tc, &[0.0, 0.0]), None);
        });
        let c = *counts.lock().unwrap();
        // Expected 706 / 706 / 353 / 235 of 2000.
        assert!(c.iter().all(|n| *n > 120), "{c:?}");
        assert!(c[0] > c[3] && c[1] > c[3], "{c:?}");
    }

    #[test]
    fn meta_read_after_write_tapers_the_tx_branch_and_skips_spent_reads() {
        let (d, ft, fo) = testkit::driver();
        fo.set_view("ledger", json!({"txCount": 4000}));
        ft.set_default("bucket", "GetAccount", FakeReply::Outcome(Outcome::from_json(&json!({"ok": true, "response": {"metadata": {}}}))));
        for _ in 0..16 {
            testkit::run_rule(&d, meta_read_after_write);
        }
        let begins = fo.calls("/write/begin");
        assert!(!begins.is_empty(), "the account branch still runs");
        for b in &begins {
            let a = &b["requests"][0]["apply"]["action"];
            assert!(a.get("createTransaction").is_none(), "no transaction on a full l2: {a}");
            let tgt = a.get("addMetadata").or_else(|| a.get("deleteMetadata")).unwrap();
            assert!(tgt["target"].get("account").is_some(), "{a}");
        }
        // A read with no budget left is not sent.
        let before = (ft.calls_of("bucket", "GetAccount").len(), ft.calls_of("bucket", "GetTransaction").len());
        testkit::with_tc_n(1, |tc| {
            let route = draw::route(tc, &d.s);
            assert!(read_account_meta(&d, &route, "o-test-l2", "t-0:own-meta1", Instant::now()).is_none());
            assert!(read_tx_meta(&d, &route, "o-test-l2", 1, Instant::now() + Duration::from_secs(2)).is_none());
        });
        assert_eq!((ft.calls_of("bucket", "GetAccount").len(), ft.calls_of("bucket", "GetTransaction").len()), before);
        assert!(ft.calls_of("bucket", "GetAccount").iter().all(|b| b["timeoutMs"].as_u64().unwrap() <= 3000));
    }
}
