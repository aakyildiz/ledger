//! Task 03 — reverts: the `revert` order generator, `revert_concurrent`
//! (k concurrent keyed reverts of one committed transaction) and
//! `revert_twice` (a rule-owned transaction reverted twice on `o-<run>-l2`).
use crate::clients::{CallOpts, Outcome};
use crate::draw;
use crate::machine::{rule, Driver};
use crate::read::{self, LedgerSel, ViewReq};
use crate::rules::metadata::meta_map;
use crate::wire::{self, Posting, U256};
use crate::write::{BulkCtx, OrderGen, WritePlan, WriteResult, ATTEMPT_TIMEOUT};
use hegel::stateful::ConcurrentRule;
use hegel::TestCase;
use serde_json::{json, Value};
use std::time::{Duration, Instant};

pub const FAMILY: &str = "reverts";
const ALREADY_REVERTED: &str = "TRANSACTION_ALREADY_REVERTED";
/// Chain budget of `revert_twice` (three sequential tracked writes).
const CHAIN_BUDGET: Duration = Duration::from_secs(12);
/// Above this many transactions the ledger gets no new reverts (DESIGN §3.2 taper).
const REVERT_TAPER_TXS: u64 = 4000;
/// The client's transport margin on top of a call's RPC deadline
/// (`Clients::call_with_op`).
const TRANSPORT_MARGIN: Duration = Duration::from_millis(1500);
/// What one chain write must have left before it starts: one attempt, with
/// the transport margin.
pub const STEP_RESERVE: Duration = Duration::from_millis(ATTEMPT_TIMEOUT.as_millis() as u64 + TRANSPORT_MARGIN.as_millis() as u64);
/// The longest RPC deadline of a client read inside a chain or rule.
pub const READ_CAP: Duration = Duration::from_secs(3);
/// The shortest RPC deadline worth sending a chain read with.
const MIN_READ: Duration = Duration::from_secs(1);

/// The retry budget of the next chain write, or `None` when less than
/// `STEP_RESERVE` is left (the chain ends without asserting). The write's
/// last attempt starts before `deadline - STEP_RESERVE`, so the whole write,
/// that attempt included, ends by `deadline`.
pub fn step_budget(deadline: Instant) -> Option<Duration> {
    deadline.saturating_duration_since(Instant::now()).checked_sub(STEP_RESERVE)
}

/// The options of a linearizable client read inside a chain: an RPC deadline
/// of `min(left - margin, READ_CAP)`, or `None` when less than `MIN_READ`
/// would be left (the chain ends without asserting).
pub fn chain_read_opts(deadline: Instant) -> Option<CallOpts> {
    let left = deadline.saturating_duration_since(Instant::now()).checked_sub(TRANSPORT_MARGIN)?;
    (left >= MIN_READ).then(|| CallOpts::linearizable().timeout(left.min(READ_CAP)))
}

/// The DESIGN §3.2 taper bands for a ledger holding `n` transactions: below
/// 500 always, 500–2000 one half, 2000–4000 one eighth, from 4000 never.
pub fn taper_count_draw(tc: &TestCase, n: u64) -> bool {
    match n {
        n if n < 500 => true,
        n if n < 2000 => draw::chance(tc, 0.5),
        n if n < REVERT_TAPER_TXS => draw::chance(tc, 0.125),
        _ => false,
    }
}

/// The taper of the rule-owned transactions on `o-<run>-l2` (never deleted,
/// DESIGN §3.1): reads l2's `txCount` and applies the §3.2 bands. `None`
/// means skip the creating step (also when l2 is unresolved or the view is
/// unavailable).
pub fn l2_taper(d: &Driver, tc: &TestCase, ledger: &str) -> Option<()> {
    let v = d.view(&[ViewReq::ledger(LedgerSel::Name(ledger.to_string()))])?.pop()?;
    if read::unresolved(&v) {
        return None;
    }
    taper_count_draw(tc, wire::u64_field(&v, "txCount")).then_some(())
}

pub fn rules() -> Vec<ConcurrentRule<Driver>> {
    vec![rule("revert_concurrent", 2.0, revert_concurrent), rule("revert_twice", 0.6, revert_twice)]
}

pub fn order_gens() -> Vec<OrderGen> {
    vec![OrderGen { name: "revert", weight: 3.0, applies: revert_applies, has_opt_in: true, gen: gen_revert }]
}

fn revert_applies(ctx: &BulkCtx) -> bool {
    let n = wire::u64_field(ctx.ledger_view, "txCount");
    n > 0 && n < REVERT_TAPER_TXS
}

/// One `revertTransaction` request (protojson).
pub fn revert_request(ledger: &str, id: u64, force: bool, at_effective_date: bool, metadata: Option<Value>, opt_in: bool) -> Value {
    let mut rt = json!({"transactionId": id.to_string()});
    if force {
        rt["force"] = json!(true);
    }
    if at_effective_date {
        rt["atEffectiveDate"] = json!(true);
    }
    if let Some(m) = metadata {
        rt["metadata"] = m;
    }
    let mut apply = json!({"ledger": ledger, "action": {"revertTransaction": rt}});
    if opt_in {
        apply["skippableReasons"] = json!(["ERROR_REASON_TRANSACTION_ALREADY_REVERTED"]);
    }
    json!({"apply": apply})
}

/// `revert` (weight 3): a committed transaction from `seek(txs)` (reserved
/// transactions are invisible), 1/8 a revert transaction (`revertSeek`),
/// `force` 1/2, `atEffectiveDate` 1/2, `metadata` 1/2; the opt-in adds
/// `ERROR_REASON_TRANSACTION_ALREADY_REVERTED`. Tapered by `txCount` (§3.2).
fn gen_revert(d: &Driver, tc: &TestCase, ctx: &BulkCtx, opt_in: bool) -> Option<Value> {
    // Reverts create transactions: the in-gen half of the taper (`applies`
    // cuts off at 4000 without a draw).
    let count = wire::u64_field(ctx.ledger_view, "txCount");
    if !taper_count_draw(tc, count) {
        return None;
    }
    let n = count.max(1);
    let probe = draw::int_in(tc, 1, n).to_string();
    let of_revert = draw::chance(tc, 0.125);
    let force = draw::chance(tc, 0.5);
    let at_effective_date = draw::chance(tc, 0.5);
    let metadata = if draw::chance(tc, 0.5) { Some(meta_map(tc)) } else { None };
    let sel = LedgerSel::Name(ctx.ledger.to_string());
    let mut views = vec![ViewReq::seek(sel.clone(), "txs", &probe, None)];
    if of_revert {
        views.push(ViewReq::named("revertSeek", json!({"ledger": ctx.ledger, "probe": probe})));
    }
    let got = d.view(&views)?;
    let pick = got.iter().rev().find(|v| v["found"] == json!(true))?;
    let id = wire::u64_field(&pick["entry"], "id");
    if id == 0 {
        return None;
    }
    Some(revert_request(ctx.ledger, id, force, at_effective_date, metadata, opt_in))
}

// ---------------------------------------------------------------- reversal

/// Checks one successful revert response against its target (fixes
/// main-drivers §1.3 #5): one log on `ledger`, the reversal of `target`,
/// a new id that reverts `target`, posting *i* the in-place reverse of the
/// original posting *i* (amount, asset, color).
pub fn check_reversal(resp: &Value, ledger: &str, target: u64, original: &[Value]) -> Result<(), String> {
    let logs = wire::apply_logs(resp);
    if logs.len() != 1 {
        return Err(format!("expected one revert log, got {}", logs.len()));
    }
    let log = &logs[0];
    if log.pointer("/payload/apply/ledgerName").and_then(Value::as_str) != Some(ledger) {
        return Err(format!("missing apply log for ledger {ledger:?}"));
    }
    let rt = wire::reverted_transaction(log).ok_or_else(|| format!("missing reversal of transaction {target}"))?;
    if wire::u64_field(rt, "revertedTransactionId") != target {
        return Err(format!("reversal names transaction {} instead of {target}", wire::u64_field(rt, "revertedTransactionId")));
    }
    let tx = rt.get("revertTransaction").ok_or("missing revert transaction")?;
    let id = wire::u64_field(tx, "id");
    if id == target || wire::u64_field(tx, "revertsTransaction") != target {
        return Err(format!("invalid reversal identity: id {id}, revertsTransaction {}", wire::u64_field(tx, "revertsTransaction")));
    }
    let got = tx.get("postings").and_then(Value::as_array).cloned().unwrap_or_default();
    if got.len() != original.len() {
        return Err(format!("expected {} reversed postings, got {}", original.len(), got.len()));
    }
    for (i, (o, g)) in original.iter().zip(got.iter()).enumerate() {
        let amount = |p: &Value| U256::from_json(p.get("amount").unwrap_or(&json!({})));
        let ok = amount(o).is_some()
            && amount(o) == amount(g)
            && wire::str_field(g, "source") == wire::str_field(o, "destination")
            && wire::str_field(g, "destination") == wire::str_field(o, "source")
            && wire::str_field(g, "asset") == wire::str_field(o, "asset")
            && wire::str_field(g, "color") == wire::str_field(o, "color");
        if !ok {
            return Err(format!("posting {i} does not exactly reverse the original"));
        }
    }
    Ok(())
}

// ---------------------------------------------------------------- revert_concurrent

/// Pre-drawn fan-out of `revert_concurrent`.
const MAX_CONCURRENT: usize = 3;

/// `revert_concurrent` (weight 2): one committed, non-reverted transaction
/// (`seek(txs)`, 3 drawn probes), its postings read back linearizably, then
/// `k ∈ 1..=grant` concurrent forced reverts under distinct keys.
pub fn revert_concurrent(d: &Driver, tc: TestCase) {
    d.guarded("revert_concurrent", &tc, |d, tc| {
        if !d.s.oracle_ok() {
            return None;
        }
        let pick = draw::ledger_pick(tc);
        let lv = d.view(&[ViewReq::ledger(LedgerSel::Pick(pick))])?.pop()?;
        let ledger = lv.get("ledger").and_then(Value::as_str)?.to_string();
        let n = wire::u64_field(&lv, "txCount");
        if n == 0 {
            return None;
        }
        let probes: Vec<String> = (0..3).map(|_| draw::int_in(tc, 1, n).to_string()).collect();
        let route = draw::route(tc, &d.s);
        let mut plans: Vec<WritePlan> = (0..MAX_CONCURRENT).map(|_| WritePlan::drawn(FAMILY, vec![], d, tc)).collect();

        let seeks: Vec<ViewReq> = probes.iter().map(|p| ViewReq::seek(LedgerSel::Name(ledger.clone()), "txs", p, None)).collect();
        let got = d.view(&seeks)?;
        let entry = got.iter().find(|v| v["found"] == json!(true) && v["entry"]["reverted"] != json!(true))?;
        let target = wire::u64_field(&entry["entry"], "id");
        if target == 0 {
            return None;
        }
        // The original postings (immutable once committed).
        let req = json!({"ledger": ledger, "transactionId": target.to_string()});
        let (_, out) = d.call(&route, "bucket", "GetTransaction", req, &CallOpts::linearizable().timeout(READ_CAP));
        let tx = out.response.as_ref().filter(|_| out.is_ok())?.get("transaction")?.clone();
        if tx.get("reverted").and_then(Value::as_bool).unwrap_or(false) {
            return None;
        }
        let postings = tx.get("postings").and_then(Value::as_array).cloned().unwrap_or_default();

        let mut grant = d.s.slots.try_acquire_up_to(MAX_CONCURRENT)?;
        let k = draw::int_in(tc, 1, grant.len() as u64) as usize;
        grant.split(k);
        plans.truncate(k);
        for p in plans.iter_mut() {
            p.requests = vec![revert_request(&ledger, target, true, false, None, false)];
            p.hint = json!({"gen": "revert_concurrent", "k": k});
        }
        let results = d.dispatch_many(plans, grant);
        judge_concurrent(&ledger, target, &postings, k, &results);
        Some(())
    });
}

/// The verbatim `MD/parallel_driver_revert_concurrent` properties over the
/// definitive outcomes of one fan-out.
pub fn judge_concurrent(ledger: &str, target: u64, postings: &[Value], k: usize, results: &[WriteResult]) {
    let definitive: Vec<&Outcome> = results.iter().filter_map(WriteResult::outcome).collect();
    if definitive.is_empty() {
        return;
    }
    let mut successes = 0;
    for (i, o) in definitive.iter().enumerate() {
        if !o.is_ok() {
            continue;
        }
        successes += 1;
        let check = check_reversal(o.response.as_ref().unwrap_or(&Value::Null), ledger, target, postings);
        check_always_or_unreachable!(
            check.is_ok(),
            "successful concurrent revert contains exactly the expected reversal",
            json!({"ledger": ledger, "txId": target, "worker": i, "error": check.err()})
        );
    }
    let reasons: Vec<&str> = definitive.iter().filter(|o| !o.is_ok()).map(|o| o.reason.as_str()).collect();
    check_always_or_unreachable!(
        successes <= 1,
        "at most one concurrent revert should succeed",
        json!({"ledger": ledger, "txId": target, "k": k, "successes": successes, "failures": reasons})
    );
    if k >= 2 {
        check_reachable!("concurrent revert path exercised", json!({"ledger": ledger, "txId": target, "k": k}));
    }
}

// ---------------------------------------------------------------- revert_twice

/// Dispatches one write of a chain on a fresh slot unit with what is left of
/// the chain budget (`step_budget`). `None` when the budget is spent, no unit
/// is free or the oracle refused.
fn chain_step(d: &Driver, plan: WritePlan, deadline: Instant) -> Option<WriteResult> {
    let budget = step_budget(deadline)?;
    let unit = d.s.slots.try_acquire_up_to(1)?.take_one()?;
    let t = d.begin_write(plan, unit).ok()?;
    Some(t.run(budget))
}

/// A definitive outcome whose failure reason makes a deterministic
/// expectation inconclusive (the ledger itself is unavailable).
fn inconclusive(o: &Outcome) -> bool {
    !o.is_ok() && o.reason.starts_with("LEDGER_")
}

/// `revert_twice` (weight 0.6): a forced `world → t-0:own-rv<N>` transaction
/// with reference `o-<run>-own-ref<M>` on `o-<run>-l2` (reserved: no
/// generator can revert it), a forced revert, and a second forced revert —
/// three sequential tracked writes within a 12 s chain budget. l2 is never
/// deleted, so the rule follows the §3.2 taper on l2's `txCount`.
pub fn revert_twice(d: &Driver, tc: TestCase) {
    d.guarded("revert_twice", &tc, |d, tc| {
        if !d.s.oracle_ok() {
            return None;
        }
        let amount = draw::amount_positive(tc);
        let asset = draw::asset(tc);
        let plans: Vec<WritePlan> = (0..3).map(|_| WritePlan::drawn(FAMILY, vec![], d, tc)).collect();
        let ledger = format!("o-{}-l2", d.s.run);
        let addr = draw::owned_address(&d.s, "t-0", "rv");
        l2_taper(d, tc, &ledger)?;
        let reference = d.s.next_own_ref("ref");
        let deadline = Instant::now() + CHAIN_BUDGET;
        let mut plans = plans.into_iter();

        let mut create = plans.next()?;
        let mut req = wire::create_tx_request(&ledger, &[Posting::new("world", &addr, amount, asset)], Some(&reference));
        req["apply"]["action"]["createTransaction"]["force"] = json!(true);
        create.requests = vec![req];
        create.hint = json!({"gen": "revert_twice", "step": "create"});
        let created = chain_step(d, create, deadline)?;
        let out = created.outcome()?;
        if !out.is_ok() {
            return None;
        }
        let logs = wire::apply_logs(out.response.as_ref()?);
        let id = wire::u64_field(wire::created_transaction(logs.first()?)?, "id");
        if id == 0 {
            return None;
        }
        let details = json!({"ledger": ledger, "txId": id, "reference": reference});

        let mut first = plans.next()?;
        first.requests = vec![revert_request(&ledger, id, true, false, None, false)];
        first.hint = json!({"gen": "revert_twice", "step": "first"});
        let r1 = chain_step(d, first, deadline)?;
        let o1 = r1.outcome()?;
        if inconclusive(o1) {
            return None;
        }
        check_sometimes!(o1.is_ok(), "first revert should succeed", json!({"details": details, "outcome": o1.summary()}));
        if !o1.is_ok() {
            return None;
        }

        let mut second = plans.next()?;
        second.requests = vec![revert_request(&ledger, id, true, false, None, false)];
        second.hint = json!({"gen": "revert_twice", "step": "second"});
        let r2 = chain_step(d, second, deadline)?;
        let o2 = r2.outcome()?;
        if inconclusive(o2) {
            return None;
        }
        if o2.is_ok() {
            check_unreachable!("double revert should fail", details);
            return Some(());
        }
        check_always_or_unreachable!(
            o2.reason == ALREADY_REVERTED,
            "double revert should return TRANSACTION_ALREADY_REVERTED",
            json!({"details": details, "outcome": o2.summary()})
        );
        check_reachable!("double revert path exercised", details);
        Some(())
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testkit::{self, FakeReply};

    fn posting(src: &str, dst: &str, amount: U256, asset: &str, color: &str) -> Value {
        let mut p = Posting::new(src, dst, amount, asset).to_json();
        if !color.is_empty() {
            p["color"] = json!(color);
        }
        p
    }

    fn revert_resp(ledger: &str, target: u64, id: u64, reverts: u64, postings: Vec<Value>) -> Value {
        json!({"logs": [{"sequence": "9", "payload": {"apply": {"ledgerName": ledger, "log": {"id": "3", "data": {"revertedTransaction": {
            "revertedTransactionId": target.to_string(),
            "revertTransaction": {"id": id.to_string(), "revertsTransaction": reverts.to_string(), "postings": postings}
        }}}}}}]})
    }

    #[test]
    fn reversal_check_in_place_and_identity() {
        let orig = vec![posting("world", "users:1", U256::MAX, "USD/2", ""), posting("users:2", "t-0:1", U256::from(5), "COIN", "AB")];
        let good = vec![posting("users:1", "world", U256::MAX, "USD/2", ""), posting("t-0:1", "users:2", U256::from(5), "COIN", "AB")];
        assert_eq!(check_reversal(&revert_resp("L", 4, 9, 4, good.clone()), "L", 4, &orig), Ok(()));
        // Reversed order is not in place.
        let swapped = vec![good[1].clone(), good[0].clone()];
        assert!(check_reversal(&revert_resp("L", 4, 9, 4, swapped), "L", 4, &orig).is_err());
        // Identity: id == target, wrong revertsTransaction, wrong reverted id, wrong ledger.
        assert!(check_reversal(&revert_resp("L", 4, 4, 4, good.clone()), "L", 4, &orig).is_err());
        assert!(check_reversal(&revert_resp("L", 4, 9, 5, good.clone()), "L", 4, &orig).is_err());
        assert!(check_reversal(&revert_resp("L", 5, 9, 4, good.clone()), "L", 4, &orig).is_err());
        assert!(check_reversal(&revert_resp("M", 4, 9, 4, good.clone()), "L", 4, &orig).is_err());
        // Amount, color and count.
        let mut bad = good.clone();
        bad[0]["amount"] = U256::from(1).to_json();
        assert!(check_reversal(&revert_resp("L", 4, 9, 4, bad), "L", 4, &orig).is_err());
        let mut bad = good.clone();
        bad[1]["color"] = json!("");
        assert!(check_reversal(&revert_resp("L", 4, 9, 4, bad), "L", 4, &orig).is_err());
        assert!(check_reversal(&revert_resp("L", 4, 9, 4, good[..1].to_vec()), "L", 4, &orig).is_err());
        assert!(check_reversal(&json!({"logs": []}), "L", 4, &orig).is_err());
    }

    #[test]
    fn revert_request_shape() {
        let r = revert_request("L", 7, true, true, Some(json!({"k0": {"stringValue": "v"}})), true);
        assert_eq!(
            r,
            json!({"apply": {"ledger": "L", "skippableReasons": ["ERROR_REASON_TRANSACTION_ALREADY_REVERTED"], "action": {"revertTransaction": {
                "transactionId": "7", "force": true, "atEffectiveDate": true, "metadata": {"k0": {"stringValue": "v"}}}}}})
        );
        assert_eq!(revert_request("L", 7, false, false, None, false), json!({"apply": {"ledger": "L", "action": {"revertTransaction": {"transactionId": "7"}}}}));
    }

    fn concurrent_setup() -> (Driver, std::sync::Arc<testkit::FakeTransport>, std::sync::Arc<testkit::FakeOracle>, Vec<Value>) {
        let (d, ft, fo) = testkit::driver();
        fo.set_live(vec!["o-test-l1".into()]);
        fo.set_view("ledger", json!({"txCount": 10}));
        fo.set_view("seek", json!({"found": true, "wrapped": false, "entry": {"id": "4", "reference": "", "reverted": false, "postingCount": 1, "force": false}}));
        let orig = vec![posting("world", "users:1", U256::from(7), "COIN", "")];
        ft.set_default("bucket", "GetTransaction", FakeReply::Outcome(Outcome::from_json(&json!({"ok": true, "response": {"transaction": {"id": "4", "postings": orig}}}))));
        (d, ft, fo, orig)
    }

    #[test]
    fn revert_concurrent_two_successes_violate_at_most_one() {
        let (d, ft, fo, _) = concurrent_setup();
        let rev = vec![posting("users:1", "world", U256::from(7), "COIN", "")];
        let ok = Outcome::from_json(&json!({"ok": true, "response": revert_resp("o-test-l1", 4, 11, 4, rev)}));
        ft.set_default("bucket", "Apply", FakeReply::Outcome(ok));
        // Run until a fan-out of at least 2 was drawn.
        for _ in 0..40 {
            testkit::reset();
            testkit::run_rule(&d, revert_concurrent);
            if fo.calls("/write/begin").len() >= 2 && testkit::fired("concurrent revert path exercised") {
                break;
            }
        }
        let a = testkit::assertions();
        let at_most_one: Vec<_> = a.iter().filter(|r| r.message == "at most one concurrent revert should succeed").collect();
        assert!(!at_most_one.is_empty(), "{a:?}");
        assert!(at_most_one.iter().any(|r| !r.cond), "two successes must make the property false: {a:?}");
        assert!(a.iter().filter(|r| r.message == "successful concurrent revert contains exactly the expected reversal").all(|r| r.cond));
        assert!(testkit::fired("concurrent revert path exercised"));
        // Distinct keys, one revert body each.
        let begins = fo.calls("/write/begin");
        let keys: std::collections::BTreeSet<_> = begins.iter().map(|b| b["idempotencyKey"].as_str().unwrap().to_string()).collect();
        assert_eq!(keys.len(), begins.len());
        assert_eq!(begins[0]["requests"][0]["apply"]["action"]["revertTransaction"]["transactionId"], "4");
    }

    #[test]
    fn revert_concurrent_one_success_one_already_reverted_holds() {
        let (d, ft, _fo, _) = concurrent_setup();
        let rev = vec![posting("users:1", "world", U256::from(7), "COIN", "")];
        ft.script(
            "bucket",
            "Apply",
            vec![
                FakeReply::Outcome(Outcome::from_json(&json!({"ok": true, "response": revert_resp("o-test-l1", 4, 11, 4, rev)}))),
                FakeReply::Outcome(Outcome::from_json(&json!({"ok": false, "code": "FailedPrecondition", "reason": ALREADY_REVERTED}))),
            ],
        );
        ft.set_default("bucket", "Apply", FakeReply::Outcome(Outcome::from_json(&json!({"ok": false, "code": "FailedPrecondition", "reason": ALREADY_REVERTED}))));
        testkit::run_rule(&d, revert_concurrent);
        let a = testkit::assertions();
        assert!(a.iter().filter(|r| r.message == "at most one concurrent revert should succeed").all(|r| r.cond), "{a:?}");
        assert!(a.iter().any(|r| r.message == "at most one concurrent revert should succeed"));
    }

    #[test]
    fn revert_twice_chain_asserts_double_revert() {
        let (d, ft, fo) = testkit::driver();
        let created = json!({"ok": true, "response": {"logs": [{"sequence": "1", "payload": {"apply": {"ledgerName": "o-test-l2", "log": {"data": {"createdTransaction": {"transaction": {"id": "12"}}}}}}}]}});
        ft.script(
            "bucket",
            "Apply",
            vec![
                FakeReply::Outcome(Outcome::from_json(&created)),
                FakeReply::Outcome(Outcome::from_json(&json!({"ok": true, "response": revert_resp("o-test-l2", 12, 13, 12, vec![])}))),
                FakeReply::Outcome(Outcome::from_json(&json!({"ok": false, "code": "FailedPrecondition", "reason": ALREADY_REVERTED}))),
            ],
        );
        testkit::run_rule(&d, revert_twice);
        let begins = fo.calls("/write/begin");
        assert_eq!(begins.len(), 3);
        let create = &begins[0]["requests"][0]["apply"];
        assert_eq!(create["ledger"], "o-test-l2");
        assert!(create["action"]["createTransaction"]["reference"].as_str().unwrap().starts_with("o-test-own-ref"));
        assert!(create["action"]["createTransaction"]["postings"][0]["destination"].as_str().unwrap().starts_with("t-0:own-rv"));
        assert_eq!(begins[1]["requests"][0]["apply"]["action"]["revertTransaction"]["transactionId"], "12");
        assert!(testkit::assertions().iter().any(|r| r.message == "first revert should succeed" && r.cond));
        assert!(testkit::assertions().iter().any(|r| r.message == "double revert should return TRANSACTION_ALREADY_REVERTED" && r.cond));
        assert!(testkit::fired("double revert path exercised"));
        assert!(!testkit::fired("double revert should fail"));
    }

    #[test]
    fn chain_budget_reserves_one_attempt_and_caps_reads() {
        let now = Instant::now();
        assert_eq!(step_budget(now), None);
        assert_eq!(step_budget(now + Duration::from_secs(5)), None, "less than one attempt plus margin");
        let b = step_budget(now + CHAIN_BUDGET).unwrap();
        assert!(b <= CHAIN_BUDGET - STEP_RESERVE && b > Duration::from_secs(6), "{b:?}");
        assert!(chain_read_opts(now).is_none());
        assert!(chain_read_opts(now + Duration::from_secs(2)).is_none(), "0.5 s after the margin is too short");
        let r = chain_read_opts(now + CHAIN_BUDGET).unwrap();
        assert_eq!((r.timeout, r.consistency), (READ_CAP, Some("linearizable")));
        let r = chain_read_opts(now + Duration::from_secs(3)).unwrap();
        assert!(r.timeout <= Duration::from_millis(1500) && r.timeout >= MIN_READ, "{:?}", r.timeout);
    }

    #[test]
    fn chain_step_without_budget_sends_nothing() {
        let (d, ft, fo) = testkit::driver();
        testkit::with_tc_n(1, |tc| {
            let plan = WritePlan::drawn(FAMILY, vec![revert_request("o-test-l2", 1, true, false, None, false)], &d, tc);
            assert!(chain_step(&d, plan, Instant::now() + Duration::from_secs(5)).is_none());
        });
        assert!(fo.calls("/write/begin").is_empty());
        assert!(ft.calls().is_empty());
    }

    #[test]
    fn taper_bands() {
        let counts = std::sync::Mutex::new([0usize; 3]);
        testkit::with_tc_n(800, |tc| {
            assert!(taper_count_draw(tc, 0) && taper_count_draw(tc, 499));
            assert!(!taper_count_draw(tc, 4000) && !taper_count_draw(tc, u64::MAX));
            let mut c = counts.lock().unwrap();
            c[0] += 1;
            c[1] += taper_count_draw(tc, 1000) as usize;
            c[2] += taper_count_draw(tc, 3000) as usize;
        });
        let [n, half, eighth] = *counts.lock().unwrap();
        assert!(half > n / 4 && half < n * 3 / 4, "{half}/{n}");
        assert!(eighth > 0 && eighth < n / 4, "{eighth}/{n}");
    }

    #[test]
    fn gen_revert_follows_the_taper_and_falls_back_from_revert_seek() {
        let (d, _ft, fo) = testkit::driver();
        fo.set_view("seek", json!({"found": true, "wrapped": false, "entry": {"id": "3", "reverted": false}}));
        // The only revert transaction is reserved: the core hides it, so
        // revertSeek answers not found and the gen falls back to seek(txs).
        fo.set_view("revertSeek", json!({"found": false, "wrapped": false, "entry": null}));
        let counts = std::sync::Mutex::new((0usize, 0usize, 0usize));
        testkit::with_tc_n(400, |tc| {
            for (n, slot) in [(100u64, 0usize), (3000, 1)] {
                let lv = json!({"ledger": "L", "txCount": n});
                let ctx = BulkCtx { run: "test", ledger: "L", ledger_view: &lv, index_in_bulk: 0, len: 1 };
                let got = gen_revert(&d, tc, &ctx, false);
                let mut c = counts.lock().unwrap();
                c.2 += (slot == 0) as usize;
                if let Some(v) = got {
                    assert_eq!(v["apply"]["action"]["revertTransaction"]["transactionId"], "3", "{v}");
                    if slot == 0 {
                        c.0 += 1;
                    } else {
                        c.1 += 1;
                    }
                }
            }
        });
        let (low, high, n) = *counts.lock().unwrap();
        assert_eq!(low, n, "below 500 transactions every draw reverts");
        assert!(high < n / 4, "2000–4000: about one in eight ({high}/{n})");
        assert!(fo.calls("/view").iter().any(|b| b.to_string().contains("revertSeek")), "the revert-of-a-revert branch ran");
    }

    #[test]
    fn revert_twice_skips_a_full_l2_and_caps_concurrent_reads() {
        let (d, ft, fo) = testkit::driver();
        fo.set_view("ledger", json!({"txCount": 4000}));
        for _ in 0..8 {
            testkit::run_rule(&d, revert_twice);
        }
        assert!(fo.calls("/write/begin").is_empty(), "no new transaction on a full l2");

        let (d, ft2, _fo, _) = concurrent_setup();
        testkit::run_rule(&d, revert_concurrent);
        let gets = ft2.calls_of("bucket", "GetTransaction");
        assert!(!gets.is_empty());
        assert!(gets.iter().all(|b| b["timeoutMs"].as_u64().unwrap() <= READ_CAP.as_millis() as u64), "{gets:?}");
        drop(ft);
    }
}
