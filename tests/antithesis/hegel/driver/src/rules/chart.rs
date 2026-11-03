//! Task 04: chart and enforcement order generators and the family's local
//! bulk rule (`chart_bulk`). Ports `M/actions.go` generateAddAccountType /
//! generateRemoveAccountType / pickPersistence and `M/apply_surface.go`
//! (nested forms with skip opt-ins, the deliberately disallowed opt-in on the
//! enforcement-mode setter).
//!
//! Every generator skips `o-<run>-l2` (DESIGN §3.1: its types `t-0..t-2` and
//! its default enforcement mode never change) and reserved ledgers, and only
//! names pool types `t-3..t-24`, so no generator produces a reserved name.
use crate::draw;
use crate::machine::{rule, Driver};
use crate::read::{LedgerSel, ViewReq};
use crate::write::{compose_bulk, BulkCtx, OrderGen, WritePlan};
use hegel::stateful::ConcurrentRule;
use hegel::TestCase;
use serde_json::{json, Value};

pub fn rules() -> Vec<ConcurrentRule<Driver>> {
    vec![rule("chart_bulk", 1.0, chart_bulk)]
}

pub fn order_gens() -> Vec<OrderGen> {
    vec![
        OrderGen { name: "chart_add", weight: 1.5, applies: chart_applies, has_opt_in: true, gen: gen_chart_add },
        OrderGen { name: "chart_remove", weight: 1.5, applies: chart_applies, has_opt_in: true, gen: gen_chart_remove },
        OrderGen { name: "enforcement", weight: 0.5, applies: chart_applies, has_opt_in: true, gen: gen_enforcement },
    ]
}

/// The lowest and highest pool type index the chart generators use
/// (`t-0..t-2` belong to l2's fixed chart).
pub const POOL_LO: u64 = 3;
pub const POOL_HI: u64 = 24;

/// A ledger the chart generators may target: neither l2 nor a reserved one.
pub fn chart_target(run: &str, ledger: &str) -> bool {
    ledger != format!("o-{run}-l2") && !ledger.starts_with(&format!("o-{run}-own"))
}

fn chart_applies(ctx: &BulkCtx) -> bool {
    chart_target(ctx.run, ctx.ledger)
}

/// `persistence`: NORMAL 1/2, EPHEMERAL 1/3, TRANSIENT 1/6.
fn persistence(tc: &TestCase) -> &'static str {
    match draw::int_in(tc, 0, 5) {
        0..=2 => "ACCOUNT_TYPE_NORMAL",
        3 | 4 => "ACCOUNT_TYPE_EPHEMERAL",
        _ => "ACCOUNT_TYPE_TRANSIENT",
    }
}

fn pool_name(tc: &TestCase) -> String {
    format!("t-{}", draw::int_in(tc, POOL_LO, POOL_HI))
}

/// The pool type names the `ledger` view lists for the target.
fn existing_pool_types(view: &Value) -> Vec<String> {
    view.get("types")
        .and_then(Value::as_array)
        .map(|ts| {
            ts.iter()
                .filter_map(|t| t.get("name").and_then(Value::as_str))
                .filter(|n| n.strip_prefix("t-").and_then(|i| i.parse::<u64>().ok()).is_some_and(|i| (POOL_LO..=POOL_HI).contains(&i)))
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default()
}

/// `addAccountType t-N` (pattern `t-N:{id}`): top-level, or nested (1/4,
/// always with the opt-in) under `apply` with `ACCOUNT_TYPE_ALREADY_EXISTS`.
fn gen_chart_add(_d: &Driver, tc: &TestCase, ctx: &BulkCtx, opt_in: bool) -> Option<Value> {
    if !chart_applies(ctx) {
        return None;
    }
    let name = pool_name(tc);
    let account_type = json!({"name": name, "pattern": format!("{name}:{{id}}"), "persistence": persistence(tc)});
    let nested = opt_in || draw::chance(tc, 0.25);
    if !nested {
        return Some(json!({"addAccountType": {"ledger": ctx.ledger, "accountType": account_type}}));
    }
    let mut apply = json!({"ledger": ctx.ledger, "action": {"addAccountType": {"accountType": account_type}}});
    if opt_in {
        apply["skippableReasons"] = json!(["ERROR_REASON_ACCOUNT_TYPE_ALREADY_EXISTS"]);
    }
    Some(json!({ "apply": apply }))
}

/// `removeAccountType` of a pool name (an existing pool type 1/2 when the
/// ledger has one, else any pool name), forms and opt-in as for adds.
fn gen_chart_remove(_d: &Driver, tc: &TestCase, ctx: &BulkCtx, opt_in: bool) -> Option<Value> {
    if !chart_applies(ctx) {
        return None;
    }
    let existing = existing_pool_types(ctx.ledger_view);
    let name = if !existing.is_empty() && draw::chance(tc, 0.5) { draw::one_of(tc, &existing).clone() } else { pool_name(tc) };
    let nested = opt_in || draw::chance(tc, 0.25);
    if !nested {
        return Some(json!({"removeAccountType": {"ledger": ctx.ledger, "name": name}}));
    }
    let mut apply = json!({"ledger": ctx.ledger, "action": {"removeAccountType": {"name": name}}});
    if opt_in {
        apply["skippableReasons"] = json!(["ERROR_REASON_ACCOUNT_TYPE_NOT_FOUND"]);
    }
    Some(json!({ "apply": apply }))
}

/// `setDefaultEnforcementMode` STRICT/AUDIT, top-level or nested; the opt-in
/// variant nests it with a skippable reason the setter does not allow, which
/// admission refuses with `VALIDATION` (M/apply_surface.go
/// generatedSkippableReason).
fn gen_enforcement(_d: &Driver, tc: &TestCase, ctx: &BulkCtx, opt_in: bool) -> Option<Value> {
    if !chart_applies(ctx) {
        return None;
    }
    let mode = if draw::chance(tc, 0.5) { "CHART_ENFORCEMENT_STRICT" } else { "CHART_ENFORCEMENT_AUDIT" };
    let nested = opt_in || draw::chance(tc, 0.5);
    if !nested {
        return Some(json!({"setDefaultEnforcementMode": {"ledger": ctx.ledger, "enforcementMode": mode}}));
    }
    let mut apply = json!({"ledger": ctx.ledger, "action": {"setDefaultEnforcementMode": {"enforcementMode": mode}}});
    if opt_in {
        apply["skippableReasons"] = json!(["ERROR_REASON_ACCOUNT_TYPE_NOT_FOUND"]);
    }
    Some(json!({ "apply": apply }))
}

/// Picks a chart target from the `ledgers` view's live list and returns it
/// with its `ledger` view. `None` when no live ledger qualifies.
pub fn pick_chart_ledger(d: &Driver, tc: &TestCase) -> Option<(String, Value)> {
    let views = d.view(&[ViewReq::ledgers()])?;
    let live: Vec<String> = views.first()?.get("live").and_then(Value::as_array)?.iter().filter_map(Value::as_str).filter(|l| chart_target(&d.s.run, l)).map(str::to_string).collect();
    if live.is_empty() {
        return None;
    }
    let ledger = draw::one_of(tc, &live).clone();
    let view = d.view(&[ViewReq::ledger(LedgerSel::Name(ledger.clone()))])?.into_iter().next()?;
    Some((ledger, view))
}

/// `compose_bulk` over this family's three generators (1–3 orders) on one
/// non-l2 live ledger (DESIGN §9.2 item 5).
pub fn chart_bulk(d: &Driver, tc: TestCase) {
    d.guarded("chart_bulk", &tc, |d, tc| {
        if !d.s.oracle_ok() {
            return None;
        }
        let (ledger, view) = pick_chart_ledger(d, tc)?;
        let len = draw::int_in(tc, 1, 3) as usize;
        let reqs = compose_bulk(d, tc, &order_gens(), &ledger, &view, len);
        if reqs.is_empty() {
            return None;
        }
        let mut plan = WritePlan::drawn("chart", reqs, d, tc);
        plan.hint = json!({"gen": "chart_bulk"});
        let unit = d.s.slots.try_acquire_up_to(1)?.take_one()?;
        let _ = d.dispatch(plan, unit);
        Some(())
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testkit;

    /// Every order a chart generator emits targets the context ledger,
    /// never l2 or a reserved ledger, names only pool types `t-3..t-24`, and
    /// carries no reserved name.
    #[test]
    fn chart_gens_never_target_l2_and_never_produce_reserved_names() {
        let (d, _, _) = testkit::driver();
        let run = d.s.run.clone();
        let l2 = format!("o-{run}-l2");
        let own = format!("o-{run}-own7");
        let view = json!({"types": [{"name": "t-5"}, {"name": "t-0"}, {"name": "t-24"}]});
        for g in order_gens() {
            for forbidden in [&l2, &own] {
                let ctx = BulkCtx { run: &run, ledger: forbidden, ledger_view: &view, index_in_bulk: 0, len: 1 };
                assert!(!(g.applies)(&ctx), "{} applies to {forbidden}", g.name);
            }
        }
        let gens = order_gens();
        testkit::with_tc_n(300, |tc| {
            for target in ["o-test-l1", "o-test-l3", "o-test-m5"] {
                for g in &gens {
                    let ctx = BulkCtx { run: &run, ledger: target, ledger_view: &view, index_in_bulk: 0, len: 1 };
                    for opt in [false, true] {
                        let v = (g.gen)(&d, tc, &ctx, opt).expect("applicable");
                        let s = v.to_string();
                        assert!(!s.contains("own"), "{}: reserved name in {s}", g.name);
                        assert!(!s.contains(&l2), "{}: l2 in {s}", g.name);
                        let ledger = v.get("apply").or_else(|| v.as_object().and_then(|o| o.values().next())).and_then(|x| x.get("ledger")).and_then(Value::as_str);
                        assert_eq!(ledger, Some(target), "{s}");
                        for t in s.split('"').filter(|t| t.starts_with("t-")) {
                            let n: u64 = t[2..].split(':').next().unwrap().parse().unwrap();
                            assert!((POOL_LO..=POOL_HI).contains(&n), "{}: type {t} outside the pool in {s}", g.name);
                        }
                    }
                }
                let ctx = BulkCtx { run: &run, ledger: &l2, ledger_view: &view, index_in_bulk: 0, len: 1 };
                for g in &gens {
                    assert!((g.gen)(&d, tc, &ctx, false).is_none(), "{} returns None on l2", g.name);
                }
            }
        });
    }

    #[test]
    fn opt_ins_have_the_documented_shapes() {
        let (d, _, _) = testkit::driver();
        let run = d.s.run.clone();
        let view = json!({});
        let ctx = BulkCtx { run: &run, ledger: "o-test-l1", ledger_view: &view, index_in_bulk: 0, len: 1 };
        testkit::with_tc_n(50, |tc| {
            let a = gen_chart_add(&d, tc, &ctx, true).unwrap();
            assert_eq!(a["apply"]["skippableReasons"], json!(["ERROR_REASON_ACCOUNT_TYPE_ALREADY_EXISTS"]));
            let p = a["apply"]["action"]["addAccountType"]["accountType"]["persistence"].as_str().unwrap();
            assert!(["ACCOUNT_TYPE_NORMAL", "ACCOUNT_TYPE_EPHEMERAL", "ACCOUNT_TYPE_TRANSIENT"].contains(&p));
            let r = gen_chart_remove(&d, tc, &ctx, true).unwrap();
            assert_eq!(r["apply"]["skippableReasons"], json!(["ERROR_REASON_ACCOUNT_TYPE_NOT_FOUND"]));
            let e = gen_enforcement(&d, tc, &ctx, true).unwrap();
            assert!(e["apply"]["action"]["setDefaultEnforcementMode"]["enforcementMode"].is_string());
            assert_eq!(e["apply"]["skippableReasons"], json!(["ERROR_REASON_ACCOUNT_TYPE_NOT_FOUND"]), "a disallowed reason on the setter");
            for v in [gen_chart_add(&d, tc, &ctx, false).unwrap(), gen_chart_remove(&d, tc, &ctx, false).unwrap(), gen_enforcement(&d, tc, &ctx, false).unwrap()] {
                assert!(v.get("apply").and_then(|a| a.get("skippableReasons")).is_none(), "no opt-in without the flag: {v}");
            }
        });
    }

    #[test]
    fn chart_bulk_writes_chart_orders_on_a_non_l2_ledger() {
        let (d, _, fo) = testkit::driver();
        fo.set_view("ledger", json!({"types": []}));
        for _ in 0..10 {
            testkit::run_rule(&d, chart_bulk);
        }
        let begins = fo.calls("/write/begin");
        assert!(!begins.is_empty());
        for b in &begins {
            assert_eq!(b["family"], "chart");
            let reqs = b["requests"].as_array().unwrap();
            assert!((1..=3).contains(&reqs.len()));
            for r in reqs {
                assert!(!r.to_string().contains("o-test-l2"), "{r}");
            }
        }
        assert!(d.s.registry.snapshot().is_empty());
    }
}
