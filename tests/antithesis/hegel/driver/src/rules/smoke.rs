//! Smoke rules: the smallest tracked write and the smallest validated read,
//! so task 00d can prove the protocol end to end. Task 99 deletes them.
use crate::clients::CallOpts;
use crate::draw;
use crate::machine::{rule, Driver};
use crate::read::{LedgerSel, ViewReq};
use crate::wire::{create_tx_request, Posting};
use crate::write::WritePlan;
use hegel::stateful::ConcurrentRule;
use hegel::TestCase;
use serde_json::{json, Value};

pub fn rules() -> Vec<ConcurrentRule<Driver>> {
    vec![rule("smoke_tx", 1.0, smoke_tx), rule("smoke_get_account", 1.0, smoke_get_account)]
}

/// One CreateTransaction `world → users:<0..31>`, `1..=1000 COIN`, on a
/// `ledgerPick`-drawn tracked ledger, through `dispatch`.
pub fn smoke_tx(d: &Driver, tc: TestCase) {
    d.guarded("smoke_tx", &tc, |d, tc| {
        if !d.s.oracle_ok() {
            return None;
        }
        let pick = draw::ledger_pick(tc);
        let dest = draw::users_address(tc);
        let amount = draw::int_in(tc, 1, 1000);
        let views = d.view(&[ViewReq::ledger(LedgerSel::Pick(pick))])?;
        let ledger = views.first()?.get("ledger").and_then(Value::as_str)?.to_string();
        let req = create_tx_request(&ledger, &[Posting::new("world", &dest, amount, "COIN")], None);
        let mut plan = WritePlan::drawn("smoke", vec![req], d, tc);
        plan.hint = json!({"gen": "smoke"});
        let unit = d.s.slots.try_acquire_up_to(1)?.take_one()?;
        let _ = d.dispatch(plan, unit);
        Some(())
    });
}

/// A linearizable GetAccount of `users:<0..31>` on a `ledgerPick`-drawn
/// tracked ledger, validated by the oracle's `Smoke` read kind.
pub fn smoke_get_account(d: &Driver, tc: TestCase) {
    d.guarded("smoke_get_account", &tc, |d, tc| {
        if !d.s.oracle_ok() {
            return None;
        }
        let pick = draw::ledger_pick(tc);
        let route = draw::route(tc, &d.s);
        let rt = d.read_begin("Smoke", &[ViewReq::ledgers(), ViewReq::ledger(LedgerSel::Pick(pick))], json!({}))?;
        let address = draw::users_address(tc);
        let ledger = rt.views().get(1)?.get("ledger").and_then(Value::as_str)?.to_string();
        let req = json!({"ledger": ledger, "address": address});
        let opts = CallOpts::linearizable();
        let (_, out) = d.call(&route, "bucket", "GetAccount", req.clone(), &opts);
        rt.end(&req, &out, &route, &opts, json!({}), json!({}));
        Some(())
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testkit;

    #[test]
    fn smoke_rules_run_the_protocol() {
        let (d, ft, fo) = testkit::driver();
        fo.set_live(vec!["o-test-l1".into(), "o-test-l2".into()]);
        testkit::run_rule(&d, smoke_tx);
        let begins = fo.calls("/write/begin");
        assert!(!begins.is_empty());
        let r = &begins[0]["requests"][0]["apply"];
        assert!(r["ledger"].as_str().unwrap().starts_with("o-test-l"));
        assert_eq!(r["action"]["createTransaction"]["postings"][0]["source"], "world");
        assert!(!fo.calls("/write/end").is_empty());
        assert!(ft.calls_of("bucket", "Apply").len() >= begins.len());

        testkit::run_rule(&d, smoke_get_account);
        let ends = fo.calls("/read/end");
        assert!(!ends.is_empty());
        assert_eq!(ends[0]["kind"], "Smoke");
        assert!(ends[0]["request"]["address"].as_str().unwrap().starts_with("users:"));
        assert_eq!(ends[0]["route"]["consistency"], "linearizable");
        assert!(d.s.registry.snapshot().is_empty());
    }
}
