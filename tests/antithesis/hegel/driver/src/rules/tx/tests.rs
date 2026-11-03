use super::*;
use crate::testkit::{self, FakeReply};
use std::collections::BTreeSet;
use std::path::PathBuf;
use std::sync::{Arc, Condvar};

fn fixture(name: &str) -> Value {
    let p = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../protocol-fixtures/02").join(name);
    serde_json::from_str(&std::fs::read_to_string(&p).unwrap_or_else(|e| panic!("{}: {e}", p.display()))).unwrap()
}

const MAP_FIELDS: [&str; 3] = ["metadata", "accountMetadata", "values"];

/// Normalized key paths of a protojson value (map keys become `*`, array
/// indices disappear), with the JSON type of each leaf.
fn paths(v: &Value, prefix: &str, out: &mut BTreeSet<String>) {
    // Every MetadataValue position shares one schema, wherever it sits.
    let mv = ["/metadata/*", "/values/*"].iter().filter_map(|m| prefix.rfind(m).map(|i| i + m.len())).max();
    let norm;
    let prefix = match mv {
        Some(i) if !prefix.starts_with("MV") => {
            norm = format!("MV{}", &prefix[i..]);
            norm.as_str()
        }
        _ => prefix,
    };
    match v {
        Value::Object(m) => {
            let is_map = MAP_FIELDS.iter().any(|f| prefix.ends_with(&format!("/{f}")));
            for (k, x) in m {
                let seg = if is_map { "*" } else { k.as_str() };
                paths(x, &format!("{prefix}/{seg}"), out);
            }
            if m.is_empty() {
                out.insert(format!("{prefix}:{{}}"));
            }
        }
        Value::Array(a) => a.iter().for_each(|x| paths(x, prefix, out)),
        Value::String(_) => {
            out.insert(format!("{prefix}:s"));
        }
        Value::Bool(_) => {
            out.insert(format!("{prefix}:b"));
        }
        Value::Number(_) => {
            out.insert(format!("{prefix}:n"));
        }
        Value::Null => {
            out.insert(format!("{prefix}:null"));
        }
    }
}

/// The schema of the Go-produced request fixtures.
fn allowed() -> BTreeSet<String> {
    let mut s = BTreeSet::new();
    for f in ["request.create-full.json", "request.create-empty.json", "request.add-account-metadata.json"] {
        paths(&fixture(f), "", &mut s);
    }
    // A zero amount renders as an empty Uint256 (protojson omits zero limbs).
    s.insert("/apply/action/createTransaction/postings/amount:{}".into());
    s
}

fn check_schema(req: &Value, allowed: &BTreeSet<String>) {
    let mut got = BTreeSet::new();
    paths(req, "", &mut got);
    for p in &got {
        assert!(allowed.contains(p), "path {p} of {req} is not in the Go fixture schema");
    }
    for leg in req.pointer("/apply/action/createTransaction/postings").and_then(Value::as_array).into_iter().flatten() {
        for (k, limb) in leg["amount"].as_object().unwrap() {
            assert!(["v0", "v1", "v2", "v3"].contains(&k.as_str()), "{leg}");
            assert!(limb.as_str().unwrap().parse::<u64>().unwrap() > 0, "zero limbs are omitted: {leg}");
        }
    }
}

fn reserved_in(v: &Value, run: &str) -> Option<String> {
    match v {
        Value::String(s) => {
            let addr = s.split(':').skip(1).any(|seg| seg.starts_with("own-"));
            (addr || s.starts_with(&format!("o-{run}-own"))).then(|| s.clone())
        }
        Value::Object(m) => m.iter().find_map(|(k, x)| if k.starts_with("own-") { Some(k.clone()) } else { reserved_in(x, run) }),
        Value::Array(a) => a.iter().find_map(|x| reserved_in(x, run)),
        _ => None,
    }
}

/// A FakeOracle whose `seek` finds an entry usable by every table.
fn aimed() -> (Driver, Arc<testkit::FakeTransport>, Arc<testkit::FakeOracle>) {
    let (d, ft, fo) = testkit::driver();
    fo.set_view(
        "seek",
        json!({"found": true, "wrapped": false, "entry": {"reference": "o-test-ref999999", "id": "3", "address": "t-1:4", "asset": "COIN", "color": "B", "input": "340282366920938463463374607431768211456", "output": "3"}}),
    );
    (d, ft, fo)
}

#[test]
fn every_gen_emits_fixture_shaped_protojson_without_reserved_names() {
    let (d, _, _) = aimed();
    let allowed = allowed();
    let view = json!({"txCount": 0});
    for g in order_gens() {
        let seen = Mutex::new(0usize);
        testkit::with_tc_n(1000, |tc| {
            let ctx = BulkCtx { run: &d.s.run, ledger: "o-test-l1", ledger_view: &view, index_in_bulk: 0, len: 1 };
            for opt in [false, g.has_opt_in] {
                if let Some(req) = (g.gen)(&d, tc, &ctx, opt) {
                    check_schema(&req, &allowed);
                    assert_eq!(reserved_in(&req, &d.s.run), None, "{} produced a reserved name: {req}", g.name);
                    assert_eq!(ledger_of(&req).as_deref(), Some("o-test-l1"));
                    *seen.lock().unwrap() += 1;
                }
            }
        });
        assert!(*seen.lock().unwrap() > 0, "{} never produced an order", g.name);
    }
}

#[test]
fn drain_ephemeral_drains_the_exact_wide_balance() {
    let (d, _, _) = aimed();
    let view = json!({"txCount": 0});
    testkit::with_tc_n(5, |tc| {
        let ctx = BulkCtx { run: &d.s.run, ledger: "o-test-l2", ledger_view: &view, index_in_bulk: 0, len: 1 };
        let req = gen_drain_ephemeral(&d, tc, &ctx, false).unwrap();
        let p = &req["apply"]["action"]["createTransaction"];
        assert_eq!(p["force"], true);
        let leg = Leg::from_json(&p["postings"][0]).unwrap();
        assert_eq!(leg.source, "t-1:4");
        assert_eq!(leg.destination, "world");
        assert_eq!(leg.color, "B");
        assert_eq!(leg.amount, U256::pow2(128).checked_sub(&U256::from(3)).unwrap());
    });
}

#[test]
fn dup_ref_skip_rebinds_to_the_seeked_reference() {
    let (d, _, _) = aimed();
    let view = json!({"txCount": 0});
    testkit::with_tc_n(5, |tc| {
        let ctx = BulkCtx { run: &d.s.run, ledger: "o-test-l1", ledger_view: &view, index_in_bulk: 0, len: 1 };
        let req = gen_dup_ref_skip(&d, tc, &ctx, false).unwrap();
        assert_eq!(req["apply"]["action"]["createTransaction"]["reference"], "o-test-ref999999");
        assert!(has_opt_in(&req));
        let fresh = gen_skip_fresh(&d, tc, &ctx, false).unwrap();
        assert!(has_opt_in(&fresh));
        assert_ne!(fresh["apply"]["action"]["createTransaction"]["reference"], "o-test-ref999999");
    });
}

#[test]
fn gens_return_none_without_a_seek_hit() {
    let (d, _, _) = testkit::driver();
    let view = json!({"txCount": 0});
    testkit::with_tc_n(50, |tc| {
        let ctx = BulkCtx { run: &d.s.run, ledger: "o-test-l1", ledger_view: &view, index_in_bulk: 0, len: 1 };
        assert!(gen_dup_ref_skip(&d, tc, &ctx, false).is_none());
        assert!(gen_drain_ephemeral(&d, tc, &ctx, false).is_none());
    });
}

#[test]
fn taper() {
    for (n, applies) in [(0, true), (499, true), (1999, true), (3999, true), (4000, false), (100_000, false)] {
        let view = json!({"txCount": n});
        let ctx = BulkCtx { run: "test", ledger: "L", ledger_view: &view, index_in_bulk: 0, len: 1 };
        assert_eq!(taper_applies(&ctx), applies, "{n}");
        for g in order_gens() {
            assert_eq!((g.applies)(&ctx), applies, "{} at {n}", g.name);
        }
    }
    let view = json!({"txCount": 0});
    testkit::with_tc_n(20, |tc| assert!(taper_draw(tc, &BulkCtx { run: "t", ledger: "L", ledger_view: &view, index_in_bulk: 0, len: 1 })));
}

#[test]
fn colors_stay_within_the_geometric_bound() {
    testkit::with_tc_n(500, |tc| {
        let c = draw::color(tc);
        assert!(c.len() <= 2 && c.chars().all(|x| x.is_ascii_uppercase()), "{c}");
        assert!(c.len() < 2 || c.as_str() <= "BL", "depth ≤ 64: {c}");
    });
}

#[test]
fn conflict_body_always_differs() {
    let cases = vec![
        vec![Leg::new("world", "fees", 1u64, "COIN", "")],
        vec![Leg::new("world", "t-0:1", u64::MAX, "COIN", "A")],
        vec![Leg::new("world", "t-0:1", U256::MAX, "COIN", "")],
        vec![Leg::new("t-0:1", "users:0", 0u64, "USD/2", ""), Leg::new("world", "fees", U256::MAX, "COIN", "")],
    ];
    for orig in cases {
        for amount in [false, true] {
            let c = conflict_postings(&orig, amount);
            assert_ne!(c, orig, "{orig:?} amount={amount}");
            assert_eq!(c.len(), orig.len());
        }
    }
    let wrapped = conflict_postings(&[Leg::new("a:1", "b:1", U256::MAX, "COIN", "")], true);
    assert_eq!(wrapped[0].amount, U256::ZERO);
    let past = conflict_postings(&[Leg::new("a:1", "b:1", u64::MAX, "COIN", "")], true);
    assert_eq!(past[0].amount, U256::pow2(64));
    assert!(!conflict_postings(&[], false).is_empty());
}

#[test]
fn decimal_parsing() {
    assert_eq!(u256_from_decimal("0"), Some(U256::ZERO));
    assert_eq!(u256_from_decimal("18446744073709551616"), Some(U256::pow2(64)));
    assert_eq!(u256_from_decimal(&U256::MAX.to_decimal()), Some(U256::MAX));
    assert_eq!(u256_from_decimal("115792089237316195423570985008687907853269984665640564039457584007913129639936"), None);
    assert_eq!(u256_from_decimal(""), None);
    assert_eq!(u256_from_decimal("-1"), None);
}

#[test]
fn go_response_fixtures_parse() {
    let skipped = fixture("response.order-skipped.json");
    assert_eq!(skipped_facts(&skipped), Some(("TRANSACTION_REFERENCE_CONFLICT".into(), "o-1a2b-l2".into(), "o-1a2b-ref1".into(), 1)));
    let created = fixture("response.created-transaction.json");
    assert_eq!(skipped_facts(&created), None);
    let (d, _, _) = testkit::driver();
    let out = Outcome::from_json(&json!({"ok": true, "response": created}));
    record_refs(&d, "o-1a2b-l2", &out);
    let (id, legs) = d.s.ext::<RefBook>().get("o-1a2b-ref2").expect("recorded");
    assert_eq!(id, 2);
    assert_eq!(legs, vec![Leg::new("world", "t-0:1", 9u64, "COIN", "")]);
}

fn ok_seq(seq: u64) -> Outcome {
    Outcome::from_json(&json!({"ok": true, "response": {"logs": [{"sequence": seq.to_string()}]}}))
}

fn fail(reason: &str) -> Outcome {
    Outcome::from_json(&json!({"ok": false, "code": "FailedPrecondition", "reason": reason}))
}

fn messages(kind: &str, msg: &str) -> Vec<bool> {
    testkit::assertions().iter().filter(|a| a.kind == kind && a.message == msg).map(|a| a.cond).collect()
}

/// Holds all general units but `keep`.
fn hold_all_but(d: &Driver, keep: usize) -> crate::write::SlotGrant {
    d.s.slots.try_acquire_up_to(crate::write::GENERAL_SLOTS - keep).unwrap()
}

#[test]
fn race_one_ack_two_conflicts_is_no_violation() {
    let (d, ft, fo) = testkit::driver();
    let _held = hold_all_but(&d, 3);
    ft.set_default("bucket", "Apply", FakeReply::Outcome(fail("TRANSACTION_REFERENCE_CONFLICT")));
    let mut done = false;
    for _ in 0..200 {
        testkit::reset();
        let before = fo.calls("/write/begin").len();
        ft.script("bucket", "Apply", vec![FakeReply::Outcome(ok_seq(1))]);
        testkit::run_rule(&d, tx_reference_race);
        let begins = fo.calls("/write/begin");
        let racers = begins.len() - before;
        assert!((2..=3).contains(&racers), "{racers} racers with 3 units granted");
        let refs: BTreeSet<_> = begins[before..].iter().map(|b| b["requests"][0]["apply"]["action"]["createTransaction"]["reference"].to_string()).collect();
        assert_eq!(refs.len(), 1, "one shared reference");
        assert!(refs.iter().next().unwrap().starts_with("\"o-test-own-ref"));
        let keys: BTreeSet<_> = begins[before..].iter().map(|b| b["idempotencyKey"].to_string()).collect();
        assert_eq!(keys.len(), racers, "distinct keys");
        assert_eq!(messages("Always", "at most one concurrent same-reference create is acked"), vec![true]);
        assert_eq!(messages("Sometimes", "concurrent same-reference race produced a winner and a conflict loser"), vec![true]);
        if racers == 3 {
            done = true;
            break;
        }
    }
    assert!(done, "never drew 3 racers");
    assert_eq!(d.s.slots.in_use().0, crate::write::GENERAL_SLOTS - 3);
}

#[test]
fn race_two_acks_violate_the_always() {
    let (d, ft, _) = testkit::driver();
    testkit::reset();
    ft.set_default("bucket", "Apply", FakeReply::Outcome(fail("TRANSACTION_REFERENCE_CONFLICT")));
    ft.script("bucket", "Apply", vec![FakeReply::Outcome(ok_seq(1)), FakeReply::Outcome(ok_seq(2))]);
    testkit::run_rule(&d, tx_reference_race);
    assert_eq!(messages("Always", "at most one concurrent same-reference create is acked"), vec![false]);
}

#[test]
fn race_never_exceeds_the_grant() {
    let (d, ft, fo) = testkit::driver();
    ft.set_default("bucket", "Apply", FakeReply::Outcome(ok_seq(1)));
    let held = hold_all_but(&d, 2);
    for _ in 0..10 {
        let before = fo.calls("/write/begin").len();
        testkit::run_rule(&d, tx_reference_race);
        assert_eq!(fo.calls("/write/begin").len() - before, 2);
    }
    drop(held);
    let _held = hold_all_but(&d, 1);
    let before = fo.calls("/write/begin").len();
    testkit::run_rule(&d, tx_reference_race);
    assert_eq!(fo.calls("/write/begin").len(), before, "one unit is not a race");
    assert_eq!(d.s.slots.in_use().0, crate::write::GENERAL_SLOTS - 1, "the lone unit is released");
}

#[test]
fn drain_probe_happy_chain_asserts_the_old_literals() {
    let (d, ft, fo) = testkit::driver();
    testkit::reset();
    ft.script("bucket", "Apply", vec![FakeReply::Outcome(ok_seq(1)), FakeReply::Outcome(ok_seq(2)), FakeReply::Outcome(fail("INSUFFICIENT_FUNDS")), FakeReply::Outcome(ok_seq(3))]);
    testkit::run_rule(&d, tx_drain_probe);
    let begins = fo.calls("/write/begin");
    assert_eq!(begins.len(), 4);
    let ct = |i: usize| begins[i]["requests"][0]["apply"]["action"]["createTransaction"].clone();
    assert_eq!(begins[0]["requests"][0]["apply"]["ledger"], "o-test-l2");
    assert_eq!(ct(0)["force"], true);
    assert!(ct(0)["reference"].as_str().unwrap().starts_with("o-test-own-ref"));
    assert_eq!(ct(1)["postings"][0]["amount"], ct(0)["postings"][0]["amount"]);
    assert!(ct(1).get("force").is_none());
    assert_eq!(ct(2)["postings"][0]["amount"], json!({"v0": "1"}));
    assert!(ct(2).get("force").is_none());
    assert_eq!(ct(3)["force"], true);
    assert!(ct(0)["postings"][0]["destination"].as_str().unwrap().starts_with("t-0:own-drain"));
    assert_eq!(messages("AlwaysOrUnreachable", "overdraft should return INSUFFICIENT_FUNDS"), vec![true]);
    assert_eq!(messages("Reachable", "insufficient funds path exercised").len(), 1);
    assert!(!testkit::assertions().iter().any(|a| a.kind == "Unreachable"), "{:?}", testkit::assertions());
}

#[test]
fn drain_probe_wrong_outcomes_fire_the_unreachables() {
    let (d, ft, _) = testkit::driver();
    testkit::reset();
    ft.script("bucket", "Apply", vec![FakeReply::Outcome(ok_seq(1)), FakeReply::Outcome(ok_seq(2)), FakeReply::Outcome(ok_seq(3))]);
    testkit::run_rule(&d, tx_drain_probe);
    assert!(testkit::fired("overdraft without Force should fail"));
    testkit::reset();
    ft.script("bucket", "Apply", vec![FakeReply::Outcome(fail("INSUFFICIENT_FUNDS"))]);
    testkit::run_rule(&d, tx_drain_probe);
    assert!(testkit::fired("funding should not fail"));
    testkit::reset();
    ft.script("bucket", "Apply", vec![FakeReply::Outcome(fail("LEDGER_NOT_FOUND"))]);
    testkit::run_rule(&d, tx_drain_probe);
    assert!(!testkit::fired("funding should not fail"), "inconclusive reasons assert nothing");
}

#[test]
fn drain_probe_stops_after_a_handoff() {
    let (d, ft, fo) = testkit::driver();
    testkit::reset();
    d.s.resolvers.set_paused(true);
    ft.set_default("bucket", "Apply", FakeReply::Outcome(Outcome::unknown("timeout")));
    testkit::run_rule(&d, tx_drain_probe);
    assert_eq!(fo.calls("/write/begin").len(), 1, "no step after the handoff");
    assert!(testkit::fired("a tracked write is handed off to a resolver"));
    assert!(!testkit::fired("funding should not fail"));
    d.s.resolvers.set_paused(false);
}

#[test]
fn transient_shapes_never_zero() {
    let (d, ft, fo) = testkit::driver();
    ft.set_default("bucket", "Apply", FakeReply::Outcome(ok_seq(1)));
    let mut shapes = BTreeSet::new();
    for _ in 0..40 {
        testkit::run_rule(&d, tx_transient);
    }
    for b in fo.calls("/write/begin") {
        let reqs = b["requests"].as_array().unwrap();
        shapes.insert(reqs.len());
        let fund = Leg::from_json(&reqs[0]["apply"]["action"]["createTransaction"]["postings"][0]).unwrap();
        assert!(!fund.amount.is_zero());
        assert_eq!(fund.source, "world");
        assert!(fund.destination.starts_with("t-2:own-tr"));
        assert_eq!(reqs[0]["apply"]["ledger"], "o-test-l2");
        if reqs.len() == 2 {
            let drain = Leg::from_json(&reqs[1]["apply"]["action"]["createTransaction"]["postings"][0]).unwrap();
            assert_eq!((drain.source.as_str(), drain.amount, &drain.color), (fund.destination.as_str(), fund.amount, &fund.color));
            assert!(drain.destination.starts_with("t-0:own-trs"));
        }
    }
    assert_eq!(shapes, BTreeSet::from([1, 2]));
}

#[test]
fn transient_unbalanced_success_is_unreachable() {
    let (d, ft, fo) = testkit::driver();
    ft.set_default("bucket", "Apply", FakeReply::Outcome(ok_seq(1)));
    for _ in 0..40 {
        testkit::reset();
        let before = fo.calls("/write/begin").len();
        testkit::run_rule(&d, tx_transient);
        let b = &fo.calls("/write/begin")[before];
        if b["requests"].as_array().unwrap().len() == 1 {
            assert!(testkit::fired("unbalanced transient batch should fail"));
            return;
        }
        assert!(testkit::fired("balanced transient batch succeeded"));
    }
    panic!("never drew the unbalanced shape");
}

#[test]
fn reference_reuse_checks_skip_facts_exactly() {
    let (d, ft, fo) = aimed();
    d.s.ext::<RefBook>().record("o-test-ref999999", 3, vec![Leg::new("world", "t-0:1", 4u64, "COIN", "")]);
    let skipped = |id: &str| {
        json!({"ok": true, "response": {"logs": [{"sequence": "9", "payload": {"apply": {"ledgerName": "o-test-l2", "log": {"data": {"orderSkipped": {"details": {"transactionReferenceConflict": {"ledger": "o-test-l2", "reference": "o-test-ref999999", "existingTransactionId": id}}}}}}}}]}})
    };
    let (mut skip_seen, mut plain_seen) = (false, false);
    for _ in 0..60 {
        for good in [true, false] {
            testkit::reset();
            let before = fo.calls("/write/begin").len();
            ft.script("bucket", "Apply", vec![FakeReply::Outcome(Outcome::from_json(&skipped(if good { "3" } else { "4" })))]);
            testkit::run_rule(&d, tx_reference_reuse);
            let b = &fo.calls("/write/begin")[before];
            let req = &b["requests"][0];
            let ct = &req["apply"]["action"]["createTransaction"];
            assert_eq!(ct["reference"], "o-test-ref999999");
            assert_ne!(ct["postings"], json!([{"source": "world", "destination": "t-0:1", "amount": {"v0": "4"}, "asset": "COIN"}]));
            if has_opt_in(req) {
                skip_seen = true;
                assert_eq!(messages("AlwaysOrUnreachable", "skip-tolerant duplicate reference must land as an OrderSkipped log"), vec![true]);
                assert_eq!(messages("AlwaysOrUnreachable", "OrderSkipped.context.existingTransactionId must point to the first tx"), vec![good]);
                assert_eq!(messages("AlwaysOrUnreachable", "OrderSkipped.context.reference must match the requested reference"), vec![true]);
                assert_eq!(messages("AlwaysOrUnreachable", "OrderSkipped.context.ledger must match the target ledger"), vec![true]);
                assert!(testkit::fired("reference-conflict skip path exercised"));
            } else {
                plain_seen = true;
                assert!(testkit::fired("duplicate reference should be rejected"), "a success without skip is a bug");
            }
        }
        if skip_seen && plain_seen {
            return;
        }
    }
    panic!("both variants expected");
}

#[test]
fn reference_reuse_conflict_is_the_expected_rejection() {
    let (d, ft, _) = aimed();
    ft.set_default("bucket", "Apply", FakeReply::Outcome(fail("TRANSACTION_REFERENCE_CONFLICT")));
    for _ in 0..30 {
        testkit::reset();
        testkit::run_rule(&d, tx_reference_reuse);
        if testkit::fired("reference conflict path exercised") {
            assert_eq!(messages("AlwaysOrUnreachable", "duplicate reference should return TRANSACTION_REFERENCE_CONFLICT"), vec![true]);
            return;
        }
        assert!(testkit::fired("skip-tolerant duplicate reference must not return a business error"));
    }
    panic!("never drew the plain variant");
}

#[test]
fn boundary_shapes() {
    let (d, ft, fo) = testkit::driver();
    ft.set_default("bucket", "Apply", FakeReply::Outcome(fail("INSUFFICIENT_FUNDS")));
    let mut amounts = BTreeSet::new();
    for _ in 0..60 {
        testkit::run_rule(&d, tx_boundary);
    }
    for b in fo.calls("/write/begin") {
        let ct = &b["requests"][0]["apply"]["action"]["createTransaction"];
        let legs: Vec<Leg> = ct["postings"].as_array().unwrap().iter().map(|p| Leg::from_json(p).unwrap()).collect();
        if legs.len() == 2 {
            assert_eq!(legs[0].amount, U256::pow2(255));
            assert_eq!(legs[0], legs[1]);
            assert!(legs[0].destination.starts_with("t-0:own-ovf"));
            assert_eq!(ct["force"], true);
        } else {
            assert!(legs[0].source.starts_with("t-0:own-void"));
            assert!(ct.get("force").is_none());
        }
        amounts.insert(legs[0].amount);
    }
    assert!(amounts.len() >= 3, "{amounts:?}");
}

#[test]
fn doomed_bulk_ends_with_a_fresh_void_source() {
    let (d, ft, fo) = testkit::driver();
    ft.set_default("bucket", "Apply", FakeReply::Outcome(fail("INSUFFICIENT_FUNDS")));
    let mut voids = BTreeSet::new();
    let mut doomed = 0;
    for _ in 0..200 {
        testkit::run_rule(&d, tx_local_bulk);
    }
    for b in fo.calls("/write/begin") {
        let reqs = b["requests"].as_array().unwrap();
        assert!(reqs.len() <= 7);
        if b["hint"]["doomed"] == true {
            doomed += 1;
            let last = Leg::from_json(&reqs.last().unwrap()["apply"]["action"]["createTransaction"]["postings"][0]).unwrap();
            assert!(last.source.starts_with("t-0:own-void"), "{last:?}");
            assert!(!last.amount.is_zero());
            assert!(reqs.last().unwrap()["apply"]["action"]["createTransaction"].get("force").is_none());
            assert!(voids.insert(last.source), "fresh void source");
        } else {
            for r in reqs {
                assert_eq!(reserved_in(r, &d.s.run), None, "{r}");
            }
        }
    }
    assert!(doomed > 0, "no doomed bulk in 200 runs");
}

#[test]
fn trailing_successor_follows_a_final_opt_in() {
    let (d, ft, fo) = aimed();
    ft.set_default("bucket", "Apply", FakeReply::Outcome(ok_seq(1)));
    for _ in 0..300 {
        testkit::run_rule(&d, tx_local_bulk);
    }
    let mut trailing = 0;
    for b in fo.calls("/write/begin") {
        let reqs = b["requests"].as_array().unwrap();
        for w in reqs.windows(2) {
            if w[1].pointer("/apply/action/addMetadata").is_some() {
                assert!(has_opt_in(&w[0]), "a trailing metadata write follows an opt-in");
                assert_eq!(ledger_of(&w[0]), ledger_of(&w[1]));
                trailing += 1;
            }
        }
    }
    assert!(trailing > 0);
}

#[test]
fn stress_submits_a_job_and_returns_without_sending() {
    let (d, ft, _) = testkit::driver();
    testkit::reset();
    let gate = Arc::new((Mutex::new(false), Condvar::new()));
    for _ in 0..crate::jobs::JOB_THREADS {
        let gate = gate.clone();
        d.submit_job(Job {
            name: "gate",
            blocks_quiet: false,
            run: Box::new(move |_| {
                let (m, cv) = &*gate;
                let mut open = m.lock().unwrap();
                while !*open {
                    open = cv.wait(open).unwrap();
                }
            }),
        });
    }
    testkit::wait_until(Duration::from_secs(5), || d.s.jobs.len() == crate::jobs::JOB_THREADS);
    testkit::run_rule(&d, tx_stress);
    assert!(ft.calls_of("bucket", "Apply").is_empty(), "the rule itself sends nothing");
    assert_eq!(d.s.jobs.len(), crate::jobs::JOB_THREADS + 1);
    ft.set_default("bucket", "Apply", FakeReply::Outcome(fail("EXECUTION_PLAN_TOO_LARGE")));
    {
        let (m, cv) = &*gate;
        *m.lock().unwrap() = true;
        cv.notify_all();
    }
    testkit::wait_until(Duration::from_secs(10), || !ft.calls_of("bucket", "Apply").is_empty() && d.s.jobs.len() == 0);
    let call = &ft.calls_of("bucket", "Apply")[0];
    let batch = &call["request"]["unsigned"];
    assert!(batch["idempotencyKey"].as_str().unwrap().starts_with("r-test-k"));
    let reqs = batch["requests"].as_array().unwrap();
    assert!((1..=5000).contains(&reqs.len()));
    let refs: BTreeSet<_> = reqs.iter().map(|r| r["apply"]["action"]["createTransaction"]["reference"].to_string()).collect();
    assert_eq!(refs.len(), reqs.len(), "unique references");
    assert!(reqs.iter().all(|r| r["apply"]["ledger"] == "r-stress-test"));
    assert_eq!(messages("Sometimes", "should be able to create bulk transactions"), vec![true]);
}

#[test]
fn stress_job_checks_one_log_per_request() {
    let (d, ft, _) = testkit::driver();
    testkit::reset();
    ft.set_default("bucket", "Apply", FakeReply::Outcome(ok_seq(1)));
    let reqs = stress_requests("r-stress-test", "k-ref", &[Leg::new("world", "t-0:1", 1u64, "COIN", "")], 3);
    stress_job(&d, UntrackedSend::new(reqs, Some("k".into())), 3, "k");
    assert_eq!(messages("AlwaysOrUnreachable", "bulk Apply should return one log per request"), vec![false]);
    assert_eq!(messages("Sometimes", "should be able to create bulk transactions"), vec![true]);
}

#[test]
fn setup_hook_creates_the_stress_ledger() {
    let (d, ft, _) = testkit::driver();
    ft.script("bucket", "Apply", vec![FakeReply::Outcome(Outcome::unknown("timeout")), FakeReply::Outcome(fail("LEDGER_ALREADY_EXISTS"))]);
    tx_setup(&d);
    let calls = ft.calls_of("bucket", "Apply");
    assert!(!calls.is_empty());
    assert_eq!(calls[0]["request"]["unsigned"]["requests"][0]["createLedger"]["name"], "r-stress-test");
    assert!(calls[0]["request"]["unsigned"]["idempotencyKey"].as_str().unwrap().starts_with("r-test-k"));
}

#[test]
fn bulk_rule_spans_ledgers_and_records_l2_refs() {
    let (d, ft, fo) = testkit::driver();
    ft.set_default("bucket", "Apply", FakeReply::Outcome(ok_seq(1)));
    for _ in 0..200 {
        testkit::run_rule(&d, tx_bulk);
    }
    let begins = fo.calls("/write/begin");
    assert!(!begins.is_empty());
    let multi = begins.iter().filter(|b| b["hint"]["ledgers"].as_array().unwrap().len() > 1).count();
    assert!(multi > 0, "no multi-ledger bulk in 200 runs");
    for b in &begins {
        assert_eq!(b["family"], "tx");
        let ls: BTreeSet<_> = b["hint"]["ledgers"].as_array().unwrap().iter().map(|x| x.to_string()).collect();
        assert_eq!(ls.len(), b["hint"]["ledgers"].as_array().unwrap().len(), "distinct ledgers");
    }
}
