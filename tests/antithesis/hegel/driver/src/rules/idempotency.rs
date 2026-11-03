//! Task 03 — idempotency: `idem_replay` (the `Replay` read kind: re-send a
//! flagged original byte for byte under its key; the oracle validates) and
//! `idem_conflict` (a guaranteed-different body under a flagged original's
//! key, `Role::Conflict`).
use crate::clients::CallOpts;
use crate::draw;
use crate::machine::{rule, Driver};
use crate::read::ViewReq;
use crate::wire::{self, Posting, U256};
use crate::write::{Role, WritePlan, WriteResult, ATTEMPT_TIMEOUT, ATTEMPT_TIMEOUT_LARGE, MAX_TRACKED_REQUESTS};
use hegel::stateful::ConcurrentRule;
use hegel::TestCase;
use serde_json::{json, Value};

pub const FAMILY: &str = "idempotency";
const CONFLICT: &str = "IDEMPOTENCY_KEY_CONFLICT";

pub fn rules() -> Vec<ConcurrentRule<Driver>> {
    vec![rule("idem_replay", 2.5, idem_replay), rule("idem_conflict", 1.5, idem_conflict)]
}

// ---------------------------------------------------------------- idem_replay

/// `idem_replay` (weight 2.5): `read_begin("Replay", [replayable])`, draw an
/// entry, fetch its original request bytes (`flagged_plan`; evicted →
/// abandon), resend them under the key (signed the same way) through a
/// drawn route, single attempt, and end the read with `params.wid`.
pub fn idem_replay(d: &Driver, tc: TestCase) {
    d.guarded("idem_replay", &tc, |d, tc| {
        if !d.s.oracle_ok() {
            return None;
        }
        let route = draw::route(tc, &d.s);
        let rt = d.read_begin("Replay", &[ViewReq::replayable()], json!({}))?;
        let entries = rt.views().first().and_then(|v| v.get("entries")).and_then(Value::as_array).cloned().unwrap_or_default();
        if entries.is_empty() {
            rt.abandon();
            return None;
        }
        let entry = entries[draw::pick(tc, entries.len())].clone();
        let wid = wire::str_field(&entry, "wid").to_string();
        let Some((key, requests, sign)) = d.s.flagged_plan(&wid) else {
            rt.abandon();
            return None;
        };
        let req = wire::apply_request(&requests, Some(&key));
        let timeout = if requests.len() > 16 { ATTEMPT_TIMEOUT_LARGE } else { ATTEMPT_TIMEOUT };
        let opts = CallOpts { timeout, max_items: None, consistency: None, sign };
        let (_, out) = d.call(&route, "bucket", "Apply", req.clone(), &opts);
        rt.end(&req, &out, &route, &opts, json!({"status": entry.get("status"), "reason": entry.get("reason")}), json!({"wid": wid}));
        Some(())
    });
}

// ---------------------------------------------------------------- conflict bodies

/// How `idem_conflict` changes the original body.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConflictVariant {
    /// The fixed regression branch (`MD/parallel_driver_idempotency_conflict`):
    /// a single create gets `world → users:0 100 USD/2` or
    /// `world → users:1 999 EUR/2`, whichever differs.
    Regression,
    /// Posting 0 of the first create: amount + 1 (wrapping).
    AmountOnly,
    /// Posting 0 of the first create: destination `fees`; without a create,
    /// a trailing `world → fees` create (or the last request dropped).
    Fees,
}

fn create_of(r: &Value) -> Option<&Value> {
    r.pointer("/apply/action/createTransaction")
}

fn first_create_with_postings(reqs: &[Value]) -> Option<usize> {
    reqs.iter().position(|r| create_of(r).and_then(|c| c.get("postings")).and_then(Value::as_array).is_some_and(|p| !p.is_empty()))
}

/// The ledger a request names, if any.
fn ledger_of(r: &Value) -> Option<String> {
    let (_, inner) = r.as_object()?.iter().next()?;
    inner.get("ledger").or_else(|| inner.get("name")).and_then(Value::as_str).map(str::to_string)
}

fn regression(orig: &[Value]) -> Option<Vec<Value>> {
    if orig.len() != 1 {
        return None;
    }
    let ct = create_of(&orig[0])?;
    let have: Vec<Option<Posting>> = ct.get("postings").and_then(Value::as_array).map(|a| a.iter().map(Posting::from_json).collect()).unwrap_or_default();
    let a = vec![Posting::new("world", "users:0", 100u64, "USD/2")];
    let b = vec![Posting::new("world", "users:1", 999u64, "EUR/2")];
    let same = |want: &[Posting]| have.len() == want.len() && have.iter().zip(want).all(|(h, w)| h.as_ref() == Some(w));
    let chosen = if same(&a) { b } else { a };
    let mut out = orig.to_vec();
    out[0]["apply"]["action"]["createTransaction"]["postings"] = json!(chosen.iter().map(Posting::to_json).collect::<Vec<_>>());
    Some(out)
}

fn amount_only(orig: &[Value]) -> Option<Vec<Value>> {
    let i = first_create_with_postings(orig)?;
    let mut out = orig.to_vec();
    let p = &mut out[i]["apply"]["action"]["createTransaction"]["postings"][0];
    let cur = U256::from_json(p.get("amount").unwrap_or(&json!({})))?;
    // Wrapping + 1: U256::MAX becomes 0.
    let next = cur.checked_add(&U256::from(1)).unwrap_or(U256::ZERO);
    p["amount"] = next.to_json();
    Some(out)
}

fn fees(orig: &[Value]) -> Option<Vec<Value>> {
    let mut out = orig.to_vec();
    if let Some(i) = first_create_with_postings(orig) {
        let p = &mut out[i]["apply"]["action"]["createTransaction"]["postings"][0];
        let src = wire::str_field(p, "source").to_string();
        let dst = wire::str_field(p, "destination").to_string();
        p["destination"] = json!(if src == "fees" || dst == "fees" { "fees:conflict" } else { "fees" });
        return Some(out);
    }
    match orig.iter().find_map(ledger_of) {
        Some(ledger) if orig.len() < MAX_TRACKED_REQUESTS => {
            let mut req = wire::create_tx_request(&ledger, &[Posting::new("world", "fees", 1u64, "COIN")], None);
            req["apply"]["action"]["createTransaction"]["force"] = json!(true);
            out.push(req);
            Some(out)
        }
        _ if orig.len() > 1 => {
            out.pop();
            Some(out)
        }
        _ => None,
    }
}

/// A body guaranteed to differ from `orig` (inventory domain rule 5), or
/// `None` when nothing applies. Inapplicable variants fall through
/// Regression → AmountOnly → Fees.
pub fn conflict_body(orig: &[Value], v: ConflictVariant) -> Option<Vec<Value>> {
    let out = match v {
        ConflictVariant::Regression => regression(orig).or_else(|| amount_only(orig)).or_else(|| fees(orig)),
        ConflictVariant::AmountOnly => amount_only(orig).or_else(|| fees(orig)),
        ConflictVariant::Fees => fees(orig),
    }?;
    // Checked, never assumed.
    (out.as_slice() != orig).then_some(out)
}

// ---------------------------------------------------------------- idem_conflict

/// `idem_conflict` (weight 1.5): a `replayable` entry (flagged commits and
/// frozen failures), a guaranteed-different body under its key, dispatched
/// with `Role::Conflict`. When the original committed, the verbatim
/// `MD/parallel_driver_idempotency_conflict` properties.
pub fn idem_conflict(d: &Driver, tc: TestCase) {
    d.guarded("idem_conflict", &tc, |d, tc| {
        if !d.s.oracle_ok() {
            return None;
        }
        let variant = *draw::one_of(tc, &[ConflictVariant::Regression, ConflictVariant::AmountOnly, ConflictVariant::Fees]);
        let mut plan = WritePlan::drawn(FAMILY, vec![], d, tc);
        let views = d.view(&[ViewReq::replayable()])?;
        let entries = views.first()?.get("entries").and_then(Value::as_array).cloned().unwrap_or_default();
        if entries.is_empty() {
            return None;
        }
        let entry = entries[draw::pick(tc, entries.len())].clone();
        let wid = wire::str_field(&entry, "wid").to_string();
        let (key, requests, sign) = d.s.flagged_plan(&wid)?;
        let body = conflict_body(&requests, variant)?;
        plan.requests = body;
        plan.track_key = true;
        plan.role = Role::Conflict { of: wid.clone() };
        plan.sign = sign;
        plan.hint = json!({"gen": "idem_conflict", "variant": format!("{variant:?}"), "originalStatus": entry.get("status")});
        let unit = d.s.slots.try_acquire_up_to(1)?.take_one()?;
        let res = d.dispatch(plan, unit);
        if wire::str_field(&entry, "status") == "committed" {
            judge_conflict(&res, &json!({"of": wid, "idempotencyKey": key, "variant": format!("{variant:?}")}));
        }
        Some(())
    });
}

/// The verbatim conflict properties for an original that committed.
pub fn judge_conflict(res: &WriteResult, details: &Value) {
    let Some(out) = res.outcome() else { return };
    if out.is_ok() {
        check_unreachable!("reusing idempotency key with different payload should fail", details);
        return;
    }
    check_always_or_unreachable!(
        out.reason == CONFLICT,
        "mismatched idempotency key should return IDEMPOTENCY_KEY_CONFLICT",
        json!({"details": details, "outcome": out.summary()})
    );
    check_reachable!("idempotency conflict path exercised", details);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clients::Outcome;
    use crate::testkit::{self, FakeReply};

    fn create(ledger: &str, ps: &[Posting]) -> Value {
        wire::create_tx_request(ledger, ps, None)
    }

    fn drawn_amount(tc: &TestCase) -> U256 {
        match draw::int_in(tc, 0, 4) {
            0 => U256::MAX,
            1 => U256::pow2(255),
            2 => U256::from(u64::MAX),
            3 => U256::ZERO,
            _ => U256::from(draw::amount(tc)),
        }
    }

    fn drawn_original(tc: &TestCase) -> Vec<Value> {
        let n = if draw::chance(tc, 0.1) { MAX_TRACKED_REQUESTS } else { draw::int_in(tc, 1, 4) as usize };
        (0..n)
            .map(|_| match draw::int_in(tc, 0, 4) {
                0 => json!({"apply": {"ledger": "o-t-l1", "action": {"addMetadata": {"target": {"account": {"addr": "t-1:2"}}, "metadata": {"k0": {"stringValue": "v"}}}}}}),
                1 => json!({"saveLedgerMetadata": {"ledger": "o-t-l2", "metadata": {"k1": {"boolValue": true}}}}),
                2 => json!({"apply": {"ledger": "o-t-l1", "action": {"revertTransaction": {"transactionId": "3"}}}}),
                _ => {
                    let k = draw::int_in(tc, 1, 3);
                    let ps: Vec<Posting> = (0..k)
                        .map(|_| {
                            let pick = |tc: &TestCase| draw::one_of(tc, &["world", "users:0", "users:1", "fees", "t-0:1"]).to_string();
                            Posting::new(&pick(tc), &pick(tc), drawn_amount(tc), draw::one_of(tc, &["USD/2", "EUR/2", "COIN"]))
                        })
                        .collect();
                    create("o-t-l1", &ps)
                }
            })
            .collect()
    }

    #[test]
    fn conflict_bodies_always_differ_from_the_original() {
        testkit::with_tc_n(400, |tc| {
            let orig = drawn_original(tc);
            for v in [ConflictVariant::Regression, ConflictVariant::AmountOnly, ConflictVariant::Fees] {
                let body = conflict_body(&orig, v).expect("a conflict body for every drawn original");
                assert_ne!(body, orig, "{v:?}");
                assert!(!body.is_empty() && body.len() <= MAX_TRACKED_REQUESTS);
            }
        });
    }

    #[test]
    fn conflict_variants_shapes() {
        // Regression: whichever fixed body differs.
        let a = create("L", &[Posting::new("world", "users:0", 100u64, "USD/2")]);
        let body = conflict_body(&[a.clone()], ConflictVariant::Regression).unwrap();
        assert_eq!(body[0]["apply"]["action"]["createTransaction"]["postings"][0]["destination"], "users:1");
        let other = create("L", &[Posting::new("world", "t-0:1", 5u64, "COIN")]);
        let body = conflict_body(&[other], ConflictVariant::Regression).unwrap();
        assert_eq!(body[0]["apply"]["action"]["createTransaction"]["postings"][0]["destination"], "users:0");
        // Amount-only wraps at the maximum.
        let max = create("L", &[Posting::new("world", "users:0", U256::MAX, "USD/2")]);
        let body = conflict_body(&[max], ConflictVariant::AmountOnly).unwrap();
        assert_eq!(body[0]["apply"]["action"]["createTransaction"]["postings"][0]["amount"], json!({}));
        let body = conflict_body(&[a.clone()], ConflictVariant::AmountOnly).unwrap();
        assert_eq!(body[0]["apply"]["action"]["createTransaction"]["postings"][0]["amount"], json!({"v0": "101"}));
        // Fees on a create, and a trailing create without one.
        let body = conflict_body(&[a], ConflictVariant::Fees).unwrap();
        assert_eq!(body[0]["apply"]["action"]["createTransaction"]["postings"][0]["destination"], "fees");
        let meta = json!({"saveLedgerMetadata": {"ledger": "L", "metadata": {}}});
        let body = conflict_body(&[meta.clone()], ConflictVariant::Regression).unwrap();
        assert_eq!(body.len(), 2);
        assert_eq!(body[1]["apply"]["ledger"], "L");
        // A full bulk without creates drops its last request.
        let full = vec![meta; MAX_TRACKED_REQUESTS];
        assert_eq!(conflict_body(&full, ConflictVariant::Fees).unwrap().len(), MAX_TRACKED_REQUESTS - 1);
    }

    fn flag_original(d: &Driver, reqs: Vec<Value>) -> String {
        let mut p = WritePlan::new("test", reqs);
        p.track_key = true;
        let t = d.begin_write(p, d.s.slots.try_acquire_up_to(1).unwrap().take_one().unwrap()).unwrap();
        let wid = t.wid().to_string();
        let _ = t.run(std::time::Duration::from_secs(1));
        wid
    }

    #[test]
    fn idem_replay_abandons_when_the_plan_was_evicted() {
        let (d, ft, fo) = testkit::driver();
        fo.set_view("replayable", json!({"entries": [{"wid": "w999", "key": "o-test-k1", "status": "committed", "reason": "", "stage": ""}]}));
        testkit::run_rule(&d, idem_replay);
        assert_eq!(fo.calls("/read/begin").len(), 1);
        assert_eq!(fo.calls("/read/abandon").len(), 1);
        assert!(fo.calls("/read/end").is_empty());
        assert!(ft.calls_of("bucket", "Apply").is_empty());
        assert!(d.s.registry.snapshot().is_empty());
    }

    #[test]
    fn idem_replay_resends_the_original_bytes() {
        let (d, ft, fo) = testkit::driver();
        let reqs = vec![create("o-test-l1", &[Posting::new("world", "users:3", 9u64, "COIN")])];
        let wid = flag_original(&d, reqs.clone());
        let (key, _, _) = d.s.flagged_plan(&wid).unwrap();
        fo.set_view("replayable", json!({"entries": [{"wid": wid, "key": key, "status": "committed", "reason": "", "stage": ""}]}));
        testkit::run_rule(&d, idem_replay);
        let ends = fo.calls("/read/end");
        assert_eq!(ends.len(), 1);
        assert_eq!(ends[0]["kind"], "Replay");
        assert_eq!(ends[0]["params"]["wid"], json!(wid));
        assert_eq!(ends[0]["request"], wire::apply_request(&reqs, Some(&key)));
        let applies = ft.calls_of("bucket", "Apply");
        assert_eq!(applies.last().unwrap()["request"], wire::apply_request(&reqs, Some(&key)));
    }

    #[test]
    fn idem_conflict_dispatches_a_conflict_and_judges_it() {
        let (d, ft, fo) = testkit::driver();
        let reqs = vec![create("o-test-l1", &[Posting::new("world", "users:3", 9u64, "COIN")])];
        let wid = flag_original(&d, reqs.clone());
        fo.set_view("replayable", json!({"entries": [{"wid": wid, "key": "k", "status": "committed", "reason": "", "stage": ""}]}));
        ft.set_default("bucket", "Apply", FakeReply::Outcome(Outcome::from_json(&json!({"ok": false, "code": "FailedPrecondition", "reason": CONFLICT}))));
        testkit::run_rule(&d, idem_conflict);
        let begins = fo.calls("/write/begin");
        let c = begins.last().unwrap();
        assert_eq!(c["role"], "conflict");
        assert_eq!(c["of"], json!(wid));
        assert_eq!(c["trackKey"], true);
        assert_eq!(c["idempotencyKey"], begins[0]["idempotencyKey"]);
        assert_ne!(c["requests"], begins[0]["requests"]);
        assert!(testkit::fired("idempotency conflict path exercised"));
        assert!(testkit::assertions().iter().any(|r| r.message == "mismatched idempotency key should return IDEMPOTENCY_KEY_CONFLICT" && r.cond));
    }

    #[test]
    fn judge_conflict_success_is_unreachable() {
        let ok = WriteResult::Definitive {
            outcome: Outcome::from_json(&json!({"ok": true, "response": {}})),
            verdict: Default::default(),
            wid: "w1".into(),
            key: "k".into(),
            attempts: vec![],
        };
        judge_conflict(&ok, &json!({}));
        assert!(testkit::assertions().iter().any(|r| r.message == "reusing idempotency key with different payload should fail"));
    }

    /// Task 03's protocol fixtures (`protocol-fixtures/03/`) round-trip
    /// through the driver's structs, and the Replay request is exactly what
    /// `idem_replay` sends for that plan.
    #[test]
    fn family_fixtures_round_trip() {
        use crate::oracle::*;
        use serde::{de::DeserializeOwned, Serialize};
        fn rt<T: Serialize + DeserializeOwned>(file: &str, v: &Value) {
            let t: T = serde_json::from_value(v.clone()).unwrap_or_else(|e| panic!("{file}: {e}"));
            assert_eq!(&serde_json::to_value(&t).unwrap(), v, "{file} does not round-trip");
        }
        let dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../protocol-fixtures/03");
        let mut n = 0;
        for e in std::fs::read_dir(&dir).unwrap() {
            let path = e.unwrap().path();
            let file = path.file_name().unwrap().to_string_lossy().to_string();
            let f: Value = serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
            let (req, resp) = (&f["request"], &f["response"]);
            match f["endpoint"].as_str().unwrap() {
                "POST /read/begin" => {
                    rt::<ReadBeginReq>(&file, req);
                    rt::<ReadBeginResp>(&file, resp);
                }
                "POST /read/end" => {
                    rt::<ReadEndReq>(&file, req);
                    rt::<ReadEndResp>(&file, resp);
                    assert_eq!(req["request"], wire::apply_request(req["request"]["unsigned"]["requests"].as_array().unwrap(), req["request"]["unsigned"]["idempotencyKey"].as_str()));
                }
                "POST /view" => {
                    rt::<ViewReqBody>(&file, req);
                    rt::<ViewResp>(&file, resp);
                }
                "POST /write/begin" => {
                    rt::<WriteBeginReq>(&file, req);
                    rt::<WriteBeginResp>(&file, resp);
                }
                other => panic!("{file}: unexpected endpoint {other}"),
            }
            n += 1;
        }
        assert!(n >= 5);
    }
}
