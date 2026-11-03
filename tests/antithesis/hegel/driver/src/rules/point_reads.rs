//! Task 05: point reads — GetAccount, GetTransaction, AggregateVolumes,
//! GetLedgerStats and GetLog by global sequence. Every read goes through a
//! read ticket, linearizable, on a drawn route; the oracle's `pointcheck`
//! read kinds validate the answers. Rust asserts nothing about content except
//! the structural balance identity on every GetAccount volume it receives
//! (`H/main.rs:635`, verbatim).
//!
//! All draws happen before `/read/begin`, except indices into the returned
//! views (the aggregate's metadata key, the GetLog sequence).
use crate::clients::{CallOpts, Outcome};
use crate::draw;
use crate::machine::{rule, Driver};
use crate::read::{LedgerSel, ViewReq};
use crate::shared::Shared;
use crate::wire::volumes_full;
use hegel::generators as gs;
use hegel::stateful::ConcurrentRule;
use hegel::TestCase;
use serde_json::{json, Value};

pub fn rules() -> Vec<ConcurrentRule<Driver>> {
    vec![
        rule("get_account", 4.0, get_account),
        rule("get_transaction", 3.0, get_transaction),
        rule("aggregate", 1.5, aggregate),
        rule("ledger_stats", 1.0, ledger_stats),
        rule("get_log", 1.0, get_log),
    ]
}

/// How far past the highest learned global sequence an unassigned GetLog
/// probe lands (DESIGN §3.2; the oracle uses the same constant).
pub const UNASSIGNED_SEQ_SLACK: u64 = 1 << 40;

/// Probability of probing an absent ledger.
const ABSENT_P: f64 = 0.02;

/// An absent-ledger probe name `o-<run>-absent-<draw>`: inside the tracked
/// prefix (so the oracle judges it), never created by anyone, never a
/// reserved (`o-<run>-own…`) or generated (`o-<run>-l<N>`/`m<N>`) name.
pub fn absent_ledger_name(s: &Shared, tc: &TestCase) -> String {
    format!("{}absent-{:016x}", s.tracked_prefix(), tc.draw(gs::integers::<u64>()))
}

/// Draws the absent-ledger flag and, when set, the absent name; otherwise the
/// target is the `ledgerPick`.
fn draw_target(d: &Driver, tc: &TestCase, pick: u32) -> (bool, LedgerSel) {
    if draw::chance(tc, ABSENT_P) {
        (true, LedgerSel::Name(absent_ledger_name(&d.s, tc)))
    } else {
        (false, LedgerSel::Pick(pick))
    }
}

/// A `ledger` view over `live ++ deleted` when aimed by pick.
fn including_deleted(mut v: ViewReq, sel: &LedgerSel) -> ViewReq {
    if matches!(sel, LedgerSel::Pick(_)) {
        v.0["includeDeleted"] = json!(true);
    }
    v
}

fn resolved_ledger(view: &Value) -> Option<String> {
    view.get("ledger").and_then(Value::as_str).map(str::to_string)
}

// ---------------------------------------------------------------- GetAccount

/// `input - output == balance` on one served account volume, exactly (256-bit).
pub fn balance_consistent(vol: &Value) -> bool {
    let Some((i, o, b)) = volumes_full(vol) else { return false };
    let (Some(i), Some(o), Some(mag)) = (i.to_u256(), o.to_u256(), b.magnitude.to_u256()) else { return false };
    if i >= o {
        !b.negative && i.checked_sub(&o) == Some(mag)
    } else {
        b.negative && o.checked_sub(&i) == Some(mag)
    }
}

fn check_balances(address: &str, out: &Outcome) {
    if !out.is_ok() {
        return;
    }
    let vols = out.response.as_ref().and_then(|r| r.get("volumes")).and_then(Value::as_array).cloned().unwrap_or_default();
    for vol in vols {
        check_always!(balance_consistent(&vol), "an account's balance equals its input minus its output", json!({"account": address, "volume": vol}));
    }
}

/// The account a GetAccount reads: the seek entry's address, or the probe
/// itself when the seek found nothing.
fn account_target(view: &Value, probe: &str) -> String {
    view.pointer("/entry/address").and_then(Value::as_str).filter(|_| view["found"] == json!(true)).unwrap_or(probe).to_string()
}

/// How many pool addresses the model-absent probe tries (M/reads.go
/// pickAbsentAccount).
const ABSENT_TRIES: usize = 8;

/// Whether a `seek` on `addr` landed on `addr` itself: the model holds a
/// volume cell (volumes table) or metadata (accountMeta table) for it.
fn seek_hits(view: &Value, addr: &str) -> bool {
    view["found"] == json!(true) && view.pointer("/entry/address").and_then(Value::as_str) == Some(addr)
}

/// The model-absent probe's target: the first candidate neither of its two
/// seeks (`views[2i]` volumes, `views[2i+1]` accountMeta) shows the model
/// knows (M/reads.go modelKnowsAccount), else the first candidate — an
/// ordinary known-account read, as the old fallback to pickCell was.
fn model_absent_target(cands: &[String], views: &[Value]) -> String {
    cands
        .iter()
        .enumerate()
        .find(|(i, c)| match (views.get(2 * i), views.get(2 * i + 1)) {
            (Some(vol), Some(meta)) => !seek_hits(vol, c) && !seek_hits(meta, c),
            _ => false,
        })
        .map_or(&cands[0], |(_, c)| c)
        .clone()
}

pub fn get_account(d: &Driver, tc: TestCase) {
    d.guarded("get_account", &tc, |d, tc| {
        if !d.s.oracle_ok() {
            return None;
        }
        let pick = draw::ledger_pick(tc);
        let route = draw::route(tc, &d.s);
        let (absent, sel) = draw_target(d, tc, pick);
        let model_absent = !absent && draw::chance(tc, 0.05);
        let meta_only = !absent && !model_absent && draw::chance(tc, 0.25);
        let collapse = draw::chance(tc, 1.0 / 3.0);
        let table = if meta_only { "accountMeta" } else { "volumes" };
        // The model-absent probe pre-draws ABSENT_TRIES pool addresses and
        // seeks each in both tables; the target is picked from those views.
        let probes: Vec<String> = if model_absent { (0..ABSENT_TRIES).map(|_| draw::pool_address(tc)).collect() } else { vec![draw::address(tc)] };
        let views: Vec<ViewReq> = if model_absent {
            probes.iter().flat_map(|p| [ViewReq::seek(sel.clone(), "volumes", p, None), ViewReq::seek(sel.clone(), "accountMeta", p, None)]).collect()
        } else {
            vec![ViewReq::seek(sel, table, &probes[0], None)]
        };

        let rt = d.read_begin("GetAccount", &views, json!({}))?;
        let view = rt.views().first()?.clone();
        let ledger = resolved_ledger(&view)?;
        let address = if model_absent { model_absent_target(&probes, rt.views()) } else { account_target(&view, &probes[0]) };
        let req = json!({"ledger": ledger, "address": address, "collapseColors": collapse});
        let opts = CallOpts::linearizable();
        let (_, out) = d.call(&route, "bucket", "GetAccount", req.clone(), &opts);
        check_balances(&address, &out);
        rt.end(&req, &out, &route, &opts, json!({"absentLedger": absent, "modelAbsent": model_absent, "metaOnly": meta_only, "collapsed": collapse}), json!({}));
        Some(())
    });
}

// ---------------------------------------------------------------- GetTransaction

/// The probed id: `1 + offset mod (txCount + 8)` — a committed, in-flight or
/// unassigned id.
pub fn transaction_id(tx_count: u64, offset: u32) -> u64 {
    1 + offset as u64 % (tx_count + 8)
}

/// The probed id on a ledger outside the fleet: `1..=64`, as M/reads.go
/// pickTransactionID drew it (any id must be NotFound there).
pub fn absent_ledger_transaction_id(offset: u32) -> u64 {
    1 + offset as u64 % 64
}

pub fn get_transaction(d: &Driver, tc: TestCase) {
    d.guarded("get_transaction", &tc, |d, tc| {
        if !d.s.oracle_ok() {
            return None;
        }
        let pick = draw::ledger_pick(tc);
        let route = draw::route(tc, &d.s);
        let (absent, sel) = draw_target(d, tc, pick);
        let offset: u32 = tc.draw(gs::integers::<u32>());

        let rt = d.read_begin("GetTransaction", &[ViewReq::ledger(sel)], json!({}))?;
        let view = rt.views().first()?.clone();
        let ledger = resolved_ledger(&view)?;
        let id = if absent { absent_ledger_transaction_id(offset) } else { transaction_id(view.get("txCount").and_then(Value::as_u64).unwrap_or(0), offset) };
        let req = json!({"ledger": ledger, "transactionId": id.to_string()});
        let opts = CallOpts::linearizable();
        let (_, out) = d.call(&route, "bucket", "GetTransaction", req.clone(), &opts);
        rt.end(&req, &out, &route, &opts, json!({"absentLedger": absent}), json!({}));
        Some(())
    });
}

// ---------------------------------------------------------------- AggregateVolumes

/// A pre-drawn accounts filter. `MetaExists` resolves its key after
/// `/read/begin` from the ledger view's declared account field types (an
/// index draw), falling back to its pre-drawn model key `k0..k3`.
///
/// Local minimal builder (address prefix/exact, metadata exists); task 06
/// owns the full filter generator and task 99 may dedupe.
#[derive(Clone, Debug, PartialEq)]
pub enum Shape {
    Prefix(String, &'static str),
    Exact(String, &'static str),
    MetaExists(String),
    And(Vec<Shape>),
    Or(Vec<Shape>),
    Not(Box<Shape>),
}

const MAX_FILTER_DEPTH: usize = 2;
const ROLES: [&str; 3] = ["ADDRESS_ROLE_ANY", "ADDRESS_ROLE_SOURCE", "ADDRESS_ROLE_DESTINATION"];

fn pool_prefix(tc: &TestCase) -> String {
    format!("t-{}:", draw::int_in(tc, 0, 24))
}

fn free_leaf(tc: &TestCase) -> Shape {
    let role = *draw::one_of(tc, &ROLES);
    if draw::chance(tc, 0.5) {
        Shape::Prefix(pool_prefix(tc), role)
    } else {
        Shape::Exact(draw::pool_address(tc), role)
    }
}

fn boolean(tc: &TestCase, depth: usize, child: fn(&TestCase, usize) -> Shape) -> Shape {
    match draw::int_in(tc, 0, 2) {
        0 => Shape::And((0..draw::int_in(tc, 2, 3)).map(|_| child(tc, depth + 1)).collect()),
        1 => Shape::Or((0..draw::int_in(tc, 2, 3)).map(|_| child(tc, depth + 1)).collect()),
        _ => Shape::Not(Box::new(child(tc, depth + 1))),
    }
}

/// An index-free accounts filter (address leaves and their compositions).
pub fn gen_free(tc: &TestCase, depth: usize) -> Shape {
    if depth >= MAX_FILTER_DEPTH || draw::chance(tc, 0.5) {
        free_leaf(tc)
    } else {
        boolean(tc, depth, gen_free)
    }
}

/// An accounts filter that usually reads a metadata index.
pub fn gen_indexed(tc: &TestCase, depth: usize) -> Shape {
    if depth >= MAX_FILTER_DEPTH || draw::chance(tc, 0.5) {
        if draw::chance(tc, 2.0 / 3.0) {
            Shape::MetaExists(draw::meta_key_model(tc))
        } else {
            free_leaf(tc)
        }
    } else {
        boolean(tc, depth, gen_indexed)
    }
}

/// Renders a shape as `common.QueryFilter` protojson; `keys` are the ledger
/// view's declared account field keys (reserved keys already hidden).
pub fn resolve(shape: &Shape, keys: &[String], tc: &TestCase) -> Value {
    match shape {
        Shape::Prefix(p, role) => json!({"address": {"hardcodedPrefix": p, "role": role}}),
        Shape::Exact(a, role) => json!({"address": {"hardcodedExact": a, "role": role}}),
        Shape::MetaExists(fallback) => {
            let key = if keys.is_empty() { fallback.clone() } else { keys[draw::pick(tc, keys.len())].clone() };
            json!({"field": {"field": {"metadata": key}, "existsCond": {}}})
        }
        Shape::And(cs) => json!({"and": {"filters": cs.iter().map(|c| resolve(c, keys, tc)).collect::<Vec<_>>()}}),
        Shape::Or(cs) => json!({"or": {"filters": cs.iter().map(|c| resolve(c, keys, tc)).collect::<Vec<_>>()}}),
        Shape::Not(c) => json!({"not": {"filter": resolve(c, keys, tc)}}),
    }
}

/// 1–3 grouping prefixes from the pool, sometimes `no-such-prefix:`.
pub fn gen_group_prefixes(tc: &TestCase) -> Vec<String> {
    (0..draw::int_in(tc, 1, 3)).map(|_| if draw::chance(tc, 0.25) { "no-such-prefix:".to_string() } else { pool_prefix(tc) }).collect()
}

fn account_field_keys(ledger_view: &Value) -> Vec<String> {
    let mut keys: Vec<String> = ledger_view.pointer("/fieldTypes/account").and_then(Value::as_object).map(|m| m.keys().cloned().collect()).unwrap_or_default();
    keys.sort();
    keys
}

pub fn aggregate(d: &Driver, tc: TestCase) {
    d.guarded("aggregate", &tc, |d, tc| {
        if !d.s.oracle_ok() {
            return None;
        }
        let pick = draw::ledger_pick(tc);
        let route = draw::route(tc, &d.s);
        let (absent, sel) = draw_target(d, tc, pick);
        // Filter class: nil 1/6, free 1/2, indexed 1/3.
        let shape = match draw::weighted(tc, &[1.0 / 6.0, 0.5, 1.0 / 3.0]) {
            Some(1) => Some(gen_free(tc, 0)),
            Some(2) => Some(gen_indexed(tc, 0)),
            _ => None,
        };
        let collapse = draw::chance(tc, 1.0 / 3.0);
        let max_precision = draw::chance(tc, 0.25);
        let prefixes = if draw::chance(tc, 0.25) { gen_group_prefixes(tc) } else { vec![] };

        // Only the ledger view: accounts filters have no date leaves, so the
        // spec's `stamps` view would go unread (06's leaf builder adds it back
        // if its leaves need it).
        let rt = d.read_begin("AggregateVolumes", &[including_deleted(ViewReq::ledger(sel.clone()), &sel)], json!({}))?;
        let view = rt.views().first()?.clone();
        let ledger = resolved_ledger(&view)?;
        let keys = account_field_keys(&view);
        let mut req = json!({"ledger": ledger, "collapseColors": collapse, "useMaxPrecision": max_precision, "groupByPrefixes": prefixes});
        if let Some(s) = &shape {
            req["filter"] = resolve(s, &keys, tc);
        }
        let opts = CallOpts::linearizable();
        let (_, out) = d.call(&route, "bucket", "AggregateVolumes", req.clone(), &opts);
        rt.end(&req, &out, &route, &opts, json!({"absentLedger": absent, "collapsed": collapse, "filtered": shape.is_some()}), json!({}));
        Some(())
    });
}

// ---------------------------------------------------------------- GetLedgerStats

pub fn ledger_stats(d: &Driver, tc: TestCase) {
    d.guarded("ledger_stats", &tc, |d, tc| {
        if !d.s.oracle_ok() {
            return None;
        }
        let pick = draw::ledger_pick(tc);
        let route = draw::route(tc, &d.s);
        let (absent, sel) = draw_target(d, tc, pick);

        let rt = d.read_begin("GetLedgerStats", &[including_deleted(ViewReq::ledger(sel.clone()), &sel)], json!({}))?;
        let ledger = resolved_ledger(rt.views().first()?)?;
        let req = json!({"ledger": ledger});
        let opts = CallOpts::linearizable();
        let (_, out) = d.call(&route, "bucket", "GetLedgerStats", req.clone(), &opts);
        rt.end(&req, &out, &route, &opts, json!({"absentLedger": absent}), json!({}));
        Some(())
    });
}

// ---------------------------------------------------------------- GetLog

/// `maxLearnedSeq + 2^40` of a `logSeqs` view, as a decimal string.
pub fn unassigned_sequence(view: &Value) -> Option<String> {
    let max: u64 = view.get("maxLearnedSeq").and_then(Value::as_str)?.parse().ok()?;
    Some(max.checked_add(UNASSIGNED_SEQ_SLACK)?.to_string())
}

fn learned_sequences(view: &Value) -> Vec<String> {
    view.get("seqs").and_then(Value::as_array).map(|a| a.iter().filter_map(|s| s.as_str().map(str::to_string)).collect()).unwrap_or_default()
}

pub fn get_log(d: &Driver, tc: TestCase) {
    d.guarded("get_log", &tc, |d, tc| {
        if !d.s.oracle_ok() {
            return None;
        }
        let route = draw::route(tc, &d.s);
        let unassigned = draw::chance(tc, 1.0 / 8.0);

        let rt = d.read_begin("GetLog", &[ViewReq::named("logSeqs", json!({}))], json!({}))?;
        let view = rt.views().first()?.clone();
        let seq = if unassigned {
            unassigned_sequence(&view)?
        } else {
            let seqs = learned_sequences(&view);
            if seqs.is_empty() {
                return None; // the guard abandons the ticket
            }
            seqs[draw::pick(tc, seqs.len())].clone()
        };
        let req = json!({"sequence": seq});
        let opts = CallOpts::linearizable();
        let (_, out) = d.call(&route, "bucket", "GetLog", req.clone(), &opts);
        rt.end(&req, &out, &route, &opts, json!({"unassigned": unassigned}), json!({}));
        Some(())
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testkit::{self, FakeReply};
    use crate::wire::biguint_json;

    fn volume(i: u64, o: u64, neg: bool, mag: u64) -> Value {
        let b = |n: u64| biguint_json(&crate::wire::BigUint::from_be(&n.to_be_bytes()));
        json!({"asset": "COIN", "volumes": {"input": b(i), "output": b(o), "balance": {"negative": neg, "magnitude": b(mag)}}})
    }

    #[test]
    fn balance_identity() {
        assert!(balance_consistent(&volume(10, 3, false, 7)));
        assert!(balance_consistent(&volume(3, 10, true, 7)));
        assert!(balance_consistent(&volume(5, 5, false, 0)));
        assert!(balance_consistent(&volume(5, 5, true, 0)), "a negative zero is zero");
        assert!(!balance_consistent(&volume(10, 3, false, 6)));
        assert!(!balance_consistent(&volume(10, 3, true, 7)));
        assert!(!balance_consistent(&volume(3, 10, false, 7)));
    }

    #[test]
    fn absent_names_stay_in_the_tracked_prefix() {
        let (d, _, _) = testkit::driver();
        let prefix = d.s.tracked_prefix();
        testkit::with_tc_n(200, |tc| {
            let n = absent_ledger_name(&d.s, tc);
            assert!(n.starts_with(&format!("{prefix}absent-")), "{n}");
            assert!(!n.starts_with(&format!("{prefix}own")), "{n}");
            assert!(!n.contains("own-"), "{n}");
        });
    }

    #[test]
    fn unassigned_sequence_is_max_learned_plus_2_pow_40() {
        assert_eq!(unassigned_sequence(&json!({"seqs": ["5", "77"], "maxLearnedSeq": "77"})), Some((77u64 + (1u64 << 40)).to_string()));
        assert_eq!(unassigned_sequence(&json!({"seqs": [], "maxLearnedSeq": "0"})), Some((1u64 << 40).to_string()));
        assert_eq!(unassigned_sequence(&json!({"maxLearnedSeq": u64::MAX.to_string()})), None);
        assert_eq!(UNASSIGNED_SEQ_SLACK, 1_099_511_627_776);
    }

    #[test]
    fn model_absent_probe_skips_known_candidates() {
        let cands: Vec<String> = ["t-1:1", "t-1:2", "t-1:3"].iter().map(|s| s.to_string()).collect();
        let hit = |a: &str| json!({"ledger": "o-test-l1", "found": true, "wrapped": false, "entry": {"address": a}});
        let miss = json!({"ledger": "o-test-l1", "found": false, "wrapped": false, "entry": null});
        // t-1:1 has a cell, t-1:2 has metadata only, t-1:3 is unknown.
        let views = vec![hit("t-1:1"), hit("t-1:5"), hit("t-1:3"), hit("t-1:2"), hit("t-1:30"), miss.clone()];
        assert_eq!(model_absent_target(&cands, &views), "t-1:3");
        // Every candidate known: fall back to the first.
        let views = vec![hit("t-1:1"), miss.clone(), miss.clone(), hit("t-1:2"), hit("t-1:3"), miss.clone()];
        assert_eq!(model_absent_target(&cands, &views), "t-1:1");
        // A seek that wrapped onto another address does not count as known.
        assert_eq!(model_absent_target(&cands[..1], &[hit("t-0:9"), miss]), "t-1:1");
        assert_eq!(account_target(&hit("t-2:2"), "t-2:1"), "t-2:2");
    }

    #[test]
    fn transaction_ids_cover_committed_and_slack() {
        assert_eq!(transaction_id(0, 0), 1);
        assert_eq!(transaction_id(0, 7), 8);
        assert_eq!(transaction_id(0, 8), 1);
        assert_eq!(transaction_id(10, 17), 18);
        assert_eq!(transaction_id(10, u32::MAX), 1 + u32::MAX as u64 % 18);
        assert_eq!(absent_ledger_transaction_id(0), 1);
        assert_eq!(absent_ledger_transaction_id(63), 64);
        assert_eq!(absent_ledger_transaction_id(64), 1);
    }

    fn walk(f: &Value, visit: &mut impl FnMut(&Value)) {
        visit(f);
        if let Some(cs) = f.pointer("/and/filters").or_else(|| f.pointer("/or/filters")).and_then(Value::as_array) {
            assert!(!cs.is_empty());
            cs.iter().for_each(|c| walk(c, visit));
        }
        if let Some(c) = f.pointer("/not/filter") {
            walk(c, visit);
        }
    }

    #[test]
    fn filters_never_name_reserved_entities_and_render_valid_shapes() {
        let keys = vec!["k1".to_string(), "status".to_string()];
        testkit::with_tc_n(300, |tc| {
            for shape in [gen_free(tc, 0), gen_indexed(tc, 0)] {
                let f = resolve(&shape, &keys, tc);
                let text = f.to_string();
                assert!(!text.contains("own-") && !text.contains("o-test-own"), "{text}");
                walk(&f, &mut |n| {
                    let arms = ["address", "field", "and", "or", "not"].iter().filter(|a| n.get(**a).is_some()).count();
                    assert_eq!(arms, 1, "{n}");
                    if let Some(k) = n.pointer("/field/field/metadata").and_then(Value::as_str) {
                        assert!(keys.contains(&k.to_string()), "{k}");
                    }
                });
            }
            for p in gen_group_prefixes(tc) {
                assert!(p == "no-such-prefix:" || (p.starts_with("t-") && p.ends_with(':')), "{p}");
            }
        });
        // Without declared keys, the pre-drawn model key is used.
        testkit::with_tc_n(1, |tc| {
            let f = resolve(&Shape::MetaExists("k2".into()), &[], tc);
            assert_eq!(f, json!({"field": {"field": {"metadata": "k2"}, "existsCond": {}}}));
        });
    }

    fn ok(v: Value) -> FakeReply {
        FakeReply::Outcome(Outcome::from_json(&json!({"ok": true, "response": v})))
    }

    #[test]
    fn rules_post_linearizable_reads_of_their_kind() {
        let (d, ft, fo) = testkit::driver();
        fo.set_live(vec!["o-test-l1".into(), "o-test-l2".into()]);
        fo.set_view("seek", json!({"found": true, "wrapped": false, "entry": {"address": "t-3:4", "asset": "COIN", "color": ""}}));
        fo.set_view("ledger", json!({"txCount": 3, "fieldTypes": {"account": {"k1": "METADATA_TYPE_STRING"}}}));
        fo.set_view("logSeqs", json!({"seqs": ["3", "9"], "maxLearnedSeq": "9"}));
        ft.set_default("bucket", "GetAccount", ok(json!({"address": "t-3:4", "volumes": [volume(10, 3, false, 7)]})));
        ft.set_default("bucket", "GetTransaction", ok(json!({})));
        ft.set_default("bucket", "AggregateVolumes", ok(json!({})));
        ft.set_default("bucket", "GetLedgerStats", ok(json!({})));
        ft.set_default("bucket", "GetLog", ok(json!({})));
        testkit::reset();

        let mut want = Vec::new();
        for (f, kind) in [(get_account as fn(&Driver, TestCase), "GetAccount"), (get_transaction, "GetTransaction"), (aggregate, "AggregateVolumes"), (ledger_stats, "GetLedgerStats"), (get_log, "GetLog")] {
            testkit::run_rule(&d, f);
            want.push(kind);
        }
        let ends = fo.calls("/read/end");
        let kinds: Vec<&str> = ends.iter().map(|e| e["kind"].as_str().unwrap()).collect();
        assert_eq!(kinds, want);
        for e in &ends {
            assert_eq!(e["route"]["consistency"], "linearizable", "{e}");
            let l = e["request"]["ledger"].as_str();
            if let Some(l) = l {
                assert!(l.starts_with("o-test-"), "{l}");
            }
        }
        let acct = &ends[0]["request"];
        assert!(!acct["address"].as_str().unwrap().is_empty());
        let tx = &ends[1]["request"];
        let id: u64 = tx["transactionId"].as_str().unwrap().parse().unwrap();
        assert!((1..=11).contains(&id), "{id}");
        // AggregateVolumes asks for the ledger view only (no unread `stamps`).
        let agg_begin = fo.calls("/read/begin").into_iter().find(|b| b["kind"] == "AggregateVolumes").unwrap();
        let names: Vec<&str> = agg_begin["views"].as_array().unwrap().iter().map(|v| v["name"].as_str().unwrap()).collect();
        assert_eq!(names, ["ledger"], "{agg_begin}");
        let seq = ends[4]["request"]["sequence"].as_str().unwrap().to_string();
        assert!(seq == "3" || seq == "9" || seq == (9u64 + (1 << 40)).to_string(), "{seq}");
        assert!(d.s.registry.snapshot().is_empty());
        assert!(fo.open_reads().is_empty());

        // The structural Always ran on the served volume (when the target was
        // a live ledger; every GetAccount answer here carries one volume).
        let a = testkit::assertions();
        assert!(a.iter().any(|r| r.message == "an account's balance equals its input minus its output" && r.cond));
    }

    #[test]
    fn get_log_without_learned_sequences_abandons() {
        let (d, _ft, fo) = testkit::driver();
        fo.set_live(vec!["o-test-l1".into()]);
        fo.set_view("logSeqs", json!({"seqs": [], "maxLearnedSeq": "0"}));
        for _ in 0..4 {
            testkit::run_rule(&d, get_log);
        }
        assert!(fo.open_reads().is_empty());
        assert_eq!(fo.calls("/read/end").len() + fo.calls("/read/abandon").len(), 4);
    }
}
