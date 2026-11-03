//! Task 11: the `exact` quiet-period flavour (DESIGN §4.10). Once the oracle
//! is quiescent (`Model()` is exactly the cluster's committed tracked state)
//! the hook feeds the oracle three things through `/quiet/section`:
//!
//! 1. the audit trail from the oracle's cursor (`audit` section), which the
//!    oracle replays in the server's own total order;
//! 2. every live tracked ledger's full state read linearizably (`ledger`
//!    section, scope `linearizable`): logs, transactions, accounts, stats and
//!    ledger info;
//! 3. for hegel-drawn nodes and ledgers, the same reads pinned to the node
//!    with `consistency: stale`, after the node reached the barrier horizon
//!    (scope `node:<addr>`).
//!
//! The oracle judges everything; the driver only reads and forwards. This
//! replaces `check_state` and `check_nodes_converge` of the old driver
//! (`H/main.rs:1094-1247`, DESIGN §6.9).
use crate::clients::{CallOpts, Outcome, Route, RouteMode};
use crate::draw;
use crate::machine::Driver;
use crate::nodes::NodeRef;
use crate::quiet::{HookPlan, QuietCtx, QuietHook};
use crate::shared::Shared;
use crate::wire::u64_field;
use hegel::TestCase;
use serde_json::{json, Value};
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

/// The hook's budget (DESIGN §4.10 table).
pub const BUDGET: Duration = Duration::from_secs(150);
/// Phase ends, in seconds of `BUDGET` (scaled to the actual budget).
const AUDIT_SHARE: u32 = 50;
const LINEARIZABLE_SHARE: u32 = 110;
/// Rows per page (and per section chunk).
pub const PAGE: u64 = 500;
/// Attempts per read before the phase gives up.
const ATTEMPTS: usize = 4;
/// A node gets this long to reach the horizon (within the phase).
const NODE_WAIT: Duration = Duration::from_secs(30);

pub fn quiet_hooks() -> Vec<QuietHook> {
    vec![QuietHook { name: "exact", budget: BUDGET, needs_oracle: true, weight: exact_weight, plan, run }]
}

/// 4, or 50 once 3 quiet periods passed without a completed exact check.
pub fn exact_weight(s: &Shared) -> f64 {
    if s.quiets_since_exact() >= 3 {
        50.0
    } else {
        4.0
    }
}

/// Every draw of the hook (made at Q0).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExactPlan {
    /// Per-node phase: every node, or the one at `node_pick mod len`.
    pub all_nodes: bool,
    pub node_pick: u32,
    /// 1–2 indices into `begin.ledgers.live`, taken modulo its length.
    pub ledger_picks: Vec<u32>,
    /// The linearizable reads' route: client and plain (no library retry) or lib.
    pub client: usize,
    pub plain: bool,
}

pub fn plan(d: &Driver, tc: &TestCase) -> HookPlan {
    let all_nodes = draw::chance(tc, 1.0 / 3.0);
    let node_pick = if all_nodes { 0 } else { draw::int_in(tc, 0, u32::MAX as u64) as u32 };
    let n = draw::int_in(tc, 1, 2) as usize;
    let ledger_picks = (0..n).map(|_| draw::ledger_pick(tc)).collect();
    let client = draw::pick(tc, d.s.clients.len());
    let plain = draw::chance(tc, 0.25);
    Box::new(ExactPlan { all_nodes, node_pick, ledger_picks, client, plain })
}

/// How a phase ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Phase {
    Done,
    /// Stopped at its deadline.
    Deadline,
    /// A read or the oracle failed after retries.
    Failed,
}

/// The state of one run: section sequence counters and the deadlines.
struct Exact<'a> {
    d: &'a Driver,
    ctx: &'a mut QuietCtx,
    audit_seq: u64,
    ledger_seq: u64,
    findings: usize,
}

impl Exact<'_> {
    fn post(&mut self, section: &str, body: Value) -> Option<Value> {
        let seq = if section == "audit" {
            self.audit_seq += 1;
            self.audit_seq - 1
        } else {
            self.ledger_seq += 1;
            self.ledger_seq - 1
        };
        let resp = self.ctx.section(self.d, section, seq, body)?;
        if let Some(v) = resp.pointer("/verdict/violations").and_then(Value::as_array).filter(|v| !v.is_empty()) {
            self.findings += v.len();
            let ids: Vec<&str> = v.iter().filter_map(|x| x.get("id").and_then(Value::as_str)).collect();
            println!("driver: exact quiet check: the oracle reports {ids:?}");
        }
        Some(resp)
    }
}

/// One read with retries (faults are paused). Lib/plain routes move to the
/// next client on each attempt; a node route keeps its node. Deterministic:
/// no draw.
fn read(d: &Driver, route: &Route, method: &str, req: &Value, opts: &CallOpts, deadline: Instant) -> Result<Outcome, Phase> {
    let n = d.s.clients.len().max(1);
    for attempt in 0..ATTEMPTS {
        let left = deadline.saturating_duration_since(Instant::now());
        if left.is_zero() {
            return Err(Phase::Deadline);
        }
        let r = Route { client: (route.client + attempt) % n, mode: route.mode.clone() };
        let o = opts.clone().timeout(left.min(Duration::from_secs(10)));
        let (_, out) = d.call(&r, "bucket", method, req.clone(), &o);
        if out.is_ok() {
            return Ok(out);
        }
        println!("driver: exact quiet check: {method} attempt {attempt}: {:?}", out.summary());
        std::thread::sleep(Duration::from_millis(200).min(deadline.saturating_duration_since(Instant::now())));
    }
    if Instant::now() >= deadline {
        Err(Phase::Deadline)
    } else {
        Err(Phase::Failed)
    }
}

/// Pages a list RPC by cursor (`options.pageSize = PAGE`), calling `page`
/// with each page's items and whether it is the last one. `page` returns
/// false to stop.
fn list_all(
    d: &Driver,
    route: &Route,
    method: &str,
    mut req: Value,
    opts: &CallOpts,
    deadline: Instant,
    mut page: impl FnMut(Vec<Value>, bool) -> bool,
) -> Phase {
    let mut cursor = String::new();
    loop {
        req["options"]["pageSize"] = json!(PAGE);
        if cursor.is_empty() {
            if let Some(o) = req["options"].as_object_mut() {
                o.remove("cursor");
            }
        } else {
            req["options"]["cursor"] = json!(cursor);
        }
        let out = match read(d, route, method, &req, &opts.clone().max_items(PAGE * 2), deadline) {
            Ok(o) => o,
            Err(p) => return p,
        };
        let items = out.items.clone().unwrap_or_default();
        let last = out.next_cursor.is_empty() || (items.is_empty() && out.complete);
        if !page(items, last) {
            return Phase::Failed;
        }
        if last {
            return Phase::Done;
        }
        if out.next_cursor == cursor {
            println!("driver: exact quiet check: {method} repeated its cursor");
            return Phase::Failed;
        }
        cursor = out.next_cursor.clone();
        if Instant::now() >= deadline {
            return Phase::Deadline;
        }
    }
}

/// The audit phase: the trail after the oracle's cursor, in chunks of `PAGE`.
fn audit_phase(ex: &mut Exact, route: &Route, deadline: Instant) -> Phase {
    let d = ex.d;
    let begin = ex.ctx.begin.clone().unwrap_or(Value::Null);
    let cursor: u64 = begin.get("auditCursor").and_then(Value::as_str).and_then(|s| s.parse().ok()).unwrap_or(0);
    let opts = CallOpts::linearizable();
    // The head: one tail read.
    let tail = match read(d, route, "ListAuditEntries", &json!({"options": {"pageSize": 1, "reverse": true}}), &opts.clone().max_items(1), deadline) {
        Ok(o) => o,
        Err(p) => return p,
    };
    let head = tail.items.as_ref().and_then(|i| i.first()).map(|e| u64_field(e, "sequence")).unwrap_or(0);
    let filter = json!({"audit": {"field": "AUDIT_FIELD_SEQUENCE", "uintCond": {"min": (cursor + 1).to_string()}}});
    let req = json!({"options": {"filter": filter}});
    let mut first = true;
    let mut resync = false;
    let mut oracle_failed = false;
    let phase = list_all(d, route, "ListAuditEntries", req, &opts, deadline, |items, last| {
        let mut body = json!({"entries": items, "complete": last});
        if first {
            body["head"] = json!(head.to_string());
            first = false;
        }
        match ex.post("audit", body) {
            Some(resp) => {
                resync = resp.pointer("/next/resync").and_then(Value::as_bool).unwrap_or(false);
                !resync
            }
            None => {
                oracle_failed = true;
                false
            }
        }
    });
    if resync {
        println!("driver: exact quiet check: the oracle resynchronised its audit replay at {head}");
        return Phase::Done;
    }
    if oracle_failed {
        return Phase::Failed;
    }
    phase
}

/// The full state of one ledger in one scope, posted part by part.
fn ledger_reads(ex: &mut Exact, route: &Route, opts: &CallOpts, ledger: &str, scope: &str, stats: bool, deadline: Instant) -> Phase {
    let d = ex.d;
    for (part, method) in [("logs", "ListLogs"), ("transactions", "ListTransactions"), ("accounts", "ListAccounts")] {
        let mut oracle_failed = false;
        let phase = list_all(d, route, method, json!({"ledger": ledger, "options": {}}), opts, deadline, |items, last| {
            let ok = ex.post("ledger", json!({"ledger": ledger, "scope": scope, "part": part, "items": items, "final": last})).is_some();
            oracle_failed |= !ok;
            ok
        });
        if phase != Phase::Done {
            return phase;
        }
    }
    let mut unary = vec![("info", "GetLedger")];
    if stats {
        unary.insert(0, ("stats", "GetLedgerStats"));
    }
    for (part, method) in unary {
        let out = match read(d, route, method, &json!({"ledger": ledger}), opts, deadline) {
            Ok(o) => o,
            Err(p) => return p,
        };
        let item = out.response.clone().unwrap_or(json!({}));
        if ex.post("ledger", json!({"ledger": ledger, "scope": scope, "part": part, "items": [item], "final": true})).is_none() {
            return Phase::Failed;
        }
    }
    Phase::Done
}

fn live_ledgers(ctx: &QuietCtx) -> Vec<String> {
    ctx.begin
        .as_ref()
        .and_then(|b| b.pointer("/ledgers/live"))
        .and_then(Value::as_array)
        .map(|l| l.iter().filter_map(Value::as_str).map(str::to_string).collect())
        .unwrap_or_default()
}

fn linearizable_phase(ex: &mut Exact, route: &Route, deadline: Instant) -> Phase {
    let opts = CallOpts::linearizable();
    for ledger in live_ledgers(ex.ctx) {
        let p = ledger_reads(ex, route, &opts, &ledger, "linearizable", true, deadline);
        if p != Phase::Done {
            return p;
        }
    }
    Phase::Done
}

/// The nodes and ledgers the plan selects (modulo the run-time lists).
pub fn per_node_targets(p: &ExactPlan, nodes: &[NodeRef], live: &[String]) -> (Vec<NodeRef>, Vec<String>) {
    let nodes = if p.all_nodes || nodes.is_empty() { nodes.to_vec() } else { vec![nodes[p.node_pick as usize % nodes.len()].clone()] };
    let mut ledgers: Vec<String> = Vec::new();
    if !live.is_empty() {
        for pick in &p.ledger_picks {
            let l = live[*pick as usize % live.len()].clone();
            if !ledgers.contains(&l) {
                ledgers.push(l);
            }
        }
    }
    (nodes, ledgers)
}

fn node_phase(ex: &mut Exact, p: &ExactPlan, route: &Route, deadline: Instant) -> Phase {
    let d = ex.d;
    let (nodes, ledgers) = per_node_targets(p, &ex.ctx.nodes.nodes, &live_ledgers(ex.ctx));
    if nodes.is_empty() {
        println!("driver: exact quiet check: no cluster node map; per-node phase skipped");
        return Phase::Failed;
    }
    let barrier = match read(d, route, "Barrier", &json!({}), &CallOpts::default(), deadline) {
        Ok(o) => o,
        Err(p) => return p,
    };
    let horizon = barrier.response.as_ref().map(|r| u64_field(r, "commitIndex")).unwrap_or(0);
    let client = ex.ctx.client;
    let mut result = Phase::Done;
    for node in &nodes {
        let wait_end = deadline.min(Instant::now() + NODE_WAIT);
        let reached = d.wait_at_horizon(node, horizon, false, wait_end);
        if reached.is_none() && wait_end >= deadline {
            return Phase::Deadline; // cut by the budget: says nothing about the node
        }
        check_sometimes!(reached.is_some(), "every node catches up after faults stop", json!({"node": node.addr, "persisted": reached, "index": horizon}));
        if reached.is_none() {
            result = Phase::Failed;
            continue;
        }
        let nroute = Route::node(client, &node.addr);
        let scope = format!("node:{}", node.addr);
        for ledger in &ledgers {
            match ledger_reads(ex, &nroute, &CallOpts::stale(), ledger, &scope, false, deadline) {
                Phase::Done => {}
                Phase::Deadline => return Phase::Deadline,
                Phase::Failed => result = Phase::Failed,
            }
        }
    }
    result
}

/// The run (no draws), stopping at `ctx.deadline`.
pub fn run(d: &Driver, ctx: &mut QuietCtx, plan: &HookPlan) {
    let Some(p) = plan.downcast_ref::<ExactPlan>() else { return };
    if ctx.begin.is_none() {
        return; // the oracle is skipped or degraded: nothing to judge against
    }
    let start = Instant::now();
    let total = ctx.deadline.saturating_duration_since(start);
    let audit_end = start + total * AUDIT_SHARE / BUDGET.as_secs() as u32;
    let lin_end = start + total * LINEARIZABLE_SHARE / BUDGET.as_secs() as u32;
    let deadline = ctx.deadline;
    let route = Route { client: p.client % d.s.clients.len().max(1), mode: if p.plain { RouteMode::Plain } else { RouteMode::Lib } };
    let mut ex = Exact { d, ctx, audit_seq: 0, ledger_seq: 0, findings: 0 };

    let audit = audit_phase(&mut ex, &route, audit_end);
    let lin = linearizable_phase(&mut ex, &route, lin_end);
    let node = node_phase(&mut ex, p, &route, deadline);
    let phases = [audit, lin, node];
    let completed = phases.iter().all(|x| *x == Phase::Done);
    let cut = phases.contains(&Phase::Deadline);
    println!(
        "driver: exact quiet check: audit {audit:?}, linearizable {lin:?}, per node {node:?}, {} finding(s), {} ms",
        ex.findings,
        start.elapsed().as_millis()
    );
    if completed {
        d.s.quiets_since_exact.store(0, Ordering::SeqCst);
    }
    check_sometimes!(completed, "the full state check completes in a quiet period", json!({"audit": format!("{audit:?}"), "linearizable": format!("{lin:?}"), "node": format!("{node:?}")}));
    if completed || cut {
        check_sometimes!(completed, "exact completes within its quiet budget", json!({"elapsedMs": start.elapsed().as_millis() as u64}));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clients::Outcome;
    use crate::testkit::{self, FakeReply};
    use std::sync::{Arc, Mutex};

    fn ok(v: Value) -> FakeReply {
        FakeReply::Outcome(Outcome::from_json(&json!({"ok": true, "response": v})))
    }
    fn page(items: Vec<Value>, next: &str) -> FakeReply {
        FakeReply::Outcome(Outcome::from_json(&json!({"ok": true, "items": items, "complete": true, "nextCursor": next})))
    }
    fn cs(local: u64) -> Value {
        json!({"localNode": local.to_string(), "syncProgress": {"status": "normal"}, "raftStatus": {"lastPersistedIndex": "7"}})
    }
    fn nodes() -> Value {
        json!({"nodes": [{"id": "1", "serviceAddress": "ledger-0:8888"}, {"id": "2", "serviceAddress": "ledger-1:8888"}]})
    }
    fn the_plan(all: bool) -> ExactPlan {
        ExactPlan { all_nodes: all, node_pick: 1, ledger_picks: vec![0, 2], client: 1, plain: false }
    }
    fn hook(budget: Duration) -> QuietHook {
        QuietHook { budget, ..quiet_hooks().remove(0) }
    }
    fn sections(fo: &testkit::FakeOracle, name: &str) -> Vec<Value> {
        fo.calls("/quiet/section").into_iter().filter(|b| b["section"] == name).collect()
    }

    #[test]
    fn weight_rises_after_three_quiet_periods_without_exact() {
        let (d, _, _) = testkit::driver();
        for (n, w) in [(0, 4.0), (2, 4.0), (3, 50.0), (9, 50.0)] {
            d.s.quiets_since_exact.store(n, Ordering::SeqCst);
            assert_eq!(exact_weight(&d.s), w, "{n}");
        }
        let h = quiet_hooks().remove(0);
        assert_eq!((h.name, h.budget, h.needs_oracle), ("exact", Duration::from_secs(150), true));
    }

    #[test]
    fn plan_draws_everything() {
        let (d, _, _) = testkit::driver();
        let seen = Arc::new(Mutex::new(Vec::new()));
        let s2 = seen.clone();
        testkit::with_tc_n(60, move |tc| {
            let p = plan(&d, tc);
            s2.lock().unwrap().push(p.downcast_ref::<ExactPlan>().unwrap().clone());
        });
        let seen = seen.lock().unwrap();
        assert!(!seen.is_empty());
        for p in seen.iter() {
            assert!((1..=2).contains(&p.ledger_picks.len()));
            assert!(p.client < 3);
        }
    }

    #[test]
    fn targets_are_taken_modulo_the_runtime_lists() {
        let ns = vec![NodeRef { id: 1, addr: "a".into() }, NodeRef { id: 2, addr: "b".into() }, NodeRef { id: 3, addr: "c".into() }];
        let live = vec!["l1".to_string(), "l2".to_string()];
        let (n, l) = per_node_targets(&ExactPlan { all_nodes: false, node_pick: 4, ledger_picks: vec![1, 3], client: 0, plain: false }, &ns, &live);
        assert_eq!(n, vec![ns[1].clone()]);
        assert_eq!(l, vec!["l2".to_string()]);
        let (n, l) = per_node_targets(&ExactPlan { all_nodes: true, node_pick: 0, ledger_picks: vec![0, 1], client: 0, plain: false }, &ns, &live);
        assert_eq!(n.len(), 3);
        assert_eq!(l.len(), 2);
        let (_, l) = per_node_targets(&the_plan(false), &ns, &[]);
        assert!(l.is_empty());
    }

    /// A full run: audit chunks (with the head), every part of every live
    /// ledger linearizably, then the drawn node's stale reads; it completes
    /// and resets the exact counter. `run` takes no TestCase: it cannot draw.
    #[test]
    fn a_full_run_posts_every_section_and_completes() {
        let (d, ft, fo) = testkit::driver();
        testkit::reset();
        fo.set_live(vec!["o-test-l1".into(), "o-test-l2".into()]);
        ft.script("cluster", "GetClusterState", vec![ok(nodes()), ok(cs(2))]);
        ft.script("bucket", "ListAuditEntries", vec![page(vec![json!({"sequence": "12"})], ""), page(vec![json!({"sequence": "1"})], "c1"), page(vec![json!({"sequence": "2"})], "")]);
        ft.script("bucket", "ListLogs", vec![page(vec![json!({"sequence": "1"})], "x"), page(vec![], "")]);
        ft.script("bucket", "Barrier", vec![ok(json!({"commitIndex": "7"}))]);
        d.s.quiets_since_exact.store(5, Ordering::SeqCst);
        crate::quiet::run_quiet(&d, Some(&hook(BUDGET)), Some(Box::new(the_plan(false))));

        let audit = sections(&fo, "audit");
        assert_eq!(audit.len(), 2);
        assert_eq!(audit[0]["body"]["head"], "12");
        assert_eq!(audit[0]["body"]["complete"], false);
        assert_eq!(audit[1]["body"]["complete"], true);
        assert_eq!(audit[1]["seq"], 1);
        let calls = ft.calls_of("bucket", "ListAuditEntries");
        assert_eq!(calls[1]["request"]["options"]["filter"]["audit"]["uintCond"]["min"], "1");
        assert_eq!(calls[2]["request"]["options"]["cursor"], "c1");

        let ledger = sections(&fo, "ledger");
        let lin: Vec<_> = ledger.iter().filter(|b| b["body"]["scope"] == "linearizable").collect();
        for l in ["o-test-l1", "o-test-l2"] {
            for part in ["logs", "transactions", "accounts", "stats", "info"] {
                assert!(lin.iter().any(|b| b["body"]["ledger"] == l && b["body"]["part"] == part && b["body"]["final"] == true), "{l} {part}");
            }
        }
        // The logs of l1 came in two chunks.
        assert!(lin.iter().any(|b| b["body"]["ledger"] == "o-test-l1" && b["body"]["part"] == "logs" && b["body"]["final"] == false));
        let node: Vec<_> = ledger.iter().filter(|b| b["body"]["scope"] == "node:ledger-1:8888").collect();
        assert_eq!(node.len(), 4, "logs, transactions, accounts, info of l1 on the drawn node");
        assert!(node.iter().all(|b| b["body"]["ledger"] == "o-test-l1" && b["body"]["part"] != "stats"));
        let stale: Vec<_> = ft.calls().into_iter().filter(|c| c["node"] == "ledger-1:8888" && c["service"] == "bucket").collect();
        assert!(!stale.is_empty() && stale.iter().all(|c| c["consistency"] == "stale"));
        // Distinct sequence numbers per section.
        let seqs: Vec<u64> = ledger.iter().map(|b| b["seq"].as_u64().unwrap()).collect();
        assert_eq!(seqs, (0..seqs.len() as u64).collect::<Vec<_>>());

        let a = testkit::assertions();
        for m in ["the full state check completes in a quiet period", "exact completes within its quiet budget", "every node catches up after faults stop"] {
            assert!(a.iter().any(|x| x.message == m && x.cond), "{m}");
        }
        assert_eq!(d.s.quiets_since_exact(), 0);
    }

    #[test]
    fn a_deadline_stop_reports_partial_work() {
        let (d, ft, _) = testkit::driver();
        testkit::reset();
        ft.set_default("bucket", "ListAuditEntries", FakeReply::Delayed(Duration::from_millis(40), Outcome::from_json(&json!({"ok": true, "items": [{"sequence": "1"}], "complete": true, "nextCursor": "more"}))));
        d.s.quiets_since_exact.store(1, Ordering::SeqCst);
        crate::quiet::run_quiet(&d, Some(&hook(Duration::from_millis(150))), Some(Box::new(the_plan(false))));
        let a = testkit::assertions();
        assert!(a.iter().any(|x| x.message == "exact completes within its quiet budget" && !x.cond));
        assert!(a.iter().any(|x| x.message == "the full state check completes in a quiet period" && !x.cond));
        assert!(!a.iter().any(|x| x.message == "the full state check completes in a quiet period" && x.cond));
        assert_eq!(d.s.quiets_since_exact(), 2, "not reset: the run did not complete");
    }

    #[test]
    fn without_a_quiet_begin_the_hook_returns_at_once() {
        let (d, ft, fo) = testkit::driver();
        testkit::reset();
        d.s.enter_degraded("test");
        crate::quiet::run_quiet(&d, Some(&hook(BUDGET)), Some(Box::new(the_plan(false))));
        assert!(fo.calls("/quiet/section").is_empty());
        assert!(ft.calls_of("bucket", "ListAuditEntries").is_empty());
        assert!(!testkit::fired("the full state check completes in a quiet period"));
    }

    /// The protocol fixtures of task 11 decode into the shapes the hook sends.
    #[test]
    fn protocol_fixtures_match_the_bodies() {
        let dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../protocol-fixtures/11");
        let mut seen = 0;
        for e in std::fs::read_dir(&dir).unwrap() {
            let path = e.unwrap().path();
            let v: Value = serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
            assert_eq!(v["endpoint"], "POST /quiet/section");
            let req: crate::oracle::QuietSectionReq = serde_json::from_value(v["request"].clone()).unwrap();
            let resp: crate::oracle::QuietSectionResp = serde_json::from_value(v["response"].clone()).unwrap();
            match req.section.as_str() {
                "audit" => {
                    assert!(req.body["entries"].is_array() && req.body["complete"].is_boolean() && req.body["head"].is_string());
                    assert!(resp.next["cursor"].is_string() && resp.next["resync"].is_boolean());
                }
                "ledger" => {
                    let s = req.body["scope"].as_str().unwrap();
                    assert!(s == "linearizable" || s.starts_with("node:"));
                    assert!(req.body["items"].is_array() && req.body["final"].is_boolean() && req.body["part"].is_string());
                }
                other => panic!("unknown section {other}"),
            }
            seen += 1;
        }
        assert!(seen >= 2);
    }
}
