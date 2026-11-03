//! Task 04: ledger lifecycle (create normal/mirror, delete, promote, recreate
//! a deleted name, create an existing name, rotation), the rule-owned
//! deletion probes (`ledger_delete_race`, `ledger_recreate_probe`) and the
//! oracle-validated ledger reads (`get_ledger`, `list_ledgers`,
//! `schema_status`).
//!
//! Ports `M/actions.go:286-338` generateLifecycle with the reservations of
//! `M/checker.go:287-312`, `MD/parallel_driver_concurrent_ledger_delete`,
//! `MD/parallel_driver_ledger_recreate`, `MD/parallel_driver_delete_ledger`,
//! `MD/parallel_driver_ledger_create` and the old hegel `ledger_lifecycle`
//! (`H/main.rs:841-901`).
//!
//! Invariants kept for the other families (DESIGN §3.1, task 04 spec): l1 and
//! l2 are never deleted; live non-reserved normal ledgers stay in
//! `[2, LIVE_TARGET]` (+ ≤ 2 mirrors); reserved ledgers `o-<run>-own<N>` are
//! invisible to the `ledgers` view, never count, and are deleted by the probe
//! that created them.
use crate::assertions::scoped_inheriting;
use crate::clients::{CallOpts, Outcome, Route};
use crate::draw;
use crate::machine::{rule, Driver};
use crate::read::ViewReq;
use crate::setup::LIVE_TARGET;
use crate::wire::{apply_logs, created_transaction, create_tx_request, Posting};
use crate::write::{SlotGrant, WritePlan, WriteResult};
use base64::Engine;
use hegel::stateful::ConcurrentRule;
use hegel::TestCase;
use serde_json::{json, Value};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

pub fn rules() -> Vec<ConcurrentRule<Driver>> {
    vec![
        rule("ledger_lifecycle", 1.0, ledger_lifecycle),
        rule("ledger_delete_race", 0.4, ledger_delete_race),
        rule("ledger_recreate_probe", 0.4, ledger_recreate_probe),
        rule("get_ledger", 1.5, get_ledger),
        rule("list_ledgers", 1.0, list_ledgers),
        rule("schema_status", 0.7, schema_status),
    ]
}

/// Mirror ledgers kept live at most.
pub const MAX_MIRRORS: usize = 2;
/// Live normal ledgers never drop below this.
pub const MIN_LIVE: usize = 2;
/// A non-reserved ledger at or above this many transactions is rotated out.
pub const ROTATION_TXS: u64 = 3000;
/// The chain budget of a rule-owned deletion probe's writes.
pub const CHAIN_BUDGET: Duration = Duration::from_secs(12);

/// Lifecycle operations in flight (soft reservations, `M/checker.go`
/// reserveLedgerCreate), so concurrent rules do not overshoot the bounds.
#[derive(Default)]
pub struct LedgersExt {
    pub pending_creates: AtomicU64,
    pub pending_deletes: AtomicU64,
}

fn names(v: &Value, key: &str) -> Vec<String> {
    v.get(key).and_then(Value::as_array).map(|a| a.iter().filter_map(Value::as_str).map(str::to_string).collect()).unwrap_or_default()
}

fn reserved(run: &str, name: &str) -> bool {
    name.starts_with(&format!("o-{run}-own"))
}

fn protected(run: &str, name: &str) -> bool {
    name == format!("o-{run}-l1") || name == format!("o-{run}-l2")
}

// ---------------------------------------------------------------- lifecycle

/// What the lifecycle rule sees (the `ledgers` view plus the transaction
/// counts of the rotation candidates).
#[derive(Clone, Debug, Default)]
pub struct LifecycleInput {
    pub live: Vec<String>,
    pub deleted: Vec<String>,
    pub mirror: Vec<String>,
    /// txCount of live non-protected ledgers.
    pub tx_counts: Vec<(String, u64)>,
    pub pending_creates: u64,
    pub pending_deletes: u64,
}

#[derive(Clone, Debug, PartialEq)]
pub enum LifecycleAction {
    CreateNormal { meta: Vec<(String, Value)> },
    CreateMirror { meta: Vec<(String, Value)> },
    Recreate(String),
    CreateExisting(String),
    Delete { name: String, rotation: bool },
    Promote(String),
}

fn draw_ledger_meta(tc: &TestCase) -> Vec<(String, Value)> {
    let n = draw::int_in(tc, 1, 2);
    let mut out: Vec<(String, Value)> = Vec::new();
    for _ in 0..n {
        let k = draw::meta_key_model(tc);
        let v = draw::meta_value(tc);
        if !out.iter().any(|(x, _)| *x == k) {
            out.push((k, v));
        }
    }
    out
}

/// The lifecycle decision (all draws). Reserved names in the input are
/// ignored; l1/l2 are never deleted; nothing is deleted at `MIN_LIVE`; nothing
/// is created at `LIVE_TARGET`.
pub fn choose_lifecycle(tc: &TestCase, run: &str, input: &LifecycleInput) -> Option<LifecycleAction> {
    let keep = |v: &Vec<String>| v.iter().filter(|n| !reserved(run, n)).cloned().collect::<Vec<_>>();
    let (live, deleted, mirror) = (keep(&input.live), keep(&input.deleted), keep(&input.mirror));
    let target = LIVE_TARGET as usize;
    if live.len() + (input.pending_creates as usize) < target {
        let meta = draw_ledger_meta(tc);
        if mirror.len() < MAX_MIRRORS && draw::chance(tc, 0.25) {
            return Some(LifecycleAction::CreateMirror { meta });
        }
        return Some(LifecycleAction::CreateNormal { meta });
    }
    if !deleted.is_empty() && draw::chance(tc, 1.0 / 32.0) {
        return Some(LifecycleAction::Recreate(draw::one_of(tc, &deleted).clone()));
    }
    if !live.is_empty() && draw::chance(tc, 1.0 / 16.0) {
        return Some(LifecycleAction::CreateExisting(draw::one_of(tc, &live).clone()));
    }
    let can_delete = live.len().saturating_sub(input.pending_deletes as usize) > MIN_LIVE;
    let deletable: Vec<String> = live.iter().filter(|n| !protected(run, n)).cloned().collect();
    let rotating: Vec<String> = input.tx_counts.iter().filter(|(n, c)| *c >= ROTATION_TXS && deletable.contains(n)).map(|(n, _)| n.clone()).collect();
    if can_delete && !rotating.is_empty() && draw::chance(tc, 0.5) {
        return Some(LifecycleAction::Delete { name: draw::one_of(tc, &rotating).clone(), rotation: true });
    }
    if can_delete && !deletable.is_empty() && draw::chance(tc, 0.25) {
        return Some(LifecycleAction::Delete { name: draw::one_of(tc, &deletable).clone(), rotation: false });
    }
    if !mirror.is_empty() && draw::chance(tc, 0.25) {
        return Some(LifecycleAction::Promote(draw::one_of(tc, &mirror).clone()));
    }
    None
}

fn meta_json(meta: &[(String, Value)]) -> Value {
    let mut m = serde_json::Map::new();
    for (k, v) in meta {
        m.insert(k.clone(), v.clone());
    }
    Value::Object(m)
}

/// The single request of a lifecycle action. Names of new ledgers come from
/// the shared ledger counter (`o-<run>-l<N>` / `o-<run>-m<N>`).
pub fn lifecycle_request(d: &Driver, action: &LifecycleAction) -> Value {
    match action {
        LifecycleAction::CreateNormal { meta } => json!({"createLedger": {"name": d.s.next_ledger_name(), "metadata": meta_json(meta)}}),
        LifecycleAction::CreateMirror { meta } => {
            let name = format!("o-{}-m{}", d.s.run, d.s.next_ledger.fetch_add(1, Ordering::SeqCst));
            json!({"createLedger": {"name": name, "mode": "LEDGER_MODE_MIRROR", "metadata": meta_json(meta), "mirrorSource": {"ledgerName": "unused"}}})
        }
        LifecycleAction::Recreate(name) | LifecycleAction::CreateExisting(name) => json!({"createLedger": {"name": name}}),
        LifecycleAction::Delete { name, .. } => json!({"deleteLedger": {"name": name}}),
        LifecycleAction::Promote(name) => json!({"promoteLedger": {"ledger": name}}),
    }
}

/// Single-request lifecycle bulks keeping the live pool in bounds.
pub fn ledger_lifecycle(d: &Driver, tc: TestCase) {
    d.guarded("ledger_lifecycle", &tc, |d, tc| {
        if !d.s.oracle_ok() {
            return None;
        }
        let run = d.s.run.clone();
        let lv = d.view(&[ViewReq::ledgers()])?.into_iter().next()?;
        let ext = d.s.ext::<LedgersExt>();
        let mut input = LifecycleInput {
            live: names(&lv, "live"),
            deleted: names(&lv, "deleted"),
            mirror: names(&lv, "mirror"),
            tx_counts: vec![],
            pending_creates: ext.pending_creates.load(Ordering::SeqCst),
            pending_deletes: ext.pending_deletes.load(Ordering::SeqCst),
        };
        let candidates: Vec<String> = input.live.iter().filter(|n| !protected(&run, n) && !reserved(&run, n)).cloned().collect();
        if input.live.len() >= LIVE_TARGET as usize && !candidates.is_empty() {
            let views: Vec<ViewReq> = candidates.iter().map(|n| ViewReq::ledger(crate::read::LedgerSel::Name(n.clone()))).collect();
            if let Some(vs) = d.view(&views) {
                input.tx_counts = candidates.iter().zip(vs.iter()).map(|(n, v)| (n.clone(), v.get("txCount").and_then(Value::as_u64).unwrap_or(0))).collect();
            }
        }
        let action = choose_lifecycle(tc, &run, &input)?;
        let mut plan = WritePlan::drawn("ledgers", vec![], d, tc);
        let unit = d.s.slots.try_acquire_up_to(1)?.take_one()?;
        plan.requests = vec![lifecycle_request(d, &action)];
        plan.hint = json!({"gen": "ledger_lifecycle", "action": format!("{action:?}").split([' ', '(', '{']).next().unwrap_or("")});
        let counter = match &action {
            LifecycleAction::CreateNormal { .. } => Some(&ext.pending_creates),
            LifecycleAction::Delete { .. } => Some(&ext.pending_deletes),
            _ => None,
        };
        if let Some(c) = counter {
            c.fetch_add(1, Ordering::SeqCst);
        }
        let r = d.dispatch(plan, unit);
        if let Some(c) = counter {
            c.fetch_sub(1, Ordering::SeqCst);
        }
        if let LifecycleAction::Delete { rotation: true, name } = &action {
            println!("driver: ledger_lifecycle rotated {name} out: {r:?}");
        }
        Some(())
    });
}

// ---------------------------------------------------------------- probes

/// A forced `world → dest` transaction (100 USD/2), optionally referenced.
fn forced_tx(ledger: &str, dest: &str, amount: u64, reference: Option<&str>) -> Value {
    let mut r = create_tx_request(ledger, &[Posting::new("world", dest, amount, "USD/2")], reference);
    r["apply"]["action"]["createTransaction"]["force"] = json!(true);
    r
}

/// A probe plan: drawn schedule and backoffs, never flagged (`trackKey`
/// stays off for rule-owned writes; `replayable` excludes them anyway).
fn probe_plan(d: &Driver, tc: &TestCase, family: &'static str, reqs: Vec<Value>) -> WritePlan {
    let mut p = WritePlan::new(family, reqs);
    p.schedule = draw::schedule(tc, &d.s, 8);
    p.backoffs_ms = draw::backoffs(tc, 8);
    p
}

/// The outcome of one step of a probe's write chain.
#[derive(Debug)]
pub enum Step {
    Ok(Outcome),
    Failed(Outcome),
    /// No slot, no time left, refused, or handed off: the rest is inconclusive.
    Inconclusive,
}

fn step_of(r: WriteResult) -> Step {
    match r {
        WriteResult::Definitive { outcome, .. } if outcome.is_ok() => Step::Ok(outcome),
        WriteResult::Definitive { outcome, .. } => Step::Failed(outcome),
        _ => Step::Inconclusive,
    }
}

/// Runs one tracked write of a chain within what is left of the deadline.
fn chain_step(d: &Driver, plan: WritePlan, deadline: Instant) -> Step {
    let left = deadline.saturating_duration_since(Instant::now());
    if left.is_zero() {
        return Step::Inconclusive;
    }
    let Some(unit) = d.s.slots.try_acquire_up_to(1).and_then(|mut g| g.take_one()) else {
        return Step::Inconclusive;
    };
    match d.begin_write(plan, unit) {
        Ok(t) => step_of(t.run(left)),
        Err(_) => Step::Inconclusive,
    }
}

/// `dispatch_many` with one budget for every plan (one unit each).
fn dispatch_many_within(d: &Driver, plans: Vec<WritePlan>, grant: SlotGrant, budget: Duration) -> Vec<WriteResult> {
    let units = grant.into_units();
    std::thread::scope(|s| {
        let hs: Vec<_> = plans
            .into_iter()
            .zip(units)
            .map(|(p, u)| {
                scoped_inheriting(s, move || match d.begin_write(p, u) {
                    Ok(t) => t.run(budget),
                    Err(e) => WriteResult::Refused(e),
                })
            })
            .collect();
        hs.into_iter().map(|h| h.join().unwrap_or_else(|e| std::panic::resume_unwind(e))).collect()
    })
}

/// A reserved ledger, writes, then one delete racing `k` writes; after the
/// delete's definitive success a sequential write must be refused
/// (`MD/parallel_driver_concurrent_ledger_delete`).
pub fn ledger_delete_race(d: &Driver, tc: TestCase) {
    d.guarded("ledger_delete_race", &tc, |d, tc| {
        if !d.s.oracle_ok() {
            return None;
        }
        let start = Instant::now();
        let deadline = start + CHAIN_BUDGET;
        let ledger = d.s.next_own_ledger();
        let n = d.s.next_own();
        let addr = |i: u64| format!("users:own-dr{n}:{i}");
        // Draws first.
        let create = probe_plan(d, tc, "ledgers", vec![json!({"createLedger": {"name": ledger}})]);
        let n_pre = draw::int_in(tc, 1, 3);
        let pre: Vec<Value> = (0..n_pre).map(|i| forced_tx(&ledger, &addr(i), draw::int_in(tc, 1, 1000), None)).collect();
        let pre = probe_plan(d, tc, "ledgers", pre);
        let delete_req = json!({"deleteLedger": {"name": ledger}});
        let delete = probe_plan(d, tc, "ledgers", vec![delete_req.clone()]);
        let conc: Vec<WritePlan> = (0..5).map(|i| probe_plan(d, tc, "ledgers", vec![forced_tx(&ledger, &addr(10 + i), draw::int_in(tc, 1, 1000), None)])).collect();
        let post = probe_plan(d, tc, "ledgers", vec![forced_tx(&ledger, &addr(20), draw::int_in(tc, 1, 1000), None)]);
        let cleanup = probe_plan(d, tc, "ledgers", vec![delete_req]);

        // EXEC.
        if !matches!(chain_step(d, create, deadline), Step::Ok(_)) {
            return None;
        }
        let _ = chain_step(d, pre, deadline);
        let mut g = match d.s.slots.try_acquire_up_to(6) {
            Some(g) if g.len() >= 2 => g,
            other => {
                // No race possible now: still delete what the probe created.
                drop(other);
                let _ = chain_step(d, cleanup, deadline);
                return None;
            }
        };
        let k = draw::int_in(tc, 1, g.len() as u64 - 1) as usize;
        g.split(1 + k);
        let left = deadline.saturating_duration_since(Instant::now());
        if left.is_zero() {
            return None;
        }
        let mut plans = vec![delete];
        plans.extend(conc.into_iter().take(k));
        let results = dispatch_many_within(d, plans, g, left);
        let deleted = match step_of_ref(&results[0]) {
            Some(Ok(())) => true,
            Some(Err(out)) => {
                if !out.reason.starts_with("LEDGER_") {
                    check_unreachable!("delete ledger should not fail unexpectedly", json!({"ledger": ledger, "outcome": out.summary()}));
                }
                false
            }
            None => false,
        };
        if !deleted {
            return None;
        }
        // Sequential write after the delete's definitive success.
        match chain_step(d, post, deadline) {
            Step::Ok(out) => {
                check_unreachable!("write after delete should fail", json!({"ledger": ledger, "concurrent": k, "outcome": out.summary()}));
            }
            Step::Failed(out) => {
                check_always_or_unreachable!(
                    out.reason == "LEDGER_DELETED" || out.reason == "LEDGER_NOT_FOUND",
                    "write to deleted ledger should return LEDGER_DELETED or LEDGER_NOT_FOUND",
                    json!({"ledger": ledger, "outcome": out.summary(), "concurrent": k})
                );
                check_reachable!("concurrent ledger delete: post-delete write rejected", json!({"ledger": ledger}));
            }
            Step::Inconclusive => {}
        }
        Some(())
    });
}

fn step_of_ref(r: &WriteResult) -> Option<Result<(), Outcome>> {
    match r {
        WriteResult::Definitive { outcome, .. } if outcome.is_ok() => Some(Ok(())),
        WriteResult::Definitive { outcome, .. } => Some(Err(outcome.clone())),
        _ => None,
    }
}

/// A read outcome that says nothing (retried).
fn transient(o: &Outcome) -> bool {
    o.is_unknown() || matches!(o.code.as_str(), "Unavailable" | "DeadlineExceeded" | "Canceled")
}

/// A Rust-direct linearizable read: one attempt plus ≤ 3 retries on
/// transient outcomes; `None` if every attempt was transient.
fn direct_read(d: &Driver, route: &Route, method: &str, req: Value, max_items: Option<u64>) -> Option<Outcome> {
    let mut opts = CallOpts::linearizable();
    opts.max_items = max_items;
    for _ in 0..4 {
        let (_, o) = d.call(route, "bucket", method, req.clone(), &opts);
        if !transient(&o) {
            return Some(o);
        }
    }
    None
}

fn is_not_found(o: &Outcome) -> bool {
    !o.ok && !o.is_unknown() && o.code == "NotFound"
}

fn is_deleted_refusal(o: &Outcome) -> bool {
    !o.ok && o.code == "FailedPrecondition" && o.reason == "LEDGER_DELETED"
}

/// Transaction ids of an Apply response's created transactions.
fn created_ids(o: &Outcome) -> Vec<u64> {
    o.response.as_ref().map(apply_logs).unwrap_or_default().iter().filter_map(|l| created_transaction(l)).filter_map(|t| crate::wire::as_u64(t.get("id")?)).collect()
}

/// The permanent-deletion contract on a reserved ledger
/// (`MD/parallel_driver_ledger_recreate`): a deleted name stays reserved,
/// hides every predecessor read, refuses writes; another ledger never
/// exposes the predecessor and may reuse its references.
pub fn ledger_recreate_probe(d: &Driver, tc: TestCase) {
    d.guarded("ledger_recreate_probe", &tc, |d, tc| {
        if !d.s.oracle_ok() {
            return None;
        }
        let deadline = Instant::now() + CHAIN_BUDGET;
        let ledger = d.s.next_own_ledger();
        let n = d.s.next_own();
        // Draws first.
        let count = if draw::chance(tc, 0.5) { 3 } else { 5 };
        let refs: Vec<String> = (0..count).map(|_| d.s.next_own_ref("ref")).collect();
        let accounts: Vec<String> = (0..count).map(|i| format!("users:own-rc{n}:{i}")).collect();
        let create = probe_plan(d, tc, "ledgers", vec![json!({"createLedger": {"name": ledger}})]);
        let seeds = probe_plan(d, tc, "ledgers", refs.iter().zip(&accounts).map(|(r, a)| forced_tx(&ledger, a, 100, Some(r))).collect());
        let delete = probe_plan(d, tc, "ledgers", vec![json!({"deleteLedger": {"name": ledger}})]);
        let deleted_write = probe_plan(d, tc, "ledgers", vec![forced_tx(&ledger, &format!("users:own-rca{n}"), 100, None)]);
        let recreate = probe_plan(d, tc, "ledgers", vec![json!({"createLedger": {"name": ledger}})]);
        let other = d.s.next_own_ledger();
        let create_other = probe_plan(d, tc, "ledgers", vec![json!({"createLedger": {"name": other}}), forced_tx(&other, &format!("users:own-rcm{n}"), 100, None)]);
        let reuse = probe_plan(d, tc, "ledgers", vec![forced_tx(&other, &format!("users:own-rcn{n}:0"), 100, Some(&refs[0]))]);
        let delete_other = probe_plan(d, tc, "ledgers", vec![json!({"deleteLedger": {"name": other}})]);
        let route = draw::route(tc, &d.s);
        let details = json!({"ledger": ledger, "otherLedger": other});

        // Predecessor: create, seed, delete.
        if !matches!(chain_step(d, create, deadline), Step::Ok(_)) {
            return None;
        }
        let ids = match chain_step(d, seeds, deadline) {
            Step::Ok(o) => created_ids(&o),
            Step::Failed(_) => vec![],
            Step::Inconclusive => return None,
        };
        if !matches!(chain_step(d, delete, deadline), Step::Ok(_)) {
            return None;
        }

        // Linearizable reads of the deleted ledger: NotFound everywhere.
        let o = direct_read(d, &route, "GetLedger", json!({"ledger": ledger}), None)?;
        check_always!(is_not_found(&o), "deleted ledger is hidden from ledger reads", json!({"ledger": ledger, "outcome": o.summary()}));
        if !is_not_found(&o) {
            return None;
        }
        for id in &ids {
            let o = direct_read(d, &route, "GetTransaction", json!({"ledger": ledger, "transactionId": id.to_string()}), None)?;
            check_always!(is_not_found(&o), "deleted ledger hides predecessor transaction point reads", json!({"ledger": ledger, "transactionId": id, "outcome": o.summary()}));
            if !is_not_found(&o) {
                return None;
            }
        }
        for a in &accounts {
            let o = direct_read(d, &route, "GetAccount", json!({"ledger": ledger, "address": a}), None)?;
            check_always!(is_not_found(&o), "deleted ledger hides predecessor account point reads", json!({"ledger": ledger, "account": a, "outcome": o.summary()}));
            if !is_not_found(&o) {
                return None;
            }
        }
        let o = direct_read(d, &route, "ListTransactions", json!({"ledger": ledger, "options": {"pageSize": 10}}), Some(1))?;
        check_always!(is_not_found(&o), "deleted ledger exposes no predecessor transactions", json!({"ledger": ledger, "outcome": o.summary()}));
        if !is_not_found(&o) {
            return None;
        }
        let o = direct_read(d, &route, "ListAccounts", json!({"ledger": ledger, "options": {"pageSize": 10}}), Some(1))?;
        check_always!(is_not_found(&o), "deleted ledger exposes no predecessor accounts", json!({"ledger": ledger, "outcome": o.summary()}));
        if !is_not_found(&o) {
            return None;
        }

        // A write and a recreate through the oracle: refused as deleted.
        match chain_step(d, deleted_write, deadline) {
            Step::Inconclusive => return None,
            Step::Ok(o) | Step::Failed(o) => {
                check_always!(is_deleted_refusal(&o), "deleted ledger rejects new transaction writes", json!({"ledger": ledger, "outcome": o.summary()}));
            }
        }
        match chain_step(d, recreate, deadline) {
            Step::Inconclusive => return None,
            Step::Ok(o) | Step::Failed(o) => {
                check_always!(is_deleted_refusal(&o), "deleted ledger name remains permanently reserved", json!({"ledger": ledger, "outcome": o.summary()}));
            }
        }

        // Another reserved ledger: isolation, then reference reuse.
        if !matches!(chain_step(d, create_other, deadline), Step::Ok(_)) {
            return None;
        }
        let mut cleanup = Some(delete_other);
        let result = (|| {
            let o = direct_read(d, &route, "ListTransactions", json!({"ledger": other, "options": {"pageSize": 100}}), Some(101))?;
            if o.is_ok() {
                let items = o.items.clone().unwrap_or_default();
                let leaked_tx = items.iter().any(|t| t.get("reference").and_then(Value::as_str).is_some_and(|r| refs.iter().any(|x| x == r)));
                check_always!(!leaked_tx, "other ledger never exposes predecessor transactions", details.clone());
                let leaked_acct = items.iter().flat_map(|t| t.get("postings").and_then(Value::as_array).cloned().unwrap_or_default()).any(|p| {
                    [p.get("source"), p.get("destination")].iter().any(|s| s.and_then(|v| v.as_str()).is_some_and(|s| accounts.iter().any(|a| a == s)))
                });
                let o2 = direct_read(d, &route, "ListAccounts", json!({"ledger": other, "options": {"pageSize": 100}}), Some(101))?;
                if o2.is_ok() {
                    let listed = o2.items.clone().unwrap_or_default().iter().any(|a| a.get("address").and_then(Value::as_str).is_some_and(|s| accounts.iter().any(|x| x == s)));
                    check_always!(!leaked_acct && !listed, "other ledger never exposes predecessor account activity", details.clone());
                }
            }
            match chain_step(d, reuse, deadline) {
                Step::Inconclusive => {}
                Step::Ok(o) | Step::Failed(o) => {
                    let d2 = json!({"ledger": ledger, "otherLedger": other, "reference": refs[0], "outcome": o.summary()});
                    check_sometimes!(o.is_ok(), "predecessor reference accepted by another ledger", d2.clone());
                    check_always!(o.is_ok(), "predecessor references are reusable in another ledger", d2);
                }
            }
            Some(())
        })();
        if let Some(p) = cleanup.take() {
            let _ = chain_step(d, p, deadline.max(Instant::now() + Duration::from_secs(2)));
        }
        result
    });
}

// ---------------------------------------------------------------- reads

/// Where a ledger-scoped read aims: a live ledger, a deleted one, or a
/// never-created absent name.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Target {
    Live,
    Deleted,
    Absent,
}

fn draw_target(tc: &TestCase, absent_pct: f64, deleted_pct: f64) -> Target {
    // `chance` (weighted booleans) rather than a wide integer, which hegel
    // biases toward small values.
    if draw::chance(tc, absent_pct / 100.0) {
        Target::Absent
    } else if draw::chance(tc, deleted_pct / (100.0 - absent_pct)) {
        Target::Deleted
    } else {
        Target::Live
    }
}

/// One ledger-scoped validated read (`GetLedger`, `GetMetadataSchemaStatus`).
fn ledger_scoped_read(d: &Driver, tc: &TestCase, kind: &'static str, absent_pct: f64) -> Option<()> {
    if !d.s.oracle_ok() {
        return None;
    }
    let target = draw_target(tc, absent_pct, 5.0);
    let absent_name = format!("o-{}-absent-{}", d.s.run, draw::ledger_pick(tc));
    let route = draw::route(tc, &d.s);
    let rt = d.read_begin(kind, &[ViewReq::ledgers()], json!({}))?;
    let lv = rt.views().first()?.clone();
    let (live, deleted) = (names(&lv, "live"), names(&lv, "deleted"));
    let (ledger, target) = match target {
        Target::Absent => (absent_name, Target::Absent),
        Target::Deleted if !deleted.is_empty() => (draw::one_of(tc, &deleted).clone(), Target::Deleted),
        _ if !live.is_empty() => (draw::one_of(tc, &live).clone(), Target::Live),
        _ => {
            rt.abandon();
            return None;
        }
    };
    let req = json!({"ledger": ledger});
    let opts = CallOpts::linearizable();
    let (_, out) = d.call(&route, "bucket", kind, req.clone(), &opts);
    rt.end(&req, &out, &route, &opts, json!({"absentLedger": target == Target::Absent, "target": format!("{target:?}")}), json!({}));
    Some(())
}

/// Read kind `GetLedger`: a live (93%), deleted (5%) or absent (2%) ledger.
pub fn get_ledger(d: &Driver, tc: TestCase) {
    d.guarded("get_ledger", &tc, |d, tc| ledger_scoped_read(d, tc, "GetLedger", 2.0));
}

/// Read kind `GetMetadataSchemaStatus`: live, deleted (5%) or absent (3%).
pub fn schema_status(d: &Driver, tc: TestCase) {
    d.guarded("schema_status", &tc, |d, tc| ledger_scoped_read(d, tc, "GetMetadataSchemaStatus", 3.0));
}

/// A forward ledger page token (`pagecursor.Cursor{Key}.Encode()`):
/// base64url without padding of `{"key":…}`.
pub fn ledger_page_token(key: &str) -> String {
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(json!({ "key": key }).to_string())
}

/// Read kind `ListLedgers`: one page with a drawn size and direction; the
/// cursor is none, the tracked range's edge (`o-<run>-` forward, `o-<run>.`
/// reverse), or a live/deleted tracked name; 1/4 a filter the endpoint must
/// refuse.
pub fn list_ledgers(d: &Driver, tc: TestCase) {
    d.guarded("list_ledgers", &tc, |d, tc| {
        if !d.s.oracle_ok() {
            return None;
        }
        let page_size = draw::page_size(tc);
        let reverse = draw::chance(tc, 0.5);
        let cursor_kind = draw::int_in(tc, 0, 2); // 0 none, 1 range edge, 2 a tracked name
        let filter = if draw::chance(tc, 0.25) { Some(json!({"address": {"hardcodedPrefix": draw::pool_address(tc)}})) } else { None };
        let route = draw::route(tc, &d.s);
        let rt = d.read_begin("ListLedgers", &[ViewReq::ledgers()], json!({}))?;
        let lv = rt.views().first()?.clone();
        let key = match cursor_kind {
            0 => None,
            1 if reverse => Some(format!("o-{}.", d.s.run)),
            1 => Some(d.s.tracked_prefix()),
            _ => {
                let mut pool = names(&lv, "live");
                pool.extend(names(&lv, "deleted"));
                if pool.is_empty() {
                    None
                } else {
                    Some(draw::one_of(tc, &pool).clone())
                }
            }
        };
        let mut options = json!({"reverse": reverse});
        if page_size > 0 {
            options["pageSize"] = json!(page_size);
        }
        if let Some(k) = &key {
            options["cursor"] = json!(ledger_page_token(k));
        }
        if let Some(f) = &filter {
            options["filter"] = f.clone();
        }
        let req = json!({ "options": options });
        let opts = CallOpts::linearizable().max_items(1001);
        let (_, out) = d.call(&route, "bucket", "ListLedgers", req.clone(), &opts);
        rt.end(&req, &out, &route, &opts, json!({"cursorKey": key, "filtered": filter.is_some()}), json!({}));
        Some(())
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testkit::{self, FakeReply};

    fn input(live: &[&str], deleted: &[&str], mirror: &[&str], counts: &[(&str, u64)]) -> LifecycleInput {
        let s = |v: &[&str]| v.iter().map(|x| x.to_string()).collect::<Vec<_>>();
        LifecycleInput { live: s(live), deleted: s(deleted), mirror: s(mirror), tx_counts: counts.iter().map(|(n, c)| (n.to_string(), *c)).collect(), pending_creates: 0, pending_deletes: 0 }
    }

    #[test]
    fn lifecycle_invariants() {
        let run = "test";
        let full = input(&["o-test-l1", "o-test-l2", "o-test-l3", "o-test-l4", "o-test-own3"], &["o-test-l9", "o-test-own1"], &["o-test-m7", "o-test-own2"], &[("o-test-l3", 3500), ("o-test-l4", 10)]);
        let at_min = input(&["o-test-l1", "o-test-l2", "o-test-own3"], &[], &[], &[]);
        let below = input(&["o-test-l1", "o-test-l2", "o-test-l5"], &["o-test-l3"], &["o-test-m6", "o-test-m8"], &[]);
        let rotated = std::cell::Cell::new(0);
        let seen_delete_l4 = std::cell::Cell::new(false);
        testkit::with_tc_n(400, |tc| {
            for inp in [&full, &at_min, &below] {
                let live_n = inp.live.iter().filter(|n| !n.contains("own")).count();
                match choose_lifecycle(tc, run, inp) {
                    Some(LifecycleAction::Delete { name, rotation }) => {
                        assert!(name != "o-test-l1" && name != "o-test-l2", "never deletes l1/l2");
                        assert!(!name.contains("own"), "never a reserved ledger");
                        assert!(live_n > MIN_LIVE, "never below {MIN_LIVE} live");
                        if rotation {
                            assert_eq!(name, "o-test-l3", "only a ledger at ≥ {ROTATION_TXS} txs rotates");
                            rotated.set(rotated.get() + 1);
                        } else if name == "o-test-l4" {
                            seen_delete_l4.set(true);
                        }
                    }
                    Some(LifecycleAction::CreateNormal { meta }) | Some(LifecycleAction::CreateMirror { meta }) => {
                        assert!(live_n < LIVE_TARGET as usize, "never creates at the target");
                        assert!((1..=2).contains(&meta.len()));
                    }
                    Some(LifecycleAction::Recreate(n)) => assert_eq!(n, "o-test-l9"),
                    Some(LifecycleAction::CreateExisting(n)) | Some(LifecycleAction::Promote(n)) => assert!(!n.contains("own")),
                    None => {}
                }
            }
            // Below the target: always a create; a mirror only below MAX_MIRRORS.
            let a = choose_lifecycle(tc, run, &below).unwrap();
            assert!(matches!(a, LifecycleAction::CreateNormal { .. }), "two mirrors already: {a:?}");
        });
        assert!(rotated.get() > 0, "rotation reached");
        assert!(seen_delete_l4.get(), "plain deletes reached");
        // Pending creates count toward the target.
        let mut pend = input(&["o-test-l1", "o-test-l2", "o-test-l3"], &[], &[], &[]);
        pend.pending_creates = 1;
        testkit::with_tc_n(50, |tc| {
            assert!(!matches!(choose_lifecycle(tc, run, &pend), Some(LifecycleAction::CreateNormal { .. }) | Some(LifecycleAction::CreateMirror { .. })));
        });
    }

    #[test]
    fn lifecycle_requests_and_names() {
        let (d, _, _) = testkit::driver();
        let meta = vec![("k1".to_string(), json!({"stringValue": "v0"}))];
        let r = lifecycle_request(&d, &LifecycleAction::CreateMirror { meta: meta.clone() });
        assert_eq!(r["createLedger"]["mode"], "LEDGER_MODE_MIRROR");
        assert_eq!(r["createLedger"]["mirrorSource"]["ledgerName"], "unused");
        assert!(r["createLedger"]["name"].as_str().unwrap().starts_with("o-test-m"));
        let r = lifecycle_request(&d, &LifecycleAction::CreateNormal { meta });
        assert!(r["createLedger"]["name"].as_str().unwrap().starts_with("o-test-l"));
        assert_eq!(r["createLedger"]["metadata"]["k1"]["stringValue"], "v0");
        assert_eq!(lifecycle_request(&d, &LifecycleAction::Promote("x".into())), json!({"promoteLedger": {"ledger": "x"}}));
        assert_eq!(lifecycle_request(&d, &LifecycleAction::Delete { name: "x".into(), rotation: false }), json!({"deleteLedger": {"name": "x"}}));
    }

    #[test]
    fn lifecycle_rule_never_targets_protected_or_reserved_ledgers() {
        let (d, _, fo) = testkit::driver();
        fo.set_live(vec!["o-test-l1".into(), "o-test-l2".into(), "o-test-l3".into(), "o-test-l4".into()]);
        fo.set_view("ledgers", json!({"deleted": ["o-test-l9"], "mirror": ["o-test-m5"]}));
        for _ in 0..40 {
            testkit::run_rule(&d, ledger_lifecycle);
        }
        for b in fo.calls("/write/begin") {
            assert_eq!(b["family"], "ledgers");
            assert_eq!(b["requests"].as_array().unwrap().len(), 1);
            let r = &b["requests"][0];
            if let Some(n) = r.pointer("/deleteLedger/name").and_then(Value::as_str) {
                assert!(n != "o-test-l1" && n != "o-test-l2" && !n.contains("own"), "{r}");
            }
            assert!(!r.to_string().contains("own"), "{r}");
        }
        assert_eq!(d.s.ext::<LedgersExt>().pending_creates.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn page_token_matches_go_pagecursor() {
        // pagecursor.Cursor{Key: "o-x-l1"}.Encode()
        assert_eq!(ledger_page_token("o-x-l1"), "eyJrZXkiOiJvLXgtbDEifQ");
    }

    fn ok_create_logs(ids: &[u64]) -> Outcome {
        let logs: Vec<Value> = ids.iter().map(|i| json!({"sequence": i.to_string(), "payload": {"apply": {"log": {"data": {"createdTransaction": {"transaction": {"id": i.to_string()}}}}}}})).collect();
        Outcome::from_json(&json!({"ok": true, "response": {"logs": logs}}))
    }

    fn fail(code: &str, reason: &str) -> FakeReply {
        FakeReply::Outcome(Outcome::from_json(&json!({"ok": false, "code": code, "reason": reason})))
    }

    #[test]
    fn delete_race_post_delete_write_rejected() {
        // Five general units held: the race gets exactly two (the delete and
        // one write), so k = 1 without a draw.
        let (d, ft, _fo) = testkit::driver();
        testkit::reset();
        let held = d.s.slots.try_acquire_up_to(5).unwrap();
        ft.script("bucket", "Apply", vec![FakeReply::Outcome(ok_create_logs(&[1])); 4]);
        ft.set_default("bucket", "Apply", fail("FailedPrecondition", "LEDGER_DELETED"));
        testkit::run_rule(&d, ledger_delete_race);
        let a = testkit::assertions();
        assert!(a.iter().any(|r| r.message == "write to deleted ledger should return LEDGER_DELETED or LEDGER_NOT_FOUND" && r.cond));
        assert!(testkit::fired("concurrent ledger delete: post-delete write rejected"));
        assert!(!testkit::fired("write after delete should fail"));
        assert!(!testkit::fired("delete ledger should not fail unexpectedly"));
        assert!(d.s.registry.snapshot().is_empty());
        drop(held);
        assert_eq!(d.s.slots.in_use(), (0, false));

        // A post-delete write that commits is the bug the rule reports.
        let (d, ft, fo) = testkit::driver();
        testkit::reset();
        let held = d.s.slots.try_acquire_up_to(5).unwrap();
        ft.set_default("bucket", "Apply", FakeReply::Outcome(ok_create_logs(&[1])));
        testkit::run_rule(&d, ledger_delete_race);
        assert!(testkit::fired("write after delete should fail"));
        let begins = fo.calls("/write/begin");
        assert!(begins.iter().all(|b| b.to_string().contains("o-test-own")), "only the reserved ledger is touched");
        assert_eq!(begins.iter().filter(|b| b.pointer("/requests/0/deleteLedger").is_some()).count(), 1);
        drop(held);

        // A delete refused for a non-ledger reason is reported.
        let (d, ft, _fo) = testkit::driver();
        testkit::reset();
        let held = d.s.slots.try_acquire_up_to(5).unwrap();
        ft.script("bucket", "Apply", vec![FakeReply::Outcome(ok_create_logs(&[1])); 2]);
        ft.set_default("bucket", "Apply", fail("InvalidArgument", "VALIDATION"));
        testkit::run_rule(&d, ledger_delete_race);
        assert!(testkit::fired("delete ledger should not fail unexpectedly"));
        drop(held);
    }

    #[test]
    fn recreate_probe_asserts_the_deletion_contract() {
        let (d, ft, fo) = testkit::driver();
        testkit::reset();
        // create, seeds, delete commit; reads NotFound; deleted write and
        // recreate refused; other ledger created; lists empty; reuse commits;
        // delete other commits.
        ft.script("bucket", "Apply", vec![FakeReply::Outcome(ok_create_logs(&[])), FakeReply::Outcome(ok_create_logs(&[1, 2, 3, 4, 5])), FakeReply::Outcome(ok_create_logs(&[]))]);
        ft.script("bucket", "Apply", vec![fail("FailedPrecondition", "LEDGER_DELETED"), fail("FailedPrecondition", "LEDGER_DELETED")]);
        ft.set_default("bucket", "Apply", FakeReply::Outcome(ok_create_logs(&[1])));
        for (m, n) in [("GetLedger", 1), ("GetTransaction", 5), ("GetAccount", 5), ("ListTransactions", 1), ("ListAccounts", 1)] {
            ft.script("bucket", m, vec![FakeReply::Outcome(Outcome::from_json(&json!({"ok": false, "code": "NotFound"}))); n]);
        }
        ft.script("bucket", "ListTransactions", vec![FakeReply::Outcome(Outcome::from_json(&json!({"ok": true, "items": [{"reference": "x", "postings": [{"source": "world", "destination": "users:own-rcm1"}]}], "complete": true})))]);
        ft.script("bucket", "ListAccounts", vec![FakeReply::Outcome(Outcome::from_json(&json!({"ok": true, "items": [{"address": "world"}], "complete": true})))]);
        testkit::run_rule(&d, ledger_recreate_probe);
        let a = testkit::assertions();
        for lit in [
            "deleted ledger is hidden from ledger reads",
            "deleted ledger hides predecessor transaction point reads",
            "deleted ledger hides predecessor account point reads",
            "deleted ledger exposes no predecessor transactions",
            "deleted ledger exposes no predecessor accounts",
            "deleted ledger rejects new transaction writes",
            "deleted ledger name remains permanently reserved",
            "other ledger never exposes predecessor transactions",
            "other ledger never exposes predecessor account activity",
            "predecessor references are reusable in another ledger",
            "predecessor reference accepted by another ledger",
        ] {
            let hits: Vec<_> = a.iter().filter(|r| r.message == lit).collect();
            assert!(!hits.is_empty(), "{lit} not evaluated");
            assert!(hits.iter().all(|r| r.cond), "{lit} failed: {:?}", hits);
        }
        // Every write went through the oracle on reserved ledgers.
        let begins = fo.calls("/write/begin");
        let creates: Vec<_> = begins.iter().filter_map(|b| b.pointer("/requests/0/createLedger/name").and_then(Value::as_str)).collect();
        assert!(creates.iter().all(|n| n.starts_with("o-test-own")), "{creates:?}");
        let deletes = begins.iter().filter(|b| b.pointer("/requests/0/deleteLedger").is_some()).count();
        assert_eq!(deletes, 2, "both reserved ledgers are deleted");
        assert!(d.s.registry.snapshot().is_empty());
        // The reused reference is the predecessor's first one.
        let reuse = begins.iter().rev().find(|b| b.pointer("/requests/0/apply/action/createTransaction/reference").is_some()).unwrap();
        assert!(reuse["requests"][0]["apply"]["action"]["createTransaction"]["reference"].as_str().unwrap().starts_with("o-test-own-ref"));
    }

    #[test]
    fn reads_post_their_kind_with_the_request_as_sent() {
        let (d, _, fo) = testkit::driver();
        fo.set_view("ledgers", json!({"deleted": ["o-test-l9"]}));
        for _ in 0..30 {
            testkit::run_rule(&d, get_ledger);
            testkit::run_rule(&d, schema_status);
            testkit::run_rule(&d, list_ledgers);
        }
        let ends = fo.calls("/read/end");
        let mut kinds = std::collections::BTreeSet::new();
        for e in &ends {
            kinds.insert(e["kind"].as_str().unwrap().to_string());
            assert_eq!(e["route"]["consistency"], "linearizable");
            match e["kind"].as_str().unwrap() {
                "GetLedger" | "GetMetadataSchemaStatus" => {
                    let l = e["request"]["ledger"].as_str().unwrap();
                    assert!(l.starts_with("o-test-"), "{l}");
                    assert!(!l.contains("own"));
                }
                "ListLedgers" => {
                    if let Some(c) = e["request"]["options"]["cursor"].as_str() {
                        let raw = base64::engine::general_purpose::URL_SAFE_NO_PAD.decode(c).unwrap();
                        let v: Value = serde_json::from_slice(&raw).unwrap();
                        assert!(v["key"].as_str().unwrap().starts_with("o-test"));
                    }
                }
                k => panic!("unexpected kind {k}"),
            }
        }
        assert_eq!(kinds.len(), 3, "{kinds:?}");
        assert!(fo.open_reads().is_empty());
        assert!(d.s.registry.snapshot().is_empty());
    }
}
