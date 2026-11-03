//! Task 04: the maintenance window (ports `MD/parallel_driver_maintenance`,
//! the old hegel admin maintenance branch `H/main.rs:938-958`, and the
//! singleton model's driver-side recovery `M/main.go:440-555`).
//!
//! `maintenance_window` enables maintenance (cluster-scoped, tracked) with up
//! to three concurrent tracked chart writes. The enable carries a follow-up
//! that 00c runs as a quiet-blocking job on the enable's definitive success,
//! wherever it is observed (rule, resolver, late outcome): it opens
//! `Shared.maint`, probes `r-maint-<run>` with raw writes, waits the pre-drawn
//! delay, and sends the tracked disable on the reserved maintenance slot. The
//! disable's own follow-up closes `Shared.maint` and probes a write.
//! At most one window is open at a time: `Shared.maint` plus a claim flag
//! taken by the rule before the enable and released when the window closes.
use crate::clients::{Outcome, Route};
use crate::draw;
use crate::machine::{rule, Driver};
use crate::oracle::OracleError;
use crate::setup::SetupHook;
use crate::shared::MaintState;
use crate::wire::{create_tx_request, Posting};
use crate::write::{compose_bulk, FollowUp, UntrackedSend, WritePlan, WriteResult};
use hegel::stateful::ConcurrentRule;
use hegel::TestCase;
use serde_json::{json, Value};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

pub fn rules() -> Vec<ConcurrentRule<Driver>> {
    vec![rule("maintenance_window", 0.4, maintenance_window)]
}

pub fn setup_hooks() -> Vec<SetupHook> {
    vec![SetupHook { name: "maint_setup", run: maint_setup }]
}

/// The disable's budgets (DESIGN §4.8: the reserved unit waits ≤ 30 s, the
/// disable runs 30 s before a resolver takes it over).
pub const MAINT_ACQUIRE: Duration = Duration::from_secs(30);
pub const DISABLE_BUDGET: Duration = Duration::from_secs(30);
pub const POST_PROBE_BUDGET: Duration = Duration::from_secs(30);

/// The Rust-checked ledger the maintenance probes write to.
pub fn maint_ledger(run: &str) -> String {
    format!("r-maint-{run}")
}

/// Family state: the window claim (one window at a time).
#[derive(Default)]
pub struct MaintExt {
    pub claimed: AtomicBool,
}

fn release_claim(d: &Driver) {
    d.s.ext::<MaintExt>().claimed.store(false, Ordering::SeqCst);
}

/// Setup hook (no draws): create `r-maint-<run>`, untracked, keyed, retried.
pub fn maint_setup(d: &Driver) {
    let name = maint_ledger(&d.s.run);
    let key = d.s.next_untracked_key();
    for i in 0..60 {
        let mut send = UntrackedSend::new(vec![json!({"createLedger": {"name": name}})], Some(key.clone()));
        send.schedule = (0..d.s.clients.len().max(1)).map(Route::lib).collect();
        let r = d.send_untracked(send);
        if r.outcome.is_ok() || r.outcome.reason == "LEDGER_ALREADY_EXISTS" {
            println!("setup: maint_setup: {name} ready");
            return;
        }
        println!("setup: maint_setup: creating {name} (attempt {i}): {}", r.outcome.summary());
        std::thread::sleep(Duration::from_millis(500));
    }
    println!("setup: maint_setup: {name} not created; maintenance probes will see NotFound");
}

/// One raw probe write to the maintenance ledger.
#[derive(Clone, Debug)]
pub struct Probe {
    pub request: Value,
    pub route: Route,
}

fn draw_probe(tc: &TestCase, d: &Driver) -> Probe {
    let dest = draw::users_address(tc);
    let amount = draw::int_in(tc, 1, 1000);
    let mut request = create_tx_request(&maint_ledger(&d.s.run), &[Posting::new("world", &dest, amount, "COIN")], None);
    request["apply"]["action"]["createTransaction"]["force"] = json!(true);
    Probe { request, route: draw::route(tc, &d.s) }
}

/// Everything the follow-ups need, drawn in the rule.
#[derive(Clone, Debug)]
pub struct WindowData {
    pub delay_ms: u64,
    pub probes: Vec<Probe>,
    pub post_probe: Probe,
    pub post_backoffs: Vec<u64>,
    pub disable_schedule: Vec<Route>,
    pub disable_backoffs: Vec<u64>,
}

impl WindowData {
    pub fn draw(tc: &TestCase, d: &Driver) -> WindowData {
        let delay_ms = draw::int_in(tc, 0, 3000);
        let n = draw::int_in(tc, 1, 2) as usize;
        let probes = (0..n).map(|_| draw_probe(tc, d)).collect();
        let post_probe = draw_probe(tc, d);
        WindowData { delay_ms, probes, post_probe, post_backoffs: draw::backoffs(tc, 8), disable_schedule: draw::schedule(tc, &d.s, 8), disable_backoffs: draw::backoffs(tc, 8) }
    }
}

pub fn maintenance_request(enabled: bool) -> Value {
    json!({"setMaintenanceMode": {"enabled": enabled}})
}

/// The enable plan: tracked, cluster-scoped, with the window follow-up.
pub fn enable_plan(d: &Driver, tc: &TestCase, data: WindowData) -> WritePlan {
    let mut p = WritePlan::drawn("maintenance", vec![maintenance_request(true)], d, tc);
    p.hint = json!({"gen": "maintenance_enable"});
    p.follow_up = Some(enable_follow_up(data));
    p
}

/// The enable's follow-up (a quiet-blocking job, DESIGN §4.9).
pub fn enable_follow_up(data: WindowData) -> FollowUp {
    Box::new(move |d: &Driver, _o: &Outcome| run_enable_follow_up(d, data))
}

/// The disable's follow-up (a quiet-blocking job).
pub fn disable_follow_up(data: WindowData) -> FollowUp {
    Box::new(move |d: &Driver, _o: &Outcome| run_disable_follow_up(d, &data))
}

fn disable_plan(data: &WindowData) -> WritePlan {
    let mut p = WritePlan::new("maintenance", vec![maintenance_request(false)]);
    p.schedule = data.disable_schedule.clone();
    p.backoffs_ms = data.disable_backoffs.clone();
    p.hint = json!({"gen": "maintenance_disable"});
    p.follow_up = Some(disable_follow_up(data.clone()));
    p
}

/// Opens the window, probes it, waits, then sends the disable on the
/// reserved maintenance unit. The disable is never abandoned: a refused
/// begin is retried, a non-definitive end goes to the resolvers.
pub fn run_enable_follow_up(d: &Driver, data: WindowData) {
    *d.s.maint.lock().unwrap_or_else(|e| e.into_inner()) = MaintState::Open { since: Instant::now() };
    for p in &data.probes {
        let mut send = UntrackedSend::new(vec![p.request.clone()], None);
        send.schedule = vec![p.route.clone()];
        let r = d.send_untracked(send);
        if r.outcome.reason == "MAINTENANCE_MODE" {
            check_always_or_unreachable!(
                r.outcome.code == "Unavailable",
                "write during maintenance should be rejected as Unavailable",
                json!({"code": r.outcome.code, "outcome": r.outcome.summary()})
            );
        }
    }
    std::thread::sleep(Duration::from_millis(data.delay_ms));
    loop {
        if !d.s.oracle_ok() {
            disable_untracked(d, &data);
            return;
        }
        let Some(unit) = d.s.slots.acquire_maintenance(MAINT_ACQUIRE) else {
            println!("driver: maintenance disable: the reserved slot is still held after {MAINT_ACQUIRE:?}; retrying");
            continue;
        };
        match d.begin_write(disable_plan(&data), unit) {
            Ok(t) => {
                // A definitive success submits the disable's follow-up; a
                // non-definitive end hands the ticket to the resolvers.
                let _ = t.run(DISABLE_BUDGET);
                return;
            }
            Err(e) => match e.kind {
                OracleError::Backlog | OracleError::Serialized | OracleError::Busy | OracleError::Other | OracleError::NotQuiet => {
                    println!("driver: maintenance disable refused ({:?}: {}); retrying", e.kind, e.message);
                    std::thread::sleep(Duration::from_secs(1));
                }
                _ => {
                    println!("driver: maintenance disable cannot go through the oracle ({:?}: {}); disabling untracked", e.kind, e.message);
                    disable_untracked(d, &data);
                    return;
                }
            },
        }
    }
}

/// Degraded-mode fallback: the cluster must not stay in maintenance, so the
/// disable goes untracked (keyed, retried until definitive).
fn disable_untracked(d: &Driver, data: &WindowData) {
    let key = d.s.next_untracked_key();
    loop {
        let mut send = UntrackedSend::new(vec![maintenance_request(false)], Some(key.clone()));
        send.schedule = data.disable_schedule.clone();
        send.backoffs_ms = data.disable_backoffs.clone();
        send.budget = DISABLE_BUDGET;
        let r = d.send_untracked(send);
        if r.outcome.is_ok() {
            run_disable_follow_up(d, data);
            return;
        }
        println!("driver: untracked maintenance disable: {}", r.outcome.summary());
        std::thread::sleep(Duration::from_secs(1));
    }
}

/// Closes the window and probes a write.
pub fn run_disable_follow_up(d: &Driver, data: &WindowData) {
    *d.s.maint.lock().unwrap_or_else(|e| e.into_inner()) = MaintState::Closed;
    release_claim(d);
    check_sometimes!(true, "should be able to disable maintenance mode", json!({}));
    let mut send = UntrackedSend::new(vec![data.post_probe.request.clone()], Some(d.s.next_untracked_key()));
    send.schedule = vec![data.post_probe.route.clone()];
    send.backoffs_ms = data.post_backoffs.clone();
    send.budget = POST_PROBE_BUDGET;
    let r = d.send_untracked(send);
    check_sometimes!(r.outcome.is_ok(), "write after disabling maintenance should succeed", json!({"outcome": r.outcome.summary()}));
}

/// Enable maintenance with `k ∈ 0..=3` concurrent tracked chart writes.
pub fn maintenance_window(d: &Driver, tc: TestCase) {
    d.guarded("maintenance_window", &tc, |d, tc| {
        if !d.s.oracle_ok() || !d.s.maint_closed() {
            return None;
        }
        // Draws first: the follow-up data, the concurrent writes, schedules.
        let data = WindowData::draw(tc, d);
        let k = draw::int_in(tc, 0, 3) as usize;
        let mut writes = Vec::new();
        if k > 0 {
            if let Some((ledger, view)) = super::chart::pick_chart_ledger(d, tc) {
                let gens = super::chart::order_gens();
                for _ in 0..k {
                    let reqs = compose_bulk(d, tc, &gens, &ledger, &view, 1);
                    if !reqs.is_empty() {
                        let mut p = WritePlan::drawn("chart", reqs, d, tc);
                        p.hint = json!({"gen": "maintenance_concurrent"});
                        writes.push(p);
                    }
                }
            }
        }
        let enable = enable_plan(d, tc, data);
        // One window at a time.
        let ext = d.s.ext::<MaintExt>();
        if ext.claimed.compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst).is_err() {
            return None;
        }
        let Some(mut grant) = d.s.slots.try_acquire_up_to(1 + writes.len()) else {
            release_claim(d);
            return None;
        };
        writes.truncate(grant.len() - 1);
        grant.split(1 + writes.len());
        let mut plans = vec![enable];
        plans.extend(writes);
        let results = d.dispatch_many(plans, grant);
        match &results[0] {
            WriteResult::Definitive { outcome, .. } if outcome.is_ok() => {
                check_sometimes!(true, "should be able to enable maintenance mode", json!({}));
            }
            WriteResult::HandedOff { .. } => {} // a resolver's success runs the follow-up
            other => {
                println!("driver: maintenance enable did not commit: {other:?}");
                release_claim(d);
            }
        }
        for r in &results[1..] {
            let hit = r.attempts().iter().any(|a| a.reason == "MAINTENANCE_MODE");
            check_sometimes!(hit, "a write attempt hits maintenance mode", json!({"attempts": r.attempts().len()}));
        }
        Some(())
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testkit::{self, FakeReply};

    fn data(d: &Driver) -> WindowData {
        let probe = Probe { request: create_tx_request(&maint_ledger(&d.s.run), &[Posting::new("world", "users:1", 1, "COIN")], None), route: Route::plain(0) };
        WindowData { delay_ms: 0, probes: vec![probe.clone()], post_probe: probe, post_backoffs: vec![1], disable_schedule: vec![Route::plain(1)], disable_backoffs: vec![1] }
    }

    fn ok_apply() -> Outcome {
        Outcome::from_json(&json!({"ok": true, "response": {"logs": [{"sequence": "1"}]}}))
    }

    fn is_maint(b: &Value, enabled: bool) -> bool {
        b["requests"][0] == maintenance_request(enabled)
    }

    #[test]
    fn window_opens_disables_on_the_reserved_slot_and_closes() {
        let (d, ft, fo) = testkit::driver();
        testkit::reset();
        ft.set_default("bucket", "Apply", FakeReply::Outcome(ok_apply()));
        // Every general unit is held: the disable still goes through.
        let held = d.s.slots.try_acquire_up_to(7).unwrap();
        assert_eq!(held.len(), 7);
        d.s.ext::<MaintExt>().claimed.store(true, Ordering::SeqCst);
        run_enable_follow_up(&d, data(&d));
        let begins = fo.calls("/write/begin");
        assert_eq!(begins.len(), 1);
        assert!(is_maint(&begins[0], false), "{}", begins[0]);
        assert_eq!(fo.calls("/write/end").len(), 1);
        // The disable's follow-up runs as a job and closes the window.
        testkit::wait_until(Duration::from_secs(5), || d.s.maint_closed() && d.s.blocking_jobs() == 0);
        assert!(!d.s.ext::<MaintExt>().claimed.load(Ordering::SeqCst));
        assert!(testkit::fired("should be able to disable maintenance mode"));
        assert!(testkit::assertions().iter().any(|a| a.message == "write after disabling maintenance should succeed" && a.cond));
        drop(held);
        assert_eq!(d.s.slots.in_use(), (0, false));
    }

    #[test]
    fn probe_refused_by_maintenance_must_be_unavailable() {
        let (d, ft, _fo) = testkit::driver();
        testkit::reset();
        // probe → MAINTENANCE_MODE; disable → ok; post probe → ok
        ft.script("bucket", "Apply", vec![FakeReply::Outcome(Outcome::from_json(&json!({"ok": false, "code": "Unavailable", "reason": "MAINTENANCE_MODE"})))]);
        ft.set_default("bucket", "Apply", FakeReply::Outcome(ok_apply()));
        run_enable_follow_up(&d, data(&d));
        let a: Vec<_> = testkit::assertions().into_iter().filter(|a| a.message == "write during maintenance should be rejected as Unavailable").collect();
        assert_eq!(a.len(), 1);
        assert!(a[0].cond);
        testkit::wait_until(Duration::from_secs(5), || d.s.maint_closed() && d.s.blocking_jobs() == 0);
    }

    #[test]
    fn rule_enable_success_opens_then_closes_the_window() {
        let (d, ft, fo) = testkit::driver();
        testkit::reset();
        fo.set_view("ledger", json!({"types": []}));
        ft.set_default("bucket", "Apply", FakeReply::Outcome(ok_apply()));
        let mut enabled = false;
        for _ in 0..20 {
            testkit::run_rule(&d, maintenance_window);
            testkit::wait_until(Duration::from_secs(5), || d.s.maint_closed() && d.s.blocking_jobs() == 0);
            if fo.calls("/write/begin").iter().any(|b| is_maint(b, true)) {
                enabled = true;
                break;
            }
        }
        assert!(enabled, "the rule enabled maintenance");
        let begins = fo.calls("/write/begin");
        let en = begins.iter().position(|b| is_maint(b, true)).unwrap();
        let dis = begins.iter().position(|b| is_maint(b, false)).expect("a disable follows");
        assert!(dis > en);
        for b in &begins {
            if !is_maint(b, true) && !is_maint(b, false) {
                assert_eq!(b["family"], "chart");
            }
        }
        assert!(testkit::fired("should be able to enable maintenance mode"));
        assert!(testkit::fired("should be able to disable maintenance mode"));
        assert!(!d.s.ext::<MaintExt>().claimed.load(Ordering::SeqCst));
        assert!(d.s.registry.snapshot().is_empty());
    }

    #[test]
    fn handed_off_enable_runs_the_follow_up_on_resolver_success() {
        let (d, ft, fo) = testkit::driver();
        testkit::reset();
        ft.script("bucket", "Apply", vec![FakeReply::Delayed(Duration::from_millis(40), Outcome::unknown("timeout"))]);
        ft.set_default("bucket", "Apply", FakeReply::Outcome(ok_apply()));
        d.s.resolvers.set_paused(true);
        let wd = data(&d);
        let mut p = WritePlan::new("maintenance", vec![maintenance_request(true)]);
        p.backoffs_ms = vec![1];
        p.follow_up = Some(enable_follow_up(wd));
        let unit = d.s.slots.try_acquire_up_to(1).unwrap().take_one().unwrap();
        let t = d.begin_write(p, unit).unwrap();
        let r = t.run(Duration::from_millis(20));
        assert!(matches!(r, WriteResult::HandedOff { .. }), "{r:?}");
        assert!(d.s.maint_closed(), "not open before the enable is definitive");
        d.s.resolvers.set_paused(false);
        testkit::wait_until(Duration::from_secs(10), || fo.calls("/write/begin").iter().any(|b| is_maint(b, false)));
        testkit::wait_until(Duration::from_secs(10), || d.s.maint_closed() && d.s.blocking_jobs() == 0 && d.s.resolvers.is_empty());
        assert!(testkit::fired("should be able to disable maintenance mode"));
    }

    #[test]
    fn setup_creates_the_maintenance_ledger_keyed() {
        let (d, ft, _) = testkit::driver();
        ft.script("bucket", "Apply", vec![FakeReply::Outcome(Outcome::unknown("timeout"))]);
        maint_setup(&d);
        let calls = ft.calls_of("bucket", "Apply");
        assert_eq!(calls.len(), 2);
        assert_eq!(calls[0]["request"], calls[1]["request"], "same key and bytes");
        assert_eq!(calls[0]["request"]["unsigned"]["requests"][0]["createLedger"]["name"], maint_ledger(&d.s.run));
        assert!(calls[0]["request"]["unsigned"]["idempotencyKey"].as_str().unwrap().starts_with("r-"));
    }
}
