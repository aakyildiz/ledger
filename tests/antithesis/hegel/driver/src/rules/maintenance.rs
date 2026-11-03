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
//! taken by the rule before the enable and released when the last piece of
//! the window holding it goes (see `WindowClaim`).
use crate::clients::{Outcome, Route};
use crate::draw;
use crate::machine::{rule, Driver};
use crate::oracle::OracleError;
use crate::setup::SetupHook;
use crate::shared::MaintState;
use crate::wire::{create_tx_request, Posting};
use crate::jobs::Job;
use crate::write::{FollowUp, UntrackedSend, WritePlan, WriteResult};
use hegel::stateful::ConcurrentRule;
use hegel::TestCase;
use serde_json::{json, Value};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
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
#[derive(Debug, Default)]
pub struct MaintExt {
    pub claimed: AtomicBool,
}

/// The window claim. The rule takes it before the enable; every piece of the
/// window that still needs it (the enable's follow-up, the disable and its
/// follow-up, a disable retry) holds a clone, and the claim is released when
/// the last clone goes. So a failed or dropped enable (in the rule or in a
/// resolver) releases it, a `/write/end` error after a committed enable does
/// not, and a window ends only after the post-disable probe.
#[derive(Debug)]
pub struct WindowClaim {
    ext: Arc<MaintExt>,
}

impl Drop for WindowClaim {
    fn drop(&mut self) {
        self.ext.claimed.store(false, Ordering::SeqCst);
    }
}

/// Takes the window claim, or `None` when a window is already claimed.
pub fn try_claim(d: &Driver) -> Option<Arc<WindowClaim>> {
    let ext = d.s.ext::<MaintExt>();
    ext.claimed.compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst).ok()?;
    Some(Arc::new(WindowClaim { ext }))
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

/// Everything the follow-ups need, drawn in the rule, plus the window claim.
#[derive(Clone, Debug)]
pub struct WindowData {
    pub delay_ms: u64,
    pub probes: Vec<Probe>,
    pub post_probe: Probe,
    pub post_backoffs: Vec<u64>,
    pub disable_schedule: Vec<Route>,
    pub disable_backoffs: Vec<u64>,
    pub claim: Option<Arc<WindowClaim>>,
}

impl WindowData {
    pub fn draw(tc: &TestCase, d: &Driver) -> WindowData {
        let delay_ms = draw::int_in(tc, 0, 3000);
        let n = draw::int_in(tc, 1, 2) as usize;
        let probes = (0..n).map(|_| draw_probe(tc, d)).collect();
        let post_probe = draw_probe(tc, d);
        WindowData { delay_ms, probes, post_probe, post_backoffs: draw::backoffs(tc, 8), disable_schedule: draw::schedule(tc, &d.s, 8), disable_backoffs: draw::backoffs(tc, 8), claim: None }
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

/// The enable's follow-up (a quiet-blocking job, DESIGN §4.9). Dropped
/// uncalled (the enable failed or was dropped), it releases its claim clone.
pub fn enable_follow_up(data: WindowData) -> FollowUp {
    Box::new(move |d: &Driver, _o: &Outcome| run_enable_follow_up(d, data))
}

/// How often a definitively failed disable is sent again before the window
/// gives up (each retry is a quiet-blocking job one second later).
pub const DISABLE_RETRIES: u32 = 30;
/// Oracle refusals of the disable's `/write/begin` that are not back-pressure
/// (protocol, unmodeled, panic, …) tolerated before the driver enters
/// degraded mode and disables untracked.
pub const DISABLE_HARD_REFUSALS: u32 = 5;

/// The disable's follow-up guard: called on the disable's definitive success
/// (closes the window); dropped uncalled after the disable's ticket existed
/// (a definitive failure, in the rule or a resolver), it submits a retry of
/// the disable, so the cluster never stays in maintenance.
struct DisableGuard {
    d: Driver,
    data: Option<WindowData>,
    armed: Arc<AtomicBool>,
    retry: u32,
}

impl Drop for DisableGuard {
    fn drop(&mut self) {
        let Some(data) = self.data.take() else { return };
        if !self.armed.load(Ordering::SeqCst) {
            return; // the begin was refused; the disable loop retries itself
        }
        if self.retry >= DISABLE_RETRIES {
            println!("driver: maintenance disable failed {} times; giving the window up", self.retry);
            return;
        }
        let retry = self.retry + 1;
        println!("driver: maintenance disable did not commit; retry {retry}");
        self.d.submit_job(Job {
            name: "maintenance-disable-retry",
            blocks_quiet: true,
            run: Box::new(move |d: &Driver| {
                std::thread::sleep(Duration::from_secs(1));
                run_disable(d, data, retry);
            }),
        });
    }
}

fn disable_plan(d: &Driver, data: &WindowData, armed: &Arc<AtomicBool>, retry: u32) -> WritePlan {
    let mut p = WritePlan::new("maintenance", vec![maintenance_request(false)]);
    p.schedule = data.disable_schedule.clone();
    p.backoffs_ms = data.disable_backoffs.clone();
    p.hint = json!({"gen": "maintenance_disable"});
    let mut guard = DisableGuard { d: d.clone(), data: Some(data.clone()), armed: armed.clone(), retry };
    p.follow_up = Some(Box::new(move |d: &Driver, _o: &Outcome| {
        if let Some(data) = guard.data.take() {
            run_disable_follow_up(d, data);
        }
    }));
    p
}

/// Opens the window, probes it, waits, then sends the disable on the
/// reserved maintenance unit.
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
    run_disable(d, data, 0);
}

/// Sends the tracked disable on the reserved maintenance unit. The disable
/// is never abandoned: back-pressure refusals of the begin are retried, a
/// non-definitive end goes to the resolvers, a definitive failure is retried
/// by the follow-up guard. Only a driver in degraded mode disables untracked;
/// repeated hard refusals enter degraded mode first (DESIGN §3: no other
/// writer while the oracle is healthy).
fn run_disable(d: &Driver, data: WindowData, retry: u32) {
    let mut hard = 0u32;
    loop {
        if !d.s.oracle_ok() {
            disable_untracked(d, &data);
            return;
        }
        let Some(unit) = d.s.slots.acquire_maintenance(MAINT_ACQUIRE) else {
            println!("driver: maintenance disable: the reserved slot is still held after {MAINT_ACQUIRE:?}; retrying");
            continue;
        };
        let armed = Arc::new(AtomicBool::new(false));
        match d.begin_write(disable_plan(d, &data, &armed, retry), unit) {
            Ok(t) => {
                // From here on the guard owns the retry; a definitive
                // success submits the disable's follow-up.
                armed.store(true, Ordering::SeqCst);
                let _ = t.run(DISABLE_BUDGET);
                return;
            }
            Err(e) => match e.kind {
                OracleError::Backlog | OracleError::Serialized | OracleError::Busy | OracleError::Other | OracleError::NotQuiet => {
                    println!("driver: maintenance disable refused ({:?}: {}); retrying", e.kind, e.message);
                    std::thread::sleep(Duration::from_secs(1));
                }
                OracleError::Degraded | OracleError::Unreachable | OracleError::Restarted => {
                    // Degraded mode follows: the loop goes untracked.
                    println!("driver: maintenance disable refused ({:?}: {}); re-checking the oracle", e.kind, e.message);
                }
                _ => {
                    hard += 1;
                    if hard >= DISABLE_HARD_REFUSALS {
                        println!("driver: maintenance disable refused {hard} times ({:?}: {}); entering degraded mode", e.kind, e.message);
                        d.s.enter_degraded("maintenance disable refused by the oracle");
                        continue;
                    }
                    println!("driver: maintenance disable refused ({:?}: {}); retry {hard}", e.kind, e.message);
                    std::thread::sleep(Duration::from_secs(u64::from(hard)));
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
            run_disable_follow_up(d, data.clone());
            return;
        }
        println!("driver: untracked maintenance disable: {}", r.outcome.summary());
        std::thread::sleep(Duration::from_secs(1));
    }
}

/// Closes the window and probes a write; the claim clone in `data` goes
/// after the probe, so no new window can disturb it.
pub fn run_disable_follow_up(d: &Driver, data: WindowData) {
    *d.s.maint.lock().unwrap_or_else(|e| e.into_inner()) = MaintState::Closed;
    check_sometimes!(true, "should be able to disable maintenance mode", json!({}));
    let mut send = UntrackedSend::new(vec![data.post_probe.request.clone()], Some(d.s.next_untracked_key()));
    send.schedule = vec![data.post_probe.route.clone()];
    send.backoffs_ms = data.post_backoffs.clone();
    send.budget = POST_PROBE_BUDGET;
    let r = d.send_untracked(send);
    check_sometimes!(r.outcome.is_ok(), "write after disabling maintenance should succeed", json!({"outcome": r.outcome.summary()}));
    drop(data);
}

/// Enable maintenance with `k ∈ 0..=3` concurrent tracked chart writes.
pub fn maintenance_window(d: &Driver, tc: TestCase) {
    d.guarded("maintenance_window", &tc, |d, tc| {
        if !d.s.oracle_ok() || !d.s.maint_closed() || d.s.ext::<MaintExt>().claimed.load(Ordering::SeqCst) {
            return None;
        }
        // Draws first: the follow-up data, the concurrent writes, schedules.
        let mut data = WindowData::draw(tc, d);
        let k = draw::int_in(tc, 0, 3) as usize;
        let mut writes = Vec::new();
        if k > 0 {
            if let Some((ledger, view)) = super::chart::pick_chart_ledger(d, tc) {
                for _ in 0..k {
                    let reqs = super::chart::compose_chart(d, tc, &ledger, &view, 1);
                    if !reqs.is_empty() {
                        let mut p = WritePlan::drawn("chart", reqs, d, tc);
                        p.hint = json!({"gen": "maintenance_concurrent"});
                        writes.push(p);
                    }
                }
            }
        }
        // One window at a time. Every early return below drops the plans
        // and with them the claim.
        data.claim = Some(try_claim(d)?);
        let enable = enable_plan(d, tc, data);
        let mut grant = d.s.slots.try_acquire_up_to(1 + writes.len())?;
        writes.truncate(grant.len() - 1);
        grant.split(1 + writes.len());
        let mut plans = vec![enable];
        plans.extend(writes);
        let results = d.dispatch_many(plans, grant);
        match &results[0] {
            WriteResult::Definitive { outcome, .. } if outcome.is_ok() => {
                check_sometimes!(true, "should be able to enable maintenance mode", json!({}));
            }
            // A resolver's success runs the follow-up; its failure drops it.
            WriteResult::HandedOff { .. } => {}
            other => println!("driver: maintenance enable did not commit: {other:?}"),
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
        WindowData { delay_ms: 0, probes: vec![probe.clone()], post_probe: probe, post_backoffs: vec![1], disable_schedule: vec![Route::plain(1)], disable_backoffs: vec![1], claim: try_claim(d) }
    }

    fn claimed(d: &Driver) -> bool {
        d.s.ext::<MaintExt>().claimed.load(Ordering::SeqCst)
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
        // The disable's Apply is slow, so its unit can be observed in use.
        ft.script("bucket", "Apply", vec![FakeReply::Outcome(ok_apply())]);
        ft.script("bucket", "Apply", vec![FakeReply::Delayed(Duration::from_millis(400), ok_apply())]);
        ft.set_default("bucket", "Apply", FakeReply::Outcome(ok_apply()));
        // Every general unit is held: the disable still goes through.
        let held = d.s.slots.try_acquire_up_to(7).unwrap();
        assert_eq!(held.len(), 7);
        let wd = data(&d);
        assert!(claimed(&d));
        std::thread::scope(|s| {
            s.spawn(|| run_enable_follow_up(&d, wd));
            // The disable runs on the reserved maintenance unit.
            testkit::wait_until(Duration::from_secs(5), || d.s.slots.in_use() == (7, true));
        });
        let begins = fo.calls("/write/begin");
        assert_eq!(begins.len(), 1);
        assert!(is_maint(&begins[0], false), "{}", begins[0]);
        assert_eq!(fo.calls("/write/end").len(), 1);
        // The disable's follow-up runs as a job and closes the window.
        testkit::wait_until(Duration::from_secs(5), || d.s.maint_closed() && d.s.blocking_jobs() == 0);
        assert!(!claimed(&d));
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
        testkit::wait_until(Duration::from_secs(5), || !claimed(&d));
    }

    /// A handed-off enable that a resolver settles as a failure drops its
    /// follow-up, and with it the window claim.
    #[test]
    fn handed_off_enable_failing_later_releases_the_claim() {
        let (d, ft, fo) = testkit::driver();
        testkit::reset();
        ft.script("bucket", "Apply", vec![FakeReply::Delayed(Duration::from_millis(40), Outcome::unknown("timeout"))]);
        ft.set_default("bucket", "Apply", FakeReply::Outcome(Outcome::from_json(&json!({"ok": false, "code": "InvalidArgument", "reason": "VALIDATION"}))));
        d.s.resolvers.set_paused(true);
        let wd = data(&d);
        let mut p = WritePlan::new("maintenance", vec![maintenance_request(true)]);
        p.backoffs_ms = vec![1];
        p.follow_up = Some(enable_follow_up(wd));
        let unit = d.s.slots.try_acquire_up_to(1).unwrap().take_one().unwrap();
        let r = d.begin_write(p, unit).unwrap().run(Duration::from_millis(20));
        assert!(matches!(r, WriteResult::HandedOff { .. }), "{r:?}");
        assert!(claimed(&d), "the claim lives with the handed-off ticket");
        d.s.resolvers.set_paused(false);
        testkit::wait_until(Duration::from_secs(10), || d.s.resolvers.is_empty() && !claimed(&d));
        assert!(d.s.maint_closed());
        assert!(!fo.calls("/write/begin").iter().any(|b| is_maint(b, false)), "no window, no disable");
    }

    /// An enable refused definitively in the rule releases the claim.
    #[test]
    fn rule_enable_failure_releases_the_claim() {
        let (d, ft, fo) = testkit::driver();
        testkit::reset();
        fo.set_view("ledger", json!({"types": []}));
        ft.set_default("bucket", "Apply", FakeReply::Outcome(Outcome::from_json(&json!({"ok": false, "code": "InvalidArgument", "reason": "VALIDATION"}))));
        for _ in 0..5 {
            testkit::run_rule(&d, maintenance_window);
        }
        assert!(fo.calls("/write/begin").iter().any(|b| is_maint(b, true)));
        assert!(!claimed(&d));
        assert!(d.s.maint_closed());
        assert_eq!(d.s.blocking_jobs(), 0);
    }

    /// A disable that fails definitively is sent again (never abandoned);
    /// the window closes on the retry's success.
    #[test]
    fn definitively_failed_disable_is_retried() {
        let (d, ft, fo) = testkit::driver();
        testkit::reset();
        // probe ok; disable refused definitively; retried disable ok; post probe ok.
        ft.script("bucket", "Apply", vec![FakeReply::Outcome(ok_apply()), FakeReply::Outcome(Outcome::from_json(&json!({"ok": false, "code": "InvalidArgument", "reason": "VALIDATION"})))]);
        ft.set_default("bucket", "Apply", FakeReply::Outcome(ok_apply()));
        run_enable_follow_up(&d, data(&d));
        testkit::wait_until(Duration::from_secs(10), || d.s.maint_closed() && d.s.blocking_jobs() == 0 && !claimed(&d));
        assert_eq!(fo.calls("/write/begin").iter().filter(|b| is_maint(b, false)).count(), 2);
        assert!(testkit::fired("should be able to disable maintenance mode"));
    }

    /// A hard oracle refusal of the disable's begin is retried through the
    /// oracle while it is healthy: no untracked disable.
    #[test]
    fn hard_refusal_of_the_disable_stays_tracked_while_the_oracle_is_healthy() {
        let (d, ft, fo) = testkit::driver();
        testkit::reset();
        ft.set_default("bucket", "Apply", FakeReply::Outcome(ok_apply()));
        fo.refuse_next_begin(400, "protocol");
        run_enable_follow_up(&d, data(&d));
        testkit::wait_until(Duration::from_secs(10), || d.s.maint_closed() && d.s.blocking_jobs() == 0 && !claimed(&d));
        assert!(d.s.oracle_ok());
        let disables: Vec<_> = ft.calls_of("bucket", "Apply").into_iter().filter(|c| c.to_string().contains("setMaintenanceMode")).collect();
        assert_eq!(disables.len(), 1, "one tracked disable, nothing untracked");
        assert!(!disables[0]["request"]["unsigned"]["idempotencyKey"].as_str().unwrap_or("").starts_with("r-"), "{}", disables[0]);
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
