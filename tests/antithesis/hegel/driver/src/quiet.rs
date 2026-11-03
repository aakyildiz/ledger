//! The `quiet_period` invariant (DESIGN §4.10, §6.7): faults stop, the
//! system must recover, every handed-off write must resolve, the oracle's
//! tickets must all be owned by the driver, and then one hegel-drawn check
//! flavour (a registered `QuietHook`) runs inside the oracle's quiet epoch.
#![allow(dead_code)] // family-facing API

use crate::clients::{CallOpts, Route};
use crate::draw;
use crate::machine::Driver;
use crate::nodes::NodeMap;
use crate::oracle::{OracleError, QuietSectionReq};
use crate::shared::Shared;
use hegel::TestCase;
use serde_json::{json, Value};
use std::collections::BTreeSet;
use std::time::{Duration, Instant};

/// Trigger probability below `QUIET_MIN_GAP` since the last quiet period.
pub const P_LOW: f64 = 1.0 / 5000.0;
/// Trigger probability from `QUIET_MAX_GAP` on.
pub const P_HIGH: f64 = 1.0 / 10.0;
pub const QUIET_MIN_GAP: Duration = Duration::from_secs(150);
pub const QUIET_MAX_GAP: Duration = Duration::from_secs(480);
/// The head pause at Q0 (covers Q1–Q4).
pub const Q0_PAUSE_SECS: u64 = 100;
pub const Q1_DEADLINE: Duration = Duration::from_secs(45);
pub const Q2_DEADLINE: Duration = Duration::from_secs(60);
pub const Q3_DEADLINE: Duration = Duration::from_secs(90);
/// `/quiet/begin` retries until the Q3 deadline plus this.
pub const Q4_EXTRA: Duration = Duration::from_secs(10);
/// The hook's pause is `budget + PAUSE_MARGIN_SECS`.
pub const PAUSE_MARGIN_SECS: u64 = 15;
/// `Q0_PAUSE_SECS + max(budget) + PAUSE_MARGIN_SECS` must stay within this.
pub const QUIET_PAUSE_LIMIT_SECS: u64 = 270;

pub type HookPlan = Box<dyn std::any::Any + Send>;

/// A check flavour of the quiet period (families register these).
pub struct QuietHook {
    pub name: &'static str,
    pub budget: Duration,
    pub needs_oracle: bool,
    /// 0 = not eligible now.
    pub weight: fn(&Shared) -> f64,
    /// All draws.
    pub plan: fn(&Driver, &TestCase) -> HookPlan,
    /// No `TestCase`; must stop at `ctx.deadline`.
    pub run: fn(&Driver, &mut QuietCtx, &HookPlan),
}

/// The quiet epoch of one quiet period. Dropping it posts `/quiet/end` if
/// `/quiet/begin` succeeded (also during an unwind).
pub struct QuietCtx {
    pub started: Instant,
    pub deadline: Instant,
    pub epoch: u64,
    /// The `/quiet/begin` response (`None` when skipped: degraded mode).
    pub begin: Option<Value>,
    pub nodes: NodeMap,
    /// A client that answered the Q1 barrier.
    pub client: usize,
    d: Driver,
    open: bool,
}

impl QuietCtx {
    pub fn remaining(&self) -> Duration {
        self.deadline.saturating_duration_since(Instant::now())
    }

    /// `POST /quiet/section`; returns the response (`verdict`, `next`).
    pub fn section(&mut self, d: &Driver, section: &str, seq: u64, body: Value) -> Option<Value> {
        if !self.open {
            return None;
        }
        let r = d.s.oracle.quiet_section(&QuietSectionReq { epoch: self.epoch, section: section.to_string(), seq, body });
        r.ok().map(|resp| serde_json::to_value(resp).unwrap_or(Value::Null))
    }
}

impl Drop for QuietCtx {
    fn drop(&mut self) {
        if !self.open {
            return;
        }
        self.open = false;
        if let Err(e) = self.d.s.oracle.quiet_end(self.epoch) {
            println!("driver: /quiet/end {}: {:?} {}", self.epoch, e.kind, e.message);
        }
    }
}

/// The trigger probability after `elapsed` since the last end: `P_LOW`
/// below `min_gap`, rising linearly to `P_HIGH` at `max_gap`, then `P_HIGH`.
pub fn elapsed_probability(elapsed: Duration, min_gap: Duration, max_gap: Duration) -> f64 {
    if elapsed <= min_gap {
        return P_LOW;
    }
    if elapsed >= max_gap {
        return P_HIGH;
    }
    let f = (elapsed - min_gap).as_secs_f64() / (max_gap - min_gap).as_secs_f64();
    P_LOW + f * (P_HIGH - P_LOW)
}

/// Runs the fault pause command for `secs` seconds (no-op without it).
pub fn pause_faults(secs: u64) {
    let Some(bin) = std::env::var_os("ANTITHESIS_STOP_FAULTS") else { return };
    let status = std::process::Command::new(&bin).arg(secs.to_string()).status();
    if std::env::var_os("ANTITHESIS_OUTPUT_DIR").is_some() {
        let ok = status.as_ref().is_ok_and(|s| s.success());
        check_always!(ok, "the fault pause command succeeds", json!({"seconds": secs, "status": format!("{status:?}")}));
    }
}

/// The flavour weights: `weight(&Shared)`, 0 for `needs_oracle` hooks in
/// degraded mode.
pub fn flavour_weights(s: &Shared, hooks: &[QuietHook]) -> Vec<f64> {
    hooks.iter().map(|h| if h.needs_oracle && !s.oracle_ok() { 0.0 } else { (h.weight)(s).max(0.0) }).collect()
}

/// The invariant.
pub fn quiet_period(d: &Driver, tc: TestCase) {
    let s = &d.s;
    if !s.is_active() {
        return;
    }
    let p = s.cfg.quiet_probability.unwrap_or_else(|| {
        let last = *s.last_quiet_end.lock().unwrap_or_else(|e| e.into_inner());
        elapsed_probability(last.elapsed(), QUIET_MIN_GAP, QUIET_MAX_GAP)
    });
    if !draw::chance(&tc, p) {
        return;
    }
    // Q0: every draw of the quiet period happens here.
    let hooks = crate::rules::quiet_hooks();
    let chosen = draw::weighted(&tc, &flavour_weights(s, &hooks));
    let plan = chosen.map(|i| (hooks[i].plan)(d, &tc));
    run_quiet(d, chosen.map(|i| &hooks[i]), plan);
}

/// Sets `last_quiet_end` when the quiet period ends, however it ends.
struct EndGuard<'a>(&'a Shared);
impl Drop for EndGuard<'_> {
    fn drop(&mut self) {
        *self.0.last_quiet_end.lock().unwrap_or_else(|e| e.into_inner()) = Instant::now();
        self.0.quiet_open.store(false, std::sync::atomic::Ordering::SeqCst);
    }
}

fn wait_for(deadline: Instant, poll: Duration, mut cond: impl FnMut() -> bool) -> bool {
    loop {
        if cond() {
            return true;
        }
        if Instant::now() + poll > deadline {
            return cond();
        }
        std::thread::sleep(poll);
    }
}

/// Q1: every client answers a Barrier.
fn barrier_all(d: &Driver, deadline: Instant) -> Vec<bool> {
    let n = d.s.clients.len();
    let mut healthy = vec![false; n];
    let poll = d.s.timing().poll;
    wait_for(deadline, poll, || {
        for (c, h) in healthy.iter_mut().enumerate() {
            if !*h {
                let (_, out) = d.call(&Route::lib(c), "bucket", "Barrier", json!({}), &CallOpts::default().timeout(Duration::from_secs(3)));
                *h = out.is_ok();
            }
        }
        !healthy.contains(&false)
    });
    healthy
}

/// The ticket audit (DESIGN §4.10): every oracle in-flight and read ticket
/// is owned by a driver registry entry. A leak enters degraded mode.
pub fn ticket_audit(d: &Driver) -> bool {
    let s = &d.s;
    for attempt in 0..5 {
        if !s.oracle_ok() {
            return true;
        }
        let p1 = s.registry.pending_begins();
        let r1 = s.registry.snapshot();
        let Ok(t) = s.oracle.tickets() else { return true };
        let r2 = s.registry.snapshot();
        let p2 = s.registry.pending_begins();
        let owned: BTreeSet<u64> = r1.union(&r2).copied().collect();
        let leaked: Vec<u64> = t.inflight.iter().chain(t.reads.iter()).filter(|x| !owned.contains(x)).copied().collect();
        if leaked.is_empty() {
            check_always!(true, "every oracle ticket has an owner in the driver", json!({"inflight": t.inflight.len(), "reads": t.reads.len()}));
            return true;
        }
        if p1 + p2 > 0 && attempt < 4 {
            // A begin was in flight: its ticket may exist before its entry.
            std::thread::sleep(Duration::from_millis(200));
            continue;
        }
        check_always!(
            false,
            "every oracle ticket has an owner in the driver",
            json!({"leaked": leaked, "inflight": t.inflight, "reads": t.reads, "registry": owned.iter().collect::<Vec<_>>(), "resolvers": s.resolvers.tickets()})
        );
        s.enter_degraded("leaked oracle ticket");
        return false;
    }
    true
}

fn oracle_drained(d: &Driver) -> bool {
    if !d.s.oracle_ok() {
        return true;
    }
    match d.s.oracle.quiet_status() {
        Ok(q) => q.inflight == 0 && q.reads == 0,
        Err(_) => !d.s.oracle_ok(),
    }
}

/// Q1–Q6 after the Q0 draws. No draw happens in here.
pub fn run_quiet(d: &Driver, hook: Option<&QuietHook>, plan: Option<HookPlan>) {
    let s = &d.s;
    let t = s.timing().clone();
    let start = Instant::now();
    let epoch = s.next_epoch.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    let name = hook.map(|h| h.name).unwrap_or("none");
    s.quiets_since_exact.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    s.quiet_open.store(true, std::sync::atomic::Ordering::SeqCst);
    let _end = EndGuard(s);
    pause_faults(t.q0_pause_secs);
    check_reachable!("a quiet period starts", json!({"hook": name, "epoch": epoch}));
    println!("driver: quiet period {epoch} ({name}) starts");

    // Q1
    let healthy = barrier_all(d, start + t.q1);
    let all = !healthy.contains(&false);
    check_sometimes!(all, "the system recovers after faults stop", json!({"healthy": healthy}));
    if !all {
        println!("driver: quiet period {epoch}: clients did not recover: {healthy:?}");
        return;
    }
    let client = healthy.iter().position(|h| *h).unwrap_or(0);

    // Q2
    let q2 = wait_for(start + t.q2, t.poll, || s.maint_closed() && !s.signatures_required() && s.blocking_jobs() == 0);
    if !q2 {
        println!(
            "driver: quiet period {epoch}: Q2 timed out (maintenance closed {}, signatures required {}, blocking jobs {})",
            s.maint_closed(),
            s.signatures_required(),
            s.blocking_jobs()
        );
        return;
    }

    // Q3
    ticket_audit(d);
    let drained = wait_for(start + t.q3, t.poll, || s.resolvers.is_empty() && s.blocking_jobs() == 0 && oracle_drained(d));
    ticket_audit(d);
    check_sometimes!(drained, "every handed-off write resolves during a quiet period", json!({"resolvers": s.resolvers.tickets(), "blockingJobs": s.blocking_jobs()}));
    if !drained {
        println!("driver: quiet period {epoch}: Q3 timed out with resolver tickets {:?}", s.resolvers.tickets());
        s.resolvers.q3_timeout();
        return;
    }

    // Q4
    let mut begin = None;
    if s.oracle_ok() {
        let deadline = start + t.q3 + t.q4_extra;
        loop {
            match s.oracle.quiet_begin(epoch) {
                Ok(r) => {
                    begin = Some(serde_json::to_value(r).unwrap_or(Value::Null));
                    break;
                }
                Err(e) if e.kind == OracleError::NotQuiet && Instant::now() + t.poll < deadline => std::thread::sleep(t.poll),
                Err(e) if e.kind == OracleError::Degraded || !s.oracle_ok() => break,
                Err(e) => {
                    println!("driver: quiet period {epoch}: /quiet/begin: {:?} {} {}", e.kind, e.message, e.details);
                    return;
                }
            }
        }
    }
    let nodes = NodeMap::discover(d).unwrap_or_default();
    let open = begin.is_some();
    let mut ctx = QuietCtx { started: start, deadline: Instant::now(), epoch, begin, nodes, client, d: d.clone(), open };

    // Q5
    let completed = match (hook, plan.as_ref()) {
        (Some(h), Some(plan)) => {
            pause_faults(h.budget.as_secs() + t.pause_margin_secs);
            ctx.deadline = Instant::now() + h.budget;
            (h.run)(d, &mut ctx, plan);
            Instant::now() <= ctx.deadline
        }
        _ => true,
    };
    check_sometimes!(completed, "the drawn quiet check completes within its budget", json!({"hook": name, "epoch": epoch}));
    // Q6: dropping the context posts /quiet/end.
    drop(ctx);
    println!("driver: quiet period {epoch} ({name}) ends after {} ms", start.elapsed().as_millis());
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testkit;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// Hook runs, per `Shared` (tests run in parallel).
    #[derive(Default)]
    struct Ran(AtomicUsize);

    fn ran(d: &Driver) -> usize {
        d.s.ext::<Ran>().0.load(Ordering::SeqCst)
    }

    fn w1(_: &Shared) -> f64 {
        1.0
    }
    fn w0(_: &Shared) -> f64 {
        0.0
    }
    fn plan(_: &Driver, _: &TestCase) -> HookPlan {
        Box::new(5u32)
    }
    fn run_counts(d: &Driver, ctx: &mut QuietCtx, p: &HookPlan) {
        assert_eq!(p.downcast_ref::<u32>(), Some(&5));
        let _ = ctx.remaining();
        d.s.ext::<Ran>().0.fetch_add(1, Ordering::SeqCst);
    }
    fn run_unwinds(_: &Driver, _: &mut QuietCtx, _: &HookPlan) {
        std::panic::panic_any(testkit::StopTestLike);
    }

    fn hook(run: fn(&Driver, &mut QuietCtx, &HookPlan)) -> QuietHook {
        QuietHook { name: "test", budget: Duration::from_secs(1), needs_oracle: true, weight: w1, plan, run }
    }

    #[test]
    fn probability_rises_with_elapsed_time() {
        assert_eq!(elapsed_probability(Duration::from_secs(0), QUIET_MIN_GAP, QUIET_MAX_GAP), P_LOW);
        assert_eq!(elapsed_probability(QUIET_MIN_GAP - Duration::from_secs(1), QUIET_MIN_GAP, QUIET_MAX_GAP), P_LOW);
        assert_eq!(elapsed_probability(QUIET_MAX_GAP, QUIET_MIN_GAP, QUIET_MAX_GAP), P_HIGH);
        assert_eq!(elapsed_probability(QUIET_MAX_GAP + Duration::from_secs(600), QUIET_MIN_GAP, QUIET_MAX_GAP), P_HIGH);
        let mid = elapsed_probability(Duration::from_secs(315), QUIET_MIN_GAP, QUIET_MAX_GAP);
        assert!(mid > P_LOW && mid < P_HIGH);
    }

    #[test]
    fn budget_table_fits_the_pause_limit() {
        assert_eq!(P_LOW, 1.0 / 5000.0);
        assert_eq!(P_HIGH, 1.0 / 10.0);
        assert_eq!(QUIET_MIN_GAP, Duration::from_secs(150));
        assert_eq!(QUIET_MAX_GAP, Duration::from_secs(480));
        assert_eq!((Q0_PAUSE_SECS, Q1_DEADLINE, Q2_DEADLINE, Q3_DEADLINE, Q4_EXTRA, PAUSE_MARGIN_SECS), (100, Duration::from_secs(45), Duration::from_secs(60), Duration::from_secs(90), Duration::from_secs(10), 15));
        let t = crate::shared::Timing::default();
        assert_eq!((t.q0_pause_secs, t.q1, t.q2, t.q3, t.q4_extra, t.pause_margin_secs), (Q0_PAUSE_SECS, Q1_DEADLINE, Q2_DEADLINE, Q3_DEADLINE, Q4_EXTRA, PAUSE_MARGIN_SECS));
        let max = crate::rules::quiet_hooks().iter().map(|h| h.budget.as_secs()).max().unwrap_or(0);
        assert!(Q0_PAUSE_SECS + max + PAUSE_MARGIN_SECS <= QUIET_PAUSE_LIMIT_SECS, "max hook budget {max} s");
        // Every registered hook can stop within its budget's pause.
        for h in crate::rules::quiet_hooks() {
            assert!(h.budget > Duration::ZERO, "{}", h.name);
        }
    }

    #[test]
    fn a_full_quiet_period_runs_the_hook_inside_the_epoch() {
        let (d, _, fo) = testkit::driver();
        testkit::reset();
        run_quiet(&d, Some(&hook(run_counts)), Some(Box::new(5u32)));
        assert_eq!(ran(&d), 1);
        assert_eq!(fo.calls("/quiet/begin").len(), 1);
        assert_eq!(fo.calls("/quiet/end").len(), 1);
        assert!(!fo.is_quiet());
        for m in ["a quiet period starts", "the system recovers after faults stop", "every handed-off write resolves during a quiet period", "every oracle ticket has an owner in the driver", "the drawn quiet check completes within its budget"] {
            assert!(testkit::assertions().iter().any(|a| a.message == m && a.cond), "{m}");
        }
        // No hook at all: the period still reaches Q6.
        run_quiet(&d, None, None);
        assert_eq!(fo.calls("/quiet/end").len(), 2);
    }

    #[test]
    fn outstanding_tickets_time_out_q3_and_skip_the_hook() {
        let (d, _, fo) = testkit::driver();
        testkit::reset();
        fo.set_status_override(Some((1, 0)));
        run_quiet(&d, Some(&hook(run_counts)), Some(Box::new(5u32)));
        assert_eq!(ran(&d), 0, "the hook does not run");
        assert!(fo.calls("/quiet/begin").is_empty());
        assert!(testkit::assertions().iter().any(|a| a.message == "every handed-off write resolves during a quiet period" && !a.cond));
        assert!(d.s.oracle_ok());
    }

    #[test]
    fn a_leaked_ticket_fails_the_audit_and_degrades() {
        let (d, _, fo) = testkit::driver();
        testkit::reset();
        fo.leak_ticket(4242);
        run_quiet(&d, Some(&hook(run_counts)), Some(Box::new(5u32)));
        let a = testkit::assertions();
        let hit = a.iter().find(|a| a.message == "every oracle ticket has an owner in the driver" && !a.cond).expect("Always false");
        assert_eq!(hit.details["leaked"], json!([4242]));
        assert!(!d.s.oracle_ok(), "degraded mode");
        assert!(fo.calls("/quiet/begin").is_empty(), "degraded: /quiet/begin skipped");
    }

    #[test]
    fn an_unwind_after_quiet_begin_still_posts_quiet_end() {
        let (d, _, fo) = testkit::driver();
        let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| run_quiet(&d, Some(&hook(run_unwinds)), Some(Box::new(5u32)))));
        assert!(r.is_err());
        assert_eq!(fo.calls("/quiet/begin").len(), 1);
        assert_eq!(fo.calls("/quiet/end").len(), 1);
        assert!(!fo.is_quiet());
    }

    #[test]
    fn flavour_draw_respects_zero_weights_and_degraded_mode() {
        let (d, _, _) = testkit::driver();
        let hooks = vec![
            QuietHook { name: "zero", budget: Duration::from_secs(1), needs_oracle: false, weight: w0, plan, run: run_counts },
            QuietHook { name: "oracle", budget: Duration::from_secs(1), needs_oracle: true, weight: w1, plan, run: run_counts },
            QuietHook { name: "rust", budget: Duration::from_secs(1), needs_oracle: false, weight: w1, plan, run: run_counts },
        ];
        testkit::with_tc_n(50, |tc| {
            let i = draw::weighted(tc, &flavour_weights(&d.s, &hooks)).unwrap();
            assert!(i == 1 || i == 2);
        });
        d.s.enter_degraded("test");
        assert_eq!(flavour_weights(&d.s, &hooks), vec![0.0, 0.0, 1.0]);
        testkit::with_tc_n(20, |tc| assert_eq!(draw::weighted(tc, &flavour_weights(&d.s, &hooks)), Some(2)));
    }

    #[test]
    fn quiet_period_is_triggered_by_a_draw() {
        let (d, _, fo) = testkit::driver();
        testkit::run_rule(&d, quiet_period);
        assert!(!fo.calls("/quiet/end").is_empty(), "QUIET_PROBABILITY=1 in tests");
    }
}
