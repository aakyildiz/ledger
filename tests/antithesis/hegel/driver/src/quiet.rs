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
pub const QUIET_MIN_GAP: Duration = Duration::from_secs(60);
pub const QUIET_MAX_GAP: Duration = Duration::from_secs(240);
/// The gaps of the first `FIRST_QUIETS` quiet periods of a process while an
/// eligible flavour has not completed yet: an Antithesis timeline is often
/// only 10–15 minutes long and fits about three quiet periods at the normal
/// gaps, while there are eight flavours (and `exact` needs Q1–Q4 plus up to
/// its 150 s budget).
pub const FIRST_MIN_GAP: Duration = Duration::from_secs(30);
pub const FIRST_MAX_GAP: Duration = Duration::from_secs(120);
pub const FIRST_QUIETS: u64 = 5;
/// The weight of each pending flavour once `exact` is no longer pending (see
/// `flavour_weights`): the pending flavours share the draw evenly, whatever
/// their base weight.
pub const PENDING_WEIGHT: f64 = 1.0;
/// Runs (Q5 reached) after which a flavour that has not completed stops
/// being pending.
pub const PENDING_RUNS: u64 = 2;
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

/// The trigger gaps: the short ones for the first `FIRST_QUIETS` quiet
/// periods while an eligible flavour has not completed, the normal ones after.
pub fn trigger_gaps(s: &Shared) -> (Duration, Duration) {
    let started = s.next_epoch.load(std::sync::atomic::Ordering::SeqCst).saturating_sub(1);
    if started < FIRST_QUIETS && any_pending(s, &crate::rules::quiet_hooks()) {
        (FIRST_MIN_GAP, FIRST_MAX_GAP)
    } else {
        (QUIET_MIN_GAP, QUIET_MAX_GAP)
    }
}

/// The time the trigger's gap counts from: the end of the last quiet period,
/// or the end of its fault pause when that is later (a pause outlives a
/// check that finishes early), so faults get a gap between two periods.
pub fn gap_start(s: &Shared) -> Instant {
    let end = *s.last_quiet_end.lock().unwrap_or_else(|e| e.into_inner());
    let pause = *s.fault_pause_until.lock().unwrap_or_else(|e| e.into_inner());
    pause.map_or(end, |p| p.max(end))
}

/// Runs the fault pause command for `secs` seconds (no-op without it).
pub fn pause_faults(s: &Shared, secs: u64) {
    let Some(bin) = std::env::var_os("ANTITHESIS_STOP_FAULTS") else { return };
    *s.fault_pause_until.lock().unwrap_or_else(|e| e.into_inner()) = Some(Instant::now() + Duration::from_secs(secs));
    let status = std::process::Command::new(&bin).arg(secs.to_string()).status();
    if std::env::var_os("ANTITHESIS_OUTPUT_DIR").is_some() {
        let ok = status.as_ref().is_ok_and(|s| s.success());
        check_always!(ok, "the fault pause command succeeds", json!({"seconds": secs, "status": format!("{status:?}")}));
    }
}

/// Per process: the flavours that completed a run within their budget, and
/// the runs (Q5 reached) of each flavour. Recorded by `run_quiet`; `exact`
/// counts as completed only through `Shared::exact_completed`, which also
/// needs every phase done.
#[derive(Default)]
pub struct FlavourRuns(std::sync::Mutex<(BTreeSet<&'static str>, std::collections::BTreeMap<&'static str, u64>)>);

pub fn flavour_completed(s: &Shared, name: &str) -> bool {
    if name == "exact" {
        return s.exact_completed();
    }
    s.ext::<FlavourRuns>().0.lock().unwrap_or_else(|e| e.into_inner()).0.contains(name)
}

pub fn flavour_runs(s: &Shared, name: &str) -> u64 {
    s.ext::<FlavourRuns>().0.lock().unwrap_or_else(|e| e.into_inner()).1.get(name).copied().unwrap_or(0)
}

fn record_run(s: &Shared, name: &'static str, completed: bool) {
    let r = s.ext::<FlavourRuns>();
    let mut g = r.0.lock().unwrap_or_else(|e| e.into_inner());
    *g.1.entry(name).or_insert(0) += 1;
    if completed {
        g.0.insert(name);
    }
}

/// The base weights: `weight(&Shared)`, 0 for `needs_oracle` hooks in
/// degraded mode (0 = not eligible now).
fn base_weights(s: &Shared, hooks: &[QuietHook]) -> Vec<f64> {
    hooks.iter().map(|h| if h.needs_oracle && !s.oracle_ok() { 0.0 } else { (h.weight)(s).max(0.0) }).collect()
}

/// A flavour is pending while it is eligible, has not completed in this
/// process and ran fewer than `PENDING_RUNS` times (so a flavour that never
/// completes does not hold the schedule).
fn pending(s: &Shared, h: &QuietHook, base: f64) -> bool {
    base > 0.0 && !flavour_completed(s, h.name) && flavour_runs(s, h.name) < PENDING_RUNS
}

/// Whether an eligible flavour is still pending.
pub fn any_pending(s: &Shared, hooks: &[QuietHook]) -> bool {
    hooks.iter().zip(base_weights(s, hooks)).any(|(h, w)| pending(s, h, w))
}

/// The flavour weights, in three stages per process:
///
/// 1. while `exact` is pending, the base weights (`exact` dominates them:
///    it feeds every oracle quiet check);
/// 2. then, while any flavour is pending, `PENDING_WEIGHT` for each pending
///    flavour and 0 for the others: every flavour gets its turn within the
///    few quiet periods of a typical timeline, at most `PENDING_RUNS` each
///    (run 076d255c…-67-0: 144 of 165 draws were `exact`, and
///    `numscript_state` and `stats_convergence` were never drawn);
/// 3. then the base weights again (`exact` 10, 50 when overdue).
pub fn flavour_weights(s: &Shared, hooks: &[QuietHook]) -> Vec<f64> {
    let base = base_weights(s, hooks);
    let pend: Vec<bool> = hooks.iter().zip(&base).map(|(h, w)| pending(s, h, *w)).collect();
    let exact_first = hooks.iter().zip(&pend).any(|(h, p)| h.name == "exact" && *p);
    if exact_first || !pend.contains(&true) {
        return base;
    }
    pend.iter().map(|p| if *p { PENDING_WEIGHT } else { 0.0 }).collect()
}

/// The invariant.
pub fn quiet_period(d: &Driver, tc: TestCase) {
    let s = &d.s;
    if !s.is_active() {
        return;
    }
    let p = s.cfg.quiet_probability.unwrap_or_else(|| {
        let (min_gap, max_gap) = trigger_gaps(s);
        elapsed_probability(gap_start(s).elapsed(), min_gap, max_gap)
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
    pause_faults(s, t.q0_pause_secs);
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
            pause_faults(s, h.budget.as_secs() + t.pause_margin_secs);
            ctx.deadline = Instant::now() + h.budget;
            (h.run)(d, &mut ctx, plan);
            let done = Instant::now() <= ctx.deadline;
            record_run(s, h.name, done);
            done
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
        let mid = elapsed_probability(Duration::from_secs(150), QUIET_MIN_GAP, QUIET_MAX_GAP);
        assert!(mid > P_LOW && mid < P_HIGH);
    }

    fn complete_all(s: &Shared) {
        s.exact_completed.store(true, Ordering::SeqCst);
        for h in crate::rules::quiet_hooks() {
            record_run(s, h.name, true);
        }
    }

    #[test]
    fn the_first_quiet_periods_use_short_gaps_while_a_flavour_is_pending() {
        let (d, _, _) = testkit::driver();
        let s = &d.s;
        assert_eq!(trigger_gaps(s), (FIRST_MIN_GAP, FIRST_MAX_GAP));
        s.next_epoch.store(1 + FIRST_QUIETS - 1, Ordering::SeqCst);
        assert_eq!(trigger_gaps(s), (FIRST_MIN_GAP, FIRST_MAX_GAP));
        s.next_epoch.store(1 + FIRST_QUIETS, Ordering::SeqCst);
        assert_eq!(trigger_gaps(s), (QUIET_MIN_GAP, QUIET_MAX_GAP), "bounded: a failing flavour does not keep the short gaps");
        s.next_epoch.store(1, Ordering::SeqCst);
        s.exact_completed.store(true, Ordering::SeqCst);
        assert_eq!(trigger_gaps(s), (FIRST_MIN_GAP, FIRST_MAX_GAP), "exact done, the other flavours are still pending");
        complete_all(s);
        assert_eq!(trigger_gaps(s), (QUIET_MIN_GAP, QUIET_MAX_GAP), "every flavour completed");
    }

    #[test]
    fn exact_dominates_until_done_then_pending_flavours_take_turns() {
        let (d, _, _) = testkit::driver();
        let s = &d.s;
        let hooks = crate::rules::quiet_hooks();
        let idx = |n: &str| hooks.iter().position(|h| h.name == n).unwrap();
        let w = flavour_weights(s, &hooks);
        assert_eq!(w, base_weights(s, &hooks), "exact pending: the base weights");
        assert_eq!(w[idx("exact")], crate::rules::quiet_exact::OVERDUE_WEIGHT);
        // One failed exact run: still pending.
        record_run(s, "exact", false);
        assert_eq!(flavour_weights(s, &hooks), base_weights(s, &hooks));

        s.exact_completed.store(true, Ordering::SeqCst);
        let w = flavour_weights(s, &hooks);
        assert_eq!(w[idx("exact")], 0.0, "exact completed: it waits for the pending flavours");
        assert_eq!(w[idx("restore_cut")], 0.0, "an ineligible flavour stays out of the draw");
        for n in ["numscript_state", "stats_convergence", "cross_node_identity", "cluster_config_convergence", "quiesce_and_absence", "eventually_correct"] {
            assert_eq!(w[idx(n)], PENDING_WEIGHT, "{n}");
        }
        // A completed flavour leaves; a failing one leaves after PENDING_RUNS runs.
        record_run(s, "numscript_state", true);
        record_run(s, "stats_convergence", false);
        let w = flavour_weights(s, &hooks);
        assert_eq!((w[idx("numscript_state")], w[idx("stats_convergence")]), (0.0, PENDING_WEIGHT));
        record_run(s, "stats_convergence", false);
        assert_eq!(flavour_weights(s, &hooks)[idx("stats_convergence")], 0.0);
        assert_eq!(flavour_runs(s, "stats_convergence"), PENDING_RUNS);
        assert!(!flavour_completed(s, "stats_convergence"));
        complete_all(s);
        assert!(!any_pending(s, &hooks));
        assert_eq!(flavour_weights(s, &hooks), base_weights(s, &hooks), "nothing pending: the base weights");

        // An exact that never completes stops dominating after PENDING_RUNS runs.
        let (d, _, _) = testkit::driver();
        record_run(&d.s, "exact", false);
        record_run(&d.s, "exact", false);
        let w = flavour_weights(&d.s, &hooks);
        assert_eq!((w[idx("exact")], w[idx("numscript_state")]), (0.0, PENDING_WEIGHT));

        // Degraded mode: exact is out, so the pending Rust flavours take turns at once.
        let (d, _, _) = testkit::driver();
        d.s.enter_degraded("test");
        let w = flavour_weights(&d.s, &hooks);
        assert_eq!((w[idx("exact")], w[idx("stats_convergence")], w[idx("numscript_state")]), (0.0, 0.0, PENDING_WEIGHT));
    }

    /// A process's schedule: exact first (usually), then every other
    /// eligible flavour within a bounded number of quiet periods, whatever
    /// the draws (also the all-minimal ones) and whether the runs complete.
    /// Run 076d255c…-67-0 never drew `numscript_state` nor
    /// `stats_convergence` in 165 quiet periods.
    #[test]
    fn every_flavour_is_drawn_within_a_bounded_number_of_quiet_periods() {
        let hooks = crate::rules::quiet_hooks();
        let eligible: Vec<&str> = {
            let (d, _, _) = testkit::driver();
            hooks.iter().zip(base_weights(&d.s, &hooks)).filter(|(_, w)| *w > 0.0).map(|(h, _)| h.name).collect()
        };
        assert!(eligible.contains(&"numscript_state") && eligible.contains(&"stats_convergence"), "{eligible:?}");
        let bound = PENDING_RUNS as usize * eligible.len();
        let firsts = std::sync::Mutex::new(Vec::new());
        testkit::with_tc_n(200, |tc| {
            let (d, _, _) = testkit::driver();
            let s = &d.s;
            let mut drawn = BTreeSet::new();
            let mut periods = 0;
            while any_pending(s, &hooks) {
                assert!(periods < bound, "{periods} periods, drawn {drawn:?}");
                let i = draw::weighted(tc, &flavour_weights(s, &hooks)).unwrap();
                if periods == 0 {
                    firsts.lock().unwrap().push(hooks[i].name);
                }
                periods += 1;
                drawn.insert(hooks[i].name);
                // A drawn run completes half of the time.
                let done = draw::chance(tc, 0.5);
                if hooks[i].name == "exact" && done {
                    s.exact_completed.store(true, Ordering::SeqCst);
                }
                record_run(s, hooks[i].name, done);
            }
            assert_eq!(drawn.len(), eligible.len(), "{drawn:?}");
        });
        let firsts = firsts.into_inner().unwrap();
        let exact = firsts.iter().filter(|n| **n == "exact").count();
        assert!(exact * 4 > firsts.len() * 3, "exact leads the first period: {exact} of {}", firsts.len());
    }

    #[test]
    fn the_gap_counts_from_the_end_of_the_fault_pause() {
        let (d, _, _) = testkit::driver();
        let s = &d.s;
        let end = Instant::now();
        *s.last_quiet_end.lock().unwrap() = end;
        assert_eq!(gap_start(s), end);
        *s.fault_pause_until.lock().unwrap() = Some(end + Duration::from_secs(100));
        assert_eq!(gap_start(s), end + Duration::from_secs(100));
        *s.fault_pause_until.lock().unwrap() = Some(end - Duration::from_secs(5));
        assert_eq!(gap_start(s), end);
    }

    #[test]
    fn budget_table_fits_the_pause_limit() {
        assert_eq!(P_LOW, 1.0 / 5000.0);
        assert_eq!(P_HIGH, 1.0 / 10.0);
        assert_eq!(QUIET_MIN_GAP, Duration::from_secs(60));
        assert_eq!(QUIET_MAX_GAP, Duration::from_secs(240));
        assert_eq!((FIRST_MIN_GAP, FIRST_MAX_GAP, FIRST_QUIETS), (Duration::from_secs(30), Duration::from_secs(120), 5));
        assert!(FIRST_MIN_GAP < FIRST_MAX_GAP && FIRST_MAX_GAP <= QUIET_MAX_GAP && QUIET_MIN_GAP < QUIET_MAX_GAP);
        // An `exact` drawn at the end of the first gap (Q0 pause, which bounds
        // Q1–Q4, plus its budget) finishes within 6.5 minutes of workload.
        let exact = crate::rules::quiet_hooks().into_iter().find(|h| h.name == "exact").expect("exact registered");
        assert!(FIRST_MAX_GAP.as_secs() + Q0_PAUSE_SECS + exact.budget.as_secs() <= 390);
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
        assert!(!flavour_completed(&d.s, "test"));
        run_quiet(&d, Some(&hook(run_counts)), Some(Box::new(5u32)));
        assert_eq!(ran(&d), 1);
        assert!(flavour_completed(&d.s, "test"), "a run within budget marks the flavour completed");
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
        assert!(!flavour_completed(&d.s, "test"));
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
