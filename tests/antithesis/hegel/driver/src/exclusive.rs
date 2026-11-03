//! The `exclusive` invariant (DESIGN §4.11, §6.7): with every rule parked
//! (resolvers and jobs may still run), a hegel-drawn registered
//! `ExclusiveAction` plans (all draws) and then runs (no draws).
#![allow(dead_code)] // family-facing API

use crate::draw;
use crate::machine::Driver;
use crate::quiet::{elapsed_probability, HookPlan};
use hegel::TestCase;
use std::time::{Duration, Instant};

pub const EXCL_MIN_GAP: Duration = Duration::from_secs(120);
pub const EXCL_MAX_GAP: Duration = Duration::from_secs(600);
/// The budget an exclusive action has to restore what it changed.
pub const EXCL_RESTORE_BUDGET: Duration = Duration::from_secs(20);

pub struct ExclusiveAction {
    pub name: &'static str,
    pub weight: f64,
    pub plan: fn(&Driver, &TestCase) -> HookPlan,
    pub run: fn(&Driver, &HookPlan),
}

/// The invariant.
pub fn exclusive(d: &Driver, tc: TestCase) {
    let s = &d.s;
    if !s.is_active() {
        return;
    }
    let actions = crate::rules::exclusive_actions();
    run_exclusive(d, &tc, &actions);
}

/// Draws the trigger, the action and its plan, then runs it.
pub fn run_exclusive(d: &Driver, tc: &TestCase, actions: &[ExclusiveAction]) -> Option<&'static str> {
    let s = &d.s;
    if actions.is_empty() {
        return None;
    }
    let p = s.cfg.exclusive_probability.unwrap_or_else(|| {
        let last = *s.last_exclusive_end.lock().unwrap_or_else(|e| e.into_inner());
        elapsed_probability(last.elapsed(), EXCL_MIN_GAP, EXCL_MAX_GAP)
    });
    if !draw::chance(tc, p) {
        return None;
    }
    let weights: Vec<f64> = actions.iter().map(|a| a.weight).collect();
    let i = draw::weighted(tc, &weights)?;
    let a = &actions[i];
    let plan = (a.plan)(d, tc);
    let started = Instant::now();
    (a.run)(d, &plan);
    *s.last_exclusive_end.lock().unwrap_or_else(|e| e.into_inner()) = Instant::now();
    println!("driver: exclusive action {} took {} ms", a.name, started.elapsed().as_millis());
    Some(a.name)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testkit;
    use std::sync::atomic::{AtomicUsize, Ordering};

    static RUNS: AtomicUsize = AtomicUsize::new(0);

    fn plan(_: &Driver, tc: &TestCase) -> HookPlan {
        Box::new(draw::int_in(tc, 0, 3))
    }
    fn run(_: &Driver, p: &HookPlan) {
        assert!(*p.downcast_ref::<u64>().unwrap() <= 3);
        RUNS.fetch_add(1, Ordering::SeqCst);
    }

    #[test]
    fn draws_an_action_by_weight_then_runs_it() {
        let (d, _, _) = testkit::driver();
        let actions = [ExclusiveAction { name: "a", weight: 1.0, plan, run }, ExclusiveAction { name: "b", weight: 0.0, plan, run }];
        let before = RUNS.load(Ordering::SeqCst);
        testkit::with_tc_n(10, |tc| assert_eq!(run_exclusive(&d, tc, &actions), Some("a")));
        assert!(RUNS.load(Ordering::SeqCst) >= before + 10);
        testkit::with_tc_n(1, |tc| assert_eq!(run_exclusive(&d, tc, &[]), None));
    }
}
