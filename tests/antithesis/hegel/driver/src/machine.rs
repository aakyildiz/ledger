//! The hegel concurrent state machine (DESIGN §6.2, §6.3): all rules in the
//! anonymous group, the two always-run invariants, and the rule wrapper.
use crate::shared::Shared;
use hegel::stateful::{ConcurrentInvariant, ConcurrentRule, ConcurrentStateMachine, ANONYMOUS_GROUP};
use hegel::TestCase;
use serde_json::json;
use std::panic::{catch_unwind, resume_unwind, AssertUnwindSafe};
use std::sync::Arc;
use std::time::Instant;

#[derive(Clone)]
pub struct Driver {
    pub s: Arc<Shared>,
}

impl ConcurrentStateMachine for Driver {
    fn rules(&self) -> Vec<ConcurrentRule<Self>> {
        crate::rules::rules()
    }

    fn invariants(&self) -> Vec<ConcurrentInvariant<Self>> {
        vec![
            ConcurrentInvariant::new_always_run("quiet_period", crate::quiet::quiet_period),
            ConcurrentInvariant::new_always_run("exclusive", crate::exclusive::exclusive),
        ]
    }
}

/// A rule in the anonymous group.
pub fn rule(name: &'static str, weight: f64, f: fn(&Driver, TestCase)) -> ConcurrentRule<Driver> {
    ConcurrentRule::new(name, ANONYMOUS_GROUP, weight, f)
}

thread_local! {
    static LAST_PANIC_LOCATION: std::cell::RefCell<String> = const { std::cell::RefCell::new(String::new()) };
}

/// Installed from inside a hegel test context (the first `guarded` call), so
/// it wraps hegel's own hook, which must exist first.
///
/// Installs (once) a panic hook that remembers the location of the last
/// panic on each thread, then forwards to the previous hook (hegel's, which
/// stores but does not print inside a test context).
pub fn install_location_hook() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        let prev = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            let loc = info.location().map(|l| format!("{}:{}:{}", l.file(), l.line(), l.column())).unwrap_or_default();
            LAST_PANIC_LOCATION.with(|s| *s.borrow_mut() = loc);
            prev(info);
        }));
    });
}

fn last_panic_location() -> String {
    LAST_PANIC_LOCATION.with(|s| s.borrow().clone())
}

/// The text of a `&str`/`String` panic payload.
pub fn panic_text(e: &(dyn std::any::Any + Send)) -> Option<String> {
    if let Some(s) = e.downcast_ref::<&str>() {
        Some(s.to_string())
    } else {
        e.downcast_ref::<String>().cloned()
    }
}

impl Driver {
    /// Runs a rule body (DESIGN §6.3): nothing before the workload is
    /// active; a `&str`/`String` panic is a driver bug, asserted and
    /// swallowed so it never ends the hegel test case; any other payload is
    /// a hegel control unwind and is resumed. Ticket guards' `Drop` runs
    /// during either unwind.
    pub fn guarded(&self, name: &'static str, tc: &TestCase, body: impl FnOnce(&Driver, &TestCase) -> Option<()>) {
        if !self.s.is_active() {
            return;
        }
        // While task 16's signature window could not be closed in time
        // (`signatures_required`, a quiet-blocking job is restoring it), every
        // unsigned write is refused with Unauthenticated. Tracked writes are
        // held back by the slots (DESIGN §4.8 (c)); Rust-checked families would
        // burn their budgets or misjudge their own probes, so every rule waits
        // for the job (task 16's request to 99). Reads lose a few rounds only.
        if self.s.signatures_required() {
            return;
        }
        install_location_hook();
        let started = Instant::now();
        let r = catch_unwind(AssertUnwindSafe(|| body(self, tc)));
        let ms = started.elapsed().as_millis() as u64;
        if started.elapsed() > self.s.timing().slow_rule {
            println!("driver: slow rule {name} took {ms} ms");
        }
        if let Err(e) = r {
            match panic_text(e.as_ref()) {
                Some(text) => {
                    let location = last_panic_location();
                    println!("driver: rule {name} panicked at {location}: {text}");
                    check_unreachable!("a driver rule panics", json!({"rule": name, "panic": text, "location": location}));
                }
                None => resume_unwind(e),
            }
        }
    }

    /// Runs a helper-thread iteration under `catch_unwind`; a panic asserts
    /// `a driver helper thread panics` and returns `None`.
    pub fn helper_iteration<T>(thread: &str, f: impl FnOnce() -> T) -> Option<T> {
        match catch_unwind(AssertUnwindSafe(f)) {
            Ok(v) => Some(v),
            Err(e) => {
                let text = panic_text(e.as_ref()).unwrap_or_else(|| "non-string panic payload".to_string());
                let location = last_panic_location();
                println!("driver: helper thread {thread} panicked at {location}: {text}");
                check_unreachable!("a driver helper thread panics", json!({"thread": thread, "panic": text, "location": location}));
                None
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testkit;

    fn panicking_rule(d: &Driver, tc: TestCase) {
        d.guarded("panicking_rule", &tc, |_, _| panic!("boom {}", 42));
    }

    fn non_string_panic_rule(d: &Driver, tc: TestCase) {
        d.guarded("non_string_panic_rule", &tc, |_, _| std::panic::panic_any(7u32));
    }

    #[test]
    fn guarded_swallows_string_panics_and_asserts() {
        let (d, _, _) = testkit::driver();
        testkit::reset();
        testkit::run_rule(&d, panicking_rule);
        let a = testkit::assertions();
        let hit: Vec<_> = a.iter().filter(|r| r.message == "a driver rule panics").collect();
        assert!(!hit.is_empty());
        assert_eq!(hit[0].details["rule"], "panicking_rule");
        assert_eq!(hit[0].details["panic"], "boom 42");
        assert!(hit[0].details["location"].as_str().unwrap().contains("machine.rs"));
    }

    #[test]
    fn guarded_skips_every_rule_while_signatures_are_required() {
        let (d, _, _) = testkit::driver();
        testkit::reset();
        d.s.signatures_required.store(true, std::sync::atomic::Ordering::SeqCst);
        testkit::run_rule(&d, panicking_rule);
        d.s.signatures_required.store(false, std::sync::atomic::Ordering::SeqCst);
        assert!(testkit::assertions().iter().all(|r| r.message != "a driver rule panics"), "the body did not run");
        testkit::run_rule(&d, panicking_rule);
        assert!(testkit::assertions().iter().any(|r| r.message == "a driver rule panics"));
    }

    #[test]
    fn guarded_resumes_other_payloads() {
        let (d, _, _) = testkit::driver();
        testkit::reset();
        let r = std::panic::catch_unwind(AssertUnwindSafe(|| testkit::run_rule(&d, non_string_panic_rule)));
        assert!(r.is_err(), "a non-string payload must unwind out of the rule");
        assert!(!testkit::fired("a driver rule panics"));
    }

    #[test]
    fn guarded_does_nothing_while_inactive() {
        let (d, _, _) = testkit::driver();
        d.s.active.store(false, std::sync::atomic::Ordering::SeqCst);
        testkit::reset();
        testkit::run_rule(&d, panicking_rule);
        assert!(!testkit::fired("a driver rule panics"));
    }

    #[test]
    fn helper_iteration_asserts_on_panic() {
        testkit::reset();
        assert_eq!(Driver::helper_iteration("t", || 5), Some(5));
        assert_eq!(Driver::helper_iteration("t", || -> u8 { panic!("x") }), None);
        assert!(testkit::fired("a driver helper thread panics"));
    }
}
