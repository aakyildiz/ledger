//! Assertion macros (DESIGN §6.6). Every Rust assertion of the driver goes
//! through one of these: each expands to the matching `antithesis_sdk` macro
//! with the same literal (so the SDK catalog sees it) and, in unit tests, also
//! records `(kind, message, cond, details)` into a sink that
//! `testkit::assertions()` returns.
//!
//! The sink is thread-local. Threads the driver spawns (resolvers, jobs,
//! scoped dispatch threads, the late poster) inherit the sink of the thread
//! that spawned them through `spawn_inheriting` / `scoped_inheriting`, so a
//! test sees what the helper threads of its own `Shared` asserted.
use serde_json::Value;

/// One recorded assertion evaluation (unit tests only).
#[derive(Clone, Debug)]
#[allow(dead_code)] // family-facing: read by unit tests through testkit::assertions()
pub struct Recorded {
    pub kind: &'static str,
    pub message: &'static str,
    pub cond: bool,
    pub details: Value,
}

#[cfg(test)]
pub type Sink = Option<std::sync::Arc<std::sync::Mutex<Vec<Recorded>>>>;
#[cfg(not(test))]
pub type Sink = ();

#[cfg(test)]
thread_local! {
    static SINK: std::cell::RefCell<Sink> = const { std::cell::RefCell::new(None) };
}

/// The current thread's sink (created on first use in tests).
#[cfg(test)]
pub fn current_sink() -> Sink {
    SINK.with(|s| {
        let mut s = s.borrow_mut();
        if s.is_none() {
            *s = Some(Default::default());
        }
        s.clone()
    })
}
#[cfg(not(test))]
pub fn current_sink() -> Sink {}

/// Installs a sink on the current thread (a spawned helper thread).
#[cfg(test)]
pub fn install_sink(sink: Sink) {
    SINK.with(|s| *s.borrow_mut() = sink);
}
#[cfg(not(test))]
pub fn install_sink(_sink: Sink) {}

/// Records one evaluation. A no-op outside unit tests.
#[cfg(test)]
pub fn record(kind: &'static str, message: &'static str, cond: bool, details: &Value) {
    if let Some(sink) = current_sink() {
        sink.lock().unwrap_or_else(|e| e.into_inner()).push(Recorded { kind, message, cond, details: details.clone() });
    }
}
#[cfg(not(test))]
#[inline(always)]
pub fn record(_kind: &'static str, _message: &'static str, _cond: bool, _details: &Value) {}

/// Spawns a named thread that inherits the caller's assertion sink.
pub fn spawn_inheriting<F>(name: &str, f: F) -> std::thread::JoinHandle<()>
where
    F: FnOnce() + Send + 'static,
{
    let sink = current_sink();
    std::thread::Builder::new()
        .name(name.to_string())
        .spawn(move || {
            install_sink(sink);
            f()
        })
        .expect("spawn driver thread")
}

/// Spawns a scoped thread that inherits the caller's assertion sink.
pub fn scoped_inheriting<'scope, 'env, T, F>(s: &'scope std::thread::Scope<'scope, 'env>, f: F) -> std::thread::ScopedJoinHandle<'scope, T>
where
    F: FnOnce() -> T + Send + 'scope,
    T: Send + 'scope,
{
    let sink = current_sink();
    s.spawn(move || {
        install_sink(sink);
        f()
    })
}

/// Converts the details argument of a check macro (a `Value` or a `&Value`).
#[doc(hidden)]
pub fn details_of<T: std::borrow::Borrow<Value>>(d: &T) -> Value {
    d.borrow().clone()
}

/// `check_always!(cond, "literal", details)`
macro_rules! check_always {
    ($cond:expr, $msg:literal) => {
        check_always!($cond, $msg, ::serde_json::json!({}))
    };
    ($cond:expr, $msg:literal, $details:expr) => {{
        let __c: bool = $cond;
        let __d: ::serde_json::Value = $crate::assertions::details_of(&$details);
        ::antithesis_sdk::assert_always!(__c, $msg, &__d);
        $crate::assertions::record("Always", $msg, __c, &__d);
    }};
}

/// `check_always_or_unreachable!(cond, "literal", details)`
macro_rules! check_always_or_unreachable {
    ($cond:expr, $msg:literal) => {
        check_always_or_unreachable!($cond, $msg, ::serde_json::json!({}))
    };
    ($cond:expr, $msg:literal, $details:expr) => {{
        let __c: bool = $cond;
        let __d: ::serde_json::Value = $crate::assertions::details_of(&$details);
        ::antithesis_sdk::assert_always_or_unreachable!(__c, $msg, &__d);
        $crate::assertions::record("AlwaysOrUnreachable", $msg, __c, &__d);
    }};
}

/// `check_sometimes!(cond, "literal", details)`
macro_rules! check_sometimes {
    ($cond:expr, $msg:literal) => {
        check_sometimes!($cond, $msg, ::serde_json::json!({}))
    };
    ($cond:expr, $msg:literal, $details:expr) => {{
        let __c: bool = $cond;
        let __d: ::serde_json::Value = $crate::assertions::details_of(&$details);
        ::antithesis_sdk::assert_sometimes!(__c, $msg, &__d);
        $crate::assertions::record("Sometimes", $msg, __c, &__d);
    }};
}

/// `check_reachable!("literal", details)`
macro_rules! check_reachable {
    ($msg:literal) => {
        check_reachable!($msg, ::serde_json::json!({}))
    };
    ($msg:literal, $details:expr) => {{
        let __d: ::serde_json::Value = $crate::assertions::details_of(&$details);
        ::antithesis_sdk::assert_reachable!($msg, &__d);
        $crate::assertions::record("Reachable", $msg, true, &__d);
    }};
}

/// `check_unreachable!("literal", details)`
macro_rules! check_unreachable {
    ($msg:literal) => {
        check_unreachable!($msg, ::serde_json::json!({}))
    };
    ($msg:literal, $details:expr) => {{
        let __d: ::serde_json::Value = $crate::assertions::details_of(&$details);
        ::antithesis_sdk::assert_unreachable!($msg, &__d);
        $crate::assertions::record("Unreachable", $msg, false, &__d);
    }};
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    #[test]
    fn macros_record_kind_message_condition_and_details() {
        crate::testkit::reset();
        let v = json!({"a": 1});
        check_always!(true, "test: always", v);
        check_always!(false, "test: always", &v);
        check_sometimes!(true, "test: sometimes");
        check_reachable!("test: reachable", json!({"b": 2}));
        check_unreachable!("test: unreachable");
        check_always_or_unreachable!(true, "test: aou");
        let got = crate::testkit::assertions();
        let kinds: Vec<(&str, &str, bool)> = got.iter().map(|r| (r.kind, r.message, r.cond)).collect();
        assert_eq!(
            kinds,
            vec![
                ("Always", "test: always", true),
                ("Always", "test: always", false),
                ("Sometimes", "test: sometimes", true),
                ("Reachable", "test: reachable", true),
                ("Unreachable", "test: unreachable", false),
                ("AlwaysOrUnreachable", "test: aou", true),
            ]
        );
        assert_eq!(got[0].details, json!({"a": 1}));
        assert_eq!(got[3].details, json!({"b": 2}));
        crate::testkit::reset();
        assert!(crate::testkit::assertions().is_empty());
    }

    #[test]
    fn helper_threads_inherit_the_sink() {
        crate::testkit::reset();
        super::spawn_inheriting("t", || check_reachable!("test: from a thread")).join().unwrap();
        std::thread::scope(|s| {
            super::scoped_inheriting(s, || check_reachable!("test: from a scoped thread")).join().unwrap();
        });
        let got = crate::testkit::assertions();
        assert_eq!(got.len(), 2);
    }
}
