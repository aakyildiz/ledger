//! Task 12: log-derived server signals (server-signals.md §4, §5).
//!
//! One `LogHook` sees every forwarded line after the built-in handling of
//! `logs.rs`. It matches on the JSON `msg` field of a structured record
//! (prefix match) and falls back to a substring match on non-JSON lines.
//!
//! - Red flags (Unreachable, one literal, the line in the details) run from
//!   boot on server lines (main cluster and restore targets).
//! - Every Reachable is evaluated only while `line.active`, so setup-time
//!   lines never satisfy a property meant to show behaviour under load.
//! - Per-node counters of each signal are kept in `SignalState` (first
//!   occurrence per node is printed).
use crate::logs::{LogHook, LogLine};
use crate::shared::Shared;
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;

pub fn log_hooks() -> Vec<LogHook> {
    vec![LogHook { name: "server_signals", run: on_line }]
}

/// Which kind of process forwarded a line (by its `HOSTNAME`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Source {
    Ledger,
    Client,
    Restore,
    Other,
}

pub fn source_of(node: &str) -> Source {
    if node.starts_with("ledger-") {
        Source::Ledger
    } else if node.starts_with("client") {
        Source::Client
    } else if node.starts_with("restore-") {
        Source::Restore
    } else {
        Source::Other
    }
}

/// Signals whose Reachable shows that a server knob took effect.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Signal {
    WalSnapshot,
    RaftSnapshotReceived,
    CheckpointRestored,
    SpoolReplayed,
    QueryCheckpointMainStore,
    CacheEpochReset,
    BloomRebuild,
    BloomPopulateRaced,
}

/// A node stopping itself on an unrecoverable command failure (printed, not asserted).
pub const FAIL_STOP: &str = "Terminal Raft command failure; stopping node";

/// Line prefixes (server-signals §4) → signal. `[D]` lines need `LOG_LEVEL=debug`.
pub const SIGNALS: [(&str, Signal); 8] = [
    ("Snapshot created", Signal::WalSnapshot),
    ("Applying snapshot sent by leader", Signal::RaftSnapshotReceived),
    ("Checkpoint restored successfully", Signal::CheckpointRestored),
    ("Unspooling operation terminated, resuming...", Signal::SpoolReplayed), // [D]
    ("Created main store query checkpoint", Signal::QueryCheckpointMainStore), // [D]
    ("Applying cluster config change: resetting cache and purging 0xFF", Signal::CacheEpochReset),
    ("Bloom filter config changed: purging blocks and rebuilding", Signal::BloomRebuild),
    ("Bloom task skipped SetReady: Rebuild occurred", Signal::BloomPopulateRaced),
];

/// Invariant red flags (server-signals §4 "Red flags"). Not
/// `Terminal Raft command failure; stopping node` (`node.go:263-266`): its
/// only source is a failed persist of the conf state after a force-remove
/// (`node.go:3558`), an I/O error, so it is a correct fail-stop under disk
/// faults. It is logged as `FAIL_STOP` below, without an assertion.
pub const RED_FLAGS: [&str; 10] = [
    "SENTINEL DIAG:",
    "CACHE COHERENCE:",
    "POST-COMMIT VOLUME ASSERTION FAILED",
    "AGGREGATED VOLUME BALANCE CHECK FAILED",
    "PERSISTED VOLUME UPDATE at invariant violation",
    "TRANSIENT VOLUME at invariant violation",
    "Preload boundary mismatch",
    "preload coverage miss: read of undeclared key",
    "Pebble lags WAL snapshot",
    "Leadership gained Ready has no entries",
];

impl Signal {
    pub fn name(self) -> &'static str {
        match self {
            Signal::WalSnapshot => "wal-snapshot",
            Signal::RaftSnapshotReceived => "raft-snapshot-received",
            Signal::CheckpointRestored => "checkpoint-restored",
            Signal::SpoolReplayed => "spool-replayed",
            Signal::QueryCheckpointMainStore => "query-checkpoint-main-store",
            Signal::CacheEpochReset => "cache-epoch-reset",
            Signal::BloomRebuild => "bloom-rebuild",
            Signal::BloomPopulateRaced => "bloom-populate-raced",
        }
    }

    /// One literal per signal (the SDK catalog needs literals at call sites).
    fn reach(self, d: &Value) {
        match self {
            Signal::WalSnapshot => check_reachable!("a node takes a WAL snapshot", d),
            Signal::RaftSnapshotReceived => check_reachable!("a follower receives a raft snapshot", d),
            Signal::CheckpointRestored => check_reachable!("a follower restores a checkpoint from the leader", d),
            Signal::SpoolReplayed => check_reachable!("a gated window completes with spool replay", d),
            Signal::QueryCheckpointMainStore => check_reachable!("a node creates a main store query checkpoint", d),
            Signal::CacheEpochReset => check_reachable!("a cluster config change resets the cache epoch", d),
            Signal::BloomRebuild => check_reachable!("a bloom config change triggers a rebuild", d),
            Signal::BloomPopulateRaced => check_reachable!("a bloom populate races a rebuild", d),
        }
    }
}

/// Log-derived counters and once-flags.
#[derive(Default)]
pub struct SignalState {
    pub counts: Mutex<BTreeMap<(String, Signal), u64>>,
    seen_parsed: AtomicBool,
    seen_ledger: AtomicBool,
    seen_client: AtomicBool,
    seen_restore: AtomicBool,
}

impl SignalState {
    #[allow(dead_code)] // read by tests and for debugging
    pub fn count(&self, node: &str, sig: Signal) -> u64 {
        self.counts.lock().unwrap_or_else(|e| e.into_inner()).get(&(node.to_string(), sig)).copied().unwrap_or(0)
    }
}

/// The text to match: the `msg` of a structured record, else the raw line.
pub fn message<'a>(l: &LogLine<'a>) -> (&'a str, bool) {
    match l.json.and_then(|j| j.get("msg")).and_then(Value::as_str) {
        Some(m) => (m, true),
        None => (l.raw, false),
    }
}

fn matches(msg: &str, structured: bool, pat: &str) -> bool {
    if structured {
        msg.starts_with(pat)
    } else {
        msg.contains(pat)
    }
}

/// The red flag a line carries, if any.
pub fn red_flag(msg: &str, structured: bool) -> Option<&'static str> {
    RED_FLAGS.iter().copied().find(|f| matches(msg, structured, f))
}

/// The knob signal a line carries, if any.
pub fn signal(msg: &str, structured: bool) -> Option<Signal> {
    SIGNALS.iter().find(|(p, _)| matches(msg, structured, p)).map(|(_, s)| *s)
}

pub fn on_line(s: &Shared, l: &LogLine) {
    let src = source_of(l.node);
    let st = s.ext::<SignalState>();
    if l.active {
        let once = |flag: &AtomicBool| !flag.swap(true, Ordering::Relaxed);
        match src {
            Source::Ledger if once(&st.seen_ledger) => check_reachable!("the driver receives ledger server logs", json!({"node": l.node})),
            // 00c's `the driver receives client logs` (logs.rs, ungated) keeps its
            // literal; this one is gated on the workload being active.
            Source::Client if once(&st.seen_client) => check_reachable!("the driver receives client logs while the workload runs", json!({"node": l.node})),
            Source::Restore if once(&st.seen_restore) => check_reachable!("the driver receives restore-target logs", json!({"node": l.node})),
            _ => {}
        }
    }
    if !matches!(src, Source::Ledger | Source::Restore) {
        return;
    }
    let (msg, structured) = message(l);
    let node_id = l.json.and_then(|j| j.get("node-id")).cloned().unwrap_or(Value::Null);
    if structured && l.active && !st.seen_parsed.swap(true, Ordering::Relaxed) {
        check_reachable!("the driver parses a structured server log record", json!({"node": l.node, "msg": msg}));
    }
    if let Some(flag) = red_flag(msg, structured) {
        println!("driver: red flag from {}: {}", l.node, l.raw);
        check_unreachable!("a server logs an invariant red flag", json!({"node": l.node, "nodeId": node_id, "flag": flag, "line": l.raw}));
    } else if matches(msg, structured, FAIL_STOP) {
        println!("driver: {} stopped on a terminal raft command failure: {}", l.node, l.raw);
    }
    if src != Source::Ledger {
        return;
    }
    let Some(sig) = signal(msg, structured) else { return };
    let first = {
        let mut c = st.counts.lock().unwrap_or_else(|e| e.into_inner());
        let n = c.entry((l.node.to_string(), sig)).or_insert(0);
        *n += 1;
        *n == 1
    };
    if first {
        println!("driver: signal {} first seen on {}", sig.name(), l.node);
    }
    if l.active {
        sig.reach(&json!({"node": l.node, "nodeId": node_id, "msg": msg}));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testkit;

    fn feed(s: &Shared, node: &str, active: bool, raw: &str) {
        let parsed: Option<Value> = if raw.trim_start().starts_with('{') { serde_json::from_str(raw).ok() } else { None };
        on_line(s, &LogLine { node, boot: 1, active, raw, json: parsed.as_ref() });
    }

    fn rec(msg: &str) -> Vec<crate::assertions::Recorded> {
        testkit::assertions().into_iter().filter(|r| r.message == msg).collect()
    }

    // Sample records shaped as server-signals §4 describes them.
    const SAMPLES: [(&str, &str); 8] = [
        (r#"{"level":"INFO","time":"2026-11-03T10:00:00Z","msg":"Snapshot created","index":1200,"node-id":1}"#, "a node takes a WAL snapshot"),
        (r#"{"level":"INFO","time":"2026-11-03T10:00:00Z","msg":"Applying snapshot sent by leader","index":1300,"snapshotSize":4096,"node-id":2}"#, "a follower receives a raft snapshot"),
        (r#"{"level":"INFO","time":"2026-11-03T10:00:00Z","msg":"Checkpoint restored successfully","checkpointId":7,"node-id":2}"#, "a follower restores a checkpoint from the leader"),
        (r#"{"level":"DEBUG","time":"2026-11-03T10:00:00Z","msg":"Unspooling operation terminated, resuming...","node-id":3}"#, "a gated window completes with spool replay"),
        (r#"{"level":"DEBUG","time":"2026-11-03T10:00:00Z","msg":"Created main store query checkpoint","node-id":1}"#, "a node creates a main store query checkpoint"),
        (r#"{"level":"INFO","time":"2026-11-03T10:00:00Z","msg":"Applying cluster config change: resetting cache and purging 0xFF","oldThreshold":50,"newThreshold":64,"raftIndex":900,"node-id":1}"#, "a cluster config change resets the cache epoch"),
        (r#"{"level":"INFO","time":"2026-11-03T10:00:00Z","msg":"Bloom filter config changed: purging blocks and rebuilding","raftIndex":901,"node-id":1}"#, "a bloom config change triggers a rebuild"),
        (r#"{"level":"INFO","time":"2026-11-03T10:00:00Z","msg":"Bloom task skipped SetReady: Rebuild occurred","reason":"rebuild","node-id":2}"#, "a bloom populate races a rebuild"),
    ];

    #[test]
    fn signals_reach_only_while_active() {
        let (d, _, _) = testkit::driver();
        testkit::reset();
        for (line, _) in SAMPLES {
            feed(&d.s, "ledger-0", false, line);
        }
        let a = testkit::assertions();
        assert!(a.iter().all(|r| r.kind != "Reachable"), "inactive lines hit no Reachable: {a:?}");
        let st = d.s.ext::<SignalState>();
        assert_eq!(st.count("ledger-0", Signal::WalSnapshot), 1, "counted even while inactive");

        for (line, msg) in SAMPLES {
            feed(&d.s, "ledger-1", true, line);
            let r = rec(msg);
            assert_eq!(r.len(), 1, "{msg}");
            assert_eq!(r[0].details["node"], "ledger-1");
        }
        assert!(testkit::fired("the driver parses a structured server log record"));
        assert!(testkit::fired("the driver receives ledger server logs"));
        assert_eq!(st.count("ledger-1", Signal::BloomPopulateRaced), 1);
    }

    #[test]
    fn without_debug_lines_the_debug_signals_stay_unhit() {
        let (d, _, _) = testkit::driver();
        testkit::reset();
        for (line, _) in SAMPLES.iter().filter(|(l, _)| !l.contains("\"DEBUG\"")) {
            feed(&d.s, "ledger-2", true, line);
        }
        assert!(!testkit::fired("a gated window completes with spool replay"));
        assert!(!testkit::fired("a node creates a main store query checkpoint"));
        assert!(testkit::fired("a node takes a WAL snapshot"));
    }

    #[test]
    fn prefix_match_on_msg_and_substring_fallback() {
        // A structured record matches only at the start of `msg`.
        assert_eq!(signal("Creating snapshot", true), None);
        assert_eq!(signal("Snapshot created", true), Some(Signal::WalSnapshot));
        assert_eq!(signal("not a Snapshot created", true), None);
        // Fallback for non-JSON lines: substring.
        assert_eq!(signal("2026 INFO Snapshot created index=3", false), Some(Signal::WalSnapshot));
        assert_eq!(red_flag("Pebble lags WAL snapshot — SyncWAL durability invariant violated, relying on Raft replay", true), Some("Pebble lags WAL snapshot"));
        assert_eq!(red_flag("Leadership gained", true), None);
        assert_eq!(red_flag("Leadership gained Ready has no entries", true), Some("Leadership gained Ready has no entries"));
    }

    #[test]
    fn red_flags_fire_from_boot_on_server_lines_only() {
        let (d, _, _) = testkit::driver();
        testkit::reset();
        let line = r#"{"level":"ERROR","time":"2026-11-03T10:00:00Z","msg":"CACHE COHERENCE: entry differs from Pebble","node-id":2}"#;
        feed(&d.s, "ledger-1", false, line);
        let r = rec("a server logs an invariant red flag");
        assert_eq!(r.len(), 1);
        assert_eq!(r[0].kind, "Unreachable");
        assert_eq!(r[0].details["line"], line);
        assert_eq!(r[0].details["flag"], "CACHE COHERENCE:");
        feed(&d.s, "restore-0", true, r#"{"msg":"Preload boundary mismatch: x"}"#);
        assert_eq!(rec("a server logs an invariant red flag").len(), 2);
        // A fail-stop on an I/O error is not an invariant red flag.
        feed(&d.s, "ledger-0", true, r#"{"msg":"Terminal Raft command failure; stopping node","error":"persisting confstate after force-remove: no space left on device"}"#);
        assert_eq!(rec("a server logs an invariant red flag").len(), 2);
        // A client relaying an error message is not a server record.
        feed(&d.s, "client1", true, "op=3 error msg=\"Preload boundary mismatch\"");
        // Not a red flag: the message only mentions the phrase later on.
        feed(&d.s, "ledger-0", true, r#"{"msg":"checked: no SENTINEL DIAG: lines"}"#);
        assert_eq!(rec("a server logs an invariant red flag").len(), 2);
    }

    #[test]
    fn per_source_reachables() {
        let (d, _, _) = testkit::driver();
        testkit::reset();
        feed(&d.s, "client2", false, "op=1 ok");
        assert!(!testkit::fired("the driver receives client logs while the workload runs"));
        feed(&d.s, "client2", true, "op=1 ok");
        feed(&d.s, "restore-0", true, "restore-agent: up");
        feed(&d.s, "ledger-0", true, "plain text line");
        assert!(testkit::fired("the driver receives client logs while the workload runs"));
        assert!(testkit::fired("the driver receives restore-target logs"));
        assert!(testkit::fired("the driver receives ledger server logs"));
        // A plain-text line is not a structured record; restore and client lines hit no signal.
        assert!(!testkit::fired("the driver parses a structured server log record"));
        feed(&d.s, "restore-0", true, r#"{"msg":"Snapshot created"}"#);
        assert!(!testkit::fired("a node takes a WAL snapshot"));
        assert_eq!(log_hooks()[0].name, "server_signals");
    }

    #[test]
    fn hook_runs_through_the_log_receiver() {
        let (d, _, _) = testkit::driver();
        testkit::reset();
        let hooks = log_hooks();
        let mut seen = std::collections::HashMap::new();
        crate::logs::handle_datagram(&d.s, &hooks, &mut seen, &json!({"node": "ledger-0", "boot": 1, "seq": 1, "line": SAMPLES[0].0}));
        assert!(testkit::fired("a node takes a WAL snapshot"));
    }
}
