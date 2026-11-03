//! Receives forwarded log lines (UDP 7777) from the ledger nodes and client
//! containers, tracks what each node believes about leadership, and hands
//! every line to the registered `LogHook`s after the built-in handling.
use crate::shared::Shared;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::net::UdpSocket;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Weak};

/// One forwarded log line (DESIGN §6.7).
#[allow(dead_code)] // family-facing API: read by log hooks
pub struct LogLine<'a> {
    pub node: &'a str,
    pub boot: u64,
    /// The workload is active (gate Reachable/Sometimes on it).
    pub active: bool,
    pub raw: &'a str,
    /// The line parsed as JSON, when it is a JSON log line.
    pub json: Option<&'a Value>,
}

pub struct LogHook {
    pub name: &'static str,
    pub run: fn(&Shared, &LogLine),
}

#[derive(Clone, Debug, Default)]
pub struct NodeState {
    pub boot: u64,
    pub seq: u64,
    pub leader: bool,
    pub term: u64,
    /// Leadership transitions seen on this boot.
    pub transitions: u64,
    pub boots_seen: u64,
}

pub struct NodeStates {
    pub latest: HashMap<String, NodeState>,
    active: Arc<AtomicBool>,
}

/// Extracts `key=value` (logfmt) or `"key":value` (JSON) from a log line.
fn field(line: &str, key: &str) -> Option<u64> {
    for pat in [format!("{key}="), format!("\"{key}\":")] {
        if let Some(i) = line.find(&pat) {
            let rest = &line[i + pat.len()..];
            let digits: String = rest.trim_start_matches('"').chars().take_while(|c| c.is_ascii_digit()).collect();
            if let Ok(n) = digits.parse() {
                return Some(n);
            }
        }
    }
    None
}

impl NodeStates {
    pub fn new(active: Arc<AtomicBool>) -> Self {
        NodeStates { latest: HashMap::new(), active }
    }

    pub fn handle(&mut self, node: &str, boot: u64, seq: u64, line: &str) {
        let active = self.active.load(Ordering::Relaxed);
        let is_ledger = node.starts_with("ledger-");

        // Guarantees: checked from boot. The binaries are built with -race.
        let race = line.contains("WARNING: DATA RACE");
        check_always!(!race, "no data race is reported by any process", json!({"node": node, "line": line}));
        let panic = line.starts_with("panic:") || line.starts_with("fatal error:");
        check_always!(!panic, "no process panics", json!({"node": node, "line": line}));

        if !is_ledger {
            return;
        }
        let entry = self.latest.entry(node.to_string()).or_default();
        if entry.boot != boot {
            if entry.boot != 0 && active {
                check_reachable!("a ledger node restarts", json!({"node": node}));
            }
            entry.boots_seen += 1;
            *entry = NodeState { boot, seq, boots_seen: entry.boots_seen, ..Default::default() };
        } else if seq < entry.seq {
            return; // out of order
        }
        entry.seq = seq;

        let gained = line.contains("Leadership gained");
        let lost = line.contains("Leadership lost");
        if gained || lost {
            check_reachable!("the driver reads leadership changes from node logs");
            entry.leader = gained;
            entry.transitions += 1;
            if let Some(t) = field(line, "term") {
                entry.term = t;
            }
        } else {
            return;
        }
        if !active {
            return;
        }
        let nodes: Vec<(&String, &NodeState)> = self.latest.iter().collect();
        let leaders: Vec<&(&String, &NodeState)> = nodes.iter().filter(|(_, n)| n.leader).collect();
        check_sometimes!(gained, "a node gains leadership while the workload runs", json!({"node": node}));
        check_sometimes!(lost, "a node loses leadership while the workload runs", json!({"node": node}));
        // Two nodes believe they lead (in different terms): the old leader has
        // not heard of the new one yet.
        check_sometimes!(
            leaders.len() >= 2,
            "two nodes believe they are leader at once",
            json!({"leaders": leaders.iter().map(|(n, s)| json!({"node": n, "term": s.term})).collect::<Vec<_>>()})
        );
        let max_transitions = nodes.iter().map(|(_, n)| n.transitions).max().unwrap_or(0);
        check_sometimes!(max_transitions > 4, "a node changes leadership role many times in one boot", json!({"left": max_transitions, "right": 4}));
    }

    #[allow(dead_code)] // family-facing API
    pub fn leader_hint(&self) -> Option<String> {
        self.latest.iter().filter(|(_, s)| s.leader).max_by_key(|(_, s)| s.term).map(|(n, _)| n.clone())
    }
}

/// Handles one datagram: built-in checks, then every log hook.
pub fn handle_datagram(s: &Shared, hooks: &[LogHook], seen_kinds: &mut HashMap<&'static str, bool>, v: &Value) {
    let node = v.get("node").and_then(Value::as_str).unwrap_or("");
    let boot = v.get("boot").and_then(Value::as_u64).unwrap_or(0);
    let seq = v.get("seq").and_then(Value::as_u64).unwrap_or(0);
    let line = v.get("line").and_then(Value::as_str).unwrap_or("");
    let kind = if node.starts_with("ledger-") {
        "ledger"
    } else if node.starts_with("client") {
        "client"
    } else {
        "other"
    };
    if seen_kinds.insert(kind, true).is_none() {
        match kind {
            "ledger" => check_reachable!("the driver receives ledger node logs"),
            "client" => check_reachable!("the driver receives client logs"),
            _ => {}
        }
    }
    s.node_states.lock().unwrap_or_else(|e| e.into_inner()).handle(node, boot, seq, line);
    if hooks.is_empty() {
        return;
    }
    let parsed: Option<Value> = if line.trim_start().starts_with('{') { serde_json::from_str(line).ok() } else { None };
    let ll = LogLine { node, boot, active: s.is_active(), raw: line, json: parsed.as_ref() };
    for h in hooks {
        let name = h.name;
        crate::machine::Driver::helper_iteration(&format!("log hook {name}"), || (h.run)(s, &ll));
    }
}

/// Starts the log receiver on a UDP port (7777).
pub fn start(s: Weak<Shared>, port: u16) {
    let sock = UdpSocket::bind(("0.0.0.0", port)).expect("bind log port");
    crate::assertions::spawn_inheriting("log-receiver", move || {
        let hooks = crate::rules::log_hooks();
        let mut buf = vec![0u8; 65536];
        let mut seen_kinds: HashMap<&'static str, bool> = HashMap::new();
        loop {
            let Ok(n) = sock.recv(&mut buf) else { continue };
            let Ok(v) = serde_json::from_slice::<Value>(&buf[..n]) else { continue };
            let Some(s) = s.upgrade() else { return };
            handle_datagram(&s, &hooks, &mut seen_kinds, &v);
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testkit;

    fn hook(s: &Shared, l: &LogLine) {
        let _ = s;
        if l.raw.contains("hello") && l.active {
            check_reachable!("test: a log hook sees a line", json!({"node": l.node, "json": l.json.is_some()}));
        }
    }

    #[test]
    fn built_in_checks_then_hooks() {
        let (d, _, _) = testkit::driver();
        testkit::reset();
        let hooks = [LogHook { name: "t", run: hook }];
        let mut seen = HashMap::new();
        handle_datagram(&d.s, &hooks, &mut seen, &json!({"node": "ledger-0", "boot": 1, "seq": 1, "line": "{\"msg\":\"hello\"}"}));
        handle_datagram(&d.s, &hooks, &mut seen, &json!({"node": "ledger-0", "boot": 1, "seq": 2, "line": "Leadership gained term=3"}));
        handle_datagram(&d.s, &hooks, &mut seen, &json!({"node": "ledger-0", "boot": 2, "seq": 1, "line": "panic: x"}));
        let a = testkit::assertions();
        assert!(a.iter().any(|r| r.message == "test: a log hook sees a line" && r.details["json"] == true));
        assert!(a.iter().any(|r| r.message == "the driver receives ledger node logs"));
        assert!(a.iter().any(|r| r.message == "a node gains leadership while the workload runs" && r.cond));
        assert!(a.iter().any(|r| r.message == "no process panics" && !r.cond));
        assert!(a.iter().any(|r| r.message == "a ledger node restarts"));
        assert_eq!(d.s.node_states.lock().unwrap().latest["ledger-0"].boot, 2);
    }
}
