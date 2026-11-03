//! Receives forwarded log lines (UDP 7777) from the ledger nodes and client
//! containers, and tracks what each node believes about leadership.
use antithesis_sdk::prelude::*;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::net::UdpSocket;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

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

    fn handle(&mut self, node: &str, boot: u64, seq: u64, line: &str) {
        let active = self.active.load(Ordering::Relaxed);
        let is_ledger = node.starts_with("ledger-");

        // Guarantees: checked from boot. The binaries are built with -race.
        let race = line.contains("WARNING: DATA RACE");
        assert_always!(!race, "no data race is reported by any process", &json!({"node": node, "line": line}));
        let panic = line.starts_with("panic:") || line.starts_with("fatal error:");
        assert_always!(!panic, "no process panics", &json!({"node": node, "line": line}));

        if !is_ledger {
            return;
        }
        let entry = self.latest.entry(node.to_string()).or_default();
        if entry.boot != boot {
            if entry.boot != 0 && active {
                assert_reachable!("a ledger node restarts", &json!({"node": node}));
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
            assert_reachable!("the driver reads leadership changes from node logs", &json!({}));
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
        assert_sometimes!(gained, "a node gains leadership while the workload runs", &json!({"node": node}));
        assert_sometimes!(lost, "a node loses leadership while the workload runs", &json!({"node": node}));
        // Two nodes believe they lead (in different terms): the old leader has
        // not heard of the new one yet.
        assert_sometimes!(leaders.len() >= 2, "two nodes believe they are leader at once", &json!({
            "leaders": leaders.iter().map(|(n, s)| json!({"node": n, "term": s.term})).collect::<Vec<_>>()
        }));
        let max_transitions = nodes.iter().map(|(_, n)| n.transitions).max().unwrap_or(0);
        assert_sometimes_greater_than!(max_transitions, 4, "a node changes leadership role many times in one boot", &json!({}));
    }

    pub fn leader_hint(&self) -> Option<String> {
        self.latest.iter().filter(|(_, s)| s.leader).max_by_key(|(_, s)| s.term).map(|(n, _)| n.clone())
    }
}

pub fn start(states: Arc<Mutex<NodeStates>>) {
    let sock = UdpSocket::bind("0.0.0.0:7777").expect("bind 7777");
    std::thread::spawn(move || {
        let mut buf = vec![0u8; 65536];
        let mut seen_kinds: HashMap<&'static str, bool> = HashMap::new();
        loop {
            let Ok(n) = sock.recv(&mut buf) else { continue };
            let Ok(v) = serde_json::from_slice::<Value>(&buf[..n]) else { continue };
            let node = v.get("node").and_then(Value::as_str).unwrap_or("");
            let boot = v.get("boot").and_then(Value::as_u64).unwrap_or(0);
            let seq = v.get("seq").and_then(Value::as_u64).unwrap_or(0);
            let line = v.get("line").and_then(Value::as_str).unwrap_or("");
            let kind = if node.starts_with("ledger-") { "ledger" } else if node.starts_with("client") { "client" } else { "other" };
            if seen_kinds.insert(kind, true).is_none() {
                match kind {
                    "ledger" => assert_reachable!("the driver receives ledger node logs", &json!({})),
                    "client" => assert_reachable!("the driver receives client logs", &json!({})),
                    _ => {}
                }
            }
            states.lock().unwrap().handle(node, boot, seq, line);
        }
    });
}
