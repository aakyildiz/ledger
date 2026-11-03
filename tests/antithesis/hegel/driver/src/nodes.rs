//! Node ids and per-node horizons (DESIGN §6.10). The old convergence check
//! read the leader's index for every node; here each node is asked about
//! itself (`GetClusterState{nodeId}` pinned to that node) and its answer is
//! accepted only if it says it is that node (`localNode == id`).
#![allow(dead_code)] // family-facing API

use crate::clients::{CallOpts, Route};
use crate::machine::Driver;
use crate::wire::{str_field, u64_field};
use serde_json::{json, Value};
use std::time::{Duration, Instant};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NodeRef {
    pub id: u64,
    /// The node's service address ("ledger-0:8888").
    pub addr: String,
}

/// Every cluster member, from `GetClusterState{}` `nodes[].{id, serviceAddress}`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct NodeMap {
    pub nodes: Vec<NodeRef>,
}

impl NodeMap {
    /// Parses a `ClusterState` response.
    pub fn from_cluster_state(cs: &Value) -> NodeMap {
        let mut nodes: Vec<NodeRef> = cs
            .get("nodes")
            .and_then(Value::as_array)
            .map(|ns| ns.iter().map(|n| NodeRef { id: u64_field(n, "id"), addr: str_field(n, "serviceAddress").to_string() }).filter(|n| n.id != 0).collect())
            .unwrap_or_default();
        nodes.sort_by_key(|n| n.id);
        NodeMap { nodes }
    }

    /// Asks the cluster (lib route on client 0, then the others) for its members.
    pub fn discover(d: &Driver) -> Option<NodeMap> {
        for c in 0..d.s.clients.len() {
            let (_, out) = d.call(&Route::lib(c), "cluster", "GetClusterState", json!({}), &CallOpts::default().timeout(Duration::from_secs(3)));
            if let Some(resp) = out.response.as_ref().filter(|_| out.is_ok()) {
                let m = NodeMap::from_cluster_state(resp);
                if !m.nodes.is_empty() {
                    return Some(m);
                }
            }
        }
        None
    }

    pub fn by_addr(&self, addr: &str) -> Option<&NodeRef> {
        self.nodes.iter().find(|n| n.addr == addr)
    }

    pub fn by_id(&self, id: u64) -> Option<&NodeRef> {
        self.nodes.iter().find(|n| n.id == id)
    }
}

/// What one node's own cluster state says about a horizon.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Horizon {
    /// The node is the one asked, in normal sync, and at (or past) the horizon.
    Reached(u64),
    /// Not yet (or the answer came from another node, or it is syncing).
    NotYet,
    /// `exact` and the node is already past the horizon: it can never equal it.
    Passed(u64),
}

/// Judges one `GetClusterState{nodeId}` answer (DESIGN §6.10).
pub fn horizon_check(cs: &Value, node_id: u64, horizon: u64, exact: bool) -> Horizon {
    if u64_field(cs, "localNode") != node_id {
        // A leader-routed (or misrouted) answer describes another node.
        return Horizon::NotYet;
    }
    let status = cs.pointer("/syncProgress/status").and_then(Value::as_str).unwrap_or("");
    if !(status.is_empty() || status == "normal") {
        return Horizon::NotYet;
    }
    let persisted = cs.get("raftStatus").map(|r| u64_field(r, "lastPersistedIndex")).unwrap_or(0);
    match (exact, persisted.cmp(&horizon)) {
        (_, std::cmp::Ordering::Less) => Horizon::NotYet,
        (true, std::cmp::Ordering::Greater) => Horizon::Passed(persisted),
        _ => Horizon::Reached(persisted),
    }
}

impl Driver {
    /// `GetClusterState{"nodeId": id}` pinned to that node.
    pub fn cluster_state(&self, node_addr: &str, node_id: u64) -> Option<Value> {
        self.cluster_state_via(0, node_addr, node_id)
    }

    pub fn cluster_state_via(&self, client: usize, node_addr: &str, node_id: u64) -> Option<Value> {
        let (_, out) = self.call(&Route::node(client, node_addr), "cluster", "GetClusterState", json!({"nodeId": node_id.to_string()}), &CallOpts::default().timeout(Duration::from_secs(2)));
        if out.is_ok() {
            out.response
        } else {
            None
        }
    }

    /// Polls the node until its own state reaches `horizon` (`exact`: equals
    /// it). Returns its `lastPersistedIndex`, or `None` at the deadline or
    /// once an exact horizon was passed.
    pub fn wait_at_horizon(&self, node: &NodeRef, horizon: u64, exact: bool, deadline: Instant) -> Option<u64> {
        loop {
            if let Some(cs) = self.cluster_state(&node.addr, node.id) {
                match horizon_check(&cs, node.id, horizon, exact) {
                    Horizon::Reached(p) => return Some(p),
                    Horizon::Passed(_) => return None,
                    Horizon::NotYet => {}
                }
            }
            if Instant::now() + Duration::from_millis(200) >= deadline {
                return None;
            }
            std::thread::sleep(Duration::from_millis(200));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clients::Outcome;
    use crate::testkit::{self, FakeReply};

    fn cs(local: u64, status: &str, persisted: u64) -> Value {
        json!({"localNode": local.to_string(), "leader": "1", "syncProgress": {"status": status}, "raftStatus": {"lastPersistedIndex": persisted.to_string()}})
    }

    #[test]
    fn horizon_logic() {
        assert_eq!(horizon_check(&cs(2, "normal", 10), 2, 10, false), Horizon::Reached(10));
        assert_eq!(horizon_check(&cs(2, "", 12), 2, 10, false), Horizon::Reached(12));
        assert_eq!(horizon_check(&cs(2, "normal", 9), 2, 10, false), Horizon::NotYet);
        assert_eq!(horizon_check(&cs(2, "normal", 12), 2, 10, true), Horizon::Passed(12));
        assert_eq!(horizon_check(&cs(2, "normal", 10), 2, 10, true), Horizon::Reached(10));
        for s in ["syncing", "snapshotting", "out_of_sync"] {
            assert_eq!(horizon_check(&cs(2, s, 50), 2, 10, false), Horizon::NotYet, "{s}");
        }
        // A leader-routed answer (localNode = the leader) is not node 2's.
        assert_eq!(horizon_check(&cs(1, "normal", 50), 2, 10, false), Horizon::NotYet);
        assert_eq!(horizon_check(&json!({}), 2, 0, false), Horizon::NotYet);
    }

    #[test]
    fn node_map_and_wait_at_horizon() {
        let m = NodeMap::from_cluster_state(&json!({"nodes": [{"id": "2", "serviceAddress": "ledger-1:8888"}, {"id": "1", "serviceAddress": "ledger-0:8888"}]}));
        assert_eq!(m.nodes, vec![NodeRef { id: 1, addr: "ledger-0:8888".into() }, NodeRef { id: 2, addr: "ledger-1:8888".into() }]);
        assert_eq!(m.by_addr("ledger-1:8888").unwrap().id, 2);

        let (d, ft, _) = testkit::driver();
        let ok = |v: Value| FakeReply::Outcome(Outcome::from_json(&json!({"ok": true, "response": v})));
        ft.script("cluster", "GetClusterState", vec![ok(cs(1, "normal", 99)), ok(cs(2, "syncing", 99)), ok(cs(2, "normal", 20))]);
        let n = NodeRef { id: 2, addr: "ledger-1:8888".into() };
        assert_eq!(d.wait_at_horizon(&n, 20, true, Instant::now() + Duration::from_secs(5)), Some(20));
        let calls = ft.calls_of("cluster", "GetClusterState");
        assert_eq!(calls.len(), 3);
        assert_eq!(calls[0]["node"], "ledger-1:8888");
        assert_eq!(calls[0]["request"], json!({"nodeId": "2"}));
    }
}
