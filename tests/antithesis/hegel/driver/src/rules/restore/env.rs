//! The restore targets as the driver sees them: the restore-agents' HTTP API
//! and the clients' `/digest` endpoint, behind a trait so unit tests can run
//! the cycle against fakes.
use crate::machine::Driver;
use crate::wire::{as_u64, u64_field};
use serde_json::{json, Value};
use std::time::Duration;

/// Deadlines of the cycle (real values; unit tests shorten them).
#[derive(Clone, Debug)]
pub struct RTiming {
    /// Wall budget of one cycle from the cut on (restore.md §4.3).
    pub cycle_budget: Duration,
    /// How long a base backup may wait for a quiet period to take the cut.
    pub cut_wait: Duration,
    /// Restore-mode or normal boot until the server answers.
    pub boot_wait: Duration,
    /// Normal boot until a Barrier succeeds; joiner catch-up.
    pub serve_wait: Duration,
    /// Index READY wait on the restored cluster.
    pub index_wait: Duration,
    /// ValidateRestore stream deadline.
    pub validate_timeout: Duration,
    /// Busy-destination budget from the first busy reply (W/internal/backup.go:21).
    pub busy_budget: Duration,
    /// Deadline of one Backup/IncrementalBackup RPC.
    pub backup_rpc: Duration,
    /// Deadline of the base job.
    pub base_deadline: Duration,
    /// Upper bound of every planned poll/backoff sleep.
    pub poll_cap: Duration,
    /// How long an agent reachability answer is reused.
    pub reach_cache: Duration,
    /// Deadline of one digest request.
    pub digest_timeout: Duration,
}

impl Default for RTiming {
    fn default() -> Self {
        RTiming {
            cycle_budget: Duration::from_secs(600),
            cut_wait: Duration::from_secs(900),
            boot_wait: Duration::from_secs(60),
            serve_wait: Duration::from_secs(120),
            index_wait: Duration::from_secs(120),
            validate_timeout: Duration::from_secs(300),
            busy_budget: Duration::from_secs(120),
            backup_rpc: Duration::from_secs(120),
            base_deadline: Duration::from_secs(300),
            poll_cap: Duration::from_secs(5),
            reach_cache: Duration::from_secs(30),
            digest_timeout: Duration::from_secs(50),
        }
    }
}

/// One agent HTTP exchange.
#[derive(Clone, Debug, PartialEq)]
pub enum AgentReply {
    Ok(Value),
    /// 409: stale gen, marker present, busy (body carries `status`).
    Conflict(Value),
    /// Another HTTP status or an unparsable answer.
    Error(String),
    /// No answer (connection refused, DNS, timeout).
    Unreachable,
}

pub trait RestoreEnv: Send + Sync {
    fn agent_count(&self) -> usize;
    fn agent_get(&self, agent: usize, path: &str, timeout: Duration) -> AgentReply;
    fn agent_post(&self, agent: usize, path: &str, body: &Value, timeout: Duration) -> AgentReply;
    /// `POST /digest` on a client (DESIGN §7 C6); the raw response.
    fn digest(&self, d: &Driver, client: usize, body: &Value, timeout: Duration) -> Result<Value, String>;
    fn joiner_enabled(&self) -> bool;
    fn timing(&self) -> RTiming;
}

/// The real environment: `RESTORE_AGENTS` (default restore-0, restore-1),
/// `RESTORE_JOINER=1` enables the joiner.
pub struct HttpEnv {
    agents: Vec<String>,
    joiner: bool,
    http: ureq::Agent,
}

impl HttpEnv {
    pub fn from_env() -> HttpEnv {
        let agents = std::env::var("RESTORE_AGENTS")
            .ok()
            .filter(|s| !s.trim().is_empty())
            .unwrap_or_else(|| "http://restore-0:8090,http://restore-1:8090".to_string())
            .split(',')
            .map(|s| s.trim().trim_end_matches('/').to_string())
            .filter(|s| !s.is_empty())
            .collect();
        let joiner = std::env::var("RESTORE_JOINER").map(|v| v.trim() == "1").unwrap_or(false);
        HttpEnv { agents, joiner, http: ureq::AgentBuilder::new().timeout_connect(Duration::from_millis(1000)).build() }
    }

    fn map(r: Result<ureq::Response, ureq::Error>) -> AgentReply {
        match r {
            Ok(resp) => resp.into_json::<Value>().map(AgentReply::Ok).unwrap_or_else(|e| AgentReply::Error(format!("bad response: {e}"))),
            Err(ureq::Error::Status(409, resp)) => AgentReply::Conflict(resp.into_json::<Value>().unwrap_or(Value::Null)),
            Err(ureq::Error::Status(code, resp)) => AgentReply::Error(format!("{code}: {}", resp.into_string().unwrap_or_default())),
            Err(ureq::Error::Transport(_)) => AgentReply::Unreachable,
        }
    }
}

impl RestoreEnv for HttpEnv {
    fn agent_count(&self) -> usize {
        self.agents.len()
    }
    fn agent_get(&self, agent: usize, path: &str, timeout: Duration) -> AgentReply {
        let Some(base) = self.agents.get(agent) else { return AgentReply::Unreachable };
        Self::map(self.http.get(&format!("{base}{path}")).timeout(timeout).call())
    }
    fn agent_post(&self, agent: usize, path: &str, body: &Value, timeout: Duration) -> AgentReply {
        let Some(base) = self.agents.get(agent) else { return AgentReply::Unreachable };
        Self::map(self.http.post(&format!("{base}{path}")).timeout(timeout).send_json(body.clone()))
    }
    fn digest(&self, d: &Driver, client: usize, body: &Value, timeout: Duration) -> Result<Value, String> {
        if client >= d.s.clients.len() {
            return Err("no such client".into());
        }
        d.s.clients.transport.post(client, "/digest", body, timeout).map_err(|e| format!("{e:?}"))
    }
    fn joiner_enabled(&self) -> bool {
        self.joiner && self.agents.len() > 1
    }
    fn timing(&self) -> RTiming {
        RTiming::default()
    }
}

/// One child exit an agent reports.
#[derive(Clone, Debug, PartialEq)]
pub struct ChildExit {
    pub mode: String,
    pub code: i64,
    pub signal: String,
    pub by_agent: bool,
}

/// `GET /status` of an agent (restore.md §4.1).
#[derive(Clone, Debug, Default, PartialEq)]
pub struct AgentStatus {
    pub gen: u64,
    pub mode: String,
    pub alive: bool,
    pub boot_count: u64,
    pub exits: Vec<ChildExit>,
    pub marker_present: bool,
    pub marker_index: u64,
    pub checkpoint0: bool,
    pub offline_running: bool,
    pub offline_failed: bool,
    pub last_offline: Option<Value>,
}

impl AgentStatus {
    pub fn parse(v: &Value) -> AgentStatus {
        let b = |k: &str| v.get(k).and_then(Value::as_bool).unwrap_or(false);
        let marker = v.get("marker").cloned().unwrap_or(json!({}));
        let offline = v.get("offline").cloned().unwrap_or(json!({}));
        AgentStatus {
            gen: u64_field(v, "gen"),
            mode: v.get("mode").and_then(Value::as_str).unwrap_or("").to_string(),
            alive: b("alive"),
            boot_count: u64_field(v, "bootCount"),
            exits: v
                .get("childExits")
                .and_then(Value::as_array)
                .map(|a| {
                    a.iter()
                        .map(|e| ChildExit {
                            mode: e.get("mode").and_then(Value::as_str).unwrap_or("").to_string(),
                            code: e.get("code").and_then(Value::as_i64).unwrap_or(0),
                            signal: e.get("signal").and_then(Value::as_str).unwrap_or("").to_string(),
                            by_agent: e.get("byAgent").and_then(Value::as_bool).unwrap_or(false),
                        })
                        .collect()
                })
                .unwrap_or_default(),
            marker_present: marker.get("present").and_then(Value::as_bool).unwrap_or(false),
            marker_index: marker.get("lastAppliedIndex").and_then(as_u64).unwrap_or(0),
            checkpoint0: b("checkpoint0"),
            offline_running: offline.get("running").and_then(Value::as_bool).unwrap_or(false),
            offline_failed: offline.get("failed").and_then(Value::as_bool).unwrap_or(false),
            last_offline: v.get("lastOffline").filter(|x| !x.is_null()).cloned(),
        }
    }

    /// The durable commit proof of a finalize (EN-3029): never trust the
    /// RPC's answer, only the marker plus `checkpoints/0`.
    pub fn committed(&self) -> bool {
        self.marker_present && self.marker_index > 0 && self.checkpoint0
    }
}
