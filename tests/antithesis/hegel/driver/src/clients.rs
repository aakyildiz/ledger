//! Talking to the client containers (DESIGN §4.5, §6.5, §6.6, §7): one HTTP
//! request per operation, outcome classification, and a UDP receiver for
//! completion events that arrive after the driver stopped waiting (late
//! outcomes). Late outcomes wake their waiters through a condvar; outcomes
//! of attempts of tickets that already ended are queued for the late poster
//! (`resolver.rs`), which reports definitive ones with `/write/late`.
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::net::UdpSocket;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

/// The definitive failure reasons (DESIGN §4.5), one per line.
pub const DEFINITIVE_REASONS: &str = include_str!("definitive_reasons.txt");

fn definitive_reasons() -> &'static HashSet<&'static str> {
    static SET: std::sync::OnceLock<HashSet<&'static str>> = std::sync::OnceLock::new();
    SET.get_or_init(|| DEFINITIVE_REASONS.lines().map(str::trim).filter(|l| !l.is_empty()).collect())
}

/// The outcome of one operation, as the client reports it (DESIGN §4.5),
/// plus `unknown` when the driver got no answer.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Outcome {
    pub ok: bool,
    pub response: Option<Value>,
    pub items: Option<Vec<Value>>,
    pub complete: bool,
    pub next_cursor: String,
    pub code: String,
    pub reason: String,
    pub message: String,
    pub metadata: BTreeMap<String, String>,
    pub local: bool,
    pub peer: String,
    pub elapsed_ms: Option<u64>,
    /// Non-empty when the driver got no answer: `timeout`, `transport`,
    /// `client-boot`, `http-<status>`.
    pub unknown: String,
}

/// How an attempt's outcome classifies (DESIGN §6.5 `AttemptRecord`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OutcomeClass {
    Ok,
    Definitive,
    NonDefinitive,
    Unknown,
}

impl Outcome {
    pub fn unknown(why: &str) -> Outcome {
        Outcome { unknown: why.to_string(), ..Default::default() }
    }

    pub fn from_json(v: &Value) -> Outcome {
        let s = |k: &str| v.get(k).and_then(Value::as_str).unwrap_or("").to_string();
        if v.get("unknown").is_some() && v.get("ok").is_none() {
            return Outcome::unknown(&s("unknown"));
        }
        Outcome {
            ok: v.get("ok").and_then(Value::as_bool).unwrap_or(false),
            response: v.get("response").cloned(),
            items: v.get("items").and_then(Value::as_array).cloned(),
            complete: v.get("complete").and_then(Value::as_bool).unwrap_or(false),
            next_cursor: s("nextCursor"),
            code: s("code"),
            reason: s("reason"),
            message: s("message"),
            metadata: v
                .get("metadata")
                .and_then(Value::as_object)
                .map(|m| m.iter().map(|(k, v)| (k.clone(), v.as_str().unwrap_or("").to_string())).collect())
                .unwrap_or_default(),
            local: v.get("local").and_then(Value::as_bool).unwrap_or(false),
            peer: s("peer"),
            elapsed_ms: v.get("elapsedMs").and_then(Value::as_u64),
            unknown: s("unknown"),
        }
    }

    /// The JSON shape the client writes (empty and false fields omitted).
    pub fn to_json(&self) -> Value {
        if self.is_unknown() {
            return json!({"unknown": self.unknown});
        }
        let mut m = Map::new();
        m.insert("ok".into(), json!(self.ok));
        if let Some(r) = &self.response {
            m.insert("response".into(), r.clone());
        }
        if let Some(i) = &self.items {
            m.insert("items".into(), json!(i));
        }
        if self.complete {
            m.insert("complete".into(), json!(true));
        }
        for (k, v) in [("nextCursor", &self.next_cursor), ("code", &self.code), ("reason", &self.reason), ("message", &self.message), ("peer", &self.peer)] {
            if !v.is_empty() {
                m.insert(k.into(), json!(v));
            }
        }
        if !self.metadata.is_empty() {
            m.insert("metadata".into(), json!(self.metadata));
        }
        if self.local {
            m.insert("local".into(), json!(true));
        }
        if let Some(e) = self.elapsed_ms {
            m.insert("elapsedMs".into(), json!(e));
        }
        Value::Object(m)
    }

    pub fn is_unknown(&self) -> bool {
        !self.unknown.is_empty()
    }

    pub fn is_ok(&self) -> bool {
        self.ok && !self.is_unknown()
    }

    /// A UDP completion event the client shortened to fit a datagram: a
    /// success without its response body. Not usable as a write outcome.
    pub fn truncated(&self) -> bool {
        self.is_ok() && self.response.is_none() && self.items.is_none() && self.message == "truncated"
    }

    pub fn class(&self) -> OutcomeClass {
        if self.is_unknown() {
            OutcomeClass::Unknown
        } else if self.ok {
            OutcomeClass::Ok
        } else if definitive(self) {
            OutcomeClass::Definitive
        } else {
            OutcomeClass::NonDefinitive
        }
    }

    pub fn summary(&self) -> Value {
        if self.is_unknown() {
            return json!({"unknown": self.unknown});
        }
        if self.ok {
            return json!({"ok": true, "items": self.items.as_ref().map(|i| i.len()), "complete": self.complete});
        }
        json!({"code": self.code, "reason": self.reason, "message": self.message, "local": self.local})
    }

    /// The highest log `sequence` of an Apply response (0 if none).
    pub fn max_log_sequence(&self) -> u64 {
        let Some(resp) = &self.response else { return 0 };
        resp.get("logs")
            .and_then(Value::as_array)
            .map(|logs| logs.iter().map(|l| crate::wire::u64_field(l, "sequence")).max().unwrap_or(0))
            .unwrap_or(0)
    }

    /// A client-local message-size error (DESIGN §4.5).
    pub fn local_message_size(&self) -> bool {
        self.local && self.code == "ResourceExhausted"
    }
}

impl Serialize for Outcome {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        self.to_json().serialize(s)
    }
}

impl<'de> Deserialize<'de> for Outcome {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let v = Value::deserialize(d)?;
        if !v.is_object() {
            return Err(serde::de::Error::custom("outcome must be an object"));
        }
        Ok(Outcome::from_json(&v))
    }
}

/// A write outcome is definitive iff it succeeded, or it is a non-local
/// failure whose reason is in the definitive list (DESIGN §4.5).
pub fn definitive(o: &Outcome) -> bool {
    if o.is_unknown() {
        return false;
    }
    if o.ok {
        return true;
    }
    !o.local && !o.reason.is_empty() && definitive_reasons().contains(o.reason.as_str())
}

/// How an RPC is routed through a client (DESIGN §6.5, §7 C1).
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum RouteMode {
    /// The shared production client (round-robin + the library's retry policy).
    Lib,
    /// Round-robin without retry policy (the driver is the only retry).
    Plain,
    /// Pinned to one node address ("ledger-1:8888", "restore-0:8888").
    Node(String),
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Route {
    pub client: usize,
    pub mode: RouteMode,
}

impl Route {
    pub fn lib(client: usize) -> Route {
        Route { client, mode: RouteMode::Lib }
    }
    pub fn plain(client: usize) -> Route {
        Route { client, mode: RouteMode::Plain }
    }
    pub fn node(client: usize, addr: &str) -> Route {
        Route { client, mode: RouteMode::Node(addr.to_string()) }
    }

    /// The `via.route` string of `/write/end` and `/write/late`.
    pub fn via(&self) -> String {
        match &self.mode {
            RouteMode::Lib => "lib".into(),
            RouteMode::Plain => "plain".into(),
            RouteMode::Node(n) => format!("node:{n}"),
        }
    }

    /// The `route` object of `/read/end`.
    pub fn read_json(&self, consistency: &str) -> Value {
        let (route, node) = match &self.mode {
            RouteMode::Lib => ("lib", ""),
            RouteMode::Plain => ("plain", ""),
            RouteMode::Node(n) => ("node", n.as_str()),
        };
        json!({"client": self.client, "route": route, "node": node, "consistency": consistency})
    }
}

/// Batch signing instructions for the client (DESIGN §7 C4, task 16).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SignSpec {
    pub key_id: String,
    /// base64 of the 32-byte ed25519 seed.
    pub seed: String,
}

/// Options of one call.
#[derive(Clone, Debug)]
pub struct CallOpts {
    pub timeout: Duration,
    pub max_items: Option<u64>,
    /// "linearizable" | "stale"; None = the server default (linearizable).
    pub consistency: Option<&'static str>,
    pub sign: Option<SignSpec>,
}

/// The default RPC deadline of a call.
pub const RPC_TIMEOUT: Duration = Duration::from_secs(5);

impl Default for CallOpts {
    fn default() -> Self {
        CallOpts { timeout: RPC_TIMEOUT, max_items: None, consistency: None, sign: None }
    }
}

#[allow(dead_code)] // family-facing API
impl CallOpts {
    pub fn linearizable() -> Self {
        CallOpts { consistency: Some("linearizable"), ..Default::default() }
    }
    pub fn stale() -> Self {
        CallOpts { consistency: Some("stale"), ..Default::default() }
    }
    pub fn timeout(mut self, d: Duration) -> Self {
        self.timeout = d;
        self
    }
    pub fn max_items(mut self, n: u64) -> Self {
        self.max_items = Some(n);
        self
    }
    pub fn sign(mut self, s: SignSpec) -> Self {
        self.sign = Some(s);
        self
    }
    pub fn consistency_str(&self) -> &'static str {
        self.consistency.unwrap_or("linearizable")
    }
}

/// Why an HTTP exchange with a client produced no outcome.
#[derive(Clone, Debug, PartialEq)]
pub enum TransportError {
    Timeout,
    Transport(String),
    Status(u16, String),
}

/// The HTTP side of the client containers (a fake in unit tests).
pub trait ClientTransport: Send + Sync {
    fn count(&self) -> usize;
    fn post(&self, client: usize, path: &str, body: &Value, timeout: Duration) -> Result<Value, TransportError>;
    fn get(&self, client: usize, path: &str, timeout: Duration) -> Result<Value, TransportError>;
    /// Called once the transport's owner exists, so a fake can deliver
    /// scripted UDP events.
    fn attach(&self, _clients: std::sync::Weak<Clients>) {}
}

/// The real transport: one `ureq` agent, one base URL per client.
pub struct HttpClientTransport {
    pub urls: Vec<String>,
    http: ureq::Agent,
}

impl HttpClientTransport {
    pub fn new(urls: Vec<String>) -> Self {
        HttpClientTransport { urls, http: ureq::AgentBuilder::new().timeout_connect(Duration::from_millis(1000)).build() }
    }

    fn map(r: Result<ureq::Response, ureq::Error>) -> Result<Value, TransportError> {
        match r {
            Ok(resp) => resp.into_json::<Value>().map_err(|e| TransportError::Transport(format!("bad response: {e}"))),
            Err(ureq::Error::Status(code, resp)) => Err(TransportError::Status(code, resp.into_string().unwrap_or_default())),
            Err(ureq::Error::Transport(t)) => {
                let s = t.to_string();
                if s.contains("timed out") || s.contains("Timeout") || s.contains("timeout") {
                    Err(TransportError::Timeout)
                } else {
                    Err(TransportError::Transport(s))
                }
            }
        }
    }
}

impl ClientTransport for HttpClientTransport {
    fn count(&self) -> usize {
        self.urls.len()
    }
    fn post(&self, client: usize, path: &str, body: &Value, timeout: Duration) -> Result<Value, TransportError> {
        Self::map(self.http.post(&format!("{}{}", self.urls[client], path)).timeout(timeout).send_json(body.clone()))
    }
    fn get(&self, client: usize, path: &str, timeout: Duration) -> Result<Value, TransportError> {
        Self::map(self.http.get(&format!("{}{}", self.urls[client], path)).timeout(timeout).call())
    }
}

/// A definitive outcome of an attempt of an ended ticket, for `/write/late`.
#[derive(Clone, Debug)]
pub struct LateEvent {
    pub ticket: u64,
    pub op: u64,
    pub client: usize,
    pub route: String,
    pub outcome: Outcome,
}

/// How long ended tickets' op ids stay mapped (DESIGN §4.9).
pub const ENDED_OPS_TTL: Duration = Duration::from_secs(600);

#[derive(Default)]
struct LateState {
    by_op: HashMap<u64, Outcome>,
    order: VecDeque<u64>,
}

pub struct Clients {
    pub transport: Arc<dyn ClientTransport>,
    boots: Vec<Mutex<String>>,
    next_op: AtomicU64,
    late: Mutex<LateState>,
    late_cv: Condvar,
    /// Ops whose late outcome an owner may still consult (live attempts).
    watched: Mutex<HashSet<u64>>,
    /// Op id → (ticket, client, via route, ended at), for attempts of ended tickets.
    ended_ops: Mutex<HashMap<u64, (u64, usize, String, Instant)>>,
    ended_pruned: Mutex<Instant>,
    /// Definitive late outcomes of ended tickets, for the poster.
    pub(crate) late_queue: Mutex<VecDeque<LateEvent>>,
    pub(crate) late_queue_cv: Condvar,
    /// The highest log sequence of every successful Apply seen (shared with `Shared`).
    pub acked_seq_high_water: Arc<AtomicU64>,
    active: Arc<AtomicBool>,
}

const LATE_CAP: usize = 200_000;

impl Clients {
    pub fn new(transport: Arc<dyn ClientTransport>, active: Arc<AtomicBool>, acked: Arc<AtomicU64>) -> Arc<Clients> {
        let boots = (0..transport.count()).map(|_| Mutex::new(String::new())).collect();
        let c = Arc::new(Clients {
            transport,
            boots,
            next_op: AtomicU64::new(1),
            late: Mutex::new(LateState::default()),
            late_cv: Condvar::new(),
            watched: Mutex::new(HashSet::new()),
            ended_ops: Mutex::new(HashMap::new()),
            ended_pruned: Mutex::new(Instant::now()),
            late_queue: Mutex::new(VecDeque::new()),
            late_queue_cv: Condvar::new(),
            acked_seq_high_water: acked,
            active,
        });
        c.transport.attach(Arc::downgrade(&c));
        c
    }

    pub fn len(&self) -> usize {
        self.transport.count()
    }

    pub fn next_op_id(&self) -> u64 {
        self.next_op.fetch_add(1, Ordering::Relaxed)
    }

    /// Starts the completion-event receiver on a UDP port (7778).
    pub fn start_receiver(self: &Arc<Self>, port: u16) {
        let me = Arc::downgrade(self);
        let sock = UdpSocket::bind(("0.0.0.0", port)).expect("bind outcome port");
        crate::assertions::spawn_inheriting("outcome-receiver", move || {
            let mut buf = vec![0u8; 65536];
            loop {
                let Ok(n) = sock.recv(&mut buf) else { continue };
                let Some(me) = me.upgrade() else { return };
                let Ok(v) = serde_json::from_slice::<Value>(&buf[..n]) else { continue };
                let Some(op) = v.get("opId").and_then(Value::as_u64) else { continue };
                let out = Outcome::from_json(v.get("outcome").unwrap_or(&Value::Null));
                me.deliver(op, out);
            }
        });
    }

    /// Records a completion event (UDP, or a fake's scripted event), wakes
    /// waiters, and queues definitive outcomes of ended tickets for the poster.
    pub fn deliver(&self, op: u64, out: Outcome) {
        self.note_apply(&out);
        let watched = self.watched.lock().unwrap_or_else(|e| e.into_inner()).contains(&op);
        if watched {
            let mut late = self.late.lock().unwrap_or_else(|e| e.into_inner());
            if late.by_op.insert(op, out.clone()).is_none() {
                late.order.push_back(op);
            }
            while late.order.len() > LATE_CAP {
                if let Some(old) = late.order.pop_front() {
                    late.by_op.remove(&old);
                }
            }
        }
        self.late_cv.notify_all();
        let ended = {
            let e = self.ended_ops.lock().unwrap_or_else(|e| e.into_inner());
            e.get(&op).filter(|(_, _, _, at)| at.elapsed() < ENDED_OPS_TTL).cloned()
        };
        if let Some((ticket, client, route, _)) = ended {
            // Non-definitive late outcomes are ignored; a truncated success
            // carries no logs to compare.
            if definitive(&out) && !out.truncated() {
                self.late_queue.lock().unwrap_or_else(|e| e.into_inner()).push_back(LateEvent { ticket, op, client, route, outcome: out });
                self.late_queue_cv.notify_all();
            }
        }
    }

    /// A completion event received for an operation, if any.
    #[allow(dead_code)] // family-facing API
    pub fn late_outcome(&self, op: u64) -> Option<Outcome> {
        self.late.lock().unwrap_or_else(|e| e.into_inner()).by_op.get(&op).cloned()
    }

    /// Waits until `accept` returns true for a late outcome of one of `ops`,
    /// or the deadline. Returns that op and its outcome.
    pub fn wait_late(&self, ops: &[u64], deadline: Instant, accept: impl Fn(&Outcome) -> bool) -> Option<(u64, Outcome)> {
        let mut late = self.late.lock().unwrap_or_else(|e| e.into_inner());
        loop {
            for op in ops {
                if let Some(o) = late.by_op.get(op) {
                    if accept(o) {
                        return Some((*op, o.clone()));
                    }
                }
            }
            let now = Instant::now();
            if now >= deadline || ops.is_empty() {
                return None;
            }
            late = self.late_cv.wait_timeout(late, deadline - now).unwrap_or_else(|e| e.into_inner()).0;
        }
    }

    /// Keeps the late outcome of `op` for its owner (call before sending).
    /// Events of unwatched, un-ended ops are dropped, which bounds memory.
    pub fn watch(&self, op: u64) {
        self.watched.lock().unwrap_or_else(|e| e.into_inner()).insert(op);
    }

    /// Forgets `ops` (their owner is done with them).
    pub fn unwatch(&self, ops: &[u64]) {
        let mut w = self.watched.lock().unwrap_or_else(|e| e.into_inner());
        for op in ops {
            w.remove(op);
        }
        drop(w);
        let mut late = self.late.lock().unwrap_or_else(|e| e.into_inner());
        for op in ops {
            late.by_op.remove(op);
        }
        let LateState { by_op, order } = &mut *late;
        order.retain(|o| by_op.contains_key(o));
    }

    /// Records that the attempts `ops` belong to a ticket that just ended.
    pub fn mark_ended(&self, ticket: u64, ops: &[(u64, usize, String)]) {
        let now = Instant::now();
        let mut e = self.ended_ops.lock().unwrap_or_else(|e| e.into_inner());
        {
            let mut last = self.ended_pruned.lock().unwrap_or_else(|e| e.into_inner());
            if last.elapsed() > Duration::from_secs(10) {
                e.retain(|_, (_, _, _, at)| at.elapsed() < ENDED_OPS_TTL);
                *last = now;
            }
        }
        for (op, client, route) in ops {
            e.insert(*op, (ticket, *client, route.clone(), now));
        }
    }

    /// GET /health on one client; returns its boot id.
    pub fn health(&self, client: usize) -> Result<String, String> {
        let v = self.transport.get(client, "/health", Duration::from_millis(1000)).map_err(|e| format!("{e:?}"))?;
        let boot = v.get("boot").and_then(Value::as_str).unwrap_or("").to_string();
        self.observe_boot(client, &boot);
        Ok(boot)
    }

    pub fn observe_boot(&self, client: usize, boot: &str) {
        if boot.is_empty() || client >= self.boots.len() {
            return;
        }
        let mut b = self.boots[client].lock().unwrap_or_else(|e| e.into_inner());
        if *b != boot {
            if !b.is_empty() && self.active.load(Ordering::Relaxed) {
                check_reachable!("the driver notices a client restart", json!({"client": client}));
            }
            *b = boot.to_string();
        }
    }

    fn note_apply(&self, out: &Outcome) {
        if out.is_ok() {
            let seq = out.max_log_sequence();
            if seq > 0 {
                self.acked_seq_high_water.fetch_max(seq, Ordering::SeqCst);
            }
        }
    }

    /// Runs one RPC through a client. Returns the op id and the outcome.
    pub fn call(&self, route: &Route, service: &str, method: &str, request: Value, opts: &CallOpts) -> (u64, Outcome) {
        let op = self.next_op_id();
        let out = self.call_with_op(op, route, service, method, request, opts);
        (op, out)
    }

    /// The op request body (DESIGN §7).
    pub fn op_body(op: u64, route: &Route, service: &str, method: &str, request: Value, opts: &CallOpts) -> Value {
        let mut body = json!({
            "opId": op,
            "service": service,
            "method": method,
            "request": request,
            "timeoutMs": opts.timeout.as_millis() as u64,
            "maxItems": opts.max_items.unwrap_or(1000),
        });
        match &route.mode {
            RouteMode::Lib => body["route"] = json!("lib"),
            RouteMode::Plain => body["route"] = json!("plain"),
            RouteMode::Node(n) => body["node"] = json!(n),
        }
        if let Some(c) = opts.consistency {
            body["consistency"] = json!(c);
        }
        if let Some(s) = &opts.sign {
            body["sign"] = json!(s);
        }
        body
    }

    pub fn call_with_op(&self, op: u64, route: &Route, service: &str, method: &str, request: Value, opts: &CallOpts) -> Outcome {
        if route.client >= self.len() {
            return Outcome::unknown("transport");
        }
        let body = Self::op_body(op, route, service, method, request, opts);
        let resp = self.transport.post(route.client, "/op", &body, opts.timeout + Duration::from_millis(1500));
        let out = match resp {
            Ok(v) => {
                self.observe_boot(route.client, v.get("boot").and_then(Value::as_str).unwrap_or(""));
                match v.get("outcome") {
                    Some(o) if o.is_object() => Outcome::from_json(o),
                    _ => Outcome::unknown("transport"),
                }
            }
            Err(TransportError::Status(code, text)) => {
                // The client answers non-200 only when it could not parse
                // the request the driver built.
                check_unreachable!(
                    "the client program accepts every request the driver builds",
                    json!({"method": method, "status": code, "body": text})
                );
                Outcome::unknown(&format!("http-{code}"))
            }
            Err(TransportError::Timeout) => Outcome::unknown("timeout"),
            Err(TransportError::Transport(_)) => Outcome::unknown("transport"),
        };
        if service == "bucket" && method == "Apply" {
            self.note_apply(&out);
            crate::write::explore_attempt(&out);
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn o(v: Value) -> Outcome {
        Outcome::from_json(&v)
    }

    #[test]
    fn definitive_table() {
        // ok
        assert!(definitive(&o(json!({"ok": true, "response": {}}))));
        assert!(definitive(&o(json!({"ok": true, "items": [], "complete": true}))));
        // every reason of the list, non-local
        for r in DEFINITIVE_REASONS.lines() {
            assert!(definitive(&o(json!({"ok": false, "code": "FailedPrecondition", "reason": r}))), "{r}");
            // local errors are never definitive
            assert!(!definitive(&o(json!({"ok": false, "code": "FailedPrecondition", "reason": r, "local": true}))), "{r}");
        }
        // the override
        assert!(!DEFINITIVE_REASONS.lines().any(|l| l == "AUDIT_DISABLED"));
        assert!(!definitive(&o(json!({"ok": false, "code": "FailedPrecondition", "reason": "AUDIT_DISABLED"}))));
        // non-definitive reasons (Unavailable/ResourceExhausted/Internal kinds)
        for r in ["MAINTENANCE_MODE", "STALE_PROPOSAL", "PRELOAD_UNAVAILABLE", "BALANCE_NOT_PRELOADED", "INDEX_BUILDING", "WRITES_BLOCKED_CLOCK_SKEW", "WRITES_BLOCKED_DISK_FULL"] {
            assert!(!definitive(&o(json!({"ok": false, "code": "Unavailable", "reason": r}))), "{r}");
        }
        // empty reason, any code
        for c in ["Unavailable", "DeadlineExceeded", "Canceled", "Aborted", "Internal", "Unknown", "ResourceExhausted", "Unauthenticated", "PermissionDenied", "InvalidArgument", "NotFound"] {
            assert!(!definitive(&o(json!({"ok": false, "code": c}))), "{c}");
        }
        // no answer
        for u in ["timeout", "transport", "client-boot", "http-502"] {
            assert!(!definitive(&Outcome::unknown(u)));
        }
        // the local message-size error
        let local = o(json!({"ok": false, "code": "ResourceExhausted", "message": "too large", "local": true}));
        assert!(!definitive(&local));
        assert!(local.local_message_size());
    }

    #[test]
    fn outcome_json_round_trips_and_truncation() {
        for v in [
            json!({"unknown": "timeout"}),
            json!({"ok": true, "response": {"logs": []}, "peer": "ledger-0:8888", "elapsedMs": 3}),
            json!({"ok": false, "code": "FailedPrecondition", "reason": "INSUFFICIENT_FUNDS", "metadata": {"a": "b"}}),
            json!({"ok": true, "items": [{"id": "1"}], "complete": true, "nextCursor": "x"}),
            json!({"ok": false, "code": "ResourceExhausted", "local": true}),
        ] {
            assert_eq!(o(v.clone()).to_json(), v);
        }
        assert!(o(json!({"ok": true, "message": "truncated"})).truncated());
        assert!(!o(json!({"ok": true, "response": {}})).truncated());
        assert_eq!(o(json!({"ok": true, "response": {"logs": [{"sequence": "7"}, {"sequence": "9"}]}})).max_log_sequence(), 9);
    }

    #[test]
    fn routes_render() {
        assert_eq!(Route::lib(1).via(), "lib");
        assert_eq!(Route::plain(1).via(), "plain");
        assert_eq!(Route::node(0, "ledger-2:8888").via(), "node:ledger-2:8888");
        assert_eq!(Route::plain(2).read_json("linearizable"), json!({"client": 2, "route": "plain", "node": "", "consistency": "linearizable"}));
        let b = Clients::op_body(5, &Route::node(0, "ledger-1:8888"), "bucket", "GetAccount", json!({}), &CallOpts::stale());
        assert_eq!(b["node"], "ledger-1:8888");
        assert_eq!(b["consistency"], "stale");
        assert!(b.get("route").is_none());
        let s = SignSpec { key_id: "k".into(), seed: "AA==".into() };
        let b = Clients::op_body(6, &Route::plain(1), "bucket", "Apply", json!({}), &CallOpts::default().sign(s));
        assert_eq!(b["route"], "plain");
        assert_eq!(b["sign"], json!({"keyId": "k", "seed": "AA=="}));
    }
}
