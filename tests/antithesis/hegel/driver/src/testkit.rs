//! Test doubles for driver unit tests (00c and every family):
//! `FakeTransport` (scripted client replies, call log, unknown/late/UDP
//! events), `FakeOracle` (an in-process oracle protocol state machine),
//! `Shared::for_tests`, `run_rule`, and the assertion capture.
#![allow(dead_code)] // family-facing test API

use crate::assertions::Recorded;
use crate::clients::{definitive, ClientTransport, Clients, Outcome, TransportError};
use crate::machine::Driver;
use crate::oracle::{OracleRetry, OracleTransport};
use crate::shared::{Config, Shared, Timing};
use hegel::TestCase;
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant};

// ---------------------------------------------------------------- assertions

/// Every assertion recorded on this thread (and the helper threads it
/// started) since the last `reset()`.
pub fn assertions() -> Vec<Recorded> {
    crate::assertions::current_sink().map(|s| s.lock().unwrap_or_else(|e| e.into_inner()).clone()).unwrap_or_default()
}

pub fn reset() {
    if let Some(s) = crate::assertions::current_sink() {
        s.lock().unwrap_or_else(|e| e.into_inner()).clear();
    }
}

/// Some assertion with this message was evaluated.
pub fn fired(message: &str) -> bool {
    assertions().iter().any(|a| a.message == message)
}

/// A non-string panic payload, like hegel's control unwinds.
#[derive(Debug)]
pub struct StopTestLike;

pub fn wait_until(timeout: Duration, mut cond: impl FnMut() -> bool) {
    let deadline = Instant::now() + timeout;
    while !cond() {
        assert!(Instant::now() < deadline, "condition not reached within {timeout:?}");
        std::thread::sleep(Duration::from_millis(5));
    }
}

// ---------------------------------------------------------------- hegel

fn settings(n: u64) -> hegel::Settings {
    hegel::Settings::new().test_cases(n).database(hegel::Database::Disabled)
}

/// Executes one rule body under a hegel test.
pub fn run_rule(d: &Driver, rule_fn: fn(&Driver, TestCase)) {
    let d = d.clone();
    hegel::Hegel::new(move |tc: TestCase| rule_fn(&d, tc)).settings(settings(1)).run();
}

/// Runs `f` with `n` hegel test cases (for draw-domain tests).
pub fn with_tc_n(n: u64, f: impl Fn(&TestCase)) {
    hegel::Hegel::new(|tc: TestCase| f(&tc)).settings(settings(n)).run();
}

// ---------------------------------------------------------------- clients

/// One scripted reply of a client to an `/op`.
#[derive(Clone, Debug)]
pub enum FakeReply {
    /// Answer at once (an `unknown` outcome is a transport timeout).
    Outcome(Outcome),
    /// No HTTP answer (timeout); optionally a UDP completion event after a delay.
    Lost(Option<(Duration, Outcome)>),
    /// Answer after a delay (an `unknown` outcome is a timeout after it).
    Delayed(Duration, Outcome),
    /// A non-200 HTTP status from the client.
    Http(u16),
}

pub struct FakeTransport {
    n: usize,
    scripts: Mutex<HashMap<(String, String), VecDeque<FakeReply>>>,
    defaults: Mutex<HashMap<(String, String), FakeReply>>,
    calls: Mutex<Vec<(usize, String, Value)>>,
    clients: Mutex<Option<Weak<Clients>>>,
}

impl FakeTransport {
    pub fn new(n: usize) -> Arc<Self> {
        Arc::new(FakeTransport { n, scripts: Mutex::default(), defaults: Mutex::default(), calls: Mutex::default(), clients: Mutex::new(None) })
    }

    /// Appends replies for `(service, method)`, consumed in order.
    pub fn script(&self, service: &str, method: &str, replies: Vec<FakeReply>) {
        self.scripts.lock().unwrap().entry((service.into(), method.into())).or_default().extend(replies);
    }

    /// The reply once the script is exhausted (default: ok with `{}`).
    pub fn set_default(&self, service: &str, method: &str, reply: FakeReply) {
        self.defaults.lock().unwrap().insert((service.into(), method.into()), reply);
    }

    /// Every `/op` body sent, in order.
    pub fn calls(&self) -> Vec<Value> {
        self.calls.lock().unwrap().iter().filter(|(_, p, _)| p == "/op").map(|(_, _, b)| b.clone()).collect()
    }

    pub fn calls_of(&self, service: &str, method: &str) -> Vec<Value> {
        self.calls().into_iter().filter(|b| b["service"] == service && b["method"] == method).collect()
    }

    /// Delivers a UDP completion event now.
    pub fn udp(&self, op: u64, out: Outcome) {
        if let Some(c) = self.clients.lock().unwrap().as_ref().and_then(Weak::upgrade) {
            c.deliver(op, out);
        }
    }

    fn reply(&self, service: &str, method: &str) -> FakeReply {
        let key = (service.to_string(), method.to_string());
        if let Some(r) = self.scripts.lock().unwrap().get_mut(&key).and_then(VecDeque::pop_front) {
            return r;
        }
        self.defaults.lock().unwrap().get(&key).cloned().unwrap_or_else(|| FakeReply::Outcome(Outcome::from_json(&json!({"ok": true, "response": {}}))))
    }
}

fn op_answer(client: usize, op: u64, o: &Outcome) -> Result<Value, TransportError> {
    if o.is_unknown() {
        return Err(TransportError::Timeout);
    }
    Ok(json!({"boot": format!("fake-{client}"), "opId": op, "outcome": o.to_json()}))
}

impl ClientTransport for FakeTransport {
    fn count(&self) -> usize {
        self.n
    }

    fn post(&self, client: usize, path: &str, body: &Value, _timeout: Duration) -> Result<Value, TransportError> {
        self.calls.lock().unwrap().push((client, path.to_string(), body.clone()));
        if path != "/op" {
            return Ok(json!({}));
        }
        let op = body["opId"].as_u64().unwrap_or(0);
        let service = body["service"].as_str().unwrap_or("");
        let method = body["method"].as_str().unwrap_or("");
        match self.reply(service, method) {
            FakeReply::Outcome(o) => op_answer(client, op, &o),
            FakeReply::Lost(udp) => {
                if let Some((delay, out)) = udp {
                    let clients = self.clients.lock().unwrap().clone();
                    crate::assertions::spawn_inheriting("fake-udp", move || {
                        std::thread::sleep(delay);
                        if let Some(c) = clients.and_then(|w| w.upgrade()) {
                            c.deliver(op, out);
                        }
                    });
                }
                Err(TransportError::Timeout)
            }
            FakeReply::Delayed(d, o) => {
                std::thread::sleep(d);
                op_answer(client, op, &o)
            }
            FakeReply::Http(code) => Err(TransportError::Status(code, String::new())),
        }
    }

    fn get(&self, client: usize, path: &str, _timeout: Duration) -> Result<Value, TransportError> {
        self.calls.lock().unwrap().push((client, path.to_string(), Value::Null));
        Ok(json!({"boot": format!("fake-{client}")}))
    }

    fn attach(&self, clients: Weak<Clients>) {
        *self.clients.lock().unwrap() = Some(clients);
    }
}

// ---------------------------------------------------------------- oracle

#[derive(Default)]
struct FoState {
    boot: String,
    next_ticket: u64,
    inflight: BTreeMap<u64, String>,
    wids: HashMap<String, u64>,
    ended: BTreeSet<u64>,
    reads: BTreeSet<u64>,
    rids: HashMap<String, (u64, Vec<Value>)>,
    live: Vec<String>,
    quiet: Option<u64>,
    predicted: Value,
    verdict: Value,
    views: HashMap<String, Value>,
    status_override: Option<(u64, u64)>,
    leaked: Vec<u64>,
    begin_refusals: VecDeque<(u16, String)>,
    calls: Vec<(String, Value)>,
    run: Option<String>,
    paused_tickets: bool,
}

/// An in-process implementation of the oracle protocol state machine
/// (tickets, in-flight cap, backlog via scripted refusals, quiet, ticket
/// listing, configurable verdicts and views, `ledgerPick` over `live`).
pub struct FakeOracle {
    st: Mutex<FoState>,
}

pub const FAKE_MAX_INFLIGHT: usize = 8;

impl FakeOracle {
    pub fn new() -> Arc<Self> {
        Arc::new(FakeOracle {
            st: Mutex::new(FoState {
                boot: "1793836800123456789".into(),
                next_ticket: 1,
                live: vec!["o-test-l1".into(), "o-test-l2".into(), "o-test-l3".into(), "o-test-l4".into()],
                predicted: json!({"ok": true, "reason": ""}),
                verdict: json!({"ok": true, "skipped": false, "violations": []}),
                ..Default::default()
            }),
        })
    }

    /// Request bodies received for a path (`"/write/end"`, …), in order.
    pub fn calls(&self, path: &str) -> Vec<Value> {
        self.st.lock().unwrap().calls.iter().filter(|(p, _)| p == path).map(|(_, b)| b.clone()).collect()
    }
    pub fn set_live(&self, live: Vec<String>) {
        self.st.lock().unwrap().live = live;
    }
    pub fn set_predicted(&self, p: Value) {
        self.st.lock().unwrap().predicted = p;
    }
    pub fn set_verdict(&self, v: Value) {
        self.st.lock().unwrap().verdict = v;
    }
    /// Extra fields of a named view's answer.
    pub fn set_view(&self, name: &str, v: Value) {
        self.st.lock().unwrap().views.insert(name.into(), v);
    }
    /// `/quiet/status` reports these (inflight, reads) whatever the tickets.
    pub fn set_status_override(&self, o: Option<(u64, u64)>) {
        self.st.lock().unwrap().status_override = o;
    }
    /// An in-flight ticket no driver entry owns.
    pub fn leak_ticket(&self, t: u64) {
        self.st.lock().unwrap().leaked.push(t);
    }
    /// The next `/write/begin` answers with this status and code.
    pub fn refuse_next_begin(&self, status: u16, code: &str) {
        self.st.lock().unwrap().begin_refusals.push_back((status, code.into()));
    }
    /// A new boot id (the oracle "restarted").
    pub fn restart(&self) {
        let mut s = self.st.lock().unwrap();
        s.boot = "1793836899999999999".into();
        s.inflight.clear();
        s.reads.clear();
    }
    pub fn pause_tickets(&self) {
        self.st.lock().unwrap().paused_tickets = true;
    }
    pub fn inflight(&self) -> Vec<u64> {
        self.st.lock().unwrap().inflight.keys().copied().collect()
    }
    pub fn open_reads(&self) -> Vec<u64> {
        self.st.lock().unwrap().reads.iter().copied().collect()
    }
    pub fn is_quiet(&self) -> bool {
        self.st.lock().unwrap().quiet.is_some()
    }

    fn resolve_view(st: &FoState, v: &Value) -> Value {
        let name = v["name"].as_str().unwrap_or("");
        let mut out = match name {
            "ledgers" => json!({"trackedPrefix": "o-test-", "live": st.live, "deleted": [], "mirror": [], "maintenance": false, "nextLedger": st.live.len() + 1}),
            "replayable" => json!({"entries": []}),
            _ => json!({}),
        };
        let ledger = if let Some(p) = v.get("ledgerPick").and_then(Value::as_u64) {
            if st.live.is_empty() {
                return json!({"ledger": null});
            }
            Some(st.live[(p as usize) % st.live.len()].clone())
        } else {
            v.get("ledger").and_then(Value::as_str).map(str::to_string)
        };
        if let Some(l) = &ledger {
            out["ledger"] = json!(l);
            if name == "ledger" {
                out["name"] = json!(l);
                out["live"] = json!(st.live.contains(l));
            }
        }
        if let Some(extra) = st.views.get(name).and_then(Value::as_object) {
            for (k, x) in extra {
                out[k.as_str()] = x.clone();
            }
        }
        out
    }

    fn err(boot: &str, status: u16, code: &str, msg: &str) -> (u16, Value) {
        (status, json!({"bootId": boot, "error": {"code": code, "message": msg, "details": {}}}))
    }

    fn handle(&self, method: &str, path: &str, body: &Value) -> (u16, Value) {
        let mut st = self.st.lock().unwrap();
        st.calls.push((path.split('?').next().unwrap_or(path).to_string(), body.clone()));
        let boot = st.boot.clone();
        let ok = |mut v: Value| {
            v["bootId"] = json!(boot);
            (200u16, v)
        };
        let verdict = st.verdict.clone();
        match (method, path.split('?').next().unwrap_or(path)) {
            ("GET", "/health") => ok(json!({"ready": st.run.is_some()})),
            ("POST", "/setup") => {
                let run = body["runId"].as_str().unwrap_or("").to_string();
                if st.run.as_ref().is_some_and(|r| *r != run) {
                    return Self::err(&boot, 409, "setup", "setup already done with a different runId");
                }
                st.run = Some(run.clone());
                ok(json!({"runId": run, "trackedPrefix": body["trackedPrefix"], "version": 0}))
            }
            ("POST", "/view") => {
                let views: Vec<Value> = body["views"].as_array().cloned().unwrap_or_default().iter().map(|v| Self::resolve_view(&st, v)).collect();
                ok(json!({"version": 0, "views": views}))
            }
            ("POST", "/write/begin") => {
                if st.quiet.is_some() {
                    return Self::err(&boot, 400, "protocol", "write during a quiet period");
                }
                let wid = body["wid"].as_str().unwrap_or("").to_string();
                if let Some(t) = st.wids.get(&wid) {
                    let t = *t;
                    return ok(json!({"ticket": t, "version": 0, "predicted": st.predicted}));
                }
                if let Some((status, code)) = st.begin_refusals.pop_front() {
                    return Self::err(&boot, status, &code, "scripted refusal");
                }
                if st.inflight.len() >= FAKE_MAX_INFLIGHT {
                    return Self::err(&boot, 409, "busy", "8 tickets in flight");
                }
                let t = st.next_ticket;
                st.next_ticket += 1;
                st.inflight.insert(t, wid.clone());
                st.wids.insert(wid, t);
                ok(json!({"ticket": t, "version": 0, "predicted": st.predicted}))
            }
            ("POST", "/write/end") => {
                let t = body["ticket"].as_u64().unwrap_or(0);
                let out = Outcome::from_json(&body["outcome"]);
                if !definitive(&out) {
                    return Self::err(&boot, 400, "protocol", "the outcome is not definitive");
                }
                if st.inflight.remove(&t).is_some() || st.ended.contains(&t) {
                    st.ended.insert(t);
                    return ok(json!({"status": if out.is_ok() { "pending" } else { "validated" }, "verdict": verdict}));
                }
                Self::err(&boot, 404, "unknown_ticket", "ticket not known on this boot")
            }
            ("POST", "/write/late") => {
                let t = body["ticket"].as_u64().unwrap_or(0);
                if !st.ended.contains(&t) {
                    return Self::err(&boot, 404, "unknown_ticket", "ticket not ended on this boot");
                }
                ok(json!({"verdict": verdict}))
            }
            ("POST", "/write/abandon") => {
                let t = body["ticket"].as_u64().unwrap_or(0);
                if body["attempts"].as_u64().unwrap_or(0) > 0 {
                    return Self::err(&boot, 400, "protocol", "a ticket with attempts must be handed to a resolver");
                }
                if st.inflight.remove(&t).is_none() && !st.ended.contains(&t) {
                    return Self::err(&boot, 404, "unknown_ticket", "ticket not known on this boot");
                }
                st.ended.insert(t);
                ok(json!({"verdict": verdict}))
            }
            ("POST", "/read/begin") => {
                let rid = body["rid"].as_str().unwrap_or("").to_string();
                if let Some((t, views)) = st.rids.get(&rid).cloned() {
                    return ok(json!({"ticket": t, "version": 0, "views": views}));
                }
                let views: Vec<Value> = body["views"].as_array().cloned().unwrap_or_default().iter().map(|v| Self::resolve_view(&st, v)).collect();
                let t = st.next_ticket;
                st.next_ticket += 1;
                st.reads.insert(t);
                st.rids.insert(rid, (t, views.clone()));
                ok(json!({"ticket": t, "version": 0, "views": views}))
            }
            ("POST", "/read/end") => {
                let t = body["ticket"].as_u64().unwrap_or(0);
                if !st.reads.remove(&t) {
                    return Self::err(&boot, 404, "unknown_ticket", "read ticket not known");
                }
                ok(json!({"verdict": verdict, "coverage": []}))
            }
            ("POST", "/read/abandon") => {
                let t = body["ticket"].as_u64().unwrap_or(0);
                st.reads.remove(&t);
                ok(json!({"verdict": verdict}))
            }
            ("GET", "/tickets") => {
                let mut inflight: Vec<u64> = st.inflight.keys().copied().collect();
                inflight.extend(st.leaked.iter().copied());
                inflight.sort();
                ok(json!({"inflight": inflight, "reads": st.reads}))
            }
            ("GET", "/quiet/status") => {
                let (i, r) = st.status_override.unwrap_or((st.inflight.len() as u64 + st.leaked.len() as u64, st.reads.len() as u64));
                ok(json!({"inflight": i, "reads": r, "pending": 0, "quiet": st.quiet.is_some()}))
            }
            ("POST", "/quiet/begin") => {
                let busy = st.status_override.is_some_and(|(i, r)| i + r > 0) || !st.inflight.is_empty() || !st.reads.is_empty() || !st.leaked.is_empty();
                if busy {
                    return Self::err(&boot, 409, "not_quiet", "tickets are outstanding");
                }
                let epoch = body["epoch"].as_u64().unwrap_or(0);
                st.quiet = Some(epoch);
                ok(json!({"epoch": epoch, "version": 0, "auditCursor": "0", "ledgers": {"live": st.live, "deleted": []}, "expect": {}, "providers": {}}))
            }
            ("POST", "/quiet/section") => ok(json!({"verdict": verdict, "next": {}})),
            ("POST", "/quiet/end") => {
                st.quiet = None;
                ok(json!({"summary": {"sections": 0, "violations": 0}}))
            }
            ("GET", "/dump") => ok(json!({"version": 0, "fingerprint": "", "ledgers": [], "inflight": st.inflight.keys().collect::<Vec<_>>(), "pending": [], "reads": st.reads})),
            ("GET", "/journal") => ok(json!({"entries": [], "next": 0})),
            _ => Self::err(&boot, 400, "protocol", "unknown endpoint"),
        }
    }
}

/// Lets an `Arc<FakeOracle>` be the driver's oracle transport while the test keeps a handle.
pub struct SharedFakeOracle(pub Arc<FakeOracle>);

impl OracleTransport for SharedFakeOracle {
    fn send(&self, method: &str, path: &str, body: Option<&Value>, _timeout: Duration) -> Result<(u16, Value), String> {
        Ok(self.0.handle(method, path, body.unwrap_or(&Value::Null)))
    }
}

// ---------------------------------------------------------------- Shared

impl Shared {
    /// A `Shared` over the doubles: 3 clients, 3 nodes, run `test`, active,
    /// `QUIET_PROBABILITY = EXCLUSIVE_PROBABILITY = 1`, short deadlines, and
    /// the helper threads started (they inherit the caller's assertion sink).
    pub fn for_tests(ft: Arc<FakeTransport>, fo: Arc<FakeOracle>) -> Arc<Shared> {
        let cfg = Config {
            run: "test".into(),
            client_urls: (0..ft.count()).map(|i| format!("http://client{i}:8080")).collect(),
            nodes: vec!["ledger-0:8888".into(), "ledger-1:8888".into(), "ledger-2:8888".into()],
            oracle_url: "http://oracle:8090".into(),
            query_checkpoint_limit: "10".into(),
            quiet_probability: Some(1.0),
            exclusive_probability: Some(1.0),
            workload_active_file: "/nonexistent".into(),
            timing: Timing {
                q0_pause_secs: 100,
                q1: Duration::from_millis(1500),
                q2: Duration::from_millis(1500),
                q3: Duration::from_millis(1500),
                q4_extra: Duration::from_millis(300),
                pause_margin_secs: 15,
                poll: Duration::from_millis(10),
                handoff_backpressure: Duration::from_millis(200),
                backlog_pause: Duration::from_secs(5),
                resolver_backoff_first: Duration::from_millis(5),
                resolver_backoff_cap: Duration::from_millis(40),
                slow_rule: Duration::from_secs(15),
                oracle_retry: OracleRetry { first: Duration::from_millis(1), cap: Duration::from_millis(5), total: Duration::from_millis(100) },
            },
        };
        let s = Shared::new(cfg, ft, Box::new(SharedFakeOracle(fo)));
        s.active.store(true, std::sync::atomic::Ordering::SeqCst);
        let _ = s.oracle.health();
        s.start_threads();
        s
    }
}

/// A driver over fresh doubles.
pub fn driver() -> (Driver, Arc<FakeTransport>, Arc<FakeOracle>) {
    let ft = FakeTransport::new(3);
    let fo = FakeOracle::new();
    let s = Shared::for_tests(ft.clone(), fo.clone());
    (Driver { s }, ft, fo)
}
