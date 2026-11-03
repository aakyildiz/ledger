//! The oracle HTTP client (DESIGN §4): typed request/response structs for
//! every endpoint (camelCase; protojson payloads stay `serde_json::Value`;
//! harness counters are numbers, server values decimal strings), one
//! `ureq::Agent` with a 60 s timeout, idempotent retry of the same body on
//! transport failures, boot-id tracking, error-code mapping, and degraded
//! mode.
#![allow(dead_code)] // family-facing API: every endpoint is typed even where 00c does not call it

use crate::clients::Outcome;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;
use std::time::{Duration, Instant};

// ---------------------------------------------------------------- structs

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Violation {
    pub id: String,
    #[serde(default)]
    pub details: Value,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Verdict {
    pub ok: bool,
    pub skipped: bool,
    #[serde(default)]
    pub violations: Vec<Violation>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HealthResp {
    pub boot_id: String,
    pub ready: bool,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SetupReq {
    pub run_id: String,
    pub tracked_prefix: String,
    pub query_checkpoint_limit: String,
    pub live_target: u64,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SetupResp {
    pub boot_id: String,
    pub run_id: String,
    pub tracked_prefix: String,
    pub version: u64,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ViewReqBody {
    pub views: Vec<Value>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ViewResp {
    pub boot_id: String,
    pub version: u64,
    pub views: Vec<Value>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WriteBeginReq {
    pub wid: String,
    pub idempotency_key: String,
    pub requests: Vec<Value>,
    pub track_key: bool,
    pub role: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub of: Option<String>,
    pub family: String,
    pub signed: bool,
    #[serde(default)]
    pub hint: Value,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WriteBeginResp {
    pub boot_id: String,
    pub ticket: u64,
    pub version: u64,
    #[serde(default)]
    pub predicted: Value,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Via {
    pub op_id: u64,
    pub client: usize,
    pub route: String,
    pub late: bool,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WriteEndReq {
    pub ticket: u64,
    pub outcome: Outcome,
    pub attempts: u64,
    pub via: Via,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WriteEndResp {
    pub boot_id: String,
    pub status: String,
    pub verdict: Verdict,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WriteLateReq {
    pub ticket: u64,
    pub outcome: Outcome,
    pub via: Via,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct VerdictResp {
    pub boot_id: String,
    pub verdict: Verdict,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WriteAbandonReq {
    pub ticket: u64,
    pub attempts: u64,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReadBeginReq {
    pub rid: String,
    pub kind: String,
    pub views: Vec<Value>,
    #[serde(default)]
    pub params: Value,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReadBeginResp {
    pub boot_id: String,
    pub ticket: u64,
    pub version: u64,
    pub views: Vec<Value>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReadRoute {
    pub client: usize,
    pub route: String,
    pub node: String,
    pub consistency: String,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReadEndReq {
    pub ticket: u64,
    pub kind: String,
    pub request: Value,
    pub outcome: Outcome,
    pub route: ReadRoute,
    #[serde(default)]
    pub hint: Value,
    #[serde(default)]
    pub params: Value,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReadEndResp {
    pub boot_id: String,
    pub verdict: Verdict,
    #[serde(default)]
    pub coverage: Vec<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReadAbandonReq {
    pub ticket: u64,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QuietStatusResp {
    pub boot_id: String,
    pub inflight: u64,
    pub reads: u64,
    pub pending: u64,
    pub quiet: bool,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EpochReq {
    pub epoch: u64,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QuietLedgers {
    #[serde(default)]
    pub live: Vec<String>,
    #[serde(default)]
    pub deleted: Vec<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LedgerExpect {
    pub tx_count: u64,
    pub log_count: u64,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QuietBeginResp {
    pub boot_id: String,
    pub epoch: u64,
    pub version: u64,
    pub audit_cursor: String,
    pub ledgers: QuietLedgers,
    #[serde(default)]
    pub expect: BTreeMap<String, LedgerExpect>,
    #[serde(default)]
    pub providers: Value,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QuietSectionReq {
    pub epoch: u64,
    pub section: String,
    pub seq: u64,
    pub body: Value,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QuietSectionResp {
    pub boot_id: String,
    pub verdict: Verdict,
    #[serde(default)]
    pub next: Value,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QuietSummary {
    pub sections: u64,
    pub violations: u64,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QuietEndResp {
    pub boot_id: String,
    pub summary: QuietSummary,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TicketsResp {
    pub boot_id: String,
    #[serde(default)]
    pub inflight: Vec<u64>,
    #[serde(default)]
    pub reads: Vec<u64>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct JournalEntry {
    pub n: u64,
    pub boot: String,
    pub ep: String,
    pub req: Value,
    pub status: u16,
    pub resp: Value,
    #[serde(default)]
    pub violations: Vec<String>,
    pub ms: u64,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct JournalResp {
    pub boot_id: String,
    pub entries: Vec<JournalEntry>,
    pub next: u64,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DumpLedger {
    pub name: String,
    pub live: bool,
    pub tx_count: u64,
    pub log_count: u64,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DumpResp {
    pub boot_id: String,
    pub version: u64,
    pub fingerprint: String,
    pub ledgers: Vec<DumpLedger>,
    #[serde(default)]
    pub inflight: Vec<u64>,
    #[serde(default)]
    pub pending: Vec<u64>,
    #[serde(default)]
    pub reads: Vec<u64>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ErrorBody {
    pub code: String,
    #[serde(default)]
    pub message: String,
    #[serde(default)]
    pub details: Value,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ErrorResp {
    pub boot_id: String,
    pub error: ErrorBody,
}

// ---------------------------------------------------------------- errors

/// The error codes of DESIGN §4.1, plus the driver's own conditions.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OracleError {
    Unmodeled,
    Protocol,
    UnknownTicket,
    Busy,
    Backlog,
    Serialized,
    NotQuiet,
    Setup,
    Panic,
    /// No answer for the whole retry budget (degraded mode follows).
    Unreachable,
    /// The driver is in degraded mode; nothing was sent.
    Degraded,
    /// A different boot id answered (degraded mode follows).
    Restarted,
    /// Anything else (unparsable body, unexpected status or code).
    Other,
}

impl OracleError {
    pub fn from_code(code: &str) -> OracleError {
        match code {
            "unmodeled" => OracleError::Unmodeled,
            "protocol" => OracleError::Protocol,
            "unknown_ticket" => OracleError::UnknownTicket,
            "busy" => OracleError::Busy,
            "backlog" => OracleError::Backlog,
            "serialized" => OracleError::Serialized,
            "not_quiet" => OracleError::NotQuiet,
            "setup" => OracleError::Setup,
            "panic" => OracleError::Panic,
            _ => OracleError::Other,
        }
    }
}

/// An oracle refusal or failure, with the service's message and details.
#[derive(Clone, Debug, PartialEq)]
pub struct OracleRefusal {
    pub kind: OracleError,
    pub status: u16,
    pub message: String,
    pub details: Value,
}

impl OracleRefusal {
    pub fn new(kind: OracleError, message: &str) -> Self {
        OracleRefusal { kind, status: 0, message: message.to_string(), details: Value::Null }
    }
}

// ---------------------------------------------------------------- transport

/// The HTTP side of the oracle (a fake in unit tests). Returns the status
/// and parsed JSON body, or a transport error description.
pub trait OracleTransport: Send + Sync {
    fn send(&self, method: &str, path: &str, body: Option<&Value>, timeout: Duration) -> Result<(u16, Value), String>;
}

pub struct HttpOracleTransport {
    base: String,
    http: ureq::Agent,
}

impl HttpOracleTransport {
    pub fn new(base: &str) -> Self {
        HttpOracleTransport {
            base: base.trim_end_matches('/').to_string(),
            http: ureq::AgentBuilder::new().timeout_connect(Duration::from_secs(2)).timeout(ORACLE_TIMEOUT).build(),
        }
    }
}

impl OracleTransport for HttpOracleTransport {
    fn send(&self, method: &str, path: &str, body: Option<&Value>, timeout: Duration) -> Result<(u16, Value), String> {
        let url = format!("{}{}", self.base, path);
        let r = match body {
            Some(b) => self.http.request(method, &url).timeout(timeout).send_json(b.clone()),
            None => self.http.request(method, &url).timeout(timeout).call(),
        };
        match r {
            Ok(resp) => {
                let status = resp.status();
                resp.into_json::<Value>().map(|v| (status, v)).map_err(|e| format!("bad body: {e}"))
            }
            Err(ureq::Error::Status(code, resp)) => {
                let text = resp.into_string().unwrap_or_default();
                match serde_json::from_str::<Value>(&text) {
                    Ok(v) => Ok((code, v)),
                    Err(_) => Err(format!("http {code}: {text}")),
                }
            }
            Err(e) => Err(e.to_string()),
        }
    }
}

/// Per-request timeout of every oracle call (DESIGN §2.5).
pub const ORACLE_TIMEOUT: Duration = Duration::from_secs(60);

/// Retry schedule of one oracle call (DESIGN §4.1).
#[derive(Clone, Copy, Debug)]
pub struct OracleRetry {
    pub first: Duration,
    pub cap: Duration,
    pub total: Duration,
}

impl Default for OracleRetry {
    fn default() -> Self {
        OracleRetry { first: Duration::from_millis(250), cap: Duration::from_secs(5), total: Duration::from_secs(300) }
    }
}

pub struct OracleClient {
    t: Box<dyn OracleTransport>,
    boot: Mutex<Option<String>>,
    ok: AtomicBool,
    pub retry: OracleRetry,
}

impl OracleClient {
    pub fn new(t: Box<dyn OracleTransport>) -> Self {
        OracleClient { t, boot: Mutex::new(None), ok: AtomicBool::new(true), retry: OracleRetry::default() }
    }

    /// False once the driver entered degraded mode (permanent).
    pub fn ok(&self) -> bool {
        self.ok.load(Ordering::SeqCst)
    }

    /// Enters degraded mode (DESIGN §4.2).
    pub fn degrade(&self, why: &str) {
        if self.ok.swap(false, Ordering::SeqCst) {
            println!("driver: oracle degraded mode: {why}");
        }
    }

    pub fn boot_id(&self) -> Option<String> {
        self.boot.lock().unwrap_or_else(|e| e.into_inner()).clone()
    }

    fn observe_boot(&self, body: &Value) -> Result<(), OracleRefusal> {
        let Some(b) = body.get("bootId").and_then(Value::as_str) else { return Ok(()) };
        let mut cur = self.boot.lock().unwrap_or_else(|e| e.into_inner());
        match cur.as_deref() {
            None => {
                *cur = Some(b.to_string());
                Ok(())
            }
            Some(old) if old == b => Ok(()),
            Some(old) => {
                let old = old.to_string();
                drop(cur);
                check_unreachable!("the oracle service never restarts", json!({"old": old, "new": b}));
                self.degrade("oracle boot id changed");
                Err(OracleRefusal::new(OracleError::Restarted, "the oracle restarted"))
            }
        }
    }

    /// One call with retry. GETs pass `None`. A refusal (non-200 with an
    /// error body) is returned as `Err` after the driver-side assertions of
    /// the DESIGN §4.1 table.
    pub fn call<Resp: DeserializeOwned>(&self, method: &str, path: &str, body: Option<&Value>) -> Result<Resp, OracleRefusal> {
        self.call_inner(method, path, body, true)
    }

    /// Like `call`, but usable before setup and in degraded mode (health).
    pub fn call_unguarded<Resp: DeserializeOwned>(&self, method: &str, path: &str, body: Option<&Value>) -> Result<Resp, OracleRefusal> {
        self.call_inner(method, path, body, false)
    }

    fn call_inner<Resp: DeserializeOwned>(&self, method: &str, path: &str, body: Option<&Value>, guarded: bool) -> Result<Resp, OracleRefusal> {
        if guarded && !self.ok() {
            return Err(OracleRefusal::new(OracleError::Degraded, "degraded mode"));
        }
        let started = Instant::now();
        let mut backoff = self.retry.first;
        loop {
            match self.t.send(method, path, body, ORACLE_TIMEOUT) {
                Ok((status, v)) => {
                    self.observe_boot(&v)?;
                    if status == 200 {
                        return serde_json::from_value::<Resp>(v.clone()).map_err(|e| {
                            println!("driver: oracle {method} {path}: unexpected response shape: {e}: {v}");
                            OracleRefusal { kind: OracleError::Other, status, message: e.to_string(), details: v }
                        });
                    }
                    if let Some(err) = v.get("error") {
                        return Err(self.refusal(method, path, status, err, body));
                    }
                    // A non-200 without an error body (a proxy, a crash page): retry.
                    println!("driver: oracle {method} {path}: http {status} without error body");
                }
                Err(e) => {
                    if started.elapsed() < Duration::from_secs(1) || backoff >= self.retry.cap {
                        println!("driver: oracle {method} {path}: {e}");
                    }
                }
            }
            if started.elapsed() + backoff > self.retry.total {
                check_unreachable!("the oracle service stays reachable", json!({"endpoint": format!("{method} {path}"), "seconds": started.elapsed().as_secs()}));
                self.degrade("oracle unreachable");
                return Err(OracleRefusal::new(OracleError::Unreachable, "oracle unreachable"));
            }
            std::thread::sleep(backoff);
            backoff = (backoff * 2).min(self.retry.cap);
        }
    }

    fn refusal(&self, method: &str, path: &str, status: u16, err: &Value, body: Option<&Value>) -> OracleRefusal {
        let code = err.get("code").and_then(Value::as_str).unwrap_or("");
        let message = err.get("message").and_then(Value::as_str).unwrap_or("").to_string();
        let details = err.get("details").cloned().unwrap_or(Value::Null);
        let kind = OracleError::from_code(code);
        let ctx = json!({"endpoint": format!("{method} {path}"), "status": status, "code": code, "message": message, "details": details});
        match kind {
            OracleError::Unmodeled | OracleError::Protocol => {
                let mut d = ctx.clone();
                d["request"] = body.cloned().unwrap_or(Value::Null);
                check_unreachable!("the oracle accepts every request the driver sends", d);
            }
            OracleError::Busy => {
                check_unreachable!("the driver never exceeds the oracle's in-flight cap", ctx);
            }
            OracleError::UnknownTicket => {
                let boot = self.boot_id();
                check_unreachable!("the oracle service never restarts", json!({"old": boot, "new": boot, "unknownTicket": ctx}));
                self.degrade("oracle forgot a ticket");
            }
            OracleError::Panic | OracleError::Setup | OracleError::Other => {
                println!("driver: oracle {method} {path}: {status} {code}: {message}");
            }
            _ => {}
        }
        OracleRefusal { kind, status, message, details }
    }

    // ---- typed endpoints

    pub fn health(&self) -> Result<HealthResp, OracleRefusal> {
        self.call_unguarded("GET", "/health", None)
    }
    pub fn setup(&self, r: &SetupReq) -> Result<SetupResp, OracleRefusal> {
        self.call("POST", "/setup", Some(&to_value(r)))
    }
    pub fn view(&self, views: Vec<Value>) -> Result<ViewResp, OracleRefusal> {
        self.call("POST", "/view", Some(&to_value(&ViewReqBody { views })))
    }
    pub fn write_begin(&self, r: &WriteBeginReq) -> Result<WriteBeginResp, OracleRefusal> {
        self.call("POST", "/write/begin", Some(&to_value(r)))
    }
    pub fn write_end(&self, r: &WriteEndReq) -> Result<WriteEndResp, OracleRefusal> {
        self.call("POST", "/write/end", Some(&to_value(r)))
    }
    pub fn write_late(&self, r: &WriteLateReq) -> Result<VerdictResp, OracleRefusal> {
        self.call("POST", "/write/late", Some(&to_value(r)))
    }
    pub fn write_abandon(&self, r: &WriteAbandonReq) -> Result<VerdictResp, OracleRefusal> {
        self.call("POST", "/write/abandon", Some(&to_value(r)))
    }
    pub fn read_begin(&self, r: &ReadBeginReq) -> Result<ReadBeginResp, OracleRefusal> {
        self.call("POST", "/read/begin", Some(&to_value(r)))
    }
    pub fn read_end(&self, r: &ReadEndReq) -> Result<ReadEndResp, OracleRefusal> {
        self.call("POST", "/read/end", Some(&to_value(r)))
    }
    pub fn read_abandon(&self, r: &ReadAbandonReq) -> Result<VerdictResp, OracleRefusal> {
        self.call("POST", "/read/abandon", Some(&to_value(r)))
    }
    pub fn quiet_status(&self) -> Result<QuietStatusResp, OracleRefusal> {
        self.call("GET", "/quiet/status", None)
    }
    pub fn quiet_begin(&self, epoch: u64) -> Result<QuietBeginResp, OracleRefusal> {
        self.call("POST", "/quiet/begin", Some(&to_value(&EpochReq { epoch })))
    }
    pub fn quiet_section(&self, r: &QuietSectionReq) -> Result<QuietSectionResp, OracleRefusal> {
        self.call("POST", "/quiet/section", Some(&to_value(r)))
    }
    /// `/quiet/end` is posted even in degraded mode when the epoch was begun.
    pub fn quiet_end(&self, epoch: u64) -> Result<QuietEndResp, OracleRefusal> {
        self.call_unguarded("POST", "/quiet/end", Some(&to_value(&EpochReq { epoch })))
    }
    pub fn tickets(&self) -> Result<TicketsResp, OracleRefusal> {
        self.call("GET", "/tickets", None)
    }
    pub fn journal(&self, from: u64, limit: u64) -> Result<JournalResp, OracleRefusal> {
        self.call_unguarded("GET", &format!("/journal?from={from}&limit={limit}"), None)
    }
    pub fn dump(&self) -> Result<DumpResp, OracleRefusal> {
        self.call_unguarded("GET", "/dump", None)
    }
}

pub fn to_value<T: Serialize>(t: &T) -> Value {
    serde_json::to_value(t).expect("oracle request serializes")
}

#[cfg(test)]
mod fixtures {
    //! Every core fixture's request and response deserializes into the
    //! driver's structs and re-serializes to the same JSON (serde_json maps
    //! are sorted, so equality is canonical).
    use super::*;
    use std::path::PathBuf;

    fn dir() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../protocol-fixtures/core")
    }

    fn rt<T: Serialize + DeserializeOwned>(file: &str, what: &str, v: &Value) {
        let t: T = serde_json::from_value(v.clone()).unwrap_or_else(|e| panic!("{file} {what}: {e}"));
        let back = serde_json::to_value(&t).unwrap();
        assert_eq!(&back, v, "{file} {what} does not round-trip");
    }

    fn check_request(file: &str, endpoint: &str, req: &Value) {
        let path = endpoint.split_once(' ').map(|(_, p)| p).unwrap_or("");
        let path = path.split('?').next().unwrap();
        match path {
            "/setup" => rt::<SetupReq>(file, "request", req),
            "/view" => rt::<ViewReqBody>(file, "request", req),
            "/write/begin" => rt::<WriteBeginReq>(file, "request", req),
            "/write/end" => rt::<WriteEndReq>(file, "request", req),
            "/write/late" => rt::<WriteLateReq>(file, "request", req),
            "/write/abandon" => rt::<WriteAbandonReq>(file, "request", req),
            "/read/begin" => rt::<ReadBeginReq>(file, "request", req),
            "/read/end" => rt::<ReadEndReq>(file, "request", req),
            "/read/abandon" => rt::<ReadAbandonReq>(file, "request", req),
            "/quiet/begin" | "/quiet/end" => rt::<EpochReq>(file, "request", req),
            "/quiet/section" => rt::<QuietSectionReq>(file, "request", req),
            other => panic!("{file}: unexpected request for {other}"),
        }
    }

    fn check_response(file: &str, endpoint: &str, status: u64, resp: &Value) {
        if status != 200 {
            rt::<ErrorResp>(file, "error response", resp);
            let code = resp["error"]["code"].as_str().unwrap();
            assert_ne!(OracleError::from_code(code), OracleError::Other, "{file}: unknown code {code}");
            return;
        }
        let path = endpoint.split_once(' ').map(|(_, p)| p).unwrap_or("");
        let path = path.split('?').next().unwrap();
        match path {
            "/health" => rt::<HealthResp>(file, "response", resp),
            "/setup" => rt::<SetupResp>(file, "response", resp),
            "/view" => rt::<ViewResp>(file, "response", resp),
            "/write/begin" => rt::<WriteBeginResp>(file, "response", resp),
            "/write/end" => rt::<WriteEndResp>(file, "response", resp),
            "/write/late" | "/write/abandon" | "/read/abandon" => rt::<VerdictResp>(file, "response", resp),
            "/read/begin" => rt::<ReadBeginResp>(file, "response", resp),
            "/read/end" => rt::<ReadEndResp>(file, "response", resp),
            "/quiet/status" => rt::<QuietStatusResp>(file, "response", resp),
            "/quiet/begin" => rt::<QuietBeginResp>(file, "response", resp),
            "/quiet/section" => rt::<QuietSectionResp>(file, "response", resp),
            "/quiet/end" => rt::<QuietEndResp>(file, "response", resp),
            "/tickets" => rt::<TicketsResp>(file, "response", resp),
            "/journal" => rt::<JournalResp>(file, "response", resp),
            "/dump" => rt::<DumpResp>(file, "response", resp),
            other => panic!("{file}: unexpected endpoint {other}"),
        }
    }

    #[test]
    fn every_core_fixture_round_trips() {
        let mut n = 0;
        let mut endpoints = std::collections::BTreeSet::new();
        let mut entries: Vec<_> = std::fs::read_dir(dir()).expect("fixtures dir").map(|e| e.unwrap().path()).collect();
        entries.sort();
        for p in entries {
            if p.extension().and_then(|e| e.to_str()) != Some("json") {
                continue;
            }
            let file = p.file_name().unwrap().to_string_lossy().to_string();
            let v: Value = serde_json::from_slice(&std::fs::read(&p).unwrap()).unwrap();
            let endpoint = v["endpoint"].as_str().unwrap();
            if endpoint == "outcome" {
                let o: Outcome = serde_json::from_value(v["response"].clone()).unwrap();
                assert_eq!(serde_json::to_value(&o).unwrap(), v["response"], "{file}: outcome does not round-trip");
                assert_eq!(crate::clients::definitive(&o), v["definitive"].as_bool().unwrap(), "{file}: definitive");
                n += 1;
                continue;
            }
            endpoints.insert(endpoint.split('?').next().unwrap().to_string());
            if !v["request"].is_null() {
                check_request(&file, endpoint, &v["request"]);
            }
            check_response(&file, endpoint, v["status"].as_u64().unwrap(), &v["response"]);
            n += 1;
        }
        assert!(n >= 44, "only {n} fixtures checked");
        for ep in ["GET /health", "POST /setup", "POST /view", "POST /write/begin", "POST /write/end", "POST /write/late", "POST /write/abandon", "POST /read/begin", "POST /read/end", "POST /read/abandon", "GET /quiet/status", "POST /quiet/begin", "POST /quiet/section", "POST /quiet/end", "GET /tickets", "GET /journal", "GET /dump"] {
            assert!(endpoints.contains(ep), "no fixture for {ep}");
        }
    }

    #[test]
    fn definitive_reasons_copy_is_byte_identical() {
        let fixture = std::fs::read(dir().join("definitive_reasons.txt")).unwrap();
        assert_eq!(crate::clients::DEFINITIVE_REASONS.as_bytes(), &fixture[..]);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    struct Scripted(Mutex<Vec<Result<(u16, Value), String>>>, Arc<Mutex<usize>>);
    impl OracleTransport for Scripted {
        fn send(&self, _m: &str, _p: &str, _b: Option<&Value>, _t: Duration) -> Result<(u16, Value), String> {
            *self.1.lock().unwrap() += 1;
            let mut s = self.0.lock().unwrap();
            if s.is_empty() {
                Err("down".into())
            } else {
                s.remove(0)
            }
        }
    }

    fn client(script: Vec<Result<(u16, Value), String>>) -> (OracleClient, Arc<Mutex<usize>>) {
        let calls = Arc::new(Mutex::new(0));
        let mut c = OracleClient::new(Box::new(Scripted(Mutex::new(script), calls.clone())));
        c.retry = OracleRetry { first: Duration::from_millis(1), cap: Duration::from_millis(4), total: Duration::from_millis(60) };
        (c, calls)
    }

    #[test]
    fn retries_then_degrades_when_unreachable() {
        crate::testkit::reset();
        let (c, calls) = client(vec![Err("x".into()), Ok((200, json!({"bootId": "1", "ready": true})))]);
        assert!(c.health().unwrap().ready);
        assert_eq!(*calls.lock().unwrap(), 2);
        let r = c.tickets();
        assert_eq!(r.unwrap_err().kind, OracleError::Unreachable);
        assert!(!c.ok());
        assert!(crate::testkit::fired("the oracle service stays reachable"));
        assert_eq!(c.tickets().unwrap_err().kind, OracleError::Degraded);
    }

    #[test]
    fn boot_change_and_unknown_ticket_degrade() {
        crate::testkit::reset();
        let (c, _) = client(vec![Ok((200, json!({"bootId": "1", "ready": true}))), Ok((200, json!({"bootId": "2", "ready": true})))]);
        c.health().unwrap();
        assert_eq!(c.health().unwrap_err().kind, OracleError::Restarted);
        assert!(!c.ok());
        assert!(crate::testkit::fired("the oracle service never restarts"));

        crate::testkit::reset();
        let (c, _) = client(vec![Ok((404, json!({"bootId": "1", "error": {"code": "unknown_ticket", "message": "", "details": {}}})))]);
        let e = c.write_abandon(&WriteAbandonReq { ticket: 1, attempts: 0 }).unwrap_err();
        assert_eq!(e.kind, OracleError::UnknownTicket);
        assert!(!c.ok());
        assert!(crate::testkit::fired("the oracle service never restarts"));
    }

    #[test]
    fn error_codes_map_and_assert() {
        crate::testkit::reset();
        let err = |code: &str, status: u16| Ok((status, json!({"bootId": "1", "error": {"code": code, "message": "m", "details": {}}})));
        let (c, _) = client(vec![err("unmodeled", 400), err("busy", 409), err("backlog", 409), err("serialized", 409), err("not_quiet", 409)]);
        let req = WriteBeginReq::default();
        assert_eq!(c.write_begin(&req).unwrap_err().kind, OracleError::Unmodeled);
        assert!(crate::testkit::fired("the oracle accepts every request the driver sends"));
        assert_eq!(c.write_begin(&req).unwrap_err().kind, OracleError::Busy);
        assert!(crate::testkit::fired("the driver never exceeds the oracle's in-flight cap"));
        assert_eq!(c.write_begin(&req).unwrap_err().kind, OracleError::Backlog);
        assert_eq!(c.write_begin(&req).unwrap_err().kind, OracleError::Serialized);
        assert_eq!(c.quiet_begin(1).unwrap_err().kind, OracleError::NotQuiet);
        assert!(c.ok());
    }
}
