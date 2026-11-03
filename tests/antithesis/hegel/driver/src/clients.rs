//! Talking to the client containers: one HTTP request per operation, and a
//! UDP receiver for completion events that arrive after the driver stopped
//! waiting.
use antithesis_sdk::prelude::*;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::net::UdpSocket;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// The outcome of one operation, as far as the driver knows it.
#[derive(Clone, Debug)]
pub enum Outcome {
    /// A unary response.
    Ok(Value),
    /// A server-stream page: items, whether the stream ended, next cursor.
    Items(Vec<Value>, bool, String),
    /// A gRPC error from the server (or the client library).
    Err { code: String, reason: String, message: String },
    /// No answer: the client was unreachable, restarted, or timed out.
    Unknown(String),
}

impl Outcome {
    fn from_json(v: &Value) -> Outcome {
        if v.get("ok").and_then(Value::as_bool).unwrap_or(false) {
            if let Some(items) = v.get("items").and_then(Value::as_array) {
                return Outcome::Items(
                    items.clone(),
                    v.get("complete").and_then(Value::as_bool).unwrap_or(false),
                    v.get("nextCursor").and_then(Value::as_str).unwrap_or("").to_string(),
                );
            }
            if v.get("response").is_none() && v.get("complete").is_some() {
                return Outcome::Items(
                    vec![],
                    v.get("complete").and_then(Value::as_bool).unwrap_or(false),
                    v.get("nextCursor").and_then(Value::as_str).unwrap_or("").to_string(),
                );
            }
            return Outcome::Ok(v.get("response").cloned().unwrap_or(json!({})));
        }
        let s = |k: &str| v.get(k).and_then(Value::as_str).unwrap_or("").to_string();
        Outcome::Err { code: s("code"), reason: s("reason"), message: s("message") }
    }

    pub fn reason(&self) -> &str {
        match self {
            Outcome::Err { reason, .. } => reason,
            _ => "",
        }
    }

    pub fn code(&self) -> &str {
        match self {
            Outcome::Err { code, .. } => code,
            _ => "",
        }
    }

    pub fn is_ok(&self) -> bool {
        matches!(self, Outcome::Ok(_) | Outcome::Items(..))
    }

    pub fn summary(&self) -> Value {
        match self {
            Outcome::Ok(_) => json!({"ok": true}),
            Outcome::Items(items, complete, _) => json!({"ok": true, "items": items.len(), "complete": complete}),
            Outcome::Err { code, reason, message } => json!({"code": code, "reason": reason, "message": message}),
            Outcome::Unknown(why) => json!({"unknown": why}),
        }
    }
}

/// Options of one call.
#[derive(Clone, Default)]
pub struct CallOpts {
    pub timeout: Option<Duration>,
    pub max_items: usize,
    /// Send directly to this node ("ledger-1:8888") instead of round-robin.
    pub node: Option<String>,
    pub stale: bool,
}

pub struct Clients {
    pub urls: Vec<String>,
    http: ureq::Agent,
    boots: Vec<Mutex<String>>,
    next_op: AtomicU64,
    /// Completion events that arrived over UDP, by op id. Operations that
    /// got no HTTP answer are resolved from here later.
    late: Mutex<HashMap<u64, Outcome>>,
    active: Arc<AtomicBool>,
}

/// The default RPC deadline the client library gets. Its retry interceptor
/// retries transient errors within it.
pub const RPC_TIMEOUT: Duration = Duration::from_secs(8);

impl Clients {
    pub fn new(urls: Vec<String>, active: Arc<AtomicBool>) -> Clients {
        let boots = urls.iter().map(|_| Mutex::new(String::new())).collect();
        Clients {
            urls,
            http: ureq::AgentBuilder::new().timeout_connect(Duration::from_millis(1000)).build(),
            boots,
            next_op: AtomicU64::new(1),
            late: Mutex::new(HashMap::new()),
            active,
        }
    }

    pub fn len(&self) -> usize {
        self.urls.len()
    }

    pub fn next_op_id(&self) -> u64 {
        self.next_op.fetch_add(1, Ordering::Relaxed)
    }

    /// Starts the completion-event receiver on UDP 7778.
    pub fn start_receiver(self: &Arc<Self>) {
        let me = self.clone();
        let sock = UdpSocket::bind("0.0.0.0:7778").expect("bind 7778");
        std::thread::spawn(move || {
            let mut buf = vec![0u8; 65536];
            loop {
                let Ok(n) = sock.recv(&mut buf) else { continue };
                let Ok(v) = serde_json::from_slice::<Value>(&buf[..n]) else { continue };
                let Some(op) = v.get("opId").and_then(Value::as_u64) else { continue };
                let out = Outcome::from_json(v.get("outcome").unwrap_or(&Value::Null));
                let mut late = me.late.lock().unwrap();
                late.insert(op, out);
                if late.len() > 200_000 {
                    // Old events are no longer consulted; keep memory bounded.
                    let cutoff = op.saturating_sub(100_000);
                    late.retain(|k, _| *k >= cutoff);
                }
            }
        });
    }

    /// A completion event received for an operation, if any.
    pub fn late_outcome(&self, op: u64) -> Option<Outcome> {
        self.late.lock().unwrap().get(&op).cloned()
    }

    /// GET /health on one client; returns its boot id.
    pub fn health(&self, client: usize) -> Result<String, String> {
        let resp = self
            .http
            .get(&format!("{}/health", self.urls[client]))
            .timeout(Duration::from_millis(1000))
            .call()
            .map_err(|e| e.to_string())?;
        let v: Value = resp.into_json().map_err(|e| e.to_string())?;
        let boot = v.get("boot").and_then(Value::as_str).unwrap_or("").to_string();
        self.observe_boot(client, &boot);
        Ok(boot)
    }

    fn observe_boot(&self, client: usize, boot: &str) {
        if boot.is_empty() {
            return;
        }
        let mut b = self.boots[client].lock().unwrap();
        if *b != boot {
            if !b.is_empty() && self.active.load(Ordering::Relaxed) {
                assert_reachable!("the driver notices a client restart", &json!({"client": client}));
            }
            *b = boot.to_string();
        }
    }

    /// Runs one RPC through a client. Returns the op id and the outcome.
    pub fn call(&self, client: usize, service: &str, method: &str, request: Value, opts: &CallOpts) -> (u64, Outcome) {
        let op = self.next_op_id();
        let timeout = opts.timeout.unwrap_or(RPC_TIMEOUT);
        let mut body = json!({
            "opId": op,
            "service": service,
            "method": method,
            "request": request,
            "timeoutMs": timeout.as_millis() as u64,
            "maxItems": if opts.max_items == 0 { 1000 } else { opts.max_items },
        });
        if let Some(node) = &opts.node {
            body["node"] = json!(node);
        }
        if opts.stale {
            body["consistency"] = json!("stale");
        }
        let started = Instant::now();
        let resp = self
            .http
            .post(&format!("{}/op", self.urls[client]))
            .timeout(timeout + Duration::from_millis(1500))
            .send_json(body);
        let out = match resp {
            Ok(r) => match r.into_json::<Value>() {
                Ok(v) => {
                    self.observe_boot(client, v.get("boot").and_then(Value::as_str).unwrap_or(""));
                    Outcome::from_json(v.get("outcome").unwrap_or(&Value::Null))
                }
                Err(e) => Outcome::Unknown(format!("bad response: {e}")),
            },
            Err(ureq::Error::Status(code, r)) => {
                // 400: the driver built a request the client could not parse.
                let text = r.into_string().unwrap_or_default();
                assert_unreachable!(
                    "the client program accepts every request the driver builds",
                    &json!({"method": method, "status": code, "body": text})
                );
                Outcome::Unknown(format!("http {code}: {text}"))
            }
            Err(e) => Outcome::Unknown(format!("{e} after {:?}", started.elapsed())),
        };
        (op, out)
    }

    /// Like call, but for an Apply with the given batch.
    pub fn apply(&self, client: usize, requests: Vec<Value>, idempotency_key: Option<&str>) -> (u64, Outcome) {
        let mut batch = json!({ "requests": requests });
        if let Some(k) = idempotency_key {
            batch["idempotencyKey"] = json!(k);
        }
        self.call(client, "bucket", "Apply", json!({ "unsigned": batch }), &CallOpts::default())
    }
}
