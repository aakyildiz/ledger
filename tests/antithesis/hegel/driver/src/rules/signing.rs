//! Task 16: signing keys, signed writes and the `requireSignatures` window
//! (DESIGN §3 `r-sign-*`, §4.5, §4.8, §4.11, §4.13; critic §3.8;
//! main-drivers §3.36).
//!
//! - Setup hook `signing_setup` bootstraps the never-revoked root key
//!   `root-<run>` (unsigned register, fixed seed, no draw) and creates the
//!   Rust-checked ledger `r-sign-<run>`.
//! - Rule `signing_keys` registers children (signed by the root or a usable
//!   child), revokes non-root keys (signed by themselves or an ancestor,
//!   `cascade` drawn), lists the keys (paged) and probes that an unsigned
//!   registration is refused, against a key-tree model (`KeyTree`).
//! - Rule `signed_write` dispatches a forced tracked transaction signed by
//!   the root (the oracle validates it), or probes that a write signed by a
//!   revoked key is refused.
//! - Exclusive action `require_signatures_window` turns `requireSignatures`
//!   on, probes one node past a Barrier horizon, and always restores `false`
//!   within 20 s (Drop guard, then a quiet-blocking job that keeps retrying
//!   while `Shared.signatures_required` holds new tracked writes back).
//!
//! Management requests are cluster-scoped and untracked (`send_untracked`,
//! keyed `r-<run>-k<N>`). Probes that expect Unauthenticated or
//! PermissionDenied use raw `Driver::call`s: those codes are non-definitive
//! for `send_untracked`, which would retry them until its budget.
use crate::clients::{definitive, CallOpts, Outcome, Route, SignSpec};
use crate::draw;
use crate::exclusive::{ExclusiveAction, EXCL_RESTORE_BUDGET};
use crate::jobs::Job;
use crate::machine::{rule, Driver};
use crate::nodes::{NodeMap, NodeRef};
use crate::quiet::HookPlan;
use crate::read::{LedgerSel, ViewReq};
use crate::setup::{SetupHook, Waits};
use crate::wire::{apply_request, create_tx_request, u64_field, Posting};
use crate::write::{UntrackedSend, WritePlan};
use base64::Engine as _;
use hegel::generators as gs;
use hegel::stateful::ConcurrentRule;
use hegel::TestCase;
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// The root key's fixed seed (no draw may happen before setup completes).
pub const ROOT_SEED: &[u8; 32] = b"formance-hegel-signing-root-0001";
/// Above this many usable children the rule stops registering.
pub const MAX_LIVE_CHILDREN: usize = 24;
/// `send_untracked` budget of one management request inside a rule.
pub const MGMT_BUDGET: Duration = Duration::from_secs(6);
/// Per-attempt deadline of management requests and probes.
pub const ATTEMPT: Duration = Duration::from_secs(3);
/// Budget of one paged ListSigningKeys traversal.
pub const LIST_BUDGET: Duration = Duration::from_secs(4);
/// Budget of a pinned probe (Barrier, horizon wait, up to 4 sends).
pub const PROBE_BUDGET: Duration = Duration::from_secs(9);
/// Budget of the enable inside the window.
pub const ENABLE_BUDGET: Duration = Duration::from_secs(8);
/// The window's probes stop this long after the enable started, leaving the
/// rest of the 20 s for the restore.
pub const PROBES_UNTIL: Duration = Duration::from_secs(13);
/// The restore always gets at least this much, even past the budget.
pub const MIN_RESTORE: Duration = Duration::from_secs(2);
/// `send_untracked` budget of one setup attempt round.
pub const SETUP_BUDGET: Duration = if cfg!(test) { Duration::from_millis(300) } else { Duration::from_secs(10) };
/// Resends of a probe on unknown/Unavailable/DeadlineExceeded.
pub const PROBE_RESENDS: usize = 3;

pub fn b64(bytes: &[u8]) -> String {
    base64::engine::general_purpose::STANDARD.encode(bytes)
}

// ---------------------------------------------------------------- model

/// Where a registration or a revoke stands.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Phase {
    InFlight,
    /// Definitive success, at this logical tick.
    Acked(u64),
    /// No definitive answer (or a definitive refusal, treated as unknown).
    Unknown,
}

#[derive(Clone, Debug)]
pub struct KeyRec {
    /// The signer of the registration (the server records it as the parent);
    /// `None` for the root.
    pub parent: Option<String>,
    /// base64 of the 32-byte seed.
    pub seed: String,
    /// base64 of the public key.
    pub public: String,
    pub reg: Phase,
    /// The registration was acknowledged while a cascading revoke on its
    /// signer or one of the signer's ancestors had started: it may have been
    /// admitted by a lagging node after the cascade, as an orphan.
    pub tainted: bool,
}

#[derive(Clone, Debug)]
pub struct RevokeRec {
    pub target: String,
    pub cascade: bool,
    /// Logical tick before the first send.
    pub started: u64,
    pub state: Phase,
}

/// The derived status of a key (task spec "Model"; logs and tests).
#[allow(dead_code)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum KeyStatus {
    Live,
    Revoked,
    Unknown,
}

/// The key tree: every key the driver registered, every revoke it sent, and
/// a logical clock ordering acknowledgements against sends.
///
/// Server semantics it follows (`processor_signing.go`, `write_set.go`):
/// registration is an upsert whose parent is the batch signer; a revoke
/// removes its target and, with `cascade`, every key reachable through the
/// stored parent links of keys still present (a key orphaned by an earlier
/// non-cascading revoke of its parent is not reached); revoking an absent key
/// succeeds. Key ids are counters, so no id is ever registered twice.
#[derive(Default)]
pub struct KeyTree {
    pub root: Option<String>,
    pub keys: BTreeMap<String, KeyRec>,
    pub revokes: Vec<RevokeRec>,
    tick: u64,
    next_child: u64,
    next_probe: u64,
}

impl KeyTree {
    /// The next logical tick.
    pub fn now(&mut self) -> u64 {
        self.tick += 1;
        self.tick
    }

    pub fn add_root(&mut self, id: &str, seed: &str, public: &str) {
        let t = self.now();
        self.keys.insert(id.to_string(), KeyRec { parent: None, seed: seed.into(), public: public.into(), reg: Phase::Acked(t), tainted: false });
        self.root = Some(id.to_string());
    }

    pub fn next_child_id(&mut self, run: &str) -> String {
        self.next_child += 1;
        format!("child-{run}-{}", self.next_child)
    }

    pub fn next_probe_id(&mut self, run: &str) -> String {
        self.next_probe += 1;
        format!("probe-{run}-{}", self.next_probe)
    }

    pub fn spec(&self, id: &str) -> Option<SignSpec> {
        self.keys.get(id).map(|k| SignSpec { key_id: id.to_string(), seed: k.seed.clone() })
    }

    pub fn root_spec(&self) -> Option<SignSpec> {
        self.root.as_deref().and_then(|r| self.spec(r))
    }

    pub fn begin_register(&mut self, id: &str, parent: &str, seed: &str, public: &str) {
        self.keys.insert(id.to_string(), KeyRec { parent: Some(parent.into()), seed: seed.into(), public: public.into(), reg: Phase::InFlight, tainted: false });
    }

    pub fn end_register(&mut self, id: &str, ok: bool) {
        let t = self.now();
        let tainted = ok && {
            let mut chain = vec![];
            if let Some(p) = self.keys.get(id).and_then(|k| k.parent.clone()) {
                chain.push(p.clone());
                chain.extend(self.ancestors(&p));
            }
            chain.iter().any(|c| self.revokes.iter().any(|r| r.cascade && r.target == *c))
        };
        if let Some(k) = self.keys.get_mut(id) {
            k.reg = if ok { Phase::Acked(t) } else { Phase::Unknown };
            k.tainted = tainted;
        }
    }

    /// Records a revoke before its first send; returns its index.
    pub fn begin_revoke(&mut self, target: &str, cascade: bool) -> usize {
        let t = self.now();
        self.revokes.push(RevokeRec { target: target.into(), cascade, started: t, state: Phase::InFlight });
        self.revokes.len() - 1
    }

    pub fn end_revoke(&mut self, idx: usize, ok: bool) {
        let t = self.now();
        if let Some(r) = self.revokes.get_mut(idx) {
            r.state = if ok { Phase::Acked(t) } else { Phase::Unknown };
        }
    }

    /// The parent chain of `id`, nearest first, excluding `id` (cycle-safe).
    pub fn ancestors(&self, id: &str) -> Vec<String> {
        let mut out: Vec<String> = vec![];
        let mut cur = self.keys.get(id).and_then(|k| k.parent.clone());
        while let Some(p) = cur {
            if p == id || out.contains(&p) {
                break;
            }
            cur = self.keys.get(&p).and_then(|k| k.parent.clone());
            out.push(p);
        }
        out
    }

    fn revokes_on<'a>(&'a self, id: &'a str) -> impl Iterator<Item = &'a RevokeRec> + 'a {
        self.revokes.iter().filter(move |r| r.target == id)
    }

    fn cascade_on_ancestor(&self, id: &str) -> bool {
        let anc = self.ancestors(id);
        self.revokes.iter().any(|r| r.cascade && anc.contains(&r.target))
    }

    fn acked_before(p: Phase, t: u64) -> bool {
        matches!(p, Phase::Acked(a) if a < t)
    }

    /// A key that signs safely now: the root, or an acknowledged key that no
    /// revoke (its own, or a cascading one on an ancestor) has touched.
    pub fn usable(&self, id: &str) -> bool {
        if self.root.as_deref() == Some(id) {
            return true;
        }
        let Some(k) = self.keys.get(id) else { return false };
        matches!(k.reg, Phase::Acked(_)) && !k.tainted && self.revokes_on(id).next().is_none() && !self.cascade_on_ancestor(id)
    }

    /// Usable non-root keys (revoke candidates; also register signers with the root).
    pub fn usable_children(&self) -> Vec<String> {
        self.keys.keys().filter(|id| self.root.as_deref() != Some(id.as_str()) && self.usable(id)).cloned().collect()
    }

    /// Signers of a revoke of `target`: itself and its usable ancestors.
    pub fn revoke_signers(&self, target: &str) -> Vec<String> {
        let mut v = vec![target.to_string()];
        v.extend(self.ancestors(target).into_iter().filter(|a| self.usable(a)));
        v
    }

    /// Must be listed by a traversal that started at tick `start`, judged with
    /// everything recorded until it ended: registration acknowledged before
    /// the start, and no revoke on the key nor any cascading revoke on an
    /// ancestor, whatever its state (in flight, unknown, or started during
    /// the traversal). Keys whose registration is unknown are never expected.
    pub fn certain_live(&self, id: &str, start: u64) -> bool {
        let Some(k) = self.keys.get(id) else { return false };
        Self::acked_before(k.reg, start) && self.revokes_on(id).next().is_none() && !self.cascade_on_ancestor(id)
    }

    /// Must not be listed by a traversal that started at tick `start`: its
    /// registration was acknowledged (so no late attempt can add it after
    /// the revoke), and a revoke on itself was acknowledged before the start,
    /// or a cascading revoke of an ancestor removed it (`absent_via`).
    pub fn certain_absent(&self, id: &str, start: u64) -> bool {
        let Some(k) = self.keys.get(id) else { return false };
        if !matches!(k.reg, Phase::Acked(_)) {
            return false;
        }
        if self.revokes_on(id).any(|r| Self::acked_before(r.state, start)) {
            return true;
        }
        (0..self.revokes.len()).any(|i| self.absent_via(id, i, start))
    }

    /// The cascading revoke `idx` (acknowledged before `start`) removed `id`:
    /// its target is an ancestor of `id`; `id` and every key between them
    /// were acknowledged before the revoke started and are untainted, so the
    /// parent links existed when it ran; and no key between them had a
    /// revoke started before the cascade was acknowledged (which could have
    /// orphaned `id` first).
    pub fn absent_via(&self, id: &str, idx: usize, start: u64) -> bool {
        let Some(r) = self.revokes.get(idx) else { return false };
        let Phase::Acked(acked) = r.state else { return false };
        if !r.cascade || acked >= start || r.target == id {
            return false;
        }
        let anc = self.ancestors(id);
        let Some(pos) = anc.iter().position(|a| *a == r.target) else { return false };
        let between = &anc[..pos];
        let on_path = std::iter::once(id.to_string()).chain(between.iter().cloned());
        for x in on_path {
            let Some(k) = self.keys.get(&x) else { return false };
            if !Self::acked_before(k.reg, r.started) || k.tainted {
                return false;
            }
        }
        !between.iter().any(|b| self.revokes_on(b).any(|o| o.started < acked))
    }

    /// The keys a traversal started at `start` must list and must not list.
    pub fn expectations(&self, start: u64) -> (Vec<String>, Vec<String>) {
        let present = self.keys.keys().filter(|id| self.certain_live(id, start)).cloned().collect();
        let absent = self.keys.keys().filter(|id| self.certain_absent(id, start)).cloned().collect();
        (present, absent)
    }

    /// The cascade `idx`'s subtree as far as it is certain: its target and
    /// every key it removed (`absent_via`).
    pub fn cascade_subtree(&self, idx: usize, start: u64) -> Vec<String> {
        let Some(r) = self.revokes.get(idx) else { return vec![] };
        let mut v = vec![r.target.clone()];
        v.extend(self.keys.keys().filter(|id| self.absent_via(id, idx, start)).cloned());
        v
    }

    /// Keys certainly revoked now whose seed can still sign (for the
    /// revoked-key write probe).
    pub fn revoked_keys(&mut self) -> Vec<String> {
        let now = self.now();
        self.keys.keys().filter(|id| self.root.as_deref() != Some(id.as_str()) && self.certain_absent(id, now)).cloned().collect()
    }

    #[allow(dead_code)]
    pub fn status(&self, id: &str) -> KeyStatus {
        let start = self.tick + 1;
        if self.certain_live(id, start) {
            KeyStatus::Live
        } else if self.certain_absent(id, start) {
            KeyStatus::Revoked
        } else {
            KeyStatus::Unknown
        }
    }
}

/// Family state in `Shared` extensions.
#[derive(Default)]
pub struct SigningState {
    pub tree: Mutex<KeyTree>,
    /// The root was registered by this run (bootstrap succeeded).
    pub root_ok: AtomicBool,
    /// `r-sign-<run>` exists.
    pub ledger_ok: AtomicBool,
}

impl SigningState {
    pub fn tree(&self) -> std::sync::MutexGuard<'_, KeyTree> {
        self.tree.lock().unwrap_or_else(|e| e.into_inner())
    }
    pub fn root_ok(&self) -> bool {
        self.root_ok.load(Ordering::SeqCst)
    }
    pub fn ledger_ok(&self) -> bool {
        self.ledger_ok.load(Ordering::SeqCst)
    }
}

pub fn sign_ledger(run: &str) -> String {
    format!("r-sign-{run}")
}

// ---------------------------------------------------------------- registration

pub fn rules() -> Vec<ConcurrentRule<Driver>> {
    vec![rule("signing_keys", 0.4, signing_keys), rule("signed_write", 0.6, signed_write)]
}

pub fn exclusive_actions() -> Vec<ExclusiveAction> {
    vec![ExclusiveAction { name: "require_signatures_window", weight: 1.0, plan: window_plan, run: window_run }]
}

pub fn setup_hooks() -> Vec<SetupHook> {
    vec![SetupHook { name: "signing_setup", run: signing_setup }]
}

// ---------------------------------------------------------------- helpers

/// `POST /pubkey {seed}` on a client (the driver has no ed25519); tries every
/// client starting at `first`.
pub fn pubkey(d: &Driver, first: usize, seed: &str) -> Option<String> {
    let n = d.s.clients.len();
    for i in 0..n {
        let c = (first + i) % n;
        if let Ok(v) = d.s.clients.transport.post(c, "/pubkey", &json!({"seed": seed}), Duration::from_secs(2)) {
            if let Some(p) = v.get("publicKey").and_then(Value::as_str) {
                return Some(p.to_string());
            }
        }
    }
    // The fake client transport answers `{}` to every non-/op path.
    #[cfg(test)]
    if n > 0 {
        return Some(format!("pub:{seed}"));
    }
    None
}

/// Every answer except one the old `IsTolerated` refused: a definitive
/// business failure or an auth refusal.
fn tolerated(o: &Outcome) -> bool {
    o.is_ok() || (!definitive(o) && o.code != "Unauthenticated" && o.code != "PermissionDenied")
}

fn auth_or_business(o: &Outcome) -> bool {
    o.is_ok() || o.code == "Unauthenticated" || o.code == "PermissionDenied" || definitive(o)
}

fn resendable(o: &Outcome) -> bool {
    o.is_unknown() || o.code == "Unavailable" || o.code == "DeadlineExceeded"
}

/// One RPC re-sent at most `PROBE_RESENDS` times on unknown, Unavailable or
/// DeadlineExceeded (never after `deadline`). Returns the first auth-class,
/// business or success answer; `None` when the probe is inconclusive.
pub fn probe_call(d: &Driver, route: &Route, method: &str, req: &Value, opts: &CallOpts, deadline: Instant) -> Option<Outcome> {
    for i in 0..=PROBE_RESENDS {
        if i > 0 && Instant::now() >= deadline {
            return None;
        }
        let (_, out) = d.call(route, "bucket", method, req.clone(), opts);
        if auth_or_business(&out) {
            return Some(out);
        }
        if !resendable(&out) {
            return None;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    None
}

/// A Barrier through `client` (lib route); its commit index.
pub fn barrier_horizon(d: &Driver, client: usize, deadline: Instant) -> Option<u64> {
    for _ in 0..3 {
        let (_, out) = d.call(&Route::lib(client), "bucket", "Barrier", json!({}), &CallOpts::default().timeout(Duration::from_secs(3)));
        if out.is_ok() {
            return out.response.as_ref().map(|r| u64_field(r, "commitIndex"));
        }
        if Instant::now() >= deadline {
            return None;
        }
    }
    None
}

/// The node behind configured address `addr`: by exact service address,
/// else by port, else the only member when there is one.
pub fn resolve_node(m: &NodeMap, addr: &str, configured: usize) -> Option<NodeRef> {
    if let Some(n) = m.by_addr(addr) {
        return Some(n.clone());
    }
    let port = addr.rsplit(':').next().unwrap_or("");
    let by_port: Vec<&NodeRef> = m.nodes.iter().filter(|n| !port.is_empty() && n.addr.rsplit(':').next() == Some(port)).collect();
    if by_port.len() == 1 {
        return Some(by_port[0].clone());
    }
    if m.nodes.len() == 1 && configured == 1 {
        return Some(m.nodes[0].clone());
    }
    None
}

/// Where a pinned probe goes.
#[derive(Clone, Debug)]
pub struct ProbePlan {
    pub node_idx: usize,
    pub client: usize,
}

impl ProbePlan {
    pub fn draw(tc: &TestCase, d: &Driver) -> ProbePlan {
        ProbePlan { node_idx: draw::pick(tc, d.s.nodes.len()), client: draw::pick(tc, d.s.clients.len()) }
    }
}

/// Safety rule 4: admission reads node-local state, so a probe whose
/// expected outcome depends on a replicated change is sent pinned to one
/// node after `Barrier` → horizon and `wait_at_horizon(node, horizon,
/// exact=false)` on that node. `None` when the node could not be confirmed
/// or the probe was inconclusive.
pub fn pinned_probe(d: &Driver, p: &ProbePlan, req: &Value, sign: Option<SignSpec>, deadline: Instant) -> Option<Outcome> {
    if d.s.nodes.is_empty() {
        return None;
    }
    let addr = d.s.nodes[p.node_idx % d.s.nodes.len()].clone();
    let node = resolve_node(&NodeMap::discover(d)?, &addr, d.s.nodes.len())?;
    let horizon = barrier_horizon(d, p.client, deadline)?;
    let wait_until = deadline.min(Instant::now() + Duration::from_secs(4));
    d.wait_at_horizon(&node, horizon, false, wait_until)?;
    let opts = CallOpts { timeout: Duration::from_secs(2), max_items: None, consistency: None, sign };
    probe_call(d, &Route::node(p.client, &addr), "Apply", req, &opts, deadline)
}

/// A paged ListSigningKeys traversal; `None` unless every page completed.
pub fn list_ids(d: &Driver, route: &Route, page_size: u64, deadline: Instant) -> Option<BTreeSet<String>> {
    let mut cursor = String::new();
    let mut ids = BTreeSet::new();
    let opts = CallOpts::linearizable().timeout(Duration::from_secs(2)).max_items(1000);
    for _ in 0..64 {
        let mut o = json!({"pageSize": page_size});
        if !cursor.is_empty() {
            o["cursor"] = json!(cursor);
        }
        let out = probe_call(d, route, "ListSigningKeys", &json!({"options": o}), &opts, deadline)?;
        if !out.is_ok() || !out.complete {
            return None;
        }
        for it in out.items.as_deref().unwrap_or(&[]) {
            if let Some(id) = it.get("keyId").and_then(Value::as_str) {
                ids.insert(id.to_string());
            }
        }
        if out.next_cursor.is_empty() || out.next_cursor == cursor {
            return Some(ids);
        }
        cursor = out.next_cursor.clone();
        if Instant::now() >= deadline {
            return None;
        }
    }
    None
}

fn untracked(requests: Vec<Value>, key: String, sign: Option<SignSpec>, schedule: &[Route], backoffs: &[u64], budget: Duration) -> UntrackedSend {
    let mut s = UntrackedSend::new(requests, Some(key));
    s.schedule = schedule.to_vec();
    s.backoffs_ms = backoffs.to_vec();
    s.attempt_timeout = ATTEMPT;
    s.budget = budget;
    s.sign = sign;
    s
}

fn forced_tx(ledger: &str, dest: &str, amount: u64) -> Value {
    let mut r = create_tx_request(ledger, &[Posting::new("world", dest, amount, "COIN")], None);
    r["apply"]["action"]["createTransaction"]["force"] = json!(true);
    r
}

// ---------------------------------------------------------------- setup

/// Bootstraps the root key (unsigned register, only valid while no key
/// exists) and creates `r-sign-<run>`. No draws.
pub fn signing_setup(d: &Driver) {
    let s = &d.s;
    let st = s.ext::<SigningState>();
    let mut w = Waits::default();
    let root_id = format!("root-{}", s.run);
    let seed = b64(ROOT_SEED);
    let lib: Vec<Route> = (0..s.clients.len().max(1)).map(Route::lib).collect();
    let public = loop {
        if let Some(p) = pubkey(d, 0, &seed) {
            break p;
        }
        w.say("signing root", "waiting for a client's /pubkey".into());
        std::thread::sleep(Duration::from_secs(1));
    };
    let reg = json!({"registerSigningKey": {"keyId": root_id, "publicKey": public}});
    let key = format!("r-{}-signing-root", s.run);
    let registered = loop {
        let r = d.send_untracked(untracked(vec![reg.clone()], key.clone(), None, &lib, &[if cfg!(test) { 50 } else { 250 }], SETUP_BUDGET));
        if r.outcome.is_ok() || r.outcome.reason.contains("ALREADY_EXISTS") {
            break true;
        }
        if r.outcome.code == "Unauthenticated" {
            // Either our own earlier attempt committed (a retry is no longer
            // a bootstrap), or keys of an earlier run exist (reused volume).
            if let Some(ids) = list_ids(d, &Route::lib(0), 100, Instant::now() + Duration::from_secs(10)) {
                break ids.contains(&root_id);
            }
        }
        w.say("signing root", format!("registering: {}", r.outcome.summary()));
        std::thread::sleep(Duration::from_secs(1));
    };
    if registered {
        st.tree().add_root(&root_id, &seed, &public);
        st.root_ok.store(true, Ordering::SeqCst);
        w.say("signing root", format!("{root_id} registered"));
    } else {
        println!("setup: signing root: other signing keys already exist on this cluster; the signing family stays idle");
    }
    let ledger = sign_ledger(&s.run);
    let lkey = format!("r-{}-signing-ledger", s.run);
    loop {
        let r = d.send_untracked(untracked(vec![json!({"createLedger": {"name": ledger}})], lkey.clone(), None, &lib, &[250], Duration::from_secs(10)));
        if r.outcome.is_ok() || r.outcome.reason == "LEDGER_ALREADY_EXISTS" {
            st.ledger_ok.store(true, Ordering::SeqCst);
            w.say(&format!("ledger {ledger}"), "created".into());
            break;
        }
        w.say(&format!("ledger {ledger}"), format!("creating: {}", r.outcome.summary()));
        std::thread::sleep(Duration::from_secs(1));
    }
}

// ---------------------------------------------------------------- signing_keys

#[derive(Clone, Debug)]
pub struct ListPlan {
    pub route: Route,
    pub page_size: u64,
}

impl ListPlan {
    fn draw(tc: &TestCase, d: &Driver) -> ListPlan {
        ListPlan { route: draw::route(tc, &d.s), page_size: draw::page_size(tc) }
    }
}

#[derive(Clone, Debug)]
pub enum KeysOp {
    Register { signer: String, seed: [u8; 32], client: usize, schedule: Vec<Route>, backoffs: Vec<u64> },
    Revoke { target: String, signer: String, cascade: bool, schedule: Vec<Route>, backoffs: Vec<u64>, list: ListPlan },
    List(ListPlan),
    UnsignedRegister(ProbePlan),
}

fn draw_seed(tc: &TestCase) -> [u8; 32] {
    let mut seed = [0u8; 32];
    for chunk in seed.chunks_mut(8) {
        chunk.copy_from_slice(&tc.draw(gs::integers::<u64>()).to_le_bytes());
    }
    seed
}

pub fn plan_keys(d: &Driver, tc: &TestCase) -> Option<KeysOp> {
    let st = d.s.ext::<SigningState>();
    if !st.root_ok() {
        return None;
    }
    let (root, children) = {
        let t = st.tree();
        (t.root.clone()?, t.usable_children())
    };
    let weights = [if children.len() < MAX_LIVE_CHILDREN { 1.0 } else { 0.0 }, if children.is_empty() { 0.0 } else { 1.0 }, 0.6, 0.4];
    Some(match draw::weighted(tc, &weights)? {
        0 => {
            let mut signers = vec![root];
            signers.extend(children);
            let signer = draw::one_of(tc, &signers).clone();
            let seed = draw_seed(tc);
            KeysOp::Register { signer, seed, client: draw::pick(tc, d.s.clients.len()), schedule: draw::schedule(tc, &d.s, 4), backoffs: draw::backoffs(tc, 4) }
        }
        1 => {
            let target = draw::one_of(tc, &children).clone();
            let signers = st.tree().revoke_signers(&target);
            let signer = draw::one_of(tc, &signers).clone();
            let cascade = draw::chance(tc, 0.5);
            KeysOp::Revoke { target, signer, cascade, schedule: draw::schedule(tc, &d.s, 4), backoffs: draw::backoffs(tc, 4), list: ListPlan::draw(tc, d) }
        }
        2 => KeysOp::List(ListPlan::draw(tc, d)),
        _ => KeysOp::UnsignedRegister(ProbePlan::draw(tc, d)),
    })
}

fn signing_keys(d: &Driver, tc: TestCase) {
    d.guarded("signing_keys", &tc, |d, tc| {
        let op = plan_keys(d, tc)?;
        exec_keys(d, op)
    });
}

pub fn exec_keys(d: &Driver, op: KeysOp) -> Option<()> {
    let st = d.s.ext::<SigningState>();
    match op {
        KeysOp::Register { signer, seed, client, schedule, backoffs } => {
            let seed = b64(&seed);
            let public = pubkey(d, client, &seed)?;
            let (id, spec) = {
                let mut t = st.tree();
                let id = t.next_child_id(&d.s.run);
                let spec = t.spec(&signer)?;
                t.begin_register(&id, &signer, &seed, &public);
                (id, spec)
            };
            let req = json!({"registerSigningKey": {"keyId": id, "publicKey": public}});
            let r = d.send_untracked(untracked(vec![req], d.s.next_untracked_key(), Some(spec), &schedule, &backoffs, MGMT_BUDGET));
            st.tree().end_register(&id, r.outcome.is_ok());
            check_sometimes!(tolerated(&r.outcome), "should be able to register signing key", json!({"keyId": id, "signer": signer, "outcome": r.outcome.summary()}));
        }
        KeysOp::Revoke { target, signer, cascade, schedule, backoffs, list } => {
            let (idx, spec) = {
                let mut t = st.tree();
                let spec = t.spec(&signer)?;
                (t.begin_revoke(&target, cascade), spec)
            };
            let req = json!({"revokeSigningKey": {"keyId": target, "cascade": cascade}});
            let r = d.send_untracked(untracked(vec![req], d.s.next_untracked_key(), Some(spec), &schedule, &backoffs, MGMT_BUDGET));
            let ok = r.outcome.is_ok();
            st.tree().end_revoke(idx, ok);
            check_sometimes!(tolerated(&r.outcome), "should be able to revoke signing key", json!({"keyId": target, "signer": signer, "cascade": cascade, "outcome": r.outcome.summary()}));
            if ok && cascade {
                list_and_check(d, &list, Some(idx));
            }
        }
        KeysOp::List(list) => {
            list_and_check(d, &list, None);
        }
        KeysOp::UnsignedRegister(p) => {
            let (id, public) = {
                let mut t = st.tree();
                let root = t.root.clone()?;
                let public = t.keys.get(&root)?.public.clone();
                (t.next_probe_id(&d.s.run), public)
            };
            let req = apply_request(&[json!({"registerSigningKey": {"keyId": id, "publicKey": public}})], Some(&d.s.next_untracked_key()));
            let out = pinned_probe(d, &p, &req, None, Instant::now() + PROBE_BUDGET)?;
            check_always_or_unreachable!(
                !out.is_ok() && out.code == "Unauthenticated",
                "an unsigned key registration after bootstrap is refused as unauthenticated",
                json!({"keyId": id, "outcome": out.summary(), "peer": out.peer})
            );
        }
    }
    Some(())
}

/// One paged traversal judged against the model: keys certainly live are
/// listed, keys certainly revoked are not, and after an acknowledged cascade
/// its certain subtree is gone while every certainly-live key stays.
pub fn list_and_check(d: &Driver, plan: &ListPlan, cascade: Option<usize>) -> Option<()> {
    let st = d.s.ext::<SigningState>();
    let start = st.tree().now();
    let listed = list_ids(d, &plan.route, plan.page_size, Instant::now() + LIST_BUDGET)?;
    let t = st.tree();
    let (present, absent) = t.expectations(start);
    let missing: Vec<&String> = present.iter().filter(|k| !listed.contains(*k)).collect();
    let resurrected: Vec<&String> = absent.iter().filter(|k| listed.contains(*k)).collect();
    check_always_or_unreachable!(
        missing.is_empty(),
        "registered key should appear in ListSigningKeys",
        json!({"missing": missing, "expected": present.len(), "listed": listed.len(), "pageSize": plan.page_size})
    );
    if !absent.is_empty() {
        check_always_or_unreachable!(
            resurrected.is_empty(),
            "revoked key should not appear in ListSigningKeys",
            json!({"listedRevoked": resurrected, "revoked": absent.len(), "pageSize": plan.page_size})
        );
    }
    if let Some(idx) = cascade {
        let subtree = t.cascade_subtree(idx, start);
        let kept: Vec<&String> = subtree.iter().filter(|k| listed.contains(*k)).collect();
        check_always_or_unreachable!(
            kept.is_empty() && missing.is_empty(),
            "a cascade revoke removes exactly the subtree",
            json!({"target": subtree.first(), "subtree": subtree, "stillListed": kept, "missingOutside": missing})
        );
    }
    Some(())
}

// ---------------------------------------------------------------- signed_write

/// Transaction emission taper per ledger (DESIGN §3.2).
fn taper(tc: &TestCase, tx_count: u64) -> bool {
    match tx_count {
        0..=499 => true,
        500..=1999 => draw::chance(tc, 0.5),
        2000..=3999 => draw::chance(tc, 0.125),
        _ => false,
    }
}

fn signed_write(d: &Driver, tc: TestCase) {
    d.guarded("signed_write", &tc, |d, tc| {
        let st = d.s.ext::<SigningState>();
        if !st.root_ok() {
            return None;
        }
        if draw::chance(tc, 1.0 / 3.0) {
            let revoked = st.tree().revoked_keys();
            if !revoked.is_empty() && st.ledger_ok() {
                let key = draw::one_of(tc, &revoked).clone();
                let p = ProbePlan::draw(tc, d);
                let dest = draw::users_address(tc);
                let amount = draw::int_in(tc, 1, 1000);
                return revoked_key_write(d, &key, &p, &dest, amount);
            }
        }
        if !d.s.oracle_ok() {
            return None;
        }
        let pick = draw::ledger_pick(tc);
        let dest = draw::address(tc);
        let amount = draw::amount_positive(tc);
        let views = d.view(&[ViewReq::ledger(LedgerSel::Pick(pick))])?;
        let lv = views.first()?;
        let ledger = lv.get("ledger").and_then(Value::as_str)?.to_string();
        if !taper(tc, u64_field(lv, "txCount")) {
            return None;
        }
        let mut plan = WritePlan::drawn("signing", vec![forced_tx(&ledger, &dest, amount)], d, tc);
        plan.sign = Some(st.tree().root_spec()?);
        plan.hint = json!({"gen": "signed_write"});
        let unit = d.s.slots.try_acquire_up_to(1)?.take_one()?;
        let res = d.dispatch(plan, unit);
        check_sometimes!(res.is_success(), "a signed tracked write commits", json!({"ledger": ledger, "result": format!("{res:?}")}));
        Some(())
    });
}

/// A write to `r-sign-<run>` signed by a key whose revoke was acknowledged,
/// pinned to a node past the horizon (safety rule 4).
pub fn revoked_key_write(d: &Driver, key: &str, p: &ProbePlan, dest: &str, amount: u64) -> Option<()> {
    let st = d.s.ext::<SigningState>();
    let spec = st.tree().spec(key)?;
    let req = apply_request(&[forced_tx(&sign_ledger(&d.s.run), dest, amount)], Some(&d.s.next_untracked_key()));
    let out = pinned_probe(d, p, &req, Some(spec), Instant::now() + PROBE_BUDGET)?;
    check_always_or_unreachable!(
        !out.is_ok() && out.code == "PermissionDenied",
        "a write signed by a revoked key is refused",
        json!({"keyId": key, "outcome": out.summary(), "peer": out.peer})
    );
    Some(())
}

// ---------------------------------------------------------------- the window

#[derive(Clone, Debug)]
pub struct WindowPlan {
    pub probe: ProbePlan,
    pub schedule: Vec<Route>,
    pub backoffs: Vec<u64>,
    pub dest: String,
    pub amount: u64,
    pub enable_budget: Duration,
    pub probes_until: Duration,
    pub budget: Duration,
    pub min_restore: Duration,
}

pub fn window_plan(d: &Driver, tc: &TestCase) -> HookPlan {
    Box::new(WindowPlan {
        probe: ProbePlan::draw(tc, d),
        schedule: draw::schedule(tc, &d.s, 4),
        backoffs: draw::backoffs(tc, 4),
        dest: draw::users_address(tc),
        amount: draw::int_in(tc, 1, 1000),
        enable_budget: ENABLE_BUDGET,
        probes_until: PROBES_UNTIL,
        budget: EXCL_RESTORE_BUDGET,
        min_restore: MIN_RESTORE,
    })
}

pub fn window_run(d: &Driver, plan: &HookPlan) {
    if let Some(p) = plan.downcast_ref::<WindowPlan>() {
        run_window(d, p, &window_probes);
    }
}

pub fn set_config(require: bool) -> Value {
    json!({"setSigningConfig": {"requireSignatures": require}})
}

/// The restore of `requireSignatures: false`, signed by the root, with one
/// key for every attempt (in the action, the guard and the job).
#[derive(Clone)]
pub struct Restore {
    pub key: String,
    pub root: SignSpec,
    pub schedule: Vec<Route>,
    pub backoffs: Vec<u64>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RestoreResult {
    Done,
    Refused,
    NotDefinitive,
}

impl Restore {
    pub fn once(&self, d: &Driver, budget: Duration) -> RestoreResult {
        let r = d.send_untracked(untracked(vec![set_config(false)], self.key.clone(), Some(self.root.clone()), &self.schedule, &self.backoffs, budget));
        if r.outcome.is_ok() {
            RestoreResult::Done
        } else if definitive(&r.outcome) {
            println!("driver: signing restore refused definitively: {}", r.outcome.summary());
            RestoreResult::Refused
        } else {
            RestoreResult::NotDefinitive
        }
    }
}

/// Restores within what is left of the budget (at least `min`); if that is
/// not definitive, holds new tracked writes back (`signatures_required`) and
/// submits a quiet-blocking job that retries until definitive.
pub fn finish_restore(d: &Driver, r: &Restore, deadline: Instant, min: Duration) -> bool {
    let budget = deadline.saturating_duration_since(Instant::now()).max(min);
    if r.once(d, budget) == RestoreResult::Done {
        check_reachable!("the require-signatures window closes", json!({"where": "action"}));
        return true;
    }
    d.s.signatures_required.store(true, Ordering::SeqCst);
    let job = r.clone();
    println!("driver: signing restore not definitive within the window budget; handing it to a job");
    d.submit_job(Job { name: "signing-restore", blocks_quiet: true, run: Box::new(move |d: &Driver| restore_job(d, job)) });
    false
}

/// The fallback job: retries the restore until definitive, then clears the
/// flag. Deterministic backoff, no draws.
pub fn restore_job(d: &Driver, mut r: Restore) {
    let mut k: u32 = 0;
    loop {
        match r.once(d, Duration::from_secs(10)) {
            RestoreResult::Done => {
                d.s.signatures_required.store(false, Ordering::SeqCst);
                check_reachable!("the require-signatures window closes", json!({"where": "job", "rounds": k + 1}));
                return;
            }
            // A frozen refusal would replay forever under the same key.
            RestoreResult::Refused => r.key = d.s.next_untracked_key(),
            RestoreResult::NotDefinitive => {}
        }
        let backoff = Duration::from_millis(250u64.saturating_mul(1u64 << k.min(5))).min(Duration::from_secs(5));
        std::thread::sleep(backoff);
        k = k.saturating_add(1);
    }
}

/// Issues the restore when dropped armed (an unwind after the enable).
pub struct WindowGuard<'a> {
    d: &'a Driver,
    restore: Restore,
    deadline: Instant,
    min: Duration,
    armed: bool,
}

impl WindowGuard<'_> {
    pub fn close(mut self) -> bool {
        self.armed = false;
        finish_restore(self.d, &self.restore, self.deadline, self.min)
    }
}

impl Drop for WindowGuard<'_> {
    fn drop(&mut self) {
        if self.armed {
            self.armed = false;
            println!("driver: signing window unwound; restoring requireSignatures=false");
            finish_restore(self.d, &self.restore, self.deadline, self.min);
        }
    }
}

/// The window: enable (signed by the root), probes, restore — the restore is
/// issued on every path once the guard exists.
pub fn run_window(d: &Driver, p: &WindowPlan, probes: &dyn Fn(&Driver, &WindowPlan, Instant)) {
    let st = d.s.ext::<SigningState>();
    if !st.root_ok() || d.s.signatures_required() || !d.s.maint_closed() {
        return;
    }
    let Some(root) = st.tree().root_spec() else { return };
    let started = Instant::now();
    let guard = WindowGuard {
        d,
        restore: Restore { key: d.s.next_untracked_key(), root: root.clone(), schedule: p.schedule.clone(), backoffs: p.backoffs.clone() },
        deadline: started + p.budget,
        min: p.min_restore,
        armed: true,
    };
    let enable = d.send_untracked(untracked(vec![set_config(true)], d.s.next_untracked_key(), Some(root), &p.schedule, &p.backoffs, p.enable_budget));
    if enable.outcome.is_ok() {
        probes(d, p, started + p.probes_until);
    } else {
        println!("driver: signing window: enable not acknowledged ({}); restoring", enable.outcome.summary());
    }
    guard.close();
}

/// The probes inside the window, pinned to the planned node past the
/// enable's horizon.
pub fn window_probes(d: &Driver, p: &WindowPlan, deadline: Instant) {
    let st = d.s.ext::<SigningState>();
    if !st.ledger_ok() {
        return;
    }
    let ledger = sign_ledger(&d.s.run);
    let unsigned = apply_request(&[forced_tx(&ledger, &p.dest, p.amount)], Some(&d.s.next_untracked_key()));
    let Some(out) = pinned_probe(d, &p.probe, &unsigned, None, deadline) else { return };
    check_always_or_unreachable!(
        !out.is_ok() && out.code == "Unauthenticated",
        "an unsigned write is refused while signatures are required",
        json!({"outcome": out.summary(), "peer": out.peer})
    );
    let Some(root) = st.tree().root_spec() else { return };
    let signed = apply_request(&[forced_tx(&ledger, &p.dest, p.amount)], Some(&d.s.next_untracked_key()));
    let addr = d.s.nodes[p.probe.node_idx % d.s.nodes.len()].clone();
    let opts = CallOpts { timeout: Duration::from_secs(2), max_items: None, consistency: None, sign: Some(root) };
    let out = probe_call(d, &Route::node(p.probe.client, &addr), "Apply", &signed, &opts, deadline);
    check_sometimes!(
        out.as_ref().is_some_and(Outcome::is_ok),
        "a signed write is accepted while signatures are required",
        json!({"outcome": out.as_ref().map(Outcome::summary)})
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testkit::{self, FakeReply};

    fn ok(v: Value) -> FakeReply {
        FakeReply::Outcome(Outcome::from_json(&json!({"ok": true, "response": v})))
    }
    fn err(code: &str) -> FakeReply {
        FakeReply::Outcome(Outcome::from_json(&json!({"ok": false, "code": code, "message": "x"})))
    }
    fn lost() -> FakeReply {
        FakeReply::Outcome(Outcome::unknown("timeout"))
    }
    fn page(ids: &[&str], next: &str) -> FakeReply {
        let items: Vec<Value> = ids.iter().map(|i| json!({"keyId": i, "publicKey": "AA=="})).collect();
        FakeReply::Outcome(Outcome::from_json(&json!({"ok": true, "items": items, "complete": true, "nextCursor": next})))
    }
    fn cluster_default(ft: &testkit::FakeTransport) {
        ft.set_default(
            "cluster",
            "GetClusterState",
            ok(json!({"nodes": [{"id": "1", "serviceAddress": "ledger-0:8888"}], "localNode": "1", "syncProgress": {"status": "normal"}, "raftStatus": {"lastPersistedIndex": "100"}})),
        );
        ft.set_default("bucket", "Barrier", ok(json!({"commitIndex": "50"})));
    }
    fn setup_root(d: &Driver) -> std::sync::Arc<SigningState> {
        let st = d.s.ext::<SigningState>();
        st.tree().add_root("root-test", &b64(ROOT_SEED), "pubroot");
        st.root_ok.store(true, Ordering::SeqCst);
        st.ledger_ok.store(true, Ordering::SeqCst);
        st
    }
    fn reg(t: &mut KeyTree, id: &str, parent: &str, ok: bool) {
        t.begin_register(id, parent, "seed", "pub");
        t.end_register(id, ok);
    }
    fn s(v: &[&str]) -> Vec<String> {
        v.iter().map(|x| x.to_string()).collect()
    }
    fn window_plan_fast() -> WindowPlan {
        WindowPlan {
            probe: ProbePlan { node_idx: 0, client: 0 },
            schedule: vec![Route::plain(0)],
            backoffs: vec![5],
            dest: "users:1".into(),
            amount: 1,
            enable_budget: Duration::from_millis(200),
            probes_until: Duration::from_millis(600),
            budget: Duration::from_millis(800),
            min_restore: Duration::from_millis(100),
        }
    }
    fn config_calls(ft: &testkit::FakeTransport) -> Vec<bool> {
        ft.calls_of("bucket", "Apply")
            .iter()
            .filter_map(|c| c["request"]["unsigned"]["requests"][0]["setSigningConfig"].as_object().map(|o| o.get("requireSignatures").and_then(Value::as_bool).unwrap_or(false)))
            .collect()
    }

    // ---- model

    #[test]
    fn cascade_subtree_and_list_expectations() {
        let mut t = KeyTree::default();
        t.add_root("r", "s", "p");
        reg(&mut t, "a", "r", true);
        reg(&mut t, "b", "a", true);
        reg(&mut t, "c", "b", true);
        reg(&mut t, "u", "a", false); // registration unknown
        reg(&mut t, "x", "r", true);
        let start = t.now();
        let (present, absent) = t.expectations(start);
        assert_eq!(present, s(&["a", "b", "c", "r", "x"]), "unknown registrations are never expected");
        assert!(absent.is_empty());
        assert_eq!(t.usable_children(), s(&["a", "b", "c", "x"]));

        // An in-flight cascade on `a`: nothing below it is expected either way.
        let i = t.begin_revoke("a", true);
        let mid = t.now();
        let (present, absent) = t.expectations(mid);
        assert_eq!(present, s(&["r", "x"]));
        assert!(absent.is_empty());
        assert_eq!(t.usable_children(), s(&["x"]));
        assert_eq!(t.revoke_signers("c"), s(&["c", "r"]), "only usable ancestors sign");

        t.end_revoke(i, true);
        let after = t.now();
        let (present, absent) = t.expectations(after);
        assert_eq!(present, s(&["r", "x"]));
        assert_eq!(absent, s(&["a", "b", "c"]), "u's registration is unknown: never expected absent");
        assert_eq!(t.cascade_subtree(i, after), s(&["a", "b", "c"]));
        assert_eq!(t.status("b"), KeyStatus::Revoked);
        assert_eq!(t.status("x"), KeyStatus::Live);
        assert_eq!(t.status("u"), KeyStatus::Unknown);
        // A list that started before the ack expects nothing absent.
        assert!(t.expectations(mid).1.is_empty());
        assert_eq!(t.revoked_keys(), s(&["a", "b", "c"]));
    }

    #[test]
    fn unknown_and_orphaning_revokes_stay_uncertain() {
        let mut t = KeyTree::default();
        t.add_root("r", "s", "p");
        reg(&mut t, "a", "r", true);
        reg(&mut t, "b", "a", true);
        reg(&mut t, "c", "b", true);
        // An unknown revoke of b (non-cascading) then an acked cascade of a:
        // c may have been orphaned first, so it is neither live nor revoked.
        let ib = t.begin_revoke("b", false);
        t.end_revoke(ib, false);
        let ia = t.begin_revoke("a", true);
        t.end_revoke(ia, true);
        let now = t.now();
        assert!(t.certain_absent("a", now));
        assert!(!t.certain_absent("b", now) || t.absent_via("b", ia, now));
        assert!(t.absent_via("b", ia, now), "b sits right below a");
        assert!(!t.certain_absent("c", now), "b had a revoke before the cascade acked");
        assert!(!t.certain_live("c", now));
        assert_eq!(t.status("c"), KeyStatus::Unknown);

        // A key acknowledged while its signer's cascade was in flight is tainted.
        let mut t = KeyTree::default();
        t.add_root("r", "s", "p");
        reg(&mut t, "a", "r", true);
        let ia = t.begin_revoke("a", true);
        reg(&mut t, "late", "a", true);
        t.end_revoke(ia, true);
        let now = t.now();
        assert!(t.keys["late"].tainted);
        assert!(!t.certain_absent("late", now), "it may live on as an orphan");
        assert!(!t.certain_live("late", now));

        // A non-cascading revoke leaves the children live.
        let mut t = KeyTree::default();
        t.add_root("r", "s", "p");
        reg(&mut t, "a", "r", true);
        reg(&mut t, "b", "a", true);
        let ia = t.begin_revoke("a", false);
        t.end_revoke(ia, true);
        let now = t.now();
        assert_eq!(t.expectations(now), (s(&["b", "r"]), s(&["a"])));
        // A revoke started during a traversal removes the key from its expectations.
        let start = t.now();
        let _ = t.begin_revoke("b", false);
        assert_eq!(t.expectations(start).0, s(&["r"]));
    }

    // ---- rules with fakes

    #[test]
    fn register_list_revoke_cascade_through_the_fakes() {
        let (d, ft, _) = testkit::driver();
        testkit::reset();
        let st = setup_root(&d);
        exec_keys(&d, KeysOp::Register { signer: "root-test".into(), seed: [1; 32], client: 0, schedule: vec![Route::plain(0)], backoffs: vec![5] });
        exec_keys(&d, KeysOp::Register { signer: "child-test-1".into(), seed: [2; 32], client: 0, schedule: vec![Route::plain(0)], backoffs: vec![5] });
        let applies = ft.calls_of("bucket", "Apply");
        assert_eq!(applies[0]["sign"]["keyId"], "root-test");
        assert_eq!(applies[0]["request"]["unsigned"]["requests"][0]["registerSigningKey"]["keyId"], "child-test-1");
        assert!(applies[0]["request"]["unsigned"]["idempotencyKey"].as_str().unwrap().starts_with("r-test-k"));
        assert_eq!(applies[1]["sign"]["keyId"], "child-test-1");
        assert_eq!(st.tree().keys["child-test-2"].parent.as_deref(), Some("child-test-1"));
        assert!(testkit::fired("should be able to register signing key"));

        // A list missing child-test-2 is a finding; a full one is not.
        ft.script("bucket", "ListSigningKeys", vec![page(&["child-test-1"], "c1"), page(&["root-test"], "")]);
        let lp = ListPlan { route: Route::plain(0), page_size: 1 };
        exec_keys(&d, KeysOp::List(lp.clone()));
        let a = testkit::assertions();
        let r: Vec<_> = a.iter().filter(|x| x.message == "registered key should appear in ListSigningKeys").collect();
        assert_eq!(r.len(), 1);
        assert!(!r[0].cond);
        assert_eq!(r[0].details["missing"], json!(["child-test-2"]));
        let lists = ft.calls_of("bucket", "ListSigningKeys");
        assert_eq!(lists[1]["request"]["options"]["cursor"], "c1");

        // Cascade revoke of child-test-1, then the list shows only the root.
        testkit::reset();
        ft.script("bucket", "ListSigningKeys", vec![page(&["root-test"], "")]);
        exec_keys(&d, KeysOp::Revoke { target: "child-test-1".into(), signer: "child-test-1".into(), cascade: true, schedule: vec![Route::plain(0)], backoffs: vec![5], list: lp.clone() });
        let a = testkit::assertions();
        for m in ["registered key should appear in ListSigningKeys", "revoked key should not appear in ListSigningKeys", "a cascade revoke removes exactly the subtree"] {
            let hits: Vec<_> = a.iter().filter(|x| x.message == m).collect();
            assert_eq!(hits.len(), 1, "{m}");
            assert!(hits[0].cond, "{m}: {:?}", hits[0].details);
        }
        assert!(testkit::fired("should be able to revoke signing key"));

        // A list still showing a cascaded child fails both revoke checks.
        testkit::reset();
        ft.script("bucket", "ListSigningKeys", vec![page(&["child-test-2", "root-test"], "")]);
        exec_keys(&d, KeysOp::List(lp));
        let a = testkit::assertions();
        let r: Vec<_> = a.iter().filter(|x| x.message == "revoked key should not appear in ListSigningKeys").collect();
        assert!(!r[0].cond);
        assert_eq!(st.tree().revoked_keys(), s(&["child-test-1", "child-test-2"]));
    }

    #[test]
    fn unsigned_register_probe_is_pinned_after_the_horizon() {
        let (d, ft, _) = testkit::driver();
        testkit::reset();
        setup_root(&d);
        cluster_default(&ft);
        ft.script("bucket", "Apply", vec![lost(), err("Unauthenticated")]);
        exec_keys(&d, KeysOp::UnsignedRegister(ProbePlan { node_idx: 0, client: 1 }));
        let applies = ft.calls_of("bucket", "Apply");
        assert_eq!(applies.len(), 2, "re-sent once after a lost answer");
        assert_eq!(applies[0]["node"], "ledger-0:8888");
        assert_eq!(applies[0]["request"], applies[1]["request"]);
        assert!(applies[0].get("sign").is_none());
        assert_eq!(ft.calls_of("bucket", "Barrier").len(), 1);
        let a = testkit::assertions();
        let r: Vec<_> = a.iter().filter(|x| x.message == "an unsigned key registration after bootstrap is refused as unauthenticated").collect();
        assert!(r[0].cond);

        testkit::reset();
        ft.script("bucket", "Apply", vec![ok(json!({}))]);
        exec_keys(&d, KeysOp::UnsignedRegister(ProbePlan { node_idx: 0, client: 1 }));
        let a = testkit::assertions();
        assert!(!a.iter().find(|x| x.message == "an unsigned key registration after bootstrap is refused as unauthenticated").unwrap().cond);

        // A node that never reaches the horizon: no probe, no assertion.
        testkit::reset();
        ft.set_default("bucket", "Barrier", ok(json!({"commitIndex": "500"})));
        let before = ft.calls_of("bucket", "Apply").len();
        let t0 = Instant::now();
        assert!(exec_keys(&d, KeysOp::UnsignedRegister(ProbePlan { node_idx: 0, client: 1 })).is_none());
        assert!(t0.elapsed() < Duration::from_secs(6));
        assert_eq!(ft.calls_of("bucket", "Apply").len(), before);
        assert!(!testkit::fired("an unsigned key registration after bootstrap is refused as unauthenticated"));
    }

    #[test]
    fn revoked_key_write_expects_permission_denied() {
        let (d, ft, _) = testkit::driver();
        testkit::reset();
        let st = setup_root(&d);
        cluster_default(&ft);
        {
            let mut t = st.tree();
            reg(&mut t, "k", "root-test", true);
            let i = t.begin_revoke("k", false);
            t.end_revoke(i, true);
        }
        ft.script("bucket", "Apply", vec![err("Unavailable"), err("PermissionDenied")]);
        revoked_key_write(&d, "k", &ProbePlan { node_idx: 0, client: 0 }, "users:1", 5);
        let applies = ft.calls_of("bucket", "Apply");
        assert_eq!(applies[0]["sign"]["keyId"], "k");
        assert_eq!(applies[0]["request"]["unsigned"]["requests"][0]["apply"]["ledger"], "r-sign-test");
        assert_eq!(applies[0]["request"]["unsigned"]["requests"][0]["apply"]["action"]["createTransaction"]["force"], true);
        let a = testkit::assertions();
        assert!(a.iter().find(|x| x.message == "a write signed by a revoked key is refused").unwrap().cond);
    }

    #[test]
    fn signed_tracked_write_is_dispatched_signed_by_the_root() {
        let (d, ft, fo) = testkit::driver();
        testkit::reset();
        setup_root(&d);
        // Run until the 1/3 branch is not taken (no revoked key exists, so
        // both branches dispatch a tracked write).
        testkit::run_rule(&d, signed_write);
        let begins = fo.calls("/write/begin");
        assert!(!begins.is_empty());
        assert_eq!(begins[0]["signed"], true);
        assert_eq!(begins[0]["family"], "signing");
        assert_eq!(begins[0]["requests"][0]["apply"]["action"]["createTransaction"]["force"], true);
        let applies = ft.calls_of("bucket", "Apply");
        assert_eq!(applies[0]["sign"]["keyId"], "root-test");
        assert!(testkit::fired("a signed tracked write commits"));
    }

    #[test]
    fn rules_idle_without_a_root() {
        let (d, ft, fo) = testkit::driver();
        testkit::run_rule(&d, signing_keys);
        testkit::run_rule(&d, signed_write);
        assert!(ft.calls().is_empty());
        assert!(fo.calls("/write/begin").is_empty());
        run_window(&d, &window_plan_fast(), &window_probes);
        assert!(ft.calls().is_empty());
    }

    #[test]
    fn signing_keys_rule_runs_under_hegel() {
        let (d, ft, _) = testkit::driver();
        setup_root(&d);
        cluster_default(&ft);
        ft.set_default("bucket", "ListSigningKeys", page(&["root-test"], ""));
        ft.set_default("bucket", "Apply", ok(json!({})));
        for _ in 0..6 {
            testkit::run_rule(&d, signing_keys);
        }
        assert!(!ft.calls().is_empty());
    }

    #[test]
    fn setup_bootstraps_the_root_and_the_ledger() {
        let (d, ft, _) = testkit::driver();
        signing_setup(&d);
        let applies = ft.calls_of("bucket", "Apply");
        assert_eq!(applies[0]["request"]["unsigned"]["requests"][0]["registerSigningKey"]["keyId"], "root-test");
        assert!(applies[0].get("sign").is_none());
        assert_eq!(applies[1]["request"]["unsigned"]["requests"][0]["createLedger"]["name"], "r-sign-test");
        let st = d.s.ext::<SigningState>();
        assert!(st.root_ok() && st.ledger_ok());
        assert_eq!(st.tree().status("root-test"), KeyStatus::Live);

        // Keys of an earlier run: Unauthenticated and the root is not listed.
        let (d, ft, _) = testkit::driver();
        ft.script("bucket", "Apply", (0..30).map(|_| err("Unauthenticated")).collect());
        ft.script("bucket", "ListSigningKeys", vec![page(&["root-old"], "")]);
        signing_setup(&d);
        let st = d.s.ext::<SigningState>();
        assert!(!st.root_ok());
        assert!(st.ledger_ok());
    }

    // ---- the window

    #[test]
    fn the_window_restores_even_when_the_probes_fail() {
        let (d, ft, _) = testkit::driver();
        testkit::reset();
        setup_root(&d);
        cluster_default(&ft);
        // enable ok, both probes fail inconclusively, restore ok.
        ft.script("bucket", "Apply", vec![ok(json!({})), err("Internal"), ok(json!({}))]);
        run_window(&d, &window_plan_fast(), &window_probes);
        assert_eq!(config_calls(&ft), vec![true, false]);
        let applies = ft.calls_of("bucket", "Apply");
        assert!(applies.iter().filter(|a| a["request"]["unsigned"]["requests"][0].get("setSigningConfig").is_some()).all(|a| a["sign"]["keyId"] == "root-test"));
        assert!(testkit::fired("the require-signatures window closes"));
        assert!(!d.s.signatures_required());
        assert!(!testkit::fired("an unsigned write is refused while signatures are required"));
    }

    #[test]
    fn the_window_probes_a_pinned_node() {
        let (d, ft, _) = testkit::driver();
        testkit::reset();
        setup_root(&d);
        cluster_default(&ft);
        ft.script("bucket", "Apply", vec![ok(json!({})), err("Unauthenticated"), ok(json!({})), ok(json!({}))]);
        let mut p = window_plan_fast();
        p.probes_until = Duration::from_secs(5);
        p.budget = Duration::from_secs(6);
        run_window(&d, &p, &window_probes);
        let applies = ft.calls_of("bucket", "Apply");
        assert_eq!(applies.len(), 4);
        assert_eq!(applies[1]["node"], "ledger-0:8888");
        assert!(applies[1].get("sign").is_none());
        assert_eq!(applies[2]["sign"]["keyId"], "root-test");
        assert_eq!(applies[2]["node"], "ledger-0:8888");
        let a = testkit::assertions();
        assert!(a.iter().find(|x| x.message == "an unsigned write is refused while signatures are required").unwrap().cond);
        assert!(a.iter().find(|x| x.message == "a signed write is accepted while signatures are required").unwrap().cond);
        assert_eq!(config_calls(&ft), vec![true, false]);
    }

    #[test]
    fn a_lost_enable_is_still_restored() {
        let (d, ft, _) = testkit::driver();
        testkit::reset();
        setup_root(&d);
        ft.set_default("bucket", "Apply", lost());
        ft.script("bucket", "Apply", vec![lost(), lost(), lost()]);
        let mut p = window_plan_fast();
        p.enable_budget = Duration::from_millis(50);
        // The restore answers ok once the enable gave up.
        let ft2 = ft.clone();
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(100));
            ft2.set_default("bucket", "Apply", ok(json!({})));
        });
        run_window(&d, &p, &window_probes);
        let cfg = config_calls(&ft);
        assert!(cfg.contains(&true) && cfg.last() == Some(&false));
        assert!(!d.s.signatures_required());
    }

    #[test]
    fn an_unwind_after_the_enable_still_restores() {
        let (d, ft, _) = testkit::driver();
        testkit::reset();
        setup_root(&d);
        ft.script("bucket", "Apply", vec![ok(json!({})), ok(json!({}))]);
        let p = window_plan_fast();
        let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| run_window(&d, &p, &|_, _, _| panic!("probe bug"))));
        assert!(r.is_err());
        assert_eq!(config_calls(&ft), vec![true, false]);
        assert!(testkit::fired("the require-signatures window closes"));
        assert!(!d.s.signatures_required());
    }

    #[test]
    fn a_stuck_restore_sets_the_flag_and_a_job_clears_it() {
        let (d, ft, _) = testkit::driver();
        testkit::reset();
        setup_root(&d);
        // enable ok; then every attempt is lost until the test releases it.
        ft.script("bucket", "Apply", vec![ok(json!({}))]);
        ft.set_default("bucket", "Apply", lost());
        let p = window_plan_fast();
        let t0 = Instant::now();
        run_window(&d, &p, &|_, _, _| {});
        assert!(t0.elapsed() < Duration::from_secs(3), "the action returns at its budget");
        assert!(d.s.signatures_required(), "new tracked writes are held back");
        assert_eq!(d.s.slots.back_pressure(), Some(crate::write::BackPressure::SignaturesRequired));
        assert!(d.s.slots.try_acquire_up_to(1).is_none());
        assert_eq!(d.s.blocking_jobs(), 1, "a quiet-blocking restore job is queued or running");
        assert!(!testkit::fired("the require-signatures window closes"));
        ft.set_default("bucket", "Apply", ok(json!({})));
        testkit::wait_until(Duration::from_secs(20), || !d.s.signatures_required());
        testkit::wait_until(Duration::from_secs(5), || d.s.blocking_jobs() == 0);
        let a = testkit::assertions();
        let closes: Vec<_> = a.iter().filter(|x| x.message == "the require-signatures window closes").collect();
        assert_eq!(closes.len(), 1);
        assert_eq!(closes[0].details["where"], "job");
        // Every restore attempt (action and job) reused one key.
        let keys: BTreeSet<String> = ft
            .calls_of("bucket", "Apply")
            .iter()
            .filter(|a| a["request"]["unsigned"]["requests"][0]["setSigningConfig"]["requireSignatures"] == json!(false))
            .map(|a| a["request"]["unsigned"]["idempotencyKey"].as_str().unwrap().to_string())
            .collect();
        assert_eq!(keys.len(), 1);
    }

    #[test]
    fn the_window_skips_while_a_restore_is_pending_or_maintenance_is_on() {
        let (d, ft, _) = testkit::driver();
        setup_root(&d);
        d.s.signatures_required.store(true, Ordering::SeqCst);
        run_window(&d, &window_plan_fast(), &window_probes);
        d.s.signatures_required.store(false, Ordering::SeqCst);
        *d.s.maint.lock().unwrap() = crate::shared::MaintState::Open { since: Instant::now() };
        run_window(&d, &window_plan_fast(), &window_probes);
        assert!(ft.calls().is_empty());
    }

    #[test]
    fn registry_and_names() {
        assert_eq!(rules().iter().map(|r| r.name.clone()).collect::<Vec<_>>(), vec!["signing_keys".to_string(), "signed_write".to_string()]);
        assert_eq!(exclusive_actions()[0].name, "require_signatures_window");
        assert_eq!(setup_hooks()[0].name, "signing_setup");
        assert_eq!(ROOT_SEED.len(), 32);
        let r = resolve_node(&NodeMap::from_cluster_state(&json!({"nodes": [{"id": "1", "serviceAddress": "127.0.0.1:9000"}]})), "localhost:9000", 1);
        assert_eq!(r.unwrap().id, 1);
    }
}
