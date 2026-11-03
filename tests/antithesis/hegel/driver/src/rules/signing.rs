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
use crate::nodes::{horizon_check, Horizon, NodeMap, NodeRef};
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
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// The root key's fixed seed (no draw may happen before setup completes).
pub const ROOT_SEED: &[u8; 32] = b"formance-hegel-signing-root-0001";
/// Above this many usable children the rule stops registering.
pub const MAX_LIVE_CHILDREN: usize = 24;
/// At this many keys in the model (pruning keeps certainly revoked leaves
/// out of it) the rule stops registering.
pub const MAX_MODEL_KEYS: usize = 256;
/// Pruned (certainly revoked) keys kept for the list and probe checks.
pub const MAX_PRUNED: usize = 64;
/// Whole time of one management request inside a rule, last attempt
/// included (`send_untracked` may start an attempt just before its budget).
pub const MGMT_TOTAL: Duration = Duration::from_secs(6);
/// Per-attempt deadline of management requests inside a rule.
pub const MGMT_ATTEMPT: Duration = Duration::from_secs(2);
/// Per-attempt deadline of the window's enable and restore.
pub const ATTEMPT: Duration = Duration::from_secs(3);
/// Budget of one paged ListSigningKeys traversal.
pub const LIST_BUDGET: Duration = Duration::from_secs(4);
/// A rule body stops starting work this long after it began (COMMON: about
/// 10 s per rule).
pub const RULE_BUDGET: Duration = Duration::from_secs(10);
/// Budget of a pinned probe (discover, Barrier, horizon wait, up to 4 sends);
/// every call is capped at what is left.
pub const PROBE_BUDGET: Duration = Duration::from_secs(9);
/// Whole time of the enable inside the window, last attempt included.
pub const ENABLE_BUDGET: Duration = Duration::from_secs(8);
/// Maximum pages of one ListSigningKeys traversal (the deadline bounds it too).
pub const MAX_PAGES: usize = 512;
/// A call is not started with less than this left before its deadline.
pub const MIN_CALL: Duration = Duration::from_millis(150);
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
    /// Ids of the revokes sent on this key (index into `KeyTree::revokes`).
    pub revokes: Vec<u64>,
}

#[derive(Clone, Debug)]
pub struct RevokeRec {
    pub target: String,
    pub cascade: bool,
    /// Logical tick before the first send.
    pub started: u64,
    pub state: Phase,
}

/// A key moved out of the tree once it was certainly absent and had no
/// recorded children: certainly absent for every traversal started at or
/// after `since`.
#[derive(Clone, Debug)]
pub struct Pruned {
    pub id: String,
    pub seed: String,
    pub since: u64,
}

/// The derived status of a key (task spec "Model"; logs and tests).
#[allow(dead_code)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum KeyStatus {
    Live,
    Revoked,
    Unknown,
}

/// The key tree: every key the driver registered that is not yet pruned,
/// every revoke on those keys, the last `MAX_PRUNED` pruned keys, and a
/// logical clock ordering acknowledgements against sends.
///
/// Server semantics it follows (`processor_signing.go`, `write_set.go`):
/// registration is an upsert whose parent is the batch signer; a revoke
/// removes its target and, with `cascade`, every key reachable through the
/// stored parent links of keys still present (a key orphaned by an earlier
/// non-cascading revoke of its parent is not reached); revoking an absent key
/// succeeds. Key ids are counters, so no id is ever registered twice.
///
/// Size: a certainly absent key with no recorded children is pruned (with
/// the revokes on it), so no remaining key's parent chain passes through a
/// pruned key; registration stops at `MAX_MODEL_KEYS` remaining keys (keys
/// with uncertain outcomes are never pruned).
#[derive(Default)]
pub struct KeyTree {
    pub root: Option<String>,
    pub keys: BTreeMap<String, KeyRec>,
    pub revokes: BTreeMap<u64, RevokeRec>,
    pub pruned: VecDeque<Pruned>,
    tick: u64,
    next_child: u64,
    next_probe: u64,
    next_revoke: u64,
}

impl KeyTree {
    /// The next logical tick.
    pub fn now(&mut self) -> u64 {
        self.tick += 1;
        self.tick
    }

    pub fn add_root(&mut self, id: &str, seed: &str, public: &str) {
        let t = self.now();
        self.keys.insert(id.to_string(), KeyRec { parent: None, seed: seed.into(), public: public.into(), reg: Phase::Acked(t), tainted: false, revokes: vec![] });
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

    /// The signing spec of a key still in the tree (never a pruned one).
    pub fn spec(&self, id: &str) -> Option<SignSpec> {
        self.keys.get(id).map(|k| SignSpec { key_id: id.to_string(), seed: k.seed.clone() })
    }

    /// The signing spec of a key, pruned ones included (revoked-key probe).
    pub fn any_spec(&self, id: &str) -> Option<SignSpec> {
        self.spec(id).or_else(|| self.pruned.iter().find(|p| p.id == id).map(|p| SignSpec { key_id: id.to_string(), seed: p.seed.clone() }))
    }

    pub fn root_spec(&self) -> Option<SignSpec> {
        self.root.as_deref().and_then(|r| self.spec(r))
    }

    pub fn begin_register(&mut self, id: &str, parent: &str, seed: &str, public: &str) {
        self.keys.insert(id.to_string(), KeyRec { parent: Some(parent.into()), seed: seed.into(), public: public.into(), reg: Phase::InFlight, tainted: false, revokes: vec![] });
    }

    pub fn end_register(&mut self, id: &str, ok: bool) {
        let t = self.now();
        let tainted = ok && {
            let mut chain = vec![];
            if let Some(p) = self.keys.get(id).and_then(|k| k.parent.clone()) {
                chain.push(p.clone());
                chain.extend(self.ancestors(&p));
            }
            chain.iter().any(|c| self.revokes_on(c).any(|r| r.cascade))
        };
        if let Some(k) = self.keys.get_mut(id) {
            k.reg = if ok { Phase::Acked(t) } else { Phase::Unknown };
            k.tainted = tainted;
        }
    }

    /// Records a revoke before its first send; returns its id.
    pub fn begin_revoke(&mut self, target: &str, cascade: bool) -> u64 {
        let t = self.now();
        self.next_revoke += 1;
        let id = self.next_revoke;
        self.revokes.insert(id, RevokeRec { target: target.into(), cascade, started: t, state: Phase::InFlight });
        if let Some(k) = self.keys.get_mut(target) {
            k.revokes.push(id);
        }
        id
    }

    pub fn end_revoke(&mut self, id: u64, ok: bool) {
        let t = self.now();
        if let Some(r) = self.revokes.get_mut(&id) {
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

    fn revokes_on<'a>(&'a self, id: &str) -> impl Iterator<Item = &'a RevokeRec> + 'a {
        self.keys.get(id).into_iter().flat_map(|k| k.revokes.iter()).filter_map(|i| self.revokes.get(i))
    }

    fn cascade_on_ancestor(&self, id: &str) -> bool {
        self.ancestors(id).iter().any(|a| self.revokes_on(a).any(|r| r.cascade))
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
        matches!(k.reg, Phase::Acked(_)) && !k.tainted && k.revokes.is_empty() && !self.cascade_on_ancestor(id)
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
        Self::acked_before(k.reg, start) && k.revokes.is_empty() && !self.cascade_on_ancestor(id)
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
        let anc = self.ancestors(id);
        anc.iter().enumerate().any(|(pos, a)| self.revokes_on(a).any(|r| self.cascade_removed(id, &anc, pos, r, start)))
    }

    /// The cascading revoke `rid` (acknowledged before `start`) removed `id`.
    pub fn absent_via(&self, id: &str, rid: u64, start: u64) -> bool {
        let Some(r) = self.revokes.get(&rid) else { return false };
        let anc = self.ancestors(id);
        let Some(pos) = anc.iter().position(|a| *a == r.target) else { return false };
        self.cascade_removed(id, &anc, pos, r, start)
    }

    /// `r`, on `anc[pos]` (an ancestor of `id`), removed `id`: it cascades
    /// and was acknowledged before `start`; `id` and every key between them
    /// were acknowledged before the revoke started and are untainted, so the
    /// parent links existed when it ran; and no key between them had a
    /// revoke started before the cascade was acknowledged (which could have
    /// orphaned `id` first).
    fn cascade_removed(&self, id: &str, anc: &[String], pos: usize, r: &RevokeRec, start: u64) -> bool {
        let Phase::Acked(acked) = r.state else { return false };
        if !r.cascade || acked >= start || r.target == id {
            return false;
        }
        let between = &anc[..pos];
        for x in std::iter::once(id).chain(between.iter().map(String::as_str)) {
            let Some(k) = self.keys.get(x) else { return false };
            if !Self::acked_before(k.reg, r.started) || k.tainted {
                return false;
            }
        }
        !between.iter().any(|b| self.revokes_on(b).any(|o| o.started < acked))
    }

    /// The keys a traversal started at `start` must list and must not list.
    pub fn expectations(&self, start: u64) -> (Vec<String>, Vec<String>) {
        let present = self.keys.keys().filter(|id| self.certain_live(id, start)).cloned().collect();
        let mut absent: Vec<String> = self.keys.keys().filter(|id| self.certain_absent(id, start)).cloned().collect();
        absent.extend(self.pruned.iter().filter(|p| p.since <= start).map(|p| p.id.clone()));
        absent.sort();
        (present, absent)
    }

    /// The cascade `rid`'s subtree as far as it is certain: its target and
    /// every key it removed (`absent_via`). Taken before `prune`.
    pub fn cascade_subtree(&self, rid: u64, start: u64) -> Vec<String> {
        let Some(r) = self.revokes.get(&rid) else { return vec![] };
        let mut v = vec![r.target.clone()];
        v.extend(self.keys.keys().filter(|id| self.absent_via(id, rid, start)).cloned());
        v
    }

    /// Keys certainly revoked now whose seed can still sign (for the
    /// revoked-key write probe), pruned ones included.
    pub fn revoked_keys(&mut self) -> Vec<String> {
        let now = self.now();
        let mut v: Vec<String> = self.keys.keys().filter(|id| self.root.as_deref() != Some(id.as_str()) && self.certain_absent(id, now)).cloned().collect();
        v.extend(self.pruned.iter().map(|p| p.id.clone()));
        v.sort();
        v
    }

    /// Moves certainly absent keys without recorded children out of the
    /// tree, leaves first, and drops settled revokes on keys no longer in it.
    pub fn prune(&mut self) {
        loop {
            let now = self.now();
            let parents: BTreeSet<&str> = self.keys.values().filter_map(|k| k.parent.as_deref()).collect();
            let gone: Vec<String> = self
                .keys
                .keys()
                .filter(|id| self.root.as_deref() != Some(id.as_str()) && !parents.contains(id.as_str()) && self.certain_absent(id, now))
                .cloned()
                .collect();
            if gone.is_empty() {
                break;
            }
            for id in gone {
                if let Some(k) = self.keys.remove(&id) {
                    self.pruned.push_back(Pruned { id, seed: k.seed, since: now });
                }
            }
        }
        while self.pruned.len() > MAX_PRUNED {
            self.pruned.pop_front();
        }
        let keys = &self.keys;
        self.revokes.retain(|_, r| r.state == Phase::InFlight || keys.contains_key(&r.target));
    }

    #[allow(dead_code)]
    pub fn status(&self, id: &str) -> KeyStatus {
        let start = self.tick + 1;
        if self.certain_live(id, start) {
            KeyStatus::Live
        } else if self.certain_absent(id, start) || self.pruned.iter().any(|p| p.id == id) {
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

/// Time left before `deadline`, `None` below `MIN_CALL`.
pub fn left(deadline: Instant) -> Option<Duration> {
    let rem = deadline.saturating_duration_since(Instant::now());
    (rem >= MIN_CALL).then_some(rem)
}

/// One RPC re-sent at most `PROBE_RESENDS` times on unknown, Unavailable or
/// DeadlineExceeded; no call starts after `deadline` and each call's timeout
/// is capped at the time left. Returns the first auth-class, business or
/// success answer; `None` when the probe is inconclusive.
pub fn probe_call(d: &Driver, route: &Route, method: &str, req: &Value, opts: &CallOpts, deadline: Instant) -> Option<Outcome> {
    for _ in 0..=PROBE_RESENDS {
        let rem = left(deadline)?;
        let o = opts.clone().timeout(opts.timeout.min(rem));
        let (_, out) = d.call(route, "bucket", method, req.clone(), &o);
        if auth_or_business(&out) {
            return Some(out);
        }
        if !resendable(&out) {
            return None;
        }
        std::thread::sleep(Duration::from_millis(100).min(deadline.saturating_duration_since(Instant::now())));
    }
    None
}

/// A Barrier through `client` (lib route); its commit index. Calls are
/// capped at the time left before `deadline`.
pub fn barrier_horizon(d: &Driver, client: usize, deadline: Instant) -> Option<u64> {
    for _ in 0..3 {
        let rem = left(deadline)?;
        let (_, out) = d.call(&Route::lib(client), "bucket", "Barrier", json!({}), &CallOpts::default().timeout(Duration::from_secs(3).min(rem)));
        if out.is_ok() {
            return out.response.as_ref().map(|r| u64_field(r, "commitIndex"));
        }
    }
    None
}

/// `NodeMap::discover` with every call capped at the time left.
pub fn discover_until(d: &Driver, deadline: Instant) -> Option<NodeMap> {
    for c in 0..d.s.clients.len() {
        let rem = left(deadline)?;
        let (_, out) = d.call(&Route::lib(c), "cluster", "GetClusterState", json!({}), &CallOpts::default().timeout(Duration::from_secs(3).min(rem)));
        if let Some(resp) = out.response.as_ref().filter(|_| out.is_ok()) {
            let m = NodeMap::from_cluster_state(resp);
            if !m.nodes.is_empty() {
                return Some(m);
            }
        }
    }
    None
}

/// `Driver::wait_at_horizon(node, horizon, exact=false)` with every poll
/// capped at the time left before `deadline`.
pub fn wait_horizon_until(d: &Driver, node: &NodeRef, horizon: u64, deadline: Instant) -> Option<u64> {
    loop {
        let rem = left(deadline)?;
        let (_, out) = d.call(&Route::node(0, &node.addr), "cluster", "GetClusterState", json!({"nodeId": node.id.to_string()}), &CallOpts::default().timeout(Duration::from_secs(2).min(rem)));
        if let Some(cs) = out.response.as_ref().filter(|_| out.is_ok()) {
            if let Horizon::Reached(p) = horizon_check(cs, node.id, horizon, false) {
                return Some(p);
            }
        }
        if Instant::now() + Duration::from_millis(200) >= deadline {
            return None;
        }
        std::thread::sleep(Duration::from_millis(200));
    }
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
    let node = resolve_node(&discover_until(d, deadline)?, &addr, d.s.nodes.len())?;
    let horizon = barrier_horizon(d, p.client, deadline)?;
    let wait_until = deadline.min(Instant::now() + Duration::from_secs(4));
    wait_horizon_until(d, &node, horizon, wait_until)?;
    let opts = CallOpts { timeout: Duration::from_secs(2), max_items: None, consistency: None, sign };
    probe_call(d, &Route::node(p.client, &addr), "Apply", req, &opts, deadline)
}

/// A paged ListSigningKeys traversal; `None` unless every page completed
/// (at most `MAX_PAGES` pages, before `deadline`). A cursor the server hands
/// back unchanged is a pagination error, not the end of the list: the
/// traversal is inconclusive (it would otherwise look like missing keys).
pub fn list_ids(d: &Driver, route: &Route, page_size: u64, deadline: Instant) -> Option<BTreeSet<String>> {
    let mut cursor = String::new();
    let mut ids = BTreeSet::new();
    let opts = CallOpts::linearizable().timeout(Duration::from_secs(2)).max_items(1000);
    for _ in 0..MAX_PAGES {
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
        if out.next_cursor.is_empty() {
            return Some(ids);
        }
        if out.next_cursor == cursor {
            println!("driver: signing: ListSigningKeys repeated cursor {cursor:?}; traversal inconclusive");
            return None;
        }
        cursor = out.next_cursor.clone();
        if Instant::now() >= deadline {
            return None;
        }
    }
    None
}

/// `total` bounds the whole send: `send_untracked` may start an attempt just
/// before its budget, so the budget is `total` minus one attempt.
fn untracked(requests: Vec<Value>, key: String, sign: Option<SignSpec>, schedule: &[Route], backoffs: &[u64], total: Duration, attempt: Duration) -> UntrackedSend {
    let mut s = UntrackedSend::new(requests, Some(key));
    s.schedule = schedule.to_vec();
    s.backoffs_ms = backoffs.to_vec();
    s.attempt_timeout = attempt.min(total);
    s.budget = total.saturating_sub(attempt);
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
        let r = d.send_untracked(untracked(vec![reg.clone()], key.clone(), None, &lib, &[if cfg!(test) { 50 } else { 250 }], SETUP_BUDGET + ATTEMPT, ATTEMPT));
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
        let r = d.send_untracked(untracked(vec![json!({"createLedger": {"name": ledger}})], lkey.clone(), None, &lib, &[250], Duration::from_secs(13), ATTEMPT));
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
    let (root, children, model) = {
        let t = st.tree();
        (t.root.clone()?, t.usable_children(), t.keys.len())
    };
    let can_register = children.len() < MAX_LIVE_CHILDREN && model < MAX_MODEL_KEYS;
    let weights = [if can_register { 1.0 } else { 0.0 }, if children.is_empty() { 0.0 } else { 1.0 }, 0.6, 0.4];
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
            // Prefer the root (3/4): a self-signed revoke whose first attempt
            // committed can never be acknowledged by a retry (the signature
            // is checked before the idempotency replay, and the key is gone).
            let signer = if draw::chance(tc, 0.75) {
                root
            } else {
                let signers = st.tree().revoke_signers(&target);
                draw::one_of(tc, &signers).clone()
            };
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
    let rule_deadline = Instant::now() + RULE_BUDGET;
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
            let r = d.send_untracked(untracked(vec![req], d.s.next_untracked_key(), Some(spec), &schedule, &backoffs, MGMT_TOTAL, MGMT_ATTEMPT));
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
            let r = d.send_untracked(untracked(vec![req], d.s.next_untracked_key(), Some(spec), &schedule, &backoffs, MGMT_TOTAL, MGMT_ATTEMPT));
            let ok = r.outcome.is_ok();
            // The cascade's certain subtree is taken before pruning moves it out.
            let subtree = {
                let mut t = st.tree();
                t.end_revoke(idx, ok);
                let now = t.now();
                let sub = (ok && cascade).then(|| t.cascade_subtree(idx, now));
                t.prune();
                sub
            };
            check_sometimes!(tolerated(&r.outcome), "should be able to revoke signing key", json!({"keyId": target, "signer": signer, "cascade": cascade, "outcome": r.outcome.summary()}));
            if let Some(sub) = subtree {
                list_and_check(d, &list, Some(sub), rule_deadline);
            }
        }
        KeysOp::List(list) => {
            list_and_check(d, &list, None, rule_deadline);
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
pub fn list_and_check(d: &Driver, plan: &ListPlan, cascade: Option<Vec<String>>, rule_deadline: Instant) -> Option<()> {
    let st = d.s.ext::<SigningState>();
    let deadline = rule_deadline.min(Instant::now() + LIST_BUDGET);
    left(deadline)?;
    let start = st.tree().now();
    let listed = list_ids(d, &plan.route, plan.page_size, deadline)?;
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
    if let Some(subtree) = cascade {
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
    let spec = st.tree().any_spec(key)?;
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
    /// Whole time of the first enable send, last attempt included.
    pub enable_budget: Duration,
    /// The probes, and the re-sends settling an unacknowledged enable, stop
    /// this long after the window started.
    pub probes_until: Duration,
    pub budget: Duration,
    pub min_restore: Duration,
    /// Per-attempt deadline of the enable and the restore.
    pub attempt: Duration,
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
        attempt: ATTEMPT,
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
    pub attempt: Duration,
    /// The idempotency key of an enable that got no definitive answer: an
    /// attempt of it may still be in transit and commit after a restore,
    /// leaving `requireSignatures` on for good. It is re-sent under the same
    /// key until definitive (later duplicates then replay instead of
    /// applying) before any restore is sent.
    pub pending_enable: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RestoreResult {
    Done,
    Refused,
    NotDefinitive,
}

impl Restore {
    /// One restore send bounded by `total` (last attempt included).
    pub fn once(&self, d: &Driver, total: Duration) -> RestoreResult {
        let r = d.send_untracked(untracked(vec![set_config(false)], self.key.clone(), Some(self.root.clone()), &self.schedule, &self.backoffs, total, self.attempt));
        if r.outcome.is_ok() {
            RestoreResult::Done
        } else if definitive(&r.outcome) {
            println!("driver: signing restore refused definitively: {}", r.outcome.summary());
            RestoreResult::Refused
        } else {
            RestoreResult::NotDefinitive
        }
    }

    /// Re-sends the pending enable under its key, bounded by `total`;
    /// true (and clears it) once an answer was definitive.
    pub fn settle_enable(&mut self, d: &Driver, total: Duration) -> bool {
        let Some(key) = self.pending_enable.clone() else { return true };
        let r = d.send_untracked(untracked(vec![set_config(true)], key, Some(self.root.clone()), &self.schedule, &self.backoffs, total, self.attempt));
        if definitive(&r.outcome) {
            self.pending_enable = None;
            true
        } else {
            false
        }
    }
}

/// Restores within what is left of the budget (at least `min`); if that is
/// not definitive, or an unsettled enable may still land, holds new tracked
/// writes back (`signatures_required`) and submits a quiet-blocking job that
/// settles the enable, then retries the restore until definitive.
pub fn finish_restore(d: &Driver, r: &Restore, deadline: Instant, min: Duration) -> bool {
    if r.pending_enable.is_none() {
        let total = deadline.saturating_duration_since(Instant::now()).max(min);
        if r.once(d, total) == RestoreResult::Done {
            check_reachable!("the require-signatures window closes", json!({"where": "action"}));
            return true;
        }
        println!("driver: signing restore not definitive within the window budget; handing it to a job");
    } else {
        // A restore now could be overtaken by the late enable; the job
        // settles the enable first.
        println!("driver: signing enable still unsettled at the window budget; handing it and the restore to a job");
    }
    d.s.signatures_required.store(true, Ordering::SeqCst);
    let job = r.clone();
    d.submit_job(Job { name: "signing-restore", blocks_quiet: true, run: Box::new(move |d: &Driver| restore_job(d, job)) });
    false
}

fn job_backoff(k: u32) -> Duration {
    Duration::from_millis(250u64.saturating_mul(1u64 << k.min(5))).min(Duration::from_secs(5))
}

/// The fallback job: settles a pending enable (same key, until definitive),
/// then retries the restore until definitive and clears the flag.
/// Deterministic backoff, no draws.
pub fn restore_job(d: &Driver, mut r: Restore) {
    let mut k: u32 = 0;
    while !r.settle_enable(d, Duration::from_secs(10)) {
        std::thread::sleep(job_backoff(k));
        k = k.saturating_add(1);
    }
    let mut rounds: u32 = 0;
    loop {
        rounds = rounds.saturating_add(1);
        match r.once(d, Duration::from_secs(10)) {
            RestoreResult::Done => {
                d.s.signatures_required.store(false, Ordering::SeqCst);
                check_reachable!("the require-signatures window closes", json!({"where": "job", "rounds": rounds}));
                return;
            }
            // A frozen refusal would replay forever under the same key.
            RestoreResult::Refused => r.key = d.s.next_untracked_key(),
            RestoreResult::NotDefinitive => {}
        }
        std::thread::sleep(job_backoff(rounds - 1));
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
/// issued on every path once the guard exists. An enable without a
/// definitive answer is re-sent under its key (up to `probes_until`) before
/// the restore, so no delayed attempt of it can commit after the restore.
pub fn run_window(d: &Driver, p: &WindowPlan, probes: &dyn Fn(&Driver, &WindowPlan, Instant)) {
    let st = d.s.ext::<SigningState>();
    if !st.root_ok() || d.s.signatures_required() || !d.s.maint_closed() {
        return;
    }
    let Some(root) = st.tree().root_spec() else { return };
    let started = Instant::now();
    let enable_key = d.s.next_untracked_key();
    let mut guard = WindowGuard {
        d,
        restore: Restore {
            key: d.s.next_untracked_key(),
            root: root.clone(),
            schedule: p.schedule.clone(),
            backoffs: p.backoffs.clone(),
            attempt: p.attempt,
            pending_enable: Some(enable_key.clone()),
        },
        deadline: started + p.budget,
        min: p.min_restore,
        armed: true,
    };
    let enable = d.send_untracked(untracked(vec![set_config(true)], enable_key, Some(root), &p.schedule, &p.backoffs, p.enable_budget, p.attempt));
    if definitive(&enable.outcome) {
        guard.restore.pending_enable = None;
    }
    if enable.outcome.is_ok() {
        probes(d, p, started + p.probes_until);
    } else {
        println!("driver: signing window: enable not acknowledged ({}); settling it, then restoring", enable.outcome.summary());
        let total = (started + p.probes_until).saturating_duration_since(Instant::now());
        if guard.restore.pending_enable.is_some() && total >= MIN_CALL {
            guard.restore.settle_enable(d, total);
        }
    }
    guard.close();
}

/// The probes inside the window, pinned to the planned node past the
/// enable's horizon; nothing is sent past `deadline`.
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
    if left(deadline).is_none() {
        return;
    }
    let Some(root) = st.tree().root_spec() else { return };
    let signed = apply_request(&[forced_tx(&ledger, &p.dest, p.amount)], Some(&d.s.next_untracked_key()));
    let addr = d.s.nodes[p.probe.node_idx % d.s.nodes.len()].clone();
    let opts = CallOpts { timeout: Duration::from_secs(2), max_items: None, consistency: None, sign: Some(root) };
    let Some(out) = probe_call(d, &Route::node(p.probe.client, &addr), "Apply", &signed, &opts, deadline) else { return };
    check_sometimes!(out.is_ok(), "a signed write is accepted while signatures are required", json!({"outcome": out.summary()}));
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
            attempt: Duration::from_millis(100),
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

        // Our own first attempt committed but its answer was lost: the retries
        // are no longer a bootstrap (Unauthenticated), and the list shows our root.
        let (d, ft, _) = testkit::driver();
        let mut replies = vec![lost()];
        replies.extend((0..10).map(|_| err("Unauthenticated")));
        ft.script("bucket", "Apply", replies);
        ft.script("bucket", "ListSigningKeys", vec![page(&["root-test"], "")]);
        signing_setup(&d);
        let st = d.s.ext::<SigningState>();
        assert!(st.root_ok(), "our root is listed: the bootstrap succeeded");
        assert!(st.ledger_ok());
        assert_eq!(st.tree().status("root-test"), KeyStatus::Live);
        assert_eq!(ft.calls_of("bucket", "ListSigningKeys").len(), 1);
    }

    // ---- the window

    #[test]
    fn the_window_restores_even_when_the_probes_fail() {
        let (d, ft, _) = testkit::driver();
        testkit::reset();
        setup_root(&d);
        cluster_default(&ft);
        // enable ok, the first (unsigned) probe fails inconclusively (so the
        // signed one is never sent), restore ok.
        ft.script("bucket", "Apply", vec![ok(json!({})), err("Internal"), ok(json!({}))]);
        run_window(&d, &window_plan_fast(), &window_probes);
        assert_eq!(config_calls(&ft), vec![true, false]);
        assert_eq!(ft.calls_of("bucket", "Apply").len(), 3);
        let applies = ft.calls_of("bucket", "Apply");
        assert!(applies.iter().filter(|a| a["request"]["unsigned"]["requests"][0].get("setSigningConfig").is_some()).all(|a| a["sign"]["keyId"] == "root-test"));
        assert!(testkit::fired("the require-signatures window closes"));
        assert!(!d.s.signatures_required());
        assert!(!testkit::fired("an unsigned write is refused while signatures are required"));

        // Both probes run and the signed one fails inconclusively: still restored.
        let (d, ft, _) = testkit::driver();
        testkit::reset();
        setup_root(&d);
        cluster_default(&ft);
        ft.script("bucket", "Apply", vec![ok(json!({})), err("Unauthenticated"), err("Internal"), ok(json!({}))]);
        let mut p = window_plan_fast();
        p.probes_until = Duration::from_secs(5);
        p.budget = Duration::from_secs(6);
        run_window(&d, &p, &window_probes);
        let applies = ft.calls_of("bucket", "Apply");
        assert_eq!(applies.len(), 4, "enable, unsigned probe, signed probe, restore");
        assert_eq!(applies[2]["sign"]["keyId"], "root-test");
        assert_eq!(config_calls(&ft), vec![true, false]);
        assert!(testkit::fired("an unsigned write is refused while signatures are required"));
        assert!(!testkit::fired("a signed write is accepted while signatures are required"), "an inconclusive probe asserts nothing");
        assert!(testkit::fired("the require-signatures window closes"));
        assert!(!d.s.signatures_required());
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
        // The enable was re-sent under its key until definitive, before any restore.
        let first_false = cfg.iter().position(|c| !c).unwrap();
        assert!(cfg[first_false..].iter().all(|c| !c), "no enable after the first restore: {cfg:?}");
        assert_eq!(enable_keys(&ft).len(), 1);
        assert!(!d.s.signatures_required());
        assert!(testkit::fired("the require-signatures window closes"));
    }

    fn enable_keys(ft: &testkit::FakeTransport) -> BTreeSet<String> {
        ft.calls_of("bucket", "Apply")
            .iter()
            .filter(|a| a["request"]["unsigned"]["requests"][0]["setSigningConfig"]["requireSignatures"] == json!(true))
            .map(|a| a["request"]["unsigned"]["idempotencyKey"].as_str().unwrap().to_string())
            .collect()
    }

    #[test]
    fn an_unsettled_enable_is_settled_by_the_job_before_the_restore() {
        let (d, ft, _) = testkit::driver();
        testkit::reset();
        setup_root(&d);
        // Every attempt is lost: the enable never settles inside the window.
        ft.set_default("bucket", "Apply", lost());
        let t0 = Instant::now();
        run_window(&d, &window_plan_fast(), &window_probes);
        assert!(t0.elapsed() < Duration::from_secs(3));
        assert!(d.s.signatures_required(), "new tracked writes are held back");
        assert_eq!(d.s.blocking_jobs(), 1);
        std::thread::sleep(Duration::from_millis(400));
        assert!(config_calls(&ft).iter().all(|c| *c), "no restore while the enable may still land: {:?}", config_calls(&ft));
        assert!(config_calls(&ft).len() >= 2, "the job keeps re-sending the enable");
        ft.set_default("bucket", "Apply", ok(json!({})));
        testkit::wait_until(Duration::from_secs(20), || !d.s.signatures_required());
        testkit::wait_until(Duration::from_secs(5), || d.s.blocking_jobs() == 0);
        let cfg = config_calls(&ft);
        let first_false = cfg.iter().position(|c| !c).unwrap();
        assert!(cfg[first_false..].iter().all(|c| !c), "{cfg:?}");
        assert_eq!(enable_keys(&ft).len(), 1, "one key for every enable attempt");
        let a = testkit::assertions();
        let closes: Vec<_> = a.iter().filter(|x| x.message == "the require-signatures window closes").collect();
        assert_eq!(closes.len(), 1);
        assert_eq!(closes[0].details["where"], "job");
    }

    #[test]
    fn a_refused_enable_needs_no_settling() {
        let (d, ft, _) = testkit::driver();
        testkit::reset();
        setup_root(&d);
        let refused = FakeReply::Outcome(Outcome::from_json(&json!({"ok": false, "code": "FailedPrecondition", "reason": "ACCOUNT_TYPE_NOT_FOUND", "message": "x"})));
        assert!(definitive(match &refused { FakeReply::Outcome(o) => o, _ => unreachable!() }));
        ft.script("bucket", "Apply", vec![refused, ok(json!({}))]);
        run_window(&d, &window_plan_fast(), &window_probes);
        assert_eq!(config_calls(&ft), vec![true, false]);
        assert!(!d.s.signatures_required());
        assert!(testkit::fired("the require-signatures window closes"));
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

    // ---- bounds

    #[test]
    fn pruning_keeps_the_model_small_and_the_absent_checks() {
        let mut t = KeyTree::default();
        t.add_root("r", "s", "p");
        // A cascaded subtree is pruned as a whole, leaves first.
        reg(&mut t, "a", "r", true);
        reg(&mut t, "b", "a", true);
        reg(&mut t, "c", "b", true);
        let i = t.begin_revoke("a", true);
        t.end_revoke(i, true);
        let now = t.now();
        let sub = t.cascade_subtree(i, now);
        assert_eq!(sub, s(&["a", "b", "c"]));
        t.prune();
        assert_eq!(t.keys.keys().cloned().collect::<Vec<_>>(), s(&["r"]));
        assert!(t.revokes.is_empty(), "settled revokes on pruned keys are dropped");
        let later = t.now();
        assert_eq!(t.expectations(later), (s(&["r"]), s(&["a", "b", "c"])));
        assert_eq!(t.status("b"), KeyStatus::Revoked);
        assert_eq!(t.any_spec("b").unwrap().key_id, "b");
        assert!(t.spec("b").is_none(), "a pruned key never signs as a usable key");
        assert_eq!(t.revoked_keys(), s(&["a", "b", "c"]));
        // A list started before the pruning does not count them.
        assert!(t.expectations(now).1.is_empty());

        // A revoked key with a live orphan child stays (its chain is needed).
        reg(&mut t, "p", "r", true);
        reg(&mut t, "o", "p", true);
        let i = t.begin_revoke("p", false);
        t.end_revoke(i, true);
        t.prune();
        assert!(t.keys.contains_key("p") && t.keys.contains_key("o"));
        let now = t.now();
        assert!(t.certain_live("o", now));
        // An unknown registration is never pruned; an in-flight revoke on a
        // pruned key is kept until it settles.
        reg(&mut t, "u", "r", false);
        reg(&mut t, "v", "r", true);
        let iv1 = t.begin_revoke("v", false);
        let iv2 = t.begin_revoke("v", false);
        t.end_revoke(iv1, true);
        t.prune();
        assert!(t.keys.contains_key("u") && !t.keys.contains_key("v"));
        assert!(t.revokes.contains_key(&iv2));
        t.end_revoke(iv2, false);
        t.prune();
        assert!(!t.revokes.contains_key(&iv2));

        // Many register/revoke cycles: bounded tree, bounded pruned list.
        for n in 0..500 {
            let id = format!("k{n}");
            reg(&mut t, &id, "r", true);
            let i = t.begin_revoke(&id, n % 2 == 0);
            t.end_revoke(i, true);
            t.prune();
        }
        assert!(t.keys.len() <= 6, "{}", t.keys.len());
        assert!(t.revokes.len() <= 4, "{}", t.revokes.len());
        assert_eq!(t.pruned.len(), MAX_PRUNED);
        assert_eq!(t.revoked_keys().len(), MAX_PRUNED + 1, "pruned ones plus p");
    }

    #[test]
    fn a_repeated_cursor_is_inconclusive() {
        let (d, ft, _) = testkit::driver();
        testkit::reset();
        setup_root(&d);
        ft.script("bucket", "ListSigningKeys", vec![page(&["child-x"], "c1"), page(&["child-y"], "c1")]);
        assert!(list_ids(&d, &Route::plain(0), 1, Instant::now() + Duration::from_secs(2)).is_none());
        exec_keys(&d, KeysOp::List(ListPlan { route: Route::plain(0), page_size: 1 }));
        // The second traversal got the default answer (whatever it is) or a
        // repeated cursor; either way no false "missing" finding on the root.
        ft.script("bucket", "ListSigningKeys", vec![page(&[], "c1"), page(&[], "c1")]);
        testkit::reset();
        exec_keys(&d, KeysOp::List(ListPlan { route: Route::plain(0), page_size: 1 }));
        assert!(!testkit::fired("registered key should appear in ListSigningKeys"));
    }

    #[test]
    fn probes_send_nothing_past_their_deadline_and_cap_timeouts() {
        let (d, ft, _) = testkit::driver();
        testkit::reset();
        setup_root(&d);
        cluster_default(&ft);
        let p = ProbePlan { node_idx: 0, client: 0 };
        let req = json!({});
        // Less than MIN_CALL left: no call at all (not even discovery).
        assert!(pinned_probe(&d, &p, &req, None, Instant::now() + Duration::from_millis(50)).is_none());
        assert!(ft.calls().is_empty(), "{:?}", ft.calls());
        // With 1 s left every call's timeout is capped at what is left.
        ft.script("bucket", "Apply", vec![err("Unauthenticated")]);
        let out = pinned_probe(&d, &p, &req, None, Instant::now() + Duration::from_secs(1)).unwrap();
        assert_eq!(out.code, "Unauthenticated");
        assert!(ft.calls().len() >= 4, "discover, Barrier, horizon poll, Apply");
        for c in ft.calls() {
            assert!(c["timeoutMs"].as_u64().unwrap() <= 1000, "{c}");
        }
        // A discovery that eats the budget leaves no time for the Barrier.
        ft.script("cluster", "GetClusterState", vec![FakeReply::Delayed(Duration::from_millis(400), Outcome::unknown("timeout"))]);
        let before = ft.calls_of("bucket", "Barrier").len();
        let t0 = Instant::now();
        assert!(pinned_probe(&d, &p, &req, None, Instant::now() + Duration::from_millis(450)).is_none());
        assert!(t0.elapsed() < Duration::from_millis(900));
        assert_eq!(ft.calls_of("bucket", "Barrier").len(), before);
    }

    #[test]
    fn untracked_sends_fit_their_total() {
        let s = untracked(vec![], "k".into(), None, &[], &[], ENABLE_BUDGET, ATTEMPT);
        assert_eq!(s.budget + s.attempt_timeout, ENABLE_BUDGET);
        let s = untracked(vec![], "k".into(), None, &[], &[], MGMT_TOTAL, MGMT_ATTEMPT);
        assert_eq!(s.budget + s.attempt_timeout, MGMT_TOTAL);
        let s = untracked(vec![], "k".into(), None, &[], &[], Duration::from_secs(1), ATTEMPT);
        assert_eq!((s.budget, s.attempt_timeout), (Duration::ZERO, Duration::from_secs(1)));
        // Revoke then list stays inside the rule budget.
        assert!(MGMT_TOTAL + LIST_BUDGET <= RULE_BUDGET);
        assert!(PROBE_BUDGET <= RULE_BUDGET);
        assert!(PROBES_UNTIL + ATTEMPT <= EXCL_RESTORE_BUDGET);
    }
}
