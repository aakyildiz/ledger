//! Task 09: query checkpoints — lifecycle writes through the oracle (create,
//! delete live / 0 / next id, non-firing or invalid cron, delete schedule),
//! frozen reads served by a checkpoint on a pinned node, the predicted read
//! that races a create, fenced registry/schedule reads, and write bursts on
//! `r-burst-<run>` right after a create (server-signals §1.1, §1.2).
//!
//! Firing crons are never sent (DESIGN §8): `setQueryCheckpointSchedule`
//! only carries a 100-year interval (`CRONS`, `M/checkpoints.go:19`) or an
//! invalid expression. The oracle validates every answer; Rust asserts only
//! the burst exploration property.
//!
//! All draws happen before slots, tickets and sends, except indices into
//! views returned by `/read/begin` (none are needed: the `checkpoints` view
//! resolves the drawn picks on the frozen snapshot itself).
use crate::clients::{CallOpts, Outcome, Route};
use crate::draw;
use crate::jobs::Job;
use crate::machine::{rule, Driver};
use crate::nodes::{horizon_check, Horizon};
use crate::read::ViewReq;
use crate::setup::SetupHook;
use crate::wire::{str_field, u64_field, Posting};
use crate::write::{UntrackedSend, WritePlan, WriteResult, IN_RULE_BUDGET};
use base64::Engine;
use hegel::stateful::ConcurrentRule;
use hegel::TestCase;
use serde_json::{json, Value};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

const FAMILY: &str = "checkpoints";

pub fn rules() -> Vec<ConcurrentRule<Driver>> {
    vec![
        rule("checkpoint_bootstrap", 1.0, checkpoint_bootstrap),
        rule("checkpoint_write", 1.0, checkpoint_write),
        rule("checkpoint_read", 1.0, checkpoint_read),
        rule("checkpoint_registry", 0.5, checkpoint_registry),
        rule("checkpoint_burst", 0.3, checkpoint_burst),
    ]
}

pub fn setup_hooks() -> Vec<SetupHook> {
    vec![SetupHook { name: "checkpoint_setup", run: checkpoint_setup }]
}

/// The non-firing schedule pool (`M/checkpoints.go:19` modelCheckpointCrons).
pub const CRONS: [&str; 3] = ["@every 876000h", "@every 900000h", "@every 950000h"];
/// The invalid cron of `M/checkpoints.go:39`.
pub const INVALID_CRON: &str = "invalid checkpoint cron";
/// Checkpoint-scoped reads are bounded on a down replica (`M/checkpoint_reads.go:56`).
pub const READ_TIMEOUT: Duration = Duration::from_secs(5);
/// The registry fence gives the pinned node this long to persist the barrier.
pub const FENCE_BUDGET: Duration = Duration::from_secs(5);
/// A GetLog probe this far past a checkpoint's max sequence names a log the
/// frozen store never held (DESIGN §3.2; the oracle accepts ≥ 2^39).
pub const UNASSIGNED_SEQ_SLACK: u64 = 1 << 40;

/// Family state shared across rules.
#[derive(Default)]
pub struct CheckpointExt {
    /// A create was acknowledged (a definitive success): `checkpoint_bootstrap` stops.
    pub created: AtomicBool,
    /// `r-burst-<run>` exists.
    pub burst_ready: AtomicBool,
    /// Bursts completed (for tests and logs).
    pub bursts: AtomicU64,
    /// A burst job is queued or running: `checkpoint_burst` waits for it, so
    /// the job queue never accumulates bursts.
    pub burst_pending: AtomicBool,
}

pub fn burst_ledger(run: &str) -> String {
    format!("r-burst-{run}")
}

// ---------------------------------------------------------------- setup

/// Setup hook (no draws, cheap): verifies the fresh-cluster baseline — an
/// empty registry and no schedule (a reused volume locally only logs) — and
/// creates `r-burst-<run>`. The first checkpoint is created by a rule, so it
/// overlaps faults (review-antithesis-fit A10).
pub fn checkpoint_setup(d: &Driver) {
    let opts = CallOpts::linearizable().timeout(Duration::from_secs(3));
    for (method, field) in [("ListQueryCheckpoints", "checkpoints"), ("GetQueryCheckpointSchedule", "cron")] {
        let mut seen = None;
        for c in 0..3 {
            let (_, out) = d.call(&Route::lib(c % d.s.clients.len().max(1)), "cluster", method, json!({}), &opts);
            if out.is_ok() {
                seen = out.response;
                break;
            }
        }
        match seen {
            None => println!("setup: checkpoint_setup: {method} did not answer; baseline unverified"),
            Some(v) => {
                let empty = match v.get(field) {
                    None | Some(Value::Null) => true,
                    Some(Value::Array(a)) => a.is_empty(),
                    Some(Value::String(s)) => s.is_empty(),
                    Some(_) => false,
                };
                if !empty {
                    println!("setup: checkpoint_setup: {method} is not empty on a fresh cluster (reused volume?): {v}");
                }
            }
        }
    }
    ensure_burst_ledger(d, 10);
}

/// Creates `r-burst-<run>` (keyed, retried), up to `tries` rounds.
fn ensure_burst_ledger(d: &Driver, tries: usize) {
    let ext = d.s.ext::<CheckpointExt>();
    if ext.burst_ready.load(Ordering::SeqCst) {
        return;
    }
    let key = d.s.next_untracked_key();
    for i in 0..tries {
        let mut send = UntrackedSend::new(vec![json!({"createLedger": {"name": burst_ledger(&d.s.run)}})], Some(key.clone()));
        send.schedule = (0..d.s.clients.len().max(1)).map(Route::lib).collect();
        let r = d.send_untracked(send);
        if r.outcome.is_ok() || r.outcome.reason == "LEDGER_ALREADY_EXISTS" {
            ext.burst_ready.store(true, Ordering::SeqCst);
            return;
        }
        println!("driver: checkpoints: creating {}: {}", burst_ledger(&d.s.run), r.outcome.summary());
        if i + 1 < tries {
            std::thread::sleep(Duration::from_millis(500));
        }
    }
}

// ---------------------------------------------------------------- lifecycle

/// One lifecycle order (`M/checkpoints.go:30-56` generateCheckpointBulk slots).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Lifecycle {
    SetSchedule(&'static str),
    DeleteSchedule,
    InvalidCron,
    DeleteZero,
    DeleteNext(String),
    DeleteLive(String),
    Create,
}

/// Weights of the lifecycle slots, in `Lifecycle` order: set schedule 1,
/// delete schedule 1, invalid cron 1, delete id 0 1, delete next id 1, delete
/// a live id 2 (×3 once the registry holds `limit − 1`, and then the oldest),
/// create 3.
pub fn lifecycle_weights(live: usize, limit: u64) -> [f64; 7] {
    let del_live = if live == 0 {
        0.0
    } else if near_limit(live, limit) {
        6.0
    } else {
        2.0
    };
    [1.0, 1.0, 1.0, 1.0, 1.0, del_live, 3.0]
}

fn near_limit(live: usize, limit: u64) -> bool {
    limit > 0 && live as u64 + 1 >= limit
}

/// The `servicepb.Request` of one lifecycle order.
pub fn lifecycle_request(l: &Lifecycle) -> Value {
    match l {
        Lifecycle::SetSchedule(c) => json!({"setQueryCheckpointSchedule": {"cron": c}}),
        Lifecycle::DeleteSchedule => json!({"deleteQueryCheckpointSchedule": {}}),
        Lifecycle::InvalidCron => json!({"setQueryCheckpointSchedule": {"cron": INVALID_CRON}}),
        Lifecycle::DeleteZero => json!({"deleteQueryCheckpoint": {}}),
        Lifecycle::DeleteNext(id) | Lifecycle::DeleteLive(id) => json!({"deleteQueryCheckpoint": {"checkpointId": id}}),
        Lifecycle::Create => json!({"createQueryCheckpoint": {}}),
    }
}

fn str_list(v: &Value, key: &str) -> Vec<String> {
    v.get(key).and_then(Value::as_array).map(|a| a.iter().filter_map(Value::as_str).map(str::to_string).collect()).unwrap_or_default()
}

/// Draws a lifecycle order against the `checkpoints` view.
pub fn draw_lifecycle(tc: &TestCase, view: &Value) -> Lifecycle {
    let live = str_list(view, "live");
    let limit = view.get("limit").and_then(Value::as_u64).unwrap_or(0);
    let next = view.get("nextId").and_then(Value::as_str).unwrap_or("1").to_string();
    match draw::weighted(tc, &lifecycle_weights(live.len(), limit)).unwrap_or(6) {
        0 => Lifecycle::SetSchedule(*draw::one_of(tc, &CRONS)),
        1 => Lifecycle::DeleteSchedule,
        2 => Lifecycle::InvalidCron,
        3 => Lifecycle::DeleteZero,
        4 => Lifecycle::DeleteNext(next),
        5 if near_limit(live.len(), limit) => Lifecycle::DeleteLive(live[0].clone()),
        5 => Lifecycle::DeleteLive(draw::one_of(tc, &live).clone()),
        _ => Lifecycle::Create,
    }
}

/// What a create's predicted read is aimed at (drawn before the slot).
pub struct PredictedAim {
    pub client: usize,
    pub node: String,
}

/// Creates one checkpoint through the write protocol. With `aim`, the
/// `/write/begin` answer's `predicted.checkpointId` drives a
/// `PredictedCheckpointRead` racing the create's attempts. `None`: no slot.
pub fn create_checkpoint(d: &Driver, plan: WritePlan, aim: Option<PredictedAim>) -> Option<WriteResult> {
    let unit = d.s.slots.try_acquire_up_to(1)?.take_one()?;
    let ticket = match d.begin_write(plan, unit) {
        Ok(t) => t,
        // `serialized` while an earlier create is undrained: the rule ends.
        Err(e) => return Some(WriteResult::Refused(e)),
    };
    let id = ticket.predicted().get("checkpointId").and_then(Value::as_str).map(str::to_string);
    let res = match (aim, id) {
        (Some(aim), Some(id)) => match d.read_begin("PredictedCheckpointRead", &[ViewReq::ledgers()], json!({"id": id})) {
            Some(rt) => match rt.views().first().and_then(|v| v.get("live")).and_then(Value::as_array).and_then(|l| l.first()).and_then(Value::as_str).map(str::to_string) {
                Some(ledger) => {
                    let route = Route::node(aim.client, &aim.node);
                    let req = json!({"ledger": ledger, "read": {"checkpointId": id}});
                    let opts = CallOpts::linearizable().timeout(READ_TIMEOUT);
                    let (res, out) = std::thread::scope(|s| {
                        let h = crate::assertions::scoped_inheriting(s, move || ticket.run(IN_RULE_BUDGET));
                        let (_, out) = d.call(&route, "bucket", "GetLedger", req.clone(), &opts);
                        (h.join().unwrap_or_else(|e| std::panic::resume_unwind(e)), out)
                    });
                    rt.end(&req, &out, &route, &opts, json!({}), json!({}));
                    res
                }
                None => {
                    rt.abandon();
                    ticket.run(IN_RULE_BUDGET)
                }
            },
            None => ticket.run(IN_RULE_BUDGET),
        },
        _ => ticket.run(IN_RULE_BUDGET),
    };
    if res.is_success() {
        d.s.ext::<CheckpointExt>().created.store(true, Ordering::SeqCst);
    }
    Some(res)
}

fn create_plan(d: &Driver, tc: &TestCase, gen: &str) -> WritePlan {
    let mut plan = WritePlan::drawn(FAMILY, vec![lifecycle_request(&Lifecycle::Create)], d, tc);
    plan.hint = json!({"gen": gen});
    plan
}

fn draw_aim(d: &Driver, tc: &TestCase) -> Option<PredictedAim> {
    if d.s.nodes.is_empty() {
        return None;
    }
    let client = draw::pick(tc, d.s.clients.len().max(1));
    let node = draw::one_of(tc, &d.s.nodes).clone();
    Some(PredictedAim { client, node })
}

/// `checkpoint_bootstrap` (1.0): until a create was acknowledged, creates a
/// probe checkpoint with probability 1/2 (the oracle's commit check
/// validates its id).
pub fn checkpoint_bootstrap(d: &Driver, tc: TestCase) {
    d.guarded("checkpoint_bootstrap", &tc, |d, tc| {
        if !d.s.oracle_ok() || d.s.ext::<CheckpointExt>().created.load(Ordering::SeqCst) {
            return None;
        }
        if !draw::chance(tc, 0.5) {
            return None;
        }
        let plan = create_plan(d, tc, "bootstrap");
        create_checkpoint(d, plan, None)?;
        Some(())
    });
}

/// `checkpoint_write` (1.0): one singleton lifecycle bulk; a create races a
/// predicted read.
pub fn checkpoint_write(d: &Driver, tc: TestCase) {
    d.guarded("checkpoint_write", &tc, |d, tc| {
        if !d.s.oracle_ok() {
            return None;
        }
        let view = d.view(&[ViewReq::named("checkpoints", json!({}))])?.into_iter().next()?;
        let order = draw_lifecycle(tc, &view);
        if order == Lifecycle::Create {
            let aim = draw_aim(d, tc);
            let plan = create_plan(d, tc, "create");
            create_checkpoint(d, plan, aim)?;
            return Some(());
        }
        let mut plan = WritePlan::drawn(FAMILY, vec![lifecycle_request(&order)], d, tc);
        plan.hint = json!({"gen": format!("{order:?}")});
        let unit = d.s.slots.try_acquire_up_to(1)?.take_one()?;
        let _ = d.dispatch(plan, unit);
        Some(())
    });
}

// ---------------------------------------------------------------- frozen reads

/// The arms of read kind `CheckpointRead` (`M/checkpoint_reads.go` draws 2–9).
pub const ARMS: [&str; 8] = ["GetAccount", "GetTransaction", "ListAccounts", "ListTransactions", "AggregateVolumes", "GetLedgerStats", "GetLog", "ListLedgers"];

const GROUP_PREFIXES: [&[&str]; 4] = [&["t-"], &["users:"], &["t-", "users:"], &["world", "t-1"]];

/// A forward page token (`pagecursor.Cursor{Key}.Encode()`).
pub fn page_token(key: &str) -> String {
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(json!({ "key": key }).to_string())
}

/// Every draw of one `CheckpointRead`, made before `/read/begin`.
#[derive(Clone, Debug)]
pub struct ReadPlan {
    pub arm: &'static str,
    pub pick: u32,
    pub ledger_pick: u32,
    pub log_pick: u32,
    pub probe: String,
    pub unassigned: bool,
    pub collapse: bool,
    pub max_precision: bool,
    pub prefixes: Vec<&'static str>,
    pub page_size: u64,
    pub reverse: bool,
    /// ListAccounts: an address; ListTransactions: a decimal id; ListLedgers:
    /// "edge" (the tracked prefix's edge) or "ledger" (the target ledger).
    pub cursor: Option<String>,
    pub route: Route,
}

pub fn draw_read_plan(d: &Driver, tc: &TestCase) -> ReadPlan {
    let arm = *draw::one_of(tc, &ARMS);
    let client = draw::pick(tc, d.s.clients.len().max(1));
    let route = if d.s.nodes.is_empty() { Route::lib(client) } else { Route::node(client, draw::one_of(tc, &d.s.nodes)) };
    let pick = draw::ledger_pick(tc);
    let mut p = ReadPlan {
        arm,
        pick,
        ledger_pick: 0,
        log_pick: 0,
        probe: String::new(),
        unassigned: false,
        collapse: false,
        max_precision: false,
        prefixes: vec![],
        page_size: 0,
        reverse: false,
        cursor: None,
        route,
    };
    if arm != "GetLog" && arm != "ListLedgers" {
        p.ledger_pick = draw::ledger_pick(tc);
    }
    match arm {
        "GetAccount" => {
            p.probe = draw::address(tc);
            p.collapse = draw::chance(tc, 1.0 / 3.0);
        }
        "GetTransaction" => p.probe = draw::int_in(tc, 1, 300).to_string(),
        "AggregateVolumes" => {
            p.collapse = draw::chance(tc, 1.0 / 3.0);
            p.max_precision = draw::chance(tc, 0.25);
            if draw::chance(tc, 1.0 / 3.0) {
                p.prefixes = draw::one_of(tc, &GROUP_PREFIXES).to_vec();
            }
        }
        "GetLog" => {
            p.log_pick = draw::ledger_pick(tc);
            p.unassigned = draw::chance(tc, 1.0 / 8.0);
        }
        "ListAccounts" | "ListTransactions" | "ListLedgers" => {
            p.page_size = draw::int_in(tc, 1, 16);
            p.reverse = draw::chance(tc, 0.5);
            if draw::chance(tc, 0.5) {
                p.cursor = Some(match arm {
                    "ListAccounts" => draw::pool_address(tc),
                    "ListTransactions" => draw::int_in(tc, 1, 256).to_string(),
                    _ => (if draw::chance(tc, 0.5) { "edge" } else { "ledger" }).to_string(),
                });
            }
        }
        _ => {}
    }
    p
}

/// The `checkpoints` view request of a read plan.
pub fn read_view(p: &ReadPlan) -> ViewReq {
    let mut params = json!({"pick": p.pick, "cpLedgerPick": p.ledger_pick, "logPick": p.log_pick});
    match p.arm {
        "GetAccount" => {
            params["table"] = json!("volumes");
            params["probe"] = json!(p.probe);
        }
        "GetTransaction" => {
            params["table"] = json!("txs");
            params["probe"] = json!(p.probe);
        }
        _ => {}
    }
    ViewReq::named("checkpoints", params)
}

/// The RPC (method, request) of a read plan against the view's target, or
/// `None` when the target cannot serve this arm (no checkpoint, no ledger).
pub fn read_request(p: &ReadPlan, target: &Value, run: &str) -> Option<(&'static str, Value)> {
    let id = target.get("id").and_then(Value::as_str)?.to_string();
    let ledger = target.get("ledger").and_then(Value::as_str).map(str::to_string);
    let seek_entry = target.get("seek").filter(|s| s["found"] == json!(true)).and_then(|s| s.get("entry"));
    let list_options = |cursor: Option<String>| {
        let mut o = json!({"read": {"checkpointId": id}, "pageSize": p.page_size, "reverse": p.reverse});
        if let Some(c) = cursor {
            o["cursor"] = json!(page_token(&c));
        }
        o
    };
    Some(match p.arm {
        "GetAccount" => {
            let address = seek_entry.and_then(|e| e.get("address")).and_then(Value::as_str).unwrap_or(&p.probe);
            ("GetAccount", json!({"ledger": ledger?, "address": address, "checkpointId": id, "collapseColors": p.collapse}))
        }
        "GetTransaction" => {
            let tx = seek_entry.map(|e| u64_field(e, "id")).filter(|&n| n > 0).unwrap_or_else(|| p.probe.parse().unwrap_or(1));
            ("GetTransaction", json!({"ledger": ledger?, "transactionId": tx.to_string(), "checkpointId": id}))
        }
        "ListAccounts" => ("ListAccounts", json!({"ledger": ledger?, "options": list_options(p.cursor.clone())})),
        "ListTransactions" => ("ListTransactions", json!({"ledger": ledger?, "options": list_options(p.cursor.clone())})),
        "AggregateVolumes" => ("AggregateVolumes", json!({"ledger": ledger?, "checkpointId": id, "collapseColors": p.collapse, "useMaxPrecision": p.max_precision, "groupByPrefixes": p.prefixes})),
        "GetLedgerStats" => ("GetLedgerStats", json!({"ledger": ledger?, "checkpointId": id})),
        "GetLog" => {
            let max = u64_field(target, "maxSequence");
            let learned = target.get("logSeq").and_then(Value::as_str).and_then(|s| s.parse::<u64>().ok());
            let seq = match learned {
                Some(s) if !p.unassigned => s,
                _ => max.saturating_add(UNASSIGNED_SEQ_SLACK),
            };
            ("GetLog", json!({"sequence": seq.to_string(), "checkpointId": id}))
        }
        "ListLedgers" => {
            let cursor = match p.cursor.as_deref() {
                Some("edge") if p.reverse => Some(format!("o-{run}.")),
                Some("edge") => Some(format!("o-{run}-")),
                Some(_) => ledger.clone().or_else(|| Some(format!("o-{run}-"))),
                None => None,
            };
            ("ListLedgers", json!({"options": list_options(cursor)}))
        }
        _ => return None,
    })
}

/// `checkpoint_read` (1.0): one checkpoint-scoped read on a pinned node, 5 s
/// timeout, against a live or retained-deleted checkpoint (read kind
/// `CheckpointRead`; the arm travels in the read params).
pub fn checkpoint_read(d: &Driver, tc: TestCase) {
    d.guarded("checkpoint_read", &tc, |d, tc| {
        if !d.s.oracle_ok() {
            return None;
        }
        let p = draw_read_plan(d, tc);
        let rt = d.read_begin("CheckpointRead", &[read_view(&p)], json!({}))?;
        let target = rt.views().first().and_then(|v| v.get("target")).cloned().unwrap_or(Value::Null);
        let Some((method, req)) = read_request(&p, &target, &d.s.run) else {
            rt.abandon();
            return None;
        };
        let mut opts = CallOpts::linearizable().timeout(READ_TIMEOUT);
        if method.starts_with("List") {
            opts = opts.max_items(1001);
        }
        let (_, out) = d.call(&p.route, "bucket", method, req.clone(), &opts);
        let id = str_field(&target, "id").to_string();
        rt.end(&req, &out, &p.route, &opts, json!({"deleted": target["deleted"]}), json!({"arm": p.arm, "id": id}));
        Some(())
    });
}

// ---------------------------------------------------------------- fenced registry reads

/// The driver-run fence of `M/checkpoint_metadata.go:67-103`.
#[derive(Clone, Debug, PartialEq)]
pub struct Fence {
    pub ready: bool,
    pub why: String,
    pub node_id: u64,
    pub barrier: u64,
    pub persisted: u64,
    pub polls: u64,
}

impl Fence {
    fn failed(why: &str, node_id: u64, barrier: u64, polls: u64) -> Fence {
        Fence { ready: false, why: why.to_string(), node_id, barrier, persisted: 0, polls }
    }
    pub fn to_json(&self) -> Value {
        json!({"ready": self.ready, "why": self.why, "nodeId": self.node_id.to_string(), "barrier": self.barrier.to_string(), "persisted": self.persisted.to_string(), "polls": self.polls})
    }
}

/// `GetClusterState{}` on the pinned node → its id (the node must say it is
/// the `Leader`; the id comes from the topology's `serviceAddress`, else
/// from `localNode` of the direct answer), `Barrier` → commit index, then
/// poll `GetClusterState{nodeId}` on that node every `poll_ms` until it is
/// itself, in normal sync and persisted at or past the barrier, within `budget`.
pub fn run_fence(d: &Driver, client: usize, node: &str, poll_ms: u64, budget: Duration) -> Fence {
    let deadline = Instant::now() + budget;
    let route = Route::node(client, node);
    let short = CallOpts::linearizable().timeout(Duration::from_secs(2));
    let (_, topo) = d.call(&route, "cluster", "GetClusterState", json!({}), &short);
    let Some(cs) = topo.response.as_ref().filter(|_| topo.is_ok()) else {
        return Fence::failed("topology unavailable", 0, 0, 0);
    };
    if str_field(cs, "state") != "Leader" {
        return Fence::failed("the pinned node is not the leader", 0, 0, 0);
    }
    let by_addr = cs.get("nodes").and_then(Value::as_array).and_then(|ns| ns.iter().find(|n| str_field(n, "serviceAddress") == node)).map(|n| u64_field(n, "id"));
    let node_id = by_addr.filter(|&id| id != 0).unwrap_or_else(|| u64_field(cs, "localNode"));
    if node_id == 0 {
        return Fence::failed("unresolved node identity", 0, 0, 0);
    }
    let (_, b) = d.call(&route, "bucket", "Barrier", json!({}), &short);
    let barrier = b.response.as_ref().filter(|_| b.is_ok()).map(|r| u64_field(r, "commitIndex")).unwrap_or(0);
    if barrier == 0 {
        return Fence::failed("barrier unavailable", node_id, 0, 0);
    }
    let mut polls = 0;
    loop {
        polls += 1;
        if let Some(cs) = d.cluster_state_via(client, node, node_id) {
            if let Horizon::Reached(p) = horizon_check(&cs, node_id, barrier, false) {
                return Fence { ready: true, why: String::new(), node_id, barrier, persisted: p, polls };
            }
        }
        let next = Instant::now() + Duration::from_millis(poll_ms);
        if next >= deadline {
            return Fence::failed("the node did not persist the barrier in time", node_id, barrier, polls);
        }
        std::thread::sleep(Duration::from_millis(poll_ms));
    }
}

/// The transient outcome a failed fence ends its read ticket with.
pub fn fence_outcome(f: &Fence) -> Outcome {
    Outcome { ok: false, code: "Unavailable".into(), local: true, message: format!("checkpoint metadata fence: {}", f.why), ..Default::default() }
}

/// `checkpoint_registry` (0.5): read kinds `CheckpointRegistry` /
/// `CheckpointSchedule`, one read ticket over the fence and the metadata
/// read on the same pinned node.
pub fn checkpoint_registry(d: &Driver, tc: TestCase) {
    d.guarded("checkpoint_registry", &tc, |d, tc| {
        if !d.s.oracle_ok() || d.s.nodes.is_empty() {
            return None;
        }
        let schedule = draw::chance(tc, 0.5);
        let client = draw::pick(tc, d.s.clients.len().max(1));
        let node = draw::one_of(tc, &d.s.nodes).clone();
        let poll_ms = draw::int_in(tc, 20, 200);
        let (kind, method) = if schedule { ("CheckpointSchedule", "GetQueryCheckpointSchedule") } else { ("CheckpointRegistry", "ListQueryCheckpoints") };
        let rt = d.read_begin(kind, &[], json!({}))?;
        let fence = run_fence(d, client, &node, poll_ms, FENCE_BUDGET);
        let route = Route::node(client, &node);
        let opts = CallOpts::linearizable().timeout(READ_TIMEOUT);
        let req = json!({});
        let out = if fence.ready { d.call(&route, "cluster", method, req.clone(), &opts).1 } else { fence_outcome(&fence) };
        rt.end(&req, &out, &route, &opts, json!({"fence": fence.to_json()}), json!({}));
        Some(())
    });
}

// ---------------------------------------------------------------- bursts

/// One small forced write of a burst on `r-burst-<run>`.
pub fn burst_body(run: &str, user: u64, amount: u64) -> Value {
    let mut r = crate::wire::create_tx_request(&burst_ledger(run), &[Posting::new("world", &format!("users:{user}"), amount, "COIN")], None);
    r["apply"]["action"]["createTransaction"]["force"] = json!(true);
    r
}

/// `checkpoint_burst` (0.3): draws a burst of 50–200 small forced writes
/// (bodies and routes), creates a checkpoint (no predicted read), then a
/// non-blocking job sends the burst as unkeyed single-attempt untracked
/// writes from all clients at once, so the burst lands while followers may
/// still be in the checkpoint's gated window (server-signals §1.1, §1.2).
pub fn checkpoint_burst(d: &Driver, tc: TestCase) {
    d.guarded("checkpoint_burst", &tc, |d, tc| {
        if !d.s.oracle_ok() || d.s.ext::<CheckpointExt>().burst_pending.load(Ordering::SeqCst) {
            return None;
        }
        let n = draw::int_in(tc, 50, 200) as usize;
        let writes: Vec<(Route, Value)> = (0..n).map(|_| (draw::route(tc, &d.s), burst_body(&d.s.run, draw::int_in(tc, 0, 31), draw::int_in(tc, 1, 1000)))).collect();
        let plan = create_plan(d, tc, "burst");
        if let WriteResult::Refused(_) = create_checkpoint(d, plan, None)? {
            return None;
        }
        submit_burst(d, writes);
        Some(())
    });
}

struct BurstPending(std::sync::Arc<CheckpointExt>);

impl Drop for BurstPending {
    fn drop(&mut self) {
        self.0.burst_pending.store(false, Ordering::SeqCst);
    }
}

/// Submits the burst job (no draws inside).
pub fn submit_burst(d: &Driver, writes: Vec<(Route, Value)>) {
    d.s.ext::<CheckpointExt>().burst_pending.store(true, Ordering::SeqCst);
    d.submit_job(Job {
        name: "checkpoint-burst",
        blocks_quiet: false,
        run: Box::new(move |d: &Driver| {
            // Cleared on every exit, a panic included (the runner catches it).
            let _pending = BurstPending(d.s.ext::<CheckpointExt>());
            ensure_burst_ledger(d, 2);
            let clients = d.s.clients.len().max(1);
            let mut by_client: Vec<Vec<(Route, Value)>> = vec![Vec::new(); clients];
            for (r, b) in writes {
                by_client[r.client % clients].push((r, b));
            }
            let total: usize = by_client.iter().map(Vec::len).sum();
            let ok: usize = std::thread::scope(|s| {
                let hs: Vec<_> = by_client
                    .into_iter()
                    .map(|group| {
                        crate::assertions::scoped_inheriting(s, move || {
                            group
                                .into_iter()
                                .filter(|(route, body)| {
                                    let mut send = UntrackedSend::new(vec![body.clone()], None);
                                    send.schedule = vec![route.clone()];
                                    d.send_untracked(send).outcome.is_ok()
                                })
                                .count()
                        })
                    })
                    .collect();
                hs.into_iter().map(|h| h.join().unwrap_or(0)).sum()
            });
            d.s.ext::<CheckpointExt>().bursts.fetch_add(1, Ordering::SeqCst);
            check_sometimes!(ok > 0, "a write burst follows a query checkpoint create", json!({"writes": total, "ok": ok}));
        }),
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testkit::{self, FakeReply};

    fn ok(v: Value) -> FakeReply {
        FakeReply::Outcome(Outcome::from_json(&json!({"ok": true, "response": v})))
    }

    fn cs(local: u64, state: &str, persisted: u64, nodes: Value) -> Value {
        json!({"state": state, "localNode": local.to_string(), "leader": "1", "nodes": nodes, "syncProgress": {"status": "normal"}, "raftStatus": {"lastPersistedIndex": persisted.to_string()}})
    }

    #[test]
    fn lifecycle_requests_never_name_reserved_or_firing_entities() {
        for l in [Lifecycle::SetSchedule(CRONS[0]), Lifecycle::DeleteSchedule, Lifecycle::InvalidCron, Lifecycle::DeleteZero, Lifecycle::DeleteNext("4".into()), Lifecycle::DeleteLive("2".into()), Lifecycle::Create] {
            let r = lifecycle_request(&l).to_string();
            assert!(!r.contains("own"), "{r}");
        }
        assert_eq!(lifecycle_request(&Lifecycle::DeleteZero), json!({"deleteQueryCheckpoint": {}}));
        assert_eq!(lifecycle_request(&Lifecycle::DeleteLive("7".into())), json!({"deleteQueryCheckpoint": {"checkpointId": "7"}}));
        // Every cron the driver sends either never fires within a run or is invalid.
        for c in CRONS {
            let hours: u64 = c.trim_start_matches("@every ").trim_end_matches('h').parse().unwrap();
            assert!(hours >= 876000, "{c}");
        }
        testkit::with_tc_n(100, |tc| {
            let b = burst_body("x", draw::int_in(tc, 0, 31), draw::int_in(tc, 1, 1000));
            assert_eq!(b["apply"]["ledger"], "r-burst-x");
            assert!(!b.to_string().contains("own-"));
            let view = json!({"live": ["1", "2", "3"], "limit": 10, "nextId": "4"});
            let l = draw_lifecycle(tc, &view);
            assert!(!lifecycle_request(&l).to_string().contains("own"));
            if let Lifecycle::DeleteNext(id) = &l {
                assert_eq!(id, "4");
            }
        });
    }

    #[test]
    fn lifecycle_weights_prefer_deleting_near_the_limit() {
        assert_eq!(lifecycle_weights(0, 10)[5], 0.0);
        assert_eq!(lifecycle_weights(3, 10)[5], 2.0);
        assert_eq!(lifecycle_weights(9, 10)[5], 6.0);
        assert_eq!(lifecycle_weights(10, 10)[5], 6.0);
        assert_eq!(lifecycle_weights(3, 10)[6], 3.0);
        testkit::with_tc_n(100, |tc| {
            let view = json!({"live": ["3", "5", "6", "7", "8", "9", "10", "11", "12"], "limit": 10, "nextId": "13"});
            if let Lifecycle::DeleteLive(id) = draw_lifecycle(tc, &view) {
                assert_eq!(id, "3", "near the limit the oldest live id goes");
            }
        });
    }

    #[test]
    fn page_token_matches_go_pagecursor() {
        assert_eq!(page_token("o-x-l1"), "eyJrZXkiOiJvLXgtbDEifQ");
    }

    #[test]
    fn fence_ready_path() {
        let (d, ft, _) = testkit::driver();
        let nodes = json!([{"id": "1", "serviceAddress": "ledger-0:8888"}, {"id": "2", "serviceAddress": "ledger-1:8888"}]);
        ft.script("cluster", "GetClusterState", vec![ok(cs(2, "Leader", 5, nodes)), ok(cs(2, "Follower", 9, json!([]))), ok(cs(2, "Follower", 12, json!([])))]);
        ft.script("bucket", "Barrier", vec![ok(json!({"commitIndex": "10"}))]);
        let f = run_fence(&d, 1, "ledger-1:8888", 20, Duration::from_secs(5));
        assert!(f.ready, "{f:?}");
        assert_eq!((f.node_id, f.barrier, f.persisted, f.polls), (2, 10, 12, 2));
        let calls = ft.calls_of("cluster", "GetClusterState");
        assert_eq!(calls[1]["request"], json!({"nodeId": "2"}));
        assert_eq!(calls[1]["node"], "ledger-1:8888");
    }

    #[test]
    fn fence_non_leader_is_transient() {
        let (d, ft, _) = testkit::driver();
        ft.script("cluster", "GetClusterState", vec![ok(cs(2, "Follower", 50, json!([])))]);
        let f = run_fence(&d, 0, "ledger-1:8888", 20, Duration::from_secs(5));
        assert!(!f.ready);
        assert!(ft.calls_of("bucket", "Barrier").is_empty(), "no barrier on a non-leader");
        let o = fence_outcome(&f);
        assert_eq!(o.code, "Unavailable");
        assert!(!o.ok);
    }

    #[test]
    fn fence_below_barrier_keeps_polling_then_gives_up() {
        let (d, ft, _) = testkit::driver();
        let nodes = json!([{"id": "1", "serviceAddress": "ledger-0:8888"}]);
        ft.script("cluster", "GetClusterState", vec![ok(cs(1, "Leader", 5, nodes))]);
        ft.set_default("cluster", "GetClusterState", ok(cs(1, "Leader", 9, json!([]))));
        ft.script("bucket", "Barrier", vec![ok(json!({"commitIndex": "10"}))]);
        let started = Instant::now();
        let f = run_fence(&d, 0, "ledger-0:8888", 20, Duration::from_millis(400));
        assert!(!f.ready);
        assert!(started.elapsed() >= Duration::from_millis(350), "polls until the budget");
        assert!(f.polls >= 5, "{f:?}");
        assert_eq!(f.barrier, 10);
    }

    #[test]
    fn fence_falls_back_to_local_node_identity() {
        let (d, ft, _) = testkit::driver();
        ft.script("cluster", "GetClusterState", vec![ok(cs(1, "Leader", 5, json!([{"id": "1", "serviceAddress": "127.0.0.1:9"}]))), ok(cs(1, "Leader", 10, json!([])))]);
        ft.script("bucket", "Barrier", vec![ok(json!({"commitIndex": "10"}))]);
        let f = run_fence(&d, 0, "localhost:9", 20, Duration::from_secs(2));
        assert!(f.ready, "{f:?}");
        assert_eq!(f.node_id, 1);
    }

    #[test]
    fn registry_rule_ends_the_ticket_with_fence_record() {
        let (d, ft, fo) = testkit::driver();
        ft.set_default("cluster", "GetClusterState", ok(cs(1, "Follower", 0, json!([]))));
        testkit::run_rule(&d, checkpoint_registry);
        let ends = fo.calls("/read/end");
        assert_eq!(ends.len(), 1);
        assert!(ends[0]["kind"] == "CheckpointRegistry" || ends[0]["kind"] == "CheckpointSchedule");
        assert_eq!(ends[0]["outcome"]["code"], "Unavailable");
        assert_eq!(ends[0]["hint"]["fence"]["ready"], false);
        assert_eq!(ends[0]["route"]["route"], "node");
        assert!(d.s.registry.snapshot().is_empty());
    }

    #[test]
    fn create_races_a_predicted_read_on_the_predicted_id() {
        let (d, ft, fo) = testkit::driver();
        fo.set_live(vec!["o-test-l1".into(), "o-test-l2".into()]);
        fo.set_predicted(json!({"ok": true, "reason": "", "checkpointId": "7"}));
        testkit::with_tc_n(1, |tc| {
            let plan = create_plan(&d, tc, "test");
            let aim = Some(PredictedAim { client: 1, node: "ledger-2:8888".into() });
            let r = create_checkpoint(&d, plan, aim).expect("a slot");
            assert!(r.is_success(), "{r:?}");
        });
        let begins = fo.calls("/read/begin");
        assert_eq!(begins[0]["kind"], "PredictedCheckpointRead");
        assert_eq!(begins[0]["params"], json!({"id": "7"}));
        let ends = fo.calls("/read/end");
        assert_eq!(ends[0]["request"], json!({"ledger": "o-test-l1", "read": {"checkpointId": "7"}}));
        assert_eq!(ends[0]["route"]["node"], "ledger-2:8888");
        let gl = ft.calls_of("bucket", "GetLedger");
        assert_eq!(gl.len(), 1);
        let applies = ft.calls_of("bucket", "Apply");
        assert_eq!(applies[0]["request"]["unsigned"]["requests"][0], json!({"createQueryCheckpoint": {}}));
        assert!(d.s.ext::<CheckpointExt>().created.load(Ordering::SeqCst));
        assert!(d.s.registry.snapshot().is_empty());
    }

    #[test]
    fn a_serialized_create_ends_without_sending() {
        let (d, ft, fo) = testkit::driver();
        fo.refuse_next_begin(409, "serialized");
        testkit::with_tc_n(1, |tc| {
            let plan = create_plan(&d, tc, "test");
            let r = create_checkpoint(&d, plan, Some(PredictedAim { client: 0, node: "ledger-0:8888".into() })).expect("a slot");
            assert!(matches!(r, WriteResult::Refused(_)));
        });
        assert!(ft.calls_of("bucket", "Apply").is_empty());
        assert!(fo.calls("/read/begin").is_empty());
        assert!(!d.s.ext::<CheckpointExt>().created.load(Ordering::SeqCst));
        assert_eq!(d.s.slots.in_use().0, 0);
    }

    #[test]
    fn read_requests_carry_the_checkpoint_and_aim_at_the_frozen_target() {
        let (d, _, _) = testkit::driver();
        let target = json!({"id": "3", "deleted": true, "maxSequence": "40", "ledger": "o-test-l1", "logSeq": "12",
            "seek": {"found": true, "wrapped": false, "entry": {"address": "t-0:1", "id": "5"}}});
        testkit::with_tc_n(200, |tc| {
            let p = draw_read_plan(&d, tc);
            let (method, req) = read_request(&p, &target, "test").expect("a request");
            assert_eq!(method, p.arm);
            let s = req.to_string();
            assert!(s.contains("\"checkpointId\":\"3\""), "{s}");
            match p.arm {
                "GetAccount" => assert_eq!(req["address"], "t-0:1"),
                "GetTransaction" => assert_eq!(req["transactionId"], "5"),
                "GetLog" if !p.unassigned => assert_eq!(req["sequence"], "12"),
                "GetLog" => assert_eq!(req["sequence"], (40u64 + UNASSIGNED_SEQ_SLACK).to_string()),
                "ListLedgers" => assert!(req.get("ledger").is_none()),
                _ => assert_eq!(req["ledger"], "o-test-l1"),
            }
            if !d.s.nodes.is_empty() {
                assert!(matches!(p.route.mode, crate::clients::RouteMode::Node(_)), "pinned");
            }
        });
        // No ledger live in the frozen state: ledger arms cannot be served.
        let bare = json!({"id": "3", "ledger": null, "maxSequence": "40"});
        testkit::with_tc_n(50, |tc| {
            let p = draw_read_plan(&d, tc);
            let r = read_request(&p, &bare, "test");
            assert_eq!(r.is_some(), p.arm == "GetLog" || p.arm == "ListLedgers", "{}", p.arm);
        });
        assert!(read_request(&draw_plan_once(&d), &Value::Null, "test").is_none());
    }

    fn draw_plan_once(d: &Driver) -> ReadPlan {
        let out = std::sync::Mutex::new(None);
        testkit::with_tc_n(1, |tc| *out.lock().unwrap() = Some(draw_read_plan(d, tc)));
        let p = out.into_inner().unwrap().unwrap();
        p
    }

    #[test]
    fn checkpoint_read_rule_posts_arm_and_id() {
        let (d, _, fo) = testkit::driver();
        fo.set_view("checkpoints", json!({"live": ["3"], "deleted": [], "target": {"id": "3", "deleted": false, "maxSequence": "40", "ledger": "o-test-l1", "logSeq": "12"}}));
        testkit::run_rule(&d, checkpoint_read);
        let ends = fo.calls("/read/end");
        assert_eq!(ends.len(), 1);
        assert_eq!(ends[0]["kind"], "CheckpointRead");
        assert_eq!(ends[0]["params"]["id"], "3");
        assert!(ARMS.contains(&ends[0]["params"]["arm"].as_str().unwrap()));
        assert_eq!(ends[0]["route"]["consistency"], "linearizable");
        // No checkpoint yet: the ticket is abandoned.
        fo.set_view("checkpoints", json!({"target": null}));
        testkit::run_rule(&d, checkpoint_read);
        assert_eq!(fo.calls("/read/end").len(), 1);
        assert!(d.s.registry.snapshot().is_empty());
    }

    #[test]
    fn burst_job_sends_unkeyed_single_attempts_from_every_client() {
        let (d, ft, _) = testkit::driver();
        testkit::reset();
        let writes: Vec<(Route, Value)> = (0..9).map(|i| (Route::plain(i % 3), burst_body(&d.s.run, i as u64, 1))).collect();
        submit_burst(&d, writes);
        let ext = d.s.ext::<CheckpointExt>();
        // Once done, the next burst may start.
        testkit::wait_until(Duration::from_secs(5), || ext.bursts.load(Ordering::SeqCst) == 1 && !ext.burst_pending.load(Ordering::SeqCst));
        let applies: Vec<Value> = ft.calls_of("bucket", "Apply").into_iter().filter(|a| a["request"]["unsigned"]["requests"][0]["apply"]["ledger"] == json!(burst_ledger(&d.s.run))).collect();
        assert_eq!(applies.len(), 9);
        assert!(applies.iter().all(|a| a["request"]["unsigned"].get("idempotencyKey").is_none()), "unkeyed");
        assert!(testkit::fired("a write burst follows a query checkpoint create"));
    }

    #[test]
    fn setup_creates_the_burst_ledger() {
        let (d, ft, _) = testkit::driver();
        checkpoint_setup(&d);
        assert!(d.s.ext::<CheckpointExt>().burst_ready.load(Ordering::SeqCst));
        let applies = ft.calls_of("bucket", "Apply");
        assert_eq!(applies[0]["request"]["unsigned"]["requests"][0], json!({"createLedger": {"name": "r-burst-test"}}));
        assert_eq!(ft.calls_of("cluster", "ListQueryCheckpoints").len(), 1);
        assert_eq!(ft.calls_of("cluster", "GetQueryCheckpointSchedule").len(), 1);
    }
}
