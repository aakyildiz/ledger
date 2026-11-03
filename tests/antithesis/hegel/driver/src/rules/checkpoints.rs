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
/// `checkpoint_registry` (fence + metadata read) stays within this from its
/// start (COMMON: nothing in a rule waits more than about 10 s).
pub const REGISTRY_BUDGET: Duration = Duration::from_secs(9);
/// Kept for the metadata read: the fence must finish this long before
/// `REGISTRY_BUDGET` runs out.
pub const METADATA_RESERVE: Duration = Duration::from_secs(2);
/// No single fence call waits longer than this.
pub const FENCE_CALL_CAP: Duration = Duration::from_secs(2);
/// A burst job sends nothing after this long (it runs on one of the two FIFO
/// job threads, ahead of quiet-blocking jobs queued behind it).
pub const BURST_BUDGET: Duration = Duration::from_secs(20);
/// One burst write waits at most this long (single attempt, unkeyed).
pub const BURST_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(2);
/// Bursts rotate to a fresh `r-burst-<run>-<gen>` ledger this often.
pub const BURSTS_PER_LEDGER: u64 = 16;
/// Bursts per run (≤ 200 writes each): the burst volume stays bounded.
pub const MAX_BURSTS: u64 = 256;
/// A GetLog probe this far past a checkpoint's max sequence names a log the
/// frozen store never held (DESIGN §3.2; the oracle accepts ≥ 2^39).
pub const UNASSIGNED_SEQ_SLACK: u64 = 1 << 40;

/// Family state shared across rules.
#[derive(Default)]
pub struct CheckpointExt {
    /// A create was acknowledged (a definitive success): `checkpoint_bootstrap` stops.
    pub created: AtomicBool,
    /// One past the newest burst-ledger generation known to exist (0: none).
    pub burst_ready: AtomicU64,
    /// Bursts submitted (drives the ledger rotation and the run cap).
    pub submitted: AtomicU64,
    /// Bursts completed (for tests and logs).
    pub bursts: AtomicU64,
    /// A burst job is queued or running: `checkpoint_burst` waits for it, so
    /// the job queue never accumulates bursts.
    pub burst_pending: AtomicBool,
}

/// The burst ledger of generation `gen`: `r-burst-<run>` first, then
/// `r-burst-<run>-<gen>` (DESIGN §3.1 `r-<family>-<run>-<N>`).
pub fn burst_ledger(run: &str, gen: u64) -> String {
    if gen == 0 {
        format!("r-burst-{run}")
    } else {
        format!("r-burst-{run}-{gen}")
    }
}

/// The time left before `deadline`, capped at `cap`; `None` once (almost) spent.
fn time_left(deadline: Instant, cap: Duration) -> Option<Duration> {
    let left = deadline.saturating_duration_since(Instant::now());
    (left >= Duration::from_millis(50)).then(|| left.min(cap))
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
            None => {
                println!("setup: checkpoint_setup: {method} did not answer; baseline unverified")
            }
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
    ensure_burst_ledger(d, 0, 10, None);
}

/// Creates the burst ledger of generation `gen` (keyed, retried), up to
/// `tries` rounds and, with a deadline, never past it. True once it exists.
fn ensure_burst_ledger(d: &Driver, gen: u64, tries: usize, deadline: Option<Instant>) -> bool {
    let ext = d.s.ext::<CheckpointExt>();
    if ext.burst_ready.load(Ordering::SeqCst) > gen {
        return true;
    }
    let name = burst_ledger(&d.s.run, gen);
    let key = d.s.next_untracked_key();
    for i in 0..tries {
        let mut send = UntrackedSend::new(vec![json!({"createLedger": {"name": name}})], Some(key.clone()));
        send.schedule = (0..d.s.clients.len().max(1)).map(Route::lib).collect();
        if let Some(dl) = deadline {
            let Some(left) = time_left(dl, send.budget) else {
                return false;
            };
            send.budget = left;
            send.attempt_timeout = send.attempt_timeout.min(left);
        }
        let r = d.send_untracked(send);
        if r.outcome.is_ok() || r.outcome.reason == "LEDGER_ALREADY_EXISTS" {
            ext.burst_ready.fetch_max(gen + 1, Ordering::SeqCst);
            return true;
        }
        println!("driver: checkpoints: creating {name}: {}", r.outcome.summary());
        if i + 1 < tries {
            std::thread::sleep(Duration::from_millis(500));
        }
    }
    false
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
        Lifecycle::DeleteNext(id) | Lifecycle::DeleteLive(id) => {
            json!({"deleteQueryCheckpoint": {"checkpointId": id}})
        }
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
    /// ListLedgers "seen" cursor: index into every tracked name the oracle saw.
    pub seen_pick: u32,
    pub probe: String,
    pub unassigned: bool,
    pub collapse: bool,
    pub max_precision: bool,
    pub prefixes: Vec<&'static str>,
    pub page_size: u64,
    pub reverse: bool,
    /// ListAccounts: an address; ListTransactions: a decimal id; ListLedgers:
    /// "edge" (the tracked prefix's edge), "ledger" (the target ledger) or
    /// "seen" (any tracked name, created after the checkpoint or deleted
    /// included: the cursor pool of `M/checkpoint_reads.go:195-197`).
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
        seen_pick: 0,
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
                    _ => draw::one_of(tc, &["edge", "ledger", "seen"]).to_string(),
                });
                if p.cursor.as_deref() == Some("seen") {
                    p.seen_pick = draw::ledger_pick(tc);
                }
            }
        }
        _ => {}
    }
    p
}

/// The `checkpoints` view request of a read plan.
pub fn read_view(p: &ReadPlan) -> ViewReq {
    let mut params = json!({"pick": p.pick, "cpLedgerPick": p.ledger_pick, "logPick": p.log_pick, "seenPick": p.seen_pick});
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
                Some("seen") => target.get("seenLedger").and_then(Value::as_str).map(str::to_string).or_else(|| ledger.clone()).or_else(|| Some(format!("o-{run}-"))),
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

/// `GetClusterState{}` on the pinned node → its id (the answer must come
/// from the `Leader`; the id is the topology entry whose `serviceAddress` is
/// the pinned address — that answer is the leader's, so its `localNode` names
/// the pinned node only when the topology has a single member), `Barrier` →
/// commit index, then poll `GetClusterState{nodeId}` on that node every
/// `poll_ms` until it is itself, in normal sync and persisted at or past the
/// barrier. Every call is capped by the time left before `deadline`.
pub fn run_fence(d: &Driver, client: usize, node: &str, poll_ms: u64, deadline: Instant) -> Fence {
    let route = Route::node(client, node);
    let opts = |t: Duration| CallOpts::linearizable().timeout(t);
    let Some(t) = time_left(deadline, FENCE_CALL_CAP) else {
        return Fence::failed("fence budget exhausted", 0, 0, 0);
    };
    let (_, topo) = d.call(&route, "cluster", "GetClusterState", json!({}), &opts(t));
    let Some(cs) = topo.response.as_ref().filter(|_| topo.is_ok()) else {
        return Fence::failed("topology unavailable", 0, 0, 0);
    };
    if str_field(cs, "state") != "Leader" {
        return Fence::failed("the pinned node is not the leader", 0, 0, 0);
    }
    let members: Vec<&Value> = cs.get("nodes").and_then(Value::as_array).map(|ns| ns.iter().collect()).unwrap_or_default();
    let by_addr = members.iter().find(|n| str_field(n, "serviceAddress") == node).map(|n| u64_field(n, "id")).filter(|&id| id != 0);
    // M/checkpoint_metadata.go:126-137 fails here; a single-member topology
    // (native runs dial "localhost" while the node advertises "127.0.0.1")
    // can only be the pinned node.
    let node_id = match by_addr {
        Some(id) => id,
        None if members.len() <= 1 => u64_field(cs, "localNode"),
        None => 0,
    };
    if node_id == 0 {
        return Fence::failed("unresolved node identity", 0, 0, 0);
    }
    let Some(t) = time_left(deadline, FENCE_CALL_CAP) else {
        return Fence::failed("fence budget exhausted", node_id, 0, 0);
    };
    let (_, b) = d.call(&route, "bucket", "Barrier", json!({}), &opts(t));
    let barrier = b.response.as_ref().filter(|_| b.is_ok()).map(|r| u64_field(r, "commitIndex")).unwrap_or(0);
    if barrier == 0 {
        return Fence::failed("barrier unavailable", node_id, 0, 0);
    }
    let mut polls = 0;
    loop {
        let Some(t) = time_left(deadline, FENCE_CALL_CAP) else {
            return Fence::failed("the node did not persist the barrier in time", node_id, barrier, polls);
        };
        polls += 1;
        let (_, out) = d.call(&route, "cluster", "GetClusterState", json!({"nodeId": node_id.to_string()}), &opts(t));
        if let Some(cs) = out.response.as_ref().filter(|_| out.is_ok()) {
            if let Horizon::Reached(p) = horizon_check(cs, node_id, barrier, false) {
                return Fence { ready: true, why: String::new(), node_id, barrier, persisted: p, polls };
            }
        }
        if Instant::now() + Duration::from_millis(poll_ms) >= deadline {
            return Fence::failed("the node did not persist the barrier in time", node_id, barrier, polls);
        }
        std::thread::sleep(Duration::from_millis(poll_ms));
    }
}

/// The transient outcome a failed fence ends its read ticket with.
pub fn fence_outcome(f: &Fence) -> Outcome {
    Outcome { ok: false, code: "Unavailable".into(), local: true, message: format!("checkpoint metadata fence: {}", f.why), ..Default::default() }
}

/// What `checkpoint_registry` reads after the fence.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Metadata {
    Registry,
    Schedule,
    /// `GetQueryCheckpointInfo` of a live or retained-deleted id
    /// (`MD/parallel_driver_query_checkpoints/main.go:117-137`).
    Info,
}

impl Metadata {
    pub fn kind(self) -> &'static str {
        match self {
            Metadata::Registry => "CheckpointRegistry",
            Metadata::Schedule => "CheckpointSchedule",
            Metadata::Info => "CheckpointInfo",
        }
    }
    pub fn method(self) -> &'static str {
        match self {
            Metadata::Registry => "ListQueryCheckpoints",
            Metadata::Schedule => "GetQueryCheckpointSchedule",
            Metadata::Info => "GetQueryCheckpointInfo",
        }
    }
}

/// `checkpoint_registry` (0.5): read kinds `CheckpointRegistry` /
/// `CheckpointSchedule` / `CheckpointInfo`, one read ticket over the fence and
/// the metadata read on the same pinned node, all within `REGISTRY_BUDGET`.
pub fn checkpoint_registry(d: &Driver, tc: TestCase) {
    d.guarded("checkpoint_registry", &tc, |d, tc| {
        if !d.s.oracle_ok() || d.s.nodes.is_empty() {
            return None;
        }
        let what = [Metadata::Registry, Metadata::Schedule, Metadata::Info][draw::weighted(tc, &[1.0, 1.0, 1.0]).unwrap_or(0)];
        let pick = draw::ledger_pick(tc);
        let client = draw::pick(tc, d.s.clients.len().max(1));
        let node = draw::one_of(tc, &d.s.nodes).clone();
        let poll_ms = draw::int_in(tc, 20, 200);
        registry_read(d, what, pick, client, &node, poll_ms)
    });
}

/// The body of `checkpoint_registry` after its draws.
pub fn registry_read(d: &Driver, what: Metadata, pick: u32, client: usize, node: &str, poll_ms: u64) -> Option<()> {
    let started = Instant::now();
    let views = if what == Metadata::Info { vec![ViewReq::named("checkpoints", json!({"pick": pick}))] } else { vec![] };
    let rt = d.read_begin(what.kind(), &views, json!({}))?;
    let req = if what == Metadata::Info {
        let id = rt.views().first().and_then(|v| v.get("target")).and_then(|t| t.get("id")).and_then(Value::as_str).map(str::to_string);
        let Some(id) = id else {
            rt.abandon();
            return None;
        };
        json!({"checkpointId": id})
    } else {
        json!({})
    };
    let end = started + REGISTRY_BUDGET;
    let fence = run_fence(d, client, node, poll_ms, end - METADATA_RESERVE);
    let route = Route::node(client, node);
    let mut opts = CallOpts::linearizable().timeout(READ_TIMEOUT);
    let out = match time_left(end, READ_TIMEOUT).filter(|_| fence.ready) {
        Some(t) => {
            opts = opts.timeout(t);
            d.call(&route, "cluster", what.method(), req.clone(), &opts).1
        }
        None => fence_outcome(&fence),
    };
    rt.end(&req, &out, &route, &opts, json!({"fence": fence.to_json()}), json!({}));
    Some(())
}

// ---------------------------------------------------------------- bursts

/// One small forced write of a burst on `r-burst-<run>`.
pub fn burst_body(ledger: &str, user: u64, amount: u64) -> Value {
    let mut r = crate::wire::create_tx_request(ledger, &[Posting::new("world", &format!("users:{user}"), amount, "COIN")], None);
    r["apply"]["action"]["createTransaction"]["force"] = json!(true);
    r
}

/// `checkpoint_burst` (0.3): draws a burst of 50–200 small forced writes
/// (bodies and routes), creates a checkpoint (no predicted read), and only if
/// the create succeeded submits a job that sends the burst as unkeyed
/// single-attempt untracked writes from all clients at once, so the burst
/// lands while followers may still be in the checkpoint's gated window
/// (server-signals §1.1, §1.2). One burst at a time, at most `MAX_BURSTS` per
/// run, rotating the ledger every `BURSTS_PER_LEDGER`.
pub fn checkpoint_burst(d: &Driver, tc: TestCase) {
    d.guarded("checkpoint_burst", &tc, |d, tc| {
        let ext = d.s.ext::<CheckpointExt>();
        let submitted = ext.submitted.load(Ordering::SeqCst);
        if !d.s.oracle_ok() || ext.burst_pending.load(Ordering::SeqCst) || submitted >= MAX_BURSTS {
            return None;
        }
        let gen = submitted / BURSTS_PER_LEDGER;
        let ledger = burst_ledger(&d.s.run, gen);
        let n = draw::int_in(tc, 50, 200) as usize;
        let writes: Vec<(Route, Value)> = (0..n).map(|_| (draw::route(tc, &d.s), burst_body(&ledger, draw::int_in(tc, 0, 31), draw::int_in(tc, 1, 1000)))).collect();
        let plan = create_plan(d, tc, "burst");
        if !create_checkpoint(d, plan, None)?.is_success() {
            // No create, no gated window to aim at.
            return None;
        }
        submit_burst(d, gen, writes);
        Some(())
    });
}

struct BurstPending(std::sync::Arc<CheckpointExt>);

impl Drop for BurstPending {
    fn drop(&mut self) {
        self.0.burst_pending.store(false, Ordering::SeqCst);
    }
}

/// Splits a burst by the client each write is routed to (one sender thread
/// per client).
pub fn group_by_client(writes: Vec<(Route, Value)>, clients: usize) -> Vec<Vec<(Route, Value)>> {
    let clients = clients.max(1);
    let mut by_client: Vec<Vec<(Route, Value)>> = vec![Vec::new(); clients];
    for (r, b) in writes {
        by_client[r.client % clients].push((r, b));
    }
    by_client
}

/// Submits the burst job (no draws inside). The job is quiet-blocking (its
/// untracked writes would otherwise overlap a quiet check) and bounded by
/// `BURST_BUDGET`: writes not sent by then are dropped.
pub fn submit_burst(d: &Driver, gen: u64, writes: Vec<(Route, Value)>) {
    submit_burst_within(d, gen, writes, BURST_BUDGET);
}

fn submit_burst_within(d: &Driver, gen: u64, writes: Vec<(Route, Value)>, budget: Duration) {
    let ext = d.s.ext::<CheckpointExt>();
    ext.burst_pending.store(true, Ordering::SeqCst);
    ext.submitted.fetch_add(1, Ordering::SeqCst);
    d.submit_job(Job {
        name: "checkpoint-burst",
        blocks_quiet: true,
        run: Box::new(move |d: &Driver| {
            // Cleared on every exit, a panic included (the runner catches it).
            let _pending = BurstPending(d.s.ext::<CheckpointExt>());
            let deadline = Instant::now() + budget;
            ensure_burst_ledger(d, gen, 1, Some(deadline));
            let groups = group_by_client(writes, d.s.clients.len());
            let total: usize = groups.iter().map(Vec::len).sum();
            let (ok, dropped): (usize, usize) = std::thread::scope(|s| {
                let hs: Vec<_> = groups
                    .into_iter()
                    .map(|group| {
                        crate::assertions::scoped_inheriting(s, move || {
                            let (mut ok, mut dropped) = (0, 0);
                            for (route, body) in group {
                                let Some(t) = time_left(deadline, BURST_ATTEMPT_TIMEOUT) else {
                                    dropped += 1;
                                    continue;
                                };
                                let mut send = UntrackedSend::new(vec![body], None);
                                send.schedule = vec![route];
                                send.attempt_timeout = t;
                                send.budget = t;
                                if d.send_untracked(send).outcome.is_ok() {
                                    ok += 1;
                                }
                            }
                            (ok, dropped)
                        })
                    })
                    .collect();
                hs.into_iter().map(|h| h.join().unwrap_or((0, 0))).fold((0, 0), |a, b| (a.0 + b.0, a.1 + b.1))
            });
            d.s.ext::<CheckpointExt>().bursts.fetch_add(1, Ordering::SeqCst);
            check_sometimes!(ok > 0, "a write burst follows a query checkpoint create", json!({"writes": total, "ok": ok, "dropped": dropped, "ledger": burst_ledger(&d.s.run, gen)}));
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
        for l in [
            Lifecycle::SetSchedule(CRONS[0]),
            Lifecycle::DeleteSchedule,
            Lifecycle::InvalidCron,
            Lifecycle::DeleteZero,
            Lifecycle::DeleteNext("4".into()),
            Lifecycle::DeleteLive("2".into()),
            Lifecycle::Create,
        ] {
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
            let b = burst_body(&burst_ledger("x", 0), draw::int_in(tc, 0, 31), draw::int_in(tc, 1, 1000));
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
        let f = run_fence(&d, 1, "ledger-1:8888", 20, Instant::now() + Duration::from_secs(5));
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
        let f = run_fence(&d, 0, "ledger-1:8888", 20, Instant::now() + Duration::from_secs(5));
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
        let f = run_fence(&d, 0, "ledger-0:8888", 20, started + Duration::from_millis(400));
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
        let f = run_fence(&d, 0, "localhost:9", 20, Instant::now() + Duration::from_secs(2));
        assert!(f.ready, "{f:?}");
        assert_eq!(f.node_id, 1);
    }

    #[test]
    fn fence_never_takes_the_leaders_identity_in_a_multi_node_cluster() {
        // The topology answer is the leader's: with several members and no
        // address match, its localNode is not the pinned node.
        let (d, ft, _) = testkit::driver();
        let nodes = json!([{"id": "1", "serviceAddress": "ledger-0:8888"}, {"id": "2", "serviceAddress": "ledger-1:8888"}]);
        ft.set_default("cluster", "GetClusterState", ok(cs(1, "Leader", 50, nodes)));
        let f = run_fence(&d, 0, "10.0.0.7:8888", 20, Instant::now() + Duration::from_secs(2));
        assert!(!f.ready);
        assert_eq!(f.why, "unresolved node identity");
        assert!(ft.calls_of("bucket", "Barrier").is_empty());
        assert_eq!(ft.calls_of("cluster", "GetClusterState").len(), 1);
    }

    #[test]
    fn fence_calls_are_capped_by_the_deadline() {
        // Every answer takes 1.5 s: the fence must stop near its deadline,
        // not after a full 2 s call cap per poll.
        let (d, ft, _) = testkit::driver();
        let nodes = json!([{"id": "1", "serviceAddress": "ledger-0:8888"}]);
        let slow = |v: Value| FakeReply::Delayed(Duration::from_millis(1500), Outcome::from_json(&json!({"ok": true, "response": v})));
        ft.script("cluster", "GetClusterState", vec![ok(cs(1, "Leader", 5, nodes))]);
        ft.set_default("cluster", "GetClusterState", slow(cs(1, "Leader", 9, json!([]))));
        ft.script("bucket", "Barrier", vec![ok(json!({"commitIndex": "10"}))]);
        let started = Instant::now();
        let f = run_fence(&d, 0, "ledger-0:8888", 20, started + Duration::from_millis(2500));
        assert!(!f.ready);
        let calls = ft.calls_of("cluster", "GetClusterState");
        assert!(calls.len() >= 2);
        // The last poll only gets what was left of the 2.5 s.
        let last = calls.last().unwrap()["timeoutMs"].as_u64().expect("timeoutMs");
        assert!(last < FENCE_CALL_CAP.as_millis() as u64, "{last}");
        assert!(started.elapsed() < Duration::from_millis(4000), "{:?}", started.elapsed());
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
    fn registry_info_reads_the_picked_checkpoint() {
        let (d, ft, fo) = testkit::driver();
        let nodes = json!([{"id": "1", "serviceAddress": "ledger-0:8888"}, {"id": "2", "serviceAddress": "ledger-1:8888"}]);
        ft.script("cluster", "GetClusterState", vec![ok(cs(2, "Leader", 0, nodes)), ok(cs(2, "Follower", 12, json!([])))]);
        ft.script("bucket", "Barrier", vec![ok(json!({"commitIndex": "10"}))]);
        fo.set_view("checkpoints", json!({"live": ["4"], "deleted": [], "target": {"id": "4", "deleted": false}}));
        registry_read(&d, Metadata::Info, 7, 0, "ledger-1:8888", 20).expect("a read");
        let begins = fo.calls("/read/begin");
        assert_eq!(begins[0]["kind"], "CheckpointInfo");
        assert_eq!(begins[0]["views"][0]["name"], "checkpoints");
        assert_eq!(begins[0]["views"][0]["pick"], 7);
        let ends = fo.calls("/read/end");
        assert_eq!(ends[0]["kind"], "CheckpointInfo");
        assert_eq!(ends[0]["request"], json!({"checkpointId": "4"}));
        assert_eq!(ends[0]["hint"]["fence"]["ready"], true);
        let infos = ft.calls_of("cluster", "GetQueryCheckpointInfo");
        assert_eq!(infos.len(), 1);
        assert_eq!(infos[0]["node"], "ledger-1:8888");
        // No checkpoint: the info ticket is abandoned, nothing is sent.
        let (d, ft, fo) = testkit::driver();
        fo.set_view("checkpoints", json!({"target": null}));
        assert!(registry_read(&d, Metadata::Info, 0, 0, "ledger-1:8888", 20).is_none());
        assert!(fo.calls("/read/end").is_empty());
        assert!(ft.calls_of("cluster", "GetQueryCheckpointInfo").is_empty());
        assert!(ft.calls_of("cluster", "GetClusterState").is_empty());
        assert!(d.s.registry.snapshot().is_empty());
    }

    #[test]
    fn registry_rule_stays_within_its_budget() {
        // Every call stalls 1.5 s: fence plus metadata read still end by
        // REGISTRY_BUDGET (plus one call's slack).
        let (d, ft, fo) = testkit::driver();
        let nodes = json!([{"id": "1", "serviceAddress": "ledger-0:8888"}, {"id": "2", "serviceAddress": "ledger-1:8888"}, {"id": "3", "serviceAddress": "ledger-2:8888"}]);
        let slow = |v: Value| FakeReply::Delayed(Duration::from_millis(1500), Outcome::from_json(&json!({"ok": true, "response": v})));
        ft.set_default("cluster", "GetClusterState", slow(cs(1, "Leader", 0, nodes)));
        ft.set_default("bucket", "Barrier", slow(json!({"commitIndex": "10"})));
        let started = Instant::now();
        testkit::run_rule(&d, checkpoint_registry);
        assert!(started.elapsed() < REGISTRY_BUDGET + Duration::from_millis(1600), "{:?}", started.elapsed());
        assert!(fo.calls("/read/end").len() <= 1);
    }

    #[test]
    fn create_races_a_predicted_read_on_the_predicted_id() {
        let (d, ft, fo) = testkit::driver();
        fo.set_live(vec!["o-test-l1".into(), "o-test-l2".into()]);
        fo.set_predicted(json!({"ok": true, "reason": "", "checkpointId": "7"}));
        // Both calls take 400 ms: the read overlaps the create's attempt.
        let delay = Duration::from_millis(400);
        ft.set_default("bucket", "Apply", FakeReply::Delayed(delay, Outcome::from_json(&json!({"ok": true, "response": {}}))));
        ft.set_default("bucket", "GetLedger", FakeReply::Delayed(delay, Outcome::from_json(&json!({"ok": true, "response": {}}))));
        let elapsed = std::sync::Mutex::new(Duration::ZERO);
        testkit::with_tc_n(1, |tc| {
            let plan = create_plan(&d, tc, "test");
            let aim = Some(PredictedAim { client: 1, node: "ledger-2:8888".into() });
            let t0 = Instant::now();
            let r = create_checkpoint(&d, plan, aim).expect("a slot");
            *elapsed.lock().unwrap() = t0.elapsed();
            assert!(r.is_success(), "{r:?}");
        });
        let elapsed = elapsed.into_inner().unwrap();
        assert!(elapsed < delay * 2 - Duration::from_millis(100), "the read runs concurrently with the create: {elapsed:?}");
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
        let target = json!({"id": "3", "deleted": true, "maxSequence": "40", "ledger": "o-test-l1", "logSeq": "12", "seenLedger": "o-test-l9",
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
                "ListLedgers" => {
                    assert!(req.get("ledger").is_none());
                    if p.cursor.as_deref() == Some("seen") {
                        assert_eq!(req["options"]["cursor"], json!(page_token("o-test-l9")));
                    }
                }
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
        let clients = d.s.clients.len();
        assert!(clients >= 2, "the fake driver has several clients");
        let ledger = burst_ledger(&d.s.run, 0);
        let writes: Vec<(Route, Value)> = (0..3 * clients).map(|i| (Route::plain(i % clients), burst_body(&ledger, i as u64, 1))).collect();
        // Each client gets its share.
        let groups = group_by_client(writes.clone(), clients);
        assert_eq!(groups.len(), clients);
        for (c, g) in groups.iter().enumerate() {
            assert_eq!(g.len(), 3);
            assert!(g.iter().all(|(r, _)| r.client == c));
        }
        // Each Apply takes 150 ms: sent one by one, 3·clients writes take
        // ≥ 450·clients ms; concurrently per client, about 450 ms.
        let delay = Duration::from_millis(150);
        ft.set_default("bucket", "Apply", FakeReply::Delayed(delay, Outcome::from_json(&json!({"ok": true, "response": {}}))));
        let started = Instant::now();
        submit_burst(&d, 0, writes);
        let ext = d.s.ext::<CheckpointExt>();
        // Once done, the next burst may start.
        testkit::wait_until(Duration::from_secs(10), || ext.bursts.load(Ordering::SeqCst) == 1 && !ext.burst_pending.load(Ordering::SeqCst));
        let elapsed = started.elapsed();
        assert!(elapsed < delay * (3 * clients as u32) - Duration::from_millis(100), "clients send concurrently: {elapsed:?}");
        let applies: Vec<Value> = ft.calls_of("bucket", "Apply").into_iter().filter(|a| a["request"]["unsigned"]["requests"][0]["apply"]["ledger"] == json!(ledger)).collect();
        assert_eq!(applies.len(), 3 * clients);
        assert!(applies.iter().all(|a| a["request"]["unsigned"].get("idempotencyKey").is_none()), "unkeyed");
        assert_eq!(ext.submitted.load(Ordering::SeqCst), 1);
        assert!(testkit::fired("a write burst follows a query checkpoint create"));
    }

    #[test]
    fn burst_job_drops_writes_past_its_budget() {
        let (d, ft, _) = testkit::driver();
        testkit::reset();
        // Each write takes 200 ms on one client; a 700 ms budget sends a few
        // and drops the rest.
        ft.set_default("bucket", "Apply", FakeReply::Delayed(Duration::from_millis(200), Outcome::from_json(&json!({"ok": true, "response": {}}))));
        let ledger = burst_ledger(&d.s.run, 0);
        d.s.ext::<CheckpointExt>().burst_ready.store(1, Ordering::SeqCst);
        let writes: Vec<(Route, Value)> = (0..50).map(|i| (Route::plain(0), burst_body(&ledger, i, 1))).collect();
        let started = Instant::now();
        submit_burst_within(&d, 0, writes, Duration::from_millis(700));
        let ext = d.s.ext::<CheckpointExt>();
        testkit::wait_until(Duration::from_secs(10), || ext.bursts.load(Ordering::SeqCst) == 1);
        assert!(started.elapsed() < Duration::from_millis(1500), "{:?}", started.elapsed());
        let sent = ft.calls_of("bucket", "Apply").len();
        assert!((1..=5).contains(&sent), "{sent}");
        assert!(testkit::assertions().iter().any(|a| format!("{a:?}").contains("\"dropped\"")), "dropped writes are recorded");
        assert!(time_left(Instant::now(), BURST_ATTEMPT_TIMEOUT).is_none());
        assert_eq!(time_left(Instant::now() + Duration::from_secs(10), BURST_ATTEMPT_TIMEOUT), Some(BURST_ATTEMPT_TIMEOUT));
    }

    #[test]
    fn burst_ledgers_rotate_and_bursts_stop_at_the_cap() {
        assert_eq!(burst_ledger("x", 0), "r-burst-x");
        assert_eq!(burst_ledger("x", 3), "r-burst-x-3");
        let (d, ft, fo) = testkit::driver();
        fo.set_predicted(json!({"ok": true, "reason": "", "checkpointId": "1"}));
        let ext = d.s.ext::<CheckpointExt>();
        ext.submitted.store(BURSTS_PER_LEDGER * 2, Ordering::SeqCst);
        testkit::run_rule(&d, checkpoint_burst);
        testkit::wait_until(Duration::from_secs(10), || ext.bursts.load(Ordering::SeqCst) == 1 && !ext.burst_pending.load(Ordering::SeqCst));
        let gen2 = burst_ledger(&d.s.run, 2);
        let applies = ft.calls_of("bucket", "Apply");
        assert!(applies.iter().any(|a| a["request"]["unsigned"]["requests"][0] == json!({"createLedger": {"name": gen2}})), "the new generation is created");
        assert!(applies.iter().any(|a| a["request"]["unsigned"]["requests"][0]["apply"]["ledger"] == json!(gen2)));
        // At the cap: nothing at all.
        ext.submitted.store(MAX_BURSTS, Ordering::SeqCst);
        let before = ft.calls_of("bucket", "Apply").len();
        testkit::run_rule(&d, checkpoint_burst);
        assert_eq!(ft.calls_of("bucket", "Apply").len(), before);
    }

    #[test]
    fn no_burst_without_a_successful_create() {
        let (d, ft, fo) = testkit::driver();
        fo.set_predicted(json!({"ok": true, "reason": "", "checkpointId": "1"}));
        ft.set_default("bucket", "Apply", FakeReply::Outcome(Outcome::from_json(&json!({"ok": false, "code": "FailedPrecondition", "reason": "CHECKPOINT_LIMIT_REACHED"}))));
        fo.set_verdict(json!({"verdict": "explained"}));
        testkit::run_rule(&d, checkpoint_burst);
        std::thread::sleep(Duration::from_millis(200));
        let ext = d.s.ext::<CheckpointExt>();
        assert_eq!(ext.submitted.load(Ordering::SeqCst), 0);
        assert!(!ext.burst_pending.load(Ordering::SeqCst));
        assert!(ft.calls_of("bucket", "Apply").iter().all(|a| a["request"]["unsigned"]["requests"][0].get("apply").is_none()), "no burst write");
    }

    #[test]
    fn setup_creates_the_burst_ledger() {
        let (d, ft, _) = testkit::driver();
        checkpoint_setup(&d);
        assert_eq!(d.s.ext::<CheckpointExt>().burst_ready.load(Ordering::SeqCst), 1);
        let applies = ft.calls_of("bucket", "Apply");
        assert_eq!(applies[0]["request"]["unsigned"]["requests"][0], json!({"createLedger": {"name": "r-burst-test"}}));
        assert_eq!(ft.calls_of("cluster", "ListQueryCheckpoints").len(), 1);
        assert_eq!(ft.calls_of("cluster", "GetQueryCheckpointSchedule").len(), 1);
    }
}
