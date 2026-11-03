//! Task 15: cross-node identity, stats convergence and cluster-config
//! convergence, as quiet hooks (DESIGN §4.10, §6.10, §7 C6).
//!
//! Every hook runs inside a quiet period: rules are parked, resolvers are
//! drained and the oracle is quiescent, so the only writes are the hooks' own
//! Barriers and the stats witness (on `r-wit-*`). `plan` draws (the client that
//! carries the pinned calls); `run` draws nothing.
//!
//! Liveness rule (COMMON.md): the "connects" and "obtains a qualified
//! observation" Always properties, and every verdict, are evaluated only when
//! the hook ran to completion (every attempt finished before the deadline). A
//! deadline stop reports `<hook> completes within its quiet budget`
//! (Sometimes, false) and evaluates nothing else.
//!
//! Attempt results are classified as
//! - `Late`: a write other than the hook's own (or a membership change) moved a
//!   fence; the attempt is invalid, never a finding;
//! - `Other`: anything else that stopped the attempt (unreachable source,
//!   member never reaching the horizon, failing reads);
//! - `Cut`: the hook's deadline stopped it.
//!
//! The qualified-observation Always is false only when every attempt ended
//! `Other`.
use crate::clients::{CallOpts, Outcome, Route};
use crate::draw;
use crate::machine::Driver;
use crate::nodes::{horizon_check, Horizon, NodeMap};
use crate::quiet::{HookPlan, QuietCtx, QuietHook};
use crate::shared::Shared;
use crate::wire::{as_u64, str_field, u64_field, Posting};
use crate::write::UntrackedSend;
use hegel::TestCase;
use serde::Serialize;
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::{Duration, Instant};

pub const CROSS_NODE_BUDGET: Duration = Duration::from_secs(90);
pub const STATS_BUDGET: Duration = Duration::from_secs(70);
pub const CLUSTER_CONFIG_BUDGET: Duration = Duration::from_secs(20);

/// Attempts per hook run (`MD/eventually_*`: 3).
pub const ATTEMPTS: usize = 3;
/// The audit window of the cross-node digest: the newest `AUDIT_WINDOW + 1`
/// sequences (main-drivers §3.2 timing gotcha: the chain grows for the
/// whole timeline).
pub const AUDIT_WINDOW: u64 = 5000;
/// Poll cadence (`MD/eventually_*` pollDelay).
const POLL: Duration = Duration::from_millis(200);
/// Per-RPC timeout.
const RPC: Duration = Duration::from_secs(5);
/// Page size for the hooks' own listings.
const PAGE: u64 = 1000;
/// How long `select_source` keeps retrying the candidates (a leader
/// election right after a restart takes a few seconds).
const SOURCE_WAIT: Duration = Duration::from_secs(15);
/// Foreign ledgers folded per stats attempt (a `plan`-seeded sample; the
/// fold reads every log of the ledger).
pub const FOREIGN_SAMPLE: usize = 8;
/// Foreign ledgers with more logs than this are not folded (`r-stress-*`
/// bulks): the fold would eat the budget before the convergence window.
pub const FOLD_LOG_LIMIT: u64 = 5000;

pub fn quiet_hooks() -> Vec<QuietHook> {
    vec![
        QuietHook { name: "cross_node_identity", budget: CROSS_NODE_BUDGET, needs_oracle: false, weight: weight_one, plan: plan_client, run: run_cross_node },
        QuietHook { name: "stats_convergence", budget: STATS_BUDGET, needs_oracle: true, weight: weight_one, plan: plan_stats, run: run_stats },
        QuietHook { name: "cluster_config_convergence", budget: CLUSTER_CONFIG_BUDGET, needs_oracle: false, weight: weight_half, plan: plan_client, run: run_cluster_config },
    ]
}

fn weight_one(_: &Shared) -> f64 {
    1.0
}
fn weight_half(_: &Shared) -> f64 {
    0.5
}

/// The hooks' draws: which client carries the pinned calls, and (stats)
/// the seed of the foreign-ledger sample.
struct Plan {
    client: usize,
    seed: u64,
}

fn plan_client(d: &Driver, tc: &TestCase) -> HookPlan {
    Box::new(Plan { client: draw::pick(tc, d.s.clients.len()), seed: 0 })
}

fn plan_stats(d: &Driver, tc: &TestCase) -> HookPlan {
    let client = draw::pick(tc, d.s.clients.len());
    Box::new(Plan { client, seed: draw::int_in(tc, 0, u64::from(u32::MAX)) })
}

fn plan_of(d: &Driver, ctx: &QuietCtx, plan: &HookPlan) -> usize {
    plan.downcast_ref::<Plan>().map(|p| p.client).filter(|c| *c < d.s.clients.len()).unwrap_or(ctx.client)
}

fn seed_of(plan: &HookPlan) -> u64 {
    plan.downcast_ref::<Plan>().map(|p| p.seed).unwrap_or(0)
}

/// Per-`Shared` state of the family.
#[derive(Default)]
pub struct ConvergenceState {
    /// Witness ledger counter (`r-wit-<run>-<N>`).
    pub witness: AtomicU64,
    /// The leader's `clusterConfig` seen by the previous completed
    /// cluster_config_convergence run.
    pub last_config: Mutex<Option<Value>>,
}

// ---------------------------------------------------------------- helpers

/// Why an attempt stopped.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub enum Fail {
    /// A late write or a membership change moved a fence (attempt invalid).
    Late(String),
    /// Any other reason.
    Other(String),
    /// The hook's deadline.
    Cut,
}

impl Fail {
    fn text(&self) -> String {
        match self {
            Fail::Late(s) => format!("late: {s}"),
            Fail::Other(s) => s.clone(),
            Fail::Cut => "deadline".into(),
        }
    }
}

/// The qualified-observation verdict: false only when every attempt failed
/// for a reason other than a late write or a membership change.
pub fn qualified_verdict(qualified: bool, failures: &[Fail]) -> bool {
    qualified || failures.iter().any(|f| matches!(f, Fail::Late(_)))
}

fn remaining(deadline: Instant) -> Duration {
    deadline.saturating_duration_since(Instant::now())
}

/// Sleeps one poll unless that would cross `deadline`; `false` if it would.
fn pause_until(deadline: Instant) -> bool {
    if Instant::now() + POLL >= deadline {
        return false;
    }
    std::thread::sleep(POLL);
    true
}

/// A read-side transient outcome (retry, never a verdict).
pub fn transient(o: &Outcome) -> bool {
    o.is_unknown() || o.local || matches!(o.code.as_str(), "Unavailable" | "DeadlineExceeded" | "Canceled" | "Aborted" | "ResourceExhausted")
}

/// One RPC, retried up to 4 times while the outcome is transient. Returns the
/// last outcome; `Cut` when the deadline leaves no time for a call.
fn call_retry(d: &Driver, route: &Route, service: &str, method: &str, req: &Value, opts: &CallOpts, deadline: Instant) -> Result<Outcome, Fail> {
    let mut last = Outcome::unknown("timeout");
    for k in 0..4u32 {
        let left = remaining(deadline);
        if left < Duration::from_millis(100) {
            return Err(Fail::Cut);
        }
        let o = opts.clone().timeout(opts.timeout.min(left));
        last = d.call(route, service, method, req.clone(), &o).1;
        if last.is_ok() || !transient(&last) {
            return Ok(last);
        }
        if k < 3 && !pause_until(deadline) {
            return Err(Fail::Cut);
        }
    }
    Ok(last)
}

fn describe(o: &Outcome) -> String {
    o.summary().to_string()
}

/// A Barrier through `route`: its commit index.
fn barrier(d: &Driver, route: &Route, deadline: Instant) -> Result<u64, Fail> {
    let o = call_retry(d, route, "bucket", "Barrier", &json!({}), &CallOpts::default().timeout(RPC), deadline)?;
    let idx = o.response.as_ref().filter(|_| o.is_ok()).map(|r| u64_field(r, "commitIndex")).unwrap_or(0);
    if idx == 0 {
        return Err(Fail::Other(format!("barrier failed: {}", describe(&o))));
    }
    Ok(idx)
}

/// Candidate node addresses: the quiet context's discovery, then the
/// configured node list.
fn candidates(d: &Driver, nodes: &NodeMap) -> Vec<String> {
    let mut out: Vec<String> = nodes.nodes.iter().map(|n| n.addr.clone()).collect();
    for a in &d.s.nodes {
        if !out.contains(a) {
            out.push(a.clone());
        }
    }
    out
}

/// The first candidate whose pinned Barrier works, and that Barrier's index.
/// While some candidate's answer is transient (no leader during an
/// election, a restarting node), rounds over the candidates are retried with
/// backoff for up to `SOURCE_WAIT`, or a quarter of the remaining budget. Any
/// non-local answer from a candidate sets `connected` (the old check was a
/// per-node dial, `MD/eventually_cross_node_identity/main.go:24`).
fn select_source(d: &Driver, client: usize, cands: &[String], deadline: Instant, connected: &mut bool) -> Result<(String, u64), Fail> {
    let mut last = "no candidate node".to_string();
    let give_up = Instant::now() + SOURCE_WAIT.min(remaining(deadline) / 4);
    let mut backoff = POLL;
    loop {
        let mut retryable = false;
        for addr in cands {
            let left = remaining(deadline);
            if left < Duration::from_millis(100) {
                return Err(Fail::Cut);
            }
            let o = d.call(&Route::node(client, addr), "bucket", "Barrier", json!({}), &CallOpts::default().timeout(RPC.min(left))).1;
            if !o.is_unknown() && !o.local {
                *connected = true;
            }
            let idx = o.response.as_ref().filter(|_| o.is_ok()).map(|r| u64_field(r, "commitIndex")).unwrap_or(0);
            if idx > 0 {
                return Ok((addr.clone(), idx));
            }
            retryable |= transient(&o);
            last = format!("{addr}: {}", describe(&o));
        }
        if !retryable || Instant::now() + backoff >= give_up {
            return Err(Fail::Other(format!("no reachable source for a quorum barrier ({last})")));
        }
        if Instant::now() + backoff >= deadline {
            return Err(Fail::Cut);
        }
        std::thread::sleep(backoff);
        backoff = (backoff * 2).min(Duration::from_secs(2));
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Member {
    pub id: u64,
    pub addr: String,
    pub suffrage: String,
}

/// The current membership from a `GetClusterState{}` answer
/// (`MD/eventually_cross_node_identity/oracle.go` membership): ids and
/// addresses unique and non-zero, suffrage Voter or Learner, sorted by id.
pub fn parse_membership(cs: &Value) -> Result<Vec<Member>, String> {
    let nodes = cs.get("nodes").and_then(Value::as_array).ok_or_else(|| "no nodes in cluster state".to_string())?;
    let (mut ids, mut addrs) = (BTreeSet::new(), BTreeSet::new());
    let mut out = Vec::new();
    for n in nodes {
        let (id, addr, suffrage) = (u64_field(n, "id"), str_field(n, "serviceAddress"), str_field(n, "suffrage"));
        if id == 0 || addr.is_empty() || !ids.insert(id) || !addrs.insert(addr.to_string()) || !(suffrage == "Voter" || suffrage == "Learner") {
            return Err(format!("invalid member identity: id={id} address={addr:?} suffrage={suffrage:?}"));
        }
        out.push(Member { id, addr: addr.to_string(), suffrage: suffrage.to_string() });
    }
    if out.is_empty() {
        return Err("empty current membership".into());
    }
    out.sort_by_key(|m| m.id);
    Ok(out)
}

/// The membership as the source sees it (and the leader id it reports).
fn membership(d: &Driver, client: usize, source: &str, deadline: Instant) -> Result<(Vec<Member>, u64), Fail> {
    let o = call_retry(d, &Route::node(client, source), "cluster", "GetClusterState", &json!({}), &CallOpts::default().timeout(RPC), deadline)?;
    let Some(cs) = o.response.as_ref().filter(|_| o.is_ok()) else {
        return Err(Fail::Other(format!("membership: {}", describe(&o))));
    };
    let members = parse_membership(cs).map_err(Fail::Other)?;
    Ok((members, u64_field(cs, "leader")))
}

/// Polls a member's own cluster state until it is at the horizon
/// (`exact`: `lastPersistedIndex == horizon`). Returns its answer. Past an
/// exact horizon is `Late`; the window ending is `Other`; the deadline `Cut`.
fn await_horizon(d: &Driver, client: usize, m: &Member, horizon: u64, exact: bool, window_end: Instant, deadline: Instant) -> Result<Value, Fail> {
    let mut last = Value::Null;
    loop {
        if Instant::now() + Duration::from_millis(100) >= deadline {
            return Err(Fail::Cut);
        }
        if let Some(cs) = d.cluster_state_via(client, &m.addr, m.id) {
            match horizon_check(&cs, m.id, horizon, exact) {
                Horizon::Reached(_) => return Ok(cs),
                Horizon::Passed(p) => return Err(Fail::Late(format!("member {} persisted {p} past the horizon {horizon}", m.id))),
                Horizon::NotYet => last = cs,
            }
        }
        let end = window_end.min(deadline);
        if !pause_until(end) {
            if window_end < deadline {
                let got = json!({"localNode": last.get("localNode"), "status": last.pointer("/syncProgress/status"), "persisted": last.pointer("/raftStatus/lastPersistedIndex")});
                return Err(Fail::Other(format!("member {} did not reach the horizon {horizon}: {got}", m.id)));
            }
            return Err(Fail::Cut);
        }
    }
}

/// Every page of a listing (`options.pageSize`/`cursor`; complete pages only,
/// cursors must not repeat).
fn list_all(d: &Driver, route: &Route, method: &str, mut req: Value, opts: &CallOpts, deadline: Instant) -> Result<Vec<Value>, Fail> {
    let mut items = Vec::new();
    let mut seen = BTreeSet::new();
    let mut cursor = String::new();
    loop {
        req["options"]["pageSize"] = json!(PAGE);
        if cursor.is_empty() {
            if let Some(o) = req["options"].as_object_mut() {
                o.remove("cursor");
            }
        } else {
            req["options"]["cursor"] = json!(cursor);
        }
        // One item past the page: the client stops reading at maxItems and
        // reports such a page incomplete (task 99: every full 1000-row page
        // made the stats observation fail).
        let o = call_retry(d, route, "bucket", method, &req, &opts.clone().max_items(PAGE + 1), deadline)?;
        if !o.is_ok() || !o.complete {
            return Err(Fail::Other(format!("{method} page: {}", describe(&o))));
        }
        items.extend(o.items.clone().unwrap_or_default());
        if o.next_cursor.is_empty() {
            return Ok(items);
        }
        if !seen.insert(o.next_cursor.clone()) {
            return Err(Fail::Other(format!("{method}: repeated cursor")));
        }
        cursor = o.next_cursor.clone();
    }
}

/// Runs `f` for every member on scoped threads (they inherit the assertion
/// sink); results in member order.
fn per_member<T: Send>(members: &[Member], f: impl Fn(&Member) -> T + Sync) -> Vec<T> {
    std::thread::scope(|s| {
        let hs: Vec<_> = members.iter().map(|m| crate::assertions::scoped_inheriting(s, || f(m))).collect();
        hs.into_iter().map(|h| h.join().unwrap_or_else(|e| std::panic::resume_unwind(e))).collect()
    })
}

fn first_fail<T>(rs: Vec<Result<T, Fail>>) -> Result<Vec<T>, Fail> {
    // A cut wins (the deadline invalidates everything), then a late write.
    if rs.iter().any(|r| matches!(r, Err(Fail::Cut))) {
        return Err(Fail::Cut);
    }
    if let Some(Err(f)) = rs.iter().find(|r| matches!(r, Err(Fail::Late(_)))) {
        return Err(f.clone());
    }
    rs.into_iter().collect()
}

// ---------------------------------------------------------------- cross-node identity

/// What one member's audit trail looks like at the horizon.
#[derive(Clone, Debug, Default, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AuditObs {
    /// First sequence of the digest window.
    pub from: u64,
    pub count: u64,
    pub last: String,
    pub sha256: String,
    /// The listing tail (`pageSize:1, reverse:true`), items stripped.
    pub tail: Option<Value>,
    pub tail_seq: u64,
    /// `hashVersion` of the tail entry.
    pub version: u64,
    /// `GetAuditEntry(tail)` answered (NotFound is a real observation).
    pub tip_present: bool,
    /// The point tip, items stripped.
    pub tip: Option<Value>,
    /// The point tip as served, items included (compared across members:
    /// the old `TipDigest`, `MD/eventually_cross_node_identity/observation.go:162`).
    #[serde(skip)]
    pub tip_full: Option<Value>,
    /// The window listing's last entry (a forward one-item listing at
    /// `tail_seq`), items stripped.
    pub listed_last: Option<Value>,
    /// The listing's last entry is the tail (whole entry, as the old
    /// `proto.Equal(lastListed, tail)`), and the stripped tip equals it.
    pub tail_matches: bool,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Observation {
    pub member: Member,
    /// The `/digest` sections `ledgers`, `accounts`, `audit`.
    pub sections: Value,
    pub audit: AuditObs,
}

impl Observation {
    fn account_count(&self) -> u64 {
        u64_field(&self.sections["accounts"], "count")
    }
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CrossReport {
    pub horizon: u64,
    pub members: Vec<Member>,
    pub observations: Vec<Observation>,
}

/// Fetches one member's digest: `(driver, client, body) -> sections`.
pub type DigestFn<'a> = dyn Fn(&Driver, usize, &Value) -> Result<Value, String> + Sync + 'a;

/// `POST /digest` on a client (DESIGN §7 C6).
pub fn post_digest(d: &Driver, client: usize, body: &Value) -> Result<Value, String> {
    let mut body = body.clone();
    body["opId"] = json!(d.s.clients.next_op_id());
    let t = Duration::from_millis(body["timeoutMs"].as_u64().unwrap_or(60_000)) + Duration::from_millis(1500);
    let resp = d.s.clients.transport.post(client, "/digest", &body, t).map_err(|e| format!("digest transport: {e:?}"))?;
    if resp.get("ok").and_then(Value::as_bool) != Some(true) {
        return Err(format!("digest failed: {} {}", str_field(&resp, "error"), resp.get("outcome").cloned().unwrap_or(Value::Null)));
    }
    Ok(resp.get("sections").cloned().unwrap_or(Value::Null))
}

fn strip_items(mut v: Value) -> Value {
    if let Some(o) = v.as_object_mut() {
        o.remove("items");
    }
    v
}

/// One member's audit tail reads.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct TailObs {
    /// The reverse one-item listing, items stripped.
    pub tail: Option<Value>,
    /// The forward one-item listing at the tail's sequence, items stripped.
    pub listed_last: Option<Value>,
    /// `GetAuditEntry(tail)` answered.
    pub tip_present: bool,
    /// The point tip as served (items included).
    pub tip_full: Option<Value>,
}

/// The audit listing filtered to exactly one sequence (forward).
fn audit_at(seq: u64) -> Value {
    json!({"options": {"pageSize": 1, "filter": {"audit": {"field": "AUDIT_FIELD_SEQUENCE", "uintCond": {"min": seq.to_string(), "max": seq.to_string()}}}}})
}

/// The listing tail, the forward listing's entry at the tail's sequence and
/// the point tip of one member (stale, pinned).
fn read_tail(d: &Driver, client: usize, m: &Member, deadline: Instant) -> Result<TailObs, Fail> {
    let route = Route::node(client, &m.addr);
    let o = call_retry(d, &route, "bucket", "ListAuditEntries", &json!({"options": {"pageSize": 1, "reverse": true}}), &CallOpts::stale().timeout(RPC).max_items(2), deadline)?;
    if !o.is_ok() || !o.complete {
        return Err(Fail::Other(format!("member {} audit tail: {}", m.id, describe(&o))));
    }
    let items = o.items.clone().unwrap_or_default();
    if items.len() > 1 {
        return Err(Fail::Other(format!("member {} audit tail page exceeded requested size", m.id)));
    }
    let Some(tail) = items.into_iter().next().map(strip_items) else { return Ok(TailObs::default()) };
    let seq = tail.get("sequence").cloned().unwrap_or(Value::Null);
    let fwd = call_retry(d, &route, "bucket", "ListAuditEntries", &audit_at(u64_field(&tail, "sequence")), &CallOpts::stale().timeout(RPC).max_items(2), deadline)?;
    if !fwd.is_ok() || !fwd.complete {
        return Err(Fail::Other(format!("member {} audit listing at the tail: {}", m.id, describe(&fwd))));
    }
    let listed = fwd.items.clone().unwrap_or_default();
    if listed.len() > 1 {
        return Err(Fail::Other(format!("member {} audit listing at the tail exceeded requested size", m.id)));
    }
    let listed_last = listed.into_iter().next().map(strip_items);
    let o = call_retry(d, &route, "bucket", "GetAuditEntry", &json!({"sequence": seq}), &CallOpts::stale().timeout(RPC), deadline)?;
    if o.is_ok() {
        let tip_full = o.response.clone();
        return Ok(TailObs { tail: Some(tail), listed_last, tip_present: tip_full.is_some(), tip_full });
    }
    if o.code == "NotFound" {
        // At an exact durable horizon absence is an observation, not skew.
        return Ok(TailObs { tail: Some(tail), listed_last, tip_present: false, tip_full: None });
    }
    Err(Fail::Other(format!("member {} audit tip: {}", m.id, describe(&o))))
}

fn audit_obs(from: u64, sections: &Value, t: TailObs) -> AuditObs {
    let TailObs { tail, listed_last, tip_present, tip_full } = t;
    let tip = tip_full.clone().map(strip_items);
    let a = &sections["audit"];
    let (count, last) = (u64_field(a, "count"), str_field(a, "lastKey").to_string());
    let tail_seq = tail.as_ref().map(|t| u64_field(t, "sequence")).unwrap_or(0);
    let version = tail.as_ref().map(|t| t.get("hashVersion").and_then(as_u64).unwrap_or(0)).unwrap_or(0);
    let listing_ends_at_tail = match &tail {
        None => count == 0,
        Some(_) => count > 0 && last == tail_seq.to_string() && listed_last == tail,
    };
    let tip_agrees = match (&tail, &tip) {
        (Some(t), Some(p)) => t == p,
        _ => true,
    };
    AuditObs { from, count, last, sha256: str_field(a, "sha256").to_string(), tail, tail_seq, version, tip_present, tip, tip_full, listed_last, tail_matches: listing_ends_at_tail && tip_agrees }
}

/// The first difference between two members' `ledgers`/`accounts` digests:
/// the first ledger (by name) whose per-ledger digest differs, else the
/// section totals.
pub fn first_difference(a: &Value, b: &Value) -> Option<Value> {
    for sec in ["ledgers", "accounts"] {
        let (x, y) = (&a[sec], &b[sec]);
        let empty = serde_json::Map::new();
        let xl = x.get("ledgers").and_then(Value::as_object).unwrap_or(&empty);
        let yl = y.get("ledgers").and_then(Value::as_object).unwrap_or(&empty);
        let names: BTreeSet<&String> = xl.keys().chain(yl.keys()).collect();
        for n in names {
            if xl.get(n) != yl.get(n) {
                return Some(json!({"section": sec, "ledger": n, "reference": xl.get(n), "observation": yl.get(n)}));
            }
        }
        let key = |v: &Value| (u64_field(v, "count"), str_field(v, "sha256").to_string(), str_field(v, "lastKey").to_string());
        if key(x) != key(y) {
            let sum = |v: &Value| json!({"count": v.get("count"), "sha256": v.get("sha256"), "lastKey": v.get("lastKey")});
            return Some(json!({"section": sec, "reference": sum(x), "observation": sum(y)}));
        }
    }
    None
}

/// Audit identity of two members: same window digest, the same tail and the
/// same tip, items included.
pub fn audit_equal(a: &AuditObs, b: &AuditObs) -> bool {
    a.from == b.from
        && a.count == b.count
        && a.last == b.last
        && a.sha256 == b.sha256
        && a.tail == b.tail
        && a.listed_last == b.listed_last
        && a.tip_present == b.tip_present
        && a.tip == b.tip
        && a.tip_full == b.tip_full
        && a.tail_matches == b.tail_matches
}

/// Timing knobs (tests shorten them).
#[derive(Clone, Copy, Debug)]
pub struct Windows {
    /// Shared convergence window of the member waits.
    pub convergence: Duration,
    /// Time kept back at the end of the budget for the closing fences.
    pub reserve: Duration,
}

pub const CROSS_WINDOWS: Windows = Windows { convergence: Duration::from_secs(60), reserve: Duration::from_secs(3) };
pub const STATS_WINDOWS: Windows = Windows { convergence: Duration::from_secs(60), reserve: Duration::from_secs(3) };

/// The shortest stats window worth opening.
const MIN_WINDOW: Duration = Duration::from_secs(1);

impl Windows {
    /// Cross-node: the end of the shared window. When the budget cannot hold
    /// the whole window plus the reserve, the window ends at the deadline, so
    /// running out is a cut (no verdict), never a qualification failure (a
    /// member missing a shortened window would be an `Other` attempt).
    fn end_or_deadline(&self, deadline: Instant) -> Instant {
        let e = Instant::now() + self.convergence;
        if e + self.reserve > deadline {
            deadline
        } else {
            e
        }
    }

    /// Stats: `now + min(convergence, remaining − reserve)`, so the window
    /// always ends before the reserve and the closing fence still runs; an
    /// uncertified member at a shortened window end is a liveness
    /// observation, not a usage verdict. `Cut` when not even `MIN_WINDOW`
    /// fits.
    fn end_within(&self, deadline: Instant) -> Result<Instant, Fail> {
        let usable = remaining(deadline).saturating_sub(self.reserve);
        if usable < MIN_WINDOW {
            return Err(Fail::Cut);
        }
        Ok(Instant::now() + self.convergence.min(usable))
    }
}

/// Below this much budget a digest is not started, and a failed digest is a
/// cut (it most likely timed out because the budget ran out).
const DIGEST_MIN: Duration = Duration::from_secs(1);

/// One member's digest, retried once on failure while the budget allows.
fn member_digest(d: &Driver, client: usize, m: &Member, from: u64, deadline: Instant, digest: &DigestFn<'_>) -> Result<Value, Fail> {
    let mut last = String::new();
    for _ in 0..2 {
        let left = remaining(deadline);
        if left < DIGEST_MIN {
            return Err(Fail::Cut);
        }
        let body = json!({
            "node": m.addr, "consistency": "stale", "sections": ["ledgers", "accounts", "audit"],
            "auditFrom": from.to_string(), "items": false, "timeoutMs": (left - Duration::from_millis(500)).as_millis() as u64,
        });
        match digest(d, client, &body) {
            Ok(s) => return Ok(s),
            Err(e) => {
                println!("driver: cross_node_identity: member {} digest failed: {e}", m.id);
                last = e;
            }
        }
    }
    if remaining(deadline) < DIGEST_MIN {
        return Err(Fail::Cut);
    }
    Err(Fail::Other(format!("member {} digest: {last}", m.id)))
}

/// One cross-node attempt (`MD/eventually_cross_node_identity/oracle.go` runAttempt).
fn cross_attempt(d: &Driver, client: usize, cands: &[String], deadline: Instant, w: Windows, digest: &DigestFn<'_>, connected: &mut bool) -> Result<CrossReport, Fail> {
    let (source, before) = select_source(d, client, cands, deadline, connected)?;
    let src = Route::node(client, &source);
    let (members, _) = membership(d, client, &source, deadline)?;
    let horizon = barrier(d, &src, deadline)?;
    if horizon != before + 1 {
        return Err(Fail::Late(format!("quiescence not established: {before} -> {horizon}")));
    }
    // Every current member participates; one shared window.
    let end = w.end_or_deadline(deadline);
    first_fail(per_member(&members, |m| await_horizon(d, client, m, horizon, true, end, deadline)))?;

    // The audit head fixes the common digest window.
    let tails = first_fail(per_member(&members, |m| read_tail(d, client, m, deadline)))?;
    let head = tails.first().and_then(|t| t.tail.as_ref()).map(|t| u64_field(t, "sequence")).unwrap_or(0);
    let from = head.saturating_sub(AUDIT_WINDOW).max(1);
    let observed = per_member(&members, |m| {
        let sections = member_digest(d, client, m, from, deadline, digest)?;
        // The member must still be exactly at the horizon.
        await_horizon(d, client, m, horizon, true, Instant::now(), deadline).map_err(|f| match f {
            Fail::Other(e) => Fail::Other(format!("after the digest: {e}")),
            f => f,
        })?;
        Ok(sections)
    });
    let sections = first_fail(observed)?;
    let observations: Vec<Observation> = members
        .iter()
        .zip(sections)
        .zip(tails)
        .map(|((m, s), t)| Observation { member: m.clone(), audit: audit_obs(from, &s, t), sections: s })
        .collect();

    // Fences: a late write, election entry or membership change invalidates the sweep.
    let (after_members, _) = membership(d, client, &source, deadline)?;
    if after_members != members {
        return Err(Fail::Late(format!("membership changed during observation: {members:?} -> {after_members:?}")));
    }
    let after = barrier(d, &src, deadline)?;
    if after != horizon + 1 {
        return Err(Fail::Late(format!("final source horizon changed: {horizon} -> {after}")));
    }
    Ok(CrossReport { horizon, members, observations })
}

/// The cross-node identity hook body.
pub fn cross_node_identity(d: &Driver, client: usize, nodes: &NodeMap, deadline: Instant, w: Windows, digest: &DigestFn<'_>) {
    let cands = candidates(d, nodes);
    // Discovery answered (the old per-node dial), or a candidate answers below.
    let mut connected = !nodes.nodes.is_empty();
    let mut failures = Vec::new();
    let mut report = None;
    let mut cut = false;
    for _ in 0..ATTEMPTS {
        match cross_attempt(d, client, &cands, deadline, w, digest, &mut connected) {
            Ok(r) => {
                report = Some(r);
                break;
            }
            Err(Fail::Cut) => {
                cut = true;
                break;
            }
            Err(f) => {
                println!("driver: cross_node_identity attempt failed: {}", f.text());
                failures.push(f);
                if !pause_until(deadline) {
                    cut = true;
                    break;
                }
            }
        }
    }
    let completed = !cut && Instant::now() <= deadline;
    check_sometimes!(completed, "cross_node_identity completes within its quiet budget", json!({"failures": failures}));
    if !completed {
        return;
    }
    check_always!(connected, "cross-node identity oracle connects to replicas", json!({"candidates": cands, "failures": failures}));
    let qualified = qualified_verdict(report.is_some(), &failures);
    check_always!(qualified, "cross-node identity oracle obtains a qualified all-member observation", json!({"attempts": failures}));
    if let Some(r) = &report {
        publish_cross(r);
    }
}

/// The verdicts of a qualified observation (`MD/.../main.go` publishReports).
pub fn publish_cross(r: &CrossReport) {
    let obs = &r.observations;
    check_sometimes!(true, "cross-node oracle reaches quiescence", json!({"index": r.horizon}));
    check_sometimes!(obs.len() >= 2, "at least two nodes converge to a common applied index", json!({"index": r.horizon, "readyNodes": obs.len(), "totalNodes": r.members.len()}));
    check_reachable!("all current replicas reach the same durable identity horizon", json!({"index": r.horizon, "members": r.members}));
    for o in obs {
        let a = &o.audit;
        check_always!(a.tail_matches && (a.count == 0 || a.tip_present), "complete audit listing and point tip agree at a common applied index", json!({"index": r.horizon, "member": o.member, "audit": a}));
    }
    let Some(reference) = obs.first() else { return };
    let mut all_equal = true;
    for o in &obs[1..] {
        let diff = first_difference(&reference.sections, &o.sections);
        let details = json!({"index": r.horizon, "reference": reference.member, "observation": o.member, "firstDifference": diff, "referenceAudit": reference.audit, "observationAudit": o.audit});
        check_always!(diff.is_none(), "account state is identical across nodes at a common applied index", details);
        let ae = audit_equal(&reference.audit, &o.audit);
        check_always!(ae, "audit hash is identical across nodes at the same sequence", details);
        check_always!(reference.audit.version == o.audit.version, "audit hash version is identical across nodes at the same sequence", details);
        all_equal = all_equal && diff.is_none() && ae;
    }
    // Deviation (task 15 acceptance): one member reaches this trivially; the
    // old driver returned before it with fewer than two observations.
    let a = &reference.audit;
    if all_equal && reference.account_count() > 0 && a.count > 0 && a.tail_matches && a.tip_present {
        check_reachable!("cross-node complete account and audit identity verified", json!({"index": r.horizon, "members": r.members.len(), "accounts": reference.account_count(), "auditCount": a.count, "auditFrom": a.from}));
    }
}

fn run_cross_node(d: &Driver, ctx: &mut QuietCtx, plan: &HookPlan) {
    let client = plan_of(d, ctx, plan);
    cross_node_identity(d, client, &ctx.nodes, ctx.deadline, CROSS_WINDOWS, &post_digest);
}

// ---------------------------------------------------------------- stats convergence

/// The counters compared (usage fold vs source).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize)]
pub struct Counts {
    pub logs: u64,
    pub postings: u64,
    pub reverts: u64,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize)]
pub struct Expected {
    pub id: u64,
    pub counts: Counts,
}

/// One tracked ledger of `/quiet/begin` `providers.usage`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize)]
pub struct Tracked {
    /// The model's transaction count (checked against the source's
    /// `transactionCount` inside the capture fence).
    pub tx: u64,
    pub counts: Counts,
}

/// Tracked source counts from `/quiet/begin` `providers.usage`.
pub fn tracked_expectations(begin: &Value) -> Option<BTreeMap<String, Tracked>> {
    let ledgers = begin.pointer("/providers/usage")?.as_object()?;
    Some(
        ledgers
            .iter()
            .map(|(name, u)| {
                (name.clone(), Tracked { tx: u64_field(u, "txCount"), counts: Counts { logs: u64_field(u, "logCount"), postings: u64_field(u, "postingCount"), reverts: u64_field(u, "revertCount") } })
            })
            .collect(),
    )
}

/// Folds one ledger's ListLogs items (`MD/eventually_stats_consistency/source.go` foldLogs):
/// contiguous ids from 1, `payload.apply.ledgerName == ledger`.
pub fn fold_logs(ledger: &str, items: &[Value]) -> Result<Counts, String> {
    let mut c = Counts::default();
    let mut last = 0u64;
    for it in items {
        let apply = it.pointer("/payload/apply").ok_or_else(|| format!("non-apply log after {last}"))?;
        let id = apply.get("log").map(|l| u64_field(l, "id")).unwrap_or(0);
        if str_field(apply, "ledgerName") != ledger || id != last + 1 {
            return Err(format!("non-contiguous or foreign log after {last}"));
        }
        last = id;
        c.logs += 1;
        let data = apply.pointer("/log/data").and_then(Value::as_object).filter(|o| !o.is_empty()).ok_or_else(|| format!("log {id} has no payload"))?;
        let postings = |tx: Option<&Value>| tx.and_then(|t| t.get("postings")).and_then(Value::as_array).map(Vec::len).unwrap_or(0) as u64;
        if let Some(ct) = data.get("createdTransaction") {
            let tx = ct.get("transaction").ok_or_else(|| format!("created log {id} has no transaction"))?;
            c.postings += postings(Some(tx));
        } else if let Some(rt) = data.get("revertedTransaction") {
            let tx = rt.get("revertTransaction").ok_or_else(|| format!("revert log {id} has no transaction"))?;
            c.reverts += 1;
            c.postings += postings(Some(tx));
        }
    }
    Ok(c)
}

fn stats_counts(s: &Value) -> Counts {
    Counts { logs: u64_field(s, "logCount"), postings: u64_field(s, "postingCount"), reverts: u64_field(s, "revertCount") }
}

#[derive(Clone, Debug, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StatsObservation {
    pub node: String,
    pub node_id: u64,
    /// A certified sweep matched every expectation.
    pub converged: bool,
    /// At least one complete sweep was certified: the witness showed
    /// `referenceCount == 1` before and after it, so the member's usage fold
    /// had passed the source horizon for the whole sweep.
    pub certified: bool,
    /// A definitive divergence outside the counters (a ledger incarnation
    /// differing from the source's, a non-transient read failure at a member
    /// past the horizon).
    pub fatal: bool,
    pub witness_references: u64,
    pub attempts: u64,
    /// The last certified sweep's mismatches: ledger → served counts.
    pub mismatches: BTreeMap<String, Counts>,
    pub error: String,
}

impl StatsObservation {
    /// The usage verdict: `Some(true)` converged, `Some(false)` a certified
    /// sweep mismatched (or a definitive divergence), `None` no certified
    /// sweep (a liveness observation, not a usage finding).
    pub fn verdict(&self) -> Option<bool> {
        if self.converged {
            Some(true)
        } else if self.fatal || (self.certified && !self.mismatches.is_empty()) {
            Some(false)
        } else {
            None
        }
    }
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StatsReport {
    pub horizon: u64,
    pub witness: String,
    pub expected: BTreeMap<String, Expected>,
    pub observations: Vec<StatsObservation>,
}

enum Sample {
    Matched,
    NotYet,
    Fatal(String),
}

/// A stale pinned unary read; `Err(None)` for a transient outcome.
fn stale_read(d: &Driver, route: &Route, method: &str, req: Value, deadline: Instant) -> Result<Value, Option<String>> {
    let t = RPC.min(remaining(deadline));
    if t < Duration::from_millis(100) {
        return Err(None);
    }
    let o = d.call(route, "bucket", method, req, &CallOpts::stale().timeout(t)).1;
    match o.response.as_ref().filter(|_| o.is_ok()) {
        Some(v) => Ok(v.clone()),
        None if transient(&o) => Err(None),
        None => Err(Some(format!("{method}: {}", describe(&o)))),
    }
}

/// One complete sweep of a member (`observe.go` sampleUsage). `deadline` is
/// the end of the member's window.
fn sample_usage(d: &Driver, route: &Route, witness: &str, wid: u64, expected: &BTreeMap<String, Expected>, deadline: Instant, obs: &mut StatsObservation) -> Sample {
    macro_rules! read {
        ($method:literal, $ledger:expr) => {
            match stale_read(d, route, $method, json!({"ledger": $ledger}), deadline) {
                Ok(v) => v,
                Err(None) => return Sample::NotYet,
                Err(Some(e)) => {
                    // A lagging stale read may not see the witness yet.
                    if $ledger == witness {
                        obs.error = e;
                        return Sample::NotYet;
                    }
                    return Sample::Fatal(e);
                }
            }
        };
    }
    let info = read!("GetLedger", witness);
    if u64_field(&info, "id") != wid {
        return Sample::Fatal(format!("usage witness incarnation changed: {wid} -> {}", u64_field(&info, "id")));
    }
    let marker = read!("GetLedgerStats", witness);
    obs.witness_references = u64_field(&marker, "referenceCount");
    if obs.witness_references != 1 {
        return Sample::NotYet;
    }
    let mut mismatches = BTreeMap::new();
    for (name, want) in expected {
        let info = read!("GetLedger", name.as_str());
        if u64_field(&info, "id") != want.id {
            return Sample::Fatal(format!("ledger {name} incarnation changed: {} -> {}", want.id, u64_field(&info, "id")));
        }
        let got = stats_counts(&read!("GetLedgerStats", name.as_str()));
        if got != want.counts {
            mismatches.insert(name.clone(), got);
        }
    }
    // The usage store has no WAL and can rewind on restart: re-check the
    // witness. Only a sweep certified at both ends counts.
    let marker = read!("GetLedgerStats", witness);
    obs.witness_references = u64_field(&marker, "referenceCount");
    if obs.witness_references != 1 {
        return Sample::NotYet;
    }
    obs.certified = true;
    obs.mismatches = mismatches;
    if obs.mismatches.is_empty() {
        Sample::Matched
    } else {
        Sample::NotYet
    }
}

/// Polls one member until a certified sweep matches (`observe.go`
/// awaitReplica + awaitUsage), or its window `end` passes. `end` is always
/// before the hook's deadline (`Windows::end_within`), so a member that never
/// matches returns its last certified sweep rather than a cut.
#[allow(clippy::too_many_arguments)]
fn await_usage(d: &Driver, client: usize, m: &Member, horizon: u64, witness: &str, wid: u64, expected: &BTreeMap<String, Expected>, end: Instant, deadline: Instant) -> Result<StatsObservation, Fail> {
    let mut obs = StatsObservation { node: m.addr.clone(), node_id: m.id, ..Default::default() };
    let end = end.min(deadline);
    match await_horizon(d, client, m, horizon, false, end, deadline) {
        Ok(_) => {}
        Err(Fail::Other(e)) => {
            obs.error = e;
            return Ok(obs);
        }
        Err(f) => return Err(f),
    }
    let route = Route::node(client, &m.addr);
    loop {
        obs.attempts += 1;
        match sample_usage(d, &route, witness, wid, expected, end, &mut obs) {
            Sample::Matched => {
                obs.converged = true;
                obs.error.clear();
                return Ok(obs);
            }
            Sample::Fatal(e) => {
                obs.fatal = true;
                obs.error = e;
                return Ok(obs);
            }
            Sample::NotYet => {}
        }
        if !pause_until(end) {
            if obs.error.is_empty() {
                obs.error = if obs.certified { "certified sweeps never matched within the window".into() } else { "the usage witness did not certify within the window".into() };
            }
            return Ok(obs);
        }
    }
}

fn apply_ok(d: &Driver, client: usize, source: &str, requests: Vec<Value>, deadline: Instant) -> Result<Value, Fail> {
    let left = remaining(deadline);
    if left < Duration::from_millis(500) {
        return Err(Fail::Cut);
    }
    let mut send = UntrackedSend::new(requests, Some(d.s.next_untracked_key()));
    send.schedule = vec![Route::node(client, source)];
    send.budget = send.budget.min(left);
    let r = d.send_untracked(send);
    match r.outcome.response.as_ref().filter(|_| r.outcome.is_ok()) {
        Some(v) => Ok(v.clone()),
        None if Instant::now() >= deadline => Err(Fail::Cut),
        None => Err(Fail::Other(format!("witness write: {}", describe(&r.outcome)))),
    }
}

/// A deterministic rank of `name` under `seed` (splitmix64 over FNV-1a).
fn sample_rank(seed: u64, name: &str) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in name.bytes() {
        h = (h ^ u64::from(b)).wrapping_mul(0x0100_0000_01b3);
    }
    let mut z = h ^ seed.wrapping_add(0x9e37_79b9_7f4a_7c15);
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

/// The foreign ledgers folded this attempt: every untracked ledger of the
/// tracked partition (reserved `o-<run>-own<N>` ledgers, any `o-` ledger the
/// model does not list: they are small) plus a `seed`-ranked sample of
/// `FOREIGN_SAMPLE` other foreign ledgers.
pub fn foreign_sample(seed: u64, partition: &[String], others: &[String]) -> Vec<String> {
    let mut ranked: Vec<&String> = others.iter().collect();
    ranked.sort_by_key(|n| (sample_rank(seed, n), (*n).clone()));
    let mut out: Vec<String> = partition.to_vec();
    out.extend(ranked.into_iter().take(FOREIGN_SAMPLE).cloned());
    out
}

/// A source `GetLedgerStats` (main-store `transactionCount`/`logCount` are
/// synchronous; DESIGN §7 C6).
fn source_stats(d: &Driver, src: &Route, name: &str, deadline: Instant) -> Result<Value, Fail> {
    let st = call_retry(d, src, "bucket", "GetLedgerStats", &json!({"ledger": name}), &CallOpts::default().timeout(RPC), deadline)?;
    st.response.clone().filter(|_| st.is_ok()).ok_or_else(|| Fail::Other(format!("source stats of {name}: {}", describe(&st))))
}

/// One stats attempt (`MD/eventually_stats_consistency/oracle.go` runAttempt).
#[allow(clippy::too_many_arguments)]
fn stats_attempt(d: &Driver, client: usize, cands: &[String], tracked: &BTreeMap<String, Tracked>, seed: u64, deadline: Instant, w: Windows, connected: &mut bool) -> Result<StatsReport, Fail> {
    let (source, _) = select_source(d, client, cands, deadline, connected)?;
    let src = Route::node(client, &source);
    let (members, _) = membership(d, client, &source, deadline)?;

    // A fresh witness ledger (foreign, Rust-owned).
    let n = d.s.ext::<ConvergenceState>().witness.fetch_add(1, Ordering::SeqCst) + 1;
    let witness = d.s.rust_ledger("wit", n);
    apply_ok(d, client, &source, vec![json!({"createLedger": {"name": witness}})], deadline)?;
    let o = call_retry(d, &src, "bucket", "GetLedger", &json!({"ledger": witness}), &CallOpts::default().timeout(RPC), deadline)?;
    let wid = o.response.as_ref().filter(|_| o.is_ok()).map(|v| u64_field(v, "id")).unwrap_or(0);
    if wid == 0 {
        return Err(Fail::Other(format!("usage witness has no ledger id: {}", describe(&o))));
    }

    // Source capture between two barriers (`source.go` captureSource).
    let before = barrier(d, &src, deadline)?;
    let rows = list_all(d, &src, "ListLedgers", json!({"options": {}}), &CallOpts::default().timeout(RPC), deadline)?;
    let prefix = d.s.tracked_prefix();
    let mut expected = BTreeMap::new();
    let mut seen = BTreeSet::new();
    let mut ids = BTreeMap::new();
    let (mut partition, mut others) = (Vec::new(), Vec::new());
    for row in &rows {
        let (name, id) = (str_field(row, "name").to_string(), u64_field(row, "id"));
        if name.is_empty() || id == 0 || !seen.insert(name.clone()) {
            return Err(Fail::Other(format!("invalid or duplicate ledger identity {name:?} id {id}")));
        }
        if row.get("deletedAt").is_some_and(|v| !v.is_null()) || name == witness {
            continue;
        }
        if let Some(t) = tracked.get(&name).filter(|_| name.starts_with(&prefix)) {
            // Tracked: exact counts from the oracle's quiescent model, checked
            // against the source inside the fence.
            let st = source_stats(d, &src, &name, deadline)?;
            let (txs, logs) = (u64_field(&st, "transactionCount"), u64_field(&st, "logCount"));
            if txs != t.tx || logs != t.counts.logs {
                return Err(Fail::Other(format!(
                    "tracked ledger {name}: source transactionCount {txs} logCount {logs}, the oracle's model at /quiet/begin txCount {} logCount {}",
                    t.tx, t.counts.logs
                )));
            }
            expected.insert(name, Expected { id, counts: t.counts });
            continue;
        }
        ids.insert(name.clone(), id);
        if name.starts_with(&prefix) {
            partition.push(name);
        } else {
            others.push(name);
        }
    }
    let sample = foreign_sample(seed, &partition, &others);
    let mut skipped = Vec::new();
    for name in sample {
        let st = source_stats(d, &src, &name, deadline)?;
        let main_logs = u64_field(&st, "logCount");
        if main_logs > FOLD_LOG_LIMIT {
            skipped.push(name);
            continue;
        }
        let items = list_all(d, &src, "ListLogs", json!({"ledger": name, "options": {}}), &CallOpts::default().timeout(RPC), deadline)?;
        let counts = fold_logs(&name, &items).map_err(|e| Fail::Other(format!("folding {name}: {e}")))?;
        if main_logs != counts.logs {
            return Err(Fail::Other(format!("incomplete source for {name}: folded {counts:?}, main logs {main_logs}")));
        }
        let id = ids[&name];
        expected.insert(name, Expected { id, counts });
    }
    if !skipped.is_empty() {
        println!("driver: stats_convergence: not folded (more than {FOLD_LOG_LIMIT} logs): {skipped:?}");
    }
    for name in tracked.keys() {
        if !expected.contains_key(name) {
            println!("driver: stats_convergence: tracked ledger {name} is not listed by the source; not observed");
        }
    }
    let after = barrier(d, &src, deadline)?;
    if after != before + 1 {
        return Err(Fail::Late(format!("source changed during capture: {before} -> {after}")));
    }
    if expected.is_empty() {
        return Err(Fail::Other("no source ledger to observe".into()));
    }

    // The witness reference: its audit entry follows every source log.
    let tx = json!({"apply": {"ledger": witness, "action": {"createTransaction": {
        "reference": witness, "force": true, "postings": [Posting::new("world", "witness", 1u64, "USD").to_json()],
    }}}});
    apply_ok(d, client, &source, vec![tx], deadline)?;
    let horizon = barrier(d, &src, deadline)?;
    if horizon != after + 2 {
        return Err(Fail::Late(format!("source changed during witness: {after} -> {horizon}")));
    }

    let end = w.end_within(deadline)?;
    let observations = first_fail(per_member(&members, |m| await_usage(d, client, m, horizon, &witness, wid, &expected, end, deadline)))?;
    let closing = barrier(d, &src, deadline)?;
    if closing != horizon + 1 {
        return Err(Fail::Late(format!("final source fence: {horizon} -> {closing}")));
    }
    Ok(StatsReport { horizon, witness, expected, observations })
}

/// The stats convergence hook body. `begin == None` (oracle skipped or
/// degraded) returns at once: not a finding.
pub fn stats_convergence(d: &Driver, client: usize, nodes: &NodeMap, begin: Option<&Value>, seed: u64, deadline: Instant, w: Windows) {
    let Some(begin) = begin else {
        println!("driver: stats_convergence skipped: no /quiet/begin");
        return;
    };
    let Some(tracked) = tracked_expectations(begin) else {
        println!("driver: stats_convergence skipped: /quiet/begin carries no providers.usage");
        return;
    };
    let cands = candidates(d, nodes);
    // Discovery answered (the old per-node dial), or a candidate answers below.
    let mut connected = !nodes.nodes.is_empty();
    let mut failures = Vec::new();
    let mut report = None;
    let mut cut = false;
    for _ in 0..ATTEMPTS {
        match stats_attempt(d, client, &cands, &tracked, seed, deadline, w, &mut connected) {
            Ok(r) => {
                report = Some(r);
                break;
            }
            Err(Fail::Cut) => {
                cut = true;
                break;
            }
            Err(f) => {
                println!("driver: stats_convergence attempt failed: {}", f.text());
                failures.push(f);
                if !pause_until(deadline) {
                    cut = true;
                    break;
                }
            }
        }
    }
    let completed = !cut && Instant::now() <= deadline;
    check_sometimes!(completed, "stats_convergence completes within its quiet budget", json!({"failures": failures}));
    if !completed {
        return;
    }
    check_always!(connected, "stats convergence oracle connects to replicas", json!({"candidates": cands, "failures": failures}));
    check_always!(qualified_verdict(report.is_some(), &failures), "stats oracle obtains a qualified observation within its bounded retry budget", json!({"attempts": failures}));
    if let Some(r) = &report {
        publish_stats(r);
    }
}

/// Usage verdicts only for members with a certified sweep (or a definitive
/// divergence); an uncertified member (the usage store still rebuilding
/// after a restart, a shortened window) is reported by the liveness
/// Sometimes, never as a usage finding.
pub fn publish_stats(r: &StatsReport) {
    for o in &r.observations {
        let details = json!({"node": o.node, "sourceIndex": r.horizon, "witness": r.witness, "expected": r.expected, "observation": o});
        let v = o.verdict();
        check_sometimes!(o.certified || o.fatal, "the usage witness certifies on a member within the stats window", details);
        let Some(converged) = v else {
            println!("driver: stats_convergence: member {} not certified: {}", o.node_id, o.error);
            continue;
        };
        check_always!(converged, "usage counters converge exactly at a fixed source horizon", details);
        if converged {
            check_reachable!("stats usage convergence verified", details);
        }
    }
}

fn run_stats(d: &Driver, ctx: &mut QuietCtx, plan: &HookPlan) {
    let client = plan_of(d, ctx, plan);
    let begin = ctx.begin.clone();
    stats_convergence(d, client, &ctx.nodes, begin.as_ref(), seed_of(plan), ctx.deadline, STATS_WINDOWS);
}

// ---------------------------------------------------------------- cluster config convergence

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ConfigReport {
    pub horizon: u64,
    pub leader: u64,
    /// member id → its own `clusterConfig`.
    pub configs: BTreeMap<u64, Value>,
}

/// Whether a `clusterConfig` answer is the persisted one. With no
/// ClusterConfig ever persisted, `GetClusterState` falls back to
/// `{rotationThreshold: <local cache threshold>}`
/// (`internal/adapter/grpc/server_cluster.go:155-160`), which differs per node
/// on purpose (DESIGN §8); the persisted config always carries the bloom
/// fields too (`proposeClusterConfigIfNeeded`).
pub fn config_persisted(c: &Value) -> bool {
    c.as_object().is_some_and(|o| o.keys().any(|k| k != "rotationThreshold"))
}

/// Members whose `clusterConfig` differs from the leader's.
pub fn config_mismatches(r: &ConfigReport) -> Vec<u64> {
    let lead = r.configs.get(&r.leader);
    r.configs.iter().filter(|(_, c)| Some(*c) != lead).map(|(id, _)| *id).collect()
}

fn config_attempt(d: &Driver, client: usize, cands: &[String], deadline: Instant) -> Result<ConfigReport, Fail> {
    let (source, _) = select_source(d, client, cands, deadline, &mut false)?;
    let src = Route::node(client, &source);
    let (members, leader) = membership(d, client, &source, deadline)?;
    if !members.iter().any(|m| m.id == leader) {
        return Err(Fail::Late(format!("leader {leader} is not a member")));
    }
    let horizon = barrier(d, &src, deadline)?;
    let states = first_fail(per_member(&members, |m| await_horizon(d, client, m, horizon, false, deadline, deadline)))?;
    let configs = members.iter().zip(states).map(|(m, cs)| (m.id, cs.get("clusterConfig").cloned().unwrap_or(Value::Null))).collect();
    let (after_members, after_leader) = membership(d, client, &source, deadline)?;
    if after_members != members || after_leader != leader {
        return Err(Fail::Late("membership or leadership changed during observation".into()));
    }
    let after = barrier(d, &src, deadline)?;
    if after != horizon + 1 {
        return Err(Fail::Late(format!("final source fence: {horizon} -> {after}")));
    }
    Ok(ConfigReport { horizon, leader, configs })
}

/// The cluster-config convergence hook body.
pub fn cluster_config_convergence(d: &Driver, client: usize, nodes: &NodeMap, deadline: Instant) {
    let cands = candidates(d, nodes);
    let mut report = None;
    let mut failures = Vec::new();
    let mut cut = false;
    for _ in 0..ATTEMPTS {
        match config_attempt(d, client, &cands, deadline) {
            Ok(r) => {
                report = Some(r);
                break;
            }
            Err(Fail::Cut) => {
                cut = true;
                break;
            }
            Err(f) => {
                println!("driver: cluster_config_convergence attempt failed: {}", f.text());
                failures.push(f);
                if !pause_until(deadline) {
                    cut = true;
                    break;
                }
            }
        }
    }
    let completed = !cut && Instant::now() <= deadline;
    check_sometimes!(completed, "cluster_config_convergence completes within its quiet budget", json!({"failures": failures}));
    if !completed {
        return;
    }
    let Some(r) = report else {
        println!("driver: cluster_config_convergence: no qualified observation: {failures:?}");
        return;
    };
    let lead = r.configs.get(&r.leader).cloned().unwrap_or(Value::Null);
    if !config_persisted(&lead) {
        // The leader never persisted a ClusterConfig: every node answers its
        // own local fallback, which is not a replicated state to compare.
        println!("driver: cluster_config_convergence: the leader's cluster config is not persisted ({lead}); not compared");
        return;
    }
    publish_config(d, &r);
}

pub fn publish_config(d: &Driver, r: &ConfigReport) {
    let lead = r.configs.get(&r.leader).cloned().unwrap_or(Value::Null);
    for (id, c) in &r.configs {
        check_always!(*c == lead, "every node's cluster config equals the leader's at a common horizon", json!({"index": r.horizon, "leader": r.leader, "node": id, "leaderConfig": lead, "nodeConfig": c}));
    }
    if config_mismatches(r).is_empty() {
        check_reachable!("cluster config convergence verified", json!({"index": r.horizon, "nodes": r.configs.len()}));
    }
    let st = d.s.ext::<ConvergenceState>();
    let mut last = st.last_config.lock().unwrap_or_else(|e| e.into_inner());
    let changed = last.as_ref().is_some_and(|p| *p != lead);
    check_sometimes!(changed, "a cluster config change was observed between quiet periods", json!({"previous": *last, "current": lead}));
    *last = Some(lead);
}

fn run_cluster_config(d: &Driver, ctx: &mut QuietCtx, plan: &HookPlan) {
    let client = plan_of(d, ctx, plan);
    cluster_config_convergence(d, client, &ctx.nodes, ctx.deadline);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testkit::{self, FakeReply};

    fn ok(v: Value) -> FakeReply {
        FakeReply::Outcome(Outcome::from_json(&json!({"ok": true, "response": v})))
    }
    fn items(v: Vec<Value>) -> FakeReply {
        FakeReply::Outcome(Outcome::from_json(&json!({"ok": true, "items": v, "complete": true})))
    }
    fn barriers(ns: &[u64]) -> Vec<FakeReply> {
        ns.iter().map(|n| ok(json!({"commitIndex": n.to_string()}))).collect()
    }
    /// A one-member cluster state (node 1 at ledger-0), as every node answers.
    fn cs1(persisted: u64, config: Value) -> Value {
        json!({"localNode": "1", "leader": "1", "state": "Leader",
               "nodes": [{"id": "1", "serviceAddress": "ledger-0:8888", "suffrage": "Voter"}],
               "raftStatus": {"lastPersistedIndex": persisted.to_string()}, "syncProgress": {"status": "normal"},
               "clusterConfig": config})
    }
    fn nodes1() -> NodeMap {
        NodeMap::from_cluster_state(&cs1(0, json!({})))
    }
    fn short() -> Windows {
        Windows { convergence: Duration::from_millis(800), reserve: Duration::from_millis(100) }
    }
    fn soon() -> Instant {
        Instant::now() + Duration::from_secs(15)
    }
    fn sections() -> Value {
        json!({
            "ledgers": {"count": 2, "sha256": "l", "lastKey": "b"},
            "accounts": {"count": 3, "sha256": "a", "lastKey": "b/x", "ledgers": {"a": {"count": 1, "sha256": "a1", "lastKey": "a/w"}, "b": {"count": 2, "sha256": "b1", "lastKey": "b/x"}}},
            "audit": {"count": 7, "sha256": "h", "lastKey": "7"},
        })
    }
    fn audit_fakes(ft: &testkit::FakeTransport) {
        ft.set_default("bucket", "ListAuditEntries", items(vec![json!({"sequence": "7", "hashVersion": 1, "hash": "eA=="})]));
        ft.set_default("bucket", "GetAuditEntry", ok(json!({"sequence": "7", "hashVersion": 1, "hash": "eA==", "items": [{"x": 1}]})));
    }
    fn hit(m: &str) -> Option<crate::assertions::Recorded> {
        testkit::assertions().into_iter().find(|a| a.message == m)
    }
    fn always_count() -> usize {
        testkit::assertions().iter().filter(|a| a.kind == "Always").count()
    }

    #[test]
    fn hooks_are_registered_with_the_design_budgets() {
        let hooks = quiet_hooks();
        let got: Vec<(&str, u64, bool)> = hooks.iter().map(|h| (h.name, h.budget.as_secs(), h.needs_oracle)).collect();
        assert_eq!(got, vec![("cross_node_identity", 90, false), ("stats_convergence", 70, true), ("cluster_config_convergence", 20, false)]);
        let (d, _, _) = testkit::driver();
        assert_eq!(hooks.iter().map(|h| (h.weight)(&d.s)).collect::<Vec<_>>(), vec![1.0, 1.0, 0.5]);
        let names: Vec<&str> = crate::rules::quiet_hooks().iter().map(|h| h.name).collect();
        for n in ["cross_node_identity", "stats_convergence", "cluster_config_convergence"] {
            assert!(names.contains(&n), "{n} registered in rules/mod.rs");
        }
    }

    #[test]
    fn membership_validation() {
        let m = parse_membership(&json!({"nodes": [
            {"id": "3", "serviceAddress": "ledger-2:8888", "suffrage": "Learner"},
            {"id": "1", "serviceAddress": "ledger-0:8888", "suffrage": "Voter"}]}))
        .unwrap();
        assert_eq!(m.iter().map(|m| m.id).collect::<Vec<_>>(), vec![1, 3]);
        for bad in [
            json!({"nodes": []}),
            json!({}),
            json!({"nodes": [{"id": "0", "serviceAddress": "a", "suffrage": "Voter"}]}),
            json!({"nodes": [{"id": "1", "serviceAddress": "", "suffrage": "Voter"}]}),
            json!({"nodes": [{"id": "1", "serviceAddress": "a", "suffrage": "Witness"}]}),
            json!({"nodes": [{"id": "1", "serviceAddress": "a", "suffrage": "Voter"}, {"id": "1", "serviceAddress": "b", "suffrage": "Voter"}]}),
            json!({"nodes": [{"id": "1", "serviceAddress": "a", "suffrage": "Voter"}, {"id": "2", "serviceAddress": "a", "suffrage": "Voter"}]}),
        ] {
            assert!(parse_membership(&bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn qualified_verdict_is_false_only_when_every_attempt_failed_otherwise() {
        assert!(qualified_verdict(true, &[Fail::Other("x".into())]));
        assert!(qualified_verdict(false, &[Fail::Other("x".into()), Fail::Late("w".into())]));
        assert!(!qualified_verdict(false, &[Fail::Other("x".into()), Fail::Other("y".into()), Fail::Other("z".into())]));
    }

    #[test]
    fn first_difference_names_the_first_differing_ledger() {
        let a = sections();
        assert_eq!(first_difference(&a, &a), None);
        let mut b = sections();
        b["accounts"]["ledgers"]["b"]["sha256"] = json!("zz");
        b["accounts"]["sha256"] = json!("other");
        let diff = first_difference(&a, &b).unwrap();
        assert_eq!((diff["section"].as_str(), diff["ledger"].as_str()), (Some("accounts"), Some("b")));
        assert_eq!(diff["observation"]["sha256"], "zz");
        // A ledger present on one side only.
        let mut c = sections();
        c["accounts"]["ledgers"].as_object_mut().unwrap().remove("a");
        assert_eq!(first_difference(&a, &c).unwrap()["ledger"], "a");
        // The ledger listing differs first.
        let mut e = sections();
        e["ledgers"]["count"] = json!(3);
        assert_eq!(first_difference(&a, &e).unwrap()["section"], "ledgers");
    }

    fn tobs(tail: Option<Value>, listed_last: Option<Value>, tip: Option<Value>) -> TailObs {
        TailObs { tail, listed_last, tip_present: tip.is_some(), tip_full: tip }
    }
    fn obs_tip(id: u64, s: Value, version: u64, tip: Value) -> Observation {
        let tail = json!({"sequence": "7", "hashVersion": version});
        Observation { member: Member { id, addr: format!("ledger-{}:8888", id - 1), suffrage: "Voter".into() }, audit: audit_obs(1, &s, tobs(Some(tail.clone()), Some(tail), Some(tip))), sections: s }
    }
    fn obs(id: u64, s: Value, version: u64) -> Observation {
        obs_tip(id, s, version, json!({"sequence": "7", "hashVersion": version}))
    }

    #[test]
    fn publish_reports_account_and_audit_differences() {
        testkit::reset();
        let mut s2 = sections();
        s2["accounts"]["ledgers"]["a"]["count"] = json!(9);
        let r = CrossReport { horizon: 11, members: vec![], observations: vec![obs(1, sections(), 1), obs(2, sections(), 1), obs(3, s2, 2)] };
        publish_cross(&r);
        let a = testkit::assertions();
        let acc: Vec<bool> = a.iter().filter(|x| x.message == "account state is identical across nodes at a common applied index").map(|x| x.cond).collect();
        assert_eq!(acc, vec![true, false]);
        let bad = a.iter().find(|x| x.message == "account state is identical across nodes at a common applied index" && !x.cond).unwrap();
        assert_eq!(bad.details["firstDifference"]["ledger"], "a");
        let ver: Vec<bool> = a.iter().filter(|x| x.message == "audit hash version is identical across nodes at the same sequence").map(|x| x.cond).collect();
        assert_eq!(ver, vec![true, false]);
        assert!(hit("at least two nodes converge to a common applied index").unwrap().cond);
        assert!(hit("cross-node complete account and audit identity verified").is_none());

        // Audit digests differ at the same tail.
        testkit::reset();
        let mut s3 = sections();
        s3["audit"]["sha256"] = json!("forked");
        publish_cross(&CrossReport { horizon: 11, members: vec![], observations: vec![obs(1, sections(), 1), obs(2, s3, 1)] });
        assert!(!hit("audit hash is identical across nodes at the same sequence").unwrap().cond);

        // A listing that does not end at the tail.
        testkit::reset();
        let mut s4 = sections();
        s4["audit"]["lastKey"] = json!("6");
        publish_cross(&CrossReport { horizon: 11, members: vec![], observations: vec![obs(1, s4, 1)] });
        assert!(!hit("complete audit listing and point tip agree at a common applied index").unwrap().cond);

        // Same headers, different tip items: the tip is compared items included.
        testkit::reset();
        let tip = |x: u64| json!({"sequence": "7", "hashVersion": 1, "items": [{"x": x}]});
        publish_cross(&CrossReport { horizon: 11, members: vec![], observations: vec![obs_tip(1, sections(), 1, tip(1)), obs_tip(2, sections(), 1, tip(2))] });
        assert!(hit("complete audit listing and point tip agree at a common applied index").unwrap().cond, "stripped tips agree");
        assert!(!hit("audit hash is identical across nodes at the same sequence").unwrap().cond);
    }

    #[test]
    fn audit_tail_and_tip_logic() {
        let s = sections();
        let t = json!({"sequence": "7", "hashVersion": 2});
        let a = audit_obs(1, &s, tobs(Some(t.clone()), Some(t.clone()), Some(t.clone())));
        assert!(a.tail_matches && a.tip_present && a.version == 2 && a.tail_seq == 7);
        let a = audit_obs(1, &s, tobs(Some(t.clone()), Some(t.clone()), Some(json!({"sequence": "7", "hashVersion": 2, "items": [1]}))));
        assert!(a.tail_matches, "the tip is stripped before the header comparison");
        let a = audit_obs(1, &s, tobs(Some(t.clone()), Some(t.clone()), Some(json!({"sequence": "7", "hashVersion": 3}))));
        assert!(!a.tail_matches, "tip differs from tail");
        let a = audit_obs(1, &s, tobs(Some(t.clone()), Some(json!({"sequence": "7", "hashVersion": 2, "hash": "other"})), Some(t.clone())));
        assert!(!a.tail_matches, "the listing's last entry differs from the tail");
        let a = audit_obs(1, &s, tobs(Some(t.clone()), None, Some(t.clone())));
        assert!(!a.tail_matches, "the listing has no entry at the tail");
        let a = audit_obs(1, &s, tobs(Some(t.clone()), Some(t.clone()), None));
        assert!(a.tail_matches && !a.tip_present, "absent tip is an observation");
        let mut empty = sections();
        empty["audit"] = json!({"count": 0, "sha256": "e", "lastKey": ""});
        assert!(audit_obs(1, &empty, TailObs::default()).tail_matches);
        assert!(!audit_obs(1, &s, TailObs::default()).tail_matches, "listing has entries, tail does not");
    }

    #[test]
    fn cross_node_identity_verifies_one_member() {
        let (d, ft, _) = testkit::driver();
        testkit::reset();
        ft.set_default("cluster", "GetClusterState", ok(cs1(11, json!({}))));
        ft.script("bucket", "Barrier", barriers(&[10, 11, 12]));
        audit_fakes(&ft);
        let bodies = Mutex::new(Vec::new());
        let dig = |_: &Driver, _: usize, b: &Value| -> Result<Value, String> {
            bodies.lock().unwrap().push(b.clone());
            Ok(sections())
        };
        cross_node_identity(&d, 1, &nodes1(), soon(), short(), &dig);
        for m in [
            "cross_node_identity completes within its quiet budget",
            "cross-node identity oracle connects to replicas",
            "cross-node identity oracle obtains a qualified all-member observation",
            "cross-node oracle reaches quiescence",
            "all current replicas reach the same durable identity horizon",
            "complete audit listing and point tip agree at a common applied index",
            "cross-node complete account and audit identity verified",
        ] {
            assert!(hit(m).is_some_and(|a| a.cond), "{m}");
        }
        assert!(!hit("at least two nodes converge to a common applied index").unwrap().cond);
        let b = bodies.lock().unwrap().clone();
        assert_eq!(b.len(), 1);
        assert_eq!((b[0]["node"].as_str(), b[0]["consistency"].as_str(), b[0]["auditFrom"].as_str()), (Some("ledger-0:8888"), Some("stale"), Some("1")));
        assert_eq!(b[0]["sections"], json!(["ledgers", "accounts", "audit"]));
        // Every per-node call is pinned and per-node cluster states name the node.
        let tails = ft.calls_of("bucket", "ListAuditEntries");
        assert_eq!((tails[0]["node"].as_str(), tails[0]["consistency"].as_str()), (Some("ledger-0:8888"), Some("stale")));
        assert_eq!(tails[0]["request"], json!({"options": {"pageSize": 1, "reverse": true}}));
        assert_eq!(ft.calls_of("bucket", "GetAuditEntry")[0]["request"], json!({"sequence": "7"}));
        assert!(ft.calls_of("cluster", "GetClusterState").iter().any(|c| c["request"] == json!({"nodeId": "1"})));
        assert!(ft.calls_of("bucket", "Barrier").iter().all(|c| c["node"] == "ledger-0:8888"));
    }

    #[test]
    fn audit_window_is_bounded() {
        let (d, ft, _) = testkit::driver();
        ft.set_default("cluster", "GetClusterState", ok(cs1(11, json!({}))));
        ft.script("bucket", "Barrier", barriers(&[10, 11, 12]));
        ft.set_default("bucket", "ListAuditEntries", items(vec![json!({"sequence": "12345", "hashVersion": 1})]));
        ft.set_default("bucket", "GetAuditEntry", ok(json!({"sequence": "12345", "hashVersion": 1})));
        let from = Mutex::new(String::new());
        let dig = |_: &Driver, _: usize, b: &Value| -> Result<Value, String> {
            *from.lock().unwrap() = b["auditFrom"].as_str().unwrap().to_string();
            Ok(sections())
        };
        cross_node_identity(&d, 0, &nodes1(), soon(), short(), &dig);
        assert_eq!(*from.lock().unwrap(), (12345 - AUDIT_WINDOW).to_string());
    }

    #[test]
    fn persisted_above_the_horizon_invalidates_the_attempt() {
        let (d, ft, _) = testkit::driver();
        testkit::reset();
        // Every member already persisted past the horizon (a late write).
        ft.set_default("cluster", "GetClusterState", ok(cs1(99, json!({}))));
        ft.script("bucket", "Barrier", barriers(&[10, 11, 20, 21, 30, 31]));
        audit_fakes(&ft);
        let dig = |_: &Driver, _: usize, _: &Value| -> Result<Value, String> { panic!("no digest after an invalid horizon") };
        cross_node_identity(&d, 0, &nodes1(), soon(), short(), &dig);
        assert!(hit("cross-node identity oracle obtains a qualified all-member observation").unwrap().cond, "late writes are not a finding");
        assert!(hit("cross-node oracle reaches quiescence").is_none());
        assert!(hit("account state is identical across nodes at a common applied index").is_none());
        assert_eq!(ft.calls_of("bucket", "Barrier").len(), 6, "three attempts");

        // A barrier that moved by more than one also invalidates.
        let (d, ft, _) = testkit::driver();
        testkit::reset();
        ft.set_default("cluster", "GetClusterState", ok(cs1(13, json!({}))));
        ft.script("bucket", "Barrier", barriers(&[10, 13, 20, 22, 30, 33]));
        cross_node_identity(&d, 0, &nodes1(), soon(), short(), &dig);
        assert!(hit("cross-node identity oracle obtains a qualified all-member observation").unwrap().cond);
        assert!(hit("cross-node oracle reaches quiescence").is_none());
    }

    #[test]
    fn a_closing_fence_that_moved_invalidates_the_attempt() {
        let (d, ft, _) = testkit::driver();
        testkit::reset();
        ft.set_default("cluster", "GetClusterState", ok(cs1(11, json!({}))));
        ft.script("bucket", "Barrier", barriers(&[10, 11, 14]));
        ft.set_default("bucket", "Barrier", ok(json!({"commitIndex": "50"})));
        audit_fakes(&ft);
        let dig = |_: &Driver, _: usize, _: &Value| -> Result<Value, String> { Ok(sections()) };
        cross_node_identity(&d, 0, &nodes1(), soon(), short(), &dig);
        assert!(hit("cross-node identity oracle obtains a qualified all-member observation").unwrap().cond);
        assert!(hit("cross-node oracle reaches quiescence").is_none(), "verdicts buffered until both fences pass");
    }

    #[test]
    fn every_attempt_failing_otherwise_is_a_finding() {
        let (d, ft, _) = testkit::driver();
        testkit::reset();
        ft.set_default("cluster", "GetClusterState", ok(cs1(11, json!({}))));
        ft.script("bucket", "Barrier", barriers(&[10, 11, 20, 21, 30, 31]));
        audit_fakes(&ft);
        let dig = |_: &Driver, _: usize, _: &Value| -> Result<Value, String> { Err("boom".into()) };
        cross_node_identity(&d, 0, &nodes1(), soon(), short(), &dig);
        assert!(hit("cross-node identity oracle connects to replicas").unwrap().cond);
        assert!(!hit("cross-node identity oracle obtains a qualified all-member observation").unwrap().cond);
    }

    #[test]
    fn a_deadline_stop_evaluates_no_always() {
        let (d, ft, _) = testkit::driver();
        testkit::reset();
        ft.set_default("cluster", "GetClusterState", ok(cs1(11, json!({}))));
        let dig = |_: &Driver, _: usize, _: &Value| -> Result<Value, String> { Ok(sections()) };
        cross_node_identity(&d, 0, &nodes1(), Instant::now(), short(), &dig);
        assert!(!hit("cross_node_identity completes within its quiet budget").unwrap().cond);
        stats_convergence(&d, 0, &nodes1(), Some(&usage_begin()), 0, Instant::now(), short());
        assert!(!hit("stats_convergence completes within its quiet budget").unwrap().cond);
        cluster_config_convergence(&d, 0, &nodes1(), Instant::now());
        assert!(!hit("cluster_config_convergence completes within its quiet budget").unwrap().cond);
        assert_eq!(always_count(), 0, "{:?}", testkit::assertions());

        // A member that never reaches the horizon before the budget ends is
        // also a cut, not a liveness failure.
        let (d, ft, _) = testkit::driver();
        testkit::reset();
        ft.set_default("cluster", "GetClusterState", ok(cs1(5, json!({}))));
        ft.script("bucket", "Barrier", barriers(&[10, 11]));
        let long = Windows { convergence: Duration::from_secs(60), reserve: Duration::ZERO };
        cross_node_identity(&d, 0, &nodes1(), Instant::now() + Duration::from_millis(900), long, &dig);
        assert!(!hit("cross_node_identity completes within its quiet budget").unwrap().cond);
        assert_eq!(always_count(), 0);
    }

    fn usage_begin() -> Value {
        json!({"providers": {"usage": {"o-test-l1": {"id": 5, "logCount": 3, "txCount": 2, "postingCount": 2, "referenceCount": 1, "revertCount": 0}}}})
    }
    fn stats(refs: u64, logs: u64, postings: u64, reverts: u64) -> FakeReply {
        ok(json!({"transactionCount": "2", "referenceCount": refs.to_string(), "logCount": logs.to_string(), "postingCount": postings.to_string(), "revertCount": reverts.to_string()}))
    }
    fn stats_fakes(ft: &testkit::FakeTransport) {
        ft.set_default("cluster", "GetClusterState", ok(cs1(100, json!({}))));
        ft.script("bucket", "Barrier", barriers(&[10, 11, 12, 14, 15]));
        ft.set_default("bucket", "GetLedger", ok(json!({"name": "x", "id": 5})));
        ft.set_default("bucket", "ListLedgers", items(vec![json!({"name": "o-test-l1", "id": 5}), json!({"name": "o-test-l9", "id": 9, "deletedAt": {"data": "1"}})]));
        ft.set_default("bucket", "GetLedgerStats", stats(1, 3, 2, 0));
    }

    #[test]
    fn stats_convergence_certifies_with_a_witness() {
        let (d, ft, _) = testkit::driver();
        testkit::reset();
        stats_fakes(&ft);
        stats_convergence(&d, 2, &nodes1(), Some(&usage_begin()), 0, soon(), short());
        for m in [
            "stats_convergence completes within its quiet budget",
            "stats convergence oracle connects to replicas",
            "stats oracle obtains a qualified observation within its bounded retry budget",
            "usage counters converge exactly at a fixed source horizon",
            "stats usage convergence verified",
        ] {
            assert!(hit(m).is_some_and(|a| a.cond), "{m}: {:?}", testkit::assertions());
        }
        let applies = ft.calls_of("bucket", "Apply");
        assert_eq!(applies.len(), 2);
        assert_eq!(applies[0]["request"]["unsigned"]["requests"][0], json!({"createLedger": {"name": "r-wit-test-1"}}));
        assert!(applies[0]["request"]["unsigned"]["idempotencyKey"].as_str().unwrap().starts_with("r-test-k"));
        let ct = &applies[1]["request"]["unsigned"]["requests"][0]["apply"];
        assert_eq!((ct["ledger"].as_str(), ct["action"]["createTransaction"]["reference"].as_str(), ct["action"]["createTransaction"]["force"].as_bool()), (Some("r-wit-test-1"), Some("r-wit-test-1"), Some(true)));
        assert!(applies.iter().all(|a| a["node"] == "ledger-0:8888"));
        // Member reads are stale and pinned; the deleted ledger is not observed.
        let st = ft.calls_of("bucket", "GetLedgerStats");
        assert!(st.iter().filter(|c| c["consistency"] == "stale").all(|c| c["node"] == "ledger-0:8888"));
        assert!(st.iter().all(|c| c["request"]["ledger"] != "o-test-l9"));
        assert!(st.iter().any(|c| c["request"]["ledger"] == "o-test-l1" && c["consistency"] == "stale"));
        let obs = &hit("usage counters converge exactly at a fixed source horizon").unwrap().details;
        assert_eq!(obs["sourceIndex"], 14);
        assert_eq!(obs["observation"]["attempts"], 1);
    }

    #[test]
    fn a_rewound_witness_forces_another_sweep() {
        let (d, ft, _) = testkit::driver();
        testkit::reset();
        stats_fakes(&ft);
        // source check of l1, then witness ok, l1 ok, witness rewound → next sweep.
        ft.script("bucket", "GetLedgerStats", vec![stats(1, 3, 2, 0), stats(1, 3, 2, 0), stats(1, 3, 2, 0), stats(0, 0, 0, 0)]);
        stats_convergence(&d, 0, &nodes1(), Some(&usage_begin()), 0, soon(), short());
        let a = hit("usage counters converge exactly at a fixed source horizon").unwrap();
        assert!(a.cond);
        assert_eq!(a.details["observation"]["attempts"], 2);
    }

    #[test]
    fn diverging_usage_is_a_finding() {
        let (d, ft, _) = testkit::driver();
        testkit::reset();
        stats_fakes(&ft);
        ft.set_default("bucket", "GetLedgerStats", stats(1, 3, 9, 0));
        stats_convergence(&d, 0, &nodes1(), Some(&usage_begin()), 0, soon(), short());
        let a = hit("usage counters converge exactly at a fixed source horizon").unwrap();
        assert!(!a.cond);
        assert_eq!(a.details["observation"]["mismatches"]["o-test-l1"]["postings"], 9);
        assert!(hit("stats usage convergence verified").is_none());
        assert!(hit("stats oracle obtains a qualified observation within its bounded retry budget").unwrap().cond);
    }

    #[test]
    fn a_witness_that_never_certifies_is_not_a_usage_finding() {
        let (d, ft, _) = testkit::driver();
        testkit::reset();
        stats_fakes(&ft);
        ft.set_default("bucket", "GetLedgerStats", stats(0, 3, 2, 0));
        stats_convergence(&d, 0, &nodes1(), Some(&usage_begin()), 0, soon(), short());
        assert!(hit("usage counters converge exactly at a fixed source horizon").is_none(), "{:?}", testkit::assertions());
        let a = hit("the usage witness certifies on a member within the stats window").unwrap();
        assert!(!a.cond);
        assert_eq!(a.details["observation"]["witnessReferences"], 0);
        assert!(a.details["observation"]["mismatches"].as_object().unwrap().is_empty(), "never compared without the certificate");
        assert!(hit("stats oracle obtains a qualified observation within its bounded retry budget").unwrap().cond);
    }

    #[test]
    fn a_mismatching_certified_sweep_is_a_finding_even_when_the_budget_clips_the_window() {
        let (d, ft, _) = testkit::driver();
        testkit::reset();
        stats_fakes(&ft);
        ft.set_default("bucket", "GetLedgerStats", stats(1, 3, 9, 0));
        // The full 60 s window does not fit in the budget: the window is
        // clipped to the budget minus the reserve, not stretched to the deadline.
        let w = Windows { convergence: Duration::from_secs(60), reserve: Duration::from_millis(500) };
        let t0 = Instant::now();
        stats_convergence(&d, 0, &nodes1(), Some(&usage_begin()), 0, t0 + Duration::from_secs(3), w);
        assert!(hit("stats_convergence completes within its quiet budget").unwrap().cond, "{:?}", testkit::assertions());
        let a = hit("usage counters converge exactly at a fixed source horizon").unwrap();
        assert!(!a.cond);
        assert_eq!(a.details["observation"]["certified"], true);
        assert!(Instant::now() < t0 + Duration::from_secs(3));

        // Not even MIN_WINDOW left after the reserve: a cut, no verdict.
        testkit::reset();
        let (d, ft, _) = testkit::driver();
        stats_fakes(&ft);
        let w = Windows { convergence: Duration::from_secs(60), reserve: Duration::from_secs(30) };
        stats_convergence(&d, 0, &nodes1(), Some(&usage_begin()), 0, Instant::now() + Duration::from_secs(5), w);
        assert!(!hit("stats_convergence completes within its quiet budget").unwrap().cond);
        assert_eq!(always_count(), 0);
    }

    #[test]
    fn tracked_counts_disagreeing_with_the_source_fail_the_attempt_not_usage() {
        let (d, ft, _) = testkit::driver();
        testkit::reset();
        stats_fakes(&ft);
        ft.script("bucket", "Barrier", barriers(&[20, 21, 30, 31]));
        // The source holds a late commit the model did not see at /quiet/begin.
        ft.set_default("bucket", "GetLedgerStats", ok(json!({"transactionCount": "3", "logCount": "4", "referenceCount": "1", "postingCount": "2", "revertCount": "0"})));
        stats_convergence(&d, 0, &nodes1(), Some(&usage_begin()), 0, soon(), short());
        assert!(hit("usage counters converge exactly at a fixed source horizon").is_none());
        let q = hit("stats oracle obtains a qualified observation within its bounded retry budget").unwrap();
        assert!(!q.cond, "three Other attempts");
        assert!(q.details["attempts"][0].to_string().contains("txCount 2"), "{}", q.details);
        let src = ft.calls_of("bucket", "GetLedgerStats");
        assert!(src.iter().any(|c| c["request"]["ledger"] == "o-test-l1" && c["consistency"] != "stale"));
    }

    #[test]
    fn untracked_partition_ledgers_are_folded() {
        let (d, ft, _) = testkit::driver();
        testkit::reset();
        stats_fakes(&ft);
        ft.set_default("bucket", "ListLedgers", items(vec![json!({"name": "o-test-l1", "id": 5}), json!({"name": "o-test-own1", "id": 5})]));
        let p = json!({"source": "world", "destination": "a", "asset": "USD", "amount": "1"});
        let log = |id: u64| json!({"payload": {"apply": {"ledgerName": "o-test-own1", "log": {"id": id.to_string(), "data": {"createdTransaction": {"transaction": {"postings": [p]}}}}}}});
        ft.set_default("bucket", "ListLogs", items(vec![log(1), log(2), log(3)]));
        ft.set_default("bucket", "GetLedgerStats", stats(1, 3, 3, 0));
        let begin = json!({"providers": {"usage": {"o-test-l1": {"id": 5, "logCount": 3, "txCount": 2, "postingCount": 3, "revertCount": 0}}}});
        stats_convergence(&d, 0, &nodes1(), Some(&begin), 0, soon(), short());
        let a = hit("usage counters converge exactly at a fixed source horizon").unwrap();
        assert!(a.cond, "{:?}", testkit::assertions());
        assert_eq!(a.details["expected"]["o-test-own1"]["counts"]["logs"], 3);
        assert_eq!(ft.calls_of("bucket", "ListLogs").iter().map(|c| c["request"]["ledger"].clone()).collect::<Vec<_>>(), vec![json!("o-test-own1")]);
    }

    #[test]
    fn the_foreign_fold_is_bounded() {
        let others: Vec<String> = (0..20).map(|i| format!("r-x-{i}")).collect();
        let part = vec!["o-test-own1".to_string()];
        let a = foreign_sample(7, &part, &others);
        assert_eq!(a.len(), 1 + FOREIGN_SAMPLE);
        assert_eq!(a[0], "o-test-own1");
        assert_eq!(a, foreign_sample(7, &part, &others), "deterministic for a seed");
        assert_ne!(a, foreign_sample(8, &part, &others), "the seed rotates the sample");
        assert_eq!(foreign_sample(1, &[], &others[..3]).len(), 3);

        // A ledger with too many logs is not folded.
        let (d, ft, _) = testkit::driver();
        testkit::reset();
        stats_fakes(&ft);
        ft.set_default("bucket", "ListLedgers", items(vec![json!({"name": "o-test-l1", "id": 5}), json!({"name": "r-stress-x", "id": 6})]));
        ft.set_default("bucket", "GetLedgerStats", ok(json!({"transactionCount": "2", "logCount": "3", "referenceCount": "1", "postingCount": "2", "revertCount": "0"})));
        ft.script("bucket", "GetLedgerStats", vec![stats(1, 3, 2, 0), ok(json!({"logCount": (FOLD_LOG_LIMIT + 1).to_string()}))]);
        stats_convergence(&d, 0, &nodes1(), Some(&usage_begin()), 0, soon(), short());
        assert!(ft.calls_of("bucket", "ListLogs").is_empty());
        let a = hit("usage counters converge exactly at a fixed source horizon").unwrap();
        assert!(a.cond);
        assert!(a.details["expected"].get("r-stress-x").is_none());
    }

    #[test]
    fn stats_without_quiet_begin_returns_quietly() {
        let (d, ft, _) = testkit::driver();
        testkit::reset();
        stats_convergence(&d, 0, &nodes1(), None, 0, soon(), short());
        stats_convergence(&d, 0, &nodes1(), Some(&json!({"providers": {}})), 0, soon(), short());
        assert!(ft.calls().is_empty());
        assert!(testkit::assertions().is_empty());
    }

    #[test]
    fn foreign_ledgers_are_folded_from_their_logs() {
        let log = |id: u64, data: Value| json!({"sequence": (id + 100).to_string(), "payload": {"apply": {"ledgerName": "r-ns-x", "log": {"id": id.to_string(), "data": data}}}});
        let p = json!({"source": "world", "destination": "a", "asset": "USD", "amount": "1"});
        let its = vec![
            log(1, json!({"createdTransaction": {"transaction": {"postings": [p, p]}}})),
            log(2, json!({"savedMetadata": {"target": {}}})),
            log(3, json!({"revertedTransaction": {"revertedTransactionId": "1", "revertTransaction": {"postings": [p]}}})),
        ];
        assert_eq!(fold_logs("r-ns-x", &its), Ok(Counts { logs: 3, postings: 3, reverts: 1 }));
        assert!(fold_logs("r-ns-y", &its).is_err(), "foreign ledger");
        assert!(fold_logs("r-ns-x", &[its[0].clone(), its[2].clone()]).is_err(), "gap");
        assert!(fold_logs("r-ns-x", &[log(1, json!({}))]).is_err(), "no payload");

        // End to end: the fold must match the main store's log count.
        let (d, ft, _) = testkit::driver();
        testkit::reset();
        stats_fakes(&ft);
        ft.set_default("bucket", "ListLedgers", items(vec![json!({"name": "r-ns-x", "id": 5})]));
        ft.set_default("bucket", "ListLogs", items(its.clone()));
        ft.set_default("bucket", "GetLedgerStats", stats(1, 3, 3, 1));
        stats_convergence(&d, 0, &nodes1(), Some(&json!({"providers": {"usage": {}}})), 0, soon(), short());
        assert!(hit("usage counters converge exactly at a fixed source horizon").is_some_and(|a| a.cond), "{:?}", testkit::assertions());
        assert_eq!(ft.calls_of("bucket", "ListLogs")[0]["request"]["ledger"], "r-ns-x");
    }

    #[test]
    fn cluster_config_convergence_and_change_detection() {
        let (d, ft, _) = testkit::driver();
        testkit::reset();
        let a = json!({"rotationThreshold": "50", "bloomVolumes": {"expectedKeys": "1000000"}});
        ft.set_default("cluster", "GetClusterState", ok(cs1(100, a.clone())));
        ft.script("bucket", "Barrier", barriers(&[10, 11, 12]));
        cluster_config_convergence(&d, 0, &nodes1(), Instant::now() + Duration::from_secs(10));
        assert!(hit("cluster_config_convergence completes within its quiet budget").unwrap().cond);
        assert!(hit("every node's cluster config equals the leader's at a common horizon").unwrap().cond);
        assert!(hit("cluster config convergence verified").is_some());
        assert!(!hit("a cluster config change was observed between quiet periods").unwrap().cond, "first run");

        testkit::reset();
        ft.set_default("cluster", "GetClusterState", ok(cs1(100, json!({"rotationThreshold": "64", "bloomVolumes": {"expectedKeys": "1000001"}}))));
        ft.script("bucket", "Barrier", barriers(&[20, 21, 22]));
        cluster_config_convergence(&d, 0, &nodes1(), Instant::now() + Duration::from_secs(10));
        assert!(hit("a cluster config change was observed between quiet periods").unwrap().cond);
        assert!(ft.calls_of("cluster", "GetClusterState").iter().any(|c| c["request"] == json!({"nodeId": "1"}) && c["node"] == "ledger-0:8888"));
    }

    #[test]
    fn an_unpersisted_leader_config_is_not_compared() {
        let (d, ft, _) = testkit::driver();
        testkit::reset();
        assert!(!config_persisted(&json!({"rotationThreshold": "50"})));
        assert!(!config_persisted(&json!({})));
        assert!(config_persisted(&json!({"rotationThreshold": "50", "bloomVolumes": {}})));
        ft.set_default("cluster", "GetClusterState", ok(cs1(100, json!({"rotationThreshold": "50"}))));
        ft.script("bucket", "Barrier", barriers(&[10, 11, 12]));
        cluster_config_convergence(&d, 0, &nodes1(), Instant::now() + Duration::from_secs(10));
        assert!(hit("cluster_config_convergence completes within its quiet budget").unwrap().cond);
        assert!(hit("every node's cluster config equals the leader's at a common horizon").is_none());
        assert!(hit("a cluster config change was observed between quiet periods").is_none());
    }

    #[test]
    fn source_selection_retries_through_a_leaderless_moment() {
        let (d, ft, _) = testkit::driver();
        testkit::reset();
        ft.set_default("cluster", "GetClusterState", ok(cs1(11, json!({}))));
        let unavailable = || FakeReply::Outcome(Outcome::from_json(&json!({"ok": false, "code": "Unavailable", "reason": "NO_LEADER"})));
        let mut script = vec![unavailable(), unavailable(), unavailable()];
        script.extend(barriers(&[10, 11, 12]));
        ft.script("bucket", "Barrier", script);
        audit_fakes(&ft);
        let dig = |_: &Driver, _: usize, _: &Value| -> Result<Value, String> { Ok(sections()) };
        // No discovery: `connected` comes from a candidate's (non-local) answer.
        cross_node_identity(&d, 0, &NodeMap::default(), soon(), short(), &dig);
        assert!(hit("cross-node identity oracle connects to replicas").unwrap().cond, "{:?}", testkit::assertions());
        assert!(hit("cross-node complete account and audit identity verified").is_some());
    }

    #[test]
    fn digests_are_retried_once_and_a_late_failure_is_a_cut() {
        let (d, ft, _) = testkit::driver();
        testkit::reset();
        ft.set_default("cluster", "GetClusterState", ok(cs1(11, json!({}))));
        ft.script("bucket", "Barrier", barriers(&[10, 11, 12]));
        audit_fakes(&ft);
        let n = AtomicU64::new(0);
        let dig = |_: &Driver, _: usize, _: &Value| -> Result<Value, String> {
            if n.fetch_add(1, Ordering::SeqCst) == 0 {
                Err("transient".into())
            } else {
                Ok(sections())
            }
        };
        cross_node_identity(&d, 0, &nodes1(), soon(), short(), &dig);
        assert_eq!(n.load(Ordering::SeqCst), 2);
        assert!(hit("cross-node complete account and audit identity verified").is_some());

        // A digest that runs until the budget is gone is a cut, not Other.
        let (d, ft, _) = testkit::driver();
        testkit::reset();
        ft.set_default("cluster", "GetClusterState", ok(cs1(11, json!({}))));
        ft.script("bucket", "Barrier", barriers(&[10, 11, 12]));
        audit_fakes(&ft);
        let slow = |_: &Driver, _: usize, b: &Value| -> Result<Value, String> {
            std::thread::sleep(Duration::from_millis(b["timeoutMs"].as_u64().unwrap()));
            Err("DeadlineExceeded".into())
        };
        cross_node_identity(&d, 0, &nodes1(), Instant::now() + Duration::from_secs(3), short(), &slow);
        assert!(!hit("cross_node_identity completes within its quiet budget").unwrap().cond);
        assert_eq!(always_count(), 0, "{:?}", testkit::assertions());
    }

    #[test]
    fn a_follower_config_differing_from_the_leader_is_a_finding() {
        let (d, _, _) = testkit::driver();
        testkit::reset();
        let a = json!({"rotationThreshold": "50"});
        let r = ConfigReport { horizon: 9, leader: 2, configs: BTreeMap::from([(1, a.clone()), (2, a.clone()), (3, json!({"rotationThreshold": "80"}))]) };
        assert_eq!(config_mismatches(&r), vec![3]);
        publish_config(&d, &r);
        let v: Vec<bool> = testkit::assertions().iter().filter(|x| x.message == "every node's cluster config equals the leader's at a common horizon").map(|x| x.cond).collect();
        assert_eq!(v, vec![true, true, false]);
        assert!(hit("cluster config convergence verified").is_none());
    }
}
