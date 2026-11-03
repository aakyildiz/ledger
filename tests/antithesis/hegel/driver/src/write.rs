//! Tracked and untracked writes (DESIGN §4.6, §4.8, §4.9, §6.5): write slots
//! with back-pressure, `begin_write` / `WriteTicket::run` / `dispatch`, the
//! attempt loop with late-outcome checks, resolver handoff, follow-ups, order
//! generators, and the write-path exploration assertions.
#![allow(dead_code)] // family-facing API

use crate::clients::{definitive, CallOpts, Outcome, OutcomeClass, Route, SignSpec};
use crate::jobs::Job;
use crate::machine::Driver;
use crate::oracle::{OracleError, OracleRefusal, Verdict, Via, WriteAbandonReq, WriteBeginReq, WriteEndReq};
use crate::shared::{current_write_owner, FlaggedPlan, Owner, Shared};
use hegel::TestCase;
use serde_json::{json, Value};
use std::sync::{Arc, Condvar, Mutex, Weak};
use std::time::{Duration, Instant};

/// In-rule budget of a tracked write before the resolver takes over.
pub const IN_RULE_BUDGET: Duration = Duration::from_secs(10);
/// Per-attempt deadline; `ATTEMPT_TIMEOUT_LARGE` above 16 requests.
pub const ATTEMPT_TIMEOUT: Duration = Duration::from_secs(4);
pub const ATTEMPT_TIMEOUT_LARGE: Duration = Duration::from_secs(5);
/// The oracle's cap on requests per tracked bulk.
pub const MAX_TRACKED_REQUESTS: usize = 64;
/// Slot capacity: 7 general units + 1 reserved for the maintenance disable.
pub const GENERAL_SLOTS: usize = 7;

// ---------------------------------------------------------------- slots

#[derive(Debug)]
struct SlotsState {
    general_free: usize,
    maintenance_free: bool,
}

struct SlotsInner {
    state: Mutex<SlotsState>,
    cv: Condvar,
}

/// The write-slot semaphore (DESIGN §4.8).
pub struct WriteSlots {
    inner: Arc<SlotsInner>,
    shared: Weak<Shared>,
}

/// One slot unit, held from before `/write/begin` until the ticket ends;
/// released on drop.
pub struct SlotUnit {
    inner: Arc<SlotsInner>,
    maintenance: bool,
}

impl std::fmt::Debug for SlotUnit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SlotUnit{{maintenance: {}}}", self.maintenance)
    }
}

impl SlotUnit {
    pub fn is_maintenance(&self) -> bool {
        self.maintenance
    }
}

impl Drop for SlotUnit {
    fn drop(&mut self) {
        let mut s = self.inner.state.lock().unwrap_or_else(|e| e.into_inner());
        if self.maintenance {
            s.maintenance_free = true;
        } else {
            s.general_free += 1;
        }
        drop(s);
        self.inner.cv.notify_all();
    }
}

/// `n ≥ 1` general units taken together.
#[derive(Debug)]
pub struct SlotGrant {
    units: Vec<SlotUnit>,
}

impl SlotGrant {
    pub fn len(&self) -> usize {
        self.units.len()
    }
    pub fn is_empty(&self) -> bool {
        self.units.is_empty()
    }
    /// Keeps `keep` units and releases the rest now.
    pub fn split(&mut self, keep: usize) {
        self.units.truncate(keep);
    }
    pub fn take_one(&mut self) -> Option<SlotUnit> {
        self.units.pop()
    }
    pub fn into_units(self) -> Vec<SlotUnit> {
        self.units
    }
}

/// Why `try_acquire_up_to` refuses (for logs and tests).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BackPressure {
    StaleHandoff,
    Backlog,
    SignaturesRequired,
    Degraded,
}

impl WriteSlots {
    pub fn new(shared: Weak<Shared>) -> Self {
        WriteSlots {
            inner: Arc::new(SlotsInner { state: Mutex::new(SlotsState { general_free: GENERAL_SLOTS, maintenance_free: true }), cv: Condvar::new() }),
            shared,
        }
    }

    /// The back-pressure condition that holds now, if any (DESIGN §4.8).
    pub fn back_pressure(&self) -> Option<BackPressure> {
        let s = self.shared.upgrade()?;
        if !s.oracle_ok() {
            return Some(BackPressure::Degraded);
        }
        if s.signatures_required() {
            return Some(BackPressure::SignaturesRequired);
        }
        if s.in_backlog_pause() {
            return Some(BackPressure::Backlog);
        }
        if s.resolvers.oldest_handoff_age().is_some_and(|a| a > s.timing().handoff_backpressure) {
            return Some(BackPressure::StaleHandoff);
        }
        None
    }

    /// Non-blocking: `None` when no general unit is free or back-pressure
    /// holds, else between 1 and `want` units.
    pub fn try_acquire_up_to(&self, want: usize) -> Option<SlotGrant> {
        if want == 0 || self.back_pressure().is_some() {
            return None;
        }
        let mut st = self.inner.state.lock().unwrap_or_else(|e| e.into_inner());
        let n = want.min(st.general_free);
        if n == 0 {
            return None;
        }
        st.general_free -= n;
        drop(st);
        Some(SlotGrant { units: (0..n).map(|_| SlotUnit { inner: self.inner.clone(), maintenance: false }).collect() })
    }

    /// Blocks up to `max_wait` for the reserved maintenance unit (exempt
    /// from back-pressure; only the maintenance-disable job uses it).
    pub fn acquire_maintenance(&self, max_wait: Duration) -> Option<SlotUnit> {
        let deadline = Instant::now() + max_wait;
        let mut st = self.inner.state.lock().unwrap_or_else(|e| e.into_inner());
        loop {
            if st.maintenance_free {
                st.maintenance_free = false;
                return Some(SlotUnit { inner: self.inner.clone(), maintenance: true });
            }
            let now = Instant::now();
            if now >= deadline {
                return None;
            }
            st = self.inner.cv.wait_timeout(st, deadline - now).unwrap_or_else(|e| e.into_inner()).0;
        }
    }

    /// Units in use (general, maintenance).
    pub fn in_use(&self) -> (usize, bool) {
        let st = self.inner.state.lock().unwrap_or_else(|e| e.into_inner());
        (GENERAL_SLOTS - st.general_free, !st.maintenance_free)
    }
}

// ---------------------------------------------------------------- plans

pub type FollowUp = Box<dyn FnOnce(&Driver, &Outcome) + Send>;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Role {
    Original,
    /// A different body under the original's key (`of` = the original's wid,
    /// which must be a recent `track_key` original, `Shared::flagged_plan`).
    Conflict { of: String },
}

pub struct WritePlan {
    pub family: &'static str,
    /// `servicepb.Request` protojson, ≤ 64.
    pub requests: Vec<Value>,
    /// `Shared::draw_track_key(tc)` for originals; forced true for conflicts.
    pub track_key: bool,
    pub role: Role,
    /// Pre-drawn, cycled (`draw::schedule`).
    pub schedule: Vec<Route>,
    /// Pre-drawn, cycled (`draw::backoffs`).
    pub backoffs_ms: Vec<u64>,
    pub sign: Option<SignSpec>,
    pub attempt_timeout: Duration,
    /// Runs as a quiet-blocking job on a definitive success.
    pub follow_up: Option<FollowUp>,
    pub hint: Value,
}

impl WritePlan {
    /// A plan with defaults: original, untracked key, one lib route on
    /// client 0, 250 ms backoff. Rules overwrite the drawn fields.
    pub fn new(family: &'static str, requests: Vec<Value>) -> Self {
        let attempt_timeout = if requests.len() > 16 { ATTEMPT_TIMEOUT_LARGE } else { ATTEMPT_TIMEOUT };
        WritePlan {
            family,
            requests,
            track_key: false,
            role: Role::Original,
            schedule: vec![Route::lib(0)],
            backoffs_ms: vec![250],
            sign: None,
            attempt_timeout,
            follow_up: None,
            hint: json!({}),
        }
    }

    /// Draws the attempt schedule (8 routes, 8 backoffs) and `track_key`.
    pub fn drawn(family: &'static str, requests: Vec<Value>, d: &Driver, tc: &TestCase) -> Self {
        let mut p = WritePlan::new(family, requests);
        p.track_key = d.s.draw_track_key(tc);
        p.schedule = crate::draw::schedule(tc, &d.s, 8);
        p.backoffs_ms = crate::draw::backoffs(tc, 8);
        p
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct AttemptRecord {
    pub op_id: u64,
    pub class: OutcomeClass,
    pub code: String,
    pub reason: String,
}

pub enum WriteResult {
    Definitive { outcome: Outcome, verdict: Verdict, wid: String, key: String, attempts: Vec<AttemptRecord> },
    HandedOff { wid: String, attempts: Vec<AttemptRecord> },
    Refused(OracleRefusal),
}

impl WriteResult {
    pub fn outcome(&self) -> Option<&Outcome> {
        match self {
            WriteResult::Definitive { outcome, .. } => Some(outcome),
            _ => None,
        }
    }
    pub fn is_success(&self) -> bool {
        self.outcome().is_some_and(Outcome::is_ok)
    }
    pub fn attempts(&self) -> &[AttemptRecord] {
        match self {
            WriteResult::Definitive { attempts, .. } | WriteResult::HandedOff { attempts, .. } => attempts,
            WriteResult::Refused(_) => &[],
        }
    }
    pub fn wid(&self) -> Option<&str> {
        match self {
            WriteResult::Definitive { wid, .. } | WriteResult::HandedOff { wid, .. } => Some(wid),
            WriteResult::Refused(_) => None,
        }
    }
}

impl std::fmt::Debug for WriteResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WriteResult::Definitive { outcome, wid, attempts, .. } => write!(f, "Definitive({wid}, {}, {} attempts)", outcome.summary(), attempts.len()),
            WriteResult::HandedOff { wid, attempts } => write!(f, "HandedOff({wid}, {} attempts)", attempts.len()),
            WriteResult::Refused(r) => write!(f, "Refused({:?}: {})", r.kind, r.message),
        }
    }
}

/// Everything a live tracked ticket needs; moves from the rule's guard to a
/// resolver (or a job) with its slot unit.
pub(crate) struct TicketState {
    pub wid: String,
    pub key: String,
    pub ticket: u64,
    pub predicted: Value,
    pub family: &'static str,
    pub requests: Vec<Value>,
    pub sign: Option<SignSpec>,
    pub schedule: Vec<Route>,
    pub backoffs_ms: Vec<u64>,
    pub attempt_timeout: Duration,
    pub follow_up: Option<FollowUp>,
    pub unit: Option<SlotUnit>,
    pub attempts: Vec<AttemptRecord>,
    /// (op id, route) of every attempt sent.
    pub ops: Vec<(u64, Route)>,
}

impl TicketState {
    pub(crate) fn op_ids(&self) -> Vec<u64> {
        self.ops.iter().map(|(o, _)| *o).collect()
    }
}

/// A usable late (or earlier) outcome: definitive and not truncated.
fn usable(o: &Outcome) -> bool {
    definitive(o) && !o.truncated()
}

/// One attempt of a tracked write: sends the identical request bytes and key.
pub(crate) fn attempt(d: &Driver, st: &mut TicketState, route: &Route) -> (u64, Outcome) {
    let req = crate::wire::apply_request(&st.requests, Some(&st.key));
    let opts = CallOpts { timeout: st.attempt_timeout, max_items: None, consistency: None, sign: st.sign.clone() };
    let op = d.s.clients.next_op_id();
    d.s.clients.watch(op);
    // Counted as sent before the call: from here on the ticket can only be
    // ended or handed off, never abandoned.
    st.ops.push((op, route.clone()));
    let out = d.s.clients.call_with_op(op, route, "bucket", "Apply", req, &opts);
    st.attempts.push(AttemptRecord { op_id: op, class: out.class(), code: out.code.clone(), reason: out.reason.clone() });
    explore_attempt(&out);
    if out.local_message_size() {
        check_unreachable!("a tracked write outcome fits the client's message limit", json!({"wid": st.wid, "ticket": st.ticket, "outcome": out.summary()}));
    }
    (op, out)
}

/// The write-path exploration assertions on one attempt's reason (DESIGN §4.9).
pub fn explore_attempt(out: &Outcome) {
    match out.reason.as_str() {
        "STALE_PROPOSAL" => check_sometimes!(true, "a write attempt is refused as a stale proposal", json!({"code": out.code})),
        "BALANCE_NOT_PRELOADED" | "PRELOAD_UNAVAILABLE" => check_sometimes!(true, "a write attempt hits a preload miss", json!({"reason": out.reason})),
        "WRITES_BLOCKED_CLOCK_SKEW" => check_sometimes!(true, "a write attempt is blocked by clock skew", json!({"code": out.code})),
        _ => {}
    }
}

/// A usable outcome that arrived late for one of the ticket's attempts.
pub(crate) fn late_definitive(d: &Driver, st: &TicketState) -> Option<(u64, Outcome)> {
    let ops = st.op_ids();
    d.s.clients.wait_late(&ops, Instant::now(), usable)
}

/// Ends a ticket with a definitive outcome: `/write/end`, follow-up job on
/// success, ended-op bookkeeping, slot release, registry removal.
pub(crate) fn finish(d: &Driver, mut st: TicketState, outcome: Outcome, op: u64, late: bool) -> WriteResult {
    let route = st.ops.iter().find(|(o, _)| *o == op).map(|(_, r)| r.clone()).unwrap_or_else(|| Route::plain(0));
    let req = WriteEndReq { ticket: st.ticket, outcome: outcome.clone(), attempts: st.ops.len() as u64, via: Via { op_id: op, client: route.client, route: route.via(), late } };
    let r = d.s.oracle.write_end(&req);
    // Every other attempt's definitive late outcome goes to /write/late (the
    // ending op's own UDP echo is not another attempt).
    d.s.clients.unwatch(&st.op_ids());
    let ended: Vec<(u64, usize, String)> = st.ops.iter().filter(|(o, _)| *o != op).map(|(o, r)| (*o, r.client, r.via())).collect();
    d.s.clients.mark_ended(st.ticket, &ended);
    if outcome.is_ok() {
        check_sometimes!(st.ops.len() > 1, "a tracked write needs more than one attempt and then commits", json!({"wid": st.wid, "attempts": st.ops.len()}));
        if let Some(f) = st.follow_up.take() {
            let o = outcome.clone();
            d.submit_job(Job { name: "follow-up", blocks_quiet: true, run: Box::new(move |d: &Driver| f(d, &o)) });
        }
    }
    if late {
        check_reachable!("a late completion event ends a tracked write", json!({"wid": st.wid, "op": op}));
    }
    d.s.registry.remove(st.ticket);
    st.unit.take();
    match r {
        Ok(resp) => {
            if !resp.verdict.violations.is_empty() {
                println!("driver: /write/end {} {}: oracle violations {:?}", st.wid, st.ticket, resp.verdict.violations.iter().map(|v| &v.id).collect::<Vec<_>>());
            }
            WriteResult::Definitive { outcome, verdict: resp.verdict, wid: st.wid.clone(), key: st.key.clone(), attempts: std::mem::take(&mut st.attempts) }
        }
        Err(e) => WriteResult::Refused(e),
    }
}

/// The live guard of one tracked write (DESIGN §4.9).
pub struct WriteTicket {
    d: Driver,
    st: Option<TicketState>,
}

impl WriteTicket {
    pub fn wid(&self) -> &str {
        &self.st.as_ref().expect("live ticket").wid
    }
    pub fn ticket(&self) -> u64 {
        self.st.as_ref().expect("live ticket").ticket
    }
    pub fn key(&self) -> &str {
        &self.st.as_ref().expect("live ticket").key
    }
    pub fn predicted(&self) -> &Value {
        &self.st.as_ref().expect("live ticket").predicted
    }

    /// The attempt loop: send through the scheduled routes, consult late
    /// outcomes of earlier attempts before each retry, stop at the first
    /// definitive outcome (`/write/end`), or hand off at the budget.
    pub fn run(mut self, budget: Duration) -> WriteResult {
        let d = self.d.clone();
        let mut st = self.st.take().expect("live ticket");
        let deadline = Instant::now() + budget;
        let mut k = 0usize;
        loop {
            if let Some((op, out)) = late_definitive(&d, &st) {
                return finish(&d, st, out, op, true);
            }
            if !st.ops.is_empty() && Instant::now() >= deadline {
                return handoff(&d, st);
            }
            let route = st.schedule[k % st.schedule.len()].clone();
            let backoff = Duration::from_millis(st.backoffs_ms[k % st.backoffs_ms.len()]);
            k += 1;
            let (op, out) = attempt(&d, &mut st, &route);
            if definitive(&out) {
                return finish(&d, st, out, op, false);
            }
            // Wait out the backoff, waking early for a late definitive
            // outcome of any attempt so far.
            let until = (Instant::now() + backoff).min(deadline);
            let ops = st.op_ids();
            if let Some((op, out)) = d.s.clients.wait_late(&ops, until, usable) {
                return finish(&d, st, out, op, true);
            }
        }
    }
}

fn handoff(d: &Driver, st: TicketState) -> WriteResult {
    let wid = st.wid.clone();
    let attempts = st.attempts.clone();
    check_sometimes!(true, "a tracked write is handed off to a resolver", json!({"wid": wid, "attempts": attempts.len()}));
    d.s.registry.set_owner(st.ticket, Owner::Resolver);
    d.s.resolvers.push(st);
    WriteResult::HandedOff { wid, attempts }
}

impl Drop for WriteTicket {
    fn drop(&mut self) {
        let Some(st) = self.st.take() else { return };
        if st.ops.is_empty() {
            // No attempt was sent (a draw unwound between begin and the first send).
            let r = self.d.s.oracle.write_abandon(&WriteAbandonReq { ticket: st.ticket, attempts: 0 });
            if let Err(e) = r {
                println!("driver: /write/abandon {} {}: {:?} {}", st.wid, st.ticket, e.kind, e.message);
            }
            self.d.s.registry.remove(st.ticket);
        } else {
            let _ = handoff(&self.d, st);
        }
    }
}

// ---------------------------------------------------------------- untracked

pub struct UntrackedSend {
    pub requests: Vec<Value>,
    /// `None` ⇒ exactly one attempt (no retry without a key).
    pub key: Option<String>,
    pub schedule: Vec<Route>,
    pub backoffs_ms: Vec<u64>,
    pub attempt_timeout: Duration,
    /// Default 10 s.
    pub budget: Duration,
    pub sign: Option<SignSpec>,
}

impl UntrackedSend {
    pub fn new(requests: Vec<Value>, key: Option<String>) -> Self {
        let attempt_timeout = if requests.len() > 16 { ATTEMPT_TIMEOUT_LARGE } else { ATTEMPT_TIMEOUT };
        UntrackedSend { requests, key, schedule: vec![Route::lib(0)], backoffs_ms: vec![250], attempt_timeout, budget: IN_RULE_BUDGET, sign: None }
    }
}

pub struct UntrackedResult {
    pub op_ids: Vec<u64>,
    pub outcome: Outcome,
    pub attempts: Vec<AttemptRecord>,
}

// ---------------------------------------------------------------- order gens

pub struct BulkCtx<'a> {
    pub run: &'a str,
    pub ledger: &'a str,
    /// The core `ledger` view of `ledger`.
    pub ledger_view: &'a Value,
    pub index_in_bulk: usize,
    pub len: usize,
}

pub struct OrderGen {
    pub name: &'static str,
    pub weight: f64,
    /// e.g. taper (§3.2), "not l2" for chart gens.
    pub applies: fn(&BulkCtx) -> bool,
    /// Offers a skippable-reason variant.
    pub has_opt_in: bool,
    /// May call `d.view()` to aim.
    pub gen: fn(&Driver, &TestCase, &BulkCtx, bool) -> Option<Value>,
}

/// Draws `len` orders by weight among `gens` whose `applies` holds, 1/4
/// opt-in per slot when offered, and returns the requests.
pub fn compose_bulk(d: &Driver, tc: &TestCase, gens: &[OrderGen], ledger: &str, ledger_view: &Value, len: usize) -> Vec<Value> {
    let run = d.s.run.clone();
    let mut out = Vec::with_capacity(len);
    for i in 0..len {
        let ctx = BulkCtx { run: &run, ledger, ledger_view, index_in_bulk: i, len };
        let weights: Vec<f64> = gens.iter().map(|g| if (g.applies)(&ctx) { g.weight } else { 0.0 }).collect();
        let Some(gi) = crate::draw::weighted(tc, &weights) else { break };
        let g = &gens[gi];
        let opt_in = g.has_opt_in && crate::draw::chance(tc, 0.25);
        if let Some(v) = (g.gen)(d, tc, &ctx, opt_in) {
            out.push(v);
        }
    }
    out
}

// ---------------------------------------------------------------- Driver API

impl Driver {
    /// Runs one RPC through a client. Returns the op id and the outcome.
    pub fn call(&self, route: &Route, service: &str, method: &str, req: Value, opts: &CallOpts) -> (u64, Outcome) {
        self.s.clients.call(route, service, method, req, opts)
    }

    /// Posts `/write/begin` and returns the ticket guard. Nothing is sent to
    /// the cluster before this returns.
    pub fn begin_write(&self, plan: WritePlan, unit: SlotUnit) -> Result<WriteTicket, OracleRefusal> {
        let WritePlan { family, requests, track_key, role, mut schedule, mut backoffs_ms, sign, attempt_timeout, follow_up, hint } = plan;
        if requests.len() > MAX_TRACKED_REQUESTS || requests.is_empty() {
            check_unreachable!("a tracked write plan has between 1 and 64 requests", json!({"family": family, "requests": requests.len()}));
            return Err(OracleRefusal::new(OracleError::Unmodeled, "plan size"));
        }
        if !self.s.oracle_ok() {
            return Err(OracleRefusal::new(OracleError::Degraded, "degraded mode"));
        }
        if schedule.is_empty() {
            schedule.push(Route::lib(0));
        }
        if backoffs_ms.is_empty() {
            backoffs_ms.push(250);
        }
        let (key, track_key, role_s, of) = match &role {
            Role::Original => (self.s.next_key(), track_key, "original", None),
            Role::Conflict { of } => match self.s.flagged_plan(of) {
                Some((k, _, _)) => (k, true, "conflict", Some(of.clone())),
                None => return Err(OracleRefusal::new(OracleError::Other, "conflict original not recorded")),
            },
        };
        let wid = self.s.next_wid();
        let req = WriteBeginReq {
            wid: wid.clone(),
            idempotency_key: key.clone(),
            requests: requests.clone(),
            track_key,
            role: role_s.to_string(),
            of,
            family: family.to_string(),
            signed: sign.is_some(),
            hint,
        };
        let resp = {
            let _pending = self.s.registry.begin_pending();
            let r = self.s.oracle.write_begin(&req);
            if let Ok(resp) = &r {
                self.s.registry.insert(resp.ticket, current_write_owner());
            }
            r
        };
        let resp = match resp {
            Ok(r) => r,
            Err(e) => {
                if e.kind == OracleError::Backlog {
                    self.s.note_backlog();
                }
                return Err(e);
            }
        };
        if track_key && role == Role::Original {
            self.s.record_flagged(FlaggedPlan { wid: wid.clone(), key: key.clone(), requests: requests.clone(), sign: sign.clone() });
        }
        Ok(WriteTicket {
            d: self.clone(),
            st: Some(TicketState {
                wid,
                key,
                ticket: resp.ticket,
                predicted: resp.predicted,
                family,
                requests,
                sign,
                schedule,
                backoffs_ms,
                attempt_timeout,
                follow_up,
                unit: Some(unit),
                attempts: vec![],
                ops: vec![],
            }),
        })
    }

    /// `begin_write` + `run(IN_RULE_BUDGET)`.
    pub fn dispatch(&self, plan: WritePlan, unit: SlotUnit) -> WriteResult {
        match self.begin_write(plan, unit) {
            Ok(t) => t.run(IN_RULE_BUDGET),
            Err(e) => WriteResult::Refused(e),
        }
    }

    /// Dispatches several plans concurrently on scoped threads, one unit each.
    pub fn dispatch_many(&self, plans: Vec<WritePlan>, grant: SlotGrant) -> Vec<WriteResult> {
        let mut units = grant.into_units();
        let mut work = Vec::new();
        let mut refused = Vec::new();
        for (i, p) in plans.into_iter().enumerate() {
            match units.pop() {
                Some(u) => work.push((i, p, u)),
                None => refused.push(i),
            }
        }
        drop(units);
        let mut results: Vec<(usize, WriteResult)> = std::thread::scope(|s| {
            let hs: Vec<_> = work
                .into_iter()
                .map(|(i, p, u)| crate::assertions::scoped_inheriting(s, move || (i, self.dispatch(p, u))))
                .collect();
            hs.into_iter().map(|h| h.join().unwrap_or_else(|e| std::panic::resume_unwind(e))).collect()
        });
        for i in refused {
            results.push((i, WriteResult::Refused(OracleRefusal::new(OracleError::Other, "no slot unit for this plan"))));
        }
        results.sort_by_key(|(i, _)| *i);
        results.into_iter().map(|(_, r)| r).collect()
    }

    /// The same retry loop without the oracle and without slots, for
    /// Rust-checked ledgers.
    pub fn send_untracked(&self, s: UntrackedSend) -> UntrackedResult {
        let schedule = if s.schedule.is_empty() { vec![Route::lib(0)] } else { s.schedule.clone() };
        let backoffs = if s.backoffs_ms.is_empty() { vec![250] } else { s.backoffs_ms.clone() };
        let req = crate::wire::apply_request(&s.requests, s.key.as_deref());
        let opts = CallOpts { timeout: s.attempt_timeout, max_items: None, consistency: None, sign: s.sign.clone() };
        let deadline = Instant::now() + s.budget;
        let mut op_ids = Vec::new();
        let mut attempts = Vec::new();
        let mut last = Outcome::unknown("transport");
        for k in 0.. {
            let route = &schedule[k % schedule.len()];
            let op = self.s.clients.next_op_id();
            self.s.clients.watch(op);
            let out = self.s.clients.call_with_op(op, route, "bucket", "Apply", req.clone(), &opts);
            op_ids.push(op);
            attempts.push(AttemptRecord { op_id: op, class: out.class(), code: out.code.clone(), reason: out.reason.clone() });
            last = out;
            if definitive(&last) || s.key.is_none() {
                break;
            }
            let until = (Instant::now() + Duration::from_millis(backoffs[k % backoffs.len()])).min(deadline);
            if let Some((_, o)) = self.s.clients.wait_late(&op_ids, until, usable) {
                last = o;
                break;
            }
            if Instant::now() >= deadline {
                break;
            }
        }
        self.s.clients.unwatch(&op_ids);
        UntrackedResult { op_ids, outcome: last, attempts }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testkit::{self, FakeReply};
    use std::sync::atomic::Ordering;

    fn plan(d: &Driver, n: usize) -> WritePlan {
        let reqs = (0..n).map(|i| crate::wire::create_tx_request(&format!("o-{}-l1", d.s.run), &[crate::wire::Posting::new("world", &format!("users:{i}"), 1, "COIN")], None)).collect();
        let mut p = WritePlan::new("test", reqs);
        p.schedule = vec![Route::plain(0), Route::lib(1), Route::plain(2)];
        p.backoffs_ms = vec![10];
        p.attempt_timeout = Duration::from_millis(200);
        p
    }

    fn ok_apply(seq: u64) -> Outcome {
        Outcome::from_json(&json!({"ok": true, "response": {"logs": [{"sequence": seq.to_string()}]}}))
    }

    // ---- slots

    #[test]
    fn slots_grant_split_release() {
        let slots = WriteSlots::new(Weak::new());
        let mut g = slots.try_acquire_up_to(5).unwrap();
        assert_eq!(g.len(), 5);
        g.split(2);
        assert_eq!(slots.in_use(), (2, false));
        let u = g.take_one().unwrap();
        assert_eq!(g.len(), 1);
        let g2 = slots.try_acquire_up_to(10).unwrap();
        assert_eq!(g2.len(), 5);
        assert!(slots.try_acquire_up_to(1).is_none(), "general units exhausted");
        // The maintenance unit is never handed to a general request.
        let m = slots.acquire_maintenance(Duration::from_millis(10)).unwrap();
        assert!(m.is_maintenance());
        assert!(slots.acquire_maintenance(Duration::from_millis(10)).is_none());
        drop(u);
        drop(g);
        drop(g2);
        assert_eq!(slots.in_use(), (0, true));
        assert_eq!(slots.try_acquire_up_to(100).unwrap().len(), GENERAL_SLOTS);
        drop(m);
        assert_eq!(slots.in_use(), (0, false));
        let units = slots.try_acquire_up_to(3).unwrap().into_units();
        assert_eq!(units.len(), 3);
    }

    #[test]
    fn slots_back_pressure_conditions_each_refuse() {
        let (d, _, fo) = testkit::driver();
        assert!(d.s.slots.try_acquire_up_to(1).is_some());
        // (b) backlog less than 5 s ago
        d.s.note_backlog();
        assert_eq!(d.s.slots.back_pressure(), Some(BackPressure::Backlog));
        assert!(d.s.slots.try_acquire_up_to(1).is_none());
        *d.s.backlog_until.lock().unwrap() = None;
        // (c) signatures required
        d.s.signatures_required.store(true, Ordering::SeqCst);
        assert!(d.s.slots.try_acquire_up_to(1).is_none());
        // the maintenance disable is exempt
        assert!(d.s.slots.acquire_maintenance(Duration::from_millis(1)).is_some());
        d.s.signatures_required.store(false, Ordering::SeqCst);
        // (a) the oldest handoff is older than the limit
        let unit = d.s.slots.try_acquire_up_to(1).unwrap().take_one().unwrap();
        fo.pause_tickets();
        let t = d.begin_write(plan(&d, 1), unit).unwrap();
        d.s.resolvers.set_paused(true);
        drop(handoff_with_attempt(&d, t));
        std::thread::sleep(d.s.timing().handoff_backpressure + Duration::from_millis(20));
        assert_eq!(d.s.slots.back_pressure(), Some(BackPressure::StaleHandoff));
        assert!(d.s.slots.try_acquire_up_to(1).is_none());
        d.s.resolvers.set_paused(false);
        // (d) degraded
        d.s.enter_degraded("test");
        assert_eq!(d.s.slots.back_pressure(), Some(BackPressure::Degraded));
        assert!(d.s.slots.try_acquire_up_to(1).is_none());
    }

    /// Fakes one sent attempt, then drops the guard (handoff).
    fn handoff_with_attempt(d: &Driver, mut t: WriteTicket) -> WriteTicket {
        t.st.as_mut().unwrap().ops.push((d.s.clients.next_op_id(), Route::plain(0)));
        t
    }

    #[test]
    fn slots_stress_never_exceed_capacity() {
        let slots = Arc::new(WriteSlots::new(Weak::new()));
        let held = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let max = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let hs: Vec<_> = (0..8)
            .map(|i| {
                let (slots, held, max) = (slots.clone(), held.clone(), max.clone());
                std::thread::spawn(move || {
                    for j in 0..2000 {
                        let maint = (i + j) % 7 == 0;
                        let units: Vec<SlotUnit> = if maint {
                            slots.acquire_maintenance(Duration::from_millis(1)).into_iter().collect()
                        } else {
                            slots.try_acquire_up_to(1 + (j % 4)).map(|g| g.into_units()).unwrap_or_default()
                        };
                        let n = held.fetch_add(units.len(), Ordering::SeqCst) + units.len();
                        max.fetch_max(n, Ordering::SeqCst);
                        assert!(n <= 8, "{n} units held");
                        std::thread::yield_now();
                        held.fetch_sub(units.len(), Ordering::SeqCst);
                        drop(units);
                    }
                })
            })
            .collect();
        for h in hs {
            h.join().unwrap();
        }
        assert!(max.load(Ordering::SeqCst) <= 8);
        assert_eq!(slots.in_use(), (0, false));
    }

    // ---- dispatch

    #[test]
    fn dispatch_definitive_first_try() {
        let (d, ft, fo) = testkit::driver();
        testkit::reset();
        ft.script("bucket", "Apply", vec![FakeReply::Outcome(ok_apply(42))]);
        let unit = d.s.slots.try_acquire_up_to(1).unwrap().take_one().unwrap();
        let r = d.dispatch(plan(&d, 1), unit);
        let WriteResult::Definitive { outcome, attempts, .. } = r else { panic!("{r:?}") };
        assert!(outcome.is_ok());
        assert_eq!(attempts.len(), 1);
        assert_eq!(d.s.acked_seq_high_water.load(Ordering::SeqCst), 42);
        assert_eq!(fo.calls("/write/end").len(), 1);
        assert_eq!(fo.calls("/write/end")[0]["attempts"], 1);
        assert!(d.s.registry.snapshot().is_empty());
        assert_eq!(d.s.slots.in_use(), (0, false));
        assert!(testkit::assertions().iter().any(|a| a.message == "a tracked write needs more than one attempt and then commits" && !a.cond));
    }

    #[test]
    fn dispatch_retries_unknown_with_same_bytes_and_key() {
        let (d, ft, fo) = testkit::driver();
        testkit::reset();
        ft.script("bucket", "Apply", vec![FakeReply::Lost(None), FakeReply::Outcome(Outcome::from_json(&json!({"ok": false, "code": "Unavailable", "reason": "STALE_PROPOSAL"}))), FakeReply::Outcome(ok_apply(7))]);
        let unit = d.s.slots.try_acquire_up_to(1).unwrap().take_one().unwrap();
        let r = d.dispatch(plan(&d, 2), unit);
        assert!(r.is_success(), "{r:?}");
        let calls = ft.calls_of("bucket", "Apply");
        assert_eq!(calls.len(), 3);
        for c in &calls[1..] {
            assert_eq!(c["request"], calls[0]["request"], "identical request bytes and key");
        }
        assert!(calls[0]["request"]["unsigned"]["idempotencyKey"].as_str().unwrap().starts_with(&format!("o-{}-k", d.s.run)));
        assert_eq!(calls[0]["route"], "plain");
        assert_eq!(calls[1]["route"], "lib");
        assert_eq!(calls[1]["opId"].as_u64(), calls[0]["opId"].as_u64().map(|o| o + 1));
        let end = &fo.calls("/write/end")[0];
        assert_eq!(end["attempts"], 3);
        assert_eq!(end["via"]["late"], false);
        assert!(testkit::fired("a write attempt is refused as a stale proposal"));
        assert!(testkit::assertions().iter().any(|a| a.message == "a tracked write needs more than one attempt and then commits" && a.cond));
    }

    #[test]
    fn late_udp_success_of_attempt_one_ends_the_ticket() {
        let (d, ft, fo) = testkit::driver();
        testkit::reset();
        // Attempt 1 gets no answer; its success arrives over UDP while
        // attempt 2 is still pending (attempt 2 itself never answers).
        ft.script(
            "bucket",
            "Apply",
            vec![
                FakeReply::Lost(Some((Duration::from_millis(50), ok_apply(9)))),
                FakeReply::Delayed(Duration::from_millis(150), Outcome::unknown("timeout")),
            ],
        );
        let unit = d.s.slots.try_acquire_up_to(1).unwrap().take_one().unwrap();
        let mut p = plan(&d, 1);
        p.backoffs_ms = vec![1];
        let r = d.dispatch(p, unit);
        assert!(r.is_success(), "{r:?}");
        let end = &fo.calls("/write/end")[0];
        assert_eq!(end["via"]["late"], true);
        let first_op = ft.calls_of("bucket", "Apply")[0]["opId"].clone();
        assert_eq!(end["via"]["opId"], first_op);
        assert!(testkit::fired("a late completion event ends a tracked write"));
    }

    #[test]
    fn truncated_late_success_is_resent() {
        let (d, ft, fo) = testkit::driver();
        testkit::reset();
        let truncated = Outcome::from_json(&json!({"ok": true, "message": "truncated"}));
        ft.script("bucket", "Apply", vec![FakeReply::Lost(Some((Duration::from_millis(1), truncated))), FakeReply::Outcome(ok_apply(11))]);
        let unit = d.s.slots.try_acquire_up_to(1).unwrap().take_one().unwrap();
        let mut p = plan(&d, 1);
        p.backoffs_ms = vec![50];
        let r = d.dispatch(p, unit);
        assert!(r.is_success(), "{r:?}");
        assert_eq!(ft.calls_of("bucket", "Apply").len(), 2, "the truncated success is not usable: resend");
        let end = &fo.calls("/write/end")[0];
        assert_eq!(end["via"]["late"], false);
        assert_eq!(end["outcome"]["response"]["logs"][0]["sequence"], "11");
    }

    #[test]
    fn budget_exhausted_hands_off_and_a_resolver_completes_it() {
        let (d, ft, fo) = testkit::driver();
        testkit::reset();
        ft.script("bucket", "Apply", vec![FakeReply::Outcome(Outcome::unknown("timeout")); 3]);
        ft.set_default("bucket", "Apply", FakeReply::Outcome(ok_apply(5)));
        d.s.resolvers.set_paused(true);
        let unit = d.s.slots.try_acquire_up_to(1).unwrap().take_one().unwrap();
        let t = d.begin_write(plan(&d, 1), unit).unwrap();
        let ticket = t.ticket();
        let r = t.run(Duration::from_millis(30));
        assert!(matches!(r, WriteResult::HandedOff { .. }), "{r:?}");
        assert!(testkit::fired("a tracked write is handed off to a resolver"));
        assert_eq!(d.s.registry.owners().get(&ticket), Some(&Owner::Resolver));
        assert_eq!(d.s.slots.in_use(), (1, false), "the unit moved with the ticket");
        assert!(!d.s.resolvers.is_empty());
        d.s.resolvers.set_paused(false);
        testkit::wait_until(Duration::from_secs(10), || d.s.resolvers.is_empty());
        assert_eq!(fo.calls("/write/end").len(), 1);
        assert!(d.s.registry.snapshot().is_empty());
        assert_eq!(d.s.slots.in_use(), (0, false));
        assert!(testkit::assertions().iter().any(|a| a.message == "a resolver settles a handed-off write" && a.cond));
        // The resolver uses the deterministic schedule: plain routes.
        let calls = ft.calls_of("bucket", "Apply");
        assert_eq!(calls.last().unwrap()["route"], "plain");
    }

    #[test]
    fn drop_before_first_send_abandons() {
        let (d, _ft, fo) = testkit::driver();
        let unit = d.s.slots.try_acquire_up_to(1).unwrap().take_one().unwrap();
        let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _t = d.begin_write(plan(&d, 1), unit).unwrap();
            // A draw unwinds here, before the first send.
            std::panic::panic_any(testkit::StopTestLike);
        }));
        assert!(r.is_err());
        let ab = fo.calls("/write/abandon");
        assert_eq!(ab.len(), 1);
        assert_eq!(ab[0]["attempts"], 0);
        assert!(d.s.registry.snapshot().is_empty());
        assert_eq!(d.s.slots.in_use(), (0, false));
        assert!(fo.inflight().is_empty());
    }

    #[test]
    fn begin_write_exposes_predicted_before_any_send() {
        let (d, ft, fo) = testkit::driver();
        fo.set_predicted(json!({"ok": true, "reason": "", "checkpointId": "7"}));
        let unit = d.s.slots.try_acquire_up_to(1).unwrap().take_one().unwrap();
        let t = d.begin_write(plan(&d, 1), unit).unwrap();
        assert_eq!(t.predicted()["checkpointId"], "7");
        assert!(ft.calls_of("bucket", "Apply").is_empty());
        assert!(d.s.registry.contains(t.ticket()));
        drop(t);
    }

    #[test]
    fn follow_up_runs_as_a_blocking_job_after_success() {
        let (d, ft, _fo) = testkit::driver();
        ft.script("bucket", "Apply", vec![FakeReply::Outcome(ok_apply(3))]);
        let ran = Arc::new(Mutex::new(None::<(bool, usize)>));
        let ran2 = ran.clone();
        let mut p = plan(&d, 1);
        p.follow_up = Some(Box::new(move |d: &Driver, o: &Outcome| {
            *ran2.lock().unwrap() = Some((o.is_ok(), d.s.blocking_jobs()));
        }));
        let unit = d.s.slots.try_acquire_up_to(1).unwrap().take_one().unwrap();
        assert!(d.dispatch(p, unit).is_success());
        testkit::wait_until(Duration::from_secs(5), || ran.lock().unwrap().is_some());
        let (ok, blocking) = ran.lock().unwrap().unwrap();
        assert!(ok);
        assert_eq!(blocking, 1, "the follow-up counts as a running quiet-blocking job");
        testkit::wait_until(Duration::from_secs(5), || d.s.blocking_jobs() == 0);
    }

    #[test]
    fn a_backlog_refusal_pauses_new_writes() {
        let (d, ft, fo) = testkit::driver();
        fo.refuse_next_begin(409, "backlog");
        let unit = d.s.slots.try_acquire_up_to(1).unwrap().take_one().unwrap();
        let r = d.dispatch(plan(&d, 1), unit);
        assert!(matches!(&r, WriteResult::Refused(e) if e.kind == OracleError::Backlog), "{r:?}");
        assert_eq!(d.s.slots.in_use(), (0, false), "the unit is released");
        assert!(d.s.in_backlog_pause());
        assert!(d.s.slots.try_acquire_up_to(1).is_none());
        assert!(ft.calls_of("bucket", "Apply").is_empty());
        fo.refuse_next_begin(409, "serialized");
        *d.s.backlog_until.lock().unwrap() = None;
        let unit = d.s.slots.try_acquire_up_to(1).unwrap().take_one().unwrap();
        assert!(matches!(d.dispatch(plan(&d, 1), unit), WriteResult::Refused(e) if e.kind == OracleError::Serialized));
        assert!(d.s.registry.snapshot().is_empty());
    }

    #[test]
    fn oversized_plans_are_a_driver_bug() {
        let (d, ft, fo) = testkit::driver();
        testkit::reset();
        let unit = d.s.slots.try_acquire_up_to(1).unwrap().take_one().unwrap();
        let r = d.dispatch(plan(&d, 65), unit);
        assert!(matches!(r, WriteResult::Refused(_)));
        assert!(testkit::fired("a tracked write plan has between 1 and 64 requests"));
        assert!(fo.calls("/write/begin").is_empty());
        assert!(ft.calls_of("bucket", "Apply").is_empty());
    }

    #[test]
    fn local_message_size_error_is_asserted_and_retried() {
        let (d, ft, _) = testkit::driver();
        testkit::reset();
        ft.script("bucket", "Apply", vec![FakeReply::Outcome(Outcome::from_json(&json!({"ok": false, "code": "ResourceExhausted", "local": true}))), FakeReply::Outcome(ok_apply(1))]);
        let unit = d.s.slots.try_acquire_up_to(1).unwrap().take_one().unwrap();
        assert!(d.dispatch(plan(&d, 1), unit).is_success());
        assert!(testkit::fired("a tracked write outcome fits the client's message limit"));
    }

    #[test]
    fn conflicts_reuse_the_original_key_and_flagged_plans_are_recorded() {
        let (d, ft, fo) = testkit::driver();
        ft.set_default("bucket", "Apply", FakeReply::Outcome(ok_apply(1)));
        let mut p = plan(&d, 1);
        p.track_key = true;
        let unit = d.s.slots.try_acquire_up_to(1).unwrap().take_one().unwrap();
        let r = d.dispatch(p, unit);
        let wid = r.wid().unwrap().to_string();
        let (key, reqs, sign) = d.s.flagged_plan(&wid).unwrap();
        assert_eq!(reqs.len(), 1);
        assert!(sign.is_none());
        let mut c = plan(&d, 2);
        c.role = Role::Conflict { of: wid.clone() };
        let unit = d.s.slots.try_acquire_up_to(1).unwrap().take_one().unwrap();
        d.dispatch(c, unit);
        let begins = fo.calls("/write/begin");
        assert_eq!(begins[1]["idempotencyKey"], json!(key));
        assert_eq!(begins[1]["role"], "conflict");
        assert_eq!(begins[1]["of"], json!(wid));
        assert_eq!(begins[1]["trackKey"], true);
        assert_eq!(d.s.track_keys_flagged(), 1, "conflicts do not count");
    }

    #[test]
    fn untracked_sends() {
        let (d, ft, _) = testkit::driver();
        ft.script("bucket", "Apply", vec![FakeReply::Outcome(Outcome::unknown("timeout")), FakeReply::Outcome(ok_apply(2))]);
        let mut s = UntrackedSend::new(vec![json!({"createLedger": {"name": "r-x"}})], None);
        s.backoffs_ms = vec![1];
        let r = d.send_untracked(s);
        assert_eq!(r.op_ids.len(), 1, "no key: exactly one attempt");
        assert!(r.outcome.is_unknown());
        let mut s = UntrackedSend::new(vec![json!({"createLedger": {"name": "r-x"}})], Some("r-k".into()));
        s.backoffs_ms = vec![1];
        let r = d.send_untracked(s);
        assert_eq!(r.op_ids.len(), 1);
        assert!(r.outcome.is_ok());
        assert_eq!(ft.calls_of("bucket", "Apply")[1]["request"]["unsigned"]["idempotencyKey"], "r-k");
    }

    #[test]
    fn dispatch_many_uses_one_unit_each() {
        let (d, ft, _) = testkit::driver();
        ft.set_default("bucket", "Apply", FakeReply::Outcome(ok_apply(1)));
        let g = d.s.slots.try_acquire_up_to(3).unwrap();
        let rs = d.dispatch_many(vec![plan(&d, 1), plan(&d, 1), plan(&d, 1), plan(&d, 1)], g);
        assert_eq!(rs.len(), 4);
        assert_eq!(rs.iter().filter(|r| r.is_success()).count(), 3);
        assert!(matches!(rs[3], WriteResult::Refused(_)));
        assert_eq!(d.s.slots.in_use(), (0, false));
    }

    #[test]
    fn compose_bulk_respects_applies_and_weights() {
        let (d, _, _) = testkit::driver();
        fn yes(_: &BulkCtx) -> bool {
            true
        }
        fn no(_: &BulkCtx) -> bool {
            false
        }
        fn g(_: &Driver, _: &TestCase, c: &BulkCtx, opt: bool) -> Option<Value> {
            Some(json!({"i": c.index_in_bulk, "opt": opt, "ledger": c.ledger}))
        }
        fn never(_: &Driver, _: &TestCase, _: &BulkCtx, _: bool) -> Option<Value> {
            panic!("not applicable")
        }
        let gens = [OrderGen { name: "g", weight: 1.0, applies: yes, has_opt_in: true, gen: g }, OrderGen { name: "n", weight: 5.0, applies: no, has_opt_in: false, gen: never }];
        testkit::with_tc_n(20, |tc| {
            let v = compose_bulk(&d, tc, &gens, "L", &json!({}), 3);
            assert_eq!(v.len(), 3);
            assert_eq!(v[2]["i"], 2);
            assert_eq!(v[0]["ledger"], "L");
        });
    }
}
