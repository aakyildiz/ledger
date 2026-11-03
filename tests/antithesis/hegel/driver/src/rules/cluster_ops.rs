//! Task 12: cluster operations (Rust-checked; no oracle involvement, these
//! RPCs never change tracked business state). Barrier monotonicity,
//! leadership transfer, compaction, Pebble checkpoints, full and incremental
//! backups (jobs), CheckStore (job), event sinks, per-node cluster state.
//!
//! Every draw happens before the first send. Each rule body stays within
//! `RULE_BUDGET` (COMMON: about 10 s). Long operations (backups, the store
//! check) are drawn here and run on the job threads (DESIGN §4.13).
use crate::clients::{definitive, CallOpts, Outcome, Route};
use crate::draw;
use crate::jobs::Job;
use crate::logs::NodeStates;
use crate::machine::{rule, Driver};
use crate::nodes::NodeMap;
use crate::setup::SetupHook;
use crate::shared::Shared;
use crate::wire::{create_tx_request, str_field, u64_field, Posting};
use crate::write::UntrackedSend;
use hegel::stateful::ConcurrentRule;
use hegel::TestCase;
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

pub fn rules() -> Vec<ConcurrentRule<Driver>> {
    vec![
        rule("barrier_monotonic", 1.0, barrier_monotonic),
        rule("transfer_leadership", 0.4, transfer_leadership),
        rule("compaction", 0.3, compaction),
        rule("cluster_checkpoint", 0.2, cluster_checkpoint),
        rule("backup", 0.3, backup),
        rule("check_store", 0.3, check_store),
        rule("event_sinks", 0.3, event_sinks),
        rule("cluster_state", 0.3, cluster_state),
    ]
}

pub fn setup_hooks() -> Vec<SetupHook> {
    vec![SetupHook { name: "ops_setup", run: ops_setup }]
}

// ---------------------------------------------------------------- rule clock

/// The wall-time budget of one rule body (COMMON: nothing in a rule waits
/// more than about 10 s).
pub const RULE_BUDGET: Duration = Duration::from_secs(10);
/// Below this, a call is not worth starting.
const MIN_CALL: Duration = Duration::from_millis(500);

/// One deadline for a whole rule body: every call takes `min(cap, left)`,
/// and optional steps are skipped once too little is left.
pub struct Clock {
    end: Instant,
}

impl Clock {
    pub fn new(budget: Duration) -> Clock {
        Clock { end: Instant::now() + budget }
    }
    pub fn left(&self) -> Duration {
        self.end.saturating_duration_since(Instant::now())
    }
    /// `min(cap, left)`, or `None` when less than `MIN_CALL` is left.
    pub fn cap(&self, cap: Duration) -> Option<Duration> {
        let left = self.left();
        (left >= MIN_CALL).then(|| cap.min(left))
    }
}

// ---------------------------------------------------------------- quiet watch

/// Whether a quiet period may be open, for job threads (00c's `Shared` has
/// no flag; "Requests to 99"). Rules are parked while a quiet period runs
/// (DESIGN §4.10), so a task-12 rule that runs knows that every quiet period
/// started so far has ended. A job then treats a quiet period as open when
/// one started after that observation and none ended since it (or two
/// started, conservatively).
#[derive(Default)]
pub struct QuietWatch {
    seen: Mutex<Option<(u64, Instant)>>,
}

/// Called at the start of every task-12 rule body.
fn note_outside_quiet(s: &Shared) {
    let e = s.next_epoch.load(Ordering::SeqCst);
    *s.ext::<QuietWatch>().seen.lock().unwrap_or_else(|e| e.into_inner()) = Some((e, Instant::now()));
}

pub fn quiet_maybe_open(s: &Shared) -> bool {
    // The real flag (added by 99), plus the inference for the window between
    // the epoch bump and the flag store.
    if s.quiet_open() {
        return true;
    }
    let Some((e_seen, at)) = *s.ext::<QuietWatch>().seen.lock().unwrap_or_else(|e| e.into_inner()) else { return false };
    let e_now = s.next_epoch.load(Ordering::SeqCst);
    let last_end = *s.last_quiet_end.lock().unwrap_or_else(|e| e.into_inner());
    e_now > e_seen + 1 || (e_now > e_seen && last_end <= at)
}

// ---------------------------------------------------------------- classification

/// The answer says nothing about the server: no answer at all, or the
/// client could not dial (a client-local `dial:` error). Other client-local
/// errors (marshalling a request or response, the local message-size limit)
/// are driver/client bugs, never transient.
fn no_answer(o: &Outcome) -> bool {
    o.is_unknown() || (!o.ok && o.local && o.message.starts_with("dial:"))
}

/// A transient (retry-safe, inconclusive) error: no answer, a remote
/// Unavailable, DeadlineExceeded or Canceled, plus the old workload's
/// `IsTransient` reasons (`EXTERNAL_SERVICE_ERROR`, ResourceExhausted +
/// `WRITES_BLOCKED_DISK_FULL`). This is the old `IsTolerated` set.
pub fn transient(o: &Outcome) -> bool {
    if o.is_ok() {
        return false;
    }
    no_answer(o)
        || (!o.local && matches!(o.code.as_str(), "Unavailable" | "DeadlineExceeded" | "Canceled"))
        || (!o.local && o.reason == "EXTERNAL_SERVICE_ERROR")
        || (!o.local && o.code == "ResourceExhausted" && o.reason == "WRITES_BLOCKED_DISK_FULL")
}

/// The backup drivers' tolerance: `IsTransient` plus caller cancellation.
/// A caller-side deadline or cancellation shows up as no answer or
/// DeadlineExceeded; a remote `Canceled` is not tolerated (`backup.go:36-45`).
pub fn backup_transient(o: &Outcome) -> bool {
    transient(o) && o.code != "Canceled"
}

/// The old `IsClassified`: ok, transient, or a known business code.
pub fn classified(o: &Outcome) -> bool {
    o.is_ok() || transient(o) || (!o.local && matches!(o.code.as_str(), "NotFound" | "AlreadyExists" | "InvalidArgument" | "FailedPrecondition"))
}

pub const BUSY_SUFFIX: &str = "backup: destination already has a running job";

/// `IsBackupInProgress`: FailedPrecondition whose message ends with the
/// destination-exclusion sentinel (exact suffix; the server also uses
/// FailedPrecondition for job-id collisions and missing checkpoints).
pub fn backup_busy(o: &Outcome) -> bool {
    !o.is_ok() && !o.is_unknown() && !o.local && o.code == "FailedPrecondition" && o.message.ends_with(BUSY_SUFFIX)
}

/// `IsNoFullCheckpoint`.
pub fn no_full_checkpoint(o: &Outcome) -> bool {
    !o.is_ok() && !o.is_unknown() && !o.local && o.code == "FailedPrecondition" && o.message.contains("full checkpoint")
}

fn resp(o: &Outcome) -> Value {
    o.response.clone().unwrap_or_else(|| json!({}))
}

// ---------------------------------------------------------------- barrier_monotonic

/// A per-node leadership signature: `node → (boot, transitions on that boot)`.
pub type LeaderSig = BTreeMap<String, (u64, u64)>;

pub fn leader_sig(ns: &NodeStates) -> LeaderSig {
    ns.latest.iter().map(|(n, st)| (n.clone(), (st.boot, st.transitions))).collect()
}

/// A leadership transition was seen between two signatures (a node gained
/// transitions on the same boot, or a new boot of a node already has some).
pub fn leadership_changed(prev: &LeaderSig, now: &LeaderSig) -> bool {
    now.iter().any(|(n, &(boot, tr))| match prev.get(n) {
        Some(&(pboot, ptr)) if pboot == boot => tr > ptr,
        _ => tr > 0,
    })
}

#[derive(Default)]
pub struct BarrierState {
    /// Highest commit index any completed barrier returned (driver-global).
    pub high: AtomicU64,
    /// Leadership signature at the previous successful barrier.
    pub last_sig: Mutex<Option<LeaderSig>>,
}

fn barrier_monotonic(d: &Driver, tc: TestCase) {
    d.guarded("barrier_monotonic", &tc, |d, tc| {
        let route = draw::route(tc, &d.s);
        note_outside_quiet(&d.s);
        let st = d.s.ext::<BarrierState>();
        // Read before the call: only barriers that completed before this one
        // started give a valid floor.
        let floor = st.high.load(Ordering::SeqCst);
        let (_, out) = d.call(&route, "bucket", "Barrier", json!({}), &CallOpts::default());
        if !out.is_ok() {
            if !transient(&out) {
                check_unreachable!("barrier monotonic driver got non-transient error", json!({"route": route.via(), "outcome": out.summary()}));
            }
            return Some(());
        }
        let idx = u64_field(&resp(&out), "commitIndex");
        let details = json!({"floor": floor, "got": idx, "route": route.via()});
        check_always!(idx > 0, "a barrier's commit index is positive", details);
        check_always!(idx >= floor, "the barrier commit index never goes back", details);
        st.high.fetch_max(idx, Ordering::SeqCst);
        let now = leader_sig(&d.s.node_states.lock().unwrap_or_else(|e| e.into_inner()));
        let prev = st.last_sig.lock().unwrap_or_else(|e| e.into_inner()).replace(now.clone());
        let changed = prev.as_ref().is_some_and(|p| leadership_changed(p, &now));
        check_sometimes!(changed, "barrier succeeded after an observed leadership change", json!({"commitIndex": idx}));
        Some(())
    });
}

// ---------------------------------------------------------------- transfer_leadership

pub const TRANSFER_TIMEOUT: Duration = Duration::from_secs(8);
const STATE_TIMEOUT: Duration = Duration::from_secs(2);
/// Total time `ops_setup` spends on the ops ledger (DESIGN §6.4: setup has
/// 3 minutes for everything).
pub const OPS_SETUP_BUDGET: Duration = Duration::from_secs(20);

/// The voters of a `ClusterState` other than `exclude`, by id (learners never).
pub fn voters_excluding(cs: &Value, exclude: &[u64]) -> Vec<u64> {
    let mut v: Vec<u64> = cs
        .get("nodes")
        .and_then(Value::as_array)
        .map(|ns| ns.iter().filter(|n| str_field(n, "suffrage") == "Voter").map(|n| u64_field(n, "id")).filter(|id| *id != 0 && !exclude.contains(id)).collect())
        .unwrap_or_default();
    v.sort_unstable();
    v.dedup();
    v
}

/// The Rust-checked ledger of this family (created by `ops_setup`, or
/// lazily by `transfer_leadership` when it is missing).
pub fn ops_ledger(s: &Shared) -> String {
    format!("r-ops-{}", s.run)
}

fn create_ops_ledger(d: &Driver, budget: Duration) -> bool {
    let name = ops_ledger(&d.s);
    let mut send = UntrackedSend::new(vec![json!({"createLedger": {"name": name}})], Some(d.s.next_untracked_key()));
    send.schedule = (0..d.s.clients.len().max(1)).map(Route::lib).collect();
    send.budget = budget;
    send.attempt_timeout = send.attempt_timeout.min(budget);
    let r = d.send_untracked(send);
    let ok = r.outcome.is_ok() || r.outcome.reason == "LEDGER_ALREADY_EXISTS";
    if !ok {
        println!("driver: creating {name}: {}", r.outcome.summary());
    }
    ok
}

fn ops_setup(d: &Driver) {
    ops_setup_within(d, OPS_SETUP_BUDGET);
}

pub fn ops_setup_within(d: &Driver, budget: Duration) {
    let clock = Clock::new(budget);
    let name = ops_ledger(&d.s);
    while let Some(b) = clock.cap(Duration::from_secs(3)) {
        if create_ops_ledger(d, b) {
            println!("setup: ledger {name} ready");
            return;
        }
        std::thread::sleep(Duration::from_secs(1).min(clock.left()));
    }
    println!("setup: giving up on {name}; transfer_leadership creates it lazily");
}

/// One TransferLeadership with its assertions. True on success.
fn transfer_once(d: &Driver, route: &Route, from: u64, target: u64, burst: bool, timeout: Duration) -> bool {
    let (_, out) = d.call(route, "cluster", "TransferLeadership", json!({"transferee": target.to_string()}), &CallOpts::default().timeout(timeout));
    let details = json!({"currentLeader": from, "targetNode": target, "burst": burst, "outcome": out.summary()});
    check_sometimes!(out.is_ok(), "should be able to transfer leadership", details);
    if !out.is_ok() {
        return false;
    }
    let new_leader = u64_field(&resp(&out), "newLeader");
    check_always_or_unreachable!(new_leader == target, "new leader should be the requested target", json!({"currentLeader": from, "targetNode": target, "newLeader": new_leader}));
    check_reachable!("leadership transfer completed", json!({"currentLeader": from, "targetNode": target, "burst": burst}));
    true
}

fn transfer_leadership(d: &Driver, tc: TestCase) {
    d.guarded("transfer_leadership", &tc, |d, tc| {
        // All draws first.
        let route = draw::route(tc, &d.s);
        let pick1 = tc.draw(hegel::generators::integers::<u32>());
        let burst = draw::chance(tc, 0.25);
        let pick2 = tc.draw(hegel::generators::integers::<u32>());
        let schedule = draw::schedule(tc, &d.s, 4);
        let backoffs = draw::backoffs(tc, 4);
        note_outside_quiet(&d.s);
        let clock = Clock::new(RULE_BUDGET);

        let (_, out) = d.call(&route, "cluster", "GetClusterState", json!({}), &CallOpts::default().timeout(STATE_TIMEOUT));
        if !out.is_ok() {
            if !transient(&out) {
                check_unreachable!("GetClusterState should not fail", json!({"outcome": out.summary()}));
            }
            return Some(());
        }
        let cs = resp(&out);
        let leader = u64_field(&cs, "leader");
        if leader == 0 {
            return Some(());
        }
        let c1 = voters_excluding(&cs, &[leader]);
        if c1.is_empty() {
            return Some(());
        }
        let t1 = c1[pick1 as usize % c1.len()];
        let timeout = clock.cap(TRANSFER_TIMEOUT)?;
        if !transfer_once(d, &route, leader, t1, false, timeout) {
            return Some(());
        }
        if burst {
            // Right after: a second transfer to another voter (bloom rebuild
            // racing a populate, server-signals §1.4; cache epoch §1.5).
            // Capped so the witness write keeps about 3 s.
            let c2 = voters_excluding(&cs, &[t1]);
            let t = clock.left().saturating_sub(Duration::from_secs(3));
            if !c2.is_empty() && t >= MIN_CALL {
                let t2 = c2[pick2 as usize % c2.len()];
                let _ = transfer_once(d, &route, t1, t2, true, t.min(TRANSFER_TIMEOUT));
            }
        }
        // A business write right after the transfer (untracked, keyed).
        let ledger = ops_ledger(&d.s);
        let budget = clock.cap(Duration::from_secs(5))?;
        let req = create_tx_request(&ledger, &[Posting::new("world", "ops:transfer", 1u64, "COIN")], None);
        let mut send = UntrackedSend::new(vec![req], Some(d.s.next_untracked_key()));
        send.schedule = schedule;
        send.backoffs_ms = backoffs;
        send.budget = budget;
        send.attempt_timeout = send.attempt_timeout.min(budget);
        let w = d.send_untracked(send);
        check_sometimes!(w.outcome.is_ok(), "business write commits after leadership transfer", json!({"ledger": ledger, "outcome": w.outcome.summary()}));
        if !w.outcome.is_ok() {
            if w.outcome.reason == "LEDGER_NOT_FOUND" {
                // `ops_setup` gave up (or the ledger is gone): create it for the next run.
                if let Some(b) = clock.cap(Duration::from_secs(3)) {
                    let _ = create_ops_ledger(d, b);
                }
            }
            return Some(());
        }
        let t = clock.cap(STATE_TIMEOUT)?;
        let (_, after) = d.call(&route, "cluster", "GetClusterState", json!({}), &CallOpts::default().timeout(t));
        if after.is_ok() {
            let l = u64_field(&resp(&after), "leader");
            check_sometimes!(l != 0, "cluster should have a leader after transfer", json!({"leader": l, "targetNode": t1}));
        }
        Some(())
    });
}

// ---------------------------------------------------------------- compaction

pub const COMPACTION_TIMEOUT: Duration = Duration::from_secs(6);
const READ_TIMEOUT: Duration = Duration::from_secs(2);

fn compaction(d: &Driver, tc: TestCase) {
    d.guarded("compaction", &tc, |d, tc| {
        let primary = draw::chance(tc, 0.5);
        let route = draw::route(tc, &d.s);
        note_outside_quiet(&d.s);
        let clock = Clock::new(RULE_BUDGET);
        let method = if primary { "CompactPrimary" } else { "CompactSecondary" };
        let (_, out) = d.call(&route, "cluster", method, json!({}), &CallOpts::default().timeout(COMPACTION_TIMEOUT));
        let tolerated = out.is_ok() || transient(&out);
        let details = json!({"route": route.via(), "outcome": out.summary()});
        if primary {
            check_sometimes!(tolerated, "primary compaction should succeed or be transiently unavailable", details);
        } else {
            check_sometimes!(tolerated, "secondary compaction should succeed or be transiently unavailable", details);
        }
        if !out.is_ok() {
            return Some(());
        }
        let r = resp(&out);
        if primary {
            check_reachable!("primary compaction completed", json!({"durationMs": r.get("durationMs")}));
        } else {
            check_reachable!("secondary compaction completed", json!({"durationMs": r.get("durationMs"), "sizeBeforeBytes": r.get("sizeBeforeBytes"), "sizeAfterBytes": r.get("sizeAfterBytes")}));
        }
        verify_readable(d, &route, primary, &clock);
        Some(())
    });
}

/// GetLedgerStats and GetAccount(`world`) on `default` after a compaction.
fn verify_readable(d: &Driver, route: &Route, primary: bool, clock: &Clock) {
    let Some(t) = clock.cap(READ_TIMEOUT) else { return };
    let (_, stats) = d.call(route, "bucket", "GetLedgerStats", json!({"ledger": "default"}), &CallOpts::linearizable().timeout(t));
    if !stats.is_ok() {
        if !transient(&stats) {
            check_unreachable!("GetLedgerStats should not fail after compaction", json!({"primary": primary, "outcome": stats.summary()}));
        }
        return;
    }
    let Some(t) = clock.cap(READ_TIMEOUT) else { return };
    let (_, acc) = d.call(route, "bucket", "GetAccount", json!({"ledger": "default", "address": "world"}), &CallOpts::linearizable().timeout(t));
    check_always_or_unreachable!(classified(&acc), "world account must be readable after compaction", json!({"primary": primary, "outcome": acc.summary()}));
}

// ---------------------------------------------------------------- cluster_checkpoint

fn cluster_checkpoint(d: &Driver, tc: TestCase) {
    d.guarded("cluster_checkpoint", &tc, |d, tc| {
        let route = draw::route(tc, &d.s);
        note_outside_quiet(&d.s);
        let (_, out) = d.call(&route, "cluster", "CreateCheckpoint", json!({}), &CallOpts::default().timeout(Duration::from_secs(8)));
        check_sometimes!(out.is_ok(), "a checkpoint is created", json!({"route": route.via(), "outcome": out.summary(), "checkpointId": out.response.as_ref().and_then(|r| r.get("checkpointId")).cloned()}));
        Some(())
    });
}

// ---------------------------------------------------------------- long jobs

/// At most one long Rust-only job (backup or CheckStore) is queued or
/// running, so one of the two job threads always stays free for other
/// families' jobs (task 04's quiet-blocking maintenance disable).
#[derive(Default)]
pub struct LongJob {
    pub inflight: AtomicBool,
}

/// Clears an in-flight flag when dropped (also on a job panic).
struct ClearOnDrop(Arc<dyn Fn() + Send + Sync>);
impl Drop for ClearOnDrop {
    fn drop(&mut self) {
        (self.0)()
    }
}

/// Claims the long-job slot. `None` when another long job holds it, or a
/// quiet-blocking job is queued or running (it must not wait behind ours).
fn claim_long_job(s: &Shared) -> Option<ClearOnDrop> {
    if s.blocking_jobs() > 0 {
        return None;
    }
    let lj = s.ext::<LongJob>();
    if lj.inflight.swap(true, Ordering::SeqCst) {
        return None;
    }
    Some(ClearOnDrop(Arc::new(move || lj.inflight.store(false, Ordering::SeqCst))))
}

// ---------------------------------------------------------------- backup

pub const BACKUP_TIMEOUT: Duration = Duration::from_secs(90);
pub const BUSY_WINDOW: Duration = Duration::from_secs(120);
/// Total wall time of one backup job (all its calls, busy retries and quiet
/// waits); a call never outlives it.
pub const BACKUP_JOB_BUDGET: Duration = Duration::from_secs(180);

/// One backup sequence: a full backup, then optionally two incrementals on
/// that export chain (the old `singleton_driver_incremental_backup` shape).
#[derive(Clone, Debug)]
pub struct BackupChain {
    pub incremental: bool,
    pub routes: Vec<Route>,
    pub busy_backoffs_ms: Vec<u64>,
}

#[derive(Clone, Debug)]
pub struct BackupPlan {
    pub chains: Vec<BackupChain>,
    /// Highest completed barrier commit index when the rule ran.
    pub barrier_floor: u64,
    pub call_timeout: Duration,
    pub busy_window: Duration,
    pub budget: Duration,
}

/// Full backups this driver has in flight, and an epoch bumped before and
/// after each full Backup call. A full backup replaces the destination's
/// checkpoint and empties its export list, so two incrementals are
/// comparable only when no full backup ran between them.
#[derive(Default)]
pub struct BackupState {
    pub full_epoch: AtomicU64,
    pub fulls_inflight: AtomicU64,
}

impl BackupState {
    /// A marker for "no full backup now", or `None` when one is in flight.
    pub fn quiet_marker(&self) -> Option<u64> {
        let e = self.full_epoch.load(Ordering::SeqCst);
        (self.fulls_inflight.load(Ordering::SeqCst) == 0).then_some(e)
    }
}

/// Counts a full Backup call in flight (also on unwind).
struct FullGuard(Arc<BackupState>);
impl FullGuard {
    fn start(st: Arc<BackupState>) -> FullGuard {
        st.fulls_inflight.fetch_add(1, Ordering::SeqCst);
        st.full_epoch.fetch_add(1, Ordering::SeqCst);
        FullGuard(st)
    }
}
impl Drop for FullGuard {
    fn drop(&mut self) {
        self.0.full_epoch.fetch_add(1, Ordering::SeqCst);
        self.0.fulls_inflight.fetch_sub(1, Ordering::SeqCst);
    }
}

pub fn backup_storage() -> Value {
    json!({"storage": {"s3": {"bucket": "backups", "region": "us-east-1", "endpoint": "http://minio:9000"}}})
}

/// Lib or Plain on a drawn client (Backup forwards to the leader itself).
fn backup_route(tc: &TestCase, s: &Shared) -> Route {
    let client = draw::pick(tc, s.clients.len().max(1));
    if draw::chance(tc, 0.5) {
        Route::lib(client)
    } else {
        Route::plain(client)
    }
}

fn draw_chain(tc: &TestCase, s: &Shared) -> BackupChain {
    let incremental = draw::int_in(tc, 0, 2) == 2;
    let routes = (0..4).map(|_| backup_route(tc, s)).collect();
    let busy_backoffs_ms = (0..8).map(|_| draw::int_in(tc, 500, 5000)).collect();
    BackupChain { incremental, routes, busy_backoffs_ms }
}

fn backup(d: &Driver, tc: TestCase) {
    d.guarded("backup", &tc, |d, tc| {
        let n = if draw::chance(tc, 1.0 / 6.0) { 2 } else { 1 };
        let chains: Vec<BackupChain> = (0..n).map(|_| draw_chain(tc, &d.s)).collect();
        note_outside_quiet(&d.s);
        let guard = claim_long_job(&d.s)?;
        let barrier_floor = d.s.ext::<BarrierState>().high.load(Ordering::SeqCst);
        let plan = BackupPlan { chains, barrier_floor, call_timeout: BACKUP_TIMEOUT, busy_window: BUSY_WINDOW, budget: BACKUP_JOB_BUDGET };
        d.submit_job(Job {
            name: "backup",
            blocks_quiet: false,
            run: Box::new(move |d| {
                let _guard = guard;
                run_backup_plan(d, &plan);
            }),
        });
        Some(())
    });
}

pub fn run_backup_plan(d: &Driver, plan: &BackupPlan) {
    let deadline = Instant::now() + plan.budget;
    if plan.chains.len() == 1 {
        run_chain(d, plan, &plan.chains[0], deadline);
        return;
    }
    std::thread::scope(|s| {
        let hs: Vec<_> = plan.chains.iter().map(|c| crate::assertions::scoped_inheriting(s, move || Driver::helper_iteration("backup chain", || run_chain(d, plan, c, deadline)))).collect();
        for h in hs {
            let _ = h.join();
        }
    });
}

/// Waits (bounded by `deadline`) while a quiet period may be open: Backup
/// proposes Start/Complete orders, which would spoil task 15's fenced
/// checks. False when the deadline came first.
fn wait_outside_quiet(d: &Driver, deadline: Instant) -> bool {
    while quiet_maybe_open(&d.s) {
        if Instant::now() + Duration::from_millis(500) >= deadline {
            return false;
        }
        std::thread::sleep(Duration::from_millis(500));
    }
    true
}

/// One Backup/IncrementalBackup with the destination-busy retry (≤
/// `busy_window` from the first busy answer). Returns the last outcome, or
/// `None` when the job budget ran out (or a quiet period stayed open) before
/// a call could start: inconclusive.
pub fn backup_call(d: &Driver, plan: &BackupPlan, chain: &BackupChain, method: &str, deadline: Instant) -> Option<Outcome> {
    let mut first_busy: Option<Instant> = None;
    let mut k = 0usize;
    loop {
        if !wait_outside_quiet(d, deadline) {
            println!("driver: {method} not started: a quiet period stayed open until the job budget ran out");
            return None;
        }
        let left = deadline.saturating_duration_since(Instant::now());
        if left < Duration::from_secs(1) {
            println!("driver: {method} not started: backup job budget spent");
            return None;
        }
        let route = &chain.routes[k % chain.routes.len().max(1)];
        let out = {
            let _full = (method == "Backup").then(|| FullGuard::start(d.s.ext::<BackupState>()));
            d.call(route, "cluster", method, backup_storage(), &CallOpts::default().timeout(plan.call_timeout.min(left))).1
        };
        let busy = backup_busy(&out);
        if busy && first_busy.is_none() {
            first_busy = Some(Instant::now());
        }
        if first_busy.is_some() {
            check_sometimes!(out.is_ok(), "backup recovery after destination busy succeeds", json!({"operation": method, "attempts": k + 1, "outcome": out.summary()}));
        }
        let Some(t0) = first_busy.filter(|_| busy) else { return Some(out) };
        let stop = (t0 + plan.busy_window).min(deadline);
        let now = Instant::now();
        if now >= stop {
            println!("driver: {method} destination still busy after {} attempts; inconclusive", k + 1);
            return Some(out);
        }
        let wait = Duration::from_millis(chain.busy_backoffs_ms[k % chain.busy_backoffs_ms.len().max(1)]).min(stop - now);
        std::thread::sleep(wait);
        k += 1;
    }
}

pub fn run_chain(d: &Driver, plan: &BackupPlan, chain: &BackupChain, deadline: Instant) {
    let Some(full) = backup_call(d, plan, chain, "Backup", deadline) else { return };
    if !full.is_ok() {
        let tolerated = backup_busy(&full) || backup_transient(&full);
        if tolerated {
            println!("driver: Backup inconclusive: {}", full.summary());
        } else if chain.incremental {
            check_unreachable!("Backup (pre-incremental) returned unexpected error", json!({"outcome": full.summary()}));
        } else {
            check_unreachable!("Backup returned unexpected error", json!({"outcome": full.summary()}));
        }
        return;
    }
    let r = resp(&full);
    let total = u64_field(&r, "totalFiles");
    let applied = u64_field(&r, "lastAppliedIndex");
    let details = json!({"filesUploaded": u64_field(&r, "filesUploaded"), "totalFiles": total, "durationMs": r.get("durationMs"), "lastLogSequence": u64_field(&r, "lastLogSequence"), "lastAppliedIndex": applied, "barrierFloor": plan.barrier_floor});
    check_always_or_unreachable!(total > 0, "backup should produce files", details);
    check_reachable!("backup completed successfully", details);
    // Backup applies its Start order before taking the checkpoint, and a
    // proposal resolves only after its Pebble commit, so the checkpoint's
    // applied index is past every barrier that completed before the rule ran.
    check_always!(applied >= plan.barrier_floor, "a full backup's applied index covers every barrier completed before it started", details);
    if !chain.incremental {
        return;
    }
    let st = d.s.ext::<BackupState>();
    let before = st.quiet_marker();
    let Some(inc) = backup_call(d, plan, chain, "IncrementalBackup", deadline) else { return };
    if !inc.is_ok() {
        if !(backup_busy(&inc) || backup_transient(&inc) || no_full_checkpoint(&inc)) {
            check_unreachable!("IncrementalBackup returned unexpected error", json!({"outcome": inc.summary()}));
        } else {
            println!("driver: IncrementalBackup inconclusive: {}", inc.summary());
        }
        return;
    }
    let r1 = resp(&inc);
    let (log1, audit1) = (u64_field(&r1, "lastLogSequence"), u64_field(&r1, "lastAuditSequence"));
    let details = json!({"logEntriesExported": u64_field(&r1, "logEntriesExported"), "auditEntriesExported": u64_field(&r1, "auditEntriesExported"), "segmentsUploaded": u64_field(&r1, "segmentsUploaded"), "durationMs": r1.get("durationMs"), "lastLogSequence": log1, "lastAuditSequence": audit1});
    check_reachable!("incremental backup completed", details);

    // A second incremental right away on the same export chain ("no new
    // entries" path, continuing an existing chain).
    let Some(inc2) = backup_call(d, plan, chain, "IncrementalBackup", deadline) else { return };
    if !inc2.is_ok() {
        if !(backup_busy(&inc2) || backup_transient(&inc2) || no_full_checkpoint(&inc2)) {
            check_unreachable!("second IncrementalBackup returned unexpected error", json!({"outcome": inc2.summary()}));
        } else {
            println!("driver: second IncrementalBackup inconclusive: {}", inc2.summary());
        }
        return;
    }
    let after = st.quiet_marker();
    let r2 = resp(&inc2);
    let (log2, audit2) = (u64_field(&r2, "lastLogSequence"), u64_field(&r2, "lastAuditSequence"));
    if !incrementals_comparable(before, after) {
        // Another chain's full backup may have restarted the export chain
        // between the two incrementals: the second may start lower.
        println!("driver: incrementals {log1}/{audit1} and {log2}/{audit2} not compared: a full backup ran in between");
        return;
    }
    check_always_or_unreachable!(log2 >= log1, "incremental backup log sequence should not regress", json!({"firstLastLogSeq": log1, "secondLastLogSeq": log2}));
    check_always_or_unreachable!(audit2 >= audit1, "incremental backup audit sequence should not regress", json!({"firstLastAuditSeq": audit1, "secondLastAuditSeq": audit2}));
}

/// Two incrementals are on one export chain when no full backup was in
/// flight before the first or after the second, and none started or ended
/// in between.
pub fn incrementals_comparable(before: Option<u64>, after: Option<u64>) -> bool {
    matches!((before, after), (Some(a), Some(b)) if a == b)
}

// ---------------------------------------------------------------- check_store

pub const CHECK_STORE_TIMEOUT: Duration = Duration::from_secs(60);
pub const CHECK_STORE_MAX_ITEMS: u64 = 100_000;

fn check_store(d: &Driver, tc: TestCase) {
    d.guarded("check_store", &tc, |d, tc| {
        let route = draw::route(tc, &d.s);
        note_outside_quiet(&d.s);
        let guard = claim_long_job(&d.s)?;
        d.submit_job(Job {
            name: "check_store",
            blocks_quiet: false,
            run: Box::new(move |d| {
                let _guard = guard;
                run_check_store(d, &route, CHECK_STORE_TIMEOUT);
            }),
        });
        Some(())
    });
}

pub fn run_check_store(d: &Driver, route: &Route, timeout: Duration) {
    let opts = CallOpts::default().timeout(timeout).max_items(CHECK_STORE_MAX_ITEMS);
    let (_, out) = d.call(route, "bucket", "CheckStore", json!({}), &opts);
    if !out.is_ok() {
        if !transient(&out) {
            check_unreachable!("CheckStore returned unexpected error", json!({"route": route.via(), "outcome": out.summary()}));
        }
        return;
    }
    let items = out.items.clone().unwrap_or_default();
    let errors: Vec<&Value> = items.iter().filter(|e| e.get("error").is_some()).collect();
    let logs_checked = items.iter().filter_map(|e| e.get("progress")).map(|p| u64_field(p, "logsChecked")).max().unwrap_or(0);
    check_always_or_unreachable!(errors.is_empty(), "CheckStore found integrity error", json!({"errors": errors.iter().take(5).collect::<Vec<_>>(), "count": errors.len()}));
    if out.complete {
        check_reachable!("CheckStore completed successfully", json!({"logsChecked": logs_checked}));
    }
}

// ---------------------------------------------------------------- event_sinks

pub const SINK_TOPICS: usize = 10;
const SINK_BUDGET: Duration = Duration::from_secs(3);
const SINK_CLEANUP_BUDGET: Duration = Duration::from_secs(2);
const LEFTOVER_CAP: usize = 16;

#[derive(Default)]
pub struct SinkState {
    pub next: AtomicU64,
    /// Sinks whose add or remove had no definitive outcome (maybe present).
    pub leftovers: Mutex<Vec<String>>,
}

pub fn sink_names(resp: &Value) -> Vec<String> {
    resp.get("sinks").and_then(Value::as_array).map(|ss| ss.iter().map(|s| str_field(s, "name").to_string()).collect()).unwrap_or_default()
}

fn sink_send(d: &Driver, req: Value, schedule: &[Route], backoffs: &[u64], budget: Duration) -> Outcome {
    let mut send = UntrackedSend::new(vec![req], Some(d.s.next_untracked_key()));
    send.schedule = schedule.to_vec();
    send.backoffs_ms = backoffs.to_vec();
    send.attempt_timeout = Duration::from_secs(2).min(budget);
    send.budget = budget;
    d.send_untracked(send).outcome
}

fn leftover(st: &SinkState, name: String) {
    let mut l = st.leftovers.lock().unwrap_or_else(|e| e.into_inner());
    if l.len() < LEFTOVER_CAP {
        l.push(name);
    }
}

fn event_sinks(d: &Driver, tc: TestCase) {
    d.guarded("event_sinks", &tc, |d, tc| {
        let topic = format!("ledger.events.{}", draw::int_in(tc, 0, SINK_TOPICS as u64 - 1));
        let schedule = draw::schedule(tc, &d.s, 4);
        let backoffs = draw::backoffs(tc, 4);
        let read_route = draw::route(tc, &d.s);
        note_outside_quiet(&d.s);
        let clock = Clock::new(RULE_BUDGET);

        let st = d.s.ext::<SinkState>();
        // Clean up one sink an earlier run may have left behind (no assertion).
        let old = st.leftovers.lock().unwrap_or_else(|e| e.into_inner()).pop();
        if let Some(old) = old {
            let out = sink_send(d, json!({"removeEventsSink": {"name": old}}), &schedule, &backoffs, SINK_CLEANUP_BUDGET);
            if !definitive(&out) {
                leftover(&st, old);
            }
        }

        let name = format!("sink-{}-{}", d.s.run, st.next.fetch_add(1, Ordering::SeqCst));
        let details = json!({"sinkName": name, "topic": topic});
        let cfg = json!({"addEventsSink": {"config": {"name": name, "nats": {"url": "nats://nats:4222", "topic": topic}, "format": "json", "batchSize": 32}}});
        let add = sink_send(d, cfg, &schedule, &backoffs, clock.cap(SINK_BUDGET)?);
        check_sometimes!(add.is_ok() || transient(&add), "should be able to add NATS event sink", json!({"sinkName": name, "outcome": add.summary()}));
        if !add.is_ok() {
            if !definitive(&add) {
                leftover(&st, name);
            }
            return Some(());
        }
        // From here the sink exists: when time runs out, leave it for cleanup.
        let Some(t) = clock.cap(Duration::from_secs(2)) else {
            leftover(&st, name);
            return Some(());
        };
        let (_, list) = d.call(&read_route, "bucket", "GetEventsSinks", json!({}), &CallOpts::linearizable().timeout(t));
        if list.is_ok() {
            let found = sink_names(&resp(&list)).iter().any(|n| *n == name);
            check_always_or_unreachable!(found, "added sink should appear in GetEventsSinks", details);
        }
        let Some(b) = clock.cap(SINK_BUDGET) else {
            leftover(&st, name);
            return Some(());
        };
        let rm = sink_send(d, json!({"removeEventsSink": {"name": name}}), &schedule, &backoffs, b);
        check_sometimes!(rm.is_ok() || transient(&rm), "should be able to remove event sink", json!({"sinkName": name, "outcome": rm.summary()}));
        if !rm.is_ok() {
            if !definitive(&rm) {
                leftover(&st, name);
            }
            return Some(());
        }
        let t = clock.cap(Duration::from_secs(2))?;
        let (_, list) = d.call(&read_route, "bucket", "GetEventsSinks", json!({}), &CallOpts::linearizable().timeout(t));
        if list.is_ok() {
            let found = sink_names(&resp(&list)).iter().any(|n| *n == name);
            check_always_or_unreachable!(!found, "removed sink should not appear in GetEventsSinks", details);
        }
        Some(())
    });
}

// ---------------------------------------------------------------- cluster_state

#[derive(Default)]
pub struct NodeCache {
    pub map: Mutex<Option<NodeMap>>,
}

/// A member map worth caching: every member has a service address, and it
/// lists at least as many members as the configured node list.
pub fn node_map_complete(m: &NodeMap, configured: usize) -> bool {
    !m.nodes.is_empty() && m.nodes.len() >= configured && m.nodes.iter().all(|n| !n.addr.is_empty())
}

/// What one node's own `GetClusterState{nodeId}` says (None when the answer
/// is not that node's).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NodeView {
    Leader { snapshot_progress: bool },
    Follower { out_of_sync: bool },
}

pub fn node_view(cs: &Value, id: u64) -> Option<NodeView> {
    if u64_field(cs, "localNode") != id {
        return None;
    }
    let leader = u64_field(cs, "leader");
    if leader == id {
        // The server maps etcd's `StateSnapshot` to "Snapshot" (node.go `progress` map).
        let snap = cs.pointer("/raftStatus/progress").and_then(Value::as_object).is_some_and(|p| p.values().any(|pi| str_field(pi, "state") == "Snapshot"));
        Some(NodeView::Leader { snapshot_progress: snap })
    } else {
        let status = cs.pointer("/syncProgress/status").and_then(Value::as_str).unwrap_or("");
        Some(NodeView::Follower { out_of_sync: matches!(status, "syncing" | "snapshotting" | "out_of_sync") })
    }
}

fn cluster_state(d: &Driver, tc: TestCase) {
    d.guarded("cluster_state", &tc, |d, tc| {
        let client = draw::pick(tc, d.s.clients.len().max(1));
        note_outside_quiet(&d.s);
        let clock = Clock::new(RULE_BUDGET);
        let cache = d.s.ext::<NodeCache>();
        let known = cache.map.lock().unwrap_or_else(|e| e.into_inner()).clone();
        let map = match known {
            Some(m) => m,
            None => {
                // One bounded discovery call (not `NodeMap::discover`, which
                // tries every client for up to 3 s each).
                let (_, out) = d.call(&Route::lib(client), "cluster", "GetClusterState", json!({}), &CallOpts::default().timeout(STATE_TIMEOUT));
                let m = NodeMap::from_cluster_state(out.response.as_ref().filter(|_| out.is_ok())?);
                if node_map_complete(&m, d.s.nodes.len()) {
                    *cache.map.lock().unwrap_or_else(|e| e.into_inner()) = Some(m.clone());
                }
                m
            }
        };
        for n in map.nodes.iter().filter(|n| !n.addr.is_empty()) {
            // `cluster_state_via` takes up to 2 s.
            if clock.left() < STATE_TIMEOUT {
                break;
            }
            let Some(cs) = d.cluster_state_via(client, &n.addr, n.id) else { continue };
            match node_view(&cs, n.id) {
                Some(NodeView::Follower { out_of_sync }) => {
                    check_sometimes!(out_of_sync, "a follower reports an out-of-sync or snapshotting status", json!({"node": n.addr, "status": cs.pointer("/syncProgress/status")}));
                }
                Some(NodeView::Leader { snapshot_progress }) => {
                    check_sometimes!(snapshot_progress, "the leader reports a follower in snapshot progress", json!({"node": n.addr, "progress": cs.pointer("/raftStatus/progress")}));
                }
                None => {}
            }
        }
        Some(())
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testkit::{self, FakeReply};

    fn ok(v: Value) -> FakeReply {
        FakeReply::Outcome(Outcome::from_json(&json!({"ok": true, "response": v})))
    }
    fn err(code: &str, reason: &str, message: &str) -> FakeReply {
        FakeReply::Outcome(Outcome::from_json(&json!({"ok": false, "code": code, "reason": reason, "message": message})))
    }
    fn out_of(r: FakeReply) -> Outcome {
        match r {
            FakeReply::Outcome(o) => o,
            _ => unreachable!(),
        }
    }
    fn recs(msg: &str) -> Vec<crate::assertions::Recorded> {
        testkit::assertions().into_iter().filter(|r| r.message == msg).collect()
    }

    #[test]
    fn classifiers() {
        let busy = out_of(err("FailedPrecondition", "", "backup already in progress for this destination: backup: destination already has a running job"));
        assert!(backup_busy(&busy));
        // Exact suffix: a trailing addition, another code, or another message is not busy.
        assert!(!backup_busy(&out_of(err("FailedPrecondition", "", "backup: destination already has a running job (retry)"))));
        assert!(!backup_busy(&out_of(err("Unavailable", "", BUSY_SUFFIX))));
        assert!(!backup_busy(&out_of(err("FailedPrecondition", "", "job id collision"))));
        assert!(!backup_busy(&Outcome::unknown("timeout")));
        assert!(no_full_checkpoint(&out_of(err("FailedPrecondition", "", "incremental backup requires a full checkpoint at the destination"))));
        assert!(!no_full_checkpoint(&busy));

        for t in [err("Unavailable", "", ""), err("DeadlineExceeded", "", ""), err("Canceled", "", ""), err("FailedPrecondition", "EXTERNAL_SERVICE_ERROR", ""), err("ResourceExhausted", "WRITES_BLOCKED_DISK_FULL", "")] {
            assert!(transient(&out_of(t)));
        }
        assert!(transient(&Outcome::unknown("timeout")));
        // Client-local: only a dial failure is transient; a marshalling error
        // or the local size limit is a driver/client bug.
        assert!(transient(&Outcome::from_json(&json!({"ok": false, "code": "Internal", "message": "dial: no route", "local": true}))));
        assert!(!transient(&Outcome::from_json(&json!({"ok": false, "code": "Internal", "message": "marshal response: x", "local": true}))));
        assert!(!transient(&Outcome::from_json(&json!({"ok": false, "code": "ResourceExhausted", "message": "grpc: trying to send message larger than max", "local": true}))));
        assert!(!classified(&Outcome::from_json(&json!({"ok": false, "code": "Internal", "message": "unmarshal", "local": true}))));
        // Backups tolerate caller-side timeouts but not a remote Canceled.
        assert!(backup_transient(&out_of(err("DeadlineExceeded", "", ""))));
        assert!(backup_transient(&Outcome::unknown("timeout")));
        assert!(backup_transient(&out_of(err("FailedPrecondition", "EXTERNAL_SERVICE_ERROR", ""))));
        assert!(!backup_transient(&out_of(err("Canceled", "", "context canceled"))));
        for t in [err("Internal", "", ""), err("Aborted", "", ""), err("ResourceExhausted", "", ""), err("NotFound", "LEDGER_NOT_FOUND", "")] {
            assert!(!transient(&out_of(t)));
        }
        assert!(!transient(&out_of(ok(json!({})))));
        assert!(classified(&out_of(err("NotFound", "", ""))));
        assert!(classified(&out_of(err("FailedPrecondition", "", ""))));
        assert!(!classified(&out_of(err("Internal", "", ""))));
        assert!(!classified(&out_of(err("Aborted", "", ""))));
    }

    #[test]
    fn voter_selection_excludes_leader_and_learners() {
        let cs = json!({"leader": "1", "nodes": [
            {"id": "1", "suffrage": "Voter"}, {"id": "2", "suffrage": "Voter"},
            {"id": "3", "suffrage": "Learner"}, {"id": "4", "suffrage": "Voter"}]});
        assert_eq!(voters_excluding(&cs, &[1]), vec![2, 4]);
        assert_eq!(voters_excluding(&cs, &[2]), vec![1, 4]);
        assert_eq!(voters_excluding(&cs, &[1, 2, 4]), Vec::<u64>::new());
        assert_eq!(voters_excluding(&json!({}), &[]), Vec::<u64>::new());
    }

    #[test]
    fn leadership_change_detection() {
        let sig = |v: &[(&str, u64, u64)]| v.iter().map(|(n, b, t)| (n.to_string(), (*b, *t))).collect::<LeaderSig>();
        let a = sig(&[("ledger-0", 1, 2), ("ledger-1", 1, 1)]);
        assert!(!leadership_changed(&a, &a));
        assert!(leadership_changed(&a, &sig(&[("ledger-0", 1, 3), ("ledger-1", 1, 1)])));
        // A restart without a transition yet is not a change; with one it is.
        assert!(!leadership_changed(&a, &sig(&[("ledger-0", 2, 0), ("ledger-1", 1, 1)])));
        assert!(leadership_changed(&a, &sig(&[("ledger-0", 2, 1), ("ledger-1", 1, 1)])));
        assert!(leadership_changed(&a, &sig(&[("ledger-0", 1, 2), ("ledger-1", 1, 1), ("ledger-2", 1, 1)])));
    }

    #[test]
    fn barrier_floor_and_errors() {
        let (d, ft, _) = testkit::driver();
        testkit::reset();
        ft.script("bucket", "Barrier", vec![ok(json!({"commitIndex": "10"})), ok(json!({"commitIndex": "12"})), ok(json!({"commitIndex": "5"}))]);
        for _ in 0..3 {
            testkit::run_rule(&d, barrier_monotonic);
        }
        let mono = recs("the barrier commit index never goes back");
        assert_eq!(mono.iter().map(|r| r.cond).collect::<Vec<_>>(), vec![true, true, false]);
        assert_eq!(mono[2].details["floor"], 12);
        assert!(recs("a barrier's commit index is positive").iter().all(|r| r.cond));
        // The first barrier has no previous signature: never a change.
        let lc = recs("barrier succeeded after an observed leadership change");
        assert_eq!(lc.len(), 3);
        assert!(!lc[0].cond);
        assert_eq!(d.s.ext::<BarrierState>().high.load(Ordering::SeqCst), 12);

        // A leadership transition between two barriers satisfies the Sometimes.
        d.s.node_states.lock().unwrap().handle("ledger-1", 7, 1, "Leadership gained term=4");
        ft.script("bucket", "Barrier", vec![ok(json!({"commitIndex": "20"}))]);
        testkit::run_rule(&d, barrier_monotonic);
        assert!(recs("barrier succeeded after an observed leadership change").last().unwrap().cond);

        testkit::reset();
        ft.script("bucket", "Barrier", vec![err("Unavailable", "", "no leader"), FakeReply::Outcome(Outcome::unknown("timeout"))]);
        testkit::run_rule(&d, barrier_monotonic);
        testkit::run_rule(&d, barrier_monotonic);
        assert!(!testkit::fired("barrier monotonic driver got non-transient error"));
        ft.script("bucket", "Barrier", vec![err("Internal", "", "boom")]);
        testkit::run_rule(&d, barrier_monotonic);
        assert!(testkit::fired("barrier monotonic driver got non-transient error"));
    }

    #[test]
    fn transfer_leadership_targets_a_voter_and_writes_after() {
        let (d, ft, _) = testkit::driver();
        testkit::reset();
        let cs = json!({"leader": "1", "nodes": [{"id": "1", "suffrage": "Voter"}, {"id": "2", "suffrage": "Voter"}, {"id": "3", "suffrage": "Learner"}]});
        ft.script("cluster", "GetClusterState", vec![ok(cs), ok(json!({"leader": "2"}))]);
        ft.set_default("cluster", "TransferLeadership", ok(json!({"newLeader": "2"})));
        testkit::run_rule(&d, transfer_leadership);
        let t = ft.calls_of("cluster", "TransferLeadership");
        assert!(!t.is_empty());
        assert_eq!(t[0]["request"], json!({"transferee": "2"}));
        assert!(recs("new leader should be the requested target")[0].cond);
        assert!(testkit::fired("leadership transfer completed"));
        let applies = ft.calls_of("bucket", "Apply");
        assert_eq!(applies.len(), 1);
        assert_eq!(applies[0]["request"]["unsigned"]["requests"][0]["apply"]["ledger"], "r-ops-test");
        assert!(applies[0]["request"]["unsigned"]["idempotencyKey"].as_str().unwrap().starts_with("r-test-k"));
        assert!(recs("business write commits after leadership transfer")[0].cond);
        assert!(recs("cluster should have a leader after transfer")[0].cond);

        testkit::reset();
        ft.script("cluster", "GetClusterState", vec![err("Internal", "", "x")]);
        testkit::run_rule(&d, transfer_leadership);
        assert!(testkit::fired("GetClusterState should not fail"));
    }

    #[test]
    fn ops_setup_creates_the_ops_ledger() {
        let (d, ft, _) = testkit::driver();
        ops_setup(&d);
        let a = ft.calls_of("bucket", "Apply");
        assert_eq!(a[0]["request"]["unsigned"]["requests"][0], json!({"createLedger": {"name": "r-ops-test"}}));
        assert_eq!(setup_hooks()[0].name, "ops_setup");
        assert!(OPS_SETUP_BUDGET <= Duration::from_secs(20));
    }

    #[test]
    fn ops_setup_gives_up_within_its_budget() {
        let (d, ft, _) = testkit::driver();
        ft.set_default("bucket", "Apply", err("Internal", "", "x"));
        let t0 = Instant::now();
        ops_setup_within(&d, Duration::from_millis(1500));
        assert!(t0.elapsed() < Duration::from_millis(3000), "{:?}", t0.elapsed());
        assert!(!ft.calls_of("bucket", "Apply").is_empty());
    }

    #[test]
    fn transfer_leadership_creates_a_missing_ops_ledger() {
        let (d, ft, _) = testkit::driver();
        testkit::reset();
        let cs = json!({"leader": "1", "nodes": [{"id": "1", "suffrage": "Voter"}, {"id": "2", "suffrage": "Voter"}]});
        ft.set_default("cluster", "GetClusterState", ok(cs));
        ft.set_default("cluster", "TransferLeadership", ok(json!({"newLeader": "2"})));
        ft.script("bucket", "Apply", vec![err("NotFound", "LEDGER_NOT_FOUND", "no ledger")]);
        testkit::run_rule(&d, transfer_leadership);
        let a = ft.calls_of("bucket", "Apply");
        assert_eq!(a.last().unwrap()["request"]["unsigned"]["requests"][0], json!({"createLedger": {"name": "r-ops-test"}}));
        assert!(!recs("business write commits after leadership transfer")[0].cond);
    }

    #[test]
    fn rule_clock_caps_calls_and_skips_when_spent() {
        let c = Clock::new(Duration::from_secs(10));
        assert_eq!(c.cap(Duration::from_secs(3)), Some(Duration::from_secs(3)));
        assert!(c.cap(Duration::from_secs(30)).unwrap() <= Duration::from_secs(10));
        let spent = Clock::new(Duration::from_millis(100));
        assert_eq!(spent.cap(Duration::from_secs(3)), None);
        // Worst case of a rule body: each call is capped by the one deadline.
        assert!(RULE_BUDGET <= Duration::from_secs(10));
        assert!(COMPACTION_TIMEOUT + 2 * READ_TIMEOUT <= RULE_BUDGET);
    }

    #[test]
    fn slow_transfer_skips_the_burst_and_the_write() {
        let (d, ft, _) = testkit::driver();
        testkit::reset();
        let cs = json!({"leader": "1", "nodes": [{"id": "1", "suffrage": "Voter"}, {"id": "2", "suffrage": "Voter"}, {"id": "3", "suffrage": "Voter"}]});
        ft.set_default("cluster", "GetClusterState", ok(cs));
        let slow = Outcome::from_json(&json!({"ok": true, "response": {"newLeader": "2"}}));
        ft.set_default("cluster", "TransferLeadership", FakeReply::Delayed(Duration::from_millis(9700), slow));
        let t0 = Instant::now();
        testkit::run_rule(&d, transfer_leadership);
        assert!(t0.elapsed() < Duration::from_secs(11), "{:?}", t0.elapsed());
        assert_eq!(ft.calls_of("cluster", "TransferLeadership").len(), 1);
        assert!(ft.calls_of("bucket", "Apply").is_empty());
        assert!(recs("business write commits after leadership transfer").is_empty(), "not evaluated without time left");
    }

    #[test]
    fn compaction_reads_back_default() {
        let (d, ft, _) = testkit::driver();
        testkit::reset();
        ft.set_default("cluster", "CompactPrimary", ok(json!({"durationMs": "3"})));
        ft.set_default("cluster", "CompactSecondary", ok(json!({"durationMs": "3"})));
        ft.script("bucket", "GetAccount", vec![err("Internal", "", "x")]);
        testkit::run_rule(&d, compaction);
        assert!(testkit::fired("primary compaction completed") || testkit::fired("secondary compaction completed"));
        assert_eq!(ft.calls_of("bucket", "GetLedgerStats")[0]["request"], json!({"ledger": "default"}));
        assert_eq!(ft.calls_of("bucket", "GetAccount")[0]["request"], json!({"ledger": "default", "address": "world"}));
        assert!(!recs("world account must be readable after compaction")[0].cond);
    }

    /// Fills both job threads so a rule's job cannot start.
    fn hold_job_threads(d: &Driver) -> Arc<(Mutex<bool>, std::sync::Condvar)> {
        let gate = Arc::new((Mutex::new(false), std::sync::Condvar::new()));
        for _ in 0..crate::jobs::JOB_THREADS {
            let g = gate.clone();
            d.submit_job(Job {
                name: "hold",
                blocks_quiet: false,
                run: Box::new(move |_| {
                    let (m, cv) = &*g;
                    let mut open = m.lock().unwrap();
                    while !*open {
                        open = cv.wait(open).unwrap();
                    }
                }),
            });
        }
        testkit::wait_until(Duration::from_secs(5), || d.s.jobs.len() == crate::jobs::JOB_THREADS);
        std::thread::sleep(Duration::from_millis(50));
        gate
    }
    fn release(g: &Arc<(Mutex<bool>, std::sync::Condvar)>) {
        *g.0.lock().unwrap() = true;
        g.1.notify_all();
    }

    #[test]
    fn backup_and_check_store_submit_a_job_and_return_without_sending() {
        let (d, ft, _) = testkit::driver();
        ft.set_default("cluster", "Backup", ok(json!({"totalFiles": 3, "lastLogSequence": "0"})));
        ft.set_default("cluster", "IncrementalBackup", ok(json!({"lastLogSequence": "0"})));
        let gate = hold_job_threads(&d);
        let before = d.s.jobs.len();
        testkit::run_rule(&d, backup);
        assert!(ft.calls_of("cluster", "Backup").is_empty());
        assert_eq!(d.s.jobs.len(), before + 1);
        assert!(d.s.ext::<LongJob>().inflight.load(Ordering::SeqCst));
        // One long job at a time, across backup and check_store.
        testkit::run_rule(&d, backup);
        testkit::run_rule(&d, check_store);
        assert_eq!(d.s.jobs.len(), before + 1);
        release(&gate);
        testkit::wait_until(Duration::from_secs(10), || !ft.calls_of("cluster", "Backup").is_empty());
        testkit::wait_until(Duration::from_secs(10), || !d.s.ext::<LongJob>().inflight.load(Ordering::SeqCst));
        let b = &ft.calls_of("cluster", "Backup")[0];
        assert_eq!(b["request"], backup_storage());
        assert_eq!(b["timeoutMs"], 90_000);

        let gate = hold_job_threads(&d);
        testkit::run_rule(&d, check_store);
        assert!(ft.calls_of("bucket", "CheckStore").is_empty());
        release(&gate);
        testkit::wait_until(Duration::from_secs(10), || !ft.calls_of("bucket", "CheckStore").is_empty());
        let c = &ft.calls_of("bucket", "CheckStore")[0];
        assert_eq!(c["maxItems"], 100_000);
        assert_eq!(c["timeoutMs"], 60_000);
        testkit::wait_until(Duration::from_secs(10), || !d.s.ext::<LongJob>().inflight.load(Ordering::SeqCst));
    }

    #[test]
    fn long_jobs_never_queue_behind_a_quiet_blocking_job() {
        let (d, _ft, _) = testkit::driver();
        let gate = Arc::new((Mutex::new(false), std::sync::Condvar::new()));
        let g = gate.clone();
        d.submit_job(Job {
            name: "blocking",
            blocks_quiet: true,
            run: Box::new(move |_| {
                let (m, cv) = &*g;
                let mut open = m.lock().unwrap();
                while !*open {
                    open = cv.wait(open).unwrap();
                }
            }),
        });
        testkit::run_rule(&d, backup);
        testkit::run_rule(&d, check_store);
        assert!(!d.s.ext::<LongJob>().inflight.load(Ordering::SeqCst));
        assert_eq!(d.s.jobs.len(), 1);
        release(&gate);
        testkit::wait_until(Duration::from_secs(5), || d.s.blocking_jobs() == 0);
    }

    fn chain(incremental: bool) -> BackupChain {
        BackupChain { incremental, routes: vec![Route::lib(0), Route::plain(1)], busy_backoffs_ms: vec![5] }
    }
    fn plan(floor: u64) -> BackupPlan {
        BackupPlan { chains: vec![], barrier_floor: floor, call_timeout: Duration::from_secs(1), busy_window: Duration::from_millis(500), budget: Duration::from_secs(5) }
    }
    fn far() -> Instant {
        Instant::now() + Duration::from_secs(5)
    }

    #[test]
    fn backup_busy_retry_and_non_regression_bookkeeping() {
        let (d, ft, _) = testkit::driver();
        testkit::reset();
        let busy = || err("FailedPrecondition", "", "x: backup: destination already has a running job");
        ft.script("cluster", "Backup", vec![busy(), busy(), ok(json!({"totalFiles": 4, "lastLogSequence": "50", "lastAppliedIndex": "120"}))]);
        ft.script("cluster", "IncrementalBackup", vec![ok(json!({"lastLogSequence": "60", "lastAuditSequence": "9"})), ok(json!({"lastLogSequence": "60", "lastAuditSequence": "9"}))]);
        run_chain(&d, &plan(100), &chain(true), far());
        let rec = recs("backup recovery after destination busy succeeds");
        assert_eq!(rec.iter().map(|r| r.cond).collect::<Vec<_>>(), vec![false, false, true]);
        assert!(recs("backup should produce files")[0].cond);
        assert!(testkit::fired("backup completed successfully"));
        assert!(recs("a full backup's applied index covers every barrier completed before it started")[0].cond);
        assert_eq!(recs("incremental backup completed").len(), 1);
        assert_eq!(ft.calls_of("cluster", "IncrementalBackup").len(), 2, "full, then two incrementals");
        assert!(recs("incremental backup log sequence should not regress")[0].cond);
        assert!(recs("incremental backup audit sequence should not regress")[0].cond);
        assert_eq!(ft.calls_of("cluster", "Backup")[1]["route"], "plain", "routes cycle");
        let st = d.s.ext::<BackupState>();
        assert_eq!(st.fulls_inflight.load(Ordering::SeqCst), 0);
        assert_eq!(st.full_epoch.load(Ordering::SeqCst), 6, "three full calls, two bumps each");

        // A second incremental that regresses on the same chain, and a full
        // backup whose applied index is behind the barrier floor.
        testkit::reset();
        ft.script("cluster", "Backup", vec![ok(json!({"totalFiles": 4, "lastLogSequence": "70", "lastAppliedIndex": "90"}))]);
        ft.script("cluster", "IncrementalBackup", vec![ok(json!({"lastLogSequence": "75", "lastAuditSequence": "9"})), ok(json!({"lastLogSequence": "74", "lastAuditSequence": "9"}))]);
        run_chain(&d, &plan(100), &chain(true), far());
        assert!(!recs("a full backup's applied index covers every barrier completed before it started")[0].cond);
        assert!(!recs("incremental backup log sequence should not regress")[0].cond);
        assert!(recs("incremental backup audit sequence should not regress")[0].cond);

        // A later chain starting lower than an earlier chain's incrementals is
        // fine: each full backup restarts the export chain.
        testkit::reset();
        ft.script("cluster", "Backup", vec![ok(json!({"totalFiles": 4, "lastLogSequence": "30", "lastAppliedIndex": "200"}))]);
        ft.script("cluster", "IncrementalBackup", vec![ok(json!({"lastLogSequence": "31", "lastAuditSequence": "2"})), ok(json!({"lastLogSequence": "31", "lastAuditSequence": "2"}))]);
        run_chain(&d, &plan(0), &chain(true), far());
        assert!(recs("incremental backup log sequence should not regress").iter().all(|r| r.cond));
        assert!(recs("incremental backup audit sequence should not regress").iter().all(|r| r.cond));

        // Busy past the window is inconclusive; tolerated incremental errors; unexpected errors.
        testkit::reset();
        ft.set_default("cluster", "Backup", busy());
        run_chain(&d, &plan(0), &chain(false), far());
        assert!(!testkit::fired("Backup returned unexpected error"));
        assert!(!testkit::fired("backup completed successfully"));
        ft.set_default("cluster", "Backup", ok(json!({"totalFiles": 1})));
        ft.script("cluster", "IncrementalBackup", vec![err("FailedPrecondition", "", "no full checkpoint at destination"), err("FailedPrecondition", "EXTERNAL_SERVICE_ERROR", "s3"), err("Internal", "", "x")]);
        run_chain(&d, &plan(0), &chain(true), far());
        run_chain(&d, &plan(0), &chain(true), far());
        assert!(!testkit::fired("IncrementalBackup returned unexpected error"));
        run_chain(&d, &plan(0), &chain(true), far());
        assert!(testkit::fired("IncrementalBackup returned unexpected error"));
        ft.script("cluster", "IncrementalBackup", vec![ok(json!({"lastLogSequence": "1"})), err("Canceled", "", "context canceled")]);
        run_chain(&d, &plan(0), &chain(true), far());
        assert!(testkit::fired("second IncrementalBackup returned unexpected error"));
        ft.script("cluster", "Backup", vec![err("InvalidArgument", "", "bad storage")]);
        run_chain(&d, &plan(0), &chain(false), far());
        assert!(testkit::fired("Backup returned unexpected error"));
        ft.script("cluster", "Backup", vec![err("Internal", "", "boom")]);
        run_chain(&d, &plan(0), &chain(true), far());
        assert!(testkit::fired("Backup (pre-incremental) returned unexpected error"));
    }

    #[test]
    fn incrementals_with_a_full_backup_in_between_are_not_compared() {
        assert!(incrementals_comparable(Some(4), Some(4)));
        assert!(!incrementals_comparable(Some(4), Some(6)), "a full backup started and ended in between");
        assert!(!incrementals_comparable(None, Some(6)), "a full backup was in flight before the first");
        assert!(!incrementals_comparable(Some(4), None), "a full backup is in flight after the second");
        let st = Arc::new(BackupState::default());
        let m0 = st.quiet_marker();
        assert_eq!(m0, Some(0));
        let g = FullGuard::start(st.clone());
        assert_eq!(st.quiet_marker(), None);
        drop(g);
        assert!(!incrementals_comparable(m0, st.quiet_marker()));
        assert!(incrementals_comparable(st.quiet_marker(), st.quiet_marker()));
    }

    #[test]
    fn the_job_budget_bounds_busy_retries() {
        let (d, ft, _) = testkit::driver();
        testkit::reset();
        ft.set_default("cluster", "Backup", err("FailedPrecondition", "", "backup: destination already has a running job"));
        let mut p = plan(0);
        p.busy_window = Duration::from_secs(120);
        let t0 = Instant::now();
        run_chain(&d, &p, &chain(true), Instant::now() + Duration::from_millis(1500));
        assert!(t0.elapsed() < Duration::from_secs(3), "{:?}", t0.elapsed());
        assert!(!testkit::fired("Backup (pre-incremental) returned unexpected error"));
        // Each call's timeout is capped by what is left of the job budget.
        assert!(ft.calls_of("cluster", "Backup").iter().all(|c| c["timeoutMs"].as_u64().unwrap() <= 1500));
    }

    #[test]
    fn backup_calls_wait_while_a_quiet_period_may_be_open() {
        let (d, ft, _) = testkit::driver();
        testkit::reset();
        assert!(!quiet_maybe_open(&d.s), "no observation yet");
        d.s.quiet_open.store(true, Ordering::SeqCst);
        assert!(quiet_maybe_open(&d.s), "the flag alone");
        d.s.quiet_open.store(false, Ordering::SeqCst);
        note_outside_quiet(&d.s);
        assert!(!quiet_maybe_open(&d.s));
        // A quiet period starts (run_quiet bumps the epoch) and has not ended.
        d.s.next_epoch.fetch_add(1, Ordering::SeqCst);
        assert!(quiet_maybe_open(&d.s));
        ft.set_default("cluster", "Backup", ok(json!({"totalFiles": 1})));
        let out = backup_call(&d, &plan(0), &chain(false), "Backup", Instant::now() + Duration::from_millis(1200));
        assert!(out.is_none());
        assert!(ft.calls_of("cluster", "Backup").is_empty());
        // It ends: the job may proceed.
        *d.s.last_quiet_end.lock().unwrap() = Instant::now();
        assert!(!quiet_maybe_open(&d.s));
        assert!(backup_call(&d, &plan(0), &chain(false), "Backup", far()).unwrap().is_ok());
        // Two starts since the last rule: conservatively open.
        d.s.next_epoch.fetch_add(1, Ordering::SeqCst);
        assert!(quiet_maybe_open(&d.s));
        note_outside_quiet(&d.s);
        assert!(!quiet_maybe_open(&d.s));
    }

    #[test]
    fn backup_rule_records_the_barrier_floor() {
        let (d, ft, _) = testkit::driver();
        testkit::reset();
        d.s.ext::<BarrierState>().high.store(500, Ordering::SeqCst);
        ft.set_default("cluster", "Backup", ok(json!({"totalFiles": 1, "lastAppliedIndex": "400"})));
        ft.set_default("cluster", "IncrementalBackup", ok(json!({})));
        testkit::run_rule(&d, backup);
        testkit::wait_until(Duration::from_secs(10), || !d.s.ext::<LongJob>().inflight.load(Ordering::SeqCst));
        let r = recs("a full backup's applied index covers every barrier completed before it started");
        assert!(!r.is_empty());
        assert_eq!(r[0].details["barrierFloor"], 500);
        assert!(!r[0].cond);
    }

    #[test]
    fn two_concurrent_chains_run_in_one_job() {
        let (d, ft, _) = testkit::driver();
        testkit::reset();
        ft.set_default("cluster", "Backup", ok(json!({"totalFiles": 2, "lastLogSequence": "5"})));
        let mut p = plan(0);
        p.chains = vec![chain(false), chain(false)];
        run_backup_plan(&d, &p);
        assert_eq!(ft.calls_of("cluster", "Backup").len(), 2);
        assert_eq!(recs("backup completed successfully").len(), 2);
    }

    #[test]
    fn check_store_assertions() {
        let (d, ft, _) = testkit::driver();
        testkit::reset();
        let items = |v: Vec<Value>, complete: bool| FakeReply::Outcome(Outcome::from_json(&json!({"ok": true, "items": v, "complete": complete})));
        ft.script("bucket", "CheckStore", vec![
            items(vec![json!({"progress": {"logsChecked": "5", "totalLogs": "5"}})], true),
            items(vec![json!({"error": {"errorType": "CHECK_STORE_ERROR_TYPE_HASH_MISMATCH"}})], false),
            err("Unavailable", "", ""),
            err("Internal", "", ""),
        ]);
        for _ in 0..4 {
            run_check_store(&d, &Route::lib(0), Duration::from_secs(1));
        }
        let aou = recs("CheckStore found integrity error");
        assert_eq!(aou.iter().map(|r| r.cond).collect::<Vec<_>>(), vec![true, false]);
        let done = recs("CheckStore completed successfully");
        assert_eq!(done.len(), 1);
        assert_eq!(done[0].details["logsChecked"], 5);
        assert_eq!(recs("CheckStore returned unexpected error").len(), 1);
    }

    #[test]
    fn event_sinks_exact_name_match() {
        let (d, ft, _) = testkit::driver();
        testkit::reset();
        // A prefix of the name (`sink-test-1`) and a longer name never count.
        ft.script("bucket", "GetEventsSinks", vec![
            ok(json!({"sinks": [{"name": "sink-test-00"}, {"name": "sink-test-0"}]})),
            ok(json!({"sinks": [{"name": "sink-test-00"}]})),
            ok(json!({"sinks": [{"name": "sink-test-10"}, {"name": "sink-test-"}]})),
        ]);
        testkit::run_rule(&d, event_sinks);
        let applies = ft.calls_of("bucket", "Apply");
        let add = &applies[0]["request"]["unsigned"]["requests"][0]["addEventsSink"]["config"];
        assert_eq!(add["name"], "sink-test-0");
        assert!(add["nats"]["topic"].as_str().unwrap().starts_with("ledger.events."));
        assert_eq!(applies[1]["request"]["unsigned"]["requests"][0], json!({"removeEventsSink": {"name": "sink-test-0"}}));
        assert!(recs("added sink should appear in GetEventsSinks")[0].cond);
        assert!(recs("removed sink should not appear in GetEventsSinks")[0].cond);
        assert!(recs("should be able to add NATS event sink")[0].cond);
        assert!(recs("should be able to remove event sink")[0].cond);

        testkit::reset();
        ft.script("bucket", "GetEventsSinks", vec![ok(json!({"sinks": [{"name": "sink-test-10"}]}))]);
        ft.script("bucket", "Apply", vec![ok(json!({"logs": []}))]);
        ft.set_default("bucket", "Apply", err("Internal", "", ""));
        testkit::run_rule(&d, event_sinks);
        assert!(!recs("added sink should appear in GetEventsSinks")[0].cond, "sink-test-1 is not sink-test-10");
        // The remove got no definitive outcome: the next run cleans it up first.
        assert_eq!(*d.s.ext::<SinkState>().leftovers.lock().unwrap(), vec!["sink-test-1".to_string()]);
    }

    #[test]
    fn node_views() {
        let f = json!({"localNode": "2", "leader": "1", "syncProgress": {"status": "snapshotting"}});
        assert_eq!(node_view(&f, 2), Some(NodeView::Follower { out_of_sync: true }));
        assert_eq!(node_view(&f, 3), None);
        let n = json!({"localNode": "2", "leader": "1", "syncProgress": {"status": "normal"}});
        assert_eq!(node_view(&n, 2), Some(NodeView::Follower { out_of_sync: false }));
        let l = json!({"localNode": "1", "leader": "1", "raftStatus": {"progress": {"2": {"state": "Replicate"}, "3": {"state": "Snapshot"}}}});
        assert_eq!(node_view(&l, 1), Some(NodeView::Leader { snapshot_progress: true }));
        let l2 = json!({"localNode": "1", "leader": "1", "raftStatus": {"progress": {"2": {"state": "Probe"}}}});
        assert_eq!(node_view(&l2, 1), Some(NodeView::Leader { snapshot_progress: false }));
    }

    #[test]
    fn cluster_state_asks_each_node_about_itself() {
        let (d, ft, _) = testkit::driver();
        testkit::reset();
        let members = json!({"nodes": [{"id": "1", "serviceAddress": "ledger-0:8888"}, {"id": "2", "serviceAddress": "ledger-1:8888"}]});
        ft.script("cluster", "GetClusterState", vec![
            ok(members),
            ok(json!({"localNode": "1", "leader": "1", "raftStatus": {"progress": {"2": {"state": "Snapshot"}}}})),
            ok(json!({"localNode": "2", "leader": "1", "syncProgress": {"status": "out_of_sync"}})),
        ]);
        testkit::run_rule(&d, cluster_state);
        let calls = ft.calls_of("cluster", "GetClusterState");
        assert_eq!(calls[1]["request"], json!({"nodeId": "1"}));
        assert_eq!(calls[1]["node"], "ledger-0:8888");
        assert_eq!(calls[2]["node"], "ledger-1:8888");
        assert!(recs("the leader reports a follower in snapshot progress")[0].cond);
        assert!(recs("a follower reports an out-of-sync or snapshotting status")[0].cond);
        // Two members of three configured nodes: not cached, so the next run rediscovers.
        assert!(d.s.ext::<NodeCache>().map.lock().unwrap().is_none());
    }

    #[test]
    fn node_maps_with_missing_addresses_are_not_cached() {
        let full = NodeMap::from_cluster_state(&json!({"nodes": [{"id": "1", "serviceAddress": "a"}, {"id": "2", "serviceAddress": "b"}, {"id": "3", "serviceAddress": "c"}]}));
        assert!(node_map_complete(&full, 3));
        let hole = NodeMap::from_cluster_state(&json!({"nodes": [{"id": "1", "serviceAddress": "a"}, {"id": "2", "serviceAddress": ""}, {"id": "3", "serviceAddress": "c"}]}));
        assert!(!node_map_complete(&hole, 3));
        assert!(!node_map_complete(&full, 4));
        assert!(!node_map_complete(&NodeMap::default(), 0));

        let (d, ft, _) = testkit::driver();
        testkit::reset();
        ft.script("cluster", "GetClusterState", vec![ok(json!({"nodes": [{"id": "1", "serviceAddress": "ledger-0:8888"}, {"id": "2", "serviceAddress": ""}, {"id": "3", "serviceAddress": "ledger-2:8888"}]}))]);
        testkit::run_rule(&d, cluster_state);
        let calls = ft.calls_of("cluster", "GetClusterState");
        assert_eq!(calls.len(), 3, "discovery + the two nodes with an address");
        assert!(d.s.ext::<NodeCache>().map.lock().unwrap().is_none());
    }

    #[test]
    fn rule_names_and_weights() {
        let r: Vec<(String, f64)> = rules().into_iter().map(|r| (r.name.clone(), r.weight)).collect();
        assert_eq!(
            r,
            vec![
                ("barrier_monotonic".into(), 1.0),
                ("transfer_leadership".into(), 0.4),
                ("compaction".into(), 0.3),
                ("cluster_checkpoint".into(), 0.2),
                ("backup".into(), 0.3),
                ("check_store".into(), 0.3),
                ("event_sinks".into(), 0.3),
                ("cluster_state".into(), 0.3),
            ]
        );
    }
}
