//! Task 12: cluster operations (Rust-checked; no oracle involvement, these
//! RPCs never change tracked business state). Barrier monotonicity,
//! leadership transfer, compaction, Pebble checkpoints, full and incremental
//! backups (jobs), CheckStore (job), event sinks, per-node cluster state.
//!
//! Every draw happens before the first send. Long operations (backups, the
//! store check) are drawn here and run on the job threads (DESIGN §4.13).
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

// ---------------------------------------------------------------- classification

/// The answer says nothing about the server: no answer at all, or an error
/// the client produced itself (dial failure, local deadline).
fn no_answer(o: &Outcome) -> bool {
    o.is_unknown() || (!o.ok && o.local)
}

/// A transient (retry-safe, inconclusive) error: no answer, Unavailable,
/// DeadlineExceeded, Canceled, plus the old workload's `IsTransient` reasons
/// (`EXTERNAL_SERVICE_ERROR`, ResourceExhausted + `WRITES_BLOCKED_DISK_FULL`).
pub fn transient(o: &Outcome) -> bool {
    if o.is_ok() {
        return false;
    }
    no_answer(o)
        || matches!(o.code.as_str(), "Unavailable" | "DeadlineExceeded" | "Canceled")
        || o.reason == "EXTERNAL_SERVICE_ERROR"
        || (o.code == "ResourceExhausted" && o.reason == "WRITES_BLOCKED_DISK_FULL")
}

/// The old `IsClassified`: ok, transient, or a known business code.
pub fn classified(o: &Outcome) -> bool {
    o.is_ok() || transient(o) || matches!(o.code.as_str(), "NotFound" | "AlreadyExists" | "InvalidArgument" | "FailedPrecondition")
}

pub const BUSY_SUFFIX: &str = "backup: destination already has a running job";

/// `IsBackupInProgress`: FailedPrecondition whose message ends with the
/// destination-exclusion sentinel (exact suffix; the server also uses
/// FailedPrecondition for job-id collisions and missing checkpoints).
pub fn backup_busy(o: &Outcome) -> bool {
    !o.is_ok() && !o.is_unknown() && o.code == "FailedPrecondition" && o.message.ends_with(BUSY_SUFFIX)
}

/// `IsNoFullCheckpoint`.
pub fn no_full_checkpoint(o: &Outcome) -> bool {
    !o.is_ok() && !o.is_unknown() && o.code == "FailedPrecondition" && o.message.contains("full checkpoint")
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

/// The Rust-checked ledger of this family (created by `ops_setup`).
pub fn ops_ledger(s: &Shared) -> String {
    format!("r-ops-{}", s.run)
}

fn ops_setup(d: &Driver) {
    let name = ops_ledger(&d.s);
    let key = d.s.next_untracked_key();
    for _ in 0..30 {
        let mut send = UntrackedSend::new(vec![json!({"createLedger": {"name": name}})], Some(key.clone()));
        send.schedule = (0..d.s.clients.len().max(1)).map(Route::lib).collect();
        let r = d.send_untracked(send);
        if r.outcome.is_ok() || r.outcome.reason == "LEDGER_ALREADY_EXISTS" {
            println!("setup: ledger {name} ready");
            return;
        }
        println!("setup: creating {name}: {}", r.outcome.summary());
        std::thread::sleep(Duration::from_secs(1));
    }
    println!("setup: giving up on {name}; transfer_leadership writes will fail");
}

/// One TransferLeadership with its assertions. True on success.
fn transfer_once(d: &Driver, route: &Route, from: u64, target: u64, burst: bool) -> bool {
    let (_, out) = d.call(route, "cluster", "TransferLeadership", json!({"transferee": target.to_string()}), &CallOpts::default().timeout(TRANSFER_TIMEOUT));
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

        let (_, out) = d.call(&route, "cluster", "GetClusterState", json!({}), &CallOpts::default().timeout(Duration::from_secs(3)));
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
        if !transfer_once(d, &route, leader, t1, false) {
            return Some(());
        }
        if burst {
            // Right after: a second transfer to another voter (bloom rebuild
            // racing a populate, server-signals §1.4; cache epoch §1.5).
            let c2 = voters_excluding(&cs, &[t1]);
            if !c2.is_empty() {
                let t2 = c2[pick2 as usize % c2.len()];
                let _ = transfer_once(d, &route, t1, t2, true);
            }
        }
        // A business write right after the transfer (untracked, keyed).
        let ledger = ops_ledger(&d.s);
        let req = create_tx_request(&ledger, &[Posting::new("world", "ops:transfer", 1u64, "COIN")], None);
        let mut send = UntrackedSend::new(vec![req], Some(d.s.next_untracked_key()));
        send.schedule = schedule;
        send.backoffs_ms = backoffs;
        send.budget = Duration::from_secs(5);
        let w = d.send_untracked(send);
        check_sometimes!(w.outcome.is_ok(), "business write commits after leadership transfer", json!({"ledger": ledger, "outcome": w.outcome.summary()}));
        if !w.outcome.is_ok() {
            return Some(());
        }
        let (_, after) = d.call(&route, "cluster", "GetClusterState", json!({}), &CallOpts::default().timeout(Duration::from_secs(3)));
        if after.is_ok() {
            let l = u64_field(&resp(&after), "leader");
            check_sometimes!(l != 0, "cluster should have a leader after transfer", json!({"leader": l, "targetNode": t1}));
        }
        Some(())
    });
}

// ---------------------------------------------------------------- compaction

pub const COMPACTION_TIMEOUT: Duration = Duration::from_secs(8);
const READ_TIMEOUT: Duration = Duration::from_secs(2);

fn compaction(d: &Driver, tc: TestCase) {
    d.guarded("compaction", &tc, |d, tc| {
        let primary = draw::chance(tc, 0.5);
        let route = draw::route(tc, &d.s);
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
        verify_readable(d, &route, primary);
        Some(())
    });
}

/// GetLedgerStats and GetAccount(`world`) on `default` after a compaction.
fn verify_readable(d: &Driver, route: &Route, primary: bool) {
    let opts = CallOpts::linearizable().timeout(READ_TIMEOUT);
    let (_, stats) = d.call(route, "bucket", "GetLedgerStats", json!({"ledger": "default"}), &opts);
    if !stats.is_ok() {
        if !transient(&stats) {
            check_unreachable!("GetLedgerStats should not fail after compaction", json!({"primary": primary, "outcome": stats.summary()}));
        }
        return;
    }
    let (_, acc) = d.call(route, "bucket", "GetAccount", json!({"ledger": "default", "address": "world"}), &opts);
    check_always_or_unreachable!(classified(&acc), "world account must be readable after compaction", json!({"primary": primary, "outcome": acc.summary()}));
}

// ---------------------------------------------------------------- cluster_checkpoint

fn cluster_checkpoint(d: &Driver, tc: TestCase) {
    d.guarded("cluster_checkpoint", &tc, |d, tc| {
        let route = draw::route(tc, &d.s);
        let (_, out) = d.call(&route, "cluster", "CreateCheckpoint", json!({}), &CallOpts::default().timeout(Duration::from_secs(8)));
        check_sometimes!(out.is_ok(), "a checkpoint is created", json!({"route": route.via(), "outcome": out.summary(), "checkpointId": out.response.as_ref().and_then(|r| r.get("checkpointId")).cloned()}));
        Some(())
    });
}

// ---------------------------------------------------------------- backup

pub const BACKUP_TIMEOUT: Duration = Duration::from_secs(90);
pub const BUSY_WINDOW: Duration = Duration::from_secs(120);

/// One backup sequence: a full backup, then optionally an incremental.
#[derive(Clone, Debug)]
pub struct BackupChain {
    pub incremental: bool,
    pub routes: Vec<Route>,
    pub busy_backoffs_ms: Vec<u64>,
}

#[derive(Clone, Debug)]
pub struct BackupPlan {
    pub chains: Vec<BackupChain>,
    /// `Shared.acked_seq_high_water` when the rule ran (before the job).
    pub acked_floor: u64,
    pub call_timeout: Duration,
    pub busy_window: Duration,
}

#[derive(Default)]
pub struct BackupState {
    /// A backup job is queued or running (at most one, so backups never
    /// monopolise the shared job threads).
    pub inflight: AtomicBool,
    /// Highest `lastLogSequence` / `lastAuditSequence` of a successful
    /// incremental that completed (driver-global).
    pub incr_log: AtomicU64,
    pub incr_audit: AtomicU64,
}

/// Clears an in-flight flag when dropped (also on a job panic).
struct ClearOnDrop(Arc<dyn Fn() + Send + Sync>);
impl Drop for ClearOnDrop {
    fn drop(&mut self) {
        (self.0)()
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
        let st = d.s.ext::<BackupState>();
        if st.inflight.swap(true, Ordering::SeqCst) {
            return Some(());
        }
        let plan = BackupPlan { chains, acked_floor: d.s.acked_seq_high_water.load(Ordering::SeqCst), call_timeout: BACKUP_TIMEOUT, busy_window: BUSY_WINDOW };
        let st2 = st.clone();
        let guard = ClearOnDrop(Arc::new(move || st2.inflight.store(false, Ordering::SeqCst)));
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
    if plan.chains.len() == 1 {
        run_chain(d, plan, &plan.chains[0]);
        return;
    }
    std::thread::scope(|s| {
        let hs: Vec<_> = plan.chains.iter().map(|c| crate::assertions::scoped_inheriting(s, move || Driver::helper_iteration("backup chain", || run_chain(d, plan, c)))).collect();
        for h in hs {
            let _ = h.join();
        }
    });
}

/// One Backup/IncrementalBackup with the destination-busy retry (≤
/// `busy_window` from the first busy answer). Returns the last outcome.
pub fn backup_call(d: &Driver, plan: &BackupPlan, chain: &BackupChain, method: &str) -> Outcome {
    let mut first_busy: Option<Instant> = None;
    let mut k = 0usize;
    loop {
        let route = &chain.routes[k % chain.routes.len().max(1)];
        let (_, out) = d.call(route, "cluster", method, backup_storage(), &CallOpts::default().timeout(plan.call_timeout));
        let busy = backup_busy(&out);
        if busy && first_busy.is_none() {
            first_busy = Some(Instant::now());
        }
        if first_busy.is_some() {
            check_sometimes!(out.is_ok(), "backup recovery after destination busy succeeds", json!({"operation": method, "attempts": k + 1, "outcome": out.summary()}));
        }
        let Some(t0) = first_busy.filter(|_| busy) else { return out };
        let deadline = t0 + plan.busy_window;
        let now = Instant::now();
        if now >= deadline {
            println!("driver: {method} destination still busy after {} attempts; inconclusive", k + 1);
            return out;
        }
        let wait = Duration::from_millis(chain.busy_backoffs_ms[k % chain.busy_backoffs_ms.len().max(1)]).min(deadline - now);
        std::thread::sleep(wait);
        k += 1;
    }
}

pub fn run_chain(d: &Driver, plan: &BackupPlan, chain: &BackupChain) {
    let full = backup_call(d, plan, chain, "Backup");
    if !full.is_ok() {
        if !(backup_busy(&full) || transient(&full)) {
            check_unreachable!("Backup returned unexpected error", json!({"outcome": full.summary()}));
        } else {
            println!("driver: Backup inconclusive: {}", full.summary());
        }
        return;
    }
    let r = resp(&full);
    let total = u64_field(&r, "totalFiles");
    let last_log = u64_field(&r, "lastLogSequence");
    let details = json!({"filesUploaded": u64_field(&r, "filesUploaded"), "totalFiles": total, "durationMs": r.get("durationMs"), "lastLogSequence": last_log, "ackedFloor": plan.acked_floor});
    check_always_or_unreachable!(total > 0, "backup should produce files", details);
    check_reachable!("backup completed successfully", details);
    check_always!(last_log >= plan.acked_floor, "a full backup's last log sequence covers every write acknowledged before it started", details);
    if !chain.incremental {
        return;
    }
    let st = d.s.ext::<BackupState>();
    // Floors read before the call: incrementals that completed earlier.
    let log_floor = st.incr_log.load(Ordering::SeqCst);
    let audit_floor = st.incr_audit.load(Ordering::SeqCst);
    let inc = backup_call(d, plan, chain, "IncrementalBackup");
    if !inc.is_ok() {
        if !(backup_busy(&inc) || transient(&inc) || no_full_checkpoint(&inc)) {
            check_unreachable!("IncrementalBackup returned unexpected error", json!({"outcome": inc.summary()}));
        } else {
            println!("driver: IncrementalBackup inconclusive: {}", inc.summary());
        }
        return;
    }
    let r = resp(&inc);
    let (log, audit) = (u64_field(&r, "lastLogSequence"), u64_field(&r, "lastAuditSequence"));
    let details = json!({"logEntriesExported": u64_field(&r, "logEntriesExported"), "auditEntriesExported": u64_field(&r, "auditEntriesExported"), "segmentsUploaded": u64_field(&r, "segmentsUploaded"), "lastLogSequence": log, "lastAuditSequence": audit, "previousLastLogSeq": log_floor, "previousLastAuditSeq": audit_floor});
    check_reachable!("incremental backup completed", details);
    check_always_or_unreachable!(log >= log_floor, "incremental backup log sequence should not regress", details);
    check_always_or_unreachable!(audit >= audit_floor, "incremental backup audit sequence should not regress", details);
    st.incr_log.fetch_max(log, Ordering::SeqCst);
    st.incr_audit.fetch_max(audit, Ordering::SeqCst);
}

// ---------------------------------------------------------------- check_store

pub const CHECK_STORE_TIMEOUT: Duration = Duration::from_secs(60);
pub const CHECK_STORE_MAX_ITEMS: u64 = 100_000;

#[derive(Default)]
pub struct CheckStoreState {
    pub inflight: AtomicBool,
}

fn check_store(d: &Driver, tc: TestCase) {
    d.guarded("check_store", &tc, |d, tc| {
        let route = draw::route(tc, &d.s);
        let st = d.s.ext::<CheckStoreState>();
        if st.inflight.swap(true, Ordering::SeqCst) {
            return Some(());
        }
        let st2 = st.clone();
        let guard = ClearOnDrop(Arc::new(move || st2.inflight.store(false, Ordering::SeqCst)));
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
const SINK_BUDGET: Duration = Duration::from_secs(4);
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

fn sink_send(d: &Driver, req: Value, schedule: &[Route], backoffs: &[u64]) -> Outcome {
    let mut send = UntrackedSend::new(vec![req], Some(d.s.next_untracked_key()));
    send.schedule = schedule.to_vec();
    send.backoffs_ms = backoffs.to_vec();
    send.attempt_timeout = Duration::from_secs(2);
    send.budget = SINK_BUDGET;
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

        let st = d.s.ext::<SinkState>();
        // Clean up one sink an earlier run may have left behind (no assertion).
        let old = st.leftovers.lock().unwrap_or_else(|e| e.into_inner()).pop();
        if let Some(old) = old {
            let out = sink_send(d, json!({"removeEventsSink": {"name": old}}), &schedule, &backoffs);
            if !definitive(&out) {
                leftover(&st, old);
            }
        }

        let name = format!("sink-{}-{}", d.s.run, st.next.fetch_add(1, Ordering::SeqCst));
        let details = json!({"sinkName": name, "topic": topic});
        let cfg = json!({"addEventsSink": {"config": {"name": name, "nats": {"url": "nats://nats:4222", "topic": topic}, "format": "json", "batchSize": 32}}});
        let add = sink_send(d, cfg, &schedule, &backoffs);
        check_sometimes!(add.is_ok() || transient(&add), "should be able to add NATS event sink", json!({"sinkName": name, "outcome": add.summary()}));
        if !add.is_ok() {
            if !definitive(&add) {
                leftover(&st, name);
            }
            return Some(());
        }
        let opts = CallOpts::linearizable().timeout(SINK_BUDGET);
        let (_, list) = d.call(&read_route, "bucket", "GetEventsSinks", json!({}), &opts);
        if list.is_ok() {
            let found = sink_names(&resp(&list)).iter().any(|n| *n == name);
            check_always_or_unreachable!(found, "added sink should appear in GetEventsSinks", details);
        }
        let rm = sink_send(d, json!({"removeEventsSink": {"name": name}}), &schedule, &backoffs);
        check_sometimes!(rm.is_ok() || transient(&rm), "should be able to remove event sink", json!({"sinkName": name, "outcome": rm.summary()}));
        if !rm.is_ok() {
            if !definitive(&rm) {
                leftover(&st, name);
            }
            return Some(());
        }
        let (_, list) = d.call(&read_route, "bucket", "GetEventsSinks", json!({}), &opts);
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
        let cache = d.s.ext::<NodeCache>();
        let known = cache.map.lock().unwrap_or_else(|e| e.into_inner()).clone();
        let map = match known {
            Some(m) => m,
            None => {
                let m = NodeMap::discover(d)?;
                *cache.map.lock().unwrap_or_else(|e| e.into_inner()) = Some(m.clone());
                m
            }
        };
        for n in &map.nodes {
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
        assert!(transient(&Outcome::from_json(&json!({"ok": false, "code": "Internal", "local": true}))));
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
        testkit::run_rule(&d, check_store);
        assert!(ft.calls_of("cluster", "Backup").is_empty());
        assert!(ft.calls_of("bucket", "CheckStore").is_empty());
        assert_eq!(d.s.jobs.len(), before + 2);
        assert!(d.s.ext::<BackupState>().inflight.load(Ordering::SeqCst));
        // A second backup while one is queued adds no job.
        testkit::run_rule(&d, backup);
        assert_eq!(d.s.jobs.len(), before + 2);
        release(&gate);
        testkit::wait_until(Duration::from_secs(10), || !ft.calls_of("cluster", "Backup").is_empty() && !ft.calls_of("bucket", "CheckStore").is_empty());
        testkit::wait_until(Duration::from_secs(10), || !d.s.ext::<BackupState>().inflight.load(Ordering::SeqCst) && !d.s.ext::<CheckStoreState>().inflight.load(Ordering::SeqCst));
        let b = &ft.calls_of("cluster", "Backup")[0];
        assert_eq!(b["request"], backup_storage());
        assert_eq!(b["timeoutMs"], 90_000);
        let c = &ft.calls_of("bucket", "CheckStore")[0];
        assert_eq!(c["maxItems"], 100_000);
        assert_eq!(c["timeoutMs"], 60_000);
    }

    fn chain(incremental: bool) -> BackupChain {
        BackupChain { incremental, routes: vec![Route::lib(0), Route::plain(1)], busy_backoffs_ms: vec![5] }
    }
    fn plan(floor: u64) -> BackupPlan {
        BackupPlan { chains: vec![], acked_floor: floor, call_timeout: Duration::from_secs(1), busy_window: Duration::from_millis(500) }
    }

    #[test]
    fn backup_busy_retry_and_non_regression_bookkeeping() {
        let (d, ft, _) = testkit::driver();
        testkit::reset();
        let busy = || err("FailedPrecondition", "", "x: backup: destination already has a running job");
        ft.script("cluster", "Backup", vec![busy(), busy(), ok(json!({"totalFiles": 4, "lastLogSequence": "50"}))]);
        ft.script("cluster", "IncrementalBackup", vec![ok(json!({"lastLogSequence": "60", "lastAuditSequence": "9"}))]);
        run_chain(&d, &plan(40), &chain(true));
        let rec = recs("backup recovery after destination busy succeeds");
        assert_eq!(rec.iter().map(|r| r.cond).collect::<Vec<_>>(), vec![false, false, true]);
        assert!(recs("backup should produce files")[0].cond);
        assert!(testkit::fired("backup completed successfully"));
        assert!(recs("a full backup's last log sequence covers every write acknowledged before it started")[0].cond);
        assert!(testkit::fired("incremental backup completed"));
        assert!(recs("incremental backup log sequence should not regress")[0].cond);
        let st = d.s.ext::<BackupState>();
        assert_eq!((st.incr_log.load(Ordering::SeqCst), st.incr_audit.load(Ordering::SeqCst)), (60, 9));
        assert_eq!(ft.calls_of("cluster", "Backup")[1]["route"], "plain", "routes cycle");

        // A later incremental that regresses, and a full backup behind the acked floor.
        testkit::reset();
        ft.script("cluster", "Backup", vec![ok(json!({"totalFiles": 4, "lastLogSequence": "70"}))]);
        ft.script("cluster", "IncrementalBackup", vec![ok(json!({"lastLogSequence": "55", "lastAuditSequence": "9"}))]);
        run_chain(&d, &plan(100), &chain(true));
        assert!(!recs("a full backup's last log sequence covers every write acknowledged before it started")[0].cond);
        assert!(!recs("incremental backup log sequence should not regress")[0].cond);
        assert!(recs("incremental backup audit sequence should not regress")[0].cond);
        assert_eq!(st.incr_log.load(Ordering::SeqCst), 60);

        // Busy past the window is inconclusive; tolerated incremental errors; unexpected errors.
        testkit::reset();
        ft.set_default("cluster", "Backup", busy());
        run_chain(&d, &plan(0), &chain(false));
        assert!(!testkit::fired("Backup returned unexpected error"));
        assert!(!testkit::fired("backup completed successfully"));
        ft.set_default("cluster", "Backup", ok(json!({"totalFiles": 1})));
        ft.script("cluster", "IncrementalBackup", vec![err("FailedPrecondition", "", "no full checkpoint at destination"), err("FailedPrecondition", "EXTERNAL_SERVICE_ERROR", "s3"), err("Internal", "", "x")]);
        run_chain(&d, &plan(0), &chain(true));
        run_chain(&d, &plan(0), &chain(true));
        assert!(!testkit::fired("IncrementalBackup returned unexpected error"));
        run_chain(&d, &plan(0), &chain(true));
        assert!(testkit::fired("IncrementalBackup returned unexpected error"));
        ft.script("cluster", "Backup", vec![err("InvalidArgument", "", "bad storage")]);
        run_chain(&d, &plan(0), &chain(false));
        assert!(testkit::fired("Backup returned unexpected error"));
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
