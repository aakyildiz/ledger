//! `Shared`: everything the driver keeps across rules, invariants, helper
//! threads and hegel re-entries (DESIGN §6.1, §6.4). Families extend it
//! through the `Extensions` type map (`ext::<T>()`), never by editing it.
#![allow(dead_code)] // family-facing API

use crate::clients::{ClientTransport, Clients, SignSpec};
use crate::jobs::Jobs;
use crate::logs::NodeStates;
use crate::oracle::{OracleClient, OracleTransport};
use crate::resolver::Resolvers;
use crate::write::WriteSlots;
use serde_json::Value;
use std::any::{Any, TypeId};
use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// Maintenance-window state (task 04 sets it; quiet Q2 waits for `Closed`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MaintState {
    Closed,
    Open { since: Instant },
}

/// Who owns a live oracle ticket in the driver (DESIGN §4.10 ticket audit).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Owner {
    Rule,
    Resolver,
    Job,
    Read,
}

thread_local! {
    static JOB_THREAD: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// Marks the current thread as a job thread (tickets it begins are owned by a job).
pub fn mark_job_thread() {
    JOB_THREAD.with(|j| j.set(true));
}

pub fn current_write_owner() -> Owner {
    if JOB_THREAD.with(|j| j.get()) {
        Owner::Job
    } else {
        Owner::Rule
    }
}

/// Every live ticket the driver owns.
#[derive(Default)]
pub struct TicketRegistry {
    map: Mutex<BTreeMap<u64, Owner>>,
    /// `/write/begin` and `/read/begin` calls in flight (their tickets may
    /// already exist in the oracle without a registry entry yet).
    pending_begins: AtomicU64,
}

impl TicketRegistry {
    pub fn insert(&self, ticket: u64, owner: Owner) {
        self.map.lock().unwrap_or_else(|e| e.into_inner()).insert(ticket, owner);
    }
    pub fn set_owner(&self, ticket: u64, owner: Owner) {
        self.insert(ticket, owner);
    }
    pub fn remove(&self, ticket: u64) {
        self.map.lock().unwrap_or_else(|e| e.into_inner()).remove(&ticket);
    }
    pub fn snapshot(&self) -> BTreeSet<u64> {
        self.map.lock().unwrap_or_else(|e| e.into_inner()).keys().copied().collect()
    }
    pub fn owners(&self) -> BTreeMap<u64, Owner> {
        self.map.lock().unwrap_or_else(|e| e.into_inner()).clone()
    }
    pub fn contains(&self, ticket: u64) -> bool {
        self.map.lock().unwrap_or_else(|e| e.into_inner()).contains_key(&ticket)
    }
    pub fn len(&self) -> usize {
        self.map.lock().unwrap_or_else(|e| e.into_inner()).len()
    }
    pub fn begin_pending(&self) -> PendingBegin<'_> {
        self.pending_begins.fetch_add(1, Ordering::SeqCst);
        PendingBegin(self)
    }
    pub fn pending_begins(&self) -> u64 {
        self.pending_begins.load(Ordering::SeqCst)
    }
}

/// Counts a begin call in flight until dropped.
pub struct PendingBegin<'a>(&'a TicketRegistry);
impl Drop for PendingBegin<'_> {
    fn drop(&mut self) {
        self.0.pending_begins.fetch_sub(1, Ordering::SeqCst);
    }
}

/// A type map for family state (`Shared::ext::<T>()`).
#[derive(Default)]
pub struct Extensions {
    map: Mutex<HashMap<TypeId, Arc<dyn Any + Send + Sync>>>,
}

impl Extensions {
    pub fn get<T: Default + Send + Sync + 'static>(&self) -> Arc<T> {
        let mut m = self.map.lock().unwrap_or_else(|e| e.into_inner());
        let e = m.entry(TypeId::of::<T>()).or_insert_with(|| Arc::new(T::default()) as Arc<dyn Any + Send + Sync>).clone();
        drop(m);
        e.downcast::<T>().expect("extension type")
    }
}

/// The newest `track_key` originals, for byte-for-byte replays (task 03).
#[derive(Clone, Debug)]
pub struct FlaggedPlan {
    pub wid: String,
    pub key: String,
    pub requests: Vec<Value>,
    pub sign: Option<SignSpec>,
}

pub const FLAGGED_PLANS_CAP: usize = 2048;
pub const TRACK_KEY_BUDGET: u64 = 4096;

/// Deadlines and pacing (DESIGN §4.8–§4.10). Unit tests shorten them.
#[derive(Clone, Debug)]
pub struct Timing {
    pub q0_pause_secs: u64,
    pub q1: Duration,
    pub q2: Duration,
    pub q3: Duration,
    pub q4_extra: Duration,
    pub pause_margin_secs: u64,
    pub poll: Duration,
    pub handoff_backpressure: Duration,
    pub backlog_pause: Duration,
    pub resolver_backoff_first: Duration,
    pub resolver_backoff_cap: Duration,
    pub slow_rule: Duration,
    pub oracle_retry: crate::oracle::OracleRetry,
}

impl Default for Timing {
    fn default() -> Self {
        Timing {
            q0_pause_secs: 100,
            q1: Duration::from_secs(45),
            q2: Duration::from_secs(60),
            q3: Duration::from_secs(90),
            q4_extra: Duration::from_secs(10),
            pause_margin_secs: 15,
            poll: Duration::from_millis(500),
            handoff_backpressure: Duration::from_secs(60),
            backlog_pause: Duration::from_secs(5),
            resolver_backoff_first: Duration::from_millis(250),
            resolver_backoff_cap: Duration::from_secs(5),
            slow_rule: Duration::from_secs(15),
            oracle_retry: crate::oracle::OracleRetry::default(),
        }
    }
}

/// Process configuration (environment, DESIGN §2.3).
#[derive(Clone, Debug)]
pub struct Config {
    pub run: String,
    pub client_urls: Vec<String>,
    pub nodes: Vec<String>,
    pub oracle_url: String,
    pub query_checkpoint_limit: String,
    pub quiet_probability: Option<f64>,
    pub exclusive_probability: Option<f64>,
    pub workload_active_file: String,
    pub timing: Timing,
}

impl Config {
    pub fn from_env() -> Config {
        let list = |k: &str| -> Vec<String> { std::env::var(k).unwrap_or_default().split(',').map(str::trim).filter(|s| !s.is_empty()).map(str::to_string).collect() };
        let prob = |k: &str| std::env::var(k).ok().and_then(|p| p.trim().parse::<f64>().ok()).map(|p| p.clamp(0.0, 1.0));
        let run = format!("{:x}", std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_micros()).unwrap_or(0));
        Config {
            run,
            client_urls: list("CLIENT_URLS"),
            nodes: list("LEDGER_NODES"),
            oracle_url: std::env::var("ORACLE_URL").unwrap_or_else(|_| "http://oracle:8090".to_string()),
            query_checkpoint_limit: std::env::var("QUERY_CHECKPOINT_LIMIT").unwrap_or_else(|_| "10".to_string()),
            quiet_probability: prob("QUIET_PROBABILITY"),
            exclusive_probability: prob("EXCLUSIVE_PROBABILITY"),
            workload_active_file: std::env::var("WORKLOAD_ACTIVE_FILE").unwrap_or_else(|_| "/workload-active".to_string()),
            timing: Timing::default(),
        }
    }
}

pub struct Shared {
    pub cfg: Config,
    /// `<run>`: lowercase hex of wall-clock micros at setup (identity only).
    pub run: String,
    pub clients: Arc<Clients>,
    pub oracle: OracleClient,
    pub slots: WriteSlots,
    pub resolvers: Resolvers,
    pub jobs: Jobs,
    pub registry: TicketRegistry,
    /// Ledger node service addresses (`LEDGER_NODES`).
    pub nodes: Vec<String>,
    pub node_states: Arc<Mutex<NodeStates>>,

    // counters (identities, never draws)
    pub next_key: AtomicU64,
    pub next_wid: AtomicU64,
    pub next_rid: AtomicU64,
    pub next_ref: AtomicU64,
    pub next_ledger: AtomicU64,
    pub next_own: AtomicU64,
    pub next_epoch: AtomicU64,
    pub next_untracked_key: AtomicU64,

    // flags
    pub active: Arc<AtomicBool>,
    pub signatures_required: AtomicBool,
    pub maint: Mutex<MaintState>,
    pub backlog_until: Mutex<Option<Instant>>,

    // quiet / exclusive timing
    pub last_quiet_end: Mutex<Instant>,
    pub last_exclusive_end: Mutex<Instant>,
    pub quiets_since_exact: AtomicU64,

    /// Max log `sequence` of every successful Apply response seen.
    pub acked_seq_high_water: Arc<AtomicU64>,

    track_keys_flagged: AtomicU64,
    flagged: Mutex<VecDeque<FlaggedPlan>>,
    ext: Extensions,
}

impl Shared {
    pub fn new(cfg: Config, client_t: Arc<dyn ClientTransport>, oracle_t: Box<dyn OracleTransport>) -> Arc<Shared> {
        let active = Arc::new(AtomicBool::new(false));
        let acked = Arc::new(AtomicU64::new(0));
        let clients = Clients::new(client_t, active.clone(), acked.clone());
        let node_states = Arc::new(Mutex::new(NodeStates::new(active.clone())));
        let mut oracle = OracleClient::new(oracle_t);
        oracle.retry = cfg.timing.oracle_retry;
        Arc::new_cyclic(|weak| Shared {
            run: cfg.run.clone(),
            nodes: cfg.nodes.clone(),
            clients,
            oracle,
            slots: WriteSlots::new(weak.clone()),
            resolvers: Resolvers::default(),
            jobs: Jobs::default(),
            registry: TicketRegistry::default(),
            node_states,
            next_key: AtomicU64::new(1),
            next_wid: AtomicU64::new(1),
            next_rid: AtomicU64::new(1),
            next_ref: AtomicU64::new(1),
            next_ledger: AtomicU64::new(1),
            next_own: AtomicU64::new(1),
            next_epoch: AtomicU64::new(1),
            next_untracked_key: AtomicU64::new(1),
            active,
            signatures_required: AtomicBool::new(false),
            maint: Mutex::new(MaintState::Closed),
            backlog_until: Mutex::new(None),
            last_quiet_end: Mutex::new(Instant::now()),
            last_exclusive_end: Mutex::new(Instant::now()),
            quiets_since_exact: AtomicU64::new(0),
            acked_seq_high_water: acked,
            track_keys_flagged: AtomicU64::new(0),
            flagged: Mutex::new(VecDeque::new()),
            ext: Extensions::default(),
            cfg,
        })
    }

    /// Starts the resolver, job and late-poster threads (idempotent).
    pub fn start_threads(self: &Arc<Self>) {
        crate::resolver::start(self);
        crate::jobs::start(self);
    }

    pub fn timing(&self) -> &Timing {
        &self.cfg.timing
    }

    // ---- flags

    /// False once the driver entered degraded mode (permanent, DESIGN §4.2).
    pub fn oracle_ok(&self) -> bool {
        self.oracle.ok()
    }

    pub fn enter_degraded(&self, why: &str) {
        self.oracle.degrade(why);
    }

    pub fn is_active(&self) -> bool {
        self.active.load(Ordering::SeqCst)
    }

    pub fn maint_closed(&self) -> bool {
        *self.maint.lock().unwrap_or_else(|e| e.into_inner()) == MaintState::Closed
    }

    pub fn signatures_required(&self) -> bool {
        self.signatures_required.load(Ordering::SeqCst)
    }

    pub fn note_backlog(&self) {
        *self.backlog_until.lock().unwrap_or_else(|e| e.into_inner()) = Some(Instant::now() + self.cfg.timing.backlog_pause);
    }

    pub fn in_backlog_pause(&self) -> bool {
        self.backlog_until.lock().unwrap_or_else(|e| e.into_inner()).is_some_and(|t| Instant::now() < t)
    }

    pub fn quiets_since_exact(&self) -> u64 {
        self.quiets_since_exact.load(Ordering::SeqCst)
    }

    pub fn reset_quiets_since_exact(&self) {
        self.quiets_since_exact.store(0, Ordering::SeqCst);
    }

    /// Queued plus running quiet-blocking jobs (DESIGN §4.13).
    pub fn blocking_jobs(&self) -> usize {
        self.jobs.blocking()
    }

    // ---- identities

    /// A tracked idempotency key `o-<run>-k<N>`.
    pub fn next_key(&self) -> String {
        format!("o-{}-k{}", self.run, self.next_key.fetch_add(1, Ordering::SeqCst))
    }
    /// A key for an untracked write that retries (`r-<run>-k<N>`).
    pub fn next_untracked_key(&self) -> String {
        format!("r-{}-k{}", self.run, self.next_untracked_key.fetch_add(1, Ordering::SeqCst))
    }
    pub fn next_wid(&self) -> String {
        format!("w{}", self.next_wid.fetch_add(1, Ordering::SeqCst))
    }
    pub fn next_rid(&self) -> String {
        format!("r{}", self.next_rid.fetch_add(1, Ordering::SeqCst))
    }
    /// A tracked reference `o-<run>-ref<N>`.
    pub fn next_ref(&self) -> String {
        format!("o-{}-ref{}", self.run, self.next_ref.fetch_add(1, Ordering::SeqCst))
    }
    /// A tracked ledger name `o-<run>-l<N>`.
    pub fn next_ledger_name(&self) -> String {
        format!("o-{}-l{}", self.run, self.next_ledger.fetch_add(1, Ordering::SeqCst))
    }
    /// The next rule-owned counter value (DESIGN §3.1).
    pub fn next_own(&self) -> u64 {
        self.next_own.fetch_add(1, Ordering::SeqCst)
    }
    /// A rule-owned ledger name `o-<run>-own<N>`.
    pub fn next_own_ledger(&self) -> String {
        format!("o-{}-own{}", self.run, self.next_own())
    }
    /// A rule-owned reference `o-<run>-own-<tag><N>`.
    pub fn next_own_ref(&self, tag: &str) -> String {
        format!("o-{}-own-{}{}", self.run, tag, self.next_own())
    }
    /// A Rust-checked ledger name `r-<family>-<run>-<N>`.
    pub fn rust_ledger(&self, family: &str, n: u64) -> String {
        format!("r-{}-{}-{}", family, self.run, n)
    }
    pub fn tracked_prefix(&self) -> String {
        format!("o-{}-", self.run)
    }

    // ---- trackKey budget and flagged plans

    /// `chance(1/4)` while fewer than 4096 keys were flagged (DESIGN §5.5).
    pub fn draw_track_key(&self, tc: &hegel::TestCase) -> bool {
        if self.track_keys_flagged.load(Ordering::SeqCst) >= TRACK_KEY_BUDGET {
            return false;
        }
        crate::draw::chance(tc, 0.25)
    }

    pub(crate) fn record_flagged(&self, p: FlaggedPlan) {
        self.track_keys_flagged.fetch_add(1, Ordering::SeqCst);
        let mut f = self.flagged.lock().unwrap_or_else(|e| e.into_inner());
        f.push_back(p);
        while f.len() > FLAGGED_PLANS_CAP {
            f.pop_front();
        }
    }

    pub fn track_keys_flagged(&self) -> u64 {
        self.track_keys_flagged.load(Ordering::SeqCst)
    }

    /// Key, request bytes and signing spec of a recent `track_key` original.
    pub fn flagged_plan(&self, wid: &str) -> Option<(String, Vec<Value>, Option<SignSpec>)> {
        let f = self.flagged.lock().unwrap_or_else(|e| e.into_inner());
        f.iter().rev().find(|p| p.wid == wid).map(|p| (p.key.clone(), p.requests.clone(), p.sign.clone()))
    }

    /// The wids of the recorded flagged originals, oldest first.
    pub fn flagged_wids(&self) -> Vec<String> {
        self.flagged.lock().unwrap_or_else(|e| e.into_inner()).iter().map(|p| p.wid.clone()).collect()
    }

    /// A family's extension state.
    pub fn ext<T: Default + Send + Sync + 'static>(&self) -> Arc<T> {
        self.ext.get::<T>()
    }
}
