//! Task 18: backup → restore → verify on separate restore targets
//! (restore.md §3–§7, DESIGN §2.3, §4.10, §4.13).
//!
//! One `RestoreCycle` at a time (a `Shared` extension). The source keeps
//! running; only the cut runs inside a quiet period:
//!
//! - `restore_base` (Idle): seeds a small Rust-checked ledger
//!   (`r-restore-<run>-<n>`, the admission probes' known state), then a job
//!   resets the agents to `gen+1` and takes a full backup into `hegel-restore`
//!   → BaseTaken.
//! - `restore_step` in BaseTaken writes one more seed transaction (the
//!   post-checkpoint delta).
//! - `restore_cut` (QuietHook): Barrier, IncrementalBackup → `S, A`, the
//!   source digest, the second-incremental fence → Cut.
//! - `restore_plan` (Cut): draws the whole restore path.
//! - `restore_step`: one job per phase: Cut → Staged (online RestoreService
//!   or offline `ledgerctl store bootstrap`), Staged → Serving (normal boot,
//!   optional joiner), Serving → Verified (digest parity, CheckStore,
//!   admission probes, optional chained restore) → reset → Idle.
//!
//! Every restore rule is a no-op while the first agent's `/status` does not
//! answer (native runs, stacks without the restore services).
use crate::clients::{CallOpts, Outcome, Route};
use crate::draw;
use crate::machine::{rule, Driver};
use crate::quiet::{HookPlan, QuietCtx, QuietHook};
use crate::shared::Shared;
use hegel::stateful::ConcurrentRule;
use hegel::TestCase;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

pub mod digest;
pub mod env;
pub mod probes;
pub mod rpc;
pub mod stage;
#[cfg(test)]
mod tests;

use digest::Digest;
use env::{AgentReply, AgentStatus, HttpEnv, RTiming, RestoreEnv};

/// The namespace of the cycle's own chain (restore.md §4.3). Each full base
/// backup resets its export list, so it never holds stale exports.
pub const NS: &str = "hegel-restore";
pub const RESTORE_NODE: &str = "restore-0:8888";
pub const JOINER_NODE: &str = "restore-1:8888";

#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum Phase {
    #[default]
    Idle,
    BaseTaken,
    Cut,
    Staged,
    Serving,
    Verified,
}

/// The seed writes on the source (rule-owned Rust-checked ledger).
#[derive(Clone, Debug, Default)]
pub struct Seed {
    pub ledger: String,
    /// Before the base backup: createLedger + T1 (world→probe:a 100, ref
    /// `r1`) + T2 (world→probe:b 7) + T4 (world→probe:c 5), key `k1`.
    pub k1: String,
    pub batch1: Vec<Value>,
    pub t1: u64,
    pub t2: u64,
    pub t4: u64,
    pub r1: String,
    /// T2 reverted (key `k2`), before the base backup.
    pub k2: String,
    /// After the base backup (the delta): T3 (world→probe:a 50, ref `r2`), key `k3`.
    pub k3: String,
    pub batch3: Vec<Value>,
    pub t3: Option<u64>,
}

#[derive(Clone, Debug, Default)]
pub struct Base {
    /// `lastLogSequence` of the full backup.
    pub f: u64,
    pub audit: u64,
    pub applied: u64,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Cut {
    pub s: u64,
    pub a: u64,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Perturb {
    pub cancel_mid: bool,
    pub cancel_after_polls: u32,
    pub duplicate_start: bool,
    pub finalize_early: bool,
    pub agent_bounce: bool,
}

/// The whole restore path, drawn by `restore_plan`.
#[derive(Clone, Debug, Default)]
pub struct Plan {
    pub online: bool,
    pub validate: bool,
    pub perturb: Perturb,
    pub poll_ms: u64,
    pub backoffs: Vec<u64>,
    pub joiner: bool,
    pub chained: bool,
    pub client: usize,
}

#[derive(Clone, Debug, Default)]
pub struct RestoreCycle {
    pub gen: u64,
    pub phase: Phase,
    pub ns: String,
    pub seed: Option<Seed>,
    pub base: Option<Base>,
    pub cut: Option<Cut>,
    pub digest: Option<Digest>,
    pub plan: Option<Plan>,
    pub joiner: bool,
    /// Start of the current budget: the base (until the cut), then the cut.
    pub started: Option<Instant>,
    pub job_running: bool,
    /// restore-0's agent boot count when the cut was taken.
    pub agent_boot: u64,
}

/// The family's state in `Shared`.
#[derive(Default)]
pub struct RestoreState {
    pub cycle: Mutex<RestoreCycle>,
    env: Mutex<Option<Arc<dyn RestoreEnv>>>,
    reach: Mutex<Option<(Instant, bool)>>,
    /// agent → (boot count, child exits already seen).
    exits: Mutex<HashMap<usize, (u64, usize)>>,
    next_seed: AtomicU64,
}

impl RestoreState {
    pub fn env(&self) -> Arc<dyn RestoreEnv> {
        self.env.lock().unwrap_or_else(|e| e.into_inner()).get_or_insert_with(|| Arc::new(HttpEnv::from_env()) as Arc<dyn RestoreEnv>).clone()
    }

    #[cfg(test)]
    pub fn set_env(&self, env: Arc<dyn RestoreEnv>) {
        *self.env.lock().unwrap_or_else(|e| e.into_inner()) = Some(env);
        *self.reach.lock().unwrap_or_else(|e| e.into_inner()) = None;
    }

    pub fn cycle(&self) -> std::sync::MutexGuard<'_, RestoreCycle> {
        self.cycle.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// The first agent answers `/status` (cached for `reach_cache`).
    pub fn reachable(&self) -> bool {
        let env = self.env();
        let t = env.timing();
        let mut r = self.reach.lock().unwrap_or_else(|e| e.into_inner());
        if let Some((at, ok)) = *r {
            if at.elapsed() < t.reach_cache {
                return ok;
            }
        }
        let ok = env.agent_count() > 0 && matches!(env.agent_get(0, "/status", Duration::from_secs(2)), AgentReply::Ok(_));
        *r = Some((Instant::now(), ok));
        ok
    }

    fn cached_unreachable(&self) -> bool {
        matches!(*self.reach.lock().unwrap_or_else(|e| e.into_inner()), Some((_, false)))
    }
}

pub fn state(s: &Shared) -> Arc<RestoreState> {
    s.ext::<RestoreState>()
}

// ---------------------------------------------------------------- registration

pub fn rules() -> Vec<ConcurrentRule<Driver>> {
    vec![rule("restore_base", 0.5, restore_base), rule("restore_plan", 0.5, restore_plan), rule("restore_step", 2.0, restore_step)]
}

pub fn quiet_hooks() -> Vec<QuietHook> {
    vec![QuietHook { name: "restore_cut", budget: Duration::from_secs(60), needs_oracle: false, weight: cut_weight, plan: cut_plan, run: cut_run }]
}

// ---------------------------------------------------------------- job context

/// What every step of the cycle works with.
pub struct Cx<'a> {
    pub d: &'a Driver,
    pub env: Arc<dyn RestoreEnv>,
    pub st: Arc<RestoreState>,
    pub t: RTiming,
    pub client: usize,
    pub gen: u64,
}

impl<'a> Cx<'a> {
    pub fn new(d: &'a Driver, client: usize, gen: u64) -> Cx<'a> {
        let st = state(&d.s);
        let env = st.env();
        let t = env.timing();
        let client = if d.s.clients.len() == 0 { 0 } else { client % d.s.clients.len() };
        Cx { d, env, st, t, client, gen }
    }

    pub fn sleep_ms(&self, ms: u64) {
        std::thread::sleep(Duration::from_millis(ms).min(self.t.poll_cap));
    }

    /// `GET /status` of an agent, with the unexpected-exit check (restore.md
    /// §4.1): a child exit the agent did not request, in restore or normal
    /// mode, within one agent boot, is a finding. Exits reported in the first
    /// status after a container restart are not judged (the restart explains
    /// them).
    pub fn status(&self, agent: usize) -> Option<AgentStatus> {
        let AgentReply::Ok(v) = self.env.agent_get(agent, "/status", Duration::from_secs(3)) else { return None };
        let s = AgentStatus::parse(&v);
        let mut seen = self.st.exits.lock().unwrap_or_else(|e| e.into_inner());
        let entry = seen.entry(agent).or_insert((s.boot_count, s.exits.len()));
        if entry.0 != s.boot_count {
            *entry = (s.boot_count, s.exits.len());
            return Some(s);
        }
        // The agent keeps the newest 64 exits.
        let from = entry.1.min(s.exits.len());
        for e in &s.exits[from..] {
            if !e.by_agent && (e.mode == "restore" || e.mode == "normal") {
                check_unreachable!(
                    "a restore-target ledger process exits unexpectedly",
                    json!({"agent": agent, "mode": e.mode, "code": e.code, "signal": e.signal, "gen": s.gen, "bootCount": s.boot_count})
                );
            }
        }
        entry.1 = s.exits.len();
        Some(s)
    }

    pub fn post(&self, agent: usize, path: &str, mut body: Value, timeout: Duration) -> AgentReply {
        body["gen"] = json!(self.gen);
        self.env.agent_post(agent, path, &body, timeout)
    }

    /// One RPC through the planned client; `node` pins it (restore RPCs
    /// always pass a node: the shared connections know no restore node).
    pub fn call(&self, node: Option<&str>, service: &str, method: &str, req: Value, opts: &CallOpts) -> Outcome {
        let route = match node {
            Some(n) => Route::node(self.client, n),
            None => Route::lib(self.client),
        };
        self.d.call(&route, service, method, req, opts).1
    }

    pub fn restore_call(&self, node: &str, method: &str, req: Value, timeout: Duration) -> Outcome {
        self.call(Some(node), "restore", method, req, &CallOpts::default().timeout(timeout))
    }

    /// A Backup or IncrementalBackup with the ported busy retry: busy is
    /// retried with the planned backoffs for `busy_budget` from the first
    /// busy reply; an ambiguous outcome is retried until `deadline` (a lost
    /// response is harmless: a retried full backup redoes the work, a retried
    /// incremental reports the same sequences with no exports).
    #[allow(clippy::result_large_err)] // the outcome is the refusal
    pub fn backup(&self, node: Option<&str>, method: &str, bucket_id: &str, backoffs: &[u64], deadline: Instant) -> Result<Value, Outcome> {
        let req = json!({"bucketId": bucket_id, "storage": rpc::storage()});
        let mut first_busy: Option<Instant> = None;
        let mut i = 0usize;
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(Outcome::unknown("timeout"));
            }
            let out = self.call(node, "cluster", method, req.clone(), &CallOpts::default().timeout(remaining.min(self.t.backup_rpc)));
            if out.is_ok() {
                if first_busy.is_some() {
                    println!("driver: restore: {method} recovered after a busy destination");
                }
                return Ok(out.response.clone().unwrap_or(json!({})));
            }
            if rpc::is_busy(&out) {
                let fb = *first_busy.get_or_insert_with(Instant::now);
                if fb.elapsed() > self.t.busy_budget {
                    return Err(out);
                }
            } else if !rpc::ambiguous(&out) {
                return Err(out);
            }
            let b = backoffs.get(i % backoffs.len().max(1)).copied().unwrap_or(500);
            i += 1;
            self.sleep_ms(b);
        }
    }
}

/// Clears `job_running` when a job ends, however it ends.
pub struct JobFlag(pub Arc<RestoreState>);
impl Drop for JobFlag {
    fn drop(&mut self) {
        self.0.cycle().job_running = false;
    }
}

/// Submits a cycle job (not quiet-blocking: it writes nothing to the source
/// cluster's business state).
fn submit(d: &Driver, st: &Arc<RestoreState>, name: &'static str, f: impl FnOnce(&Driver) + Send + 'static) {
    st.cycle().job_running = true;
    let flag = JobFlag(st.clone());
    d.submit_job(crate::jobs::Job {
        name,
        blocks_quiet: false,
        run: Box::new(move |d| {
            let _flag = flag;
            f(d)
        }),
    });
}

// ---------------------------------------------------------------- budget

/// Whether the cycle ran out of its wall budget: from the base until the cut
/// (`cut_wait`: quiet periods come every 1.5–4 minutes), then `cycle_budget`
/// from the cut (deviation from the single 10-minute budget, recorded in the
/// porting file).
pub fn expired(c: &RestoreCycle, t: &RTiming) -> bool {
    let Some(started) = c.started else { return false };
    let limit = match c.phase {
        Phase::Idle => return false,
        Phase::BaseTaken => t.cut_wait,
        _ => t.cycle_budget,
    };
    started.elapsed() > limit
}

/// Ends the cycle: reset the agents (same gen), wipe the chained namespace,
/// back to Idle. `verified` feeds the budget coverage.
pub fn end_cycle(cx: &Cx, verified: bool, why: &str) {
    let (phase, gen) = {
        let c = cx.st.cycle();
        (c.phase, c.gen)
    };
    println!("driver: restore: cycle g{gen} ends in {phase:?}: {why}");
    for agent in 0..cx.env.agent_count() {
        let _ = cx.post(agent, "/reset", json!({}), Duration::from_secs(60));
    }
    let _ = cx.post(0, "/wipe-namespace", json!({"bucketId": format!("restore-g{gen}")}), Duration::from_secs(60));
    check_sometimes!(verified, "a restore cycle completes within its wall budget", json!({"gen": gen, "phase": format!("{phase:?}"), "why": why}));
    let mut c = cx.st.cycle();
    let job_running = c.job_running;
    *c = RestoreCycle { gen, job_running, ..Default::default() };
}

// ---------------------------------------------------------------- restore_base

const SEED_BUDGET: Duration = Duration::from_secs(4);

fn seed_send(d: &Driver, client: usize, requests: Vec<Value>, key: &str, backoffs: &[u64]) -> Option<Value> {
    let mut send = crate::write::UntrackedSend::new(requests, Some(key.to_string()));
    send.schedule = vec![Route::lib(client)];
    send.backoffs_ms = backoffs.to_vec();
    send.budget = SEED_BUDGET;
    let r = d.send_untracked(send);
    if r.outcome.is_ok() {
        r.outcome.response.clone()
    } else {
        println!("driver: restore: seed write {key}: {}", r.outcome.summary());
        None
    }
}

fn created_ids(resp: &Value) -> Vec<u64> {
    crate::wire::apply_logs(resp).iter().filter_map(|l| crate::wire::created_transaction(l).map(|t| crate::wire::u64_field(t, "id"))).collect()
}

pub fn tx(ledger: &str, src: &str, dst: &str, amount: u64, reference: Option<&str>) -> Value {
    crate::wire::create_tx_request(ledger, &[crate::wire::Posting::new(src, dst, amount, "COIN")], reference)
}

pub fn revert(ledger: &str, id: u64) -> Value {
    json!({"apply": {"ledger": ledger, "action": {"revertTransaction": {"transactionId": id.to_string()}}}})
}

/// The pre-checkpoint seed batch.
pub fn seed_batch1(ledger: &str, r1: &str) -> Vec<Value> {
    vec![
        json!({"createLedger": {"name": ledger}}),
        tx(ledger, "world", "probe:a", 100, Some(r1)),
        tx(ledger, "world", "probe:b", 7, None),
        tx(ledger, "world", "probe:c", 5, None),
    ]
}

pub fn restore_base(d: &Driver, tc: TestCase) {
    d.guarded("restore_base", &tc, |d, tc| {
        let st = state(&d.s);
        {
            let c = st.cycle();
            if c.phase != Phase::Idle || c.job_running {
                return None;
            }
        }
        // Draws.
        let client = draw::pick(tc, d.s.clients.len().max(1));
        let backoffs = draw::backoffs(tc, 8);
        if !st.reachable() {
            return None;
        }
        // Seed (synchronously: rules are parked during quiet periods, so the
        // seed never overlaps another family's quiet check).
        let n = st.next_seed.fetch_add(1, Ordering::SeqCst) + 1;
        let ledger = d.s.rust_ledger("restore", n);
        let mut seed = Seed {
            ledger: ledger.clone(),
            k1: d.s.next_untracked_key(),
            r1: format!("r-{}-restore-ref{n}a", d.s.run),
            k2: d.s.next_untracked_key(),
            k3: d.s.next_untracked_key(),
            ..Default::default()
        };
        seed.batch1 = seed_batch1(&ledger, &seed.r1);
        seed.batch3 = vec![tx(&ledger, "world", "probe:a", 50, Some(&format!("r-{}-restore-ref{n}b", d.s.run)))];
        let resp = seed_send(d, client, seed.batch1.clone(), &seed.k1, &backoffs)?;
        let ids = created_ids(&resp);
        let [t1, t2, t4] = ids[..] else {
            println!("driver: restore: unexpected seed response {resp}");
            return None;
        };
        (seed.t1, seed.t2, seed.t4) = (t1, t2, t4);
        seed_send(d, client, vec![revert(&ledger, t2)], &seed.k2, &backoffs)?;
        {
            let mut c = st.cycle();
            if c.phase != Phase::Idle || c.job_running {
                return None;
            }
            c.seed = Some(seed);
        }
        submit(d, &st, "restore_base", move |d| base_job(d, client, backoffs));
        Some(())
    });
}

/// Idle → BaseTaken: reset the agents to `gen+1`, full backup into `ns`.
pub fn base_job(d: &Driver, client: usize, backoffs: Vec<u64>) {
    let mut cx = Cx::new(d, client, 0);
    let Some(s0) = cx.status(0) else {
        println!("driver: restore: restore-0 does not answer; base postponed");
        return;
    };
    let gen = cx.st.cycle().gen.max(s0.gen) + 1;
    cx.gen = gen;
    if !matches!(cx.post(0, "/reset", json!({}), Duration::from_secs(60)), AgentReply::Ok(_)) {
        println!("driver: restore: reset of restore-0 to g{gen} failed; base postponed");
        return;
    }
    for agent in 1..cx.env.agent_count() {
        let _ = cx.post(agent, "/reset", json!({}), Duration::from_secs(60));
    }
    {
        cx.st.cycle().gen = gen;
    }
    let deadline = Instant::now() + cx.t.base_deadline;
    match cx.backup(None, "Backup", NS, &backoffs, deadline) {
        Ok(resp) => {
            let base = Base {
                f: crate::wire::u64_field(&resp, "lastLogSequence"),
                audit: crate::wire::u64_field(&resp, "lastAuditSequence"),
                applied: crate::wire::u64_field(&resp, "lastAppliedIndex"),
            };
            println!("driver: restore: g{gen} base backup F={} A={} applied={}", base.f, base.audit, base.applied);
            check_sometimes!(true, "restore base backup completes", json!({"gen": gen, "F": base.f, "totalFiles": resp.get("totalFiles")}));
            let mut c = cx.st.cycle();
            c.phase = Phase::BaseTaken;
            c.ns = NS.to_string();
            c.base = Some(base);
            c.started = Some(Instant::now());
        }
        Err(out) => println!("driver: restore: g{gen} base backup failed (stays Idle): {}", out.summary()),
    }
}

// ---------------------------------------------------------------- restore_cut

fn cut_weight(s: &Shared) -> f64 {
    let st = state(s);
    if st.cached_unreachable() {
        return 0.0;
    }
    let c = st.cycle();
    let seeded = c.seed.as_ref().is_some_and(|s| s.t3.is_some());
    if c.phase == Phase::BaseTaken && seeded && !c.job_running {
        6.0
    } else {
        0.0
    }
}

struct CutPlan {
    backoffs: Vec<u64>,
}

fn cut_plan(_d: &Driver, tc: &TestCase) -> HookPlan {
    Box::new(CutPlan { backoffs: draw::backoffs(tc, 8) })
}

fn cut_run(d: &Driver, ctx: &mut QuietCtx, plan: &HookPlan) {
    let Some(p) = plan.downcast_ref::<CutPlan>() else { return };
    let st = state(&d.s);
    let (gen, f, seeded) = {
        let c = st.cycle();
        if c.phase != Phase::BaseTaken || c.job_running {
            return;
        }
        (c.gen, c.base.as_ref().map(|b| b.f).unwrap_or(0), c.seed.as_ref().is_some_and(|s| s.t3.is_some()))
    };
    if !seeded {
        return;
    }
    let cx = Cx::new(d, ctx.client, gen);
    let completed = take_cut(&cx, f, &p.backoffs, ctx.deadline);
    check_sometimes!(completed, "restore_cut completes within its quiet budget", json!({"gen": gen}));
}

/// The cut (restore.md §4.3 step 2). Returns whether it reached an outcome
/// (a cut, or a definite reason not to take one) before the deadline.
pub fn take_cut(cx: &Cx, f: u64, backoffs: &[u64], deadline: Instant) -> bool {
    let gen = cx.gen;
    let ns = NS;
    let b = cx.call(None, "bucket", "Barrier", json!({}), &CallOpts::default().timeout(Duration::from_secs(5)));
    if !b.is_ok() {
        println!("driver: restore: cut g{gen}: barrier {}", b.summary());
        return Instant::now() < deadline;
    }
    let first = match cx.backup(None, "IncrementalBackup", ns, backoffs, deadline) {
        Ok(r) => r,
        Err(out) => {
            if rpc::is_no_full_checkpoint(&out) {
                // The namespace lost its full checkpoint: start over.
                println!("driver: restore: cut g{gen}: no full checkpoint; back to Idle");
                let mut c = cx.st.cycle();
                c.phase = Phase::Idle;
                c.base = None;
                c.seed = None;
                c.started = None;
                return true;
            }
            println!("driver: restore: cut g{gen}: incremental backup {}", out.summary());
            return Instant::now() < deadline && !out.is_unknown();
        }
    };
    let cut = Cut { s: crate::wire::u64_field(&first, "lastLogSequence"), a: crate::wire::u64_field(&first, "lastAuditSequence") };
    // Decide emptiness from S > F, not from logEntriesExported: a retried
    // call reports 0 exports.
    check_sometimes!(cut.s > f, "a restore cut has a non-empty delta", json!({"gen": gen, "S": cut.s, "F": f}));
    if cut.s <= f {
        println!("driver: restore: cut g{gen}: empty delta (S={} F={f}); skipped", cut.s);
        return true;
    }
    let remaining = deadline.saturating_duration_since(Instant::now());
    let src = match digest::take(cx.d, cx.env.as_ref(), &digest::DigestTarget { client: cx.client, node: None, consistency: "linearizable" }, remaining.min(cx.t.digest_timeout)) {
        Ok(dg) => dg,
        Err(e) => {
            println!("driver: restore: cut g{gen}: source digest: {e}");
            return Instant::now() < deadline;
        }
    };
    // The fence: a second incremental must export nothing and report the
    // same sequences, which proves nothing committed between the cut and the
    // digest. Ambiguous outcomes are retried (a lost fence response is
    // harmless: the retry reports the same S/A, and a commit after S shows
    // as S2 > S whichever attempt exported it).
    let fence = match cx.backup(None, "IncrementalBackup", ns, backoffs, deadline) {
        Ok(r) => r,
        Err(out) => {
            println!("driver: restore: cut g{gen}: fence {}", out.summary());
            return Instant::now() < deadline && !out.is_unknown();
        }
    };
    let (s2, a2) = (crate::wire::u64_field(&fence, "lastLogSequence"), crate::wire::u64_field(&fence, "lastAuditSequence"));
    let details = json!({"gen": gen, "S": cut.s, "A": cut.a, "S2": s2, "A2": a2, "fence": fence});
    check_always_or_unreachable!(s2 >= cut.s, "a restore cut's incremental backup log sequence does not regress", details);
    check_always_or_unreachable!(a2 >= cut.a, "a restore cut's incremental backup audit sequence does not regress", details);
    let holds = s2 == cut.s
        && a2 == cut.a
        && crate::wire::u64_field(&fence, "logEntriesExported") == 0
        && crate::wire::u64_field(&fence, "auditEntriesExported") == 0;
    check_sometimes!(holds, "a restore cut fence holds", details);
    if !holds {
        // Something committed after the cut: drop it, keep the base; a later
        // quiet period takes a new cut on the same chain.
        println!("driver: restore: cut g{gen}: fence broken; cut dropped");
        return true;
    }
    let boot = cx.status(0).map(|s| s.boot_count).unwrap_or(0);
    let mut c = cx.st.cycle();
    if c.phase != Phase::BaseTaken || c.gen != gen {
        return true;
    }
    println!("driver: restore: g{gen} cut S={} A={} F={f}", cut.s, cut.a);
    c.phase = Phase::Cut;
    c.cut = Some(cut);
    c.digest = Some(src);
    c.started = Some(Instant::now());
    c.agent_boot = boot;
    true
}

// ---------------------------------------------------------------- restore_plan

pub fn restore_plan(d: &Driver, tc: TestCase) {
    d.guarded("restore_plan", &tc, |d, tc| {
        let st = state(&d.s);
        {
            let c = st.cycle();
            if c.phase != Phase::Cut || c.plan.is_some() {
                return None;
            }
        }
        let joiner_on = st.env().joiner_enabled();
        let online = draw::chance(tc, 0.75);
        let validate = !online && draw::chance(tc, 0.5);
        let perturb = if online {
            Perturb {
                cancel_mid: draw::chance(tc, 0.25),
                cancel_after_polls: draw::int_in(tc, 0, 3) as u32,
                duplicate_start: draw::chance(tc, 0.25),
                finalize_early: draw::chance(tc, 0.25),
                agent_bounce: draw::chance(tc, 0.125),
            }
        } else {
            Perturb::default()
        };
        let plan = Plan {
            online,
            validate,
            perturb,
            poll_ms: draw::int_in(tc, 200, 3000),
            backoffs: draw::backoffs(tc, 8),
            joiner: joiner_on && draw::chance(tc, 0.5),
            chained: draw::chance(tc, 0.25),
            client: draw::pick(tc, d.s.clients.len().max(1)),
        };
        let mut c = st.cycle();
        if c.phase == Phase::Cut && c.plan.is_none() {
            println!("driver: restore: g{} plan {plan:?}", c.gen);
            c.joiner = plan.joiner;
            c.plan = Some(plan);
        }
        Some(())
    });
}

// ---------------------------------------------------------------- restore_step

pub fn restore_step(d: &Driver, tc: TestCase) {
    d.guarded("restore_step", &tc, |d, tc| {
        let st = state(&d.s);
        let client = draw::pick(tc, d.s.clients.len().max(1));
        let backoffs = draw::backoffs(tc, 4);
        let (phase, gen, has_plan, expired_now, need_delta) = {
            let c = st.cycle();
            if c.job_running || c.phase == Phase::Idle {
                return None;
            }
            let t = st.env().timing();
            (c.phase, c.gen, c.plan.is_some(), expired(&c, &t), c.seed.as_ref().is_some_and(|s| s.t3.is_none()))
        };
        if !st.reachable() {
            return None;
        }
        if expired_now {
            let c = client;
            submit(d, &st, "restore_reset", move |d| end_cycle(&Cx::new(d, c, gen), false, "wall budget expired"));
            return Some(());
        }
        match phase {
            Phase::BaseTaken if need_delta => {
                // The post-checkpoint delta (a rule, so never during a quiet check).
                let (k3, batch3) = {
                    let c = st.cycle();
                    let s = c.seed.as_ref()?;
                    (s.k3.clone(), s.batch3.clone())
                };
                let resp = seed_send(d, client, batch3, &k3, &backoffs)?;
                let t3 = created_ids(&resp).first().copied()?;
                let mut c = st.cycle();
                if c.gen == gen && c.phase == Phase::BaseTaken {
                    if let Some(s) = c.seed.as_mut() {
                        s.t3 = Some(t3);
                    }
                }
            }
            Phase::Cut if has_plan => submit(d, &st, "restore_stage", move |d| stage::stage_job(d, gen)),
            Phase::Staged => submit(d, &st, "restore_serve", move |d| stage::serve_job(d, gen)),
            Phase::Serving => submit(d, &st, "restore_verify", move |d| stage::verify_job(d, gen)),
            _ => {}
        }
        Some(())
    });
}
