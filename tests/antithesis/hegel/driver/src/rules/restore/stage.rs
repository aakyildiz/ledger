//! The long steps of the cycle, each run as one job (DESIGN §4.13):
//! Cut → Staged (online or offline), Staged → Serving, Serving → Verified
//! (and the optional chained restore) → Idle.
use super::digest::{self, Digest, DigestTarget};
use super::env::AgentReply;
use super::rpc::{self, DlState};
use super::{end_cycle, probes, Cx, Perturb, Phase, Plan, JOINER_NODE, NS, RESTORE_NODE};
use crate::clients::{CallOpts, Outcome};
use crate::machine::Driver;
use serde_json::{json, Value};
use std::time::{Duration, Instant};

/// What one restore into an agent works with.
#[derive(Clone, Debug)]
pub struct StageCtx {
    pub agent: usize,
    pub node: &'static str,
    pub cluster: String,
    pub ns: String,
    /// The cut's log sequence (PreviewRestore `lastSequence`).
    pub expect_seq: u64,
    /// The full backup's applied index (the marker's boundary, min 1).
    pub expect_applied: u64,
    /// The source's ledger names (a subset of the preview's: PreviewRestore
    /// also lists soft-deleted ledgers, implementer note 2).
    pub expect_ledgers: Vec<String>,
    pub perturb: Perturb,
    pub poll_ms: u64,
    pub validate: bool,
    pub deadline: Instant,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StageResult {
    /// Finalize committed (durable proof seen).
    Staged,
    /// Try again later from the same phase (nothing to discard).
    Retry,
    /// Failed staging must be discarded: reset the agent, then retry.
    Reset,
}

fn left(deadline: Instant) -> Duration {
    deadline.saturating_duration_since(Instant::now())
}

/// Reads the durable proof after a finalize (EN-3029): only the marker and
/// `checkpoints/0` count, never the RPC answer.
fn finalize_proof(cx: &Cx, sc: &StageCtx, rpc_ok: bool, out: &Outcome) -> StageResult {
    // The status may lag a just-committed finalize by nothing (the marker is
    // written before the RPC answers), but the agent may be restarting.
    let mut status = None;
    for _ in 0..5 {
        if let Some(s) = cx.status(sc.agent) {
            status = Some(s);
            break;
        }
        cx.sleep_ms(sc.poll_ms);
    }
    let Some(s) = status else { return StageResult::Retry };
    let details = json!({"gen": cx.gen, "cluster": sc.cluster, "rpcOk": rpc_ok, "outcome": out.summary(),
        "marker": {"present": s.marker_present, "lastAppliedIndex": s.marker_index}, "checkpoint0": s.checkpoint0});
    if s.committed() {
        let want = sc.expect_applied.max(1);
        let mut d = details.clone();
        d["expected"] = json!(want);
        check_always!(s.marker_index == want, "the restore marker records the base backup's applied index", d);
        return StageResult::Staged;
    }
    if rpc_ok {
        check_unreachable!("finalize succeeded without durable proof", details);
    }
    StageResult::Reset
}

/// Starts restore mode and waits until RestoreService answers (NotFound for
/// an unknown job id) rather than Unavailable or Unimplemented.
fn boot_restore_mode(cx: &Cx, sc: &StageCtx) -> Result<(), StageResult> {
    match cx.post(sc.agent, "/start", json!({"mode": "restore", "clusterId": sc.cluster, "nodeId": 1}), Duration::from_secs(60)) {
        AgentReply::Ok(_) => {}
        AgentReply::Conflict(body) => {
            let s = super::env::AgentStatus::parse(body.get("status").unwrap_or(&Value::Null));
            if s.marker_present {
                // A finalize committed earlier (lost answer, then a restart).
                return Err(finalize_proof(cx, sc, false, &Outcome::unknown("marker")));
            }
            println!("driver: restore: g{} start restore mode refused: {body}", cx.gen);
            return Err(StageResult::Retry);
        }
        r => {
            println!("driver: restore: g{} start restore mode: {r:?}", cx.gen);
            return Err(StageResult::Retry);
        }
    }
    let until = Instant::now() + cx.t.boot_wait.min(left(sc.deadline));
    loop {
        let out = cx.restore_call(sc.node, "GetDownloadStatus", json!({"jobId": "hegel-probe"}), Duration::from_secs(3));
        if out.is_ok() || (!out.is_unknown() && out.code == "NotFound") {
            return Ok(());
        }
        // Unimplemented: the child is not (yet) in restore mode; Unavailable:
        // it is still booting. Both are expected while it boots.
        if Instant::now() > until {
            println!("driver: restore: g{} restore mode does not answer: {}", cx.gen, out.summary());
            return Err(StageResult::Retry);
        }
        cx.sleep_ms(sc.poll_ms);
    }
}

/// `StartDownloadBackup`: `Ok(Some(job))`, or `Ok(None)` when a previous
/// start (whose answer was lost) already staged the backup.
fn start_download(cx: &Cx, sc: &StageCtx) -> Result<Option<String>, StageResult> {
    let req = json!({"bucketId": sc.ns, "storage": rpc::storage()});
    loop {
        if left(sc.deadline).is_zero() {
            return Err(StageResult::Retry);
        }
        let out = cx.restore_call(sc.node, "StartDownloadBackup", req.clone(), Duration::from_secs(10));
        if out.is_ok() {
            let job = out.response.as_ref().and_then(|r| r.get("jobId")).and_then(Value::as_str).unwrap_or("").to_string();
            return Ok(Some(job));
        }
        if rpc::failed_precondition(&out, "backup already downloaded") {
            return Ok(None);
        }
        if rpc::failed_precondition(&out, "another download is already in progress") || rpc::ambiguous(&out) || rpc::unimplemented(&out) {
            // A previous start whose answer was lost is still running: wait
            // until it ends (then "already downloaded" or a fresh start).
            cx.sleep_ms(sc.poll_ms);
            continue;
        }
        println!("driver: restore: g{} StartDownloadBackup: {}", cx.gen, out.summary());
        return Err(StageResult::Retry);
    }
}

/// Online restore (restore.md §4.3 step 3) into `sc.agent`.
pub fn online(cx: &Cx, sc: &StageCtx) -> StageResult {
    let p = sc.perturb;
    let mut canceled = false;
    // The cancel took effect (the job reported CANCELED), so the download
    // that later succeeds is a retry. A cancel that lands after the job
    // finished leaves it SUCCEEDED and retries nothing.
    let mut cancel_took = false;
    let mut bounced = false;
    'attempt: for attempt in 0..6 {
        if left(sc.deadline).is_zero() {
            return StageResult::Retry;
        }
        if attempt > 0 {
            println!("driver: restore: g{} online restore attempt {}", cx.gen, attempt + 1);
        }
        if let Err(r) = boot_restore_mode(cx, sc) {
            return r;
        }
        let job = match start_download(cx, sc) {
            Ok(j) => j,
            Err(r) => return r,
        };
        let fresh = job.is_some();
        if let Some(mut job) = job {
            if p.duplicate_start {
                // A duplicate start is refused while the first runs, or after
                // it staged; if the first already failed, it starts anew.
                let dup = cx.restore_call(sc.node, "StartDownloadBackup", json!({"bucketId": sc.ns, "storage": rpc::storage()}), Duration::from_secs(10));
                if dup.is_ok() {
                    if let Some(j) = dup.response.as_ref().and_then(|r| r.get("jobId")).and_then(Value::as_str) {
                        job = j.to_string();
                    }
                }
                println!("driver: restore: g{} duplicate StartDownloadBackup: {}", cx.gen, dup.summary());
            }
            let mut polls = 0u32;
            loop {
                if left(sc.deadline).is_zero() {
                    return StageResult::Retry;
                }
                let out = cx.restore_call(sc.node, "GetDownloadStatus", json!({"jobId": job}), Duration::from_secs(5));
                if !out.is_ok() {
                    if !out.is_unknown() && out.code == "NotFound" {
                        // The server restarted: the job is gone and staging was discarded.
                        continue 'attempt;
                    }
                    cx.sleep_ms(sc.poll_ms);
                    continue;
                }
                let resp = out.response.clone().unwrap_or(json!({}));
                match rpc::download_state(&resp) {
                    DlState::Running | DlState::Unknown => {
                        polls += 1;
                        if p.cancel_mid && !canceled && polls > p.cancel_after_polls {
                            let c = cx.restore_call(sc.node, "CancelDownload", json!({"jobId": job}), Duration::from_secs(5));
                            println!("driver: restore: g{} CancelDownload: {}", cx.gen, c.summary());
                            canceled = true;
                        } else if p.agent_bounce && !bounced && polls >= 1 {
                            bounced = true;
                            let _ = cx.post(sc.agent, "/stop", json!({"graceful": false}), Duration::from_secs(60));
                            println!("driver: restore: g{} restore-agent child stopped mid-download", cx.gen);
                            continue 'attempt;
                        }
                        cx.sleep_ms(sc.poll_ms);
                    }
                    DlState::Succeeded => break,
                    DlState::Canceled => {
                        cancel_took |= canceled;
                        continue 'attempt;
                    }
                    DlState::Failed => {
                        let msg = resp.get("errorMessage").and_then(Value::as_str).unwrap_or("").to_string();
                        if let Some(frag) = rpc::corrupt_backup(&msg) {
                            check_unreachable!("restore download finds a corrupt backup", json!({"gen": cx.gen, "ns": sc.ns, "error": msg, "match": frag}));
                            return StageResult::Reset;
                        }
                        println!("driver: restore: g{} download failed (retried): {msg}", cx.gen);
                        cx.sleep_ms(sc.poll_ms);
                        continue 'attempt;
                    }
                }
            }
        }
        check_sometimes!(true, "an online restore download succeeds", json!({"gen": cx.gen, "ns": sc.ns}));
        if cancel_took {
            check_sometimes!(true, "a canceled restore download is retried", json!({"gen": cx.gen}));
        }
        // Finalize before validate: only when this server staged the backup
        // just now, so no validation can have passed yet.
        if p.finalize_early && fresh {
            let out = cx.restore_call(sc.node, "FinalizeRestore", json!({}), Duration::from_secs(30));
            if out.is_ok() {
                // A finalize without validation must never succeed.
                check_always_or_unreachable!(false, "finalize before validate is refused as a failed precondition", json!({"gen": cx.gen, "outcome": out.summary()}));
                return finalize_proof(cx, sc, true, &out);
            }
            if !rpc::ambiguous(&out) {
                let refused = rpc::failed_precondition(&out, "has not passed validation");
                check_always_or_unreachable!(refused, "finalize before validate is refused as a failed precondition", json!({"gen": cx.gen, "outcome": out.summary()}));
                check_sometimes!(refused, "a finalize before validate is refused", json!({"gen": cx.gen}));
            }
        }
        // Validate (stream). An incomplete stream is retried, never a pass.
        loop {
            let remaining = left(sc.deadline);
            if remaining.is_zero() {
                return StageResult::Retry;
            }
            let opts = CallOpts::default().timeout(remaining.min(cx.t.validate_timeout)).max_items(1_000_000);
            let out = cx.call(Some(sc.node), "restore", "ValidateRestore", json!({}), &opts);
            if out.is_ok() && out.complete {
                let errors: Vec<Value> = out.items.clone().unwrap_or_default().into_iter().filter(|e| e.get("error").is_some()).collect();
                check_always!(errors.is_empty(), "the staged restore validates clean", json!({"gen": cx.gen, "ns": sc.ns, "errors": errors.iter().take(10).collect::<Vec<_>>(), "count": errors.len()}));
                if !errors.is_empty() {
                    return StageResult::Reset;
                }
                break;
            }
            if rpc::failed_precondition(&out, "no backup downloaded") {
                continue 'attempt;
            }
            if out.is_ok() || rpc::ambiguous(&out) || rpc::failed_precondition(&out, "already running") || rpc::unimplemented(&out) {
                cx.sleep_ms(sc.poll_ms);
                continue;
            }
            println!("driver: restore: g{} ValidateRestore: {}", cx.gen, out.summary());
            return StageResult::Reset;
        }
        // Preview.
        let mut previewed = false;
        for _ in 0..5 {
            let out = cx.restore_call(sc.node, "PreviewRestore", json!({}), Duration::from_secs(10));
            if out.is_ok() {
                let r = out.response.clone().unwrap_or(json!({}));
                let last = crate::wire::u64_field(&r, "lastSequence");
                check_always!(last == sc.expect_seq, "a restore preview reports the cut sequence", json!({"gen": cx.gen, "lastSequence": last, "S": sc.expect_seq}));
                let names: Vec<String> = r.get("ledgerNames").and_then(Value::as_array).map(|a| a.iter().filter_map(|n| n.as_str().map(str::to_string)).collect()).unwrap_or_default();
                let missing: Vec<&String> = sc.expect_ledgers.iter().filter(|n| !names.contains(n)).collect();
                check_always!(missing.is_empty(), "a restore preview lists the source ledgers", json!({"gen": cx.gen, "missing": missing, "preview": names.len(), "source": sc.expect_ledgers.len()}));
                previewed = true;
                break;
            }
            if rpc::failed_precondition(&out, "no backup downloaded") {
                continue 'attempt;
            }
            cx.sleep_ms(sc.poll_ms);
        }
        if !previewed {
            return StageResult::Retry;
        }
        // Finalize: success or error, only the durable proof decides.
        let out = cx.restore_call(sc.node, "FinalizeRestore", json!({}), Duration::from_secs(60).min(left(sc.deadline).max(Duration::from_secs(5))));
        if rpc::failed_precondition(&out, "has not passed validation") {
            // The server restarted after validation (the flag is in memory,
            // gotcha 19): download and validate again.
            continue 'attempt;
        }
        return finalize_proof(cx, sc, out.is_ok(), &out);
    }
    StageResult::Retry
}

/// Offline restore: `ledgerctl store bootstrap` through the agent.
pub fn offline(cx: &Cx, sc: &StageCtx) -> StageResult {
    let _ = cx.post(sc.agent, "/stop", json!({"graceful": true}), Duration::from_secs(60));
    let timeout = left(sc.deadline).max(Duration::from_secs(5));
    let reply = cx.post(sc.agent, "/offline-bootstrap", json!({"bucketId": sc.ns, "validate": sc.validate}), timeout);
    let mut result = match reply {
        AgentReply::Ok(v) => Some(v),
        _ => None,
    };
    if result.is_none() {
        // Lost answer or a busy agent: wait for the run to end and use its record.
        loop {
            if left(sc.deadline).is_zero() {
                return StageResult::Retry;
            }
            match cx.status(sc.agent) {
                Some(s) if !s.offline_running => {
                    result = s.last_offline.filter(|o| crate::wire::u64_field(o, "gen") == cx.gen);
                    break;
                }
                _ => cx.sleep_ms(sc.poll_ms),
            }
        }
    }
    let res = result.unwrap_or(json!({}));
    let exit = res.get("exit").and_then(Value::as_i64).unwrap_or(-1);
    let tail: Vec<String> = res.get("tail").and_then(Value::as_array).map(|a| a.iter().filter_map(|l| l.as_str().map(str::to_string)).collect()).unwrap_or_default();
    let text = tail.join("\n");
    let details = json!({"gen": cx.gen, "validate": sc.validate, "exit": exit, "verdict": res.get("verdictLine"), "tail": tail.iter().rev().take(10).collect::<Vec<_>>()});
    if let Some(frag) = rpc::corrupt_backup(&text) {
        check_unreachable!("restore download finds a corrupt backup", json!({"gen": cx.gen, "ns": sc.ns, "match": frag, "details": details}));
        return StageResult::Reset;
    }
    if sc.validate && text.contains("Failed to validate backup") {
        check_always!(false, "the staged restore validates clean", details);
        return StageResult::Reset;
    }
    let r = finalize_proof(cx, sc, false, &Outcome::unknown("offline"));
    if r == StageResult::Staged && exit == 0 {
        if sc.validate {
            check_sometimes!(true, "an offline bootstrap with validation succeeds", details);
        } else {
            check_sometimes!(true, "an offline bootstrap without validation succeeds", details);
        }
    }
    if r != StageResult::Staged {
        println!("driver: restore: g{} offline bootstrap did not commit: {details}", cx.gen);
    }
    r
}

/// Stop, start normal: the marker drives the first boot (restore.md §2.4
/// step 7). Waits until a pinned Barrier succeeds.
pub fn serve(cx: &Cx, agent: usize, node: &'static str, cluster: &str, deadline: Instant) -> bool {
    let _ = cx.post(agent, "/stop", json!({"graceful": true}), Duration::from_secs(60));
    if !matches!(cx.post(agent, "/start", json!({"mode": "normal", "clusterId": cluster, "nodeId": 1}), Duration::from_secs(60)), AgentReply::Ok(_)) {
        return false;
    }
    let until = Instant::now() + cx.t.serve_wait.min(left(deadline));
    loop {
        let out = cx.call(Some(node), "bucket", "Barrier", json!({}), &CallOpts::default().timeout(Duration::from_secs(5)));
        if out.is_ok() {
            break;
        }
        if Instant::now() > until {
            println!("driver: restore: g{} {node} does not serve: {}", cx.gen, out.summary());
            return false;
        }
        cx.sleep_ms(1000);
    }
    if let Some(s) = cx.status(agent) {
        check_always!(!s.marker_present, "the restore marker is consumed on the first normal boot", json!({"gen": cx.gen, "node": node, "marker": s.marker_index}));
    }
    true
}

struct Snapshot {
    plan: Plan,
    seq: u64,
    applied: u64,
    f: u64,
    digest: Digest,
    deadline: Instant,
    agent_boot: u64,
}

fn snapshot(cx: &Cx, gen: u64, phase: Phase) -> Option<Snapshot> {
    let c = cx.st.cycle();
    if c.gen != gen || c.phase != phase {
        return None;
    }
    Some(Snapshot {
        plan: c.plan.clone()?,
        seq: c.cut?.s,
        applied: c.base.as_ref()?.applied,
        f: c.base.as_ref()?.f,
        digest: c.digest.clone()?,
        deadline: c.started? + cx.t.cycle_budget,
        agent_boot: c.agent_boot,
    })
}

fn advance(cx: &Cx, gen: u64, from: Phase, to: Phase) {
    let mut c = cx.st.cycle();
    if c.gen == gen && c.phase == from {
        println!("driver: restore: g{gen} {from:?} -> {to:?}");
        c.phase = to;
    }
}

fn plan_client(d: &Driver, st: &super::RestoreState) -> usize {
    let _ = d;
    st.cycle().plan.as_ref().map(|p| p.client).unwrap_or(0)
}

/// Cut → Staged.
pub fn stage_job(d: &Driver, gen: u64) {
    let client = plan_client(d, &super::state(&d.s));
    let cx = Cx::new(d, client, gen);
    let Some(sn) = snapshot(&cx, gen, Phase::Cut) else { return };
    let sc = StageCtx {
        agent: 0,
        node: RESTORE_NODE,
        cluster: format!("restore-g{gen}"),
        ns: NS.to_string(),
        expect_seq: sn.seq,
        expect_applied: sn.applied,
        expect_ledgers: sn.digest.ledger_names(),
        perturb: sn.plan.perturb,
        poll_ms: sn.plan.poll_ms,
        validate: sn.plan.validate,
        deadline: sn.deadline,
    };
    let r = if sn.plan.online { online(&cx, &sc) } else { offline(&cx, &sc) };
    match r {
        StageResult::Staged => advance(&cx, gen, Phase::Cut, Phase::Staged),
        StageResult::Retry => {}
        StageResult::Reset => {
            // Failed staging is discarded before the next attempt.
            let _ = cx.post(0, "/reset", json!({}), Duration::from_secs(60));
        }
    }
}

/// Staged → Serving (and the joiner).
pub fn serve_job(d: &Driver, gen: u64) {
    let client = plan_client(d, &super::state(&d.s));
    let cx = Cx::new(d, client, gen);
    let Some(sn) = snapshot(&cx, gen, Phase::Staged) else { return };
    if !serve(&cx, 0, RESTORE_NODE, &format!("restore-g{gen}"), sn.deadline) {
        return;
    }
    check_sometimes!(true, "a restored cluster serves", json!({"gen": gen}));
    if sn.plan.joiner && cx.env.agent_count() > 1 {
        let _ = cx.post(1, "/reset", json!({}), Duration::from_secs(60));
        let r = cx.post(1, "/start", json!({"mode": "join", "clusterId": format!("restore-g{gen}"), "nodeId": 2, "join": "restore-0:7777"}), Duration::from_secs(60));
        println!("driver: restore: g{gen} joiner start: {r:?}");
    }
    advance(&cx, gen, Phase::Staged, Phase::Serving);
}

/// Every index of the restored cluster is READY (currentVersion > 0, no
/// pending rewrite), or the wait timed out (the digest reads no index).
fn wait_indexes(cx: &Cx, node: &str, deadline: Instant) {
    let until = Instant::now() + cx.t.index_wait.min(left(deadline));
    loop {
        let out = cx.call(Some(node), "bucket", "GetIndexStatus", json!({}), &CallOpts::default());
        if out.is_ok() {
            let r = out.response.clone().unwrap_or(json!({}));
            let ready = r.get("indexes").and_then(Value::as_array).is_none_or(|a| {
                a.iter().all(|e| crate::wire::u64_field(e, "currentVersion") > 0 && crate::wire::u64_field(e, "pendingVersion") == 0)
            });
            if ready {
                return;
            }
        }
        if Instant::now() > until {
            println!("driver: restore: g{} indexes not READY on {node} in time", cx.gen);
            return;
        }
        cx.sleep_ms(1000);
    }
}

/// CheckStore on a node: `Some(errors)` when the stream completed.
pub fn check_store(cx: &Cx, node: &str, deadline: Instant) -> Option<Vec<Value>> {
    let timeout = left(deadline).min(Duration::from_secs(300));
    if timeout.is_zero() {
        return None;
    }
    let out = cx.call(Some(node), "bucket", "CheckStore", json!({}), &CallOpts::default().timeout(timeout).max_items(1_000_000));
    if !(out.is_ok() && out.complete) {
        println!("driver: restore: g{} CheckStore on {node} inconclusive: {}", cx.gen, out.summary());
        return None;
    }
    Some(out.items.unwrap_or_default().into_iter().filter(|e| e.get("error").is_some()).collect())
}

fn assert_check_store(cx: &Cx, when: &str, node: &str, deadline: Instant) {
    if let Some(errors) = check_store(cx, node, deadline) {
        check_always!(errors.is_empty(), "the restored store check finds no error", json!({"gen": cx.gen, "when": when, "node": node, "errors": errors.iter().take(10).collect::<Vec<_>>(), "count": errors.len()}));
    }
}

/// Serving → Verified → (chained restore) → Idle.
pub fn verify_job(d: &Driver, gen: u64) {
    let client = plan_client(d, &super::state(&d.s));
    let cx = Cx::new(d, client, gen);
    let Some(sn) = snapshot(&cx, gen, Phase::Serving) else { return };
    wait_indexes(&cx, RESTORE_NODE, sn.deadline);
    let target = DigestTarget { client: cx.client, node: Some(RESTORE_NODE), consistency: "linearizable" };
    let restored = match digest::take(d, cx.env.as_ref(), &target, left(sn.deadline).min(cx.t.digest_timeout)) {
        Ok(dg) => dg,
        Err(e) => {
            println!("driver: restore: g{gen} restored digest: {e}");
            return;
        }
    };
    let diffs = digest::compare(&sn.digest, &restored);
    let ctx = json!({"gen": gen, "S": sn.seq, "F": sn.f, "node": RESTORE_NODE});
    digest::assert_sections(&diffs, &ctx);
    let all_equal = diffs.iter().all(|x| x.equal);

    if sn.plan.joiner && cx.env.agent_count() > 1 {
        // A follower answers linearizable reads only once it caught up with
        // the restored genesis; a stale digest is taken for coverage.
        let t1 = DigestTarget { client: cx.client, node: Some(JOINER_NODE), consistency: "linearizable" };
        let until = Instant::now() + cx.t.serve_wait.min(left(sn.deadline));
        loop {
            match digest::take(d, cx.env.as_ref(), &t1, left(sn.deadline).min(cx.t.digest_timeout)) {
                Ok(j) => {
                    let eq = digest::compare(&sn.digest, &j).iter().all(|x| x.equal);
                    check_always_or_unreachable!(eq, "a restored joiner syncs from the restored genesis", json!({"gen": gen, "diffs": digest::compare(&sn.digest, &j).into_iter().filter(|x| !x.equal).map(|x| json!({"section": x.section, "diff": x.details})).collect::<Vec<_>>()}));
                    let stale = DigestTarget { client: cx.client, node: Some(JOINER_NODE), consistency: "stale" };
                    let _ = digest::take(d, cx.env.as_ref(), &stale, left(sn.deadline).min(cx.t.digest_timeout));
                    break;
                }
                Err(e) if Instant::now() > until => {
                    println!("driver: restore: g{gen} joiner digest: {e}");
                    break;
                }
                Err(_) => cx.sleep_ms(1000),
            }
        }
    }

    assert_check_store(&cx, "before probes", RESTORE_NODE, sn.deadline);
    // Chained restore, part 1: a full backup of the restored cluster into its
    // own namespace (empty bucket id → its cluster id restore-g<gen>), which
    // is wiped first so no object of an earlier chain can be read (review
    // finding 4).
    let chain_ns = format!("restore-g{gen}");
    let mut chain_base = None;
    if sn.plan.chained {
        match cx.post(0, "/wipe-namespace", json!({"bucketId": chain_ns}), Duration::from_secs(90)) {
            AgentReply::Ok(_) => match cx.backup(Some(RESTORE_NODE), "Backup", "", &sn.plan.backoffs, Instant::now() + cx.t.base_deadline.min(left(sn.deadline))) {
                Ok(r) => chain_base = Some(r),
                Err(out) => println!("driver: restore: g{gen} chained base backup: {}", out.summary()),
            },
            r => println!("driver: restore: g{gen} chained namespace not wiped ({r:?}); no chained restore"),
        }
    }
    let seed = cx.st.cycle().seed.clone();
    if let Some(seed) = seed {
        probes::run(&cx, RESTORE_NODE, &seed, sn.seq);
    }
    assert_check_store(&cx, "after probes", RESTORE_NODE, sn.deadline);
    check_sometimes!(all_equal && sn.seq > sn.f, "a restore round trip verifies with a non-empty delta", json!({"gen": gen, "S": sn.seq, "F": sn.f}));
    advance(&cx, gen, Phase::Serving, Phase::Verified);
    if let Some(cb) = chain_base {
        chained(&cx, &chain_ns, &cb, &sn);
    }
    let restarted = cx.status(0).is_some_and(|s| s.boot_count != sn.agent_boot);
    check_sometimes!(restarted, "a restore cycle survives a restart of restore-0", json!({"gen": gen}));
    end_cycle(&cx, true, "verified");
}

/// Chained restore, part 2: an incremental of the restored cluster (its
/// delta is the probes' writes), then restore that chain into restore-0
/// under a new cluster id and compare with the restored cluster's digest.
fn chained(cx: &Cx, ns: &str, chain_base: &Value, sn: &Snapshot) {
    let gen = cx.gen;
    let inc = match cx.backup(Some(RESTORE_NODE), "IncrementalBackup", "", &sn.plan.backoffs, Instant::now() + cx.t.base_deadline.min(left(sn.deadline))) {
        Ok(r) => r,
        Err(out) => {
            println!("driver: restore: g{gen} chained incremental: {}", out.summary());
            return;
        }
    };
    let target = DigestTarget { client: cx.client, node: Some(RESTORE_NODE), consistency: "linearizable" };
    let reference = match digest::take(cx.d, cx.env.as_ref(), &target, left(sn.deadline).min(cx.t.digest_timeout)) {
        Ok(dg) => dg,
        Err(e) => {
            println!("driver: restore: g{gen} chained reference digest: {e}");
            return;
        }
    };
    // The joiner belongs to the cluster being replaced: stop it first.
    for agent in (1..cx.env.agent_count()).filter(|_| sn.plan.joiner) {
        let _ = cx.post(agent, "/reset", json!({}), Duration::from_secs(60));
    }
    let _ = cx.post(0, "/reset", json!({}), Duration::from_secs(60));
    let cluster = format!("restore-g{gen}c");
    let sc = StageCtx {
        agent: 0,
        node: RESTORE_NODE,
        cluster: cluster.clone(),
        ns: ns.to_string(),
        expect_seq: crate::wire::u64_field(&inc, "lastLogSequence"),
        expect_applied: crate::wire::u64_field(chain_base, "lastAppliedIndex"),
        expect_ledgers: reference.ledger_names(),
        perturb: Perturb::default(),
        poll_ms: sn.plan.poll_ms,
        validate: true,
        deadline: sn.deadline,
    };
    if online(cx, &sc) != StageResult::Staged || !serve(cx, 0, RESTORE_NODE, &cluster, sn.deadline) {
        println!("driver: restore: g{gen} chained restore did not complete");
        return;
    }
    wait_indexes(cx, RESTORE_NODE, sn.deadline);
    match digest::take(cx.d, cx.env.as_ref(), &target, left(sn.deadline).min(cx.t.digest_timeout)) {
        Ok(chain) => {
            let diffs = digest::compare(&reference, &chain);
            digest::assert_sections(&diffs, &json!({"gen": gen, "chained": true, "ns": ns}));
            check_sometimes!(diffs.iter().all(|x| x.equal), "a chained restore verifies", json!({"gen": gen}));
        }
        Err(e) => println!("driver: restore: g{gen} chained digest: {e}"),
    }
}
