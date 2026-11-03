//! Unit tests of the restore cycle with a fake environment (agents and
//! digests) and the testkit's scripted clients.
use super::digest::Digest;
use super::env::{AgentReply, RTiming, RestoreEnv};
use super::stage::{self, StageCtx, StageResult};
use super::*;
use crate::testkit::{self, FakeReply, FakeTransport};
use std::sync::Mutex;

#[derive(Default, Clone)]
struct FakeAgent {
    gen: u64,
    mode: String,
    alive: bool,
    boot: u64,
    exits: Vec<Value>,
    marker: bool,
    /// `/status` reports a committed marker (a finalize committed whose
    /// answer was lost) without the agent's `/start` seeing it.
    marker_on_status: bool,
    posts: Vec<(String, Value)>,
}

struct FakeEnv {
    reachable: Mutex<bool>,
    agents: Mutex<Vec<FakeAgent>>,
    /// Digest sections returned for every request, by node ("" = lib route).
    digests: Mutex<HashMap<String, Map>>,
    digest_calls: Mutex<Vec<Value>>,
    joiner: bool,
}

type Map = serde_json::Map<String, Value>;

impl FakeEnv {
    fn new() -> Arc<FakeEnv> {
        Arc::new(FakeEnv {
            reachable: Mutex::new(true),
            agents: Mutex::new(vec![FakeAgent { mode: "idle".into(), boot: 1, ..Default::default() }, FakeAgent { mode: "idle".into(), boot: 1, ..Default::default() }]),
            digests: Mutex::new(HashMap::new()),
            digest_calls: Mutex::new(Vec::new()),
            joiner: false,
        })
    }
    fn agent(&self, i: usize) -> FakeAgent {
        self.agents.lock().unwrap()[i].clone()
    }
    fn with_agent(&self, i: usize, f: impl FnOnce(&mut FakeAgent)) {
        f(&mut self.agents.lock().unwrap()[i]);
    }
    fn set_digest(&self, node: &str, sections: Map) {
        self.digests.lock().unwrap().insert(node.to_string(), sections);
    }
    fn status_json(a: &FakeAgent) -> Value {
        let marker = a.marker || a.marker_on_status;
        json!({"gen": a.gen, "mode": a.mode, "alive": a.alive, "bootCount": a.boot, "childExits": a.exits,
            "marker": {"present": marker, "lastAppliedIndex": if marker { 5 } else { 0 }}, "checkpoint0": marker,
            "offline": {"running": false, "failed": false}})
    }
}

impl RestoreEnv for FakeEnv {
    fn agent_count(&self) -> usize {
        2
    }
    fn agent_get(&self, agent: usize, _path: &str, _timeout: Duration) -> AgentReply {
        if !*self.reachable.lock().unwrap() {
            return AgentReply::Unreachable;
        }
        AgentReply::Ok(Self::status_json(&self.agent(agent)))
    }
    fn agent_post(&self, agent: usize, path: &str, body: &Value, _timeout: Duration) -> AgentReply {
        if !*self.reachable.lock().unwrap() {
            return AgentReply::Unreachable;
        }
        let mut agents = self.agents.lock().unwrap();
        let a = &mut agents[agent];
        a.posts.push((path.to_string(), body.clone()));
        let gen = body["gen"].as_u64().unwrap_or(0);
        if gen < a.gen {
            return AgentReply::Conflict(json!({"error": "stale gen"}));
        }
        match path {
            "/reset" => {
                a.gen = gen;
                a.mode = "idle".into();
                a.alive = false;
                a.marker = false;
                a.marker_on_status = false;
            }
            "/start" => {
                if body["mode"] == "restore" && a.marker {
                    return AgentReply::Conflict(json!({"error": "marker present", "status": Self::status_json(a)}));
                }
                a.mode = body["mode"].as_str().unwrap_or("").to_string();
                a.alive = true;
                if a.mode == "normal" {
                    a.marker = false;
                    a.marker_on_status = false;
                }
            }
            "/stop" => a.alive = false,
            "/offline-bootstrap" => {
                a.marker = true;
                return AgentReply::Ok(json!({"gen": gen, "exit": 0, "verdictLine": "Bootstrap complete", "tail": ["Bootstrap complete (index=5, ledgers=3)."]}));
            }
            _ => {}
        }
        AgentReply::Ok(Self::status_json(a))
    }
    fn digest(&self, _d: &Driver, _client: usize, body: &Value, _timeout: Duration) -> Result<Value, String> {
        self.digest_calls.lock().unwrap().push(body.clone());
        let node = body.get("node").and_then(Value::as_str).unwrap_or("").to_string();
        let all = self.digests.lock().unwrap().get(&node).cloned().ok_or("no digest")?;
        let mut sections = Map::new();
        for s in body["sections"].as_array().unwrap() {
            let s = s.as_str().unwrap();
            sections.insert(s.to_string(), all.get(s).cloned().unwrap_or(json!({"count": 0, "sha256": "e3", "items": []})));
        }
        Ok(json!({"ok": true, "sections": sections}))
    }
    fn joiner_enabled(&self) -> bool {
        self.joiner
    }
    fn timing(&self) -> RTiming {
        RTiming {
            cycle_budget: Duration::from_secs(20),
            cut_wait: Duration::from_secs(20),
            boot_wait: Duration::from_millis(300),
            serve_wait: Duration::from_millis(300),
            index_wait: Duration::from_millis(100),
            validate_timeout: Duration::from_secs(2),
            busy_budget: Duration::from_millis(100),
            backup_rpc: Duration::from_secs(1),
            base_deadline: Duration::from_secs(2),
            poll_cap: Duration::from_millis(2),
            reach_cache: Duration::ZERO,
            digest_timeout: Duration::from_secs(1),
        }
    }
}

fn ok(v: Value) -> FakeReply {
    FakeReply::Outcome(Outcome::from_json(&json!({"ok": true, "response": v})))
}

fn err(code: &str, message: &str) -> FakeReply {
    FakeReply::Outcome(Outcome::from_json(&json!({"ok": false, "code": code, "message": message})))
}

fn setup() -> (Driver, Arc<FakeTransport>, Arc<FakeEnv>) {
    let (d, ft, _fo) = testkit::driver();
    testkit::reset();
    let env = FakeEnv::new();
    state(&d.s).set_env(env.clone());
    (d, ft, env)
}

fn sections(sha: &str) -> Map {
    let mut m = Map::new();
    m.insert("ledgers".into(), json!({"count": 2, "items": [{"name": "o-test-l1", "id": 1}, {"name": "r-restore-test-1", "id": 2}]}));
    for s in ["indexes", "signingKeys", "sinks"] {
        m.insert(s.into(), json!({"count": 0, "items": []}));
    }
    for s in digest::HASH_SECTIONS {
        m.insert(s.to_string(), json!({"count": 4, "sha256": sha, "lastKey": "k"}));
    }
    m
}

fn digest_of(m: &Map) -> Digest {
    Digest { sections: m.clone() }
}

fn put_cycle(d: &Driver, f: impl FnOnce(&mut RestoreCycle)) {
    f(&mut state(&d.s).cycle());
}

fn fired_cond(message: &str, cond: bool) -> bool {
    testkit::assertions().iter().any(|a| a.message == message && a.cond == cond)
}

fn sc(seq: u64, perturb: Perturb) -> StageCtx {
    StageCtx {
        agent: 0,
        node: RESTORE_NODE,
        cluster: "restore-g1".into(),
        ns: NS.into(),
        expect_seq: seq,
        expect_applied: 5,
        expect_ledgers: vec!["o-test-l1".into()],
        perturb,
        poll_ms: 1,
        validate: true,
        deadline: Instant::now() + Duration::from_secs(10),
    }
}

/// Scripts a clean online download: boot probe, start, poll SUCCEEDED,
/// validate clean, preview at `seq`.
fn script_download(ft: &FakeTransport, seq: u64) {
    ft.set_default("restore", "GetDownloadStatus", ok(json!({"state": "DOWNLOAD_STATE_SUCCEEDED"})));
    ft.set_default("restore", "StartDownloadBackup", ok(json!({"jobId": "j1"})));
    ft.set_default("restore", "ValidateRestore", FakeReply::Outcome(Outcome::from_json(&json!({"ok": true, "items": [{"progress": {}}], "complete": true}))));
    ft.set_default("restore", "PreviewRestore", ok(json!({"lastSequence": seq.to_string(), "ledgerNames": ["o-test-l1", "o-test-deleted"]})));
}

#[test]
fn a_lost_finalize_answer_with_the_marker_present_is_committed() {
    let (d, ft, env) = setup();
    script_download(&ft, 40);
    ft.set_default("restore", "FinalizeRestore", FakeReply::Lost(None));
    env.with_agent(0, |a| a.marker_on_status = true);
    let cx = Cx::new(&d, 0, 1);
    assert_eq!(stage::online(&cx, &sc(40, Perturb::default())), StageResult::Staged);
    assert!(fired_cond("the restore marker records the base backup's applied index", true));
    assert!(fired_cond("a restore preview reports the cut sequence", true));
    assert!(fired_cond("a restore preview lists the source ledgers", true), "the preview's soft-deleted ledger is tolerated");
    assert!(fired_cond("the staged restore validates clean", true));
    assert!(fired_cond("an online restore download succeeds", true));
    assert!(!testkit::fired("finalize succeeded without durable proof"));
    // Every restore RPC is pinned to restore-0.
    for c in ft.calls().iter().filter(|c| c["service"] == "restore") {
        assert_eq!(c["node"], RESTORE_NODE, "{c}");
    }
}

#[test]
fn a_lost_finalize_answer_without_the_marker_resets() {
    let (d, ft, _env) = setup();
    script_download(&ft, 40);
    ft.set_default("restore", "FinalizeRestore", FakeReply::Lost(None));
    let cx = Cx::new(&d, 0, 1);
    assert_eq!(stage::online(&cx, &sc(40, Perturb::default())), StageResult::Reset);
    assert!(!testkit::fired("finalize succeeded without durable proof"));
}

#[test]
fn a_successful_finalize_without_proof_is_a_finding() {
    let (d, ft, _env) = setup();
    script_download(&ft, 40);
    ft.set_default("restore", "FinalizeRestore", ok(json!({"message": "done"})));
    let cx = Cx::new(&d, 0, 1);
    assert_eq!(stage::online(&cx, &sc(40, Perturb::default())), StageResult::Reset);
    assert!(testkit::fired("finalize succeeded without durable proof"));
}

#[test]
fn restart_and_perturbations_are_handled() {
    let (d, ft, env) = setup();
    script_download(&ft, 40);
    // The boot probe is Unimplemented then NotFound (the child entered
    // restore mode); the first start is answered "another download is in
    // progress" (a lost earlier start), then succeeds; the duplicate start is
    // refused; the first poll is RUNNING (cancel), then CANCELED; the retry
    // succeeds.
    ft.script("restore", "GetDownloadStatus", vec![err("Unimplemented", ""), err("NotFound", "unknown job id"), ok(json!({"state": "DOWNLOAD_STATE_RUNNING"})), ok(json!({"state": "DOWNLOAD_STATE_CANCELED"}))]);
    ft.script("restore", "StartDownloadBackup", vec![err("FailedPrecondition", "another download is already in progress"), ok(json!({"jobId": "j1"})), err("FailedPrecondition", "another download is already in progress")]);
    ft.script("restore", "FinalizeRestore", vec![err("FailedPrecondition", "staged backup has not passed validation")]);
    ft.set_default("restore", "FinalizeRestore", FakeReply::Lost(None));
    env.with_agent(0, |a| a.marker_on_status = true);
    let p = Perturb { cancel_mid: true, cancel_after_polls: 0, duplicate_start: true, finalize_early: true, agent_bounce: false };
    let cx = Cx::new(&d, 0, 1);
    assert_eq!(stage::online(&cx, &sc(40, p)), StageResult::Staged);
    assert_eq!(ft.calls_of("restore", "CancelDownload").len(), 1);
    assert!(fired_cond("a canceled restore download is retried", true));
    assert!(fired_cond("finalize before validate is refused as a failed precondition", true));
    assert!(fired_cond("a finalize before validate is refused", true));
}

#[test]
fn a_cancel_that_lands_after_the_download_finished_is_no_retry() {
    let (d, ft, env) = setup();
    script_download(&ft, 40);
    // The boot probe (Unimplemented, then NotFound), then RUNNING (the
    // cancel is sent), then SUCCEEDED: the cancel came too late.
    ft.script("restore", "GetDownloadStatus", vec![err("Unimplemented", ""), err("NotFound", "unknown job id"), ok(json!({"state": "DOWNLOAD_STATE_RUNNING"})), ok(json!({"state": "DOWNLOAD_STATE_SUCCEEDED"}))]);
    ft.set_default("restore", "FinalizeRestore", FakeReply::Lost(None));
    env.with_agent(0, |a| a.marker_on_status = true);
    let p = Perturb { cancel_mid: true, cancel_after_polls: 0, ..Perturb::default() };
    let cx = Cx::new(&d, 0, 1);
    assert_eq!(stage::online(&cx, &sc(40, p)), StageResult::Staged);
    assert_eq!(ft.calls_of("restore", "CancelDownload").len(), 1);
    assert!(!testkit::fired("a canceled restore download is retried"));
}

#[test]
fn a_corrupt_backup_is_unreachable_and_a_transport_failure_is_retried() {
    let (d, ft, _env) = setup();
    script_download(&ft, 40);
    ft.script("restore", "GetDownloadStatus", vec![
        ok(json!({})),
        ok(json!({"state": "DOWNLOAD_STATE_FAILED", "errorMessage": "downloading data/x: dial tcp: connection refused"})),
        ok(json!({})),
        ok(json!({"state": "DOWNLOAD_STATE_FAILED", "errorMessage": "applying exports: size mismatch in segment exports/logs-1: got 3 bytes, manifest declares 4"})),
    ]);
    let cx = Cx::new(&d, 0, 1);
    assert_eq!(stage::online(&cx, &sc(40, Perturb::default())), StageResult::Reset);
    let hits: Vec<_> = testkit::assertions().into_iter().filter(|a| a.message == "restore download finds a corrupt backup").collect();
    assert_eq!(hits.len(), 1, "only the integrity failure");
    assert_eq!(hits[0].details["match"], "size mismatch in segment");
}

#[test]
fn validation_errors_and_preview_mismatch_are_findings() {
    let (d, ft, env) = setup();
    script_download(&ft, 40);
    ft.set_default("restore", "PreviewRestore", ok(json!({"lastSequence": "39", "ledgerNames": []})));
    ft.set_default("restore", "FinalizeRestore", FakeReply::Lost(None));
    env.with_agent(0, |a| a.marker_on_status = true);
    let cx = Cx::new(&d, 0, 1);
    stage::online(&cx, &sc(40, Perturb::default()));
    assert!(fired_cond("a restore preview reports the cut sequence", false));
    assert!(fired_cond("a restore preview lists the source ledgers", false));

    testkit::reset();
    ft.set_default("restore", "ValidateRestore", FakeReply::Outcome(Outcome::from_json(&json!({"ok": true, "items": [{"error": {"message": "hash mismatch"}}], "complete": true}))));
    assert_eq!(stage::online(&cx, &sc(40, Perturb::default())), StageResult::Reset);
    assert!(fired_cond("the staged restore validates clean", false));
}

#[test]
fn the_cut_decides_emptiness_from_s_and_f_and_requires_the_fence() {
    let (d, ft, env) = setup();
    env.set_digest("", sections("aa"));
    put_cycle(&d, |c| {
        c.gen = 1;
        c.phase = Phase::BaseTaken;
        c.base = Some(Base { f: 10, audit: 3, applied: 5 });
    });
    let cx = Cx::new(&d, 0, 1);
    let deadline = Instant::now() + Duration::from_secs(5);
    // S == F: skipped, Sometimes false.
    ft.script("cluster", "IncrementalBackup", vec![ok(json!({"lastLogSequence": "10", "lastAuditSequence": "3", "logEntriesExported": "0"}))]);
    assert!(take_cut(&cx, 10, &[1], deadline));
    assert!(fired_cond("a restore cut has a non-empty delta", false));
    assert_eq!(state(&d.s).cycle().phase, Phase::BaseTaken);
    // A broken fence (something committed after the cut): dropped.
    testkit::reset();
    ft.script("cluster", "IncrementalBackup", vec![
        ok(json!({"lastLogSequence": "12", "lastAuditSequence": "5", "logEntriesExported": "2"})),
        ok(json!({"lastLogSequence": "13", "lastAuditSequence": "6", "logEntriesExported": "1", "auditEntriesExported": "1"})),
    ]);
    assert!(take_cut(&cx, 10, &[1], deadline));
    assert!(fired_cond("a restore cut fence holds", false));
    assert!(fired_cond("a restore cut's incremental backup log sequence does not regress", true));
    assert_eq!(state(&d.s).cycle().phase, Phase::BaseTaken);
    // A lost fence answer is retried; the fence holds: Cut.
    testkit::reset();
    ft.script("cluster", "IncrementalBackup", vec![
        FakeReply::Lost(None),
        ok(json!({"lastLogSequence": "12", "lastAuditSequence": "5", "logEntriesExported": "2"})),
        err("FailedPrecondition", "x: backup: destination already has a running job"),
        FakeReply::Lost(None),
        ok(json!({"lastLogSequence": "12", "lastAuditSequence": "5"})),
    ]);
    assert!(take_cut(&cx, 10, &[1], deadline));
    assert!(fired_cond("a restore cut has a non-empty delta", true));
    assert!(fired_cond("a restore cut fence holds", true));
    let st = state(&d.s);
    let c = st.cycle();
    assert_eq!(c.phase, Phase::Cut);
    assert_eq!(c.cut, Some(Cut { s: 12, a: 5 }));
    assert_eq!(c.digest.as_ref().unwrap().ledger_names(), vec!["o-test-l1", "r-restore-test-1"]);
    let calls = ft.calls_of("cluster", "IncrementalBackup");
    assert!(calls.iter().all(|c| c["request"]["bucketId"] == NS));
}

#[test]
fn a_regressing_fence_is_a_finding() {
    let (d, ft, env) = setup();
    env.set_digest("", sections("aa"));
    put_cycle(&d, |c| {
        c.gen = 1;
        c.phase = Phase::BaseTaken;
    });
    ft.script("cluster", "IncrementalBackup", vec![ok(json!({"lastLogSequence": "12", "lastAuditSequence": "5"})), ok(json!({"lastLogSequence": "11", "lastAuditSequence": "4"}))]);
    let cx = Cx::new(&d, 0, 1);
    take_cut(&cx, 10, &[1], Instant::now() + Duration::from_secs(5));
    assert!(fired_cond("a restore cut's incremental backup log sequence does not regress", false));
    assert!(fired_cond("a restore cut's incremental backup audit sequence does not regress", false));
}

#[test]
fn restore_cut_weight_is_zero_outside_base_taken() {
    let (d, _ft, _env) = setup();
    let hook = &quiet_hooks()[0];
    assert_eq!(hook.name, "restore_cut");
    assert_eq!(hook.budget, Duration::from_secs(60));
    assert!(!hook.needs_oracle);
    for phase in [Phase::Idle, Phase::Cut, Phase::Staged, Phase::Serving, Phase::Verified] {
        put_cycle(&d, |c| {
            c.phase = phase;
            c.seed = Some(Seed { t3: Some(4), ..Default::default() });
        });
        assert_eq!((hook.weight)(&d.s), 0.0, "{phase:?}");
    }
    put_cycle(&d, |c| c.phase = Phase::BaseTaken);
    assert_eq!((hook.weight)(&d.s), 6.0);
    // Not before the delta is seeded, nor while a job runs.
    put_cycle(&d, |c| c.seed = Some(Seed::default()));
    assert_eq!((hook.weight)(&d.s), 0.0);
    put_cycle(&d, |c| {
        c.seed = Some(Seed { t3: Some(4), ..Default::default() });
        c.job_running = true;
    });
    assert_eq!((hook.weight)(&d.s), 0.0);
}

#[test]
fn every_rule_is_a_no_op_without_a_reachable_agent() {
    let (d, ft, env) = setup();
    *env.reachable.lock().unwrap() = false;
    for phase in [Phase::Idle, Phase::BaseTaken, Phase::Cut, Phase::Staged, Phase::Serving] {
        put_cycle(&d, |c| {
            c.phase = phase;
            c.plan = Some(Plan { poll_ms: 1, ..Default::default() });
            c.seed = Some(Seed::default());
            c.started = Some(Instant::now());
        });
        for r in rules() {
            testkit::run_rule(&d, r.apply);
        }
        assert!(!state(&d.s).cycle().job_running, "{phase:?}");
    }
    assert!(ft.calls().is_empty(), "no RPC: {:?}", ft.calls());
    assert!(env.agent(0).posts.is_empty());
    assert_eq!((quiet_hooks()[0].weight)(&d.s), 0.0, "an unreachable agent disables the cut");
}

#[test]
fn rules_have_the_spec_weights_and_steps_run_as_jobs() {
    let (d, ft, env) = setup();
    let w: Vec<(String, f64)> = rules().iter().map(|r| (r.name.clone(), r.weight)).collect();
    assert_eq!(w, vec![("restore_base".to_string(), 0.5), ("restore_plan".to_string(), 0.5), ("restore_step".to_string(), 2.0)]);
    // A slow restore server: the step returns at once, the job carries the wait.
    ft.set_default("restore", "GetDownloadStatus", FakeReply::Delayed(Duration::from_millis(400), Outcome::from_json(&json!({"ok": false, "code": "Unavailable"}))));
    env.set_digest("", sections("aa"));
    put_cycle(&d, |c| {
        c.gen = 1;
        c.phase = Phase::Cut;
        c.base = Some(Base { f: 1, audit: 1, applied: 5 });
        c.cut = Some(Cut { s: 4, a: 2 });
        c.digest = Some(digest_of(&sections("aa")));
        c.plan = Some(Plan { online: true, poll_ms: 1, backoffs: vec![1], ..Default::default() });
        c.started = Some(Instant::now());
    });
    let t0 = Instant::now();
    testkit::run_rule(&d, restore_step);
    assert!(t0.elapsed() < Duration::from_millis(300), "the rule did not block: {:?}", t0.elapsed());
    assert!(state(&d.s).cycle().job_running, "a stage job was submitted");
    testkit::wait_until(Duration::from_secs(10), || !state(&d.s).cycle().job_running);
    assert_eq!(state(&d.s).cycle().phase, Phase::Cut, "restore mode never answered: retry later");
}

#[test]
fn restore_plan_draws_the_path_once() {
    let (d, _ft, _env) = setup();
    put_cycle(&d, |c| c.phase = Phase::Cut);
    testkit::run_rule(&d, restore_plan);
    let p = state(&d.s).cycle().plan.clone().expect("a plan");
    assert!((200..=3000).contains(&p.poll_ms));
    assert!(!p.joiner, "the joiner is off without RESTORE_JOINER=1");
    assert!(p.online || p.perturb == Perturb::default());
}

#[test]
fn the_budget_runs_from_the_base_until_the_cut_then_from_the_cut() {
    let t = RTiming { cut_wait: Duration::from_millis(50), cycle_budget: Duration::from_millis(100), ..RTiming::default() };
    let mut c = RestoreCycle { phase: Phase::BaseTaken, started: Some(Instant::now() - Duration::from_millis(60)), ..Default::default() };
    assert!(expired(&c, &t));
    c.phase = Phase::Cut;
    assert!(!expired(&c, &t));
    c.started = Some(Instant::now() - Duration::from_millis(150));
    assert!(expired(&c, &t));
    c.phase = Phase::Idle;
    assert!(!expired(&c, &t));
}

#[test]
fn an_expired_cycle_resets_without_a_finding() {
    let (d, _ft, env) = setup();
    put_cycle(&d, |c| {
        c.gen = 3;
        c.phase = Phase::Staged;
        c.plan = Some(Plan::default());
        c.started = Some(Instant::now() - Duration::from_secs(60));
    });
    env.with_agent(0, |a| a.gen = 3);
    testkit::run_rule(&d, restore_step);
    testkit::wait_until(Duration::from_secs(5), || state(&d.s).cycle().phase == Phase::Idle && !state(&d.s).cycle().job_running);
    assert_eq!(state(&d.s).cycle().gen, 3);
    assert!(fired_cond("a restore cycle completes within its wall budget", false));
    let a = testkit::assertions();
    assert!(a.iter().all(|r| r.kind == "Sometimes" || r.kind == "Reachable" || r.cond), "no failing Always: {a:?}");
    let posts: Vec<String> = env.agent(0).posts.iter().map(|p| p.0.clone()).collect();
    assert!(posts.contains(&"/reset".to_string()));
    assert!(posts.contains(&"/wipe-namespace".to_string()));
    assert_eq!(env.agent(0).posts.iter().find(|p| p.0 == "/wipe-namespace").unwrap().1["bucketId"], "restore-g3");
}

#[test]
fn unexpected_child_exits_are_findings_except_after_a_restart() {
    let (d, _ft, env) = setup();
    let cx = Cx::new(&d, 0, 1);
    cx.status(0);
    env.with_agent(0, |a| a.exits.push(json!({"mode": "restore", "code": 0, "byAgent": true})));
    cx.status(0);
    assert!(!testkit::fired("a restore-target ledger process exits unexpectedly"));
    // A container restart: the first status after it is not judged.
    env.with_agent(0, |a| {
        a.boot = 2;
        a.exits = vec![json!({"mode": "normal", "code": 1, "byAgent": false})];
    });
    cx.status(0);
    assert!(!testkit::fired("a restore-target ledger process exits unexpectedly"));
    env.with_agent(0, |a| a.exits.push(json!({"mode": "normal", "code": 2, "byAgent": false})));
    cx.status(0);
    assert!(testkit::fired("a restore-target ledger process exits unexpectedly"));
    // Join-mode exits are not judged (the joiner is optional coverage).
    testkit::reset();
    env.with_agent(0, |a| a.exits.push(json!({"mode": "join", "code": 1, "byAgent": false})));
    cx.status(0);
    assert!(!testkit::fired("a restore-target ledger process exits unexpectedly"));
}

#[test]
fn offline_bootstrap_stages_with_proof() {
    let (d, _ft, env) = setup();
    let cx = Cx::new(&d, 0, 1);
    let mut s = sc(40, Perturb::default());
    s.validate = false;
    assert_eq!(stage::offline(&cx, &s), StageResult::Staged);
    assert!(fired_cond("an offline bootstrap without validation succeeds", true));
    assert!(fired_cond("the restore marker records the base backup's applied index", true));
    let off = env.agent(0).posts.iter().find(|p| p.0 == "/offline-bootstrap").unwrap().1.clone();
    assert_eq!(off["bucketId"], NS);
    assert_eq!(off["validate"], false);
}

#[test]
fn serve_then_verify_compares_every_section() {
    let (d, ft, env) = setup();
    env.set_digest(RESTORE_NODE, sections("aa"));
    ft.set_default("bucket", "CheckStore", FakeReply::Outcome(Outcome::from_json(&json!({"ok": true, "items": [], "complete": true}))));
    put_cycle(&d, |c| {
        c.gen = 1;
        c.phase = Phase::Staged;
        c.base = Some(Base { f: 1, audit: 1, applied: 5 });
        c.cut = Some(Cut { s: 4, a: 2 });
        c.digest = Some(digest_of(&sections("aa")));
        c.plan = Some(Plan { online: true, poll_ms: 1, backoffs: vec![1], ..Default::default() });
        c.started = Some(Instant::now());
    });
    env.with_agent(0, |a| a.gen = 1);
    stage::serve_job(&d, 1);
    assert_eq!(state(&d.s).cycle().phase, Phase::Serving);
    assert!(fired_cond("the restore marker is consumed on the first normal boot", true));
    assert!(fired_cond("a restored cluster serves", true));
    let starts: Vec<Value> = env.agent(0).posts.iter().filter(|p| p.0 == "/start").map(|p| p.1.clone()).collect();
    assert_eq!(starts.last().unwrap()["mode"], "normal");
    assert_eq!(starts.last().unwrap()["clusterId"], "restore-g1");
    stage::verify_job(&d, 1);
    for m in ["restored ledgers match the source at the cut", "restored audit matches the source at the cut", "restored metadata schemas match the source at the cut", "the restored store check finds no error", "a restore round trip verifies with a non-empty delta", "a restore cycle completes within its wall budget"] {
        assert!(fired_cond(m, true), "{m}");
    }
    assert_eq!(state(&d.s).cycle().phase, Phase::Idle);
    // The restored digest was pinned to restore-0.
    assert!(env.digest_calls.lock().unwrap().iter().all(|c| c["node"] == RESTORE_NODE));

    // A different restored digest fails the section's property.
    testkit::reset();
    env.set_digest(RESTORE_NODE, sections("bb"));
    put_cycle(&d, |c| {
        c.gen = 1;
        c.phase = Phase::Serving;
        c.base = Some(Base { f: 1, audit: 1, applied: 5 });
        c.cut = Some(Cut { s: 4, a: 2 });
        c.digest = Some(digest_of(&sections("aa")));
        c.plan = Some(Plan { online: true, poll_ms: 1, backoffs: vec![1], ..Default::default() });
        c.started = Some(Instant::now());
    });
    stage::verify_job(&d, 1);
    assert!(fired_cond("restored transactions match the source at the cut", false));
    assert!(fired_cond("restored ledgers match the source at the cut", true));
}

#[test]
fn admission_probes_judge_definite_outcomes() {
    let (d, ft, _env) = setup();
    let seed = Seed {
        ledger: "r-restore-test-1".into(),
        k1: "k1".into(),
        batch1: seed_batch1("r-restore-test-1", "ref1"),
        t1: 0,
        t2: 1,
        t4: 2,
        r1: "ref1".into(),
        k2: "k2".into(),
        k3: "k3".into(),
        batch3: vec![tx("r-restore-test-1", "world", "probe:a", 50, Some("ref2"))],
        t3: Some(4),
    };
    let created = |id: u64, seq: u64, ins: &str| json!({"sequence": seq.to_string(), "payload": {"apply": {"log": {"data": {"createdTransaction": {"transaction": {"id": id.to_string(), "insertedAt": ins}}}}}}});
    ft.script("bucket", "ListTransactions", vec![FakeReply::Outcome(Outcome::from_json(&json!({"ok": true, "complete": true, "items": [
        {"id": "4", "insertedAt": "2026-11-03T04:00:00.5Z"}, {"id": "3", "insertedAt": "2026-11-03T04:00:00.1Z"}]})))]);
    ft.script("bucket", "ListLedgers", vec![FakeReply::Outcome(Outcome::from_json(&json!({"ok": true, "complete": true, "items": [{"name": "a", "id": 3}, {"name": "b", "id": 7}]})))]);
    ft.script("bucket", "Apply", vec![
        ok(json!({"logs": [created(5, 41, "2026-11-03T04:00:01Z")]})),          // first write
        ok(json!({"logs": [{}, created(0, 2, ""), created(1, 3, ""), created(2, 4, "")]})), // replay k1
        FakeReply::Outcome(Outcome::from_json(&json!({"ok": false, "code": "FailedPrecondition", "reason": "IDEMPOTENCY_KEY_CONFLICT"}))),
        ok(json!({"logs": [created(4, 9, "")]})),                               // replay k3
        FakeReply::Outcome(Outcome::from_json(&json!({"ok": false, "code": "FailedPrecondition", "reason": "IDEMPOTENCY_KEY_CONFLICT"}))),
        FakeReply::Outcome(Outcome::from_json(&json!({"ok": false, "code": "FailedPrecondition", "reason": "TRANSACTION_REFERENCE_CONFLICT"}))),
        FakeReply::Outcome(Outcome::from_json(&json!({"ok": false, "code": "FailedPrecondition", "reason": "TRANSACTION_ALREADY_REVERTED"}))),
        ok(json!({"logs": []})),                                                // revert T4
        FakeReply::Outcome(Outcome::from_json(&json!({"ok": false, "code": "FailedPrecondition", "reason": "TRANSACTION_ALREADY_REVERTED"}))),
        FakeReply::Outcome(Outcome::from_json(&json!({"ok": false, "code": "FailedPrecondition", "reason": "INSUFFICIENT_FUNDS"}))),
        ok(json!({"logs": []})),                                                // spend 150
        ok(json!({"logs": []})),                                                // create ledger
    ]);
    ft.script("bucket", "GetLedger", vec![ok(json!({"ledger": {"name": "x", "id": 8}}))]);
    let cx = Cx::new(&d, 0, 1);
    let res = probes::run(&cx, RESTORE_NODE, &seed, 40);
    assert_eq!(res.len(), 12, "{res:?}");
    assert!(res.iter().all(|r| r.ok), "{res:?}");
    assert!(fired_cond("post-restore admission probes behave as predicted", true));
    let applies = ft.calls_of("bucket", "Apply");
    assert!(applies.iter().all(|c| c["node"] == RESTORE_NODE));
    // The overdraft is the balance + 1 (100 + the delta's 50).
    assert_eq!(applies[9]["request"]["unsigned"]["requests"][0]["apply"]["action"]["createTransaction"]["postings"][0]["source"], "probe:a");

    // A wrong next id is a finding.
    testkit::reset();
    ft.script("bucket", "ListTransactions", vec![FakeReply::Outcome(Outcome::from_json(&json!({"ok": true, "complete": true, "items": [{"id": "4", "insertedAt": "2026-11-03T04:00:00.5Z"}]})))]);
    ft.script("bucket", "Apply", vec![ok(json!({"logs": [created(9, 41, "2026-11-03T04:00:01Z")]}))]);
    let res = probes::run(&cx, RESTORE_NODE, &seed, 40);
    assert!(!res.iter().find(|r| r.name == "next ids and clock").unwrap().ok);
    assert!(fired_cond("post-restore admission probes behave as predicted", false));
}

#[test]
fn seed_and_base_take_the_backup_into_the_cycle_namespace() {
    let (d, ft, env) = setup();
    let created = |id: u64| json!({"payload": {"apply": {"log": {"data": {"createdTransaction": {"transaction": {"id": id.to_string()}}}}}}});
    ft.script("bucket", "Apply", vec![ok(json!({"logs": [{}, created(0), created(1), created(2)]})), ok(json!({"logs": [{}]}))]);
    ft.script("cluster", "Backup", vec![err("FailedPrecondition", "x: backup: destination already has a running job"), ok(json!({"lastLogSequence": "30", "lastAuditSequence": "9", "lastAppliedIndex": "77", "totalFiles": 4}))]);
    env.with_agent(0, |a| a.gen = 6);
    testkit::run_rule(&d, restore_base);
    testkit::wait_until(Duration::from_secs(5), || !state(&d.s).cycle().job_running);
    let c = state(&d.s).cycle().clone();
    assert_eq!(c.phase, Phase::BaseTaken);
    assert_eq!(c.gen, 7, "gen continues from the agent's");
    assert_eq!(c.base.as_ref().unwrap().f, 30);
    assert_eq!(c.base.as_ref().unwrap().applied, 77);
    let seed = c.seed.unwrap();
    assert_eq!((seed.t1, seed.t2, seed.t4), (0, 1, 2));
    assert!(seed.ledger.starts_with("r-restore-test-"));
    let backups = ft.calls_of("cluster", "Backup");
    assert_eq!(backups.len(), 2, "busy is retried");
    assert!(backups.iter().all(|b| b["request"]["bucketId"] == NS));
    assert_eq!(env.agent(0).gen, 7);
    assert!(fired_cond("restore base backup completes", true));
    // The delta seed is written by restore_step in BaseTaken.
    ft.script("bucket", "Apply", vec![ok(json!({"logs": [created(4)]}))]);
    testkit::run_rule(&d, restore_step);
    assert_eq!(state(&d.s).cycle().seed.as_ref().unwrap().t3, Some(4));
}
