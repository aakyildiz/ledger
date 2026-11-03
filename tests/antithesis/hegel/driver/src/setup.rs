//! Setup (DESIGN §6.4): runs once per process, before the machine, with no
//! hegel draw. Each wait prints what it waits for whenever that changes.
use crate::clients::{CallOpts, HttpClientTransport, Route};
use crate::machine::Driver;
use crate::oracle::{HttpOracleTransport, OracleError, SetupReq};
use crate::shared::{Config, Shared};
use crate::wire::{str_field, u64_field};
use crate::write::{UntrackedSend, WritePlan, WriteResult};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Cheap fixed setup work registered by families (no draws).
pub struct SetupHook {
    pub name: &'static str,
    pub run: fn(&Driver),
}

pub const LOG_PORT: u16 = 7777;
pub const OUTCOME_PORT: u16 = 7778;
pub const LIVE_TARGET: u64 = 4;

/// Prints a wait's status only when it changes.
#[derive(Default)]
pub struct Waits(HashMap<String, String>);

impl Waits {
    pub fn say(&mut self, who: &str, msg: String) {
        if self.0.get(who) != Some(&msg) {
            println!("setup: {who}: {msg}");
            self.0.insert(who.to_string(), msg);
        }
    }
}

/// The whole setup. Returns the live `Shared`.
pub fn run() -> Arc<Shared> {
    let started = Instant::now();
    antithesis_sdk::antithesis_init();
    let cfg = Config::from_env();
    println!("setup: run {} clients {:?} nodes {:?} oracle {}", cfg.run, cfg.client_urls, cfg.nodes, cfg.oracle_url);
    let s = Shared::new(cfg.clone(), Arc::new(HttpClientTransport::new(cfg.client_urls.clone())), Box::new(HttpOracleTransport::new(&cfg.oracle_url)));
    crate::logs::start(Arc::downgrade(&s), LOG_PORT);
    s.clients.start_receiver(OUTCOME_PORT);
    let d = Driver { s: s.clone() };
    let mut w = Waits::default();

    wait_until_up(&d, &mut w);
    wait_for_oracle(&d, &mut w);
    s.start_threads();
    admin_setup(&d, &mut w);
    for h in crate::rules::setup_hooks() {
        println!("setup: hook {}", h.name);
        (h.run)(&d);
    }
    wait_until_serving(&d, &mut w);
    println!("setup: complete after {} ms", started.elapsed().as_millis());
    antithesis_sdk::lifecycle::setup_complete(&json!({"run": s.run, "nodes": s.nodes, "clients": s.cfg.client_urls}));

    let file = s.cfg.workload_active_file.clone();
    w.say("workload", format!("waiting for {file}"));
    while !std::path::Path::new(&file).exists() {
        std::thread::sleep(Duration::from_millis(100));
    }
    s.active.store(true, Ordering::SeqCst);
    *s.last_quiet_end.lock().unwrap_or_else(|e| e.into_inner()) = Instant::now();
    *s.last_exclusive_end.lock().unwrap_or_else(|e| e.into_inner()) = Instant::now();
    if std::env::var_os("ANTITHESIS_OUTPUT_DIR").is_some() {
        let present = std::env::var_os("ANTITHESIS_STOP_FAULTS").is_some_and(|v| !v.is_empty());
        check_always!(present, "the fault pause command exists in Antithesis", json!({}));
    }
    check_reachable!("the workload starts", json!({"run": s.run}));
    println!("setup: workload active; total setup time {} ms", started.elapsed().as_millis());
    s
}

/// Every client answers `/health`; every node answers, and the cluster has
/// a leader and every voter.
pub fn wait_until_up(d: &Driver, w: &mut Waits) {
    let s = &d.s;
    for c in 0..s.clients.len() {
        loop {
            match s.clients.health(c) {
                Ok(_) => break,
                Err(e) => w.say(&format!("client {c}"), format!("waiting for /health: {e}")),
            }
            std::thread::sleep(Duration::from_secs(1));
        }
        w.say(&format!("client {c}"), "up".into());
    }
    loop {
        let mut ready = true;
        for node in &s.nodes {
            let (_, out) = d.call(&Route::node(0, node), "cluster", "GetClusterState", json!({}), &CallOpts::default().timeout(Duration::from_secs(2)));
            match out.response.as_ref().filter(|_| out.is_ok()) {
                Some(cs) => {
                    let voters = cs.get("nodes").and_then(Value::as_array).map_or(0, |ns| ns.iter().filter(|n| str_field(n, "suffrage") == "Voter").count());
                    if u64_field(cs, "leader") == 0 || voters < s.nodes.len() {
                        w.say(node, format!("waiting: leader={} voters={voters}", u64_field(cs, "leader")));
                        ready = false;
                    } else {
                        w.say(node, "up".into());
                    }
                }
                None => {
                    w.say(node, format!("waiting: {}", out.summary()));
                    ready = false;
                }
            }
        }
        if ready {
            return;
        }
        std::thread::sleep(Duration::from_secs(1));
    }
}

/// The oracle answers `/health` (records its boot id).
pub fn wait_for_oracle(d: &Driver, w: &mut Waits) {
    loop {
        match d.s.oracle.health() {
            Ok(h) => {
                w.say("oracle", format!("up (boot {})", h.boot_id));
                return;
            }
            Err(e) => w.say("oracle", format!("waiting for /health: {:?} {}", e.kind, e.message)),
        }
        std::thread::sleep(Duration::from_secs(1));
    }
}

/// The fixed shapes of the four tracked ledgers (no draws).
pub fn tracked_ledger_requests(run: &str) -> Vec<Value> {
    vec![
        json!({"createLedger": {"name": format!("o-{run}-l1")}}),
        json!({"createLedger": {
            "name": format!("o-{run}-l2"),
            "accountTypes": {
                "t-0": {"name": "t-0", "pattern": "t-0:{id}"},
                "t-1": {"name": "t-1", "pattern": "t-1:{id}", "persistence": "ACCOUNT_TYPE_EPHEMERAL"},
                "t-2": {"name": "t-2", "pattern": "t-2:{id}", "persistence": "ACCOUNT_TYPE_TRANSIENT"},
            },
            "defaultEnforcementMode": "CHART_ENFORCEMENT_AUDIT",
        }}),
        json!({"createLedger": {
            "name": format!("o-{run}-l3"),
            "initialSchema": [
                {"key": "k0", "type": "METADATA_TYPE_STRING"},
                {"targetType": "TARGET_TYPE_TRANSACTION", "key": "k1", "type": "METADATA_TYPE_INT64"},
            ],
        }}),
        json!({"createLedger": {"name": format!("o-{run}-l4"), "metadata": {"region": {"stringValue": "eu"}, "tier": {"intValue": "1"}}}}),
    ]
}

/// `default` (untracked), `/setup`, and the tracked ledgers through the
/// write protocol, each retried until definitive.
pub fn admin_setup(d: &Driver, w: &mut Waits) {
    let s = &d.s;
    // `default`: a port of MD/first_default_ledger, untracked.
    let key = format!("r-{}-default", s.run);
    loop {
        let mut send = UntrackedSend::new(vec![json!({"createLedger": {"name": "default"}})], Some(key.clone()));
        send.schedule = (0..s.clients.len().max(1)).map(Route::lib).collect();
        let r = d.send_untracked(send);
        if r.outcome.is_ok() || r.outcome.reason == "LEDGER_ALREADY_EXISTS" {
            w.say("ledger default", "created".into());
            break;
        }
        w.say("ledger default", format!("creating: {}", r.outcome.summary()));
        std::thread::sleep(Duration::from_secs(1));
    }
    let req = SetupReq { run_id: s.run.clone(), tracked_prefix: s.tracked_prefix(), query_checkpoint_limit: s.cfg.query_checkpoint_limit.clone(), live_target: LIVE_TARGET };
    loop {
        match s.oracle.setup(&req) {
            Ok(_) => break,
            Err(e) => w.say("oracle /setup", format!("{:?}: {} {}", e.kind, e.message, e.details)),
        }
        std::thread::sleep(Duration::from_secs(1));
    }
    w.say("oracle /setup", "done".into());
    let reqs = tracked_ledger_requests(&s.run);
    s.next_ledger.store(reqs.len() as u64 + 1, Ordering::SeqCst);
    for (i, r) in reqs.into_iter().enumerate() {
        let who = format!("tracked ledger l{}", i + 1);
        loop {
            let Some(mut grant) = s.slots.try_acquire_up_to(1) else {
                w.say(&who, "waiting for a write slot".into());
                std::thread::sleep(Duration::from_millis(200));
                continue;
            };
            let unit = grant.take_one().expect("one unit");
            let mut plan = WritePlan::new("setup", vec![r.clone()]);
            plan.schedule = (0..s.clients.len().max(1)).map(Route::lib).collect();
            match d.dispatch(plan, unit) {
                WriteResult::Definitive { outcome, .. } => {
                    w.say(&who, format!("definitive: {}", outcome.summary()));
                    break;
                }
                WriteResult::HandedOff { wid, .. } => {
                    w.say(&who, format!("{wid} handed off; waiting for the resolvers"));
                    while !s.resolvers.is_empty() {
                        std::thread::sleep(Duration::from_millis(200));
                    }
                    break;
                }
                WriteResult::Refused(e) => {
                    w.say(&who, format!("refused: {:?} {}", e.kind, e.message));
                    if e.kind == OracleError::Degraded {
                        break;
                    }
                    std::thread::sleep(Duration::from_secs(1));
                }
            }
        }
    }
}

/// Every client reads every tracked ledger.
pub fn wait_until_serving(d: &Driver, w: &mut Waits) {
    let s = &d.s;
    for c in 0..s.clients.len() {
        for i in 1..=LIVE_TARGET {
            let name = format!("o-{}-l{i}", s.run);
            loop {
                let (_, out) = d.call(&Route::lib(c), "bucket", "GetLedger", json!({"ledger": name}), &CallOpts::default());
                if out.is_ok() {
                    break;
                }
                w.say(&format!("client {c} ledger {name}"), format!("waiting: {}", out.summary()));
                std::thread::sleep(Duration::from_secs(1));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testkit;

    #[test]
    fn admin_setup_creates_default_untracked_and_tracked_ledgers_through_the_oracle() {
        let (d, ft, fo) = testkit::driver();
        let mut w = Waits::default();
        admin_setup(&d, &mut w);
        let applies = ft.calls_of("bucket", "Apply");
        assert_eq!(applies[0]["request"]["unsigned"]["idempotencyKey"], format!("r-{}-default", d.s.run));
        assert_eq!(applies[0]["request"]["unsigned"]["requests"][0], json!({"createLedger": {"name": "default"}}));
        let begins = fo.calls("/write/begin");
        assert_eq!(begins.len(), 4);
        for (i, b) in begins.iter().enumerate() {
            assert_eq!(b["requests"][0]["createLedger"]["name"], format!("o-{}-l{}", d.s.run, i + 1));
        }
        assert_eq!(begins[1]["requests"][0]["createLedger"]["defaultEnforcementMode"], "CHART_ENFORCEMENT_AUDIT");
        let setup = fo.calls("/setup");
        assert_eq!(setup[0]["trackedPrefix"], format!("o-{}-", d.s.run));
        assert_eq!(setup[0]["liveTarget"], 4);
        assert_eq!(fo.calls("/write/end").len(), 4);
        assert_eq!(d.s.next_ledger_name(), format!("o-{}-l5", d.s.run));
        wait_until_serving(&d, &mut w);
    }
}
