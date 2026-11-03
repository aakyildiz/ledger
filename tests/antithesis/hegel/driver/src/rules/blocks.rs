//! Scenario blocks (task 17; main-drivers §3.35, DESIGN §3 "Scenario
//! ledgers", §7 C7, §9.1). The client runs `pkg/scenario`'s 26 business
//! blocks on the fixed scenario ledgers (`marketplace`, `gaming`, `lending`,
//! `billing`, `treasury`, `dept-*`, …), which the oracle never sees. Every
//! choice a block makes comes from a list of 16 raw `u64` values drawn here
//! with hegel (critic §3.7: no seed into a PRNG); the client maps them with
//! `scenario.RandFunc`'s legacy one-draw modulo.
//!
//! Classification ports `W/internal/block/block.go:122-198`; the per-block
//! property pair (`block <name> succeeded` must-hit Reachable, `block <name>
//! failed` Unreachable) is produced by `scenario_blocks!` with one literal
//! call site per message.
use crate::clients::TransportError;
use crate::draw;
use crate::machine::{rule, Driver};
use crate::setup::SetupHook;
use crate::wire::{apply_logs, created_transaction, parse_biguint_full};
use hegel::generators as gs;
use hegel::stateful::ConcurrentRule;
use hegel::TestCase;
use serde_json::{json, Value};
use std::time::{Duration, Instant};

/// The six setup groups, by scenario prefix (the client's `blockGroups` keys).
pub const GROUPS: [&str; 6] = ["marketplace", "gaming", "lending", "subscription", "multicurrency", "payroll"];

/// Raw values pre-drawn per block run (the largest block uses 5).
pub const VALUES_PER_RUN: usize = 16;

/// The client-side deadline of one `/block` call, so the rule stays under 10 s.
pub const BLOCK_TIMEOUT: Duration = Duration::from_millis(8000);

/// Extra HTTP wait beyond the client-side deadline.
const HTTP_SLACK: Duration = Duration::from_millis(1500);

/// The whole setup hook's budget (DESIGN §6.4: setup ≤ 3 minutes).
const SETUP_BUDGET: Duration = Duration::from_secs(120);

/// Declares the 26 blocks: the name list and one literal `check_reachable!`
/// / `check_unreachable!` call site per block (DESIGN §9.1), so the Antithesis
/// catalog sees 52 literal messages. `blocks_table_is_consistent` checks that
/// each pair is exactly `block <name> succeeded` / `block <name> failed`.
macro_rules! scenario_blocks {
    ($( $name:literal => $ok:literal, $failed:literal; )*) => {
        /// `(name, succeeded literal, failed literal)` of every registered block.
        pub const BLOCKS: &[(&str, &str, &str)] = &[$(($name, $ok, $failed)),*];

        /// Asserts `block <name> succeeded`; false for an unknown name.
        fn report_succeeded(name: &str, details: &Value) -> bool {
            match name {
                $($name => {
                    check_reachable!($ok, details);
                    true
                })*
                _ => false,
            }
        }

        /// Asserts `block <name> failed`; false for an unknown name.
        fn report_failed(name: &str, details: &Value) -> bool {
            match name {
                $($name => {
                    check_unreachable!($failed, details);
                    true
                })*
                _ => false,
            }
        }
    };
}

scenario_blocks! {
    "marketplace/deposit" => "block marketplace/deposit succeeded", "block marketplace/deposit failed";
    "marketplace/purchase" => "block marketplace/purchase succeeded", "block marketplace/purchase failed";
    "marketplace/revert" => "block marketplace/revert succeeded", "block marketplace/revert failed";
    "marketplace/payout" => "block marketplace/payout succeeded", "block marketplace/payout failed";
    "marketplace/metadata" => "block marketplace/metadata succeeded", "block marketplace/metadata failed";
    "gaming/topup" => "block gaming/topup succeeded", "block gaming/topup failed";
    "gaming/buy_item" => "block gaming/buy_item succeeded", "block gaming/buy_item failed";
    "gaming/p2p_trade" => "block gaming/p2p_trade succeeded", "block gaming/p2p_trade failed";
    "gaming/promotion" => "block gaming/promotion succeeded", "block gaming/promotion failed";
    "gaming/clawback" => "block gaming/clawback succeeded", "block gaming/clawback failed";
    "lending/fund_pool" => "block lending/fund_pool succeeded", "block lending/fund_pool failed";
    "lending/disburse" => "block lending/disburse succeeded", "block lending/disburse failed";
    "lending/repay" => "block lending/repay succeeded", "block lending/repay failed";
    "lending/provision" => "block lending/provision succeeded", "block lending/provision failed";
    "lending/write_off" => "block lending/write_off succeeded", "block lending/write_off failed";
    "multicurrency/fund" => "block multicurrency/fund succeeded", "block multicurrency/fund failed";
    "multicurrency/fx" => "block multicurrency/fx succeeded", "block multicurrency/fx failed";
    "multicurrency/vendor_pay" => "block multicurrency/vendor_pay succeeded", "block multicurrency/vendor_pay failed";
    "payroll/fund_clearing" => "block payroll/fund_clearing succeeded", "block payroll/fund_clearing failed";
    "payroll/distribute" => "block payroll/distribute succeeded", "block payroll/distribute failed";
    "payroll/pay_salary" => "block payroll/pay_salary succeeded", "block payroll/pay_salary failed";
    "payroll/cost_alloc" => "block payroll/cost_alloc succeeded", "block payroll/cost_alloc failed";
    "subscription/fund" => "block subscription/fund succeeded", "block subscription/fund failed";
    "subscription/charge" => "block subscription/charge succeeded", "block subscription/charge failed";
    "subscription/recognize" => "block subscription/recognize succeeded", "block subscription/recognize failed";
    "subscription/credit" => "block subscription/credit succeeded", "block subscription/credit failed";
}

pub fn rules() -> Vec<ConcurrentRule<Driver>> {
    vec![rule("block_run", 1.5, block_run)]
}

pub fn setup_hooks() -> Vec<SetupHook> {
    vec![SetupHook { name: "blocks_setup", run: blocks_setup }]
}

// ---------------------------------------------------------------- classification

/// How a `/block` answer classifies (`W/internal/block/block.go:136-157`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BlockClass {
    Succeeded,
    /// `ErrSkip`: the block's precondition was not met.
    Skipped,
    /// Unavailable, and (loosened, the harness imposes a deadline)
    /// DeadlineExceeded, Canceled and client-local errors.
    Transient,
    /// FailedPrecondition: idempotent retries, already reverted, overdraft races.
    Expected,
    Failed,
}

pub fn classify(resp: &Value) -> BlockClass {
    let b = |k: &str| resp.get(k).and_then(Value::as_bool).unwrap_or(false);
    let code = resp.get("code").and_then(Value::as_str).unwrap_or("");
    if b("ok") {
        BlockClass::Succeeded
    } else if b("skipped") {
        BlockClass::Skipped
    } else if b("local") || matches!(code, "Unavailable" | "DeadlineExceeded" | "Canceled") {
        BlockClass::Transient
    } else if code == "FailedPrecondition" {
        BlockClass::Expected
    } else {
        BlockClass::Failed
    }
}

/// The first malformed post-commit volume entry of a block's Apply response,
/// if any (port of `CheckPostCommitVolumes`, `W/internal/checks.go:61-86`:
/// every entry carries input and output and both parse). Every created
/// transaction of the response is checked.
pub fn malformed_pcv(response: &Value) -> Option<Value> {
    for log in apply_logs(response) {
        let Some(tx) = created_transaction(&log) else { continue };
        let Some(by_account) = tx.pointer("/postCommitVolumes/volumesByAccount") else { continue };
        let Some(by_account) = by_account.as_object() else {
            return Some(json!({"why": "volumesByAccount is not an object", "pcv": by_account}));
        };
        for (account, assets) in by_account {
            let entries = assets.get("volumes").and_then(Value::as_array).cloned().unwrap_or_default();
            for e in entries {
                let vols = e.get("volumes");
                let field_ok = |k: &str| vols.and_then(|v| v.get(k)).is_some_and(|f| parse_biguint_full(f).is_some());
                if !(field_ok("input") && field_ok("output")) {
                    return Some(json!({"account": account, "asset": e.get("asset"), "color": e.get("color"), "entry": e}));
                }
            }
        }
    }
    None
}

/// Asserts what one `/block` answer shows. Returns its class.
pub fn report(name: &str, resp: &Value) -> BlockClass {
    let exhausted = resp.get("exhausted").and_then(Value::as_bool).unwrap_or(false);
    check_sometimes!(!exhausted, "a block runs within its pre-drawn values", json!({"block": name, "used": resp.get("used")}));
    let class = classify(resp);
    let details = json!({
        "block": name,
        "code": resp.get("code"),
        "reason": resp.get("reason"),
        "message": resp.get("message"),
    });
    match class {
        BlockClass::Succeeded => {
            report_succeeded(name, &details);
            let bad = resp.get("response").and_then(malformed_pcv);
            check_always!(bad.is_none(), "a block's post-commit volumes are well formed", json!({"block": name, "entry": bad}));
        }
        BlockClass::Skipped | BlockClass::Transient => {}
        BlockClass::Expected => println!("driver: block {name} precondition failed (expected): {}", details["message"]),
        BlockClass::Failed => {
            println!("driver: block {name} failed: {details}");
            report_failed(name, &details);
        }
    }
    class
}

// ---------------------------------------------------------------- rule

/// `block_run` (weight 1.5): one block, 16 pre-drawn values, one client.
pub fn block_run(d: &Driver, tc: TestCase) {
    d.guarded("block_run", &tc, |d, tc| {
        let n = d.s.clients.len();
        if n == 0 {
            return None;
        }
        // Draws (all before the call).
        let i = draw::weighted(tc, &vec![1.0; BLOCKS.len()])?;
        let values: Vec<u64> = (0..VALUES_PER_RUN).map(|_| tc.draw(gs::integers::<u64>())).collect();
        let client = draw::pick(tc, n);
        let name = BLOCKS[i].0;
        // Exec.
        let resp = post_block(d, client, name, &values)?;
        report(name, &resp);
        Some(())
    });
}

/// The `/block` request body.
pub fn block_body(op: u64, name: &str, values: &[u64]) -> Value {
    json!({
        "opId": op,
        "name": name,
        "values": values.iter().map(u64::to_string).collect::<Vec<_>>(),
        "timeoutMs": BLOCK_TIMEOUT.as_millis() as u64,
    })
}

/// Sends one `/block`. `None` when no answer came (transient).
fn post_block(d: &Driver, client: usize, name: &str, values: &[u64]) -> Option<Value> {
    let op = d.s.clients.next_op_id();
    let body = block_body(op, name, values);
    match d.s.clients.transport.post(client, "/block", &body, BLOCK_TIMEOUT + HTTP_SLACK) {
        Ok(v) => {
            d.s.clients.observe_boot(client, v.get("boot").and_then(Value::as_str).unwrap_or(""));
            Some(v)
        }
        Err(TransportError::Status(code, text)) => {
            check_unreachable!("the client program accepts every block request", json!({"block": name, "status": code, "body": text}));
            None
        }
        Err(_) => None,
    }
}

// ---------------------------------------------------------------- setup

/// `blocks_setup` (no draws): `/block-setup` for the six groups, through
/// the clients in turn, retried until ok. A non-transient failure, or no
/// success within the setup budget, fails `should be able to setup
/// scenario` (`W/internal/block/block.go:106`).
pub fn blocks_setup(d: &Driver) {
    let n = d.s.clients.len();
    if n == 0 {
        return;
    }
    let deadline = Instant::now() + SETUP_BUDGET;
    let mut attempt = 0usize;
    for group in GROUPS {
        let mut last = Value::Null;
        let ok = loop {
            let client = attempt % n;
            attempt += 1;
            let op = d.s.clients.next_op_id();
            let body = json!({"opId": op, "group": group, "timeoutMs": BLOCK_TIMEOUT.as_millis() as u64});
            match d.s.clients.transport.post(client, "/block-setup", &body, BLOCK_TIMEOUT + HTTP_SLACK) {
                Ok(v) => {
                    d.s.clients.observe_boot(client, v.get("boot").and_then(Value::as_str).unwrap_or(""));
                    if v.get("ok").and_then(Value::as_bool).unwrap_or(false) {
                        break true;
                    }
                    last = v.clone();
                    if classify(&v) != BlockClass::Transient {
                        break false;
                    }
                }
                Err(TransportError::Status(code, text)) => {
                    last = json!({"status": code, "body": text});
                    break false;
                }
                Err(e) => last = json!({"transport": format!("{e:?}")}),
            }
            if Instant::now() >= deadline {
                break false;
            }
            println!("setup: block group {group}: retrying after {last}");
            std::thread::sleep(d.s.timing().poll.min(Duration::from_secs(1)));
        };
        println!("setup: block group {group}: {}", if ok { "ok".to_string() } else { format!("failed: {last}") });
        check_always!(ok, "should be able to setup scenario", json!({"group": group, "error": last}));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clients::ClientTransport;
    use crate::shared::Shared;
    use crate::testkit::{self, SharedFakeOracle};
    use std::collections::VecDeque;
    use std::sync::{Arc, Mutex};

    /// The 26 registered block names (HEGEL-INVENTORY "Registered block
    /// checklist", `pkg/scenario/*.go`; kept in sync with the client's
    /// `ext_block_test.go`).
    const EXPECTED: [&str; 26] = [
        "gaming/buy_item", "gaming/clawback", "gaming/p2p_trade", "gaming/promotion", "gaming/topup",
        "lending/disburse", "lending/fund_pool", "lending/provision", "lending/repay", "lending/write_off",
        "marketplace/deposit", "marketplace/metadata", "marketplace/payout", "marketplace/purchase", "marketplace/revert",
        "multicurrency/fund", "multicurrency/fx", "multicurrency/vendor_pay",
        "payroll/cost_alloc", "payroll/distribute", "payroll/fund_clearing", "payroll/pay_salary",
        "subscription/charge", "subscription/credit", "subscription/fund", "subscription/recognize",
    ];

    #[test]
    fn blocks_table_is_consistent() {
        let mut names: Vec<&str> = BLOCKS.iter().map(|b| b.0).collect();
        names.sort_unstable();
        assert_eq!(names, EXPECTED.to_vec());
        for (name, ok, failed) in BLOCKS {
            assert_eq!(*ok, format!("block {name} succeeded"));
            assert_eq!(*failed, format!("block {name} failed"));
        }
        let mut groups: Vec<&str> = EXPECTED.iter().map(|n| n.split('/').next().unwrap()).collect();
        groups.dedup();
        let mut want = GROUPS.to_vec();
        want.sort_unstable();
        assert_eq!(groups, want);
    }

    #[test]
    fn report_functions_fire_their_literal() {
        testkit::reset();
        for (name, ok, failed) in BLOCKS {
            assert!(report_succeeded(name, &json!({})));
            assert!(report_failed(name, &json!({})));
            let a = testkit::assertions();
            let n = a.len();
            assert_eq!((a[n - 2].kind, a[n - 2].message), ("Reachable", *ok));
            assert_eq!((a[n - 1].kind, a[n - 1].message), ("Unreachable", *failed));
        }
        assert!(!report_succeeded("nope/nope", &json!({})));
        assert!(!report_failed("nope/nope", &json!({})));
    }

    fn pcv_response(entry: Value) -> Value {
        json!({"logs": [{"sequence": "9", "payload": {"apply": {"log": {"data": {"createdTransaction": {"transaction": {
            "id": "1",
            "postCommitVolumes": {"volumesByAccount": {"customer:1": {"volumes": [entry]}}},
        }}}}}}}]})
    }

    #[test]
    fn classification_table() {
        let c = |v: Value| classify(&v);
        assert_eq!(c(json!({"ok": true})), BlockClass::Succeeded);
        assert_eq!(c(json!({"ok": false, "skipped": true})), BlockClass::Skipped);
        for code in ["Unavailable", "DeadlineExceeded", "Canceled"] {
            assert_eq!(c(json!({"ok": false, "code": code})), BlockClass::Transient, "{code}");
        }
        assert_eq!(c(json!({"ok": false, "code": "ResourceExhausted", "local": true})), BlockClass::Transient);
        assert_eq!(c(json!({"ok": false, "code": "FailedPrecondition", "reason": "INSUFFICIENT_FUNDS"})), BlockClass::Expected);
        for code in ["Internal", "Unknown", "Aborted", "InvalidArgument", "NotFound", "AlreadyExists", ""] {
            assert_eq!(c(json!({"ok": false, "code": code})), BlockClass::Failed, "{code}");
        }
    }

    #[test]
    fn post_commit_volume_shape() {
        let good = json!({"asset": "USD/2", "volumes": {"input": {"magnitude": "Cg=="}, "output": {}}});
        assert!(malformed_pcv(&pcv_response(good)).is_none());
        let missing = json!({"asset": "USD/2", "volumes": {"input": {"magnitude": "Cg=="}}});
        assert_eq!(malformed_pcv(&pcv_response(missing)).unwrap()["account"], "customer:1");
        let bad = json!({"asset": "USD/2", "volumes": {"input": {"magnitude": "!!"}, "output": {}}});
        assert!(malformed_pcv(&pcv_response(bad)).is_some());
        assert!(malformed_pcv(&pcv_response(json!({"asset": "USD/2"}))).is_some());
        // Responses without a created transaction carry nothing to check.
        assert!(malformed_pcv(&json!({"logs": [{"payload": {"apply": {"log": {"data": {"savedMetadata": {}}}}}}]})).is_none());
        assert!(malformed_pcv(&json!({})).is_none());
    }

    /// A client transport that answers `/block` and `/block-setup` from scripts.
    #[derive(Default)]
    struct BlockTransport {
        replies: Mutex<VecDeque<Result<Value, TransportError>>>,
        default: Mutex<Option<Value>>,
        calls: Mutex<Vec<(usize, String, Value)>>,
    }

    impl ClientTransport for BlockTransport {
        fn count(&self) -> usize {
            3
        }
        fn post(&self, client: usize, path: &str, body: &Value, _timeout: Duration) -> Result<Value, TransportError> {
            self.calls.lock().unwrap().push((client, path.to_string(), body.clone()));
            if let Some(r) = self.replies.lock().unwrap().pop_front() {
                return r;
            }
            Ok(self.default.lock().unwrap().clone().unwrap_or_else(|| json!({"boot": "b", "ok": true})))
        }
        fn get(&self, _client: usize, _path: &str, _timeout: Duration) -> Result<Value, TransportError> {
            Ok(json!({"boot": "b"}))
        }
    }

    fn block_driver() -> (Driver, Arc<BlockTransport>) {
        let (base, _, fo) = testkit::driver();
        let t = Arc::new(BlockTransport::default());
        let s = Shared::new(base.s.cfg.clone(), t.clone(), Box::new(SharedFakeOracle(fo)));
        s.active.store(true, std::sync::atomic::Ordering::SeqCst);
        (Driver { s }, t)
    }

    fn last_block_call(t: &BlockTransport) -> Value {
        t.calls.lock().unwrap().iter().rev().find(|(_, p, _)| p == "/block").map(|c| c.2.clone()).expect("a /block call")
    }

    fn fired(kind: &str, message: &str) -> Vec<bool> {
        testkit::assertions().iter().filter(|a| a.kind == kind && a.message == message).map(|a| a.cond).collect()
    }

    fn literals_of(name: &str) -> (&'static str, &'static str) {
        let b = BLOCKS.iter().find(|b| b.0 == name).expect("a registered block");
        (b.1, b.2)
    }

    #[test]
    fn block_run_sends_pre_drawn_values_and_reports_success() {
        let (d, t) = block_driver();
        let good = json!({"asset": "USD/2", "volumes": {"input": {"magnitude": "Cg=="}, "output": {}}});
        *t.default.lock().unwrap() = Some(json!({"boot": "b", "ok": true, "skipped": false, "exhausted": false, "used": 2, "response": pcv_response(good)}));
        testkit::reset();
        testkit::run_rule(&d, block_run);
        let body = last_block_call(&t);
        let name = body["name"].as_str().unwrap();
        assert!(EXPECTED.contains(&name));
        assert_eq!(body["timeoutMs"], 8000);
        let values = body["values"].as_array().unwrap();
        assert_eq!(values.len(), VALUES_PER_RUN);
        assert!(values.iter().all(|v| v.as_str().unwrap().parse::<u64>().is_ok()));
        let (ok, failed) = literals_of(name);
        assert_eq!(fired("Reachable", ok), vec![true]);
        assert!(fired("Unreachable", failed).is_empty());
        assert_eq!(fired("Always", "a block's post-commit volumes are well formed"), vec![true]);
        assert_eq!(fired("Sometimes", "a block runs within its pre-drawn values"), vec![true]);
    }

    #[test]
    fn block_run_flags_malformed_volumes_and_exhaustion() {
        let (d, t) = block_driver();
        let missing = json!({"asset": "USD/2", "volumes": {"input": {}}});
        *t.default.lock().unwrap() = Some(json!({"ok": true, "exhausted": true, "used": 16, "response": pcv_response(missing)}));
        testkit::reset();
        testkit::run_rule(&d, block_run);
        assert_eq!(fired("Always", "a block's post-commit volumes are well formed"), vec![false]);
        assert_eq!(fired("Sometimes", "a block runs within its pre-drawn values"), vec![false]);
    }

    #[test]
    fn block_run_classifies_failures() {
        for (resp, fails) in [
            (json!({"ok": false, "code": "Internal", "message": "boom"}), true),
            (json!({"ok": false, "code": "Unknown", "message": "block panicked"}), true),
            (json!({"ok": false, "code": "FailedPrecondition", "reason": "INSUFFICIENT_FUNDS"}), false),
            (json!({"ok": false, "code": "DeadlineExceeded"}), false),
            (json!({"ok": false, "code": "Unavailable"}), false),
            (json!({"ok": false, "code": "ResourceExhausted", "local": true}), false),
            (json!({"ok": false, "skipped": true}), false),
        ] {
            let (d, t) = block_driver();
            *t.default.lock().unwrap() = Some(resp.clone());
            testkit::reset();
            testkit::run_rule(&d, block_run);
            let name = last_block_call(&t)["name"].as_str().unwrap().to_string();
            let (ok, failed) = literals_of(&name);
            assert!(fired("Reachable", ok).is_empty(), "{resp}");
            assert_eq!(!fired("Unreachable", failed).is_empty(), fails, "{resp}");
            assert!(fired("Always", "a block's post-commit volumes are well formed").is_empty());
        }
        // No answer: nothing but no panic either.
        let (d, t) = block_driver();
        t.replies.lock().unwrap().push_back(Err(TransportError::Timeout));
        testkit::reset();
        testkit::run_rule(&d, block_run);
        assert!(!testkit::fired("a driver rule panics"));
        assert!(testkit::assertions().iter().all(|a| !a.message.starts_with("block ")));
    }

    #[test]
    fn blocks_setup_retries_until_every_group_is_set_up() {
        let (d, t) = block_driver();
        {
            let mut r = t.replies.lock().unwrap();
            r.push_back(Err(TransportError::Timeout));
            r.push_back(Ok(json!({"ok": false, "code": "Unavailable"})));
        }
        testkit::reset();
        blocks_setup(&d);
        let calls = t.calls.lock().unwrap().clone();
        let groups: Vec<&str> = calls.iter().map(|c| c.2["group"].as_str().unwrap()).collect();
        assert_eq!(groups, vec!["marketplace", "marketplace", "marketplace", "gaming", "lending", "subscription", "multicurrency", "payroll"]);
        assert!(calls.iter().all(|c| c.1 == "/block-setup" && c.2["timeoutMs"] == 8000));
        // Clients in turn.
        assert_eq!(calls.iter().map(|c| c.0).collect::<Vec<_>>(), vec![0, 1, 2, 0, 1, 2, 0, 1]);
        assert_eq!(fired("Always", "should be able to setup scenario"), vec![true; 6]);
    }

    #[test]
    fn blocks_setup_reports_a_definitive_failure() {
        let (d, t) = block_driver();
        t.replies.lock().unwrap().push_back(Ok(json!({"ok": false, "code": "InvalidArgument", "message": "bad script"})));
        testkit::reset();
        blocks_setup(&d);
        let got = fired("Always", "should be able to setup scenario");
        assert_eq!(got.len(), 6);
        assert_eq!(got[0], false);
        assert!(got[1..].iter().all(|c| *c));
    }
}
