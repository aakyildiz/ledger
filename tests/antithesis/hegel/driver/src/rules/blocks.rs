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
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

/// The six setup groups, by scenario prefix (the client's `blockGroups` keys).
pub const GROUPS: [&str; 6] = ["marketplace", "gaming", "lending", "subscription", "multicurrency", "payroll"];

/// Raw values pre-drawn per block run (the largest block uses 5).
pub const VALUES_PER_RUN: usize = 16;

/// The client-side deadline of one `/block` call, so the rule stays under 10 s.
pub const BLOCK_TIMEOUT: Duration = Duration::from_millis(8000);

/// Extra HTTP wait beyond the client-side deadline.
const HTTP_SLACK: Duration = Duration::from_millis(1500);

/// The client-side time a whole chain (the drawn block and its chained
/// consumers) may use, so the rule stays at about 10 s.
pub const CHAIN_BUDGET: Duration = BLOCK_TIMEOUT;

/// A chained consumer runs only with at least this much of the chain budget left.
pub const CHAIN_MIN_LEFT: Duration = Duration::from_millis(2000);

/// How often a run chains consumers after its drawn block succeeds.
pub const CHAIN_PROBABILITY: f64 = 0.5;

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

/// How a `/block` answer classifies (`W/internal/block/block.go:136-157`,
/// loosened as the porting file records).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BlockClass {
    Succeeded,
    /// `ErrSkip`: the block's precondition was not met.
    Skipped,
    /// Unavailable, and (loosened) DeadlineExceeded, Canceled and
    /// client-local errors (the harness imposes a deadline), reason
    /// `EXTERNAL_SERVICE_ERROR` under any code and ResourceExhausted with a
    /// `WRITES_BLOCKED_*` reason (`W/internal/client.go` `IsTransient`, DESIGN
    /// §4.5), Unauthenticated and PermissionDenied (task 16's signature window).
    Transient,
    /// FailedPrecondition: idempotent retries, already reverted, overdraft
    /// races; and (loosened) NotFound `METADATA_NOT_FOUND`: concurrent
    /// `marketplace/metadata` blocks on one address, or a retried unkeyed
    /// delete whose first attempt committed.
    Expected,
    Failed,
}

pub fn classify(resp: &Value) -> BlockClass {
    let b = |k: &str| resp.get(k).and_then(Value::as_bool).unwrap_or(false);
    let code = resp.get("code").and_then(Value::as_str).unwrap_or("");
    let reason = resp.get("reason").and_then(Value::as_str).unwrap_or("");
    if b("ok") {
        BlockClass::Succeeded
    } else if b("skipped") {
        BlockClass::Skipped
    } else if b("local")
        || matches!(code, "Unavailable" | "DeadlineExceeded" | "Canceled" | "Unauthenticated" | "PermissionDenied")
        || reason == "EXTERNAL_SERVICE_ERROR"
        || (code == "ResourceExhausted" && reason.starts_with("WRITES_BLOCKED_"))
    {
        BlockClass::Transient
    } else if code == "FailedPrecondition" || (code == "NotFound" && reason == "METADATA_NOT_FOUND") {
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
        let Some(tx) = created_transaction(&log).filter(|t| !t.is_null()) else {
            // A created transaction without its payload (`W/internal/apply.go:28-31`).
            if let Some(ct) = log.pointer("/payload/apply/log/data/createdTransaction") {
                return Some(json!({"why": "createdTransaction carries no transaction", "createdTransaction": ct}));
            }
            continue;
        };
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
    check_always_or_unreachable!(!exhausted, "a block never runs out of its pre-drawn values", json!({"block": name, "used": resp.get("used")}));
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

/// Which groups' setup has succeeded (`GROUPS` order).
#[derive(Default)]
pub struct BlocksState {
    ready: [AtomicBool; 6],
}

impl BlocksState {
    fn index(group: &str) -> Option<usize> {
        GROUPS.iter().position(|g| *g == group)
    }
    pub fn is_ready(&self, group: &str) -> bool {
        Self::index(group).is_some_and(|i| self.ready[i].load(Ordering::SeqCst))
    }
    fn mark_ready(&self, group: &str) {
        if let Some(i) = Self::index(group) {
            self.ready[i].store(true, Ordering::SeqCst);
        }
    }
}

/// The setup group of a block name (`marketplace/deposit` → `marketplace`).
pub fn group_of(name: &str) -> &str {
    name.split('/').next().unwrap_or(name)
}

/// The consumer chained after a producer block, with values that make it
/// pick the entity the producer just funded. Every `Intn` / `Int63n` of
/// `pkg/scenario` takes exactly one value modulo its bound (the client's
/// `valueRandom`), so the mapping is exact:
///
/// - `subscription/fund` → `subscription/charge`, same values: the
///   subscriber is the first draw of both (`% 50`), and the smallest fund
///   (5000) covers the largest charge (5000).
/// - `subscription/charge` → `subscription/recognize`: the charge moved
///   funds into `revenue:deferred`, which recognize needs.
/// - `marketplace/deposit` → `marketplace/purchase`, same values: the
///   customer is the first draw of both (`% 50`); a deposit (≥ 100000) covers
///   the purchase minimum (1000).
/// - `marketplace/purchase` → `marketplace/payout`, values rotated by one:
///   the merchant is purchase's second draw and payout's first (`% 10`).
/// - `lending/fund_pool` → `lending/disburse` (no balance draw in disburse's
///   pool check; any loan amount fits a pool of ≥ 100000).
/// - `lending/disburse` → `lending/provision`, same values: the borrower is
///   the first draw of both (`% 10`).
/// - `payroll/fund_clearing` → `payroll/distribute` (fund_clearing draws
///   nothing and covers every department).
/// - `payroll/distribute` → `payroll/cost_alloc`, first value mapped to the
///   allocation that draws from the department just funded:
///   engineering (0) → 1, sales (1) → 2, operations (2) → 0.
///
/// Without chaining, each of these consumers draws its entity independently
/// of the funding block (1 in 50 for charge, 1 in 10 for payout and
/// provision), so short Antithesis timelines never saw them succeed.
pub fn chained_consumer(name: &str, values: &[u64]) -> Option<(&'static str, Vec<u64>)> {
    let mut v = values.to_vec();
    let next = match name {
        "subscription/fund" => "subscription/charge",
        "subscription/charge" => "subscription/recognize",
        "marketplace/deposit" => "marketplace/purchase",
        "marketplace/purchase" => {
            if !v.is_empty() {
                v.rotate_left(1);
            }
            "marketplace/payout"
        }
        "lending/fund_pool" => "lending/disburse",
        "lending/disburse" => "lending/provision",
        "payroll/fund_clearing" => "payroll/distribute",
        "payroll/distribute" => {
            if let Some(first) = v.first_mut() {
                *first = (*first % 3 + 1) % 3;
            }
            "payroll/cost_alloc"
        }
        _ => return None,
    };
    Some((next, v))
}

/// `block_run` (weight 1.5): one block, 16 pre-drawn values, one client.
/// While its group is not set up yet (the setup hook ran out of budget on
/// transient failures), the run makes one setup attempt instead. With
/// probability `CHAIN_PROBABILITY` a succeeded block is followed, through the
/// same client, by its chained consumers (`chained_consumer`) while the chain
/// budget lasts.
pub fn block_run(d: &Driver, tc: TestCase) {
    d.guarded("block_run", &tc, |d, tc| {
        let n = d.s.clients.len();
        if n == 0 {
            return None;
        }
        // Blocks send unsigned Applies, which every node refuses while
        // signatures are required (task 16, DESIGN §4.8).
        if d.s.signatures_required() {
            return None;
        }
        // Draws (all before the call).
        let i = draw::weighted(tc, &vec![1.0; BLOCKS.len()])?;
        let values: Vec<u64> = (0..VALUES_PER_RUN).map(|_| tc.draw(gs::integers::<u64>())).collect();
        let client = draw::pick(tc, n);
        let chain = draw::chance(tc, CHAIN_PROBABILITY);
        let name = BLOCKS[i].0;
        let group = group_of(name);
        if !d.s.ext::<BlocksState>().is_ready(group) {
            match setup_attempt(d, client, group) {
                SetupTry::Transient(last) => println!("driver: block group {group} still not set up: {last}"),
                done => conclude_setup(d, group, done),
            }
            return Some(());
        }
        // Exec.
        run_blocks(d, client, name, values, chain);
        Some(())
    });
}

/// Runs `name` through `client`, then, when `chain` is set, its chained
/// consumers through the same client while each succeeds and the chain
/// budget lasts. Returns the names of the blocks sent, in order.
pub fn run_blocks(d: &Driver, client: usize, name: &'static str, values: Vec<u64>, chain: bool) -> Vec<&'static str> {
    let deadline = Instant::now() + CHAIN_BUDGET;
    let mut sent = vec![name];
    let Some(resp) = post_block(d, client, name, &values, BLOCK_TIMEOUT) else { return sent };
    let mut class = report(name, &resp);
    let (mut name, mut values) = (name, values);
    while chain && class == BlockClass::Succeeded {
        let Some((next, next_values)) = chained_consumer(name, &values) else { break };
        let left = deadline.saturating_duration_since(Instant::now());
        if left < CHAIN_MIN_LEFT {
            break;
        }
        sent.push(next);
        let Some(resp) = post_block(d, client, next, &next_values, left) else { break };
        class = report(next, &resp);
        // A consumer that skips right after its producer committed can be a
        // concurrent drain (two blocks on one entity), so this is a
        // Sometimes, not an Always; it is evaluated only on a non-transient
        // answer.
        if class != BlockClass::Transient {
            check_sometimes!(
                class == BlockClass::Succeeded,
                "a chained consumer block succeeds after its producer commits",
                json!({"producer": name, "consumer": next, "code": resp.get("code"), "reason": resp.get("reason"), "skipped": resp.get("skipped")})
            );
        }
        (name, values) = (next, next_values);
    }
    sent
}

/// The `/block` request body.
pub fn block_body(op: u64, name: &str, values: &[u64], timeout: Duration) -> Value {
    json!({
        "opId": op,
        "name": name,
        "values": values.iter().map(u64::to_string).collect::<Vec<_>>(),
        "timeoutMs": timeout.as_millis() as u64,
    })
}

/// Sends one `/block` with a client-side deadline of `timeout`. `None` when
/// no answer came (transient).
fn post_block(d: &Driver, client: usize, name: &str, values: &[u64], timeout: Duration) -> Option<Value> {
    let op = d.s.clients.next_op_id();
    let body = block_body(op, name, values, timeout);
    match d.s.clients.transport.post(client, "/block", &body, timeout + HTTP_SLACK) {
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

/// One `/block-setup` attempt's outcome.
#[derive(Debug)]
pub enum SetupTry {
    Ok,
    /// No answer, or a transient one (`classify`); worth retrying.
    Transient(Value),
    /// A definitive failure (`should be able to setup scenario` is false).
    Definitive(Value),
}

/// Sends one `/block-setup` for `group` through `client`.
pub fn setup_attempt(d: &Driver, client: usize, group: &str) -> SetupTry {
    let op = d.s.clients.next_op_id();
    let body = json!({"opId": op, "group": group, "timeoutMs": BLOCK_TIMEOUT.as_millis() as u64});
    match d.s.clients.transport.post(client, "/block-setup", &body, BLOCK_TIMEOUT + HTTP_SLACK) {
        Ok(v) => {
            d.s.clients.observe_boot(client, v.get("boot").and_then(Value::as_str).unwrap_or(""));
            if v.get("ok").and_then(Value::as_bool).unwrap_or(false) {
                SetupTry::Ok
            } else if classify(&v) == BlockClass::Transient {
                SetupTry::Transient(v)
            } else {
                SetupTry::Definitive(v)
            }
        }
        Err(TransportError::Status(code, text)) => SetupTry::Definitive(json!({"status": code, "body": text})),
        Err(e) => SetupTry::Transient(json!({"transport": format!("{e:?}")})),
    }
}

/// Asserts a concluded setup attempt (`Ok` or `Definitive`).
fn conclude_setup(d: &Driver, group: &str, done: SetupTry) {
    let (ok, last) = match done {
        SetupTry::Ok => {
            d.s.ext::<BlocksState>().mark_ready(group);
            (true, Value::Null)
        }
        SetupTry::Definitive(v) | SetupTry::Transient(v) => (false, v),
    };
    println!("setup: block group {group}: {}", if ok { "ok".to_string() } else { format!("failed: {last}") });
    check_always!(ok, "should be able to setup scenario", json!({"group": group, "error": last}));
}

/// `blocks_setup` (no draws): `/block-setup` for the six groups, through
/// the clients in turn, retried while the answers are transient, as the old
/// loop retried Unavailable without limit (`W/internal/block/block.go:85-106`).
/// A definitive failure fails `should be able to setup scenario`. A group
/// still answered only transiently when the hook budget runs out is not
/// asserted here: `block_run` keeps retrying it, one attempt per run, and
/// runs none of its blocks until it is set up.
pub fn blocks_setup(d: &Driver) {
    blocks_setup_within(d, SETUP_BUDGET);
}

fn blocks_setup_within(d: &Driver, budget: Duration) {
    let n = d.s.clients.len();
    if n == 0 {
        return;
    }
    let deadline = Instant::now() + budget;
    let mut attempt = 0usize;
    for group in GROUPS {
        loop {
            let client = attempt % n;
            attempt += 1;
            match setup_attempt(d, client, group) {
                SetupTry::Transient(last) => {
                    if Instant::now() >= deadline {
                        println!("setup: block group {group}: not set up within the hook budget, block_run retries it: {last}");
                        break;
                    }
                    println!("setup: block group {group}: retrying after {last}");
                    std::thread::sleep(d.s.timing().poll.min(Duration::from_secs(1)));
                }
                done => {
                    conclude_setup(d, group, done);
                    break;
                }
            }
        }
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
        for code in ["Internal", "Unknown", "Aborted", "InvalidArgument", "NotFound", "AlreadyExists", "ResourceExhausted", ""] {
            assert_eq!(c(json!({"ok": false, "code": code})), BlockClass::Failed, "{code}");
        }
        // Loosened (porting file): metadata races, signature window,
        // external services and blocked writes.
        assert_eq!(c(json!({"ok": false, "code": "NotFound", "reason": "METADATA_NOT_FOUND"})), BlockClass::Expected);
        assert_eq!(c(json!({"ok": false, "code": "NotFound", "reason": "LEDGER_NOT_FOUND"})), BlockClass::Failed);
        for code in ["Unauthenticated", "PermissionDenied"] {
            assert_eq!(c(json!({"ok": false, "code": code, "reason": "SIGNATURE_REQUIRED"})), BlockClass::Transient, "{code}");
            assert_eq!(c(json!({"ok": false, "code": code})), BlockClass::Transient, "{code}");
        }
        for code in ["Internal", "Unavailable", "FailedPrecondition"] {
            assert_eq!(c(json!({"ok": false, "code": code, "reason": "EXTERNAL_SERVICE_ERROR"})), BlockClass::Transient, "{code}");
        }
        for reason in ["WRITES_BLOCKED_DISK_FULL", "WRITES_BLOCKED_CLOCK_SKEW"] {
            assert_eq!(c(json!({"ok": false, "code": "ResourceExhausted", "reason": reason})), BlockClass::Transient, "{reason}");
        }
        assert_eq!(c(json!({"ok": false, "code": "ResourceExhausted", "reason": "OTHER"})), BlockClass::Failed);
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
        // A created transaction without its payload is malformed.
        let empty = json!({"logs": [{"payload": {"apply": {"log": {"data": {"createdTransaction": {}}}}}}]});
        assert_eq!(malformed_pcv(&empty).unwrap()["why"], "createdTransaction carries no transaction");
        let null = json!({"logs": [{"payload": {"apply": {"log": {"data": {"createdTransaction": {"transaction": null}}}}}}]});
        assert!(malformed_pcv(&null).is_some());
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
        let d = Driver { s };
        for g in GROUPS {
            d.s.ext::<BlocksState>().mark_ready(g);
        }
        (d, t)
    }

    /// A driver whose groups are not set up yet.
    fn fresh_driver() -> (Driver, Arc<BlockTransport>) {
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
        // The drawn block is the first `/block`; chained consumers may follow.
        let calls: Vec<Value> = t.calls.lock().unwrap().iter().filter(|c| c.1 == "/block").map(|c| c.2.clone()).collect();
        let body = &calls[0];
        let name = body["name"].as_str().unwrap();
        assert!(EXPECTED.contains(&name));
        assert_eq!(body["timeoutMs"], 8000);
        let values = body["values"].as_array().unwrap();
        assert_eq!(values.len(), VALUES_PER_RUN);
        assert!(values.iter().all(|v| v.as_str().unwrap().parse::<u64>().is_ok()));
        let n = calls.len();
        for c in &calls {
            let (ok, failed) = literals_of(c["name"].as_str().unwrap());
            assert_eq!(fired("Reachable", ok), vec![true]);
            assert!(fired("Unreachable", failed).is_empty());
        }
        assert_eq!(fired("Always", "a block's post-commit volumes are well formed"), vec![true; n]);
        assert_eq!(fired("Sometimes", "a block runs within its pre-drawn values"), vec![true; n]);
        assert_eq!(fired("AlwaysOrUnreachable", "a block never runs out of its pre-drawn values"), vec![true; n]);
    }

    #[test]
    fn block_run_flags_malformed_volumes_and_exhaustion() {
        let (d, t) = block_driver();
        let missing = json!({"asset": "USD/2", "volumes": {"input": {}}});
        *t.default.lock().unwrap() = Some(json!({"ok": true, "exhausted": true, "used": 16, "response": pcv_response(missing)}));
        testkit::reset();
        testkit::run_rule(&d, block_run);
        // One evaluation per block sent (the drawn block and any chained consumer).
        let n = t.calls.lock().unwrap().iter().filter(|c| c.1 == "/block").count();
        assert!(n >= 1);
        assert_eq!(fired("Always", "a block's post-commit volumes are well formed"), vec![false; n]);
        assert_eq!(fired("Sometimes", "a block runs within its pre-drawn values"), vec![false; n]);
        assert_eq!(fired("AlwaysOrUnreachable", "a block never runs out of its pre-drawn values"), vec![false; n]);
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
            (json!({"ok": false, "code": "NotFound", "reason": "METADATA_NOT_FOUND"}), false),
            (json!({"ok": false, "code": "NotFound", "reason": "LEDGER_NOT_FOUND"}), true),
            (json!({"ok": false, "code": "Unauthenticated", "reason": "SIGNATURE_REQUIRED"}), false),
            (json!({"ok": false, "code": "PermissionDenied"}), false),
            (json!({"ok": false, "code": "ResourceExhausted", "reason": "WRITES_BLOCKED_DISK_FULL"}), false),
            (json!({"ok": false, "code": "ResourceExhausted"}), true),
            (json!({"ok": false, "code": "Internal", "reason": "EXTERNAL_SERVICE_ERROR"}), false),
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

    #[test]
    fn blocks_setup_never_fails_on_transient_answers_alone() {
        let (d, t) = fresh_driver();
        *t.default.lock().unwrap() = Some(json!({"ok": false, "code": "Unavailable"}));
        testkit::reset();
        blocks_setup_within(&d, Duration::ZERO);
        // One attempt per group, then the budget is out: no assertion.
        assert_eq!(t.calls.lock().unwrap().len(), 6);
        assert!(fired("Always", "should be able to setup scenario").is_empty());
        assert!(GROUPS.iter().all(|g| !d.s.ext::<BlocksState>().is_ready(g)));
        // block_run retries the setup instead of running a block.
        testkit::run_rule(&d, block_run);
        let calls = t.calls.lock().unwrap().clone();
        assert_eq!(calls.len(), 7);
        assert_eq!(calls[6].1, "/block-setup");
        assert!(fired("Always", "should be able to setup scenario").is_empty());
        assert!(testkit::assertions().iter().all(|a| !a.message.starts_with("block ")));
        // Once the setup answers ok, the group is ready and asserted true.
        *t.default.lock().unwrap() = None;
        testkit::run_rule(&d, block_run);
        let calls = t.calls.lock().unwrap().clone();
        assert_eq!(calls[7].1, "/block-setup");
        let g2 = calls[7].2["group"].as_str().unwrap();
        assert!(d.s.ext::<BlocksState>().is_ready(g2));
        assert_eq!(fired("Always", "should be able to setup scenario"), vec![true]);
    }

    #[test]
    fn blocks_setup_marks_groups_ready() {
        let (d, _t) = fresh_driver();
        testkit::reset();
        blocks_setup(&d);
        assert!(GROUPS.iter().all(|g| d.s.ext::<BlocksState>().is_ready(g)));
        assert_eq!(fired("Always", "should be able to setup scenario"), vec![true; 6]);
    }

    #[test]
    fn block_run_waits_out_the_signature_window() {
        let (d, t) = block_driver();
        d.s.signatures_required.store(true, std::sync::atomic::Ordering::SeqCst);
        testkit::reset();
        testkit::run_rule(&d, block_run);
        assert!(t.calls.lock().unwrap().is_empty());
        assert!(testkit::assertions().iter().all(|a| !a.message.starts_with("block ")));
    }

    /// A value that `Intn(n)` maps to `k` (the client's one-draw modulo).
    fn intn(v: u64, n: u64) -> u64 {
        v % n
    }

    #[test]
    fn chained_consumers_pick_the_funded_entity() {
        let values: Vec<u64> = (0..VALUES_PER_RUN as u64).map(|i| 1_000_003 * (i + 7) + i).collect();
        // Every producer and consumer is a registered block of one group.
        for (name, _, _) in BLOCKS {
            if let Some((next, v)) = chained_consumer(name, &values) {
                assert!(BLOCKS.iter().any(|b| b.0 == next), "{next}");
                assert_eq!(group_of(name), group_of(next));
                assert_eq!(v.len(), VALUES_PER_RUN);
            }
        }
        // fund → charge: the same subscriber (first draw, % 50).
        let (next, v) = chained_consumer("subscription/fund", &values).unwrap();
        assert_eq!(next, "subscription/charge");
        assert_eq!(intn(v[0], 50), intn(values[0], 50));
        assert_eq!(chained_consumer("subscription/charge", &values).unwrap().0, "subscription/recognize");
        // deposit → purchase: the same customer (first draw, % 50).
        let (next, v) = chained_consumer("marketplace/deposit", &values).unwrap();
        assert_eq!(next, "marketplace/purchase");
        assert_eq!(intn(v[0], 50), intn(values[0], 50));
        // purchase → payout: purchase's merchant (second draw) is payout's first.
        let (next, v) = chained_consumer("marketplace/purchase", &values).unwrap();
        assert_eq!(next, "marketplace/payout");
        assert_eq!(intn(v[0], 10), intn(values[1], 10));
        // fund_pool → disburse → provision: the same borrower (first draw, % 10).
        assert_eq!(chained_consumer("lending/fund_pool", &values).unwrap().0, "lending/disburse");
        let (next, v) = chained_consumer("lending/disburse", &values).unwrap();
        assert_eq!(next, "lending/provision");
        assert_eq!(intn(v[0], 10), intn(values[0], 10));
        // fund_clearing → distribute → cost_alloc from the funded department.
        assert_eq!(chained_consumer("payroll/fund_clearing", &values).unwrap().0, "payroll/distribute");
        // MultiLedgerPayrollDepartments order and payrollCostAlloc's `from`.
        let departments = ["engineering", "sales", "operations"];
        let alloc_from = ["operations", "engineering", "sales"];
        for k in 0..3u64 {
            let mut vals = values.clone();
            vals[0] = 3 * 1_000 + k;
            let (next, v) = chained_consumer("payroll/distribute", &vals).unwrap();
            assert_eq!(next, "payroll/cost_alloc");
            assert_eq!(alloc_from[intn(v[0], 3) as usize], departments[k as usize]);
        }
        // Consumers without a chained consumer of their own end the chain.
        for name in ["subscription/recognize", "marketplace/payout", "lending/provision", "payroll/cost_alloc", "gaming/topup"] {
            assert!(chained_consumer(name, &values).is_none(), "{name}");
        }
    }

    #[test]
    fn run_blocks_chains_consumers_on_the_same_client() {
        let (d, t) = block_driver();
        testkit::reset();
        let values: Vec<u64> = (0..VALUES_PER_RUN as u64).collect();
        let sent = run_blocks(&d, 2, "subscription/fund", values.clone(), true);
        assert_eq!(sent, vec!["subscription/fund", "subscription/charge", "subscription/recognize"]);
        let calls = t.calls.lock().unwrap().clone();
        assert_eq!(calls.len(), 3);
        assert!(calls.iter().all(|c| c.0 == 2 && c.1 == "/block"));
        assert_eq!(calls[0].2["timeoutMs"], 8000);
        assert!(calls[1].2["timeoutMs"].as_u64().unwrap() <= 8000);
        assert!(calls[1].2["timeoutMs"].as_u64().unwrap() >= CHAIN_MIN_LEFT.as_millis() as u64);
        assert_eq!(calls[1].2["values"], calls[0].2["values"]);
        assert_eq!(fired("Sometimes", "a chained consumer block succeeds after its producer commits"), vec![true, true]);
        for name in &sent {
            assert_eq!(fired("Reachable", literals_of(name).0), vec![true], "{name}");
        }
        // No chain without the draw.
        let (d, t) = block_driver();
        testkit::reset();
        assert_eq!(run_blocks(&d, 0, "subscription/fund", values.clone(), false), vec!["subscription/fund"]);
        assert_eq!(t.calls.lock().unwrap().len(), 1);
        assert!(fired("Sometimes", "a chained consumer block succeeds after its producer commits").is_empty());
    }

    #[test]
    fn run_blocks_stops_at_the_first_unsucceeded_block() {
        let values: Vec<u64> = (0..VALUES_PER_RUN as u64).collect();
        // A skipped consumer: false Sometimes, chain ends.
        let (d, t) = block_driver();
        {
            let mut r = t.replies.lock().unwrap();
            r.push_back(Ok(json!({"ok": true})));
            r.push_back(Ok(json!({"ok": false, "skipped": true})));
        }
        testkit::reset();
        assert_eq!(run_blocks(&d, 0, "marketplace/deposit", values.clone(), true), vec!["marketplace/deposit", "marketplace/purchase"]);
        assert_eq!(fired("Sometimes", "a chained consumer block succeeds after its producer commits"), vec![false]);
        // A transient consumer: no evaluation.
        let (d, t) = block_driver();
        {
            let mut r = t.replies.lock().unwrap();
            r.push_back(Ok(json!({"ok": true})));
            r.push_back(Ok(json!({"ok": false, "code": "Unavailable"})));
        }
        testkit::reset();
        assert_eq!(run_blocks(&d, 0, "lending/disburse", values.clone(), true), vec!["lending/disburse", "lending/provision"]);
        assert!(fired("Sometimes", "a chained consumer block succeeds after its producer commits").is_empty());
        // A producer that did not succeed chains nothing.
        let (d, t) = block_driver();
        t.replies.lock().unwrap().push_back(Ok(json!({"ok": false, "skipped": true})));
        testkit::reset();
        assert_eq!(run_blocks(&d, 0, "payroll/distribute", values, true), vec!["payroll/distribute"]);
        assert_eq!(t.calls.lock().unwrap().len(), 1);
    }

    #[test]
    fn group_of_maps_every_block_to_a_group() {
        for (name, _, _) in BLOCKS {
            assert!(GROUPS.contains(&group_of(name)), "{name}");
        }
    }
}
