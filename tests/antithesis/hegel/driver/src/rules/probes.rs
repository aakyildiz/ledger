//! Task 14 — Rust probes on Rust-checked ledgers (DESIGN §3, §4.10, §6.8):
//!
//! - `stale_probe`: unit increments of `probe:main` on `r-probe-<run>`, each
//!   followed by a stale GetAccount whose balance must stay within the
//!   attempted-write bound (port of `MD/parallel_driver_stale_reads`);
//! - `malformed_write`: malformed CreateTransaction requests on
//!   `r-bad-<run>`, which must be refused as invalid and never commit (the
//!   malformed half of `MD/parallel_driver_definitive_errors`);
//! - quiet hook `quiesce_and_absence`: a linearizable read after a write-free
//!   idle window must complete (issue #327, `MD/eventually_quiesce_read`),
//!   and `r-bad-<run>` must hold no transaction;
//! - quiet hook `eventually_correct`: per ledger (tracked and foreign),
//!   double entry over ListAccounts, list/get cross-checks under Barrier
//!   fences (`MD/eventually_correct`), ledger stats and aggregate double
//!   entry (`MD/parallel_driver_stats_consistency`, `…_invariants`).
//!
//! None of this needs the oracle: the probes work in degraded mode and the
//! hooks handle `QuietCtx.begin == None`.
use crate::clients::{definitive, CallOpts, Outcome, Route};
use crate::draw;
use crate::machine::{rule, Driver};
use crate::quiet::{HookPlan, QuietCtx, QuietHook};
use crate::setup::SetupHook;
use crate::shared::Shared;
use crate::wire::{parse_biguint_full, parse_signed_full, u64_field, BigUint, Posting, SignedBigInt, U256};
use crate::write::{UntrackedSend, ATTEMPT_TIMEOUT, IN_RULE_BUDGET};
use hegel::stateful::ConcurrentRule;
use hegel::TestCase;
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::{Duration, Instant};

pub const PROBE_ACCOUNT: &str = "probe:main";
pub const PROBE_ASSET: &str = "USD/2";

/// `r-probe-<run>`: the stale-read probe account and the quiesce writes.
pub fn probe_ledger(s: &Shared) -> String {
    format!("r-probe-{}", s.run)
}

/// `r-bad-<run>`: malformed requests only; must stay empty.
pub fn bad_ledger(s: &Shared) -> String {
    format!("r-bad-{}", s.run)
}

/// The Rust-side state of this family (one per `Shared`).
#[derive(Default)]
pub struct ProbeState {
    pub probe_ready: AtomicBool,
    pub bad_ready: AtomicBool,
    pub counters: Mutex<StaleCounters>,
    /// Counter for unique references (identities, never draws).
    next_ref: AtomicU64,
    /// References of malformed writes that were definitively rejected.
    rejected: Mutex<BTreeSet<String>>,
    rejected_total: AtomicU64,
}

/// Cap on remembered rejected references (the count keeps growing).
const REJECTED_CAP: usize = 4096;

impl ProbeState {
    fn next_ref(&self, s: &Shared, tag: &str) -> String {
        format!("r-{}-{}{}", s.run, tag, self.next_ref.fetch_add(1, Ordering::SeqCst))
    }

    fn note_rejected(&self, reference: &str) {
        self.rejected_total.fetch_add(1, Ordering::SeqCst);
        let mut r = self.rejected.lock().unwrap_or_else(|e| e.into_inner());
        if r.len() < REJECTED_CAP {
            r.insert(reference.to_string());
        }
    }
}

// ---------------------------------------------------------------- registries

pub fn rules() -> Vec<ConcurrentRule<Driver>> {
    vec![rule("stale_probe", 1.0, stale_probe), rule("malformed_write", 0.5, malformed_write)]
}

pub fn quiet_hooks() -> Vec<QuietHook> {
    vec![
        QuietHook { name: "quiesce_and_absence", budget: Duration::from_secs(45), needs_oracle: false, weight: weight_one, plan: plan_quiesce, run: run_quiesce_hook },
        QuietHook { name: "eventually_correct", budget: Duration::from_secs(90), needs_oracle: false, weight: weight_one, plan: plan_eventually_correct, run: run_eventually_correct_hook },
    ]
}

pub fn setup_hooks() -> Vec<SetupHook> {
    vec![SetupHook { name: "probes_setup", run: probes_setup }]
}

fn weight_one(_: &Shared) -> f64 {
    1.0
}

// ---------------------------------------------------------------- outcome helpers

/// An error the probes never report: no answer, client-local, or a
/// transient server condition (`W/internal` `IsTolerated`).
pub fn tolerated(o: &Outcome) -> bool {
    if o.is_ok() {
        return false;
    }
    o.is_unknown()
        || o.local
        || matches!(o.code.as_str(), "Unavailable" | "DeadlineExceeded" | "Canceled")
        || (o.code == "ResourceExhausted" && o.reason == "WRITES_BLOCKED_DISK_FULL")
}

/// The ledger is gone (deleted, or not created on the serving node yet).
fn ledger_gone(o: &Outcome) -> bool {
    !o.is_ok() && matches!(o.reason.as_str(), "LEDGER_NOT_FOUND" | "LEDGER_DELETED")
}

fn remaining(deadline: Instant) -> Duration {
    deadline.saturating_duration_since(Instant::now())
}

/// A call deadline: at most `cap`, never past `deadline`.
fn call_timeout(deadline: Instant, cap: Duration) -> Option<Duration> {
    let r = remaining(deadline);
    if r < Duration::from_millis(200) {
        None
    } else {
        Some(r.min(cap))
    }
}

// ---------------------------------------------------------------- setup

const SETUP_BUDGET: Duration = Duration::from_secs(30);
const SETUP_RETRY: Duration = Duration::from_millis(500);

/// Creates `r-probe-<run>` and `r-bad-<run>` (untracked, keyed). Bounded:
/// rules and hooks tolerate a ledger that does not exist.
pub fn probes_setup(d: &Driver) {
    let st = d.s.ext::<ProbeState>();
    for (ledger, flag) in [(probe_ledger(&d.s), &st.probe_ready), (bad_ledger(&d.s), &st.bad_ready)] {
        let key = d.s.next_untracked_key();
        let deadline = Instant::now() + SETUP_BUDGET;
        loop {
            let mut send = UntrackedSend::new(vec![json!({"createLedger": {"name": ledger}})], Some(key.clone()));
            send.schedule = (0..d.s.clients.len().max(1)).map(Route::lib).collect();
            send.budget = remaining(deadline).min(IN_RULE_BUDGET);
            let r = d.send_untracked(send);
            if r.outcome.is_ok() || r.outcome.reason == "LEDGER_ALREADY_EXISTS" {
                flag.store(true, Ordering::SeqCst);
                println!("setup: probes: ledger {ledger} ready");
                break;
            }
            if Instant::now() + SETUP_RETRY >= deadline {
                println!("setup: probes: ledger {ledger} not created: {}", r.outcome.summary());
                break;
            }
            std::thread::sleep(SETUP_RETRY);
        }
    }
}

// ---------------------------------------------------------------- volumes

/// Why a volume entry could not be read.
#[derive(Debug, PartialEq, Eq)]
pub enum VolumeError {
    /// A required field is absent (`input`, `output`, `balance`).
    Absent { input: bool, output: bool, balance: bool },
    /// A field is present but does not decode.
    Invalid,
}

/// Strictly parses one `common.AccountVolume`: the `volumes` message and its
/// three fields must be present (protojson writes a set empty message as
/// `{}`), and every value must decode.
pub fn parse_volume_strict(entry: &Value) -> Result<(BigUint, BigUint, SignedBigInt), VolumeError> {
    let vols = entry.get("volumes").filter(|v| v.is_object());
    let field = |k: &str| vols.and_then(|v| v.get(k)).filter(|x| !x.is_null());
    let (i, o, b) = (field("input"), field("output"), field("balance"));
    let (Some(i), Some(o), Some(b)) = (i, o, b) else {
        return Err(VolumeError::Absent { input: i.is_some(), output: o.is_some(), balance: b.is_some() });
    };
    let input = biguint_strict(i).ok_or(VolumeError::Invalid)?;
    let output = biguint_strict(o).ok_or(VolumeError::Invalid)?;
    let balance = signed_strict(b).ok_or(VolumeError::Invalid)?;
    Ok((input, output, balance))
}

fn biguint_strict(v: &Value) -> Option<BigUint> {
    if !v.is_object() {
        return None;
    }
    parse_biguint_full(v)
}

/// A `common.SignedBigInt` that decodes completely.
fn signed_strict(raw: &Value) -> Option<SignedBigInt> {
    if !raw.is_object() {
        return None;
    }
    if let Some(m) = raw.get("magnitude") {
        biguint_strict(m)?;
    }
    if raw.get("negative").is_some_and(|n| !n.is_boolean()) {
        return None;
    }
    parse_signed_full(raw)
}

/// `balance` compared with a non-negative count.
pub fn signed_cmp_u64(b: &SignedBigInt, n: u64) -> std::cmp::Ordering {
    use std::cmp::Ordering::*;
    if b.negative {
        return Less;
    }
    match b.magnitude.to_u128() {
        Some(m) => m.cmp(&(n as u128)),
        None => Greater,
    }
}

fn biguint_string(b: &BigUint) -> String {
    b.to_u128().map(|m| m.to_string()).unwrap_or_else(|| format!("0x{}", hex(&b.be)))
}

fn signed_string(b: &SignedBigInt) -> String {
    let mag = biguint_string(&b.magnitude);
    if b.negative {
        format!("-{mag}")
    } else {
        mag
    }
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// The `(asset, color)` volume entries of an account (`common.Account`).
fn volume_entries(account: &Value) -> Vec<&Value> {
    account.get("volumes").and_then(Value::as_array).map(|a| a.iter().collect()).unwrap_or_default()
}

fn entry_key(entry: &Value) -> (String, String) {
    (entry.get("asset").and_then(Value::as_str).unwrap_or("").to_string(), entry.get("color").and_then(Value::as_str).unwrap_or("").to_string())
}

fn entry_balance(entry: &Value) -> Option<&Value> {
    entry.get("volumes").and_then(|v| v.get("balance")).filter(|b| !b.is_null())
}

// ---------------------------------------------------------------- signed sums

/// An exact signed sum of `SignedBigInt` values of any width: two unsigned
/// accumulators (little-endian `u64` limbs), equal iff the sum is zero.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SignedSum {
    pos: Vec<u64>,
    neg: Vec<u64>,
}

fn limbs_of(be: &[u8]) -> Vec<u64> {
    be.rchunks(8)
        .map(|chunk| {
            let mut buf = [0u8; 8];
            buf[8 - chunk.len()..].copy_from_slice(chunk);
            u64::from_be_bytes(buf)
        })
        .collect()
}

fn add_limbs(acc: &mut Vec<u64>, x: &[u64]) {
    if acc.len() < x.len() {
        acc.resize(x.len(), 0);
    }
    let mut carry = false;
    for (i, slot) in acc.iter_mut().enumerate() {
        let xi = x.get(i).copied().unwrap_or(0);
        let (a, c1) = slot.overflowing_add(xi);
        let (b, c2) = a.overflowing_add(carry as u64);
        *slot = b;
        carry = c1 || c2;
    }
    if carry {
        acc.push(1);
    }
}

fn trimmed(v: &[u64]) -> &[u64] {
    let n = v.iter().rposition(|x| *x != 0).map(|i| i + 1).unwrap_or(0);
    &v[..n]
}

impl SignedSum {
    pub fn add(&mut self, b: &SignedBigInt) {
        let l = limbs_of(&b.magnitude.be);
        if b.negative {
            add_limbs(&mut self.neg, &l);
        } else {
            add_limbs(&mut self.pos, &l);
        }
    }
    pub fn is_zero(&self) -> bool {
        trimmed(&self.pos) == trimmed(&self.neg)
    }
    /// Positive and negative parts, hex, for details.
    pub fn describe(&self) -> Value {
        let h = |v: &[u64]| trimmed(v).iter().rev().map(|x| format!("{x:016x}")).collect::<String>();
        json!({"positive": format!("0x{}", h(&self.pos)), "negative": format!("0x{}", h(&self.neg))})
    }
}

// ---------------------------------------------------------------- stale_probe

/// Attempted/acked unit increments of `probe:main` (DESIGN §6.8 "unit
/// increment"). `attempted` counts before the send, so every increment that
/// can be visible to a read is counted; `acked` counts proven commits.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct StaleCounters {
    pub attempted: u64,
    pub acked: u64,
}

/// Whether an increment's outcome proves it committed: a success, or a
/// reference conflict on our own unique reference (an earlier attempt of
/// this increment committed).
pub fn increment_committed(o: &Outcome) -> bool {
    o.is_ok() || (definitive(o) && o.reason == "TRANSACTION_REFERENCE_CONFLICT")
}

/// Deadlines of one `stale_probe` round (unit tests shrink them).
#[derive(Clone, Debug)]
pub struct StaleTiming {
    pub rule_budget: Duration,
    pub attempt: Duration,
    pub read: Duration,
    /// The client HTTP exchange's margin over the RPC deadline.
    pub http_margin: Duration,
}

impl Default for StaleTiming {
    fn default() -> Self {
        StaleTiming { rule_budget: Duration::from_secs(10), attempt: Duration::from_secs(2), read: Duration::from_secs(2), http_margin: Duration::from_millis(1500) }
    }
}

impl StaleTiming {
    /// The longest one iteration can take.
    pub fn iteration_worst(&self) -> Duration {
        self.attempt + self.http_margin + self.read + self.http_margin
    }
}

/// The pre-drawn iterations of a round: (write route, stale read route).
pub struct StalePlan {
    pub iterations: Vec<(Route, Route)>,
}

pub const STALE_MAX_ITERATIONS: u64 = 5;

/// The stale read's route: pinned to one node 1/2, else round-robin.
fn stale_route(tc: &TestCase, s: &Shared) -> Route {
    let client = draw::pick(tc, s.clients.len().max(1));
    if !s.nodes.is_empty() && draw::chance(tc, 0.5) {
        return Route::node(client, draw::one_of(tc, &s.nodes));
    }
    if draw::chance(tc, 0.5) {
        Route::lib(client)
    } else {
        Route::plain(client)
    }
}

pub fn stale_probe(d: &Driver, tc: TestCase) {
    d.guarded("stale_probe", &tc, |d, tc| {
        let n = draw::int_in(tc, 1, STALE_MAX_ITERATIONS);
        let iterations = (0..n).map(|_| (draw::route(tc, &d.s), stale_route(tc, &d.s))).collect();
        run_stale_round(d, &StalePlan { iterations }, &StaleTiming::default());
        Some(())
    });
}

/// What one round did (for tests and logs).
#[derive(Debug, Default, PartialEq, Eq)]
pub struct StaleRound {
    pub iterations: usize,
    pub reads_ok: usize,
    pub lag: bool,
}

/// Runs a pre-drawn round; no draw happens in here.
pub fn run_stale_round(d: &Driver, plan: &StalePlan, t: &StaleTiming) -> StaleRound {
    let st = d.s.ext::<ProbeState>();
    let ledger = probe_ledger(&d.s);
    let started = Instant::now();
    let mut round = StaleRound::default();
    for (i, (write_route, read_route)) in plan.iterations.iter().take(STALE_MAX_ITERATIONS as usize).enumerate() {
        if i > 0 && started.elapsed() + t.iteration_worst() > t.rule_budget {
            break;
        }
        round.iterations += 1;
        // 1. One keyed, forced unit increment with a unique reference, single attempt.
        let reference = st.next_ref(&d.s, "probe");
        let req = forced_tx(&ledger, "world", PROBE_ACCOUNT, U256::from(1), PROBE_ASSET, Some(&reference));
        st.counters.lock().unwrap_or_else(|e| e.into_inner()).attempted += 1;
        let send = UntrackedSend {
            requests: vec![req],
            key: Some(d.s.next_untracked_key()),
            schedule: vec![write_route.clone()],
            // Single attempt: with a zero retry budget the loop stops right
            // after the first attempt (its deadline bounds the attempt).
            backoffs_ms: vec![0],
            attempt_timeout: t.attempt,
            budget: Duration::ZERO,
            sign: None,
        };
        let res = d.send_untracked(send);
        if increment_committed(&res.outcome) {
            st.counters.lock().unwrap_or_else(|e| e.into_inner()).acked += 1;
        }
        // 2. A stale GetAccount of the probe.
        let acked_before = st.counters.lock().unwrap_or_else(|e| e.into_inner()).acked;
        let req = json!({"ledger": ledger, "address": PROBE_ACCOUNT});
        let (_, out) = d.call(read_route, "bucket", "GetAccount", req, &CallOpts::stale().timeout(t.read));
        let attempted_after = st.counters.lock().unwrap_or_else(|e| e.into_inner()).attempted;
        let details = json!({"ledger": ledger, "round": i, "attempted": attempted_after, "acked": acked_before, "route": read_route.via(), "peer": out.peer});
        let (ok, lag) = check_stale_read(&out, acked_before, attempted_after, &details);
        if ok {
            round.reads_ok += 1;
        }
        round.lag |= lag;
    }
    let c = *st.counters.lock().unwrap_or_else(|e| e.into_inner());
    let summary = json!({"ledger": ledger, "attempted": c.attempted, "acked": c.acked, "iterations": round.iterations});
    check_sometimes!(round.reads_ok > 0, "stale read of probe account succeeded", summary);
    check_sometimes!(round.lag, "stale read observed lag behind acked writes", summary);
    round
}

/// Judges one stale read of the probe. Returns (read succeeded, lag seen).
pub fn check_stale_read(out: &Outcome, acked_before: u64, attempted_after: u64, details: &Value) -> (bool, bool) {
    if !out.is_ok() {
        // Transient, or the serving node's prefix predates the ledger
        // (NotFound is itself a valid prefix).
        if !tolerated(out) && out.code != "NotFound" {
            let mut d = details.clone();
            d["error"] = out.summary();
            check_unreachable!("stale read GetAccount returned unexpected error", d);
        }
        return (false, false);
    }
    let account = out.response.clone().unwrap_or(Value::Null);
    let probe_key = (PROBE_ASSET.to_string(), String::new());
    let Some(entry) = volume_entries(&account).into_iter().find(|e| entry_key(e) == probe_key) else {
        // Valid prefix: no increment applied yet on the serving node.
        return (true, false);
    };
    let (input, output, balance) = match parse_volume_strict(entry) {
        Ok(v) => v,
        Err(VolumeError::Absent { input, output, balance }) => {
            let mut d = details.clone();
            d["hasInput"] = json!(input);
            d["hasOutput"] = json!(output);
            d["hasBalance"] = json!(balance);
            check_unreachable!("stale read returned volume with absent required field", d);
            return (true, false);
        }
        Err(VolumeError::Invalid) => {
            let mut d = details.clone();
            d["volume"] = entry.clone();
            check_unreachable!("stale read returned invalid typed volume", d);
            return (true, false);
        }
    };
    let mut d = details.clone();
    d["input"] = json!(biguint_string(&input));
    d["output"] = json!(biguint_string(&output));
    d["balance"] = json!(signed_string(&balance));
    let within = !balance.negative && signed_cmp_u64(&balance, attempted_after) != std::cmp::Ordering::Greater;
    check_always!(within, "stale read balance stays within attempted write bounds", d);
    let single = !balance.negative && balance.magnitude == input && output.is_zero();
    check_always!(single, "stale read probe volumes are single-direction consistent", d);
    let lag = signed_cmp_u64(&balance, acked_before) == std::cmp::Ordering::Less;
    (true, lag)
}

/// A forced single-posting CreateTransaction.
fn forced_tx(ledger: &str, source: &str, dest: &str, amount: U256, asset: &str, reference: Option<&str>) -> Value {
    let mut req = crate::wire::create_tx_request(ledger, &[Posting::new(source, dest, amount, asset)], reference);
    req["apply"]["action"]["createTransaction"]["force"] = json!(true);
    req
}

// ---------------------------------------------------------------- malformed_write

/// The malformed request shapes. Each fails admission with InvalidArgument.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MalformedArm {
    /// `asset_invalid` (`ERROR_REASON_VALIDATION`).
    InvalidAsset,
    /// `account_address_invalid_char` (`ERROR_REASON_VALIDATION`).
    InvalidAddressChar,
    /// `empty_transaction` (`ERROR_REASON_VALIDATION`).
    EmptyPostings,
    /// `metadata_key_limit_exceeded`: the server maps the per-key size limit
    /// to `ERROR_REASON_METADATA_LIMIT_EXCEEDED` (kind Validation, still
    /// InvalidArgument; `internal/domain/reason.go`), not `VALIDATION`.
    MetadataKeyTooLong,
}

pub const MALFORMED_ARMS: [MalformedArm; 4] = [MalformedArm::InvalidAsset, MalformedArm::InvalidAddressChar, MalformedArm::EmptyPostings, MalformedArm::MetadataKeyTooLong];

/// The invalid asset literal of `MD/parallel_driver_definitive_errors`.
pub const INVALID_ASSET: &str = "!!not a valid asset!!";
/// Bytes outside the address alphabet `[a-zA-Z0-9_:-]`.
pub const INVALID_ADDRESS_CHARS: [char; 8] = ['!', '#', '$', '%', '&', '*', '@', '.'];
/// Over the default per-key ceiling (256 bytes, `DefaultMetadataMaxKeyBytes`).
pub const TOO_LONG_KEY_LEN: usize = 1024;

impl MalformedArm {
    pub fn name(self) -> &'static str {
        match self {
            MalformedArm::InvalidAsset => "asset_invalid",
            MalformedArm::InvalidAddressChar => "account_address_invalid_char",
            MalformedArm::EmptyPostings => "empty_transaction",
            MalformedArm::MetadataKeyTooLong => "metadata_key_limit_exceeded",
        }
    }
    pub fn expected_reason(self) -> &'static str {
        match self {
            MalformedArm::MetadataKeyTooLong => "METADATA_LIMIT_EXCEEDED",
            _ => "VALIDATION",
        }
    }
}

/// Builds the malformed request of `arm` (one CreateTransaction, forced,
/// with a unique reference): the malformation is its only defect.
pub fn malformed_request(arm: MalformedArm, ledger: &str, reference: &str, bad_char: char) -> Value {
    match arm {
        MalformedArm::InvalidAsset => forced_tx(ledger, "world", "users:1", U256::from(1), INVALID_ASSET, Some(reference)),
        MalformedArm::InvalidAddressChar => forced_tx(ledger, "world", &format!("users:bad{bad_char}x"), U256::from(1), "USD/2", Some(reference)),
        MalformedArm::EmptyPostings => {
            json!({"apply": {"ledger": ledger, "action": {"createTransaction": {"postings": [], "reference": reference, "force": true}}}})
        }
        MalformedArm::MetadataKeyTooLong => {
            let mut req = forced_tx(ledger, "world", "users:1", U256::from(1), "USD/2", Some(reference));
            req["apply"]["action"]["createTransaction"]["metadata"] = json!({ "k".repeat(TOO_LONG_KEY_LEN): {"stringValue": "v"} });
            req
        }
    }
}

pub fn malformed_write(d: &Driver, tc: TestCase) {
    d.guarded("malformed_write", &tc, |d, tc| {
        // PLAN: draws only.
        let arm = *draw::one_of(tc, &MALFORMED_ARMS);
        let bad_char = *draw::one_of(tc, &INVALID_ADDRESS_CHARS);
        let schedule = draw::schedule(tc, &d.s, 4);
        let backoffs = draw::backoffs(tc, 4);
        // EXEC.
        let st = d.s.ext::<ProbeState>();
        if !st.bad_ready.load(Ordering::SeqCst) {
            return None;
        }
        let ledger = bad_ledger(&d.s);
        let reference = st.next_ref(&d.s, "bad");
        let req = malformed_request(arm, &ledger, &reference, bad_char);
        let send = UntrackedSend { requests: vec![req], key: Some(d.s.next_untracked_key()), schedule, backoffs_ms: backoffs, attempt_timeout: ATTEMPT_TIMEOUT, budget: IN_RULE_BUDGET, sign: None };
        let res = d.send_untracked(send);
        judge_malformed(&st, arm, &ledger, &reference, &res.outcome);
        Some(())
    });
}

/// The verdict on one malformed write's outcome. Returns whether it was
/// definitively rejected.
pub fn judge_malformed(st: &ProbeState, arm: MalformedArm, ledger: &str, reference: &str, out: &Outcome) -> bool {
    let details = json!({"ledger": ledger, "arm": arm.name(), "reference": reference, "outcome": out.summary(), "expectedReason": arm.expected_reason()});
    if out.is_ok() {
        check_unreachable!("a malformed request never commits", details);
        check_sometimes!(false, "definitive write rejection recorded for absence check", details);
        return false;
    }
    let rejected = definitive(out);
    if rejected {
        let invalid = out.code == "InvalidArgument" && out.reason == arm.expected_reason();
        check_always_or_unreachable!(invalid, "a malformed request is refused as invalid", details);
        st.note_rejected(reference);
    }
    check_sometimes!(rejected, "definitive write rejection recorded for absence check", details);
    rejected
}

// ---------------------------------------------------------------- paging

#[derive(Debug)]
#[allow(dead_code)] // payloads are read through Debug in logs
pub enum PageError {
    /// The hook's deadline arrived.
    Deadline,
    /// The ledger is gone.
    Gone,
    /// A tolerated (transient) error.
    Tolerated(Outcome),
    /// Any other error (the caller asserts).
    Unexpected(Outcome),
    /// The listing did not converge (a page overflowed or cursors looped).
    Incomplete,
}

const MAX_PAGES: usize = 2000;

/// Pages a server-streaming listing to its end (linearizable reads).
pub fn page_all(d: &Driver, route: &Route, method: &str, ledger: Option<&str>, page_size: u64, deadline: Instant) -> Result<Vec<Value>, PageError> {
    let mut items = Vec::new();
    let mut cursor = String::new();
    let mut seen = BTreeSet::new();
    for _ in 0..MAX_PAGES {
        let timeout = call_timeout(deadline, Duration::from_secs(5)).ok_or(PageError::Deadline)?;
        let mut options = json!({"pageSize": page_size});
        if !cursor.is_empty() {
            options["cursor"] = json!(cursor);
        }
        let mut req = json!({"options": options});
        if let Some(l) = ledger {
            req["ledger"] = json!(l);
        }
        let (_, out) = d.call(route, "bucket", method, req, &CallOpts::linearizable().timeout(timeout).max_items(page_size + 1));
        if !out.is_ok() {
            return Err(if ledger_gone(&out) {
                PageError::Gone
            } else if tolerated(&out) {
                PageError::Tolerated(out)
            } else {
                PageError::Unexpected(out)
            });
        }
        if !out.complete {
            return Err(PageError::Incomplete);
        }
        items.extend(out.items.clone().unwrap_or_default());
        if out.next_cursor.is_empty() {
            return Ok(items);
        }
        if !seen.insert(out.next_cursor.clone()) {
            return Err(PageError::Incomplete);
        }
        cursor = out.next_cursor.clone();
    }
    Err(PageError::Incomplete)
}

// ---------------------------------------------------------------- quiesce_and_absence

pub const QUIESCE_IDLE_SECS: [u64; 3] = [2, 5, 8];
pub const QUIESCE_WRITE_TRIES: usize = 3;
pub const QUIESCE_WRITE_TIMEOUT: Duration = Duration::from_secs(8);
pub const QUIESCE_READ_TIMEOUT: Duration = Duration::from_secs(30);
const HTTP_MARGIN: Duration = Duration::from_millis(1500);
/// The absence listing's share of the quiesce budget.
const ABSENCE_BUDGET: Duration = Duration::from_secs(8);

/// The quiesce hook's draws.
#[derive(Clone, Debug)]
pub struct QuiescePlan {
    pub dest: String,
    pub amount: u64,
    pub idle: Duration,
    /// One route per write try.
    pub write_routes: Vec<Route>,
    pub backoffs_ms: Vec<u64>,
}

fn plan_quiesce(d: &Driver, tc: &TestCase) -> HookPlan {
    let dest = draw::users_address(tc);
    let amount = draw::amount_positive(tc);
    let idle = Duration::from_secs(*draw::one_of(tc, &QUIESCE_IDLE_SECS));
    let write_routes = (0..QUIESCE_WRITE_TRIES).map(|_| draw::route(tc, &d.s)).collect();
    let backoffs_ms = draw::backoffs(tc, QUIESCE_WRITE_TRIES);
    Box::new(QuiescePlan { dest, amount, idle, write_routes, backoffs_ms })
}

fn run_quiesce_hook(d: &Driver, ctx: &mut QuietCtx, plan: &HookPlan) {
    let Some(plan) = plan.downcast_ref::<QuiescePlan>() else { return };
    // The hook does not use the oracle: `ctx.begin` may be None.
    let r = run_quiesce(d, ctx.client, ctx.deadline, plan);
    println!("driver: quiesce_and_absence: {r:?}");
}

/// What the quiesce hook did.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct QuiesceReport {
    pub absence_checked: bool,
    pub write_acked: bool,
    pub read_done: bool,
    pub read_ok: bool,
}

/// (b) then (a): the absence check first, so a hanging probe read (the
/// property under test) cannot starve it; then the write, the idle window
/// with no request at all, and the 30 s linearizable read.
pub fn run_quiesce(d: &Driver, client: usize, deadline: Instant, plan: &QuiescePlan) -> QuiesceReport {
    let st = d.s.ext::<ProbeState>();
    let mut rep = QuiesceReport { absence_checked: absence_check(d, &st, client, deadline.min(Instant::now() + ABSENCE_BUDGET)), ..Default::default() };

    // (a) the write: at most 3 tries of 8 s each, same key and bytes,
    // leaving room for the idle window and the read.
    let ledger = probe_ledger(&d.s);
    let tail = plan.idle + QUIESCE_READ_TIMEOUT + HTTP_MARGIN;
    let key = d.s.next_untracked_key();
    let req = forced_tx(&ledger, "world", &plan.dest, U256::from(plan.amount.max(1)), "COIN", None);
    for k in 0..QUIESCE_WRITE_TRIES {
        let room = remaining(deadline).saturating_sub(tail);
        if room < Duration::from_millis(500) {
            break;
        }
        let attempt = QUIESCE_WRITE_TIMEOUT.min(room.saturating_sub(HTTP_MARGIN).max(Duration::from_millis(200)));
        let route = plan.write_routes.get(k).cloned().unwrap_or_else(|| Route::plain(client));
        let send = UntrackedSend {
            requests: vec![req.clone()],
            key: Some(key.clone()),
            schedule: vec![route],
            backoffs_ms: vec![0],
            attempt_timeout: attempt,
            // One attempt per try (zero retry budget); tries are counted here.
            budget: Duration::ZERO,
            sign: None,
        };
        let res = d.send_untracked(send);
        if res.outcome.is_ok() {
            rep.write_acked = true;
            break;
        }
        println!("driver: quiesce_and_absence: write try {} failed: {}", k + 1, res.outcome.summary());
        if definitive(&res.outcome) {
            break;
        }
        let backoff = Duration::from_millis(plan.backoffs_ms.get(k).copied().unwrap_or(500));
        if remaining(deadline).saturating_sub(tail) <= backoff {
            break;
        }
        std::thread::sleep(backoff);
    }
    if rep.write_acked && remaining(deadline) >= tail {
        // The idle window: no request of any kind (no Barrier: it is a write).
        std::thread::sleep(plan.idle);
        let req = json!({"ledger": ledger, "address": plan.dest});
        // `plain`: no library retry policy can hide a hang behind retries.
        let started = Instant::now();
        let (_, out) = d.call(&Route::plain(client), "bucket", "GetAccount", req, &CallOpts::linearizable().timeout(QUIESCE_READ_TIMEOUT));
        let elapsed = started.elapsed();
        rep.read_done = true;
        rep.read_ok = out.is_ok();
        let details = json!({"ledger": ledger, "address": plan.dest, "idleSeconds": plan.idle.as_secs(), "elapsedMs": elapsed.as_millis() as u64, "client": client, "outcome": out.summary()});
        check_always!(quiesce_read_completed(&out, elapsed), "linearizable read completes on a quiescent cluster", details);
        if out.is_ok() {
            check_reachable!("quiescent linearizable read returned data", details);
        }
    }
    check_sometimes!(rep.absence_checked && rep.read_done, "quiesce_and_absence completes within its quiet budget", json!({"report": format!("{rep:?}")}));
    rep
}

/// The probe read completed: success, or an error the 30 s deadline did not
/// cause (`MD/eventually_quiesce_read`: `err == nil || probeCtx.Err() != DeadlineExceeded`).
pub fn quiesce_read_completed(out: &Outcome, elapsed: Duration) -> bool {
    if out.is_ok() {
        return true;
    }
    let deadline_hit = out.unknown == "timeout" || elapsed + Duration::from_millis(100) >= QUIESCE_READ_TIMEOUT;
    !deadline_hit
}

/// (b): `r-bad-<run>` holds no transaction. Returns whether it concluded.
fn absence_check(d: &Driver, st: &ProbeState, client: usize, deadline: Instant) -> bool {
    if !st.bad_ready.load(Ordering::SeqCst) {
        return true; // nothing was ever sent there
    }
    let ledger = bad_ledger(&d.s);
    let txs = match page_all(d, &Route::lib(client), "ListTransactions", Some(&ledger), 100, deadline) {
        Ok(txs) => txs,
        Err(e) => {
            println!("driver: quiesce_and_absence: listing {ledger} inconclusive: {e:?}");
            return false;
        }
    };
    let rejected = st.rejected.lock().unwrap_or_else(|e| e.into_inner()).clone();
    let total = st.rejected_total.load(Ordering::SeqCst);
    let mut found_rejected = false;
    for tx in &txs {
        let reference = tx.get("reference").and_then(Value::as_str).unwrap_or("");
        let d = json!({"ledger": ledger, "reference": reference, "transaction": tx, "rejectedCount": total});
        // Every request sent to r-bad is malformed: any transaction there is a commit of one.
        check_unreachable!("a malformed request never commits", d);
        if rejected.contains(reference) {
            found_rejected = true;
            check_always!(false, "definitively rejected write never appears in the ledger", d);
        }
    }
    if total > 0 && !found_rejected {
        check_always!(true, "definitively rejected write never appears in the ledger", json!({"ledger": ledger, "rejectedCount": total, "transactions": txs.len()}));
    }
    true
}

// ---------------------------------------------------------------- eventually_correct

/// Accounts cross-checked with GetAccount per ledger.
pub const EC_SAMPLES: usize = 20;
pub const EC_PAGE_SIZE: u64 = 100;
/// Fence attempts per ledger: the first read and up to 2 re-reads.
pub const EC_FENCE_TRIES: usize = 3;

#[derive(Clone, Debug)]
pub struct EcPlan {
    /// Indices into the listed accounts (modulo the count), drawn here.
    pub samples: Vec<u32>,
}

fn plan_eventually_correct(_: &Driver, tc: &TestCase) -> HookPlan {
    let samples = (0..EC_SAMPLES).map(|_| draw::ledger_pick(tc)).collect();
    Box::new(EcPlan { samples })
}

fn run_eventually_correct_hook(d: &Driver, ctx: &mut QuietCtx, plan: &HookPlan) {
    let Some(plan) = plan.downcast_ref::<EcPlan>() else { return };
    // The hook does not use the oracle: `ctx.begin` may be None.
    let r = run_eventually_correct(d, ctx.client, ctx.deadline, plan);
    println!("driver: eventually_correct: {r:?}");
}

/// The outcome of a fenced observation (`after == before + 1`).
#[derive(Debug, PartialEq, Eq)]
pub enum Fence<T> {
    /// No proposal but the closing barrier crossed this observation.
    Quiescent { obs: T, tries: usize },
    /// Every try saw other proposals.
    NotQuiescent { tries: usize },
    /// A barrier or the observation could not be taken.
    Aborted,
}

/// Barrier, observe, barrier; accepted only when the commit index moved by
/// exactly one (the closing barrier itself). Otherwise re-reads, at most
/// `max_tries` observations in all.
pub fn fenced<T>(max_tries: usize, mut barrier: impl FnMut() -> Option<u64>, mut observe: impl FnMut() -> Option<T>) -> Fence<T> {
    for tries in 1..=max_tries {
        let Some(before) = barrier() else { return Fence::Aborted };
        let Some(obs) = observe() else { return Fence::Aborted };
        let Some(after) = barrier() else { return Fence::Aborted };
        if before.checked_add(1) == Some(after) {
            return Fence::Quiescent { obs, tries };
        }
    }
    Fence::NotQuiescent { tries: max_tries }
}

/// One Barrier's commit index (a few tries on transient errors).
fn barrier(d: &Driver, route: &Route, deadline: Instant) -> Option<u64> {
    for attempt in 1..=5 {
        let timeout = call_timeout(deadline, Duration::from_secs(5))?;
        let (_, out) = d.call(route, "bucket", "Barrier", json!({}), &CallOpts::default().timeout(timeout));
        if out.is_ok() {
            return Some(u64_field(out.response.as_ref().unwrap_or(&Value::Null), "commitIndex"));
        }
        if !tolerated(&out) {
            check_unreachable!("barrier returned unexpected error", json!({"error": out.summary(), "attempt": attempt}));
            return None;
        }
        std::thread::sleep(Duration::from_millis(200).min(remaining(deadline)));
    }
    None
}

/// What one fenced read of a ledger observed.
#[derive(Debug, Default)]
pub struct LedgerObs {
    pub accounts: usize,
    /// Sum of listed balances per (asset, color).
    pub sums: BTreeMap<(String, String), SignedSum>,
    /// (listed account, GetAccount response) of the sampled accounts.
    pub pairs: Vec<(Value, Value)>,
}

/// Per-ledger result.
#[derive(Debug, PartialEq, Eq)]
pub enum LedgerCheck {
    Checked,
    NotQuiescent,
    Skipped,
}

/// The report of one `eventually_correct` run.
#[derive(Debug, Default)]
pub struct EcReport {
    pub ledgers: usize,
    pub checked: usize,
    pub not_quiescent: usize,
    pub skipped: usize,
    pub completed: bool,
}

pub fn run_eventually_correct(d: &Driver, client: usize, deadline: Instant, plan: &EcPlan) -> EcReport {
    let mut rep = EcReport::default();
    let ledgers = match page_all(d, &Route::lib(client), "ListLedgers", None, EC_PAGE_SIZE, deadline) {
        Ok(items) => items,
        Err(e) => {
            println!("driver: eventually_correct: ListLedgers: {e:?}");
            check_sometimes!(false, "eventually_correct completes within its quiet budget", json!({"stage": "ListLedgers"}));
            return rep;
        }
    };
    let names: Vec<String> = ledgers
        .iter()
        .filter(|l| l.get("deletedAt").is_none_or(Value::is_null))
        .filter_map(|l| l.get("name").and_then(Value::as_str).map(str::to_string))
        .collect();
    rep.ledgers = names.len();
    let mut reached_all = true;
    for name in &names {
        if remaining(deadline) < Duration::from_secs(1) {
            // Ledgers not reached before the deadline get no verdict.
            reached_all = false;
            break;
        }
        match check_ledger(d, client, deadline, name, plan) {
            LedgerCheck::Checked => rep.checked += 1,
            LedgerCheck::NotQuiescent => rep.not_quiescent += 1,
            LedgerCheck::Skipped => rep.skipped += 1,
        }
    }
    rep.completed = reached_all && Instant::now() <= deadline;
    check_sometimes!(rep.completed, "eventually_correct completes within its quiet budget", json!({"ledgers": rep.ledgers, "checked": rep.checked, "notQuiescent": rep.not_quiescent, "skipped": rep.skipped}));
    rep
}

fn check_ledger(d: &Driver, client: usize, deadline: Instant, ledger: &str, plan: &EcPlan) -> LedgerCheck {
    let read_route = Route::lib(client);
    // Barriers go without the library retry policy: a retried Barrier adds
    // a second entry and only costs a re-read, but plain keeps fences clean.
    let fence_route = Route::plain(client);
    if !stats_and_aggregates(d, &read_route, deadline, ledger) {
        return LedgerCheck::Skipped;
    }
    let fence = fenced(EC_FENCE_TRIES, || barrier(d, &fence_route, deadline), || observe_ledger(d, &read_route, deadline, ledger, plan));
    match fence {
        Fence::Quiescent { obs, tries } => {
            check_sometimes!(true, "barrier quiescence achieved", json!({"ledger": ledger, "tries": tries}));
            evaluate_ledger(ledger, &obs);
            LedgerCheck::Checked
        }
        Fence::NotQuiescent { tries } => {
            check_sometimes!(false, "barrier quiescence achieved", json!({"ledger": ledger, "tries": tries}));
            println!("driver: eventually_correct: {ledger}: no quiescent fence after {tries} tries; no verdict");
            LedgerCheck::NotQuiescent
        }
        Fence::Aborted => LedgerCheck::Skipped,
    }
}

/// Lists every account (asserting balance presence on the way) and fetches
/// the sampled ones. `None` = inconclusive (deadline, transient, gone).
fn observe_ledger(d: &Driver, route: &Route, deadline: Instant, ledger: &str, plan: &EcPlan) -> Option<LedgerObs> {
    let accounts = match page_all(d, route, "ListAccounts", Some(ledger), EC_PAGE_SIZE, deadline) {
        Ok(a) => a,
        Err(PageError::Unexpected(out)) => {
            check_unreachable!("listAccounts returned unexpected error", json!({"ledger": ledger, "error": out.summary()}));
            return None;
        }
        Err(e) => {
            println!("driver: eventually_correct: {ledger}: ListAccounts inconclusive: {e:?}");
            return None;
        }
    };
    let mut obs = LedgerObs { accounts: accounts.len(), ..Default::default() };
    sum_balances(ledger, &accounts, &mut obs.sums);
    if accounts.is_empty() {
        return Some(obs);
    }
    let picked: BTreeSet<usize> = plan.samples.iter().map(|s| *s as usize % accounts.len()).collect();
    for i in picked {
        let listed = &accounts[i];
        let address = listed.get("address").and_then(Value::as_str).unwrap_or("").to_string();
        let timeout = call_timeout(deadline, Duration::from_secs(5))?;
        let (_, out) = d.call(route, "bucket", "GetAccount", json!({"ledger": ledger, "address": address}), &CallOpts::linearizable().timeout(timeout));
        if out.is_ok() {
            obs.pairs.push((listed.clone(), out.response.clone().unwrap_or(Value::Null)));
        } else if ledger_gone(&out) {
            return None;
        } else if !tolerated(&out) {
            check_unreachable!("GetAccount returned unexpected error", json!({"ledger": ledger, "address": address, "error": out.summary()}));
        }
    }
    Some(obs)
}

/// Sums listed balances per (asset, color); asserts every balance is
/// present and valid (`MD/eventually_correct` `checkBalanced`).
pub fn sum_balances(ledger: &str, accounts: &[Value], sums: &mut BTreeMap<(String, String), SignedSum>) {
    for account in accounts {
        let address = account.get("address").and_then(Value::as_str).unwrap_or("");
        for entry in volume_entries(account) {
            let key = entry_key(entry);
            let sum = sums.entry(key.clone()).or_default();
            let raw = entry_balance(entry);
            check_always!(raw.is_some(), "double-entry: account volume balance field is absent", json!({"ledger": ledger, "account": address, "asset": key.0, "color": key.1}));
            let Some(raw) = raw else { continue };
            let parsed = signed_strict(raw);
            check_always!(parsed.is_some(), "double-entry: account volume balance is invalid", json!({"ledger": ledger, "account": address, "asset": key.0, "color": key.1, "balance": raw}));
            if let Some(b) = parsed {
                sum.add(&b);
            }
        }
    }
}

/// The per-(asset, color) balances of an account; entries whose balance
/// does not decode are left out (asserted elsewhere).
fn balances_of(account: &Value) -> BTreeMap<(String, String), SignedBigInt> {
    volume_entries(account).into_iter().filter_map(|e| entry_balance(e).and_then(signed_strict).map(|b| (entry_key(e), b))).collect()
}

/// The verdicts on a quiescent observation.
pub fn evaluate_ledger(ledger: &str, obs: &LedgerObs) {
    if obs.sums.is_empty() {
        check_always!(true, "double-entry: sum of balances should be 0", json!({"ledger": ledger, "note": "no accounts"}));
    }
    for ((asset, color), sum) in &obs.sums {
        check_always!(sum.is_zero(), "double-entry: sum of balances should be 0", json!({"ledger": ledger, "asset": asset, "color": color, "sum": sum.describe(), "accounts": obs.accounts}));
    }
    for (listed, got) in &obs.pairs {
        let address = listed.get("address").and_then(Value::as_str).unwrap_or("");
        for e in volume_entries(got) {
            let k = entry_key(e);
            let raw = entry_balance(e);
            check_always!(raw.is_some(), "cross-check: GetAccount volume balance is absent", json!({"ledger": ledger, "account": address, "asset": k.0, "color": k.1}));
            if let Some(raw) = raw {
                check_always!(signed_strict(raw).is_some(), "cross-check: GetAccount volume balance is invalid", json!({"ledger": ledger, "account": address, "asset": k.0, "color": k.1, "balance": raw}));
            }
        }
        // Balances on every (asset, color) of either side; a bucket missing
        // on one side reads as a zero balance there (drained volumes may be
        // evicted).
        let (lb, gb) = (balances_of(listed), balances_of(got));
        let keys: BTreeSet<&(String, String)> = lb.keys().chain(gb.keys()).collect();
        let zero = SignedBigInt::default();
        for k in keys {
            let (l, g) = (lb.get(k).unwrap_or(&zero), gb.get(k).unwrap_or(&zero));
            let details = json!({"ledger": ledger, "account": address, "asset": k.0, "color": k.1, "listBalance": signed_string(l), "actualBalance": signed_string(g)});
            if l == g {
                check_reachable!("list/get balance pair verified matching", details);
            } else {
                check_unreachable!("list/get balance divergence persisted past quiescence", details);
            }
        }
        // Metadata: the whole map, typed values compared exactly.
        let empty = serde_json::Map::new();
        let lm = listed.get("metadata").and_then(Value::as_object).unwrap_or(&empty);
        let gm = got.get("metadata").and_then(Value::as_object).unwrap_or(&empty);
        for (key, lv) in lm {
            let gv = gm.get(key);
            check_always_or_unreachable!(gv.is_some(), "list metadata key should exist in getaccount", json!({"ledger": ledger, "account": address, "key": key}));
            if let Some(gv) = gv {
                check_always_or_unreachable!(lv == gv, "list metadata value should match getaccount", json!({"ledger": ledger, "account": address, "key": key, "listVal": lv, "getVal": gv}));
            }
        }
        check_always_or_unreachable!(lm.len() == gm.len(), "metadata key count should match between list and get", json!({"ledger": ledger, "account": address, "listKeys": lm.len(), "getKeys": gm.len()}));
    }
    check_reachable!("can check all volumes for consistency", json!({"ledger": ledger, "accounts": obs.accounts, "pairs": obs.pairs.len()}));
}

/// GetLedgerStats (`logCount ≥ transactionCount`) and AggregateVolumes
/// (input == output per asset, exact U256). Returns false when the ledger
/// is gone or the deadline passed.
fn stats_and_aggregates(d: &Driver, route: &Route, deadline: Instant, ledger: &str) -> bool {
    let Some(timeout) = call_timeout(deadline, Duration::from_secs(5)) else { return false };
    let (_, out) = d.call(route, "bucket", "GetLedgerStats", json!({"ledger": ledger}), &CallOpts::linearizable().timeout(timeout));
    if out.is_ok() {
        check_stats(ledger, out.response.as_ref().unwrap_or(&Value::Null));
    } else if ledger_gone(&out) {
        return false;
    } else if !tolerated(&out) {
        println!("driver: eventually_correct: {ledger}: GetLedgerStats failed: {}", out.summary());
    }
    let Some(timeout) = call_timeout(deadline, Duration::from_secs(5)) else { return false };
    let (_, out) = d.call(route, "bucket", "AggregateVolumes", json!({"ledger": ledger}), &CallOpts::linearizable().timeout(timeout));
    if out.is_ok() {
        check_aggregates(ledger, out.response.as_ref().unwrap_or(&Value::Null));
    } else if ledger_gone(&out) {
        return false;
    } else if !tolerated(&out) {
        check_unreachable!("AggregateVolumes returned unexpected error", json!({"ledger": ledger, "error": out.summary()}));
    }
    true
}

pub fn check_stats(ledger: &str, stats: &Value) {
    let tx = u64_field(stats, "transactionCount");
    let logs = u64_field(stats, "logCount");
    check_always!(logs >= tx, "log count must be >= transaction count (logs include metadata, reverts, etc.)", json!({"ledger": ledger, "transactionCount": tx, "logCount": logs}));
}

/// `common.Uint256` of an aggregate: absent = 0 (a nil message reads as
/// zero, like `Uint256.ToBigInt` on nil); present but undecodable = `None`.
fn aggregate_u256(v: Option<&Value>) -> Option<U256> {
    match v {
        None | Some(Value::Null) => Some(U256::ZERO),
        Some(x) => U256::from_json(x),
    }
}

pub fn check_aggregates(ledger: &str, resp: &Value) {
    for vol in resp.get("volumes").and_then(Value::as_array).cloned().unwrap_or_default() {
        let input = aggregate_u256(vol.get("input"));
        let output = aggregate_u256(vol.get("output"));
        // Both must decode: two undecodable sides never compare equal.
        let equal = matches!((input, output), (Some(i), Some(o)) if i == o);
        check_always!(
            equal,
            "aggregate double-entry: input must equal output",
            json!({"ledger": ledger, "asset": vol.get("asset"), "color": vol.get("color"), "input": input.map(|x| x.to_decimal()), "output": output.map(|x| x.to_decimal()), "volume": vol})
        );
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::testkit::{self, FakeReply};
    use base64::Engine;

    fn o(v: Value) -> Outcome {
        Outcome::from_json(&v)
    }

    fn b64(bytes: &[u8]) -> String {
        base64::engine::general_purpose::STANDARD.encode(bytes)
    }

    fn mag(n: u128) -> Value {
        if n == 0 {
            json!({})
        } else {
            let be = n.to_be_bytes();
            let first = be.iter().position(|x| *x != 0).unwrap();
            json!({"magnitude": b64(&be[first..])})
        }
    }

    fn signed(n: i128) -> Value {
        json!({"negative": n < 0, "magnitude": mag(n.unsigned_abs())})
    }

    fn vol(asset: &str, color: &str, input: u128, output: u128) -> Value {
        json!({"asset": asset, "color": color, "volumes": {"input": mag(input), "output": mag(output), "balance": signed(input as i128 - output as i128)}})
    }

    fn account(addr: &str, vols: Vec<Value>, meta: Value) -> Value {
        json!({"address": addr, "metadata": meta, "volumes": vols})
    }

    fn hits<'a>(a: &'a [crate::assertions::Recorded], msg: &str) -> Vec<&'a crate::assertions::Recorded> {
        a.iter().filter(|r| r.message == msg).collect()
    }

    fn ready(d: &Driver) {
        let st = d.s.ext::<ProbeState>();
        st.probe_ready.store(true, Ordering::SeqCst);
        st.bad_ready.store(true, Ordering::SeqCst);
    }

    fn ok_items(items: Vec<Value>) -> FakeReply {
        FakeReply::Outcome(Outcome { ok: true, items: Some(items), complete: true, ..Default::default() })
    }

    fn barrier_replies(start: u64, steps: &[u64]) -> Vec<FakeReply> {
        let mut c = start;
        let mut v = vec![FakeReply::Outcome(o(json!({"ok": true, "response": {"commitIndex": c.to_string()}})))];
        for s in steps {
            c += s;
            v.push(FakeReply::Outcome(o(json!({"ok": true, "response": {"commitIndex": c.to_string()}}))));
        }
        v
    }

    fn fast() -> StaleTiming {
        StaleTiming { rule_budget: Duration::from_secs(10), attempt: Duration::from_millis(50), read: Duration::from_millis(50), http_margin: Duration::from_millis(10) }
    }

    // ---- stale bounds bookkeeping

    #[test]
    fn increments_count_attempted_before_and_acked_after() {
        let (d, ft, _) = testkit::driver();
        testkit::reset();
        ft.script(
            "bucket",
            "Apply",
            vec![
                FakeReply::Outcome(o(json!({"ok": true, "response": {"logs": [{"sequence": "1"}]}}))),
                FakeReply::Outcome(Outcome::unknown("timeout")),
                FakeReply::Outcome(o(json!({"ok": false, "code": "Unavailable", "reason": "STALE_PROPOSAL"}))),
                FakeReply::Outcome(o(json!({"ok": false, "code": "AlreadyExists", "reason": "TRANSACTION_REFERENCE_CONFLICT"}))),
                FakeReply::Outcome(o(json!({"ok": false, "code": "NotFound", "reason": "LEDGER_NOT_FOUND"}))),
            ],
        );
        let r = Route::plain(0);
        let plan = StalePlan { iterations: vec![(r.clone(), r.clone()); 5] };
        let round = run_stale_round(&d, &plan, &fast());
        assert_eq!(round.iterations, 5);
        let c = *d.s.ext::<ProbeState>().counters.lock().unwrap();
        // ok and our own reference conflict prove a commit; unknown,
        // non-definitive and a definitive refusal only count as attempted.
        assert_eq!(c, StaleCounters { attempted: 5, acked: 2 });
        // Exactly one attempt per increment, keyed, forced, unit, unique reference.
        let applies = ft.calls_of("bucket", "Apply");
        assert_eq!(applies.len(), 5);
        let refs: BTreeSet<String> = applies.iter().map(|a| a["request"]["unsigned"]["requests"][0]["apply"]["action"]["createTransaction"]["reference"].as_str().unwrap().to_string()).collect();
        assert_eq!(refs.len(), 5);
        for a in &applies {
            let ct = &a["request"]["unsigned"]["requests"][0]["apply"]["action"]["createTransaction"];
            assert_eq!(ct["force"], true);
            assert_eq!(ct["postings"][0]["destination"], PROBE_ACCOUNT);
            assert_eq!(ct["postings"][0]["amount"], json!({"v0": "1"}));
            assert!(a["request"]["unsigned"]["idempotencyKey"].as_str().unwrap().starts_with("r-test-k"));
            assert_eq!(a["request"]["unsigned"]["requests"][0]["apply"]["ledger"], "r-probe-test");
        }
        for g in ft.calls_of("bucket", "GetAccount") {
            assert_eq!(g["consistency"], "stale");
            assert_eq!(g["request"]["address"], PROBE_ACCOUNT);
        }
        assert!(hits(&testkit::assertions(), "stale read of probe account succeeded").iter().any(|r| r.cond));
    }

    #[test]
    fn stale_read_bounds() {
        testkit::reset();
        let details = json!({});
        let read = |input: u128, output: u128| o(json!({"ok": true, "response": account(PROBE_ACCOUNT, vec![vol(PROBE_ASSET, "", input, output)], json!({}))}));
        assert_eq!(check_stale_read(&read(2, 0), 3, 4, &details), (true, true));
        assert_eq!(check_stale_read(&read(4, 0), 3, 4, &details), (true, false));
        assert_eq!(check_stale_read(&read(0, 0), 0, 4, &details), (true, false));
        let a = testkit::assertions();
        assert!(hits(&a, "stale read balance stays within attempted write bounds").iter().all(|r| r.cond));
        assert!(hits(&a, "stale read probe volumes are single-direction consistent").iter().all(|r| r.cond));
        // Above the attempted bound.
        testkit::reset();
        check_stale_read(&read(5, 0), 3, 4, &details);
        assert!(hits(&testkit::assertions(), "stale read balance stays within attempted write bounds").iter().any(|r| !r.cond));
        // A torn read (output on a receive-only account).
        testkit::reset();
        check_stale_read(&read(3, 1), 3, 4, &details);
        let a = testkit::assertions();
        assert!(hits(&a, "stale read probe volumes are single-direction consistent").iter().any(|r| !r.cond));
        assert!(hits(&a, "stale read balance stays within attempted write bounds").iter().all(|r| r.cond));
        // Beyond 128 bits is over any bound.
        testkit::reset();
        let big = json!({"magnitude": b64(&[1u8; 20])});
        let huge = o(json!({"ok": true, "response": {"volumes": [{"asset": PROBE_ASSET, "volumes": {"input": big, "output": {}, "balance": {"magnitude": big}}}]}}));
        check_stale_read(&huge, 3, 4, &details);
        assert!(hits(&testkit::assertions(), "stale read balance stays within attempted write bounds").iter().any(|r| !r.cond));
        // Absent field / invalid value / errors.
        testkit::reset();
        let absent = o(json!({"ok": true, "response": {"volumes": [{"asset": PROBE_ASSET, "volumes": {"input": {}, "balance": {}}}]}}));
        check_stale_read(&absent, 0, 1, &details);
        let invalid = o(json!({"ok": true, "response": {"volumes": [{"asset": PROBE_ASSET, "volumes": {"input": {"magnitude": "!!"}, "output": {}, "balance": {}}}]}}));
        check_stale_read(&invalid, 0, 1, &details);
        assert_eq!(check_stale_read(&o(json!({"ok": false, "code": "NotFound", "reason": "LEDGER_NOT_FOUND"})), 0, 1, &details), (false, false));
        check_stale_read(&o(json!({"ok": false, "code": "Unavailable"})), 0, 1, &details);
        check_stale_read(&Outcome::unknown("timeout"), 0, 1, &details);
        let a = testkit::assertions();
        let ab = hits(&a, "stale read returned volume with absent required field");
        assert_eq!(ab.len(), 1);
        assert_eq!(ab[0].details["hasOutput"], false);
        assert_eq!(hits(&a, "stale read returned invalid typed volume").len(), 1);
        assert!(hits(&a, "stale read GetAccount returned unexpected error").is_empty());
        check_stale_read(&o(json!({"ok": false, "code": "Internal", "message": "boom"})), 0, 1, &details);
        assert_eq!(hits(&testkit::assertions(), "stale read GetAccount returned unexpected error").len(), 1);
        // Another asset or a colored bucket is not the probe volume.
        testkit::reset();
        let other = o(json!({"ok": true, "response": account(PROBE_ACCOUNT, vec![vol("COIN", "", 9, 0), vol(PROBE_ASSET, "RED", 9, 0)], json!({}))}));
        assert_eq!(check_stale_read(&other, 0, 1, &details), (true, false));
        assert!(testkit::assertions().iter().all(|r| r.cond));
    }

    #[test]
    fn stale_round_is_bounded_with_a_slow_fake() {
        let (d, ft, _) = testkit::driver();
        let slow = Duration::from_millis(120);
        ft.set_default("bucket", "Apply", FakeReply::Delayed(slow, o(json!({"ok": true, "response": {}}))));
        ft.set_default("bucket", "GetAccount", FakeReply::Delayed(slow, o(json!({"ok": true, "response": {}}))));
        // Scaled: a 1 s budget, worst iteration 400 ms.
        let t = StaleTiming { rule_budget: Duration::from_millis(1000), attempt: Duration::from_millis(150), read: Duration::from_millis(150), http_margin: Duration::from_millis(50) };
        let r = Route::plain(0);
        let plan = StalePlan { iterations: vec![(r.clone(), r.clone()); 9] };
        let started = Instant::now();
        let round = run_stale_round(&d, &plan, &t);
        assert!(started.elapsed() <= t.rule_budget, "{:?}", started.elapsed());
        assert!(round.iterations >= 1 && round.iterations < 5, "{round:?}");
        assert_eq!(ft.calls_of("bucket", "Apply").len(), round.iterations);
        // A fast fake never runs more than 5 iterations, whatever the plan.
        let (d, ft, _) = testkit::driver();
        let round = run_stale_round(&d, &plan, &fast());
        assert_eq!(round.iterations, 5);
        assert_eq!(ft.calls_of("bucket", "Apply").len(), 5);
        let def = StaleTiming::default();
        assert_eq!((def.rule_budget, def.attempt), (Duration::from_secs(10), Duration::from_secs(2)));
        assert!(def.iteration_worst() < def.rule_budget);
    }

    #[test]
    fn stale_probe_rule_draws_at_most_five_iterations() {
        let (d, ft, _) = testkit::driver();
        testkit::run_rule(&d, stale_probe);
        let n = ft.calls_of("bucket", "Apply").len();
        assert!((1..=5).contains(&n), "{n}");
        assert_eq!(ft.calls_of("bucket", "GetAccount").len(), n);
    }

    // ---- malformed writes

    #[test]
    fn malformed_builders_produce_each_arm() {
        let ct = |v: &Value| v["apply"]["action"]["createTransaction"].clone();
        let a = malformed_request(MalformedArm::InvalidAsset, "r-bad-x", "ref1", '!');
        assert_eq!(ct(&a)["postings"][0]["asset"], INVALID_ASSET);
        assert_eq!(ct(&a)["reference"], "ref1");
        assert_eq!(a["apply"]["ledger"], "r-bad-x");
        for c in INVALID_ADDRESS_CHARS {
            let a = malformed_request(MalformedArm::InvalidAddressChar, "r-bad-x", "ref2", c);
            let dest = ct(&a)["postings"][0]["destination"].as_str().unwrap().to_string();
            assert!(dest.bytes().any(|b| !(b.is_ascii_alphanumeric() || b == b':' || b == b'_' || b == b'-')), "{dest}");
            assert_eq!(ct(&a)["postings"][0]["asset"], "USD/2");
        }
        let a = malformed_request(MalformedArm::EmptyPostings, "r-bad-x", "ref3", '!');
        assert_eq!(ct(&a)["postings"], json!([]));
        assert!(ct(&a).get("script").is_none());
        let a = malformed_request(MalformedArm::MetadataKeyTooLong, "r-bad-x", "ref4", '!');
        let keys: Vec<String> = ct(&a)["metadata"].as_object().unwrap().keys().cloned().collect();
        assert_eq!(keys.len(), 1);
        assert!(keys[0].len() > 256 && keys[0].bytes().all(|b| b == b'k'));
        assert_eq!(ct(&a)["postings"][0]["asset"], "USD/2");
        for arm in MALFORMED_ARMS {
            assert!(definitive(&o(json!({"ok": false, "code": "InvalidArgument", "reason": arm.expected_reason()}))), "{arm:?}");
        }
        assert_eq!(MalformedArm::EmptyPostings.expected_reason(), "VALIDATION");
    }

    #[test]
    fn malformed_verdicts() {
        let st = ProbeState::default();
        testkit::reset();
        let inv = o(json!({"ok": false, "code": "InvalidArgument", "reason": "VALIDATION"}));
        assert!(judge_malformed(&st, MalformedArm::InvalidAsset, "r-bad-x", "a", &inv));
        assert!(st.rejected.lock().unwrap().contains("a"));
        let a = testkit::assertions();
        assert!(hits(&a, "a malformed request is refused as invalid").iter().all(|r| r.cond));
        assert!(hits(&a, "definitive write rejection recorded for absence check").iter().all(|r| r.cond));
        testkit::reset();
        judge_malformed(&st, MalformedArm::MetadataKeyTooLong, "r-bad-x", "b", &inv);
        assert!(hits(&testkit::assertions(), "a malformed request is refused as invalid").iter().any(|r| !r.cond));
        testkit::reset();
        assert!(!judge_malformed(&st, MalformedArm::EmptyPostings, "r-bad-x", "c", &o(json!({"ok": true, "response": {}}))));
        assert_eq!(hits(&testkit::assertions(), "a malformed request never commits").len(), 1);
        testkit::reset();
        assert!(!judge_malformed(&st, MalformedArm::EmptyPostings, "r-bad-x", "d", &Outcome::unknown("timeout")));
        assert!(hits(&testkit::assertions(), "a malformed request is refused as invalid").is_empty());
        assert_eq!(st.rejected_total.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn malformed_rule_sends_keyed_requests_to_r_bad() {
        let (d, ft, _) = testkit::driver();
        ready(&d);
        testkit::reset();
        ft.set_default("bucket", "Apply", FakeReply::Outcome(o(json!({"ok": false, "code": "InvalidArgument", "reason": "VALIDATION"}))));
        testkit::run_rule(&d, malformed_write);
        let applies = ft.calls_of("bucket", "Apply");
        assert_eq!(applies.len(), 1, "a definitive outcome ends the retry loop");
        assert_eq!(applies[0]["request"]["unsigned"]["requests"][0]["apply"]["ledger"], "r-bad-test");
        assert!(applies[0]["request"]["unsigned"]["idempotencyKey"].as_str().unwrap().starts_with("r-test-k"));
        assert!(hits(&testkit::assertions(), "definitive write rejection recorded for absence check").iter().all(|r| r.cond));
        // Not ready: nothing is sent.
        let (d, ft, _) = testkit::driver();
        testkit::run_rule(&d, malformed_write);
        assert!(ft.calls_of("bucket", "Apply").is_empty());
    }

    // ---- double entry

    #[test]
    fn signed_sums_are_exact_with_colors() {
        let two255 = {
            let mut b = vec![0u8; 32];
            b[0] = 0x80;
            b
        };
        let big = |neg: bool, bytes: &[u8]| json!({"negative": neg, "magnitude": {"magnitude": b64(bytes)}});
        let accounts = vec![
            json!({"address": "world", "volumes": [
                {"asset": "USD/2", "volumes": {"input": {}, "output": {}, "balance": big(true, &two255)}},
                {"asset": "USD/2", "color": "RED", "volumes": {"input": {}, "output": {}, "balance": signed(-7)}},
            ]}),
            json!({"address": "a", "volumes": [{"asset": "USD/2", "volumes": {"input": {}, "output": {}, "balance": big(false, &two255)}}]}),
            json!({"address": "b", "volumes": [{"asset": "USD/2", "color": "RED", "volumes": {"input": {}, "output": {}, "balance": signed(5)}}]}),
            json!({"address": "c", "volumes": [{"asset": "USD/2", "color": "RED", "volumes": {"input": {}, "output": {}, "balance": signed(2)}}]}),
        ];
        testkit::reset();
        let mut sums = BTreeMap::new();
        sum_balances("l", &accounts, &mut sums);
        assert_eq!(sums.len(), 2);
        assert!(sums.values().all(SignedSum::is_zero));
        // A colored bucket is its own bucket: moving the RED +2 to uncolored breaks both.
        let mut sums2 = BTreeMap::new();
        let mut acc2 = accounts.clone();
        acc2[3] = json!({"address": "c", "volumes": [{"asset": "USD/2", "volumes": {"input": {}, "output": {}, "balance": signed(2)}}]});
        sum_balances("l", &acc2, &mut sums2);
        assert!(sums2.values().all(|s| !s.is_zero()));
        // 2^255 + 2^255 overflows 256 bits and stays exact.
        let mut s = SignedSum::default();
        let b = parse_signed_full(&big(false, &two255)).unwrap();
        s.add(&b);
        s.add(&b);
        assert!(!s.is_zero());
        let mut wide = vec![0u8; 33];
        wide[0] = 1;
        s.add(&parse_signed_full(&big(true, &wide)).unwrap());
        assert!(s.is_zero(), "{:?}", s.describe());
        // Carries across limbs.
        let mut s = SignedSum::default();
        s.add(&parse_signed_full(&big(false, &[0xff; 8])).unwrap());
        s.add(&parse_signed_full(&signed(1)).unwrap());
        let mut nine = vec![0u8; 9];
        nine[0] = 1;
        s.add(&parse_signed_full(&big(true, &nine)).unwrap());
        assert!(s.is_zero(), "{:?}", s.describe());
        // A non-zero bucket fails the Always.
        let mut obs = LedgerObs { accounts: 1, ..Default::default() };
        sum_balances("l", &[json!({"address": "x", "volumes": [{"asset": "COIN", "volumes": {"input": {}, "output": {}, "balance": signed(3)}}]})], &mut obs.sums);
        evaluate_ledger("l", &obs);
        assert!(hits(&testkit::assertions(), "double-entry: sum of balances should be 0").iter().any(|r| !r.cond));
        // Absent and invalid balances.
        testkit::reset();
        let mut sums = BTreeMap::new();
        sum_balances("l", &[json!({"address": "x", "volumes": [{"asset": "COIN", "volumes": {"input": {}}}, {"asset": "EUR/2", "volumes": {"balance": {"magnitude": {"magnitude": "%%"}}}}]})], &mut sums);
        let a = testkit::assertions();
        assert!(hits(&a, "double-entry: account volume balance field is absent").iter().any(|r| !r.cond));
        assert!(hits(&a, "double-entry: account volume balance is invalid").iter().any(|r| !r.cond));
    }

    #[test]
    fn aggregates_and_stats() {
        testkit::reset();
        check_aggregates("l", &json!({"volumes": [
            {"asset": "USD/2", "input": {"v0": "5", "v3": "1"}, "output": {"v0": "5", "v3": "1"}},
            {"asset": "COIN"},
        ]}));
        assert!(testkit::assertions().iter().all(|r| r.cond));
        testkit::reset();
        // Undecodable on both sides never compares equal (the old None == None).
        check_aggregates("l", &json!({"volumes": [{"asset": "X", "input": {"v9": "1"}, "output": {"v9": "1"}}, {"asset": "Y", "input": {"v0": "2"}, "output": {"v0": "1"}}]}));
        assert_eq!(hits(&testkit::assertions(), "aggregate double-entry: input must equal output").iter().filter(|r| !r.cond).count(), 2);
        testkit::reset();
        check_stats("l", &json!({"transactionCount": "3", "logCount": "5"}));
        check_stats("l", &json!({}));
        check_stats("l", &json!({"transactionCount": "6", "logCount": "5"}));
        let a = testkit::assertions();
        let s = hits(&a, "log count must be >= transaction count (logs include metadata, reverts, etc.)");
        assert_eq!(s.iter().map(|r| r.cond).collect::<Vec<_>>(), vec![true, true, false]);
    }

    // ---- fences

    #[test]
    fn fence_accepts_only_one_step_and_rereads_twice() {
        let seq = |v: Vec<Option<u64>>| {
            let mut it = v.into_iter();
            move || it.next().flatten()
        };
        let mut n = 0;
        assert_eq!(fenced(3, seq(vec![Some(10), Some(11)]), || { n += 1; Some(n) }), Fence::Quiescent { obs: 1, tries: 1 });
        let mut n = 0;
        let f = fenced(3, seq(vec![Some(10), Some(13), Some(13), Some(15), Some(20), Some(21)]), || { n += 1; Some(n) });
        assert_eq!(f, Fence::Quiescent { obs: 3, tries: 3 });
        let mut n = 0;
        let f = fenced(3, seq(vec![Some(1), Some(3), Some(3), Some(5), Some(5), Some(7), Some(7), Some(8)]), || { n += 1; Some(n) });
        assert_eq!(f, Fence::NotQuiescent { tries: 3 });
        assert_eq!(n, 3, "the first read and two re-reads");
        assert_eq!(fenced(1, seq(vec![Some(4), Some(4)]), || Some(())), Fence::NotQuiescent { tries: 1 });
        assert_eq!(fenced(1, seq(vec![Some(u64::MAX), Some(0)]), || Some(())), Fence::NotQuiescent { tries: 1 });
        assert_eq!(fenced(3, seq(vec![Some(1), None]), || Some(())), Fence::Aborted);
        assert_eq!(fenced(3, seq(vec![Some(1)]), || None::<()>), Fence::Aborted);
    }

    // ---- hooks over the fakes

    #[test]
    fn eventually_correct_checks_every_ledger_under_a_fence() {
        let (d, ft, _) = testkit::driver();
        testkit::reset();
        ft.script("bucket", "ListLedgers", vec![ok_items(vec![json!({"name": "o-test-l1"}), json!({"name": "gone", "deletedAt": "2026-01-01T00:00:00Z"}), json!({"name": "r-ns-test-1"})])]);
        let world = account("world", vec![vol("USD/2", "", 0, 10)], json!({}));
        let alice = account("users:1", vec![vol("USD/2", "", 10, 0)], json!({"tier": {"stringValue": "gold"}}));
        // l1 is read twice (first fence not quiescent), then r-ns once (empty).
        ft.script("bucket", "ListAccounts", vec![ok_items(vec![alice.clone(), world.clone()]), ok_items(vec![alice.clone(), world.clone()]), ok_items(vec![])]);
        let get = |a: &Value| FakeReply::Outcome(o(json!({"ok": true, "response": a})));
        ft.script("bucket", "GetAccount", vec![get(&alice), get(&world), get(&alice), get(&world)]);
        let mut b = barrier_replies(10, &[2, 1, 1]);
        b.extend(barrier_replies(40, &[1]));
        ft.script("bucket", "Barrier", b);
        ft.set_default("bucket", "GetLedgerStats", FakeReply::Outcome(o(json!({"ok": true, "response": {"transactionCount": "1", "logCount": "2"}}))));
        ft.set_default("bucket", "AggregateVolumes", FakeReply::Outcome(o(json!({"ok": true, "response": {"volumes": [{"asset": "USD/2", "input": {"v0": "10"}, "output": {"v0": "10"}}]}}))));
        let rep = run_eventually_correct(&d, 0, Instant::now() + Duration::from_secs(20), &EcPlan { samples: vec![0, 1, 2, 3] });
        assert_eq!((rep.ledgers, rep.checked), (2, 2), "{rep:?}");
        assert!(rep.completed);
        let a = testkit::assertions();
        assert!(a.iter().filter(|r| r.kind != "Sometimes").all(|r| r.cond), "{:#?}", a.iter().filter(|r| !r.cond).collect::<Vec<_>>());
        assert!(!hits(&a, "list/get balance pair verified matching").is_empty());
        assert_eq!(hits(&a, "can check all volumes for consistency").len(), 2);
        assert!(hits(&a, "barrier quiescence achieved").iter().all(|r| r.cond));
        assert!(!hits(&a, "aggregate double-entry: input must equal output").is_empty());
        assert_eq!(hits(&a, "log count must be >= transaction count (logs include metadata, reverts, etc.)").len(), 2);
        assert!(ft.calls_of("bucket", "GetLedgerStats").iter().all(|c| c["request"]["ledger"] != "gone"));
        assert!(ft.calls_of("bucket", "Barrier").iter().all(|c| c["route"] == "plain"));
    }

    #[test]
    fn eventually_correct_reports_divergence_and_metadata_only_under_quiescence() {
        let (d, ft, _) = testkit::driver();
        testkit::reset();
        let listed = account("users:1", vec![vol("USD/2", "", 10, 0)], json!({"tier": {"stringValue": "gold"}}));
        let got = account("users:1", vec![vol("USD/2", "", 11, 0)], json!({"tier": {"stringValue": "silver"}, "x": {"stringValue": "y"}}));
        ft.script("bucket", "ListLedgers", vec![ok_items(vec![json!({"name": "l"})])]);
        ft.set_default("bucket", "ListAccounts", ok_items(vec![listed.clone()]));
        ft.set_default("bucket", "GetAccount", FakeReply::Outcome(o(json!({"ok": true, "response": got}))));
        // Never quiescent: no verdict on the observation.
        ft.script("bucket", "Barrier", barrier_replies(1, &[2, 0, 2, 0, 2]));
        let plan = EcPlan { samples: vec![7] };
        let rep = run_eventually_correct(&d, 0, Instant::now() + Duration::from_secs(20), &plan);
        assert_eq!(rep.not_quiescent, 1);
        let a = testkit::assertions();
        assert!(hits(&a, "list/get balance divergence persisted past quiescence").is_empty());
        assert!(hits(&a, "double-entry: sum of balances should be 0").is_empty());
        assert!(hits(&a, "barrier quiescence achieved").iter().all(|r| !r.cond));
        assert_eq!(ft.calls_of("bucket", "ListAccounts").len(), 3, "first read and two re-reads");
        // Quiescent: divergence and metadata mismatches are reported.
        testkit::reset();
        ft.script("bucket", "ListLedgers", vec![ok_items(vec![json!({"name": "l"})])]);
        ft.script("bucket", "Barrier", barrier_replies(1, &[1]));
        run_eventually_correct(&d, 0, Instant::now() + Duration::from_secs(20), &plan);
        let a = testkit::assertions();
        assert_eq!(hits(&a, "list/get balance divergence persisted past quiescence").len(), 1);
        assert!(hits(&a, "list metadata value should match getaccount").iter().any(|r| !r.cond));
        assert!(hits(&a, "metadata key count should match between list and get").iter().any(|r| !r.cond));
        assert!(hits(&a, "double-entry: sum of balances should be 0").iter().any(|r| !r.cond));
    }

    #[test]
    fn eventually_correct_flags_unexpected_errors_and_respects_the_deadline() {
        let (d, ft, _) = testkit::driver();
        testkit::reset();
        ft.script("bucket", "ListLedgers", vec![ok_items(vec![json!({"name": "l"})])]);
        ft.set_default("bucket", "ListAccounts", FakeReply::Outcome(o(json!({"ok": false, "code": "Internal", "message": "x"}))));
        ft.set_default("bucket", "AggregateVolumes", FakeReply::Outcome(o(json!({"ok": false, "code": "Internal", "message": "x"}))));
        run_eventually_correct(&d, 0, Instant::now() + Duration::from_secs(20), &EcPlan { samples: vec![0] });
        let a = testkit::assertions();
        assert_eq!(hits(&a, "listAccounts returned unexpected error").len(), 1);
        assert_eq!(hits(&a, "AggregateVolumes returned unexpected error").len(), 1);
        // A ledger deleted meanwhile is skipped without verdict.
        testkit::reset();
        ft.script("bucket", "ListLedgers", vec![ok_items(vec![json!({"name": "l"})])]);
        ft.script("bucket", "GetLedgerStats", vec![FakeReply::Outcome(o(json!({"ok": false, "code": "NotFound", "reason": "LEDGER_NOT_FOUND"})))]);
        let rep = run_eventually_correct(&d, 0, Instant::now() + Duration::from_secs(20), &EcPlan { samples: vec![0] });
        assert_eq!(rep.skipped, 1);
        // A deadline already passed: no verdict, incompletion is reported.
        testkit::reset();
        let rep = run_eventually_correct(&d, 0, Instant::now(), &EcPlan { samples: vec![0] });
        assert!(!rep.completed);
        assert!(hits(&testkit::assertions(), "eventually_correct completes within its quiet budget").iter().all(|r| !r.cond));
    }

    #[test]
    fn quiesce_writes_idles_and_reads_with_its_own_deadline() {
        let (d, ft, _) = testkit::driver();
        ready(&d);
        testkit::reset();
        ft.script("bucket", "Apply", vec![FakeReply::Outcome(o(json!({"ok": false, "code": "Unavailable"}))), FakeReply::Outcome(o(json!({"ok": true, "response": {}})))]);
        ft.script("bucket", "ListTransactions", vec![ok_items(vec![])]);
        let plan = QuiescePlan { dest: "users:3".into(), amount: 7, idle: Duration::from_millis(20), write_routes: vec![Route::plain(1), Route::lib(2), Route::plain(0)], backoffs_ms: vec![10, 10, 10] };
        d.s.ext::<ProbeState>().note_rejected("r-test-bad0");
        let rep = run_quiesce(&d, 1, Instant::now() + Duration::from_secs(45), &plan);
        assert_eq!(rep, QuiesceReport { absence_checked: true, write_acked: true, read_done: true, read_ok: true });
        let applies = ft.calls_of("bucket", "Apply");
        assert_eq!(applies.len(), 2);
        assert_eq!(applies[0]["request"], applies[1]["request"], "same key and bytes on every try");
        assert_eq!(applies[0]["timeoutMs"], 8000);
        let ct = &applies[0]["request"]["unsigned"]["requests"][0]["apply"]["action"]["createTransaction"];
        assert_eq!(ct["force"], true);
        assert_eq!(ct["postings"][0]["destination"], "users:3");
        assert_eq!(applies[0]["request"]["unsigned"]["requests"][0]["apply"]["ledger"], "r-probe-test");
        let reads = ft.calls_of("bucket", "GetAccount");
        assert_eq!(reads.len(), 1);
        assert_eq!(reads[0]["timeoutMs"], 30000);
        assert_eq!(reads[0]["route"], "plain");
        assert_eq!(reads[0]["consistency"], "linearizable");
        // Nothing between the acked write and the read.
        let calls = ft.calls();
        let n = calls.len();
        assert_eq!(calls[n - 1]["method"], "GetAccount");
        assert_eq!(calls[n - 2]["method"], "Apply");
        let a = testkit::assertions();
        assert!(hits(&a, "linearizable read completes on a quiescent cluster").iter().all(|r| r.cond));
        assert_eq!(hits(&a, "quiescent linearizable read returned data").len(), 1);
        assert_eq!(hits(&a, "definitively rejected write never appears in the ledger").iter().filter(|r| r.cond).count(), 1);
        assert!(hits(&a, "quiesce_and_absence completes within its quiet budget").iter().all(|r| r.cond));
    }

    #[test]
    fn quiesce_read_completion_and_absence_violations() {
        assert!(quiesce_read_completed(&o(json!({"ok": true, "response": {}})), Duration::from_secs(31)));
        assert!(quiesce_read_completed(&o(json!({"ok": false, "code": "DeadlineExceeded"})), Duration::from_secs(3)));
        assert!(!quiesce_read_completed(&o(json!({"ok": false, "code": "DeadlineExceeded"})), Duration::from_secs(30)));
        assert!(!quiesce_read_completed(&Outcome::unknown("timeout"), Duration::from_secs(5)));
        assert!(quiesce_read_completed(&Outcome::unknown("transport"), Duration::from_secs(5)));

        let (d, ft, _) = testkit::driver();
        ready(&d);
        testkit::reset();
        d.s.ext::<ProbeState>().note_rejected("r-test-bad7");
        ft.script("bucket", "ListTransactions", vec![ok_items(vec![json!({"id": "0", "reference": "r-test-bad7"})])]);
        // The write never succeeds: no probe read, the hook is incomplete.
        ft.set_default("bucket", "Apply", FakeReply::Outcome(o(json!({"ok": false, "code": "Unavailable"}))));
        let plan = QuiescePlan { dest: "users:3".into(), amount: 7, idle: Duration::from_millis(10), write_routes: vec![Route::plain(0); 3], backoffs_ms: vec![5, 5, 5] };
        let rep = run_quiesce(&d, 0, Instant::now() + Duration::from_secs(45), &plan);
        assert!(!rep.write_acked && !rep.read_done);
        assert_eq!(ft.calls_of("bucket", "Apply").len(), 3, "at most 3 tries");
        assert!(ft.calls_of("bucket", "GetAccount").is_empty());
        let a = testkit::assertions();
        assert!(hits(&a, "definitively rejected write never appears in the ledger").iter().any(|r| !r.cond));
        assert_eq!(hits(&a, "a malformed request never commits").len(), 1);
        assert!(hits(&a, "linearizable read completes on a quiescent cluster").is_empty());
        assert!(hits(&a, "quiesce_and_absence completes within its quiet budget").iter().all(|r| !r.cond));
        // Not enough budget left for idle + the 30 s read: no write is sent.
        let (d, ft, _) = testkit::driver();
        let rep = run_quiesce(&d, 0, Instant::now() + Duration::from_secs(20), &plan);
        assert!(!rep.write_acked && ft.calls_of("bucket", "Apply").is_empty());
    }

    #[test]
    fn hooks_run_inside_a_quiet_period_without_the_oracle() {
        let (d, ft, fo) = testkit::driver();
        ready(&d);
        d.s.enter_degraded("test");
        testkit::reset();
        ft.set_default("bucket", "ListLedgers", ok_items(vec![]));
        ft.set_default("bucket", "ListTransactions", ok_items(vec![]));
        let hooks = quiet_hooks();
        let ec = hooks.iter().find(|h| h.name == "eventually_correct").unwrap();
        crate::quiet::run_quiet(&d, Some(ec), Some(Box::new(EcPlan { samples: vec![1] })));
        let qa = hooks.iter().find(|h| h.name == "quiesce_and_absence").unwrap();
        let plan = QuiescePlan { dest: "users:0".into(), amount: 1, idle: Duration::ZERO, write_routes: vec![Route::plain(0)], backoffs_ms: vec![1] };
        crate::quiet::run_quiet(&d, Some(qa), Some(Box::new(plan)));
        assert!(fo.calls("/quiet/begin").is_empty(), "degraded: begin skipped, ctx.begin is None");
        let a = testkit::assertions();
        assert!(hits(&a, "eventually_correct completes within its quiet budget").iter().any(|r| r.cond));
        assert!(hits(&a, "quiesce_and_absence completes within its quiet budget").iter().any(|r| r.cond));
        assert!(hits(&a, "linearizable read completes on a quiescent cluster").iter().all(|r| r.cond));
    }

    #[test]
    fn registries_and_budgets() {
        let names: Vec<String> = rules().into_iter().map(|r| r.name).collect();
        assert_eq!(names, vec!["stale_probe", "malformed_write"]);
        let all: Vec<String> = crate::rules::rules().into_iter().map(|r| r.name).collect();
        assert!(all.contains(&"stale_probe".to_string()) && all.contains(&"malformed_write".to_string()));
        let hooks = crate::rules::quiet_hooks();
        let qa = hooks.iter().find(|h| h.name == "quiesce_and_absence").unwrap();
        let ec = hooks.iter().find(|h| h.name == "eventually_correct").unwrap();
        assert_eq!((qa.budget, ec.budget), (Duration::from_secs(45), Duration::from_secs(90)));
        assert!(!qa.needs_oracle && !ec.needs_oracle);
        assert!(crate::rules::setup_hooks().iter().any(|h| h.name == "probes_setup"));
        // Absence listing + one 8 s write try + the longest idle + the read fit the budget.
        assert!(ABSENCE_BUDGET + QUIESCE_WRITE_TIMEOUT + HTTP_MARGIN + Duration::from_secs(8) + QUIESCE_READ_TIMEOUT + HTTP_MARGIN <= qa.budget + Duration::from_secs(15));
    }

    #[test]
    fn plans_draw_from_the_domains_and_names_are_never_reserved() {
        let (d, _, _) = testkit::driver();
        testkit::with_tc_n(50, |tc| {
            let p = plan_quiesce(&d, tc);
            let p = p.downcast_ref::<QuiescePlan>().unwrap();
            assert!(p.amount >= 1);
            assert!(QUIESCE_IDLE_SECS.contains(&p.idle.as_secs()));
            assert!(p.dest.starts_with("users:") && !p.dest.contains("own-"));
            assert_eq!(p.write_routes.len(), QUIESCE_WRITE_TRIES);
            let e = plan_eventually_correct(&d, tc);
            assert_eq!(e.downcast_ref::<EcPlan>().unwrap().samples.len(), EC_SAMPLES);
        });
        for n in [probe_ledger(&d.s), bad_ledger(&d.s)] {
            assert!(n.starts_with("r-"), "{n}: foreign to the oracle");
        }
        assert!(!PROBE_ACCOUNT.contains("own-"));
    }

    #[test]
    fn setup_creates_both_ledgers_and_tolerates_existing_ones() {
        let (d, ft, _) = testkit::driver();
        ft.script("bucket", "Apply", vec![FakeReply::Outcome(o(json!({"ok": true, "response": {}}))), FakeReply::Outcome(o(json!({"ok": false, "code": "AlreadyExists", "reason": "LEDGER_ALREADY_EXISTS"})))]);
        probes_setup(&d);
        let st = d.s.ext::<ProbeState>();
        assert!(st.probe_ready.load(Ordering::SeqCst) && st.bad_ready.load(Ordering::SeqCst));
        let names: Vec<Value> = ft.calls_of("bucket", "Apply").iter().map(|a| a["request"]["unsigned"]["requests"][0]["createLedger"]["name"].clone()).collect();
        assert_eq!(names, vec![json!("r-probe-test"), json!("r-bad-test")]);
    }
}
