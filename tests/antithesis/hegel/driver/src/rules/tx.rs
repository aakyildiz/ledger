//! Task 02: transaction writes on oracle-tracked ledgers (DESIGN §3, §4.5–4.9,
//! §6.5): the transaction order generators, the mixed-bulk rules (`tx_bulk`
//! over every family's generators, `tx_local_bulk` over this family's), the
//! rule-owned deterministic probes on `o-<run>-l2` (transient batches,
//! reference reuse, the drain chain, 256-bit boundaries), the fresh-reference
//! race, and untracked stress bulks on `r-stress-<run>` (sent from a job).
//!
//! Every tracked write is validated by the oracle. Rust re-asserts an old
//! main-driver literal only when the outcome is deterministic from reserved
//! state the rule owns and definitive (COMMON.md).

use crate::clients::Outcome;
use crate::draw;
use crate::jobs::Job;
use crate::machine::{rule, Driver};
use crate::read::{unresolved, LedgerSel, ViewReq};
use crate::setup::SetupHook;
use crate::wire::{as_u64, str_field, U256};
use crate::write::{compose_bulk, BulkCtx, OrderGen, UntrackedSend, WritePlan, WriteResult};
use hegel::stateful::ConcurrentRule;
use hegel::TestCase;
use serde_json::{json, Map, Value};
use std::collections::{BTreeMap, VecDeque};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

pub const FAMILY: &str = "tx";
pub const SKIP_REF_CONFLICT: &str = "ERROR_REASON_TRANSACTION_REFERENCE_CONFLICT";

/// Definitive reasons that make the rest of a deterministic rule
/// inconclusive (DESIGN §3.1 scope; the oracle still judges the write).
pub const INCONCLUSIVE: [&str; 4] = ["LEDGER_DELETED", "LEDGER_NOT_FOUND", "LEDGER_IN_MIRROR_MODE", "ACCOUNT_NOT_MATCHING_TYPE"];

/// The drain-probe chain budget (four sequential tracked writes).
#[cfg(not(test))]
pub const DRAIN_CHAIN_BUDGET: Duration = Duration::from_secs(12);
#[cfg(test)]
pub const DRAIN_CHAIN_BUDGET: Duration = Duration::from_millis(400);

/// Stress bulks: per-attempt timeout and total budget (in a job).
pub const STRESS_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(60);
pub const STRESS_BUDGET: Duration = Duration::from_secs(180);
/// At most this many posting templates are drawn per stress bulk; the bulk
/// cycles them (each order still carries its own unique reference). Drawing
/// all 5000 postings would cost ~30 000 hegel draws in one rule.
pub const STRESS_TEMPLATES: usize = 64;
/// At most this many stress orders are submitted per run, which bounds
/// `r-stress-<run>` (never deleted).
pub const STRESS_ORDER_CAP: usize = 100_000;
/// `tx_setup`'s retry deadline (the setup budget is shared by every hook).
pub const SETUP_DEADLINE: Duration = Duration::from_secs(20);

pub fn rules() -> Vec<ConcurrentRule<Driver>> {
    vec![
        rule("tx_bulk", 14.0, tx_bulk),
        rule("tx_local_bulk", 2.0, tx_local_bulk),
        rule("tx_transient", 1.5, tx_transient),
        rule("tx_reference_race", 1.0, tx_reference_race),
        rule("tx_reference_reuse", 0.8, tx_reference_reuse),
        rule("tx_drain_probe", 0.8, tx_drain_probe),
        rule("tx_boundary", 0.6, tx_boundary),
        rule("tx_stress", 0.4, tx_stress),
    ]
}

pub fn order_gens() -> Vec<OrderGen> {
    vec![
        OrderGen { name: "tx_chaos", weight: 10.0, applies: taper_applies, has_opt_in: true, gen: gen_chaos },
        OrderGen { name: "tx_drain_ephemeral", weight: 2.5, applies: taper_applies, has_opt_in: false, gen: gen_drain_ephemeral },
        OrderGen { name: "tx_dup_ref_skip", weight: 0.6, applies: taper_applies, has_opt_in: false, gen: gen_dup_ref_skip },
        OrderGen { name: "tx_rejected", weight: 0.6, applies: taper_applies, has_opt_in: false, gen: gen_rejected },
        OrderGen { name: "tx_skip_fresh", weight: 0.3, applies: taper_applies, has_opt_in: false, gen: gen_skip_fresh },
    ]
}

pub fn setup_hooks() -> Vec<SetupHook> {
    vec![SetupHook { name: "tx_setup", run: tx_setup }]
}

// ---------------------------------------------------------------- wire helpers

/// One posting (with a color, which `wire::Posting` does not carry).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Leg {
    pub source: String,
    pub destination: String,
    pub amount: U256,
    pub asset: String,
    pub color: String,
}

impl Leg {
    pub fn new(source: &str, destination: &str, amount: impl Into<U256>, asset: &str, color: &str) -> Leg {
        Leg { source: source.into(), destination: destination.into(), amount: amount.into(), asset: asset.into(), color: color.into() }
    }

    pub fn to_json(&self) -> Value {
        let mut p = json!({"source": self.source, "destination": self.destination, "amount": self.amount.to_json(), "asset": self.asset});
        if !self.color.is_empty() {
            p["color"] = json!(self.color);
        }
        p
    }

    pub fn from_json(v: &Value) -> Option<Leg> {
        Some(Leg {
            source: v.get("source")?.as_str()?.to_string(),
            destination: v.get("destination")?.as_str()?.to_string(),
            amount: U256::from_json(v.get("amount").unwrap_or(&json!({})))?,
            asset: v.get("asset")?.as_str()?.to_string(),
            color: str_field(v, "color").to_string(),
        })
    }
}

/// A `CreateTransaction` payload builder.
#[derive(Clone, Debug, Default)]
pub struct Create {
    pub postings: Vec<Leg>,
    pub reference: Option<String>,
    pub force: bool,
    pub metadata: Option<Value>,
    pub account_metadata: Option<(String, Value)>,
    pub timestamp: Option<u64>,
    pub skip: bool,
}

impl Create {
    pub fn request(&self, ledger: &str) -> Value {
        let mut ct = Map::new();
        if !self.postings.is_empty() {
            ct.insert("postings".into(), Value::Array(self.postings.iter().map(Leg::to_json).collect()));
        }
        if let Some(r) = &self.reference {
            ct.insert("reference".into(), json!(r));
        }
        if self.force {
            ct.insert("force".into(), json!(true));
        }
        if let Some(m) = &self.metadata {
            ct.insert("metadata".into(), m.clone());
        }
        if let Some((addr, m)) = &self.account_metadata {
            ct.insert("accountMetadata".into(), json!({ addr.as_str(): {"values": m} }));
        }
        if let Some(ts) = self.timestamp {
            ct.insert("timestamp".into(), json!({"data": ts.to_string()}));
        }
        let mut apply = json!({"ledger": ledger, "action": {"createTransaction": Value::Object(ct)}});
        if self.skip {
            apply["skippableReasons"] = json!([SKIP_REF_CONFLICT]);
        }
        json!({ "apply": apply })
    }
}

/// An account `addMetadata` order.
pub fn add_account_metadata(ledger: &str, addr: &str, meta: Value) -> Value {
    json!({"apply": {"ledger": ledger, "action": {"addMetadata": {"target": {"account": {"addr": addr}}, "metadata": meta}}}})
}

/// 1–2 model keys `k0..k3` with typed values.
pub fn meta_map(tc: &TestCase) -> Value {
    let n = 1 + draw::pick(tc, 2);
    let mut m = Map::new();
    for _ in 0..n {
        m.insert(draw::meta_key_model(tc), draw::meta_value(tc));
    }
    Value::Object(m)
}

/// The request carries a skippable-reason opt-in.
pub fn has_opt_in(req: &Value) -> bool {
    req.pointer("/apply/skippableReasons").and_then(Value::as_array).is_some_and(|a| !a.is_empty())
}

/// The ledger a request targets (any arm: `{"<arm>": {"ledger": …}}`).
pub fn ledger_of(req: &Value) -> Option<String> {
    req.as_object()?.values().find_map(|arm| arm.get("ledger").and_then(Value::as_str)).map(str::to_string)
}

/// A decimal string as a `U256` (`None` above 2^256 − 1 or not decimal).
pub fn u256_from_decimal(s: &str) -> Option<U256> {
    if s.is_empty() || !s.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let mut v = [0u64; 4];
    for b in s.bytes() {
        let mut carry = (b - b'0') as u128;
        for limb in v.iter_mut() {
            let cur = (*limb as u128) * 10 + carry;
            *limb = cur as u64;
            carry = cur >> 64;
        }
        if carry != 0 {
            return None;
        }
    }
    Some(U256 { v })
}

fn tx_count(view: &Value) -> u64 {
    view.get("txCount").and_then(as_u64).unwrap_or(0)
}

/// `applies` of every transaction gen: never at `txCount ≥ 4000` (DESIGN §3.2).
pub fn taper_applies(ctx: &BulkCtx) -> bool {
    tx_count(ctx.ledger_view) < 4000
}

/// The in-gen half of the taper: below 500 always, 500–2000 half,
/// 2000–4000 one in eight.
pub fn taper_draw(tc: &TestCase, ctx: &BulkCtx) -> bool {
    taper_count_draw(tc, tx_count(ctx.ledger_view))
}

/// The taper bands of DESIGN §3.2 for a ledger holding `n` transactions.
pub fn taper_count_draw(tc: &TestCase, n: u64) -> bool {
    match n {
        n if n < 500 => true,
        n if n < 2000 => draw::chance(tc, 0.5),
        n if n < 4000 => draw::chance(tc, 0.125),
        _ => false,
    }
}

fn l2(d: &Driver) -> String {
    format!("o-{}-l2", d.s.run)
}

/// The taper for the rules that commit on l2 (never deleted, DESIGN §3.1):
/// reads l2's `txCount` and applies the §3.2 bands; `None` means skip the
/// rule (also when the view is unavailable or l2 is not resolved).
fn l2_taper(d: &Driver, tc: &TestCase) -> Option<()> {
    let views = d.view(&[ViewReq::ledger(LedgerSel::Name(l2(d)))])?;
    let v = views.first()?;
    if unresolved(v) {
        return None;
    }
    taper_count_draw(tc, tx_count(v)).then_some(())
}

fn stress_ledger(d: &Driver) -> String {
    format!("r-stress-{}", d.s.run)
}

/// A seek probe in the reference space `o-<run>-ref<N>` (lexicographic).
fn ref_probe(tc: &TestCase, run: &str) -> String {
    format!("o-{run}-ref{}", draw::int_in(tc, 0, 99_999))
}

/// The `entry` of a found `seek` answer.
fn seek_entry(views: &[Value]) -> Option<&Value> {
    let v = views.first()?;
    if v.get("found").and_then(Value::as_bool) != Some(true) {
        return None;
    }
    v.get("entry").filter(|e| e.is_object())
}

fn seek_ref(d: &Driver, ledger: &str, probe: &str) -> Option<(String, u64)> {
    let views = d.view(&[ViewReq::seek(LedgerSel::Name(ledger.to_string()), "txByRef", probe, None)])?;
    let e = seek_entry(&views)?;
    let r = e.get("reference")?.as_str()?.to_string();
    let id = e.get("id").and_then(as_u64)?;
    if r.is_empty() {
        return None;
    }
    Some((r, id))
}

// ---------------------------------------------------------------- order gens

/// tx_chaos: 1–4 postings (source `world` 1/2 else a pool address,
/// destination a pool address, self-transfers kept), asset, color, amount
/// (0 allowed), force 1/8, a unique reference, tx metadata 1/2, a backdated
/// timestamp 1/2, account metadata on the last destination 1/3. With the
/// opt-in: `REFERENCE_CONFLICT`, and half rebind the reference to an
/// existing one (M/apply_surface.go maybeAddSkippableReason).
pub fn gen_chaos(d: &Driver, tc: &TestCase, ctx: &BulkCtx, opt_in: bool) -> Option<Value> {
    if !taper_draw(tc, ctx) {
        return None;
    }
    let mut c = chaos_payload(d, tc);
    if opt_in {
        c.skip = true;
        if draw::chance(tc, 0.5) {
            let probe = ref_probe(tc, ctx.run);
            if let Some((r, _)) = seek_ref(d, ctx.ledger, &probe) {
                c.reference = Some(r);
            }
        }
    }
    Some(c.request(ctx.ledger))
}

fn chaos_payload(d: &Driver, tc: &TestCase) -> Create {
    let force = draw::chance(tc, 0.125);
    let n = 1 + draw::pick(tc, 4);
    let mut postings = Vec::with_capacity(n);
    for _ in 0..n {
        let source = if draw::chance(tc, 0.5) { "world".to_string() } else { draw::pool_address(tc) };
        let destination = draw::pool_address(tc);
        let asset = draw::asset(tc);
        let color = draw::color(tc);
        let amount = draw::amount(tc);
        postings.push(Leg::new(&source, &destination, amount, asset, &color));
    }
    let metadata = draw::chance(tc, 0.5).then(|| meta_map(tc));
    let timestamp = draw::chance(tc, 0.5).then(|| draw::int_in(tc, 1, 1_500_000_000_000_000));
    let account_metadata = draw::chance(tc, 1.0 / 3.0).then(|| (postings[n - 1].destination.clone(), meta_map(tc)));
    Create { postings, reference: Some(d.s.next_ref()), force, metadata, account_metadata, timestamp, skip: false }
}

/// tx_drain_ephemeral: forced drain of an ephemeral cell's full positive
/// balance to `world` (M/actions.go generateDrainTransaction).
pub fn gen_drain_ephemeral(d: &Driver, tc: &TestCase, ctx: &BulkCtx, _opt_in: bool) -> Option<Value> {
    if !taper_draw(tc, ctx) {
        return None;
    }
    let probe = draw::pool_address(tc);
    let views = d.view(&[ViewReq::seek(LedgerSel::Name(ctx.ledger.to_string()), "volumes", &probe, Some("ephemeral-positive"))])?;
    let e = seek_entry(&views)?;
    let input = u256_from_decimal(str_field(e, "input"))?;
    let output = u256_from_decimal(str_field(e, "output"))?;
    let balance = input.checked_sub(&output).filter(|b| !b.is_zero())?;
    let leg = Leg::new(str_field(e, "address"), "world", balance, str_field(e, "asset"), str_field(e, "color"));
    Some(Create { postings: vec![leg], force: true, ..Default::default() }.request(ctx.ledger))
}

/// tx_dup_ref_skip: a create rebound to an existing reference with the
/// REFERENCE_CONFLICT opt-in (the skip that fires).
pub fn gen_dup_ref_skip(d: &Driver, tc: &TestCase, ctx: &BulkCtx, _opt_in: bool) -> Option<Value> {
    if !taper_draw(tc, ctx) {
        return None;
    }
    let probe = ref_probe(tc, ctx.run);
    let dest = draw::pool_address(tc);
    let (r, _) = seek_ref(d, ctx.ledger, &probe)?;
    let c = Create { postings: vec![Leg::new("world", &dest, 1u64, draw::ASSETS[0], "")], reference: Some(r), skip: true, ..Default::default() };
    Some(c.request(ctx.ledger))
}

/// tx_rejected: empty postings (VALIDATION, admission), a duplicate of an
/// existing reference without skip, or `2 × 2^255` into one account
/// (VOLUME_OVERFLOW).
pub fn gen_rejected(d: &Driver, tc: &TestCase, ctx: &BulkCtx, _opt_in: bool) -> Option<Value> {
    if !taper_draw(tc, ctx) {
        return None;
    }
    match draw::int_in(tc, 0, 2) {
        0 => Some(Create::default().request(ctx.ledger)),
        1 => {
            let probe = ref_probe(tc, ctx.run);
            let dest = draw::pool_address(tc);
            let (r, _) = seek_ref(d, ctx.ledger, &probe)?;
            Some(Create { postings: vec![Leg::new("world", &dest, 1u64, draw::ASSETS[0], "")], reference: Some(r), ..Default::default() }.request(ctx.ledger))
        }
        _ => {
            let dest = draw::pool_address(tc);
            let half = U256::pow2(255);
            Some(Create { postings: vec![Leg::new("world", &dest, half, draw::ASSETS[0], ""), Leg::new("world", &dest, half, draw::ASSETS[0], "")], ..Default::default() }.request(ctx.ledger))
        }
    }
}

/// tx_skip_fresh: a fresh reference with the REFERENCE_CONFLICT opt-in (the
/// skip that never fires).
pub fn gen_skip_fresh(d: &Driver, tc: &TestCase, ctx: &BulkCtx, _opt_in: bool) -> Option<Value> {
    if !taper_draw(tc, ctx) {
        return None;
    }
    let mut c = chaos_payload(d, tc);
    c.skip = true;
    Some(c.request(ctx.ledger))
}

// ---------------------------------------------------------------- reference book

/// Acked transactions with a reference on l2 seen by this family's bulks:
/// reference → (id, postings). Lets `tx_reference_reuse` mutate the original
/// body; bounded.
#[derive(Default)]
pub struct RefBook {
    inner: Mutex<(BTreeMap<String, (u64, Vec<Leg>)>, VecDeque<String>)>,
}

pub const REF_BOOK_CAP: usize = 1024;

impl RefBook {
    pub fn record(&self, reference: &str, id: u64, postings: Vec<Leg>) {
        let mut g = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        if g.0.insert(reference.to_string(), (id, postings)).is_none() {
            g.1.push_back(reference.to_string());
        }
        while g.1.len() > REF_BOOK_CAP {
            if let Some(old) = g.1.pop_front() {
                g.0.remove(&old);
            }
        }
    }
    pub fn get(&self, reference: &str) -> Option<(u64, Vec<Leg>)> {
        self.inner.lock().unwrap_or_else(|e| e.into_inner()).0.get(reference).cloned()
    }
}

/// Records the created transactions with a reference on `ledger` of a
/// successful Apply response.
pub fn record_refs(d: &Driver, ledger: &str, out: &Outcome) {
    let Some(resp) = out.response.as_ref().filter(|_| out.is_ok()) else { return };
    let book = d.s.ext::<RefBook>();
    for log in crate::wire::apply_logs(resp) {
        if log.pointer("/payload/apply/ledgerName").and_then(Value::as_str) != Some(ledger) {
            continue;
        }
        let Some(tx) = crate::wire::created_transaction(&log) else { continue };
        let r = str_field(tx, "reference");
        let Some(id) = tx.get("id").and_then(as_u64) else { continue };
        let legs: Option<Vec<Leg>> = tx.get("postings").and_then(Value::as_array).map(|ps| ps.iter().filter_map(Leg::from_json).collect());
        if !r.is_empty() {
            book.record(r, id, legs.unwrap_or_default());
        }
    }
}

/// A body that differs from the original's postings: the last destination
/// moved to `fees` (or away from it), or the last amount + 1 (wrapping at
/// 2^256 − 1 to 0). Always unequal to `orig` when `orig` is non-empty.
pub fn conflict_postings(orig: &[Leg], amount_variant: bool) -> Vec<Leg> {
    let mut out = orig.to_vec();
    let Some(last) = out.last_mut() else {
        return vec![Leg::new("world", "fees", 1u64, draw::ASSETS[0], "")];
    };
    if amount_variant {
        last.amount = last.amount.checked_add(&U256::from(1)).unwrap_or(U256::ZERO);
    } else if last.destination == "fees" {
        last.destination = "users:0".into();
    } else {
        last.destination = "fees".into();
    }
    out
}

// ---------------------------------------------------------------- outcomes

fn inconclusive(o: &Outcome) -> bool {
    !o.is_ok() && INCONCLUSIVE.contains(&o.reason.as_str())
}

/// The OrderSkipped facts of the first log of an Apply response: (reason
/// derived from the details arm, ledger, reference, existing tx id).
/// `existingTransactionId` is an `optional` proto field, so the server
/// always renders it (even 0); an absent one is `None` and never matches.
pub fn skipped_facts(resp: &Value) -> Option<(String, String, String, Option<u64>)> {
    let logs = crate::wire::apply_logs(resp);
    let sk = crate::wire::order_skipped(logs.first()?)?;
    let details = sk.get("details")?.as_object()?;
    let (arm, facts) = details.iter().next()?;
    let reason = match arm.as_str() {
        "transactionReferenceConflict" => "TRANSACTION_REFERENCE_CONFLICT".to_string(),
        other => other.to_string(),
    };
    Some((reason, str_field(facts, "ledger").to_string(), str_field(facts, "reference").to_string(), facts.get("existingTransactionId").and_then(as_u64)))
}

// ---------------------------------------------------------------- bulk rules

pub fn tx_bulk(d: &Driver, tc: TestCase) {
    d.guarded("tx_bulk", &tc, |d, tc| bulk_body(d, tc, &crate::rules::order_gens(), "tx_bulk"));
}

pub fn tx_local_bulk(d: &Driver, tc: TestCase) {
    d.guarded("tx_local_bulk", &tc, |d, tc| bulk_body(d, tc, &order_gens(), "tx_local_bulk"));
}

fn live_list(v: &Value) -> Vec<String> {
    v.get("live").and_then(Value::as_array).map(|a| a.iter().filter_map(|x| x.as_str().map(str::to_string)).collect()).unwrap_or_default()
}

/// The mixed bulk: 1 ledger (5/6) or 2–3 distinct live ledgers (1/6),
/// `bulk_size_mixed` orders composed over `gens`, an optional trailing
/// successor after a final opt-in, and 1/10 a doomed final order.
fn bulk_body(d: &Driver, tc: &TestCase, gens: &[OrderGen], rule_name: &'static str) -> Option<()> {
    if !d.s.oracle_ok() {
        return None;
    }
    let pick = draw::ledger_pick(tc);
    let views = d.view(&[ViewReq::ledgers(), ViewReq::ledger(LedgerSel::Pick(pick))])?;
    let live = live_list(views.first()?);
    let first_view = views.get(1)?.clone();
    if live.is_empty() || unresolved(&first_view) {
        return None;
    }
    let first = first_view.get("ledger").and_then(Value::as_str)?.to_string();
    let mut ledgers = vec![(first.clone(), first_view)];
    if live.len() >= 2 && draw::chance(tc, 1.0 / 6.0) {
        let n = if live.len() >= 3 && draw::chance(tc, 0.5) { 3 } else { 2 };
        let mut rest: Vec<String> = live.iter().filter(|l| **l != first).cloned().collect();
        let mut extra = Vec::new();
        while extra.len() + 1 < n && !rest.is_empty() {
            extra.push(rest.remove(draw::pick(tc, rest.len())));
        }
        let ev = d.view(&extra.iter().map(|l| ViewReq::ledger(LedgerSel::Name(l.clone()))).collect::<Vec<_>>())?;
        for (l, v) in extra.into_iter().zip(ev) {
            if !unresolved(&v) {
                ledgers.push((l, v));
            }
        }
    }
    // Every chosen ledger gets at least one order, so a multi-ledger pick
    // really spans its ledgers.
    let size = draw::bulk_size_mixed(tc).max(ledgers.len());
    let mut counts = vec![1usize; ledgers.len()];
    for _ in ledgers.len()..size {
        counts[draw::pick(tc, ledgers.len())] += 1;
    }
    let mut reqs = Vec::new();
    for ((l, v), n) in ledgers.iter().zip(&counts) {
        if *n > 0 {
            reqs.extend(compose_bulk(d, tc, gens, l, v, *n));
        }
    }
    if let Some(last) = reqs.last() {
        if has_opt_in(last) && draw::chance(tc, 0.5) {
            let l = ledger_of(last).unwrap_or_else(|| first.clone());
            let addr = draw::pool_address(tc);
            reqs.push(add_account_metadata(&l, &addr, meta_map(tc)));
        }
    }
    let doomed = draw::chance(tc, 0.1);
    if doomed {
        let l = reqs.last().and_then(ledger_of).unwrap_or_else(|| first.clone());
        let amount = draw::amount_positive(tc);
        let asset = draw::asset(tc);
        let void = draw::owned_address(&d.s, "t-0", "void");
        reqs.push(Create { postings: vec![Leg::new(&void, "world", amount, asset, "")], ..Default::default() }.request(&l));
    }
    if reqs.is_empty() {
        return None;
    }
    let n = reqs.len();
    let mut plan = WritePlan::drawn(FAMILY, reqs, d, tc);
    plan.hint = json!({"rule": rule_name, "doomed": doomed, "ledgers": ledgers.iter().map(|(l, _)| l.clone()).collect::<Vec<_>>()});
    let unit = d.s.slots.try_acquire_up_to(1)?.take_one()?;
    let res = d.dispatch(plan, unit);
    if let WriteResult::Definitive { outcome, .. } = &res {
        check_sometimes!(outcome.is_ok() && n >= 2, "a multi-order bulk commits", json!({"rule": rule_name, "orders": n}));
        if doomed {
            check_sometimes!(!outcome.is_ok(), "mid-failing atomic bulk definitively rejected", json!({"rule": rule_name, "reason": outcome.reason}));
        }
        record_refs(d, &l2(d), outcome);
    }
    Some(())
}

// ---------------------------------------------------------------- l2 probes

/// Dispatches one plan on a fresh unit; `None` when no unit is free.
fn dispatch_one(d: &Driver, plan: WritePlan) -> Option<WriteResult> {
    let unit = d.s.slots.try_acquire_up_to(1)?.take_one()?;
    Some(d.dispatch(plan, unit))
}

/// tx_transient: on l2, balanced = fund `t-2:own-tr<N>` from world and drain
/// it to `t-0:own-trs<N>` in two forced creates of one Apply; unbalanced =
/// the fund alone (MD/parallel_driver_transient_accounts).
pub fn tx_transient(d: &Driver, tc: TestCase) {
    d.guarded("tx_transient", &tc, |d, tc| {
        if !d.s.oracle_ok() {
            return None;
        }
        l2_taper(d, tc)?;
        let balanced = draw::chance(tc, 0.5);
        let amount = draw::amount_positive(tc);
        let asset = draw::asset(tc);
        let color = draw::color(tc);
        let ledger = l2(d);
        let n = d.s.next_own();
        let tr = format!("t-2:own-tr{n}");
        let trs = format!("t-0:own-trs{n}");
        let mut reqs = vec![Create { postings: vec![Leg::new("world", &tr, amount, asset, &color)], force: true, ..Default::default() }.request(&ledger)];
        if balanced {
            reqs.push(Create { postings: vec![Leg::new(&tr, &trs, amount, asset, &color)], force: true, ..Default::default() }.request(&ledger));
        }
        let mut plan = WritePlan::drawn(FAMILY, reqs, d, tc);
        plan.hint = json!({"rule": "tx_transient", "balanced": balanced});
        let res = dispatch_one(d, plan)?;
        let WriteResult::Definitive { outcome, .. } = &res else { return Some(()) };
        if inconclusive(outcome) {
            return Some(());
        }
        let details = json!({"ledger": ledger, "account": tr, "amount": amount.to_string(), "outcome": outcome.summary()});
        if balanced {
            check_sometimes!(outcome.is_ok(), "balanced transient batch should succeed", details);
            if outcome.is_ok() {
                check_reachable!("balanced transient batch succeeded", details);
            }
        } else if outcome.is_ok() {
            check_unreachable!("unbalanced transient batch should fail", details);
        } else {
            check_always_or_unreachable!(outcome.reason == "TRANSIENT_ACCOUNT_NON_ZERO", "unbalanced transient batch should return TRANSIENT_ACCOUNT_NON_ZERO", details);
            check_reachable!("transient account non-zero path exercised", details);
        }
        Some(())
    });
}

/// tx_reference_race: 2..=7 concurrent creates of one fresh reserved
/// reference on l2, distinct keys, all from `world`
/// (MD/parallel_driver_reference_race, half 1).
pub fn tx_reference_race(d: &Driver, tc: TestCase) {
    d.guarded("tx_reference_race", &tc, |d, tc| {
        if !d.s.oracle_ok() {
            return None;
        }
        l2_taper(d, tc)?;
        let mut g = d.s.slots.try_acquire_up_to(7)?;
        if g.len() < 2 {
            return None;
        }
        let k = draw::int_in(tc, 2, g.len() as u64) as usize;
        g.split(k);
        let ledger = l2(d);
        let reference = d.s.next_own_ref("ref");
        let mut plans = Vec::with_capacity(k);
        for i in 0..k {
            let dest = draw::users_address(tc);
            let amount = draw::amount(tc);
            let asset = draw::asset(tc);
            let c = Create { postings: vec![Leg::new("world", &dest, amount, asset, "")], reference: Some(reference.clone()), ..Default::default() };
            let mut plan = WritePlan::drawn(FAMILY, vec![c.request(&ledger)], d, tc);
            plan.hint = json!({"rule": "tx_reference_race", "racer": i, "of": k});
            plans.push(plan);
        }
        let results = d.dispatch_many(plans, g);
        let mut acked = 0usize;
        let mut conflicts = 0usize;
        let mut conclusive = true;
        for r in &results {
            match r {
                WriteResult::Definitive { outcome, .. } if outcome.is_ok() => acked += 1,
                WriteResult::Definitive { outcome, .. } if outcome.reason == "TRANSACTION_REFERENCE_CONFLICT" => conflicts += 1,
                WriteResult::Definitive { .. } => {}
                _ => conclusive = false,
            }
        }
        let details = json!({"ledger": ledger, "reference": reference, "racers": k, "acked": acked, "conflicts": conflicts});
        check_always!(acked <= 1, "at most one concurrent same-reference create is acked", details);
        if conclusive {
            check_sometimes!(acked == 1 && conflicts >= 1, "concurrent same-reference race produced a winner and a conflict loser", details);
        }
        Some(())
    });
}

/// tx_reference_reuse: resend an acked l2 reference with a different body
/// and a fresh key, half with the skip opt-in
/// (MD/parallel_driver_reference_conflict, …_skip).
pub fn tx_reference_reuse(d: &Driver, tc: TestCase) {
    d.guarded("tx_reference_reuse", &tc, |d, tc| {
        if !d.s.oracle_ok() {
            return None;
        }
        let ledger = l2(d);
        let probe = ref_probe(tc, &d.s.run);
        let skip = draw::chance(tc, 0.5);
        let amount_variant = draw::chance(tc, 0.5);
        let (reference, first_id) = seek_ref(d, &ledger, &probe)?;
        let orig = d.s.ext::<RefBook>().get(&reference).filter(|(id, _)| *id == first_id).map(|(_, p)| p).unwrap_or_default();
        // The reserved marker key also guarantees different business bytes
        // when the original postings are unknown (no generator writes `own-`).
        let c = Create {
            postings: conflict_postings(&orig, amount_variant),
            reference: Some(reference.clone()),
            force: true,
            metadata: Some(json!({"own-conflict": {"stringValue": "second"}})),
            skip,
            ..Default::default()
        };
        let mut plan = WritePlan::drawn(FAMILY, vec![c.request(&ledger)], d, tc);
        plan.hint = json!({"rule": "tx_reference_reuse", "skip": skip});
        let res = dispatch_one(d, plan)?;
        let WriteResult::Definitive { outcome, .. } = &res else { return Some(()) };
        if inconclusive(outcome) {
            return Some(());
        }
        let details = json!({"ledger": ledger, "reference": reference, "firstTxId": first_id, "outcome": outcome.summary()});
        if !skip {
            if outcome.is_ok() {
                check_unreachable!("duplicate reference should be rejected", details);
                return Some(());
            }
            check_always_or_unreachable!(outcome.reason == "TRANSACTION_REFERENCE_CONFLICT", "duplicate reference should return TRANSACTION_REFERENCE_CONFLICT", details);
            check_reachable!("reference conflict path exercised", details);
            return Some(());
        }
        if !outcome.is_ok() {
            check_unreachable!("skip-tolerant duplicate reference must not return a business error", details);
            return Some(());
        }
        let facts = outcome.response.as_ref().and_then(skipped_facts);
        check_always_or_unreachable!(facts.is_some(), "skip-tolerant duplicate reference must land as an OrderSkipped log", details);
        let Some((reason, f_ledger, f_ref, f_id)) = facts else { return Some(()) };
        check_always_or_unreachable!(reason == "TRANSACTION_REFERENCE_CONFLICT", "OrderSkipped reason must be TRANSACTION_REFERENCE_CONFLICT", json!({"reason": reason}));
        check_always_or_unreachable!(f_ref == reference, "OrderSkipped.context.reference must match the requested reference", json!({"context_reference": f_ref, "expected": reference}));
        check_always_or_unreachable!(f_ledger == ledger, "OrderSkipped.context.ledger must match the target ledger", json!({"context_ledger": f_ledger, "expected": ledger}));
        check_always_or_unreachable!(f_id == Some(first_id), "OrderSkipped.context.existingTransactionId must point to the first tx", json!({"context_existingTransactionId": f_id, "expected": first_id}));
        check_reachable!("reference-conflict skip path exercised", details);
        Some(())
    });
}

/// One step of the drain chain: a fresh unit, `run(remaining)`.
fn chain_step(d: &Driver, plan: WritePlan, deadline: Instant) -> Option<Outcome> {
    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        return None;
    }
    let unit = d.s.slots.try_acquire_up_to(1)?.take_one()?;
    let t = d.begin_write(plan, unit).ok()?;
    match t.run(remaining) {
        WriteResult::Definitive { outcome, .. } => Some(outcome),
        _ => None,
    }
}

/// tx_drain_probe: fund a reserved l2 account, drain it exactly, then a
/// unit overdraft without and with force (MD/parallel_driver_insufficient_funds).
pub fn tx_drain_probe(d: &Driver, tc: TestCase) {
    d.guarded("tx_drain_probe", &tc, |d, tc| {
        if !d.s.oracle_ok() {
            return None;
        }
        l2_taper(d, tc)?;
        let amount = draw::int_in(tc, 100, 10_099);
        let asset = draw::asset(tc);
        let ledger = l2(d);
        let n = d.s.next_own();
        let acct = format!("t-0:own-drain{n}");
        let sink = format!("t-0:own-sink{n}");
        let reference = d.s.next_own_ref("ref");
        let steps = [
            Create { postings: vec![Leg::new("world", &acct, amount, asset, "")], reference: Some(reference.clone()), force: true, ..Default::default() },
            Create { postings: vec![Leg::new(&acct, &sink, amount, asset, "")], ..Default::default() },
            Create { postings: vec![Leg::new(&acct, &sink, 1u64, asset, "")], ..Default::default() },
            Create { postings: vec![Leg::new(&acct, &sink, 1u64, asset, "")], force: true, ..Default::default() },
        ];
        let mut plans: Vec<WritePlan> = steps
            .iter()
            .enumerate()
            .map(|(i, c)| {
                let mut p = WritePlan::drawn(FAMILY, vec![c.request(&ledger)], d, tc);
                p.hint = json!({"rule": "tx_drain_probe", "step": i});
                p
            })
            .collect();
        let deadline = Instant::now() + DRAIN_CHAIN_BUDGET;
        let details = json!({"ledger": ledger, "account": acct, "amount": amount, "asset": asset});

        let fund = chain_step(d, plans.remove(0), deadline)?;
        if inconclusive(&fund) {
            return Some(());
        }
        if !fund.is_ok() {
            check_unreachable!("funding should not fail", json!({"details": details, "outcome": fund.summary()}));
            return Some(());
        }
        let drain = chain_step(d, plans.remove(0), deadline)?;
        if inconclusive(&drain) {
            return Some(());
        }
        if !drain.is_ok() {
            check_unreachable!("exact balance transfer should succeed", json!({"details": details, "outcome": drain.summary()}));
            return Some(());
        }
        let over = chain_step(d, plans.remove(0), deadline)?;
        if inconclusive(&over) {
            return Some(());
        }
        if over.is_ok() {
            check_unreachable!("overdraft without Force should fail", details);
            return Some(());
        }
        check_always_or_unreachable!(over.reason == "INSUFFICIENT_FUNDS", "overdraft should return INSUFFICIENT_FUNDS", json!({"details": details, "outcome": over.summary()}));
        check_reachable!("insufficient funds path exercised", details);
        let forced = chain_step(d, plans.remove(0), deadline)?;
        if inconclusive(&forced) {
            return Some(());
        }
        if !forced.is_ok() {
            check_unreachable!("overdraft with Force should succeed", json!({"details": details, "outcome": forced.summary()}));
        }
        Some(())
    });
}

/// tx_boundary: unforced `{1, 2^63, 2^256−1}` from a fresh unfunded reserved
/// account, or the overflow pair `2 × 2^255` into a reserved account
/// (MD/parallel_driver_definitive_errors amount half; M/actions.go
/// overflowTransaction).
pub fn tx_boundary(d: &Driver, tc: TestCase) {
    d.guarded("tx_boundary", &tc, |d, tc| {
        if !d.s.oracle_ok() {
            return None;
        }
        let shape = draw::int_in(tc, 0, 3);
        let asset = draw::asset(tc);
        let ledger = l2(d);
        let (c, wide) = match shape {
            0..=2 => {
                let amount = match shape {
                    0 => U256::from(1),
                    1 => U256::from(1u64 << 63),
                    _ => U256::MAX,
                };
                let void = draw::owned_address(&d.s, "t-0", "void");
                (Create { postings: vec![Leg::new(&void, "world", amount, asset, "")], ..Default::default() }, shape == 2)
            }
            _ => {
                let ovf = draw::owned_address(&d.s, "t-0", "ovf");
                let half = U256::pow2(255);
                (Create { postings: vec![Leg::new("world", &ovf, half, asset, ""), Leg::new("world", &ovf, half, asset, "")], force: true, ..Default::default() }, true)
            }
        };
        let mut plan = WritePlan::drawn(FAMILY, vec![c.request(&ledger)], d, tc);
        plan.hint = json!({"rule": "tx_boundary", "shape": shape});
        let res = dispatch_one(d, plan)?;
        if let WriteResult::Definitive { outcome, .. } = &res {
            if wide && !inconclusive(outcome) {
                check_sometimes!(!outcome.is_ok(), "a 256-bit boundary amount is definitively rejected", json!({"shape": shape, "outcome": outcome.summary()}));
            }
        }
        Some(())
    });
}

// ---------------------------------------------------------------- stress

/// The stress bulk's requests: `size` forced world → pool creates cycling
/// the drawn templates, each with a unique reference.
pub fn stress_requests(ledger: &str, tag: &str, templates: &[Leg], size: usize) -> Vec<Value> {
    (0..size)
        .map(|i| {
            let leg = templates[i % templates.len()].clone();
            Create { postings: vec![leg], reference: Some(format!("{tag}-{i}")), force: true, ..Default::default() }.request(ledger)
        })
        .collect()
}

/// Admission state of the stress jobs: at most one queued or running (each
/// holds a job thread for up to `STRESS_BUDGET`, and the two job threads
/// also serve other families' quiet-blocking jobs), and at most
/// `STRESS_ORDER_CAP` orders per run.
#[derive(Default)]
pub struct StressGate {
    pub inflight: AtomicUsize,
    pub orders: AtomicUsize,
}

impl StressGate {
    fn open(&self) -> bool {
        self.inflight.load(Ordering::SeqCst) == 0 && self.orders.load(Ordering::SeqCst) < STRESS_ORDER_CAP
    }
}

/// Releases the stress slot when the job ends (also on unwind).
struct StressSlot(Arc<StressGate>);

impl Drop for StressSlot {
    fn drop(&mut self) {
        self.0.inflight.fetch_sub(1, Ordering::SeqCst);
    }
}

/// tx_stress: a geometric 1..5000 untracked bulk on `r-stress-<run>`, sent
/// from a job (MD/parallel_driver_transactions). Skipped while a stress job
/// is queued or running, and once the per-run order cap is reached.
pub fn tx_stress(d: &Driver, tc: TestCase) {
    d.guarded("tx_stress", &tc, |d, tc| {
        let gate = d.s.ext::<StressGate>();
        if !gate.open() {
            return None;
        }
        let size = draw::bulk_size_stress(tc);
        let nt = size.min(STRESS_TEMPLATES);
        let mut templates = Vec::with_capacity(nt);
        for _ in 0..nt {
            let dest = draw::pool_address(tc);
            let amount = draw::amount(tc);
            let asset = draw::asset(tc);
            templates.push(Leg::new("world", &dest, amount, asset, ""));
        }
        let schedule = draw::schedule(tc, &d.s, 8);
        let backoffs = draw::backoffs(tc, 8);
        let key = d.s.next_untracked_key();
        let ledger = stress_ledger(d);
        let requests = stress_requests(&ledger, &format!("{key}-ref"), &templates, size);
        let mut send = UntrackedSend::new(requests, Some(key.clone()));
        send.schedule = schedule;
        send.backoffs_ms = backoffs;
        send.attempt_timeout = STRESS_ATTEMPT_TIMEOUT;
        send.budget = STRESS_BUDGET;
        if gate.inflight.compare_exchange(0, 1, Ordering::SeqCst, Ordering::SeqCst).is_err() {
            return Some(()); // a concurrent tx_stress won the slot
        }
        let slot = StressSlot(gate.clone());
        gate.orders.fetch_add(size, Ordering::SeqCst);
        d.submit_job(Job {
            name: "tx-stress",
            blocks_quiet: false,
            run: Box::new(move |d: &Driver| {
                let _slot = slot;
                stress_job(d, send, size, &key)
            }),
        });
        Some(())
    });
}

fn stress_job(d: &Driver, send: UntrackedSend, size: usize, key: &str) {
    let r = d.send_untracked(send);
    let out = &r.outcome;
    if !crate::clients::definitive(out) {
        return;
    }
    if out.reason == "LEDGER_NOT_FOUND" {
        // tx_setup ran out of time: create the ledger for the next bulk.
        create_stress_ledger(d);
    }
    let details = json!({"key": key, "size": size, "outcome": out.summary()});
    if out.is_ok() {
        let logs = out.response.as_ref().map(|resp| crate::wire::apply_logs(resp).len()).unwrap_or(0);
        check_always_or_unreachable!(logs == size, "bulk Apply should return one log per request", json!({"key": key, "size": size, "logs": logs}));
    }
    check_sometimes!(out.is_ok() || out.reason == "EXECUTION_PLAN_TOO_LARGE", "should be able to create bulk transactions", details);
}

/// One keyed, idempotent `createLedger` of `r-stress-<run>`; true when it
/// exists afterwards.
fn create_stress_ledger_with(d: &Driver, key: &str) -> (bool, Outcome) {
    let ledger = stress_ledger(d);
    let mut send = UntrackedSend::new(vec![json!({"createLedger": {"name": ledger}})], Some(key.to_string()));
    send.schedule = (0..d.s.clients.len().max(1)).map(crate::clients::Route::lib).collect();
    let r = d.send_untracked(send);
    (r.outcome.is_ok() || r.outcome.reason == "LEDGER_ALREADY_EXISTS", r.outcome)
}

fn create_stress_ledger(d: &Driver) -> bool {
    create_stress_ledger_with(d, &d.s.next_untracked_key()).0
}

/// Setup hook: creates `r-stress-<run>` (untracked, keyed, retried for up
/// to `SETUP_DEADLINE`; no draws). A stress job that still finds no ledger
/// creates it.
pub fn tx_setup(d: &Driver) {
    let ledger = stress_ledger(d);
    let key = d.s.next_untracked_key();
    let deadline = Instant::now() + SETUP_DEADLINE;
    loop {
        let (ready, outcome) = create_stress_ledger_with(d, &key);
        if ready {
            println!("setup: tx_setup: {ledger} ready");
            return;
        }
        if Instant::now() >= deadline {
            println!("setup: tx_setup: {ledger} not created: {}", outcome.summary());
            return;
        }
        std::thread::sleep(Duration::from_millis(500));
    }
}

#[cfg(test)]
mod tests;
