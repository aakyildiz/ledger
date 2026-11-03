//! Task 07: indexes, typed metadata, driver-polled readiness, retype
//! windows, index introspection and inspection.
//!
//! Rules: `index_bootstrap`, `metadata_index_bootstrap`, `schema_bulk`, `index_op`,
//! `typed_metadata_cycle`, `index_poll`, `index_introspect`,
//! `inspect_index`. Order gens: `schema_set`, `schema_remove`.
//! Every tracked verdict comes from the oracle (`indexcheck`); the driver
//! re-asserts only the deterministic `MD/parallel_driver_typed_metadata`
//! literals on its rule-owned key. The `INDEX_BUILDING` write exploration
//! lives in `write::explore_attempt`, on every `Apply` answer (the only
//! write-path `INDEX_BUILDING` is a `createQueryCheckpoint` while the audit
//! projection rebuilds).
use crate::clients::{CallOpts, Outcome, Route};
use crate::draw;
use crate::machine::{rule, Driver};
use crate::rules::filters::{active_metadata_index_keys, Target};
use crate::rules::lists::KeyListPlan;
use crate::read::{unresolved, LedgerSel, ViewReq};
use crate::write::{BulkCtx, OrderGen, WritePlan, WriteResult, IN_RULE_BUDGET};
use hegel::stateful::ConcurrentRule;
use hegel::TestCase;
use serde_json::{json, Value};
use std::collections::BTreeSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;
use std::time::{Duration, Instant};

const FAMILY: &str = "indexes";

pub fn rules() -> Vec<ConcurrentRule<Driver>> {
    vec![
        rule("index_bootstrap", 1.0, index_bootstrap),
        rule("metadata_index_bootstrap", 1.0, metadata_index_bootstrap),
        rule("schema_bulk", 0.6, schema_bulk),
        rule("index_op", 1.0, index_op),
        rule("typed_metadata_cycle", 0.5, typed_metadata_cycle),
        rule("index_poll", 1.5, index_poll),
        rule("index_introspect", 1.0, index_introspect),
        rule("inspect_index", 0.7, inspect_index),
    ]
}

pub fn order_gens() -> Vec<OrderGen> {
    vec![
        OrderGen { name: "schema_set", weight: 0.4, applies: always, has_opt_in: false, gen: gen_schema_set },
        OrderGen { name: "schema_remove", weight: 0.15, applies: always, has_opt_in: false, gen: gen_schema_remove },
    ]
}

fn always(_: &BulkCtx) -> bool {
    true
}

// ---------------------------------------------------------------- domains

/// The 11 `MetadataType`s.
pub const METADATA_TYPES: [&str; 11] = [
    "METADATA_TYPE_STRING",
    "METADATA_TYPE_INT64",
    "METADATA_TYPE_BOOL",
    "METADATA_TYPE_UINT64",
    "METADATA_TYPE_INT8",
    "METADATA_TYPE_INT16",
    "METADATA_TYPE_INT32",
    "METADATA_TYPE_UINT8",
    "METADATA_TYPE_UINT16",
    "METADATA_TYPE_UINT32",
    "METADATA_TYPE_DATETIME",
];

pub const TARGETS: [&str; 3] = ["TARGET_TYPE_ACCOUNT", "TARGET_TYPE_TRANSACTION", "TARGET_TYPE_LEDGER"];
const INDEXABLE_TARGETS: [&str; 2] = ["TARGET_TYPE_ACCOUNT", "TARGET_TYPE_TRANSACTION"];

/// The tx builtins the workload churns (`match.WorkloadTxBuiltins`).
const TX_BUILTINS: [&str; 7] = [
    "TX_BUILTIN_INDEX_REFERENCE",
    "TX_BUILTIN_INDEX_TIMESTAMP",
    "TX_BUILTIN_INDEX_INSERTED_AT",
    "TX_BUILTIN_INDEX_REVERTED_AT",
    "TX_BUILTIN_INDEX_ADDRESS",
    "TX_BUILTIN_INDEX_SOURCE_ADDRESS",
    "TX_BUILTIN_INDEX_DESTINATION_ADDRESS",
];

/// Bootstrap set (main-drivers §3.15).
const BOOTSTRAP_TX_BUILTINS: [&str; 5] = [
    "TX_BUILTIN_INDEX_REFERENCE",
    "TX_BUILTIN_INDEX_ADDRESS",
    "TX_BUILTIN_INDEX_SOURCE_ADDRESS",
    "TX_BUILTIN_INDEX_DESTINATION_ADDRESS",
    "TX_BUILTIN_INDEX_TIMESTAMP",
];

pub fn tx_builtin_id(b: &str) -> Value {
    json!({"txBuiltin": b})
}
pub fn asset_index_id() -> Value {
    json!({"accountBuiltin": "ACCT_BUILTIN_INDEX_ASSET"})
}
pub fn log_date_index_id() -> Value {
    json!({"logBuiltin": "LOG_BUILTIN_INDEX_DATE"})
}
pub fn metadata_index_id(target: &str, key: &str) -> Value {
    json!({"metadata": {"target": target, "key": key}})
}

/// The builtin workload indexes (`match.WorkloadIndexes`).
pub fn workload_builtins() -> Vec<Value> {
    let mut v = vec![asset_index_id(), log_date_index_id()];
    v.extend(TX_BUILTINS.iter().map(|b| tx_builtin_id(b)));
    v
}

pub fn create_index(ledger: &str, id: Value) -> Value {
    json!({"createIndex": {"ledger": ledger, "id": id}})
}
pub fn drop_index(ledger: &str, id: Value) -> Value {
    json!({"dropIndex": {"ledger": ledger, "id": id}})
}
pub fn set_field_type(ledger: &str, target: &str, key: &str, ty: &str) -> Value {
    json!({"setMetadataFieldType": {"ledger": ledger, "targetType": target, "key": key, "type": ty}})
}
pub fn remove_field_type(ledger: &str, target: &str, key: &str) -> Value {
    json!({"removeMetadataFieldType": {"ledger": ledger, "targetType": target, "key": key}})
}
pub fn account_string_meta(ledger: &str, addr: &str, key: &str, value: &str) -> Value {
    json!({"apply": {"ledger": ledger, "action": {"addMetadata": {
        "target": {"account": {"addr": addr}},
        "metadata": {key: {"stringValue": value}},
    }}}})
}

/// The declared (non-reserved) keys of one target in a core `ledger` view.
fn declared_keys(ledger_view: &Value, target: &str) -> Vec<String> {
    let field = match target {
        "TARGET_TYPE_ACCOUNT" => "account",
        "TARGET_TYPE_TRANSACTION" => "transaction",
        _ => "ledger",
    };
    let mut keys: Vec<String> = ledger_view["fieldTypes"][field].as_object().map(|m| m.keys().cloned().collect()).unwrap_or_default();
    keys.retain(|k| !k.starts_with("own-"));
    keys.sort();
    keys
}

/// The ledger's index pool: workload builtins plus one metadata index per
/// declared key of both indexable targets (`match.LedgerIndexPool`).
fn index_pool(ledger_view: &Value) -> Vec<Value> {
    let mut pool = workload_builtins();
    for t in INDEXABLE_TARGETS {
        for k in declared_keys(ledger_view, t) {
            pool.push(metadata_index_id(t, &k));
        }
    }
    pool
}

fn ledger_of(v: &Value) -> Option<String> {
    v.get("ledger").and_then(Value::as_str).map(str::to_string)
}

fn dispatch_one(d: &Driver, plan: WritePlan) -> Option<WriteResult> {
    let unit = d.s.slots.try_acquire_up_to(1)?.take_one()?;
    Some(d.dispatch(plan, unit))
}

// ---------------------------------------------------------------- order gens

fn gen_schema_set(_d: &Driver, tc: &TestCase, ctx: &BulkCtx, _opt_in: bool) -> Option<Value> {
    let target = *draw::one_of(tc, &TARGETS);
    let key = draw::meta_key_model(tc);
    let ty = *draw::one_of(tc, &METADATA_TYPES);
    Some(set_field_type(ctx.ledger, target, &key, ty))
}

fn gen_schema_remove(_d: &Driver, tc: &TestCase, ctx: &BulkCtx, _opt_in: bool) -> Option<Value> {
    let declared: Vec<(&str, String)> = TARGETS.iter().flat_map(|t| declared_keys(ctx.ledger_view, t).into_iter().map(move |k| (*t, k))).collect();
    if declared.is_empty() || draw::chance(tc, 0.25) {
        let target = *draw::one_of(tc, &TARGETS);
        return Some(remove_field_type(ctx.ledger, target, &draw::meta_key_model(tc)));
    }
    let (t, k) = &declared[draw::pick(tc, declared.len())];
    Some(remove_field_type(ctx.ledger, t, k))
}

// ---------------------------------------------------------------- index_bootstrap

/// Set once the bootstrap bulk reached a definitive outcome.
#[derive(Default)]
pub struct IndexBootstrap {
    pub done: AtomicBool,
}

pub fn bootstrap_requests(ledger: &str) -> Vec<Value> {
    let mut reqs: Vec<Value> = BOOTSTRAP_TX_BUILTINS.iter().map(|b| create_index(ledger, tx_builtin_id(b))).collect();
    reqs.push(create_index(ledger, asset_index_id()));
    reqs
}

/// Creates the builtin indexes on `o-<run>-l1` in one tracked bulk, while not
/// done, with `chance(1/2)` per run, so index builds overlap faults (no setup
/// hook, review-antithesis-fit A10).
pub fn index_bootstrap(d: &Driver, tc: TestCase) {
    d.guarded("index_bootstrap", &tc, |d, tc| {
        if !d.s.oracle_ok() {
            return None;
        }
        let st = d.s.ext::<IndexBootstrap>();
        if st.done.load(Ordering::SeqCst) || !draw::chance(tc, 0.5) {
            return None;
        }
        let ledger = format!("{}l1", d.s.tracked_prefix());
        let mut plan = WritePlan::drawn(FAMILY, bootstrap_requests(&ledger), d, tc);
        plan.hint = json!({"gen": "index_bootstrap"});
        let res = dispatch_one(d, plan)?;
        if let Some(out) = res.outcome() {
            if out.ok || out.reason == "INDEX_ALREADY_EXISTS" {
                st.done.store(true, Ordering::SeqCst);
            }
        }
        Some(())
    });
}

// ---------------------------------------------------------------- metadata_index_bootstrap

/// The metadata indexes kept on `o-<run>-l1`: (target, key, declared type).
/// Non-reserved, so list queries can filter on them and the per-target
/// metadata-index coverage probes and retype windows become reachable early
/// in every timeline.
pub const META_INDEX_SEEDS: [(&str, &str, &str); 2] = [("TARGET_TYPE_ACCOUNT", "k0", "METADATA_TYPE_STRING"), ("TARGET_TYPE_TRANSACTION", "k1", "METADATA_TYPE_INT64")];

/// Set once every seed index existed in the model (or its create reached a
/// definitive success / `INDEX_ALREADY_EXISTS`).
#[derive(Default)]
pub struct MetaIndexBootstrap {
    pub done: AtomicBool,
}

fn has_index(lv: &Value, canonical: &str) -> bool {
    lv["indexes"].as_array().is_some_and(|a| a.iter().any(|e| e["canonical"] == canonical))
}

/// The steps still missing on l1 (pure): per seed whose index the model
/// lacks, a `setMetadataFieldType` (unless already declared with the seed's
/// type), then a `createIndex`, each its own tracked write (a create must
/// see the declaration committed).
pub fn meta_bootstrap_steps(ledger: &str, lv: &Value) -> Vec<Value> {
    let field = |t: &str| if t == "TARGET_TYPE_ACCOUNT" { "account" } else { "transaction" };
    let mut out = Vec::new();
    for (t, k, ty) in META_INDEX_SEEDS {
        if has_index(lv, &format!("metadata:{t}:{k}")) {
            continue;
        }
        if lv["fieldTypes"][field(t)][k] != ty {
            out.push(set_field_type(ledger, t, k, ty));
        }
        out.push(create_index(ledger, metadata_index_id(t, k)));
    }
    out
}

/// Declares and indexes the seed keys on l1 as separate tracked writes:
/// `chance(1/2)` per run until done, then `chance(1/8)` to re-create a seed
/// index a drop or a field-type removal took away.
pub fn metadata_index_bootstrap(d: &Driver, tc: TestCase) {
    d.guarded("metadata_index_bootstrap", &tc, |d, tc| {
        if !d.s.oracle_ok() {
            return None;
        }
        let st = d.s.ext::<MetaIndexBootstrap>();
        let p = if st.done.load(Ordering::SeqCst) { 1.0 / 8.0 } else { 0.5 };
        if !draw::chance(tc, p) {
            return None;
        }
        let ledger = format!("{}l1", d.s.tracked_prefix());
        let views = d.view(&[ViewReq::ledger(LedgerSel::Name(ledger.clone()))])?;
        let lv = views.first()?;
        if unresolved(lv) {
            return None;
        }
        // Every step's plan is drawn before the first write (as in
        // `typed_metadata_cycle`), so the draws do not depend on outcomes.
        let plans: Vec<(bool, WritePlan)> = meta_bootstrap_steps(&ledger, lv)
            .into_iter()
            .enumerate()
            .map(|(i, req)| {
                let create = req.get("createIndex").is_some();
                let mut plan = WritePlan::drawn(FAMILY, vec![req], d, tc);
                plan.hint = json!({"gen": "metadata_index_bootstrap", "step": i});
                (create, plan)
            })
            .collect();
        let deadline = Instant::now() + CHAIN_BUDGET;
        for (create, plan) in plans {
            let now = Instant::now();
            if now >= deadline {
                return None;
            }
            let unit = d.s.slots.try_acquire_up_to(1)?.take_one()?;
            let ticket = d.begin_write(plan, unit).ok()?;
            let res = ticket.run((deadline - now).min(IN_RULE_BUDGET));
            let out = res.outcome()?; // a handoff ends the attempt
            if !(out.ok || (create && out.reason == "INDEX_ALREADY_EXISTS")) {
                return None;
            }
        }
        st.done.store(true, Ordering::SeqCst);
        Some(())
    });
}

// ---------------------------------------------------------------- schema_bulk

/// The types a retype probe re-declares an indexed key to (when it does not
/// re-declare the current type).
const RETYPE_TYPES: [&str; 4] = ["METADATA_TYPE_STRING", "METADATA_TYPE_INT64", "METADATA_TYPE_BOOL", "METADATA_TYPE_UINT64"];

/// A retype of a declared key with an active, non-reserved metadata index
/// (pure): the target, the key and the `setMetadataFieldType`. `same`
/// re-declares the current type (a window opens either way, model.go
/// `applySetMetadataFieldType`).
pub fn plan_retype(ledger: &str, lv: &Value, seed: u32, same: bool, ty: &str) -> Option<(Target, String, Value)> {
    let mut cands: Vec<(&str, String, String)> = Vec::new();
    for t in INDEXABLE_TARGETS {
        let field = if t == "TARGET_TYPE_ACCOUNT" { "account" } else { "transaction" };
        let declared = declared_keys(lv, t);
        for k in active_metadata_index_keys(lv, t) {
            if declared.contains(&k) {
                let cur = lv["fieldTypes"][field][k.as_str()].as_str().unwrap_or("").to_string();
                cands.push((t, k, cur));
            }
        }
    }
    if cands.is_empty() {
        return None;
    }
    let (t, k, cur) = &cands[seed as usize % cands.len()];
    let new_ty = if same && !cur.is_empty() { cur.as_str() } else { ty };
    let target = if *t == "TARGET_TYPE_ACCOUNT" { Target::Accounts } else { Target::Transactions };
    Some((target, k.clone(), set_field_type(ledger, t, k, new_ty)))
}

/// The family's local bulk: 1–2 orders of its own gens on a drawn ledger;
/// 1/4 of the time first a retype probe on l1: re-declare the type of a key
/// with an active metadata index, then list on that key right away, so a
/// page is served while its retype window is open.
pub fn schema_bulk(d: &Driver, tc: TestCase) {
    d.guarded("schema_bulk", &tc, |d, tc| {
        if !d.s.oracle_ok() {
            return None;
        }
        let pick = draw::ledger_pick(tc);
        let retype = draw::chance(tc, 0.25);
        if retype {
            let seed = draw::int_in(tc, 0, u32::MAX as u64) as u32;
            let same = draw::chance(tc, 0.5);
            let ty = *draw::one_of(tc, &RETYPE_TYPES);
            let probe = KeyListPlan::draw(d, tc);
            let ledger = format!("{}l1", d.s.tracked_prefix());
            let views = d.view(&[ViewReq::ledger(LedgerSel::Name(ledger.clone()))])?;
            let lv = views.first()?;
            if !unresolved(lv) {
                if let Some((target, key, req)) = plan_retype(&ledger, lv, seed, same, ty) {
                    let mut plan = WritePlan::drawn(FAMILY, vec![req], d, tc);
                    plan.hint = json!({"gen": "schema_bulk", "retype": key});
                    if dispatch_one(d, plan)?.is_success() {
                        probe.run(d, &ledger, target, &key);
                    }
                    return Some(());
                }
            }
        }
        let views = d.view(&[ViewReq::ledger(LedgerSel::Pick(pick))])?;
        let lv = views.first()?;
        let ledger = ledger_of(lv)?;
        let len = draw::int_in(tc, 1, 2) as usize;
        let reqs = crate::write::compose_bulk(d, tc, &order_gens(), &ledger, lv, len);
        if reqs.is_empty() {
            return None;
        }
        let mut plan = WritePlan::drawn(FAMILY, reqs, d, tc);
        plan.hint = json!({"gen": "schema_bulk"});
        dispatch_one(d, plan)?;
        Some(())
    });
}

// ---------------------------------------------------------------- index_op

/// Draws one index op on an aimed ledger (pure: views in, request out).
pub fn plan_index_op(tc: &TestCase, ledger: &str, ledger_view: &Value, gaps: &[Value]) -> Value {
    if draw::chance(tc, 1.0 / 16.0) {
        // Probe: an undeclared metadata field → METADATA_FIELD_NOT_IN_SCHEMA.
        let target = *draw::one_of(tc, &INDEXABLE_TARGETS);
        return create_index(ledger, metadata_index_id(target, &format!("undeclared-{}", draw::meta_key_model(tc))));
    }
    let p_create = if gaps.is_empty() { 1.0 / 8.0 } else { 1.0 / 3.0 };
    let pool = index_pool(ledger_view);
    if draw::chance(tc, p_create) {
        // Metadata gaps first (3/4): the builtins are bootstrapped, and a
        // metadata index is what the field-leaf queries need.
        let meta: Vec<&Value> = gaps.iter().filter(|g| g["id"].get("metadata").is_some()).collect();
        if !meta.is_empty() && draw::chance(tc, 0.75) {
            return create_index(ledger, meta[draw::pick(tc, meta.len())]["id"].clone());
        }
        if !gaps.is_empty() {
            return create_index(ledger, gaps[draw::pick(tc, gaps.len())]["id"].clone());
        }
        // No gap: every pool index exists, so this is a duplicate
        // (INDEX_ALREADY_EXISTS).
        return create_index(ledger, pool[draw::pick(tc, pool.len())].clone());
    }
    drop_index(ledger, pool[draw::pick(tc, pool.len())].clone())
}

pub fn index_op(d: &Driver, tc: TestCase) {
    d.guarded("index_op", &tc, |d, tc| {
        if !d.s.oracle_ok() {
            return None;
        }
        let pick = draw::ledger_pick(tc);
        let views = d.view(&[ViewReq::ledger(LedgerSel::Pick(pick)), ViewReq::named("indexGaps", json!({"ledgerPick": pick}))])?;
        let ledger = ledger_of(views.first()?)?;
        let gaps: Vec<Value> = views.get(1)?["gaps"][ledger.as_str()].as_array().cloned().unwrap_or_default();
        let req = plan_index_op(tc, &ledger, &views[0], &gaps);
        let mut plan = WritePlan::drawn(FAMILY, vec![req], d, tc);
        plan.hint = json!({"gen": "index_op", "gap": !gaps.is_empty()});
        dispatch_one(d, plan)?;
        Some(())
    });
}

// ---------------------------------------------------------------- typed_metadata_cycle

/// The cycles in flight (their `<N>`), so a cycle's leftover sweep never
/// touches the keys of a cycle that is still running.
#[derive(Default)]
pub struct TypedMeta {
    pub inflight: Mutex<BTreeSet<u64>>,
}

/// Removes a cycle from the in-flight set when it ends, however it ends.
struct Inflight<'a>(&'a TypedMeta, u64);

impl Drop for Inflight<'_> {
    fn drop(&mut self) {
        self.0.inflight.lock().unwrap_or_else(|e| e.into_inner()).remove(&self.1);
    }
}

/// The shared reserved key every cycle writes an unconvertible text to;
/// declared INT64 and indexed by whichever cycle first finds the index absent
/// from the model, never removed, so `inspect_index` can summarise (and page
/// through) an indexed key holding unconvertible values.
pub const BAD_KEY: &str = "own-bad";

/// At most this many leftover keys of cut-short cycles are removed per cycle.
pub const MAX_SWEEP: usize = 2;

/// The deterministic assertion a cycle step re-asserts on its outcome.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StepCheck {
    None,
    Retype,
    Int64OverUncoercible,
    BoolOverNumeric,
}

/// The `<N>` of a per-cycle reserved key (`own-score<N>` / `own-bad<N>`).
fn cycle_key_n(key: &str) -> Option<u64> {
    let rest = key.strip_prefix("own-score").or_else(|| key.strip_prefix("own-bad"))?;
    if rest.is_empty() || !rest.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    rest.parse().ok()
}

/// What a cycle learns from the owned `ledger` view of `l1` (pure): whether
/// the shared `own-bad` index exists, and the per-cycle keys of cycles no
/// longer in flight (cut short before their removes: their declaration, and
/// for `own-score<N>` its index, would otherwise stay on l1 for good).
pub fn cycle_plan_inputs(l1_owned: &Value, inflight: &BTreeSet<u64>) -> (bool, Vec<String>) {
    let bad_canon = format!("metadata:TARGET_TYPE_ACCOUNT:{BAD_KEY}");
    let bad_indexed = l1_owned["indexes"].as_array().is_some_and(|a| a.iter().any(|e| e["canonical"] == bad_canon.as_str()));
    let mut leftovers: Vec<String> = l1_owned["fieldTypes"]["account"]
        .as_object()
        .map(|m| m.keys().filter(|k| cycle_key_n(k).is_some_and(|n| !inflight.contains(&n))).cloned().collect())
        .unwrap_or_default();
    leftovers.sort();
    leftovers.truncate(MAX_SWEEP);
    (bad_indexed, leftovers)
}

/// The steps of one typed-metadata cycle on reserved entities (pure).
/// Removing a declared type drops its metadata index in the same order
/// (`applyRemoveMetadataFieldType`), so the removes are also the cleanup.
pub fn cycle_steps(ledger: &str, n: u64, value: u64, retypes: &[&str], bad_indexed: bool, leftovers: &[String]) -> Vec<(Value, StepCheck)> {
    let key = format!("own-score{n}");
    let bad = format!("own-bad{n}");
    let addr = format!("users:own-score{n}");
    let acct = "TARGET_TYPE_ACCOUNT";
    let mut s: Vec<(Value, StepCheck)> = leftovers.iter().map(|k| (remove_field_type(ledger, acct, k), StepCheck::None)).collect();
    s.push((set_field_type(ledger, acct, &key, "METADATA_TYPE_INT64"), StepCheck::None));
    s.push((create_index(ledger, metadata_index_id(acct, &key)), StepCheck::None));
    s.push((account_string_meta(ledger, &addr, &key, &value.to_string()), StepCheck::None));
    for t in retypes {
        s.push((set_field_type(ledger, acct, &key, t), StepCheck::Retype));
    }
    s.push((remove_field_type(ledger, acct, &key), StepCheck::None));
    // A fresh, undeclared key per cycle, so the INT64 declaration really
    // lands over an uncoercible stored value every time.
    s.push((account_string_meta(ledger, &addr, &bad, &format!("not-a-number-{value}")), StepCheck::None));
    s.push((set_field_type(ledger, acct, &bad, "METADATA_TYPE_INT64"), StepCheck::Int64OverUncoercible));
    s.push((set_field_type(ledger, acct, &key, "METADATA_TYPE_BOOL"), StepCheck::BoolOverNumeric));
    s.push((remove_field_type(ledger, acct, &key), StepCheck::None));
    s.push((remove_field_type(ledger, acct, &bad), StepCheck::None));
    // Inspection coverage last: a duplicate create (two cycles racing on
    // the absent index) fails here and costs no other step.
    s.push((account_string_meta(ledger, &addr, BAD_KEY, &format!("not-a-number-{value}")), StepCheck::None));
    if !bad_indexed {
        s.push((set_field_type(ledger, acct, BAD_KEY, "METADATA_TYPE_INT64"), StepCheck::None));
        s.push((create_index(ledger, metadata_index_id(acct, BAD_KEY)), StepCheck::None));
    }
    s
}

/// A definitive outcome that is not an Internal/Unknown failure
/// (`internal.IsClassified` restricted to definitive outcomes).
fn classified(o: &Outcome) -> bool {
    o.ok || !matches!(o.code.as_str(), "Internal" | "Unknown")
}

fn check_step(check: StepCheck, out: &Outcome, d: Value) {
    let c = classified(out);
    match check {
        StepCheck::None => {}
        StepCheck::Retype => check_always_or_unreachable!(c, "changing metadata type should not crash", d),
        StepCheck::Int64OverUncoercible => check_always_or_unreachable!(c, "declaring an int64 type over an uncoercible value should not crash", d),
        StepCheck::BoolOverNumeric => check_always_or_unreachable!(c, "declaring a bool type over a numeric value should not crash", d),
    }
}

const CHAIN_BUDGET: Duration = Duration::from_secs(12);

pub fn typed_metadata_cycle(d: &Driver, tc: TestCase) {
    d.guarded("typed_metadata_cycle", &tc, |d, tc| {
        if !d.s.oracle_ok() {
            return None;
        }
        // Draws: value, retype sequence, every step's plan.
        let value = draw::int_in(tc, 0, 999);
        let kinds = ["METADATA_TYPE_STRING", "METADATA_TYPE_BOOL", "METADATA_TYPE_INT64"];
        let retypes: Vec<&str> = (0..3).map(|_| *draw::one_of(tc, &kinds)).collect();
        let st = d.s.ext::<TypedMeta>();
        let n = d.s.next_own();
        let ledger = format!("{}l1", d.s.tracked_prefix());
        let inflight = {
            let mut g = st.inflight.lock().unwrap_or_else(|e| e.into_inner());
            g.insert(n);
            g.clone()
        };
        let _inflight = Inflight(&st, n);
        // The model, not a counter, decides whether the shared index is
        // still to be made (a cut-short first cycle must not lose it).
        let views = d.view(&[ViewReq::ledger(LedgerSel::Name(ledger.clone())).owned()])?;
        let lv = views.first()?;
        if unresolved(lv) {
            return None;
        }
        let (bad_indexed, leftovers) = cycle_plan_inputs(lv, &inflight);
        let steps = cycle_steps(&ledger, n, value, &retypes, bad_indexed, &leftovers);
        let plans: Vec<(WritePlan, StepCheck)> = steps
            .into_iter()
            .enumerate()
            .map(|(i, (req, check))| {
                let mut p = WritePlan::drawn(FAMILY, vec![req], d, tc);
                p.hint = json!({"gen": "typed_metadata_cycle", "step": i, "n": n});
                (p, check)
            })
            .collect();

        let deadline = Instant::now() + CHAIN_BUDGET;
        for (i, (plan, check)) in plans.into_iter().enumerate() {
            let now = Instant::now();
            if now >= deadline {
                return None;
            }
            let unit = d.s.slots.try_acquire_up_to(1)?.take_one()?;
            let ticket = d.begin_write(plan, unit).ok()?;
            let res = ticket.run((deadline - now).min(IN_RULE_BUDGET));
            let out = res.outcome()?.clone(); // a handoff ends the cycle
            check_step(check, &out, json!({"ledger": ledger, "n": n, "step": i, "outcome": out.summary()}));
            if !out.ok {
                return None;
            }
        }
        Some(())
    });
}

// ---------------------------------------------------------------- index_poll

pub const POLL_TIMEOUT: Duration = Duration::from_secs(2);

/// One readiness poll on one read ticket: per node a stale, node-pinned
/// `GetIndexStatus`; the per-node outcomes are posted as the outcome payload.
pub fn index_poll(d: &Driver, tc: TestCase) {
    d.guarded("index_poll", &tc, |d, tc| {
        if !d.s.oracle_ok() || d.s.nodes.is_empty() {
            return None;
        }
        let pick = draw::ledger_pick(tc);
        let all = d.s.nodes.clone();
        let nodes = if draw::chance(tc, 0.5) { all.clone() } else { vec![draw::one_of(tc, &all).clone()] };
        let client = draw::pick(tc, d.s.clients.len().max(1));
        let rt = d.read_begin("IndexStatusPoll", &[ViewReq::ledger(LedgerSel::Pick(pick))], json!({"nodes": nodes, "allNodes": all}))?;
        let ledger = ledger_of(rt.views().first()?)?;
        let req = json!({"ledger": ledger});
        let opts = CallOpts::stale().timeout(POLL_TIMEOUT);
        let per_node: Vec<Value> = nodes
            .iter()
            .map(|n| {
                let (_, out) = d.call(&Route::node(client, n), "bucket", "GetIndexStatus", req.clone(), &opts);
                json!({"node": n, "outcome": out.to_json()})
            })
            .collect();
        let out = Outcome { ok: true, response: Some(json!({"perNode": per_node})), ..Default::default() };
        rt.end(&req, &out, &Route::plain(client), &opts, json!({"allNodes": nodes.len() == all.len()}), json!({}));
        Some(())
    });
}

// ---------------------------------------------------------------- index_introspect

fn absent_ledger(d: &Driver, tc: &TestCase) -> String {
    format!("{}absent-{}", d.s.tracked_prefix(), draw::int_in(tc, 0, u32::MAX as u64))
}

/// An id to look up: 1/4 an id the workload never registers (its key drawn
/// before begin), else one of the ledger's pool (drawn after begin: an index
/// into the view, DESIGN §4.7).
fn lookup_id(tc: &TestCase, unregistered: bool, unregistered_key: &str, ledger_view: Option<&Value>) -> Value {
    match ledger_view {
        Some(lv) if !unregistered => {
            let pool = index_pool(lv);
            pool[draw::pick(tc, pool.len())].clone()
        }
        _ => metadata_index_id("TARGET_TYPE_ACCOUNT", &format!("unregistered-{unregistered_key}")),
    }
}

pub fn index_introspect(d: &Driver, tc: TestCase) {
    d.guarded("index_introspect", &tc, |d, tc| {
        if !d.s.oracle_ok() {
            return None;
        }
        // 0 list ledger, 1 list all, 2 list bucket, 3 GetIndex, 4 GetIndexStatus
        // (weights 2:1:1:2:2, as a uniform index into a list).
        let arm = *draw::one_of(tc, &[0usize, 0, 1, 2, 3, 3, 4, 4]);
        let absent = draw::chance(tc, 0.02);
        let absent_name = if absent { Some(absent_ledger(d, tc)) } else { None };
        let pick = draw::ledger_pick(tc);
        let unregistered = draw::chance(tc, 0.25);
        let unregistered_key = draw::meta_key_model(tc);
        let bucket_slot = arm == 3 && draw::chance(tc, 1.0 / 8.0);
        let route = draw::route(tc, &d.s);
        let opts = CallOpts::linearizable();

        let (kind, method): (&'static str, &str) = match arm {
            0..=2 => ("ListIndexes", "ListIndexes"),
            3 => ("GetIndex", "GetIndex"),
            _ => ("GetIndexStatus", "GetIndexEntryStatus"),
        };
        let needs_ledger = matches!(arm, 0 | 3 | 4) && !bucket_slot;
        let views: Vec<ViewReq> = if needs_ledger && absent_name.is_none() { vec![ViewReq::ledger(LedgerSel::Pick(pick))] } else { vec![] };
        let rt = d.read_begin(kind, &views, json!({}))?;
        let lv = rt.views().first().cloned();
        let ledger = match (&absent_name, &lv) {
            (Some(a), _) => a.clone(),
            (None, Some(v)) => ledger_of(v)?,
            (None, None) => String::new(),
        };
        let req = match arm {
            0 => json!({"scope": "SCOPE_LEDGER", "ledger": ledger}),
            1 => json!({"scope": "SCOPE_ALL"}),
            2 => json!({"scope": "SCOPE_BUCKET"}),
            _ => {
                let id = lookup_id(tc, unregistered, &unregistered_key, lv.as_ref());
                if bucket_slot {
                    json!({"id": id})
                } else {
                    json!({"ledger": ledger, "id": id})
                }
            }
        };
        let (_, out) = d.call(&route, "bucket", method, req.clone(), &opts);
        rt.end(&req, &out, &route, &opts, json!({"absentLedger": absent, "arm": arm}), json!({}));
        Some(())
    });
}

// ---------------------------------------------------------------- inspect_index

const INSPECT_MODES: [&str; 4] = [
    "INSPECT_INDEX_MODE_DISTINCT_VALUES",
    "INSPECT_INDEX_MODE_FACETS",
    "INSPECT_INDEX_MODE_SUMMARY",
    "INSPECT_INDEX_MODE_SUMMARY",
];

/// Task 99 sets `HEGEL_HAVE_CHECKPOINTS=1` once task 09's `checkpoints` view
/// exists (an unknown view is a `400 protocol`).
fn have_checkpoints() -> bool {
    std::env::var("HEGEL_HAVE_CHECKPOINTS").is_ok_and(|v| v.trim() == "1")
}

/// The resume cursor of an inspection page, if any.
pub fn inspect_next_cursor(resp: &Value) -> Option<String> {
    ["distinctValues", "facets"]
        .iter()
        .filter_map(|arm| resp.get(*arm).and_then(|p| p.get("nextCursor")).and_then(Value::as_str))
        .find(|c| !c.is_empty())
        .map(str::to_string)
}

pub fn inspect_index(d: &Driver, tc: TestCase) {
    d.guarded("inspect_index", &tc, |d, tc| {
        if !d.s.oracle_ok() {
            return None;
        }
        let tx = draw::chance(tc, 0.5);
        let (target, field) = if tx { ("TARGET_TYPE_TRANSACTION", "transaction") } else { ("TARGET_TYPE_ACCOUNT", "account") };
        let mode = *draw::one_of(tc, &INSPECT_MODES);
        let page_size = draw::page_size(tc);
        let undeclared = draw::chance(tc, 0.2);
        let undeclared_key = format!("undeclared-{}", draw::meta_key_model(tc));
        let checkpoint = have_checkpoints() && draw::chance(tc, 0.25);
        // Reads cannot disturb rule-owned entities: 1/4 inspect l1 with the
        // owned view, so the typed-metadata cycle's keys are inspected too.
        let owned_l1 = draw::chance(tc, 0.25);
        let absent = draw::chance(tc, 1.0 / 20.0);
        let absent_name = if absent { Some(absent_ledger(d, tc)) } else { None };
        let pick = draw::ledger_pick(tc);
        let route = draw::route(tc, &d.s);
        let opts = CallOpts::linearizable();

        let mut views = Vec::new();
        if !absent {
            views.push(if owned_l1 { ViewReq::ledger(LedgerSel::Name(format!("{}l1", d.s.tracked_prefix()))).owned() } else { ViewReq::ledger(LedgerSel::Pick(pick)) });
        }
        if checkpoint {
            views.push(ViewReq::named("checkpoints", json!({})));
        }
        let rt = d.read_begin("InspectIndex", &views, json!({}))?;
        let (ledger, key) = if let Some(a) = absent_name {
            (a, undeclared_key)
        } else {
            let lv = rt.views().first()?.clone();
            if unresolved(&lv) {
                return None;
            }
            let ledger = ledger_of(&lv)?;
            let mut keys: Vec<String> = lv["fieldTypes"][field].as_object().map(|m| m.keys().cloned().collect()).unwrap_or_default();
            keys.sort();
            if undeclared {
                (ledger, undeclared_key)
            } else if keys.is_empty() {
                rt.abandon();
                return None;
            } else {
                let k = keys[draw::pick(tc, keys.len())].clone();
                (ledger, k)
            }
        };
        let mut req = json!({"ledger": ledger, "targetType": target, "metadataKey": key, "mode": mode, "pageSize": page_size});
        if checkpoint {
            let live: Vec<Value> = rt.views().last().and_then(|v| v["live"].as_array().cloned()).unwrap_or_default();
            if !live.is_empty() {
                req["checkpointId"] = live[draw::pick(tc, live.len())].clone();
            }
        }
        let (_, out) = d.call(&route, "bucket", "InspectIndex", req.clone(), &opts);
        rt.end(&req, &out, &route, &opts, json!({"absentLedger": absent, "undeclared": undeclared, "ownedL1": owned_l1}), json!({}));

        // One resume through the opaque cursor, on a second ticket.
        let cursor = out.response.as_ref().filter(|_| out.ok).and_then(inspect_next_cursor)?;
        let rt2 = d.read_begin("InspectIndex", &[], json!({}))?;
        let mut req2 = req.clone();
        req2["cursor"] = json!(cursor);
        let (_, out2) = d.call(&route, "bucket", "InspectIndex", req2.clone(), &opts);
        rt2.end(&req2, &out2, &route, &opts, json!({"resume": true}), json!({}));
        Some(())
    });
}

// ---------------------------------------------------------------- tests

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testkit::{self, FakeReply};

    fn ok_outcome(resp: Value) -> Outcome {
        Outcome { ok: true, response: Some(resp), ..Default::default() }
    }

    fn reserved(v: &Value) -> bool {
        let s = v.to_string();
        s.contains("own-") || s.contains("-own")
    }

    #[test]
    fn index_bootstrap_bulk_shape_and_done_flag() {
        let (d, ft, fo) = testkit::driver();
        let st = d.s.ext::<IndexBootstrap>();
        for _ in 0..40 {
            if st.done.load(Ordering::SeqCst) {
                break;
            }
            testkit::run_rule(&d, index_bootstrap);
        }
        assert!(st.done.load(Ordering::SeqCst), "a definitive success sets done");
        let begins = fo.calls("/write/begin");
        assert_eq!(begins.len(), 1, "exactly one bootstrap bulk");
        let reqs = begins[0]["requests"].as_array().unwrap();
        assert_eq!(reqs.len(), 6);
        for r in reqs {
            assert_eq!(r["createIndex"]["ledger"], "o-test-l1");
        }
        let ids: Vec<Value> = reqs.iter().map(|r| r["createIndex"]["id"].clone()).collect();
        assert!(ids.contains(&asset_index_id()));
        for b in BOOTSTRAP_TX_BUILTINS {
            assert!(ids.contains(&tx_builtin_id(b)));
        }
        let n = ft.calls_of("bucket", "Apply").len();
        for _ in 0..10 {
            testkit::run_rule(&d, index_bootstrap);
        }
        assert_eq!(fo.calls("/write/begin").len(), 1, "done: no further bulk");
        assert_eq!(ft.calls_of("bucket", "Apply").len(), n);

        // INDEX_ALREADY_EXISTS (a reused volume) is definitive too.
        let (d, ft, _) = testkit::driver();
        ft.set_default("bucket", "Apply", FakeReply::Outcome(Outcome { ok: false, code: "AlreadyExists".into(), reason: "INDEX_ALREADY_EXISTS".into(), ..Default::default() }));
        let st = d.s.ext::<IndexBootstrap>();
        for _ in 0..40 {
            if st.done.load(Ordering::SeqCst) {
                break;
            }
            testkit::run_rule(&d, index_bootstrap);
        }
        assert!(st.done.load(Ordering::SeqCst));
    }

    fn view_with(account: &[&str], transaction: &[&str]) -> Value {
        let m = |ks: &[&str]| -> Value { ks.iter().map(|k| (k.to_string(), json!("METADATA_TYPE_STRING"))).collect::<serde_json::Map<String, Value>>().into() };
        json!({"name": "o-test-l1", "ledger": "o-test-l1", "fieldTypes": {"account": m(account), "transaction": m(transaction), "ledger": {}}})
    }

    #[test]
    fn index_op_only_creates_metadata_indexes_on_declared_keys() {
        let lv = view_with(&["k0", "k2"], &["k1"]);
        let gaps = vec![json!({"canonical": "metadata:TARGET_TYPE_ACCOUNT:k2", "id": metadata_index_id("TARGET_TYPE_ACCOUNT", "k2")})];
        let declared = |t: &str, k: &str| (t == "TARGET_TYPE_ACCOUNT" && (k == "k0" || k == "k2")) || (t == "TARGET_TYPE_TRANSACTION" && k == "k1");
        let (creates, probes) = (std::sync::atomic::AtomicU64::new(0), std::sync::atomic::AtomicU64::new(0));
        testkit::with_tc_n(400, |tc| {
            let g: &[Value] = if draw::chance(tc, 0.5) { &gaps } else { &[] };
            let r = plan_index_op(tc, "o-test-l1", &lv, g);
            assert!(!reserved(&r), "{r}");
            if let Some(ci) = r.get("createIndex") {
                creates.fetch_add(1, Ordering::SeqCst);
                if let Some(m) = ci["id"].get("metadata") {
                    let (t, k) = (m["target"].as_str().unwrap(), m["key"].as_str().unwrap());
                    if k.starts_with("undeclared-") {
                        probes.fetch_add(1, Ordering::SeqCst);
                    } else {
                        assert!(declared(t, k), "metadata index on an undeclared key outside the probe: {r}");
                    }
                }
            } else {
                assert!(r.get("dropIndex").is_some(), "{r}");
            }
        });
        assert!(creates.load(Ordering::SeqCst) > 0);
    }

    #[test]
    fn index_op_runs_through_the_oracle() {
        let (d, _, fo) = testkit::driver();
        fo.set_view("ledger", view_with(&["k0"], &[]));
        for _ in 0..5 {
            testkit::run_rule(&d, index_op);
        }
        let begins = fo.calls("/write/begin");
        assert!(!begins.is_empty());
        for b in &begins {
            assert_eq!(b["requests"].as_array().unwrap().len(), 1, "single-request bulk");
        }
        assert!(fo.calls("/view").iter().any(|v| v.to_string().contains("indexGaps")));
    }

    #[test]
    fn no_gen_output_contains_a_reserved_name() {
        let lv = json!({"fieldTypes": {"account": {"k0": "METADATA_TYPE_INT64", "own-score3": "METADATA_TYPE_INT64"},
            "transaction": {"k3": "METADATA_TYPE_BOOL"}, "ledger": {"own-x": "METADATA_TYPE_STRING"}}});
        let (d, _, _) = testkit::driver();
        let run = d.s.run.clone();
        testkit::with_tc_n(300, |tc| {
            for g in order_gens() {
                let ctx = BulkCtx { run: &run, ledger: "o-test-l3", ledger_view: &lv, index_in_bulk: 0, len: 1 };
                if let Some(v) = (g.gen)(&d, tc, &ctx, false) {
                    assert!(!reserved(&v), "{} produced a reserved name: {v}", g.name);
                }
            }
            let r = plan_index_op(tc, "o-test-l3", &lv, &[]);
            assert!(!reserved(&r), "index_op: {r}");
        });
    }

    #[test]
    fn schema_set_draws_every_shape() {
        let (d, _, _) = testkit::driver();
        let lv = json!({});
        let run = d.s.run.clone();
        testkit::with_tc_n(50, |tc| {
            let ctx = BulkCtx { run: &run, ledger: "o-test-l1", ledger_view: &lv, index_in_bulk: 0, len: 1 };
            let v = gen_schema_set(&d, tc, &ctx, false).unwrap();
            let s = &v["setMetadataFieldType"];
            assert!(TARGETS.contains(&s["targetType"].as_str().unwrap()));
            assert!(METADATA_TYPES.contains(&s["type"].as_str().unwrap()));
            assert!(["k0", "k1", "k2", "k3"].contains(&s["key"].as_str().unwrap()));
        });
    }

    #[test]
    fn poll_pins_each_node_sets_stale_and_uses_one_ticket() {
        let (d, ft, fo) = testkit::driver();
        ft.set_default("bucket", "GetIndexStatus", FakeReply::Outcome(ok_outcome(json!({"lastIndexedSequence": "3", "lastLogSequence": "3", "lag": "0", "indexes": []}))));
        for _ in 0..6 {
            testkit::run_rule(&d, index_poll);
        }
        let begins = fo.calls("/read/begin");
        let ends = fo.calls("/read/end");
        assert!(!ends.is_empty());
        assert_eq!(begins.len(), ends.len(), "one ticket per poll");
        let calls = ft.calls_of("bucket", "GetIndexStatus");
        let polled: usize = ends.iter().map(|e| e["outcome"]["response"]["perNode"].as_array().unwrap().len()).sum();
        assert_eq!(calls.len(), polled, "every per-node call is in the payload");
        for c in &calls {
            assert_eq!(c["consistency"], "stale", "{c}");
            assert!(c["node"].as_str().is_some_and(|n| n.starts_with("ledger-")), "pinned: {c}");
            assert_eq!(c["timeoutMs"], 2000);
        }
        for (b, e) in begins.iter().zip(&ends) {
            assert_eq!(b["kind"], "IndexStatusPoll");
            assert_eq!(b["params"]["allNodes"].as_array().unwrap().len(), 3);
            let nodes = b["params"]["nodes"].as_array().unwrap();
            assert!(nodes.len() == 1 || nodes.len() == 3);
            let per = e["outcome"]["response"]["perNode"].as_array().unwrap();
            assert_eq!(per.len(), nodes.len());
            for (n, p) in nodes.iter().zip(per) {
                assert_eq!(&p["node"], n);
                assert_eq!(p["outcome"]["ok"], true);
            }
        }
        assert!(d.s.registry.snapshot().is_empty());
    }

    #[test]
    fn cycle_steps_stay_on_reserved_entities() {
        let steps = cycle_steps("o-test-l1", 7, 42, &["METADATA_TYPE_STRING", "METADATA_TYPE_BOOL", "METADATA_TYPE_INT64"], false, &[]);
        for (req, _) in &steps {
            let s = req.to_string();
            assert!(s.contains("own-score7") || s.contains("\"own-bad7\"") || s.contains("\"own-bad\""), "{s}");
            assert!(s.contains("\"o-test-l1\""));
        }
        let checks: Vec<StepCheck> = steps.iter().map(|(_, c)| *c).collect();
        assert_eq!(checks.iter().filter(|c| **c == StepCheck::Retype).count(), 3);
        assert!(checks.contains(&StepCheck::Int64OverUncoercible));
        assert!(checks.contains(&StepCheck::BoolOverNumeric));
        assert_eq!(steps[2].0["apply"]["action"]["addMetadata"]["metadata"]["own-score7"]["stringValue"], "42");
        assert_eq!(steps[2].0["apply"]["action"]["addMetadata"]["target"]["account"]["addr"], "users:own-score7");
        // The INT64 declaration lands on a fresh key that holds the
        // uncoercible text written just before it.
        let int64 = steps.iter().position(|(_, c)| *c == StepCheck::Int64OverUncoercible).unwrap();
        assert_eq!(steps[int64].0["setMetadataFieldType"]["key"], "own-bad7");
        assert_eq!(steps[int64 - 1].0["apply"]["action"]["addMetadata"]["metadata"]["own-bad7"]["stringValue"], "not-a-number-42");
        assert!(steps.iter().any(|(r, _)| r["apply"]["action"]["addMetadata"]["metadata"]["own-bad"]["stringValue"] == "not-a-number-42"));
        // Every per-cycle key is removed again (the remove drops its index).
        for k in ["own-score7", "own-bad7"] {
            let last_set = steps.iter().rposition(|(r, _)| r["setMetadataFieldType"]["key"] == k).unwrap();
            assert!(steps[last_set..].iter().any(|(r, _)| r["removeMetadataFieldType"]["key"] == k), "{k} removed at the end");
        }
        // The shared index is created (declared first) only while absent,
        // and last.
        let creates_bad = |st: &[(Value, StepCheck)]| st.iter().any(|(r, _)| r["createIndex"]["id"]["metadata"]["key"] == "own-bad");
        assert!(creates_bad(&steps));
        assert_eq!(steps.last().unwrap().0["createIndex"]["id"]["metadata"]["key"], "own-bad");
        assert_eq!(steps[steps.len() - 2].0["setMetadataFieldType"]["key"], "own-bad");
        let later = cycle_steps("o-test-l1", 8, 1, &["METADATA_TYPE_STRING"], true, &["own-score3".into()]);
        assert!(!creates_bad(&later));
        assert_eq!(later[0].0["removeMetadataFieldType"]["key"], "own-score3", "leftovers are swept first");
    }

    #[test]
    fn cycle_plan_inputs_read_the_model() {
        let inflight: BTreeSet<u64> = [5].into_iter().collect();
        let lv = json!({
            "fieldTypes": {"account": {"own-score3": "METADATA_TYPE_INT64", "own-score5": "METADATA_TYPE_INT64",
                "own-bad4": "METADATA_TYPE_INT64", "own-bad": "METADATA_TYPE_INT64", "k0": "METADATA_TYPE_STRING",
                "own-scorex": "METADATA_TYPE_INT64", "own-score9": "METADATA_TYPE_BOOL"}},
            "indexes": [{"canonical": "metadata:TARGET_TYPE_ACCOUNT:own-score3", "state": "ambiguous"}],
        });
        let (bad_indexed, leftovers) = cycle_plan_inputs(&lv, &inflight);
        assert!(!bad_indexed, "own-bad declared but not indexed");
        assert_eq!(leftovers, vec!["own-bad4".to_string(), "own-score3".to_string()], "capped, in-flight and shared keys spared");
        let lv2 = json!({"fieldTypes": {"account": {}}, "indexes": [{"canonical": "metadata:TARGET_TYPE_ACCOUNT:own-bad"}]});
        assert_eq!(cycle_plan_inputs(&lv2, &inflight), (true, vec![]));
    }

    #[test]
    fn typed_metadata_cycle_creates_the_shared_index_while_the_model_lacks_it() {
        let creates_bad = |fo: &testkit::FakeOracle| {
            fo.calls("/write/begin").iter().any(|b| b["requests"].as_array().is_some_and(|rs| rs.iter().any(|r| r["createIndex"]["id"]["metadata"]["key"] == BAD_KEY)))
        };
        // Any number of cycles: absent from the model, every cycle tries.
        let (d, _, fo) = testkit::driver();
        fo.set_view("ledger", json!({"fieldTypes": {"account": {}}, "indexes": []}));
        testkit::run_rule(&d, typed_metadata_cycle);
        testkit::run_rule(&d, typed_metadata_cycle);
        assert!(creates_bad(&fo));
        assert!(fo.calls("/view").iter().filter(|v| v.to_string().contains("\"owned\":true")).count() >= 2, "the owned l1 view per cycle");
        // Present in the model: no cycle re-creates it.
        let (d, _, fo) = testkit::driver();
        fo.set_view("ledger", json!({"fieldTypes": {"account": {}}, "indexes": [{"canonical": "metadata:TARGET_TYPE_ACCOUNT:own-bad"}]}));
        testkit::run_rule(&d, typed_metadata_cycle);
        assert!(!creates_bad(&fo));
        assert!(d.s.ext::<TypedMeta>().inflight.lock().unwrap().is_empty(), "the cycle left the in-flight set");
    }

    #[test]
    fn typed_metadata_cycle_runs_and_asserts() {
        let (d, _, fo) = testkit::driver();
        testkit::reset();
        testkit::run_rule(&d, typed_metadata_cycle);
        let begins = fo.calls("/write/begin");
        assert!(begins.len() >= 10, "{} steps", begins.len());
        assert!(testkit::fired("changing metadata type should not crash"));
        assert!(testkit::fired("declaring an int64 type over an uncoercible value should not crash"));
        assert!(testkit::fired("declaring a bool type over a numeric value should not crash"));
        let failing: Vec<_> = testkit::assertions().into_iter().filter(|a| !a.cond && a.kind != "Sometimes").collect();
        assert!(failing.is_empty(), "{failing:?}");
        assert!(d.s.registry.snapshot().is_empty());
    }

    #[test]
    fn introspect_and_inspect_use_their_kinds() {
        let (d, ft, fo) = testkit::driver();
        fo.set_view("ledger", view_with(&["k0"], &["k1"]));
        ft.set_default("bucket", "InspectIndex", FakeReply::Outcome(ok_outcome(json!({"distinctValues": {"values": [], "hasMore": true, "nextCursor": "c1"}}))));
        for _ in 0..12 {
            testkit::run_rule(&d, index_introspect);
            testkit::run_rule(&d, inspect_index);
        }
        let ends = fo.calls("/read/end");
        let kinds: std::collections::BTreeSet<String> = ends.iter().map(|e| e["kind"].as_str().unwrap().to_string()).collect();
        assert!(kinds.iter().all(|k| ["ListIndexes", "GetIndex", "GetIndexStatus", "InspectIndex"].contains(&k.as_str())), "{kinds:?}");
        for e in &ends {
            assert_eq!(e["route"]["consistency"], "linearizable");
        }
        assert!(ends.iter().any(|e| e["kind"] == "InspectIndex" && e["request"]["cursor"] == "c1"), "a resume on its own ticket");
        assert!(d.s.registry.snapshot().is_empty());
    }

    #[test]
    fn meta_bootstrap_steps_follow_the_model() {
        let empty = json!({"fieldTypes": {"account": {}, "transaction": {}}, "indexes": []});
        let steps = meta_bootstrap_steps("o-test-l1", &empty);
        assert_eq!(steps.len(), 4);
        assert_eq!(steps[0], set_field_type("o-test-l1", "TARGET_TYPE_ACCOUNT", "k0", "METADATA_TYPE_STRING"));
        assert_eq!(steps[1], create_index("o-test-l1", metadata_index_id("TARGET_TYPE_ACCOUNT", "k0")));
        assert_eq!(steps[2], set_field_type("o-test-l1", "TARGET_TYPE_TRANSACTION", "k1", "METADATA_TYPE_INT64"));
        assert_eq!(steps[3], create_index("o-test-l1", metadata_index_id("TARGET_TYPE_TRANSACTION", "k1")));
        // Declared with the seed type: only the create. Indexed (any state): nothing.
        let partial = json!({"fieldTypes": {"account": {"k0": "METADATA_TYPE_STRING"}, "transaction": {"k1": "METADATA_TYPE_BOOL"}},
            "indexes": [{"canonical": "metadata:TARGET_TYPE_ACCOUNT:k0", "state": "ambiguous"}]});
        let steps = meta_bootstrap_steps("o-test-l1", &partial);
        assert_eq!(steps, vec![set_field_type("o-test-l1", "TARGET_TYPE_TRANSACTION", "k1", "METADATA_TYPE_INT64"), create_index("o-test-l1", metadata_index_id("TARGET_TYPE_TRANSACTION", "k1"))]);
        let partial2 = json!({"fieldTypes": {"account": {}, "transaction": {"k1": "METADATA_TYPE_INT64"}},
            "indexes": [{"canonical": "metadata:TARGET_TYPE_ACCOUNT:k0", "state": "active"}]});
        assert_eq!(meta_bootstrap_steps("o-test-l1", &partial2), vec![create_index("o-test-l1", metadata_index_id("TARGET_TYPE_TRANSACTION", "k1"))]);
    }

    #[test]
    fn metadata_index_bootstrap_sends_separate_tracked_writes_then_stops() {
        let (d, _, fo) = testkit::driver();
        fo.set_view("ledger", json!({"fieldTypes": {"account": {}, "transaction": {}}, "indexes": []}));
        let st = d.s.ext::<MetaIndexBootstrap>();
        for _ in 0..40 {
            if st.done.load(Ordering::SeqCst) {
                break;
            }
            testkit::run_rule(&d, metadata_index_bootstrap);
        }
        assert!(st.done.load(Ordering::SeqCst));
        let begins = fo.calls("/write/begin");
        assert_eq!(begins.len(), 4, "declare + create per target, one write each");
        for b in &begins {
            let reqs = b["requests"].as_array().unwrap();
            assert_eq!(reqs.len(), 1, "never a declaration and its create in one bulk");
            assert!(!reserved(&reqs[0]));
        }
        assert!(begins[0]["requests"][0]["setMetadataFieldType"].is_object());
        assert_eq!(begins[1]["requests"][0]["createIndex"]["ledger"], "o-test-l1");
        assert!(fo.calls("/view").iter().all(|v| !v.to_string().contains("\"owned\":true")), "a public view: no reserved key is read");
        // The model has both indexes: re-armed runs send nothing.
        fo.set_view("ledger", json!({"fieldTypes": {"account": {"k0": "METADATA_TYPE_STRING"}, "transaction": {"k1": "METADATA_TYPE_INT64"}},
            "indexes": [{"canonical": "metadata:TARGET_TYPE_ACCOUNT:k0", "state": "active"}, {"canonical": "metadata:TARGET_TYPE_TRANSACTION:k1", "state": "active"}]}));
        for _ in 0..40 {
            testkit::run_rule(&d, metadata_index_bootstrap);
        }
        assert_eq!(fo.calls("/write/begin").len(), 4);
        assert!(d.s.registry.snapshot().is_empty());
    }

    #[test]
    fn metadata_index_bootstrap_accepts_an_existing_index() {
        let (d, ft, fo) = testkit::driver();
        fo.set_view("ledger", json!({"fieldTypes": {"account": {"k0": "METADATA_TYPE_STRING"}, "transaction": {"k1": "METADATA_TYPE_INT64"}}, "indexes": []}));
        ft.set_default("bucket", "Apply", FakeReply::Outcome(Outcome { ok: false, code: "AlreadyExists".into(), reason: "INDEX_ALREADY_EXISTS".into(), ..Default::default() }));
        let st = d.s.ext::<MetaIndexBootstrap>();
        for _ in 0..40 {
            if st.done.load(Ordering::SeqCst) {
                break;
            }
            testkit::run_rule(&d, metadata_index_bootstrap);
        }
        assert!(st.done.load(Ordering::SeqCst));
        assert_eq!(fo.calls("/write/begin").len(), 2);
    }

    #[test]
    fn index_op_prefers_metadata_gaps() {
        let lv = view_with(&["k0"], &["k1"]);
        let gaps = vec![
            json!({"canonical": "tx:TX_BUILTIN_INDEX_REFERENCE", "id": tx_builtin_id("TX_BUILTIN_INDEX_REFERENCE")}),
            json!({"canonical": "tx:TX_BUILTIN_INDEX_TIMESTAMP", "id": tx_builtin_id("TX_BUILTIN_INDEX_TIMESTAMP")}),
            json!({"canonical": "acct:ACCT_BUILTIN_INDEX_ASSET", "id": asset_index_id()}),
            json!({"canonical": "metadata:TARGET_TYPE_ACCOUNT:k0", "id": metadata_index_id("TARGET_TYPE_ACCOUNT", "k0")}),
        ];
        let (meta, builtin) = (std::sync::atomic::AtomicU64::new(0), std::sync::atomic::AtomicU64::new(0));
        testkit::with_tc_n(600, |tc| {
            let r = plan_index_op(tc, "o-test-l1", &lv, &gaps);
            if let Some(ci) = r.get("createIndex") {
                match ci["id"].get("metadata") {
                    Some(m) if m["key"] == "k0" => meta.fetch_add(1, Ordering::SeqCst),
                    Some(_) => 0,
                    None => builtin.fetch_add(1, Ordering::SeqCst),
                };
            }
        });
        let (m, b) = (meta.load(Ordering::SeqCst), builtin.load(Ordering::SeqCst));
        assert!(m > 2 * b, "metadata gap {m} vs builtin gaps {b}");
        assert!(b > 0, "builtin gaps still created");
    }

    #[test]
    fn plan_retype_targets_active_non_reserved_indexed_keys() {
        let lv = json!({"fieldTypes": {"account": {"k0": "METADATA_TYPE_STRING", "k2": "METADATA_TYPE_BOOL", "own-bad": "METADATA_TYPE_INT64"}, "transaction": {"k1": "METADATA_TYPE_INT64"}},
            "indexes": [
                {"canonical": "metadata:TARGET_TYPE_ACCOUNT:k0", "state": "active"},
                {"canonical": "metadata:TARGET_TYPE_ACCOUNT:k2", "state": "ambiguous"},
                {"canonical": "metadata:TARGET_TYPE_ACCOUNT:own-bad", "state": "active"},
                {"canonical": "metadata:TARGET_TYPE_TRANSACTION:k1", "state": "active"},
            ]});
        let mut seen = BTreeSet::new();
        for seed in 0..8u32 {
            let (t, k, req) = plan_retype("o-test-l1", &lv, seed, false, "METADATA_TYPE_BOOL").unwrap();
            let s = &req["setMetadataFieldType"];
            assert_eq!(s["key"], k.as_str());
            assert_eq!(s["type"], "METADATA_TYPE_BOOL");
            match k.as_str() {
                "k0" => assert_eq!((t, s["targetType"].as_str().unwrap()), (Target::Accounts, "TARGET_TYPE_ACCOUNT")),
                "k1" => assert_eq!((t, s["targetType"].as_str().unwrap()), (Target::Transactions, "TARGET_TYPE_TRANSACTION")),
                other => panic!("retype of a key without an active, non-reserved index: {other}"),
            }
            seen.insert(k);
        }
        assert_eq!(seen.len(), 2);
        let (_, k, req) = plan_retype("o-test-l1", &lv, 0, true, "METADATA_TYPE_BOOL").unwrap();
        let want = if k == "k0" { "METADATA_TYPE_STRING" } else { "METADATA_TYPE_INT64" };
        assert_eq!(req["setMetadataFieldType"]["type"], want, "same: the current type re-declared");
        assert!(plan_retype("o-test-l1", &json!({"fieldTypes": {"account": {"k0": "METADATA_TYPE_STRING"}}, "indexes": []}), 0, false, "METADATA_TYPE_BOOL").is_none());
    }

    #[test]
    fn schema_bulk_retype_probe_lists_on_the_retyped_key() {
        let (d, ft, fo) = testkit::driver();
        fo.set_view("ledger", json!({"fieldTypes": {"account": {"k0": "METADATA_TYPE_STRING"}, "transaction": {"k1": "METADATA_TYPE_INT64"}},
            "indexes": [{"canonical": "metadata:TARGET_TYPE_ACCOUNT:k0", "state": "active"}, {"canonical": "metadata:TARGET_TYPE_TRANSACTION:k1", "state": "active"}]}));
        ft.set_default("bucket", "ListAccounts", FakeReply::Outcome(Outcome { ok: true, items: Some(vec![]), complete: true, ..Default::default() }));
        ft.set_default("bucket", "ListTransactions", FakeReply::Outcome(Outcome { ok: true, items: Some(vec![]), complete: true, ..Default::default() }));
        for _ in 0..40 {
            testkit::run_rule(&d, schema_bulk);
        }
        let retypes: Vec<Value> = fo.calls("/write/begin").into_iter().filter(|b| b["hint"]["retype"].is_string()).collect();
        assert!(!retypes.is_empty(), "a retype probe ran");
        let probes: Vec<Value> = fo.calls("/read/end").into_iter().filter(|e| e["hint"]["gen"] == "retype_probe").collect();
        assert_eq!(probes.len(), retypes.len(), "one list per committed retype");
        for (w, r) in retypes.iter().zip(&probes) {
            let key = w["hint"]["retype"].as_str().unwrap();
            let tt = w["requests"][0]["setMetadataFieldType"]["targetType"].as_str().unwrap();
            assert_eq!(r["kind"], if tt == "TARGET_TYPE_ACCOUNT" { "ListAccounts" } else { "ListTransactions" });
            assert_eq!(r["request"]["ledger"], "o-test-l1");
            assert!(r["request"]["options"]["filter"].to_string().contains(&format!("\"metadata\":\"{key}\"")), "{r}");
        }
        assert!(d.s.registry.snapshot().is_empty());
    }

    #[test]
    fn next_cursor_of_both_paged_arms() {
        assert_eq!(inspect_next_cursor(&json!({"facets": {"nextCursor": "x"}})), Some("x".into()));
        assert_eq!(inspect_next_cursor(&json!({"distinctValues": {"nextCursor": ""}})), None);
        assert_eq!(inspect_next_cursor(&json!({"summary": {}})), None);
    }
}
