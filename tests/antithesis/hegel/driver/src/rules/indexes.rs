//! Task 07: indexes, typed metadata, driver-polled readiness, retype
//! windows, index introspection and inspection.
//!
//! Rules: `index_bootstrap`, `schema_bulk`, `index_op`,
//! `typed_metadata_cycle`, `index_poll`, `index_introspect`,
//! `inspect_index`. Order gens: `schema_set`, `schema_remove`.
//! Every tracked verdict comes from the oracle (`indexcheck`); the driver
//! re-asserts only the deterministic `MD/parallel_driver_typed_metadata`
//! literals on its rule-owned key, and the `INDEX_BUILDING` write
//! exploration.
use crate::clients::{CallOpts, Outcome, Route};
use crate::draw;
use crate::machine::{rule, Driver};
use crate::read::{unresolved, LedgerSel, ViewReq};
use crate::write::{AttemptRecord, BulkCtx, OrderGen, WritePlan, WriteResult, IN_RULE_BUDGET};
use hegel::stateful::ConcurrentRule;
use hegel::TestCase;
use serde_json::{json, Value};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

const FAMILY: &str = "indexes";

pub fn rules() -> Vec<ConcurrentRule<Driver>> {
    vec![
        rule("index_bootstrap", 1.0, index_bootstrap),
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

/// The family's exploration on the attempt log of each of its writes
/// (DESIGN §4.9: `INDEX_BUILDING` belongs to task 07).
fn explore_attempts(attempts: &[AttemptRecord]) {
    for a in attempts {
        if a.reason == "INDEX_BUILDING" {
            check_sometimes!(true, "a write attempt is refused while an index builds", json!({"code": a.code}));
        }
    }
}

fn dispatch_one(d: &Driver, plan: WritePlan) -> Option<WriteResult> {
    let unit = d.s.slots.try_acquire_up_to(1)?.take_one()?;
    let res = d.dispatch(plan, unit);
    explore_attempts(res.attempts());
    Some(res)
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

// ---------------------------------------------------------------- schema_bulk

/// The family's local bulk: 1–2 orders of its own gens on a drawn ledger.
pub fn schema_bulk(d: &Driver, tc: TestCase) {
    d.guarded("schema_bulk", &tc, |d, tc| {
        if !d.s.oracle_ok() {
            return None;
        }
        let pick = draw::ledger_pick(tc);
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

/// Cycles run so far; the first few keep their uncoercible key's index so
/// `inspect_index` can summarise a key holding unconvertible values.
#[derive(Default)]
pub struct TypedMeta {
    pub cycles: AtomicU64,
}

/// How many cycles keep their uncoercible key (declared INT64, indexed).
pub const KEPT_BAD_KEYS: u64 = 3;

/// The deterministic assertion a cycle step re-asserts on its outcome.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StepCheck {
    None,
    Retype,
    Int64OverUncoercible,
    BoolOverNumeric,
}

/// The steps of one typed-metadata cycle on reserved entities (pure).
pub fn cycle_steps(ledger: &str, n: u64, value: u64, retypes: &[&str], keep_bad: bool) -> Vec<(Value, StepCheck)> {
    let key = format!("own-score{n}");
    let bad = format!("own-score{n}-bad");
    let addr = format!("users:own-score{n}");
    let acct = "TARGET_TYPE_ACCOUNT";
    let mut s = vec![
        (set_field_type(ledger, acct, &key, "METADATA_TYPE_INT64"), StepCheck::None),
        (create_index(ledger, metadata_index_id(acct, &key)), StepCheck::None),
        (account_string_meta(ledger, &addr, &key, &value.to_string()), StepCheck::None),
    ];
    for t in retypes {
        s.push((set_field_type(ledger, acct, &key, t), StepCheck::Retype));
    }
    s.push((remove_field_type(ledger, acct, &key), StepCheck::None));
    s.push((account_string_meta(ledger, &addr, &bad, "not-a-number"), StepCheck::None));
    s.push((set_field_type(ledger, acct, &bad, "METADATA_TYPE_INT64"), StepCheck::Int64OverUncoercible));
    if keep_bad {
        s.push((create_index(ledger, metadata_index_id(acct, &bad)), StepCheck::None));
    }
    s.push((set_field_type(ledger, acct, &key, "METADATA_TYPE_BOOL"), StepCheck::BoolOverNumeric));
    s.push((remove_field_type(ledger, acct, &key), StepCheck::None));
    if !keep_bad {
        s.push((remove_field_type(ledger, acct, &bad), StepCheck::None));
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
        let keep_bad = st.cycles.fetch_add(1, Ordering::SeqCst) < KEPT_BAD_KEYS;
        let n = d.s.next_own();
        let ledger = format!("{}l1", d.s.tracked_prefix());
        let steps = cycle_steps(&ledger, n, value, &retypes, keep_bad);
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
            explore_attempts(res.attempts());
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

/// An id to look up: 1/4 an id the workload never registers, else one of the
/// ledger's pool (drawn after begin: an index into the view).
fn lookup_id(tc: &TestCase, unregistered: bool, ledger_view: Option<&Value>) -> Value {
    if unregistered || ledger_view.is_none() {
        return metadata_index_id("TARGET_TYPE_ACCOUNT", &format!("unregistered-{}", draw::meta_key_model(tc)));
    }
    let pool = index_pool(ledger_view.unwrap());
    pool[draw::pick(tc, pool.len())].clone()
}

pub fn index_introspect(d: &Driver, tc: TestCase) {
    d.guarded("index_introspect", &tc, |d, tc| {
        if !d.s.oracle_ok() {
            return None;
        }
        // 0 list ledger, 1 list all, 2 list bucket, 3 GetIndex, 4 GetIndexStatus.
        let arm = draw::weighted(tc, &[1.0, 0.5, 0.5, 1.0, 1.0])?;
        let absent = draw::chance(tc, 0.02);
        let absent_name = if absent { Some(absent_ledger(d, tc)) } else { None };
        let pick = draw::ledger_pick(tc);
        let unregistered = draw::chance(tc, 0.25);
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
                let id = lookup_id(tc, unregistered, lv.as_ref());
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
        let (ledger, key) = if absent {
            (absent_ledger(d, tc), undeclared_key)
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
        let steps = cycle_steps("o-test-l1", 7, 42, &["METADATA_TYPE_STRING", "METADATA_TYPE_BOOL", "METADATA_TYPE_INT64"], true);
        for (req, _) in &steps {
            let s = req.to_string();
            assert!(s.contains("own-score7"), "{s}");
            assert!(s.contains("\"o-test-l1\""));
        }
        let checks: Vec<StepCheck> = steps.iter().map(|(_, c)| *c).collect();
        assert_eq!(checks.iter().filter(|c| **c == StepCheck::Retype).count(), 3);
        assert!(checks.contains(&StepCheck::Int64OverUncoercible));
        assert!(checks.contains(&StepCheck::BoolOverNumeric));
        assert_eq!(steps[2].0["apply"]["action"]["addMetadata"]["metadata"]["own-score7"]["stringValue"], "42");
        assert_eq!(steps[2].0["apply"]["action"]["addMetadata"]["target"]["account"]["addr"], "users:own-score7");
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
    fn index_building_attempts_are_explored() {
        testkit::reset();
        explore_attempts(&[AttemptRecord { op_id: 1, class: crate::clients::OutcomeClass::NonDefinitive, code: "Unavailable".into(), reason: "INDEX_BUILDING".into() }]);
        assert!(testkit::fired("a write attempt is refused while an index builds"));
    }

    #[test]
    fn next_cursor_of_both_paged_arms() {
        assert_eq!(inspect_next_cursor(&json!({"facets": {"nextCursor": "x"}})), Some("x".into()));
        assert_eq!(inspect_next_cursor(&json!({"distinctValues": {"nextCursor": ""}})), None);
        assert_eq!(inspect_next_cursor(&json!({"summary": {}})), None);
    }
}
