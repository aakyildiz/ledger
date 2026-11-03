//! Task 08: prepared queries.
//!
//! Rules: `prepared_write` (the family's local bulk: one prepared-query order
//! from its own gens), `prepared_list` (ListPreparedQueries),
//! `prepared_execute` (ExecutePreparedQuery, LIST or AGGREGATE, plus one
//! follow-up page through the server cursor), `projection_probe` (a reserved
//! `accountHasAsset` query on `o-<run>-l1` read after an acknowledged write).
//! Order gens: `pq_create`, `pq_update`, `pq_delete`.
//!
//! Every verdict on tracked state comes from the oracle (`preparedcheck`).
//! The driver re-asserts one old literal verbatim: the Reachable
//! `projection-aligned prepared query succeeded`. A `serialized` refusal of
//! the oracle's per-name begin hook is an expected outcome.
use crate::clients::{CallOpts, Outcome, Route};
use crate::draw;
use crate::machine::{rule, Driver};
use crate::read::{unresolved, LedgerSel, ViewReq};
use crate::wire::Posting;
use crate::write::{compose_bulk, BulkCtx, OrderGen, WritePlan, WriteResult, IN_RULE_BUDGET};
use hegel::stateful::ConcurrentRule;
use hegel::TestCase;
use serde_json::{json, Map, Value};
use std::time::{Duration, Instant};

const FAMILY: &str = "prepared";

pub fn rules() -> Vec<ConcurrentRule<Driver>> {
    vec![
        rule("prepared_write", 0.8, prepared_write),
        rule("prepared_list", 0.6, prepared_list),
        rule("prepared_execute", 1.2, prepared_execute),
        rule("projection_probe", 0.3, projection_probe),
    ]
}

pub fn order_gens() -> Vec<OrderGen> {
    vec![
        OrderGen { name: "pq_create", weight: 0.3, applies: always, has_opt_in: false, gen: gen_pq_create },
        OrderGen { name: "pq_update", weight: 0.2, applies: alone, has_opt_in: false, gen: gen_pq_update },
        OrderGen { name: "pq_delete", weight: 0.15, applies: always, has_opt_in: false, gen: gen_pq_delete },
    ]
}

fn always(_: &BulkCtx) -> bool {
    true
}

/// `pq_update` only in a single-order bulk: the oracle refuses (`serialized`)
/// an update sharing a bulk with other orders, because its FSM-stage
/// FILTER_COMPILATION_ERROR is re-evaluated on every attempt and must not
/// depend on balances or other state (DESIGN §4.5).
fn alone(ctx: &BulkCtx) -> bool {
    ctx.len == 1
}

// ---------------------------------------------------------------- domains

pub const ACCOUNTS: &str = "QUERY_TARGET_ACCOUNTS";
pub const TRANSACTIONS: &str = "QUERY_TARGET_TRANSACTIONS";
pub const MODE_LIST: &str = "QUERY_MODE_LIST";
pub const MODE_AGGREGATE: &str = "QUERY_MODE_AGGREGATE_VOLUMES";

/// The shared name pool `q-0..q-99` (small on purpose: creates, updates and
/// deletes collide, so ALREADY_EXISTS and NOT_FOUND come from churn).
pub fn pool_name(tc: &TestCase) -> String {
    format!("q-{}", draw::int_in(tc, 0, 99))
}

/// Address prefixes: the old `parallel_driver_queries` prefix, the
/// `parallel_driver_update_query` pairs `{users:,world}`, `{world,users:}`,
/// `{users:0,users:1}`, and pool prefixes.
pub const PREFIXES: [&str; 6] = ["users:", "world", "users:0", "users:1", "t-", "t-1:"];

const META_STRINGS: [&str; 4] = ["v0", "7", "42", "true"];
const ASSET_BASES: [&str; 3] = ["USD", "EUR", "COIN"];

/// Hands out parameter names `p0, p1, …` within one filter.
#[derive(Default)]
pub struct Paramizer {
    n: usize,
}

impl Paramizer {
    fn next(&mut self) -> String {
        let s = format!("p{}", self.n);
        self.n += 1;
        s
    }
}

fn address_leaf(tc: &TestCase, pz: &mut Paramizer) -> Value {
    let exact = draw::chance(tc, 0.25);
    let param = draw::chance(tc, 0.5);
    let v = if exact { draw::address(tc) } else { draw::one_of(tc, &PREFIXES).to_string() };
    let key = match (exact, param) {
        (false, false) => "hardcodedPrefix",
        (true, false) => "hardcodedExact",
        (false, true) => "paramPrefix",
        (true, true) => "paramExact",
    };
    let val = if param { pz.next() } else { v };
    json!({"address": {key: val}})
}

fn meta_leaf(tc: &TestCase, pz: &mut Paramizer) -> Value {
    let key = draw::meta_key_model(tc);
    match draw::int_in(tc, 0, 3) {
        0 => json!({"field": {"field": {"metadata": key}, "existsCond": {}}}),
        1 => json!({"field": {"field": {"metadata": key}, "stringCond": {"hardcoded": draw::one_of(tc, &META_STRINGS)}}}),
        _ => json!({"field": {"field": {"metadata": key}, "stringCond": {"param": pz.next()}}}),
    }
}

/// A well-formed leaf (address or metadata field) valid on both targets.
fn common_leaf(tc: &TestCase, pz: &mut Paramizer) -> Value {
    if draw::chance(tc, 0.7) {
        address_leaf(tc, pz)
    } else {
        meta_leaf(tc, pz)
    }
}

/// The has-asset leaf is task 06's builder (deduped by 99).
pub use crate::rules::filters::has_asset;

/// A malformed filter: a shape no target accepts, or a leaf invalid on
/// `target`. FILTER_COMPILATION_ERROR at save (admission for a create; for an
/// update, admission or the FSM against the stored target).
pub fn malformed_filter(tc: &TestCase, target: &str) -> Value {
    match draw::int_in(tc, 0, 2) {
        0 => json!({"address": {}}),
        1 => json!({"field": {"stringCond": {"hardcoded": "v0"}}}),
        _ if target == ACCOUNTS => json!({"reverted": {"value": true}}),
        _ => has_asset("USD", 2),
    }
}

/// The local filter builder: `None` is the match-all (absent) filter (1/8);
/// 1/16 a malformed filter; otherwise prefix/exact address, has-asset (bare,
/// ACCOUNTS only), metadata field, or an and/or of two common leaves, with
/// parameter references `p<N>` on half of the parameterizable leaves.
pub fn filter(tc: &TestCase, target: &str) -> Option<Value> {
    if draw::chance(tc, 1.0 / 8.0) {
        return None;
    }
    if draw::chance(tc, 1.0 / 16.0) {
        return Some(malformed_filter(tc, target));
    }
    let mut pz = Paramizer::default();
    Some(match draw::int_in(tc, 0, 7) {
        0..=3 => address_leaf(tc, &mut pz),
        4 if target == ACCOUNTS => {
            let base = *draw::one_of(tc, &ASSET_BASES);
            has_asset(base, *draw::one_of(tc, &[0u64, 2, 2, 8]))
        }
        4 | 5 => meta_leaf(tc, &mut pz),
        6 => json!({"and": {"filters": [common_leaf(tc, &mut pz), common_leaf(tc, &mut pz)]}}),
        _ => json!({"or": {"filters": [common_leaf(tc, &mut pz), common_leaf(tc, &mut pz)]}}),
    })
}

pub fn target(tc: &TestCase) -> &'static str {
    if draw::chance(tc, 0.5) {
        ACCOUNTS
    } else {
        TRANSACTIONS
    }
}

pub fn create_pq(ledger: &str, name: &str, target: &str, filter: Option<Value>) -> Value {
    let mut q = json!({"name": name, "target": target});
    if let Some(f) = filter {
        q["filter"] = f;
    }
    json!({"createPreparedQuery": {"ledger": ledger, "query": q}})
}

pub fn update_pq(ledger: &str, name: &str, filter: Option<Value>) -> Value {
    let mut r = json!({"ledger": ledger, "name": name});
    if let Some(f) = filter {
        r["filter"] = f;
    }
    json!({"updatePreparedQuery": r})
}

pub fn delete_pq(ledger: &str, name: &str) -> Value {
    json!({"deletePreparedQuery": {"ledger": ledger, "name": name}})
}

/// The non-reserved stored names of a core `ledger` view.
fn stored_names(ledger_view: &Value) -> Vec<String> {
    ledger_view["preparedQueries"]
        .as_array()
        .map(|a| a.iter().filter_map(Value::as_str).filter(|n| !n.starts_with("own-")).map(str::to_string).collect())
        .unwrap_or_default()
}

/// A stored name (3/4 when any) or a pool name (possibly absent).
fn existing_or_pool(tc: &TestCase, ledger_view: &Value) -> String {
    let names = stored_names(ledger_view);
    if !names.is_empty() && draw::chance(tc, 0.75) {
        return names[draw::pick(tc, names.len())].clone();
    }
    pool_name(tc)
}

// ---------------------------------------------------------------- order gens

fn gen_pq_create(_d: &Driver, tc: &TestCase, ctx: &BulkCtx, _opt_in: bool) -> Option<Value> {
    let name = pool_name(tc);
    let t = target(tc);
    Some(create_pq(ctx.ledger, &name, t, filter(tc, t)))
}

/// An update carries no target; the filter is built for a drawn one, so a
/// target-specific leaf can be invalid for the STORED target (an FSM-stage,
/// uncommitted FILTER_COMPILATION_ERROR the begin hook keeps stable).
fn gen_pq_update(_d: &Driver, tc: &TestCase, ctx: &BulkCtx, _opt_in: bool) -> Option<Value> {
    let name = existing_or_pool(tc, ctx.ledger_view);
    let t = target(tc);
    Some(update_pq(ctx.ledger, &name, filter(tc, t)))
}

fn gen_pq_delete(_d: &Driver, tc: &TestCase, ctx: &BulkCtx, _opt_in: bool) -> Option<Value> {
    let name = existing_or_pool(tc, ctx.ledger_view);
    Some(delete_pq(ctx.ledger, &name))
}

fn ledger_of(v: &Value) -> Option<String> {
    v.get("ledger").and_then(Value::as_str).map(str::to_string)
}

// ---------------------------------------------------------------- prepared_write

/// One prepared-query order from the family's gens (`compose_bulk`, length 1)
/// on a `ledgerPick`-drawn tracked ledger. `serialized` is expected.
pub fn prepared_write(d: &Driver, tc: TestCase) {
    d.guarded("prepared_write", &tc, |d, tc| {
        if !d.s.oracle_ok() {
            return None;
        }
        let pick = draw::ledger_pick(tc);
        let views = d.view(&[ViewReq::ledger(LedgerSel::Pick(pick))])?;
        let lv = views.first()?;
        let ledger = ledger_of(lv)?;
        let reqs = compose_bulk(d, tc, &order_gens(), &ledger, lv, 1);
        if reqs.is_empty() {
            return None;
        }
        let mut plan = WritePlan::drawn(FAMILY, reqs, d, tc);
        plan.hint = json!({"gen": "prepared_write"});
        let unit = d.s.slots.try_acquire_up_to(1)?.take_one()?;
        let _ = d.dispatch(plan, unit);
        Some(())
    });
}

// ---------------------------------------------------------------- prepared_list

pub fn prepared_list(d: &Driver, tc: TestCase) {
    d.guarded("prepared_list", &tc, |d, tc| {
        if !d.s.oracle_ok() {
            return None;
        }
        let pick = draw::ledger_pick(tc);
        let route = draw::route(tc, &d.s);
        let rt = d.read_begin("ListPreparedQueries", &[ViewReq::ledger(LedgerSel::Pick(pick))], json!({}))?;
        let v = rt.views().first()?.clone();
        if unresolved(&v) {
            rt.abandon();
            return None;
        }
        let ledger = ledger_of(&v)?;
        let req = json!({"ledger": ledger});
        let opts = CallOpts::linearizable();
        let (_, out) = d.call(&route, "bucket", "ListPreparedQueries", req.clone(), &opts);
        rt.end(&req, &out, &route, &opts, json!({}), json!({}));
        Some(())
    });
}

// ---------------------------------------------------------------- prepared_execute

/// Page sizes the executor echoes unchanged (M/prepared_queries_read.go:226-228:
/// the size the server serves a request with).
pub fn exec_page_size(tc: &TestCase) -> u64 {
    match draw::int_in(tc, 0, 31) {
        0 | 1 => 100,
        2 => 1000,
        _ => *draw::one_of(tc, &[1u64, 2, 3, 5, 10, 50]),
    }
}

/// Every draw of one execution made before `/read/begin`.
#[derive(Clone, Debug)]
pub struct ExecPlan {
    pub pick: u32,
    pub route: Route,
    pub aggregate: bool,
    /// AGGREGATE with reverse (misuse probe).
    pub reverse_probe: bool,
    /// AGGREGATE aimed at a non-ACCOUNTS query (misuse probe).
    pub target_probe: bool,
    /// Draw a pool name instead of a stored one.
    pub pool: Option<String>,
    /// The pool name executed when nothing is stored.
    pub fallback: String,
    /// Parameter values drawn before `/read/begin`; picked by index after.
    pub values: ParamPool,
    pub page_size: u64,
    pub reverse: bool,
    /// The follow-up page resumes in the opposite direction.
    pub flip: bool,
    pub follow_route: Route,
}

pub fn plan_exec(d: &Driver, tc: &TestCase) -> ExecPlan {
    let pick = draw::ledger_pick(tc);
    let route = draw::route(tc, &d.s);
    let aggregate = draw::chance(tc, 1.0 / 3.0);
    let reverse_probe = aggregate && draw::chance(tc, 1.0 / 8.0);
    let target_probe = aggregate && draw::chance(tc, 1.0 / 8.0);
    let pool = if draw::chance(tc, 1.0 / 8.0) { Some(pool_name(tc)) } else { None };
    let fallback = pool_name(tc);
    let values = ParamPool::draw(tc);
    let page_size = exec_page_size(tc);
    let reverse = !aggregate && draw::chance(tc, 0.5);
    let flip = draw::chance(tc, 0.25);
    let follow_route = draw::route(tc, &d.s);
    ExecPlan { pick, route, aggregate, reverse_probe, target_probe, pool, fallback, values, page_size, reverse, flip, follow_route }
}

/// One stored query of the `preparedQueries` view.
#[derive(Clone, Debug, PartialEq)]
pub struct StoredQuery {
    pub name: String,
    pub target: String,
    pub params: Vec<(String, String)>,
}

pub fn parse_queries(view: &Value) -> Vec<StoredQuery> {
    view["queries"]
        .as_array()
        .map(|a| {
            a.iter()
                .map(|q| StoredQuery {
                    name: q["name"].as_str().unwrap_or("").to_string(),
                    target: q["target"].as_str().unwrap_or("").to_string(),
                    params: q["params"]
                        .as_array()
                        .map(|ps| ps.iter().map(|p| (p["name"].as_str().unwrap_or("").to_string(), p["kind"].as_str().unwrap_or("").to_string())).collect())
                        .unwrap_or_default(),
                })
                .filter(|q| !q.name.is_empty())
                .collect()
        })
        .unwrap_or_default()
}

/// A `ParameterValue` protojson for a parameter kind.
pub fn param_value(tc: &TestCase, kind: &str) -> Value {
    match kind {
        "address" => {
            if draw::chance(tc, 0.5) {
                json!({"stringValue": draw::one_of(tc, &PREFIXES)})
            } else {
                json!({"stringValue": draw::address(tc)})
            }
        }
        "string" => json!({"stringValue": draw::one_of(tc, &META_STRINGS)}),
        "uint" => json!({"uint64Value": draw::one_of(tc, &[0u64, 1, 2, 5, 42, 200]).to_string()}),
        "int" => json!({"int64Value": (draw::int_in(tc, 0, 10) as i64 - 4).to_string()}),
        _ => json!({"boolValue": draw::chance(tc, 0.5)}),
    }
}

pub const PARAM_KINDS: [&str; 5] = ["address", "string", "uint", "int", "bool"];
/// Values drawn per kind before `/read/begin`.
pub const VALUES_PER_KIND: usize = 3;

/// How `draw_params` spoils the parameters.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Spoil {
    None,
    Missing,
    Mistyped,
}

/// Every value draw of an execution's parameters, made before `/read/begin`
/// (DESIGN §4.7): `VALUES_PER_KIND` values per kind and the spoil mode. After
/// begin, only indices into these are drawn.
#[derive(Clone, Debug)]
pub struct ParamPool {
    pub by_kind: Vec<(&'static str, Vec<Value>)>,
    pub spoil: Spoil,
}

impl ParamPool {
    pub fn draw(tc: &TestCase) -> Self {
        let by_kind = PARAM_KINDS.iter().map(|k| (*k, (0..VALUES_PER_KIND).map(|_| param_value(tc, k)).collect())).collect();
        let spoil = match draw::int_in(tc, 0, 7) {
            0 => Spoil::Missing,
            1 => Spoil::Mistyped,
            _ => Spoil::None,
        };
        ParamPool { by_kind, spoil }
    }

    fn values(&self, kind: &str) -> &[Value] {
        let k = if PARAM_KINDS.contains(&kind) { kind } else { "bool" };
        self.by_kind.iter().find(|(n, _)| *n == k).map(|(_, v)| v.as_slice()).unwrap_or(&[])
    }
}

/// A value of a type the kind's condition rejects.
pub fn mistyped_value(kind: &str) -> Value {
    if kind == "uint" || kind == "int" {
        json!({"stringValue": "not-a-number"})
    } else {
        json!({"uint64Value": "1"})
    }
}

/// Parameters for the stored references from the pre-drawn pool (index draws
/// only): one value per reference; 1/8 one reference left out, 1/8 one bound
/// with the wrong type.
pub fn draw_params(tc: &TestCase, pool: &ParamPool, params: &[(String, String)]) -> Map<String, Value> {
    let mut out = Map::new();
    if params.is_empty() {
        return out;
    }
    let spoiled = match pool.spoil {
        Spoil::None => None,
        s => Some((draw::pick(tc, params.len()), s)),
    };
    for (i, (name, kind)) in params.iter().enumerate() {
        match spoiled {
            Some((j, Spoil::Missing)) if j == i => continue,
            Some((j, Spoil::Mistyped)) if j == i => {
                out.insert(name.clone(), mistyped_value(kind));
            }
            _ => {
                let vs = pool.values(kind);
                if vs.is_empty() {
                    continue;
                }
                out.insert(name.clone(), vs[draw::pick(tc, vs.len())].clone());
            }
        }
    }
    out
}

/// Chooses the query to execute among the stored ones (index draw after
/// `/read/begin`): AGGREGATE prefers ACCOUNTS queries, its target probe
/// prefers the others; a pool name when drawn or when nothing is stored.
pub fn choose_query(tc: &TestCase, plan: &ExecPlan, stored: &[StoredQuery]) -> StoredQuery {
    if let Some(n) = &plan.pool {
        return stored.iter().find(|q| &q.name == n).cloned().unwrap_or(StoredQuery { name: n.clone(), target: String::new(), params: vec![] });
    }
    let preferred: Vec<&StoredQuery> = if plan.aggregate {
        stored.iter().filter(|q| (q.target == ACCOUNTS) != plan.target_probe).collect()
    } else {
        stored.iter().collect()
    };
    let list: Vec<&StoredQuery> = if preferred.is_empty() { stored.iter().collect() } else { preferred };
    if list.is_empty() {
        return StoredQuery { name: plan.fallback.clone(), target: String::new(), params: vec![] };
    }
    list[draw::pick(tc, list.len())].clone()
}

pub fn exec_request(ledger: &str, q: &StoredQuery, params: &Map<String, Value>, plan: &ExecPlan) -> Value {
    let mut req = json!({"ledger": ledger, "queryName": q.name});
    if !params.is_empty() {
        req["parameters"] = Value::Object(params.clone());
    }
    if plan.aggregate {
        req["mode"] = json!(MODE_AGGREGATE);
        if plan.reverse_probe {
            req["reverse"] = json!(true);
        }
    } else {
        req["mode"] = json!(MODE_LIST);
        req["pageSize"] = json!(plan.page_size);
        if plan.reverse {
            req["reverse"] = json!(true);
        }
    }
    req
}

/// The follow-up page request: the first request with the server cursor,
/// reversed when the plan flips.
pub fn follow_request(first: &Value, cursor: &str, flip: bool) -> Value {
    let mut req = first.clone();
    req["cursor"] = json!(cursor);
    let rev = first.get("reverse").and_then(Value::as_bool).unwrap_or(false) != flip;
    match req.as_object_mut() {
        Some(o) if !rev => {
            o.remove("reverse");
        }
        _ => req["reverse"] = json!(true),
    }
    req
}

/// The server cursor of a successful LIST page, when it advertises more.
pub fn next_cursor(out: &Outcome) -> Option<String> {
    if !out.is_ok() {
        return None;
    }
    let c = out.response.as_ref()?.get("cursor")?;
    let next = c.get("next").and_then(Value::as_str)?;
    (!next.is_empty()).then(|| next.to_string())
}

pub fn prepared_execute(d: &Driver, tc: TestCase) {
    d.guarded("prepared_execute", &tc, |d, tc| {
        if !d.s.oracle_ok() {
            return None;
        }
        let plan = plan_exec(d, tc);
        let rt = d.read_begin("ExecutePreparedQuery", &[ViewReq::named("preparedQueries", json!({"ledgerPick": plan.pick}))], json!({}))?;
        let v = rt.views().first()?.clone();
        if unresolved(&v) {
            rt.abandon();
            return None;
        }
        let ledger = ledger_of(&v)?;
        let stored = parse_queries(&v);
        let q = choose_query(tc, &plan, &stored);
        let params = draw_params(tc, &plan.values, &q.params);
        let req = exec_request(&ledger, &q, &params, &plan);
        let opts = CallOpts::linearizable();
        let (_, out) = d.call(&plan.route, "bucket", "ExecutePreparedQuery", req.clone(), &opts);
        let first_ticket = rt.ticket();
        let hint = json!({"aggregate": plan.aggregate, "reverseProbe": plan.reverse_probe, "targetProbe": plan.target_probe, "pool": plan.pool.is_some()});
        let verdict = rt.end(&req, &out, &plan.route, &opts, hint, json!({}));
        if plan.aggregate || !verdict.is_some_and(|v| v.ok) {
            return Some(());
        }
        // One follow-up page through the server's cursor, on a new ticket.
        let cursor = next_cursor(&out)?;
        let rt2 = d.read_begin("ExecutePreparedQuery", &[], json!({}))?;
        let req2 = follow_request(&req, &cursor, plan.flip);
        let (_, out2) = d.call(&plan.follow_route, "bucket", "ExecutePreparedQuery", req2.clone(), &opts);
        rt2.end(&req2, &out2, &plan.follow_route, &opts, json!({"followUp": true, "flip": plan.flip}), json!({"prevTicket": first_ticket}));
        Some(())
    });
}

// ---------------------------------------------------------------- projection_probe

/// Chain budget for the probe's two writes.
pub const PROBE_CHAIN_BUDGET: Duration = Duration::from_secs(12);

/// Page size of the probe's execution (served unchanged).
pub const PROBE_PAGE_SIZE: u64 = 1000;
/// Pages the probe follows through the server cursor until one reaches its
/// account (one read ticket per page).
pub const PROBE_MAX_PAGES: usize = 4;

/// `USD/2` (fixed regression branch, 1/2) or a drawn base with a precision in
/// `1..=255` other than 2 (`domain.MaxHasAssetPrecision`; every such asset is
/// valid): no other generator writes those assets (`draw::ASSETS`), so the
/// probe account is among the few matches and on the first page.
pub fn probe_asset(tc: &TestCase) -> (&'static str, u64) {
    if draw::chance(tc, 0.5) {
        return ("USD", 2);
    }
    let p = draw::int_in(tc, 1, 254);
    (*draw::one_of(tc, &ASSET_BASES), if p >= 2 { p + 1 } else { p })
}

/// Whether an ascending ACCOUNTS page reached `addr` (it lists it or an
/// address after it), so later pages cannot hold it.
pub fn page_reaches(out: &Outcome, addr: &str) -> bool {
    let rows = out.response.as_ref().and_then(|r| r.get("cursor")).and_then(|c| c.get("accountData")).and_then(Value::as_array);
    rows.is_some_and(|rows| rows.iter().filter_map(|a| a.get("address").and_then(Value::as_str)).any(|a| a >= addr))
}

/// `FormatAsset(base, precision)`: a bare base for precision 0.
pub fn format_asset(base: &str, precision: u64) -> String {
    if precision == 0 {
        base.to_string()
    } else {
        format!("{base}/{precision}")
    }
}

/// The forced unit write `world → addr` in `asset`.
pub fn forced_unit_tx(ledger: &str, addr: &str, asset: &str) -> Value {
    let mut ct = json!({"postings": [Posting::new("world", addr, 1, asset).to_json()]});
    ct["force"] = json!(true);
    json!({"apply": {"ledger": ledger, "action": {"createTransaction": ct}}})
}

fn run_probe_write(d: &Driver, plan: WritePlan, budget: Duration) -> Option<WriteResult> {
    let unit = d.s.slots.try_acquire_up_to(1)?.take_one()?;
    match d.begin_write(plan, unit) {
        Ok(t) => Some(t.run(budget)),
        Err(e) => Some(WriteResult::Refused(e)),
    }
}

/// On `o-<run>-l1` (never deleted): a tracked create of the reserved query
/// `own-pq-asset<N>` = `accountHasAsset{base, precision}`, a forced unit write
/// `world → users:own-pq<N>` in that asset, then an ascending execution that
/// follows the server cursor (one ticket per page, at most
/// `PROBE_MAX_PAGES`) until a page reaches the probe account. The oracle's
/// window check of each page is the read-your-acknowledged-write membership
/// check; the asset index comes from task 07 (absent: the oracle gates the
/// INDEX_* error legally).
pub fn projection_probe(d: &Driver, tc: TestCase) {
    d.guarded("projection_probe", &tc, |d, tc| {
        if !d.s.oracle_ok() {
            return None;
        }
        let (base, precision) = probe_asset(tc);
        let route = draw::route(tc, &d.s);
        let ledger = format!("{}l1", d.s.tracked_prefix());
        let n = d.s.next_own();
        let name = format!("own-pq-asset{n}");
        let addr = format!("users:own-pq{n}");
        let asset = format_asset(base, precision);
        let mut create = WritePlan::drawn(FAMILY, vec![create_pq(&ledger, &name, ACCOUNTS, Some(has_asset(base, precision)))], d, tc);
        create.hint = json!({"gen": "projection_probe", "step": "create"});
        let mut write = WritePlan::drawn(FAMILY, vec![forced_unit_tx(&ledger, &addr, &asset)], d, tc);
        write.hint = json!({"gen": "projection_probe", "step": "write"});

        let start = Instant::now();
        let r1 = run_probe_write(d, create, IN_RULE_BUDGET.min(PROBE_CHAIN_BUDGET))?;
        if !r1.is_success() {
            return Some(());
        }
        let left = PROBE_CHAIN_BUDGET.saturating_sub(start.elapsed());
        if left < Duration::from_millis(500) {
            return Some(());
        }
        let r2 = run_probe_write(d, write, left.min(IN_RULE_BUDGET))?;
        if !r2.is_success() {
            return Some(());
        }
        let first = json!({"ledger": ledger, "queryName": name, "pageSize": PROBE_PAGE_SIZE, "mode": MODE_LIST});
        let opts = CallOpts::linearizable();
        let mut prev: Option<(u64, String)> = None;
        for page in 0..PROBE_MAX_PAGES {
            let (req, params, views) = match &prev {
                None => (first.clone(), json!({}), vec![ViewReq::named("preparedQueries", json!({"ledger": ledger, "owned": true}))]),
                Some((tk, cursor)) => (follow_request(&first, cursor, false), json!({"prevTicket": tk}), vec![]),
            };
            let rt = d.read_begin("ExecutePreparedQuery", &views, json!({}))?;
            let (_, out) = d.call(&route, "bucket", "ExecutePreparedQuery", req.clone(), &opts);
            let ticket = rt.ticket();
            let verdict = rt.end(&req, &out, &route, &opts, json!({"projectionProbe": true, "page": page}), params);
            if page == 0 && out.is_ok() {
                check_reachable!("projection-aligned prepared query succeeded", json!({"ledger": ledger, "queryName": name, "assetBase": base, "precision": precision}));
            }
            // A page the oracle did not validate is not remembered: its
            // follow-up could not be judged.
            if !verdict.is_some_and(|v| v.ok) || page_reaches(&out, &addr) {
                break;
            }
            let Some(cursor) = next_cursor(&out) else { break };
            prev = Some((ticket, cursor));
        }
        Some(())
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testkit::{self, FakeReply};
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn reserved(v: &Value) -> bool {
        let s = v.to_string();
        s.contains("own-") || s.contains("-own")
    }

    fn ok_outcome(resp: Value) -> Outcome {
        Outcome { ok: true, response: Some(resp), ..Default::default() }
    }

    #[test]
    fn no_gen_output_contains_a_reserved_name() {
        let lv = json!({"name": "o-test-l3", "preparedQueries": ["q-1", "own-pq-asset3", "q-7"]});
        let (d, _, _) = testkit::driver();
        let run = d.s.run.clone();
        testkit::with_tc_n(500, |tc| {
            for g in order_gens() {
                let ctx = BulkCtx { run: &run, ledger: "o-test-l3", ledger_view: &lv, index_in_bulk: 0, len: 1 };
                let v = (g.gen)(&d, tc, &ctx, false).expect("gen produces an order");
                assert!(!reserved(&v), "{} produced a reserved name: {v}", g.name);
                let name = v.as_object().unwrap().values().next().unwrap();
                let name = name.get("name").or_else(|| name["query"].get("name")).and_then(Value::as_str).unwrap();
                assert!(name.starts_with("q-") && name[2..].parse::<u32>().unwrap() < 100, "{name}");
            }
        });
    }

    #[test]
    fn filter_builder_draws_every_shape() {
        let seen: Vec<AtomicUsize> = (0..8).map(|_| AtomicUsize::new(0)).collect();
        testkit::with_tc_n(800, |tc| {
            let t = target(tc);
            let f = filter(tc, t);
            let Some(f) = f else {
                seen[0].fetch_add(1, Ordering::SeqCst);
                return;
            };
            let s = f.to_string();
            if s.contains("\"param") {
                seen[1].fetch_add(1, Ordering::SeqCst);
            }
            if s.contains("accountHasAsset") && t == ACCOUNTS {
                seen[2].fetch_add(1, Ordering::SeqCst);
            }
            if s.contains("\"field\"") {
                seen[3].fetch_add(1, Ordering::SeqCst);
            }
            if s.contains("hardcodedPrefix") {
                seen[4].fetch_add(1, Ordering::SeqCst);
            }
            if f == json!({"address": {}}) || s.contains("reverted") || (s.contains("accountHasAsset") && t == TRANSACTIONS) {
                seen[5].fetch_add(1, Ordering::SeqCst);
            }
            if s.contains("\"and\"") || s.contains("\"or\"") {
                seen[6].fetch_add(1, Ordering::SeqCst);
                // has-asset is never composed.
                assert!(!s.contains("accountHasAsset"), "{s}");
            }
            // Parameter names are identifiers p<N>, unique within a filter.
            let mut names: Vec<String> = s.match_indices("\"p").filter_map(|(i, _)| s[i + 1..].split('"').next().map(str::to_string)).filter(|n| n.len() > 1 && n[1..].chars().all(|c| c.is_ascii_digit())).collect();
            let all = names.len();
            names.sort();
            names.dedup();
            assert_eq!(all, names.len(), "{s}");
        });
        for (i, c) in seen.iter().enumerate().take(7) {
            assert!(c.load(Ordering::SeqCst) > 0, "shape {i} never drawn");
        }
    }

    #[test]
    fn params_follow_the_stored_references() {
        let refs = vec![("p0".to_string(), "address".to_string()), ("p1".to_string(), "uint".to_string()), ("p2".to_string(), "string".to_string())];
        let (full, missing, mistyped) = (AtomicUsize::new(0), AtomicUsize::new(0), AtomicUsize::new(0));
        testkit::with_tc_n(400, |tc| {
            let pool = ParamPool::draw(tc);
            let p = draw_params(tc, &pool, &refs);
            // Well-typed values come from the pool drawn before begin.
            for (n, k) in &refs {
                if let Some(v) = p.get(n) {
                    assert!(pool.values(k).contains(v) || *v == mistyped_value(k), "{n}={v}");
                }
            }
            assert!(p.keys().all(|k| refs.iter().any(|(n, _)| n == k)));
            if p.len() < refs.len() {
                missing.fetch_add(1, Ordering::SeqCst);
                assert_eq!(p.len(), refs.len() - 1);
                return;
            }
            let wrong = p.get("p1").unwrap().get("stringValue").is_some() || p.get("p0").unwrap().get("uint64Value").is_some() || p.get("p2").unwrap().get("uint64Value").is_some();
            if wrong {
                mistyped.fetch_add(1, Ordering::SeqCst);
            } else {
                full.fetch_add(1, Ordering::SeqCst);
            }
        });
        assert!(full.load(Ordering::SeqCst) > 0 && missing.load(Ordering::SeqCst) > 0 && mistyped.load(Ordering::SeqCst) > 0);
    }

    #[test]
    fn page_sizes_are_served_unchanged() {
        testkit::with_tc_n(300, |tc| {
            let p = exec_page_size(tc);
            assert!([1, 2, 3, 5, 10, 50, 100, 1000].contains(&p), "{p}");
        });
    }

    #[test]
    fn follow_request_flips_direction() {
        let first = json!({"ledger": "l", "queryName": "q-1", "mode": MODE_LIST, "pageSize": 2});
        let f = follow_request(&first, "c", false);
        assert_eq!(f["cursor"], "c");
        assert!(f.get("reverse").is_none());
        assert_eq!(follow_request(&first, "c", true)["reverse"], true);
        let mut rev = first.clone();
        rev["reverse"] = json!(true);
        assert!(follow_request(&rev, "c", true).get("reverse").is_none());
        assert_eq!(follow_request(&rev, "c", false)["reverse"], true);
    }

    #[test]
    fn prepared_write_sends_one_order_and_accepts_serialized() {
        let (d, ft, fo) = testkit::driver();
        fo.set_view("ledger", json!({"preparedQueries": ["q-3"]}));
        for _ in 0..6 {
            testkit::run_rule(&d, prepared_write);
        }
        let begins = fo.calls("/write/begin");
        assert!(!begins.is_empty());
        for b in &begins {
            let reqs = b["requests"].as_array().unwrap();
            assert_eq!(reqs.len(), 1);
            let k = reqs[0].as_object().unwrap().keys().next().unwrap().clone();
            assert!(["createPreparedQuery", "updatePreparedQuery", "deletePreparedQuery"].contains(&k.as_str()), "{k}");
            assert_eq!(b["family"], "prepared");
        }
        let applies = ft.calls_of("bucket", "Apply").len();
        fo.refuse_next_begin(409, "serialized");
        testkit::reset();
        testkit::run_rule(&d, prepared_write);
        assert!(!testkit::assertions().iter().any(|a| a.message.contains("oracle accepts")), "{:?}", testkit::assertions());
        assert_eq!(ft.calls_of("bucket", "Apply").len(), applies, "a serialized bulk is never sent");
        assert!(d.s.registry.snapshot().is_empty());
    }

    #[test]
    fn prepared_list_reads_through_the_oracle() {
        let (d, ft, fo) = testkit::driver();
        testkit::run_rule(&d, prepared_list);
        let ends = fo.calls("/read/end");
        assert_eq!(ends.len(), 1);
        assert_eq!(ends[0]["kind"], "ListPreparedQueries");
        assert!(ends[0]["request"]["ledger"].as_str().unwrap().starts_with("o-test-l"));
        assert_eq!(ft.calls_of("bucket", "ListPreparedQueries").len(), 1);
    }

    #[test]
    fn prepared_execute_takes_a_follow_up_page() {
        let (d, ft, fo) = testkit::driver();
        fo.set_view("preparedQueries", json!({"queries": [
            {"name": "q-1", "target": ACCOUNTS, "params": [{"name": "p0", "kind": "address"}]},
            {"name": "q-2", "target": TRANSACTIONS, "params": []}
        ]}));
        ft.set_default("bucket", "ExecutePreparedQuery", FakeReply::Outcome(ok_outcome(json!({"cursor": {"pageSize": 2, "hasMore": true, "next": "tok"}}))));
        for _ in 0..30 {
            testkit::run_rule(&d, prepared_execute);
        }
        let ends = fo.calls("/read/end");
        assert!(!ends.is_empty());
        let follow: Vec<&Value> = ends.iter().filter(|e| e["request"].get("cursor").is_some()).collect();
        assert!(!follow.is_empty(), "some LIST execution took a follow-up page");
        for f in &follow {
            assert_eq!(f["request"]["cursor"], "tok");
            assert_eq!(f["request"]["mode"], MODE_LIST);
            let prev = f["params"]["prevTicket"].as_u64().unwrap();
            assert!(ends.iter().any(|e| e["ticket"].as_u64() == Some(prev) && e["request"].get("cursor").is_none()));
        }
        for e in &ends {
            let r = &e["request"];
            assert_eq!(e["kind"], "ExecutePreparedQuery");
            if r["mode"] == MODE_AGGREGATE {
                assert!(r.get("cursor").is_none() && r.get("pageSize").is_none());
            } else {
                assert!([1, 2, 3, 5, 10, 50, 100, 1000].contains(&r["pageSize"].as_u64().unwrap()));
            }
            assert!(!reserved(r));
        }
        assert!(d.s.registry.snapshot().is_empty());
    }

    #[test]
    fn projection_probe_creates_writes_and_executes() {
        let (d, ft, fo) = testkit::driver();
        ft.set_default("bucket", "ExecutePreparedQuery", FakeReply::Outcome(ok_outcome(json!({"cursor": {"pageSize": 100}}))));
        testkit::reset();
        testkit::run_rule(&d, projection_probe);
        let begins = fo.calls("/write/begin");
        assert_eq!(begins.len(), 2);
        let c = &begins[0]["requests"][0]["createPreparedQuery"];
        assert_eq!(c["ledger"], "o-test-l1");
        let name = c["query"]["name"].as_str().unwrap();
        assert!(name.starts_with("own-pq-asset"));
        let ha = &c["query"]["filter"]["accountHasAsset"];
        let ct = &begins[1]["requests"][0]["apply"]["action"]["createTransaction"];
        assert_eq!(ct["force"], true);
        let p = &ct["postings"][0];
        assert_eq!(p["source"], "world");
        assert!(p["destination"].as_str().unwrap().starts_with("users:own-pq"));
        assert_eq!(p["asset"].as_str().unwrap(), format_asset(ha["assetBase"].as_str().unwrap(), ha["precision"].as_u64().unwrap_or(0)));
        let ends = fo.calls("/read/end");
        assert_eq!(ends.len(), 1);
        assert_eq!(ends[0]["request"]["queryName"], name);
        assert_eq!(ends[0]["request"]["pageSize"], PROBE_PAGE_SIZE);
        assert!(testkit::fired("projection-aligned prepared query succeeded"));

        // A failed create stops the chain.
        let (d, ft, fo) = testkit::driver();
        ft.set_default("bucket", "Apply", FakeReply::Outcome(Outcome { ok: false, code: "AlreadyExists".into(), reason: "PREPARED_QUERY_ALREADY_EXISTS".into(), ..Default::default() }));
        testkit::run_rule(&d, projection_probe);
        assert_eq!(fo.calls("/write/begin").len(), 1);
        assert!(fo.calls("/read/end").is_empty());
    }

    #[test]
    fn probe_assets_cover_the_fixed_branch_and_others() {
        let (usd2, other) = (AtomicUsize::new(0), AtomicUsize::new(0));
        testkit::with_tc_n(200, |tc| {
            let (b, p) = probe_asset(tc);
            assert!(ASSET_BASES.contains(&b) && p <= 255);
            if b != "USD" || p != 2 {
                assert!(p >= 1 && p != 2, "drawn precision {p} collides with the generators' assets");
            }
            if (b, p) == ("USD", 2) {
                usd2.fetch_add(1, Ordering::SeqCst);
            } else {
                other.fetch_add(1, Ordering::SeqCst);
            }
        });
        assert!(usd2.load(Ordering::SeqCst) > 0 && other.load(Ordering::SeqCst) > 0);
        assert_eq!(format_asset("USD", 0), "USD");
        assert_eq!(format_asset("EUR", 2), "EUR/2");
    }

    #[test]
    fn pq_update_only_in_a_single_order_bulk() {
        let lv = json!({"preparedQueries": []});
        let gens = order_gens();
        let upd = gens.iter().find(|g| g.name == "pq_update").unwrap();
        for len in 1..4 {
            let ctx = BulkCtx { run: "r", ledger: "o-test-l1", ledger_view: &lv, index_in_bulk: 0, len };
            assert_eq!((upd.applies)(&ctx), len == 1);
        }
        assert!(gens.iter().filter(|g| g.name != "pq_update").all(|g| (g.applies)(&BulkCtx { run: "r", ledger: "l", ledger_view: &lv, index_in_bulk: 1, len: 3 })));
    }

    #[test]
    fn page_reaches_compares_addresses() {
        let page = |addrs: &[&str]| ok_outcome(json!({"cursor": {"accountData": addrs.iter().map(|a| json!({"address": a})).collect::<Vec<_>>()}}));
        assert!(page_reaches(&page(&["t-1:2", "users:own-pq3"]), "users:own-pq3"));
        assert!(page_reaches(&page(&["users:9", "world"]), "users:own-pq3"));
        assert!(!page_reaches(&page(&["t-1:2", "users:31"]), "users:own-pq3"));
        assert!(!page_reaches(&Outcome::default(), "users:own-pq3"));
    }

    #[test]
    fn projection_probe_follows_the_cursor_until_its_account() {
        let (d, ft, fo) = testkit::driver();
        // Every page lists accounts before the probe's and advertises more.
        ft.set_default("bucket", "ExecutePreparedQuery", FakeReply::Outcome(ok_outcome(json!({"cursor": {"pageSize": PROBE_PAGE_SIZE, "hasMore": true, "next": "more", "accountData": [{"address": "t-0:1"}]}}))));
        testkit::run_rule(&d, projection_probe);
        let ends = fo.calls("/read/end");
        assert_eq!(ends.len(), PROBE_MAX_PAGES);
        assert!(ends[0]["request"].get("cursor").is_none());
        for w in ends.windows(2) {
            assert_eq!(w[1]["request"]["cursor"], "more");
            assert!(w[1]["request"].get("reverse").is_none(), "ascending");
            assert_eq!(w[1]["params"]["prevTicket"], w[0]["ticket"]);
            assert_eq!(w[1]["request"]["queryName"], w[0]["request"]["queryName"]);
        }

        // A page that reaches the account ends the traversal.
        let (d, ft, fo) = testkit::driver();
        ft.set_default("bucket", "ExecutePreparedQuery", FakeReply::Outcome(ok_outcome(json!({"cursor": {"pageSize": PROBE_PAGE_SIZE, "hasMore": true, "next": "more", "accountData": [{"address": "world"}]}}))));
        testkit::run_rule(&d, projection_probe);
        assert_eq!(fo.calls("/read/end").len(), 1);
        assert!(d.s.registry.snapshot().is_empty());
    }

    /// Writes the protocol fixtures (`HG/protocol-fixtures/08/`) the Go side
    /// decodes with protojson (preparedcheck's fixture test). Regenerated by
    /// `PROTOCOL_FIXTURES_WRITE=1 cargo test prepared::tests::protocol_fixtures`;
    /// otherwise the files must match the builders.
    #[test]
    fn protocol_fixtures() {
        let plan = |aggregate, reverse_probe, reverse| ExecPlan {
            pick: 0,
            route: Route::lib(0),
            aggregate,
            reverse_probe,
            target_probe: false,
            pool: None,
            fallback: "q-0".into(),
            values: ParamPool { by_kind: vec![], spoil: Spoil::None },
            page_size: 5,
            reverse,
            flip: false,
            follow_route: Route::lib(0),
        };
        let q = StoredQuery { name: "q-1".into(), target: ACCOUNTS.into(), params: vec![] };
        let mut params = Map::new();
        params.insert("p0".into(), json!({"stringValue": "users:"}));
        params.insert("p1".into(), json!({"uint64Value": "42"}));
        params.insert("p2".into(), json!({"int64Value": "-3"}));
        params.insert("p3".into(), json!({"boolValue": true}));
        let list = exec_request("o-test-l1", &q, &params, &plan(false, false, true));
        let fixtures = vec![
            ("execute-list-params.json", list.clone()),
            ("execute-follow-up.json", follow_request(&list, "opaque", true)),
            ("execute-aggregate.json", exec_request("o-test-l1", &q, &Map::new(), &plan(true, false, false))),
            ("execute-aggregate-reverse.json", exec_request("o-test-l1", &q, &Map::new(), &plan(true, true, false))),
            ("execute-mistyped.json", exec_request("o-test-l1", &q, &{
                let mut m = Map::new();
                m.insert("p0".into(), mistyped_value("address"));
                m.insert("p1".into(), mistyped_value("uint"));
                m
            }, &plan(false, false, false))),
            ("create-param-filters.json", json!({"requests": [
                create_pq("o-test-l1", "q-1", ACCOUNTS, Some(json!({"and": {"filters": [
                    {"address": {"paramPrefix": "p0"}},
                    {"field": {"field": {"metadata": "k1"}, "stringCond": {"param": "p1"}}}
                ]}}))),
                create_pq("o-test-l1", "q-2", TRANSACTIONS, Some(json!({"or": {"filters": [
                    {"address": {"paramExact": "p0"}},
                    {"field": {"field": {"metadata": "k0"}, "existsCond": {}}}
                ]}}))),
                create_pq("o-test-l1", "q-3", ACCOUNTS, Some(has_asset("USD", 2))),
                create_pq("o-test-l1", "q-4", ACCOUNTS, None),
                update_pq("o-test-l1", "q-1", Some(json!({"address": {"hardcodedPrefix": "world"}}))),
                update_pq("o-test-l1", "q-2", None),
                delete_pq("o-test-l1", "q-3"),
                forced_unit_tx("o-test-l1", "users:own-pq1", "USD/2"),
            ]})),
            // Every malformed shape `malformed_filter` sends, at create and
            // at update (the Go side decodes them strictly and requires the
            // server's validators to refuse each).
            ("malformed-filters.json", json!({"requests": [
                create_pq("o-test-l1", "q-1", ACCOUNTS, Some(json!({"address": {}}))),
                create_pq("o-test-l1", "q-2", TRANSACTIONS, Some(json!({"field": {"stringCond": {"hardcoded": "v0"}}}))),
                create_pq("o-test-l1", "q-3", ACCOUNTS, Some(json!({"reverted": {"value": true}}))),
                create_pq("o-test-l1", "q-4", TRANSACTIONS, Some(has_asset("USD", 2))),
                update_pq("o-test-l1", "q-5", Some(json!({"address": {}}))),
                update_pq("o-test-l1", "q-6", Some(json!({"field": {"stringCond": {"hardcoded": "v0"}}}))),
            ]})),
        ];
        let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../protocol-fixtures/08");
        let write = std::env::var("PROTOCOL_FIXTURES_WRITE").is_ok();
        if write {
            std::fs::create_dir_all(&dir).unwrap();
        }
        for (name, v) in fixtures {
            let path = dir.join(name);
            let text = serde_json::to_string_pretty(&v).unwrap() + "\n";
            if write {
                std::fs::write(&path, &text).unwrap();
            } else {
                let on_disk = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
                let a: Value = serde_json::from_str(&on_disk).unwrap();
                assert_eq!(a, v, "{name} differs from the builders");
            }
        }
    }
}
