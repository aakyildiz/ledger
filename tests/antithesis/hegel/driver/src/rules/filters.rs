//! Query-filter generation for the list rules (task 06): hegel-driven ports
//! of `genAccountFilter`, `genTransactionFilter` (M/queries.go),
//! `genLogFilter` (M/queries_logs.go), `rollFilterShape` (M/filter_shapes.go),
//! the unsupported-builtin probe, the malformed-cursor menu (M/cursors.go) and
//! `page_token` (M/queries.go `pageToken`, Go's `pagecursor.Encode`).
//!
//! Every draw is made before `/read/begin`: a generator returns a [`Plan`] —
//! the filter's shape with pre-drawn values — and leaves that need the
//! ledger's state (declared field types, a committed reference, learned
//! stamps) carry pre-drawn `u32` picks that [`Plan::resolve`] turns into
//! indices into the views returned with the read ticket. Resolution draws
//! nothing. Filters are protojson `QueryFilter` values (camelCase, 64-bit
//! integers as strings, enums by name, out-of-range enums as numbers).
#![allow(dead_code)]

use crate::draw::{self, chance, int_in};
use base64::Engine;
use hegel::TestCase;
use serde_json::{json, Map, Value};

/// `domain.MaxFilterDepth`: the compiler refuses deeper trees.
pub const MAX_FILTER_DEPTH: u64 = 100;
/// Generated boolean nesting stays well under the limit (M `maxQueryGenDepth`).
const MAX_GEN_DEPTH: usize = 3;

/// `oneIn(n)`: nominal probability 1/n.
fn one_in(tc: &TestCase, n: u64) -> bool {
    chance(tc, 1.0 / n as f64)
}

fn pick_u32(tc: &TestCase) -> u32 {
    int_in(tc, 0, u32::MAX as u64) as u32
}

fn pool_name(tc: &TestCase) -> String {
    format!("t-{}", int_in(tc, 0, 24))
}

// ---------------------------------------------------------------- targets

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Target {
    Accounts,
    Transactions,
    Logs,
}

// ---------------------------------------------------------------- page tokens

/// The forward page token of a resume key, as Go's
/// `pagecursor.Cursor{Key: key}.Encode()`: unpadded base64url of
/// `{"key":"…"}` with `encoding/json`'s string escaping. The empty key is the
/// first page (no token).
pub fn page_token(key: &str) -> String {
    if key.is_empty() {
        return String::new();
    }
    let json = format!("{{\"key\":{}}}", go_json_string(key));
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(json.as_bytes())
}

/// `encoding/json`'s rendering of a string (HTML-safe escaping).
fn go_json_string(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '<' | '>' | '&' | '\u{2028}' | '\u{2029}' => out.push_str(&format!("\\u{:04x}", c as u32)),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

/// The malformed-cursor menu (M/cursors.go): tokens an endpoint keyed by a
/// uint64 must refuse.
pub const MALFORMED_CURSORS: [&str; 7] = ["not-a-cursor", "-1", "0x10", " 12", "12 ", "1.0", "18446744073709551616"];

/// One read in sixteen: a malformed token, raw (1/2) or wrapped in a valid
/// page token (M `rollMalformedCursor`).
pub fn roll_malformed_cursor(tc: &TestCase) -> Option<String> {
    if !one_in(tc, 16) {
        return None;
    }
    let bad = *draw::one_of(tc, &MALFORMED_CURSORS);
    Some(if one_in(tc, 2) { page_token(bad) } else { bad.to_string() })
}

/// `match.EffectivePageSize`: 0 → the server default (100), above 1000 → 1000.
pub fn effective_page_size(requested: u64) -> u64 {
    match requested {
        0 => 100,
        n if n > 1000 => 1000,
        n => n,
    }
}

// ---------------------------------------------------------------- builders

pub fn role_name(r: u64) -> Value {
    match r {
        0 => json!("ADDRESS_ROLE_ANY"),
        1 => json!("ADDRESS_ROLE_SOURCE"),
        2 => json!("ADDRESS_ROLE_DESTINATION"),
        n => json!(n),
    }
}

pub fn addr_prefix(prefix: &str, role: u64) -> Value {
    json!({"address": {"hardcodedPrefix": prefix, "role": role_name(role)}})
}

pub fn addr_exact(addr: &str, role: u64) -> Value {
    json!({"address": {"hardcodedExact": addr, "role": role_name(role)}})
}

pub fn reference(value: &str) -> Value {
    json!({"reference": {"cond": {"hardcoded": value}}})
}

pub fn reverted(value: bool) -> Value {
    json!({"reverted": {"value": value}})
}

pub const TX_BUILTINS: [&str; 8] = [
    "TX_BUILTIN_INDEX_REFERENCE",
    "TX_BUILTIN_INDEX_TIMESTAMP",
    "TX_BUILTIN_INDEX_ID",
    "TX_BUILTIN_INDEX_ADDRESS",
    "TX_BUILTIN_INDEX_SOURCE_ADDRESS",
    "TX_BUILTIN_INDEX_DESTINATION_ADDRESS",
    "TX_BUILTIN_INDEX_INSERTED_AT",
    "TX_BUILTIN_INDEX_REVERTED_AT",
];

fn enum_value(names: &[&str], n: u64) -> Value {
    match names.get(n as usize) {
        Some(s) => json!(s),
        None => json!(n),
    }
}

pub fn uint_cond(min: Option<u64>, max: Option<u64>) -> Map<String, Value> {
    let mut m = Map::new();
    if let Some(v) = min {
        m.insert("min".into(), json!(v.to_string()));
    }
    if let Some(v) = max {
        m.insert("max".into(), json!(v.to_string()));
    }
    m
}

pub fn builtin_uint(field: &str, cond: Map<String, Value>) -> Value {
    json!({"builtinUint": {"field": field, "cond": Value::Object(cond)}})
}

pub fn tx_id_range(lo: u64, hi: u64) -> Value {
    builtin_uint("TX_BUILTIN_INDEX_ID", uint_cond(Some(lo), Some(hi)))
}

pub fn has_asset(base: &str, precision: u64) -> Value {
    json!({"accountHasAsset": {"assetBase": base, "precision": precision}})
}

pub fn meta_exists(key: &str, include_null: bool) -> Value {
    let cond = if include_null { json!({"includeNull": true}) } else { json!({}) };
    json!({"field": {"field": {"metadata": key}, "existsCond": cond}})
}

fn field_with(key: &str, arm: &str, cond: Value) -> Value {
    let mut fc = Map::new();
    fc.insert("field".into(), json!({"metadata": key}));
    fc.insert(arm.into(), cond);
    json!({"field": Value::Object(fc)})
}

pub fn field_string(key: &str, v: &str) -> Value {
    field_with(key, "stringCond", json!({"hardcoded": v}))
}

pub fn field_bool(key: &str, v: bool) -> Value {
    field_with(key, "boolCond", json!({"hardcoded": v}))
}

fn int_cond(lo: Option<i64>, hi: Option<i64>, lo_ex: bool, hi_ex: bool) -> Value {
    let mut m = Map::new();
    if let Some(v) = lo {
        m.insert("min".into(), json!(v.to_string()));
        if lo_ex {
            m.insert("minExclusive".into(), json!(true));
        }
    }
    if let Some(v) = hi {
        m.insert("max".into(), json!(v.to_string()));
        if hi_ex {
            m.insert("maxExclusive".into(), json!(true));
        }
    }
    Value::Object(m)
}

fn uint_cond_ex(lo: Option<u64>, hi: Option<u64>, lo_ex: bool, hi_ex: bool) -> Value {
    let mut m = uint_cond(lo, hi);
    if lo.is_some() && lo_ex {
        m.insert("minExclusive".into(), json!(true));
    }
    if hi.is_some() && hi_ex {
        m.insert("maxExclusive".into(), json!(true));
    }
    Value::Object(m)
}

pub fn and(children: Vec<Value>) -> Value {
    json!({"and": {"filters": children}})
}

pub fn or(children: Vec<Value>) -> Value {
    json!({"or": {"filters": children}})
}

pub fn not(child: Value) -> Value {
    json!({"not": {"filter": child}})
}

pub fn log_ledger(name: &str) -> Value {
    json!({"ledger": {"cond": {"hardcoded": name}}})
}

pub fn log_id(cond: Value) -> Value {
    json!({"logId": {"cond": cond}})
}

pub fn log_date(cond: Value) -> Value {
    json!({"logBuiltinUint": {"field": "LOG_BUILTIN_INDEX_DATE", "cond": cond}})
}

// ---------------------------------------------------------------- resolution context

/// What the views returned with the read ticket tell the generators.
#[derive(Clone, Debug, Default)]
pub struct FilterCtx {
    pub ledger: String,
    /// Declared (key, type) pairs per target, sorted by key.
    pub account_fields: Vec<(String, String)>,
    pub tx_fields: Vec<(String, String)>,
    /// A committed reference (`seek(txByRef)`), if any.
    pub reference: Option<String>,
    /// Learned transaction timestamps (`stamps.txDates`).
    pub tx_stamps: Vec<u64>,
    /// Learned log dates (`stamps.logDates`).
    pub log_dates: Vec<u64>,
    /// The ledger's log count (its newest ledger-local log id).
    pub log_count: u64,
}

fn field_list(v: &Value) -> Vec<(String, String)> {
    let mut out: Vec<(String, String)> = v.as_object().map(|m| m.iter().map(|(k, t)| (k.clone(), t.as_str().unwrap_or("").to_string())).collect()).unwrap_or_default();
    out.sort();
    out
}

fn u64_list(v: Option<&Value>) -> Vec<u64> {
    v.and_then(Value::as_array).map(|a| a.iter().filter_map(|x| x.as_str().and_then(|s| s.parse().ok()).or_else(|| x.as_u64())).collect()).unwrap_or_default()
}

impl FilterCtx {
    /// Reads the core `ledger` view and, when given, `seek(txByRef)` and
    /// `stamps` views of one ledger.
    pub fn from_views(ledger_view: &Value, seek_ref: Option<&Value>, stamps: Option<&Value>) -> FilterCtx {
        let ft = &ledger_view["fieldTypes"];
        FilterCtx {
            ledger: ledger_view["ledger"].as_str().or_else(|| ledger_view["name"].as_str()).unwrap_or("").to_string(),
            account_fields: field_list(&ft["account"]),
            tx_fields: field_list(&ft["transaction"]),
            reference: seek_ref.filter(|s| s["found"].as_bool() == Some(true)).and_then(|s| s["entry"]["reference"].as_str()).map(str::to_string),
            tx_stamps: u64_list(stamps.and_then(|s| s.get("txDates"))),
            log_dates: u64_list(stamps.and_then(|s| s.get("logDates"))),
            log_count: ledger_view["logCount"].as_u64().unwrap_or(0),
        }
    }
}

// ---------------------------------------------------------------- plans

/// Pre-drawn bounds of a numeric range (M `intBound` / `uintBound` and the
/// exclusive rolls).
#[derive(Clone, Debug)]
pub struct BoundPlan {
    open: bool,
    extremum: Option<bool>, // Some(true) = MAX, Some(false) = MIN
    delta: u64,
    exclusive: bool,
}

fn draw_bound(tc: &TestCase) -> BoundPlan {
    let open = one_in(tc, 4);
    let extremum = if !open && one_in(tc, 8) { Some(chance(tc, 0.5)) } else { None };
    BoundPlan { open, extremum, delta: int_in(tc, 0, 63), exclusive: one_in(tc, 4) }
}

/// A `genFieldLeaf` draw: everything a kind-compatible Field leaf on a
/// declared key needs, whatever the key's type turns out to be.
#[derive(Clone, Debug)]
pub struct FieldPlan {
    seed: u32,
    exists: bool,
    include_null: bool,
    str_hit: bool,
    str_value: u32,
    bool_value: bool,
    center: u64,
    lo: BoundPlan,
    hi: BoundPlan,
    split: bool,
}

/// Metadata string values the generators write (`draw::meta_value` pool).
const META_STRINGS: [&str; 11] = ["v0", "7", "42", "true", "false", "200", "-1", "-200", "40000", "5000000000", "030"];

fn draw_field(tc: &TestCase) -> FieldPlan {
    FieldPlan {
        seed: pick_u32(tc),
        exists: one_in(tc, 6),
        include_null: one_in(tc, 2),
        str_hit: !one_in(tc, 4),
        str_value: pick_u32(tc),
        bool_value: one_in(tc, 2),
        center: int_in(tc, 0, 1023),
        lo: draw_bound(tc),
        hi: draw_bound(tc),
        split: one_in(tc, 3),
    }
}

fn is_unsigned(t: &str) -> bool {
    t.starts_with("METADATA_TYPE_UINT")
}

impl FieldPlan {
    fn resolve(&self, seeds: &[(String, String)]) -> Option<Value> {
        if seeds.is_empty() {
            return None;
        }
        let (key, ty) = &seeds[self.seed as usize % seeds.len()];
        if self.exists {
            return Some(meta_exists(key, self.include_null));
        }
        Some(match ty.as_str() {
            "METADATA_TYPE_STRING" => {
                let v = if self.str_hit { META_STRINGS[self.str_value as usize % META_STRINGS.len()] } else { "absent-value" };
                field_string(key, v)
            }
            "METADATA_TYPE_BOOL" => field_bool(key, self.bool_value),
            t if is_unsigned(t) => {
                let b = |p: &BoundPlan, lo: bool| -> Option<u64> {
                    if p.open {
                        return None;
                    }
                    if let Some(max) = p.extremum {
                        return Some(if max { u64::MAX } else { 0 });
                    }
                    Some(if lo { self.center.saturating_sub(p.delta) } else { self.center.saturating_add(p.delta) })
                };
                let (lo, hi) = (b(&self.lo, true), b(&self.hi, false));
                if lo.is_some() && hi.is_some() && self.split {
                    and(vec![field_with(key, "uintCond", uint_cond_ex(lo, None, self.lo.exclusive, false)), field_with(key, "uintCond", uint_cond_ex(None, hi, false, self.hi.exclusive))])
                } else {
                    field_with(key, "uintCond", uint_cond_ex(lo, hi, self.lo.exclusive, self.hi.exclusive))
                }
            }
            _ => {
                // Signed and datetime both take int bounds.
                let c = self.center as i64;
                let b = |p: &BoundPlan, lo: bool| -> Option<i64> {
                    if p.open {
                        return None;
                    }
                    if let Some(max) = p.extremum {
                        return Some(if max { i64::MAX } else { i64::MIN });
                    }
                    Some(if lo { c - p.delta as i64 } else { c + p.delta as i64 })
                };
                let (lo, hi) = (b(&self.lo, true), b(&self.hi, false));
                if lo.is_some() && hi.is_some() && self.split {
                    and(vec![field_with(key, "intCond", int_cond(lo, None, self.lo.exclusive, false)), field_with(key, "intCond", int_cond(None, hi, false, self.hi.exclusive))])
                } else {
                    field_with(key, "intCond", int_cond(lo, hi, self.lo.exclusive, self.hi.exclusive))
                }
            }
        })
    }
}

/// `genMismatchedFieldLeaf`: a bare Field leaf whose condition kind the
/// declared type rejects (the FILTER_COMPILATION probe).
fn mismatched(seeds: &[(String, String)], seed: u32) -> Option<Value> {
    if seeds.is_empty() {
        return None;
    }
    let (key, ty) = &seeds[seed as usize % seeds.len()];
    Some(match ty.as_str() {
        "METADATA_TYPE_STRING" => field_bool(key, true),
        "METADATA_TYPE_BOOL" => field_with(key, "intCond", int_cond(Some(0), None, false, false)),
        _ => field_string(key, "x"),
    })
}

/// Pre-drawn `rollOpenOrExclusive` (M/queries.go).
#[derive(Clone, Debug)]
pub struct RangeRoll {
    min_open: bool,
    min_ex: bool,
    min_extremum: Option<bool>,
    max_open: bool,
    max_ex: bool,
    max_extremum: Option<bool>,
}

fn draw_range_roll(tc: &TestCase) -> RangeRoll {
    let min_open = one_in(tc, 4);
    let (min_ex, min_extremum) = if min_open { (false, None) } else { (one_in(tc, 4), if one_in(tc, 16) { Some(one_in(tc, 2)) } else { None }) };
    let max_open = one_in(tc, 4);
    let (max_ex, max_extremum) = if max_open { (false, None) } else { (one_in(tc, 4), if one_in(tc, 16) { Some(one_in(tc, 2)) } else { None }) };
    RangeRoll { min_open, min_ex, min_extremum, max_open, max_ex, max_extremum }
}

impl RangeRoll {
    fn apply(&self, lo: u64, hi: u64) -> Map<String, Value> {
        let ext = |e: Option<bool>, v: u64| match e {
            Some(false) => 0,
            Some(true) => u64::MAX,
            None => v,
        };
        let mut m = Map::new();
        if !self.min_open {
            m.insert("min".into(), json!(ext(self.min_extremum, lo).to_string()));
            if self.min_ex {
                m.insert("minExclusive".into(), json!(true));
            }
        }
        if !self.max_open {
            m.insert("max".into(), json!(ext(self.max_extremum, hi).to_string()));
            if self.max_ex {
                m.insert("maxExclusive".into(), json!(true));
            }
        }
        m
    }
}

/// A filter's shape with its pre-drawn values.
#[derive(Clone, Debug)]
pub enum Plan {
    /// The absent top-level filter (the universe).
    Universe,
    /// A leaf or tree that needs no view.
    Fixed(Value),
    And(Vec<Plan>),
    Or(Vec<Plan>),
    Not(Box<Plan>),
    /// `genFieldLeaf` over the target's declared fields, else `fallback`.
    Field { target: Target, plan: FieldPlan, fallback: Box<Plan> },
    /// `genMismatchedFieldLeaf`, else `meta_exists(fallback_key)`.
    Mismatch { target: Target, seed: u32, fallback_key: String },
    /// `seedReference`: a committed reference (3/4), else "" (1/8) or a miss.
    Reference { hit: bool, empty: bool },
    /// `genDateLeaf` over the learned timestamps.
    TxDate { field: &'static str, a: u32, b: u32, fa: u64, fb: u64, roll: RangeRoll },
    /// A LOGS ledger leaf: the target ledger, or "no-such-ledger".
    LogLedger { other: bool },
    /// `genLogDateCond` around learned log dates.
    LogDate(LogDatePlan),
}

#[derive(Clone, Debug)]
pub struct LogDatePlan {
    has_min: bool,
    min_pick: u32,
    min_off: u64,
    min_ex: bool,
    want_max: bool,
    max_pick: u32,
    max_off: u64,
    max_ex: bool,
}

const LOG_DATE_JITTER: u64 = 1000;

impl LogDatePlan {
    fn resolve(&self, dates: &[u64]) -> Value {
        if dates.is_empty() {
            return json!({});
        }
        let at = |p: u32, off: u64| {
            // d ± jitter, clamped at 0 (M/queries_logs.go:105-113 guarded on
            // off > d, which underflowed for off <= d < jitter - off).
            dates[p as usize % dates.len()].saturating_add(off).saturating_sub(LOG_DATE_JITTER)
        };
        let mut m = Map::new();
        if self.has_min {
            m.insert("min".into(), json!(at(self.min_pick, self.min_off).to_string()));
            if self.min_ex {
                m.insert("minExclusive".into(), json!(true));
            }
        }
        if !self.has_min || self.want_max {
            m.insert("max".into(), json!(at(self.max_pick, self.max_off).to_string()));
            if self.max_ex {
                m.insert("maxExclusive".into(), json!(true));
            }
        }
        Value::Object(m)
    }
}

impl Plan {
    /// Builds the filter from the views. `None` is the absent filter.
    pub fn resolve(&self, ctx: &FilterCtx) -> Option<Value> {
        Some(match self {
            Plan::Universe => return None,
            Plan::Fixed(v) => v.clone(),
            Plan::And(c) => and(c.iter().map(|p| p.resolve(ctx).unwrap_or_else(|| json!({}))).collect()),
            Plan::Or(c) => or(c.iter().map(|p| p.resolve(ctx).unwrap_or_else(|| json!({}))).collect()),
            Plan::Not(c) => not(c.resolve(ctx).unwrap_or_else(|| json!({}))),
            Plan::Field { target, plan, fallback } => match plan.resolve(seeds(ctx, *target)) {
                Some(v) => v,
                None => return fallback.resolve(ctx),
            },
            Plan::Mismatch { target, seed, fallback_key } => mismatched(seeds(ctx, *target), *seed).unwrap_or_else(|| meta_exists(fallback_key, false)),
            Plan::Reference { hit, empty } => match (&ctx.reference, hit, empty) {
                (Some(r), true, _) => reference(r),
                (_, _, true) => reference(""),
                _ => reference("t-ref:absent"),
            },
            Plan::TxDate { field, a, b, fa, fb, roll } => {
                let (mut x, mut y) = if ctx.tx_stamps.is_empty() {
                    (*fa, *fb)
                } else {
                    (ctx.tx_stamps[*a as usize % ctx.tx_stamps.len()], ctx.tx_stamps[*b as usize % ctx.tx_stamps.len()])
                };
                if x > y {
                    std::mem::swap(&mut x, &mut y);
                }
                builtin_uint(field, roll.apply(x, y))
            }
            Plan::LogLedger { other } => log_ledger(if *other { "no-such-ledger" } else { &ctx.ledger }),
            Plan::LogDate(p) => log_date(p.resolve(&ctx.log_dates)),
        })
    }
}

fn seeds(ctx: &FilterCtx, target: Target) -> &[(String, String)] {
    match target {
        Target::Transactions => &ctx.tx_fields,
        _ => &ctx.account_fields,
    }
}

/// `genBoolean`: And/Or over 0..3 children ({0,1,2,2,3}), or Not.
fn gen_boolean(tc: &TestCase, depth: usize, gen: &dyn Fn(&TestCase, usize) -> Plan) -> Plan {
    let children = |tc: &TestCase| -> Vec<Plan> {
        let n = *draw::one_of(tc, &[0usize, 1, 2, 2, 3]);
        (0..n).map(|_| gen(tc, depth + 1)).collect()
    };
    match int_in(tc, 0, 2) {
        0 => Plan::And(children(tc)),
        1 => Plan::Or(children(tc)),
        _ => Plan::Not(Box::new(gen(tc, depth + 1))),
    }
}

/// `genAccountAssetFilter` (M/indexes.go): a has-asset leaf on a workload
/// asset; 1/16 a precision past the index's one-byte cell, 1/8 a cell
/// nothing touched.
pub fn gen_account_asset(tc: &TestCase) -> Value {
    let (base, precision) = *draw::one_of(tc, &[("USD", 2u64), ("EUR", 2), ("COIN", 0)]);
    if one_in(tc, 16) {
        return has_asset(base, 256 + int_in(tc, 0, 999));
    }
    if one_in(tc, 8) {
        return if one_in(tc, 2) { has_asset(base, precision + 4) } else { has_asset("ZZZ", 0) };
    }
    has_asset(base, precision)
}

/// `genAccountFilter` (sequential rolls as in M/queries.go).
pub fn gen_account_filter(tc: &TestCase) -> Plan {
    if one_in(tc, 8) {
        return Plan::Fixed(meta_exists(&format!("undeclared-{}", draw::meta_key_model(tc)), false));
    }
    if one_in(tc, 12) {
        return Plan::Mismatch { target: Target::Accounts, seed: pick_u32(tc), fallback_key: draw::meta_key_model(tc) };
    }
    if one_in(tc, 6) {
        return Plan::Fixed(gen_account_asset(tc));
    }
    if one_in(tc, 16) {
        return Plan::Fixed(if one_in(tc, 2) { reverted(true) } else { tx_id_range(0, 255) });
    }
    if one_in(tc, 4) {
        return Plan::Universe;
    }
    if one_in(tc, 2) {
        gen_account_indexed(tc, 0)
    } else {
        gen_account_free(tc, 0)
    }
}

/// `genAccountFilterIndexed` at the top level: metadata Field leaves on the
/// declared keys, has-asset leaves and address leaves, composed. Task 05's
/// AggregateVolumes draws it (M/aggregates.go runAggregateQuery).
pub fn gen_account_filter_indexed(tc: &TestCase) -> Plan {
    gen_account_indexed(tc, 0)
}

/// `genAccountFilterFree` at the top level (index-free address filters).
pub fn gen_account_filter_free(tc: &TestCase) -> Plan {
    gen_account_free(tc, 0)
}

fn gen_account_indexed(tc: &TestCase, depth: usize) -> Plan {
    if depth >= MAX_GEN_DEPTH || one_in(tc, 2) {
        if one_in(tc, 4) {
            return Plan::Fixed(gen_account_asset(tc));
        }
        let plan = draw_field(tc);
        let use_field = !one_in(tc, 3);
        let fallback = gen_account_free(tc, depth);
        return if use_field { Plan::Field { target: Target::Accounts, plan, fallback: Box::new(fallback) } } else { fallback };
    }
    gen_boolean(tc, depth, &gen_account_indexed)
}

fn gen_account_free(tc: &TestCase, depth: usize) -> Plan {
    if depth >= MAX_GEN_DEPTH || one_in(tc, 2) {
        let role = int_in(tc, 0, 2);
        return Plan::Fixed(if one_in(tc, 2) { addr_prefix(&format!("{}:", pool_name(tc)), role) } else { addr_exact(&draw::pool_address(tc), role) });
    }
    gen_boolean(tc, depth, &gen_account_free)
}

/// `genTransactionFilter` (M/queries.go).
pub fn gen_transaction_filter(tc: &TestCase) -> Plan {
    if one_in(tc, 8) {
        return Plan::Fixed(meta_exists(&format!("undeclared-{}", draw::meta_key_model(tc)), false));
    }
    if one_in(tc, 12) {
        return Plan::Mismatch { target: Target::Transactions, seed: pick_u32(tc), fallback_key: draw::meta_key_model(tc) };
    }
    if one_in(tc, 16) {
        return Plan::Fixed(has_asset("USD", 2));
    }
    if one_in(tc, 4) {
        return Plan::Universe;
    }
    if !one_in(tc, 3) {
        gen_tx_indexed(tc, 0)
    } else {
        gen_tx_free(tc, 0)
    }
}

fn gen_tx_indexed(tc: &TestCase, depth: usize) -> Plan {
    if depth >= MAX_GEN_DEPTH || one_in(tc, 2) {
        return match int_in(tc, 0, 9) {
            0 => Plan::Reference { hit: !one_in(tc, 4), empty: one_in(tc, 8) },
            1..=3 => gen_tx_date(tc),
            4..=6 => Plan::Fixed(gen_tx_address(tc)),
            7 | 8 => {
                let plan = draw_field(tc);
                Plan::Field { target: Target::Transactions, plan, fallback: Box::new(gen_tx_free(tc, depth)) }
            }
            _ => gen_tx_free(tc, depth),
        };
    }
    gen_boolean(tc, depth, &gen_tx_indexed)
}

fn gen_tx_date(tc: &TestCase) -> Plan {
    let field = *draw::one_of(tc, &["TX_BUILTIN_INDEX_TIMESTAMP", "TX_BUILTIN_INDEX_INSERTED_AT", "TX_BUILTIN_INDEX_REVERTED_AT"]);
    Plan::TxDate { field, a: pick_u32(tc), b: pick_u32(tc), fa: int_in(tc, 0, 1023), fb: int_in(tc, 0, 1023), roll: draw_range_roll(tc) }
}

/// `genTxAddressLeaf`: a pool address (exact or cut to its type prefix),
/// `world`, or a broad or unmatchable prefix, under a random role.
fn gen_tx_address(tc: &TestCase) -> Value {
    let role = int_in(tc, 0, 2);
    let addr = draw::pool_address(tc);
    match int_in(tc, 0, 3) {
        0 => addr_exact(&addr, role),
        1 => addr_exact("world", role),
        2 => addr_prefix(&addr[..addr.find(':').unwrap_or(0) + 1], role),
        _ => addr_prefix(if one_in(tc, 8) { "absent:" } else { "t-" }, role),
    }
}

fn gen_tx_free(tc: &TestCase, depth: usize) -> Plan {
    if depth >= MAX_GEN_DEPTH || one_in(tc, 2) {
        return Plan::Fixed(match int_in(tc, 0, 2) {
            0 => reverted(true),
            1 => reverted(false),
            _ => {
                let lo = int_in(tc, 0, 255);
                let hi = lo + int_in(tc, 0, 255);
                builtin_uint("TX_BUILTIN_INDEX_ID", draw_range_roll(tc).apply(lo, hi))
            }
        });
    }
    gen_boolean(tc, depth, &gen_tx_free)
}

/// One read in twenty-four: a range over a builtin with no uint64 order
/// (`rollUnsupportedBuiltinField`).
pub fn roll_unsupported_builtin(tc: &TestCase) -> Option<Value> {
    if !one_in(tc, 24) {
        return None;
    }
    let field = *draw::one_of(tc, &["TX_BUILTIN_INDEX_REFERENCE", "TX_BUILTIN_INDEX_ADDRESS", "TX_BUILTIN_INDEX_SOURCE_ADDRESS", "TX_BUILTIN_INDEX_DESTINATION_ADDRESS"]);
    Some(builtin_uint(field, uint_cond(Some(1), Some(1_000_000))))
}

/// `genLogFilter`: LOGS-valid leaves (ledger name, log id range, log date
/// range) under combinators, depth ≤ 2.
pub fn gen_log_filter(tc: &TestCase, depth: usize) -> Plan {
    if depth >= 2 || one_in(tc, 3) {
        return match int_in(tc, 0, 2) {
            0 => Plan::LogLedger { other: one_in(tc, 4) },
            1 => {
                let mut m = Map::new();
                if one_in(tc, 2) {
                    m.insert("min".into(), json!(int_in(tc, 0, 31).to_string()));
                    m.insert("minExclusive".into(), json!(one_in(tc, 2)));
                }
                if one_in(tc, 2) {
                    m.insert("max".into(), json!((8 + int_in(tc, 0, 63)).to_string()));
                    m.insert("maxExclusive".into(), json!(one_in(tc, 2)));
                }
                Plan::Fixed(log_id(Value::Object(m)))
            }
            _ => Plan::LogDate(LogDatePlan {
                has_min: one_in(tc, 2),
                min_pick: pick_u32(tc),
                min_off: int_in(tc, 0, 2 * LOG_DATE_JITTER),
                min_ex: one_in(tc, 2),
                want_max: one_in(tc, 2),
                max_pick: pick_u32(tc),
                max_off: int_in(tc, 0, 2 * LOG_DATE_JITTER),
                max_ex: one_in(tc, 2),
            }),
        };
    }
    gen_boolean(tc, depth, &gen_log_filter)
}

// ---------------------------------------------------------------- arbitrary shapes

fn any_enum(tc: &TestCase, max_declared: u64) -> u64 {
    match int_in(tc, 0, 7) {
        0 => max_declared + 1,
        1 => i32::MAX as u64,
        _ => int_in(tc, 0, max_declared),
    }
}

fn any_string(tc: &TestCase) -> String {
    match int_in(tc, 0, 5) {
        0 => String::new(),
        1 => "a".into(),
        2 => "\u{0}".into(),
        3 => format!("{}:", pool_name(tc)),
        4 => draw::pool_address(tc),
        _ => draw::meta_key_model(tc),
    }
}

fn any_param(tc: &TestCase) -> String {
    draw::one_of(tc, &["", "p0", "p1"]).to_string()
}

/// `anyStringCondition`; `None` is an absent condition.
fn any_string_cond(tc: &TestCase) -> Option<Value> {
    match int_in(tc, 0, 3) {
        0 => None,
        1 => Some(json!({})),
        2 => Some(json!({"hardcoded": any_string(tc)})),
        _ => Some(json!({"param": any_param(tc)})),
    }
}

fn any_uint_cond(tc: &TestCase) -> Option<Value> {
    if one_in(tc, 6) {
        return None;
    }
    let bound = |tc: &TestCase| -> Option<u64> {
        if one_in(tc, 2) {
            return None;
        }
        Some(match int_in(tc, 0, 7) {
            0 => 0,
            1 => 1,
            2 => 2,
            3 => 5,
            4 => 100,
            5 => u64::MAX - 1,
            6 => u64::MAX,
            _ => int_in(tc, 0, u64::MAX),
        })
    };
    let (lo, hi) = (bound(tc), bound(tc));
    let mut m = uint_cond(lo, hi);
    if one_in(tc, 2) {
        m.insert("minExclusive".into(), json!(true));
    }
    if one_in(tc, 2) {
        m.insert("maxExclusive".into(), json!(true));
    }
    if one_in(tc, 6) {
        m.insert("paramMin".into(), json!(any_param(tc)));
    }
    if one_in(tc, 6) {
        m.insert("paramMax".into(), json!(any_param(tc)));
    }
    Some(Value::Object(m))
}

fn any_int_cond(tc: &TestCase) -> Value {
    let bound = |tc: &TestCase| -> Option<i64> {
        if one_in(tc, 2) {
            None
        } else {
            Some(*draw::one_of(tc, &[i64::MIN, -1, 0, 1, 5, i64::MAX]))
        }
    };
    let (lo, hi) = (bound(tc), bound(tc));
    let mut v = int_cond(lo, hi, false, false);
    let m = v.as_object_mut().unwrap();
    if one_in(tc, 2) {
        m.insert("minExclusive".into(), json!(true));
    }
    if one_in(tc, 2) {
        m.insert("maxExclusive".into(), json!(true));
    }
    if one_in(tc, 6) {
        m.insert("paramMin".into(), json!(any_param(tc)));
    }
    if one_in(tc, 6) {
        m.insert("paramMax".into(), json!(any_param(tc)));
    }
    v
}

fn any_field_condition(tc: &TestCase) -> Value {
    let mut fc = Map::new();
    if !one_in(tc, 6) {
        let key = match int_in(tc, 0, 2) {
            0 => String::new(),
            _ => draw::meta_key_model(tc),
        };
        fc.insert("field".into(), json!({"metadata": key}));
    }
    match int_in(tc, 0, 5) {
        0 => {}
        1 => {
            fc.insert("stringCond".into(), any_string_cond(tc).unwrap_or_else(|| json!({})));
        }
        2 => {
            fc.insert("intCond".into(), any_int_cond(tc));
        }
        3 => {
            fc.insert("uintCond".into(), any_uint_cond(tc).unwrap_or_else(|| json!({})));
        }
        4 => {
            let b = match int_in(tc, 0, 2) {
                0 => json!({}),
                1 => json!({"hardcoded": one_in(tc, 2)}),
                _ => json!({"param": any_param(tc)}),
            };
            fc.insert("boolCond".into(), b);
        }
        _ => {
            fc.insert("existsCond".into(), if one_in(tc, 2) { json!({"includeNull": true}) } else { json!({}) });
        }
    }
    Value::Object(fc)
}

fn any_address_match(tc: &TestCase) -> Value {
    let mut am = Map::new();
    am.insert("role".into(), role_name(any_enum(tc, 2)));
    match int_in(tc, 0, 4) {
        0 => {}
        1 => {
            am.insert("hardcodedPrefix".into(), json!(any_string(tc)));
        }
        2 => {
            am.insert("hardcodedExact".into(), json!(any_string(tc)));
        }
        3 => {
            am.insert("paramPrefix".into(), json!(any_param(tc)));
        }
        _ => {
            am.insert("paramExact".into(), json!(any_param(tc)));
        }
    }
    Value::Object(am)
}

const AUDIT_FIELDS: [&str; 10] = [
    "AUDIT_FIELD_UNSPECIFIED",
    "AUDIT_FIELD_SEQUENCE",
    "AUDIT_FIELD_PROPOSAL_ID",
    "AUDIT_FIELD_TIMESTAMP",
    "AUDIT_FIELD_LOG_SEQUENCE",
    "AUDIT_FIELD_OUTCOME",
    "AUDIT_FIELD_CALLER_SUBJECT",
    "AUDIT_FIELD_LEDGER",
    "AUDIT_FIELD_ORDER_TYPE",
    "AUDIT_FIELD_IDEMPOTENCY_KEY",
];

fn any_audit_condition(tc: &TestCase) -> Value {
    let mut ac = Map::new();
    ac.insert("field".into(), enum_value(&AUDIT_FIELDS, any_enum(tc, 9)));
    match int_in(tc, 0, 3) {
        0 => {}
        1 => {
            ac.insert("stringCond".into(), any_string_cond(tc).unwrap_or_else(|| json!({})));
        }
        2 => {
            ac.insert("uintCond".into(), any_uint_cond(tc).unwrap_or_else(|| json!({})));
        }
        _ => {
            ac.insert("stringPrefix".into(), json!(any_string(tc)));
        }
    }
    Value::Object(ac)
}

fn with_cond(arm: &str, cond: Option<Value>) -> Value {
    let mut inner = Map::new();
    if let Some(c) = cond {
        inner.insert("cond".into(), c);
    }
    let mut m = Map::new();
    m.insert(arm.into(), Value::Object(inner));
    Value::Object(m)
}

/// `genAnyLeaf`: one leaf of any arm, every sub-field drawn independently
/// (including unset oneofs and out-of-range enums), or an empty node.
pub fn gen_any_leaf(tc: &TestCase) -> Value {
    match int_in(tc, 0, 10) {
        0 => json!({"field": any_field_condition(tc)}),
        1 => json!({"address": any_address_match(tc)}),
        2 => with_cond("reference", any_string_cond(tc)),
        3 => {
            let field = enum_value(&TX_BUILTINS, any_enum(tc, 7));
            let mut b = Map::new();
            b.insert("field".into(), field);
            if let Some(c) = any_uint_cond(tc) {
                b.insert("cond".into(), c);
            }
            json!({"builtinUint": Value::Object(b)})
        }
        4 => with_cond("ledger", any_string_cond(tc)),
        5 => with_cond("logId", any_uint_cond(tc)),
        6 => {
            let field = enum_value(&["LOG_BUILTIN_INDEX_UNSPECIFIED", "LOG_BUILTIN_INDEX_DATE"], any_enum(tc, 1));
            let mut b = Map::new();
            b.insert("field".into(), field);
            if let Some(c) = any_uint_cond(tc) {
                b.insert("cond".into(), c);
            }
            json!({"logBuiltinUint": Value::Object(b)})
        }
        7 => {
            let base = *draw::one_of(tc, &["", "USD", "EUR", "COIN", "ZZZ"]);
            let precision = *draw::one_of(tc, &[0u64, 2, 255, 256, u32::MAX as u64]);
            has_asset(base, precision)
        }
        8 => reverted(one_in(tc, 2)),
        9 => json!({"audit": any_audit_condition(tc)}),
        _ => json!({}),
    }
}

/// `genAnyFilter`: combinators over 0..3 children, an empty node, or a leaf.
pub fn gen_any_filter(tc: &TestCase, depth: usize) -> Value {
    if depth >= MAX_GEN_DEPTH || one_in(tc, 2) {
        if one_in(tc, 8) {
            return json!({});
        }
        return gen_any_leaf(tc);
    }
    let children = |tc: &TestCase| -> Vec<Value> { (0..int_in(tc, 0, 3)).map(|_| gen_any_filter(tc, depth + 1)).collect() };
    match int_in(tc, 0, 3) {
        0 => and(children(tc)),
        1 => or(children(tc)),
        2 => json!({"not": {}}),
        _ => not(gen_any_filter(tc, depth + 1)),
    }
}

/// `depthChain`: `n` Not nodes around `f`.
pub fn depth_chain(f: Value, n: u64) -> Value {
    let mut f = f;
    for _ in 0..n {
        f = not(f);
    }
    f
}

/// A pre-drawn `rollFilterShape` (one call in eight rewrites one node of the
/// resolved filter into an arbitrary wire shape).
#[derive(Clone, Debug)]
pub enum ShapeRoll {
    Keep,
    /// The filter was absent: replace it with this tree.
    Root(Value),
    /// Rewrite the node at `pick mod nodes` (tree order).
    Node { pick: u32, op: ShapeOp },
}

#[derive(Clone, Debug)]
pub enum ShapeOp {
    /// Unset the node's oneof.
    Unset,
    /// Wrap the node in Not.
    WrapNot,
    /// Replace with any leaf.
    Leaf(Value),
    /// Replace with any tree.
    Tree(Value),
    /// Wrap in a Not chain landing just under, at or past MaxFilterDepth.
    Chain(u64),
}

/// Draws the shape roll. `absent` says whether the filter will be absent
/// (known before `/read/begin`: only `Plan::Universe` resolves to none).
pub fn draw_shape(tc: &TestCase, absent: bool) -> ShapeRoll {
    if !one_in(tc, 8) {
        return ShapeRoll::Keep;
    }
    if absent {
        return ShapeRoll::Root(gen_any_filter(tc, 0));
    }
    let pick = pick_u32(tc);
    let op = match int_in(tc, 0, 4) {
        0 => ShapeOp::Unset,
        1 => ShapeOp::WrapNot,
        2 => ShapeOp::Leaf(gen_any_leaf(tc)),
        3 => ShapeOp::Tree(gen_any_filter(tc, 1)),
        _ => ShapeOp::Chain(MAX_FILTER_DEPTH - 2 + int_in(tc, 0, 3)),
    };
    ShapeRoll::Node { pick, op }
}

/// Lists the paths of every node in tree order (`collectNodes`).
fn collect_paths(f: &Value, path: &mut Vec<usize>, out: &mut Vec<Vec<usize>>) {
    out.push(path.clone());
    for (i, child) in children_of(f).into_iter().enumerate() {
        path.push(i);
        collect_paths(child, path, out);
        path.pop();
    }
}

fn children_of(f: &Value) -> Vec<&Value> {
    if let Some(a) = f.pointer("/and/filters").or_else(|| f.pointer("/or/filters")).and_then(Value::as_array) {
        return a.iter().collect();
    }
    f.pointer("/not/filter").into_iter().collect()
}

fn child_mut(f: &mut Value, i: usize) -> Option<&mut Value> {
    if f.pointer("/and/filters").is_some() {
        return f.pointer_mut("/and/filters").and_then(|a| a.get_mut(i));
    }
    if f.pointer("/or/filters").is_some() {
        return f.pointer_mut("/or/filters").and_then(|a| a.get_mut(i));
    }
    f.pointer_mut("/not/filter")
}

/// Every node of a filter, in tree order (for tests).
pub fn node_count(f: &Value) -> usize {
    let mut out = vec![];
    collect_paths(f, &mut vec![], &mut out);
    out.len()
}

impl ShapeRoll {
    pub fn apply(&self, f: Option<Value>) -> Option<Value> {
        match (self, f) {
            (ShapeRoll::Keep, f) => f,
            (ShapeRoll::Root(v), None) => Some(v.clone()),
            (ShapeRoll::Root(_), f) => f,
            (ShapeRoll::Node { .. }, None) => None,
            (ShapeRoll::Node { pick, op }, Some(mut f)) => {
                let mut paths = vec![];
                collect_paths(&f, &mut vec![], &mut paths);
                let path = &paths[*pick as usize % paths.len()];
                let mut node = &mut f;
                for &i in path {
                    node = child_mut(node, i).expect("collected path exists");
                }
                let old = std::mem::replace(node, json!({}));
                *node = match op {
                    ShapeOp::Unset => json!({}),
                    ShapeOp::WrapNot => not(old),
                    ShapeOp::Leaf(v) | ShapeOp::Tree(v) => v.clone(),
                    ShapeOp::Chain(n) => depth_chain(old, *n),
                };
                Some(f)
            }
        }
    }
}

/// The nesting depth of a filter (a leaf is 1).
pub fn filter_depth(f: &Value) -> usize {
    1 + children_of(f).into_iter().map(filter_depth).max().unwrap_or(0)
}

/// Whether some node references a query parameter (which a plain list query
/// never binds, so the server must refuse it).
pub fn has_param(f: &Value) -> bool {
    match f {
        Value::Object(m) => m.iter().any(|(k, v)| (k.starts_with("param") && v.is_string()) || has_param(v)),
        Value::Array(a) => a.iter().any(has_param),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testkit;
    use std::collections::BTreeSet;
    use std::path::PathBuf;
    use std::sync::Mutex;

    fn fixtures_dir() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../protocol-fixtures/06")
    }

    fn ctx() -> FilterCtx {
        FilterCtx {
            ledger: "o-test-l3".into(),
            account_fields: vec![("k0".into(), "METADATA_TYPE_STRING".into()), ("k2".into(), "METADATA_TYPE_BOOL".into())],
            tx_fields: vec![("k1".into(), "METADATA_TYPE_INT64".into()), ("k3".into(), "METADATA_TYPE_UINT64".into())],
            reference: Some("o-test-ref7".into()),
            tx_stamps: vec![1_700_000_000_000_000, 1_700_000_000_000_100],
            log_dates: vec![1_700_000_000_000_050],
            log_count: 12,
        }
    }

    #[test]
    fn page_token_matches_go_fixture() {
        let raw = std::fs::read_to_string(fixtures_dir().join("page_tokens.json")).expect("06/page_tokens.json");
        let v: Value = serde_json::from_str(&raw).unwrap();
        let cases = v["cases"].as_array().unwrap();
        assert!(cases.len() >= 10);
        for c in cases {
            let key = c["key"].as_str().unwrap();
            assert_eq!(page_token(key), c["token"].as_str().unwrap(), "key {key:?}");
        }
        assert_eq!(page_token(""), "");
    }

    #[test]
    fn effective_page_size_mirrors_the_server_clamp() {
        assert_eq!(effective_page_size(0), 100);
        assert_eq!(effective_page_size(7), 7);
        assert_eq!(effective_page_size(1000), 1000);
        assert_eq!(effective_page_size(1001), 1000);
    }

    /// An enum field serialized as a number: enums in range go by name
    /// (`role_name`, `enum_value`), so a number is an out-of-range value.
    fn has_numeric_enum(f: &Value) -> bool {
        match f {
            Value::Object(m) => m.iter().any(|(k, v)| ((k == "role" || k == "field") && v.is_number()) || has_numeric_enum(v)),
            Value::Array(a) => a.iter().any(has_numeric_enum),
            _ => false,
        }
    }

    #[test]
    fn numeric_enum_detector() {
        assert!(has_numeric_enum(&json!({"and": {"filters": [{"builtinUint": {"field": 8}}]}})));
        assert!(has_numeric_enum(&json!({"address": {"role": 2147483647}})));
        assert!(!has_numeric_enum(&json!({"address": {"role": "ADDRESS_ROLE_ANY", "hardcodedPrefix": ":3,"}})));
    }

    #[test]
    fn log_date_jitter_never_underflows() {
        let p = LogDatePlan { has_min: true, min_pick: 0, min_off: 500, min_ex: false, want_max: false, max_pick: 0, max_off: 0, max_ex: false };
        // off <= d < jitter - off: the old guard subtracted past zero.
        assert_eq!(p.resolve(&[600]), json!({"min": "100"}));
        assert_eq!(p.resolve(&[400]), json!({"min": "0"}));
        assert_eq!(p.resolve(&[10_000]), json!({"min": "9500"}));
    }

    #[test]
    fn malformed_cursor_menu_and_rate() {
        let seen = Mutex::new((0u32, 0u32, BTreeSet::new()));
        testkit::with_tc_n(400, |tc| {
            let mut s = seen.lock().unwrap();
            s.0 += 1;
            if let Some(c) = roll_malformed_cursor(tc) {
                s.1 += 1;
                assert!(MALFORMED_CURSORS.contains(&c.as_str()) || MALFORMED_CURSORS.iter().any(|b| page_token(b) == c), "{c}");
                s.2.insert(c);
            }
        });
        let s = seen.lock().unwrap();
        assert!(s.1 > 0, "a malformed cursor is drawn sometimes");
        assert!(s.1 < s.0, "and not always");
    }

    /// The shape classes `rollFilterShape` can produce: kept, root, unset,
    /// wrapped, any leaf, any tree, depth chain past the limit — and the
    /// arbitrary leaves cover unset oneofs, parameter references and
    /// out-of-range enums.
    #[test]
    fn roll_filter_shape_produces_each_shape_class() {
        let classes = Mutex::new(BTreeSet::new());
        testkit::with_tc_n(1500, |tc| {
            let absent = chance(tc, 0.2);
            let roll = draw_shape(tc, absent);
            let base = if absent { None } else { Some(and(vec![addr_prefix("t-1:", 0), reverted(true)])) };
            let out = roll.apply(base.clone());
            let mut c = classes.lock().unwrap();
            match &roll {
                ShapeRoll::Keep => {
                    assert_eq!(out, base);
                    c.insert("keep");
                }
                ShapeRoll::Root(_) => {
                    assert!(base.is_none() && out.is_some());
                    c.insert("root");
                }
                ShapeRoll::Node { op, .. } => {
                    let f = out.unwrap();
                    let class = match op {
                        ShapeOp::Unset => "unset",
                        ShapeOp::WrapNot => "wrap",
                        ShapeOp::Leaf(_) => "leaf",
                        ShapeOp::Tree(_) => "tree",
                        ShapeOp::Chain(n) => {
                            assert!(filter_depth(&f) as u64 > *n);
                            if filter_depth(&f) as u64 > MAX_FILTER_DEPTH {
                                c.insert("past-max-depth");
                            }
                            "chain"
                        }
                    };
                    c.insert(class);
                    if has_param(&f) {
                        c.insert("param");
                    }
                    if has_numeric_enum(&f) {
                        c.insert("enum-out-of-range");
                    }
                    let s = f.to_string();
                    if s.contains("{}") {
                        c.insert("unset-oneof");
                    }
                }
            }
        });
        let c = classes.lock().unwrap();
        for want in ["keep", "root", "unset", "wrap", "leaf", "tree", "chain", "past-max-depth", "param", "unset-oneof", "enum-out-of-range"] {
            assert!(c.contains(want), "missing shape class {want}: {c:?}");
        }
    }

    #[test]
    fn generators_resolve_and_never_produce_reserved_names() {
        let ctx = ctx();
        testkit::with_tc_n(300, |tc| {
            for (target, plan) in [(Target::Accounts, gen_account_filter(tc)), (Target::Transactions, gen_transaction_filter(tc)), (Target::Logs, gen_log_filter(tc, 0))] {
                let absent = matches!(plan, Plan::Universe);
                let f = draw_shape(tc, absent).apply(plan.resolve(&ctx));
                if let Some(f) = &f {
                    let s = f.to_string();
                    assert!(!s.contains("own-"), "{target:?}: {s}");
                    assert!(!s.contains("null"), "no JSON null in a protojson filter: {s}");
                }
                if absent {
                    assert!(plan.resolve(&ctx).is_none());
                }
            }
            if let Some(u) = roll_unsupported_builtin(tc) {
                assert!(u["builtinUint"]["field"].as_str().unwrap() != "TX_BUILTIN_INDEX_ID");
            }
        });
    }

    #[test]
    fn plans_resolve_against_the_views() {
        let c = ctx();
        let fp = FieldPlan { seed: 0, exists: false, include_null: false, str_hit: false, str_value: 0, bool_value: true, center: 10, lo: BoundPlan { open: false, extremum: None, delta: 3, exclusive: false }, hi: BoundPlan { open: true, extremum: None, delta: 0, exclusive: false }, split: false };
        let p = Plan::Field { target: Target::Accounts, plan: fp.clone(), fallback: Box::new(Plan::Fixed(reverted(true))) };
        assert_eq!(p.resolve(&c), Some(field_string("k0", "absent-value")));
        let p = Plan::Field { target: Target::Transactions, plan: fp.clone(), fallback: Box::new(Plan::Fixed(reverted(true))) };
        assert_eq!(p.resolve(&c), Some(field_with("k1", "intCond", json!({"min": "7"}))));
        let empty = FilterCtx::default();
        assert_eq!(p.resolve(&empty), Some(reverted(true)), "no declared field: the fallback");
        assert_eq!(Plan::Mismatch { target: Target::Accounts, seed: 1, fallback_key: "k1".into() }.resolve(&c), Some(field_with("k2", "intCond", json!({"min": "0"}))));
        assert_eq!(Plan::Mismatch { target: Target::Accounts, seed: 1, fallback_key: "k1".into() }.resolve(&empty), Some(meta_exists("k1", false)));
        assert_eq!(Plan::Reference { hit: true, empty: false }.resolve(&c), Some(reference("o-test-ref7")));
        assert_eq!(Plan::Reference { hit: true, empty: false }.resolve(&empty), Some(reference("t-ref:absent")));
        assert_eq!(Plan::LogLedger { other: false }.resolve(&c), Some(log_ledger("o-test-l3")));
        let ld = LogDatePlan { has_min: true, min_pick: 0, min_off: 0, min_ex: false, want_max: false, max_pick: 0, max_off: 0, max_ex: false };
        assert_eq!(Plan::LogDate(ld.clone()).resolve(&c), Some(log_date(json!({"min": "1699999999999050"}))));
        assert_eq!(Plan::LogDate(ld).resolve(&empty), Some(log_date(json!({}))));
        let views = FilterCtx::from_views(
            &json!({"ledger": "L", "logCount": 3, "fieldTypes": {"account": {"b": "METADATA_TYPE_BOOL", "a": "METADATA_TYPE_STRING"}, "transaction": {}}}),
            Some(&json!({"found": true, "entry": {"reference": "r", "id": "2"}})),
            Some(&json!({"txDates": ["5", "9"], "logDates": [], "maxLogSeq": "4"})),
        );
        assert_eq!(views.account_fields[0].0, "a");
        assert_eq!(views.reference.as_deref(), Some("r"));
        assert_eq!(views.tx_stamps, vec![5, 9]);
        assert_eq!(views.log_count, 3);
    }

    /// The deterministic builder cases of `06/filters/builders.json` and a
    /// fixed-seed sample of generator output in `06/filters/generated.json`:
    /// the Go test `TestFilterFixtures_ClientAccepts` unmarshals every one
    /// with the client's protojson options. `UPDATE_FIXTURES=1` rewrites
    /// them; without it both files must match the current builders and
    /// generators exactly, so the Go acceptance test cannot go stale.
    #[test]
    fn filter_fixtures() {
        let dir = fixtures_dir().join("filters");
        let builders = serde_json::to_string_pretty(&json!({"cases": builder_cases()})).unwrap() + "\n";
        let generated = serde_json::to_string(&json!({"cases": generated_sample()})).unwrap() + "\n";
        let (bpath, gpath) = (dir.join("builders.json"), dir.join("generated.json"));
        if std::env::var("UPDATE_FIXTURES").is_ok() {
            std::fs::create_dir_all(&dir).unwrap();
            std::fs::write(&bpath, &builders).unwrap();
            std::fs::write(&gpath, &generated).unwrap();
        }
        // Compared as text: the depth-chain case nests past serde_json's
        // parser recursion limit.
        let got = std::fs::read_to_string(&bpath).expect("06/filters/builders.json");
        assert_eq!(got, builders, "rerun with UPDATE_FIXTURES=1 and the Go fixture test");
        let got = std::fs::read_to_string(&gpath).expect("06/filters/generated.json");
        assert!(got == generated, "06/filters/generated.json is stale: rerun with UPDATE_FIXTURES=1 and the Go fixture test");
    }

    /// 200 fixed-seed generator draws per target, resolved against [`ctx`].
    fn generated_sample() -> Vec<Value> {
        let c = ctx();
        let out = Mutex::new(Vec::new());
        let settings = hegel::Settings::new().test_cases(200).seed(Some(6)).database(hegel::Database::Disabled);
        hegel::Hegel::new(|tc: TestCase| {
            let mut o = out.lock().unwrap();
            for (target, plan) in [("ACCOUNTS", gen_account_filter(&tc)), ("TRANSACTIONS", gen_transaction_filter(&tc)), ("LOGS", gen_log_filter(&tc, 0))] {
                let absent = matches!(plan, Plan::Universe);
                if let Some(f) = draw_shape(&tc, absent).apply(plan.resolve(&c)) {
                    o.push(json!({"target": target, "filter": f}));
                }
            }
        })
        .settings(settings)
        .run();
        out.into_inner().unwrap()
    }

    fn builder_cases() -> Vec<Value> {
        let deep = depth_chain(addr_prefix("t-0:", 0), MAX_FILTER_DEPTH + 1);
        vec![
            json!({"name": "addr-prefix-role", "target": "ACCOUNTS", "filter": addr_prefix("t-3:", 1), "refused": false}),
            json!({"name": "addr-exact", "target": "TRANSACTIONS", "filter": addr_exact("world", 2), "refused": false}),
            json!({"name": "reference", "target": "TRANSACTIONS", "filter": reference("o-test-ref1"), "refused": false}),
            json!({"name": "reverted-on-accounts", "target": "ACCOUNTS", "filter": reverted(true), "refused": true}),
            json!({"name": "tx-id-range", "target": "TRANSACTIONS", "filter": tx_id_range(0, 255), "refused": false}),
            json!({"name": "date-extremum", "target": "TRANSACTIONS", "filter": builtin_uint("TX_BUILTIN_INDEX_TIMESTAMP", uint_cond(Some(0), Some(u64::MAX))), "refused": false}),
            json!({"name": "has-asset-overflow", "target": "ACCOUNTS", "filter": has_asset("USD", 300), "refused": true}),
            json!({"name": "meta-exists", "target": "ACCOUNTS", "filter": meta_exists("undeclared-k1", true), "refused": false}),
            json!({"name": "field-int-split", "target": "TRANSACTIONS", "filter": and(vec![field_with("k1", "intCond", int_cond(Some(i64::MIN), None, true, false)), field_with("k1", "intCond", int_cond(None, Some(i64::MAX), false, true))]), "refused": false}),
            json!({"name": "field-bool", "target": "ACCOUNTS", "filter": field_bool("k2", false), "refused": false}),
            json!({"name": "empty-or-and-not", "target": "TRANSACTIONS", "filter": or(vec![and(vec![]), not(reverted(false))]), "refused": false}),
            json!({"name": "log-leaves", "target": "LOGS", "filter": and(vec![log_ledger("no-such-ledger"), log_id(uint_cond_ex(Some(3), Some(9), true, false)), log_date(json!({}))]), "refused": false}),
            json!({"name": "unset-node", "target": "ACCOUNTS", "filter": and(vec![json!({}), addr_prefix("t-", 0)]), "refused": true}),
            json!({"name": "param-ref", "target": "TRANSACTIONS", "filter": json!({"reference": {"cond": {"param": "p0"}}}), "refused": true}),
            json!({"name": "role-out-of-range", "target": "ACCOUNTS", "filter": json!({"address": {"hardcodedPrefix": "t-", "role": 2147483647}}), "refused": false}),
            json!({"name": "builtin-out-of-range", "target": "TRANSACTIONS", "filter": json!({"builtinUint": {"field": 8, "cond": {"min": "1"}}}), "refused": true}),
            json!({"name": "audit-on-logs", "target": "LOGS", "filter": json!({"audit": {"field": "AUDIT_FIELD_LEDGER", "stringPrefix": "\u{0}"}}), "refused": true}),
            json!({"name": "not-without-child", "target": "LOGS", "filter": json!({"not": {}}), "refused": false}),
            json!({"name": "depth-past-max", "target": "ACCOUNTS", "filter": deep, "refused": true}),
            json!({"name": "inverted-range", "target": "TRANSACTIONS", "filter": tx_id_range(9, 3), "refused": false}),
        ]
    }
}
