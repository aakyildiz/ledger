//! Audit reads (task 10): `audit_list` (one `ListAuditEntries` page, read
//! kind `ListAuditEntries`) and `audit_entry` (`GetAuditEntry`, read kind
//! `GetAuditEntry`). Ports `M/audit.go` `runAuditQuery`/`runAuditEntryRead`
//! (filters per `genAuditFilter`, refused probes, `rollAuditCursor`, the
//! malformed-cursor menu) and `MD/parallel_driver_audit` (the verbatim
//! `audit trail should contain entries`). The oracle judges every page and
//! entry, foreign-tolerant (`W/oraclecheck/auditcheck`).
//!
//! Every draw happens before `/read/begin`: the rule aims with `/view`
//! (`auditSamples`, `replayable`, `ledgers`), draws the whole request, then
//! takes the ticket, sends and posts. Audit pages are read linearizably.
use crate::clients::{CallOpts, Outcome};
use crate::draw::{self, chance, int_in, one_of, pick};
use crate::machine::{rule, Driver};
use crate::read::ViewReq;
use base64::Engine;
use hegel::stateful::ConcurrentRule;
use hegel::TestCase;
use serde_json::{json, Map, Value};

pub fn rules() -> Vec<ConcurrentRule<Driver>> {
    vec![rule("audit_list", 1.0, audit_list), rule("audit_entry", 0.5, audit_entry)]
}

/// Extra items the client may stream past the effective page size, so an
/// overlong page still arrives `complete` and is judged.
const ITEM_SLACK: u64 = 64;

/// The audit order-type tokens a filter may ask for (`M/audit.go` `auditOrderTypes`).
pub const ORDER_TYPES: [&str; 13] = [
    "create_transaction",
    "revert_transaction",
    "add_metadata",
    "delete_metadata",
    "set_metadata_field_type",
    "remove_metadata_field_type",
    "create_index",
    "drop_index",
    "add_account_type",
    "remove_account_type",
    "update_default_enforcement_mode",
    "save_ledger_metadata",
    "delete_ledger_metadata",
];

/// Tokens the uint64-keyed endpoints must refuse (`M/cursors.go` `malformedCursors`).
pub const MALFORMED_CURSORS: [&str; 7] = ["not-a-cursor", "-1", "0x10", " 12", "12 ", "1.0", "18446744073709551616"];

/// The server's page size for a requested one (`match.EffectivePageSize`).
pub fn effective_page_size(requested: u64) -> u64 {
    match requested {
        0 => 100,
        n if n > 1000 => 1000,
        n => n,
    }
}

/// A forward page token for `key` (`pkg/pagecursor`: base64url, unpadded, of
/// `{"key":…}` as Go's `encoding/json` writes it).
pub fn page_token(key: &str) -> String {
    if key.is_empty() {
        return String::new();
    }
    let mut s = String::from("{\"key\":\"");
    for ch in key.chars() {
        match ch {
            '"' => s.push_str("\\\""),
            '\\' => s.push_str("\\\\"),
            '\n' => s.push_str("\\n"),
            '\r' => s.push_str("\\r"),
            '\t' => s.push_str("\\t"),
            '<' | '>' | '&' | '\u{2028}' | '\u{2029}' => s.push_str(&format!("\\u{:04x}", ch as u32)),
            c if (c as u32) < 0x20 => s.push_str(&format!("\\u{:04x}", c as u32)),
            c => s.push(c),
        }
    }
    s.push_str("\"}");
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(s.as_bytes())
}

// ---------------------------------------------------------------- views

/// One sampled served entry (the `auditSamples` view, decimal strings).
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Sample {
    pub seq: u64,
    pub proposal_id: u64,
    pub timestamp: u64,
    pub subject: String,
    pub key: String,
    pub ledgers: Vec<String>,
    pub failed: bool,
    pub min_log: u64,
    pub max_log: u64,
    /// The order types the oracle's model committed under this entry.
    pub order_types: Vec<String>,
}

fn num(v: &Value, k: &str) -> u64 {
    match &v[k] {
        Value::String(s) => s.parse().unwrap_or(0),
        Value::Number(n) => n.as_u64().unwrap_or(0),
        _ => 0,
    }
}

fn strs(v: &Value) -> Vec<String> {
    v.as_array().map(|a| a.iter().filter_map(Value::as_str).map(str::to_string).collect()).unwrap_or_default()
}

pub fn parse_samples(v: &Value) -> Vec<Sample> {
    v["samples"]
        .as_array()
        .map(|a| {
            a.iter()
                .map(|s| Sample {
                    seq: num(s, "seq"),
                    proposal_id: num(s, "proposalId"),
                    timestamp: num(s, "timestamp"),
                    subject: s["subject"].as_str().unwrap_or("").to_string(),
                    key: s["key"].as_str().unwrap_or("").to_string(),
                    ledgers: strs(&s["ledgers"]),
                    failed: s["outcome"] == "failure",
                    min_log: num(s, "minLog"),
                    max_log: num(s, "maxLog"),
                    order_types: strs(&s["orderTypes"]),
                })
                .collect()
        })
        .unwrap_or_default()
}

/// What the generators aim at: the drawn sample, the ledger fleet, the
/// tracked key namespace and a committed log sequence.
#[derive(Clone, Debug, Default)]
pub struct GenCtx {
    pub sample: Sample,
    pub ledgers: Vec<String>,
    pub key_prefix: String,
    pub log_seq: u64,
}

impl GenCtx {
    /// Draws the sample (and a log sequence of a success sample) from the views.
    pub fn draw(tc: &TestCase, samples: &[Sample], tracked_live: &[String], key_prefix: &str) -> GenCtx {
        let sample = if samples.is_empty() { Sample::default() } else { samples[pick(tc, samples.len())].clone() };
        let mut ledgers: Vec<String> = tracked_live.to_vec();
        for s in samples {
            for l in &s.ledgers {
                if !ledgers.contains(l) {
                    ledgers.push(l.clone());
                }
            }
        }
        let with_logs: Vec<&Sample> = samples.iter().filter(|s| !s.failed && s.min_log != 0).collect();
        let log_seq = if with_logs.is_empty() {
            0
        } else {
            let s = with_logs[pick(tc, with_logs.len())];
            int_in(tc, s.min_log, s.max_log.max(s.min_log))
        };
        GenCtx { sample, ledgers, key_prefix: key_prefix.to_string(), log_seq }
    }
}

// ---------------------------------------------------------------- filters

/// The refusal a filter is built to draw (`M/audit.go` `auditProbe`); the
/// oracle derives it again from the request, this is for the hint only.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Probe {
    None,
    SeqInOr,
    Not,
    UnspecifiedField,
    TypeMismatch,
    KeyParam,
    NulPrefix,
}

impl Probe {
    pub fn name(self) -> &'static str {
        match self {
            Probe::None => "none",
            Probe::SeqInOr => "seq-in-or",
            Probe::Not => "not",
            Probe::UnspecifiedField => "unspecified-field",
            Probe::TypeMismatch => "type-mismatch",
            Probe::KeyParam => "key-param",
            Probe::NulPrefix => "nul-prefix",
        }
    }
}

/// A uint range: `None` bounds are open.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct UintRange {
    pub min: Option<u64>,
    pub max: Option<u64>,
    pub min_exclusive: bool,
    pub max_exclusive: bool,
}

impl UintRange {
    pub fn closed(lo: u64, hi: u64) -> Self {
        UintRange { min: Some(lo), max: Some(hi), ..Default::default() }
    }
    pub fn to_json(&self) -> Value {
        let mut o = Map::new();
        if let Some(m) = self.min {
            o.insert("min".into(), json!(m.to_string()));
        }
        if let Some(m) = self.max {
            o.insert("max".into(), json!(m.to_string()));
        }
        if self.min_exclusive {
            o.insert("minExclusive".into(), json!(true));
        }
        if self.max_exclusive {
            o.insert("maxExclusive".into(), json!(true));
        }
        Value::Object(o)
    }
}

pub fn audit_string(field: &str, value: &str) -> Value {
    json!({"audit": {"field": field, "stringCond": {"hardcoded": value}}})
}
pub fn audit_uint(field: &str, r: &UintRange) -> Value {
    json!({"audit": {"field": field, "uintCond": r.to_json()}})
}
pub fn key_prefix_leaf(prefix: &str) -> Value {
    json!({"audit": {"field": "AUDIT_FIELD_IDEMPOTENCY_KEY", "stringPrefix": prefix}})
}
pub fn key_param_leaf() -> Value {
    json!({"audit": {"field": "AUDIT_FIELD_IDEMPOTENCY_KEY", "stringCond": {"param": "p0"}}})
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

/// 0 or `u64::MAX`, the bounds where the server's normalisation has nowhere
/// to go (`M/queries.go` `uintExtremum`).
fn uint_extremum(tc: &TestCase) -> u64 {
    if chance(tc, 0.5) {
        0
    } else {
        u64::MAX
    }
}

/// Opens or makes exclusive either side (`M/queries.go` `rollOpenOrExclusive`).
fn roll_open_or_exclusive(tc: &TestCase, r: &mut UintRange) {
    if chance(tc, 0.25) {
        r.min = None;
    } else {
        r.min_exclusive = chance(tc, 0.25);
        if chance(tc, 1.0 / 16.0) {
            r.min = Some(uint_extremum(tc));
        }
    }
    if chance(tc, 0.25) {
        r.max = None;
    } else {
        r.max_exclusive = chance(tc, 0.25);
        if chance(tc, 1.0 / 16.0) {
            r.max = Some(uint_extremum(tc));
        }
    }
}

/// A two-sided range within `spread` of `center`, then opened or made
/// exclusive (`M/audit.go` `uintRangeAround`).
fn uint_range_around(tc: &TestCase, center: u64, spread: u64) -> UintRange {
    let lo = center - center.min(int_in(tc, 0, spread - 1));
    let hi = center.saturating_add(int_in(tc, 0, spread - 1));
    let mut r = UintRange::closed(lo, hi);
    roll_open_or_exclusive(tc, &mut r);
    r
}

/// A closed range that certainly holds a value and has no extremum, so the
/// seq-inside-Or refusal fires (`M/audit.go` `satisfiableSeqRange`).
fn satisfiable_seq_range(tc: &TestCase, center: u64) -> UintRange {
    let lo = center - center.min(int_in(tc, 0, 15));
    let hi = lo + int_in(tc, 0, 15);
    UintRange::closed(lo, hi)
}

/// The idempotency-key leaf, exact or prefix (`genIdempotencyKeyLeaf`).
fn key_leaf(tc: &TestCase, g: &GenCtx) -> Value {
    let mut key = g.sample.key.clone();
    if key.is_empty() || chance(tc, 1.0 / 8.0) {
        key = format!("{}-none", g.key_prefix);
    }
    if !chance(tc, 0.5) {
        return audit_string("AUDIT_FIELD_IDEMPOTENCY_KEY", &key);
    }
    let ns = g.key_prefix.len();
    let mut prefix = g.key_prefix.clone();
    if chance(tc, 0.5) && key.len() > ns && key.is_ascii() {
        prefix = key[..ns + pick(tc, key.len() - ns)].to_string();
    }
    key_prefix_leaf(&prefix)
}

/// One audit leaf over the eight fields (`genAuditLeaf`), aimed at the sample.
/// `indexed` excludes the sequence field (an Or child must be narrowed).
pub fn gen_leaf(tc: &TestCase, g: &GenCtx, indexed: bool) -> Value {
    let kind = if indexed { *one_of(tc, &[0u8, 1, 3, 4, 5, 6, 7, 8]) } else { int_in(tc, 0, 8) as u8 };
    match kind {
        0 => {
            let ledger = if g.ledgers.is_empty() || chance(tc, 1.0 / 8.0) { "no-such-ledger".to_string() } else { one_of(tc, &g.ledgers).clone() };
            audit_string("AUDIT_FIELD_LEDGER", &ledger)
        }
        1 => audit_string("AUDIT_FIELD_OUTCOME", one_of(tc, &["success", "failure"])),
        2 => audit_uint("AUDIT_FIELD_SEQUENCE", &uint_range_around(tc, g.sample.seq, 16)),
        3 => audit_uint("AUDIT_FIELD_PROPOSAL_ID", &uint_range_around(tc, g.sample.proposal_id, 16)),
        // A second either side of a served timestamp, in microseconds.
        4 => audit_uint("AUDIT_FIELD_TIMESTAMP", &uint_range_around(tc, g.sample.timestamp, 1_000_000)),
        5 => audit_uint("AUDIT_FIELD_LOG_SEQUENCE", &uint_range_around(tc, g.log_seq, 8)),
        6 => {
            let subject = if g.sample.subject.is_empty() || chance(tc, 1.0 / 8.0) { one_of(tc, &["nobody", ""]).to_string() } else { g.sample.subject.clone() };
            audit_string("AUDIT_FIELD_CALLER_SUBJECT", &subject)
        }
        7 => key_leaf(tc, g),
        _ => {
            // Aimed at an order type the sample's bulk committed most of the
            // time, so the order-type scope check and the known-entry
            // certification see selective filters that match.
            let token: String = if chance(tc, 1.0 / 8.0) {
                "no_such_order".to_string()
            } else if !g.sample.order_types.is_empty() && chance(tc, 0.75) {
                one_of(tc, &g.sample.order_types).clone()
            } else {
                one_of(tc, &ORDER_TYPES).to_string()
            };
            audit_string("AUDIT_FIELD_ORDER_TYPE", &token)
        }
    }
}

/// A leaf the audit compiler must refuse (`genRefusedAuditLeaf`).
pub fn gen_refused_leaf(tc: &TestCase, g: &GenCtx) -> (Value, Probe) {
    match int_in(tc, 0, 3) {
        0 => (audit_string("AUDIT_FIELD_UNSPECIFIED", "anything"), Probe::UnspecifiedField),
        1 => {
            if chance(tc, 0.5) {
                (audit_string("AUDIT_FIELD_SEQUENCE", "not-a-number"), Probe::TypeMismatch)
            } else {
                (audit_uint("AUDIT_FIELD_LEDGER", &uint_range_around(tc, g.sample.seq, 16)), Probe::TypeMismatch)
            }
        }
        2 => (key_param_leaf(), Probe::KeyParam),
        _ => (key_prefix_leaf(&format!("{}\u{0}bad", g.key_prefix)), Probe::NulPrefix),
    }
}

/// A ListAuditEntries filter (`genAuditFilter`): nothing, one leaf, an And or
/// Or of leaves, or a refused probe. Each arm is a fresh roll against the
/// ones above it.
pub fn gen_filter(tc: &TestCase, g: &GenCtx) -> (Option<Value>, Probe) {
    if chance(tc, 1.0 / 6.0) {
        return (None, Probe::None);
    }
    if chance(tc, 1.0 / 8.0) {
        if chance(tc, 0.5) {
            return (Some(not(gen_leaf(tc, g, false))), Probe::Not);
        }
        let seq = audit_uint("AUDIT_FIELD_SEQUENCE", &satisfiable_seq_range(tc, g.sample.seq));
        return (Some(or(vec![seq, gen_leaf(tc, g, true)])), Probe::SeqInOr);
    }
    if chance(tc, 0.25) {
        let (f, p) = gen_refused_leaf(tc, g);
        return (Some(f), p);
    }
    if chance(tc, 0.25) {
        let n = *one_of(tc, &[0usize, 1, 2, 2, 3]);
        return (Some(and((0..n).map(|_| gen_leaf(tc, g, false)).collect())), Probe::None);
    }
    if chance(tc, 1.0 / 6.0) {
        return (Some(or(vec![gen_leaf(tc, g, true), gen_leaf(tc, g, true)])), Probe::None);
    }
    (Some(gen_leaf(tc, g, false)), Probe::None)
}

/// A resume token half the time: a served sequence a few below it, so the
/// skip lands inside the zone (`rollAuditCursor`); then, one read in 16, a
/// malformed token, raw or wrapped (`rollMalformedCursor`).
pub fn gen_cursor(tc: &TestCase, samples: &[Sample]) -> (String, bool) {
    let mut cursor = String::new();
    if chance(tc, 0.5) {
        let seq = if samples.is_empty() { 0 } else { samples[pick(tc, samples.len())].seq };
        let seq = seq - seq.min(int_in(tc, 0, 7));
        if seq != 0 {
            cursor = page_token(&seq.to_string());
        }
    }
    if chance(tc, 1.0 / 16.0) {
        let bad = *one_of(tc, &MALFORMED_CURSORS);
        let token = if chance(tc, 0.5) { page_token(bad) } else { bad.to_string() };
        return (token, true);
    }
    (cursor, false)
}

/// The `ListOptions` of a request.
pub fn options(page_size: u64, cursor: &str, reverse: bool, filter: Option<&Value>) -> Value {
    let mut o = Map::new();
    if page_size != 0 {
        o.insert("pageSize".into(), json!(page_size));
    }
    if !cursor.is_empty() {
        o.insert("cursor".into(), json!(cursor));
    }
    if reverse {
        o.insert("reverse".into(), json!(true));
    }
    if let Some(f) = filter {
        o.insert("filter".into(), f.clone());
    }
    Value::Object(o)
}

/// `MD/parallel_driver_audit` (95): a successful, complete, unfiltered first
/// page holds entries — deterministic, because setup wrote entries (tracked
/// ledger creations through the oracle, the `default` ledger).
pub fn check_trail_has_entries(filter: Option<&Value>, cursor: &str, out: &Outcome) {
    if filter.is_some() || !cursor.is_empty() || !out.is_ok() || !out.complete {
        return;
    }
    let count = out.items.as_ref().map_or(0, Vec::len);
    check_always_or_unreachable!(count > 0, "audit trail should contain entries", json!({"count": count}));
}

fn tracked_keys(replayable: &Value) -> Vec<String> {
    replayable["entries"].as_array().map(|a| a.iter().filter_map(|e| e["key"].as_str()).map(str::to_string).collect()).unwrap_or_default()
}

/// `audit_list` (`M/audit.go` `runAuditQuery`): one page. 1/8 of the reads
/// filter on a tracked key from `replayable` (the audit half of
/// `MD/parallel_driver_bulk_atomicity`: the oracle checks that a key rejected
/// for good has no success entry and one failure entry per proposal).
pub fn audit_list(d: &Driver, tc: TestCase) {
    d.guarded("audit_list", &tc, |d, tc| {
        if !d.s.oracle_ok() {
            return None;
        }
        let views = d.view(&[ViewReq::named("auditSamples", json!({})), ViewReq::replayable(), ViewReq::ledgers()])?;
        let samples = parse_samples(&views[0]);
        let key_prefix = views[0]["keyPrefix"].as_str().map(str::to_string).unwrap_or_else(|| format!("{}k", d.s.tracked_prefix()));
        let g = GenCtx::draw(tc, &samples, &strs(&views[2]["live"]), &key_prefix);
        let keys = tracked_keys(&views[1]);
        let key_mode = chance(tc, 1.0 / 8.0) && !keys.is_empty();
        let (filter, probe) = if key_mode {
            (Some(audit_string("AUDIT_FIELD_IDEMPOTENCY_KEY", &keys[pick(tc, keys.len())])), Probe::None)
        } else {
            gen_filter(tc, &g)
        };
        let page_size = draw::page_size(tc);
        let reverse = chance(tc, 0.5);
        let (cursor, malformed) = gen_cursor(tc, &samples);
        let route = draw::route(tc, &d.s);

        let rt = d.read_begin("ListAuditEntries", &[], json!({}))?;
        let req = json!({"options": options(page_size, &cursor, reverse, filter.as_ref())});
        let opts = CallOpts::linearizable().max_items(effective_page_size(page_size) + ITEM_SLACK);
        let (_, out) = d.call(&route, "bucket", "ListAuditEntries", req.clone(), &opts);
        rt.end(&req, &out, &route, &opts, json!({"probe": probe.name(), "malformed": malformed, "keyMode": key_mode}), json!({}));
        check_trail_has_entries(filter.as_ref(), &cursor, &out);
        Some(())
    });
}

/// `audit_entry` (`M/audit.go` `runAuditEntryRead`): a sampled sequence, or
/// one read in four `u64::MAX`, which must be NotFound.
pub fn audit_entry(d: &Driver, tc: TestCase) {
    d.guarded("audit_entry", &tc, |d, tc| {
        if !d.s.oracle_ok() {
            return None;
        }
        let views = d.view(&[ViewReq::named("auditSamples", json!({}))])?;
        let samples = parse_samples(&views[0]);
        let seq = if chance(tc, 0.25) {
            u64::MAX
        } else if samples.is_empty() {
            return None;
        } else {
            samples[pick(tc, samples.len())].seq
        };
        let route = draw::route(tc, &d.s);

        let rt = d.read_begin("GetAuditEntry", &[], json!({}))?;
        let req = json!({"sequence": seq.to_string()});
        let opts = CallOpts::linearizable();
        let (_, out) = d.call(&route, "bucket", "GetAuditEntry", req.clone(), &opts);
        rt.end(&req, &out, &route, &opts, json!({"absent": seq == u64::MAX}), json!({}));
        Some(())
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testkit::{self, FakeReply};
    use std::path::PathBuf;

    fn fixtures_dir() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../protocol-fixtures/10")
    }

    fn sample_view() -> Value {
        json!({"keyPrefix": "o-test-k", "samples": [
            {"seq": "3", "proposalId": "11", "timestamp": "1700000000000000", "subject": "", "key": "o-test-k4", "ledgers": ["o-test-l1"], "outcome": "success", "minLog": "5", "maxLog": "6", "orderTypes": ["create_transaction"]},
            {"seq": "4", "proposalId": "12", "timestamp": "1700000000000100", "subject": "svc", "key": "", "ledgers": ["default"], "outcome": "failure", "minLog": "0", "maxLog": "0", "orderTypes": []}
        ]})
    }

    #[test]
    fn page_tokens_match_go_fixture() {
        let raw = std::fs::read_to_string(fixtures_dir().join("page_tokens.json")).expect("10/page_tokens.json");
        let fx: Value = serde_json::from_str(&raw).unwrap();
        let cases = fx["cases"].as_array().unwrap();
        assert!(!cases.is_empty());
        for c in cases {
            assert_eq!(page_token(c["key"].as_str().unwrap()), c["token"].as_str().unwrap(), "key {:?}", c["key"]);
        }
    }

    #[test]
    fn samples_parse() {
        let s = parse_samples(&sample_view());
        assert_eq!(s.len(), 2);
        assert_eq!(s[0].seq, 3);
        assert_eq!(s[0].min_log, 5);
        assert!(s[1].failed);
        assert_eq!(s[1].subject, "svc");
        assert_eq!(s[0].order_types, vec!["create_transaction".to_string()]);
        assert!(s[1].order_types.is_empty());
    }

    #[test]
    fn order_type_leaves_aim_at_the_sample() {
        let samples = parse_samples(&sample_view());
        let aimed = std::sync::atomic::AtomicUsize::new(0);
        testkit::with_tc_n(400, |tc| {
            let g = GenCtx { sample: samples[0].clone(), ledgers: vec![], key_prefix: "o-test-k".into(), log_seq: 5 };
            let leaf = gen_leaf(tc, &g, false);
            if leaf["audit"]["field"] == "AUDIT_FIELD_ORDER_TYPE" {
                let token = leaf["audit"]["stringCond"]["hardcoded"].as_str().unwrap().to_string();
                assert!(token == "no_such_order" || ORDER_TYPES.contains(&token.as_str()), "{token}");
                if token == "create_transaction" {
                    aimed.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
            }
        });
        assert!(aimed.load(std::sync::atomic::Ordering::Relaxed) > 0);
    }

    /// The fixed builder shapes (one per generator arm and every refused
    /// probe), with the probe each is built to draw. The Go test
    /// `TestFilterFixtures_ClientAccepts` unmarshals each with the client's
    /// strict protojson and checks the oracle classifies it the same way.
    fn builder_cases() -> Vec<(&'static str, Value, Probe)> {
        let r = UintRange { min: Some(3), max: Some(9), min_exclusive: true, max_exclusive: false };
        let open = UintRange { min: None, max: Some(u64::MAX), min_exclusive: false, max_exclusive: true };
        vec![
            ("ledger", audit_string("AUDIT_FIELD_LEDGER", "o-test-l1"), Probe::None),
            ("outcome", audit_string("AUDIT_FIELD_OUTCOME", "failure"), Probe::None),
            ("seq", audit_uint("AUDIT_FIELD_SEQUENCE", &r), Probe::None),
            ("proposal", audit_uint("AUDIT_FIELD_PROPOSAL_ID", &open), Probe::None),
            ("timestamp", audit_uint("AUDIT_FIELD_TIMESTAMP", &UintRange::closed(0, 1_700_000_000_000_000)), Probe::None),
            ("log-seq", audit_uint("AUDIT_FIELD_LOG_SEQUENCE", &UintRange::closed(4, 12)), Probe::None),
            ("subject-empty", audit_string("AUDIT_FIELD_CALLER_SUBJECT", ""), Probe::None),
            ("key-exact", audit_string("AUDIT_FIELD_IDEMPOTENCY_KEY", "o-test-k4"), Probe::None),
            ("key-prefix", key_prefix_leaf("o-test-k"), Probe::None),
            ("order-type", audit_string("AUDIT_FIELD_ORDER_TYPE", "add_metadata"), Probe::None),
            ("and-empty", and(vec![]), Probe::None),
            ("and", and(vec![audit_uint("AUDIT_FIELD_SEQUENCE", &r), audit_string("AUDIT_FIELD_OUTCOME", "success")]), Probe::None),
            ("or-indexed", or(vec![audit_string("AUDIT_FIELD_LEDGER", "default"), key_prefix_leaf("o-test-k1")]), Probe::None),
            ("seq-in-or", or(vec![audit_uint("AUDIT_FIELD_SEQUENCE", &UintRange::closed(1, 5)), audit_string("AUDIT_FIELD_LEDGER", "x")]), Probe::SeqInOr),
            ("not", not(audit_string("AUDIT_FIELD_OUTCOME", "success")), Probe::Not),
            ("unspecified", audit_string("AUDIT_FIELD_UNSPECIFIED", "anything"), Probe::UnspecifiedField),
            ("string-on-seq", audit_string("AUDIT_FIELD_SEQUENCE", "not-a-number"), Probe::TypeMismatch),
            ("uint-on-ledger", audit_uint("AUDIT_FIELD_LEDGER", &r), Probe::TypeMismatch),
            ("key-param", key_param_leaf(), Probe::KeyParam),
            ("key-nul-prefix", key_prefix_leaf("o-test-k\u{0}bad"), Probe::NulPrefix),
        ]
    }

    #[test]
    fn filter_fixtures() {
        let cases: Vec<Value> = builder_cases().into_iter().map(|(n, f, p)| json!({"name": n, "filter": f, "probe": p.name()})).collect();
        let doc = serde_json::to_string_pretty(&json!({"cases": cases})).unwrap() + "\n";
        let path = fixtures_dir().join("filters.json");
        if std::env::var("UPDATE_FIXTURES").is_ok() {
            std::fs::create_dir_all(fixtures_dir()).unwrap();
            std::fs::write(&path, &doc).unwrap();
        }
        let got = std::fs::read_to_string(&path).expect("10/filters.json");
        assert_eq!(got, doc, "rerun with UPDATE_FIXTURES=1 and the Go fixture test");
    }

    #[test]
    fn generated_filters_are_well_formed() {
        let samples = parse_samples(&sample_view());
        testkit::with_tc_n(300, |tc| {
            let g = GenCtx::draw(tc, &samples, &["o-test-l1".to_string()], "o-test-k");
            let (f, probe) = gen_filter(tc, &g);
            let Some(f) = f else { return };
            let text = f.to_string();
            assert!(!text.contains("own-"), "no reserved name: {text}");
            match probe {
                Probe::Not => assert!(f.get("not").is_some()),
                Probe::SeqInOr => assert_eq!(f["or"]["filters"][0]["audit"]["field"], "AUDIT_FIELD_SEQUENCE"),
                Probe::KeyParam => assert!(text.contains("\"param\"")),
                Probe::NulPrefix => assert!(text.contains("\\u0000")),
                _ => {}
            }
            if probe == Probe::None {
                // An Or child is never a bare sequence bound.
                if let Some(children) = f.get("or").and_then(|o| o["filters"].as_array()) {
                    assert!(children.iter().all(|c| c["audit"]["field"] != "AUDIT_FIELD_SEQUENCE"), "{text}");
                }
            }
            let (cursor, malformed) = gen_cursor(tc, &samples);
            if !malformed && !cursor.is_empty() {
                let raw = base64::engine::general_purpose::URL_SAFE_NO_PAD.decode(&cursor).unwrap();
                let v: Value = serde_json::from_slice(&raw).unwrap();
                let seq: u64 = v["key"].as_str().unwrap().parse().unwrap();
                assert!((1..=4).contains(&seq), "a served sequence a few below: {seq}");
            }
        });
    }

    #[test]
    fn trail_has_entries_only_on_unfiltered_first_pages() {
        testkit::reset();
        let empty = Outcome::from_json(&json!({"ok": true, "items": [], "complete": true}));
        check_trail_has_entries(Some(&json!({})), "", &empty);
        check_trail_has_entries(None, "tok", &empty);
        let truncated = Outcome::from_json(&json!({"ok": true, "items": [], "complete": false}));
        check_trail_has_entries(None, "", &truncated);
        assert!(!testkit::fired("audit trail should contain entries"));
        let one = Outcome::from_json(&json!({"ok": true, "items": [{"sequence": "1"}], "complete": true}));
        check_trail_has_entries(None, "", &one);
        let rec = testkit::assertions();
        let a = rec.iter().find(|a| a.message == "audit trail should contain entries").unwrap();
        assert!(a.cond);
        check_trail_has_entries(None, "", &empty);
        assert!(testkit::assertions().iter().any(|a| a.message == "audit trail should contain entries" && !a.cond));
    }

    #[test]
    fn rules_read_through_tickets() {
        let (d, ft, fo) = testkit::driver();
        fo.set_view("auditSamples", sample_view());
        ft.set_default("bucket", "ListAuditEntries", FakeReply::Outcome(Outcome::from_json(&json!({"ok": true, "items": [{"sequence": "3"}], "complete": true}))));
        for _ in 0..8 {
            testkit::run_rule(&d, audit_list);
            testkit::run_rule(&d, audit_entry);
        }
        let ends = fo.calls("/read/end");
        assert!(ends.iter().any(|e| e["kind"] == "ListAuditEntries"));
        for e in &ends {
            assert_eq!(e["route"]["consistency"], "linearizable");
            match e["kind"].as_str().unwrap() {
                "ListAuditEntries" => assert!(e["request"]["options"].is_object()),
                "GetAuditEntry" => {
                    let s = e["request"]["sequence"].as_str().unwrap();
                    assert!(s == "3" || s == "4" || s == u64::MAX.to_string(), "{s}");
                }
                k => panic!("unexpected kind {k}"),
            }
        }
        assert!(fo.open_reads().is_empty());
        assert!(d.s.registry.snapshot().is_empty());
        let lists = ft.calls_of("bucket", "ListAuditEntries");
        assert!(!lists.is_empty());
        for l in &lists {
            let ps = l["request"]["options"]["pageSize"].as_u64().unwrap_or(0);
            assert_eq!(l["maxItems"].as_u64().unwrap(), effective_page_size(ps) + ITEM_SLACK);
        }
    }
}
