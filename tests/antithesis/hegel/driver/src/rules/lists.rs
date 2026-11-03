//! List-query rules (task 06): `list_accounts`, `list_transactions`,
//! `list_logs` (one validated page each, read kinds `ListAccounts`,
//! `ListTransactions`, `ListLogs`) and `list_traverse` (up to 8 pages
//! following `nextCursor`, one read ticket per page, plus the clean-EOF
//! pagination properties of `MD/parallel_driver_list_completeness` and
//! `MD/parallel_driver_timestamp_order`).
//!
//! Every draw happens before `/read/begin` (filters are pre-drawn
//! [`Plan`]s resolved against the views returned with the ticket). The oracle
//! judges each page; Rust only re-checks structure across pages it can judge
//! without the model, and never assumes the transactions endpoint's order:
//! `reverse=false` is newest first there (model-reads §4.6).
use super::filters::{self, FilterCtx, Plan, ShapeRoll};
use crate::clients::{CallOpts, Outcome, Route};
use crate::draw::{self, chance, int_in};
use crate::machine::{rule, Driver};
use crate::read::{LedgerSel, ViewReq};
use hegel::stateful::ConcurrentRule;
use hegel::TestCase;
use serde_json::{json, Map, Value};
use std::collections::HashSet;
use std::time::{Duration, Instant};

pub fn rules() -> Vec<ConcurrentRule<Driver>> {
    vec![
        rule("list_accounts", 2.0, list_accounts),
        rule("list_transactions", 3.0, list_transactions),
        rule("list_logs", 1.5, list_logs),
        rule("list_traverse", 0.7, list_traverse),
    ]
}

/// Extra items the client may stream past the effective page size, so a page
/// that overshoots by a few rows still arrives `complete` and is judged.
const ITEM_SLACK: u64 = 64;

/// The `ListOptions` of a request.
fn options(page_size: u64, cursor: &str, reverse: bool, filter: Option<Value>) -> Value {
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
        o.insert("filter".into(), f);
    }
    Value::Object(o)
}

fn list_opts(page_size: u64) -> CallOpts {
    CallOpts::linearizable().max_items(filters::effective_page_size(page_size) + ITEM_SLACK)
}

fn ledger_of(v: &Value) -> Option<String> {
    v.get("ledger").and_then(Value::as_str).map(str::to_string)
}

/// A reference-shaped `seek(txByRef)` probe (the seek wraps).
fn ref_probe(d: &Driver, tc: &TestCase) -> String {
    format!("{}ref{}", d.s.tracked_prefix(), int_in(tc, 0, 4096))
}

/// `list_accounts` (M `runAccountQuery`): filter per `genAccountFilter` then
/// `rollFilterShape`, reverse 1/2, a pool address cursor 1/2.
pub fn list_accounts(d: &Driver, tc: TestCase) {
    d.guarded("list_accounts", &tc, |d, tc| {
        if !d.s.oracle_ok() {
            return None;
        }
        let pick = draw::ledger_pick(tc);
        let plan = filters::gen_account_filter(tc);
        let shape = filters::draw_shape(tc, matches!(plan, Plan::Universe));
        let page_size = draw::page_size(tc);
        let reverse = chance(tc, 0.5);
        let cursor = if chance(tc, 0.5) { filters::page_token(&draw::pool_address(tc)) } else { String::new() };
        let route = draw::route(tc, &d.s);

        let rt = d.read_begin("ListAccounts", &[ViewReq::ledger(LedgerSel::Pick(pick))], json!({}))?;
        let ctx = FilterCtx::from_views(&rt.views()[0], None, None);
        let filter = shape.apply(plan.resolve(&ctx));
        let req = json!({"ledger": ctx.ledger, "options": options(page_size, &cursor, reverse, filter)});
        let opts = list_opts(page_size);
        let (_, out) = d.call(&route, "bucket", "ListAccounts", req.clone(), &opts);
        rt.end(&req, &out, &route, &opts, json!({"gen": "list_accounts"}), json!({}));
        Some(())
    });
}

/// `list_transactions` (M `runTransactionQuery`): filter per
/// `genTransactionFilter` (or the unsupported-builtin probe 1/24, unshaped),
/// a malformed cursor 1/16, else an `afterID` in 1..=256 1/2.
pub fn list_transactions(d: &Driver, tc: TestCase) {
    d.guarded("list_transactions", &tc, |d, tc| {
        if !d.s.oracle_ok() {
            return None;
        }
        let pick = draw::ledger_pick(tc);
        let probe = ref_probe(d, tc);
        let plan = filters::gen_transaction_filter(tc);
        let (plan, shape) = match filters::roll_unsupported_builtin(tc) {
            Some(u) => (Plan::Fixed(u), ShapeRoll::Keep),
            None => {
                let s = filters::draw_shape(tc, matches!(plan, Plan::Universe));
                (plan, s)
            }
        };
        let page_size = draw::page_size(tc);
        let reverse = chance(tc, 0.5);
        let cursor = match filters::roll_malformed_cursor(tc) {
            Some(bad) => bad,
            None if chance(tc, 0.5) => filters::page_token(&int_in(tc, 1, 256).to_string()),
            None => String::new(),
        };
        let route = draw::route(tc, &d.s);

        let sel = LedgerSel::Pick(pick);
        let rt = d.read_begin("ListTransactions", &[ViewReq::ledger(sel.clone()), ViewReq::seek(sel.clone(), "txByRef", &probe, None), ViewReq::stamps(sel)], json!({}))?;
        let v = rt.views();
        let ctx = FilterCtx::from_views(&v[0], Some(&v[1]), Some(&v[2]));
        let filter = shape.apply(plan.resolve(&ctx));
        let req = json!({"ledger": ctx.ledger, "options": options(page_size, &cursor, reverse, filter)});
        let opts = list_opts(page_size);
        let (_, out) = d.call(&route, "bucket", "ListTransactions", req.clone(), &opts);
        rt.end(&req, &out, &route, &opts, json!({"gen": "list_transactions"}), json!({}));
        Some(())
    });
}

/// `rollLogResumeID`: a ledger-local log id a few below the head, 1/8 past it.
struct ResumePlan {
    past: bool,
    off: u64,
    frac: u64,
}

impl ResumePlan {
    fn draw(tc: &TestCase) -> ResumePlan {
        ResumePlan { past: chance(tc, 1.0 / 8.0), off: int_in(tc, 0, 15), frac: int_in(tc, 0, u64::MAX) }
    }
    fn id(&self, head: u64) -> u64 {
        if head == 0 || self.past {
            head + 1 + self.off
        } else {
            1 + self.frac % head
        }
    }
}

/// `list_logs` (M `runLogQuery`): `genLogFilter` 3/4 then `rollFilterShape`,
/// a malformed cursor 1/16, else a resume id near the head 1/2, reverse 1/2.
pub fn list_logs(d: &Driver, tc: TestCase) {
    d.guarded("list_logs", &tc, |d, tc| {
        if !d.s.oracle_ok() {
            return None;
        }
        let pick = draw::ledger_pick(tc);
        let plan = if !chance(tc, 0.25) { filters::gen_log_filter(tc, 0) } else { Plan::Universe };
        let shape = filters::draw_shape(tc, matches!(plan, Plan::Universe));
        let page_size = draw::page_size(tc);
        let reverse = chance(tc, 0.5);
        let malformed = filters::roll_malformed_cursor(tc);
        let resume = if malformed.is_none() && chance(tc, 0.5) { Some(ResumePlan::draw(tc)) } else { None };
        let route = draw::route(tc, &d.s);

        let sel = LedgerSel::Pick(pick);
        let rt = d.read_begin("ListLogs", &[ViewReq::ledger(sel.clone()), ViewReq::stamps(sel)], json!({}))?;
        let v = rt.views();
        let ctx = FilterCtx::from_views(&v[0], None, Some(&v[1]));
        let cursor = match (&malformed, &resume) {
            (Some(bad), _) => bad.clone(),
            (None, Some(r)) => filters::page_token(&r.id(ctx.log_count).to_string()),
            _ => String::new(),
        };
        let filter = shape.apply(plan.resolve(&ctx));
        let req = json!({"ledger": ctx.ledger, "options": options(page_size, &cursor, reverse, filter)});
        let opts = list_opts(page_size);
        let (_, out) = d.call(&route, "bucket", "ListLogs", req.clone(), &opts);
        let verdict = rt.end(&req, &out, &route, &opts, json!({"gen": "list_logs"}), json!({}));
        if verdict.is_some_and(|v| v.violations.iter().any(|x| x.id == "oracle: log query outside model")) {
            recheck_logs(d, &route, &ctx.ledger);
        }
        Some(())
    });
}

/// Diagnostics after a ListLogs finding (M `recheckLogIDs` /
/// `recheckLogKinds`): the ledger's logs unfiltered at a later horizon. Not
/// validated; printed only.
fn recheck_logs(d: &Driver, route: &Route, ledger: &str) {
    let req = json!({"ledger": ledger, "options": {"pageSize": 200}});
    let (_, out) = d.call(route, "bucket", "ListLogs", req, &CallOpts::linearizable().max_items(201));
    let rows: Vec<String> = out.items.unwrap_or_default().iter().map(|l| format!("{}:{}", log_local_id(l), log_kind(l))).collect();
    println!("driver: list_logs recheck {ledger}: ok={} code={} rows=[{}]", out.ok, out.code, rows.join(","));
}

fn u64_of(v: &Value) -> Option<u64> {
    v.as_str().and_then(|s| s.parse().ok()).or_else(|| v.as_u64())
}

fn log_local_id(l: &Value) -> u64 {
    l.pointer("/payload/apply/log/id").and_then(u64_of).unwrap_or(0)
}

fn log_kind(l: &Value) -> String {
    l.pointer("/payload/apply/log/data").and_then(Value::as_object).and_then(|m| m.keys().next().cloned()).unwrap_or_else(|| "?".into())
}

/// Budget of one traversal (the whole rule stays under ~10 s).
const TRAVERSE_BUDGET: Duration = Duration::from_secs(8);
const TRAVERSE_PAGE_TIMEOUT: Duration = Duration::from_secs(2);
const TRAVERSE_MAX_PAGES: u64 = 8;

/// What one traversal saw, for the cross-page checks.
#[derive(Debug, Default)]
pub struct Traversal {
    pub logs: bool,
    pub reverse: bool,
    pub requested_page_size: u64,
    /// Every page fetched, in order.
    pub pages: Vec<Outcome>,
}

impl Traversal {
    /// Every page answered and streamed to its clean end.
    pub fn clean(&self) -> bool {
        !self.pages.is_empty() && self.pages.iter().all(|p| p.is_ok() && p.complete)
    }
}

/// The clean-EOF pagination properties, evaluated only when every page was
/// complete. Ascending transaction ids is `reverse=true` (the endpoint
/// inverts the flag); log ids ascend for `reverse=false`.
pub fn check_traversal(t: &Traversal) {
    if !t.clean() {
        return;
    }
    let effective = filters::effective_page_size(t.requested_page_size) as usize;
    let bounded = t.pages.iter().all(|p| p.items.as_ref().map_or(0, Vec::len) <= effective);
    let lens: Vec<usize> = t.pages.iter().map(|p| p.items.as_ref().map_or(0, Vec::len)).collect();
    let details = json!({"logs": t.logs, "reverse": t.reverse, "requested": t.requested_page_size, "effective": effective, "pages": lens});
    check_always!(bounded, "clean-EOF list pagination respects the requested page size", details);
    check_sometimes!(t.pages.len() > 1 && t.pages.iter().take(t.pages.len() - 1).all(|p| !p.next_cursor.is_empty()), "a listing spans several pages", details);
    let items = t.pages.iter().flat_map(|p| p.items.clone().unwrap_or_default());
    if t.logs {
        let seqs: Vec<u64> = items.map(|l| l.get("sequence").and_then(u64_of).unwrap_or(0)).collect();
        let ascending = !t.reverse;
        let monotone = seqs.windows(2).all(|w| if ascending { w[1] > w[0] } else { w[1] < w[0] });
        check_always!(monotone, "ListLogs pagination yields strictly monotone log sequences in the requested direction", json!({"reverse": t.reverse, "sequences": seqs}));
    } else {
        let ids: Vec<u64> = items.map(|tx| tx.get("id").and_then(u64_of).unwrap_or(0)).collect();
        let mut seen = HashSet::new();
        let dups: Vec<u64> = ids.iter().copied().filter(|id| !seen.insert(*id)).collect();
        check_always!(dups.is_empty(), "clean-EOF list pagination yields no duplicate transaction IDs", json!({"reverse": t.reverse, "duplicates": dups, "ids": ids}));
        if t.reverse {
            let increasing = ids.windows(2).all(|w| w[1] > w[0]);
            check_always!(increasing, "clean-EOF list pagination yields strictly increasing transaction IDs", json!({"ids": ids}));
        }
    }
}

/// `list_traverse`: ListTransactions or ListLogs, unfiltered, following
/// `nextCursor` for up to 8 pages with one read ticket per page; stops early
/// at the traversal budget. Each page is judged by the oracle; the
/// cross-page structure by [`check_traversal`].
pub fn list_traverse(d: &Driver, tc: TestCase) {
    d.guarded("list_traverse", &tc, |d, tc| {
        if !d.s.oracle_ok() {
            return None;
        }
        let pick = draw::ledger_pick(tc);
        let logs = chance(tc, 0.5);
        let page_size = draw::page_size(tc);
        let reverse = chance(tc, 0.5);
        let max_pages = int_in(tc, 1, TRAVERSE_MAX_PAGES);
        let route = draw::route(tc, &d.s);

        let (kind, method) = if logs { ("ListLogs", "ListLogs") } else { ("ListTransactions", "ListTransactions") };
        let started = Instant::now();
        let mut t = Traversal { logs, reverse, requested_page_size: page_size, pages: vec![] };
        let mut ledger: Option<String> = None;
        let mut cursor = String::new();
        let opts = list_opts(page_size).timeout(TRAVERSE_PAGE_TIMEOUT);
        for _ in 0..max_pages {
            if started.elapsed() >= TRAVERSE_BUDGET {
                break;
            }
            let sel = match &ledger {
                Some(l) => LedgerSel::Name(l.clone()),
                None => LedgerSel::Pick(pick),
            };
            let rt = d.read_begin(kind, &[ViewReq::ledger(sel)], json!({}))?;
            let name = ledger_of(&rt.views()[0])?;
            ledger.get_or_insert(name.clone());
            let req = json!({"ledger": name, "options": options(page_size, &cursor, reverse, None)});
            let (_, out) = d.call(&route, "bucket", method, req.clone(), &opts);
            rt.end(&req, &out, &route, &opts, json!({"gen": "list_traverse", "page": t.pages.len()}), json!({}));
            let next = out.next_cursor.clone();
            let stop = !(out.is_ok() && out.complete) || next.is_empty();
            t.pages.push(out);
            if stop {
                break;
            }
            cursor = next;
        }
        check_traversal(&t);
        Some(())
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testkit::{self, FakeReply};

    fn page(items: Vec<Value>, next: &str) -> Outcome {
        Outcome { ok: true, items: Some(items), complete: true, next_cursor: next.into(), ..Default::default() }
    }

    fn tx(id: u64) -> Value {
        json!({"id": id.to_string()})
    }

    fn log(seq: u64) -> Value {
        json!({"sequence": seq.to_string(), "payload": {"apply": {"log": {"id": seq.to_string()}}}})
    }

    fn fired_cond(msg: &str) -> Vec<bool> {
        testkit::assertions().iter().filter(|a| a.message == msg).map(|a| a.cond).collect()
    }

    #[test]
    fn traversal_checks() {
        testkit::reset();
        // Ascending transactions (reverse=true), clean.
        check_traversal(&Traversal { logs: false, reverse: true, requested_page_size: 2, pages: vec![page(vec![tx(1), tx(2)], "c"), page(vec![tx(3)], "")] });
        assert_eq!(fired_cond("clean-EOF list pagination respects the requested page size"), vec![true]);
        assert_eq!(fired_cond("clean-EOF list pagination yields no duplicate transaction IDs"), vec![true]);
        assert_eq!(fired_cond("clean-EOF list pagination yields strictly increasing transaction IDs"), vec![true]);
        assert_eq!(fired_cond("a listing spans several pages"), vec![true]);

        // Descending transactions: no increasing check; a duplicate and an
        // oversized page are findings.
        testkit::reset();
        check_traversal(&Traversal { logs: false, reverse: false, requested_page_size: 1, pages: vec![page(vec![tx(5), tx(4)], "c"), page(vec![tx(4)], "")] });
        assert_eq!(fired_cond("clean-EOF list pagination respects the requested page size"), vec![false]);
        assert_eq!(fired_cond("clean-EOF list pagination yields no duplicate transaction IDs"), vec![false]);
        assert!(fired_cond("clean-EOF list pagination yields strictly increasing transaction IDs").is_empty());

        // The page size compares against the effective size, not the request.
        testkit::reset();
        check_traversal(&Traversal { logs: false, reverse: true, requested_page_size: 0, pages: vec![page((1..=100).map(tx).collect(), "")] });
        assert_eq!(fired_cond("clean-EOF list pagination respects the requested page size"), vec![true]);

        // Logs: ascending for reverse=false, descending for reverse=true.
        testkit::reset();
        check_traversal(&Traversal { logs: true, reverse: true, requested_page_size: 2, pages: vec![page(vec![log(9), log(7)], "c"), page(vec![log(3)], "")] });
        assert_eq!(fired_cond("ListLogs pagination yields strictly monotone log sequences in the requested direction"), vec![true]);
        testkit::reset();
        check_traversal(&Traversal { logs: true, reverse: false, requested_page_size: 2, pages: vec![page(vec![log(9), log(7)], "")] });
        assert_eq!(fired_cond("ListLogs pagination yields strictly monotone log sequences in the requested direction"), vec![false]);

        // An incomplete page: nothing is evaluated.
        testkit::reset();
        let mut p = page(vec![tx(2), tx(1)], "");
        p.complete = false;
        check_traversal(&Traversal { logs: false, reverse: true, requested_page_size: 1, pages: vec![p] });
        assert!(testkit::assertions().is_empty());
    }

    fn run_many(d: &Driver, f: fn(&Driver, TestCase), n: u64) {
        let d = d.clone();
        hegel::Hegel::new(move |tc: TestCase| f(&d, tc)).settings(hegel::Settings::new().test_cases(n).database(hegel::Database::Disabled)).run();
    }

    #[test]
    fn list_rules_send_well_formed_requests_and_end_their_tickets() {
        let (d, ft, fo) = testkit::driver();
        fo.set_live(vec!["o-test-l1".into(), "o-test-l3".into()]);
        fo.set_view("ledger", json!({"logCount": 9, "fieldTypes": {"account": {"k0": "METADATA_TYPE_STRING"}, "transaction": {"k1": "METADATA_TYPE_INT64"}, "ledger": {}}}));
        fo.set_view("stamps", json!({"txDates": ["1700000000000000"], "logDates": ["1700000000000100"], "maxLogSeq": "9"}));
        fo.set_view("seek", json!({"found": true, "wrapped": false, "entry": {"reference": "o-test-ref3", "id": "3"}}));
        for m in ["ListAccounts", "ListTransactions", "ListLogs"] {
            ft.set_default("bucket", m, FakeReply::Outcome(page(vec![], "")));
        }
        run_many(&d, list_accounts, 40);
        run_many(&d, list_transactions, 40);
        run_many(&d, list_logs, 40);
        for (kind, method) in [("ListAccounts", "ListAccounts"), ("ListTransactions", "ListTransactions"), ("ListLogs", "ListLogs")] {
            let ends: Vec<Value> = fo.calls("/read/end").into_iter().filter(|e| e["kind"] == kind).collect();
            assert!(ends.len() >= 30, "{kind}: {}", ends.len());
            assert_eq!(ends.len(), ft.calls_of("bucket", method).len());
            for e in &ends {
                assert!(e["request"]["ledger"].as_str().unwrap().starts_with("o-test-l"));
                assert_eq!(e["route"]["consistency"], "linearizable");
                let ps = e["request"]["options"]["pageSize"].as_u64().unwrap_or(0);
                assert!(ps == 0 || [1, 2, 3, 5, 10, 50].contains(&ps) || ps > 1000);
                assert!(!e["request"].to_string().contains("own-"));
            }
            for b in ft.calls_of("bucket", method) {
                let ps = b["request"]["options"]["pageSize"].as_u64().unwrap_or(0);
                assert_eq!(b["maxItems"].as_u64().unwrap(), filters::effective_page_size(ps) + ITEM_SLACK);
            }
        }
        // Some transactions read aims at the seeded reference or a malformed cursor.
        let tx_reqs: Vec<String> = fo.calls("/read/end").into_iter().filter(|e| e["kind"] == "ListTransactions").map(|e| e["request"].to_string()).collect();
        assert!(tx_reqs.iter().any(|r| r.contains("o-test-ref3") || r.contains("TX_BUILTIN_INDEX") || r.contains("cursor")));
        assert!(fo.open_reads().is_empty());
        assert!(d.s.registry.snapshot().is_empty());
    }

    #[test]
    fn list_traverse_follows_next_cursors_one_ticket_per_page() {
        let (d, ft, fo) = testkit::driver();
        fo.set_live(vec!["o-test-l1".into()]);
        let tok1 = filters::page_token("2");
        let tok2 = filters::page_token("4");
        for m in ["ListTransactions", "ListLogs"] {
            ft.script("bucket", m, vec![FakeReply::Outcome(page(vec![tx(1), tx(2)], &tok1)), FakeReply::Outcome(page(vec![tx(3), tx(4)], &tok2))]);
            ft.set_default("bucket", m, FakeReply::Outcome(page(vec![], "")));
        }
        testkit::run_rule(&d, list_traverse);
        let calls: Vec<Value> = ft.calls().into_iter().filter(|c| c["service"] == "bucket").collect();
        let ends = fo.calls("/read/end");
        assert_eq!(calls.len(), ends.len(), "one read ticket per page");
        assert!(!calls.is_empty() && calls.len() <= 8);
        if calls.len() > 1 {
            assert_eq!(calls[1]["request"]["options"]["cursor"], json!(tok1));
            assert_eq!(calls[0]["request"]["ledger"], calls[1]["request"]["ledger"]);
        }
        assert!(fo.open_reads().is_empty());
    }
}
