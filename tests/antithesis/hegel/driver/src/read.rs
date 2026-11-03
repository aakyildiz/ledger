//! Reads (DESIGN §4.7, §6.6): views for aiming, read tickets for validated
//! reads. A `ReadTicket` is registered in the driver registry while held and
//! abandons itself on drop.
#![allow(dead_code)] // family-facing API

use crate::clients::{CallOpts, Outcome, Route};
use crate::machine::Driver;
use crate::oracle::{ReadAbandonReq, ReadBeginReq, ReadEndReq, ReadRoute, Verdict};
use crate::shared::Owner;
use serde_json::{json, Value};

/// A ledger target: a name, or a `ledgerPick` resolved by the oracle on the
/// view snapshot.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LedgerSel {
    Name(String),
    Pick(u32),
}

impl LedgerSel {
    fn apply(&self, v: &mut Value) {
        match self {
            LedgerSel::Name(n) => v["ledger"] = json!(n),
            LedgerSel::Pick(p) => v["ledgerPick"] = json!(p),
        }
    }
}

/// One view request (`{"name": …, params…}`).
#[derive(Clone, Debug, PartialEq)]
pub struct ViewReq(pub Value);

impl ViewReq {
    /// A family view: `params` (an object) merged with the name.
    pub fn named(name: &str, params: Value) -> Self {
        let mut v = json!({"name": name});
        if let Some(o) = params.as_object() {
            for (k, x) in o {
                v[k.as_str()] = x.clone();
            }
        }
        ViewReq(v)
    }
    pub fn ledgers() -> Self {
        ViewReq(json!({"name": "ledgers"}))
    }
    /// The `ledgers` view including reserved ledgers.
    pub fn ledgers_owned() -> Self {
        ViewReq(json!({"name": "ledgers", "owned": true}))
    }
    pub fn ledger(sel: LedgerSel) -> Self {
        let mut v = json!({"name": "ledger"});
        sel.apply(&mut v);
        ViewReq(v)
    }
    pub fn seek(sel: LedgerSel, table: &str, probe: &str, filter: Option<&str>) -> Self {
        let mut v = json!({"name": "seek", "table": table, "probe": probe});
        sel.apply(&mut v);
        if let Some(f) = filter {
            v["filter"] = json!(f);
        }
        ViewReq(v)
    }
    pub fn replayable() -> Self {
        ViewReq(json!({"name": "replayable"}))
    }
    pub fn stamps(sel: LedgerSel) -> Self {
        let mut v = json!({"name": "stamps"});
        sel.apply(&mut v);
        ViewReq(v)
    }
    /// Adds `"owned": true` (reserved entities visible).
    pub fn owned(mut self) -> Self {
        self.0["owned"] = json!(true);
        self
    }
}

/// A view answer whose ledger could not be resolved (`live` empty).
pub fn unresolved(v: &Value) -> bool {
    v.get("ledger").is_some_and(Value::is_null)
}

/// A held read ticket (DESIGN §4.7).
pub struct ReadTicket {
    d: Driver,
    kind: &'static str,
    ticket: u64,
    views: Vec<Value>,
    open: bool,
}

impl ReadTicket {
    pub fn views(&self) -> &[Value] {
        &self.views
    }
    pub fn ticket(&self) -> u64 {
        self.ticket
    }
    pub fn kind(&self) -> &'static str {
        self.kind
    }

    /// Posts `/read/end` with the request exactly as sent and its outcome.
    pub fn end(mut self, req: &Value, out: &Outcome, route: &Route, opts: &CallOpts, hint: Value, params: Value) -> Option<Verdict> {
        self.open = false;
        let body = ReadEndReq {
            ticket: self.ticket,
            kind: self.kind.to_string(),
            request: req.clone(),
            outcome: out.clone(),
            route: serde_json::from_value::<ReadRoute>(route.read_json(opts.consistency_str())).unwrap_or_default(),
            hint,
            params,
        };
        let r = self.d.s.oracle.read_end(&body);
        self.d.s.registry.remove(self.ticket);
        match r {
            Ok(resp) => {
                if !resp.verdict.violations.is_empty() {
                    println!("driver: /read/end {} {}: oracle violations {:?}", self.kind, self.ticket, resp.verdict.violations.iter().map(|v| &v.id).collect::<Vec<_>>());
                }
                Some(resp.verdict)
            }
            Err(_) => None,
        }
    }

    /// Drops the ticket without validation.
    pub fn abandon(mut self) {
        self.do_abandon();
    }

    fn do_abandon(&mut self) {
        if !self.open {
            return;
        }
        self.open = false;
        if let Err(e) = self.d.s.oracle.read_abandon(&ReadAbandonReq { ticket: self.ticket }) {
            println!("driver: /read/abandon {}: {:?} {}", self.ticket, e.kind, e.message);
        }
        self.d.s.registry.remove(self.ticket);
    }
}

impl Drop for ReadTicket {
    fn drop(&mut self) {
        self.do_abandon();
    }
}

impl Driver {
    /// Views for aiming only (no ticket). `None` in degraded mode or on a refusal.
    pub fn view(&self, views: &[ViewReq]) -> Option<Vec<Value>> {
        if !self.s.oracle_ok() {
            return None;
        }
        let r = self.s.oracle.view(views.iter().map(|v| v.0.clone()).collect()).ok()?;
        check_always_or_unreachable!(r.views.len() == views.len(), "the oracle answers one view per view request", serde_json::json!({"asked": views.len(), "got": r.views.len()}));
        Some(r.views)
    }

    /// Takes a read ticket with views computed on the same snapshot. A view
    /// whose `ledgerPick` found no live ledger ends the read (abandon, `None`).
    pub fn read_begin(&self, kind: &'static str, views: &[ViewReq], params: Value) -> Option<ReadTicket> {
        if !self.s.oracle_ok() {
            return None;
        }
        let req = ReadBeginReq { rid: self.s.next_rid(), kind: kind.to_string(), views: views.iter().map(|v| v.0.clone()).collect(), params: if params.is_null() { json!({}) } else { params } };
        let resp = {
            let _pending = self.s.registry.begin_pending();
            let r = self.s.oracle.read_begin(&req);
            if let Ok(resp) = &r {
                self.s.registry.insert(resp.ticket, Owner::Read);
            }
            r
        };
        let resp = resp.ok()?;
        check_always_or_unreachable!(resp.views.len() == views.len(), "the oracle answers one view per view request", json!({"asked": views.len(), "got": resp.views.len()}));
        let t = ReadTicket { d: self.clone(), kind, ticket: resp.ticket, views: resp.views, open: true };
        if t.views.iter().any(unresolved) {
            t.abandon();
            return None;
        }
        Some(t)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testkit;

    #[test]
    fn drop_abandons_end_consumes_and_ledger_pick_is_echoed() {
        let (d, _, fo) = testkit::driver();
        fo.set_live(vec!["o-test-l1".into(), "o-test-l2".into(), "o-test-l3".into()]);
        {
            let t = d.read_begin("GetAccount", &[ViewReq::ledgers(), ViewReq::ledger(LedgerSel::Pick(4)), ViewReq::seek(LedgerSel::Pick(4), "volumes", "t-0:1", None)], json!({})).unwrap();
            assert_eq!(t.views()[1]["ledger"], "o-test-l2", "live[4 mod 3]");
            assert_eq!(t.views()[2]["ledger"], "o-test-l2", "same pick, same ledger");
            assert!(d.s.registry.contains(t.ticket()));
            assert_eq!(fo.open_reads().len(), 1);
        }
        assert_eq!(fo.calls("/read/abandon").len(), 1, "drop abandons");
        assert!(fo.open_reads().is_empty());
        assert!(d.s.registry.snapshot().is_empty());

        let t = d.read_begin("GetAccount", &[ViewReq::ledger(LedgerSel::Name("o-test-l3".into()))], json!({})).unwrap();
        assert_eq!(t.views()[0]["ledger"], "o-test-l3");
        let out = Outcome::from_json(&json!({"ok": true, "response": {}}));
        let v = t.end(&json!({"ledger": "o-test-l3", "address": "t-0:1"}), &out, &Route::plain(2), &CallOpts::linearizable(), json!({}), json!({}));
        assert!(v.unwrap().ok);
        assert_eq!(fo.calls("/read/end").len(), 1);
        assert_eq!(fo.calls("/read/end")[0]["route"], json!({"client": 2, "route": "plain", "node": "", "consistency": "linearizable"}));
        assert_eq!(fo.calls("/read/abandon").len(), 1, "end consumes the ticket: no abandon");
        assert!(d.s.registry.snapshot().is_empty());

        // No live ledger: the view answers {"ledger": null} and the read ends.
        fo.set_live(vec![]);
        assert!(d.read_begin("GetAccount", &[ViewReq::seek(LedgerSel::Pick(77), "volumes", "t-0:1", None)], json!({})).is_none());
        assert_eq!(fo.calls("/read/abandon").len(), 2);
        assert!(fo.open_reads().is_empty());
    }

    #[test]
    fn view_requests_render() {
        assert_eq!(ViewReq::seek(LedgerSel::Name("L".into()), "txs", "40", Some("has-metadata")).0, json!({"name": "seek", "ledger": "L", "table": "txs", "probe": "40", "filter": "has-metadata"}));
        assert_eq!(ViewReq::stamps(LedgerSel::Pick(2)).0, json!({"name": "stamps", "ledgerPick": 2}));
        assert_eq!(ViewReq::named("logSeqs", json!({"ledgerPick": 1, "n": 3})).0, json!({"name": "logSeqs", "ledgerPick": 1, "n": 3}));
        assert_eq!(ViewReq::ledgers().owned().0, json!({"name": "ledgers", "owned": true}));
    }
}
