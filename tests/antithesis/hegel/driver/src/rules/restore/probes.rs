//! Post-restore admission probes (restore.md §5.3, contract step 7): the
//! restored cluster is disposable, so the driver writes to it and checks
//! that every value the restored projection feeds into admission is right.
//! The probes work on the seed ledger, whose whole history the driver wrote
//! (T1, T2 + its revert and T4 before the base backup, T3 in the delta), so
//! every expected outcome is known. Only definite outcomes are judged.
use super::{revert, tx, Cx, Seed};
use crate::clients::{CallOpts, Outcome};
use crate::wire::{apply_logs, apply_request, created_transaction, u64_field};
use serde_json::{json, Value};
use std::time::Duration;

/// One judged probe.
#[derive(Clone, Debug)]
pub struct ProbeResult {
    #[allow(dead_code)] // read by unit tests
    pub name: &'static str,
    pub ok: bool,
    pub details: Value,
}

/// An RFC 3339 timestamp as (seconds since the epoch, nanos), enough to
/// order protojson Timestamps whose fractional digits vary.
/// A served timestamp: the ledger's protojson `Timestamp` is
/// `{"data": "<unix micros>"}` (seen on the stack, task 99); an RFC 3339
/// string is accepted too.
pub fn ts_value(v: &Value) -> Option<(i64, u32)> {
    if let Some(s) = v.as_str() {
        return parse_ts(s);
    }
    let d = v.get("data")?;
    let micros: i64 = d.as_str().and_then(|s| s.parse().ok()).or_else(|| d.as_i64())?;
    Some((micros.div_euclid(1_000_000), (micros.rem_euclid(1_000_000) * 1000) as u32))
}

pub fn parse_ts(s: &str) -> Option<(i64, u32)> {
    let s = s.strip_suffix('Z')?;
    let (date, time) = s.split_once('T')?;
    let mut d = date.split('-').map(|x| x.parse::<i64>());
    let (y, mo, da) = (d.next()?.ok()?, d.next()?.ok()?, d.next()?.ok()?);
    let (hms, frac) = time.split_once('.').unwrap_or((time, ""));
    let mut t = hms.split(':').map(|x| x.parse::<i64>());
    let (h, mi, se) = (t.next()?.ok()?, t.next()?.ok()?, t.next()?.ok()?);
    let nanos = if frac.is_empty() { 0 } else { format!("{frac:0<9}")[..9].parse::<u32>().ok()? };
    // Days from civil (Howard Hinnant).
    let y2 = if mo <= 2 { y - 1 } else { y };
    let era = if y2 >= 0 { y2 } else { y2 - 399 } / 400;
    let yoe = y2 - era * 400;
    let doy = (153 * (if mo > 2 { mo - 3 } else { mo + 9 }) + 2) / 5 + da - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146097 + doe - 719468;
    Some((days * 86400 + h * 3600 + mi * 60 + se, nanos))
}

struct Probe<'a> {
    cx: &'a Cx<'a>,
    node: &'a str,
}

impl Probe<'_> {
    /// An Apply; with a key, ambiguous outcomes are retried (the key makes
    /// the retry safe); without one, an ambiguous outcome is returned.
    fn apply(&self, requests: Vec<Value>, key: Option<&str>) -> Outcome {
        let mut out = Outcome::unknown("not sent");
        for _ in 0..if key.is_some() { 5 } else { 1 } {
            out = self.cx.call(Some(self.node), "bucket", "Apply", apply_request(&requests, key), &CallOpts::default().timeout(Duration::from_secs(10)));
            if !super::rpc::ambiguous(&out) {
                return out;
            }
            self.cx.sleep_ms(500);
        }
        out
    }

    fn list(&self, method: &str, req: Value) -> Option<Vec<Value>> {
        let out = self.cx.call(Some(self.node), "bucket", method, req, &CallOpts::linearizable().max_items(1001)); // one past the 1000-row page, so a full page completes
        (out.is_ok() && out.complete && out.next_cursor.is_empty()).then(|| out.items.unwrap_or_default())
    }
}

fn ids(out: &Outcome) -> Vec<u64> {
    out.response.as_ref().map(|r| apply_logs(r).iter().filter_map(|l| created_transaction(l).map(|t| u64_field(t, "id"))).collect()).unwrap_or_default()
}

fn reason(out: &Outcome) -> String {
    if out.is_ok() {
        "ok".into()
    } else if out.is_unknown() {
        format!("unknown:{}", out.unknown)
    } else {
        out.reason.clone()
    }
}

/// Runs the probes and evaluates `post-restore admission probes behave as
/// predicted` over the definite ones. Returns them (for tests).
pub fn run(cx: &Cx, node: &str, seed: &Seed, s: u64) -> Vec<ProbeResult> {
    let p = Probe { cx, node };
    let l = seed.ledger.as_str();
    let mut res: Vec<ProbeResult> = Vec::new();
    let mut judge = |name: &'static str, out: &Outcome, ok: bool, details: Value| {
        if super::rpc::ambiguous(out) {
            return;
        }
        res.push(ProbeResult { name, ok, details: json!({"probe": name, "outcome": out.summary(), "reason": reason(out), "details": details}) });
    };

    // The restored state the next decisions depend on.
    let txs = p.list("ListTransactions", json!({"ledger": l, "options": {"pageSize": 1000}}));
    let ledgers = p.list("ListLedgers", json!({"options": {"pageSize": 1000}}));
    let first_write = txs.is_some();

    // Next ids, log sequence and HLC: the first write on the restored cluster.
    let out = p.apply(vec![tx(l, "world", "probe:d", 1, None)], Some(&cx.d.s.next_untracked_key()));
    if let (true, Some(txs)) = (first_write, &txs) {
        let max_id = txs.iter().map(|t| u64_field(t, "id")).max().unwrap_or(0);
        let max_ins = txs.iter().filter_map(|t| t.get("insertedAt").and_then(ts_value)).max();
        let logs = out.response.as_ref().map(apply_logs).unwrap_or_default();
        let seq = logs.first().map(|l| u64_field(l, "sequence")).unwrap_or(0);
        let created = logs.first().and_then(created_transaction).cloned().unwrap_or(json!({}));
        let id = u64_field(&created, "id");
        let ins = created.get("insertedAt").and_then(ts_value);
        let ok = out.is_ok() && seq == s + 1 && id == max_id + 1 && matches!((ins, max_ins), (Some(a), Some(b)) if a > b);
        judge("next ids and clock", &out, ok, json!({"sequence": seq, "expectedSequence": s + 1, "id": id, "expectedId": max_id + 1, "insertedAt": created.get("insertedAt"), "maxRestoredInsertedAt": max_ins}));
    }

    // Idempotency: replays of a checkpointed key (k1) and a delta key (k3)
    // return the committed transactions; a changed body is a conflict.
    let replay = p.apply(seed.batch1.clone(), Some(&seed.k1));
    judge("idempotent replay before the checkpoint", &replay, replay.is_ok() && ids(&replay) == vec![seed.t1, seed.t2, seed.t4], json!({"ids": ids(&replay), "expected": [seed.t1, seed.t2, seed.t4]}));
    let mut changed = seed.batch1.clone();
    changed[1] = tx(l, "world", "probe:a", 101, Some(&seed.r1));
    let conflict = p.apply(changed, Some(&seed.k1));
    judge("idempotency conflict before the checkpoint", &conflict, conflict.reason == "IDEMPOTENCY_KEY_CONFLICT", json!({}));
    if let Some(t3) = seed.t3 {
        let replay = p.apply(seed.batch3.clone(), Some(&seed.k3));
        judge("idempotent replay in the delta", &replay, replay.is_ok() && ids(&replay) == vec![t3], json!({"ids": ids(&replay), "expected": [t3]}));
        let conflict = p.apply(vec![tx(l, "world", "probe:a", 51, None)], Some(&seed.k3));
        judge("idempotency conflict in the delta", &conflict, conflict.reason == "IDEMPOTENCY_KEY_CONFLICT", json!({}));
    }

    // References: reusing a committed reference conflicts.
    let out = p.apply(vec![tx(l, "world", "probe:e", 1, Some(&seed.r1))], None);
    judge("reference conflict", &out, out.reason == "TRANSACTION_REFERENCE_CONFLICT", json!({"reference": seed.r1}));

    // Reverts: T2 was reverted before the checkpoint; T4 was not, so it
    // reverts exactly once.
    let out = p.apply(vec![revert(l, seed.t2)], None);
    judge("revert of a reverted transaction", &out, out.reason == "TRANSACTION_ALREADY_REVERTED", json!({"id": seed.t2}));
    let out = p.apply(vec![revert(l, seed.t4)], Some(&cx.d.s.next_untracked_key()));
    judge("revert of a live transaction", &out, out.is_ok(), json!({"id": seed.t4}));
    if out.is_ok() {
        let again = p.apply(vec![revert(l, seed.t4)], None);
        judge("second revert", &again, again.reason == "TRANSACTION_ALREADY_REVERTED", json!({"id": seed.t4}));
    }

    // Balances: probe:a holds 100 (+50 from the delta).
    let b = 100 + if seed.t3.is_some() { 50 } else { 0 };
    let out = p.apply(vec![tx(l, "probe:a", "probe:z", b + 1, None)], None);
    judge("overdraft refused", &out, out.reason == "INSUFFICIENT_FUNDS", json!({"balance": b}));
    if !super::rpc::ambiguous(&out) {
        let out = p.apply(vec![tx(l, "probe:a", "probe:z", b, None)], Some(&cx.d.s.next_untracked_key()));
        judge("whole balance spendable", &out, out.is_ok(), json!({"balance": b}));
    }

    // Next ledger id: above every id the restored cluster lists (deleted
    // ledgers included), which CheckStore does not verify
    // (SubGlobNextLedgerID, backup/README.md:276-283).
    if let Some(ledgers) = ledgers {
        let max_id = ledgers.iter().map(|x| u64_field(x, "id")).max().unwrap_or(0);
        let name = format!("{l}-next");
        let out = p.apply(vec![json!({"createLedger": {"name": name}})], Some(&cx.d.s.next_untracked_key()));
        if out.is_ok() {
            let got = cx.call(Some(node), "bucket", "GetLedger", json!({"ledger": name}), &CallOpts::linearizable());
            if got.is_ok() {
                let info = got.response.clone().unwrap_or(json!({}));
                let id = u64_field(info.get("ledger").unwrap_or(&info), "id");
                judge("next ledger id", &got, id > max_id, json!({"id": id, "maxListed": max_id}));
            }
        } else {
            judge("next ledger id", &out, false, json!({"create": out.summary()}));
        }
    }

    let failed: Vec<&Value> = res.iter().filter(|r| !r.ok).map(|r| &r.details).collect();
    if !res.is_empty() {
        check_always!(failed.is_empty(), "post-restore admission probes behave as predicted", json!({"gen": cx.gen, "ledger": l, "S": s, "judged": res.len(), "failed": failed}));
    }
    println!("driver: restore: g{} admission probes: {} judged, {} failed", cx.gen, res.len(), failed.len());
    res
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timestamps_order_whatever_their_precision() {
        let a = parse_ts("2026-11-03T04:00:00.5Z").unwrap();
        let b = parse_ts("2026-11-03T04:00:00.123456789Z").unwrap();
        let c = parse_ts("2026-11-03T04:00:01Z").unwrap();
        assert!(b < a && a < c);
        assert_eq!(parse_ts("1970-01-01T00:00:00Z"), Some((0, 0)));
        assert_eq!(parse_ts("2000-03-01T00:00:00Z").unwrap().0, 951868800);
        assert!(parse_ts("nope").is_none());
        // The served protojson form (unix micros).
        assert_eq!(ts_value(&json!({"data": "1793711713261142"})), Some((1793711713, 261142000)));
        assert_eq!(ts_value(&json!("1970-01-01T00:00:00Z")), Some((0, 0)));
        assert!(ts_value(&json!({"data": "x"})).is_none());
    }
}
