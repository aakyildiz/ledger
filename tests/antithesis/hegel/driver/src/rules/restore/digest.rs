//! Digest parity (restore.md §5.1–§5.2): the client's `/digest` hashes every
//! section at the cut on the source and again on the restored cluster; the
//! driver compares them section by section and reports the first difference.
use super::env::RestoreEnv;
use crate::machine::Driver;
use serde_json::{json, Map, Value};
use std::time::Duration;

/// Small sections: fetched with their items, so a difference names the
/// first differing key and fields that are filled at read time can be
/// normalized away (`mirrorSyncProgress`, implementer note 3).
pub const ITEM_SECTIONS: &[&str] = &["ledgers", "indexes", "signingKeys", "sinks"];
/// Large sections: hashes, counts, last keys and per-ledger hashes only.
pub const HASH_SECTIONS: &[&str] = &["accounts", "transactions", "logs", "stats", "audit", "numscripts", "preparedQueries", "schemas"];

/// Every compared section, in report order. Not compared: `aggregates`
/// (derived from accounts), `checkpoints` (rows are kept but marked
/// restored: an expected difference, restore.md §5.1).
pub fn compared_sections() -> Vec<&'static str> {
    ITEM_SECTIONS.iter().chain(HASH_SECTIONS.iter()).copied().collect()
}

/// One digest: section name → `{count, sha256, lastKey, items?, ledgers?}`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Digest {
    pub sections: Map<String, Value>,
}

impl Digest {
    /// Ledger names of the `ledgers` section (soft-deleted included, as
    /// ListLedgers serves them).
    pub fn ledger_names(&self) -> Vec<String> {
        self.items("ledgers").iter().filter_map(|l| l.get("name").and_then(Value::as_str).map(str::to_string)).collect()
    }

    pub fn items(&self, section: &str) -> Vec<Value> {
        self.sections.get(section).and_then(|s| s.get("items")).and_then(Value::as_array).cloned().unwrap_or_default()
    }
}

/// Where a digest is read.
pub struct DigestTarget<'a> {
    pub client: usize,
    /// `None` = the shared production route; `Some(node)` = pinned.
    pub node: Option<&'a str>,
    pub consistency: &'static str,
}

fn request(t: &DigestTarget, op: u64, sections: &[&str], items: bool, timeout: Duration) -> Value {
    let mut b = json!({
        "opId": op,
        "consistency": t.consistency,
        "sections": sections,
        "items": items,
        "timeoutMs": timeout.as_millis() as u64,
    });
    match t.node {
        Some(n) => b["node"] = json!(n),
        None => b["route"] = json!("lib"),
    }
    b
}

/// Takes the digest of every compared section (two requests: items for the
/// small sections, hashes for the large ones).
pub fn take(d: &Driver, env: &dyn RestoreEnv, t: &DigestTarget, timeout: Duration) -> Result<Digest, String> {
    let mut out = Digest::default();
    for (sections, items) in [(ITEM_SECTIONS, true), (HASH_SECTIONS, false)] {
        let op = d.s.clients.next_op_id();
        let resp = env.digest(d, t.client, &request(t, op, sections, items, timeout), timeout + Duration::from_secs(5))?;
        if resp.get("ok").and_then(Value::as_bool) != Some(true) {
            return Err(format!("digest failed: {} {}", resp.get("error").and_then(Value::as_str).unwrap_or(""), resp.get("outcome").cloned().unwrap_or(Value::Null)));
        }
        let Some(secs) = resp.get("sections").and_then(Value::as_object) else {
            return Err("digest without sections".into());
        };
        for s in sections.iter() {
            let Some(v) = secs.get(*s) else { return Err(format!("digest without section {s}")) };
            out.sections.insert(s.to_string(), v.clone());
        }
    }
    Ok(out)
}

/// Fields filled at read time, not part of the restored state.
fn normalize(section: &str, item: &Value) -> Value {
    let mut v = item.clone();
    if section == "ledgers" {
        if let Some(o) = v.as_object_mut() {
            o.remove("mirrorSyncProgress");
        }
    }
    v
}

fn item_key(section: &str, item: &Value) -> String {
    let f = |k: &str| item.get(k).map(|v| v.as_str().map(str::to_string).unwrap_or_else(|| v.to_string())).unwrap_or_default();
    match section {
        "ledgers" | "sinks" => f("name"),
        "signingKeys" => f("keyId"),
        "indexes" => format!("{}/{}", f("ledger"), f("id")),
        _ => String::new(),
    }
}

/// One section's comparison.
#[derive(Clone, Debug, PartialEq)]
pub struct SectionDiff {
    pub section: &'static str,
    pub equal: bool,
    pub details: Value,
}

fn compare_items(section: &'static str, src: &[Value], dst: &[Value]) -> SectionDiff {
    let a: Vec<Value> = src.iter().map(|i| normalize(section, i)).collect();
    let b: Vec<Value> = dst.iter().map(|i| normalize(section, i)).collect();
    if a == b {
        return SectionDiff { section, equal: true, details: json!({"count": a.len()}) };
    }
    let first = (0..a.len().max(b.len())).find(|&i| a.get(i) != b.get(i)).unwrap_or(0);
    let key = a.get(first).or(b.get(first)).map(|i| item_key(section, i)).unwrap_or_default();
    SectionDiff {
        section,
        equal: false,
        details: json!({
            "sourceCount": a.len(), "restoredCount": b.len(), "firstDiffIndex": first, "firstDiffKey": key,
            "source": a.get(first), "restored": b.get(first),
        }),
    }
}

fn compare_hashes(section: &'static str, src: &Value, dst: &Value) -> SectionDiff {
    let field = |v: &Value, k: &str| v.get(k).cloned().unwrap_or(Value::Null);
    let equal = field(src, "sha256") == field(dst, "sha256") && field(src, "count") == field(dst, "count");
    if equal {
        return SectionDiff { section, equal, details: json!({"count": field(src, "count")}) };
    }
    // Per-ledger digests localise the difference.
    let empty = Map::new();
    let sl = src.get("ledgers").and_then(Value::as_object).unwrap_or(&empty);
    let dl = dst.get("ledgers").and_then(Value::as_object).unwrap_or(&empty);
    let mut names: Vec<&String> = sl.keys().chain(dl.keys()).collect();
    names.sort();
    names.dedup();
    let differing: Vec<Value> = names
        .into_iter()
        .filter(|n| sl.get(*n) != dl.get(*n))
        .take(8)
        .map(|n| json!({"ledger": n, "source": sl.get(n), "restored": dl.get(n)}))
        .collect();
    SectionDiff {
        section,
        equal,
        details: json!({
            "sourceCount": field(src, "count"), "restoredCount": field(dst, "count"),
            "sourceLastKey": field(src, "lastKey"), "restoredLastKey": field(dst, "lastKey"),
            "ledgers": differing,
        }),
    }
}

/// Compares every section of `compared_sections()`.
pub fn compare(src: &Digest, dst: &Digest) -> Vec<SectionDiff> {
    compared_sections()
        .into_iter()
        .map(|s| {
            let a = src.sections.get(s).cloned().unwrap_or(Value::Null);
            let b = dst.sections.get(s).cloned().unwrap_or(Value::Null);
            if ITEM_SECTIONS.contains(&s) {
                compare_items(s, &src.items(s), &dst.items(s))
            } else {
                compare_hashes(s, &a, &b)
            }
        })
        .collect()
}

/// Evaluates the per-section Always properties (one literal per section).
pub fn assert_sections(diffs: &[SectionDiff], ctx: &Value) {
    for diff in diffs {
        let mut details = ctx.clone();
        details["section"] = json!(diff.section);
        details["diff"] = diff.details.clone();
        let ok = diff.equal;
        match diff.section {
            "ledgers" => check_always!(ok, "restored ledgers match the source at the cut", details),
            "accounts" => check_always!(ok, "restored accounts match the source at the cut", details),
            "transactions" => check_always!(ok, "restored transactions match the source at the cut", details),
            "logs" => check_always!(ok, "restored logs match the source at the cut", details),
            "stats" => check_always!(ok, "restored stats match the source at the cut", details),
            "audit" => check_always!(ok, "restored audit matches the source at the cut", details),
            "numscripts" => check_always!(ok, "restored numscripts match the source at the cut", details),
            "preparedQueries" => check_always!(ok, "restored prepared queries match the source at the cut", details),
            "indexes" => check_always!(ok, "restored indexes match the source at the cut", details),
            "signingKeys" => check_always!(ok, "restored signing keys match the source at the cut", details),
            "sinks" => check_always!(ok, "restored sinks match the source at the cut", details),
            "schemas" => check_always!(ok, "restored metadata schemas match the source at the cut", details),
            other => println!("driver: restore: no property for digest section {other}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn digest(ledgers: Value, tx_sha: &str, per_ledger: Value) -> Digest {
        let mut d = Digest::default();
        d.sections.insert("ledgers".into(), json!({"count": 1, "items": ledgers}));
        for s in ["indexes", "signingKeys", "sinks"] {
            d.sections.insert(s.into(), json!({"count": 0, "items": []}));
        }
        for s in HASH_SECTIONS {
            d.sections.insert(s.to_string(), json!({"count": 3, "sha256": "aa", "lastKey": "l/3"}));
        }
        d.sections.insert("transactions".into(), json!({"count": 3, "sha256": tx_sha, "lastKey": "l/3", "ledgers": per_ledger}));
        d
    }

    #[test]
    fn equal_digests_compare_equal_and_mirror_progress_is_ignored() {
        let a = digest(json!([{"name": "m", "mirrorSyncProgress": {"x": 1}}]), "aa", json!({}));
        let b = digest(json!([{"name": "m", "mirrorSyncProgress": {"x": 2}}]), "aa", json!({}));
        let diffs = compare(&a, &b);
        assert_eq!(diffs.len(), 12);
        assert!(diffs.iter().all(|d| d.equal), "{diffs:?}");
        assert_eq!(a.ledger_names(), vec!["m".to_string()]);
    }

    #[test]
    fn a_difference_reports_section_ledger_and_first_key() {
        let a = digest(json!([{"name": "l", "id": 1}]), "aa", json!({"l": {"count": 3, "sha256": "x"}, "k": {"count": 1, "sha256": "y"}}));
        let b = digest(json!([{"name": "l", "id": 2}]), "bb", json!({"l": {"count": 3, "sha256": "z"}, "k": {"count": 1, "sha256": "y"}}));
        let diffs = compare(&a, &b);
        let l = diffs.iter().find(|d| d.section == "ledgers").unwrap();
        assert!(!l.equal);
        assert_eq!(l.details["firstDiffKey"], "l");
        assert_eq!(l.details["source"]["id"], 1);
        let t = diffs.iter().find(|d| d.section == "transactions").unwrap();
        assert!(!t.equal);
        assert_eq!(t.details["ledgers"], json!([{"ledger": "l", "source": {"count": 3, "sha256": "x"}, "restored": {"count": 3, "sha256": "z"}}]));
        crate::testkit::reset();
        assert_sections(&diffs, &json!({"gen": 1}));
        let a = crate::testkit::assertions();
        assert!(a.iter().any(|r| r.message == "restored transactions match the source at the cut" && !r.cond && r.details["section"] == "transactions"));
        assert!(a.iter().any(|r| r.message == "restored audit matches the source at the cut" && r.cond));
        assert_eq!(a.len(), 12);
    }
}
