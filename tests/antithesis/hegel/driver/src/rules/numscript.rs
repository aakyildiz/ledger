//! Task 13: the Numscript library lifecycle and script transactions, on the
//! Rust-checked ledgers `r-ns-<run>-1` and `r-ns-<run>-2` (DESIGN §3: the
//! oracle never models Numscript, critic Q7). Ports
//! `MD/parallel_driver_numscript_lifecycle`, `MD/parallel_driver_numscripts`,
//! the Numscript branch of `MD/parallel_driver_transactions`, and the inline
//! Numscript rule of the retired `H/main.rs:800-817`, with the retired
//! `H/model.rs` status machinery re-homed in `numscript/model.rs`.
//!
//! Rules: `ns_save` (0.5), `ns_read` (0.5), `ns_exec` (1.0), `ns_unresolved`
//! (0.15, the keyed preparation-gap path). Setup hook
//! `numscript_setup`. Quiet hook `numscript_state` (30 s, weight 0.5, no oracle).
mod model;
mod semver;

use crate::clients::{definitive, CallOpts, Outcome, Route};
use crate::draw;
use crate::machine::{rule, Driver};
use crate::quiet::{HookPlan, QuietCtx, QuietHook};
use crate::setup::SetupHook;
use crate::wire::{apply_logs, created_transaction, parse_biguint_full, postings_of, str_field, u64_field, BigUint, Posting, U256};
use crate::write::UntrackedSend;
use hegel::stateful::ConcurrentRule;
use hegel::TestCase;
use model::{evaluate, compare_volumes, Layout, ListedTx, ListedVolume, NsLedger, NsState, ReadWindow, ScriptRec, Selector, Status, Template, TxRec, Vars, VersionRec, SCRIPT_CAP, TEMPLATES, TRANSFER_V1, TX_CAP, VERSION_CAP};
use semver::{next_after, Bump, Semver};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::Ordering;
use std::sync::MutexGuard;
use std::time::{Duration, Instant};

/// Number of `r-ns-*` ledgers.
pub const LEDGERS: usize = 2;
pub const TRANSFER: &str = "transfer";
pub const V1: Semver = Semver::new(1, 0, 0);
pub const V2: Semver = Semver::new(2, 0, 0);
pub const QUIET_BUDGET: Duration = Duration::from_secs(30);
/// Bound of one setup step (the whole setup must stay under 3 minutes).
const SETUP_STEP: Duration = Duration::from_secs(40);
/// Total bound of the paged reads of one rule.
const READ_BUDGET: Duration = Duration::from_secs(8);

pub fn rules() -> Vec<ConcurrentRule<Driver>> {
    vec![rule("ns_save", 0.5, ns_save), rule("ns_read", 0.5, ns_read), rule("ns_exec", 1.0, ns_exec), rule("ns_unresolved", 0.15, ns_unresolved)]
}

pub fn setup_hooks() -> Vec<SetupHook> {
    vec![SetupHook { name: "numscript_setup", run: numscript_setup }]
}

fn quiet_weight(_: &crate::shared::Shared) -> f64 {
    0.5
}

pub fn quiet_hooks() -> Vec<QuietHook> {
    vec![QuietHook { name: "numscript_state", budget: QUIET_BUDGET, needs_oracle: false, weight: quiet_weight, plan: quiet_plan, run: quiet_run }]
}

fn lock(st: &NsState) -> MutexGuard<'_, Vec<NsLedger>> {
    st.ledgers.lock().unwrap_or_else(|e| e.into_inner())
}

/// The old drivers' `IsTolerated`: success or a non-definitive outcome
/// (unknown, Unavailable, DeadlineExceeded, …).
fn tolerated(out: &Outcome) -> bool {
    out.is_ok() || !definitive(out)
}

fn status_of(out: &Outcome) -> Status {
    if out.is_ok() {
        Status::Acked
    } else if definitive(out) {
        Status::Rejected
    } else {
        Status::Unknown
    }
}

/// The answer to a re-save of an acked version: success is judged by the
/// immutability property; a definitive refusal must be
/// `NUMSCRIPT_VERSION_ALREADY_EXISTS` with kind AlreadyExists
/// (`processor_numscript_library.go:46-48`, `domain/errors.go`).
fn resave_refusal_ok(out: &Outcome) -> bool {
    out.is_ok() || !definitive(out) || (out.reason == "NUMSCRIPT_VERSION_ALREADY_EXISTS" && out.code == "AlreadyExists")
}

fn save_request(ledger: &str, name: &str, version: Semver, content: &str) -> Value {
    json!({"saveNumscript": {"name": name, "content": content, "version": version.to_string(), "ledger": ledger}})
}

fn lib_schedule(d: &Driver) -> Vec<Route> {
    (0..d.s.clients.len().max(1)).map(Route::lib).collect()
}

// ------------------------------------------------------------- setup

/// One keyed setup write, retried (same key) until it is definitive or the
/// step bound passes. `Some(true)` on success or on `tolerated_reason`.
fn setup_write(d: &Driver, req: Value, tolerated_reason: &str) -> bool {
    let key = d.s.next_untracked_key();
    let deadline = Instant::now() + SETUP_STEP;
    loop {
        let mut send = UntrackedSend::new(vec![req.clone()], Some(key.clone()));
        send.schedule = lib_schedule(d);
        let r = d.send_untracked(send);
        if r.outcome.is_ok() || r.outcome.reason == tolerated_reason {
            return true;
        }
        if definitive(&r.outcome) || Instant::now() >= deadline {
            println!("setup: numscript: {req} failed: {}", r.outcome.summary());
            return false;
        }
        std::thread::sleep(Duration::from_secs(1));
    }
}

/// Creates `r-ns-<run>-1` and `-2` (untracked, keyed, retried) and saves
/// `transfer@1.0.0` on each. No draws.
fn numscript_setup(d: &Driver) {
    let st = d.s.ext::<NsState>();
    for n in 1..=LEDGERS as u64 {
        let name = d.s.rust_ledger("ns", n);
        let mut l = NsLedger::new(&name);
        let created = setup_write(d, json!({"createLedger": {"name": name}}), "LEDGER_ALREADY_EXISTS");
        let saved = created && setup_write(d, save_request(&name, TRANSFER, V1, TRANSFER_V1), "NUMSCRIPT_VERSION_ALREADY_EXISTS");
        let mut s = ScriptRec::default();
        s.versions.insert(V1, VersionRec { content: TRANSFER_V1.to_string(), template: Template::Transfer, status: if saved { Status::Acked } else { Status::Unknown } });
        l.library.insert(TRANSFER.to_string(), s);
        l.ready = created && saved;
        println!("setup: numscript ledger {name} ready={}", l.ready);
        lock(&st).push(l);
    }
}

// ------------------------------------------------------------- draws

/// A drawn whitespace/comment layout of `transfer` (tags are counters).
fn draw_layout(d: &Driver, tc: &TestCase) -> Layout {
    let st = d.s.ext::<NsState>();
    let leading = draw::int_in(tc, 0, 2) as u8;
    let indent = draw::int_in(tc, 0, 8) as u8;
    let tabs = draw::chance(tc, 0.25);
    let gap = draw::int_in(tc, 1, 3) as u8;
    let crlf = draw::chance(tc, 0.25);
    let n = draw::int_in(tc, 0, 3);
    let mut comments = Vec::new();
    for _ in 0..n {
        let at = draw::int_in(tc, 0, 9) as u8;
        let block = draw::chance(tc, 0.5);
        comments.push((at, block, st.next_ref.fetch_add(1, Ordering::SeqCst) as u32));
    }
    let trailing = draw::int_in(tc, 0, 2) as u8;
    Layout { leading, indent, tabs, gap, crlf, comments, trailing }
}

fn draw_bump(tc: &TestCase) -> (Bump, u32) {
    let kind = match draw::int_in(tc, 0, 2) {
        0 => Bump::Major,
        1 => Bump::Minor,
        _ => Bump::Patch,
    };
    // Half the bumps are by exactly one, the rest anywhere in the uint32 room.
    let r = if draw::chance(tc, 0.5) { 0 } else { draw::int_in(tc, 0, u32::MAX as u64) as u32 };
    (kind, r)
}

/// The first version of a new `s-<N>` name: `N.0.0` with N in 1..=10
/// (`MD/parallel_driver_numscripts`) or three uint32 components below
/// `u32::MAX` (`MD/parallel_driver_numscript_lifecycle`).
fn draw_first_version(tc: &TestCase) -> Semver {
    if draw::chance(tc, 0.5) {
        Semver::new(draw::int_in(tc, 1, 10) as u32, 0, 0)
    } else {
        let c = |tc: &TestCase| draw::int_in(tc, 0, u32::MAX as u64 - 1) as u32;
        Semver::new(c(tc), c(tc), c(tc))
    }
}

/// Vars of one script transaction: from = a pool address or `world`, to =
/// another pool address, a positive `COIN` or `USD/2` amount (Numscript drops
/// zero-amount postings, `numscript@v0.1.2/internal/funds/funds.go:412`).
fn draw_vars(tc: &TestCase) -> Vars {
    let from = if draw::chance(tc, 0.125) { "world".to_string() } else { draw::pool_address(tc) };
    let n = draw::int_in(tc, 0, 24);
    let mut i = draw::int_in(tc, 0, 99);
    if format!("t-{n}:{i}") == from {
        i = (i + 1) % 100; // never a self-transfer
    }
    let to = format!("t-{n}:{i}");
    let asset = if draw::chance(tc, 0.5) { "COIN" } else { "USD/2" };
    let amount = draw::amount_positive(tc);
    Vars { from, to, amount, asset: asset.to_string() }
}

// ------------------------------------------------------------- ns_save

enum SaveTarget {
    /// Re-save an acked version with other content (expected rejection).
    Resave { name: String, version: Semver },
    /// A next version of `transfer` (`fixed` = the `1.0.0 → 2.0.0` branch).
    Transfer { layout: Layout, fixed: bool, bump: (Bump, u32) },
    /// A next version of an existing `s-<N>`.
    Existing { name: String, template: Template, bump: (Bump, u32) },
    /// The first version of a new `s-<N>`.
    New { template: Template, version: Semver },
}

pub fn ns_save(d: &Driver, tc: TestCase) {
    d.guarded("ns_save", &tc, |d, tc| {
        let st = d.s.ext::<NsState>();
        let li = draw::pick(tc, LEDGERS);
        let (ready, acked, s_names) = {
            let g = lock(&st);
            let l = g.get(li)?;
            let acked: Vec<(String, Semver)> = l.library.iter().flat_map(|(n, s)| s.acked().into_iter().map(move |v| (n.clone(), v))).collect();
            let s_names: Vec<String> = l.library.keys().filter(|n| n.as_str() != TRANSFER).cloned().collect();
            (l.ready, acked, s_names)
        };
        if !ready {
            return None;
        }
        // PLAN: every draw.
        let target = if !acked.is_empty() && draw::chance(tc, 0.125) {
            let (name, version) = draw::one_of(tc, &acked).clone();
            SaveTarget::Resave { name, version }
        } else {
            match draw::int_in(tc, 0, 3) {
                0 | 1 => {
                    let layout = draw_layout(d, tc);
                    let fixed = draw::chance(tc, 0.5);
                    SaveTarget::Transfer { layout, fixed, bump: draw_bump(tc) }
                }
                2 if !s_names.is_empty() => {
                    let name = draw::one_of(tc, &s_names).clone();
                    SaveTarget::Existing { name, template: *draw::one_of(tc, &TEMPLATES), bump: draw_bump(tc) }
                }
                _ => SaveTarget::New { template: *draw::one_of(tc, &TEMPLATES), version: draw_first_version(tc) },
            }
        };
        let schedule = draw::schedule(tc, &d.s, 8);
        let backoffs = draw::backoffs(tc, 8);
        // EXEC: allocate the version under the model lock (concurrent saves
        // never race on one version), record it Pending, send.
        let tag = st.next_ref.fetch_add(1, Ordering::SeqCst) as u32;
        let (ledger, name, version, content, resave, had_acked) = {
            let mut g = lock(&st);
            let l = g.get_mut(li)?;
            let ledger = l.name.clone();
            match target {
                SaveTarget::Resave { name, version } => {
                    let old = l.library.get(&name)?.versions.get(&version)?.content.clone();
                    (ledger, name, version, format!("{old}\n// resave {tag}\n"), true, true)
                }
                SaveTarget::Transfer { layout, fixed, bump } => {
                    let s = l.library.entry(TRANSFER.to_string()).or_default();
                    if s.versions.len() >= VERSION_CAP {
                        return None;
                    }
                    let base = s.max_any().unwrap_or(V1);
                    let v = if fixed && base < V2 { V2 } else { next_after(base, bump.0, bump.1)? };
                    let content = layout.render();
                    let had = s.max_acked().is_some();
                    s.versions.insert(v, VersionRec { content: content.clone(), template: Template::Transfer, status: Status::Pending });
                    (ledger, TRANSFER.to_string(), v, content, false, had)
                }
                SaveTarget::Existing { name, template, bump } => {
                    let s = l.library.get_mut(&name)?;
                    if s.versions.len() >= VERSION_CAP {
                        return None;
                    }
                    let v = next_after(s.max_any()?, bump.0, bump.1)?;
                    let content = template.text(Some(tag));
                    let had = s.max_acked().is_some();
                    s.versions.insert(v, VersionRec { content: content.clone(), template, status: Status::Pending });
                    (ledger, name, v, content, false, had)
                }
                SaveTarget::New { template, version } => {
                    if l.next_script >= SCRIPT_CAP {
                        return None;
                    }
                    l.next_script += 1;
                    let name = format!("s-{}", l.next_script);
                    let content = template.text(Some(tag));
                    let mut s = ScriptRec::default();
                    s.versions.insert(version, VersionRec { content: content.clone(), template, status: Status::Pending });
                    l.library.insert(name.clone(), s);
                    (ledger, name, version, content, false, false)
                }
            }
        };
        let mut send = UntrackedSend::new(vec![save_request(&ledger, &name, version, &content)], Some(d.s.next_untracked_key()));
        send.schedule = schedule;
        send.backoffs_ms = backoffs;
        let r = d.send_untracked(send);
        let out = &r.outcome;
        let details = json!({"ledger": ledger, "scriptName": name, "version": version.to_string(), "outcome": out.summary(), "attempts": r.attempts.len()});
        if resave {
            check_always_or_unreachable!(!out.is_ok(), "a saved numscript version is immutable", details.clone());
            check_always_or_unreachable!(resave_refusal_ok(out), "a re-save of a saved numscript version is refused as already existing", details);
            return Some(());
        }
        {
            let mut g = lock(&st);
            if let Some(rec) = g.get_mut(li).and_then(|l| l.library.get_mut(&name)).and_then(|s| s.versions.get_mut(&version)) {
                rec.status = status_of(out);
            }
        }
        if had_acked {
            check_sometimes!(tolerated(out), "should be able to save second numscript version", details);
        } else {
            check_sometimes!(tolerated(out), "should be able to save numscript", details);
        }
        if definitive(out) && !out.is_ok() {
            println!("driver: ns_save: {name}@{version} on {ledger} refused: {}", out.summary());
        }
        Some(())
    });
}

// ------------------------------------------------------------- ns_read

enum ReadOp {
    Get { selector: String },
    List { page_size: u64 },
    Versions,
}

/// The read windows of every name, as of now.
fn windows(st: &NsState, li: usize, after: bool) -> BTreeMap<String, BTreeSet<Semver>> {
    let g = lock(st);
    let Some(l) = g.get(li) else { return BTreeMap::new() };
    l.library.keys().map(|n| (n.clone(), if after { l.possible_versions(n) } else { l.acked_versions(n) })).collect()
}

fn content_of(st: &NsState, li: usize, name: &str, v: Semver) -> Option<String> {
    lock(st).get(li)?.library.get(name)?.versions.get(&v).map(|r| r.content.clone())
}

pub fn ns_read(d: &Driver, tc: TestCase) {
    d.guarded("ns_read", &tc, |d, tc| {
        let st = d.s.ext::<NsState>();
        let li = draw::pick(tc, LEDGERS);
        let (ready, ledger, names) = {
            let g = lock(&st);
            let l = g.get(li)?;
            (l.ready, l.name.clone(), l.acked_names())
        };
        if !ready || names.is_empty() {
            return None;
        }
        let name = draw::one_of(tc, &names).clone();
        let acked: Vec<Semver> = lock(&st).get(li)?.acked_versions(&name).into_iter().collect();
        let op = match draw::int_in(tc, 0, 3) {
            0 | 1 => {
                let selector = match draw::int_in(tc, 0, 4) {
                    0 => String::new(),
                    1 => "latest".to_string(),
                    2 => draw::one_of(tc, &acked).to_string(),
                    3 => draw::one_of(tc, &acked).major.to_string(),
                    _ => {
                        let v = draw::one_of(tc, &acked);
                        format!("{}.{}", v.major, v.minor)
                    }
                };
                ReadOp::Get { selector }
            }
            2 => ReadOp::List { page_size: draw::page_size(tc) },
            _ => ReadOp::Versions,
        };
        let route = draw::route(tc, &d.s);
        // EXEC
        let before = windows(&st, li, false);
        match op {
            ReadOp::Get { selector } => {
                let req = json!({"ledger": ledger, "name": name, "version": selector});
                let (_, out) = d.call(&route, "bucket", "GetNumscript", req, &CallOpts::default());
                let after = windows(&st, li, true);
                let w = ReadWindow { acked_before: before.get(&name).cloned().unwrap_or_default(), possible_after: after.get(&name).cloned().unwrap_or_default() };
                eval_get(&st, li, &ledger, &name, &selector, &w, &out);
            }
            ReadOp::List { page_size } => {
                let (items, complete, err) = list_numscripts(d, &route, &ledger, page_size);
                let after = windows(&st, li, true);
                if let Some(out) = err {
                    check_always_or_unreachable!(
                        tolerated(&out),
                        "ListNumscripts should not return unexpected error",
                        json!({"ledger": ledger, "outcome": out.summary()})
                    );
                } else {
                    check_always_or_unreachable!(true, "ListNumscripts should not return unexpected error", json!({"ledger": ledger}));
                }
                if complete {
                    eval_list(&st, li, &ledger, &before, &after, &items);
                }
            }
            ReadOp::Versions => {
                let req = json!({"ledger": ledger, "name": name});
                let (_, out) = d.call(&route, "bucket", "ListNumscriptVersions", req, &CallOpts::default());
                let after = windows(&st, li, true);
                let w = ReadWindow { acked_before: before.get(&name).cloned().unwrap_or_default(), possible_after: after.get(&name).cloned().unwrap_or_default() };
                eval_versions(&ledger, &name, &w, &out);
            }
        }
        Some(())
    });
}

/// A server refusal that says the script or version does not exist: a
/// definitive reason, or the bare `NotFound` code GetNumscript answers with.
fn refused_as_missing(out: &Outcome) -> bool {
    !out.is_ok() && (definitive(out) || (!out.local && !out.is_unknown() && out.code == "NotFound"))
}

/// GetNumscript on a name with an acked version matching the selector.
fn eval_get(st: &NsState, li: usize, ledger: &str, name: &str, selector: &str, w: &ReadWindow, out: &Outcome) {
    let details = |extra: Value| json!({"ledger": ledger, "scriptName": name, "selector": selector, "ackedBefore": w.acked_before.iter().map(|v| v.to_string()).collect::<Vec<_>>(), "possibleAfter": w.possible_after.iter().map(|v| v.to_string()).collect::<Vec<_>>(), "got": extra});
    if !out.is_ok() {
        if refused_as_missing(out) {
            // A definitive refusal of a selector some acked version matches. The
            // server answers a missing script with a bare gRPC NotFound, no reason
            // (`controller_default.go` GetNumscript, `adapter/grpc/server.go:662-664`).
            check_always_or_unreachable!(false, "a numscript read returns a saved version and its content", details(out.summary()));
        }
        return;
    }
    let resp = out.response.clone().unwrap_or(Value::Null);
    let got_name = str_field(&resp, "name");
    check_always_or_unreachable!(got_name == name, "saved numscript should be readable", details(json!({"name": got_name})));
    let got_v = Semver::parse(str_field(&resp, "version"));
    let legal = got_v.is_some_and(|v| w.legal_resolution(selector, v));
    let content_ok = got_v.and_then(|v| content_of(st, li, name, v)).is_some_and(|c| c.trim() == str_field(&resp, "content").trim());
    let got = json!({"version": str_field(&resp, "version"), "contentMatches": content_ok});
    if selector.is_empty() || selector == "latest" {
        check_always_or_unreachable!(legal, "GetNumscript should return latest version", details(got.clone()));
    }
    check_always_or_unreachable!(legal && content_ok, "a numscript read returns a saved version and its content", details(got));
}

/// Pages ListNumscripts. Returns the items, whether the traversal is
/// complete, and the first failed outcome (if any).
fn list_numscripts(d: &Driver, route: &Route, ledger: &str, page_size: u64) -> (Vec<Value>, bool, Option<Outcome>) {
    let deadline = Instant::now() + READ_BUDGET;
    let mut items = Vec::new();
    let mut cursor = String::new();
    loop {
        // Each page gets what is left of the rule's read budget.
        let left = deadline.saturating_duration_since(Instant::now());
        if left < Duration::from_millis(200) {
            return (items, false, None);
        }
        let req = json!({"ledger": ledger, "options": {"pageSize": page_size, "cursor": cursor}});
        let (_, out) = d.call(route, "bucket", "ListNumscripts", req, &CallOpts::default().timeout(left.min(crate::clients::RPC_TIMEOUT)).max_items(1001));
        if !out.is_ok() {
            return (items, false, Some(out));
        }
        items.extend(out.items.clone().unwrap_or_default());
        if !out.complete {
            return (items, false, None);
        }
        if out.next_cursor.is_empty() {
            return (items, true, None);
        }
        cursor = out.next_cursor.clone();
    }
}

fn eval_list(st: &NsState, li: usize, ledger: &str, before: &BTreeMap<String, BTreeSet<Semver>>, after: &BTreeMap<String, BTreeSet<Semver>>, items: &[Value]) {
    let listed: BTreeMap<&str, &Value> = items.iter().map(|i| (str_field(i, "name"), i)).collect();
    let missing: Vec<&String> = before.iter().filter(|(n, acked)| !acked.is_empty() && !listed.contains_key(n.as_str())).map(|(n, _)| n).collect();
    check_always_or_unreachable!(missing.is_empty(), "saved numscript should appear in ListNumscripts", json!({"ledger": ledger, "missing": missing, "listed": listed.keys().collect::<Vec<_>>()}));
    let mut bad = Vec::new();
    for (name, item) in &listed {
        let w = ReadWindow { acked_before: before.get(*name).cloned().unwrap_or_default(), possible_after: after.get(*name).cloned().unwrap_or_default() };
        let v = Semver::parse(str_field(item, "version"));
        let legal = v.is_some_and(|v| w.legal_latest(v));
        let content_ok = v.and_then(|v| content_of(st, li, name, v)).is_some_and(|c| c.trim() == str_field(item, "content").trim());
        if !(legal && content_ok) {
            bad.push(json!({"name": name, "version": str_field(item, "version"), "contentMatches": content_ok, "possible": w.possible_after.iter().map(|v| v.to_string()).collect::<Vec<_>>()}));
        }
    }
    check_always_or_unreachable!(bad.is_empty(), "a numscript read returns a saved version and its content", json!({"ledger": ledger, "op": "ListNumscripts", "bad": bad}));
}

fn eval_versions(ledger: &str, name: &str, w: &ReadWindow, out: &Outcome) {
    let win = json!({"ackedBefore": w.acked_before.iter().map(|v| v.to_string()).collect::<Vec<_>>(), "possibleAfter": w.possible_after.iter().map(|v| v.to_string()).collect::<Vec<_>>()});
    if !out.is_ok() {
        if refused_as_missing(out) {
            check_always_or_unreachable!(false, "a numscript read returns a saved version and its content", json!({"ledger": ledger, "scriptName": name, "op": "ListNumscriptVersions", "outcome": out.summary(), "window": win}));
        }
        return;
    }
    let resp = out.response.clone().unwrap_or(Value::Null);
    let latest = str_field(&resp, "latestVersion").to_string();
    let legal = Semver::parse(&latest).is_some_and(|v| w.legal_latest(v));
    check_always_or_unreachable!(legal, "latest pointer should be the greatest saved semver", json!({"ledger": ledger, "scriptName": name, "latest": latest, "window": win}));
    let listed: Option<BTreeSet<Semver>> = resp.get("versions").and_then(Value::as_array).unwrap_or(&vec![]).iter().map(|e| Semver::parse(str_field(e, "version"))).collect();
    // `latestVersion` and `versions` come from one read handle
    // (`query/numscript.go` ReadAllNumscriptVersions): the pointer is the
    // greatest listed version, whatever saves were concurrent.
    let max_listed = listed.as_ref().and_then(|l| l.iter().next_back().copied());
    let pointer_ok = max_listed.is_some() && Semver::parse(&latest) == max_listed;
    check_always_or_unreachable!(
        pointer_ok,
        "latest pointer should be the greatest saved semver",
        json!({"ledger": ledger, "scriptName": name, "latest": latest, "maxListed": max_listed.map(|v| v.to_string()), "versions": resp.get("versions")})
    );
    let ok = listed.as_ref().is_some_and(|l| w.legal_listing(l));
    check_always_or_unreachable!(ok, "a numscript read returns a saved version and its content", json!({"ledger": ledger, "scriptName": name, "op": "ListNumscriptVersions", "versions": resp.get("versions"), "window": win}));
}

// ------------------------------------------------------------- ns_exec

/// The checked parts of a successful single-request Apply.
pub(crate) struct ExecCheck {
    /// One log whose created transaction's postings are one of the candidates.
    pub ok: bool,
    pub postings: Option<Vec<Posting>>,
    pub id: Option<u64>,
    /// Problems with the created transaction's `postCommitVolumes` (empty
    /// when there is no created transaction: `ok` already fails then).
    pub volume_problems: Vec<String>,
}

/// The created transaction of a successful single-request Apply.
pub(crate) fn check_exec_response(resp: Option<&Value>, candidates: &[Vec<Posting>]) -> ExecCheck {
    let logs = resp.map(apply_logs).unwrap_or_default();
    let tx = logs.first().and_then(created_transaction).cloned();
    let postings = tx.as_ref().and_then(postings_of);
    let id = tx.as_ref().map(|t| u64_field(t, "id"));
    let ok = logs.len() == 1 && postings.as_ref().is_some_and(|p| candidates.contains(p));
    let volume_problems = match (&tx, &postings) {
        (Some(t), Some(p)) => post_commit_volume_problems(t, p),
        _ => Vec::new(),
    };
    ExecCheck { ok, postings, id, volume_problems }
}

/// The old `CheckPostCommitVolumes` (`workload/internal/checks.go:56-87`,
/// reached from `MD/parallel_driver_numscripts` via `CheckCreatedTransaction`),
/// strengthened: when `postCommitVolumes` is present, every posting's
/// (account, asset) has an uncolored entry with decodable input and output
/// (the server always sets both, `processor_volumes.go:60-80`), and each
/// cumulative volume covers what this transaction moved: input ≥ the
/// amounts received, output ≥ the amounts sent. An absent
/// `postCommitVolumes` is tolerated, like the old helper.
pub(crate) fn post_commit_volume_problems(tx: &Value, postings: &[Posting]) -> Vec<String> {
    let Some(pcv) = tx.get("postCommitVolumes") else { return Vec::new() };
    let by_account = pcv.get("volumesByAccount").and_then(Value::as_object);
    let zero = U256::from(0u64);
    let mut moved: BTreeMap<(String, String), (U256, U256)> = BTreeMap::new();
    for p in postings {
        let src = moved.entry((p.source.clone(), p.asset.clone())).or_insert((zero, zero));
        src.1 = src.1.checked_add(&p.amount).unwrap_or(src.1);
        let dst = moved.entry((p.destination.clone(), p.asset.clone())).or_insert((zero, zero));
        dst.0 = dst.0.checked_add(&p.amount).unwrap_or(dst.0);
    }
    let mut out = Vec::new();
    for ((account, asset), (got_in, got_out)) in &moved {
        let entry = by_account
            .and_then(|m| m.get(account))
            .and_then(|v| v.get("volumes"))
            .and_then(Value::as_array)
            .and_then(|es| es.iter().find(|e| str_field(e, "asset") == asset && str_field(e, "color").is_empty()));
        let Some(entry) = entry else {
            out.push(format!("{account} {asset}: no post-commit volume entry"));
            continue;
        };
        let vols = entry.get("volumes");
        let field = |k: &str| vols.and_then(|v| v.get(k)).filter(|x| x.is_object()).and_then(parse_biguint_full);
        match (field("input"), field("output")) {
            (Some(i), Some(o)) => {
                // Beyond 256 bits counts as large enough.
                let below = |v: &BigUint, moved: &U256| v.to_u256().is_some_and(|v| v < *moved);
                if below(&i, got_in) || below(&o, got_out) {
                    out.push(format!("{account} {asset}: post-commit input {} output {} below this transaction's in {} out {}", vols.and_then(|v| v.get("input")).unwrap_or(&Value::Null), vols.and_then(|v| v.get("output")).unwrap_or(&Value::Null), got_in.to_json(), got_out.to_json()));
                }
            }
            _ => out.push(format!("{account} {asset}: post-commit volume entry has a missing or invalid input/output: {entry}")),
        }
    }
    out
}

fn exec_request(ledger: &str, sel: &Selector, vars: &Vars, reference: &str) -> Value {
    let mut ct = json!({"reference": reference});
    match sel {
        Selector::Inline => ct["script"] = json!({"plain": TRANSFER_V1, "vars": vars.to_json()}),
        Selector::Latest(name) => ct["scriptReference"] = json!({"name": name, "version": "latest", "vars": vars.to_json()}),
        Selector::Pinned(name, v) => ct["scriptReference"] = json!({"name": name, "version": v.to_string(), "vars": vars.to_json()}),
    }
    json!({"apply": {"ledger": ledger, "action": {"createTransaction": ct}}})
}

pub fn ns_exec(d: &Driver, tc: TestCase) {
    d.guarded("ns_exec", &tc, |d, tc| {
        let st = d.s.ext::<NsState>();
        let li = draw::pick(tc, LEDGERS);
        let (ready, full, names) = {
            let g = lock(&st);
            let l = g.get(li)?;
            (l.ready, l.txs.len() >= TX_CAP, l.acked_names())
        };
        if !ready || full {
            return None;
        }
        // PLAN
        let sel = match draw::int_in(tc, 0, 3) {
            0 => Selector::Inline,
            _ if names.is_empty() => Selector::Inline,
            m => {
                let name = draw::one_of(tc, &names).clone();
                if m == 3 {
                    let acked: Vec<Semver> = lock(&st).get(li)?.acked_versions(&name).into_iter().collect();
                    Selector::Pinned(name, *draw::one_of(tc, &acked))
                } else {
                    Selector::Latest(name)
                }
            }
        };
        let vars = draw_vars(tc);
        let schedule = draw::schedule(tc, &d.s, 8);
        let backoffs = draw::backoffs(tc, 8);
        // EXEC
        let reference = format!("r-{}-ns{}", d.s.run, st.next_ref.fetch_add(1, Ordering::SeqCst));
        let key = d.s.next_untracked_key();
        let (ledger, floor, idx) = {
            let mut g = lock(&st);
            let l = g.get_mut(li)?;
            let floor = l.floor(&sel);
            let candidates = l.candidates_since(&sel, &vars, floor);
            if candidates.is_empty() {
                return None;
            }
            let idx = l.record_tx(TxRec { reference: reference.clone(), key: key.clone(), selector: sel.clone(), vars: vars.clone(), candidates, floor, status: Status::Pending, tx_id: None, committed: None });
            (l.name.clone(), floor, idx)
        };
        let mut send = UntrackedSend::new(vec![exec_request(&ledger, &sel, &vars, &reference)], Some(key));
        send.schedule = schedule;
        send.backoffs_ms = backoffs;
        let r = d.send_untracked(send);
        let out = &r.outcome;
        let mut details = json!({"ledger": ledger, "reference": reference, "selector": format!("{sel:?}"), "vars": vars.to_json(), "outcome": out.summary(), "attempts": r.attempts.len()});
        let status = status_of(out);
        // Candidates as of the answer, with the floor of the send: a save of
        // `name` that started while the transaction was in flight may be the
        // version `latest` resolved to.
        let candidates = lock(&st).get(li).map(|l| l.candidates_since(&sel, &vars, floor)).unwrap_or_default();
        let (mut committed, mut tx_id) = (None, None);
        if out.is_ok() {
            let c = check_exec_response(out.response.as_ref(), &candidates);
            details["expected"] = json!(candidates.iter().map(|c| c.iter().map(Posting::to_json).collect::<Vec<_>>()).collect::<Vec<_>>());
            details["got"] = json!(c.postings.as_ref().map(|p| p.iter().map(Posting::to_json).collect::<Vec<_>>()));
            check_always!(c.ok, "a numscript transaction commits exactly the postings its script implies", details.clone());
            details["volumeProblems"] = json!(c.volume_problems);
            check_always!(c.volume_problems.is_empty(), "a numscript transaction reports consistent post-commit volumes", details);
            committed = c.postings;
            tx_id = c.id;
        }
        {
            let mut g = lock(&st);
            if let Some(rec) = g.get_mut(li).and_then(|l| l.txs.get_mut(idx)) {
                rec.candidates = candidates;
                rec.status = status;
                rec.committed = committed;
                rec.tx_id = tx_id;
            }
        }
        match sel {
            Selector::Inline => check_sometimes!(out.is_ok(), "a numscript transaction commits", json!({"ledger": ledger, "outcome": out.summary()})),
            _ => check_sometimes!(tolerated(out), "should be able to use saved numscript in transaction", json!({"ledger": ledger, "reference": reference, "outcome": out.summary()})),
        }
        if definitive(out) && !out.is_ok() {
            println!("driver: ns_exec {reference} on {ledger} refused: {}", out.summary());
        }
        Some(())
    });
}

// ------------------------------------------------------------- ns_unresolved

/// The whole retry budget of one `ns_unresolved` send (every attempt is
/// refused as retryable, so the send always runs to its budget).
const UNRESOLVED_BUDGET: Duration = Duration::from_secs(3);

/// An inline script whose destination comes from `meta(@<cfg>, "dest")` of an
/// account that never has that key: dependency resolution fails after a
/// mutable metadata read, so it can never commit.
pub fn unresolved_script(cfg: &str) -> String {
    format!("vars {{\n  account $dst = meta(@{cfg}, \"dest\")\n}}\nsend [COIN 1] (\n  source = @world\n  destination = $dst\n)\n")
}

/// The keyed-retry path of a preparation gap (EN-1406, EN-1557): with an
/// idempotency key, admission cannot tell a failed resolution from the replay
/// of a frozen outcome, so it forwards the order marked `preload_unavailable`
/// and the FSM, finding no frozen outcome, answers the retryable
/// `PRELOAD_UNAVAILABLE` without auditing or freezing it
/// (`admission.go` `classifyResolutionFailure`, `machine.go`); without a key
/// it fails fast with `DEPENDENCY_DISCOVERY_FAILED`. The other templates of
/// this family never read state while resolving, so this rule is the only
/// source of `a write attempt hits a preload miss` (`write::explore_attempt`
/// sees every attempt). The `cfg` account is fresh per send and nothing ever
/// writes its metadata, so a commit is a finding.
pub fn ns_unresolved(d: &Driver, tc: TestCase) {
    d.guarded("ns_unresolved", &tc, |d, tc| {
        let st = d.s.ext::<NsState>();
        let li = draw::pick(tc, LEDGERS);
        let keyed = draw::chance(tc, 0.75);
        let schedule = draw::schedule(tc, &d.s, 4);
        let backoffs = draw::backoffs(tc, 4);
        let ledger = {
            let g = lock(&st);
            let l = g.get(li)?;
            if !l.ready {
                return None;
            }
            l.name.clone()
        };
        let n = st.next_ref.fetch_add(1, Ordering::SeqCst);
        let reference = format!("r-{}-nsu{n}", d.s.run);
        let cfg = format!("ns-cfg:{n}");
        let req = json!({"apply": {"ledger": ledger, "action": {"createTransaction": {"reference": reference, "script": {"plain": unresolved_script(&cfg), "vars": {}}}}}});
        let mut send = UntrackedSend::new(vec![req], keyed.then(|| d.s.next_untracked_key()));
        send.schedule = schedule;
        send.backoffs_ms = backoffs;
        send.budget = UNRESOLVED_BUDGET;
        let r = d.send_untracked(send);
        let reasons: Vec<&str> = r.attempts.iter().map(|a| a.reason.as_str()).collect();
        let details = json!({"ledger": ledger, "reference": reference, "keyed": keyed, "outcome": r.outcome.summary(), "reasons": reasons});
        check_always!(!r.outcome.is_ok(), "a numscript whose meta() source is absent never commits", details);
        Some(())
    });
}

// ------------------------------------------------------------- quiet hook

fn quiet_plan(_d: &Driver, tc: &TestCase) -> HookPlan {
    Box::new(draw::chance(tc, 0.5))
}

/// Pages a whole streamed listing (ListTransactions, ListAccounts) through
/// one route. `None` if it could not finish before the deadline.
fn list_all(d: &Driver, route: &Route, method: &str, ledger: &str, deadline: Instant) -> Option<Vec<Value>> {
    let mut items = Vec::new();
    let mut cursor = String::new();
    let mut failures = 0;
    loop {
        let left = deadline.saturating_duration_since(Instant::now());
        if left < Duration::from_millis(200) {
            return None;
        }
        let req = json!({"ledger": ledger, "options": {"pageSize": 1000, "cursor": cursor}});
        let opts = CallOpts::default().timeout(left.min(Duration::from_secs(10))).max_items(1001);
        let (_, out) = d.call(route, "bucket", method, req, &opts);
        if !out.is_ok() || !out.complete {
            failures += 1;
            if failures > 3 {
                println!("driver: numscript_state: {method} {ledger} failed: {}", out.summary());
                return None;
            }
            // Back off (300, 600, 900 ms) inside the deadline before retrying the page.
            let pause = Duration::from_millis(300 * failures as u64).min(deadline.saturating_duration_since(Instant::now()));
            std::thread::sleep(pause);
            continue;
        }
        items.extend(out.items.clone().unwrap_or_default());
        if out.next_cursor.is_empty() {
            return Some(items);
        }
        cursor = out.next_cursor.clone();
    }
}

fn listed_txs(items: &[Value]) -> Option<Vec<ListedTx>> {
    items.iter().map(|t| Some(ListedTx { id: u64_field(t, "id"), reference: str_field(t, "reference").to_string(), postings: postings_of(t)? })).collect()
}

fn listed_volumes(items: &[Value]) -> Vec<ListedVolume> {
    let mut out = Vec::new();
    for a in items {
        for v in a.get("volumes").and_then(Value::as_array).unwrap_or(&vec![]) {
            let (input, output, balance) = crate::wire::volumes(v).unwrap_or((i128::MIN, i128::MIN, i128::MIN));
            out.push(ListedVolume { address: str_field(a, "address").to_string(), asset: str_field(v, "asset").to_string(), color: str_field(v, "color").to_string(), input, output, balance });
        }
    }
    out
}

/// Checks one ledger fully, or returns `None` (deadline, persistent
/// failure). The Always properties are evaluated only for a fully read ledger.
fn check_ledger(d: &Driver, route: &Route, li: usize, deadline: Instant) -> Option<()> {
    let st = d.s.ext::<NsState>();
    let (ready, ledger) = {
        let g = lock(&st);
        let l = g.get(li)?;
        (l.ready, l.name.clone())
    };
    if !ready {
        return Some(());
    }
    let mut txs = listed_txs(&list_all(d, route, "ListTransactions", &ledger, deadline)?)?;
    let mut accounts = list_all(d, route, "ListAccounts", &ledger, deadline)?;
    let opts = CallOpts::default().timeout(deadline.saturating_duration_since(Instant::now()).min(Duration::from_secs(5)));
    let (_, stats) = d.call(route, "bucket", "GetLedgerStats", json!({"ledger": ledger}), &opts);
    if !stats.is_ok() || Instant::now() > deadline {
        return None;
    }
    let count = stats.response.as_ref().map(|r| u64_field(r, "numscriptExecutionCount")).unwrap_or(0);
    let mut resolved_wrong = Vec::new();
    let mut e;
    let mut mismatches;
    let mut overdraft;
    let mut attempt = 0;
    loop {
        attempt += 1;
        (e, overdraft) = {
            let mut g = lock(&st);
            let l = g.get_mut(li)?;
            (evaluate(l, &txs), l.overdraft.len())
        };
        // Resolution is sticky (a record resolved here is Acked next time), so
        // keep every pass's verdicts.
        resolved_wrong.append(&mut e.resolved_wrong);
        mismatches = compare_volumes(&e.expected, &listed_volumes(&accounts));
        if mismatches.is_empty() || attempt >= 3 {
            break;
        }
        // The two listings are separate snapshots: a write the driver gave up
        // on can still land between them (a client-side retry outlives the Q1
        // barrier). Re-read accounts, then transactions (so every commit the
        // accounts reflect is listed), and judge again; only a mismatch that
        // persists over three reads is asserted.
        println!("driver: numscript_state: {ledger}: {} volume mismatches on read {attempt}, re-reading", mismatches.len());
        accounts = list_all(d, route, "ListAccounts", &ledger, deadline)?;
        txs = listed_txs(&list_all(d, route, "ListTransactions", &ledger, deadline)?)?;
    }
    let base = json!({"ledger": ledger, "listed": txs.len(), "acked": e.acked_checked, "resolved": e.resolved_committed, "unresolved": e.unresolved, "foreign": e.foreign, "overdraftable": overdraft, "reads": attempt});
    check_always!(e.acked_lost.is_empty(), "an acknowledged numscript transaction survives faults", json!({"summary": base, "lost": e.acked_lost.iter().take(20).collect::<Vec<_>>()}));
    check_always!(e.rejected_listed.is_empty(), "a definitively rejected numscript transaction never takes effect", json!({"summary": base, "committed": e.rejected_listed.iter().take(20).collect::<Vec<_>>()}));
    check_always!(mismatches.is_empty(), "a numscript ledger's balances equal its committed postings", json!({"summary": base, "mismatches": mismatches.iter().take(20).collect::<Vec<_>>()}));
    // An unacknowledged transaction found committed: its listed postings must
    // be one its script could produce (the ack path checks this in ns_exec).
    check_always!(
        resolved_wrong.is_empty(),
        "a numscript transaction commits exactly the postings its script implies",
        json!({"summary": base, "op": "quiet resolution", "wrong": resolved_wrong.iter().take(20).collect::<Vec<_>>()})
    );
    // The count of committed script transactions does not depend on records
    // that stay unresolved (they are not committed as of this listing).
    check_sometimes!(count == e.committed_scripts, "numscript execution count caught up", json!({"ledger": ledger, "count": count, "committed": e.committed_scripts, "unresolved": e.unresolved}));
    if count > e.committed_scripts {
        println!("driver: numscript_state: {ledger}: execution count {count} above {} committed script transactions ({} unresolved)", e.committed_scripts, e.unresolved);
    }
    Some(())
}

fn quiet_run(d: &Driver, ctx: &mut QuietCtx, plan: &HookPlan) {
    let swap = plan.downcast_ref::<bool>().copied().unwrap_or(false);
    let order: [usize; LEDGERS] = if swap { [1, 0] } else { [0, 1] };
    let route = Route::lib(ctx.client);
    let mut complete = true;
    for li in order {
        if Instant::now() >= ctx.deadline || check_ledger(d, &route, li, ctx.deadline).is_none() {
            complete = false;
            break;
        }
    }
    check_sometimes!(complete, "numscript_state completes within its quiet budget", json!({"swap": swap, "remainingMs": ctx.remaining().as_millis() as u64}));
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testkit::{self, FakeReply};

    fn ok(v: Value) -> FakeReply {
        FakeReply::Outcome(Outcome::from_json(&json!({"ok": true, "response": v})))
    }

    fn setup(d: &Driver) {
        numscript_setup(d);
        let st = d.s.ext::<NsState>();
        let g = lock(&st);
        assert_eq!(g.len(), 2);
        assert_eq!(g[0].name, "r-ns-test-1");
        assert!(g.iter().all(|l| l.ready && l.library[TRANSFER].acked() == vec![V1]));
    }

    #[test]
    fn setup_creates_the_two_ledgers_and_saves_transfer() {
        let (d, ft, fo) = testkit::driver();
        setup(&d);
        let applies = ft.calls_of("bucket", "Apply");
        assert_eq!(applies.len(), 4);
        assert_eq!(applies[0]["request"]["unsigned"]["requests"][0], json!({"createLedger": {"name": "r-ns-test-1"}}));
        let save = &applies[1]["request"]["unsigned"]["requests"][0]["saveNumscript"];
        assert_eq!(save["version"], "1.0.0");
        assert_eq!(save["name"], "transfer");
        assert_eq!(save["content"], TRANSFER_V1);
        for a in &applies {
            assert!(a["request"]["unsigned"]["idempotencyKey"].as_str().unwrap().starts_with("r-test-k"));
        }
        assert!(fo.calls("/write/begin").is_empty(), "never through the oracle");
    }

    #[test]
    fn setup_tolerates_existing_entities_and_marks_failures() {
        let (d, ft, _) = testkit::driver();
        let refuse = |r: &str, c: &str| FakeReply::Outcome(Outcome::from_json(&json!({"ok": false, "code": c, "reason": r})));
        ft.script("bucket", "Apply", vec![refuse("LEDGER_ALREADY_EXISTS", "AlreadyExists"), refuse("NUMSCRIPT_VERSION_ALREADY_EXISTS", "AlreadyExists"), refuse("NUMSCRIPT_PARSE_ERROR", "InvalidArgument")]);
        numscript_setup(&d);
        let st = d.s.ext::<NsState>();
        let g = lock(&st);
        assert!(g[0].ready);
        assert!(!g[1].ready, "the second ledger's creation was refused");
    }

    #[test]
    fn ns_save_allocates_strictly_greater_versions_and_semantically_identical_transfers() {
        let (d, ft, _) = testkit::driver();
        setup(&d);
        testkit::reset();
        for _ in 0..40 {
            testkit::run_rule(&d, ns_save);
        }
        let st = d.s.ext::<NsState>();
        let g = lock(&st);
        let mut saves = 0;
        for c in ft.calls_of("bucket", "Apply").iter().skip(4) {
            let s = &c["request"]["unsigned"]["requests"][0]["saveNumscript"];
            let name = s["name"].as_str().unwrap();
            let v = Semver::parse(s["version"].as_str().unwrap()).expect("full semver");
            let content = s["content"].as_str().unwrap();
            let l = g.iter().find(|l| l.name == s["ledger"].as_str().unwrap()).unwrap();
            let rec = &l.library[name].versions[&v];
            if rec.content != content {
                assert!(content.contains("// resave "), "only a re-save sends other content for a known version");
                continue;
            }
            saves += 1;
            if name == TRANSFER {
                assert_eq!(model::normalize(content), model::normalize(TRANSFER_V1), "{content:?}");
            } else {
                assert!(name.starts_with("s-"));
            }
        }
        assert!(saves > 0);
        for l in g.iter() {
            assert!(l.library.values().all(|s| s.versions.values().all(|r| r.status == Status::Acked)), "the fake acks everything");
            assert!(l.library[TRANSFER].versions.keys().all(|v| *v >= V1));
        }
        // With a fake that acks everything, a re-save "succeeds": the immutability property sees it.
        for a in testkit::assertions().iter().filter(|a| a.message == "a saved numscript version is immutable") {
            assert!(!a.cond);
        }
        assert!(testkit::assertions().iter().any(|a| a.message.starts_with("should be able to save") && a.cond));
    }

    #[test]
    fn concurrent_style_allocation_never_reuses_a_version() {
        // Two saves planned from the same snapshot still get distinct versions.
        let mut l = NsLedger::new("x");
        let mut s = ScriptRec::default();
        s.versions.insert(V1, VersionRec { content: String::new(), template: Template::Transfer, status: Status::Acked });
        l.library.insert(TRANSFER.into(), s);
        let s = l.library.get_mut(TRANSFER).unwrap();
        let a = next_after(s.max_any().unwrap(), Bump::Patch, 0).unwrap();
        s.versions.insert(a, VersionRec { content: String::new(), template: Template::Transfer, status: Status::Pending });
        let b = next_after(s.max_any().unwrap(), Bump::Patch, 0).unwrap();
        assert!(b > a && a > V1);
    }

    #[test]
    fn vars_are_positive_and_never_self_transfers() {
        testkit::with_tc_n(300, |tc| {
            let v = draw_vars(tc);
            assert!(v.amount > 0);
            assert_ne!(v.from, v.to);
            assert!(v.from == "world" || v.from.starts_with("t-"));
            assert!(v.to.starts_with("t-") && !v.to.contains("own-") && !v.from.contains("own-"));
            assert!(v.asset == "COIN" || v.asset == "USD/2");
            let f = draw_first_version(tc);
            assert!(f.major < u32::MAX && f.minor < u32::MAX && f.patch < u32::MAX);
        });
    }

    #[test]
    fn exec_response_check() {
        let v = Vars { from: "world".into(), to: "t-1:1".into(), amount: u64::MAX, asset: "COIN".into() };
        let cands = vec![Template::Transfer.postings(&v)];
        let resp = |ps: &[Posting]| json!({"logs": [{"sequence": "3", "payload": {"apply": {"log": {"data": {"createdTransaction": {"transaction": {"id": "7", "postings": ps.iter().map(Posting::to_json).collect::<Vec<_>>()}}}}}}}]});
        let c = check_exec_response(Some(&resp(&cands[0])), &cands);
        assert!(c.ok);
        assert_eq!(c.postings, Some(cands[0].clone()));
        assert_eq!(c.id, Some(7));
        assert!(c.volume_problems.is_empty(), "no postCommitVolumes is tolerated");
        let other = Template::Reversed.postings(&v);
        assert!(!check_exec_response(Some(&resp(&other)), &cands).ok);
        assert!(!check_exec_response(Some(&json!({})), &cands).ok, "no created transaction");
        assert!(check_exec_response(Some(&resp(&other)), &[cands[0].clone(), other.clone()]).ok, "any candidate");
        let req = exec_request("L", &Selector::Pinned("s-1".into(), Semver::new(1, 2, 3)), &v, "ref");
        assert_eq!(req["apply"]["action"]["createTransaction"]["scriptReference"], json!({"name": "s-1", "version": "1.2.3", "vars": {"from": "world", "to": "t-1:1", "amount": "COIN 18446744073709551615"}}));
        assert_eq!(exec_request("L", &Selector::Latest("transfer".into()), &v, "r")["apply"]["action"]["createTransaction"]["scriptReference"]["version"], "latest");
        assert_eq!(exec_request("L", &Selector::Inline, &v, "r")["apply"]["action"]["createTransaction"]["script"]["plain"], TRANSFER_V1);
    }

    #[test]
    fn ns_unresolved_sends_a_meta_read_that_never_resolves() {
        let (d, ft, _) = testkit::driver();
        setup(&d);
        testkit::reset();
        let unavailable = FakeReply::Outcome(Outcome::from_json(&json!({"ok": false, "code": "Unavailable", "reason": "PRELOAD_UNAVAILABLE", "message": "preload unavailable"})));
        ft.set_default("bucket", "Apply", unavailable);
        // Each keyed send retries the retryable refusal for its whole 3 s budget.
        for _ in 0..3 {
            testkit::run_rule(&d, ns_unresolved);
        }
        let calls = ft.calls_of("bucket", "Apply");
        let probes: Vec<&Value> = calls.iter().skip(4).collect();
        assert!(!probes.is_empty());
        for c in &probes {
            let ct = &c["request"]["unsigned"]["requests"][0]["apply"]["action"]["createTransaction"];
            let plain = ct["script"]["plain"].as_str().unwrap();
            assert!(plain.contains("account $dst = meta(@ns-cfg:") && plain.contains(", \"dest\")"), "{plain}");
            assert!(ct["reference"].as_str().unwrap().starts_with("r-test-nsu"));
        }
        assert!(testkit::fired("a write attempt hits a preload miss"));
        let rec: Vec<_> = testkit::assertions().into_iter().filter(|r| r.message == "a numscript whose meta() source is absent never commits").collect();
        assert!(!rec.is_empty() && rec.iter().all(|r| r.cond));

        // A commit is a finding.
        testkit::reset();
        ft.set_default("bucket", "Apply", ok(json!({"logs": []})));
        testkit::run_rule(&d, ns_unresolved);
        assert!(testkit::assertions().iter().any(|r| r.message == "a numscript whose meta() source is absent never commits" && !r.cond));
    }

    #[test]
    fn ns_exec_records_and_checks_transactions() {
        let (d, ft, _) = testkit::driver();
        setup(&d);
        testkit::reset();
        for _ in 0..20 {
            testkit::run_rule(&d, ns_exec);
        }
        let calls = ft.calls_of("bucket", "Apply");
        let execs: Vec<&Value> = calls.iter().skip(4).collect();
        assert!(!execs.is_empty());
        let mut refs = BTreeSet::new();
        for c in &execs {
            let ct = &c["request"]["unsigned"]["requests"][0]["apply"]["action"]["createTransaction"];
            assert!(refs.insert(ct["reference"].as_str().unwrap().to_string()), "unique references");
            let vars = if ct.get("script").is_some() { &ct["script"]["vars"] } else { &ct["scriptReference"]["vars"] };
            let amount = vars["amount"].as_str().unwrap();
            assert!(!amount.ends_with(" 0"), "{amount}");
        }
        // The fake answers `{}`: an ok Apply without a created transaction violates the exact-postings property.
        assert!(testkit::assertions().iter().any(|a| a.message == "a numscript transaction commits exactly the postings its script implies" && !a.cond));
        let st = d.s.ext::<NsState>();
        assert!(lock(&st).iter().flat_map(|l| l.txs.iter()).all(|t| t.status == Status::Acked));
    }

    #[test]
    fn ns_read_checks_against_acked_saves() {
        let (d, ft, _) = testkit::driver();
        setup(&d);
        ft.set_default("bucket", "GetNumscript", ok(json!({"name": "transfer", "version": "1.0.0", "content": TRANSFER_V1})));
        ft.set_default("bucket", "ListNumscriptVersions", ok(json!({"latestVersion": "1.0.0", "versions": [{"version": "1.0.0"}]})));
        ft.set_default("bucket", "ListNumscripts", FakeReply::Outcome(Outcome::from_json(&json!({"ok": true, "complete": true, "items": [{"name": "transfer", "version": "1.0.0", "content": TRANSFER_V1}]}))));
        testkit::reset();
        for _ in 0..30 {
            testkit::run_rule(&d, ns_read);
        }
        let a = testkit::assertions();
        assert!(!a.is_empty());
        for r in &a {
            assert!(r.cond, "{} {}", r.message, r.details);
        }
        // A stale latest pointer after an acked 2.0.0 is a violation.
        {
            let st = d.s.ext::<NsState>();
            let mut g = lock(&st);
            for l in g.iter_mut() {
                l.library.get_mut(TRANSFER).unwrap().versions.insert(V2, VersionRec { content: TRANSFER_V1.into(), template: Template::Transfer, status: Status::Acked });
            }
        }
        let w = ReadWindow { acked_before: [V1, V2].into_iter().collect(), possible_after: [V1, V2].into_iter().collect() };
        testkit::reset();
        eval_versions("L", TRANSFER, &w, &Outcome::from_json(&json!({"ok": true, "response": {"latestVersion": "1.0.0", "versions": [{"version": "1.0.0"}, {"version": "2.0.0"}]}})));
        assert!(testkit::assertions().iter().any(|a| a.message == "latest pointer should be the greatest saved semver" && !a.cond));
        testkit::reset();
        let st = d.s.ext::<NsState>();
        eval_get(&st, 0, "L", TRANSFER, "latest", &w, &Outcome::from_json(&json!({"ok": false, "code": "NotFound", "reason": "NUMSCRIPT_NOT_FOUND"})));
        assert!(testkit::assertions().iter().any(|a| a.message == "a numscript read returns a saved version and its content" && !a.cond));
        testkit::reset();
        eval_get(&st, 0, "L", TRANSFER, "latest", &w, &Outcome::unknown("timeout"));
        assert!(testkit::assertions().is_empty(), "a transient outcome checks nothing");
    }

    #[test]
    fn quiet_hook_resolves_and_checks_with_fakes() {
        let (d, ft, _) = testkit::driver();
        setup(&d);
        let st = d.s.ext::<NsState>();
        let v = Vars { from: "world".into(), to: "t-0:1".into(), amount: 10, asset: "COIN".into() };
        let postings = Template::Transfer.postings(&v);
        {
            let mut g = lock(&st);
            g[1].ready = false; // the fake serves one listing for every ledger
            let mk = |r: &str, status| TxRec { reference: r.into(), key: "k".into(), selector: Selector::Inline, vars: v.clone(), candidates: vec![postings.clone()], floor: None, status, tx_id: None, committed: if status == Status::Acked { Some(postings.clone()) } else { None } };
            g[0].record_tx(mk("a", Status::Acked));
            g[0].record_tx(mk("b", Status::Unknown));
            g[0].record_tx(mk("c", Status::Rejected));
        }
        let tx = |id: &str, r: &str| json!({"id": id, "reference": r, "postings": postings.iter().map(Posting::to_json).collect::<Vec<_>>()});
        ft.set_default("bucket", "ListTransactions", FakeReply::Outcome(Outcome::from_json(&json!({"ok": true, "complete": true, "items": [tx("1", "b"), tx("0", "a")]}))));
        let big = |m: &str| if m.is_empty() { json!({}) } else { json!({"magnitude": m}) };
        let vol = |i: &str, o: &str| json!({"asset": "COIN", "volumes": {"input": big(i), "output": big(o), "balance": if o.is_empty() { json!({"magnitude": big(i)}) } else { json!({"negative": true, "magnitude": big(o)}) }}});
        // two committed transactions of 10: 20 = base64 "FA=="
        ft.set_default("bucket", "ListAccounts", FakeReply::Outcome(Outcome::from_json(&json!({"ok": true, "complete": true, "items": [{"address": "world", "volumes": [vol("", "FA==")]}, {"address": "t-0:1", "volumes": [vol("FA==", "")]}]}))));
        ft.set_default("bucket", "GetLedgerStats", ok(json!({"numscriptExecutionCount": "2"})));
        testkit::reset();
        let hook = &quiet_hooks()[0];
        crate::quiet::run_quiet(&d, Some(hook), Some(Box::new(false)));
        let a = testkit::assertions();
        for m in [
            "an acknowledged numscript transaction survives faults",
            "a definitively rejected numscript transaction never takes effect",
            "a numscript ledger's balances equal its committed postings",
            "numscript execution count caught up",
            "numscript_state completes within its quiet budget",
            "a numscript transaction commits exactly the postings its script implies",
        ] {
            assert!(a.iter().any(|r| r.message == m && r.cond), "{m}: {:?}", a.iter().filter(|r| r.message == m).collect::<Vec<_>>());
        }
        assert_eq!(lock(&st)[0].txs[1].status, Status::Acked, "the unknown record resolved as committed");
        // Now the listing loses the acked transaction.
        ft.set_default("bucket", "ListTransactions", FakeReply::Outcome(Outcome::from_json(&json!({"ok": true, "complete": true, "items": [tx("1", "b"), tx("2", "c")]}))));
        testkit::reset();
        crate::quiet::run_quiet(&d, Some(hook), Some(Box::new(true)));
        let a = testkit::assertions();
        assert!(a.iter().any(|r| r.message == "an acknowledged numscript transaction survives faults" && !r.cond));
        assert!(a.iter().any(|r| r.message == "a definitively rejected numscript transaction never takes effect" && !r.cond));
    }

    #[test]
    fn quiet_hook_flags_wrong_resolved_postings_and_rereads_on_mismatch() {
        let (d, ft, _) = testkit::driver();
        setup(&d);
        let st = d.s.ext::<NsState>();
        let v = Vars { from: "world".into(), to: "t-0:1".into(), amount: 10, asset: "COIN".into() };
        let postings = Template::Transfer.postings(&v);
        {
            let mut g = lock(&st);
            g[1].ready = false;
            g[0].record_tx(TxRec { reference: "u".into(), key: "k".into(), selector: Selector::Inline, vars: v.clone(), candidates: vec![postings.clone()], floor: None, status: Status::Unknown, tx_id: None, committed: None });
            g[0].record_tx(TxRec { reference: "late".into(), key: "k".into(), selector: Selector::Inline, vars: v.clone(), candidates: vec![postings.clone()], floor: None, status: Status::Unknown, tx_id: None, committed: None });
        }
        let tx = |id: &str, r: &str, ps: &[Posting]| json!({"id": id, "reference": r, "postings": ps.iter().map(Posting::to_json).collect::<Vec<_>>()});
        let wrong = Template::Reversed.postings(&v);
        let list = |items: Vec<Value>| FakeReply::Outcome(Outcome::from_json(&json!({"ok": true, "complete": true, "items": items})));
        // First listing: "u" committed with the reversed postings, "late" not yet.
        // Accounts already reflect both (a late commit between the listings);
        // the re-read lists "late" too, so balances agree on the second read.
        ft.script("bucket", "ListTransactions", vec![list(vec![tx("0", "u", &wrong)]), list(vec![tx("0", "u", &wrong), tx("1", "late", &postings)])]);
        let big = |m: &str| if m.is_empty() { json!({}) } else { json!({"magnitude": m}) };
        // world: in 10 (u reversed) out 10 (late); t-0:1: in 10 out 10. 10 = "Cg=="
        let vol = json!({"asset": "COIN", "volumes": {"input": big("Cg=="), "output": big("Cg=="), "balance": {}}});
        ft.set_default("bucket", "ListAccounts", list(vec![json!({"address": "world", "volumes": [vol.clone()]}), json!({"address": "t-0:1", "volumes": [vol]})]));
        ft.set_default("bucket", "GetLedgerStats", ok(json!({"numscriptExecutionCount": "2"})));
        testkit::reset();
        crate::quiet::run_quiet(&d, Some(&quiet_hooks()[0]), Some(Box::new(false)));
        let a = testkit::assertions();
        let has = |m: &str, c: bool| a.iter().any(|r| r.message == m && r.cond == c);
        assert!(has("a numscript transaction commits exactly the postings its script implies", false), "the resolved record's postings are none of its candidates");
        assert!(has("a numscript ledger's balances equal its committed postings", true), "the re-read reconciles the late commit");
        assert!(!has("a numscript ledger's balances equal its committed postings", false));
        assert!(has("numscript execution count caught up", true));
        assert_eq!(ft.calls_of("bucket", "ListTransactions").len(), 2);
    }

    #[test]
    fn count_caught_up_is_evaluated_with_unresolved_records() {
        let (d, ft, _) = testkit::driver();
        setup(&d);
        let st = d.s.ext::<NsState>();
        let v = Vars { from: "world".into(), to: "t-0:1".into(), amount: 10, asset: "COIN".into() };
        {
            let mut g = lock(&st);
            g[1].ready = false;
            g[0].record_tx(TxRec { reference: "lost".into(), key: "k".into(), selector: Selector::Inline, vars: v.clone(), candidates: vec![Template::Transfer.postings(&v)], floor: None, status: Status::Unknown, tx_id: None, committed: None });
        }
        let empty = FakeReply::Outcome(Outcome::from_json(&json!({"ok": true, "complete": true, "items": []})));
        ft.set_default("bucket", "ListTransactions", empty.clone());
        ft.set_default("bucket", "ListAccounts", empty);
        ft.set_default("bucket", "GetLedgerStats", ok(json!({"numscriptExecutionCount": "0"})));
        testkit::reset();
        crate::quiet::run_quiet(&d, Some(&quiet_hooks()[0]), Some(Box::new(false)));
        assert!(testkit::assertions().iter().any(|r| r.message == "numscript execution count caught up" && r.cond));
    }

    #[test]
    fn post_commit_volumes_are_checked() {
        let v = Vars { from: "t-0:1".into(), to: "t-0:2".into(), amount: 10, asset: "COIN".into() };
        let ps = Template::ViaHub.postings(&v);
        let b = |m: &str| if m.is_empty() { json!({}) } else { json!({"magnitude": m}) };
        let e = |asset: &str, i: &str, o: &str| json!({"asset": asset, "volumes": {"input": b(i), "output": b(o)}});
        // 10 = "Cg==", 9 = "CQ==", 30 = "Hg=="
        let tx = |pcv: Value| json!({"postings": ps.iter().map(Posting::to_json).collect::<Vec<_>>(), "postCommitVolumes": pcv});
        let good = json!({"volumesByAccount": {"t-0:1": {"volumes": [e("COIN", "", "Cg==")]}, "ns:hub": {"volumes": [e("COIN", "Hg==", "Hg==")]}, "t-0:2": {"volumes": [e("COIN", "Cg==", "")]}}});
        assert!(post_commit_volume_problems(&tx(good.clone()), &ps).is_empty(), "{:?}", post_commit_volume_problems(&tx(good), &ps));
        let missing = json!({"volumesByAccount": {"t-0:1": {"volumes": [e("COIN", "", "Cg==")]}, "ns:hub": {"volumes": [e("COIN", "Hg==", "Hg==")]}}});
        assert_eq!(post_commit_volume_problems(&tx(missing), &ps).len(), 1, "t-0:2 has no entry");
        let low = json!({"volumesByAccount": {"t-0:1": {"volumes": [e("COIN", "", "CQ==")]}, "ns:hub": {"volumes": [e("COIN", "Hg==", "Hg==")]}, "t-0:2": {"volumes": [e("COIN", "Cg==", "")]}}});
        assert_eq!(post_commit_volume_problems(&tx(low), &ps).len(), 1, "output 9 below the 10 sent");
        let no_out = json!({"volumesByAccount": {"t-0:1": {"volumes": [{"asset": "COIN", "volumes": {"input": {}}}]}, "ns:hub": {"volumes": [e("COIN", "Hg==", "Hg==")]}, "t-0:2": {"volumes": [e("COIN", "Cg==", "")]}}});
        assert_eq!(post_commit_volume_problems(&tx(no_out), &ps).len(), 1, "output field missing");
        let bad_b64 = json!({"volumesByAccount": {"t-0:1": {"volumes": [e("COIN", "", "!!")]}, "ns:hub": {"volumes": [e("COIN", "Hg==", "Hg==")]}, "t-0:2": {"volumes": [e("COIN", "Cg==", "")]}}});
        assert_eq!(post_commit_volume_problems(&tx(bad_b64), &ps).len(), 1, "undecodable output");
        let colored = json!({"volumesByAccount": {"t-0:1": {"volumes": [{"asset": "COIN", "color": "red", "volumes": {"input": {}, "output": b("Cg==")}}]}, "ns:hub": {"volumes": [e("COIN", "Hg==", "Hg==")]}, "t-0:2": {"volumes": [e("COIN", "Cg==", "")]}}});
        assert_eq!(post_commit_volume_problems(&tx(colored), &ps).len(), 1, "only a colored entry");
        assert!(post_commit_volume_problems(&json!({"postings": []}), &ps).is_empty(), "absent postCommitVolumes is tolerated");
    }

    #[test]
    fn read_checks_cover_bare_not_found_pointer_consistency_and_resave_reasons() {
        let (d, _, _) = testkit::driver();
        setup(&d);
        let st = d.s.ext::<NsState>();
        let w = ReadWindow { acked_before: [V1].into_iter().collect(), possible_after: [V1, V2].into_iter().collect() };
        let fails = |m: &str| testkit::assertions().iter().any(|a| a.message == m && !a.cond);
        // GetNumscript answers a missing script with a bare NotFound.
        testkit::reset();
        eval_get(&st, 0, "L", TRANSFER, "1.0.0", &w, &Outcome::from_json(&json!({"ok": false, "code": "NotFound", "message": "numscript \"transfer\" not found"})));
        assert!(fails("a numscript read returns a saved version and its content"));
        testkit::reset();
        eval_get(&st, 0, "L", TRANSFER, "1.0.0", &w, &Outcome::from_json(&json!({"ok": false, "code": "NotFound", "local": true})));
        assert!(testkit::assertions().is_empty(), "a client-local error checks nothing");
        testkit::reset();
        eval_get(&st, 0, "L", TRANSFER, "1.0.0", &w, &Outcome::from_json(&json!({"ok": false, "code": "Unavailable"})));
        assert!(testkit::assertions().is_empty());
        // ListNumscriptVersions: the pointer must be the greatest listed version.
        testkit::reset();
        eval_versions("L", TRANSFER, &w, &Outcome::from_json(&json!({"ok": true, "response": {"latestVersion": "1.0.0", "versions": [{"version": "2.0.0"}, {"version": "1.0.0"}]}})));
        assert!(fails("latest pointer should be the greatest saved semver"), "2.0.0 is listed above the pointer");
        testkit::reset();
        eval_versions("L", TRANSFER, &w, &Outcome::from_json(&json!({"ok": true, "response": {"latestVersion": "2.0.0", "versions": [{"version": "2.0.0"}, {"version": "1.0.0"}]}})));
        assert!(testkit::assertions().iter().all(|a| a.cond), "{:?}", testkit::assertions());
        testkit::reset();
        eval_versions("L", TRANSFER, &w, &Outcome::from_json(&json!({"ok": true, "response": {"latestVersion": "", "versions": []}})));
        assert!(fails("latest pointer should be the greatest saved semver"));
        // Re-save refusals.
        let refusal = |r: &str, c: &str| Outcome::from_json(&json!({"ok": false, "code": c, "reason": r}));
        assert!(resave_refusal_ok(&refusal("NUMSCRIPT_VERSION_ALREADY_EXISTS", "AlreadyExists")));
        assert!(!resave_refusal_ok(&refusal("NUMSCRIPT_PARSE_ERROR", "InvalidArgument")));
        assert!(!resave_refusal_ok(&refusal("NUMSCRIPT_VERSION_ALREADY_EXISTS", "InvalidArgument")));
        assert!(resave_refusal_ok(&Outcome::unknown("timeout")));
    }

    #[test]
    fn quiet_hook_reports_incomplete_runs() {
        let (d, ft, _) = testkit::driver();
        setup(&d);
        ft.set_default("bucket", "ListTransactions", FakeReply::Outcome(Outcome::unknown("timeout")));
        testkit::reset();
        crate::quiet::run_quiet(&d, Some(&quiet_hooks()[0]), Some(Box::new(false)));
        let a = testkit::assertions();
        assert!(a.iter().any(|r| r.message == "numscript_state completes within its quiet budget" && !r.cond));
        assert!(!a.iter().any(|r| r.message == "an acknowledged numscript transaction survives faults"), "no Always on a partial check");
    }

    #[test]
    fn registrations() {
        let names: Vec<String> = rules().into_iter().map(|r| r.name).collect();
        assert_eq!(names, vec!["ns_save", "ns_read", "ns_exec", "ns_unresolved"]);
        let h = &quiet_hooks()[0];
        assert_eq!((h.name, h.budget, h.needs_oracle), ("numscript_state", Duration::from_secs(30), false));
        assert!(crate::rules::quiet_hooks().iter().any(|h| h.name == "numscript_state"));
        assert!(crate::rules::setup_hooks().iter().any(|h| h.name == "numscript_setup"));
    }
}
