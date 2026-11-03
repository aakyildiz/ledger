//! The Rust model of the `r-ns-*` ledgers (task 13; DESIGN §3, critic Q7):
//! the Numscript library per ledger (`name → version → {content, status}`),
//! the script transactions by unique reference (a port of the retired
//! `H/model.rs` `TxRec` without metadata histories), the overdraft-able
//! accounts, the script texts, and the pure checks the rules and the quiet
//! hook evaluate.
use super::semver::Semver;
use crate::wire::Posting;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::atomic::AtomicU64;
use std::sync::Mutex;

/// The status machinery of `H/model.rs`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Status {
    /// Sent, no answer yet.
    Pending,
    /// The system acknowledged it.
    Acked,
    /// Definitively rejected (keyed write, definitive reason): never takes effect.
    Rejected,
    /// No definitive answer: it may or may not have taken effect.
    Unknown,
}

impl Status {
    /// Whether the write may have taken effect.
    pub fn possible(self) -> bool {
        self != Status::Rejected
    }
}

// ------------------------------------------------------------- scripts

/// The `transfer` script of the old drivers (`MD/parallel_driver_numscripts`,
/// `MD/parallel_driver_numscript_lifecycle`, `MD/parallel_driver_transactions`),
/// saved as version `1.0.0` on every `r-ns-*` ledger at setup.
pub const TRANSFER_V1: &str = "
	vars {
		account $from
		account $to
		monetary $amount
	}
	send $amount (
		source = $from allowing unbounded overdraft
		destination = $to
	)
";

/// The script lines of `transfer`, one token list per line, with the
/// nesting level used for indentation.
const TRANSFER_LINES: [(u8, &[&str]); 9] = [
    (0, &["vars", "{"]),
    (1, &["account", "$from"]),
    (1, &["account", "$to"]),
    (1, &["monetary", "$amount"]),
    (0, &["}"]),
    (0, &["send", "$amount", "("]),
    (1, &["source", "=", "$from", "allowing", "unbounded", "overdraft"]),
    (1, &["destination", "=", "$to"]),
    (0, &[")"]),
];

/// A drawn whitespace/comment variant of `transfer`. Every rendering is
/// semantically identical to `TRANSFER_V1` (DESIGN Appendix A C17: the
/// `latest` selector resolves at apply time, `processor_transaction.go:47-70`).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Layout {
    /// Blank lines before the script.
    pub leading: u8,
    /// Spaces per nesting level (0 = flush left), or a tab when `tabs`.
    pub indent: u8,
    pub tabs: bool,
    /// Spaces between tokens of one line (≥ 1).
    pub gap: u8,
    /// `\r\n` instead of `\n`.
    pub crlf: bool,
    /// Comments: (line index before which it goes, block instead of line comment, tag).
    pub comments: Vec<(u8, bool, u32)>,
    /// Blank lines after the script.
    pub trailing: u8,
}

impl Layout {
    pub fn render(&self) -> String {
        let nl = if self.crlf { "\r\n" } else { "\n" };
        let gap = " ".repeat(self.gap.max(1) as usize);
        let mut out = String::new();
        for _ in 0..self.leading {
            out.push_str(nl);
        }
        for (i, (level, tokens)) in TRANSFER_LINES.iter().enumerate() {
            let pad = if self.tabs { "\t".repeat(*level as usize) } else { " ".repeat(*level as usize * self.indent as usize) };
            for (_, block, tag) in self.comments.iter().filter(|(at, _, _)| *at as usize == i) {
                if *block {
                    out.push_str(&format!("{pad}/* note {tag} */{nl}"));
                } else {
                    out.push_str(&format!("{pad}// note {tag}{nl}"));
                }
            }
            out.push_str(&pad);
            out.push_str(&tokens.join(&gap));
            out.push_str(nl);
        }
        for (_, block, tag) in self.comments.iter().filter(|(at, _, _)| *at as usize >= TRANSFER_LINES.len()) {
            if *block {
                out.push_str(&format!("/* tail {tag} */{nl}"));
            } else {
                out.push_str(&format!("// tail {tag}{nl}"));
            }
        }
        for _ in 0..self.trailing {
            out.push_str(nl);
        }
        out
    }
}

/// The script's tokens with comments and whitespace removed (Numscript's
/// lexer skips `WS`, `/* */` and `//…NEWLINE`, `numscript@v0.1.2/Lexer.g4:2-5`).
#[cfg(test)]
pub fn normalize(script: &str) -> Vec<String> {
    let mut s = String::with_capacity(script.len());
    let b = script.as_bytes();
    let mut i = 0;
    while i < b.len() {
        if b[i..].starts_with(b"/*") {
            match script[i + 2..].find("*/") {
                Some(e) => i = i + 2 + e + 2,
                None => i = b.len(),
            }
            s.push(' ');
        } else if b[i..].starts_with(b"//") {
            match script[i..].find('\n') {
                Some(e) => i += e + 1,
                None => i = b.len(),
            }
            s.push(' ');
        } else {
            s.push(b[i] as char);
            i += 1;
        }
    }
    s.split_whitespace().map(str::to_string).collect()
}

/// The script templates. `transfer` is always `Transfer`; `s-<N>` scripts
/// are drawn among all three, so `latest` on them can resolve to different
/// postings (the exec rule accepts any version the selector could resolve to).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Template {
    /// `send $amount (source = $from allowing unbounded overdraft destination = $to)`.
    Transfer,
    /// The same send in the other direction.
    Reversed,
    /// Two sends through `ns:hub` (both sources allow unbounded overdraft).
    ViaHub,
}

pub const HUB: &str = "ns:hub";
pub const TEMPLATES: [Template; 3] = [Template::Transfer, Template::Reversed, Template::ViaHub];

impl Template {
    /// The fixed text of an `s-<N>` template (with an optional tag comment
    /// so two saves of one template still differ).
    pub fn text(self, tag: Option<u32>) -> String {
        let body = match self {
            Template::Transfer => TRANSFER_V1.to_string(),
            Template::Reversed => "vars {\n  account $from\n  account $to\n  monetary $amount\n}\nsend $amount (\n  source = $to allowing unbounded overdraft\n  destination = $from\n)\n".to_string(),
            Template::ViaHub => format!(
                "vars {{\n  account $from\n  account $to\n  monetary $amount\n}}\nsend $amount (\n  source = $from allowing unbounded overdraft\n  destination = @{HUB}\n)\nsend $amount (\n  source = @{HUB} allowing unbounded overdraft\n  destination = $to\n)\n"
            ),
        };
        match tag {
            Some(t) => format!("// template {t}\n{body}"),
            None => body,
        }
    }

    /// The postings the script implies for these vars (all sources allow
    /// unbounded overdraft, so the result is unconditional).
    pub fn postings(self, v: &Vars) -> Vec<Posting> {
        let p = |s: &str, d: &str| Posting::new(s, d, v.amount, &v.asset);
        match self {
            Template::Transfer => vec![p(&v.from, &v.to)],
            Template::Reversed => vec![p(&v.to, &v.from)],
            Template::ViaHub => vec![p(&v.from, HUB), p(HUB, &v.to)],
        }
    }

    /// Accounts the script may overdraw.
    pub fn overdraft_sources(self, v: &Vars) -> Vec<String> {
        match self {
            Template::Transfer => vec![v.from.clone()],
            Template::Reversed => vec![v.to.clone()],
            Template::ViaHub => vec![v.from.clone(), HUB.to_string()],
        }
    }
}

/// The variables of one script transaction.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Vars {
    pub from: String,
    pub to: String,
    pub amount: u64,
    /// `COIN` or `USD/2`.
    pub asset: String,
}

impl Vars {
    /// `{"from", "to", "amount": "<ASSET> <n>"}` (Numscript monetary syntax).
    pub fn to_json(&self) -> serde_json::Value {
        serde_json::json!({"from": self.from, "to": self.to, "amount": format!("{} {}", self.asset, self.amount)})
    }
}

// ------------------------------------------------------------- library

#[derive(Clone, Debug)]
pub struct VersionRec {
    pub content: String,
    pub template: Template,
    pub status: Status,
}

/// One library name: every version ever sent, with its status.
#[derive(Clone, Debug, Default)]
pub struct ScriptRec {
    pub versions: BTreeMap<Semver, VersionRec>,
}

impl ScriptRec {
    pub fn acked(&self) -> Vec<Semver> {
        self.versions.iter().filter(|(_, r)| r.status == Status::Acked).map(|(v, _)| *v).collect()
    }
    /// Every version that may be stored (not definitively rejected).
    pub fn possible(&self) -> Vec<Semver> {
        self.versions.iter().filter(|(_, r)| r.status.possible()).map(|(v, _)| *v).collect()
    }
    pub fn max_acked(&self) -> Option<Semver> {
        self.acked().into_iter().max()
    }
    /// The greatest version ever sent, whatever its status (new versions are
    /// allocated above it, so two saves never race on one version).
    pub fn max_any(&self) -> Option<Semver> {
        self.versions.keys().next_back().copied()
    }
}

/// What a linearizable library read may return for one name: versions acked
/// before the read was sent are certainly stored; versions not
/// definitively rejected by the time it ended may be stored.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ReadWindow {
    pub acked_before: BTreeSet<Semver>,
    pub possible_after: BTreeSet<Semver>,
}

impl ReadWindow {
    /// A version the selector may legally resolve to: stored possibly,
    /// matching the selector, and not below the greatest matching version
    /// acked before the read.
    pub fn legal_resolution(&self, selector: &str, got: Semver) -> bool {
        let floor = self.acked_before.iter().filter(|v| v.matches(selector)).max();
        got.matches(selector) && self.possible_after.contains(&got) && floor.is_none_or(|f| got >= *f)
    }

    /// The `latest` pointer of ListNumscriptVersions (DESIGN task 13:
    /// `latest ∈ max(acked ∪ unknown)` and `≥ max acked`): it is the
    /// greatest stored version, so it must be possible, at least the greatest
    /// acked-before version, and no possible version can be stored above it
    /// … except one still in flight, which is why only the floor is checked
    /// against acked versions.
    pub fn legal_latest(&self, got: Semver) -> bool {
        self.legal_resolution("latest", got)
    }

    /// A version listing: contains every acked-before version, and only
    /// possible versions.
    pub fn legal_listing(&self, listed: &BTreeSet<Semver>) -> bool {
        self.acked_before.is_subset(listed) && listed.is_subset(&self.possible_after)
    }
}

// ------------------------------------------------------------- transactions

/// How a transaction names its script.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Selector {
    Inline,
    Latest(String),
    Pinned(String, Semver),
}

/// One script transaction (unique reference), `H/model.rs` `TxRec` without
/// metadata histories.
#[derive(Clone, Debug)]
#[allow(dead_code)] // `key` kept for Debug output in assertion details
pub struct TxRec {
    pub reference: String,
    pub key: String,
    pub selector: Selector,
    pub vars: Vars,
    /// Every postings list the script could produce (one per template the
    /// selector could resolve to).
    pub candidates: Vec<Vec<Posting>>,
    pub status: Status,
    pub tx_id: Option<u64>,
    /// The postings the system reported (ack or quiet listing).
    pub committed: Option<Vec<Posting>>,
}

/// One `r-ns-*` ledger.
#[derive(Clone, Debug, Default)]
pub struct NsLedger {
    pub name: String,
    /// Created and `transfer@1.0.0` saved at setup.
    pub ready: bool,
    pub library: BTreeMap<String, ScriptRec>,
    pub txs: Vec<TxRec>,
    pub by_ref: HashMap<String, usize>,
    /// Sources of `allowing unbounded overdraft` plus `world`.
    pub overdraft: BTreeSet<String>,
    /// Counter for `s-<N>` names (identities, never draws).
    pub next_script: u64,
}

/// At most this many transactions per ledger (taper; keeps the quiet
/// listing inside its 30 s budget).
pub const TX_CAP: usize = 4000;
/// At most this many `s-<N>` names per ledger.
pub const SCRIPT_CAP: u64 = 16;
/// At most this many versions per name.
pub const VERSION_CAP: usize = 24;

impl NsLedger {
    pub fn new(name: &str) -> Self {
        NsLedger { name: name.to_string(), overdraft: ["world".to_string()].into_iter().collect(), ..Default::default() }
    }

    /// The read window of one name at this moment (`acked_before` when
    /// taken before the send, `possible_after` when taken after the answer).
    pub fn acked_versions(&self, name: &str) -> BTreeSet<Semver> {
        self.library.get(name).map(|s| s.acked().into_iter().collect()).unwrap_or_default()
    }
    pub fn possible_versions(&self, name: &str) -> BTreeSet<Semver> {
        self.library.get(name).map(|s| s.possible().into_iter().collect()).unwrap_or_default()
    }

    /// Names with at least one acked version, sorted.
    pub fn acked_names(&self) -> Vec<String> {
        self.library.iter().filter(|(_, s)| s.max_acked().is_some()).map(|(n, _)| n.clone()).collect()
    }

    /// The candidate postings of a transaction with this selector: inline
    /// and `transfer` resolve to `Transfer`; a pinned version to its
    /// template; `latest` to the template of every possible version at or
    /// above the greatest acked one (deduplicated, in version order).
    pub fn candidates(&self, sel: &Selector, vars: &Vars) -> Vec<Vec<Posting>> {
        self.candidates_since(sel, vars, self.floor(sel))
    }

    /// The greatest acked version of a `latest` selector's name (None for
    /// other selectors or without an acked version).
    pub fn floor(&self, sel: &Selector) -> Option<Semver> {
        match sel {
            Selector::Latest(name) => self.library.get(name).and_then(ScriptRec::max_acked),
            _ => None,
        }
    }

    /// `candidates` with the `latest` floor taken when the transaction was
    /// sent: evaluated after the answer, it also covers versions whose save
    /// started while the transaction was in flight (their content may be the
    /// greatest stored version when the FSM resolves `latest`).
    pub fn candidates_since(&self, sel: &Selector, vars: &Vars, floor: Option<Semver>) -> Vec<Vec<Posting>> {
        let templates: Vec<Template> = match sel {
            Selector::Inline => vec![Template::Transfer],
            Selector::Pinned(name, v) => self.library.get(name).and_then(|s| s.versions.get(v)).map(|r| vec![r.template]).unwrap_or_default(),
            Selector::Latest(name) => {
                let Some(s) = self.library.get(name) else { return vec![] };
                let mut t = Vec::new();
                for (v, r) in &s.versions {
                    if r.status.possible() && floor.is_none_or(|f| *v >= f) && !t.contains(&r.template) {
                        t.push(r.template);
                    }
                }
                t
            }
        };
        templates.into_iter().map(|t| t.postings(vars)).collect()
    }

    pub fn template_of(&self, sel: &Selector) -> Vec<Template> {
        match sel {
            Selector::Inline => vec![Template::Transfer],
            Selector::Pinned(name, v) => self.library.get(name).and_then(|s| s.versions.get(v)).map(|r| vec![r.template]).unwrap_or_default(),
            Selector::Latest(name) => self.library.get(name).map(|s| s.versions.values().map(|r| r.template).collect()).unwrap_or_default(),
        }
    }

    pub fn record_tx(&mut self, rec: TxRec) -> usize {
        for t in self.template_of(&rec.selector) {
            self.overdraft.extend(t.overdraft_sources(&rec.vars));
        }
        let i = self.txs.len();
        self.by_ref.insert(rec.reference.clone(), i);
        self.txs.push(rec);
        i
    }
}

/// The extension state of the family (`Shared::ext::<NsState>()`).
#[derive(Default)]
pub struct NsState {
    pub ledgers: Mutex<Vec<NsLedger>>,
    /// Counter for transaction references and comment tags.
    pub next_ref: AtomicU64,
}

// ------------------------------------------------------------- quiet evaluation

/// A transaction as ListTransactions reports it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ListedTx {
    pub id: u64,
    pub reference: String,
    pub postings: Vec<Posting>,
}

/// The verdicts of one fully listed ledger (the quiet hook asserts them).
#[derive(Clone, Debug, Default)]
pub struct Evaluation {
    /// Acked references missing from the listing or listed with other postings.
    pub acked_lost: Vec<String>,
    pub acked_checked: usize,
    /// Rejected references found in the listing.
    pub rejected_listed: Vec<String>,
    pub rejected_checked: usize,
    /// Unknown/pending records found committed in the listing.
    pub resolved_committed: usize,
    /// Unknown/pending records not in the listing (stay unknown).
    pub unresolved: usize,
    /// Listed transactions whose reference the model never sent.
    pub foreign: Vec<u64>,
    /// Committed script transactions (acked + resolved).
    pub committed_scripts: u64,
    /// Expected `(address, asset) → (input, output)` from committed postings.
    pub expected: BTreeMap<(String, String), (u128, u128)>,
}

/// Resolves Unknown/Pending records against a complete listing (reference →
/// committed?), then evaluates the survival, rejection and balance
/// expectations. Resolution mutates the model: a listed Unknown becomes Acked
/// with the listed postings.
pub fn evaluate(l: &mut NsLedger, listed: &[ListedTx]) -> Evaluation {
    let by_ref: HashMap<&str, &ListedTx> = listed.iter().map(|t| (t.reference.as_str(), t)).collect();
    let mut e = Evaluation::default();
    for rec in l.txs.iter_mut() {
        let hit = by_ref.get(rec.reference.as_str()).copied();
        match rec.status {
            Status::Acked => {
                e.acked_checked += 1;
                let same = hit.is_some_and(|t| rec.committed.as_ref().is_none_or(|c| *c == t.postings));
                if !same {
                    e.acked_lost.push(rec.reference.clone());
                }
            }
            Status::Rejected => {
                e.rejected_checked += 1;
                if hit.is_some() {
                    e.rejected_listed.push(rec.reference.clone());
                }
            }
            Status::Unknown | Status::Pending => match hit {
                Some(t) => {
                    rec.status = Status::Acked;
                    rec.tx_id = Some(t.id);
                    rec.committed = Some(t.postings.clone());
                    e.resolved_committed += 1;
                }
                None => e.unresolved += 1,
            },
        }
    }
    let known: BTreeSet<&str> = l.txs.iter().map(|r| r.reference.as_str()).collect();
    e.foreign = listed.iter().filter(|t| !known.contains(t.reference.as_str())).map(|t| t.id).collect();
    for rec in l.txs.iter().filter(|r| r.status == Status::Acked) {
        e.committed_scripts += 1;
        let postings = rec.committed.clone().or_else(|| rec.candidates.first().cloned()).unwrap_or_default();
        add_postings(&mut e.expected, &postings);
    }
    e
}

pub fn add_postings(m: &mut BTreeMap<(String, String), (u128, u128)>, postings: &[Posting]) {
    for p in postings {
        let a = p.amount.to_u64().map(u128::from).unwrap_or(u128::MAX / 4);
        m.entry((p.source.clone(), p.asset.clone())).or_default().1 += a;
        m.entry((p.destination.clone(), p.asset.clone())).or_default().0 += a;
    }
}

/// An account volume as ListAccounts reports it (color `""` only: the
/// workload never colors).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ListedVolume {
    pub address: String,
    pub asset: String,
    pub color: String,
    pub input: i128,
    pub output: i128,
    pub balance: i128,
}

/// The mismatches between expected volumes and the listing: every expected
/// `(address, asset)` must be listed with exactly its input and output (and
/// a balance equal to their difference); a listed non-zero volume the model
/// does not expect is a mismatch too.
pub fn compare_volumes(expected: &BTreeMap<(String, String), (u128, u128)>, listed: &[ListedVolume]) -> Vec<String> {
    let mut out = Vec::new();
    let mut seen: BTreeMap<(String, String), (i128, i128, i128)> = BTreeMap::new();
    for v in listed {
        if !v.color.is_empty() {
            out.push(format!("{} {} has unexpected color {:?}", v.address, v.asset, v.color));
            continue;
        }
        seen.insert((v.address.clone(), v.asset.clone()), (v.input, v.output, v.balance));
    }
    for ((addr, asset), (i, o)) in expected {
        if *i == 0 && *o == 0 {
            continue;
        }
        let (ei, eo) = (*i as i128, *o as i128);
        match seen.get(&(addr.clone(), asset.clone())) {
            Some((gi, go, gb)) if *gi == ei && *go == eo && *gb == ei - eo => {}
            Some((gi, go, gb)) => out.push(format!("{addr} {asset}: expected in {ei} out {eo}, got in {gi} out {go} balance {gb}")),
            None => out.push(format!("{addr} {asset}: expected in {ei} out {eo}, not listed")),
        }
    }
    for ((addr, asset), (gi, go, _)) in &seen {
        if (*gi != 0 || *go != 0) && !expected.contains_key(&(addr.clone(), asset.clone())) {
            out.push(format!("{addr} {asset}: listed in {gi} out {go}, never posted"));
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn vars(from: &str, to: &str, amount: u64) -> Vars {
        Vars { from: from.into(), to: to.into(), amount, asset: "COIN".into() }
    }

    #[test]
    fn every_layout_normalises_to_the_v1_script() {
        let canon = normalize(TRANSFER_V1);
        assert_eq!(canon.first().map(String::as_str), Some("vars"));
        assert_eq!(normalize(&Layout::default().render()), canon);
        let l = Layout { leading: 2, indent: 0, tabs: false, gap: 3, crlf: true, comments: vec![(0, false, 1), (4, true, 2), (9, false, 3), (9, true, 4)], trailing: 3 };
        let r = l.render();
        assert_ne!(r, TRANSFER_V1);
        assert!(r.contains("// note 1\r\n"));
        assert_eq!(normalize(&r), canon);
        assert_eq!(normalize(&Layout { tabs: true, indent: 4, ..Default::default() }.render()), canon);
        // the comment stripper itself
        assert_eq!(normalize("a /* x */ b // y\nc"), vec!["a", "b", "c"]);
        assert_eq!(normalize(&Template::Transfer.text(Some(7))), canon);
        assert_ne!(normalize(&Template::Reversed.text(None)), canon);
    }

    #[test]
    fn expected_postings_builder() {
        let v = vars("t-1:2", "t-3:4", 9);
        assert_eq!(Template::Transfer.postings(&v), vec![Posting::new("t-1:2", "t-3:4", 9u64, "COIN")]);
        assert_eq!(Template::Reversed.postings(&v), vec![Posting::new("t-3:4", "t-1:2", 9u64, "COIN")]);
        assert_eq!(Template::ViaHub.postings(&v), vec![Posting::new("t-1:2", HUB, 9u64, "COIN"), Posting::new(HUB, "t-3:4", 9u64, "COIN")]);
        assert_eq!(v.to_json()["amount"], "COIN 9");

        let mut l = NsLedger::new("r-ns-x-1");
        let mut s = ScriptRec::default();
        s.versions.insert(Semver::new(1, 0, 0), VersionRec { content: String::new(), template: Template::Transfer, status: Status::Acked });
        s.versions.insert(Semver::new(2, 0, 0), VersionRec { content: String::new(), template: Template::Reversed, status: Status::Unknown });
        s.versions.insert(Semver::new(3, 0, 0), VersionRec { content: String::new(), template: Template::ViaHub, status: Status::Rejected });
        l.library.insert("s-1".into(), s.clone());
        // latest: 1.0.0 (acked) or 2.0.0 (unknown); 3.0.0 was rejected.
        let c = l.candidates(&Selector::Latest("s-1".into()), &v);
        assert_eq!(c, vec![Template::Transfer.postings(&v), Template::Reversed.postings(&v)]);
        // once 2.0.0 is acked, 1.0.0 can no longer be the latest
        l.library.get_mut("s-1").unwrap().versions.get_mut(&Semver::new(2, 0, 0)).unwrap().status = Status::Acked;
        assert_eq!(l.candidates(&Selector::Latest("s-1".into()), &v), vec![Template::Reversed.postings(&v)]);
        // A floor taken before 2.0.0 was acked still accepts 1.0.0's template.
        assert_eq!(l.candidates_since(&Selector::Latest("s-1".into()), &v, Some(Semver::new(1, 0, 0))), vec![Template::Transfer.postings(&v), Template::Reversed.postings(&v)]);
        // A save that started after the send (4.0.0, pending) is covered when evaluated after the answer.
        l.library.get_mut("s-1").unwrap().versions.insert(Semver::new(4, 0, 0), VersionRec { content: String::new(), template: Template::ViaHub, status: Status::Pending });
        assert!(l.candidates_since(&Selector::Latest("s-1".into()), &v, Some(Semver::new(2, 0, 0))).contains(&Template::ViaHub.postings(&v)));
        l.library.get_mut("s-1").unwrap().versions.remove(&Semver::new(4, 0, 0));
        assert_eq!(l.candidates(&Selector::Pinned("s-1".into(), Semver::new(1, 0, 0)), &v), vec![Template::Transfer.postings(&v)]);
        assert_eq!(l.candidates(&Selector::Inline, &v), vec![Template::Transfer.postings(&v)]);
        assert!(l.candidates(&Selector::Latest("nope".into()), &v).is_empty());
        l.record_tx(TxRec { reference: "r".into(), key: "k".into(), selector: Selector::Latest("s-1".into()), vars: v.clone(), candidates: c, status: Status::Pending, tx_id: None, committed: None });
        assert!(l.overdraft.contains("t-1:2") && l.overdraft.contains("t-3:4") && l.overdraft.contains(HUB) && l.overdraft.contains("world"));
    }

    #[test]
    fn latest_pointer_legality_with_unknown_saves() {
        let v = |a| Semver::new(a, 0, 0);
        let w = ReadWindow { acked_before: [v(1), v(2)].into_iter().collect(), possible_after: [v(1), v(2), v(3), v(4)].into_iter().collect() };
        assert!(!w.legal_latest(v(1)), "below the greatest acked");
        assert!(w.legal_latest(v(2)));
        assert!(w.legal_latest(v(3)), "an unknown save may have committed");
        assert!(w.legal_latest(v(4)));
        assert!(!w.legal_latest(v(5)), "never sent");
        assert!(w.legal_resolution("1", v(1)), "the major-1 floor is 1.0.0");
        assert!(!w.legal_resolution("1", v(2)));
        assert!(w.legal_resolution("2.0", v(2)));
        assert!(w.legal_resolution("3", v(3)), "no acked floor for major 3");
        assert!(w.legal_listing(&[v(1), v(2), v(4)].into_iter().collect()));
        assert!(!w.legal_listing(&[v(2)].into_iter().collect()), "an acked version is missing");
        assert!(!w.legal_listing(&[v(1), v(2), v(9)].into_iter().collect()), "a version never sent");
        let empty = ReadWindow::default();
        assert!(!empty.legal_latest(v(1)));
    }

    fn rec(reference: &str, status: Status, postings: Vec<Posting>) -> TxRec {
        TxRec {
            reference: reference.into(),
            key: format!("k-{reference}"),
            selector: Selector::Inline,
            vars: vars("world", "t-0:0", 1),
            candidates: vec![postings.clone()],
            status,
            tx_id: None,
            committed: if status == Status::Acked { Some(postings) } else { None },
        }
    }

    #[test]
    fn quiet_resolution_and_balances() {
        let p = |s: &str, d: &str, a: u64| vec![Posting::new(s, d, a, "COIN")];
        let mut l = NsLedger::new("r-ns-x-1");
        l.record_tx(rec("a", Status::Acked, p("world", "t-0:1", 5)));
        l.record_tx(rec("b", Status::Unknown, p("t-0:1", "t-0:2", 2)));
        l.record_tx(rec("c", Status::Unknown, p("world", "t-0:3", 7)));
        l.record_tx(rec("d", Status::Rejected, p("world", "t-0:4", 9)));
        l.record_tx(rec("e", Status::Pending, p("world", "t-0:5", 1)));
        let listed = vec![
            ListedTx { id: 0, reference: "a".into(), postings: p("world", "t-0:1", 5) },
            ListedTx { id: 1, reference: "b".into(), postings: p("t-0:1", "t-0:2", 2) },
        ];
        let e = evaluate(&mut l, &listed);
        assert!(e.acked_lost.is_empty() && e.rejected_listed.is_empty() && e.foreign.is_empty());
        assert_eq!((e.acked_checked, e.rejected_checked, e.resolved_committed, e.unresolved, e.committed_scripts), (1, 1, 1, 2, 2));
        assert_eq!(l.txs[1].status, Status::Acked);
        assert_eq!(l.txs[1].tx_id, Some(1));
        assert_eq!(l.txs[2].status, Status::Unknown);
        assert_eq!(e.expected[&("world".to_string(), "COIN".to_string())], (0, 5));
        assert_eq!(e.expected[&("t-0:1".to_string(), "COIN".to_string())], (5, 2));
        let vol = |a: &str, i: i128, o: i128| ListedVolume { address: a.into(), asset: "COIN".into(), color: String::new(), input: i, output: o, balance: i - o };
        let good = vec![vol("world", 0, 5), vol("t-0:1", 5, 2), vol("t-0:2", 2, 0)];
        assert!(compare_volumes(&e.expected, &good).is_empty(), "{:?}", compare_volumes(&e.expected, &good));
        let mut bad = good.clone();
        bad[1].input = 6;
        bad[1].balance = 4;
        assert_eq!(compare_volumes(&e.expected, &bad).len(), 1);
        let mut extra = good.clone();
        extra.push(vol("t-0:9", 1, 0));
        assert_eq!(compare_volumes(&e.expected, &extra).len(), 1);
        assert_eq!(compare_volumes(&e.expected, &good[..2]).len(), 1, "t-0:2 missing");

        // An acked transaction lost, a rejected one committed, a foreign one.
        let listed2 = vec![
            ListedTx { id: 1, reference: "b".into(), postings: p("t-0:1", "t-0:2", 2) },
            ListedTx { id: 3, reference: "d".into(), postings: p("world", "t-0:4", 9) },
            ListedTx { id: 4, reference: "zzz".into(), postings: p("world", "t-0:4", 9) },
        ];
        let e2 = evaluate(&mut l, &listed2);
        assert_eq!(e2.acked_lost, vec!["a".to_string()]);
        assert_eq!(e2.rejected_listed, vec!["d".to_string()]);
        assert_eq!(e2.foreign, vec![4]);
        // an acked transaction listed with other postings is lost too
        let listed3 = vec![ListedTx { id: 0, reference: "a".into(), postings: p("world", "t-0:1", 6) }, ListedTx { id: 1, reference: "b".into(), postings: p("t-0:1", "t-0:2", 2) }];
        assert_eq!(evaluate(&mut l, &listed3).acked_lost, vec!["a".to_string()]);
    }
}
