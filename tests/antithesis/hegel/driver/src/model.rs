//! What the driver knows the ledger should contain.
use crate::wire::Posting;
use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Status {
    /// Sent, no answer yet.
    Pending,
    /// The system acknowledged it.
    Acked,
    /// The system definitively rejected it (an idempotency-keyed write with a
    /// business error): it must never take effect.
    Rejected,
    /// The outcome is unknown: it may or may not take effect.
    Unknown,
}

/// One CreateTransaction the driver sent (possibly several times, under one
/// idempotency key).
#[derive(Clone, Debug)]
#[allow(dead_code)] // kept for Debug output in assertion details
pub struct TxRec {
    pub ops: Vec<u64>,
    pub reference: String,
    pub postings: Vec<Posting>,
    pub force: bool,
    pub key: Option<String>,
    pub sent: Instant,
    pub acked_at: Option<Instant>,
    pub status: Status,
    pub tx_id: Option<u64>,
    pub reason: String,
    /// Part of a batch the driver built to fail atomically.
    pub doomed_batch: bool,
    /// The exact request sent, for replays under the same idempotency key.
    /// None for a transaction sent as part of a batch.
    pub request: Option<serde_json::Value>,
}

#[derive(Clone, Debug)]
#[allow(dead_code)] // kept for Debug output in assertion details
pub struct RevertRec {
    pub op: u64,
    pub key: Option<String>,
    pub sent: Instant,
    pub status: Status,
    pub new_id: Option<u64>,
    pub acked_at: Option<Instant>,
}

/// One write to a metadata key; `value` None is a delete.
#[derive(Clone, Debug)]
pub struct MetaWrite {
    pub op: u64,
    pub value: Option<String>,
    pub sent: Instant,
    pub acked: Option<Instant>,
    pub status: Status,
}

#[derive(Default, Debug)]
pub struct KeyHistory {
    pub writes: Vec<MetaWrite>,
}

impl KeyHistory {
    fn overwritten_before(&self, w: &MetaWrite, t: Instant) -> bool {
        let Some(w_acked) = w.acked else { return false };
        self.writes.iter().any(|o| o.sent > w_acked && o.status == Status::Acked && o.acked.is_some_and(|a| a < t))
    }

    /// Whether a read that started at `start` and ended at `end` may return `got`.
    pub fn allows(&self, got: &Option<String>, start: Instant, end: Instant) -> bool {
        let initial_possible = !self.writes.iter().any(|w| w.status == Status::Acked && w.acked.is_some_and(|a| a < start));
        if got.is_none() && initial_possible {
            return true;
        }
        self.writes.iter().any(|w| {
            w.status != Status::Rejected && w.sent < end && !self.overwritten_before(w, start) && &w.value == got
        })
    }

    pub fn has_overlap(&self, start: Instant, end: Instant) -> bool {
        self.writes.iter().any(|w| w.sent < end && w.acked.map_or(true, |a| a > start))
    }

    pub fn prune(&mut self) {
        if self.writes.len() <= 48 {
            return;
        }
        let now = Instant::now();
        let horizon = now.checked_sub(Duration::from_secs(30)).unwrap_or(now);
        let keep: Vec<bool> = self
            .writes
            .iter()
            .map(|w| match w.acked {
                None => now.duration_since(w.sent) < Duration::from_secs(120),
                Some(_) => !self.overwritten_before(w, horizon),
            })
            .collect();
        let mut it = keep.into_iter();
        self.writes.retain(|_| it.next().unwrap());
    }
}

pub struct LedgerModel {
    pub name: String,
    pub txs: Vec<TxRec>,
    pub by_ref: HashMap<String, usize>,
    /// Acknowledged transaction id -> index in txs.
    pub by_id: HashMap<u64, usize>,
    /// Revert attempts by target transaction id.
    pub reverts: HashMap<u64, Vec<RevertRec>>,
    /// Ids of acknowledged revert transactions.
    pub revert_ids: HashMap<u64, u64>,
    pub meta: HashMap<(String, String), KeyHistory>,
    /// Accounts a forced write may have overdrawn.
    pub may_be_negative: HashSet<String>,
}

impl LedgerModel {
    pub fn new(name: &str) -> Self {
        LedgerModel {
            name: name.to_string(),
            txs: vec![],
            by_ref: HashMap::new(),
            by_id: HashMap::new(),
            reverts: HashMap::new(),
            revert_ids: HashMap::new(),
            meta: HashMap::new(),
            may_be_negative: HashSet::new(),
        }
    }

    pub fn acked_ids(&self) -> Vec<u64> {
        self.by_id.keys().copied().collect()
    }
}

/// Ephemeral ledger lifecycle.
#[derive(Clone, Debug, PartialEq)]
pub enum Lifecycle {
    Creating,
    Live,
    /// Delete acknowledged at this instant.
    Deleted(Instant),
    /// Delete outcome unknown.
    MaybeDeleted,
}

pub struct Model {
    pub ledgers: Vec<LedgerModel>,
    pub ephemeral: HashMap<String, Lifecycle>,
    /// Highest Barrier commit index acknowledged so far, with when.
    pub barrier_high: Vec<(Instant, u64)>,
}

impl Model {
    pub fn new(names: &[&str]) -> Self {
        Model {
            ledgers: names.iter().map(|n| LedgerModel::new(n)).collect(),
            ephemeral: HashMap::new(),
            barrier_high: vec![],
        }
    }
}
