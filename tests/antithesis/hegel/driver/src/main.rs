//! Antithesis driver for the formance ledger v3: a hegel concurrent state
//! machine whose rules send every RPC through the client containers. Writes
//! and reads on oracle-tracked ledgers follow the oracle protocol (DESIGN
//! §4); the Go oracle service validates them. Rust keeps the checks the
//! oracle does not model.
#[macro_use]
mod assertions;

mod clients;
mod draw;
mod exclusive;
mod jobs;
mod logs;
mod machine;
mod nodes;
mod oracle;
mod quiet;
mod read;
mod resolver;
mod rules;
mod setup;
mod shared;
#[cfg(test)]
mod testkit;
mod wire;
mod write;

use hegel::TestCase;
use machine::Driver;
use shared::Shared;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};

static SHARED: OnceLock<Arc<Shared>> = OnceLock::new();
static ENTRIES: AtomicU64 = AtomicU64::new(0);

#[hegel::main(phases = [hegel::Phase::Generate])]
fn main(tc: TestCase) {
    let n = ENTRIES.fetch_add(1, Ordering::SeqCst);
    // antithesis_init, receivers, waits, admin setup, oracle /setup and the
    // tracked ledgers, setup hooks, setup_complete, the workload-active
    // wait — no draws.
    let s = SHARED.get_or_init(setup::run).clone();
    if n > 0 {
        println!("driver: hegel callback entered again (#{n}); reusing live state");
    }
    hegel::stateful::machine(Driver { s }).steps(i64::MAX).min_concurrency(2).max_concurrency(4).run_concurrent(tc);
}
