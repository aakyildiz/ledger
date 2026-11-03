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

use hegel::{HealthCheck, Hegel, Phase, Settings, TestCase};
use machine::Driver;
use shared::Shared;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};

static SHARED: OnceLock<Arc<Shared>> = OnceLock::new();
static ENTRIES: AtomicU64 = AtomicU64::new(0);

/// Steps of one hegel test case (env `HEGEL_STEPS_PER_CASE`).
///
/// hegel records every choice of a test case until the case ends, and a
/// `#[hegel::main]` binary runs ONE test case with the choice limit removed,
/// so the driver's memory grew with the run's length (about 3 KB per step:
/// 700 MB after 45 minutes of the local stack, task 99). The driver now runs
/// an unbounded sequence of bounded test cases instead; state lives in
/// `Shared`, so a new case continues the same workload (DESIGN §6.4 re-entry),
/// and each case draws its own concurrency level.
const STEPS_PER_CASE: i64 = 2_000;

fn steps_per_case() -> i64 {
    std::env::var("HEGEL_STEPS_PER_CASE").ok().and_then(|v| v.trim().parse().ok()).filter(|n: &i64| *n > 0).unwrap_or(STEPS_PER_CASE)
}

fn main() {
    // The engine's environment profile (`workload` inside Antithesis: urandom
    // backend, no database, every health check suppressed), with only the
    // Generate phase, as `#[hegel::main(phases = [Generate])]` had, and an
    // unbounded number of test cases.
    let settings = Settings::new().phases([Phase::Generate]).test_cases(u64::MAX).suppress_health_check(HealthCheck::all());
    Hegel::new(driver_case).settings(settings).run();
}

fn driver_case(tc: TestCase) {
    let n = ENTRIES.fetch_add(1, Ordering::SeqCst);
    // antithesis_init, receivers, waits, admin setup, oracle /setup and the
    // tracked ledgers, setup hooks, setup_complete, the workload-active
    // wait — no draws.
    let s = SHARED.get_or_init(setup::run).clone();
    if n > 0 && n % 50 == 1 {
        println!("driver: hegel test case #{n} starts; reusing live state");
    }
    hegel::stateful::machine(Driver { s }).steps(steps_per_case()).min_concurrency(2).max_concurrency(4).run_concurrent(tc);
}
