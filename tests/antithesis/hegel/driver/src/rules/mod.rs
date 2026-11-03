//! The rule registries (DESIGN §6.2). Every family adds lines **only**
//! between its own marker pairs (typically `pub mod <fam>;` and
//! `v.extend(<fam>::rules());`); marker lines never change, so family hunks
//! merge cleanly. `smoke` is registered outside the marker blocks.
use crate::machine::Driver;
use hegel::stateful::ConcurrentRule;
pub mod smoke;
// ==== 02-tx-writes mods ====
// ==== end 02-tx-writes mods ====
// ==== 03-reverts-metadata-idempotency mods ====
// ==== end 03-reverts-metadata-idempotency mods ====
// ==== 04-ledgers-chart-maintenance mods ====
pub mod chart;
pub mod ledgers;
pub mod maintenance;
// ==== end 04-ledgers-chart-maintenance mods ====
// ==== 05-point-reads mods ====
// ==== end 05-point-reads mods ====
// ==== 06-list-queries mods ====
// ==== end 06-list-queries mods ====
// ==== 07-indexes-typed-metadata mods ====
// ==== end 07-indexes-typed-metadata mods ====
// ==== 08-prepared-queries mods ====
// ==== end 08-prepared-queries mods ====
// ==== 09-query-checkpoints mods ====
// ==== end 09-query-checkpoints mods ====
// ==== 10-audit mods ====
// ==== end 10-audit mods ====
// ==== 11-quiet-exact mods ====
// ==== end 11-quiet-exact mods ====
// ==== 12-cluster-ops-signals mods ====
// ==== end 12-cluster-ops-signals mods ====
// ==== 13-numscript mods ====
// ==== end 13-numscript mods ====
// ==== 14-rust-probes mods ====
// ==== end 14-rust-probes mods ====
// ==== 15-convergence mods ====
// ==== end 15-convergence mods ====
// ==== 16-signing mods ====
// ==== end 16-signing mods ====
// ==== 17-scenario-blocks mods ====
// ==== end 17-scenario-blocks mods ====
// ==== 18-restore mods ====
// ==== end 18-restore mods ====

pub fn rules() -> Vec<ConcurrentRule<Driver>> {
    let mut v = Vec::new();
    v.extend(smoke::rules());
    // ==== 02-tx-writes rules ====
    // ==== end 02-tx-writes rules ====
    // ==== 03-reverts-metadata-idempotency rules ====
    // ==== end 03-reverts-metadata-idempotency rules ====
    // ==== 04-ledgers-chart-maintenance rules ====
    v.extend(ledgers::rules());
    v.extend(chart::rules());
    v.extend(maintenance::rules());
    // ==== end 04-ledgers-chart-maintenance rules ====
    // ==== 05-point-reads rules ====
    // ==== end 05-point-reads rules ====
    // ==== 06-list-queries rules ====
    // ==== end 06-list-queries rules ====
    // ==== 07-indexes-typed-metadata rules ====
    // ==== end 07-indexes-typed-metadata rules ====
    // ==== 08-prepared-queries rules ====
    // ==== end 08-prepared-queries rules ====
    // ==== 09-query-checkpoints rules ====
    // ==== end 09-query-checkpoints rules ====
    // ==== 10-audit rules ====
    // ==== end 10-audit rules ====
    // ==== 11-quiet-exact rules ====
    // ==== end 11-quiet-exact rules ====
    // ==== 12-cluster-ops-signals rules ====
    // ==== end 12-cluster-ops-signals rules ====
    // ==== 13-numscript rules ====
    // ==== end 13-numscript rules ====
    // ==== 14-rust-probes rules ====
    // ==== end 14-rust-probes rules ====
    // ==== 15-convergence rules ====
    // ==== end 15-convergence rules ====
    // ==== 16-signing rules ====
    // ==== end 16-signing rules ====
    // ==== 17-scenario-blocks rules ====
    // ==== end 17-scenario-blocks rules ====
    // ==== 18-restore rules ====
    // ==== end 18-restore rules ====
    keep_named(v, std::env::var("RULES").ok().as_deref())
}

#[allow(dead_code)] // family-facing: task 02's tx_bulk composes over it
pub fn order_gens() -> Vec<crate::write::OrderGen> {
    let mut v = Vec::new();
    v.extend(std::iter::empty()); // families extend below
    // ==== 02-tx-writes order_gens ====
    // ==== end 02-tx-writes order_gens ====
    // ==== 03-reverts-metadata-idempotency order_gens ====
    // ==== end 03-reverts-metadata-idempotency order_gens ====
    // ==== 04-ledgers-chart-maintenance order_gens ====
    v.extend(chart::order_gens());
    // ==== end 04-ledgers-chart-maintenance order_gens ====
    // ==== 05-point-reads order_gens ====
    // ==== end 05-point-reads order_gens ====
    // ==== 06-list-queries order_gens ====
    // ==== end 06-list-queries order_gens ====
    // ==== 07-indexes-typed-metadata order_gens ====
    // ==== end 07-indexes-typed-metadata order_gens ====
    // ==== 08-prepared-queries order_gens ====
    // ==== end 08-prepared-queries order_gens ====
    // ==== 09-query-checkpoints order_gens ====
    // ==== end 09-query-checkpoints order_gens ====
    // ==== 10-audit order_gens ====
    // ==== end 10-audit order_gens ====
    // ==== 11-quiet-exact order_gens ====
    // ==== end 11-quiet-exact order_gens ====
    // ==== 12-cluster-ops-signals order_gens ====
    // ==== end 12-cluster-ops-signals order_gens ====
    // ==== 13-numscript order_gens ====
    // ==== end 13-numscript order_gens ====
    // ==== 14-rust-probes order_gens ====
    // ==== end 14-rust-probes order_gens ====
    // ==== 15-convergence order_gens ====
    // ==== end 15-convergence order_gens ====
    // ==== 16-signing order_gens ====
    // ==== end 16-signing order_gens ====
    // ==== 17-scenario-blocks order_gens ====
    // ==== end 17-scenario-blocks order_gens ====
    // ==== 18-restore order_gens ====
    // ==== end 18-restore order_gens ====
    v
}

pub fn quiet_hooks() -> Vec<crate::quiet::QuietHook> {
    let mut v = Vec::new();
    v.extend(std::iter::empty()); // families extend below
    // ==== 02-tx-writes quiet_hooks ====
    // ==== end 02-tx-writes quiet_hooks ====
    // ==== 03-reverts-metadata-idempotency quiet_hooks ====
    // ==== end 03-reverts-metadata-idempotency quiet_hooks ====
    // ==== 04-ledgers-chart-maintenance quiet_hooks ====
    // ==== end 04-ledgers-chart-maintenance quiet_hooks ====
    // ==== 05-point-reads quiet_hooks ====
    // ==== end 05-point-reads quiet_hooks ====
    // ==== 06-list-queries quiet_hooks ====
    // ==== end 06-list-queries quiet_hooks ====
    // ==== 07-indexes-typed-metadata quiet_hooks ====
    // ==== end 07-indexes-typed-metadata quiet_hooks ====
    // ==== 08-prepared-queries quiet_hooks ====
    // ==== end 08-prepared-queries quiet_hooks ====
    // ==== 09-query-checkpoints quiet_hooks ====
    // ==== end 09-query-checkpoints quiet_hooks ====
    // ==== 10-audit quiet_hooks ====
    // ==== end 10-audit quiet_hooks ====
    // ==== 11-quiet-exact quiet_hooks ====
    // ==== end 11-quiet-exact quiet_hooks ====
    // ==== 12-cluster-ops-signals quiet_hooks ====
    // ==== end 12-cluster-ops-signals quiet_hooks ====
    // ==== 13-numscript quiet_hooks ====
    // ==== end 13-numscript quiet_hooks ====
    // ==== 14-rust-probes quiet_hooks ====
    // ==== end 14-rust-probes quiet_hooks ====
    // ==== 15-convergence quiet_hooks ====
    // ==== end 15-convergence quiet_hooks ====
    // ==== 16-signing quiet_hooks ====
    // ==== end 16-signing quiet_hooks ====
    // ==== 17-scenario-blocks quiet_hooks ====
    // ==== end 17-scenario-blocks quiet_hooks ====
    // ==== 18-restore quiet_hooks ====
    // ==== end 18-restore quiet_hooks ====
    v
}

pub fn exclusive_actions() -> Vec<crate::exclusive::ExclusiveAction> {
    let mut v = Vec::new();
    v.extend(std::iter::empty()); // families extend below
    // ==== 02-tx-writes exclusive_actions ====
    // ==== end 02-tx-writes exclusive_actions ====
    // ==== 03-reverts-metadata-idempotency exclusive_actions ====
    // ==== end 03-reverts-metadata-idempotency exclusive_actions ====
    // ==== 04-ledgers-chart-maintenance exclusive_actions ====
    // ==== end 04-ledgers-chart-maintenance exclusive_actions ====
    // ==== 05-point-reads exclusive_actions ====
    // ==== end 05-point-reads exclusive_actions ====
    // ==== 06-list-queries exclusive_actions ====
    // ==== end 06-list-queries exclusive_actions ====
    // ==== 07-indexes-typed-metadata exclusive_actions ====
    // ==== end 07-indexes-typed-metadata exclusive_actions ====
    // ==== 08-prepared-queries exclusive_actions ====
    // ==== end 08-prepared-queries exclusive_actions ====
    // ==== 09-query-checkpoints exclusive_actions ====
    // ==== end 09-query-checkpoints exclusive_actions ====
    // ==== 10-audit exclusive_actions ====
    // ==== end 10-audit exclusive_actions ====
    // ==== 11-quiet-exact exclusive_actions ====
    // ==== end 11-quiet-exact exclusive_actions ====
    // ==== 12-cluster-ops-signals exclusive_actions ====
    // ==== end 12-cluster-ops-signals exclusive_actions ====
    // ==== 13-numscript exclusive_actions ====
    // ==== end 13-numscript exclusive_actions ====
    // ==== 14-rust-probes exclusive_actions ====
    // ==== end 14-rust-probes exclusive_actions ====
    // ==== 15-convergence exclusive_actions ====
    // ==== end 15-convergence exclusive_actions ====
    // ==== 16-signing exclusive_actions ====
    // ==== end 16-signing exclusive_actions ====
    // ==== 17-scenario-blocks exclusive_actions ====
    // ==== end 17-scenario-blocks exclusive_actions ====
    // ==== 18-restore exclusive_actions ====
    // ==== end 18-restore exclusive_actions ====
    v
}

pub fn setup_hooks() -> Vec<crate::setup::SetupHook> {
    let mut v = Vec::new();
    v.extend(std::iter::empty()); // families extend below
    // ==== 02-tx-writes setup_hooks ====
    // ==== end 02-tx-writes setup_hooks ====
    // ==== 03-reverts-metadata-idempotency setup_hooks ====
    // ==== end 03-reverts-metadata-idempotency setup_hooks ====
    // ==== 04-ledgers-chart-maintenance setup_hooks ====
    v.extend(maintenance::setup_hooks());
    // ==== end 04-ledgers-chart-maintenance setup_hooks ====
    // ==== 05-point-reads setup_hooks ====
    // ==== end 05-point-reads setup_hooks ====
    // ==== 06-list-queries setup_hooks ====
    // ==== end 06-list-queries setup_hooks ====
    // ==== 07-indexes-typed-metadata setup_hooks ====
    // ==== end 07-indexes-typed-metadata setup_hooks ====
    // ==== 08-prepared-queries setup_hooks ====
    // ==== end 08-prepared-queries setup_hooks ====
    // ==== 09-query-checkpoints setup_hooks ====
    // ==== end 09-query-checkpoints setup_hooks ====
    // ==== 10-audit setup_hooks ====
    // ==== end 10-audit setup_hooks ====
    // ==== 11-quiet-exact setup_hooks ====
    // ==== end 11-quiet-exact setup_hooks ====
    // ==== 12-cluster-ops-signals setup_hooks ====
    // ==== end 12-cluster-ops-signals setup_hooks ====
    // ==== 13-numscript setup_hooks ====
    // ==== end 13-numscript setup_hooks ====
    // ==== 14-rust-probes setup_hooks ====
    // ==== end 14-rust-probes setup_hooks ====
    // ==== 15-convergence setup_hooks ====
    // ==== end 15-convergence setup_hooks ====
    // ==== 16-signing setup_hooks ====
    // ==== end 16-signing setup_hooks ====
    // ==== 17-scenario-blocks setup_hooks ====
    // ==== end 17-scenario-blocks setup_hooks ====
    // ==== 18-restore setup_hooks ====
    // ==== end 18-restore setup_hooks ====
    v
}

pub fn log_hooks() -> Vec<crate::logs::LogHook> {
    let mut v = Vec::new();
    v.extend(std::iter::empty()); // families extend below
    // ==== 02-tx-writes log_hooks ====
    // ==== end 02-tx-writes log_hooks ====
    // ==== 03-reverts-metadata-idempotency log_hooks ====
    // ==== end 03-reverts-metadata-idempotency log_hooks ====
    // ==== 04-ledgers-chart-maintenance log_hooks ====
    // ==== end 04-ledgers-chart-maintenance log_hooks ====
    // ==== 05-point-reads log_hooks ====
    // ==== end 05-point-reads log_hooks ====
    // ==== 06-list-queries log_hooks ====
    // ==== end 06-list-queries log_hooks ====
    // ==== 07-indexes-typed-metadata log_hooks ====
    // ==== end 07-indexes-typed-metadata log_hooks ====
    // ==== 08-prepared-queries log_hooks ====
    // ==== end 08-prepared-queries log_hooks ====
    // ==== 09-query-checkpoints log_hooks ====
    // ==== end 09-query-checkpoints log_hooks ====
    // ==== 10-audit log_hooks ====
    // ==== end 10-audit log_hooks ====
    // ==== 11-quiet-exact log_hooks ====
    // ==== end 11-quiet-exact log_hooks ====
    // ==== 12-cluster-ops-signals log_hooks ====
    // ==== end 12-cluster-ops-signals log_hooks ====
    // ==== 13-numscript log_hooks ====
    // ==== end 13-numscript log_hooks ====
    // ==== 14-rust-probes log_hooks ====
    // ==== end 14-rust-probes log_hooks ====
    // ==== 15-convergence log_hooks ====
    // ==== end 15-convergence log_hooks ====
    // ==== 16-signing log_hooks ====
    // ==== end 16-signing log_hooks ====
    // ==== 17-scenario-blocks log_hooks ====
    // ==== end 17-scenario-blocks log_hooks ====
    // ==== 18-restore log_hooks ====
    // ==== end 18-restore log_hooks ====
    v
}

/// The `RULES` filter (00d, for `run_native.sh`): with a comma-separated
/// allow-list, keep only the named rules; unset or empty keeps them all.
fn keep_named(v: Vec<ConcurrentRule<Driver>>, allow: Option<&str>) -> Vec<ConcurrentRule<Driver>> {
    let Some(list) = allow.map(str::trim).filter(|l| !l.is_empty()) else {
        return v;
    };
    let names: Vec<&str> = list.split(',').map(str::trim).filter(|n| !n.is_empty()).collect();
    for n in &names {
        if !v.iter().any(|r| r.name == *n) {
            println!("driver: RULES names unknown rule {n:?}");
        }
    }
    let kept: Vec<_> = v.into_iter().filter(|r| names.contains(&r.name.as_str())).collect();
    println!("driver: RULES keeps {} rule(s): {:?}", kept.len(), kept.iter().map(|r| r.name.as_str()).collect::<Vec<_>>());
    kept
}

#[cfg(test)]
mod tests {
    /// The 17 family marker pairs of each of the seven sections exist, in
    /// order, exactly once, each `end` right after (or after the family's
    /// own lines following) its opening marker.
    #[test]
    fn marker_pairs() {
        const FAMILIES: [&str; 17] = [
            "02-tx-writes", "03-reverts-metadata-idempotency", "04-ledgers-chart-maintenance", "05-point-reads",
            "06-list-queries", "07-indexes-typed-metadata", "08-prepared-queries", "09-query-checkpoints", "10-audit",
            "11-quiet-exact", "12-cluster-ops-signals", "13-numscript", "14-rust-probes", "15-convergence", "16-signing",
            "17-scenario-blocks", "18-restore",
        ];
        const SECTIONS: [&str; 7] = ["mods", "rules", "order_gens", "quiet_hooks", "exclusive_actions", "setup_hooks", "log_hooks"];
        let src = include_str!("mod.rs");
        let markers: Vec<String> = src.lines().map(str::trim).filter(|l| l.starts_with("// ==== ") && l.ends_with(" ====")).map(str::to_string).collect();
        let mut expected = Vec::new();
        for s in SECTIONS {
            for f in FAMILIES {
                expected.push(format!("// ==== {f} {s} ===="));
                expected.push(format!("// ==== end {f} {s} ===="));
            }
        }
        assert_eq!(markers.len(), 17 * 7 * 2);
        assert_eq!(markers, expected);
        // Each section's markers sit inside the right registry function.
        let fns = ["pub fn rules()", "pub fn order_gens()", "pub fn quiet_hooks()", "pub fn exclusive_actions()", "pub fn setup_hooks()", "pub fn log_hooks()"];
        let mut pos = src.find("pub mod smoke;").unwrap();
        for (i, s) in SECTIONS.iter().enumerate() {
            if i > 0 {
                let f = src.find(fns[i - 1]).unwrap();
                assert!(f > pos, "{s} after its function");
                pos = f;
            }
            let first = src.find(&format!("// ==== 02-tx-writes {s} ====")).unwrap();
            let last = src.find(&format!("// ==== end 18-restore {s} ====")).unwrap();
            assert!(first > pos && last > first, "{s}");
            if i + 1 < SECTIONS.len() {
                assert!(last < src.find(fns[i]).unwrap(), "{s} before the next function");
            }
        }
    }

    #[test]
    fn smoke_rules_are_registered() {
        let names: Vec<String> = super::rules().into_iter().map(|r| r.name).collect();
        assert!(names.contains(&"smoke_tx".to_string()));
        assert!(names.contains(&"smoke_get_account".to_string()));
    }

    #[test]
    fn rules_filter_keeps_only_named_rules() {
        let all = || super::smoke::rules();
        let n = all().len();
        assert_eq!(super::keep_named(all(), None).len(), n);
        assert_eq!(super::keep_named(all(), Some("  ")).len(), n);
        let kept = super::keep_named(all(), Some("smoke_tx, nope"));
        assert_eq!(kept.iter().map(|r| r.name.as_str()).collect::<Vec<_>>(), vec!["smoke_tx"]);
    }
}
