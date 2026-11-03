//! Draw primitives and the domains of DESIGN §6.8. Every random choice of
//! the driver comes from hegel through these. Identities (keys, wids,
//! references, ledger and owned-entity counters) are counters in `Shared`,
//! never draws.
#![allow(dead_code)] // family-facing API

use crate::clients::{Route, RouteMode};
use crate::shared::Shared;
use crate::wire::U256;
use hegel::generators as gs;
use hegel::TestCase;
use serde_json::{json, Value};

/// An index in `0..n` (0 when `n <= 1`, without a draw).
pub fn pick(tc: &TestCase, n: usize) -> usize {
    if n <= 1 {
        return 0;
    }
    tc.draw(gs::integers::<usize>().min_value(0).max_value(n - 1))
}

/// An integer in `lo..=hi`.
pub fn int_in(tc: &TestCase, lo: u64, hi: u64) -> u64 {
    if lo >= hi {
        return lo;
    }
    tc.draw(gs::integers::<u64>().min_value(lo).max_value(hi))
}

pub fn chance(tc: &TestCase, p: f64) -> bool {
    if p <= 0.0 {
        return false;
    }
    if p >= 1.0 {
        return true;
    }
    tc.draw(gs::weighted_booleans(p))
}

/// A weighted choice: an index whose weight is > 0, or `None` when no
/// weight is positive. A weight of 0 is never chosen.
pub fn weighted(tc: &TestCase, weights: &[f64]) -> Option<usize> {
    let pos: Vec<usize> = (0..weights.len()).filter(|&i| weights[i] > 0.0 && weights[i].is_finite()).collect();
    match pos.len() {
        0 => return None,
        1 => return Some(pos[0]),
        _ => {}
    }
    let total: f64 = pos.iter().map(|&i| weights[i]).sum();
    const SCALE: u64 = 1 << 32;
    let r = int_in(tc, 0, SCALE - 1) as f64 / SCALE as f64 * total;
    let mut acc = 0.0;
    for &i in &pos {
        acc += weights[i];
        if r < acc {
            return Some(i);
        }
    }
    pos.last().copied()
}

/// One element of a non-empty slice.
pub fn one_of<'a, T>(tc: &TestCase, xs: &'a [T]) -> &'a T {
    &xs[pick(tc, xs.len())]
}

// ---------------------------------------------------------------- domains

/// amount (general): full `u64` including 0 and `u64::MAX` — 1/8 one of
/// `{0, 1, 2^63, u64::MAX}`, 3/8 `1..=1000`, else any `u64`.
pub fn amount(tc: &TestCase) -> u64 {
    match int_in(tc, 0, 7) {
        0 => *one_of(tc, &[0u64, 1, 1 << 63, u64::MAX]),
        1..=3 => int_in(tc, 1, 1000),
        _ => tc.draw(gs::integers::<u64>()),
    }
}

/// amount (positive): the general shape without 0.
pub fn amount_positive(tc: &TestCase) -> u64 {
    match int_in(tc, 0, 7) {
        0 => *one_of(tc, &[1u64, 1 << 63, u64::MAX]),
        1..=3 => int_in(tc, 1, 1000),
        _ => int_in(tc, 1, u64::MAX),
    }
}

/// amount (boundary, 256-bit): `2^255` or `2^256 − 1`.
pub fn amount_u256_boundary(tc: &TestCase) -> U256 {
    if chance(tc, 0.5) {
        U256::pow2(255)
    } else {
        U256::MAX
    }
}

/// A pool address `t-<N>:<i>` (`N ∈ 0..=24`, `i ∈ 0..=99`).
pub fn pool_address(tc: &TestCase) -> String {
    format!("t-{}:{}", int_in(tc, 0, 24), int_in(tc, 0, 99))
}

/// `users:<0..=31>`.
pub fn users_address(tc: &TestCase) -> String {
    format!("users:{}", int_in(tc, 0, 31))
}

/// An address: `world` (1/8), a pool address (1/2), or a users address.
/// Never an `own-` segment.
pub fn address(tc: &TestCase) -> String {
    match int_in(tc, 0, 7) {
        0 => "world".to_string(),
        1..=4 => pool_address(tc),
        _ => users_address(tc),
    }
}

/// A rule-owned address `<account_type>:own-<tag><counter>` (counter from
/// `Shared::next_own`, not a draw). Used only by rule-owned probes.
pub fn owned_address(s: &Shared, account_type: &str, tag: &str) -> String {
    format!("{account_type}:own-{tag}{}", s.next_own())
}

/// `ledgerPick`: any `u32` (DESIGN §4.7).
pub fn ledger_pick(tc: &TestCase) -> u32 {
    tc.draw(gs::integers::<u32>())
}

pub const ASSETS: [&str; 3] = ["USD/2", "EUR/2", "COIN"];
pub const BLOCK_ASSETS: [&str; 4] = ["USD/2", "EUR/2", "COIN", "GBP/2"];

pub fn asset(tc: &TestCase) -> &'static str {
    one_of(tc, &ASSETS)
}

/// Assets for scenario blocks only (adds `GBP/2`).
pub fn block_asset(tc: &TestCase) -> &'static str {
    one_of(tc, &BLOCK_ASSETS)
}

/// A color: geometric depth through repeated `chance(1/2)` (at most 64), as
/// bijective base-26 letters: `""` with probability 1/2, then `A..Z`, `AA..`.
pub fn color(tc: &TestCase) -> String {
    let mut depth = 0;
    while depth < 64 && chance(tc, 0.5) {
        depth += 1;
    }
    color_name(depth)
}

pub fn color_name(mut k: usize) -> String {
    let mut out = Vec::new();
    while k > 0 {
        k -= 1;
        out.push(b'A' + (k % 26) as u8);
        k /= 26;
    }
    out.reverse();
    String::from_utf8(out).unwrap()
}

/// Model metadata keys `k0..k3`.
pub fn meta_key_model(tc: &TestCase) -> String {
    format!("k{}", int_in(tc, 0, 3))
}

/// Decimal metadata keys `0..=998` (main-driver ports).
pub fn meta_key_decimal(tc: &TestCase) -> String {
    int_in(tc, 0, 998).to_string()
}

const META_STRINGS: [&str; 11] = ["v0", "7", "42", "true", "false", "200", "-1", "-200", "40000", "5000000000", "030"];
const META_INTS: [i64; 6] = [0, 1, -1, 42, -200, 5000000000];
const META_UINTS: [u64; 6] = [0, 1, 42, 200, 40000, 5000000000];
const META_DATETIMES: [i64; 4] = [0, 1, -1000000, 1700000000000000];
const META_NULL_ORIGINALS: [&str; 3] = ["", "null", "030"];

/// A `MetadataValue` protojson: one of 6 kinds, each from a small pool
/// (`M/config.go`, `M/actions.go randomMetaValue`).
pub fn meta_value(tc: &TestCase) -> Value {
    match int_in(tc, 0, 5) {
        0 => json!({"intValue": one_of(tc, &META_INTS).to_string()}),
        1 => json!({"uintValue": one_of(tc, &META_UINTS).to_string()}),
        2 => json!({"boolValue": chance(tc, 0.5)}),
        3 => {
            let o = *one_of(tc, &META_NULL_ORIGINALS);
            if o.is_empty() {
                json!({"nullValue": {}})
            } else {
                json!({"nullValue": {"original": o}})
            }
        }
        4 => json!({"datetimeValue": one_of(tc, &META_DATETIMES).to_string()}),
        _ => json!({"stringValue": one_of(tc, &META_STRINGS)}),
    }
}

/// Bulk size (mixed): 5/8 → 1, 2/8 → 2, 1/8 → `3..=5`.
pub fn bulk_size_mixed(tc: &TestCase) -> usize {
    match int_in(tc, 0, 7) {
        0..=4 => 1,
        5 | 6 => 2,
        _ => int_in(tc, 3, 5) as usize,
    }
}

/// Bulk size (stress): `1..=5000`, truncated geometric with `p = 0.001`
/// (untracked, `r-stress-*` only, sent from a job).
pub fn bulk_size_stress(tc: &TestCase) -> usize {
    // Inverse-CDF of the geometric distribution on a 32-bit uniform draw.
    let u = (int_in(tc, 0, (1 << 32) - 1) as f64 + 0.5) / (1u64 << 32) as f64;
    let n = ((1.0 - u).ln() / (1.0f64 - 0.001).ln()).floor() as usize + 1;
    n.clamp(1, 5000)
}

/// Page size: `{1,2,3,5,10,50}`, 1/16 zero (server default), 1/32 above
/// 1000 (clamped by the server).
pub fn page_size(tc: &TestCase) -> u64 {
    match int_in(tc, 0, 31) {
        0 | 1 => 0,
        2 => int_in(tc, 1001, 5000),
        _ => *one_of(tc, &[1u64, 2, 3, 5, 10, 50]),
    }
}

/// A backoff in `50..=2000` ms.
pub fn backoff_ms(tc: &TestCase) -> u64 {
    int_in(tc, 50, 2000)
}

/// A route: client `0..n`; `Lib` 1/2, `Plain` 1/4, `Node(any)` 1/4.
pub fn route(tc: &TestCase, s: &Shared) -> Route {
    let client = pick(tc, s.clients.len().max(1));
    let mode = match int_in(tc, 0, 3) {
        0 | 1 => RouteMode::Lib,
        2 => RouteMode::Plain,
        _ if !s.nodes.is_empty() => RouteMode::Node(one_of(tc, &s.nodes).clone()),
        _ => RouteMode::Plain,
    };
    Route { client, mode }
}

/// An attempt schedule of `n` routes (cycled by the write loop).
pub fn schedule(tc: &TestCase, s: &Shared, n: usize) -> Vec<Route> {
    (0..n.max(1)).map(|_| route(tc, s)).collect()
}

/// `n` backoffs (cycled by the write loop).
pub fn backoffs(tc: &TestCase, n: usize) -> Vec<u64> {
    (0..n.max(1)).map(|_| backoff_ms(tc)).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testkit;

    #[test]
    fn color_names() {
        assert_eq!(color_name(0), "");
        assert_eq!(color_name(1), "A");
        assert_eq!(color_name(26), "Z");
        assert_eq!(color_name(27), "AA");
        assert_eq!(color_name(64), "BL");
    }

    #[test]
    fn domains_never_produce_reserved_names_and_respect_ranges() {
        let (d, _, _) = testkit::driver();
        testkit::with_tc_n(200, |tc| {
            for a in [address(tc), pool_address(tc), users_address(tc)] {
                assert!(!a.split(':').skip(1).any(|seg| seg.starts_with("own-")), "{a}");
                assert!(a == "world" || a.starts_with("t-") || a.starts_with("users:"), "{a}");
            }
            assert!(amount_positive(tc) > 0);
            let n = bulk_size_mixed(tc);
            assert!((1..=5).contains(&n));
            assert!((1..=5000).contains(&bulk_size_stress(tc)));
            assert!((50..=2000).contains(&backoff_ms(tc)));
            let p = page_size(tc);
            assert!(p == 0 || [1, 2, 3, 5, 10, 50].contains(&p) || p > 1000);
            assert!(ASSETS.contains(&asset(tc)));
            assert!(color(tc).chars().all(|c| c.is_ascii_uppercase()));
            let k = meta_key_model(tc);
            assert!(["k0", "k1", "k2", "k3"].contains(&k.as_str()));
            assert!(meta_key_decimal(tc).parse::<u32>().unwrap() <= 998);
            let v = meta_value(tc);
            assert_eq!(v.as_object().unwrap().len(), 1);
            let r = route(tc, &d.s);
            assert!(r.client < d.s.clients.len());
            assert_eq!(schedule(tc, &d.s, 8).len(), 8);
            let b = amount_u256_boundary(tc);
            assert!(b == U256::pow2(255) || b == U256::MAX);
        });
        let o = owned_address(&d.s, "t-0", "drain");
        assert!(o.starts_with("t-0:own-drain"), "{o}");
    }

    #[test]
    fn weighted_never_picks_zero_weights() {
        testkit::with_tc_n(100, |tc| {
            let i = weighted(tc, &[0.0, 1.0, 0.0, 3.0]).unwrap();
            assert!(i == 1 || i == 3);
            assert_eq!(weighted(tc, &[0.0, 0.0]), None);
            assert_eq!(weighted(tc, &[]), None);
            assert_eq!(weighted(tc, &[0.0, 2.0]), Some(1));
        });
    }
}
