//! Semantic versions of the Numscript library (task 13): ordering, parsing,
//! partial selectors and the drawn "strictly greater" bumps of
//! `MD/parallel_driver_numscript_lifecycle` (`numscriptVersions`), with every
//! component in the uint32 range.
use std::fmt;

/// `major.minor.patch`, each a `u32` (the server's `internal/pkg/semver`).
/// The derived order is the semver order (fields compared in order).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Semver {
    pub major: u32,
    pub minor: u32,
    pub patch: u32,
}

impl Semver {
    pub const fn new(major: u32, minor: u32, patch: u32) -> Self {
        Semver { major, minor, patch }
    }

    /// A full `a.b.c` version; anything else is `None`.
    pub fn parse(s: &str) -> Option<Semver> {
        let mut it = s.split('.');
        let a = it.next()?.parse().ok()?;
        let b = it.next()?.parse().ok()?;
        let c = it.next()?.parse().ok()?;
        if it.next().is_some() {
            return None;
        }
        Some(Semver::new(a, b, c))
    }

    /// Whether this version matches a GetNumscript selector: `""`/`latest`
    /// (every version), `a` (same major), `a.b` (same major and minor) or
    /// an exact `a.b.c`.
    pub fn matches(&self, selector: &str) -> bool {
        if selector.is_empty() || selector == "latest" {
            return true;
        }
        let parts: Vec<&str> = selector.split('.').collect();
        let comp = |i: usize| parts.get(i).and_then(|p| p.parse::<u32>().ok());
        match parts.len() {
            1 => comp(0) == Some(self.major),
            2 => comp(0) == Some(self.major) && comp(1) == Some(self.minor),
            3 => comp(0) == Some(self.major) && comp(1) == Some(self.minor) && comp(2) == Some(self.patch),
            _ => false,
        }
    }
}

impl fmt::Display for Semver {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}.{}", self.major, self.minor, self.patch)
    }
}

/// Which component a bump increments.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Bump {
    Major,
    Minor,
    Patch,
}

/// Increments one component by `1 + r mod (u32::MAX − component)` (so by
/// at least 1 and never past `u32::MAX`) and resets the lower ones, as the
/// old driver does (`later.Major++; later.Minor, later.Patch = 0, 0`). `None`
/// when that component is already `u32::MAX`.
pub fn bump(base: Semver, kind: Bump, r: u32) -> Option<Semver> {
    let step = |c: u32| -> Option<u32> {
        let room = u32::MAX - c;
        if room == 0 {
            return None;
        }
        Some(c + 1 + (r % room))
    };
    match kind {
        Bump::Major => Some(Semver::new(step(base.major)?, 0, 0)),
        Bump::Minor => Some(Semver::new(base.major, step(base.minor)?, 0)),
        Bump::Patch => Some(Semver::new(base.major, base.minor, step(base.patch)?)),
    }
}

/// The next version after `base`: the drawn bump, else the first of
/// patch, minor, major that still has room. `None` only for
/// `u32::MAX.u32::MAX.u32::MAX`. The result is always strictly greater.
pub fn next_after(base: Semver, kind: Bump, r: u32) -> Option<Semver> {
    bump(base, kind, r).or_else(|| [Bump::Patch, Bump::Minor, Bump::Major].into_iter().find_map(|k| bump(base, k, r)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ordering_is_semver_ordering() {
        let v = |s: &str| Semver::parse(s).unwrap();
        assert!(v("1.0.0") < v("2.0.0"));
        assert!(v("1.10.0") > v("1.9.99"));
        assert!(v("1.2.3") < v("1.2.4"));
        assert!(v("0.4294967295.4294967295") < v("1.0.0"));
        assert!(v("10.0.0") > v("9.0.0"), "numeric, not lexical");
        assert_eq!(v("4294967295.1.2").to_string(), "4294967295.1.2");
        for bad in ["", "1", "1.2", "1.2.3.4", "a.b.c", "4294967296.0.0", "-1.0.0", "latest"] {
            assert_eq!(Semver::parse(bad), None, "{bad}");
        }
    }

    #[test]
    fn selectors() {
        let v = Semver::new(1, 2, 3);
        for s in ["", "latest", "1", "1.2", "1.2.3"] {
            assert!(v.matches(s), "{s}");
        }
        for s in ["2", "1.3", "1.2.4", "x", "1.2.3.4"] {
            assert!(!v.matches(s), "{s}");
        }
    }

    #[test]
    fn bumps_are_strictly_greater_and_stay_in_range() {
        let m = u32::MAX;
        let bases = [Semver::new(0, 0, 0), Semver::new(1, 0, 0), Semver::new(m - 1, m - 1, m - 1), Semver::new(m, m, m - 1), Semver::new(m, m - 1, m), Semver::new(m - 1, m, m), Semver::new(7, m, 3)];
        for base in bases {
            for kind in [Bump::Major, Bump::Minor, Bump::Patch] {
                for r in [0, 1, 2, 12345, m / 2, m - 1, m] {
                    if let Some(n) = bump(base, kind, r) {
                        assert!(n > base, "{base} {kind:?} {r} -> {n}");
                    }
                    let n = next_after(base, kind, r).expect("room somewhere");
                    assert!(n > base, "{base} {kind:?} {r} -> {n}");
                }
            }
        }
        assert_eq!(next_after(Semver::new(m, m, m), Bump::Major, 0), None);
        assert_eq!(bump(Semver::new(1, 2, 3), Bump::Major, 0), Some(Semver::new(2, 0, 0)));
        assert_eq!(bump(Semver::new(1, 2, 3), Bump::Minor, 0), Some(Semver::new(1, 3, 0)));
        assert_eq!(bump(Semver::new(1, 2, 3), Bump::Patch, 0), Some(Semver::new(1, 2, 4)));
        assert_eq!(bump(Semver::new(m - 1, 0, 0), Bump::Major, m), Some(Semver::new(m, 0, 0)));
        assert_eq!(bump(Semver::new(1, m, 0), Bump::Minor, 0), None);
        assert_eq!(next_after(Semver::new(1, m, 0), Bump::Minor, 0), Some(Semver::new(1, m, 1)));
    }
}
