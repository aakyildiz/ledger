//! protojson helpers for the ledger's wire types: postings, `Uint256`
//! (four 64-bit limbs, `v0` least significant), `BigUint`/`SignedBigInt`
//! (big-endian magnitude bytes, base64) of any width, and accessors for the
//! fields families read from responses.
#![allow(dead_code)] // family-facing API

use base64::Engine;
use serde_json::{json, Map, Value};
use std::cmp::Ordering;

/// An unsigned 256-bit integer, as `common.Uint256` carries it.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub struct U256 {
    /// Little-endian limbs: `v[0]` is `v0`, the least significant.
    pub v: [u64; 4],
}

impl PartialOrd for U256 {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for U256 {
    fn cmp(&self, other: &Self) -> Ordering {
        for i in (0..4).rev() {
            match self.v[i].cmp(&other.v[i]) {
                Ordering::Equal => continue,
                o => return o,
            }
        }
        Ordering::Equal
    }
}

impl From<u64> for U256 {
    fn from(n: u64) -> Self {
        U256 { v: [n, 0, 0, 0] }
    }
}

impl U256 {
    pub const ZERO: U256 = U256 { v: [0; 4] };
    pub const MAX: U256 = U256 { v: [u64::MAX; 4] };

    pub fn from_limbs(v0: u64, v1: u64, v2: u64, v3: u64) -> Self {
        U256 { v: [v0, v1, v2, v3] }
    }

    /// `2^bit` for `bit < 256`.
    pub fn pow2(bit: u32) -> Self {
        assert!(bit < 256, "U256::pow2 out of range");
        let mut v = [0u64; 4];
        v[(bit / 64) as usize] = 1u64 << (bit % 64);
        U256 { v }
    }

    pub fn is_zero(&self) -> bool {
        self.v == [0; 4]
    }

    pub fn to_u64(&self) -> Option<u64> {
        if self.v[1] == 0 && self.v[2] == 0 && self.v[3] == 0 {
            Some(self.v[0])
        } else {
            None
        }
    }

    pub fn checked_add(&self, o: &U256) -> Option<U256> {
        let mut out = [0u64; 4];
        let mut carry = false;
        for (i, slot) in out.iter_mut().enumerate() {
            let (a, c1) = self.v[i].overflowing_add(o.v[i]);
            let (b, c2) = a.overflowing_add(carry as u64);
            *slot = b;
            carry = c1 || c2;
        }
        if carry {
            None
        } else {
            Some(U256 { v: out })
        }
    }

    pub fn checked_sub(&self, o: &U256) -> Option<U256> {
        let mut out = [0u64; 4];
        let mut borrow = false;
        for (i, slot) in out.iter_mut().enumerate() {
            let (a, b1) = self.v[i].overflowing_sub(o.v[i]);
            let (b, b2) = a.overflowing_sub(borrow as u64);
            *slot = b;
            borrow = b1 || b2;
        }
        if borrow {
            None
        } else {
            Some(U256 { v: out })
        }
    }

    /// `common.Uint256` protojson: non-zero limbs only, as decimal strings
    /// (protojson omits zero `fixed64` fields).
    pub fn to_json(&self) -> Value {
        let mut m = Map::new();
        for (i, limb) in self.v.iter().enumerate() {
            if *limb != 0 {
                m.insert(format!("v{i}"), Value::String(limb.to_string()));
            }
        }
        Value::Object(m)
    }

    /// Parses `common.Uint256` protojson (absent limbs are zero; limbs may be
    /// strings or numbers).
    pub fn from_json(v: &Value) -> Option<U256> {
        let o = v.as_object()?;
        let mut out = [0u64; 4];
        for (k, val) in o {
            let i = match k.as_str() {
                "v0" => 0,
                "v1" => 1,
                "v2" => 2,
                "v3" => 3,
                _ => return None,
            };
            out[i] = as_u64(val)?;
        }
        Some(U256 { v: out })
    }

    /// Big-endian bytes without leading zeros.
    pub fn to_be_bytes_trimmed(&self) -> Vec<u8> {
        let mut b = Vec::with_capacity(32);
        for i in (0..4).rev() {
            b.extend_from_slice(&self.v[i].to_be_bytes());
        }
        let first = b.iter().position(|x| *x != 0).unwrap_or(b.len());
        b.split_off(first)
    }

    /// From big-endian bytes; `None` above 256 bits.
    pub fn from_be_bytes(bytes: &[u8]) -> Option<U256> {
        let first = bytes.iter().position(|x| *x != 0).unwrap_or(bytes.len());
        let bytes = &bytes[first..];
        if bytes.len() > 32 {
            return None;
        }
        let mut padded = [0u8; 32];
        padded[32 - bytes.len()..].copy_from_slice(bytes);
        let mut v = [0u64; 4];
        for (i, limb) in v.iter_mut().enumerate() {
            let start = 32 - (i + 1) * 8;
            *limb = u64::from_be_bytes(padded[start..start + 8].try_into().unwrap());
        }
        Some(U256 { v })
    }

    /// Decimal rendering.
    pub fn to_decimal(&self) -> String {
        if self.is_zero() {
            return "0".to_string();
        }
        let mut limbs = self.v;
        let mut digits = Vec::new();
        while limbs != [0; 4] {
            // Divide by 10 limb by limb from the most significant.
            let mut rem: u128 = 0;
            for i in (0..4).rev() {
                let cur = (rem << 64) | limbs[i] as u128;
                limbs[i] = (cur / 10) as u64;
                rem = cur % 10;
            }
            digits.push(b'0' + rem as u8);
        }
        digits.reverse();
        String::from_utf8(digits).unwrap()
    }
}

/// An arbitrary-width non-negative integer (`common.BigUint` magnitude), as
/// big-endian bytes without leading zeros. Values beyond 128 bits are kept
/// exactly (the old helper truncated above 120 bits).
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub struct BigUint {
    pub be: Vec<u8>,
}

impl BigUint {
    pub fn from_be(bytes: &[u8]) -> Self {
        let first = bytes.iter().position(|x| *x != 0).unwrap_or(bytes.len());
        BigUint { be: bytes[first..].to_vec() }
    }
    pub fn is_zero(&self) -> bool {
        self.be.is_empty()
    }
    pub fn to_u256(&self) -> Option<U256> {
        U256::from_be_bytes(&self.be)
    }
    pub fn to_u128(&self) -> Option<u128> {
        if self.be.len() > 16 {
            return None;
        }
        Some(self.be.iter().fold(0u128, |acc, b| (acc << 8) | *b as u128))
    }
}

impl PartialOrd for BigUint {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for BigUint {
    fn cmp(&self, other: &Self) -> Ordering {
        self.be.len().cmp(&other.be.len()).then_with(|| self.be.cmp(&other.be))
    }
}

/// `common.SignedBigInt`.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub struct SignedBigInt {
    pub negative: bool,
    pub magnitude: BigUint,
}

/// Parses `common.BigUint` protojson `{"magnitude": "<b64>"}` (absent = 0).
pub fn parse_biguint_full(v: &Value) -> Option<BigUint> {
    let Some(m) = v.get("magnitude") else { return Some(BigUint::default()) };
    let bytes = base64::engine::general_purpose::STANDARD.decode(m.as_str()?).ok()?;
    Some(BigUint::from_be(&bytes))
}

/// Parses `common.SignedBigInt` protojson
/// `{"negative": bool, "magnitude": {"magnitude": "<b64>"}}`.
pub fn parse_signed_full(v: &Value) -> Option<SignedBigInt> {
    let magnitude = match v.get("magnitude") {
        Some(m) => parse_biguint_full(m)?,
        None => BigUint::default(),
    };
    let negative = v.get("negative").and_then(Value::as_bool).unwrap_or(false) && !magnitude.is_zero();
    Some(SignedBigInt { negative, magnitude })
}

/// `common.BigUint` protojson for a magnitude.
pub fn biguint_json(b: &BigUint) -> Value {
    if b.is_zero() {
        json!({})
    } else {
        json!({"magnitude": base64::engine::general_purpose::STANDARD.encode(&b.be)})
    }
}

/// common.BigUint as an i128 when it fits (compatibility helper).
pub fn parse_biguint(v: &Value) -> Option<i128> {
    let b = parse_biguint_full(v)?;
    let n = b.to_u128()?;
    i128::try_from(n).ok()
}

/// common.SignedBigInt as an i128 when it fits (compatibility helper).
pub fn parse_signed(v: &Value) -> Option<i128> {
    let s = parse_signed_full(v)?;
    let n = i128::try_from(s.magnitude.to_u128()?).ok()?;
    Some(if s.negative { -n } else { n })
}

/// A posting as the driver models it.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Posting {
    pub source: String,
    pub destination: String,
    pub amount: U256,
    pub asset: String,
}

impl Posting {
    pub fn new(source: &str, destination: &str, amount: impl Into<U256>, asset: &str) -> Self {
        Posting { source: source.to_string(), destination: destination.to_string(), amount: amount.into(), asset: asset.to_string() }
    }

    pub fn to_json(&self) -> Value {
        json!({
            "source": self.source,
            "destination": self.destination,
            "amount": self.amount.to_json(),
            "asset": self.asset,
        })
    }

    pub fn from_json(v: &Value) -> Option<Posting> {
        Some(Posting {
            source: v.get("source")?.as_str()?.to_string(),
            destination: v.get("destination")?.as_str()?.to_string(),
            amount: U256::from_json(v.get("amount").unwrap_or(&json!({})))?,
            asset: v.get("asset")?.as_str()?.to_string(),
        })
    }
}

/// common.Uint256 for a u64 amount.
pub fn uint256(n: u64) -> Value {
    U256::from(n).to_json()
}

/// Parses a common.Uint256 that fits in a u64.
pub fn parse_uint256(v: &Value) -> Option<u64> {
    U256::from_json(v)?.to_u64()
}

/// A fixed64/uint64 field: protojson writes 64-bit integers as strings.
pub fn as_u64(v: &Value) -> Option<u64> {
    match v {
        Value::String(s) => s.parse().ok(),
        Value::Number(n) => n.as_u64(),
        _ => None,
    }
}

/// A u64 field that may be absent (proto3 default 0).
pub fn u64_field(v: &Value, key: &str) -> u64 {
    v.get(key).and_then(as_u64).unwrap_or(0)
}

pub fn str_field<'a>(v: &'a Value, key: &str) -> &'a str {
    v.get(key).and_then(Value::as_str).unwrap_or("")
}

pub fn string_meta(s: &str) -> Value {
    json!({ "stringValue": s })
}

/// One `CreateTransaction` request (`servicepb.Request` protojson).
pub fn create_tx_request(ledger: &str, postings: &[Posting], reference: Option<&str>) -> Value {
    let mut ct = json!({ "postings": postings.iter().map(Posting::to_json).collect::<Vec<_>>() });
    if let Some(r) = reference {
        ct["reference"] = json!(r);
    }
    json!({ "apply": { "ledger": ledger, "action": { "createTransaction": ct } } })
}

/// The `ApplyRequest` body for a batch (`{"unsigned": ApplyBatch}`).
pub fn apply_request(requests: &[Value], idempotency_key: Option<&str>) -> Value {
    let mut batch = json!({ "requests": requests });
    if let Some(k) = idempotency_key {
        batch["idempotencyKey"] = json!(k);
    }
    json!({ "unsigned": batch })
}

/// The logs of an Apply response.
pub fn apply_logs(resp: &Value) -> Vec<Value> {
    resp.get("logs").and_then(Value::as_array).cloned().unwrap_or_default()
}

/// The created transaction of an Apply log, if it is one.
pub fn created_transaction(log: &Value) -> Option<&Value> {
    log.pointer("/payload/apply/log/data/createdTransaction/transaction")
}

/// The revert transaction of an Apply log, if it is one.
pub fn reverted_transaction(log: &Value) -> Option<&Value> {
    log.pointer("/payload/apply/log/data/revertedTransaction")
}

pub fn order_skipped(log: &Value) -> Option<&Value> {
    log.pointer("/payload/apply/log/data/orderSkipped")
}

pub fn postings_of(tx: &Value) -> Option<Vec<Posting>> {
    tx.get("postings")
        .and_then(Value::as_array)
        .map(|ps| ps.iter().map(Posting::from_json).collect::<Option<Vec<_>>>())
        .unwrap_or(Some(vec![]))
}

/// Volumes (input, output, balance) of one account volume entry, exact.
pub fn volumes_full(v: &Value) -> Option<(BigUint, BigUint, SignedBigInt)> {
    let vols = v.get("volumes")?;
    Some((
        parse_biguint_full(vols.get("input").unwrap_or(&Value::Null)).unwrap_or_default(),
        parse_biguint_full(vols.get("output").unwrap_or(&Value::Null)).unwrap_or_default(),
        parse_signed_full(vols.get("balance").unwrap_or(&Value::Null)).unwrap_or_default(),
    ))
}

/// Volumes (input, output, balance) of one account volume entry, when they
/// fit in an i128.
pub fn volumes(v: &Value) -> Option<(i128, i128, i128)> {
    let vols = v.get("volumes")?;
    Some((
        parse_biguint(vols.get("input").unwrap_or(&Value::Null)).or(Some(0))?,
        parse_biguint(vols.get("output").unwrap_or(&Value::Null)).or(Some(0))?,
        parse_signed(vols.get("balance").unwrap_or(&Value::Null)).or(Some(0))?,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn u256_round_trips_including_the_maximum() {
        for v in [U256::ZERO, U256::from(1), U256::from(u64::MAX), U256::pow2(64), U256::pow2(127), U256::pow2(255), U256::MAX, U256::from_limbs(1, 2, 3, 4)] {
            let j = v.to_json();
            assert_eq!(U256::from_json(&j), Some(v), "{j}");
            assert_eq!(U256::from_be_bytes(&v.to_be_bytes_trimmed()), Some(v));
        }
        assert_eq!(
            U256::MAX.to_json(),
            json!({"v0": "18446744073709551615", "v1": "18446744073709551615", "v2": "18446744073709551615", "v3": "18446744073709551615"})
        );
        assert_eq!(U256::ZERO.to_json(), json!({}));
        assert_eq!(U256::from_json(&json!({"v0": 5, "v2": "7"})), Some(U256::from_limbs(5, 0, 7, 0)));
        assert_eq!(U256::from_json(&json!({"v9": "1"})), None);
        assert_eq!(
            U256::MAX.to_decimal(),
            "115792089237316195423570985008687907853269984665640564039457584007913129639935"
        );
    }

    #[test]
    fn u256_checked_arithmetic() {
        assert_eq!(U256::MAX.checked_add(&U256::from(1)), None);
        assert_eq!(U256::pow2(255).checked_add(&U256::pow2(255)), None);
        assert_eq!(U256::from(u64::MAX).checked_add(&U256::from(1)), Some(U256::pow2(64)));
        assert_eq!(U256::pow2(64).checked_sub(&U256::from(1)), Some(U256::from(u64::MAX)));
        assert_eq!(U256::ZERO.checked_sub(&U256::from(1)), None);
        assert_eq!(U256::MAX.checked_sub(&U256::MAX), Some(U256::ZERO));
        assert!(U256::pow2(200) > U256::from(u64::MAX));
    }

    #[test]
    fn biguint_beyond_127_bits_is_exact() {
        let big = U256::pow2(130).checked_add(&U256::from(3)).unwrap();
        let j = biguint_json(&BigUint::from_be(&big.to_be_bytes_trimmed()));
        let parsed = parse_biguint_full(&j).unwrap();
        assert_eq!(parsed.to_u256(), Some(big));
        assert_eq!(parsed.to_u128(), None);
        assert_eq!(parse_biguint(&j), None, "the i128 helper reports overflow instead of truncating");
        let s = parse_signed_full(&json!({"negative": true, "magnitude": j})).unwrap();
        assert!(s.negative);
        assert_eq!(s.magnitude.to_u256(), Some(big));
        // 2^127 exactly: does not fit an i128.
        let j127 = biguint_json(&BigUint::from_be(&U256::pow2(127).to_be_bytes_trimmed()));
        assert_eq!(parse_biguint_full(&j127).unwrap().to_u128(), Some(1u128 << 127));
        assert_eq!(parse_biguint(&j127), None);
        // The fixture form: 100.
        assert_eq!(parse_biguint(&json!({"magnitude": "ZA=="})), Some(100));
        assert_eq!(parse_signed(&json!({"magnitude": {"magnitude": "ZA=="}})), Some(100));
        assert_eq!(parse_biguint(&json!({})), Some(0));
    }

    #[test]
    fn postings_round_trip() {
        let p = Posting::new("world", "users:1", U256::pow2(255), "COIN");
        assert_eq!(Posting::from_json(&p.to_json()), Some(p));
    }
}
