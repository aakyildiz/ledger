//! protojson helpers for the ledger's wire types.
use base64::Engine;
use serde_json::{json, Value};

/// A posting as the driver models it.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Posting {
    pub source: String,
    pub destination: String,
    pub amount: u64,
    pub asset: String,
}

impl Posting {
    pub fn to_json(&self) -> Value {
        json!({
            "source": self.source,
            "destination": self.destination,
            "amount": uint256(self.amount),
            "asset": self.asset,
        })
    }

    /// Parses a common.Posting. Amounts above u64 are reported as None.
    pub fn from_json(v: &Value) -> Option<Posting> {
        Some(Posting {
            source: v.get("source")?.as_str()?.to_string(),
            destination: v.get("destination")?.as_str()?.to_string(),
            amount: parse_uint256(v.get("amount")?)?,
            asset: v.get("asset")?.as_str()?.to_string(),
        })
    }
}

/// common.Uint256 for a u64 amount.
pub fn uint256(n: u64) -> Value {
    json!({ "v0": n.to_string() })
}

/// Parses a common.Uint256 that fits in a u64.
pub fn parse_uint256(v: &Value) -> Option<u64> {
    for limb in ["v1", "v2", "v3"] {
        if v.get(limb).and_then(as_u64).unwrap_or(0) != 0 {
            return None;
        }
    }
    Some(v.get("v0").and_then(as_u64).unwrap_or(0))
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

/// common.BigUint: big-endian magnitude bytes, base64 in protojson.
pub fn parse_biguint(v: &Value) -> Option<i128> {
    let Some(m) = v.get("magnitude") else { return Some(0) };
    let bytes = base64::engine::general_purpose::STANDARD.decode(m.as_str()?).ok()?;
    if bytes.len() > 15 {
        return None;
    }
    Some(bytes.iter().fold(0i128, |acc, b| (acc << 8) | *b as i128))
}

/// common.SignedBigInt.
pub fn parse_signed(v: &Value) -> Option<i128> {
    let mag = match v.get("magnitude") {
        Some(m) => parse_biguint(m)?,
        None => 0,
    };
    let neg = v.get("negative").and_then(Value::as_bool).unwrap_or(false);
    Some(if neg { -mag } else { mag })
}

pub fn str_field<'a>(v: &'a Value, key: &str) -> &'a str {
    v.get(key).and_then(Value::as_str).unwrap_or("")
}

pub fn string_meta(s: &str) -> Value {
    json!({ "stringValue": s })
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

/// Volumes (input, output, balance) of one account volume entry.
pub fn volumes(v: &Value) -> Option<(i128, i128, i128)> {
    let vols = v.get("volumes")?;
    Some((
        parse_biguint(vols.get("input").unwrap_or(&Value::Null)).or(Some(0))?,
        parse_biguint(vols.get("output").unwrap_or(&Value::Null)).or(Some(0))?,
        parse_signed(vols.get("balance").unwrap_or(&Value::Null)).or(Some(0))?,
    ))
}
