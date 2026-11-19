use core_types::{DecimalFracPolicy, OrderSide, Outcome, ParseDecimalError, parse_decimal_scaled};
use serde::Deserialize;

use crate::error::FeedError;

/// Integer fixed-point multiplier for 8-decimal Binance prices and quantities (`1.00000000 = 100_000_000`).
pub const E8_SCALE: u64 = 100_000_000;

/// Integer fixed-point multiplier for 6-decimal Polymarket prices and shares (`1.000000 = 1_000_000`).
pub const E6_SCALE: u64 = 1_000_000;

/// Parses a non-negative ASCII decimal string into integer `10^8`-scaled units (e.g., Binance `@bookTicker`/`@aggTrade`).
pub fn parse_decimal_e8(bytes: &[u8]) -> Result<u64, ParseDecimalError> {
    parse_decimal_scaled(bytes, 8, E8_SCALE, DecimalFracPolicy::Strict)
}

/// Parses a non-negative high-precision ASCII decimal string (e.g., 18-decimal Chainlink feed) into `10^8` units.
pub fn parse_decimal_e8_truncating(bytes: &[u8]) -> Result<u64, ParseDecimalError> {
    parse_decimal_scaled(bytes, 8, E8_SCALE, DecimalFracPolicy::TruncateExtra)
}

/// Parses a non-negative ASCII decimal into integer `10^6`-scaled micro-units, allowing extra trailing `'0'` padding.
pub fn parse_decimal_micro_flexible(bytes: &[u8]) -> Result<u64, ParseDecimalError> {
    parse_decimal_scaled(bytes, 6, E6_SCALE, DecimalFracPolicy::AllowTrailingZeros)
}

/// Parses an RFC 3339 timestamp string into non-negative Unix milliseconds.
pub fn parse_rfc3339_ms(raw: &str) -> Result<u64, FeedError> {
    let dt = chrono::DateTime::parse_from_rfc3339(raw.trim()).map_err(|e| {
        FeedError::TimestampParse {
            raw: raw.to_string(),
            detail: e.to_string(),
        }
    })?;
    let ms = dt.timestamp_millis();
    u64::try_from(ms).map_err(|_| FeedError::TimestampParse {
        raw: raw.to_string(),
        detail: format!("negative Unix timestamp ({ms} ms)"),
    })
}

/// Maps Polymarket outcome labels (`"Up"`, `"Yes"`, `"Down"`, `"No"`) case-insensitively.
#[must_use]
pub fn parse_outcome_label(label: &str) -> Option<Outcome> {
    if label.trim().eq_ignore_ascii_case("up") || label.trim().eq_ignore_ascii_case("yes") {
        Some(Outcome::Up)
    } else if label.trim().eq_ignore_ascii_case("down") || label.trim().eq_ignore_ascii_case("no") {
        Some(Outcome::Down)
    } else {
        None
    }
}

/// Maps Polymarket order side strings (`"BUY"`, `"SELL"`) case-insensitively.
#[must_use]
pub fn parse_order_side(raw: &str) -> Option<OrderSide> {
    if raw.trim().eq_ignore_ascii_case("buy") || raw.trim().eq_ignore_ascii_case("bid") {
        Some(OrderSide::Buy)
    } else if raw.trim().eq_ignore_ascii_case("sell") || raw.trim().eq_ignore_ascii_case("ask") {
        Some(OrderSide::Sell)
    } else {
        None
    }
}

/// Deserializes a JSON field encoded either as a `u64` integer or string integer.
pub(crate) fn de_string_or_u64<'de, D>(deserializer: D) -> Result<u64, D::Error>
where
    D: serde::Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum StringOrInt {
        Num(u64),
        Text(String),
    }

    match StringOrInt::deserialize(deserializer)? {
        StringOrInt::Num(v) => Ok(v),
        StringOrInt::Text(s) => s.trim().parse::<u64>().map_err(serde::de::Error::custom),
    }
}

/// Deserializes an optional JSON field encoded as a `u64`, string integer, or `null`.
pub(crate) fn de_opt_string_or_u64<'de, D>(deserializer: D) -> Result<Option<u64>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum OptStringOrInt {
        Null,
        Num(u64),
        Text(String),
    }

    match Option::<OptStringOrInt>::deserialize(deserializer)? {
        None | Some(OptStringOrInt::Null) => Ok(None),
        Some(OptStringOrInt::Num(v)) => Ok(Some(v)),
        Some(OptStringOrInt::Text(s)) => {
            let trimmed = s.trim();
            if trimmed.is_empty() {
                Ok(None)
            } else {
                trimmed
                    .parse::<u64>()
                    .map(Some)
                    .map_err(serde::de::Error::custom)
            }
        }
    }
}

/// Deserializes either a JSON string array (`["a", "b"]`) or a JSON-encoded string (`"[\"a\",\"b\"]"`).
pub(crate) fn de_string_vec_or_json_str<'de, D>(deserializer: D) -> Result<Vec<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum ArrayOrEncoded {
        Vec(Vec<String>),
        Encoded(String),
    }

    match Option::<ArrayOrEncoded>::deserialize(deserializer)? {
        None => Ok(Vec::new()),
        Some(ArrayOrEncoded::Vec(v)) => Ok(v),
        Some(ArrayOrEncoded::Encoded(s)) => {
            let trimmed = s.trim();
            if trimmed.is_empty() {
                Ok(Vec::new())
            } else {
                serde_json::from_str::<Vec<String>>(trimmed).map_err(serde::de::Error::custom)
            }
        }
    }
}

/// Deserializes an optional decimal string or number into a string representation for exact fixed-point parsing.
pub(crate) fn de_opt_decimal_as_string<'de, D>(deserializer: D) -> Result<Option<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum DecimalVal {
        Text(String),
        Num(serde_json::Number),
    }

    match Option::<DecimalVal>::deserialize(deserializer)? {
        None => Ok(None),
        Some(DecimalVal::Text(s)) => {
            let t = s.trim();
            Ok((!t.is_empty()).then(|| t.to_string()))
        }
        Some(DecimalVal::Num(n)) => Ok(Some(n.to_string())),
    }
}
