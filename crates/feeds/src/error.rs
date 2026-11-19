use core_types::{ClockError, MathError, Outcome, ParseDecimalError, TelemetryError, WireError};
use thiserror::Error;

/// Typed errors produced by market discovery, orderbook maintenance, and WebSocket ingestion actors.
#[derive(Debug, Error)]
pub enum FeedError {
    /// Outbound REST request failed.
    #[error("HTTP request failed on {endpoint}: {source}")]
    Http {
        /// Endpoint URL or logical operation label.
        endpoint: String,
        /// Underlying HTTP client error.
        #[source]
        source: reqwest::Error,
    },
    /// Upstream HTTP API returned a non-success status code.
    #[error("HTTP {status} from {endpoint}: {body_snippet}")]
    HttpStatus {
        /// Endpoint URL or logical operation label.
        endpoint: String,
        /// HTTP status code.
        status: u16,
        /// Truncated response body snippet.
        body_snippet: String,
    },
    /// JSON payload decoding failed.
    #[error("JSON decode error in {context}: {source}")]
    Json {
        /// Parser context identifier.
        context: &'static str,
        /// Underlying serde JSON error.
        #[source]
        source: serde_json::Error,
    },
    /// Fixed-point decimal parsing failed.
    #[error("decimal parse error in {field}: {source}")]
    ParseDecimal {
        /// Logical field name.
        field: &'static str,
        /// Underlying fixed-point parse error.
        #[source]
        source: ParseDecimalError,
    },
    /// RFC 3339 or integer exchange timestamp could not be parsed.
    #[error("timestamp parse error for `{raw}`: {detail}")]
    TimestampParse {
        /// Offending timestamp string.
        raw: String,
        /// Parse failure reason.
        detail: String,
    },
    /// Discovery could not locate any valid 5m BTC market within the remaining window.
    #[error("no active 5-minute BTC market found: {detail}")]
    DiscoveryEmpty {
        /// Contextual search summary.
        detail: String,
    },
    /// Discovered Gamma or CLOB market metadata violated structural invariants.
    #[error("invalid discovered market metadata: {detail}")]
    InvalidMarket {
        /// Structural violation description.
        detail: String,
    },
    /// Maintained orderbook entered a crossed state (`best_bid >= best_ask`).
    #[error(
        "crossed {outcome:?} order book: best_bid={best_bid_micro} >= best_ask={best_ask_micro}"
    )]
    BookCrossed {
        /// Affected market outcome.
        outcome: Outcome,
        /// Best bid price in micro-USDC.
        best_bid_micro: u64,
        /// Best ask price in micro-USDC.
        best_ask_micro: u64,
    },
    /// Monotonic clock or offset estimator failure.
    #[error("clock error: {0}")]
    Clock(#[from] ClockError),
    /// Math invariant failure from `core-types`.
    #[error("math domain error: {0}")]
    Math(#[from] MathError),
    /// Wire encoding or fee schedule failure from `core-types`.
    #[error("wire domain error: {0}")]
    Wire(#[from] WireError),
    /// Telemetry histogram or JSONL I/O error.
    #[error("telemetry error: {0}")]
    Telemetry(#[from] TelemetryError),
    /// WebSocket transport or protocol error.
    #[error("WebSocket error on {feed}: {detail}")]
    WebSocket {
        /// Logical feed name.
        feed: &'static str,
        /// Failure detail.
        detail: String,
    },
    /// Clock synchronization against Binance and Polymarket REST endpoints failed.
    #[error("clock sync failed: {detail}")]
    ClockSync {
        /// Contextual failure reason.
        detail: String,
    },
    /// Local filesystem or OS I/O error.
    #[error("I/O error on {context}: {source}")]
    Io {
        /// Context path or action.
        context: String,
        /// Underlying OS I/O error.
        #[source]
        source: std::io::Error,
    },
}

/// Truncates upstream response snippets to keep error messages bounded.
pub(crate) fn sanitize_snippet(raw: &str) -> String {
    const MAX_LEN: usize = 240;
    let trimmed = raw.trim();
    if trimmed.len() <= MAX_LEN {
        trimmed.to_string()
    } else {
        let mut out: String = trimmed.chars().take(MAX_LEN).collect();
        out.push_str("...");
        out
    }
}
