use std::fmt;
use std::path::PathBuf;

use thiserror::Error;

/// Errors returned while parsing ASCII decimal slices into fixed-point micro-units.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ParseDecimalError {
    /// Input slice was empty.
    #[error("decimal input is empty")]
    Empty,
    /// Input contained a non-digit, non-dot byte.
    #[error("invalid byte 0x{byte:02x} at index {position}")]
    InvalidByte {
        /// Raw byte value encountered.
        byte: u8,
        /// Zero-based byte offset in input slice.
        position: usize,
    },
    /// Input contained more than one decimal point.
    #[error("multiple decimal points in input")]
    MultipleDecimalPoints,
    /// Input had a decimal point without leading or trailing digits.
    #[error("malformed decimal point placement")]
    MalformedDecimalPoint,
    /// Input exceeded six fractional digits.
    #[error("fractional precision {count} exceeds 6 decimals")]
    TooManyFractionalDigits {
        /// Number of fractional digits parsed.
        count: usize,
    },
    /// Parsed value overflowed `u64`.
    #[error("decimal value overflowed u64 micro-units")]
    Overflow,
}

/// Errors returned by bounded floating-point, book-validation, and tick-quantization helpers.
#[derive(Debug, Clone, PartialEq, Error)]
pub enum MathError {
    /// Floating-point input was `NaN` or infinite.
    #[error("non-finite floating-point value: {value}")]
    NonFiniteInput {
        /// Offending non-finite float value.
        value: f64,
    },
    /// Input failed strict positivity invariant.
    #[error("expected strictly positive finite value, got {value}")]
    NonPositiveFinite {
        /// Offending float value (`<= 0.0`).
        value: f64,
    },
    /// Price exceeded supported numeric bounds before micro conversion.
    #[error("price {value} out of supported range [0, 1_000_000]")]
    OutOfRangePrice {
        /// Offending dollar price.
        value: f64,
    },
    /// Tick size was zero or exceeded `$1.00`.
    #[error("invalid tick size {tick_micro} micro-USDC")]
    InvalidTickSize {
        /// Invalid tick size in micro-USDC.
        tick_micro: u64,
    },
    /// Book snapshot had unsorted, duplicate, or zero-size levels.
    #[error("invalid {side} book level at index {index}: {reason}")]
    InvalidBookLevel {
        /// Side of book (`"bid"` or `"ask"`).
        side: &'static str,
        /// Zero-based level index.
        index: usize,
        /// Structural violation reason.
        reason: &'static str,
    },
    /// Order book best bid is greater than or equal to best ask.
    #[error("crossed order book: best_bid={best_bid_micro} >= best_ask={best_ask_micro}")]
    CrossedBook {
        /// Best bid price in micro-USDC.
        best_bid_micro: u64,
        /// Best ask price in micro-USDC.
        best_ask_micro: u64,
    },
    /// Market descriptor failed structural validation.
    #[error("invalid market descriptor: {detail}")]
    InvalidMarketDescriptor {
        /// Validation failure description.
        detail: String,
    },
    /// Integer conversion or addition overflowed.
    #[error("arithmetic overflow in math conversion")]
    Overflow,
}

/// Errors returned when validating or encoding CLOB wire quantities.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum WireError {
    /// Market tick size does not match Polymarket's supported matrix.
    #[error("unsupported tick size {tick_micro} micro-USDC")]
    UnsupportedTickSize {
        /// Unsupported tick size in micro-USDC.
        tick_micro: u64,
    },
    /// Order price violated open `(0.0, 1.0)` or closed `[0.0, 1.0]` USDC bounds.
    #[error("price {price_micro} micro-USDC out of binary bounds: {bound_desc}")]
    PriceOutOfBounds {
        /// Price value in micro-USDC.
        price_micro: u64,
        /// Exact bound constraint violated.
        bound_desc: &'static str,
    },
    /// Price is not an exact integer multiple of `tick_micro`.
    #[error("price {price_micro} is not aligned to tick {tick_micro}")]
    PriceNotTickAligned {
        /// Price value in micro-USDC.
        price_micro: u64,
        /// Market tick increment in micro-USDC.
        tick_micro: u64,
    },
    /// Rounded share count fell to zero after applying size decimals.
    #[error("rounded order size is zero from raw {raw_shares_micro} micro-shares")]
    ZeroRoundedSize {
        /// Unrounded micro-shares input.
        raw_shares_micro: u64,
    },
    /// Fee rate coefficient exceeded maximum (`1.0` = `1_000_000` micro-units).
    #[error("fee rate {fee_rate_e6} exceeds 1_000_000 micro-units")]
    InvalidFeeRate {
        /// Invalid fee coefficient scaled by `10^6`.
        fee_rate_e6: u64,
    },
    /// Fixed-point fee or wire multiplication/addition overflowed.
    #[error("arithmetic overflow in wire encoding")]
    ArithmeticOverflow,
}

/// Errors returned while loading or validating runtime configuration.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ConfigError {
    /// Loading `.env` failed with line content redacted to avoid printing secrets.
    #[error("failed to load .env file: {detail}")]
    EnvFile {
        /// Sanitized `.env` parse or I/O detail.
        detail: String,
    },
    /// An environment variable failed UTF-8, parsing, or format validation.
    #[error("invalid config key {key}='{value}': {reason}")]
    InvalidValue {
        /// Environment variable key name.
        key: &'static str,
        /// Sanitized or non-secret input value.
        value: String,
        /// Failure reason.
        reason: String,
    },
    /// Cross-field invariant failed.
    #[error("invalid config invariant: {detail}")]
    InvalidInvariant {
        /// Invariant failure description.
        detail: String,
    },
}

impl ConfigError {
    /// Maps a `dotenvy::Error` to a sanitized `ConfigError::EnvFile`, stripping line content on parse errors.
    #[must_use]
    pub fn from_dotenvy(err: &dotenvy::Error) -> Self {
        let detail = match err {
            dotenvy::Error::LineParse(_, col) => {
                format!("malformed .env syntax near byte offset {col} (line content redacted)")
            }
            dotenvy::Error::Io(io_err) => format!("I/O error reading .env: {io_err}"),
            _ => "unexpected .env error (content redacted)".to_string(),
        };
        Self::EnvFile { detail }
    }
}

/// Errors returned by monotonic/system clock operations and Windows timer APIs.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ClockError {
    /// System wall clock preceded Unix epoch by `behind_ms`.
    #[error("system clock precedes Unix epoch by {behind_ms} ms")]
    SystemTimeBeforeEpoch {
        /// Milliseconds before 1970-01-01T00:00:00Z.
        behind_ms: u128,
    },
    /// Millisecond or nanosecond timestamp exceeded `u64::MAX`.
    #[error("clock timestamp overflowed u64 range")]
    TimestampOverflow,
    /// `ClockOffsetEstimator` has not yet recorded an exchange time sample.
    #[error("exchange clock offset is unsynced")]
    UnsyncedClock,
    /// `exchange - local` delta exceeded signed `i64` millisecond range.
    #[error("exchange clock offset exceeded i64 range")]
    OffsetOverflow,
    /// `timeBeginPeriod` returned a non-zero multimedia timer error code.
    #[error("Windows timeBeginPeriod failed with code {code}")]
    WindowsTimerFailed {
        /// Win32 MMRESULT error code.
        code: u32,
    },
    /// `QueryPerformanceFrequency` returned failure or zero frequency.
    #[error("Windows QueryPerformanceFrequency unavailable (os_err={os_error})")]
    QpcUnavailable {
        /// OS error string from `std::io::Error::last_os_error()`.
        os_error: String,
    },
}

/// Errors returned by HDR latency tracking or JSONL telemetry serialization.
#[derive(Debug, Error)]
pub enum TelemetryError {
    /// HdrHistogram initialization failed.
    #[error("histogram creation failed: {0}")]
    HistogramInit(String),
    /// Recording a latency value into HdrHistogram failed.
    #[error("histogram record failed: {0}")]
    HistogramRecord(String),
    /// Underlying file I/O failed at `path`.
    #[error("telemetry I/O failed at '{path}': {source}")]
    Io {
        /// File path associated with the failed I/O operation.
        path: PathBuf,
        /// Underlying OS I/O error.
        source: std::io::Error,
    },
    /// JSON serialization failed.
    #[error("telemetry JSON serialization failed: {0}")]
    Json(#[from] serde_json::Error),
}

/// Redacts secret presence into a static marker string.
#[must_use]
pub(crate) const fn redact_presence(is_present: bool) -> &'static str {
    if is_present { "[REDACTED]" } else { "None" }
}

/// Formats fixed 6-decimal micro-units into an ASCII decimal buffer.
pub(crate) fn fmt_micro_units(f: &mut fmt::Formatter<'_>, raw: u64) -> fmt::Result {
    let whole = raw / 1_000_000;
    let frac = raw % 1_000_000;
    write!(f, "{whole}.{frac:06}")
}
