use core_types::MathError;

/// Execution and settlement error variants for Polymarket CLOB operations.
#[derive(Debug, thiserror::Error)]
pub enum ExecutionError {
    /// Base64 decoding failed.
    #[error("Base64 decode error: {0}")]
    Base64(#[from] base64::DecodeError),

    /// HMAC key initialization failed.
    #[error("HMAC key error: {0}")]
    HmacKey(String),

    /// EIP-712 signing or signature recovery error.
    #[error("EIP-712 signing error: {0}")]
    Signing(String),

    /// REST API communication or protocol error.
    #[error("REST API error: {status_code:?}: {detail}")]
    Rest {
        status_code: Option<u16>,
        detail: String,
    },

    /// WebSocket communication or protocol error.
    #[error("WebSocket error: {0}")]
    WebSocket(String),

    /// Settlement execution error.
    #[error("Settlement error: {0}")]
    Settlement(String),

    /// Consecutive errors tripped execution circuit breaker.
    #[error("Circuit breaker tripped: {0}")]
    CircuitBreaker(String),

    /// JSON serialization or deserialization error.
    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    /// Reqwest HTTP transport error.
    #[error("HTTP transport error: {0}")]
    Reqwest(#[from] reqwest::Error),

    /// Mathematical or configuration error from core-types.
    #[error("Core math error: {0}")]
    Core(#[from] MathError),

    /// Channel send or receive error.
    #[error("Channel error: {0}")]
    Channel(String),

    /// Configuration or environment error.
    #[error("Config error: {0}")]
    Config(String),
}
