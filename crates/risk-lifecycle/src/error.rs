use thiserror::Error;

/// Typed error conditions encountered across Stations 7–8, lifecycle FSM, and runtime actors.
#[derive(Debug, Error, PartialEq, Eq, Clone)]
pub enum RiskLifecycleError {
    /// Attempted to compute sizing or limits with a non-positive or zero contract price.
    #[error("Price must be strictly positive: {0:?}")]
    NonPositivePrice(String),

    /// Price is not finite or exceeds one dollar.
    #[error("Invalid contract price: {0}")]
    InvalidPrice(String),

    /// Configuration parameters violate boundary constraints.
    #[error("Invalid configuration: {0}")]
    InvalidConfig(String),

    /// Circuit breaker tripped and blocked execution.
    #[error("Circuit breaker tripped: {0}")]
    CircuitBreakerTripped(String),

    /// Channel send failed because receiver was dropped.
    #[error("Actor communication channel closed")]
    ChannelClosed,

    /// Order registry desync or illegal state transition.
    #[error("FSM or registry error: {0}")]
    StateError(String),
}
