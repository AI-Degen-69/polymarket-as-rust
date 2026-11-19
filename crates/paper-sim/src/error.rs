use core_types::{MicroUsdc, SharesMicro, WireError};
use thiserror::Error;

/// Typed errors emitted by the paper simulator matching engine and ledger.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum PaperSimError {
    /// Maker BUY order crosses the current best ask (post-only rejection).
    #[error("post-only crossed: buy price {price} >= best ask {best_ask}")]
    PostOnlyCrossed {
        price: MicroUsdc,
        best_ask: MicroUsdc,
    },

    /// Maker SELL order crosses the current best bid (post-only rejection).
    #[error("post-only crossed sell: sell price {price} <= best bid {best_bid}")]
    PostOnlyCrossedSell {
        price: MicroUsdc,
        best_bid: MicroUsdc,
    },

    /// Simulated order ID was not found in the resting book.
    #[error("order not found: sim_id={sim_id}")]
    OrderNotFound { sim_id: u64 },

    /// Order is not in a cancellable state.
    #[error("order cannot be cancelled in state `{status}`: sim_id={sim_id}")]
    InvalidCancelState { sim_id: u64, status: &'static str },

    /// Order price is zero or exceeds $1.00.
    #[error("invalid order price: {price}")]
    InvalidOrderPrice { price: MicroUsdc },

    /// Order size is zero.
    #[error("invalid order size: {shares}")]
    InvalidOrderSize { shares: SharesMicro },

    /// Arithmetic overflow in monetary or share calculations.
    #[error("arithmetic overflow in paper simulator calculation")]
    ArithmeticOverflow,

    /// Propagated core-types wire error.
    #[error(transparent)]
    Wire(#[from] WireError),
}
