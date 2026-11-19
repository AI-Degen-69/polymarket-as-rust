//! Event-driven FIFO queue-aware paper trading simulator and replay harness
//! for Polymarket 5-minute binary prediction markets.
//!
//! Provides top-of-book and L2 FIFO matching engine simulation, wire and cancel
//! flight latency modeling, cash/inventory/fee accounting with VWAP pair merges,
//! and baseline comparative telemetry.

#![forbid(unsafe_code)]
#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::indexing_slicing
    )
)]

/// Cash, inventory, fee, and atomic VWAP pair-merge accounting ledger.
pub mod accounting;
/// Typed error definitions for the paper simulation crate.
pub mod error;
/// Strict FIFO queue priority matching engine with simulated wire latency.
pub mod fifo_book;
/// Performance, fill ratio, markout, and null-baseline comparative telemetry.
pub mod telemetry;

pub use accounting::{MergeEvent, PaperLedger};
pub use error::PaperSimError;
pub use fifo_book::{
    FifoBrokerConfig, FifoPaperBroker, NewOrderParams, QueueDecayPolicy, SimFill, SimOrder,
    SimOrderStatus,
};
pub use telemetry::{
    BaselineTelemetry, FillMarkoutProbe, NaiveBaselineEngine, NaiveQuotesInput, TrajectorySnapshot,
};
