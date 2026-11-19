//! Polymarket CLOB Execution, EIP-712 Signing, Heartbeats & Settlement Crate.
//!
//! Provides production execution actors, L2 HMAC signing, dual-protocol EIP-712 order signing,
//! heartbeat keep-alive, maker fill reconciliation, and settlement workers.

pub mod auth;
pub mod error;

pub use auth::{L2Headers, L2HmacSigner, compute_l2_signature, strip_query_string};
pub use error::ExecutionError;
