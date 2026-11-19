//! Polymarket CLOB Execution, EIP-712 Signing, Heartbeats & Settlement Crate.
//!
//! Provides production execution actors, L2 HMAC signing, dual-protocol EIP-712 order signing,
//! heartbeat keep-alive, maker fill reconciliation, and settlement workers.

pub mod auth;
pub mod eip712;
pub mod error;

pub use auth::{L2Headers, L2HmacSigner, compute_l2_signature, strip_query_string};
pub use eip712::{
    CTF_EXCHANGE, CTF_NEG_RISK_EXCHANGE, DEPOSIT_WALLET_DOMAIN_NAME, DEPOSIT_WALLET_DOMAIN_VERSION,
    DepositWalletConstants, EXCHANGE_DOMAIN_NAME, MAX_SAFE_INTEGER, MarketDomain,
    ORDER_TYPE_STRING, Order, OrderSigner, POLY_V2_EXCHANGE, POLYGON_CHAIN_ID,
    TYPED_DATA_SIGN_TYPE_STRING, generate_salt,
};
pub use error::ExecutionError;
