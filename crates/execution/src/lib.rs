//! Polymarket CLOB Execution, EIP-712 Signing, Heartbeats & Settlement Crate.
//!
//! Provides production execution actors, L2 HMAC signing, dual-protocol EIP-712 order signing,
//! heartbeat keep-alive, maker fill reconciliation, and settlement workers.

pub mod auth;
pub mod clob_rest;
pub mod eip712;
pub mod error;
pub mod heartbeat;
pub mod settlement;
pub mod user_ws;

pub use auth::{L2Headers, L2HmacSigner, compute_l2_signature, strip_query_string};
pub use clob_rest::{
    CancelResponse, ClobApiError, ClobRestClient, DEFAULT_CLOB_REST_URL, MAX_BATCH_SIZE,
    OpenOrderRecord, OpenOrdersResponse, OrderPlacementResponse, PlaceOrderPayload, TradeMakerFill,
    TradeRecord, VenueMode, WireOrder,
};
pub use eip712::{
    CTF_EXCHANGE, CTF_NEG_RISK_EXCHANGE, DEPOSIT_WALLET_DOMAIN_NAME, DEPOSIT_WALLET_DOMAIN_VERSION,
    DepositWalletConstants, EXCHANGE_DOMAIN_NAME, MAX_SAFE_INTEGER, MarketDomain,
    ORDER_TYPE_STRING, Order, OrderSigner, POLY_V2_EXCHANGE, POLYGON_CHAIN_ID,
    TYPED_DATA_SIGN_TYPE_STRING, generate_salt,
};
pub use error::ExecutionError;
pub use heartbeat::{
    DEFAULT_HEARTBEAT_INTERVAL, HeartbeatRequest, HeartbeatResponse, OrderHeartbeatActor,
    execute_shutdown_cancel,
};
pub use settlement::{
    ApprovalState, CTF_ADDRESS, DEFAULT_MIN_BATCH_MICRO, DEFAULT_RELAYER_URL, MergeTarget,
    PUSD_ADDRESS, ROUTER_ADDRESS, RelayerClient, SettlementConfig, SettlementDecision,
    SettlementReceipt, SettlementWorker, build_merge_calldata, check_preflight, decide_settlement,
    merge_target_for_market, min_batch_for_gas, verify_wallet_type,
};
pub use user_ws::{
    ClobUserWsActor, DEFAULT_USER_WS_URL, FillReconciler, LegDelta, OrderRegistrySnapshot,
    TradeStage, USER_WS_PING_INTERVAL, USER_WS_PONG_TIMEOUT, UserOrderEvent, UserWsCredentials,
    subscription_frame,
};
