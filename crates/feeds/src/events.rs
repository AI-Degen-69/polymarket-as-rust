use std::mem::size_of;

use core_types::{FixedBook, LatencySummary, MicroUsdc, OrderSide, Outcome, SharesMicro};
use serde::{Deserialize, Serialize};

/// Default per-feed `rtrb` SPSC ringbuffer capacity (power of two, >= 4096 slots).
pub const DEFAULT_RING_CAPACITY: usize = 4096;

/// Logical market data feed identifier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum FeedId {
    /// Polymarket CLOB market WebSocket stream.
    PolymarketClob,
    /// Binance Spot combined `@bookTicker` / `@aggTrade` WebSocket stream.
    BinanceSpot,
    /// Optional Polymarket PolyBolt Chainlink TWAP WebSocket stream.
    PolyBolt,
}

impl FeedId {
    /// Stable lowercase identifier for logs and telemetry records.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::PolymarketClob => "polymarket_clob",
            Self::BinanceSpot => "binance_spot",
            Self::PolyBolt => "polybolt",
        }
    }
}

/// Zero-allocation `Copy` hot-path event emitted by `ClobMarketWsActor`.
// Intentional inline `FixedBook<10>` preserves zero-allocation `Copy` semantics inside `rtrb` slots.
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum PolyMarketEvent {
    /// Full or resynced L2 book snapshot trimmed to top 10 levels per side.
    BookSnapshot {
        /// Market outcome (`Up` or `Down`).
        outcome: Outcome,
        /// Stack-allocated sorted top-10 L2 book.
        book: FixedBook<10>,
        /// Local monotonic clock receive timestamp in nanoseconds.
        rx_mono_ns: u64,
        /// Exchange timestamp in Unix milliseconds.
        exch_ts_ms: u64,
    },
    /// Single-level L2 book delta with exchange top-of-book hint.
    PriceChange {
        /// Market outcome (`Up` or `Down`).
        outcome: Outcome,
        /// Order side (`Buy` = bid, `Sell` = ask).
        side: OrderSide,
        /// Level price in micro-USDC.
        price: MicroUsdc,
        /// New resting level size in micro-shares (`0` deletes level).
        size: SharesMicro,
        /// Exchange best bid hint in micro-USDC.
        best_bid: Option<MicroUsdc>,
        /// Exchange best ask hint in micro-USDC.
        best_ask: Option<MicroUsdc>,
        /// Local monotonic clock receive timestamp in nanoseconds.
        rx_mono_ns: u64,
        /// Exchange timestamp in Unix milliseconds.
        exch_ts_ms: u64,
    },
    /// Public trade print on an outcome book.
    LastTrade {
        /// Market outcome (`Up` or `Down`).
        outcome: Outcome,
        /// Aggressor side.
        side: OrderSide,
        /// Execution price in micro-USDC.
        price: MicroUsdc,
        /// Executed size in micro-shares.
        size: SharesMicro,
        /// Local monotonic clock receive timestamp in nanoseconds.
        rx_mono_ns: u64,
        /// Exchange timestamp in Unix milliseconds.
        exch_ts_ms: u64,
    },
    /// Minimum tick size transition notification.
    TickSizeChange {
        /// Market outcome (`Up` or `Down`).
        outcome: Outcome,
        /// Updated minimum tick size in micro-USDC.
        new_tick_micro: MicroUsdc,
        /// Local monotonic clock receive timestamp in nanoseconds.
        rx_mono_ns: u64,
        /// Exchange timestamp in Unix milliseconds.
        exch_ts_ms: u64,
    },
    /// Best bid/ask summary update (`custom_feature_enabled: true`).
    BestBidAsk {
        /// Market outcome (`Up` or `Down`).
        outcome: Outcome,
        /// Best bid price in micro-USDC.
        best_bid: Option<MicroUsdc>,
        /// Best ask price in micro-USDC.
        best_ask: Option<MicroUsdc>,
        /// Local monotonic clock receive timestamp in nanoseconds.
        rx_mono_ns: u64,
        /// Exchange timestamp in Unix milliseconds.
        exch_ts_ms: u64,
    },
}

/// Zero-allocation `Copy` hot-path event emitted by `BinanceSpotWsActor`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum BinanceSpotEvent {
    /// Immediate top-of-book quote from `btcusdt@bookTicker`.
    BookTicker {
        /// Best bid USD scaled by `10^8`.
        bid_e8: u64,
        /// Best ask USD scaled by `10^8`.
        ask_e8: u64,
        /// Best bid quantity in BTC scaled by `10^8`.
        bid_qty_e8: u64,
        /// Best ask quantity in BTC scaled by `10^8`.
        ask_qty_e8: u64,
        /// Binance book ticker update ID.
        update_id: u64,
        /// Local monotonic clock receive timestamp in nanoseconds.
        rx_mono_ns: u64,
    },
    /// Aggregated trade tick from `btcusdt@aggTrade`.
    AggTrade {
        /// Trade execution USD price scaled by `10^8`.
        price_e8: u64,
        /// Trade BTC quantity scaled by `10^8`.
        qty_e8: u64,
        /// `true` when buyer was maker (sell aggressor).
        is_buyer_maker: bool,
        /// Exchange trade timestamp in Unix milliseconds.
        exch_ts_ms: u64,
        /// Local monotonic clock receive timestamp in nanoseconds.
        rx_mono_ns: u64,
    },
}

/// Zero-allocation `Copy` reference price/TWAP event emitted by `PolyBoltTwapWsActor`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OracleTwapEvent {
    /// Reference spot price from PolyBolt `price.crypto`.
    ChainlinkSpot {
        /// USD spot price scaled by `10^8`.
        spot_e8: u64,
        /// Dense monotonic stream sequence number.
        seq: u64,
        /// Server-reported dropped frame count if any.
        dropped: u32,
        /// Oracle observation timestamp in Unix milliseconds.
        exch_ts_ms: u64,
        /// Local monotonic clock receive timestamp in nanoseconds.
        rx_mono_ns: u64,
    },
    /// 60-second Chainlink TWAP price from PolyBolt `price.crypto.twap`.
    ChainlinkTwap {
        /// 60-second TWAP USD price scaled by `10^8`.
        twap_e8: u64,
        /// Optional most recent spot USD price scaled by `10^8` (`0` if unseen).
        spot_e8: u64,
        /// Dense monotonic stream sequence number.
        seq: u64,
        /// Server-reported dropped frame count if any.
        dropped: u32,
        /// Oracle observation timestamp in Unix milliseconds.
        exch_ts_ms: u64,
        /// Local monotonic clock receive timestamp in nanoseconds.
        rx_mono_ns: u64,
    },
}

/// Lossless control-plane health and diagnostic events pushed over bounded `tokio::sync::mpsc`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum FeedHealthEvent {
    /// WebSocket TLS handshake and initial subscription succeeded.
    Connected {
        /// Feed source identifier.
        feed: FeedId,
        /// Local wall-clock timestamp in Unix milliseconds.
        ts_unix_ms: u64,
        /// Local monotonic nanoseconds.
        mono_ns: u64,
    },
    /// Connection closed or errored before reconnection.
    Disconnected {
        /// Feed source identifier.
        feed: FeedId,
        /// Disconnect or close reason.
        reason: String,
        /// Local wall-clock timestamp in Unix milliseconds.
        ts_unix_ms: u64,
        /// Local monotonic nanoseconds.
        mono_ns: u64,
    },
    /// Actor is hydrating fresh REST snapshots after reconnect before re-arming `Ready`.
    ResyncingRest {
        /// Feed source identifier.
        feed: FeedId,
        /// Local wall-clock timestamp in Unix milliseconds.
        ts_unix_ms: u64,
        /// Local monotonic nanoseconds.
        mono_ns: u64,
    },
    /// Stream is live, synced, and delivering valid frames.
    Ready {
        /// Feed source identifier.
        feed: FeedId,
        /// Local wall-clock timestamp in Unix milliseconds.
        ts_unix_ms: u64,
        /// Local monotonic nanoseconds.
        mono_ns: u64,
    },
    /// No frame received within configured feed watchdog timeout.
    StaleTimeout {
        /// Feed source identifier.
        feed: FeedId,
        /// Elapsed milliseconds since last message.
        silent_ms: u64,
        /// Local wall-clock timestamp in Unix milliseconds.
        ts_unix_ms: u64,
        /// Local monotonic nanoseconds.
        mono_ns: u64,
    },
    /// SPSC `rtrb` consumer lagged behind producer without blocking network I/O.
    RingOverflow {
        /// Feed source identifier.
        feed: FeedId,
        /// Cumulative dropped events on this actor.
        dropped_count: u64,
        /// Local wall-clock timestamp in Unix milliseconds.
        ts_unix_ms: u64,
        /// Local monotonic nanoseconds.
        mono_ns: u64,
    },
    /// Market settled or resolved cleanly according to CLOB WebSocket feed.
    MarketResolved {
        /// Feed source identifier.
        feed: FeedId,
        /// Local wall-clock timestamp in Unix milliseconds.
        ts_unix_ms: u64,
        /// Local monotonic nanoseconds.
        mono_ns: u64,
    },
    /// Malformed payload or unmapped asset ID logged explicitly rather than dropped silently.
    ParseWarning {
        /// Feed source identifier.
        feed: FeedId,
        /// Explicit parse failure detail.
        reason: String,
        /// Local wall-clock timestamp in Unix milliseconds.
        ts_unix_ms: u64,
        /// Local monotonic nanoseconds.
        mono_ns: u64,
    },
}

/// Summary counters and HDR parse latency stats returned when a feed actor exits.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct FeedStats {
    /// Logical feed identifier.
    pub feed: FeedId,
    /// Total WebSocket frames handled.
    pub frames_received: u64,
    /// Total typed events successfully pushed to `rtrb`.
    pub events_pushed: u64,
    /// Total `PushError::Full` drops.
    pub ring_overflows: u64,
    /// Explicit malformed JSON or unknown asset warnings.
    pub parse_warnings: u64,
    /// Reconnect attempts performed.
    pub reconnects: u64,
    /// Stale feed watchdog trips.
    pub stale_timeouts: u64,
    /// Heartbeat pongs received from server.
    pub pongs_received: u64,
    /// Heartbeat pings answered to server.
    pub pings_replied: u64,
    /// Health channel events dropped because control channel was full or closed.
    pub health_events_dropped: u64,
    /// Ingress frame parse latency percentiles (`None` if zero frames parsed).
    pub latency: Option<LatencySummary>,
}

const _: () = {
    assert!(size_of::<PolyMarketEvent>() <= 512);
    assert!(size_of::<BinanceSpotEvent>() <= 64);
    assert!(size_of::<OracleTwapEvent>() <= 64);
};
