use serde::{Deserialize, Serialize, Serializer};

use crate::error::MathError;
use crate::money::{
    FeeSchedule, FinitePosF64, MicroUsdc, ONE_DOLLAR_MICRO, SharesMicro, TickPrecision,
};

/// Interned outcome identifier eliminating 77-digit token ID strings from the hot path.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[repr(u8)]
pub enum Outcome {
    /// UP / YES outcome token.
    Up = 0,
    /// DOWN / NO outcome token.
    Down = 1,
}

impl Outcome {
    /// Returns the complementary binary outcome.
    #[must_use]
    pub const fn opposite(self) -> Self {
        match self {
            Self::Up => Self::Down,
            Self::Down => Self::Up,
        }
    }
}

/// Polymarket settlement contract generation detected via `GET /book`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ProtocolVersion {
    /// Legacy Conditional Token Framework (`CTF`, domain `version: "2"`).
    CtfV1,
    /// Polymarket Protocol V2 (`ExchangeV3`, `PositionManager`, `Router`, domain `version: "3"`).
    PolyV2,
}

/// Polymarket EIP-712 wallet signature mode (`0..=3`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[repr(u8)]
pub enum WalletMode {
    /// Direct EOA signature (`signatureType = 0`).
    Eoa = 0,
    /// Legacy Polymarket Proxy (`signatureType = 1`).
    PolyProxy = 1,
    /// Gnosis Safe proxy (`signatureType = 2`).
    GnosisSafe = 2,
    /// Poly v2 Deposit Wallet (`signatureType = 3`, ERC-7739 `TypedDataSign` wrapper).
    DepositWallet1271 = 3,
}

impl WalletMode {
    /// Numeric `signatureType` integer expected by Polymarket CLOB.
    #[must_use]
    pub const fn as_u8(self) -> u8 {
        self as u8
    }
}

/// Runtime capital execution safety mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ExecutionMode {
    /// Zero-capital queue-aware paper simulation.
    Paper,
    /// Live signing and payload validation without submitting orders to CLOB.
    DryRun,
    /// Live CLOB execution (requires explicit CLI confirmation + `DRY_RUN=false`).
    CanaryLive,
}

/// Station 2/5 remaining-variance formulation toggle.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum VarianceMode {
    /// Audit-fixed binary CARA variance `p(1-p) * max(tau/300, tau_floor_frac)`.
    BinaryCara,
    /// Literal Library formula `sigma_0^2 * ((T-t)/T)^2` for paper A/B benchmarking.
    LibraryStrict,
}

/// Station 3 liquidity decay estimator toggle.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum KappaMode {
    /// Trade-size-normalized depth regression with touch cap (audit-fixed default).
    TradeNormalizedDepth,
    /// Literal Library log-slope estimator `(ln(V_k) - ln(V_1)) / (delta_k - delta_1)` for A/B.
    LibraryLogSlope,
}

/// Station 1 Savitzky-Golay filter mode toggle.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum SgMode {
    /// Zero-lag right-endpoint causal projection `[3, -5, -3, 9, 31] / 35`.
    Causal,
    /// Literal Library centered weights `[-3, 12, 17, 12, -3] / 35` (200ms phase lag) for A/B.
    Symmetric,
}

/// Static + dynamic metadata for an active 5-minute BTC Up/Down epoch.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MarketDescriptor {
    /// Condition ID (`0x...`).
    pub condition_id: String,
    /// Market slug (e.g. `btc-updown-5m-...`).
    pub slug: String,
    /// Human-readable market question.
    pub question: String,
    /// Settlement contract version (`CtfV1` vs `PolyV2`).
    pub version: ProtocolVersion,
    /// Whether market routes through NegRisk exchange/adapter.
    pub neg_risk: bool,
    /// CLOB asset / position ID for UP outcome.
    pub asset_id_up: String,
    /// CLOB asset / position ID for DOWN outcome.
    pub asset_id_dn: String,
    /// Current minimum tick increment in micro-USDC.
    pub tick_size_micro: MicroUsdc,
    /// Minimum share order size in micro-shares.
    pub min_order_size_micro: SharesMicro,
    /// Configured taker delay in milliseconds (`50ms` default on crypto up/down).
    pub taker_delay_ms: u64,
    /// Market fee schedule.
    pub fee_schedule: FeeSchedule,
    /// Epoch start timestamp in Unix milliseconds.
    pub start_ts_ms: u64,
    /// Epoch expiry timestamp in Unix milliseconds.
    pub expiry_ts_ms: u64,
    /// Inception Chainlink TWAP strike price in USD if available.
    pub strike_price_usd: Option<FinitePosF64>,
}

impl MarketDescriptor {
    /// Validates structural invariants of a discovered 5-minute market before trading starts.
    pub fn validate(&self) -> Result<(), MathError> {
        if self.condition_id.trim().is_empty()
            || self.asset_id_up.trim().is_empty()
            || self.asset_id_dn.trim().is_empty()
        {
            return Err(MathError::InvalidMarketDescriptor {
                detail: "condition_id and both asset IDs must be non-empty".to_string(),
            });
        }
        if self.asset_id_up.trim() == self.asset_id_dn.trim() {
            return Err(MathError::InvalidMarketDescriptor {
                detail: "asset_id_up and asset_id_dn must be distinct".to_string(),
            });
        }
        TickPrecision::from_micro(self.tick_size_micro).map_err(|_| {
            MathError::InvalidTickSize {
                tick_micro: self.tick_size_micro.0,
            }
        })?;
        self.fee_schedule
            .validate()
            .map_err(|err| MathError::InvalidMarketDescriptor {
                detail: err.to_string(),
            })?;
        if self.min_order_size_micro.0 == 0 {
            return Err(MathError::InvalidMarketDescriptor {
                detail: "min_order_size_micro must be > 0".to_string(),
            });
        }
        if self.expiry_ts_ms <= self.start_ts_ms {
            return Err(MathError::InvalidMarketDescriptor {
                detail: format!(
                    "expiry_ts_ms ({}) must exceed start_ts_ms ({})",
                    self.expiry_ts_ms, self.start_ts_ms
                ),
            });
        }
        Ok(())
    }
}

/// Single L2 price level in fixed-point units.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct BookLevel {
    /// Price in micro-USDC.
    pub price: MicroUsdc,
    /// Resting size in micro-shares.
    pub size: SharesMicro,
}

impl BookLevel {
    /// Empty sentinel level used in fixed-size stack arrays.
    pub const EMPTY: Self = Self {
        price: MicroUsdc::ZERO,
        size: SharesMicro::ZERO,
    };
}

/// Stack-allocated, `Copy`, zero-allocation L2 orderbook snapshot for one outcome (`N <= 255`).
#[derive(Debug, Clone, Copy)]
pub struct FixedBook<const N: usize> {
    /// Outcome (`Up` or `Down`).
    pub outcome: Outcome,
    bids: [BookLevel; N],
    asks: [BookLevel; N],
    bid_len: u8,
    ask_len: u8,
    /// Monotonic local receive timestamp in nanoseconds.
    pub last_update_ns: u64,
    /// Exchange event timestamp in Unix milliseconds.
    pub exchange_ts_ms: u64,
}

impl<const N: usize> PartialEq for FixedBook<N> {
    fn eq(&self, other: &Self) -> bool {
        self.outcome == other.outcome
            && self.last_update_ns == other.last_update_ns
            && self.exchange_ts_ms == other.exchange_ts_ms
            && self.bids_slice() == other.bids_slice()
            && self.asks_slice() == other.asks_slice()
    }
}

impl<const N: usize> Eq for FixedBook<N> {}

impl<const N: usize> Serialize for FixedBook<N> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        #[derive(Serialize)]
        struct FixedBookView<'a> {
            outcome: Outcome,
            bids: &'a [BookLevel],
            asks: &'a [BookLevel],
            last_update_ns: u64,
            exchange_ts_ms: u64,
        }

        FixedBookView {
            outcome: self.outcome,
            bids: self.bids_slice(),
            asks: self.asks_slice(),
            last_update_ns: self.last_update_ns,
            exchange_ts_ms: self.exchange_ts_ms,
        }
        .serialize(serializer)
    }
}

impl<const N: usize> FixedBook<N> {
    /// Creates an empty stack-allocated book for `outcome` (`N <= 255` enforced at compile time).
    #[must_use]
    pub const fn empty(outcome: Outcome) -> Self {
        const { assert!(N <= u8::MAX as usize) };
        Self {
            outcome,
            bids: [BookLevel::EMPTY; N],
            asks: [BookLevel::EMPTY; N],
            bid_len: 0,
            ask_len: 0,
            last_update_ns: 0,
            exchange_ts_ms: 0,
        }
    }

    /// Constructs a validated `FixedBook<N>` from sorted non-zero L2 slices, keeping top `N` levels.
    pub fn try_from_slices(
        outcome: Outcome,
        bids_in: &[BookLevel],
        asks_in: &[BookLevel],
        last_update_ns: u64,
        exchange_ts_ms: u64,
    ) -> Result<Self, MathError> {
        const { assert!(N <= u8::MAX as usize) };
        validate_levels("bid", bids_in.iter().take(N), true)?;
        validate_levels("ask", asks_in.iter().take(N), false)?;

        let mut book = Self::empty(outcome);
        let b_count = bids_in.len().min(N);
        let a_count = asks_in.len().min(N);

        for (dst, src) in book.bids.iter_mut().zip(bids_in.iter().take(b_count)) {
            *dst = *src;
        }
        for (dst, src) in book.asks.iter_mut().zip(asks_in.iter().take(a_count)) {
            *dst = *src;
        }
        book.bid_len = b_count as u8;
        book.ask_len = a_count as u8;
        book.last_update_ns = last_update_ns;
        book.exchange_ts_ms = exchange_ts_ms;
        Ok(book)
    }

    /// Active bid level count (`<= N`).
    #[must_use]
    pub fn bid_len(&self) -> usize {
        usize::from(self.bid_len).min(N)
    }

    /// Active ask level count (`<= N`).
    #[must_use]
    pub fn ask_len(&self) -> usize {
        usize::from(self.ask_len).min(N)
    }

    /// Returns active bid levels safely without panicking.
    #[must_use]
    pub fn bids_slice(&self) -> &[BookLevel] {
        self.bids.get(..self.bid_len()).unwrap_or(&[])
    }

    /// Returns active ask levels safely without panicking.
    #[must_use]
    pub fn asks_slice(&self) -> &[BookLevel] {
        self.asks.get(..self.ask_len()).unwrap_or(&[])
    }

    /// Best bid level if present.
    #[must_use]
    pub fn best_bid(&self) -> Option<BookLevel> {
        self.bids_slice().first().copied()
    }

    /// Best ask level if present.
    #[must_use]
    pub fn best_ask(&self) -> Option<BookLevel> {
        self.asks_slice().first().copied()
    }

    /// Returns `true` when `best_bid >= best_ask` (crossed or locked book).
    #[must_use]
    pub fn is_crossed(&self) -> bool {
        match (self.best_bid(), self.best_ask()) {
            (Some(bid), Some(ask)) => bid.price >= ask.price,
            _ => false,
        }
    }

    /// Returns raw arithmetic mid-price in USD, distinguishing crossed/overflow states from empty books.
    pub fn raw_mid_usd(&self) -> Result<Option<f64>, MathError> {
        let (Some(bid), Some(ask)) = (self.best_bid(), self.best_ask()) else {
            return Ok(None);
        };
        if bid.price >= ask.price {
            return Err(MathError::CrossedBook {
                best_bid_micro: bid.price.0,
                best_ask_micro: ask.price.0,
            });
        }
        let mid_sum = bid
            .price
            .0
            .checked_add(ask.price.0)
            .ok_or(MathError::Overflow)?;
        Ok(Some((mid_sum as f64) / (2.0 * ONE_DOLLAR_MICRO as f64)))
    }
}

fn validate_levels<'a>(
    side: &'static str,
    levels: impl Iterator<Item = &'a BookLevel>,
    descending: bool,
) -> Result<(), MathError> {
    let mut prev_price: Option<MicroUsdc> = None;
    for (index, level) in levels.enumerate() {
        if level.price.0 == 0 || level.price.0 >= ONE_DOLLAR_MICRO {
            return Err(MathError::InvalidBookLevel {
                side,
                index,
                reason: "price must lie in (0, 1_000_000)",
            });
        }
        if level.size.0 == 0 {
            return Err(MathError::InvalidBookLevel {
                side,
                index,
                reason: "level size must be > 0",
            });
        }
        if let Some(prev) = prev_price {
            let valid_order = if descending {
                level.price < prev
            } else {
                level.price > prev
            };
            if !valid_order {
                return Err(MathError::InvalidBookLevel {
                    side,
                    index,
                    reason: "levels are unsorted or contain duplicate price",
                });
            }
        }
        prev_price = Some(level.price);
    }
    Ok(())
}
