use std::fmt;

use serde::{Deserialize, Deserializer, Serialize};

use crate::error::{MathError, ParseDecimalError, WireError, fmt_micro_units};

/// Fixed-point denominator: `1.000000` unit = `1_000_000` micro-units.
pub const MICRO_SCALE: u64 = 1_000_000;
/// Exact representation of `$1.00 USDC`.
pub const ONE_DOLLAR_MICRO: u64 = 1_000_000;
/// Exact representation of `$0.01 USDC` (1 cent).
pub const ONE_CENT_MICRO: u64 = 10_000;
/// Exact representation of `1.000000` outcome share.
pub const ONE_SHARE_MICRO: u64 = 1_000_000;
/// Polymarket size precision step (`0.01` shares = `10_000` micro-shares).
pub const SIZE_STEP_MICRO: u64 = 10_000;

/// Fixed-point USDC amount scaled by `10^6` (`$1.00 = 1_000_000`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[repr(transparent)]
pub struct MicroUsdc(pub u64);

impl MicroUsdc {
    /// Zero micro-USDC.
    pub const ZERO: Self = Self(0);
    /// `$1.00` in micro-USDC.
    pub const ONE_DOLLAR: Self = Self(ONE_DOLLAR_MICRO);
    /// `$0.01` in micro-USDC.
    pub const ONE_CENT: Self = Self(ONE_CENT_MICRO);

    /// Constructs a `MicroUsdc` from raw `10^-6` USDC integer units.
    #[must_use]
    pub const fn from_raw(raw: u64) -> Self {
        Self(raw)
    }

    /// Parses an ASCII decimal string (e.g. `"0.52"`) without floating-point rounding.
    pub fn from_usd_str(s: &str) -> Result<Self, ParseDecimalError> {
        parse_decimal_micro(s.as_bytes()).map(Self)
    }

    /// Returns raw `10^-6` USDC integer units.
    #[must_use]
    pub const fn as_u64(self) -> u64 {
        self.0
    }

    /// Converts to `f64` dollars for analytical math outside wire quantization.
    #[must_use]
    pub fn to_usd_f64(self) -> f64 {
        self.0 as f64 / MICRO_SCALE as f64
    }

    /// Returns the absolute difference `|self - other|` without underflow.
    #[must_use]
    pub const fn abs_diff(self, other: Self) -> Self {
        Self(self.0.abs_diff(other.0))
    }

    /// Checked addition returning `None` on overflow.
    #[must_use]
    pub const fn checked_add(self, rhs: Self) -> Option<Self> {
        match self.0.checked_add(rhs.0) {
            Some(v) => Some(Self(v)),
            None => None,
        }
    }

    /// Checked subtraction returning `None` on underflow.
    #[must_use]
    pub const fn checked_sub(self, rhs: Self) -> Option<Self> {
        match self.0.checked_sub(rhs.0) {
            Some(v) => Some(Self(v)),
            None => None,
        }
    }

    /// Saturating subtraction guarding against negative values.
    #[must_use]
    pub const fn saturating_sub(self, rhs: Self) -> Self {
        Self(self.0.saturating_sub(rhs.0))
    }
}

impl fmt::Display for MicroUsdc {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt_micro_units(f, self.0)
    }
}

/// Fixed-point outcome token quantity scaled by `10^6` (`1 share = 1_000_000`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[repr(transparent)]
pub struct SharesMicro(pub u64);

impl SharesMicro {
    /// Zero shares.
    pub const ZERO: Self = Self(0);

    /// Constructs `SharesMicro` from raw `10^-6` share integer units.
    #[must_use]
    pub const fn from_raw(raw: u64) -> Self {
        Self(raw)
    }

    /// Constructs `SharesMicro` from whole integer shares.
    pub fn from_whole_shares(shares: u64) -> Result<Self, WireError> {
        shares
            .checked_mul(ONE_SHARE_MICRO)
            .map(Self)
            .ok_or(WireError::ArithmeticOverflow)
    }

    /// Parses an ASCII decimal share count (e.g. `"10.00"`) without floating-point rounding.
    pub fn from_shares_str(s: &str) -> Result<Self, ParseDecimalError> {
        parse_decimal_micro(s.as_bytes()).map(Self)
    }

    /// Returns raw `10^-6` share units.
    #[must_use]
    pub const fn as_u64(self) -> u64 {
        self.0
    }

    /// Converts to `f64` shares for analytical pricing math.
    #[must_use]
    pub fn to_shares_f64(self) -> f64 {
        self.0 as f64 / MICRO_SCALE as f64
    }

    /// Saturating subtraction guarding against underflow.
    #[must_use]
    pub const fn saturating_sub(self, rhs: Self) -> Self {
        Self(self.0.saturating_sub(rhs.0))
    }
}

impl fmt::Display for SharesMicro {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt_micro_units(f, self.0)
    }
}

/// Signed net inventory `q = q_UP - q_DOWN` scaled by `10^6`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[repr(transparent)]
pub struct SignedSharesMicro(pub i64);

impl SignedSharesMicro {
    /// Zero net inventory.
    pub const ZERO: Self = Self(0);

    /// Returns raw signed `10^-6` share units.
    #[must_use]
    pub const fn as_i64(self) -> i64 {
        self.0
    }

    /// Returns absolute net shares as `SharesMicro` without overflow even at `i64::MIN`.
    #[must_use]
    pub const fn abs_micro(self) -> SharesMicro {
        SharesMicro(self.0.unsigned_abs())
    }

    /// Converts net signed inventory to `f64` shares.
    #[must_use]
    pub fn to_shares_f64(self) -> f64 {
        self.0 as f64 / MICRO_SCALE as f64
    }
}

/// Strictly positive, finite `f64` guarding against `NaN`, `inf`, `<= 0.0` math poisoning.
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd, Serialize)]
#[repr(transparent)]
pub struct FinitePosF64(f64);

impl FinitePosF64 {
    /// Validates `value.is_finite() && value > 0.0` at construction.
    pub fn try_new(value: f64) -> Result<Self, MathError> {
        if !value.is_finite() {
            return Err(MathError::NonFiniteInput { value });
        }
        if value <= 0.0 {
            return Err(MathError::NonPositiveFinite { value });
        }
        Ok(Self(value))
    }

    /// Returns the validated positive finite `f64`.
    #[must_use]
    pub const fn get(self) -> f64 {
        self.0
    }
}

impl<'de> Deserialize<'de> for FinitePosF64 {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = f64::deserialize(deserializer)?;
        Self::try_new(value).map_err(serde::de::Error::custom)
    }
}

/// Order side on the Polymarket CLOB wire (`0 = BUY`, `1 = SELL`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[repr(u8)]
pub enum OrderSide {
    /// Buy outcome tokens using USDC collateral.
    Buy = 0,
    /// Sell outcome tokens for USDC collateral.
    Sell = 1,
}

impl OrderSide {
    /// Numeric EIP-712 `side` integer (`0` or `1`).
    #[must_use]
    pub const fn as_u8(self) -> u8 {
        self as u8
    }

    /// Canonical REST/WS string (`"BUY"` or `"SELL"`).
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Buy => "BUY",
            Self::Sell => "SELL",
        }
    }
}

/// Polymarket CLOB tick precision matrix from `clob-rest-api.md:83-93` and official docs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum TickPrecision {
    /// Tick `0.1`: 1 price decimal, 2 size decimals, 3 amount decimals.
    Tenth,
    /// Tick `0.01`: 2 price decimals, 2 size decimals, 4 amount decimals.
    Cent,
    /// Tick `0.005`: 3 price decimals, 2 size decimals, 5 amount decimals.
    HalfCent,
    /// Tick `0.0025`: 4 price decimals, 2 size decimals, 6 amount decimals.
    QuarterCent,
    /// Tick `0.001`: 3 price decimals, 2 size decimals, 5 amount decimals.
    Mill,
    /// Tick `0.0001`: 4 price decimals, 2 size decimals, 6 amount decimals.
    TenthMill,
}

impl TickPrecision {
    /// Maps a micro-USDC tick increment into a supported `TickPrecision`.
    pub const fn from_micro(tick_micro: MicroUsdc) -> Result<Self, WireError> {
        match tick_micro.0 {
            100_000 => Ok(Self::Tenth),
            10_000 => Ok(Self::Cent),
            5_000 => Ok(Self::HalfCent),
            2_500 => Ok(Self::QuarterCent),
            1_000 => Ok(Self::Mill),
            100 => Ok(Self::TenthMill),
            other => Err(WireError::UnsupportedTickSize { tick_micro: other }),
        }
    }

    /// Returns tick size in micro-USDC.
    #[must_use]
    pub const fn tick_micro(self) -> MicroUsdc {
        match self {
            Self::Tenth => MicroUsdc(100_000),
            Self::Cent => MicroUsdc(10_000),
            Self::HalfCent => MicroUsdc(5_000),
            Self::QuarterCent => MicroUsdc(2_500),
            Self::Mill => MicroUsdc(1_000),
            Self::TenthMill => MicroUsdc(100),
        }
    }

    /// Maximum fractional digits permitted for wire prices.
    #[must_use]
    pub const fn price_decimals(self) -> u8 {
        match self {
            Self::Tenth => 1,
            Self::Cent => 2,
            Self::HalfCent | Self::Mill => 3,
            Self::QuarterCent | Self::TenthMill => 4,
        }
    }

    /// Maximum fractional digits permitted for share quantities (`2` across all ticks).
    #[must_use]
    pub const fn size_decimals(self) -> u8 {
        2
    }

    /// Maximum fractional digits permitted for USDC notional amounts.
    #[must_use]
    pub const fn amount_decimals(self) -> u8 {
        match self {
            Self::Tenth => 3,
            Self::Cent => 4,
            Self::HalfCent | Self::Mill => 5,
            Self::QuarterCent | Self::TenthMill => 6,
        }
    }
}

/// Validated 6-decimal EIP-712 wire amounts (`makerAmount`, `takerAmount`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct WireAmounts {
    /// Side (`BUY` or `SELL`).
    pub side: OrderSide,
    /// Maker asset amount in `10^-6` base units.
    pub maker_amount: u64,
    /// Taker asset amount in `10^-6` base units.
    pub taker_amount: u64,
    /// Effective price used to compute amounts.
    pub price_micro: MicroUsdc,
    /// Effective share count rounded down to `size_decimals` (`2` decimals).
    pub rounded_shares_micro: SharesMicro,
}

/// Encodes a BUY order (`makerAmount = price * size`, `takerAmount = size`) in 6-decimal wire units.
pub fn encode_buy_amounts(
    price: MicroUsdc,
    shares: SharesMicro,
    tick: TickPrecision,
) -> Result<WireAmounts, WireError> {
    encode_amounts_inner(OrderSide::Buy, price, shares, tick)
}

/// Encodes a SELL order (`makerAmount = size`, `takerAmount = price * size`) in 6-decimal wire units.
pub fn encode_sell_amounts(
    price: MicroUsdc,
    shares: SharesMicro,
    tick: TickPrecision,
) -> Result<WireAmounts, WireError> {
    encode_amounts_inner(OrderSide::Sell, price, shares, tick)
}

fn encode_amounts_inner(
    side: OrderSide,
    price: MicroUsdc,
    shares: SharesMicro,
    tick: TickPrecision,
) -> Result<WireAmounts, WireError> {
    if price.0 == 0 || price.0 >= ONE_DOLLAR_MICRO {
        return Err(WireError::PriceOutOfBounds {
            price_micro: price.0,
            bound_desc: "expected strictly inside (0, 1_000_000)",
        });
    }
    let tick_raw = tick.tick_micro().0;
    if !price.0.is_multiple_of(tick_raw) {
        return Err(WireError::PriceNotTickAligned {
            price_micro: price.0,
            tick_micro: tick_raw,
        });
    }

    let rounded_shares_raw = (shares.0 / SIZE_STEP_MICRO) * SIZE_STEP_MICRO;
    if rounded_shares_raw == 0 {
        return Err(WireError::ZeroRoundedSize {
            raw_shares_micro: shares.0,
        });
    }

    let prod = u128::from(price.0)
        .checked_mul(u128::from(rounded_shares_raw))
        .ok_or(WireError::ArithmeticOverflow)?;
    let usd_micro_u128 = prod / u128::from(MICRO_SCALE);
    let amount_step = 10_u128.pow(u32::from(6_u8.saturating_sub(tick.amount_decimals())));
    let usd_micro_aligned = (usd_micro_u128 / amount_step) * amount_step;
    let usd_micro = u64::try_from(usd_micro_aligned).map_err(|_| WireError::ArithmeticOverflow)?;

    let (maker_amount, taker_amount) = match side {
        OrderSide::Buy => (usd_micro, rounded_shares_raw),
        OrderSide::Sell => (rounded_shares_raw, usd_micro),
    };

    Ok(WireAmounts {
        side,
        maker_amount,
        taker_amount,
        price_micro: price,
        rounded_shares_micro: SharesMicro(rounded_shares_raw),
    })
}

fn classify_decimal_byte(
    byte: u8,
    pos: usize,
    seen_dot: &mut bool,
    int_digits: usize,
) -> Result<Option<u8>, ParseDecimalError> {
    if byte == b'.' {
        if *seen_dot {
            return Err(ParseDecimalError::MultipleDecimalPoints);
        }
        if int_digits == 0 {
            return Err(ParseDecimalError::MalformedDecimalPoint);
        }
        *seen_dot = true;
        return Ok(None);
    }
    if !byte.is_ascii_digit() {
        return Err(ParseDecimalError::InvalidByte {
            byte,
            position: pos,
        });
    }
    Ok(Some(byte - b'0'))
}

/// Zero-allocation ASCII parser converting decimal strings (`"0.52"`, `"10"`) to `10^-6` micro-units.
pub fn parse_decimal_micro(bytes: &[u8]) -> Result<u64, ParseDecimalError> {
    if bytes.is_empty() {
        return Err(ParseDecimalError::Empty);
    }

    let (mut whole, mut frac, mut frac_digits, mut int_digits) = (0_u64, 0_u64, 0_usize, 0_usize);
    let mut seen_dot = false;

    for (pos, &byte) in bytes.iter().enumerate() {
        let Some(digit) = classify_decimal_byte(byte, pos, &mut seen_dot, int_digits)? else {
            continue;
        };
        if !seen_dot {
            int_digits += 1;
            whole = whole
                .checked_mul(10)
                .and_then(|v| v.checked_add(u64::from(digit)))
                .ok_or(ParseDecimalError::Overflow)?;
        } else {
            frac_digits += 1;
            if frac_digits > 6 {
                return Err(ParseDecimalError::TooManyFractionalDigits { count: frac_digits });
            }
            frac = frac * 10 + u64::from(digit);
        }
    }

    if seen_dot && frac_digits == 0 {
        return Err(ParseDecimalError::MalformedDecimalPoint);
    }
    for _ in frac_digits..6 {
        frac *= 10;
    }

    whole
        .checked_mul(MICRO_SCALE)
        .and_then(|w| w.checked_add(frac))
        .ok_or(ParseDecimalError::Overflow)
}

/// Quantizes a raw floating-point dollar price downward to an exact multiple of `tick_micro`.
///
/// Accepts `[0.0, 1_000_000.0]` USD (supporting both binary `[0.0, 1.0]` quotes and spot USD prices);
/// Polymarket binary `(0, $1.00)` contract bounds are enforced in `encode_buy_amounts`/`encode_sell_amounts`.
/// Uses a tight ULP-scaled tolerance (`8 * EPSILON`) so exact binary floats like `0.29_f64` do not
/// drop a cent, while genuine sub-tick values like `0.5099999999995` floor conservatively to `0.50`.
pub fn floor_to_tick(raw_usd: f64, tick_micro: MicroUsdc) -> Result<MicroUsdc, MathError> {
    if !raw_usd.is_finite() {
        return Err(MathError::NonFiniteInput { value: raw_usd });
    }
    if !(0.0..=1_000_000.0).contains(&raw_usd) {
        return Err(MathError::OutOfRangePrice { value: raw_usd });
    }
    if tick_micro.0 == 0 || tick_micro.0 > ONE_DOLLAR_MICRO {
        return Err(MathError::InvalidTickSize {
            tick_micro: tick_micro.0,
        });
    }

    let ulp_tol = 8.0 * f64::EPSILON * raw_usd.max(1.0);
    let scaled = ((raw_usd + ulp_tol) * MICRO_SCALE as f64).floor();
    // 2^64 exact threshold avoids IEEE-754 `u64::MAX as f64` rounding up to `2^64`.
    const TWO_POW_64: f64 = 18_446_744_073_709_551_616.0;
    if !scaled.is_finite() || scaled < 0.0 || scaled >= TWO_POW_64 {
        return Err(MathError::Overflow);
    }
    let raw_micro = scaled as u64;
    let aligned = (raw_micro / tick_micro.0) * tick_micro.0;
    Ok(MicroUsdc(aligned))
}

/// Polymarket fee configuration per market (`feeRate` scaled by `10^6`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(try_from = "RawFeeSchedule")]
pub struct FeeSchedule {
    /// Taker fee coefficient scaled by `10^6` (e.g. `50_000` for `0.05`).
    pub fee_rate_e6: u64,
    /// Makers pay zero platform fee (`true` on Polymarket CLOB).
    pub maker_fee_zero: bool,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawFeeSchedule {
    fee_rate_e6: u64,
    maker_fee_zero: bool,
}

impl TryFrom<RawFeeSchedule> for FeeSchedule {
    type Error = WireError;

    fn try_from(raw: RawFeeSchedule) -> Result<Self, Self::Error> {
        Self::new(raw.fee_rate_e6, raw.maker_fee_zero)
    }
}

impl FeeSchedule {
    /// Constructs a validated fee schedule (`fee_rate_e6 <= 1_000_000`).
    pub const fn new(fee_rate_e6: u64, maker_fee_zero: bool) -> Result<Self, WireError> {
        if fee_rate_e6 > ONE_DOLLAR_MICRO {
            return Err(WireError::InvalidFeeRate { fee_rate_e6 });
        }
        Ok(Self {
            fee_rate_e6,
            maker_fee_zero,
        })
    }

    /// Validates `self.fee_rate_e6 <= 1_000_000` (guards structs built directly or deserialized).
    pub const fn validate(self) -> Result<(), WireError> {
        if self.fee_rate_e6 > ONE_DOLLAR_MICRO {
            return Err(WireError::InvalidFeeRate {
                fee_rate_e6: self.fee_rate_e6,
            });
        }
        Ok(())
    }

    /// Post-Nov 20, 2026 5m/15m BTC taker fee rate (`0.05`), 0% maker fee.
    #[must_use]
    pub const fn btc_5m_default() -> Self {
        Self {
            fee_rate_e6: 50_000,
            maker_fee_zero: true,
        }
    }

    /// Pre-Nov 20, 2026 crypto taker fee rate (`0.07`), 0% maker fee.
    #[must_use]
    pub const fn btc_5m_legacy() -> Self {
        Self {
            fee_rate_e6: 70_000,
            maker_fee_zero: true,
        }
    }

    /// Computes exact taker fee `shares * feeRate * p * (1 - p)` rounded half-up to 5 decimals.
    ///
    /// Fees whose rounded 5-decimal value is `< 0.00001 USDC` (`< 10` micro-USDC) become `0`.
    pub fn compute_taker_fee_micro(
        self,
        shares: SharesMicro,
        price: MicroUsdc,
    ) -> Result<MicroUsdc, WireError> {
        if self.fee_rate_e6 > ONE_DOLLAR_MICRO {
            return Err(WireError::InvalidFeeRate {
                fee_rate_e6: self.fee_rate_e6,
            });
        }
        if price.0 > ONE_DOLLAR_MICRO {
            return Err(WireError::PriceOutOfBounds {
                price_micro: price.0,
                bound_desc: "expected inside [0, 1_000_000]",
            });
        }
        let one_minus_p = ONE_DOLLAR_MICRO - price.0;

        // Result is scaled by 10^24 because each of the four factors carries 10^6 scale.
        let num = u128::from(shares.0)
            .checked_mul(u128::from(self.fee_rate_e6))
            .and_then(|v| v.checked_mul(u128::from(price.0)))
            .and_then(|v| v.checked_mul(u128::from(one_minus_p)))
            .ok_or(WireError::ArithmeticOverflow)?;

        // 0.00001 USDC = 10 micro-USDC = 10^19 in 10^24-scaled numerator units.
        const UNIT_5DP: u128 = 10_000_000_000_000_000_000;
        let rounded_5dp = num
            .checked_add(UNIT_5DP / 2)
            .ok_or(WireError::ArithmeticOverflow)?
            / UNIT_5DP;
        if rounded_5dp == 0 {
            return Ok(MicroUsdc::ZERO);
        }

        let micro_u128 = rounded_5dp
            .checked_mul(10)
            .ok_or(WireError::ArithmeticOverflow)?;
        let micro = u64::try_from(micro_u128).map_err(|_| WireError::ArithmeticOverflow)?;
        Ok(MicroUsdc(micro))
    }

    /// Computes maker fee (`0` when `maker_fee_zero` is enabled), validating rate and price bounds first.
    pub fn compute_maker_fee_micro(
        self,
        shares: SharesMicro,
        price: MicroUsdc,
    ) -> Result<MicroUsdc, WireError> {
        self.validate()?;
        if price.0 > ONE_DOLLAR_MICRO {
            return Err(WireError::PriceOutOfBounds {
                price_micro: price.0,
                bound_desc: "expected inside [0, 1_000_000]",
            });
        }
        if self.maker_fee_zero {
            return Ok(MicroUsdc::ZERO);
        }
        self.compute_taker_fee_micro(shares, price)
    }
}
