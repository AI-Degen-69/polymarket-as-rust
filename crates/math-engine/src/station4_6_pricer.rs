use core_types::{
    FeeSchedule, FinitePosF64, MathError, MicroUsdc, MonotonicClock, ONE_DOLLAR_MICRO,
    SignedSharesMicro, TickPrecision, floor_to_tick,
};
use serde::{Deserialize, Serialize};

use crate::safe_clamp;

/// Configuration for Stations 4–6 (`s*`, reservation price `r_i`, dual bids, Post-Only guards).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PricerConfig {
    /// Strictly positive CARA risk aversion parameter `gamma` (`0.15` `USD^-1` default).
    pub gamma: FinitePosF64,
    /// Whether Station 4 adds `gamma * V_rem` to `(2/gamma) * ln(1 + gamma/kappa)` (`true` default).
    pub include_variance_spread: bool,
    /// Whether Station 4 enforces taker-fee profitability floor `fee_rate*p*(1-p) + tick` (`false` default).
    pub enforce_fee_floor: bool,
    /// Minimum combined spread floor in micro-USDC (`10_000` = `$0.01` default).
    pub min_combined_spread_micro: MicroUsdc,
    /// Minimum combined spread floor in integer ticks (`1` default).
    pub min_spread_ticks: u32,
    /// Maximum allowed sum `bid_up + bid_dn` in micro-USDC (`980_000` = `$0.98` default).
    pub max_combined_bid_micro: MicroUsdc,
    /// Optional cross-tick pair-cost loss budget in micro-USDC (`None` disables cross-tick cap).
    pub max_cross_tick_loss_micro: Option<u64>,
    /// Optional extreme-probability entry guard in `(0.5, 1.0)` (`Some(0.80)` default; `None` disables entry guard).
    pub max_entry_prob: Option<f64>,
}

impl PricerConfig {
    /// Builds a configuration anchored on validated `gamma` with locked operator risk floors.
    #[must_use]
    pub const fn from_gamma(gamma: FinitePosF64) -> Self {
        Self {
            gamma,
            include_variance_spread: true,
            enforce_fee_floor: false,
            min_combined_spread_micro: MicroUsdc(10_000),
            min_spread_ticks: 1,
            max_combined_bid_micro: MicroUsdc(980_000),
            max_cross_tick_loss_micro: None,
            max_entry_prob: Some(0.80),
        }
    }

    /// Constructs the locked default configuration (`gamma = 0.15`, `spread >= $0.01`, `pair <= $0.98`, `max_entry_prob = 0.80`).
    pub fn try_default() -> Result<Self, MathError> {
        let gamma = FinitePosF64::try_new(0.15)?;
        Ok(Self::from_gamma(gamma))
    }

    /// Validates spread floor and maximum combined bid invariants.
    pub fn validate(&self) -> Result<(), MathError> {
        let _ = FinitePosF64::try_new(self.gamma.get())?;
        if self.min_spread_ticks == 0 {
            return Err(err_desc("min_spread_ticks must be >= 1"));
        }
        if self.min_combined_spread_micro.0 == 0
            || self.min_combined_spread_micro.0 >= ONE_DOLLAR_MICRO
        {
            return Err(err_desc(
                "min_combined_spread_micro must lie in (0, 1_000_000)",
            ));
        }
        if self.max_combined_bid_micro.0 == 0 || self.max_combined_bid_micro.0 >= ONE_DOLLAR_MICRO {
            return Err(err_desc(
                "max_combined_bid_micro must lie in (0, 1_000_000)",
            ));
        }
        if let Some(p_max) = self.max_entry_prob
            && (!p_max.is_finite() || p_max <= 0.5 || p_max >= 1.0)
        {
            return Err(err_desc("max_entry_prob must be finite in (0.5, 1.0)"));
        }
        Ok(())
    }
}

/// Active unmerged inventory cost basis and escalation override context for Station 6.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct CostContext {
    /// Active VWAP cost basis per unmerged UP share (`None` when flat UP).
    pub avg_cost_up: Option<MicroUsdc>,
    /// Active VWAP cost basis per unmerged DOWN share (`None` when flat DOWN).
    pub avg_cost_dn: Option<MicroUsdc>,
    /// Bypass cross-tick pair-cost cap during risk escalation (Tier 3/4 or flatten phases).
    pub allow_cross_tick_override: bool,
}

/// Live orderbook and resting quote boundaries consumed by Station 6's 3-way Post-Only guard.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct PostOnlyBounds {
    /// Current best ask on UP book (`None` suppresses UP bid to prevent crossing empty ask).
    pub best_ask_up: Option<MicroUsdc>,
    /// Current best ask on DOWN book (`None` suppresses DOWN bid to prevent crossing empty ask).
    pub best_ask_dn: Option<MicroUsdc>,
    /// Current best bid on UP book.
    pub best_bid_up: Option<MicroUsdc>,
    /// Current best bid on DOWN book.
    pub best_bid_dn: Option<MicroUsdc>,
    /// Our own currently active resting UP bid price, if any (self-cross guard).
    pub active_bid_up: Option<MicroUsdc>,
    /// Our own currently active resting DOWN bid price, if any (self-cross guard).
    pub active_bid_dn: Option<MicroUsdc>,
}

/// Explicit reason why Stations 1–6 produced no active passive quote on this tick.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SuppressionReason {
    /// Station 1 Savitzky-Golay filter is still warming up (`< 5` samples).
    WarmingUp,
    /// Active UP or DOWN L2 orderbook is missing bid or ask liquidity.
    EmptyOrderBook,
    /// Live UP or DOWN L2 orderbook is locked/crossed (`best_bid >= best_ask`).
    CrossedOrderBook,
    /// Quantized combined pair bid exceeded `max_combined_bid_micro` even after 1-tick step-down.
    PairMarginViolation,
    /// Both UP and DOWN bid legs were suppressed by tail tick bounds or Post-Only ceilings.
    BothLegsSuppressed,
}

/// Passive dual-bid output of Station 6.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum QuoteOutput {
    /// At least one valid Post-Only, tick-aligned passive bid leg is active.
    Active {
        /// Final UP passive bid in micro-USDC (`None` if suppressed).
        bid_up: Option<MicroUsdc>,
        /// Final DOWN passive bid in micro-USDC (`None` if suppressed).
        bid_dn: Option<MicroUsdc>,
        /// Combined `bid_up + bid_dn` pair cost when both legs are active.
        pair_cost_micro: Option<MicroUsdc>,
        /// Implied gross pair margin `(1.00 - pair_cost) * 10_000` in basis points.
        gross_margin_bps: Option<i64>,
    },
    /// Quoting suppressed cleanly with explicit reason.
    Suppressed(SuppressionReason),
}

/// Per-tick inputs passed from Stations 1–3 into Stations 4–6 (`DualBidPricer`).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PricerInput {
    /// Station 1 denoised UP fair mid-price (`USD/share`).
    pub s_i: f64,
    /// Station 2 remaining variance `V_rem` (`USD^2`).
    pub v_rem: f64,
    /// Station 3 validated liquidity intensity `kappa` (`USD^-1`).
    pub kappa: FinitePosF64,
    /// Signed net inventory `q = q_UP - q_DOWN` (`1 share = 1_000_000`).
    pub inventory_q: SignedSharesMicro,
    /// Validated market minimum tick increment in micro-USDC.
    pub tick_size_micro: MicroUsdc,
    /// Market taker/maker fee schedule.
    pub fee_schedule: FeeSchedule,
    /// Live L2 + own-resting Post-Only boundaries.
    pub bounds: PostOnlyBounds,
    /// Active orphaned-inventory VWAP cost basis and risk-escalation override flag.
    pub cost_context: CostContext,
}

/// Step-by-step trace and latencies for Stations 4–6.
#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
pub struct PricerTrace {
    /// Raw Station 4 optimal spread before floor clamp (`USD`).
    pub s_star_raw: f64,
    /// Effective minimum spread floor (`USD`).
    pub s_min: f64,
    /// Final Station 4 optimal full spread `max(s_star_raw, s_min)` (`USD`).
    pub s_star: f64,
    /// Unclamped Station 5 reservation price `s_i - q * gamma * V_rem` (`USD`).
    pub r_i_unclamped: f64,
    /// Station 5 reservation price clamped to `[tick, 1 - tick]` (`USD`).
    pub r_i: f64,
    /// Unrounded target UP bid `r_i - s*/2` (`USD`).
    pub bid_up_raw: f64,
    /// Unrounded target DOWN bid `1.0 - (r_i + s*/2)` (`USD`).
    pub bid_dn_raw: f64,
    /// Tick-floored UP bid before pair-cost / Post-Only guards.
    pub bid_up_floored: Option<MicroUsdc>,
    /// Tick-floored DOWN bid before pair-cost / Post-Only guards.
    pub bid_dn_floored: Option<MicroUsdc>,
    /// `true` when 1-tick pair-cost step-down adjusted either leg.
    pub pair_cost_adjusted: bool,
    /// `true` when cross-tick pair-cost cap lowered or suppressed `bid_up`.
    pub cross_tick_clamped_up: bool,
    /// `true` when cross-tick pair-cost cap lowered or suppressed `bid_dn`.
    pub cross_tick_clamped_dn: bool,
    /// `true` when extreme-probability entry guard suppressed `bid_up`.
    pub entry_guard_suppressed_up: bool,
    /// `true` when extreme-probability entry guard suppressed `bid_dn`.
    pub entry_guard_suppressed_dn: bool,
    /// `true` when UP Post-Only ceiling lowered `bid_up`.
    pub post_only_clamped_up: bool,
    /// `true` when DOWN Post-Only ceiling lowered `bid_dn`.
    pub post_only_clamped_dn: bool,
    /// `true` when UP bid was suppressed by missing ask or `ceiling < tick`.
    pub post_only_suppressed_up: bool,
    /// `true` when DOWN bid was suppressed by missing ask or `ceiling < tick`.
    pub post_only_suppressed_dn: bool,
    /// Final passive quote decision.
    pub quote: QuoteOutput,
    /// Station 4 compute latency in nanoseconds.
    pub s4_ns: u64,
    /// Station 5 compute latency in nanoseconds.
    pub s5_ns: u64,
    /// Station 6 compute latency in nanoseconds.
    pub s6_ns: u64,
}

/// Zero-allocation Avellaneda-Stoikov Stations 4–6 dual-bid pricer.
#[derive(Debug, Clone, Copy)]
pub struct DualBidPricer {
    config: PricerConfig,
}

impl DualBidPricer {
    /// Constructs a validated Stations 4–6 pricer.
    pub fn new(config: PricerConfig) -> Result<Self, MathError> {
        config.validate()?;
        Ok(Self { config })
    }

    /// Returns active configuration.
    #[must_use]
    pub const fn config(&self) -> PricerConfig {
        self.config
    }

    /// Computes Stations 4–6 (`s*`, `r_i`, tick flooring, pair-cost cap, 3-way Post-Only guard).
    pub fn price(
        &self,
        input: &PricerInput,
        clock: &MonotonicClock,
    ) -> Result<PricerTrace, MathError> {
        validate_pricer_input(input)?;
        let tick_usd = input.tick_size_micro.to_usd_f64();

        let t4 = clock_ns(clock)?;
        let (s_star_raw, s_min, s_star) = compute_station4(self.config, input, tick_usd)?;
        let t5 = clock_ns(clock)?;
        let (r_i_unclamped, r_i) = compute_station5(self.config.gamma.get(), input, tick_usd)?;
        let t6 = clock_ns(clock)?;
        let mut trace = compute_station6(
            self.config,
            input,
            (s_star_raw, s_min, s_star),
            (r_i_unclamped, r_i),
        )?;
        let t_end = clock_ns(clock)?;
        trace.s4_ns = t5.saturating_sub(t4);
        trace.s5_ns = t6.saturating_sub(t5);
        trace.s6_ns = t_end.saturating_sub(t6);
        Ok(trace)
    }
}

fn validate_pricer_input(input: &PricerInput) -> Result<(), MathError> {
    if !input.s_i.is_finite() {
        return Err(MathError::NonFiniteInput { value: input.s_i });
    }
    if !(0.0..=1.0).contains(&input.s_i) {
        return Err(MathError::OutOfRangePrice { value: input.s_i });
    }
    if !input.v_rem.is_finite() {
        return Err(MathError::NonFiniteInput { value: input.v_rem });
    }
    if input.v_rem < 0.0 {
        return Err(err_desc("v_rem must be >= 0.0"));
    }
    let _ = FinitePosF64::try_new(input.kappa.get())?;
    TickPrecision::from_micro(input.tick_size_micro).map_err(|_| MathError::InvalidTickSize {
        tick_micro: input.tick_size_micro.0,
    })?;
    input
        .fee_schedule
        .validate()
        .map_err(|e| MathError::InvalidMarketDescriptor {
            detail: e.to_string(),
        })?;
    Ok(())
}

fn compute_station4(
    cfg: PricerConfig,
    input: &PricerInput,
    tick_usd: f64,
) -> Result<(f64, f64, f64), MathError> {
    let gamma = cfg.gamma.get();
    let kappa = input.kappa.get();
    let var_term = if cfg.include_variance_spread {
        gamma * input.v_rem
    } else {
        0.0
    };
    let s_star_raw = var_term + (2.0 / gamma) * (1.0 + gamma / kappa).ln();
    if !s_star_raw.is_finite() || s_star_raw <= 0.0 {
        return Err(MathError::NonFiniteInput { value: s_star_raw });
    }
    let cfg_min = cfg.min_combined_spread_micro.to_usd_f64();
    let base_min = (f64::from(cfg.min_spread_ticks) * tick_usd).max(cfg_min);
    let s_min = if cfg.enforce_fee_floor {
        let fee_rate = input.fee_schedule.fee_rate_e6 as f64 / 1_000_000.0;
        let fee_floor = fee_rate * input.s_i * (1.0 - input.s_i) + tick_usd;
        base_min.max(fee_floor)
    } else {
        base_min
    };
    let s_star = safe_clamp(s_star_raw.max(s_min), s_min, 2.0)?;
    Ok((s_star_raw, s_min, s_star))
}

fn compute_station5(
    gamma: f64,
    input: &PricerInput,
    tick_usd: f64,
) -> Result<(f64, f64), MathError> {
    let q_shares = input.inventory_q.to_shares_f64();
    let r_i_unclamped = input.s_i - q_shares * gamma * input.v_rem;
    let r_i = safe_clamp(r_i_unclamped, tick_usd, 1.0 - tick_usd)?;
    Ok((r_i_unclamped, r_i))
}

fn compute_station6(
    cfg: PricerConfig,
    input: &PricerInput,
    (s_star_raw, s_min, s_star): (f64, f64, f64),
    (r_i_unclamped, r_i): (f64, f64),
) -> Result<PricerTrace, MathError> {
    let tick_micro = input.tick_size_micro;
    let tick_usd = tick_micro.to_usd_f64();
    let half = s_star / 2.0;
    let bid_up_raw = r_i - half;
    let bid_dn_raw = 1.0 - (r_i + half);
    let bid_up_floored = quantize_bid_leg(bid_up_raw, tick_micro, tick_usd)?;
    let bid_dn_floored = quantize_bid_leg(bid_dn_raw, tick_micro, tick_usd)?;

    let (quote, flags) =
        apply_pair_and_post_only(cfg, input, (bid_up_floored, bid_dn_floored), tick_micro);
    Ok(PricerTrace {
        s_star_raw,
        s_min,
        s_star,
        r_i_unclamped,
        r_i,
        bid_up_raw,
        bid_dn_raw,
        bid_up_floored,
        bid_dn_floored,
        pair_cost_adjusted: flags.pair_adjusted,
        cross_tick_clamped_up: flags.cross_tick_clamped_up,
        cross_tick_clamped_dn: flags.cross_tick_clamped_dn,
        entry_guard_suppressed_up: flags.entry_guard_suppressed_up,
        entry_guard_suppressed_dn: flags.entry_guard_suppressed_dn,
        post_only_clamped_up: flags.clamped_up,
        post_only_clamped_dn: flags.clamped_dn,
        post_only_suppressed_up: flags.suppressed_up,
        post_only_suppressed_dn: flags.suppressed_dn,
        quote,
        s4_ns: 0,
        s5_ns: 0,
        s6_ns: 0,
    })
}

#[derive(Default)]
struct Station6Flags {
    pair_adjusted: bool,
    cross_tick_clamped_up: bool,
    cross_tick_clamped_dn: bool,
    entry_guard_suppressed_up: bool,
    entry_guard_suppressed_dn: bool,
    clamped_up: bool,
    clamped_dn: bool,
    suppressed_up: bool,
    suppressed_dn: bool,
}

fn apply_pair_and_post_only(
    cfg: PricerConfig,
    input: &PricerInput,
    (bid_up_floored, bid_dn_floored): (Option<MicroUsdc>, Option<MicroUsdc>),
    tick: MicroUsdc,
) -> (QuoteOutput, Station6Flags) {
    let mut flags = Station6Flags::default();
    let (mut up_guarded, mut dn_guarded) = (bid_up_floored, bid_dn_floored);

    if let Some(p_max) = cfg.max_entry_prob {
        apply_entry_prob_guard(
            &mut up_guarded,
            &mut dn_guarded,
            input.inventory_q,
            p_max,
            &mut flags,
        );
    }

    if let Some(loss) = cfg.max_cross_tick_loss_micro
        && !input.cost_context.allow_cross_tick_override
    {
        if input.inventory_q.0 > 0
            && let Some(cost_up) = input.cost_context.avg_cost_up
        {
            dn_guarded = apply_cross_tick_cap(
                dn_guarded,
                cost_up,
                cfg.max_combined_bid_micro,
                loss,
                tick,
                &mut flags.cross_tick_clamped_dn,
            );
        } else if input.inventory_q.0 < 0
            && let Some(cost_dn) = input.cost_context.avg_cost_dn
        {
            up_guarded = apply_cross_tick_cap(
                up_guarded,
                cost_dn,
                cfg.max_combined_bid_micro,
                loss,
                tick,
                &mut flags.cross_tick_clamped_up,
            );
        }
    }

    let (up_paired, dn_paired, violated) = enforce_pair_cost(
        up_guarded,
        dn_guarded,
        input.inventory_q,
        tick,
        cfg.max_combined_bid_micro,
        &mut flags.pair_adjusted,
    );
    if violated {
        return (
            QuoteOutput::Suppressed(SuppressionReason::PairMarginViolation),
            flags,
        );
    }

    let b = input.bounds;
    let (mut bid_up, clamped_up, sup_up) = apply_post_only_leg(
        up_paired,
        b.best_ask_up,
        b.best_bid_dn,
        b.active_bid_dn,
        tick,
    );
    let (mut bid_dn, clamped_dn, sup_dn) = apply_post_only_leg(
        dn_paired,
        b.best_ask_dn,
        b.best_bid_up,
        b.active_bid_up,
        tick,
    );
    flags.clamped_up = clamped_up;
    flags.clamped_dn = clamped_dn;
    flags.suppressed_up = sup_up;
    flags.suppressed_dn = sup_dn;

    if let Some(p_max) = cfg.max_entry_prob {
        apply_entry_prob_guard(
            &mut bid_up,
            &mut bid_dn,
            input.inventory_q,
            p_max,
            &mut flags,
        );
    }

    let quote = finalize_quote(bid_up, bid_dn);
    (quote, flags)
}

/// Returns `true` when `bid` lies strictly outside `[1.0 - p_max, p_max]` in micro-USDC.
#[must_use]
pub fn exceeds_entry_prob_bounds(bid: MicroUsdc, p_max: f64) -> bool {
    let max_micro = ((p_max * ONE_DOLLAR_MICRO as f64) + 1e-9).floor() as u64;
    let min_micro = ONE_DOLLAR_MICRO.saturating_sub(max_micro);
    bid.0 > max_micro || bid.0 < min_micro
}

fn apply_entry_prob_guard(
    bid_up: &mut Option<MicroUsdc>,
    bid_dn: &mut Option<MicroUsdc>,
    inventory_q: SignedSharesMicro,
    p_max: f64,
    flags: &mut Station6Flags,
) {
    if inventory_q.0 >= 0
        && let Some(u) = *bid_up
        && exceeds_entry_prob_bounds(u, p_max)
    {
        *bid_up = None;
        flags.entry_guard_suppressed_up = true;
    }
    if inventory_q.0 <= 0
        && let Some(d) = *bid_dn
        && exceeds_entry_prob_bounds(d, p_max)
    {
        *bid_dn = None;
        flags.entry_guard_suppressed_dn = true;
    }
}

fn apply_cross_tick_cap(
    leg_bid: Option<MicroUsdc>,
    held_avg_cost: MicroUsdc,
    max_combined_bid_micro: MicroUsdc,
    loss_micro: u64,
    tick: MicroUsdc,
    clamped_flag: &mut bool,
) -> Option<MicroUsdc> {
    let bid = leg_bid?;
    let budget = u128::from(max_combined_bid_micro.0).saturating_add(u128::from(loss_micro));
    let cost = u128::from(held_avg_cost.0);
    if budget <= cost {
        *clamped_flag = true;
        return None;
    }
    let tick_u128 = u128::from(tick.0);
    let cap_u128 = ((budget - cost) / tick_u128) * tick_u128;
    if cap_u128 < tick_u128 {
        *clamped_flag = true;
        return None;
    }
    let cap = MicroUsdc(u64::try_from(cap_u128).unwrap_or(u64::MAX));
    if bid > cap {
        *clamped_flag = true;
        Some(cap)
    } else {
        Some(bid)
    }
}

fn quantize_bid_leg(
    raw_usd: f64,
    tick_micro: MicroUsdc,
    tick_usd: f64,
) -> Result<Option<MicroUsdc>, MathError> {
    if !raw_usd.is_finite() {
        return Err(MathError::NonFiniteInput { value: raw_usd });
    }
    if raw_usd < tick_usd {
        return Ok(None);
    }
    let capped = raw_usd.min(1.0 - tick_usd);
    let floored = floor_to_tick(capped, tick_micro)?;
    if floored < tick_micro {
        Ok(None)
    } else {
        Ok(Some(floored))
    }
}

/// Checks `bid_up + bid_dn <= pair_cap`; steps down heavier-inventory leg by 1 tick before failing cleanly.
pub(crate) fn enforce_pair_cost(
    bid_up: Option<MicroUsdc>,
    bid_dn: Option<MicroUsdc>,
    inventory_q: SignedSharesMicro,
    tick: MicroUsdc,
    max_combined_bid_micro: MicroUsdc,
    adjusted: &mut bool,
) -> (Option<MicroUsdc>, Option<MicroUsdc>, bool) {
    let (Some(mut up), Some(mut dn)) = (bid_up, bid_dn) else {
        return (bid_up, bid_dn, false);
    };
    let pair_cap = max_combined_bid_micro
        .0
        .min(ONE_DOLLAR_MICRO.saturating_sub(tick.0));
    if up.0.saturating_add(dn.0) <= pair_cap {
        return (Some(up), Some(dn), false);
    }
    *adjusted = true;
    if inventory_q.0 >= 0 {
        if up.0 < tick.0 * 2 {
            return (Some(up), Some(dn), true);
        }
        up = MicroUsdc(up.0 - tick.0);
    } else {
        if dn.0 < tick.0 * 2 {
            return (Some(up), Some(dn), true);
        }
        dn = MicroUsdc(dn.0 - tick.0);
    }
    let still_bad = up.0.saturating_add(dn.0) > pair_cap;
    (Some(up), Some(dn), still_bad)
}

fn apply_post_only_leg(
    target: Option<MicroUsdc>,
    best_ask_leg: Option<MicroUsdc>,
    best_bid_opp: Option<MicroUsdc>,
    active_bid_opp: Option<MicroUsdc>,
    tick: MicroUsdc,
) -> (Option<MicroUsdc>, bool, bool) {
    let Some(tgt) = target else {
        return (None, false, false);
    };
    let Some(ask) = best_ask_leg else {
        return (None, false, true);
    };
    let mut tightest_ask = ask.0;
    if let Some(b_opp) = best_bid_opp {
        tightest_ask = tightest_ask.min(ONE_DOLLAR_MICRO.saturating_sub(b_opp.0));
    }
    if let Some(a_opp) = active_bid_opp {
        tightest_ask = tightest_ask.min(ONE_DOLLAR_MICRO.saturating_sub(a_opp.0));
    }
    let max_bid_raw = tightest_ask.saturating_sub(tick.0);
    let ceiling = MicroUsdc((max_bid_raw / tick.0) * tick.0);
    if ceiling < tick {
        return (None, false, true);
    }
    if tgt > ceiling {
        (Some(ceiling), true, false)
    } else {
        (Some(tgt), false, false)
    }
}

fn finalize_quote(bid_up: Option<MicroUsdc>, bid_dn: Option<MicroUsdc>) -> QuoteOutput {
    if bid_up.is_none() && bid_dn.is_none() {
        return QuoteOutput::Suppressed(SuppressionReason::BothLegsSuppressed);
    }
    let pair_cost_micro = bid_up.zip(bid_dn).map(|(u, d)| MicroUsdc(u.0 + d.0));
    let gross_margin_bps = pair_cost_micro.map(|c| {
        let diff = (ONE_DOLLAR_MICRO as i64) - (c.0.min(2 * ONE_DOLLAR_MICRO) as i64);
        (diff * 10_000) / ONE_DOLLAR_MICRO as i64
    });
    QuoteOutput::Active {
        bid_up,
        bid_dn,
        pair_cost_micro,
        gross_margin_bps,
    }
}

fn clock_ns(clock: &MonotonicClock) -> Result<u64, MathError> {
    clock.now_ns().map_err(|_| MathError::Overflow)
}

fn err_desc(msg: &str) -> MathError {
    MathError::InvalidMarketDescriptor {
        detail: msg.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use core_types::ONE_SHARE_MICRO;

    use super::*;

    fn assert_close(actual: f64, expected: f64, tol: f64) {
        assert!(
            (actual - expected).abs() <= tol,
            "actual={actual} expected={expected} diff={}",
            (actual - expected).abs()
        );
    }

    fn base_bounds() -> PostOnlyBounds {
        PostOnlyBounds {
            best_ask_up: Some(MicroUsdc(950_000)),
            best_ask_dn: Some(MicroUsdc(950_000)),
            best_bid_up: Some(MicroUsdc(100_000)),
            best_bid_dn: Some(MicroUsdc(100_000)),
            active_bid_up: None,
            active_bid_dn: None,
        }
    }

    fn make_input(q_whole: i64, tick_micro: u64, bounds: PostOnlyBounds) -> PricerInput {
        PricerInput {
            s_i: 0.50,
            v_rem: 0.03,
            kappa: FinitePosF64::try_new(50.0).unwrap(),
            inventory_q: SignedSharesMicro(q_whole * ONE_SHARE_MICRO as i64),
            tick_size_micro: MicroUsdc(tick_micro),
            fee_schedule: FeeSchedule::btc_5m_default(),
            bounds,
            cost_context: CostContext::default(),
        }
    }

    #[test]
    fn vector4_stations_4_to_6_zero_and_positive_inventory_exact() {
        let clock = MonotonicClock::start();
        let mut cfg = PricerConfig::try_default().unwrap();
        cfg.include_variance_spread = false;
        let pricer_no_var = DualBidPricer::new(cfg).unwrap();

        let t0 = pricer_no_var
            .price(&make_input(0, 10_000, base_bounds()), &clock)
            .unwrap();
        assert_close(t0.s_star, 0.039_940_12, 1e-6);
        assert_close(t0.r_i, 0.50, 1e-9);
        assert_eq!(
            t0.quote,
            QuoteOutput::Active {
                bid_up: Some(MicroUsdc(480_000)),
                bid_dn: Some(MicroUsdc(480_000)),
                pair_cost_micro: Some(MicroUsdc(960_000)),
                gross_margin_bps: Some(400),
            }
        );

        let t_pos = pricer_no_var
            .price(&make_input(20, 10_000, base_bounds()), &clock)
            .unwrap();
        assert_close(t_pos.r_i, 0.41, 1e-9);
        assert_eq!(
            t_pos.quote,
            QuoteOutput::Active {
                bid_up: Some(MicroUsdc(390_000)),
                bid_dn: Some(MicroUsdc(570_000)),
                pair_cost_micro: Some(MicroUsdc(960_000)),
                gross_margin_bps: Some(400),
            }
        );

        let pricer_with_var = DualBidPricer::new(PricerConfig::try_default().unwrap()).unwrap();
        let t_var = pricer_with_var
            .price(&make_input(0, 10_000, base_bounds()), &clock)
            .unwrap();
        assert_close(t_var.s_star, 0.044_440_12, 1e-6);
        assert_eq!(
            t_var.quote,
            QuoteOutput::Active {
                bid_up: Some(MicroUsdc(470_000)),
                bid_dn: Some(MicroUsdc(470_000)),
                pair_cost_micro: Some(MicroUsdc(940_000)),
                gross_margin_bps: Some(600),
            }
        );
    }

    #[test]
    fn adversarial_heavy_inventory_tail_tick_change_pair_cost_and_3way_post_only() {
        let clock = MonotonicClock::start();
        let pricer = DualBidPricer::new(PricerConfig::try_default().unwrap()).unwrap();

        // Heavy q=+100 shifts r_i downward cleanly, q=-100 shifts r_i upward cleanly; zero NaN/negative.
        let mut inp_long = make_input(100, 10_000, base_bounds());
        inp_long.v_rem = 0.125;
        let tr_long = pricer.price(&inp_long, &clock).unwrap();
        assert!(tr_long.r_i < 0.50 && tr_long.r_i >= 0.01);
        assert!(tr_long.bid_up_floored.is_none()); // negative raw bid suppressed cleanly
        assert!(matches!(
            tr_long.quote,
            QuoteOutput::Active {
                bid_up: None,
                bid_dn: Some(_),
                ..
            }
        ));

        let mut inp_short = make_input(-100, 10_000, base_bounds());
        inp_short.v_rem = 0.125;
        let tr_short = pricer.price(&inp_short, &clock).unwrap();
        assert!(tr_short.r_i > 0.50 && tr_short.r_i <= 0.99);
        assert!(tr_short.bid_dn_floored.is_none());

        // Dynamic tick change 0.01 -> 0.001 at tail p=0.02 produces valid 3-decimal ticks, pair_cost <= 0.98.
        // Disable entry guard for this sub-case so flat tail tick alignment is exercised directly.
        let mut tail_cfg = PricerConfig::try_default().unwrap();
        tail_cfg.max_entry_prob = None;
        let tail_pricer = DualBidPricer::new(tail_cfg).unwrap();
        let mut inp_tail = make_input(0, 1_000, base_bounds());
        inp_tail.s_i = 0.02;
        let tr_tail = tail_pricer.price(&inp_tail, &clock).unwrap();
        match tr_tail.quote {
            QuoteOutput::Active {
                bid_up,
                bid_dn,
                pair_cost_micro,
                ..
            } => {
                if let Some(u) = bid_up {
                    assert_eq!(u.0 % 1_000, 0);
                }
                if let Some(d) = bid_dn {
                    assert_eq!(d.0 % 1_000, 0);
                }
                if let Some(p) = pair_cost_micro {
                    assert!(p.0 <= 980_000);
                }
            }
            other => panic!("expected Active quote for tail tick-size check, got {other:?}"),
        }

        // Pair-cost 1-tick step-down ($0.99 -> $0.98) and unresolvable ($1.00 -> PairMarginViolation).
        let mut adjusted = false;
        let (u99, d99, v99) = enforce_pair_cost(
            Some(MicroUsdc(490_000)),
            Some(MicroUsdc(500_000)),
            SignedSharesMicro(0),
            MicroUsdc(10_000),
            MicroUsdc(980_000),
            &mut adjusted,
        );
        assert!(adjusted && !v99);
        assert_eq!(u99.unwrap().0 + d99.unwrap().0, 980_000);

        let mut adjusted2 = false;
        let (_, _, v100) = enforce_pair_cost(
            Some(MicroUsdc(500_000)),
            Some(MicroUsdc(500_000)),
            SignedSharesMicro(0),
            MicroUsdc(10_000),
            MicroUsdc(980_000),
            &mut adjusted2,
        );
        assert!(adjusted2 && v100);

        // 3-way Post-Only guard: empty ask, best_ask == tick, opp best_bid, opp active_bid.
        let mut b1 = base_bounds();
        b1.best_ask_up = None;
        let t_no_ask = pricer.price(&make_input(0, 10_000, b1), &clock).unwrap();
        assert!(t_no_ask.post_only_suppressed_up);
        assert!(matches!(
            t_no_ask.quote,
            QuoteOutput::Active {
                bid_up: None,
                bid_dn: Some(_),
                ..
            }
        ));

        let mut b2 = base_bounds();
        b2.best_ask_up = Some(MicroUsdc(10_000)); // equals tick -> UP suppressed
        b2.best_bid_up = Some(MicroUsdc(550_000)); // 1 - 0.55 - 0.01 = 0.44 < 0.47 -> DN clamped!
        let t_syn = pricer.price(&make_input(0, 10_000, b2), &clock).unwrap();
        assert!(t_syn.post_only_suppressed_up);
        assert!(t_syn.post_only_clamped_dn);
        assert!(matches!(
            t_syn.quote,
            QuoteOutput::Active {
                bid_up: None,
                bid_dn: Some(MicroUsdc(440_000)),
                ..
            }
        ));

        let mut b3 = base_bounds();
        b3.active_bid_dn = Some(MicroUsdc(540_000)); // 1 - 0.54 - 0.01 = 0.45 -> UP self-cross clamped!
        let t_self = pricer.price(&make_input(0, 10_000, b3), &clock).unwrap();
        assert!(t_self.post_only_clamped_up);
        assert!(matches!(
            t_self.quote,
            QuoteOutput::Active {
                bid_up: Some(MicroUsdc(450_000)),
                ..
            }
        ));
    }

    #[test]
    fn bbo_join_at_99_cent_pair_cost_is_post_only_safe() {
        let clock = MonotonicClock::start();
        let cfg = PricerConfig {
            gamma: FinitePosF64::try_new(0.02).unwrap(),
            include_variance_spread: false,
            enforce_fee_floor: false,
            min_combined_spread_micro: MicroUsdc(10_000),
            min_spread_ticks: 1,
            max_combined_bid_micro: MicroUsdc(990_000),
            max_cross_tick_loss_micro: None,
            max_entry_prob: None,
        };
        let pricer = DualBidPricer::new(cfg).unwrap();
        let bounds = PostOnlyBounds {
            best_bid_up: Some(MicroUsdc(340_000)),
            best_ask_up: Some(MicroUsdc(350_000)),
            best_bid_dn: Some(MicroUsdc(650_000)),
            best_ask_dn: Some(MicroUsdc(660_000)),
            active_bid_up: None,
            active_bid_dn: None,
        };
        let input = PricerInput {
            s_i: 0.345,
            v_rem: 0.03,
            kappa: FinitePosF64::try_new(200.0).unwrap(),
            inventory_q: SignedSharesMicro::ZERO,
            tick_size_micro: MicroUsdc(10_000),
            fee_schedule: FeeSchedule::btc_5m_default(),
            bounds,
            cost_context: CostContext::default(),
        };
        let t = pricer.price(&input, &clock).unwrap();
        assert_close(t.s_min, 0.01, 1e-12);
        assert!(!t.post_only_clamped_up);
        assert!(!t.post_only_clamped_dn);
        assert!(!t.pair_cost_adjusted);
        assert_eq!(
            t.quote,
            QuoteOutput::Active {
                bid_up: Some(MicroUsdc(340_000)),
                bid_dn: Some(MicroUsdc(650_000)),
                pair_cost_micro: Some(MicroUsdc(990_000)),
                gross_margin_bps: Some(100),
            }
        );

        let mut self_input = input;
        self_input.bounds.active_bid_up = Some(MicroUsdc(340_000));
        self_input.bounds.active_bid_dn = Some(MicroUsdc(650_000));
        let t_self = pricer.price(&self_input, &clock).unwrap();
        assert!(!t_self.post_only_clamped_up);
        assert!(!t_self.post_only_clamped_dn);
        assert_eq!(t_self.quote, t.quote);
    }

    #[test]
    fn cross_tick_pair_cap_clamps_symmetrically_and_respects_override_and_underflow() {
        let clock = MonotonicClock::start();
        let mut base_cfg = PricerConfig::try_default().unwrap();
        base_cfg.include_variance_spread = false;
        base_cfg.enforce_fee_floor = false;
        base_cfg.min_combined_spread_micro = MicroUsdc(20_000);
        base_cfg.min_spread_ticks = 2;
        base_cfg.max_combined_bid_micro = MicroUsdc(980_000);

        // Input with q=+20 (heavy UP), s_i=0.80, v_rem=0.04, gamma=0.15 =>
        // r_i = 0.80 - 20 * 0.15 * 0.04 = 0.68, s* ~ 0.0399 => bid_up=$0.66, bid_dn=$0.30 (or similar).
        // Tune v_rem=0 so r_i=s_i=0.72, s*=0.0399 => bid_up=$0.70, bid_dn=$0.26.
        let mut inp = make_input(20, 10_000, base_bounds());
        inp.s_i = 0.72;
        inp.v_rem = 0.0;
        inp.cost_context = CostContext {
            avg_cost_up: Some(MicroUsdc(910_000)),
            avg_cost_dn: None,
            allow_cross_tick_override: false,
        };

        let base_pricer = DualBidPricer::new(base_cfg).unwrap();
        let t_base = base_pricer.price(&inp, &clock).unwrap();
        assert_eq!(
            t_base.quote,
            QuoteOutput::Active {
                bid_up: Some(MicroUsdc(700_000)),
                bid_dn: Some(MicroUsdc(260_000)),
                pair_cost_micro: Some(MicroUsdc(960_000)),
                gross_margin_bps: Some(400),
            }
        );

        // Loss = 0 => cap = 980_000 + 0 - 910_000 = 70_000 ($0.07).
        let mut cfg_zero = base_cfg;
        cfg_zero.max_cross_tick_loss_micro = Some(0);
        let pricer_zero = DualBidPricer::new(cfg_zero).unwrap();
        let t_zero = pricer_zero.price(&inp, &clock).unwrap();
        // Stations 4-5 must be 100% identical to baseline!
        assert_close(t_zero.s_star, t_base.s_star, 1e-12);
        assert_close(t_zero.r_i, t_base.r_i, 1e-12);
        assert!(!t_zero.cross_tick_clamped_up);
        assert!(t_zero.cross_tick_clamped_dn);
        assert_eq!(
            t_zero.quote,
            QuoteOutput::Active {
                bid_up: Some(MicroUsdc(700_000)),
                bid_dn: Some(MicroUsdc(70_000)),
                pair_cost_micro: Some(MicroUsdc(770_000)),
                gross_margin_bps: Some(2_300),
            }
        );

        // Loss = 20_000 => cap = 980_000 + 20_000 - 910_000 = 90_000 ($0.09).
        let mut cfg_20k = base_cfg;
        cfg_20k.max_cross_tick_loss_micro = Some(20_000);
        let pricer_20k = DualBidPricer::new(cfg_20k).unwrap();
        let t_20k = pricer_20k.price(&inp, &clock).unwrap();
        assert!(t_20k.cross_tick_clamped_dn);
        assert_eq!(
            t_20k.quote,
            QuoteOutput::Active {
                bid_up: Some(MicroUsdc(700_000)),
                bid_dn: Some(MicroUsdc(90_000)),
                pair_cost_micro: Some(MicroUsdc(790_000)),
                gross_margin_bps: Some(2_100),
            }
        );

        // Risk escalation override bypasses cross-tick cap cleanly.
        let mut inp_override = inp;
        inp_override.cost_context.allow_cross_tick_override = true;
        let t_override = pricer_zero.price(&inp_override, &clock).unwrap();
        assert!(!t_override.cross_tick_clamped_dn);
        assert_eq!(t_override.quote, t_base.quote);

        // Symmetry for q < 0 (heavy DN): avg_cost_dn = $0.91, bid_up = $0.26 -> clamped to $0.07 / $0.09.
        let mut inp_neg = make_input(-20, 10_000, base_bounds());
        inp_neg.s_i = 0.28;
        inp_neg.v_rem = 0.0;
        inp_neg.cost_context = CostContext {
            avg_cost_up: None,
            avg_cost_dn: Some(MicroUsdc(910_000)),
            allow_cross_tick_override: false,
        };
        let t_neg_zero = pricer_zero.price(&inp_neg, &clock).unwrap();
        assert!(t_neg_zero.cross_tick_clamped_up);
        assert!(!t_neg_zero.cross_tick_clamped_dn);
        assert_eq!(
            t_neg_zero.quote,
            QuoteOutput::Active {
                bid_up: Some(MicroUsdc(70_000)),
                bid_dn: Some(MicroUsdc(700_000)),
                pair_cost_micro: Some(MicroUsdc(770_000)),
                gross_margin_bps: Some(2_300),
            }
        );
        let t_neg_20k = pricer_20k.price(&inp_neg, &clock).unwrap();
        assert!(t_neg_20k.cross_tick_clamped_up);
        assert_eq!(
            t_neg_20k.quote,
            QuoteOutput::Active {
                bid_up: Some(MicroUsdc(90_000)),
                bid_dn: Some(MicroUsdc(700_000)),
                pair_cost_micro: Some(MicroUsdc(790_000)),
                gross_margin_bps: Some(2_100),
            }
        );

        // Zero underflow when cost_up >= max_combined_bid_micro + loss (suppresses DN cleanly).
        for extreme_cost in [980_000, 990_000, 995_000] {
            let mut inp_underflow = inp;
            inp_underflow.cost_context.avg_cost_up = Some(MicroUsdc(extreme_cost));
            let t_uf = pricer_zero.price(&inp_underflow, &clock).unwrap();
            assert!(t_uf.cross_tick_clamped_dn);
            assert_eq!(
                t_uf.quote,
                QuoteOutput::Active {
                    bid_up: Some(MicroUsdc(700_000)),
                    bid_dn: None,
                    pair_cost_micro: None,
                    gross_margin_bps: None,
                }
            );
        }
    }

    #[test]
    fn extreme_probability_entry_guard_blocks_fresh_entries_but_allows_hedges() {
        let clock = MonotonicClock::start();
        let mut cfg = PricerConfig::try_default().unwrap();
        cfg.include_variance_spread = false;
        cfg.enforce_fee_floor = false;
        cfg.max_entry_prob = Some(0.85);
        let pricer = DualBidPricer::new(cfg).unwrap();

        // Bounds with wide ask so Post-Only never suppresses $0.91.
        let mut bounds = base_bounds();
        bounds.best_ask_up = Some(MicroUsdc(950_000));
        bounds.best_ask_dn = Some(MicroUsdc(950_000));

        // Flat (q=0) at s_i = 0.93 => bid_up=$0.91 (> 0.85) and bid_dn=$0.05 (< 0.15): both blocked!
        let mut inp_flat = make_input(0, 10_000, bounds);
        inp_flat.s_i = 0.93;
        inp_flat.v_rem = 0.0;
        let t_flat = pricer.price(&inp_flat, &clock).unwrap();
        assert!(t_flat.entry_guard_suppressed_up);
        assert!(t_flat.entry_guard_suppressed_dn);
        assert_eq!(
            t_flat.quote,
            QuoteOutput::Suppressed(SuppressionReason::BothLegsSuppressed)
        );

        // Boundary check: exact $0.85 (bid_up=$0.85, bid_dn=$0.11): $0.85 allowed, $0.11 blocked.
        let mut inp_edge = make_input(0, 10_000, bounds);
        inp_edge.s_i = 0.87;
        inp_edge.v_rem = 0.0;
        let t_edge = pricer.price(&inp_edge, &clock).unwrap();
        assert!(!t_edge.entry_guard_suppressed_up);
        assert!(t_edge.entry_guard_suppressed_dn);
        assert_eq!(
            t_edge.quote,
            QuoteOutput::Active {
                bid_up: Some(MicroUsdc(850_000)),
                bid_dn: None,
                pair_cost_micro: None,
                gross_margin_bps: None,
            }
        );

        // Boundary check: exact $0.15 is allowed (avoids 1.0 - 0.85 IEEE-754 precision trap).
        let mut inp_edge_low = make_input(0, 10_000, bounds);
        inp_edge_low.s_i = 0.17;
        inp_edge_low.v_rem = 0.0;
        let t_edge_low = pricer.price(&inp_edge_low, &clock).unwrap();
        assert!(!t_edge_low.entry_guard_suppressed_up);
        assert!(!t_edge_low.entry_guard_suppressed_dn);
        assert_eq!(
            t_edge_low.quote,
            QuoteOutput::Active {
                bid_up: Some(MicroUsdc(150_000)),
                bid_dn: Some(MicroUsdc(810_000)),
                pair_cost_micro: Some(MicroUsdc(960_000)),
                gross_margin_bps: Some(400),
            }
        );

        // Long UP (q=+20) at s_i = 0.93 => bid_up=$0.91 blocked, bid_dn=$0.05 ALLOWED as hedge!
        let mut inp_long = make_input(20, 10_000, bounds);
        inp_long.s_i = 0.93;
        inp_long.v_rem = 0.0;
        let t_long = pricer.price(&inp_long, &clock).unwrap();
        assert!(t_long.entry_guard_suppressed_up);
        assert!(!t_long.entry_guard_suppressed_dn);
        assert_eq!(
            t_long.quote,
            QuoteOutput::Active {
                bid_up: None,
                bid_dn: Some(MicroUsdc(50_000)),
                pair_cost_micro: None,
                gross_margin_bps: None,
            }
        );

        // Long DOWN (q=-20) at s_i = 0.07 => bid_dn=$0.91 blocked, bid_up=$0.05 ALLOWED as hedge!
        let mut inp_short = make_input(-20, 10_000, bounds);
        inp_short.s_i = 0.07;
        inp_short.v_rem = 0.0;
        let t_short = pricer.price(&inp_short, &clock).unwrap();
        assert!(!t_short.entry_guard_suppressed_up);
        assert!(t_short.entry_guard_suppressed_dn);
        assert_eq!(
            t_short.quote,
            QuoteOutput::Active {
                bid_up: Some(MicroUsdc(50_000)),
                bid_dn: None,
                pair_cost_micro: None,
                gross_margin_bps: None,
            }
        );

        // Invalid max_entry_prob bounds rejected cleanly.
        for bad in [0.5, 0.49, 1.0, 1.01, f64::NAN, f64::INFINITY] {
            let mut bad_cfg = cfg;
            bad_cfg.max_entry_prob = Some(bad);
            assert!(DualBidPricer::new(bad_cfg).is_err());
        }
    }
}
