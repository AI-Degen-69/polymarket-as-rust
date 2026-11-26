use core_types::{MicroUsdc, ONE_SHARE_MICRO, Outcome, SharesMicro, SignedSharesMicro};
use serde::{Deserialize, Serialize};

use crate::error::RiskLifecycleError;
use crate::lifecycle::EpochPhase;

/// Multi-tier inventory and dollar-notional risk classification.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RiskTier {
    /// Utilization `u <= 0.20`: symmetric baseline quoting with linear sizing taper.
    Tier1Normal,
    /// Utilization `0.20 < u <= 0.60`: asymmetric quoting (2.0x reducing, 0.5x accumulating).
    Tier2ModerateSkew,
    /// Utilization `0.60 < u <= 0.90`: one-sided quoting (2.5x reducing, 0x accumulating / cancel).
    Tier3OneSided,
    /// Utilization `u > 0.90`: hard emergency stop, drop quotes, fire taker flatten FAK.
    Tier4HardStop,
}

/// Returns `true` when Station 6 cross-tick pair-cost capping may be bypassed
/// to allow tail-risk reduction during Tier 3/4 escalation or end-of-epoch
/// passive offload / mandatory flatten phases.
#[must_use]
pub const fn allows_cross_tick_override(tier: RiskTier, phase: Option<EpochPhase>) -> bool {
    matches!(tier, RiskTier::Tier3OneSided | RiskTier::Tier4HardStop)
        || matches!(
            phase,
            Some(EpochPhase::PreExpiryLockout | EpochPhase::MandatoryFlatten)
        )
}

/// Stateful latch for early Tier-1/Tier-2 passive 1:1 deficit cross-tick override (B+).
///
/// Unlocks cross-tick pair-cost capping when:
/// - `trigger_price_micro` is configured (`Some(p)`),
/// - `cap_tier1_reducing_to_delta` is enabled (preventing reducing-leg overshoot),
/// - active risk tier is `Tier1Normal` or `Tier2ModerateSkew`,
/// - `|q| >= min_order_size_micro`, and
/// - the reducing (opposing) leg's `best_ask >= trigger_price_micro`.
///
/// Once triggered, the override stays latched for the active `heavy_leg` until
/// `|q| < min_order_size_micro`, the heavy leg flips, or `reset()` is called on epoch rollover.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DeficitHedgeOverrideLatch {
    latched_heavy_leg: Option<Outcome>,
}

impl DeficitHedgeOverrideLatch {
    /// Creates an unlatched deficit hedge override tracker.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            latched_heavy_leg: None,
        }
    }

    /// Resets the latch (must be called on epoch rollover).
    pub fn reset(&mut self) {
        self.latched_heavy_leg = None;
    }

    /// Returns `true` when the override is currently latched.
    #[must_use]
    pub const fn is_latched(&self) -> bool {
        self.latched_heavy_leg.is_some()
    }

    /// Returns the heavy leg for which the override is currently latched, if any.
    #[must_use]
    pub const fn latched_heavy_leg(&self) -> Option<Outcome> {
        self.latched_heavy_leg
    }

    /// Evaluates whether the B+ deficit cross-tick override is active on this tick.
    #[allow(clippy::too_many_arguments)]
    pub fn evaluate(
        &mut self,
        tier: RiskTier,
        inventory_q: SignedSharesMicro,
        min_order_size_micro: SharesMicro,
        best_ask_up: Option<MicroUsdc>,
        best_ask_dn: Option<MicroUsdc>,
        trigger_price_micro: Option<MicroUsdc>,
        cap_tier1_reducing_to_delta: bool,
    ) -> bool {
        let Some(trigger) = trigger_price_micro else {
            self.latched_heavy_leg = None;
            return false;
        };
        if !cap_tier1_reducing_to_delta {
            self.latched_heavy_leg = None;
            return false;
        }
        let abs_q = inventory_q.0.unsigned_abs();
        if abs_q == 0 || abs_q < min_order_size_micro.as_u64() {
            self.latched_heavy_leg = None;
            return false;
        }
        let heavy = if inventory_q.0 > 0 {
            Outcome::Up
        } else {
            Outcome::Down
        };
        if self.latched_heavy_leg != Some(heavy) {
            self.latched_heavy_leg = None;
        }
        if !matches!(tier, RiskTier::Tier1Normal | RiskTier::Tier2ModerateSkew) {
            self.latched_heavy_leg = None;
            return false;
        }
        if self.latched_heavy_leg == Some(heavy) {
            return true;
        }
        let opposing_ask = match heavy {
            Outcome::Up => best_ask_dn,
            Outcome::Down => best_ask_up,
        };
        if opposing_ask.is_some_and(|ask| ask >= trigger) {
            self.latched_heavy_leg = Some(heavy);
            return true;
        }
        false
    }
}

/// Point-in-time snapshot of the execution order registry and position ledger.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrderRegistrySnapshot {
    /// Active unmerged filled UP outcome shares.
    pub unhedged_up: SharesMicro,
    /// Active unmerged filled DOWN outcome shares.
    pub unhedged_dn: SharesMicro,
    /// Total acquisition cost basis of unmerged UP shares in micro-USDC.
    pub unhedged_cost_up_micro: MicroUsdc,
    /// Total acquisition cost basis of unmerged DOWN shares in micro-USDC.
    pub unhedged_cost_dn_micro: MicroUsdc,
    /// Committed dollar notional of resting open UP bids.
    pub open_bid_notional_up: MicroUsdc,
    /// Committed dollar notional of resting open DOWN bids.
    pub open_bid_notional_dn: MicroUsdc,
    /// Committed open bid shares for UP outcome.
    pub open_bid_shares_up: SharesMicro,
    /// Committed open bid shares for DOWN outcome.
    pub open_bid_shares_dn: SharesMicro,
    /// Cost basis of matched hedged pairs.
    pub paired_cost_basis: MicroUsdc,
    /// Total available unallocated USDC capital for this market.
    pub available_usdc_micro: MicroUsdc,
}

impl Default for OrderRegistrySnapshot {
    fn default() -> Self {
        Self {
            unhedged_up: SharesMicro::ZERO,
            unhedged_dn: SharesMicro::ZERO,
            unhedged_cost_up_micro: MicroUsdc::ZERO,
            unhedged_cost_dn_micro: MicroUsdc::ZERO,
            open_bid_notional_up: MicroUsdc::ZERO,
            open_bid_notional_dn: MicroUsdc::ZERO,
            open_bid_shares_up: SharesMicro::ZERO,
            open_bid_shares_dn: SharesMicro::ZERO,
            paired_cost_basis: MicroUsdc::ZERO,
            available_usdc_micro: MicroUsdc::ZERO,
        }
    }
}

/// Sizing and policy limits for Station 7 dollar-notional risk engine.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RiskConfig {
    /// Maximum allowable unhedged dollar capital per leg (`V_max`).
    pub v_max_micro: MicroUsdc,
    /// Operator configured base order size in whole shares.
    pub base_order_size_shares: u64,
    /// Market minimum allowable order size in whole shares (typically 5).
    pub min_order_size_shares: u64,
    /// Threshold upper bound for Tier 1 Normal (default 0.20).
    pub tier1_max_u: f64,
    /// Threshold upper bound for Tier 2 Moderate Skew (default 0.60).
    pub tier2_max_u: f64,
    /// Threshold upper bound for Tier 3 One-Sided (default 0.90).
    pub tier3_max_u: f64,
    /// Cap Tier-1 reducing-leg size to `|Delta_q|` when `|Delta_q| >= 1` share (default `false`).
    pub cap_tier1_reducing_to_delta: bool,
}

impl Default for RiskConfig {
    fn default() -> Self {
        Self {
            v_max_micro: MicroUsdc(50_000_000), // $50.00
            base_order_size_shares: 20,
            min_order_size_shares: 5,
            tier1_max_u: 0.20,
            tier2_max_u: 0.60,
            tier3_max_u: 0.90,
            cap_tier1_reducing_to_delta: false,
        }
    }
}

impl RiskConfig {
    /// Validates risk configuration invariants.
    pub fn validate(&self) -> Result<(), RiskLifecycleError> {
        if self.v_max_micro.0 == 0 {
            return Err(RiskLifecycleError::InvalidConfig(
                "v_max_micro must be strictly positive".to_string(),
            ));
        }
        if self.base_order_size_shares == 0 {
            return Err(RiskLifecycleError::InvalidConfig(
                "base_order_size_shares must be strictly positive".to_string(),
            ));
        }
        if self.min_order_size_shares == 0 {
            return Err(RiskLifecycleError::InvalidConfig(
                "min_order_size_shares must be strictly positive".to_string(),
            ));
        }
        if !(0.0 < self.tier1_max_u
            && self.tier1_max_u < self.tier2_max_u
            && self.tier2_max_u < self.tier3_max_u)
        {
            return Err(RiskLifecycleError::InvalidConfig(
                "Risk tier thresholds must be strictly increasing: 0 < t1 < t2 < t3".to_string(),
            ));
        }
        Ok(())
    }
}

/// Action emitted when emergency liquidation or flatten is required.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum FlattenAction {
    /// No flattening required.
    None,
    /// Dust remaining below minimum CLOB order size; cannot be liquidated via FAK.
    ResidualDust {
        /// Residual unhedged shares.
        unhedged_shares: SharesMicro,
        /// Minimum order size required by exchange.
        min_order_size: SharesMicro,
    },
    /// Taker FAK rebalance order to be submitted immediately.
    FlattenFak {
        /// Outcome leg to buy or sell.
        outcome: Outcome,
        /// Maximum limit price for FAK order.
        price: MicroUsdc,
        /// Size in shares to rebalance.
        size: SharesMicro,
    },
}

/// Input parameters passed to `DollarNotionalRiskEngine::evaluate`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct RiskEvaluationInput {
    /// Proposed tick-floored UP bid from Station 6.
    pub bid_up: Option<MicroUsdc>,
    /// Proposed tick-floored DOWN bid from Station 6.
    pub bid_dn: Option<MicroUsdc>,
    /// Live orderbook best bid on UP outcome.
    pub best_bid_up: Option<MicroUsdc>,
    /// Live orderbook best bid on DOWN outcome.
    pub best_bid_dn: Option<MicroUsdc>,
    /// Live orderbook best ask on UP outcome.
    pub best_ask_up: Option<MicroUsdc>,
    /// Live orderbook best ask on DOWN outcome.
    pub best_ask_dn: Option<MicroUsdc>,
    /// Whether an emergency flatten order is already in-flight.
    pub flatten_in_flight: bool,
}

/// Output decision of Station 7 dollar-notional risk evaluation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct RiskDecision {
    /// Active risk tier based on cost-basis utilization.
    pub tier: RiskTier,
    /// Cost-basis utilization ratio `u` in [0, inf) scaled to 1e6.
    pub utilization_u_e6: u64,
    /// Heavy outcome leg if net delta or exposure is nonzero.
    pub heavy_leg: Option<Outcome>,
    /// Authorized quote size for UP bid (`None` if suppressed / 0).
    pub size_up: Option<SharesMicro>,
    /// Authorized quote size for DOWN bid (`None` if suppressed / 0).
    pub size_dn: Option<SharesMicro>,
    /// Dollar-cap max capacity `Q_max(p_up)` in shares.
    pub q_max_up: SharesMicro,
    /// Dollar-cap max capacity `Q_max(p_dn)` in shares.
    pub q_max_dn: SharesMicro,
    /// Required liquidation action if in Tier 4.
    pub flatten: FlattenAction,
}

/// Station 7: Absolute Dollar-Notional Risk Engine.
///
/// Computes cost-basis utilization `u`, enforces 4-tier risk policies,
/// prevents mark-price martingale blowups, and clips order sizes to
/// dollar-notional and affordability limits.
#[derive(Debug, Clone)]
pub struct DollarNotionalRiskEngine {
    config: RiskConfig,
}

impl DollarNotionalRiskEngine {
    /// Creates a new risk engine with validated configuration.
    pub fn new(config: RiskConfig) -> Result<Self, RiskLifecycleError> {
        config.validate()?;
        Ok(Self { config })
    }

    /// Returns the active risk configuration.
    #[must_use]
    pub const fn config(&self) -> RiskConfig {
        self.config
    }

    /// Evaluates Station 7 risk limits for proposed UP and DOWN bid prices.
    pub fn evaluate(
        &self,
        snapshot: &OrderRegistrySnapshot,
        input: &RiskEvaluationInput,
    ) -> Result<RiskDecision, RiskLifecycleError> {
        let v_max = self.config.v_max_micro.0;

        // 1. Identify heavy vs reducing leg and unhedged delta.
        let (heavy_leg, unhedged_cost_heavy, open_bid_notional_heavy, net_delta_shares) =
            if snapshot.unhedged_up > snapshot.unhedged_dn {
                let delta = snapshot
                    .unhedged_up
                    .as_u64()
                    .saturating_sub(snapshot.unhedged_dn.as_u64());
                (
                    Some(Outcome::Up),
                    snapshot.unhedged_cost_up_micro.0,
                    snapshot.open_bid_notional_up.0,
                    delta,
                )
            } else if snapshot.unhedged_dn > snapshot.unhedged_up {
                let delta = snapshot
                    .unhedged_dn
                    .as_u64()
                    .saturating_sub(snapshot.unhedged_up.as_u64());
                (
                    Some(Outcome::Down),
                    snapshot.unhedged_cost_dn_micro.0,
                    snapshot.open_bid_notional_dn.0,
                    delta,
                )
            } else {
                // Flat unhedged positions: check open bid notionals.
                if snapshot.open_bid_notional_up.0 >= snapshot.open_bid_notional_dn.0
                    && snapshot.open_bid_notional_up.0 > 0
                {
                    (Some(Outcome::Up), 0, snapshot.open_bid_notional_up.0, 0)
                } else if snapshot.open_bid_notional_dn.0 > 0 {
                    (Some(Outcome::Down), 0, snapshot.open_bid_notional_dn.0, 0)
                } else {
                    (None, 0, 0, 0)
                }
            };

        // 2. Compute cost-basis utilization ratio `u`.
        let total_heavy_committed = unhedged_cost_heavy.saturating_add(open_bid_notional_heavy);
        let u = total_heavy_committed as f64 / v_max as f64;
        let u_e6 = (u * 1_000_000.0).round() as u64;

        // 3. Classify into Risk Tier.
        let tier = if u <= self.config.tier1_max_u {
            RiskTier::Tier1Normal
        } else if u <= self.config.tier2_max_u {
            RiskTier::Tier2ModerateSkew
        } else if u <= self.config.tier3_max_u {
            RiskTier::Tier3OneSided
        } else {
            RiskTier::Tier4HardStop
        };

        let min_order_shares = self.config.min_order_size_shares;
        let min_order_micro =
            SharesMicro::from_raw(min_order_shares.saturating_mul(ONE_SHARE_MICRO));

        // 4. Handle Tier 4 Hard Stop (emergency liquidation).
        if tier == RiskTier::Tier4HardStop {
            let flatten = if input.flatten_in_flight {
                FlattenAction::None
            } else if net_delta_shares < min_order_micro.as_u64() {
                if net_delta_shares > 0 {
                    FlattenAction::ResidualDust {
                        unhedged_shares: SharesMicro::from_raw(net_delta_shares),
                        min_order_size: min_order_micro,
                    }
                } else {
                    FlattenAction::None
                }
            } else {
                // Flatten net_delta_shares via aggressive taker order.
                let delta_shares_micro = SharesMicro::from_raw(net_delta_shares);
                match heavy_leg {
                    Some(Outcome::Up) => {
                        // We are long UP: can sell UP at best_bid_up or buy DOWN at best_ask_dn.
                        // Buy DOWN at ask if available, otherwise sell UP at bid.
                        if let Some(ask_dn) = input.best_ask_dn.filter(|p| p.0 > 0) {
                            FlattenAction::FlattenFak {
                                outcome: Outcome::Down,
                                price: ask_dn,
                                size: delta_shares_micro,
                            }
                        } else if let Some(bid_up) = input.best_bid_up.filter(|p| p.0 > 0) {
                            FlattenAction::FlattenFak {
                                outcome: Outcome::Up,
                                price: bid_up,
                                size: delta_shares_micro,
                            }
                        } else {
                            FlattenAction::None
                        }
                    }
                    Some(Outcome::Down) => {
                        // We are long DOWN: buy UP at best_ask_up or sell DOWN at best_bid_dn.
                        if let Some(ask_up) = input.best_ask_up.filter(|p| p.0 > 0) {
                            FlattenAction::FlattenFak {
                                outcome: Outcome::Up,
                                price: ask_up,
                                size: delta_shares_micro,
                            }
                        } else if let Some(bid_dn) = input.best_bid_dn.filter(|p| p.0 > 0) {
                            FlattenAction::FlattenFak {
                                outcome: Outcome::Down,
                                price: bid_dn,
                                size: delta_shares_micro,
                            }
                        } else {
                            FlattenAction::None
                        }
                    }
                    None => FlattenAction::None,
                }
            };

            return Ok(RiskDecision {
                tier,
                utilization_u_e6: u_e6,
                heavy_leg,
                size_up: None,
                size_dn: None,
                q_max_up: SharesMicro::ZERO,
                q_max_dn: SharesMicro::ZERO,
                flatten,
            });
        }

        // 5. Compute Q_max(p), remaining capacity, and affordable shares for each leg.
        let (q_max_up, size_up) = if let Some(p_up) = input.bid_up {
            if p_up.0 == 0 {
                return Err(RiskLifecycleError::NonPositivePrice(
                    "bid_up is 0".to_string(),
                ));
            }
            let q_max_whole = v_max / p_up.0;
            let q_max = SharesMicro::from_raw(q_max_whole.saturating_mul(ONE_SHARE_MICRO));

            let size = self.compute_leg_size(
                Outcome::Up,
                p_up,
                q_max,
                tier,
                heavy_leg,
                snapshot.unhedged_up,
                snapshot.unhedged_cost_up_micro,
                snapshot.open_bid_notional_up,
                snapshot.open_bid_shares_up,
                snapshot.open_bid_shares_dn,
                net_delta_shares,
                snapshot.available_usdc_micro,
            );
            (q_max, size)
        } else {
            (SharesMicro::ZERO, None)
        };

        let (q_max_dn, size_dn) = if let Some(p_dn) = input.bid_dn {
            if p_dn.0 == 0 {
                return Err(RiskLifecycleError::NonPositivePrice(
                    "bid_dn is 0".to_string(),
                ));
            }
            let q_max_whole = v_max / p_dn.0;
            let q_max = SharesMicro::from_raw(q_max_whole.saturating_mul(ONE_SHARE_MICRO));

            let size = self.compute_leg_size(
                Outcome::Down,
                p_dn,
                q_max,
                tier,
                heavy_leg,
                snapshot.unhedged_dn,
                snapshot.unhedged_cost_dn_micro,
                snapshot.open_bid_notional_dn,
                snapshot.open_bid_shares_dn,
                snapshot.open_bid_shares_up,
                net_delta_shares,
                snapshot.available_usdc_micro,
            );
            (q_max, size)
        } else {
            (SharesMicro::ZERO, None)
        };

        Ok(RiskDecision {
            tier,
            utilization_u_e6: u_e6,
            heavy_leg,
            size_up,
            size_dn,
            q_max_up,
            q_max_dn,
            flatten: FlattenAction::None,
        })
    }

    /// Computes allowable quote size for a single leg according to the 4-tier policy.
    #[allow(clippy::too_many_arguments)]
    fn compute_leg_size(
        &self,
        outcome: Outcome,
        price: MicroUsdc,
        q_max: SharesMicro,
        tier: RiskTier,
        heavy_leg: Option<Outcome>,
        unhedged_leg: SharesMicro,
        unhedged_cost_leg: MicroUsdc,
        open_bid_notional_leg: MicroUsdc,
        open_bid_shares_leg: SharesMicro,
        _open_bid_shares_opposing: SharesMicro,
        net_delta_shares: u64,
        available_usdc: MicroUsdc,
    ) -> Option<SharesMicro> {
        let v_max = self.config.v_max_micro.0;
        let base_shares = self.config.base_order_size_shares;
        let min_order_micro = SharesMicro::from_raw(
            self.config
                .min_order_size_shares
                .saturating_mul(ONE_SHARE_MICRO),
        );

        // Maximum affordable shares given current unallocated cash.
        let affordable_shares_whole = available_usdc.0 / price.0;
        let affordable =
            SharesMicro::from_raw(affordable_shares_whole.saturating_mul(ONE_SHARE_MICRO));

        // Remaining single-leg capacity up to Q_max(p).
        let committed_leg_shares = unhedged_leg
            .as_u64()
            .saturating_add(open_bid_shares_leg.as_u64());
        let remaining_capacity =
            SharesMicro::from_raw(q_max.as_u64().saturating_sub(committed_leg_shares));

        // Baseline Station 7 linear size scaling.
        let committed_dollars = unhedged_cost_leg.0.saturating_add(open_bid_notional_leg.0);
        let linear_shares_whole = if committed_dollars >= v_max {
            0
        } else {
            let frac = (v_max - committed_dollars) as f64 / v_max as f64;
            (base_shares as f64 * frac).floor() as u64
        };
        let linear_size =
            SharesMicro::from_raw(linear_shares_whole.saturating_mul(ONE_SHARE_MICRO));

        let is_accumulating = heavy_leg == Some(outcome);
        let is_reducing = heavy_leg.is_some() && heavy_leg != Some(outcome);

        let target_size = match tier {
            RiskTier::Tier1Normal => {
                // Tier 1: linear Station 7 scaling on both legs, optionally capping reducing-leg overshoot.
                if is_reducing
                    && self.config.cap_tier1_reducing_to_delta
                    && net_delta_shares >= ONE_SHARE_MICRO
                {
                    let desired_reducing = net_delta_shares.max(min_order_micro.as_u64());
                    let max_reducing = SharesMicro::from_raw(
                        desired_reducing.saturating_sub(open_bid_shares_leg.as_u64()),
                    );
                    linear_size
                        .min(max_reducing)
                        .min(remaining_capacity)
                        .min(affordable)
                } else {
                    linear_size.min(remaining_capacity).min(affordable)
                }
            }
            RiskTier::Tier2ModerateSkew => {
                if is_accumulating {
                    // Accumulating leg: min(linear, floor(0.5 * base)).
                    let half_base = SharesMicro::from_raw(
                        ((base_shares as f64 * 0.5).floor() as u64).saturating_mul(ONE_SHARE_MICRO),
                    );
                    linear_size
                        .min(half_base)
                        .min(remaining_capacity)
                        .min(affordable)
                } else if is_reducing {
                    // Reducing leg: min(floor(2.0 * base), |Delta_q| - open_red, affordable).
                    let double_base = SharesMicro::from_raw(
                        base_shares
                            .saturating_mul(2)
                            .saturating_mul(ONE_SHARE_MICRO),
                    );
                    let delta_minus_open =
                        net_delta_shares.saturating_sub(open_bid_shares_leg.as_u64());
                    let max_reducing = SharesMicro::from_raw(delta_minus_open);
                    double_base.min(max_reducing).min(affordable)
                } else {
                    linear_size.min(remaining_capacity).min(affordable)
                }
            }
            RiskTier::Tier3OneSided => {
                if is_accumulating {
                    // Accumulating leg: 0 (immediate cancellation).
                    SharesMicro::ZERO
                } else if is_reducing {
                    // Reducing leg: min(floor(2.5 * base), |Delta_q| - open_red, affordable).
                    let two_point_five_base = SharesMicro::from_raw(
                        ((base_shares as f64 * 2.5).floor() as u64).saturating_mul(ONE_SHARE_MICRO),
                    );
                    let delta_minus_open =
                        net_delta_shares.saturating_sub(open_bid_shares_leg.as_u64());
                    let max_reducing = SharesMicro::from_raw(delta_minus_open);
                    two_point_five_base.min(max_reducing).min(affordable)
                } else {
                    linear_size.min(remaining_capacity).min(affordable)
                }
            }
            RiskTier::Tier4HardStop => SharesMicro::ZERO,
        };

        // Suppress any order size strictly below market min_order_size.
        if target_size < min_order_micro {
            None
        } else {
            Some(target_size)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_vector_1_first_order_cap() {
        // First-Order Cap Vector: V_max=$50, base=100, p=$0.60, unhedged=0
        // -> Q_max=83, output size=83 ($49.80 <= $50.00, NOT 100!).
        let config = RiskConfig {
            v_max_micro: MicroUsdc(50_000_000), // $50.00
            base_order_size_shares: 100,
            min_order_size_shares: 5,
            ..Default::default()
        };
        let engine = DollarNotionalRiskEngine::new(config).unwrap();

        let snapshot = OrderRegistrySnapshot {
            available_usdc_micro: MicroUsdc(100_000_000), // $100.00
            ..Default::default()
        };

        let input = RiskEvaluationInput {
            bid_up: Some(MicroUsdc(600_000)), // $0.60
            ..Default::default()
        };
        let decision = engine.evaluate(&snapshot, &input).unwrap();

        assert_eq!(decision.tier, RiskTier::Tier1Normal);
        assert_eq!(
            decision.q_max_up,
            SharesMicro::from_raw(83 * ONE_SHARE_MICRO)
        );
        assert_eq!(
            decision.size_up,
            Some(SharesMicro::from_raw(83 * ONE_SHARE_MICRO))
        );
        // Cost: 83 * $0.60 = $49.80 <= $50.00.
        let cost = 83 * 600_000;
        assert!(cost <= 50_000_000);
    }

    #[test]
    fn test_vector_2_martingale_crash() {
        // Martingale Crash Vector: Bought 100 UP @ $0.50 (cost=$50), price crashes to $0.10
        // -> u = 50/50 = 1.00 (Tier4HardStop, NOT Tier 1!).
        let config = RiskConfig {
            v_max_micro: MicroUsdc(50_000_000), // $50.00
            base_order_size_shares: 20,
            min_order_size_shares: 5,
            ..Default::default()
        };
        let engine = DollarNotionalRiskEngine::new(config).unwrap();

        let snapshot = OrderRegistrySnapshot {
            unhedged_up: SharesMicro::from_raw(100 * ONE_SHARE_MICRO),
            unhedged_cost_up_micro: MicroUsdc(50_000_000), // $50.00 acquisition cost
            available_usdc_micro: MicroUsdc(100_000_000),
            ..Default::default()
        };

        let input = RiskEvaluationInput {
            bid_up: Some(MicroUsdc(100_000)), // Crashed to $0.10
            best_bid_up: Some(MicroUsdc(90_000)),
            best_bid_dn: Some(MicroUsdc(890_000)),
            best_ask_up: Some(MicroUsdc(110_000)),
            best_ask_dn: Some(MicroUsdc(910_000)),
            ..Default::default()
        };
        let decision = engine.evaluate(&snapshot, &input).unwrap();

        assert_eq!(decision.tier, RiskTier::Tier4HardStop);
        assert_eq!(decision.utilization_u_e6, 1_000_000); // u = 1.00
        assert_eq!(decision.size_up, None);
        assert_eq!(decision.size_dn, None);
        assert!(matches!(decision.flatten, FlattenAction::FlattenFak { .. }));
    }

    #[test]
    fn test_vector_3_reducing_leg_overshoot() {
        // Reducing-Leg Overshoot Vector: Delta_q = +15, Tier 3 (2.5x * 20 = 50),
        // p_dn = $0.10 (Q_max = 500) -> reducing DN bid capped strictly at 15 shares!
        let config = RiskConfig {
            v_max_micro: MicroUsdc(50_000_000), // $50.00
            base_order_size_shares: 20,
            min_order_size_shares: 5,
            tier1_max_u: 0.20,
            tier2_max_u: 0.60,
            tier3_max_u: 0.90,
            ..Default::default()
        };
        let engine = DollarNotionalRiskEngine::new(config).unwrap();

        // Let unhedged_cost be $35 (u = 35/50 = 0.70 => Tier 3 One-Sided).
        let snapshot = OrderRegistrySnapshot {
            unhedged_up: SharesMicro::from_raw(15 * ONE_SHARE_MICRO),
            unhedged_cost_up_micro: MicroUsdc(35_000_000),
            available_usdc_micro: MicroUsdc(100_000_000),
            ..Default::default()
        };

        let input = RiskEvaluationInput {
            bid_up: Some(MicroUsdc(600_000)),
            bid_dn: Some(MicroUsdc(100_000)), // $0.10 => Q_max = 500 shares
            ..Default::default()
        };

        let decision = engine.evaluate(&snapshot, &input).unwrap();

        assert_eq!(decision.tier, RiskTier::Tier3OneSided);
        // Accumulating leg UP is forced to 0 / None.
        assert_eq!(decision.size_up, None);
        // Reducing leg DN is capped strictly at 15 shares (Delta_q), NOT 50 (2.5x base) or 500 (Q_max).
        assert_eq!(
            decision.size_dn,
            Some(SharesMicro::from_raw(15 * ONE_SHARE_MICRO))
        );
    }

    #[test]
    fn cross_tick_override_requires_tier_3_4_or_flatten_phase() {
        assert!(!allows_cross_tick_override(RiskTier::Tier1Normal, None));
        assert!(!allows_cross_tick_override(
            RiskTier::Tier1Normal,
            Some(EpochPhase::ActiveQuoting)
        ));
        assert!(!allows_cross_tick_override(
            RiskTier::Tier2ModerateSkew,
            Some(EpochPhase::ActiveQuoting)
        ));
        assert!(!allows_cross_tick_override(
            RiskTier::Tier1Normal,
            Some(EpochPhase::TerminalLockout)
        ));

        assert!(allows_cross_tick_override(
            RiskTier::Tier3OneSided,
            Some(EpochPhase::ActiveQuoting)
        ));
        assert!(allows_cross_tick_override(
            RiskTier::Tier4HardStop,
            Some(EpochPhase::ActiveQuoting)
        ));
        assert!(allows_cross_tick_override(
            RiskTier::Tier1Normal,
            Some(EpochPhase::PreExpiryLockout)
        ));
        assert!(allows_cross_tick_override(
            RiskTier::Tier2ModerateSkew,
            Some(EpochPhase::MandatoryFlatten)
        ));
    }

    #[test]
    fn tier1_reducing_cap_prevents_flip_overshoot_while_preserving_dust_quoting() {
        let base_cfg = RiskConfig::default();
        let capped_cfg = RiskConfig {
            cap_tier1_reducing_to_delta: true,
            ..Default::default()
        };
        let base_engine = DollarNotionalRiskEngine::new(base_cfg).unwrap();
        let capped_engine = DollarNotionalRiskEngine::new(capped_cfg).unwrap();

        let input = RiskEvaluationInput {
            bid_up: Some(MicroUsdc(500_000)),
            bid_dn: Some(MicroUsdc(480_000)),
            ..Default::default()
        };

        // Case 1: q = +15sh (cost = $7.50, u = 0.15 -> Tier1Normal)
        let snap_15 = OrderRegistrySnapshot {
            unhedged_up: SharesMicro::from_raw(15 * ONE_SHARE_MICRO),
            unhedged_cost_up_micro: MicroUsdc(7_500_000),
            available_usdc_micro: MicroUsdc(100_000_000),
            ..Default::default()
        };
        let d_base = base_engine.evaluate(&snap_15, &input).unwrap();
        assert_eq!(d_base.tier, RiskTier::Tier1Normal);
        assert_eq!(
            d_base.size_dn,
            Some(SharesMicro::from_raw(20 * ONE_SHARE_MICRO))
        );
        let d_cap = capped_engine.evaluate(&snap_15, &input).unwrap();
        assert_eq!(d_cap.tier, RiskTier::Tier1Normal);
        assert_eq!(
            d_cap.size_dn,
            Some(SharesMicro::from_raw(15 * ONE_SHARE_MICRO))
        );

        // Case 2: q = +4.99sh (1 <= |q| < 5) -> caps DN to 5sh (min order size), avoiding 20sh flip
        let snap_4_99 = OrderRegistrySnapshot {
            unhedged_up: SharesMicro::from_raw(4_990_000),
            unhedged_cost_up_micro: MicroUsdc(2_495_000),
            available_usdc_micro: MicroUsdc(100_000_000),
            ..Default::default()
        };
        let d_4_99 = capped_engine.evaluate(&snap_4_99, &input).unwrap();
        assert_eq!(d_4_99.tier, RiskTier::Tier1Normal);
        assert_eq!(
            d_4_99.size_dn,
            Some(SharesMicro::from_raw(5 * ONE_SHARE_MICRO))
        );

        // Case 3: q = +0.01sh (< 1sh dust) -> keeps 20sh so dust never stalls quoting
        let snap_dust = OrderRegistrySnapshot {
            unhedged_up: SharesMicro::from_raw(10_000),
            unhedged_cost_up_micro: MicroUsdc(5_000),
            available_usdc_micro: MicroUsdc(100_000_000),
            ..Default::default()
        };
        let d_dust = capped_engine.evaluate(&snap_dust, &input).unwrap();
        assert_eq!(d_dust.tier, RiskTier::Tier1Normal);
        assert_eq!(
            d_dust.size_dn,
            Some(SharesMicro::from_raw(20 * ONE_SHARE_MICRO))
        );

        // Case 4: Symmetric q = -15sh -> caps UP to 15sh
        let snap_neg_15 = OrderRegistrySnapshot {
            unhedged_dn: SharesMicro::from_raw(15 * ONE_SHARE_MICRO),
            unhedged_cost_dn_micro: MicroUsdc(7_500_000),
            available_usdc_micro: MicroUsdc(100_000_000),
            ..Default::default()
        };
        let d_neg = capped_engine.evaluate(&snap_neg_15, &input).unwrap();
        assert_eq!(d_neg.tier, RiskTier::Tier1Normal);
        assert_eq!(
            d_neg.size_up,
            Some(SharesMicro::from_raw(15 * ONE_SHARE_MICRO))
        );
    }

    #[test]
    fn deficit_hedge_override_latch_respects_thresholds_tiers_and_resets() {
        let min_sh = SharesMicro::from_raw(5 * ONE_SHARE_MICRO);
        let trig_65 = Some(MicroUsdc(650_000));
        let mut latch = DeficitHedgeOverrideLatch::new();

        // 1. Disabled when trigger is None or cap_tier1_reducing_to_delta is false.
        assert!(!latch.evaluate(
            RiskTier::Tier1Normal,
            SignedSharesMicro(20 * ONE_SHARE_MICRO as i64),
            min_sh,
            Some(MicroUsdc(300_000)),
            Some(MicroUsdc(700_000)),
            None,
            true,
        ));
        assert!(!latch.evaluate(
            RiskTier::Tier1Normal,
            SignedSharesMicro(20 * ONE_SHARE_MICRO as i64),
            min_sh,
            Some(MicroUsdc(300_000)),
            Some(MicroUsdc(700_000)),
            trig_65,
            false,
        ));
        assert!(!latch.is_latched());

        // 2. Inventory threshold around abs(q) = 5 sh: 4.99 sh does not qualify, 5.00 sh qualifies.
        assert!(!latch.evaluate(
            RiskTier::Tier1Normal,
            SignedSharesMicro(4_990_000),
            min_sh,
            Some(MicroUsdc(300_000)),
            Some(MicroUsdc(700_000)),
            trig_65,
            true,
        ));
        assert!(!latch.is_latched());

        // 3. Opposing ask strictly below trigger ($0.64 < $0.65) does not qualify.
        assert!(!latch.evaluate(
            RiskTier::Tier1Normal,
            SignedSharesMicro(5 * ONE_SHARE_MICRO as i64),
            min_sh,
            Some(MicroUsdc(360_000)),
            Some(MicroUsdc(640_000)),
            trig_65,
            true,
        ));
        assert!(!latch.is_latched());

        // 4. Opposing ask at trigger ($0.65 >= $0.65) latches for Tier 1 long UP.
        assert!(latch.evaluate(
            RiskTier::Tier1Normal,
            SignedSharesMicro(5 * ONE_SHARE_MICRO as i64),
            min_sh,
            Some(MicroUsdc(350_000)),
            Some(MicroUsdc(650_000)),
            trig_65,
            true,
        ));
        assert_eq!(latch.latched_heavy_leg(), Some(Outcome::Up));

        // 5. Latch persists when opposing ask dips back below trigger ($0.58) while abs(q) >= 5 sh.
        assert!(latch.evaluate(
            RiskTier::Tier1Normal,
            SignedSharesMicro(5 * ONE_SHARE_MICRO as i64),
            min_sh,
            Some(MicroUsdc(420_000)),
            Some(MicroUsdc(580_000)),
            trig_65,
            true,
        ));
        assert!(latch.is_latched());

        // 6. Also works across Tier1 -> Tier2 without losing latch.
        assert!(latch.evaluate(
            RiskTier::Tier2ModerateSkew,
            SignedSharesMicro(15 * ONE_SHARE_MICRO as i64),
            min_sh,
            Some(MicroUsdc(450_000)),
            Some(MicroUsdc(550_000)),
            trig_65,
            true,
        ));

        // 7. Sign flip (long UP -> long DOWN) clears the old latch and re-checks UP ask.
        assert!(!latch.evaluate(
            RiskTier::Tier1Normal,
            SignedSharesMicro(-(10 * ONE_SHARE_MICRO as i64)),
            min_sh,
            Some(MicroUsdc(640_000)),
            Some(MicroUsdc(360_000)),
            trig_65,
            true,
        ));
        assert!(!latch.is_latched());

        // 8. Long DOWN checks best_ask_up against trigger ($0.70 >= $0.65).
        assert!(latch.evaluate(
            RiskTier::Tier2ModerateSkew,
            SignedSharesMicro(-(10 * ONE_SHARE_MICRO as i64)),
            min_sh,
            Some(MicroUsdc(700_000)),
            Some(MicroUsdc(300_000)),
            trig_65,
            true,
        ));
        assert_eq!(latch.latched_heavy_leg(), Some(Outcome::Down));

        // 9. Clears when inventory drops below 5 sh, and stays clear on rebound until re-triggered.
        assert!(!latch.evaluate(
            RiskTier::Tier1Normal,
            SignedSharesMicro(-(4_990_000)),
            min_sh,
            Some(MicroUsdc(500_000)),
            Some(MicroUsdc(500_000)),
            trig_65,
            true,
        ));
        assert!(!latch.is_latched());

        // 10. Explicit reset clears latch (epoch rollover).
        assert!(latch.evaluate(
            RiskTier::Tier1Normal,
            SignedSharesMicro(10 * ONE_SHARE_MICRO as i64),
            min_sh,
            Some(MicroUsdc(300_000)),
            Some(MicroUsdc(700_000)),
            trig_65,
            true,
        ));
        latch.reset();
        assert!(!latch.is_latched());
    }

    #[test]
    fn tier4_ignores_zero_top_of_book_prices() {
        let engine = DollarNotionalRiskEngine::new(RiskConfig::default()).unwrap();
        let snapshot_up = OrderRegistrySnapshot {
            unhedged_up: SharesMicro::from_raw(10 * ONE_SHARE_MICRO),
            unhedged_cost_up_micro: MicroUsdc(48_000_000),
            available_usdc_micro: MicroUsdc(100_000_000),
            ..Default::default()
        };
        let input_zero = RiskEvaluationInput {
            best_ask_dn: Some(MicroUsdc::ZERO),
            best_bid_up: Some(MicroUsdc::ZERO),
            best_ask_up: Some(MicroUsdc::ZERO),
            best_bid_dn: Some(MicroUsdc::ZERO),
            ..Default::default()
        };
        let dec_up = engine.evaluate(&snapshot_up, &input_zero).unwrap();
        assert_eq!(dec_up.tier, RiskTier::Tier4HardStop);
        assert_eq!(dec_up.flatten, FlattenAction::None);

        let snapshot_dn = OrderRegistrySnapshot {
            unhedged_dn: SharesMicro::from_raw(10 * ONE_SHARE_MICRO),
            unhedged_cost_dn_micro: MicroUsdc(48_000_000),
            available_usdc_micro: MicroUsdc(100_000_000),
            ..Default::default()
        };
        let dec_dn = engine.evaluate(&snapshot_dn, &input_zero).unwrap();
        assert_eq!(dec_dn.tier, RiskTier::Tier4HardStop);
        assert_eq!(dec_dn.flatten, FlattenAction::None);
    }
}
