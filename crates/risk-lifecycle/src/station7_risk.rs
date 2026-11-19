use core_types::{MicroUsdc, ONE_SHARE_MICRO, Outcome, SharesMicro};
use serde::{Deserialize, Serialize};

use crate::error::RiskLifecycleError;

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
                        if let Some(ask_dn) = input.best_ask_dn {
                            FlattenAction::FlattenFak {
                                outcome: Outcome::Down,
                                price: ask_dn,
                                size: delta_shares_micro,
                            }
                        } else if let Some(bid_up) = input.best_bid_up {
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
                        if let Some(ask_up) = input.best_ask_up {
                            FlattenAction::FlattenFak {
                                outcome: Outcome::Up,
                                price: ask_up,
                                size: delta_shares_micro,
                            }
                        } else if let Some(bid_dn) = input.best_bid_dn {
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
                // Tier 1: linear Station 7 scaling on both legs, capped by remaining capacity & cash.
                linear_size.min(remaining_capacity).min(affordable)
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
}
