use core_types::{MicroUsdc, ONE_SHARE_MICRO, Outcome, SharesMicro};
use serde::{Deserialize, Serialize};

use crate::error::RiskLifecycleError;
use crate::station7_risk::OrderRegistrySnapshot;

/// Deterministic 4-phase 300-second prediction market epoch lifecycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum EpochPhase {
    /// `tau > 60s`: Standard two-sided dual-bid quoting.
    ActiveQuoting,
    /// `30s < tau <= 60s`: Freeze new accumulation; offload inventory passively.
    PreExpiryLockout,
    /// `15s < tau <= 30s`: Cancel all passive orders; fire aggressive FAK flatten.
    MandatoryFlatten,
    /// `tau <= 15s`: Absolute lockout; zero orders permitted; settle paired collateral.
    TerminalLockout,
}

/// Action commanded by the lifecycle controller on each evaluation tick.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum LifecycleAction {
    /// Regular market-making quoting authorized.
    ContinueQuoting {
        /// Active phase.
        phase: EpochPhase,
        /// Remaining time to expiry in seconds.
        tau_secs: u64,
        /// Whether accumulation is prohibited in this phase.
        prohibit_accumulation: bool,
    },
    /// Cancel all standing maker quotes across both orderbooks.
    CancelAllResting {
        /// Active phase.
        phase: EpochPhase,
        /// Remaining time to expiry in seconds.
        tau_secs: u64,
        /// Reason for cancellation.
        reason: String,
    },
    /// Execute aggressive taker FAK order to close net unhedged delta.
    ExecuteFlattenFak {
        /// Outcome leg to buy or sell.
        outcome: Outcome,
        /// Limit price for FAK order.
        price: MicroUsdc,
        /// Quantity to flatten in micro-shares.
        size: SharesMicro,
        /// Reason for flattening.
        reason: String,
    },
    /// Net unhedged delta is below exchange minimum order size; cannot be flattened.
    ResidualDustUnflattenable {
        /// Active phase.
        phase: EpochPhase,
        /// Residual unhedged shares.
        unhedged_shares: SharesMicro,
        /// Minimum order size required by exchange.
        min_order_size: SharesMicro,
    },
    /// Market has reached resolution time; prepare rollover to next 5m market.
    EpochRollover {
        /// Timestamp of expired epoch in ms.
        expired_expiry_ms: u64,
        /// Paired outcome shares available for merge/redemption.
        paired_shares_to_settle: SharesMicro,
    },
}

/// Configuration thresholds for the 300s epoch lifecycle controller.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LifecycleConfig {
    /// Horizon duration in seconds for PreExpiryLockout (default 60s).
    pub pre_expiry_lockout_secs: u64,
    /// Horizon duration in seconds for MandatoryFlatten (default 30s).
    pub mandatory_flatten_secs: u64,
    /// Horizon duration in seconds for TerminalLockout (default 15s).
    pub terminal_lockout_secs: u64,
    /// Minimum order size in whole shares.
    pub min_order_size_shares: u64,
}

impl Default for LifecycleConfig {
    fn default() -> Self {
        Self {
            pre_expiry_lockout_secs: 60,
            mandatory_flatten_secs: 30,
            terminal_lockout_secs: 15,
            min_order_size_shares: 5,
        }
    }
}

impl LifecycleConfig {
    /// Validates lifecycle horizon thresholds.
    pub fn validate(&self) -> Result<(), RiskLifecycleError> {
        if !(self.pre_expiry_lockout_secs > self.mandatory_flatten_secs
            && self.mandatory_flatten_secs > self.terminal_lockout_secs
            && self.terminal_lockout_secs > 0)
        {
            return Err(RiskLifecycleError::InvalidConfig(
                "Lifecycle horizons must satisfy: lockout (60s) > flatten (30s) > terminal (15s) > 0".to_string(),
            ));
        }
        if self.min_order_size_shares == 0 {
            return Err(RiskLifecycleError::InvalidConfig(
                "min_order_size_shares must be strictly positive".to_string(),
            ));
        }
        Ok(())
    }
}

/// Inputs passed to `EpochLifecycleController::evaluate`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LifecycleInput {
    /// Current adjusted Unix timestamp in ms.
    pub now_ms: u64,
    /// Market expiry timestamp in Unix ms.
    pub expiry_ms: u64,
    /// Best ask on UP book if present.
    pub best_ask_up: Option<MicroUsdc>,
    /// Best ask on DOWN book if present.
    pub best_ask_dn: Option<MicroUsdc>,
    /// Best bid on UP book if present.
    pub best_bid_up: Option<MicroUsdc>,
    /// Best bid on DOWN book if present.
    pub best_bid_dn: Option<MicroUsdc>,
    /// Whether an emergency flatten FAK order is currently in-flight.
    pub flatten_in_flight: bool,
}

/// Controller governing the 4-phase 300s prediction market epoch lifecycle.
#[derive(Debug, Clone)]
pub struct EpochLifecycleController {
    config: LifecycleConfig,
    last_phase: Option<EpochPhase>,
    rollover_emitted: bool,
    last_expiry_ms: Option<u64>,
}

impl EpochLifecycleController {
    /// Creates a new lifecycle controller with validated configuration.
    pub fn new(config: LifecycleConfig) -> Result<Self, RiskLifecycleError> {
        config.validate()?;
        Ok(Self {
            config,
            last_phase: None,
            rollover_emitted: false,
            last_expiry_ms: None,
        })
    }

    /// Evaluates active lifecycle state and returns commanded action.
    pub fn evaluate(
        &mut self,
        input: &LifecycleInput,
        snapshot: &OrderRegistrySnapshot,
    ) -> LifecycleAction {
        if self
            .last_expiry_ms
            .is_some_and(|prev| prev != input.expiry_ms)
        {
            self.reset_for_new_epoch();
        }
        self.last_expiry_ms = Some(input.expiry_ms);

        let tau_ms = input.expiry_ms.saturating_sub(input.now_ms);
        let tau_secs = tau_ms / 1000;

        // Check if market has fully expired:
        if input.now_ms >= input.expiry_ms {
            if !self.rollover_emitted {
                self.rollover_emitted = true;
                let paired = snapshot.unhedged_up.min(snapshot.unhedged_dn);
                return LifecycleAction::EpochRollover {
                    expired_expiry_ms: input.expiry_ms,
                    paired_shares_to_settle: paired,
                };
            }
            return LifecycleAction::CancelAllResting {
                phase: EpochPhase::TerminalLockout,
                tau_secs: 0,
                reason: "Market expired (tau = 0s)".to_string(),
            };
        }

        let phase = if tau_secs > self.config.pre_expiry_lockout_secs {
            EpochPhase::ActiveQuoting
        } else if tau_secs > self.config.mandatory_flatten_secs {
            EpochPhase::PreExpiryLockout
        } else if tau_secs > self.config.terminal_lockout_secs {
            EpochPhase::MandatoryFlatten
        } else {
            EpochPhase::TerminalLockout
        };

        self.last_phase = Some(phase);

        let min_shares_micro = SharesMicro::from_raw(
            self.config
                .min_order_size_shares
                .saturating_mul(ONE_SHARE_MICRO),
        );

        let (heavy_leg, net_delta_shares) = if snapshot.unhedged_up > snapshot.unhedged_dn {
            (
                Some(Outcome::Up),
                snapshot
                    .unhedged_up
                    .as_u64()
                    .saturating_sub(snapshot.unhedged_dn.as_u64()),
            )
        } else if snapshot.unhedged_dn > snapshot.unhedged_up {
            (
                Some(Outcome::Down),
                snapshot
                    .unhedged_dn
                    .as_u64()
                    .saturating_sub(snapshot.unhedged_up.as_u64()),
            )
        } else {
            (None, 0)
        };

        match phase {
            EpochPhase::ActiveQuoting => LifecycleAction::ContinueQuoting {
                phase,
                tau_secs,
                prohibit_accumulation: false,
            },

            EpochPhase::PreExpiryLockout => {
                // If completely flat, cancel both sides and stay flat!
                if net_delta_shares == 0 {
                    LifecycleAction::CancelAllResting {
                        phase,
                        tau_secs,
                        reason: "Flat position in PreExpiryLockout (tau <= 60s)".to_string(),
                    }
                } else {
                    // Accumulation is prohibited; only inventory reduction allowed.
                    LifecycleAction::ContinueQuoting {
                        phase,
                        tau_secs,
                        prohibit_accumulation: true,
                    }
                }
            }

            EpochPhase::MandatoryFlatten => {
                // Mandatory flatten window (15s < tau <= 30s):
                // Cancel all passive quotes and rebalance net delta via FAK.
                if net_delta_shares == 0 {
                    LifecycleAction::CancelAllResting {
                        phase,
                        tau_secs,
                        reason: "Flat in MandatoryFlatten; quotes canceled".to_string(),
                    }
                } else if net_delta_shares < min_shares_micro.as_u64() {
                    // Dust Trap: cannot flatten via CLOB because delta < min_order_size.
                    LifecycleAction::ResidualDustUnflattenable {
                        phase,
                        unhedged_shares: SharesMicro::from_raw(net_delta_shares),
                        min_order_size: min_shares_micro,
                    }
                } else if input.flatten_in_flight {
                    LifecycleAction::CancelAllResting {
                        phase,
                        tau_secs,
                        reason: format!(
                            "Flatten order in flight; resting quotes canceled (heavy={heavy_leg:?}, ask_up={:?}, bid_up={:?}, ask_dn={:?}, bid_dn={:?})",
                            input.best_ask_up,
                            input.best_bid_up,
                            input.best_ask_dn,
                            input.best_bid_dn
                        ),
                    }
                } else {
                    let delta_micro = SharesMicro::from_raw(net_delta_shares);
                    match heavy_leg {
                        Some(Outcome::Up) => {
                            // Long UP: buy DOWN at ask or sell UP at bid.
                            if let Some(ask_dn) = input.best_ask_dn.filter(|p| p.0 > 0) {
                                LifecycleAction::ExecuteFlattenFak {
                                    outcome: Outcome::Down,
                                    price: ask_dn,
                                    size: delta_micro,
                                    reason: "Mandatory flatten BUY DOWN at ask".to_string(),
                                }
                            } else if let Some(bid_up) = input.best_bid_up.filter(|p| p.0 > 0) {
                                LifecycleAction::ExecuteFlattenFak {
                                    outcome: Outcome::Up,
                                    price: bid_up,
                                    size: delta_micro,
                                    reason: "Mandatory flatten SELL UP at bid".to_string(),
                                }
                            } else {
                                LifecycleAction::CancelAllResting {
                                    phase,
                                    tau_secs,
                                    reason: format!(
                                        "Mandatory flatten liquidity unavailable (heavy=Up, ask_dn={:?}, bid_up={:?})",
                                        input.best_ask_dn, input.best_bid_up
                                    ),
                                }
                            }
                        }
                        Some(Outcome::Down) => {
                            // Long DOWN: buy UP at ask or sell DOWN at bid.
                            if let Some(ask_up) = input.best_ask_up.filter(|p| p.0 > 0) {
                                LifecycleAction::ExecuteFlattenFak {
                                    outcome: Outcome::Up,
                                    price: ask_up,
                                    size: delta_micro,
                                    reason: "Mandatory flatten BUY UP at ask".to_string(),
                                }
                            } else if let Some(bid_dn) = input.best_bid_dn.filter(|p| p.0 > 0) {
                                LifecycleAction::ExecuteFlattenFak {
                                    outcome: Outcome::Down,
                                    price: bid_dn,
                                    size: delta_micro,
                                    reason: "Mandatory flatten SELL DOWN at bid".to_string(),
                                }
                            } else {
                                LifecycleAction::CancelAllResting {
                                    phase,
                                    tau_secs,
                                    reason: format!(
                                        "Mandatory flatten liquidity unavailable (heavy=Down, ask_up={:?}, bid_dn={:?})",
                                        input.best_ask_up, input.best_bid_dn
                                    ),
                                }
                            }
                        }
                        None => LifecycleAction::CancelAllResting {
                            phase,
                            tau_secs,
                            reason: "Flat delta in MandatoryFlatten".to_string(),
                        },
                    }
                }
            }

            EpochPhase::TerminalLockout => LifecycleAction::CancelAllResting {
                phase,
                tau_secs,
                reason: "TerminalLockout (tau <= 15s): all quoting ceased".to_string(),
            },
        }
    }

    /// Resets the controller for a newly discovered epoch.
    pub fn reset_for_new_epoch(&mut self) {
        self.last_phase = None;
        self.rollover_emitted = false;
        self.last_expiry_ms = None;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_vector_5_dust_trap() {
        // Dust Trap Vector:
        // Delta_q = +3 < min_order_size (5) at tau = 25s
        // -> emits ResidualDustUnflattenable, fires 0 FAK orders.
        let config = LifecycleConfig {
            pre_expiry_lockout_secs: 60,
            mandatory_flatten_secs: 30,
            terminal_lockout_secs: 15,
            min_order_size_shares: 5,
        };
        let mut controller = EpochLifecycleController::new(config).unwrap();

        let expiry_ms = 300_000;
        let now_ms = 275_000; // tau = 25s (in MandatoryFlatten: 15 < tau <= 30)

        let snapshot = OrderRegistrySnapshot {
            unhedged_up: SharesMicro::from_raw(3 * ONE_SHARE_MICRO), // Delta_q = +3
            unhedged_dn: SharesMicro::ZERO,
            ..Default::default()
        };

        let input = LifecycleInput {
            now_ms,
            expiry_ms,
            best_ask_up: Some(MicroUsdc(600_000)),
            best_ask_dn: Some(MicroUsdc(420_000)),
            best_bid_up: Some(MicroUsdc(580_000)),
            best_bid_dn: Some(MicroUsdc(400_000)),
            flatten_in_flight: false,
        };

        let action = controller.evaluate(&input, &snapshot);
        assert_eq!(
            action,
            LifecycleAction::ResidualDustUnflattenable {
                phase: EpochPhase::MandatoryFlatten,
                unhedged_shares: SharesMicro::from_raw(3 * ONE_SHARE_MICRO),
                min_order_size: SharesMicro::from_raw(5 * ONE_SHARE_MICRO),
            }
        );
    }

    #[test]
    fn test_lifecycle_transitions() {
        let config = LifecycleConfig::default();
        let mut controller = EpochLifecycleController::new(config).unwrap();
        let snapshot = OrderRegistrySnapshot::default();

        let expiry_ms = 300_000;

        // tau = 100s -> ActiveQuoting
        let input_active = LifecycleInput {
            now_ms: 200_000,
            expiry_ms,
            best_ask_up: None,
            best_ask_dn: None,
            best_bid_up: None,
            best_bid_dn: None,
            flatten_in_flight: false,
        };
        assert!(matches!(
            controller.evaluate(&input_active, &snapshot),
            LifecycleAction::ContinueQuoting {
                phase: EpochPhase::ActiveQuoting,
                prohibit_accumulation: false,
                ..
            }
        ));

        // tau = 45s, flat -> CancelAllResting in PreExpiryLockout
        let input_pre_expiry = LifecycleInput {
            now_ms: 255_000,
            expiry_ms,
            ..input_active
        };
        assert!(matches!(
            controller.evaluate(&input_pre_expiry, &snapshot),
            LifecycleAction::CancelAllResting {
                phase: EpochPhase::PreExpiryLockout,
                ..
            }
        ));

        // tau = 10s -> TerminalLockout
        let input_terminal = LifecycleInput {
            now_ms: 290_000,
            expiry_ms,
            ..input_active
        };
        assert!(matches!(
            controller.evaluate(&input_terminal, &snapshot),
            LifecycleAction::CancelAllResting {
                phase: EpochPhase::TerminalLockout,
                ..
            }
        ));

        // tau = 0s -> EpochRollover
        let input_expired = LifecycleInput {
            now_ms: 300_000,
            expiry_ms,
            ..input_active
        };
        assert!(matches!(
            controller.evaluate(&input_expired, &snapshot),
            LifecycleAction::EpochRollover { .. }
        ));
    }

    #[test]
    fn test_mandatory_flatten_net_19_emits_execute_flatten_fak() {
        let mut controller = EpochLifecycleController::new(LifecycleConfig::default()).unwrap();
        let expiry_ms = 300_000;
        let now_ms = 275_000; // tau = 25s (MandatoryFlatten: 15 < tau <= 30)

        // Q = (0, 19) -> net_delta = -19 -> buy 19 UP at best_ask_up.
        let snapshot_short_up = OrderRegistrySnapshot {
            unhedged_up: SharesMicro::ZERO,
            unhedged_dn: SharesMicro::from_raw(19 * ONE_SHARE_MICRO),
            ..Default::default()
        };
        let input_with_ask_up = LifecycleInput {
            now_ms,
            expiry_ms,
            best_ask_up: Some(MicroUsdc(520_000)),
            best_ask_dn: Some(MicroUsdc(490_000)),
            best_bid_up: Some(MicroUsdc(500_000)),
            best_bid_dn: Some(MicroUsdc(480_000)),
            flatten_in_flight: false,
        };
        assert_eq!(
            controller.evaluate(&input_with_ask_up, &snapshot_short_up),
            LifecycleAction::ExecuteFlattenFak {
                outcome: Outcome::Up,
                price: MicroUsdc(520_000),
                size: SharesMicro::from_raw(19 * ONE_SHARE_MICRO),
                reason: "Mandatory flatten BUY UP at ask".to_string(),
            }
        );

        // When best_ask_up is unavailable, fallback sells 19 DN at best_bid_dn.
        let input_bid_dn_only = LifecycleInput {
            best_ask_up: None,
            ..input_with_ask_up
        };
        assert_eq!(
            controller.evaluate(&input_bid_dn_only, &snapshot_short_up),
            LifecycleAction::ExecuteFlattenFak {
                outcome: Outcome::Down,
                price: MicroUsdc(480_000),
                size: SharesMicro::from_raw(19 * ONE_SHARE_MICRO),
                reason: "Mandatory flatten SELL DOWN at bid".to_string(),
            }
        );

        // Q = (19, 0) -> net_delta = +19 -> buy 19 DN at best_ask_dn.
        let snapshot_long_up = OrderRegistrySnapshot {
            unhedged_up: SharesMicro::from_raw(19 * ONE_SHARE_MICRO),
            unhedged_dn: SharesMicro::ZERO,
            ..Default::default()
        };
        assert_eq!(
            controller.evaluate(&input_with_ask_up, &snapshot_long_up),
            LifecycleAction::ExecuteFlattenFak {
                outcome: Outcome::Down,
                price: MicroUsdc(490_000),
                size: SharesMicro::from_raw(19 * ONE_SHARE_MICRO),
                reason: "Mandatory flatten BUY DOWN at ask".to_string(),
            }
        );
    }
}
