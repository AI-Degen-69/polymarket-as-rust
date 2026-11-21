use core_types::{MicroUsdc, ONE_SHARE_MICRO, Outcome, SharesMicro};
use serde::{Deserialize, Serialize};

use crate::error::RiskLifecycleError;

/// Explicit reason for canceling an active or resting order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CancelReason {
    /// Binance spot jump triggered fast-path defensive cancel (< 15ms).
    DefensiveCancel,
    /// Active bid price crosses or touches best ask on the book.
    CrossesBook,
    /// Station 7 risk tier suppressed this leg to 0.
    RiskSuppressed,
    /// Active remaining size strictly exceeds new authorized risk size.
    ExcessiveSize,
    /// 300s lifecycle epoch transitioned to lockout or mandatory flatten.
    LifecycleLockout,
    /// Circuit breaker tripped (feed stale, clock drift, API errors).
    CircuitBreaker,
    /// Remaining size after partial fill is a stub below minimum order size.
    StubRefresh,
    /// Continuous deadband drift exceeded threshold (`CancelAndReplace`).
    DeadbandDrift,
}

/// A fully sized and priced quote proposal ready for discrete gate evaluation.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct SizedQuote {
    /// Final tick-floored limit price.
    pub price: MicroUsdc,
    /// Authorized order size in micro-shares.
    pub size: SharesMicro,
    /// Unrounded target price in USD from Station 6 for continuous deadband check.
    pub price_raw_usd: f64,
}

/// Discrete order action emitted for an individual outcome leg.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum OrderAction {
    /// Resting order is within deadband and valid; preserve FIFO queue seniority.
    Hold,
    /// Submit a new passive maker limit order.
    SubmitNew {
        /// Target outcome leg.
        outcome: Outcome,
        /// Limit price in micro-USDC.
        price: MicroUsdc,
        /// Order quantity in micro-shares.
        size: SharesMicro,
    },
    /// Cancel existing resting order.
    Cancel {
        /// Target outcome leg.
        outcome: Outcome,
        /// Exchange or simulator order ID.
        order_id: String,
        /// Explicit reason for cancellation.
        reason: CancelReason,
    },
    /// Atomically queue replacement behind cancel.
    CancelAndReplace {
        /// Target outcome leg.
        outcome: Outcome,
        /// Existing order ID to cancel.
        old_order_id: String,
        /// New limit price.
        new_price: MicroUsdc,
        /// New limit size.
        new_size: SharesMicro,
        /// Reason for update.
        reason: CancelReason,
    },
}

/// Strict per-leg order state machine to prevent race conditions and double-fills.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub enum OrderSlotState {
    /// No resting order and no in-flight requests.
    #[default]
    Idle,
    /// New order placement request submitted; awaiting exchange ACK.
    PendingPlace {
        /// Monotonic client sequence number.
        seq: u64,
        /// High-res timestamp when placement was sent (`ns`).
        sent_ns: u64,
        /// Price of order in flight.
        price: MicroUsdc,
        /// Quantity of order in flight.
        size: SharesMicro,
    },
    /// Order is confirmed live and resting on the exchange orderbook.
    Live {
        /// Exchange assigned order identifier.
        order_id: String,
        /// Active limit price.
        price: MicroUsdc,
        /// Original placed quantity.
        original_size: SharesMicro,
        /// Remaining unfilled quantity.
        remaining_size: SharesMicro,
    },
    /// Order cancellation request in flight; replacement optionally queued behind ACK.
    PendingCancel {
        /// Order identifier being canceled.
        order_id: String,
        /// Next quote to place once cancel confirms.
        next_quote: Option<SizedQuote>,
        /// High-res timestamp when cancel was sent (`ns`).
        sent_ns: u64,
        /// Cancellation reason.
        reason: CancelReason,
    },
    /// Order status uncertain (e.g. `order_not_found` during cancel); quoting frozen.
    PendingReconcile {
        /// Order identifier requiring verification.
        order_id: String,
    },
}

/// Configuration for Station 8 Continuous Hysteresis Deadband.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct HysteresisConfig {
    /// Buffer added to 1.0 tick before triggering cancel-replace (default 0.25).
    pub hysteresis_buf: f64,
    /// Minimum order size in whole shares.
    pub min_order_size_shares: u64,
}

impl Default for HysteresisConfig {
    fn default() -> Self {
        Self {
            hysteresis_buf: 0.25,
            min_order_size_shares: 5,
        }
    }
}

impl HysteresisConfig {
    /// Validates hysteresis parameters.
    pub fn validate(&self) -> Result<(), RiskLifecycleError> {
        if self.hysteresis_buf < 0.0 || !self.hysteresis_buf.is_finite() {
            return Err(RiskLifecycleError::InvalidConfig(
                "hysteresis_buf must be finite and non-negative".to_string(),
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

/// Inputs provided to Station 8 Hysteresis Gate evaluation for one leg.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct HysteresisInput {
    /// Target outcome.
    pub outcome: Outcome,
    /// Proposed sized quote from Stations 6 & 7 (`None` if suppressed).
    pub proposed_quote: Option<SizedQuote>,
    /// Active market tick increment in micro-USDC.
    pub tick_size_micro: MicroUsdc,
    /// Current best ask on this book leg (`None` if book is empty).
    pub best_ask: Option<MicroUsdc>,
    /// Whether fast-path lead-lag defensive cancel was tripped.
    pub defensive_cancel: bool,
    /// High-res current timestamp in nanoseconds.
    pub now_ns: u64,
    /// Client sequence counter generator.
    pub next_seq: u64,
}

/// Station 8: Queue Preservation & Continuous Hysteresis Gate.
#[derive(Debug, Clone)]
pub struct HysteresisGate {
    config: HysteresisConfig,
    state_up: OrderSlotState,
    state_dn: OrderSlotState,
}

impl HysteresisGate {
    /// Creates a new hysteresis gate with validated configuration.
    pub fn new(config: HysteresisConfig) -> Result<Self, RiskLifecycleError> {
        config.validate()?;
        Ok(Self {
            config,
            state_up: OrderSlotState::Idle,
            state_dn: OrderSlotState::Idle,
        })
    }

    /// Returns current state for an outcome leg.
    #[must_use]
    pub fn state(&self, outcome: Outcome) -> &OrderSlotState {
        match outcome {
            Outcome::Up => &self.state_up,
            Outcome::Down => &self.state_dn,
        }
    }

    /// Sets state directly (used during actor reconciliation).
    pub fn set_state(&mut self, outcome: Outcome, state: OrderSlotState) {
        match outcome {
            Outcome::Up => self.state_up = state,
            Outcome::Down => self.state_dn = state,
        }
    }

    /// Evaluates discrete action for an outcome leg and updates FSM accordingly.
    pub fn evaluate_leg(&mut self, input: &HysteresisInput) -> OrderAction {
        let outcome = input.outcome;
        let min_shares_micro = SharesMicro::from_raw(
            self.config
                .min_order_size_shares
                .saturating_mul(ONE_SHARE_MICRO),
        );
        let tick_usd = input.tick_size_micro.0 as f64 / 1_000_000.0;
        let deadband_usd = (1.0 + self.config.hysteresis_buf) * tick_usd;

        let current_state = match outcome {
            Outcome::Up => &self.state_up,
            Outcome::Down => &self.state_dn,
        };

        match current_state {
            OrderSlotState::Idle => {
                // If Idle and we have an authorized quote >= min_order_size, submit new.
                if let Some(quote) = input.proposed_quote
                    && !input.defensive_cancel
                    && quote.size >= min_shares_micro
                    && quote.price.0 > 0
                    && input.best_ask.is_none_or(|ask| quote.price < ask)
                {
                    let action = OrderAction::SubmitNew {
                        outcome,
                        price: quote.price,
                        size: quote.size,
                    };
                    let next_state = OrderSlotState::PendingPlace {
                        seq: input.next_seq,
                        sent_ns: input.now_ns,
                        price: quote.price,
                        size: quote.size,
                    };
                    self.set_state(outcome, next_state);
                    return action;
                }
                OrderAction::Hold
            }

            OrderSlotState::PendingPlace { .. }
            | OrderSlotState::PendingCancel { .. }
            | OrderSlotState::PendingReconcile { .. } => {
                // Never emit new placement while in-flight or reconciling!
                OrderAction::Hold
            }

            OrderSlotState::Live {
                order_id,
                price: active_price,
                remaining_size: active_remaining,
                ..
            } => {
                let order_id = order_id.clone();
                let active_price = *active_price;
                let active_remaining = *active_remaining;

                // 1. Fast-path checks for immediate cancellation:
                // (a) Binance jump defensive cancel
                if input.defensive_cancel {
                    let next_state = OrderSlotState::PendingCancel {
                        order_id: order_id.clone(),
                        next_quote: None,
                        sent_ns: input.now_ns,
                        reason: CancelReason::DefensiveCancel,
                    };
                    self.set_state(outcome, next_state);
                    return OrderAction::Cancel {
                        outcome,
                        order_id,
                        reason: CancelReason::DefensiveCancel,
                    };
                }

                // (b) Active bid crosses or touches best ask
                if let Some(best_ask) = input.best_ask
                    && active_price >= best_ask
                {
                    let next_state = OrderSlotState::PendingCancel {
                        order_id: order_id.clone(),
                        next_quote: None,
                        sent_ns: input.now_ns,
                        reason: CancelReason::CrossesBook,
                    };
                    self.set_state(outcome, next_state);
                    return OrderAction::Cancel {
                        outcome,
                        order_id,
                        reason: CancelReason::CrossesBook,
                    };
                }

                // (c) Risk engine suppressed quote to None / 0
                let target_quote = match input.proposed_quote {
                    Some(q) if q.size >= min_shares_micro => q,
                    _ => {
                        let next_state = OrderSlotState::PendingCancel {
                            order_id: order_id.clone(),
                            next_quote: None,
                            sent_ns: input.now_ns,
                            reason: CancelReason::RiskSuppressed,
                        };
                        self.set_state(outcome, next_state);
                        return OrderAction::Cancel {
                            outcome,
                            order_id,
                            reason: CancelReason::RiskSuppressed,
                        };
                    }
                };

                // (d) Active remaining size exceeds newly authorized risk size
                if active_remaining > target_quote.size {
                    let next_state = OrderSlotState::PendingCancel {
                        order_id: order_id.clone(),
                        next_quote: Some(target_quote),
                        sent_ns: input.now_ns,
                        reason: CancelReason::ExcessiveSize,
                    };
                    self.set_state(outcome, next_state);
                    return OrderAction::CancelAndReplace {
                        outcome,
                        old_order_id: order_id,
                        new_price: target_quote.price,
                        new_size: target_quote.size,
                        reason: CancelReason::ExcessiveSize,
                    };
                }

                // (e) Partial fill stub refresh
                if active_remaining < min_shares_micro && active_remaining > SharesMicro::ZERO {
                    let next_state = OrderSlotState::PendingCancel {
                        order_id: order_id.clone(),
                        next_quote: Some(target_quote),
                        sent_ns: input.now_ns,
                        reason: CancelReason::StubRefresh,
                    };
                    self.set_state(outcome, next_state);
                    return OrderAction::CancelAndReplace {
                        outcome,
                        old_order_id: order_id,
                        new_price: target_quote.price,
                        new_size: target_quote.size,
                        reason: CancelReason::StubRefresh,
                    };
                }

                // 2. True Continuous Deadband evaluation on unrounded target price:
                let active_price_usd = active_price.0 as f64 / 1_000_000.0;
                let price_diff = (target_quote.price_raw_usd - active_price_usd).abs();

                if price_diff >= deadband_usd && target_quote.price != active_price {
                    // Price moved significantly outside the deadband and maps to a distinct tick.
                    let next_state = OrderSlotState::PendingCancel {
                        order_id: order_id.clone(),
                        next_quote: Some(target_quote),
                        sent_ns: input.now_ns,
                        reason: CancelReason::DeadbandDrift,
                    };
                    self.set_state(outcome, next_state);
                    OrderAction::CancelAndReplace {
                        outcome,
                        old_order_id: order_id,
                        new_price: target_quote.price,
                        new_size: target_quote.size,
                        reason: CancelReason::DeadbandDrift,
                    }
                } else {
                    // Within deadband: hold order to preserve FIFO queue seniority!
                    OrderAction::Hold
                }
            }
        }
    }

    /// Handles order placement confirmation ACK from exchange or simulator.
    pub fn on_place_ack(
        &mut self,
        outcome: Outcome,
        order_id: String,
        price: MicroUsdc,
        size: SharesMicro,
    ) {
        let state = OrderSlotState::Live {
            order_id,
            price,
            original_size: size,
            remaining_size: size,
        };
        self.set_state(outcome, state);
    }

    /// Handles order cancellation confirmation ACK from exchange or simulator.
    ///
    /// Returns queued `Some(SizedQuote)` if replacement was waiting behind cancel.
    pub fn on_cancel_ack(&mut self, outcome: Outcome) -> Option<SizedQuote> {
        let current_state = self.state(outcome).clone();
        if let OrderSlotState::PendingCancel { next_quote, .. } = current_state {
            self.set_state(outcome, OrderSlotState::Idle);
            next_quote
        } else {
            self.set_state(outcome, OrderSlotState::Idle);
            None
        }
    }

    /// Handles fill event, updating remaining size.
    pub fn on_fill(&mut self, outcome: Outcome, filled_shares: SharesMicro) {
        let current_state = self.state(outcome).clone();
        if let OrderSlotState::Live {
            order_id,
            price,
            original_size,
            remaining_size,
        } = current_state
        {
            let remaining = remaining_size.saturating_sub(filled_shares);
            if remaining == SharesMicro::ZERO {
                self.set_state(outcome, OrderSlotState::Idle);
            } else {
                self.set_state(
                    outcome,
                    OrderSlotState::Live {
                        order_id,
                        price,
                        original_size,
                        remaining_size: remaining,
                    },
                );
            }
        }
    }

    /// Handles fill event for a specific order ID, ignoring stale fills from superseded orders.
    pub fn on_fill_for_order(
        &mut self,
        outcome: Outcome,
        filled_order_id: &str,
        filled_shares: SharesMicro,
    ) {
        if let OrderSlotState::Live { order_id, .. } = self.state(outcome)
            && order_id == filled_order_id
        {
            self.on_fill(outcome, filled_shares);
        }
    }

    /// Transitions slot into `PendingReconcile` on unverified errors.
    pub fn on_reconcile_required(&mut self, outcome: Outcome, order_id: String) {
        self.set_state(outcome, OrderSlotState::PendingReconcile { order_id });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_vector_4_continuous_hysteresis() {
        // Continuous Hysteresis Vector:
        // Active $0.49, p_target_raw jitters 0.49999 <-> 0.50001 (diff = 0.01001 < 0.0125)
        // -> HOLD (zero churn!). When p_target_raw = 0.5030 (diff = 0.0130 >= 0.0125)
        // -> CancelAndReplace to $0.50!
        let config = HysteresisConfig {
            hysteresis_buf: 0.25,
            min_order_size_shares: 5,
        };
        let mut gate = HysteresisGate::new(config).unwrap();

        // 1. Setup active Live order at $0.49 for 20 shares.
        let active_price = MicroUsdc(490_000);
        let active_size = SharesMicro::from_raw(20 * ONE_SHARE_MICRO);
        gate.set_state(
            Outcome::Up,
            OrderSlotState::Live {
                order_id: "order_123".to_string(),
                price: active_price,
                original_size: active_size,
                remaining_size: active_size,
            },
        );

        let tick_micro = MicroUsdc(10_000); // 1 cent tick ($0.01)

        // 2. Test jitter case A: p_target_raw = 0.49999
        // diff = |0.49999 - 0.49| = 0.00999 < 0.0125 -> HOLD.
        let input_a = HysteresisInput {
            outcome: Outcome::Up,
            proposed_quote: Some(SizedQuote {
                price: MicroUsdc(490_000), // floored at $0.49
                size: active_size,
                price_raw_usd: 0.49999,
            }),
            tick_size_micro: tick_micro,
            best_ask: Some(MicroUsdc(520_000)),
            defensive_cancel: false,
            now_ns: 1_000_000,
            next_seq: 1,
        };
        assert_eq!(gate.evaluate_leg(&input_a), OrderAction::Hold);
        assert!(matches!(
            gate.state(Outcome::Up),
            OrderSlotState::Live { .. }
        ));

        // 3. Test jitter case B: p_target_raw = 0.50001
        // Note: floor_to_tick(0.50001) would be $0.50!
        // But diff = |0.50001 - 0.49| = 0.01001 < 0.0125 -> STILL HOLD!
        let input_b = HysteresisInput {
            outcome: Outcome::Up,
            proposed_quote: Some(SizedQuote {
                price: MicroUsdc(500_000), // floored at $0.50
                size: active_size,
                price_raw_usd: 0.50001,
            }),
            tick_size_micro: tick_micro,
            best_ask: Some(MicroUsdc(520_000)),
            defensive_cancel: false,
            now_ns: 2_000_000,
            next_seq: 2,
        };
        assert_eq!(gate.evaluate_leg(&input_b), OrderAction::Hold);
        assert!(matches!(
            gate.state(Outcome::Up),
            OrderSlotState::Live { .. }
        ));

        // 4. Test significant shift: p_target_raw = 0.5030
        // diff = |0.5030 - 0.49| = 0.0130 >= 0.0125
        // target_price = $0.50 != $0.49
        // -> CancelAndReplace to $0.50!
        let input_c = HysteresisInput {
            outcome: Outcome::Up,
            proposed_quote: Some(SizedQuote {
                price: MicroUsdc(500_000), // $0.50
                size: active_size,
                price_raw_usd: 0.5030,
            }),
            tick_size_micro: tick_micro,
            best_ask: Some(MicroUsdc(520_000)),
            defensive_cancel: false,
            now_ns: 3_000_000,
            next_seq: 3,
        };
        let action = gate.evaluate_leg(&input_c);
        assert_eq!(
            action,
            OrderAction::CancelAndReplace {
                outcome: Outcome::Up,
                old_order_id: "order_123".to_string(),
                new_price: MicroUsdc(500_000),
                new_size: active_size,
                reason: CancelReason::DeadbandDrift,
            }
        );
        // State should transition to PendingCancel with next_quote queued.
        assert!(matches!(
            gate.state(Outcome::Up),
            OrderSlotState::PendingCancel { .. }
        ));
    }

    #[test]
    fn test_defensive_cancel_bypasses_deadband() {
        let config = HysteresisConfig::default();
        let mut gate = HysteresisGate::new(config).unwrap();

        let active_price = MicroUsdc(490_000);
        let active_size = SharesMicro::from_raw(20 * ONE_SHARE_MICRO);
        gate.set_state(
            Outcome::Up,
            OrderSlotState::Live {
                order_id: "order_def".to_string(),
                price: active_price,
                original_size: active_size,
                remaining_size: active_size,
            },
        );

        let input = HysteresisInput {
            outcome: Outcome::Up,
            proposed_quote: Some(SizedQuote {
                price: active_price,
                size: active_size,
                price_raw_usd: 0.4900,
            }),
            tick_size_micro: MicroUsdc(10_000),
            best_ask: Some(MicroUsdc(520_000)),
            defensive_cancel: true,
            now_ns: 1_000_000,
            next_seq: 1,
        };

        assert_eq!(
            gate.evaluate_leg(&input),
            OrderAction::Cancel {
                outcome: Outcome::Up,
                order_id: "order_def".to_string(),
                reason: CancelReason::DefensiveCancel,
            }
        );
    }
}
