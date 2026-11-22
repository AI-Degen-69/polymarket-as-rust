use std::collections::BTreeMap;

use core_types::{MicroUsdc, ONE_DOLLAR_MICRO, OrderSide, Outcome, SharesMicro};
use serde::{Deserialize, Serialize};

use crate::error::PaperSimError;

/// Queue decay policy for book updates ahead of resting orders.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum QueueDecayPolicy {
    /// Only public trade prints deplete volume ahead in the queue (default conservative).
    #[default]
    Conservative,
    /// Book depth drops immediately reduce `volume_ahead = min(volume_ahead, new_depth)`.
    Optimistic,
}

/// Configuration parameters for simulated wire latency and queue behavior.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct FifoBrokerConfig {
    /// One-way wire placement latency in milliseconds (default `40ms`).
    pub order_latency_ms: u64,
    /// Cancel in-flight wire latency in milliseconds (default `40ms`).
    pub cancel_latency_ms: u64,
    /// Policy governing queue depletion on book depth drops.
    pub queue_decay_policy: QueueDecayPolicy,
}

impl Default for FifoBrokerConfig {
    fn default() -> Self {
        Self {
            order_latency_ms: 40,
            cancel_latency_ms: 40,
            queue_decay_policy: QueueDecayPolicy::Conservative,
        }
    }
}

/// Lifecycle state machine of a simulated resting order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SimOrderStatus {
    /// In flight to exchange; cannot be filled until `active_at_ms`.
    PendingPlace { active_at_ms: u64 },
    /// Resting on the exchange orderbook with queue priority.
    Live,
    /// Cancel request in flight; STILL subject to fills until `cancel_at_ms`!
    PendingCancel { cancel_at_ms: u64 },
    /// Completely filled.
    Filled,
    /// Cancel confirmed and removed from the active book.
    Cancelled,
}

impl SimOrderStatus {
    /// Returns true if the order is live or in-flight cancelling (and thus fillable).
    #[must_use]
    pub const fn is_fillable(self) -> bool {
        matches!(self, Self::Live | Self::PendingCancel { .. })
    }

    /// Returns true if the order is actively resting or in-flight (not final).
    #[must_use]
    pub const fn is_active(self) -> bool {
        matches!(
            self,
            Self::PendingPlace { .. } | Self::Live | Self::PendingCancel { .. }
        )
    }

    /// String representation for logging.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::PendingPlace { .. } => "PendingPlace",
            Self::Live => "Live",
            Self::PendingCancel { .. } => "PendingCancel",
            Self::Filled => "Filled",
            Self::Cancelled => "Cancelled",
        }
    }
}

/// Simulated resting order tracked in the FIFO matching engine.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SimOrder {
    /// Monotonic unique order sequence ID.
    pub sim_id: u64,
    /// Market outcome (`Up` or `Down`).
    pub outcome: Outcome,
    /// Order side (`Buy` or `Sell`).
    pub side: OrderSide,
    /// Limit price in micro-USDC.
    pub price: MicroUsdc,
    /// Initial order size in micro-shares.
    pub original_size: SharesMicro,
    /// Unfilled order size in micro-shares.
    pub remaining_size: SharesMicro,
    /// Volume ahead of this order in the FIFO queue at this price level.
    pub volume_ahead: SharesMicro,
    /// Current lifecycle status.
    pub status: SimOrderStatus,
    /// Creation timestamp in Unix milliseconds.
    pub created_at_ms: u64,
    /// Post-only flag (always true for passive maker quotes).
    pub is_post_only: bool,
}

/// Execution fill event emitted when a resting order matches a public trade.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct SimFill {
    /// ID of the simulated order that filled.
    pub sim_id: u64,
    /// Market outcome (`Up` or `Down`).
    pub outcome: Outcome,
    /// Order side (`Buy` or `Sell`).
    pub side: OrderSide,
    /// Execution price in micro-USDC.
    pub price: MicroUsdc,
    /// Executed fill quantity in micro-shares.
    pub filled_shares: SharesMicro,
    /// Execution timestamp in Unix milliseconds.
    pub fill_ts_ms: u64,
    /// True if executed as a passive maker fill (0 platform fee).
    pub is_maker: bool,
}

/// Top-of-book and L2 FIFO queue-aware paper broker matching engine.
#[derive(Debug, Clone)]
pub struct FifoPaperBroker {
    config: FifoBrokerConfig,
    next_seq: u64,
    orders: BTreeMap<u64, SimOrder>,
}

/// Parameters for submitting a new simulated maker order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct NewOrderParams {
    /// Market outcome (`Up` or `Down`).
    pub outcome: Outcome,
    /// Order side (`Buy` or `Sell`).
    pub side: OrderSide,
    /// Limit price in micro-USDC.
    pub price: MicroUsdc,
    /// Initial order size in micro-shares.
    pub size: SharesMicro,
    /// Submission timestamp in Unix milliseconds.
    pub now_ms: u64,
    /// Current best bid price (if available) for post-only crossing validation.
    pub current_best_bid: Option<MicroUsdc>,
    /// Current best ask price (if available) for post-only crossing validation.
    pub current_best_ask: Option<MicroUsdc>,
    /// Book depth resting at this price level ahead in queue.
    pub book_depth_at_price: SharesMicro,
}

impl FifoPaperBroker {
    /// Constructs a new FIFO paper broker with the given latency configuration.
    #[must_use]
    pub const fn new(config: FifoBrokerConfig) -> Self {
        Self {
            config,
            next_seq: 0,
            orders: BTreeMap::new(),
        }
    }

    /// Returns the configuration.
    #[must_use]
    pub const fn config(&self) -> &FifoBrokerConfig {
        &self.config
    }

    /// Advances the broker clock, promoting pending placements to live
    /// and finalizing pending cancels that reached their flight deadline.
    pub fn update_time(&mut self, now_ms: u64) {
        for order in self.orders.values_mut() {
            match order.status {
                SimOrderStatus::PendingPlace { active_at_ms } if now_ms >= active_at_ms => {
                    order.status = SimOrderStatus::Live;
                }
                SimOrderStatus::PendingCancel { cancel_at_ms } if now_ms >= cancel_at_ms => {
                    order.status = SimOrderStatus::Cancelled;
                }
                _ => {}
            }
        }
    }

    /// Submits a new maker limit order with strict Post-Only crossing validation,
    /// FIFO queue initialization, and simulated wire placement latency.
    pub fn submit_new(&mut self, params: NewOrderParams) -> Result<u64, PaperSimError> {
        if params.price.0 == 0 || params.price.0 >= ONE_DOLLAR_MICRO {
            return Err(PaperSimError::InvalidOrderPrice {
                price: params.price,
            });
        }
        if params.size.0 == 0 {
            return Err(PaperSimError::InvalidOrderSize {
                shares: params.size,
            });
        }

        // Post-Only crossing rejection check
        match params.side {
            OrderSide::Buy => {
                if let Some(best_ask) = params.current_best_ask
                    && params.price.0 >= best_ask.0
                {
                    return Err(PaperSimError::PostOnlyCrossed {
                        price: params.price,
                        best_ask,
                    });
                }
            }
            OrderSide::Sell => {
                if let Some(best_bid) = params.current_best_bid
                    && params.price.0 <= best_bid.0
                {
                    return Err(PaperSimError::PostOnlyCrossedSell {
                        price: params.price,
                        best_bid,
                    });
                }
            }
        }

        // Advance time first to ensure accurate state
        self.update_time(params.now_ms);

        self.next_seq = self
            .next_seq
            .checked_add(1)
            .ok_or(PaperSimError::ArithmeticOverflow)?;
        let sim_id = self.next_seq;

        let status = if self.config.order_latency_ms == 0 {
            SimOrderStatus::Live
        } else {
            SimOrderStatus::PendingPlace {
                active_at_ms: params.now_ms.saturating_add(self.config.order_latency_ms),
            }
        };

        let order = SimOrder {
            sim_id,
            outcome: params.outcome,
            side: params.side,
            price: params.price,
            original_size: params.size,
            remaining_size: params.size,
            volume_ahead: params.book_depth_at_price,
            status,
            created_at_ms: params.now_ms,
            is_post_only: true,
        };

        self.orders.insert(sim_id, order);
        Ok(sim_id)
    }

    /// Requests cancellation of a resting order, modeling wire flight latency.
    ///
    /// The order remains fillable until `now_ms >= cancel_at_ms`.
    pub fn cancel(&mut self, sim_id: u64, now_ms: u64) -> Result<(), PaperSimError> {
        self.update_time(now_ms);
        let order = self
            .orders
            .get_mut(&sim_id)
            .ok_or(PaperSimError::OrderNotFound { sim_id })?;

        match order.status {
            SimOrderStatus::Live => {
                if self.config.cancel_latency_ms == 0 {
                    order.status = SimOrderStatus::Cancelled;
                } else {
                    order.status = SimOrderStatus::PendingCancel {
                        cancel_at_ms: now_ms.saturating_add(self.config.cancel_latency_ms),
                    };
                }
                Ok(())
            }
            SimOrderStatus::PendingPlace { active_at_ms } => {
                // Cancel was fired while placement was still in flight.
                // The cancel arrives at now_ms + cancel_latency_ms.
                let cancel_at_ms = now_ms.saturating_add(self.config.cancel_latency_ms);
                if cancel_at_ms <= active_at_ms {
                    // Cancel catches up or arrives before/with placement -> never goes live
                    order.status = SimOrderStatus::Cancelled;
                } else {
                    // Goes live at active_at_ms, dies at cancel_at_ms
                    order.status = SimOrderStatus::PendingCancel { cancel_at_ms };
                }
                Ok(())
            }
            SimOrderStatus::PendingCancel { .. } => Ok(()),
            SimOrderStatus::Filled => Err(PaperSimError::InvalidCancelState {
                sim_id,
                status: "Filled",
            }),
            SimOrderStatus::Cancelled => Err(PaperSimError::InvalidCancelState {
                sim_id,
                status: "Cancelled",
            }),
        }
    }

    /// Immediately cancels every active or pending-cancel order (used at epoch settlement
    /// so expired-epoch orders cannot receive post-settlement fills).
    pub fn force_cancel_all(&mut self) -> usize {
        let mut cancelled = 0_usize;
        for order in self.orders.values_mut() {
            if order.status.is_active() {
                order.status = SimOrderStatus::Cancelled;
                cancelled = cancelled.saturating_add(1);
            }
        }
        cancelled
    }

    /// Evaluates a public trade event against fillable resting orders on the book.
    ///
    /// Follows strict FIFO queue priority:
    /// - Trade through (`trade_price < order.price` for Buy): fills `min(remaining, trade_size)`.
    /// - At limit (`trade_price == order.price`): decrements `volume_ahead`; fills `min(remaining, trade_size - prev_vol)`.
    /// - Out of range: no fill.
    pub fn on_public_trade(
        &mut self,
        outcome: Outcome,
        trade_price: MicroUsdc,
        trade_size: SharesMicro,
        now_ms: u64,
    ) -> Vec<SimFill> {
        self.update_time(now_ms);

        let mut fills = Vec::new();
        let mut trade_avail = trade_size.0;

        for order in self.orders.values_mut() {
            if order.outcome != outcome
                || !order.status.is_fillable()
                || order.remaining_size.0 == 0
            {
                continue;
            }

            match order.side {
                OrderSide::Buy => {
                    if trade_price.0 < order.price.0 {
                        // Traded strictly through our bid
                        let fill_qty = order.remaining_size.0.min(trade_avail);
                        if fill_qty > 0 {
                            order.remaining_size.0 =
                                order.remaining_size.0.saturating_sub(fill_qty);
                            order.volume_ahead = SharesMicro::ZERO;
                            trade_avail = trade_avail.saturating_sub(fill_qty);
                            fills.push(SimFill {
                                sim_id: order.sim_id,
                                outcome,
                                side: order.side,
                                price: order.price,
                                filled_shares: SharesMicro(fill_qty),
                                fill_ts_ms: now_ms,
                                is_maker: true,
                            });
                        }
                    } else if trade_price.0 == order.price.0 {
                        // Traded at our bid price: queue depletion
                        let prev_vol = order.volume_ahead.0;
                        order.volume_ahead.0 = order.volume_ahead.0.saturating_sub(trade_avail);
                        let reachable = trade_avail.saturating_sub(prev_vol);
                        let fill_qty = order.remaining_size.0.min(reachable);
                        if fill_qty > 0 {
                            order.remaining_size.0 =
                                order.remaining_size.0.saturating_sub(fill_qty);
                            trade_avail = trade_avail.saturating_sub(fill_qty);
                            fills.push(SimFill {
                                sim_id: order.sim_id,
                                outcome,
                                side: order.side,
                                price: order.price,
                                filled_shares: SharesMicro(fill_qty),
                                fill_ts_ms: now_ms,
                                is_maker: true,
                            });
                        }
                    }
                }
                OrderSide::Sell => {
                    if trade_price.0 > order.price.0 {
                        // Traded strictly through our ask
                        let fill_qty = order.remaining_size.0.min(trade_avail);
                        if fill_qty > 0 {
                            order.remaining_size.0 =
                                order.remaining_size.0.saturating_sub(fill_qty);
                            order.volume_ahead = SharesMicro::ZERO;
                            trade_avail = trade_avail.saturating_sub(fill_qty);
                            fills.push(SimFill {
                                sim_id: order.sim_id,
                                outcome,
                                side: order.side,
                                price: order.price,
                                filled_shares: SharesMicro(fill_qty),
                                fill_ts_ms: now_ms,
                                is_maker: true,
                            });
                        }
                    } else if trade_price.0 == order.price.0 {
                        // Traded at our ask price: queue depletion
                        let prev_vol = order.volume_ahead.0;
                        order.volume_ahead.0 = order.volume_ahead.0.saturating_sub(trade_avail);
                        let reachable = trade_avail.saturating_sub(prev_vol);
                        let fill_qty = order.remaining_size.0.min(reachable);
                        if fill_qty > 0 {
                            order.remaining_size.0 =
                                order.remaining_size.0.saturating_sub(fill_qty);
                            trade_avail = trade_avail.saturating_sub(fill_qty);
                            fills.push(SimFill {
                                sim_id: order.sim_id,
                                outcome,
                                side: order.side,
                                price: order.price,
                                filled_shares: SharesMicro(fill_qty),
                                fill_ts_ms: now_ms,
                                is_maker: true,
                            });
                        }
                    }
                }
            }

            if order.remaining_size.0 == 0 {
                order.status = SimOrderStatus::Filled;
            }
        }

        fills
    }

    /// Handles book depth drop at `price`, optionally reducing `volume_ahead` under `Optimistic` decay.
    pub fn on_book_update(
        &mut self,
        outcome: Outcome,
        price: MicroUsdc,
        new_depth: SharesMicro,
        now_ms: u64,
    ) {
        self.update_time(now_ms);
        if self.config.queue_decay_policy == QueueDecayPolicy::Optimistic {
            for order in self.orders.values_mut() {
                if order.outcome == outcome && order.price == price && order.status.is_fillable() {
                    order.volume_ahead.0 = order.volume_ahead.0.min(new_depth.0);
                }
            }
        }
    }

    /// Retrieves an order by its unique sequence ID.
    #[must_use]
    pub fn get_order(&self, sim_id: u64) -> Option<&SimOrder> {
        self.orders.get(&sim_id)
    }

    /// Returns all orders currently tracked.
    #[must_use]
    pub const fn orders(&self) -> &BTreeMap<u64, SimOrder> {
        &self.orders
    }

    /// Returns active resting order for an outcome and side (if any).
    #[must_use]
    pub fn active_order(&self, outcome: Outcome, side: OrderSide) -> Option<&SimOrder> {
        self.orders.values().rev().find(|o| {
            o.outcome == outcome
                && o.side == side
                && matches!(
                    o.status,
                    SimOrderStatus::PendingPlace { .. } | SimOrderStatus::Live
                )
        })
    }

    /// Returns total active unhedged open bid notional in micro-USDC for an outcome.
    #[must_use]
    pub fn open_bid_notional(&self, outcome: Outcome) -> u64 {
        let mut notional = 0_u64;
        for order in self.orders.values() {
            if order.outcome == outcome
                && order.side == OrderSide::Buy
                && matches!(
                    order.status,
                    SimOrderStatus::PendingPlace { .. } | SimOrderStatus::Live
                )
            {
                let cost = (order.remaining_size.0 as u128 * order.price.0 as u128
                    / core_types::MICRO_SCALE as u128) as u64;
                notional = notional.saturating_add(cost);
            }
        }
        notional
    }

    /// Returns total active open bid shares for an outcome.
    #[must_use]
    pub fn open_bid_shares(&self, outcome: Outcome) -> SharesMicro {
        let mut total = 0_u64;
        for order in self.orders.values() {
            if order.outcome == outcome
                && order.side == OrderSide::Buy
                && matches!(
                    order.status,
                    SimOrderStatus::PendingPlace { .. } | SimOrderStatus::Live
                )
            {
                total = total.saturating_add(order.remaining_size.0);
            }
        }
        SharesMicro(total)
    }
}
