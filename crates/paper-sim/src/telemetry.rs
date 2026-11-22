use std::collections::VecDeque;

use core_types::{
    FeeSchedule, LatencySummary, LatencyTracker, MicroUsdc, OrderSide, Outcome, SharesMicro,
    SignedSharesMicro, TickPrecision,
};
use serde::{Deserialize, Serialize};

use crate::accounting::PaperLedger;
use crate::fifo_book::{FifoBrokerConfig, FifoPaperBroker, NewOrderParams, SimFill};

/// Input parameters for updating naive baseline quotes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NaiveQuotesInput {
    pub mid_up: Option<MicroUsdc>,
    pub mid_dn: Option<MicroUsdc>,
    pub best_bid_up: Option<MicroUsdc>,
    pub best_ask_up: Option<MicroUsdc>,
    pub best_bid_dn: Option<MicroUsdc>,
    pub best_ask_dn: Option<MicroUsdc>,
    pub tick: TickPrecision,
    pub depth_up: SharesMicro,
    pub depth_dn: SharesMicro,
    pub now_ms: u64,
}

/// Snapshot of skew and spread trajectory at a point in time.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct TrajectorySnapshot {
    /// Timestamp in Unix milliseconds.
    pub ts_ms: u64,
    /// Time remaining to epoch expiry in seconds.
    pub tau_secs: f64,
    /// Optimal spread `s*` from Station 4.
    pub s_star: f64,
    /// Reservation price `r_i` from Station 5.
    pub r_i: f64,
    /// Raw unrounded UP bid from Station 6.
    pub bid_up_raw: f64,
    /// Quantized UP bid micro-price.
    pub bid_up: Option<MicroUsdc>,
    /// Quantized DOWN bid micro-price.
    pub bid_dn: Option<MicroUsdc>,
    /// Current net inventory `q = q_UP - q_DOWN`.
    pub q_inventory: i64,
}

/// Markout probe tracking mid-price drift 1s and 5s after a maker fill.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct FillMarkoutProbe {
    pub fill_ts_ms: u64,
    pub outcome: Outcome,
    pub side: OrderSide,
    pub fill_price: MicroUsdc,
    pub mid_at_fill: MicroUsdc,
    pub checked_1s: bool,
    pub checked_5s: bool,
}

/// Naive static-spread (`mid ± 1 tick`) benchmark engine running in parallel.
#[derive(Debug, Clone)]
pub struct NaiveBaselineEngine {
    pub broker: FifoPaperBroker,
    pub ledger: PaperLedger,
    pub base_size: SharesMicro,
    pub active_up_id: Option<u64>,
    pub active_dn_id: Option<u64>,
}

impl NaiveBaselineEngine {
    /// Constructs a naive static-spread benchmark engine with the same latency config.
    #[must_use]
    pub const fn new(
        config: FifoBrokerConfig,
        fee_schedule: FeeSchedule,
        base_size: SharesMicro,
    ) -> Self {
        Self {
            broker: FifoPaperBroker::new(config),
            ledger: PaperLedger::new(fee_schedule),
            base_size,
            active_up_id: None,
            active_dn_id: None,
        }
    }

    /// Updates naive quotes pegged strictly to `mid - 1 tick` on both sides.
    pub fn update_quotes(&mut self, input: NaiveQuotesInput) {
        let tick_micro = input.tick.tick_micro();

        // Update UP naive quote
        if let Some(mid) = input.mid_up
            && mid.0 > tick_micro.0
        {
            let target_price = MicroUsdc(mid.0.saturating_sub(tick_micro.0));
            let should_replace = match self.active_up_id.and_then(|id| self.broker.get_order(id)) {
                Some(order) => order.price != target_price || !order.status.is_active(),
                None => true,
            };
            if should_replace {
                if let Some(old_id) = self.active_up_id {
                    let _ = self.broker.cancel(old_id, input.now_ms);
                }
                if let Ok(new_id) = self.broker.submit_new(NewOrderParams {
                    outcome: Outcome::Up,
                    side: OrderSide::Buy,
                    price: target_price,
                    size: self.base_size,
                    now_ms: input.now_ms,
                    current_best_bid: input.best_bid_up,
                    current_best_ask: input.best_ask_up,
                    book_depth_at_price: input.depth_up,
                }) {
                    self.active_up_id = Some(new_id);
                }
            }
        }

        // Update DOWN naive quote
        if let Some(mid) = input.mid_dn
            && mid.0 > tick_micro.0
        {
            let target_price = MicroUsdc(mid.0.saturating_sub(tick_micro.0));
            let should_replace = match self.active_dn_id.and_then(|id| self.broker.get_order(id)) {
                Some(order) => order.price != target_price || !order.status.is_active(),
                None => true,
            };
            if should_replace {
                if let Some(old_id) = self.active_dn_id {
                    let _ = self.broker.cancel(old_id, input.now_ms);
                }
                if let Ok(new_id) = self.broker.submit_new(NewOrderParams {
                    outcome: Outcome::Down,
                    side: OrderSide::Buy,
                    price: target_price,
                    size: self.base_size,
                    now_ms: input.now_ms,
                    current_best_bid: input.best_bid_dn,
                    current_best_ask: input.best_ask_dn,
                    book_depth_at_price: input.depth_dn,
                }) {
                    self.active_dn_id = Some(new_id);
                }
            }
        }
    }

    /// Evaluates public trade against naive resting orders.
    pub fn on_public_trade(
        &mut self,
        outcome: Outcome,
        trade_price: MicroUsdc,
        trade_size: SharesMicro,
        now_ms: u64,
    ) {
        let fills = self
            .broker
            .on_public_trade(outcome, trade_price, trade_size, now_ms);
        for fill in &fills {
            let _ = self.ledger.record_fill(fill);
        }
    }

    /// Evaluates directional public trade against naive resting orders.
    pub fn on_public_trade_with_side(
        &mut self,
        outcome: Outcome,
        trade_side: OrderSide,
        trade_price: MicroUsdc,
        trade_size: SharesMicro,
        now_ms: u64,
    ) {
        let fills = self.broker.on_public_trade_with_side(
            outcome,
            trade_side,
            trade_price,
            trade_size,
            now_ms,
        );
        for fill in &fills {
            let _ = self.ledger.record_fill(fill);
        }
    }

    /// Cancels all active naive baseline quotes and immediately halts pending fills on expiry.
    pub fn cancel_all(&mut self, now_ms: u64) -> usize {
        let mut cancelled = 0_usize;
        if let Some(id) = self.active_up_id.take()
            && self.broker.cancel(id, now_ms).is_ok()
        {
            cancelled = cancelled.saturating_add(1);
        }
        if let Some(id) = self.active_dn_id.take()
            && self.broker.cancel(id, now_ms).is_ok()
        {
            cancelled = cancelled.saturating_add(1);
        }
        cancelled.saturating_add(self.broker.force_cancel_all())
    }
}

/// Baseline telemetry tracker collecting RPM, trajectories, paired latency,
/// fill ratios, post-fill markouts, and null-baseline comparative performance.
pub struct BaselineTelemetry {
    cancel_replace_timestamps_ms: VecDeque<u64>,
    trajectories: Vec<TrajectorySnapshot>,
    unpaired_start_ts_ms: Option<u64>,
    pair_latency_tracker: LatencyTracker,
    total_posted_shares: SharesMicro,
    total_filled_shares: SharesMicro,
    markout_probes: Vec<FillMarkoutProbe>,
    markout_1s_sum_micro: i64,
    markout_1s_count: u64,
    markout_5s_sum_micro: i64,
    markout_5s_count: u64,
    pub naive_engine: NaiveBaselineEngine,
}

impl BaselineTelemetry {
    /// Constructs a new baseline telemetry engine.
    pub fn new(
        config: FifoBrokerConfig,
        fee_schedule: FeeSchedule,
        base_size: SharesMicro,
    ) -> Result<Self, core_types::TelemetryError> {
        let pair_latency_tracker = LatencyTracker::new()?;
        Ok(Self {
            cancel_replace_timestamps_ms: VecDeque::with_capacity(1024),
            trajectories: Vec::with_capacity(512),
            unpaired_start_ts_ms: None,
            pair_latency_tracker,
            total_posted_shares: SharesMicro::ZERO,
            total_filled_shares: SharesMicro::ZERO,
            markout_probes: Vec::with_capacity(256),
            markout_1s_sum_micro: 0,
            markout_1s_count: 0,
            markout_5s_sum_micro: 0,
            markout_5s_count: 0,
            naive_engine: NaiveBaselineEngine::new(config, fee_schedule, base_size),
        })
    }

    /// Records a CancelAndReplace event timestamp for rolling 60s RPM.
    pub fn record_cancel_replace(&mut self, now_ms: u64) {
        self.cancel_replace_timestamps_ms.push_back(now_ms);
        self.prune_rpm_timestamps(now_ms);
    }

    /// Prunes timestamps older than trailing 60 seconds.
    fn prune_rpm_timestamps(&mut self, now_ms: u64) {
        let cutoff = now_ms.saturating_sub(60_000);
        while let Some(&front) = self.cancel_replace_timestamps_ms.front() {
            if front < cutoff {
                self.cancel_replace_timestamps_ms.pop_front();
            } else {
                break;
            }
        }
    }

    /// Returns current CancelAndReplace requests per minute (RPM) over trailing 60s.
    #[must_use]
    pub fn rpm_60s(&mut self, now_ms: u64) -> usize {
        self.prune_rpm_timestamps(now_ms);
        self.cancel_replace_timestamps_ms.len()
    }

    /// Records an order submission to track posted volume for fill ratio.
    pub fn record_order_posted(&mut self, size: SharesMicro) {
        self.total_posted_shares.0 = self.total_posted_shares.0.saturating_add(size.0);
    }

    /// Records a fill event: updates filled shares, starts/completes paired fill latency,
    /// and enqueues a markout probe.
    pub fn record_fill(
        &mut self,
        fill: &SimFill,
        inventory_before: SignedSharesMicro,
        inventory_after: SignedSharesMicro,
        mid_at_fill: MicroUsdc,
    ) {
        self.total_filled_shares.0 = self
            .total_filled_shares
            .0
            .saturating_add(fill.filled_shares.0);

        // Paired fill latency tracking:
        // If inventory was flat before and is now non-zero, start clock.
        if inventory_before.as_i64() == 0 && inventory_after.as_i64() != 0 {
            self.unpaired_start_ts_ms = Some(fill.fill_ts_ms);
        } else if inventory_before.as_i64() != 0 && inventory_after.as_i64() == 0 {
            // Rebalanced completely flat!
            if let Some(start_ts) = self.unpaired_start_ts_ms.take() {
                let latency_ms = fill.fill_ts_ms.saturating_sub(start_ts);
                let latency_ns = latency_ms.saturating_mul(1_000_000);
                let _ = self.pair_latency_tracker.record_ns(latency_ns);
            }
        }

        // Enqueue markout probe
        self.markout_probes.push(FillMarkoutProbe {
            fill_ts_ms: fill.fill_ts_ms,
            outcome: fill.outcome,
            side: fill.side,
            fill_price: fill.price,
            mid_at_fill,
            checked_1s: false,
            checked_5s: false,
        });
    }

    /// Evaluates pending markout probes against current mid prices.
    pub fn update_markouts(&mut self, now_ms: u64, mid_up: MicroUsdc, mid_dn: MicroUsdc) {
        for probe in &mut self.markout_probes {
            let current_mid = match probe.outcome {
                Outcome::Up => mid_up,
                Outcome::Down => mid_dn,
            };

            // 1-second markout probe
            if !probe.checked_1s && now_ms >= probe.fill_ts_ms.saturating_add(1000) {
                probe.checked_1s = true;
                // For BUY fill, price dropping represents adverse selection (negative markout)
                let markout = (current_mid.0 as i64) - (probe.mid_at_fill.0 as i64);
                self.markout_1s_sum_micro = self.markout_1s_sum_micro.saturating_add(markout);
                self.markout_1s_count = self.markout_1s_count.saturating_add(1);
            }

            // 5-second markout probe
            if !probe.checked_5s && now_ms >= probe.fill_ts_ms.saturating_add(5000) {
                probe.checked_5s = true;
                let markout = (current_mid.0 as i64) - (probe.mid_at_fill.0 as i64);
                self.markout_5s_sum_micro = self.markout_5s_sum_micro.saturating_add(markout);
                self.markout_5s_count = self.markout_5s_count.saturating_add(1);
            }
        }

        // Retain only uncompleted probes (completed 5s probes can be pruned)
        self.markout_probes.retain(|p| !p.checked_5s);
    }

    /// Records a (T - t) trajectory snapshot.
    pub fn record_trajectory(&mut self, snapshot: TrajectorySnapshot) {
        self.trajectories.push(snapshot);
    }

    /// Returns the overall fill ratio `filled / posted`.
    #[must_use]
    pub fn fill_ratio(&self) -> f64 {
        if self.total_posted_shares.0 == 0 {
            0.0
        } else {
            self.total_filled_shares.0 as f64 / self.total_posted_shares.0 as f64
        }
    }

    /// Average 1-second post-fill markout in basis points.
    #[must_use]
    pub fn avg_markout_1s_bps(&self) -> f64 {
        if self.markout_1s_count == 0 {
            0.0
        } else {
            let avg_micro = self.markout_1s_sum_micro as f64 / self.markout_1s_count as f64;
            // 1 USDC = 10,000 bps. 1 micro-USDC = 0.01 bps.
            avg_micro / 100.0
        }
    }

    /// Average 5-second post-fill markout in basis points.
    #[must_use]
    pub fn avg_markout_5s_bps(&self) -> f64 {
        if self.markout_5s_count == 0 {
            0.0
        } else {
            let avg_micro = self.markout_5s_sum_micro as f64 / self.markout_5s_count as f64;
            avg_micro / 100.0
        }
    }

    /// Returns latency percentiles for roundtrip paired fills.
    #[must_use]
    pub fn paired_fill_latency_summary(&self) -> LatencySummary {
        self.pair_latency_tracker.summary()
    }

    /// Returns reference to all recorded trajectory snapshots.
    #[must_use]
    pub fn trajectories(&self) -> &[TrajectorySnapshot] {
        &self.trajectories
    }

    /// Clears pending cross-epoch markout probes and unpaired-fill timer on epoch rollover.
    pub fn reset_epoch(&mut self) {
        self.markout_probes.clear();
        self.unpaired_start_ts_ms = None;
    }

    /// Returns count of pending uncompleted markout probes.
    #[must_use]
    pub fn pending_markout_probe_count(&self) -> usize {
        self.markout_probes.len()
    }
}
