use core_types::MicroUsdc;
use serde::{Deserialize, Serialize};

/// State of an individual circuit breaker or overall risk gate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum BreakerStatus {
    /// Normal operation; all checks green.
    ArmActive,
    /// Breaker tripped; quoting halted.
    Tripped {
        /// Reason for trip.
        reason: BreakerReason,
    },
}

/// Explicit reason why a circuit breaker was tripped.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum BreakerReason {
    /// Consecutive HTTP/WS API errors (e.g. >= 5 within 10s).
    ConsecutiveApiErrors,
    /// Polymarket CLOB WebSocket silent for > 3,000ms.
    PolymarketFeedStale,
    /// Binance BTC WebSocket silent for > 2,000ms.
    BinanceFeedStale,
    /// System clock drifted > 1,000ms from exchange server time.
    ClockDriftExceeded,
    /// Dynamic tick size changed; waiting for pricing pipeline sync.
    TickSizeChangeFreeze,
    /// Actor thread panic or supervisor emergency kill-switch.
    SupervisorKillSwitch,
}

/// Configuration thresholds for circuit breakers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BreakerConfig {
    /// Max consecutive errors allowed before tripping (default 5).
    pub max_consecutive_errors: u32,
    /// Time window for error tracking in milliseconds (default 10,000ms).
    pub error_window_ms: u64,
    /// Polymarket staleness timeout in milliseconds (default 3,000ms).
    pub poly_stale_ms: u64,
    /// Binance staleness timeout in milliseconds (default 2,000ms).
    pub binance_stale_ms: u64,
    /// Maximum clock drift in milliseconds (default 1,000ms).
    pub max_clock_drift_ms: i64,
}

impl Default for BreakerConfig {
    fn default() -> Self {
        Self {
            max_consecutive_errors: 5,
            error_window_ms: 10_000,
            poly_stale_ms: 3_000,
            binance_stale_ms: 2_000,
            max_clock_drift_ms: 1_000,
        }
    }
}

/// Unified circuit breaker suite monitoring feeds, errors, clock drift, and tick changes.
#[derive(Debug, Clone)]
pub struct CircuitBreakers {
    config: BreakerConfig,
    consecutive_errors: u32,
    first_error_ts_ms: u64,
    last_poly_msg_ns: u64,
    last_binance_msg_ns: u64,
    active_tick_size: MicroUsdc,
    frozen_for_tick_change: bool,
    kill_switch_tripped: bool,
}

impl CircuitBreakers {
    /// Initializes circuit breakers with default or custom configuration.
    #[must_use]
    pub fn new(config: BreakerConfig, initial_tick_size: MicroUsdc, now_ns: u64) -> Self {
        Self {
            config,
            consecutive_errors: 0,
            first_error_ts_ms: 0,
            last_poly_msg_ns: now_ns,
            last_binance_msg_ns: now_ns,
            active_tick_size: initial_tick_size,
            frozen_for_tick_change: false,
            kill_switch_tripped: false,
        }
    }

    /// Records receipt of a message from the Polymarket CLOB feed.
    pub fn record_poly_heartbeat(&mut self, now_ns: u64) {
        self.last_poly_msg_ns = now_ns;
    }

    /// Records receipt of a message from the Binance spot feed.
    pub fn record_binance_heartbeat(&mut self, now_ns: u64) {
        self.last_binance_msg_ns = now_ns;
    }

    /// Records an execution API error (e.g. 400, 401, 503).
    pub fn record_api_error(&mut self, now_ms: u64) {
        if self.consecutive_errors == 0
            || now_ms.saturating_sub(self.first_error_ts_ms) > self.config.error_window_ms
        {
            self.first_error_ts_ms = now_ms;
            self.consecutive_errors = 1;
        } else {
            self.consecutive_errors = self.consecutive_errors.saturating_add(1);
        }
    }

    /// Records a successful API response, resetting the consecutive error counter.
    pub fn record_api_success(&mut self) {
        self.consecutive_errors = 0;
        self.first_error_ts_ms = 0;
    }

    /// Signals a market tick size change event.
    pub fn on_tick_size_change(&mut self, new_tick_size: MicroUsdc) {
        if new_tick_size != self.active_tick_size {
            self.active_tick_size = new_tick_size;
            self.frozen_for_tick_change = true;
        }
    }

    /// Acknowledges that pricing pipeline has recomputed with the new tick size.
    pub fn acknowledge_tick_size_sync(&mut self) {
        self.frozen_for_tick_change = false;
    }

    /// Manually trips emergency supervisor kill-switch.
    pub fn trip_kill_switch(&mut self) {
        self.kill_switch_tripped = true;
    }

    /// Evaluates all circuit breakers against current timestamps and clock offset.
    #[must_use]
    pub fn check_status(&self, now_ns: u64, clock_offset_ms: i64) -> BreakerStatus {
        if self.kill_switch_tripped {
            return BreakerStatus::Tripped {
                reason: BreakerReason::SupervisorKillSwitch,
            };
        }

        // 1. Check consecutive API errors breaker
        if self.consecutive_errors >= self.config.max_consecutive_errors {
            return BreakerStatus::Tripped {
                reason: BreakerReason::ConsecutiveApiErrors,
            };
        }

        // 2. Check Polymarket feed staleness breaker
        let poly_age_ns = now_ns.saturating_sub(self.last_poly_msg_ns);
        let poly_stale_ns = self.config.poly_stale_ms.saturating_mul(1_000_000);
        if poly_age_ns > poly_stale_ns {
            return BreakerStatus::Tripped {
                reason: BreakerReason::PolymarketFeedStale,
            };
        }

        // 3. Check Binance feed staleness breaker
        let binance_age_ns = now_ns.saturating_sub(self.last_binance_msg_ns);
        let binance_stale_ns = self.config.binance_stale_ms.saturating_mul(1_000_000);
        if binance_age_ns > binance_stale_ns {
            return BreakerStatus::Tripped {
                reason: BreakerReason::BinanceFeedStale,
            };
        }

        // 4. Check Clock Drift breaker
        if clock_offset_ms.abs() > self.config.max_clock_drift_ms {
            return BreakerStatus::Tripped {
                reason: BreakerReason::ClockDriftExceeded,
            };
        }

        // 5. Check Tick Size Change freeze
        if self.frozen_for_tick_change {
            return BreakerStatus::Tripped {
                reason: BreakerReason::TickSizeChangeFreeze,
            };
        }

        BreakerStatus::ArmActive
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_feed_staleness_and_consecutive_errors() {
        let config = BreakerConfig {
            max_consecutive_errors: 5,
            error_window_ms: 10_000,
            poly_stale_ms: 3_000,
            binance_stale_ms: 2_000,
            max_clock_drift_ms: 1_000,
        };
        let mut breakers = CircuitBreakers::new(config, MicroUsdc(10_000), 0);

        // Initially active at now_ns = 0
        assert_eq!(breakers.check_status(0, 0), BreakerStatus::ArmActive);

        // Advance 2.5s -> Binance feed is 2,500ms old (> 2,000ms stale threshold)
        assert_eq!(
            breakers.check_status(2_500_000_000, 0),
            BreakerStatus::Tripped {
                reason: BreakerReason::BinanceFeedStale,
            }
        );

        // Refresh Binance heartbeat -> active again
        breakers.record_binance_heartbeat(2_500_000_000);
        assert_eq!(
            breakers.check_status(2_500_000_000, 0),
            BreakerStatus::ArmActive
        );

        // Test 5 consecutive errors within 10s
        for _ in 0..4 {
            breakers.record_api_error(1_000);
            assert_eq!(
                breakers.check_status(2_500_000_000, 0),
                BreakerStatus::ArmActive
            );
        }
        breakers.record_api_error(1_500); // 5th error
        assert_eq!(
            breakers.check_status(2_500_000_000, 0),
            BreakerStatus::Tripped {
                reason: BreakerReason::ConsecutiveApiErrors,
            }
        );

        // Success resets counter
        breakers.record_api_success();
        assert_eq!(
            breakers.check_status(2_500_000_000, 0),
            BreakerStatus::ArmActive
        );
    }

    #[test]
    fn test_clock_drift_and_tick_change_freeze() {
        let config = BreakerConfig::default();
        let mut breakers = CircuitBreakers::new(config, MicroUsdc(10_000), 10_000_000_000);

        // Drift = 1,200ms (> 1,000ms threshold)
        assert_eq!(
            breakers.check_status(10_000_000_000, 1_200),
            BreakerStatus::Tripped {
                reason: BreakerReason::ClockDriftExceeded,
            }
        );

        // Normal drift = 50ms -> Active
        assert_eq!(
            breakers.check_status(10_000_000_000, 50),
            BreakerStatus::ArmActive
        );

        // Tick size change from 0.01 to 0.001
        breakers.on_tick_size_change(MicroUsdc(1_000));
        assert_eq!(
            breakers.check_status(10_000_000_000, 0),
            BreakerStatus::Tripped {
                reason: BreakerReason::TickSizeChangeFreeze,
            }
        );

        // Acknowledge sync -> Active again
        breakers.acknowledge_tick_size_sync();
        assert_eq!(
            breakers.check_status(10_000_000_000, 0),
            BreakerStatus::ArmActive
        );
    }
}
