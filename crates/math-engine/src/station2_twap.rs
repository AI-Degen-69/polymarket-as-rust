use core_types::{MathError, VarianceMode};
use serde::Serialize;

use crate::safe_clamp;

const SPOT_RING_CAP: usize = 1024;
const LEAD_LAG_BUCKET_MS: u64 = 50;
const LEAD_LAG_RING_CAP: usize = 128;
const MIN_VOL_SAMPLE_MS: u64 = 1_000;
const MAX_LEAD_LAG_WINDOW_MS: u64 = 6_000;
const MAX_LEAD_LAG_COOLDOWN_MS: u64 = 10_000;
const SECONDS_PER_YEAR: f64 = 31_536_000.0;

/// Single BTC spot reference observation buffered inside Station 2.
#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
pub struct SpotSample {
    /// Sample timestamp in Unix milliseconds.
    pub ts_ms: u64,
    /// BTC/USD spot price.
    pub price_usd: f64,
}

impl SpotSample {
    const EMPTY: Self = Self {
        ts_ms: 0,
        price_usd: 0.0,
    };
}

/// Configuration parameters for Station 2 rolling TWAP, residual variance, and Binance lead-lag.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Station2Config {
    /// Residual variance mode (`BinaryCara` default vs `LibraryStrict` A/B toggle).
    pub variance_mode: VarianceMode,
    /// Rolling TWAP & terminal lock-in horizon in milliseconds (`60_000` default).
    pub window_ms: u64,
    /// Contract epoch horizon in seconds (`300.0` default).
    pub epoch_secs: f64,
    /// Lower fraction floor applied in `BinaryCara` mode (`0.20` default).
    pub tau_floor_frac: f64,
    /// Literal Library initial epoch variance `sigma_0^2` (`0.0008` default).
    pub library_sigma0_sq: f64,
    /// EWMA decay coefficient for empirical binary mid-price variance (`0.05` default).
    pub ewma_mid_alpha: f64,
    /// EWMA decay coefficient for annualized Binance spot volatility (`0.10` default).
    pub spot_vol_ewma_alpha: f64,
    /// Prior annualized Binance volatility before ticks arrive (`0.60` default).
    pub default_sigma_binance: f64,
    /// Fair-probability jump `|delta_p_star|` threshold triggering defensive cancel (`0.02` default).
    pub lead_lag_cancel_threshold: f64,
    /// Rolling Binance lead-lag lookback window in ms (`0` = legacy tick-to-tick).
    pub lead_lag_window_ms: u64,
    /// Per-side defensive cancel cooldown duration in ms (`0` = legacy one-shot).
    pub lead_lag_cooldown_ms: u64,
    /// Cancel only the endangered side on lead-lag move (`false` = legacy symmetric cancel).
    pub lead_lag_directional: bool,
}

impl Default for Station2Config {
    fn default() -> Self {
        Self {
            variance_mode: VarianceMode::BinaryCara,
            window_ms: 60_000,
            epoch_secs: 300.0,
            tau_floor_frac: 0.20,
            library_sigma0_sq: 0.0008,
            ewma_mid_alpha: 0.05,
            spot_vol_ewma_alpha: 0.10,
            default_sigma_binance: 0.60,
            lead_lag_cancel_threshold: 0.02,
            lead_lag_window_ms: 0,
            lead_lag_cooldown_ms: 0,
            lead_lag_directional: false,
        }
    }
}

impl Station2Config {
    /// Validates finite positivity and interval invariants for Station 2 parameters.
    pub fn validate(&self) -> Result<(), MathError> {
        if self.window_ms == 0 {
            return Err(err_desc("station2 window_ms must be > 0"));
        }
        check_pos_finite(self.epoch_secs)?;
        check_range(self.tau_floor_frac, 0.001, 1.0, "tau_floor_frac")?;
        check_range(self.library_sigma0_sq, 0.0, 10.0, "library_sigma0_sq")?;
        check_range(self.ewma_mid_alpha, 1e-6, 1.0, "ewma_mid_alpha")?;
        check_range(self.spot_vol_ewma_alpha, 1e-6, 1.0, "spot_vol_ewma_alpha")?;
        check_range(
            self.default_sigma_binance,
            0.10,
            3.00,
            "default_sigma_binance",
        )?;
        check_pos_finite(self.lead_lag_cancel_threshold)?;
        if self.lead_lag_cancel_threshold >= 1.0 {
            return Err(err_desc("station2 lead_lag_cancel_threshold must be < 1.0"));
        }
        if self.lead_lag_window_ms > MAX_LEAD_LAG_WINDOW_MS {
            return Err(err_desc("station2 lead_lag_window_ms must be <= 6000"));
        }
        if self.lead_lag_window_ms > 0 && self.lead_lag_window_ms < LEAD_LAG_BUCKET_MS {
            return Err(err_desc("station2 lead_lag_window_ms must be 0 or >= 50ms"));
        }
        if self.lead_lag_cooldown_ms > MAX_LEAD_LAG_COOLDOWN_MS {
            return Err(err_desc("station2 lead_lag_cooldown_ms must be <= 10000"));
        }
        Ok(())
    }
}

/// Cross-venue lead-lag signal derived from real-time Binance BTC spot moves.
#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
pub struct LeadLagSignal {
    /// Black-Scholes binary risk-neutral probability `Phi(d_2)` if spot and strike exist.
    pub p_spot_star: Option<f64>,
    /// Change in `p_spot_star` over the configured lookback window (or previous tick).
    pub delta_p_star: f64,
    /// Annualized Binance realized EWMA volatility.
    pub sigma_binance: f64,
    /// `true` when either UP or DOWN defensive cancel / cooldown is active.
    pub trigger_defensive_cancel: bool,
    /// `true` when UP defensive cancel / cooldown is active (BTC dropped vs reference).
    pub trigger_defensive_cancel_up: bool,
    /// `true` when DOWN defensive cancel / cooldown is active (BTC rose vs reference).
    pub trigger_defensive_cancel_dn: bool,
}

/// Complete output of Station 2 for one evaluation step.
#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
pub struct Station2Output {
    /// Exact trailing 60-second ZOH time-weighted BTC average price in USD.
    pub twap_60s_usd: Option<f64>,
    /// Terminal `[T-60s, T]` Chainlink settlement lock-in TWAP estimate when active.
    pub terminal_twap_usd: Option<f64>,
    /// `true` when `now_ms >= expiry_ms - window_ms`.
    pub terminal_window_active: bool,
    /// Fraction of `[T-60s, T]` already locked in (`0.0..=1.0`).
    pub terminal_lock_fraction: f64,
    /// Non-negative remaining horizon `tau = (T - t)` in seconds.
    pub tau_secs: f64,
    /// Effective decayed variance reported for this mode.
    pub sigma_eff_sq: f64,
    /// Empirical EWMA mid-price variance per second (`USD^2/s`).
    pub sigma_ewma_sq: f64,
    /// Remaining variance `V_rem` consumed by Stations 4 and 5 (`USD^2`).
    pub v_rem: f64,
    /// Cross-venue Binance lead-lag signal.
    pub lead_lag: LeadLagSignal,
}

/// Zero-allocation Station 2 engine tracking 60s TWAP, variance decay, and Binance lead-lag.
#[derive(Debug, Clone, Copy)]
pub struct RollingTwapAndVariance {
    config: Station2Config,
    samples: [SpotSample; SPOT_RING_CAP],
    head: usize,
    len: usize,
    stored_area: f64,
    evictions_since_resync: u32,
    last_spot_usd: Option<f64>,
    last_spot_ts_ms: Option<u64>,
    last_vol_sample: Option<SpotSample>,
    sigma_binance: f64,
    ll_samples: [SpotSample; LEAD_LAG_RING_CAP],
    ll_head: usize,
    ll_len: usize,
    cooldown_until_up_ms: u64,
    cooldown_until_dn_ms: u64,
    prev_p_spot_star: Option<f64>,
    last_lead_lag: LeadLagSignal,
    last_strike_usd: Option<f64>,
    spot_dirty: bool,
    last_mid: Option<(u64, f64)>,
    sigma_ewma_sq: f64,
    term_expiry_ms: u64,
    term_last_ts_ms: u64,
    term_area: f64,
    term_total_ms: u64,
    term_valid: bool,
}

impl RollingTwapAndVariance {
    /// Creates an initialized Station 2 engine from validated configuration.
    pub fn new(config: Station2Config) -> Result<Self, MathError> {
        config.validate()?;
        let default_sigma = config.default_sigma_binance;
        Ok(Self {
            config,
            samples: [SpotSample::EMPTY; SPOT_RING_CAP],
            head: 0,
            len: 0,
            stored_area: 0.0,
            evictions_since_resync: 0,
            last_spot_usd: None,
            last_spot_ts_ms: None,
            last_vol_sample: None,
            sigma_binance: default_sigma,
            ll_samples: [SpotSample::EMPTY; LEAD_LAG_RING_CAP],
            ll_head: 0,
            ll_len: 0,
            cooldown_until_up_ms: 0,
            cooldown_until_dn_ms: 0,
            prev_p_spot_star: None,
            last_lead_lag: initial_lead_lag(default_sigma),
            last_strike_usd: None,
            spot_dirty: false,
            last_mid: None,
            sigma_ewma_sq: 0.0,
            term_expiry_ms: 0,
            term_last_ts_ms: 0,
            term_area: 0.0,
            term_total_ms: 0,
            term_valid: false,
        })
    }

    /// Resets rolling samples and volatility accumulators (mandatory on epoch rollover).
    pub const fn reset(&mut self) {
        self.samples = [SpotSample::EMPTY; SPOT_RING_CAP];
        self.head = 0;
        self.len = 0;
        self.stored_area = 0.0;
        self.evictions_since_resync = 0;
        self.last_spot_usd = None;
        self.last_spot_ts_ms = None;
        self.last_vol_sample = None;
        self.sigma_binance = self.config.default_sigma_binance;
        self.ll_samples = [SpotSample::EMPTY; LEAD_LAG_RING_CAP];
        self.ll_head = 0;
        self.ll_len = 0;
        self.cooldown_until_up_ms = 0;
        self.cooldown_until_dn_ms = 0;
        self.prev_p_spot_star = None;
        self.last_lead_lag = initial_lead_lag(self.config.default_sigma_binance);
        self.last_strike_usd = None;
        self.spot_dirty = false;
        self.last_mid = None;
        self.sigma_ewma_sq = 0.0;
        self.term_expiry_ms = 0;
        self.term_last_ts_ms = 0;
        self.term_area = 0.0;
        self.term_total_ms = 0;
        self.term_valid = false;
    }

    /// Dynamically updates contract epoch horizon (`300.0` for 5m, `900.0` for 15m, `3600.0` for 1h).
    pub fn set_epoch_secs(&mut self, epoch_secs: f64) -> Result<(), MathError> {
        check_pos_finite(epoch_secs)?;
        self.config.epoch_secs = epoch_secs;
        Ok(())
    }

    /// Returns count of samples currently held in the circular buffer.
    #[must_use]
    pub const fn sample_len(&self) -> usize {
        self.len
    }

    /// Latest recorded BTC/USD spot price if any.
    #[must_use]
    pub const fn last_spot_usd(&self) -> Option<f64> {
        self.last_spot_usd
    }

    /// Inserts one BTC spot sample, maintaining monotonic ZOH ordering in `[SpotSample; 1024]`.
    pub fn push_spot(&mut self, ts_ms: u64, price_usd: f64) -> Result<(), MathError> {
        let _ = self.push_spot_accepted(ts_ms, price_usd)?;
        Ok(())
    }

    /// Inserts one BTC spot sample, returning `true` if accepted or `false` if timestamp precedes `last`.
    pub fn push_spot_accepted(&mut self, ts_ms: u64, price_usd: f64) -> Result<bool, MathError> {
        check_pos_finite(price_usd)?;
        if let Some(last) = self.last_sample()
            && ts_ms < last.ts_ms
        {
            return Ok(false);
        }
        if self.term_valid && ts_ms <= self.term_last_ts_ms {
            self.term_valid = false;
        }
        if let Some(last) = self.last_sample()
            && ts_ms == last.ts_ms
        {
            self.overwrite_tail_price(price_usd);
            self.record_lead_lag_sample(ts_ms, price_usd);
            if let Some(ref mut lv) = self.last_vol_sample
                && lv.ts_ms == ts_ms
            {
                lv.price_usd = price_usd;
            }
            self.spot_dirty = true;
            return Ok(true);
        }
        self.maybe_update_spot_vol(ts_ms, price_usd);
        let cutoff = ts_ms.saturating_sub(self.config.window_ms);
        self.evict_expired_before(cutoff);
        if self.len >= SPOT_RING_CAP {
            self.coalesce_oldest_pair();
        }
        self.append_sample(SpotSample { ts_ms, price_usd });
        self.record_lead_lag_sample(ts_ms, price_usd);
        self.last_spot_ts_ms = Some(ts_ms);
        self.last_spot_usd = Some(price_usd);
        self.spot_dirty = true;
        Ok(true)
    }

    /// Returns passive lead-lag snapshot without consuming or mutating jump triggers.
    #[must_use]
    pub const fn peek_lead_lag(&self) -> LeadLagSignal {
        LeadLagSignal {
            p_spot_star: self.last_lead_lag.p_spot_star,
            delta_p_star: 0.0,
            sigma_binance: self.sigma_binance,
            trigger_defensive_cancel: false,
            trigger_defensive_cancel_up: false,
            trigger_defensive_cancel_dn: false,
        }
    }

    /// Returns `true` when UP-leg defensive cancel cooldown is active at `now_ms`.
    #[must_use]
    pub const fn is_cooldown_up_active(&self, now_ms: u64) -> bool {
        self.config.lead_lag_cooldown_ms > 0 && now_ms < self.cooldown_until_up_ms
    }

    /// Returns `true` when DOWN-leg defensive cancel cooldown is active at `now_ms`.
    #[must_use]
    pub const fn is_cooldown_dn_active(&self, now_ms: u64) -> bool {
        self.config.lead_lag_cooldown_ms > 0 && now_ms < self.cooldown_until_dn_ms
    }

    /// Clears one-shot jump delta/cancel latch after a skipped/suppressed pipeline evaluation.
    pub const fn clear_one_shot_lead_lag(&mut self) {
        self.last_lead_lag.delta_p_star = 0.0;
        self.last_lead_lag.trigger_defensive_cancel = false;
        self.last_lead_lag.trigger_defensive_cancel_up = false;
        self.last_lead_lag.trigger_defensive_cancel_dn = false;
    }

    /// Updates empirical EWMA binary mid-price variance rate (`USD^2/s`).
    pub fn update_mid(&mut self, ts_ms: u64, mid_usd: f64) -> Result<(), MathError> {
        check_pos_finite(mid_usd)?;
        if mid_usd > 1.0 {
            return Err(MathError::OutOfRangePrice { value: mid_usd });
        }
        if let Some((prev_ts, prev_mid)) = self.last_mid {
            if ts_ms > prev_ts {
                let dt_secs = (ts_ms - prev_ts) as f64 / 1000.0;
                let diff = mid_usd - prev_mid;
                let inst_var = (diff * diff / dt_secs).clamp(0.0, 1.0);
                let alpha = self.config.ewma_mid_alpha;
                self.sigma_ewma_sq = (1.0 - alpha) * self.sigma_ewma_sq + alpha * inst_var;
                self.last_mid = Some((ts_ms, mid_usd));
            } else if ts_ms == prev_ts {
                self.last_mid = Some((ts_ms, mid_usd));
            }
        } else {
            self.last_mid = Some((ts_ms, mid_usd));
        }
        Ok(())
    }

    /// Computes exact ZOH time-weighted average over `[max(start_ms, first.ts_ms), end_ms]`.
    #[must_use]
    pub fn twap_over_window(&self, start_ms: u64, end_ms: u64) -> Option<f64> {
        let first = self.sample_at(0)?;
        if first.ts_ms > end_ms {
            return None;
        }
        let eff_start = start_ms.max(first.ts_ms);
        let mut weighted = 0.0_f64;
        let mut total_dt = 0.0_f64;
        for idx in 0..self.len {
            let Some(cur) = self.sample_at(idx) else {
                continue;
            };
            let next_ts = self.sample_at(idx + 1).map_or(end_ms, |s| s.ts_ms);
            let seg_start = cur.ts_ms.max(eff_start);
            let seg_end = next_ts.min(end_ms);
            if seg_end > seg_start {
                let dt = (seg_end - seg_start) as f64;
                weighted += cur.price_usd * dt;
                total_dt += dt;
            }
        }
        if total_dt > 0.0 {
            Some(weighted / total_dt)
        } else {
            self.last_active_price_at(end_ms)
        }
    }

    /// Computes Binance lead-lag fair probability `Phi(d_2)` and jump detector using the last spot timestamp.
    pub fn compute_lead_lag(
        &mut self,
        spot_usd: f64,
        strike_usd: Option<f64>,
        tau_secs: f64,
    ) -> Result<LeadLagSignal, MathError> {
        let ts_ms = self.last_spot_ts_ms.unwrap_or(0);
        self.compute_lead_lag_at(ts_ms, spot_usd, strike_usd, tau_secs)
    }

    /// Computes rolling-window or tick-to-tick Binance lead-lag fair probability and directional cancels at `ts_ms`.
    pub fn compute_lead_lag_at(
        &mut self,
        ts_ms: u64,
        spot_usd: f64,
        strike_usd: Option<f64>,
        tau_secs: f64,
    ) -> Result<LeadLagSignal, MathError> {
        check_pos_finite(spot_usd)?;
        if !tau_secs.is_finite() || tau_secs < 0.0 {
            return Err(err_desc("tau_secs must be finite and >= 0.0"));
        }
        let sigma = self.sigma_binance;
        let Some(strike) = strike_usd else {
            let sig = initial_lead_lag(sigma);
            self.last_lead_lag = sig;
            self.last_strike_usd = None;
            self.spot_dirty = false;
            return Ok(sig);
        };
        check_pos_finite(strike)?;
        let p_spot_star = binary_spot_prob(spot_usd, strike, sigma, tau_secs);
        let delta_p_star = if self.config.lead_lag_window_ms > 0 {
            if let Some(ref_spot) = self.lead_lag_ref_spot(ts_ms, self.config.lead_lag_window_ms) {
                let p_ref = binary_spot_prob(ref_spot, strike, sigma, tau_secs);
                p_spot_star - p_ref
            } else {
                self.prev_p_spot_star.map_or(0.0, |p| p_spot_star - p)
            }
        } else {
            self.prev_p_spot_star.map_or(0.0, |p| p_spot_star - p)
        };
        self.prev_p_spot_star = Some(p_spot_star);

        let thresh = self.config.lead_lag_cancel_threshold;
        let mut raw_up = delta_p_star <= -thresh;
        let mut raw_dn = delta_p_star >= thresh;
        if !self.config.lead_lag_directional && (raw_up || raw_dn) {
            raw_up = true;
            raw_dn = true;
        }
        if self.config.lead_lag_cooldown_ms > 0 {
            let until = ts_ms.saturating_add(self.config.lead_lag_cooldown_ms);
            if raw_up {
                self.cooldown_until_up_ms = self.cooldown_until_up_ms.max(until);
            }
            if raw_dn {
                self.cooldown_until_dn_ms = self.cooldown_until_dn_ms.max(until);
            }
        }
        let trigger_up = raw_up || self.is_cooldown_up_active(ts_ms);
        let trigger_dn = raw_dn || self.is_cooldown_dn_active(ts_ms);
        let sig = LeadLagSignal {
            p_spot_star: Some(p_spot_star),
            delta_p_star,
            sigma_binance: sigma,
            trigger_defensive_cancel: trigger_up || trigger_dn,
            trigger_defensive_cancel_up: trigger_up,
            trigger_defensive_cancel_dn: trigger_dn,
        };
        self.last_lead_lag = sig;
        self.last_strike_usd = Some(strike);
        self.spot_dirty = false;
        Ok(sig)
    }

    /// Evaluates Station 2 outputs (`twap_60s`, terminal lock-in, `tau_secs`, `v_rem`, lead-lag).
    pub fn evaluate(
        &mut self,
        now_ms: u64,
        expiry_ms: u64,
        p_mid: f64,
        strike_usd: Option<f64>,
    ) -> Result<Station2Output, MathError> {
        if !p_mid.is_finite() {
            return Err(MathError::NonFiniteInput { value: p_mid });
        }
        if !(0.0..=1.0).contains(&p_mid) {
            return Err(MathError::OutOfRangePrice { value: p_mid });
        }
        let tau_secs = expiry_ms.saturating_sub(now_ms) as f64 / 1000.0;
        let (sigma_eff_sq, v_rem) = self.compute_variance(tau_secs, p_mid)?;
        // Evaluate terminal tracker before post-expiry eviction if not yet locked at `expiry_ms`.
        let (terminal_window_active, terminal_lock_fraction, terminal_twap_usd) =
            self.update_terminal_tracker(now_ms, expiry_ms);
        let twap_60s_usd = self.twap_60s_fast(now_ms);
        let lead_lag = self.resolve_lead_lag(now_ms, strike_usd, tau_secs)?;

        Ok(Station2Output {
            twap_60s_usd,
            terminal_twap_usd,
            terminal_window_active,
            terminal_lock_fraction,
            tau_secs,
            sigma_eff_sq,
            sigma_ewma_sq: self.sigma_ewma_sq,
            v_rem,
            lead_lag,
        })
    }

    /// Computes `[expiry_ms - window_ms, expiry_ms]` terminal TWAP when available,
    /// falling back to the trailing `window_ms` TWAP at `now_ms`.
    #[must_use]
    pub fn settlement_twap_usd(&mut self, now_ms: u64, expiry_ms: u64) -> Option<f64> {
        let (_, _, terminal_twap) = self.update_terminal_tracker(now_ms, expiry_ms);
        terminal_twap.or_else(|| self.twap_60s_fast(now_ms))
    }

    /// Computes trailing 60-second ZOH TWAP in O(1) using incremental `stored_area`.
    #[must_use]
    pub fn twap_60s_fast(&mut self, now_ms: u64) -> Option<f64> {
        let cutoff = now_ms.saturating_sub(self.config.window_ms);
        let first = self.sample_at(0)?;
        if first.ts_ms > now_ms {
            return None;
        }
        let last = self.last_sample()?;
        if last.ts_ms > now_ms {
            return self.twap_over_window(cutoff, now_ms);
        }
        self.evict_expired_before(cutoff);
        let first = self.sample_at(0)?;
        let eff_start = cutoff.max(first.ts_ms);
        let pre_trim = if first.ts_ms < cutoff {
            first.price_usd * ((cutoff - first.ts_ms) as f64)
        } else {
            0.0
        };
        let tail = last.price_usd * (now_ms.saturating_sub(last.ts_ms) as f64);
        let area = (self.stored_area - pre_trim) + tail;
        let total_dt = now_ms.saturating_sub(eff_start) as f64;
        if total_dt > 0.0 {
            Some(area / total_dt)
        } else {
            Some(last.price_usd)
        }
    }

    fn resolve_lead_lag(
        &mut self,
        now_ms: u64,
        strike_usd: Option<f64>,
        tau_secs: f64,
    ) -> Result<LeadLagSignal, MathError> {
        if let Some(spot) = self.last_spot_usd
            && (self.spot_dirty
                || self.last_strike_usd != strike_usd
                || (self.config.lead_lag_window_ms > 0 && strike_usd.is_some()))
        {
            let sig = self.compute_lead_lag_at(now_ms, spot, strike_usd, tau_secs)?;
            self.clear_one_shot_lead_lag();
            return Ok(sig);
        }
        let mut sig = self.last_lead_lag;
        sig.sigma_binance = self.sigma_binance;
        sig.trigger_defensive_cancel_up |= self.is_cooldown_up_active(now_ms);
        sig.trigger_defensive_cancel_dn |= self.is_cooldown_dn_active(now_ms);
        sig.trigger_defensive_cancel =
            sig.trigger_defensive_cancel_up || sig.trigger_defensive_cancel_dn;
        self.clear_one_shot_lead_lag();
        Ok(sig)
    }

    fn ll_slot_idx(&self, logical: usize) -> usize {
        (self.ll_head + logical) % LEAD_LAG_RING_CAP
    }

    fn ll_sample_at(&self, logical: usize) -> Option<SpotSample> {
        if logical >= self.ll_len {
            return None;
        }
        self.ll_samples.get(self.ll_slot_idx(logical)).copied()
    }

    fn ll_sample_mut(&mut self, logical: usize) -> Option<&mut SpotSample> {
        if logical >= self.ll_len {
            return None;
        }
        let idx = self.ll_slot_idx(logical);
        self.ll_samples.get_mut(idx)
    }

    fn record_lead_lag_sample(&mut self, ts_ms: u64, price_usd: f64) {
        if let Some(last_idx) = self.ll_len.checked_sub(1)
            && let Some(tail) = self.ll_sample_mut(last_idx)
            && (tail.ts_ms / LEAD_LAG_BUCKET_MS == ts_ms / LEAD_LAG_BUCKET_MS
                || ts_ms <= tail.ts_ms)
        {
            tail.ts_ms = tail.ts_ms.max(ts_ms);
            tail.price_usd = price_usd;
            return;
        }
        if self.ll_len == LEAD_LAG_RING_CAP {
            self.ll_head = (self.ll_head + 1) % LEAD_LAG_RING_CAP;
            self.ll_len = self.ll_len.saturating_sub(1);
        }
        let idx = self.ll_slot_idx(self.ll_len);
        if let Some(slot) = self.ll_samples.get_mut(idx) {
            *slot = SpotSample { ts_ms, price_usd };
            self.ll_len = self.ll_len.saturating_add(1);
        }
    }

    fn lead_lag_ref_spot(&self, now_ms: u64, window_ms: u64) -> Option<f64> {
        if self.ll_len == 0 {
            return None;
        }
        let ref_ts = now_ms.saturating_sub(window_ms);
        for rev in 0..self.ll_len {
            let idx = self.ll_len - 1 - rev;
            if let Some(s) = self.ll_sample_at(idx)
                && s.ts_ms <= ref_ts
            {
                return Some(s.price_usd);
            }
        }
        self.ll_sample_at(0).map(|s| s.price_usd)
    }

    fn compute_variance(&self, tau_secs: f64, p_mid: f64) -> Result<(f64, f64), MathError> {
        let tau_frac = safe_clamp(tau_secs / self.config.epoch_secs, 0.0, 1.0)?;
        match self.config.variance_mode {
            VarianceMode::BinaryCara => {
                let cara = p_mid * (1.0 - p_mid) * tau_frac.max(self.config.tau_floor_frac);
                let v_rem = cara.max(self.sigma_ewma_sq * tau_secs);
                let sigma_eff_sq = if tau_secs > 0.0 {
                    v_rem / tau_secs
                } else {
                    0.0
                };
                Ok((sigma_eff_sq, v_rem))
            }
            VarianceMode::LibraryStrict => {
                let sigma_eff_sq = self.config.library_sigma0_sq * tau_frac.powi(2);
                let v_rem = sigma_eff_sq * tau_frac;
                Ok((sigma_eff_sq, v_rem))
            }
        }
    }

    fn update_terminal_tracker(&mut self, now_ms: u64, expiry_ms: u64) -> (bool, f64, Option<f64>) {
        let term_start = expiry_ms.saturating_sub(self.config.window_ms);
        if now_ms < term_start {
            return (false, 0.0, None);
        }
        if self.term_valid && self.term_expiry_ms == expiry_ms && self.term_last_ts_ms == expiry_ms
        {
            let twap = (self.term_total_ms > 0).then(|| self.term_area / self.term_total_ms as f64);
            return (true, 1.0, twap);
        }
        let span = expiry_ms.saturating_sub(term_start);
        let t_end = now_ms.min(expiry_ms);
        let frac = if span == 0 {
            1.0
        } else {
            ((t_end - term_start) as f64 / span as f64).clamp(0.0, 1.0)
        };
        let last_ts = self.last_sample().map_or(0, |s| s.ts_ms);
        let can_step = self.term_valid
            && self.term_expiry_ms == expiry_ms
            && t_end >= self.term_last_ts_ms
            && last_ts <= t_end;
        let twap = if can_step {
            self.advance_terminal_area(t_end)
        } else {
            self.init_terminal_area(term_start, t_end, expiry_ms)
        };
        (true, frac, twap)
    }

    fn init_terminal_area(&mut self, term_start: u64, t_end: u64, expiry_ms: u64) -> Option<f64> {
        let Some((area, total_ms)) = self.integrate_area_and_dt(term_start, t_end) else {
            self.term_valid = false;
            return self.last_active_price_at(t_end);
        };
        self.term_expiry_ms = expiry_ms;
        self.term_last_ts_ms = t_end;
        self.term_area = area;
        self.term_total_ms = total_ms;
        self.term_valid = true;
        if total_ms > 0 {
            Some(area / total_ms as f64)
        } else {
            self.last_active_price_at(t_end)
        }
    }

    fn advance_terminal_area(&mut self, t_end: u64) -> Option<f64> {
        if t_end > self.term_last_ts_ms
            && let Some((add_area, add_ms)) =
                self.integrate_area_and_dt(self.term_last_ts_ms, t_end)
        {
            self.term_area += add_area;
            self.term_total_ms = self.term_total_ms.saturating_add(add_ms);
            self.term_last_ts_ms = t_end;
        }
        if self.term_total_ms > 0 {
            Some(self.term_area / self.term_total_ms as f64)
        } else {
            self.last_active_price_at(t_end)
        }
    }

    fn integrate_area_and_dt(&self, start_ms: u64, end_ms: u64) -> Option<(f64, u64)> {
        let first = self.sample_at(0)?;
        if first.ts_ms > end_ms || end_ms <= start_ms {
            return None;
        }
        let eff_start = start_ms.max(first.ts_ms);
        let mut area = 0.0_f64;
        let mut total_ms = 0_u64;
        let mut right_ts = end_ms;
        for rev in 0..self.len {
            let idx = self.len - 1 - rev;
            let cur = self.sample_at(idx)?;
            if cur.ts_ms < end_ms {
                let seg_start = cur.ts_ms.max(eff_start);
                let seg_end = right_ts.min(end_ms);
                if seg_end > seg_start {
                    let dt = seg_end - seg_start;
                    area += cur.price_usd * (dt as f64);
                    total_ms = total_ms.saturating_add(dt);
                }
            }
            right_ts = cur.ts_ms;
            if cur.ts_ms <= eff_start {
                break;
            }
        }
        Some((area, total_ms))
    }

    fn slot_idx(&self, logical: usize) -> usize {
        (self.head + logical) % SPOT_RING_CAP
    }

    fn sample_at(&self, logical: usize) -> Option<SpotSample> {
        if logical >= self.len {
            return None;
        }
        self.samples.get(self.slot_idx(logical)).copied()
    }

    fn sample_mut(&mut self, logical: usize) -> Option<&mut SpotSample> {
        if logical >= self.len {
            return None;
        }
        let idx = self.slot_idx(logical);
        self.samples.get_mut(idx)
    }

    fn last_sample(&self) -> Option<SpotSample> {
        self.len.checked_sub(1).and_then(|i| self.sample_at(i))
    }

    fn last_active_price_at(&self, end_ms: u64) -> Option<f64> {
        let mut latest = None;
        for idx in 0..self.len {
            if let Some(s) = self.sample_at(idx)
                && s.ts_ms <= end_ms
            {
                latest = Some(s.price_usd);
            }
        }
        latest
    }

    fn overwrite_tail_price(&mut self, price_usd: f64) {
        if let Some(last_idx) = self.len.checked_sub(1)
            && let Some(slot) = self.sample_mut(last_idx)
        {
            slot.price_usd = price_usd;
        }
        self.last_spot_usd = Some(price_usd);
    }

    fn maybe_update_spot_vol(&mut self, ts_ms: u64, price_usd: f64) {
        let Some(prev_vol) = self.last_vol_sample else {
            self.last_vol_sample = Some(SpotSample { ts_ms, price_usd });
            return;
        };
        if ts_ms >= prev_vol.ts_ms.saturating_add(MIN_VOL_SAMPLE_MS) {
            self.update_spot_vol(prev_vol, ts_ms, price_usd);
            self.last_vol_sample = Some(SpotSample { ts_ms, price_usd });
        }
    }

    fn update_spot_vol(&mut self, prev: SpotSample, ts_ms: u64, price_usd: f64) {
        let dt_secs = (ts_ms - prev.ts_ms) as f64 / 1000.0;
        if dt_secs <= 0.0 || prev.price_usd <= 0.0 {
            return;
        }
        let ret = (price_usd / prev.price_usd).ln();
        let dt_yr = dt_secs / SECONDS_PER_YEAR;
        let inst_var = (ret * ret / dt_yr).clamp(0.01, 9.0);
        let alpha = self.config.spot_vol_ewma_alpha;
        let prev_var = self.sigma_binance * self.sigma_binance;
        self.sigma_binance = ((1.0 - alpha) * prev_var + alpha * inst_var)
            .sqrt()
            .clamp(0.10, 3.00);
    }

    fn evict_expired_before(&mut self, cutoff_ms: u64) {
        let mut popped_any = false;
        while self.len >= 2 {
            let (Some(first), Some(second)) = (self.sample_at(0), self.sample_at(1)) else {
                break;
            };
            if second.ts_ms > cutoff_ms {
                break;
            }
            let dt = second.ts_ms.saturating_sub(first.ts_ms) as f64;
            self.stored_area = (self.stored_area - first.price_usd * dt).max(0.0);
            self.head = (self.head + 1) % SPOT_RING_CAP;
            self.len = self.len.saturating_sub(1);
            self.evictions_since_resync = self.evictions_since_resync.saturating_add(1);
            popped_any = true;
        }
        if self.len <= 1 {
            self.stored_area = 0.0;
        } else if popped_any && self.evictions_since_resync >= 4_096 {
            self.resync_stored_area();
            self.evictions_since_resync = 0;
        }
    }

    fn resync_stored_area(&mut self) {
        let mut area = 0.0_f64;
        for idx in 0..self.len.saturating_sub(1) {
            if let (Some(cur), Some(next)) = (self.sample_at(idx), self.sample_at(idx + 1)) {
                area += cur.price_usd * (next.ts_ms.saturating_sub(cur.ts_ms) as f64);
            }
        }
        self.stored_area = area;
    }

    fn coalesce_oldest_pair(&mut self) {
        if self.len < 3 {
            return;
        }
        let (Some(s0), Some(s1), Some(s2)) =
            (self.sample_at(0), self.sample_at(1), self.sample_at(2))
        else {
            return;
        };
        let dt0 = s1.ts_ms.saturating_sub(s0.ts_ms) as f64;
        let dt1 = s2.ts_ms.saturating_sub(s1.ts_ms) as f64;
        let total = dt0 + dt1;
        let merged_px = if total > 0.0 {
            (s0.price_usd * dt0 + s1.price_usd * dt1) / total
        } else {
            s1.price_usd
        };
        if let Some(slot1) = self.sample_mut(1) {
            *slot1 = SpotSample {
                ts_ms: s0.ts_ms,
                price_usd: merged_px,
            };
        }
        self.head = (self.head + 1) % SPOT_RING_CAP;
        self.len = self.len.saturating_sub(1);
    }

    fn append_sample(&mut self, sample: SpotSample) {
        if self.len >= SPOT_RING_CAP {
            return;
        }
        if let Some(prev) = self.last_sample() {
            let dt = sample.ts_ms.saturating_sub(prev.ts_ms) as f64;
            self.stored_area += prev.price_usd * dt;
        }
        let idx = self.slot_idx(self.len);
        if let Some(slot) = self.samples.get_mut(idx) {
            *slot = sample;
            self.len = self.len.saturating_add(1);
        }
    }
}

/// Standard normal probability density function `phi(x)`.
#[must_use]
pub fn norm_pdf(x: f64) -> f64 {
    const INV_SQRT_2PI: f64 = 0.398_942_280_401_432_7;
    INV_SQRT_2PI * (-0.5 * x * x).exp()
}

/// Standard normal cumulative distribution `Phi(x)` (Abramowitz & Stegun 7.1.26, `|err| < 1.5e-7`).
#[must_use]
pub fn norm_cdf(x: f64) -> f64 {
    if x == 0.0 {
        return 0.5;
    }
    const A1: f64 = 0.254_829_592;
    const A2: f64 = -0.284_496_736;
    const A3: f64 = 1.421_413_741;
    const A4: f64 = -1.453_152_027;
    const A5: f64 = 1.061_405_429;
    const P: f64 = 0.327_591_1;
    let sign = if x < 0.0 { -1.0 } else { 1.0 };
    let z = x.abs() * std::f64::consts::FRAC_1_SQRT_2;
    let t = 1.0 / (1.0 + P * z);
    let poly = ((((A5 * t + A4) * t + A3) * t + A2) * t + A1) * t;
    let erf = 1.0 - poly * (-z * z).exp();
    0.5 * (1.0 + sign * erf)
}

fn binary_spot_prob(spot_usd: f64, strike_usd: f64, sigma: f64, tau_secs: f64) -> f64 {
    let tau_yr = tau_secs / SECONDS_PER_YEAR;
    let vol_sqrt_t = sigma * tau_yr.sqrt();
    if tau_secs <= 0.0 || vol_sqrt_t < 1e-12 {
        return if spot_usd >= strike_usd { 1.0 } else { 0.0 };
    }
    let d2 = ((spot_usd / strike_usd).ln() - 0.5 * sigma * sigma * tau_yr) / vol_sqrt_t;
    norm_cdf(d2)
}

const fn initial_lead_lag(sigma: f64) -> LeadLagSignal {
    LeadLagSignal {
        p_spot_star: None,
        delta_p_star: 0.0,
        sigma_binance: sigma,
        trigger_defensive_cancel: false,
        trigger_defensive_cancel_up: false,
        trigger_defensive_cancel_dn: false,
    }
}

fn check_pos_finite(v: f64) -> Result<(), MathError> {
    if !v.is_finite() {
        return Err(MathError::NonFiniteInput { value: v });
    }
    if v <= 0.0 {
        return Err(MathError::NonPositiveFinite { value: v });
    }
    Ok(())
}

fn check_range(v: f64, lo: f64, hi: f64, field: &'static str) -> Result<(), MathError> {
    if !v.is_finite() {
        return Err(MathError::NonFiniteInput { value: v });
    }
    if v < lo || v > hi {
        return Err(MathError::InvalidMarketDescriptor {
            detail: format!("station2 `{field}`={v} must lie in [{lo}, {hi}]"),
        });
    }
    Ok(())
}

fn err_desc(msg: &str) -> MathError {
    MathError::InvalidMarketDescriptor {
        detail: msg.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_close(actual: f64, expected: f64, tol: f64) {
        assert!(
            (actual - expected).abs() <= tol,
            "actual={actual} expected={expected} diff={}",
            (actual - expected).abs()
        );
    }

    #[test]
    fn vector2_rolling_60s_twap_and_variance_modes_exact() {
        let mut cara = RollingTwapAndVariance::new(Station2Config::default()).unwrap();
        cara.push_spot(0, 90_000.0).unwrap();
        cara.push_spot(20_000, 90_300.0).unwrap();

        let out_mid = cara.evaluate(60_000, 210_000, 0.50, None).unwrap();
        assert_close(out_mid.twap_60s_usd.unwrap(), 90_200.0, 1e-9);
        assert_close(out_mid.tau_secs, 150.0, 1e-9);
        assert_close(out_mid.v_rem, 0.125, 1e-9);
        assert!(!out_mid.terminal_window_active);

        let out_near = cara.evaluate(180_000, 210_000, 0.50, None).unwrap();
        assert_close(out_near.tau_secs, 30.0, 1e-9);
        assert_close(out_near.v_rem, 0.25 * 0.20, 1e-9);
        assert!(out_near.terminal_window_active);
        assert_close(out_near.terminal_lock_fraction, 0.5, 1e-9);

        let strict_cfg = Station2Config {
            variance_mode: VarianceMode::LibraryStrict,
            ..Station2Config::default()
        };
        let mut strict = RollingTwapAndVariance::new(strict_cfg).unwrap();
        strict.push_spot(0, 90_000.0).unwrap();
        let strict_out = strict.evaluate(150_000, 300_000, 0.50, None).unwrap();
        assert_close(strict_out.sigma_eff_sq, 0.0002, 1e-12);
        assert_close(strict_out.v_rem, 0.0001, 1e-12);
    }

    #[test]
    fn adversarial_near_expiry_clock_overshoot_lead_lag_and_coalesce() {
        assert_close(norm_cdf(0.0), 0.5, 1e-12);
        assert_close(norm_cdf(1.959_963_984_540_054), 0.975, 2e-7);
        assert_close(norm_pdf(0.0), 0.398_942_280_4, 1e-9);

        let mut st2 = RollingTwapAndVariance::new(Station2Config::default()).unwrap();
        st2.push_spot(1_000, 90_000.0).unwrap();

        // Near expiry tau=1s, tau=0s, and overshoot now_ms > expiry_ms must never NaN or divide by zero.
        let s_1s = st2
            .evaluate(299_000, 300_000, 0.50, Some(90_000.0))
            .unwrap();
        assert_close(s_1s.tau_secs, 1.0, 1e-9);
        assert!(s_1s.v_rem.is_finite() && s_1s.v_rem > 0.0);
        assert!(s_1s.lead_lag.p_spot_star.unwrap().is_finite());

        let s_0s = st2
            .evaluate(300_000, 300_000, 0.50, Some(89_900.0))
            .unwrap();
        assert_eq!(s_0s.tau_secs, 0.0);
        assert_eq!(s_0s.lead_lag.p_spot_star, Some(1.0));

        let s_over = st2
            .evaluate(305_000, 300_000, 0.50, Some(90_100.0))
            .unwrap();
        assert_eq!(s_over.tau_secs, 0.0);
        assert_eq!(s_over.lead_lag.p_spot_star, Some(0.0));

        // Lead-lag jump triggers defensive cancel immediately.
        let mut ll = RollingTwapAndVariance::new(Station2Config::default()).unwrap();
        let sig1 = ll
            .compute_lead_lag(90_000.0, Some(90_000.0), 150.0)
            .unwrap();
        assert!(!sig1.trigger_defensive_cancel);
        let sig2 = ll
            .compute_lead_lag(90_200.0, Some(90_000.0), 150.0)
            .unwrap();
        assert!(sig2.delta_p_star > 0.05);
        assert!(sig2.trigger_defensive_cancel);

        // High-rate >1500 samples within 60s preserves ZOH TWAP within 1e-6 via interval coalescing.
        let mut burst = RollingTwapAndVariance::new(Station2Config::default()).unwrap();
        let mut expected_weighted = 0.0_f64;
        for i in 0_u64..1_550 {
            let ts = i * 38; // 1550 samples in 0..58,862 ms
            let px = 90_000.0 + (i as f64) * 0.25;
            burst.push_spot(ts, px).unwrap();
            let seg_end = if i + 1 < 1_550 { (i + 1) * 38 } else { 60_000 };
            expected_weighted += px * ((seg_end - ts) as f64);
        }
        assert!(burst.sample_len() <= SPOT_RING_CAP);
        let twap = burst.twap_over_window(0, 60_000).unwrap();
        assert_close(twap, expected_weighted / 60_000.0, 1e-6);
        let twap_fast = burst.twap_60s_fast(60_000).unwrap();
        assert_close(twap_fast, twap, 1e-9);

        // Incremental terminal tracker across eviction + rollover matches twap_over_window within 1e-9.
        let mut term_test = RollingTwapAndVariance::new(Station2Config::default()).unwrap();
        for step in 0_u64..150 {
            let ts = 220_000 + step * 500; // 220s..294.5s across 240s term_start
            let px = 90_000.0 + (step as f64) * 1.25;
            term_test.push_spot(ts, px).unwrap();
            let out = term_test.evaluate(ts + 100, 300_000, 0.50, None).unwrap();
            let exact_60s = term_test
                .twap_over_window((ts + 100).saturating_sub(60_000), ts + 100)
                .unwrap();
            assert_close(out.twap_60s_usd.unwrap(), exact_60s, 1e-9);
            if ts + 100 >= 240_000 {
                let exact_term = term_test.twap_over_window(240_000, ts + 100).unwrap();
                assert_close(out.terminal_twap_usd.unwrap(), exact_term, 1e-9);
            }
        }

        // Out-of-order past timestamp ignored cleanly; non-finite rejected.
        burst.push_spot(100, 1.0).unwrap();
        assert!(burst.last_spot_usd().unwrap() > 80_000.0);
        assert!(burst.push_spot(70_000, f64::NAN).is_err());
        assert!(burst.push_spot(70_000, 0.0).is_err());

        // One-shot lead-lag never stays latched across consecutive silent evaluate() ticks.
        ll.push_spot(2_000, 90_000.0).unwrap();
        let _ = ll.evaluate(2_100, 300_000, 0.50, Some(90_000.0)).unwrap();
        ll.push_spot(3_000, 90_250.0).unwrap();
        let ev1 = ll.evaluate(3_100, 300_000, 0.50, Some(90_000.0)).unwrap();
        assert!(ev1.lead_lag.trigger_defensive_cancel);
        let ev2 = ll.evaluate(3_200, 300_000, 0.50, Some(90_000.0)).unwrap();
        assert!(!ev2.lead_lag.trigger_defensive_cancel);
        assert_eq!(ev2.lead_lag.delta_p_star, 0.0);

        // Post-expiry terminal TWAP remains locked even under late or rejected OOO spots.
        let mut locked = RollingTwapAndVariance::new(Station2Config::default()).unwrap();
        locked.push_spot(240_000, 90_000.0).unwrap();
        locked.push_spot(270_000, 90_200.0).unwrap();
        let at_exp = locked.evaluate(300_000, 300_000, 0.50, None).unwrap();
        assert_close(at_exp.terminal_twap_usd.unwrap(), 90_100.0, 1e-9);
        assert!(!locked.push_spot_accepted(250_000, 80_000.0).unwrap());
        let after_ooo = locked.evaluate(330_200, 300_000, 0.50, None).unwrap();
        assert_close(after_ooo.terminal_twap_usd.unwrap(), 90_100.0, 1e-9);
        locked.push_spot(330_000, 95_000.0).unwrap();
        let after_exp = locked.evaluate(330_100, 300_000, 0.50, None).unwrap();
        assert_close(after_exp.terminal_twap_usd.unwrap(), 90_100.0, 1e-9);
    }

    #[test]
    fn subsecond_bookticker_spam_preserves_1s_sigma_binance() {
        let mut st2 = RollingTwapAndVariance::new(Station2Config::default()).unwrap();
        let t0 = 10_000_u64;
        // 199 duplicate BookTicker ticks within 995ms must leave initial 0.60 untouched.
        for i in 0..200_u64 {
            st2.push_spot(t0 + i * 5, 90_000.0).unwrap();
        }
        assert_close(st2.peek_lead_lag().sigma_binance, 0.60, 1e-12);

        // Interleave realistic 1s spot changes (+/- $25/s) with 5ms zero-change spam.
        let mut spot = 90_000.0;
        for sec in 1..=10_u64 {
            spot += if sec % 2 == 1 { 25.0 } else { -25.0 };
            for sub in 0..200_u64 {
                st2.push_spot(t0 + sec * 1_000 + sub * 5, spot).unwrap();
            }
        }
        let sig = st2.peek_lead_lag().sigma_binance;
        assert!(
            sig > 0.25,
            "expected realistic >0.25 1s sigma despite 5ms flat spam, got {sig}"
        );
    }

    #[test]
    fn rolling_directional_lead_lag_drop_rally_theta_and_symmetric() {
        let dir_cfg = Station2Config {
            lead_lag_cancel_threshold: 0.02,
            lead_lag_window_ms: 1_000,
            lead_lag_cooldown_ms: 500,
            lead_lag_directional: true,
            ..Station2Config::default()
        };
        let mut st2 = RollingTwapAndVariance::new(dir_cfg).unwrap();
        let strike = Some(90_000.0);
        let exp = 300_000_u64;

        // Baseline at t=10_000..11_000
        for step in 0..=10_u64 {
            st2.push_spot(10_000 + step * 100, 90_000.0).unwrap();
        }
        let calm = st2.evaluate(11_000, exp, 0.50, strike).unwrap();
        assert!(!calm.lead_lag.trigger_defensive_cancel);

        // Multi-tick gradual drop (-$18 every 100ms = -$180 over 1s): no single tick >= 2c, rolling is.
        for step in 1..=10_u64 {
            let t = 11_000 + step * 100;
            let px = 90_000.0 - (step as f64) * 18.0;
            st2.push_spot(t, px).unwrap();
        }
        let ev_drop = st2.evaluate(12_000, exp, 0.50, strike).unwrap();
        assert!(ev_drop.lead_lag.delta_p_star <= -0.02);
        assert!(ev_drop.lead_lag.trigger_defensive_cancel);
        assert!(ev_drop.lead_lag.trigger_defensive_cancel_up);
        assert!(!ev_drop.lead_lag.trigger_defensive_cancel_dn);

        // Silent tick 400ms later still holds UP cancel via window/cooldown, DOWN stays clear.
        let ev_hold = st2.evaluate(12_400, exp, 0.50, strike).unwrap();
        assert!(ev_hold.lead_lag.trigger_defensive_cancel_up);
        assert!(!ev_hold.lead_lag.trigger_defensive_cancel_dn);

        // Once 1000ms window rolls past drop and cooldown expires (t=13_600), both clear.
        let ev_clear = st2.evaluate(13_600, exp, 0.50, strike).unwrap();
        assert!(!ev_clear.lead_lag.trigger_defensive_cancel);
        assert!(!ev_clear.lead_lag.trigger_defensive_cancel_up);
        assert!(!ev_clear.lead_lag.trigger_defensive_cancel_dn);

        // Pure theta decay at constant off-strike spot gives zero delta_p_star in rolling mode.
        let mut theta = RollingTwapAndVariance::new(dir_cfg).unwrap();
        theta.push_spot(10_000, 90_150.0).unwrap();
        let _ = theta.evaluate(10_000, exp, 0.60, strike).unwrap();
        let ev_theta = theta.evaluate(270_000, exp, 0.70, strike).unwrap();
        assert_close(ev_theta.lead_lag.delta_p_star, 0.0, 1e-12);
        assert!(!ev_theta.lead_lag.trigger_defensive_cancel);

        // Multi-tick rally triggers ONLY DOWN side in directional mode.
        let mut rally = RollingTwapAndVariance::new(dir_cfg).unwrap();
        for step in 0..=10_u64 {
            rally.push_spot(10_000 + step * 100, 90_000.0).unwrap();
        }
        let _ = rally.evaluate(11_000, exp, 0.50, strike).unwrap();
        for step in 1..=10_u64 {
            rally
                .push_spot(11_000 + step * 100, 90_000.0 + (step as f64) * 18.0)
                .unwrap();
        }
        let ev_rally = rally.evaluate(12_000, exp, 0.50, strike).unwrap();
        assert!(ev_rally.lead_lag.delta_p_star >= 0.02);
        assert!(!ev_rally.lead_lag.trigger_defensive_cancel_up);
        assert!(ev_rally.lead_lag.trigger_defensive_cancel_dn);

        // Symmetric mode cancels both sides.
        let sym_cfg = Station2Config {
            lead_lag_directional: false,
            ..dir_cfg
        };
        let mut sym = RollingTwapAndVariance::new(sym_cfg).unwrap();
        sym.push_spot(10_000, 90_000.0).unwrap();
        let _ = sym.evaluate(10_500, exp, 0.50, strike).unwrap();
        sym.push_spot(10_800, 89_800.0).unwrap();
        let ev_sym = sym.evaluate(10_900, exp, 0.50, strike).unwrap();
        assert!(ev_sym.lead_lag.trigger_defensive_cancel_up);
        assert!(ev_sym.lead_lag.trigger_defensive_cancel_dn);
    }

    #[test]
    fn high_rate_burst_preserves_rolling_lead_lag_reference_window() {
        let cfg = Station2Config {
            lead_lag_cancel_threshold: 0.02,
            lead_lag_window_ms: 1_500,
            lead_lag_cooldown_ms: 0,
            lead_lag_directional: true,
            ..Station2Config::default()
        };
        let mut st2 = RollingTwapAndVariance::new(cfg).unwrap();
        // Push 2,000 samples over 2,000ms (1 sample/ms > 128 raw capacity):
        // first 500ms at 90_000, next 1,500ms ramping down to 89_750.
        for ms in 0..=2_000_u64 {
            let px = if ms <= 500 {
                90_000.0
            } else {
                90_000.0 - ((ms - 500) as f64) * (250.0 / 1_500.0)
            };
            st2.push_spot(10_000 + ms, px).unwrap();
        }
        // Lookup 1,500ms before 12_000 => ts <= 10_500 => ~90_000.0 (preserved by 50ms bucketing).
        let ref_px = st2.lead_lag_ref_spot(12_000, 1_500).unwrap();
        assert_close(ref_px, 90_000.0, 1e-9);
        let ev = st2.evaluate(12_000, 300_000, 0.50, Some(90_000.0)).unwrap();
        assert!(ev.lead_lag.trigger_defensive_cancel_up);
        assert!(!ev.lead_lag.trigger_defensive_cancel_dn);
    }

    #[test]
    fn set_epoch_secs_scales_15m_remaining_variance_correctly() {
        let mut st2 = RollingTwapAndVariance::new(Station2Config::default()).unwrap();
        assert!(st2.set_epoch_secs(0.0).is_err());
        assert!(st2.set_epoch_secs(-900.0).is_err());
        assert!(st2.set_epoch_secs(f64::NAN).is_err());

        st2.set_epoch_secs(900.0).unwrap();
        // With T = 900s and tau = 450s remaining at p_mid = 0.50:
        // tau_frac = 450 / 900 = 0.50, v_rem = 0.25 * 0.50 = 0.125 (vs clamped 0.25 if T = 300s).
        let out = st2.evaluate(450_000, 900_000, 0.50, None).unwrap();
        assert_close(out.tau_secs, 450.0, 1e-9);
        assert_close(out.v_rem, 0.125, 1e-9);
    }
}
