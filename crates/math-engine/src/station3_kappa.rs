use core_types::{FixedBook, KappaMode, MathError, Outcome, SharesMicro};
use serde::Serialize;

use crate::safe_clamp;

/// Configuration parameters for Station 3 UP-book liquidity decay (`kappa`) estimation.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct KappaConfig {
    /// Estimator mode (`TradeNormalizedDepth` default vs `LibraryLogSlope` A/B toggle).
    pub mode: KappaMode,
    /// Minimum clamped `kappa` (`15.0` `USD^-1` default -> widest safety spread).
    pub kappa_min: f64,
    /// Maximum clamped `kappa` (`200.0` `USD^-1` default -> tightest safety spread).
    pub kappa_max: f64,
    /// Explicit fallback `kappa` when depth estimation fails or degenerates (`50.0` default).
    pub kappa_fallback: f64,
    /// Initial baseline average public trade size `v_bar` in shares (`100.0` default).
    pub v_bar_initial: f64,
    /// Lower safety floor on EWMA public trade size `v_bar` (`10.0` shares default).
    pub v_bar_floor: f64,
    /// EWMA decay coefficient for `v_bar` updates (`0.10` default).
    pub v_bar_ewma_alpha: f64,
}

impl Default for KappaConfig {
    fn default() -> Self {
        Self {
            mode: KappaMode::TradeNormalizedDepth,
            kappa_min: 15.0,
            kappa_max: 200.0,
            kappa_fallback: 50.0,
            v_bar_initial: 100.0,
            v_bar_floor: 10.0,
            v_bar_ewma_alpha: 0.10,
        }
    }
}

impl KappaConfig {
    /// Validates clamping bounds, fallback inclusion, and EWMA coefficient invariants.
    pub fn validate(&self) -> Result<(), MathError> {
        check_pos_finite(self.kappa_min)?;
        check_pos_finite(self.kappa_max)?;
        if self.kappa_max < self.kappa_min {
            return Err(err_desc("kappa_max must be >= kappa_min"));
        }
        check_pos_finite(self.kappa_fallback)?;
        if self.kappa_fallback < self.kappa_min || self.kappa_fallback > self.kappa_max {
            return Err(err_desc(
                "kappa_fallback must lie inside [kappa_min, kappa_max]",
            ));
        }
        check_pos_finite(self.v_bar_floor)?;
        check_pos_finite(self.v_bar_initial)?;
        if self.v_bar_initial < self.v_bar_floor {
            return Err(err_desc("v_bar_initial must be >= v_bar_floor"));
        }
        if !self.v_bar_ewma_alpha.is_finite()
            || self.v_bar_ewma_alpha <= 0.0
            || self.v_bar_ewma_alpha > 1.0
        {
            return Err(err_desc("v_bar_ewma_alpha must lie in (0, 1]"));
        }
        Ok(())
    }
}

/// Output produced by Station 3 liquidity depth intensity estimation.
#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
pub struct KappaOutput {
    /// Clamped liquidity decay intensity `kappa` (`USD^-1`).
    pub kappa: f64,
    /// Raw unclamped positive estimate if computable (`None` if non-finite or insufficient levels).
    pub raw_kappa: Option<f64>,
    /// Current EWMA average trade size `v_bar` (shares, floored at `v_bar_floor`).
    pub v_bar: f64,
    /// `true` when explicit fallback `kappa_fallback` was invoked.
    pub fallback_used: bool,
    /// Total valid UP L2 levels across bid/ask sides used in estimation.
    pub levels_used: u8,
}

/// Zero-allocation Station 3 UP-book liquidity elasticity (`kappa`) estimator.
#[derive(Debug, Clone, Copy)]
pub struct KappaEstimator {
    config: KappaConfig,
    v_bar: f64,
}

impl KappaEstimator {
    /// Creates an initialized Station 3 estimator from validated configuration.
    pub fn new(config: KappaConfig) -> Result<Self, MathError> {
        config.validate()?;
        Ok(Self {
            v_bar: config.v_bar_initial,
            config,
        })
    }

    /// Current EWMA trade size baseline `v_bar` in shares.
    #[must_use]
    pub const fn v_bar(&self) -> f64 {
        self.v_bar
    }

    /// Active estimator configuration.
    #[must_use]
    pub const fn config(&self) -> KappaConfig {
        self.config
    }

    /// Resets `v_bar` to initial baseline (used on epoch rollover).
    pub const fn reset(&mut self) {
        self.v_bar = self.config.v_bar_initial;
    }

    /// Updates EWMA public trade size `v_bar`, ignoring non-positive sizes and enforcing `v_bar_floor`.
    pub fn update_trade_size(&mut self, shares: SharesMicro) {
        let s = shares.to_shares_f64();
        if !s.is_finite() || s <= 0.0 {
            return;
        }
        let alpha = self.config.v_bar_ewma_alpha;
        let next = (1.0 - alpha) * self.v_bar + alpha * s;
        if next.is_finite() {
            self.v_bar = next.max(self.config.v_bar_floor);
        }
    }

    /// Estimates `kappa` strictly from the Polymarket UP orderbook (`Outcome::Up` required).
    pub fn estimate(
        &self,
        book_up: &FixedBook<10>,
        mid_usd: f64,
    ) -> Result<KappaOutput, MathError> {
        if book_up.outcome != Outcome::Up {
            return Err(err_desc(
                "Station 3 KappaEstimator operates strictly on UP book (never mirror-pooled)",
            ));
        }
        if !mid_usd.is_finite() {
            return Err(MathError::NonFiniteInput { value: mid_usd });
        }
        if !(0.0..=1.0).contains(&mid_usd) {
            return Err(MathError::OutOfRangePrice { value: mid_usd });
        }

        let (bid_buf, bid_n) = collect_side_cumulative(book_up.bids_slice(), mid_usd, true);
        let (ask_buf, ask_n) = collect_side_cumulative(book_up.asks_slice(), mid_usd, false);
        let bid_est = bid_buf.get(..bid_n).and_then(|s| self.raw_side_kappa(s));
        let ask_est = ask_buf.get(..ask_n).and_then(|s| self.raw_side_kappa(s));

        let total_levels = u8::try_from(bid_n + ask_n).unwrap_or(u8::MAX);
        match (bid_est, ask_est) {
            (Some(b), Some(a)) => self.finalize_raw(b.min(a), total_levels),
            (Some(b), None) => self.finalize_raw(b, total_levels),
            (None, Some(a)) => self.finalize_raw(a, total_levels),
            (None, None) => Ok(self.fallback_output(None)),
        }
    }

    /// Pure helper estimating `kappa` from pre-aggregated cumulative `(delta_j, V_cum_j)` levels.
    pub fn estimate_from_cumulative_levels(
        &self,
        levels: &[(f64, f64)],
    ) -> Result<KappaOutput, MathError> {
        let len_u8 = u8::try_from(levels.len()).unwrap_or(u8::MAX);
        match self.raw_side_kappa(levels) {
            Some(raw) => self.finalize_raw(raw, len_u8),
            None => Ok(self.fallback_output(None)),
        }
    }

    fn raw_side_kappa(&self, levels: &[(f64, f64)]) -> Option<f64> {
        if !validate_cumulative_levels(levels) {
            return None;
        }
        match self.config.mode {
            KappaMode::TradeNormalizedDepth => self.trade_normalized_raw(levels),
            KappaMode::LibraryLogSlope => log_slope_raw(levels),
        }
    }

    fn trade_normalized_raw(&self, levels: &[(f64, f64)]) -> Option<f64> {
        let &(delta_1, v_1) = levels.first()?;
        if !self.v_bar.is_finite() || self.v_bar <= 0.0 {
            return None;
        }
        let mut num = 0.0_f64;
        let mut den = 0.0_f64;
        for &(delta_j, v_cum_j) in levels {
            num += delta_j * (v_cum_j / self.v_bar);
            den += delta_j * delta_j;
        }
        if !den.is_finite() || den <= 0.0 {
            return None;
        }
        let kappa_emp = num / den;
        let kappa_touch = (v_1 / self.v_bar) / delta_1;
        let raw = kappa_emp.min(kappa_touch);
        raw.is_finite().then_some(raw)
    }

    fn finalize_raw(&self, raw: f64, levels_used: u8) -> Result<KappaOutput, MathError> {
        if !raw.is_finite() || raw <= 0.0 {
            return Ok(KappaOutput {
                kappa: self.config.kappa_fallback,
                raw_kappa: raw.is_finite().then_some(raw),
                v_bar: self.v_bar,
                fallback_used: true,
                levels_used,
            });
        }
        let kappa = safe_clamp(raw, self.config.kappa_min, self.config.kappa_max)?;
        Ok(KappaOutput {
            kappa,
            raw_kappa: Some(raw),
            v_bar: self.v_bar,
            fallback_used: false,
            levels_used,
        })
    }

    const fn fallback_output(&self, raw_kappa: Option<f64>) -> KappaOutput {
        KappaOutput {
            kappa: self.config.kappa_fallback,
            raw_kappa,
            v_bar: self.v_bar,
            fallback_used: true,
            levels_used: 0,
        }
    }
}

fn collect_side_cumulative(
    levels: &[core_types::BookLevel],
    mid_usd: f64,
    is_bid: bool,
) -> ([(f64, f64); 5], usize) {
    let mut out = [(0.0_f64, 0.0_f64); 5];
    let mut count = 0_usize;
    let mut cum_shares = 0.0_f64;
    for lvl in levels {
        if count >= 5 {
            break;
        }
        let px = lvl.price.to_usd_f64();
        let delta = if is_bid { mid_usd - px } else { px - mid_usd };
        let sz = lvl.size.to_shares_f64();
        if delta > 0.0 && sz > 0.0 {
            cum_shares += sz;
            if let Some(slot) = out.get_mut(count) {
                *slot = (delta, cum_shares);
                count += 1;
            }
        }
    }
    (out, count)
}

fn validate_cumulative_levels(levels: &[(f64, f64)]) -> bool {
    if levels.len() < 2 {
        return false;
    }
    let mut prev_delta = 0.0_f64;
    let mut prev_vol = 0.0_f64;
    for &(delta, vol) in levels {
        if !delta.is_finite() || !vol.is_finite() {
            return false;
        }
        if delta <= 0.0 || delta <= prev_delta || vol <= 0.0 || vol < prev_vol {
            return false;
        }
        prev_delta = delta;
        prev_vol = vol;
    }
    true
}

fn log_slope_raw(levels: &[(f64, f64)]) -> Option<f64> {
    let &(delta_1, v_1) = levels.first()?;
    let &(delta_k, v_k) = levels.last()?;
    let span = delta_k - delta_1;
    if !span.is_finite() || span <= 0.0 || v_1 <= 0.0 || v_k <= 0.0 {
        return None;
    }
    let raw = (v_k.ln() - v_1.ln()) / span;
    raw.is_finite().then_some(raw)
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

fn err_desc(msg: &str) -> MathError {
    MathError::InvalidMarketDescriptor {
        detail: msg.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use core_types::{BookLevel, MicroUsdc};

    use super::*;

    fn assert_close(actual: f64, expected: f64, tol: f64) {
        assert!(
            (actual - expected).abs() <= tol,
            "actual={actual} expected={expected} diff={}",
            (actual - expected).abs()
        );
    }

    fn lvl(cents: u64, whole_shares: u64) -> BookLevel {
        BookLevel {
            price: MicroUsdc(cents * 10_000),
            size: SharesMicro(whole_shares * 1_000_000),
        }
    }

    #[test]
    fn vector3_log_slope_scale_invariance_and_spoof_wall_touch_cap() {
        // LibraryLogSlope: (0.01, 500), (0.05, 5000) -> 57.564627
        let slope_cfg = KappaConfig {
            mode: KappaMode::LibraryLogSlope,
            ..KappaConfig::default()
        };
        let slope_est = KappaEstimator::new(slope_cfg).unwrap();
        let out_slope = slope_est
            .estimate_from_cumulative_levels(&[(0.01, 500.0), (0.05, 5000.0)])
            .unwrap();
        assert!(!out_slope.fallback_used);
        assert_close(out_slope.raw_kappa.unwrap(), 57.564_627, 1e-6);
        assert_close(out_slope.kappa, 57.564_627, 1e-6);

        // TradeNormalizedDepth 10x scale linearity before clamping:
        let norm_est = KappaEstimator::new(KappaConfig::default()).unwrap();
        let base = norm_est
            .estimate_from_cumulative_levels(&[(0.01, 100.0), (0.02, 200.0)])
            .unwrap();
        let ten_x = norm_est
            .estimate_from_cumulative_levels(&[(0.01, 1000.0), (0.02, 2000.0)])
            .unwrap();
        assert_close(
            ten_x.raw_kappa.unwrap(),
            base.raw_kappa.unwrap() * 10.0,
            1e-9,
        );
        assert_close(ten_x.kappa, 200.0, 1e-9);

        // Spoof wall [5, 5005, 5010, 5015, 5020] with v_bar=100 -> kappa_touch=5.0 -> clamped to 15.0!
        let spoof = norm_est
            .estimate_from_cumulative_levels(&[
                (0.01, 5.0),
                (0.02, 5005.0),
                (0.03, 5010.0),
                (0.04, 5015.0),
                (0.05, 5020.0),
            ])
            .unwrap();
        assert!(!spoof.fallback_used);
        assert_close(spoof.raw_kappa.unwrap(), 5.0, 1e-9);
        assert_close(spoof.kappa, 15.0, 1e-9);
    }

    #[test]
    fn adversarial_empty_one_level_inverted_delta_down_book_and_v_bar_floor() {
        let mut est = KappaEstimator::new(KappaConfig::default()).unwrap();

        // v_bar floor >= 10.0 even under tiny 1-share trades, ignoring 0.
        est.update_trade_size(SharesMicro::ZERO);
        assert_close(est.v_bar(), 100.0, 1e-9);
        for _ in 0..200 {
            est.update_trade_size(SharesMicro(1_000_000));
        }
        assert_close(est.v_bar(), 10.0, 1e-9);

        // Reject Outcome::Down (must operate on UP book only).
        let dn_book = FixedBook::<10>::empty(Outcome::Down);
        assert!(est.estimate(&dn_book, 0.50).is_err());

        // Empty UP book -> explicit fallback=50.0, zero NaN.
        let empty = FixedBook::<10>::empty(Outcome::Up);
        let res_empty = est.estimate(&empty, 0.50).unwrap();
        assert!(res_empty.fallback_used);
        assert_eq!(res_empty.kappa, 50.0);

        // 1-level book -> fallback=50.0.
        let one_lvl =
            FixedBook::<10>::try_from_slices(Outcome::Up, &[lvl(49, 100)], &[lvl(51, 100)], 1, 1)
                .unwrap();
        let res_one = est.estimate(&one_lvl, 0.50).unwrap();
        assert!(res_one.fallback_used);
        assert_eq!(res_one.kappa, 50.0);

        // Inverted delta / NaN / equal cumulative volume -> explicit fallback, zero NaN.
        let inv = est
            .estimate_from_cumulative_levels(&[(0.05, 100.0), (0.01, 200.0)])
            .unwrap();
        assert!(inv.fallback_used);
        assert_eq!(inv.kappa, 50.0);

        let nan_in = est
            .estimate_from_cumulative_levels(&[(0.01, f64::NAN), (0.02, 200.0)])
            .unwrap();
        assert!(nan_in.fallback_used);
        assert_eq!(nan_in.kappa, 50.0);
    }
}
