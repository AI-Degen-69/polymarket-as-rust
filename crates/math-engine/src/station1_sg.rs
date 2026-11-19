use core_types::{MathError, SgMode};
use serde::Serialize;

use crate::safe_clamp;

/// Zero-lag 5-point quadratic Savitzky-Golay right-endpoint projection weights (`[3,-5,-3,9,31]/35`).
pub const CAUSAL_WEIGHTS: [f64; 5] = [
    3.0 / 35.0,
    -5.0 / 35.0,
    -3.0 / 35.0,
    9.0 / 35.0,
    31.0 / 35.0,
];

/// Centered 5-point quadratic Savitzky-Golay weights (`[-3,12,17,12,-3]/35`) for A/B replay.
pub const SYMMETRIC_WEIGHTS: [f64; 5] = [
    -3.0 / 35.0,
    12.0 / 35.0,
    17.0 / 35.0,
    12.0 / 35.0,
    -3.0 / 35.0,
];

/// Output produced by Station 1 once the 5-sample Savitzky-Golay window is populated.
#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
pub struct SgOutput {
    /// Raw UP mid-price fed into the filter on this tick (`USD/share`).
    pub raw_mid: f64,
    /// Unclamped linear combination `sum(w_k * p_k)`.
    pub sg_unclamped: f64,
    /// Denoised mid-price clamped to live `[best_bid, best_ask]` intersected with `[tick, 1 - tick]`.
    pub s_i: f64,
    /// `true` when `sg_unclamped` overshot the BBO or binary tick bounds and was clamped.
    pub clamped_to_bbo: bool,
}

/// Stack-allocated 5-point Savitzky-Golay microstructure noise filter (`Copy`, zero heap).
#[derive(Debug, Clone, Copy)]
pub struct SavitzkyGolayFilter {
    mode: SgMode,
    buf: [f64; 5],
    count: u8,
}

impl SavitzkyGolayFilter {
    /// Creates an empty Station 1 filter configured for `Causal` (default) or `Symmetric` weights.
    #[must_use]
    pub const fn new(mode: SgMode) -> Self {
        Self {
            mode,
            buf: [0.0; 5],
            count: 0,
        }
    }

    /// Active filter weight mode.
    #[must_use]
    pub const fn mode(&self) -> SgMode {
        self.mode
    }

    /// Number of samples currently buffered (`0..=5`).
    #[must_use]
    pub const fn sample_count(&self) -> u8 {
        self.count
    }

    /// Returns `true` once 5 samples have been observed.
    #[must_use]
    pub const fn is_ready(&self) -> bool {
        self.count >= 5
    }

    /// Clears the sample buffer; mandatory on 5-minute epoch rollover or feed reconnect.
    pub const fn reset(&mut self) {
        self.buf = [0.0; 5];
        self.count = 0;
    }

    /// Pushes one raw mid-price sample and returns `Some(SgOutput)` once 5 samples are buffered.
    pub fn push(
        &mut self,
        raw_mid: f64,
        best_bid_usd: f64,
        best_ask_usd: f64,
        tick_usd: f64,
    ) -> Result<Option<SgOutput>, MathError> {
        validate_sg_inputs(raw_mid, best_bid_usd, best_ask_usd, tick_usd)?;
        self.push_sample(raw_mid);
        if self.count < 5 {
            return Ok(None);
        }

        let weights = match self.mode {
            SgMode::Causal => &CAUSAL_WEIGHTS,
            SgMode::Symmetric => &SYMMETRIC_WEIGHTS,
        };
        let sg_unclamped: f64 = self
            .buf
            .iter()
            .zip(weights.iter())
            .map(|(x, w)| x * w)
            .sum();
        let (lo, hi) = clamp_bounds(best_bid_usd, best_ask_usd, tick_usd);
        let s_i = safe_clamp(sg_unclamped, lo, hi)?;
        let clamped_to_bbo = (s_i - sg_unclamped).abs() > 1e-12;

        Ok(Some(SgOutput {
            raw_mid,
            sg_unclamped,
            s_i,
            clamped_to_bbo,
        }))
    }

    fn push_sample(&mut self, raw_mid: f64) {
        if self.count < 5 {
            if let Some(slot) = self.buf.get_mut(usize::from(self.count)) {
                *slot = raw_mid;
            }
            self.count = self.count.saturating_add(1);
        } else {
            self.buf.rotate_left(1);
            if let Some(last) = self.buf.last_mut() {
                *last = raw_mid;
            }
        }
    }
}

fn validate_sg_inputs(
    raw_mid: f64,
    best_bid_usd: f64,
    best_ask_usd: f64,
    tick_usd: f64,
) -> Result<(), MathError> {
    for val in [raw_mid, best_bid_usd, best_ask_usd, tick_usd] {
        if !val.is_finite() {
            return Err(MathError::NonFiniteInput { value: val });
        }
    }
    if !(0.0..=1.0).contains(&raw_mid) {
        return Err(MathError::OutOfRangePrice { value: raw_mid });
    }
    if !(0.0..=1.0).contains(&best_bid_usd) {
        return Err(MathError::OutOfRangePrice {
            value: best_bid_usd,
        });
    }
    if !(0.0..=1.0).contains(&best_ask_usd) {
        return Err(MathError::OutOfRangePrice {
            value: best_ask_usd,
        });
    }
    if tick_usd <= 0.0 || tick_usd >= 0.5 {
        return Err(MathError::NonPositiveFinite { value: tick_usd });
    }
    Ok(())
}

fn clamp_bounds(best_bid_usd: f64, best_ask_usd: f64, tick_usd: f64) -> (f64, f64) {
    let lo_tick = tick_usd;
    let hi_tick = 1.0 - tick_usd;
    let cand_lo = best_bid_usd.max(lo_tick);
    let cand_hi = best_ask_usd.min(hi_tick);
    if cand_lo <= cand_hi {
        (cand_lo, cand_hi)
    } else {
        (lo_tick, hi_tick)
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
    fn vector1_symmetric_and_causal_match_exact_weights() {
        let samples = [0.49, 0.50, 0.52, 0.51, 0.50];

        let mut sym = SavitzkyGolayFilter::new(SgMode::Symmetric);
        let mut sym_out = None;
        for (idx, &m) in samples.iter().enumerate() {
            let res = sym.push(m, 0.45, 0.55, 0.01).unwrap();
            if idx < 4 {
                assert!(res.is_none());
            } else {
                sym_out = res;
            }
        }
        let sym_res = sym_out.unwrap();
        assert_close(sym_res.sg_unclamped, 0.514_000, 1e-9);
        assert_close(sym_res.s_i, 0.514_000, 1e-9);
        assert!(!sym_res.clamped_to_bbo);

        let mut causal = SavitzkyGolayFilter::new(SgMode::Causal);
        let mut causal_out = None;
        for &m in &samples {
            causal_out = causal.push(m, 0.45, 0.55, 0.01).unwrap();
        }
        let causal_res = causal_out.unwrap();
        assert_close(causal_res.sg_unclamped, 0.500_000, 1e-9);
        assert_close(causal_res.s_i, 0.500_000, 1e-9);
        assert!(!causal_res.clamped_to_bbo);
    }

    #[test]
    fn vector1_overshoot_clamped_to_valid_range_and_bbo() {
        let samples = [0.99, 0.50, 0.50, 0.90, 0.99];
        let mut filter = SavitzkyGolayFilter::new(SgMode::Causal);
        let mut last = None;
        for &m in &samples {
            last = filter.push(m, 0.98, 0.99, 0.01).unwrap();
        }
        let out = last.unwrap();
        assert_close(out.sg_unclamped, 1.078_857_142_857, 1e-6);
        assert!(out.s_i <= 0.99);
        assert_close(out.s_i, 0.99, 1e-9);
        assert!(out.clamped_to_bbo);
    }

    #[test]
    fn adversarial_step_jump_clamp_reset_and_non_finite_guards() {
        let mut filter = SavitzkyGolayFilter::new(SgMode::Causal);
        // 4 ticks at 0.50 -> tick 5 step-jump to 0.60 -> tick 6 still at 0.60 gives unclamped 0.6143.
        for _ in 0..4 {
            let _ = filter.push(0.50, 0.49, 0.51, 0.01).unwrap();
        }
        let tick1 = filter.push(0.60, 0.59, 0.61, 0.01).unwrap().unwrap();
        assert_close(tick1.sg_unclamped, 20.60 / 35.0, 1e-9);
        assert_close(tick1.s_i, 0.59, 1e-9);
        assert!(tick1.clamped_to_bbo);

        let tick2 = filter.push(0.60, 0.59, 0.61, 0.01).unwrap().unwrap();
        assert_close(tick2.sg_unclamped, 0.614_285_714, 1e-6);
        assert!(tick2.s_i <= 0.61);
        assert_close(tick2.s_i, 0.61, 1e-9);
        assert!(tick2.clamped_to_bbo);

        assert!(filter.is_ready());
        filter.reset();
        assert!(!filter.is_ready());
        assert_eq!(filter.sample_count(), 0);
        assert!(filter.push(0.50, 0.49, 0.51, 0.01).unwrap().is_none());

        assert!(filter.push(f64::NAN, 0.49, 0.51, 0.01).is_err());
        assert!(filter.push(0.50, f64::INFINITY, 0.51, 0.01).is_err());
        assert!(filter.push(-0.01, 0.49, 0.51, 0.01).is_err());
    }
}
