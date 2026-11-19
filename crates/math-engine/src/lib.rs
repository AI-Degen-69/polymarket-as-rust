//! Zero-allocation quantitative math engine implementing Stations 1–6
//! of the Polymarket 5-minute BTC Avellaneda-Stoikov pricing pipeline,
//! cross-venue Binance lead-lag fair probability, and 3-way Post-Only guards.

#![forbid(unsafe_code)]
#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::indexing_slicing
    )
)]

use core_types::MathError;

/// Unified zero-allocation Stations 1–6 math pipeline orchestrator and telemetry trace.
pub mod pipeline;
/// Station 1: Savitzky-Golay 5-point microstructure noise filter.
pub mod station1_sg;
/// Station 2: Rolling 60-second ZOH TWAP, variance decay, and cross-venue Binance lead-lag.
pub mod station2_twap;
/// Station 3: UP-book liquidity decay (`kappa`) estimator with spoof-wall touch guard.
pub mod station3_kappa;
/// Stations 4–6: Optimal spread `s*`, reservation price `r_i`, dual bids, and 3-way Post-Only guard.
pub mod station4_6_pricer;

pub use pipeline::{
    MathPipeline, MathPipelineConfig, PipelineFlags, PipelineInput, PipelineTrace, StationTimings,
};
pub use station1_sg::{CAUSAL_WEIGHTS, SYMMETRIC_WEIGHTS, SavitzkyGolayFilter, SgOutput};
pub use station2_twap::{
    LeadLagSignal, RollingTwapAndVariance, SpotSample, Station2Config, Station2Output, norm_cdf,
    norm_pdf,
};
pub use station3_kappa::{KappaConfig, KappaEstimator, KappaOutput};
pub use station4_6_pricer::{
    DualBidPricer, PostOnlyBounds, PricerConfig, PricerInput, PricerTrace, QuoteOutput,
    SuppressionReason,
};

/// Clamps `val` to `[lo, hi]` after verifying all inputs are finite and `lo <= hi`.
///
/// Unlike `f64::clamp`, this never panics on `NaN`, infinity, or inverted bounds.
pub fn safe_clamp(val: f64, lo: f64, hi: f64) -> Result<f64, MathError> {
    if !val.is_finite() {
        return Err(MathError::NonFiniteInput { value: val });
    }
    if !lo.is_finite() {
        return Err(MathError::NonFiniteInput { value: lo });
    }
    if !hi.is_finite() {
        return Err(MathError::NonFiniteInput { value: hi });
    }
    if lo > hi {
        return Err(MathError::InvalidMarketDescriptor {
            detail: format!("invalid clamp bounds: lo ({lo}) > hi ({hi})"),
        });
    }
    Ok(val.clamp(lo, hi))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn safe_clamp_validates_finiteness_and_bound_order() {
        assert_eq!(safe_clamp(0.5, 0.1, 0.9).unwrap(), 0.5);
        assert_eq!(safe_clamp(-1.0, 0.1, 0.9).unwrap(), 0.1);
        assert_eq!(safe_clamp(2.0, 0.1, 0.9).unwrap(), 0.9);
        assert!(safe_clamp(f64::NAN, 0.1, 0.9).is_err());
        assert!(safe_clamp(0.5, f64::NEG_INFINITY, 0.9).is_err());
        assert!(safe_clamp(0.5, 0.9, 0.1).is_err());
    }
}
