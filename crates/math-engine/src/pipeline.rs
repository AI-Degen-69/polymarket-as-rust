use core_types::{
    EngineConfig, FeeSchedule, FinitePosF64, FixedBook, MathError, MicroUsdc, MonotonicClock,
    Outcome, SgMode, SharesMicro, SignedSharesMicro, TickPrecision,
};
use serde::Serialize;

use crate::station1_sg::{SavitzkyGolayFilter, SgOutput};
use crate::station2_twap::{LeadLagSignal, RollingTwapAndVariance, Station2Config, Station2Output};
use crate::station3_kappa::{KappaConfig, KappaEstimator, KappaOutput};
use crate::station4_6_pricer::{
    DualBidPricer, PostOnlyBounds, PricerConfig, PricerInput, PricerTrace, QuoteOutput,
    SuppressionReason,
};

/// Unified configuration for the zero-allocation Stations 1–6 math pipeline.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct MathPipelineConfig {
    /// Station 1 Savitzky-Golay filter mode (`Causal` default).
    pub sg_mode: SgMode,
    /// Station 2 TWAP, variance decay, and Binance lead-lag configuration.
    pub station2: Station2Config,
    /// Station 3 UP-book liquidity decay (`kappa`) configuration.
    pub kappa: KappaConfig,
    /// Stations 4–6 optimal spread, reservation price, and Post-Only configuration.
    pub pricer: PricerConfig,
}

impl MathPipelineConfig {
    /// Constructs locked operator defaults (`BinaryCara`, `Causal`, `TradeNormalizedDepth`, `gamma=0.15`).
    pub fn try_default() -> Result<Self, MathError> {
        let cfg = Self {
            sg_mode: SgMode::Causal,
            station2: Station2Config::default(),
            kappa: KappaConfig::default(),
            pricer: PricerConfig::try_default()?,
        };
        cfg.validate()?;
        Ok(cfg)
    }

    /// Maps validated runtime `EngineConfig` into `MathPipelineConfig`.
    pub fn from_engine_config(cfg: &EngineConfig) -> Result<Self, MathError> {
        let gamma = FinitePosF64::try_new(cfg.gamma)?;
        let pricer = PricerConfig {
            gamma,
            include_variance_spread: cfg.include_variance_spread,
            enforce_fee_floor: cfg.enforce_fee_floor,
            min_combined_spread_micro: cfg.min_combined_spread_micro,
            min_spread_ticks: cfg.min_spread_ticks,
            max_combined_bid_micro: cfg.max_combined_bid_micro,
        };
        let station2 = Station2Config {
            variance_mode: cfg.variance_mode,
            ..Station2Config::default()
        };
        let kappa = KappaConfig {
            mode: cfg.kappa_mode,
            ..KappaConfig::default()
        };
        let out = Self {
            sg_mode: cfg.sg_mode,
            station2,
            kappa,
            pricer,
        };
        out.validate()?;
        Ok(out)
    }

    /// Validates all sub-station configurations.
    pub fn validate(&self) -> Result<(), MathError> {
        self.station2.validate()?;
        self.kappa.validate()?;
        self.pricer.validate()?;
        Ok(())
    }
}

/// Borrowed, stack-only input bundle passed to `MathPipeline::evaluate()`.
#[derive(Debug, Clone, Copy)]
pub struct PipelineInput<'a> {
    /// Current adjusted Unix wall-clock timestamp in milliseconds.
    pub now_ms: u64,
    /// Active 5-minute epoch expiration timestamp in Unix milliseconds.
    pub expiry_ms: u64,
    /// Current minimum tick increment in micro-USDC.
    pub tick_size_micro: MicroUsdc,
    /// Active market fee schedule.
    pub fee_schedule: FeeSchedule,
    /// Signed net UP-DOWN inventory (`q = q_UP - q_DOWN`).
    pub inventory_q: SignedSharesMicro,
    /// Stack-allocated top-10 UP orderbook snapshot.
    pub book_up: &'a FixedBook<10>,
    /// Stack-allocated top-10 DOWN orderbook snapshot.
    pub book_dn: &'a FixedBook<10>,
    /// Optional latest BTC/USD spot reference price.
    pub spot_usd: Option<f64>,
    /// Optional inception BTC/USD Chainlink strike price `K`.
    pub strike_usd: Option<f64>,
    /// Currently active resting UP bid price if any (3-way self-cross guard).
    pub active_bid_up: Option<MicroUsdc>,
    /// Currently active resting DOWN bid price if any (3-way self-cross guard).
    pub active_bid_dn: Option<MicroUsdc>,
}

/// Per-station and total nanosecond compute latencies for one evaluation tick.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize)]
pub struct StationTimings {
    /// Station 1 compute duration (`ns`).
    pub s1_ns: u64,
    /// Station 2 compute duration (`ns`).
    pub s2_ns: u64,
    /// Station 3 compute duration (`ns`).
    pub s3_ns: u64,
    /// Station 4 compute duration (`ns`).
    pub s4_ns: u64,
    /// Station 5 compute duration (`ns`).
    pub s5_ns: u64,
    /// Station 6 compute duration (`ns`).
    pub s6_ns: u64,
    /// End-to-end `MathPipeline::evaluate` compute duration (`ns`).
    pub total_compute_ns: u64,
}

/// Explicit diagnostic and safety flags emitted on each evaluation tick.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize)]
pub struct PipelineFlags {
    /// `true` while Station 1 has fewer than 5 samples.
    pub sg_warming_up: bool,
    /// `true` when Station 1 clamped an overshoot/undershoot to BBO/tick bounds.
    pub sg_clamped_to_bbo: bool,
    /// `true` when Station 3 invoked explicit fallback `kappa_fallback`.
    pub kappa_fallback: bool,
    /// `true` when `strike_usd` is `None` (lead-lag inactive).
    pub strike_missing: bool,
    /// `true` when either UP or DOWN L2 book was crossed/locked.
    pub crossed_book: bool,
    /// `true` when Binance lead-lag jump triggered defensive maker cancel.
    pub defensive_cancel: bool,
}

/// Complete step-by-step trace of Stations 1–6 (`Copy`, zero heap allocations).
#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
pub struct PipelineTrace {
    /// Adjusted Unix timestamp of evaluation (`ms`).
    pub now_ms: u64,
    /// Market expiry timestamp (`ms`).
    pub expiry_ms: u64,
    /// Active tick size (`micro-USDC`).
    pub tick_size_micro: MicroUsdc,
    /// Signed net inventory `q` (`micro-shares`).
    pub inventory_q: SignedSharesMicro,
    /// Raw arithmetic UP mid-price (`USD`) if UP book had two-sided BBO.
    pub raw_mid: Option<f64>,
    /// Live UP best bid if present.
    pub best_bid_up: Option<MicroUsdc>,
    /// Live UP best ask if present.
    pub best_ask_up: Option<MicroUsdc>,
    /// Live DOWN best bid if present.
    pub best_bid_dn: Option<MicroUsdc>,
    /// Live DOWN best ask if present.
    pub best_ask_dn: Option<MicroUsdc>,
    /// Latest BTC/USD spot price recorded in Station 2 if any.
    pub spot_usd: Option<f64>,
    /// Epoch strike price `K` if known.
    pub strike_usd: Option<f64>,
    /// Station 1 output (`None` during warmup or empty/crossed book).
    pub sg: Option<SgOutput>,
    /// Station 2 output (`None` before Station 1 warmup completes).
    pub station2: Option<Station2Output>,
    /// Station 3 output (`None` before Station 1 warmup completes).
    pub kappa: Option<KappaOutput>,
    /// Stations 4–6 trace (`None` before Station 1 warmup completes).
    pub pricer: Option<PricerTrace>,
    /// Final quote output (`Active` or `Suppressed`).
    pub quote: QuoteOutput,
    /// Diagnostic and safety flags.
    pub flags: PipelineFlags,
    /// Detailed per-station nanosecond timings.
    pub timings: StationTimings,
    /// Top-level duplicate of `timings.total_compute_ns` for JSONL telemetry schema parity.
    pub total_compute_ns: u64,
}

/// Zero-allocation orchestrator wiring Stations 1–6 plus Binance lead-lag fast path.
#[derive(Debug, Clone, Copy)]
pub struct MathPipeline {
    config: MathPipelineConfig,
    clock: MonotonicClock,
    sg: SavitzkyGolayFilter,
    st2: RollingTwapAndVariance,
    st3: KappaEstimator,
    pricer: DualBidPricer,
    pending_defensive_cancel: bool,
}

impl MathPipeline {
    /// Initializes Stations 1–6 from validated configuration and monotonic QPC clock.
    pub fn new(config: MathPipelineConfig, clock: MonotonicClock) -> Result<Self, MathError> {
        config.validate()?;
        Ok(Self {
            config,
            clock,
            sg: SavitzkyGolayFilter::new(config.sg_mode),
            st2: RollingTwapAndVariance::new(config.station2)?,
            st3: KappaEstimator::new(config.kappa)?,
            pricer: DualBidPricer::new(config.pricer)?,
            pending_defensive_cancel: false,
        })
    }

    /// Returns active configuration.
    #[must_use]
    pub const fn config(&self) -> MathPipelineConfig {
        self.config
    }

    /// Clears all stateful filter and TWAP buffers on 5-minute epoch rollover or reconnect.
    pub const fn reset_epoch(&mut self) {
        self.sg.reset();
        self.st2.reset();
        self.st3.reset();
        self.pending_defensive_cancel = false;
    }

    /// Computes `[expiry_ms - 60s, expiry_ms]` terminal TWAP when available,
    /// falling back to the trailing 60s TWAP at `now_ms`.
    #[must_use]
    pub fn settlement_twap_usd(&mut self, now_ms: u64, expiry_ms: u64) -> Option<f64> {
        self.st2.settlement_twap_usd(now_ms, expiry_ms)
    }

    /// Fast-path Binance Spot ingress updating 60s TWAP, spot vol, and `<1ms` defensive cancel signal.
    pub fn on_binance_spot(
        &mut self,
        ts_ms: u64,
        spot_usd: f64,
        strike_usd: Option<f64>,
        expiry_ms: u64,
    ) -> Result<LeadLagSignal, MathError> {
        if !self.st2.push_spot_accepted(ts_ms, spot_usd)? {
            return Ok(self.st2.peek_lead_lag());
        }
        let tau_secs = expiry_ms.saturating_sub(ts_ms) as f64 / 1000.0;
        let sig = self.st2.compute_lead_lag(spot_usd, strike_usd, tau_secs)?;
        if sig.trigger_defensive_cancel {
            self.pending_defensive_cancel = true;
        }
        Ok(sig)
    }

    /// Updates Station 3's EWMA trade size `v_bar` on UP public trades (ignores DOWN to avoid double counting).
    pub fn on_poly_trade(&mut self, outcome: Outcome, size: SharesMicro) {
        if outcome == Outcome::Up {
            self.st3.update_trade_size(size);
        }
    }

    /// Runs Stations 1–6 cleanly with zero heap allocations on valid hot-path inputs.
    pub fn evaluate(&mut self, input: &PipelineInput<'_>) -> Result<PipelineTrace, MathError> {
        let eval_t0 = clock_ns(&self.clock)?;
        validate_input_books_and_meta(input)?;
        if let Some(spot) = input.spot_usd
            && self.st2.last_spot_usd() != Some(spot)
        {
            let _ = self.on_binance_spot(input.now_ms, spot, input.strike_usd, input.expiry_ms)?;
        }

        let mut flags = PipelineFlags {
            strike_missing: input.strike_usd.is_none(),
            ..PipelineFlags::default()
        };
        if input.book_up.is_crossed() || input.book_dn.is_crossed() {
            flags.crossed_book = true;
            flags.defensive_cancel = self.take_defensive_cancel();
            return self.suppressed_result(
                input,
                None,
                SuppressionReason::CrossedOrderBook,
                flags,
                eval_t0,
            );
        }
        self.evaluate_uncrossed(input, flags, eval_t0)
    }

    fn take_defensive_cancel(&mut self) -> bool {
        self.st2.clear_one_shot_lead_lag();
        std::mem::take(&mut self.pending_defensive_cancel)
    }

    fn evaluate_uncrossed(
        &mut self,
        input: &PipelineInput<'_>,
        mut flags: PipelineFlags,
        eval_t0: u64,
    ) -> Result<PipelineTrace, MathError> {
        let (Some(up_bid), Some(up_ask), Some(raw_mid)) = (
            input.book_up.best_bid(),
            input.book_up.best_ask(),
            input.book_up.raw_mid_usd()?,
        ) else {
            flags.defensive_cancel = self.take_defensive_cancel();
            return self.suppressed_result(
                input,
                None,
                SuppressionReason::EmptyOrderBook,
                flags,
                eval_t0,
            );
        };
        let (t1_end, s1_ns, sg_opt) = self.run_s1(
            raw_mid,
            up_bid.price.to_usd_f64(),
            up_ask.price.to_usd_f64(),
            input.tick_size_micro.to_usd_f64(),
            eval_t0,
        )?;
        let Some(sg_out) = sg_opt else {
            flags.sg_warming_up = true;
            flags.defensive_cancel = self.take_defensive_cancel();
            return self.suppressed_with_s1(input, raw_mid, flags, eval_t0, s1_ns);
        };
        flags.sg_clamped_to_bbo = sg_out.clamped_to_bbo;
        self.finish_stations_2_to_6(input, raw_mid, sg_out, flags, (t1_end, s1_ns, eval_t0))
    }

    fn run_s1(
        &mut self,
        raw_mid: f64,
        bid_usd: f64,
        ask_usd: f64,
        tick_usd: f64,
        t1: u64,
    ) -> Result<(u64, u64, Option<SgOutput>), MathError> {
        let sg_opt = self.sg.push(raw_mid, bid_usd, ask_usd, tick_usd)?;
        let t2 = clock_ns(&self.clock)?;
        Ok((t2, t2.saturating_sub(t1), sg_opt))
    }

    fn finish_stations_2_to_6(
        &mut self,
        input: &PipelineInput<'_>,
        raw_mid: f64,
        sg_out: SgOutput,
        mut flags: PipelineFlags,
        (t2, s1_ns, eval_t0): (u64, u64, u64),
    ) -> Result<PipelineTrace, MathError> {
        self.st2.update_mid(input.now_ms, sg_out.s_i)?;
        let st2_out =
            self.st2
                .evaluate(input.now_ms, input.expiry_ms, sg_out.s_i, input.strike_usd)?;
        let t3 = clock_ns(&self.clock)?;
        let s2_ns = t3.saturating_sub(t2);
        let latched = std::mem::take(&mut self.pending_defensive_cancel);
        flags.defensive_cancel = st2_out.lead_lag.trigger_defensive_cancel || latched;

        let kappa_out = self.st3.estimate(input.book_up, sg_out.s_i)?;
        let kappa_pos = FinitePosF64::try_new(kappa_out.kappa)?;
        let t4 = clock_ns(&self.clock)?;
        let s3_ns = t4.saturating_sub(t3);
        flags.kappa_fallback = kappa_out.fallback_used;

        let pricer_in = build_pricer_input(input, sg_out.s_i, st2_out.v_rem, kappa_pos);
        let pricer_trace = self.pricer.price(&pricer_in, &self.clock)?;
        let total = clock_ns(&self.clock)?.saturating_sub(eval_t0);
        let timings = build_timings(s1_ns, s2_ns, s3_ns, &pricer_trace, total);
        let mut trace = empty_trace(input, self.st2.last_spot_usd(), Some(raw_mid), flags);
        trace.sg = Some(sg_out);
        trace.station2 = Some(st2_out);
        trace.kappa = Some(kappa_out);
        trace.pricer = Some(pricer_trace);
        trace.quote = pricer_trace.quote;
        trace.timings = timings;
        trace.total_compute_ns = total;
        Ok(trace)
    }

    fn suppressed_with_s1(
        &self,
        input: &PipelineInput<'_>,
        raw_mid: f64,
        flags: PipelineFlags,
        eval_t0: u64,
        s1_ns: u64,
    ) -> Result<PipelineTrace, MathError> {
        let total = clock_ns(&self.clock)?.saturating_sub(eval_t0);
        let mut trace = empty_trace(input, self.st2.last_spot_usd(), Some(raw_mid), flags);
        trace.quote = QuoteOutput::Suppressed(SuppressionReason::WarmingUp);
        trace.timings.s1_ns = s1_ns;
        trace.timings.total_compute_ns = total;
        trace.total_compute_ns = total;
        Ok(trace)
    }

    fn suppressed_result(
        &self,
        input: &PipelineInput<'_>,
        raw_mid: Option<f64>,
        reason: SuppressionReason,
        flags: PipelineFlags,
        eval_t0: u64,
    ) -> Result<PipelineTrace, MathError> {
        let total = clock_ns(&self.clock)?.saturating_sub(eval_t0);
        let mut trace = empty_trace(input, self.st2.last_spot_usd(), raw_mid, flags);
        trace.quote = QuoteOutput::Suppressed(reason);
        trace.timings.total_compute_ns = total;
        trace.total_compute_ns = total;
        Ok(trace)
    }
}

fn validate_input_books_and_meta(input: &PipelineInput<'_>) -> Result<(), MathError> {
    TickPrecision::from_micro(input.tick_size_micro).map_err(|_| MathError::InvalidTickSize {
        tick_micro: input.tick_size_micro.0,
    })?;
    input
        .fee_schedule
        .validate()
        .map_err(|e| MathError::InvalidMarketDescriptor {
            detail: e.to_string(),
        })?;
    if input.book_up.outcome != Outcome::Up || input.book_dn.outcome != Outcome::Down {
        return Err(MathError::InvalidMarketDescriptor {
            detail: "PipelineInput book_up must be Outcome::Up and book_dn must be Outcome::Down"
                .to_string(),
        });
    }
    Ok(())
}

fn build_pricer_input(
    input: &PipelineInput<'_>,
    s_i: f64,
    v_rem: f64,
    kappa: FinitePosF64,
) -> PricerInput {
    PricerInput {
        s_i,
        v_rem,
        kappa,
        inventory_q: input.inventory_q,
        tick_size_micro: input.tick_size_micro,
        fee_schedule: input.fee_schedule,
        bounds: PostOnlyBounds {
            best_ask_up: input.book_up.best_ask().map(|l| l.price),
            best_ask_dn: input.book_dn.best_ask().map(|l| l.price),
            best_bid_up: input.book_up.best_bid().map(|l| l.price),
            best_bid_dn: input.book_dn.best_bid().map(|l| l.price),
            active_bid_up: input.active_bid_up,
            active_bid_dn: input.active_bid_dn,
        },
    }
}

const fn build_timings(
    s1_ns: u64,
    s2_ns: u64,
    s3_ns: u64,
    pricer: &PricerTrace,
    total_compute_ns: u64,
) -> StationTimings {
    StationTimings {
        s1_ns,
        s2_ns,
        s3_ns,
        s4_ns: pricer.s4_ns,
        s5_ns: pricer.s5_ns,
        s6_ns: pricer.s6_ns,
        total_compute_ns,
    }
}

fn empty_trace(
    input: &PipelineInput<'_>,
    spot_usd: Option<f64>,
    raw_mid: Option<f64>,
    flags: PipelineFlags,
) -> PipelineTrace {
    PipelineTrace {
        now_ms: input.now_ms,
        expiry_ms: input.expiry_ms,
        tick_size_micro: input.tick_size_micro,
        inventory_q: input.inventory_q,
        raw_mid,
        best_bid_up: input.book_up.best_bid().map(|l| l.price),
        best_ask_up: input.book_up.best_ask().map(|l| l.price),
        best_bid_dn: input.book_dn.best_bid().map(|l| l.price),
        best_ask_dn: input.book_dn.best_ask().map(|l| l.price),
        spot_usd,
        strike_usd: input.strike_usd,
        sg: None,
        station2: None,
        kappa: None,
        pricer: None,
        quote: QuoteOutput::Suppressed(SuppressionReason::WarmingUp),
        flags,
        timings: StationTimings::default(),
        total_compute_ns: 0,
    }
}

fn clock_ns(clock: &MonotonicClock) -> Result<u64, MathError> {
    clock.now_ns().map_err(|_| MathError::Overflow)
}

#[cfg(test)]
mod tests {
    use core_types::{BookLevel, ONE_SHARE_MICRO};

    use super::*;

    fn lvl(cents: u64, whole_shares: u64) -> BookLevel {
        BookLevel {
            price: MicroUsdc(cents * 10_000),
            size: SharesMicro(whole_shares * ONE_SHARE_MICRO),
        }
    }

    fn sample_up_dn_books() -> (FixedBook<10>, FixedBook<10>) {
        let up = FixedBook::<10>::try_from_slices(
            Outcome::Up,
            &[lvl(49, 100), lvl(48, 200), lvl(47, 300)],
            &[lvl(51, 100), lvl(52, 200), lvl(53, 300)],
            1,
            1,
        )
        .unwrap();
        let dn = FixedBook::<10>::try_from_slices(
            Outcome::Down,
            &[lvl(49, 100), lvl(48, 200), lvl(47, 300)],
            &[lvl(51, 100), lvl(52, 200), lvl(53, 300)],
            1,
            1,
        )
        .unwrap();
        (up, dn)
    }

    #[test]
    fn pipeline_warmup_evaluation_lead_lag_and_suppression() {
        let clock = MonotonicClock::start();
        let cfg = MathPipelineConfig::from_engine_config(&EngineConfig::default()).unwrap();
        let mut pipe = MathPipeline::new(cfg, clock).unwrap();
        let (up, dn) = sample_up_dn_books();

        // Crossed book is flagged and suppressed without error.
        let crossed_up =
            FixedBook::<10>::try_from_slices(Outcome::Up, &[lvl(52, 100)], &[lvl(51, 100)], 1, 1)
                .unwrap();
        let crossed_in = PipelineInput {
            now_ms: 100_000,
            expiry_ms: 300_000,
            tick_size_micro: MicroUsdc(10_000),
            fee_schedule: FeeSchedule::btc_5m_default(),
            inventory_q: SignedSharesMicro::ZERO,
            book_up: &crossed_up,
            book_dn: &dn,
            spot_usd: Some(90_000.0),
            strike_usd: Some(90_000.0),
            active_bid_up: None,
            active_bid_dn: None,
        };
        let cr = pipe.evaluate(&crossed_in).unwrap();
        assert!(cr.flags.crossed_book);
        assert_eq!(
            cr.quote,
            QuoteOutput::Suppressed(SuppressionReason::CrossedOrderBook)
        );

        // 4 warmup ticks suppress with WarmingUp.
        for tick in 0..4_u64 {
            let inp = PipelineInput {
                now_ms: 100_000 + tick * 100,
                expiry_ms: 300_000,
                tick_size_micro: MicroUsdc(10_000),
                fee_schedule: FeeSchedule::btc_5m_default(),
                inventory_q: SignedSharesMicro::ZERO,
                book_up: &up,
                book_dn: &dn,
                spot_usd: Some(90_000.0),
                strike_usd: Some(90_000.0),
                active_bid_up: None,
                active_bid_dn: None,
            };
            let tr = pipe.evaluate(&inp).unwrap();
            assert!(tr.flags.sg_warming_up);
            assert_eq!(
                tr.quote,
                QuoteOutput::Suppressed(SuppressionReason::WarmingUp)
            );
        }

        // Tick 5 emits active quotes and serialized JSONL contains payload.total_compute_ns.
        let active_in = PipelineInput {
            now_ms: 100_400,
            expiry_ms: 300_000,
            tick_size_micro: MicroUsdc(10_000),
            fee_schedule: FeeSchedule::btc_5m_default(),
            inventory_q: SignedSharesMicro::ZERO,
            book_up: &up,
            book_dn: &dn,
            spot_usd: Some(90_000.0),
            strike_usd: Some(90_000.0),
            active_bid_up: None,
            active_bid_dn: None,
        };
        let active = pipe.evaluate(&active_in).unwrap();
        assert!(!active.flags.sg_warming_up);
        assert!(matches!(
            active.quote,
            QuoteOutput::Active {
                bid_up: Some(_),
                bid_dn: Some(_),
                ..
            }
        ));
        let json_val = serde_json::to_value(active).unwrap();
        assert!(json_val["total_compute_ns"].is_u64());

        // Fast-path Binance jump sets defensive_cancel once and clears on subsequent silent evaluate().
        let sig = pipe
            .on_binance_spot(100_450, 90_300.0, Some(90_000.0), 300_000)
            .unwrap();
        assert!(sig.trigger_defensive_cancel);
        let jump_in = PipelineInput {
            spot_usd: Some(90_300.0),
            ..active_in
        };
        let after_jump = pipe.evaluate(&jump_in).unwrap();
        assert!(after_jump.flags.defensive_cancel);
        let second_silent = pipe.evaluate(&jump_in).unwrap();
        assert!(!second_silent.flags.defensive_cancel);
        assert_eq!(second_silent.station2.unwrap().lead_lag.delta_p_star, 0.0);
        assert!(
            !second_silent
                .station2
                .unwrap()
                .lead_lag
                .trigger_defensive_cancel
        );

        // Out-of-order past spot timestamp is rejected cleanly without mutating delta_p_star.
        let ooo = pipe
            .on_binance_spot(50_000, 88_000.0, Some(90_000.0), 300_000)
            .unwrap();
        assert_eq!(ooo.delta_p_star, 0.0);
        assert!(!ooo.trigger_defensive_cancel);

        // Epoch reset clears SG warmup cleanly.
        pipe.reset_epoch();
        let after_reset = pipe.evaluate(&active_in).unwrap();
        assert!(after_reset.flags.sg_warming_up);
    }
}
