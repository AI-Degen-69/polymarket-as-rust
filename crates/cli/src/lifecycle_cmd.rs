use std::error::Error;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use core_types::{
    ClockOffsetEstimator, EngineConfig, FeeSchedule, FixedBook, HighResTimerGuard, JsonlWriter,
    LatencyTracker, MarketDescriptor, MicroUsdc, MonotonicClock, ONE_SHARE_MICRO, OrderSide,
    Outcome, SharesMicro, SignedSharesMicro, TickPrecision,
};
use feeds::{
    BinanceSpotEvent, BookMaintainer, DEFAULT_RING_CAPACITY, FeedHealthEvent, FeedId,
    GammaDiscoveryClient, OracleTwapEvent, PolyMarketEvent,
};
use math_engine::{
    CostContext, MathPipeline, MathPipelineConfig, PipelineInput, QuoteOutput, SuppressionReason,
};
use paper_sim::{
    BaselineTelemetry, FifoBrokerConfig, FifoPaperBroker, MergeEvent, NaiveQuotesInput,
    NewOrderParams, PaperLedger, SimFill, SimOrderStatus, TrajectorySnapshot,
};
use risk_lifecycle::{
    ActorPipelineHarness, ActorSupervisor, BreakerConfig, BreakerStatus, CancelReason,
    CircuitBreakers, CorePinning, DollarNotionalRiskEngine, EpochLifecycleController, EpochPhase,
    ExecutionCommand, FaultInjection, FlattenAction, HysteresisConfig, HysteresisGate,
    LifecycleConfig, OrderAction, OrderRegistrySnapshot, OrderSlotState, QuoteProposal, RiskActor,
    RiskActorTrace, RiskConfig, RiskTier,
};
use rtrb::RingBuffer;
use tokio::sync::{mpsc, oneshot};

use crate::feeds_cmd::{parse_duration_arg, shutdown_actors, spawn_actors};
use crate::feeds_report::{RawJsonlEnvelope, ReplayPolyEvent};
use crate::lifecycle_report::{
    CancelAllRecord, LifecycleDisplayMode, LifecycleReportStats, LifecycleStepRow,
    LifecycleStreamReporter, OrderActionTelemetry, print_breaker_trip, print_epoch_banner,
    print_epoch_rollover, print_lifecycle_fill, print_lifecycle_flatten, print_lifecycle_header,
    print_lifecycle_merge, print_lifecycle_summary, print_phase_transition, print_tier_transition,
    record_breaker_jsonl, record_cancel_all_jsonl, record_epoch_rollover_jsonl,
    record_lifecycle_fill_jsonl, record_lifecycle_flatten_jsonl, record_lifecycle_merge_jsonl,
    record_lifecycle_summary_jsonl, record_order_action_jsonl, record_phase_transition_jsonl,
    record_risk_tier_jsonl,
};
use crate::theme::{self, Badge};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LifecycleSourceMode {
    Live,
    SimEpoch,
    Replay,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CliFaultMode {
    None,
    Stale,
    Tier4,
    TickChange,
    Disconnect,
}

impl From<CliFaultMode> for FaultInjection {
    fn from(mode: CliFaultMode) -> Self {
        match mode {
            CliFaultMode::None => FaultInjection::None,
            CliFaultMode::Stale => FaultInjection::StalePoly,
            CliFaultMode::Tier4 => FaultInjection::ForceTier4,
            CliFaultMode::TickChange => FaultInjection::TickChange,
            CliFaultMode::Disconnect => FaultInjection::StalePoly,
        }
    }
}

pub(crate) struct LifecycleArgs {
    pub(crate) source: LifecycleSourceMode,
    pub(crate) display: LifecycleDisplayMode,
    pub(crate) input: Option<PathBuf>,
    pub(crate) duration: String,
    pub(crate) allow_mid_window: bool,
    pub(crate) fast_forward: bool,
    pub(crate) fault: CliFaultMode,
    pub(crate) v_max: u64,
    pub(crate) base_size: u64,
    pub(crate) queue_decay: paper_sim::QueueDecayPolicy,
    pub(crate) math: crate::MathOverrides,
    pub(crate) pinning: CorePinning,
    pub(crate) jsonl: PathBuf,
}

/// One Pricing actor tick handed to the pinned Pricing OS thread.
struct PricingJob {
    seq: u64,
    now_ms: u64,
    expiry_ms: u64,
    tick_size_micro: MicroUsdc,
    fee_schedule: FeeSchedule,
    inventory_q: SignedSharesMicro,
    book_up: FixedBook<10>,
    book_dn: FixedBook<10>,
    spot_usd: Option<f64>,
    strike_usd: Option<f64>,
    active_bid_up: Option<MicroUsdc>,
    active_bid_dn: Option<MicroUsdc>,
    cost_context: CostContext,
    spot_ticks: Vec<(u64, f64)>,
    up_trade_sizes: Vec<SharesMicro>,
    reset_epoch: bool,
}

struct PricingAck {
    quote: QuoteOutput,
    s_star: f64,
    r_i: f64,
    bid_up_raw: f64,
    twap_60s: Option<f64>,
    total_compute_ns: u64,
    error: Option<String>,
}

/// Feedback applied to `RiskActor` / `HysteresisGate` from local FIFO broker & market events.
#[derive(Debug, Clone)]
enum RiskFeedback {
    OnPlaceAck {
        outcome: Outcome,
        order_id: String,
        price: MicroUsdc,
        size: SharesMicro,
    },
    OnCancelAck {
        outcome: Outcome,
    },
    OnFill {
        outcome: Outcome,
        order_id: String,
        filled_shares: SharesMicro,
    },
    ClearFlattenInFlight,
    TickSizeChange(MicroUsdc),
    AckTickSizeSync,
    ResetEpoch {
        new_tick_size: MicroUsdc,
        now_ns: u64,
    },
}

/// One Risk actor tick handed to the pinned Risk OS thread.
struct RiskJob {
    now_ns: u64,
    clock_offset_ms: i64,
    feedback: Vec<RiskFeedback>,
}

#[derive(Default)]
struct RiskAck {
    trace: Option<RiskActorTrace>,
    risk_ns: u64,
    error: Option<String>,
}

/// Pricing actor body executed on its dedicated (optionally pinned) OS thread.
fn pricing_actor_loop(
    mut pipeline: MathPipeline,
    harness: ActorPipelineHarness,
    mut jobs: mpsc::Receiver<(PricingJob, oneshot::Sender<PricingAck>)>,
    shutdown: Arc<AtomicBool>,
) {
    while let Some((job, ack)) = jobs.blocking_recv() {
        if shutdown.load(Ordering::Acquire) {
            break;
        }
        if job.reset_epoch {
            pipeline.reset_epoch();
        }
        for (ts_ms, spot) in &job.spot_ticks {
            let _ = pipeline.on_binance_spot(*ts_ms, *spot, job.strike_usd, job.expiry_ms);
        }
        for trade_size in &job.up_trade_sizes {
            pipeline.on_poly_trade(Outcome::Up, *trade_size);
        }
        let input = PipelineInput {
            now_ms: job.now_ms,
            expiry_ms: job.expiry_ms,
            tick_size_micro: job.tick_size_micro,
            fee_schedule: job.fee_schedule,
            inventory_q: job.inventory_q,
            book_up: &job.book_up,
            book_dn: &job.book_dn,
            spot_usd: job.spot_usd,
            strike_usd: job.strike_usd,
            active_bid_up: job.active_bid_up,
            active_bid_dn: job.active_bid_dn,
            cost_context: job.cost_context,
        };

        let reply = match pipeline.evaluate(&input) {
            Ok(trace) => {
                let s_star = trace.pricer.map_or(0.0, |p| p.s_star);
                let r_i = trace.pricer.map_or(0.50, |p| p.r_i);
                let bid_up_raw = trace.pricer.map_or(0.0, |p| p.bid_up_raw);
                let bid_dn_raw = trace.pricer.map_or(0.0, |p| p.bid_dn_raw);
                let twap_60s = trace.station2.and_then(|s| s.twap_60s_usd);
                harness.publish_quote(QuoteProposal {
                    seq: job.seq,
                    now_ms: job.now_ms,
                    expiry_ms: job.expiry_ms,
                    tick_size_micro: job.tick_size_micro,
                    quote: trace.quote,
                    bid_up_raw,
                    bid_dn_raw,
                    best_bid_up: trace.best_bid_up,
                    best_bid_dn: trace.best_bid_dn,
                    best_ask_up: trace.best_ask_up,
                    best_ask_dn: trace.best_ask_dn,
                    defensive_cancel: trace.flags.defensive_cancel,
                    total_compute_ns: trace.timings.total_compute_ns,
                });
                PricingAck {
                    quote: trace.quote,
                    s_star,
                    r_i,
                    bid_up_raw,
                    twap_60s,
                    total_compute_ns: trace.timings.total_compute_ns,
                    error: None,
                }
            }
            Err(err) => PricingAck {
                quote: QuoteOutput::Suppressed(SuppressionReason::EmptyOrderBook),
                s_star: 0.0,
                r_i: 0.50,
                bid_up_raw: 0.0,
                twap_60s: None,
                total_compute_ns: 0,
                error: Some(err.to_string()),
            },
        };

        let failed = reply.error.is_some();
        if ack.send(reply).is_err() || failed {
            break;
        }
    }
}

/// Risk actor body executed on its dedicated (optionally pinned) OS thread.
fn risk_actor_loop(
    mut actor: RiskActor,
    mut jobs: mpsc::Receiver<(RiskJob, oneshot::Sender<RiskAck>)>,
    shutdown: Arc<AtomicBool>,
) {
    while let Some((job, ack)) = jobs.blocking_recv() {
        if shutdown.load(Ordering::Acquire) {
            break;
        }

        for item in job.feedback {
            match item {
                RiskFeedback::OnPlaceAck {
                    outcome,
                    order_id,
                    price,
                    size,
                } => actor
                    .hysteresis_mut()
                    .on_place_ack(outcome, order_id, price, size),
                RiskFeedback::OnCancelAck { outcome } => {
                    let _ = actor.hysteresis_mut().on_cancel_ack(outcome);
                }
                RiskFeedback::OnFill {
                    outcome,
                    order_id,
                    filled_shares,
                } => {
                    actor
                        .hysteresis_mut()
                        .on_fill_for_order(outcome, &order_id, filled_shares);
                }
                RiskFeedback::ClearFlattenInFlight => actor.clear_flatten_in_flight(),
                RiskFeedback::TickSizeChange(new_tick) => actor.on_tick_size_change(new_tick),
                RiskFeedback::AckTickSizeSync => actor.acknowledge_tick_size_sync(),
                RiskFeedback::ResetEpoch {
                    new_tick_size,
                    now_ns,
                } => actor.reset_for_new_epoch(new_tick_size, now_ns),
            }
        }

        let start = std::time::Instant::now();
        let stepped = actor.step(job.now_ns, job.clock_offset_ms);
        let mut report = RiskAck {
            risk_ns: start.elapsed().as_nanos() as u64,
            ..Default::default()
        };

        match stepped {
            Ok(trace) => report.trace = trace,
            Err(err) => {
                report.error = Some(err.to_string());
                let _ = ack.send(report);
                break;
            }
        }

        if ack.send(report).is_err() {
            break;
        }
    }
}

pub(crate) fn run(args: LifecycleArgs) -> Result<(), Box<dyn Error>> {
    let mut check_cfg = EngineConfig::from_env()?;
    args.math.apply_to(&mut check_cfg)?;

    if args.input.is_some() && args.source != LifecycleSourceMode::Replay {
        return Err("`--input` is only valid with `--source replay`".into());
    }
    if args.source == LifecycleSourceMode::SimEpoch && args.math.is_any_set() {
        return Err(
            "math override flags (--gamma, --max-combined-bid-micro, --include-variance-spread, \
             --min-spread-ticks, --fee-spread-floor) apply only to `--source live` or `--source replay`; \
             sim-epoch uses synthetic quote proposals"
                .into(),
        );
    }
    if args.source == LifecycleSourceMode::SimEpoch && args.pinning.is_active() {
        return Err(
            "--pin-pricing-core/--pin-risk-core apply to the live actor topology; \
             sim-epoch runs a deterministic single-thread epoch loop"
                .into(),
        );
    }
    let replay_input = if args.source == LifecycleSourceMode::Replay {
        if args.fault != CliFaultMode::None {
            return Err("`--inject-fault` is not supported with `--source replay`".into());
        }
        if args.pinning.is_active() {
            return Err(
                "--pin-pricing-core/--pin-risk-core are not supported with `--source replay`"
                    .into(),
            );
        }
        Some(crate::log_paths::resolve_replay_input(
            args.input.as_deref(),
            &args.jsonl,
        )?)
    } else {
        None
    };

    let _timer_guard = HighResTimerGuard::acquire_1ms()?;
    let clock = MonotonicClock::start();

    print_lifecycle_header(
        match args.source {
            LifecycleSourceMode::Live => "Live WebSockets",
            LifecycleSourceMode::SimEpoch => "Simulated 5m Epoch (SimEpoch)",
            LifecycleSourceMode::Replay => "Offline Replay (JSONL)",
        },
        args.display,
        args.v_max,
        args.base_size,
        args.fast_forward,
        match args.fault {
            CliFaultMode::None => "None",
            CliFaultMode::Stale => "Stale Feed (> 3.5s silence)",
            CliFaultMode::Tier4 => "Force Tier 4 Hard Stop (u = 0.95)",
            CliFaultMode::TickChange => "Tick Size Change (0.01 -> 0.001)",
            CliFaultMode::Disconnect => "Polymarket WS Disconnect",
        },
        &args.jsonl,
    );

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;

    rt.block_on(async move {
        match args.source {
            LifecycleSourceMode::SimEpoch => run_sim_epoch(args, clock).await,
            LifecycleSourceMode::Live => run_live_epoch(args, clock).await,
            LifecycleSourceMode::Replay => {
                let input_path =
                    replay_input.ok_or_else(|| "missing resolved replay input path".to_string())?;
                run_replay_epoch(args, input_path, clock).await
            }
        }
    })
}

async fn run_sim_epoch(args: LifecycleArgs, clock: MonotonicClock) -> Result<(), Box<dyn Error>> {
    let mut jsonl = JsonlWriter::create(&args.jsonl)?;
    let mut pricing_lat = LatencyTracker::new()?;
    let mut risk_lat = LatencyTracker::new()?;
    let mut total_lat = LatencyTracker::new()?;
    let mut stats = LifecycleReportStats::default();

    let (harness, mut priority_rx, mut routine_rx) = ActorPipelineHarness::new(64, 16);

    let v_max_micro = MicroUsdc(args.v_max.saturating_mul(1_000_000));
    let risk_config = RiskConfig {
        v_max_micro,
        base_order_size_shares: args.base_size,
        min_order_size_shares: 5,
        ..Default::default()
    };
    let risk_engine = DollarNotionalRiskEngine::new(risk_config)?;

    let hyst_config = HysteresisConfig {
        hysteresis_buf: 0.25,
        min_order_size_shares: 5,
    };
    let hysteresis = HysteresisGate::new(hyst_config)?;

    let lifecycle_config = LifecycleConfig {
        pre_expiry_lockout_secs: 60,
        mandatory_flatten_secs: 30,
        terminal_lockout_secs: 15,
        min_order_size_shares: 5,
    };
    let lifecycle = EpochLifecycleController::new(lifecycle_config)?;

    let breaker_config = BreakerConfig {
        poly_stale_ms: 3_000,
        binance_stale_ms: 2_000,
        max_clock_drift_ms: 1_000,
        ..Default::default()
    };
    let breakers = CircuitBreakers::new(breaker_config, MicroUsdc(10_000), clock.now_ns()?);

    let mut risk_actor = RiskActor::new(
        harness.clone(),
        risk_engine,
        hysteresis,
        lifecycle,
        breakers,
        args.fault.into(),
    );

    // Simulated 5m epoch time range: 300 seconds down to 0 seconds.
    let start_ms = 1_700_000_000_000u64;
    let expiry_ms = start_ms + 300_000; // 300s horizon
    print_epoch_banner(1, "sim-epoch", None, start_ms, expiry_ms);
    let mut reporter = LifecycleStreamReporter::new(args.display, theme::current());
    let mut resting_bid_up: Option<MicroUsdc> = None;
    let mut resting_bid_dn: Option<MicroUsdc> = None;

    let mut current_phase: Option<EpochPhase> = None;
    let mut current_tier = RiskTier::Tier1Normal;
    let mut sim_q_up = SharesMicro::ZERO;
    let mut sim_q_dn = SharesMicro::ZERO;
    let mut sim_cost_up = MicroUsdc::ZERO;
    let mut sim_cost_dn = MicroUsdc::ZERO;

    // Simulation steps: advance from t=0s to t=305s
    let step_secs = if args.fast_forward { 5 } else { 1 };
    let total_steps = 305 / step_secs;

    let mut last_now_ms = start_ms;
    let mut last_now_ns = clock.now_ns()?;

    for step_idx in 0..=total_steps {
        let elapsed_secs = (step_idx * step_secs).min(305);
        let now_ms = start_ms + (elapsed_secs * 1000);
        let now_ns = clock.now_ns()? + (elapsed_secs * 1_000_000_000);
        last_now_ms = now_ms;
        last_now_ns = now_ns;
        let tau_secs = expiry_ms.saturating_sub(now_ms) / 1000;

        // Simulated market pricing & BBO
        let mid_up = 0.50 + 0.02 * ((elapsed_secs as f64 / 30.0).sin());
        let bid_up_val = ((mid_up - 0.02).clamp(0.05, 0.95) * 1_000_000.0).round() as u64;
        let bid_dn_val = (((1.0 - mid_up) - 0.02).clamp(0.05, 0.95) * 1_000_000.0).round() as u64;

        let pricing_start = clock.now_ns()?;
        let step_quote = QuoteOutput::Active {
            bid_up: Some(MicroUsdc(bid_up_val)),
            bid_dn: Some(MicroUsdc(bid_dn_val)),
            pair_cost_micro: Some(MicroUsdc(bid_up_val + bid_dn_val)),
            gross_margin_bps: Some(400),
        };
        let proposal = QuoteProposal {
            seq: step_idx + 1,
            now_ms,
            expiry_ms,
            tick_size_micro: MicroUsdc(10_000),
            quote: step_quote,
            bid_up_raw: mid_up - 0.02,
            bid_dn_raw: (1.0 - mid_up) - 0.02,
            best_bid_up: Some(MicroUsdc(bid_up_val.saturating_sub(10_000))),
            best_bid_dn: Some(MicroUsdc(bid_dn_val.saturating_sub(10_000))),
            best_ask_up: Some(MicroUsdc(bid_up_val.saturating_add(20_000))),
            best_ask_dn: Some(MicroUsdc(bid_dn_val.saturating_add(20_000))),
            defensive_cancel: args.fault == CliFaultMode::Disconnect,
            total_compute_ns: 12_500,
        };
        harness.publish_quote(proposal);
        let pricing_ns = clock.now_ns()?.saturating_sub(pricing_start).max(12_500);
        pricing_lat.record_ns(pricing_ns)?;

        // Simulate fills during ActiveQuoting to build realistic inventory
        if elapsed_secs == 40 && sim_q_up == SharesMicro::ZERO {
            sim_q_up = SharesMicro::from_raw(args.base_size * ONE_SHARE_MICRO);
            sim_cost_up = MicroUsdc(args.base_size * bid_up_val);
        }

        // Publish updated order registry snapshot
        let snapshot = OrderRegistrySnapshot {
            unhedged_up: sim_q_up,
            unhedged_dn: sim_q_dn,
            unhedged_cost_up_micro: sim_cost_up,
            unhedged_cost_dn_micro: sim_cost_dn,
            open_bid_notional_up: MicroUsdc::ZERO,
            open_bid_notional_dn: MicroUsdc::ZERO,
            open_bid_shares_up: SharesMicro::ZERO,
            open_bid_shares_dn: SharesMicro::ZERO,
            paired_cost_basis: MicroUsdc::ZERO,
            available_usdc_micro: MicroUsdc(100_000_000),
        };
        harness.publish_registry_snapshot(snapshot);

        // Evaluate Risk step
        let risk_start = clock.now_ns()?;
        let trace_opt = risk_actor.step(now_ns, 0)?;
        let risk_ns = clock.now_ns()?.saturating_sub(risk_start).max(6_200);
        risk_lat.record_ns(risk_ns)?;
        total_lat.record_ns(pricing_ns + risk_ns)?;

        if let Some(trace) = trace_opt {
            stats.ticks_evaluated += 1;

            // Track phase transitions
            if current_phase != Some(trace.phase) {
                stats.phase_transitions += 1;
                print_phase_transition(
                    current_phase,
                    trace.phase,
                    tau_secs,
                    match trace.phase {
                        EpochPhase::ActiveQuoting => "Initial epoch quoting window",
                        EpochPhase::PreExpiryLockout => "T-60s reached; accumulation frozen",
                        EpochPhase::MandatoryFlatten => {
                            "T-30s reached; mandatory flatten triggered"
                        }
                        EpochPhase::TerminalLockout => "T-15s reached; terminal settlement lockout",
                    },
                );
                record_phase_transition_jsonl(
                    &mut jsonl,
                    now_ms,
                    now_ns,
                    current_phase,
                    trace.phase,
                    tau_secs,
                    "Epoch timer horizon elapsed",
                )?;
                current_phase = Some(trace.phase);
            }

            // Track tier transitions
            if current_tier != trace.tier {
                print_tier_transition(
                    trace.tier,
                    trace.utilization_u_e6,
                    trace.flatten_action.outcome(),
                );
                record_risk_tier_jsonl(
                    &mut jsonl,
                    now_ms,
                    now_ns,
                    trace.tier,
                    trace.utilization_u_e6,
                    trace.flatten_action.outcome(),
                )?;
                current_tier = trace.tier;
            }

            // Drain priority commands
            while let Ok(cmd) = priority_rx.try_recv() {
                match cmd {
                    risk_lifecycle::ExecutionCommand::CancelAll { reason } => {
                        resting_bid_up = None;
                        resting_bid_dn = None;
                        let _ = cancel_active_sim_slots_and_record(
                            &mut risk_actor,
                            &mut stats,
                            &mut jsonl,
                            &trace,
                            now_ns,
                            &reason,
                        )?;
                    }
                    risk_lifecycle::ExecutionCommand::ExecuteFlattenFak {
                        outcome,
                        price,
                        size,
                        reason,
                    } => {
                        resting_bid_up = None;
                        resting_bid_dn = None;
                        let _ = cancel_active_sim_slots_and_record(
                            &mut risk_actor,
                            &mut stats,
                            &mut jsonl,
                            &trace,
                            now_ns,
                            &reason,
                        )?;
                        risk_actor.clear_flatten_in_flight();

                        let (target_shares, opposite_shares) = match outcome {
                            Outcome::Up => (sim_q_up, sim_q_dn),
                            Outcome::Down => (sim_q_dn, sim_q_up),
                        };
                        let net_delta_raw =
                            target_shares.as_u64().abs_diff(opposite_shares.as_u64());
                        let flatten_raw = size.as_u64().min(net_delta_raw);
                        if flatten_raw > 0 {
                            stats.flattens_executed += 1;
                            let side = if target_shares > opposite_shares {
                                OrderSide::Sell
                            } else {
                                OrderSide::Buy
                            };
                            match (outcome, side) {
                                (Outcome::Up, OrderSide::Buy) => {
                                    sim_q_up = SharesMicro::from_raw(
                                        sim_q_up.as_u64().saturating_add(flatten_raw),
                                    );
                                }
                                (Outcome::Down, OrderSide::Buy) => {
                                    sim_q_dn = SharesMicro::from_raw(
                                        sim_q_dn.as_u64().saturating_add(flatten_raw),
                                    );
                                }
                                (Outcome::Up, OrderSide::Sell) => {
                                    sim_q_up = SharesMicro::from_raw(
                                        sim_q_up.as_u64().saturating_sub(flatten_raw),
                                    );
                                }
                                (Outcome::Down, OrderSide::Sell) => {
                                    sim_q_dn = SharesMicro::from_raw(
                                        sim_q_dn.as_u64().saturating_sub(flatten_raw),
                                    );
                                }
                            }
                            let merged = sim_q_up.min(sim_q_dn);
                            sim_q_up = SharesMicro::from_raw(
                                sim_q_up.as_u64().saturating_sub(merged.as_u64()),
                            );
                            sim_q_dn = SharesMicro::from_raw(
                                sim_q_dn.as_u64().saturating_sub(merged.as_u64()),
                            );
                            if sim_q_up == SharesMicro::ZERO {
                                sim_cost_up = MicroUsdc::ZERO;
                            }
                            if sim_q_dn == SharesMicro::ZERO {
                                sim_cost_dn = MicroUsdc::ZERO;
                            }

                            print_lifecycle_flatten(
                                "sim-epoch",
                                outcome,
                                side,
                                price,
                                SharesMicro::from_raw(flatten_raw),
                                MicroUsdc::ZERO,
                                &reason,
                            );
                            record_lifecycle_flatten_jsonl(
                                &mut jsonl,
                                now_ms,
                                now_ns,
                                "sim-epoch",
                                outcome,
                                side,
                                price,
                                SharesMicro::from_raw(flatten_raw),
                                0,
                                0,
                                &reason,
                            )?;
                        }
                    }
                    risk_lifecycle::ExecutionCommand::Cancel { outcome, .. }
                        if !matches!(
                            risk_actor.hysteresis_mut().state(outcome),
                            OrderSlotState::Idle
                        ) =>
                    {
                        match outcome {
                            Outcome::Up => resting_bid_up = None,
                            Outcome::Down => resting_bid_dn = None,
                        }
                        stats.orders_canceled += 1;
                        let _ = risk_actor.hysteresis_mut().on_cancel_ack(outcome);
                    }
                    _ => {}
                }
            }

            // Drain routine commands
            while let Ok(cmd) = routine_rx.try_recv() {
                match cmd {
                    risk_lifecycle::ExecutionCommand::SubmitNew {
                        outcome,
                        price,
                        size,
                    } => {
                        match outcome {
                            Outcome::Up => resting_bid_up = Some(price),
                            Outcome::Down => resting_bid_dn = Some(price),
                        }
                        stats.orders_submitted += 1;
                        risk_actor.hysteresis_mut().on_place_ack(
                            outcome,
                            format!("sim_{:?}_{}", outcome, step_idx),
                            price,
                            size,
                        );
                    }
                    risk_lifecycle::ExecutionCommand::CancelAndReplace {
                        outcome,
                        new_price,
                        new_size,
                        ..
                    } => {
                        match outcome {
                            Outcome::Up => resting_bid_up = Some(new_price),
                            Outcome::Down => resting_bid_dn = Some(new_price),
                        }
                        stats.orders_replaced += 1;
                        let _ = risk_actor.hysteresis_mut().on_cancel_ack(outcome);
                        risk_actor.hysteresis_mut().on_place_ack(
                            outcome,
                            format!("sim_{:?}_{}", outcome, step_idx),
                            new_price,
                            new_size,
                        );
                    }
                    risk_lifecycle::ExecutionCommand::Cancel { outcome, .. }
                        if !matches!(
                            risk_actor.hysteresis_mut().state(outcome),
                            OrderSlotState::Idle
                        ) =>
                    {
                        match outcome {
                            Outcome::Up => resting_bid_up = None,
                            Outcome::Down => resting_bid_dn = None,
                        }
                        stats.orders_canceled += 1;
                        let _ = risk_actor.hysteresis_mut().on_cancel_ack(outcome);
                    }
                    _ => {}
                }
            }

            let net_delta = (sim_q_up.as_u64() as i64) - (sim_q_dn.as_u64() as i64);

            reporter.on_step(&LifecycleStepRow {
                now_ms,
                tau_secs,
                phase: trace.phase,
                tier: trace.tier,
                u_e6: trace.utilization_u_e6,
                q_up: sim_q_up,
                q_dn: sim_q_dn,
                net_delta: net_delta / 1_000_000,
                quote: step_quote,
                spot_usd: None,
                strike_usd: None,
                resting_bid_up,
                resting_bid_dn,
                net_pnl_micro: 0,
                action_up: &trace.action_up,
                action_dn: &trace.action_dn,
                cancel_all_reason: trace.cancel_all_reason.as_deref(),
                flatten_in_flight: trace.flatten_in_flight,
                best_bid_up: trace.best_bid_up,
                best_ask_up: trace.best_ask_up,
                best_bid_dn: trace.best_bid_dn,
                best_ask_dn: trace.best_ask_dn,
                pricing_ns,
                risk_ns,
            });

            let action_telemetry = OrderActionTelemetry::from_trace(&trace);
            record_order_action_jsonl(
                &mut jsonl,
                now_ms,
                now_ns,
                Outcome::Up,
                &trace.action_up,
                tau_secs,
                action_telemetry,
            )?;
            record_order_action_jsonl(
                &mut jsonl,
                now_ms,
                now_ns,
                Outcome::Down,
                &trace.action_dn,
                tau_secs,
                action_telemetry,
            )?;

            if let BreakerStatus::Tripped { .. } = trace.breaker {
                stats.breaker_trips += 1;
                print_breaker_trip(trace.breaker);
                record_breaker_jsonl(&mut jsonl, now_ms, now_ns, trace.breaker, 0)?;
            }

            if !args.fast_forward {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    }

    stats.set_final_inventory(sim_q_up, sim_q_dn);

    let p_summary = pricing_lat.summary();
    let r_summary = risk_lat.summary();
    let t_summary = total_lat.summary();

    print_lifecycle_summary(&stats, &p_summary, &r_summary, &t_summary);
    record_lifecycle_summary_jsonl(
        &mut jsonl,
        last_now_ms,
        last_now_ns,
        "sim-epoch",
        "sim-epoch",
        &stats,
        &p_summary,
        &r_summary,
        &t_summary,
    )?;
    jsonl.close()?;
    Ok(())
}

fn cancel_active_sim_slots_and_record(
    risk_actor: &mut RiskActor,
    stats: &mut LifecycleReportStats,
    jsonl: &mut JsonlWriter,
    trace: &RiskActorTrace,
    now_ns: u64,
    reason_str: &str,
) -> Result<usize, Box<dyn Error>> {
    let cancel_reason = if matches!(trace.breaker, BreakerStatus::Tripped { .. }) {
        CancelReason::CircuitBreaker
    } else {
        CancelReason::LifecycleLockout
    };
    let telemetry = OrderActionTelemetry::from_trace(trace);
    let mut canceled_count = 0usize;

    for outcome in [Outcome::Up, Outcome::Down] {
        let order_id_opt = match risk_actor.hysteresis_mut().state(outcome) {
            OrderSlotState::Idle => None,
            OrderSlotState::Live { order_id, .. }
            | OrderSlotState::PendingCancel { order_id, .. }
            | OrderSlotState::PendingReconcile { order_id } => Some(order_id.clone()),
            OrderSlotState::PendingPlace { seq, .. } => Some(format!("pending_{seq}")),
        };
        if let Some(order_id) = order_id_opt {
            let _ = risk_actor.hysteresis_mut().on_cancel_ack(outcome);
            stats.orders_canceled = stats.orders_canceled.saturating_add(1);
            canceled_count = canceled_count.saturating_add(1);
            let already_in_trace = {
                let act = match outcome {
                    Outcome::Up => &trace.action_up,
                    Outcome::Down => &trace.action_dn,
                };
                matches!(act, OrderAction::Cancel { order_id: tid, .. } if *tid == order_id)
            };
            if !already_in_trace {
                record_order_action_jsonl(
                    jsonl,
                    trace.now_ms,
                    now_ns,
                    outcome,
                    &OrderAction::Cancel {
                        outcome,
                        order_id,
                        reason: cancel_reason,
                    },
                    trace.tau_secs,
                    telemetry,
                )?;
            }
        }
    }

    if canceled_count > 0 {
        record_cancel_all_jsonl(
            jsonl,
            trace.now_ms,
            now_ns,
            CancelAllRecord {
                market_slug: "sim-epoch",
                phase: Some(trace.phase),
                tau_secs: trace.tau_secs,
                canceled_count,
                flatten_in_flight: Some(trace.flatten_in_flight),
                best_bid_up: trace.best_bid_up,
                best_ask_up: trace.best_ask_up,
                best_bid_dn: trace.best_bid_dn,
                best_ask_dn: trace.best_ask_dn,
                reason: reason_str,
            },
        )?;
    }

    Ok(canceled_count)
}

fn fixed_book_mid(book: &FixedBook<10>) -> Option<MicroUsdc> {
    let bid = book.best_bid()?.price;
    let ask = book.best_ask()?.price;
    Some(MicroUsdc(bid.0.saturating_add(ask.0) / 2))
}

fn fixed_book_depth_at(book: &FixedBook<10>, price: MicroUsdc) -> SharesMicro {
    for lvl in book.bids_slice() {
        if lvl.price == price {
            return lvl.size;
        }
    }
    for lvl in book.asks_slice() {
        if lvl.price == price {
            return lvl.size;
        }
    }
    SharesMicro::ZERO
}

struct LiveLatencies<'a> {
    pricing: &'a mut LatencyTracker,
    risk: &'a mut LatencyTracker,
    total: &'a mut LatencyTracker,
}

struct LiveActorChannels<'a> {
    pricing_tx: &'a mpsc::Sender<(PricingJob, oneshot::Sender<PricingAck>)>,
    risk_tx: &'a mpsc::Sender<(RiskJob, oneshot::Sender<RiskAck>)>,
}

struct CancelAllParams<'a> {
    now_ms: u64,
    now_ns: u64,
    tau_secs: u64,
    cancel_reason: CancelReason,
    reason_str: &'a str,
    r_trace: Option<&'a RiskActorTrace>,
}

struct PendingEpochSettlement {
    expired_slug: String,
    winning_outcome: Outcome,
    settle_delta_micro: i64,
    total_realized_pnl_micro: i64,
    pairs_merged_micro: u64,
    fees_paid_micro: u64,
    estimated_rebate_micro: u64,
}

/// Local 8-station FIFO paper simulator wired into the live Pricing + Risk actor pipeline.
struct LivePaperSimulator {
    desc: MarketDescriptor,
    book_up: BookMaintainer,
    book_dn: BookMaintainer,
    latest_up: Option<FixedBook<10>>,
    latest_dn: Option<FixedBook<10>>,
    fallback_up: FixedBook<10>,
    fallback_dn: FixedBook<10>,
    latest_spot: Option<f64>,
    latest_twap_60s: Option<f64>,
    strike_usd: Option<f64>,
    pending_spot_ticks: Vec<(u64, f64)>,
    pending_up_trades: Vec<SharesMicro>,
    pending_reset_epoch: bool,
    pending_tick_sync_ack: bool,
    broker: FifoPaperBroker,
    ledger: PaperLedger,
    telemetry: BaselineTelemetry,
    harness: ActorPipelineHarness,
    priority_rx: mpsc::Receiver<ExecutionCommand>,
    routine_rx: mpsc::Receiver<ExecutionCommand>,
    pending_feedback: Vec<RiskFeedback>,
    v_max_micro: MicroUsdc,
    base_size_shares: u64,
    cumulative_merged_cost_micro: u64,
    step_count: u64,
    current_phase: Option<EpochPhase>,
    current_tier: RiskTier,
    market_resolved_flag: bool,
    epoch_settled: bool,
    pending_settlement: Option<PendingEpochSettlement>,
    epoch_num: usize,
    reporter: LifecycleStreamReporter,
    stats: LifecycleReportStats,
}

impl LivePaperSimulator {
    #[allow(clippy::too_many_arguments)]
    fn new(
        desc: MarketDescriptor,
        broker_config: FifoBrokerConfig,
        v_max_micro: MicroUsdc,
        base_size_shares: u64,
        display: LifecycleDisplayMode,
        harness: ActorPipelineHarness,
        priority_rx: mpsc::Receiver<ExecutionCommand>,
        routine_rx: mpsc::Receiver<ExecutionCommand>,
    ) -> Result<Self, Box<dyn Error>> {
        let base_shares_micro =
            SharesMicro::from_raw(base_size_shares.saturating_mul(ONE_SHARE_MICRO));
        let ledger = PaperLedger::new(desc.fee_schedule);
        let telemetry =
            BaselineTelemetry::new(broker_config, desc.fee_schedule, base_shares_micro)?;
        let strike_usd = desc.strike_price_usd.map(|f| f.get());

        let sim = Self {
            desc,
            book_up: BookMaintainer::new(Outcome::Up),
            book_dn: BookMaintainer::new(Outcome::Down),
            latest_up: None,
            latest_dn: None,
            fallback_up: FixedBook::<10>::empty(Outcome::Up),
            fallback_dn: FixedBook::<10>::empty(Outcome::Down),
            latest_spot: None,
            latest_twap_60s: None,
            strike_usd,
            pending_spot_ticks: Vec::with_capacity(64),
            pending_up_trades: Vec::with_capacity(32),
            pending_reset_epoch: false,
            pending_tick_sync_ack: false,
            broker: FifoPaperBroker::new(broker_config),
            ledger,
            telemetry,
            harness,
            priority_rx,
            routine_rx,
            pending_feedback: Vec::with_capacity(16),
            v_max_micro,
            base_size_shares,
            cumulative_merged_cost_micro: 0,
            step_count: 0,
            current_phase: None,
            current_tier: RiskTier::Tier1Normal,
            market_resolved_flag: false,
            epoch_settled: false,
            pending_settlement: None,
            epoch_num: 1,
            reporter: LifecycleStreamReporter::new(display, theme::current()),
            stats: LifecycleReportStats::default(),
        };
        let _ = sim.publish_registry_snapshot();
        Ok(sim)
    }

    fn build_registry_snapshot(&self) -> OrderRegistrySnapshot {
        let open_up_notional = self.broker.open_bid_notional(Outcome::Up);
        let open_dn_notional = self.broker.open_bid_notional(Outcome::Down);
        let open_up_shares = self.broker.open_bid_shares(Outcome::Up);
        let open_dn_shares = self.broker.open_bid_shares(Outcome::Down);

        let committed = self
            .ledger
            .cost_basis_up_micro
            .saturating_add(self.ledger.cost_basis_dn_micro)
            .saturating_add(open_up_notional)
            .saturating_add(open_dn_notional);

        let two_leg_budget = i128::from(self.v_max_micro.0.saturating_mul(2))
            .saturating_add(i128::from(self.ledger.realized_pnl_micro))
            .max(0) as u64;
        let available_usdc_micro = MicroUsdc(two_leg_budget.saturating_sub(committed));

        OrderRegistrySnapshot {
            unhedged_up: self.ledger.filled_up,
            unhedged_dn: self.ledger.filled_dn,
            unhedged_cost_up_micro: MicroUsdc(self.ledger.cost_basis_up_micro),
            unhedged_cost_dn_micro: MicroUsdc(self.ledger.cost_basis_dn_micro),
            open_bid_notional_up: MicroUsdc(open_up_notional),
            open_bid_notional_dn: MicroUsdc(open_dn_notional),
            open_bid_shares_up: open_up_shares,
            open_bid_shares_dn: open_dn_shares,
            paired_cost_basis: MicroUsdc(self.cumulative_merged_cost_micro),
            available_usdc_micro,
        }
    }

    fn publish_registry_snapshot(&self) -> OrderRegistrySnapshot {
        let snapshot = self.build_registry_snapshot();
        self.harness.publish_registry_snapshot(snapshot);
        snapshot
    }

    fn observe_spot(&mut self, spot: f64, ev_ts_ms: u64) {
        if spot > 0.0 && spot.is_finite() {
            self.latest_spot = Some(spot);
            if self.strike_usd.is_none() {
                self.strike_usd = Some(spot);
            }
            self.pending_spot_ticks.push((ev_ts_ms, spot));
        }
    }

    fn on_binance_event(&mut self, ev: BinanceSpotEvent, ev_ts_ms: u64) {
        let spot = match ev {
            BinanceSpotEvent::BookTicker { bid_e8, ask_e8, .. } => {
                (bid_e8 as f64 + ask_e8 as f64) / 200_000_000.0
            }
            BinanceSpotEvent::AggTrade { price_e8, .. } => price_e8 as f64 / 100_000_000.0,
        };
        self.observe_spot(spot, ev_ts_ms);
    }

    fn on_oracle_event(&mut self, ev: OracleTwapEvent) {
        let (spot_e8, exch_ts_ms) = match ev {
            OracleTwapEvent::ChainlinkSpot {
                spot_e8,
                exch_ts_ms,
                ..
            } => (spot_e8, exch_ts_ms),
            OracleTwapEvent::ChainlinkTwap {
                twap_e8,
                spot_e8,
                exch_ts_ms,
                ..
            } => {
                let effective = if spot_e8 > 0 { spot_e8 } else { twap_e8 };
                (effective, exch_ts_ms)
            }
        };
        if spot_e8 > 0 {
            self.observe_spot(spot_e8 as f64 / 100_000_000.0, exch_ts_ms);
        }
    }

    fn on_health_event(&mut self, ev: FeedHealthEvent) {
        if let FeedHealthEvent::MarketResolved {
            feed: FeedId::PolymarketClob,
            ..
        } = ev
        {
            self.market_resolved_flag = true;
        }
    }

    fn on_poly_event(
        &mut self,
        ev: PolyMarketEvent,
        now_ms: u64,
        now_ns: u64,
        jsonl: &mut JsonlWriter,
    ) -> Result<(), Box<dyn Error>> {
        match ev {
            PolyMarketEvent::BookSnapshot {
                outcome,
                book,
                rx_mono_ns,
                exch_ts_ms,
                ..
            } => {
                let applied = match outcome {
                    Outcome::Up => self.book_up.apply_snapshot(
                        book.bids_slice(),
                        book.asks_slice(),
                        rx_mono_ns,
                        exch_ts_ms,
                    ),
                    Outcome::Down => self.book_dn.apply_snapshot(
                        book.bids_slice(),
                        book.asks_slice(),
                        rx_mono_ns,
                        exch_ts_ms,
                    ),
                };
                if let Ok(valid_book) = applied {
                    match outcome {
                        Outcome::Up => self.latest_up = Some(valid_book),
                        Outcome::Down => self.latest_dn = Some(valid_book),
                    }
                    if let Some(active) = self.broker.active_order(outcome, OrderSide::Buy) {
                        let active_price = active.price;
                        let depth = fixed_book_depth_at(&valid_book, active_price);
                        self.broker.on_book_update(
                            outcome,
                            OrderSide::Buy,
                            active_price,
                            depth,
                            now_ms,
                        );
                    }
                }
            }
            PolyMarketEvent::PriceChange {
                outcome,
                side,
                price,
                size,
                rx_mono_ns,
                exch_ts_ms,
                ..
            } => {
                match outcome {
                    Outcome::Up => {
                        let _ = self.book_up.apply_delta_allow_transient_cross(
                            side, price, size, rx_mono_ns, exch_ts_ms,
                        );
                        if !self.book_up.is_crossed()
                            && let Ok(top) = self.book_up.top_n::<10>()
                        {
                            self.latest_up = Some(top);
                        }
                    }
                    Outcome::Down => {
                        let _ = self.book_dn.apply_delta_allow_transient_cross(
                            side, price, size, rx_mono_ns, exch_ts_ms,
                        );
                        if !self.book_dn.is_crossed()
                            && let Ok(top) = self.book_dn.top_n::<10>()
                        {
                            self.latest_dn = Some(top);
                        }
                    }
                }
                if side == OrderSide::Buy {
                    self.broker
                        .on_book_update(outcome, side, price, size, now_ms);
                }
            }
            PolyMarketEvent::LastTrade {
                outcome,
                side,
                price,
                size,
                ..
            } => {
                if outcome == Outcome::Up {
                    self.pending_up_trades.push(size);
                }
                let fills = self
                    .broker
                    .on_public_trade_with_side(outcome, side, price, size, now_ms);
                for fill in &fills {
                    self.process_fill(fill, now_ms, now_ns, jsonl)?;
                }
                self.telemetry
                    .naive_engine
                    .on_public_trade_with_side(outcome, side, price, size, now_ms);
            }
            PolyMarketEvent::TickSizeChange { new_tick_micro, .. } => {
                self.desc.tick_size_micro = new_tick_micro;
                self.pending_feedback
                    .push(RiskFeedback::TickSizeChange(new_tick_micro));
                self.pending_tick_sync_ack = true;
            }
            PolyMarketEvent::BestBidAsk { .. } => {}
        }
        let _ = self.publish_registry_snapshot();
        Ok(())
    }

    fn process_fill(
        &mut self,
        fill: &SimFill,
        now_ms: u64,
        now_ns: u64,
        jsonl: &mut JsonlWriter,
    ) -> Result<(), Box<dyn Error>> {
        self.stats.fills_count = self.stats.fills_count.saturating_add(1);
        print_lifecycle_fill(&self.desc.slug, fill);
        record_lifecycle_fill_jsonl(jsonl, now_ms, now_ns, &self.desc.slug, fill)?;

        let inv_before = self.ledger.net_inventory();
        if let Some(merge) = self.ledger.record_fill(fill)? {
            self.record_merge_event(&merge, now_ms, now_ns, jsonl)?;
        }
        let inv_after = self.ledger.net_inventory();

        let mid_at_fill = match fill.outcome {
            Outcome::Up => self
                .latest_up
                .as_ref()
                .and_then(fixed_book_mid)
                .unwrap_or(fill.price),
            Outcome::Down => self
                .latest_dn
                .as_ref()
                .and_then(fixed_book_mid)
                .unwrap_or(fill.price),
        };
        self.telemetry
            .record_fill(fill, inv_before, inv_after, mid_at_fill);

        self.pending_feedback.push(RiskFeedback::OnFill {
            outcome: fill.outcome,
            order_id: fill.sim_id.to_string(),
            filled_shares: fill.filled_shares,
        });
        Ok(())
    }

    fn record_merge_event(
        &mut self,
        merge: &MergeEvent,
        now_ms: u64,
        now_ns: u64,
        jsonl: &mut JsonlWriter,
    ) -> Result<(), Box<dyn Error>> {
        self.stats.merges_count = self.stats.merges_count.saturating_add(1);
        self.stats.total_pairs_merged = self.ledger.total_pairs_merged;
        let merged_cost = (u128::from(merge.pair_cost.0)
            .saturating_mul(u128::from(merge.merged_pairs.0))
            / u128::from(ONE_SHARE_MICRO)) as u64;
        self.cumulative_merged_cost_micro = self
            .cumulative_merged_cost_micro
            .saturating_add(merged_cost);
        print_lifecycle_merge(&self.desc.slug, merge, self.ledger.total_pairs_merged);
        record_lifecycle_merge_jsonl(
            jsonl,
            now_ms,
            now_ns,
            &self.desc.slug,
            merge,
            self.ledger.total_pairs_merged,
        )?;
        Ok(())
    }

    fn book_ref(&self, outcome: Outcome) -> &FixedBook<10> {
        match outcome {
            Outcome::Up => self.latest_up.as_ref().unwrap_or(&self.fallback_up),
            Outcome::Down => self.latest_dn.as_ref().unwrap_or(&self.fallback_dn),
        }
    }

    /// Enforces `--v-max` and `--base-size` limits consistently on resting order submissions.
    fn cap_order_size_for_leg(
        &self,
        outcome: Outcome,
        price: MicroUsdc,
        requested: SharesMicro,
    ) -> Option<SharesMicro> {
        if price.0 == 0 || requested.0 == 0 {
            return None;
        }
        let (unhedged_leg, unhedged_cost_leg, unhedged_opp) = match outcome {
            Outcome::Up => (
                self.ledger.filled_up,
                self.ledger.cost_basis_up_micro,
                self.ledger.filled_dn,
            ),
            Outcome::Down => (
                self.ledger.filled_dn,
                self.ledger.cost_basis_dn_micro,
                self.ledger.filled_up,
            ),
        };
        let is_reducing = unhedged_opp > unhedged_leg;
        let base_cap_shares = if is_reducing {
            ((self.base_size_shares as f64) * 2.5).floor() as u64
        } else {
            self.base_size_shares
        };
        let max_base = SharesMicro::from_raw(base_cap_shares.saturating_mul(ONE_SHARE_MICRO));

        let remaining_v_max = self.v_max_micro.0.saturating_sub(unhedged_cost_leg);
        let max_by_v_max_whole = remaining_v_max / price.0;
        let max_by_v_max =
            SharesMicro::from_raw(max_by_v_max_whole.saturating_mul(ONE_SHARE_MICRO));

        let q_max_whole = self.v_max_micro.0 / price.0;
        let q_max = SharesMicro::from_raw(q_max_whole.saturating_mul(ONE_SHARE_MICRO));
        let remaining_q_cap =
            SharesMicro::from_raw(q_max.as_u64().saturating_sub(unhedged_leg.as_u64()));

        let capped = requested
            .min(max_base)
            .min(max_by_v_max)
            .min(remaining_q_cap);
        if capped < self.desc.min_order_size_micro {
            None
        } else {
            Some(capped)
        }
    }

    fn submit_broker_order(
        &mut self,
        outcome: Outcome,
        price: MicroUsdc,
        requested_size: SharesMicro,
        now_ms: u64,
    ) -> Option<(u64, SharesMicro)> {
        let size = self.cap_order_size_for_leg(outcome, price, requested_size)?;
        let book = self.book_ref(outcome);
        let best_bid = book.best_bid().map(|l| l.price);
        let best_ask = book.best_ask().map(|l| l.price);
        let depth = fixed_book_depth_at(book, price);
        match self.broker.submit_new(NewOrderParams {
            outcome,
            side: OrderSide::Buy,
            price,
            size,
            now_ms,
            current_best_bid: best_bid,
            current_best_ask: best_ask,
            book_depth_at_price: depth,
        }) {
            Ok(sim_id) => Some((sim_id, size)),
            Err(_) => None,
        }
    }

    fn cancel_all_resting_orders(&mut self, now_ms: u64) -> Vec<(Outcome, u64)> {
        let mut active: Vec<(Outcome, u64)> = self
            .broker
            .orders()
            .values()
            .filter(|o| {
                matches!(
                    o.status,
                    SimOrderStatus::PendingPlace { .. } | SimOrderStatus::Live
                )
            })
            .map(|o| (o.outcome, o.sim_id))
            .collect();
        active.sort_by_key(|(_, sim_id)| *sim_id);
        let mut canceled = Vec::with_capacity(active.len());
        for (outcome, sim_id) in active {
            if self.broker.cancel(sim_id, now_ms).is_ok() {
                canceled.push((outcome, sim_id));
            }
        }
        canceled
    }

    fn cancel_all_and_record(
        &mut self,
        params: CancelAllParams<'_>,
        jsonl: &mut JsonlWriter,
    ) -> Result<usize, Box<dyn Error>> {
        let canceled = self.cancel_all_resting_orders(params.now_ms);
        let canceled_count = canceled.len();
        self.stats.orders_canceled = self.stats.orders_canceled.saturating_add(canceled_count);
        let telemetry = params
            .r_trace
            .map(OrderActionTelemetry::from_trace)
            .unwrap_or_default();

        for (outcome, sim_id) in canceled {
            let id_str = sim_id.to_string();
            let already_in_trace = params.r_trace.is_some_and(|t| {
                let act = match outcome {
                    Outcome::Up => &t.action_up,
                    Outcome::Down => &t.action_dn,
                };
                matches!(act, OrderAction::Cancel { order_id, .. } if *order_id == id_str)
            });
            if !already_in_trace {
                record_order_action_jsonl(
                    jsonl,
                    params.now_ms,
                    params.now_ns,
                    outcome,
                    &OrderAction::Cancel {
                        outcome,
                        order_id: id_str,
                        reason: params.cancel_reason,
                    },
                    params.tau_secs,
                    telemetry,
                )?;
            }
        }

        if canceled_count > 0 {
            record_cancel_all_jsonl(
                jsonl,
                params.now_ms,
                params.now_ns,
                CancelAllRecord {
                    market_slug: &self.desc.slug,
                    phase: params.r_trace.map(|t| t.phase).or(self.current_phase),
                    tau_secs: params.tau_secs,
                    canceled_count,
                    flatten_in_flight: params.r_trace.map(|t| t.flatten_in_flight),
                    best_bid_up: params.r_trace.and_then(|t| t.best_bid_up),
                    best_ask_up: params.r_trace.and_then(|t| t.best_ask_up),
                    best_bid_dn: params.r_trace.and_then(|t| t.best_bid_dn),
                    best_ask_dn: params.r_trace.and_then(|t| t.best_ask_dn),
                    reason: params.reason_str,
                },
            )?;
        }

        Ok(canceled_count)
    }

    fn cancel_leg_order(&mut self, outcome: Outcome, order_id: &str, now_ms: u64) -> bool {
        let sim_id_opt = order_id.parse::<u64>().ok().or_else(|| {
            self.broker
                .active_order(outcome, OrderSide::Buy)
                .map(|o| o.sim_id)
        });
        if let Some(sim_id) = sim_id_opt {
            if let Some(order) = self.broker.orders().get(&sim_id)
                && matches!(
                    order.status,
                    SimOrderStatus::PendingCancel { .. }
                        | SimOrderStatus::PendingPlaceThenCancel { .. }
                        | SimOrderStatus::Cancelled
                )
            {
                return false;
            }
            return self.broker.cancel(sim_id, now_ms).is_ok();
        }
        false
    }

    fn execute_commands(
        &mut self,
        now_ms: u64,
        now_ns: u64,
        tau_secs: u64,
        r_trace: Option<&RiskActorTrace>,
        jsonl: &mut JsonlWriter,
    ) -> Result<(), Box<dyn Error>> {
        let telemetry = r_trace
            .map(OrderActionTelemetry::from_trace)
            .unwrap_or_default();

        while let Ok(cmd) = self.priority_rx.try_recv() {
            match cmd {
                ExecutionCommand::CancelAll { reason } => {
                    let cancel_reason = if r_trace
                        .is_some_and(|t| matches!(t.breaker, BreakerStatus::Tripped { .. }))
                    {
                        CancelReason::CircuitBreaker
                    } else {
                        CancelReason::LifecycleLockout
                    };
                    let _ = self.cancel_all_and_record(
                        CancelAllParams {
                            now_ms,
                            now_ns,
                            tau_secs,
                            cancel_reason,
                            reason_str: &reason,
                            r_trace,
                        },
                        jsonl,
                    )?;
                    self.pending_feedback.push(RiskFeedback::OnCancelAck {
                        outcome: Outcome::Up,
                    });
                    self.pending_feedback.push(RiskFeedback::OnCancelAck {
                        outcome: Outcome::Down,
                    });
                }
                ExecutionCommand::Cancel {
                    outcome, order_id, ..
                } => {
                    if self.cancel_leg_order(outcome, &order_id, now_ms) {
                        self.stats.orders_canceled = self.stats.orders_canceled.saturating_add(1);
                    }
                    self.pending_feedback
                        .push(RiskFeedback::OnCancelAck { outcome });
                }
                ExecutionCommand::ExecuteFlattenFak {
                    outcome,
                    price,
                    size,
                    reason,
                } => {
                    let _ = self.cancel_all_and_record(
                        CancelAllParams {
                            now_ms,
                            now_ns,
                            tau_secs,
                            cancel_reason: CancelReason::LifecycleLockout,
                            reason_str: &reason,
                            r_trace,
                        },
                        jsonl,
                    )?;
                    self.pending_feedback.push(RiskFeedback::OnCancelAck {
                        outcome: Outcome::Up,
                    });
                    self.pending_feedback.push(RiskFeedback::OnCancelAck {
                        outcome: Outcome::Down,
                    });
                    self.pending_feedback
                        .push(RiskFeedback::ClearFlattenInFlight);

                    let (target_shares, opposite_shares) = match outcome {
                        Outcome::Up => (self.ledger.filled_up, self.ledger.filled_dn),
                        Outcome::Down => (self.ledger.filled_dn, self.ledger.filled_up),
                    };
                    let net_delta = target_shares.0.abs_diff(opposite_shares.0);
                    let flatten_shares = size.0.min(net_delta);
                    self.stats.flattens_executed = self.stats.flattens_executed.saturating_add(1);
                    if flatten_shares > 0 {
                        let side = if target_shares > opposite_shares {
                            OrderSide::Sell
                        } else {
                            OrderSide::Buy
                        };
                        let (fee, merge_opt) = self.ledger.record_taker_flatten_with_merge(
                            outcome,
                            side,
                            price,
                            SharesMicro(flatten_shares),
                        )?;
                        if let Some(merge) = merge_opt {
                            self.record_merge_event(&merge, now_ms, now_ns, jsonl)?;
                        }
                        print_lifecycle_flatten(
                            &self.desc.slug,
                            outcome,
                            side,
                            price,
                            SharesMicro(flatten_shares),
                            fee,
                            &reason,
                        );
                        record_lifecycle_flatten_jsonl(
                            jsonl,
                            now_ms,
                            now_ns,
                            &self.desc.slug,
                            outcome,
                            side,
                            price,
                            SharesMicro(flatten_shares),
                            fee.0,
                            self.ledger.realized_pnl_micro,
                            &reason,
                        )?;
                    }
                }
                _ => {}
            }
        }

        while let Ok(cmd) = self.routine_rx.try_recv() {
            match cmd {
                ExecutionCommand::SubmitNew {
                    outcome,
                    price,
                    size,
                } => {
                    if let Some(existing_id) = self
                        .broker
                        .active_order(outcome, OrderSide::Buy)
                        .map(|o| o.sim_id)
                        && self.broker.cancel(existing_id, now_ms).is_ok()
                    {
                        self.stats.orders_canceled = self.stats.orders_canceled.saturating_add(1);
                        record_order_action_jsonl(
                            jsonl,
                            now_ms,
                            now_ns,
                            outcome,
                            &OrderAction::Cancel {
                                outcome,
                                order_id: existing_id.to_string(),
                                reason: CancelReason::RiskSuppressed,
                            },
                            tau_secs,
                            telemetry,
                        )?;
                    }
                    if let Some((sim_id, capped_size)) =
                        self.submit_broker_order(outcome, price, size, now_ms)
                    {
                        self.stats.orders_submitted = self.stats.orders_submitted.saturating_add(1);
                        self.telemetry.record_order_posted(capped_size);
                        self.pending_feedback.push(RiskFeedback::OnPlaceAck {
                            outcome,
                            order_id: sim_id.to_string(),
                            price,
                            size: capped_size,
                        });
                    } else {
                        self.pending_feedback
                            .push(RiskFeedback::OnCancelAck { outcome });
                    }
                }
                ExecutionCommand::CancelAndReplace {
                    outcome,
                    old_order_id,
                    new_price,
                    new_size,
                    reason,
                } => {
                    let canceled_old = self.cancel_leg_order(outcome, &old_order_id, now_ms);
                    self.telemetry.record_cancel_replace(now_ms);
                    self.pending_feedback
                        .push(RiskFeedback::OnCancelAck { outcome });
                    if let Some((new_id, capped_size)) =
                        self.submit_broker_order(outcome, new_price, new_size, now_ms)
                    {
                        self.stats.orders_replaced = self.stats.orders_replaced.saturating_add(1);
                        self.telemetry.record_order_posted(capped_size);
                        self.pending_feedback.push(RiskFeedback::OnPlaceAck {
                            outcome,
                            order_id: new_id.to_string(),
                            price: new_price,
                            size: capped_size,
                        });
                    } else if canceled_old {
                        self.stats.orders_canceled = self.stats.orders_canceled.saturating_add(1);
                        record_order_action_jsonl(
                            jsonl,
                            now_ms,
                            now_ns,
                            outcome,
                            &OrderAction::Cancel {
                                outcome,
                                order_id: old_order_id,
                                reason,
                            },
                            tau_secs,
                            telemetry,
                        )?;
                    }
                }
                ExecutionCommand::Cancel {
                    outcome, order_id, ..
                } => {
                    if self.cancel_leg_order(outcome, &order_id, now_ms) {
                        self.stats.orders_canceled = self.stats.orders_canceled.saturating_add(1);
                    }
                    self.pending_feedback
                        .push(RiskFeedback::OnCancelAck { outcome });
                }
                _ => {}
            }
        }

        let _ = self.publish_registry_snapshot();
        Ok(())
    }

    async fn step_tick(
        &mut self,
        now_ms: u64,
        now_ns: u64,
        clock_offset_ms: i64,
        actors: &LiveActorChannels<'_>,
        latencies: &mut LiveLatencies<'_>,
        jsonl: &mut JsonlWriter,
    ) -> Result<(), Box<dyn Error>> {
        self.broker.update_time(now_ms);
        let _ = self.publish_registry_snapshot();

        let snap_up = *self.latest_up.as_ref().unwrap_or(&self.fallback_up);
        let snap_dn = *self.latest_dn.as_ref().unwrap_or(&self.fallback_dn);
        let inventory_q = self.ledger.net_inventory();
        let active_bid_up = self
            .broker
            .active_order(Outcome::Up, OrderSide::Buy)
            .map(|o| o.price);
        let active_bid_dn = self
            .broker
            .active_order(Outcome::Down, OrderSide::Buy)
            .map(|o| o.price);

        self.step_count = self.step_count.saturating_add(1);
        let pricing_job = PricingJob {
            seq: self.step_count,
            now_ms,
            expiry_ms: self.desc.expiry_ts_ms,
            tick_size_micro: self.desc.tick_size_micro,
            fee_schedule: self.desc.fee_schedule,
            inventory_q,
            book_up: snap_up,
            book_dn: snap_dn,
            spot_usd: self.latest_spot,
            strike_usd: self.strike_usd,
            active_bid_up,
            active_bid_dn,
            cost_context: CostContext {
                avg_cost_up: self.ledger.avg_cost_up(),
                avg_cost_dn: self.ledger.avg_cost_dn(),
                allow_cross_tick_override: risk_lifecycle::allows_cross_tick_override(
                    self.current_tier,
                    self.current_phase,
                ),
            },
            spot_ticks: std::mem::take(&mut self.pending_spot_ticks),
            up_trade_sizes: std::mem::take(&mut self.pending_up_trades),
            reset_epoch: std::mem::take(&mut self.pending_reset_epoch),
        };

        let (pricing_ack_tx, pricing_ack_rx) = oneshot::channel();
        actors
            .pricing_tx
            .send((pricing_job, pricing_ack_tx))
            .await
            .map_err(|_| "Pricing actor channel closed unexpectedly")?;
        let pricing_ack = pricing_ack_rx
            .await
            .map_err(|_| "Pricing actor dropped ack channel")?;
        if let Some(err) = pricing_ack.error {
            return Err(format!("Pricing actor failed: {err}").into());
        }
        if let Some(twap) = pricing_ack.twap_60s {
            self.latest_twap_60s = Some(twap);
        }
        latencies.pricing.record_ns(pricing_ack.total_compute_ns)?;

        if self.pending_tick_sync_ack {
            self.pending_feedback.push(RiskFeedback::AckTickSizeSync);
            self.pending_tick_sync_ack = false;
        }

        let risk_job = RiskJob {
            now_ns,
            clock_offset_ms,
            feedback: std::mem::take(&mut self.pending_feedback),
        };
        let (risk_ack_tx, risk_ack_rx) = oneshot::channel();
        actors
            .risk_tx
            .send((risk_job, risk_ack_tx))
            .await
            .map_err(|_| "Risk actor channel closed unexpectedly")?;
        let risk_ack = risk_ack_rx
            .await
            .map_err(|_| "Risk actor dropped ack channel")?;
        if let Some(err) = risk_ack.error {
            return Err(format!("Risk actor failed: {err}").into());
        }
        let risk_ns = risk_ack.risk_ns;
        latencies.risk.record_ns(risk_ns)?;
        latencies
            .total
            .record_ns(pricing_ack.total_compute_ns.saturating_add(risk_ns))?;

        let tau_secs = self.desc.expiry_ts_ms.saturating_sub(now_ms) / 1000;
        self.execute_commands(now_ms, now_ns, tau_secs, risk_ack.trace.as_ref(), jsonl)?;

        let mid_up = fixed_book_mid(&snap_up);
        let mid_dn = fixed_book_mid(&snap_dn);
        if let (Some(m_up), Some(m_dn)) = (mid_up, mid_dn) {
            self.telemetry.update_markouts(now_ms, m_up, m_dn);
        }
        let tick_prec =
            TickPrecision::from_micro(self.desc.tick_size_micro).unwrap_or(TickPrecision::Cent);
        let best_ask_up = snap_up.best_ask().map(|l| l.price);
        let best_ask_dn = snap_dn.best_ask().map(|l| l.price);
        let naive_up_px =
            paper_sim::NaiveBaselineEngine::target_bid_price(mid_up, best_ask_up, tick_prec);
        let naive_dn_px =
            paper_sim::NaiveBaselineEngine::target_bid_price(mid_dn, best_ask_dn, tick_prec);
        self.telemetry.naive_engine.update_quotes(NaiveQuotesInput {
            mid_up,
            mid_dn,
            best_bid_up: snap_up.best_bid().map(|l| l.price),
            best_ask_up,
            best_bid_dn: snap_dn.best_bid().map(|l| l.price),
            best_ask_dn,
            tick: tick_prec,
            depth_up: naive_up_px.map_or(SharesMicro::ZERO, |px| fixed_book_depth_at(&snap_up, px)),
            depth_dn: naive_dn_px.map_or(SharesMicro::ZERO, |px| fixed_book_depth_at(&snap_dn, px)),
            now_ms,
        });

        let (desired_bid_up, desired_bid_dn) = match pricing_ack.quote {
            QuoteOutput::Active { bid_up, bid_dn, .. } => (bid_up, bid_dn),
            QuoteOutput::Suppressed(_) => (None, None),
        };
        self.telemetry.record_trajectory(TrajectorySnapshot {
            ts_ms: now_ms,
            tau_secs: (self.desc.expiry_ts_ms.saturating_sub(now_ms) as f64) / 1000.0,
            s_star: pricing_ack.s_star,
            r_i: pricing_ack.r_i,
            bid_up_raw: pricing_ack.bid_up_raw,
            bid_up: desired_bid_up,
            bid_dn: desired_bid_dn,
            q_inventory: self.ledger.net_inventory().as_i64(),
        });

        if let Some(r_trace) = risk_ack.trace {
            self.stats.ticks_evaluated = self.stats.ticks_evaluated.saturating_add(1);
            if matches!(r_trace.action_up, OrderAction::Hold)
                && self
                    .broker
                    .active_order(Outcome::Up, OrderSide::Buy)
                    .is_some()
            {
                self.stats.orders_held = self.stats.orders_held.saturating_add(1);
            }
            if matches!(r_trace.action_dn, OrderAction::Hold)
                && self
                    .broker
                    .active_order(Outcome::Down, OrderSide::Buy)
                    .is_some()
            {
                self.stats.orders_held = self.stats.orders_held.saturating_add(1);
            }

            if self.current_phase != Some(r_trace.phase) {
                self.stats.phase_transitions = self.stats.phase_transitions.saturating_add(1);
                print_phase_transition(
                    self.current_phase,
                    r_trace.phase,
                    tau_secs,
                    "Live epoch horizon",
                );
                record_phase_transition_jsonl(
                    jsonl,
                    now_ms,
                    now_ns,
                    self.current_phase,
                    r_trace.phase,
                    tau_secs,
                    "Live epoch horizon transition",
                )?;
                self.current_phase = Some(r_trace.phase);
            }

            if self.current_tier != r_trace.tier {
                print_tier_transition(
                    r_trace.tier,
                    r_trace.utilization_u_e6,
                    r_trace.flatten_action.outcome(),
                );
                record_risk_tier_jsonl(
                    jsonl,
                    now_ms,
                    now_ns,
                    r_trace.tier,
                    r_trace.utilization_u_e6,
                    r_trace.flatten_action.outcome(),
                )?;
                self.current_tier = r_trace.tier;
            }

            if let BreakerStatus::Tripped { .. } = r_trace.breaker {
                self.stats.breaker_trips = self.stats.breaker_trips.saturating_add(1);
                print_breaker_trip(r_trace.breaker);
                record_breaker_jsonl(jsonl, now_ms, now_ns, r_trace.breaker, clock_offset_ms)?;
            }

            let resting_bid_up = self
                .broker
                .active_order(Outcome::Up, OrderSide::Buy)
                .map(|o| o.price);
            let resting_bid_dn = self
                .broker
                .active_order(Outcome::Down, OrderSide::Buy)
                .map(|o| o.price);
            let m_up = mid_up.unwrap_or(MicroUsdc(500_000));
            let m_dn = mid_dn.unwrap_or(MicroUsdc(500_000));
            let net_pnl_micro = self.ledger.net_pnl_micro(m_up, m_dn);
            let net_delta = self.ledger.net_inventory().as_i64() / ONE_SHARE_MICRO as i64;
            self.reporter.on_step(&LifecycleStepRow {
                now_ms,
                tau_secs,
                phase: r_trace.phase,
                tier: r_trace.tier,
                u_e6: r_trace.utilization_u_e6,
                q_up: self.ledger.filled_up,
                q_dn: self.ledger.filled_dn,
                net_delta,
                quote: pricing_ack.quote,
                spot_usd: self.latest_spot,
                strike_usd: self.strike_usd,
                resting_bid_up,
                resting_bid_dn,
                net_pnl_micro,
                action_up: &r_trace.action_up,
                action_dn: &r_trace.action_dn,
                cancel_all_reason: r_trace.cancel_all_reason.as_deref(),
                flatten_in_flight: r_trace.flatten_in_flight,
                best_bid_up: r_trace.best_bid_up,
                best_ask_up: r_trace.best_ask_up,
                best_bid_dn: r_trace.best_bid_dn,
                best_ask_dn: r_trace.best_ask_dn,
                pricing_ns: pricing_ack.total_compute_ns,
                risk_ns,
            });

            let action_telemetry = OrderActionTelemetry::from_trace(&r_trace);
            record_order_action_jsonl(
                jsonl,
                now_ms,
                now_ns,
                Outcome::Up,
                &r_trace.action_up,
                tau_secs,
                action_telemetry,
            )?;
            record_order_action_jsonl(
                jsonl,
                now_ms,
                now_ns,
                Outcome::Down,
                &r_trace.action_dn,
                tau_secs,
                action_telemetry,
            )?;
        }

        Ok(())
    }

    fn resolve_winning_outcome(&self) -> Outcome {
        if let (Some(ref_px), Some(strike)) =
            (self.latest_twap_60s.or(self.latest_spot), self.strike_usd)
        {
            if ref_px >= strike {
                return Outcome::Up;
            }
            return Outcome::Down;
        }
        let mid_up = self
            .latest_up
            .as_ref()
            .and_then(fixed_book_mid)
            .unwrap_or(MicroUsdc(500_000));
        let mid_dn = self
            .latest_dn
            .as_ref()
            .and_then(fixed_book_mid)
            .unwrap_or(MicroUsdc(500_000));
        if mid_up >= mid_dn {
            Outcome::Up
        } else {
            Outcome::Down
        }
    }

    fn settle_expired_epoch_if_needed(
        &mut self,
        now_ms: u64,
        now_ns: u64,
        jsonl: &mut JsonlWriter,
    ) -> Result<(), Box<dyn Error>> {
        if self.epoch_settled {
            return Ok(());
        }
        let tau_secs = self.desc.expiry_ts_ms.saturating_sub(now_ms) / 1000;
        let _ = self.cancel_all_and_record(
            CancelAllParams {
                now_ms,
                now_ns,
                tau_secs,
                cancel_reason: CancelReason::LifecycleLockout,
                reason_str: "Epoch expiry settlement cancel",
                r_trace: None,
            },
            jsonl,
        )?;
        let winning_outcome = self.resolve_winning_outcome();
        let settle_delta = self.ledger.settle_epoch(winning_outcome);
        let _ = self
            .telemetry
            .naive_engine
            .ledger
            .settle_epoch(winning_outcome);
        self.stats.epochs_completed = self.stats.epochs_completed.saturating_add(1);
        self.stats.settlement_pnl_micro =
            self.stats.settlement_pnl_micro.saturating_add(settle_delta);
        self.stats.realized_pnl_micro = self.ledger.realized_pnl_micro;
        self.epoch_settled = true;
        self.pending_settlement = Some(PendingEpochSettlement {
            expired_slug: self.desc.slug.clone(),
            winning_outcome,
            settle_delta_micro: settle_delta,
            total_realized_pnl_micro: self.ledger.realized_pnl_micro,
            pairs_merged_micro: self.ledger.total_pairs_merged.0,
            fees_paid_micro: self.ledger.fees_paid_micro,
            estimated_rebate_micro: self.ledger.estimated_rebate_micro,
        });
        let _ = self.publish_registry_snapshot();
        Ok(())
    }

    fn rollover_to_market(
        &mut self,
        next_desc: MarketDescriptor,
        now_ms: u64,
        now_ns: u64,
        jsonl: &mut JsonlWriter,
    ) -> Result<(), Box<dyn Error>> {
        self.settle_expired_epoch_if_needed(now_ms, now_ns, jsonl)?;
        if let Some(pending) = self.pending_settlement.take() {
            print_epoch_rollover(
                &pending.expired_slug,
                Some(&next_desc.slug),
                pending.winning_outcome,
                pending.settle_delta_micro,
                pending.total_realized_pnl_micro,
            );
            record_epoch_rollover_jsonl(
                jsonl,
                now_ms,
                now_ns,
                &pending.expired_slug,
                Some(&next_desc.slug),
                pending.winning_outcome,
                pending.settle_delta_micro,
                pending.total_realized_pnl_micro,
                pending.pairs_merged_micro,
                pending.fees_paid_micro,
                pending.estimated_rebate_micro,
            )?;
        }

        let broker_cfg = *self.broker.config();
        self.broker = FifoPaperBroker::new(broker_cfg);
        self.telemetry.naive_engine.broker = FifoPaperBroker::new(broker_cfg);
        self.telemetry.naive_engine.active_up_id = None;
        self.telemetry.naive_engine.active_dn_id = None;
        self.telemetry
            .naive_engine
            .ledger
            .set_fee_schedule(next_desc.fee_schedule);
        self.ledger.set_fee_schedule(next_desc.fee_schedule);

        self.book_up = BookMaintainer::new(Outcome::Up);
        self.book_dn = BookMaintainer::new(Outcome::Down);
        self.latest_up = None;
        self.latest_dn = None;
        self.latest_twap_60s = None;
        self.strike_usd = next_desc
            .strike_price_usd
            .map(|f| f.get())
            .or(self.latest_spot);
        self.pending_spot_ticks.clear();
        self.pending_up_trades.clear();
        self.pending_reset_epoch = true;
        self.pending_tick_sync_ack = false;
        self.current_phase = None;
        self.current_tier = RiskTier::Tier1Normal;
        self.market_resolved_flag = false;
        self.epoch_settled = false;
        self.stats.epoch_rollovers = self.stats.epoch_rollovers.saturating_add(1);
        self.pending_feedback.push(RiskFeedback::ResetEpoch {
            new_tick_size: next_desc.tick_size_micro,
            now_ns,
        });
        self.desc = next_desc;
        self.epoch_num = self.epoch_num.saturating_add(1);
        self.reporter.reset_epoch();
        print_epoch_banner(
            self.epoch_num,
            &self.desc.slug,
            self.strike_usd,
            self.desc.start_ts_ms,
            self.desc.expiry_ts_ms,
        );
        let _ = self.publish_registry_snapshot();
        Ok(())
    }

    fn finalize_stats(
        &mut self,
        now_ms: u64,
        now_ns: u64,
        jsonl: &mut JsonlWriter,
    ) -> Result<(), Box<dyn Error>> {
        let tau_secs = self.desc.expiry_ts_ms.saturating_sub(now_ms) / 1000;
        let _ = self.cancel_all_and_record(
            CancelAllParams {
                now_ms,
                now_ns,
                tau_secs,
                cancel_reason: CancelReason::LifecycleLockout,
                reason_str: "Session shutdown cancel",
                r_trace: None,
            },
            jsonl,
        )?;
        if let Some(pending) = self.pending_settlement.take() {
            print_epoch_rollover(
                &pending.expired_slug,
                None,
                pending.winning_outcome,
                pending.settle_delta_micro,
                pending.total_realized_pnl_micro,
            );
            record_epoch_rollover_jsonl(
                jsonl,
                now_ms,
                now_ns,
                &pending.expired_slug,
                None,
                pending.winning_outcome,
                pending.settle_delta_micro,
                pending.total_realized_pnl_micro,
                pending.pairs_merged_micro,
                pending.fees_paid_micro,
                pending.estimated_rebate_micro,
            )?;
        }

        let mid_up = self
            .latest_up
            .as_ref()
            .and_then(fixed_book_mid)
            .unwrap_or(MicroUsdc(500_000));
        let mid_dn = self
            .latest_dn
            .as_ref()
            .and_then(fixed_book_mid)
            .unwrap_or(MicroUsdc(500_000));

        self.stats.realized_pnl_micro = self.ledger.realized_pnl_micro;
        self.stats.unrealized_pnl_micro = self.ledger.unrealized_pnl_micro(mid_up, mid_dn);
        self.stats.fees_paid_micro = self.ledger.fees_paid_micro;
        self.stats.estimated_rebate_micro = self.ledger.estimated_rebate_micro;
        self.stats.total_pairs_merged = self.ledger.total_pairs_merged;
        self.stats
            .set_final_inventory(self.ledger.filled_up, self.ledger.filled_dn);
        self.stats.rpm_60s = self.telemetry.rpm_60s(now_ms);
        self.stats.fill_ratio = self.telemetry.fill_ratio();
        self.stats.markout_1s_bps = self.telemetry.avg_markout_1s_bps();
        self.stats.markout_5s_bps = self.telemetry.avg_markout_5s_bps();
        self.stats.baseline_pnl_micro = self
            .telemetry
            .naive_engine
            .ledger
            .net_pnl_micro(mid_up, mid_dn);
        Ok(())
    }
}

fn build_risk_actor(
    harness: ActorPipelineHarness,
    v_max_micro: MicroUsdc,
    base_size_shares: u64,
    min_order_size_shares: u64,
    initial_tick_size: MicroUsdc,
    now_ns: u64,
    fault: FaultInjection,
) -> Result<RiskActor, Box<dyn Error>> {
    let risk_config = RiskConfig {
        v_max_micro,
        base_order_size_shares: base_size_shares,
        min_order_size_shares,
        ..Default::default()
    };
    let risk_engine = DollarNotionalRiskEngine::new(risk_config)?;
    let hysteresis = HysteresisGate::new(HysteresisConfig {
        hysteresis_buf: 0.25,
        min_order_size_shares,
    })?;
    let lifecycle = EpochLifecycleController::new(LifecycleConfig {
        pre_expiry_lockout_secs: 60,
        mandatory_flatten_secs: 30,
        terminal_lockout_secs: 15,
        min_order_size_shares,
    })?;
    let breakers = CircuitBreakers::new(BreakerConfig::default(), initial_tick_size, now_ns);
    Ok(RiskActor::new(
        harness,
        risk_engine,
        hysteresis,
        lifecycle,
        breakers,
        fault,
    ))
}

struct ManagedLifecycleSession {
    sim: LivePaperSimulator,
    supervisor: ActorSupervisor,
    actor_shutdown: Arc<AtomicBool>,
    pricing_tx: mpsc::Sender<(PricingJob, oneshot::Sender<PricingAck>)>,
    risk_tx: mpsc::Sender<(RiskJob, oneshot::Sender<RiskAck>)>,
}

impl ManagedLifecycleSession {
    fn shutdown_returning_sim(self) -> Result<LivePaperSimulator, Box<dyn Error>> {
        self.actor_shutdown.store(true, Ordering::Release);
        drop(self.pricing_tx);
        drop(self.risk_tx);
        self.supervisor.join()?;
        Ok(self.sim)
    }
}

fn spawn_managed_session(
    desc: MarketDescriptor,
    cfg: &EngineConfig,
    args: &LifecycleArgs,
    initial_now_ns: u64,
    clock: MonotonicClock,
) -> Result<ManagedLifecycleSession, Box<dyn Error>> {
    let (harness, priority_rx, routine_rx) = ActorPipelineHarness::new(64, 16);
    let v_max_micro = MicroUsdc(args.v_max.saturating_mul(1_000_000));
    let min_order_shares = (desc.min_order_size_micro.as_u64() / ONE_SHARE_MICRO).max(1);

    let risk_actor = build_risk_actor(
        harness.clone(),
        v_max_micro,
        args.base_size,
        min_order_shares,
        desc.tick_size_micro,
        initial_now_ns,
        args.fault.into(),
    )?;

    let pipe_cfg = MathPipelineConfig::from_engine_config(cfg)?;
    let math_pipeline = MathPipeline::new(pipe_cfg, clock)?;

    let broker_cfg = FifoBrokerConfig {
        queue_decay_policy: args.queue_decay,
        ..FifoBrokerConfig::default()
    };
    let sim = LivePaperSimulator::new(
        desc,
        broker_cfg,
        v_max_micro,
        args.base_size,
        args.display,
        harness.clone(),
        priority_rx,
        routine_rx,
    )?;

    let mut supervisor = ActorSupervisor::new(harness);
    let actor_shutdown = supervisor.shutdown_flag();
    let (pricing_tx, pricing_rx) = mpsc::channel::<(PricingJob, oneshot::Sender<PricingAck>)>(4);
    let (risk_tx, risk_rx) = mpsc::channel::<(RiskJob, oneshot::Sender<RiskAck>)>(4);

    let pricing_harness = sim.harness.clone();
    let pricing_shutdown = Arc::clone(&actor_shutdown);
    supervisor.spawn("pricing", args.pinning.pricing_core, move || {
        pricing_actor_loop(math_pipeline, pricing_harness, pricing_rx, pricing_shutdown);
    })?;

    let risk_shutdown = Arc::clone(&actor_shutdown);
    supervisor.spawn("risk", args.pinning.risk_core, move || {
        risk_actor_loop(risk_actor, risk_rx, risk_shutdown);
    })?;

    Ok(ManagedLifecycleSession {
        sim,
        supervisor,
        actor_shutdown,
        pricing_tx,
        risk_tx,
    })
}

fn finalize_and_report_session(
    sim: &mut LivePaperSimulator,
    final_ms: u64,
    final_ns: u64,
    source_tag: &str,
    (pricing_lat, risk_lat, total_lat): (
        &mut LatencyTracker,
        &mut LatencyTracker,
        &mut LatencyTracker,
    ),
    mut jsonl: JsonlWriter,
) -> Result<(), Box<dyn Error>> {
    sim.finalize_stats(final_ms, final_ns, &mut jsonl)?;
    let p_summary = pricing_lat.summary();
    let r_summary = risk_lat.summary();
    let t_summary = total_lat.summary();

    print_lifecycle_summary(&sim.stats, &p_summary, &r_summary, &t_summary);
    record_lifecycle_summary_jsonl(
        &mut jsonl,
        final_ms,
        final_ns,
        source_tag,
        &sim.desc.slug,
        &sim.stats,
        &p_summary,
        &r_summary,
        &t_summary,
    )?;
    jsonl.close()?;
    Ok(())
}

async fn run_live_epoch(args: LifecycleArgs, clock: MonotonicClock) -> Result<(), Box<dyn Error>> {
    let mut cfg = EngineConfig::from_env()?;
    args.math.apply_to(&mut cfg)?;
    feeds::install_tls_provider();

    let duration_limit = parse_duration_arg(&args.duration)?;

    let t = theme::current();
    println!(
        "  {} Discovering active Polymarket 5-minute BTC market...",
        t.badge(Badge::Info)
    );
    let fresh = match crate::window_gate::wait_and_discover_fresh_5m_market(
        &cfg,
        args.allow_mid_window,
        15_000,
    )
    .await?
    {
        crate::window_gate::GateOutcome::Ready(fresh) => fresh,
        crate::window_gate::GateOutcome::Cancelled => return Ok(()),
    };
    let crate::window_gate::FreshWindowDiscovery {
        discovery,
        offset_est,
        desc,
        ..
    } = fresh;

    let mut jsonl = JsonlWriter::create(&args.jsonl)?;
    let mut pricing_lat = LatencyTracker::new()?;
    let mut risk_lat = LatencyTracker::new()?;
    let mut total_lat = LatencyTracker::new()?;
    let base_raw_ms = MonotonicClock::unix_now_ms()?;
    let base_adj_ms = offset_est.adjusted_unix_ms(base_raw_ms)?;
    let base_mono_ns = clock.now_ns()?;
    println!(
        "  {} Discovered: {} (expires in {}s)",
        t.badge(Badge::Ready),
        t.cyan(&desc.slug),
        desc.expiry_ts_ms.saturating_sub(base_adj_ms) / 1000
    );

    let (poly_prod, mut poly_cons) = RingBuffer::new(DEFAULT_RING_CAPACITY);
    let (bin_prod, mut binance_cons) = RingBuffer::new(DEFAULT_RING_CAPACITY);
    let (ora_prod, mut oracle_cons) = RingBuffer::new(DEFAULT_RING_CAPACITY);
    let (health_tx, mut health_rx) = mpsc::channel(1024);

    let handles = spawn_actors(
        &cfg,
        &desc,
        &discovery,
        clock,
        (poly_prod, bin_prod, ora_prod),
        health_tx,
    )?;

    let mut sess = spawn_managed_session(desc, &cfg, &args, base_mono_ns, clock)?;

    println!(
        "  {} Actors: {} | local 8-station FIFO paper simulator armed (V_max=${}, base={}sh, 40ms wire latency)",
        t.badge(Badge::Info),
        args.pinning.summary(),
        args.v_max,
        args.base_size
    );
    print_epoch_banner(
        sess.sim.epoch_num,
        &sess.sim.desc.slug,
        sess.sim.strike_usd,
        sess.sim.desc.start_ts_ms,
        sess.sim.desc.expiry_ts_ms,
    );

    let deadline = tokio::time::Instant::now() + duration_limit;
    let sleep_deadline = tokio::time::sleep_until(deadline);
    tokio::pin!(sleep_deadline);
    let ctrl_c = tokio::signal::ctrl_c();
    tokio::pin!(ctrl_c);
    let mut interval = tokio::time::interval(Duration::from_millis(100));
    let mut last_rollover_probe: Option<tokio::time::Instant> = None;
    let mut loop_error: Option<String> = None;

    loop {
        tokio::select! {
            _ = &mut ctrl_c => {
                println!(
                    "\n  {} Ctrl+C received, shutting down live 8-station simulator...",
                    t.badge(Badge::Signal)
                );
                break;
            }
            _ = &mut sleep_deadline => {
                println!(
                    "\n  {} Reached target duration {}, shutting down...",
                    t.badge(Badge::Info),
                    args.duration
                );
                break;
            }
            _ = interval.tick() => {
                let now_ns = clock.now_ns()?;
                let now_ms = base_adj_ms.saturating_add(now_ns.saturating_sub(base_mono_ns) / 1_000_000);
                let clock_offset_ms = offset_est.offset_ms().unwrap_or(0);

                while let Ok(ev) = binance_cons.pop() {
                    let rx_ns = match ev {
                        BinanceSpotEvent::BookTicker { rx_mono_ns, .. }
                        | BinanceSpotEvent::AggTrade { rx_mono_ns, .. } => rx_mono_ns,
                    };
                    let ev_ts_ms = base_adj_ms.saturating_add(rx_ns.saturating_sub(base_mono_ns) / 1_000_000);
                    sess.sim.on_binance_event(ev, ev_ts_ms);
                }
                while let Ok(ev) = oracle_cons.pop() {
                    sess.sim.on_oracle_event(ev);
                }
                while let Ok(ev) = poly_cons.pop() {
                    sess.sim.on_poly_event(ev, now_ms, now_ns, &mut jsonl)?;
                }
                while let Ok(ev) = health_rx.try_recv() {
                    sess.sim.on_health_event(ev);
                }

                let mut latencies = LiveLatencies {
                    pricing: &mut pricing_lat,
                    risk: &mut risk_lat,
                    total: &mut total_lat,
                };
                let actors = LiveActorChannels {
                    pricing_tx: &sess.pricing_tx,
                    risk_tx: &sess.risk_tx,
                };
                if let Err(err) = sess
                    .sim
                    .step_tick(
                        now_ms,
                        now_ns,
                        clock_offset_ms,
                        &actors,
                        &mut latencies,
                        &mut jsonl,
                    )
                    .await
                {
                    loop_error = Some(err.to_string());
                    break;
                }

                let tau_secs = sess.sim.desc.expiry_ts_ms.saturating_sub(now_ms) / 1000;
                if now_ms >= sess.sim.desc.expiry_ts_ms || (sess.sim.market_resolved_flag && tau_secs == 0) {
                    if let Err(err) = sess.sim.settle_expired_epoch_if_needed(now_ms, now_ns, &mut jsonl) {
                        loop_error = Some(err.to_string());
                        break;
                    }
                    let probe_due = last_rollover_probe
                        .is_none_or(|t| t.elapsed() >= Duration::from_secs(1));
                    if probe_due {
                        last_rollover_probe = Some(tokio::time::Instant::now());
                        match discover_next_5m_market(
                            &discovery,
                            &offset_est,
                            &sess.sim.desc.condition_id,
                        )
                        .await
                        {
                            Ok(next_desc) => {
                                if let Err(err) = handles
                                    .send_clob_rollover(
                                        next_desc.asset_id_up.clone(),
                                        next_desc.asset_id_dn.clone(),
                                    )
                                    .await
                                {
                                    loop_error = Some(err.to_string());
                                    break;
                                }
                                if let Err(err) = sess.sim.rollover_to_market(next_desc, now_ms, now_ns, &mut jsonl) {
                                    loop_error = Some(err.to_string());
                                    break;
                                }
                                last_rollover_probe = None;
                            }
                            Err(err) => {
                                eprintln!(
                                    "  {} Awaiting next 5m BTC market after {}: {err}",
                                    t.badge(Badge::Epoch),
                                    sess.sim.desc.slug
                                );
                            }
                        }
                    }
                }
            }
        }
    }

    let final_ns = clock.now_ns()?;
    let final_ms = base_adj_ms.saturating_add(final_ns.saturating_sub(base_mono_ns) / 1_000_000);

    sess.actor_shutdown.store(true, Ordering::Release);
    let ManagedLifecycleSession {
        mut sim,
        supervisor,
        pricing_tx,
        risk_tx,
        ..
    } = sess;
    drop(pricing_tx);
    drop(risk_tx);
    let supervisor_result = supervisor.join();
    shutdown_actors(handles, cfg.polybolt_enabled).await;

    if let Some(message) = loop_error {
        return Err(message.into());
    }
    supervisor_result?;

    finalize_and_report_session(
        &mut sim,
        final_ms,
        final_ns,
        "live",
        (&mut pricing_lat, &mut risk_lat, &mut total_lat),
        jsonl,
    )
}

async fn run_replay_epoch(
    args: LifecycleArgs,
    input_path: PathBuf,
    clock: MonotonicClock,
) -> Result<(), Box<dyn Error>> {
    let mut cfg = EngineConfig::from_env()?;
    args.math.apply_to(&mut cfg)?;
    let mut jsonl = JsonlWriter::create(&args.jsonl)?;
    let mut pricing_lat = LatencyTracker::new()?;
    let mut risk_lat = LatencyTracker::new()?;
    let mut total_lat = LatencyTracker::new()?;

    let reader = BufReader::new(File::open(&input_path)?);
    let mut max_outer_mono = 0_u64;
    let mut time_anchor: Option<(u64, u64)> = None;
    let mut next_eval_ms: Option<u64> = None;
    let mut last_sim_ms = 0_u64;
    let mut last_sim_ns = 0_u64;
    let mut epoch = 0_u64;
    let mut session: Option<ManagedLifecycleSession> = None;
    // Spot ticks observed at/after the outgoing market's expiry. They must not
    // mutate the prior settlement, but they seed the next market after rollover.
    let mut post_expiry_spots: Vec<(u64, f64)> = Vec::new();

    for line_res in reader.lines() {
        let line = line_res?;
        if line.trim().is_empty() {
            continue;
        }
        let env: RawJsonlEnvelope = serde_json::from_str(&line)?;
        if env.mono_ns < max_outer_mono {
            return Err(format!(
                "non-monotonic replay mono_ns: {} < {}",
                env.mono_ns, max_outer_mono
            )
            .into());
        }
        max_outer_mono = env.mono_ns;

        let (anchor_ts_ms, anchor_mono_ns) =
            *time_anchor.get_or_insert((env.ts_unix_ms, env.mono_ns));
        let cur_sim_ns = env.mono_ns.saturating_sub(anchor_mono_ns);
        let cur_sim_ms = anchor_ts_ms.saturating_add(cur_sim_ns / 1_000_000);
        last_sim_ms = cur_sim_ms;
        last_sim_ns = cur_sim_ns;

        // If the active market expired at or before this event, settle it at the
        // expiry boundary before post-expiry prices can mutate settlement state.
        if let Some(sess) = session.as_mut()
            && !sess.sim.epoch_settled
            && cur_sim_ms >= sess.sim.desc.expiry_ts_ms
        {
            let expiry_ms = sess.sim.desc.expiry_ts_ms;
            {
                let mut latencies = LiveLatencies {
                    pricing: &mut pricing_lat,
                    risk: &mut risk_lat,
                    total: &mut total_lat,
                };
                step_replay_eval_grid(
                    sess,
                    anchor_ts_ms,
                    expiry_ms,
                    &mut next_eval_ms,
                    &mut latencies,
                    &mut jsonl,
                )
                .await?;
            }
            let expiry_ns = expiry_ms
                .saturating_sub(anchor_ts_ms)
                .saturating_mul(1_000_000);
            sess.sim
                .settle_expired_epoch_if_needed(expiry_ms, expiry_ns, &mut jsonl)?;
        }
        if let Some(sess) = session.as_mut()
            && sess.sim.epoch_settled
            && cur_sim_ms >= sess.sim.desc.expiry_ts_ms
        {
            match env.event.as_str() {
                "market_discovered" => {
                    let desc: MarketDescriptor = serde_json::from_value(env.payload)?;
                    desc.validate()?;
                    if desc.condition_id != sess.sim.desc.condition_id {
                        epoch = epoch.saturating_add(1);
                        sess.sim
                            .rollover_to_market(desc, cur_sim_ms, cur_sim_ns, &mut jsonl)?;
                        for (ts_ms, spot) in post_expiry_spots.drain(..) {
                            sess.sim.observe_spot(spot, ts_ms);
                        }
                        next_eval_ms =
                            Some(cur_sim_ms.checked_add(100).ok_or(
                                "replay timestamp too large to advance the evaluation grid",
                            )?);
                    }
                }
                "binance_tick" => {
                    let ev: BinanceSpotEvent = serde_json::from_value(env.payload)?;
                    let spot = match &ev {
                        BinanceSpotEvent::BookTicker { bid_e8, ask_e8, .. } => {
                            (*bid_e8 as f64 + *ask_e8 as f64) / 200_000_000.0
                        }
                        BinanceSpotEvent::AggTrade { price_e8, .. } => {
                            *price_e8 as f64 / 100_000_000.0
                        }
                    };
                    if spot > 0.0 && spot.is_finite() {
                        sess.sim.latest_spot = Some(spot);
                        post_expiry_spots.push((cur_sim_ms, spot));
                    }
                }
                "polybolt_event" => {
                    let ev: OracleTwapEvent = serde_json::from_value(env.payload)?;
                    let spot_opt = match &ev {
                        OracleTwapEvent::ChainlinkSpot { spot_e8, .. } => {
                            Some(*spot_e8 as f64 / 100_000_000.0)
                        }
                        OracleTwapEvent::ChainlinkTwap {
                            twap_e8, spot_e8, ..
                        } => {
                            let effective = if *spot_e8 > 0 { *spot_e8 } else { *twap_e8 };
                            Some(effective as f64 / 100_000_000.0)
                        }
                    };
                    if let Some(spot) = spot_opt
                        && spot > 0.0
                        && spot.is_finite()
                    {
                        sess.sim.latest_spot = Some(spot);
                        post_expiry_spots.push((cur_sim_ms, spot));
                    }
                }
                "poly_book" | "poly_price_change" | "poly_trade" | "poly_tick_size"
                | "poly_bba" => {}
                "clock_sync" | "feed_health" | "feeds_summary" => {}
                other => {
                    return Err(format!("unknown event `{other}` in replay input").into());
                }
            }
            continue;
        }

        if let Some(sess) = session.as_mut() {
            let mut latencies = LiveLatencies {
                pricing: &mut pricing_lat,
                risk: &mut risk_lat,
                total: &mut total_lat,
            };
            step_replay_eval_grid(
                sess,
                anchor_ts_ms,
                cur_sim_ms,
                &mut next_eval_ms,
                &mut latencies,
                &mut jsonl,
            )
            .await?;
        }

        match env.event.as_str() {
            "market_discovered" => {
                let desc: MarketDescriptor = serde_json::from_value(env.payload)?;
                desc.validate()?;
                if let Some(sess) = session.as_mut() {
                    if desc.condition_id != sess.sim.desc.condition_id {
                        epoch = epoch.saturating_add(1);
                        sess.sim
                            .rollover_to_market(desc, cur_sim_ms, cur_sim_ns, &mut jsonl)?;
                        for (ts_ms, spot) in post_expiry_spots.drain(..) {
                            sess.sim.observe_spot(spot, ts_ms);
                        }
                        next_eval_ms =
                            Some(cur_sim_ms.checked_add(100).ok_or(
                                "replay timestamp too large to advance the evaluation grid",
                            )?);
                    }
                } else {
                    let sess = spawn_managed_session(desc, &cfg, &args, cur_sim_ns, clock)?;
                    print_epoch_banner(
                        sess.sim.epoch_num,
                        &sess.sim.desc.slug,
                        sess.sim.strike_usd,
                        sess.sim.desc.start_ts_ms,
                        sess.sim.desc.expiry_ts_ms,
                    );
                    next_eval_ms = Some(
                        cur_sim_ms
                            .checked_add(100)
                            .ok_or("replay timestamp too large to advance the evaluation grid")?,
                    );
                    session = Some(sess);
                }
            }
            "binance_tick" => {
                if let Some(sess) = session.as_mut() {
                    let ev: BinanceSpotEvent = serde_json::from_value(env.payload)?;
                    sess.sim.on_binance_event(ev, cur_sim_ms);
                }
            }
            "polybolt_event" => {
                if let Some(sess) = session.as_mut() {
                    let ev: OracleTwapEvent = serde_json::from_value(env.payload)?;
                    sess.sim.on_oracle_event(ev);
                }
            }
            "poly_book" | "poly_price_change" | "poly_trade" | "poly_tick_size" | "poly_bba" => {
                if let Some(sess) = session.as_mut() {
                    let ev = replay_poly_to_market_event(env.event.as_str(), env.payload, epoch)?;
                    sess.sim
                        .on_poly_event(ev, cur_sim_ms, cur_sim_ns, &mut jsonl)?;
                }
            }
            "clock_sync" | "feed_health" | "feeds_summary" => {}
            other => {
                return Err(format!("unknown event `{other}` in replay input").into());
            }
        }
    }

    let mut sess = session.ok_or("replay file contains no `market_discovered` event")?;
    {
        let (anchor_ts_ms, _) = time_anchor.unwrap_or((last_sim_ms, 0));
        let mut latencies = LiveLatencies {
            pricing: &mut pricing_lat,
            risk: &mut risk_lat,
            total: &mut total_lat,
        };
        step_replay_eval_grid(
            &mut sess,
            anchor_ts_ms,
            last_sim_ms,
            &mut next_eval_ms,
            &mut latencies,
            &mut jsonl,
        )
        .await?;
    }
    if last_sim_ms >= sess.sim.desc.expiry_ts_ms {
        sess.sim
            .settle_expired_epoch_if_needed(last_sim_ms, last_sim_ns, &mut jsonl)?;
    }

    let mut sim = sess.shutdown_returning_sim()?;
    finalize_and_report_session(
        &mut sim,
        last_sim_ms,
        last_sim_ns,
        "replay",
        (&mut pricing_lat, &mut risk_lat, &mut total_lat),
        jsonl,
    )
}

async fn step_replay_eval_grid(
    sess: &mut ManagedLifecycleSession,
    anchor_ts_ms: u64,
    cur_sim_ms: u64,
    next_eval_ms: &mut Option<u64>,
    latencies: &mut LiveLatencies<'_>,
    jsonl: &mut JsonlWriter,
) -> Result<(), Box<dyn Error>> {
    const MAX_REPLAY_GAP_CATCHUP_MS: u64 = 600_000;
    if let Some(target) = *next_eval_ms
        && cur_sim_ms.saturating_sub(target) > MAX_REPLAY_GAP_CATCHUP_MS
    {
        eprintln!(
            "  {} replay timestamp gap of {}ms exceeds {MAX_REPLAY_GAP_CATCHUP_MS}ms; fast-forwarding evaluation grid",
            theme::current().badge(Badge::Warn),
            cur_sim_ms.saturating_sub(target)
        );
        *next_eval_ms = Some(cur_sim_ms);
    }

    while let Some(eval_ms) = *next_eval_ms {
        if cur_sim_ms < eval_ms {
            break;
        }
        let eval_ns = eval_ms
            .saturating_sub(anchor_ts_ms)
            .saturating_mul(1_000_000);
        let actors = LiveActorChannels {
            pricing_tx: &sess.pricing_tx,
            risk_tx: &sess.risk_tx,
        };
        sess.sim
            .step_tick(eval_ms, eval_ns, 0, &actors, latencies, jsonl)
            .await?;
        let next_ms = eval_ms
            .checked_add(100)
            .ok_or("replay timestamp too large to advance the evaluation grid")?;
        *next_eval_ms = Some(next_ms);
    }
    Ok(())
}

fn replay_poly_to_market_event(
    tag: &str,
    payload: serde_json::Value,
    epoch: u64,
) -> Result<PolyMarketEvent, Box<dyn Error>> {
    let ev: ReplayPolyEvent = serde_json::from_value(payload)?;
    match ev {
        ReplayPolyEvent::BookSnapshot {
            outcome,
            book,
            rx_mono_ns,
            exch_ts_ms,
        } => {
            if tag != "poly_book" {
                return Err(format!("outer tag `{tag}` mismatches BookSnapshot").into());
            }
            if book.outcome != outcome {
                return Err("replay BookSnapshot outcome mismatch".into());
            }
            let fixed = FixedBook::<10>::try_from_slices(
                outcome,
                &book.bids,
                &book.asks,
                book.last_update_ns,
                book.exchange_ts_ms,
            )?;
            Ok(PolyMarketEvent::BookSnapshot {
                outcome,
                book: fixed,
                rx_mono_ns,
                exch_ts_ms,
                epoch,
            })
        }
        ReplayPolyEvent::PriceChange {
            outcome,
            side,
            price,
            size,
            best_bid,
            best_ask,
            rx_mono_ns,
            exch_ts_ms,
        } => {
            if tag != "poly_price_change" {
                return Err(format!("outer tag `{tag}` mismatches PriceChange").into());
            }
            Ok(PolyMarketEvent::PriceChange {
                outcome,
                side,
                price,
                size,
                best_bid,
                best_ask,
                rx_mono_ns,
                exch_ts_ms,
                epoch,
            })
        }
        ReplayPolyEvent::LastTrade {
            outcome,
            side,
            price,
            size,
            rx_mono_ns,
            exch_ts_ms,
        } => {
            if tag != "poly_trade" {
                return Err(format!("outer tag `{tag}` mismatches LastTrade").into());
            }
            Ok(PolyMarketEvent::LastTrade {
                outcome,
                side,
                price,
                size,
                rx_mono_ns,
                exch_ts_ms,
                epoch,
            })
        }
        ReplayPolyEvent::TickSizeChange {
            outcome,
            new_tick_micro,
            rx_mono_ns,
            exch_ts_ms,
        } => {
            if tag != "poly_tick_size" {
                return Err(format!("outer tag `{tag}` mismatches TickSizeChange").into());
            }
            Ok(PolyMarketEvent::TickSizeChange {
                outcome,
                new_tick_micro,
                rx_mono_ns,
                exch_ts_ms,
                epoch,
            })
        }
        ReplayPolyEvent::BestBidAsk {
            outcome,
            best_bid,
            best_ask,
            rx_mono_ns,
            exch_ts_ms,
        } => {
            if tag != "poly_bba" {
                return Err(format!("outer tag `{tag}` mismatches BestBidAsk").into());
            }
            Ok(PolyMarketEvent::BestBidAsk {
                outcome,
                best_bid,
                best_ask,
                rx_mono_ns,
                exch_ts_ms,
                epoch,
            })
        }
    }
}

async fn discover_next_5m_market(
    discovery: &GammaDiscoveryClient,
    offset_est: &ClockOffsetEstimator,
    expired_condition_id: &str,
) -> Result<MarketDescriptor, Box<dyn Error>> {
    let _sync = discovery.sync_clock(offset_est).await;
    let adj_now = offset_est.adjusted_unix_ms(MonotonicClock::unix_now_ms()?)?;
    let next_desc = discovery.discover_active_5m_btc(adj_now, 15_000).await?;
    if next_desc.condition_id == expired_condition_id {
        return Err("Gamma still returning expired condition_id; awaiting next 5m market".into());
    }
    Ok(next_desc)
}

trait FlattenOutcomeExt {
    fn outcome(&self) -> Option<Outcome>;
}

impl FlattenOutcomeExt for FlattenAction {
    fn outcome(&self) -> Option<Outcome> {
        match self {
            FlattenAction::FlattenFak { outcome, .. } => Some(*outcome),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use core_types::{BookLevel, ProtocolVersion};
    use std::path::Path;

    fn test_market(slug: &str, start_ts_ms: u64, expiry_ts_ms: u64) -> MarketDescriptor {
        MarketDescriptor {
            condition_id: format!("cond_{slug}"),
            slug: slug.to_string(),
            question: "BTC 5m test".to_string(),
            version: ProtocolVersion::PolyV2,
            neg_risk: false,
            asset_id_up: format!("up_{slug}"),
            asset_id_dn: format!("dn_{slug}"),
            tick_size_micro: MicroUsdc(10_000),
            min_order_size_micro: SharesMicro(5_000_000),
            taker_delay_ms: 50,
            fee_schedule: FeeSchedule::btc_5m_default(),
            start_ts_ms,
            expiry_ts_ms,
            strike_price_usd: None,
        }
    }

    fn test_book(
        outcome: Outcome,
        best_bid_micro: u64,
        best_ask_micro: u64,
        depth_shares: u64,
        ts_ms: u64,
    ) -> FixedBook<10> {
        let bids = [
            BookLevel {
                price: MicroUsdc(best_bid_micro),
                size: SharesMicro(depth_shares * ONE_SHARE_MICRO),
            },
            BookLevel {
                price: MicroUsdc(best_bid_micro.saturating_sub(10_000)),
                size: SharesMicro((depth_shares + 50) * ONE_SHARE_MICRO),
            },
        ];
        let asks = [
            BookLevel {
                price: MicroUsdc(best_ask_micro),
                size: SharesMicro(depth_shares * ONE_SHARE_MICRO),
            },
            BookLevel {
                price: MicroUsdc(best_ask_micro.saturating_add(10_000)),
                size: SharesMicro((depth_shares + 50) * ONE_SHARE_MICRO),
            },
        ];
        FixedBook::<10>::try_from_slices(outcome, &bids, &asks, ts_ms * 1_000_000, ts_ms).unwrap()
    }

    fn temp_jsonl_path(tag: &str) -> PathBuf {
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("poly_as_live_sim_{tag}_{unique}.jsonl"))
    }

    struct TestActors {
        sim: LivePaperSimulator,
        supervisor: ActorSupervisor,
        actor_shutdown: Arc<AtomicBool>,
        pricing_tx: mpsc::Sender<(PricingJob, oneshot::Sender<PricingAck>)>,
        risk_tx: mpsc::Sender<(RiskJob, oneshot::Sender<RiskAck>)>,
        pricing_lat: LatencyTracker,
        risk_lat: LatencyTracker,
        total_lat: LatencyTracker,
        jsonl: JsonlWriter,
        jsonl_path: PathBuf,
    }

    fn setup_test_actors(
        tag: &str,
        desc: MarketDescriptor,
        v_max_usd: u64,
        base_size: u64,
        order_latency_ms: u64,
    ) -> TestActors {
        let clock = MonotonicClock::start();
        let (harness, priority_rx, routine_rx) = ActorPipelineHarness::new(64, 16);
        let v_max_micro = MicroUsdc(v_max_usd * 1_000_000);
        let risk_actor = build_risk_actor(
            harness.clone(),
            v_max_micro,
            base_size,
            5,
            desc.tick_size_micro,
            1_000,
            FaultInjection::None,
        )
        .unwrap();

        let cfg = EngineConfig::from_lookup(|_| Ok(None)).unwrap();
        let pipe_cfg = MathPipelineConfig::from_engine_config(&cfg).unwrap();
        let math_pipeline = MathPipeline::new(pipe_cfg, clock).unwrap();

        let broker_cfg = FifoBrokerConfig {
            order_latency_ms,
            cancel_latency_ms: order_latency_ms,
            ..Default::default()
        };
        let sim = LivePaperSimulator::new(
            desc,
            broker_cfg,
            v_max_micro,
            base_size,
            LifecycleDisplayMode::Stream,
            harness.clone(),
            priority_rx,
            routine_rx,
        )
        .unwrap();

        let mut supervisor = ActorSupervisor::new(harness);
        let actor_shutdown = supervisor.shutdown_flag();
        let (pricing_tx, pricing_rx) = mpsc::channel(4);
        let (risk_tx, risk_rx) = mpsc::channel(4);

        let pricing_harness = sim.harness.clone();
        let pricing_shutdown = Arc::clone(&actor_shutdown);
        supervisor
            .spawn("pricing", None, move || {
                pricing_actor_loop(math_pipeline, pricing_harness, pricing_rx, pricing_shutdown);
            })
            .unwrap();

        let risk_shutdown = Arc::clone(&actor_shutdown);
        supervisor
            .spawn("risk", None, move || {
                risk_actor_loop(risk_actor, risk_rx, risk_shutdown);
            })
            .unwrap();

        let jsonl_path = temp_jsonl_path(tag);
        let jsonl = JsonlWriter::create(&jsonl_path).unwrap();

        TestActors {
            sim,
            supervisor,
            actor_shutdown,
            pricing_tx,
            risk_tx,
            pricing_lat: LatencyTracker::new().unwrap(),
            risk_lat: LatencyTracker::new().unwrap(),
            total_lat: LatencyTracker::new().unwrap(),
            jsonl,
            jsonl_path,
        }
    }

    impl TestActors {
        async fn step(&mut self, now_ms: u64) {
            let mut lat = LiveLatencies {
                pricing: &mut self.pricing_lat,
                risk: &mut self.risk_lat,
                total: &mut self.total_lat,
            };
            let actors = LiveActorChannels {
                pricing_tx: &self.pricing_tx,
                risk_tx: &self.risk_tx,
            };
            self.sim
                .step_tick(
                    now_ms,
                    now_ms * 1_000_000,
                    0,
                    &actors,
                    &mut lat,
                    &mut self.jsonl,
                )
                .await
                .unwrap();
        }

        fn shutdown_and_read_jsonl(mut self, final_ms: u64) -> String {
            self.actor_shutdown.store(true, Ordering::Release);
            drop(self.pricing_tx);
            drop(self.risk_tx);
            self.supervisor.join().unwrap();

            self.sim
                .finalize_stats(final_ms, final_ms * 1_000_000, &mut self.jsonl)
                .unwrap();
            let p = self.pricing_lat.summary();
            let r = self.risk_lat.summary();
            let t = self.total_lat.summary();
            record_lifecycle_summary_jsonl(
                &mut self.jsonl,
                final_ms,
                final_ms * 1_000_000,
                "live",
                &self.sim.desc.slug,
                &self.sim.stats,
                &p,
                &r,
                &t,
            )
            .unwrap();
            self.jsonl.close().unwrap();

            let contents = std::fs::read_to_string(&self.jsonl_path).unwrap();
            let _ = std::fs::remove_file(&self.jsonl_path);
            contents
        }
    }

    #[tokio::test]
    async fn test_live_8station_fifo_fill_merge_and_registry_wiring() {
        let start_ms = 1_700_000_000_000_u64;
        let expiry_ms = start_ms + 300_000;
        let mut ctx = setup_test_actors(
            "fill_merge",
            test_market("btc-5m-1", start_ms, expiry_ms),
            50,
            20,
            0,
        );

        let up_book = test_book(Outcome::Up, 490_000, 510_000, 50, start_ms);
        let dn_book = test_book(Outcome::Down, 490_000, 510_000, 50, start_ms);
        ctx.sim
            .on_poly_event(
                PolyMarketEvent::BookSnapshot {
                    outcome: Outcome::Up,
                    book: up_book,
                    rx_mono_ns: 1_000,
                    exch_ts_ms: start_ms,
                    epoch: 0,
                },
                start_ms,
                1_000,
                &mut ctx.jsonl,
            )
            .unwrap();
        ctx.sim
            .on_poly_event(
                PolyMarketEvent::BookSnapshot {
                    outcome: Outcome::Down,
                    book: dn_book,
                    rx_mono_ns: 1_000,
                    exch_ts_ms: start_ms,
                    epoch: 0,
                },
                start_ms,
                1_000,
                &mut ctx.jsonl,
            )
            .unwrap();
        ctx.sim.on_binance_event(
            BinanceSpotEvent::AggTrade {
                price_e8: 9_500_000_000_000,
                qty_e8: 100_000_000,
                is_buyer_maker: false,
                exch_ts_ms: start_ms,
                rx_mono_ns: 1_000,
            },
            start_ms,
        );

        // Warm up 5 SG samples + 1 Risk evaluation tick to submit UP and DN passive bids.
        for i in 0..7_u64 {
            ctx.step(start_ms + i * 100).await;
        }

        assert!(ctx.sim.stats.orders_submitted >= 2);
        let reg = ctx.sim.harness.registry_slot.load();
        assert!(reg.open_bid_shares_up.0 > 0);
        assert!(reg.open_bid_shares_dn.0 > 0);
        assert!(reg.open_bid_notional_up.0 > 0);
        assert!(reg.open_bid_notional_dn.0 > 0);

        let up_order = ctx
            .sim
            .broker
            .active_order(Outcome::Up, OrderSide::Buy)
            .unwrap()
            .clone();
        let dn_order = ctx
            .sim
            .broker
            .active_order(Outcome::Down, OrderSide::Buy)
            .unwrap()
            .clone();

        // Trade through UP bid for 10 shares -> passive maker fill + rebate accrual + registry update.
        let t_fill_up = start_ms + 750;
        ctx.sim
            .on_poly_event(
                PolyMarketEvent::LastTrade {
                    outcome: Outcome::Up,
                    side: OrderSide::Sell,
                    price: MicroUsdc(up_order.price.0 - 10_000),
                    size: SharesMicro(10 * ONE_SHARE_MICRO),
                    rx_mono_ns: t_fill_up * 1_000_000,
                    exch_ts_ms: t_fill_up,
                    epoch: 0,
                },
                t_fill_up,
                t_fill_up * 1_000_000,
                &mut ctx.jsonl,
            )
            .unwrap();

        assert_eq!(ctx.sim.ledger.filled_up, SharesMicro(10 * ONE_SHARE_MICRO));
        assert!(ctx.sim.ledger.estimated_rebate_micro > 0);
        assert_eq!(ctx.sim.stats.fills_count, 1);

        let reg_after_up = ctx.sim.harness.registry_slot.load();
        assert_eq!(reg_after_up.unhedged_up, SharesMicro(10 * ONE_SHARE_MICRO));
        assert!(reg_after_up.unhedged_cost_up_micro.0 > 0);

        // Step one tick with nonzero inventory_q > 0, verifying Pricing skewed bid_up downward.
        ctx.step(start_ms + 800).await;
        let skewed_proposal = ctx.sim.harness.pricing_slot.load();
        if let QuoteOutput::Active {
            bid_up: Some(b_up),
            bid_dn: Some(b_dn),
            ..
        } = skewed_proposal.quote
        {
            assert!(
                b_up <= b_dn,
                "positive UP inventory should skew UP bid <= DN bid ({b_up} vs {b_dn})"
            );
        }

        // Trade through DN bid for 10 shares -> triggers atomic VWAP pair merge!
        let t_fill_dn = start_ms + 850;
        ctx.sim
            .on_poly_event(
                PolyMarketEvent::LastTrade {
                    outcome: Outcome::Down,
                    side: OrderSide::Sell,
                    price: MicroUsdc(dn_order.price.0 - 10_000),
                    size: SharesMicro(10 * ONE_SHARE_MICRO),
                    rx_mono_ns: t_fill_dn * 1_000_000,
                    exch_ts_ms: t_fill_dn,
                    epoch: 0,
                },
                t_fill_dn,
                t_fill_dn * 1_000_000,
                &mut ctx.jsonl,
            )
            .unwrap();

        assert_eq!(ctx.sim.ledger.filled_up, SharesMicro::ZERO);
        assert_eq!(ctx.sim.ledger.filled_dn, SharesMicro::ZERO);
        assert_eq!(
            ctx.sim.ledger.total_pairs_merged,
            SharesMicro(10 * ONE_SHARE_MICRO)
        );
        assert_eq!(ctx.sim.stats.merges_count, 1);
        assert!(ctx.sim.ledger.realized_pnl_micro > 0);

        let log_contents = ctx.shutdown_and_read_jsonl(start_ms + 900);
        assert!(log_contents.contains("\"event\":\"fill\""));
        assert!(log_contents.contains("\"event\":\"merge\""));
        assert!(log_contents.contains("\"event\":\"summary\""));
    }

    #[tokio::test]
    async fn test_live_8station_v_max_and_base_size_enforcement() {
        let start_ms = 1_700_000_000_000_u64;
        let expiry_ms = start_ms + 300_000;
        // V_max = $10, base_size = 50 -> at ~$0.47, Q_max <= 21 shares (< 50).
        let mut ctx = setup_test_actors(
            "vmax_cap",
            test_market("btc-5m-vmax", start_ms, expiry_ms),
            10,
            50,
            0,
        );

        let up_book = test_book(Outcome::Up, 490_000, 510_000, 100, start_ms);
        let dn_book = test_book(Outcome::Down, 490_000, 510_000, 100, start_ms);
        ctx.sim
            .on_poly_event(
                PolyMarketEvent::BookSnapshot {
                    outcome: Outcome::Up,
                    book: up_book,
                    rx_mono_ns: 1_000,
                    exch_ts_ms: start_ms,
                    epoch: 0,
                },
                start_ms,
                1_000,
                &mut ctx.jsonl,
            )
            .unwrap();
        ctx.sim
            .on_poly_event(
                PolyMarketEvent::BookSnapshot {
                    outcome: Outcome::Down,
                    book: dn_book,
                    rx_mono_ns: 1_000,
                    exch_ts_ms: start_ms,
                    epoch: 0,
                },
                start_ms,
                1_000,
                &mut ctx.jsonl,
            )
            .unwrap();
        ctx.sim.on_binance_event(
            BinanceSpotEvent::AggTrade {
                price_e8: 9_500_000_000_000,
                qty_e8: 100_000_000,
                is_buyer_maker: false,
                exch_ts_ms: start_ms,
                rx_mono_ns: 1_000,
            },
            start_ms,
        );

        for i in 0..8_u64 {
            ctx.step(start_ms + i * 100).await;
        }

        let up_order = ctx
            .sim
            .broker
            .active_order(Outcome::Up, OrderSide::Buy)
            .unwrap()
            .clone();
        let up_notional = (up_order.original_size.0 as u128 * up_order.price.0 as u128) / 1_000_000;
        assert!(up_order.original_size.0 < 50 * ONE_SHARE_MICRO);
        assert!(up_notional <= 10_000_000, "order must obey V_max=$10 cap");
        assert!(
            ctx.sim.stats.orders_held > 0,
            "steady resting order must be held without self-churning"
        );
        assert_eq!(ctx.sim.stats.flattens_executed, 0);

        // Fill 20 shares UP @ $0.47 => $9.40 cost => u = 0.94 > 0.90 => Tier4HardStop Flatten!
        let fill_t = start_ms + 850;
        ctx.sim
            .on_poly_event(
                PolyMarketEvent::LastTrade {
                    outcome: Outcome::Up,
                    side: OrderSide::Sell,
                    price: MicroUsdc(up_order.price.0 - 10_000),
                    size: SharesMicro(20 * ONE_SHARE_MICRO),
                    rx_mono_ns: fill_t * 1_000_000,
                    exch_ts_ms: fill_t,
                    epoch: 0,
                },
                fill_t,
                fill_t * 1_000_000,
                &mut ctx.jsonl,
            )
            .unwrap();

        ctx.step(start_ms + 900).await;
        assert!(
            ctx.sim.stats.flattens_executed >= 1,
            "exceeding 0.90 * V_max must trigger Tier4 flatten"
        );
        assert_eq!(ctx.sim.ledger.net_inventory(), SignedSharesMicro::ZERO);
        assert!(ctx.sim.ledger.fees_paid_micro > 0);

        let log_contents = ctx.shutdown_and_read_jsonl(start_ms + 1_000);
        assert!(log_contents.contains("\"event\":\"flatten\""));
    }

    #[tokio::test]
    async fn test_live_8station_t30s_flatten_and_300s_epoch_rollover_settlement() {
        let start_ms = 1_700_000_000_000_u64;
        let expiry_ms = start_ms + 300_000;
        let mut ctx = setup_test_actors(
            "rollover",
            test_market("btc-5m-epoch1", start_ms, expiry_ms),
            50,
            20,
            0,
        );

        let up_book = test_book(Outcome::Up, 490_000, 510_000, 100, start_ms);
        let dn_book = test_book(Outcome::Down, 490_000, 510_000, 100, start_ms);
        ctx.sim
            .on_poly_event(
                PolyMarketEvent::BookSnapshot {
                    outcome: Outcome::Up,
                    book: up_book,
                    rx_mono_ns: 1_000,
                    exch_ts_ms: start_ms,
                    epoch: 0,
                },
                start_ms,
                1_000,
                &mut ctx.jsonl,
            )
            .unwrap();
        ctx.sim
            .on_poly_event(
                PolyMarketEvent::BookSnapshot {
                    outcome: Outcome::Down,
                    book: dn_book,
                    rx_mono_ns: 1_000,
                    exch_ts_ms: start_ms,
                    epoch: 0,
                },
                start_ms,
                1_000,
                &mut ctx.jsonl,
            )
            .unwrap();
        ctx.sim.on_binance_event(
            BinanceSpotEvent::AggTrade {
                price_e8: 9_500_000_000_000,
                qty_e8: 100_000_000,
                is_buyer_maker: false,
                exch_ts_ms: start_ms,
                rx_mono_ns: 1_000,
            },
            start_ms,
        );

        // Warm up & place bids during ActiveQuoting.
        for i in 0..7_u64 {
            ctx.step(start_ms + i * 100).await;
        }
        let up_order = ctx
            .sim
            .broker
            .active_order(Outcome::Up, OrderSide::Buy)
            .unwrap()
            .clone();

        // Fill 15 shares UP @ resting bid, leaving net_delta = +15.
        let t_fill = start_ms + 750;
        ctx.sim
            .on_poly_event(
                PolyMarketEvent::LastTrade {
                    outcome: Outcome::Up,
                    side: OrderSide::Sell,
                    price: MicroUsdc(up_order.price.0 - 10_000),
                    size: SharesMicro(15 * ONE_SHARE_MICRO),
                    rx_mono_ns: t_fill * 1_000_000,
                    exch_ts_ms: t_fill,
                    epoch: 0,
                },
                t_fill,
                t_fill * 1_000_000,
                &mut ctx.jsonl,
            )
            .unwrap();
        assert_eq!(ctx.sim.ledger.filled_up, SharesMicro(15 * ONE_SHARE_MICRO));

        // Advance to T-25s (MandatoryFlatten window: 15 < tau <= 30) -> triggers FlattenFak.
        let t_flatten = expiry_ms - 25_000;
        ctx.step(t_flatten).await;
        assert_eq!(ctx.sim.current_phase, Some(EpochPhase::MandatoryFlatten));
        assert!(ctx.sim.stats.flattens_executed >= 1);
        assert_eq!(ctx.sim.ledger.net_inventory(), SignedSharesMicro::ZERO);

        // Record a 3-share residual dust fill (< min_order_size = 5) so epoch expiry exercises
        // terminal settlement_pnl_micro as well.
        let dust_fill = SimFill {
            sim_id: 999,
            outcome: Outcome::Up,
            side: OrderSide::Buy,
            price: MicroUsdc(450_000),
            filled_shares: SharesMicro(3 * ONE_SHARE_MICRO),
            fill_ts_ms: t_flatten + 100,
            is_maker: true,
        };
        ctx.sim
            .process_fill(
                &dust_fill,
                t_flatten + 100,
                (t_flatten + 100) * 1_000_000,
                &mut ctx.jsonl,
            )
            .unwrap();
        assert_eq!(ctx.sim.ledger.filled_up, SharesMicro(3 * ONE_SHARE_MICRO));

        // BTC spot above strike => Outcome::Up wins at settlement (+$3.00 - $1.35 = +$1.65 delta).
        ctx.sim.on_binance_event(
            BinanceSpotEvent::AggTrade {
                price_e8: 9_550_000_000_000,
                qty_e8: 100_000_000,
                is_buyer_maker: false,
                exch_ts_ms: expiry_ms,
                rx_mono_ns: expiry_ms * 1_000_000,
            },
            expiry_ms,
        );

        let next_market = test_market("btc-5m-epoch2", expiry_ms, expiry_ms + 300_000);
        ctx.sim
            .rollover_to_market(
                next_market,
                expiry_ms,
                expiry_ms * 1_000_000,
                &mut ctx.jsonl,
            )
            .unwrap();

        assert_eq!(ctx.sim.stats.epochs_completed, 1);
        assert_eq!(ctx.sim.stats.epoch_rollovers, 1);
        assert_eq!(ctx.sim.stats.settlement_pnl_micro, 1_650_000);
        assert_eq!(ctx.sim.ledger.filled_up, SharesMicro::ZERO);
        assert_eq!(ctx.sim.ledger.filled_dn, SharesMicro::ZERO);
        assert_eq!(ctx.sim.desc.slug, "btc-5m-epoch2");

        // Feed Epoch 2 snapshots and verify RiskActor transitions back to ActiveQuoting cleanly.
        let up_book_2 = test_book(Outcome::Up, 490_000, 510_000, 100, expiry_ms + 100);
        let dn_book_2 = test_book(Outcome::Down, 490_000, 510_000, 100, expiry_ms + 100);
        ctx.sim
            .on_poly_event(
                PolyMarketEvent::BookSnapshot {
                    outcome: Outcome::Up,
                    book: up_book_2,
                    rx_mono_ns: (expiry_ms + 100) * 1_000_000,
                    exch_ts_ms: expiry_ms + 100,
                    epoch: 0,
                },
                expiry_ms + 100,
                (expiry_ms + 100) * 1_000_000,
                &mut ctx.jsonl,
            )
            .unwrap();
        ctx.sim
            .on_poly_event(
                PolyMarketEvent::BookSnapshot {
                    outcome: Outcome::Down,
                    book: dn_book_2,
                    rx_mono_ns: (expiry_ms + 100) * 1_000_000,
                    exch_ts_ms: expiry_ms + 100,
                    epoch: 0,
                },
                expiry_ms + 100,
                (expiry_ms + 100) * 1_000_000,
                &mut ctx.jsonl,
            )
            .unwrap();

        for i in 1..=6_u64 {
            ctx.step(expiry_ms + i * 100).await;
        }
        assert_eq!(ctx.sim.current_phase, Some(EpochPhase::ActiveQuoting));

        let log_contents = ctx.shutdown_and_read_jsonl(expiry_ms + 700);
        assert!(log_contents.contains("\"event\":\"flatten\""));
        assert!(log_contents.contains("\"event\":\"epoch_rollover\""));
        assert!(log_contents.contains("\"event\":\"summary\""));
    }

    #[tokio::test]
    async fn test_live_8station_wire_latency_price_change_and_tick_sync() {
        let start_ms = 1_700_000_000_000_u64;
        let expiry_ms = start_ms + 300_000;
        let mut ctx = setup_test_actors(
            "latency_tick",
            test_market("btc-5m-latency", start_ms, expiry_ms),
            50,
            20,
            40,
        );

        let up_book = test_book(Outcome::Up, 490_000, 510_000, 50, start_ms);
        let dn_book = test_book(Outcome::Down, 490_000, 510_000, 50, start_ms);
        for (outcome, book) in [(Outcome::Up, up_book), (Outcome::Down, dn_book)] {
            ctx.sim
                .on_poly_event(
                    PolyMarketEvent::BookSnapshot {
                        outcome,
                        book,
                        rx_mono_ns: 1_000,
                        exch_ts_ms: start_ms,
                        epoch: 0,
                    },
                    start_ms,
                    1_000,
                    &mut ctx.jsonl,
                )
                .unwrap();
        }
        ctx.sim.on_binance_event(
            BinanceSpotEvent::BookTicker {
                bid_e8: 9_500_000_000_000,
                ask_e8: 9_500_100_000_000,
                bid_qty_e8: 100_000_000,
                ask_qty_e8: 100_000_000,
                update_id: 1,
                rx_mono_ns: 1_000,
            },
            start_ms,
        );

        // Warm up 5 SG samples (ticks 0..4) -> tick 4 at start_ms+400 submits order (active at +440).
        for i in 0..5_u64 {
            ctx.step(start_ms + i * 100).await;
        }
        let up_order = ctx
            .sim
            .broker
            .active_order(Outcome::Up, OrderSide::Buy)
            .unwrap()
            .clone();
        assert!(matches!(
            up_order.status,
            SimOrderStatus::PendingPlace { .. }
        ));

        // Trade at +20ms (while order is PendingPlace) must NOT fill.
        let t_early = up_order.created_at_ms + 20;
        ctx.sim
            .on_poly_event(
                PolyMarketEvent::LastTrade {
                    outcome: Outcome::Up,
                    side: OrderSide::Sell,
                    price: MicroUsdc(up_order.price.0 - 10_000),
                    size: SharesMicro(10 * ONE_SHARE_MICRO),
                    rx_mono_ns: t_early * 1_000_000,
                    exch_ts_ms: t_early,
                    epoch: 0,
                },
                t_early,
                t_early * 1_000_000,
                &mut ctx.jsonl,
            )
            .unwrap();
        assert_eq!(ctx.sim.ledger.filled_up, SharesMicro::ZERO);

        // PriceChange delta updates book & routes to broker; then trade at +50ms fills cleanly.
        let t_live = up_order.created_at_ms + 50;
        ctx.sim
            .on_poly_event(
                PolyMarketEvent::PriceChange {
                    outcome: Outcome::Up,
                    side: OrderSide::Buy,
                    price: MicroUsdc(480_000),
                    size: SharesMicro(40 * ONE_SHARE_MICRO),
                    best_bid: Some(MicroUsdc(490_000)),
                    best_ask: Some(MicroUsdc(510_000)),
                    rx_mono_ns: t_live * 1_000_000,
                    exch_ts_ms: t_live,
                    epoch: 0,
                },
                t_live,
                t_live * 1_000_000,
                &mut ctx.jsonl,
            )
            .unwrap();
        ctx.sim
            .on_poly_event(
                PolyMarketEvent::LastTrade {
                    outcome: Outcome::Up,
                    side: OrderSide::Sell,
                    price: MicroUsdc(up_order.price.0 - 10_000),
                    size: SharesMicro(10 * ONE_SHARE_MICRO),
                    rx_mono_ns: t_live * 1_000_000,
                    exch_ts_ms: t_live,
                    epoch: 0,
                },
                t_live,
                t_live * 1_000_000,
                &mut ctx.jsonl,
            )
            .unwrap();
        assert_eq!(ctx.sim.ledger.filled_up, SharesMicro(10 * ONE_SHARE_MICRO));

        // TickSizeChange updates descriptor, recomputes Pricing, and keeps Risk synced without tripping.
        let t_tick = up_order.created_at_ms + 100;
        ctx.sim
            .on_poly_event(
                PolyMarketEvent::TickSizeChange {
                    outcome: Outcome::Up,
                    new_tick_micro: MicroUsdc(1_000),
                    rx_mono_ns: t_tick * 1_000_000,
                    exch_ts_ms: t_tick,
                    epoch: 0,
                },
                t_tick,
                t_tick * 1_000_000,
                &mut ctx.jsonl,
            )
            .unwrap();
        assert_eq!(ctx.sim.desc.tick_size_micro, MicroUsdc(1_000));
        ctx.step(t_tick).await;
        assert_eq!(ctx.sim.stats.breaker_trips, 0);

        let _ = ctx.shutdown_and_read_jsonl(t_tick + 100);
    }

    #[tokio::test]
    async fn test_live_8station_orders_canceled_reconciles_between_summary_and_jsonl() {
        let start_ms = 1_700_000_000_000_u64;
        let expiry_ms = start_ms + 300_000;
        let mut ctx = setup_test_actors(
            "reconcile_cancels",
            test_market("btc-5m-reconcile", start_ms, expiry_ms),
            50,
            20,
            0,
        );

        let up_book = test_book(Outcome::Up, 490_000, 510_000, 100, start_ms);
        let dn_book = test_book(Outcome::Down, 490_000, 510_000, 100, start_ms);
        for (outcome, book) in [(Outcome::Up, up_book), (Outcome::Down, dn_book)] {
            ctx.sim
                .on_poly_event(
                    PolyMarketEvent::BookSnapshot {
                        outcome,
                        book,
                        rx_mono_ns: 1_000,
                        exch_ts_ms: start_ms,
                        epoch: 0,
                    },
                    start_ms,
                    1_000,
                    &mut ctx.jsonl,
                )
                .unwrap();
        }
        ctx.sim.on_binance_event(
            BinanceSpotEvent::AggTrade {
                price_e8: 9_500_000_000_000,
                qty_e8: 100_000_000,
                is_buyer_maker: false,
                exch_ts_ms: start_ms,
                rx_mono_ns: 1_000,
            },
            start_ms,
        );

        // Warm up SG filter and place UP + DN passive orders.
        for i in 0..7_u64 {
            ctx.step(start_ms + i * 100).await;
        }
        assert!(
            ctx.sim
                .broker
                .active_order(Outcome::Up, OrderSide::Buy)
                .is_some()
        );
        assert!(
            ctx.sim
                .broker
                .active_order(Outcome::Down, OrderSide::Buy)
                .is_some()
        );

        // Trigger a Binance spot jump so DefensiveCancel emits individual OrderAction::Cancel events.
        let t_jump = start_ms + 750;
        ctx.sim.on_binance_event(
            BinanceSpotEvent::AggTrade {
                price_e8: 9_550_000_000_000,
                qty_e8: 100_000_000,
                is_buyer_maker: false,
                exch_ts_ms: t_jump,
                rx_mono_ns: t_jump * 1_000_000,
            },
            t_jump,
        );
        ctx.step(start_ms + 800).await;
        assert_eq!(ctx.sim.stats.orders_canceled, 2);

        // Step forward so resting UP + DN quotes re-place cleanly.
        for i in 9..12_u64 {
            ctx.step(start_ms + i * 100).await;
        }
        let dn_order = ctx
            .sim
            .broker
            .active_order(Outcome::Down, OrderSide::Buy)
            .unwrap()
            .clone();

        // Fill 19 shares on DN so Q = (0, 19) (net = -19), while UP and DN orders remain open.
        let t_fill_dn = start_ms + 1_150;
        ctx.sim
            .on_poly_event(
                PolyMarketEvent::LastTrade {
                    outcome: Outcome::Down,
                    side: OrderSide::Sell,
                    price: MicroUsdc(dn_order.price.0 - 10_000),
                    size: SharesMicro(19 * ONE_SHARE_MICRO),
                    rx_mono_ns: t_fill_dn * 1_000_000,
                    exch_ts_ms: t_fill_dn,
                    epoch: 0,
                },
                t_fill_dn,
                t_fill_dn * 1_000_000,
                &mut ctx.jsonl,
            )
            .unwrap();
        assert_eq!(ctx.sim.ledger.filled_dn, SharesMicro(19 * ONE_SHARE_MICRO));

        // Jump to T-25s (MandatoryFlatten): cancels remaining resting orders + flattens 19 shares.
        let t_flatten = expiry_ms - 25_000;
        ctx.step(t_flatten).await;
        assert_eq!(ctx.sim.current_phase, Some(EpochPhase::MandatoryFlatten));
        assert_eq!(ctx.sim.stats.flattens_executed, 1);
        assert_eq!(ctx.sim.ledger.net_inventory(), SignedSharesMicro::ZERO);

        // Next tick at T-24s emits Hold with explicit MandatoryFlatten telemetry.
        ctx.step(expiry_ms - 24_000).await;

        let log_contents = ctx.shutdown_and_read_jsonl(expiry_ms - 23_900);

        let mut jsonl_cancel_count = 0usize;
        let mut jsonl_submit_count = 0usize;
        let mut jsonl_replace_count = 0usize;
        let mut jsonl_flatten_count = 0usize;
        let mut jsonl_cancel_all_events = 0usize;
        let mut saw_mandatory_flatten_telemetry = false;
        let mut summary_payload: Option<serde_json::Value> = None;

        for line in log_contents.lines() {
            let entry: serde_json::Value = serde_json::from_str(line).unwrap();
            let ev = entry["event"].as_str().unwrap();
            let payload = &entry["payload"];
            match ev {
                "order_action" => {
                    if payload.get("phase").and_then(|v| v.as_str()) == Some("MandatoryFlatten")
                        && payload.get("flatten_in_flight").is_some()
                        && payload.get("best_ask_up_micro").is_some()
                    {
                        saw_mandatory_flatten_telemetry = true;
                    }
                    if let Some(obj) = payload["action"].as_object() {
                        if obj.contains_key("Cancel") {
                            jsonl_cancel_count += 1;
                        } else if obj.contains_key("SubmitNew") {
                            jsonl_submit_count += 1;
                        } else if obj.contains_key("CancelAndReplace") {
                            jsonl_replace_count += 1;
                        }
                    }
                }
                "flatten" => jsonl_flatten_count += 1,
                "cancel_all" => jsonl_cancel_all_events += 1,
                "summary" => summary_payload = Some(payload.clone()),
                _ => {}
            }
        }

        let summary = summary_payload.expect("summary event must be recorded");
        assert_eq!(
            summary["orders_canceled"].as_u64().unwrap() as usize,
            jsonl_cancel_count
        );
        assert_eq!(
            summary["orders_submitted"].as_u64().unwrap() as usize,
            jsonl_submit_count
        );
        assert_eq!(
            summary["orders_replaced"].as_u64().unwrap() as usize,
            jsonl_replace_count
        );
        assert_eq!(
            summary["flattens_executed"].as_u64().unwrap() as usize,
            jsonl_flatten_count
        );
        assert!(jsonl_cancel_count >= 4);
        assert!(jsonl_cancel_all_events >= 1);
        assert!(saw_mandatory_flatten_telemetry);
    }

    #[test]
    fn sim_epoch_rejects_math_override_flags_explicitly() {
        let err = run(LifecycleArgs {
            source: LifecycleSourceMode::SimEpoch,
            display: LifecycleDisplayMode::Stream,
            input: None,
            duration: "10s".to_string(),
            allow_mid_window: false,
            fast_forward: true,
            fault: CliFaultMode::None,
            v_max: 50,
            base_size: 20,
            queue_decay: paper_sim::QueueDecayPolicy::Conservative,
            math: crate::MathOverrides {
                gamma: Some(0.02),
                ..Default::default()
            },
            pinning: CorePinning::default(),
            jsonl: temp_jsonl_path("sim_epoch_rejects_math"),
        })
        .unwrap_err();
        assert!(
            err.to_string().contains("sim-epoch"),
            "expected sim-epoch error message, got: {err}"
        );
    }

    fn write_replay_fixture(path: &Path, include_second_epoch: bool) {
        let start_ms = 1_700_000_000_000_u64;
        let expiry_ms = start_ms + 300_000;
        let desc1 = test_market("btc-5m-replay-e1", start_ms, expiry_ms);
        let up_snap = test_book(Outcome::Up, 490_000, 510_000, 5, start_ms + 10);
        let dn_snap = test_book(Outcome::Down, 490_000, 510_000, 5, start_ms + 15);
        let base_mono = 1_000_000_000_u64;

        let mut writer = JsonlWriter::create(path).unwrap();
        writer
            .write_event(start_ms, base_mono, "market_discovered", &desc1)
            .unwrap();
        writer
            .write_event(
                start_ms + 5,
                base_mono + 5_000_000,
                "binance_tick",
                &BinanceSpotEvent::BookTicker {
                    bid_e8: 9_000_000_000_000,
                    ask_e8: 9_000_100_000_000,
                    bid_qty_e8: 100_000_000,
                    ask_qty_e8: 100_000_000,
                    update_id: 1,
                    rx_mono_ns: base_mono + 5_000_000,
                },
            )
            .unwrap();
        writer
            .write_event(
                start_ms + 10,
                base_mono + 10_000_000,
                "poly_book",
                &PolyMarketEvent::BookSnapshot {
                    outcome: Outcome::Up,
                    book: up_snap,
                    rx_mono_ns: base_mono + 10_000_000,
                    exch_ts_ms: start_ms + 10,
                    epoch: 0,
                },
            )
            .unwrap();
        writer
            .write_event(
                start_ms + 15,
                base_mono + 15_000_000,
                "poly_book",
                &PolyMarketEvent::BookSnapshot {
                    outcome: Outcome::Down,
                    book: dn_snap,
                    rx_mono_ns: base_mono + 15_000_000,
                    exch_ts_ms: start_ms + 15,
                    epoch: 0,
                },
            )
            .unwrap();
        // Advance past 5-point SG warmup (500ms) + 40ms order latency so bids go Live.
        writer
            .write_event(
                start_ms + 620,
                base_mono + 620_000_000,
                "binance_tick",
                &BinanceSpotEvent::BookTicker {
                    bid_e8: 9_000_000_000_000,
                    ask_e8: 9_000_100_000_000,
                    bid_qty_e8: 100_000_000,
                    ask_qty_e8: 100_000_000,
                    update_id: 2,
                    rx_mono_ns: base_mono + 620_000_000,
                },
            )
            .unwrap();
        // Hit both resting UP and DN bids to trigger deterministic fills + merge.
        writer
            .write_event(
                start_ms + 750,
                base_mono + 750_000_000,
                "poly_trade",
                &PolyMarketEvent::LastTrade {
                    outcome: Outcome::Up,
                    side: OrderSide::Sell,
                    price: MicroUsdc(400_000),
                    size: SharesMicro(15 * ONE_SHARE_MICRO),
                    rx_mono_ns: base_mono + 750_000_000,
                    exch_ts_ms: start_ms + 750,
                    epoch: 0,
                },
            )
            .unwrap();
        writer
            .write_event(
                start_ms + 850,
                base_mono + 850_000_000,
                "poly_trade",
                &PolyMarketEvent::LastTrade {
                    outcome: Outcome::Down,
                    side: OrderSide::Sell,
                    price: MicroUsdc(400_000),
                    size: SharesMicro(15 * ONE_SHARE_MICRO),
                    rx_mono_ns: base_mono + 850_000_000,
                    exch_ts_ms: start_ms + 850,
                    epoch: 0,
                },
            )
            .unwrap();
        writer
            .write_event(
                start_ms + 1_050,
                base_mono + 1_050_000_000,
                "binance_tick",
                &BinanceSpotEvent::AggTrade {
                    price_e8: 9_000_050_000_000,
                    qty_e8: 50_000_000,
                    is_buyer_maker: false,
                    exch_ts_ms: start_ms + 1_050,
                    rx_mono_ns: base_mono + 1_050_000_000,
                },
            )
            .unwrap();

        if include_second_epoch {
            let e2_start = expiry_ms;
            let e2_expiry = e2_start + 300_000;
            let desc2 = test_market("btc-5m-replay-e2", e2_start, e2_expiry);
            let e2_mono = base_mono + 300_100 * 1_000_000;
            writer
                .write_event(e2_start + 100, e2_mono, "market_discovered", &desc2)
                .unwrap();
            writer
                .write_event(
                    e2_start + 110,
                    e2_mono + 10_000_000,
                    "poly_book",
                    &PolyMarketEvent::BookSnapshot {
                        outcome: Outcome::Up,
                        book: test_book(Outcome::Up, 490_000, 500_000, 5, e2_start + 110),
                        rx_mono_ns: e2_mono + 10_000_000,
                        exch_ts_ms: e2_start + 110,
                        epoch: 1,
                    },
                )
                .unwrap();
            writer
                .write_event(
                    e2_start + 115,
                    e2_mono + 15_000_000,
                    "poly_book",
                    &PolyMarketEvent::BookSnapshot {
                        outcome: Outcome::Down,
                        book: test_book(Outcome::Down, 500_000, 510_000, 5, e2_start + 115),
                        rx_mono_ns: e2_mono + 15_000_000,
                        exch_ts_ms: e2_start + 115,
                        epoch: 1,
                    },
                )
                .unwrap();
            writer
                .write_event(
                    e2_start + 350,
                    e2_mono + 250_000_000,
                    "binance_tick",
                    &BinanceSpotEvent::AggTrade {
                        price_e8: 9_001_000_000_000,
                        qty_e8: 50_000_000,
                        is_buyer_maker: false,
                        exch_ts_ms: e2_start + 350,
                        rx_mono_ns: e2_mono + 250_000_000,
                    },
                )
                .unwrap();
        }
        writer.close().unwrap();
    }

    fn read_summary_payload(jsonl_path: &Path) -> serde_json::Value {
        let contents = std::fs::read_to_string(jsonl_path).unwrap();
        contents
            .lines()
            .filter_map(|l| serde_json::from_str::<serde_json::Value>(l).ok())
            .find(|v| v["event"].as_str() == Some("summary"))
            .expect("summary event must be recorded")["payload"]
            .clone()
    }

    #[test]
    fn replay_cli_validation_and_math_overrides() {
        let fixture = temp_jsonl_path("replay_val_in");
        let out = temp_jsonl_path("replay_val_out");
        write_replay_fixture(&fixture, false);

        // `--input` rejected with `--source live`.
        let err_live = run(LifecycleArgs {
            source: LifecycleSourceMode::Live,
            display: LifecycleDisplayMode::Stream,
            input: Some(fixture.clone()),
            duration: "10s".to_string(),
            allow_mid_window: false,
            fast_forward: false,
            fault: CliFaultMode::None,
            v_max: 50,
            base_size: 20,
            queue_decay: paper_sim::QueueDecayPolicy::Conservative,
            math: crate::MathOverrides::default(),
            pinning: CorePinning::default(),
            jsonl: out.clone(),
        })
        .unwrap_err();
        assert!(err_live.to_string().contains("--input"), "err={err_live}");

        // `--input == --jsonl` rejected with `--source replay`.
        let err_overwrite = run(LifecycleArgs {
            source: LifecycleSourceMode::Replay,
            display: LifecycleDisplayMode::Stream,
            input: Some(fixture.clone()),
            duration: "10s".to_string(),
            allow_mid_window: false,
            fast_forward: false,
            fault: CliFaultMode::None,
            v_max: 50,
            base_size: 20,
            queue_decay: paper_sim::QueueDecayPolicy::Conservative,
            math: crate::MathOverrides::default(),
            pinning: CorePinning::default(),
            jsonl: fixture.clone(),
        })
        .unwrap_err();
        assert!(
            err_overwrite.to_string().contains("overwrite"),
            "err={err_overwrite}"
        );

        // Math overrides + queue-decay accepted with `--source replay`.
        let ok_res = run(LifecycleArgs {
            source: LifecycleSourceMode::Replay,
            display: LifecycleDisplayMode::Stream,
            input: Some(fixture.clone()),
            duration: "10s".to_string(),
            allow_mid_window: false,
            fast_forward: false,
            fault: CliFaultMode::None,
            v_max: 50,
            base_size: 20,
            queue_decay: paper_sim::QueueDecayPolicy::Optimistic,
            math: crate::MathOverrides {
                gamma: Some(0.02),
                include_variance_spread: Some(false),
                min_spread_ticks: Some(1),
                max_combined_bid_micro: Some(990_000),
                fee_spread_floor: Some(false),
                ..Default::default()
            },
            pinning: CorePinning::default(),
            jsonl: out.clone(),
        });
        assert!(ok_res.is_ok(), "replay failed: {ok_res:?}");

        let _ = std::fs::remove_file(&fixture);
        let _ = std::fs::remove_file(&out);
    }

    #[test]
    fn replay_is_deterministic_across_runs() {
        let fixture = temp_jsonl_path("replay_det_in");
        let out_a = temp_jsonl_path("replay_det_a");
        let out_b = temp_jsonl_path("replay_det_b");
        write_replay_fixture(&fixture, false);

        let make_args = |jsonl: PathBuf| LifecycleArgs {
            source: LifecycleSourceMode::Replay,
            display: LifecycleDisplayMode::Stream,
            input: Some(fixture.clone()),
            duration: "10s".to_string(),
            allow_mid_window: false,
            fast_forward: false,
            fault: CliFaultMode::None,
            v_max: 50,
            base_size: 20,
            queue_decay: paper_sim::QueueDecayPolicy::Optimistic,
            math: crate::MathOverrides {
                gamma: Some(0.02),
                include_variance_spread: Some(false),
                min_spread_ticks: Some(1),
                max_combined_bid_micro: Some(990_000),
                fee_spread_floor: Some(false),
                ..Default::default()
            },
            pinning: CorePinning::default(),
            jsonl,
        };

        run(make_args(out_a.clone())).unwrap();
        run(make_args(out_b.clone())).unwrap();

        let sum_a = read_summary_payload(&out_a);
        let sum_b = read_summary_payload(&out_b);
        for key in [
            "ticks_evaluated",
            "orders_submitted",
            "orders_replaced",
            "orders_canceled",
            "orders_held",
            "fills_count",
            "merges_count",
            "realized_pnl_micro",
            "unrealized_pnl_micro",
            "baseline_pnl_micro",
        ] {
            assert_eq!(sum_a[key], sum_b[key], "mismatch on {key}");
        }
        assert!(sum_a["fills_count"].as_u64().unwrap() >= 1);

        let _ = std::fs::remove_file(&fixture);
        let _ = std::fs::remove_file(&out_a);
        let _ = std::fs::remove_file(&out_b);
    }

    #[test]
    fn replay_handles_multi_epoch_rollover_cleanly() {
        let fixture = temp_jsonl_path("replay_multi_in");
        let out = temp_jsonl_path("replay_multi_out");
        write_replay_fixture(&fixture, true);

        run(LifecycleArgs {
            source: LifecycleSourceMode::Replay,
            display: LifecycleDisplayMode::Stream,
            input: Some(fixture.clone()),
            duration: "10s".to_string(),
            allow_mid_window: false,
            fast_forward: false,
            fault: CliFaultMode::None,
            v_max: 50,
            base_size: 20,
            queue_decay: paper_sim::QueueDecayPolicy::Conservative,
            math: crate::MathOverrides::default(),
            pinning: CorePinning::default(),
            jsonl: out.clone(),
        })
        .unwrap();

        let sum = read_summary_payload(&out);
        assert_eq!(sum["epoch_rollovers"].as_u64().unwrap(), 1);
        let contents = std::fs::read_to_string(&out).unwrap();
        assert!(contents.contains("\"event\":\"epoch_rollover\""));

        let _ = std::fs::remove_file(&fixture);
        let _ = std::fs::remove_file(&out);
    }
}
