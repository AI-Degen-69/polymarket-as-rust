use std::error::Error;
use std::path::PathBuf;
use std::time::Duration;

use core_types::{
    ClockOffsetEstimator, EngineConfig, FixedBook, HighResTimerGuard, JsonlWriter, LatencyTracker,
    MarketDescriptor, MicroUsdc, MonotonicClock, ONE_SHARE_MICRO, Outcome, SharesMicro,
    SignedSharesMicro,
};
use feeds::{
    BinanceSpotEvent, BookMaintainer, DEFAULT_RING_CAPACITY, GammaDiscoveryClient, PolyMarketEvent,
};
use math_engine::{MathPipeline, MathPipelineConfig, PipelineInput, QuoteOutput};
use risk_lifecycle::{
    ActorPipelineHarness, BreakerConfig, BreakerStatus, CircuitBreakers, DollarNotionalRiskEngine,
    EpochLifecycleController, EpochPhase, FaultInjection, FlattenAction, HysteresisConfig,
    HysteresisGate, LifecycleConfig, OrderRegistrySnapshot, QuoteProposal, RiskActor, RiskConfig,
    RiskTier,
};
use rtrb::RingBuffer;
use tokio::sync::mpsc;

use crate::feeds_cmd::{parse_duration_arg, shutdown_actors, spawn_actors};
use crate::lifecycle_report::{
    LifecycleReportStats, LifecycleStepRow, print_breaker_trip, print_lifecycle_header,
    print_lifecycle_step, print_lifecycle_summary, print_phase_transition, print_tier_transition,
    record_breaker_jsonl, record_order_action_jsonl, record_phase_transition_jsonl,
    record_risk_tier_jsonl,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LifecycleSourceMode {
    Live,
    SimEpoch,
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
    pub(crate) duration: String,
    pub(crate) fast_forward: bool,
    pub(crate) fault: CliFaultMode,
    pub(crate) v_max: u64,
    pub(crate) base_size: u64,
    pub(crate) jsonl: PathBuf,
}

pub(crate) fn run(args: LifecycleArgs) -> Result<(), Box<dyn Error>> {
    let _timer_guard = HighResTimerGuard::acquire_1ms()?;
    let clock = MonotonicClock::start();

    print_lifecycle_header(
        match args.source {
            LifecycleSourceMode::Live => "Live WebSockets",
            LifecycleSourceMode::SimEpoch => "Simulated 5m Epoch (SimEpoch)",
        },
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

    let mut current_phase: Option<EpochPhase> = None;
    let mut current_tier = RiskTier::Tier1Normal;
    let mut sim_q_up = SharesMicro::ZERO;
    let sim_q_dn = SharesMicro::ZERO;
    let mut sim_cost_up = MicroUsdc::ZERO;
    let sim_cost_dn = MicroUsdc::ZERO;

    // Simulation steps: advance from t=0s to t=305s
    let step_secs = if args.fast_forward { 5 } else { 1 };
    let total_steps = 305 / step_secs;

    for step_idx in 0..=total_steps {
        let elapsed_secs = (step_idx * step_secs).min(305);
        let now_ms = start_ms + (elapsed_secs * 1000);
        let now_ns = clock.now_ns()? + (elapsed_secs * 1_000_000_000);
        let tau_secs = expiry_ms.saturating_sub(now_ms) / 1000;

        // Simulated market pricing & BBO
        let mid_up = 0.50 + 0.02 * ((elapsed_secs as f64 / 30.0).sin());
        let bid_up_val = ((mid_up - 0.02).clamp(0.05, 0.95) * 1_000_000.0).round() as u64;
        let bid_dn_val = (((1.0 - mid_up) - 0.02).clamp(0.05, 0.95) * 1_000_000.0).round() as u64;

        let pricing_start = clock.now_ns()?;
        let proposal = QuoteProposal {
            seq: step_idx + 1,
            now_ms,
            expiry_ms,
            tick_size_micro: MicroUsdc(10_000),
            quote: QuoteOutput::Active {
                bid_up: Some(MicroUsdc(bid_up_val)),
                bid_dn: Some(MicroUsdc(bid_dn_val)),
                pair_cost_micro: Some(MicroUsdc(bid_up_val + bid_dn_val)),
                gross_margin_bps: Some(400),
            },
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
                    risk_lifecycle::ExecutionCommand::CancelAll { .. } => {
                        stats.orders_canceled += 2;
                    }
                    risk_lifecycle::ExecutionCommand::ExecuteFlattenFak { .. } => {
                        stats.flattens_executed += 1;
                    }
                    risk_lifecycle::ExecutionCommand::Cancel { .. } => {
                        stats.orders_canceled += 1;
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
                        stats.orders_replaced += 1;
                        let _ = risk_actor.hysteresis_mut().on_cancel_ack(outcome);
                        risk_actor.hysteresis_mut().on_place_ack(
                            outcome,
                            format!("sim_{:?}_{}", outcome, step_idx),
                            new_price,
                            new_size,
                        );
                    }
                    risk_lifecycle::ExecutionCommand::Cancel { outcome, .. } => {
                        stats.orders_canceled += 1;
                        let _ = risk_actor.hysteresis_mut().on_cancel_ack(outcome);
                    }
                    _ => {}
                }
            }

            let net_delta = (sim_q_up.as_u64() as i64) - (sim_q_dn.as_u64() as i64);

            print_lifecycle_step(&LifecycleStepRow {
                tau_secs,
                phase: trace.phase,
                tier: trace.tier,
                u_e6: trace.utilization_u_e6,
                q_up: sim_q_up,
                q_dn: sim_q_dn,
                net_delta: net_delta / 1_000_000,
                action_up: &trace.action_up,
                action_dn: &trace.action_dn,
                pricing_ns,
                risk_ns,
            });

            record_order_action_jsonl(
                &mut jsonl,
                now_ms,
                now_ns,
                Outcome::Up,
                &trace.action_up,
                tau_secs,
            )?;
            record_order_action_jsonl(
                &mut jsonl,
                now_ms,
                now_ns,
                Outcome::Down,
                &trace.action_dn,
                tau_secs,
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

    let p_summary = pricing_lat.summary();
    let r_summary = risk_lat.summary();
    let t_summary = total_lat.summary();

    print_lifecycle_summary(&stats, &p_summary, &r_summary, &t_summary);
    Ok(())
}

async fn run_live_epoch(args: LifecycleArgs, clock: MonotonicClock) -> Result<(), Box<dyn Error>> {
    let cfg = EngineConfig::from_env()?;
    feeds::install_tls_provider();
    let mut jsonl = JsonlWriter::create(&args.jsonl)?;
    let mut pricing_lat = LatencyTracker::new()?;
    let mut risk_lat = LatencyTracker::new()?;
    let mut total_lat = LatencyTracker::new()?;
    let mut stats = LifecycleReportStats::default();

    let duration_limit = parse_duration_arg(&args.duration)?;

    println!("  [1/4] Discovering active Polymarket 5-minute BTC market...");
    let (discovery, offset_est, desc) = discover_live_market(&cfg).await?;
    println!(
        "  Discovered: {} (expires in {}s)",
        desc.slug,
        desc.expiry_ts_ms
            .saturating_sub(MonotonicClock::unix_now_ms()?)
            / 1000
    );

    let (poly_prod, mut poly_cons) = RingBuffer::new(DEFAULT_RING_CAPACITY);
    let (bin_prod, mut binance_cons) = RingBuffer::new(DEFAULT_RING_CAPACITY);
    let (ora_prod, _oracle_cons) = RingBuffer::new(DEFAULT_RING_CAPACITY);
    let (health_tx, _health_rx) = mpsc::channel(1024);

    let handles = spawn_actors(
        &cfg,
        &desc,
        &discovery,
        clock,
        (poly_prod, bin_prod, ora_prod),
        health_tx,
    )?;

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

    let breaker_config = BreakerConfig::default();
    let breakers = CircuitBreakers::new(breaker_config, desc.tick_size_micro, clock.now_ns()?);

    let mut risk_actor = RiskActor::new(
        harness.clone(),
        risk_engine,
        hysteresis,
        lifecycle,
        breakers,
        args.fault.into(),
    );

    let pipe_cfg = MathPipelineConfig::from_engine_config(&cfg)?;
    let mut math_pipeline = MathPipeline::new(pipe_cfg, clock)?;

    let mut book_up = BookMaintainer::new(Outcome::Up);
    let mut book_dn = BookMaintainer::new(Outcome::Down);
    let mut latest_up: Option<FixedBook<10>> = None;
    let mut latest_dn: Option<FixedBook<10>> = None;
    let fallback_up = FixedBook::<10>::empty(Outcome::Up);
    let fallback_dn = FixedBook::<10>::empty(Outcome::Down);

    let deadline = tokio::time::Instant::now() + duration_limit;
    let mut step_count = 0u64;
    let mut current_phase: Option<EpochPhase> = None;
    let mut current_tier = RiskTier::Tier1Normal;
    let mut latest_spot: Option<f64> = None;

    while tokio::time::Instant::now() < deadline {
        let now_ms = MonotonicClock::unix_now_ms()?;
        let now_ns = clock.now_ns()?;
        let tau_secs = desc.expiry_ts_ms.saturating_sub(now_ms) / 1000;

        // Drain feed events
        while let Ok(ev) = poly_cons.pop() {
            match ev {
                PolyMarketEvent::BookSnapshot {
                    outcome,
                    book,
                    rx_mono_ns,
                    exch_ts_ms,
                } => match outcome {
                    Outcome::Up => {
                        let _ = book_up.apply_snapshot(
                            book.bids_slice(),
                            book.asks_slice(),
                            rx_mono_ns,
                            exch_ts_ms,
                        );
                        latest_up = Some(book);
                    }
                    Outcome::Down => {
                        let _ = book_dn.apply_snapshot(
                            book.bids_slice(),
                            book.asks_slice(),
                            rx_mono_ns,
                            exch_ts_ms,
                        );
                        latest_dn = Some(book);
                    }
                },
                PolyMarketEvent::PriceChange {
                    outcome,
                    side,
                    price,
                    size,
                    rx_mono_ns,
                    exch_ts_ms,
                    ..
                } => match outcome {
                    Outcome::Up => {
                        let _ = book_up
                            .apply_price_change_delta(side, price, size, rx_mono_ns, exch_ts_ms);
                        if let Ok(top) = book_up.top_n::<10>() {
                            latest_up = Some(top);
                        }
                    }
                    Outcome::Down => {
                        let _ = book_dn
                            .apply_price_change_delta(side, price, size, rx_mono_ns, exch_ts_ms);
                        if let Ok(top) = book_dn.top_n::<10>() {
                            latest_dn = Some(top);
                        }
                    }
                },
                _ => {}
            }
        }

        while let Ok(ev) = binance_cons.pop() {
            match ev {
                BinanceSpotEvent::BookTicker { bid_e8, ask_e8, .. } => {
                    latest_spot = Some((bid_e8 as f64 + ask_e8 as f64) / 200_000_000.0);
                }
                BinanceSpotEvent::AggTrade { price_e8, .. } => {
                    latest_spot = Some(price_e8 as f64 / 100_000_000.0);
                }
            }
        }

        let snap_up = latest_up.as_ref().unwrap_or(&fallback_up);
        let snap_dn = latest_dn.as_ref().unwrap_or(&fallback_dn);

        let pipe_input = PipelineInput {
            now_ms,
            expiry_ms: desc.expiry_ts_ms,
            tick_size_micro: desc.tick_size_micro,
            fee_schedule: desc.fee_schedule,
            inventory_q: SignedSharesMicro(0),
            book_up: snap_up,
            book_dn: snap_dn,
            spot_usd: latest_spot,
            strike_usd: desc.strike_price_usd.map(|f| f.get()),
            active_bid_up: None,
            active_bid_dn: None,
        };

        let trace = math_pipeline.evaluate(&pipe_input)?;
        pricing_lat.record_ns(trace.timings.total_compute_ns)?;

        step_count += 1;
        let proposal = QuoteProposal {
            seq: step_count,
            now_ms,
            expiry_ms: desc.expiry_ts_ms,
            tick_size_micro: desc.tick_size_micro,
            quote: trace.quote,
            bid_up_raw: trace.pricer.map_or(0.0, |p| p.bid_up_raw),
            bid_dn_raw: trace.pricer.map_or(0.0, |p| p.bid_dn_raw),
            best_bid_up: trace.best_bid_up,
            best_bid_dn: trace.best_bid_dn,
            best_ask_up: trace.best_ask_up,
            best_ask_dn: trace.best_ask_dn,
            defensive_cancel: trace.flags.defensive_cancel,
            total_compute_ns: trace.timings.total_compute_ns,
        };
        harness.publish_quote(proposal);

        let risk_start = clock.now_ns()?;
        let clock_drift = offset_est.offset_ms().unwrap_or(0);
        let trace_opt = risk_actor.step(now_ns, clock_drift)?;
        let risk_ns = clock.now_ns()?.saturating_sub(risk_start);
        risk_lat.record_ns(risk_ns)?;
        total_lat.record_ns(trace.timings.total_compute_ns + risk_ns)?;

        if let Some(r_trace) = trace_opt {
            stats.ticks_evaluated += 1;

            if current_phase != Some(r_trace.phase) {
                stats.phase_transitions += 1;
                print_phase_transition(
                    current_phase,
                    r_trace.phase,
                    tau_secs,
                    "Live epoch horizon",
                );
                record_phase_transition_jsonl(
                    &mut jsonl,
                    now_ms,
                    now_ns,
                    current_phase,
                    r_trace.phase,
                    tau_secs,
                    "Live epoch horizon transition",
                )?;
                current_phase = Some(r_trace.phase);
            }

            if current_tier != r_trace.tier {
                print_tier_transition(
                    r_trace.tier,
                    r_trace.utilization_u_e6,
                    r_trace.flatten_action.outcome(),
                );
                record_risk_tier_jsonl(
                    &mut jsonl,
                    now_ms,
                    now_ns,
                    r_trace.tier,
                    r_trace.utilization_u_e6,
                    r_trace.flatten_action.outcome(),
                )?;
                current_tier = r_trace.tier;
            }

            // Drain channels
            while let Ok(cmd) = priority_rx.try_recv() {
                match cmd {
                    risk_lifecycle::ExecutionCommand::CancelAll { .. } => {
                        stats.orders_canceled += 2
                    }
                    risk_lifecycle::ExecutionCommand::ExecuteFlattenFak { .. } => {
                        stats.flattens_executed += 1
                    }
                    _ => {}
                }
            }

            while let Ok(cmd) = routine_rx.try_recv() {
                match cmd {
                    risk_lifecycle::ExecutionCommand::SubmitNew {
                        outcome,
                        price,
                        size,
                    } => {
                        stats.orders_submitted += 1;
                        risk_actor.hysteresis_mut().on_place_ack(
                            outcome,
                            format!("live_{step_count}"),
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
                        stats.orders_replaced += 1;
                        let _ = risk_actor.hysteresis_mut().on_cancel_ack(outcome);
                        risk_actor.hysteresis_mut().on_place_ack(
                            outcome,
                            format!("live_{step_count}"),
                            new_price,
                            new_size,
                        );
                    }
                    _ => {}
                }
            }

            print_lifecycle_step(&LifecycleStepRow {
                tau_secs,
                phase: r_trace.phase,
                tier: r_trace.tier,
                u_e6: r_trace.utilization_u_e6,
                q_up: SharesMicro::ZERO,
                q_dn: SharesMicro::ZERO,
                net_delta: 0,
                action_up: &r_trace.action_up,
                action_dn: &r_trace.action_dn,
                pricing_ns: trace.timings.total_compute_ns,
                risk_ns,
            });

            record_order_action_jsonl(
                &mut jsonl,
                now_ms,
                now_ns,
                Outcome::Up,
                &r_trace.action_up,
                tau_secs,
            )?;
            record_order_action_jsonl(
                &mut jsonl,
                now_ms,
                now_ns,
                Outcome::Down,
                &r_trace.action_dn,
                tau_secs,
            )?;
        }

        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    shutdown_actors(handles, cfg.polybolt_enabled).await;

    let p_summary = pricing_lat.summary();
    let r_summary = risk_lat.summary();
    let t_summary = total_lat.summary();

    print_lifecycle_summary(&stats, &p_summary, &r_summary, &t_summary);
    Ok(())
}

async fn discover_live_market(
    cfg: &EngineConfig,
) -> Result<(GammaDiscoveryClient, ClockOffsetEstimator, MarketDescriptor), Box<dyn Error>> {
    let discovery = GammaDiscoveryClient::new()?;
    let offset_est = ClockOffsetEstimator::new(cfg.clock_drift_max_ms);
    let _sync = discovery.sync_clock(&offset_est).await?;
    let adj_now = offset_est.adjusted_unix_ms(MonotonicClock::unix_now_ms()?)?;
    let desc = discovery.discover_active_5m_btc(adj_now, 15_000).await?;
    Ok((discovery, offset_est, desc))
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
