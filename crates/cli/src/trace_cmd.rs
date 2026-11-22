use std::error::Error;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::time::Duration;

use core_types::{
    ClockOffsetEstimator, EngineConfig, FixedBook, HighResTimerGuard, JsonlWriter, KappaMode,
    LatencyTracker, MarketDescriptor, MicroUsdc, MonotonicClock, ONE_SHARE_MICRO, Outcome, SgMode,
    SignedSharesMicro, TickPrecision, VarianceMode,
};
use feeds::{
    BinanceSpotEvent, BookMaintainer, DEFAULT_RING_CAPACITY, FeedHealthEvent, GammaDiscoveryClient,
    OracleTwapEvent, PolyMarketEvent,
};
use math_engine::{CostContext, MathPipeline, MathPipelineConfig, PipelineInput};
use rtrb::RingBuffer;
use tokio::sync::mpsc;

use crate::feeds_cmd::{ActorHandles, shutdown_actors, spawn_actors};
use crate::feeds_report::{RawJsonlEnvelope, ReplayFixedBook, ReplayPolyEvent};
use crate::trace_report::{
    TraceReportStats, print_trace_header, record_and_print_tick, write_and_print_summary,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TraceSourceMode {
    Live,
    Replay,
}

impl TraceSourceMode {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Live => "live",
            Self::Replay => "replay",
        }
    }
}

pub(crate) struct TraceArgs {
    pub(crate) source: TraceSourceMode,
    pub(crate) input: Option<PathBuf>,
    pub(crate) sg_mode: SgMode,
    pub(crate) kappa_mode: KappaMode,
    pub(crate) variance_mode: VarianceMode,
    pub(crate) ticks: usize,
    pub(crate) inventory: i64,
    pub(crate) math: crate::MathOverrides,
    pub(crate) jsonl: PathBuf,
}

struct TraceState {
    desc: Option<MarketDescriptor>,
    book_up: BookMaintainer,
    book_dn: BookMaintainer,
    latest_up: Option<FixedBook<10>>,
    latest_dn: Option<FixedBook<10>>,
    spot_usd: Option<f64>,
    strike_usd: Option<f64>,
    strike_source: &'static str,
    replay_time_anchor: Option<(u64, u64)>,
    max_outer_mono: u64,
    next_eval_ms: Option<u64>,
    stats: TraceReportStats,
}

impl TraceState {
    fn new() -> Self {
        Self {
            desc: None,
            book_up: BookMaintainer::new(Outcome::Up),
            book_dn: BookMaintainer::new(Outcome::Down),
            latest_up: None,
            latest_dn: None,
            spot_usd: None,
            strike_usd: None,
            strike_source: "none",
            replay_time_anchor: None,
            max_outer_mono: 0,
            next_eval_ms: None,
            stats: TraceReportStats::default(),
        }
    }

    fn set_market(&mut self, desc: MarketDescriptor) {
        if let Some(k) = desc.strike_price_usd {
            self.strike_usd = Some(k.get());
            self.strike_source = "market_metadata";
        } else {
            self.strike_usd = None;
            self.strike_source = "pending_first_spot";
        }
        self.desc = Some(desc);
    }

    fn observe_spot(&mut self, spot: f64) {
        if spot > 0.0 && spot.is_finite() {
            self.spot_usd = Some(spot);
            if self.strike_usd.is_none() {
                self.strike_usd = Some(spot);
                self.strike_source = "inferred_first_spot";
            }
        }
    }
}

struct LiveRings {
    poly_cons: rtrb::Consumer<PolyMarketEvent>,
    binance_cons: rtrb::Consumer<BinanceSpotEvent>,
    oracle_cons: rtrb::Consumer<OracleTwapEvent>,
    health_rx: mpsc::Receiver<FeedHealthEvent>,
}

pub(crate) fn run(args: TraceArgs) -> Result<(), Box<dyn Error>> {
    if args.ticks == 0 {
        return Err("`--ticks` must be >= 1".into());
    }
    let inv_micro = args
        .inventory
        .checked_mul(ONE_SHARE_MICRO as i64)
        .ok_or("`--inventory` share count overflows i64 micro-units")?;
    let inventory_q = SignedSharesMicro(inv_micro);

    let mut cfg = EngineConfig::from_env()?;
    cfg.sg_mode = args.sg_mode;
    cfg.kappa_mode = args.kappa_mode;
    cfg.variance_mode = args.variance_mode;
    args.math.apply_to(&mut cfg)?;

    match args.source {
        TraceSourceMode::Replay => {
            let input_path =
                crate::log_paths::resolve_replay_input(args.input.as_deref(), &args.jsonl)?;
            run_replay(&args, &input_path, &cfg, inventory_q)
        }
        TraceSourceMode::Live => {
            if args.input.is_some() {
                return Err("`--input` is only valid with `--source replay`".into());
            }
            let rt = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()?;
            rt.block_on(run_live(&args, &cfg, inventory_q))
        }
    }
}

fn run_replay(
    args: &TraceArgs,
    input_path: &Path,
    cfg: &EngineConfig,
    inventory_q: SignedSharesMicro,
) -> Result<(), Box<dyn Error>> {
    let clock = MonotonicClock::start();
    let pipe_cfg = MathPipelineConfig::from_engine_config(cfg)?;
    let mut pipe = MathPipeline::new(pipe_cfg, clock)?;
    let mut tracker = LatencyTracker::new()?;
    let mut state = TraceState::new();
    let mut jsonl = JsonlWriter::create(&args.jsonl)?;
    let mut header_printed = false;

    let reader = BufReader::new(File::open(input_path)?);
    for line_res in reader.lines() {
        let line = line_res?;
        if line.trim().is_empty() {
            continue;
        }
        let env: RawJsonlEnvelope = serde_json::from_str(&line)?;
        if env.mono_ns < state.max_outer_mono {
            return Err(format!(
                "non-monotonic replay mono_ns: {} < {}",
                env.mono_ns, state.max_outer_mono
            )
            .into());
        }
        state.max_outer_mono = env.mono_ns;
        let _ = state
            .replay_time_anchor
            .get_or_insert((env.ts_unix_ms, env.mono_ns));
        process_replay_env(
            env,
            &mut state,
            &mut pipe,
            cfg,
            args,
            inventory_q,
            (&mut header_printed, &mut tracker, &mut jsonl, &clock),
        )?;
        if state.stats.evaluated_ticks >= args.ticks {
            break;
        }
    }

    finalize_replay_output(&state, args, &tracker, jsonl, &clock)
}

fn finalize_replay_output(
    state: &TraceState,
    args: &TraceArgs,
    tracker: &LatencyTracker,
    mut jsonl: JsonlWriter,
    clock: &MonotonicClock,
) -> Result<(), Box<dyn Error>> {
    let slug = state.desc.as_ref().map_or("unknown", |d| d.slug.as_str());
    write_and_print_summary(
        TraceSourceMode::Replay.as_str(),
        slug,
        state.strike_source,
        &state.stats,
        tracker.summary(),
        (&mut jsonl, clock, &args.jsonl),
    )?;
    jsonl.close()?;
    if state.stats.evaluated_ticks < args.ticks {
        return Err(format!(
            "replay ended after {} post-warmup ticks (< requested {})",
            state.stats.evaluated_ticks, args.ticks
        )
        .into());
    }
    Ok(())
}

fn process_replay_env(
    env: RawJsonlEnvelope,
    state: &mut TraceState,
    pipe: &mut MathPipeline,
    cfg: &EngineConfig,
    args: &TraceArgs,
    inventory_q: SignedSharesMicro,
    (header_printed, tracker, jsonl, clock): (
        &mut bool,
        &mut LatencyTracker,
        &mut JsonlWriter,
        &MonotonicClock,
    ),
) -> Result<(), Box<dyn Error>> {
    let anchor = state
        .replay_time_anchor
        .unwrap_or((env.ts_unix_ms, env.mono_ns));
    match env.event.as_str() {
        "market_discovered" => {
            let desc: MarketDescriptor = serde_json::from_value(env.payload)?;
            desc.validate()?;
            state.set_market(desc);
        }
        "binance_tick" => {
            let ev: BinanceSpotEvent = serde_json::from_value(env.payload)?;
            let rx_ns = binance_rx_mono_ns(&ev);
            let mono_for_ev = if rx_ns > 0 { rx_ns } else { env.mono_ns };
            let ev_ts_ms = mono_ns_to_adjusted_ms(anchor, mono_for_ev);
            apply_binance_to_state(ev_ts_ms, ev, state, pipe)?;
        }
        "poly_book" | "poly_price_change" | "poly_trade" | "poly_tick_size" | "poly_bba" => {
            let ev: ReplayPolyEvent = serde_json::from_value(env.payload)?;
            apply_replay_poly_to_state(ev, state, pipe)?;
        }
        "clock_sync" | "feed_health" | "polybolt_event" | "feeds_summary" => return Ok(()),
        other => return Err(format!("unknown event `{other}` in replay input").into()),
    }
    maybe_eval_replay_tick(
        mono_ns_to_adjusted_ms(anchor, env.mono_ns),
        state,
        pipe,
        cfg,
        args,
        inventory_q,
        (header_printed, tracker, jsonl, clock),
    )
}

fn maybe_eval_replay_tick(
    now_ms: u64,
    state: &mut TraceState,
    pipe: &mut MathPipeline,
    cfg: &EngineConfig,
    args: &TraceArgs,
    inventory_q: SignedSharesMicro,
    (header_printed, tracker, jsonl, clock): (
        &mut bool,
        &mut LatencyTracker,
        &mut JsonlWriter,
        &MonotonicClock,
    ),
) -> Result<(), Box<dyn Error>> {
    if state.next_eval_ms.is_some_and(|next| now_ms < next) {
        return Ok(());
    }
    let (Some(desc), Some(up), Some(dn)) = (state.desc.clone(), state.latest_up, state.latest_dn)
    else {
        return Ok(());
    };
    state.next_eval_ms = Some(now_ms.saturating_add(100));
    if !*header_printed {
        print_trace_header(
            TraceSourceMode::Replay.as_str(),
            &desc,
            cfg,
            state.strike_usd,
            state.strike_source,
            &args.jsonl,
        );
        *header_printed = true;
    }
    eval_and_record_tick(
        TraceSourceMode::Replay.as_str(),
        now_ms,
        inventory_q,
        (&desc, cfg, &up, &dn),
        state,
        pipe,
        (tracker, jsonl, clock),
    )
}

fn eval_and_record_tick(
    source: &str,
    now_ms: u64,
    inventory_q: SignedSharesMicro,
    (desc, cfg, up, dn): (
        &MarketDescriptor,
        &EngineConfig,
        &FixedBook<10>,
        &FixedBook<10>,
    ),
    state: &mut TraceState,
    pipe: &mut MathPipeline,
    (tracker, jsonl, clock): (&mut LatencyTracker, &mut JsonlWriter, &MonotonicClock),
) -> Result<(), Box<dyn Error>> {
    let inp = PipelineInput {
        now_ms,
        expiry_ms: desc.expiry_ts_ms,
        tick_size_micro: desc.tick_size_micro,
        fee_schedule: desc.fee_schedule,
        inventory_q,
        book_up: up,
        book_dn: dn,
        spot_usd: state.spot_usd,
        strike_usd: state.strike_usd,
        active_bid_up: None,
        active_bid_dn: None,
        cost_context: CostContext::default(),
    };
    let trace = pipe.evaluate(&inp)?;
    if trace.flags.sg_warming_up {
        state.stats.warmup_ticks = state.stats.warmup_ticks.saturating_add(1);
        return Ok(());
    }
    tracker.record_ns(trace.total_compute_ns)?;
    let tick_num = state.stats.evaluated_ticks.saturating_add(1);
    record_and_print_tick(
        tick_num,
        source,
        state.strike_source,
        (desc, cfg, up, dn),
        trace,
        (&mut state.stats, jsonl, clock),
    )
}

async fn run_live(
    args: &TraceArgs,
    cfg: &EngineConfig,
    inventory_q: SignedSharesMicro,
) -> Result<(), Box<dyn Error>> {
    let clock = MonotonicClock::start();
    let _timer_guard = HighResTimerGuard::acquire_1ms()?;
    feeds::install_tls_provider();

    let mut pipe = MathPipeline::new(MathPipelineConfig::from_engine_config(cfg)?, clock)?;
    let mut tracker = LatencyTracker::new()?;
    let mut jsonl = JsonlWriter::create(&args.jsonl)?;
    let (discovery, offset_est, desc) = discover_live_market(cfg).await?;
    let base_adj_ms = offset_est.adjusted_unix_ms(MonotonicClock::unix_now_ms()?)?;
    let base_mono_ns = clock.now_ns()?;
    let mut state = TraceState::new();
    state.set_market(desc.clone());
    print_trace_header(
        TraceSourceMode::Live.as_str(),
        &desc,
        cfg,
        state.strike_usd,
        state.strike_source,
        &args.jsonl,
    );

    let (handles, mut rings) = spawn_live_pipeline(cfg, &desc, &discovery, clock)?;
    let loop_res = drive_live_trace(
        (args, cfg, inventory_q, (base_adj_ms, base_mono_ns)),
        &mut rings,
        &mut state,
        &mut pipe,
        (&mut tracker, &mut jsonl, &clock),
    )
    .await;
    finish_live_trace(
        handles,
        &mut rings,
        (cfg, args, (base_adj_ms, base_mono_ns)),
        loop_res,
        (&mut state, &mut pipe, &tracker, jsonl, &clock),
    )
    .await
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

fn spawn_live_pipeline(
    cfg: &EngineConfig,
    desc: &MarketDescriptor,
    discovery: &GammaDiscoveryClient,
    clock: MonotonicClock,
) -> Result<(ActorHandles, LiveRings), Box<dyn Error>> {
    let (poly_prod, poly_cons) = RingBuffer::new(DEFAULT_RING_CAPACITY);
    let (bin_prod, binance_cons) = RingBuffer::new(DEFAULT_RING_CAPACITY);
    let (ora_prod, oracle_cons) = RingBuffer::new(DEFAULT_RING_CAPACITY);
    let (health_tx, health_rx) = mpsc::channel(1024);
    let handles = spawn_actors(
        cfg,
        desc,
        discovery,
        clock,
        (poly_prod, bin_prod, ora_prod),
        health_tx,
    )?;
    let rings = LiveRings {
        poly_cons,
        binance_cons,
        oracle_cons,
        health_rx,
    };
    Ok((handles, rings))
}

const fn binance_rx_mono_ns(ev: &BinanceSpotEvent) -> u64 {
    match *ev {
        BinanceSpotEvent::BookTicker { rx_mono_ns, .. }
        | BinanceSpotEvent::AggTrade { rx_mono_ns, .. } => rx_mono_ns,
    }
}

const fn mono_ns_to_adjusted_ms((base_adj_ms, base_mono_ns): (u64, u64), mono_ns: u64) -> u64 {
    if mono_ns >= base_mono_ns {
        base_adj_ms.saturating_add((mono_ns - base_mono_ns) / 1_000_000)
    } else {
        base_adj_ms.saturating_sub((base_mono_ns - mono_ns) / 1_000_000)
    }
}

fn mono_adjusted_now_ms(
    time_anchor: (u64, u64),
    clock: &MonotonicClock,
) -> Result<u64, Box<dyn Error>> {
    Ok(mono_ns_to_adjusted_ms(time_anchor, clock.now_ns()?))
}

async fn drive_live_trace(
    (args, cfg, inventory_q, time_anchor): (
        &TraceArgs,
        &EngineConfig,
        SignedSharesMicro,
        (u64, u64),
    ),
    rings: &mut LiveRings,
    state: &mut TraceState,
    pipe: &mut MathPipeline,
    (tracker, jsonl, clock): (&mut LatencyTracker, &mut JsonlWriter, &MonotonicClock),
) -> Result<(), Box<dyn Error>> {
    let max_wait =
        Duration::from_secs(45).max(Duration::from_secs((args.ticks as u64).saturating_mul(2)));
    let deadline = tokio::time::sleep(max_wait);
    tokio::pin!(deadline);
    let mut drain_tick = tokio::time::interval(Duration::from_millis(5));
    let mut eval_tick = tokio::time::interval(Duration::from_millis(100));
    let ctrl_c = tokio::signal::ctrl_c();
    tokio::pin!(ctrl_c);

    loop {
        tokio::select! {
            _ = &mut deadline => break,
            _ = &mut ctrl_c => return Err("interrupted by Ctrl-C".into()),
            _ = drain_tick.tick() => {
                drain_live_rings(time_anchor, rings, state, pipe)?;
            }
            _ = eval_tick.tick() => {
                let done = step_live_eval(
                    (args, cfg, inventory_q, time_anchor),
                    rings,
                    state,
                    pipe,
                    (tracker, jsonl, clock),
                )?;
                if done { break; }
            }
        }
    }
    Ok(())
}

fn step_live_eval(
    (args, cfg, inventory_q, time_anchor): (
        &TraceArgs,
        &EngineConfig,
        SignedSharesMicro,
        (u64, u64),
    ),
    rings: &mut LiveRings,
    state: &mut TraceState,
    pipe: &mut MathPipeline,
    (tracker, jsonl, clock): (&mut LatencyTracker, &mut JsonlWriter, &MonotonicClock),
) -> Result<bool, Box<dyn Error>> {
    drain_live_rings(time_anchor, rings, state, pipe)?;
    let adj = mono_adjusted_now_ms(time_anchor, clock)?;
    if let (Some(desc), Some(up), Some(dn), Some(_)) = (
        state.desc.clone(),
        state.latest_up,
        state.latest_dn,
        state.spot_usd,
    ) {
        eval_and_record_tick(
            TraceSourceMode::Live.as_str(),
            adj,
            inventory_q,
            (&desc, cfg, &up, &dn),
            state,
            pipe,
            (tracker, jsonl, clock),
        )?;
    }
    Ok(state.stats.evaluated_ticks >= args.ticks)
}

async fn finish_live_trace(
    handles: ActorHandles,
    rings: &mut LiveRings,
    (cfg, args, time_anchor): (&EngineConfig, &TraceArgs, (u64, u64)),
    loop_res: Result<(), Box<dyn Error>>,
    (state, pipe, tracker, mut jsonl, clock): (
        &mut TraceState,
        &mut MathPipeline,
        &LatencyTracker,
        JsonlWriter,
        &MonotonicClock,
    ),
) -> Result<(), Box<dyn Error>> {
    let ((_clob, _bin, _pb), shutdown_err) = shutdown_actors(handles, cfg.polybolt_enabled).await;
    let drain_res = drain_live_rings(time_anchor, rings, state, pipe);
    let slug = state.desc.as_ref().map_or("unknown", |d| d.slug.as_str());
    let summary_res = write_and_print_summary(
        TraceSourceMode::Live.as_str(),
        slug,
        state.strike_source,
        &state.stats,
        tracker.summary(),
        (&mut jsonl, clock, &args.jsonl),
    );
    let close_res = jsonl.close();
    summary_res?;
    close_res?;
    loop_res?;
    if let Some(err) = shutdown_err {
        return Err(err.into());
    }
    drain_res?;
    if state.stats.evaluated_ticks < args.ticks {
        return Err(format!(
            "live trace timed out after {} post-warmup ticks (< requested {})",
            state.stats.evaluated_ticks, args.ticks
        )
        .into());
    }
    Ok(())
}

fn drain_live_rings(
    time_anchor: (u64, u64),
    rings: &mut LiveRings,
    state: &mut TraceState,
    pipe: &mut MathPipeline,
) -> Result<(), Box<dyn Error>> {
    while let Ok(ev) = rings.poly_cons.pop() {
        apply_live_poly_to_state(ev, state, pipe)?;
    }
    while let Ok(ev) = rings.binance_cons.pop() {
        let ev_ts_ms = mono_ns_to_adjusted_ms(time_anchor, binance_rx_mono_ns(&ev));
        apply_binance_to_state(ev_ts_ms, ev, state, pipe)?;
    }
    while let Ok(_ev) = rings.oracle_cons.pop() {}
    while let Ok(ev) = rings.health_rx.try_recv() {
        log_live_health(&ev);
    }
    Ok(())
}

fn log_live_health(ev: &FeedHealthEvent) {
    let t = crate::theme::current();
    match ev {
        FeedHealthEvent::StaleTimeout {
            feed, silent_ms, ..
        } => eprintln!(
            "  {} feed `{feed:?}` stale timeout ({silent_ms}ms)",
            t.badge(crate::theme::Badge::Warn)
        ),
        FeedHealthEvent::RingOverflow {
            feed,
            dropped_count,
            ..
        } => eprintln!(
            "  {} feed `{feed:?}` ring overflow (dropped={dropped_count})",
            t.badge(crate::theme::Badge::Warn)
        ),
        FeedHealthEvent::ParseWarning { feed, reason, .. } => {
            eprintln!(
                "  {} feed `{feed:?}` parse warning: {reason}",
                t.badge(crate::theme::Badge::Warn)
            );
        }
        FeedHealthEvent::Disconnected { feed, reason, .. } => {
            eprintln!(
                "  {} feed `{feed:?}` disconnected: {reason}",
                t.badge(crate::theme::Badge::Warn)
            );
        }
        FeedHealthEvent::MarketResolved { feed, .. } => {
            eprintln!(
                "  {} market_resolved reported by feed `{feed:?}`",
                t.badge(crate::theme::Badge::Info)
            );
        }
        FeedHealthEvent::Connected { .. }
        | FeedHealthEvent::ResyncingRest { .. }
        | FeedHealthEvent::Ready { .. } => {}
    }
}

fn apply_binance_to_state(
    ts_ms: u64,
    ev: BinanceSpotEvent,
    state: &mut TraceState,
    pipe: &mut MathPipeline,
) -> Result<(), Box<dyn Error>> {
    let spot = match ev {
        BinanceSpotEvent::BookTicker { bid_e8, ask_e8, .. } => {
            if bid_e8 == 0 || ask_e8 < bid_e8 {
                return Err(format!(
                    "invalid Binance BookTicker BBO: bid_e8={bid_e8} ask_e8={ask_e8}"
                )
                .into());
            }
            (bid_e8 as f64 + ask_e8 as f64) / 200_000_000.0
        }
        BinanceSpotEvent::AggTrade { price_e8, .. } => {
            if price_e8 == 0 {
                return Err("invalid Binance AggTrade zero price".into());
            }
            price_e8 as f64 / 100_000_000.0
        }
    };
    state.observe_spot(spot);
    if let Some(desc) = &state.desc {
        let _ = pipe.on_binance_spot(ts_ms, spot, state.strike_usd, desc.expiry_ts_ms)?;
    }
    Ok(())
}

fn apply_replay_poly_to_state(
    ev: ReplayPolyEvent,
    state: &mut TraceState,
    pipe: &mut MathPipeline,
) -> Result<(), Box<dyn Error>> {
    match ev {
        ReplayPolyEvent::BookSnapshot {
            outcome,
            book,
            rx_mono_ns,
            exch_ts_ms,
        } => apply_replay_book_snapshot(outcome, book, rx_mono_ns, exch_ts_ms, state)?,
        ReplayPolyEvent::PriceChange {
            outcome,
            side,
            price,
            size,
            rx_mono_ns,
            exch_ts_ms,
            ..
        } => apply_delta_state(outcome, side, price, size, rx_mono_ns, exch_ts_ms, state)?,
        ReplayPolyEvent::LastTrade { outcome, size, .. } => pipe.on_poly_trade(outcome, size),
        ReplayPolyEvent::TickSizeChange { new_tick_micro, .. } => {
            apply_tick_update(new_tick_micro, state)?;
        }
        ReplayPolyEvent::BestBidAsk { .. } => {}
    }
    Ok(())
}

fn apply_replay_book_snapshot(
    outcome: Outcome,
    book: ReplayFixedBook,
    rx_mono_ns: u64,
    exch_ts_ms: u64,
    state: &mut TraceState,
) -> Result<(), Box<dyn Error>> {
    if book.outcome != outcome {
        return Err("replay BookSnapshot outcome mismatch".into());
    }
    if book.bids.len() > 10 || book.asks.len() > 10 {
        return Err("replay BookSnapshot exceeds 10 levels".into());
    }
    let fixed = FixedBook::<10>::try_from_slices(
        outcome,
        &book.bids,
        &book.asks,
        book.last_update_ns,
        book.exchange_ts_ms,
    )?;
    apply_book_snapshot_state(
        outcome,
        &book.bids,
        &book.asks,
        fixed,
        (rx_mono_ns, exch_ts_ms),
        state,
    )
}

fn apply_live_poly_to_state(
    ev: PolyMarketEvent,
    state: &mut TraceState,
    pipe: &mut MathPipeline,
) -> Result<(), Box<dyn Error>> {
    match ev {
        PolyMarketEvent::BookSnapshot {
            outcome,
            book,
            rx_mono_ns,
            exch_ts_ms,
            ..
        } => {
            apply_book_snapshot_state(
                outcome,
                book.bids_slice(),
                book.asks_slice(),
                book,
                (rx_mono_ns, exch_ts_ms),
                state,
            )?;
        }
        PolyMarketEvent::PriceChange {
            outcome,
            side,
            price,
            size,
            rx_mono_ns,
            exch_ts_ms,
            ..
        } => apply_delta_state(outcome, side, price, size, rx_mono_ns, exch_ts_ms, state)?,
        PolyMarketEvent::LastTrade { outcome, size, .. } => pipe.on_poly_trade(outcome, size),
        PolyMarketEvent::TickSizeChange { new_tick_micro, .. } => {
            apply_tick_update(new_tick_micro, state)?;
        }
        PolyMarketEvent::BestBidAsk { .. } => {}
    }
    Ok(())
}

fn apply_book_snapshot_state(
    outcome: Outcome,
    bids: &[core_types::BookLevel],
    asks: &[core_types::BookLevel],
    fixed: FixedBook<10>,
    (rx_mono_ns, exch_ts_ms): (u64, u64),
    state: &mut TraceState,
) -> Result<(), Box<dyn Error>> {
    match outcome {
        Outcome::Up => {
            state
                .book_up
                .apply_snapshot(bids, asks, rx_mono_ns, exch_ts_ms)?;
            state.latest_up = Some(fixed);
        }
        Outcome::Down => {
            state
                .book_dn
                .apply_snapshot(bids, asks, rx_mono_ns, exch_ts_ms)?;
            state.latest_dn = Some(fixed);
        }
    }
    Ok(())
}

fn apply_delta_state(
    outcome: Outcome,
    side: core_types::OrderSide,
    price: MicroUsdc,
    size: core_types::SharesMicro,
    rx_mono_ns: u64,
    exch_ts_ms: u64,
    state: &mut TraceState,
) -> Result<(), Box<dyn Error>> {
    let (m, slot) = match outcome {
        Outcome::Up => (&mut state.book_up, &mut state.latest_up),
        Outcome::Down => (&mut state.book_dn, &mut state.latest_dn),
    };
    m.apply_delta_allow_transient_cross(side, price, size, rx_mono_ns, exch_ts_ms)?;
    if m.is_crossed() {
        *slot = None;
    } else {
        *slot = Some(m.top_n::<10>()?);
    }
    Ok(())
}

fn apply_tick_update(new_tick: MicroUsdc, state: &mut TraceState) -> Result<(), Box<dyn Error>> {
    TickPrecision::from_micro(new_tick)?;
    if let Some(desc) = &mut state.desc {
        desc.tick_size_micro = new_tick;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::*;

    fn temp_path(tag: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("poly_as_trace_{tag}_{nanos}.jsonl"))
    }

    #[test]
    fn trace_rejects_zero_ticks_live_input_and_self_overwrite() {
        let zero_ticks = TraceArgs {
            source: TraceSourceMode::Replay,
            input: Some(PathBuf::from("logs/phase1-feeds.jsonl")),
            sg_mode: SgMode::Causal,
            kappa_mode: KappaMode::TradeNormalizedDepth,
            variance_mode: VarianceMode::BinaryCara,
            ticks: 0,
            inventory: 0,
            math: crate::MathOverrides::default(),
            jsonl: temp_path("zero"),
        };
        assert!(run(zero_ticks).is_err());

        let live_with_input = TraceArgs {
            source: TraceSourceMode::Live,
            input: Some(PathBuf::from("logs/phase1-feeds.jsonl")),
            sg_mode: SgMode::Causal,
            kappa_mode: KappaMode::TradeNormalizedDepth,
            variance_mode: VarianceMode::BinaryCara,
            ticks: 5,
            inventory: 0,
            math: crate::MathOverrides::default(),
            jsonl: temp_path("live"),
        };
        assert!(run(live_with_input).is_err());

        let replay_in = temp_path("self_in");
        std::fs::write(&replay_in, "{}\n").unwrap();
        assert!(crate::log_paths::resolve_replay_input(Some(&replay_in), &replay_in).is_err());
        let missing = temp_path("missing");
        assert!(crate::log_paths::resolve_replay_input(Some(&missing), &temp_path("out")).is_err());
        let _ = std::fs::remove_file(&replay_in);

        // Malformed Binance rows fail closed and transient cross clears cached snapshot.
        let mut st = TraceState::new();
        let cfg = MathPipelineConfig::try_default().unwrap();
        let mut pipe = MathPipeline::new(cfg, MonotonicClock::start()).unwrap();
        let bad_bbo = BinanceSpotEvent::BookTicker {
            bid_e8: 9_000_000_000_000,
            ask_e8: 8_900_000_000_000,
            bid_qty_e8: 1,
            ask_qty_e8: 1,
            update_id: 1,
            rx_mono_ns: 10,
        };
        assert!(apply_binance_to_state(100, bad_bbo, &mut st, &mut pipe).is_err());
        let zero_tr = BinanceSpotEvent::AggTrade {
            price_e8: 0,
            qty_e8: 1,
            is_buyer_maker: false,
            exch_ts_ms: 100,
            rx_mono_ns: 10,
        };
        assert!(apply_binance_to_state(100, zero_tr, &mut st, &mut pipe).is_err());

        apply_delta_state(
            Outcome::Up,
            core_types::OrderSide::Buy,
            MicroUsdc(490_000),
            core_types::SharesMicro(1_000_000),
            10,
            100,
            &mut st,
        )
        .unwrap();
        apply_delta_state(
            Outcome::Up,
            core_types::OrderSide::Sell,
            MicroUsdc(510_000),
            core_types::SharesMicro(1_000_000),
            11,
            101,
            &mut st,
        )
        .unwrap();
        assert!(st.latest_up.is_some());
        apply_delta_state(
            Outcome::Up,
            core_types::OrderSide::Buy,
            MicroUsdc(520_000),
            core_types::SharesMicro(1_000_000),
            12,
            102,
            &mut st,
        )
        .unwrap();
        assert!(st.latest_up.is_none());
    }
}
