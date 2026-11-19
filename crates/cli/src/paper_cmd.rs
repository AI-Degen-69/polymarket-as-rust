use std::error::Error;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::time::Duration;

use core_types::{
    ClockOffsetEstimator, EngineConfig, FixedBook, HighResTimerGuard, JsonlWriter, LatencyTracker,
    MarketDescriptor, MicroUsdc, MonotonicClock, ONE_SHARE_MICRO, OrderSide, Outcome, SharesMicro,
};
use feeds::{
    BinanceSpotEvent, BookMaintainer, DEFAULT_RING_CAPACITY, FeedHealthEvent, GammaDiscoveryClient,
    PolyMarketEvent,
};
use math_engine::{MathPipeline, MathPipelineConfig, PipelineInput, QuoteOutput};
use paper_sim::{
    BaselineTelemetry, FifoBrokerConfig, FifoPaperBroker, NaiveQuotesInput, NewOrderParams,
    PaperLedger, TrajectorySnapshot,
};
use rtrb::RingBuffer;
use tokio::sync::mpsc;

use crate::feeds_cmd::{parse_duration_arg, shutdown_actors, spawn_actors};
use crate::feeds_report::{RawJsonlEnvelope, ReplayPolyEvent, canonical_target_path};
use crate::paper_report::{
    PaperReportStats, PaperSummaryDisplay, print_fill_event, print_merge_event, print_order_event,
    print_paper_header, print_summary_table, print_trade_eval, record_fill_jsonl,
    record_merge_jsonl, record_order_jsonl, record_summary_jsonl, record_tick_jsonl,
};
use crate::trace_cmd::TraceSourceMode;

pub(crate) struct PaperArgs {
    pub(crate) source: TraceSourceMode,
    pub(crate) input: Option<PathBuf>,
    pub(crate) duration: String,
    pub(crate) v_max: u64,
    pub(crate) base_size: u64,
    pub(crate) latency_ms: u64,
    pub(crate) jsonl: PathBuf,
}

struct PaperState {
    desc: Option<MarketDescriptor>,
    book_up: BookMaintainer,
    book_dn: BookMaintainer,
    latest_up: Option<FixedBook<10>>,
    latest_dn: Option<FixedBook<10>>,
    spot_usd: Option<f64>,
    strike_usd: Option<f64>,
    replay_time_anchor: Option<(u64, u64)>,
    max_outer_mono: u64,
    next_eval_ms: Option<u64>,
    next_summary_ms: Option<u64>,
    active_up_id: Option<u64>,
    active_dn_id: Option<u64>,
    stats: PaperReportStats,
}

type SimTuple<'a> = (
    &'a mut FifoPaperBroker,
    &'a mut PaperLedger,
    &'a mut BaselineTelemetry,
);

impl PaperState {
    fn new() -> Self {
        Self {
            desc: None,
            book_up: BookMaintainer::new(Outcome::Up),
            book_dn: BookMaintainer::new(Outcome::Down),
            latest_up: None,
            latest_dn: None,
            spot_usd: None,
            strike_usd: None,
            replay_time_anchor: None,
            max_outer_mono: 0,
            next_eval_ms: None,
            next_summary_ms: None,
            active_up_id: None,
            active_dn_id: None,
            stats: PaperReportStats::default(),
        }
    }

    fn set_market(&mut self, desc: MarketDescriptor) {
        if let Some(k) = desc.strike_price_usd {
            self.strike_usd = Some(k.get());
        } else {
            self.strike_usd = None;
        }
        self.desc = Some(desc);
    }

    fn observe_spot(&mut self, spot: f64) {
        if spot > 0.0 && spot.is_finite() {
            self.spot_usd = Some(spot);
            if self.strike_usd.is_none() {
                self.strike_usd = Some(spot);
            }
        }
    }
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

pub(crate) fn run(args: PaperArgs) -> Result<(), Box<dyn Error>> {
    let _duration = parse_duration_arg(&args.duration)?;
    let cfg = EngineConfig::from_env()?;

    match args.source {
        TraceSourceMode::Replay => {
            let input_path = resolve_replay_input(args.input.as_deref(), &args.jsonl)?;
            run_replay(&args, &input_path, &cfg)
        }
        TraceSourceMode::Live => {
            if args.input.is_some() {
                return Err("`--input` is only valid with `--source replay`".into());
            }
            let rt = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()?;
            rt.block_on(run_live(&args, &cfg))
        }
    }
}

fn resolve_replay_input(input: Option<&Path>, jsonl: &Path) -> Result<PathBuf, Box<dyn Error>> {
    let input_path = input.map_or_else(
        || PathBuf::from("logs/phase1-feeds.jsonl"),
        Path::to_path_buf,
    );
    if !input_path.exists() {
        return Err(format!("replay input `{}` does not exist", input_path.display()).into());
    }
    if canonical_target_path(&input_path)? == canonical_target_path(jsonl)? {
        return Err("refusing to overwrite `--input` replay log with `--jsonl` output".into());
    }
    Ok(input_path)
}

fn run_replay(
    args: &PaperArgs,
    input_path: &Path,
    cfg: &EngineConfig,
) -> Result<(), Box<dyn Error>> {
    let clock = MonotonicClock::start();
    let pipe_cfg = MathPipelineConfig::from_engine_config(cfg)?;
    let mut pipe = MathPipeline::new(pipe_cfg, clock)?;
    let mut tracker = LatencyTracker::new()?;
    let mut state = PaperState::new();
    let mut jsonl = JsonlWriter::create(&args.jsonl)?;
    let mut header_printed = false;

    let broker_config = FifoBrokerConfig {
        order_latency_ms: args.latency_ms,
        cancel_latency_ms: args.latency_ms,
        ..Default::default()
    };
    let base_size = SharesMicro(args.base_size * ONE_SHARE_MICRO);
    let mut broker = FifoPaperBroker::new(broker_config);
    let mut ledger = PaperLedger::new(core_types::FeeSchedule::btc_5m_default());
    let mut telemetry = BaselineTelemetry::new(
        broker_config,
        core_types::FeeSchedule::btc_5m_default(),
        base_size,
    )?;

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
            (&mut broker, &mut ledger, &mut telemetry),
            cfg,
            args,
            (&mut header_printed, &mut tracker, &mut jsonl, &clock),
        )?;
    }

    // Finalize summary output
    finalize_paper_output(
        &state,
        &ledger,
        &mut telemetry,
        args,
        &tracker,
        jsonl,
        &clock,
    )
}

fn finalize_paper_output(
    state: &PaperState,
    ledger: &PaperLedger,
    telemetry: &mut BaselineTelemetry,
    args: &PaperArgs,
    tracker: &LatencyTracker,
    mut jsonl: JsonlWriter,
    clock: &MonotonicClock,
) -> Result<(), Box<dyn Error>> {
    let slug = state.desc.as_ref().map_or("unknown", |d| d.slug.as_str());
    let now_ms = state.next_eval_ms.unwrap_or(0);
    let mid_up = state
        .latest_up
        .as_ref()
        .and_then(fixed_book_mid)
        .unwrap_or(MicroUsdc(500_000));
    let mid_dn = state
        .latest_dn
        .as_ref()
        .and_then(fixed_book_mid)
        .unwrap_or(MicroUsdc(500_000));

    let unrealized = ledger.unrealized_pnl_micro(mid_up, mid_dn);
    let baseline_pnl = telemetry.naive_engine.ledger.net_pnl_micro(mid_up, mid_dn);
    let latency_sum = tracker.summary();

    let disp = PaperSummaryDisplay {
        q_up: ledger.filled_up,
        q_dn: ledger.filled_dn,
        net_q: ledger.net_inventory(),
        pairs_merged: ledger.total_pairs_merged,
        realized_pnl: ledger.realized_pnl_micro,
        unrealized_pnl: unrealized,
        fees_paid: ledger.fees_paid_micro,
        rpm_60s: telemetry.rpm_60s(now_ms),
        fill_ratio: telemetry.fill_ratio(),
        markout_1s_bps: telemetry.avg_markout_1s_bps(),
        markout_5s_bps: telemetry.avg_markout_5s_bps(),
        baseline_pnl,
        latency: &latency_sum,
    };

    print_summary_table(&disp);

    record_summary_jsonl(
        args.source.as_str(),
        slug,
        &state.stats,
        &disp,
        now_ms,
        &mut jsonl,
        clock,
    )?;

    jsonl.close()?;
    Ok(())
}

fn process_replay_env(
    env: RawJsonlEnvelope,
    state: &mut PaperState,
    pipe: &mut MathPipeline,
    sim: SimTuple<'_>,
    cfg: &EngineConfig,
    args: &PaperArgs,
    (header_printed, tracker, jsonl, clock): (
        &mut bool,
        &mut LatencyTracker,
        &mut JsonlWriter,
        &MonotonicClock,
    ),
) -> Result<(), Box<dyn Error>> {
    let (broker, ledger, telemetry) = sim;
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
            let spot = match ev {
                BinanceSpotEvent::BookTicker { bid_e8, ask_e8, .. } => {
                    (bid_e8 as f64 + ask_e8 as f64) / 200_000_000.0
                }
                BinanceSpotEvent::AggTrade { price_e8, .. } => price_e8 as f64 / 100_000_000.0,
            };
            state.observe_spot(spot);
            if let Some(desc) = &state.desc {
                let _ =
                    pipe.on_binance_spot(env.ts_unix_ms, spot, state.strike_usd, desc.expiry_ts_ms);
            }
        }
        "poly_book" | "poly_price_change" | "poly_trade" | "poly_tick_size" | "poly_bba" => {
            let ev: ReplayPolyEvent = serde_json::from_value(env.payload)?;
            match ev {
                ReplayPolyEvent::BookSnapshot {
                    outcome,
                    book,
                    rx_mono_ns,
                    exch_ts_ms,
                } => {
                    let fixed = FixedBook::<10>::try_from_slices(
                        outcome,
                        &book.bids,
                        &book.asks,
                        book.last_update_ns,
                        book.exchange_ts_ms,
                    )?;
                    match outcome {
                        Outcome::Up => {
                            let _ = state.book_up.apply_snapshot(
                                fixed.bids_slice(),
                                fixed.asks_slice(),
                                rx_mono_ns,
                                exch_ts_ms,
                            );
                            state.latest_up = Some(fixed);
                        }
                        Outcome::Down => {
                            let _ = state.book_dn.apply_snapshot(
                                fixed.bids_slice(),
                                fixed.asks_slice(),
                                rx_mono_ns,
                                exch_ts_ms,
                            );
                            state.latest_dn = Some(fixed);
                        }
                    }
                }
                ReplayPolyEvent::PriceChange {
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
                            let _ = state.book_up.apply_price_change_delta(
                                side, price, size, rx_mono_ns, exch_ts_ms,
                            );
                            if let Ok(top) = state.book_up.top_n::<10>() {
                                state.latest_up = Some(top);
                            }
                        }
                        Outcome::Down => {
                            let _ = state.book_dn.apply_price_change_delta(
                                side, price, size, rx_mono_ns, exch_ts_ms,
                            );
                            if let Ok(top) = state.book_dn.top_n::<10>() {
                                state.latest_dn = Some(top);
                            }
                        }
                    }
                    broker.on_book_update(outcome, price, size, exch_ts_ms);
                }
                ReplayPolyEvent::LastTrade {
                    outcome,
                    price,
                    size,
                    exch_ts_ms,
                    ..
                } => {
                    pipe.on_poly_trade(outcome, size);

                    // Check volume ahead before trade execution
                    let vol_ahead_before = broker
                        .active_order(outcome, OrderSide::Buy)
                        .map_or(SharesMicro::ZERO, |o| o.volume_ahead);

                    // Execute trade against broker
                    let fills = broker.on_public_trade(outcome, price, size, exch_ts_ms);

                    let vol_ahead_after = broker
                        .active_order(outcome, OrderSide::Buy)
                        .map_or(SharesMicro::ZERO, |o| o.volume_ahead);

                    let filled_total: u64 = fills.iter().map(|f| f.filled_shares.0).sum();
                    let reachable = size.0.saturating_sub(vol_ahead_before.0);

                    if filled_total > 0 || vol_ahead_before.0 > 0 {
                        print_trade_eval(
                            outcome,
                            price,
                            size,
                            vol_ahead_before,
                            vol_ahead_after,
                            SharesMicro(reachable),
                            SharesMicro(filled_total),
                        );
                    }

                    let mid_up = state
                        .latest_up
                        .as_ref()
                        .and_then(fixed_book_mid)
                        .unwrap_or(price);
                    let mid_dn = state
                        .latest_dn
                        .as_ref()
                        .and_then(fixed_book_mid)
                        .unwrap_or(price);
                    let mid_at_fill = match outcome {
                        Outcome::Up => mid_up,
                        Outcome::Down => mid_dn,
                    };

                    let slug = state.desc.as_ref().map_or("btc-5m", |d| d.slug.as_str());
                    for fill in &fills {
                        state.stats.fills_count = state.stats.fills_count.saturating_add(1);
                        print_fill_event(fill);
                        record_fill_jsonl(slug, fill, jsonl, clock)?;

                        let inv_before = ledger.net_inventory();
                        if let Some(merge) = ledger.record_fill(fill)? {
                            state.stats.merges_count = state.stats.merges_count.saturating_add(1);
                            state.stats.total_pairs_merged.0 = state
                                .stats
                                .total_pairs_merged
                                .0
                                .saturating_add(merge.merged_pairs.0);
                            print_merge_event(&merge, ledger.total_pairs_merged);
                            record_merge_jsonl(
                                slug,
                                &merge,
                                ledger.total_pairs_merged,
                                exch_ts_ms,
                                jsonl,
                                clock,
                            )?;
                        }
                        let inv_after = ledger.net_inventory();
                        telemetry.record_fill(fill, inv_before, inv_after, mid_at_fill);
                    }

                    // Feed public trade into naive baseline engine
                    telemetry
                        .naive_engine
                        .on_public_trade(outcome, price, size, exch_ts_ms);
                }
                ReplayPolyEvent::TickSizeChange { new_tick_micro, .. } => {
                    if let Some(desc) = state.desc.as_mut() {
                        desc.tick_size_micro = new_tick_micro;
                    }
                }
                ReplayPolyEvent::BestBidAsk { .. } => {}
            }
        }
        _ => {}
    }

    // Schedule or trigger 100ms evaluation tick
    let cur_sim_ms = anchor
        .0
        .saturating_add(env.mono_ns.saturating_sub(anchor.1) / 1_000_000);
    if state.next_eval_ms.is_none() {
        state.next_eval_ms = Some(cur_sim_ms);
        state.next_summary_ms = Some(cur_sim_ms.saturating_add(5_000));
    }

    while state
        .next_eval_ms
        .is_some_and(|target| cur_sim_ms >= target)
    {
        let eval_now = state.next_eval_ms.unwrap_or(cur_sim_ms);
        state.next_eval_ms = Some(eval_now.saturating_add(100));

        if !*header_printed && let Some(desc) = state.desc.as_ref() {
            print_paper_header(
                TraceSourceMode::Replay.as_str(),
                desc,
                args.v_max,
                args.base_size,
                args.latency_ms,
                &args.duration,
                &args.jsonl,
            );
            *header_printed = true;
        }

        eval_sim_step(
            eval_now,
            state,
            pipe,
            (broker, ledger, telemetry),
            cfg,
            args,
            (tracker, jsonl, clock),
        )?;

        // Check periodic summary
        if state
            .next_summary_ms
            .is_some_and(|target| eval_now >= target)
        {
            state.next_summary_ms = Some(eval_now.saturating_add(5_000));
            if let Some(desc) = state.desc.as_ref() {
                let mid_up = state
                    .latest_up
                    .as_ref()
                    .and_then(fixed_book_mid)
                    .unwrap_or(MicroUsdc(500_000));
                let mid_dn = state
                    .latest_dn
                    .as_ref()
                    .and_then(fixed_book_mid)
                    .unwrap_or(MicroUsdc(500_000));
                let unrealized = ledger.unrealized_pnl_micro(mid_up, mid_dn);
                let baseline_pnl = telemetry.naive_engine.ledger.net_pnl_micro(mid_up, mid_dn);
                let latency_sum = tracker.summary();

                let disp = PaperSummaryDisplay {
                    q_up: ledger.filled_up,
                    q_dn: ledger.filled_dn,
                    net_q: ledger.net_inventory(),
                    pairs_merged: ledger.total_pairs_merged,
                    realized_pnl: ledger.realized_pnl_micro,
                    unrealized_pnl: unrealized,
                    fees_paid: ledger.fees_paid_micro,
                    rpm_60s: telemetry.rpm_60s(eval_now),
                    fill_ratio: telemetry.fill_ratio(),
                    markout_1s_bps: telemetry.avg_markout_1s_bps(),
                    markout_5s_bps: telemetry.avg_markout_5s_bps(),
                    baseline_pnl,
                    latency: &latency_sum,
                };

                print_summary_table(&disp);

                record_summary_jsonl(
                    args.source.as_str(),
                    &desc.slug,
                    &state.stats,
                    &disp,
                    eval_now,
                    jsonl,
                    clock,
                )?;
            }
        }
    }

    Ok(())
}

fn eval_sim_step(
    now_ms: u64,
    state: &mut PaperState,
    pipe: &mut MathPipeline,
    sim: SimTuple<'_>,
    _cfg: &EngineConfig,
    args: &PaperArgs,
    (tracker, jsonl, clock): (&mut LatencyTracker, &mut JsonlWriter, &MonotonicClock),
) -> Result<(), Box<dyn Error>> {
    let (broker, ledger, telemetry) = sim;
    let (Some(desc), Some(up_book), Some(dn_book)) = (
        state.desc.as_ref(),
        state.latest_up.as_ref(),
        state.latest_dn.as_ref(),
    ) else {
        return Ok(());
    };

    broker.update_time(now_ms);

    let inventory_q = ledger.net_inventory();
    let inp = PipelineInput {
        now_ms,
        expiry_ms: desc.expiry_ts_ms,
        tick_size_micro: desc.tick_size_micro,
        fee_schedule: desc.fee_schedule,
        inventory_q,
        book_up: up_book,
        book_dn: dn_book,
        spot_usd: state.spot_usd,
        strike_usd: state.strike_usd,
        active_bid_up: broker
            .active_order(Outcome::Up, OrderSide::Buy)
            .map(|o| o.price),
        active_bid_dn: broker
            .active_order(Outcome::Down, OrderSide::Buy)
            .map(|o| o.price),
    };

    let trace = pipe.evaluate(&inp)?;
    if trace.flags.sg_warming_up {
        return Ok(());
    }

    state.stats.total_ticks = state.stats.total_ticks.saturating_add(1);
    state.stats.total_compute_ns_sum = state
        .stats
        .total_compute_ns_sum
        .saturating_add(trace.total_compute_ns);
    let _ = tracker.record_ns(trace.total_compute_ns);

    let base_size = SharesMicro(args.base_size * ONE_SHARE_MICRO);
    let best_bid_up = up_book.best_bid().map(|l| l.price);
    let best_ask_up = up_book.best_ask().map(|l| l.price);
    let best_bid_dn = dn_book.best_bid().map(|l| l.price);
    let best_ask_dn = dn_book.best_ask().map(|l| l.price);

    let (desired_bid_up, desired_bid_dn) = match trace.quote {
        QuoteOutput::Active { bid_up, bid_dn, .. } => (bid_up, bid_dn),
        QuoteOutput::Suppressed(_) => (None, None),
    };

    // Manage UP quote
    match desired_bid_up {
        Some(target_price) => {
            let should_replace = match state.active_up_id.and_then(|id| broker.get_order(id)) {
                Some(order) => order.price != target_price || !order.status.is_active(),
                None => true,
            };
            if should_replace {
                if let Some(old_id) = state.active_up_id {
                    let _ = broker.cancel(old_id, now_ms);
                    state.stats.orders_cancelled = state.stats.orders_cancelled.saturating_add(1);
                    telemetry.record_cancel_replace(now_ms);
                }
                let depth = fixed_book_depth_at(up_book, target_price);
                if let Ok(new_id) = broker.submit_new(NewOrderParams {
                    outcome: Outcome::Up,
                    side: OrderSide::Buy,
                    price: target_price,
                    size: base_size,
                    now_ms,
                    current_best_bid: best_bid_up,
                    current_best_ask: best_ask_up,
                    book_depth_at_price: depth,
                }) {
                    state.active_up_id = Some(new_id);
                    state.stats.orders_placed = state.stats.orders_placed.saturating_add(1);
                    telemetry.record_order_posted(base_size);
                    if let Some(order) = broker.get_order(new_id) {
                        print_order_event("SUBMIT", order);
                        record_order_jsonl(&desc.slug, "submit", order, now_ms, jsonl, clock)?;
                    }
                }
            }
        }
        None => {
            if let Some(old_id) = state.active_up_id.take() {
                let _ = broker.cancel(old_id, now_ms);
                state.stats.orders_cancelled = state.stats.orders_cancelled.saturating_add(1);
                telemetry.record_cancel_replace(now_ms);
            }
        }
    }

    // Manage DOWN quote
    match desired_bid_dn {
        Some(target_price) => {
            let should_replace = match state.active_dn_id.and_then(|id| broker.get_order(id)) {
                Some(order) => order.price != target_price || !order.status.is_active(),
                None => true,
            };
            if should_replace {
                if let Some(old_id) = state.active_dn_id {
                    let _ = broker.cancel(old_id, now_ms);
                    state.stats.orders_cancelled = state.stats.orders_cancelled.saturating_add(1);
                    telemetry.record_cancel_replace(now_ms);
                }
                let depth = fixed_book_depth_at(dn_book, target_price);
                if let Ok(new_id) = broker.submit_new(NewOrderParams {
                    outcome: Outcome::Down,
                    side: OrderSide::Buy,
                    price: target_price,
                    size: base_size,
                    now_ms,
                    current_best_bid: best_bid_dn,
                    current_best_ask: best_ask_dn,
                    book_depth_at_price: depth,
                }) {
                    state.active_dn_id = Some(new_id);
                    state.stats.orders_placed = state.stats.orders_placed.saturating_add(1);
                    telemetry.record_order_posted(base_size);
                    if let Some(order) = broker.get_order(new_id) {
                        print_order_event("SUBMIT", order);
                        record_order_jsonl(&desc.slug, "submit", order, now_ms, jsonl, clock)?;
                    }
                }
            }
        }
        None => {
            if let Some(old_id) = state.active_dn_id.take() {
                let _ = broker.cancel(old_id, now_ms);
                state.stats.orders_cancelled = state.stats.orders_cancelled.saturating_add(1);
                telemetry.record_cancel_replace(now_ms);
            }
        }
    }

    // Update naive baseline quotes
    let mid_up = fixed_book_mid(up_book);
    let mid_dn = fixed_book_mid(dn_book);
    let tick_prec = core_types::TickPrecision::from_micro(desc.tick_size_micro)
        .unwrap_or(core_types::TickPrecision::Cent);
    telemetry.naive_engine.update_quotes(NaiveQuotesInput {
        mid_up,
        mid_dn,
        best_bid_up,
        best_ask_up,
        best_bid_dn,
        best_ask_dn,
        tick: tick_prec,
        depth_up: fixed_book_depth_at(up_book, mid_up.unwrap_or(MicroUsdc(500_000))),
        depth_dn: fixed_book_depth_at(dn_book, mid_dn.unwrap_or(MicroUsdc(500_000))),
        now_ms,
    });

    // Update markouts
    if let (Some(m_up), Some(m_dn)) = (mid_up, mid_dn) {
        telemetry.update_markouts(now_ms, m_up, m_dn);
    }

    // Record trajectory snapshot
    let snapshot = TrajectorySnapshot {
        ts_ms: now_ms,
        tau_secs: (desc.expiry_ts_ms.saturating_sub(now_ms) as f64) / 1000.0,
        s_star: trace.pricer.map_or(0.0, |p| p.s_star),
        r_i: trace.pricer.map_or(0.50, |p| p.r_i),
        bid_up_raw: trace.pricer.map_or(0.48, |p| p.bid_up_raw),
        bid_up: desired_bid_up,
        bid_dn: desired_bid_dn,
        q_inventory: inventory_q.as_i64(),
    };
    telemetry.record_trajectory(snapshot);
    record_tick_jsonl(
        state.stats.total_ticks,
        args.source.as_str(),
        &desc.slug,
        now_ms,
        snapshot,
        jsonl,
        clock,
    )?;

    Ok(())
}

async fn run_live(args: &PaperArgs, cfg: &EngineConfig) -> Result<(), Box<dyn Error>> {
    let clock = MonotonicClock::start();
    let _timer_guard = HighResTimerGuard::acquire_1ms()?;
    feeds::install_tls_provider();

    let pipe_cfg = MathPipelineConfig::from_engine_config(cfg)?;
    let mut pipe = MathPipeline::new(pipe_cfg, clock)?;
    let mut tracker = LatencyTracker::new()?;
    let mut jsonl = JsonlWriter::create(&args.jsonl)?;
    let (discovery, offset_est, desc) = discover_live_market(cfg).await?;
    let base_adj_ms = offset_est.adjusted_unix_ms(MonotonicClock::unix_now_ms()?)?;
    let base_mono_ns = clock.now_ns()?;

    let mut state = PaperState::new();
    state.set_market(desc.clone());
    print_paper_header(
        TraceSourceMode::Live.as_str(),
        &desc,
        args.v_max,
        args.base_size,
        args.latency_ms,
        &args.duration,
        &args.jsonl,
    );

    let broker_config = FifoBrokerConfig {
        order_latency_ms: args.latency_ms,
        cancel_latency_ms: args.latency_ms,
        ..Default::default()
    };
    let base_size = SharesMicro(args.base_size * ONE_SHARE_MICRO);
    let mut broker = FifoPaperBroker::new(broker_config);
    let mut ledger = PaperLedger::new(desc.fee_schedule);
    let mut telemetry = BaselineTelemetry::new(broker_config, desc.fee_schedule, base_size)?;

    let (poly_prod, mut poly_cons) = RingBuffer::new(DEFAULT_RING_CAPACITY);
    let (bin_prod, mut binance_cons) = RingBuffer::new(DEFAULT_RING_CAPACITY);
    let (ora_prod, mut oracle_cons) = RingBuffer::new(DEFAULT_RING_CAPACITY);
    let (health_tx, mut health_rx) = mpsc::channel(1024);

    let handles = spawn_actors(
        cfg,
        &desc,
        &discovery,
        clock,
        (poly_prod, bin_prod, ora_prod),
        health_tx,
    )?;

    let target_duration = parse_duration_arg(&args.duration)?;
    let deadline = tokio::time::Instant::now() + target_duration;
    let mut interval = tokio::time::interval(Duration::from_millis(100));

    println!("Streaming live market feeds for paper simulation... (Press Ctrl+C to stop)");

    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                println!("\nReceived Ctrl+C, shutting down paper simulator...");
                break;
            }
            _ = tokio::time::sleep_until(deadline) => {
                println!("\nReached target duration {}, shutting down...", args.duration);
                break;
            }
            _ = interval.tick() => {
                // Drain rings
                while let Ok(ev) = binance_cons.pop() {
                    let (spot, rx_ns) = match ev {
                        BinanceSpotEvent::BookTicker { bid_e8, ask_e8, rx_mono_ns, .. } => {
                            let mid = (bid_e8 as f64 + ask_e8 as f64) / 200_000_000.0;
                            (mid, rx_mono_ns)
                        }
                        BinanceSpotEvent::AggTrade { price_e8, rx_mono_ns, .. } => {
                            let px = price_e8 as f64 / 100_000_000.0;
                            (px, rx_mono_ns)
                        }
                    };
                    state.observe_spot(spot);
                    let ev_ts_ms = base_adj_ms.saturating_add(rx_ns.saturating_sub(base_mono_ns) / 1_000_000);
                    let _ = pipe.on_binance_spot(ev_ts_ms, spot, state.strike_usd, desc.expiry_ts_ms);
                }
                while let Ok(_ev) = oracle_cons.pop() {}
                while let Ok(ev) = poly_cons.pop() {
                    match ev {
                        PolyMarketEvent::BookSnapshot { outcome, book, rx_mono_ns, exch_ts_ms } => {
                            match outcome {
                                Outcome::Up => {
                                    let _ = state.book_up.apply_snapshot(book.bids_slice(), book.asks_slice(), rx_mono_ns, exch_ts_ms);
                                    state.latest_up = Some(book);
                                }
                                Outcome::Down => {
                                    let _ = state.book_dn.apply_snapshot(book.bids_slice(), book.asks_slice(), rx_mono_ns, exch_ts_ms);
                                    state.latest_dn = Some(book);
                                }
                            }
                        }
                        PolyMarketEvent::PriceChange { outcome, side, price, size, rx_mono_ns, exch_ts_ms, .. } => {
                            match outcome {
                                Outcome::Up => {
                                    let _ = state.book_up.apply_price_change_delta(side, price, size, rx_mono_ns, exch_ts_ms);
                                    if let Ok(top) = state.book_up.top_n::<10>() {
                                        state.latest_up = Some(top);
                                    }
                                }
                                Outcome::Down => {
                                    let _ = state.book_dn.apply_price_change_delta(side, price, size, rx_mono_ns, exch_ts_ms);
                                    if let Ok(top) = state.book_dn.top_n::<10>() {
                                        state.latest_dn = Some(top);
                                    }
                                }
                            }
                            broker.on_book_update(outcome, price, size, exch_ts_ms);
                        }
                        PolyMarketEvent::LastTrade { outcome, price, size, exch_ts_ms, .. } => {
                            pipe.on_poly_trade(outcome, size);

                            let vol_ahead_before = broker
                                .active_order(outcome, OrderSide::Buy)
                                .map_or(SharesMicro::ZERO, |o| o.volume_ahead);

                            let fills = broker.on_public_trade(outcome, price, size, exch_ts_ms);

                            let vol_ahead_after = broker
                                .active_order(outcome, OrderSide::Buy)
                                .map_or(SharesMicro::ZERO, |o| o.volume_ahead);

                            let filled_total: u64 = fills.iter().map(|f| f.filled_shares.0).sum();
                            let reachable = size.0.saturating_sub(vol_ahead_before.0);

                            if filled_total > 0 || vol_ahead_before.0 > 0 {
                                print_trade_eval(
                                    outcome,
                                    price,
                                    size,
                                    vol_ahead_before,
                                    vol_ahead_after,
                                    SharesMicro(reachable),
                                    SharesMicro(filled_total),
                                );
                            }

                            let mid_up = state.latest_up.as_ref().and_then(fixed_book_mid).unwrap_or(price);
                            let mid_dn = state.latest_dn.as_ref().and_then(fixed_book_mid).unwrap_or(price);
                            let mid_at_fill = match outcome {
                                Outcome::Up => mid_up,
                                Outcome::Down => mid_dn,
                            };

                            for fill in &fills {
                                state.stats.fills_count = state.stats.fills_count.saturating_add(1);
                                print_fill_event(fill);
                                record_fill_jsonl(&desc.slug, fill, &mut jsonl, &clock)?;

                                let inv_before = ledger.net_inventory();
                                if let Some(merge) = ledger.record_fill(fill)? {
                                    state.stats.merges_count = state.stats.merges_count.saturating_add(1);
                                    state.stats.total_pairs_merged.0 = state.stats.total_pairs_merged.0.saturating_add(merge.merged_pairs.0);
                                    print_merge_event(&merge, ledger.total_pairs_merged);
                                    record_merge_jsonl(&desc.slug, &merge, ledger.total_pairs_merged, exch_ts_ms, &mut jsonl, &clock)?;
                                }
                                let inv_after = ledger.net_inventory();
                                telemetry.record_fill(fill, inv_before, inv_after, mid_at_fill);
                            }

                            telemetry.naive_engine.on_public_trade(outcome, price, size, exch_ts_ms);
                        }
                        PolyMarketEvent::TickSizeChange { new_tick_micro, .. } => {
                            if let Some(d) = state.desc.as_mut() {
                                d.tick_size_micro = new_tick_micro;
                            }
                        }
                        PolyMarketEvent::BestBidAsk { .. } => {}
                    }
                }
                while let Ok(h) = health_rx.try_recv() {
                    if let FeedHealthEvent::MarketResolved { .. } = h {
                        println!("[HEALTH] Active 5m epoch market resolved!");
                    }
                }

                // Compute now_ms from base clock
                let now_ns = clock.now_ns()?;
                let now_ms = base_adj_ms.saturating_add(now_ns.saturating_sub(base_mono_ns) / 1_000_000);

                eval_sim_step(
                    now_ms,
                    &mut state,
                    &mut pipe,
                    (&mut broker, &mut ledger, &mut telemetry),
                    cfg,
                    args,
                    (&mut tracker, &mut jsonl, &clock),
                )?;

                // Print periodic summary every 5 seconds
                if state.next_summary_ms.is_none() {
                    state.next_summary_ms = Some(now_ms.saturating_add(5_000));
                }
                if state.next_summary_ms.is_some_and(|target| now_ms >= target) {
                    state.next_summary_ms = Some(now_ms.saturating_add(5_000));
                    let mid_up = state.latest_up.as_ref().and_then(fixed_book_mid).unwrap_or(MicroUsdc(500_000));
                    let mid_dn = state.latest_dn.as_ref().and_then(fixed_book_mid).unwrap_or(MicroUsdc(500_000));
                    let unrealized = ledger.unrealized_pnl_micro(mid_up, mid_dn);
                    let baseline_pnl = telemetry.naive_engine.ledger.net_pnl_micro(mid_up, mid_dn);
                    let latency_sum = tracker.summary();

                    let disp = PaperSummaryDisplay {
                        q_up: ledger.filled_up,
                        q_dn: ledger.filled_dn,
                        net_q: ledger.net_inventory(),
                        pairs_merged: ledger.total_pairs_merged,
                        realized_pnl: ledger.realized_pnl_micro,
                        unrealized_pnl: unrealized,
                        fees_paid: ledger.fees_paid_micro,
                        rpm_60s: telemetry.rpm_60s(now_ms),
                        fill_ratio: telemetry.fill_ratio(),
                        markout_1s_bps: telemetry.avg_markout_1s_bps(),
                        markout_5s_bps: telemetry.avg_markout_5s_bps(),
                        baseline_pnl,
                        latency: &latency_sum,
                    };

                    print_summary_table(&disp);

                    record_summary_jsonl(
                        args.source.as_str(),
                        &desc.slug,
                        &state.stats,
                        &disp,
                        now_ms,
                        &mut jsonl,
                        &clock,
                    )?;
                }
            }
        }
    }

    // Cancel all resting quotes before shutdown
    if let Some(id) = state.active_up_id {
        let _ = broker.cancel(id, MonotonicClock::unix_now_ms()?);
    }
    if let Some(id) = state.active_dn_id {
        let _ = broker.cancel(id, MonotonicClock::unix_now_ms()?);
    }

    shutdown_actors(handles, cfg.polybolt_enabled).await;

    finalize_paper_output(
        &state,
        &ledger,
        &mut telemetry,
        args,
        &tracker,
        jsonl,
        &clock,
    )
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
