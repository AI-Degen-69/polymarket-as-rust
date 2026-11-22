use std::error::Error;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime};

use core_types::{
    ClockOffsetEstimator, EngineConfig, HighResTimerGuard, JsonlWriter, MarketDescriptor,
    MonotonicClock, Outcome,
};
use feeds::{
    BinanceSpotEvent, BinanceSpotWsActor, BinanceWsCommand, BinanceWsConfig, BookMaintainer,
    ClobMarketWsActor, ClobWsCommand, ClobWsConfig, DEFAULT_RING_CAPACITY, FeedHealthEvent, FeedId,
    FeedStats, GammaDiscoveryClient, OracleTwapEvent, PolyBoltTwapWsActor, PolyBoltWsCommand,
    PolyBoltWsConfig, PolyMarketEvent,
};
use rtrb::RingBuffer;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::feeds_report::{
    CaptureReportData, apply_health_event, format_binance_sample, format_oracle_sample,
    format_poly_sample, print_header_and_market, print_sections_b_to_e, push_sample, run_replay,
    write_summary_event,
};
use crate::log_paths;
use crate::window_gate::{self, FreshWindowDiscovery, GateOutcome};

pub(crate) struct FeedsArgs {
    pub(crate) discover: bool,
    pub(crate) duration: Option<String>,
    pub(crate) windows: Option<u32>,
    pub(crate) allow_mid_window: bool,
    pub(crate) replay: Option<PathBuf>,
    pub(crate) replay_out: Option<PathBuf>,
    pub(crate) jsonl: Option<PathBuf>,
}

enum CaptureTarget {
    Explicit(PathBuf),
    Auto {
        inprogress_path: PathBuf,
        start_stamp: String,
    },
}

impl CaptureTarget {
    fn write_path(&self) -> &Path {
        match self {
            Self::Explicit(p)
            | Self::Auto {
                inprogress_path: p, ..
            } => p,
        }
    }
}

pub(crate) struct ActorHandles {
    clob_cmd: mpsc::Sender<ClobWsCommand>,
    binance_cmd: mpsc::Sender<BinanceWsCommand>,
    polybolt_cmd: mpsc::Sender<PolyBoltWsCommand>,
    clob_task: JoinHandle<Result<FeedStats, feeds::FeedError>>,
    binance_task: JoinHandle<Result<FeedStats, feeds::FeedError>>,
    polybolt_task: JoinHandle<Result<FeedStats, feeds::FeedError>>,
}

impl ActorHandles {
    /// Commands `ClobMarketWsActor` to unsubscribe from the expired token pair and
    /// subscribe to the newly discovered 5m market tokens without tearing down the socket,
    /// returning the new actor epoch once subscriptions have switched.
    pub(crate) async fn send_clob_rollover(
        &self,
        new_asset_id_up: String,
        new_asset_id_dn: String,
    ) -> Result<u64, Box<dyn Error>> {
        let (ack_tx, ack_rx) = tokio::sync::oneshot::channel();
        self.clob_cmd
            .send(ClobWsCommand::Rollover {
                new_asset_id_up,
                new_asset_id_dn,
                ack: Some(ack_tx),
            })
            .await
            .map_err(|e| format!("failed to send CLOB rollover command: {e}"))?;
        ack_rx
            .await
            .map_err(|e| format!("CLOB actor exited before acknowledging rollover: {e}").into())
    }
}

struct Rings<'a> {
    poly_cons: &'a mut rtrb::Consumer<PolyMarketEvent>,
    binance_cons: &'a mut rtrb::Consumer<BinanceSpotEvent>,
    oracle_cons: &'a mut rtrb::Consumer<OracleTwapEvent>,
    health_rx: &'a mut mpsc::Receiver<FeedHealthEvent>,
}

pub(crate) fn run(args: FeedsArgs) -> Result<(), Box<dyn Error>> {
    if let Some(replay_path) = &args.replay {
        if args.discover {
            return Err("cannot pass both --discover and --replay together".into());
        }
        if args.windows.is_some() {
            return Err("`--windows` is only valid with `--discover`".into());
        }
        return run_replay(replay_path, args.replay_out.as_deref());
    }
    if !args.discover {
        return Err(
            "specify `--discover` for live feeds or `--replay <file>` for offline replay".into(),
        );
    }
    let duration = resolve_capture_duration(args.duration.as_deref(), args.windows)?;
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    rt.block_on(run_discover(
        duration,
        args.windows,
        args.allow_mid_window,
        args.jsonl.as_deref(),
    ))
}

async fn run_discover(
    duration: Duration,
    target_windows: Option<u32>,
    allow_mid_window: bool,
    jsonl_arg: Option<&Path>,
) -> Result<(), Box<dyn Error>> {
    let clock = MonotonicClock::start();
    let _timer_guard = HighResTimerGuard::acquire_1ms()?;
    let cfg = EngineConfig::from_env()?;
    feeds::install_tls_provider();

    // Wait for a fresh 5-minute boundary BEFORE reserving `.inprogress.jsonl` or starting timers.
    let fresh = match window_gate::wait_and_discover_fresh_5m_market(&cfg, allow_mid_window, 10_000)
        .await?
    {
        GateOutcome::Ready(fresh) => fresh,
        GateOutcome::Cancelled => return Ok(()),
    };
    let FreshWindowDiscovery {
        discovery,
        offset_est,
        sync_report,
        mut desc,
    } = fresh;

    let target = match jsonl_arg {
        Some(path) => CaptureTarget::Explicit(path.to_path_buf()),
        None => {
            let start_stamp = log_paths::format_utc_stamp(SystemTime::now());
            let inprogress_path = log_paths::reserve_capture_inprogress_path(
                Path::new(log_paths::CAPTURES_DIR),
                &start_stamp,
                duration,
            )?;
            CaptureTarget::Auto {
                inprogress_path,
                start_stamp,
            }
        }
    };

    let raw_now_ms = MonotonicClock::unix_now_ms()?;
    let adj_now_ms = offset_est.adjusted_unix_ms(raw_now_ms)?;
    print_header_and_market(&desc, &sync_report, cfg.polybolt_enabled);
    let mut jsonl = JsonlWriter::create(target.write_path())?;
    jsonl.write_event(raw_now_ms, clock.now_ns()?, "market_discovered", &desc)?;
    jsonl.write_event(raw_now_ms, clock.now_ns()?, "clock_sync", &sync_report)?;

    let (poly_prod, mut poly_cons) = RingBuffer::new(DEFAULT_RING_CAPACITY);
    let (bin_prod, mut bin_cons) = RingBuffer::new(DEFAULT_RING_CAPACITY);
    let (ora_prod, mut ora_cons) = RingBuffer::new(DEFAULT_RING_CAPACITY);
    let (health_tx, mut health_rx) = mpsc::channel(1024);

    let handles = spawn_actors(
        &cfg,
        &desc,
        &discovery,
        clock,
        (poly_prod, bin_prod, ora_prod),
        health_tx,
    )?;
    let mut data = init_capture_data(cfg.polybolt_enabled);
    let mut rings = Rings {
        poly_cons: &mut poly_cons,
        binance_cons: &mut bin_cons,
        oracle_cons: &mut ora_cons,
        health_rx: &mut health_rx,
    };
    let max_rollovers =
        target_windows.map(|n| usize::try_from(n.saturating_sub(1)).unwrap_or(usize::MAX));
    let mut rollover_ctx = CaptureRolloverCtx {
        discovery: &discovery,
        offset_est: &offset_est,
        handles: &handles,
        current_desc: &mut desc,
        epoch: 0,
        rollovers: 0,
        max_rollovers,
        last_rollover_attempt: None,
    };
    let effective_duration = match target_windows {
        Some(n) => window_capture_duration(rollover_ctx.current_desc.start_ts_ms, adj_now_ms, n),
        None => duration,
    };
    let capture_start = Instant::now();
    let capture_res = capture_until_deadline(
        effective_duration,
        &clock,
        &mut rings,
        &mut data,
        &mut jsonl,
        &mut rollover_ctx,
    )
    .await;
    let actual_dur = capture_start.elapsed();
    let windows_touched = 1 + rollover_ctx.rollovers;
    finish_discover_report(
        handles,
        capture_res,
        (
            &cfg,
            &desc.slug,
            target,
            actual_dur,
            windows_touched,
            &clock,
        ),
        (&mut rings, &mut data),
        jsonl,
    )
    .await
}

async fn finish_discover_report(
    handles: ActorHandles,
    capture_res: Result<(), Box<dyn Error>>,
    (cfg, slug, target, actual_dur, windows_touched, clock): (
        &EngineConfig,
        &str,
        CaptureTarget,
        Duration,
        usize,
        &MonotonicClock,
    ),
    (rings, data): (&mut Rings<'_>, &mut CaptureReportData),
    mut jsonl: JsonlWriter,
) -> Result<(), Box<dyn Error>> {
    let ((clob_stats, binance_stats, polybolt_stats), shutdown_err) =
        shutdown_actors(handles, cfg.polybolt_enabled).await;
    drain_all(clock, rings, data, &mut jsonl)?;
    write_summary_event(
        &mut jsonl,
        clock,
        slug,
        data,
        &clob_stats,
        &binance_stats,
        &polybolt_stats,
    )?;
    jsonl.close()?;
    let final_path = match target {
        CaptureTarget::Explicit(p) => p,
        CaptureTarget::Auto {
            inprogress_path,
            start_stamp,
        } => log_paths::finalize_capture_file(
            &inprogress_path,
            &start_stamp,
            actual_dur,
            windows_touched,
        )?,
    };
    print_sections_b_to_e(
        data,
        &clob_stats,
        &binance_stats,
        &polybolt_stats,
        cfg.polybolt_enabled,
        &final_path,
    );
    capture_res?;
    if let Some(err) = shutdown_err {
        return Err(err.into());
    }
    Ok(())
}

fn init_capture_data(polybolt_enabled: bool) -> CaptureReportData {
    let pb_state = if polybolt_enabled {
        "Connecting".to_string()
    } else {
        "Disabled (local 60s TWAP fallback)".to_string()
    };
    CaptureReportData::new(pb_state)
}

pub(crate) fn spawn_actors(
    cfg: &EngineConfig,
    desc: &MarketDescriptor,
    discovery: &GammaDiscoveryClient,
    clock: MonotonicClock,
    producers: (
        rtrb::Producer<PolyMarketEvent>,
        rtrb::Producer<BinanceSpotEvent>,
        rtrb::Producer<OracleTwapEvent>,
    ),
    health_tx: mpsc::Sender<FeedHealthEvent>,
) -> Result<ActorHandles, Box<dyn Error>> {
    let (clob_cmd, clob_rx) = mpsc::channel(8);
    let (binance_cmd, binance_rx) = mpsc::channel(8);
    let (polybolt_cmd, polybolt_rx) = mpsc::channel(8);

    let clob_actor = ClobMarketWsActor::new(
        ClobWsConfig::new(desc.asset_id_up.clone(), desc.asset_id_dn.clone()),
        discovery.clone(),
    )?
    .with_clock(clock);
    let binance_actor = BinanceSpotWsActor::new(BinanceWsConfig::default())?.with_clock(clock);
    let polybolt_actor =
        PolyBoltTwapWsActor::new(PolyBoltWsConfig::from_engine_config(cfg))?.with_clock(clock);

    let clob_task = tokio::spawn(clob_actor.run(producers.0, health_tx.clone(), clob_rx));
    let binance_task = tokio::spawn(binance_actor.run(producers.1, health_tx.clone(), binance_rx));
    let polybolt_task = tokio::spawn(polybolt_actor.run(producers.2, health_tx, polybolt_rx));

    Ok(ActorHandles {
        clob_cmd,
        binance_cmd,
        polybolt_cmd,
        clob_task,
        binance_task,
        polybolt_task,
    })
}

struct CaptureRolloverCtx<'a> {
    discovery: &'a GammaDiscoveryClient,
    offset_est: &'a ClockOffsetEstimator,
    handles: &'a ActorHandles,
    current_desc: &'a mut MarketDescriptor,
    epoch: u64,
    rollovers: usize,
    max_rollovers: Option<usize>,
    last_rollover_attempt: Option<tokio::time::Instant>,
}

async fn capture_until_deadline(
    duration: Duration,
    clock: &MonotonicClock,
    rings: &mut Rings<'_>,
    data: &mut CaptureReportData,
    jsonl: &mut JsonlWriter,
    rollover: &mut CaptureRolloverCtx<'_>,
) -> Result<(), Box<dyn Error>> {
    let deadline = tokio::time::sleep(duration);
    tokio::pin!(deadline);
    let mut poll_tick = tokio::time::interval(Duration::from_millis(5));
    let ctrl_c = tokio::signal::ctrl_c();
    tokio::pin!(ctrl_c);

    loop {
        tokio::select! {
            _ = &mut deadline => break,
            _ = &mut ctrl_c => {
                println!(
                    "  {} Ctrl-C received, shutting down feeds cleanly...",
                    crate::theme::current().badge(crate::theme::Badge::Signal)
                );
                break;
            }
            _ = poll_tick.tick() => {
                drain_all(clock, rings, data, jsonl)?;
                maybe_rollover_capture(clock, rings, data, jsonl, rollover).await?;
            }
        }
    }
    Ok(())
}

async fn maybe_rollover_capture(
    clock: &MonotonicClock,
    rings: &mut Rings<'_>,
    data: &mut CaptureReportData,
    jsonl: &mut JsonlWriter,
    rollover: &mut CaptureRolloverCtx<'_>,
) -> Result<(), Box<dyn Error>> {
    let raw_now_ms = MonotonicClock::unix_now_ms()?;
    let adjusted_now_ms = rollover.offset_est.adjusted_unix_ms(raw_now_ms)?;
    if adjusted_now_ms < rollover.current_desc.expiry_ts_ms {
        return Ok(());
    }
    if rollover
        .max_rollovers
        .is_some_and(|limit| rollover.rollovers >= limit)
    {
        return Ok(());
    }
    let probe_due = rollover
        .last_rollover_attempt
        .is_none_or(|t| t.elapsed() >= Duration::from_secs(2));
    if !probe_due {
        return Ok(());
    }
    rollover.last_rollover_attempt = Some(tokio::time::Instant::now());

    let next_desc = match rollover
        .discovery
        .discover_active_5m_btc(adjusted_now_ms, 10_000)
        .await
    {
        Ok(desc) => desc,
        Err(err) => {
            eprintln!(
                "  {} rollover market discovery failed after {}: {err}",
                crate::theme::current().badge(crate::theme::Badge::Warn),
                rollover.current_desc.slug
            );
            return Ok(());
        }
    };
    if next_desc.condition_id == rollover.current_desc.condition_id {
        return Ok(());
    }

    let new_epoch = rollover
        .handles
        .send_clob_rollover(next_desc.asset_id_up.clone(), next_desc.asset_id_dn.clone())
        .await?;

    // Drain only pre-rollover CLOB events before switching the report maintainers and logging
    // the new `market_discovered` record so JSONL consumers never mix epochs.
    drain_poly_before_epoch(clock, rings, data, jsonl, Some(new_epoch))?;
    drain_non_poly(clock, rings, data, jsonl)?;

    data.maintainer_up = BookMaintainer::new(Outcome::Up);
    data.maintainer_dn = BookMaintainer::new(Outcome::Down);
    data.latest_up_book = None;
    data.latest_dn_book = None;
    rollover.epoch = new_epoch;
    rollover.rollovers += 1;
    let post_rollover_ms = MonotonicClock::unix_now_ms()?;
    jsonl.write_event(
        post_rollover_ms,
        clock.now_ns()?,
        "market_discovered",
        &next_desc,
    )?;
    println!(
        "  {} {} -> {} (epoch={})",
        crate::theme::current().badge(crate::theme::Badge::Epoch),
        rollover.current_desc.slug,
        crate::theme::current().cyan(&next_desc.slug),
        rollover.epoch
    );
    *rollover.current_desc = next_desc;
    rollover.last_rollover_attempt = None;
    Ok(())
}

pub(crate) async fn shutdown_actors(
    handles: ActorHandles,
    polybolt_enabled: bool,
) -> ((FeedStats, FeedStats, FeedStats), Option<String>) {
    if handles
        .clob_cmd
        .send(ClobWsCommand::Shutdown)
        .await
        .is_err()
    {
        eprintln!("WARN: clob_ws command channel already closed before Shutdown");
    }
    if handles
        .binance_cmd
        .send(BinanceWsCommand::Shutdown)
        .await
        .is_err()
    {
        eprintln!("WARN: binance_ws command channel already closed before Shutdown");
    }
    if handles
        .polybolt_cmd
        .send(PolyBoltWsCommand::Shutdown)
        .await
        .is_err()
        && polybolt_enabled
    {
        eprintln!("WARN: polybolt_ws command channel already closed before Shutdown");
    }

    let (clob_res, binance_res, polybolt_res) = tokio::join!(
        await_actor_stats("clob_ws", handles.clob_task, FeedId::PolymarketClob),
        await_actor_stats("binance_ws", handles.binance_task, FeedId::BinanceSpot),
        await_actor_stats("polybolt_ws", handles.polybolt_task, FeedId::PolyBolt),
    );
    let mut errors = Vec::new();
    let clob = collect_actor_result(clob_res, FeedId::PolymarketClob, &mut errors);
    let binance = collect_actor_result(binance_res, FeedId::BinanceSpot, &mut errors);
    let polybolt = collect_actor_result(polybolt_res, FeedId::PolyBolt, &mut errors);
    let combined_err = (!errors.is_empty()).then(|| errors.join("; "));
    ((clob, binance, polybolt), combined_err)
}

fn collect_actor_result(
    res: Result<FeedStats, String>,
    feed: FeedId,
    errors: &mut Vec<String>,
) -> FeedStats {
    match res {
        Ok(stats) => stats,
        Err(err) => {
            errors.push(err);
            zero_feed_stats(feed)
        }
    }
}

const fn zero_feed_stats(feed: FeedId) -> FeedStats {
    FeedStats {
        feed,
        frames_received: 0,
        events_pushed: 0,
        ring_overflows: 0,
        parse_warnings: 0,
        reconnects: 0,
        stale_timeouts: 0,
        pongs_received: 0,
        pings_replied: 0,
        health_events_dropped: 0,
        latency: None,
    }
}

async fn await_actor_stats(
    name: &str,
    handle: JoinHandle<Result<FeedStats, feeds::FeedError>>,
    _feed: FeedId,
) -> Result<FeedStats, String> {
    let joined = tokio::time::timeout(Duration::from_secs(3), handle)
        .await
        .map_err(|_| format!("{name} actor timed out during shutdown"))?
        .map_err(|e| format!("{name} actor panicked/cancelled: {e}"))?;
    joined.map_err(|e| format!("{name} exited with error: {e}"))
}

fn drain_all(
    clock: &MonotonicClock,
    rings: &mut Rings<'_>,
    data: &mut CaptureReportData,
    jsonl: &mut JsonlWriter,
) -> Result<(), Box<dyn Error>> {
    drain_poly_before_epoch(clock, rings, data, jsonl, None)?;
    drain_non_poly(clock, rings, data, jsonl)?;
    Ok(())
}

fn drain_poly_before_epoch(
    clock: &MonotonicClock,
    rings: &mut Rings<'_>,
    data: &mut CaptureReportData,
    jsonl: &mut JsonlWriter,
    before_epoch: Option<u64>,
) -> Result<(), Box<dyn Error>> {
    data.peak_poly_slots = data.peak_poly_slots.max(rings.poly_cons.slots());
    while let Ok(epoch) = rings.poly_cons.peek().map(PolyMarketEvent::epoch) {
        if before_epoch.is_some_and(|limit| epoch >= limit) {
            break;
        }
        if let Ok(ev) = rings.poly_cons.pop() {
            record_poly_event(clock, ev, data, jsonl)?;
        }
    }
    Ok(())
}

fn drain_non_poly(
    clock: &MonotonicClock,
    rings: &mut Rings<'_>,
    data: &mut CaptureReportData,
    jsonl: &mut JsonlWriter,
) -> Result<(), Box<dyn Error>> {
    data.peak_binance_slots = data.peak_binance_slots.max(rings.binance_cons.slots());
    data.peak_oracle_slots = data.peak_oracle_slots.max(rings.oracle_cons.slots());

    while let Ok(ev) = rings.binance_cons.pop() {
        record_binance_event(clock, ev, data, jsonl)?;
    }
    while let Ok(ev) = rings.oracle_cons.pop() {
        data.polybolt_events = data.polybolt_events.saturating_add(1);
        push_sample(&mut data.samples_poly, format_oracle_sample(ev));
        jsonl.write_event(
            MonotonicClock::unix_now_ms()?,
            clock.now_ns()?,
            "polybolt_event",
            &ev,
        )?;
    }
    while let Ok(ev) = rings.health_rx.try_recv() {
        apply_health_event(&mut data.health_counts, &ev);
        jsonl.write_event(
            MonotonicClock::unix_now_ms()?,
            clock.now_ns()?,
            "feed_health",
            &ev,
        )?;
    }
    Ok(())
}

fn record_poly_event(
    clock: &MonotonicClock,
    ev: PolyMarketEvent,
    data: &mut CaptureReportData,
    jsonl: &mut JsonlWriter,
) -> Result<(), Box<dyn Error>> {
    let tag = match ev {
        PolyMarketEvent::BookSnapshot {
            outcome,
            book,
            rx_mono_ns,
            exch_ts_ms,
            ..
        } => {
            data.poly_books = data.poly_books.saturating_add(1);
            sync_report_snapshot(outcome, book, rx_mono_ns, exch_ts_ms, data);
            "poly_book"
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
            data.poly_price_changes = data.poly_price_changes.saturating_add(1);
            apply_report_delta(outcome, side, price, size, rx_mono_ns, exch_ts_ms, data);
            "poly_price_change"
        }
        PolyMarketEvent::LastTrade { .. } => {
            data.poly_trades = data.poly_trades.saturating_add(1);
            "poly_trade"
        }
        PolyMarketEvent::TickSizeChange { .. } => {
            data.poly_tick_changes = data.poly_tick_changes.saturating_add(1);
            "poly_tick_size"
        }
        PolyMarketEvent::BestBidAsk { .. } => {
            data.poly_bba = data.poly_bba.saturating_add(1);
            "poly_bba"
        }
    };
    push_sample(&mut data.samples_poly, format_poly_sample(ev));
    jsonl.write_event(MonotonicClock::unix_now_ms()?, clock.now_ns()?, tag, &ev)?;
    Ok(())
}

fn sync_report_snapshot(
    outcome: Outcome,
    book: core_types::FixedBook<10>,
    rx_mono_ns: u64,
    exch_ts_ms: u64,
    data: &mut CaptureReportData,
) {
    let target = match outcome {
        Outcome::Up => (&mut data.maintainer_up, &mut data.latest_up_book),
        Outcome::Down => (&mut data.maintainer_dn, &mut data.latest_dn_book),
    };
    match target
        .0
        .apply_snapshot(book.bids_slice(), book.asks_slice(), rx_mono_ns, exch_ts_ms)
    {
        Ok(valid_book) => *target.1 = Some(valid_book),
        Err(err) => {
            eprintln!("WARN: local report snapshot sync failed for {outcome:?}: {err}");
        }
    }
}

fn apply_report_delta(
    outcome: Outcome,
    side: core_types::OrderSide,
    price: core_types::MicroUsdc,
    size: core_types::SharesMicro,
    rx_mono_ns: u64,
    exch_ts_ms: u64,
    data: &mut CaptureReportData,
) {
    let target = match outcome {
        Outcome::Up => (&mut data.maintainer_up, &mut data.latest_up_book),
        Outcome::Down => (&mut data.maintainer_dn, &mut data.latest_dn_book),
    };
    if let Err(err) = target
        .0
        .apply_delta_allow_transient_cross(side, price, size, rx_mono_ns, exch_ts_ms)
    {
        eprintln!("WARN: local report delta invalid for {outcome:?}: {err}");
        return;
    }
    if !target.0.is_crossed() {
        match target.0.top_n::<10>() {
            Ok(top) => *target.1 = Some(top),
            Err(err) => {
                eprintln!("WARN: local report top_n failed for {outcome:?}: {err}");
            }
        }
    }
}

fn record_binance_event(
    clock: &MonotonicClock,
    ev: BinanceSpotEvent,
    data: &mut CaptureReportData,
    jsonl: &mut JsonlWriter,
) -> Result<(), Box<dyn Error>> {
    match ev {
        BinanceSpotEvent::BookTicker { .. } => {
            data.binance_tickers = data.binance_tickers.saturating_add(1);
        }
        BinanceSpotEvent::AggTrade { .. } => {
            data.binance_trades = data.binance_trades.saturating_add(1);
        }
    }
    push_sample(&mut data.samples_binance, format_binance_sample(ev));
    jsonl.write_event(
        MonotonicClock::unix_now_ms()?,
        clock.now_ns()?,
        "binance_tick",
        &ev,
    )?;
    Ok(())
}

/// Resolves either `--windows <N>` (`N * 300s`, `N >= 1`) or `--duration` (default `20s`).
pub(crate) fn resolve_capture_duration(
    duration: Option<&str>,
    windows: Option<u32>,
) -> Result<Duration, Box<dyn Error>> {
    if duration.is_some() && windows.is_some() {
        return Err("cannot pass both --duration and --windows together".into());
    }
    if let Some(count) = windows {
        if count == 0 {
            return Err("`--windows` must be >= 1".into());
        }
        let secs = u64::from(count)
            .checked_mul(300)
            .ok_or("`--windows` duration overflow")?;
        return Ok(Duration::from_secs(secs));
    }
    parse_duration_arg(duration.unwrap_or("20s"))
}

/// Computes remaining capture duration to align stop time with the end of window `N`.
pub(crate) fn window_capture_duration(
    desc_start_ts_ms: u64,
    adj_now_ms: u64,
    target_windows: u32,
) -> Duration {
    let total_ms = u64::from(target_windows).saturating_mul(window_gate::WINDOW_MS);
    let target_end_ms = desc_start_ts_ms.saturating_add(total_ms);
    Duration::from_millis(target_end_ms.saturating_sub(adj_now_ms))
}

/// Parses human duration suffixes (`20s`, `500ms`, `1m`) into `std::time::Duration`.
pub(crate) fn parse_duration_arg(raw: &str) -> Result<Duration, Box<dyn Error>> {
    let trimmed = raw.trim();
    let dur = if let Some(ms) = trimmed.strip_suffix("ms") {
        Duration::from_millis(ms.trim().parse::<u64>()?)
    } else if let Some(s) = trimmed.strip_suffix('s') {
        Duration::from_secs(s.trim().parse::<u64>()?)
    } else if let Some(m) = trimmed.strip_suffix('m') {
        Duration::from_secs(
            m.trim()
                .parse::<u64>()?
                .checked_mul(60)
                .ok_or("duration overflow")?,
        )
    } else {
        return Err(format!("expected duration like `20s`, `500ms`, or `1m`, got `{raw}`").into());
    };
    if dur.is_zero() {
        return Err("duration must be > 0".into());
    }
    Ok(dur)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn duration_arg_parses_valid_and_rejects_invalid() {
        assert_eq!(parse_duration_arg("20s").unwrap(), Duration::from_secs(20));
        assert_eq!(
            parse_duration_arg("500ms").unwrap(),
            Duration::from_millis(500)
        );
        assert_eq!(parse_duration_arg("2m").unwrap(), Duration::from_secs(120));
        assert!(parse_duration_arg("0s").is_err());
        assert!(parse_duration_arg("20h").is_err());
    }

    #[test]
    fn resolve_capture_duration_handles_windows_and_conflicts() {
        assert_eq!(
            resolve_capture_duration(None, None).unwrap(),
            Duration::from_secs(20)
        );
        assert_eq!(
            resolve_capture_duration(Some("45s"), None).unwrap(),
            Duration::from_secs(45)
        );
        assert_eq!(
            resolve_capture_duration(None, Some(1)).unwrap(),
            Duration::from_secs(300)
        );
        assert_eq!(
            resolve_capture_duration(None, Some(3)).unwrap(),
            Duration::from_secs(900)
        );
        assert!(resolve_capture_duration(None, Some(0)).is_err());
        assert!(resolve_capture_duration(Some("20s"), Some(1)).is_err());
        assert!(resolve_capture_duration(None, Some(u32::MAX)).is_ok());
    }

    #[test]
    fn window_capture_duration_aligns_stop_to_window_end() {
        let start_ms = 1_795_345_200_000_u64;
        assert_eq!(
            window_capture_duration(start_ms, start_ms + 500, 1),
            Duration::from_millis(299_500)
        );
        assert_eq!(
            window_capture_duration(start_ms, start_ms + 1_200, 2),
            Duration::from_millis(598_800)
        );
    }
}
