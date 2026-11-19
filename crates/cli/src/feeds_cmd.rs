use std::error::Error;
use std::path::{Path, PathBuf};
use std::time::Duration;

use core_types::{
    ClockOffsetEstimator, EngineConfig, HighResTimerGuard, JsonlWriter, MarketDescriptor,
    MonotonicClock, Outcome,
};
use feeds::{
    BinanceSpotEvent, BinanceSpotWsActor, BinanceWsCommand, BinanceWsConfig, ClobMarketWsActor,
    ClobWsCommand, ClobWsConfig, DEFAULT_RING_CAPACITY, FeedHealthEvent, FeedId, FeedStats,
    GammaDiscoveryClient, OracleTwapEvent, PolyBoltTwapWsActor, PolyBoltWsCommand,
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

pub(crate) struct FeedsArgs {
    pub(crate) discover: bool,
    pub(crate) duration: String,
    pub(crate) replay: Option<PathBuf>,
    pub(crate) replay_out: Option<PathBuf>,
    pub(crate) jsonl: PathBuf,
}

struct ActorHandles {
    clob_cmd: mpsc::Sender<ClobWsCommand>,
    binance_cmd: mpsc::Sender<BinanceWsCommand>,
    polybolt_cmd: mpsc::Sender<PolyBoltWsCommand>,
    clob_task: JoinHandle<Result<FeedStats, feeds::FeedError>>,
    binance_task: JoinHandle<Result<FeedStats, feeds::FeedError>>,
    polybolt_task: JoinHandle<Result<FeedStats, feeds::FeedError>>,
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
        return run_replay(replay_path, args.replay_out.as_deref());
    }
    if !args.discover {
        return Err(
            "specify `--discover` for live feeds or `--replay <file>` for offline replay".into(),
        );
    }
    let duration = parse_duration_arg(&args.duration)?;
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    rt.block_on(run_discover(duration, &args.jsonl))
}

async fn run_discover(duration: Duration, jsonl_path: &Path) -> Result<(), Box<dyn Error>> {
    let clock = MonotonicClock::start();
    let _timer_guard = HighResTimerGuard::acquire_1ms()?;
    let cfg = EngineConfig::from_env()?;
    feeds::install_tls_provider();

    let (discovery, desc, mut jsonl) = setup_discovery_and_log(&cfg, &clock, jsonl_path).await?;
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
    let capture_res =
        capture_until_deadline(duration, &clock, &mut rings, &mut data, &mut jsonl).await;
    finish_discover_report(
        handles,
        capture_res,
        (&cfg, &desc.slug, jsonl_path, &clock),
        (&mut rings, &mut data),
        jsonl,
    )
    .await
}

async fn finish_discover_report(
    handles: ActorHandles,
    capture_res: Result<(), Box<dyn Error>>,
    (cfg, slug, jsonl_path, clock): (&EngineConfig, &str, &Path, &MonotonicClock),
    (rings, data): (&mut Rings<'_>, &mut CaptureReportData),
    mut jsonl: JsonlWriter,
) -> Result<(), Box<dyn Error>> {
    let ((clob_stats, binance_stats, polybolt_stats), shutdown_err) =
        shutdown_actors(handles, cfg.polybolt_enabled).await;
    drain_all(clock, rings, data, &mut jsonl)?;
    print_sections_b_to_e(
        data,
        &clob_stats,
        &binance_stats,
        &polybolt_stats,
        cfg.polybolt_enabled,
        jsonl_path,
    );
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
    capture_res?;
    if let Some(err) = shutdown_err {
        return Err(err.into());
    }
    Ok(())
}

async fn setup_discovery_and_log(
    cfg: &EngineConfig,
    clock: &MonotonicClock,
    jsonl_path: &Path,
) -> Result<(GammaDiscoveryClient, MarketDescriptor, JsonlWriter), Box<dyn Error>> {
    let discovery = GammaDiscoveryClient::new()?;
    let offset_est = ClockOffsetEstimator::new(cfg.clock_drift_max_ms);
    let sync = discovery.sync_clock(&offset_est).await?;
    let raw_now_ms = MonotonicClock::unix_now_ms()?;
    let adjusted_now_ms = offset_est.adjusted_unix_ms(raw_now_ms)?;
    let desc = discovery
        .discover_active_5m_btc(adjusted_now_ms, 10_000)
        .await?;

    print_header_and_market(&desc, &sync, cfg.polybolt_enabled);
    let mut jsonl = JsonlWriter::create(jsonl_path)?;
    jsonl.write_event(raw_now_ms, clock.now_ns()?, "market_discovered", &desc)?;
    jsonl.write_event(raw_now_ms, clock.now_ns()?, "clock_sync", &sync)?;
    Ok((discovery, desc, jsonl))
}

fn init_capture_data(polybolt_enabled: bool) -> CaptureReportData {
    let pb_state = if polybolt_enabled {
        "Connecting".to_string()
    } else {
        "Disabled (local 60s TWAP fallback)".to_string()
    };
    CaptureReportData::new(pb_state)
}

fn spawn_actors(
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

async fn capture_until_deadline(
    duration: Duration,
    clock: &MonotonicClock,
    rings: &mut Rings<'_>,
    data: &mut CaptureReportData,
    jsonl: &mut JsonlWriter,
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
                println!("  [SIGNAL]             : Ctrl-C received, shutting down feeds cleanly...");
                break;
            }
            _ = poll_tick.tick() => {
                drain_all(clock, rings, data, jsonl)?;
            }
        }
    }
    Ok(())
}

async fn shutdown_actors(
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
    data.peak_poly_slots = data.peak_poly_slots.max(rings.poly_cons.slots());
    data.peak_binance_slots = data.peak_binance_slots.max(rings.binance_cons.slots());
    data.peak_oracle_slots = data.peak_oracle_slots.max(rings.oracle_cons.slots());

    while let Ok(ev) = rings.poly_cons.pop() {
        record_poly_event(clock, ev, data, jsonl)?;
    }
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
}
