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
    OracleTwapEvent, PolyMarketEvent,
};
use historical_data::{
    BookQualityFilter, CanonicalEvent, CanonicalEventKind, DataSource, SourceFilter,
    canonical_event_matches_filters, canonical_metadata_to_descriptor, is_canonical_jsonl_line,
    read_canonical_jsonl,
};
use math_engine::{CostContext, MathPipeline, MathPipelineConfig, PipelineInput, QuoteOutput};
use paper_sim::{
    BaselineTelemetry, FifoBrokerConfig, FifoPaperBroker, NewOrderParams, PaperLedger,
    TrajectorySnapshot,
};
use rtrb::RingBuffer;
use tokio::sync::mpsc;

use crate::feeds_cmd::{parse_duration_arg, shutdown_actors, spawn_actors};
use crate::feeds_report::{RawJsonlEnvelope, ReplayPolyEvent};
use crate::lifecycle_report::{print_epoch_rollover, record_epoch_rollover_jsonl};
use crate::paper_report::{
    PaperReportStats, PaperSummaryDisplay, print_fill_event, print_merge_event, print_order_event,
    print_paper_header, print_summary_table, print_trade_eval, record_fill_jsonl,
    record_merge_jsonl, record_order_jsonl, record_summary_jsonl, record_tick_jsonl,
};
use crate::trace_cmd::TraceSourceMode;

#[derive(Debug, Clone)]
struct PendingEpochSettlement {
    expired_slug: String,
    winning_outcome: Outcome,
    settle_delta_micro: i64,
    total_realized_pnl_micro: i64,
    pairs_merged_micro: u64,
    fees_paid_micro: u64,
    estimated_rebate_micro: u64,
}

pub(crate) struct PaperArgs {
    pub(crate) source: TraceSourceMode,
    pub(crate) input: Option<PathBuf>,
    pub(crate) duration: String,
    pub(crate) allow_mid_window: bool,
    pub(crate) v_max: u64,
    pub(crate) base_size: u64,
    pub(crate) latency_ms: u64,
    pub(crate) queue_decay: paper_sim::QueueDecayPolicy,
    pub(crate) math: crate::MathOverrides,
    pub(crate) source_filter: SourceFilter,
    pub(crate) min_book_quality: BookQualityFilter,
    pub(crate) jsonl: PathBuf,
}

struct PaperState {
    desc: Option<MarketDescriptor>,
    explicit_winning_outcome: Option<Outcome>,
    epoch_settled: bool,
    book_up: BookMaintainer,
    book_dn: BookMaintainer,
    latest_up: Option<FixedBook<10>>,
    latest_dn: Option<FixedBook<10>>,
    spot_usd: Option<f64>,
    twap_60s_usd: Option<f64>,
    oracle_twap_60s_usd: Option<f64>,
    strike_usd: Option<f64>,
    clob_epoch: u64,
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
            explicit_winning_outcome: None,
            epoch_settled: false,
            book_up: BookMaintainer::new(Outcome::Up),
            book_dn: BookMaintainer::new(Outcome::Down),
            latest_up: None,
            latest_dn: None,
            spot_usd: None,
            twap_60s_usd: None,
            oracle_twap_60s_usd: None,
            strike_usd: None,
            clob_epoch: 0,
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
            self.strike_usd = self.spot_usd;
        }
        self.explicit_winning_outcome = None;
        self.epoch_settled = false;
        self.twap_60s_usd = None;
        self.oracle_twap_60s_usd = None;
        self.book_up = BookMaintainer::new(Outcome::Up);
        self.book_dn = BookMaintainer::new(Outcome::Down);
        self.latest_up = None;
        self.latest_dn = None;
        self.active_up_id = None;
        self.active_dn_id = None;
        self.desc = Some(desc);
    }

    fn resolve_winning_outcome(&self) -> Outcome {
        if let Some(explicit) = self.explicit_winning_outcome {
            return explicit;
        }
        let imp_up = self.latest_up.as_ref().and_then(terminal_implied_price);
        let imp_dn = self.latest_dn.as_ref().and_then(terminal_implied_price);

        // 1. Unambiguous terminal CLOB consensus (`>= $0.85` vs `<= $0.15`,
        //    including one-sided `$0.99 / None` or `None / $0.01` books at expiry).
        const HIGH_CONF: u64 = 850_000;
        const LOW_CONF: u64 = 150_000;
        if let Some(u) = imp_up
            && u.0 >= HIGH_CONF
            && imp_dn.is_none_or(|d| d.0 <= LOW_CONF)
        {
            return Outcome::Up;
        }
        if let Some(d) = imp_dn
            && d.0 >= HIGH_CONF
            && imp_up.is_none_or(|u| u.0 <= LOW_CONF)
        {
            return Outcome::Down;
        }
        if let Some(u) = imp_up
            && u.0 <= LOW_CONF
            && imp_dn.is_none()
        {
            return Outcome::Down;
        }
        if let Some(d) = imp_dn
            && d.0 <= LOW_CONF
            && imp_up.is_none()
        {
            return Outcome::Up;
        }

        // 2. Terminal instantaneous spot vs strike (never trailing 60s TWAP).
        if let (Some(spot), Some(strike)) = (self.spot_usd, self.strike_usd) {
            if spot >= strike {
                return Outcome::Up;
            }
            return Outcome::Down;
        }

        // 3. Fallback book-implied tiebreaker.
        if imp_up.unwrap_or(MicroUsdc(500_000)) >= imp_dn.unwrap_or(MicroUsdc(500_000)) {
            Outcome::Up
        } else {
            Outcome::Down
        }
    }

    fn settle_expired_epoch(
        &mut self,
        now_ms: u64,
        broker: &mut FifoPaperBroker,
        ledger: &mut PaperLedger,
        telemetry: &mut BaselineTelemetry,
        pipe: &mut MathPipeline,
    ) -> Option<PendingEpochSettlement> {
        let (expired_slug, expiry_ts_ms) = {
            let desc = self.desc.as_ref()?;
            (desc.slug.clone(), desc.expiry_ts_ms)
        };
        if let Some(id) = self.active_up_id.take()
            && broker.cancel(id, now_ms).is_ok()
        {
            self.stats.orders_cancelled = self.stats.orders_cancelled.saturating_add(1);
            telemetry.record_cancel_replace(now_ms);
        }
        if let Some(id) = self.active_dn_id.take()
            && broker.cancel(id, now_ms).is_ok()
        {
            self.stats.orders_cancelled = self.stats.orders_cancelled.saturating_add(1);
            telemetry.record_cancel_replace(now_ms);
        }
        let _ = broker.force_cancel_all();

        if let Some(twap) = pipe.settlement_twap_usd(now_ms, expiry_ts_ms) {
            self.twap_60s_usd = Some(twap);
        }

        let winning_outcome = self.resolve_winning_outcome();
        let settle_delta = ledger.settle_epoch(winning_outcome);
        telemetry.reset_epoch();

        Some(PendingEpochSettlement {
            expired_slug,
            winning_outcome,
            settle_delta_micro: settle_delta,
            total_realized_pnl_micro: ledger.realized_pnl_micro,
            pairs_merged_micro: ledger.total_pairs_merged.0,
            fees_paid_micro: ledger.fees_paid_micro,
            estimated_rebate_micro: ledger.estimated_rebate_micro,
        })
    }

    fn rollover_to_market(
        &mut self,
        next_desc: MarketDescriptor,
        next_epoch: u64,
        broker: &mut FifoPaperBroker,
        ledger: &mut PaperLedger,
        telemetry: &mut BaselineTelemetry,
        pipe: &mut MathPipeline,
    ) {
        let broker_cfg = *broker.config();
        *broker = FifoPaperBroker::new(broker_cfg);
        ledger.set_fee_schedule(next_desc.fee_schedule);
        telemetry.reset_epoch();

        let epoch_secs =
            (next_desc.expiry_ts_ms.saturating_sub(next_desc.start_ts_ms) as f64 / 1000.0).max(1.0);
        self.clob_epoch = next_epoch;
        self.set_market(next_desc);
        pipe.reset_epoch();
        let _ = pipe.set_epoch_secs(epoch_secs);
        self.stats.epoch_rollovers = self.stats.epoch_rollovers.saturating_add(1);
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

fn terminal_implied_price(book: &FixedBook<10>) -> Option<MicroUsdc> {
    match (book.best_bid(), book.best_ask()) {
        (Some(bid), Some(ask)) => Some(MicroUsdc(bid.price.0.saturating_add(ask.price.0) / 2)),
        (Some(bid), None) => Some(bid.price),
        (None, Some(ask)) => Some(ask.price),
        (None, None) => None,
    }
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
    let mut cfg = EngineConfig::from_env()?;
    args.math.apply_to(&mut cfg)?;

    match args.source {
        TraceSourceMode::Replay => {
            let input_path =
                crate::log_paths::resolve_replay_input(args.input.as_deref(), &args.jsonl)?;
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

fn run_replay(
    args: &PaperArgs,
    input_path: &Path,
    cfg: &EngineConfig,
) -> Result<(), Box<dyn Error>> {
    let first_non_empty = {
        let probe = BufReader::new(File::open(input_path)?);
        let mut found = None;
        for line_res in probe.lines() {
            let line = line_res?;
            if !line.trim().is_empty() {
                found = Some(line);
                break;
            }
        }
        found.ok_or("replay input file is empty")?
    };

    if is_canonical_jsonl_line(&first_non_empty) {
        return run_canonical_replay(args, input_path, cfg);
    }
    if args.source_filter == SourceFilter::PmdataOnly {
        return Err(
            "live JSONL replay input contains no `pmdata` events under `--source-filter pmdata`"
                .into(),
        );
    }

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
        queue_decay_policy: args.queue_decay,
    };
    let mut broker = FifoPaperBroker::new(broker_config);
    let mut ledger = PaperLedger::new(core_types::FeeSchedule::btc_5m_default());
    let mut telemetry = BaselineTelemetry::new()?;

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

#[allow(clippy::too_many_lines)]
fn run_canonical_replay(
    args: &PaperArgs,
    input_path: &Path,
    cfg: &EngineConfig,
) -> Result<(), Box<dyn Error>> {
    let clock = MonotonicClock::start();
    let events = read_canonical_jsonl(input_path)?;
    if events.is_empty() {
        return Err("canonical replay input file contains 0 events".into());
    }

    let mut has_pmdata = false;
    let mut has_live = false;
    let mut filtered_events: Vec<CanonicalEvent> = Vec::with_capacity(events.len());
    let mut filtered_out = 0_usize;

    for ev in events {
        ev.validate_provenance_invariants()?;
        if !canonical_event_matches_filters(&ev, args.source_filter, args.min_book_quality) {
            filtered_out = filtered_out.saturating_add(1);
            continue;
        }
        match ev.provenance.source {
            DataSource::Pmdata => has_pmdata = true,
            DataSource::LiveJsonl => has_live = true,
        }
        filtered_events.push(ev);
    }

    if filtered_events.is_empty() {
        return Err(format!(
            "all events filtered out under --source-filter {} --min-book-quality {}",
            args.source_filter.as_str(),
            args.min_book_quality.as_str()
        )
        .into());
    }
    historical_data::validate_canonical_replay_quality(&filtered_events, args.min_book_quality)?;

    if has_pmdata {
        eprintln!(
            "  [PROVENANCE] PMData historical events present (live={has_live}, pmdata=true, filtered_out={filtered_out}): mono_ns=None, clock_sync=None; causal ordering uses exchange/provider UTC replay_order_ts_ms. Live-vs-PMData equivalence UNVERIFIED."
        );
    }

    let pipe_cfg = MathPipelineConfig::from_engine_config(cfg)?;
    let mut pipe = MathPipeline::new(pipe_cfg, clock)?;
    let mut tracker = LatencyTracker::new()?;
    let mut state = PaperState::new();
    let mut jsonl = JsonlWriter::create(&args.jsonl)?;
    let mut header_printed = false;

    let broker_config = FifoBrokerConfig {
        order_latency_ms: args.latency_ms,
        cancel_latency_ms: args.latency_ms,
        queue_decay_policy: args.queue_decay,
    };
    let mut broker = FifoPaperBroker::new(broker_config);
    let mut ledger = PaperLedger::new(core_types::FeeSchedule::btc_5m_default());
    let mut telemetry = BaselineTelemetry::new()?;

    let mut pending_resolutions: std::collections::HashMap<String, Outcome> =
        std::collections::HashMap::new();
    for ev in &filtered_events {
        if let CanonicalEventKind::MarketResolution {
            winning_outcome, ..
        } = ev.kind
        {
            pending_resolutions.insert(ev.provenance.market_slug.clone(), winning_outcome);
        }
    }

    let mut epoch = 0_u64;
    let mut last_sim_ms = 0_u64;
    let mut prev_sim_ms: Option<u64> = None;

    for ev in filtered_events {
        let cur_sim_ms = ev.timestamp.replay_order_ts_ms;
        if let Some(prev) = prev_sim_ms
            && cur_sim_ms < prev
        {
            return Err(format!(
                "non-monotonic canonical replay_order_ts_ms: {cur_sim_ms} < {prev}"
            )
            .into());
        }
        prev_sim_ms = Some(cur_sim_ms);
        last_sim_ms = cur_sim_ms;

        if let CanonicalEventKind::MarketResolution {
            winning_outcome, ..
        } = ev.kind
            && state
                .desc
                .as_ref()
                .is_some_and(|d| d.slug == ev.provenance.market_slug)
        {
            state.explicit_winning_outcome = Some(winning_outcome);
        }

        if let Some(expiry_ms) = state.desc.as_ref().map(|d| d.expiry_ts_ms)
            && !state.epoch_settled
            && cur_sim_ms >= expiry_ms
        {
            while state.next_eval_ms.is_some_and(|target| expiry_ms >= target) {
                let eval_now = state.next_eval_ms.unwrap_or(expiry_ms);
                state.next_eval_ms = Some(eval_now.saturating_add(100));
                eval_sim_step(
                    eval_now,
                    &mut state,
                    &mut pipe,
                    (&mut broker, &mut ledger, &mut telemetry),
                    cfg,
                    args,
                    (&mut tracker, &mut jsonl, &clock),
                )?;
            }
            if state.explicit_winning_outcome.is_none()
                && let Some(slug) = state.desc.as_ref().map(|d| &d.slug)
                && let Some(&win) = pending_resolutions.get(slug)
            {
                state.explicit_winning_outcome = Some(win);
            }
            if let Some(pending) = state.settle_expired_epoch(
                expiry_ms,
                &mut broker,
                &mut ledger,
                &mut telemetry,
                &mut pipe,
            ) {
                let expiry_ns = clock.now_ns().unwrap_or(0);
                record_epoch_rollover_jsonl(
                    &mut jsonl,
                    expiry_ms,
                    expiry_ns,
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
            state.epoch_settled = true;
        }

        if !matches!(ev.kind, CanonicalEventKind::MarketMetadata { .. })
            && !state.epoch_settled
            && let Some(desc) = state.desc.as_ref()
        {
            let bounded_ms = cur_sim_ms.min(desc.expiry_ts_ms);
            if let Some(target) = state.next_eval_ms
                && bounded_ms.saturating_sub(target) > 600_000
            {
                state.next_eval_ms = Some(bounded_ms);
            }
            while state.next_eval_ms.is_some_and(|target| target < bounded_ms) {
                let eval_now = state.next_eval_ms.unwrap_or(bounded_ms);
                state.next_eval_ms = Some(eval_now.saturating_add(100));
                if !header_printed && let Some(d) = state.desc.as_ref() {
                    print_paper_header(
                        TraceSourceMode::Replay.as_str(),
                        d,
                        args.v_max,
                        args.base_size,
                        args.latency_ms,
                        &args.duration,
                        &args.jsonl,
                    );
                    header_printed = true;
                }
                eval_sim_step(
                    eval_now,
                    &mut state,
                    &mut pipe,
                    (&mut broker, &mut ledger, &mut telemetry),
                    cfg,
                    args,
                    (&mut tracker, &mut jsonl, &clock),
                )?;
            }
        }

        let active_slug_matches = state
            .desc
            .as_ref()
            .is_some_and(|d| d.slug == ev.provenance.market_slug);

        match ev.kind {
            CanonicalEventKind::MarketMetadata { .. } => {
                let (desc, _) = canonical_metadata_to_descriptor(&ev.kind)?;
                let epoch_secs =
                    (desc.expiry_ts_ms.saturating_sub(desc.start_ts_ms) as f64 / 1000.0).max(1.0);
                if let Some(prev) = state.desc.as_ref() {
                    if prev.condition_id != desc.condition_id || prev.slug != desc.slug {
                        if !state.epoch_settled && cur_sim_ms < prev.expiry_ts_ms {
                            return Err(format!(
                                "overlapping `market_metadata` rollover from `{}` (expiry={}) to `{}` at replay_order_ts_ms={cur_sim_ms}",
                                prev.slug, prev.expiry_ts_ms, desc.slug
                            )
                            .into());
                        }
                        epoch = epoch.saturating_add(1);
                        state.rollover_to_market(
                            desc,
                            epoch,
                            &mut broker,
                            &mut ledger,
                            &mut telemetry,
                            &mut pipe,
                        );
                        state.next_eval_ms = Some(
                            cur_sim_ms
                                .max(state.desc.as_ref().map_or(cur_sim_ms, |d| d.start_ts_ms)),
                        );
                    }
                } else {
                    pipe.set_epoch_secs(epoch_secs)?;
                    ledger.set_fee_schedule(desc.fee_schedule);
                    state.set_market(desc);
                    state.next_eval_ms = Some(
                        cur_sim_ms.max(state.desc.as_ref().map_or(cur_sim_ms, |d| d.start_ts_ms)),
                    );
                }
            }
            CanonicalEventKind::BookSnapshot {
                outcome,
                bids,
                asks,
            } => {
                if active_slug_matches {
                    let exch_ms = ev.timestamp.exchange_ts_ms.unwrap_or(cur_sim_ms);
                    let mono_ns = ev.timestamp.mono_ns.unwrap_or(0);
                    let fixed =
                        CanonicalEvent::to_fixed_book_10(outcome, &bids, &asks, mono_ns, exch_ms)?;
                    match outcome {
                        Outcome::Up => {
                            if let Ok(valid) = state.book_up.apply_snapshot(
                                fixed.bids_slice(),
                                fixed.asks_slice(),
                                mono_ns,
                                exch_ms,
                            ) {
                                state.latest_up = Some(valid);
                            }
                        }
                        Outcome::Down => {
                            if let Ok(valid) = state.book_dn.apply_snapshot(
                                fixed.bids_slice(),
                                fixed.asks_slice(),
                                mono_ns,
                                exch_ms,
                            ) {
                                state.latest_dn = Some(valid);
                            }
                        }
                    }
                }
            }
            CanonicalEventKind::PriceChange {
                outcome,
                side,
                price,
                size,
                ..
            } => {
                if active_slug_matches {
                    let exch_ms = ev.timestamp.exchange_ts_ms.unwrap_or(cur_sim_ms);
                    let mono_ns = ev.timestamp.mono_ns.unwrap_or(0);
                    match outcome {
                        Outcome::Up => {
                            let _ = state.book_up.apply_delta_allow_transient_cross(
                                side, price, size, mono_ns, exch_ms,
                            );
                            if !state.book_up.is_crossed()
                                && let Ok(top) = state.book_up.top_n::<10>()
                            {
                                state.latest_up = Some(top);
                            }
                        }
                        Outcome::Down => {
                            let _ = state.book_dn.apply_delta_allow_transient_cross(
                                side, price, size, mono_ns, exch_ms,
                            );
                            if !state.book_dn.is_crossed()
                                && let Ok(top) = state.book_dn.top_n::<10>()
                            {
                                state.latest_dn = Some(top);
                            }
                        }
                    }
                    broker.on_book_update(outcome, side, price, size, exch_ms);
                }
            }
            CanonicalEventKind::TradePrint {
                outcome,
                side,
                price,
                size,
                ..
            } => {
                if active_slug_matches {
                    let exch_ms = ev.timestamp.exchange_ts_ms.unwrap_or(cur_sim_ms);
                    handle_replay_trade_print(
                        &mut state,
                        &mut pipe,
                        (&mut broker, &mut ledger, &mut telemetry),
                        (outcome, side, price, size, exch_ms),
                        &mut jsonl,
                        &clock,
                    )?;
                }
            }
            CanonicalEventKind::TickSizeChange { new_tick_micro, .. } => {
                if active_slug_matches && let Some(desc) = state.desc.as_mut() {
                    desc.tick_size_micro = new_tick_micro;
                }
            }
            CanonicalEventKind::BinanceSpot { event } => {
                let spot = match event {
                    BinanceSpotEvent::BookTicker { bid_e8, ask_e8, .. } => {
                        (bid_e8 as f64 + ask_e8 as f64) / 200_000_000.0
                    }
                    BinanceSpotEvent::AggTrade { price_e8, .. } => price_e8 as f64 / 100_000_000.0,
                };
                state.observe_spot(spot);
                if let Some(desc) = &state.desc {
                    let _ =
                        pipe.on_binance_spot(cur_sim_ms, spot, state.strike_usd, desc.expiry_ts_ms);
                }
            }
            CanonicalEventKind::MarketResolution {
                winning_outcome, ..
            } => {
                if active_slug_matches {
                    state.explicit_winning_outcome = Some(winning_outcome);
                }
            }
            CanonicalEventKind::ClockSync { .. }
            | CanonicalEventKind::BestBidAsk { .. }
            | CanonicalEventKind::OnchainFill { .. } => {}
        }

        if let Some(desc) = state.desc.as_ref() {
            let bounded_ms = cur_sim_ms.min(desc.expiry_ts_ms);
            if state.next_eval_ms.is_none() {
                let start_eval = bounded_ms.max(desc.start_ts_ms);
                state.next_eval_ms = Some(start_eval);
                state.next_summary_ms = Some(start_eval.saturating_add(5_000));
            }
            if let Some(target) = state.next_eval_ms
                && bounded_ms.saturating_sub(target) > 600_000
            {
                state.next_eval_ms = Some(bounded_ms);
            }
            while state
                .next_eval_ms
                .is_some_and(|target| bounded_ms >= target)
            {
                let eval_now = state.next_eval_ms.unwrap_or(bounded_ms);
                state.next_eval_ms = Some(eval_now.saturating_add(100));
                if !header_printed && let Some(d) = state.desc.as_ref() {
                    print_paper_header(
                        TraceSourceMode::Replay.as_str(),
                        d,
                        args.v_max,
                        args.base_size,
                        args.latency_ms,
                        &args.duration,
                        &args.jsonl,
                    );
                    header_printed = true;
                }
                eval_sim_step(
                    eval_now,
                    &mut state,
                    &mut pipe,
                    (&mut broker, &mut ledger, &mut telemetry),
                    cfg,
                    args,
                    (&mut tracker, &mut jsonl, &clock),
                )?;
            }
        }
    }

    if let Some(desc) = state.desc.as_ref()
        && !state.epoch_settled
        && (last_sim_ms >= desc.expiry_ts_ms || state.explicit_winning_outcome.is_some())
    {
        let settle_ts = last_sim_ms.max(desc.expiry_ts_ms);
        if let Some(pending) = state.settle_expired_epoch(
            settle_ts,
            &mut broker,
            &mut ledger,
            &mut telemetry,
            &mut pipe,
        ) {
            let now_ns = clock.now_ns().unwrap_or(0);
            record_epoch_rollover_jsonl(
                &mut jsonl,
                settle_ts,
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
        state.epoch_settled = true;
    }

    if state.desc.is_none() {
        return Err("canonical replay file contains no `market_metadata` event".into());
    }

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

fn handle_replay_trade_print(
    state: &mut PaperState,
    pipe: &mut MathPipeline,
    sim: SimTuple<'_>,
    trade: (Outcome, OrderSide, MicroUsdc, SharesMicro, u64),
    jsonl: &mut JsonlWriter,
    clock: &MonotonicClock,
) -> Result<(), Box<dyn Error>> {
    let (broker, ledger, telemetry) = sim;
    let (outcome, side, price, size, exch_ts_ms) = trade;
    pipe.on_poly_trade(outcome, size);

    let vol_ahead_before = broker
        .active_order(outcome, OrderSide::Buy)
        .map_or(SharesMicro::ZERO, |o| o.volume_ahead);

    let fills = broker.on_public_trade_with_side(outcome, side, price, size, exch_ts_ms);

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

    Ok(())
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
    let now_ms = state
        .next_eval_ms
        .unwrap_or_else(|| MonotonicClock::unix_now_ms().unwrap_or(0));
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
    let latency_sum = tracker.summary();

    let disp = PaperSummaryDisplay {
        q_up: ledger.filled_up,
        q_dn: ledger.filled_dn,
        net_q: ledger.net_inventory(),
        pairs_merged: ledger.total_pairs_merged,
        realized_pnl: ledger.realized_pnl_micro,
        unrealized_pnl: unrealized,
        fees_paid: ledger.fees_paid_micro,
        estimated_rebate: ledger.estimated_rebate_micro,
        rpm_60s: telemetry.rpm_60s(now_ms),
        fill_ratio: telemetry.fill_ratio(),
        markout_1s_bps: telemetry.avg_markout_1s_bps(),
        markout_5s_bps: telemetry.avg_markout_5s_bps(),
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
            let epoch_secs =
                (desc.expiry_ts_ms.saturating_sub(desc.start_ts_ms) as f64 / 1000.0).max(1.0);
            let _ = pipe.set_epoch_secs(epoch_secs);
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
                    broker.on_book_update(outcome, side, price, size, exch_ts_ms);
                }
                ReplayPolyEvent::LastTrade {
                    outcome,
                    side,
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

                    // Execute directional trade against broker
                    let fills =
                        broker.on_public_trade_with_side(outcome, side, price, size, exch_ts_ms);

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
                let latency_sum = tracker.summary();

                let disp = PaperSummaryDisplay {
                    q_up: ledger.filled_up,
                    q_dn: ledger.filled_dn,
                    net_q: ledger.net_inventory(),
                    pairs_merged: ledger.total_pairs_merged,
                    realized_pnl: ledger.realized_pnl_micro,
                    unrealized_pnl: unrealized,
                    fees_paid: ledger.fees_paid_micro,
                    estimated_rebate: ledger.estimated_rebate_micro,
                    rpm_60s: telemetry.rpm_60s(eval_now),
                    fill_ratio: telemetry.fill_ratio(),
                    markout_1s_bps: telemetry.avg_markout_1s_bps(),
                    markout_5s_bps: telemetry.avg_markout_5s_bps(),
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

fn effective_order_size(
    outcome: Outcome,
    target_price: MicroUsdc,
    base_size: SharesMicro,
    inventory_q: core_types::SignedSharesMicro,
    max_entry_prob: Option<f64>,
    max_entry_price: Option<f64>,
    min_order_size_micro: SharesMicro,
) -> Option<SharesMicro> {
    if math_engine::is_entry_guarded(target_price, max_entry_prob, max_entry_price) {
        let offset = match outcome {
            Outcome::Up if inventory_q.0 < 0 => inventory_q.0.unsigned_abs(),
            Outcome::Down if inventory_q.0 > 0 => inventory_q.0.unsigned_abs(),
            _ => 0,
        };
        let capped = base_size.min(SharesMicro::from_raw(offset));
        return (capped >= min_order_size_micro).then_some(capped);
    }
    Some(base_size)
}

fn eval_sim_step(
    now_ms: u64,
    state: &mut PaperState,
    pipe: &mut MathPipeline,
    sim: SimTuple<'_>,
    cfg: &EngineConfig,
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
        cost_context: CostContext {
            avg_cost_up: ledger.avg_cost_up(),
            avg_cost_dn: ledger.avg_cost_dn(),
            allow_cross_tick_override: false,
        },
    };

    let trace = pipe.evaluate(&inp)?;
    if let Some(st2) = trace.station2 {
        state.twap_60s_usd = st2.terminal_twap_usd.or(st2.twap_60s_usd);
    }
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
    let sized_up = desired_bid_up.and_then(|target_price| {
        effective_order_size(
            Outcome::Up,
            target_price,
            base_size,
            inventory_q,
            cfg.max_entry_prob,
            cfg.max_entry_price,
            desc.min_order_size_micro,
        )
        .map(|sz| (target_price, sz))
    });
    match sized_up {
        Some((target_price, order_size)) => {
            let should_replace = match state.active_up_id.and_then(|id| broker.get_order(id)) {
                Some(order) => {
                    order.price != target_price
                        || order.remaining_size > order_size
                        || !order.status.is_active()
                }
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
                    size: order_size,
                    now_ms,
                    current_best_bid: best_bid_up,
                    current_best_ask: best_ask_up,
                    book_depth_at_price: depth,
                }) {
                    state.active_up_id = Some(new_id);
                    state.stats.orders_placed = state.stats.orders_placed.saturating_add(1);
                    telemetry.record_order_posted(order_size);
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
    let sized_dn = desired_bid_dn.and_then(|target_price| {
        effective_order_size(
            Outcome::Down,
            target_price,
            base_size,
            inventory_q,
            cfg.max_entry_prob,
            cfg.max_entry_price,
            desc.min_order_size_micro,
        )
        .map(|sz| (target_price, sz))
    });
    match sized_dn {
        Some((target_price, order_size)) => {
            let should_replace = match state.active_dn_id.and_then(|id| broker.get_order(id)) {
                Some(order) => {
                    order.price != target_price
                        || order.remaining_size > order_size
                        || !order.status.is_active()
                }
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
                    size: order_size,
                    now_ms,
                    current_best_bid: best_bid_dn,
                    current_best_ask: best_ask_dn,
                    book_depth_at_price: depth,
                }) {
                    state.active_dn_id = Some(new_id);
                    state.stats.orders_placed = state.stats.orders_placed.saturating_add(1);
                    telemetry.record_order_posted(order_size);
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

    let mid_up = fixed_book_mid(up_book);
    let mid_dn = fixed_book_mid(dn_book);

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

    let target_duration = parse_duration_arg(&args.duration)?;
    let fresh = match crate::window_gate::wait_and_discover_fresh_5m_market(
        cfg,
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
        mut desc,
        ..
    } = fresh;

    let pipe_cfg = MathPipelineConfig::from_engine_config(cfg)?;
    let mut pipe = MathPipeline::new(pipe_cfg, clock)?;
    let mut tracker = LatencyTracker::new()?;
    let mut jsonl = JsonlWriter::create(&args.jsonl)?;
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
        queue_decay_policy: args.queue_decay,
    };
    let mut broker = FifoPaperBroker::new(broker_config);
    let mut ledger = PaperLedger::new(desc.fee_schedule);
    let mut telemetry = BaselineTelemetry::new()?;

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

    let deadline = tokio::time::Instant::now() + target_duration;
    let mut interval = tokio::time::interval(Duration::from_millis(100));
    let mut last_rollover_probe: Option<tokio::time::Instant> = None;
    let mut market_resolved_flag = false;
    let mut epoch_settled = false;
    let mut pending_settlement: Option<PendingEpochSettlement> = None;

    let t = crate::theme::current();
    println!(
        "  {} Streaming live market feeds for paper simulation... {}",
        t.badge(crate::theme::Badge::Info),
        t.muted("(Press Ctrl+C to stop)")
    );

    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                println!(
                    "\n  {} Received Ctrl+C, shutting down paper simulator...",
                    t.badge(crate::theme::Badge::Signal)
                );
                break;
            }
            _ = tokio::time::sleep_until(deadline) => {
                println!(
                    "\n  {} Reached target duration {}, shutting down...",
                    t.badge(crate::theme::Badge::Info),
                    args.duration
                );
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
                    if !epoch_settled {
                        let ev_ts_ms = base_adj_ms.saturating_add(rx_ns.saturating_sub(base_mono_ns) / 1_000_000);
                        let _ = pipe.on_binance_spot(ev_ts_ms, spot, state.strike_usd, desc.expiry_ts_ms);
                    }
                }
                while let Ok(ev) = oracle_cons.pop() {
                    if epoch_settled {
                        continue;
                    }
                    match ev {
                        OracleTwapEvent::ChainlinkSpot { spot_e8, .. } => {
                            if spot_e8 > 0 {
                                state.observe_spot(spot_e8 as f64 / 100_000_000.0);
                            }
                        }
                        OracleTwapEvent::ChainlinkTwap { twap_e8, spot_e8, .. } => {
                            if twap_e8 > 0 {
                                state.oracle_twap_60s_usd = Some(twap_e8 as f64 / 100_000_000.0);
                            }
                            if spot_e8 > 0 {
                                state.observe_spot(spot_e8 as f64 / 100_000_000.0);
                            }
                        }
                    }
                }
                while let Ok(ev) = poly_cons.pop() {
                    if epoch_settled || ev.epoch() != state.clob_epoch {
                        continue;
                    }
                    handle_live_poly_event(
                        ev,
                        &desc.slug,
                        &mut state,
                        &mut pipe,
                        (&mut broker, &mut ledger, &mut telemetry),
                        (&mut jsonl, &clock),
                    )?;
                }
                while let Ok(h) = health_rx.try_recv() {
                    if !epoch_settled
                        && let FeedHealthEvent::MarketResolved { .. } = h
                    {
                        println!(
                            "  {} Active 5m epoch market resolved!",
                            t.badge(crate::theme::Badge::Ready)
                        );
                        market_resolved_flag = true;
                    }
                }

                // Compute now_ms from base clock
                let now_ns = clock.now_ns()?;
                let now_ms = base_adj_ms.saturating_add(now_ns.saturating_sub(base_mono_ns) / 1_000_000);
                state.next_eval_ms = Some(now_ms);

                let tau_secs = desc.expiry_ts_ms.saturating_sub(now_ms) / 1000;
                let is_expired = now_ms >= desc.expiry_ts_ms || (market_resolved_flag && tau_secs == 0);

                if is_expired {
                    if !epoch_settled {
                        pending_settlement = state.settle_expired_epoch(
                            now_ms,
                            &mut broker,
                            &mut ledger,
                            &mut telemetry,
                            &mut pipe,
                        );
                        epoch_settled = true;
                    }

                    let probe_due = last_rollover_probe
                        .is_none_or(|t| t.elapsed() >= Duration::from_secs(1));
                    if probe_due {
                        last_rollover_probe = Some(tokio::time::Instant::now());
                        match discover_next_5m_market(
                            &discovery,
                            &offset_est,
                            &desc.condition_id,
                        )
                        .await
                        {
                            Ok(next_desc) => {
                                println!(
                                    "\n  {} Discovered next 5m market: {} (expires in {}s)",
                                    t.badge(crate::theme::Badge::Epoch),
                                    t.cyan(&next_desc.slug),
                                    next_desc.expiry_ts_ms.saturating_sub(now_ms) / 1000
                                );
                                let next_epoch = match handles
                                    .send_clob_rollover(
                                        next_desc.asset_id_up.clone(),
                                        next_desc.asset_id_dn.clone(),
                                    )
                                    .await
                                {
                                    Ok(ep) => ep,
                                    Err(err) => {
                                        eprintln!(
                                            "  {} Failed to switch CLOB feed subscriptions: {err}",
                                            t.badge(crate::theme::Badge::Fail)
                                        );
                                        break;
                                    }
                                };

                                while health_rx.try_recv().is_ok() {}

                                if let Some(pending) = pending_settlement.take() {
                                    print_epoch_rollover(
                                        &pending.expired_slug,
                                        Some(&next_desc.slug),
                                        pending.winning_outcome,
                                        pending.settle_delta_micro,
                                        pending.total_realized_pnl_micro,
                                    );
                                    record_epoch_rollover_jsonl(
                                        &mut jsonl,
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

                                state.rollover_to_market(
                                    next_desc.clone(),
                                    next_epoch,
                                    &mut broker,
                                    &mut ledger,
                                    &mut telemetry,
                                    &mut pipe,
                                );

                                desc = next_desc;
                                epoch_settled = false;
                                market_resolved_flag = false;
                                last_rollover_probe = None;

                                while let Ok(ev) = poly_cons.pop() {
                                    if ev.epoch() != state.clob_epoch {
                                        continue;
                                    }
                                    handle_live_poly_event(
                                        ev,
                                        &desc.slug,
                                        &mut state,
                                        &mut pipe,
                                        (&mut broker, &mut ledger, &mut telemetry),
                                        (&mut jsonl, &clock),
                                    )?;
                                }

                                print_paper_header(
                                    TraceSourceMode::Live.as_str(),
                                    &desc,
                                    args.v_max,
                                    args.base_size,
                                    args.latency_ms,
                                    &args.duration,
                                    &args.jsonl,
                                );
                            }
                            Err(err) => {
                                eprintln!(
                                    "  {} Awaiting next 5m BTC market after {}: {err}",
                                    t.badge(crate::theme::Badge::Epoch),
                                    desc.slug
                                );
                            }
                        }
                    }
                } else {
                    eval_sim_step(
                        now_ms,
                        &mut state,
                        &mut pipe,
                        (&mut broker, &mut ledger, &mut telemetry),
                        cfg,
                        args,
                        (&mut tracker, &mut jsonl, &clock),
                    )?;
                }

                // Print periodic summary every 5 seconds
                if state.next_summary_ms.is_none() {
                    state.next_summary_ms = Some(now_ms.saturating_add(5_000));
                }
                if state.next_summary_ms.is_some_and(|target| now_ms >= target) {
                    state.next_summary_ms = Some(now_ms.saturating_add(5_000));
                    let mid_up = state.latest_up.as_ref().and_then(fixed_book_mid).unwrap_or(MicroUsdc(500_000));
                    let mid_dn = state.latest_dn.as_ref().and_then(fixed_book_mid).unwrap_or(MicroUsdc(500_000));
                    let unrealized = ledger.unrealized_pnl_micro(mid_up, mid_dn);
                    let latency_sum = tracker.summary();

                    let disp = PaperSummaryDisplay {
                        q_up: ledger.filled_up,
                        q_dn: ledger.filled_dn,
                        net_q: ledger.net_inventory(),
                        pairs_merged: ledger.total_pairs_merged,
                        realized_pnl: ledger.realized_pnl_micro,
                        unrealized_pnl: unrealized,
                        fees_paid: ledger.fees_paid_micro,
                        estimated_rebate: ledger.estimated_rebate_micro,
                        rpm_60s: telemetry.rpm_60s(now_ms),
                        fill_ratio: telemetry.fill_ratio(),
                        markout_1s_bps: telemetry.avg_markout_1s_bps(),
                        markout_5s_bps: telemetry.avg_markout_5s_bps(),
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
    let shutdown_ms = MonotonicClock::unix_now_ms()?;
    if let Some(id) = state.active_up_id.take() {
        let _ = broker.cancel(id, shutdown_ms);
    }
    if let Some(id) = state.active_dn_id.take() {
        let _ = broker.cancel(id, shutdown_ms);
    }
    let _ = broker.force_cancel_all();

    shutdown_actors(handles, cfg.polybolt_enabled).await;

    if let Some(pending) = pending_settlement.take() {
        let now_ns = clock.now_ns().unwrap_or(0);
        let now_ms = base_adj_ms.saturating_add(now_ns.saturating_sub(base_mono_ns) / 1_000_000);
        print_epoch_rollover(
            &pending.expired_slug,
            None,
            pending.winning_outcome,
            pending.settle_delta_micro,
            pending.total_realized_pnl_micro,
        );
        let _ = record_epoch_rollover_jsonl(
            &mut jsonl,
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
        );
    }

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

fn handle_live_poly_event(
    ev: PolyMarketEvent,
    slug: &str,
    state: &mut PaperState,
    pipe: &mut MathPipeline,
    (broker, ledger, telemetry): SimTuple<'_>,
    (jsonl, clock): (&mut JsonlWriter, &MonotonicClock),
) -> Result<(), Box<dyn Error>> {
    match ev {
        PolyMarketEvent::BookSnapshot {
            outcome,
            book,
            rx_mono_ns,
            exch_ts_ms,
            ..
        } => match outcome {
            Outcome::Up => {
                let _ = state.book_up.apply_snapshot(
                    book.bids_slice(),
                    book.asks_slice(),
                    rx_mono_ns,
                    exch_ts_ms,
                );
                state.latest_up = Some(book);
            }
            Outcome::Down => {
                let _ = state.book_dn.apply_snapshot(
                    book.bids_slice(),
                    book.asks_slice(),
                    rx_mono_ns,
                    exch_ts_ms,
                );
                state.latest_dn = Some(book);
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
        } => {
            match outcome {
                Outcome::Up => {
                    let _ = state
                        .book_up
                        .apply_price_change_delta(side, price, size, rx_mono_ns, exch_ts_ms);
                    if let Ok(top) = state.book_up.top_n::<10>() {
                        state.latest_up = Some(top);
                    }
                }
                Outcome::Down => {
                    let _ = state
                        .book_dn
                        .apply_price_change_delta(side, price, size, rx_mono_ns, exch_ts_ms);
                    if let Ok(top) = state.book_dn.top_n::<10>() {
                        state.latest_dn = Some(top);
                    }
                }
            }
            broker.on_book_update(outcome, side, price, size, exch_ts_ms);
        }
        PolyMarketEvent::LastTrade {
            outcome,
            side,
            price,
            size,
            exch_ts_ms,
            ..
        } => {
            pipe.on_poly_trade(outcome, size);

            let vol_ahead_before = broker
                .active_order(outcome, OrderSide::Buy)
                .map_or(SharesMicro::ZERO, |o| o.volume_ahead);

            let fills = broker.on_public_trade_with_side(outcome, side, price, size, exch_ts_ms);

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
        }
        PolyMarketEvent::TickSizeChange { new_tick_micro, .. } => {
            if let Some(d) = state.desc.as_mut() {
                d.tick_size_micro = new_tick_micro;
            }
        }
        PolyMarketEvent::BestBidAsk { .. } => {}
    }
    Ok(())
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

#[cfg(test)]
mod tests {
    use super::*;
    use core_types::{BookLevel, FeeSchedule, ProtocolVersion};
    use paper_sim::SimFill;

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

    fn test_book(outcome: Outcome, best_bid_micro: u64, best_ask_micro: u64) -> FixedBook<10> {
        let bids = [BookLevel {
            price: MicroUsdc(best_bid_micro),
            size: SharesMicro(100 * ONE_SHARE_MICRO),
        }];
        let asks = [BookLevel {
            price: MicroUsdc(best_ask_micro),
            size: SharesMicro(100 * ONE_SHARE_MICRO),
        }];
        FixedBook::try_from_slices(outcome, &bids, &asks, 0, 0).unwrap()
    }

    #[test]
    fn test_paper_epoch_settlement_and_rollover() {
        let clock = MonotonicClock::start();
        let start_ms = 1_700_000_000_000_u64;
        let expiry_ms = start_ms + 300_000;
        let market1 = test_market("btc-5m-epoch1", start_ms, expiry_ms);

        let mut state = PaperState::new();
        state.set_market(market1.clone());
        state.observe_spot(95_000.0);
        state.strike_usd = Some(94_000.0); // Spot > Strike -> Outcome::Up wins
        state.latest_up = Some(test_book(Outcome::Up, 490_000, 510_000));
        state.latest_dn = Some(test_book(Outcome::Down, 490_000, 510_000));

        let broker_config = FifoBrokerConfig::default();
        let base_size = SharesMicro(20 * ONE_SHARE_MICRO);
        let mut broker = FifoPaperBroker::new(broker_config);
        let mut ledger = PaperLedger::new(market1.fee_schedule);
        let mut telemetry = BaselineTelemetry::new().unwrap();

        let cfg = EngineConfig::default();
        let pipe_cfg = MathPipelineConfig::from_engine_config(&cfg).unwrap();
        let mut pipe = MathPipeline::new(pipe_cfg, clock).unwrap();

        // Simulate an active UP order
        let order_id = broker
            .submit_new(NewOrderParams {
                outcome: Outcome::Up,
                side: OrderSide::Buy,
                price: MicroUsdc(480_000),
                size: base_size,
                now_ms: start_ms,
                current_best_bid: Some(MicroUsdc(490_000)),
                current_best_ask: Some(MicroUsdc(510_000)),
                book_depth_at_price: SharesMicro::ZERO,
            })
            .unwrap();
        state.active_up_id = Some(order_id);

        // Record a fill on UP: 20 shares @ $0.48
        let fill = SimFill {
            sim_id: order_id,
            outcome: Outcome::Up,
            side: OrderSide::Buy,
            price: MicroUsdc(480_000),
            filled_shares: base_size,
            fill_ts_ms: start_ms + 1000,
            is_maker: true,
        };
        let _ = ledger.record_fill(&fill).unwrap();
        assert_eq!(ledger.filled_up, base_size);

        // At expiry, settle epoch
        let pending = state
            .settle_expired_epoch(
                expiry_ms,
                &mut broker,
                &mut ledger,
                &mut telemetry,
                &mut pipe,
            )
            .expect("should produce pending settlement");

        assert_eq!(pending.expired_slug, "btc-5m-epoch1");
        assert_eq!(pending.winning_outcome, Outcome::Up);
        // 20 shares bought at $0.48, settled at $1.00 -> gain = 20 * (1.00 - 0.48) = $10.40 = 10_400_000 micro
        assert_eq!(pending.settle_delta_micro, 10_400_000);
        assert_eq!(pending.total_realized_pnl_micro, 10_400_000);
        assert_eq!(ledger.filled_up, SharesMicro::ZERO);
        assert_eq!(state.active_up_id, None);

        // Rollover to next market
        let market2 = test_market("btc-5m-epoch2", expiry_ms, expiry_ms + 300_000);
        state.rollover_to_market(
            market2.clone(),
            1,
            &mut broker,
            &mut ledger,
            &mut telemetry,
            &mut pipe,
        );

        assert_eq!(state.desc.as_ref().unwrap().slug, "btc-5m-epoch2");
        assert_eq!(state.clob_epoch, 1);
        assert_eq!(state.stats.epoch_rollovers, 1);
        assert!(state.latest_up.is_none());
        assert!(state.latest_dn.is_none());
        assert_eq!(state.active_up_id, None);
        assert_eq!(state.active_dn_id, None);
    }

    #[test]
    fn test_winning_outcome_resolution() {
        let mut state = PaperState::new();
        state.set_market(test_market("btc-5m-test", 0, 300_000));

        // Case 1: Spot >= Strike -> Up (when book is 50/50 or absent)
        state.spot_usd = Some(95_000.0);
        state.strike_usd = Some(95_000.0);
        assert_eq!(state.resolve_winning_outcome(), Outcome::Up);

        // Case 2: Spot < Strike -> Down
        state.spot_usd = Some(94_999.0);
        state.strike_usd = Some(95_000.0);
        assert_eq!(state.resolve_winning_outcome(), Outcome::Down);

        // Case 3: Trailing 60s TWAP must NOT override instantaneous terminal spot
        state.spot_usd = Some(95_500.0);
        state.twap_60s_usd = Some(94_900.0);
        state.oracle_twap_60s_usd = Some(94_800.0);
        state.strike_usd = Some(95_000.0);
        assert_eq!(state.resolve_winning_outcome(), Outcome::Up);

        // Case 4: Unambiguous one-sided terminal CLOB book ($0.99 bid, no ask) overrides spot drift
        state.spot_usd = Some(94_990.0);
        state.strike_usd = Some(95_000.0);
        state.latest_up = Some(
            FixedBook::try_from_slices(
                Outcome::Up,
                &[BookLevel {
                    price: MicroUsdc(990_000),
                    size: SharesMicro(100 * ONE_SHARE_MICRO),
                }],
                &[],
                0,
                0,
            )
            .unwrap(),
        );
        state.latest_dn = None;
        assert_eq!(state.resolve_winning_outcome(), Outcome::Up);

        // Case 5: Ambiguous book + no strike -> fallback to book implied (UP >= DN -> Up)
        state.spot_usd = None;
        state.twap_60s_usd = None;
        state.oracle_twap_60s_usd = None;
        state.strike_usd = None;
        state.latest_up = Some(test_book(Outcome::Up, 550_000, 560_000));
        state.latest_dn = Some(test_book(Outcome::Down, 440_000, 450_000));
        assert_eq!(state.resolve_winning_outcome(), Outcome::Up);

        // Case 6: Fallback to book implied (UP < DN -> Down)
        state.latest_up = Some(test_book(Outcome::Up, 440_000, 450_000));
        state.latest_dn = Some(test_book(Outcome::Down, 550_000, 560_000));
        assert_eq!(state.resolve_winning_outcome(), Outcome::Down);
    }

    #[test]
    fn test_paper_sim_post_rollover_resumes_quoting() {
        let clock = MonotonicClock::start();
        let start_ms = 1_700_000_000_000_u64;
        let expiry_ms = start_ms + 300_000;
        let market1 = test_market("btc-5m-epoch1", start_ms, expiry_ms);

        let mut state = PaperState::new();
        state.set_market(market1.clone());

        let broker_config = FifoBrokerConfig::default();
        let mut broker = FifoPaperBroker::new(broker_config);
        let mut ledger = PaperLedger::new(market1.fee_schedule);
        let mut telemetry = BaselineTelemetry::new().unwrap();

        let cfg = EngineConfig::default();
        let pipe_cfg = MathPipelineConfig::from_engine_config(&cfg).unwrap();
        let mut pipe = MathPipeline::new(pipe_cfg, clock).unwrap();
        let mut tracker = LatencyTracker::new().unwrap();
        let temp_dir = std::env::temp_dir();
        let test_jsonl = temp_dir.join("test_rollover_resume.jsonl");
        let mut jsonl = JsonlWriter::create(&test_jsonl).unwrap();
        let args = PaperArgs {
            source: TraceSourceMode::Live,
            input: None,
            duration: "300s".to_string(),
            allow_mid_window: false,
            v_max: 50,
            base_size: 20,
            latency_ms: 0,
            queue_decay: paper_sim::QueueDecayPolicy::Conservative,
            math: crate::MathOverrides::default(),
            source_filter: SourceFilter::All,
            min_book_quality: BookQualityFilter::Any,
            jsonl: test_jsonl.clone(),
        };

        // Rollover to market 2
        let market2 = test_market("btc-5m-epoch2", expiry_ms, expiry_ms + 300_000);
        state.rollover_to_market(
            market2.clone(),
            1,
            &mut broker,
            &mut ledger,
            &mut telemetry,
            &mut pipe,
        );

        // Before book snapshots arrive: latest_up and latest_dn are None
        assert!(state.latest_up.is_none());
        assert!(state.latest_dn.is_none());
        // eval_sim_step should return Ok(()) without panicking or placing quotes
        let res = eval_sim_step(
            expiry_ms + 100,
            &mut state,
            &mut pipe,
            (&mut broker, &mut ledger, &mut telemetry),
            &cfg,
            &args,
            (&mut tracker, &mut jsonl, &clock),
        );
        assert!(res.is_ok());
        assert!(broker.active_order(Outcome::Up, OrderSide::Buy).is_none());
        assert!(broker.active_order(Outcome::Down, OrderSide::Buy).is_none());

        // Book snapshots arrive for market 2
        let up_book = test_book(Outcome::Up, 490_000, 510_000);
        let dn_book = test_book(Outcome::Down, 490_000, 510_000);
        state
            .book_up
            .apply_snapshot(
                up_book.bids_slice(),
                up_book.asks_slice(),
                clock.now_ns().unwrap(),
                expiry_ms + 200,
            )
            .unwrap();
        state
            .book_dn
            .apply_snapshot(
                dn_book.bids_slice(),
                dn_book.asks_slice(),
                clock.now_ns().unwrap(),
                expiry_ms + 200,
            )
            .unwrap();
        state.latest_up = Some(up_book);
        state.latest_dn = Some(dn_book);
        state.observe_spot(95_000.0);
        state.strike_usd = Some(95_000.0);

        // Feed binance spot to prime the pipeline
        for i in 0..15 {
            let t = expiry_ms + 200 + i * 100;
            let _ = pipe.on_binance_spot(
                t,
                95_000.0 + (i as f64 * 0.1),
                state.strike_usd,
                market2.expiry_ts_ms,
            );
        }

        // Now eval_sim_step evaluates without panic
        let res = eval_sim_step(
            expiry_ms + 2000,
            &mut state,
            &mut pipe,
            (&mut broker, &mut ledger, &mut telemetry),
            &cfg,
            &args,
            (&mut tracker, &mut jsonl, &clock),
        );
        assert!(res.is_ok());
        let _ = std::fs::remove_file(test_jsonl);
    }

    #[test]
    fn test_settlement_uses_terminal_spot_not_60s_twap() {
        let clock = MonotonicClock::start();
        let start_ms = 1_700_000_000_000_u64;
        let expiry_ms = start_ms + 300_000;
        let market = test_market("btc-5m-spot-settle", start_ms, expiry_ms);

        let mut state = PaperState::new();
        state.set_market(market.clone());
        state.strike_usd = Some(95_000.0);

        let broker_config = FifoBrokerConfig::default();
        let mut broker = FifoPaperBroker::new(broker_config);
        let mut ledger = PaperLedger::new(market.fee_schedule);
        let mut telemetry = BaselineTelemetry::new().unwrap();

        let cfg = EngineConfig::default();
        let pipe_cfg = MathPipelineConfig::from_engine_config(&cfg).unwrap();
        let mut pipe = MathPipeline::new(pipe_cfg, clock).unwrap();

        // Feed [T-60s, T-1s] at $94,900, then move at T-1s to $95,500.
        // Trailing 60s TWAP stays < $95,000, but terminal spot at expiry >= strike -> Up wins.
        let _ = pipe.on_binance_spot(expiry_ms - 60_000, 94_900.0, state.strike_usd, expiry_ms);
        let _ = pipe.on_binance_spot(expiry_ms - 1_000, 95_500.0, state.strike_usd, expiry_ms);
        state.observe_spot(95_500.0);

        let pending = state
            .settle_expired_epoch(
                expiry_ms,
                &mut broker,
                &mut ledger,
                &mut telemetry,
                &mut pipe,
            )
            .expect("should settle using terminal spot");
        assert_eq!(pending.winning_outcome, Outcome::Up);
        assert!(state.twap_60s_usd.is_some_and(|v| v < 95_000.0));
    }

    #[test]
    fn test_settlement_cancels_primary_broker_without_post_expiry_fills() {
        let clock = MonotonicClock::start();
        let start_ms = 1_700_000_000_000_u64;
        let expiry_ms = start_ms + 300_000;
        let market = test_market("btc-5m-cancel-both", start_ms, expiry_ms);

        let mut state = PaperState::new();
        state.set_market(market.clone());
        state.observe_spot(95_000.0);
        state.strike_usd = Some(94_000.0);

        let broker_config = FifoBrokerConfig {
            order_latency_ms: 40,
            cancel_latency_ms: 40,
            ..Default::default()
        };
        let base_size = SharesMicro(20 * ONE_SHARE_MICRO);
        let mut broker = FifoPaperBroker::new(broker_config);
        let mut ledger = PaperLedger::new(market.fee_schedule);
        let mut telemetry = BaselineTelemetry::new().unwrap();

        let cfg = EngineConfig::default();
        let pipe_cfg = MathPipelineConfig::from_engine_config(&cfg).unwrap();
        let mut pipe = MathPipeline::new(pipe_cfg, clock).unwrap();

        // Place active orders in primary broker (past 40ms placement latency).
        let up_id = broker
            .submit_new(NewOrderParams {
                outcome: Outcome::Up,
                side: OrderSide::Buy,
                price: MicroUsdc(480_000),
                size: base_size,
                now_ms: start_ms,
                current_best_bid: Some(MicroUsdc(490_000)),
                current_best_ask: Some(MicroUsdc(510_000)),
                book_depth_at_price: SharesMicro::ZERO,
            })
            .unwrap();
        state.active_up_id = Some(up_id);

        // Settle at expiry: the primary active order must be cancelled and unfillable.
        let _ = state
            .settle_expired_epoch(
                expiry_ms,
                &mut broker,
                &mut ledger,
                &mut telemetry,
                &mut pipe,
            )
            .unwrap();

        assert_eq!(state.active_up_id, None);
        assert_eq!(state.active_dn_id, None);
        assert!(broker.active_order(Outcome::Up, OrderSide::Buy).is_none());

        // Trade prints arriving immediately after settlement (even within 40ms cancel latency)
        // must produce zero fills.
        let post_fills = broker.on_public_trade(
            Outcome::Up,
            MicroUsdc(470_000),
            SharesMicro(50 * ONE_SHARE_MICRO),
            expiry_ms + 10,
        );
        assert!(post_fills.is_empty());
    }

    #[test]
    fn test_rollover_clears_pending_markout_probes() {
        let clock = MonotonicClock::start();
        let start_ms = 1_700_000_000_000_u64;
        let expiry_ms = start_ms + 300_000;
        let market1 = test_market("btc-5m-markout-1", start_ms, expiry_ms);
        let market2 = test_market("btc-5m-markout-2", expiry_ms, expiry_ms + 300_000);

        let mut state = PaperState::new();
        state.set_market(market1.clone());
        state.observe_spot(95_000.0);

        let broker_config = FifoBrokerConfig::default();
        let base_size = SharesMicro(20 * ONE_SHARE_MICRO);
        let mut broker = FifoPaperBroker::new(broker_config);
        let mut ledger = PaperLedger::new(market1.fee_schedule);
        let mut telemetry = BaselineTelemetry::new().unwrap();
        let cfg = EngineConfig::default();
        let pipe_cfg = MathPipelineConfig::from_engine_config(&cfg).unwrap();
        let mut pipe = MathPipeline::new(pipe_cfg, clock).unwrap();

        // Fill near the end of epoch 1 when UP mid is $0.90.
        let fill = SimFill {
            sim_id: 1,
            outcome: Outcome::Up,
            side: OrderSide::Buy,
            price: MicroUsdc(890_000),
            filled_shares: base_size,
            fill_ts_ms: expiry_ms - 500,
            is_maker: true,
        };
        let inv_before = ledger.net_inventory();
        let _ = ledger.record_fill(&fill).unwrap();
        let inv_after = ledger.net_inventory();
        telemetry.record_fill(&fill, inv_before, inv_after, MicroUsdc(900_000));
        assert_eq!(telemetry.pending_markout_probe_count(), 1);

        // Settle and rollover to epoch 2 where fresh market opens at $0.50.
        let _ = state.settle_expired_epoch(
            expiry_ms,
            &mut broker,
            &mut ledger,
            &mut telemetry,
            &mut pipe,
        );
        assert_eq!(telemetry.pending_markout_probe_count(), 0);

        state.rollover_to_market(
            market2,
            1,
            &mut broker,
            &mut ledger,
            &mut telemetry,
            &mut pipe,
        );
        assert_eq!(telemetry.pending_markout_probe_count(), 0);

        // Evaluating markouts in epoch 2 at $0.50 must NOT match epoch 1's $0.90 fill.
        telemetry.update_markouts(expiry_ms + 6_000, MicroUsdc(500_000), MicroUsdc(500_000));
        assert_eq!(telemetry.avg_markout_1s_bps(), 0.0);
        assert_eq!(telemetry.avg_markout_5s_bps(), 0.0);
    }

    #[test]
    fn test_stale_epoch_poly_events_are_ignored_after_rollover() {
        let clock = MonotonicClock::start();
        let start_ms = 1_700_000_000_000_u64;
        let expiry_ms = start_ms + 300_000;
        let market1 = test_market("btc-5m-stale-1", start_ms, expiry_ms);
        let market2 = test_market("btc-5m-stale-2", expiry_ms, expiry_ms + 300_000);

        let mut state = PaperState::new();
        state.set_market(market1.clone());

        let broker_config = FifoBrokerConfig::default();
        let mut broker = FifoPaperBroker::new(broker_config);
        let mut ledger = PaperLedger::new(market1.fee_schedule);
        let mut telemetry = BaselineTelemetry::new().unwrap();
        let cfg = EngineConfig::default();
        let pipe_cfg = MathPipelineConfig::from_engine_config(&cfg).unwrap();
        let mut pipe = MathPipeline::new(pipe_cfg, clock).unwrap();

        state.rollover_to_market(
            market2,
            1,
            &mut broker,
            &mut ledger,
            &mut telemetry,
            &mut pipe,
        );
        assert_eq!(state.clob_epoch, 1);

        let temp_dir = std::env::temp_dir();
        let test_jsonl = temp_dir.join("test_stale_epoch_guard.jsonl");
        let mut jsonl = JsonlWriter::create(&test_jsonl).unwrap();

        let (mut prod, mut cons) = RingBuffer::new(16);
        let stale_book = test_book(Outcome::Up, 110_000, 120_000);
        let fresh_book = test_book(Outcome::Up, 490_000, 510_000);
        prod.push(PolyMarketEvent::BookSnapshot {
            outcome: Outcome::Up,
            book: stale_book,
            rx_mono_ns: 100,
            exch_ts_ms: expiry_ms - 10,
            epoch: 0,
        })
        .unwrap();
        prod.push(PolyMarketEvent::BookSnapshot {
            outcome: Outcome::Up,
            book: fresh_book,
            rx_mono_ns: 200,
            exch_ts_ms: expiry_ms + 50,
            epoch: 1,
        })
        .unwrap();

        while let Ok(ev) = cons.pop() {
            if ev.epoch() != state.clob_epoch {
                continue;
            }
            handle_live_poly_event(
                ev,
                "btc-5m-stale-2",
                &mut state,
                &mut pipe,
                (&mut broker, &mut ledger, &mut telemetry),
                (&mut jsonl, &clock),
            )
            .unwrap();
        }

        assert_eq!(
            state.latest_up.unwrap().best_bid().unwrap().price,
            MicroUsdc(490_000)
        );
        let _ = std::fs::remove_file(test_jsonl);
    }

    #[test]
    fn test_directional_trade_filtering_and_rebate_carry_across_rollover() {
        let clock = MonotonicClock::start();
        let start_ms = 1_700_000_000_000_u64;
        let expiry_ms = start_ms + 300_000;
        let market1 = test_market("btc-5m-dir-1", start_ms, expiry_ms);
        let mut market2 = test_market("btc-5m-dir-2", expiry_ms, expiry_ms + 300_000);
        market2.fee_schedule.fee_rate_e6 = 0;

        let mut state = PaperState::new();
        state.set_market(market1.clone());

        let broker_config = FifoBrokerConfig {
            order_latency_ms: 0,
            cancel_latency_ms: 0,
            ..Default::default()
        };
        let base_size = SharesMicro(20 * ONE_SHARE_MICRO);
        let mut broker = FifoPaperBroker::new(broker_config);
        let mut ledger = PaperLedger::new(market1.fee_schedule);
        let mut telemetry = BaselineTelemetry::new().unwrap();
        let cfg = EngineConfig::default();
        let pipe_cfg = MathPipelineConfig::from_engine_config(&cfg).unwrap();
        let mut pipe = MathPipeline::new(pipe_cfg, clock).unwrap();

        let _ = broker
            .submit_new(NewOrderParams {
                outcome: Outcome::Up,
                side: OrderSide::Buy,
                price: MicroUsdc(490_000),
                size: base_size,
                now_ms: start_ms,
                current_best_bid: Some(MicroUsdc(490_000)),
                current_best_ask: Some(MicroUsdc(510_000)),
                book_depth_at_price: SharesMicro::ZERO,
            })
            .unwrap();

        let temp_dir = std::env::temp_dir();
        let test_jsonl = temp_dir.join("test_directional_rebate_carry.jsonl");
        let mut jsonl = JsonlWriter::create(&test_jsonl).unwrap();

        // Buy-aggressor trade must NOT fill resting Buy bid.
        handle_live_poly_event(
            PolyMarketEvent::LastTrade {
                outcome: Outcome::Up,
                side: OrderSide::Buy,
                price: MicroUsdc(480_000),
                size: SharesMicro(10 * ONE_SHARE_MICRO),
                rx_mono_ns: 100,
                exch_ts_ms: start_ms + 100,
                epoch: 0,
            },
            "btc-5m-dir-1",
            &mut state,
            &mut pipe,
            (&mut broker, &mut ledger, &mut telemetry),
            (&mut jsonl, &clock),
        )
        .unwrap();
        assert_eq!(ledger.filled_up, SharesMicro::ZERO);

        // Sell-aggressor trade fills resting Buy bid and accrues maker rebate.
        handle_live_poly_event(
            PolyMarketEvent::LastTrade {
                outcome: Outcome::Up,
                side: OrderSide::Sell,
                price: MicroUsdc(480_000),
                size: base_size,
                rx_mono_ns: 200,
                exch_ts_ms: start_ms + 200,
                epoch: 0,
            },
            "btc-5m-dir-1",
            &mut state,
            &mut pipe,
            (&mut broker, &mut ledger, &mut telemetry),
            (&mut jsonl, &clock),
        )
        .unwrap();
        assert_eq!(ledger.filled_up, base_size);
        let prior_rebate = ledger.estimated_rebate_micro;
        assert!(prior_rebate > 0);

        // Rollover into a zero-fee market must preserve prior epoch accrued rebate.
        state.rollover_to_market(
            market2,
            1,
            &mut broker,
            &mut ledger,
            &mut telemetry,
            &mut pipe,
        );
        assert_eq!(ledger.estimated_rebate_micro, prior_rebate);
        let _ = std::fs::remove_file(test_jsonl);
    }

    #[test]
    fn paper_hedge_order_size_caps_to_offsetting_inventory_above_max_entry_prob() {
        let base_size = SharesMicro(20 * ONE_SHARE_MICRO);
        let min_size = SharesMicro(5 * ONE_SHARE_MICRO);

        // 5 UP shares held, base_size = 20, DN bid = $0.90, max_entry_prob = 0.80 -> capped to 5 shares.
        assert_eq!(
            effective_order_size(
                Outcome::Down,
                MicroUsdc(900_000),
                base_size,
                core_types::SignedSharesMicro(5 * ONE_SHARE_MICRO as i64),
                Some(0.80),
                None,
                min_size,
            ),
            Some(SharesMicro(5 * ONE_SHARE_MICRO))
        );

        // 3 UP shares held (< 5 min order size) -> suppressed so zero excess directional shares open.
        assert_eq!(
            effective_order_size(
                Outcome::Down,
                MicroUsdc(900_000),
                base_size,
                core_types::SignedSharesMicro(3 * ONE_SHARE_MICRO as i64),
                Some(0.80),
                None,
                min_size,
            ),
            None
        );

        // Within [0.20, 0.80] -> full base_size preserved.
        assert_eq!(
            effective_order_size(
                Outcome::Down,
                MicroUsdc(500_000),
                base_size,
                core_types::SignedSharesMicro(5 * ONE_SHARE_MICRO as i64),
                Some(0.80),
                None,
                min_size,
            ),
            Some(base_size)
        );

        // Upper-only cap 0.60 caps reducing DN bid at $0.65 to offsetting UP shares (5), while preserving cheap $0.30 entries.
        assert_eq!(
            effective_order_size(
                Outcome::Down,
                MicroUsdc(650_000),
                base_size,
                core_types::SignedSharesMicro(5 * ONE_SHARE_MICRO as i64),
                Some(0.80),
                Some(0.60),
                min_size,
            ),
            Some(SharesMicro(5 * ONE_SHARE_MICRO))
        );
        assert_eq!(
            effective_order_size(
                Outcome::Down,
                MicroUsdc(300_000),
                base_size,
                core_types::SignedSharesMicro(0),
                Some(0.80),
                Some(0.60),
                min_size,
            ),
            Some(base_size)
        );
    }

    #[test]
    fn canonical_replay_handles_15m_pmdata_and_source_quality_filters() {
        use historical_data::{
            BookDerivation, CANONICAL_SCHEMA_VERSION, CanonicalEvent, CanonicalEventKind,
            CanonicalTimestamp, DataSource, EventProvenance, ResolutionSource, TimingEstimate,
            TimingQuality, UpDownInterval, write_canonical_jsonl,
        };

        let ts0 = 1_793_146_500_000_u64;
        let expiry = ts0 + 900_000;
        let slug = "btc-updown-15m-1793146500";
        let est = || CanonicalTimestamp {
            exchange_ts_ms: Some(ts0),
            exchange_ts_us: Some((ts0 as i64) * 1_000),
            provider_local_ts_us: Some((ts0 as i64) * 1_000 + 2_000),
            unix_ts_ms: None,
            mono_ns: None,
            clock_sync: None,
            replay_order_ts_ms: ts0,
            timing_estimation: Some(TimingEstimate {
                method: "exchange_ts_us_to_ms_floor".to_string(),
                uncertainty_note: "test PMData".to_string(),
            }),
        };
        let mut t1 = est();
        t1.exchange_ts_ms = Some(ts0 + 500);
        t1.replay_order_ts_ms = ts0 + 500;
        let mut t2 = est();
        t2.exchange_ts_ms = Some(ts0 + 2_000);
        t2.replay_order_ts_ms = ts0 + 2_000;
        let mut t_end = est();
        t_end.exchange_ts_ms = Some(expiry);
        t_end.replay_order_ts_ms = expiry;

        let prov = |deriv: BookDerivation, orig: &str| EventProvenance {
            source: DataSource::Pmdata,
            original_event_type: orig.to_string(),
            market_slug: slug.to_string(),
            timing_quality: TimingQuality::ExchangeAndProviderUtc,
            book_derivation: deriv,
            normalization_version: CANONICAL_SCHEMA_VERSION,
            raw_price_f64: None,
            raw_size_f64: None,
            quality_flags: Vec::new(),
        };

        let events = vec![
            CanonicalEvent {
                schema_version: CANONICAL_SCHEMA_VERSION,
                provenance: prov(BookDerivation::NotApplicable, "market_metadata"),
                timestamp: est(),
                kind: CanonicalEventKind::MarketMetadata {
                    condition_id: Some("0xcid15m".to_string()),
                    slug: slug.to_string(),
                    question: Some("BTC 15m Test".to_string()),
                    interval: UpDownInterval::M15,
                    version: Some(core_types::ProtocolVersion::CtfV1),
                    neg_risk: Some(false),
                    asset_id_up: Some("up15m".to_string()),
                    asset_id_dn: Some("dn15m".to_string()),
                    yes_outcome_label: Some("Up".to_string()),
                    no_outcome_label: Some("Down".to_string()),
                    tick_size_micro: MicroUsdc(10_000),
                    min_order_size_micro: SharesMicro(5_000_000),
                    taker_delay_ms: Some(50),
                    fee_schedule: core_types::FeeSchedule::btc_5m_default(),
                    start_ts_ms: ts0,
                    expiry_ts_ms: expiry,
                    strike_price_usd: None,
                },
            },
            CanonicalEvent {
                schema_version: CANONICAL_SCHEMA_VERSION,
                provenance: prov(BookDerivation::DirectPmdataYes, "book"),
                timestamp: t1.clone(),
                kind: CanonicalEventKind::BookSnapshot {
                    outcome: Outcome::Up,
                    bids: vec![core_types::BookLevel {
                        price: MicroUsdc(480_000),
                        size: SharesMicro(25_000_000),
                    }],
                    asks: vec![core_types::BookLevel {
                        price: MicroUsdc(520_000),
                        size: SharesMicro(25_000_000),
                    }],
                },
            },
            CanonicalEvent {
                schema_version: CANONICAL_SCHEMA_VERSION,
                provenance: prov(BookDerivation::ComplementaryFromYesL2, "book"),
                timestamp: t1,
                kind: CanonicalEventKind::BookSnapshot {
                    outcome: Outcome::Down,
                    bids: vec![core_types::BookLevel {
                        price: MicroUsdc(480_000),
                        size: SharesMicro(25_000_000),
                    }],
                    asks: vec![core_types::BookLevel {
                        price: MicroUsdc(520_000),
                        size: SharesMicro(25_000_000),
                    }],
                },
            },
            CanonicalEvent {
                schema_version: CANONICAL_SCHEMA_VERSION,
                provenance: prov(BookDerivation::NotApplicable, "last_trade_price"),
                timestamp: t2,
                kind: CanonicalEventKind::TradePrint {
                    outcome: Outcome::Up,
                    side: OrderSide::Sell,
                    price: MicroUsdc(480_000),
                    size: SharesMicro(10_000_000),
                    trade_id: Some("tr-15m".to_string()),
                },
            },
            CanonicalEvent {
                schema_version: CANONICAL_SCHEMA_VERSION,
                provenance: prov(BookDerivation::NotApplicable, "market_resolved"),
                timestamp: t_end,
                kind: CanonicalEventKind::MarketResolution {
                    winning_outcome: Outcome::Down,
                    resolution_source: ResolutionSource::ExternalOverride,
                    raw_winning_label: Some("Down".to_string()),
                },
            },
        ];

        let dir = std::env::temp_dir();
        let in_path = dir.join(format!("paper_canon_15m_{}.jsonl", std::process::id()));
        let out_ok = dir.join(format!("paper_canon_15m_out_{}.jsonl", std::process::id()));
        let out_err = dir.join(format!("paper_canon_15m_err_{}.jsonl", std::process::id()));
        write_canonical_jsonl(&in_path, &events).unwrap();

        run(PaperArgs {
            source: TraceSourceMode::Replay,
            input: Some(in_path.clone()),
            duration: "900s".to_string(),
            allow_mid_window: true,
            v_max: 50,
            base_size: 20,
            latency_ms: 0,
            queue_decay: paper_sim::QueueDecayPolicy::Conservative,
            math: crate::MathOverrides::default(),
            source_filter: SourceFilter::PmdataOnly,
            min_book_quality: BookQualityFilter::Any,
            jsonl: out_ok.clone(),
        })
        .unwrap();
        let log_txt = std::fs::read_to_string(&out_ok).unwrap();
        assert!(
            log_txt.lines().any(|line| {
                serde_json::from_str::<serde_json::Value>(line).is_ok_and(|v| {
                    v["event"] == "summary"
                        && v["payload"]["total_ticks"].as_u64().unwrap_or(0) >= 1
                })
            }),
            "expected summary with total_ticks >= 1 in {log_txt}"
        );
        // Verify explicit MarketResolution (`Down`) was honored during epoch settlement.
        assert!(
            log_txt.contains("\"winning_outcome\":\"Down\""),
            "expected settlement with winning_outcome=Down in {log_txt}"
        );

        // Filtering for LiveOnly on pure PMData input fails cleanly.
        let err = run(PaperArgs {
            source: TraceSourceMode::Replay,
            input: Some(in_path.clone()),
            duration: "900s".to_string(),
            allow_mid_window: true,
            v_max: 50,
            base_size: 20,
            latency_ms: 0,
            queue_decay: paper_sim::QueueDecayPolicy::Conservative,
            math: crate::MathOverrides::default(),
            source_filter: SourceFilter::LiveOnly,
            min_book_quality: BookQualityFilter::Any,
            jsonl: out_err.clone(),
        })
        .unwrap_err();
        assert!(err.to_string().contains("filtered out"), "err={err}");

        let _ = std::fs::remove_file(&in_path);
        let _ = std::fs::remove_file(&out_ok);
        let _ = std::fs::remove_file(&out_err);
    }
}
