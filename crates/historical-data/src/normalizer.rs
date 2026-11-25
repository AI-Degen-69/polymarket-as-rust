use std::collections::HashSet;
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

static CANONICAL_TMP_SEQ: AtomicU64 = AtomicU64::new(1);

use core_types::{
    BookLevel, FeeSchedule, MicroUsdc, ONE_DOLLAR_MICRO, OrderSide, Outcome, SharesMicro,
    TickPrecision,
};
use feeds::{BinanceSpotEvent, BookMaintainer, ClockSyncReport, parse_order_side};
use serde::{Deserialize, Serialize};

use crate::canonical::{
    BookDerivation, CANONICAL_SCHEMA_VERSION, CanonicalEvent, CanonicalEventKind,
    CanonicalTimestamp, ClockSyncMeta, DataSource, EventProvenance, QualityFlag, ResolutionSource,
    TimingEstimate, TimingQuality,
};
use crate::catalog::{OutcomeMapping, infer_interval_from_slug, parse_slug_start_secs};
use crate::config::{IngestionMode, UpDownInterval};
use crate::error::HistoricalError;
use crate::parquet_reader::{
    CatalogEntry, RawPmdataL2Row, RawPmdataOnchainFillRow, RawPmdataTradeRow, read_l2_parquet,
    read_onchain_fills_parquet, read_trades_parquet,
};

const MICRO_SCALE_F64: f64 = 1_000_000.0;
const DEFAULT_TICK_MICRO: MicroUsdc = MicroUsdc(10_000);
const DEFAULT_MIN_ORDER_SIZE_MICRO: SharesMicro = SharesMicro(5_000_000);
// 2026-11-20T23:59:00Z = 1_795_219_140_000 ms (fee rate shifted from 0.07 to 0.05).
const NOV_20_2026_FEE_CUTOFF_MS: u64 = 1_795_219_140_000;

/// Input Parquet file paths for normalizing one PMData market.
#[derive(Debug, Clone)]
pub struct PmdataMarketFiles {
    pub market_slug: String,
    pub yes_l2_path: PathBuf,
    pub no_l2_path: Option<PathBuf>,
    pub trades_path: Option<PathBuf>,
    pub onchain_fills_path: Option<PathBuf>,
}

/// Summary counters emitted by `normalize_pmdata_market`.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct NormalizationSummary {
    pub market_slug: String,
    pub mode: IngestionMode,
    pub total_events: usize,
    pub book_snapshots: usize,
    pub price_changes: usize,
    pub trade_prints: usize,
    pub duplicate_trades_filtered: usize,
    pub missing_trade_id_count: usize,
    pub tick_size_changes: usize,
    pub onchain_fills: usize,
    pub has_explicit_resolution: bool,
    pub derived_down_events: usize,
    pub crossed_books_observed: usize,
    pub sub_tick_prices: usize,
    pub below_min_size_count: usize,
}

/// Complete normalized stream for one PMData market.
#[derive(Debug, Clone)]
pub struct NormalizedMarketStream {
    pub events: Vec<CanonicalEvent>,
    pub summary: NormalizationSummary,
}

/// Converts a floating-point PMData price in `[0, 1]` to `MicroUsdc` without silent NaN/bounds loss.
pub fn convert_price_to_micro(
    raw: f64,
    tick_micro: MicroUsdc,
    allow_endpoints: bool,
    context: &str,
) -> Result<(MicroUsdc, Vec<QualityFlag>), HistoricalError> {
    if !raw.is_finite() {
        return Err(HistoricalError::Normalization {
            context: context.to_string(),
            detail: format!("non-finite price `{raw}`"),
        });
    }
    if !(0.0..=1.0).contains(&raw) {
        return Err(HistoricalError::Normalization {
            context: context.to_string(),
            detail: format!("price `{raw}` outside [0.0, 1.0]"),
        });
    }
    let scaled = raw * MICRO_SCALE_F64;
    let rounded = scaled.round();
    let mut flags = Vec::new();
    if (scaled - rounded).abs() > 1e-4 {
        flags.push(QualityFlag::FloatConversionRounding);
    }
    if rounded < 0.0 || rounded > ONE_DOLLAR_MICRO as f64 {
        return Err(HistoricalError::Normalization {
            context: context.to_string(),
            detail: format!("rounded price `{rounded}` out of bounds"),
        });
    }
    let micro = rounded as u64;
    if !allow_endpoints && (micro == 0 || micro >= ONE_DOLLAR_MICRO) {
        return Err(HistoricalError::Normalization {
            context: context.to_string(),
            detail: format!("price micro `{micro}` must lie strictly inside (0, 1_000_000)"),
        });
    }
    if tick_micro.0 > 0
        && micro > 0
        && micro < ONE_DOLLAR_MICRO
        && !micro.is_multiple_of(tick_micro.0)
    {
        flags.push(QualityFlag::SubTickPrice);
    }
    Ok((MicroUsdc(micro), flags))
}

/// Converts a floating-point PMData share quantity to `SharesMicro`.
pub fn convert_size_to_micro(
    raw: f64,
    min_order_size_micro: SharesMicro,
    allow_zero: bool,
    context: &str,
) -> Result<(SharesMicro, Vec<QualityFlag>), HistoricalError> {
    if !raw.is_finite() || raw < 0.0 {
        return Err(HistoricalError::Normalization {
            context: context.to_string(),
            detail: format!("invalid share quantity `{raw}`"),
        });
    }
    let scaled = raw * MICRO_SCALE_F64;
    let rounded = scaled.round();
    const TWO_POW_64: f64 = 18_446_744_073_709_551_616.0;
    if !rounded.is_finite() || rounded < 0.0 || rounded >= TWO_POW_64 {
        return Err(HistoricalError::Normalization {
            context: context.to_string(),
            detail: format!("share quantity `{raw}` overflows u64"),
        });
    }
    let mut flags = Vec::new();
    if (scaled - rounded).abs() > 1e-4 {
        flags.push(QualityFlag::FloatConversionRounding);
    }
    let micro = rounded as u64;
    if !allow_zero && micro == 0 {
        return Err(HistoricalError::Normalization {
            context: context.to_string(),
            detail: "share quantity rounded to 0 where > 0 is required".to_string(),
        });
    }
    if micro > 0 && micro < min_order_size_micro.0 {
        flags.push(QualityFlag::BelowMinOrderSize);
    }
    Ok((SharesMicro(micro), flags))
}

/// Computes the historical Polymarket fee schedule applicable at `start_ts_ms`.
#[must_use]
pub const fn historical_fee_schedule(start_ts_ms: u64) -> FeeSchedule {
    if start_ts_ms < NOV_20_2026_FEE_CUTOFF_MS {
        FeeSchedule::btc_5m_legacy()
    } else {
        FeeSchedule::btc_5m_default()
    }
}

/// Normalizes a PMData market's Parquet files (`l2` Yes, optional `l2` No, optional `trades`,
/// optional `onchain_fills`) into a causally ordered canonical stream preserving full provenance.
///
/// # Errors
/// - `HistoricalError::Normalization` if `IngestionMode::Full` is selected without `no_l2_path`,
///   if catalog outcome metadata (`yes_outcome`/`no_outcome`) is missing or ambiguous, if any
///   Parquet row `market_slug` or `catalog_meta.market_slug` disagrees with `files.market_slug`,
///   or if `resolution_override` conflicts with an observed PMData `market_resolved` outcome.
/// - `HistoricalError::Io` / `InvalidParquet` / `SchemaMismatch` if any input file is missing or malformed.
pub fn normalize_pmdata_market(
    files: &PmdataMarketFiles,
    mode: IngestionMode,
    catalog_meta: Option<&CatalogEntry>,
    resolution_override: Option<Outcome>,
) -> Result<NormalizedMarketStream, HistoricalError> {
    let slug = files.market_slug.trim();
    if slug.is_empty() {
        return Err(HistoricalError::Normalization {
            context: "pmdata".to_string(),
            detail: "market_slug must be non-empty".to_string(),
        });
    }
    if let Some(meta) = catalog_meta
        && meta.market_slug.trim() != slug
    {
        return Err(HistoricalError::Normalization {
            context: slug.to_string(),
            detail: format!(
                "catalog entry slug `{}` does not match requested market `{slug}`",
                meta.market_slug
            ),
        });
    }
    if mode == IngestionMode::Full && files.no_l2_path.is_none() {
        return Err(HistoricalError::Normalization {
            context: slug.to_string(),
            detail: "IngestionMode::Full requires no_l2_path (`?outcome=No`); use QuotaSaving mode to derive complementary Down book from Yes L2".to_string(),
        });
    }

    let mapping = OutcomeMapping::from_catalog(catalog_meta, slug)?;
    let interval =
        infer_interval_from_slug(slug).ok_or_else(|| HistoricalError::Normalization {
            context: slug.to_string(),
            detail: "cannot infer UpDown interval (5m/15m/1h) from market_slug".to_string(),
        })?;
    let (start_ts_ms, expiry_ts_ms) = resolve_market_window(slug, interval, catalog_meta)?;
    let fee_schedule = historical_fee_schedule(start_ts_ms);

    let yes_rows = read_l2_parquet(&files.yes_l2_path)?;
    if yes_rows.is_empty() {
        return Err(HistoricalError::Normalization {
            context: slug.to_string(),
            detail: format!(
                "Yes L2 file `{}` contains 0 rows",
                files.yes_l2_path.display()
            ),
        });
    }
    validate_slug_in_rows(
        yes_rows.iter().map(|r| r.market_slug.as_str()),
        slug,
        &files.yes_l2_path,
    )?;

    let no_rows = match (&files.no_l2_path, mode) {
        (Some(p), IngestionMode::Full) => {
            let rows = read_l2_parquet(p)?;
            validate_slug_in_rows(rows.iter().map(|r| r.market_slug.as_str()), slug, p)?;
            Some(rows)
        }
        _ => None,
    };
    let trade_rows = match &files.trades_path {
        Some(p) => {
            let rows = read_trades_parquet(p)?;
            validate_slug_in_rows(rows.iter().map(|r| r.market_slug.as_str()), slug, p)?;
            rows
        }
        None => Vec::new(),
    };
    let fill_rows = match &files.onchain_fills_path {
        Some(p) => read_onchain_fills_parquet(p)?,
        None => Vec::new(),
    };

    let mut summary = NormalizationSummary {
        market_slug: slug.to_string(),
        mode,
        ..Default::default()
    };

    let mut tick_micro = DEFAULT_TICK_MICRO;
    let mut tick_timeline: Vec<(i64, MicroUsdc)> = Vec::new();
    let min_size = DEFAULT_MIN_ORDER_SIZE_MICRO;

    // 1. Build L2 canonical stream preserving exact file order for Yes and (when present) No.
    let mut l2_stream: Vec<TimedEvent> = Vec::with_capacity(yes_rows.len() * 2);
    let mut seen_market_resolved = false;
    let mut resolved_outcome: Option<Outcome> = None;
    let mut yes_book_tracker = BookMaintainer::new(mapping.yes_maps_to);
    let mut yes_book_crossed = false;
    let mut down_derivation_invalidated = false;
    let mut yes_mono_ts_us: Option<i64> = None;

    for row in &yes_rows {
        let prev_tick = tick_micro;
        let mut produced = convert_l2_row(
            row,
            mapping.yes_maps_to,
            BookDerivation::DirectPmdataYes,
            &mut tick_micro,
            min_size,
            mode == IngestionMode::QuotaSaving,
            mapping.no_maps_to,
            mapping,
            Some((
                &mut yes_book_tracker,
                &mut yes_book_crossed,
                &mut down_derivation_invalidated,
            )),
            &mut summary,
        )?;
        if tick_micro != prev_tick {
            tick_timeline.push((row.timestamp_us, tick_micro));
        }
        let clamped_ts = yes_mono_ts_us.map_or(row.timestamp_us, |p| p.max(row.timestamp_us));
        yes_mono_ts_us = Some(clamped_ts);
        for item in &mut produced {
            item.sort_ts_us = clamped_ts;
            if let CanonicalEventKind::MarketResolution {
                winning_outcome, ..
            } = item.event.kind
            {
                if let Some(prev) = resolved_outcome
                    && prev != winning_outcome
                {
                    return Err(HistoricalError::Normalization {
                        context: slug.to_string(),
                        detail: format!(
                            "conflicting market_resolved outcomes in Yes L2: {prev:?} vs {winning_outcome:?}"
                        ),
                    });
                }
                resolved_outcome = Some(winning_outcome);
                seen_market_resolved = true;
                summary.has_explicit_resolution = true;
            }
        }
        l2_stream.append(&mut produced);
    }

    if let Some(no_list) = &no_rows {
        let mut no_tick = DEFAULT_TICK_MICRO;
        let mut no_mono_ts_us: Option<i64> = None;
        for row in no_list {
            // Market-level events (`tick_size_change`, `market_resolved`) already come from Yes L2;
            // keep only book & price_change rows from No L2 to avoid duplicate market-level events,
            // EXCEPT if Yes lacked `market_resolved` and No carries it. Still update `no_tick` and
            // validate `market_resolved` consistency across both streams.
            let et = row.event_type.trim();
            if et == "tick_size_change" {
                let _ = convert_l2_row(
                    row,
                    mapping.no_maps_to,
                    BookDerivation::DirectPmdataNo,
                    &mut no_tick,
                    min_size,
                    false,
                    mapping.yes_maps_to,
                    mapping,
                    None,
                    &mut summary,
                )?;
                continue;
            }
            let mut produced = convert_l2_row(
                row,
                mapping.no_maps_to,
                BookDerivation::DirectPmdataNo,
                &mut no_tick,
                min_size,
                false,
                mapping.yes_maps_to,
                mapping,
                None,
                &mut summary,
            )?;
            let clamped_ts = no_mono_ts_us.map_or(row.timestamp_us, |p| p.max(row.timestamp_us));
            no_mono_ts_us = Some(clamped_ts);
            for item in &mut produced {
                item.sort_ts_us = clamped_ts;
            }
            if et == "market_resolved" {
                for item in &produced {
                    if let CanonicalEventKind::MarketResolution {
                        winning_outcome, ..
                    } = item.event.kind
                    {
                        if let Some(prev) = resolved_outcome
                            && prev != winning_outcome
                        {
                            return Err(HistoricalError::Normalization {
                                context: slug.to_string(),
                                detail: format!(
                                    "conflicting market_resolved outcomes between Yes ({prev:?}) and No ({winning_outcome:?}) L2"
                                ),
                            });
                        }
                        resolved_outcome = Some(winning_outcome);
                    }
                }
                if seen_market_resolved {
                    continue;
                }
                seen_market_resolved = true;
                summary.has_explicit_resolution = true;
            }
            l2_stream.append(&mut produced);
        }
    }

    // Causal interleave of Yes + No L2 streams when Full mode provides both files.
    // `sort_ts_us` is clamped monotonically per file so intra-file record order never inverts.
    if no_rows.is_some() {
        l2_stream.sort_by_key(|t| (t.sort_ts_us, t.sort_local_us, t.stream_rank));
    }

    // 2. Build TradePrint stream with explicit deduplication and missing `trade_id` flagging.
    let mut trades_stream: Vec<TimedEvent> = Vec::with_capacity(trade_rows.len());
    let mut seen_trades: HashSet<TradeDedupKey> = HashSet::new();
    for (idx, row) in trade_rows.iter().enumerate() {
        let active_tick = tick_in_force_at(&tick_timeline, row.timestamp_us);
        if let Some(ev) = convert_trade_row(
            row,
            idx,
            &mapping,
            active_tick,
            min_size,
            &mut seen_trades,
            &mut summary,
        )? {
            trades_stream.push(ev);
        }
    }
    trades_stream.sort_by_key(|t| (t.sort_ts_us, t.sort_local_us, t.stream_rank));

    // 3. Build optional OnchainFills stream (kept strictly distinct from TradePrint).
    let mut onchain_stream: Vec<TimedEvent> = Vec::with_capacity(fill_rows.len());
    for (idx, row) in fill_rows.iter().enumerate() {
        let active_tick = tick_in_force_at(&tick_timeline, row.timestamp_us);
        onchain_stream.push(convert_onchain_row(
            row,
            idx,
            slug,
            &mapping,
            active_tick,
            min_size,
        )?);
    }
    onchain_stream.sort_by_key(|t| (t.sort_ts_us, t.sort_local_us, t.stream_rank));
    summary.onchain_fills = onchain_stream.len();

    // 4. Merge streams causally without look-ahead bias (events at <= timestamp never jump ahead).
    let mut merged = merge_causal_streams(l2_stream, trades_stream);
    if !onchain_stream.is_empty() {
        merged = merge_causal_streams(merged, onchain_stream);
    }

    // 5. Emit MarketMetadata first, then causal events with non-decreasing `replay_order_ts_ms`.
    let mut events = Vec::with_capacity(merged.len() + 2);
    let mut meta_flags = vec![
        QualityFlag::MissingBinanceSpot,
        QualityFlag::EstimatedReplayClock,
    ];
    if !seen_market_resolved {
        meta_flags.push(QualityFlag::MissingMarketResolved);
    }
    if mode == IngestionMode::QuotaSaving {
        meta_flags.push(QualityFlag::DerivedComplementaryDownBook);
    }

    events.push(CanonicalEvent {
        schema_version: CANONICAL_SCHEMA_VERSION,
        provenance: EventProvenance {
            source: DataSource::Pmdata,
            original_event_type: "catalog_metadata".to_string(),
            market_slug: slug.to_string(),
            timing_quality: TimingQuality::ExchangeAndProviderUtc,
            book_derivation: BookDerivation::NotApplicable,
            normalization_version: CANONICAL_SCHEMA_VERSION,
            raw_price_f64: None,
            raw_size_f64: None,
            quality_flags: meta_flags,
        },
        timestamp: CanonicalTimestamp {
            exchange_ts_ms: Some(start_ts_ms),
            exchange_ts_us: Some((start_ts_ms as i64).saturating_mul(1_000)),
            provider_local_ts_us: None,
            unix_ts_ms: None,
            mono_ns: None,
            clock_sync: None,
            replay_order_ts_ms: start_ts_ms,
            timing_estimation: Some(TimingEstimate {
                method: "market_slug_epoch_start".to_string(),
                uncertainty_note: "PMData lacks local mono_ns and clock_sync; start_ts_ms derived from slug/catalog".to_string(),
            }),
        },
        kind: CanonicalEventKind::MarketMetadata {
            condition_id: None,
            slug: slug.to_string(),
            question: catalog_meta.and_then(|c| c.question.clone()),
            interval,
            version: None,
            neg_risk: None,
            asset_id_up: None,
            asset_id_dn: None,
            yes_outcome_label: catalog_meta.and_then(|c| c.yes_outcome.clone()),
            no_outcome_label: catalog_meta.and_then(|c| c.no_outcome.clone()),
            tick_size_micro: DEFAULT_TICK_MICRO,
            min_order_size_micro: min_size,
            taker_delay_ms: None,
            fee_schedule,
            start_ts_ms,
            expiry_ts_ms,
            strike_price_usd: None,
        },
    });

    let mut monotonic_replay_ms = start_ts_ms;
    for mut item in merged {
        if item.event.timestamp.replay_order_ts_ms < monotonic_replay_ms {
            item.event.timestamp.replay_order_ts_ms = monotonic_replay_ms;
        } else {
            monotonic_replay_ms = item.event.timestamp.replay_order_ts_ms;
        }
        item.event.validate_provenance_invariants()?;
        events.push(item.event);
    }

    if let Some(winning_outcome) = resolution_override {
        if let Some(observed) = resolved_outcome {
            if observed != winning_outcome {
                return Err(HistoricalError::Normalization {
                    context: slug.to_string(),
                    detail: format!(
                        "explicit resolution_override ({winning_outcome:?}) conflicts with observed PMData market_resolved ({observed:?})"
                    ),
                });
            }
        } else if !seen_market_resolved {
            summary.has_explicit_resolution = true;
            events.push(CanonicalEvent {
            schema_version: CANONICAL_SCHEMA_VERSION,
            provenance: EventProvenance {
                source: DataSource::Pmdata,
                original_event_type: "external_resolution_override".to_string(),
                market_slug: slug.to_string(),
                timing_quality: TimingQuality::ExchangeOnlyUtc,
                book_derivation: BookDerivation::NotApplicable,
                normalization_version: CANONICAL_SCHEMA_VERSION,
                raw_price_f64: None,
                raw_size_f64: None,
                quality_flags: vec![QualityFlag::MissingMarketResolved, QualityFlag::EstimatedReplayClock],
            },
            timestamp: CanonicalTimestamp {
                exchange_ts_ms: Some(expiry_ts_ms),
                exchange_ts_us: Some((expiry_ts_ms as i64).saturating_mul(1_000)),
                provider_local_ts_us: None,
                unix_ts_ms: None,
                mono_ns: None,
                clock_sync: None,
                replay_order_ts_ms: monotonic_replay_ms.max(expiry_ts_ms),
                timing_estimation: Some(TimingEstimate {
                    method: "external_resolution_at_expiry".to_string(),
                    uncertainty_note: "PMData market_resolved event absent; applied explicit verified resolution override at market expiry".to_string(),
                }),
            },
            kind: CanonicalEventKind::MarketResolution {
                winning_outcome,
                resolution_source: ResolutionSource::ExternalOverride,
                raw_winning_label: None,
            },
        });
        }
    }

    summary.total_events = events.len();
    Ok(NormalizedMarketStream { events, summary })
}

/// Normalizes an existing live JSONL capture file into `CanonicalEvent` records while
/// preserving original `mono_ns`, `ts_unix_ms`, `clock_sync`, and `BinanceSpot` ticks untouched.
pub fn normalize_live_jsonl(path: &Path) -> Result<Vec<CanonicalEvent>, HistoricalError> {
    #[derive(Deserialize)]
    struct RawEnv {
        ts_unix_ms: u64,
        mono_ns: u64,
        event: String,
        payload: serde_json::Value,
    }
    #[derive(Deserialize)]
    struct ReplayBook {
        #[serde(rename = "outcome")]
        _outcome: Outcome,
        bids: Vec<BookLevel>,
        asks: Vec<BookLevel>,
        #[allow(dead_code)]
        last_update_ns: u64,
        #[allow(dead_code)]
        exchange_ts_ms: u64,
    }
    #[derive(Deserialize)]
    enum ReplayPoly {
        BookSnapshot {
            outcome: Outcome,
            book: ReplayBook,
            rx_mono_ns: u64,
            exch_ts_ms: u64,
        },
        PriceChange {
            outcome: Outcome,
            side: OrderSide,
            price: MicroUsdc,
            size: SharesMicro,
            best_bid: Option<MicroUsdc>,
            best_ask: Option<MicroUsdc>,
            rx_mono_ns: u64,
            exch_ts_ms: u64,
        },
        LastTrade {
            outcome: Outcome,
            side: OrderSide,
            price: MicroUsdc,
            size: SharesMicro,
            rx_mono_ns: u64,
            exch_ts_ms: u64,
        },
        TickSizeChange {
            outcome: Outcome,
            new_tick_micro: MicroUsdc,
            rx_mono_ns: u64,
            exch_ts_ms: u64,
        },
        BestBidAsk {
            outcome: Outcome,
            best_bid: Option<MicroUsdc>,
            best_ask: Option<MicroUsdc>,
            rx_mono_ns: u64,
            exch_ts_ms: u64,
        },
    }

    let file = File::open(path).map_err(|source| HistoricalError::Io {
        context: format!("open live jsonl `{}`", path.display()),
        source,
    })?;
    let reader = BufReader::new(file);
    let mut out = Vec::new();
    let mut current_slug = "unknown".to_string();
    let mut timing_quality = TimingQuality::LiveMonotonicUnsynced;
    let mut anchor: Option<(u64, u64)> = None;
    let mut clock_sync_meta: Option<ClockSyncMeta> = None;

    for (line_idx, line_res) in reader.lines().enumerate() {
        let line = line_res.map_err(|source| HistoricalError::Io {
            context: format!(
                "read live jsonl line {} of `{}`",
                line_idx + 1,
                path.display()
            ),
            source,
        })?;
        if line.trim().is_empty() {
            continue;
        }
        let env: RawEnv = serde_json::from_str(&line).map_err(|source| HistoricalError::Json {
            context: format!("line {} of `{}`", line_idx + 1, path.display()),
            source,
        })?;
        let (anchor_ms, anchor_ns) = *anchor.get_or_insert((env.ts_unix_ms, env.mono_ns));
        let replay_order_ts_ms =
            anchor_ms.saturating_add(env.mono_ns.saturating_sub(anchor_ns) / 1_000_000);

        match env.event.as_str() {
            "market_discovered" => {
                let desc: core_types::MarketDescriptor = serde_json::from_value(env.payload)
                    .map_err(|source| HistoricalError::Json {
                        context: "market_discovered".to_string(),
                        source,
                    })?;
                current_slug = desc.slug.clone();
                let interval = infer_interval_from_slug(&desc.slug).unwrap_or(UpDownInterval::M5);
                out.push(CanonicalEvent {
                    schema_version: CANONICAL_SCHEMA_VERSION,
                    provenance: EventProvenance {
                        source: DataSource::LiveJsonl,
                        original_event_type: "market_discovered".to_string(),
                        market_slug: current_slug.clone(),
                        timing_quality,
                        book_derivation: BookDerivation::DirectLiveWs,
                        normalization_version: CANONICAL_SCHEMA_VERSION,
                        raw_price_f64: None,
                        raw_size_f64: None,
                        quality_flags: Vec::new(),
                    },
                    timestamp: CanonicalTimestamp {
                        exchange_ts_ms: Some(desc.start_ts_ms),
                        exchange_ts_us: None,
                        provider_local_ts_us: None,
                        unix_ts_ms: Some(env.ts_unix_ms),
                        mono_ns: Some(env.mono_ns),
                        clock_sync: clock_sync_meta.clone(),
                        replay_order_ts_ms,
                        timing_estimation: None,
                    },
                    kind: CanonicalEventKind::MarketMetadata {
                        condition_id: Some(desc.condition_id),
                        slug: desc.slug,
                        question: Some(desc.question),
                        interval,
                        version: Some(desc.version),
                        neg_risk: Some(desc.neg_risk),
                        asset_id_up: Some(desc.asset_id_up),
                        asset_id_dn: Some(desc.asset_id_dn),
                        yes_outcome_label: Some("Up".to_string()),
                        no_outcome_label: Some("Down".to_string()),
                        tick_size_micro: desc.tick_size_micro,
                        min_order_size_micro: desc.min_order_size_micro,
                        taker_delay_ms: Some(desc.taker_delay_ms),
                        fee_schedule: desc.fee_schedule,
                        start_ts_ms: desc.start_ts_ms,
                        expiry_ts_ms: desc.expiry_ts_ms,
                        strike_price_usd: desc.strike_price_usd.map(|p| p.get()),
                    },
                });
            }
            "clock_sync" => {
                let rep: ClockSyncReport =
                    serde_json::from_value(env.payload).map_err(|source| {
                        HistoricalError::Json {
                            context: "clock_sync".to_string(),
                            source,
                        }
                    })?;
                timing_quality = if rep.drift_exceeded {
                    TimingQuality::LiveMonotonicUnsynced
                } else {
                    TimingQuality::LiveMonotonicSynced
                };
                let meta = ClockSyncMeta {
                    source: rep.source,
                    offset_ms: rep.offset_ms,
                    clob_offset_ms: rep.clob_offset_ms,
                    binance_offset_ms: rep.binance_offset_ms,
                    drift_exceeded: rep.drift_exceeded,
                };
                clock_sync_meta = Some(meta.clone());
                out.push(CanonicalEvent {
                    schema_version: CANONICAL_SCHEMA_VERSION,
                    provenance: EventProvenance {
                        source: DataSource::LiveJsonl,
                        original_event_type: "clock_sync".to_string(),
                        market_slug: current_slug.clone(),
                        timing_quality,
                        book_derivation: BookDerivation::NotApplicable,
                        normalization_version: CANONICAL_SCHEMA_VERSION,
                        raw_price_f64: None,
                        raw_size_f64: None,
                        quality_flags: Vec::new(),
                    },
                    timestamp: CanonicalTimestamp {
                        exchange_ts_ms: Some(rep.exchange_unix_ms),
                        exchange_ts_us: None,
                        provider_local_ts_us: None,
                        unix_ts_ms: Some(env.ts_unix_ms),
                        mono_ns: Some(env.mono_ns),
                        clock_sync: Some(meta.clone()),
                        replay_order_ts_ms,
                        timing_estimation: None,
                    },
                    kind: CanonicalEventKind::ClockSync { report: meta },
                });
            }
            "binance_tick" => {
                let b_ev: BinanceSpotEvent =
                    serde_json::from_value(env.payload).map_err(|source| {
                        HistoricalError::Json {
                            context: "binance_tick".to_string(),
                            source,
                        }
                    })?;
                let (exch_ms, rx_ns) = match b_ev {
                    BinanceSpotEvent::BookTicker { rx_mono_ns, .. } => (None, rx_mono_ns),
                    BinanceSpotEvent::AggTrade {
                        exch_ts_ms,
                        rx_mono_ns,
                        ..
                    } => (Some(exch_ts_ms), rx_mono_ns),
                };
                out.push(CanonicalEvent {
                    schema_version: CANONICAL_SCHEMA_VERSION,
                    provenance: EventProvenance {
                        source: DataSource::LiveJsonl,
                        original_event_type: "binance_tick".to_string(),
                        market_slug: current_slug.clone(),
                        timing_quality,
                        book_derivation: BookDerivation::NotApplicable,
                        normalization_version: CANONICAL_SCHEMA_VERSION,
                        raw_price_f64: None,
                        raw_size_f64: None,
                        quality_flags: Vec::new(),
                    },
                    timestamp: CanonicalTimestamp {
                        exchange_ts_ms: exch_ms,
                        exchange_ts_us: None,
                        provider_local_ts_us: None,
                        unix_ts_ms: Some(env.ts_unix_ms),
                        mono_ns: Some(if rx_ns > 0 { rx_ns } else { env.mono_ns }),
                        clock_sync: clock_sync_meta.clone(),
                        replay_order_ts_ms,
                        timing_estimation: None,
                    },
                    kind: CanonicalEventKind::BinanceSpot { event: b_ev },
                });
            }
            "poly_book" | "poly_price_change" | "poly_trade" | "poly_tick_size" | "poly_bba" => {
                let p_ev: ReplayPoly = serde_json::from_value(env.payload).map_err(|source| {
                    HistoricalError::Json {
                        context: env.event.clone(),
                        source,
                    }
                })?;
                let (rx_ns, exch_ms, kind, deriv) = match p_ev {
                    ReplayPoly::BookSnapshot {
                        outcome,
                        book,
                        rx_mono_ns,
                        exch_ts_ms,
                    } => (
                        rx_mono_ns,
                        exch_ts_ms,
                        CanonicalEventKind::BookSnapshot {
                            outcome,
                            bids: book.bids,
                            asks: book.asks,
                        },
                        BookDerivation::DirectLiveWs,
                    ),
                    ReplayPoly::PriceChange {
                        outcome,
                        side,
                        price,
                        size,
                        best_bid,
                        best_ask,
                        rx_mono_ns,
                        exch_ts_ms,
                    } => (
                        rx_mono_ns,
                        exch_ts_ms,
                        CanonicalEventKind::PriceChange {
                            outcome,
                            side,
                            price,
                            size,
                            best_bid,
                            best_ask,
                        },
                        BookDerivation::DirectLiveWs,
                    ),
                    ReplayPoly::LastTrade {
                        outcome,
                        side,
                        price,
                        size,
                        rx_mono_ns,
                        exch_ts_ms,
                    } => (
                        rx_mono_ns,
                        exch_ts_ms,
                        CanonicalEventKind::TradePrint {
                            outcome,
                            side,
                            price,
                            size,
                            trade_id: None,
                        },
                        BookDerivation::NotApplicable,
                    ),
                    ReplayPoly::TickSizeChange {
                        outcome,
                        new_tick_micro,
                        rx_mono_ns,
                        exch_ts_ms,
                    } => (
                        rx_mono_ns,
                        exch_ts_ms,
                        CanonicalEventKind::TickSizeChange {
                            outcome: Some(outcome),
                            new_tick_micro,
                        },
                        BookDerivation::NotApplicable,
                    ),
                    ReplayPoly::BestBidAsk {
                        outcome,
                        best_bid,
                        best_ask,
                        rx_mono_ns,
                        exch_ts_ms,
                    } => (
                        rx_mono_ns,
                        exch_ts_ms,
                        CanonicalEventKind::BestBidAsk {
                            outcome,
                            best_bid,
                            best_ask,
                        },
                        BookDerivation::DirectLiveWs,
                    ),
                };
                out.push(CanonicalEvent {
                    schema_version: CANONICAL_SCHEMA_VERSION,
                    provenance: EventProvenance {
                        source: DataSource::LiveJsonl,
                        original_event_type: env.event,
                        market_slug: current_slug.clone(),
                        timing_quality,
                        book_derivation: deriv,
                        normalization_version: CANONICAL_SCHEMA_VERSION,
                        raw_price_f64: None,
                        raw_size_f64: None,
                        quality_flags: Vec::new(),
                    },
                    timestamp: CanonicalTimestamp {
                        exchange_ts_ms: Some(exch_ms),
                        exchange_ts_us: None,
                        provider_local_ts_us: None,
                        unix_ts_ms: Some(env.ts_unix_ms),
                        mono_ns: Some(rx_ns),
                        clock_sync: clock_sync_meta.clone(),
                        replay_order_ts_ms,
                        timing_estimation: None,
                    },
                    kind,
                });
            }
            "feed_health" | "feeds_summary" | "polybolt_event" => {}
            other => {
                return Err(HistoricalError::Normalization {
                    context: path.display().to_string(),
                    detail: format!(
                        "unsupported live JSONL event `{other}` at line {}",
                        line_idx + 1
                    ),
                });
            }
        }
    }
    Ok(out)
}

struct TempFileCleanupGuard {
    path: PathBuf,
    armed: bool,
}

impl TempFileCleanupGuard {
    fn new(path: PathBuf) -> Self {
        Self { path, armed: true }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for TempFileCleanupGuard {
    fn drop(&mut self) {
        if self.armed {
            let _ = std::fs::remove_file(&self.path);
        }
    }
}

/// Writes canonical events as newline-delimited JSON atomically.
pub fn write_canonical_jsonl(
    path: &Path,
    events: &[CanonicalEvent],
) -> Result<(), HistoricalError> {
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        std::fs::create_dir_all(parent).map_err(|source| HistoricalError::Io {
            context: format!("create canonical output dir `{}`", parent.display()),
            source,
        })?;
    }
    let base_name = path
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("canonical.jsonl");
    let pid = std::process::id();
    let (tmp_path, file) = loop {
        let seq = CANONICAL_TMP_SEQ.fetch_add(1, Ordering::Relaxed);
        let candidate = path.with_file_name(format!("{base_name}.tmp.{pid}.{seq}"));
        match OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&candidate)
        {
            Ok(f) => break (candidate, f),
            Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(source) => {
                return Err(HistoricalError::Io {
                    context: format!("create temp canonical jsonl `{}`", candidate.display()),
                    source,
                });
            }
        }
    };
    let mut cleanup = TempFileCleanupGuard::new(tmp_path.clone());
    {
        let mut writer = BufWriter::with_capacity(64 * 1024, file);
        for ev in events {
            serde_json::to_writer(&mut writer, ev).map_err(|source| HistoricalError::Json {
                context: format!("serialize canonical event `{}`", path.display()),
                source,
            })?;
            writer
                .write_all(b"\n")
                .map_err(|source| HistoricalError::Io {
                    context: format!("write newline `{}`", tmp_path.display()),
                    source,
                })?;
        }
        writer.flush().map_err(|source| HistoricalError::Io {
            context: format!("flush canonical jsonl `{}`", tmp_path.display()),
            source,
        })?;
        writer
            .get_ref()
            .sync_all()
            .map_err(|source| HistoricalError::Io {
                context: format!("sync canonical jsonl `{}`", tmp_path.display()),
                source,
            })?;
    }
    std::fs::rename(&tmp_path, path).map_err(|source| HistoricalError::Io {
        context: format!("rename canonical jsonl to `{}`", path.display()),
        source,
    })?;
    cleanup.disarm();
    Ok(())
}

/// Reads a canonical JSONL file back into memory.
pub fn read_canonical_jsonl(path: &Path) -> Result<Vec<CanonicalEvent>, HistoricalError> {
    let file = File::open(path).map_err(|source| HistoricalError::Io {
        context: format!("open canonical jsonl `{}`", path.display()),
        source,
    })?;
    let reader = BufReader::new(file);
    let mut events = Vec::new();
    for (idx, line_res) in reader.lines().enumerate() {
        let line = line_res.map_err(|source| HistoricalError::Io {
            context: format!(
                "read canonical jsonl line {} of `{}`",
                idx + 1,
                path.display()
            ),
            source,
        })?;
        if line.trim().is_empty() {
            continue;
        }
        let ev: CanonicalEvent =
            serde_json::from_str(&line).map_err(|source| HistoricalError::Json {
                context: format!("canonical line {} of `{}`", idx + 1, path.display()),
                source,
            })?;
        if ev.schema_version != CANONICAL_SCHEMA_VERSION {
            return Err(HistoricalError::Normalization {
                context: path.display().to_string(),
                detail: format!(
                    "unsupported canonical schema_version {} at line {} (expected {CANONICAL_SCHEMA_VERSION})",
                    ev.schema_version,
                    idx + 1
                ),
            });
        }
        ev.validate_provenance_invariants()?;
        events.push(ev);
    }
    Ok(events)
}

struct TimedEvent {
    sort_ts_us: i64,
    sort_local_us: i64,
    stream_rank: u8,
    event: CanonicalEvent,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct TradeDedupKey {
    timestamp_us: i64,
    local_timestamp_us: Option<i64>,
    outcome: Outcome,
    side: OrderSide,
    price_micro: u64,
    size_micro: u64,
    trade_id: Option<String>,
}

fn validate_slug_in_rows<'a, I>(
    slugs: I,
    expected_slug: &str,
    path: &Path,
) -> Result<(), HistoricalError>
where
    I: Iterator<Item = &'a str>,
{
    for raw in slugs {
        let trimmed = raw.trim();
        if trimmed != expected_slug {
            return Err(HistoricalError::Normalization {
                context: expected_slug.to_string(),
                detail: format!(
                    "source `{}` contains row for market `{trimmed}` (expected `{expected_slug}`)",
                    path.display()
                ),
            });
        }
    }
    Ok(())
}

fn tick_in_force_at(timeline: &[(i64, MicroUsdc)], ts_us: i64) -> MicroUsdc {
    let mut current = DEFAULT_TICK_MICRO;
    for &(change_ts, tick) in timeline {
        if change_ts <= ts_us {
            current = tick;
        } else {
            break;
        }
    }
    current
}

fn resolve_market_window(
    slug: &str,
    interval: UpDownInterval,
    catalog_meta: Option<&CatalogEntry>,
) -> Result<(u64, u64), HistoricalError> {
    if let Some(start_s) = parse_slug_start_secs(slug) {
        let start_ms = start_s.saturating_mul(1_000);
        let expiry_ms = start_ms.saturating_add(interval.duration_ms());
        return Ok((start_ms, expiry_ms));
    }
    if let Some(meta) = catalog_meta
        && let (Some(s), Some(e)) = (meta.market_start_ms, meta.market_end_ms)
        && s > 0
        && e > s
    {
        return Ok((s as u64, e as u64));
    }
    Err(HistoricalError::Normalization {
        context: slug.to_string(),
        detail: "cannot determine start_ts_ms and expiry_ts_ms from slug or catalog".to_string(),
    })
}

fn make_pmdata_timestamp(
    timestamp_us: i64,
    local_timestamp_us: i64,
    is_block_only: bool,
) -> Result<(CanonicalTimestamp, TimingQuality, Vec<QualityFlag>), HistoricalError> {
    if timestamp_us < 0 {
        return Err(HistoricalError::Normalization {
            context: "pmdata_timestamp".to_string(),
            detail: format!("negative PMData exchange timestamp_us `{timestamp_us}`"),
        });
    }
    let exch_ms = (timestamp_us as u64) / 1_000;
    let mut flags = vec![QualityFlag::EstimatedReplayClock];
    let is_boundary = timestamp_us == local_timestamp_us && (timestamp_us % 86_400_000_000 == 0);
    if is_boundary {
        flags.push(QualityFlag::DailyBoundarySnapshot);
    }
    let timing_quality = if is_block_only {
        TimingQuality::BlockTimestampOnly
    } else if local_timestamp_us > 0 {
        TimingQuality::ExchangeAndProviderUtc
    } else {
        TimingQuality::ExchangeOnlyUtc
    };
    let ts = CanonicalTimestamp {
        exchange_ts_ms: Some(exch_ms),
        exchange_ts_us: Some(timestamp_us),
        provider_local_ts_us: (!is_block_only && local_timestamp_us > 0)
            .then_some(local_timestamp_us),
        unix_ts_ms: None,
        mono_ns: None,
        clock_sync: None,
        replay_order_ts_ms: exch_ms,
        timing_estimation: Some(TimingEstimate {
            method: "exchange_ts_us_to_ms_floor".to_string(),
            uncertainty_note: "PMData has no local mono_ns or clock_sync; replay_order_ts_ms floored from exchange timestamp[us]".to_string(),
        }),
    };
    Ok((ts, timing_quality, flags))
}

#[allow(clippy::too_many_arguments)]
fn convert_l2_row(
    row: &RawPmdataL2Row,
    outcome: Outcome,
    derivation: BookDerivation,
    tick_micro: &mut MicroUsdc,
    min_size: SharesMicro,
    derive_complementary: bool,
    opposite_outcome: Outcome,
    mapping: OutcomeMapping,
    mut book_tracker: Option<(&mut BookMaintainer, &mut bool, &mut bool)>,
    summary: &mut NormalizationSummary,
) -> Result<Vec<TimedEvent>, HistoricalError> {
    let (ts, timing_quality, mut base_flags) =
        make_pmdata_timestamp(row.timestamp_us, row.local_timestamp_us, false)?;
    let slug = row.market_slug.clone();
    let et = row.event_type.trim();
    let stream_rank = match derivation {
        BookDerivation::DirectPmdataYes => 0,
        BookDerivation::DirectPmdataNo => 1,
        _ => 2,
    };

    match et {
        "book" => {
            if row.bid_prices.len() != row.bid_sizes.len()
                || row.ask_prices.len() != row.ask_sizes.len()
            {
                return Err(HistoricalError::Normalization {
                    context: slug,
                    detail: "PMData book row has mismatched price/size list lengths".to_string(),
                });
            }
            let mut bids = Vec::with_capacity(row.bid_prices.len());
            for (&p, &s) in row.bid_prices.iter().zip(row.bid_sizes.iter()) {
                let (pm, pflags) = convert_price_to_micro(p, *tick_micro, false, &slug)?;
                let (sm, sflags) = convert_size_to_micro(s, min_size, false, &slug)?;
                extend_unique_flags(&mut base_flags, &pflags);
                extend_unique_flags(&mut base_flags, &sflags);
                bids.push(BookLevel {
                    price: pm,
                    size: sm,
                });
            }
            let mut asks = Vec::with_capacity(row.ask_prices.len());
            for (&p, &s) in row.ask_prices.iter().zip(row.ask_sizes.iter()) {
                let (pm, pflags) = convert_price_to_micro(p, *tick_micro, false, &slug)?;
                let (sm, sflags) = convert_size_to_micro(s, min_size, false, &slug)?;
                extend_unique_flags(&mut base_flags, &pflags);
                extend_unique_flags(&mut base_flags, &sflags);
                asks.push(BookLevel {
                    price: pm,
                    size: sm,
                });
            }
            // Ensure canonical descending bids / ascending asks even if source emitted reverse,
            // and coalesce any duplicate rounded `MicroUsdc` price levels.
            bids.sort_by_key(|a| std::cmp::Reverse(a.price));
            asks.sort_by_key(|a| a.price);
            coalesce_book_levels(&mut bids);
            coalesce_book_levels(&mut asks);

            let is_crossed = bids
                .first()
                .zip(asks.first())
                .is_some_and(|(b, a)| b.price >= a.price);
            if let Some((tracker, tracker_crossed, down_invalidated)) = book_tracker.as_mut() {
                if is_crossed {
                    **tracker_crossed = true;
                    **down_invalidated = true;
                } else {
                    let exch_ms = ts.exchange_ts_ms.unwrap_or(0);
                    let applied_ok = tracker.apply_snapshot(&bids, &asks, 0, exch_ms).is_ok();
                    **tracker_crossed = !applied_ok;
                    if applied_ok {
                        // A complete uncrossed snapshot repairs any earlier skipped Down derivation.
                        **down_invalidated = false;
                    } else {
                        **down_invalidated = true;
                    }
                }
            }
            if is_crossed {
                push_unique_flag(&mut base_flags, QualityFlag::CrossedBookObserved);
                summary.crossed_books_observed = summary.crossed_books_observed.saturating_add(1);
            }
            tally_quality_flags(&base_flags, summary);
            summary.book_snapshots = summary.book_snapshots.saturating_add(1);

            let mut out = vec![TimedEvent {
                sort_ts_us: row.timestamp_us,
                sort_local_us: row.local_timestamp_us,
                stream_rank,
                event: CanonicalEvent {
                    schema_version: CANONICAL_SCHEMA_VERSION,
                    provenance: EventProvenance {
                        source: DataSource::Pmdata,
                        original_event_type: "book".to_string(),
                        market_slug: slug.clone(),
                        timing_quality,
                        book_derivation: derivation,
                        normalization_version: CANONICAL_SCHEMA_VERSION,
                        raw_price_f64: None,
                        raw_size_f64: None,
                        quality_flags: base_flags.clone(),
                    },
                    timestamp: ts.clone(),
                    kind: CanonicalEventKind::BookSnapshot {
                        outcome,
                        bids: bids.clone(),
                        asks: asks.clone(),
                    },
                },
            }];

            if derive_complementary && !is_crossed {
                let comp_bids: Vec<BookLevel> = asks
                    .iter()
                    .map(|l| BookLevel {
                        price: MicroUsdc(ONE_DOLLAR_MICRO.saturating_sub(l.price.0)),
                        size: l.size,
                    })
                    .collect();
                let comp_asks: Vec<BookLevel> = bids
                    .iter()
                    .map(|l| BookLevel {
                        price: MicroUsdc(ONE_DOLLAR_MICRO.saturating_sub(l.price.0)),
                        size: l.size,
                    })
                    .collect();
                let mut comp_flags = base_flags;
                push_unique_flag(&mut comp_flags, QualityFlag::DerivedComplementaryDownBook);
                summary.book_snapshots = summary.book_snapshots.saturating_add(1);
                summary.derived_down_events = summary.derived_down_events.saturating_add(1);
                out.push(TimedEvent {
                    sort_ts_us: row.timestamp_us,
                    sort_local_us: row.local_timestamp_us,
                    stream_rank: 1,
                    event: CanonicalEvent {
                        schema_version: CANONICAL_SCHEMA_VERSION,
                        provenance: EventProvenance {
                            source: DataSource::Pmdata,
                            original_event_type: "book".to_string(),
                            market_slug: slug,
                            timing_quality,
                            book_derivation: BookDerivation::ComplementaryFromYesL2,
                            normalization_version: CANONICAL_SCHEMA_VERSION,
                            raw_price_f64: None,
                            raw_size_f64: None,
                            quality_flags: comp_flags,
                        },
                        timestamp: ts,
                        kind: CanonicalEventKind::BookSnapshot {
                            outcome: opposite_outcome,
                            bids: comp_bids,
                            asks: comp_asks,
                        },
                    },
                });
            }
            Ok(out)
        }
        "price_change" => {
            let raw_p = row.pc_price.ok_or_else(|| HistoricalError::Normalization {
                context: slug.clone(),
                detail: "price_change row missing pc_price".to_string(),
            })?;
            let raw_s = row.pc_size.ok_or_else(|| HistoricalError::Normalization {
                context: slug.clone(),
                detail: "price_change row missing pc_size".to_string(),
            })?;
            let side_raw =
                row.pc_side
                    .as_deref()
                    .ok_or_else(|| HistoricalError::Normalization {
                        context: slug.clone(),
                        detail: "price_change row missing pc_side".to_string(),
                    })?;
            let side =
                parse_order_side(side_raw).ok_or_else(|| HistoricalError::Normalization {
                    context: slug.clone(),
                    detail: format!("unrecognized pc_side `{side_raw}`"),
                })?;
            let (price, pflags) = convert_price_to_micro(raw_p, *tick_micro, false, &slug)?;
            let (size, sflags) = convert_size_to_micro(raw_s, min_size, true, &slug)?;
            extend_unique_flags(&mut base_flags, &pflags);
            extend_unique_flags(&mut base_flags, &sflags);

            let best_bid = match row.best_bid {
                Some(b) if b > 0.0 && b < 1.0 => {
                    let (bm, bflags) = convert_price_to_micro(b, *tick_micro, false, &slug)?;
                    extend_unique_flags(&mut base_flags, &bflags);
                    Some(bm)
                }
                _ => None,
            };
            let best_ask = match row.best_ask {
                Some(a) if a > 0.0 && a < 1.0 => {
                    let (am, aflags) = convert_price_to_micro(a, *tick_micro, false, &slug)?;
                    extend_unique_flags(&mut base_flags, &aflags);
                    Some(am)
                }
                _ => None,
            };
            let crossed_hint = best_bid.zip(best_ask).is_some_and(|(b, a)| b >= a);
            if crossed_hint {
                push_unique_flag(&mut base_flags, QualityFlag::CrossedBookObserved);
                summary.crossed_books_observed = summary.crossed_books_observed.saturating_add(1);
            }
            tally_quality_flags(&base_flags, summary);
            summary.price_changes = summary.price_changes.saturating_add(1);

            let mut out = vec![TimedEvent {
                sort_ts_us: row.timestamp_us,
                sort_local_us: row.local_timestamp_us,
                stream_rank,
                event: CanonicalEvent {
                    schema_version: CANONICAL_SCHEMA_VERSION,
                    provenance: EventProvenance {
                        source: DataSource::Pmdata,
                        original_event_type: "price_change".to_string(),
                        market_slug: slug.clone(),
                        timing_quality,
                        book_derivation: derivation,
                        normalization_version: CANONICAL_SCHEMA_VERSION,
                        raw_price_f64: Some(raw_p),
                        raw_size_f64: Some(raw_s),
                        quality_flags: base_flags.clone(),
                    },
                    timestamp: ts.clone(),
                    kind: CanonicalEventKind::PriceChange {
                        outcome,
                        side,
                        price,
                        size,
                        best_bid,
                        best_ask,
                    },
                },
            }];

            let (tracker_uncrossed, down_invalidated) =
                if let Some((tracker, tracker_crossed, invalidated)) = book_tracker.as_mut() {
                    let exch_ms = ts.exchange_ts_ms.unwrap_or(0);
                    let _ =
                        tracker.apply_delta_allow_transient_cross(side, price, size, 0, exch_ms);
                    let uncrossed = !**tracker_crossed
                        && !tracker.is_crossed()
                        && tracker
                            .best_bid()
                            .zip(tracker.best_ask())
                            .is_some_and(|(b, a)| b.price < a.price);
                    (uncrossed, &mut **invalidated)
                } else {
                    (false, &mut false)
                };
            let proven_uncrossed = match best_bid.zip(best_ask) {
                Some((b, a)) => b < a,
                None => !crossed_hint && tracker_uncrossed,
            } && price.0 > 0
                && price.0 < ONE_DOLLAR_MICRO;
            // Once a Yes update is skipped, halt derived Down deltas until the next uncrossed
            // `book` snapshot resets the full Down book, preventing stale level drift.
            if derive_complementary && !proven_uncrossed {
                *down_invalidated = true;
            }
            if derive_complementary && proven_uncrossed && !*down_invalidated {
                let comp_side = match side {
                    OrderSide::Buy => OrderSide::Sell,
                    OrderSide::Sell => OrderSide::Buy,
                };
                let comp_price = MicroUsdc(ONE_DOLLAR_MICRO.saturating_sub(price.0));
                let comp_best_bid =
                    best_ask.map(|a| MicroUsdc(ONE_DOLLAR_MICRO.saturating_sub(a.0)));
                let comp_best_ask =
                    best_bid.map(|b| MicroUsdc(ONE_DOLLAR_MICRO.saturating_sub(b.0)));
                let mut comp_flags = base_flags;
                push_unique_flag(&mut comp_flags, QualityFlag::DerivedComplementaryDownBook);
                summary.price_changes = summary.price_changes.saturating_add(1);
                summary.derived_down_events = summary.derived_down_events.saturating_add(1);
                out.push(TimedEvent {
                    sort_ts_us: row.timestamp_us,
                    sort_local_us: row.local_timestamp_us,
                    stream_rank: 1,
                    event: CanonicalEvent {
                        schema_version: CANONICAL_SCHEMA_VERSION,
                        provenance: EventProvenance {
                            source: DataSource::Pmdata,
                            original_event_type: "price_change".to_string(),
                            market_slug: slug,
                            timing_quality,
                            book_derivation: BookDerivation::ComplementaryFromYesL2,
                            normalization_version: CANONICAL_SCHEMA_VERSION,
                            raw_price_f64: Some(raw_p),
                            raw_size_f64: Some(raw_s),
                            quality_flags: comp_flags,
                        },
                        timestamp: ts,
                        kind: CanonicalEventKind::PriceChange {
                            outcome: opposite_outcome,
                            side: comp_side,
                            price: comp_price,
                            size,
                            best_bid: comp_best_bid,
                            best_ask: comp_best_ask,
                        },
                    },
                });
            }
            Ok(out)
        }
        "tick_size_change" => {
            let raw_t = row
                .new_tick_size
                .ok_or_else(|| HistoricalError::Normalization {
                    context: slug.clone(),
                    detail: "tick_size_change row missing new_tick_size".to_string(),
                })?;
            let (new_tick, flags) = convert_price_to_micro(raw_t, MicroUsdc(1), false, &slug)?;
            TickPrecision::from_micro(new_tick).map_err(|_| HistoricalError::Normalization {
                context: slug.clone(),
                detail: format!("unsupported new_tick_size `{raw_t}` ({} micro)", new_tick.0),
            })?;
            *tick_micro = new_tick;
            extend_unique_flags(&mut base_flags, &flags);
            summary.tick_size_changes = summary.tick_size_changes.saturating_add(1);
            Ok(vec![TimedEvent {
                sort_ts_us: row.timestamp_us,
                sort_local_us: row.local_timestamp_us,
                stream_rank: 0,
                event: CanonicalEvent {
                    schema_version: CANONICAL_SCHEMA_VERSION,
                    provenance: EventProvenance {
                        source: DataSource::Pmdata,
                        original_event_type: "tick_size_change".to_string(),
                        market_slug: slug,
                        timing_quality,
                        book_derivation: BookDerivation::NotApplicable,
                        normalization_version: CANONICAL_SCHEMA_VERSION,
                        raw_price_f64: Some(raw_t),
                        raw_size_f64: None,
                        quality_flags: base_flags,
                    },
                    timestamp: ts,
                    kind: CanonicalEventKind::TickSizeChange {
                        outcome: None,
                        new_tick_micro: new_tick,
                    },
                },
            }])
        }
        "market_resolved" => {
            let raw_win =
                row.winning_outcome
                    .as_deref()
                    .ok_or_else(|| HistoricalError::Normalization {
                        context: slug.clone(),
                        detail: "market_resolved row missing winning_outcome".to_string(),
                    })?;
            let mapped =
                parse_resolution_outcome(raw_win, mapping.yes_maps_to, mapping.no_maps_to, &slug)?;
            Ok(vec![TimedEvent {
                sort_ts_us: row.timestamp_us,
                sort_local_us: row.local_timestamp_us,
                stream_rank: 0,
                event: CanonicalEvent {
                    schema_version: CANONICAL_SCHEMA_VERSION,
                    provenance: EventProvenance {
                        source: DataSource::Pmdata,
                        original_event_type: "market_resolved".to_string(),
                        market_slug: slug,
                        timing_quality,
                        book_derivation: BookDerivation::NotApplicable,
                        normalization_version: CANONICAL_SCHEMA_VERSION,
                        raw_price_f64: None,
                        raw_size_f64: None,
                        quality_flags: base_flags,
                    },
                    timestamp: ts,
                    kind: CanonicalEventKind::MarketResolution {
                        winning_outcome: mapped,
                        resolution_source: ResolutionSource::PmdataMarketResolved,
                        raw_winning_label: Some(raw_win.to_string()),
                    },
                },
            }])
        }
        other => Err(HistoricalError::Normalization {
            context: slug,
            detail: format!("unknown PMData L2 event_type `{other}`"),
        }),
    }
}

fn parse_resolution_outcome(
    raw_win: &str,
    yes_outcome: Outcome,
    no_outcome: Outcome,
    slug: &str,
) -> Result<Outcome, HistoricalError> {
    let t = raw_win.trim();
    if t.eq_ignore_ascii_case("yes") {
        Ok(yes_outcome)
    } else if t.eq_ignore_ascii_case("no") {
        Ok(no_outcome)
    } else if t.eq_ignore_ascii_case("up") {
        Ok(Outcome::Up)
    } else if t.eq_ignore_ascii_case("down") {
        Ok(Outcome::Down)
    } else {
        Err(HistoricalError::Normalization {
            context: slug.to_string(),
            detail: format!("unrecognized winning_outcome `{raw_win}`"),
        })
    }
}

fn convert_trade_row(
    row: &RawPmdataTradeRow,
    _idx: usize,
    mapping: &OutcomeMapping,
    tick_micro: MicroUsdc,
    min_size: SharesMicro,
    seen: &mut HashSet<TradeDedupKey>,
    summary: &mut NormalizationSummary,
) -> Result<Option<TimedEvent>, HistoricalError> {
    let slug = &row.market_slug;
    let outcome = mapping.map_pmdata_outcome(&row.outcome, slug)?;
    let side = parse_order_side(&row.side).ok_or_else(|| HistoricalError::Normalization {
        context: slug.clone(),
        detail: format!("unrecognized trade side `{}`", row.side),
    })?;
    let (price, pflags) = convert_price_to_micro(row.price, tick_micro, false, slug)?;
    let (size, sflags) = convert_size_to_micro(row.size, min_size, false, slug)?;
    let normalized_trade_id = row
        .trade_id
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string);

    let key = TradeDedupKey {
        timestamp_us: row.timestamp_us,
        local_timestamp_us: normalized_trade_id
            .is_none()
            .then_some(row.local_timestamp_us),
        outcome,
        side,
        price_micro: price.0,
        size_micro: size.0,
        trade_id: normalized_trade_id.clone(),
    };
    if !seen.insert(key) {
        summary.duplicate_trades_filtered = summary.duplicate_trades_filtered.saturating_add(1);
        return Ok(None);
    }

    let (ts, timing_quality, mut flags) =
        make_pmdata_timestamp(row.timestamp_us, row.local_timestamp_us, false)?;
    extend_unique_flags(&mut flags, &pflags);
    extend_unique_flags(&mut flags, &sflags);
    if normalized_trade_id.is_none() {
        push_unique_flag(&mut flags, QualityFlag::MissingTradeId);
        summary.missing_trade_id_count = summary.missing_trade_id_count.saturating_add(1);
    }
    tally_quality_flags(&flags, summary);
    summary.trade_prints = summary.trade_prints.saturating_add(1);

    Ok(Some(TimedEvent {
        sort_ts_us: row.timestamp_us,
        sort_local_us: row.local_timestamp_us,
        stream_rank: 3,
        event: CanonicalEvent {
            schema_version: CANONICAL_SCHEMA_VERSION,
            provenance: EventProvenance {
                source: DataSource::Pmdata,
                original_event_type: row.event_type.clone(),
                market_slug: slug.clone(),
                timing_quality,
                book_derivation: BookDerivation::NotApplicable,
                normalization_version: CANONICAL_SCHEMA_VERSION,
                raw_price_f64: Some(row.price),
                raw_size_f64: Some(row.size),
                quality_flags: flags,
            },
            timestamp: ts,
            kind: CanonicalEventKind::TradePrint {
                outcome,
                side,
                price,
                size,
                trade_id: normalized_trade_id,
            },
        },
    }))
}

fn convert_onchain_row(
    row: &RawPmdataOnchainFillRow,
    _idx: usize,
    slug: &str,
    mapping: &OutcomeMapping,
    tick_micro: MicroUsdc,
    min_size: SharesMicro,
) -> Result<TimedEvent, HistoricalError> {
    let outcome = mapping.map_pmdata_outcome(&row.outcome, slug)?;
    let maker_side =
        parse_order_side(&row.maker_side).ok_or_else(|| HistoricalError::Normalization {
            context: slug.to_string(),
            detail: format!("unrecognized onchain maker_side `{}`", row.maker_side),
        })?;
    let taker_side =
        parse_order_side(&row.taker_side).ok_or_else(|| HistoricalError::Normalization {
            context: slug.to_string(),
            detail: format!("unrecognized onchain taker_side `{}`", row.taker_side),
        })?;
    let (price, pflags) = convert_price_to_micro(row.price, tick_micro, false, slug)?;
    let (token_amount_micro, sflags) =
        convert_size_to_micro(row.token_amount, min_size, false, slug)?;
    let (usd_amount_micro, _) =
        convert_size_to_micro(row.usd_amount, SharesMicro::ZERO, true, slug)?;
    let (ts, timing_quality, mut flags) = make_pmdata_timestamp(row.timestamp_us, 0, true)?;
    extend_unique_flags(&mut flags, &pflags);
    extend_unique_flags(&mut flags, &sflags);

    Ok(TimedEvent {
        sort_ts_us: row.timestamp_us,
        sort_local_us: row.timestamp_us,
        stream_rank: 4,
        event: CanonicalEvent {
            schema_version: CANONICAL_SCHEMA_VERSION,
            provenance: EventProvenance {
                source: DataSource::Pmdata,
                original_event_type: "onchain_fill".to_string(),
                market_slug: slug.to_string(),
                timing_quality,
                book_derivation: BookDerivation::NotApplicable,
                normalization_version: CANONICAL_SCHEMA_VERSION,
                raw_price_f64: Some(row.price),
                raw_size_f64: Some(row.token_amount),
                quality_flags: flags,
            },
            timestamp: ts,
            kind: CanonicalEventKind::OnchainFill {
                outcome,
                maker: row.maker.clone(),
                taker: row.taker.clone(),
                maker_side,
                taker_side,
                price,
                usd_amount_micro: MicroUsdc(usd_amount_micro.0),
                token_amount_micro,
                transaction_hash: row.transaction_hash.clone(),
                log_index: row.log_index,
            },
        },
    })
}

fn merge_causal_streams(a: Vec<TimedEvent>, b: Vec<TimedEvent>) -> Vec<TimedEvent> {
    let mut out = Vec::with_capacity(a.len() + b.len());
    let mut it_a = a.into_iter().peekable();
    let mut it_b = b.into_iter().peekable();
    loop {
        match (it_a.peek(), it_b.peek()) {
            (Some(ea), Some(eb)) => {
                let key_a = (ea.sort_ts_us, ea.sort_local_us, ea.stream_rank);
                let key_b = (eb.sort_ts_us, eb.sort_local_us, eb.stream_rank);
                if key_a <= key_b {
                    if let Some(next) = it_a.next() {
                        out.push(next);
                    }
                } else if let Some(next) = it_b.next() {
                    out.push(next);
                }
            }
            (Some(_), None) => {
                out.extend(it_a);
                break;
            }
            (None, Some(_)) => {
                out.extend(it_b);
                break;
            }
            (None, None) => break,
        }
    }
    out
}

pub(crate) fn coalesce_book_levels(levels: &mut Vec<BookLevel>) {
    if levels.len() < 2 {
        return;
    }
    let mut write = 0_usize;
    for read in 1..levels.len() {
        let Some(cur) = levels.get(read).copied() else {
            continue;
        };
        if let Some(prev) = levels.get_mut(write) {
            if prev.price == cur.price {
                prev.size = SharesMicro(prev.size.0.saturating_add(cur.size.0));
            } else {
                write = write.saturating_add(1);
                if let Some(slot) = levels.get_mut(write) {
                    *slot = cur;
                }
            }
        }
    }
    levels.truncate(write.saturating_add(1));
}

fn push_unique_flag(dst: &mut Vec<QualityFlag>, flag: QualityFlag) {
    if !dst.contains(&flag) {
        dst.push(flag);
    }
}

fn extend_unique_flags(dst: &mut Vec<QualityFlag>, src: &[QualityFlag]) {
    for &f in src {
        push_unique_flag(dst, f);
    }
}

fn tally_quality_flags(flags: &[QualityFlag], summary: &mut NormalizationSummary) {
    if flags.contains(&QualityFlag::SubTickPrice) {
        summary.sub_tick_prices = summary.sub_tick_prices.saturating_add(1);
    }
    if flags.contains(&QualityFlag::BelowMinOrderSize) {
        summary.below_min_size_count = summary.below_min_size_count.saturating_add(1);
    }
}

/// Validates that canonical `BookSnapshot` and `PriceChange` replay reconstructs uncrossed books cleanly.
pub fn verify_reconstructed_books_for_stream(
    events: &[CanonicalEvent],
) -> Result<usize, HistoricalError> {
    let mut up = BookMaintainer::new(Outcome::Up);
    let mut dn = BookMaintainer::new(Outcome::Down);
    let mut snapshots_checked = 0_usize;
    for ev in events {
        let exch_ms = ev
            .timestamp
            .exchange_ts_ms
            .unwrap_or(ev.timestamp.replay_order_ts_ms);
        let mono_ns = ev.timestamp.mono_ns.unwrap_or(0);
        match &ev.kind {
            CanonicalEventKind::BookSnapshot {
                outcome,
                bids,
                asks,
            } => {
                if ev
                    .provenance
                    .quality_flags
                    .contains(&QualityFlag::CrossedBookObserved)
                {
                    continue;
                }
                let m = match outcome {
                    Outcome::Up => &mut up,
                    Outcome::Down => &mut dn,
                };
                if m.is_crossed() {
                    return Err(HistoricalError::Normalization {
                        context: ev.provenance.market_slug.clone(),
                        detail: format!(
                            "unrepaired transient cross on {outcome:?} before snapshot at {exch_ms}ms"
                        ),
                    });
                }
                m.apply_snapshot(bids, asks, mono_ns, exch_ms)
                    .map_err(|err| HistoricalError::Normalization {
                        context: ev.provenance.market_slug.clone(),
                        detail: format!("snapshot reconstruction failed for {outcome:?}: {err}"),
                    })?;
                snapshots_checked = snapshots_checked.saturating_add(1);
            }
            CanonicalEventKind::PriceChange {
                outcome,
                side,
                price,
                size,
                ..
            } => {
                let m = match outcome {
                    Outcome::Up => &mut up,
                    Outcome::Down => &mut dn,
                };
                m.apply_delta_allow_transient_cross(*side, *price, *size, mono_ns, exch_ms)
                    .map_err(|err| HistoricalError::Normalization {
                        context: ev.provenance.market_slug.clone(),
                        detail: format!("delta reconstruction failed for {outcome:?}: {err}"),
                    })?;
            }
            _ => {}
        }
    }
    if up.is_crossed() || dn.is_crossed() {
        return Err(HistoricalError::Normalization {
            context: "book_reconstruction".to_string(),
            detail: format!(
                "unrepaired crossed book at end of stream (up_crossed={}, dn_crossed={})",
                up.is_crossed(),
                dn.is_crossed()
            ),
        });
    }
    Ok(snapshots_checked)
}
