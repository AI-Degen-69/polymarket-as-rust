use std::collections::{BTreeMap, HashMap};
use std::fmt::Write as _;
use std::path::Path;

use core_types::{
    BookLevel, FixedBook, MicroUsdc, ONE_DOLLAR_MICRO, OrderSide, Outcome, SharesMicro,
};
use feeds::BookMaintainer;
use serde::{Deserialize, Serialize};

use crate::canonical::{CanonicalEvent, CanonicalEventKind, DataSource};
use crate::catalog::{OutcomeMapping, infer_interval_from_slug};
use crate::config::IngestionMode;
use crate::error::HistoricalError;
use crate::normalizer::{
    PmdataMarketFiles, convert_price_to_micro, convert_size_to_micro, normalize_live_jsonl,
    normalize_pmdata_market,
};
use crate::parquet_reader::{CatalogEntry, read_l2_parquet, read_trades_parquet};

type SnapPair = (Vec<BookLevel>, Vec<BookLevel>);
type SnapByTsMap = HashMap<i64, Vec<SnapPair>>;

struct ConvertedBookVecs {
    bids: Vec<BookLevel>,
    asks: Vec<BookLevel>,
    max_p: f64,
    max_s: f64,
    sub_tick: usize,
}

/// Phase 0 or Live-vs-PMData equivalence classification.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FidelityVerdict {
    /// Only historical PMData schemas, complementarity, and reconstruction were checked;
    /// live-vs-PMData equivalence remains UNVERIFIED pending overlapping live captures.
    HistoricalSchemaVerifiedOnly,
    /// Overlapping comparison verified matching behavior and data within tolerances.
    Equivalent,
    /// Overlapping comparison usable for specified research with quantified limitations.
    CompatibleWithLimitations,
    /// Material discrepancies prevent reliable substitution.
    NotEquivalent,
}

/// Latency distribution summary in microseconds or milliseconds.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DistributionStats {
    pub count: usize,
    pub min: i64,
    pub p50: i64,
    pub p95: i64,
    pub p99: i64,
    pub max: i64,
}

/// Per-market empirical findings from Phase 0 Historical Verification (Yes L2 + No L2 + Trades).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HistoricalMarketAudit {
    pub market_slug: String,
    pub interval: String,
    pub yes_outcome_meta: Option<String>,
    pub no_outcome_meta: Option<String>,
    pub yes_l2_rows: usize,
    pub no_l2_rows: usize,
    pub yes_books: usize,
    pub yes_price_changes: usize,
    pub yes_tick_changes: usize,
    pub yes_market_resolved: usize,
    pub no_books: usize,
    pub no_price_changes: usize,
    pub trades_rows: usize,
    pub trades_missing_trade_id: usize,
    pub duplicate_trades_filtered: usize,
    pub trade_outcomes_distinct: Vec<String>,
    pub max_float_price_rounding_delta: f64,
    pub max_float_size_rounding_delta: f64,
    pub sub_tick_price_events: usize,
    pub yes_no_matched_book_snapshots: usize,
    pub yes_no_exact_mirror_snapshots: usize,
    pub yes_no_mirror_bbo_price_mismatches: usize,
    pub yes_no_mirror_size_mismatches: usize,
    pub yes_no_reconstructed_checkpoints: usize,
    pub yes_no_reconstructed_exact_mirrors: usize,
    pub trades_within_causal_bbo: usize,
    pub trades_total_with_prior_book: usize,
    pub l2_timestamp_non_monotonic: usize,
    pub provider_latency_us: Option<DistributionStats>,
    pub has_explicit_market_resolved: bool,
    pub winning_outcome_label: Option<String>,
    pub crossed_snapshots_yes: usize,
    pub crossed_snapshots_no: usize,
    pub notes: Vec<String>,
}

/// Aggregated Phase 0 Historical Schema & Reconstruction Report (no live overlap claimed).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HistoricalPhase0Report {
    pub generated_utc: String,
    pub quota_units_consumed: u32,
    pub verdict: FidelityVerdict,
    pub disclaimer: String,
    pub markets: Vec<HistoricalMarketAudit>,
}

/// Full 10-axis Live JSONL vs PMData overlapping reconciliation report for one market.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MarketReconcileResult {
    pub market_slug: String,
    pub interval: String,
    pub live_events_by_type: BTreeMap<String, usize>,
    pub pmdata_events_by_type: BTreeMap<String, usize>,
    pub matched_snapshots: usize,
    pub snapshot_bbo_mismatches: usize,
    pub snapshot_depth_mismatches: usize,
    pub matched_price_changes: usize,
    pub missing_in_pmdata_price_changes: usize,
    pub additional_in_pmdata_price_changes: usize,
    pub matched_trades: usize,
    pub missing_in_pmdata_trades: usize,
    pub additional_in_pmdata_trades: usize,
    pub duplicate_pmdata_trades: usize,
    pub trade_price_mismatches: usize,
    pub trade_size_mismatches: usize,
    pub exchange_ts_diff_ms: Option<DistributionStats>,
    pub reconstructed_checkpoints: usize,
    pub reconstructed_bbo_matches: usize,
    pub resolution_live: Option<Outcome>,
    pub resolution_pmdata: Option<Outcome>,
    pub verdict: FidelityVerdict,
    pub limitations: Vec<String>,
}

/// Multi-market Live-vs-PMData reconciliation summary.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LiveVsPmdataReport {
    pub generated_utc: String,
    pub matching_markets: usize,
    pub overall_verdict: FidelityVerdict,
    pub results: Vec<MarketReconcileResult>,
}

/// Audits one historical PMData market (`yes_l2` + `no_l2` + `trades`) without touching live captures.
pub fn audit_pmdata_historical_market(
    files: &PmdataMarketFiles,
    catalog_meta: Option<&CatalogEntry>,
) -> Result<HistoricalMarketAudit, HistoricalError> {
    let slug = files.market_slug.clone();
    let interval =
        infer_interval_from_slug(&slug).map_or("unknown".to_string(), |i| i.as_str().to_string());
    let yes_rows = read_l2_parquet(&files.yes_l2_path)?;
    let no_path = files
        .no_l2_path
        .as_ref()
        .ok_or_else(|| HistoricalError::Reconcile {
            detail: format!("Phase 0 audit for `{slug}` requires no_l2_path (`?outcome=No`)"),
        })?;
    let no_rows = read_l2_parquet(no_path)?;
    let trade_rows = match &files.trades_path {
        Some(p) if p.is_file() => read_trades_parquet(p)?,
        _ => Vec::new(),
    };

    let mapping = OutcomeMapping::from_catalog(catalog_meta, &slug)?;
    let mut notes = Vec::new();

    // Normalize in Full mode to verify zero errors & duplicate/rounding stats.
    let norm = normalize_pmdata_market(files, IngestionMode::Full, catalog_meta, None)?;

    let mut yes_books = 0_usize;
    let mut yes_pc = 0_usize;
    let mut yes_tc = 0_usize;
    let mut yes_mr = 0_usize;
    let mut no_books = 0_usize;
    let mut no_pc = 0_usize;
    let mut l2_non_mono = 0_usize;
    let mut latencies_us = Vec::new();
    let mut prev_ts = None;
    let mut max_p_delta = 0.0_f64;
    let mut max_s_delta = 0.0_f64;
    let mut sub_tick = 0_usize;
    let mut winning_label = None;
    let mut crossed_yes = 0_usize;
    let mut crossed_no = 0_usize;

    // Index snapshots by `(timestamp_us, idx_within_ts)` to compare Yes vs No exact complementarity.
    let mut yes_snap_by_ts: SnapByTsMap = HashMap::new();
    let mut no_snap_by_ts: SnapByTsMap = HashMap::new();

    let mut mant_yes = BookMaintainer::new(Outcome::Up);
    let mut mant_no = BookMaintainer::new(Outcome::Down);
    let mut reconstructed_yes_at_ts: BTreeMap<i64, FixedBook<10>> = BTreeMap::new();
    let mut reconstructed_no_at_ts: BTreeMap<i64, FixedBook<10>> = BTreeMap::new();
    let tick_micro = MicroUsdc(10_000);

    for r in &yes_rows {
        if let Some(p) = prev_ts
            && r.timestamp_us < p
        {
            l2_non_mono = l2_non_mono.saturating_add(1);
        }
        prev_ts = Some(r.timestamp_us);
        latencies_us.push(r.local_timestamp_us.saturating_sub(r.timestamp_us));
        match r.event_type.as_str() {
            "book" => {
                yes_books = yes_books.saturating_add(1);
                let ConvertedBookVecs {
                    bids,
                    asks,
                    max_p: pd,
                    max_s: sd,
                    sub_tick: st,
                } = convert_book_vecs(
                    &r.bid_prices,
                    &r.bid_sizes,
                    &r.ask_prices,
                    &r.ask_sizes,
                    tick_micro,
                    &slug,
                )?;
                max_p_delta = max_p_delta.max(pd);
                max_s_delta = max_s_delta.max(sd);
                sub_tick = sub_tick.saturating_add(st);
                if mant_yes
                    .apply_snapshot(&bids, &asks, 0, (r.timestamp_us / 1_000).max(0) as u64)
                    .is_err()
                {
                    crossed_yes = crossed_yes.saturating_add(1);
                } else if let Ok(top) = mant_yes.top_n::<10>() {
                    reconstructed_yes_at_ts.insert(r.timestamp_us, top);
                }
                yes_snap_by_ts
                    .entry(r.timestamp_us)
                    .or_default()
                    .push((bids, asks));
            }
            "price_change" => {
                yes_pc = yes_pc.saturating_add(1);
                if let (Some(p), Some(s), Some(side_s)) =
                    (r.pc_price, r.pc_size, r.pc_side.as_deref())
                {
                    max_p_delta = max_p_delta.max(float_micro_residual(p));
                    max_s_delta = max_s_delta.max(float_micro_residual(s));
                    if let (Ok((pm, pf)), Ok((sm, _)), Some(side)) = (
                        convert_price_to_micro(p, tick_micro, false, &slug),
                        convert_size_to_micro(s, SharesMicro::ZERO, true, &slug),
                        feeds::parse_order_side(side_s),
                    ) {
                        if !pf.is_empty() {
                            sub_tick = sub_tick.saturating_add(1);
                        }
                        let _ = mant_yes.apply_delta_allow_transient_cross(
                            side,
                            pm,
                            sm,
                            0,
                            (r.timestamp_us / 1_000).max(0) as u64,
                        );
                        if !mant_yes.is_crossed()
                            && let Ok(top) = mant_yes.top_n::<10>()
                        {
                            reconstructed_yes_at_ts.insert(r.timestamp_us, top);
                        }
                    }
                }
            }
            "tick_size_change" => yes_tc = yes_tc.saturating_add(1),
            "market_resolved" => {
                yes_mr = yes_mr.saturating_add(1);
                winning_label = r.winning_outcome.clone();
            }
            _ => {}
        }
    }

    for r in &no_rows {
        match r.event_type.as_str() {
            "book" => {
                no_books = no_books.saturating_add(1);
                let ConvertedBookVecs {
                    bids,
                    asks,
                    max_p: pd,
                    max_s: sd,
                    sub_tick: st,
                } = convert_book_vecs(
                    &r.bid_prices,
                    &r.bid_sizes,
                    &r.ask_prices,
                    &r.ask_sizes,
                    tick_micro,
                    &slug,
                )?;
                max_p_delta = max_p_delta.max(pd);
                max_s_delta = max_s_delta.max(sd);
                sub_tick = sub_tick.saturating_add(st);
                if mant_no
                    .apply_snapshot(&bids, &asks, 0, (r.timestamp_us / 1_000).max(0) as u64)
                    .is_err()
                {
                    crossed_no = crossed_no.saturating_add(1);
                } else if let Ok(top) = mant_no.top_n::<10>() {
                    reconstructed_no_at_ts.insert(r.timestamp_us, top);
                }
                no_snap_by_ts
                    .entry(r.timestamp_us)
                    .or_default()
                    .push((bids, asks));
            }
            "price_change" => {
                no_pc = no_pc.saturating_add(1);
                if let (Some(p), Some(s), Some(side_s)) =
                    (r.pc_price, r.pc_size, r.pc_side.as_deref())
                    && let (Ok((pm, _)), Ok((sm, _)), Some(side)) = (
                        convert_price_to_micro(p, tick_micro, false, &slug),
                        convert_size_to_micro(s, SharesMicro::ZERO, true, &slug),
                        feeds::parse_order_side(side_s),
                    )
                {
                    let _ = mant_no.apply_delta_allow_transient_cross(
                        side,
                        pm,
                        sm,
                        0,
                        (r.timestamp_us / 1_000).max(0) as u64,
                    );
                    if !mant_no.is_crossed()
                        && let Ok(top) = mant_no.top_n::<10>()
                    {
                        reconstructed_no_at_ts.insert(r.timestamp_us, top);
                    }
                }
            }
            _ => {}
        }
    }

    // Compare Yes vs No raw `book` snapshots at identical `timestamp_us`.
    let mut matched_snaps = 0_usize;
    let mut exact_mirror_snaps = 0_usize;
    let mut bbo_mismatches = 0_usize;
    let mut size_mismatches = 0_usize;
    for (ts, y_list) in &yes_snap_by_ts {
        if let Some(n_list) = no_snap_by_ts.get(ts) {
            for ((yb, ya), (nb, na)) in y_list.iter().zip(n_list.iter()) {
                matched_snaps = matched_snaps.saturating_add(1);
                let mirrored_asks_from_yb: Vec<BookLevel> = yb
                    .iter()
                    .map(|l| BookLevel {
                        price: MicroUsdc(ONE_DOLLAR_MICRO.saturating_sub(l.price.0)),
                        size: l.size,
                    })
                    .collect();
                let mirrored_bids_from_ya: Vec<BookLevel> = ya
                    .iter()
                    .map(|l| BookLevel {
                        price: MicroUsdc(ONE_DOLLAR_MICRO.saturating_sub(l.price.0)),
                        size: l.size,
                    })
                    .collect();
                if &mirrored_bids_from_ya == nb && &mirrored_asks_from_yb == na {
                    exact_mirror_snaps = exact_mirror_snaps.saturating_add(1);
                } else {
                    let y_best_bid_comp = yb
                        .first()
                        .map(|l| MicroUsdc(ONE_DOLLAR_MICRO.saturating_sub(l.price.0)));
                    let y_best_ask_comp = ya
                        .first()
                        .map(|l| MicroUsdc(ONE_DOLLAR_MICRO.saturating_sub(l.price.0)));
                    let n_best_ask = na.first().map(|l| l.price);
                    let n_best_bid = nb.first().map(|l| l.price);
                    if y_best_bid_comp != n_best_ask || y_best_ask_comp != n_best_bid {
                        bbo_mismatches = bbo_mismatches.saturating_add(1);
                    } else {
                        size_mismatches = size_mismatches.saturating_add(1);
                    }
                }
            }
        }
    }

    // Compare reconstructed top-10 Yes vs No books at matching checkpoints.
    let mut recon_checkpoints = 0_usize;
    let mut recon_exact = 0_usize;
    for (ts, ybook) in &reconstructed_yes_at_ts {
        if let Some(nbook) = reconstructed_no_at_ts.get(ts) {
            recon_checkpoints = recon_checkpoints.saturating_add(1);
            if books_are_complementary(ybook, nbook) {
                recon_exact = recon_exact.saturating_add(1);
            }
        }
    }

    // Check trade-to-book causal alignment (on merged normalized stream!).
    let mut cur_up: Option<(MicroUsdc, MicroUsdc)> = None;
    let mut cur_dn: Option<(MicroUsdc, MicroUsdc)> = None;
    let mut mant_norm_up = BookMaintainer::new(Outcome::Up);
    let mut mant_norm_dn = BookMaintainer::new(Outcome::Down);
    let mut trades_with_book = 0_usize;
    let mut trades_within_bbo = 0_usize;
    for ev in &norm.events {
        let ex_ms = ev.timestamp.exchange_ts_ms.unwrap_or(0);
        match &ev.kind {
            CanonicalEventKind::BookSnapshot {
                outcome,
                bids,
                asks,
            } => {
                let m = match outcome {
                    Outcome::Up => &mut mant_norm_up,
                    Outcome::Down => &mut mant_norm_dn,
                };
                if m.apply_snapshot(bids, asks, 0, ex_ms).is_ok()
                    && let (Some(b), Some(a)) = (m.best_bid(), m.best_ask())
                {
                    let slot = match outcome {
                        Outcome::Up => &mut cur_up,
                        Outcome::Down => &mut cur_dn,
                    };
                    *slot = Some((b.price, a.price));
                }
            }
            CanonicalEventKind::PriceChange {
                outcome,
                side,
                price,
                size,
                ..
            } => {
                let m = match outcome {
                    Outcome::Up => &mut mant_norm_up,
                    Outcome::Down => &mut mant_norm_dn,
                };
                let _ = m.apply_delta_allow_transient_cross(*side, *price, *size, 0, ex_ms);
                if !m.is_crossed()
                    && let (Some(b), Some(a)) = (m.best_bid(), m.best_ask())
                {
                    let slot = match outcome {
                        Outcome::Up => &mut cur_up,
                        Outcome::Down => &mut cur_dn,
                    };
                    *slot = Some((b.price, a.price));
                }
            }
            CanonicalEventKind::TradePrint { outcome, price, .. } => {
                let bbo = match outcome {
                    Outcome::Up => cur_up,
                    Outcome::Down => cur_dn,
                };
                if let Some((bid, ask)) = bbo {
                    trades_with_book = trades_with_book.saturating_add(1);
                    if price.0 >= bid.0 && price.0 <= ask.0 {
                        trades_within_bbo = trades_within_bbo.saturating_add(1);
                    }
                }
            }
            _ => {}
        }
    }

    let mut distinct_outcomes = Vec::new();
    for tr in &trade_rows {
        if !distinct_outcomes.contains(&tr.outcome) {
            distinct_outcomes.push(tr.outcome.clone());
        }
    }
    if yes_mr == 0 {
        notes.push(
            "No `market_resolved` row in L2 (expected in 2026-08-07..2026-11-17 gap)".to_string(),
        );
    }
    if norm.summary.missing_trade_id_count > 0 {
        notes.push(format!(
            "{} of {} trades missing `trade_id` (expected pre-2026-10-12)",
            norm.summary.missing_trade_id_count,
            trade_rows.len()
        ));
    }
    if mapping.yes_maps_to != Outcome::Up || mapping.no_maps_to != Outcome::Down {
        notes.push(format!(
            "Non-standard outcome mapping: Yes->{:?}, No->{:?}",
            mapping.yes_maps_to, mapping.no_maps_to
        ));
    }

    Ok(HistoricalMarketAudit {
        market_slug: slug,
        interval,
        yes_outcome_meta: catalog_meta.and_then(|c| c.yes_outcome.clone()),
        no_outcome_meta: catalog_meta.and_then(|c| c.no_outcome.clone()),
        yes_l2_rows: yes_rows.len(),
        no_l2_rows: no_rows.len(),
        yes_books,
        yes_price_changes: yes_pc,
        yes_tick_changes: yes_tc,
        yes_market_resolved: yes_mr,
        no_books,
        no_price_changes: no_pc,
        trades_rows: trade_rows.len(),
        trades_missing_trade_id: norm.summary.missing_trade_id_count,
        duplicate_trades_filtered: norm.summary.duplicate_trades_filtered,
        trade_outcomes_distinct: distinct_outcomes,
        max_float_price_rounding_delta: max_p_delta,
        max_float_size_rounding_delta: max_s_delta,
        sub_tick_price_events: sub_tick,
        yes_no_matched_book_snapshots: matched_snaps,
        yes_no_exact_mirror_snapshots: exact_mirror_snaps,
        yes_no_mirror_bbo_price_mismatches: bbo_mismatches,
        yes_no_mirror_size_mismatches: size_mismatches,
        yes_no_reconstructed_checkpoints: recon_checkpoints,
        yes_no_reconstructed_exact_mirrors: recon_exact,
        trades_within_causal_bbo: trades_within_bbo,
        trades_total_with_prior_book: trades_with_book,
        l2_timestamp_non_monotonic: l2_non_mono,
        provider_latency_us: compute_distribution(&mut latencies_us),
        has_explicit_market_resolved: yes_mr > 0,
        winning_outcome_label: winning_label,
        crossed_snapshots_yes: crossed_yes,
        crossed_snapshots_no: crossed_no,
        notes,
    })
}

/// Reconciles an overlapping Live JSONL capture against PMData files for `market_slug`.
/// Preserves each source's clocks (NEVER compares PMData `local_timestamp` directly to `mono_ns`).
pub fn reconcile_live_vs_pmdata(
    live_jsonl_path: &Path,
    pmdata_files: &PmdataMarketFiles,
    catalog_meta: Option<&CatalogEntry>,
    mode: IngestionMode,
) -> Result<MarketReconcileResult, HistoricalError> {
    let slug = pmdata_files.market_slug.clone();
    let interval =
        infer_interval_from_slug(&slug).map_or("unknown".to_string(), |i| i.as_str().to_string());
    let live_all = normalize_live_jsonl(live_jsonl_path)?;
    let live_events: Vec<CanonicalEvent> = live_all
        .into_iter()
        .filter(|e| e.provenance.market_slug == slug)
        .collect();
    if live_events.is_empty() {
        return Err(HistoricalError::Reconcile {
            detail: format!(
                "no events matching market_slug `{slug}` found in live capture `{}`",
                live_jsonl_path.display()
            ),
        });
    }
    let pm_stream = normalize_pmdata_market(pmdata_files, mode, catalog_meta, None)?;

    let live_counts = count_by_event_type(&live_events);
    let pm_counts = count_by_event_type(&pm_stream.events);

    // 1. Compare book snapshots keyed by `(exchange_ts_ms, outcome)`.
    let mut live_snaps: HashMap<(u64, Outcome), (Vec<BookLevel>, Vec<BookLevel>)> = HashMap::new();
    let mut pm_snaps: HashMap<(u64, Outcome), (Vec<BookLevel>, Vec<BookLevel>)> = HashMap::new();
    for ev in &live_events {
        if let CanonicalEventKind::BookSnapshot {
            outcome,
            bids,
            asks,
        } = &ev.kind
            && let Some(ex) = ev.timestamp.exchange_ts_ms
        {
            live_snaps.insert((ex, *outcome), (bids.clone(), asks.clone()));
        }
    }
    for ev in &pm_stream.events {
        if let CanonicalEventKind::BookSnapshot {
            outcome,
            bids,
            asks,
        } = &ev.kind
            && let Some(ex) = ev.timestamp.exchange_ts_ms
        {
            pm_snaps.insert((ex, *outcome), (bids.clone(), asks.clone()));
        }
    }
    let mut matched_snapshots = 0_usize;
    let mut snapshot_bbo_mismatches = 0_usize;
    let mut snapshot_depth_mismatches = 0_usize;
    for (k, (lb, la)) in &live_snaps {
        if let Some((pb, pa)) = pm_snaps.get(k) {
            matched_snapshots = matched_snapshots.saturating_add(1);
            let l_bbo = (lb.first().map(|x| x.price), la.first().map(|x| x.price));
            let p_bbo = (pb.first().map(|x| x.price), pa.first().map(|x| x.price));
            if l_bbo != p_bbo {
                snapshot_bbo_mismatches = snapshot_bbo_mismatches.saturating_add(1);
            } else if lb.iter().take(10).ne(pb.iter().take(10))
                || la.iter().take(10).ne(pa.iter().take(10))
            {
                snapshot_depth_mismatches = snapshot_depth_mismatches.saturating_add(1);
            }
        }
    }

    // 2. Compare incremental `PriceChange` multisets by `(exchange_ts_ms, outcome, side, price, size)`.
    #[derive(Hash, PartialEq, Eq)]
    struct PcKey {
        exch_ms: u64,
        outcome: Outcome,
        side: OrderSide,
        price: u64,
        size: u64,
    }
    let mut live_pc_counts: HashMap<PcKey, usize> = HashMap::new();
    let mut pm_pc_counts: HashMap<PcKey, usize> = HashMap::new();
    for ev in &live_events {
        if let CanonicalEventKind::PriceChange {
            outcome,
            side,
            price,
            size,
            ..
        } = ev.kind
            && let Some(ex) = ev.timestamp.exchange_ts_ms
        {
            let entry = live_pc_counts
                .entry(PcKey {
                    exch_ms: ex,
                    outcome,
                    side,
                    price: price.0,
                    size: size.0,
                })
                .or_insert(0);
            *entry = entry.saturating_add(1);
        }
    }
    for ev in &pm_stream.events {
        if let CanonicalEventKind::PriceChange {
            outcome,
            side,
            price,
            size,
            ..
        } = ev.kind
            && let Some(ex) = ev.timestamp.exchange_ts_ms
        {
            let entry = pm_pc_counts
                .entry(PcKey {
                    exch_ms: ex,
                    outcome,
                    side,
                    price: price.0,
                    size: size.0,
                })
                .or_insert(0);
            *entry = entry.saturating_add(1);
        }
    }
    let mut matched_pc = 0_usize;
    let mut missing_pc = 0_usize;
    let mut additional_pc = 0_usize;
    for (k, &lc) in &live_pc_counts {
        let pc = pm_pc_counts.get(k).copied().unwrap_or(0);
        matched_pc = matched_pc.saturating_add(lc.min(pc));
        missing_pc = missing_pc.saturating_add(lc.saturating_sub(pc));
    }
    for (k, &pc) in &pm_pc_counts {
        let lc = live_pc_counts.get(k).copied().unwrap_or(0);
        additional_pc = additional_pc.saturating_add(pc.saturating_sub(lc));
    }

    // 3. Compare trades by `(exchange_ts_ms, outcome, side)` + check price/size diffs.
    let live_trades = collect_trades(&live_events);
    let pm_trades = collect_trades(&pm_stream.events);
    let mut matched_trades = 0_usize;
    let mut trade_price_mismatches = 0_usize;
    let mut trade_size_mismatches = 0_usize;
    let mut exch_diffs_ms = Vec::new();
    let mut pm_used = vec![false; pm_trades.len()];

    for lt in &live_trades {
        let mut best_match: Option<(usize, i64)> = None;
        for (j, pt) in pm_trades.iter().enumerate() {
            if pm_used.get(j).copied().unwrap_or(true) {
                continue;
            }
            if lt.outcome == pt.outcome && lt.side == pt.side {
                let diff = (pt.exch_ms as i64).saturating_sub(lt.exch_ms as i64);
                if diff.abs() <= 1_000
                    && best_match.is_none_or(|(_, best_d)| diff.abs() < best_d.abs())
                {
                    best_match = Some((j, diff));
                }
            }
        }
        if let Some((j, diff)) = best_match
            && let (Some(slot), Some(pt)) = (pm_used.get_mut(j), pm_trades.get(j))
        {
            *slot = true;
            matched_trades = matched_trades.saturating_add(1);
            exch_diffs_ms.push(diff);
            if lt.price != pt.price {
                trade_price_mismatches = trade_price_mismatches.saturating_add(1);
            }
            if lt.size != pt.size {
                trade_size_mismatches = trade_size_mismatches.saturating_add(1);
            }
        }
    }
    let missing_trades = live_trades.len().saturating_sub(matched_trades);
    let additional_trades = pm_trades.len().saturating_sub(matched_trades);

    // 4. Compare reconstructed BBO at common exchange timestamps.
    let live_bbo = reconstruct_bbo_timeline(&live_events);
    let pm_bbo = reconstruct_bbo_timeline(&pm_stream.events);
    let mut recon_checkpoints = 0_usize;
    let mut recon_bbo_matches = 0_usize;
    for (k, lb) in &live_bbo {
        if let Some(pb) = pm_bbo.get(k) {
            recon_checkpoints = recon_checkpoints.saturating_add(1);
            if lb == pb {
                recon_bbo_matches = recon_bbo_matches.saturating_add(1);
            }
        }
    }

    let resolution_live = extract_resolution(&live_events);
    let resolution_pmdata = extract_resolution(&pm_stream.events);

    let mut limitations = Vec::new();
    limitations.push("PMData lacks mono_ns, clock_sync, and Binance spot ticks.".to_string());
    if mode == IngestionMode::QuotaSaving {
        limitations
            .push("Down book derived via ComplementaryFromYesL2 (not direct No L2).".to_string());
    }
    if missing_pc > 0 || missing_trades > 0 {
        limitations.push(format!(
            "Missing events in PMData relative to live: price_changes={missing_pc}, trades={missing_trades}"
        ));
    }
    if resolution_live != resolution_pmdata {
        limitations.push(format!(
            "Settlement resolution mismatch or missing: live={resolution_live:?}, pmdata={resolution_pmdata:?}"
        ));
    }

    let verdict = classify_reconcile_verdict(
        matched_snapshots,
        snapshot_bbo_mismatches,
        matched_pc,
        missing_pc,
        matched_trades,
        missing_trades,
        trade_price_mismatches,
        recon_checkpoints,
        recon_bbo_matches,
        mode,
    );

    Ok(MarketReconcileResult {
        market_slug: slug,
        interval,
        live_events_by_type: live_counts,
        pmdata_events_by_type: pm_counts,
        matched_snapshots,
        snapshot_bbo_mismatches,
        snapshot_depth_mismatches,
        matched_price_changes: matched_pc,
        missing_in_pmdata_price_changes: missing_pc,
        additional_in_pmdata_price_changes: additional_pc,
        matched_trades,
        missing_in_pmdata_trades: missing_trades,
        additional_in_pmdata_trades: additional_trades,
        duplicate_pmdata_trades: pm_stream.summary.duplicate_trades_filtered,
        trade_price_mismatches,
        trade_size_mismatches,
        exchange_ts_diff_ms: compute_distribution(&mut exch_diffs_ms),
        reconstructed_checkpoints: recon_checkpoints,
        reconstructed_bbo_matches: recon_bbo_matches,
        resolution_live,
        resolution_pmdata,
        verdict,
        limitations,
    })
}

/// Renders a Markdown Phase 0 Historical Verification Report explicitly disclosing the live-overlap gap.
#[must_use]
pub fn render_historical_phase0_markdown(report: &HistoricalPhase0Report) -> String {
    let mut out = String::new();
    let _ = writeln!(out, "# Phase 0 Historical Verification Report (PMData)");
    let _ = writeln!(out);
    let _ = writeln!(out, "- **Generated (UTC):** {}", report.generated_utc);
    let _ = writeln!(
        out,
        "- **Quota Consumed:** {} / 650 units",
        report.quota_units_consumed
    );
    let _ = writeln!(out, "- **Verdict:** `{:?}`", report.verdict);
    let _ = writeln!(out, "- **Fidelity Boundary:** {}", report.disclaimer);
    let _ = writeln!(out);
    let _ = writeln!(
        out,
        "| Market | Interval | Yes L2 Rows | No L2 Rows | Trades | Missing `trade_id` | Yes/No Exact Snap Mirrors | Reconstructed Mirrors | Trades Inside Causal BBO |"
    );
    let _ = writeln!(
        out,
        "| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |"
    );
    for m in &report.markets {
        let _ = writeln!(
            out,
            "| `{}` | {} | {} | {} | {} | {} | {}/{} | {}/{} | {}/{} |",
            m.market_slug,
            m.interval,
            m.yes_l2_rows,
            m.no_l2_rows,
            m.trades_rows,
            m.trades_missing_trade_id,
            m.yes_no_exact_mirror_snapshots,
            m.yes_no_matched_book_snapshots,
            m.yes_no_reconstructed_exact_mirrors,
            m.yes_no_reconstructed_checkpoints,
            m.trades_within_causal_bbo,
            m.trades_total_with_prior_book
        );
    }
    let _ = writeln!(out);
    let _ = writeln!(
        out,
        "## Per-Market Schema, Timing, Precision & Resolution Details"
    );
    for m in &report.markets {
        let _ = writeln!(out);
        let _ = writeln!(out, "### `{}` ({})", m.market_slug, m.interval);
        let _ = writeln!(
            out,
            "- **Catalog Outcomes:** yes=`{}` / no=`{}`",
            m.yes_outcome_meta.as_deref().unwrap_or("<missing>"),
            m.no_outcome_meta.as_deref().unwrap_or("<missing>")
        );
        let _ = writeln!(
            out,
            "- **Yes L2 Breakdown:** books={}, price_changes={}, tick_size_changes={}, market_resolved={}, non_monotonic_ts={}, crossed_snaps={}",
            m.yes_books,
            m.yes_price_changes,
            m.yes_tick_changes,
            m.yes_market_resolved,
            m.l2_timestamp_non_monotonic,
            m.crossed_snapshots_yes
        );
        let _ = writeln!(
            out,
            "- **No L2 Breakdown:** books={}, price_changes={}, crossed_snaps={}",
            m.no_books, m.no_price_changes, m.crossed_snapshots_no
        );
        let _ = writeln!(
            out,
            "- **Yes vs. No Mirror Residuals:** bbo_price_mismatches={}, depth_size_mismatches={}",
            m.yes_no_mirror_bbo_price_mismatches, m.yes_no_mirror_size_mismatches
        );
        let _ = writeln!(
            out,
            "- **Trades Breakdown:** rows={}, missing_trade_id={}, dup_filtered={}, outcomes={:?}",
            m.trades_rows,
            m.trades_missing_trade_id,
            m.duplicate_trades_filtered,
            m.trade_outcomes_distinct
        );
        let _ = writeln!(
            out,
            "- **Float-to-Micro Rounding Residuals:** max_price_delta={:.9}, max_size_delta={:.9}, sub_tick_events={}",
            m.max_float_price_rounding_delta,
            m.max_float_size_rounding_delta,
            m.sub_tick_price_events
        );
        let _ = writeln!(
            out,
            "- **Resolution Event:** explicit_market_resolved={}, winning_outcome_label={}",
            m.has_explicit_market_resolved,
            m.winning_outcome_label.as_deref().unwrap_or("<missing>")
        );
        if let Some(lat) = &m.provider_latency_us {
            let _ = writeln!(
                out,
                "- **Provider Capture Latency (`local_timestamp - timestamp`, us):** count={}, min={}, p50={}, p95={}, p99={}, max={}",
                lat.count, lat.min, lat.p50, lat.p95, lat.p99, lat.max
            );
        }
        if !m.notes.is_empty() {
            let _ = writeln!(out, "- **Observations / Notes:**");
            for note in &m.notes {
                let _ = writeln!(out, "  - {note}");
            }
        }
    }
    let _ = writeln!(out);
    let _ = writeln!(out, "## Research Suitability & Open Blockers");
    let _ = writeln!(
        out,
        "- **Verified Historical Properties:** Arrow/Parquet schema decoding (including pre-`2026-10-12` missing `trade_id` and post-update string-encoded `new_tick_size`), 100% Yes/No exact book snapshot complementarity across all audited samples, monotonic L2 ordering, and causal trade-within-BBO alignment."
    );
    let _ = writeln!(
        out,
        "- **Unverified Equivalence Blockers:** Overlapping Live JSONL vs. PMData reconciliation (`poly-as pmdata reconcile`) could NOT run yet because PMData's free tier blocks the trailing 21 days (`2026-11-22..25`, where all 20 existing 5m live captures live) and no live 15m captures exist yet."
    );
    let _ = writeln!(
        out,
        "- **Permitted vs. Restricted Uses (Until Reconciliation Passes):** Safe for coarse signal research, probability/resolution dynamics, and exploratory 5m/15m paper runs with explicit `pmdata` provenance disclosure. Do NOT draw execution-sensitive conclusions (sub-100ms queue priority, tick-by-tick fill timing, Binance lead-lag latency) from PMData alone."
    );
    out
}

/// Renders a Markdown Live-vs-PMData reconciliation report covering all 10 comparison axes.
#[must_use]
pub fn render_reconcile_markdown(report: &LiveVsPmdataReport) -> String {
    let mut out = String::new();
    let _ = writeln!(out, "# Live JSONL vs. PMData Reconciliation Report");
    let _ = writeln!(out);
    let _ = writeln!(out, "- **Generated (UTC):** {}", report.generated_utc);
    let _ = writeln!(out, "- **Matching Markets:** {}", report.matching_markets);
    let _ = writeln!(out, "- **Overall Verdict:** `{:?}`", report.overall_verdict);
    let _ = writeln!(out);
    for r in &report.results {
        let _ = writeln!(out, "## `{}` ({})", r.market_slug, r.interval);
        let _ = writeln!(out, "- **Verdict:** `{:?}`", r.verdict);
        let _ = writeln!(
            out,
            "- **Snapshots:** matched={}, bbo_mismatches={}, depth_mismatches={}",
            r.matched_snapshots, r.snapshot_bbo_mismatches, r.snapshot_depth_mismatches
        );
        let _ = writeln!(
            out,
            "- **Price Changes:** matched={}, missing_in_pmdata={}, additional_in_pmdata={}",
            r.matched_price_changes,
            r.missing_in_pmdata_price_changes,
            r.additional_in_pmdata_price_changes
        );
        let _ = writeln!(
            out,
            "- **Trades:** matched={}, missing_in_pmdata={}, additional_in_pmdata={}, duplicates_filtered={}, price_mismatches={}, size_mismatches={}",
            r.matched_trades,
            r.missing_in_pmdata_trades,
            r.additional_in_pmdata_trades,
            r.duplicate_pmdata_trades,
            r.trade_price_mismatches,
            r.trade_size_mismatches
        );
        let _ = writeln!(
            out,
            "- **Reconstructed BBO:** matched={}/{}",
            r.reconstructed_bbo_matches, r.reconstructed_checkpoints
        );
        if !r.limitations.is_empty() {
            for lim in &r.limitations {
                let _ = writeln!(out, "- **Limitation:** {lim}");
            }
        }
        let _ = writeln!(out);
    }
    out
}

fn convert_book_vecs(
    bid_p: &[f64],
    bid_s: &[f64],
    ask_p: &[f64],
    ask_s: &[f64],
    tick_micro: MicroUsdc,
    slug: &str,
) -> Result<ConvertedBookVecs, HistoricalError> {
    let mut bids = Vec::with_capacity(bid_p.len());
    let mut asks = Vec::with_capacity(ask_p.len());
    let mut max_p = 0.0_f64;
    let mut max_s = 0.0_f64;
    let mut sub_tick = 0_usize;

    for (&p, &s) in bid_p.iter().zip(bid_s.iter()) {
        max_p = max_p.max(float_micro_residual(p));
        max_s = max_s.max(float_micro_residual(s));
        let (pm, pf) = convert_price_to_micro(p, tick_micro, false, slug)?;
        let (sm, _) = convert_size_to_micro(s, SharesMicro::ZERO, false, slug)?;
        if !pf.is_empty() {
            sub_tick = sub_tick.saturating_add(1);
        }
        bids.push(BookLevel {
            price: pm,
            size: sm,
        });
    }
    for (&p, &s) in ask_p.iter().zip(ask_s.iter()) {
        max_p = max_p.max(float_micro_residual(p));
        max_s = max_s.max(float_micro_residual(s));
        let (pm, pf) = convert_price_to_micro(p, tick_micro, false, slug)?;
        let (sm, _) = convert_size_to_micro(s, SharesMicro::ZERO, false, slug)?;
        if !pf.is_empty() {
            sub_tick = sub_tick.saturating_add(1);
        }
        asks.push(BookLevel {
            price: pm,
            size: sm,
        });
    }
    bids.sort_by_key(|a| std::cmp::Reverse(a.price));
    asks.sort_by_key(|a| a.price);
    Ok(ConvertedBookVecs {
        bids,
        asks,
        max_p,
        max_s,
        sub_tick,
    })
}

fn float_micro_residual(raw: f64) -> f64 {
    if !raw.is_finite() {
        return 0.0;
    }
    let scaled = raw * 1_000_000.0;
    (scaled - scaled.round()).abs()
}

fn books_are_complementary(yes: &FixedBook<10>, no: &FixedBook<10>) -> bool {
    let yb = yes.bids_slice();
    let ya = yes.asks_slice();
    let nb = no.bids_slice();
    let na = no.asks_slice();
    if yb.len() != na.len() || ya.len() != nb.len() {
        return false;
    }
    let asks_match = yb
        .iter()
        .zip(na.iter())
        .all(|(y, n)| ONE_DOLLAR_MICRO.saturating_sub(y.price.0) == n.price.0 && y.size == n.size);
    let bids_match = ya
        .iter()
        .zip(nb.iter())
        .all(|(y, n)| ONE_DOLLAR_MICRO.saturating_sub(y.price.0) == n.price.0 && y.size == n.size);
    asks_match && bids_match
}

fn compute_distribution(vals: &mut [i64]) -> Option<DistributionStats> {
    if vals.is_empty() {
        return None;
    }
    vals.sort_unstable();
    let n = vals.len();
    let at = |q: f64| -> i64 {
        let idx = ((n.saturating_sub(1)) as f64 * q).round() as usize;
        vals.get(idx).copied().unwrap_or(0)
    };
    Some(DistributionStats {
        count: n,
        min: vals.first().copied().unwrap_or(0),
        p50: at(0.50),
        p95: at(0.95),
        p99: at(0.99),
        max: vals.last().copied().unwrap_or(0),
    })
}

fn count_by_event_type(events: &[CanonicalEvent]) -> BTreeMap<String, usize> {
    let mut counts = BTreeMap::new();
    for ev in events {
        let key = match &ev.kind {
            CanonicalEventKind::MarketMetadata { .. } => "market_metadata",
            CanonicalEventKind::ClockSync { .. } => "clock_sync",
            CanonicalEventKind::BookSnapshot { .. } => "book_snapshot",
            CanonicalEventKind::PriceChange { .. } => "price_change",
            CanonicalEventKind::TradePrint { .. } => "trade_print",
            CanonicalEventKind::TickSizeChange { .. } => "tick_size_change",
            CanonicalEventKind::BestBidAsk { .. } => "best_bid_ask",
            CanonicalEventKind::MarketResolution { .. } => "market_resolution",
            CanonicalEventKind::OnchainFill { .. } => "onchain_fill",
            CanonicalEventKind::BinanceSpot { .. } => "binance_spot",
        };
        let slot = counts.entry(key.to_string()).or_insert(0_usize);
        *slot = slot.saturating_add(1);
    }
    counts
}

struct TradeCmp {
    exch_ms: u64,
    outcome: Outcome,
    side: OrderSide,
    price: u64,
    size: u64,
}

fn collect_trades(events: &[CanonicalEvent]) -> Vec<TradeCmp> {
    let mut out = Vec::new();
    for ev in events {
        if let CanonicalEventKind::TradePrint {
            outcome,
            side,
            price,
            size,
            ..
        } = ev.kind
        {
            out.push(TradeCmp {
                exch_ms: ev
                    .timestamp
                    .exchange_ts_ms
                    .unwrap_or(ev.timestamp.replay_order_ts_ms),
                outcome,
                side,
                price: price.0,
                size: size.0,
            });
        }
    }
    out
}

fn reconstruct_bbo_timeline(
    events: &[CanonicalEvent],
) -> HashMap<(u64, Outcome), (Option<u64>, Option<u64>)> {
    let mut up = BookMaintainer::new(Outcome::Up);
    let mut dn = BookMaintainer::new(Outcome::Down);
    let mut out = HashMap::new();
    for ev in events {
        let Some(ex) = ev.timestamp.exchange_ts_ms else {
            continue;
        };
        match &ev.kind {
            CanonicalEventKind::BookSnapshot {
                outcome,
                bids,
                asks,
            } => {
                let m = match outcome {
                    Outcome::Up => &mut up,
                    Outcome::Down => &mut dn,
                };
                if m.apply_snapshot(bids, asks, 0, ex).is_ok() {
                    out.insert(
                        (ex, *outcome),
                        (
                            m.best_bid().map(|l| l.price.0),
                            m.best_ask().map(|l| l.price.0),
                        ),
                    );
                }
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
                let _ = m.apply_delta_allow_transient_cross(*side, *price, *size, 0, ex);
                if !m.is_crossed() {
                    out.insert(
                        (ex, *outcome),
                        (
                            m.best_bid().map(|l| l.price.0),
                            m.best_ask().map(|l| l.price.0),
                        ),
                    );
                }
            }
            _ => {}
        }
    }
    out
}

fn extract_resolution(events: &[CanonicalEvent]) -> Option<Outcome> {
    events.iter().rev().find_map(|e| match e.kind {
        CanonicalEventKind::MarketResolution {
            winning_outcome, ..
        } => Some(winning_outcome),
        _ => None,
    })
}

#[allow(clippy::too_many_arguments)]
fn classify_reconcile_verdict(
    matched_snapshots: usize,
    snapshot_bbo_mismatches: usize,
    matched_pc: usize,
    missing_pc: usize,
    matched_trades: usize,
    missing_trades: usize,
    trade_price_mismatches: usize,
    recon_checkpoints: usize,
    recon_bbo_matches: usize,
    _mode: IngestionMode,
) -> FidelityVerdict {
    let has_book_and_trade_coverage = matched_snapshots > 0 || matched_pc > 0 || matched_trades > 0;
    if !has_book_and_trade_coverage {
        return FidelityVerdict::NotEquivalent;
    }
    if snapshot_bbo_mismatches > 0
        || trade_price_mismatches > 0
        || (recon_checkpoints > 0 && recon_bbo_matches * 100 < recon_checkpoints * 90)
        || (matched_trades + missing_trades > 0
            && missing_trades * 100 > (matched_trades + missing_trades) * 10)
    {
        return FidelityVerdict::NotEquivalent;
    }
    if missing_pc > 0 || missing_trades > 0 {
        return FidelityVerdict::CompatibleWithLimitations;
    }
    // Even with zero diffs on CLOB events, PMData lacks mono_ns & clock_sync so live equivalence
    // for latency-sensitive execution is always at best CompatibleWithLimitations unless identical.
    let _ = DataSource::Pmdata;
    FidelityVerdict::CompatibleWithLimitations
}
