use std::error::Error;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::{Component, Path, PathBuf};

use core_types::{
    BookLevel, FixedBook, JsonlWriter, MarketDescriptor, MicroUsdc, MonotonicClock,
    ONE_DOLLAR_MICRO, OrderSide, Outcome, SharesMicro, TickPrecision,
};
use feeds::{
    BinanceSpotEvent, BookMaintainer, ClockSyncReport, DEFAULT_RING_CAPACITY, FeedHealthEvent,
    FeedId, FeedStats, OracleTwapEvent, PolyMarketEvent,
};
use serde::{Deserialize, Serialize};

use crate::theme::{self, Badge, Theme};
use crate::window_gate::{format_local_datetime, format_local_hms, format_utc_hms};

pub(crate) struct CaptureReportData {
    pub(crate) poly_books: u64,
    pub(crate) poly_price_changes: u64,
    pub(crate) poly_trades: u64,
    pub(crate) poly_tick_changes: u64,
    pub(crate) poly_bba: u64,
    pub(crate) binance_tickers: u64,
    pub(crate) binance_trades: u64,
    pub(crate) polybolt_events: u64,
    pub(crate) peak_poly_slots: usize,
    pub(crate) peak_binance_slots: usize,
    pub(crate) peak_oracle_slots: usize,
    pub(crate) health_counts: HealthSummaryCounts,
    pub(crate) maintainer_up: BookMaintainer,
    pub(crate) maintainer_dn: BookMaintainer,
    pub(crate) latest_up_book: Option<FixedBook<10>>,
    pub(crate) latest_dn_book: Option<FixedBook<10>>,
    pub(crate) samples_binance: Vec<String>,
    pub(crate) samples_poly: Vec<String>,
    pub(crate) latest_binance_px_e8: Option<u64>,
}

impl CaptureReportData {
    pub(crate) fn new(polybolt_state: String) -> Self {
        Self {
            poly_books: 0,
            poly_price_changes: 0,
            poly_trades: 0,
            poly_tick_changes: 0,
            poly_bba: 0,
            binance_tickers: 0,
            binance_trades: 0,
            polybolt_events: 0,
            peak_poly_slots: 0,
            peak_binance_slots: 0,
            peak_oracle_slots: 0,
            health_counts: HealthSummaryCounts {
                polybolt_state,
                ..HealthSummaryCounts::default()
            },
            maintainer_up: BookMaintainer::new(Outcome::Up),
            maintainer_dn: BookMaintainer::new(Outcome::Down),
            latest_up_book: None,
            latest_dn_book: None,
            samples_binance: Vec::new(),
            samples_poly: Vec::new(),
            latest_binance_px_e8: None,
        }
    }
}

#[derive(Default, Clone, Serialize, Deserialize)]
pub(crate) struct HealthSummaryCounts {
    pub(crate) clob_state: String,
    pub(crate) binance_state: String,
    pub(crate) polybolt_state: String,
    pub(crate) ring_overflows: u64,
    pub(crate) stale_timeouts: u64,
    pub(crate) parse_warnings: u64,
    pub(crate) disconnects: u64,
    pub(crate) market_resolved: bool,
}

#[derive(Serialize)]
struct FeedsSummaryRecord<'a> {
    market_slug: &'a str,
    poly_books: u64,
    poly_price_changes: u64,
    poly_trades: u64,
    poly_tick_changes: u64,
    poly_bba: u64,
    binance_tickers: u64,
    binance_trades: u64,
    polybolt_events: u64,
    peak_poly_slots: usize,
    peak_binance_slots: usize,
    peak_oracle_slots: usize,
    ring_capacity: usize,
    health: &'a HealthSummaryCounts,
    clob_stats: &'a FeedStats,
    binance_stats: &'a FeedStats,
    polybolt_stats: &'a FeedStats,
}

pub(crate) fn truncate_token_id(id: &str) -> String {
    let trimmed = id.trim();
    if trimmed.len() > 14 {
        format!(
            "{}...{}",
            &trimmed[..6],
            &trimmed[trimmed.len().saturating_sub(6)..]
        )
    } else {
        trimmed.to_string()
    }
}

pub(crate) fn print_header_and_market(
    desc: &MarketDescriptor,
    sync: &ClockSyncReport,
    polybolt_enabled: bool,
    start_unix_ms: u64,
) {
    let t = theme::current();
    println!();
    println!(
        "{}",
        t.banner(
            "POLY-AS FEEDS & CAPTURE :: MARKET DISCOVERY, L2 BOOK & WS TELEMETRY",
            None
        )
    );
    println!("{}", t.section("(a) DISCOVERED 5M BTC MARKET & CLOCK SYNC"));
    println!(
        "{}",
        t.kv("Started:", &format_local_datetime(start_unix_ms))
    );

    let type_label = match (desc.version, desc.neg_risk) {
        (core_types::ProtocolVersion::CtfV1, false) => "Standard Binary CTF v1",
        (core_types::ProtocolVersion::CtfV1, true) => "CTF v1 (NegRisk)",
        (core_types::ProtocolVersion::PolyV2, false) => "Protocol v2 Binary",
        (core_types::ProtocolVersion::PolyV2, true) => "Protocol v2 (NegRisk)",
    };
    let market_display = if !desc.question.trim().is_empty() {
        format!("{} [{}] ({type_label})", desc.question, t.cyan(&desc.slug))
    } else {
        format!("{} ({type_label})", t.cyan(&desc.slug))
    };
    println!("{}", t.kv("Market:", &market_display));

    let cond_display = if desc.condition_id.len() > 18 {
        format!(
            "{}...{} (on-chain CTF condition)",
            &desc.condition_id[..10],
            &desc.condition_id[desc.condition_id.len().saturating_sub(8)..]
        )
    } else {
        format!("{} (on-chain CTF condition)", desc.condition_id)
    };
    println!("{}", t.kv("Condition ID:", &cond_display));

    let asset_up_trunc = truncate_token_id(&desc.asset_id_up);
    let asset_dn_trunc = truncate_token_id(&desc.asset_id_dn);
    println!(
        "{}",
        t.kv(
            "Asset Up / Down:",
            &format!("{asset_up_trunc} (Up) / {asset_dn_trunc} (Down) [ERC-1155 tokens]")
        )
    );

    let start_local = format_local_hms(desc.start_ts_ms);
    let expiry_local = format_local_hms(desc.expiry_ts_ms);
    let span_s = desc.expiry_ts_ms.saturating_sub(desc.start_ts_ms) / 1_000;
    let start_utc = format_utc_hms(desc.start_ts_ms);
    let expiry_utc = format_utc_hms(desc.expiry_ts_ms);
    println!(
        "{}",
        t.kv(
            "Window:",
            &format!(
                "{} -> {} local [{start_utc} -> {expiry_utc} UTC] ({}s span)",
                t.cyan(&start_local),
                t.cyan(&expiry_local),
                span_s
            )
        )
    );
    print_clock_sync_banner(sync, polybolt_enabled);
}

fn print_clock_sync_banner(sync: &ClockSyncReport, polybolt_enabled: bool) {
    let t = theme::current();
    let fmt_offset = |opt: Option<i64>| match opt {
        Some(ms) => format!("{ms}ms"),
        None => "n/a".to_string(),
    };
    let drift_label = if sync.drift_exceeded { "tripped" } else { "ok" };
    println!(
        "{}",
        t.kv(
            "Clock Offset:",
            &format!(
                "{}ms (source={}, clob={}, binance={}, drift={})",
                sync.offset_ms,
                sync.source,
                fmt_offset(sync.clob_offset_ms),
                fmt_offset(sync.binance_offset_ms),
                drift_label
            )
        )
    );
    if sync.drift_exceeded {
        println!(
            "  {} CLOCK WARNING: host drift {}ms exceeds limit; run `w32tm /resync` before live trading",
            t.badge(Badge::Warn),
            sync.offset_ms
        );
    }
    if let Some(err) = &sync.binance_error {
        println!(
            "  {} SYNC WARNING: Binance /time failed (fallback used): {err}",
            t.badge(Badge::Warn)
        );
    }
    let oracle_desc = if polybolt_enabled {
        "PolyBolt Chainlink (connecting external oracle)"
    } else {
        "Local Binance Spot 60s TWAP (canonical engine oracle)"
    };
    println!("{}", t.kv("Oracle:", oracle_desc));
}

pub(crate) fn format_feeds_progress_line(
    t: Theme,
    now_ms: u64,
    desc: &MarketDescriptor,
    window_idx: usize,
    max_windows: Option<usize>,
    data: &CaptureReportData,
) -> String {
    let now_local = format_local_hms(now_ms);
    let rem_s = desc.expiry_ts_ms.saturating_sub(now_ms) / 1_000;
    let expiry_local = format_local_hms(desc.expiry_ts_ms);

    let win_str = match max_windows {
        Some(max) => format!("Win {:>2}/{:<2}", window_idx, max),
        None => format!("Win #{:>3}", window_idx),
    };

    let clob_ev = data.poly_books + data.poly_price_changes + data.poly_trades + data.poly_bba;
    let bin_ev = data.binance_tickers + data.binance_trades;

    let bbo_str = {
        let (up_bid, up_ask) = match &data.latest_up_book {
            Some(up) => (
                Theme::fmt_cents(up.bids_slice().first().map(|l| l.price)),
                Theme::fmt_cents(up.asks_slice().first().map(|l| l.price)),
            ),
            None => ("--".to_string(), "--".to_string()),
        };
        let (dn_bid, dn_ask) = match &data.latest_dn_book {
            Some(dn) => (
                Theme::fmt_cents(dn.bids_slice().first().map(|l| l.price)),
                Theme::fmt_cents(dn.asks_slice().first().map(|l| l.price)),
            ),
            None => ("--".to_string(), "--".to_string()),
        };
        format!(
            "Up {:>5}/{:<5} · Dn {:>5}/{:<5}",
            up_bid, up_ask, dn_bid, dn_ask
        )
    };

    let btc_str = match data.latest_binance_px_e8 {
        Some(px) => format!("BTC ${:>8.2}", (px as f64) / 1e8),
        None => format!("{:>13}", "BTC --"),
    };

    format!(
        "  [{}] {}  {} (ends {}, {:>3}s left) {} CLOB: {:>8} {} Bin: {:>7} {} {} {} {}",
        t.muted(&now_local),
        t.badge(Badge::Live),
        t.cyan(&win_str),
        expiry_local,
        rem_s,
        t.bar(),
        clob_ev,
        t.bar(),
        bin_ev,
        t.bar(),
        bbo_str,
        t.bar(),
        t.bold(&btc_str)
    )
}

pub(crate) fn push_sample(buf: &mut Vec<String>, line: String) {
    if buf.len() < 4 {
        buf.push(line);
    }
}

pub(crate) fn format_binance_sample(ev: BinanceSpotEvent) -> String {
    match ev {
        BinanceSpotEvent::BookTicker {
            bid_e8,
            ask_e8,
            bid_qty_e8,
            ask_qty_e8,
            update_id,
            ..
        } => format!(
            "btcusdt@bookTicker uid={} bid=${:.2} (qty={:.4}) ask=${:.2} (qty={:.4})",
            update_id,
            bid_e8 as f64 / 1e8,
            bid_qty_e8 as f64 / 1e8,
            ask_e8 as f64 / 1e8,
            ask_qty_e8 as f64 / 1e8,
        ),
        BinanceSpotEvent::AggTrade {
            price_e8,
            qty_e8,
            is_buyer_maker,
            ..
        } => format!(
            "btcusdt@aggTrade   px=${:.2} qty={:.5} buyer_maker={}",
            price_e8 as f64 / 1e8,
            qty_e8 as f64 / 1e8,
            is_buyer_maker,
        ),
    }
}

fn fmt_opt_price(p: Option<MicroUsdc>) -> String {
    match p {
        Some(v) => format!("${v}"),
        None => "None".to_string(),
    }
}

pub(crate) fn format_poly_sample(ev: PolyMarketEvent) -> String {
    match ev {
        PolyMarketEvent::BookSnapshot { outcome, book, .. } => format!(
            "clob::book        {outcome:?} bids={} asks={} bbo={}/{}",
            book.bid_len(),
            book.ask_len(),
            fmt_opt_price(book.best_bid().map(|l| l.price)),
            fmt_opt_price(book.best_ask().map(|l| l.price)),
        ),
        PolyMarketEvent::PriceChange {
            outcome,
            side,
            price,
            size,
            best_bid,
            best_ask,
            ..
        } => format!(
            "clob::price_chg   {outcome:?} {side:?} ${price} x {size} (bbo={}/{})",
            fmt_opt_price(best_bid),
            fmt_opt_price(best_ask),
        ),
        PolyMarketEvent::LastTrade {
            outcome,
            side,
            price,
            size,
            ..
        } => format!("clob::trade       {outcome:?} {side:?} ${price} x {size}"),
        PolyMarketEvent::TickSizeChange {
            outcome,
            new_tick_micro,
            ..
        } => format!("clob::tick_size   {outcome:?} new_tick=${new_tick_micro}"),
        PolyMarketEvent::BestBidAsk {
            outcome,
            best_bid,
            best_ask,
            ..
        } => format!(
            "clob::bba         {outcome:?} bid={} ask={}",
            fmt_opt_price(best_bid),
            fmt_opt_price(best_ask),
        ),
    }
}

pub(crate) fn format_oracle_sample(ev: OracleTwapEvent) -> String {
    match ev {
        OracleTwapEvent::ChainlinkSpot { spot_e8, seq, .. } => {
            format!(
                "polybolt::spot    px=${:.2} seq={seq}",
                spot_e8 as f64 / 1e8
            )
        }
        OracleTwapEvent::ChainlinkTwap { twap_e8, seq, .. } => {
            format!(
                "polybolt::twap60  px=${:.2} seq={seq}",
                twap_e8 as f64 / 1e8
            )
        }
    }
}

pub(crate) fn apply_health_event(counts: &mut HealthSummaryCounts, ev: &FeedHealthEvent) {
    match ev {
        FeedHealthEvent::Connected { feed, .. } => set_state(counts, *feed, "Connected"),
        FeedHealthEvent::Ready { feed, .. } => set_state(counts, *feed, "Ready"),
        FeedHealthEvent::Disconnected { feed, .. } => {
            counts.disconnects = counts.disconnects.saturating_add(1);
            set_state(counts, *feed, "Disconnected");
        }
        FeedHealthEvent::ResyncingRest { feed, .. } => set_state(counts, *feed, "ResyncingRest"),
        FeedHealthEvent::MarketResolved { feed, .. } => {
            counts.market_resolved = true;
            set_state(counts, *feed, "MarketResolved");
        }
        FeedHealthEvent::StaleTimeout { feed, .. } => {
            counts.stale_timeouts = counts.stale_timeouts.saturating_add(1);
            set_state(counts, *feed, "StaleTimeout");
        }
        FeedHealthEvent::RingOverflow { .. } => {
            counts.ring_overflows = counts.ring_overflows.saturating_add(1);
        }
        FeedHealthEvent::ParseWarning { .. } => {
            counts.parse_warnings = counts.parse_warnings.saturating_add(1);
        }
    }
}

fn set_state(counts: &mut HealthSummaryCounts, feed: FeedId, label: &str) {
    match feed {
        FeedId::PolymarketClob => counts.clob_state = label.to_string(),
        FeedId::BinanceSpot => counts.binance_state = label.to_string(),
        FeedId::PolyBolt => counts.polybolt_state = label.to_string(),
    }
}

pub(crate) fn print_sections_b_to_e(
    data: &CaptureReportData,
    clob_stats: &FeedStats,
    binance_stats: &FeedStats,
    polybolt_stats: &FeedStats,
    polybolt_enabled: bool,
    jsonl_path: &Path,
) {
    print_section_b(data);
    print_section_c(data);
    print_section_d(data, clob_stats, binance_stats, polybolt_stats);
    print_section_e(
        clob_stats,
        binance_stats,
        polybolt_stats,
        polybolt_enabled,
        jsonl_path,
    );
}

fn print_section_b(data: &CaptureReportData) {
    let t = theme::current();
    println!();
    println!(
        "{}",
        t.section("(b) LIVE RAW -> PARSED FRAMES & EVENT TOTALS")
    );
    println!(
        "{}",
        t.kv(
            "Binance Totals:",
            &format!(
                "bookTicker={} | aggTrade={}",
                data.binance_tickers, data.binance_trades
            )
        )
    );
    println!(
        "{}",
        t.kv(
            "Polymarket Totals:",
            &format!(
                "book={} | price_change={} | trade={} | tick_size={} | bba={}",
                data.poly_books,
                data.poly_price_changes,
                data.poly_trades,
                data.poly_tick_changes,
                data.poly_bba
            )
        )
    );
    for s in &data.samples_binance {
        println!("{}", t.kv("[BINANCE SAMPLE]:", s));
    }
    for s in &data.samples_poly {
        println!("{}", t.kv("[POLY SAMPLE]:", s));
    }
}

fn print_section_c(data: &CaptureReportData) {
    let t = theme::current();
    println!();
    println!(
        "{}",
        t.section("(c) MAINTAINED L2 TOP-5 DEPTH & UP+DOWN PARITY")
    );
    print_book_side("UP", data.latest_up_book.as_ref());
    print_book_side("DOWN", data.latest_dn_book.as_ref());
    print_parity_check(data.latest_up_book.as_ref(), data.latest_dn_book.as_ref());
}

fn print_book_side(label: &str, book: Option<&FixedBook<10>>) {
    let t = theme::current();
    let Some(b) = book else {
        println!(
            "{}",
            t.kv(&format!("{label} Top-5:"), "<no snapshot received>")
        );
        return;
    };
    let bids: String = b
        .bids_slice()
        .iter()
        .take(5)
        .map(|l| format!("${}x{}", l.price, l.size))
        .collect::<Vec<_>>()
        .join(", ");
    let asks: String = b
        .asks_slice()
        .iter()
        .take(5)
        .map(|l| format!("${}x{}", l.price, l.size))
        .collect::<Vec<_>>()
        .join(", ");
    println!(
        "{}",
        t.kv(&format!("{label} Bids (Top 5):"), &format!("[{bids}]"))
    );
    println!(
        "{}",
        t.kv(&format!("{label} Asks (Top 5):"), &format!("[{asks}]"))
    );
}

fn print_parity_check(up: Option<&FixedBook<10>>, dn: Option<&FixedBook<10>>) {
    let t = theme::current();
    let (Some(u), Some(d)) = (up, dn) else {
        println!(
            "{}",
            t.kv("UP+DOWN Parity:", "pending both outcome snapshots")
        );
        return;
    };
    let (Some(ub), Some(ua), Some(db), Some(da)) =
        (u.best_bid(), u.best_ask(), d.best_bid(), d.best_ask())
    else {
        println!(
            "{}",
            t.kv("UP+DOWN Parity:", "one-sided book (missing BBO on a leg)")
        );
        return;
    };
    let implied_ask_up = MicroUsdc(ONE_DOLLAR_MICRO.saturating_sub(db.price.0));
    let combined_ask = ua.price.0.saturating_add(da.price.0);
    let combined_bid = ub.price.0.saturating_add(db.price.0);
    println!(
        "{}",
        t.kv(
            "UP+DOWN Parity:",
            &format!(
                "ask_up=${} vs (1-bid_dn)=${} | sum_bid=${} | sum_ask=${}",
                ua.price,
                implied_ask_up,
                MicroUsdc(combined_bid),
                MicroUsdc(combined_ask),
            )
        )
    );
}

fn style_health_state(state: &str) -> String {
    let t = theme::current();
    if state.eq_ignore_ascii_case("ready") || state.eq_ignore_ascii_case("active") {
        t.green(state)
    } else if state.eq_ignore_ascii_case("disabled") {
        t.muted(state)
    } else if state.contains("Stale") || state.contains("Disconnect") || state.contains("Error") {
        t.red(state)
    } else {
        t.yellow(state)
    }
}

fn print_section_d(
    data: &CaptureReportData,
    clob: &FeedStats,
    binance: &FeedStats,
    polybolt: &FeedStats,
) {
    let t = theme::current();
    let total_overflows = clob.ring_overflows + binance.ring_overflows + polybolt.ring_overflows;
    let total_warnings = clob.parse_warnings + binance.parse_warnings + polybolt.parse_warnings;
    let total_reconnects = clob.reconnects + binance.reconnects + polybolt.reconnects;
    let total_pongs = clob.pongs_received + binance.pongs_received + polybolt.pongs_received;
    let total_pings = clob.pings_replied + binance.pings_replied + polybolt.pings_replied;
    let health_drops =
        clob.health_events_dropped + binance.health_events_dropped + polybolt.health_events_dropped;
    println!();
    println!("{}", t.section("(d) FEED HEALTH & RINGBUFFER UTILIZATION"));
    println!(
        "{}",
        t.kv(
            "Health States:",
            &format!(
                "CLOB={} | Binance={} | PolyBolt={}",
                style_health_state(&data.health_counts.clob_state),
                style_health_state(&data.health_counts.binance_state),
                style_health_state(&data.health_counts.polybolt_state),
            )
        )
    );
    println!(
        "{}",
        t.kv(
            "Ring Utilization:",
            &format!(
                "poly_peak={}/{} | binance_peak={}/{} | oracle_peak={}/{}",
                data.peak_poly_slots,
                DEFAULT_RING_CAPACITY,
                data.peak_binance_slots,
                DEFAULT_RING_CAPACITY,
                data.peak_oracle_slots,
                DEFAULT_RING_CAPACITY,
            )
        )
    );
    println!(
        "{}",
        t.kv(
            "Counters:",
            &format!(
                "overflows={total_overflows} (clob={}, bin={}) | stale={} | warnings={total_warnings} | reconnects={total_reconnects} | pongs={total_pongs}/pings={total_pings} | health_drops={health_drops}",
                clob.ring_overflows, binance.ring_overflows, data.health_counts.stale_timeouts,
            )
        )
    );
}

fn print_section_e(
    clob: &FeedStats,
    binance: &FeedStats,
    polybolt: &FeedStats,
    polybolt_enabled: bool,
    jsonl_path: &Path,
) {
    let t = theme::current();
    println!();
    println!("{}", t.section("(e) INGRESS PARSE LATENCY (HDR HISTOGRAM)"));
    print_latency_line("Polymarket CLOB:", clob);
    print_latency_line("Binance Spot:", binance);
    if polybolt_enabled || polybolt.latency.is_some() {
        print_latency_line("PolyBolt Oracle:", polybolt);
    }
    println!(
        "{}",
        t.kv(
            "JSONL Telemetry:",
            &format!("written and closed at {}", jsonl_path.display())
        )
    );
    println!("{}", t.divider());
}

fn print_latency_line(label: &str, stats: &FeedStats) {
    let t = theme::current();
    let Some(l) = stats.latency else {
        println!("{}", t.kv(label, "0 parsed frames"));
        return;
    };
    println!(
        "{}",
        t.kv(
            label,
            &format!(
                "n={} (0ns={}) | P50={:.3} us | P95={:.3} us | P99={:.3} us | P99.9={:.3} us | max={:.3} us",
                l.count,
                l.zero_ns_samples,
                l.p50_ns as f64 / 1_000.0,
                l.p95_ns as f64 / 1_000.0,
                l.p99_ns as f64 / 1_000.0,
                l.p999_ns as f64 / 1_000.0,
                l.max_ns as f64 / 1_000.0,
            )
        )
    );
}

pub(crate) fn write_summary_event(
    jsonl: &mut JsonlWriter,
    clock: &MonotonicClock,
    slug: &str,
    data: &CaptureReportData,
    clob_stats: &FeedStats,
    binance_stats: &FeedStats,
    polybolt_stats: &FeedStats,
) -> Result<(), Box<dyn Error>> {
    jsonl.write_event(
        MonotonicClock::unix_now_ms()?,
        clock.now_ns()?,
        "feeds_summary",
        &FeedsSummaryRecord {
            market_slug: slug,
            poly_books: data.poly_books,
            poly_price_changes: data.poly_price_changes,
            poly_trades: data.poly_trades,
            poly_tick_changes: data.poly_tick_changes,
            poly_bba: data.poly_bba,
            binance_tickers: data.binance_tickers,
            binance_trades: data.binance_trades,
            polybolt_events: data.polybolt_events,
            peak_poly_slots: data.peak_poly_slots,
            peak_binance_slots: data.peak_binance_slots,
            peak_oracle_slots: data.peak_oracle_slots,
            ring_capacity: DEFAULT_RING_CAPACITY,
            health: &data.health_counts,
            clob_stats,
            binance_stats,
            polybolt_stats,
        },
    )?;
    Ok(())
}

struct ReplayTally {
    total_lines: u64,
    binance_count: u64,
    poly_count: u64,
    oracle_count: u64,
    non_monotonic: u64,
    inner_non_monotonic: u64,
}

struct ReplayState {
    max_outer_mono: u64,
    max_poly_rx: u64,
    max_binance_rx: u64,
    max_oracle_rx: u64,
    max_health_poly: u64,
    max_health_bin: u64,
    max_health_pb: u64,
    book_up: BookMaintainer,
    book_dn: BookMaintainer,
    tally: ReplayTally,
}

impl ReplayState {
    fn new() -> Self {
        Self {
            max_outer_mono: 0,
            max_poly_rx: 0,
            max_binance_rx: 0,
            max_oracle_rx: 0,
            max_health_poly: 0,
            max_health_bin: 0,
            max_health_pb: 0,
            book_up: BookMaintainer::new(Outcome::Up),
            book_dn: BookMaintainer::new(Outcome::Down),
            tally: ReplayTally {
                total_lines: 0,
                binance_count: 0,
                poly_count: 0,
                oracle_count: 0,
                non_monotonic: 0,
                inner_non_monotonic: 0,
            },
        }
    }
}

pub(crate) fn run_replay(
    replay_path: &Path,
    replay_out: Option<&Path>,
) -> Result<(), Box<dyn Error>> {
    if let Some(out_path) = replay_out
        && canonical_target_path(replay_path)? == canonical_target_path(out_path)?
    {
        return Err(format!(
            "--replay-out ({}) must not point to input capture file ({})",
            out_path.display(),
            replay_path.display()
        )
        .into());
    }
    let tally = tally_replay_lines(replay_path)?;
    if tally.total_lines == 0 {
        return Err(format!(
            "replay file {} contains 0 JSONL rows",
            replay_path.display()
        )
        .into());
    }
    if tally.non_monotonic > 0 || tally.inner_non_monotonic > 0 {
        return Err(format!(
            "monotonicity violation in {}: outer_non_mono={}, inner_non_mono={}",
            replay_path.display(),
            tally.non_monotonic,
            tally.inner_non_monotonic
        )
        .into());
    }
    if let Some(out_path) = replay_out {
        write_replay_report(replay_path, out_path, &tally)?;
    }
    println!(
        "  {} REPLAY VERIFIED: {} -> lines={} binance={} poly={} oracle={} non_monotonic={}",
        theme::current().badge(Badge::Pass),
        replay_path.display(),
        tally.total_lines,
        tally.binance_count,
        tally.poly_count,
        tally.oracle_count,
        tally.non_monotonic
    );
    Ok(())
}

fn write_replay_report(
    replay_path: &Path,
    out_path: &Path,
    tally: &ReplayTally,
) -> Result<(), Box<dyn Error>> {
    let clock = MonotonicClock::start();
    let mut writer = JsonlWriter::create(out_path)?;
    writer.write_event(
        MonotonicClock::unix_now_ms()?,
        clock.now_ns()?,
        "replay_verified",
        &serde_json::json!({
            "source": replay_path.display().to_string(),
            "total_lines": tally.total_lines,
            "binance_ticks": tally.binance_count,
            "poly_events": tally.poly_count,
            "oracle_events": tally.oracle_count,
            "non_monotonic_rows": tally.non_monotonic,
            "inner_non_monotonic_rows": tally.inner_non_monotonic,
        }),
    )?;
    writer.close()?;
    Ok(())
}

pub(crate) fn canonical_target_path(path: &Path) -> Result<PathBuf, Box<dyn Error>> {
    if path.exists() {
        return Ok(normalize_lexical(&std::fs::canonicalize(path)?));
    }
    let abs = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()?.join(path)
    };
    let norm = normalize_lexical(&abs);
    if let Some(parent) = norm.parent()
        && parent.exists()
        && let Some(name) = norm.file_name()
    {
        return Ok(normalize_lexical(
            &std::fs::canonicalize(parent)?.join(name),
        ));
    }
    Ok(norm)
}

fn normalize_lexical(path: &Path) -> PathBuf {
    let mut out = PathBuf::new();
    for comp in path.components() {
        match comp {
            Component::CurDir => {}
            Component::ParentDir => {
                if !out.pop() {
                    out.push(comp);
                }
            }
            other => out.push(other),
        }
    }
    out
}

#[derive(Deserialize)]
pub(crate) struct RawJsonlEnvelope {
    pub(crate) ts_unix_ms: u64,
    pub(crate) mono_ns: u64,
    pub(crate) event: String,
    pub(crate) payload: serde_json::Value,
}

#[derive(Deserialize)]
pub(crate) struct ReplayFixedBook {
    pub(crate) outcome: Outcome,
    pub(crate) bids: Vec<BookLevel>,
    pub(crate) asks: Vec<BookLevel>,
    pub(crate) last_update_ns: u64,
    pub(crate) exchange_ts_ms: u64,
}

#[derive(Deserialize)]
pub(crate) enum ReplayPolyEvent {
    BookSnapshot {
        outcome: Outcome,
        book: ReplayFixedBook,
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

#[derive(Deserialize)]
struct ReplayFeedsSummary {
    market_slug: String,
    health: HealthSummaryCounts,
    clob_stats: FeedStats,
    binance_stats: FeedStats,
    polybolt_stats: FeedStats,
}

fn tally_replay_lines(replay_path: &Path) -> Result<ReplayTally, Box<dyn Error>> {
    let reader = BufReader::new(File::open(replay_path)?);
    let mut state = ReplayState::new();
    for line_res in reader.lines() {
        let line = line_res?;
        if line.trim().is_empty() {
            continue;
        }
        let env: RawJsonlEnvelope = serde_json::from_str(&line)?;
        let _ = env.ts_unix_ms;
        state.tally.total_lines = state.tally.total_lines.saturating_add(1);
        if env.mono_ns < state.max_outer_mono {
            state.tally.non_monotonic = state.tally.non_monotonic.saturating_add(1);
        }
        state.max_outer_mono = state.max_outer_mono.max(env.mono_ns);
        validate_replay_event(&env.event, env.payload, &mut state)?;
    }
    state.book_up.check_uncrossed()?;
    state.book_dn.check_uncrossed()?;
    Ok(state.tally)
}

fn check_inner_mono(rx_ns: u64, max_so_far: &mut u64, state: &mut ReplayState) {
    if rx_ns < *max_so_far {
        state.tally.inner_non_monotonic = state.tally.inner_non_monotonic.saturating_add(1);
    }
    *max_so_far = (*max_so_far).max(rx_ns);
}

fn validate_replay_event(
    event: &str,
    payload: serde_json::Value,
    state: &mut ReplayState,
) -> Result<(), Box<dyn Error>> {
    match event {
        "market_discovered" => {
            serde_json::from_value::<MarketDescriptor>(payload)?.validate()?;
            // Live CLOB actors discard both books at rollover without requiring
            // the outgoing epoch to end uncrossed (transient crosses up to
            // 500ms are tolerated). Reset unconditionally so replay matches live.
            state.book_up = BookMaintainer::new(Outcome::Up);
            state.book_dn = BookMaintainer::new(Outcome::Down);
        }
        "clock_sync" => {
            serde_json::from_value::<ClockSyncReport>(payload)?;
        }
        "feed_health" => validate_replay_health(payload, state)?,
        "binance_tick" => validate_replay_binance(payload, state)?,
        "polybolt_event" => validate_replay_oracle(payload, state)?,
        "poly_book" | "poly_price_change" | "poly_trade" | "poly_tick_size" | "poly_bba" => {
            validate_replay_poly_event(event, payload, state)?;
        }
        "feeds_summary" => {
            let s: ReplayFeedsSummary = serde_json::from_value(payload)?;
            let _ = (
                &s.market_slug,
                &s.health,
                s.clob_stats,
                s.binance_stats,
                s.polybolt_stats,
            );
        }
        other => return Err(format!("unrecognized JSONL event tag `{other}`").into()),
    }
    Ok(())
}

fn validate_replay_health(
    payload: serde_json::Value,
    state: &mut ReplayState,
) -> Result<(), Box<dyn Error>> {
    let ev: FeedHealthEvent = serde_json::from_value(payload)?;
    let (feed, mono_ns) = health_feed_and_mono(&ev);
    let mut max_h = match feed {
        FeedId::PolymarketClob => state.max_health_poly,
        FeedId::BinanceSpot => state.max_health_bin,
        FeedId::PolyBolt => state.max_health_pb,
    };
    check_inner_mono(mono_ns, &mut max_h, state);
    match feed {
        FeedId::PolymarketClob => state.max_health_poly = max_h,
        FeedId::BinanceSpot => state.max_health_bin = max_h,
        FeedId::PolyBolt => state.max_health_pb = max_h,
    }
    Ok(())
}

fn validate_replay_binance(
    payload: serde_json::Value,
    state: &mut ReplayState,
) -> Result<(), Box<dyn Error>> {
    let ev: BinanceSpotEvent = serde_json::from_value(payload)?;
    let rx_ns = match ev {
        BinanceSpotEvent::BookTicker {
            bid_e8,
            ask_e8,
            rx_mono_ns,
            ..
        } => {
            if bid_e8 == 0 || ask_e8 < bid_e8 {
                return Err(format!("invalid replay binance BBO {bid_e8}/{ask_e8}").into());
            }
            rx_mono_ns
        }
        BinanceSpotEvent::AggTrade {
            price_e8,
            rx_mono_ns,
            ..
        } => {
            if price_e8 == 0 {
                return Err("invalid replay binance aggTrade price=0".into());
            }
            rx_mono_ns
        }
    };
    let mut max_rx = state.max_binance_rx;
    check_inner_mono(rx_ns, &mut max_rx, state);
    state.max_binance_rx = max_rx;
    state.tally.binance_count += 1;
    Ok(())
}

fn validate_replay_oracle(
    payload: serde_json::Value,
    state: &mut ReplayState,
) -> Result<(), Box<dyn Error>> {
    let ev: OracleTwapEvent = serde_json::from_value(payload)?;
    let rx_ns = match ev {
        OracleTwapEvent::ChainlinkSpot {
            spot_e8,
            rx_mono_ns,
            ..
        } => {
            if spot_e8 == 0 {
                return Err("invalid replay polybolt spot=0".into());
            }
            rx_mono_ns
        }
        OracleTwapEvent::ChainlinkTwap {
            twap_e8,
            rx_mono_ns,
            ..
        } => {
            if twap_e8 == 0 {
                return Err("invalid replay polybolt twap=0".into());
            }
            rx_mono_ns
        }
    };
    let mut max_rx = state.max_oracle_rx;
    check_inner_mono(rx_ns, &mut max_rx, state);
    state.max_oracle_rx = max_rx;
    state.tally.oracle_count += 1;
    Ok(())
}

fn health_feed_and_mono(ev: &FeedHealthEvent) -> (FeedId, u64) {
    match ev {
        FeedHealthEvent::Connected { feed, mono_ns, .. }
        | FeedHealthEvent::Disconnected { feed, mono_ns, .. }
        | FeedHealthEvent::ResyncingRest { feed, mono_ns, .. }
        | FeedHealthEvent::Ready { feed, mono_ns, .. }
        | FeedHealthEvent::MarketResolved { feed, mono_ns, .. }
        | FeedHealthEvent::StaleTimeout { feed, mono_ns, .. }
        | FeedHealthEvent::RingOverflow { feed, mono_ns, .. }
        | FeedHealthEvent::ParseWarning { feed, mono_ns, .. } => (*feed, *mono_ns),
    }
}

fn validate_replay_poly_event(
    tag: &str,
    payload: serde_json::Value,
    state: &mut ReplayState,
) -> Result<(), Box<dyn Error>> {
    let ev: ReplayPolyEvent = serde_json::from_value(payload)?;
    let rx_ns = check_poly_event_variant(tag, &ev, state)?;
    let mut max_rx = state.max_poly_rx;
    check_inner_mono(rx_ns, &mut max_rx, state);
    state.max_poly_rx = max_rx;
    state.tally.poly_count += 1;
    Ok(())
}

fn poly_event_tag(ev: &ReplayPolyEvent) -> &'static str {
    match ev {
        ReplayPolyEvent::BookSnapshot { .. } => "poly_book",
        ReplayPolyEvent::PriceChange { .. } => "poly_price_change",
        ReplayPolyEvent::LastTrade { .. } => "poly_trade",
        ReplayPolyEvent::TickSizeChange { .. } => "poly_tick_size",
        ReplayPolyEvent::BestBidAsk { .. } => "poly_bba",
    }
}

fn check_poly_event_variant(
    tag: &str,
    ev: &ReplayPolyEvent,
    state: &mut ReplayState,
) -> Result<u64, Box<dyn Error>> {
    if poly_event_tag(ev) != tag {
        return Err(format!("outer tag `{tag}` mismatches inner PolyMarketEvent variant").into());
    }
    match ev {
        ReplayPolyEvent::BookSnapshot {
            outcome,
            book,
            rx_mono_ns,
            exch_ts_ms,
        } => validate_replay_book_snap(*outcome, book, *rx_mono_ns, *exch_ts_ms, state),
        ReplayPolyEvent::PriceChange { .. } => validate_replay_price_change(ev, state),
        ReplayPolyEvent::LastTrade { .. }
        | ReplayPolyEvent::TickSizeChange { .. }
        | ReplayPolyEvent::BestBidAsk { .. } => validate_replay_misc_poly(ev),
    }
}

fn validate_replay_misc_poly(ev: &ReplayPolyEvent) -> Result<u64, Box<dyn Error>> {
    match ev {
        ReplayPolyEvent::LastTrade {
            outcome,
            side,
            price,
            size,
            rx_mono_ns,
            exch_ts_ms,
        } => {
            let _ = (*outcome, *side, *exch_ts_ms);
            if price.0 == 0 || price.0 >= ONE_DOLLAR_MICRO || size.0 == 0 {
                return Err("invalid replay poly_trade price or size".into());
            }
            Ok(*rx_mono_ns)
        }
        ReplayPolyEvent::TickSizeChange {
            outcome,
            new_tick_micro,
            rx_mono_ns,
            exch_ts_ms,
        } => {
            let _ = (*outcome, *exch_ts_ms);
            TickPrecision::from_micro(*new_tick_micro)?;
            Ok(*rx_mono_ns)
        }
        ReplayPolyEvent::BestBidAsk {
            outcome,
            best_bid,
            best_ask,
            rx_mono_ns,
            exch_ts_ms,
        } => {
            let _ = (*outcome, *exch_ts_ms);
            check_opt_poly_px(*best_bid)?;
            check_opt_poly_px(*best_ask)?;
            Ok(*rx_mono_ns)
        }
        ReplayPolyEvent::BookSnapshot { .. } | ReplayPolyEvent::PriceChange { .. } => {
            Err("unexpected variant in misc poly validator".into())
        }
    }
}

fn validate_replay_price_change(
    ev: &ReplayPolyEvent,
    state: &mut ReplayState,
) -> Result<u64, Box<dyn Error>> {
    let ReplayPolyEvent::PriceChange {
        outcome,
        side,
        price,
        size,
        best_bid,
        best_ask,
        rx_mono_ns,
        exch_ts_ms,
    } = ev
    else {
        return Err("expected PriceChange".into());
    };
    check_opt_poly_px(*best_bid)?;
    check_opt_poly_px(*best_ask)?;
    let m = pick_maintainer(*outcome, &mut state.book_up, &mut state.book_dn);
    m.apply_delta_allow_transient_cross(*side, *price, *size, *rx_mono_ns, *exch_ts_ms)?;
    Ok(*rx_mono_ns)
}

fn validate_replay_book_snap(
    outcome: Outcome,
    book: &ReplayFixedBook,
    rx_mono_ns: u64,
    exch_ts_ms: u64,
    state: &mut ReplayState,
) -> Result<u64, Box<dyn Error>> {
    if book.outcome != outcome {
        return Err("replay BookSnapshot outcome mismatch".into());
    }
    if book.bids.len() > 10 || book.asks.len() > 10 {
        return Err(format!(
            "replay BookSnapshot exceeds 10-level limit (bids={}, asks={})",
            book.bids.len(),
            book.asks.len()
        )
        .into());
    }
    FixedBook::<10>::try_from_slices(
        outcome,
        &book.bids,
        &book.asks,
        book.last_update_ns,
        book.exchange_ts_ms,
    )?;
    let m = pick_maintainer(outcome, &mut state.book_up, &mut state.book_dn);
    m.apply_snapshot(&book.bids, &book.asks, rx_mono_ns, exch_ts_ms)?;
    Ok(rx_mono_ns)
}

fn check_opt_poly_px(px: Option<MicroUsdc>) -> Result<(), Box<dyn Error>> {
    if let Some(p) = px
        && p.0 > ONE_DOLLAR_MICRO
    {
        return Err(format!("replay price {} exceeds $1.00", p.0).into());
    }
    Ok(())
}

fn pick_maintainer<'a>(
    outcome: Outcome,
    up: &'a mut BookMaintainer,
    dn: &'a mut BookMaintainer,
) -> &'a mut BookMaintainer {
    match outcome {
        Outcome::Up => up,
        Outcome::Down => dn,
    }
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
        std::env::temp_dir().join(format!("poly_as_replay_{tag}_{nanos}.jsonl"))
    }

    #[test]
    fn adversarial_replay_preserves_input_rejects_same_out_empty_unknown_and_non_monotonic() {
        let input = temp_path("ok");
        let sample = concat!(
            r#"{"ts_unix_ms":1000,"mono_ns":10,"event":"binance_tick","payload":{"BookTicker":{"bid_e8":9000000000000,"ask_e8":9000100000000,"bid_qty_e8":100000000,"ask_qty_e8":100000000,"update_id":1,"rx_mono_ns":5}}}"#,
            "\n"
        );
        std::fs::write(&input, sample).unwrap();
        let before = std::fs::read(&input).unwrap();

        // Replay must never overwrite or truncate its input file and must reject same path (even via .. through nonexistent dir).
        assert!(run_replay(&input, None).is_ok());
        assert_eq!(std::fs::read(&input).unwrap(), before);
        assert!(run_replay(&input, Some(&input)).is_err());
        if let Some(parent) = input.parent()
            && let Some(fname) = input.file_name()
        {
            let sneaky = parent.join("nonexistent_dir_xyz").join("..").join(fname);
            assert!(run_replay(&input, Some(&sneaky)).is_err());
        }

        // Tag vs variant mismatch and EOF crossed book must fail cleanly.
        let mismatch = temp_path("mismatch");
        let mm_line = r#"{"ts_unix_ms":1,"mono_ns":10,"event":"poly_book","payload":{"LastTrade":{"outcome":"Up","side":"Buy","price":500000,"size":1000000,"rx_mono_ns":5,"exch_ts_ms":1000}}}"#;
        std::fs::write(&mismatch, mm_line).unwrap();
        assert!(run_replay(&mismatch, None).is_err());

        let crossed = temp_path("crossed");
        let cr_lines = concat!(
            r#"{"ts_unix_ms":1,"mono_ns":10,"event":"poly_price_change","payload":{"PriceChange":{"outcome":"Up","side":"Buy","price":550000,"size":1000000,"best_bid":550000,"best_ask":560000,"rx_mono_ns":5,"exch_ts_ms":1000}}}"#,
            "\n",
            r#"{"ts_unix_ms":2,"mono_ns":11,"event":"poly_price_change","payload":{"PriceChange":{"outcome":"Up","side":"Sell","price":540000,"size":1000000,"best_bid":550000,"best_ask":540000,"rx_mono_ns":6,"exch_ts_ms":1001}}}"#,
            "\n"
        );
        std::fs::write(&crossed, cr_lines).unwrap();
        assert!(run_replay(&crossed, None).is_err());

        let _ = std::fs::remove_file(&mismatch);
        let _ = std::fs::remove_file(&crossed);

        // Multi-step non-monotonicity `100 -> 50 -> 60` must fail (not be overwritten before failing!).
        let non_mono = temp_path("nonmono");
        let nm_data = concat!(
            r#"{"ts_unix_ms":1000,"mono_ns":100,"event":"binance_tick","payload":{"BookTicker":{"bid_e8":9000000000000,"ask_e8":9000100000000,"bid_qty_e8":100000000,"ask_qty_e8":100000000,"update_id":1,"rx_mono_ns":10}}}"#,
            "\n",
            r#"{"ts_unix_ms":1001,"mono_ns":50,"event":"binance_tick","payload":{"BookTicker":{"bid_e8":9000000000000,"ask_e8":9000100000000,"bid_qty_e8":100000000,"ask_qty_e8":100000000,"update_id":2,"rx_mono_ns":11}}}"#,
            "\n",
            r#"{"ts_unix_ms":1002,"mono_ns":60,"event":"binance_tick","payload":{"BookTicker":{"bid_e8":9000000000000,"ask_e8":9000100000000,"bid_qty_e8":100000000,"ask_qty_e8":100000000,"update_id":3,"rx_mono_ns":12}}}"#,
            "\n"
        );
        std::fs::write(&non_mono, nm_data).unwrap();
        let err = run_replay(&non_mono, None).unwrap_err().to_string();
        assert!(err.contains("outer_non_mono=2"));

        // Empty file and unknown event tags must fail cleanly.
        let empty = temp_path("empty");
        std::fs::write(&empty, "").unwrap();
        assert!(run_replay(&empty, None).is_err());

        let unknown = temp_path("unknown");
        std::fs::write(
            &unknown,
            r#"{"ts_unix_ms":1,"mono_ns":1,"event":"bogus","payload":{}}"#,
        )
        .unwrap();
        assert!(run_replay(&unknown, None).is_err());

        let _ = std::fs::remove_file(&input);
        let _ = std::fs::remove_file(&non_mono);
        let _ = std::fs::remove_file(&empty);
        let _ = std::fs::remove_file(&unknown);
    }

    #[test]
    fn adversarial_replay_rejects_book_snapshot_exceeding_10_levels() {
        let too_deep = temp_path("over10");
        let bids = (0..11)
            .map(|i| {
                let px = 500_000_u64 - (i as u64) * 10_000;
                format!(r#"{{"price":{px},"size":1000000}}"#)
            })
            .collect::<Vec<_>>()
            .join(",");
        let line = format!(
            r#"{{"ts_unix_ms":1,"mono_ns":10,"event":"poly_book","payload":{{"BookSnapshot":{{"outcome":"Up","book":{{"outcome":"Up","bids":[{bids}],"asks":[{{"price":510000,"size":1000000}}],"last_update_ns":10,"exchange_ts_ms":1000}},"rx_mono_ns":10,"exch_ts_ms":1000}}}}}}"#
        );
        std::fs::write(&too_deep, line).unwrap();
        let err = run_replay(&too_deep, None).unwrap_err().to_string();
        let _ = std::fs::remove_file(&too_deep);
        assert!(err.contains("exceeds 10-level limit"), "err={err}");
        assert!(err.contains("bids=11"), "err={err}");
    }

    #[test]
    fn multi_epoch_capture_passes_replay_validation() {
        use core_types::{FeeSchedule, ProtocolVersion};

        let multi = temp_path("multi_epoch");
        let desc1 = MarketDescriptor {
            condition_id: "0x1111111111111111111111111111111111111111111111111111111111111111"
                .to_string(),
            slug: "btc-updown-5m-1000".to_string(),
            question: "BTC 5m epoch 1".to_string(),
            version: ProtocolVersion::PolyV2,
            neg_risk: false,
            asset_id_up: "1001".to_string(),
            asset_id_dn: "1002".to_string(),
            tick_size_micro: MicroUsdc(10_000),
            min_order_size_micro: SharesMicro(5_000_000),
            taker_delay_ms: 50,
            fee_schedule: FeeSchedule::btc_5m_default(),
            start_ts_ms: 1_000_000,
            expiry_ts_ms: 1_300_000,
            strike_price_usd: None,
        };
        let desc2 = MarketDescriptor {
            condition_id: "0x2222222222222222222222222222222222222222222222222222222222222222"
                .to_string(),
            slug: "btc-updown-5m-1300".to_string(),
            question: "BTC 5m epoch 2".to_string(),
            asset_id_up: "2001".to_string(),
            asset_id_dn: "2002".to_string(),
            start_ts_ms: 1_300_000,
            expiry_ts_ms: 1_600_000,
            ..desc1.clone()
        };

        let mut writer = JsonlWriter::create(&multi).unwrap();
        writer
            .write_event(1_000_000, 10, "market_discovered", &desc1)
            .unwrap();
        let snap1 = PolyMarketEvent::BookSnapshot {
            outcome: Outcome::Up,
            book: FixedBook::try_from_slices(
                Outcome::Up,
                &[BookLevel {
                    price: MicroUsdc(480_000),
                    size: SharesMicro(10_000_000),
                }],
                &[BookLevel {
                    price: MicroUsdc(490_000),
                    size: SharesMicro(10_000_000),
                }],
                10,
                1_000_000,
            )
            .unwrap(),
            rx_mono_ns: 20,
            exch_ts_ms: 1_000_000,
            epoch: 0,
        };
        writer
            .write_event(1_000_005, 20, "poly_book", &snap1)
            .unwrap();
        // Transient cross at the epoch boundary: live tolerates crosses up to
        // 500ms before resync, and discards the book at rollover. Replay must
        // accept this and reset instead of rejecting the capture.
        let crossed_delta = PolyMarketEvent::PriceChange {
            outcome: Outcome::Up,
            side: OrderSide::Buy,
            price: MicroUsdc(550_000),
            size: SharesMicro(10_000_000),
            best_bid: None,
            best_ask: None,
            rx_mono_ns: 25,
            exch_ts_ms: 1_000_010,
            epoch: 0,
        };
        writer
            .write_event(1_000_010, 25, "poly_price_change", &crossed_delta)
            .unwrap();
        writer
            .write_event(1_300_000, 30, "market_discovered", &desc2)
            .unwrap();
        // Second epoch starts from a delta, not a snapshot, so the test depends
        // on the reset: bid 510k crosses the old ask 490k when the reset is
        // missing, and is uncrossed (empty book) after the reset.
        let epoch2_delta = PolyMarketEvent::PriceChange {
            outcome: Outcome::Up,
            side: OrderSide::Buy,
            price: MicroUsdc(510_000),
            size: SharesMicro(10_000_000),
            best_bid: None,
            best_ask: None,
            rx_mono_ns: 40,
            exch_ts_ms: 1_300_000,
            epoch: 1,
        };
        writer
            .write_event(1_300_005, 40, "poly_price_change", &epoch2_delta)
            .unwrap();
        writer.close().unwrap();

        let res = run_replay(&multi, None);
        let _ = std::fs::remove_file(&multi);
        assert!(res.is_ok(), "multi-epoch replay validation failed: {res:?}");
    }

    #[test]
    fn test_truncate_token_id() {
        assert_eq!(truncate_token_id("123"), "123");
        let long_id =
            "95711136203495911997127094189614164136721183539547019195760563976669053234852";
        assert_eq!(truncate_token_id(long_id), "957111...234852");
    }

    #[test]
    fn test_feeds_progress_line_contains_live_and_bbo() {
        use core_types::{FeeSchedule, ProtocolVersion};

        let desc = MarketDescriptor {
            condition_id: "0x123".to_string(),
            slug: "btc-updown-5m-1000".to_string(),
            question: "BTC 5m".to_string(),
            version: ProtocolVersion::PolyV2,
            neg_risk: false,
            asset_id_up: "1".to_string(),
            asset_id_dn: "2".to_string(),
            tick_size_micro: MicroUsdc(10_000),
            min_order_size_micro: SharesMicro(5_000_000),
            taker_delay_ms: 50,
            fee_schedule: FeeSchedule::btc_5m_default(),
            start_ts_ms: 1_000_000,
            expiry_ts_ms: 1_300_000,
            strike_price_usd: None,
        };
        let mut data = CaptureReportData::new("Connected".to_string());
        data.poly_books = 10;
        data.binance_tickers = 25;
        data.latest_binance_px_e8 = Some(9_845_050_000_000);

        let line = format_feeds_progress_line(Theme::plain(), 1_100_000, &desc, 1, Some(3), &data);
        assert!(line.contains("[LIVE]"));
        assert!(line.starts_with("  ["));
        assert!(line.contains("] [LIVE]"));
        assert!(line.contains("Win  1/3"));
        assert!(line.contains("CLOB:       10"));
        assert!(line.contains("Bin:      25"));
        assert!(line.contains("BTC $98450.50"));
    }
}
