use std::error::Error;
use std::path::Path;

use core_types::{
    EngineConfig, FixedBook, JsonlWriter, LatencySummary, MarketDescriptor, MicroUsdc,
    MonotonicClock,
};
use math_engine::{PipelineTrace, QuoteOutput};
use serde::Serialize;

#[derive(Default)]
pub(crate) struct TraceReportStats {
    pub(crate) warmup_ticks: u64,
    pub(crate) evaluated_ticks: usize,
    pub(crate) suppressed_ticks: u64,
    pub(crate) kappa_fallbacks: u64,
    pub(crate) defensive_cancels: u64,
    pub(crate) total_compute_ns_sum: u64,
}

#[derive(Serialize)]
struct MathTraceEnvelope<'a> {
    tick_index: usize,
    source: &'a str,
    strike_source: &'a str,
    market_slug: &'a str,
    gamma: f64,
    fee_rate_e6: u64,
    v_bar: Option<f64>,
    up_bid_levels: u8,
    up_ask_levels: u8,
    #[serde(flatten)]
    trace: PipelineTrace,
}

#[derive(Serialize)]
struct TraceSummaryEnvelope<'a> {
    source: &'a str,
    market_slug: &'a str,
    strike_source: &'a str,
    warmup_ticks: u64,
    evaluated_ticks: usize,
    suppressed_ticks: u64,
    kappa_fallbacks: u64,
    defensive_cancels: u64,
    latency: LatencySummary,
    total_compute_ns: u64,
}

pub(crate) fn print_trace_header(
    source: &str,
    desc: &MarketDescriptor,
    cfg: &EngineConfig,
    strike_usd: Option<f64>,
    strike_source: &str,
    jsonl_path: &Path,
) {
    println!("=== POLY-AS PHASE 2 MATH ENGINE TRACE ({source}) ===");
    println!(
        "  (a) Market           : {} | ver={:?} | tick=${} | min_size={} | fee_rate={}",
        desc.slug,
        desc.version,
        desc.tick_size_micro,
        desc.min_order_size_micro,
        desc.fee_schedule.fee_rate_e6 as f64 / 1_000_000.0
    );
    println!(
        "  (a) Modes & Strike   : sg={:?} | kappa={:?} | var={:?} | gamma={} | strike={} ({})",
        cfg.sg_mode,
        cfg.kappa_mode,
        cfg.variance_mode,
        cfg.gamma,
        opt_usd(strike_usd),
        strike_source
    );
    println!("  (a) Telemetry JSONL  : {}", jsonl_path.display());
    println!();
}

pub(crate) fn record_and_print_tick(
    idx: usize,
    source: &str,
    strike_source: &str,
    (desc, cfg, up_book, dn_book): (
        &MarketDescriptor,
        &EngineConfig,
        &FixedBook<10>,
        &FixedBook<10>,
    ),
    trace: PipelineTrace,
    (stats, jsonl, clock): (&mut TraceReportStats, &mut JsonlWriter, &MonotonicClock),
) -> Result<(), Box<dyn Error>> {
    stats.evaluated_ticks = stats.evaluated_ticks.saturating_add(1);
    stats.total_compute_ns_sum = stats
        .total_compute_ns_sum
        .saturating_add(trace.total_compute_ns);
    if matches!(trace.quote, QuoteOutput::Suppressed(_)) {
        stats.suppressed_ticks = stats.suppressed_ticks.saturating_add(1);
    }
    if trace.flags.kappa_fallback {
        stats.kappa_fallbacks = stats.kappa_fallbacks.saturating_add(1);
    }
    if trace.flags.defensive_cancel {
        stats.defensive_cancels = stats.defensive_cancels.saturating_add(1);
    }

    print_tick_trace(idx, source, (desc, cfg, up_book, dn_book), &trace);
    let v_bar = trace.kappa.map(|k| k.v_bar);
    let env = MathTraceEnvelope {
        tick_index: idx,
        source,
        strike_source,
        market_slug: &desc.slug,
        gamma: cfg.gamma,
        fee_rate_e6: desc.fee_schedule.fee_rate_e6,
        v_bar,
        up_bid_levels: u8::try_from(up_book.bid_len()).unwrap_or(10),
        up_ask_levels: u8::try_from(up_book.ask_len()).unwrap_or(10),
        trace,
    };
    jsonl.write_event(trace.now_ms, clock.now_ns()?, "math_trace", &env)?;
    Ok(())
}

pub(crate) fn print_tick_trace(
    idx: usize,
    source: &str,
    (desc, cfg, up_book, dn_book): (
        &MarketDescriptor,
        &EngineConfig,
        &FixedBook<10>,
        &FixedBook<10>,
    ),
    tr: &PipelineTrace,
) {
    println!("--- Tick #{idx:02} ({source}) now_ms={} ---", tr.now_ms);
    println!(
        "  (a) BBO & Spot       : UP={}/{} | DN={}/{} | btc_spot={}",
        opt_micro(tr.best_bid_up),
        opt_micro(tr.best_ask_up),
        opt_micro(tr.best_bid_dn),
        opt_micro(tr.best_ask_dn),
        opt_usd(tr.spot_usd),
    );
    let (Some(sg), Some(s2), Some(kap), Some(pr)) = (tr.sg, tr.station2, tr.kappa, tr.pricer)
    else {
        println!(
            "  (c-d) Suppressed     : {:?} | total={}ns",
            tr.quote, tr.total_compute_ns
        );
        return;
    };
    println!(
        "  (b) Inputs           : raw_mid={:.6} q={:.2} gamma={} tau={:.1}s tick=${} fee={} v_bar={:.1} depth_up={}/{} dn={}/{}",
        sg.raw_mid,
        tr.inventory_q.to_shares_f64(),
        cfg.gamma,
        s2.tau_secs,
        desc.tick_size_micro,
        desc.fee_schedule.fee_rate_e6 as f64 / 1_000_000.0,
        kap.v_bar,
        up_book.bid_len().min(5),
        up_book.ask_len().min(5),
        dn_book.bid_len().min(5),
        dn_book.ask_len().min(5),
    );
    print_tick_stations(&sg, &s2, &kap, &pr);
    print_tick_outputs_and_timing(tr, s2.lead_lag.trigger_defensive_cancel);
}

fn print_tick_stations(
    sg: &math_engine::SgOutput,
    s2: &math_engine::Station2Output,
    kap: &math_engine::KappaOutput,
    pr: &math_engine::PricerTrace,
) {
    println!(
        "  (c) S1 SG -> S2 -> S3: s_i={:.6}(clamp={}) | twap60={} v_rem={:.6} p*={} | kappa={:.2}(raw={},fb={},lvl={})",
        sg.s_i,
        sg.clamped_to_bbo,
        opt_usd(s2.twap_60s_usd),
        s2.v_rem,
        opt_prob(s2.lead_lag.p_spot_star),
        kap.kappa,
        opt_prob(kap.raw_kappa),
        kap.fallback_used,
        kap.levels_used,
    );
    println!(
        "  (c) S4 -> S5 -> S6   : s*={:.6}(min={:.4}) | r_i={:.6} | raw={:.4}/{:.4} floor={}/{} po_clamp={}/{}",
        pr.s_star,
        pr.s_min,
        pr.r_i,
        pr.bid_up_raw,
        pr.bid_dn_raw,
        opt_micro(pr.bid_up_floored),
        opt_micro(pr.bid_dn_floored),
        pr.post_only_clamped_up,
        pr.post_only_clamped_dn,
    );
}

fn print_tick_outputs_and_timing(tr: &PipelineTrace, lead_cancel: bool) {
    match tr.quote {
        QuoteOutput::Active {
            bid_up,
            bid_dn,
            pair_cost_micro,
            gross_margin_bps,
        } => println!(
            "  (d) Quote Output     : bid_up=${} bid_dn=${} pair_cost=${} margin={}bps defensive_cancel={}",
            opt_micro(bid_up),
            opt_micro(bid_dn),
            opt_micro(pair_cost_micro),
            gross_margin_bps.map_or_else(|| "none".to_string(), |b| b.to_string()),
            tr.flags.defensive_cancel || lead_cancel,
        ),
        QuoteOutput::Suppressed(reason) => println!(
            "  (d) Quote Output     : SUPPRESSED({reason:?}) defensive_cancel={}",
            tr.flags.defensive_cancel || lead_cancel
        ),
    }
    let tm = tr.timings;
    println!(
        "  (e) Latency (ns)     : s1={} s2={} s3={} s4={} s5={} s6={} | total={}ns ({:.3}us)",
        tm.s1_ns,
        tm.s2_ns,
        tm.s3_ns,
        tm.s4_ns,
        tm.s5_ns,
        tm.s6_ns,
        tr.total_compute_ns,
        tr.total_compute_ns as f64 / 1000.0
    );
}

pub(crate) fn write_and_print_summary(
    source: &str,
    slug: &str,
    strike_source: &str,
    stats: &TraceReportStats,
    summary: LatencySummary,
    (jsonl, clock, jsonl_path): (&mut JsonlWriter, &MonotonicClock, &Path),
) -> Result<(), Box<dyn Error>> {
    print_summary_stdout(strike_source, stats, summary, jsonl_path);
    let rec = TraceSummaryEnvelope {
        source,
        market_slug: slug,
        strike_source,
        warmup_ticks: stats.warmup_ticks,
        evaluated_ticks: stats.evaluated_ticks,
        suppressed_ticks: stats.suppressed_ticks,
        kappa_fallbacks: stats.kappa_fallbacks,
        defensive_cancels: stats.defensive_cancels,
        latency: summary,
        total_compute_ns: stats.total_compute_ns_sum,
    };
    jsonl.write_event(
        MonotonicClock::unix_now_ms()?,
        clock.now_ns()?,
        "trace_summary",
        &rec,
    )?;
    Ok(())
}

fn print_summary_stdout(
    strike_source: &str,
    stats: &TraceReportStats,
    summary: LatencySummary,
    jsonl_path: &Path,
) {
    println!();
    println!("=== PHASE 2 TRACE SUMMARY ===");
    println!(
        "  Ticks                : evaluated={} warmup={} suppressed={} kappa_fb={} def_cancel={}",
        stats.evaluated_ticks,
        stats.warmup_ticks,
        stats.suppressed_ticks,
        stats.kappa_fallbacks,
        stats.defensive_cancels
    );
    println!(
        "  HDR Compute Latency  : count={} min={}ns P50={:.3}us P95={:.3}us P99={:.3}us max={:.3}us",
        summary.count,
        summary.min_ns,
        summary.p50_ns as f64 / 1000.0,
        summary.p95_ns as f64 / 1000.0,
        summary.p99_ns as f64 / 1000.0,
        summary.max_ns as f64 / 1000.0
    );
    println!(
        "  Strike Source        : {strike_source} | JSONL={}",
        jsonl_path.display()
    );
}

fn opt_micro(val: Option<MicroUsdc>) -> String {
    val.map_or_else(|| "none".to_string(), |v| v.to_string())
}

fn opt_usd(val: Option<f64>) -> String {
    val.map_or_else(|| "none".to_string(), |v| format!("${v:.2}"))
}

fn opt_prob(val: Option<f64>) -> String {
    val.map_or_else(|| "none".to_string(), |v| format!("{v:.4}"))
}
