use core_types::{JsonlWriter, LatencySummary};
use serde::Serialize;
use std::path::Path;

#[derive(Debug, Default)]
pub(crate) struct ExecReportStats {
    pub(crate) ticks: usize,
    pub(crate) active_quotes: usize,
    pub(crate) signed_orders: usize,
}

#[derive(Serialize)]
struct SignedOrderEvent<'a> {
    tick: usize,
    leg: &'a str,
    price_micro: u64,
    maker_amount: String,
    taker_amount: String,
    signature_prefix: String,
    recovered_match: bool,
}

#[derive(Serialize)]
struct LatencyEvent {
    tick: usize,
    tau1_ingress_ns: u64,
    tau2_compute_ns: u64,
    tau3_sign_ns: u64,
    tau4_egress_ns: u64,
}

#[derive(Serialize)]
struct SettlementPreviewEvent<'a> {
    target: &'a str,
    calldata: &'a str,
    pairable_micro: u64,
    decision: &'a str,
}

#[derive(Serialize)]
struct SummaryEvent {
    ticks: usize,
    active_quotes: usize,
    signed_orders: u64,
    tau3_p50_us: u64,
    tau3_p99_us: u64,
    canary_latch: &'static str,
}

pub(crate) fn print_exec_header(mode: &str, jsonl: &Path, canary_max_usd: u64) {
    println!();
    println!("================================================================================");
    println!("  POLYMARKET 5-MINUTE BTC HFT BOT -- PHASE 5 LIVE EXECUTION (DRY-RUN TRACE)");
    println!("================================================================================");
    println!("  Mode:               {mode}");
    println!("  Canary Cap:         ${canary_max_usd}");
    println!("  JSONL Telemetry:    {}", jsonl.display());
    println!(
        "  Safety Latch:       Dry-run never touches CLOB; canary needs --confirm-live-capital"
    );
    println!("--------------------------------------------------------------------------------");
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn record_signed_order(
    jsonl: &mut JsonlWriter,
    ts_ms: u64,
    mono_ns: u64,
    tick: usize,
    leg: &str,
    price_micro: u64,
    maker_amount: &str,
    taker_amount: &str,
    signature_hex: &str,
    recovered_match: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let prefix_len = signature_hex.len().min(18);
    let prefix = signature_hex
        .get(..prefix_len)
        .unwrap_or(signature_hex)
        .to_string();
    jsonl.write_event(
        ts_ms,
        mono_ns,
        "signed_order",
        &SignedOrderEvent {
            tick,
            leg,
            price_micro,
            maker_amount: maker_amount.to_string(),
            taker_amount: taker_amount.to_string(),
            signature_prefix: prefix,
            recovered_match,
        },
    )?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn record_latency(
    jsonl: &mut JsonlWriter,
    ts_ms: u64,
    mono_ns: u64,
    tick: usize,
    t1: u64,
    t2: u64,
    t3: u64,
    t4: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    jsonl.write_event(
        ts_ms,
        mono_ns,
        "latency_checkpoints",
        &LatencyEvent {
            tick,
            tau1_ingress_ns: t1,
            tau2_compute_ns: t2,
            tau3_sign_ns: t3,
            tau4_egress_ns: t4,
        },
    )?;
    Ok(())
}

pub(crate) fn record_settlement_preview(
    jsonl: &mut JsonlWriter,
    ts_ms: u64,
    mono_ns: u64,
    target: &str,
    calldata: &str,
    pairable_micro: u64,
    decision: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    jsonl.write_event(
        ts_ms,
        mono_ns,
        "settlement_preview",
        &SettlementPreviewEvent {
            target,
            calldata,
            pairable_micro,
            decision,
        },
    )?;
    Ok(())
}

pub(crate) fn print_exec_summary(
    stats: &ExecReportStats,
    sign_summary: &LatencySummary,
    settlement_calldata: &str,
) {
    println!("--------------------------------------------------------------------------------");
    println!(
        "  Ticks: {}  Active quotes: {}  Signed orders: {}",
        stats.ticks, stats.active_quotes, stats.signed_orders
    );
    println!(
        "  tau_3 signing P50: {}us  P99: {}us  (budget < 80us release, < 10ms debug)",
        sign_summary.p50_ns / 1000,
        sign_summary.p99_ns / 1000
    );
    println!("  Settlement preview: {settlement_calldata}");
    println!("  Secrets: no full secrets written (signature prefixes only, ephemeral keys)");
}

pub(crate) fn record_summary_jsonl(
    jsonl: &mut JsonlWriter,
    ts_ms: u64,
    mono_ns: u64,
    stats: &ExecReportStats,
    sign_summary: &LatencySummary,
) -> Result<(), Box<dyn std::error::Error>> {
    jsonl.write_event(
        ts_ms,
        mono_ns,
        "exec_summary",
        &SummaryEvent {
            ticks: stats.ticks,
            active_quotes: stats.active_quotes,
            signed_orders: stats.signed_orders as u64,
            tau3_p50_us: sign_summary.p50_ns / 1000,
            tau3_p99_us: sign_summary.p99_ns / 1000,
            canary_latch: "dry-run-no-capital-at-risk",
        },
    )?;
    Ok(())
}
