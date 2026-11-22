use std::error::Error;
use std::path::Path;

use core_types::{
    JsonlWriter, LatencySummary, MarketDescriptor, MicroUsdc, MonotonicClock, SharesMicro,
    SignedSharesMicro,
};
use paper_sim::{MergeEvent, SimFill, SimOrder, TrajectorySnapshot};
use serde::Serialize;

use crate::theme::{self, Badge, Theme};

#[derive(Debug, Clone)]
pub(crate) struct PaperReportStats {
    pub(crate) total_ticks: usize,
    pub(crate) orders_placed: u64,
    pub(crate) orders_cancelled: u64,
    pub(crate) fills_count: u64,
    pub(crate) merges_count: u64,
    pub(crate) total_pairs_merged: SharesMicro,
    pub(crate) total_compute_ns_sum: u64,
    pub(crate) epoch_rollovers: u64,
}

impl Default for PaperReportStats {
    fn default() -> Self {
        Self {
            total_ticks: 0,
            orders_placed: 0,
            orders_cancelled: 0,
            fills_count: 0,
            merges_count: 0,
            total_pairs_merged: SharesMicro::ZERO,
            total_compute_ns_sum: 0,
            epoch_rollovers: 0,
        }
    }
}

#[derive(Serialize)]
struct PaperFillJsonl<'a> {
    market_slug: &'a str,
    sim_id: u64,
    outcome: &'a str,
    side: &'a str,
    price_micro: u64,
    filled_shares_micro: u64,
    fill_ts_ms: u64,
    is_maker: bool,
}

#[derive(Serialize)]
struct PaperMergeJsonl<'a> {
    market_slug: &'a str,
    merged_pairs_micro: u64,
    avg_cost_up_micro: u64,
    avg_cost_dn_micro: u64,
    pair_cost_micro: u64,
    realized_gain_micro: i64,
    total_merged_micro: u64,
}

#[derive(Serialize)]
struct PaperOrderJsonl<'a> {
    market_slug: &'a str,
    sim_id: u64,
    action: &'a str,
    outcome: &'a str,
    side: &'a str,
    price_micro: u64,
    size_micro: u64,
    volume_ahead_micro: u64,
    now_ms: u64,
}

#[derive(Serialize)]
struct PaperSummaryJsonl<'a> {
    source: &'a str,
    market_slug: &'a str,
    total_ticks: usize,
    epoch_rollovers: u64,
    q_up_micro: u64,
    q_dn_micro: u64,
    net_q_micro: i64,
    pairs_merged_micro: u64,
    realized_pnl_micro: i64,
    unrealized_pnl_micro: i64,
    net_pnl_micro: i64,
    fees_paid_micro: u64,
    rpm_60s: usize,
    fill_ratio: f64,
    post_fill_markout_1s_bps: f64,
    post_fill_markout_5s_bps: f64,
    baseline_pnl_micro: i64,
    edge_pnl_micro: i64,
    compute_latency: LatencySummary,
}

#[derive(Serialize)]
struct PaperTickJsonl<'a> {
    tick_index: usize,
    source: &'a str,
    market_slug: &'a str,
    now_ms: u64,
    #[serde(flatten)]
    trajectory: TrajectorySnapshot,
}

pub(crate) fn print_paper_header(
    source: &str,
    desc: &MarketDescriptor,
    v_max: u64,
    base_size: u64,
    latency_ms: u64,
    duration: &str,
    jsonl_path: &Path,
) {
    let t = theme::current();
    println!();
    println!(
        "{}",
        t.banner(
            &format!("POLY-AS PHASE 3 :: FIFO PAPER SIMULATOR ({source})"),
            None
        )
    );
    println!(
        "{}",
        t.kv(
            "Market & Epoch:",
            &format!(
                "{} | ver={:?} | tick=${} | min_size={}",
                t.cyan(&desc.slug),
                desc.version,
                desc.tick_size_micro,
                desc.min_order_size_micro
            )
        )
    );
    println!(
        "{}",
        t.kv(
            "Capital & Sizing:",
            &format!("V_max=${v_max} | base_size={base_size} shares | duration={duration}")
        )
    );
    println!(
        "{}",
        t.kv(
            "Wire Latency:",
            &format!("{latency_ms}ms placement / {latency_ms}ms cancel (FIFO queue-aware)")
        )
    );
    println!(
        "{}",
        t.kv("Telemetry JSONL:", &jsonl_path.display().to_string())
    );
    println!("{}", t.divider());
}

pub(crate) fn print_order_event(action: &str, order: &SimOrder) {
    let t = theme::current();
    let outcome_str = match order.outcome {
        core_types::Outcome::Up => t.bold_green("UP"),
        core_types::Outcome::Down => t.bold_cyan("DN"),
    };
    let action_styled = match action {
        "CANCEL" | "CANCELLED" => t.yellow(&format!("{action:<7}")),
        _ => t.cyan(&format!("{action:<7}")),
    };
    let side_str = order.side.as_str();
    println!(
        "  {} #{:<4} {} | {} {:<4} @ {} | sz={:<6} | vol_ahead={:<7} | status={}",
        t.badge(Badge::Order),
        order.sim_id,
        action_styled,
        outcome_str,
        side_str,
        Theme::fmt_price_compact(Some(order.price)),
        order.original_size,
        order.volume_ahead,
        order.status.as_str()
    );
}

pub(crate) fn print_trade_eval(
    outcome: core_types::Outcome,
    price: MicroUsdc,
    size: SharesMicro,
    vol_ahead_before: SharesMicro,
    vol_ahead_after: SharesMicro,
    reachable: SharesMicro,
    filled: SharesMicro,
) {
    let t = theme::current();
    let outcome_str = match outcome {
        core_types::Outcome::Up => "UP",
        core_types::Outcome::Down => "DN",
    };
    println!(
        "  {} {:<2} | px={} | sz={:<6} | vol_ahead: {:<6} -> {:<6} | reachable: {:<6} | filled: {:<6}",
        t.badge(Badge::Trade),
        outcome_str,
        Theme::fmt_price_compact(Some(price)),
        size,
        vol_ahead_before,
        vol_ahead_after,
        reachable,
        filled
    );
}

pub(crate) fn print_fill_event(fill: &SimFill) {
    let t = theme::current();
    let outcome_str = match fill.outcome {
        core_types::Outcome::Up => t.bold_green("UP"),
        core_types::Outcome::Down => t.bold_cyan("DN"),
    };
    println!(
        "  {} #{:<4} {} {:<4} | {:<7} @ {} (maker={})",
        t.badge(Badge::Fill),
        fill.sim_id,
        outcome_str,
        fill.side.as_str(),
        fill.filled_shares,
        Theme::fmt_price_compact(Some(fill.price)),
        fill.is_maker
    );
}

pub(crate) fn print_merge_event(merge: &MergeEvent, total_merged: SharesMicro) {
    let t = theme::current();
    println!(
        "  {} {:<6} pairs | pair_cost={} (UP={}, DN={}) -> PnL={} (total_merged={})",
        t.badge(Badge::Merge),
        merge.merged_pairs,
        Theme::fmt_price_compact(Some(merge.pair_cost)),
        Theme::fmt_price_compact(Some(merge.avg_cost_up)),
        Theme::fmt_price_compact(Some(merge.avg_cost_dn)),
        t.fmt_pnl(merge.realized_gain_micro),
        total_merged
    );
}

/// Aggregated display variables for printing and recording paper simulation summaries.
pub(crate) struct PaperSummaryDisplay<'a> {
    pub(crate) q_up: SharesMicro,
    pub(crate) q_dn: SharesMicro,
    pub(crate) net_q: SignedSharesMicro,
    pub(crate) pairs_merged: SharesMicro,
    pub(crate) realized_pnl: i64,
    pub(crate) unrealized_pnl: i64,
    pub(crate) fees_paid: u64,
    pub(crate) rpm_60s: usize,
    pub(crate) fill_ratio: f64,
    pub(crate) markout_1s_bps: f64,
    pub(crate) markout_5s_bps: f64,
    pub(crate) baseline_pnl: i64,
    pub(crate) latency: &'a LatencySummary,
}

pub(crate) fn print_summary_table(disp: &PaperSummaryDisplay<'_>) {
    let t = theme::current();
    let net_pnl = disp.realized_pnl + disp.unrealized_pnl;
    let edge_pnl = net_pnl - disp.baseline_pnl;

    println!();
    println!(
        "{}",
        t.banner("PHASE 3 PAPER SIMULATOR ROLLING SUMMARY", None)
    );
    println!("{}", t.section("INVENTORY & PNL"));
    println!(
        "{}",
        t.kv(
            "Inventory:",
            &format!(
                "UP={} | DN={} | Delta_q={:+.6} | Merged={}",
                disp.q_up,
                disp.q_dn,
                disp.net_q.to_shares_f64(),
                disp.pairs_merged
            )
        )
    );
    println!(
        "{}",
        t.kv(
            "PnL (Real / Unreal):",
            &format!(
                "{} / {} | Net={}",
                t.fmt_pnl(disp.realized_pnl),
                t.fmt_pnl(disp.unrealized_pnl),
                t.fmt_pnl(net_pnl)
            )
        )
    );
    println!("{}", t.section("METRICS & LATENCY"));
    println!(
        "{}",
        t.kv(
            "Execution Metrics:",
            &format!(
                "RPM={} | FillRatio={:.2}% | Markout 1s={:+.1}bps | Markout 5s={:+.1}bps",
                disp.rpm_60s,
                disp.fill_ratio * 100.0,
                disp.markout_1s_bps,
                disp.markout_5s_bps,
            )
        )
    );
    println!(
        "{}",
        t.kv(
            "Fees & Alpha Edge:",
            &format!(
                "FeesPaid=${} | BaselinePnL={} | A-S Alpha Edge={}",
                MicroUsdc(disp.fees_paid),
                t.fmt_pnl(disp.baseline_pnl),
                t.fmt_pnl(edge_pnl)
            )
        )
    );
    println!(
        "{}",
        t.kv(
            "Compute Latency:",
            &format!(
                "P50={:.1}us | P99={:.1}us (samples={})",
                disp.latency.p50_ns as f64 / 1_000.0,
                disp.latency.p99_ns as f64 / 1_000.0,
                disp.latency.count,
            )
        )
    );
    println!("{}", t.divider());
    println!();
}

pub(crate) fn record_fill_jsonl(
    market_slug: &str,
    fill: &SimFill,
    jsonl: &mut JsonlWriter,
    clock: &MonotonicClock,
) -> Result<(), Box<dyn Error>> {
    let rec = PaperFillJsonl {
        market_slug,
        sim_id: fill.sim_id,
        outcome: match fill.outcome {
            core_types::Outcome::Up => "up",
            core_types::Outcome::Down => "down",
        },
        side: fill.side.as_str(),
        price_micro: fill.price.0,
        filled_shares_micro: fill.filled_shares.0,
        fill_ts_ms: fill.fill_ts_ms,
        is_maker: fill.is_maker,
    };
    jsonl.write_event(fill.fill_ts_ms, clock.now_ns()?, "fill", &rec)?;
    Ok(())
}

pub(crate) fn record_merge_jsonl(
    market_slug: &str,
    merge: &MergeEvent,
    total_merged: SharesMicro,
    now_ms: u64,
    jsonl: &mut JsonlWriter,
    clock: &MonotonicClock,
) -> Result<(), Box<dyn Error>> {
    let rec = PaperMergeJsonl {
        market_slug,
        merged_pairs_micro: merge.merged_pairs.0,
        avg_cost_up_micro: merge.avg_cost_up.0,
        avg_cost_dn_micro: merge.avg_cost_dn.0,
        pair_cost_micro: merge.pair_cost.0,
        realized_gain_micro: merge.realized_gain_micro,
        total_merged_micro: total_merged.0,
    };
    jsonl.write_event(now_ms, clock.now_ns()?, "merge", &rec)?;
    Ok(())
}

pub(crate) fn record_order_jsonl(
    market_slug: &str,
    action: &str,
    order: &SimOrder,
    now_ms: u64,
    jsonl: &mut JsonlWriter,
    clock: &MonotonicClock,
) -> Result<(), Box<dyn Error>> {
    let rec = PaperOrderJsonl {
        market_slug,
        sim_id: order.sim_id,
        action,
        outcome: match order.outcome {
            core_types::Outcome::Up => "up",
            core_types::Outcome::Down => "down",
        },
        side: order.side.as_str(),
        price_micro: order.price.0,
        size_micro: order.original_size.0,
        volume_ahead_micro: order.volume_ahead.0,
        now_ms,
    };
    jsonl.write_event(now_ms, clock.now_ns()?, "order", &rec)?;
    Ok(())
}

pub(crate) fn record_summary_jsonl(
    source: &str,
    market_slug: &str,
    stats: &PaperReportStats,
    disp: &PaperSummaryDisplay<'_>,
    now_ms: u64,
    jsonl: &mut JsonlWriter,
    clock: &MonotonicClock,
) -> Result<(), Box<dyn Error>> {
    let net_pnl = disp.realized_pnl + disp.unrealized_pnl;
    let edge_pnl = net_pnl - disp.baseline_pnl;

    let rec = PaperSummaryJsonl {
        source,
        market_slug,
        total_ticks: stats.total_ticks,
        epoch_rollovers: stats.epoch_rollovers,
        q_up_micro: disp.q_up.0,
        q_dn_micro: disp.q_dn.0,
        net_q_micro: disp.net_q.as_i64(),
        pairs_merged_micro: disp.pairs_merged.0,
        realized_pnl_micro: disp.realized_pnl,
        unrealized_pnl_micro: disp.unrealized_pnl,
        net_pnl_micro: net_pnl,
        fees_paid_micro: disp.fees_paid,
        rpm_60s: disp.rpm_60s,
        fill_ratio: disp.fill_ratio,
        post_fill_markout_1s_bps: disp.markout_1s_bps,
        post_fill_markout_5s_bps: disp.markout_5s_bps,
        baseline_pnl_micro: disp.baseline_pnl,
        edge_pnl_micro: edge_pnl,
        compute_latency: *disp.latency,
    };
    jsonl.write_event(now_ms, clock.now_ns()?, "summary", &rec)?;
    jsonl.flush()?;
    Ok(())
}

pub(crate) fn record_tick_jsonl(
    tick_index: usize,
    source: &str,
    market_slug: &str,
    now_ms: u64,
    trajectory: TrajectorySnapshot,
    jsonl: &mut JsonlWriter,
    clock: &MonotonicClock,
) -> Result<(), Box<dyn Error>> {
    let rec = PaperTickJsonl {
        tick_index,
        source,
        market_slug,
        now_ms,
        trajectory,
    };
    jsonl.write_event(now_ms, clock.now_ns()?, "tick", &rec)?;
    Ok(())
}
