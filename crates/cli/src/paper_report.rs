use std::error::Error;
use std::path::Path;

use core_types::{
    JsonlWriter, LatencySummary, MarketDescriptor, MicroUsdc, MonotonicClock, SharesMicro,
    SignedSharesMicro,
};
use paper_sim::{MergeEvent, SimFill, SimOrder, SimOrderStatus, TrajectorySnapshot};
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
    estimated_rebate_micro: u64,
    rpm_60s: usize,
    fill_ratio: f64,
    post_fill_markout_1s_bps: f64,
    post_fill_markout_5s_bps: f64,
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
        t.banner(&format!("POLY-AS FIFO PAPER SIMULATOR ({source})"), None)
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
    println!("{}", format_order_event(theme::current(), action, order));
}

fn format_order_event(t: Theme, action: &str, order: &SimOrder) -> String {
    let outcome_str = match order.outcome {
        core_types::Outcome::Up => t.bold_green("UP"),
        core_types::Outcome::Down => t.bold_cyan("DN"),
    };
    let action_styled = match action {
        "CANCEL" | "CANCELLED" => t.yellow(&format!("{action:<7}")),
        _ => t.cyan(&format!("{action:<7}")),
    };
    let label = status_label(order.status);
    let status = match order.status {
        SimOrderStatus::Cancelled => t.yellow(label),
        SimOrderStatus::Filled | SimOrderStatus::Live => t.green(label),
        _ => t.gray(label),
    };
    let bar = t.bar();
    format!(
        "  {} #{:<4} {action_styled} {bar} {outcome_str} {} {} @ {} {bar} {} {} {bar} {}",
        t.badge(Badge::Order),
        order.sim_id,
        order.side.as_str(),
        fmt_shares(order.original_size),
        Theme::fmt_cents(Some(order.price)),
        t.muted("Ahead"),
        fmt_shares(order.volume_ahead),
        status,
    )
}

/// Human status text for a simulated order (no CamelCase debug names).
fn status_label(status: SimOrderStatus) -> &'static str {
    match status {
        SimOrderStatus::PendingPlace { .. } => "Pending place",
        SimOrderStatus::PendingPlaceThenCancel { .. } => "Place+cancel in flight",
        SimOrderStatus::Live => "Live",
        SimOrderStatus::PendingCancel { .. } => "Cancel in flight",
        SimOrderStatus::Filled => "Filled",
        SimOrderStatus::Cancelled => "Cancelled",
    }
}

/// Whole-share quantity (`20`, `1.5`) from micro-shares.
fn fmt_shares(shares: SharesMicro) -> String {
    let raw = shares.as_u64();
    let whole = raw / 1_000_000;
    let frac = raw % 1_000_000;
    if frac == 0 {
        whole.to_string()
    } else {
        format!("{whole}.{frac:06}")
            .trim_end_matches('0')
            .to_string()
    }
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
    println!(
        "{}",
        format_trade_eval(
            theme::current(),
            TradeEval {
                outcome,
                price,
                size,
                vol_ahead_before,
                vol_ahead_after,
                reachable,
                filled,
            },
        )
    );
}

/// One public-trade evaluation against the resting queue.
struct TradeEval {
    outcome: core_types::Outcome,
    price: MicroUsdc,
    size: SharesMicro,
    vol_ahead_before: SharesMicro,
    vol_ahead_after: SharesMicro,
    reachable: SharesMicro,
    filled: SharesMicro,
}

fn format_trade_eval(t: Theme, eval: TradeEval) -> String {
    let TradeEval {
        outcome,
        price,
        size,
        vol_ahead_before,
        vol_ahead_after,
        reachable,
        filled,
    } = eval;
    let outcome_str = match outcome {
        core_types::Outcome::Up => t.green("UP"),
        core_types::Outcome::Down => t.cyan("DN"),
    };
    let filled_str = if filled.as_u64() > 0 {
        t.bold_green(&fmt_shares(filled))
    } else {
        t.muted(&fmt_shares(filled))
    };
    let bar = t.bar();
    format!(
        "  {} {outcome_str} {bar} {} @ {} {bar} {} {} {} {} {bar} {} {} {bar} {} {filled_str}",
        t.badge(Badge::Trade),
        fmt_shares(size),
        Theme::fmt_cents(Some(price)),
        t.muted("Ahead"),
        fmt_shares(vol_ahead_before),
        t.arrow(),
        fmt_shares(vol_ahead_after),
        t.muted("Reach"),
        fmt_shares(reachable),
        t.muted("Filled"),
    )
}

pub(crate) fn print_fill_event(fill: &SimFill) {
    println!("{}", format_fill_event(theme::current(), fill));
}

fn format_fill_event(t: Theme, fill: &SimFill) -> String {
    let outcome_str = match fill.outcome {
        core_types::Outcome::Up => t.bold_green("UP"),
        core_types::Outcome::Down => t.bold_cyan("DN"),
    };
    let liquidity = if fill.is_maker {
        t.badge(Badge::Maker)
    } else {
        t.badge(Badge::Taker)
    };
    format!(
        "  {} #{:<4} {outcome_str} {} {} @ {} {} {liquidity}",
        t.badge(Badge::Fill),
        fill.sim_id,
        fill.side.as_str(),
        fmt_shares(fill.filled_shares),
        Theme::fmt_cents(Some(fill.price)),
        t.bar(),
    )
}

pub(crate) fn print_merge_event(merge: &MergeEvent, total_merged: SharesMicro) {
    println!(
        "{}",
        format_merge_event(theme::current(), merge, total_merged)
    );
}

fn format_merge_event(t: Theme, merge: &MergeEvent, total_merged: SharesMicro) -> String {
    let bar = t.bar();
    format!(
        "  {} {} pairs {bar} cost {} (UP {} {} DN {}) {bar} PnL {} {bar} total {}",
        t.badge(Badge::Merge),
        fmt_shares(merge.merged_pairs),
        Theme::fmt_cents(Some(merge.pair_cost)),
        Theme::fmt_cents(Some(merge.avg_cost_up)),
        t.dot(),
        Theme::fmt_cents(Some(merge.avg_cost_dn)),
        t.fmt_pnl(merge.realized_gain_micro),
        fmt_shares(total_merged),
    )
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
    pub(crate) estimated_rebate: u64,
    pub(crate) rpm_60s: usize,
    pub(crate) fill_ratio: f64,
    pub(crate) markout_1s_bps: f64,
    pub(crate) markout_5s_bps: f64,
    pub(crate) latency: &'a LatencySummary,
}

pub(crate) fn print_summary_table(disp: &PaperSummaryDisplay<'_>) {
    let t = theme::current();
    let net_pnl = disp.realized_pnl + disp.unrealized_pnl;
    let net_pnl_3pct = net_pnl.saturating_add(disp.estimated_rebate as i64);

    println!();
    println!("{}", t.banner("FIFO PAPER SIMULATOR ROLLING SUMMARY", None));
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
                "{} / {} | Net(0%)={} | Reb(3%)=${} | Net(+3%)={}",
                t.fmt_pnl(disp.realized_pnl),
                t.fmt_pnl(disp.unrealized_pnl),
                t.fmt_pnl(net_pnl),
                MicroUsdc(disp.estimated_rebate),
                t.fmt_pnl(net_pnl_3pct)
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
        t.kv("Fees:", &format!("FeesPaid=${}", MicroUsdc(disp.fees_paid)),)
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
        estimated_rebate_micro: disp.estimated_rebate,
        rpm_60s: disp.rpm_60s,
        fill_ratio: disp.fill_ratio,
        post_fill_markout_1s_bps: disp.markout_1s_bps,
        post_fill_markout_5s_bps: disp.markout_5s_bps,
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

#[cfg(test)]
mod row_tests {
    use super::*;
    use core_types::{MicroUsdc, OrderSide, Outcome, SharesMicro};
    use paper_sim::{MergeEvent, SimFill};

    #[test]
    fn paper_fill_row_is_columnar() {
        let fill = SimFill {
            sim_id: 3,
            outcome: Outcome::Up,
            side: OrderSide::Buy,
            price: MicroUsdc(480_000),
            filled_shares: SharesMicro::from_raw(20_000_000),
            fill_ts_ms: 0,
            is_maker: false,
        };
        let row = format_fill_event(Theme::plain(), &fill);
        assert!(row.contains("[FILL] #3"));
        assert!(row.contains("UP BUY 20 @ 48¢"));
        assert!(row.contains("[TAKER]"));
        assert!(!row.contains("maker="));
        assert!(row.len() <= 140, "{row}");
    }

    #[test]
    fn paper_merge_and_trade_rows_drop_key_values() {
        let merge = MergeEvent {
            merged_pairs: SharesMicro::from_raw(10_000_000),
            avg_cost_up: MicroUsdc(470_000),
            avg_cost_dn: MicroUsdc(480_000),
            pair_cost: MicroUsdc(950_000),
            realized_gain_micro: 500_000,
        };
        let row = format_merge_event(Theme::plain(), &merge, SharesMicro::from_raw(10_000_000));
        assert!(row.contains("10 pairs"));
        assert!(row.contains("cost 95¢"));
        assert!(row.contains("PnL +$0.50"));
        assert!(!row.contains("pair_cost="));

        let trade = format_trade_eval(
            Theme::plain(),
            TradeEval {
                outcome: Outcome::Up,
                price: MicroUsdc(480_000),
                size: SharesMicro::from_raw(20_000_000),
                vol_ahead_before: SharesMicro::ZERO,
                vol_ahead_after: SharesMicro::from_raw(5_000_000),
                reachable: SharesMicro::from_raw(15_000_000),
                filled: SharesMicro::from_raw(10_000_000),
            },
        );
        assert!(trade.contains("| Ahead 0 -> 5 |"));
        assert!(trade.contains("Reach 15"));
        assert!(trade.contains("Filled 10"));
        assert!(!trade.contains('='));
        assert!(trade.len() <= 140, "{trade}");
    }
}
