use core_types::{
    JsonlWriter, LatencySummary, MICRO_SCALE, MicroUsdc, OrderSide, Outcome, SharesMicro,
};
use paper_sim::{MergeEvent, SimFill};
use risk_lifecycle::{BreakerStatus, EpochPhase, OrderAction, RiskActorTrace, RiskTier};
use serde::Serialize;
use std::path::Path;

/// Statistics gathered during the lifecycle evaluation run.
#[derive(Debug)]
pub(crate) struct LifecycleReportStats {
    pub(crate) ticks_evaluated: usize,
    pub(crate) orders_submitted: usize,
    pub(crate) orders_canceled: usize,
    pub(crate) orders_replaced: usize,
    pub(crate) orders_held: usize,
    pub(crate) flattens_executed: usize,
    pub(crate) phase_transitions: usize,
    pub(crate) breaker_trips: usize,
    pub(crate) fills_count: u64,
    pub(crate) merges_count: u64,
    pub(crate) total_pairs_merged: SharesMicro,
    pub(crate) realized_pnl_micro: i64,
    pub(crate) unrealized_pnl_micro: i64,
    pub(crate) settlement_pnl_micro: i64,
    pub(crate) fees_paid_micro: u64,
    pub(crate) estimated_rebate_micro: u64,
    pub(crate) epochs_completed: usize,
    pub(crate) epoch_rollovers: usize,
    pub(crate) final_q_up: SharesMicro,
    pub(crate) final_q_dn: SharesMicro,
    pub(crate) final_net_delta_micro: i64,
    pub(crate) rpm_60s: usize,
    pub(crate) fill_ratio: f64,
    pub(crate) markout_1s_bps: f64,
    pub(crate) markout_5s_bps: f64,
    pub(crate) baseline_pnl_micro: i64,
}

impl Default for LifecycleReportStats {
    fn default() -> Self {
        Self {
            ticks_evaluated: 0,
            orders_submitted: 0,
            orders_canceled: 0,
            orders_replaced: 0,
            orders_held: 0,
            flattens_executed: 0,
            phase_transitions: 0,
            breaker_trips: 0,
            fills_count: 0,
            merges_count: 0,
            total_pairs_merged: SharesMicro::ZERO,
            realized_pnl_micro: 0,
            unrealized_pnl_micro: 0,
            settlement_pnl_micro: 0,
            fees_paid_micro: 0,
            estimated_rebate_micro: 0,
            epochs_completed: 0,
            epoch_rollovers: 0,
            final_q_up: SharesMicro::ZERO,
            final_q_dn: SharesMicro::ZERO,
            final_net_delta_micro: 0,
            rpm_60s: 0,
            fill_ratio: 0.0,
            markout_1s_bps: 0.0,
            markout_5s_bps: 0.0,
            baseline_pnl_micro: 0,
        }
    }
}

#[derive(Serialize)]
struct PhaseTransitionEvent {
    from_phase: Option<EpochPhase>,
    to_phase: EpochPhase,
    tau_secs: u64,
    reason: String,
}

#[derive(Serialize)]
struct RiskTierEvent {
    tier: RiskTier,
    utilization_u_e6: u64,
    heavy_leg: Option<Outcome>,
}

#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct OrderActionTelemetry<'a> {
    pub(crate) phase: Option<EpochPhase>,
    pub(crate) cancel_all_reason: Option<&'a str>,
    pub(crate) flatten_in_flight: Option<bool>,
    pub(crate) best_bid_up: Option<MicroUsdc>,
    pub(crate) best_ask_up: Option<MicroUsdc>,
    pub(crate) best_bid_dn: Option<MicroUsdc>,
    pub(crate) best_ask_dn: Option<MicroUsdc>,
}

impl<'a> OrderActionTelemetry<'a> {
    pub(crate) fn from_trace(trace: &'a RiskActorTrace) -> Self {
        if trace.phase == EpochPhase::MandatoryFlatten || trace.cancel_all_reason.is_some() {
            Self {
                phase: Some(trace.phase),
                cancel_all_reason: trace.cancel_all_reason.as_deref(),
                flatten_in_flight: Some(trace.flatten_in_flight),
                best_bid_up: trace.best_bid_up,
                best_ask_up: trace.best_ask_up,
                best_bid_dn: trace.best_bid_dn,
                best_ask_dn: trace.best_ask_dn,
            }
        } else {
            Self::default()
        }
    }
}

#[derive(Serialize)]
struct OrderActionEvent<'a> {
    outcome: Outcome,
    action: OrderAction,
    tau_secs: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    phase: Option<EpochPhase>,
    #[serde(skip_serializing_if = "Option::is_none")]
    cancel_all_reason: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    flatten_in_flight: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    best_bid_up_micro: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    best_ask_up_micro: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    best_bid_dn_micro: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    best_ask_dn_micro: Option<u64>,
}

pub(crate) struct CancelAllRecord<'a> {
    pub(crate) market_slug: &'a str,
    pub(crate) phase: Option<EpochPhase>,
    pub(crate) tau_secs: u64,
    pub(crate) canceled_count: usize,
    pub(crate) flatten_in_flight: Option<bool>,
    pub(crate) best_bid_up: Option<MicroUsdc>,
    pub(crate) best_ask_up: Option<MicroUsdc>,
    pub(crate) best_bid_dn: Option<MicroUsdc>,
    pub(crate) best_ask_dn: Option<MicroUsdc>,
    pub(crate) reason: &'a str,
}

#[derive(Serialize)]
struct CancelAllEvent<'a> {
    market_slug: &'a str,
    phase: Option<EpochPhase>,
    tau_secs: u64,
    canceled_count: usize,
    flatten_in_flight: Option<bool>,
    best_bid_up_micro: Option<u64>,
    best_ask_up_micro: Option<u64>,
    best_bid_dn_micro: Option<u64>,
    best_ask_dn_micro: Option<u64>,
    reason: &'a str,
}

#[derive(Serialize)]
struct BreakerEvent {
    status: BreakerStatus,
    clock_offset_ms: i64,
}

#[derive(Serialize)]
struct LifecycleFillEvent<'a> {
    market_slug: &'a str,
    sim_id: u64,
    outcome: Outcome,
    side: &'a str,
    price_micro: u64,
    filled_shares_micro: u64,
    fill_ts_ms: u64,
    is_maker: bool,
}

#[derive(Serialize)]
struct LifecycleMergeEvent<'a> {
    market_slug: &'a str,
    merged_pairs_micro: u64,
    avg_cost_up_micro: u64,
    avg_cost_dn_micro: u64,
    pair_cost_micro: u64,
    realized_gain_micro: i64,
    total_merged_micro: u64,
}

#[derive(Serialize)]
struct LifecycleFlattenEvent<'a> {
    market_slug: &'a str,
    outcome: Outcome,
    side: &'a str,
    price_micro: u64,
    size_micro: u64,
    fee_micro: u64,
    realized_pnl_micro: i64,
    reason: &'a str,
}

#[derive(Serialize)]
struct EpochRolloverEvent<'a> {
    expired_slug: &'a str,
    next_slug: Option<&'a str>,
    winning_outcome: Outcome,
    settlement_pnl_delta_micro: i64,
    total_realized_pnl_micro: i64,
    pairs_merged_micro: u64,
    fees_paid_micro: u64,
    estimated_rebate_micro: u64,
}

#[derive(Serialize)]
struct LifecycleSummaryEvent<'a> {
    source: &'a str,
    market_slug: &'a str,
    ticks_evaluated: usize,
    phase_transitions: usize,
    orders_submitted: usize,
    orders_replaced: usize,
    orders_canceled: usize,
    orders_held: usize,
    flattens_executed: usize,
    breaker_trips: usize,
    fills_count: u64,
    merges_count: u64,
    total_pairs_merged_micro: u64,
    q_up_micro: u64,
    q_dn_micro: u64,
    net_delta_micro: i64,
    realized_pnl_micro: i64,
    unrealized_pnl_micro: i64,
    net_pnl_micro: i64,
    settlement_pnl_micro: i64,
    fees_paid_micro: u64,
    estimated_rebate_micro: u64,
    epochs_completed: usize,
    epoch_rollovers: usize,
    rpm_60s: usize,
    fill_ratio: f64,
    markout_1s_bps: f64,
    markout_5s_bps: f64,
    baseline_pnl_micro: i64,
    pricing_latency: LatencySummary,
    risk_latency: LatencySummary,
    total_latency: LatencySummary,
}

pub(crate) fn print_lifecycle_header(
    source: &str,
    v_max_usd: u64,
    base_size: u64,
    fast_forward: bool,
    fault: &str,
    jsonl_path: &Path,
) {
    println!();
    println!("================================================================================");
    println!("  POLYMARKET 5-MINUTE BTC HFT BOT -- PHASE 4 RISK & 300s LIFECYCLE ACTORS");
    println!("================================================================================");
    println!("  Mode:               {}", source);
    println!("  Dollar Cap (V_max): ${:.2} (MicroUsdc)", v_max_usd);
    println!("  Base Order Size:    {} shares", base_size);
    println!("  Fast-Forward:       {}", fast_forward);
    println!("  Fault Injection:    {}", fault);
    println!("  JSONL Telemetry:    {}", jsonl_path.display());
    println!(
        "  Actor Topology:     Lock-free ArcSwap (Pricing -> Risk) + Priority Bounded Channels"
    );
    println!("  Core Pinning:       Pricing OS-Thread, Risk OS-Thread, Execution Tokio Task");
    println!("  High-Res Timer:     Active (timeBeginPeriod 1ms / QPC)");
    println!("--------------------------------------------------------------------------------");
    println!("  [Time] [Phase]             [Tier]        [Util] [q UP/DN/Net]  [Emitted Actions]");
    println!("--------------------------------------------------------------------------------");
}

/// Data for a single lifecycle terminal status line.
pub(crate) struct LifecycleStepRow<'a> {
    pub(crate) tau_secs: u64,
    pub(crate) phase: EpochPhase,
    pub(crate) tier: RiskTier,
    pub(crate) u_e6: u64,
    pub(crate) q_up: SharesMicro,
    pub(crate) q_dn: SharesMicro,
    pub(crate) net_delta: i64,
    pub(crate) action_up: &'a OrderAction,
    pub(crate) action_dn: &'a OrderAction,
    pub(crate) cancel_all_reason: Option<&'a str>,
    pub(crate) flatten_in_flight: bool,
    pub(crate) best_bid_up: Option<MicroUsdc>,
    pub(crate) best_ask_up: Option<MicroUsdc>,
    pub(crate) best_bid_dn: Option<MicroUsdc>,
    pub(crate) best_ask_dn: Option<MicroUsdc>,
    pub(crate) pricing_ns: u64,
    pub(crate) risk_ns: u64,
}

fn fmt_opt_usd(px: Option<MicroUsdc>) -> String {
    px.map_or_else(|| "none".to_string(), |p| format!("${p}"))
}

pub(crate) fn print_lifecycle_step(row: &LifecycleStepRow<'_>) {
    let u_pct = (row.u_e6 as f64) / 10_000.0;
    let up_str = format_action(row.action_up);
    let dn_str = format_action(row.action_dn);

    let total_us = (row.pricing_ns + row.risk_ns) / 1_000;
    let risk_us = row.risk_ns / 1_000;

    if row.phase == EpochPhase::MandatoryFlatten || row.cancel_all_reason.is_some() {
        let reason = row.cancel_all_reason.unwrap_or("none");
        println!(
            "  T-{:03}s  {:<20} {:<13} {:>5.1}% ({:>2},{:>2},d={:>+2})  UP: {:<16} | DN: {:<16} [Risk: {}us, Total: {}us] | fif={} UP={}/{} DN={}/{} reason={}",
            row.tau_secs,
            format!("{:?}", row.phase),
            format!("{:?}", row.tier),
            u_pct,
            row.q_up.as_u64() / 1_000_000,
            row.q_dn.as_u64() / 1_000_000,
            row.net_delta,
            up_str,
            dn_str,
            risk_us,
            total_us,
            row.flatten_in_flight,
            fmt_opt_usd(row.best_bid_up),
            fmt_opt_usd(row.best_ask_up),
            fmt_opt_usd(row.best_bid_dn),
            fmt_opt_usd(row.best_ask_dn),
            reason,
        );
    } else {
        println!(
            "  T-{:03}s  {:<20} {:<13} {:>5.1}% ({:>2},{:>2},d={:>+2})  UP: {:<16} | DN: {:<16} [Risk: {}us, Total: {}us]",
            row.tau_secs,
            format!("{:?}", row.phase),
            format!("{:?}", row.tier),
            u_pct,
            row.q_up.as_u64() / 1_000_000,
            row.q_dn.as_u64() / 1_000_000,
            row.net_delta,
            up_str,
            dn_str,
            risk_us,
            total_us,
        );
    }
}

fn format_action(action: &OrderAction) -> String {
    match action {
        OrderAction::Hold => "Hold".to_string(),
        OrderAction::SubmitNew { price, size, .. } => {
            format!(
                "SubmitNew {}@${:.2}",
                size.as_u64() / 1_000_000,
                price.0 as f64 / 1_000_000.0
            )
        }
        OrderAction::Cancel { reason, .. } => format!("Cancel({:?})", reason),
        OrderAction::CancelAndReplace {
            new_price,
            new_size,
            reason,
            ..
        } => {
            format!(
                "Replace({:?}) {}@${:.2}",
                reason,
                new_size.as_u64() / 1_000_000,
                new_price.0 as f64 / 1_000_000.0
            )
        }
    }
}

pub(crate) fn print_phase_transition(
    from: Option<EpochPhase>,
    to: EpochPhase,
    tau_secs: u64,
    reason: &str,
) {
    println!();
    println!(
        "  >>> PHASE TRANSITION: {:?} -> {:?} (T-{}s) | Reason: {}",
        from, to, tau_secs, reason
    );
    println!();
}

pub(crate) fn print_tier_transition(to: RiskTier, u_e6: u64, heavy: Option<Outcome>) {
    println!(
        "  --- RISK TIER SHIFT: {:?} (u = {:.1}%, heavy: {:?})",
        to,
        (u_e6 as f64) / 10_000.0,
        heavy
    );
}

pub(crate) fn print_breaker_trip(status: BreakerStatus) {
    println!();
    println!("  !!! CIRCUIT BREAKER TRIPPED: {:?}", status);
    println!();
}

pub(crate) fn print_lifecycle_fill(market_slug: &str, fill: &SimFill) {
    let leg = match fill.outcome {
        Outcome::Up => "UP",
        Outcome::Down => "DN",
    };
    println!(
        "  >>> [FILL] {} | #{} {} {} {} @ ${} (maker={})",
        market_slug,
        fill.sim_id,
        leg,
        fill.side.as_str(),
        fill.filled_shares,
        fill.price,
        fill.is_maker
    );
}

pub(crate) fn print_lifecycle_merge(
    market_slug: &str,
    merge: &MergeEvent,
    total_merged: SharesMicro,
) {
    println!(
        "  *** [MERGE] {} | {} pairs | pair_cost=${} (UP=${}, DN=${}) -> PnL={} (total={})",
        market_slug,
        merge.merged_pairs,
        merge.pair_cost,
        merge.avg_cost_up,
        merge.avg_cost_dn,
        fmt_pnl(merge.realized_gain_micro),
        total_merged
    );
}

pub(crate) fn print_lifecycle_flatten(
    market_slug: &str,
    outcome: Outcome,
    side: OrderSide,
    price: MicroUsdc,
    size: SharesMicro,
    fee: MicroUsdc,
    reason: &str,
) {
    let leg = match outcome {
        Outcome::Up => "UP",
        Outcome::Down => "DN",
    };
    println!(
        "  !!! [FLATTEN-FAK] {} | {} {} {} @ ${} | fee=${} | reason={}",
        market_slug,
        leg,
        side.as_str(),
        size,
        price,
        fee,
        reason
    );
}

pub(crate) fn print_epoch_rollover(
    expired_slug: &str,
    next_slug: Option<&str>,
    winning_outcome: Outcome,
    settlement_pnl_delta_micro: i64,
    total_realized_pnl_micro: i64,
) {
    println!();
    println!(
        "  === [EPOCH ROLLOVER] {} -> {} | winner={:?} | settle_delta={} | cumulative_realized={}",
        expired_slug,
        next_slug.unwrap_or("awaiting-next"),
        winning_outcome,
        fmt_pnl(settlement_pnl_delta_micro),
        fmt_pnl(total_realized_pnl_micro)
    );
    println!();
}

pub(crate) fn print_lifecycle_summary(
    stats: &LifecycleReportStats,
    pricing_lat: &LatencySummary,
    risk_lat: &LatencySummary,
    total_lat: &LatencySummary,
) {
    let net_pnl_micro = stats
        .realized_pnl_micro
        .saturating_add(stats.unrealized_pnl_micro);
    println!();
    println!("================================================================================");
    println!("  PHASE 4 LIFECYCLE & RISK EXECUTION SUMMARY");
    println!("================================================================================");
    println!("  Total Ticks Evaluated:   {}", stats.ticks_evaluated);
    println!("  Phase Transitions:       {}", stats.phase_transitions);
    println!("  Orders Submitted:        {}", stats.orders_submitted);
    println!("  Orders Replaced:         {}", stats.orders_replaced);
    println!("  Orders Canceled:         {}", stats.orders_canceled);
    println!("  Orders Held (FIFO):      {}", stats.orders_held);
    println!("  Flattens Executed:       {}", stats.flattens_executed);
    println!("  Breaker Trips Handled:   {}", stats.breaker_trips);
    println!("--------------------------------------------------------------------------------");
    println!(
        "  Paper Fills / Merges:    {} fills | {} merges ({} pairs)",
        stats.fills_count, stats.merges_count, stats.total_pairs_merged
    );
    println!(
        "  Final Inventory:         UP={} | DN={} | Net={:+.6} sh",
        stats.final_q_up,
        stats.final_q_dn,
        stats.final_net_delta_micro as f64 / MICRO_SCALE as f64
    );
    println!(
        "  PnL (Realized / Unreal): {} / {} | Net={}",
        fmt_pnl(stats.realized_pnl_micro),
        fmt_pnl(stats.unrealized_pnl_micro),
        fmt_pnl(net_pnl_micro)
    );
    println!(
        "  Settlement / Fees / Reb: Settle={} | Fees=${} | EstRebate=${}",
        fmt_pnl(stats.settlement_pnl_micro),
        MicroUsdc(stats.fees_paid_micro),
        MicroUsdc(stats.estimated_rebate_micro)
    );
    println!(
        "  Epochs / Telemetry:      epochs={} rollovers={} | RPM={} | Fill={:.2}% | MO(1s/5s)={:+.1}/{:+.1}bps | Base={}",
        stats.epochs_completed,
        stats.epoch_rollovers,
        stats.rpm_60s,
        stats.fill_ratio * 100.0,
        stats.markout_1s_bps,
        stats.markout_5s_bps,
        fmt_pnl(stats.baseline_pnl_micro)
    );
    println!("--------------------------------------------------------------------------------");
    println!("  Compute Latency HDR Percentiles (P50 / P95 / P99 / max in microseconds):");
    println!(
        "    Pricing Pipeline:      {} / {} / {} / {} us (Target P99 < 25us)",
        pricing_lat.p50_ns / 1_000,
        pricing_lat.p95_ns / 1_000,
        pricing_lat.p99_ns / 1_000,
        pricing_lat.max_ns / 1_000
    );
    println!(
        "    Risk & Hysteresis:     {} / {} / {} / {} us (Target P99 < 10us)",
        risk_lat.p50_ns / 1_000,
        risk_lat.p95_ns / 1_000,
        risk_lat.p99_ns / 1_000,
        risk_lat.max_ns / 1_000
    );
    println!(
        "    Total Internal Path:   {} / {} / {} / {} us (Target P99 < 100us)",
        total_lat.p50_ns / 1_000,
        total_lat.p95_ns / 1_000,
        total_lat.p99_ns / 1_000,
        total_lat.max_ns / 1_000
    );
    println!("================================================================================");
}

pub(crate) fn record_phase_transition_jsonl(
    jsonl: &mut JsonlWriter,
    now_ms: u64,
    mono_ns: u64,
    from_phase: Option<EpochPhase>,
    to_phase: EpochPhase,
    tau_secs: u64,
    reason: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    jsonl.write_event(
        now_ms,
        mono_ns,
        "phase_transition",
        &PhaseTransitionEvent {
            from_phase,
            to_phase,
            tau_secs,
            reason: reason.to_string(),
        },
    )?;
    Ok(())
}

pub(crate) fn record_risk_tier_jsonl(
    jsonl: &mut JsonlWriter,
    now_ms: u64,
    mono_ns: u64,
    tier: RiskTier,
    utilization_u_e6: u64,
    heavy_leg: Option<Outcome>,
) -> Result<(), Box<dyn std::error::Error>> {
    jsonl.write_event(
        now_ms,
        mono_ns,
        "risk_tier",
        &RiskTierEvent {
            tier,
            utilization_u_e6,
            heavy_leg,
        },
    )?;
    Ok(())
}

pub(crate) fn record_order_action_jsonl(
    jsonl: &mut JsonlWriter,
    now_ms: u64,
    mono_ns: u64,
    outcome: Outcome,
    action: &OrderAction,
    tau_secs: u64,
    telemetry: OrderActionTelemetry<'_>,
) -> Result<(), Box<dyn std::error::Error>> {
    jsonl.write_event(
        now_ms,
        mono_ns,
        "order_action",
        &OrderActionEvent {
            outcome,
            action: action.clone(),
            tau_secs,
            phase: telemetry.phase,
            cancel_all_reason: telemetry.cancel_all_reason,
            flatten_in_flight: telemetry.flatten_in_flight,
            best_bid_up_micro: telemetry.best_bid_up.map(|p| p.0),
            best_ask_up_micro: telemetry.best_ask_up.map(|p| p.0),
            best_bid_dn_micro: telemetry.best_bid_dn.map(|p| p.0),
            best_ask_dn_micro: telemetry.best_ask_dn.map(|p| p.0),
        },
    )?;
    Ok(())
}

pub(crate) fn record_cancel_all_jsonl(
    jsonl: &mut JsonlWriter,
    now_ms: u64,
    mono_ns: u64,
    record: CancelAllRecord<'_>,
) -> Result<(), Box<dyn std::error::Error>> {
    jsonl.write_event(
        now_ms,
        mono_ns,
        "cancel_all",
        &CancelAllEvent {
            market_slug: record.market_slug,
            phase: record.phase,
            tau_secs: record.tau_secs,
            canceled_count: record.canceled_count,
            flatten_in_flight: record.flatten_in_flight,
            best_bid_up_micro: record.best_bid_up.map(|p| p.0),
            best_ask_up_micro: record.best_ask_up.map(|p| p.0),
            best_bid_dn_micro: record.best_bid_dn.map(|p| p.0),
            best_ask_dn_micro: record.best_ask_dn.map(|p| p.0),
            reason: record.reason,
        },
    )?;
    Ok(())
}

pub(crate) fn record_breaker_jsonl(
    jsonl: &mut JsonlWriter,
    now_ms: u64,
    mono_ns: u64,
    status: BreakerStatus,
    clock_offset_ms: i64,
) -> Result<(), Box<dyn std::error::Error>> {
    jsonl.write_event(
        now_ms,
        mono_ns,
        "breaker",
        &BreakerEvent {
            status,
            clock_offset_ms,
        },
    )?;
    Ok(())
}

pub(crate) fn record_lifecycle_fill_jsonl(
    jsonl: &mut JsonlWriter,
    now_ms: u64,
    mono_ns: u64,
    market_slug: &str,
    fill: &SimFill,
) -> Result<(), Box<dyn std::error::Error>> {
    jsonl.write_event(
        now_ms,
        mono_ns,
        "fill",
        &LifecycleFillEvent {
            market_slug,
            sim_id: fill.sim_id,
            outcome: fill.outcome,
            side: fill.side.as_str(),
            price_micro: fill.price.0,
            filled_shares_micro: fill.filled_shares.0,
            fill_ts_ms: fill.fill_ts_ms,
            is_maker: fill.is_maker,
        },
    )?;
    Ok(())
}

pub(crate) fn record_lifecycle_merge_jsonl(
    jsonl: &mut JsonlWriter,
    now_ms: u64,
    mono_ns: u64,
    market_slug: &str,
    merge: &MergeEvent,
    total_merged: SharesMicro,
) -> Result<(), Box<dyn std::error::Error>> {
    jsonl.write_event(
        now_ms,
        mono_ns,
        "merge",
        &LifecycleMergeEvent {
            market_slug,
            merged_pairs_micro: merge.merged_pairs.0,
            avg_cost_up_micro: merge.avg_cost_up.0,
            avg_cost_dn_micro: merge.avg_cost_dn.0,
            pair_cost_micro: merge.pair_cost.0,
            realized_gain_micro: merge.realized_gain_micro,
            total_merged_micro: total_merged.0,
        },
    )?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn record_lifecycle_flatten_jsonl(
    jsonl: &mut JsonlWriter,
    now_ms: u64,
    mono_ns: u64,
    market_slug: &str,
    outcome: Outcome,
    side: OrderSide,
    price: MicroUsdc,
    size: SharesMicro,
    fee_micro: u64,
    realized_pnl_micro: i64,
    reason: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    jsonl.write_event(
        now_ms,
        mono_ns,
        "flatten",
        &LifecycleFlattenEvent {
            market_slug,
            outcome,
            side: side.as_str(),
            price_micro: price.0,
            size_micro: size.0,
            fee_micro,
            realized_pnl_micro,
            reason,
        },
    )?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn record_epoch_rollover_jsonl(
    jsonl: &mut JsonlWriter,
    now_ms: u64,
    mono_ns: u64,
    expired_slug: &str,
    next_slug: Option<&str>,
    winning_outcome: Outcome,
    settlement_pnl_delta_micro: i64,
    total_realized_pnl_micro: i64,
    pairs_merged_micro: u64,
    fees_paid_micro: u64,
    estimated_rebate_micro: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    jsonl.write_event(
        now_ms,
        mono_ns,
        "epoch_rollover",
        &EpochRolloverEvent {
            expired_slug,
            next_slug,
            winning_outcome,
            settlement_pnl_delta_micro,
            total_realized_pnl_micro,
            pairs_merged_micro,
            fees_paid_micro,
            estimated_rebate_micro,
        },
    )?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn record_lifecycle_summary_jsonl(
    jsonl: &mut JsonlWriter,
    now_ms: u64,
    mono_ns: u64,
    source: &str,
    market_slug: &str,
    stats: &LifecycleReportStats,
    pricing_lat: &LatencySummary,
    risk_lat: &LatencySummary,
    total_lat: &LatencySummary,
) -> Result<(), Box<dyn std::error::Error>> {
    let net_pnl_micro = stats
        .realized_pnl_micro
        .saturating_add(stats.unrealized_pnl_micro);
    jsonl.write_event(
        now_ms,
        mono_ns,
        "summary",
        &LifecycleSummaryEvent {
            source,
            market_slug,
            ticks_evaluated: stats.ticks_evaluated,
            phase_transitions: stats.phase_transitions,
            orders_submitted: stats.orders_submitted,
            orders_replaced: stats.orders_replaced,
            orders_canceled: stats.orders_canceled,
            orders_held: stats.orders_held,
            flattens_executed: stats.flattens_executed,
            breaker_trips: stats.breaker_trips,
            fills_count: stats.fills_count,
            merges_count: stats.merges_count,
            total_pairs_merged_micro: stats.total_pairs_merged.0,
            q_up_micro: stats.final_q_up.0,
            q_dn_micro: stats.final_q_dn.0,
            net_delta_micro: stats.final_net_delta_micro,
            realized_pnl_micro: stats.realized_pnl_micro,
            unrealized_pnl_micro: stats.unrealized_pnl_micro,
            net_pnl_micro,
            settlement_pnl_micro: stats.settlement_pnl_micro,
            fees_paid_micro: stats.fees_paid_micro,
            estimated_rebate_micro: stats.estimated_rebate_micro,
            epochs_completed: stats.epochs_completed,
            epoch_rollovers: stats.epoch_rollovers,
            rpm_60s: stats.rpm_60s,
            fill_ratio: stats.fill_ratio,
            markout_1s_bps: stats.markout_1s_bps,
            markout_5s_bps: stats.markout_5s_bps,
            baseline_pnl_micro: stats.baseline_pnl_micro,
            pricing_latency: *pricing_lat,
            risk_latency: *risk_lat,
            total_latency: *total_lat,
        },
    )?;
    jsonl.flush()?;
    Ok(())
}

fn fmt_pnl(micro: i64) -> String {
    let sign = if micro < 0 { "-" } else { "+" };
    let abs = micro.unsigned_abs();
    let dollars = abs / MICRO_SCALE;
    let rem = abs % MICRO_SCALE;
    format!("{sign}${dollars}.{rem:06}")
}
