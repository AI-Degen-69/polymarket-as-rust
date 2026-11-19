use core_types::{JsonlWriter, LatencySummary, Outcome, SharesMicro};
use risk_lifecycle::{BreakerStatus, EpochPhase, OrderAction, RiskTier};
use serde::Serialize;
use std::path::Path;

/// Statistics gathered during the lifecycle evaluation run.
#[derive(Debug, Default)]
pub(crate) struct LifecycleReportStats {
    pub(crate) ticks_evaluated: usize,
    pub(crate) orders_submitted: usize,
    pub(crate) orders_canceled: usize,
    pub(crate) orders_replaced: usize,
    pub(crate) orders_held: usize,
    pub(crate) flattens_executed: usize,
    pub(crate) phase_transitions: usize,
    pub(crate) breaker_trips: usize,
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

#[derive(Serialize)]
struct OrderActionEvent {
    outcome: Outcome,
    action: OrderAction,
    tau_secs: u64,
}

#[derive(Serialize)]
struct BreakerEvent {
    status: BreakerStatus,
    clock_offset_ms: i64,
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
    pub(crate) pricing_ns: u64,
    pub(crate) risk_ns: u64,
}

pub(crate) fn print_lifecycle_step(row: &LifecycleStepRow<'_>) {
    let u_pct = (row.u_e6 as f64) / 10_000.0;
    let up_str = format_action(row.action_up);
    let dn_str = format_action(row.action_dn);

    let total_us = (row.pricing_ns + row.risk_ns) / 1_000;
    let risk_us = row.risk_ns / 1_000;

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

pub(crate) fn print_lifecycle_summary(
    stats: &LifecycleReportStats,
    pricing_lat: &LatencySummary,
    risk_lat: &LatencySummary,
    total_lat: &LatencySummary,
) {
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
) -> Result<(), Box<dyn std::error::Error>> {
    jsonl.write_event(
        now_ms,
        mono_ns,
        "order_action",
        &OrderActionEvent {
            outcome,
            action: action.clone(),
            tau_secs,
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
