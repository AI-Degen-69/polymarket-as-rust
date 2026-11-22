use std::path::Path;

use core_types::{
    JsonlWriter, LatencySummary, MICRO_SCALE, MicroUsdc, OrderSide, Outcome, SharesMicro,
};
use math_engine::{QuoteOutput, SuppressionReason};
use paper_sim::{MergeEvent, SimFill};
use risk_lifecycle::{BreakerStatus, EpochPhase, OrderAction, RiskActorTrace, RiskTier};
use serde::Serialize;

use crate::theme::{self, Badge, Theme};
use crate::window_gate::format_utc_hms;

pub(crate) const PRICING_BUDGET_US: u64 = 25;
pub(crate) const RISK_BUDGET_US: u64 = 10;
pub(crate) const TOTAL_BUDGET_US: u64 = 100;
pub(crate) const HEARTBEAT_NORMAL_MS: u64 = 5_000;
pub(crate) const HEARTBEAT_FAST_MS: u64 = 1_000;

/// Fixed column widths for stream rows: event badge, phase badge, tier badge.
const EVENT_BADGE_WIDTH: usize = 10;
const PHASE_BADGE_WIDTH: usize = 10;
const TIER_BADGE_WIDTH: usize = 4;
/// Fixed value width keeps `Pricing` / `Risk` / `Total` labels aligned across latency rows.
const LATENCY_VALUE_WIDTH: usize = 10;

/// Content of the middle badge column: epoch phase, or any other badge such as `[MAKER]`.
#[derive(Debug, Clone, Copy)]
enum MidColumn {
    Phase(EpochPhase),
    Badge(Badge),
}

/// Builds the canonical prefix shared by every stream row: `HH:MM:SSZ T-NNNs [EVENT] [MID] [TIER] `.
/// Missing badges leave their column blank so positions never shift.
fn row_prefix(
    t: Theme,
    utc: &str,
    tau_secs: u64,
    event: Option<Badge>,
    mid: Option<MidColumn>,
    tier: Option<RiskTier>,
) -> String {
    let event = event.map_or_else(
        || " ".repeat(EVENT_BADGE_WIDTH),
        |kind| {
            let pad = EVENT_BADGE_WIDTH.saturating_sub(kind.label().chars().count());
            format!("{}{}", t.badge(kind), " ".repeat(pad))
        },
    );
    let mid = mid.map_or_else(
        || " ".repeat(PHASE_BADGE_WIDTH),
        |mid| match mid {
            MidColumn::Phase(phase) => t.phase_badge(phase),
            MidColumn::Badge(kind) => {
                let pad = PHASE_BADGE_WIDTH.saturating_sub(kind.label().chars().count());
                format!("{}{}", t.badge(kind), " ".repeat(pad))
            }
        },
    );
    let tier = tier.map_or_else(|| " ".repeat(TIER_BADGE_WIDTH), |tier| t.tier_badge(tier));
    format!("{} T-{:03}s {event}{mid} {tier} ", t.muted(utc), tau_secs)
}

/// Terminal output mode for `poly-as lifecycle`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LifecycleDisplayMode {
    Stream,
    Raw,
}

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

impl LifecycleReportStats {
    pub(crate) fn set_final_inventory(&mut self, q_up: SharesMicro, q_dn: SharesMicro) {
        self.final_q_up = q_up;
        self.final_q_dn = q_dn;
        self.final_net_delta_micro = (q_up.as_u64() as i64) - (q_dn.as_u64() as i64);
    }

    fn resolved_net_delta_micro(&self) -> i64 {
        if self.final_net_delta_micro != 0 || self.final_q_up == self.final_q_dn {
            self.final_net_delta_micro
        } else {
            (self.final_q_up.as_u64() as i64) - (self.final_q_dn.as_u64() as i64)
        }
    }
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
    display: LifecycleDisplayMode,
    v_max_usd: u64,
    base_size: u64,
    fast_forward: bool,
    fault: &str,
    jsonl_path: &Path,
) {
    let t = theme::current();
    let display_label = match display {
        LifecycleDisplayMode::Stream => "stream (events + 5s/1s heartbeat)",
        LifecycleDisplayMode::Raw => "raw (every 10 Hz tick)",
    };
    println!();
    println!(
        "{}",
        t.banner(
            "POLY-AS PHASE 4 :: RISK & 300s LIFECYCLE ACTORS",
            Some(&format!("Source: {source}  |  Display: {display_label}"))
        )
    );
    println!(
        "{}",
        t.kv(
            "Dollar Cap (V_max):",
            &format!("${v_max_usd}.00 (MicroUsdc)")
        )
    );
    println!(
        "{}",
        t.kv("Base Order Size:", &format!("{base_size} shares"))
    );
    println!("{}", t.kv("Fast-Forward:", &fast_forward.to_string()));
    println!("{}", t.kv("Fault Injection:", fault));
    println!(
        "{}",
        t.kv("JSONL Telemetry:", &jsonl_path.display().to_string())
    );
    println!(
        "{}",
        t.kv(
            "Actor Topology:",
            "Lock-free ArcSwap (Pricing -> Risk) + Priority Bounded Channels"
        )
    );
    println!(
        "{}",
        t.kv(
            "Core Pinning:",
            "Pricing OS-Thread, Risk OS-Thread, Execution Tokio Task"
        )
    );
    println!(
        "{}",
        t.kv("High-Res Timer:", "Active (timeBeginPeriod 1ms / QPC)")
    );
    println!("{}", t.divider());
    match display {
        LifecycleDisplayMode::Stream => println!(
            "{}",
            t.muted(
                "UTC       Tau    Phase      Tier  Util | BTC vs Strike | UP BBO    DN BBO    | Quotes (Pair)  | Inv      | PnL"
            )
        ),
        LifecycleDisplayMode::Raw => println!(
            "{}",
            t.muted(
                "UTC       Tau    Phase      Tier  Util  (qUP,qDN,d)     UP / DN Actions & Latency"
            )
        ),
    }
    println!("{}", t.divider());
}

/// Prints a prominent banner at the start of an epoch or upon rollover.
pub(crate) fn print_epoch_banner(
    epoch_num: usize,
    slug: &str,
    strike_usd: Option<f64>,
    start_ts_ms: u64,
    expiry_ts_ms: u64,
) {
    let t = theme::current();
    let strike_str = strike_usd.map_or_else(|| "pending".to_string(), |s| format!("${s:.2}"));
    let start_utc = format_utc_hms(start_ts_ms);
    let end_utc = format_utc_hms(expiry_ts_ms);
    let span_secs = expiry_ts_ms.saturating_sub(start_ts_ms) / 1_000;
    println!();
    println!("{}", t.section(&format!("EPOCH #{epoch_num} :: {slug}")));
    println!(
        "  {} {}  {}  {} {} -> {} ({span_secs}s)",
        t.muted("Strike:"),
        t.bold(&strike_str),
        t.muted("|"),
        t.muted("Window:"),
        t.cyan(&start_utc),
        t.cyan(&end_utc),
    );
}

/// Data for a single lifecycle terminal evaluation tick.
pub(crate) struct LifecycleStepRow<'a> {
    pub(crate) now_ms: u64,
    pub(crate) tau_secs: u64,
    pub(crate) phase: EpochPhase,
    pub(crate) tier: RiskTier,
    pub(crate) u_e6: u64,
    pub(crate) q_up: SharesMicro,
    pub(crate) q_dn: SharesMicro,
    pub(crate) net_delta: i64,
    pub(crate) quote: QuoteOutput,
    pub(crate) spot_usd: Option<f64>,
    pub(crate) strike_usd: Option<f64>,
    pub(crate) resting_bid_up: Option<MicroUsdc>,
    pub(crate) resting_bid_dn: Option<MicroUsdc>,
    pub(crate) net_pnl_micro: i64,
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

/// Stateful CLI-side formatter separating high-signal events from periodic idle heartbeats.
pub(crate) struct LifecycleStreamReporter {
    display: LifecycleDisplayMode,
    theme: Theme,
    last_heartbeat_ms: Option<u64>,
    last_suppression: Option<SuppressionReason>,
    last_cancel_all_reason: Option<String>,
}

impl LifecycleStreamReporter {
    pub(crate) fn new(display: LifecycleDisplayMode, theme: Theme) -> Self {
        Self {
            display,
            theme,
            last_heartbeat_ms: None,
            last_suppression: None,
            last_cancel_all_reason: None,
        }
    }

    pub(crate) fn reset_epoch(&mut self) {
        self.last_heartbeat_ms = None;
        self.last_suppression = None;
        self.last_cancel_all_reason = None;
    }

    pub(crate) fn on_step(&mut self, row: &LifecycleStepRow<'_>) {
        for line in self.format_step(row) {
            println!("{line}");
        }
    }

    pub(crate) fn format_step(&mut self, row: &LifecycleStepRow<'_>) -> Vec<String> {
        match self.display {
            LifecycleDisplayMode::Raw => vec![self.format_raw_line(row)],
            LifecycleDisplayMode::Stream => self.format_stream_lines(row),
        }
    }

    fn format_raw_line(&self, row: &LifecycleStepRow<'_>) -> String {
        let t = self.theme;
        let utc = format_utc_hms(row.now_ms);
        let u_pct = (row.u_e6 as f64) / 10_000.0;
        let up_str = format_action_styled(t, row.action_up);
        let dn_str = format_action_styled(t, row.action_dn);
        let total_ns = row.pricing_ns.saturating_add(row.risk_ns);
        let suppr_note = match row.quote {
            QuoteOutput::Suppressed(reason) => {
                format!(" | {}", t.yellow(&format!("SUPPR({reason:?})")))
            }
            QuoteOutput::Active { .. } => String::new(),
        };

        let base = format!(
            "{} T-{:03}s {} {} {:>5.1}% ({:>2},{:>2},d={:>+2})  UP: {} | DN: {} [Risk: {}, Total: {}]{}",
            t.muted(&utc),
            row.tau_secs,
            t.phase_badge(row.phase),
            t.tier_badge(row.tier),
            u_pct,
            row.q_up.as_u64() / 1_000_000,
            row.q_dn.as_u64() / 1_000_000,
            row.net_delta,
            up_str,
            dn_str,
            t.fmt_latency_us(row.risk_ns, RISK_BUDGET_US),
            t.fmt_latency_us(total_ns, TOTAL_BUDGET_US),
            suppr_note,
        );

        if row.phase == EpochPhase::MandatoryFlatten || row.cancel_all_reason.is_some() {
            let reason = row.cancel_all_reason.unwrap_or("none");
            format!(
                "{base} | fif={} UP={} DN={} reason={}",
                row.flatten_in_flight,
                Theme::fmt_bbo(row.best_bid_up, row.best_ask_up),
                Theme::fmt_bbo(row.best_bid_dn, row.best_ask_dn),
                t.yellow(reason),
            )
        } else {
            base
        }
    }

    fn format_stream_lines(&mut self, row: &LifecycleStepRow<'_>) -> Vec<String> {
        let t = self.theme;
        let utc = format_utc_hms(row.now_ms);
        let mut lines = Vec::new();

        // 1. Suppression state start / change / resume
        match row.quote {
            QuoteOutput::Suppressed(reason) => {
                if self.last_suppression != Some(reason) {
                    let detail = match self.last_suppression {
                        Some(prev) => format!("{prev:?} -> {reason:?}"),
                        None => format!("{reason:?}"),
                    };
                    lines.push(format!(
                        "{}| quoting suppressed: {}",
                        row_prefix(
                            t,
                            &utc,
                            row.tau_secs,
                            Some(Badge::Suppr),
                            Some(MidColumn::Phase(row.phase)),
                            None
                        ),
                        t.yellow(&detail),
                    ));
                    self.last_suppression = Some(reason);
                }
            }
            QuoteOutput::Active { .. } => {
                if let Some(prev) = self.last_suppression.take() {
                    lines.push(format!(
                        "{}| quoting resumed (cleared {prev:?})",
                        row_prefix(
                            t,
                            &utc,
                            row.tau_secs,
                            Some(Badge::Resume),
                            Some(MidColumn::Phase(row.phase)),
                            None
                        ),
                    ));
                }
            }
        }

        // 2. Order actions or cancel-all transitions
        let has_up_action = !matches!(row.action_up, OrderAction::Hold);
        let has_dn_action = !matches!(row.action_dn, OrderAction::Hold);
        let prev_cancel_all = self.last_cancel_all_reason.take();
        let (cancel_all_changed, cleared_cancel_all) =
            match (row.cancel_all_reason, prev_cancel_all.as_deref()) {
                (Some(cur), Some(prev)) => (cur != prev, None),
                (Some(_), None) => (true, None),
                (None, Some(prev)) => (false, Some(prev.to_string())),
                (None, None) => (false, None),
            };
        self.last_cancel_all_reason = row.cancel_all_reason.map(str::to_string);

        if let Some(prev) = cleared_cancel_all {
            lines.push(format!(
                "{}| cancel-all cleared (was {})",
                row_prefix(
                    t,
                    &utc,
                    row.tau_secs,
                    Some(Badge::Resume),
                    Some(MidColumn::Phase(row.phase)),
                    Some(row.tier)
                ),
                t.muted(&prev),
            ));
        }

        if has_up_action || has_dn_action || cancel_all_changed {
            let up_str = format_action_styled(t, row.action_up);
            let dn_str = format_action_styled(t, row.action_dn);
            let mut action_line = format!(
                "{}UP {} | DN {}",
                row_prefix(
                    t,
                    &utc,
                    row.tau_secs,
                    Some(Badge::Order),
                    Some(MidColumn::Phase(row.phase)),
                    Some(row.tier)
                ),
                up_str,
                dn_str,
            );
            if let Some(reason) = row.cancel_all_reason {
                action_line.push_str(&format!(
                    " | fif={} UP={} DN={} reason={}",
                    row.flatten_in_flight,
                    Theme::fmt_bbo(row.best_bid_up, row.best_ask_up),
                    Theme::fmt_bbo(row.best_bid_dn, row.best_ask_dn),
                    t.yellow(reason),
                ));
            }
            lines.push(action_line);
        }

        // 3. Latency breaches (compare raw nanoseconds; fixed-width values align the labels)
        let total_ns = row.pricing_ns.saturating_add(row.risk_ns);
        if row.pricing_ns > PRICING_BUDGET_US.saturating_mul(1_000)
            || row.risk_ns > RISK_BUDGET_US.saturating_mul(1_000)
            || total_ns > TOTAL_BUDGET_US.saturating_mul(1_000)
        {
            let ideal = t.muted(&format!(
                "(Ideal: Total < {TOTAL_BUDGET_US}us | Pricing < {PRICING_BUDGET_US}us AND Risk < {RISK_BUDGET_US}us)"
            ));
            lines.push(format!(
                "{}{} Pricing + {} Risk = {} Total | {ideal}",
                row_prefix(t, &utc, row.tau_secs, Some(Badge::Latency), None, None),
                t.fmt_latency_us_width(row.pricing_ns, PRICING_BUDGET_US, LATENCY_VALUE_WIDTH),
                t.fmt_latency_us_width(row.risk_ns, RISK_BUDGET_US, LATENCY_VALUE_WIDTH),
                t.fmt_latency_us_width(total_ns, TOTAL_BUDGET_US, LATENCY_VALUE_WIDTH),
            ));
        }

        // 4. Periodic 1-line heartbeat (1s during MandatoryFlatten/T3/T4, else 5s)
        let fast_heartbeat = row.phase == EpochPhase::MandatoryFlatten
            || matches!(row.tier, RiskTier::Tier3OneSided | RiskTier::Tier4HardStop);
        let interval_ms = if fast_heartbeat {
            HEARTBEAT_FAST_MS
        } else {
            HEARTBEAT_NORMAL_MS
        };
        if self
            .last_heartbeat_ms
            .is_none_or(|last| row.now_ms.saturating_sub(last) >= interval_ms)
        {
            self.last_heartbeat_ms = Some(row.now_ms);
            lines.push(self.format_heartbeat_line(row, &utc));
        }

        lines
    }

    fn format_heartbeat_line(&self, row: &LifecycleStepRow<'_>, utc: &str) -> String {
        let t = self.theme;
        let u_pct = (row.u_e6 as f64) / 10_000.0;
        let btc = t.fmt_spot_vs_strike(row.spot_usd, row.strike_usd);
        let up_bbo = Theme::fmt_bbo(row.best_bid_up, row.best_ask_up);
        let dn_bbo = Theme::fmt_bbo(row.best_bid_dn, row.best_ask_dn);
        let quotes = Theme::fmt_pair_quotes(row.resting_bid_up, row.resting_bid_dn);
        let q_up_sh = row.q_up.as_u64() / 1_000_000;
        let q_dn_sh = row.q_dn.as_u64() / 1_000_000;
        let inv = format!("{q_up_sh}/{q_dn_sh}/{:+}", row.net_delta);
        let pnl = t.fmt_pnl(row.net_pnl_micro);
        let sep = t.muted("|");

        format!(
            "{}{:>5.1}% {sep} BTC {} {sep} UP {} DN {} {sep} Q {} {sep} {} {sep} {}",
            row_prefix(
                t,
                utc,
                row.tau_secs,
                Some(Badge::Heartbeat),
                Some(MidColumn::Phase(row.phase)),
                Some(row.tier)
            ),
            u_pct,
            btc,
            up_bbo,
            dn_bbo,
            quotes,
            inv,
            pnl,
        )
    }
}

fn format_action_styled(t: Theme, action: &OrderAction) -> String {
    match action {
        OrderAction::Hold => t.muted("Hold"),
        OrderAction::SubmitNew {
            outcome,
            price,
            size,
        } => {
            let leg = match outcome {
                Outcome::Up => t.bold_green("UP"),
                Outcome::Down => t.bold_red("DN"),
            };
            format!("{} {} @ {}", fmt_shares_3dp(*size), leg, fmt_cents(*price))
        }
        OrderAction::Cancel { reason, .. } => t.yellow(&format!("Cancel({reason:?})")),
        OrderAction::CancelAndReplace {
            new_price,
            new_size,
            reason,
            ..
        } => t.cyan(&format!(
            "Replace({reason:?}) {}@{}",
            new_size.as_u64() / 1_000_000,
            Theme::fmt_price_compact(Some(*new_price))
        )),
    }
}

/// Formats micro-shares with up to 3 decimals, trimming trailing zeros (`5`, `0.01`, `2.5`).
fn fmt_shares_3dp(shares: SharesMicro) -> String {
    let raw = shares.as_u64();
    let whole = raw / 1_000_000;
    let mut digits = format!("{:06}", raw % 1_000_000);
    digits.truncate(3);
    while digits.ends_with('0') {
        digits.pop();
    }
    if digits.is_empty() {
        whole.to_string()
    } else {
        format!("{whole}.{digits}")
    }
}

/// Formats a dollar price as cents (`50c`), keeping up to 2 decimals for sub-cent prices.
fn fmt_cents(price: MicroUsdc) -> String {
    let cents = price.0 / 10_000;
    let mut digits = format!("{:02}", (price.0 % 10_000) / 100);
    while digits.ends_with('0') {
        digits.pop();
    }
    if digits.is_empty() {
        format!("{cents}c")
    } else {
        format!("{cents}.{digits}c")
    }
}

pub(crate) fn print_phase_transition(
    from: Option<EpochPhase>,
    to: EpochPhase,
    tau_secs: u64,
    reason: &str,
) {
    let t = theme::current();
    let from_label = from.map_or_else(|| "Init".to_string(), |p| format!("{p:?}"));
    println!();
    println!(
        "  {} {} -> {} (T-{tau_secs}s) | {}",
        t.badge(Badge::Phase),
        t.muted(&from_label),
        t.phase_badge(to),
        reason
    );
}

pub(crate) fn print_tier_transition(to: RiskTier, u_e6: u64, heavy: Option<Outcome>) {
    let t = theme::current();
    println!(
        "  {} {} u = {:.1}% | heavy: {:?}",
        t.badge(Badge::Tier),
        t.tier_badge(to),
        (u_e6 as f64) / 10_000.0,
        heavy
    );
}

pub(crate) fn print_breaker_trip(status: BreakerStatus) {
    let t = theme::current();
    println!();
    println!(
        "  {} Circuit breaker tripped: {}",
        t.badge(Badge::Breaker),
        t.bold_red(&format!("{status:?}"))
    );
}

pub(crate) fn print_lifecycle_fill(now_ms: u64, tau_secs: u64, tier: RiskTier, fill: &SimFill) {
    let t = theme::current();
    let utc = format_utc_hms(now_ms);
    let leg = match fill.outcome {
        Outcome::Up => t.bold_green("UP"),
        Outcome::Down => t.bold_red("DN"),
    };
    let side = match fill.side {
        OrderSide::Buy => t.bold_green("BUY"),
        OrderSide::Sell => t.bold_red("SELL"),
    };
    let liquidity = if fill.is_maker {
        Badge::Maker
    } else {
        Badge::Taker
    };
    println!(
        "{}#{} {} {} {:>9} @ {}",
        row_prefix(
            t,
            &utc,
            tau_secs,
            Some(Badge::Fill),
            Some(MidColumn::Badge(liquidity)),
            Some(tier)
        ),
        fill.sim_id,
        leg,
        side,
        fmt_shares_3dp(fill.filled_shares),
        fmt_cents(fill.price),
    );
}

pub(crate) fn print_lifecycle_merge(
    now_ms: u64,
    tau_secs: u64,
    phase: Option<EpochPhase>,
    tier: RiskTier,
    merge: &MergeEvent,
    total_merged: SharesMicro,
) {
    let t = theme::current();
    let utc = format_utc_hms(now_ms);
    println!(
        "{}pairs: {} | pair_cost={} (UP={}, DN={}) -> PnL={} (total={})",
        row_prefix(
            t,
            &utc,
            tau_secs,
            Some(Badge::Merge),
            phase.map(MidColumn::Phase),
            Some(tier),
        ),
        merge.merged_pairs,
        Theme::fmt_price_compact(Some(merge.pair_cost)),
        Theme::fmt_price_compact(Some(merge.avg_cost_up)),
        Theme::fmt_price_compact(Some(merge.avg_cost_dn)),
        t.fmt_pnl(merge.realized_gain_micro),
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
    let t = theme::current();
    let leg = match outcome {
        Outcome::Up => "UP",
        Outcome::Down => "DN",
    };
    println!(
        "  {} {} | {} {} {} @ {} | fee=${fee} | reason={}",
        t.badge(Badge::Flatten),
        t.muted(market_slug),
        t.bold(leg),
        side.as_str(),
        size,
        Theme::fmt_price_compact(Some(price)),
        t.yellow(reason)
    );
}

pub(crate) fn print_epoch_rollover(
    expired_slug: &str,
    next_slug: Option<&str>,
    winning_outcome: Outcome,
    settlement_pnl_delta_micro: i64,
    total_realized_pnl_micro: i64,
) {
    let t = theme::current();
    println!();
    println!(
        "  {} {} -> {} | winner={:?} | settle_delta={} | cumulative_realized={}",
        t.badge(Badge::Epoch),
        t.muted(expired_slug),
        t.cyan(next_slug.unwrap_or("awaiting-next")),
        winning_outcome,
        t.fmt_pnl(settlement_pnl_delta_micro),
        t.fmt_pnl(total_realized_pnl_micro)
    );
}

pub(crate) fn print_lifecycle_summary(
    stats: &LifecycleReportStats,
    pricing_lat: &LatencySummary,
    risk_lat: &LatencySummary,
    total_lat: &LatencySummary,
) {
    let t = theme::current();
    let net_pnl_micro = stats
        .realized_pnl_micro
        .saturating_add(stats.unrealized_pnl_micro);
    println!();
    println!(
        "{}",
        t.banner("PHASE 4 LIFECYCLE & RISK EXECUTION SUMMARY", None)
    );
    println!("{}", t.section("EXECUTION & ORDERS"));
    println!(
        "{}",
        t.kv("Ticks Evaluated:", &stats.ticks_evaluated.to_string())
    );
    println!(
        "{}",
        t.kv("Phase Transitions:", &stats.phase_transitions.to_string())
    );
    println!(
        "{}",
        t.kv(
            "Orders (Sub/Rep/Cxl):",
            &format!(
                "{} / {} / {} (held FIFO: {})",
                stats.orders_submitted,
                stats.orders_replaced,
                stats.orders_canceled,
                stats.orders_held
            )
        )
    );
    println!(
        "{}",
        t.kv(
            "Flattens / Breakers:",
            &format!("{} / {}", stats.flattens_executed, stats.breaker_trips)
        )
    );

    println!("{}", t.section("INVENTORY & PNL"));
    println!(
        "{}",
        t.kv(
            "Paper Fills / Merges:",
            &format!(
                "{} fills | {} merges ({} pairs)",
                stats.fills_count, stats.merges_count, stats.total_pairs_merged
            )
        )
    );
    println!(
        "{}",
        t.kv(
            "Final Inventory:",
            &format!(
                "UP={} | DN={} | Net={:+.6} sh",
                stats.final_q_up,
                stats.final_q_dn,
                stats.resolved_net_delta_micro() as f64 / MICRO_SCALE as f64
            )
        )
    );
    println!(
        "{}",
        t.kv(
            "PnL (Real / Unreal):",
            &format!(
                "{} / {} | Net={}",
                t.fmt_pnl(stats.realized_pnl_micro),
                t.fmt_pnl(stats.unrealized_pnl_micro),
                t.fmt_pnl(net_pnl_micro)
            )
        )
    );
    println!(
        "{}",
        t.kv(
            "Settle / Fees / Reb:",
            &format!(
                "Settle={} | Fees=${} | EstRebate=${}",
                t.fmt_pnl(stats.settlement_pnl_micro),
                MicroUsdc(stats.fees_paid_micro),
                MicroUsdc(stats.estimated_rebate_micro)
            )
        )
    );
    println!(
        "{}",
        t.kv(
            "Epochs / Telemetry:",
            &format!(
                "epochs={} rollovers={} | RPM={} | Fill={:.2}% | MO(1s/5s)={:+.1}/{:+.1}bps | Base={}",
                stats.epochs_completed,
                stats.epoch_rollovers,
                stats.rpm_60s,
                stats.fill_ratio * 100.0,
                stats.markout_1s_bps,
                stats.markout_5s_bps,
                t.fmt_pnl(stats.baseline_pnl_micro)
            )
        )
    );

    println!(
        "{}",
        t.section("COMPUTE LATENCY HDR (P50 / P95 / P99 / MAX)")
    );
    println!(
        "{}",
        t.kv(
            "Pricing Pipeline:",
            &format!(
                "{}us / {}us / {} / {}us (Target P99 < {PRICING_BUDGET_US}us)",
                pricing_lat.p50_ns / 1_000,
                pricing_lat.p95_ns / 1_000,
                t.fmt_latency_us(pricing_lat.p99_ns, PRICING_BUDGET_US),
                pricing_lat.max_ns / 1_000
            )
        )
    );
    println!(
        "{}",
        t.kv(
            "Risk & Hysteresis:",
            &format!(
                "{}us / {}us / {} / {}us (Target P99 < {RISK_BUDGET_US}us)",
                risk_lat.p50_ns / 1_000,
                risk_lat.p95_ns / 1_000,
                t.fmt_latency_us(risk_lat.p99_ns, RISK_BUDGET_US),
                risk_lat.max_ns / 1_000
            )
        )
    );
    println!(
        "{}",
        t.kv(
            "Total Internal Path:",
            &format!(
                "{}us / {}us / {} / {}us (Target P99 < {TOTAL_BUDGET_US}us)",
                total_lat.p50_ns / 1_000,
                total_lat.p95_ns / 1_000,
                t.fmt_latency_us(total_lat.p99_ns, TOTAL_BUDGET_US),
                total_lat.max_ns / 1_000
            )
        )
    );
    println!("{}", t.divider());
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
            net_delta_micro: stats.resolved_net_delta_micro(),
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

#[cfg(test)]
mod tests {
    use super::*;
    use risk_lifecycle::CancelReason;

    fn sample_row<'a>(
        now_ms: u64,
        tau_secs: u64,
        action_up: &'a OrderAction,
        action_dn: &'a OrderAction,
    ) -> LifecycleStepRow<'a> {
        LifecycleStepRow {
            now_ms,
            tau_secs,
            phase: EpochPhase::ActiveQuoting,
            tier: RiskTier::Tier1Normal,
            u_e6: 120_000,
            q_up: SharesMicro::from_raw(20 * 1_000_000),
            q_dn: SharesMicro::ZERO,
            net_delta: 20,
            quote: QuoteOutput::Active {
                bid_up: Some(MicroUsdc(500_000)),
                bid_dn: Some(MicroUsdc(470_000)),
                pair_cost_micro: Some(MicroUsdc(970_000)),
                gross_margin_bps: Some(300),
            },
            spot_usd: Some(90_014.0),
            strike_usd: Some(90_000.0),
            resting_bid_up: Some(MicroUsdc(500_000)),
            resting_bid_dn: Some(MicroUsdc(470_000)),
            net_pnl_micro: 200_000,
            action_up,
            action_dn,
            cancel_all_reason: None,
            flatten_in_flight: false,
            best_bid_up: Some(MicroUsdc(510_000)),
            best_ask_up: Some(MicroUsdc(520_000)),
            best_bid_dn: Some(MicroUsdc(480_000)),
            best_ask_dn: Some(MicroUsdc(490_000)),
            pricing_ns: 12_000,
            risk_ns: 4_000,
        }
    }

    #[test]
    fn stream_helpers_format_sizes_and_cents() {
        assert_eq!(fmt_shares_3dp(SharesMicro::from_raw(5_000_000)), "5");
        assert_eq!(fmt_shares_3dp(SharesMicro::from_raw(10_000)), "0.01");
        assert_eq!(fmt_shares_3dp(SharesMicro::from_raw(2_500_000)), "2.5");
        assert_eq!(fmt_shares_3dp(SharesMicro::from_raw(1_234_560)), "1.234");
        assert_eq!(fmt_cents(MicroUsdc(500_000)), "50c");
        assert_eq!(fmt_cents(MicroUsdc(550_000)), "55c");
        assert_eq!(fmt_cents(MicroUsdc(555_000)), "55.5c");
    }

    #[test]
    fn stream_rows_share_canonical_badge_columns() {
        let t = Theme::plain();
        let order = row_prefix(
            t,
            "00:00:00Z",
            300,
            Some(Badge::Order),
            Some(MidColumn::Phase(EpochPhase::ActiveQuoting)),
            Some(RiskTier::Tier1Normal),
        );
        let latency = row_prefix(t, "00:00:00Z", 300, Some(Badge::Latency), None, None);
        let heartbeat = row_prefix(
            t,
            "00:00:00Z",
            300,
            Some(Badge::Heartbeat),
            Some(MidColumn::Phase(EpochPhase::ActiveQuoting)),
            Some(RiskTier::Tier1Normal),
        );
        assert_eq!(order.chars().count(), latency.chars().count());
        assert_eq!(order.chars().count(), heartbeat.chars().count());
        // Compare character columns, not byte offsets: `♥` is multi-byte.
        let quote_col = order.find("[QUOTE]").map(|i| order[..i].chars().count());
        let hb_quote_col = heartbeat
            .find("[QUOTE]")
            .map(|i| heartbeat[..i].chars().count());
        assert_eq!(hb_quote_col, quote_col);
        let tier_col = order.find("[T1]").map(|i| order[..i].chars().count());
        let hb_tier_col = heartbeat
            .find("[T1]")
            .map(|i| heartbeat[..i].chars().count());
        assert_eq!(hb_tier_col, tier_col);
    }

    #[test]
    fn stream_mode_suppresses_10hz_hold_noise_and_respects_5s_and_1s_heartbeats() {
        let mut rep = LifecycleStreamReporter::new(LifecycleDisplayMode::Stream, Theme::plain());
        let hold = OrderAction::Hold;
        let base_ms = 1_795_345_200_000_u64;

        // Tick 0: initial heartbeat emitted.
        let l0 = rep.format_step(&sample_row(base_ms, 300, &hold, &hold));
        assert_eq!(l0.len(), 1);
        assert!(l0[0].contains("[QUOTE]"));
        assert!(l0[0].contains("♥ [HB]"));
        assert!(l0[0].contains("+$14 (+2bp)"));
        assert!(l0[0].contains("UP .51/.52 DN .48/.49"));
        assert!(l0[0].contains("Q .50/.47 (.97)"));
        assert!(l0[0].contains("20/0/+20"));
        assert!(l0[0].contains("+$0.20"));
        assert!(l0[0].chars().count() <= 135);

        // 100ms..4900ms idle Hold ticks emit nothing.
        for offset in (100..5_000).step_by(100) {
            let lines = rep.format_step(&sample_row(base_ms + offset, 299, &hold, &hold));
            assert!(
                lines.is_empty(),
                "unexpected output at +{offset}ms: {lines:?}"
            );
        }

        // At +5000ms: next heartbeat fires.
        let l5 = rep.format_step(&sample_row(base_ms + 5_000, 295, &hold, &hold));
        assert_eq!(l5.len(), 1);

        // MandatoryFlatten switches heartbeat cadence to 1s.
        let mut flatten_row = sample_row(base_ms + 6_000, 28, &hold, &hold);
        flatten_row.phase = EpochPhase::MandatoryFlatten;
        let lf1 = rep.format_step(&flatten_row);
        assert_eq!(lf1.len(), 1);
        assert!(lf1[0].contains("[FLATTEN]"));

        // +500ms into MandatoryFlatten is silent, +1000ms emits.
        flatten_row.now_ms = base_ms + 6_500;
        assert!(rep.format_step(&flatten_row).is_empty());
        flatten_row.now_ms = base_ms + 7_000;
        assert_eq!(rep.format_step(&flatten_row).len(), 1);
    }

    #[test]
    fn stream_mode_emits_immediately_on_actions_suppressions_and_latency_breaches() {
        let mut rep = LifecycleStreamReporter::new(LifecycleDisplayMode::Stream, Theme::plain());
        let hold = OrderAction::Hold;
        let base_ms = 1_795_345_200_000_u64;

        // Seed initial heartbeat.
        assert_eq!(
            rep.format_step(&sample_row(base_ms, 300, &hold, &hold))
                .len(),
            1
        );

        // Suppression start emits immediately; repeat is deduplicated; change emits; resume emits.
        let mut suppr_row = sample_row(base_ms + 200, 300, &hold, &hold);
        suppr_row.quote = QuoteOutput::Suppressed(SuppressionReason::WarmingUp);
        let s1 = rep.format_step(&suppr_row);
        assert_eq!(s1.len(), 1);
        assert!(s1[0].contains("[SUPPR]"));
        assert!(s1[0].contains("WarmingUp"));

        suppr_row.now_ms = base_ms + 300;
        assert!(rep.format_step(&suppr_row).is_empty());

        suppr_row.now_ms = base_ms + 400;
        suppr_row.quote = QuoteOutput::Suppressed(SuppressionReason::CrossedOrderBook);
        let s2 = rep.format_step(&suppr_row);
        assert_eq!(s2.len(), 1);
        assert!(s2[0].contains("WarmingUp -> CrossedOrderBook"));

        let resume_row = sample_row(base_ms + 500, 299, &hold, &hold);
        let r1 = rep.format_step(&resume_row);
        assert_eq!(r1.len(), 1);
        assert!(r1[0].contains("[RESUME]"));
        assert!(r1[0].contains("CrossedOrderBook"));

        // Order action emits immediately between heartbeats.
        let submit = OrderAction::SubmitNew {
            outcome: Outcome::Up,
            price: MicroUsdc(500_000),
            size: SharesMicro::from_raw(20 * 1_000_000),
        };
        let cancel = OrderAction::Cancel {
            outcome: Outcome::Down,
            order_id: "ord-1".to_string(),
            reason: CancelReason::RiskSuppressed,
        };
        let act_lines = rep.format_step(&sample_row(base_ms + 600, 299, &submit, &cancel));
        assert_eq!(act_lines.len(), 1);
        assert!(act_lines[0].contains("[ORDER]"));
        assert!(act_lines[0].contains("20 UP @ 50c"));
        assert!(act_lines[0].contains("Cancel(RiskSuppressed)"));

        // Latency breach emits immediately.
        let mut lat_row = sample_row(base_ms + 700, 299, &hold, &hold);
        lat_row.pricing_ns = 31_000;
        let lat_lines = rep.format_step(&lat_row);
        assert_eq!(lat_lines.len(), 1);
        assert!(lat_lines[0].contains("[LATENCY]"));
        assert!(lat_lines[0].contains("31us"));
        assert!(lat_lines[0].contains("Pricing"));
    }

    #[test]
    fn raw_mode_prints_every_tick_and_theme_toggles_ansi() {
        let hold = OrderAction::Hold;
        let row = sample_row(1_795_345_200_000, 300, &hold, &hold);

        let mut plain_raw = LifecycleStreamReporter::new(LifecycleDisplayMode::Raw, Theme::plain());
        let p1 = plain_raw.format_step(&row);
        let p2 = plain_raw.format_step(&row);
        assert_eq!(p1.len(), 1);
        assert_eq!(p2.len(), 1);
        assert!(!p1[0].contains('\x1b'));
        assert!(p1[0].contains("Hold"));

        let mut styled_raw =
            LifecycleStreamReporter::new(LifecycleDisplayMode::Raw, Theme::styled());
        let s1 = styled_raw.format_step(&row);
        assert_eq!(s1.len(), 1);
        assert!(s1[0].contains('\x1b'));
    }

    #[test]
    fn stream_mode_emits_cancel_all_clear_notice_and_deduplicates_repeats() {
        let mut rep = LifecycleStreamReporter::new(LifecycleDisplayMode::Stream, Theme::plain());
        let hold = OrderAction::Hold;
        let base_ms = 1_795_345_200_000_u64;

        // Seed initial heartbeat with cancel_all_reason = None.
        assert_eq!(
            rep.format_step(&sample_row(base_ms, 300, &hold, &hold))
                .len(),
            1
        );

        // Transition None -> Some("circuit_breaker"): emits [ORDER] with reason.
        let mut active_row = sample_row(base_ms + 100, 300, &hold, &hold);
        active_row.cancel_all_reason = Some("circuit_breaker");
        let l1 = rep.format_step(&active_row);
        assert_eq!(l1.len(), 1);
        assert!(l1[0].contains("[ORDER]"));
        assert!(l1[0].contains("reason=circuit_breaker"));

        // Repeated Some("circuit_breaker") stays silent.
        active_row.now_ms = base_ms + 200;
        assert!(rep.format_step(&active_row).is_empty());

        // Transition Some("circuit_breaker") -> Some("stale_feed"): emits [ORDER] with new reason.
        active_row.now_ms = base_ms + 300;
        active_row.cancel_all_reason = Some("stale_feed");
        let l2 = rep.format_step(&active_row);
        assert_eq!(l2.len(), 1);
        assert!(l2[0].contains("reason=stale_feed"));

        // Transition Some("stale_feed") -> None: emits recovery/clear notice.
        let cleared_row = sample_row(base_ms + 400, 299, &hold, &hold);
        let l3 = rep.format_step(&cleared_row);
        assert_eq!(l3.len(), 1);
        assert!(l3[0].contains("[RESUME]"));
        assert!(l3[0].contains("cancel-all cleared (was stale_feed)"));

        // Repeated None stays silent.
        assert!(
            rep.format_step(&sample_row(base_ms + 500, 299, &hold, &hold))
                .is_empty()
        );
    }

    #[test]
    fn stream_mode_detects_sub_microsecond_latency_breaches() {
        let mut rep = LifecycleStreamReporter::new(LifecycleDisplayMode::Stream, Theme::plain());
        let hold = OrderAction::Hold;
        let base_ms = 1_795_345_200_000_u64;

        // Seed initial heartbeat.
        assert_eq!(
            rep.format_step(&sample_row(base_ms, 300, &hold, &hold))
                .len(),
            1
        );

        // Exact pricing (25us) and risk (10us) budgets stay silent.
        let mut exact_row = sample_row(base_ms + 100, 300, &hold, &hold);
        exact_row.pricing_ns = PRICING_BUDGET_US * 1_000;
        exact_row.risk_ns = RISK_BUDGET_US * 1_000;
        assert!(rep.format_step(&exact_row).is_empty());

        // Pricing +1ns over budget (25_001ns) triggers [LATENCY] event.
        let mut pricing_over = sample_row(base_ms + 200, 300, &hold, &hold);
        pricing_over.pricing_ns = PRICING_BUDGET_US * 1_000 + 1;
        pricing_over.risk_ns = 4_000;
        let p_lines = rep.format_step(&pricing_over);
        assert_eq!(p_lines.len(), 1);
        assert!(p_lines[0].contains("[LATENCY]"));
        assert!(p_lines[0].contains("25.001us"));
        assert!(p_lines[0].contains("Pricing"));

        // Risk +1ns over budget (10_001ns) triggers [LATENCY] event.
        let mut risk_over = sample_row(base_ms + 300, 300, &hold, &hold);
        risk_over.pricing_ns = 10_000;
        risk_over.risk_ns = RISK_BUDGET_US * 1_000 + 1;
        let r_lines = rep.format_step(&risk_over);
        assert_eq!(r_lines.len(), 1);
        assert!(r_lines[0].contains("[LATENCY]"));
        assert!(r_lines[0].contains("10.001us"));
        assert!(r_lines[0].contains("Risk"));
    }
}
