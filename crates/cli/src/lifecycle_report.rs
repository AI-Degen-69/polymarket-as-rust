use std::path::Path;

use core_types::{
    JsonlWriter, LatencySummary, MICRO_SCALE, MicroUsdc, OrderSide, Outcome, SharesMicro,
};
use math_engine::{QuoteOutput, SuppressionReason};
use paper_sim::{MergeEvent, SimFill};
use risk_lifecycle::{
    BreakerReason, BreakerStatus, EpochPhase, OrderAction, RiskActorTrace, RiskTier,
};
use serde::Serialize;

use crate::theme::{self, Badge, Theme};
use crate::window_gate::format_utc_hms;

pub(crate) const PRICING_BUDGET_US: u64 = 25;
pub(crate) const RISK_BUDGET_US: u64 = 10;
pub(crate) const TOTAL_BUDGET_US: u64 = 100;
pub(crate) const HEARTBEAT_NORMAL_MS: u64 = 5_000;
pub(crate) const HEARTBEAT_FAST_MS: u64 = 1_000;

/// Fixed column widths for stream rows: event badge, phase badge, tier badge.
const EVENT_BADGE_WIDTH: usize = 9;
const PHASE_BADGE_WIDTH: usize = 10;
const TIER_BADGE_WIDTH: usize = 4;
/// Fixed leg column so `UP` / `DN` actions line up even when one side holds or cancels.
const LEG_WIDTH: usize = 21;
/// Fixed status column for suppression/resume notices; matches `LEG_WIDTH` so columns align with `[ORDER]` rows.
const STATUS_WIDTH: usize = LEG_WIDTH;
/// Fixed value width keeps `Pricing` / `Risk` / `Total` labels aligned across latency rows.
const LATENCY_VALUE_WIDTH: usize = 10;
/// Nominal latency rows are reported as a health check at most this often.
const LATENCY_PERIOD_MS: u64 = 10_000;
/// Minor latency breaches (`<= 2x` budget) share a cooldown so Windows timer jitter does not spam.
const LATENCY_MINOR_BREACH_COOLDOWN_MS: u64 = 5_000;
/// Width budget for phase and order rows; longer trailing text is ellipsized.
const ROW_WIDTH_BUDGET: usize = 130;
/// Label column for the framed epoch block (`★ EPOCH ROLLOVER`, `◆ EPOCH #2`).
const EPOCH_LABEL_WIDTH: usize = 19;

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
        |kind| theme::pad_visible_right(&t.badge(kind), EVENT_BADGE_WIDTH),
    );
    let mid = mid.map_or_else(
        || " ".repeat(PHASE_BADGE_WIDTH),
        |mid| match mid {
            MidColumn::Phase(phase) => t.phase_badge(phase),
            MidColumn::Badge(kind) => theme::pad_visible_right(&t.badge(kind), PHASE_BADGE_WIDTH),
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
        LifecycleDisplayMode::Stream => println!("{}", format_stream_header(t)),
        LifecycleDisplayMode::Raw => println!(
            "{}",
            t.muted(
                "UTC       Tau    Phase      Tier  Util  (qUP,qDN,d)     UP / DN Actions & Latency"
            )
        ),
    }
    println!("{}", t.divider());
}

fn format_stream_header(t: Theme) -> String {
    let bar = t.bar();
    t.muted(&format!(
        "UTC       Tau    Event    Phase      Tier Util   {bar} BTC      {bar} Bk (bid/ask)      {bar} Q (ours)   {bar} Inv (UP/DN)  {bar} PnL"
    ))
}

/// Prints a framed banner anchoring a new epoch in the scrollback.
pub(crate) fn print_epoch_banner(
    epoch_num: usize,
    slug: &str,
    strike_usd: Option<f64>,
    start_ts_ms: u64,
    expiry_ts_ms: u64,
) {
    println!();
    println!(
        "{}",
        format_epoch_banner(
            theme::current(),
            epoch_num,
            slug,
            strike_usd,
            start_ts_ms,
            expiry_ts_ms,
        )
    );
}

fn format_epoch_banner(
    t: Theme,
    epoch_num: usize,
    slug: &str,
    strike_usd: Option<f64>,
    start_ts_ms: u64,
    expiry_ts_ms: u64,
) -> String {
    let strike_str = strike_usd.map_or_else(
        || "pending".to_string(),
        |s| format!("${}", fmt_usd_grouped(s)),
    );
    let start_utc = format_utc_hms(start_ts_ms);
    let end_utc = format_utc_hms(expiry_ts_ms);
    let span_secs = expiry_ts_ms.saturating_sub(start_ts_ms) / 1_000;
    let label = epoch_label(t, t.epoch_icon(false), &format!("EPOCH #{epoch_num}"));
    let bar = t.bar();
    let rule = t.epoch_rule();
    format!(
        "{rule}\n{label}{bar} {} {bar} {} {} {bar} {} {} {} {} ({span_secs}s)\n{rule}",
        t.cyan(slug),
        t.muted("Strike:"),
        t.bold(&strike_str),
        t.muted("Window:"),
        t.cyan(&start_utc),
        t.arrow(),
        t.cyan(&end_utc),
    )
}

/// Groups the integer part of a USD amount with thousands separators (`$61,295.92`).
fn fmt_usd_grouped(value: f64) -> String {
    let raw = format!("{value:.2}");
    let (whole, cents) = raw.split_once('.').unwrap_or((raw.as_str(), "00"));
    format!("{}.{cents}", group_thousands(whole))
}

/// Padded, colored label that anchors the framed epoch blocks.
fn epoch_label(t: Theme, icon: &str, title: &str) -> String {
    let raw = format!("  {icon} {title}");
    let padded = format!(
        "{raw}{}",
        " ".repeat(EPOCH_LABEL_WIDTH.saturating_sub(raw.chars().count()))
    );
    t.bold_cyan(&padded)
}

/// `UP` / `DOWN` label for epoch winners.
fn outcome_label(outcome: Outcome) -> &'static str {
    match outcome {
        Outcome::Up => "UP",
        Outcome::Down => "DOWN",
    }
}

fn short_epoch_transition(t: Theme, expired: &str, next: &str) -> String {
    match (expired.rsplit_once('-'), next.rsplit_once('-')) {
        (Some((expired_prefix, _)), Some((next_prefix, next_tail)))
            if expired_prefix == next_prefix =>
        {
            format!("{} {} {}", t.cyan(expired), t.arrow(), t.cyan(next_tail))
        }
        _ => format!("{} {} {}", t.cyan(expired), t.arrow(), t.cyan(next)),
    }
}

/// Human text for a breaker trip reason (never Rust enum debug output).
fn breaker_reason_text(reason: BreakerReason) -> &'static str {
    match reason {
        BreakerReason::ConsecutiveApiErrors => "consecutive API errors exceeded the limit",
        BreakerReason::PolymarketFeedStale => "Polymarket feed stale (>3s without a message)",
        BreakerReason::BinanceFeedStale => "Binance BTC feed stale (>2s without a message)",
        BreakerReason::ClockDriftExceeded => "clock drift beyond the 1s tolerance",
        BreakerReason::TickSizeChangeFreeze => "tick size changed; awaiting pricing sync",
        BreakerReason::SupervisorKillSwitch => "supervisor kill-switch engaged",
    }
}

/// Station 6 per-leg suppression flags and entry-guard bounds for explaining paused quotes.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct SuppressionContext {
    pub(crate) bid_up_floored: Option<MicroUsdc>,
    pub(crate) bid_dn_floored: Option<MicroUsdc>,
    pub(crate) entry_guard_suppressed_up: bool,
    pub(crate) entry_guard_suppressed_dn: bool,
    pub(crate) post_only_clamped_up: bool,
    pub(crate) post_only_clamped_dn: bool,
    pub(crate) post_only_suppressed_up: bool,
    pub(crate) post_only_suppressed_dn: bool,
    pub(crate) cross_tick_clamped_up: bool,
    pub(crate) cross_tick_clamped_dn: bool,
    pub(crate) max_entry_prob: Option<f64>,
    pub(crate) max_entry_price: Option<f64>,
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
    pub(crate) suppression_ctx: Option<SuppressionContext>,
    pub(crate) spot_usd: Option<f64>,
    pub(crate) strike_usd: Option<f64>,
    pub(crate) resting_bid_up: Option<MicroUsdc>,
    pub(crate) resting_bid_dn: Option<MicroUsdc>,
    pub(crate) avg_cost_up: Option<MicroUsdc>,
    pub(crate) avg_cost_dn: Option<MicroUsdc>,
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
    last_quoted_up: Option<MicroUsdc>,
    last_quoted_dn: Option<MicroUsdc>,
    last_latency_ms: Option<u64>,
    last_minor_breach_ms: Option<u64>,
    last_minor_breach_kind: Option<(bool, bool)>,
    suppressed_minor_breaches: u32,
    verbose_latency: bool,
}

impl LifecycleStreamReporter {
    pub(crate) fn new(display: LifecycleDisplayMode, theme: Theme) -> Self {
        Self {
            display,
            theme,
            last_heartbeat_ms: None,
            last_suppression: None,
            last_cancel_all_reason: None,
            last_quoted_up: None,
            last_quoted_dn: None,
            last_latency_ms: None,
            last_minor_breach_ms: None,
            last_minor_breach_kind: None,
            suppressed_minor_breaches: 0,
            verbose_latency: false,
        }
    }

    /// Enables `--verbose-latency`: every tick prints its latency row, bypassing throttling.
    pub(crate) fn with_verbose_latency(mut self, enabled: bool) -> Self {
        self.verbose_latency = enabled;
        self
    }

    pub(crate) fn reset_epoch(&mut self) {
        self.last_heartbeat_ms = None;
        self.last_suppression = None;
        self.last_cancel_all_reason = None;
        self.last_quoted_up = None;
        self.last_quoted_dn = None;
        self.last_latency_ms = None;
        self.last_minor_breach_ms = None;
        self.last_minor_breach_kind = None;
        self.suppressed_minor_breaches = 0;
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
        let up_str = format_action_styled(t, Outcome::Up, row.action_up, None, row.resting_bid_up);
        let dn_str =
            format_action_styled(t, Outcome::Down, row.action_dn, None, row.resting_bid_dn);
        let total_ns = row.pricing_ns.saturating_add(row.risk_ns);
        let bar = t.bar();
        let suppr_note = match row.quote {
            QuoteOutput::Suppressed(SuppressionReason::BothLegsSuppressed) => {
                let text = if let Some(ctx) = row.suppression_ctx {
                    let up = format_leg_suppression(Outcome::Up, ctx, row.best_ask_up);
                    let dn = format_leg_suppression(Outcome::Down, ctx, row.best_ask_dn);
                    format!("Suppressing: {up} {bar} {dn}")
                } else {
                    format!(
                        "Suppressing: {}",
                        suppression_detail(SuppressionReason::BothLegsSuppressed)
                    )
                };
                format!(" {bar} {}", t.yellow(&text))
            }
            QuoteOutput::Suppressed(reason) => format!(
                " {bar} {}",
                t.yellow(&format!("Suppressing: {}", suppression_detail(reason)))
            ),
            QuoteOutput::Active { .. } => String::new(),
        };

        let base = format!(
            "{} T-{:03}s {} {} Util {:>5.1}% Inv {}/{} (Δ{}) {bar} {} {bar} {} {bar} Risk {} {bar} Total {}{suppr_note}",
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
        );

        if row.phase == EpochPhase::MandatoryFlatten || row.cancel_all_reason.is_some() {
            let reason = row.cancel_all_reason.map_or_else(
                || phase_flat_reason(row.phase).unwrap_or_else(|| "none".to_string()),
                humanize_reason,
            );
            let in_flight = if row.flatten_in_flight {
                format!(" {}", t.bold_yellow("[In-Flight]"))
            } else {
                String::new()
            };
            format!("{base} {bar}{in_flight} {}", t.gray(&reason))
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
                    let prefix = status_prefix(
                        t,
                        &utc,
                        row.tau_secs,
                        row.phase,
                        Badge::Suppr,
                        "Quoting paused",
                    );
                    let line = if reason == SuppressionReason::BothLegsSuppressed
                        && let Some(ctx) = row.suppression_ctx
                    {
                        let up_cell = format_leg_suppression(Outcome::Up, ctx, row.best_ask_up);
                        let dn_cell = format_leg_suppression(Outcome::Down, ctx, row.best_ask_dn);
                        format!(
                            "{prefix} {} {} {}",
                            theme::pad_visible_right(&t.yellow(&up_cell), LEG_WIDTH),
                            t.bar(),
                            t.yellow(&dn_cell),
                        )
                    } else {
                        let detail = match self.last_suppression {
                            Some(prev) => format!(
                                "{} {} {}",
                                suppression_label(prev),
                                t.arrow(),
                                suppression_label(reason)
                            ),
                            None => suppression_detail(reason).to_string(),
                        };
                        format!("{prefix} {}", t.yellow(&detail))
                    };
                    lines.push(line);
                    self.last_suppression = Some(reason);
                }
            }
            QuoteOutput::Active { bid_up, bid_dn, .. } => {
                if self.last_suppression.take().is_some() {
                    let resume_detail = match (bid_up, bid_dn) {
                        (Some(_), Some(_)) => "UP & DN active",
                        (Some(_), None) => "UP active (DN suppressed)",
                        (None, Some(_)) => "DN active (UP suppressed)",
                        (None, None) => "Quoting active",
                    };
                    lines.push(format!(
                        "{} {}",
                        status_prefix(
                            t,
                            &utc,
                            row.tau_secs,
                            row.phase,
                            Badge::Resume,
                            "Quoting resumed"
                        ),
                        t.gray(resume_detail),
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
                "{} {}",
                status_prefix(
                    t,
                    &utc,
                    row.tau_secs,
                    row.phase,
                    Badge::Resume,
                    "Quoting resumed"
                ),
                t.gray(&format!("Cancel-all cleared (was {prev})")),
            ));
        }

        if has_up_action || has_dn_action || cancel_all_changed {
            let prev_price_for =
                |cached: Option<MicroUsdc>, resting: Option<MicroUsdc>, action: &OrderAction| {
                    match action {
                        OrderAction::CancelAndReplace { new_price, .. } => {
                            cached.or(resting).filter(|&p| p != *new_price)
                        }
                        _ => cached,
                    }
                };
            let prev_up = prev_price_for(self.last_quoted_up, row.resting_bid_up, row.action_up);
            let prev_dn = prev_price_for(self.last_quoted_dn, row.resting_bid_dn, row.action_dn);
            let up_leg =
                format_action_styled(t, Outcome::Up, row.action_up, prev_up, row.resting_bid_up);
            let dn_leg =
                format_action_styled(t, Outcome::Down, row.action_dn, prev_dn, row.resting_bid_dn);
            // Widen both leg cells together so `DN` never drifts on a long requote price.
            let leg_width = LEG_WIDTH
                .max(theme::visible_width(&up_leg))
                .max(theme::visible_width(&dn_leg));
            let bar = t.bar();
            let mut action_line = format!(
                "{}{} {bar} {}",
                row_prefix(
                    t,
                    &utc,
                    row.tau_secs,
                    Some(Badge::Order),
                    Some(MidColumn::Phase(row.phase)),
                    Some(row.tier)
                ),
                theme::pad_visible_right(&up_leg, leg_width),
                theme::pad_visible_right(&dn_leg, leg_width),
            );
            let replace_reason = |action: &OrderAction| match action {
                OrderAction::CancelAndReplace { reason, .. }
                | OrderAction::Cancel { reason, .. } => Some(*reason),
                _ => None,
            };
            let explicit = match (replace_reason(row.action_up), replace_reason(row.action_dn)) {
                (Some(r_up), Some(r_dn)) if r_up == r_dn => Some(format!("{r_up:?}")),
                (Some(r_up), Some(r_dn)) => Some(format!("UP {r_up:?} {} DN {r_dn:?}", t.dot())),
                (Some(r), None) | (None, Some(r)) => Some(format!("{r:?}")),
                (None, None) => None,
            };
            let mut notes = Vec::new();
            let in_flight = row.cancel_all_reason.is_some() && row.flatten_in_flight;
            if in_flight {
                notes.push(t.bold_yellow("[In-Flight]"));
            }
            if let Some(detail) = explicit.or_else(|| {
                row.cancel_all_reason
                    .map(humanize_reason)
                    .or_else(|| phase_flat_reason(row.phase))
            }) {
                let reserved = theme::visible_width(&action_line)
                    + 3
                    + if in_flight {
                        theme::visible_width(&t.bold_yellow("[In-Flight]")) + 3
                    } else {
                        0
                    };
                let room = ROW_WIDTH_BUDGET.saturating_sub(reserved).max(8);
                notes.push(t.gray(&truncate_to_width(t, &detail, room)));
            }
            if !notes.is_empty() {
                action_line.push_str(&format!(" {bar} {}", notes.join(&format!(" {bar} "))));
            }
            lines.push(action_line.trim_end().to_string());
        }

        let next_quoted = |action: &OrderAction, resting: Option<MicroUsdc>| match action {
            OrderAction::SubmitNew { price, .. } => Some(*price),
            OrderAction::CancelAndReplace { new_price, .. } => Some(*new_price),
            OrderAction::Cancel { .. } => None,
            OrderAction::Hold => resting,
        };
        self.last_quoted_up = next_quoted(row.action_up, row.resting_bid_up);
        self.last_quoted_dn = next_quoted(row.action_dn, row.resting_bid_dn);

        // 3. Latency: severe breaches print immediately; minor breaches (<= 2x budget) cooldown for 5s; nominal ticks every 10s
        let total_ns = row.pricing_ns.saturating_add(row.risk_ns);
        let pricing_breach = row.pricing_ns > PRICING_BUDGET_US.saturating_mul(1_000);
        let risk_breach = row.risk_ns > RISK_BUDGET_US.saturating_mul(1_000);
        let total_breach = total_ns > TOTAL_BUDGET_US.saturating_mul(1_000);
        let breach = pricing_breach || risk_breach || total_breach;
        let severe_breach = row.pricing_ns > PRICING_BUDGET_US.saturating_mul(2_000)
            || row.risk_ns > RISK_BUDGET_US.saturating_mul(2_000)
            || total_breach;
        let minor_breach = breach && !severe_breach;
        let breach_kind = (pricing_breach, risk_breach);
        let minor_due = minor_breach
            && (self.last_minor_breach_kind != Some(breach_kind)
                || self.last_minor_breach_ms.is_none_or(|last| {
                    row.now_ms.saturating_sub(last) >= LATENCY_MINOR_BREACH_COOLDOWN_MS
                }));
        // First tick only seeds the throttle window; nominal rows report every 10s after that.
        let previous_latency = self.last_latency_ms.get_or_insert(row.now_ms);
        let periodic = row.now_ms.saturating_sub(*previous_latency) >= LATENCY_PERIOD_MS;
        if self.verbose_latency || severe_breach || minor_due || periodic {
            self.last_latency_ms = Some(row.now_ms);
            if minor_breach {
                self.last_minor_breach_ms = Some(row.now_ms);
                self.last_minor_breach_kind = Some(breach_kind);
            }
            let pricing =
                t.fmt_latency_us_width(row.pricing_ns, PRICING_BUDGET_US, LATENCY_VALUE_WIDTH);
            let risk = t.fmt_latency_us_width(row.risk_ns, RISK_BUDGET_US, LATENCY_VALUE_WIDTH);
            let total = t.fmt_latency_us_width(total_ns, TOTAL_BUDGET_US, LATENCY_VALUE_WIDTH);
            let bar = t.bar();
            let mut line = format!(
                "{}Pricing {pricing} {bar} Risk {risk} {bar} Total {total}",
                row_prefix(t, &utc, row.tau_secs, Some(Badge::Latency), None, None),
            );
            let suppressed = std::mem::take(&mut self.suppressed_minor_breaches);
            if breach {
                let mut breaches = Vec::new();
                if pricing_breach {
                    breaches.push(format!("Pricing >{PRICING_BUDGET_US}us"));
                }
                if risk_breach {
                    breaches.push(format!("Risk >{RISK_BUDGET_US}us"));
                }
                if total_breach {
                    breaches.push(format!("Total >{TOTAL_BUDGET_US}us"));
                }
                line.push_str(&format!(" {bar} {}", t.bold_yellow(&breaches.join(", "))));
                if suppressed > 0 {
                    line.push_str(&t.gray(&format!(" (+{suppressed} minor)")));
                }
            } else if suppressed > 0 {
                line.push_str(&format!(
                    " {bar} {}",
                    t.gray(&format!("+{suppressed} minor"))
                ));
            }
            lines.push(line);
        } else if minor_breach {
            self.suppressed_minor_breaches = self.suppressed_minor_breaches.saturating_add(1);
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

    /// Renders the heartbeat at the richest detail level that still fits [`ROW_WIDTH_BUDGET`].
    /// Renders the one fixed heartbeat shape so every beat scans the same way.
    fn format_heartbeat_line(&self, row: &LifecycleStepRow<'_>, utc: &str) -> String {
        let t = self.theme;
        let u_pct = (row.u_e6 as f64) / 10_000.0;
        let btc = t.fmt_spot_vs_strike(row.spot_usd, row.strike_usd);
        let book = compact_book(t, row);
        let quote = compact_quote(row);
        let net_delta_micro = i128::from(row.q_up.as_u64()) - i128::from(row.q_dn.as_u64());
        let up_inv = if net_delta_micro > 0
            && let Some(cost) = row.avg_cost_up
        {
            format!(
                "{}@{}",
                fmt_shares_2dp(row.q_up.as_u64()),
                Theme::fmt_cents(Some(cost))
            )
        } else {
            fmt_shares_2dp(row.q_up.as_u64())
        };
        let dn_inv = if net_delta_micro < 0
            && let Some(cost) = row.avg_cost_dn
        {
            format!(
                "{}@{}",
                fmt_shares_2dp(row.q_dn.as_u64()),
                Theme::fmt_cents(Some(cost))
            )
        } else {
            fmt_shares_2dp(row.q_dn.as_u64())
        };
        let inv = format!(
            "Inv {up_inv}/{dn_inv} (Δ{})",
            fmt_signed_shares_2dp(net_delta_micro)
        );
        let bar = t.bar();

        format!(
            "{prefix}Util {u_pct:.1}% {bar} BTC {btc} {bar} {book} {bar} {quote} {bar} {inv} {bar} PnL {pnl}",
            prefix = row_prefix(
                t,
                utc,
                row.tau_secs,
                Some(Badge::Heartbeat),
                Some(MidColumn::Phase(row.phase)),
                Some(row.tier)
            ),
            pnl = t.fmt_pnl(row.net_pnl_micro),
        )
    }
}

/// Compact book cell: whole-cent prices without the currency glyph (`Bk 47/50·47/50`).
fn compact_book(t: Theme, row: &LifecycleStepRow<'_>) -> String {
    format!(
        "Bk {}/{}{}{}/{}",
        fmt_cents_plain(row.best_bid_up),
        fmt_cents_plain(row.best_ask_up),
        t.dot(),
        fmt_cents_plain(row.best_bid_dn),
        fmt_cents_plain(row.best_ask_dn),
    )
}

/// Compact quote cell: our resting pair in whole cents, plus pair cost when both legs rest (`Q 50/47 (97¢)`).
fn compact_quote(row: &LifecycleStepRow<'_>) -> String {
    match (row.resting_bid_up, row.resting_bid_dn) {
        (Some(up), Some(dn)) => format!(
            "Q {}/{} ({})",
            fmt_cents_plain(Some(up)),
            fmt_cents_plain(Some(dn)),
            Theme::fmt_cents(Some(MicroUsdc(up.0.saturating_add(dn.0))))
        ),
        _ => format!(
            "Q {}/{}",
            fmt_cents_plain(row.resting_bid_up),
            fmt_cents_plain(row.resting_bid_dn),
        ),
    }
}

/// Builds the shared prefix for status notices (`[SUPPR]` / `[RESUME]`): badge columns plus a
/// fixed-width status label ending in the column bar.
fn status_prefix(
    t: Theme,
    utc: &str,
    tau_secs: u64,
    phase: EpochPhase,
    badge: Badge,
    status: &str,
) -> String {
    format!(
        "{}{} {}",
        row_prefix(
            t,
            utc,
            tau_secs,
            Some(badge),
            Some(MidColumn::Phase(phase)),
            None
        ),
        theme::pad_visible_right(status, STATUS_WIDTH),
        t.bar(),
    )
}

/// Human-readable suppression label used inside status notices.
fn suppression_label(reason: SuppressionReason) -> &'static str {
    match reason {
        SuppressionReason::WarmingUp => "Warming up",
        SuppressionReason::EmptyOrderBook => "Empty order book",
        SuppressionReason::CrossedOrderBook => "Crossed order book",
        SuppressionReason::PairMarginViolation => "Pair margin violation",
        SuppressionReason::BothLegsSuppressed => "Both legs suppressed",
    }
}

/// Full suppression detail for the first paused notice.
fn suppression_detail(reason: SuppressionReason) -> &'static str {
    match reason {
        SuppressionReason::BothLegsSuppressed => "Both legs suppressed",
        other => suppression_label(other),
    }
}

fn format_prob_cents(prob: f64) -> String {
    let micro = ((prob * 1_000_000.0) + 1e-9).floor() as u64;
    Theme::fmt_cents(Some(MicroUsdc(micro)))
}

/// Formats the exact Station 6 gate and triggering price for one suppressed leg (`<= LEG_WIDTH` chars).
fn format_leg_suppression(
    outcome: Outcome,
    ctx: SuppressionContext,
    best_ask: Option<MicroUsdc>,
) -> String {
    let (leg, bid_floored, entry_guarded, post_only_clamped, post_only_suppressed, cross_tick) =
        match outcome {
            Outcome::Up => (
                "UP",
                ctx.bid_up_floored,
                ctx.entry_guard_suppressed_up,
                ctx.post_only_clamped_up,
                ctx.post_only_suppressed_up,
                ctx.cross_tick_clamped_up,
            ),
            Outcome::Down => (
                "DN",
                ctx.bid_dn_floored,
                ctx.entry_guard_suppressed_dn,
                ctx.post_only_clamped_dn,
                ctx.post_only_suppressed_dn,
                ctx.cross_tick_clamped_dn,
            ),
        };

    if entry_guarded {
        let px = if post_only_clamped
            && let Some(p) = bid_floored
            && !math_engine::is_entry_guarded(p, ctx.max_entry_prob, ctx.max_entry_price)
            && let Some(ask) = best_ask
        {
            Some(p.min(MicroUsdc(ask.0.saturating_sub(10_000))))
        } else {
            bid_floored
        };
        if let Some(p) = px {
            if let Some(cap) = ctx.max_entry_price
                && math_engine::exceeds_entry_price_cap(p, cap)
            {
                return format!(
                    "{leg}: {} > {} cap",
                    Theme::fmt_cents(Some(p)),
                    format_prob_cents(cap)
                );
            }
            if let Some(p_max) = ctx.max_entry_prob {
                let max_micro = ((p_max * 1_000_000.0) + 1e-9).floor() as u64;
                let min_micro = 1_000_000u64.saturating_sub(max_micro);
                if p.0 < min_micro {
                    return format!(
                        "{leg}: {} < {} floor",
                        Theme::fmt_cents(Some(p)),
                        Theme::fmt_cents(Some(MicroUsdc(min_micro)))
                    );
                }
                if p.0 > max_micro {
                    return format!(
                        "{leg}: {} > {} ceil",
                        Theme::fmt_cents(Some(p)),
                        Theme::fmt_cents(Some(MicroUsdc(max_micro)))
                    );
                }
            }
        }
        return format!("{leg}: entry guard");
    }

    if post_only_suppressed {
        return match bid_floored {
            Some(p) => format!("{leg}: {} post-only", Theme::fmt_cents(Some(p))),
            None => format!("{leg}: post-only"),
        };
    }

    if cross_tick {
        return match bid_floored {
            Some(p) => format!("{leg}: {} cross-tick", Theme::fmt_cents(Some(p))),
            None => format!("{leg}: cross-tick cap"),
        };
    }

    format!("{leg}: <1¢ min tick")
}

/// Turns an operator reason string (`stale_feed`) into readable words (`stale feed`).
fn humanize_reason(raw: &str) -> String {
    raw.replace('_', " ")
}

/// Explains the current lockout/flatten phase when no explicit reason string is present.
fn phase_flat_reason(phase: EpochPhase) -> Option<String> {
    match phase {
        EpochPhase::ActiveQuoting => None,
        EpochPhase::PreExpiryLockout => {
            Some("Flat position in PreExpiryLockout (τ ≤ 60s)".to_string())
        }
        EpochPhase::MandatoryFlatten => Some("Flattening inventory before expiry".to_string()),
        EpochPhase::TerminalLockout => Some("Terminal lockout — settlement imminent".to_string()),
    }
}

fn format_action_styled(
    t: Theme,
    outcome: Outcome,
    action: &OrderAction,
    prev_price: Option<MicroUsdc>,
    resting_price: Option<MicroUsdc>,
) -> String {
    let leg = match outcome {
        Outcome::Up => t.bold_green("UP"),
        Outcome::Down => t.bold_red("DN"),
    };
    let arrow = t.arrow();
    match action {
        OrderAction::Hold => match resting_price {
            Some(px) => format!(
                "{leg}: {}",
                t.muted(&format!("hold @ {}", Theme::fmt_cents(Some(px))))
            ),
            None => format!("{leg}: {}", t.muted("idle")),
        },
        OrderAction::SubmitNew { price, size, .. } => format!(
            "{leg}: {} @ {}",
            fmt_shares_3dp(*size),
            Theme::fmt_cents(Some(*price))
        ),
        OrderAction::Cancel { .. } => match resting_price.or(prev_price) {
            Some(px) => format!(
                "{leg}: {}",
                t.yellow(&format!("Cancel ({})", Theme::fmt_cents(Some(px))))
            ),
            None => format!("{leg}: {}", t.yellow("Cancel")),
        },
        OrderAction::CancelAndReplace {
            new_price,
            new_size,
            ..
        } => match prev_price {
            Some(old) => format!(
                "{leg}: {} @ {} {arrow} {}",
                fmt_shares_3dp(*new_size),
                t.strikethrough(&Theme::fmt_cents(Some(old))),
                Theme::fmt_cents(Some(*new_price)),
            ),
            None => format!(
                "{leg}: {} @ {}",
                fmt_shares_3dp(*new_size),
                Theme::fmt_cents(Some(*new_price)),
            ),
        },
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

/// Formats micro-shares rounded to 2 decimal places, trimming trailing zeros (`0`, `1.5`, `20`).
fn fmt_shares_2dp(raw_micro: u64) -> String {
    let hundredths = raw_micro.saturating_add(5_000) / 10_000;
    let whole = hundredths / 100;
    let rem = hundredths % 100;
    if rem == 0 {
        whole.to_string()
    } else if rem.is_multiple_of(10) {
        format!("{whole}.{}", rem / 10)
    } else {
        format!("{whole}.{rem:02}")
    }
}

/// Clamps plain text to `max_chars`, appending the theme's ellipsis when it is shortened.
fn truncate_to_width(t: Theme, text: &str, max_chars: usize) -> String {
    let width = text.chars().count();
    if width <= max_chars {
        return text.to_string();
    }
    let marker = t.ellipsis();
    let keep = max_chars.saturating_sub(marker.chars().count());
    let mut out: String = text.chars().take(keep).collect();
    out.push_str(marker);
    out
}

/// Rounds micro-USDC to whole cents without a currency glyph (`47`, `--`).
fn fmt_cents_plain(px: Option<MicroUsdc>) -> String {
    match px {
        None => "--".to_string(),
        Some(MicroUsdc(micro)) => format!("{}", micro.saturating_add(5_000) / 10_000),
    }
}

/// Formats signed micro-share inventory delta up to 2 decimal places (`0`, `+20`, `-1.5`).
fn fmt_signed_shares_2dp(delta_micro: i128) -> String {
    let mag_micro = u64::try_from(delta_micro.unsigned_abs()).unwrap_or(u64::MAX);
    let mag = fmt_shares_2dp(mag_micro);
    if mag == "0" {
        mag
    } else if delta_micro < 0 {
        format!("-{mag}")
    } else {
        format!("+{mag}")
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
    let meaning = match to {
        EpochPhase::ActiveQuoting => "quoting active; normal entry",
        EpochPhase::PreExpiryLockout => "no new entries; reduce risk",
        EpochPhase::MandatoryFlatten => "unwind inventory before expiry",
        EpochPhase::TerminalLockout => "settlement imminent; all quoting ceased",
    };
    let rule = t.phase_rule();
    let head = format!(
        "  {} {} :: {} {} {} (T-{tau_secs:03}s) {} ",
        t.phase_icon(to),
        t.bold("PHASE"),
        t.muted(&from_label),
        t.arrow(),
        t.phase_tag_styled(to),
        t.bar(),
    );
    let used = theme::visible_width(&head);
    let budget = ROW_WIDTH_BUDGET.saturating_sub(used + 3);
    let reason_text = truncate_to_width(t, reason, budget.saturating_sub(20).max(12));
    let meaning_max = budget.saturating_sub(theme::visible_width(&reason_text));
    let meaning_text = truncate_to_width(t, meaning, meaning_max);
    println!();
    println!("{rule}");
    println!("{head}{reason_text} — {}", t.muted(&meaning_text));
    println!("{rule}");
}

pub(crate) fn print_tier_transition(
    now_ms: u64,
    tau_secs: u64,
    phase: EpochPhase,
    to: RiskTier,
    u_e6: u64,
    heavy: Option<Outcome>,
) {
    let t = theme::current();
    let utc = format_utc_hms(now_ms);
    let heavy_label = match heavy {
        Some(Outcome::Up) => "UP",
        Some(Outcome::Down) => "DN",
        None => "none",
    };
    let util_cell =
        theme::pad_visible_right(&format!("Util {:.1}%", (u_e6 as f64) / 10_000.0), LEG_WIDTH);
    println!(
        "{}{} {} Heavy: {heavy_label}",
        row_prefix(
            t,
            &utc,
            tau_secs,
            Some(Badge::Tier),
            Some(MidColumn::Phase(phase)),
            Some(to)
        ),
        util_cell,
        t.bar(),
    );
}

pub(crate) fn print_breaker_trip(now_ms: u64, tau_secs: u64, status: BreakerStatus) {
    println!(
        "{}",
        format_breaker_line(theme::current(), now_ms, tau_secs, status)
    );
}

fn format_breaker_line(t: Theme, now_ms: u64, tau_secs: u64, status: BreakerStatus) -> String {
    let utc = format_utc_hms(now_ms);
    let (state, reason) = match status {
        BreakerStatus::ArmActive => ("Armed", "all risk gates healthy".to_string()),
        BreakerStatus::Tripped { reason } => {
            ("Quoting halted", breaker_reason_text(reason).to_string())
        }
    };
    format!(
        "{}{} {} {}",
        row_prefix(t, &utc, tau_secs, Some(Badge::Breaker), None, None),
        theme::pad_visible_right(&t.bold(state), STATUS_WIDTH),
        t.bar(),
        t.gray(&reason),
    )
}

pub(crate) fn print_lifecycle_fill(
    now_ms: u64,
    tau_secs: u64,
    tier: RiskTier,
    fill: &SimFill,
    q_up: SharesMicro,
    q_dn: SharesMicro,
    realized_pnl_micro: i64,
) {
    println!(
        "{}",
        format_fill_line(
            theme::current(),
            now_ms,
            tau_secs,
            tier,
            FillImpact {
                fill,
                q_up,
                q_dn,
                realized_pnl_micro,
            },
        )
    );
}

/// Post-fill context rendered after the fill itself.
struct FillImpact<'a> {
    fill: &'a SimFill,
    q_up: SharesMicro,
    q_dn: SharesMicro,
    realized_pnl_micro: i64,
}

fn format_fill_line(
    t: Theme,
    now_ms: u64,
    tau_secs: u64,
    tier: RiskTier,
    impact: FillImpact<'_>,
) -> String {
    let FillImpact {
        fill,
        q_up,
        q_dn,
        realized_pnl_micro,
    } = impact;
    let utc = format_utc_hms(now_ms);
    let leg = match fill.outcome {
        Outcome::Up => t.bold_green("UP"),
        Outcome::Down => t.bold_cyan("DN"),
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
    let notional_micro =
        (u128::from(fill.price.0) * u128::from(fill.filled_shares.0)) / u128::from(1_000_000u64);
    let notional = format!("${}", fmt_usd_grouped(notional_micro as f64 / 1_000_000.0));
    let net_delta = i128::from(q_up.0) - i128::from(q_dn.0);
    let bar = t.bar();
    format!(
        "{}#{} {} {} {} @ {} {bar} {} {} {bar} Inv {}/{} (Δ{}) {bar} {} {}",
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
        Theme::fmt_cents(Some(fill.price)),
        t.muted("Notional"),
        t.bold(&notional),
        fmt_shares_2dp(q_up.0),
        fmt_shares_2dp(q_dn.0),
        fmt_signed_shares_2dp(net_delta),
        t.muted("PnL"),
        t.fmt_pnl(realized_pnl_micro),
    )
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
    let bar = t.bar();
    println!(
        "{}Pairs {} {bar} Cost {} (UP {} {} DN {}) {bar} PnL {} {bar} Total {}",
        row_prefix(
            t,
            &utc,
            tau_secs,
            Some(Badge::Merge),
            phase.map(MidColumn::Phase),
            Some(tier),
        ),
        fmt_shares_2dp(merge.merged_pairs.0),
        Theme::fmt_cents(Some(merge.pair_cost)),
        Theme::fmt_cents(Some(merge.avg_cost_up)),
        t.dot(),
        Theme::fmt_cents(Some(merge.avg_cost_dn)),
        t.fmt_pnl(merge.realized_gain_micro),
        fmt_shares_2dp(total_merged.0),
    );
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn print_lifecycle_flatten(
    now_ms: u64,
    tau_secs: u64,
    tier: RiskTier,
    outcome: Outcome,
    side: OrderSide,
    price: MicroUsdc,
    size: SharesMicro,
    fee: MicroUsdc,
    reason: &str,
) {
    let t = theme::current();
    let utc = format_utc_hms(now_ms);
    let leg = match outcome {
        Outcome::Up => t.bold_green("UP"),
        Outcome::Down => t.bold_red("DN"),
    };
    let leg_action = format!(
        "{leg} {} {} @ {}",
        side.as_str(),
        fmt_shares_3dp(size),
        Theme::fmt_cents(Some(price))
    );
    let bar = t.bar();
    println!(
        "{}{} {bar} Fee ${:.2} {bar} {}",
        row_prefix(t, &utc, tau_secs, Some(Badge::Flatten), None, Some(tier)),
        theme::pad_visible_right(&leg_action, LEG_WIDTH),
        fee.0 as f64 / MICRO_SCALE as f64,
        t.gray(reason),
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
        "{}",
        format_epoch_rollover_block(
            theme::current(),
            expired_slug,
            next_slug,
            winning_outcome,
            settlement_pnl_delta_micro,
            total_realized_pnl_micro,
        )
    );
}

fn format_epoch_rollover_block(
    t: Theme,
    expired_slug: &str,
    next_slug: Option<&str>,
    winning_outcome: Outcome,
    settlement_pnl_delta_micro: i64,
    total_realized_pnl_micro: i64,
) -> String {
    let bar = t.bar();
    let label = epoch_label(t, t.epoch_icon(true), "EPOCH ROLLOVER");
    let next = match next_slug {
        Some(next) => short_epoch_transition(t, expired_slug, next),
        None => format!(
            "{} {} {}",
            t.cyan(expired_slug),
            t.arrow(),
            t.muted("awaiting-next")
        ),
    };
    let rule = t.epoch_rule();
    format!(
        "{rule}\n{label}{bar} {next} {bar} {} {} {bar} {} {} {bar} {} {}\n{rule}",
        t.muted("Winner:"),
        t.bold(outcome_label(winning_outcome)),
        t.muted("Settle:"),
        t.fmt_pnl(settlement_pnl_delta_micro),
        t.muted("CumPnL:"),
        t.fmt_pnl(total_realized_pnl_micro),
    )
}

pub(crate) fn print_lifecycle_summary(
    stats: &LifecycleReportStats,
    pricing_lat: &LatencySummary,
    risk_lat: &LatencySummary,
    total_lat: &LatencySummary,
) {
    println!();
    println!(
        "{}",
        format_lifecycle_summary(theme::current(), stats, pricing_lat, risk_lat, total_lat)
    );
}

/// Label column and right-aligned value column for the summary blocks.
const SUMMARY_LABEL_WIDTH: usize = 16;
const SUMMARY_VALUE_WIDTH: usize = 12;

/// Groups a digit string in threes (`6295` -> `6,295`).
fn group_thousands(raw: &str) -> String {
    let mut out = String::new();
    for (idx, ch) in raw.chars().enumerate() {
        if idx > 0 && (raw.len() - idx).is_multiple_of(3) {
            out.push(',');
        }
        out.push(ch);
    }
    out
}

fn fmt_count(n: u64) -> String {
    group_thousands(&n.to_string())
}

/// One `label value` cell with a fixed-width right-aligned number.
fn summary_cell(t: Theme, label: &str, value: &str) -> String {
    format!(
        "{} {}",
        t.muted(&format!("{label:<width$}", width = SUMMARY_LABEL_WIDTH)),
        theme::pad_visible_left(value, SUMMARY_VALUE_WIDTH)
    )
}

fn push_summary_section(out: &mut String, t: Theme, title: &str) {
    out.push_str(&format!("\n  {}", t.bold_cyan(title)));
}

fn push_summary_row(out: &mut String, t: Theme, cells: &[(&str, &str)]) {
    out.push('\n');
    out.push_str(&summary_row(t, cells));
}

fn summary_row(t: Theme, cells: &[(&str, &str)]) -> String {
    let bar = t.bar();
    let body = cells
        .iter()
        .map(|(label, value)| summary_cell(t, label, value))
        .collect::<Vec<_>>()
        .join(&format!(" {bar} "));
    format!("    {body}")
}

fn signed_bps(bps: f64) -> String {
    format!("{bps:+.1}bp")
}

fn format_lifecycle_summary(
    t: Theme,
    stats: &LifecycleReportStats,
    pricing_lat: &LatencySummary,
    risk_lat: &LatencySummary,
    total_lat: &LatencySummary,
) -> String {
    let net_pnl_micro = stats
        .realized_pnl_micro
        .saturating_add(stats.unrealized_pnl_micro);
    let rule = t.epoch_rule();
    let mut out = String::new();

    out.push_str(&rule);
    out.push('\n');
    out.push_str(&format!(
        "  {} {} {}",
        t.bold_cyan("■ LIFECYCLE SUMMARY"),
        t.muted("│"),
        t.gray("execution, risk and telemetry totals for this run")
    ));
    out.push('\n');
    out.push_str(&rule);

    // Execution & fills
    let ticks = fmt_count(stats.ticks_evaluated as u64);
    let phases = fmt_count(stats.phase_transitions as u64);
    let submitted = fmt_count(stats.orders_submitted as u64);
    let replaced = fmt_count(stats.orders_replaced as u64);
    let canceled = fmt_count(stats.orders_canceled as u64);
    let held = fmt_count(stats.orders_held as u64);
    let fills = fmt_count(stats.fills_count);
    let merges = fmt_count(stats.merges_count);
    let pairs = fmt_shares_2dp(stats.total_pairs_merged.0);
    let epochs = fmt_count(stats.epochs_completed as u64);
    let rollovers = fmt_count(stats.epoch_rollovers as u64);
    let rpm = fmt_count(stats.rpm_60s as u64);
    push_summary_section(&mut out, t, "EXECUTION & FILLS");
    push_summary_row(
        &mut out,
        t,
        &[
            ("Ticks", &ticks),
            ("Phase shifts", &phases),
            ("Orders", &submitted),
        ],
    );
    push_summary_row(
        &mut out,
        t,
        &[
            ("Replaced", &replaced),
            ("Canceled", &canceled),
            ("Held (FIFO)", &held),
        ],
    );
    push_summary_row(
        &mut out,
        t,
        &[
            ("Fills", &fills),
            ("Merges", &merges),
            ("Pairs merged", &pairs),
        ],
    );
    push_summary_row(
        &mut out,
        t,
        &[
            ("Epochs", &epochs),
            ("Rollovers", &rollovers),
            ("RPM (60s)", &rpm),
        ],
    );

    // Inventory & PnL
    let q_up = fmt_shares_2dp(stats.final_q_up.0);
    let q_dn = fmt_shares_2dp(stats.final_q_dn.0);
    let net_delta = fmt_signed_shares_2dp(i128::from(stats.resolved_net_delta_micro()));
    let realized = t.fmt_pnl(stats.realized_pnl_micro);
    let unrealized = t.fmt_pnl(stats.unrealized_pnl_micro);
    let net_pnl = t.fmt_pnl(net_pnl_micro);
    let settlement = t.fmt_pnl(stats.settlement_pnl_micro);
    let fees = format!("${:.2}", stats.fees_paid_micro as f64 / MICRO_SCALE as f64);
    let rebate = format!(
        "${:.2}",
        stats.estimated_rebate_micro as f64 / MICRO_SCALE as f64
    );
    let fill_ratio = format!("{:.1}%", stats.fill_ratio * 100.0);
    let markout_1s = signed_bps(stats.markout_1s_bps);
    let markout_5s = signed_bps(stats.markout_5s_bps);
    push_summary_section(&mut out, t, "INVENTORY & PNL");
    push_summary_row(
        &mut out,
        t,
        &[
            ("Inventory UP", &q_up),
            ("Inventory DN", &q_dn),
            ("Net delta", &net_delta),
        ],
    );
    push_summary_row(
        &mut out,
        t,
        &[
            ("Realized", &realized),
            ("Unrealized", &unrealized),
            ("Net PnL", &net_pnl),
        ],
    );
    push_summary_row(
        &mut out,
        t,
        &[
            ("Settlement", &settlement),
            ("Fees paid", &fees),
            ("Est rebate", &rebate),
        ],
    );
    push_summary_row(
        &mut out,
        t,
        &[
            ("Fill ratio", &fill_ratio),
            ("Markout 1s", &markout_1s),
            ("Markout 5s", &markout_5s),
        ],
    );

    // Risk & breakers
    let breakers_raw = fmt_count(stats.breaker_trips as u64);
    let breakers = if stats.breaker_trips > 0 {
        t.bold_red(&breakers_raw)
    } else {
        t.muted(&breakers_raw)
    };
    let flattens_raw = fmt_count(stats.flattens_executed as u64);
    let flattens = if stats.flattens_executed > 0 {
        t.yellow(&flattens_raw)
    } else {
        t.muted(&flattens_raw)
    };
    let baseline = t.fmt_pnl(stats.baseline_pnl_micro);
    push_summary_section(&mut out, t, "RISK & BREAKERS");
    push_summary_row(
        &mut out,
        t,
        &[
            ("Breakers", &breakers),
            ("Flattens", &flattens),
            ("Baseline PnL", &baseline),
        ],
    );

    out.push_str("\n\n");
    out.push_str(&format!(
        "  {} {}\n",
        t.bold_cyan("COMPUTE LATENCY HDR"),
        t.gray("(microseconds; P99 vs budget)")
    ));
    out.push_str(&format_latency_table(
        t,
        &[
            ("Pricing", pricing_lat, PRICING_BUDGET_US),
            ("Risk", risk_lat, RISK_BUDGET_US),
            ("Total", total_lat, TOTAL_BUDGET_US),
        ],
    ));
    out
}

fn format_latency_table(t: Theme, rows: &[(&str, &LatencySummary, u64)]) -> String {
    const STAGE_W: usize = 18;
    const NUM_W: usize = 8;
    const TARGET_W: usize = 9;
    const COLS: [usize; 6] = [STAGE_W, NUM_W, NUM_W, NUM_W, NUM_W, TARGET_W];
    let (h, v, cross, corners) = if t.box_drawing() {
        ("─", "│", "┼", ("┌", "┐", "├", "┤", "└", "┘"))
    } else {
        ("-", "|", "+", ("+", "+", "+", "+", "+", "+"))
    };

    let border = |left: &str, mid: &str, right: &str| {
        let mut line = String::from("  ");
        line.push_str(left);
        for (idx, width) in COLS.iter().enumerate() {
            line.push_str(&h.repeat(width + 2));
            line.push_str(if idx + 1 == COLS.len() { right } else { mid });
        }
        line
    };
    let row = |cells: [String; 6]| {
        let mut line = String::from("  ");
        line.push_str(v);
        for (idx, (text, width)) in cells.iter().zip(COLS).enumerate() {
            line.push(' ');
            if idx == 0 {
                line.push_str(&theme::pad_visible_right(text, width));
            } else {
                line.push_str(&theme::pad_visible_left(text, width));
            }
            line.push(' ');
            line.push_str(v);
        }
        line
    };

    let (top_left, top_right, mid_left, mid_right, bot_left, bot_right) = corners;
    let mut lines = vec![
        border(top_left, cross, top_right),
        row([
            t.muted("Stage"),
            t.muted("P50"),
            t.muted("P95"),
            t.muted("P99"),
            t.muted("MAX"),
            t.muted("Target"),
        ]),
        border(mid_left, cross, mid_right),
    ];
    for (name, lat, budget) in rows {
        lines.push(row([
            (*name).to_string(),
            format!("{}", lat.p50_ns / 1_000),
            format!("{}", lat.p95_ns / 1_000),
            t.fmt_latency_us_width(lat.p99_ns, *budget, NUM_W),
            format!("{}", lat.max_ns / 1_000),
            format!("<{budget}µs"),
        ]));
    }
    lines.push(border(bot_left, cross, bot_right));
    lines.join("\n")
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
            suppression_ctx: None,
            spot_usd: Some(90_014.0),
            strike_usd: Some(90_000.0),
            resting_bid_up: Some(MicroUsdc(500_000)),
            resting_bid_dn: Some(MicroUsdc(470_000)),
            avg_cost_up: None,
            avg_cost_dn: None,
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
        assert_eq!(fmt_shares_2dp(0), "0");
        assert_eq!(fmt_shares_2dp(1_500_000), "1.5");
        assert_eq!(fmt_shares_2dp(20_000_000), "20");
        assert_eq!(fmt_shares_2dp(1_250_000), "1.25");
        assert_eq!(fmt_signed_shares_2dp(0), "0");
        assert_eq!(fmt_signed_shares_2dp(20_000_000), "+20");
        assert_eq!(fmt_signed_shares_2dp(-1_500_000), "-1.5");
        assert_eq!(Theme::fmt_cents(Some(MicroUsdc(500_000))), "50¢");
        assert_eq!(Theme::fmt_cents(Some(MicroUsdc(550_000))), "55¢");
        assert_eq!(Theme::fmt_cents(Some(MicroUsdc(555_000))), "55.5¢");
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
        assert!(l0[0].contains("[♥ BEAT]"));
        assert!(l0[0].contains("BTC +$14 (+2bp)"));
        assert!(l0[0].contains("Bk 51/52-48/49"));
        assert!(l0[0].contains("Q 50/47"));
        assert!(l0[0].contains("Inv 20/0 (Δ+20)"));
        assert!(l0[0].contains("PnL +$0.20"));
        assert!(
            l0[0].chars().count() <= 140,
            "heartbeat must not wrap: {} -> {}",
            l0[0].chars().count(),
            l0[0]
        );

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
        assert!(s1[0].contains("Quoting paused"));
        assert!(s1[0].contains("Warming up"));

        suppr_row.now_ms = base_ms + 300;
        assert!(rep.format_step(&suppr_row).is_empty());

        suppr_row.now_ms = base_ms + 400;
        suppr_row.quote = QuoteOutput::Suppressed(SuppressionReason::CrossedOrderBook);
        let s2 = rep.format_step(&suppr_row);
        assert_eq!(s2.len(), 1);
        assert!(s2[0].contains("Warming up"));
        assert!(s2[0].contains("Crossed order book"));

        let resume_row = sample_row(base_ms + 500, 299, &hold, &hold);
        let r1 = rep.format_step(&resume_row);
        assert_eq!(r1.len(), 1);
        assert!(r1[0].contains("[RESUME]"));
        assert!(r1[0].contains("Quoting resumed"));
        assert!(r1[0].contains("UP & DN active"));

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
        assert!(act_lines[0].contains("UP: 20 @ 50¢"));
        assert!(act_lines[0].contains("DN: Cancel"));
        assert!(act_lines[0].contains("RiskSuppressed"));

        let replace = OrderAction::CancelAndReplace {
            outcome: Outcome::Down,
            old_order_id: "ord-2".to_string(),
            new_price: MicroUsdc(370_000),
            new_size: SharesMicro::from_raw(20 * 1_000_000),
            reason: CancelReason::DeadbandDrift,
        };
        let rep_lines = rep.format_step(&sample_row(base_ms + 650, 299, &hold, &replace));
        assert_eq!(rep_lines.len(), 1);
        assert!(rep_lines[0].contains("UP: hold @ 50¢"));
        assert!(rep_lines[0].contains("DN: 20 @ ~47¢~"));
        assert!(rep_lines[0].contains("37¢"));
        assert!(rep_lines[0].contains("DeadbandDrift"));

        // Paired requote tracks prior SubmitNew prices in both plain and styled modes.
        for (mode_idx, theme) in [Theme::plain(), Theme::styled()].into_iter().enumerate() {
            let mut paired_rep = LifecycleStreamReporter::new(LifecycleDisplayMode::Stream, theme);
            let sub_up = OrderAction::SubmitNew {
                outcome: Outcome::Up,
                price: MicroUsdc(500_000),
                size: SharesMicro::from_raw(20 * 1_000_000),
            };
            let sub_dn = OrderAction::SubmitNew {
                outcome: Outcome::Down,
                price: MicroUsdc(480_000),
                size: SharesMicro::from_raw(20 * 1_000_000),
            };
            paired_rep.format_step(&sample_row(base_ms, 300, &sub_up, &sub_dn));

            let req_up = OrderAction::CancelAndReplace {
                outcome: Outcome::Up,
                old_order_id: "ord-u".to_string(),
                new_price: MicroUsdc(494_800),
                new_size: SharesMicro::from_raw(20 * 1_000_000),
                reason: CancelReason::DeadbandDrift,
            };
            let req_dn = OrderAction::CancelAndReplace {
                outcome: Outcome::Down,
                old_order_id: "ord-d".to_string(),
                new_price: MicroUsdc(465_200),
                new_size: SharesMicro::from_raw(20 * 1_000_000),
                reason: CancelReason::DeadbandDrift,
            };
            let lines = paired_rep.format_step(&sample_row(base_ms + 100, 299, &req_up, &req_dn));
            assert_eq!(lines.len(), 1);
            if mode_idx == 0 {
                assert!(lines[0].contains("UP: 20 @ ~50¢~"));
                assert!(lines[0].contains("49.48¢"));
                assert!(lines[0].contains("DN: 20 @ ~48¢~"));
                assert!(lines[0].contains("46.52¢"));
                assert!(lines[0].contains("DeadbandDrift"));
            } else {
                assert!(lines[0].contains("\x1b[2;9m50¢\x1b[0m"));
                assert!(lines[0].contains("\x1b[2;9m48¢\x1b[0m"));
                assert!(lines[0].contains("\x1b[90mDeadbandDrift\x1b[0m"));
            }
        }

        // Same-price CancelAndReplace (e.g. size-only ExcessiveSize) omits redundant strikethrough.
        let mut size_only_rep =
            LifecycleStreamReporter::new(LifecycleDisplayMode::Stream, Theme::plain());
        let sub_same = OrderAction::SubmitNew {
            outcome: Outcome::Up,
            price: MicroUsdc(500_000),
            size: SharesMicro::from_raw(20 * 1_000_000),
        };
        size_only_rep.format_step(&sample_row(base_ms, 300, &sub_same, &hold));
        let resize_up = OrderAction::CancelAndReplace {
            outcome: Outcome::Up,
            old_order_id: "ord-u".to_string(),
            new_price: MicroUsdc(500_000),
            new_size: SharesMicro::from_raw(10 * 1_000_000),
            reason: CancelReason::ExcessiveSize,
        };
        let resize_lines =
            size_only_rep.format_step(&sample_row(base_ms + 100, 299, &resize_up, &hold));
        assert_eq!(resize_lines.len(), 1);
        assert!(resize_lines[0].contains("UP: 10 @ 50¢"));
        assert!(resize_lines[0].contains("DN: hold @ 47¢"));
        assert!(resize_lines[0].contains("ExcessiveSize"));
        assert!(!resize_lines[0].contains("~50¢~"));

        // Epoch rollover reset clears cached prior quote prices so stale prices do not leak.
        size_only_rep.reset_epoch();
        let post_reset_req = OrderAction::CancelAndReplace {
            outcome: Outcome::Up,
            old_order_id: "ord-u2".to_string(),
            new_price: MicroUsdc(480_000),
            new_size: SharesMicro::from_raw(10 * 1_000_000),
            reason: CancelReason::ExcessiveSize,
        };
        let mut next_epoch_row = sample_row(base_ms + 200, 300, &post_reset_req, &hold);
        next_epoch_row.resting_bid_up = None;
        let post_reset = size_only_rep.format_step(&next_epoch_row);
        assert_eq!(post_reset.len(), 2);
        assert!(post_reset[0].contains("UP: 10 @ 48¢"));
        assert!(post_reset[0].contains("DN: hold @ 47¢"));
        assert!(post_reset[0].contains("ExcessiveSize"));
        assert!(!post_reset[0].contains("~"));

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
        assert!(p1[0].contains("hold"));

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
        assert!(l1[0].contains("circuit breaker"));

        // Repeated Some("circuit_breaker") stays silent.
        active_row.now_ms = base_ms + 200;
        assert!(rep.format_step(&active_row).is_empty());

        // Transition Some("circuit_breaker") -> Some("stale_feed"): emits [ORDER] with new reason.
        active_row.now_ms = base_ms + 300;
        active_row.cancel_all_reason = Some("stale_feed");
        let l2 = rep.format_step(&active_row);
        assert_eq!(l2.len(), 1);
        assert!(l2[0].contains("stale feed"));

        // Transition Some("stale_feed") -> None: emits recovery/clear notice.
        let cleared_row = sample_row(base_ms + 400, 299, &hold, &hold);
        let l3 = rep.format_step(&cleared_row);
        assert_eq!(l3.len(), 1);
        assert!(l3[0].contains("[RESUME]"));
        assert!(l3[0].contains("Cancel-all cleared (was stale_feed)"));

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

    #[test]
    fn heartbeat_pipe_columns_align_across_varying_rows_plain_and_styled() {
        let hold = OrderAction::Hold;
        let base_ms = 1_795_345_200_000_u64;

        let mut row1 = sample_row(base_ms, 178, &hold, &hold);
        row1.u_e6 = 20_000;
        row1.best_bid_up = Some(MicroUsdc(210_000));
        row1.best_ask_up = Some(MicroUsdc(220_000));
        row1.best_bid_dn = Some(MicroUsdc(780_000));
        row1.best_ask_dn = Some(MicroUsdc(790_000));
        row1.resting_bid_up = Some(MicroUsdc(210_000));
        row1.resting_bid_dn = Some(MicroUsdc(740_000));
        row1.net_pnl_micro = 157_800;

        let mut row2 = sample_row(base_ms + 5_000, 173, &hold, &hold);
        row2.phase = EpochPhase::PreExpiryLockout;
        row2.tier = RiskTier::Tier2ModerateSkew;
        row2.u_e6 = 455_000;
        row2.spot_usd = Some(89_971.0);
        row2.best_bid_up = None;
        row2.best_ask_up = Some(MicroUsdc(10_000));
        row2.best_bid_dn = Some(MicroUsdc(990_000));
        row2.best_ask_dn = None;
        row2.resting_bid_up = None;
        row2.resting_bid_dn = Some(MicroUsdc(780_000));
        row2.q_up = SharesMicro::ZERO;
        row2.q_dn = SharesMicro::from_raw(1_500_000);
        row2.net_delta = -1;
        row2.net_pnl_micro = -202_800;

        let mut row3 = sample_row(base_ms + 10_000, 28, &hold, &hold);
        row3.phase = EpochPhase::MandatoryFlatten;
        row3.tier = RiskTier::Tier3OneSided;
        row3.u_e6 = 800_000;
        row3.spot_usd = None;
        row3.best_bid_up = None;
        row3.best_ask_up = None;
        row3.best_bid_dn = Some(MicroUsdc(500_000));
        row3.best_ask_dn = Some(MicroUsdc(510_000));
        row3.resting_bid_up = Some(MicroUsdc(500_000));
        row3.resting_bid_dn = None;
        row3.q_up = SharesMicro::from_raw(20_250_000);
        row3.q_dn = SharesMicro::from_raw(10_500_000);
        row3.net_delta = 9;
        row3.net_pnl_micro = 0;

        let mut row4 = sample_row(base_ms + 15_000, 5, &hold, &hold);
        row4.phase = EpochPhase::TerminalLockout;
        row4.tier = RiskTier::Tier4HardStop;
        row4.u_e6 = 1_000_000;
        row4.spot_usd = Some(88_766.0);
        row4.resting_bid_up = Some(MicroUsdc(500_000));
        row4.resting_bid_dn = Some(MicroUsdc(500_000));
        row4.q_up = SharesMicro::ZERO;
        row4.q_dn = SharesMicro::ZERO;
        row4.net_delta = 0;
        row4.net_pnl_micro = 1_234_567;

        let mut row5 = sample_row(base_ms + 20_000, 140, &hold, &hold);
        row5.u_e6 = 200_000;
        row5.spot_usd = None;
        row5.best_bid_up = Some(MicroUsdc(473_318));
        row5.best_ask_up = Some(MicroUsdc(503_318));
        row5.best_bid_dn = Some(MicroUsdc(466_682));
        row5.best_ask_dn = Some(MicroUsdc(496_682));
        row5.resting_bid_up = Some(MicroUsdc(499_439));
        row5.resting_bid_dn = Some(MicroUsdc(465_196));
        row5.q_up = SharesMicro::from_raw(1_000_250_000);
        row5.q_dn = SharesMicro::ZERO;
        row5.net_delta = 1_000;
        row5.net_pnl_micro = 0;

        let pipe_cols = |line: &str| -> Vec<usize> {
            theme::strip_ansi(line)
                .chars()
                .enumerate()
                .filter_map(|(idx, ch)| matches!(ch, '|' | '│').then_some(idx))
                .collect()
        };

        for theme in [Theme::plain(), Theme::styled()] {
            let mut rep = LifecycleStreamReporter::new(LifecycleDisplayMode::Stream, theme);
            let header = format_stream_header(theme);
            let expected_pipes = pipe_cols(&header);
            assert_eq!(expected_pipes.len(), 5, "header must contain 5 separators");

            let lines: Vec<String> = [&row1, &row2, &row3, &row4, &row5]
                .into_iter()
                .map(|row| {
                    let emitted = rep.format_step(row);
                    assert!(!emitted.is_empty());
                    // Heartbeats are always emitted last; a periodic latency row may precede them.
                    emitted.into_iter().last().expect("heartbeat emitted")
                })
                .collect();

            let stripped_r2 = theme::strip_ansi(&lines[1]);
            assert!(stripped_r2.contains("Inv 0/1.5"));
            assert!(stripped_r2.contains("Bk --/1"));
            assert!(stripped_r2.contains("99/--"));
            assert!(stripped_r2.contains("Q --/78"));

            let stripped_r3 = theme::strip_ansi(&lines[2]);
            assert!(stripped_r3.contains("Inv 20.25/10.5"));

            let stripped_r4 = theme::strip_ansi(&lines[3]);
            assert!(stripped_r4.contains("Inv 0/0"));
            assert!(stripped_r4.contains("Q 50/50"));

            let stripped_r5 = theme::strip_ansi(&lines[4]);
            assert!(stripped_r5.contains("Bk 47/50"));
            assert!(stripped_r5.contains("47/50"));
            assert!(stripped_r5.contains("Q 50/47"));
            assert!(stripped_r5.contains("Inv 1000.25/0"));

            let beat_col = lines[0]
                .find("[♥ BEAT]")
                .map(|i| lines[0][..i].chars().count());
            for (idx, line) in lines.iter().enumerate() {
                let plain = theme::strip_ansi(line);
                assert_eq!(
                    plain.matches(['|', '│']).count(),
                    expected_pipes.len(),
                    "row {idx} separator count drifted: {plain}"
                );
                assert!(
                    plain.chars().count() <= 140,
                    "row {idx} wraps ({} chars): {plain}",
                    plain.chars().count()
                );
                assert_eq!(
                    line.find("[♥ BEAT]").map(|i| line[..i].chars().count()),
                    beat_col,
                    "row {idx} event column drifted: {plain}"
                );
            }
        }
    }
    #[test]
    fn latency_rows_are_throttled_and_breaches_print_immediately() {
        let hold = OrderAction::Hold;
        let base_ms = 1_795_345_200_000_u64;
        let mut rep = LifecycleStreamReporter::new(LifecycleDisplayMode::Stream, Theme::plain());

        // Tick 0: heartbeat only; the throttle window is seeded here.
        let first = rep.format_step(&sample_row(base_ms, 300, &hold, &hold));
        assert_eq!(first.len(), 1);
        assert!(first[0].contains("[♥ BEAT]"));

        // +5s: heartbeat only, nominal latency stays quiet.
        let mid = rep.format_step(&sample_row(base_ms + 5_000, 295, &hold, &hold));
        assert_eq!(mid.len(), 1);
        assert!(mid[0].contains("[♥ BEAT]"));

        // +10s: periodic health check returns.
        let periodic = rep.format_step(&sample_row(base_ms + 10_000, 290, &hold, &hold));
        assert_eq!(periodic.len(), 2);
        assert!(periodic[0].contains("[LATENCY]"));
        assert!(!periodic[0].contains(">"));

        // A breach is never throttled when severe (>2x budget).
        let mut breach = sample_row(base_ms + 10_100, 289, &hold, &hold);
        breach.risk_ns = RISK_BUDGET_US * 1_000 + 1;
        let breach_lines = rep.format_step(&breach);
        assert_eq!(breach_lines.len(), 1);
        assert!(breach_lines[0].contains("Risk >10us"));

        // `--verbose-latency` restores per-tick rows.
        let mut verbose =
            LifecycleStreamReporter::new(LifecycleDisplayMode::Stream, Theme::plain())
                .with_verbose_latency(true);
        assert_eq!(
            verbose
                .format_step(&sample_row(base_ms, 300, &hold, &hold))
                .len(),
            2
        );
        assert_eq!(
            verbose
                .format_step(&sample_row(base_ms + 100, 299, &hold, &hold))
                .len(),
            1
        );
    }

    #[test]
    fn stream_rows_stay_within_a_standard_terminal_width_in_plain_and_styled_modes() {
        let hold = OrderAction::Hold;
        let replace = OrderAction::CancelAndReplace {
            outcome: Outcome::Down,
            old_order_id: "ord-2".to_string(),
            new_price: MicroUsdc(320_000),
            new_size: SharesMicro::from_raw(20 * 1_000_000),
            reason: CancelReason::DeadbandDrift,
        };
        let base_ms = 1_795_345_200_000_u64;

        for theme in [Theme::plain(), Theme::styled()] {
            let mut rep = LifecycleStreamReporter::new(LifecycleDisplayMode::Stream, theme);
            for (idx, line) in rep
                .format_step(&sample_row(base_ms, 101, &hold, &replace))
                .iter()
                .enumerate()
            {
                assert!(
                    theme::visible_width(line) <= 140,
                    "styled line {idx} exceeds 140 visible chars: {}",
                    theme::visible_width(line)
                );
            }
            let suppr_lines = {
                let mut rep = LifecycleStreamReporter::new(LifecycleDisplayMode::Stream, theme);
                let mut row = sample_row(base_ms, 240, &hold, &hold);
                row.quote = QuoteOutput::Suppressed(SuppressionReason::BothLegsSuppressed);
                rep.format_step(&row)
            };
            for line in &suppr_lines {
                assert!(
                    theme::visible_width(line) <= 140,
                    "suppression wraps: {line}"
                );
            }
            assert!(theme::strip_ansi(&suppr_lines[0]).contains("Quoting paused"));
            assert_eq!(theme::visible_width(&theme.phase_rule()), 124);
        }
    }

    #[test]
    fn suppression_and_resume_report_gate_price_and_align_with_order_columns() {
        let hold = OrderAction::Hold;
        let base_ms = 1_795_345_200_000_u64;
        let submit_up = OrderAction::SubmitNew {
            outcome: Outcome::Up,
            price: MicroUsdc(200_000),
            size: SharesMicro::from_raw(20 * 1_000_000),
        };
        let cancel_up = OrderAction::Cancel {
            outcome: Outcome::Up,
            order_id: "ord-up".to_string(),
            reason: CancelReason::RiskSuppressed,
        };

        let ctx = SuppressionContext {
            bid_up_floored: Some(MicroUsdc(190_000)),
            bid_dn_floored: Some(MicroUsdc(750_000)),
            entry_guard_suppressed_up: true,
            entry_guard_suppressed_dn: true,
            max_entry_prob: Some(0.80),
            max_entry_price: Some(0.60),
            ..Default::default()
        };

        let bar_positions = |line: &str| -> Vec<usize> {
            line.chars()
                .enumerate()
                .filter_map(|(idx, ch)| (ch == '|').then_some(idx))
                .collect()
        };

        let mut rep = LifecycleStreamReporter::new(LifecycleDisplayMode::Stream, Theme::plain());
        let mut order_row = sample_row(base_ms, 213, &cancel_up, &hold);
        order_row.quote = QuoteOutput::Active {
            bid_up: Some(MicroUsdc(200_000)),
            bid_dn: None,
            pair_cost_micro: None,
            gross_margin_bps: None,
        };
        let order_lines = rep.format_step(&order_row);
        let order_line = order_lines
            .iter()
            .find(|l| l.contains("[ORDER]"))
            .expect("expected order line");
        let order_bars = bar_positions(order_line);
        assert!(order_bars.len() >= 2);

        let mut suppr_row = sample_row(base_ms + 1_000, 212, &cancel_up, &hold);
        suppr_row.quote = QuoteOutput::Suppressed(SuppressionReason::BothLegsSuppressed);
        suppr_row.suppression_ctx = Some(ctx);
        let suppr_lines = rep.format_step(&suppr_row);
        let suppr_line = &suppr_lines[0];
        assert!(suppr_line.contains("[SUPPR]"));
        assert!(suppr_line.contains("UP: 19¢ < 20¢ floor"));
        assert!(suppr_line.contains("DN: 75¢ > 60¢ cap"));
        let suppr_bars = bar_positions(suppr_line);
        assert_eq!(suppr_bars.len(), 2);
        assert_eq!(suppr_bars[0], order_bars[0]);
        assert_eq!(suppr_bars[1], order_bars[1]);

        let mut resume_row = sample_row(base_ms + 2_000, 211, &submit_up, &hold);
        resume_row.quote = QuoteOutput::Active {
            bid_up: Some(MicroUsdc(200_000)),
            bid_dn: None,
            pair_cost_micro: None,
            gross_margin_bps: None,
        };
        let resume_lines = rep.format_step(&resume_row);
        let resume_line = &resume_lines[0];
        assert!(resume_line.contains("[RESUME]"));
        assert!(resume_line.contains("UP active (DN suppressed)"));
        let resume_bars = bar_positions(resume_line);
        assert_eq!(resume_bars[0], order_bars[0]);

        for theme in [Theme::plain(), Theme::styled()] {
            let mut r = LifecycleStreamReporter::new(LifecycleDisplayMode::Stream, theme);
            let mut s = sample_row(base_ms, 212, &hold, &hold);
            s.quote = QuoteOutput::Suppressed(SuppressionReason::BothLegsSuppressed);
            s.suppression_ctx = Some(ctx);
            for line in r.format_step(&s) {
                assert!(theme::visible_width(&line) <= 140, "{line}");
            }
        }

        let mixed_ctx = SuppressionContext {
            bid_up_floored: Some(MicroUsdc(500_000)),
            post_only_suppressed_up: true,
            ..Default::default()
        };
        assert_eq!(
            format_leg_suppression(Outcome::Up, mixed_ctx, Some(MicroUsdc(500_000))),
            "UP: 50¢ post-only"
        );
        assert_eq!(
            format_leg_suppression(Outcome::Down, mixed_ctx, None),
            "DN: <1¢ min tick"
        );
    }

    #[test]
    fn order_idle_cancel_price_suppression_transition_latency_cooldown_and_heartbeat_enrichment() {
        let hold = OrderAction::Hold;
        let base_ms = 1_795_345_200_000_u64;

        // 1. [ORDER] idle vs hold @ px and Cancel (px)
        let mut rep = LifecycleStreamReporter::new(LifecycleDisplayMode::Stream, Theme::plain());
        let _ = rep.format_step(&sample_row(base_ms, 300, &hold, &hold));

        let cancel_up = OrderAction::Cancel {
            outcome: Outcome::Up,
            order_id: "ord-u".to_string(),
            reason: CancelReason::RiskSuppressed,
        };
        let mut row_cancel = sample_row(base_ms + 100, 299, &cancel_up, &hold);
        row_cancel.resting_bid_up = Some(MicroUsdc(200_000));
        row_cancel.resting_bid_dn = None;
        let lines = rep.format_step(&row_cancel);
        assert_eq!(lines.len(), 1);
        assert!(lines[0].contains("UP: Cancel (20¢)"), "{}", lines[0]);
        assert!(lines[0].contains("DN: idle"), "{}", lines[0]);

        // 2. [SUPPR] WarmingUp -> BothLegsSuppressed preserves gate + price columns
        let mut suppr_rep =
            LifecycleStreamReporter::new(LifecycleDisplayMode::Stream, Theme::plain());
        let mut warm_row = sample_row(base_ms, 300, &hold, &hold);
        warm_row.quote = QuoteOutput::Suppressed(SuppressionReason::WarmingUp);
        let w = suppr_rep.format_step(&warm_row);
        assert!(w[0].contains("Warming up"), "{}", w[0]);

        let mut both_row = sample_row(base_ms + 100, 299, &hold, &hold);
        both_row.quote = QuoteOutput::Suppressed(SuppressionReason::BothLegsSuppressed);
        both_row.suppression_ctx = Some(SuppressionContext {
            bid_up_floored: Some(MicroUsdc(190_000)),
            bid_dn_floored: Some(MicroUsdc(750_000)),
            entry_guard_suppressed_up: true,
            entry_guard_suppressed_dn: true,
            max_entry_prob: Some(0.80),
            max_entry_price: Some(0.60),
            ..Default::default()
        });
        let b = suppr_rep.format_step(&both_row);
        assert!(b[0].contains("UP: 19¢ < 20¢ floor"), "{}", b[0]);
        assert!(b[0].contains("DN: 75¢ > 60¢ cap"), "{}", b[0]);

        // 3. [LATENCY] minor breach cooldown + severe breach bypass + (+N minor) rollup
        let mut lat_rep =
            LifecycleStreamReporter::new(LifecycleDisplayMode::Stream, Theme::plain());
        let _ = lat_rep.format_step(&sample_row(base_ms, 300, &hold, &hold));

        let mut minor1 = sample_row(base_ms + 500, 299, &hold, &hold);
        minor1.risk_ns = 14_000;
        let m1 = lat_rep.format_step(&minor1);
        assert_eq!(m1.len(), 1);
        assert!(m1[0].contains("Risk >10us"), "{}", m1[0]);

        let mut minor2 = sample_row(base_ms + 1_500, 298, &hold, &hold);
        minor2.risk_ns = 15_000;
        assert!(lat_rep.format_step(&minor2).is_empty());

        let mut severe = sample_row(base_ms + 2_000, 297, &hold, &hold);
        severe.risk_ns = 25_000;
        let sev = lat_rep.format_step(&severe);
        assert_eq!(sev.len(), 1);
        assert!(sev[0].contains("Risk >10us (+1 minor)"), "{}", sev[0]);

        // 4. [♥ BEAT] pair cost on Q and unhedged avg cost on Inv
        let mut hb_rep = LifecycleStreamReporter::new(LifecycleDisplayMode::Stream, Theme::plain());
        let mut hb_row = sample_row(base_ms, 300, &hold, &hold);
        hb_row.avg_cost_up = Some(MicroUsdc(480_000));
        let hb = hb_rep.format_step(&hb_row);
        assert!(hb[0].contains("Q 50/47 (97¢)"), "{}", hb[0]);
        assert!(hb[0].contains("Inv 20@48¢/0 (Δ+20)"), "{}", hb[0]);
    }
}

#[cfg(test)]
mod row_grammar_tests {
    use super::*;
    use core_types::MicroUsdc;

    fn sample_fill(maker: bool, outcome: Outcome, side: OrderSide) -> SimFill {
        SimFill {
            sim_id: 12,
            outcome,
            side,
            price: MicroUsdc(480_000),
            filled_shares: SharesMicro::from_raw(20_000_000),
            fill_ts_ms: 0,
            is_maker: maker,
        }
    }

    #[test]
    fn fill_row_reads_as_columns_without_key_values() {
        let fill = sample_fill(true, Outcome::Up, OrderSide::Buy);
        let row = format_fill_line(
            Theme::plain(),
            0,
            300,
            RiskTier::Tier1Normal,
            FillImpact {
                fill: &fill,
                q_up: SharesMicro::from_raw(20_000_000),
                q_dn: SharesMicro::ZERO,
                realized_pnl_micro: 0,
            },
        );
        assert!(row.contains("[FILL]"));
        assert!(row.contains("[MAKER]"));
        assert!(row.contains("UP BUY 20 @ 48¢"));
        assert!(row.contains("| Notional $9.60 |"));
        assert!(row.contains("Inv 20/0 (Δ+20)"));
        assert!(row.contains("| PnL +$0.00"));
        assert!(!row.contains('='));
        assert!(row.len() <= 140, "{row}");
    }

    #[test]
    fn taker_fill_shows_taker_badge_and_down_leg() {
        let fill = sample_fill(false, Outcome::Down, OrderSide::Sell);
        let row = format_fill_line(
            Theme::plain(),
            0,
            120,
            RiskTier::Tier2ModerateSkew,
            FillImpact {
                fill: &fill,
                q_up: SharesMicro::ZERO,
                q_dn: SharesMicro::from_raw(1_500_000),
                realized_pnl_micro: -250_000,
            },
        );
        assert!(row.contains("[TAKER]"));
        assert!(row.contains("DN SELL 20 @ 48¢"));
        assert!(row.contains("Inv 0/1.5 (Δ-1.5)"));
        assert!(row.contains("PnL -$0.25"));
        assert!(row.len() <= 140, "{row}");
    }

    #[test]
    fn breaker_rows_use_human_reason_and_state() {
        let tripped = format_breaker_line(
            Theme::plain(),
            0,
            300,
            BreakerStatus::Tripped {
                reason: BreakerReason::PolymarketFeedStale,
            },
        );
        assert!(tripped.contains("00:00:00Z T-300s [BREAKER]"));
        assert!(tripped.contains("Quoting halted"));
        assert!(tripped.contains("Polymarket feed stale"));
        assert!(!tripped.contains('{'), "{tripped}");
        assert!(tripped.len() <= 140, "{tripped}");

        let armed = format_breaker_line(Theme::plain(), 0, 300, BreakerStatus::ArmActive);
        assert!(armed.contains("Armed"));
        assert!(armed.contains("all risk gates healthy"));
    }

    #[test]
    fn epoch_blocks_are_double_ruled_and_free_of_debug_dumps() {
        let rollover = format_epoch_rollover_block(
            Theme::plain(),
            "btc-updown-5m-1795405200",
            Some("btc-updown-5m-1795405500"),
            Outcome::Down,
            0,
            0,
        );
        let lines: Vec<&str> = rollover.lines().collect();
        assert_eq!(lines.len(), 3);
        assert!(lines[0].chars().all(|c| c == '='));
        assert!(lines[2].chars().all(|c| c == '='));
        assert!(lines[1].contains("* EPOCH ROLLOVER"));
        assert!(lines[1].contains("btc-updown-5m-1795405200 -> 1795405500"));
        assert!(lines[1].contains("Winner: DOWN"));
        assert!(lines[1].contains("Settle: +$0.00"));
        assert!(lines[1].contains("CumPnL: +$0.00"));
        assert!(!lines[1].contains("winner="), "{}", lines[1]);

        let banner = format_epoch_banner(
            Theme::plain(),
            2,
            "btc-updown-5m-1795405200",
            Some(61_295.92),
            0,
            300_000,
        );
        let blines: Vec<&str> = banner.lines().collect();
        assert_eq!(blines.len(), 3);
        assert!(blines[0].chars().all(|c| c == '='));
        assert!(blines[2].chars().all(|c| c == '='));
        assert!(blines[1].contains("+ EPOCH #2"));
        assert!(blines[1].contains("Strike: $61,295.92"));
        assert!(blines[1].contains("(300s)"));
        for line in blines.iter().chain(lines.iter()) {
            assert!(line.len() <= 140, "{line}");
        }
    }
}

#[cfg(test)]
mod summary_tests {
    use super::*;

    fn lat(p99_ns: u64) -> LatencySummary {
        LatencySummary {
            count: 10,
            zero_ns_samples: 0,
            min_ns: 1_000,
            p50_ns: 12_000,
            p95_ns: 20_000,
            p99_ns,
            p999_ns: 30_000,
            max_ns: 30_000,
        }
    }

    fn sample_stats() -> LifecycleReportStats {
        LifecycleReportStats {
            ticks_evaluated: 1_234,
            orders_submitted: 340,
            orders_replaced: 88,
            orders_canceled: 41,
            orders_held: 12,
            flattens_executed: 1,
            phase_transitions: 3,
            breaker_trips: 2,
            fills_count: 120,
            merges_count: 58,
            total_pairs_merged: SharesMicro::from_raw(58_000_000),
            realized_pnl_micro: 120_000,
            unrealized_pnl_micro: -20_000,
            settlement_pnl_micro: 50_000,
            fees_paid_micro: 10_000,
            estimated_rebate_micro: 2_500,
            epochs_completed: 1,
            epoch_rollovers: 0,
            final_q_up: SharesMicro::from_raw(20_000_000),
            final_q_dn: SharesMicro::from_raw(18_000_000),
            rpm_60s: 4,
            fill_ratio: 0.42,
            markout_1s_bps: 1.2,
            markout_5s_bps: -0.4,
            baseline_pnl_micro: 0,
            ..Default::default()
        }
    }

    #[test]
    fn summary_is_framed_grouped_and_free_of_key_values() {
        let stats = sample_stats();
        let block = format_lifecycle_summary(
            Theme::plain(),
            &stats,
            &lat(12_000),
            &lat(9_000),
            &lat(24_000),
        );
        assert!(block.contains("■ LIFECYCLE SUMMARY") || block.contains("LIFECYCLE SUMMARY"));
        assert!(block.contains("EXECUTION & FILLS"));
        assert!(block.contains("INVENTORY & PNL"));
        assert!(block.contains("RISK & BREAKERS"));
        assert!(block.contains("COMPUTE LATENCY HDR"));
        assert!(block.contains('+') && block.contains('|'));
        assert!(!block.contains("key="));
        assert!(!block.contains("Orders (Sub/Rep/Cxl):"));
        for line in block.lines() {
            assert!(line.chars().count() <= 140, "{line}");
        }
    }

    #[test]
    fn summary_marks_breaker_counts_and_latency_breaches() {
        let stats = sample_stats();
        let block = format_lifecycle_summary(
            Theme::plain(),
            &stats,
            &lat(12_000),
            &lat(9_000),
            &lat(24_000),
        );
        assert!(block.contains("Breakers"));
        assert!(block.contains("2"));

        let styled = format_lifecycle_summary(
            Theme::styled(),
            &stats,
            &lat(30_000),
            &lat(9_000),
            &lat(120_000),
        );
        // Breaker count and the two breached P99 cells all render in alarm colors.
        assert!(
            styled.contains("\x1b[1;31m"),
            "no breach highlight in summary"
        );
        assert!(styled.contains("\x1b[33m"), "breaker count not highlighted");
    }
}
