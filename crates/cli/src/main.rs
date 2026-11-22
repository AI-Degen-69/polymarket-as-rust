//! Unified terminal CLI binary `poly-as` for Polymarket 5-minute BTC market making.

#![forbid(unsafe_code)]
#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::indexing_slicing
    )
)]

mod doctor;
mod exec_cmd;
mod exec_report;
mod feeds_cmd;
mod feeds_report;
mod lifecycle_cmd;
mod lifecycle_report;
mod log_paths;
mod paper_cmd;
mod paper_report;
mod sandbox_cmd;
mod sandbox_report;
mod theme;
mod trace_cmd;
mod trace_report;
mod window_gate;

use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Parser, Subcommand, ValueEnum};
use core_types::{KappaMode, NexWsVersion, SgMode, VarianceMode};

use exec_cmd::{ExecArgs, ExecMode};
use feeds_cmd::FeedsArgs;
use lifecycle_cmd::{CliFaultMode, LifecycleArgs, LifecycleSourceMode};
use trace_cmd::{TraceArgs, TraceSourceMode};

#[derive(Debug, Clone, Copy, ValueEnum)]
enum CliTraceSource {
    Live,
    Replay,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum CliLifecycleSource {
    Live,
    SimEpoch,
    Replay,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum CliLifecycleDisplay {
    Stream,
    Raw,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum CliFaultModeArg {
    None,
    Stale,
    Tier4,
    TickChange,
    Disconnect,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum CliSgMode {
    Causal,
    Symmetric,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum CliKappaMode {
    Normalized,
    LogSlope,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum CliVarianceMode {
    BinaryCara,
    LibraryStrict,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum CliExecMode {
    #[value(name = "dry-run")]
    DryRun,
    #[value(name = "canary-live")]
    CanaryLive,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum CliQueueDecay {
    Conservative,
    Optimistic,
}

impl From<CliQueueDecay> for paper_sim::QueueDecayPolicy {
    fn from(policy: CliQueueDecay) -> Self {
        match policy {
            CliQueueDecay::Conservative => Self::Conservative,
            CliQueueDecay::Optimistic => Self::Optimistic,
        }
    }
}

/// CLI override value for `--max-entry-prob` supporting either a probability bound or `off`/`none`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum MaxEntryProbOverride {
    Off,
    Value(f64),
}

fn parse_max_entry_prob_override(raw: &str) -> Result<MaxEntryProbOverride, String> {
    let trimmed = raw.trim();
    if trimmed.eq_ignore_ascii_case("off") || trimmed.eq_ignore_ascii_case("none") {
        return Ok(MaxEntryProbOverride::Off);
    }
    let val = trimmed.parse::<f64>().map_err(|_| {
        format!("invalid --max-entry-prob '{raw}': expected finite float in (0.5, 1.0) or 'off'")
    })?;
    if !val.is_finite() || val <= 0.5 || val >= 1.0 {
        return Err("`--max-entry-prob` must be finite in (0.5, 1.0) or 'off'".to_string());
    }
    Ok(MaxEntryProbOverride::Value(val))
}

/// Optional Stations 4–6 math competitiveness overrides for live/replay A/B testing.
#[derive(Debug, Clone, Copy, Default, clap::Args)]
pub(crate) struct MathOverrides {
    /// Override CARA risk aversion parameter `gamma` (`0.15` default).
    #[arg(long)]
    pub(crate) gamma: Option<f64>,
    /// Override maximum combined UP+DOWN bid in micro-USDC (`<= 990000`, `980000` default).
    #[arg(long)]
    pub(crate) max_combined_bid_micro: Option<u64>,
    /// Override whether Station 4 adds `gamma * V_rem` variance spread (`true` default).
    #[arg(long)]
    pub(crate) include_variance_spread: Option<bool>,
    /// Override minimum spread floor in ticks (`1` default; `2` restores legacy 2-tick floor).
    #[arg(long)]
    pub(crate) min_spread_ticks: Option<u32>,
    /// Override whether Station 4 enforces taker-fee spread floor (`false` default; auto-disabled when `--min-spread-ticks 1` unless set).
    #[arg(long)]
    pub(crate) fee_spread_floor: Option<bool>,
    /// Optional maximum allowed cross-tick pair loss in micro-USDC (`0` enforces `cost_old + bid_new <= max_combined_bid_micro`).
    #[arg(long)]
    pub(crate) max_cross_tick_loss_micro: Option<u64>,
    /// Override extreme-probability entry guard in `(0.5, 1.0)` (`0.80` default; pass `off` or `none` to disable).
    #[arg(long, value_parser = parse_max_entry_prob_override)]
    pub(crate) max_entry_prob: Option<MaxEntryProbOverride>,
    /// Override Binance lead-lag `|delta_p_star|` threshold in `(0.0, 1.0)` (`0.02` default).
    #[arg(long)]
    pub(crate) lead_lag_threshold: Option<f64>,
    /// Override rolling Binance lead-lag lookback window in ms (`0..=6000`, `1000` default).
    #[arg(long)]
    pub(crate) lead_lag_window_ms: Option<u64>,
    /// Override per-side lead-lag cancel cooldown in ms (`0..=10000`, `1000` default).
    #[arg(long)]
    pub(crate) lead_lag_cooldown_ms: Option<u64>,
    /// Override whether Binance lead-lag cancels only the endangered leg (`true` default).
    #[arg(long)]
    pub(crate) lead_lag_directional: Option<bool>,
}

impl MathOverrides {
    pub(crate) const fn is_any_set(self) -> bool {
        self.gamma.is_some()
            || self.max_combined_bid_micro.is_some()
            || self.include_variance_spread.is_some()
            || self.min_spread_ticks.is_some()
            || self.fee_spread_floor.is_some()
            || self.max_cross_tick_loss_micro.is_some()
            || self.max_entry_prob.is_some()
            || self.lead_lag_threshold.is_some()
            || self.lead_lag_window_ms.is_some()
            || self.lead_lag_cooldown_ms.is_some()
            || self.lead_lag_directional.is_some()
    }

    pub(crate) fn apply_to(
        self,
        cfg: &mut core_types::EngineConfig,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(gamma) = self.gamma {
            cfg.gamma = gamma;
        }
        if let Some(include_var) = self.include_variance_spread {
            cfg.include_variance_spread = include_var;
        }
        if let Some(ticks) = self.min_spread_ticks {
            if ticks == 0 {
                return Err("`--min-spread-ticks` must be >= 1".into());
            }
            cfg.min_spread_ticks = ticks;
            cfg.min_combined_spread_micro =
                core_types::MicroUsdc(u64::from(ticks).saturating_mul(core_types::ONE_CENT_MICRO));
            if ticks == 1 && self.fee_spread_floor.is_none() {
                cfg.enforce_fee_floor = false;
            }
        }
        if let Some(fee_floor) = self.fee_spread_floor {
            cfg.enforce_fee_floor = fee_floor;
        }
        if let Some(max_bid) = self.max_combined_bid_micro {
            if max_bid == 0 || max_bid > 990_000 {
                return Err("`--max-combined-bid-micro` must be in 1..=990000".into());
            }
            cfg.max_combined_bid_micro = core_types::MicroUsdc(max_bid);
        }
        if let Some(loss) = self.max_cross_tick_loss_micro {
            cfg.max_cross_tick_loss_micro = Some(loss);
        }
        if let Some(entry_prob) = self.max_entry_prob {
            match entry_prob {
                MaxEntryProbOverride::Off => cfg.max_entry_prob = None,
                MaxEntryProbOverride::Value(p_max) => {
                    if !p_max.is_finite() || p_max <= 0.5 || p_max >= 1.0 {
                        return Err("`--max-entry-prob` must be finite in (0.5, 1.0)".into());
                    }
                    cfg.max_entry_prob = Some(p_max);
                }
            }
        }
        if let Some(th) = self.lead_lag_threshold {
            if !th.is_finite() || th <= 0.0 || th >= 1.0 {
                return Err("`--lead-lag-threshold` must be finite in (0.0, 1.0)".into());
            }
            cfg.lead_lag_cancel_threshold = th;
        }
        if let Some(win) = self.lead_lag_window_ms {
            if win > 6_000 {
                return Err("`--lead-lag-window-ms` must be <= 6000".into());
            }
            if win > 0 && win < 50 {
                return Err("`--lead-lag-window-ms` must be 0 or >= 50ms".into());
            }
            cfg.lead_lag_window_ms = win;
        }
        if let Some(cd) = self.lead_lag_cooldown_ms {
            if cd > 10_000 {
                return Err("`--lead-lag-cooldown-ms` must be <= 10000".into());
            }
            cfg.lead_lag_cooldown_ms = cd;
        }
        if let Some(dir) = self.lead_lag_directional {
            cfg.lead_lag_directional = dir;
        }
        cfg.validate()?;
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum CliWsVersion {
    #[value(name = "original")]
    Original,
    #[value(name = "subs-v2")]
    SubsV2,
}

#[derive(Debug, Subcommand)]
enum SandboxCommands {
    /// Prove the allowlisted public route: egress IP, pinned ingress, TCP 443, TLS SNI/Host, GET /health.
    Health {
        /// Fail when the observed egress IPv4 differs from NEX_ALLOWLISTED_EGRESS_IP.
        #[arg(long, default_value_t = false)]
        require_allowlisted_egress: bool,
        /// Output path for structured JSONL telemetry.
        #[arg(long)]
        jsonl: Option<PathBuf>,
    },
    /// List sandbox markets through Gamma preprod edge credentials (NEX CLOB fallback).
    Markets {
        /// Maximum markets requested.
        #[arg(long, default_value_t = 20)]
        limit: usize,
        /// Output path for structured JSONL telemetry.
        #[arg(long)]
        jsonl: Option<PathBuf>,
    },
    /// Read `GET /book?token_id=...` for one sandbox outcome.
    Book {
        /// Sandbox outcome asset ID (V2 position ID or CTF token ID).
        #[arg(long)]
        asset_id: String,
        /// Output path for structured JSONL telemetry.
        #[arg(long)]
        jsonl: Option<PathBuf>,
    },
    /// Place, read back, and cancel the minimum-size post-only limit buy at one tick.
    Validate {
        /// Sandbox outcome asset ID to quote.
        #[arg(long)]
        asset_id: String,
        /// Explicit acknowledgement that this is an Amoy test-pUSD sandbox order.
        #[arg(long, default_value_t = false)]
        confirm_sandbox_trade: bool,
        /// Output path for structured JSONL telemetry.
        #[arg(long)]
        jsonl: Option<PathBuf>,
    },
    /// Stream the NEX market and authenticated user feeds with heartbeats and snapshot rebuilds.
    Feeds {
        /// UP / Yes outcome asset ID.
        #[arg(long)]
        up_asset_id: String,
        /// DOWN / No outcome asset ID.
        #[arg(long)]
        dn_asset_id: String,
        /// Capture duration (`20s`, `60s`).
        #[arg(long, default_value = "20s")]
        duration: String,
        /// Wire path family (`original` or `subs-v2`).
        #[arg(long, value_enum, default_value_t = CliWsVersion::SubsV2)]
        ws_version: CliWsVersion,
        /// Test hook: force a socket rollover after N seconds to exercise snapshot rebuild.
        #[arg(long)]
        reconnect_after_secs: Option<u64>,
        /// Output path for structured JSONL telemetry.
        #[arg(long)]
        jsonl: Option<PathBuf>,
    },
}

#[derive(Debug, Parser)]
#[command(
    name = "poly-as",
    version,
    about = "Polymarket 5-Minute BTC Avellaneda-Stoikov HFT Bot (Terminal CLI)"
)]
struct Cli {
    /// Control ANSI terminal color output (`auto`, `always`, `never`).
    #[arg(long, global = true, value_enum, default_value_t = theme::ColorChoice::Auto)]
    color: theme::ColorChoice,
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// Phase 0 diagnostic: verify Windows clock resolution, fixed-point wire math, config redaction, and JSONL telemetry.
    Doctor {
        /// Output path for structured JSONL telemetry log (defaults to `logs/doctor/<DD-MM_HH-MM-SSZ>.jsonl`).
        #[arg(long)]
        jsonl: Option<PathBuf>,
    },
    /// Phase 1 feed verifier: live Gamma/CLOB discovery, Binance Spot & Polymarket WS streams, or offline replay.
    Feeds {
        /// Discover active 5-minute BTC market and stream live feeds.
        #[arg(long)]
        discover: bool,
        /// Live capture duration (`20s`, `500ms`, `1m`; defaults to `20s` when `--windows` is omitted).
        #[arg(long, conflicts_with = "windows")]
        duration: Option<String>,
        /// Capture `N` full 5-minute windows (`N >= 1`) and stop cleanly at the end of window `N`.
        #[arg(long, conflicts_with = "duration")]
        windows: Option<u32>,
        /// Allow starting mid-window instead of waiting idle for the next fresh 5-minute boundary.
        #[arg(long, default_value_t = false)]
        allow_mid_window: bool,
        /// Replay recorded JSONL feed log offline.
        #[arg(long)]
        replay: Option<PathBuf>,
        /// Optional separate output JSONL report path for `--replay` (never overwrites `--replay`).
        #[arg(long)]
        replay_out: Option<PathBuf>,
        /// Output path for structured JSONL telemetry log during `--discover` (defaults to `logs/captures/<stamp>_<dur>.inprogress.jsonl` renamed on close).
        #[arg(long)]
        jsonl: Option<PathBuf>,
    },
    /// Phase 2 math engine tracer: stream live or replayed ticks through Stations 1–6.
    Trace {
        /// Feed source (`live` WebSockets or `replay` JSONL log).
        #[arg(long, value_enum, default_value_t = CliTraceSource::Live)]
        source: CliTraceSource,
        /// Input JSONL path used when `--source replay` (defaults to newest capture in `logs/captures/`).
        #[arg(long)]
        input: Option<PathBuf>,
        /// Station 1 Savitzky-Golay filter mode (`causal` default or `symmetric`).
        #[arg(long, value_enum, default_value_t = CliSgMode::Causal)]
        sg_mode: CliSgMode,
        /// Station 3 liquidity decay estimator (`normalized` default or `log-slope`).
        #[arg(long, value_enum, default_value_t = CliKappaMode::Normalized)]
        kappa_mode: CliKappaMode,
        /// Station 2/5 remaining variance mode (`binary-cara` default or `library-strict`).
        #[arg(long, value_enum, default_value_t = CliVarianceMode::BinaryCara)]
        variance_mode: CliVarianceMode,
        /// Number of post-warmup 100ms evaluation ticks to print and record (`>= 1`).
        #[arg(long, default_value_t = 25)]
        ticks: usize,
        /// Simulated net UP-DOWN share inventory `q` (integer shares).
        #[arg(long, default_value_t = 0)]
        inventory: i64,
        #[command(flatten)]
        math: MathOverrides,
        /// Output path for structured JSONL telemetry log (defaults to `logs/trace/<DD-MM_HH-MM-SSZ>.jsonl`).
        #[arg(long)]
        jsonl: Option<PathBuf>,
    },
    /// Phase 3 paper simulator: run FIFO queue-aware paper trading against live WebSockets or replayed JSONL logs.
    #[command(alias = "sim")]
    Paper {
        /// Feed source (`live` WebSockets or `replay` JSONL log).
        #[arg(long, value_enum, default_value_t = CliTraceSource::Live)]
        source: CliTraceSource,
        /// Input JSONL path used when `--source replay` (defaults to newest capture in `logs/captures/`).
        #[arg(long)]
        input: Option<PathBuf>,
        /// Simulation duration (`60s`, `300s`, `5m`).
        #[arg(long, default_value = "60s")]
        duration: String,
        /// Allow starting `--source live` mid-window instead of waiting idle for the next fresh 5-minute boundary.
        #[arg(long, default_value_t = false)]
        allow_mid_window: bool,
        /// Maximum dollar position capital in USD (`$50` default).
        #[arg(long, default_value_t = 50)]
        v_max: u64,
        /// Base order size in whole shares (`20` default).
        #[arg(long, default_value_t = 20)]
        base_size: u64,
        /// Simulated wire placement & cancel latency in milliseconds (`40ms` default).
        #[arg(long, default_value_t = 40)]
        latency_ms: u64,
        /// FIFO queue decay policy (`conservative` default or `optimistic`).
        #[arg(long, value_enum, default_value_t = CliQueueDecay::Conservative)]
        queue_decay: CliQueueDecay,
        #[command(flatten)]
        math: MathOverrides,
        /// Output path for structured JSONL telemetry log (defaults to `logs/paper/<DD-MM_HH-MM-SSZ>.jsonl`).
        #[arg(long)]
        jsonl: Option<PathBuf>,
    },
    /// Phase 4 risk & lifecycle actor pipeline: run deterministic 300s epoch, live feeds, or offline replay with 4-tier risk limits.
    Lifecycle {
        /// Source mode (`live` WebSockets, `sim-epoch` simulated 300s epoch, or `replay` JSONL log).
        #[arg(long, value_enum, default_value_t = CliLifecycleSource::SimEpoch)]
        source: CliLifecycleSource,
        /// Terminal display mode (`stream` events + 5s/1s heartbeats, or `raw` 10 Hz tick lines).
        #[arg(long, value_enum, default_value_t = CliLifecycleDisplay::Stream)]
        display: CliLifecycleDisplay,
        /// Input JSONL path used when `--source replay` (defaults to newest capture in `logs/captures/`).
        #[arg(long)]
        input: Option<PathBuf>,
        /// Live run duration (`60s`, `90s`, `300s`).
        #[arg(long, default_value = "90s")]
        duration: String,
        /// Allow starting `--source live` mid-window instead of waiting idle for the next fresh 5-minute boundary.
        #[arg(long, default_value_t = false)]
        allow_mid_window: bool,
        /// Fast-forward virtual clock in sim-epoch mode.
        #[arg(long)]
        fast_forward: bool,
        /// Inject adversarial fault mode (`none`, `stale`, `tier4`, `tick-change`, `disconnect`).
        #[arg(long, value_enum, default_value_t = CliFaultModeArg::None, alias = "fault")]
        inject_fault: CliFaultModeArg,
        /// Maximum dollar position capital in USD (`$50` default).
        #[arg(long, default_value_t = 50)]
        v_max: u64,
        /// Base order size in whole shares (`20` default).
        #[arg(long, default_value_t = 20)]
        base_size: u64,
        /// FIFO queue decay policy (`conservative` default or `optimistic`).
        #[arg(long, value_enum, default_value_t = CliQueueDecay::Conservative)]
        queue_decay: CliQueueDecay,
        #[command(flatten)]
        math: MathOverrides,
        /// Pin the live Pricing actor OS thread to a zero-based logical CPU core.
        #[arg(long)]
        pin_pricing_core: Option<usize>,
        /// Pin the live Risk actor OS thread to a zero-based logical CPU core.
        #[arg(long)]
        pin_risk_core: Option<usize>,
        /// Output path for structured JSONL telemetry log (defaults to `logs/lifecycle/<DD-MM_HH-MM-SSZ>.jsonl`).
        #[arg(long)]
        jsonl: Option<PathBuf>,
    },
    /// NEX sandbox (Amoy 80002) health gate, market discovery, validation order, and feeds.
    Sandbox {
        /// Sandbox subcommand.
        #[command(subcommand)]
        action: SandboxCommands,
    },
    /// Phase 5 execution tracer: EIP-712 signing, dry-run dispatch, and settlement preview.
    Exec {
        /// Execution safety mode (`dry-run` default or `canary-live`).
        #[arg(long, value_enum, default_value_t = CliExecMode::DryRun)]
        mode: CliExecMode,
        /// Explicit live-capital acknowledgement required for `canary-live`.
        #[arg(long, default_value_t = false)]
        confirm_live_capital: bool,
        /// Canary dollar cap in USD (`<= 25` enforced).
        #[arg(long, default_value_t = 10)]
        canary_max_usd: u64,
        /// Dry-run trace duration (`60s` default).
        #[arg(long, default_value = "60s")]
        duration: String,
        /// Output path for structured JSONL telemetry log (defaults to `logs/exec/<DD-MM_HH-MM-SSZ>.jsonl`).
        #[arg(long)]
        jsonl: Option<PathBuf>,
    },
}

fn dispatch_cli(cli: Cli) -> Result<(), Box<dyn std::error::Error>> {
    theme::init(cli.color);
    match cli.command {
        Commands::Doctor { jsonl } => {
            let path = jsonl.unwrap_or_else(|| log_paths::default_feature_log_path("doctor"));
            doctor::run(&path)
        }
        Commands::Feeds {
            discover,
            duration,
            windows,
            allow_mid_window,
            replay,
            replay_out,
            jsonl,
        } => feeds_cmd::run(FeedsArgs {
            discover,
            duration,
            windows,
            allow_mid_window,
            replay,
            replay_out,
            jsonl,
        }),
        Commands::Trace {
            source,
            input,
            sg_mode,
            kappa_mode,
            variance_mode,
            ticks,
            inventory,
            math,
            jsonl,
        } => trace_cmd::run(TraceArgs {
            source: match source {
                CliTraceSource::Live => TraceSourceMode::Live,
                CliTraceSource::Replay => TraceSourceMode::Replay,
            },
            input,
            sg_mode: match sg_mode {
                CliSgMode::Causal => SgMode::Causal,
                CliSgMode::Symmetric => SgMode::Symmetric,
            },
            kappa_mode: match kappa_mode {
                CliKappaMode::Normalized => KappaMode::TradeNormalizedDepth,
                CliKappaMode::LogSlope => KappaMode::LibraryLogSlope,
            },
            variance_mode: match variance_mode {
                CliVarianceMode::BinaryCara => VarianceMode::BinaryCara,
                CliVarianceMode::LibraryStrict => VarianceMode::LibraryStrict,
            },
            ticks,
            inventory,
            math,
            jsonl: jsonl.unwrap_or_else(|| log_paths::default_feature_log_path("trace")),
        }),
        Commands::Paper {
            source,
            input,
            duration,
            allow_mid_window,
            v_max,
            base_size,
            latency_ms,
            queue_decay,
            math,
            jsonl,
        } => paper_cmd::run(paper_cmd::PaperArgs {
            source: match source {
                CliTraceSource::Live => TraceSourceMode::Live,
                CliTraceSource::Replay => TraceSourceMode::Replay,
            },
            input,
            duration,
            allow_mid_window,
            v_max,
            base_size,
            latency_ms,
            queue_decay: queue_decay.into(),
            math,
            jsonl: jsonl.unwrap_or_else(|| log_paths::default_feature_log_path("paper")),
        }),
        Commands::Lifecycle {
            source,
            display,
            input,
            duration,
            allow_mid_window,
            fast_forward,
            inject_fault,
            v_max,
            base_size,
            queue_decay,
            math,
            pin_pricing_core,
            pin_risk_core,
            jsonl,
        } => lifecycle_cmd::run(LifecycleArgs {
            source: match source {
                CliLifecycleSource::Live => LifecycleSourceMode::Live,
                CliLifecycleSource::SimEpoch => LifecycleSourceMode::SimEpoch,
                CliLifecycleSource::Replay => LifecycleSourceMode::Replay,
            },
            display: match display {
                CliLifecycleDisplay::Stream => lifecycle_report::LifecycleDisplayMode::Stream,
                CliLifecycleDisplay::Raw => lifecycle_report::LifecycleDisplayMode::Raw,
            },
            input,
            duration,
            allow_mid_window,
            fast_forward,
            fault: match inject_fault {
                CliFaultModeArg::None => CliFaultMode::None,
                CliFaultModeArg::Stale => CliFaultMode::Stale,
                CliFaultModeArg::Tier4 => CliFaultMode::Tier4,
                CliFaultModeArg::TickChange => CliFaultMode::TickChange,
                CliFaultModeArg::Disconnect => CliFaultMode::Disconnect,
            },
            v_max,
            base_size,
            queue_decay: queue_decay.into(),
            math,
            pinning: risk_lifecycle::CorePinning {
                pricing_core: pin_pricing_core,
                risk_core: pin_risk_core,
            },
            jsonl: jsonl.unwrap_or_else(|| log_paths::default_feature_log_path("lifecycle")),
        }),
        Commands::Sandbox { action } => match action {
            SandboxCommands::Health {
                require_allowlisted_egress,
                jsonl,
            } => sandbox_cmd::run(
                jsonl.unwrap_or_else(|| log_paths::default_sandbox_log_path("health")),
                sandbox_cmd::SandboxAction::Health {
                    require_allowlisted_egress,
                },
            ),
            SandboxCommands::Markets { limit, jsonl } => sandbox_cmd::run(
                jsonl.unwrap_or_else(|| log_paths::default_sandbox_log_path("markets")),
                sandbox_cmd::SandboxAction::Markets { limit },
            ),
            SandboxCommands::Book { asset_id, jsonl } => sandbox_cmd::run(
                jsonl.unwrap_or_else(|| log_paths::default_sandbox_log_path("book")),
                sandbox_cmd::SandboxAction::Book { asset_id },
            ),
            SandboxCommands::Validate {
                asset_id,
                confirm_sandbox_trade,
                jsonl,
            } => sandbox_cmd::run(
                jsonl.unwrap_or_else(|| log_paths::default_sandbox_log_path("validate")),
                sandbox_cmd::SandboxAction::Validate {
                    asset_id,
                    confirm_sandbox_trade,
                },
            ),
            SandboxCommands::Feeds {
                up_asset_id,
                dn_asset_id,
                duration,
                ws_version,
                reconnect_after_secs,
                jsonl,
            } => sandbox_cmd::run(
                jsonl.unwrap_or_else(|| log_paths::default_sandbox_log_path("feeds")),
                sandbox_cmd::SandboxAction::Feeds {
                    up_asset_id,
                    dn_asset_id,
                    duration,
                    ws_version: match ws_version {
                        CliWsVersion::Original => NexWsVersion::Original,
                        CliWsVersion::SubsV2 => NexWsVersion::SubsV2,
                    },
                    reconnect_after_secs,
                },
            ),
        },
        Commands::Exec {
            mode,
            confirm_live_capital,
            canary_max_usd,
            duration,
            jsonl,
        } => exec_cmd::run(ExecArgs {
            mode: match mode {
                CliExecMode::DryRun => ExecMode::DryRun,
                CliExecMode::CanaryLive => ExecMode::CanaryLive,
            },
            confirm_live_capital,
            canary_max_usd,
            duration,
            jsonl: jsonl.unwrap_or_else(|| log_paths::default_feature_log_path("exec")),
        }),
    }
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    theme::init(cli.color);
    match dispatch_cli(cli) {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            let t = theme::current();
            eprintln!("{} {err}", t.bold_red("ERROR:"));
            let mut cause = err.source();
            while let Some(inner) = cause {
                eprintln!("  {} {inner}", t.muted("caused by:"));
                cause = inner.source();
            }
            ExitCode::FAILURE
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cli_parses_default_and_custom_doctor_jsonl_path() {
        let default_cli = Cli::try_parse_from(["poly-as", "doctor"]).unwrap();
        match default_cli.command {
            Commands::Doctor { jsonl } => {
                assert!(jsonl.is_none());
            }
            _ => panic!("expected Doctor"),
        }

        let custom_cli =
            Cli::try_parse_from(["poly-as", "doctor", "--jsonl", "out/custom.jsonl"]).unwrap();
        match custom_cli.command {
            Commands::Doctor { jsonl } => {
                assert_eq!(jsonl, Some(PathBuf::from("out/custom.jsonl")));
            }
            _ => panic!("expected Doctor"),
        }
    }

    #[test]
    fn cli_parses_feeds_discover_and_replay_flags() {
        let live = Cli::try_parse_from([
            "poly-as",
            "feeds",
            "--discover",
            "--duration",
            "20s",
            "--jsonl",
            "logs/phase1-feeds.jsonl",
        ])
        .unwrap();
        match live.command {
            Commands::Feeds {
                discover,
                duration,
                windows,
                allow_mid_window,
                replay,
                replay_out,
                jsonl,
            } => {
                assert!(discover);
                assert_eq!(duration.as_deref(), Some("20s"));
                assert!(windows.is_none());
                assert!(!allow_mid_window);
                assert!(replay.is_none());
                assert!(replay_out.is_none());
                assert_eq!(jsonl, Some(PathBuf::from("logs/phase1-feeds.jsonl")));
            }
            _ => panic!("expected Feeds"),
        }

        let auto_discover =
            Cli::try_parse_from(["poly-as", "feeds", "--discover", "--duration", "20s"]).unwrap();
        match auto_discover.command {
            Commands::Feeds {
                discover, jsonl, ..
            } => {
                assert!(discover);
                assert!(jsonl.is_none());
            }
            _ => panic!("expected Feeds"),
        }

        let windowed = Cli::try_parse_from([
            "poly-as",
            "feeds",
            "--discover",
            "--windows",
            "3",
            "--allow-mid-window",
        ])
        .unwrap();
        match windowed.command {
            Commands::Feeds {
                discover,
                duration,
                windows,
                allow_mid_window,
                ..
            } => {
                assert!(discover);
                assert!(duration.is_none());
                assert_eq!(windows, Some(3));
                assert!(allow_mid_window);
            }
            _ => panic!("expected Feeds"),
        }

        assert!(
            Cli::try_parse_from([
                "poly-as",
                "feeds",
                "--discover",
                "--duration",
                "60s",
                "--windows",
                "2",
            ])
            .is_err()
        );

        let replay =
            Cli::try_parse_from(["poly-as", "feeds", "--replay", "logs/rec.jsonl"]).unwrap();
        match replay.command {
            Commands::Feeds {
                discover, replay, ..
            } => {
                assert!(!discover);
                assert_eq!(replay, Some(PathBuf::from("logs/rec.jsonl")));
            }
            _ => panic!("expected Feeds"),
        }
    }

    #[test]
    fn cli_parses_trace_live_and_replay_ab_flags() {
        let live = Cli::try_parse_from([
            "poly-as",
            "trace",
            "--source",
            "live",
            "--sg-mode",
            "causal",
            "--ticks",
            "20",
            "--jsonl",
            "logs/phase2-trace.jsonl",
        ])
        .unwrap();
        match live.command {
            Commands::Trace {
                source,
                ticks,
                inventory,
                input,
                ..
            } => {
                assert!(matches!(source, CliTraceSource::Live));
                assert_eq!(ticks, 20);
                assert_eq!(inventory, 0);
                assert!(input.is_none());
            }
            _ => panic!("expected Trace"),
        }

        let ab = Cli::try_parse_from([
            "poly-as",
            "trace",
            "--source",
            "replay",
            "--input",
            "logs/phase1-feeds.jsonl",
            "--sg-mode",
            "symmetric",
            "--kappa-mode",
            "log-slope",
            "--variance-mode",
            "library-strict",
            "--ticks",
            "25",
            "--inventory",
            "20",
            "--jsonl",
            "logs/phase2-trace-ab.jsonl",
        ])
        .unwrap();
        match ab.command {
            Commands::Trace {
                source,
                sg_mode,
                kappa_mode,
                variance_mode,
                ticks,
                inventory,
                ..
            } => {
                assert!(matches!(source, CliTraceSource::Replay));
                assert!(matches!(sg_mode, CliSgMode::Symmetric));
                assert!(matches!(kappa_mode, CliKappaMode::LogSlope));
                assert!(matches!(variance_mode, CliVarianceMode::LibraryStrict));
                assert_eq!(ticks, 25);
                assert_eq!(inventory, 20);
            }
            _ => panic!("expected Trace"),
        }
    }

    #[test]
    fn cli_parses_sandbox_health_and_feeds() {
        let health = Cli::try_parse_from(["poly-as", "sandbox", "health"]).unwrap();
        match health.command {
            Commands::Sandbox {
                action:
                    SandboxCommands::Health {
                        require_allowlisted_egress,
                        jsonl,
                    },
            } => {
                assert!(!require_allowlisted_egress);
                assert!(jsonl.is_none());
            }
            _ => panic!("expected sandbox health"),
        }

        let feeds = Cli::try_parse_from([
            "poly-as",
            "sandbox",
            "feeds",
            "--up-asset-id",
            "111",
            "--dn-asset-id",
            "222",
            "--duration",
            "30s",
            "--ws-version",
            "subs-v2",
            "--reconnect-after-secs",
            "10",
        ])
        .unwrap();
        match feeds.command {
            Commands::Sandbox {
                action:
                    SandboxCommands::Feeds {
                        up_asset_id,
                        dn_asset_id,
                        duration,
                        ws_version,
                        reconnect_after_secs,
                        ..
                    },
            } => {
                assert_eq!(up_asset_id, "111");
                assert_eq!(dn_asset_id, "222");
                assert_eq!(duration, "30s");
                assert!(matches!(ws_version, CliWsVersion::SubsV2));
                assert_eq!(reconnect_after_secs, Some(10));
            }
            _ => panic!("expected sandbox feeds"),
        }
    }

    #[test]
    fn cli_parses_paper_and_sim_alias_flags() {
        let paper = Cli::try_parse_from([
            "poly-as",
            "paper",
            "--source",
            "live",
            "--duration",
            "60s",
            "--v-max",
            "100",
            "--base-size",
            "25",
            "--latency-ms",
            "50",
            "--jsonl",
            "logs/phase3-paper.jsonl",
        ])
        .unwrap();
        match paper.command {
            Commands::Paper {
                source,
                duration,
                allow_mid_window,
                v_max,
                base_size,
                latency_ms,
                jsonl,
                ..
            } => {
                assert!(matches!(source, CliTraceSource::Live));
                assert_eq!(duration, "60s");
                assert!(!allow_mid_window);
                assert_eq!(v_max, 100);
                assert_eq!(base_size, 25);
                assert_eq!(latency_ms, 50);
                assert_eq!(jsonl, Some(PathBuf::from("logs/phase3-paper.jsonl")));
            }
            _ => panic!("expected Paper"),
        }

        let paper_mid =
            Cli::try_parse_from(["poly-as", "paper", "--source", "live", "--allow-mid-window"])
                .unwrap();
        match paper_mid.command {
            Commands::Paper {
                source,
                allow_mid_window,
                ..
            } => {
                assert!(matches!(source, CliTraceSource::Live));
                assert!(allow_mid_window);
            }
            _ => panic!("expected Paper"),
        }

        let sim = Cli::try_parse_from([
            "poly-as",
            "sim",
            "--source",
            "replay",
            "--input",
            "logs/custom.jsonl",
        ])
        .unwrap();
        match sim.command {
            Commands::Paper {
                source,
                input,
                v_max,
                base_size,
                latency_ms,
                ..
            } => {
                assert!(matches!(source, CliTraceSource::Replay));
                assert_eq!(input, Some(PathBuf::from("logs/custom.jsonl")));
                assert_eq!(v_max, 50);
                assert_eq!(base_size, 20);
                assert_eq!(latency_ms, 40);
            }
            _ => panic!("expected Paper"),
        }
    }

    #[test]
    fn cli_parses_math_competitiveness_and_queue_decay_overrides() {
        let lc = Cli::try_parse_from([
            "poly-as",
            "lifecycle",
            "--source",
            "live",
            "--duration",
            "60s",
            "--gamma",
            "0.02",
            "--include-variance-spread",
            "false",
            "--min-spread-ticks",
            "1",
            "--max-combined-bid-micro",
            "990000",
            "--queue-decay",
            "optimistic",
            "--jsonl",
            "logs/bbo-verify.jsonl",
        ])
        .unwrap();
        match lc.command {
            Commands::Lifecycle {
                allow_mid_window,
                queue_decay,
                math,
                ..
            } => {
                assert!(!allow_mid_window);
                assert_eq!(queue_decay, CliQueueDecay::Optimistic);
                assert_eq!(math.gamma, Some(0.02));
                assert_eq!(math.include_variance_spread, Some(false));
                assert_eq!(math.min_spread_ticks, Some(1));
                assert_eq!(math.max_combined_bid_micro, Some(990_000));
                assert_eq!(math.fee_spread_floor, None);

                let mut cfg = core_types::EngineConfig::default();
                math.apply_to(&mut cfg).unwrap();
                assert_eq!(cfg.gamma, 0.02);
                assert!(!cfg.include_variance_spread);
                assert_eq!(cfg.min_spread_ticks, 1);
                assert_eq!(cfg.min_combined_spread_micro, core_types::MicroUsdc(10_000));
                assert_eq!(cfg.max_combined_bid_micro, core_types::MicroUsdc(990_000));
                assert!(!cfg.enforce_fee_floor);
            }
            _ => panic!("expected Lifecycle"),
        }

        let paper_default = Cli::try_parse_from(["poly-as", "paper"]).unwrap();
        match paper_default.command {
            Commands::Paper {
                queue_decay, math, ..
            } => {
                assert_eq!(queue_decay, CliQueueDecay::Conservative);
                let mut cfg = core_types::EngineConfig::default();
                math.apply_to(&mut cfg).unwrap();
                assert_eq!(cfg.gamma, 0.15);
                assert!(cfg.include_variance_spread);
                assert_eq!(cfg.min_spread_ticks, 1);
                assert_eq!(cfg.min_combined_spread_micro, core_types::MicroUsdc(10_000));
                assert_eq!(cfg.max_combined_bid_micro, core_types::MicroUsdc(980_000));
                assert!(!cfg.enforce_fee_floor);
                assert_eq!(cfg.max_entry_prob, Some(0.80));
            }
            _ => panic!("expected Paper"),
        }

        let trace_legacy = Cli::try_parse_from([
            "poly-as",
            "trace",
            "--min-spread-ticks",
            "2",
            "--fee-spread-floor",
            "true",
        ])
        .unwrap();
        match trace_legacy.command {
            Commands::Trace { math, .. } => {
                let mut cfg = core_types::EngineConfig::default();
                math.apply_to(&mut cfg).unwrap();
                assert_eq!(cfg.min_spread_ticks, 2);
                assert_eq!(cfg.min_combined_spread_micro, core_types::MicroUsdc(20_000));
                assert!(cfg.enforce_fee_floor);
            }
            _ => panic!("expected Trace"),
        }

        let trace_override = Cli::try_parse_from([
            "poly-as",
            "trace",
            "--gamma",
            "0.05",
            "--include-variance-spread",
            "false",
            "--min-spread-ticks",
            "1",
            "--fee-spread-floor",
            "true",
            "--max-combined-bid-micro",
            "990000",
        ])
        .unwrap();
        match trace_override.command {
            Commands::Trace { math, .. } => {
                let mut cfg = core_types::EngineConfig::default();
                math.apply_to(&mut cfg).unwrap();
                assert_eq!(cfg.gamma, 0.05);
                assert!(!cfg.include_variance_spread);
                assert_eq!(cfg.min_spread_ticks, 1);
                assert!(cfg.enforce_fee_floor);
                assert_eq!(cfg.max_combined_bid_micro, core_types::MicroUsdc(990_000));
            }
            _ => panic!("expected Trace"),
        }

        let mut bad_bid_cfg = core_types::EngineConfig::default();
        assert!(
            MathOverrides {
                max_combined_bid_micro: Some(995_000),
                ..Default::default()
            }
            .apply_to(&mut bad_bid_cfg)
            .is_err()
        );
        let mut zero_ticks_cfg = core_types::EngineConfig::default();
        assert!(
            MathOverrides {
                min_spread_ticks: Some(0),
                ..Default::default()
            }
            .apply_to(&mut zero_ticks_cfg)
            .is_err()
        );

        let lc_replay = Cli::try_parse_from([
            "poly-as",
            "lifecycle",
            "--source",
            "replay",
            "--input",
            "logs/feeds.jsonl",
            "--gamma",
            "0.08",
        ])
        .unwrap();
        match lc_replay.command {
            Commands::Lifecycle {
                source,
                input,
                math,
                ..
            } => {
                assert!(matches!(source, CliLifecycleSource::Replay));
                assert_eq!(input, Some(PathBuf::from("logs/feeds.jsonl")));
                assert_eq!(math.gamma, Some(0.08));
            }
            _ => panic!("expected Lifecycle replay"),
        }

        let lc_mid = Cli::try_parse_from([
            "poly-as",
            "lifecycle",
            "--source",
            "live",
            "--allow-mid-window",
        ])
        .unwrap();
        match lc_mid.command {
            Commands::Lifecycle {
                source,
                allow_mid_window,
                ..
            } => {
                assert!(matches!(source, CliLifecycleSource::Live));
                assert!(allow_mid_window);
            }
            _ => panic!("expected Lifecycle live"),
        }
    }

    #[test]
    fn cli_parses_global_color_flag_before_and_after_subcommand() {
        let default_cli = Cli::try_parse_from(["poly-as", "doctor"]).unwrap();
        assert_eq!(default_cli.color, theme::ColorChoice::Auto);

        let before = Cli::try_parse_from(["poly-as", "--color", "always", "doctor"]).unwrap();
        assert_eq!(before.color, theme::ColorChoice::Always);

        let after = Cli::try_parse_from(["poly-as", "lifecycle", "--color", "never"]).unwrap();
        assert_eq!(after.color, theme::ColorChoice::Never);
        match after.command {
            Commands::Lifecycle { display, .. } => {
                assert_eq!(display, CliLifecycleDisplay::Stream);
            }
            _ => panic!("expected Lifecycle"),
        }

        let raw_disp = Cli::try_parse_from(["poly-as", "lifecycle", "--display", "raw"]).unwrap();
        match raw_disp.command {
            Commands::Lifecycle { display, .. } => {
                assert_eq!(display, CliLifecycleDisplay::Raw);
            }
            _ => panic!("expected Lifecycle"),
        }
    }

    #[test]
    fn cli_parses_and_validates_cost_aware_math_overrides() {
        let parsed = Cli::try_parse_from([
            "poly-as",
            "lifecycle",
            "--source",
            "replay",
            "--max-cross-tick-loss-micro",
            "20000",
            "--max-entry-prob",
            "0.85",
        ])
        .unwrap();
        match parsed.command {
            Commands::Lifecycle { math, .. } => {
                assert!(math.is_any_set());
                assert_eq!(math.max_cross_tick_loss_micro, Some(20_000));
                assert_eq!(math.max_entry_prob, Some(MaxEntryProbOverride::Value(0.85)));
                let mut cfg = core_types::EngineConfig::default();
                math.apply_to(&mut cfg).unwrap();
                assert_eq!(cfg.max_cross_tick_loss_micro, Some(20_000));
                assert_eq!(cfg.max_entry_prob, Some(0.85));
            }
            _ => panic!("expected Lifecycle"),
        }

        let zero_loss =
            Cli::try_parse_from(["poly-as", "paper", "--max-cross-tick-loss-micro", "0"]).unwrap();
        match zero_loss.command {
            Commands::Paper { math, .. } => {
                let mut cfg = core_types::EngineConfig::default();
                math.apply_to(&mut cfg).unwrap();
                assert_eq!(cfg.max_cross_tick_loss_micro, Some(0));
                assert_eq!(cfg.max_entry_prob, Some(0.80));
            }
            _ => panic!("expected Paper"),
        }

        for off_flag in ["off", "none"] {
            let parsed_off =
                Cli::try_parse_from(["poly-as", "paper", "--max-entry-prob", off_flag]).unwrap();
            match parsed_off.command {
                Commands::Paper { math, .. } => {
                    assert!(math.is_any_set());
                    assert_eq!(math.max_entry_prob, Some(MaxEntryProbOverride::Off));
                    let mut cfg = core_types::EngineConfig::default();
                    math.apply_to(&mut cfg).unwrap();
                    assert_eq!(cfg.max_entry_prob, None);
                }
                _ => panic!("expected Paper"),
            }
        }

        for bad_prob in [0.5, 0.49, 1.0, 1.05, f64::NAN, f64::INFINITY] {
            let mut cfg = core_types::EngineConfig::default();
            let err = MathOverrides {
                max_entry_prob: Some(MaxEntryProbOverride::Value(bad_prob)),
                ..Default::default()
            }
            .apply_to(&mut cfg);
            assert!(err.is_err(), "expected error for max_entry_prob={bad_prob}");
        }

        for bad_cli_prob in ["0.5", "1.0", "bogus"] {
            assert!(
                Cli::try_parse_from(["poly-as", "lifecycle", "--max-entry-prob", bad_cli_prob])
                    .is_err(),
                "expected CLI parse error for --max-entry-prob {bad_cli_prob}"
            );
        }

        assert!(
            Cli::try_parse_from(["poly-as", "lifecycle", "--max-cross-tick-loss-micro", "-5"])
                .is_err()
        );
    }

    #[test]
    fn cli_parses_and_validates_lead_lag_overrides() {
        let parsed = Cli::try_parse_from([
            "poly-as",
            "lifecycle",
            "--source",
            "replay",
            "--lead-lag-threshold",
            "0.015",
            "--lead-lag-window-ms",
            "1000",
            "--lead-lag-cooldown-ms",
            "500",
            "--lead-lag-directional",
            "true",
        ])
        .unwrap();
        match parsed.command {
            Commands::Lifecycle { math, .. } => {
                assert!(math.is_any_set());
                let mut cfg = core_types::EngineConfig::default();
                math.apply_to(&mut cfg).unwrap();
                assert!((cfg.lead_lag_cancel_threshold - 0.015).abs() < 1e-12);
                assert_eq!(cfg.lead_lag_window_ms, 1_000);
                assert_eq!(cfg.lead_lag_cooldown_ms, 500);
                assert!(cfg.lead_lag_directional);
            }
            _ => panic!("expected Lifecycle"),
        }

        let mut cfg = core_types::EngineConfig::default();
        assert!(
            MathOverrides {
                lead_lag_threshold: Some(0.0),
                ..Default::default()
            }
            .apply_to(&mut cfg)
            .is_err()
        );
        assert!(
            MathOverrides {
                lead_lag_window_ms: Some(6_001),
                ..Default::default()
            }
            .apply_to(&mut cfg)
            .is_err()
        );
        assert!(
            MathOverrides {
                lead_lag_window_ms: Some(25),
                ..Default::default()
            }
            .apply_to(&mut cfg)
            .is_err()
        );
        assert!(
            MathOverrides {
                lead_lag_cooldown_ms: Some(10_001),
                ..Default::default()
            }
            .apply_to(&mut cfg)
            .is_err()
        );
    }
}
