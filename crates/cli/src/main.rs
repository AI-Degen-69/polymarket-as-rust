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
mod paper_cmd;
mod paper_report;
mod sandbox_cmd;
mod sandbox_report;
mod trace_cmd;
mod trace_report;

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
        #[arg(long, default_value = "logs/sandbox-health.jsonl")]
        jsonl: PathBuf,
    },
    /// List sandbox markets through Gamma preprod edge credentials (NEX CLOB fallback).
    Markets {
        /// Maximum markets requested.
        #[arg(long, default_value_t = 20)]
        limit: usize,
        /// Output path for structured JSONL telemetry.
        #[arg(long, default_value = "logs/sandbox-markets.jsonl")]
        jsonl: PathBuf,
    },
    /// Read `GET /book?token_id=...` for one sandbox outcome.
    Book {
        /// Sandbox outcome asset ID (V2 position ID or CTF token ID).
        #[arg(long)]
        asset_id: String,
        /// Output path for structured JSONL telemetry.
        #[arg(long, default_value = "logs/sandbox-book.jsonl")]
        jsonl: PathBuf,
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
        #[arg(long, default_value = "logs/sandbox-validate.jsonl")]
        jsonl: PathBuf,
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
        #[arg(long, default_value = "logs/sandbox-feeds.jsonl")]
        jsonl: PathBuf,
    },
}

#[derive(Debug, Parser)]
#[command(
    name = "poly-as",
    version,
    about = "Polymarket 5-Minute BTC Avellaneda-Stoikov HFT Bot (Terminal CLI)"
)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// Phase 0 diagnostic: verify Windows clock resolution, fixed-point wire math, config redaction, and JSONL telemetry.
    Doctor {
        /// Output path for structured JSONL telemetry log.
        #[arg(long, default_value = "logs/phase0-doctor.jsonl")]
        jsonl: PathBuf,
    },
    /// Phase 1 feed verifier: live Gamma/CLOB discovery, Binance Spot & Polymarket WS streams, or offline replay.
    Feeds {
        /// Discover active 5-minute BTC market and stream live feeds.
        #[arg(long)]
        discover: bool,
        /// Live capture duration (`20s`, `500ms`, `1m`).
        #[arg(long, default_value = "20s")]
        duration: String,
        /// Replay recorded JSONL feed log offline.
        #[arg(long)]
        replay: Option<PathBuf>,
        /// Optional separate output JSONL report path for `--replay` (never overwrites `--replay`).
        #[arg(long)]
        replay_out: Option<PathBuf>,
        /// Output path for structured JSONL telemetry log during `--discover`.
        #[arg(long, default_value = "logs/phase1-feeds.jsonl")]
        jsonl: PathBuf,
    },
    /// Phase 2 math engine tracer: stream live or replayed ticks through Stations 1–6.
    Trace {
        /// Feed source (`live` WebSockets or `replay` JSONL log).
        #[arg(long, value_enum, default_value_t = CliTraceSource::Live)]
        source: CliTraceSource,
        /// Input JSONL path used when `--source replay` (defaults to `logs/phase1-feeds.jsonl`).
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
        /// Output path for structured JSONL telemetry log.
        #[arg(long, default_value = "logs/phase2-trace.jsonl")]
        jsonl: PathBuf,
    },
    /// Phase 3 paper simulator: run FIFO queue-aware paper trading against live WebSockets or replayed JSONL logs.
    #[command(alias = "sim")]
    Paper {
        /// Feed source (`live` WebSockets or `replay` JSONL log).
        #[arg(long, value_enum, default_value_t = CliTraceSource::Live)]
        source: CliTraceSource,
        /// Input JSONL path used when `--source replay` (defaults to `logs/phase1-feeds.jsonl`).
        #[arg(long)]
        input: Option<PathBuf>,
        /// Simulation duration (`60s`, `300s`, `5m`).
        #[arg(long, default_value = "60s")]
        duration: String,
        /// Maximum dollar position capital in USD (`$50` default).
        #[arg(long, default_value_t = 50)]
        v_max: u64,
        /// Base order size in whole shares (`20` default).
        #[arg(long, default_value_t = 20)]
        base_size: u64,
        /// Simulated wire placement & cancel latency in milliseconds (`40ms` default).
        #[arg(long, default_value_t = 40)]
        latency_ms: u64,
        /// Output path for structured JSONL telemetry log.
        #[arg(long, default_value = "logs/phase3-paper.jsonl")]
        jsonl: PathBuf,
    },
    /// Phase 4 risk & lifecycle actor pipeline: run deterministic 300s epoch or live feeds with 4-tier risk limits.
    Lifecycle {
        /// Source mode (`live` WebSockets or `sim-epoch` simulated 300s epoch).
        #[arg(long, value_enum, default_value_t = CliLifecycleSource::SimEpoch)]
        source: CliLifecycleSource,
        /// Live run duration (`60s`, `90s`, `300s`).
        #[arg(long, default_value = "90s")]
        duration: String,
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
        /// Pin the live Pricing actor OS thread to a zero-based logical CPU core.
        #[arg(long)]
        pin_pricing_core: Option<usize>,
        /// Pin the live Risk actor OS thread to a zero-based logical CPU core.
        #[arg(long)]
        pin_risk_core: Option<usize>,
        /// Output path for structured JSONL telemetry log.
        #[arg(long, default_value = "logs/phase4-lifecycle.jsonl")]
        jsonl: PathBuf,
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
        /// Output path for structured JSONL telemetry log.
        #[arg(long, default_value = "logs/phase5-exec-dry.jsonl")]
        jsonl: PathBuf,
    },
}

fn dispatch_cli(cli: Cli) -> Result<(), Box<dyn std::error::Error>> {
    match cli.command {
        Commands::Doctor { jsonl } => doctor::run(&jsonl),
        Commands::Feeds {
            discover,
            duration,
            replay,
            replay_out,
            jsonl,
        } => feeds_cmd::run(FeedsArgs {
            discover,
            duration,
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
            jsonl,
        }),
        Commands::Paper {
            source,
            input,
            duration,
            v_max,
            base_size,
            latency_ms,
            jsonl,
        } => paper_cmd::run(paper_cmd::PaperArgs {
            source: match source {
                CliTraceSource::Live => TraceSourceMode::Live,
                CliTraceSource::Replay => TraceSourceMode::Replay,
            },
            input,
            duration,
            v_max,
            base_size,
            latency_ms,
            jsonl,
        }),
        Commands::Lifecycle {
            source,
            duration,
            fast_forward,
            inject_fault,
            v_max,
            base_size,
            pin_pricing_core,
            pin_risk_core,
            jsonl,
        } => lifecycle_cmd::run(LifecycleArgs {
            source: match source {
                CliLifecycleSource::Live => LifecycleSourceMode::Live,
                CliLifecycleSource::SimEpoch => LifecycleSourceMode::SimEpoch,
            },
            duration,
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
            pinning: risk_lifecycle::CorePinning {
                pricing_core: pin_pricing_core,
                risk_core: pin_risk_core,
            },
            jsonl,
        }),
        Commands::Sandbox { action } => match action {
            SandboxCommands::Health {
                require_allowlisted_egress,
                jsonl,
            } => sandbox_cmd::run(
                jsonl,
                sandbox_cmd::SandboxAction::Health {
                    require_allowlisted_egress,
                },
            ),
            SandboxCommands::Markets { limit, jsonl } => {
                sandbox_cmd::run(jsonl, sandbox_cmd::SandboxAction::Markets { limit })
            }
            SandboxCommands::Book { asset_id, jsonl } => {
                sandbox_cmd::run(jsonl, sandbox_cmd::SandboxAction::Book { asset_id })
            }
            SandboxCommands::Validate {
                asset_id,
                confirm_sandbox_trade,
                jsonl,
            } => sandbox_cmd::run(
                jsonl,
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
                jsonl,
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
            jsonl,
        }),
    }
}

fn main() -> ExitCode {
    match dispatch_cli(Cli::parse()) {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("ERROR: {err}");
            let mut cause = err.source();
            while let Some(inner) = cause {
                eprintln!("  caused by: {inner}");
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
                assert_eq!(jsonl, PathBuf::from("logs/phase0-doctor.jsonl"));
            }
            _ => panic!("expected Doctor"),
        }

        let custom_cli =
            Cli::try_parse_from(["poly-as", "doctor", "--jsonl", "out/custom.jsonl"]).unwrap();
        match custom_cli.command {
            Commands::Doctor { jsonl } => {
                assert_eq!(jsonl, PathBuf::from("out/custom.jsonl"));
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
                replay,
                replay_out,
                jsonl,
            } => {
                assert!(discover);
                assert_eq!(duration, "20s");
                assert!(replay.is_none());
                assert!(replay_out.is_none());
                assert_eq!(jsonl, PathBuf::from("logs/phase1-feeds.jsonl"));
            }
            _ => panic!("expected Feeds"),
        }

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
                assert_eq!(jsonl, PathBuf::from("logs/sandbox-health.jsonl"));
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
                v_max,
                base_size,
                latency_ms,
                jsonl,
                ..
            } => {
                assert!(matches!(source, CliTraceSource::Live));
                assert_eq!(duration, "60s");
                assert_eq!(v_max, 100);
                assert_eq!(base_size, 25);
                assert_eq!(latency_ms, 50);
                assert_eq!(jsonl, PathBuf::from("logs/phase3-paper.jsonl"));
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
}
