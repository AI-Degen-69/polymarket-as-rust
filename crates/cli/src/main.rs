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
mod feeds_cmd;
mod feeds_report;
mod trace_cmd;
mod trace_report;

use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Parser, Subcommand, ValueEnum};
use core_types::{KappaMode, SgMode, VarianceMode};

use feeds_cmd::FeedsArgs;
use trace_cmd::{TraceArgs, TraceSourceMode};

#[derive(Debug, Clone, Copy, ValueEnum)]
enum CliTraceSource {
    Live,
    Replay,
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
            Commands::Feeds { .. } | Commands::Trace { .. } => panic!("expected Doctor"),
        }

        let custom_cli =
            Cli::try_parse_from(["poly-as", "doctor", "--jsonl", "out/custom.jsonl"]).unwrap();
        match custom_cli.command {
            Commands::Doctor { jsonl } => {
                assert_eq!(jsonl, PathBuf::from("out/custom.jsonl"));
            }
            Commands::Feeds { .. } | Commands::Trace { .. } => panic!("expected Doctor"),
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
            Commands::Doctor { .. } | Commands::Trace { .. } => panic!("expected Feeds"),
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
            Commands::Doctor { .. } | Commands::Trace { .. } => panic!("expected Feeds"),
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
            Commands::Doctor { .. } | Commands::Feeds { .. } => panic!("expected Trace"),
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
            Commands::Doctor { .. } | Commands::Feeds { .. } => panic!("expected Trace"),
        }
    }
}
