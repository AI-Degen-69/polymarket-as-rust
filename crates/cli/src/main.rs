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

use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Parser, Subcommand};

use feeds_cmd::FeedsArgs;

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
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    let result = match cli.command {
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
    };

    match result {
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
            Commands::Feeds { .. } => panic!("expected Doctor"),
        }

        let custom_cli =
            Cli::try_parse_from(["poly-as", "doctor", "--jsonl", "out/custom.jsonl"]).unwrap();
        match custom_cli.command {
            Commands::Doctor { jsonl } => {
                assert_eq!(jsonl, PathBuf::from("out/custom.jsonl"));
            }
            Commands::Feeds { .. } => panic!("expected Doctor"),
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
            Commands::Doctor { .. } => panic!("expected Feeds"),
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
            Commands::Doctor { .. } => panic!("expected Feeds"),
        }
    }
}
