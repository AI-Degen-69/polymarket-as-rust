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

use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Parser, Subcommand};

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
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    let result = match cli.command {
        Commands::Doctor { jsonl } => doctor::run(&jsonl),
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
        }

        let custom_cli =
            Cli::try_parse_from(["poly-as", "doctor", "--jsonl", "out/custom.jsonl"]).unwrap();
        match custom_cli.command {
            Commands::Doctor { jsonl } => {
                assert_eq!(jsonl, PathBuf::from("out/custom.jsonl"));
            }
        }
    }
}
