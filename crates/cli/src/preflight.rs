//! Process-start preflight: refuse live work while the host clock is unsafe.
//!
//! A stopped Windows Time service let the host clock free-run ~1s behind the
//! exchange, which tripped the `ClockDriftExceeded` breaker on the first tick.
//! The same drift is now caught before any feed or order starts.

use std::error::Error;
use std::process::Command;

use core_types::{ClockOffsetEstimator, EngineConfig};
use feeds::GammaDiscoveryClient;

use crate::theme::{self, Badge};

/// Elevated-PowerShell remedy for a drifting or unsynchronized host clock.
pub(crate) const CLOCK_FIX_HINT: &str = "Set-Service w32time -StartupType Automatic; \
     Start-Service w32time; w32tm /resync";

enum ClockCheck {
    Measured { offset_ms: i64, source: String },
    Unavailable(String),
}

enum TimeService {
    Running,
    Stopped,
    Unknown,
}

/// Measures the host clock against the exchange and prints the preflight verdict.
///
/// Fails when `|offset|` exceeds `cfg.clock_drift_max_ms`, because the risk
/// breaker would halt quoting on the first tick anyway.
pub(crate) fn check_host_clock(cfg: &EngineConfig) -> Result<(), Box<dyn Error>> {
    let t = theme::current();
    println!();
    println!("{}", t.section("PREFLIGHT :: HOST CLOCK & TIME SYNC"));

    let mut drift_failure = None;
    match measure_host_clock(cfg) {
        ClockCheck::Measured { offset_ms, source } => {
            let limit = cfg.clock_drift_max_ms;
            let ok = drift_within_tolerance(offset_ms, limit);
            println!(
                "{}",
                t.kv(
                    "Host Clock:",
                    &format!(
                        "offset {offset_ms:+}ms vs {source} (limit {limit}ms) {}",
                        t.pass_fail(ok)
                    )
                )
            );
            if !ok {
                drift_failure = Some(format!(
                    "host clock drift {offset_ms:+}ms exceeds the {limit}ms tolerance"
                ));
            }
        }
        ClockCheck::Unavailable(reason) => {
            println!(
                "{}",
                t.kv(
                    "Host Clock:",
                    &format!(
                        "unavailable, continuing unverified {} ({reason})",
                        t.badge(Badge::Warn)
                    )
                )
            );
        }
    }

    let service = query_time_service();
    match service {
        TimeService::Running => println!(
            "{}",
            t.kv(
                "Windows Time Service:",
                &format!("running {}", t.badge(Badge::Pass))
            )
        ),
        TimeService::Unknown => println!(
            "{}",
            t.kv("Windows Time Service:", &t.muted("state unavailable"))
        ),
        TimeService::Stopped => {
            println!(
                "{}",
                t.kv(
                    "Windows Time Service:",
                    &format!(
                        "stopped; clock free-runs until it starts {}",
                        t.badge(Badge::Warn)
                    )
                )
            );
            print_fix_hint();
        }
    }

    if let Some(reason) = drift_failure {
        print_fix_hint();
        return Err(format!("{reason}; fix the host clock and rerun").into());
    }
    Ok(())
}

fn print_fix_hint() {
    let t = theme::current();
    println!(
        "  {} Fix it in an elevated PowerShell, then rerun:",
        t.badge(Badge::Warn)
    );
    println!("      {CLOCK_FIX_HINT}");
}

fn drift_within_tolerance(offset_ms: i64, limit_ms: i64) -> bool {
    offset_ms.unsigned_abs() <= limit_ms.unsigned_abs()
}

fn measure_host_clock(cfg: &EngineConfig) -> ClockCheck {
    let measured = (|| -> Result<(i64, String), Box<dyn Error>> {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        rt.block_on(async {
            let discovery = GammaDiscoveryClient::new()?;
            let estimator = ClockOffsetEstimator::new(cfg.clock_drift_max_ms);
            let report = discovery.sync_clock(&estimator).await?;
            Ok((report.offset_ms, report.source))
        })
    })();
    match measured {
        Ok((offset_ms, source)) => ClockCheck::Measured { offset_ms, source },
        Err(err) => ClockCheck::Unavailable(err.to_string()),
    }
}

fn query_time_service() -> TimeService {
    let Ok(output) = Command::new("sc.exe").args(["query", "w32time"]).output() else {
        return TimeService::Unknown;
    };
    if !output.status.success() {
        return TimeService::Unknown;
    }
    let text = String::from_utf8_lossy(&output.stdout);
    match parse_sc_state(&text) {
        Some(4) => TimeService::Running,
        Some(1) => TimeService::Stopped,
        _ => TimeService::Unknown,
    }
}

/// Extracts the SCM state code (`4` running, `1` stopped) from `sc.exe` output.
///
/// `sc.exe` localizes the labels but keeps the numeric state; exit codes print as `0`.
fn parse_sc_state(output: &str) -> Option<u32> {
    output
        .split_whitespace()
        .filter_map(|token| token.parse::<u32>().ok())
        .find(|code| (1..=7).contains(code))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn drift_tolerance_is_inclusive_at_the_limit() {
        assert!(drift_within_tolerance(0, 1_000));
        assert!(drift_within_tolerance(-1_000, 1_000));
        assert!(drift_within_tolerance(1_000, 1_000));
        assert!(!drift_within_tolerance(1_001, 1_000));
        assert!(!drift_within_tolerance(-1_098, 1_000));
    }

    #[test]
    fn sc_state_parsing_skips_type_and_exit_codes() {
        let running = "\r\nSERVICE_NAME: w32time\r\n        TYPE               : 20  WIN32_SHARE_PROCESS\r\n        STATE              : 4  RUNNING\r\n                                (STOPPABLE, NOT_PAUSABLE)\r\n        WIN32_EXIT_CODE    : 0  (0x0)\r\n";
        assert_eq!(parse_sc_state(running), Some(4));

        let stopped = "SERVICE_NAME: w32time\n        TYPE               : 20  WIN32_SHARE_PROCESS\n        STATE              : 1  STOPPED\n        WIN32_EXIT_CODE    : 0  (0x0)\n";
        assert_eq!(parse_sc_state(stopped), Some(1));

        assert_eq!(parse_sc_state("no state here"), None);
    }
}
