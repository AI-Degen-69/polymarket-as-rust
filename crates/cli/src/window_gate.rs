//! Shared fresh 5-minute window alignment gate for live capture and trading commands.

use std::error::Error;
use std::time::Duration;

use chrono::{DateTime, Utc};
use core_types::{ClockOffsetEstimator, EngineConfig, MarketDescriptor, MonotonicClock};
use feeds::{ClockSyncReport, GammaDiscoveryClient};

/// Duration of one Polymarket 5-minute BTC epoch in milliseconds.
pub(crate) const WINDOW_MS: u64 = 300_000;

/// Grace period after a 5-minute UTC boundary where a session is still considered fresh (`<= 1s`).
pub(crate) const FRESH_WINDOW_GRACE_MS: u64 = 1_000;

/// Maximum Gamma discovery retries right after a fresh boundary (20 x 500ms = 10s).
const GAMMA_BOUNDARY_RETRY_ATTEMPTS: usize = 20;
const GAMMA_BOUNDARY_RETRY_INTERVAL: Duration = Duration::from_millis(500);

/// Decision computed from `adjusted_now_ms` and `--allow-mid-window`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WindowGatePlan {
    ProceedImmediately {
        current_window_start_ms: u64,
    },
    WaitUntil {
        elapsed_in_window_ms: u64,
        wait_ms: u64,
        next_window_start_ms: u64,
    },
}

/// Outcome of waiting for and discovering a fresh 5-minute market.
pub(crate) enum GateOutcome<T> {
    Ready(T),
    Cancelled,
}

/// Discovery artifacts returned once the fresh window opens and Gamma serves its market.
pub(crate) struct FreshWindowDiscovery {
    pub(crate) discovery: GammaDiscoveryClient,
    pub(crate) offset_est: ClockOffsetEstimator,
    pub(crate) sync_report: ClockSyncReport,
    pub(crate) desc: MarketDescriptor,
}

/// Computes whether the caller should start immediately or wait until the next 300s UTC boundary.
pub(crate) fn plan_fresh_window_gate(
    adjusted_now_ms: u64,
    allow_mid_window: bool,
) -> WindowGatePlan {
    let current_window_start_ms = (adjusted_now_ms / WINDOW_MS).saturating_mul(WINDOW_MS);
    let elapsed_in_window_ms = adjusted_now_ms % WINDOW_MS;
    if allow_mid_window || elapsed_in_window_ms <= FRESH_WINDOW_GRACE_MS {
        return WindowGatePlan::ProceedImmediately {
            current_window_start_ms,
        };
    }
    let next_window_start_ms = ((adjusted_now_ms / WINDOW_MS) + 1).saturating_mul(WINDOW_MS);
    let wait_ms = next_window_start_ms.saturating_sub(adjusted_now_ms);
    WindowGatePlan::WaitUntil {
        elapsed_in_window_ms,
        wait_ms,
        next_window_start_ms,
    }
}

/// Returns `true` when `desc` belongs to the target window (`start_ts_ms >= min_window_start_ms`).
pub(crate) fn descriptor_matches_window(desc: &MarketDescriptor, min_window_start_ms: u64) -> bool {
    desc.start_ts_ms >= min_window_start_ms
}

/// Formats a Unix millisecond timestamp as `HH:MM:SSZ` UTC for status messages.
pub(crate) fn format_utc_hms(unix_ms: u64) -> String {
    i64::try_from(unix_ms)
        .ok()
        .and_then(DateTime::<Utc>::from_timestamp_millis)
        .map_or_else(
            || format!("{unix_ms}ms"),
            |dt| dt.format("%H:%M:%SZ").to_string(),
        )
}

/// Synchronizes exchange clock offset, waits idle until the next fresh 5-minute boundary when
/// mid-window (unless `allow_mid_window`), and discovers the matching active 5-minute BTC market.
pub(crate) async fn wait_and_discover_fresh_5m_market(
    cfg: &EngineConfig,
    allow_mid_window: bool,
    min_remaining_ms: u64,
) -> Result<GateOutcome<FreshWindowDiscovery>, Box<dyn Error>> {
    let discovery = GammaDiscoveryClient::new()?;
    let offset_est = ClockOffsetEstimator::new(cfg.clock_drift_max_ms);
    let mut sync_report = discovery.sync_clock(&offset_est).await?;
    let adjusted_now_ms = offset_est.adjusted_unix_ms(MonotonicClock::unix_now_ms()?)?;

    match plan_fresh_window_gate(adjusted_now_ms, allow_mid_window) {
        WindowGatePlan::ProceedImmediately {
            current_window_start_ms,
        } => {
            if !allow_mid_window {
                println!(
                    "  [WAIT] Fresh 5m window active ({} UTC); proceeding immediately...",
                    format_utc_hms(current_window_start_ms)
                );
            }
            let desc = discovery
                .discover_active_5m_btc(adjusted_now_ms, min_remaining_ms)
                .await?;
            Ok(GateOutcome::Ready(FreshWindowDiscovery {
                discovery,
                offset_est,
                sync_report,
                desc,
            }))
        }
        WindowGatePlan::WaitUntil {
            elapsed_in_window_ms,
            wait_ms,
            next_window_start_ms,
        } => {
            let elapsed_s = elapsed_in_window_ms as f64 / 1_000.0;
            let wait_s = wait_ms as f64 / 1_000.0;
            let target_utc = format_utc_hms(next_window_start_ms);
            println!(
                "  [WAIT] Mid-window ({elapsed_s:.1}s into current 5m window); waiting {wait_s:.1}s until next fresh boundary {target_utc} UTC... (Ctrl+C to cancel)"
            );

            let ctrl_c = tokio::signal::ctrl_c();
            tokio::pin!(ctrl_c);

            loop {
                let current_adj = offset_est.adjusted_unix_ms(MonotonicClock::unix_now_ms()?)?;
                if current_adj >= next_window_start_ms {
                    break;
                }
                let remaining_ms = next_window_start_ms.saturating_sub(current_adj);
                tokio::select! {
                    _ = &mut ctrl_c => {
                        println!("  [WAIT] Ctrl+C received; cancelled before window open.");
                        return Ok(GateOutcome::Cancelled);
                    }
                    _ = tokio::time::sleep(Duration::from_millis(remaining_ms)) => {}
                }
            }

            println!(
                "  [WAIT] Fresh 5m window boundary reached ({target_utc} UTC); discovering market..."
            );

            if let Ok(refreshed) = discovery.sync_clock(&offset_est).await {
                sync_report = refreshed;
            }

            let mut last_reason = String::from("no active 5m BTC market returned");
            let mut printed_stale_notice = false;

            for attempt in 0..GAMMA_BOUNDARY_RETRY_ATTEMPTS {
                let now_ms = offset_est.adjusted_unix_ms(MonotonicClock::unix_now_ms()?)?;
                let discover_res = tokio::select! {
                    _ = &mut ctrl_c => {
                        println!("  [WAIT] Ctrl+C received; cancelled during boundary discovery.");
                        return Ok(GateOutcome::Cancelled);
                    }
                    res = discovery.discover_active_5m_btc(now_ms, min_remaining_ms) => res,
                };

                match discover_res {
                    Ok(desc) if descriptor_matches_window(&desc, next_window_start_ms) => {
                        return Ok(GateOutcome::Ready(FreshWindowDiscovery {
                            discovery,
                            offset_est,
                            sync_report,
                            desc,
                        }));
                    }
                    Ok(stale) => {
                        if !printed_stale_notice {
                            println!(
                                "  [WAIT] Gamma still serving prior window ({}); retrying for {target_utc} UTC...",
                                stale.slug
                            );
                            printed_stale_notice = true;
                        }
                        last_reason = format!(
                            "Gamma still serving prior window {} (start={})",
                            stale.slug,
                            format_utc_hms(stale.start_ts_ms)
                        );
                    }
                    Err(err) => {
                        last_reason = err.to_string();
                    }
                }

                if attempt + 1 < GAMMA_BOUNDARY_RETRY_ATTEMPTS {
                    tokio::select! {
                        _ = &mut ctrl_c => {
                            println!("  [WAIT] Ctrl+C received; cancelled during boundary retry.");
                            return Ok(GateOutcome::Cancelled);
                        }
                        _ = tokio::time::sleep(GAMMA_BOUNDARY_RETRY_INTERVAL) => {}
                    }
                }
            }

            Err(format!(
                "timed out waiting for fresh 5m BTC market at {target_utc} UTC: {last_reason}"
            )
            .into())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use core_types::{FeeSchedule, MicroUsdc, ProtocolVersion, SharesMicro};

    fn test_descriptor(start_ts_ms: u64, expiry_ts_ms: u64) -> MarketDescriptor {
        MarketDescriptor {
            condition_id: "cond_test".to_string(),
            slug: format!("btc-updown-5m-{}", start_ts_ms / 1_000),
            question: "Bitcoin Up or Down - 5m".to_string(),
            version: ProtocolVersion::PolyV2,
            neg_risk: false,
            asset_id_up: "111".to_string(),
            asset_id_dn: "222".to_string(),
            tick_size_micro: MicroUsdc(10_000),
            min_order_size_micro: SharesMicro(5_000_000),
            taker_delay_ms: 50,
            fee_schedule: FeeSchedule::btc_5m_default(),
            start_ts_ms,
            expiry_ts_ms,
            strike_price_usd: None,
        }
    }

    #[test]
    fn fresh_window_gate_boundary_calculations() {
        // Aligned 2026-11-22T11:00:00Z = 1_795_345_200_000 ms (divisible by 300_000).
        let boundary = 1_795_345_200_000_u64;
        assert_eq!(boundary % WINDOW_MS, 0);

        // t = 0ms -> proceed immediately
        assert_eq!(
            plan_fresh_window_gate(boundary, false),
            WindowGatePlan::ProceedImmediately {
                current_window_start_ms: boundary,
            }
        );

        // t = 500ms -> within 1_000ms grace period -> proceed immediately
        assert_eq!(
            plan_fresh_window_gate(boundary + 500, false),
            WindowGatePlan::ProceedImmediately {
                current_window_start_ms: boundary,
            }
        );

        // t = 1_000ms -> inclusive grace boundary -> proceed immediately
        assert_eq!(
            plan_fresh_window_gate(boundary + 1_000, false),
            WindowGatePlan::ProceedImmediately {
                current_window_start_ms: boundary,
            }
        );

        // t = 1_500ms -> mid-window -> wait 298_500ms for next boundary
        assert_eq!(
            plan_fresh_window_gate(boundary + 1_500, false),
            WindowGatePlan::WaitUntil {
                elapsed_in_window_ms: 1_500,
                wait_ms: 298_500,
                next_window_start_ms: boundary + WINDOW_MS,
            }
        );

        // t = 299_500ms -> mid-window -> wait 500ms for next boundary
        assert_eq!(
            plan_fresh_window_gate(boundary + 299_500, false),
            WindowGatePlan::WaitUntil {
                elapsed_in_window_ms: 299_500,
                wait_ms: 500,
                next_window_start_ms: boundary + WINDOW_MS,
            }
        );

        // allow_mid_window = true bypasses wait even in the middle of an epoch
        assert_eq!(
            plan_fresh_window_gate(boundary + 150_000, true),
            WindowGatePlan::ProceedImmediately {
                current_window_start_ms: boundary,
            }
        );
    }

    #[test]
    fn descriptor_window_verification_checks_start_ts() {
        let target_start = 1_795_345_500_000_u64;
        let prior = test_descriptor(target_start - WINDOW_MS, target_start);
        let exact = test_descriptor(target_start, target_start + WINDOW_MS);
        let later = test_descriptor(target_start + WINDOW_MS, target_start + 2 * WINDOW_MS);

        assert!(!descriptor_matches_window(&prior, target_start));
        assert!(descriptor_matches_window(&exact, target_start));
        assert!(descriptor_matches_window(&later, target_start));
    }

    #[test]
    fn format_utc_hms_formats_known_timestamp() {
        assert_eq!(format_utc_hms(1_795_345_200_000), "11:00:00Z");
        assert_eq!(format_utc_hms(1_795_345_500_000), "11:05:00Z");
    }
}
