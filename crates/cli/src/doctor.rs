use std::error::Error;
use std::hint::black_box;
use std::path::Path;

use core_types::{
    ClockOffsetEstimator, EngineConfig, ExecutionMode, FeeSchedule, HighResTimerGuard, JsonlWriter,
    LatencyTracker, MicroUsdc, MonotonicClock, SharesMicro, TickPrecision, WireAmounts,
    encode_buy_amounts, encode_sell_amounts, floor_to_tick, parse_decimal_micro,
};
use serde::Serialize;

use crate::theme::{self, Badge};

#[derive(Serialize)]
struct DoctorEnvRecord<'a> {
    execution_mode: &'a str,
    dry_run: bool,
    wallet_mode: u8,
    qpc_hz: u64,
    qpc_resolution_ns: f64,
    timer_1ms_active: bool,
    drift_threshold_ms: i64,
    gamma: f64,
    v_max_micro: u64,
    base_shares_micro: u64,
    min_combined_spread_micro: u64,
    max_combined_bid_micro: u64,
    polybolt_enabled: bool,
    has_clob_credentials: bool,
}

#[derive(Serialize)]
struct WireCheckRecord<'a> {
    label: &'a str,
    side: &'a str,
    price_micro: u64,
    shares_micro: u64,
    maker_amount: u64,
    taker_amount: u64,
}

#[derive(Serialize)]
struct FeeCheckRecord {
    fee_rate_e6: u64,
    price_micro: u64,
    shares_micro: u64,
    taker_fee_micro: u64,
    maker_fee_micro: u64,
}

struct WireSamples {
    buy_52: WireAmounts,
    sell_52: WireAmounts,
    buy_29: WireAmounts,
}

struct ClockSelfTest {
    unsynced_trip: bool,
    loopback_offset: i64,
    loopback_trip: bool,
    synthetic_drift: i64,
    overdrift_trip: bool,
}

pub fn run(jsonl_path: &Path) -> Result<(), Box<dyn Error>> {
    let t = theme::current();
    let clock = MonotonicClock::start();
    let timer_guard = HighResTimerGuard::acquire_1ms()?;
    let cfg = EngineConfig::from_env()?;
    let mut jsonl = JsonlWriter::create(jsonl_path)?;

    println!();
    println!(
        "{}",
        t.banner(
            "POLY-AS DOCTOR :: ENVIRONMENT, CLOCK & FIXED-POINT AUDIT",
            None
        )
    );

    print_and_log_env(&clock, &timer_guard, &cfg, &mut jsonl)?;
    print_input_vectors();
    let wire = print_and_log_wire_checks(&clock, &mut jsonl)?;
    print_and_log_fee_checks(&clock, &mut jsonl)?;
    verify_and_print_decisions(&wire)?;
    run_latency_benchmark(&clock, &mut jsonl)?;

    jsonl.close()?;
    println!(
        "{}",
        t.kv(
            "JSONL Telemetry:",
            &format!(
                "{} written, flushed, and closed cleanly at {}",
                t.badge(Badge::Pass),
                jsonl_path.display()
            )
        )
    );
    println!("{}", t.divider());
    Ok(())
}

fn print_and_log_env(
    clock: &MonotonicClock,
    timer_guard: &HighResTimerGuard,
    cfg: &EngineConfig,
    jsonl: &mut JsonlWriter,
) -> Result<(), Box<dyn Error>> {
    let t = theme::current();
    let (qpc_hz, qpc_res_ns) = MonotonicClock::qpc_resolution()?;
    let now_ms = MonotonicClock::unix_now_ms()?;
    let mono_ns = clock.now_ns()?;
    let self_test = run_clock_self_test(now_ms, cfg.clock_drift_max_ms)?;

    println!();
    println!(
        "{}",
        t.section("(a) DATA SOURCE, CONFIG & WINDOWS CLOCK STATUS")
    );
    println!(
        "{}",
        t.kv(
            "OS Timer Guard:",
            &format!(
                "active={} period={}ms (Win32 timeBeginPeriod)",
                timer_guard.is_active(),
                timer_guard.period_ms()
            )
        )
    );
    println!(
        "{}",
        t.kv(
            "QPC Clock:",
            &format!("freq={qpc_hz} Hz | resolution={qpc_res_ns:.2} ns/tick")
        )
    );
    println!(
        "{}",
        t.kv(
            "Clock Self-Test:",
            &format!(
                "unsynced_latched={} | loopback_offset={}ms tripped={} | synthetic(+{}ms)_tripped={} (max={}ms)",
                self_test.unsynced_trip,
                self_test.loopback_offset,
                self_test.loopback_trip,
                self_test.synthetic_drift,
                self_test.overdrift_trip,
                cfg.clock_drift_max_ms
            )
        )
    );
    print_config_summary(cfg);
    write_doctor_env_event(now_ms, mono_ns, qpc_hz, qpc_res_ns, timer_guard, cfg, jsonl)
}

fn print_config_summary(cfg: &EngineConfig) {
    let t = theme::current();
    println!(
        "{}",
        t.kv(
            "Execution Mode:",
            &format!("{:?} (dry_run={})", cfg.execution_mode, cfg.dry_run)
        )
    );
    println!(
        "{}",
        t.kv(
            "Wallet Signature:",
            &format!(
                "{:?} (signatureType={})",
                cfg.wallet_mode,
                cfg.wallet_mode.as_u8()
            )
        )
    );
    println!(
        "{}",
        t.kv(
            "Locked Math/Risk:",
            &format!(
                "gamma={:.2} | V_max=${:.2} | base_size={} shares | min_spread=${:.2} | max_pair_bid=${:.2}",
                cfg.gamma,
                cfg.max_notional_per_leg_micro.to_usd_f64(),
                cfg.base_order_size_shares.to_shares_f64(),
                cfg.min_combined_spread_micro.to_usd_f64(),
                cfg.max_combined_bid_micro.to_usd_f64(),
            )
        )
    );
    println!(
        "{}",
        t.kv(
            "Pipeline Toggles:",
            &format!(
                "variance={:?} | kappa={:?} | sg={:?} | include_var={} | polybolt={}",
                cfg.variance_mode,
                cfg.kappa_mode,
                cfg.sg_mode,
                cfg.include_variance_spread,
                cfg.polybolt_enabled,
            )
        )
    );
    println!(
        "{}",
        t.kv("Redacted Credentials:", &format!("{:?}", cfg.credentials))
    );
}

fn write_doctor_env_event(
    now_ms: u64,
    mono_ns: u64,
    qpc_hz: u64,
    qpc_res_ns: f64,
    timer_guard: &HighResTimerGuard,
    cfg: &EngineConfig,
    jsonl: &mut JsonlWriter,
) -> Result<(), Box<dyn Error>> {
    let exec_mode_str = match cfg.execution_mode {
        ExecutionMode::Paper => "paper",
        ExecutionMode::DryRun => "dry-run",
        ExecutionMode::CanaryLive => "canary-live",
    };
    jsonl.write_event(
        now_ms,
        mono_ns,
        "doctor_env",
        &DoctorEnvRecord {
            execution_mode: exec_mode_str,
            dry_run: cfg.dry_run,
            wallet_mode: cfg.wallet_mode.as_u8(),
            qpc_hz,
            qpc_resolution_ns: qpc_res_ns,
            timer_1ms_active: timer_guard.is_active(),
            drift_threshold_ms: cfg.clock_drift_max_ms,
            gamma: cfg.gamma,
            v_max_micro: cfg.max_notional_per_leg_micro.as_u64(),
            base_shares_micro: cfg.base_order_size_shares.as_u64(),
            min_combined_spread_micro: cfg.min_combined_spread_micro.as_u64(),
            max_combined_bid_micro: cfg.max_combined_bid_micro.as_u64(),
            polybolt_enabled: cfg.polybolt_enabled,
            has_clob_credentials: cfg.credentials.has_clob_credentials(),
        },
    )?;
    Ok(())
}

fn run_clock_self_test(now_ms: u64, max_drift_ms: i64) -> Result<ClockSelfTest, Box<dyn Error>> {
    let est = ClockOffsetEstimator::new(max_drift_ms);
    let unsynced_trip = est.is_drift_exceeded();
    ensure_check(
        unsynced_trip,
        "ClockOffsetEstimator failed to latch when unsynced",
    )?;

    let loopback_offset = est.record_sample(now_ms, now_ms)?;
    let loopback_trip = est.is_drift_exceeded();
    ensure_check(
        !loopback_trip && loopback_offset == 0,
        "ClockOffsetEstimator loopback 0ms self-test failed",
    )?;

    let synthetic_drift = max_drift_ms.saturating_add(250).max(1_250);
    let synthetic_exch = now_ms.saturating_add(synthetic_drift.unsigned_abs());
    let _ = est.record_sample(now_ms, synthetic_exch)?;
    let overdrift_trip = est.is_drift_exceeded();
    let msg =
        format!("ClockOffsetEstimator failed to trip on synthetic +{synthetic_drift}ms drift");
    ensure_check(overdrift_trip, &msg)?;
    Ok(ClockSelfTest {
        unsynced_trip,
        loopback_offset,
        loopback_trip,
        synthetic_drift,
        overdrift_trip,
    })
}

fn print_input_vectors() {
    let t = theme::current();
    println!();
    println!("{}", t.section("(b) EXACT INPUT VECTORS"));
    println!(
        "{}",
        t.kv(
            "Wire Sample 1:",
            "BUY/SELL 10.00 shares @ $0.52 (tick=0.01)"
        )
    );
    println!(
        "{}",
        t.kv(
            "Wire Sample 2:",
            "BUY 10.00 shares @ $0.29 (IEEE-754 float trap check)"
        )
    );
    println!(
        "{}",
        t.kv(
            "Fee Schedule Curves:",
            "100 shares @ feeRate=0.05 (current) and feeRate=0.07 (legacy)"
        )
    );
    println!(
        "{}",
        t.kv(
            "Tick & Diff Inputs:",
            "floor_to_tick(0.4999, 0.01) | abs_diff($0.52, $0.51)"
        )
    );
}

fn print_and_log_wire_checks(
    clock: &MonotonicClock,
    jsonl: &mut JsonlWriter,
) -> Result<WireSamples, Box<dyn Error>> {
    let t = theme::current();
    let p_52 = MicroUsdc::from_usd_str("0.52")?;
    let p_29 = MicroUsdc::from_usd_str("0.29")?;
    let s_10 = SharesMicro::from_shares_str("10.00")?;
    let wire = WireSamples {
        buy_52: encode_buy_amounts(p_52, s_10, TickPrecision::Cent)?,
        sell_52: encode_sell_amounts(p_52, s_10, TickPrecision::Cent)?,
        buy_29: encode_buy_amounts(p_29, s_10, TickPrecision::Cent)?,
    };

    println!();
    println!(
        "{}",
        t.section("(c) STEP-BY-STEP WIRE ENCODING & DYNAMIC FEE CURVE")
    );
    println!(
        "  {} parse(\"0.52\")={} micro | parse(\"10.00\")={} micro -> notional=({}*{} )/1e6 = {}",
        t.muted("[1]"),
        p_52.as_u64(),
        s_10.as_u64(),
        p_52.as_u64(),
        s_10.as_u64(),
        wire.buy_52.maker_amount
    );
    println!(
        "  {} BUY  10 @ $0.52  -> makerAmount=\"{}\" (USD)    | takerAmount=\"{}\" (shares)",
        t.muted("[2]"),
        wire.buy_52.maker_amount,
        wire.buy_52.taker_amount
    );
    println!(
        "  {} SELL 10 @ $0.52  -> makerAmount=\"{}\" (shares) | takerAmount=\"{}\" (USD)",
        t.muted("[3]"),
        wire.sell_52.maker_amount,
        wire.sell_52.taker_amount
    );
    println!(
        "  {} BUY  10 @ $0.29  -> makerAmount=\"{}\" (exact; avoids 2899999 float drift)",
        t.muted("[4]"),
        wire.buy_29.maker_amount
    );
    log_wire_events(clock, &wire, jsonl)?;
    Ok(wire)
}

fn log_wire_events(
    clock: &MonotonicClock,
    wire: &WireSamples,
    jsonl: &mut JsonlWriter,
) -> Result<(), Box<dyn Error>> {
    for (label, sample) in [
        ("buy_10_at_0.52", wire.buy_52),
        ("sell_10_at_0.52", wire.sell_52),
        ("buy_10_at_0.29", wire.buy_29),
    ] {
        jsonl.write_event(
            MonotonicClock::unix_now_ms()?,
            clock.now_ns()?,
            "wire_check",
            &WireCheckRecord {
                label,
                side: sample.side.as_str(),
                price_micro: sample.price_micro.as_u64(),
                shares_micro: sample.rounded_shares_micro.as_u64(),
                maker_amount: sample.maker_amount,
                taker_amount: sample.taker_amount,
            },
        )?;
    }
    Ok(())
}

fn print_and_log_fee_checks(
    clock: &MonotonicClock,
    jsonl: &mut JsonlWriter,
) -> Result<(), Box<dyn Error>> {
    let s_100 = SharesMicro::from_whole_shares(100)?;
    let sched_05 = FeeSchedule::btc_5m_default();
    let sched_07 = FeeSchedule::btc_5m_legacy();

    println!("  [5] Taker Fee Curve (100 shares = shares * feeRate * p * (1 - p)):");
    for p_raw in [10_000_u64, 100_000, 500_000, 900_000, 990_000] {
        let p = MicroUsdc(p_raw);
        let fee_05 = sched_05.compute_taker_fee_micro(s_100, p)?;
        let fee_07 = sched_07.compute_taker_fee_micro(s_100, p)?;
        let maker_05 = sched_05.compute_maker_fee_micro(s_100, p)?;
        let maker_07 = sched_07.compute_maker_fee_micro(s_100, p)?;
        println!(
            "      p=${:.2} -> fee(0.05)=${} ({} micro) | fee(0.07)=${} ({} micro) | maker=${}",
            p.to_usd_f64(),
            fee_05,
            fee_05.as_u64(),
            fee_07,
            fee_07.as_u64(),
            maker_05
        );
        log_fee_pair(
            clock,
            jsonl,
            p,
            s_100,
            (sched_05.fee_rate_e6, fee_05, maker_05),
        )?;
        log_fee_pair(
            clock,
            jsonl,
            p,
            s_100,
            (sched_07.fee_rate_e6, fee_07, maker_07),
        )?;
    }
    Ok(())
}

fn log_fee_pair(
    clock: &MonotonicClock,
    jsonl: &mut JsonlWriter,
    price: MicroUsdc,
    shares: SharesMicro,
    (fee_rate_e6, taker, maker): (u64, MicroUsdc, MicroUsdc),
) -> Result<(), Box<dyn Error>> {
    jsonl.write_event(
        MonotonicClock::unix_now_ms()?,
        clock.now_ns()?,
        "fee_check",
        &FeeCheckRecord {
            fee_rate_e6,
            price_micro: price.as_u64(),
            shares_micro: shares.as_u64(),
            taker_fee_micro: taker.as_u64(),
            maker_fee_micro: maker.as_u64(),
        },
    )?;
    Ok(())
}

fn verify_and_print_decisions(wire: &WireSamples) -> Result<(), Box<dyn Error>> {
    let s_100 = SharesMicro::from_whole_shares(100)?;
    let fee_05_50 =
        FeeSchedule::btc_5m_default().compute_taker_fee_micro(s_100, MicroUsdc(500_000))?;
    let fee_07_50 =
        FeeSchedule::btc_5m_legacy().compute_taker_fee_micro(s_100, MicroUsdc(500_000))?;
    let fee_maker_50 =
        FeeSchedule::btc_5m_default().compute_maker_fee_micro(s_100, MicroUsdc(500_000))?;
    let floored_4999 = floor_to_tick(0.4999, MicroUsdc(10_000))?;
    let diff_52_51 = MicroUsdc(520_000).abs_diff(MicroUsdc(510_000));

    assert_decision_invariants(
        wire,
        fee_05_50,
        fee_07_50,
        fee_maker_50,
        floored_4999,
        diff_52_51,
    )?;

    let t = theme::current();
    println!();
    println!("{}", t.section("(d) EXACT OUTPUTS & INVARIANT DECISIONS"));
    println!(
        "{}",
        t.kv(
            "BUY 10 @ $0.52:",
            &format!(
                "makerAmount=\"{}\" takerAmount=\"{}\" {}",
                wire.buy_52.maker_amount,
                wire.buy_52.taker_amount,
                t.badge(Badge::Pass)
            )
        )
    );
    println!(
        "{}",
        t.kv(
            "SELL 10 @ $0.52:",
            &format!(
                "makerAmount=\"{}\" takerAmount=\"{}\" {}",
                wire.sell_52.maker_amount,
                wire.sell_52.taker_amount,
                t.badge(Badge::Pass)
            )
        )
    );
    println!(
        "{}",
        t.kv(
            "BUY 10 @ $0.29:",
            &format!(
                "makerAmount=\"{}\" takerAmount=\"{}\" {}",
                wire.buy_29.maker_amount,
                wire.buy_29.taker_amount,
                t.badge(Badge::Pass)
            )
        )
    );
    println!(
        "{}",
        t.kv(
            "Fee 100 @ $0.50:",
            &format!(
                "fee(0.05)=${fee_05_50} | fee(0.07)=${fee_07_50} | maker=${fee_maker_50} {}",
                t.badge(Badge::Pass)
            )
        )
    );
    println!(
        "{}",
        t.kv(
            "Tick & Diff Guards:",
            &format!(
                "floor(0.4999)=${floored_4999} ({}) | abs_diff($0.52,$0.51)={} >= 10000 {}",
                floored_4999.as_u64(),
                diff_52_51.as_u64(),
                t.badge(Badge::Pass)
            )
        )
    );
    Ok(())
}

fn assert_decision_invariants(
    wire: &WireSamples,
    fee_05_50: MicroUsdc,
    fee_07_50: MicroUsdc,
    fee_maker_50: MicroUsdc,
    floored_4999: MicroUsdc,
    diff_52_51: MicroUsdc,
) -> Result<(), Box<dyn Error>> {
    ensure_check(
        wire.buy_52.maker_amount == 5_200_000 && wire.buy_52.taker_amount == 10_000_000,
        "BUY 10 @ $0.52 wire invariant failed",
    )?;
    ensure_check(
        wire.sell_52.maker_amount == 10_000_000 && wire.sell_52.taker_amount == 5_200_000,
        "SELL 10 @ $0.52 wire invariant failed",
    )?;
    ensure_check(
        wire.buy_29.maker_amount == 2_900_000 && wire.buy_29.taker_amount == 10_000_000,
        "BUY 10 @ $0.29 exact wire invariant failed",
    )?;
    ensure_check(
        fee_05_50 == MicroUsdc(1_250_000)
            && fee_07_50 == MicroUsdc(1_750_000)
            && fee_maker_50 == MicroUsdc::ZERO,
        "50-cent taker/maker fee invariant failed",
    )?;
    ensure_check(
        floored_4999 == MicroUsdc(490_000) && diff_52_51 == MicroUsdc(10_000),
        "tick floor or hysteresis diff invariant failed",
    )?;
    Ok(())
}

fn run_latency_benchmark(
    clock: &MonotonicClock,
    jsonl: &mut JsonlWriter,
) -> Result<(), Box<dyn Error>> {
    let t = theme::current();
    println!();
    println!(
        "{}",
        t.section("(e) COMPUTE LATENCY BENCHMARK (1,000 FIXED-POINT + FEE + TICK CYCLES)")
    );
    let summary = measure_latency_cycles(clock)?;
    println!(
        "{}",
        t.kv(
            "Samples:",
            &format!(
                "{} (0ns_clamped={}) | min={} ns ({:.3} us)",
                summary.count,
                summary.zero_ns_samples,
                summary.min_ns,
                summary.min_ns as f64 / 1_000.0
            )
        )
    );
    println!(
        "{}",
        t.kv(
            "Percentiles:",
            &format!(
                "P50={} ns ({:.3} us) | P95={} ns ({:.3} us) | P99={} ns ({:.3} us) | P99.9={} ns ({:.3} us) | max={} ns ({:.3} us)",
                summary.p50_ns,
                summary.p50_ns as f64 / 1_000.0,
                summary.p95_ns,
                summary.p95_ns as f64 / 1_000.0,
                summary.p99_ns,
                summary.p99_ns as f64 / 1_000.0,
                summary.p999_ns,
                summary.p999_ns as f64 / 1_000.0,
                summary.max_ns,
                summary.max_ns as f64 / 1_000.0
            )
        )
    );
    jsonl.write_event(
        MonotonicClock::unix_now_ms()?,
        clock.now_ns()?,
        "latency_summary",
        &summary,
    )?;
    Ok(())
}

fn measure_latency_cycles(
    clock: &MonotonicClock,
) -> Result<core_types::LatencySummary, Box<dyn Error>> {
    let s_100 = SharesMicro::from_whole_shares(100)?;
    let sched_05 = FeeSchedule::btc_5m_default();
    let mut tracker = LatencyTracker::new()?;

    for idx in 0_u64..1_000 {
        let t0 = clock.now_ns()?;
        let p_bytes = if idx % 2 == 0 { b"0.52" } else { b"0.29" };
        let parsed_p = MicroUsdc(parse_decimal_micro(black_box(p_bytes))?);
        let parsed_s = SharesMicro(parse_decimal_micro(black_box(b"10.00"))?);
        let wire = encode_buy_amounts(
            black_box(parsed_p),
            black_box(parsed_s),
            TickPrecision::Cent,
        )?;
        let fee = sched_05.compute_taker_fee_micro(black_box(s_100), black_box(parsed_p))?;
        let snapped = floor_to_tick(black_box(0.4999), MicroUsdc(10_000))?;
        black_box((wire, fee, snapped));
        let dt_ns = clock.now_ns()?.saturating_sub(t0);
        tracker.record_ns(dt_ns)?;
    }
    Ok(tracker.summary())
}

fn ensure_check(cond: bool, msg: &str) -> Result<(), Box<dyn Error>> {
    if cond { Ok(()) } else { Err(msg.into()) }
}

#[cfg(test)]
mod tests {
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::*;

    #[test]
    fn doctor_run_executes_and_writes_complete_jsonl_audit() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("poly_as_doctor_test_{unique}.jsonl"));

        run(&path).unwrap();

        let contents = std::fs::read_to_string(&path).unwrap();
        let lines: Vec<serde_json::Value> = contents
            .lines()
            .map(|l| serde_json::from_str(l).unwrap())
            .collect();
        let fee_rates: Vec<u64> = lines
            .iter()
            .filter(|v| v["event"] == "fee_check")
            .map(|v| v["payload"]["fee_rate_e6"].as_u64().unwrap())
            .collect();

        assert!(lines.iter().any(|v| v["event"] == "doctor_env"));
        assert!(lines.iter().any(|v| v["event"] == "wire_check"));
        assert!(fee_rates.contains(&50_000));
        assert!(fee_rates.contains(&70_000));
        assert!(lines.iter().any(|v| v["event"] == "latency_summary"));

        let _ = std::fs::remove_file(&path);
    }
}
