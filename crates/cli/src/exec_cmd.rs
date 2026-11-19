use std::error::Error;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use core_types::{
    BookLevel, FeeSchedule, FixedBook, HighResTimerGuard, JsonlWriter, LatencyTracker,
    MarketDescriptor, MicroUsdc, MonotonicClock, Outcome, ProtocolVersion, SharesMicro,
    SignedSharesMicro, TickPrecision, WalletMode, encode_buy_amounts,
};
use execution::{
    L2HmacSigner, MarketDomain, OrderSigner, SettlementConfig, SettlementWorker, decide_settlement,
};
use math_engine::{MathPipeline, MathPipelineConfig, PipelineInput, QuoteOutput};
use secrecy::SecretString;

use crate::exec_report::{
    ExecReportStats, print_exec_header, print_exec_summary, record_latency,
    record_settlement_preview, record_signed_order, record_summary_jsonl,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ExecMode {
    DryRun,
    CanaryLive,
}

pub(crate) struct ExecArgs {
    pub(crate) mode: ExecMode,
    pub(crate) confirm_live_capital: bool,
    pub(crate) canary_max_usd: u64,
    pub(crate) duration: String,
    pub(crate) jsonl: PathBuf,
}

pub(crate) fn parse_duration_secs(raw: &str) -> Result<u64, Box<dyn Error>> {
    let text = raw.trim().to_ascii_lowercase();
    if let Some(stripped) = text.strip_suffix("ms") {
        let value: u64 = stripped
            .trim()
            .parse()
            .map_err(|_| format!("invalid --duration '{raw}'"))?;
        return Ok(value.div_ceil(1000).max(1));
    }
    if let Some(stripped) = text.strip_suffix('s') {
        let value: u64 = stripped
            .trim()
            .parse()
            .map_err(|_| format!("invalid --duration '{raw}'"))?;
        if value == 0 {
            return Err(format!("--duration '{raw}' must be >= 1s").into());
        }
        return Ok(value);
    }
    if let Some(stripped) = text.strip_suffix('m') {
        let value: u64 = stripped
            .trim()
            .parse()
            .map_err(|_| format!("invalid --duration '{raw}'"))?;
        return Ok(value.saturating_mul(60).max(1));
    }
    Err(format!("invalid --duration '{raw}' (try 60s, 500ms, 1m)").into())
}

fn unix_ms_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(1_700_000_000_000)
}

fn synth_market() -> MarketDescriptor {
    MarketDescriptor {
        condition_id: "0x0000000000000000000000000000000000000000000000000000000000000000"
            .to_string(),
        slug: "btc-updown-5m-synth".to_string(),
        question: "Will BTC go Up in the next 5 minutes? (synthetic dry-run)".to_string(),
        version: ProtocolVersion::PolyV2,
        neg_risk: false,
        asset_id_up: "101010101010101010101010101010101010101010101010101010101010101".to_string(),
        asset_id_dn: "202020202020202020202020202020202020202020202020202020202020202".to_string(),
        tick_size_micro: MicroUsdc::from_raw(10_000),
        min_order_size_micro: SharesMicro::from_raw(5_000_000),
        taker_delay_ms: 50,
        fee_schedule: FeeSchedule::btc_5m_default(),
        start_ts_ms: 1_700_000_000_000,
        expiry_ts_ms: 1_700_000_300_000,
        strike_price_usd: None,
    }
}

fn synth_book(outcome: Outcome, base_bid_micro: u64, now_ns: u64, exch_ms: u64) -> FixedBook<10> {
    let b1 = BookLevel {
        price: MicroUsdc::from_raw(base_bid_micro),
        size: SharesMicro::from_raw(20_000_000),
    };
    let b2 = BookLevel {
        price: MicroUsdc::from_raw(base_bid_micro.saturating_sub(10_000)),
        size: SharesMicro::from_raw(30_000_000),
    };
    let a1 = BookLevel {
        price: MicroUsdc::from_raw(base_bid_micro.saturating_add(20_000)),
        size: SharesMicro::from_raw(20_000_000),
    };
    let a2 = BookLevel {
        price: MicroUsdc::from_raw(base_bid_micro.saturating_add(30_000)),
        size: SharesMicro::from_raw(30_000_000),
    };
    FixedBook::try_from_slices(outcome, &[b1, b2], &[a1, a2], now_ns, exch_ms)
        .unwrap_or_else(|_| FixedBook::empty(outcome))
}

fn check_canary_latch(args: &ExecArgs) -> Result<(), Box<dyn Error>> {
    if args.mode != ExecMode::CanaryLive {
        return Ok(());
    }
    if !args.confirm_live_capital {
        return Err(
            "refusing canary-live: pass --confirm-live-capital to acknowledge real capital at risk"
                .into(),
        );
    }
    if args.canary_max_usd > 25 {
        return Err(format!(
            "refusing canary-live: --canary-max-usd {} exceeds $25 safety cap",
            args.canary_max_usd
        )
        .into());
    }
    let dry = std::env::var("DRY_RUN").unwrap_or_else(|_| "true".to_string());
    if !dry.trim().eq_ignore_ascii_case("false") {
        return Err(
            "refusing canary-live: DRY_RUN != false (set DRY_RUN=false to arm live capital)".into(),
        );
    }
    Err("canary-live dispatch is gated: dry-run verification only in this build".into())
}

pub(crate) fn run(args: ExecArgs) -> Result<(), Box<dyn Error>> {
    check_canary_latch(&args)?;
    if args.mode == ExecMode::CanaryLive {
        return Err("canary-live refused without operator sign-off".into());
    }
    let _timer_guard = HighResTimerGuard::acquire_1ms()?;
    let clock = MonotonicClock::start();
    let duration_secs = parse_duration_secs(&args.duration)?;

    print_exec_header("dry-run", &args.jsonl, args.canary_max_usd);

    let mut jsonl = JsonlWriter::create(&args.jsonl)?;
    let mut sign_lat = LatencyTracker::new()?;
    let mut compute_lat = LatencyTracker::new()?;
    let mut stats = ExecReportStats::default();

    let market = synth_market();
    let domain = MarketDomain::for_market(&market);

    println!("  (a) Market discovery (synthetic dry-run, zero capital at risk)");
    println!("      slug:             {}", market.slug);
    println!(
        "      version:          {:?} (domain v{})",
        market.version, domain.domain_version
    );
    println!("      verifying:        {:?}", domain.exchange_address);
    println!("      wallet_mode:      DepositWallet1271 (sig 3, ERC-7739 TypedDataSign)");
    println!(
        "      tick:             0.01  min_size: 5 shares  takerDelay: 50ms  feeRate: dynamic~0.05"
    );

    let test_privkey = SecretString::new(
        "0x4c0883a69102937d6231471b5dbb6204fe5129617082792ae468d01a3f362318".into(),
    );
    let order_signer = OrderSigner::new(
        &test_privkey,
        WalletMode::DepositWallet1271,
        Some(deposit_wallet_value()),
    )?;
    let l2_secret = SecretString::new("aGVsbG8td29ybGRfc2VjcmV0".into());
    let l2_signer = L2HmacSigner::new(
        "0xeE3B778A783510BC833384919F709e3d2fee1624",
        "dry-run-key",
        &l2_secret,
        SecretString::new("dry-run-pass".into()),
    )?;

    let mut pipeline = MathPipeline::new(MathPipelineConfig::try_default()?, clock)?;
    let tick_precision = TickPrecision::from_micro(market.tick_size_micro)?;
    let display_ticks = (duration_secs.clamp(1, 60) as usize).clamp(6, 12);

    for tick in 0..display_ticks {
        let tick_t0 = clock.now_ns()?;
        let ts_ms = unix_ms_now().saturating_add(tick as u64 * 100);
        let mono_ns = clock.now_ns()?;
        let drift = (tick as u64).saturating_mul(2_000);
        let upper = if drift.min(20_000) > 460_000 {
            440_000
        } else {
            460_000 - drift.min(20_000)
        };
        let book_up = synth_book(Outcome::Up, 520_000 + drift, mono_ns, ts_ms);
        let book_dn = synth_book(Outcome::Down, upper, mono_ns, ts_ms);

        let c0 = clock.now_ns()?;
        let input = PipelineInput {
            now_ms: ts_ms,
            expiry_ms: market.expiry_ts_ms,
            tick_size_micro: market.tick_size_micro,
            fee_schedule: market.fee_schedule,
            inventory_q: SignedSharesMicro::ZERO,
            book_up: &book_up,
            book_dn: &book_dn,
            spot_usd: Some(97_500.0),
            strike_usd: None,
            active_bid_up: None,
            active_bid_dn: None,
        };
        let trace = pipeline.evaluate(&input)?;
        let c1 = clock.now_ns()?;
        let tau2 = c1.saturating_sub(c0).max(1);
        compute_lat.record_ns(tau2)?;
        let tau1 = c0.saturating_sub(tick_t0).max(1);
        stats.ticks += 1;

        let (bid_up_opt, bid_dn_opt) = match trace.quote {
            QuoteOutput::Active { bid_up, bid_dn, .. } => (bid_up, bid_dn),
            QuoteOutput::Suppressed(_) => (None, None),
        };
        if bid_up_opt.is_some() && bid_dn_opt.is_some() {
            stats.active_quotes += 1;
        }

        let s_i = trace.sg.map(|s| s.s_i).unwrap_or(0.52);
        let v_rem = trace.station2.map(|s| s.v_rem).unwrap_or(0.0);
        let kappa = trace.kappa.map(|k| k.kappa).unwrap_or(50.0);
        println!(
            "  tick {tick:02}: s_i={s_i:.4} V_rem={v_rem:.5} kappa={kappa:.1} quote=({},{}) tau2={}us",
            bid_up_opt.map(|p| p.0).unwrap_or(0),
            bid_dn_opt.map(|p| p.0).unwrap_or(0),
            tau2 / 1000
        );

        if tick + 1 == display_ticks
            && let (Some(bid_up), Some(bid_dn)) = (bid_up_opt, bid_dn_opt)
        {
            sign_and_record(
                &order_signer,
                &domain,
                &l2_signer,
                &mut jsonl,
                &mut sign_lat,
                &mut stats,
                ts_ms,
                mono_ns,
                tick,
                bid_up,
                bid_dn,
                &market,
                tick_precision,
            )?;
        }

        record_latency(&mut jsonl, ts_ms, mono_ns, tick, tau1, tau2, 0, 1_200)?;
    }

    let worker = SettlementWorker::new(
        SettlementConfig::default(),
        market.clone(),
        "0xeE3B778A783510BC833384919F709e3d2fee1624".to_string(),
        None,
    );
    let preview_snap = execution::OrderRegistrySnapshot {
        confirmed_up: 6_000_000,
        confirmed_dn: 6_000_000,
        ..execution::OrderRegistrySnapshot::default()
    };
    let decision = decide_settlement(&preview_snap, SettlementConfig::default().min_batch_micro);
    let calldata = worker.preview_calldata(decision.amount_micro);
    let target_label = match market.version {
        ProtocolVersion::PolyV2 => "Router.merge",
        ProtocolVersion::CtfV1 => "CTF.mergePositions",
    };
    println!(
        "  (d) Settlement preview: {target_label} pairable={} decision={} ",
        decision.amount_micro, decision.reason
    );
    println!("      calldata: {calldata}");
    record_settlement_preview(
        &mut jsonl,
        unix_ms_now(),
        clock.now_ns()?,
        target_label,
        &calldata,
        decision.amount_micro,
        decision.reason,
    )?;

    let sign_summary = sign_lat.summary();
    let compute_summary = compute_lat.summary();
    print_exec_summary(&stats, &sign_summary, &calldata);
    println!(
        "      compute P50: {}us P99: {}us | signing samples: {}",
        compute_summary.p50_ns / 1000,
        compute_summary.p99_ns / 1000,
        sign_summary.count
    );
    record_summary_jsonl(
        &mut jsonl,
        unix_ms_now(),
        clock.now_ns()?,
        &stats,
        &sign_summary,
    )?;

    if cfg!(debug_assertions) {
        if sign_summary.p99_ns > 10_000_000 {
            return Err(format!(
                "tau_3 signing P99 {}us exceeds debug budget 10000us",
                sign_summary.p99_ns / 1000
            )
            .into());
        }
    } else if sign_summary.p99_ns > 80_000 {
        return Err(format!(
            "tau_3 signing P99 {}us exceeds 80us budget",
            sign_summary.p99_ns / 1000
        )
        .into());
    }
    if stats.active_quotes == 0 {
        return Err("no active quotes produced during dry-run".into());
    }
    Ok(())
}

fn deposit_wallet_value() -> alloy::primitives::Address {
    use std::str::FromStr;
    alloy::primitives::Address::from_str("0xeE3B778A783510BC833384919F709e3d2fee1624")
        .unwrap_or(alloy::primitives::Address::ZERO)
}

#[allow(clippy::too_many_arguments)]
fn sign_and_record(
    order_signer: &OrderSigner,
    domain: &MarketDomain,
    l2_signer: &L2HmacSigner,
    jsonl: &mut JsonlWriter,
    sign_lat: &mut LatencyTracker,
    stats: &mut ExecReportStats,
    ts_ms: u64,
    mono_ns: u64,
    tick: usize,
    bid_up: MicroUsdc,
    bid_dn: MicroUsdc,
    market: &MarketDescriptor,
    tick_precision: TickPrecision,
) -> Result<(), Box<dyn Error>> {
    use std::time::Instant;
    let legs = [
        ("UP", bid_up, market.asset_id_up.as_str()),
        ("DN", bid_dn, market.asset_id_dn.as_str()),
    ];
    for (leg, price, token_id) in legs {
        let wire = encode_buy_amounts(price, SharesMicro::from_raw(20_000_000), tick_precision)?;
        let t0 = Instant::now();
        let salt = execution::generate_salt();
        let order = order_signer.build_order(
            salt,
            token_id,
            wire.maker_amount,
            wire.taker_amount,
            true,
            ts_ms / 1000,
        )?;
        let sig = order_signer.sign_order(&order, domain)?;
        let elapsed_ns = u64::try_from(t0.elapsed().as_nanos()).unwrap_or(u64::MAX);
        sign_lat.record_ns(elapsed_ns.max(1))?;
        let recovered = order_signer.verify_signature(&order, &sig, domain)?;
        let recovered_match = recovered == order_signer.eoa_address();
        stats.signed_orders += 1;
        let l2 = l2_signer.sign_request_now("POST", "/order", Some("{\"dry\":true}"))?;
        println!(
            "  (b) {leg} bid={} maker={} taker={} sig={}.. recover={} L2key={}",
            price.0,
            wire.maker_amount,
            wire.taker_amount,
            sig.get(..18).unwrap_or(&sig),
            if recovered_match { "ok" } else { "MISMATCH" },
            l2.poly_api_key,
        );
        record_signed_order(
            jsonl,
            ts_ms,
            mono_ns,
            tick,
            leg,
            price.0,
            &wire.maker_amount.to_string(),
            &wire.taker_amount.to_string(),
            &sig,
            recovered_match,
        )?;
        if !recovered_match {
            return Err("EIP-712 recover self-check failed".into());
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canary_without_confirm_refuses() {
        let args = ExecArgs {
            mode: ExecMode::CanaryLive,
            confirm_live_capital: false,
            canary_max_usd: 10,
            duration: "5s".to_string(),
            jsonl: PathBuf::from("logs/test-exec.jsonl"),
        };
        assert!(check_canary_latch(&args).is_err());
    }

    #[test]
    fn duration_parses_units() {
        assert_eq!(parse_duration_secs("60s").unwrap_or(0), 60);
        assert_eq!(parse_duration_secs("500ms").unwrap_or(0), 1);
        assert_eq!(parse_duration_secs("1m").unwrap_or(0), 60);
        assert!(parse_duration_secs("bogus").is_err());
    }
}
