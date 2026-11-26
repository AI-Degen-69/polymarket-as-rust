#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::indexing_slicing
    )
)]

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};

use core_types::{
    BookLevel, FeeSchedule, FinitePosF64, FixedBook, JsonlWriter, MicroUsdc, ONE_SHARE_MICRO,
    OrderSide, Outcome, ProtocolVersion, SharesMicro,
};
use feeds::{BinanceSpotEvent, PolyMarketEvent};
use math_engine::QuoteOutput;
use paper_sim::PaperLedger;
use risk_lifecycle::{
    ActorPipelineHarness, BreakerConfig, CircuitBreakers, DollarNotionalRiskEngine,
    EpochLifecycleController, EpochPhase, ExecutionCommand, FaultInjection, FlattenAction,
    HysteresisConfig, HysteresisGate, LifecycleConfig, OrderRegistrySnapshot, QuoteProposal,
    RiskActor, RiskActorTrace, RiskConfig, RiskTier,
};
use serde_json::Value;

static TEMP_SEQ: AtomicU64 = AtomicU64::new(0);

fn temp_jsonl_path(tag: &str) -> PathBuf {
    let seq = TEMP_SEQ.fetch_add(1, Ordering::Relaxed);
    let pid = std::process::id();
    std::env::temp_dir().join(format!("poly_as_flatten_inv_{tag}_{pid}_{seq}.jsonl"))
}

fn lvl(cents: u64, whole_shares: u64) -> BookLevel {
    BookLevel {
        price: MicroUsdc(cents * 10_000),
        size: SharesMicro(whole_shares * ONE_SHARE_MICRO),
    }
}

fn make_book(
    outcome: Outcome,
    bids: &[BookLevel],
    asks: &[BookLevel],
    ts_ms: u64,
) -> FixedBook<10> {
    FixedBook::<10>::try_from_slices(outcome, bids, asks, ts_ms * 1_000_000, ts_ms).unwrap()
}

fn test_market(slug: &str, start_ms: u64, expiry_ms: u64) -> core_types::MarketDescriptor {
    core_types::MarketDescriptor {
        condition_id: format!("cond_{slug}"),
        slug: slug.to_string(),
        question: "BTC 5m flatten investigation".to_string(),
        version: ProtocolVersion::PolyV2,
        neg_risk: false,
        asset_id_up: format!("up_{slug}"),
        asset_id_dn: format!("dn_{slug}"),
        tick_size_micro: MicroUsdc(10_000),
        min_order_size_micro: SharesMicro(5 * ONE_SHARE_MICRO),
        taker_delay_ms: 50,
        fee_schedule: FeeSchedule::btc_5m_default(),
        start_ts_ms: start_ms,
        expiry_ts_ms: expiry_ms,
        strike_price_usd: Some(FinitePosF64::try_new(60_000.0).unwrap()),
    }
}

struct ReplayFixture {
    writer: JsonlWriter,
    start_ms: u64,
    base_mono_ns: u64,
}

impl ReplayFixture {
    fn create(path: &Path, slug: &str, start_ms: u64) -> Self {
        let mut writer = JsonlWriter::create(path).unwrap();
        let base_mono_ns = 1_000_000_000_u64;
        let desc = test_market(slug, start_ms, start_ms + 300_000);
        writer
            .write_event(start_ms, base_mono_ns, "market_discovered", &desc)
            .unwrap();
        Self {
            writer,
            start_ms,
            base_mono_ns,
        }
    }

    fn mono_at(&self, offset_ms: u64) -> (u64, u64) {
        (
            self.start_ms + offset_ms,
            self.base_mono_ns + offset_ms * 1_000_000,
        )
    }

    fn binance_spot(&mut self, offset_ms: u64, usd: f64, update_id: u64) {
        let (ts_ms, mono_ns) = self.mono_at(offset_ms);
        let e8 = (usd * 100_000_000.0).round() as u64;
        self.writer
            .write_event(
                ts_ms,
                mono_ns,
                "binance_tick",
                &BinanceSpotEvent::BookTicker {
                    bid_e8: e8,
                    ask_e8: e8 + 1_000_000,
                    bid_qty_e8: 100_000_000,
                    ask_qty_e8: 100_000_000,
                    update_id,
                    rx_mono_ns: mono_ns,
                },
            )
            .unwrap();
    }

    fn poly_book(
        &mut self,
        offset_ms: u64,
        outcome: Outcome,
        bids: &[BookLevel],
        asks: &[BookLevel],
    ) {
        let (ts_ms, mono_ns) = self.mono_at(offset_ms);
        let book = make_book(outcome, bids, asks, ts_ms);
        self.writer
            .write_event(
                ts_ms,
                mono_ns,
                "poly_book",
                &PolyMarketEvent::BookSnapshot {
                    outcome,
                    book,
                    rx_mono_ns: mono_ns,
                    exch_ts_ms: ts_ms,
                    epoch: 0,
                },
            )
            .unwrap();
    }

    fn poly_trade(
        &mut self,
        offset_ms: u64,
        outcome: Outcome,
        side: OrderSide,
        price_cents: u64,
        shares_micro: u64,
    ) {
        let (ts_ms, mono_ns) = self.mono_at(offset_ms);
        self.writer
            .write_event(
                ts_ms,
                mono_ns,
                "poly_trade",
                &PolyMarketEvent::LastTrade {
                    outcome,
                    side,
                    price: MicroUsdc(price_cents * 10_000),
                    size: SharesMicro(shares_micro),
                    rx_mono_ns: mono_ns,
                    exch_ts_ms: ts_ms,
                    epoch: 0,
                },
            )
            .unwrap();
    }

    fn close(self) {
        self.writer.close().unwrap();
    }
}

struct ParsedReplay {
    fills: Vec<Value>,
    merges: Vec<Value>,
    flattens: Vec<Value>,
    cancel_alls: Vec<Value>,
    phase_transitions: Vec<Value>,
    order_actions_dedup: Vec<(u64, u64, Value, Value)>,
    window_end: Value,
    summary: Value,
}

fn run_cli_replay_config6(input_path: &Path, output_path: &Path) -> ParsedReplay {
    let bin = env!("CARGO_BIN_EXE_poly-as");
    let output = Command::new(bin)
        .args([
            "lifecycle",
            "--source",
            "replay",
            "--input",
            input_path.to_str().unwrap(),
            "--jsonl",
            output_path.to_str().unwrap(),
            "--display",
            "stream",
            "--v-max",
            "50",
            "--base-size",
            "20",
            "--queue-decay",
            "conservative",
            "--max-entry-price",
            "0.65",
            "--tier1-hedge-trigger-price",
            "off",
            "--entry-cutoff-secs",
            "120",
            "--entry-open-delay-secs",
            "15",
            "--cap-tier1-hedge",
            "true",
        ])
        .output()
        .unwrap();

    assert!(
        output.status.success(),
        "poly-as replay failed: stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    let txt = std::fs::read_to_string(output_path).unwrap();
    let mut fills = Vec::new();
    let mut merges = Vec::new();
    let mut flattens = Vec::new();
    let mut cancel_alls = Vec::new();
    let mut phase_transitions = Vec::new();
    let mut actions_by_tick: BTreeMap<(u64, u64), Vec<Value>> = BTreeMap::new();
    let mut window_end = None;
    let mut summary = None;

    for line in txt.lines() {
        if line.trim().is_empty() {
            continue;
        }
        let row: Value = serde_json::from_str(line).unwrap();
        let ev = row["event"].as_str().unwrap();
        let ts_ms = row["ts_unix_ms"].as_u64().unwrap();
        let mono_ns = row["mono_ns"].as_u64().unwrap();
        let payload = row["payload"].clone();
        match ev {
            "fill" => fills.push(payload),
            "merge" => merges.push(payload),
            "flatten" => flattens.push(payload),
            "cancel_all" => cancel_alls.push(payload),
            "phase_transition" => phase_transitions.push(payload),
            "order_action" => actions_by_tick
                .entry((ts_ms, mono_ns))
                .or_default()
                .push(payload),
            "window_end" => window_end = Some(payload),
            "summary" => summary = Some(payload),
            _ => {}
        }
    }

    let order_actions_dedup = actions_by_tick
        .into_iter()
        .filter_map(|((ts, mono), mut vec)| {
            if vec.len() == 2 {
                let b = vec.pop().unwrap();
                let a = vec.pop().unwrap();
                Some((ts, mono, a, b))
            } else {
                None
            }
        })
        .collect();

    ParsedReplay {
        fills,
        merges,
        flattens,
        cancel_alls,
        phase_transitions,
        order_actions_dedup,
        window_end: window_end.expect("window_end event missing"),
        summary: summary.expect("summary event missing"),
    }
}

/// Seeds a standard Config #6 epoch up to `t = 16.5s` where 20 Down shares fill passively
/// (or `fill_shares_micro` partial fill if smaller trade size is used) and no Up fill occurs.
fn seed_down_entry_fixture(
    tag: &str,
    slug: &str,
    start_ms: u64,
    fill_trade_shares_micro: u64,
) -> (PathBuf, ReplayFixture) {
    let path = temp_jsonl_path(tag);
    let mut fx = ReplayFixture::create(&path, slug, start_ms);
    fx.binance_spot(5, 60_000.0, 1);
    // Keep depth at 49c (10sh) and 45c (10sh) so 47c/48c has 0 queue ahead.
    fx.poly_book(
        10,
        Outcome::Up,
        &[lvl(49, 10), lvl(45, 10)],
        &[lvl(51, 50), lvl(52, 50)],
    );
    fx.poly_book(
        15,
        Outcome::Down,
        &[lvl(49, 10), lvl(45, 10)],
        &[lvl(51, 50), lvl(52, 50)],
    );
    // Step past entry_open_delay_secs (15s) + SG warmup + 40ms order latency.
    fx.binance_spot(15_200, 60_000.0, 2);
    fx.binance_spot(16_000, 60_000.0, 3);
    // Sell trade hits our resting Down bid at ~47c-48c.
    fx.poly_trade(
        16_200,
        Outcome::Down,
        OrderSide::Sell,
        40,
        fill_trade_shares_micro,
    );
    (path, fx)
}

/// Executed FAK flatten fill returned by `PartialFlattenHarness::step_with_book`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct FlattenFillResult {
    outcome: Outcome,
    side: OrderSide,
    price_micro: u64,
    filled_shares_micro: u64,
    fee_micro: u64,
}

/// Direct RiskActor + PaperLedger harness simulating depth-bounded partial FAK fills
/// (since LivePaperSimulator::execute_commands in lifecycle_cmd.rs ignores book depth).
struct PartialFlattenHarness {
    harness: ActorPipelineHarness,
    priority_rx: tokio::sync::mpsc::Receiver<ExecutionCommand>,
    routine_rx: tokio::sync::mpsc::Receiver<ExecutionCommand>,
    actor: RiskActor,
    ledger: PaperLedger,
    start_ms: u64,
    expiry_ms: u64,
    seq: u64,
}

impl PartialFlattenHarness {
    fn new(start_ms: u64) -> Self {
        let (harness, priority_rx, routine_rx) = ActorPipelineHarness::new(64, 16);
        let risk_engine = DollarNotionalRiskEngine::new(RiskConfig {
            v_max_micro: MicroUsdc(50_000_000),
            base_order_size_shares: 20,
            min_order_size_shares: 5,
            cap_tier1_reducing_to_delta: true,
            ..Default::default()
        })
        .unwrap();
        let hysteresis = HysteresisGate::new(HysteresisConfig {
            hysteresis_buf: 0.25,
            min_order_size_shares: 5,
        })
        .unwrap();
        let lifecycle = EpochLifecycleController::new(LifecycleConfig {
            pre_expiry_lockout_secs: 60,
            mandatory_flatten_secs: 30,
            terminal_lockout_secs: 15,
            min_order_size_shares: 5,
            entry_open_delay_secs: 15,
            entry_cutoff_secs: Some(120),
            ..Default::default()
        })
        .unwrap();
        let breakers = CircuitBreakers::new(BreakerConfig::default(), MicroUsdc(10_000), 0);
        let actor = RiskActor::new(
            harness.clone(),
            risk_engine,
            hysteresis,
            lifecycle,
            breakers,
            FaultInjection::None,
        );
        Self {
            harness,
            priority_rx,
            routine_rx,
            actor,
            ledger: PaperLedger::new(FeeSchedule::btc_5m_default()),
            start_ms,
            expiry_ms: start_ms + 300_000,
            seq: 0,
        }
    }

    fn seed_long_down(&mut self, shares: u64, price_cents: u64) {
        self.ledger
            .record_fill(&paper_sim::SimFill {
                sim_id: 1,
                outcome: Outcome::Down,
                side: OrderSide::Buy,
                price: MicroUsdc(price_cents * 10_000),
                filled_shares: SharesMicro(shares * ONE_SHARE_MICRO),
                fill_ts_ms: self.start_ms + 112_986,
                is_maker: true,
            })
            .unwrap();
        self.publish_snapshot();
    }

    fn publish_snapshot(&self) {
        self.harness
            .publish_registry_snapshot(OrderRegistrySnapshot {
                unhedged_up: self.ledger.filled_up,
                unhedged_dn: self.ledger.filled_dn,
                unhedged_cost_up_micro: MicroUsdc(self.ledger.cost_basis_up_micro),
                unhedged_cost_dn_micro: MicroUsdc(self.ledger.cost_basis_dn_micro),
                available_usdc_micro: MicroUsdc(100_000_000),
                ..Default::default()
            });
    }

    /// Evaluates one tick with given BBOs and optional depth-capped FAK execution.
    fn step_with_book(
        &mut self,
        offset_ms: u64,
        best_bid_up: Option<u64>,
        best_ask_up: Option<(u64, u64)>, // (cents, depth_shares)
        best_bid_dn: Option<(u64, u64)>, // (cents, depth_shares)
        best_ask_dn: Option<u64>,
        clear_in_flight_immediately: bool,
    ) -> (RiskActorTrace, Option<FlattenFillResult>) {
        self.seq += 1;
        let now_ms = self.start_ms + offset_ms;
        let now_ns = offset_ms * 1_000_000;
        let quote = match (best_ask_up, best_ask_dn) {
            (Some(_), Some(_)) => QuoteOutput::Active {
                bid_up: best_bid_up.map(|c| MicroUsdc(c * 10_000)),
                bid_dn: best_bid_dn.map(|(c, _)| MicroUsdc(c * 10_000)),
                pair_cost_micro: None,
                gross_margin_bps: None,
            },
            _ => QuoteOutput::Suppressed(math_engine::SuppressionReason::EmptyOrderBook),
        };
        self.harness.publish_quote(QuoteProposal {
            seq: self.seq,
            now_ms,
            expiry_ms: self.expiry_ms,
            tick_size_micro: MicroUsdc(10_000),
            quote,
            best_bid_up: best_bid_up.map(|c| MicroUsdc(c * 10_000)),
            best_ask_up: best_ask_up.map(|(c, _)| MicroUsdc(c * 10_000)),
            best_bid_dn: best_bid_dn.map(|(c, _)| MicroUsdc(c * 10_000)),
            best_ask_dn: best_ask_dn.map(|c| MicroUsdc(c * 10_000)),
            ..Default::default()
        });

        let trace = self.actor.step(now_ns, 0).unwrap().unwrap();
        while self.routine_rx.try_recv().is_ok() {}

        let mut executed = None;
        while let Ok(cmd) = self.priority_rx.try_recv() {
            if let ExecutionCommand::ExecuteFlattenFak {
                outcome,
                price,
                size,
                ..
            } = cmd
            {
                let (side, avail_micro) = match outcome {
                    Outcome::Up => (
                        OrderSide::Buy,
                        best_ask_up
                            .filter(|(c, _)| c * 10_000 <= price.0)
                            .map_or(0, |(_, sh)| sh * ONE_SHARE_MICRO),
                    ),
                    Outcome::Down => (
                        OrderSide::Sell,
                        best_bid_dn
                            .filter(|(c, _)| c * 10_000 >= price.0)
                            .map_or(0, |(_, sh)| sh * ONE_SHARE_MICRO),
                    ),
                };
                let fill_shares = size.0.min(avail_micro);
                if fill_shares > 0 {
                    let (fee, _) = self
                        .ledger
                        .record_taker_flatten_with_merge(
                            outcome,
                            side,
                            price,
                            SharesMicro(fill_shares),
                        )
                        .unwrap();
                    executed = Some(FlattenFillResult {
                        outcome,
                        side,
                        price_micro: price.0,
                        filled_shares_micro: fill_shares,
                        fee_micro: fee.0,
                    });
                }
                if clear_in_flight_immediately {
                    self.actor.clear_flatten_in_flight();
                }
                self.publish_snapshot();
            }
        }
        (trace, executed)
    }
}

#[test]
fn scenario_1_empty_opposing_book_throughout_flatten_window() {
    let start_ms = 1_795_657_800_000_u64;
    let (in_path, mut fx) = seed_down_entry_fixture(
        "s1_empty",
        "btc-updown-5m-1795657800",
        start_ms,
        120 * ONE_SHARE_MICRO,
    );
    // Shift to Bk 99/-----/1 before PreExpiryLockout (T-63s = 237s) and hold through expiry.
    fx.poly_book(237_000, Outcome::Up, &[lvl(99, 100)], &[]);
    fx.poly_book(237_010, Outcome::Down, &[], &[lvl(1, 100)]);
    fx.binance_spot(269_500, 60_100.0, 4);
    fx.binance_spot(285_500, 60_100.0, 5);
    fx.binance_spot(300_200, 60_100.0, 6);
    fx.close();

    let out_path = temp_jsonl_path("s1_empty_out");
    let res = run_cli_replay_config6(&in_path, &out_path);

    assert_eq!(res.fills.len(), 1);
    let fill_price = res.fills[0]["price_micro"].as_u64().unwrap();
    let fill_shares = res.fills[0]["filled_shares_micro"].as_u64().unwrap();
    assert_eq!(fill_shares, 20 * ONE_SHARE_MICRO);
    assert!(res.flattens.is_empty(), "expected 0 flatten executions");
    assert!(
        res.cancel_alls.is_empty(),
        "no cancel_all JSONL event emitted when resting quotes were already canceled at T-63s"
    );
    assert!(
        res.phase_transitions
            .iter()
            .any(|pt| pt["to_phase"].as_str() == Some("MandatoryFlatten"))
    );
    assert_eq!(res.summary["flattens_executed"].as_u64().unwrap(), 0);
    assert_eq!(res.summary["fees_paid_micro"].as_u64().unwrap(), 0);
    assert!(
        res.summary["flatten_unavailable_ticks"].as_u64().unwrap() >= 145,
        "summary flatten_unavailable_ticks = {}",
        res.summary["flatten_unavailable_ticks"]
    );
    assert_eq!(res.window_end["flattens_executed"].as_u64().unwrap(), 0);
    assert_eq!(res.window_end["flatten_attempts"].as_u64().unwrap(), 0);
    assert!(
        res.window_end["flatten_unavailable_ticks"]
            .as_u64()
            .unwrap()
            >= 145
    );
    assert_eq!(
        res.window_end["residual_q_dn_micro"].as_u64().unwrap(),
        20 * ONE_SHARE_MICRO
    );
    assert_eq!(res.window_end["residual_q_up_micro"].as_u64().unwrap(), 0);
    assert_eq!(
        res.window_end["max_abs_unpaired_micro"].as_u64().unwrap(),
        20 * ONE_SHARE_MICRO
    );

    let mf_ticks: Vec<_> = res
        .order_actions_dedup
        .iter()
        .filter(|(_, _, up, _)| up.get("phase").and_then(Value::as_str) == Some("MandatoryFlatten"))
        .collect();
    assert!(
        mf_ticks.len() >= 145,
        "got {} MandatoryFlatten ticks",
        mf_ticks.len()
    );
    assert!(mf_ticks.iter().all(|(_, _, up, _)| {
        up.get("cancel_all_reason")
            .and_then(Value::as_str)
            .is_some_and(|r| r.contains("Mandatory flatten liquidity unavailable"))
    }));

    assert_eq!(res.window_end["winning_outcome"].as_str().unwrap(), "Up");
    let expected_loss = -((fill_shares as i64 * fill_price as i64) / 1_000_000);
    assert_eq!(
        res.window_end["settlement_pnl_delta_micro"]
            .as_i64()
            .unwrap(),
        expected_loss
    );
    assert_eq!(
        res.summary["settlement_pnl_micro"].as_i64().unwrap(),
        expected_loss
    );

    let _ = std::fs::remove_file(&in_path);
    let _ = std::fs::remove_file(&out_path);
}

#[test]
fn scenario_2_liquidity_appears_after_initially_empty_book() {
    let start_ms = 1_795_657_900_000_u64;
    let (in_path, mut fx) = seed_down_entry_fixture(
        "s2_reappear",
        "btc-updown-5m-1795657900",
        start_ms,
        120 * ONE_SHARE_MICRO,
    );
    // Empty opposing liquidity entering MandatoryFlatten (T-30s = 270s).
    fx.poly_book(268_000, Outcome::Up, &[lvl(99, 100)], &[]);
    fx.poly_book(268_010, Outcome::Down, &[], &[lvl(1, 100)]);
    // At T-22s (278s), opposing Up ask reappears at 95c for 50sh.
    fx.poly_book(278_000, Outcome::Up, &[lvl(94, 50)], &[lvl(95, 50)]);
    fx.poly_book(278_010, Outcome::Down, &[lvl(5, 50)], &[lvl(6, 50)]);
    fx.binance_spot(300_200, 60_100.0, 4);
    fx.close();

    let out_path = temp_jsonl_path("s2_reappear_out");
    let res = run_cli_replay_config6(&in_path, &out_path);

    let unavail_ticks = res
        .order_actions_dedup
        .iter()
        .filter(|(_, _, up, _)| {
            up.get("phase").and_then(Value::as_str) == Some("MandatoryFlatten")
                && up
                    .get("cancel_all_reason")
                    .and_then(Value::as_str)
                    .is_some_and(|r| r.contains("Mandatory flatten liquidity unavailable"))
        })
        .count();
    assert!(
        unavail_ticks >= 70,
        "expected ~80 unavailable ticks during 270..278s, got {unavail_ticks}"
    );

    assert_eq!(res.flattens.len(), 1);
    assert_eq!(res.summary["flattens_executed"].as_u64().unwrap(), 1);
    assert_eq!(res.flattens[0]["outcome"].as_str().unwrap(), "Up");
    assert_eq!(res.flattens[0]["side"].as_str().unwrap(), "BUY");
    assert_eq!(res.flattens[0]["price_micro"].as_u64().unwrap(), 950_000);
    assert_eq!(
        res.flattens[0]["size_micro"].as_u64().unwrap(),
        20 * ONE_SHARE_MICRO
    );

    assert_eq!(res.merges.len(), 1);
    assert_eq!(
        res.merges[0]["merged_pairs_micro"].as_u64().unwrap(),
        20 * ONE_SHARE_MICRO
    );
    assert_eq!(
        res.window_end["settlement_pnl_delta_micro"]
            .as_i64()
            .unwrap(),
        0
    );

    let _ = std::fs::remove_file(&in_path);
    let _ = std::fs::remove_file(&out_path);
}

#[test]
fn scenario_3_partial_flatten_then_liquidity_disappears() {
    // Part A: Verify CLI replay now bounds the flatten fill to displayed 10sh depth
    // and leaves remaining 10sh Down unflattened once liquidity vanishes at T-29s.
    let start_ms = 1_795_658_000_000_u64;
    let (in_path, mut fx) = seed_down_entry_fixture(
        "s3_cli",
        "btc-updown-5m-1795658000",
        start_ms,
        120 * ONE_SHARE_MICRO,
    );
    // Provide only 10sh at 90c on Up ask before MandatoryFlatten (T-32s = 268s), then empty at T-29s.
    fx.poly_book(268_000, Outcome::Up, &[lvl(89, 50)], &[lvl(90, 10)]);
    fx.poly_book(268_010, Outcome::Down, &[], &[lvl(11, 50)]);
    fx.poly_book(271_000, Outcome::Up, &[lvl(99, 50)], &[]);
    fx.poly_book(271_010, Outcome::Down, &[], &[lvl(1, 50)]);
    fx.binance_spot(300_200, 60_100.0, 4);
    fx.close();

    let out_path = temp_jsonl_path("s3_cli_out");
    let cli_res = run_cli_replay_config6(&in_path, &out_path);
    assert_eq!(cli_res.flattens.len(), 1);
    assert_eq!(
        cli_res.flattens[0]["price_micro"].as_u64().unwrap(),
        900_000
    );
    assert_eq!(
        cli_res.flattens[0]["size_micro"].as_u64().unwrap(),
        10 * ONE_SHARE_MICRO
    );
    assert_eq!(
        cli_res.flattens[0]["requested_size_micro"]
            .as_u64()
            .unwrap(),
        20 * ONE_SHARE_MICRO
    );
    assert_eq!(
        cli_res.window_end["flatten_shares_filled_micro"]
            .as_u64()
            .unwrap(),
        10 * ONE_SHARE_MICRO
    );
    assert_eq!(
        cli_res.window_end["residual_q_dn_micro"].as_u64().unwrap(),
        10 * ONE_SHARE_MICRO
    );
    let _ = std::fs::remove_file(&in_path);
    let _ = std::fs::remove_file(&out_path);

    // Part B: Realistic depth-bounded partial FAK fill via RiskActor + PaperLedger.
    let mut h = PartialFlattenHarness::new(start_ms);
    h.seed_long_down(20, 45); // 20sh Down @ $0.45 = $9.00 cost basis

    // T-29.5s (270.5s): Up ask has 10sh @ 90c -> partial fill 10sh, leaves 10sh Down ($4.50).
    let (tr1, ex1) = h.step_with_book(270_500, Some(89), Some((90, 10)), None, Some(11), true);
    assert_eq!(tr1.phase, EpochPhase::MandatoryFlatten);
    assert_eq!(
        ex1.map(|r| (r.outcome, r.side, r.price_micro, r.filled_shares_micro)),
        Some((Outcome::Up, OrderSide::Buy, 900_000, 10 * ONE_SHARE_MICRO))
    );
    assert_eq!(h.ledger.filled_up, SharesMicro::ZERO);
    assert_eq!(h.ledger.filled_dn, SharesMicro(10 * ONE_SHARE_MICRO));
    assert_eq!(h.ledger.cost_basis_dn_micro, 4_500_000);
    assert_eq!(
        h.ledger.total_pairs_merged,
        SharesMicro(10 * ONE_SHARE_MICRO)
    );
    // 10 pairs @ ($0.45 + $0.90 = $1.35) -> -$3.50 merge loss minus taker fee.
    assert_eq!(h.ledger.total_merge_gain_micro, -3_500_000);
    let fee1 = ex1.unwrap().fee_micro as i64;
    assert!(fee1 > 0);

    // T-29.0s..T-16.0s: opposing liquidity disappears -> actor retries every tick, no execution.
    for t in (271_000..=284_000).step_by(1_000) {
        let (tr, ex) = h.step_with_book(t, Some(99), None, None, Some(1), true);
        assert_eq!(tr.phase, EpochPhase::MandatoryFlatten);
        assert!(ex.is_none());
        assert!(
            tr.cancel_all_reason
                .as_deref()
                .is_some_and(|r| r.contains("Mandatory flatten liquidity unavailable"))
        );
    }

    // Settle Up win -> remaining 10sh Down ($4.50) expires worthless.
    let settle_delta = h.ledger.settle_epoch(Outcome::Up);
    assert_eq!(settle_delta, -4_500_000);
    assert_eq!(h.ledger.filled_dn, SharesMicro::ZERO);
    assert_eq!(h.ledger.realized_pnl_micro, -3_500_000 - fee1 - 4_500_000);
}

#[test]
fn scenario_4_partial_flatten_then_successful_retry() {
    let start_ms = 1_795_658_100_000_u64;
    let mut h = PartialFlattenHarness::new(start_ms);
    h.seed_long_down(20, 45); // 20sh Down @ $0.45 = $9.00

    // Attempt 1 (T-29s = 271s): Up ask has 10sh @ 85c -> partial fill 10sh.
    let (tr1, ex1) = h.step_with_book(271_000, Some(84), Some((85, 10)), None, Some(16), true);
    assert_eq!(tr1.phase, EpochPhase::MandatoryFlatten);
    assert_eq!(
        ex1.map(|r| (r.outcome, r.side, r.price_micro, r.filled_shares_micro)),
        Some((Outcome::Up, OrderSide::Buy, 850_000, 10 * ONE_SHARE_MICRO))
    );
    assert_eq!(h.ledger.filled_dn, SharesMicro(10 * ONE_SHARE_MICRO));

    // Attempt 2 (T-28s = 272s): Up ask now shows 20sh @ 88c -> retry buys remaining 10sh.
    let (tr2, ex2) = h.step_with_book(272_000, Some(87), Some((88, 20)), None, Some(13), true);
    assert_eq!(tr2.phase, EpochPhase::MandatoryFlatten);
    assert_eq!(
        ex2.map(|r| (r.outcome, r.side, r.price_micro, r.filled_shares_micro)),
        Some((Outcome::Up, OrderSide::Buy, 880_000, 10 * ONE_SHARE_MICRO))
    );
    assert_eq!(h.ledger.filled_up, SharesMicro::ZERO);
    assert_eq!(h.ledger.filled_dn, SharesMicro::ZERO);
    assert_eq!(
        h.ledger.total_pairs_merged,
        SharesMicro(20 * ONE_SHARE_MICRO)
    );
    // Merge 1: 10*(1.00 - 0.45 - 0.85) = -$3.00; Merge 2: 10*(1.00 - 0.45 - 0.88) = -$3.30.
    assert_eq!(h.ledger.total_merge_gain_micro, -6_300_000);
    let total_fees = (ex1.unwrap().fee_micro + ex2.unwrap().fee_micro) as i64;
    assert_eq!(h.ledger.fees_paid_micro as i64, total_fees);
    assert_eq!(h.ledger.settle_epoch(Outcome::Up), 0);
    assert_eq!(h.ledger.realized_pnl_micro, -6_300_000 - total_fees);
}

#[test]
fn scenario_5_extreme_but_available_opposing_prices() {
    // Sub-case A: BUY UP @ 99c (extreme ask, no Down bid).
    let start_ms_a = 1_795_658_200_000_u64;
    let (in_a, mut fx_a) = seed_down_entry_fixture(
        "s5a_extreme",
        "btc-updown-5m-1795658200",
        start_ms_a,
        120 * ONE_SHARE_MICRO,
    );
    fx_a.poly_book(268_000, Outcome::Up, &[lvl(98, 50)], &[lvl(99, 50)]);
    fx_a.poly_book(268_010, Outcome::Down, &[], &[lvl(2, 50)]);
    fx_a.binance_spot(300_200, 60_100.0, 4);
    fx_a.close();

    let out_a = temp_jsonl_path("s5a_extreme_out");
    let res_a = run_cli_replay_config6(&in_a, &out_a);
    let entry_px_a = res_a.fills[0]["price_micro"].as_u64().unwrap();
    assert_eq!(res_a.flattens.len(), 1);
    assert_eq!(res_a.flattens[0]["outcome"].as_str().unwrap(), "Up");
    assert_eq!(res_a.flattens[0]["side"].as_str().unwrap(), "BUY");
    assert_eq!(res_a.flattens[0]["price_micro"].as_u64().unwrap(), 990_000);
    assert_eq!(res_a.merges.len(), 1);
    assert_eq!(
        res_a.window_end["settlement_pnl_delta_micro"]
            .as_i64()
            .unwrap(),
        0
    );
    let fee_a = res_a.flattens[0]["fee_micro"].as_u64().unwrap() as i64;
    let expected_net_a = 20 * (1_000_000_i64 - entry_px_a as i64 - 990_000_i64) - fee_a;
    assert_eq!(
        res_a.window_end["net_pnl_micro"].as_i64().unwrap(),
        expected_net_a
    );
    let _ = std::fs::remove_file(&in_a);
    let _ = std::fs::remove_file(&out_a);

    // Sub-case B: SELL DOWN @ 1c fallback (best_ask_up = None, best_bid_dn = 1c).
    let start_ms_b = 1_795_658_300_000_u64;
    let (in_b, mut fx_b) = seed_down_entry_fixture(
        "s5b_sell_fallback",
        "btc-updown-5m-1795658300",
        start_ms_b,
        120 * ONE_SHARE_MICRO,
    );
    fx_b.poly_book(268_000, Outcome::Up, &[lvl(99, 50)], &[]);
    fx_b.poly_book(268_010, Outcome::Down, &[lvl(1, 50)], &[lvl(2, 50)]);
    fx_b.binance_spot(300_200, 60_100.0, 4);
    fx_b.close();

    let out_b = temp_jsonl_path("s5b_sell_fallback_out");
    let res_b = run_cli_replay_config6(&in_b, &out_b);
    let entry_px = res_b.fills[0]["price_micro"].as_u64().unwrap();
    assert_eq!(res_b.flattens.len(), 1);
    assert_eq!(res_b.flattens[0]["outcome"].as_str().unwrap(), "Down");
    assert_eq!(res_b.flattens[0]["side"].as_str().unwrap(), "SELL");
    assert_eq!(res_b.flattens[0]["price_micro"].as_u64().unwrap(), 10_000);
    assert_eq!(res_b.merges.len(), 0);
    assert_eq!(
        res_b.window_end["settlement_pnl_delta_micro"]
            .as_i64()
            .unwrap(),
        0
    );
    let fee = res_b.flattens[0]["fee_micro"].as_u64().unwrap() as i64;
    let expected_net = 20 * (10_000_i64 - entry_px as i64) - fee;
    assert_eq!(
        res_b.window_end["net_pnl_micro"].as_i64().unwrap(),
        expected_net
    );
    let _ = std::fs::remove_file(&in_b);
    let _ = std::fs::remove_file(&out_b);
}

#[test]
fn scenario_6_multiple_flatten_attempts_with_changing_book_conditions() {
    let start_ms = 1_795_658_400_000_u64;
    let mut h = PartialFlattenHarness::new(start_ms);
    h.seed_long_down(20, 45); // 20sh Down @ $0.45

    // 1. T-29.5s..T-28.0s: empty opposing book -> no execution, retries keep firing.
    for t in [270_500_u64, 271_000, 272_000] {
        let (_, ex) = h.step_with_book(t, Some(99), None, None, Some(1), true);
        assert!(ex.is_none());
    }

    // 2. T-27.0s: 6sh Up ask @ 92c appears -> BUY 6sh Up @ 92c (14sh Down left).
    let (_, ex1) = h.step_with_book(273_000, Some(91), Some((92, 6)), None, Some(9), true);
    assert_eq!(
        ex1.map(|r| (r.outcome, r.side, r.price_micro, r.filled_shares_micro)),
        Some((Outcome::Up, OrderSide::Buy, 920_000, 6 * ONE_SHARE_MICRO))
    );
    assert_eq!(h.ledger.filled_dn, SharesMicro(14 * ONE_SHARE_MICRO));

    // 3. T-26.0s..T-25.0s: liquidity vanishes again -> unavailable retries.
    for t in [274_000_u64, 275_000] {
        let (tr, ex) = h.step_with_book(t, Some(99), None, None, Some(1), true);
        assert!(ex.is_none());
        assert!(
            tr.cancel_all_reason
                .as_deref()
                .is_some_and(|r| r.contains("Mandatory flatten liquidity unavailable"))
        );
    }

    // 4. T-24.0s: Up ask still None, Down bid 6sh @ 5c appears -> fallback SELL 6sh Down @ 5c (8sh left).
    let (_, ex2) = h.step_with_book(276_000, Some(95), None, Some((5, 6)), Some(6), true);
    assert_eq!(
        ex2.map(|r| (r.outcome, r.side, r.price_micro, r.filled_shares_micro)),
        Some((Outcome::Down, OrderSide::Sell, 50_000, 6 * ONE_SHARE_MICRO))
    );
    assert_eq!(h.ledger.filled_dn, SharesMicro(8 * ONE_SHARE_MICRO));

    // 5. T-22.0s: Up ask 20sh @ 94c appears -> BUY 8sh Up @ 94c -> completely flat!
    let (_, ex3) = h.step_with_book(278_000, Some(93), Some((94, 20)), None, Some(7), true);
    assert_eq!(
        ex3.map(|r| (r.outcome, r.side, r.price_micro, r.filled_shares_micro)),
        Some((Outcome::Up, OrderSide::Buy, 940_000, 8 * ONE_SHARE_MICRO))
    );
    assert_eq!(h.ledger.filled_up, SharesMicro::ZERO);
    assert_eq!(h.ledger.filled_dn, SharesMicro::ZERO);
    assert_eq!(
        h.ledger.total_pairs_merged,
        SharesMicro(14 * ONE_SHARE_MICRO)
    );
    // Merge 1: 6*(1.00 - 0.45 - 0.92) = -$2.22; Sell: 6*(0.05 - 0.45) = -$2.40; Merge 2: 8*(1.00 - 0.45 - 0.94) = -$3.12.
    assert_eq!(h.ledger.total_merge_gain_micro, -2_220_000 - 3_120_000);
    let fees = (ex1.unwrap().fee_micro + ex2.unwrap().fee_micro + ex3.unwrap().fee_micro) as i64;
    assert_eq!(h.ledger.settle_epoch(Outcome::Up), 0);
    assert_eq!(
        h.ledger.realized_pnl_micro,
        -2_220_000 - 2_400_000 - 3_120_000 - fees
    );
}

#[test]
fn scenario_7_residual_dust_after_otherwise_successful_flatten() {
    // Part A: CLI replay with 3sh passive partial fill entering MandatoryFlatten with full opposing liquidity.
    let start_ms = 1_795_658_500_000_u64;
    let (in_path, mut fx) = seed_down_entry_fixture(
        "s7_dust_cli",
        "btc-updown-5m-1795658500",
        start_ms,
        3 * ONE_SHARE_MICRO,
    );
    // Keep ample opposing liquidity during MandatoryFlatten: Up ask = 90c (50sh).
    fx.poly_book(268_000, Outcome::Up, &[lvl(89, 50)], &[lvl(90, 50)]);
    fx.poly_book(268_010, Outcome::Down, &[lvl(10, 50)], &[lvl(11, 50)]);
    fx.binance_spot(300_200, 60_100.0, 4);
    fx.close();

    let out_path = temp_jsonl_path("s7_dust_cli_out");
    let cli_res = run_cli_replay_config6(&in_path, &out_path);
    assert_eq!(cli_res.fills.len(), 1);
    let fill_px = cli_res.fills[0]["price_micro"].as_u64().unwrap();
    let fill_sh = cli_res.fills[0]["filled_shares_micro"].as_u64().unwrap();
    assert_eq!(fill_sh, 3 * ONE_SHARE_MICRO);
    assert!(cli_res.flattens.is_empty());
    assert_eq!(cli_res.summary["flattens_executed"].as_u64().unwrap(), 0);

    let dust_ticks = cli_res
        .order_actions_dedup
        .iter()
        .filter(|(_, _, up, _)| {
            up.get("phase").and_then(Value::as_str) == Some("MandatoryFlatten")
                && up
                    .get("cancel_all_reason")
                    .and_then(Value::as_str)
                    .is_some_and(|r| r.contains("Residual dust below min order size"))
        })
        .count();
    assert!(dust_ticks >= 145, "got {dust_ticks} dust ticks");
    assert_eq!(
        cli_res.window_end["settlement_pnl_delta_micro"]
            .as_i64()
            .unwrap(),
        -((fill_sh as i64 * fill_px as i64) / 1_000_000)
    );
    let _ = std::fs::remove_file(&in_path);
    let _ = std::fs::remove_file(&out_path);

    // Part B: Partial FAK flatten leaves 3sh residual dust (< 5sh min_order_size), then settles unhedged.
    let mut h = PartialFlattenHarness::new(start_ms + 100_000);
    h.seed_long_down(20, 45);

    // Partial fill of 17sh Up @ 90c leaves 3sh Down ($1.35 cost basis).
    let (_, ex1) = h.step_with_book(271_000, Some(89), Some((90, 17)), None, Some(11), true);
    assert_eq!(
        ex1.map(|r| (r.outcome, r.side, r.price_micro, r.filled_shares_micro)),
        Some((Outcome::Up, OrderSide::Buy, 900_000, 17 * ONE_SHARE_MICRO))
    );
    assert_eq!(h.ledger.filled_dn, SharesMicro(3 * ONE_SHARE_MICRO));
    assert_eq!(h.ledger.cost_basis_dn_micro, 1_350_000);

    // Next tick in MandatoryFlatten (with ample 50sh @ 90c liquidity!) traps 3sh in ResidualDust.
    let (tr2, ex2) = h.step_with_book(272_000, Some(89), Some((90, 50)), None, Some(11), true);
    assert!(ex2.is_none());
    assert_eq!(
        tr2.flatten_action,
        FlattenAction::ResidualDust {
            unhedged_shares: SharesMicro(3 * ONE_SHARE_MICRO),
            min_order_size: SharesMicro(5 * ONE_SHARE_MICRO),
        }
    );
    assert_eq!(tr2.tier, RiskTier::Tier4HardStop);
    assert_eq!(
        tr2.cancel_all_reason.as_deref(),
        Some("Residual dust below min order size")
    );

    let settle = h.ledger.settle_epoch(Outcome::Up);
    assert_eq!(settle, -1_350_000);
    let fee1 = ex1.unwrap().fee_micro as i64;
    // 17 pairs @ ($0.45 + $0.90 = $1.35) -> -$5.95 merge loss, -$1.35 dust settlement, -fee1.
    assert_eq!(h.ledger.total_merge_gain_micro, -5_950_000);
    assert_eq!(h.ledger.realized_pnl_micro, -5_950_000 - fee1 - 1_350_000);
}

#[test]
fn scenario_8_cli_replay_shallow_l2_partial_flatten_then_vanish() {
    let start_ms = 1_795_658_800_000_u64;
    let (in_path, mut fx) = seed_down_entry_fixture(
        "s8_cli_partial_vanish",
        "btc-updown-5m-1795658800",
        start_ms,
        120 * ONE_SHARE_MICRO,
    );
    // Enter MandatoryFlatten with empty opposing liquidity, then expose only 10sh @ 90c on Up ask.
    fx.poly_book(268_000, Outcome::Up, &[lvl(99, 100)], &[]);
    fx.poly_book(268_010, Outcome::Down, &[], &[lvl(1, 100)]);
    fx.poly_book(271_000, Outcome::Up, &[lvl(89, 10)], &[lvl(90, 10)]);
    fx.binance_spot(300_200, 60_100.0, 4);
    fx.close();

    let out_path = temp_jsonl_path("s8_cli_partial_vanish_out");
    let res = run_cli_replay_config6(&in_path, &out_path);

    let fill_price = res.fills[0]["price_micro"].as_u64().unwrap();
    // Single 10sh partial fill @ 90c; consumed level must not double-fill on later ticks.
    assert_eq!(res.flattens.len(), 1);
    assert_eq!(res.flattens[0]["outcome"].as_str().unwrap(), "Up");
    assert_eq!(res.flattens[0]["side"].as_str().unwrap(), "BUY");
    assert_eq!(res.flattens[0]["price_micro"].as_u64().unwrap(), 900_000);
    assert_eq!(
        res.flattens[0]["size_micro"].as_u64().unwrap(),
        10 * ONE_SHARE_MICRO
    );
    assert_eq!(
        res.flattens[0]["requested_size_micro"].as_u64().unwrap(),
        20 * ONE_SHARE_MICRO
    );

    assert_eq!(res.window_end["flattens_executed"].as_u64().unwrap(), 1);
    assert_eq!(
        res.window_end["flatten_shares_filled_micro"]
            .as_u64()
            .unwrap(),
        10 * ONE_SHARE_MICRO
    );
    assert_eq!(
        res.window_end["residual_q_dn_micro"].as_u64().unwrap(),
        10 * ONE_SHARE_MICRO
    );
    assert_eq!(res.window_end["residual_q_up_micro"].as_u64().unwrap(), 0);
    assert!(
        res.window_end["flatten_unavailable_ticks"]
            .as_u64()
            .unwrap()
            >= 100,
        "expected unavailable ticks after 10sh ask consumed, got {}",
        res.window_end["flatten_unavailable_ticks"]
    );
    let expected_settle = -((10 * ONE_SHARE_MICRO as i64 * fill_price as i64) / 1_000_000);
    assert_eq!(
        res.window_end["settlement_pnl_delta_micro"]
            .as_i64()
            .unwrap(),
        expected_settle
    );

    let _ = std::fs::remove_file(&in_path);
    let _ = std::fs::remove_file(&out_path);
}

#[test]
fn scenario_9_cli_replay_shallow_l2_partial_flatten_then_refill_retry() {
    let start_ms = 1_795_659_100_000_u64;
    let (in_path, mut fx) = seed_down_entry_fixture(
        "s9_cli_partial_refill",
        "btc-updown-5m-1795659100",
        start_ms,
        120 * ONE_SHARE_MICRO,
    );
    fx.poly_book(268_000, Outcome::Up, &[lvl(99, 100)], &[]);
    fx.poly_book(268_010, Outcome::Down, &[], &[lvl(1, 100)]);
    // Attempt 1 @ T-29s: only 10sh Up ask @ 85c.
    fx.poly_book(271_000, Outcome::Up, &[lvl(84, 10)], &[lvl(85, 10)]);
    // Refill @ T-26s: 20sh Up ask @ 88c completes remaining 10sh flatten.
    fx.poly_book(274_000, Outcome::Up, &[lvl(87, 20)], &[lvl(88, 20)]);
    fx.binance_spot(300_200, 60_100.0, 4);
    fx.close();

    let out_path = temp_jsonl_path("s9_cli_partial_refill_out");
    let res = run_cli_replay_config6(&in_path, &out_path);

    assert_eq!(res.flattens.len(), 2);
    assert_eq!(res.flattens[0]["price_micro"].as_u64().unwrap(), 850_000);
    assert_eq!(
        res.flattens[0]["size_micro"].as_u64().unwrap(),
        10 * ONE_SHARE_MICRO
    );
    assert_eq!(
        res.flattens[0]["requested_size_micro"].as_u64().unwrap(),
        20 * ONE_SHARE_MICRO
    );
    assert_eq!(res.flattens[1]["price_micro"].as_u64().unwrap(), 880_000);
    assert_eq!(
        res.flattens[1]["size_micro"].as_u64().unwrap(),
        10 * ONE_SHARE_MICRO
    );
    assert_eq!(
        res.flattens[1]["requested_size_micro"].as_u64().unwrap(),
        10 * ONE_SHARE_MICRO
    );

    assert_eq!(res.window_end["flattens_executed"].as_u64().unwrap(), 2);
    assert_eq!(
        res.window_end["flatten_shares_filled_micro"]
            .as_u64()
            .unwrap(),
        20 * ONE_SHARE_MICRO
    );
    assert_eq!(res.window_end["residual_q_dn_micro"].as_u64().unwrap(), 0);
    assert_eq!(
        res.window_end["settlement_pnl_delta_micro"]
            .as_i64()
            .unwrap(),
        0
    );

    let _ = std::fs::remove_file(&in_path);
    let _ = std::fs::remove_file(&out_path);
}

#[test]
fn scenario_10_cli_replay_shallow_l2_partial_flatten_leaves_dust() {
    let start_ms = 1_795_659_400_000_u64;
    let (in_path, mut fx) = seed_down_entry_fixture(
        "s10_cli_partial_dust",
        "btc-updown-5m-1795659400",
        start_ms,
        120 * ONE_SHARE_MICRO,
    );
    fx.poly_book(268_000, Outcome::Up, &[lvl(99, 100)], &[]);
    fx.poly_book(268_010, Outcome::Down, &[], &[lvl(1, 100)]);
    // Partial fill of 17sh @ 90c leaves 3sh Down dust.
    fx.poly_book(271_000, Outcome::Up, &[lvl(89, 17)], &[lvl(90, 17)]);
    // Even after deep refill @ T-26s, 3sh dust cannot be flattened.
    fx.poly_book(274_000, Outcome::Up, &[lvl(90, 20)], &[lvl(91, 20)]);
    fx.poly_book(274_010, Outcome::Down, &[lvl(9, 20)], &[lvl(10, 20)]);
    fx.binance_spot(300_200, 60_100.0, 4);
    fx.close();

    let out_path = temp_jsonl_path("s10_cli_partial_dust_out");
    let res = run_cli_replay_config6(&in_path, &out_path);

    let fill_price = res.fills[0]["price_micro"].as_u64().unwrap();
    assert_eq!(res.flattens.len(), 1);
    assert_eq!(
        res.flattens[0]["size_micro"].as_u64().unwrap(),
        17 * ONE_SHARE_MICRO
    );
    assert_eq!(
        res.flattens[0]["requested_size_micro"].as_u64().unwrap(),
        20 * ONE_SHARE_MICRO
    );
    assert_eq!(res.window_end["flattens_executed"].as_u64().unwrap(), 1);
    assert_eq!(
        res.window_end["flatten_shares_filled_micro"]
            .as_u64()
            .unwrap(),
        17 * ONE_SHARE_MICRO
    );
    assert_eq!(
        res.window_end["residual_q_dn_micro"].as_u64().unwrap(),
        3 * ONE_SHARE_MICRO
    );
    assert!(res.window_end["residual_dust_ticks"].as_u64().unwrap() >= 100);
    let expected_dust_settle = -((3 * ONE_SHARE_MICRO as i64 * fill_price as i64) / 1_000_000);
    assert_eq!(
        res.window_end["settlement_pnl_delta_micro"]
            .as_i64()
            .unwrap(),
        expected_dust_settle
    );

    let _ = std::fs::remove_file(&in_path);
    let _ = std::fs::remove_file(&out_path);
}
