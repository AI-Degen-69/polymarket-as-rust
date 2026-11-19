#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::indexing_slicing
    )
)]

use std::collections::HashSet;

use core_types::{FeeSchedule, MicroUsdc, OrderSide, Outcome, SharesMicro};
use paper_sim::{
    FifoBrokerConfig, FifoPaperBroker, NewOrderParams, PaperLedger, SimFill, SimOrderStatus,
};

#[test]
fn test_adversarial_1_cancel_in_flight_adverse_fill_race() {
    // Adversarial 1: Emit Cancel at t=100ms (latency=40ms), trade hits at t=120ms
    // -> order MUST still fill (realistic adverse selection during cancel RTT!).
    let mut broker = FifoPaperBroker::new(FifoBrokerConfig {
        order_latency_ms: 10,
        cancel_latency_ms: 40,
        ..Default::default()
    });

    // Place order at t=0ms, active at t=10ms
    let sim_id = broker
        .submit_new(NewOrderParams {
            outcome: Outcome::Up,
            side: OrderSide::Buy,
            price: MicroUsdc(480_000),
            size: SharesMicro(20_000_000),
            now_ms: 0,
            current_best_bid: Some(MicroUsdc(480_000)),
            current_best_ask: Some(MicroUsdc(500_000)),
            book_depth_at_price: SharesMicro::ZERO, // At top of queue
        })
        .expect("submit ok");

    // Advance to t=50ms: order is Live
    broker.update_time(50);
    assert_eq!(
        broker.get_order(sim_id).expect("exists").status,
        SimOrderStatus::Live
    );

    // Cancel sent at t=100ms (in flight until 100 + 40 = 140ms)
    broker.cancel(sim_id, 100).expect("cancel sent");
    assert_eq!(
        broker.get_order(sim_id).expect("exists").status,
        SimOrderStatus::PendingCancel { cancel_at_ms: 140 }
    );

    // Adverse public trade hits at t=120ms (while cancel is in-flight!)
    let fills = broker.on_public_trade(
        Outcome::Up,
        MicroUsdc(480_000),
        SharesMicro(20_000_000),
        120,
    );

    assert_eq!(
        fills.len(),
        1,
        "Order must still fill during cancel in-flight window!"
    );
    assert_eq!(fills[0].filled_shares, SharesMicro(20_000_000));
    assert_eq!(
        broker.get_order(sim_id).expect("exists").status,
        SimOrderStatus::Filled
    );

    // At t=150ms, cancel expires, but order is already Filled
    broker.update_time(150);
    assert_eq!(
        broker.get_order(sim_id).expect("exists").status,
        SimOrderStatus::Filled
    );
}

#[test]
fn test_adversarial_2_same_millisecond_multi_order_placement() {
    // Adversarial 2: Place 10 orders at identical ts_ms -> 10 distinct monotonic sim_ids,
    // zero collision or overwrites.
    let mut broker = FifoPaperBroker::new(FifoBrokerConfig::default());
    let mut ids = HashSet::new();

    let ts_ms = 1_700_000_000_000_u64;
    for i in 1..=10 {
        let price = MicroUsdc(450_000 + i * 1_000);
        let id = broker
            .submit_new(NewOrderParams {
                outcome: Outcome::Up,
                side: OrderSide::Buy,
                price,
                size: SharesMicro(10_000_000),
                now_ms: ts_ms,
                current_best_bid: Some(MicroUsdc(450_000)),
                current_best_ask: Some(MicroUsdc(500_000)),
                book_depth_at_price: SharesMicro(100_000),
            })
            .expect("placement ok");
        assert!(ids.insert(id), "sim_id must be globally unique");
    }

    assert_eq!(ids.len(), 10, "must track exactly 10 unique IDs");
    assert_eq!(broker.orders().len(), 10, "book must contain 10 orders");
}

#[test]
fn test_adversarial_3_replay_determinism() {
    // Adversarial 3: Running identical sequence twice produces byte-for-byte
    // identical final PnL, fill count, and merged pairs.
    let run_sim = || -> (i64, u64, SharesMicro) {
        let mut broker = FifoPaperBroker::new(FifoBrokerConfig {
            order_latency_ms: 20,
            cancel_latency_ms: 20,
            ..Default::default()
        });
        let mut ledger = PaperLedger::new(FeeSchedule::btc_5m_default());

        // Step 1: Place UP and DOWN bids
        let id_up = broker
            .submit_new(NewOrderParams {
                outcome: Outcome::Up,
                side: OrderSide::Buy,
                price: MicroUsdc(480_000),
                size: SharesMicro(30_000_000),
                now_ms: 0,
                current_best_bid: Some(MicroUsdc(480_000)),
                current_best_ask: Some(MicroUsdc(500_000)),
                book_depth_at_price: SharesMicro(10_000_000),
            })
            .unwrap();

        let id_dn = broker
            .submit_new(NewOrderParams {
                outcome: Outcome::Down,
                side: OrderSide::Buy,
                price: MicroUsdc(490_000),
                size: SharesMicro(30_000_000),
                now_ms: 0,
                current_best_bid: Some(MicroUsdc(490_000)),
                current_best_ask: Some(MicroUsdc(510_000)),
                book_depth_at_price: SharesMicro(5_000_000),
            })
            .unwrap();

        // Step 2: Trade hits UP at t=50ms
        let fills_up =
            broker.on_public_trade(Outcome::Up, MicroUsdc(480_000), SharesMicro(25_000_000), 50);
        for f in &fills_up {
            let _ = ledger.record_fill(f);
        }

        // Step 3: Trade hits DN at t=100ms
        let fills_dn = broker.on_public_trade(
            Outcome::Down,
            MicroUsdc(490_000),
            SharesMicro(30_000_000),
            100,
        );
        for f in &fills_dn {
            let _ = ledger.record_fill(f);
        }

        // Step 4: Cancel remainder at t=150ms
        let _ = broker.cancel(id_up, 150);
        let _ = broker.cancel(id_dn, 150);
        broker.update_time(200);

        (
            ledger.realized_pnl_micro,
            ledger.total_fills_count,
            ledger.total_pairs_merged,
        )
    };

    let run1 = run_sim();
    let run2 = run_sim();
    assert_eq!(run1, run2, "Sim runs must be 100% deterministic");
    assert!(run1.1 > 0, "must have fills");
    assert!(run1.2.0 > 0, "must have merged pairs");
}

#[test]
fn test_order_in_flight_placement_latency_blocks_fill() {
    let mut broker = FifoPaperBroker::new(FifoBrokerConfig {
        order_latency_ms: 40,
        cancel_latency_ms: 40,
        ..Default::default()
    });

    // Submit at t=100ms -> active at t=140ms
    let _id = broker
        .submit_new(NewOrderParams {
            outcome: Outcome::Up,
            side: OrderSide::Buy,
            price: MicroUsdc(480_000),
            size: SharesMicro(10_000_000),
            now_ms: 100,
            current_best_bid: Some(MicroUsdc(480_000)),
            current_best_ask: Some(MicroUsdc(500_000)),
            book_depth_at_price: SharesMicro::ZERO,
        })
        .unwrap();

    // Trade arrives at t=120ms (before order arrives at exchange!)
    let fills = broker.on_public_trade(
        Outcome::Up,
        MicroUsdc(480_000),
        SharesMicro(10_000_000),
        120,
    );
    assert_eq!(
        fills.len(),
        0,
        "Order still in flight at t=120ms cannot be filled"
    );

    // Trade arrives at t=150ms (after order arrives at exchange)
    let fills_later = broker.on_public_trade(
        Outcome::Up,
        MicroUsdc(480_000),
        SharesMicro(10_000_000),
        150,
    );
    assert_eq!(
        fills_later.len(),
        1,
        "Order is live at t=150ms and fills successfully"
    );
}

#[test]
fn test_mark_to_market_and_settlement_pnl() {
    let mut ledger = PaperLedger::new(FeeSchedule::btc_5m_default());

    // Buy 20 UP @ $0.45 = $9.00 cost basis
    let fill = SimFill {
        sim_id: 1,
        outcome: Outcome::Up,
        side: OrderSide::Buy,
        price: MicroUsdc(450_000),
        filled_shares: SharesMicro(20_000_000),
        fill_ts_ms: 100,
        is_maker: true,
    };
    let _ = ledger.record_fill(&fill).unwrap();

    // Mark to market at UP mid = $0.55: mark value = 20 * $0.55 = $11.00 -> unrealized = +$2.00
    let u_pnl = ledger.unrealized_pnl_micro(MicroUsdc(550_000), MicroUsdc(450_000));
    assert_eq!(u_pnl, 2_000_000); // +$2.000000

    // Settlement PnL if UP wins: pays $1.00 each = $20.00 -> PnL = $20.00 - $9.00 = +$11.00
    let settle_up = ledger.settlement_pnl_micro(Outcome::Up);
    assert_eq!(settle_up, 11_000_000);

    // Settlement PnL if DOWN wins: pays $0.00 -> PnL = -$9.00
    let settle_dn = ledger.settlement_pnl_micro(Outcome::Down);
    assert_eq!(settle_dn, -9_000_000);
}
