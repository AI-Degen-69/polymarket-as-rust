#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::indexing_slicing
    )
)]

use core_types::{FeeSchedule, MicroUsdc, OrderSide, Outcome, SharesMicro};
use paper_sim::{
    FifoBrokerConfig, FifoPaperBroker, NewOrderParams, PaperLedger, PaperSimError, SimFill,
};

#[test]
fn test_vector_1_fifo_partial_fill() {
    // Vector 1: Order size=20 @ $0.48 with volume_ahead=100.
    // Trade #1 size=80 @ $0.48 -> 0 filled (volume_ahead=20).
    // Trade #2 size=25 @ $0.48 -> 5 shares filled (remaining=15, volume_ahead=0), NOT full 20!
    let mut broker = FifoPaperBroker::new(FifoBrokerConfig {
        order_latency_ms: 0,
        cancel_latency_ms: 0,
        ..Default::default()
    });

    let order_id = broker
        .submit_new(NewOrderParams {
            outcome: Outcome::Up,
            side: OrderSide::Buy,
            price: MicroUsdc(480_000),     // $0.48
            size: SharesMicro(20_000_000), // 20 shares
            now_ms: 100,
            current_best_bid: Some(MicroUsdc(480_000)),
            current_best_ask: Some(MicroUsdc(500_000)),
            book_depth_at_price: SharesMicro(100_000_000), // volume_ahead = 100 shares
        })
        .expect("order placement should succeed");

    // Trade #1: 80 shares @ $0.48
    let fills_1 = broker.on_public_trade(
        Outcome::Up,
        MicroUsdc(480_000),
        SharesMicro(80_000_000),
        110,
    );
    assert_eq!(fills_1.len(), 0, "Trade #1 should yield 0 fills");

    let order_after_1 = broker.get_order(order_id).expect("order must exist");
    assert_eq!(
        order_after_1.remaining_size,
        SharesMicro(20_000_000),
        "remaining should still be 20"
    );
    assert_eq!(
        order_after_1.volume_ahead,
        SharesMicro(20_000_000),
        "volume_ahead should be 20"
    );

    // Trade #2: 25 shares @ $0.48
    let fills_2 = broker.on_public_trade(
        Outcome::Up,
        MicroUsdc(480_000),
        SharesMicro(25_000_000),
        120,
    );
    assert_eq!(fills_2.len(), 1, "Trade #2 should yield 1 fill");
    assert_eq!(
        fills_2[0].filled_shares,
        SharesMicro(5_000_000),
        "Trade #2 must fill exactly 5 shares"
    );
    assert_eq!(fills_2[0].price, MicroUsdc(480_000));

    let order_after_2 = broker.get_order(order_id).expect("order must exist");
    assert_eq!(
        order_after_2.remaining_size,
        SharesMicro(15_000_000),
        "remaining must be 15"
    );
    assert_eq!(
        order_after_2.volume_ahead,
        SharesMicro::ZERO,
        "volume_ahead must be 0"
    );
}

#[test]
fn test_vector_2_trade_through_volume_cap() {
    // Vector 2: Order size=50 @ $0.48. Trade size=12 @ $0.47 -> 12 shares filled (remaining=38), NOT 50!
    let mut broker = FifoPaperBroker::new(FifoBrokerConfig {
        order_latency_ms: 0,
        cancel_latency_ms: 0,
        ..Default::default()
    });

    let order_id = broker
        .submit_new(NewOrderParams {
            outcome: Outcome::Up,
            side: OrderSide::Buy,
            price: MicroUsdc(480_000),
            size: SharesMicro(50_000_000),
            now_ms: 100,
            current_best_bid: Some(MicroUsdc(480_000)),
            current_best_ask: Some(MicroUsdc(500_000)),
            book_depth_at_price: SharesMicro(100_000_000),
        })
        .expect("order placement should succeed");

    let fills = broker.on_public_trade(
        Outcome::Up,
        MicroUsdc(470_000),      // Traded through at $0.47
        SharesMicro(12_000_000), // Size 12 shares
        110,
    );

    assert_eq!(fills.len(), 1, "Must generate 1 fill event");
    assert_eq!(
        fills[0].filled_shares,
        SharesMicro(12_000_000),
        "Must fill exactly 12 shares, capped by traded volume"
    );

    let order = broker.get_order(order_id).expect("order must exist");
    assert_eq!(
        order.remaining_size,
        SharesMicro(38_000_000),
        "remaining size must be 38 shares"
    );
    assert_eq!(
        order.volume_ahead,
        SharesMicro::ZERO,
        "volume_ahead wiped by trade-through"
    );
}

#[test]
fn test_vector_3_post_only_rejection_in_sim() {
    // Vector 3: Submitting bid @ $0.52 when best_ask == $0.52 -> rejected as PostOnlyCrossed,
    // never added to resting book.
    let mut broker = FifoPaperBroker::new(FifoBrokerConfig::default());

    let err = broker
        .submit_new(NewOrderParams {
            outcome: Outcome::Up,
            side: OrderSide::Buy,
            price: MicroUsdc(520_000), // $0.52
            size: SharesMicro(20_000_000),
            now_ms: 100,
            current_best_bid: Some(MicroUsdc(510_000)),
            current_best_ask: Some(MicroUsdc(520_000)), // best_ask == $0.52
            book_depth_at_price: SharesMicro::ZERO,
        })
        .unwrap_err();

    match err {
        PaperSimError::PostOnlyCrossed { price, best_ask } => {
            assert_eq!(price, MicroUsdc(520_000));
            assert_eq!(best_ask, MicroUsdc(520_000));
        }
        other => panic!("expected PostOnlyCrossed error, got: {other:?}"),
    }

    assert_eq!(
        broker.orders().len(),
        0,
        "rejected order must not be added to book"
    );
}

#[test]
fn test_vector_4_vwap_pair_merge_math() {
    // Vector 4: Buy 10 UP @ $0.47 + 10 UP @ $0.49 (avg_up = $0.48), Buy 10 DN @ $0.48
    // -> merge 10 pairs -> pair_cost = $0.96, realized_pnl = +$0.400000,
    // residual filled_up = 10 (cost_basis = $4.80), filled_dn = 0.
    let mut ledger = PaperLedger::new(FeeSchedule::btc_5m_default());

    // Buy 10 UP @ $0.47
    let fill_up_1 = SimFill {
        sim_id: 1,
        outcome: Outcome::Up,
        side: OrderSide::Buy,
        price: MicroUsdc(470_000),
        filled_shares: SharesMicro(10_000_000),
        fill_ts_ms: 100,
        is_maker: true,
    };
    let m1 = ledger.record_fill(&fill_up_1).expect("fill record ok");
    assert!(m1.is_none(), "no merge yet");

    // Buy 10 UP @ $0.49
    let fill_up_2 = SimFill {
        sim_id: 2,
        outcome: Outcome::Up,
        side: OrderSide::Buy,
        price: MicroUsdc(490_000),
        filled_shares: SharesMicro(10_000_000),
        fill_ts_ms: 200,
        is_maker: true,
    };
    let m2 = ledger.record_fill(&fill_up_2).expect("fill record ok");
    assert!(m2.is_none(), "no merge yet");

    assert_eq!(ledger.filled_up, SharesMicro(20_000_000));
    assert_eq!(ledger.cost_basis_up_micro, 9_600_000); // $9.60 total
    assert_eq!(ledger.vwap_up(), Some(MicroUsdc(480_000))); // $0.48 VWAP

    // Buy 10 DN @ $0.48 -> triggers merge of 10 pairs!
    let fill_dn = SimFill {
        sim_id: 3,
        outcome: Outcome::Down,
        side: OrderSide::Buy,
        price: MicroUsdc(480_000),
        filled_shares: SharesMicro(10_000_000),
        fill_ts_ms: 300,
        is_maker: true,
    };
    let m3 = ledger.record_fill(&fill_dn).expect("fill record ok");
    let merge = m3.expect("must produce merge event");

    assert_eq!(merge.merged_pairs, SharesMicro(10_000_000));
    assert_eq!(merge.avg_cost_up, MicroUsdc(480_000));
    assert_eq!(merge.avg_cost_dn, MicroUsdc(480_000));
    assert_eq!(merge.pair_cost, MicroUsdc(960_000)); // $0.96
    assert_eq!(merge.realized_gain_micro, 400_000); // +$0.400000

    assert_eq!(ledger.realized_pnl_micro, 400_000);
    assert_eq!(ledger.filled_up, SharesMicro(10_000_000));
    assert_eq!(ledger.cost_basis_up_micro, 4_800_000); // $4.80
    assert_eq!(ledger.filled_dn, SharesMicro::ZERO);
    assert_eq!(ledger.cost_basis_dn_micro, 0);
    assert_eq!(ledger.total_pairs_merged, SharesMicro(10_000_000));
}

#[test]
fn test_vector_5_taker_flatten_fee_deduction() {
    // Vector 5: Taker Flatten Fee Deduction: FAK 100 shares @ $0.50 (feeRate=0.05)
    // -> fees_paid = $1.250000, net PnL reduced by exact $1.25.
    let mut ledger = PaperLedger::new(FeeSchedule::btc_5m_default());

    // Assume we hold 100 shares of UP acquired at $0.50
    let fill_up = SimFill {
        sim_id: 1,
        outcome: Outcome::Up,
        side: OrderSide::Buy,
        price: MicroUsdc(500_000),
        filled_shares: SharesMicro(100_000_000),
        fill_ts_ms: 100,
        is_maker: true,
    };
    let _ = ledger.record_fill(&fill_up).expect("fill ok");
    assert_eq!(ledger.realized_pnl_micro, 0);
    assert_eq!(ledger.fees_paid_micro, 0);

    // FAK flatten sell 100 shares UP @ $0.50
    let fee = ledger
        .record_taker_flatten(
            Outcome::Up,
            OrderSide::Sell,
            MicroUsdc(500_000),
            SharesMicro(100_000_000),
        )
        .expect("flatten should succeed");

    assert_eq!(fee, MicroUsdc(1_250_000), "fee must be exactly $1.250000");
    assert_eq!(
        ledger.fees_paid_micro, 1_250_000,
        "fees_paid must be $1.250000"
    );
    assert_eq!(
        ledger.realized_pnl_micro, -1_250_000,
        "net realized PnL must be reduced by exact $1.25 (-$1.250000)"
    );
    assert_eq!(ledger.filled_up, SharesMicro::ZERO);
    assert_eq!(ledger.cost_basis_up_micro, 0);
}

#[test]
fn test_vector_6_maker_rebate_accrual_and_epoch_settlement() {
    let mut ledger = PaperLedger::new(FeeSchedule::btc_5m_default());

    // Maker fill: 100 shares UP @ $0.50 -> wV = 100 * 0.50 * 0.50 = 25.00
    // Counterfactual fee = 25 * 0.05 = $1.25; 20% maker rebate share = $0.25 = 250_000 micro-USDC.
    let fill = SimFill {
        sim_id: 1,
        outcome: Outcome::Up,
        side: OrderSide::Buy,
        price: MicroUsdc(500_000),
        filled_shares: SharesMicro(100_000_000),
        fill_ts_ms: 100,
        is_maker: true,
    };
    assert!(ledger.record_fill(&fill).unwrap().is_none());
    assert_eq!(ledger.maker_weighted_volume_micro, 25_000_000);
    assert_eq!(ledger.estimated_rebate_micro, 250_000);

    // Settle epoch with UP winning: 100 UP @ $1.00 - $50.00 cost = +$50.00
    let expected_settled = ledger.settlement_pnl_micro(Outcome::Up);
    assert_eq!(expected_settled, 50_000_000);
    let delta = ledger.settle_epoch(Outcome::Up);
    assert_eq!(delta, 50_000_000);
    assert_eq!(ledger.realized_pnl_micro, 50_000_000);
    assert_eq!(ledger.filled_up, SharesMicro::ZERO);
    assert_eq!(ledger.filled_dn, SharesMicro::ZERO);
    assert_eq!(ledger.cost_basis_up_micro, 0);
    assert_eq!(ledger.cost_basis_dn_micro, 0);
}
