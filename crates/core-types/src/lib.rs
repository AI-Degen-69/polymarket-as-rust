//! Core domain types, fixed-point 1e6 arithmetic, fail-closed clock guards,
//! redacted configuration validation, and atomic telemetry for `poly-as`.

#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::indexing_slicing
    )
)]

/// Monotonic QPC clock, exchange offset drift breaker, and Windows timer guard.
pub mod clock;
/// Redacted `.env` / JSON configuration loader and cross-field invariant validator.
pub mod config;
/// Typed domain, decimal parse, wire, clock, and telemetry error enums.
pub mod error;
/// Outcome enums, Polymarket market descriptor, and bounded stack-allocated `FixedBook<N>`.
pub mod market;
/// Fixed-point 1e6 micro-USDC / micro-share arithmetic, tick snapping, wire encoding, and fee math.
pub mod money;
/// NEX sandbox (Amoy `80002`) profile, WebSocket paths, and data-origin tags.
pub mod sandbox;
/// HDR latency histogram tracking and atomic newline-delimited JSON telemetry writer.
pub mod telemetry;

pub use clock::{ClockOffsetEstimator, HighResTimerGuard, MonotonicClock};
pub use config::{EngineConfig, LiveCredentials};
pub use error::{ClockError, ConfigError, MathError, ParseDecimalError, TelemetryError, WireError};
pub use market::{
    BookLevel, ExecutionMode, FixedBook, KappaMode, MarketDescriptor, Outcome, ProtocolVersion,
    SgMode, VarianceMode, WalletMode,
};
pub use money::{
    DecimalFracPolicy, FeeSchedule, FinitePosF64, MICRO_SCALE, MicroUsdc, ONE_CENT_MICRO,
    ONE_DOLLAR_MICRO, ONE_SHARE_MICRO, OrderSide, SIZE_STEP_MICRO, SharesMicro, SignedSharesMicro,
    TickPrecision, WireAmounts, encode_buy_amounts, encode_sell_amounts, floor_to_tick,
    parse_decimal_micro, parse_decimal_scaled,
};
pub use sandbox::{
    DataOrigin, NEX_AMOY_EXCHANGE, NEX_AMOY_POSITION_MANAGER, NEX_CHAIN_ID, NEX_CLOB_URL,
    NEX_GAMMA_URL, NEX_INGRESS_IP, NEX_MARKET_WS_PATH, NEX_RELAYER_URL, NEX_USER_WS_PATH,
    NEX_V2_MARKET_CAP_PER_IP, NEX_V2_MARKET_WS_PATH, NEX_V2_USER_CAP_PER_IP,
    NEX_V2_USER_CAP_PER_KEY, NEX_V2_USER_WS_PATH, NEX_WS_URL, NexWsVersion, SandboxProfile,
};
pub use telemetry::{JsonlWriter, LatencySummary, LatencyTracker};

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::*;

    fn lookup_map<'a>(
        map: &'a HashMap<&'static str, &'static str>,
    ) -> impl Fn(&'static str) -> Result<Option<String>, ConfigError> + 'a {
        move |k| Ok(map.get(k).map(|s| (*s).to_string()))
    }

    #[test]
    fn numeric_parse_decimal_micro_vectors() {
        assert_eq!(parse_decimal_micro(b"0.52"), Ok(520_000));
        assert_eq!(parse_decimal_micro(b"0.29"), Ok(290_000));
        assert_eq!(parse_decimal_micro(b"10"), Ok(10_000_000));
        assert_eq!(parse_decimal_micro(b"10.25"), Ok(10_250_000));
        assert_eq!(parse_decimal_micro(b"0.0001"), Ok(100));
        assert_eq!(parse_decimal_micro(b"0.123456"), Ok(123_456));
        assert_eq!(parse_decimal_micro(b"0"), Ok(0));
    }

    #[test]
    fn numeric_wire_encoding_buy_and_sell_vectors() {
        let ten_shares = SharesMicro::from_shares_str("10.00").unwrap();
        let p_52 = MicroUsdc::from_usd_str("0.52").unwrap();
        let buy = encode_buy_amounts(p_52, ten_shares, TickPrecision::Cent).unwrap();
        assert_eq!(buy.maker_amount, 5_200_000);
        assert_eq!(buy.taker_amount, 10_000_000);

        let sell = encode_sell_amounts(p_52, ten_shares, TickPrecision::Cent).unwrap();
        assert_eq!(sell.maker_amount, 10_000_000);
        assert_eq!(sell.taker_amount, 5_200_000);

        // Exact integer math avoids `0.29 * 10.0` float drift (`2899999`).
        let p_29 = MicroUsdc::from_usd_str("0.29").unwrap();
        let buy_29 = encode_buy_amounts(p_29, ten_shares, TickPrecision::Cent).unwrap();
        assert_eq!(buy_29.maker_amount, 2_900_000);
        assert_eq!(buy_29.taker_amount, 10_000_000);

        // Share truncation down to 2 size decimals (`0.01`).
        let unrounded_shares = SharesMicro::from_shares_str("10.129").unwrap();
        let buy_trunc = encode_buy_amounts(p_52, unrounded_shares, TickPrecision::Cent).unwrap();
        assert_eq!(buy_trunc.taker_amount, 10_120_000);
        assert_eq!(buy_trunc.maker_amount, 5_262_400);
    }

    #[test]
    fn numeric_wire_encoding_all_tick_precisions() {
        let shares = SharesMicro::from_shares_str("1.00").unwrap();
        let cases = [
            (TickPrecision::Tenth, 100_000, 100_000),
            (TickPrecision::Cent, 10_000, 10_000),
            (TickPrecision::HalfCent, 5_000, 5_000),
            (TickPrecision::QuarterCent, 2_500, 2_500),
            (TickPrecision::Mill, 1_000, 1_000),
            (TickPrecision::TenthMill, 100, 100),
        ];
        for (tick, price_raw, expected_usd) in cases {
            let enc = encode_buy_amounts(MicroUsdc(price_raw), shares, tick).unwrap();
            assert_eq!(enc.maker_amount, expected_usd);
            assert_eq!(enc.taker_amount, 1_000_000);
        }
    }

    #[test]
    fn numeric_fee_schedule_vectors() {
        let shares_100 = SharesMicro::from_whole_shares(100).unwrap();
        let sched_05 = FeeSchedule::btc_5m_default();
        assert_eq!(
            sched_05
                .compute_taker_fee_micro(shares_100, MicroUsdc(500_000))
                .unwrap(),
            MicroUsdc(1_250_000)
        );
        assert_eq!(
            sched_05
                .compute_taker_fee_micro(shares_100, MicroUsdc(100_000))
                .unwrap(),
            MicroUsdc(450_000)
        );
        assert_eq!(
            sched_05
                .compute_taker_fee_micro(shares_100, MicroUsdc(900_000))
                .unwrap(),
            MicroUsdc(450_000)
        );
        assert_eq!(
            sched_05
                .compute_taker_fee_micro(shares_100, MicroUsdc(10_000))
                .unwrap(),
            MicroUsdc(49_500)
        );

        let sched_07 = FeeSchedule::btc_5m_legacy();
        assert_eq!(
            sched_07
                .compute_taker_fee_micro(shares_100, MicroUsdc(500_000))
                .unwrap(),
            MicroUsdc(1_750_000)
        );

        let tiny_shares = SharesMicro::from_shares_str("0.01").unwrap();
        assert_eq!(
            sched_05
                .compute_taker_fee_micro(tiny_shares, MicroUsdc(8_000))
                .unwrap(),
            MicroUsdc::ZERO
        );
        assert_eq!(
            sched_05
                .compute_maker_fee_micro(shares_100, MicroUsdc(500_000))
                .unwrap(),
            MicroUsdc::ZERO
        );
    }

    #[test]
    fn numeric_tick_floor_and_hysteresis_diff() {
        assert_eq!(
            floor_to_tick(0.4999, MicroUsdc(10_000)).unwrap(),
            MicroUsdc(490_000)
        );
        assert_eq!(
            floor_to_tick(0.29, MicroUsdc(10_000)).unwrap(),
            MicroUsdc(290_000)
        );
        assert_eq!(
            MicroUsdc(520_000).abs_diff(MicroUsdc(510_000)),
            MicroUsdc(10_000)
        );
    }

    #[test]
    fn config_locked_defaults_validate() {
        let cfg = EngineConfig::from_lookup(|_| Ok(None)).unwrap();
        assert_eq!(cfg.wallet_mode, WalletMode::DepositWallet1271);
        assert_eq!(cfg.max_notional_per_leg_micro, MicroUsdc(50_000_000));
        assert_eq!(cfg.base_order_size_shares, SharesMicro(20_000_000));
        assert_eq!(cfg.min_combined_spread_micro, MicroUsdc(20_000));
        assert_eq!(cfg.max_combined_bid_micro, MicroUsdc(980_000));
        assert_eq!(cfg.variance_mode, VarianceMode::BinaryCara);
        assert_eq!(cfg.kappa_mode, KappaMode::TradeNormalizedDepth);
        assert_eq!(cfg.sg_mode, SgMode::Causal);
        assert!(!cfg.polybolt_enabled);
        assert!(cfg.include_variance_spread);

        let json = serde_json::to_string(&cfg).unwrap();
        let roundtrip: EngineConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(roundtrip.gamma, cfg.gamma);
        assert_eq!(roundtrip.wallet_mode, cfg.wallet_mode);
    }

    #[test]
    fn adversarial_parse_decimal_rejects_malformed_inputs() {
        let bad: &[&[u8]] = &[
            b"",
            b"-0.50",
            b"+0.50",
            b"NaN",
            b"inf",
            b"1.1234567",
            b".5",
            b"1.",
            b".",
            b"1..2",
            b" 0.5",
            b"0.5 ",
            b"99999999999999999999",
        ];
        for input in bad {
            assert!(
                parse_decimal_micro(input).is_err(),
                "expected error for {:?}",
                String::from_utf8_lossy(input)
            );
        }
    }

    #[test]
    fn adversarial_floor_to_tick_and_finite_pos_reject_invalid() {
        assert!(floor_to_tick(f64::NAN, MicroUsdc(10_000)).is_err());
        assert!(floor_to_tick(f64::INFINITY, MicroUsdc(10_000)).is_err());
        assert!(floor_to_tick(f64::NEG_INFINITY, MicroUsdc(10_000)).is_err());
        assert!(floor_to_tick(-0.01, MicroUsdc(10_000)).is_err());
        assert!(floor_to_tick(1_000_001.0, MicroUsdc(10_000)).is_err());
        assert!(floor_to_tick(1e20, MicroUsdc(10_000)).is_err());
        assert!(floor_to_tick(0.50, MicroUsdc(0)).is_err());
        assert!(floor_to_tick(0.50, MicroUsdc(1_000_001)).is_err());

        assert!(FinitePosF64::try_new(f64::NAN).is_err());
        assert!(FinitePosF64::try_new(0.0).is_err());
        assert!(FinitePosF64::try_new(-1.0).is_err());
        assert!(serde_json::from_str::<FinitePosF64>("0.0").is_err());
        assert!(serde_json::from_str::<FinitePosF64>("-5.0").is_err());
        assert!(serde_json::from_str::<FinitePosF64>("102500.5").is_ok());
    }

    #[test]
    fn adversarial_config_redacts_secrets_in_debug_json_and_errors() {
        let mut map = HashMap::new();
        map.insert("SIGNER_SECRET_KEY", "SECRET_SENTINEL_123");
        map.insert("CLOB_API_KEY", "SECRET_API_KEY_SENTINEL");
        map.insert("CLOB_API_SECRET", "SECRET_HMAC_SENTINEL");
        map.insert("CLOB_API_PASSPHRASE", "SECRET_PASS_SENTINEL");
        map.insert("RELAYER_API_KEY", "SECRET_RELAYER_SENTINEL");
        map.insert("CHAINLINK_API_KEY", "SECRET_CHAINLINK_SENTINEL");

        let cfg = EngineConfig::from_lookup(lookup_map(&map)).unwrap();
        let dbg = format!("{cfg:?}");
        let json = serde_json::to_string(&cfg).unwrap();

        for sentinel in [
            "SECRET_SENTINEL_123",
            "SECRET_API_KEY_SENTINEL",
            "SECRET_HMAC_SENTINEL",
            "SECRET_PASS_SENTINEL",
            "SECRET_RELAYER_SENTINEL",
            "SECRET_CHAINLINK_SENTINEL",
        ] {
            assert!(!dbg.contains(sentinel), "Debug leaked {sentinel}");
            assert!(!json.contains(sentinel), "JSON leaked {sentinel}");
        }
        assert!(dbg.contains("[REDACTED]"));

        let dotenv_err = dotenvy::Error::LineParse("SIGNER_SECRET_KEY=LEAK_ME".to_string(), 0);
        let sanitized = ConfigError::from_dotenvy(&dotenv_err);
        assert!(!sanitized.to_string().contains("LEAK_ME"));
    }

    #[test]
    fn adversarial_clock_offset_fail_closed_and_drift_breaker() {
        let est = ClockOffsetEstimator::new(1_000);
        assert!(!est.has_sample());
        assert!(est.is_drift_exceeded());
        assert_eq!(est.offset_ms(), Err(ClockError::UnsyncedClock));
        assert_eq!(est.adjusted_unix_ms(1_000), Err(ClockError::UnsyncedClock));

        assert_eq!(
            est.record_sample(0, u64::MAX),
            Err(ClockError::OffsetOverflow)
        );
        assert!(!est.has_sample());

        assert_eq!(
            est.record_sample(1_700_000_000_000, 1_700_000_000_250),
            Ok(250)
        );
        assert!(est.has_sample());
        assert!(!est.is_drift_exceeded());
        assert_eq!(est.adjusted_unix_ms(1_000), Ok(1_250));

        assert_eq!(
            est.record_sample(1_700_000_000_000, 1_700_000_001_250),
            Ok(1_250)
        );
        assert!(est.is_drift_exceeded());

        assert_eq!(
            est.record_sample(1_700_000_001_250, 1_700_000_000_000),
            Ok(-1_250)
        );
        assert!(est.is_drift_exceeded());
        assert_eq!(est.adjusted_unix_ms(500), Ok(0));
    }

    #[test]
    fn adversarial_fixed_book_validation_and_crossed_states() {
        let empty = FixedBook::<5>::empty(Outcome::Up);
        assert!(!empty.is_crossed());
        assert_eq!(empty.raw_mid_usd(), Ok(None));

        let normal = FixedBook::<5>::try_from_slices(
            Outcome::Up,
            &[BookLevel {
                price: MicroUsdc(480_000),
                size: SharesMicro(10_000_000),
            }],
            &[BookLevel {
                price: MicroUsdc(520_000),
                size: SharesMicro(10_000_000),
            }],
            100,
            200,
        )
        .unwrap();
        assert!(!normal.is_crossed());
        assert_eq!(normal.raw_mid_usd(), Ok(Some(0.5)));

        let crossed = FixedBook::<5>::try_from_slices(
            Outcome::Up,
            &[BookLevel {
                price: MicroUsdc(520_000),
                size: SharesMicro(10_000_000),
            }],
            &[BookLevel {
                price: MicroUsdc(510_000),
                size: SharesMicro(10_000_000),
            }],
            100,
            200,
        )
        .unwrap();
        assert!(crossed.is_crossed());
        assert!(matches!(
            crossed.raw_mid_usd(),
            Err(MathError::CrossedBook {
                best_bid_micro: 520_000,
                best_ask_micro: 510_000
            })
        ));

        // Zero size and unsorted book levels must be rejected.
        assert!(
            FixedBook::<5>::try_from_slices(
                Outcome::Up,
                &[BookLevel {
                    price: MicroUsdc(500_000),
                    size: SharesMicro::ZERO,
                }],
                &[],
                100,
                200,
            )
            .is_err()
        );
        assert!(
            FixedBook::<5>::try_from_slices(
                Outcome::Up,
                &[
                    BookLevel {
                        price: MicroUsdc(480_000),
                        size: SharesMicro(1_000_000)
                    },
                    BookLevel {
                        price: MicroUsdc(490_000),
                        size: SharesMicro(1_000_000)
                    },
                ],
                &[],
                100,
                200,
            )
            .is_err()
        );
    }

    #[test]
    fn adversarial_market_descriptor_rejects_invalid_fields() {
        let valid = MarketDescriptor {
            condition_id: "0xcond".to_string(),
            slug: "btc-updown-5m-1700000000".to_string(),
            question: "Will BTC be up in 5m?".to_string(),
            version: ProtocolVersion::PolyV2,
            neg_risk: false,
            asset_id_up: "100".to_string(),
            asset_id_dn: "200".to_string(),
            tick_size_micro: MicroUsdc(10_000),
            min_order_size_micro: SharesMicro(5_000_000),
            taker_delay_ms: 50,
            fee_schedule: FeeSchedule::btc_5m_default(),
            start_ts_ms: 1_700_000_000_000,
            expiry_ts_ms: 1_700_000_300_000,
            strike_price_usd: Some(FinitePosF64::try_new(95_000.0).unwrap()),
        };
        assert!(valid.validate().is_ok());

        let mut bad_window = valid.clone();
        bad_window.expiry_ts_ms = bad_window.start_ts_ms;
        assert!(bad_window.validate().is_err());

        let mut bad_assets = valid.clone();
        bad_assets.asset_id_dn = format!(" {} ", bad_assets.asset_id_up);
        assert!(bad_assets.validate().is_err());

        let mut bad_fee_sched = valid;
        bad_fee_sched.fee_schedule = FeeSchedule {
            fee_rate_e6: ONE_DOLLAR_MICRO + 1,
            maker_fee_zero: true,
        };
        assert!(bad_fee_sched.validate().is_err());
    }

    #[test]
    fn adversarial_config_rejects_broken_invariants() {
        let mut map = HashMap::new();
        map.insert("ENGINE_GAMMA", "0");
        assert!(EngineConfig::from_lookup(lookup_map(&map)).is_err());

        map.insert("ENGINE_GAMMA", "0.15");
        map.insert("MAX_COMBINED_BID_USD", "0.99");
        map.insert("MIN_COMBINED_SPREAD_USD", "0.02");
        assert!(EngineConfig::from_lookup(lookup_map(&map)).is_err());

        map.insert("MAX_COMBINED_BID_USD", "0.98");
        map.insert("POLY_SIG_TYPE", "9");
        assert!(EngineConfig::from_lookup(lookup_map(&map)).is_err());

        let mut canary = HashMap::new();
        canary.insert("EXECUTION_MODE", "canary-live");
        canary.insert("DRY_RUN", "true");
        assert!(EngineConfig::from_lookup(lookup_map(&canary)).is_err());

        canary.insert("DRY_RUN", "false");
        assert!(EngineConfig::from_lookup(lookup_map(&canary)).is_err());

        let mut bad_paper = HashMap::new();
        bad_paper.insert("EXECUTION_MODE", "paper");
        bad_paper.insert("DRY_RUN", "false");
        assert!(EngineConfig::from_lookup(lookup_map(&bad_paper)).is_err());

        let mut polybolt = HashMap::new();
        polybolt.insert("POLYBOLT_ENABLED", "true");
        assert!(EngineConfig::from_lookup(lookup_map(&polybolt)).is_err());

        polybolt.insert("CHAINLINK_API_KEY", "SECRET_CHAINLINK_SENTINEL");
        assert!(EngineConfig::from_lookup(lookup_map(&polybolt)).is_err());

        polybolt.insert("CLOB_API_KEY", "SECRET_API_KEY_SENTINEL");
        polybolt.insert("CLOB_API_SECRET", "SECRET_HMAC_SENTINEL");
        polybolt.insert("CLOB_API_PASSPHRASE", "SECRET_PASS_SENTINEL");
        assert!(EngineConfig::from_lookup(lookup_map(&polybolt)).is_ok());

        let mut zero_addr = HashMap::new();
        zero_addr.insert(
            "POLYMARKET_PROXY_ADDRESS",
            "0x0000000000000000000000000000000000000000",
        );
        assert!(EngineConfig::from_lookup(lookup_map(&zero_addr)).is_err());

        for bad_relayer in [
            "http://insecure.example/submit",
            "http://localhost.evil.com/submit",
            "http://127.0.0.1.evil.com/submit",
            "http://localhost@evil.com/submit",
            "https://?bad",
            "https://user@example.com/submit",
            "https://example.com:0/submit",
        ] {
            let mut bad_url = HashMap::new();
            bad_url.insert("POLYMARKET_RELAYER_URL", bad_relayer);
            assert!(
                EngineConfig::from_lookup(lookup_map(&bad_url)).is_err(),
                "expected URL rejection for {bad_relayer}"
            );
        }
        let mut local_url = HashMap::new();
        local_url.insert("POLYMARKET_RELAYER_URL", "http://localhost:8080/submit");
        assert!(EngineConfig::from_lookup(lookup_map(&local_url)).is_ok());

        let mut unaligned_size = HashMap::new();
        unaligned_size.insert("BASE_ORDER_SIZE_SHARES", "10.005");
        assert!(EngineConfig::from_lookup(lookup_map(&unaligned_size)).is_err());

        assert!(serde_json::from_str::<EngineConfig>(r#"{"gamma": -0.5}"#).is_err());
        assert!(serde_json::from_str::<EngineConfig>(r#"{"unknown_field": 1}"#).is_err());
        let lookup_err: Result<EngineConfig, ConfigError> = EngineConfig::from_lookup(|k| {
            Err(ConfigError::InvalidValue {
                key: k,
                value: "<non-utf8>".to_string(),
                reason: "bad utf8".to_string(),
            })
        });
        assert!(lookup_err.is_err());
    }

    #[test]
    fn adversarial_wire_and_fee_reject_invalid_inputs() {
        let shares = SharesMicro::from_shares_str("10.00").unwrap();
        assert!(encode_buy_amounts(MicroUsdc(525_000), shares, TickPrecision::Cent).is_err());
        assert!(encode_buy_amounts(MicroUsdc::ZERO, shares, TickPrecision::Cent).is_err());
        assert!(encode_buy_amounts(MicroUsdc::ONE_DOLLAR, shares, TickPrecision::Cent).is_err());
        assert!(
            encode_buy_amounts(
                MicroUsdc(500_000),
                SharesMicro::from_shares_str("0.009").unwrap(),
                TickPrecision::Cent
            )
            .is_err()
        );
        assert!(FeeSchedule::new(1_000_001, true).is_err());
        assert!(
            serde_json::from_str::<FeeSchedule>(r#"{"fee_rate_e6":1000001,"maker_fee_zero":true}"#)
                .is_err()
        );
        let invalid_sched = FeeSchedule {
            fee_rate_e6: 1_000_001,
            maker_fee_zero: true,
        };
        assert!(
            invalid_sched
                .compute_maker_fee_micro(shares, MicroUsdc(500_000))
                .is_err()
        );
        assert_eq!(
            FeeSchedule::btc_5m_default().compute_maker_fee_micro(shares, MicroUsdc::ONE_DOLLAR),
            Ok(MicroUsdc::ZERO)
        );
        assert!(
            FeeSchedule::btc_5m_default()
                .compute_maker_fee_micro(shares, MicroUsdc(ONE_DOLLAR_MICRO + 1))
                .is_err()
        );
    }

    #[test]
    fn adversarial_telemetry_zero_ns_tracking_and_atomic_jsonl() {
        let mut tracker = LatencyTracker::new().unwrap();
        tracker.record_ns(0).unwrap();
        tracker.record_ns(250).unwrap();
        let summary = tracker.summary();
        assert_eq!(summary.count, 2);
        assert_eq!(summary.zero_ns_samples, 1);
        assert_eq!(summary.min_ns, 1);

        let clock = MonotonicClock::start();
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("poly_as_test_{unique}.jsonl"));

        let mut writer = JsonlWriter::create(&path).unwrap();
        let mono_ns = clock.now_ns().unwrap();
        writer
            .write_event(1_700_000_000_000, mono_ns, "test_summary", &summary)
            .unwrap();
        writer.close().unwrap();

        let contents = std::fs::read_to_string(&path).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(contents.trim()).unwrap();
        assert_eq!(parsed["event"], "test_summary");
        assert_eq!(parsed["payload"]["zero_ns_samples"], 1);
        let _ = std::fs::remove_file(&path);
    }
}
