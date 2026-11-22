//! Market discovery (`GammaDiscoveryClient`), L2 book maintenance (`BookMaintainer`),
//! and lock-free multi-feed WebSocket ingestion actors (`ClobMarketWsActor`, `BinanceSpotWsActor`,
//! optional `PolyBoltTwapWsActor`) pushing `Copy` events onto dedicated `rtrb` SPSC rings.

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

/// Binance Spot combined `@bookTicker` and `@aggTrade` WebSocket actor.
pub mod binance_ws;
/// Sorted L2 orderbook maintainer producing stack-allocated `FixedBook<N>`.
pub mod book_maintainer;
/// Polymarket CLOB market WebSocket actor with `"PING"`/`"PONG"`, stale watchdog, and REST resync.
pub mod clob_ws;
/// Gamma V1/V2 market discovery, CLOB `/book` / `/clob-markets` hydration, and REST clock sync.
pub mod discovery;
/// Typed feed discovery, parsing, orderbook, and WebSocket error enums.
pub mod error;
/// Zero-allocation `Copy` ringbuffer event enums and lossless health telemetry types.
pub mod events;
/// Zero-allocation ASCII `10^8` / `10^6` parsers and flexible Polymarket JSON decoders.
pub mod parse;
/// Optional PolyBolt Chainlink 60s TWAP WebSocket actor (`POLYBOLT_ENABLED=false` default).
pub mod polybolt_ws;
mod ws_common;

use std::sync::Once;

pub use binance_ws::{
    BinanceSpotWsActor, BinanceWsCommand, BinanceWsConfig, DEFAULT_BINANCE_WS_URL,
    parse_binance_ws_text,
};
pub use book_maintainer::BookMaintainer;
pub use clob_ws::{
    ClobMarketWsActor, ClobParseBatch, ClobWsCommand, ClobWsConfig, DEFAULT_CLOB_WS_URL,
    parse_clob_ws_text,
};
pub use discovery::{
    ClockSyncReport, DEFAULT_BINANCE_REST_URL, DEFAULT_CLOB_URL, DEFAULT_GAMMA_URL,
    GammaDiscoveryClient, parse_gamma_market_tokens,
};
pub use error::FeedError;
pub use events::{
    BinanceSpotEvent, DEFAULT_RING_CAPACITY, FeedHealthEvent, FeedId, FeedStats, OracleTwapEvent,
    PolyMarketEvent,
};
pub use parse::{
    E6_SCALE, E8_SCALE, parse_decimal_e8, parse_decimal_e8_truncating,
    parse_decimal_micro_flexible, parse_order_side, parse_outcome_label, parse_rfc3339_ms,
};
pub use polybolt_ws::{
    DEFAULT_POLYBOLT_WS_URL, POLYBOLT_CLOSE_AUTH_FAILED, POLYBOLT_CLOSE_DRAINING,
    POLYBOLT_CLOSE_POLICY_VIOLATION, PolyBoltParsed, PolyBoltSeqTracker, PolyBoltTwapWsActor,
    PolyBoltWsCommand, PolyBoltWsConfig, parse_polybolt_ws_text,
};

/// Installs the `rustls` `ring` `CryptoProvider` once per process before opening TLS sockets.
pub fn install_tls_provider() {
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::{Duration, Instant};

    use core_types::{
        BookLevel, ClockOffsetEstimator, EngineConfig, MicroUsdc, OrderSide, Outcome,
        ProtocolVersion, SharesMicro,
    };
    use futures_util::{SinkExt, StreamExt};
    use rtrb::RingBuffer;
    use secrecy::SecretString;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use tokio::sync::mpsc;
    use tokio_tungstenite::accept_async;
    use tokio_tungstenite::tungstenite::Message;
    use tokio_tungstenite::tungstenite::protocol::CloseFrame;
    use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;

    use super::*;

    #[test]
    fn numeric_parse_e8_and_flexible_micro_vectors() {
        assert_eq!(parse_decimal_e8(b"91250.12345678"), Ok(9_125_012_345_678));
        assert_eq!(parse_decimal_e8(b"12.450"), Ok(1_245_000_000));
        assert_eq!(parse_decimal_micro_flexible(b"0.52000000"), Ok(520_000));
        assert!(parse_decimal_micro_flexible(b"0.5200001").is_err());
        assert_eq!(
            parse_rfc3339_ms("2026-11-19T02:00:00Z").unwrap(),
            1_795_053_600_000
        );
    }

    #[test]
    fn numeric_gamma_v1_and_v2_token_mapping() {
        let (up_v1, dn_v1) = parse_gamma_market_tokens(
            Some("v1"),
            &["Down".to_string(), "Up".to_string()],
            &["dn_111".to_string(), "up_222".to_string()],
            &[],
        )
        .unwrap();
        assert_eq!(up_v1, "up_222");
        assert_eq!(dn_v1, "dn_111");

        let (up_v2, dn_v2) = parse_gamma_market_tokens(
            Some("v2"),
            &["Up".to_string(), "Down".to_string()],
            &[],
            &["pos_up_1".to_string(), "pos_dn_2".to_string()],
        )
        .unwrap();
        assert_eq!(up_v2, "pos_up_1");
        assert_eq!(dn_v2, "pos_dn_2");
    }

    #[test]
    fn numeric_book_maintainer_sorts_deletes_and_detects_crossed() {
        let mut bm = BookMaintainer::new(Outcome::Up);
        // Polymarket emits bids ascending and asks descending; BookMaintainer must normalize.
        let book = bm
            .apply_snapshot(
                &[
                    BookLevel {
                        price: MicroUsdc(460_000),
                        size: SharesMicro(50_000_000),
                    },
                    BookLevel {
                        price: MicroUsdc(480_000),
                        size: SharesMicro(10_000_000),
                    },
                    BookLevel {
                        price: MicroUsdc(490_000),
                        size: SharesMicro(20_000_000),
                    },
                ],
                &[
                    BookLevel {
                        price: MicroUsdc(550_000),
                        size: SharesMicro(40_000_000),
                    },
                    BookLevel {
                        price: MicroUsdc(510_000),
                        size: SharesMicro(15_000_000),
                    },
                ],
                1_000,
                2_000,
            )
            .unwrap();
        assert_eq!(book.best_bid().unwrap().price, MicroUsdc(490_000));
        assert_eq!(book.best_ask().unwrap().price, MicroUsdc(510_000));
        assert_eq!(book.bids_slice()[1].price, MicroUsdc(480_000));
        assert_eq!(book.asks_slice()[1].price, MicroUsdc(550_000));

        // Zero-size price_change removes top bid `0.49`, exposing `0.48`.
        let updated = bm
            .apply_price_change(
                OrderSide::Buy,
                MicroUsdc(490_000),
                SharesMicro::ZERO,
                1_100,
                2_100,
            )
            .unwrap();
        assert_eq!(updated.best_bid().unwrap().price, MicroUsdc(480_000));

        // Strict `apply_price_change` must reject crossing deltas atomically without poisoning book state.
        let err = bm
            .apply_price_change(
                OrderSide::Buy,
                MicroUsdc(510_000),
                SharesMicro(5_000_000),
                1_200,
                2_200,
            )
            .unwrap_err();
        assert!(matches!(err, FeedError::BookCrossed { .. }));
        assert!(!bm.is_crossed());
        assert_eq!(bm.best_bid().unwrap().price, MicroUsdc(480_000));

        // Atomic `apply_snapshot` also rejects crossed snapshots without clobbering existing book.
        let crossed_snap = bm.apply_snapshot(
            &[BookLevel {
                price: MicroUsdc(550_000),
                size: SharesMicro(1_000_000),
            }],
            &[BookLevel {
                price: MicroUsdc(540_000),
                size: SharesMicro(1_000_000),
            }],
            1_250,
            2_250,
        );
        assert!(matches!(crossed_snap, Err(FeedError::BookCrossed { .. })));
        assert_eq!(bm.best_bid().unwrap().price, MicroUsdc(480_000));
        assert_eq!(bm.best_ask().unwrap().price, MicroUsdc(510_000));

        // Transient sweep delta helper allows crossed state until opposing level clears.
        bm.apply_delta_allow_transient_cross(
            OrderSide::Buy,
            MicroUsdc(510_000),
            SharesMicro(5_000_000),
            1_300,
            2_300,
        )
        .unwrap();
        assert!(bm.is_crossed());
        bm.apply_delta_allow_transient_cross(
            OrderSide::Sell,
            MicroUsdc(510_000),
            SharesMicro::ZERO,
            1_301,
            2_301,
        )
        .unwrap();
        assert!(bm.check_uncrossed().is_ok());
    }

    #[test]
    fn numeric_clob_and_binance_ws_payload_vectors() {
        let mut up = BookMaintainer::new(Outcome::Up);
        let mut dn = BookMaintainer::new(Outcome::Down);

        let book_arr = r#"[{"event_type":"book","asset_id":"UP1","timestamp":"1782753357257","bids":[{"price":"0.47","size":"10"},{"price":"0.49","size":"25"}],"asks":[{"price":"0.54","size":"30"},{"price":"0.51","size":"12"}]}]"#;
        let b1 = parse_clob_ws_text(book_arr, "UP1", "DN1", &mut up, &mut dn, 100).unwrap();
        assert!(b1.warnings.is_empty());
        assert_eq!(b1.events.len(), 1);
        match b1.events[0] {
            PolyMarketEvent::BookSnapshot { outcome, book, .. } => {
                assert_eq!(outcome, Outcome::Up);
                assert_eq!(book.best_bid().unwrap().price, MicroUsdc(490_000));
                assert_eq!(book.best_ask().unwrap().price, MicroUsdc(510_000));
            }
            _ => panic!("expected BookSnapshot"),
        }

        let pc = r#"{"event_type":"price_change","timestamp":"1782753358000","price_changes":[{"asset_id":"UP1","price":"0.50","size":"18","side":"BUY","best_bid":"0.50","best_ask":"0.51"},{"asset_id":"DN1","price":"0.49","size":"22","side":"BUY","best_bid":"0.49","best_ask":"0.50"}]}"#;
        let b2 = parse_clob_ws_text(pc, "UP1", "DN1", &mut up, &mut dn, 200).unwrap();
        assert_eq!(b2.events.len(), 2);

        let lt = r#"{"event_type":"last_trade_price","asset_id":"UP1","price":"0.50","size":"12.5","side":"BUY","timestamp":"1782753358100"}"#;
        assert!(matches!(
            parse_clob_ws_text(lt, "UP1", "DN1", &mut up, &mut dn, 300)
                .unwrap()
                .events[0],
            PolyMarketEvent::LastTrade {
                outcome: Outcome::Up,
                ..
            }
        ));

        let ts = r#"{"event_type":"tick_size_change","asset_id":"DN1","old_tick_size":"0.01","new_tick_size":"0.001","timestamp":"1782753358200"}"#;
        assert!(matches!(
            parse_clob_ws_text(ts, "UP1", "DN1", &mut up, &mut dn, 400)
                .unwrap()
                .events[0],
            PolyMarketEvent::TickSizeChange {
                outcome: Outcome::Down,
                new_tick_micro: MicroUsdc(1_000),
                ..
            }
        ));

        let bt = r#"{"stream":"btcusdt@bookTicker","data":{"u":400900217,"s":"BTCUSDT","b":"91250.00","B":"12.45","a":"91250.10","A":"8.12"}}"#;
        assert!(matches!(
            parse_binance_ws_text(bt, 500).unwrap().unwrap(),
            BinanceSpotEvent::BookTicker {
                bid_e8: 9_125_000_000_000,
                ask_e8: 9_125_010_000_000,
                update_id: 400_900_217,
                ..
            }
        ));

        // Live Binance `@aggTrade` uses `"a"` as integer aggTrade ID while `@bookTicker` uses `"a"` as string price.
        let at = r#"{"stream":"btcusdt@aggTrade","data":{"e":"aggTrade","E":1782753358301,"s":"BTCUSDT","a":4122876520,"p":"91250.55","q":"0.25","f":100,"l":101,"T":1782753358300,"m":true,"M":true}}"#;
        assert!(matches!(
            parse_binance_ws_text(at, 600).unwrap().unwrap(),
            BinanceSpotEvent::AggTrade {
                price_e8: 9_125_055_000_000,
                qty_e8: 25_000_000,
                is_buyer_maker: true,
                ..
            }
        ));
        let raw_at = r#"{"e":"aggTrade","E":1782753358301,"s":"BTCUSDT","a":4122876521,"p":"91251.00","q":"0.50","T":1782753358305,"m":false}"#;
        assert!(matches!(
            parse_binance_ws_text(raw_at, 601).unwrap().unwrap(),
            BinanceSpotEvent::AggTrade {
                price_e8: 9_125_100_000_000,
                qty_e8: 50_000_000,
                is_buyer_maker: false,
                ..
            }
        ));
    }

    #[tokio::test]
    async fn numeric_discovery_v2_and_clock_sync_against_mock_http() {
        let now_ms = 1_795_053_500_000_u64;
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let srv = tokio::spawn(async move {
            for _ in 0..8_usize {
                let Ok((mut sock, _)) = listener.accept().await else {
                    break;
                };
                tokio::spawn(async move {
                    let mut buf = [0_u8; 4096];
                    let n = sock.read(&mut buf).await.unwrap_or(0);
                    let req = String::from_utf8_lossy(&buf[..n]);
                    let path = req
                        .lines()
                        .next()
                        .unwrap_or("")
                        .split_whitespace()
                        .nth(1)
                        .unwrap_or("/");
                    let body = if path.starts_with("/api/v3/time") {
                        r#"{"serverTime":1795053500120}"#.to_string()
                    } else if path.starts_with("/time") {
                        "1795053500".to_string()
                    } else if path.contains("offset=0") && path.contains("tag_slug=5m") {
                        r#"[{"markets":[{"slug":"btc-updown-5m-1795053300","question":"Bitcoin Up or Down","conditionId":"0xcondv2","version":"v2","negRisk":false,"positionIds":["v2_up","v2_dn"],"outcomes":["Up","Down"],"endDate":"2026-11-19T02:00:00Z","eventStartTime":"2026-11-19T01:55:00Z","active":true,"closed":false,"enableOrderBook":true}]}]"#.to_string()
                    } else if path.starts_with("/events") {
                        "[]".to_string()
                    } else if path.starts_with("/book") {
                        r#"{"timestamp":"1795053500100","tick_size":"0.01","min_order_size":"5","neg_risk":false,"version":"v2","bids":[{"price":"0.48","size":"10"}],"asks":[{"price":"0.52","size":"10"}]}"#.to_string()
                    } else if path.starts_with("/clob-markets/") {
                        r#"{"mos":5,"mts":0.01,"itode":true,"fd":{"r":0.05,"e":1,"to":true},"v":"v2"}"#.to_string()
                    } else {
                        "[]".to_string()
                    };
                    let resp = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                        body.len(),
                        body
                    );
                    let _ = sock.write_all(resp.as_bytes()).await;
                });
            }
        });

        let base = format!("http://{addr}");
        let client = GammaDiscoveryClient::with_base_urls(&base, &base, &base).unwrap();
        let est = ClockOffsetEstimator::new(1_000);
        let sync = client.sync_clock(&est).await.unwrap();
        assert_eq!(sync.source, "binance_rest_ms");
        assert!(est.has_sample());

        let desc = client.discover_active_5m_btc(now_ms, 30_000).await.unwrap();
        assert_eq!(desc.version, ProtocolVersion::PolyV2);
        assert_eq!(desc.asset_id_up, "v2_up");
        assert_eq!(desc.asset_id_dn, "v2_dn");
        assert_eq!(desc.fee_schedule.fee_rate_e6, 50_000);
        srv.abort();
    }

    #[tokio::test]
    async fn numeric_polybolt_disabled_degrades_cleanly_and_parses_twap() {
        let engine_cfg = EngineConfig::from_lookup(|_| Ok(None)).unwrap();
        let pb_cfg = PolyBoltWsConfig::from_engine_config(&engine_cfg);
        assert!(!pb_cfg.can_connect());

        let (prod, _cons) = RingBuffer::new(16);
        let (htx, _hrx) = mpsc::channel(8);
        let (_ctx, crx) = mpsc::channel(4);
        let actor = PolyBoltTwapWsActor::new(pb_cfg).unwrap();
        let stats = actor.run(prod, htx, crx).await.unwrap();
        assert_eq!(stats.frames_received, 0);

        let mut seq = PolyBoltSeqTracker::default();
        let spot = r#"{"channel":"price.crypto","seq":1,"ts":1788886175000,"payload":{"value":"95000.25","timestamp":1788886175000}}"#;
        assert!(matches!(
            parse_polybolt_ws_text(spot, 100, &mut seq).unwrap(),
            PolyBoltParsed::Event {
                event: OracleTwapEvent::ChainlinkSpot {
                    spot_e8: 9_500_025_000_000,
                    ..
                },
                warning: None,
            }
        ));
        let twap = r#"{"channel":"price.crypto.twap","seq":2,"ts":1788886176000,"payload":{"data":[{"timestamp":1788886176000,"value":"94950.50"}],"window_seconds":60}}"#;
        assert!(matches!(
            parse_polybolt_ws_text(twap, 200, &mut seq).unwrap(),
            PolyBoltParsed::Event {
                event: OracleTwapEvent::ChainlinkTwap {
                    twap_e8: 9_495_050_000_000,
                    spot_e8: 9_500_025_000_000,
                    ..
                },
                warning: None,
            }
        ));
    }

    #[tokio::test]
    async fn adversarial_stale_feed_watchdog_fires_within_3500ms() {
        let ws_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let ws_addr = ws_listener.local_addr().unwrap();
        let srv = tokio::spawn(async move {
            let (sock, _) = ws_listener.accept().await.unwrap();
            let mut ws = accept_async(sock).await.unwrap();
            let _ = ws.next().await; // read subscribe and stay completely silent
            tokio::time::sleep(Duration::from_secs(6)).await;
        });

        let disc = GammaDiscoveryClient::new().unwrap();
        let mut cfg = ClobWsConfig::new("UP1", "DN1");
        cfg.ws_url = format!("ws://{ws_addr}");
        cfg.stale_timeout = Duration::from_millis(3_100);
        cfg.ping_interval = Duration::from_secs(30);

        let (prod, _cons) = RingBuffer::new(64);
        let (htx, mut hrx) = mpsc::channel(32);
        let (ctx, crx) = mpsc::channel(8);
        let actor = ClobMarketWsActor::new(cfg, disc).unwrap();
        let handle = tokio::spawn(actor.run(prod, htx, crx));

        let t0 = Instant::now();
        let mut stale_fired = false;
        while t0.elapsed() < Duration::from_millis(3_500) {
            if let Ok(Some(ev)) = tokio::time::timeout(Duration::from_millis(200), hrx.recv()).await
                && matches!(ev, FeedHealthEvent::StaleTimeout { silent_ms, .. } if silent_ms >= 3_050)
            {
                stale_fired = true;
                break;
            }
        }
        let _ = ctx.send(ClobWsCommand::Shutdown).await;
        let _ = handle.await;
        srv.abort();
        assert!(stale_fired, "expected StaleTimeout within 3500ms");
    }

    #[tokio::test]
    async fn adversarial_reconnect_rest_resync_blocks_ready_until_books_loaded() {
        let http_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let http_addr = http_listener.local_addr().unwrap();
        let ws_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let ws_addr = ws_listener.local_addr().unwrap();
        let rest_hits = Arc::new(AtomicUsize::new(0));
        let rest_hits_srv = Arc::clone(&rest_hits);

        let http_srv = tokio::spawn(async move {
            loop {
                let Ok((mut sock, _)) = http_listener.accept().await else {
                    break;
                };
                let hits = Arc::clone(&rest_hits_srv);
                tokio::spawn(async move {
                    let mut buf = [0_u8; 2048];
                    let _ = sock.read(&mut buf).await;
                    tokio::time::sleep(Duration::from_millis(120)).await;
                    hits.fetch_add(1, Ordering::SeqCst);
                    let body = r#"{"timestamp":"1782753360000","bids":[{"price":"0.48","size":"15"}],"asks":[{"price":"0.52","size":"15"}]}"#;
                    let resp = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                        body.len(),
                        body
                    );
                    let _ = sock.write_all(resp.as_bytes()).await;
                });
            }
        });

        let ws_srv = tokio::spawn(async move {
            // Connection 1: accept, read subscribe, then drop immediately.
            let (sock1, _) = ws_listener.accept().await.unwrap();
            let mut ws1 = accept_async(sock1).await.unwrap();
            let _ = ws1.next().await;
            let _ = ws1.close(None).await;

            // Connection 2: accept, keep open cleanly.
            let (sock2, _) = ws_listener.accept().await.unwrap();
            let mut ws2 = accept_async(sock2).await.unwrap();
            let _ = ws2.next().await;
            tokio::time::sleep(Duration::from_secs(2)).await;
        });

        let http_base = format!("http://{http_addr}");
        let disc =
            GammaDiscoveryClient::with_base_urls(&http_base, &http_base, &http_base).unwrap();
        let mut cfg = ClobWsConfig::new("UP1", "DN1");
        cfg.ws_url = format!("ws://{ws_addr}");
        cfg.reconnect_backoff = Duration::from_millis(50);

        let (prod, cons) = RingBuffer::new(64);
        let (htx, mut hrx) = mpsc::channel(32);
        let (ctx, crx) = mpsc::channel(8);
        let handle = tokio::spawn(
            ClobMarketWsActor::new(cfg, disc)
                .unwrap()
                .run(prod, htx, crx),
        );

        let mut states: Vec<&'static str> = Vec::new();
        let deadline = Instant::now() + Duration::from_secs(3);
        while Instant::now() < deadline {
            if let Ok(Some(ev)) = tokio::time::timeout(Duration::from_millis(100), hrx.recv()).await
            {
                let tag = match ev {
                    FeedHealthEvent::Connected { .. } => "connected",
                    FeedHealthEvent::Ready { .. } => "ready",
                    FeedHealthEvent::Disconnected { .. } => "disconnected",
                    FeedHealthEvent::ResyncingRest { .. } => "resyncing_rest",
                    _ => continue,
                };
                states.push(tag);
                if tag == "ready" {
                    break;
                }
            }
        }
        let _ = ctx.send(ClobWsCommand::Shutdown).await;
        let _ = handle.await;
        ws_srv.abort();
        http_srv.abort();

        assert_eq!(rest_hits.load(Ordering::SeqCst), 2);
        // Connection #1 closed without snapshots so Ready must appear ONLY after Connection #2 REST resync.
        assert_eq!(states.iter().filter(|s| **s == "ready").count(), 1);
        let pos_resync = states.iter().position(|s| *s == "resyncing_rest").unwrap();
        let pos_ready = states.iter().position(|s| *s == "ready").unwrap();
        assert!(pos_resync < pos_ready);
        assert!(
            cons.slots() >= 2,
            "expected both UP and DOWN REST snapshots pushed"
        );
    }

    #[test]
    fn adversarial_malformed_json_and_unknown_asset_id_never_panic() {
        let mut up = BookMaintainer::new(Outcome::Up);
        let mut dn = BookMaintainer::new(Outcome::Down);

        assert!(parse_clob_ws_text("{bad json", "UP1", "DN1", &mut up, &mut dn, 1).is_err());

        let unknown_type = r#"{"event_type":"unexpected_event","asset_id":"UP1"}"#;
        let r1 = parse_clob_ws_text(unknown_type, "UP1", "DN1", &mut up, &mut dn, 2).unwrap();
        assert_eq!(r1.warnings.len(), 1);
        assert!(r1.events.is_empty());

        let unknown_asset = r#"{"event_type":"price_change","timestamp":"1782753358000","price_changes":[{"asset_id":"999999","price":"0.50","size":"10","side":"BUY"}]}"#;
        let r2 = parse_clob_ws_text(unknown_asset, "UP1", "DN1", &mut up, &mut dn, 3).unwrap();
        assert_eq!(r2.warnings.len(), 1);
        assert!(r2.events.is_empty());

        let binance_bad = "{broken";
        assert!(parse_binance_ws_text(binance_bad, 4).is_err());
    }

    #[tokio::test]
    async fn adversarial_ringbuffer_full_does_not_block_ws_ping_pong() {
        let ws_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let ws_addr = ws_listener.local_addr().unwrap();

        let srv = tokio::spawn(async move {
            let (sock, _) = ws_listener.accept().await.unwrap();
            let mut ws = accept_async(sock).await.unwrap();
            let _ = ws.next().await; // consume subscribe
            for i in 0..16_u64 {
                let ts = 1_782_753_358_000 + i;
                let msg = format!(
                    r#"{{"event_type":"last_trade_price","asset_id":"UP1","price":"0.50","size":"5","side":"BUY","timestamp":"{ts}"}}"#
                );
                let _ = ws.send(Message::Text(msg.into())).await;
            }
            // Wait for client PING and reply PONG to verify event loop never blocked on full ring!
            while let Some(Ok(frame)) = ws.next().await {
                if matches!(frame, Message::Text(ref t) if t.as_str() == "PING") {
                    let _ = ws.send(Message::Text("PONG".into())).await;
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        });

        let disc = GammaDiscoveryClient::new().unwrap();
        let mut cfg = ClobWsConfig::new("UP1", "DN1");
        cfg.ws_url = format!("ws://{ws_addr}");
        cfg.ping_interval = Duration::from_millis(80);
        cfg.stale_timeout = Duration::from_secs(5);

        let (prod, _cons) = RingBuffer::new(4); // intentionally never drained!
        let (htx, mut hrx) = mpsc::channel(64);
        let (ctx, crx) = mpsc::channel(4);
        let handle = tokio::spawn(
            ClobMarketWsActor::new(cfg, disc)
                .unwrap()
                .run(prod, htx, crx),
        );

        let mut saw_overflow = false;
        let deadline = Instant::now() + Duration::from_secs(2);
        while Instant::now() < deadline {
            if let Ok(Some(FeedHealthEvent::RingOverflow { .. })) =
                tokio::time::timeout(Duration::from_millis(100), hrx.recv()).await
            {
                saw_overflow = true;
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
        let _ = ctx.send(ClobWsCommand::Shutdown).await;
        let stats = handle.await.unwrap().unwrap();
        srv.abort();

        assert!(saw_overflow);
        assert!(stats.ring_overflows >= 12);
        assert!(
            stats.pongs_received >= 1,
            "WS ping/pong must stay responsive under full ring"
        );
    }

    #[tokio::test]
    async fn adversarial_polybolt_close_4003_and_seq_gap() {
        let mut seq = PolyBoltSeqTracker::default();
        let f1 =
            r#"{"channel":"price.crypto.twap","seq":10,"ts":1000,"payload":{"value":"95000.00"}}"#;
        let f3 = r#"{"channel":"price.crypto.twap","seq":12,"dropped":1,"ts":2000,"payload":{"value":"95005.00"}}"#;
        assert!(matches!(
            parse_polybolt_ws_text(f1, 10, &mut seq).unwrap(),
            PolyBoltParsed::Event { warning: None, .. }
        ));
        assert!(matches!(
            parse_polybolt_ws_text(f3, 20, &mut seq).unwrap(),
            PolyBoltParsed::Event {
                warning: Some(_),
                ..
            }
        ));

        let ws_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let ws_addr = ws_listener.local_addr().unwrap();
        let srv = tokio::spawn(async move {
            let (sock, _) = ws_listener.accept().await.unwrap();
            let mut ws = accept_async(sock).await.unwrap();
            let _ = ws.next().await; // auth
            let _ = ws
                .send(Message::Text(r#"{"op":"authed","rid":"a1"}"#.into()))
                .await;
            let _ = ws.next().await; // subscribe
            let _ = ws
                .close(Some(CloseFrame {
                    code: CloseCode::Library(POLYBOLT_CLOSE_DRAINING),
                    reason: "draining".into(),
                }))
                .await;
            let (sock2, _) = ws_listener.accept().await.unwrap();
            let mut ws2 = accept_async(sock2).await.unwrap();
            let _ = ws2.next().await;
            let _ = ws2
                .send(Message::Text(r#"{"op":"authed","rid":"a1"}"#.into()))
                .await;
            tokio::time::sleep(Duration::from_secs(1)).await;
        });

        let cfg = PolyBoltWsConfig {
            enabled: true,
            ws_url: format!("ws://{ws_addr}"),
            api_key: Some(SecretString::from("k")),
            api_secret: Some(SecretString::from("s")),
            api_passphrase: Some(SecretString::from("p")),
            chainlink_api_key: Some(SecretString::from("clk")),
            connect_timeout: Duration::from_secs(2),
            stale_timeout: Duration::from_secs(5),
            reconnect_backoff: Duration::from_millis(40),
            drain_jitter_max: Duration::from_millis(20),
        };
        let (prod, _cons) = RingBuffer::new(16);
        let (htx, mut hrx) = mpsc::channel(32);
        let (ctx, crx) = mpsc::channel(4);
        let handle = tokio::spawn(PolyBoltTwapWsActor::new(cfg).unwrap().run(prod, htx, crx));

        let mut saw_4003 = false;
        let deadline = Instant::now() + Duration::from_secs(2);
        while Instant::now() < deadline {
            if let Ok(Some(FeedHealthEvent::Disconnected { reason, .. })) =
                tokio::time::timeout(Duration::from_millis(100), hrx.recv()).await
                && reason.contains("4003")
            {
                saw_4003 = true;
                break;
            }
        }
        let _ = ctx.send(PolyBoltWsCommand::Shutdown).await;
        let stats = handle.await.unwrap().unwrap();
        srv.abort();
        assert!(saw_4003);
        assert!(stats.reconnects >= 1);
    }

    #[test]
    fn numeric_polybolt_live_json_numbers_empty_snapshot_and_errors() {
        let mut seq = PolyBoltSeqTracker::default();
        // Live numeric JSON value + 18-decimal full_accuracy_value.
        let live_num = r#"{"v":1,"channel":"price.crypto","seq":1,"ts":1788973001000,"payload":{"symbol":"btcusd","value":64126.0,"full_accuracy_value":"64126.123456789012345678","timestamp":1788973001000,"source":"chainlink"}}"#;
        assert!(matches!(
            parse_polybolt_ws_text(live_num, 100, &mut seq).unwrap(),
            PolyBoltParsed::Event {
                event: OracleTwapEvent::ChainlinkSpot {
                    spot_e8: 6_412_612_345_678,
                    ..
                },
                warning: None,
            }
        ));

        // Empty snapshot `data: []` returns Control preserving any seq warning cleanly.
        let empty_snap = r#"{"v":1,"channel":"price.crypto.twap","seq":2,"snapshot":true,"ts":1788973002000,"payload":{"symbol":"btcusd","data":[]}}"#;
        assert!(matches!(
            parse_polybolt_ws_text(empty_snap, 110, &mut seq).unwrap(),
            PolyBoltParsed::Control { warning: None }
        ));

        // Numeric JSON in history snapshot array.
        let hist_num = r#"{"v":1,"channel":"price.crypto.twap","seq":3,"snapshot":true,"ts":1788973003000,"payload":{"symbol":"btcusd","data":[{"timestamp":1788973003000,"value":64125.5}]}}"#;
        assert!(matches!(
            parse_polybolt_ws_text(hist_num, 120, &mut seq).unwrap(),
            PolyBoltParsed::Event {
                event: OracleTwapEvent::ChainlinkTwap {
                    twap_e8: 6_412_550_000_000,
                    ..
                },
                warning: None,
            }
        ));

        // Error acknowledgement with code & status 4010.
        let err_ack = r#"{"op":"error","code":"chainlink_key_required","status":4010,"channel":"price.crypto","rid":"s1"}"#;
        assert!(matches!(
            parse_polybolt_ws_text(err_ack, 130, &mut seq).unwrap(),
            PolyBoltParsed::Error {
                code: Some(ref c),
                status: Some(4010),
                ..
            } if c == "chainlink_key_required"
        ));

        // Unknown channel emits explicit ParseWarning rather than silent discard.
        let unknown = r#"{"channel":"price.unknown","seq":1,"ts":1788973004000,"payload":{"value":10.5,"timestamp":1788973004000}}"#;
        assert!(matches!(
            parse_polybolt_ws_text(unknown, 140, &mut seq).unwrap(),
            PolyBoltParsed::Control { warning: Some(_) }
        ));

        // Omitted value + omitted data (unlike explicit `data: []`) must fail loudly rather than silent Control.
        let missing_payload_vals = r#"{"channel":"price.crypto","seq":2,"ts":1788973005000,"payload":{"symbol":"btcusd"}}"#;
        assert!(matches!(
            parse_polybolt_ws_text(missing_payload_vals, 150, &mut seq),
            Err(FeedError::WebSocket {
                feed: "polybolt",
                ..
            })
        ));
    }

    #[tokio::test]
    async fn adversarial_polybolt_enabled_without_creds_fails_explicitly() {
        let mut engine_cfg = EngineConfig::from_lookup(|_| Ok(None)).unwrap();
        engine_cfg.polybolt_enabled = true;
        let pb_cfg = PolyBoltWsConfig::from_engine_config(&engine_cfg);
        assert!(!pb_cfg.can_connect());

        let (prod, _cons) = RingBuffer::new(16);
        let (htx, mut hrx) = mpsc::channel(8);
        let (_ctx, crx) = mpsc::channel(4);
        let res = PolyBoltTwapWsActor::new(pb_cfg)
            .unwrap()
            .run(prod, htx, crx)
            .await;
        assert!(matches!(
            res,
            Err(FeedError::WebSocket {
                feed: "polybolt",
                ..
            })
        ));
        assert!(matches!(
            hrx.try_recv(),
            Ok(FeedHealthEvent::Disconnected {
                feed: FeedId::PolyBolt,
                ..
            })
        ));

        // Server close 4001 must fail explicitly with Err(FeedError::WebSocket) instead of exiting Ok.
        let ws_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let ws_addr = ws_listener.local_addr().unwrap();
        let srv = tokio::spawn(async move {
            let (sock, _) = ws_listener.accept().await.unwrap();
            let mut ws = accept_async(sock).await.unwrap();
            let _ = ws.next().await;
            let _ = ws
                .close(Some(CloseFrame {
                    code: CloseCode::Library(POLYBOLT_CLOSE_AUTH_FAILED),
                    reason: "auth failed".into(),
                }))
                .await;
        });
        let cfg_4001 = PolyBoltWsConfig {
            enabled: true,
            ws_url: format!("ws://{ws_addr}"),
            api_key: Some(SecretString::from("k")),
            api_secret: Some(SecretString::from("s")),
            api_passphrase: Some(SecretString::from("p")),
            chainlink_api_key: Some(SecretString::from("clk")),
            connect_timeout: Duration::from_secs(2),
            stale_timeout: Duration::from_secs(5),
            reconnect_backoff: Duration::from_millis(20),
            drain_jitter_max: Duration::from_millis(10),
        };
        let (prod2, _cons2) = RingBuffer::new(8);
        let (htx2, _hrx2) = mpsc::channel(8);
        let (_ctx2, crx2) = mpsc::channel(4);
        let res2 = PolyBoltTwapWsActor::new(cfg_4001)
            .unwrap()
            .run(prod2, htx2, crx2)
            .await;
        srv.abort();
        assert!(matches!(
            res2,
            Err(FeedError::WebSocket {
                feed: "polybolt",
                ..
            })
        ));
    }

    #[test]
    fn adversarial_gamma_v1_v2_strict_and_clob_sweep_no_drop() {
        // Explicit "v2" missing positionIds must NOT fall back silently to clobTokenIds.
        let bad_v2 = parse_gamma_market_tokens(
            Some("v2"),
            &["Up".to_string(), "Down".to_string()],
            &["v1_a".to_string(), "v1_b".to_string()],
            &[],
        );
        assert!(matches!(bad_v2, Err(FeedError::InvalidMarket { .. })));

        // Unsupported version strings must fail closed rather than defaulting to CtfV1.
        let bad_v3 = parse_gamma_market_tokens(
            Some("v3"),
            &["Up".to_string(), "Down".to_string()],
            &["a".to_string(), "b".to_string()],
            &[],
        );
        assert!(matches!(bad_v3, Err(FeedError::InvalidMarket { .. })));

        // CLOB sweep: bid=0.52 crosses ask=0.50 in item 0, ask=0.50 size=0 uncrosses in item 1 -> ZERO dropped events!
        let mut up = BookMaintainer::new(Outcome::Up);
        let mut dn = BookMaintainer::new(Outcome::Down);
        let snap = r#"{"event_type":"book","asset_id":"UP1","timestamp":"1782753357000","bids":[{"price":"0.48","size":"10"}],"asks":[{"price":"0.50","size":"10"}]}"#;
        parse_clob_ws_text(snap, "UP1", "DN1", &mut up, &mut dn, 100).unwrap();

        let sweep = r#"{"event_type":"price_change","timestamp":"1782753358000","price_changes":[{"asset_id":"UP1","price":"0.52","size":"15","side":"BUY"},{"asset_id":"UP1","price":"0.50","size":"0","side":"SELL"}]}"#;
        let out = parse_clob_ws_text(sweep, "UP1", "DN1", &mut up, &mut dn, 200).unwrap();
        assert_eq!(out.events.len(), 2);
        assert!(out.warnings.is_empty());
        assert!(!up.is_crossed());
        assert_eq!(up.best_bid().unwrap().price, MicroUsdc(520_000));
    }

    #[tokio::test]
    async fn adversarial_polybolt_zero_price_and_error_status_4001() {
        let mut seq = PolyBoltSeqTracker::default();
        let zero_spot = r#"{"channel":"price.crypto","seq":1,"ts":1000,"payload":{"value":"0"}}"#;
        assert!(matches!(
            parse_polybolt_ws_text(zero_spot, 10, &mut seq),
            Err(FeedError::WebSocket {
                feed: "polybolt",
                ..
            })
        ));
        let zero_twap =
            r#"{"channel":"price.crypto.twap","seq":1,"ts":1000,"payload":{"value":0}}"#;
        assert!(matches!(
            parse_polybolt_ws_text(zero_twap, 20, &mut seq),
            Err(FeedError::WebSocket {
                feed: "polybolt",
                ..
            })
        ));

        let ws_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let ws_addr = ws_listener.local_addr().unwrap();
        let srv = tokio::spawn(async move {
            let (sock, _) = ws_listener.accept().await.unwrap();
            let mut ws = accept_async(sock).await.unwrap();
            let _ = ws.next().await; // auth
            let _ = ws
                .send(Message::Text(
                    r#"{"op":"error","status":4001,"code":"auth_failed"}"#.into(),
                ))
                .await;
            tokio::time::sleep(Duration::from_millis(100)).await;
        });

        let cfg = PolyBoltWsConfig {
            enabled: true,
            ws_url: format!("ws://{ws_addr}"),
            api_key: Some(SecretString::from("k")),
            api_secret: Some(SecretString::from("s")),
            api_passphrase: Some(SecretString::from("p")),
            chainlink_api_key: Some(SecretString::from("clk")),
            connect_timeout: Duration::from_secs(2),
            stale_timeout: Duration::from_secs(5),
            reconnect_backoff: Duration::from_millis(20),
            drain_jitter_max: Duration::from_millis(10),
        };
        let (prod, _cons) = RingBuffer::new(8);
        let (htx, _hrx) = mpsc::channel(8);
        let (_ctx, crx) = mpsc::channel(4);
        let res = PolyBoltTwapWsActor::new(cfg)
            .unwrap()
            .run(prod, htx, crx)
            .await;
        srv.abort();
        assert!(matches!(
            res,
            Err(FeedError::WebSocket {
                feed: "polybolt",
                ..
            })
        ));
    }

    #[tokio::test]
    async fn clob_rollover_acknowledges_and_stamps_new_epoch_events() {
        let http_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let http_addr = http_listener.local_addr().unwrap();
        let ws_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let ws_addr = ws_listener.local_addr().unwrap();

        let http_srv = tokio::spawn(async move {
            loop {
                let Ok((mut sock, _)) = http_listener.accept().await else {
                    break;
                };
                tokio::spawn(async move {
                    let mut buf = [0_u8; 2048];
                    let _ = sock.read(&mut buf).await;
                    let body = r#"{"timestamp":"1782753360000","bids":[{"price":"0.48","size":"15"}],"asks":[{"price":"0.52","size":"15"}]}"#;
                    let resp = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                        body.len(),
                        body
                    );
                    let _ = sock.write_all(resp.as_bytes()).await;
                });
            }
        });

        let ws_srv = tokio::spawn(async move {
            let (sock, _) = ws_listener.accept().await.unwrap();
            let mut ws = accept_async(sock).await.unwrap();
            let _ = ws.next().await; // initial subscribe UP1/DN1
            let _ = ws
                .send(Message::Text(
                    r#"{"event_type":"book","asset_id":"UP1","timestamp":"1782753357000","bids":[{"price":"0.40","size":"10"}],"asks":[{"price":"0.42","size":"10"}]}"#.into(),
                ))
                .await;
            let _ = ws.next().await; // rollover unsubscribe UP1/DN1
            let _ = ws.next().await; // rollover subscribe UP2/DN2
            let _ = ws
                .send(Message::Text(
                    r#"{"event_type":"book","asset_id":"UP2","timestamp":"1782753361000","bids":[{"price":"0.49","size":"10"}],"asks":[{"price":"0.51","size":"10"}]}"#.into(),
                ))
                .await;
            tokio::time::sleep(Duration::from_secs(2)).await;
        });

        let http_base = format!("http://{http_addr}");
        let disc =
            GammaDiscoveryClient::with_base_urls(&http_base, &http_base, &http_base).unwrap();
        let mut cfg = ClobWsConfig::new("UP1", "DN1");
        cfg.ws_url = format!("ws://{ws_addr}");
        cfg.ping_interval = Duration::from_secs(30);
        cfg.stale_timeout = Duration::from_secs(10);

        let (prod, mut cons) = RingBuffer::new(64);
        let (htx, _hrx) = mpsc::channel(32);
        let (ctx, crx) = mpsc::channel(8);
        let actor = ClobMarketWsActor::new(cfg, disc).unwrap();
        let handle = tokio::spawn(actor.run(prod, htx, crx));

        // Wait for epoch-0 event from UP1.
        let mut saw_epoch0 = false;
        let t0 = Instant::now();
        while t0.elapsed() < Duration::from_secs(2) {
            if let Ok(ev) = cons.pop() {
                assert_eq!(ev.epoch(), 0);
                saw_epoch0 = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(saw_epoch0, "expected epoch 0 snapshot before rollover");

        // Trigger Rollover and await ack with new epoch = 1.
        let (ack_tx, ack_rx) = tokio::sync::oneshot::channel();
        ctx.send(ClobWsCommand::Rollover {
            new_asset_id_up: "UP2".to_string(),
            new_asset_id_dn: "DN2".to_string(),
            ack: Some(ack_tx),
        })
        .await
        .unwrap();
        let new_epoch = tokio::time::timeout(Duration::from_secs(2), ack_rx)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(new_epoch, 1);

        // Wait for epoch-1 events from UP2/DN2 (REST resync + WS frame).
        let mut saw_epoch1 = false;
        let t1 = Instant::now();
        while t1.elapsed() < Duration::from_secs(2) {
            while let Ok(ev) = cons.pop() {
                if ev.epoch() == 1 {
                    saw_epoch1 = true;
                }
            }
            if saw_epoch1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(saw_epoch1, "expected epoch 1 event after rollover");

        let _ = ctx.send(ClobWsCommand::Shutdown).await;
        let _ = handle.await;
        ws_srv.abort();
        http_srv.abort();
    }
}
