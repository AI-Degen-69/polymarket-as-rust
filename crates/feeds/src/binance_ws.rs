use std::time::{Duration, Instant};

use core_types::{LatencyTracker, MonotonicClock};
use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

use crate::error::FeedError;
use crate::events::{BinanceSpotEvent, FeedHealthEvent, FeedId, FeedStats};
use crate::parse::parse_decimal_e8;
use crate::ws_common::{
    ConnectStep, HealthThrottle, check_stale_watchdog, connect_ws_bounded, emit_disconnect,
    emit_health_transition, empty_stats, finalize_stats, now_pair, push_ring_event,
    record_parse_warning, sleep_until_cancel,
};

/// Default Binance Spot combined WebSocket stream URL (`@bookTicker` + `@aggTrade`).
pub const DEFAULT_BINANCE_WS_URL: &str =
    "wss://stream.binance.com:9443/stream?streams=btcusdt@bookTicker/btcusdt@aggTrade";

type WsWriter = SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>;
type WsReader = SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>;

/// Configuration for `BinanceSpotWsActor`.
#[derive(Debug, Clone)]
pub struct BinanceWsConfig {
    /// WebSocket stream URL (`wss://stream.binance.com:9443/stream?...` default).
    pub ws_url: String,
    /// Bounded TCP/TLS/WebSocket connect timeout (`5s` default).
    pub connect_timeout: Duration,
    /// Stale feed watchdog threshold (`2,000ms` default per Register R8).
    pub stale_timeout: Duration,
    /// Proactive connection rotation before Binance's 24h connection cutoff (`23h` default).
    pub rolling_reconnect: Duration,
    /// Delay before attempting reconnect (`500ms` default).
    pub reconnect_backoff: Duration,
}

impl Default for BinanceWsConfig {
    fn default() -> Self {
        Self {
            ws_url: DEFAULT_BINANCE_WS_URL.to_string(),
            connect_timeout: Duration::from_secs(5),
            stale_timeout: Duration::from_millis(2_000),
            rolling_reconnect: Duration::from_secs(23 * 3_600),
            reconnect_backoff: Duration::from_millis(500),
        }
    }
}

/// Control commands sent to `BinanceSpotWsActor`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinanceWsCommand {
    /// Graceful shutdown request.
    Shutdown,
}

/// Binance Spot combined WebSocket actor feeding `rtrb::Producer<BinanceSpotEvent>`.
#[derive(Debug)]
pub struct BinanceSpotWsActor {
    config: BinanceWsConfig,
    stats: FeedStats,
    tracker: LatencyTracker,
    clock: MonotonicClock,
    throttle: HealthThrottle,
}

impl BinanceSpotWsActor {
    /// Creates a new Binance Spot WebSocket actor.
    pub fn new(config: BinanceWsConfig) -> Result<Self, FeedError> {
        Ok(Self {
            config,
            stats: empty_stats(FeedId::BinanceSpot),
            tracker: LatencyTracker::new()?,
            clock: MonotonicClock::start(),
            throttle: HealthThrottle::default(),
        })
    }

    /// Overrides the monotonic clock so all actors share one process-wide `t=0` origin.
    #[must_use]
    pub const fn with_clock(mut self, clock: MonotonicClock) -> Self {
        self.clock = clock;
        self
    }

    /// Runs the Binance WebSocket stream, RFC 6455 Pong responder, and 23h rollover loop until `Shutdown`.
    pub async fn run(
        mut self,
        mut producer: rtrb::Producer<BinanceSpotEvent>,
        health_tx: mpsc::Sender<FeedHealthEvent>,
        mut cmd_rx: mpsc::Receiver<BinanceWsCommand>,
    ) -> Result<FeedStats, FeedError> {
        crate::install_tls_provider();

        loop {
            let exit = self
                .run_one_session(&mut producer, &health_tx, &mut cmd_rx)
                .await?;
            if exit == SessionExit::Shutdown {
                break;
            }
            self.stats.reconnects = self.stats.reconnects.saturating_add(1);
            let cancelled = sleep_until_cancel(self.config.reconnect_backoff, async {
                while let Some(cmd) = cmd_rx.recv().await {
                    if matches!(cmd, BinanceWsCommand::Shutdown) {
                        break;
                    }
                }
            })
            .await;
            if cancelled {
                break;
            }
        }
        Ok(finalize_stats(self.stats, &self.tracker))
    }

    async fn run_one_session(
        &mut self,
        producer: &mut rtrb::Producer<BinanceSpotEvent>,
        health_tx: &mpsc::Sender<FeedHealthEvent>,
        cmd_rx: &mut mpsc::Receiver<BinanceWsCommand>,
    ) -> Result<SessionExit, FeedError> {
        let ws_stream = match connect_ws_bounded(
            &self.config.ws_url,
            self.config.connect_timeout,
            cmd_rx,
        )
        .await
        {
            ConnectStep::Connected(ws) => *ws,
            ConnectStep::Failed(reason) => {
                self.emit_disconnect(health_tx, reason).await?;
                return Ok(SessionExit::Reconnect);
            }
            ConnectStep::Command(None | Some(BinanceWsCommand::Shutdown)) => {
                return Ok(SessionExit::Shutdown);
            }
        };
        let (mut writer, mut reader) = ws_stream.split();
        let (ts_unix_ms, mono_ns) = now_pair(&self.clock)?;
        emit_health_transition(
            health_tx,
            &mut self.stats,
            FeedHealthEvent::Connected {
                feed: FeedId::BinanceSpot,
                ts_unix_ms,
                mono_ns,
            },
        )
        .await;
        emit_health_transition(
            health_tx,
            &mut self.stats,
            FeedHealthEvent::Ready {
                feed: FeedId::BinanceSpot,
                ts_unix_ms,
                mono_ns,
            },
        )
        .await;
        self.pump_frames(&mut writer, &mut reader, producer, health_tx, cmd_rx)
            .await
    }

    async fn pump_frames(
        &mut self,
        writer: &mut WsWriter,
        reader: &mut WsReader,
        producer: &mut rtrb::Producer<BinanceSpotEvent>,
        health_tx: &mpsc::Sender<FeedHealthEvent>,
        cmd_rx: &mut mpsc::Receiver<BinanceWsCommand>,
    ) -> Result<SessionExit, FeedError> {
        let watchdog_step = (self.config.stale_timeout / 10)
            .clamp(Duration::from_millis(50), Duration::from_millis(250));
        let mut watchdog_tick = tokio::time::interval(watchdog_step);
        watchdog_tick.tick().await;
        let rolling_sleep = tokio::time::sleep(self.config.rolling_reconnect);
        tokio::pin!(rolling_sleep);
        let mut last_msg_at = Instant::now();

        loop {
            let step = tokio::select! {
                cmd = cmd_rx.recv() => match cmd {
                    None | Some(BinanceWsCommand::Shutdown) => {
                        let _ = writer.send(Message::Close(None)).await;
                        Some(SessionExit::Shutdown)
                    }
                },
                _ = &mut rolling_sleep => {
                    let _ = writer.send(Message::Close(None)).await;
                    self.emit_disconnect(health_tx, "23h rolling reconnect".to_string()).await?;
                    Some(SessionExit::Reconnect)
                },
                _ = watchdog_tick.tick() => {
                    self.check_watchdog(writer, health_tx, &last_msg_at).await?
                }
                frame = reader.next() => {
                    self.on_ws_frame(frame, writer, producer, health_tx, &mut last_msg_at).await?
                }
            };
            if let Some(exit) = step {
                return Ok(exit);
            }
        }
    }

    async fn check_watchdog(
        &mut self,
        writer: &mut WsWriter,
        health_tx: &mpsc::Sender<FeedHealthEvent>,
        last_msg_at: &Instant,
    ) -> Result<Option<SessionExit>, FeedError> {
        let tripped = check_stale_watchdog(
            FeedId::BinanceSpot,
            self.config.stale_timeout,
            last_msg_at,
            &self.clock,
            &mut self.stats,
            health_tx,
            writer,
        )
        .await?;
        Ok(tripped.then_some(SessionExit::Reconnect))
    }

    async fn on_ws_frame(
        &mut self,
        frame: Option<Result<Message, tokio_tungstenite::tungstenite::Error>>,
        writer: &mut WsWriter,
        producer: &mut rtrb::Producer<BinanceSpotEvent>,
        health_tx: &mpsc::Sender<FeedHealthEvent>,
        last_msg_at: &mut Instant,
    ) -> Result<Option<SessionExit>, FeedError> {
        let msg = match frame {
            None => {
                self.emit_disconnect(health_tx, "stream EOF".to_string())
                    .await?;
                return Ok(Some(SessionExit::Reconnect));
            }
            Some(Err(err)) => {
                self.emit_disconnect(health_tx, format!("WS read error: {err}"))
                    .await?;
                return Ok(Some(SessionExit::Reconnect));
            }
            Some(Ok(msg)) => msg,
        };
        *last_msg_at = Instant::now();
        self.stats.frames_received = self.stats.frames_received.saturating_add(1);
        self.dispatch_ws_message(msg, writer, producer, health_tx)
            .await
    }

    async fn dispatch_ws_message(
        &mut self,
        msg: Message,
        writer: &mut WsWriter,
        producer: &mut rtrb::Producer<BinanceSpotEvent>,
        health_tx: &mpsc::Sender<FeedHealthEvent>,
    ) -> Result<Option<SessionExit>, FeedError> {
        match msg {
            Message::Text(txt) => self.handle_text(&txt, producer, health_tx)?,
            Message::Ping(payload) => {
                self.stats.pings_replied = self.stats.pings_replied.saturating_add(1);
                if let Err(err) = writer.send(Message::Pong(payload)).await {
                    self.emit_disconnect(health_tx, format!("RFC6455 Pong failed: {err}"))
                        .await?;
                    return Ok(Some(SessionExit::Reconnect));
                }
            }
            Message::Close(c) => {
                self.emit_disconnect(health_tx, format!("server close: {c:?}"))
                    .await?;
                return Ok(Some(SessionExit::Reconnect));
            }
            Message::Pong(_) => {
                self.stats.pongs_received = self.stats.pongs_received.saturating_add(1);
            }
            Message::Binary(bin) => {
                record_parse_warning(
                    FeedId::BinanceSpot,
                    format!("unexpected binary WS frame ({} bytes)", bin.len()),
                    &mut self.stats,
                    &mut self.throttle,
                    health_tx,
                    &self.clock,
                )?;
            }
            Message::Frame(_) => {}
        }
        Ok(None)
    }

    fn handle_text(
        &mut self,
        txt: &str,
        producer: &mut rtrb::Producer<BinanceSpotEvent>,
        health_tx: &mpsc::Sender<FeedHealthEvent>,
    ) -> Result<(), FeedError> {
        let t0 = self.clock.now_ns()?;
        let parsed = parse_binance_ws_text(txt, t0);
        let dt_ns = self.clock.now_ns()?.saturating_sub(t0);
        self.tracker.record_ns(dt_ns)?;

        match parsed {
            Ok(Some(ev)) => push_ring_event(
                producer,
                ev,
                FeedId::BinanceSpot,
                &mut self.stats,
                &mut self.throttle,
                health_tx,
                &self.clock,
            )?,
            Ok(None) => record_parse_warning(
                FeedId::BinanceSpot,
                "unrecognized Binance stream message".to_string(),
                &mut self.stats,
                &mut self.throttle,
                health_tx,
                &self.clock,
            )?,
            Err(err) => record_parse_warning(
                FeedId::BinanceSpot,
                err.to_string(),
                &mut self.stats,
                &mut self.throttle,
                health_tx,
                &self.clock,
            )?,
        }
        Ok(())
    }

    async fn emit_disconnect(
        &mut self,
        health_tx: &mpsc::Sender<FeedHealthEvent>,
        reason: String,
    ) -> Result<(), FeedError> {
        emit_disconnect(
            FeedId::BinanceSpot,
            &self.clock,
            &mut self.stats,
            health_tx,
            reason,
        )
        .await
    }
}

/// Pure testable parser for Binance Spot `@bookTicker` and `@aggTrade` messages (combined or raw).
pub fn parse_binance_ws_text(
    text: &str,
    rx_mono_ns: u64,
) -> Result<Option<BinanceSpotEvent>, FeedError> {
    let env: BinanceWireEnvelope<'_> =
        serde_json::from_str(text.trim()).map_err(|source| FeedError::Json {
            context: "binance_ws_frame",
            source,
        })?;
    let stream_hint = env.stream;
    let fields = env.fields();

    if fields.event_type == Some("aggTrade")
        || stream_hint.is_some_and(|s| s.ends_with("@aggTrade"))
    {
        return decode_agg_trade(&fields, rx_mono_ns);
    }
    if fields.b.is_some()
        && fields.a.is_some()
        && fields.bid_qty.is_some()
        && fields.ask_qty.is_some()
    {
        return decode_book_ticker(&fields, rx_mono_ns);
    }
    Ok(None)
}

fn decode_book_ticker(
    f: &BinanceRawFields<'_>,
    rx_mono_ns: u64,
) -> Result<Option<BinanceSpotEvent>, FeedError> {
    let (Some(b), Some(bq), Some(a), Some(aq), Some(u)) = (f.b, f.bid_qty, f.a, f.ask_qty, f.u)
    else {
        return Ok(None);
    };
    let bid_e8 = parse_e8_field("bookTicker.b", b)?;
    let ask_e8 = parse_e8_field("bookTicker.a", a)?;
    if bid_e8 == 0 || ask_e8 < bid_e8 {
        return Err(FeedError::WebSocket {
            feed: "binance_spot",
            detail: format!("invalid bookTicker BBO {bid_e8}/{ask_e8}"),
        });
    }
    Ok(Some(BinanceSpotEvent::BookTicker {
        bid_e8,
        ask_e8,
        bid_qty_e8: parse_e8_field("bookTicker.B", bq)?,
        ask_qty_e8: parse_e8_field("bookTicker.A", aq)?,
        update_id: u,
        rx_mono_ns,
    }))
}

fn decode_agg_trade(
    f: &BinanceRawFields<'_>,
    rx_mono_ns: u64,
) -> Result<Option<BinanceSpotEvent>, FeedError> {
    let (Some(p), Some(q), Some(ts), Some(m)) = (f.p, f.q, f.trade_ts_ms, f.m) else {
        return Ok(None);
    };
    let price_e8 = parse_e8_field("aggTrade.p", p)?;
    if price_e8 == 0 {
        return Err(FeedError::WebSocket {
            feed: "binance_spot",
            detail: "invalid aggTrade price=0".to_string(),
        });
    }
    Ok(Some(BinanceSpotEvent::AggTrade {
        price_e8,
        qty_e8: parse_e8_field("aggTrade.q", q)?,
        is_buyer_maker: m,
        exch_ts_ms: ts,
        rx_mono_ns,
    }))
}

fn parse_e8_field(field: &'static str, raw: &str) -> Result<u64, FeedError> {
    parse_decimal_e8(raw.trim().as_bytes())
        .map_err(|source| FeedError::ParseDecimal { field, source })
}

#[derive(PartialEq, Eq)]
enum SessionExit {
    Reconnect,
    Shutdown,
}

#[derive(Deserialize)]
struct BinanceWireEnvelope<'a> {
    #[serde(default, borrow)]
    stream: Option<&'a str>,
    #[serde(default, borrow)]
    data: Option<BinanceRawFields<'a>>,
    #[serde(default, rename = "e", borrow)]
    event_type: Option<&'a str>,
    #[serde(default)]
    u: Option<u64>,
    #[serde(default, borrow)]
    b: Option<&'a str>,
    #[serde(default, rename = "B", borrow)]
    bid_qty: Option<&'a str>,
    #[serde(default, borrow, deserialize_with = "de_opt_str_or_ignore_int")]
    a: Option<&'a str>,
    #[serde(default, rename = "A", borrow)]
    ask_qty: Option<&'a str>,
    #[serde(default, borrow)]
    p: Option<&'a str>,
    #[serde(default, borrow)]
    q: Option<&'a str>,
    #[serde(default, rename = "T")]
    trade_ts_ms: Option<u64>,
    #[serde(default)]
    m: Option<bool>,
}

impl<'a> BinanceWireEnvelope<'a> {
    fn fields(&self) -> BinanceRawFields<'a> {
        match &self.data {
            Some(inner) => *inner,
            None => BinanceRawFields {
                event_type: self.event_type,
                u: self.u,
                b: self.b,
                bid_qty: self.bid_qty,
                a: self.a,
                ask_qty: self.ask_qty,
                p: self.p,
                q: self.q,
                trade_ts_ms: self.trade_ts_ms,
                m: self.m,
            },
        }
    }
}

#[derive(Clone, Copy, Deserialize)]
struct BinanceRawFields<'a> {
    #[serde(default, rename = "e", borrow)]
    event_type: Option<&'a str>,
    #[serde(default)]
    u: Option<u64>,
    #[serde(default, borrow)]
    b: Option<&'a str>,
    #[serde(default, rename = "B", borrow)]
    bid_qty: Option<&'a str>,
    // `@bookTicker` uses `"a"` as string ask price; `@aggTrade` uses `"a"` as integer aggTrade ID.
    #[serde(default, borrow, deserialize_with = "de_opt_str_or_ignore_int")]
    a: Option<&'a str>,
    #[serde(default, rename = "A", borrow)]
    ask_qty: Option<&'a str>,
    #[serde(default, borrow)]
    p: Option<&'a str>,
    #[serde(default, borrow)]
    q: Option<&'a str>,
    #[serde(default, rename = "T")]
    trade_ts_ms: Option<u64>,
    #[serde(default)]
    m: Option<bool>,
}

fn de_opt_str_or_ignore_int<'de, D>(deserializer: D) -> Result<Option<&'de str>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    struct VisitorImpl;
    impl<'de> serde::de::Visitor<'de> for VisitorImpl {
        type Value = Option<&'de str>;

        fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("a borrowed string, integer ID, or null")
        }

        fn visit_borrowed_str<E: serde::de::Error>(self, v: &'de str) -> Result<Self::Value, E> {
            Ok(Some(v))
        }

        fn visit_u64<E: serde::de::Error>(self, _v: u64) -> Result<Self::Value, E> {
            Ok(None)
        }

        fn visit_i64<E: serde::de::Error>(self, _v: i64) -> Result<Self::Value, E> {
            Ok(None)
        }

        fn visit_none<E: serde::de::Error>(self) -> Result<Self::Value, E> {
            Ok(None)
        }

        fn visit_unit<E: serde::de::Error>(self) -> Result<Self::Value, E> {
            Ok(None)
        }
    }
    deserializer.deserialize_any(VisitorImpl)
}
