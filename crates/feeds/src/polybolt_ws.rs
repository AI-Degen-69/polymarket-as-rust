use std::borrow::Cow;
use std::collections::HashMap;
use std::time::{Duration, Instant};

use core_types::{EngineConfig, LatencyTracker, MonotonicClock};
use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use secrecy::{ExposeSecret, SecretString};
use serde::Deserialize;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

use crate::error::FeedError;
use crate::events::{FeedHealthEvent, FeedId, FeedStats, OracleTwapEvent};
use crate::parse::{
    de_opt_decimal_as_string, de_opt_string_or_u64, de_string_or_u64, parse_decimal_e8_truncating,
};
use crate::ws_common::{
    ConnectStep, HealthThrottle, check_stale_watchdog, close_ws_bounded, connect_ws_bounded,
    emit_disconnect, emit_health_transition, empty_stats, finalize_stats, now_pair,
    push_ring_event, record_parse_warning, sleep_until_cancel,
};

/// Default Polymarket PolyBolt authenticated reference-price WebSocket URL.
pub const DEFAULT_POLYBOLT_WS_URL: &str = "wss://ws-live-v2.polymarket.com/ws";
/// Close code sent by PolyBolt servers when credentials or policies fail permanently.
pub const POLYBOLT_CLOSE_AUTH_FAILED: u16 = 4001;
/// Close code sent by PolyBolt servers when draining connections before maintenance.
pub const POLYBOLT_CLOSE_DRAINING: u16 = 4003;
/// Close code sent by PolyBolt servers when rate/connection policy forbids reconnect.
pub const POLYBOLT_CLOSE_POLICY_VIOLATION: u16 = 4008;

type WsWriter = SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>;
type WsReader = SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>;

/// Configuration for optional `PolyBoltTwapWsActor` (`POLYBOLT_ENABLED=false` default).
#[derive(Clone)]
pub struct PolyBoltWsConfig {
    /// Whether PolyBolt TWAP subscription is enabled.
    pub enabled: bool,
    /// PolyBolt WebSocket URL.
    pub ws_url: String,
    /// CLOB API key required by PolyBolt auth.
    pub api_key: Option<SecretString>,
    /// CLOB API secret required by PolyBolt auth.
    pub api_secret: Option<SecretString>,
    /// CLOB API passphrase required by PolyBolt auth.
    pub api_passphrase: Option<SecretString>,
    /// Institutional Chainlink API key required for `price.crypto` / `price.crypto.twap`.
    pub chainlink_api_key: Option<SecretString>,
    /// Bounded TCP/TLS/WebSocket connect timeout (`5s` default).
    pub connect_timeout: Duration,
    /// Stale inactivity watchdog threshold (`5,000ms` default).
    pub stale_timeout: Duration,
    /// Base reconnect backoff delay (`750ms` default).
    pub reconnect_backoff: Duration,
    /// Maximum uniform jitter added on `4003` connection-drain reconnects (`10s` default per docs).
    pub drain_jitter_max: Duration,
}

impl std::fmt::Debug for PolyBoltWsConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PolyBoltWsConfig")
            .field("enabled", &self.enabled)
            .field("ws_url", &self.ws_url)
            .field("has_api_key", &self.api_key.is_some())
            .field("has_api_secret", &self.api_secret.is_some())
            .field("has_api_passphrase", &self.api_passphrase.is_some())
            .field("has_chainlink_api_key", &self.chainlink_api_key.is_some())
            .field("connect_timeout", &self.connect_timeout)
            .field("stale_timeout", &self.stale_timeout)
            .field("reconnect_backoff", &self.reconnect_backoff)
            .field("drain_jitter_max", &self.drain_jitter_max)
            .finish()
    }
}

impl PolyBoltWsConfig {
    /// Builds `PolyBoltWsConfig` from validated `EngineConfig` without exposing secret fields.
    #[must_use]
    pub fn from_engine_config(cfg: &EngineConfig) -> Self {
        Self {
            enabled: cfg.polybolt_enabled,
            ws_url: DEFAULT_POLYBOLT_WS_URL.to_string(),
            api_key: cfg.credentials.clob_api_key.clone(),
            api_secret: cfg.credentials.clob_api_secret.clone(),
            api_passphrase: cfg.credentials.clob_api_passphrase.clone(),
            chainlink_api_key: cfg.credentials.chainlink_api_key.clone(),
            connect_timeout: Duration::from_secs(5),
            stale_timeout: Duration::from_secs(5),
            reconnect_backoff: Duration::from_millis(750),
            drain_jitter_max: Duration::from_secs(10),
        }
    }

    /// Returns `true` when PolyBolt is enabled and all CLOB + Chainlink auth fields are present.
    #[must_use]
    pub fn can_connect(&self) -> bool {
        self.enabled
            && self.api_key.is_some()
            && self.api_secret.is_some()
            && self.api_passphrase.is_some()
            && self.chainlink_api_key.is_some()
    }
}

/// Control commands sent to `PolyBoltTwapWsActor`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PolyBoltWsCommand {
    /// Graceful shutdown request.
    Shutdown,
}

/// Tracks dense per-channel sequence numbers across frames within a single connection.
#[derive(Debug, Default)]
pub struct PolyBoltSeqTracker {
    last_seq: HashMap<String, u64>,
    last_spot_e8: u64,
}

impl PolyBoltSeqTracker {
    /// Resets per-connection sequence baselines after reconnect.
    pub fn reset(&mut self) {
        self.last_seq.clear();
    }

    fn observe_seq(&mut self, channel: &str, seq: Option<u64>, dropped: u32) -> Option<String> {
        let mut warning = None;
        if let Some(s) = seq {
            if let Some(prev_slot) = self.last_seq.get_mut(channel) {
                let prev = *prev_slot;
                *prev_slot = s;
                if s > prev.saturating_add(1) {
                    warning = Some(format!(
                        "PolyBolt seq gap on `{channel}`: prev={prev} got={s}"
                    ));
                }
            } else {
                self.last_seq.insert(channel.to_string(), s);
            }
        }
        if dropped > 0 && warning.is_none() {
            warning = Some(format!(
                "PolyBolt server reported dropped={dropped} on `{channel}`"
            ));
        }
        warning
    }
}

/// Result of parsing one PolyBolt WebSocket JSON text frame.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PolyBoltParsed {
    /// Authentication succeeded (`op == "authed"`).
    Authed,
    /// Control frame or empty initial history snapshot with optional diagnostic warning.
    Control {
        /// Optional warning if seq gap or empty/unknown metadata occurred.
        warning: Option<String>,
    },
    /// Server reported control/auth error frame.
    Error {
        /// Machine-readable server error code if provided.
        code: Option<String>,
        /// Optional numeric error status (e.g., `4010` `chainlink_key_required`).
        status: Option<u16>,
        /// Human-readable diagnostic summary.
        detail: String,
    },
    /// Parsed oracle spot or 60s TWAP update plus optional seq/drop/timestamp warning.
    Event {
        /// Parsed `Copy` oracle event.
        event: OracleTwapEvent,
        /// Optional sequence-gap, server-drop, or missing-timestamp warning.
        warning: Option<String>,
    },
}

/// Optional PolyBolt Chainlink TWAP WebSocket actor (`POLYBOLT_ENABLED=false` degrades cleanly).
#[derive(Debug)]
pub struct PolyBoltTwapWsActor {
    config: PolyBoltWsConfig,
    stats: FeedStats,
    tracker: LatencyTracker,
    clock: MonotonicClock,
    throttle: HealthThrottle,
    seq_tracker: PolyBoltSeqTracker,
}

impl PolyBoltTwapWsActor {
    /// Creates a new optional PolyBolt actor.
    pub fn new(config: PolyBoltWsConfig) -> Result<Self, FeedError> {
        Ok(Self {
            config,
            stats: empty_stats(FeedId::PolyBolt),
            tracker: LatencyTracker::new()?,
            clock: MonotonicClock::start(),
            throttle: HealthThrottle::default(),
            seq_tracker: PolyBoltSeqTracker::default(),
        })
    }

    /// Overrides the monotonic clock so all actors share one process-wide `t=0` origin.
    #[must_use]
    pub const fn with_clock(mut self, clock: MonotonicClock) -> Self {
        self.clock = clock;
        self
    }

    /// Runs PolyBolt stream if enabled and authenticated; fails closed when enabled with missing credentials.
    pub async fn run(
        mut self,
        mut producer: rtrb::Producer<OracleTwapEvent>,
        health_tx: mpsc::Sender<FeedHealthEvent>,
        mut cmd_rx: mpsc::Receiver<PolyBoltWsCommand>,
    ) -> Result<FeedStats, FeedError> {
        if !self.config.enabled {
            return Ok(self.stats);
        }
        if !self.config.can_connect() {
            let reason =
                "POLYBOLT_ENABLED=true requires CLOB API credentials and CHAINLINK_API_KEY"
                    .to_string();
            self.emit_disconnect(&health_tx, reason.clone()).await?;
            return Err(FeedError::WebSocket {
                feed: "polybolt",
                detail: reason,
            });
        }
        crate::install_tls_provider();

        loop {
            self.seq_tracker.reset();
            let exit = self
                .run_one_session(&mut producer, &health_tx, &mut cmd_rx)
                .await?;
            let delay = match exit {
                SessionExit::Shutdown => break,
                SessionExit::FatalError(detail) => {
                    return Err(FeedError::WebSocket {
                        feed: "polybolt",
                        detail,
                    });
                }
                SessionExit::Reconnect => self.config.reconnect_backoff,
                SessionExit::ReconnectJittered => self.jittered_backoff()?,
            };
            self.stats.reconnects = self.stats.reconnects.saturating_add(1);
            if wait_polybolt_backoff(delay, &mut cmd_rx).await {
                break;
            }
        }
        Ok(finalize_stats(self.stats, &self.tracker))
    }

    fn jittered_backoff(&self) -> Result<Duration, FeedError> {
        let max_ms = u64::try_from(self.config.drain_jitter_max.as_millis()).unwrap_or(10_000);
        let jitter_ms = if max_ms == 0 {
            0
        } else {
            self.clock.now_ns()? % max_ms.saturating_add(1)
        };
        Ok(self.config.reconnect_backoff + Duration::from_millis(jitter_ms))
    }

    async fn run_one_session(
        &mut self,
        producer: &mut rtrb::Producer<OracleTwapEvent>,
        health_tx: &mpsc::Sender<FeedHealthEvent>,
        cmd_rx: &mut mpsc::Receiver<PolyBoltWsCommand>,
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
            ConnectStep::Command(None | Some(PolyBoltWsCommand::Shutdown)) => {
                return Ok(SessionExit::Shutdown);
            }
        };
        let (mut writer, mut reader) = ws_stream.split();
        if let Err(reason) = send_polybolt_auth(&mut writer, &self.config).await {
            self.emit_disconnect(health_tx, reason).await?;
            return Ok(SessionExit::Reconnect);
        }
        let (ts_unix_ms, mono_ns) = now_pair(&self.clock)?;
        emit_health_transition(
            health_tx,
            &mut self.stats,
            FeedHealthEvent::Connected {
                feed: FeedId::PolyBolt,
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
        producer: &mut rtrb::Producer<OracleTwapEvent>,
        health_tx: &mpsc::Sender<FeedHealthEvent>,
        cmd_rx: &mut mpsc::Receiver<PolyBoltWsCommand>,
    ) -> Result<SessionExit, FeedError> {
        let mut last_msg_at = Instant::now();
        let watchdog_step = (self.config.stale_timeout / 10)
            .clamp(Duration::from_millis(50), Duration::from_millis(250));
        let mut watchdog = tokio::time::interval(watchdog_step);
        watchdog.tick().await;

        loop {
            let step = tokio::select! {
                cmd = cmd_rx.recv() => match cmd {
                    None | Some(PolyBoltWsCommand::Shutdown) => {
                        close_ws_bounded(writer).await;
                        Some(SessionExit::Shutdown)
                    }
                },
                _ = watchdog.tick() => {
                    if check_stale_watchdog(
                        FeedId::PolyBolt,
                        self.config.stale_timeout,
                        &last_msg_at,
                        &self.clock,
                        &mut self.stats,
                        health_tx,
                        writer,
                    )
                    .await? {
                        Some(SessionExit::Reconnect)
                    } else {
                        None
                    }
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

    async fn on_ws_frame(
        &mut self,
        frame: Option<Result<Message, tokio_tungstenite::tungstenite::Error>>,
        writer: &mut WsWriter,
        producer: &mut rtrb::Producer<OracleTwapEvent>,
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
        match msg {
            Message::Text(txt) => self.handle_text(&txt, writer, producer, health_tx).await,
            Message::Ping(p) => self.reply_pong(p, writer, health_tx).await,
            Message::Pong(_) => {
                self.stats.pongs_received = self.stats.pongs_received.saturating_add(1);
                Ok(None)
            }
            Message::Close(c) => self.on_close_frame(c, health_tx).await,
            Message::Binary(bin) => {
                self.warn(
                    format!("unexpected binary WS frame ({} bytes)", bin.len()),
                    health_tx,
                )?;
                Ok(None)
            }
            Message::Frame(_) => Ok(None),
        }
    }

    async fn reply_pong(
        &mut self,
        payload: tokio_tungstenite::tungstenite::Bytes,
        writer: &mut WsWriter,
        health_tx: &mpsc::Sender<FeedHealthEvent>,
    ) -> Result<Option<SessionExit>, FeedError> {
        self.stats.pings_replied = self.stats.pings_replied.saturating_add(1);
        if let Err(err) = writer.send(Message::Pong(payload)).await {
            self.emit_disconnect(health_tx, format!("RFC6455 Pong failed: {err}"))
                .await?;
            return Ok(Some(SessionExit::Reconnect));
        }
        Ok(None)
    }

    async fn on_close_frame(
        &mut self,
        close: Option<tokio_tungstenite::tungstenite::protocol::CloseFrame>,
        health_tx: &mpsc::Sender<FeedHealthEvent>,
    ) -> Result<Option<SessionExit>, FeedError> {
        let code = close.as_ref().map(|c| u16::from(c.code));
        match code {
            Some(POLYBOLT_CLOSE_DRAINING) => {
                self.emit_disconnect(health_tx, "PolyBolt close 4003 (draining)".to_string())
                    .await?;
                Ok(Some(SessionExit::ReconnectJittered))
            }
            Some(POLYBOLT_CLOSE_AUTH_FAILED | POLYBOLT_CLOSE_POLICY_VIOLATION) => {
                let c_num = code.unwrap_or(0);
                let reason = format!("PolyBolt permanent close {c_num}: {close:?}");
                self.emit_disconnect(health_tx, reason.clone()).await?;
                Ok(Some(SessionExit::FatalError(reason)))
            }
            _ => {
                self.emit_disconnect(health_tx, format!("server close: {close:?}"))
                    .await?;
                Ok(Some(SessionExit::Reconnect))
            }
        }
    }

    async fn handle_text(
        &mut self,
        txt: &str,
        writer: &mut WsWriter,
        producer: &mut rtrb::Producer<OracleTwapEvent>,
        health_tx: &mpsc::Sender<FeedHealthEvent>,
    ) -> Result<Option<SessionExit>, FeedError> {
        let t0 = self.clock.now_ns()?;
        let parsed = parse_polybolt_ws_text(txt, t0, &mut self.seq_tracker);
        let dt_ns = self.clock.now_ns()?.saturating_sub(t0);
        self.tracker.record_ns(dt_ns)?;
        self.dispatch_parsed(parsed, writer, producer, health_tx)
            .await
    }

    async fn dispatch_parsed(
        &mut self,
        parsed: Result<PolyBoltParsed, FeedError>,
        writer: &mut WsWriter,
        producer: &mut rtrb::Producer<OracleTwapEvent>,
        health_tx: &mpsc::Sender<FeedHealthEvent>,
    ) -> Result<Option<SessionExit>, FeedError> {
        match parsed {
            Ok(PolyBoltParsed::Authed) => self.on_authed(writer, health_tx).await,
            Ok(PolyBoltParsed::Control { warning }) => {
                if let Some(w) = warning {
                    self.warn(w, health_tx)?;
                }
                Ok(None)
            }
            Ok(PolyBoltParsed::Error {
                code,
                status,
                detail,
            }) => {
                self.on_protocol_error(code.as_deref(), status, detail, writer, health_tx)
                    .await
            }
            Ok(PolyBoltParsed::Event { event, warning }) => {
                self.emit_oracle_event(event, warning, producer, health_tx)?;
                Ok(None)
            }
            Err(err) => {
                self.warn(err.to_string(), health_tx)?;
                Ok(None)
            }
        }
    }

    async fn on_protocol_error(
        &mut self,
        code: Option<&str>,
        status: Option<u16>,
        detail: String,
        writer: &mut WsWriter,
        health_tx: &mpsc::Sender<FeedHealthEvent>,
    ) -> Result<Option<SessionExit>, FeedError> {
        close_ws_bounded(writer).await;
        self.warn(detail.clone(), health_tx)?;
        self.emit_disconnect(health_tx, detail.clone()).await?;
        let permanent = matches!(code, Some("auth_invalid" | "chainlink_key_required"))
            || matches!(status, Some(4001 | 4008 | 4010));
        if permanent {
            Ok(Some(SessionExit::FatalError(detail)))
        } else {
            Ok(Some(SessionExit::Reconnect))
        }
    }

    async fn on_authed(
        &mut self,
        writer: &mut WsWriter,
        health_tx: &mpsc::Sender<FeedHealthEvent>,
    ) -> Result<Option<SessionExit>, FeedError> {
        if let Err(reason) = send_polybolt_subscriptions(writer).await {
            self.emit_disconnect(health_tx, reason).await?;
            return Ok(Some(SessionExit::Reconnect));
        }
        let (ts_unix_ms, mono_ns) = now_pair(&self.clock)?;
        emit_health_transition(
            health_tx,
            &mut self.stats,
            FeedHealthEvent::Ready {
                feed: FeedId::PolyBolt,
                ts_unix_ms,
                mono_ns,
            },
        )
        .await;
        Ok(None)
    }

    fn emit_oracle_event(
        &mut self,
        event: OracleTwapEvent,
        warning: Option<String>,
        producer: &mut rtrb::Producer<OracleTwapEvent>,
        health_tx: &mpsc::Sender<FeedHealthEvent>,
    ) -> Result<(), FeedError> {
        if let Some(w) = warning {
            self.warn(w, health_tx)?;
        }
        push_ring_event(
            producer,
            event,
            FeedId::PolyBolt,
            &mut self.stats,
            &mut self.throttle,
            health_tx,
            &self.clock,
        )
    }

    fn warn(
        &mut self,
        reason: String,
        health_tx: &mpsc::Sender<FeedHealthEvent>,
    ) -> Result<(), FeedError> {
        record_parse_warning(
            FeedId::PolyBolt,
            reason,
            &mut self.stats,
            &mut self.throttle,
            health_tx,
            &self.clock,
        )
    }

    async fn emit_disconnect(
        &mut self,
        health_tx: &mpsc::Sender<FeedHealthEvent>,
        reason: String,
    ) -> Result<(), FeedError> {
        emit_disconnect(
            FeedId::PolyBolt,
            &self.clock,
            &mut self.stats,
            health_tx,
            reason,
        )
        .await
    }
}

/// Pure testable parser for Polymarket PolyBolt `price.crypto` and `price.crypto.twap` messages.
pub fn parse_polybolt_ws_text(
    text: &str,
    rx_mono_ns: u64,
    seq_tracker: &mut PolyBoltSeqTracker,
) -> Result<PolyBoltParsed, FeedError> {
    let dto: PolyBoltFrameDto<'_> =
        serde_json::from_str(text.trim()).map_err(|source| FeedError::Json {
            context: "polybolt_ws_frame",
            source,
        })?;
    if let Some(op) = dto.op.as_deref() {
        return Ok(parse_control_op(op, &dto));
    }

    let channel = dto.channel.as_deref().unwrap_or_default();
    let dropped = dto.dropped.unwrap_or(0);
    let mut warning = seq_tracker.observe_seq(channel, dto.seq, dropped);
    let Some(payload) = dto.payload else {
        if warning.is_none() {
            warning = Some("PolyBolt non-control frame missing payload".to_string());
        }
        return Ok(PolyBoltParsed::Control { warning });
    };
    let Some((val_str, obs_ts)) = extract_value_and_ts(&payload, dto.ts)? else {
        return Ok(PolyBoltParsed::Control { warning });
    };
    if obs_ts.is_none() || dto.seq.is_none() {
        let extra = "PolyBolt data envelope missing ts or seq";
        warning = Some(warning.map_or_else(|| extra.to_string(), |w| format!("{w}; {extra}")));
    }
    let val_e8 = parse_decimal_e8_truncating(val_str.trim().as_bytes()).map_err(|source| {
        FeedError::ParseDecimal {
            field: "polybolt.value",
            source,
        }
    })?;
    build_oracle_event(
        channel,
        val_e8,
        (
            obs_ts.unwrap_or(0),
            dto.seq.unwrap_or(0),
            dropped,
            rx_mono_ns,
        ),
        warning,
        seq_tracker,
    )
}

fn parse_control_op(op: &str, dto: &PolyBoltFrameDto<'_>) -> PolyBoltParsed {
    match op {
        "authed" => PolyBoltParsed::Authed,
        "subscribed" | "unsubscribed" | "pong" => PolyBoltParsed::Control { warning: None },
        "error" => {
            let code_txt = dto.code.as_deref().unwrap_or("unknown_error");
            let detail = match (dto.status, dto.message.as_deref()) {
                (Some(st), Some(msg)) => {
                    format!("PolyBolt error code={code_txt} status={st}: {msg}")
                }
                (Some(st), None) => format!("PolyBolt error code={code_txt} status={st}"),
                (None, Some(msg)) => format!("PolyBolt error code={code_txt}: {msg}"),
                (None, None) => format!("PolyBolt error code={code_txt}"),
            };
            PolyBoltParsed::Error {
                code: dto.code.as_deref().map(str::to_string),
                status: dto.status,
                detail,
            }
        }
        other => PolyBoltParsed::Control {
            warning: Some(format!("unrecognized PolyBolt control op `{other}`")),
        },
    }
}

fn build_oracle_event(
    channel: &str,
    val_e8: u64,
    (exch_ts_ms, seq, dropped, rx_mono_ns): (u64, u64, u32, u64),
    warning: Option<String>,
    seq_tracker: &mut PolyBoltSeqTracker,
) -> Result<PolyBoltParsed, FeedError> {
    if val_e8 == 0 {
        return Err(FeedError::WebSocket {
            feed: "polybolt",
            detail: format!("non-positive oracle price `0` on channel `{channel}`"),
        });
    }
    match channel {
        "price.crypto" => {
            seq_tracker.last_spot_e8 = val_e8;
            Ok(PolyBoltParsed::Event {
                event: OracleTwapEvent::ChainlinkSpot {
                    spot_e8: val_e8,
                    seq,
                    dropped,
                    exch_ts_ms,
                    rx_mono_ns,
                },
                warning,
            })
        }
        "price.crypto.twap" => Ok(PolyBoltParsed::Event {
            event: OracleTwapEvent::ChainlinkTwap {
                twap_e8: val_e8,
                spot_e8: seq_tracker.last_spot_e8,
                seq,
                dropped,
                exch_ts_ms,
                rx_mono_ns,
            },
            warning,
        }),
        _ => Ok(PolyBoltParsed::Control {
            warning: Some(format!("unrecognized PolyBolt channel `{channel}`")),
        }),
    }
}

fn extract_value_and_ts<'a>(
    payload: &'a PolyBoltPayloadDto<'a>,
    env_ts: Option<u64>,
) -> Result<Option<(&'a str, Option<u64>)>, FeedError> {
    if let Some(v) = payload
        .full_accuracy_value
        .as_deref()
        .or(payload.value.as_deref())
    {
        return Ok(Some((v, payload.timestamp.or(env_ts))));
    }
    if let Some(points) = &payload.data {
        if let Some(last) = points.last() {
            let Some(v) = last
                .full_accuracy_value
                .as_deref()
                .or(last.value.as_deref())
            else {
                return Err(FeedError::WebSocket {
                    feed: "polybolt",
                    detail: "history snapshot point missing value and full_accuracy_value"
                        .to_string(),
                });
            };
            return Ok(Some((v, Some(last.timestamp))));
        }
        return Ok(None);
    }
    Err(FeedError::WebSocket {
        feed: "polybolt",
        detail: "missing value and history snapshot points in payload".to_string(),
    })
}

async fn send_polybolt_auth(writer: &mut WsWriter, cfg: &PolyBoltWsConfig) -> Result<(), String> {
    let (Some(key), Some(secret), Some(pass), Some(chainlink)) = (
        &cfg.api_key,
        &cfg.api_secret,
        &cfg.api_passphrase,
        &cfg.chainlink_api_key,
    ) else {
        return Err("missing PolyBolt CLOB or Chainlink credentials".to_string());
    };
    let msg = serde_json::json!({
        "op": "auth",
        "rid": "a1",
        "auth": {
            "apiKey": key.expose_secret(),
            "secret": secret.expose_secret(),
            "passphrase": pass.expose_secret(),
        },
        "chainlink_api_key": chainlink.expose_secret(),
    });
    writer
        .send(Message::Text(msg.to_string().into()))
        .await
        .map_err(|e| format!("PolyBolt auth send failed: {e}"))
}

async fn send_polybolt_subscriptions(writer: &mut WsWriter) -> Result<(), String> {
    let msg = serde_json::json!({
        "op": "subscribe",
        "rid": "s1",
        "subscriptions": [
            {
                "channel": "price.crypto",
                "filter": { "symbol": "btcusd", "provider": "chainlink" }
            },
            {
                "channel": "price.crypto.twap",
                "filter": { "symbol": "btcusd", "window_seconds": 60, "provider": "chainlink" }
            }
        ]
    });
    writer
        .send(Message::Text(msg.to_string().into()))
        .await
        .map_err(|e| format!("PolyBolt subscribe send failed: {e}"))
}

async fn wait_polybolt_backoff(
    delay: Duration,
    cmd_rx: &mut mpsc::Receiver<PolyBoltWsCommand>,
) -> bool {
    sleep_until_cancel(delay, async {
        while let Some(cmd) = cmd_rx.recv().await {
            if matches!(cmd, PolyBoltWsCommand::Shutdown) {
                break;
            }
        }
    })
    .await
}

#[derive(PartialEq, Eq)]
enum SessionExit {
    Reconnect,
    ReconnectJittered,
    Shutdown,
    FatalError(String),
}

#[derive(Deserialize)]
struct PolyBoltFrameDto<'a> {
    #[serde(default, borrow)]
    op: Option<Cow<'a, str>>,
    #[serde(default, borrow)]
    channel: Option<Cow<'a, str>>,
    #[serde(default)]
    seq: Option<u64>,
    #[serde(default)]
    dropped: Option<u32>,
    #[serde(default, deserialize_with = "de_opt_string_or_u64")]
    ts: Option<u64>,
    #[serde(default, borrow)]
    code: Option<Cow<'a, str>>,
    #[serde(default)]
    status: Option<u16>,
    #[serde(default, borrow)]
    message: Option<Cow<'a, str>>,
    #[serde(default, borrow)]
    payload: Option<PolyBoltPayloadDto<'a>>,
}

#[derive(Deserialize)]
struct PolyBoltPayloadDto<'a> {
    #[serde(default, deserialize_with = "de_opt_decimal_as_string")]
    value: Option<String>,
    #[serde(default, borrow)]
    full_accuracy_value: Option<Cow<'a, str>>,
    #[serde(default, deserialize_with = "de_opt_string_or_u64")]
    timestamp: Option<u64>,
    #[serde(default)]
    data: Option<Vec<PolyBoltHistoryPointDto<'a>>>,
}

#[derive(Deserialize)]
struct PolyBoltHistoryPointDto<'a> {
    #[serde(deserialize_with = "de_string_or_u64")]
    timestamp: u64,
    #[serde(default, deserialize_with = "de_opt_decimal_as_string")]
    value: Option<String>,
    #[serde(default, borrow)]
    full_accuracy_value: Option<Cow<'a, str>>,
}
