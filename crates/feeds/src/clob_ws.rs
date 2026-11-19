use std::borrow::Cow;
use std::time::{Duration, Instant};

use core_types::{BookLevel, LatencyTracker, MicroUsdc, MonotonicClock, Outcome, SharesMicro};
use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

use crate::book_maintainer::BookMaintainer;
use crate::discovery::GammaDiscoveryClient;
use crate::error::FeedError;
use crate::events::{FeedHealthEvent, FeedId, FeedStats, PolyMarketEvent};
use crate::parse::{de_opt_string_or_u64, parse_decimal_micro_flexible, parse_order_side};
use crate::ws_common::{
    ConnectStep, HealthThrottle, check_stale_watchdog, connect_ws_bounded, emit_disconnect,
    emit_health_transition, empty_stats, finalize_stats, now_pair, push_ring_event,
    record_parse_warning,
};

/// Default Polymarket CLOB market WebSocket endpoint.
pub const DEFAULT_CLOB_WS_URL: &str = "wss://ws-subscriptions-clob.polymarket.com/ws/market";

const SUSTAINED_CROSS_RESYNC_MS: u64 = 500;

type WsWriter = SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>;
type WsReader = SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>;

/// Configuration for `ClobMarketWsActor`.
#[derive(Debug, Clone)]
pub struct ClobWsConfig {
    /// WebSocket URL (`wss://ws-subscriptions-clob.polymarket.com/ws/market` default).
    pub ws_url: String,
    /// Initial UP outcome asset ID.
    pub asset_id_up: String,
    /// Initial DOWN outcome asset ID.
    pub asset_id_dn: String,
    /// Bounded TCP/TLS/WebSocket connect timeout (`5s` default).
    pub connect_timeout: Duration,
    /// Client application-level `"PING"` cadence (`10s` default per Polymarket docs).
    pub ping_interval: Duration,
    /// Maximum wait for server `"PONG"` after sending `"PING"` (`15s` default).
    pub pong_timeout: Duration,
    /// Inactivity watchdog threshold triggering `StaleTimeout` + reconnect (`3,000ms` default).
    pub stale_timeout: Duration,
    /// Delay before attempting reconnect after socket closure or failure (`500ms` default).
    pub reconnect_backoff: Duration,
}

impl ClobWsConfig {
    /// Builds default configuration for `asset_id_up` and `asset_id_dn`.
    #[must_use]
    pub fn new(asset_id_up: impl Into<String>, asset_id_dn: impl Into<String>) -> Self {
        Self {
            ws_url: DEFAULT_CLOB_WS_URL.to_string(),
            asset_id_up: asset_id_up.into(),
            asset_id_dn: asset_id_dn.into(),
            connect_timeout: Duration::from_secs(5),
            ping_interval: Duration::from_secs(10),
            pong_timeout: Duration::from_secs(15),
            stale_timeout: Duration::from_millis(3_000),
            reconnect_backoff: Duration::from_millis(500),
        }
    }
}

/// Control commands sent to `ClobMarketWsActor`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClobWsCommand {
    /// Zero-reconnect epoch rollover (`unsubscribe` old pair, `subscribe` new pair).
    Rollover {
        /// Next epoch UP asset ID.
        new_asset_id_up: String,
        /// Next epoch DOWN asset ID.
        new_asset_id_dn: String,
    },
    /// Graceful shutdown request.
    Shutdown,
}

/// Polymarket CLOB WebSocket market stream actor maintaining sorted `Up`/`Down` L2 books.
#[derive(Debug)]
pub struct ClobMarketWsActor {
    config: ClobWsConfig,
    discovery: GammaDiscoveryClient,
    active_up: String,
    active_dn: String,
    book_up: BookMaintainer,
    book_dn: BookMaintainer,
    up_synced: bool,
    dn_synced: bool,
    ready_emitted: bool,
    crossed_since: Option<Instant>,
    stats: FeedStats,
    tracker: LatencyTracker,
    clock: MonotonicClock,
    throttle: HealthThrottle,
    scratch: ClobParseScratch,
}

#[derive(Debug, Default)]
struct ClobParseScratch {
    batch: ClobParseBatch,
    bids: Vec<BookLevel>,
    asks: Vec<BookLevel>,
}

impl ClobMarketWsActor {
    /// Creates a new actor bound to `config` and `discovery` for REST resyncs.
    pub fn new(config: ClobWsConfig, discovery: GammaDiscoveryClient) -> Result<Self, FeedError> {
        let active_up = config.asset_id_up.clone();
        let active_dn = config.asset_id_dn.clone();
        Ok(Self {
            config,
            discovery,
            active_up,
            active_dn,
            book_up: BookMaintainer::new(Outcome::Up),
            book_dn: BookMaintainer::new(Outcome::Down),
            up_synced: false,
            dn_synced: false,
            ready_emitted: false,
            crossed_since: None,
            stats: empty_stats(FeedId::PolymarketClob),
            tracker: LatencyTracker::new()?,
            clock: MonotonicClock::start(),
            throttle: HealthThrottle::default(),
            scratch: ClobParseScratch::default(),
        })
    }

    /// Overrides the monotonic clock so all actors share one process-wide `t=0` origin.
    #[must_use]
    pub const fn with_clock(mut self, clock: MonotonicClock) -> Self {
        self.clock = clock;
        self
    }

    /// Runs WebSocket ingestion, heartbeat, stale watchdog, and REST-resync loop until `Shutdown`.
    pub async fn run(
        mut self,
        mut producer: rtrb::Producer<PolyMarketEvent>,
        health_tx: mpsc::Sender<FeedHealthEvent>,
        mut cmd_rx: mpsc::Receiver<ClobWsCommand>,
    ) -> Result<FeedStats, FeedError> {
        crate::install_tls_provider();
        loop {
            let outcome = self
                .run_one_session(&mut producer, &health_tx, &mut cmd_rx)
                .await?;
            match outcome {
                SessionExit::Shutdown => break,
                SessionExit::ImmediateReconnect => {
                    self.stats.reconnects = self.stats.reconnects.saturating_add(1);
                }
                SessionExit::Reconnect => {
                    self.stats.reconnects = self.stats.reconnects.saturating_add(1);
                    if self.wait_reconnect_cmd(&mut cmd_rx).await {
                        break;
                    }
                }
            }
        }
        Ok(finalize_stats(self.stats, &self.tracker))
    }

    async fn run_one_session(
        &mut self,
        producer: &mut rtrb::Producer<PolyMarketEvent>,
        health_tx: &mpsc::Sender<FeedHealthEvent>,
        cmd_rx: &mut mpsc::Receiver<ClobWsCommand>,
    ) -> Result<SessionExit, FeedError> {
        self.reset_sync_state();
        let ws_stream = match self.connect_session(health_tx, cmd_rx).await? {
            Ok(ws) => ws,
            Err(exit) => return Ok(exit),
        };
        let (mut writer, mut reader) = ws_stream.split();
        if let Err(reason) = send_subscribe(&mut writer, &self.active_up, &self.active_dn).await {
            self.emit_disconnect(health_tx, reason).await?;
            return Ok(SessionExit::Reconnect);
        }
        self.emit_transition(health_tx, true).await?;
        if self.stats.reconnects > 0
            && let Some(exit) = self.resync_rest_books(producer, health_tx, cmd_rx).await?
        {
            return Ok(exit);
        }
        self.pump_session_frames(&mut writer, &mut reader, producer, health_tx, cmd_rx)
            .await
    }

    async fn connect_session(
        &mut self,
        health_tx: &mpsc::Sender<FeedHealthEvent>,
        cmd_rx: &mut mpsc::Receiver<ClobWsCommand>,
    ) -> Result<Result<WebSocketStream<MaybeTlsStream<TcpStream>>, SessionExit>, FeedError> {
        match connect_ws_bounded(&self.config.ws_url, self.config.connect_timeout, cmd_rx).await {
            ConnectStep::Connected(ws) => Ok(Ok(*ws)),
            ConnectStep::Failed(reason) => {
                self.emit_disconnect(health_tx, reason).await?;
                Ok(Err(SessionExit::Reconnect))
            }
            ConnectStep::Command(None | Some(ClobWsCommand::Shutdown)) => {
                Ok(Err(SessionExit::Shutdown))
            }
            ConnectStep::Command(Some(ClobWsCommand::Rollover {
                new_asset_id_up,
                new_asset_id_dn,
            })) => {
                self.commit_rollover(new_asset_id_up, new_asset_id_dn);
                Ok(Err(SessionExit::ImmediateReconnect))
            }
        }
    }

    fn reset_sync_state(&mut self) {
        self.up_synced = false;
        self.dn_synced = false;
        self.ready_emitted = false;
        self.crossed_since = None;
    }

    fn commit_rollover(&mut self, new_up: String, new_dn: String) {
        self.active_up = new_up;
        self.active_dn = new_dn;
        self.book_up = BookMaintainer::new(Outcome::Up);
        self.book_dn = BookMaintainer::new(Outcome::Down);
        self.reset_sync_state();
    }

    async fn emit_transition(
        &mut self,
        health_tx: &mpsc::Sender<FeedHealthEvent>,
        connected_only: bool,
    ) -> Result<(), FeedError> {
        let (ts_unix_ms, mono_ns) = now_pair(&self.clock)?;
        let ev = if connected_only {
            FeedHealthEvent::Connected {
                feed: FeedId::PolymarketClob,
                ts_unix_ms,
                mono_ns,
            }
        } else {
            FeedHealthEvent::Ready {
                feed: FeedId::PolymarketClob,
                ts_unix_ms,
                mono_ns,
            }
        };
        emit_health_transition(health_tx, &mut self.stats, ev).await;
        Ok(())
    }

    async fn maybe_emit_ready(
        &mut self,
        health_tx: &mpsc::Sender<FeedHealthEvent>,
    ) -> Result<(), FeedError> {
        if self.up_synced && self.dn_synced && !self.ready_emitted {
            self.ready_emitted = true;
            self.emit_transition(health_tx, false).await?;
        }
        Ok(())
    }

    async fn resync_rest_books(
        &mut self,
        producer: &mut rtrb::Producer<PolyMarketEvent>,
        health_tx: &mpsc::Sender<FeedHealthEvent>,
        cmd_rx: &mut mpsc::Receiver<ClobWsCommand>,
    ) -> Result<Option<SessionExit>, FeedError> {
        let (ts_unix_ms, mono_ns) = now_pair(&self.clock)?;
        let resync_ev = FeedHealthEvent::ResyncingRest {
            feed: FeedId::PolymarketClob,
            ts_unix_ms,
            mono_ns,
        };
        emit_health_transition(health_tx, &mut self.stats, resync_ev).await;
        self.up_synced = false;
        self.dn_synced = false;
        self.ready_emitted = false;
        let fetched = tokio::select! {
            res = self.fetch_both_rest_books() => res,
            cmd = cmd_rx.recv() => return Ok(Some(self.handle_pending_cmd(cmd))),
        };
        self.apply_rest_resync_pair(fetched, producer, health_tx)
            .await
    }

    async fn fetch_both_rest_books(&self) -> (RestBookResult, RestBookResult) {
        tokio::join!(
            self.discovery.fetch_rest_book(&self.active_up),
            self.discovery.fetch_rest_book(&self.active_dn),
        )
    }

    async fn apply_rest_resync_pair(
        &mut self,
        fetched: (RestBookResult, RestBookResult),
        producer: &mut rtrb::Producer<PolyMarketEvent>,
        health_tx: &mpsc::Sender<FeedHealthEvent>,
    ) -> Result<Option<SessionExit>, FeedError> {
        for (outcome, res) in [(Outcome::Up, fetched.0), (Outcome::Down, fetched.1)] {
            if !self
                .push_rest_book_snapshot(outcome, res, producer, health_tx)
                .await?
            {
                return Ok(Some(SessionExit::Reconnect));
            }
        }
        self.crossed_since = None;
        self.maybe_emit_ready(health_tx).await?;
        Ok(None)
    }

    async fn push_rest_book_snapshot(
        &mut self,
        outcome: Outcome,
        res: RestBookResult,
        producer: &mut rtrb::Producer<PolyMarketEvent>,
        health_tx: &mpsc::Sender<FeedHealthEvent>,
    ) -> Result<bool, FeedError> {
        let (bids, asks, exch_ts_ms) = match res {
            Ok(data) => data,
            Err(err) => {
                self.emit_disconnect(health_tx, format!("REST resync {outcome:?} failed: {err}"))
                    .await?;
                return Ok(false);
            }
        };
        let rx_ns = self.clock.now_ns()?;
        let book_res = match outcome {
            Outcome::Up => self.book_up.apply_snapshot(&bids, &asks, rx_ns, exch_ts_ms),
            Outcome::Down => self.book_dn.apply_snapshot(&bids, &asks, rx_ns, exch_ts_ms),
        };
        let book = match book_res {
            Ok(b) => b,
            Err(err) => {
                self.emit_disconnect(health_tx, format!("REST resync {outcome:?} invalid: {err}"))
                    .await?;
                return Ok(false);
            }
        };
        self.push_resync_event_strict(outcome, book, rx_ns, exch_ts_ms, producer, health_tx)
            .await
    }

    async fn push_resync_event_strict(
        &mut self,
        outcome: Outcome,
        book: core_types::FixedBook<10>,
        rx_mono_ns: u64,
        exch_ts_ms: u64,
        producer: &mut rtrb::Producer<PolyMarketEvent>,
        health_tx: &mpsc::Sender<FeedHealthEvent>,
    ) -> Result<bool, FeedError> {
        let ev = PolyMarketEvent::BookSnapshot {
            outcome,
            book,
            rx_mono_ns,
            exch_ts_ms,
        };
        if producer.push(ev).is_err() {
            self.stats.ring_overflows = self.stats.ring_overflows.saturating_add(1);
            self.emit_disconnect(
                health_tx,
                format!("rtrb full during REST resync snapshot ({outcome:?}); retrying"),
            )
            .await?;
            return Ok(false);
        }
        self.stats.events_pushed = self.stats.events_pushed.saturating_add(1);
        self.mark_outcome_synced(outcome);
        Ok(true)
    }

    fn mark_outcome_synced(&mut self, outcome: Outcome) {
        match outcome {
            Outcome::Up => self.up_synced = true,
            Outcome::Down => self.dn_synced = true,
        }
    }

    async fn pump_session_frames(
        &mut self,
        writer: &mut WsWriter,
        reader: &mut WsReader,
        producer: &mut rtrb::Producer<PolyMarketEvent>,
        health_tx: &mpsc::Sender<FeedHealthEvent>,
        cmd_rx: &mut mpsc::Receiver<ClobWsCommand>,
    ) -> Result<SessionExit, FeedError> {
        let watchdog_step = (self.config.stale_timeout / 10)
            .clamp(Duration::from_millis(50), Duration::from_millis(250));
        let mut ping_tick = tokio::time::interval(self.config.ping_interval);
        ping_tick.tick().await;
        let mut watchdog_tick = tokio::time::interval(watchdog_step);
        watchdog_tick.tick().await;
        let mut io = ClobSessionIo {
            writer,
            producer,
            health_tx,
            cmd_rx,
            last_msg_at: Instant::now(),
            awaiting_pong: None,
        };
        loop {
            let step = tokio::select! {
                cmd = io.cmd_rx.recv() => self.on_command(cmd, &mut io).await?,
                _ = ping_tick.tick() => self.send_app_ping(&mut io).await?,
                _ = watchdog_tick.tick() => self.on_watchdog(&mut io).await?,
                frame = reader.next() => self.on_ws_frame(frame, &mut io).await?,
            };
            if let Some(exit) = step {
                return Ok(exit);
            }
        }
    }

    async fn on_command(
        &mut self,
        cmd: Option<ClobWsCommand>,
        io: &mut ClobSessionIo<'_>,
    ) -> Result<Option<SessionExit>, FeedError> {
        match cmd {
            None | Some(ClobWsCommand::Shutdown) => {
                let _ = io.writer.send(Message::Close(None)).await;
                Ok(Some(SessionExit::Shutdown))
            }
            Some(ClobWsCommand::Rollover {
                new_asset_id_up,
                new_asset_id_dn,
            }) => {
                if let Err(reason) = self
                    .apply_rollover(io.writer, new_asset_id_up, new_asset_id_dn)
                    .await
                {
                    self.emit_disconnect(io.health_tx, reason).await?;
                    return Ok(Some(SessionExit::Reconnect));
                }
                let res = self
                    .resync_rest_books(io.producer, io.health_tx, io.cmd_rx)
                    .await?;
                io.last_msg_at = Instant::now();
                Ok(res)
            }
        }
    }

    async fn send_app_ping(
        &mut self,
        io: &mut ClobSessionIo<'_>,
    ) -> Result<Option<SessionExit>, FeedError> {
        if let Err(err) = io.writer.send(Message::Text("PING".into())).await {
            self.emit_disconnect(io.health_tx, format!("PING send failed: {err}"))
                .await?;
            return Ok(Some(SessionExit::Reconnect));
        }
        io.awaiting_pong.get_or_insert_with(Instant::now);
        Ok(None)
    }

    async fn on_watchdog(
        &mut self,
        io: &mut ClobSessionIo<'_>,
    ) -> Result<Option<SessionExit>, FeedError> {
        if let Some(sent_at) = io.awaiting_pong
            && sent_at.elapsed() >= self.config.pong_timeout
        {
            let _ = io.writer.send(Message::Close(None)).await;
            let reason = format!("PONG timeout after {}ms", sent_at.elapsed().as_millis());
            self.emit_disconnect(io.health_tx, reason).await?;
            return Ok(Some(SessionExit::Reconnect));
        }
        if self.check_sustained_cross(io.health_tx)? {
            let res = self
                .resync_rest_books(io.producer, io.health_tx, io.cmd_rx)
                .await?;
            io.last_msg_at = Instant::now();
            return Ok(res);
        }
        let stale = check_stale_watchdog(
            FeedId::PolymarketClob,
            self.config.stale_timeout,
            &io.last_msg_at,
            &self.clock,
            &mut self.stats,
            io.health_tx,
            io.writer,
        )
        .await?;
        Ok(stale.then_some(SessionExit::Reconnect))
    }

    async fn on_ws_frame(
        &mut self,
        frame: Option<Result<Message, tokio_tungstenite::tungstenite::Error>>,
        io: &mut ClobSessionIo<'_>,
    ) -> Result<Option<SessionExit>, FeedError> {
        let msg = match frame {
            None => {
                self.emit_disconnect(io.health_tx, "stream EOF".to_string())
                    .await?;
                return Ok(Some(SessionExit::Reconnect));
            }
            Some(Err(err)) => {
                self.emit_disconnect(io.health_tx, format!("WS read error: {err}"))
                    .await?;
                return Ok(Some(SessionExit::Reconnect));
            }
            Some(Ok(msg)) => msg,
        };
        io.last_msg_at = Instant::now();
        self.stats.frames_received = self.stats.frames_received.saturating_add(1);
        let res = self.dispatch_ws_message(msg, io).await?;
        io.last_msg_at = Instant::now();
        Ok(res)
    }

    async fn dispatch_ws_message(
        &mut self,
        msg: Message,
        io: &mut ClobSessionIo<'_>,
    ) -> Result<Option<SessionExit>, FeedError> {
        match msg {
            Message::Text(txt) => return self.handle_text_message(&txt, io).await,
            Message::Ping(data) => {
                self.stats.pings_replied = self.stats.pings_replied.saturating_add(1);
                if let Err(err) = io.writer.send(Message::Pong(data)).await {
                    self.emit_disconnect(io.health_tx, format!("RFC6455 Pong failed: {err}"))
                        .await?;
                    return Ok(Some(SessionExit::Reconnect));
                }
            }
            Message::Pong(_) => {
                io.awaiting_pong = None;
                self.stats.pongs_received = self.stats.pongs_received.saturating_add(1);
            }
            Message::Close(c) => {
                self.emit_disconnect(io.health_tx, format!("server close: {c:?}"))
                    .await?;
                return Ok(Some(SessionExit::Reconnect));
            }
            Message::Binary(bin) => {
                self.warn(
                    format!("unexpected binary WS frame ({} bytes)", bin.len()),
                    io.health_tx,
                )?;
            }
            Message::Frame(_) => {}
        }
        Ok(None)
    }

    async fn handle_text_message(
        &mut self,
        text: &str,
        io: &mut ClobSessionIo<'_>,
    ) -> Result<Option<SessionExit>, FeedError> {
        let trimmed = text.trim();
        if trimmed == "PONG" {
            io.awaiting_pong = None;
            self.stats.pongs_received = self.stats.pongs_received.saturating_add(1);
            return Ok(None);
        }
        if trimmed == "PING" {
            self.stats.pings_replied = self.stats.pings_replied.saturating_add(1);
            if let Err(err) = io.writer.send(Message::Text("PONG".into())).await {
                self.emit_disconnect(io.health_tx, format!("app PONG send failed: {err}"))
                    .await?;
                return Ok(Some(SessionExit::Reconnect));
            }
            return Ok(None);
        }
        self.parse_and_emit(trimmed, io.producer, io.health_tx, io.cmd_rx)
            .await
    }

    async fn parse_and_emit(
        &mut self,
        trimmed: &str,
        producer: &mut rtrb::Producer<PolyMarketEvent>,
        health_tx: &mpsc::Sender<FeedHealthEvent>,
        cmd_rx: &mut mpsc::Receiver<ClobWsCommand>,
    ) -> Result<Option<SessionExit>, FeedError> {
        let t0 = self.clock.now_ns()?;
        let parsed = parse_clob_ws_text_with_scratch(
            trimmed,
            &self.active_up,
            &self.active_dn,
            &mut self.book_up,
            &mut self.book_dn,
            t0,
            &mut self.scratch,
        );
        let dt_ns = self.clock.now_ns()?.saturating_sub(t0);
        self.tracker.record_ns(dt_ns)?;
        if let Err(err) = parsed {
            self.warn(err.to_string(), health_tx)?;
            return Ok(None);
        }
        self.flush_scratch_batch(producer, health_tx, cmd_rx).await
    }

    async fn flush_scratch_batch(
        &mut self,
        producer: &mut rtrb::Producer<PolyMarketEvent>,
        health_tx: &mpsc::Sender<FeedHealthEvent>,
        cmd_rx: &mut mpsc::Receiver<ClobWsCommand>,
    ) -> Result<Option<SessionExit>, FeedError> {
        let mut scratch = std::mem::take(&mut self.scratch);
        self.emit_scratch_events(&scratch.batch.events, producer, health_tx)?;
        for warn in scratch.batch.warnings.drain(..) {
            self.warn(warn, health_tx)?;
        }
        if scratch.batch.market_resolved {
            let (ts_unix_ms, mono_ns) = now_pair(&self.clock)?;
            let resolved_ev = FeedHealthEvent::MarketResolved {
                feed: FeedId::PolymarketClob,
                ts_unix_ms,
                mono_ns,
            };
            emit_health_transition(health_tx, &mut self.stats, resolved_ev).await;
        }
        let needs_resync = scratch.batch.needs_resync || self.check_sustained_cross(health_tx)?;
        scratch.batch.clear();
        self.scratch = scratch;
        self.maybe_emit_ready(health_tx).await?;
        if needs_resync {
            return self.resync_rest_books(producer, health_tx, cmd_rx).await;
        }
        Ok(None)
    }

    fn emit_scratch_events(
        &mut self,
        events: &[PolyMarketEvent],
        producer: &mut rtrb::Producer<PolyMarketEvent>,
        health_tx: &mpsc::Sender<FeedHealthEvent>,
    ) -> Result<(), FeedError> {
        for &ev in events {
            let before = self.stats.events_pushed;
            push_ring_event(
                producer,
                ev,
                FeedId::PolymarketClob,
                &mut self.stats,
                &mut self.throttle,
                health_tx,
                &self.clock,
            )?;
            if self.stats.events_pushed > before
                && let PolyMarketEvent::BookSnapshot { outcome, .. } = ev
            {
                self.mark_outcome_synced(outcome);
            }
        }
        Ok(())
    }

    fn check_sustained_cross(
        &mut self,
        health_tx: &mpsc::Sender<FeedHealthEvent>,
    ) -> Result<bool, FeedError> {
        if self.book_up.is_crossed() || self.book_dn.is_crossed() {
            let started = *self.crossed_since.get_or_insert_with(Instant::now);
            if started.elapsed() >= Duration::from_millis(SUSTAINED_CROSS_RESYNC_MS) {
                self.warn(
                    format!(
                        "CLOB book remained crossed >= {}ms; triggering REST resync",
                        started.elapsed().as_millis()
                    ),
                    health_tx,
                )?;
                return Ok(true);
            }
        } else {
            self.crossed_since = None;
        }
        Ok(false)
    }

    fn warn(
        &mut self,
        reason: String,
        health_tx: &mpsc::Sender<FeedHealthEvent>,
    ) -> Result<(), FeedError> {
        record_parse_warning(
            FeedId::PolymarketClob,
            reason,
            &mut self.stats,
            &mut self.throttle,
            health_tx,
            &self.clock,
        )
    }

    async fn apply_rollover(
        &mut self,
        writer: &mut WsWriter,
        new_up: String,
        new_dn: String,
    ) -> Result<(), String> {
        let old_up = std::mem::replace(&mut self.active_up, new_up);
        let old_dn = std::mem::replace(&mut self.active_dn, new_dn);
        self.book_up = BookMaintainer::new(Outcome::Up);
        self.book_dn = BookMaintainer::new(Outcome::Down);
        self.reset_sync_state();

        let unsub = serde_json::json!({
            "assets_ids": [&old_up, &old_dn],
            "operation": "unsubscribe",
        });
        writer
            .send(Message::Text(unsub.to_string().into()))
            .await
            .map_err(|e| format!("rollover unsubscribe failed: {e}"))?;

        let sub = serde_json::json!({
            "assets_ids": [&self.active_up, &self.active_dn],
            "operation": "subscribe",
        });
        writer
            .send(Message::Text(sub.to_string().into()))
            .await
            .map_err(|e| format!("rollover subscribe failed: {e}"))?;
        Ok(())
    }

    async fn wait_reconnect_cmd(&mut self, cmd_rx: &mut mpsc::Receiver<ClobWsCommand>) -> bool {
        tokio::select! {
            _ = tokio::time::sleep(self.config.reconnect_backoff) => false,
            cmd = cmd_rx.recv() => self.handle_pending_cmd(cmd) == SessionExit::Shutdown,
        }
    }

    fn handle_pending_cmd(&mut self, cmd: Option<ClobWsCommand>) -> SessionExit {
        match cmd {
            None | Some(ClobWsCommand::Shutdown) => SessionExit::Shutdown,
            Some(ClobWsCommand::Rollover {
                new_asset_id_up,
                new_asset_id_dn,
            }) => {
                self.commit_rollover(new_asset_id_up, new_asset_id_dn);
                SessionExit::ImmediateReconnect
            }
        }
    }

    async fn emit_disconnect(
        &mut self,
        health_tx: &mpsc::Sender<FeedHealthEvent>,
        reason: String,
    ) -> Result<(), FeedError> {
        emit_disconnect(
            FeedId::PolymarketClob,
            &self.clock,
            &mut self.stats,
            health_tx,
            reason,
        )
        .await
    }
}

type RestBookResult = Result<(Vec<BookLevel>, Vec<BookLevel>, u64), FeedError>;

/// Output of parsing one CLOB WebSocket JSON text frame or snapshot batch.
#[derive(Debug, Default)]
pub struct ClobParseBatch {
    /// Successfully decoded `Copy` market events ready for `rtrb`.
    pub events: Vec<PolyMarketEvent>,
    /// Non-fatal per-item warnings (unknown `asset_id`, invalid level, missing timestamp).
    pub warnings: Vec<String>,
    /// `true` if a full `book` frame was invalid/crossed and requires REST resync.
    pub needs_resync: bool,
    /// `true` if the WS feed delivered a `market_resolved` frame.
    pub market_resolved: bool,
}

impl ClobParseBatch {
    fn clear(&mut self) {
        self.events.clear();
        self.warnings.clear();
        self.needs_resync = false;
        self.market_resolved = false;
    }
}

/// Pure testable parser for Polymarket CLOB WebSocket JSON frames (`book`, `price_change`, `last_trade_price`, `tick_size_change`, `best_bid_ask`).
pub fn parse_clob_ws_text(
    text: &str,
    asset_id_up: &str,
    asset_id_dn: &str,
    book_up: &mut BookMaintainer,
    book_dn: &mut BookMaintainer,
    rx_mono_ns: u64,
) -> Result<ClobParseBatch, FeedError> {
    let mut scratch = ClobParseScratch::default();
    parse_clob_ws_text_with_scratch(
        text,
        asset_id_up,
        asset_id_dn,
        book_up,
        book_dn,
        rx_mono_ns,
        &mut scratch,
    )?;
    Ok(scratch.batch)
}

fn parse_clob_ws_text_with_scratch(
    text: &str,
    up_id: &str,
    dn_id: &str,
    book_up: &mut BookMaintainer,
    book_dn: &mut BookMaintainer,
    rx_mono_ns: u64,
    scratch: &mut ClobParseScratch,
) -> Result<(), FeedError> {
    scratch.batch.clear();
    let trimmed = text.trim();
    if trimmed.is_empty() || trimmed == "PONG" || trimmed == "PING" {
        return Ok(());
    }
    if trimmed.starts_with('[') {
        let frames: Vec<ClobWsFrame<'_>> = decode_json_value(trimmed, "clob_ws_array")?;
        for frame in frames {
            process_clob_frame(frame, up_id, dn_id, book_up, book_dn, rx_mono_ns, scratch);
        }
        return Ok(());
    }
    if trimmed.starts_with('{') {
        let frame: ClobWsFrame<'_> = decode_json_value(trimmed, "clob_ws_object")?;
        process_clob_frame(frame, up_id, dn_id, book_up, book_dn, rx_mono_ns, scratch);
        return Ok(());
    }
    Err(FeedError::WebSocket {
        feed: "polymarket_clob",
        detail: format!("unexpected non-JSON WS frame: {trimmed}"),
    })
}

fn decode_json_value<'a, T: Deserialize<'a>>(
    trimmed: &'a str,
    context: &'static str,
) -> Result<T, FeedError> {
    serde_json::from_str(trimmed).map_err(|source| FeedError::Json { context, source })
}

fn process_clob_frame(
    frame: ClobWsFrame<'_>,
    up_id: &str,
    dn_id: &str,
    book_up: &mut BookMaintainer,
    book_dn: &mut BookMaintainer,
    rx_mono_ns: u64,
    scratch: &mut ClobParseScratch,
) {
    match frame {
        ClobWsFrame::Book(f) => {
            decode_book_frame(f, up_id, dn_id, book_up, book_dn, rx_mono_ns, scratch);
        }
        ClobWsFrame::PriceChange(f) => {
            decode_price_change_frame(
                f,
                up_id,
                dn_id,
                book_up,
                book_dn,
                rx_mono_ns,
                &mut scratch.batch,
            );
        }
        ClobWsFrame::LastTradePrice(f) => {
            decode_last_trade_frame(f, up_id, dn_id, rx_mono_ns, &mut scratch.batch);
        }
        ClobWsFrame::TickSizeChange(f) => {
            decode_tick_size_frame(f, up_id, dn_id, rx_mono_ns, &mut scratch.batch);
        }
        ClobWsFrame::BestBidAsk(f) => {
            decode_bba_frame(f, up_id, dn_id, rx_mono_ns, &mut scratch.batch);
        }
        ClobWsFrame::MarketResolved => scratch.batch.market_resolved = true,
        ClobWsFrame::NewMarket => {}
        ClobWsFrame::Unknown => scratch
            .batch
            .warnings
            .push("unrecognized CLOB WS event_type".to_string()),
    }
}

fn map_outcome(asset_id: &str, up_id: &str, dn_id: &str) -> Result<Outcome, String> {
    let trimmed = asset_id.trim();
    if trimmed == up_id {
        Ok(Outcome::Up)
    } else if trimmed == dn_id {
        Ok(Outcome::Down)
    } else {
        Err(format!("unknown asset_id `{trimmed}`"))
    }
}

fn extract_timestamp_warn(ts: Option<u64>, label: &'static str, out: &mut ClobParseBatch) -> u64 {
    match ts {
        Some(v) => v,
        None => {
            out.warnings
                .push(format!("CLOB `{label}` frame omitted exchange timestamp"));
            0
        }
    }
}

fn decode_book_frame(
    f: WsBookFrame<'_>,
    up_id: &str,
    dn_id: &str,
    book_up: &mut BookMaintainer,
    book_dn: &mut BookMaintainer,
    rx_mono_ns: u64,
    scratch: &mut ClobParseScratch,
) {
    let outcome = match map_outcome(&f.asset_id, up_id, dn_id) {
        Ok(o) => o,
        Err(w) => {
            scratch.batch.warnings.push(w);
            return;
        }
    };
    let Some(ts_ms) = f.timestamp else {
        scratch
            .batch
            .warnings
            .push("CLOB `book` frame missing timestamp".to_string());
        scratch.batch.needs_resync = true;
        return;
    };
    if let Err(err) = parse_ws_levels_into(&f.bids, &f.asks, &mut scratch.bids, &mut scratch.asks) {
        scratch.batch.warnings.push(err.to_string());
        scratch.batch.needs_resync = true;
        return;
    }
    let maintainer = match outcome {
        Outcome::Up => book_up,
        Outcome::Down => book_dn,
    };
    match maintainer.apply_snapshot(&scratch.bids, &scratch.asks, rx_mono_ns, ts_ms) {
        Ok(book) => scratch.batch.events.push(PolyMarketEvent::BookSnapshot {
            outcome,
            book,
            rx_mono_ns,
            exch_ts_ms: ts_ms,
        }),
        Err(err) => {
            scratch.batch.warnings.push(err.to_string());
            scratch.batch.needs_resync = true;
        }
    }
}

fn decode_price_change_frame(
    f: WsPriceChangeFrame<'_>,
    up_id: &str,
    dn_id: &str,
    book_up: &mut BookMaintainer,
    book_dn: &mut BookMaintainer,
    rx_mono_ns: u64,
    out: &mut ClobParseBatch,
) {
    let ts_ms = extract_timestamp_warn(f.timestamp, "price_change", out);
    for item in f.price_changes {
        decode_price_change_item(
            item,
            (up_id, dn_id),
            (book_up, book_dn),
            (rx_mono_ns, ts_ms),
            out,
        );
    }
}

fn decode_price_change_item(
    item: WsPriceChangeItemDto<'_>,
    (up_id, dn_id): (&str, &str),
    (book_up, book_dn): (&mut BookMaintainer, &mut BookMaintainer),
    (rx_mono_ns, ts_ms): (u64, u64),
    out: &mut ClobParseBatch,
) {
    let Ok(outcome) = map_outcome(&item.asset_id, up_id, dn_id).map_err(|w| out.warnings.push(w))
    else {
        return;
    };
    let Some(side) = parse_order_side(&item.side) else {
        out.warnings.push(format!("invalid side `{}`", item.side));
        return;
    };
    let (Ok(price), Ok(size)) = (
        parse_decimal_micro_flexible(item.price.trim().as_bytes()).map(MicroUsdc),
        parse_decimal_micro_flexible(item.size.trim().as_bytes()).map(SharesMicro),
    ) else {
        out.warnings
            .push("invalid decimal in price_change item".to_string());
        return;
    };
    let best_bid = parse_opt_warn(item.best_bid.as_deref(), out);
    let best_ask = parse_opt_warn(item.best_ask.as_deref(), out);
    let maintainer = match outcome {
        Outcome::Up => book_up,
        Outcome::Down => book_dn,
    };
    if let Err(err) =
        maintainer.apply_delta_allow_transient_cross(side, price, size, rx_mono_ns, ts_ms)
    {
        out.warnings.push(err.to_string());
        return;
    }
    out.events.push(PolyMarketEvent::PriceChange {
        outcome,
        side,
        price,
        size,
        best_bid,
        best_ask,
        rx_mono_ns,
        exch_ts_ms: ts_ms,
    });
}

fn parse_opt_warn(raw: Option<&str>, out: &mut ClobParseBatch) -> Option<MicroUsdc> {
    match parse_opt_micro(raw) {
        Ok(v) => v,
        Err(w) => {
            out.warnings.push(w);
            None
        }
    }
}

fn decode_last_trade_frame(
    f: WsLastTradeFrame<'_>,
    up_id: &str,
    dn_id: &str,
    rx_mono_ns: u64,
    out: &mut ClobParseBatch,
) {
    let outcome = match map_outcome(&f.asset_id, up_id, dn_id) {
        Ok(o) => o,
        Err(w) => {
            out.warnings.push(w);
            return;
        }
    };
    let Some(side) = parse_order_side(&f.side) else {
        out.warnings
            .push(format!("invalid trade side `{}`", f.side));
        return;
    };
    let (Ok(price), Ok(size)) = (
        parse_decimal_micro_flexible(f.price.trim().as_bytes()).map(MicroUsdc),
        parse_decimal_micro_flexible(f.size.trim().as_bytes()).map(SharesMicro),
    ) else {
        out.warnings
            .push("invalid trade decimal in last_trade_price".to_string());
        return;
    };
    let exch_ts_ms = extract_timestamp_warn(f.timestamp, "last_trade_price", out);
    out.events.push(PolyMarketEvent::LastTrade {
        outcome,
        side,
        price,
        size,
        rx_mono_ns,
        exch_ts_ms,
    });
}

fn decode_tick_size_frame(
    f: WsTickSizeFrame<'_>,
    up_id: &str,
    dn_id: &str,
    rx_mono_ns: u64,
    out: &mut ClobParseBatch,
) {
    let outcome = match map_outcome(&f.asset_id, up_id, dn_id) {
        Ok(o) => o,
        Err(w) => {
            out.warnings.push(w);
            return;
        }
    };
    let Ok(new_tick_micro) =
        parse_decimal_micro_flexible(f.new_tick_size.trim().as_bytes()).map(MicroUsdc)
    else {
        out.warnings
            .push(format!("invalid new_tick_size `{}`", f.new_tick_size));
        return;
    };
    let exch_ts_ms = extract_timestamp_warn(f.timestamp, "tick_size_change", out);
    out.events.push(PolyMarketEvent::TickSizeChange {
        outcome,
        new_tick_micro,
        rx_mono_ns,
        exch_ts_ms,
    });
}

fn decode_bba_frame(
    f: WsBestBidAskFrame<'_>,
    up_id: &str,
    dn_id: &str,
    rx_mono_ns: u64,
    out: &mut ClobParseBatch,
) {
    let outcome = match map_outcome(&f.asset_id, up_id, dn_id) {
        Ok(o) => o,
        Err(w) => {
            out.warnings.push(w);
            return;
        }
    };
    let Ok(best_bid) = parse_opt_micro(f.best_bid.as_deref()) else {
        out.warnings
            .push("invalid best_bid in best_bid_ask".to_string());
        return;
    };
    let Ok(best_ask) = parse_opt_micro(f.best_ask.as_deref()) else {
        out.warnings
            .push("invalid best_ask in best_bid_ask".to_string());
        return;
    };
    let exch_ts_ms = extract_timestamp_warn(f.timestamp, "best_bid_ask", out);
    out.events.push(PolyMarketEvent::BestBidAsk {
        outcome,
        best_bid,
        best_ask,
        rx_mono_ns,
        exch_ts_ms,
    });
}

fn parse_ws_levels_into(
    bids_raw: &[WsLevelDto<'_>],
    asks_raw: &[WsLevelDto<'_>],
    bids: &mut Vec<BookLevel>,
    asks: &mut Vec<BookLevel>,
) -> Result<(), FeedError> {
    parse_side_levels_into(bids_raw, bids, "ws_bid.price", "ws_bid.size")?;
    parse_side_levels_into(asks_raw, asks, "ws_ask.price", "ws_ask.size")
}

fn parse_side_levels_into(
    raw: &[WsLevelDto<'_>],
    dst: &mut Vec<BookLevel>,
    price_field: &'static str,
    size_field: &'static str,
) -> Result<(), FeedError> {
    dst.clear();
    dst.reserve(raw.len());
    for l in raw {
        let price = MicroUsdc(
            parse_decimal_micro_flexible(l.price.trim().as_bytes()).map_err(|source| {
                FeedError::ParseDecimal {
                    field: price_field,
                    source,
                }
            })?,
        );
        let size = SharesMicro(
            parse_decimal_micro_flexible(l.size.trim().as_bytes()).map_err(|source| {
                FeedError::ParseDecimal {
                    field: size_field,
                    source,
                }
            })?,
        );
        dst.push(BookLevel { price, size });
    }
    Ok(())
}

fn parse_opt_micro(raw: Option<&str>) -> Result<Option<MicroUsdc>, String> {
    let Some(val) = raw else {
        return Ok(None);
    };
    let s = val.trim();
    if s.is_empty() {
        Ok(None)
    } else {
        parse_decimal_micro_flexible(s.as_bytes())
            .map(|v| Some(MicroUsdc(v)))
            .map_err(|e| format!("invalid decimal `{s}`: {e}"))
    }
}

async fn send_subscribe(
    writer: &mut WsWriter,
    asset_up: &str,
    asset_dn: &str,
) -> Result<(), String> {
    #[derive(Serialize)]
    struct SubscribeMsg<'a> {
        assets_ids: [&'a str; 2],
        #[serde(rename = "type")]
        kind: &'static str,
        custom_feature_enabled: bool,
    }
    let payload = serde_json::to_string(&SubscribeMsg {
        assets_ids: [asset_up, asset_dn],
        kind: "market",
        custom_feature_enabled: true,
    })
    .map_err(|e| format!("subscribe serialize failed: {e}"))?;
    writer
        .send(Message::Text(payload.into()))
        .await
        .map_err(|e| format!("subscribe send failed: {e}"))
}

struct ClobSessionIo<'a> {
    writer: &'a mut WsWriter,
    producer: &'a mut rtrb::Producer<PolyMarketEvent>,
    health_tx: &'a mpsc::Sender<FeedHealthEvent>,
    cmd_rx: &'a mut mpsc::Receiver<ClobWsCommand>,
    last_msg_at: Instant,
    awaiting_pong: Option<Instant>,
}

#[derive(PartialEq, Eq)]
enum SessionExit {
    Reconnect,
    ImmediateReconnect,
    Shutdown,
}

#[derive(Deserialize)]
#[serde(tag = "event_type")]
enum ClobWsFrame<'a> {
    #[serde(rename = "book", borrow)]
    Book(WsBookFrame<'a>),
    #[serde(rename = "price_change", borrow)]
    PriceChange(WsPriceChangeFrame<'a>),
    #[serde(rename = "last_trade_price", borrow)]
    LastTradePrice(WsLastTradeFrame<'a>),
    #[serde(rename = "tick_size_change", borrow)]
    TickSizeChange(WsTickSizeFrame<'a>),
    #[serde(rename = "best_bid_ask", borrow)]
    BestBidAsk(WsBestBidAskFrame<'a>),
    #[serde(rename = "new_market")]
    NewMarket,
    #[serde(rename = "market_resolved")]
    MarketResolved,
    #[serde(other)]
    Unknown,
}

#[derive(Deserialize)]
struct WsLevelDto<'a> {
    #[serde(borrow)]
    price: Cow<'a, str>,
    #[serde(borrow)]
    size: Cow<'a, str>,
}

#[derive(Deserialize)]
struct WsBookFrame<'a> {
    #[serde(borrow)]
    asset_id: Cow<'a, str>,
    #[serde(default, deserialize_with = "de_opt_string_or_u64")]
    timestamp: Option<u64>,
    #[serde(default, borrow)]
    bids: Vec<WsLevelDto<'a>>,
    #[serde(default, borrow)]
    asks: Vec<WsLevelDto<'a>>,
}

#[derive(Deserialize)]
struct WsPriceChangeFrame<'a> {
    #[serde(default, deserialize_with = "de_opt_string_or_u64")]
    timestamp: Option<u64>,
    #[serde(default, borrow)]
    price_changes: Vec<WsPriceChangeItemDto<'a>>,
}

#[derive(Deserialize)]
struct WsPriceChangeItemDto<'a> {
    #[serde(borrow)]
    asset_id: Cow<'a, str>,
    #[serde(borrow)]
    price: Cow<'a, str>,
    #[serde(borrow)]
    size: Cow<'a, str>,
    #[serde(borrow)]
    side: Cow<'a, str>,
    #[serde(default, borrow)]
    best_bid: Option<Cow<'a, str>>,
    #[serde(default, borrow)]
    best_ask: Option<Cow<'a, str>>,
}

#[derive(Deserialize)]
struct WsLastTradeFrame<'a> {
    #[serde(borrow)]
    asset_id: Cow<'a, str>,
    #[serde(borrow)]
    price: Cow<'a, str>,
    #[serde(borrow)]
    size: Cow<'a, str>,
    #[serde(borrow)]
    side: Cow<'a, str>,
    #[serde(default, deserialize_with = "de_opt_string_or_u64")]
    timestamp: Option<u64>,
}

#[derive(Deserialize)]
struct WsTickSizeFrame<'a> {
    #[serde(borrow)]
    asset_id: Cow<'a, str>,
    #[serde(borrow)]
    new_tick_size: Cow<'a, str>,
    #[serde(default, deserialize_with = "de_opt_string_or_u64")]
    timestamp: Option<u64>,
}

#[derive(Deserialize)]
struct WsBestBidAskFrame<'a> {
    #[serde(borrow)]
    asset_id: Cow<'a, str>,
    #[serde(default, borrow)]
    best_bid: Option<Cow<'a, str>>,
    #[serde(default, borrow)]
    best_ask: Option<Cow<'a, str>>,
    #[serde(default, deserialize_with = "de_opt_string_or_u64")]
    timestamp: Option<u64>,
}
