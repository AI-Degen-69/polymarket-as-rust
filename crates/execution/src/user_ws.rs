//! Authenticated `/ws/user` actor and maker-safe fill reconciler.
//!
//! Wire format: <https://docs.polymarket.com/trading/realtime-order-updates> (API tab).
//! Frames are flat objects tagged by `event_type` (`"order"` / `"trade"`), sometimes batched in arrays.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwap;
use core_types::DataOrigin;
use core_types::money::{DecimalFracPolicy, MICRO_SCALE, parse_decimal_scaled};
use futures_util::{SinkExt, StreamExt};
use secrecy::{ExposeSecret, SecretString};
use serde::Deserialize;
use tokio::sync::watch;
use tokio::time::{Instant, MissedTickBehavior};
use tokio_tungstenite::tungstenite::Message;

use crate::clob_rest::{ClobRestClient, OpenOrderRecord, TradeMakerFill, TradeRecord};
use crate::error::ExecutionError;

/// Production authenticated user-channel endpoint.
pub const DEFAULT_USER_WS_URL: &str = "wss://ws-subscriptions-clob.polymarket.com/ws/user";
/// Application-level `PING` cadence required by the venue.
pub const USER_WS_PING_INTERVAL: Duration = Duration::from_secs(10);
/// Session is declared dead if no `PONG` arrives within this window.
pub const USER_WS_PONG_TIMEOUT: Duration = Duration::from_secs(15);
const RECONNECT_BACKOFF_MIN: Duration = Duration::from_millis(250);
const RECONNECT_BACKOFF_MAX: Duration = Duration::from_secs(10);

/// Settlement lifecycle stage of a trade. Ordering is the legal forward direction.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum TradeStage {
    /// Matched off-chain (includes `MATCHED_NOT_BROADCASTED`, `RETRYING`, `DELAYED`).
    Matched,
    /// Included in a block, not yet final.
    Mined,
    /// Final on-chain. Terminal.
    Confirmed,
    /// On-chain settlement failed. Terminal; inventory must be reversed.
    Failed,
}

impl TradeStage {
    /// Parses raw (`MINED`) and SDK-prefixed (`TRADE_STATUS_MINED`) status strings.
    #[must_use]
    pub fn parse(raw: &str) -> Option<Self> {
        let upper = raw.trim().to_ascii_uppercase();
        let core = upper.strip_prefix("TRADE_STATUS_").unwrap_or(&upper);
        match core {
            "MATCHED" | "MATCHED_NOT_BROADCASTED" | "RETRYING" | "DELAYED" => Some(Self::Matched),
            "MINED" => Some(Self::Mined),
            "CONFIRMED" => Some(Self::Confirmed),
            "FAILED" => Some(Self::Failed),
            _ => None,
        }
    }

    const fn is_terminal(self) -> bool {
        matches!(self, Self::Confirmed | Self::Failed)
    }
}

/// Signed inventory change in `10^-6` share units for the UP and DOWN legs.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct LegDelta {
    pub up: i64,
    pub dn: i64,
}

impl LegDelta {
    const fn add(self, other: Self) -> Self {
        Self {
            up: self.up.saturating_add(other.up),
            dn: self.dn.saturating_add(other.dn),
        }
    }
}

/// Lock-free published view of fills and order state (single writer: the user WS actor).
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize)]
pub struct OrderRegistrySnapshot {
    /// Provenance tag: sandbox fills and balances never enter production estimates.
    pub origin: DataOrigin,
    /// Final on-chain UP inventory (micro-shares). Only this feeds settlement.
    pub confirmed_up: i64,
    /// Final on-chain DOWN inventory (micro-shares).
    pub confirmed_dn: i64,
    /// Matched/mined UP fills that may still fail (micro-shares).
    pub unconfirmed_up: i64,
    /// Matched/mined DOWN fills that may still fail (micro-shares).
    pub unconfirmed_dn: i64,
    /// Our resting order count as last seen.
    pub open_orders: usize,
    /// `true` until REST parity succeeds after (re)connect. Quoting must pause while set.
    pub reconciling: bool,
    /// Distinct trades attributed to us.
    pub trades_tracked: usize,
    /// Trades reversed after `FAILED`.
    pub failed_reversals: u64,
    /// Unexpected payloads (unknown status, `FAILED` after `CONFIRMED`, unparsable order events).
    pub anomalies: u64,
    /// User WS sessions that ended and were re-established.
    pub ws_reconnects: u64,
}

impl OrderRegistrySnapshot {
    /// Total UP exposure, confirmed plus pending (micro-shares). Use this for risk.
    #[must_use]
    pub const fn q_up(&self) -> i64 {
        self.confirmed_up.saturating_add(self.unconfirmed_up)
    }

    /// Total DOWN exposure, confirmed plus pending (micro-shares). Use this for risk.
    #[must_use]
    pub const fn q_dn(&self) -> i64 {
        self.confirmed_dn.saturating_add(self.unconfirmed_dn)
    }

    /// Mergeable pairs backed only by `CONFIRMED` fills (micro-shares).
    #[must_use]
    pub fn pairable_confirmed(&self) -> u64 {
        u64::try_from(self.confirmed_up.min(self.confirmed_dn)).unwrap_or(0)
    }

    /// New quotes are allowed only after REST parity.
    #[must_use]
    pub const fn quoting_allowed(&self) -> bool {
        !self.reconciling
    }
}

/// `event_type: "order"` payload (only the fields the reconciler needs).
#[derive(Debug, Clone, Deserialize)]
pub struct UserOrderEvent {
    pub id: String,
    #[serde(default)]
    pub maker_address: Option<String>,
    /// `PLACEMENT` / `UPDATE` / `CANCELLATION`.
    #[serde(rename = "type", default)]
    pub kind: Option<String>,
    #[serde(default)]
    pub status: Option<String>,
}

struct TrackedTrade {
    delta: LegDelta,
    stage: TradeStage,
}

/// Maker-safe, idempotent fill accounting keyed by `trade_id`.
pub struct FillReconciler {
    maker_address: String,
    up_token: String,
    dn_token: String,
    known_orders: HashSet<String>,
    open_orders: HashSet<String>,
    trades: HashMap<String, TrackedTrade>,
    snap: OrderRegistrySnapshot,
}

impl FillReconciler {
    /// Starts in `reconciling` state: no quoting until the first REST parity check passes.
    #[must_use]
    pub fn new(maker_address: &str, up_token: &str, dn_token: &str) -> Self {
        Self::new_with_origin(DataOrigin::Production, maker_address, up_token, dn_token)
    }

    /// Same as [`Self::new`], tagging every published fill/balance with `origin`.
    #[must_use]
    pub fn new_with_origin(
        origin: DataOrigin,
        maker_address: &str,
        up_token: &str,
        dn_token: &str,
    ) -> Self {
        Self {
            maker_address: maker_address.to_ascii_lowercase(),
            up_token: up_token.to_string(),
            dn_token: dn_token.to_string(),
            known_orders: HashSet::new(),
            open_orders: HashSet::new(),
            trades: HashMap::new(),
            snap: OrderRegistrySnapshot {
                origin,
                reconciling: true,
                ..OrderRegistrySnapshot::default()
            },
        }
    }

    /// Current accounting state.
    #[must_use]
    pub fn snapshot(&self) -> OrderRegistrySnapshot {
        let mut s = self.snap.clone();
        s.open_orders = self.open_orders.len();
        s.trades_tracked = self.trades.len();
        s
    }

    /// Marks an order id as ours (e.g. right after `POST /order` succeeds).
    pub fn register_order(&mut self, order_id: &str) {
        self.known_orders.insert(order_id.to_string());
        self.open_orders.insert(order_id.to_string());
    }

    /// Sets or clears the reconcile lock.
    pub fn set_reconciling(&mut self, on: bool) {
        self.snap.reconciling = on;
    }

    fn is_our_address(&self, addr: Option<&str>) -> bool {
        addr.is_some_and(|a| a.eq_ignore_ascii_case(&self.maker_address))
    }

    /// Tracks our resting order set from `event_type: "order"`.
    pub fn apply_order_event(&mut self, ev: &UserOrderEvent) {
        if !self.known_orders.contains(&ev.id) && !self.is_our_address(ev.maker_address.as_deref())
        {
            return;
        }
        self.known_orders.insert(ev.id.clone());
        let kind = ev.kind.as_deref().unwrap_or("").to_ascii_uppercase();
        let status = ev.status.as_deref().unwrap_or("").to_ascii_uppercase();
        let closed = kind == "CANCELLATION" || matches!(status.as_str(), "CANCELED" | "MATCHED");
        if closed {
            self.open_orders.remove(&ev.id);
        } else {
            self.open_orders.insert(ev.id.clone());
        }
    }

    /// Applies a trade (WS or REST). Duplicate or regressive status updates are no-ops.
    ///
    /// # Errors
    /// Returns [`ExecutionError::Parse`] if an amount or side on one of our fills is malformed.
    pub fn apply_trade(&mut self, t: &TradeRecord) -> Result<(), ExecutionError> {
        let Some(stage) = TradeStage::parse(&t.status) else {
            self.snap.anomalies = self.snap.anomalies.saturating_add(1);
            return Ok(());
        };

        if let Some(tracked) = self.trades.get_mut(&t.id) {
            let prev = tracked.stage;
            if prev.is_terminal() || stage <= prev {
                if prev == TradeStage::Confirmed && stage == TradeStage::Failed {
                    self.snap.anomalies = self.snap.anomalies.saturating_add(1);
                }
                return Ok(());
            }
            tracked.stage = stage;
            let d = tracked.delta;
            match stage {
                TradeStage::Confirmed => {
                    self.shift_unconfirmed(d, -1);
                    self.shift_confirmed(d, 1);
                }
                TradeStage::Failed => {
                    self.shift_unconfirmed(d, -1);
                    self.snap.failed_reversals = self.snap.failed_reversals.saturating_add(1);
                }
                TradeStage::Matched | TradeStage::Mined => {}
            }
            return Ok(());
        }

        let Some(delta) = self.our_delta(t)? else {
            return Ok(());
        };
        self.trades
            .insert(t.id.clone(), TrackedTrade { delta, stage });
        match stage {
            TradeStage::Confirmed => self.shift_confirmed(delta, 1),
            TradeStage::Failed => {}
            TradeStage::Matched | TradeStage::Mined => self.shift_unconfirmed(delta, 1),
        }
        Ok(())
    }

    /// Replays REST state after a gap, then releases the reconcile lock.
    ///
    /// # Errors
    /// Propagates [`Self::apply_trade`] errors; the lock stays held on failure (fail closed).
    pub fn reconcile_from_rest(
        &mut self,
        open: &[OpenOrderRecord],
        trades: &[TradeRecord],
    ) -> Result<(), ExecutionError> {
        let ours: HashSet<String> = open
            .iter()
            .filter(|o| {
                self.known_orders.contains(&o.id) || self.is_our_address(Some(&o.maker_address))
            })
            .map(|o| o.id.clone())
            .collect();
        self.known_orders.extend(ours.iter().cloned());
        self.open_orders = ours;
        for t in trades {
            self.apply_trade(t)?;
        }
        self.snap.reconciling = false;
        Ok(())
    }

    /// Extracts only OUR side of a trade. As maker, the top-level `asset_id`/`side` belong to the
    /// taker and must never be booked: a taker buying DOWN can fill our resting UP bid.
    fn our_delta(&self, t: &TradeRecord) -> Result<Option<LegDelta>, ExecutionError> {
        let is_taker = match t
            .trader_side
            .as_deref()
            .map(str::to_ascii_uppercase)
            .as_deref()
        {
            Some("TAKER") => true,
            Some("MAKER") => false,
            _ => self.known_orders.contains(&t.taker_order_id),
        };
        if is_taker {
            return self.leg_delta(&t.asset_id, &t.side, &t.size).map(Some);
        }
        let mut total = LegDelta::default();
        let mut any = false;
        for mo in t
            .maker_orders
            .iter()
            .filter(|mo| self.is_our_maker_fill(mo))
        {
            any = true;
            total = total.add(self.leg_delta(&mo.asset_id, &mo.side, &mo.matched_amount)?);
        }
        Ok(any.then_some(total))
    }

    fn is_our_maker_fill(&self, mo: &TradeMakerFill) -> bool {
        self.known_orders.contains(&mo.order_id) || self.is_our_address(mo.maker_address.as_deref())
    }

    fn leg_delta(&self, asset: &str, side: &str, amount: &str) -> Result<LegDelta, ExecutionError> {
        let qty = parse_decimal_scaled(
            amount.trim().as_bytes(),
            6,
            MICRO_SCALE,
            DecimalFracPolicy::AllowTrailingZeros,
        )
        .map_err(|e| ExecutionError::Parse(format!("fill amount '{amount}': {e}")))?;
        let qty = i64::try_from(qty)
            .map_err(|_| ExecutionError::Parse(format!("fill amount '{amount}' overflows i64")))?;
        let signed = match side.trim().to_ascii_uppercase().as_str() {
            "BUY" => qty,
            "SELL" => qty.saturating_neg(),
            other => return Err(ExecutionError::Parse(format!("fill side '{other}'"))),
        };
        Ok(if asset == self.up_token {
            LegDelta { up: signed, dn: 0 }
        } else if asset == self.dn_token {
            LegDelta { up: 0, dn: signed }
        } else {
            LegDelta::default()
        })
    }

    fn shift_unconfirmed(&mut self, d: LegDelta, sign: i64) {
        self.snap.unconfirmed_up = self
            .snap
            .unconfirmed_up
            .saturating_add(d.up.saturating_mul(sign));
        self.snap.unconfirmed_dn = self
            .snap
            .unconfirmed_dn
            .saturating_add(d.dn.saturating_mul(sign));
    }

    fn shift_confirmed(&mut self, d: LegDelta, sign: i64) {
        self.snap.confirmed_up = self
            .snap
            .confirmed_up
            .saturating_add(d.up.saturating_mul(sign));
        self.snap.confirmed_dn = self
            .snap
            .confirmed_dn
            .saturating_add(d.dn.saturating_mul(sign));
    }

    /// Applies one text frame (object or array). Malformed trades fail closed; others are counted.
    ///
    /// # Errors
    /// Returns an error if a `trade` event cannot be parsed or applied.
    pub fn apply_frame(&mut self, text: &str) -> Result<(), ExecutionError> {
        let Ok(value) = serde_json::from_str::<serde_json::Value>(text) else {
            self.snap.anomalies = self.snap.anomalies.saturating_add(1);
            return Ok(());
        };
        let events = match value {
            serde_json::Value::Array(items) => items,
            other => vec![other],
        };
        for ev in events {
            match ev.get("event_type").and_then(serde_json::Value::as_str) {
                Some("trade") => {
                    let trade: TradeRecord = serde_json::from_value(ev)?;
                    self.apply_trade(&trade)?;
                }
                Some("order") => match serde_json::from_value::<UserOrderEvent>(ev) {
                    Ok(order) => self.apply_order_event(&order),
                    Err(_) => self.snap.anomalies = self.snap.anomalies.saturating_add(1),
                },
                _ => {}
            }
        }
        Ok(())
    }
}

/// CLOB L2 credentials for the user channel. `Debug` never prints values.
pub struct UserWsCredentials {
    pub api_key: SecretString,
    pub secret: SecretString,
    pub passphrase: SecretString,
}

impl std::fmt::Debug for UserWsCredentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("UserWsCredentials([REDACTED])")
    }
}

/// Builds the subscription frame. The result holds secrets: send it, never log it.
#[must_use]
pub fn subscription_frame(creds: &UserWsCredentials, markets: &[String]) -> String {
    serde_json::json!({
        "auth": {
            "apiKey": creds.api_key.expose_secret(),
            "secret": creds.secret.expose_secret(),
            "passphrase": creds.passphrase.expose_secret(),
        },
        "markets": markets,
        "type": "user",
    })
    .to_string()
}

enum SessionEnd {
    Shutdown,
    Dropped,
}

fn ws_err(e: impl std::fmt::Display) -> ExecutionError {
    ExecutionError::WebSocket(e.to_string())
}

/// Owns the [`FillReconciler`] and publishes snapshots to an `ArcSwap` after every change.
pub struct ClobUserWsActor {
    url: String,
    creds: UserWsCredentials,
    markets: Vec<String>,
    maker_address: String,
    rest: ClobRestClient,
    reconciler: FillReconciler,
    snapshot: Arc<ArcSwap<OrderRegistrySnapshot>>,
    ping_interval: Duration,
    pong_timeout: Duration,
    backoff_min: Duration,
}

impl ClobUserWsActor {
    /// Creates an actor for one market (`condition_id`).
    #[must_use]
    pub fn new(
        url: impl Into<String>,
        creds: UserWsCredentials,
        condition_id: &str,
        maker_address: &str,
        rest: ClobRestClient,
        reconciler: FillReconciler,
    ) -> Self {
        let snapshot = Arc::new(ArcSwap::from_pointee(reconciler.snapshot()));
        Self {
            url: url.into(),
            creds,
            markets: vec![condition_id.to_string()],
            maker_address: maker_address.to_string(),
            rest,
            reconciler,
            snapshot,
            ping_interval: USER_WS_PING_INTERVAL,
            pong_timeout: USER_WS_PONG_TIMEOUT,
            backoff_min: RECONNECT_BACKOFF_MIN,
        }
    }

    /// Overrides timing (tests).
    #[must_use]
    pub const fn with_timing(
        mut self,
        ping: Duration,
        pong_timeout: Duration,
        backoff_min: Duration,
    ) -> Self {
        self.ping_interval = ping;
        self.pong_timeout = pong_timeout;
        self.backoff_min = backoff_min;
        self
    }

    /// Shared read handle for the quoting loop and settlement worker.
    #[must_use]
    pub fn snapshot_handle(&self) -> Arc<ArcSwap<OrderRegistrySnapshot>> {
        Arc::clone(&self.snapshot)
    }

    fn publish(&self) {
        self.snapshot.store(Arc::new(self.reconciler.snapshot()));
    }

    /// Runs until `shutdown` flips to `true`. Every session drop re-arms the reconcile lock.
    ///
    /// # Errors
    /// Currently never returns `Err`; session failures trigger reconnect with backoff.
    pub async fn run(mut self, mut shutdown: watch::Receiver<bool>) -> Result<(), ExecutionError> {
        let mut backoff = self.backoff_min;
        loop {
            if *shutdown.borrow() {
                return Ok(());
            }
            self.reconciler.set_reconciling(true);
            self.publish();
            let started = Instant::now();
            if let Ok(SessionEnd::Shutdown) = self.session(&mut shutdown).await {
                self.reconciler.set_reconciling(true);
                self.publish();
                return Ok(());
            }
            self.reconciler.set_reconciling(true);
            self.reconciler.snap.ws_reconnects =
                self.reconciler.snap.ws_reconnects.saturating_add(1);
            self.publish();
            if started.elapsed() > RECONNECT_BACKOFF_MAX {
                backoff = self.backoff_min;
            }
            tokio::select! {
                () = tokio::time::sleep(backoff) => {}
                res = shutdown.changed() => {
                    if res.is_err() || *shutdown.borrow() {
                        self.reconciler.set_reconciling(true);
                        self.publish();
                        return Ok(());
                    }
                }
            }
            backoff = backoff.saturating_mul(2).min(RECONNECT_BACKOFF_MAX);
        }
    }

    async fn reconcile_rest(&mut self) -> Result<(), ExecutionError> {
        let market = self.markets.first().map(String::as_str);
        let open = self.rest.get_open_orders(market, None).await?;
        let trades = self.rest.get_trades(&self.maker_address, market).await?;
        self.reconciler.reconcile_from_rest(&open, &trades)
    }

    async fn session(
        &mut self,
        shutdown: &mut watch::Receiver<bool>,
    ) -> Result<SessionEnd, ExecutionError> {
        let lower = self.url.to_ascii_lowercase();
        if !lower.starts_with("wss://")
            && !lower.starts_with("ws://127.0.0.1")
            && !lower.starts_with("ws://localhost")
        {
            return Err(ExecutionError::WebSocket(
                "Unencrypted ws:// endpoint rejected: user channel requires wss://".to_string(),
            ));
        }

        let (ws, _) = tokio_tungstenite::connect_async(self.url.as_str())
            .await
            .map_err(ws_err)?;
        let (mut tx, mut rx) = ws.split();
        tx.send(Message::Text(
            subscription_frame(&self.creds, &self.markets).into(),
        ))
        .await
        .map_err(ws_err)?;

        // Subscribe first so events during the REST call buffer in the socket; trade_id dedupe
        // makes the overlap harmless.
        self.reconcile_rest().await?;
        self.publish();

        let mut ping = tokio::time::interval(self.ping_interval);
        ping.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let mut last_pong = Instant::now();
        loop {
            tokio::select! {
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        let _ = tx.send(Message::Close(None)).await;
                        return Ok(SessionEnd::Shutdown);
                    }
                }
                _ = ping.tick() => {
                    tx.send(Message::Text("PING".into())).await.map_err(ws_err)?;
                }
                () = tokio::time::sleep_until(last_pong + self.pong_timeout) => {
                    return Err(ExecutionError::WebSocket("PONG watchdog timeout".into()));
                }
                msg = rx.next() => match msg {
                    None | Some(Ok(Message::Close(_))) => return Ok(SessionEnd::Dropped),
                    Some(Err(e)) => return Err(ws_err(e)),
                    Some(Ok(Message::Text(text))) => {
                        if text.as_str().trim() == "PONG" {
                            last_pong = Instant::now();
                        } else {
                            self.reconciler.apply_frame(text.as_str())?;
                            self.publish();
                        }
                    }
                    Some(Ok(Message::Ping(payload))) => {
                        tx.send(Message::Pong(payload)).await.map_err(ws_err)?;
                    }
                    Some(Ok(_)) => {}
                },
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::L2HmacSigner;
    use tokio::net::TcpListener;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    type TestResult = Result<(), Box<dyn std::error::Error>>;

    const ME: &str = "0xeE3B778A783510BC833384919F709e3d2fee1624";
    const OTHER: &str = "0x1111111111111111111111111111111111111111";
    const UP: &str = "111";
    const DN: &str = "222";

    fn maker_trade(id: &str, status: &str, our_amount: &str) -> String {
        // Taker bought DOWN; it matched our resting UP bid (complementary mint) plus another maker.
        format!(
            r#"{{"event_type":"trade","type":"TRADE","id":"{id}","taker_order_id":"t-1","market":"0xc",
            "asset_id":"{DN}","side":"BUY","size":"9","price":"0.48","status":"{status}","owner":"k",
            "maker_orders":[
              {{"order_id":"o-ours","owner":"k","maker_address":"{ME}","matched_amount":"{our_amount}","price":"0.52","asset_id":"{UP}","side":"BUY"}},
              {{"order_id":"o-x","owner":"x","maker_address":"{OTHER}","matched_amount":"4","price":"0.48","asset_id":"{DN}","side":"SELL"}}
            ],"trader_side":"MAKER","timestamp":"1"}}"#
        )
    }

    #[test]
    fn maker_fill_books_our_leg_not_taker_leg_and_dedupes() -> TestResult {
        let mut r = FillReconciler::new(ME, UP, DN);
        r.apply_frame(&maker_trade("tr-1", "MATCHED", "5"))?;
        let s = r.snapshot();
        assert_eq!((s.unconfirmed_up, s.unconfirmed_dn), (5_000_000, 0));
        assert_eq!(s.q_dn(), 0, "taker's DOWN leg must not be booked");

        for status in ["MINED", "MINED", "CONFIRMED", "CONFIRMED", "MATCHED"] {
            r.apply_frame(&maker_trade("tr-1", status, "5"))?;
        }
        let s = r.snapshot();
        assert_eq!((s.confirmed_up, s.unconfirmed_up), (5_000_000, 0));
        assert_eq!(s.trades_tracked, 1);
        Ok(())
    }

    #[test]
    fn failed_trade_reverses_and_failed_after_confirmed_is_ignored() -> TestResult {
        let mut r = FillReconciler::new(ME, UP, DN);
        r.apply_frame(&maker_trade("tr-1", "MATCHED", "5"))?;
        r.apply_frame(&maker_trade("tr-1", "TRADE_STATUS_FAILED", "5"))?;
        r.apply_frame(&maker_trade("tr-1", "CONFIRMED", "5"))?;
        let s = r.snapshot();
        assert_eq!((s.q_up(), s.q_dn(), s.failed_reversals), (0, 0, 1));

        r.apply_frame(&maker_trade("tr-2", "CONFIRMED", "2.5"))?;
        r.apply_frame(&maker_trade("tr-2", "FAILED", "2.5"))?;
        let s = r.snapshot();
        assert_eq!((s.confirmed_up, s.anomalies), (2_500_000, 1));
        Ok(())
    }

    #[test]
    fn taker_flatten_books_top_level_leg_and_null_maker_orders_parse() -> TestResult {
        let mut r = FillReconciler::new(ME, UP, DN);
        let frame = format!(
            r#"[{{"event_type":"trade","id":"tr-9","taker_order_id":"o-flat","asset_id":"{DN}","side":"SELL",
            "size":"3","price":"0.4","status":"MATCHED","owner":"k","maker_orders":null,"trader_side":"TAKER"}},
            {{"event_type":"order","id":"o-2","maker_address":"{ME}","type":"PLACEMENT","status":"LIVE"}}]"#
        );
        r.apply_frame(&frame)?;
        let s = r.snapshot();
        assert_eq!((s.unconfirmed_dn, s.open_orders), (-3_000_000, 1));

        r.apply_frame(r#"{"event_type":"order","id":"o-2","type":"CANCELLATION"}"#)?;
        assert_eq!(r.snapshot().open_orders, 0);
        Ok(())
    }

    #[test]
    fn foreign_and_unknown_payloads_do_not_touch_inventory() -> TestResult {
        let mut r = FillReconciler::new(ME, UP, DN);
        let foreign = maker_trade("tr-1", "MATCHED", "5").replace(ME, OTHER);
        r.apply_frame(&foreign)?;
        r.apply_frame(&maker_trade("tr-2", "WEIRD", "5"))?;
        r.apply_frame("not json")?;
        let s = r.snapshot();
        assert_eq!(
            (s.q_up(), s.q_dn(), s.trades_tracked, s.anomalies),
            (0, 0, 0, 2)
        );
        assert!(
            r.apply_frame(&maker_trade("tr-3", "MATCHED", "abc"))
                .is_err()
        );
        Ok(())
    }

    #[test]
    fn rest_reconcile_adds_missed_fill_once_and_releases_lock() -> TestResult {
        let mut r = FillReconciler::new(ME, UP, DN);
        assert!(!r.snapshot().quoting_allowed());
        r.apply_frame(&maker_trade("tr-1", "MATCHED", "5"))?;
        let seen: TradeRecord = serde_json::from_str(&maker_trade("tr-1", "CONFIRMED", "5"))?;
        let missed: TradeRecord = serde_json::from_str(&maker_trade("tr-2", "MINED", "1"))?;
        r.reconcile_from_rest(&[], &[seen.clone(), missed.clone(), seen, missed])?;
        let s = r.snapshot();
        assert!(s.quoting_allowed());
        assert_eq!(
            (s.confirmed_up, s.unconfirmed_up, s.trades_tracked),
            (5_000_000, 1_000_000, 2)
        );
        Ok(())
    }

    #[test]
    fn credentials_debug_and_frame_shape() -> TestResult {
        let creds = test_creds();
        assert_eq!(format!("{creds:?}"), "UserWsCredentials([REDACTED])");
        let v: serde_json::Value =
            serde_json::from_str(&subscription_frame(&creds, &["0xc".into()]))?;
        assert_eq!(v.pointer("/type"), Some(&"user".into()));
        assert_eq!(v.pointer("/markets/0"), Some(&"0xc".into()));
        assert_eq!(v.pointer("/auth/apiKey"), Some(&"api-key-1234".into()));
        Ok(())
    }

    fn test_creds() -> UserWsCredentials {
        UserWsCredentials {
            api_key: SecretString::new("api-key-1234".into()),
            secret: SecretString::new("c2VjcmV0".into()),
            passphrase: SecretString::new("pass".into()),
        }
    }

    async fn wait_for(
        handle: &ArcSwap<OrderRegistrySnapshot>,
        what: &str,
        pred: impl Fn(&OrderRegistrySnapshot) -> bool,
    ) -> Result<(), String> {
        let deadline = Instant::now() + Duration::from_secs(5);
        while Instant::now() < deadline {
            if pred(&handle.load()) {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        Err(format!("timed out waiting for {what}: {:?}", handle.load()))
    }

    /// Adversarial: socket drops while a fill is unseen -> quoting halts until REST parity books it.
    #[tokio::test]
    async fn adversarial_ws_drop_holds_reconcile_lock_until_rest_parity() -> TestResult {
        let rest_server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/data/orders"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({"data": []})))
            .mount(&rest_server)
            .await;
        // First parity check: no trades. Later checks: the fill missed during the drop (slow REST).
        Mock::given(method("GET"))
            .and(path("/data/trades"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([])))
            .up_to_n_times(1)
            .with_priority(1)
            .mount(&rest_server)
            .await;
        let missed: serde_json::Value =
            serde_json::from_str(&maker_trade("tr-missed", "CONFIRMED", "2"))?;
        Mock::given(method("GET"))
            .and(path("/data/trades"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(serde_json::json!([missed]))
                    .set_delay(Duration::from_millis(400)),
            )
            .with_priority(2)
            .mount(&rest_server)
            .await;

        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let ws_url = format!("ws://{}", listener.local_addr()?);
        let seen_frame = maker_trade("tr-seen", "MATCHED", "5");
        let server = tokio::spawn(async move {
            // Session 1: verify subscription, push one fill, then drop without a close frame.
            let (tcp, _) = listener.accept().await?;
            let mut ws = tokio_tungstenite::accept_async(tcp).await?;
            let sub = ws.next().await.ok_or("no sub")??;
            let sub: serde_json::Value = serde_json::from_str(sub.to_text()?)?;
            if sub.pointer("/type").and_then(serde_json::Value::as_str) != Some("user")
                || sub
                    .pointer("/markets/0")
                    .and_then(serde_json::Value::as_str)
                    != Some("0xc")
            {
                return Err("bad subscription".into());
            }
            ws.send(Message::Text(seen_frame.into())).await?;
            tokio::time::sleep(Duration::from_millis(50)).await;
            drop(ws);
            // Session 2: stay up, answer PINGs.
            let (tcp, _) = listener.accept().await?;
            let mut ws = tokio_tungstenite::accept_async(tcp).await?;
            while let Some(Ok(msg)) = ws.next().await {
                if msg.to_text().is_ok_and(|t| t == "PING") {
                    ws.send(Message::Text("PONG".into())).await?;
                }
            }
            Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
        });

        let secret = SecretString::new("c2VjcmV0".into());
        let signer = L2HmacSigner::new(
            ME,
            "api-key-1234",
            &secret,
            SecretString::new("pass".into()),
        )?;
        let rest = ClobRestClient::new(rest_server.uri(), signer)?;
        let actor = ClobUserWsActor::new(
            ws_url,
            test_creds(),
            "0xc",
            ME,
            rest,
            FillReconciler::new(ME, UP, DN),
        )
        .with_timing(
            Duration::from_millis(100),
            Duration::from_secs(2),
            Duration::from_millis(20),
        );
        let handle = actor.snapshot_handle();
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let task = tokio::spawn(actor.run(shutdown_rx));

        wait_for(&handle, "reconnect with lock held", |s| {
            s.ws_reconnects >= 1 && s.reconciling && s.unconfirmed_up == 5_000_000
        })
        .await?;
        assert!(!handle.load().quoting_allowed());
        wait_for(&handle, "REST parity", |s| {
            !s.reconciling && s.confirmed_up == 2_000_000
        })
        .await?;
        assert_eq!(handle.load().q_up(), 7_000_000);

        shutdown_tx.send(true)?;
        tokio::time::timeout(Duration::from_secs(2), task).await???;
        server.abort();
        Ok(())
    }
}
