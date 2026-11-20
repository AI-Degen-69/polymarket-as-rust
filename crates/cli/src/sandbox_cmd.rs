//! `poly-as sandbox` commands: health gate, discovery, book read, validation order,
//! and market/user WebSocket feeds against the NEX preprod sandbox.

use std::error::Error;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use core_types::{
    DataOrigin, HighResTimerGuard, JsonlWriter, NexWsVersion, Outcome, SandboxProfile, WalletMode,
};
use execution::{
    ClobUserWsActor, FillReconciler, L2HmacSigner, NexFeedBudget, NexHttp, NexTradingSession,
    OrderSigner, UserWsCredentials, check_public_connection, discover_markets, fetch_book,
    find_market_by_asset,
};
use feeds::{
    ClobMarketWsActor, ClobWsCommand, ClobWsConfig, DEFAULT_RING_CAPACITY, FeedHealthEvent,
    GammaDiscoveryClient, PolyMarketEvent,
};
use rtrb::RingBuffer;
use secrecy::ExposeSecret;
use serde::Serialize;
use tokio::sync::{mpsc, watch};

use crate::exec_cmd::parse_duration_secs;
use crate::sandbox_report::{
    print_book, print_discovery, print_gate, print_market_summary, print_sandbox_header,
    print_validation,
};

/// Actions exposed by `poly-as sandbox`.
pub(crate) enum SandboxAction {
    Health {
        require_allowlisted_egress: bool,
    },
    Markets {
        limit: usize,
    },
    Book {
        asset_id: String,
    },
    Validate {
        asset_id: String,
        confirm_sandbox_trade: bool,
    },
    Feeds {
        up_asset_id: String,
        dn_asset_id: String,
        duration: String,
        ws_version: NexWsVersion,
        reconnect_after_secs: Option<u64>,
    },
}

fn unix_ms_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(1_700_000_000_000)
}

fn mono_ns_now() -> u64 {
    static START: OnceLock<Instant> = OnceLock::new();
    let start = START.get_or_init(Instant::now);
    u64::try_from(start.elapsed().as_nanos()).unwrap_or(u64::MAX)
}

fn write_event<T: Serialize>(
    jsonl: &mut JsonlWriter,
    event: &str,
    payload: &T,
) -> Result<(), Box<dyn Error>> {
    jsonl.write_event(unix_ms_now(), mono_ns_now(), event, payload)?;
    Ok(())
}

#[derive(Serialize)]
struct SandboxEnvelope<'a, T: Serialize> {
    origin: DataOrigin,
    network: &'static str,
    data: &'a T,
}

fn write_sandbox_event<T: Serialize>(
    jsonl: &mut JsonlWriter,
    event: &str,
    data: &T,
) -> Result<(), Box<dyn Error>> {
    write_event(
        jsonl,
        event,
        &SandboxEnvelope {
            origin: DataOrigin::Sandbox,
            network: "nex-amoy-80002",
            data,
        },
    )
}

/// Runs one sandbox action with a fresh Tokio runtime.
pub(crate) fn run(jsonl_path: PathBuf, action: SandboxAction) -> Result<(), Box<dyn Error>> {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    rt.block_on(async move {
        let profile = SandboxProfile::from_env()?;
        feeds::install_tls_provider();
        match action {
            SandboxAction::Health {
                require_allowlisted_egress,
            } => run_health(&profile, &jsonl_path, require_allowlisted_egress).await,
            SandboxAction::Markets { limit } => run_markets(&profile, &jsonl_path, limit).await,
            SandboxAction::Book { asset_id } => run_book(&profile, &jsonl_path, &asset_id).await,
            SandboxAction::Validate {
                asset_id,
                confirm_sandbox_trade,
            } => run_validate(&profile, &jsonl_path, &asset_id, confirm_sandbox_trade).await,
            SandboxAction::Feeds {
                up_asset_id,
                dn_asset_id,
                duration,
                ws_version,
                reconnect_after_secs,
            } => {
                run_feeds(
                    &profile,
                    &jsonl_path,
                    &up_asset_id,
                    &dn_asset_id,
                    &duration,
                    ws_version,
                    reconnect_after_secs,
                )
                .await
            }
        }
    })
}

async fn run_health(
    profile: &SandboxProfile,
    jsonl_path: &Path,
    require_allowlisted_egress: bool,
) -> Result<(), Box<dyn Error>> {
    print_sandbox_header("health", jsonl_path);
    let mut jsonl = JsonlWriter::create(jsonl_path)?;
    let report = check_public_connection(profile).await?;
    print_gate(&report);
    write_sandbox_event(&mut jsonl, "sandbox_health_gate", &report)?;
    jsonl.close()?;

    if require_allowlisted_egress && !report.egress_allowed() {
        return Err(format!(
            "egress {} is not the registered allowlisted IPv4 {:?}; re-allowlist before trading",
            report.egress_ip.as_deref().unwrap_or("<unknown>"),
            report.allowlisted_egress_ip
        )
        .into());
    }
    report.require_healthy()?;
    println!(
        "GATE: PASS  /health=200 from egress {}",
        report.egress_ip.as_deref().unwrap_or("<unknown>")
    );
    Ok(())
}

async fn run_markets(
    profile: &SandboxProfile,
    jsonl_path: &Path,
    limit: usize,
) -> Result<(), Box<dyn Error>> {
    print_sandbox_header("markets", jsonl_path);
    let gate = check_public_connection(profile).await?;
    print_gate(&gate);
    gate.require_healthy()?;

    let mut jsonl = JsonlWriter::create(jsonl_path)?;
    write_sandbox_event(&mut jsonl, "sandbox_health_gate", &gate)?;
    let mut discovery = discover_markets(profile, limit.max(1)).await?;
    // The NEX CLOB route returns the whole list; present only the requested page.
    discovery.markets.truncate(limit.max(1));
    print_discovery(&discovery);
    write_sandbox_event(&mut jsonl, "sandbox_market_discovery", &discovery)?;
    jsonl.close()?;
    Ok(())
}

async fn run_book(
    profile: &SandboxProfile,
    jsonl_path: &Path,
    asset_id: &str,
) -> Result<(), Box<dyn Error>> {
    print_sandbox_header("book", jsonl_path);
    let gate = check_public_connection(profile).await?;
    print_gate(&gate);
    gate.require_healthy()?;

    let mut jsonl = JsonlWriter::create(jsonl_path)?;
    write_sandbox_event(&mut jsonl, "sandbox_health_gate", &gate)?;
    let book = fetch_book(profile, asset_id).await?;
    print_book(&book);
    write_sandbox_event(&mut jsonl, "sandbox_book", &book)?;
    jsonl.close()?;
    Ok(())
}

async fn run_validate(
    profile: &SandboxProfile,
    jsonl_path: &Path,
    asset_id: &str,
    confirm_sandbox_trade: bool,
) -> Result<(), Box<dyn Error>> {
    print_sandbox_header("validate", jsonl_path);
    if !confirm_sandbox_trade {
        return Err(
            "refusing to place a sandbox order: pass --confirm-sandbox-trade (Amoy test pUSD only)"
                .into(),
        );
    }
    let gate = check_public_connection(profile).await?;
    print_gate(&gate);
    gate.require_healthy()?;

    let mut jsonl = JsonlWriter::create(jsonl_path)?;
    write_sandbox_event(&mut jsonl, "sandbox_health_gate", &gate)?;

    let market = find_market_by_asset(profile, asset_id).await?;
    print_market_summary(&market);
    let outcome = if market.up_asset_id == asset_id.trim() {
        Outcome::Up
    } else if market.dn_asset_id == asset_id.trim() {
        Outcome::Down
    } else {
        return Err(format!("asset {asset_id} is not in market {}", market.condition_id).into());
    };
    let book = fetch_book(profile, market.asset_id(outcome)).await?;
    print_book(&book);

    // The session refuses to open without a passing gate, Amoy key, and sandbox L2 credentials.
    let session = NexTradingSession::open(profile.clone(), gate)?;
    let result = session.validate_order(&market, book, outcome).await?;
    print_validation(&result);
    write_sandbox_event(&mut jsonl, "sandbox_validation_order", &result)?;
    jsonl.close()?;
    println!(
        "VALIDATION: order {} placed (post-only), read-back={}, cancel/reconcile={}",
        result.placement.order_id,
        if result.readback.is_some() {
            "yes"
        } else {
            "no"
        },
        result.cancel_detail.as_deref().unwrap_or("none")
    );
    Ok(())
}

struct FeedSessionState {
    connected_sessions: u64,
    snapshots_seen: u64,
    snapshot_seen: bool,
    up_has_liquidity: bool,
    dn_has_liquidity: bool,
    ready_events: u64,
    stale_events: u64,
    last_market_msg: Option<Instant>,
    user_ready: bool,
    user_reconcile_seen: bool,
}

impl FeedSessionState {
    fn new() -> Self {
        Self {
            connected_sessions: 0,
            snapshots_seen: 0,
            snapshot_seen: false,
            up_has_liquidity: false,
            dn_has_liquidity: false,
            ready_events: 0,
            stale_events: 0,
            last_market_msg: None,
            user_ready: false,
            user_reconcile_seen: false,
        }
    }

    fn book_has_liquidity(&self) -> bool {
        self.up_has_liquidity || self.dn_has_liquidity
    }

    fn reset_books(&mut self) {
        self.up_has_liquidity = false;
        self.dn_has_liquidity = false;
    }

    fn readiness(&self, stale_after_ms: u64) -> execution::NexFeedReadiness {
        execution::NexFeedReadiness {
            snapshot_seen: self.snapshot_seen,
            last_market_msg_age_ms: self.last_market_msg.map(|t| t.elapsed().as_millis() as u64),
            book_has_liquidity: self.book_has_liquidity(),
            user_ready: self.user_ready,
            stale_after_ms,
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_feeds(
    profile: &SandboxProfile,
    jsonl_path: &Path,
    up_asset_id: &str,
    dn_asset_id: &str,
    duration_raw: &str,
    ws_version: NexWsVersion,
    reconnect_after_secs: Option<u64>,
) -> Result<(), Box<dyn Error>> {
    print_sandbox_header("feeds", jsonl_path);
    let duration = Duration::from_secs(parse_duration_secs(duration_raw)?);
    let _timer_guard = HighResTimerGuard::acquire_1ms()?;
    let gate = check_public_connection(profile).await?;
    print_gate(&gate);
    gate.require_healthy()?;

    let mut jsonl = JsonlWriter::create(jsonl_path)?;
    write_sandbox_event(&mut jsonl, "sandbox_health_gate", &gate)?;

    // Resolve the parent market so the user feed can filter by condition ID.
    let market = find_market_by_asset(profile, up_asset_id).await?;
    print_market_summary(&market);

    // Only subs-v2 has application connection limits; original feeds are unlimited.
    let budget = NexFeedBudget::new(profile.shared_nat_sockets);
    let _market_permit = (ws_version == NexWsVersion::SubsV2)
        .then(|| budget.reserve_market())
        .transpose()?;

    let discovery = GammaDiscoveryClient::with_base_urls(
        profile.gamma_url.clone(),
        profile.clob_url.clone(),
        "https://api.binance.com",
    )?;
    let mut config = ClobWsConfig::new(up_asset_id, dn_asset_id);
    config.ws_url = profile.market_ws_url(ws_version);
    config.ping_interval = Duration::from_secs(10);
    config.pong_timeout = Duration::from_secs(15);
    config.stale_timeout = Duration::from_millis(execution::NEX_STALE_AFTER_MS);
    // NEX declares a feed stale after 60s without activity or a missed PONG; the 10s
    // PING/PONG keep-alive must not trip the watchdog.
    config.reconnect_backoff = Duration::from_millis(500);
    let actor = ClobMarketWsActor::new(config, discovery)?;

    let (producer, mut consumer) = RingBuffer::new(DEFAULT_RING_CAPACITY);
    let (health_tx, mut health_rx) = mpsc::channel(1024);
    let (cmd_tx, cmd_rx) = mpsc::channel(16);
    let market_task = tokio::spawn(actor.run(producer, health_tx, cmd_rx));

    // Authenticated user feed. Gamma edge credentials do not authenticate this feed.
    let mut user_task = None;
    let mut user_snapshot = None;
    let mut shutdown_tx = None;
    let mut user_permit = None;
    let user_status: String;
    if profile.has_trading_api_credentials() {
        if ws_version == NexWsVersion::SubsV2 {
            user_permit = Some(budget.reserve_user()?);
        }
        let (user_shutdown_tx, user_shutdown_rx) = watch::channel(false);
        match open_user_feed(
            profile,
            &market.condition_id,
            up_asset_id,
            dn_asset_id,
            ws_version,
        )
        .await
        {
            Ok((actor, handle, preflight)) => {
                user_status = format!("authenticated (preflight open orders: {preflight})");
                user_snapshot = Some(handle);
                shutdown_tx = Some(user_shutdown_tx);
                user_task = Some(tokio::spawn(actor.run(user_shutdown_rx)));
            }
            Err(err) => {
                // Release the reservation: no user socket was opened.
                user_permit = None;
                user_status = format!("blocked: {err}");
            }
        }
    } else {
        user_status =
            "blocked: NEX_API_KEY / NEX_API_SECRET / NEX_API_PASSPHRASE not configured".to_string();
    }

    println!("(e) LIVE FEEDS");
    println!(
        "    Market path:            {}",
        profile.market_ws_url(ws_version)
    );
    println!(
        "    User path:              {}",
        profile.user_ws_url(ws_version)
    );
    println!("    User feed:              {user_status}");
    println!(
        "    Socket budget (subs-v2): local {:?} shared_nat={} caps market={} user_ip={} user_key={}",
        budget.usage(),
        profile.shared_nat_sockets,
        core_types::NEX_V2_MARKET_CAP_PER_IP,
        core_types::NEX_V2_USER_CAP_PER_IP,
        core_types::NEX_V2_USER_CAP_PER_KEY
    );

    let mut state = FeedSessionState::new();
    let started = Instant::now();
    let mut reconnect_sent = false;
    let mut printed_market_events = 0_usize;
    let mut last_user_state = String::new();
    let tick = Duration::from_millis(100);
    let mut last_readiness = None;

    while started.elapsed() < duration {
        tokio::time::sleep(tick).await;

        if let Some(after) = reconnect_after_secs
            && !reconnect_sent
            && started.elapsed() >= Duration::from_secs(after)
        {
            reconnect_sent = true;
            println!("    [probe] forcing a real socket reconnect to exercise snapshot rebuild");
            let _ = cmd_tx.send(ClobWsCommand::ForceReconnect).await;
        }

        while let Ok(health) = health_rx.try_recv() {
            match &health {
                FeedHealthEvent::Connected { .. } => {
                    state.connected_sessions = state.connected_sessions.saturating_add(1);
                    state.snapshot_seen = false;
                    state.reset_books();
                    println!(
                        "    [health] market session {} connected (snapshot required)",
                        state.connected_sessions
                    );
                }
                FeedHealthEvent::Ready { .. } => {
                    state.ready_events = state.ready_events.saturating_add(1);
                    println!("    [health] market READY after snapshot/REST resync");
                }
                FeedHealthEvent::ResyncingRest { .. } => {
                    state.snapshot_seen = false;
                    state.reset_books();
                    println!("    [health] market reconnecting: REST snapshot resync");
                }
                FeedHealthEvent::Disconnected { reason, .. } => {
                    state.snapshot_seen = false;
                    state.reset_books();
                    println!("    [health] market disconnected: {reason}");
                }
                FeedHealthEvent::StaleTimeout { silent_ms, .. } => {
                    state.stale_events = state.stale_events.saturating_add(1);
                    state.snapshot_seen = false;
                    state.reset_books();
                    println!("    [health] STALE FEED ({silent_ms}ms silent): quoting blocked");
                }
                other => println!("    [health] {other:?}"),
            }
            write_sandbox_event(&mut jsonl, "sandbox_feed_health", &health)?;
        }

        while let Ok(event) = consumer.pop() {
            state.last_market_msg = Some(Instant::now());
            if let PolyMarketEvent::BookSnapshot { outcome, book, .. } = &event {
                state.snapshots_seen = state.snapshots_seen.saturating_add(1);
                state.snapshot_seen = true;
                let has_liquidity = book.bid_len() > 0 || book.ask_len() > 0;
                match outcome {
                    Outcome::Up => state.up_has_liquidity = has_liquidity,
                    Outcome::Down => state.dn_has_liquidity = has_liquidity,
                }
            }
            if printed_market_events < 6 {
                match &event {
                    PolyMarketEvent::BookSnapshot {
                        outcome,
                        book,
                        exch_ts_ms,
                        ..
                    } => println!(
                        "    [market] snapshot {:?} bids={} asks={} exch_ms={}",
                        outcome,
                        book.bid_len(),
                        book.ask_len(),
                        exch_ts_ms
                    ),
                    PolyMarketEvent::PriceChange {
                        outcome,
                        price,
                        size,
                        ..
                    } => println!("    [market] delta {:?} {} x {}", outcome, price.0, size.0),
                    PolyMarketEvent::LastTrade {
                        outcome,
                        price,
                        size,
                        ..
                    } => println!("    [market] trade {:?} {} x {}", outcome, price.0, size.0),
                    _ => println!("    [market] {event:?}"),
                }
                printed_market_events += 1;
            }
            write_sandbox_event(&mut jsonl, "sandbox_market_event", &event)?;
        }

        if let Some(handle) = &user_snapshot {
            let snap = handle.load();
            let state_key = format!(
                "reconciling={} confirmed_up={} confirmed_dn={} unconfirmed_up={} unconfirmed_dn={} anomalies={}",
                snap.reconciling,
                snap.confirmed_up,
                snap.confirmed_dn,
                snap.unconfirmed_up,
                snap.unconfirmed_dn,
                snap.anomalies
            );
            if state_key != last_user_state {
                println!("    [user] {state_key}");
                last_user_state = state_key;
                write_sandbox_event(&mut jsonl, "sandbox_user_snapshot", &*snap)?;
            }
            state.user_ready = !snap.reconciling;
            if !snap.reconciling {
                state.user_reconcile_seen = true;
            }
        }

        let readiness = state.readiness(execution::NEX_STALE_AFTER_MS);
        if last_readiness != Some(readiness.quoting_allowed()) {
            println!(
                "    [gate] quoting_allowed={} (snapshot={} liquidity={} fresh={} user_ready={})",
                readiness.quoting_allowed(),
                readiness.snapshot_seen,
                readiness.book_has_liquidity,
                readiness
                    .last_market_msg_age_ms
                    .is_some_and(|age| age <= readiness.stale_after_ms),
                readiness.user_ready
            );
            last_readiness = Some(readiness.quoting_allowed());
            write_sandbox_event(&mut jsonl, "sandbox_feed_readiness", &readiness)?;
        }
    }

    let _ = cmd_tx.send(ClobWsCommand::Shutdown).await;
    if let Some(tx) = shutdown_tx {
        let _ = tx.send(true);
    }
    let market_stats = match tokio::time::timeout(Duration::from_secs(5), market_task).await {
        Ok(Ok(Ok(stats))) => Some(stats),
        Ok(Ok(Err(err))) => {
            println!("    market actor error: {err}");
            None
        }
        Ok(Err(join_err)) => {
            println!("    market actor join error: {join_err}");
            None
        }
        Err(_) => {
            println!("    market actor did not stop within 5s");
            None
        }
    };
    if let Some(task) = user_task {
        let _ = tokio::time::timeout(Duration::from_secs(5), task).await;
    }
    drop(user_permit);

    let final_readiness = state.readiness(execution::NEX_STALE_AFTER_MS);
    #[derive(Serialize)]
    struct FeedSummary<'a> {
        origin: DataOrigin,
        ws_version: &'a str,
        market_path: String,
        duration_secs: u64,
        frames_received: u64,
        pongs_received: u64,
        reconnects: u64,
        stale_timeouts: u64,
        connected_sessions: u64,
        snapshots_seen: u64,
        ready_events: u64,
        snapshot_rebuild_ok: bool,
        book_has_liquidity: bool,
        quoting_allowed: bool,
        user_feed: &'a str,
        user_reconciled: bool,
    }
    let summary = FeedSummary {
        origin: DataOrigin::Sandbox,
        ws_version: match ws_version {
            NexWsVersion::Original => "original",
            NexWsVersion::SubsV2 => "subs-v2",
        },
        market_path: profile.market_ws_url(ws_version),
        duration_secs: duration.as_secs(),
        frames_received: market_stats.map_or(0, |s| s.frames_received),
        pongs_received: market_stats.map_or(0, |s| s.pongs_received),
        reconnects: market_stats.map_or(0, |s| s.reconnects),
        stale_timeouts: state.stale_events,
        connected_sessions: state.connected_sessions,
        snapshots_seen: state.snapshots_seen,
        ready_events: state.ready_events,
        snapshot_rebuild_ok: state.connected_sessions >= 2
            && state.snapshots_seen >= 2 * state.connected_sessions,
        book_has_liquidity: state.book_has_liquidity(),
        quoting_allowed: final_readiness.quoting_allowed(),
        user_feed: &user_status,
        user_reconciled: state.user_reconcile_seen,
    };
    println!("--------------------------------------------------------------------------------");
    println!("(f) FEED SUMMARY");
    println!(
        "    frames={} pongs={} sessions={} snapshots={} ready={} reconnects={} stale={}",
        summary.frames_received,
        summary.pongs_received,
        summary.connected_sessions,
        summary.snapshots_seen,
        summary.ready_events,
        summary.reconnects,
        summary.stale_timeouts
    );
    println!(
        "    snapshot_rebuild_ok={} book_has_liquidity={} quoting_allowed={} user_reconciled={}",
        summary.snapshot_rebuild_ok,
        summary.book_has_liquidity,
        summary.quoting_allowed,
        summary.user_reconciled
    );
    write_sandbox_event(&mut jsonl, "sandbox_feeds_summary", &summary)?;
    jsonl.close()?;
    Ok(())
}

async fn open_user_feed(
    profile: &SandboxProfile,
    condition_id: &str,
    up_asset_id: &str,
    dn_asset_id: &str,
    ws_version: NexWsVersion,
) -> Result<
    (
        ClobUserWsActor,
        std::sync::Arc<arc_swap::ArcSwap<execution::OrderRegistrySnapshot>>,
        String,
    ),
    Box<dyn Error + Send + Sync>,
> {
    let maker_address = match (profile.wallet_mode, &profile.trading_address) {
        (_, Some(addr)) => addr.clone(),
        (WalletMode::Eoa, None) => {
            let key = profile
                .private_key
                .as_ref()
                .ok_or("NEX_TRADING_ADDRESS or POLYMARKET_PRIVATE_KEY required")?;
            let signer = OrderSigner::new(key, WalletMode::Eoa, None)?;
            format!("{:#x}", signer.eoa_address())
        }
        (mode, None) => {
            return Err(format!("{mode:?} requires NEX_TRADING_ADDRESS").into());
        }
    };
    let api_key = profile.api_key.as_ref().ok_or("NEX_API_KEY missing")?;
    let api_secret = profile
        .api_secret
        .as_ref()
        .ok_or("NEX_API_SECRET missing")?;
    let passphrase = profile
        .api_passphrase
        .clone()
        .ok_or("NEX_API_PASSPHRASE missing")?;
    let signer = L2HmacSigner::new(
        maker_address.clone(),
        secrecy::ExposeSecret::expose_secret(api_key),
        api_secret,
        passphrase.clone(),
    )?;
    let http = NexHttp::new(profile)?;
    let rest = execution::ClobRestClient::with_http_client(
        &profile.clob_url,
        signer,
        http.client().clone(),
    );
    // Preflight authenticated REST: Gamma credentials do not work here.
    let preflight = rest
        .get_open_orders(Some(condition_id), None)
        .await
        .map(|orders| format!("{} open", orders.len()))?;

    let creds = UserWsCredentials {
        api_key: secrecy::SecretString::new(
            secrecy::ExposeSecret::expose_secret(api_key)
                .to_string()
                .into(),
        ),
        secret: secrecy::SecretString::new(
            secrecy::ExposeSecret::expose_secret(api_secret)
                .to_string()
                .into(),
        ),
        passphrase: secrecy::SecretString::new(passphrase.expose_secret().to_string().into()),
    };
    let reconciler = FillReconciler::new_with_origin(
        DataOrigin::Sandbox,
        &maker_address,
        up_asset_id,
        dn_asset_id,
    );
    let actor = ClobUserWsActor::new(
        profile.user_ws_url(ws_version),
        creds,
        condition_id,
        &maker_address,
        rest,
        reconciler,
    );
    let handle = actor.snapshot_handle();
    Ok((actor, handle, preflight))
}
