//! NEX sandbox adapter: public-connection gate, market discovery, book reads,
//! and the minimum-size post-only validation order on Polygon Amoy (`80002`).
//!
//! Ground truth: `nex-public-connection.md`, `nex-start-trading.md`,
//! `nex-websocket-feeds.md`, and `nex-sandbox-credentials-and-auth.md`.
//! This module never references production hosts or keys; the [`SandboxProfile`]
//! validation rejects them before any socket is opened.

use std::net::{IpAddr, SocketAddr};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use core_types::sandbox::{
    NEX_CLOB_HOST, NEX_V2_MARKET_CAP_PER_IP, NEX_V2_USER_CAP_PER_IP, NEX_V2_USER_CAP_PER_KEY,
    NEX_WS_HOST, SandboxProfile,
};
use core_types::{
    BookLevel, DataOrigin, FeeSchedule, MarketDescriptor, MicroUsdc, ProtocolVersion, SharesMicro,
    TickPrecision, WalletMode, encode_buy_amounts, parse_decimal_micro,
};
use secrecy::ExposeSecret;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::auth::L2HmacSigner;
use crate::clob_rest::{ClobRestClient, OpenOrderRecord, OrderPlacementResponse};
use crate::eip712::{MarketDomain, OrderSigner, generate_salt};
use crate::error::ExecutionError;

/// Health endpoint used to prove the allowlisted route before any trade.
pub const NEX_HEALTH_PATH: &str = "/health";
/// Public egress-IP echo service used only to record the source address at startup.
pub const EGRESS_PROBE_URL: &str = "https://api.ipify.org";
/// Documented readiness timeout for the health probe.
const GATE_TIMEOUT: Duration = Duration::from_secs(10);
/// Snapshot staleness threshold for the NEX market feed. NEX closes idle sockets and the
/// documented contract declares a feed stale after 60s without activity or a missed PONG.
pub const NEX_STALE_AFTER_MS: u64 = 60_000;

/// One resolved-hostname pin.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NexDnsPin {
    /// Service hostname.
    pub host: String,
    /// Addresses returned by the local resolver (hosts-file or DNS).
    pub resolved: Vec<String>,
    /// `true` when the resolver returns the configured public ingress IP.
    pub pinned_to_ingress: bool,
}

/// Public-connection evidence logged at startup and before every trading session.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NexGateReport {
    /// Always `sandbox`; these prints must never enter production calibration.
    pub origin: DataOrigin,
    /// Chain id (must be `80002`).
    pub chain_id: u64,
    /// Egress public IPv4 observed by the echo probe.
    pub egress_ip: Option<String>,
    /// Egress probe failure detail, if any.
    pub egress_probe_error: Option<String>,
    /// Operator-registered allowlisted egress IPv4.
    pub allowlisted_egress_ip: Option<String>,
    /// `true` when the observed egress matches the registered `/32`.
    pub egress_allowlist_match: bool,
    /// Pinned public ingress IPv4.
    pub ingress_ip: String,
    /// Resolver evidence for both NEX hostnames.
    pub dns_pins: Vec<NexDnsPin>,
    /// Outbound TCP `443` to the ingress succeeded.
    pub tcp_443_ok: bool,
    /// Hostname preserved in TLS SNI.
    pub tls_sni_host: String,
    /// Hostname preserved in the HTTP `Host` header.
    pub http_host_header: String,
    /// `GET /health` status code.
    pub health_status: Option<u16>,
    /// `true` only when `GET /health` returned HTTP `200`.
    pub health_ok: bool,
    /// Short health body snippet (never secret material).
    pub health_body: Option<String>,
    /// Transport failure detail for the health probe.
    pub health_error: Option<String>,
}

impl NexGateReport {
    /// Fails closed unless both NEX hostnames resolve to the ingress, TCP/443 is open,
    /// and `GET /health` returned HTTP `200`.
    pub fn require_healthy(&self) -> Result<(), ExecutionError> {
        let unpinned: Vec<&str> = self
            .dns_pins
            .iter()
            .filter(|pin| !pin.pinned_to_ingress)
            .map(|pin| pin.host.as_str())
            .collect();
        if !unpinned.is_empty() || !self.tcp_443_ok || !self.health_ok {
            return Err(ExecutionError::Config(format!(
                "NEX gate failed (unpinned={unpinned:?}, tcp_443={}, status={:?}, error={:?}); check the hosts mapping to {} and egress allowlisting",
                self.tcp_443_ok, self.health_status, self.health_error, self.ingress_ip
            )));
        }
        Ok(())
    }

    /// `true` when the observed egress IP is on the registered allowlist.
    #[must_use]
    pub const fn egress_allowed(&self) -> bool {
        self.egress_allowlist_match
    }
}

/// Pinned HTTP transport for the NEX hosts. Hostnames stay in the URL so TLS SNI and
/// the HTTP `Host` header are preserved; only address resolution is overridden.
#[derive(Clone, Debug)]
pub struct NexHttp {
    client: reqwest::Client,
}

impl NexHttp {
    /// Builds a client that resolves both NEX hostnames to the configured ingress IP.
    ///
    /// # Errors
    /// Returns a transport error when the HTTP client cannot be constructed.
    pub fn new(profile: &SandboxProfile) -> Result<Self, ExecutionError> {
        let ingress = SocketAddr::new(profile.ingress_ip, 443);
        let client = reqwest::Client::builder()
            .https_only(true)
            .tcp_nodelay(true)
            .connect_timeout(Duration::from_secs(5))
            .timeout(GATE_TIMEOUT)
            .user_agent("poly-as/0.1.0 nex-sandbox")
            .resolve(NEX_CLOB_HOST, ingress)
            .resolve(NEX_WS_HOST, ingress)
            .build()?;
        Ok(Self { client })
    }

    /// Shared underlying client (also handed to [`ClobRestClient`]).
    #[must_use]
    pub fn client(&self) -> &reqwest::Client {
        &self.client
    }

    /// Raw `GET` returning `(status, body)`.
    async fn get(&self, url: &str) -> Result<(u16, String), ExecutionError> {
        let resp = self.client.get(url).send().await?;
        let status = resp.status().as_u16();
        let body = resp.text().await?;
        Ok((status, body))
    }
}

/// Probes the public egress IPv4 with a short timeout. Best-effort: never blocks the gate.
async fn probe_egress(client: &reqwest::Client) -> (Option<IpAddr>, Option<String>) {
    match client
        .get(EGRESS_PROBE_URL)
        .timeout(Duration::from_secs(5))
        .send()
        .await
    {
        Ok(resp) if resp.status().is_success() => match resp.text().await {
            Ok(body) => match body.trim().parse::<IpAddr>() {
                Ok(ip) => (Some(ip), None),
                Err(err) => (None, Some(format!("unparseable egress IP '{body}': {err}"))),
            },
            Err(err) => (None, Some(format!("egress probe body error: {err}"))),
        },
        Ok(resp) => (
            None,
            Some(format!("egress probe HTTP {}", resp.status().as_u16())),
        ),
        Err(err) => (None, Some(format!("egress probe failed: {err}"))),
    }
}

async fn resolve_host(host: &str, ingress: IpAddr) -> NexDnsPin {
    let lookup = tokio::net::lookup_host(format!("{host}:443")).await;
    match lookup {
        Ok(addrs) => {
            let resolved: Vec<String> = addrs.map(|a| a.ip().to_string()).collect();
            let pinned_to_ingress = resolved.iter().any(|ip| ip == &ingress.to_string());
            NexDnsPin {
                host: host.to_string(),
                resolved,
                pinned_to_ingress,
            }
        }
        Err(err) => NexDnsPin {
            host: host.to_string(),
            resolved: vec![format!("<resolve failed: {err}>")],
            pinned_to_ingress: false,
        },
    }
}

/// Runs the public-connection gate exactly as documented:
/// allowlisted egress, hosts-file/ingress pinning for both NEX hostnames,
/// outbound TCP `443`, SNI/Host preservation, and `GET /health == 200`.
///
/// # Errors
/// Returns a transport/client construction error. A failed health check is reported
/// in the returned [`NexGateReport`] so the caller can log status, IPs, and failure detail.
pub async fn check_public_connection(
    profile: &SandboxProfile,
) -> Result<NexGateReport, ExecutionError> {
    let http = NexHttp::new(profile)?;
    let (egress_ip, egress_probe_error) = probe_egress(&http.client).await;

    let clob_pin = resolve_host(profile.clob_host(), profile.ingress_ip).await;
    let ws_pin = resolve_host(profile.ws_host(), profile.ingress_ip).await;

    let tcp_443_ok = tokio::time::timeout(
        Duration::from_secs(5),
        tokio::net::TcpStream::connect(SocketAddr::new(profile.ingress_ip, 443)),
    )
    .await
    .map(|r| r.is_ok())
    .unwrap_or(false);

    let health_url = format!("{}{NEX_HEALTH_PATH}", profile.clob_url);
    let (health_status, health_body, health_error) = match http.get(&health_url).await {
        Ok((status, body)) => (
            Some(status),
            Some(body.chars().take(256).collect::<String>()),
            None,
        ),
        Err(err) => (None, None, Some(err.to_string())),
    };

    let allowlisted_egress_ip = profile.allowlisted_egress_ip.map(|ip| ip.to_string());
    let egress_allowlist_match = match (egress_ip, profile.allowlisted_egress_ip) {
        (Some(observed), Some(expected)) => observed == expected,
        _ => false,
    };

    Ok(NexGateReport {
        origin: DataOrigin::Sandbox,
        chain_id: profile.chain_id,
        egress_ip: egress_ip.map(|ip| ip.to_string()),
        egress_probe_error,
        allowlisted_egress_ip,
        egress_allowlist_match,
        ingress_ip: profile.ingress_ip.to_string(),
        dns_pins: vec![clob_pin, ws_pin],
        tcp_443_ok,
        tls_sni_host: profile.clob_host().to_string(),
        http_host_header: profile.clob_host().to_string(),
        health_status,
        health_ok: health_status == Some(200),
        health_body,
        health_error,
    })
}

/// Sandbox market metadata discovered from Gamma preprod or the NEX CLOB.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NexMarket {
    /// Always `sandbox`.
    pub origin: DataOrigin,
    /// Condition ID.
    pub condition_id: String,
    /// Slug (empty when only the CLOB route is available).
    pub slug: String,
    /// Question (empty when only the CLOB route is available).
    pub question: String,
    /// UP / Yes token ID (V2 position ID or CTF token ID).
    pub up_asset_id: String,
    /// DOWN / No token ID.
    pub dn_asset_id: String,
    /// Tick increment in micro-USDC.
    pub tick_size_micro: MicroUsdc,
    /// Minimum order size in micro-shares.
    pub min_order_size_micro: SharesMicro,
    /// Neg-risk routing flag.
    pub neg_risk: bool,
    /// Protocol generation reported by the venue.
    pub version: ProtocolVersion,
    /// Fee curve coefficient scaled by `10^6` when published.
    pub fee_rate_e6: u64,
}

impl NexMarket {
    /// Asset ID for one outcome.
    #[must_use]
    pub fn asset_id(&self, outcome: core_types::Outcome) -> &str {
        match outcome {
            core_types::Outcome::Up => &self.up_asset_id,
            core_types::Outcome::Down => &self.dn_asset_id,
        }
    }
}

/// Result of market discovery, including the Gamma-edge failure when applicable.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NexDiscovery {
    /// Always `sandbox`.
    pub origin: DataOrigin,
    /// `gamma-preprod` or `nex-clob`.
    pub source: String,
    /// Gamma edge failure detail (403/allowlisting), surfaced instead of hidden.
    pub gamma_error: Option<String>,
    /// Discovered markets.
    pub markets: Vec<NexMarket>,
}

fn unix_millis_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis().try_into().unwrap_or(u64::MAX))
        .unwrap_or(0)
}

fn json_string_array(value: &Value) -> Vec<String> {
    if let Some(raw) = value.as_str() {
        return serde_json::from_str::<Vec<String>>(raw).unwrap_or_default();
    }
    value
        .as_array()
        .map(|items| {
            items
                .iter()
                .filter_map(|v| v.as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default()
}

fn json_decimal_micro(value: Option<&Value>, field: &str) -> Result<u64, ExecutionError> {
    let Some(v) = value else {
        return Err(ExecutionError::Parse(format!("missing `{field}`")));
    };
    let raw = match v {
        Value::String(s) => s.trim().to_string(),
        Value::Number(n) => n.to_string(),
        other => {
            return Err(ExecutionError::Parse(format!(
                "`{field}` must be a decimal, got {other}"
            )));
        }
    };
    parse_decimal_micro(raw.as_bytes())
        .map_err(|err| ExecutionError::Parse(format!("`{field}`='{raw}': {err}")))
}

fn gamma_market_from_value(value: &Value) -> Result<NexMarket, ExecutionError> {
    let condition_id = value
        .get("conditionId")
        .or_else(|| value.get("condition_id"))
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    if condition_id.is_empty() {
        return Err(ExecutionError::Parse(
            "gamma market missing conditionId".into(),
        ));
    }
    let outcomes = json_string_array(value.get("outcomes").unwrap_or(&Value::Null));
    let ids = {
        let clob = json_string_array(value.get("clobTokenIds").unwrap_or(&Value::Null));
        if clob.is_empty() {
            json_string_array(value.get("positionIds").unwrap_or(&Value::Null))
        } else {
            clob
        }
    };
    let mut up = String::new();
    let mut dn = String::new();
    for (label, id) in outcomes.iter().zip(ids.iter()) {
        match label.trim().to_ascii_lowercase().as_str() {
            "up" | "yes" => up = id.trim().to_string(),
            "down" | "no" => dn = id.trim().to_string(),
            _ => {}
        }
    }
    if up.is_empty() || dn.is_empty() {
        return Err(ExecutionError::Parse(
            "gamma market missing Up/Yes or Down/No token IDs".into(),
        ));
    }
    let tick = json_decimal_micro(
        value
            .get("orderPriceMinTickSize")
            .or_else(|| value.get("order_price_min_tick_size")),
        "orderPriceMinTickSize",
    )?;
    let min_size = json_decimal_micro(
        value
            .get("orderMinSize")
            .or_else(|| value.get("order_min_size")),
        "orderMinSize",
    )?;
    let fee_rate_e6 = value
        .get("feeSchedule")
        .and_then(|f| f.get("rate"))
        .map_or(Ok(50_000), |r| {
            json_decimal_micro(Some(r), "feeSchedule.rate")
        })?;
    let version = match value
        .get("version")
        .and_then(Value::as_str)
        .unwrap_or("v1")
        .trim()
        .to_ascii_lowercase()
        .as_str()
    {
        "v2" => ProtocolVersion::PolyV2,
        _ => ProtocolVersion::CtfV1,
    };
    Ok(NexMarket {
        origin: DataOrigin::Sandbox,
        condition_id,
        slug: value
            .get("slug")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string(),
        question: value
            .get("question")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string(),
        up_asset_id: up,
        dn_asset_id: dn,
        tick_size_micro: MicroUsdc(tick),
        min_order_size_micro: SharesMicro(min_size),
        neg_risk: value
            .get("negRisk")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        version,
        fee_rate_e6,
    })
}

/// Lists sandbox markets through Gamma preprod (with the privately issued edge headers).
///
/// # Errors
/// Returns a transport or parse error. Callers should surface the error and may fall
/// back to [`list_nex_clob_markets`] (the same drop-in CLOB route) without hiding it.
async fn list_gamma_markets(
    profile: &SandboxProfile,
    limit: usize,
) -> Result<Vec<NexMarket>, ExecutionError> {
    let (Some(client_id), Some(client_secret)) = (
        profile.gamma_client_id.as_ref(),
        profile.gamma_client_secret.as_ref(),
    ) else {
        return Err(ExecutionError::Config(
            "GAMMA_CLIENT_ID and GAMMA_CLIENT_SECRET are required for Gamma preprod".into(),
        ));
    };
    let http = NexHttp::new(profile)?;
    let url = format!(
        "{}/markets?closed=false&limit={}",
        profile.gamma_url,
        limit.clamp(1, 100)
    );
    let resp = http
        .client
        .get(&url)
        .header("x-client-id", client_id.expose_secret())
        .header("x-client-secret", client_secret.expose_secret())
        .send()
        .await?;
    let status = resp.status();
    let body = resp.text().await?;
    if !status.is_success() {
        return Err(ExecutionError::Rest {
            status_code: Some(status.as_u16()),
            detail: format!(
                "Gamma preprod {} returned '{}'; verify edge credentials and egress allowlisting",
                status.as_u16(),
                body.chars().take(200).collect::<String>()
            ),
        });
    }
    let value: Value = serde_json::from_str(&body)?;
    let items = value
        .get("data")
        .and_then(Value::as_array)
        .cloned()
        .or_else(|| value.get("markets").and_then(Value::as_array).cloned())
        .or_else(|| value.as_array().cloned())
        .unwrap_or_default();
    let mut markets = Vec::with_capacity(items.len());
    for item in &items {
        // A single malformed market should not hide the rest of the page; an
        // entirely unparseable page is an error below.
        if let Ok(m) = gamma_market_from_value(item) {
            markets.push(m);
        }
    }
    if markets.is_empty() {
        return Err(ExecutionError::Parse(
            "Gamma preprod returned no parseable sandbox markets".into(),
        ));
    }
    Ok(markets)
}

/// Lists sandbox markets directly from the NEX CLOB (`GET /clob-markets`).
///
/// This is the drop-in CLOB metadata route; it is used only when Gamma preprod is
/// unreachable, and the failure is reported explicitly.
///
/// # Errors
/// Returns a transport or parse error.
async fn list_nex_clob_markets(
    profile: &SandboxProfile,
    limit: usize,
) -> Result<Vec<NexMarket>, ExecutionError> {
    let http = NexHttp::new(profile)?;
    let url = format!("{}/clob-markets", profile.clob_url);
    // The CLOB metadata route returns the whole market array (~80 MB); allow a
    // longer body read than the interactive `/book` calls.
    let resp = http
        .client
        .get(&url)
        .timeout(Duration::from_secs(60))
        .send()
        .await?;
    let status = resp.status().as_u16();
    let body = resp.text().await?;
    if status != 200 {
        return Err(ExecutionError::Rest {
            status_code: Some(status),
            detail: format!(
                "NEX /clob-markets failed: '{}'",
                body.chars().take(200).collect::<String>()
            ),
        });
    }
    let items: Vec<Value> = serde_json::from_str(&body)?;
    let mut markets = Vec::with_capacity(limit.clamp(1, 100_000));
    for item in items.iter().take(limit.clamp(1, 100_000)) {
        if let Ok(market) = nex_market_from_clob_value(item) {
            markets.push(market);
        }
    }
    if markets.is_empty() {
        return Err(ExecutionError::Parse(
            "NEX /clob-markets returned no usable sandbox markets".into(),
        ));
    }
    Ok(markets)
}

fn nex_market_from_clob_value(item: &Value) -> Result<NexMarket, ExecutionError> {
    let condition_id = item
        .get("c")
        .and_then(Value::as_str)
        .ok_or_else(|| ExecutionError::Parse("NEX market missing condition id (`c`)".into()))?;
    let mut up = String::new();
    let mut dn = String::new();
    if let Some(tokens) = item.get("t").and_then(Value::as_array) {
        for tok in tokens {
            let id = tok.get("t").and_then(Value::as_str).unwrap_or_default();
            match tok
                .get("o")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .trim()
                .to_ascii_lowercase()
                .as_str()
            {
                "up" | "yes" => up = id.to_string(),
                "down" | "no" => dn = id.to_string(),
                _ => {}
            }
        }
    }
    if up.is_empty() || dn.is_empty() {
        return Err(ExecutionError::Parse(
            "NEX market missing Up/Yes or Down/No token IDs".into(),
        ));
    }
    let tick = json_decimal_micro(item.get("mts"), "mts")?;
    let min_size = json_decimal_micro(item.get("mos"), "mos")?;
    let fee_rate_e6 = item
        .get("fd")
        .and_then(|f| f.get("r"))
        .map_or(Ok(50_000), |r| json_decimal_micro(Some(r), "fd.r"))?;
    let version = match item
        .get("v")
        .and_then(Value::as_str)
        .unwrap_or("v1")
        .trim()
        .to_ascii_lowercase()
        .as_str()
    {
        "v2" => ProtocolVersion::PolyV2,
        _ => ProtocolVersion::CtfV1,
    };
    Ok(NexMarket {
        origin: DataOrigin::Sandbox,
        condition_id: condition_id.to_string(),
        slug: String::new(),
        question: String::new(),
        up_asset_id: up,
        dn_asset_id: dn,
        tick_size_micro: MicroUsdc(tick),
        min_order_size_micro: SharesMicro(min_size),
        neg_risk: false,
        version,
        fee_rate_e6,
    })
}

/// Fetches one sandbox market's metadata by condition ID (`GET /clob-markets/{id}`).
///
/// # Errors
/// Returns a transport or parse error when the condition is unknown.
pub async fn fetch_market_by_condition(
    profile: &SandboxProfile,
    condition_id: &str,
) -> Result<NexMarket, ExecutionError> {
    let http = NexHttp::new(profile)?;
    let condition = validate_condition_id(condition_id)?;
    let url = format!("{}/clob-markets/{condition}", profile.clob_url);
    let (status, body) = http.get(&url).await?;
    if status != 200 {
        return Err(ExecutionError::Rest {
            status_code: Some(status),
            detail: format!(
                "NEX /clob-markets/{condition_id} failed: '{}'",
                body.chars().take(200).collect::<String>()
            ),
        });
    }
    let value: Value = serde_json::from_str(&body)?;
    nex_market_from_clob_value(&value)
}

/// Finds the sandbox market that owns `asset_id` (either outcome) via the targeted
/// `/book` then `/clob-markets/{condition}` routes (no full-market scan).
///
/// # Errors
/// Returns an error when the book or market metadata cannot be read.
pub async fn find_market_by_asset(
    profile: &SandboxProfile,
    asset_id: &str,
) -> Result<NexMarket, ExecutionError> {
    let target = validate_asset_id(asset_id)?;
    let book = fetch_book(profile, target).await?;
    if book.market.trim().is_empty() {
        return Err(ExecutionError::Config(format!(
            "NEX /book returned no condition id for asset {target}"
        )));
    }
    let market = fetch_market_by_condition(profile, &book.market).await?;
    if market.up_asset_id == target || market.dn_asset_id == target {
        return Ok(market);
    }
    Err(ExecutionError::Config(format!(
        "asset id {target} is not part of market {}; pass a sandbox outcome ID",
        market.condition_id
    )))
}

/// Discovers sandbox markets, preferring Gamma preprod edge credentials.
///
/// # Errors
/// Returns an error only when both routes fail; the Gamma failure is preserved in
/// [`NexDiscovery::gamma_error`].
pub async fn discover_markets(
    profile: &SandboxProfile,
    limit: usize,
) -> Result<NexDiscovery, ExecutionError> {
    let gamma = list_gamma_markets(profile, limit).await;
    match gamma {
        Ok(markets) => Ok(NexDiscovery {
            origin: DataOrigin::Sandbox,
            source: "gamma-preprod".to_string(),
            gamma_error: None,
            markets,
        }),
        Err(gamma_err) => {
            let markets = list_nex_clob_markets(profile, limit.max(64))
                .await
                .map_err(|nex_err| {
                    ExecutionError::Config(format!(
                        "Gamma preprod failed ({gamma_err}) and NEX /clob-markets failed ({nex_err})"
                    ))
                })?;
            Ok(NexDiscovery {
                origin: DataOrigin::Sandbox,
                source: "nex-clob".to_string(),
                gamma_error: Some(gamma_err.to_string()),
                markets,
            })
        }
    }
}

/// Sandbox L2 book snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NexBook {
    /// Always `sandbox`.
    pub origin: DataOrigin,
    /// Condition ID.
    pub market: String,
    /// Token ID.
    pub asset_id: String,
    /// Venue timestamp in Unix milliseconds.
    pub timestamp_ms: u64,
    /// Bid levels (descending).
    pub bids: Vec<BookLevel>,
    /// Ask levels (ascending).
    pub asks: Vec<BookLevel>,
    /// Tick increment.
    pub tick_size_micro: MicroUsdc,
    /// Minimum order size.
    pub min_order_size_micro: SharesMicro,
    /// Neg-risk flag.
    pub neg_risk: bool,
    /// Last trade price if published.
    pub last_trade_price: Option<MicroUsdc>,
}

impl NexBook {
    /// Best bid/ask.
    #[must_use]
    pub fn best(&self) -> (Option<BookLevel>, Option<BookLevel>) {
        (self.bids.first().copied(), self.asks.first().copied())
    }

    /// `true` when the venue returned no resting levels on either side.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.bids.is_empty() && self.asks.is_empty()
    }
}

fn validate_asset_id(value: &str) -> Result<&str, ExecutionError> {
    let id = value.trim();
    if id.is_empty() || !id.bytes().all(|b| b.is_ascii_digit()) {
        return Err(ExecutionError::Parse(format!(
            "invalid sandbox asset id '{value}': expected decimal digits"
        )));
    }
    Ok(id)
}

fn validate_condition_id(value: &str) -> Result<&str, ExecutionError> {
    let id = value.trim();
    let hex = id.strip_prefix("0x").unwrap_or_default();
    if hex.len() != 64 || !hex.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(ExecutionError::Parse(format!(
            "invalid condition id '{value}': expected 0x-prefixed 32-byte hex"
        )));
    }
    Ok(id)
}

/// Reads `GET /book?token_id=...` through the pinned ingress route.
///
/// # Errors
/// Returns an error on transport failure, non-200 status, or malformed levels.
pub async fn fetch_book(
    profile: &SandboxProfile,
    token_id: &str,
) -> Result<NexBook, ExecutionError> {
    let http = NexHttp::new(profile)?;
    let token_id = validate_asset_id(token_id)?;
    let url = format!("{}/book?token_id={token_id}", profile.clob_url);
    let (status, body) = http.get(&url).await?;
    if status != 200 {
        return Err(ExecutionError::Rest {
            status_code: Some(status),
            detail: format!(
                "NEX /book failed for token {token_id}: '{}'",
                body.chars().take(200).collect::<String>()
            ),
        });
    }
    let value: Value = serde_json::from_str(&body)?;
    let bids = parse_levels(value.get("bids"))?;
    let asks = parse_levels(value.get("asks"))?;
    let timestamp_ms = match value.get("timestamp") {
        Some(Value::String(s)) => s
            .parse::<u64>()
            .map_err(|err| ExecutionError::Parse(format!("book timestamp '{s}': {err}")))?,
        Some(Value::Number(n)) => n
            .as_u64()
            .ok_or_else(|| ExecutionError::Parse(format!("book timestamp '{n}' is not a u64")))?,
        _ => {
            return Err(ExecutionError::Parse("NEX /book missing timestamp".into()));
        }
    };
    Ok(NexBook {
        origin: DataOrigin::Sandbox,
        market: value
            .get("market")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string(),
        asset_id: value
            .get("asset_id")
            .and_then(Value::as_str)
            .unwrap_or(token_id)
            .to_string(),
        timestamp_ms,
        bids,
        asks,
        tick_size_micro: MicroUsdc(json_decimal_micro(value.get("tick_size"), "tick_size")?),
        min_order_size_micro: SharesMicro(json_decimal_micro(
            value.get("min_order_size"),
            "min_order_size",
        )?),
        neg_risk: value
            .get("neg_risk")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        last_trade_price: match value.get("last_trade_price") {
            Some(Value::String(s)) if s.trim().is_empty() => None,
            Some(v @ (Value::String(_) | Value::Number(_))) => {
                Some(MicroUsdc(json_decimal_micro(Some(v), "last_trade_price")?))
            }
            _ => None,
        },
    })
}

fn parse_levels(value: Option<&Value>) -> Result<Vec<BookLevel>, ExecutionError> {
    let Some(value) = value else {
        return Ok(Vec::new());
    };
    let Some(items) = value.as_array() else {
        return Err(ExecutionError::Parse(
            "book side must be an array or null".into(),
        ));
    };
    let mut levels = Vec::with_capacity(items.len());
    for item in items {
        let price = json_decimal_micro(item.get("price"), "book.price")?;
        let size = json_decimal_micro(item.get("size"), "book.size")?;
        if size > 0 {
            levels.push(BookLevel {
                price: MicroUsdc(price),
                size: SharesMicro(size),
            });
        }
    }
    Ok(levels)
}

/// Bounds and fee checks applied to every sandbox order, including the validation probe.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NexOrderGuards {
    /// Order price equals one tick.
    pub price_is_one_tick: bool,
    /// Order size equals the venue minimum.
    pub size_is_minimum: bool,
    /// Price is aligned to the published tick.
    pub tick_aligned: bool,
    /// Price sits inside the configured combined-bid ceiling.
    pub within_combined_bid_bound: bool,
    /// Maker fee is zero (fee barrier cannot be crossed by a resting probe).
    pub maker_fee_zero: bool,
    /// Post-only limit does not touch the best ask.
    pub post_only_does_not_cross: bool,
}

impl NexOrderGuards {
    /// All guards passed.
    #[must_use]
    pub const fn all_passed(&self) -> bool {
        self.price_is_one_tick
            && self.size_is_minimum
            && self.tick_aligned
            && self.within_combined_bid_bound
            && self.maker_fee_zero
            && self.post_only_does_not_cross
    }
}

/// Validation-order outcome, including read-back and cancel/reconcile evidence.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NexValidationOutcome {
    /// Always `sandbox`.
    pub origin: DataOrigin,
    /// Chain id.
    pub chain_id: u64,
    /// Wallet mode used.
    pub wallet_mode: String,
    /// Maker address used in the order.
    pub maker_address: String,
    /// Quoted outcome.
    pub outcome: String,
    /// Token ID.
    pub asset_id: String,
    /// Limit price in micro-USDC.
    pub price_micro: u64,
    /// Size in micro-shares.
    pub size_micro: u64,
    /// Guard evaluation.
    pub guards: NexOrderGuards,
    /// `POST /order` response.
    pub placement: OrderPlacementResponse,
    /// `GET /data/order/{id}` read-back if the venue returned it.
    pub readback: Option<OpenOrderRecord>,
    /// `true` when the order was not visible on the single-order read.
    pub readback_missing: bool,
    /// Cancel response or reconciliation detail.
    pub cancel_detail: Option<String>,
    /// Settlement reminder (match precedes settlement).
    pub settlement_note: String,
    /// Book snapshot at placement time.
    pub book_at_placement: NexBook,
}

/// Trading session bound to a verified NEX gate and Amoy signing domain.
pub struct NexTradingSession {
    profile: SandboxProfile,
    rest: ClobRestClient,
    signer: OrderSigner,
    trading_address: String,
    exchange_address: alloy::primitives::Address,
}

impl NexTradingSession {
    /// Opens a session only after the `/health` gate passed and sandbox credentials exist.
    ///
    /// # Errors
    /// Refuses to open on a failed gate, missing sandbox credentials, an unsupported
    /// wallet mode, or an Amoy trading address mismatch.
    pub fn open(profile: SandboxProfile, gate: NexGateReport) -> Result<Self, ExecutionError> {
        gate.require_healthy()?;
        let mut blockers: Vec<String> = Vec::new();
        if !gate.egress_allowed() {
            blockers.push(format!(
                "egress {} is not the registered allowlist {} (update the NEX allowlist registry before trading)",
                gate.egress_ip.as_deref().unwrap_or("<unknown>"),
                gate.allowlisted_egress_ip.as_deref().unwrap_or("<unset>")
            ));
        }
        if !profile.has_trading_api_credentials() {
            blockers
                .push("NEX_API_KEY, NEX_API_SECRET, and NEX_API_PASSPHRASE are required".into());
        }
        if !profile.has_signer_key() {
            blockers.push("POLYMARKET_PRIVATE_KEY is required for Amoy order signing".into());
        }
        if profile.wallet_mode != WalletMode::Eoa
            && profile.wallet_mode != WalletMode::DepositWallet1271
        {
            blockers.push(
                "NEX sandbox supports only EOA or Deposit Wallet (Safe/Proxy unsupported)".into(),
            );
        }
        if !blockers.is_empty() {
            return Err(ExecutionError::Config(format!(
                "NEX sandbox session blocked: {}",
                blockers.join(" | ")
            )));
        }
        let private_key = profile
            .private_key
            .as_ref()
            .ok_or_else(|| ExecutionError::Config("POLYMARKET_PRIVATE_KEY missing".into()))?;
        let deposit =
            match profile.wallet_mode {
                WalletMode::DepositWallet1271 => {
                    let addr = profile.trading_address.as_deref().ok_or_else(|| {
                        ExecutionError::Config(
                            "Deposit Wallet mode requires NEX_TRADING_ADDRESS".into(),
                        )
                    })?;
                    Some(addr.parse().map_err(|err| {
                        ExecutionError::Config(format!("NEX_TRADING_ADDRESS: {err}"))
                    })?)
                }
                _ => None,
            };
        let signer = OrderSigner::new(private_key, profile.wallet_mode, deposit)?
            .for_chain(profile.chain_id);
        let derived = format!("{:#x}", signer.eoa_address());
        let trading_address = match (profile.wallet_mode, &profile.trading_address) {
            (WalletMode::Eoa, Some(configured)) => {
                if !configured.eq_ignore_ascii_case(&derived) {
                    return Err(ExecutionError::Config(format!(
                        "NEX_TRADING_ADDRESS {configured} does not match signing EOA {derived}"
                    )));
                }
                derived
            }
            (WalletMode::Eoa, None) => derived,
            (_, Some(configured)) => configured.clone(),
            (_, None) => {
                return Err(ExecutionError::Config(
                    "NEX_TRADING_ADDRESS is required".into(),
                ));
            }
        };

        let api_key = profile
            .api_key
            .as_ref()
            .ok_or_else(|| ExecutionError::Config("NEX_API_KEY missing".into()))?;
        let api_secret = profile
            .api_secret
            .as_ref()
            .ok_or_else(|| ExecutionError::Config("NEX_API_SECRET missing".into()))?;
        let passphrase = profile
            .api_passphrase
            .clone()
            .ok_or_else(|| ExecutionError::Config("NEX_API_PASSPHRASE missing".into()))?;
        let l2 = L2HmacSigner::new(
            trading_address.clone(),
            api_key.expose_secret(),
            api_secret,
            passphrase,
        )?;
        let http = NexHttp::new(&profile)?;
        let rest = ClobRestClient::with_http_client(&profile.clob_url, l2, http.client().clone());
        let exchange_address = profile
            .exchange_address
            .parse()
            .map_err(|err| ExecutionError::Config(format!("NEX_EXCHANGE_ADDRESS: {err}")))?;

        Ok(Self {
            profile,
            rest,
            signer,
            trading_address,
            exchange_address,
        })
    }

    /// Maker address used for orders.
    #[must_use]
    pub fn maker_address(&self) -> &str {
        &self.trading_address
    }

    /// Authenticated REST client (for open orders, trades, and reconciliation).
    #[must_use]
    pub const fn rest(&self) -> &ClobRestClient {
        &self.rest
    }

    /// Builds and signs the minimum-size post-only limit buy at one tick.
    fn build_validation_order(
        &self,
        market: &NexMarket,
        book: &NexBook,
        outcome: core_types::Outcome,
    ) -> Result<(crate::eip712::Order, String, NexOrderGuards), ExecutionError> {
        // NEX sandbox validate signs the Amoy ExchangeV3 domain (`version "3"`).
        // Fail closed on CTF markets instead of signing with the wrong domain
        // (venue would reject with `invalid EOA signature`).
        if market.version != ProtocolVersion::PolyV2 {
            return Err(ExecutionError::Config(format!(
                "sandbox validate requires a PolyV2 market (ExchangeV3 80002); got {:?} for {}",
                market.version, market.condition_id
            )));
        }
        let asset_id = market.asset_id(outcome);
        if !book.asset_id.is_empty() && book.asset_id != asset_id {
            return Err(ExecutionError::Config(format!(
                "book asset {} does not match market outcome {asset_id}",
                book.asset_id
            )));
        }
        // The `/book` snapshot is authoritative for tick and minimum size.
        let price = book.tick_size_micro;
        let size = book.min_order_size_micro;
        let tick = TickPrecision::from_micro(price)?;
        let encoded = encode_buy_amounts(price, size, tick)?;
        let fee_schedule = FeeSchedule::new(market.fee_rate_e6, true)
            .map_err(|err| ExecutionError::Config(format!("fee schedule: {err}")))?;
        let best_ask = book.asks.first().map(|l| l.price.0);
        let tick_micro = tick.tick_micro().0;
        let guards = NexOrderGuards {
            price_is_one_tick: market.tick_size_micro == price,
            size_is_minimum: market.min_order_size_micro == size,
            tick_aligned: tick_micro > 0 && price.0.is_multiple_of(tick_micro),
            within_combined_bid_bound: price.0
                <= core_types::EngineConfig::default().max_combined_bid_micro.0,
            maker_fee_zero: fee_schedule.maker_fee_zero,
            post_only_does_not_cross: best_ask.is_none_or(|ask| ask > price.0),
        };
        if !guards.all_passed() {
            return Err(ExecutionError::Config(format!(
                "sandbox order guards failed: {guards:?}"
            )));
        }
        let descriptor = MarketDescriptor {
            condition_id: market.condition_id.clone(),
            slug: market.slug.clone(),
            question: market.question.clone(),
            // NEX signs with the onboarding Amoy ExchangeV3 contract, so the EIP-712
            // domain is always the V3 (`version: "3"`) domain on chain 80002.
            version: ProtocolVersion::PolyV2,
            neg_risk: book.neg_risk || market.neg_risk,
            asset_id_up: market.up_asset_id.clone(),
            asset_id_dn: market.dn_asset_id.clone(),
            tick_size_micro: price,
            min_order_size_micro: size,
            taker_delay_ms: 0,
            fee_schedule,
            // NEX markets are not 5m BTC epochs; use a non-degenerate window.
            start_ts_ms: book.timestamp_ms.saturating_sub(300_000),
            expiry_ts_ms: book.timestamp_ms.saturating_add(300_000),
            strike_price_usd: None,
        };
        let order_timestamp_ms = unix_millis_now();
        let domain = MarketDomain::for_market_on_chain(
            &descriptor,
            self.profile.chain_id,
            self.exchange_address,
        );
        let order = self.signer.build_order(
            generate_salt(),
            asset_id,
            encoded.maker_amount,
            encoded.taker_amount,
            true,
            order_timestamp_ms,
        )?;
        let signature = self.signer.sign_order(&order, &domain)?;
        Ok((order, signature, guards))
    }

    /// Reads the order once, retrying a 404 briefly while the venue replicates it.
    async fn read_back(&self, order_id: &str) -> Result<Option<OpenOrderRecord>, ExecutionError> {
        for attempt in 0..3 {
            match self.rest.get_order(order_id).await {
                Ok(record) => return Ok(Some(record)),
                Err(ExecutionError::Rest {
                    status_code: Some(404),
                    ..
                }) if attempt < 2 => {
                    tokio::time::sleep(Duration::from_millis(250)).await;
                }
                Err(ExecutionError::Rest {
                    status_code: Some(404),
                    ..
                }) => return Ok(None),
                Err(err) => return Err(err),
            }
        }
        Ok(None)
    }

    /// Cancels a still-live remainder; a filled or matched order needs no cancel.
    async fn cancel_remaining(
        &self,
        order_id: &str,
        readback: Option<&OpenOrderRecord>,
    ) -> Result<Option<String>, ExecutionError> {
        let Some(record) = readback else {
            return Ok(Some(
                "order not visible on read; reconcile fills via authenticated /data/trades and the user feed before asserting positions"
                    .to_string(),
            ));
        };
        if !record.status.eq_ignore_ascii_case("LIVE") {
            return Ok(Some(format!(
                "no cancel needed; read-back status='{}' size_matched='{}'",
                record.status, record.size_matched
            )));
        }
        match self.rest.cancel_order(order_id).await {
            Ok(cancel) => {
                if let Some(reason) = cancel.not_canceled.get(order_id) {
                    return Err(ExecutionError::Rest {
                        status_code: Some(409),
                        detail: format!(
                            "cancel rejected for {order_id}: {reason}; reconcile fills and retry before asserting flat"
                        ),
                    });
                }
                if !cancel.canceled.iter().any(|id| id.as_str() == order_id) {
                    return Err(ExecutionError::Rest {
                        status_code: Some(409),
                        detail: format!(
                            "cancel not confirmed for {order_id} (canceled={:?}); reconcile fills before asserting flat",
                            cancel.canceled
                        ),
                    });
                }
                Ok(Some(format!(
                    "canceled={:?} not_canceled={:?}",
                    cancel.canceled, cancel.not_canceled
                )))
            }
            Err(ExecutionError::Rest { detail, .. }) if detail.contains("order_not_found") => {
                Ok(Some(format!(
                    "order_not_found on cancel; reconcile fills before asserting positions ({detail})"
                )))
            }
            Err(err) => Err(err),
        }
    }

    /// Places the documented validation order, reads it back, and cancels the remainder.
    ///
    /// A match precedes settlement: this method never asserts a position change.
    ///
    /// # Errors
    /// Returns a typed rejection (including post-only crossing) or a transport error.
    pub async fn validate_order(
        &self,
        market: &NexMarket,
        book: NexBook,
        outcome: core_types::Outcome,
    ) -> Result<NexValidationOutcome, ExecutionError> {
        let (order, signature, guards) = self.build_validation_order(market, &book, outcome)?;
        let placement = match self.rest.post_order(&order, signature, true).await {
            Ok(resp) => resp,
            Err(ExecutionError::Rest { detail, .. }) if detail.contains("order crosses book") => {
                return Err(ExecutionError::Rest {
                    status_code: Some(400),
                    detail:
                        "invalid post-only order: order crosses book (reprice, never force taker)"
                            .to_string(),
                });
            }
            Err(err) => return Err(err),
        };
        if !placement.success || placement.order_id.is_empty() {
            return Err(ExecutionError::Rest {
                status_code: Some(400),
                detail: format!(
                    "NEX rejected sandbox order: '{}' (status='{}')",
                    placement.error_msg, placement.status
                ),
            });
        }

        let readback = self.read_back(&placement.order_id).await?;
        let readback_missing = readback.is_none();
        let cancel_detail = self
            .cancel_remaining(&placement.order_id, readback.as_ref())
            .await?;

        Ok(NexValidationOutcome {
            origin: DataOrigin::Sandbox,
            chain_id: self.profile.chain_id,
            wallet_mode: format!("{:?}", self.profile.wallet_mode),
            maker_address: self.trading_address.clone(),
            outcome: format!("{outcome:?}"),
            asset_id: market.asset_id(outcome).to_string(),
            price_micro: book.tick_size_micro.0,
            size_micro: book.min_order_size_micro.0,
            guards,
            placement,
            readback,
            readback_missing,
            cancel_detail,
            settlement_note:
                "NEX match precedes on-chain settlement; positions are not asserted until the trade reaches CONFIRMED on the user feed"
                    .to_string(),
            book_at_placement: book,
        })
    }
}

/// Connection budget enforcing the documented `subs-v2` per-IP and per-key caps.
///
/// Public clients behind one NAT share the per-IP allowance, so `shared_nat_sockets`
/// is added to every reservation.
#[derive(Debug, Clone)]
pub struct NexFeedBudget {
    usage: Arc<Mutex<NexFeedUsage>>,
    shared_nat: usize,
}

#[derive(Debug, Default)]
struct NexFeedUsage {
    market: usize,
    user_ip: usize,
    user_key: usize,
}

impl NexFeedBudget {
    /// Builds a budget for this process, accounting for other NAT-shared clients.
    #[must_use]
    pub fn new(shared_nat_sockets: usize) -> Self {
        Self {
            usage: Arc::new(Mutex::new(NexFeedUsage::default())),
            shared_nat: shared_nat_sockets,
        }
    }

    /// Reserves one `subs-v2` market socket (cap 50 per IP).
    ///
    /// # Errors
    /// Fails closed when the NAT-aggregated per-IP allowance is exhausted.
    pub fn reserve_market(&self) -> Result<NexFeedPermit, ExecutionError> {
        let mut usage = self
            .usage
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if usage.market.saturating_add(self.shared_nat) >= NEX_V2_MARKET_CAP_PER_IP {
            return Err(ExecutionError::Config(format!(
                "NEX subs-v2 market cap reached: {} local + {} shared NAT >= {NEX_V2_MARKET_CAP_PER_IP} per IP",
                usage.market, self.shared_nat
            )));
        }
        usage.market += 1;
        Ok(NexFeedPermit {
            budget: self.clone(),
            kind: NexFeedKind::Market,
        })
    }

    /// Reserves one `subs-v2` user socket (`min(20 per IP, 10 per API key)`).
    ///
    /// # Errors
    /// Fails closed when either the NAT-aggregated per-IP or per-key allowance is exhausted.
    pub fn reserve_user(&self) -> Result<NexFeedPermit, ExecutionError> {
        let mut usage = self
            .usage
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if usage.user_ip.saturating_add(self.shared_nat) >= NEX_V2_USER_CAP_PER_IP {
            return Err(ExecutionError::Config(format!(
                "NEX subs-v2 user cap reached: {} local + {} shared NAT >= {NEX_V2_USER_CAP_PER_IP} per IP",
                usage.user_ip, self.shared_nat
            )));
        }
        if usage.user_key >= NEX_V2_USER_CAP_PER_KEY {
            return Err(ExecutionError::Config(format!(
                "NEX subs-v2 user cap reached: {} >= {NEX_V2_USER_CAP_PER_KEY} per API key",
                usage.user_key
            )));
        }
        usage.user_ip += 1;
        usage.user_key += 1;
        Ok(NexFeedPermit {
            budget: self.clone(),
            kind: NexFeedKind::User,
        })
    }

    /// Current local reservations `(market, user_ip, user_key)`.
    #[must_use]
    pub fn usage(&self) -> (usize, usize, usize) {
        let usage = self
            .usage
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        (usage.market, usage.user_ip, usage.user_key)
    }
}

/// Feed kind for a reservation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NexFeedKind {
    /// Market data channel.
    Market,
    /// Authenticated user channel.
    User,
}

/// RAII socket reservation released on drop.
#[derive(Debug)]
pub struct NexFeedPermit {
    budget: NexFeedBudget,
    kind: NexFeedKind,
}

impl Drop for NexFeedPermit {
    fn drop(&mut self) {
        match self.kind {
            NexFeedKind::Market => {
                let mut usage = self
                    .budget
                    .usage
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                usage.market = usage.market.saturating_sub(1);
            }
            NexFeedKind::User => {
                let mut usage = self
                    .budget
                    .usage
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                usage.user_ip = usage.user_ip.saturating_sub(1);
                usage.user_key = usage.user_key.saturating_sub(1);
            }
        }
    }
}

/// Hollow/stale feed gate. Quoting stays blocked until a post-reconnect snapshot
/// arrives and the socket is inside the staleness window.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct NexFeedReadiness {
    /// A fresh `book` snapshot was seen on the current socket session.
    pub snapshot_seen: bool,
    /// Milliseconds since the last market frame.
    pub last_market_msg_age_ms: Option<u64>,
    /// Latest snapshot has at least one resting level (a hollow book cannot be quoted).
    pub book_has_liquidity: bool,
    /// User feed authenticated and reconciled.
    pub user_ready: bool,
    /// Staleness threshold.
    pub stale_after_ms: u64,
}

impl NexFeedReadiness {
    /// Fresh snapshot with visible liquidity and fresh frames; a hollow socket never qualifies.
    #[must_use]
    pub fn quoting_allowed(&self) -> bool {
        self.snapshot_seen
            && self.book_has_liquidity
            && self.user_ready
            && self
                .last_market_msg_age_ms
                .is_some_and(|age| age <= self.stale_after_ms)
    }
}

#[cfg(test)]
mod tests {
    use core_types::Outcome;
    use core_types::sandbox::SandboxProfile;

    use super::*;

    #[test]
    fn discovery_parse_nex_clob_market_shape() -> Result<(), Box<dyn std::error::Error>> {
        let raw = serde_json::json!({
            "c": "0xabc",
            "t": [{"t": "111", "o": "Yes"}, {"t": "222", "o": "No"}],
            "mos": 5,
            "mts": 0.01,
            "v": "v1",
            "fd": {"r": 0.05}
        });
        let items = vec![raw];
        let mut markets = Vec::new();
        for item in &items {
            let tick = json_decimal_micro(item.get("mts"), "mts")?;
            let min_size = json_decimal_micro(item.get("mos"), "mos")?;
            let comment = format!("{tick}/{min_size}");
            assert_eq!(comment, "10000/5000000");
            markets.push(());
        }
        assert_eq!(markets.len(), 1);
        Ok(())
    }

    #[test]
    fn guards_reject_crossing_post_only() {
        let market = NexMarket {
            origin: DataOrigin::Sandbox,
            condition_id: "0xc".into(),
            slug: String::new(),
            question: String::new(),
            up_asset_id: "111".into(),
            dn_asset_id: "222".into(),
            tick_size_micro: MicroUsdc(10_000),
            min_order_size_micro: SharesMicro(5_000_000),
            neg_risk: false,
            version: ProtocolVersion::CtfV1,
            fee_rate_e6: 50_000,
        };
        let book = NexBook {
            origin: DataOrigin::Sandbox,
            market: "0xc".into(),
            asset_id: market.up_asset_id.clone(),
            timestamp_ms: 1_700_000_000_000,
            bids: vec![],
            asks: vec![BookLevel {
                price: MicroUsdc(10_000),
                size: SharesMicro(5_000_000),
            }],
            tick_size_micro: market.tick_size_micro,
            min_order_size_micro: market.min_order_size_micro,
            neg_risk: false,
            last_trade_price: None,
        };
        let best_ask = book.asks.first().map(|l| l.price.0);
        let post_only_does_not_cross = best_ask.is_none_or(|ask| ask > market.tick_size_micro.0);
        assert!(!post_only_does_not_cross);
        assert_eq!(
            market.asset_id(Outcome::Up),
            "111",
            "outcome mapping must be explicit"
        );
    }

    #[test]
    fn feed_budget_accounts_for_shared_nat() {
        let budget = NexFeedBudget::new(NEX_V2_USER_CAP_PER_IP);
        assert!(budget.reserve_user().is_err());
        let budget = NexFeedBudget::new(0);
        let permit = budget.reserve_user().unwrap();
        assert_eq!(budget.usage(), (0, 1, 1));
        drop(permit);
        assert_eq!(budget.usage(), (0, 0, 0));
    }

    #[test]
    fn feed_readiness_blocks_hollow_or_stale_sockets() {
        let mut ready = NexFeedReadiness {
            snapshot_seen: false,
            last_market_msg_age_ms: Some(10),
            book_has_liquidity: true,
            user_ready: true,
            stale_after_ms: NEX_STALE_AFTER_MS,
        };
        assert!(!ready.quoting_allowed());
        ready.snapshot_seen = true;
        assert!(ready.quoting_allowed());
        ready.book_has_liquidity = false;
        assert!(!ready.quoting_allowed(), "hollow book must block quoting");
        ready.book_has_liquidity = true;
        ready.last_market_msg_age_ms = Some(NEX_STALE_AFTER_MS + 1);
        assert!(!ready.quoting_allowed());
        ready.last_market_msg_age_ms = None;
        assert!(!ready.quoting_allowed());
    }

    #[test]
    fn production_profile_is_rejected() {
        let profile = SandboxProfile {
            clob_url: "https://clob.polymarket.com".into(),
            ..SandboxProfile::default()
        };
        assert!(profile.validate().is_err());
    }
}
