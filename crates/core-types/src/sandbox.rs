//! NEX sandbox (preprod, Polygon Amoy `80002`) profile and origin tagging.
//!
//! Ground truth: `AI Trading Library/03-exchanges-and-protocols/polymarket/nex/`.
//! The sandbox is a drop-in CLOB replacement: routes and payloads are unchanged,
//! only the base URLs, Amoy chain id, and sandbox credentials differ. Production
//! hosts, keys, and addresses are rejected here by construction.

use std::fmt;
use std::net::IpAddr;

use secrecy::SecretString;
use serde::{Deserialize, Serialize};

use crate::error::{ConfigError, redact_presence};
use crate::market::WalletMode;

/// NEX sandbox trading API hostname.
pub const NEX_CLOB_HOST: &str = "nex.preprod.defi.euw2.polymarket.sh";
/// NEX sandbox WebSocket hostname.
pub const NEX_WS_HOST: &str = "nex-ws.preprod.defi.euw2.polymarket.sh";
/// NEX sandbox trading API base URL.
pub const NEX_CLOB_URL: &str = "https://nex.preprod.defi.euw2.polymarket.sh";
/// NEX sandbox WebSocket base URL.
pub const NEX_WS_URL: &str = "wss://nex-ws.preprod.defi.euw2.polymarket.sh";
/// Gamma preprod (sandbox market metadata) base URL.
pub const NEX_GAMMA_URL: &str = "https://gamma-api-preprod.polymarket.dev";
/// Relayer preprod base URL.
pub const NEX_RELAYER_URL: &str = "https://relayer-preprod.polymarket.dev";
/// Polygon Amoy chain id.
pub const NEX_CHAIN_ID: u64 = 80_002;
/// Published public ingress IPv4 for the allowlisted route.
pub const NEX_INGRESS_IP: &str = "51.24.102.79";
/// Amoy `ExchangeV3` contract published in the NEX onboarding example.
pub const NEX_AMOY_EXCHANGE: &str = "0xe3333700cA9d93003F00f0F71f8515005F6c00Aa";
/// Amoy position manager published in the NEX onboarding example.
pub const NEX_AMOY_POSITION_MANAGER: &str = "0x006F54F7f9A22e0000CC2AB60031000000ae9fEF";
/// Original market channel path (no application connection limit).
pub const NEX_MARKET_WS_PATH: &str = "/ws/market";
/// Original authenticated user channel path (no application connection limit).
pub const NEX_USER_WS_PATH: &str = "/ws/user";
/// Versioned `subs-v2` market path (50 sockets per IP).
pub const NEX_V2_MARKET_WS_PATH: &str = "/beta/ws/market";
/// Versioned `subs-v2` user path (20 per IP and 10 per API key; both apply).
pub const NEX_V2_USER_WS_PATH: &str = "/beta/ws/user";
/// `subs-v2` market connection cap per public IP (NAT-aggregated).
pub const NEX_V2_MARKET_CAP_PER_IP: usize = 50;
/// `subs-v2` user connection cap per public IP (NAT-aggregated).
pub const NEX_V2_USER_CAP_PER_IP: usize = 20;
/// `subs-v2` user connection cap per API key.
pub const NEX_V2_USER_CAP_PER_KEY: usize = 10;

/// Data provenance tag. Every sandbox fill, balance, and calibration input carries this so
/// Amoy test prints never feed production volatility, arrival-rate, or PnL estimates.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DataOrigin {
    /// Production Polymarket / Binance data.
    #[default]
    Production,
    /// NEX sandbox (Amoy test pUSD, isolated books).
    Sandbox,
    /// Locally simulated paper data.
    Paper,
}

impl DataOrigin {
    /// Stable lowercase tag written to every JSONL record.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Production => "production",
            Self::Sandbox => "sandbox",
            Self::Paper => "paper",
        }
    }
}

impl fmt::Display for DataOrigin {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// WebSocket wire path family.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum NexWsVersion {
    /// Original `/ws/market` and `/ws/user` paths.
    Original,
    /// Versioned `/beta/ws/market` and `/beta/ws/user` paths with documented caps.
    SubsV2,
}

impl NexWsVersion {
    /// Market channel path for this family.
    #[must_use]
    pub const fn market_path(self) -> &'static str {
        match self {
            Self::Original => NEX_MARKET_WS_PATH,
            Self::SubsV2 => NEX_V2_MARKET_WS_PATH,
        }
    }

    /// User channel path for this family.
    #[must_use]
    pub const fn user_path(self) -> &'static str {
        match self {
            Self::Original => NEX_USER_WS_PATH,
            Self::SubsV2 => NEX_V2_USER_WS_PATH,
        }
    }
}

/// Production exchange contracts that must never appear in a sandbox signing domain.
const PRODUCTION_EXCHANGES: [&str; 3] = [
    "0xE111180000d2663C0091e4f400237545B87B996B",
    "0xe2222d279d744050d28e00520010520000310F59",
    "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E",
];

/// Validated NEX sandbox profile loaded strictly from the operator environment.
///
/// Production base URLs, chain `137`, and the Safe/Proxy wallet modes are rejected.
/// `Debug` and serialization never expose secret material.
#[derive(Clone)]
pub struct SandboxProfile {
    /// Trading API base URL (no trailing slash).
    pub clob_url: String,
    /// WebSocket base URL (`wss://`).
    pub ws_url: String,
    /// Gamma preprod base URL (requires edge credentials).
    pub gamma_url: String,
    /// Relayer preprod base URL.
    pub relayer_url: String,
    /// Must be [`NEX_CHAIN_ID`].
    pub chain_id: u64,
    /// Amoy `ExchangeV3` verifying contract.
    pub exchange_address: String,
    /// Amoy position manager.
    pub position_manager: String,
    /// Public ingress IPv4 that both NEX hostnames are pinned to.
    pub ingress_ip: IpAddr,
    /// Operator's registered allowlisted egress IPv4 (`/32`).
    pub allowlisted_egress_ip: Option<IpAddr>,
    /// Amoy JSON-RPC URL used for approvals / balance reads.
    pub amoy_rpc_url: Option<String>,
    /// Public Amoy trading address (EOA or Deposit Wallet).
    pub trading_address: Option<String>,
    /// Wallet signature mode: only `Eoa` or `DepositWallet1271` are legal.
    pub wallet_mode: WalletMode,
    /// Sockets already used by other clients sharing this NAT (counts against per-IP caps).
    pub shared_nat_sockets: usize,
    /// Sandbox L2 API key (never the production CLOB key).
    pub api_key: Option<SecretString>,
    /// Sandbox L2 HMAC secret.
    pub api_secret: Option<SecretString>,
    /// Sandbox L2 passphrase.
    pub api_passphrase: Option<SecretString>,
    /// Amoy signing EOA private key (`POLYMARKET_PRIVATE_KEY`).
    pub private_key: Option<SecretString>,
    /// Gamma preprod `x-client-id`.
    pub gamma_client_id: Option<SecretString>,
    /// Gamma preprod `x-client-secret`.
    pub gamma_client_secret: Option<SecretString>,
}

impl Default for SandboxProfile {
    fn default() -> Self {
        Self {
            clob_url: NEX_CLOB_URL.to_string(),
            ws_url: NEX_WS_URL.to_string(),
            gamma_url: NEX_GAMMA_URL.to_string(),
            relayer_url: NEX_RELAYER_URL.to_string(),
            chain_id: NEX_CHAIN_ID,
            exchange_address: NEX_AMOY_EXCHANGE.to_string(),
            position_manager: NEX_AMOY_POSITION_MANAGER.to_string(),
            ingress_ip: parse_ip(NEX_INGRESS_IP).unwrap_or(IpAddr::from([51, 24, 102, 79])),
            allowlisted_egress_ip: None,
            amoy_rpc_url: None,
            trading_address: None,
            wallet_mode: WalletMode::Eoa,
            shared_nat_sockets: 0,
            api_key: None,
            api_secret: None,
            api_passphrase: None,
            private_key: None,
            gamma_client_id: None,
            gamma_client_secret: None,
        }
    }
}

impl fmt::Debug for SandboxProfile {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SandboxProfile")
            .field("clob_url", &self.clob_url)
            .field("ws_url", &self.ws_url)
            .field("gamma_url", &self.gamma_url)
            .field("relayer_url", &self.relayer_url)
            .field("chain_id", &self.chain_id)
            .field("exchange_address", &self.exchange_address)
            .field("position_manager", &self.position_manager)
            .field("ingress_ip", &self.ingress_ip)
            .field("allowlisted_egress_ip", &self.allowlisted_egress_ip)
            .field(
                "amoy_rpc_url",
                &redact_presence(self.amoy_rpc_url.is_some()),
            )
            .field("trading_address", &self.trading_address)
            .field("wallet_mode", &self.wallet_mode)
            .field("shared_nat_sockets", &self.shared_nat_sockets)
            .field("api_key", &redact_presence(self.api_key.is_some()))
            .field("api_secret", &redact_presence(self.api_secret.is_some()))
            .field(
                "api_passphrase",
                &redact_presence(self.api_passphrase.is_some()),
            )
            .field("private_key", &redact_presence(self.private_key.is_some()))
            .field(
                "gamma_client_id",
                &redact_presence(self.gamma_client_id.is_some()),
            )
            .field(
                "gamma_client_secret",
                &redact_presence(self.gamma_client_secret.is_some()),
            )
            .finish()
    }
}

impl SandboxProfile {
    /// Loads `.env` if present, then reads the sandbox profile from process environment.
    pub fn from_env() -> Result<Self, ConfigError> {
        match dotenvy::dotenv() {
            Ok(_) => {}
            Err(err) if err.not_found() => {}
            Err(err) => return Err(ConfigError::from_dotenvy(&err)),
        }
        Self::from_lookup(|key| match std::env::var(key) {
            Ok(val) => Ok(Some(val)),
            Err(std::env::VarError::NotPresent) => Ok(None),
            Err(std::env::VarError::NotUnicode(_)) => Err(ConfigError::InvalidValue {
                key,
                value: "<non-utf8>".to_string(),
                reason: "environment variable contains invalid UTF-8 bytes".to_string(),
            }),
        })
    }

    /// Parses and validates a profile from an injected key lookup (no global env mutation).
    pub fn from_lookup<F>(lookup: F) -> Result<Self, ConfigError>
    where
        F: Fn(&'static str) -> Result<Option<String>, ConfigError>,
    {
        let mut profile = Self::default();
        if let Some(v) = non_empty(&lookup, "NEX_CLOB_URL")? {
            profile.clob_url = trim_end_slash(v);
        }
        if let Some(v) = non_empty(&lookup, "NEX_WS_URL")? {
            profile.ws_url = trim_end_slash(v);
        }
        if let Some(v) = non_empty(&lookup, "GAMMA_PREPROD_URL")? {
            profile.gamma_url = trim_end_slash(v);
        }
        if let Some(v) = non_empty(&lookup, "NEX_RELAYER_URL")? {
            profile.relayer_url = trim_end_slash(v);
        }
        if let Some(v) = non_empty(&lookup, "NEX_CHAIN_ID")? {
            profile.chain_id = v.parse::<u64>().map_err(|_| {
                invalid(
                    "NEX_CHAIN_ID",
                    v.clone(),
                    "expected integer chain id (80002)",
                )
            })?;
        }
        if let Some(v) = non_empty(&lookup, "NEX_EXCHANGE_ADDRESS")? {
            profile.exchange_address = v;
        }
        if let Some(v) = non_empty(&lookup, "NEX_POSITION_MANAGER")? {
            profile.position_manager = v;
        }
        if let Some(v) = non_empty(&lookup, "NEX_INGRESS_IP")? {
            profile.ingress_ip = parse_ip(&v).ok_or_else(|| {
                invalid("NEX_INGRESS_IP", v.clone(), "expected IPv4 ingress address")
            })?;
        }
        if let Some(v) = non_empty(&lookup, "NEX_ALLOWLISTED_EGRESS_IP")? {
            profile.allowlisted_egress_ip = Some(parse_ip(&v).ok_or_else(|| {
                invalid(
                    "NEX_ALLOWLISTED_EGRESS_IP",
                    v.clone(),
                    "expected allowlisted source IPv4 (/32)",
                )
            })?);
        }
        if let Some(v) = non_empty(&lookup, "NEX_SHARED_NAT_SOCKETS")? {
            profile.shared_nat_sockets = v.parse::<usize>().map_err(|_| {
                invalid(
                    "NEX_SHARED_NAT_SOCKETS",
                    v.clone(),
                    "expected non-negative integer socket count",
                )
            })?;
        }
        profile.amoy_rpc_url = non_empty(&lookup, "AMOY_RPC_URL")?;
        profile.trading_address = non_empty(&lookup, "NEX_TRADING_ADDRESS")?;
        profile.wallet_mode = match non_empty(&lookup, "NEX_WALLET_MODE")?
            .map(|v| v.to_ascii_lowercase())
            .as_deref()
        {
            None | Some("eoa") => WalletMode::Eoa,
            Some("deposit-wallet" | "deposit_wallet" | "deposit" | "1271") => {
                WalletMode::DepositWallet1271
            }
            Some("proxy" | "safe" | "polymarket-proxy" | "gnosis-safe") => {
                return Err(invalid(
                    "NEX_WALLET_MODE",
                    "proxy/safe".to_string(),
                    "NEX sandbox supports only EOA or Deposit Wallet; Safe/Proxy are unsupported",
                ));
            }
            Some(other) => {
                return Err(invalid(
                    "NEX_WALLET_MODE",
                    other.to_string(),
                    "expected eoa or deposit-wallet",
                ));
            }
        };
        profile.api_key = non_empty(&lookup, "NEX_API_KEY")?.map(SecretString::from);
        profile.api_secret = non_empty(&lookup, "NEX_API_SECRET")?.map(SecretString::from);
        profile.api_passphrase = non_empty(&lookup, "NEX_API_PASSPHRASE")?.map(SecretString::from);
        profile.private_key = non_empty(&lookup, "POLYMARKET_PRIVATE_KEY")?.map(SecretString::from);
        profile.gamma_client_id = non_empty(&lookup, "GAMMA_CLIENT_ID")?.map(SecretString::from);
        profile.gamma_client_secret =
            non_empty(&lookup, "GAMMA_CLIENT_SECRET")?.map(SecretString::from);
        profile.validate()?;
        Ok(profile)
    }

    /// Validates that this profile can only ever reach sandbox hosts on Amoy.
    pub fn validate(&self) -> Result<(), ConfigError> {
        validate_endpoint(
            "NEX_CLOB_URL",
            &self.clob_url,
            "https",
            "nex.preprod.defi.euw2.polymarket.sh",
        )?;
        validate_endpoint(
            "NEX_WS_URL",
            &self.ws_url,
            "wss",
            "nex-ws.preprod.defi.euw2.polymarket.sh",
        )?;
        validate_endpoint(
            "GAMMA_PREPROD_URL",
            &self.gamma_url,
            "https",
            "gamma-api-preprod.polymarket.dev",
        )?;
        validate_endpoint(
            "NEX_RELAYER_URL",
            &self.relayer_url,
            "https",
            "relayer-preprod.polymarket.dev",
        )?;
        if self.chain_id != NEX_CHAIN_ID {
            return Err(invariant(format!(
                "NEX sandbox requires chain id {NEX_CHAIN_ID} (Polygon Amoy), got {}",
                self.chain_id
            )));
        }
        if self.wallet_mode != WalletMode::Eoa && self.wallet_mode != WalletMode::DepositWallet1271
        {
            return Err(invariant(
                "NEX sandbox supports only EOA or Deposit Wallet; Safe/Proxy are unsupported",
            ));
        }
        if let Some(addr) = &self.trading_address {
            validate_hex_address("NEX_TRADING_ADDRESS", addr)?;
        }
        validate_hex_address("NEX_EXCHANGE_ADDRESS", &self.exchange_address)?;
        validate_hex_address("NEX_POSITION_MANAGER", &self.position_manager)?;
        if PRODUCTION_EXCHANGES
            .iter()
            .any(|prod| self.exchange_address.eq_ignore_ascii_case(prod))
        {
            return Err(invariant(
                "NEX_EXCHANGE_ADDRESS points at a production Polygon exchange; sandbox must use the Amoy ExchangeV3",
            ));
        }
        if let Some(rpc) = &self.amoy_rpc_url
            && !rpc.starts_with("https://")
        {
            return Err(invariant(
                "AMOY_RPC_URL must use https:// (no plaintext RPC)",
            ));
        }
        if self.api_key.is_some() != self.api_secret.is_some()
            || self.api_key.is_some() != self.api_passphrase.is_some()
        {
            return Err(invariant(
                "NEX_API_KEY, NEX_API_SECRET, and NEX_API_PASSPHRASE must be set together",
            ));
        }
        if self.gamma_client_id.is_some() != self.gamma_client_secret.is_some() {
            return Err(invariant(
                "GAMMA_CLIENT_ID and GAMMA_CLIENT_SECRET must be set together",
            ));
        }
        Ok(())
    }

    /// `true` when sandbox L2 trading credentials are present.
    #[must_use]
    pub const fn has_trading_api_credentials(&self) -> bool {
        self.api_key.is_some() && self.api_secret.is_some() && self.api_passphrase.is_some()
    }

    /// `true` when the Amoy signing key is present.
    #[must_use]
    pub const fn has_signer_key(&self) -> bool {
        self.private_key.is_some()
    }

    /// `true` when Gamma preprod edge credentials are present.
    #[must_use]
    pub const fn has_gamma_edge_credentials(&self) -> bool {
        self.gamma_client_id.is_some() && self.gamma_client_secret.is_some()
    }

    /// Full market WebSocket URL for a path family.
    #[must_use]
    pub fn market_ws_url(&self, version: NexWsVersion) -> String {
        format!("{}{}", self.ws_url, version.market_path())
    }

    /// Full user WebSocket URL for a path family.
    #[must_use]
    pub fn user_ws_url(&self, version: NexWsVersion) -> String {
        format!("{}{}", self.ws_url, version.user_path())
    }

    /// Endpoint host name for the trading API.
    #[must_use]
    pub fn clob_host(&self) -> &str {
        host_of(&self.clob_url).unwrap_or("nex.preprod.defi.euw2.polymarket.sh")
    }

    /// Endpoint host name for WebSockets.
    #[must_use]
    pub fn ws_host(&self) -> &str {
        host_of(&self.ws_url).unwrap_or("nex-ws.preprod.defi.euw2.polymarket.sh")
    }
}

fn validate_endpoint(
    key: &'static str,
    url: &str,
    scheme: &str,
    expected_host: &str,
) -> Result<(), ConfigError> {
    let prefix = format!("{scheme}://");
    let Some(rest) = url.strip_prefix(&prefix) else {
        return Err(invalid(
            key,
            url.to_string(),
            &format!("expected {scheme}:// URL"),
        ));
    };
    let host_port = rest.split(['/', '?', '#']).next().unwrap_or("");
    let host = host_port.split(':').next().unwrap_or("");
    if !host.eq_ignore_ascii_case(expected_host) {
        return Err(invalid(
            key,
            url.to_string(),
            &format!("sandbox path requires host {expected_host} (production hosts are rejected)"),
        ));
    }
    if let Some(port) = host_port.split_once(':').map(|(_, p)| p)
        && !port.is_empty()
        && !port.bytes().all(|b| b.is_ascii_digit())
    {
        return Err(invalid(key, url.to_string(), "malformed port"));
    }
    Ok(())
}

fn validate_hex_address(key: &'static str, addr: &str) -> Result<(), ConfigError> {
    let hex = addr
        .strip_prefix("0x")
        .or_else(|| addr.strip_prefix("0X"))
        .ok_or_else(|| invalid(key, addr.to_string(), "address must start with 0x"))?;
    if hex.len() != 40 || !hex.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(invalid(
            key,
            addr.to_string(),
            "address must be 40 hex characters",
        ));
    }
    if hex.bytes().all(|b| b == b'0') {
        return Err(invalid(
            key,
            addr.to_string(),
            "zero address is not allowed",
        ));
    }
    Ok(())
}

fn host_of(url: &str) -> Option<&str> {
    let (_, rest) = url.split_once("://")?;
    let host_port = rest.split(['/', '?', '#']).next()?;
    Some(host_port.split(':').next().unwrap_or(host_port))
}

fn trim_end_slash(mut s: String) -> String {
    while s.ends_with('/') {
        s.pop();
    }
    s
}

fn parse_ip(raw: &str) -> Option<IpAddr> {
    raw.trim().parse::<IpAddr>().ok()
}

fn non_empty<F>(lookup: &F, key: &'static str) -> Result<Option<String>, ConfigError>
where
    F: Fn(&'static str) -> Result<Option<String>, ConfigError>,
{
    match lookup(key)? {
        Some(raw) => {
            let trimmed = raw.trim();
            if trimmed.is_empty() {
                Ok(None)
            } else {
                Ok(Some(trimmed.to_string()))
            }
        }
        None => Ok(None),
    }
}

fn invalid(key: &'static str, value: String, reason: &str) -> ConfigError {
    ConfigError::InvalidValue {
        key,
        value,
        reason: reason.to_string(),
    }
}

fn invariant(detail: impl Into<String>) -> ConfigError {
    ConfigError::InvalidInvariant {
        detail: detail.into(),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    fn lookup_map<'a>(
        map: &'a HashMap<&'static str, &'static str>,
    ) -> impl Fn(&'static str) -> Result<Option<String>, ConfigError> + 'a {
        move |k| Ok(map.get(k).map(|s| (*s).to_string()))
    }

    #[test]
    fn default_profile_is_amoy_sandbox_only() {
        let p = SandboxProfile::from_lookup(|_| Ok(None)).unwrap();
        assert_eq!(p.chain_id, NEX_CHAIN_ID);
        assert_eq!(p.clob_url, NEX_CLOB_URL);
        assert_eq!(p.ws_url, NEX_WS_URL);
        assert_eq!(
            p.market_ws_url(NexWsVersion::Original),
            format!("{NEX_WS_URL}/ws/market")
        );
        assert_eq!(
            p.market_ws_url(NexWsVersion::SubsV2),
            format!("{NEX_WS_URL}/beta/ws/market")
        );
        assert!(!p.has_trading_api_credentials());
        assert!(!p.has_signer_key());
    }

    #[test]
    fn production_hosts_and_chain_are_rejected() {
        let mut map = HashMap::new();
        map.insert("NEX_CLOB_URL", "https://clob.polymarket.com");
        assert!(SandboxProfile::from_lookup(lookup_map(&map)).is_err());

        map.insert("NEX_CLOB_URL", NEX_CLOB_URL);
        map.insert("NEX_CHAIN_ID", "137");
        assert!(SandboxProfile::from_lookup(lookup_map(&map)).is_err());

        map.insert("NEX_CHAIN_ID", "80002");
        map.insert(
            "NEX_WS_URL",
            "wss://ws-subscriptions-clob.polymarket.com/ws/market",
        );
        assert!(SandboxProfile::from_lookup(lookup_map(&map)).is_err());
    }

    #[test]
    fn production_exchange_address_is_rejected() {
        let mut map = HashMap::new();
        map.insert(
            "NEX_EXCHANGE_ADDRESS",
            "0xE111180000d2663C0091e4f400237545B87B996B",
        );
        assert!(SandboxProfile::from_lookup(lookup_map(&map)).is_err());
    }

    #[test]
    fn safe_and_proxy_wallet_modes_are_rejected() {
        let mut map = HashMap::new();
        map.insert("NEX_WALLET_MODE", "safe");
        assert!(SandboxProfile::from_lookup(lookup_map(&map)).is_err());
        map.insert("NEX_WALLET_MODE", "deposit-wallet");
        let p = SandboxProfile::from_lookup(lookup_map(&map)).unwrap();
        assert_eq!(p.wallet_mode, WalletMode::DepositWallet1271);
    }

    #[test]
    fn secrets_are_redacted_in_debug() {
        let mut map = HashMap::new();
        map.insert("POLYMARKET_PRIVATE_KEY", "SENTINEL_PRIVATE_KEY");
        map.insert("NEX_API_KEY", "SENTINEL_API_KEY");
        map.insert("NEX_API_SECRET", "SENTINEL_API_SECRET");
        map.insert("NEX_API_PASSPHRASE", "SENTINEL_PASS");
        map.insert("GAMMA_CLIENT_ID", "SENTINEL_GAMMA_ID");
        map.insert("GAMMA_CLIENT_SECRET", "SENTINEL_GAMMA_SECRET");
        let p = SandboxProfile::from_lookup(lookup_map(&map)).unwrap();
        let dbg = format!("{p:?}");
        for sentinel in [
            "SENTINEL_PRIVATE_KEY",
            "SENTINEL_API_KEY",
            "SENTINEL_API_SECRET",
            "SENTINEL_PASS",
            "SENTINEL_GAMMA_ID",
            "SENTINEL_GAMMA_SECRET",
        ] {
            assert!(!dbg.contains(sentinel), "Debug leaked {sentinel}");
        }
        assert!(dbg.contains("[REDACTED]"));
    }

    #[test]
    fn origin_tags_are_lowercase() {
        assert_eq!(DataOrigin::Sandbox.as_str(), "sandbox");
        assert_eq!(DataOrigin::Production.as_str(), "production");
        assert_eq!(DataOrigin::Paper.as_str(), "paper");
    }
}
