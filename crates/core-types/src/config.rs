use std::fmt;

use secrecy::SecretString;
use serde::{Deserialize, Serialize};

use crate::error::{ConfigError, redact_presence};
use crate::market::{ExecutionMode, KappaMode, SgMode, VarianceMode, WalletMode};
use crate::money::{
    MicroUsdc, ONE_CENT_MICRO, ONE_DOLLAR_MICRO, SIZE_STEP_MICRO, SharesMicro, parse_decimal_micro,
};

const MAX_GAMMA: f64 = 100.0;
const MAX_NOTIONAL_PER_LEG_MICRO: u64 = 1_000_000 * ONE_DOLLAR_MICRO;
const MAX_HYSTERESIS_TICKS: f64 = 10.0;
const MAX_CLOCK_DRIFT_MS: i64 = 60_000;

/// Optional live operator credentials wrapped in `SecretString` and excluded from serialization.
#[derive(Clone, Default)]
pub struct LiveCredentials {
    /// Signing EOA hex private key.
    pub signer_secret_key: Option<SecretString>,
    /// CLOB L2 API key UUID.
    pub clob_api_key: Option<SecretString>,
    /// CLOB L2 base64url HMAC secret.
    pub clob_api_secret: Option<SecretString>,
    /// CLOB L2 API passphrase.
    pub clob_api_passphrase: Option<SecretString>,
    /// Polymarket gasless relayer API key.
    pub relayer_api_key: Option<SecretString>,
    /// Optional institutional Chainlink Data Streams API key.
    pub chainlink_api_key: Option<SecretString>,
}

impl LiveCredentials {
    /// Returns `true` when CLOB L2 API credentials (`key`, `secret`, `passphrase`) are populated.
    #[must_use]
    pub fn has_clob_api_key_set(&self) -> bool {
        self.clob_api_key.is_some()
            && self.clob_api_secret.is_some()
            && self.clob_api_passphrase.is_some()
    }

    /// Returns `true` when all CLOB L2 and Chainlink Data Streams credentials for PolyBolt are populated.
    #[must_use]
    pub fn has_polybolt_credentials(&self) -> bool {
        self.has_clob_api_key_set() && self.chainlink_api_key.is_some()
    }

    /// Returns `true` when all required CLOB L1/L2 trading secrets are populated.
    #[must_use]
    pub fn has_clob_credentials(&self) -> bool {
        self.signer_secret_key.is_some() && self.has_clob_api_key_set()
    }
}

impl fmt::Debug for LiveCredentials {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LiveCredentials")
            .field(
                "signer_secret_key",
                &redact_presence(self.signer_secret_key.is_some()),
            )
            .field(
                "clob_api_key",
                &redact_presence(self.clob_api_key.is_some()),
            )
            .field(
                "clob_api_secret",
                &redact_presence(self.clob_api_secret.is_some()),
            )
            .field(
                "clob_api_passphrase",
                &redact_presence(self.clob_api_passphrase.is_some()),
            )
            .field(
                "relayer_api_key",
                &redact_presence(self.relayer_api_key.is_some()),
            )
            .field(
                "chainlink_api_key",
                &redact_presence(self.chainlink_api_key.is_some()),
            )
            .finish()
    }
}

/// Validated runtime configuration matching locked operator defaults.
#[derive(Clone, Serialize, Deserialize)]
#[serde(try_from = "RawEngineConfig")]
pub struct EngineConfig {
    /// Execution safety mode (`Paper` default).
    pub execution_mode: ExecutionMode,
    /// Explicit dry-run safety latch (`true` default).
    pub dry_run: bool,
    /// Wallet EIP-712 signature mode (`DepositWallet1271` = `3` default, `GnosisSafe` = `2` supported).
    pub wallet_mode: WalletMode,
    /// Public proxy / deposit wallet funder address (`0x...`).
    pub proxy_address: Option<String>,
    /// Public signing EOA address (`0x...`).
    pub signer_address: Option<String>,
    /// Public relayer key owner address (`0x...`).
    pub relayer_address: Option<String>,
    /// Gasless Polymarket Relayer v2 endpoint.
    pub relayer_url: String,
    /// Enable gasless relayer settlement (`true` default).
    pub gasless_relayer_enabled: bool,
    /// Enable optional PolyBolt TWAP socket (`false` default).
    pub polybolt_enabled: bool,
    /// CARA inventory risk aversion parameter `gamma` (`0.15` default).
    pub gamma: f64,
    /// Maximum unhedged dollar notional allowed per leg (`$50.00` default).
    pub max_notional_per_leg_micro: MicroUsdc,
    /// Baseline order size in outcome shares (`20` shares default).
    pub base_order_size_shares: SharesMicro,
    /// Minimum combined spread floor (`$0.02` default, covering `1.25%` max taker drag).
    pub min_combined_spread_micro: MicroUsdc,
    /// Minimum combined spread floor in integer ticks (`2` default).
    pub min_spread_ticks: u32,
    /// Maximum combined dual-bid cost (`$0.98` default).
    pub max_combined_bid_micro: MicroUsdc,
    /// Continuous hysteresis deadband buffer (`0.25` ticks default -> `1.25 * tick`).
    pub hysteresis_tick_buffer: f64,
    /// Remaining variance mode (`BinaryCara` default).
    pub variance_mode: VarianceMode,
    /// Arrival intensity estimator (`TradeNormalizedDepth` default).
    pub kappa_mode: KappaMode,
    /// Savitzky-Golay filter mode (`Causal` default).
    pub sg_mode: SgMode,
    /// Whether Station 4 includes `gamma * V_rem` term (`true` default).
    pub include_variance_spread: bool,
    /// Whether Station 4 enforces taker-fee profitability floor (`true` default).
    pub enforce_fee_floor: bool,
    /// Maximum permitted local-to-exchange clock drift in milliseconds (`1000ms` default).
    pub clock_drift_max_ms: i64,
    /// Non-serializable live credentials (excluded from serde/JSONL).
    #[serde(skip)]
    pub credentials: LiveCredentials,
}

#[derive(Deserialize)]
#[serde(default, deny_unknown_fields)]
struct RawEngineConfig {
    execution_mode: ExecutionMode,
    dry_run: bool,
    wallet_mode: WalletMode,
    proxy_address: Option<String>,
    signer_address: Option<String>,
    relayer_address: Option<String>,
    relayer_url: String,
    gasless_relayer_enabled: bool,
    polybolt_enabled: bool,
    gamma: f64,
    max_notional_per_leg_micro: MicroUsdc,
    base_order_size_shares: SharesMicro,
    min_combined_spread_micro: MicroUsdc,
    min_spread_ticks: u32,
    max_combined_bid_micro: MicroUsdc,
    hysteresis_tick_buffer: f64,
    variance_mode: VarianceMode,
    kappa_mode: KappaMode,
    sg_mode: SgMode,
    include_variance_spread: bool,
    enforce_fee_floor: bool,
    clock_drift_max_ms: i64,
}

impl Default for RawEngineConfig {
    fn default() -> Self {
        let d = EngineConfig::default();
        Self {
            execution_mode: d.execution_mode,
            dry_run: d.dry_run,
            wallet_mode: d.wallet_mode,
            proxy_address: d.proxy_address,
            signer_address: d.signer_address,
            relayer_address: d.relayer_address,
            relayer_url: d.relayer_url,
            gasless_relayer_enabled: d.gasless_relayer_enabled,
            polybolt_enabled: d.polybolt_enabled,
            gamma: d.gamma,
            max_notional_per_leg_micro: d.max_notional_per_leg_micro,
            base_order_size_shares: d.base_order_size_shares,
            min_combined_spread_micro: d.min_combined_spread_micro,
            min_spread_ticks: d.min_spread_ticks,
            max_combined_bid_micro: d.max_combined_bid_micro,
            hysteresis_tick_buffer: d.hysteresis_tick_buffer,
            variance_mode: d.variance_mode,
            kappa_mode: d.kappa_mode,
            sg_mode: d.sg_mode,
            include_variance_spread: d.include_variance_spread,
            enforce_fee_floor: d.enforce_fee_floor,
            clock_drift_max_ms: d.clock_drift_max_ms,
        }
    }
}

impl TryFrom<RawEngineConfig> for EngineConfig {
    type Error = ConfigError;

    fn try_from(raw: RawEngineConfig) -> Result<Self, Self::Error> {
        let cfg = Self {
            execution_mode: raw.execution_mode,
            dry_run: raw.dry_run,
            wallet_mode: raw.wallet_mode,
            proxy_address: raw.proxy_address,
            signer_address: raw.signer_address,
            relayer_address: raw.relayer_address,
            relayer_url: raw.relayer_url,
            gasless_relayer_enabled: raw.gasless_relayer_enabled,
            polybolt_enabled: raw.polybolt_enabled,
            gamma: raw.gamma,
            max_notional_per_leg_micro: raw.max_notional_per_leg_micro,
            base_order_size_shares: raw.base_order_size_shares,
            min_combined_spread_micro: raw.min_combined_spread_micro,
            min_spread_ticks: raw.min_spread_ticks,
            max_combined_bid_micro: raw.max_combined_bid_micro,
            hysteresis_tick_buffer: raw.hysteresis_tick_buffer,
            variance_mode: raw.variance_mode,
            kappa_mode: raw.kappa_mode,
            sg_mode: raw.sg_mode,
            include_variance_spread: raw.include_variance_spread,
            enforce_fee_floor: raw.enforce_fee_floor,
            clock_drift_max_ms: raw.clock_drift_max_ms,
            credentials: LiveCredentials::default(),
        };
        cfg.validate()?;
        Ok(cfg)
    }
}

impl fmt::Debug for EngineConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EngineConfig")
            .field("execution_mode", &self.execution_mode)
            .field("dry_run", &self.dry_run)
            .field("wallet_mode", &self.wallet_mode)
            .field("proxy_address", &self.proxy_address)
            .field("signer_address", &self.signer_address)
            .field("relayer_address", &self.relayer_address)
            .field("relayer_url", &self.relayer_url)
            .field("gasless_relayer_enabled", &self.gasless_relayer_enabled)
            .field("polybolt_enabled", &self.polybolt_enabled)
            .field("gamma", &self.gamma)
            .field(
                "max_notional_per_leg_micro",
                &self.max_notional_per_leg_micro,
            )
            .field("base_order_size_shares", &self.base_order_size_shares)
            .field("min_combined_spread_micro", &self.min_combined_spread_micro)
            .field("min_spread_ticks", &self.min_spread_ticks)
            .field("max_combined_bid_micro", &self.max_combined_bid_micro)
            .field("hysteresis_tick_buffer", &self.hysteresis_tick_buffer)
            .field("variance_mode", &self.variance_mode)
            .field("kappa_mode", &self.kappa_mode)
            .field("sg_mode", &self.sg_mode)
            .field("include_variance_spread", &self.include_variance_spread)
            .field("enforce_fee_floor", &self.enforce_fee_floor)
            .field("clock_drift_max_ms", &self.clock_drift_max_ms)
            .field("credentials", &self.credentials)
            .finish()
    }
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            execution_mode: ExecutionMode::Paper,
            dry_run: true,
            wallet_mode: WalletMode::DepositWallet1271,
            proxy_address: None,
            signer_address: None,
            relayer_address: None,
            relayer_url: "https://relayer-v2.polymarket.com/submit".to_string(),
            gasless_relayer_enabled: true,
            polybolt_enabled: false,
            gamma: 0.15,
            max_notional_per_leg_micro: MicroUsdc(50_000_000),
            base_order_size_shares: SharesMicro(20_000_000),
            min_combined_spread_micro: MicroUsdc(20_000),
            min_spread_ticks: 2,
            max_combined_bid_micro: MicroUsdc(980_000),
            hysteresis_tick_buffer: 0.25,
            variance_mode: VarianceMode::BinaryCara,
            kappa_mode: KappaMode::TradeNormalizedDepth,
            sg_mode: SgMode::Causal,
            include_variance_spread: true,
            enforce_fee_floor: true,
            clock_drift_max_ms: 1_000,
            credentials: LiveCredentials::default(),
        }
    }
}

impl EngineConfig {
    /// Loads `.env` if present (redacting secret lines on parse errors) and validates process env.
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

    /// Parses and validates configuration from an injected key-lookup function (no global env mutation).
    pub fn from_lookup<F>(lookup: F) -> Result<Self, ConfigError>
    where
        F: Fn(&'static str) -> Result<Option<String>, ConfigError>,
    {
        let mut cfg = Self::default();
        parse_mode_fields(&lookup, &mut cfg)?;
        parse_address_and_flag_fields(&lookup, &mut cfg)?;
        parse_math_and_risk_fields(&lookup, &mut cfg)?;
        parse_pipeline_fields(&lookup, &mut cfg)?;
        cfg.credentials = parse_credentials(&lookup)?;
        cfg.validate()?;
        Ok(cfg)
    }

    /// Validates cross-field mode latches, address shapes, credentials, and math/risk bounds.
    pub fn validate(&self) -> Result<(), ConfigError> {
        validate_modes_and_credentials(self)?;
        validate_addresses(self)?;
        validate_math_bounds(self)?;
        Ok(())
    }
}

fn parse_mode_fields<F>(lookup: &F, cfg: &mut EngineConfig) -> Result<(), ConfigError>
where
    F: Fn(&'static str) -> Result<Option<String>, ConfigError>,
{
    if let Some(v) = non_empty(lookup, "EXECUTION_MODE")? {
        cfg.execution_mode = match v.to_ascii_lowercase().as_str() {
            "paper" => ExecutionMode::Paper,
            "dry-run" | "dry_run" => ExecutionMode::DryRun,
            "canary-live" | "canary_live" => ExecutionMode::CanaryLive,
            _ => {
                return Err(invalid_val(
                    "EXECUTION_MODE",
                    v,
                    "expected paper, dry-run, or canary-live",
                ));
            }
        };
    }
    if let Some(v) = non_empty(lookup, "DRY_RUN")? {
        cfg.dry_run = parse_bool("DRY_RUN", &v)?;
    }
    if let Some(v) = non_empty(lookup, "POLY_SIG_TYPE")? {
        cfg.wallet_mode = match v.as_str() {
            "0" => WalletMode::Eoa,
            "1" => WalletMode::PolyProxy,
            "2" => WalletMode::GnosisSafe,
            "3" => WalletMode::DepositWallet1271,
            _ => return Err(invalid_val("POLY_SIG_TYPE", v, "expected 0, 1, 2, or 3")),
        };
    }
    Ok(())
}

fn parse_address_and_flag_fields<F>(lookup: &F, cfg: &mut EngineConfig) -> Result<(), ConfigError>
where
    F: Fn(&'static str) -> Result<Option<String>, ConfigError>,
{
    cfg.proxy_address = non_empty(lookup, "POLYMARKET_PROXY_ADDRESS")?;
    cfg.signer_address = non_empty(lookup, "POLYMARKET_SIGNER_ADDRESS")?;
    cfg.relayer_address = non_empty(lookup, "RELAYER_API_KEY_ADDRESS")?;

    if let Some(v) = non_empty(lookup, "POLYMARKET_RELAYER_URL")? {
        cfg.relayer_url = v;
    }
    if let Some(v) = non_empty(lookup, "GASLESS_RELAYER_ENABLED")? {
        cfg.gasless_relayer_enabled = parse_bool("GASLESS_RELAYER_ENABLED", &v)?;
    }
    if let Some(v) = non_empty(lookup, "POLYBOLT_ENABLED")? {
        cfg.polybolt_enabled = parse_bool("POLYBOLT_ENABLED", &v)?;
    }
    Ok(())
}

fn parse_math_and_risk_fields<F>(lookup: &F, cfg: &mut EngineConfig) -> Result<(), ConfigError>
where
    F: Fn(&'static str) -> Result<Option<String>, ConfigError>,
{
    if let Some(v) = non_empty(lookup, "ENGINE_GAMMA")? {
        cfg.gamma = parse_f64("ENGINE_GAMMA", &v)?;
    }
    if let Some(v) = non_empty(lookup, "MAX_NOTIONAL_PER_LEG_USD")? {
        cfg.max_notional_per_leg_micro = MicroUsdc(parse_micro("MAX_NOTIONAL_PER_LEG_USD", &v)?);
    }
    if let Some(v) = non_empty(lookup, "BASE_ORDER_SIZE_SHARES")? {
        cfg.base_order_size_shares = SharesMicro(parse_micro("BASE_ORDER_SIZE_SHARES", &v)?);
    }
    if let Some(v) = non_empty(lookup, "MIN_COMBINED_SPREAD_USD")? {
        cfg.min_combined_spread_micro = MicroUsdc(parse_micro("MIN_COMBINED_SPREAD_USD", &v)?);
    }
    if let Some(v) = non_empty(lookup, "MAX_COMBINED_BID_USD")? {
        cfg.max_combined_bid_micro = MicroUsdc(parse_micro("MAX_COMBINED_BID_USD", &v)?);
    }
    if let Some(v) = non_empty(lookup, "HYSTERESIS_TICK_BUFFER")? {
        cfg.hysteresis_tick_buffer = parse_f64("HYSTERESIS_TICK_BUFFER", &v)?;
    }
    Ok(())
}

fn parse_pipeline_fields<F>(lookup: &F, cfg: &mut EngineConfig) -> Result<(), ConfigError>
where
    F: Fn(&'static str) -> Result<Option<String>, ConfigError>,
{
    if let Some(v) = non_empty(lookup, "VARIANCE_MODE")? {
        cfg.variance_mode = match v.to_ascii_lowercase().as_str() {
            "binary-cara" | "binary_cara" => VarianceMode::BinaryCara,
            "library-strict" | "library_strict" => VarianceMode::LibraryStrict,
            _ => {
                return Err(invalid_val(
                    "VARIANCE_MODE",
                    v,
                    "expected binary-cara or library-strict",
                ));
            }
        };
    }
    if let Some(v) = non_empty(lookup, "KAPPA_MODE")? {
        cfg.kappa_mode = match v.to_ascii_lowercase().as_str() {
            "trade-normalized" | "trade_normalized" => KappaMode::TradeNormalizedDepth,
            "log-slope" | "log_slope" => KappaMode::LibraryLogSlope,
            _ => {
                return Err(invalid_val(
                    "KAPPA_MODE",
                    v,
                    "expected trade-normalized or log-slope",
                ));
            }
        };
    }
    if let Some(v) = non_empty(lookup, "SG_MODE")? {
        cfg.sg_mode = match v.to_ascii_lowercase().as_str() {
            "causal" => SgMode::Causal,
            "symmetric" => SgMode::Symmetric,
            _ => return Err(invalid_val("SG_MODE", v, "expected causal or symmetric")),
        };
    }
    if let Some(v) = non_empty(lookup, "INCLUDE_VARIANCE_SPREAD")? {
        cfg.include_variance_spread = parse_bool("INCLUDE_VARIANCE_SPREAD", &v)?;
    }
    if let Some(v) = non_empty(lookup, "ENFORCE_FEE_FLOOR")? {
        cfg.enforce_fee_floor = parse_bool("ENFORCE_FEE_FLOOR", &v)?;
    }
    if let Some(v) = non_empty(lookup, "MIN_SPREAD_TICKS")? {
        cfg.min_spread_ticks = v
            .parse::<u32>()
            .map_err(|_| invalid_val("MIN_SPREAD_TICKS", v, "expected positive integer ticks"))?;
    }
    if let Some(v) = non_empty(lookup, "CLOCK_DRIFT_MAX_MS")? {
        cfg.clock_drift_max_ms = v
            .parse::<i64>()
            .map_err(|_| invalid_val("CLOCK_DRIFT_MAX_MS", v, "expected integer milliseconds"))?;
    }
    Ok(())
}

fn parse_credentials<F>(lookup: &F) -> Result<LiveCredentials, ConfigError>
where
    F: Fn(&'static str) -> Result<Option<String>, ConfigError>,
{
    Ok(LiveCredentials {
        signer_secret_key: non_empty(lookup, "SIGNER_SECRET_KEY")?.map(SecretString::from),
        clob_api_key: non_empty(lookup, "CLOB_API_KEY")?.map(SecretString::from),
        clob_api_secret: non_empty(lookup, "CLOB_API_SECRET")?.map(SecretString::from),
        clob_api_passphrase: non_empty(lookup, "CLOB_API_PASSPHRASE")?.map(SecretString::from),
        relayer_api_key: non_empty(lookup, "RELAYER_API_KEY")?.map(SecretString::from),
        chainlink_api_key: non_empty(lookup, "CHAINLINK_API_KEY")?.map(SecretString::from),
    })
}

fn validate_modes_and_credentials(cfg: &EngineConfig) -> Result<(), ConfigError> {
    if cfg.execution_mode == ExecutionMode::CanaryLive && cfg.dry_run {
        return Err(invariant(
            "canary-live mode cannot run with dry_run=true; set DRY_RUN=false explicitly",
        ));
    }
    if cfg.execution_mode != ExecutionMode::CanaryLive && !cfg.dry_run {
        return Err(invariant(
            "paper and dry-run modes require dry_run=true to keep order gates armed",
        ));
    }
    if cfg.polybolt_enabled && !cfg.credentials.has_polybolt_credentials() {
        return Err(invariant(
            "polybolt_enabled=true requires CLOB_API_KEY, CLOB_API_SECRET, CLOB_API_PASSPHRASE, and CHAINLINK_API_KEY",
        ));
    }
    if cfg.execution_mode == ExecutionMode::CanaryLive {
        if !cfg.credentials.has_clob_credentials() {
            return Err(invariant(
                "canary-live requires SIGNER_SECRET_KEY, CLOB_API_KEY, CLOB_API_SECRET, and CLOB_API_PASSPHRASE",
            ));
        }
        if cfg.signer_address.is_none() {
            return Err(invariant(
                "canary-live requires POLYMARKET_SIGNER_ADDRESS to be configured",
            ));
        }
        if cfg.wallet_mode != WalletMode::Eoa && cfg.proxy_address.is_none() {
            return Err(invariant(
                "canary-live proxy/1271 wallet mode requires POLYMARKET_PROXY_ADDRESS",
            ));
        }
        if cfg.gasless_relayer_enabled
            && (cfg.credentials.relayer_api_key.is_none() || cfg.relayer_address.is_none())
        {
            return Err(invariant(
                "canary-live with gasless relayer requires RELAYER_API_KEY and RELAYER_API_KEY_ADDRESS",
            ));
        }
    }
    Ok(())
}

fn validate_addresses(cfg: &EngineConfig) -> Result<(), ConfigError> {
    validate_relayer_url(&cfg.relayer_url)?;
    if let Some(addr) = &cfg.proxy_address {
        validate_eth_address("proxy_address", addr)?;
    }
    if let Some(addr) = &cfg.signer_address {
        validate_eth_address("signer_address", addr)?;
    }
    if let Some(addr) = &cfg.relayer_address {
        validate_eth_address("relayer_address", addr)?;
    }
    if cfg.wallet_mode == WalletMode::Eoa
        && let (Some(proxy), Some(signer)) = (&cfg.proxy_address, &cfg.signer_address)
        && !proxy.eq_ignore_ascii_case(signer)
    {
        return Err(invariant(
            "EOA wallet mode cannot use distinct proxy_address and signer_address",
        ));
    }
    Ok(())
}

fn validate_math_bounds(cfg: &EngineConfig) -> Result<(), ConfigError> {
    validate_risk_limits(cfg)?;
    validate_spread_and_timing(cfg)
}

fn validate_risk_limits(cfg: &EngineConfig) -> Result<(), ConfigError> {
    if !cfg.gamma.is_finite() || cfg.gamma <= 0.0 || cfg.gamma > MAX_GAMMA {
        return Err(invariant(format!(
            "gamma must be finite in (0, {MAX_GAMMA}], got {}",
            cfg.gamma
        )));
    }
    if cfg.max_notional_per_leg_micro.0 == 0
        || cfg.max_notional_per_leg_micro.0 > MAX_NOTIONAL_PER_LEG_MICRO
    {
        return Err(invariant(format!(
            "max_notional_per_leg_micro must be in (0, {MAX_NOTIONAL_PER_LEG_MICRO}]"
        )));
    }
    if cfg.base_order_size_shares.0 == 0
        || !cfg.base_order_size_shares.0.is_multiple_of(SIZE_STEP_MICRO)
    {
        return Err(invariant(format!(
            "base_order_size_shares ({}) must be > 0 and aligned to {SIZE_STEP_MICRO}",
            cfg.base_order_size_shares.0
        )));
    }
    Ok(())
}

fn validate_spread_and_timing(cfg: &EngineConfig) -> Result<(), ConfigError> {
    if cfg.min_spread_ticks == 0 {
        return Err(invariant("min_spread_ticks must be >= 1"));
    }
    if cfg.min_combined_spread_micro.0 < ONE_CENT_MICRO
        || cfg.min_combined_spread_micro.0 >= ONE_DOLLAR_MICRO
    {
        return Err(invariant(format!(
            "min_combined_spread_micro ({}) must be in [{ONE_CENT_MICRO}, {ONE_DOLLAR_MICRO})",
            cfg.min_combined_spread_micro.0
        )));
    }
    let sum = cfg
        .max_combined_bid_micro
        .0
        .checked_add(cfg.min_combined_spread_micro.0)
        .ok_or_else(|| invariant("max_combined_bid + min_combined_spread overflowed"))?;
    if cfg.max_combined_bid_micro.0 == 0 || sum > ONE_DOLLAR_MICRO {
        return Err(invariant(format!(
            "max_combined_bid ({}) + min_combined_spread ({}) must be <= {ONE_DOLLAR_MICRO}",
            cfg.max_combined_bid_micro.0, cfg.min_combined_spread_micro.0
        )));
    }
    if !cfg.hysteresis_tick_buffer.is_finite()
        || cfg.hysteresis_tick_buffer < 0.0
        || cfg.hysteresis_tick_buffer > MAX_HYSTERESIS_TICKS
    {
        return Err(invariant(format!(
            "hysteresis_tick_buffer must be in [0, {MAX_HYSTERESIS_TICKS}], got {}",
            cfg.hysteresis_tick_buffer
        )));
    }
    if cfg.clock_drift_max_ms <= 0 || cfg.clock_drift_max_ms > MAX_CLOCK_DRIFT_MS {
        return Err(invariant(format!(
            "clock_drift_max_ms must be in (0, {MAX_CLOCK_DRIFT_MS}], got {}",
            cfg.clock_drift_max_ms
        )));
    }
    Ok(())
}

fn validate_eth_address(field: &str, addr: &str) -> Result<(), ConfigError> {
    let hex = addr
        .strip_prefix("0x")
        .or_else(|| addr.strip_prefix("0X"))
        .ok_or_else(|| invariant(format!("{field} must start with 0x prefix")))?;
    if hex.len() != 40 || !hex.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(invariant(format!(
            "{field} must be a 40-hex-character EVM address"
        )));
    }
    if hex.bytes().all(|b| b == b'0') {
        return Err(invariant(format!("{field} cannot be the zero address")));
    }
    Ok(())
}

fn validate_relayer_url(url: &str) -> Result<(), ConfigError> {
    if url
        .bytes()
        .any(|b| b.is_ascii_whitespace() || b.is_ascii_control())
    {
        return Err(invariant(
            "relayer_url cannot contain whitespace or control chars",
        ));
    }
    if let Some(rest) = url.strip_prefix("https://") {
        let authority = rest.split(['/', '?', '#']).next().unwrap_or("");
        return validate_https_authority(authority);
    }
    if let Some(after) = url
        .strip_prefix("http://localhost")
        .or_else(|| url.strip_prefix("http://127.0.0.1"))
    {
        if has_valid_local_suffix(after) {
            return Ok(());
        }
        return Err(invariant(format!(
            "invalid localhost relayer_url suffix: {url}"
        )));
    }
    Err(invariant(format!(
        "relayer_url must use https:// (or localhost http://), got {url}"
    )))
}

fn validate_https_authority(authority: &str) -> Result<(), ConfigError> {
    if authority.is_empty() || authority.contains('@') || authority.starts_with('.') {
        return Err(invariant(
            "relayer_url must contain a clean host without userinfo",
        ));
    }
    let (host, port_opt) = match authority.split_once(':') {
        Some((h, p)) => (h, Some(p)),
        None => (authority, None),
    };
    if host.is_empty() || host.starts_with('.') || host.ends_with('.') {
        return Err(invariant("relayer_url host cannot be empty or malformed"));
    }
    if let Some(port) = port_opt
        && (port.is_empty()
            || !port.bytes().all(|b| b.is_ascii_digit())
            || !port.parse::<u16>().is_ok_and(|p| p > 0))
    {
        return Err(invariant("relayer_url port must be in 1..=65535"));
    }
    Ok(())
}

fn has_valid_local_suffix(after: &str) -> bool {
    if after.is_empty() || after.starts_with('/') {
        return true;
    }
    if let Some(port_and_tail) = after.strip_prefix(':') {
        let port = port_and_tail.split(['/', '?', '#']).next().unwrap_or("");
        return !port.is_empty()
            && port.bytes().all(|b| b.is_ascii_digit())
            && port.parse::<u16>().is_ok_and(|p| p > 0);
    }
    false
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
            } else if trimmed.len() == raw.len() {
                Ok(Some(raw))
            } else {
                Ok(Some(trimmed.to_string()))
            }
        }
        None => Ok(None),
    }
}

fn parse_bool(key: &'static str, raw: &str) -> Result<bool, ConfigError> {
    match raw.to_ascii_lowercase().as_str() {
        "true" | "1" => Ok(true),
        "false" | "0" => Ok(false),
        _ => Err(invalid_val(key, raw.to_string(), "expected true or false")),
    }
}

fn parse_f64(key: &'static str, raw: &str) -> Result<f64, ConfigError> {
    raw.parse::<f64>()
        .map_err(|_| invalid_val(key, raw.to_string(), "expected valid floating-point number"))
}

fn parse_micro(key: &'static str, raw: &str) -> Result<u64, ConfigError> {
    parse_decimal_micro(raw.as_bytes()).map_err(|_| {
        invalid_val(
            key,
            raw.to_string(),
            "expected non-negative decimal with at most 6 fractional digits",
        )
    })
}

fn invalid_val(key: &'static str, value: String, reason: &str) -> ConfigError {
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
