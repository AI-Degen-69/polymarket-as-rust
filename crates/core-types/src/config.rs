use std::fmt;
use std::sync::OnceLock;

use secrecy::SecretString;
use serde::{Deserialize, Serialize};

const EMBEDDED_DEFAULTS_JSON: &str = include_str!("../../../DEFAULTS.json");

use crate::error::{ConfigError, redact_presence};
use crate::market::{ExecutionMode, KappaMode, SgMode, VarianceMode, WalletMode};
use crate::money::{
    MicroUsdc, ONE_CENT_MICRO, ONE_DOLLAR_MICRO, SIZE_STEP_MICRO, SharesMicro, TickPrecision,
    parse_decimal_micro,
};

const MAX_GAMMA: f64 = 100.0;
const MAX_NOTIONAL_PER_LEG_MICRO: u64 = 1_000_000 * ONE_DOLLAR_MICRO;
const MAX_HYSTERESIS_TICKS: f64 = 10.0;
const MAX_CLOCK_DRIFT_MS: i64 = 60_000;
const MAX_LEAD_LAG_WINDOW_MS: u64 = 6_000;
const MIN_LEAD_LAG_WINDOW_MS: u64 = 50;
const MAX_LEAD_LAG_COOLDOWN_MS: u64 = 10_000;
const EPOCH_DURATION_SECS: u64 = 300;
const MAX_ENTRY_OPEN_DELAY_SECS: u64 = 240;
const MAX_MIN_SPREAD_TICKS: u32 =
    ((ONE_DOLLAR_MICRO / TickPrecision::TenthMill.tick_micro().0) - 1) as u32;

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
    /// CARA inventory risk aversion parameter `gamma` (`0.12` default).
    pub gamma: f64,
    /// Maximum unhedged dollar notional allowed per leg (`$50.00` default).
    pub max_notional_per_leg_micro: MicroUsdc,
    /// Baseline order size in outcome shares (`20` shares default).
    pub base_order_size_shares: SharesMicro,
    /// Minimum combined spread floor (`$0.01` default, calibrated 1-tick BBO floor).
    pub min_combined_spread_micro: MicroUsdc,
    /// Minimum combined spread floor in integer ticks (`1` default).
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
    /// Whether Station 4 enforces taker-fee profitability floor (`false` default).
    pub enforce_fee_floor: bool,
    /// Optional cross-tick pair-cost loss budget in micro-USDC (`Some(80_000)` default; `None` disables cross-tick cap).
    pub max_cross_tick_loss_micro: Option<u64>,
    /// Optional extreme-probability entry guard in `(0.5, 1.0)` (`Some(0.80)` default; `None` disables entry guard).
    pub max_entry_prob: Option<f64>,
    /// Optional upper-only favorite entry price cap in `(0.0, 1.0)` (`Some(0.65)` default; `None` disables upper cap, suppresses fresh entries strictly above cap while preserving cheap legs and hedges).
    pub max_entry_price: Option<f64>,
    /// Binance lead-lag fair-probability shift triggering defensive cancel (`0.015` default).
    pub lead_lag_cancel_threshold: f64,
    /// Rolling Binance lead-lag lookback window in ms (`1000ms` default; `0` = legacy tick-to-tick).
    pub lead_lag_window_ms: u64,
    /// Per-side defensive cancel suppression cooldown in ms (`1000ms` default; `0` = legacy one-shot).
    pub lead_lag_cooldown_ms: u64,
    /// Cancel only the endangered side on lead-lag move (`true` default; `false` = legacy symmetric cancel).
    pub lead_lag_directional: bool,
    /// Asymmetric reducing-leg skew multiplier in `[0.0, 1.0]` (`1.0` default).
    pub hedge_skew_mult: f64,
    /// Cap Tier-1 reducing-leg size to `|Delta_q|` when `|Delta_q| >= 1` share (`true` default).
    pub cap_tier1_reducing_to_delta: bool,
    /// Optional opposing-ask price trigger in `(0.0, 1.0)` unlocking Tier-1/Tier-2 passive 1:1 deficit cross-tick override (`None` = off default).
    pub tier1_hedge_trigger_price: Option<f64>,
    /// Seconds after epoch open during which fresh Leg-1 entries are skipped (`15` default; `0` = disabled).
    pub entry_open_delay_secs: u64,
    /// Optional elapsed seconds after epoch open at which fresh Leg-1 entries stop (`Some(120)` default; `None` = disabled).
    pub entry_cutoff_secs: Option<u64>,
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
    max_cross_tick_loss_micro: Option<u64>,
    max_entry_prob: Option<f64>,
    max_entry_price: Option<f64>,
    lead_lag_cancel_threshold: f64,
    lead_lag_window_ms: u64,
    lead_lag_cooldown_ms: u64,
    lead_lag_directional: bool,
    hedge_skew_mult: f64,
    cap_tier1_reducing_to_delta: bool,
    tier1_hedge_trigger_price: Option<f64>,
    entry_open_delay_secs: u64,
    entry_cutoff_secs: Option<u64>,
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
            max_cross_tick_loss_micro: d.max_cross_tick_loss_micro,
            max_entry_prob: d.max_entry_prob,
            max_entry_price: d.max_entry_price,
            lead_lag_cancel_threshold: d.lead_lag_cancel_threshold,
            lead_lag_window_ms: d.lead_lag_window_ms,
            lead_lag_cooldown_ms: d.lead_lag_cooldown_ms,
            lead_lag_directional: d.lead_lag_directional,
            hedge_skew_mult: d.hedge_skew_mult,
            cap_tier1_reducing_to_delta: d.cap_tier1_reducing_to_delta,
            tier1_hedge_trigger_price: d.tier1_hedge_trigger_price,
            entry_open_delay_secs: d.entry_open_delay_secs,
            entry_cutoff_secs: d.entry_cutoff_secs,
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
            max_cross_tick_loss_micro: raw.max_cross_tick_loss_micro,
            max_entry_prob: raw.max_entry_prob,
            max_entry_price: raw.max_entry_price,
            lead_lag_cancel_threshold: raw.lead_lag_cancel_threshold,
            lead_lag_window_ms: raw.lead_lag_window_ms,
            lead_lag_cooldown_ms: raw.lead_lag_cooldown_ms,
            lead_lag_directional: raw.lead_lag_directional,
            hedge_skew_mult: raw.hedge_skew_mult,
            cap_tier1_reducing_to_delta: raw.cap_tier1_reducing_to_delta,
            tier1_hedge_trigger_price: raw.tier1_hedge_trigger_price,
            entry_open_delay_secs: raw.entry_open_delay_secs,
            entry_cutoff_secs: raw.entry_cutoff_secs,
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
            .field("max_cross_tick_loss_micro", &self.max_cross_tick_loss_micro)
            .field("max_entry_prob", &self.max_entry_prob)
            .field("max_entry_price", &self.max_entry_price)
            .field("lead_lag_cancel_threshold", &self.lead_lag_cancel_threshold)
            .field("lead_lag_window_ms", &self.lead_lag_window_ms)
            .field("lead_lag_cooldown_ms", &self.lead_lag_cooldown_ms)
            .field("lead_lag_directional", &self.lead_lag_directional)
            .field("hedge_skew_mult", &self.hedge_skew_mult)
            .field(
                "cap_tier1_reducing_to_delta",
                &self.cap_tier1_reducing_to_delta,
            )
            .field("tier1_hedge_trigger_price", &self.tier1_hedge_trigger_price)
            .field("entry_open_delay_secs", &self.entry_open_delay_secs)
            .field("entry_cutoff_secs", &self.entry_cutoff_secs)
            .field("clock_drift_max_ms", &self.clock_drift_max_ms)
            .field("credentials", &self.credentials)
            .finish()
    }
}

/// Queue decay model default stored in `DEFAULTS.json`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DefaultQueueDecay {
    /// Conservative queue-fill model (`DEFAULTS.json` simulation floor).
    Conservative,
    /// Optimistic queue-fill model.
    Optimistic,
}

/// Simulation and maker-rebate defaults loaded from root `DEFAULTS.json`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SimulationDefaults {
    /// Default queue decay policy (`conservative` or `optimistic`).
    pub queue_decay: DefaultQueueDecay,
    /// Strict live maker rebate floor below Tier-1 weighted volume (`0` = `0%`).
    pub maker_rebate_floor_e6: u64,
    /// Post-Nov-21 Tier-1 (`$200K–$1M` 30d wV) maker rebate share (`30_000` = `3%`).
    pub maker_rebate_tier1_e6: u64,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct EmbeddedEngineDefaults {
    execution_mode: ExecutionMode,
    dry_run: bool,
    wallet_mode: WalletMode,
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
    max_cross_tick_loss_micro: Option<u64>,
    max_entry_prob: Option<f64>,
    max_entry_price: Option<f64>,
    lead_lag_cancel_threshold: f64,
    lead_lag_window_ms: u64,
    lead_lag_cooldown_ms: u64,
    lead_lag_directional: bool,
    hedge_skew_mult: f64,
    cap_tier1_reducing_to_delta: bool,
    tier1_hedge_trigger_price: Option<f64>,
    entry_open_delay_secs: u64,
    entry_cutoff_secs: Option<u64>,
    clock_drift_max_ms: i64,
}

/// Provenance metadata recorded in repository root `DEFAULTS.json`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DefaultsProvenance {
    /// Source sweep folder relative to repository root.
    pub source_sweep: String,
    /// Sweep run identifier.
    pub run_id: String,
    /// Run date window (UTC).
    pub date: String,
    /// Dataset capture and window count summary.
    pub dataset: String,
    /// Selected configuration and promoted override parameters.
    pub selected_config: String,
    /// Supporting selection and roadmap report path.
    pub report: String,
    /// Validation status tag (e.g., provisional research baseline).
    pub status: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct EmbeddedDefaultsFile {
    #[serde(default)]
    provenance: Option<DefaultsProvenance>,
    engine: EmbeddedEngineDefaults,
    simulation: SimulationDefaults,
}

#[derive(Clone)]
struct DefaultBundle {
    engine: EngineConfig,
    simulation: SimulationDefaults,
    provenance: Option<DefaultsProvenance>,
}

impl DefaultBundle {
    fn fallback() -> Self {
        let engine = EngineConfig::hardcoded_fallback();
        let simulation = SimulationDefaults {
            queue_decay: DefaultQueueDecay::Conservative,
            maker_rebate_floor_e6: 0,
            maker_rebate_tier1_e6: 30_000,
        };
        Self {
            engine,
            simulation,
            provenance: None,
        }
    }
}

static DEFAULT_BUNDLE: OnceLock<DefaultBundle> = OnceLock::new();

fn parse_defaults_bundle_from_str(json: &str) -> Result<DefaultBundle, ConfigError> {
    let parsed: EmbeddedDefaultsFile =
        serde_json::from_str(json).map_err(|err| ConfigError::InvalidValue {
            key: "DEFAULTS.json",
            value: "<embedded>".to_string(),
            reason: err.to_string(),
        })?;
    let e = parsed.engine;
    let engine = EngineConfig {
        execution_mode: e.execution_mode,
        dry_run: e.dry_run,
        wallet_mode: e.wallet_mode,
        proxy_address: None,
        signer_address: None,
        relayer_address: None,
        relayer_url: e.relayer_url,
        gasless_relayer_enabled: e.gasless_relayer_enabled,
        polybolt_enabled: e.polybolt_enabled,
        gamma: e.gamma,
        max_notional_per_leg_micro: e.max_notional_per_leg_micro,
        base_order_size_shares: e.base_order_size_shares,
        min_combined_spread_micro: e.min_combined_spread_micro,
        min_spread_ticks: e.min_spread_ticks,
        max_combined_bid_micro: e.max_combined_bid_micro,
        hysteresis_tick_buffer: e.hysteresis_tick_buffer,
        variance_mode: e.variance_mode,
        kappa_mode: e.kappa_mode,
        sg_mode: e.sg_mode,
        include_variance_spread: e.include_variance_spread,
        enforce_fee_floor: e.enforce_fee_floor,
        max_cross_tick_loss_micro: e.max_cross_tick_loss_micro,
        max_entry_prob: e.max_entry_prob,
        max_entry_price: e.max_entry_price,
        lead_lag_cancel_threshold: e.lead_lag_cancel_threshold,
        lead_lag_window_ms: e.lead_lag_window_ms,
        lead_lag_cooldown_ms: e.lead_lag_cooldown_ms,
        lead_lag_directional: e.lead_lag_directional,
        hedge_skew_mult: e.hedge_skew_mult,
        cap_tier1_reducing_to_delta: e.cap_tier1_reducing_to_delta,
        tier1_hedge_trigger_price: e.tier1_hedge_trigger_price,
        entry_open_delay_secs: e.entry_open_delay_secs,
        entry_cutoff_secs: e.entry_cutoff_secs,
        clock_drift_max_ms: e.clock_drift_max_ms,
        credentials: LiveCredentials::default(),
    };
    validate_modes(&engine)?;
    validate_addresses(&engine)?;
    validate_math_bounds(&engine)?;
    Ok(DefaultBundle {
        engine,
        simulation: parsed.simulation,
        provenance: parsed.provenance,
    })
}

fn parse_embedded_defaults_bundle() -> Result<DefaultBundle, ConfigError> {
    parse_defaults_bundle_from_str(EMBEDDED_DEFAULTS_JSON)
}

fn default_bundle() -> &'static DefaultBundle {
    DEFAULT_BUNDLE.get_or_init(|| {
        parse_embedded_defaults_bundle().unwrap_or_else(|err| {
            eprintln!("warning: embedded DEFAULTS.json failed to parse/validate ({err}); using hardcoded fallback");
            DefaultBundle::fallback()
        })
    })
}

/// Returns simulation defaults loaded from repository root `DEFAULTS.json`.
#[must_use]
pub fn default_simulation_settings() -> SimulationDefaults {
    default_bundle().simulation
}

/// Returns optional provenance metadata loaded from repository root `DEFAULTS.json`.
#[must_use]
pub fn default_provenance() -> Option<&'static DefaultsProvenance> {
    default_bundle().provenance.as_ref()
}

impl EngineConfig {
    fn hardcoded_fallback() -> Self {
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
            gamma: 0.12,
            max_notional_per_leg_micro: MicroUsdc(50_000_000),
            base_order_size_shares: SharesMicro(20_000_000),
            min_combined_spread_micro: MicroUsdc(10_000),
            min_spread_ticks: 1,
            max_combined_bid_micro: MicroUsdc(980_000),
            hysteresis_tick_buffer: 0.25,
            variance_mode: VarianceMode::BinaryCara,
            kappa_mode: KappaMode::TradeNormalizedDepth,
            sg_mode: SgMode::Causal,
            include_variance_spread: true,
            enforce_fee_floor: false,
            max_cross_tick_loss_micro: Some(80_000),
            max_entry_prob: Some(0.80),
            max_entry_price: Some(0.65),
            lead_lag_cancel_threshold: 0.015,
            lead_lag_window_ms: 1_000,
            lead_lag_cooldown_ms: 1_000,
            lead_lag_directional: true,
            hedge_skew_mult: 1.0,
            cap_tier1_reducing_to_delta: true,
            tier1_hedge_trigger_price: None,
            entry_open_delay_secs: 15,
            entry_cutoff_secs: Some(120),
            clock_drift_max_ms: 1_000,
            credentials: LiveCredentials::default(),
        }
    }
}

impl Default for EngineConfig {
    fn default() -> Self {
        default_bundle().engine.clone()
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
        validate_modes(self)?;
        validate_credentials(self)?;
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
    if let Some(v) = non_empty(lookup, "MAX_CROSS_TICK_LOSS_MICRO")? {
        if v.eq_ignore_ascii_case("off") || v.eq_ignore_ascii_case("none") {
            cfg.max_cross_tick_loss_micro = None;
        } else {
            cfg.max_cross_tick_loss_micro = Some(v.parse::<u64>().map_err(|_| {
                invalid_val(
                    "MAX_CROSS_TICK_LOSS_MICRO",
                    v,
                    "expected non-negative integer micro-USDC or 'off'",
                )
            })?);
        }
    }
    if let Some(v) = non_empty(lookup, "MAX_ENTRY_PROB")? {
        if v.eq_ignore_ascii_case("off") || v.eq_ignore_ascii_case("none") {
            cfg.max_entry_prob = None;
        } else {
            cfg.max_entry_prob = Some(parse_f64("MAX_ENTRY_PROB", &v)?);
        }
    }
    if let Some(v) = non_empty(lookup, "MAX_ENTRY_PRICE")? {
        if v.eq_ignore_ascii_case("off") || v.eq_ignore_ascii_case("none") {
            cfg.max_entry_price = None;
        } else {
            cfg.max_entry_price = Some(parse_f64("MAX_ENTRY_PRICE", &v)?);
        }
    }
    if let Some(v) = non_empty(lookup, "HEDGE_SKEW_MULT")? {
        cfg.hedge_skew_mult = parse_f64("HEDGE_SKEW_MULT", &v)?;
    }
    if let Some(v) = non_empty(lookup, "CAP_TIER1_REDUCING_TO_DELTA")? {
        cfg.cap_tier1_reducing_to_delta = parse_bool("CAP_TIER1_REDUCING_TO_DELTA", &v)?;
    }
    if let Some(v) = non_empty(lookup, "TIER1_HEDGE_TRIGGER_PRICE")? {
        if v.eq_ignore_ascii_case("off") || v.eq_ignore_ascii_case("none") {
            cfg.tier1_hedge_trigger_price = None;
        } else {
            cfg.tier1_hedge_trigger_price = Some(parse_f64("TIER1_HEDGE_TRIGGER_PRICE", &v)?);
        }
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
    if let Some(v) = non_empty(lookup, "LEAD_LAG_CANCEL_THRESHOLD")? {
        cfg.lead_lag_cancel_threshold = parse_f64("LEAD_LAG_CANCEL_THRESHOLD", &v)?;
    }
    if let Some(v) = non_empty(lookup, "LEAD_LAG_WINDOW_MS")? {
        cfg.lead_lag_window_ms = v.parse::<u64>().map_err(|_| {
            invalid_val(
                "LEAD_LAG_WINDOW_MS",
                v,
                "expected non-negative integer milliseconds",
            )
        })?;
    }
    if let Some(v) = non_empty(lookup, "LEAD_LAG_COOLDOWN_MS")? {
        cfg.lead_lag_cooldown_ms = v.parse::<u64>().map_err(|_| {
            invalid_val(
                "LEAD_LAG_COOLDOWN_MS",
                v,
                "expected non-negative integer milliseconds",
            )
        })?;
    }
    if let Some(v) = non_empty(lookup, "LEAD_LAG_DIRECTIONAL")? {
        cfg.lead_lag_directional = parse_bool("LEAD_LAG_DIRECTIONAL", &v)?;
    }
    if let Some(v) = non_empty(lookup, "ENTRY_OPEN_DELAY_SECS")? {
        cfg.entry_open_delay_secs = v.parse::<u64>().map_err(|_| {
            invalid_val(
                "ENTRY_OPEN_DELAY_SECS",
                v,
                "expected non-negative integer seconds",
            )
        })?;
    }
    if let Some(v) = non_empty(lookup, "ENTRY_CUTOFF_SECS")? {
        if v.eq_ignore_ascii_case("off") || v.eq_ignore_ascii_case("none") {
            cfg.entry_cutoff_secs = None;
        } else {
            cfg.entry_cutoff_secs = Some(v.parse::<u64>().map_err(|_| {
                invalid_val(
                    "ENTRY_CUTOFF_SECS",
                    v,
                    "expected positive integer seconds or 'off'",
                )
            })?);
        }
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

fn validate_modes(cfg: &EngineConfig) -> Result<(), ConfigError> {
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
    Ok(())
}

fn validate_credentials(cfg: &EngineConfig) -> Result<(), ConfigError> {
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
    if cfg.min_spread_ticks == 0 || cfg.min_spread_ticks > MAX_MIN_SPREAD_TICKS {
        return Err(invariant(format!(
            "MIN_SPREAD_TICKS ({}) must be in 1..={MAX_MIN_SPREAD_TICKS}",
            cfg.min_spread_ticks
        )));
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
    if let Some(p_max) = cfg.max_entry_prob
        && (!p_max.is_finite() || p_max <= 0.5 || p_max >= 1.0)
    {
        return Err(invariant(format!(
            "max_entry_prob must be finite in (0.5, 1.0), got {p_max}"
        )));
    }
    if let Some(p_cap) = cfg.max_entry_price
        && (!p_cap.is_finite() || p_cap <= 0.0 || p_cap >= 1.0)
    {
        return Err(invariant(format!(
            "max_entry_price must be finite in (0.0, 1.0), got {p_cap}"
        )));
    }
    if !cfg.lead_lag_cancel_threshold.is_finite()
        || cfg.lead_lag_cancel_threshold <= 0.0
        || cfg.lead_lag_cancel_threshold >= 1.0
    {
        return Err(invariant(format!(
            "lead_lag_cancel_threshold must be finite in (0.0, 1.0), got {}",
            cfg.lead_lag_cancel_threshold
        )));
    }
    if cfg.lead_lag_window_ms > MAX_LEAD_LAG_WINDOW_MS {
        return Err(invariant(format!(
            "lead_lag_window_ms ({}) must be <= {MAX_LEAD_LAG_WINDOW_MS}",
            cfg.lead_lag_window_ms
        )));
    }
    if cfg.lead_lag_window_ms > 0 && cfg.lead_lag_window_ms < MIN_LEAD_LAG_WINDOW_MS {
        return Err(invariant(format!(
            "lead_lag_window_ms ({}) must be 0 or >= {MIN_LEAD_LAG_WINDOW_MS}ms",
            cfg.lead_lag_window_ms
        )));
    }
    if cfg.lead_lag_cooldown_ms > MAX_LEAD_LAG_COOLDOWN_MS {
        return Err(invariant(format!(
            "lead_lag_cooldown_ms ({}) must be <= {MAX_LEAD_LAG_COOLDOWN_MS}",
            cfg.lead_lag_cooldown_ms
        )));
    }
    if !cfg.hedge_skew_mult.is_finite() || !(0.0..=1.0).contains(&cfg.hedge_skew_mult) {
        return Err(invariant(format!(
            "hedge_skew_mult must be finite in [0.0, 1.0], got {}",
            cfg.hedge_skew_mult
        )));
    }
    if let Some(p_trig) = cfg.tier1_hedge_trigger_price {
        if !p_trig.is_finite() || p_trig <= 0.0 || p_trig >= 1.0 {
            return Err(invariant(format!(
                "tier1_hedge_trigger_price must be finite in (0.0, 1.0), got {p_trig}"
            )));
        }
        if !cfg.cap_tier1_reducing_to_delta {
            return Err(invariant(
                "tier1_hedge_trigger_price requires cap_tier1_reducing_to_delta=true to prevent reducing-leg overshoot",
            ));
        }
    }
    if cfg.entry_open_delay_secs > MAX_ENTRY_OPEN_DELAY_SECS {
        return Err(invariant(format!(
            "entry_open_delay_secs ({}) must be <= {MAX_ENTRY_OPEN_DELAY_SECS}",
            cfg.entry_open_delay_secs
        )));
    }
    if let Some(cutoff) = cfg.entry_cutoff_secs {
        if cutoff == 0 || cutoff > EPOCH_DURATION_SECS {
            return Err(invariant(format!(
                "entry_cutoff_secs ({cutoff}) must be in 1..={EPOCH_DURATION_SECS}"
            )));
        }
        if cfg.entry_open_delay_secs >= cutoff {
            return Err(invariant(format!(
                "entry_open_delay_secs ({}) must be < entry_cutoff_secs ({cutoff})",
                cfg.entry_open_delay_secs
            )));
        }
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
    fn lead_lag_config_defaults_env_and_bounds() {
        let def = EngineConfig::from_lookup(|_| Ok(None)).unwrap();
        assert!((def.lead_lag_cancel_threshold - 0.015).abs() < 1e-12);
        assert_eq!(def.lead_lag_window_ms, 1_000);
        assert_eq!(def.lead_lag_cooldown_ms, 1_000);
        assert!(def.lead_lag_directional);

        let mut env_ok = HashMap::new();
        env_ok.insert("LEAD_LAG_CANCEL_THRESHOLD", "0.02");
        env_ok.insert("LEAD_LAG_WINDOW_MS", "1000");
        env_ok.insert("LEAD_LAG_COOLDOWN_MS", "500");
        env_ok.insert("LEAD_LAG_DIRECTIONAL", "true");
        let parsed = EngineConfig::from_lookup(lookup_map(&env_ok)).unwrap();
        assert!((parsed.lead_lag_cancel_threshold - 0.02).abs() < 1e-12);
        assert_eq!(parsed.lead_lag_window_ms, 1_000);
        assert_eq!(parsed.lead_lag_cooldown_ms, 500);
        assert!(parsed.lead_lag_directional);

        // Threshold bounds: <= 0.0 or >= 1.0 or NaN rejected
        for bad_th in ["0.0", "-0.01", "1.0", "1.5", "NaN"] {
            let mut m = HashMap::new();
            m.insert("LEAD_LAG_CANCEL_THRESHOLD", bad_th);
            assert!(
                EngineConfig::from_lookup(lookup_map(&m)).is_err(),
                "expected error for threshold={bad_th}"
            );
        }

        // Window > 6000 rejected
        let mut bad_win = HashMap::new();
        bad_win.insert("LEAD_LAG_WINDOW_MS", "6001");
        assert!(EngineConfig::from_lookup(lookup_map(&bad_win)).is_err());

        // Window in 1..49 rejected (below 50ms bucket resolution)
        for sub_bucket in ["1", "10", "49"] {
            let mut bad_sub = HashMap::new();
            bad_sub.insert("LEAD_LAG_WINDOW_MS", sub_bucket);
            assert!(
                EngineConfig::from_lookup(lookup_map(&bad_sub)).is_err(),
                "expected error for window={sub_bucket}"
            );
        }

        // Cooldown > 10000 rejected
        let mut bad_cd = HashMap::new();
        bad_cd.insert("LEAD_LAG_COOLDOWN_MS", "10001");
        assert!(EngineConfig::from_lookup(lookup_map(&bad_cd)).is_err());
    }

    /// `.env.example` is operator-facing, so it must mirror `EngineConfig::default()`.
    #[test]
    fn env_example_tuning_matches_defaults() {
        let raw =
            std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/../../.env.example"))
                .expect("read .env.example");
        let map: HashMap<&str, &str> = raw
            .lines()
            .filter(|line| !line.trim_start().starts_with('#'))
            .filter_map(|line| line.split_once('='))
            .map(|(key, value)| (key.trim(), value.trim()))
            .collect();

        const TUNING_KEYS: [&str; 25] = [
            "EXECUTION_MODE",
            "DRY_RUN",
            "ENGINE_GAMMA",
            "MAX_NOTIONAL_PER_LEG_USD",
            "BASE_ORDER_SIZE_SHARES",
            "MIN_COMBINED_SPREAD_USD",
            "MAX_COMBINED_BID_USD",
            "HYSTERESIS_TICK_BUFFER",
            "VARIANCE_MODE",
            "KAPPA_MODE",
            "SG_MODE",
            "INCLUDE_VARIANCE_SPREAD",
            "MAX_ENTRY_PROB",
            "MAX_ENTRY_PRICE",
            "MAX_CROSS_TICK_LOSS_MICRO",
            "LEAD_LAG_CANCEL_THRESHOLD",
            "LEAD_LAG_WINDOW_MS",
            "LEAD_LAG_COOLDOWN_MS",
            "LEAD_LAG_DIRECTIONAL",
            "HEDGE_SKEW_MULT",
            "CAP_TIER1_REDUCING_TO_DELTA",
            "TIER1_HEDGE_TRIGGER_PRICE",
            "ENTRY_OPEN_DELAY_SECS",
            "ENTRY_CUTOFF_SECS",
            "CLOCK_DRIFT_MAX_MS",
        ];
        for key in TUNING_KEYS {
            assert!(map.contains_key(key), ".env.example is missing {key}");
        }

        let parsed =
            EngineConfig::from_lookup(|key| Ok(map.get(key).map(|value| (*value).to_string())))
                .unwrap();
        let def = EngineConfig::default();
        macro_rules! assert_same {
            ($($field:ident),+ $(,)?) => {
                $(
                    assert_eq!(
                        parsed.$field,
                        def.$field,
                        ".env.example drifts from default on {}",
                        stringify!($field)
                    );
                )+
            };
        }
        assert_same!(
            execution_mode,
            dry_run,
            gamma,
            max_notional_per_leg_micro,
            base_order_size_shares,
            min_combined_spread_micro,
            max_combined_bid_micro,
            hysteresis_tick_buffer,
            variance_mode,
            kappa_mode,
            sg_mode,
            include_variance_spread,
            max_entry_prob,
            max_entry_price,
            max_cross_tick_loss_micro,
            lead_lag_cancel_threshold,
            lead_lag_window_ms,
            lead_lag_cooldown_ms,
            lead_lag_directional,
            hedge_skew_mult,
            cap_tier1_reducing_to_delta,
            tier1_hedge_trigger_price,
            entry_open_delay_secs,
            entry_cutoff_secs,
            clock_drift_max_ms,
        );
    }

    #[test]
    fn embedded_defaults_json_parses_validates_and_matches_default() {
        let bundle =
            parse_embedded_defaults_bundle().expect("DEFAULTS.json must parse and validate");
        let cfg = EngineConfig::default();
        assert_eq!(cfg.gamma, bundle.engine.gamma);
        assert_eq!(
            cfg.max_combined_bid_micro,
            bundle.engine.max_combined_bid_micro
        );
        assert_eq!(cfg.max_entry_prob, bundle.engine.max_entry_prob);
        assert_eq!(cfg.max_entry_price, bundle.engine.max_entry_price);
        assert_eq!(
            cfg.max_cross_tick_loss_micro,
            bundle.engine.max_cross_tick_loss_micro
        );
        assert_eq!(cfg.gamma, 0.12);
        assert_eq!(cfg.max_combined_bid_micro, MicroUsdc(980_000));
        assert_eq!(cfg.max_entry_prob, Some(0.80));
        assert_eq!(cfg.max_entry_price, Some(0.65));
        assert_eq!(
            cfg.cap_tier1_reducing_to_delta,
            bundle.engine.cap_tier1_reducing_to_delta
        );
        assert!(cfg.cap_tier1_reducing_to_delta);
        assert_eq!(
            cfg.tier1_hedge_trigger_price,
            bundle.engine.tier1_hedge_trigger_price
        );
        assert_eq!(cfg.tier1_hedge_trigger_price, None);
        assert_eq!(
            cfg.entry_open_delay_secs,
            bundle.engine.entry_open_delay_secs
        );
        assert_eq!(cfg.entry_open_delay_secs, 15);
        assert_eq!(cfg.entry_cutoff_secs, bundle.engine.entry_cutoff_secs);
        assert_eq!(cfg.entry_cutoff_secs, Some(120));

        let fallback = EngineConfig::hardcoded_fallback();
        assert_eq!(fallback.max_entry_price, cfg.max_entry_price);
        assert_eq!(
            fallback.cap_tier1_reducing_to_delta,
            cfg.cap_tier1_reducing_to_delta
        );
        assert_eq!(
            fallback.tier1_hedge_trigger_price,
            cfg.tier1_hedge_trigger_price
        );
        assert_eq!(fallback.entry_open_delay_secs, cfg.entry_open_delay_secs);
        assert_eq!(fallback.entry_cutoff_secs, cfg.entry_cutoff_secs);
        assert_eq!(
            default_simulation_settings().queue_decay,
            DefaultQueueDecay::Conservative
        );
        assert_eq!(default_simulation_settings().maker_rebate_floor_e6, 0);
        assert_eq!(default_simulation_settings().maker_rebate_tier1_e6, 30_000);

        let prov = default_provenance().expect("DEFAULTS.json provenance must be populated");
        assert_eq!(
            prov.source_sweep,
            "logs/sweeps/25-11_21-55-50Z_boundary-9runs_20caps-226w"
        );
        assert_eq!(prov.run_id, "boundary-9runs_20caps-226w");
        assert_eq!(
            prov.selected_config,
            "Config #6/9 (max_entry_price=0.65, entry_cutoff_secs=120, tier1_hedge_trigger_price=off)"
        );
        assert_eq!(prov.report, "docs/boundary-sweep-selection-report.md");
    }

    #[test]
    fn embedded_defaults_parser_allows_polybolt_without_credentials() {
        let json = EMBEDDED_DEFAULTS_JSON.replacen(
            "\"polybolt_enabled\": false",
            "\"polybolt_enabled\": true",
            1,
        );
        let bundle = parse_defaults_bundle_from_str(&json)
            .expect("embedded parser must allow polybolt_enabled=true before env credentials load");
        assert!(bundle.engine.polybolt_enabled);
        assert!(
            bundle.engine.validate().is_err(),
            "full EngineConfig::validate() must still require PolyBolt credentials"
        );
    }
}
