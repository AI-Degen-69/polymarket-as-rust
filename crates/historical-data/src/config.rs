use std::fmt;
use std::path::PathBuf;

use core_types::ConfigError;
use secrecy::{ExposeSecret, SecretString};
use serde::{Deserialize, Serialize};

use crate::error::HistoricalError;

/// Absolute hard maximum download quota units allowed per billing cycle.
pub const HARD_MAX_QUOTA_CAP: u32 = 650;

/// Default PMData API v1 base URL.
pub const DEFAULT_PMDATA_BASE_URL: &str = "https://api.pmdata.dev/v1";

/// Default public PMData Polymarket markets catalog Parquet URL (free, 0 quota).
pub const DEFAULT_PMDATA_CATALOG_URL: &str =
    "https://public-data.pmdata.dev/v1/polymarket/markets.parquet";

/// Default local cache directory for downloaded PMData archives and Parquet files.
pub const DEFAULT_PMDATA_CACHE_DIR: &str = "data/pmdata";

/// Default persistent download manifest path.
pub const DEFAULT_PMDATA_MANIFEST_PATH: &str = "data/pmdata/manifest.json";

/// Explicit PMData L2 ingestion mode controlling whether both outcome books are downloaded.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum IngestionMode {
    /// Default reference-quality mode: downloads both Yes L2 and No L2 (`?outcome=No`), plus trades.
    #[default]
    Full,
    /// Quota-saving mode: downloads Yes L2 plus trades and derives the complementary Down book
    /// only when validated uncrossed in `(0, 1)`, stamped with `ComplementaryFromYesL2`.
    QuotaSaving,
}

impl IngestionMode {
    /// Stable lowercase identifier.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Full => "full",
            Self::QuotaSaving => "quota-saving",
        }
    }
}

/// Replay source filter restricting which provenance origins enter backtest evaluation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum SourceFilter {
    /// Allow both `live_jsonl` and `pmdata` events while retaining full provenance.
    #[default]
    All,
    /// Allow only `live_jsonl` events.
    LiveOnly,
    /// Allow only `pmdata` events.
    PmdataOnly,
}

impl SourceFilter {
    /// Stable CLI label.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::All => "all",
            Self::LiveOnly => "live",
            Self::PmdataOnly => "pmdata",
        }
    }
}

/// Book provenance quality threshold for replay filtering.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum BookQualityFilter {
    /// Accept direct live, direct PMData Yes/No, and validated complementary Down books.
    #[default]
    Any,
    /// Require directly observed book data on Up/Yes (rejects synthetic/unverified derivations).
    DirectYesRequired,
    /// Require directly observed books on BOTH Up/Yes and Down/No (rejects `ComplementaryFromYesL2`).
    DirectBothRequired,
}

impl BookQualityFilter {
    /// Stable CLI label.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Any => "any",
            Self::DirectYesRequired => "direct-yes",
            Self::DirectBothRequired => "direct-both",
        }
    }
}

/// Supported PMData Polymarket dataset channels.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PmdataDataset {
    /// Incremental order-book snapshots, price changes, tick-size changes, and resolutions.
    L2,
    /// WebSocket `last_trade_price` executions.
    Trades,
    /// Polygon on-chain settlement fills (`maker`, `taker`, `transactionHash`).
    OnchainFills,
}

impl PmdataDataset {
    /// URL path segment expected by `api.pmdata.dev/v1/polymarket/{data_type}/...`.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::L2 => "l2",
            Self::Trades => "trades",
            Self::OnchainFills => "onchain_fills",
        }
    }
}

/// Supported PMData L2 outcome query parameter (`Yes` default vs `?outcome=No`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum PmdataL2Outcome {
    /// Default Yes/Up book (no query parameter required).
    Yes,
    /// Explicit `?outcome=No` book.
    No,
}

impl PmdataL2Outcome {
    /// Stable lowercase identifier for cache filenames and manifest keys.
    #[must_use]
    pub const fn as_slug_suffix(self) -> &'static str {
        match self {
            Self::Yes => "yes",
            Self::No => "no",
        }
    }

    /// Optional query parameter value sent to PMData API (`None` for default `Yes`).
    #[must_use]
    pub const fn query_param_value(self) -> Option<&'static str> {
        match self {
            Self::Yes => None,
            Self::No => Some("No"),
        }
    }
}

/// Supported UpDown intervals for BTC historical ingestion.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum UpDownInterval {
    /// 5-minute markets (`300s` horizon, 288 markets/day, 72 quota per Day Bundle).
    M5,
    /// 15-minute markets (`900s` horizon, 96 markets/day, 24 quota per Day Bundle).
    M15,
    /// 1-hour markets (`3600s` horizon, 24 markets/day, 6 quota per Day Bundle).
    H1,
}

impl UpDownInterval {
    /// Wire interval token used in PMData Day Bundles and market slugs.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::M5 => "5m",
            Self::M15 => "15m",
            Self::H1 => "1h",
        }
    }

    /// Nominal market duration in seconds.
    #[must_use]
    pub const fn duration_secs(self) -> u64 {
        match self {
            Self::M5 => 300,
            Self::M15 => 900,
            Self::H1 => 3_600,
        }
    }

    /// Nominal market duration in milliseconds.
    #[must_use]
    pub const fn duration_ms(self) -> u64 {
        self.duration_secs() * 1_000
    }

    /// Official PMData Day Bundle quota units charged per archive (`-75%` vs individual markets).
    #[must_use]
    pub const fn day_bundle_quota_units(self) -> u32 {
        match self {
            Self::M5 => 72,
            Self::M15 => 24,
            Self::H1 => 6,
        }
    }

    /// Expected catalog `series` tag for `btc` (`updown.{interval}.btc`).
    #[must_use]
    pub fn btc_series_tag(self) -> String {
        format!("updown.{}.btc", self.as_str())
    }

    /// Parses `"5m"`, `"15m"`, or `"1h"` case-insensitively.
    pub fn parse(raw: &str) -> Result<Self, HistoricalError> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "5m" => Ok(Self::M5),
            "15m" => Ok(Self::M15),
            "1h" => Ok(Self::H1),
            other => Err(HistoricalError::Config {
                key: "interval",
                detail: format!("unsupported UpDown interval `{other}`; expected 5m, 15m, or 1h"),
            }),
        }
    }
}

/// Validated PMData client and cache configuration with redacted API credentials.
#[derive(Clone)]
pub struct PmdataConfig {
    /// Optional `PMDATA_API_KEY` secret (required for authenticated downloads; not needed for catalog/dry-run).
    pub api_key: Option<SecretString>,
    /// Base URL for PMData v1 API (`https://api.pmdata.dev/v1`).
    pub base_url: String,
    /// Public catalog Parquet URL (`https://public-data.pmdata.dev/v1/polymarket/markets.parquet`).
    pub catalog_url: String,
    /// Local directory storing downloaded ZIPs, extracted Parquet files, and catalog.
    pub cache_dir: PathBuf,
    /// Persistent download manifest path tracking every request and quota unit.
    pub manifest_path: PathBuf,
    /// Hard quota ceiling (`1..=650`).
    pub quota_cap: u32,
    /// Maximum pre-send / transport connection retries (never retries ambiguous post-send failures).
    pub max_retries: u32,
    /// HTTP request timeout in seconds.
    pub request_timeout_secs: u64,
}

impl Default for PmdataConfig {
    fn default() -> Self {
        Self {
            api_key: None,
            base_url: DEFAULT_PMDATA_BASE_URL.to_string(),
            catalog_url: DEFAULT_PMDATA_CATALOG_URL.to_string(),
            cache_dir: PathBuf::from(DEFAULT_PMDATA_CACHE_DIR),
            manifest_path: PathBuf::from(DEFAULT_PMDATA_MANIFEST_PATH),
            quota_cap: HARD_MAX_QUOTA_CAP,
            max_retries: 2,
            request_timeout_secs: 90,
        }
    }
}

impl fmt::Debug for PmdataConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let redacted = match &self.api_key {
            Some(s) if !s.expose_secret().trim().is_empty() => "[REDACTED]",
            _ => "<unset>",
        };
        f.debug_struct("PmdataConfig")
            .field("api_key", &redacted)
            .field("base_url", &self.base_url)
            .field("catalog_url", &self.catalog_url)
            .field("cache_dir", &self.cache_dir)
            .field("manifest_path", &self.manifest_path)
            .field("quota_cap", &self.quota_cap)
            .field("max_retries", &self.max_retries)
            .field("request_timeout_secs", &self.request_timeout_secs)
            .finish()
    }
}

impl PmdataConfig {
    /// Loads `.env` if present and builds `PmdataConfig` from environment variables.
    pub fn from_env() -> Result<Self, HistoricalError> {
        match dotenvy::dotenv() {
            Ok(_) => {}
            Err(err) if err.not_found() => {}
            Err(err) => {
                let sanitized = ConfigError::from_dotenvy(&err);
                return Err(HistoricalError::Config {
                    key: ".env",
                    detail: sanitized.to_string(),
                });
            }
        }
        Self::from_lookup(|key| match std::env::var(key) {
            Ok(v) => Ok(Some(v)),
            Err(std::env::VarError::NotPresent) => Ok(None),
            Err(std::env::VarError::NotUnicode(_)) => Err(HistoricalError::Config {
                key,
                detail: "environment variable contains invalid UTF-8".to_string(),
            }),
        })
    }

    /// Loads `PmdataConfig` from a custom key-lookup function (deterministic for tests).
    pub fn from_lookup<F>(lookup: F) -> Result<Self, HistoricalError>
    where
        F: Fn(&'static str) -> Result<Option<String>, HistoricalError>,
    {
        let mut cfg = Self::default();
        if let Some(key) = non_empty(&lookup, "PMDATA_API_KEY")? {
            cfg.api_key = Some(SecretString::from(key));
        }
        if let Some(url) = non_empty(&lookup, "PMDATA_BASE_URL")? {
            cfg.base_url = trim_trailing_slash(url);
        }
        if let Some(url) = non_empty(&lookup, "PMDATA_CATALOG_URL")? {
            cfg.catalog_url = url;
        }
        if let Some(dir) = non_empty(&lookup, "PMDATA_CACHE_DIR")? {
            cfg.cache_dir = PathBuf::from(dir);
        }
        if let Some(path) = non_empty(&lookup, "PMDATA_MANIFEST_PATH")? {
            cfg.manifest_path = PathBuf::from(path);
        }
        if let Some(cap_raw) = non_empty(&lookup, "PMDATA_QUOTA_CAP")? {
            let parsed = cap_raw
                .parse::<u32>()
                .map_err(|_| HistoricalError::Config {
                    key: "PMDATA_QUOTA_CAP",
                    detail: format!("invalid integer `{cap_raw}`"),
                })?;
            cfg.quota_cap = parsed;
        }
        cfg.validate()?;
        Ok(cfg)
    }

    /// Validates all non-secret invariants including hard `650` quota ceiling.
    pub fn validate(&self) -> Result<(), HistoricalError> {
        if self.quota_cap == 0 || self.quota_cap > HARD_MAX_QUOTA_CAP {
            return Err(HistoricalError::Config {
                key: "PMDATA_QUOTA_CAP",
                detail: format!(
                    "quota_cap ({}) must be in 1..={HARD_MAX_QUOTA_CAP}",
                    self.quota_cap
                ),
            });
        }
        if self.base_url.trim().is_empty() {
            return Err(HistoricalError::Config {
                key: "PMDATA_BASE_URL",
                detail: "base_url must be non-empty".to_string(),
            });
        }
        if self.catalog_url.trim().is_empty() {
            return Err(HistoricalError::Config {
                key: "PMDATA_CATALOG_URL",
                detail: "catalog_url must be non-empty".to_string(),
            });
        }
        if self.request_timeout_secs == 0 {
            return Err(HistoricalError::Config {
                key: "request_timeout_secs",
                detail: "request_timeout_secs must be > 0".to_string(),
            });
        }
        Ok(())
    }

    /// Returns the exposed API key secret only when explicitly required for HTTP auth headers.
    pub(crate) fn require_api_key(&self) -> Result<&str, HistoricalError> {
        match &self.api_key {
            Some(secret) if !secret.expose_secret().trim().is_empty() => {
                Ok(secret.expose_secret().trim())
            }
            _ => Err(HistoricalError::Config {
                key: "PMDATA_API_KEY",
                detail: "PMDATA_API_KEY is required for authenticated PMData downloads".to_string(),
            }),
        }
    }
}

fn non_empty<F>(lookup: &F, key: &'static str) -> Result<Option<String>, HistoricalError>
where
    F: Fn(&'static str) -> Result<Option<String>, HistoricalError>,
{
    Ok(lookup(key)?.and_then(|s| {
        let trimmed = s.trim();
        (!trimmed.is_empty()).then(|| trimmed.to_string())
    }))
}

fn trim_trailing_slash(mut s: String) -> String {
    while s.ends_with('/') {
        s.pop();
    }
    s
}
