use std::time::Duration;

use core_types::{
    BookLevel, ClockOffsetEstimator, FeeSchedule, MarketDescriptor, MicroUsdc, MonotonicClock,
    ProtocolVersion, SharesMicro,
};
use serde::{Deserialize, Serialize};

use crate::error::{FeedError, sanitize_snippet};
use crate::parse::{
    de_opt_decimal_as_string, de_opt_string_or_u64, de_string_vec_or_json_str,
    parse_decimal_micro_flexible, parse_outcome_label, parse_rfc3339_ms,
};

/// Default Polymarket Gamma REST API base URL.
pub const DEFAULT_GAMMA_URL: &str = "https://gamma-api.polymarket.com";
/// Default Polymarket CLOB REST API base URL.
pub const DEFAULT_CLOB_URL: &str = "https://clob.polymarket.com";
/// Default Binance Spot REST API base URL for ms-precision clock sync.
pub const DEFAULT_BINANCE_REST_URL: &str = "https://api.binance.com";

const FIVE_MIN_SEC: u64 = 300;
const FIVE_MIN_MS: u64 = 300_000;
const DISCOVERY_HORIZON_MS: u64 = 360_000;
const DEFAULT_TAKER_DELAY_MS: u64 = 50;

/// Diagnostic report produced by initial or periodic clock offset synchronization.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClockSyncReport {
    /// Primary time source used for `ClockOffsetEstimator` (`binance_rest_ms` or `clob_time_sec_fallback`).
    pub source: String,
    /// Local wall-clock midpoint timestamp in Unix milliseconds.
    pub local_unix_ms: u64,
    /// Exchange timestamp from primary source in Unix milliseconds.
    pub exchange_unix_ms: u64,
    /// Estimated signed offset (`exchange - local`) recorded into `ClockOffsetEstimator`.
    pub offset_ms: i64,
    /// Signed offset against CLOB `GET /time` (1s granularity) if reachable.
    pub clob_offset_ms: Option<i64>,
    /// Signed offset against Binance `GET /api/v3/time` (1ms granularity) if reachable.
    pub binance_offset_ms: Option<i64>,
    /// Error message when Binance `/api/v3/time` failed during fallback.
    pub binance_error: Option<String>,
    /// Error message when CLOB `/time` failed.
    pub clob_error: Option<String>,
    /// `true` if `|offset_ms| > drift_threshold_ms`.
    pub drift_exceeded: bool,
}

/// Asynchronous Gamma + CLOB + Binance REST discovery and snapshot client.
#[derive(Debug, Clone)]
pub struct GammaDiscoveryClient {
    http: reqwest::Client,
    gamma_base_url: String,
    clob_base_url: String,
    binance_rest_base_url: String,
}

impl GammaDiscoveryClient {
    /// Creates a discovery client targeting production Polymarket and Binance endpoints.
    pub fn new() -> Result<Self, FeedError> {
        Self::with_base_urls(
            DEFAULT_GAMMA_URL,
            DEFAULT_CLOB_URL,
            DEFAULT_BINANCE_REST_URL,
        )
    }

    /// Creates a discovery client with explicit base URLs (supports local mock HTTP servers in tests).
    pub fn with_base_urls(
        gamma_base_url: impl Into<String>,
        clob_base_url: impl Into<String>,
        binance_rest_base_url: impl Into<String>,
    ) -> Result<Self, FeedError> {
        crate::install_tls_provider();
        let http = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(3))
            .timeout(Duration::from_secs(6))
            .tcp_nodelay(true)
            .user_agent("poly-as/0.1.0")
            .build()
            .map_err(|source| FeedError::Http {
                endpoint: "reqwest::Client::builder".to_string(),
                source,
            })?;
        Ok(Self {
            http,
            gamma_base_url: trim_trailing_slash(gamma_base_url.into()),
            clob_base_url: trim_trailing_slash(clob_base_url.into()),
            binance_rest_base_url: trim_trailing_slash(binance_rest_base_url.into()),
        })
    }

    /// Returns configured CLOB base URL.
    #[must_use]
    pub fn clob_base_url(&self) -> &str {
        &self.clob_base_url
    }

    /// Syncs `estimator` against Binance `GET /api/v3/time` (1ms precision) and checks CLOB `GET /time`.
    pub async fn sync_clock(
        &self,
        estimator: &ClockOffsetEstimator,
    ) -> Result<ClockSyncReport, FeedError> {
        let (binance_res, clob_res) =
            tokio::join!(self.measure_binance_time(), self.measure_clob_time());

        let clob_offset_ms = clob_res
            .as_ref()
            .ok()
            .and_then(|(local, exch)| i64::try_from(i128::from(*exch) - i128::from(*local)).ok());
        let binance_offset_ms = binance_res
            .as_ref()
            .ok()
            .and_then(|(local, exch)| i64::try_from(i128::from(*exch) - i128::from(*local)).ok());
        let binance_error = binance_res.as_ref().err().map(ToString::to_string);
        let clob_error = clob_res.as_ref().err().map(ToString::to_string);

        let (source, local_unix_ms, exchange_unix_ms) = match (binance_res, clob_res) {
            (Ok((l, e)), _) => ("binance_rest_ms".to_string(), l, e),
            (Err(_), Ok((l, e))) => ("clob_time_sec_fallback".to_string(), l, e),
            (Err(b_err), Err(c_err)) => {
                return Err(FeedError::ClockSync {
                    detail: format!("both Binance ({b_err}) and CLOB ({c_err}) /time calls failed"),
                });
            }
        };

        let offset_ms = estimator.record_sample(local_unix_ms, exchange_unix_ms)?;
        Ok(ClockSyncReport {
            source,
            local_unix_ms,
            exchange_unix_ms,
            offset_ms,
            clob_offset_ms,
            binance_offset_ms,
            binance_error,
            clob_error,
            drift_exceeded: estimator.is_drift_exceeded(),
        })
    }

    /// Discovers the active 5-minute BTC Up/Down market expiring in `(now_ms + min_remaining_ms, now_ms + 360s]`.
    pub async fn discover_active_5m_btc(
        &self,
        now_ms: u64,
        min_remaining_ms: u64,
    ) -> Result<MarketDescriptor, FeedError> {
        let (slug_hit, slug_err) = self.probe_computed_slugs(now_ms, min_remaining_ms).await;
        if let Some(market) = slug_hit {
            return self.hydrate_descriptor(market).await;
        }
        if let Some(market) = self
            .search_paginated_events(now_ms, min_remaining_ms)
            .await?
        {
            return self.hydrate_descriptor(market).await;
        }
        let err_note = slug_err.map_or_else(String::new, |e| format!(" (slug_probe_err={e})"));
        Err(FeedError::DiscoveryEmpty {
            detail: format!(
                "checked deterministic 5m BTC slugs and paginated Gamma /events around {now_ms}ms{err_note}"
            ),
        })
    }

    /// Fetches and normalizes a fresh REST L2 orderbook snapshot (`GET /book?token_id=...`).
    pub async fn fetch_rest_book(
        &self,
        token_id: &str,
    ) -> Result<(Vec<BookLevel>, Vec<BookLevel>, u64), FeedError> {
        let url = format!("{}/book?token_id={}", self.clob_base_url, token_id.trim());
        let book: ClobBookResponse = self.get_json(&url, "clob_book").await?;
        parse_clob_book_levels(&book)
    }

    async fn probe_computed_slugs(
        &self,
        now_ms: u64,
        min_remaining_ms: u64,
    ) -> (Option<GammaMarketDto>, Option<String>) {
        let now_sec = now_ms / 1_000;
        let current_slot = (now_sec / FIVE_MIN_SEC) * FIVE_MIN_SEC;
        let candidate_slots = [current_slot, current_slot.saturating_add(FIVE_MIN_SEC)];
        let mut last_err = None;

        for slot in candidate_slots {
            let url = format!("{}/events?slug=btc-updown-5m-{slot}", self.gamma_base_url);
            match self
                .get_json::<Vec<GammaEventDto>>(&url, "gamma_events_by_slug")
                .await
            {
                Ok(events) => {
                    if let Some(m) = select_matching_market(&events, now_ms, min_remaining_ms) {
                        return (Some(m), last_err);
                    }
                }
                Err(err) => {
                    last_err = Some(err.to_string());
                }
            }
        }
        (None, last_err)
    }

    async fn search_paginated_events(
        &self,
        now_ms: u64,
        min_remaining_ms: u64,
    ) -> Result<Option<GammaMarketDto>, FeedError> {
        const PAGE_SIZE: usize = 50;
        let mut best: Option<(u64, GammaMarketDto)> = None;
        for page in 0..5_usize {
            let offset = page * PAGE_SIZE;
            let url = format!(
                "{}/events?active=true&closed=false&tag_slug=5m&order=endDate&ascending=true&limit={PAGE_SIZE}&offset={offset}",
                self.gamma_base_url
            );
            let events: Vec<GammaEventDto> = self.get_json(&url, "gamma_events_paginated").await?;
            if events.is_empty() {
                break;
            }
            if let Some(m) = select_matching_market(&events, now_ms, min_remaining_ms) {
                let exp = m
                    .end_date
                    .as_deref()
                    .and_then(|s| parse_rfc3339_ms(s).ok())
                    .unwrap_or(u64::MAX);
                if best.as_ref().is_none_or(|(b_exp, _)| exp < *b_exp) {
                    best = Some((exp, m));
                }
            }
            if events.len() < PAGE_SIZE {
                break;
            }
        }
        Ok(best.map(|(_, m)| m))
    }

    async fn hydrate_descriptor(&self, m: GammaMarketDto) -> Result<MarketDescriptor, FeedError> {
        let cm_url = format!("{}/clob-markets/{}", self.clob_base_url, m.condition_id);
        let cm: ClobMarketInfoResponse = self.get_json(&cm_url, "clob_market_info").await?;
        let pre_version = resolve_consistent_version(m.version.as_deref(), cm.v.as_deref(), None)?;
        let ver_hint = match pre_version {
            ProtocolVersion::CtfV1 => "v1",
            ProtocolVersion::PolyV2 => "v2",
        };
        let (up_id, dn_id) = extract_up_dn_tokens(&m, Some(ver_hint))?;
        let book_url = format!("{}/book?token_id={up_id}", self.clob_base_url);
        let book: ClobBookResponse = self.get_json(&book_url, "clob_book_hydrate").await?;
        let version = resolve_consistent_version(
            m.version.as_deref(),
            cm.v.as_deref(),
            book.version.as_deref(),
        )?;
        assemble_descriptor(&m, &book, &cm, version, up_id, dn_id)
    }

    async fn measure_binance_time(&self) -> Result<(u64, u64), FeedError> {
        let url = format!("{}/api/v3/time", self.binance_rest_base_url);
        let t0 = MonotonicClock::unix_now_ms()?;
        let body: BinanceTimeResponse = self.get_json(&url, "binance_time").await?;
        let t1 = MonotonicClock::unix_now_ms()?;
        Ok((midpoint_ms(t0, t1), body.server_time))
    }

    async fn measure_clob_time(&self) -> Result<(u64, u64), FeedError> {
        let url = format!("{}/time", self.clob_base_url);
        let t0 = MonotonicClock::unix_now_ms()?;
        let secs: serde_json::Value = self.get_json(&url, "clob_time").await?;
        let t1 = MonotonicClock::unix_now_ms()?;
        let unix_ms = parse_clob_time_value(&secs)?;
        Ok((midpoint_ms(t0, t1), unix_ms))
    }

    async fn get_json<T: for<'de> Deserialize<'de>>(
        &self,
        url: &str,
        context: &'static str,
    ) -> Result<T, FeedError> {
        let resp = self
            .http
            .get(url)
            .send()
            .await
            .map_err(|source| FeedError::Http {
                endpoint: url.to_string(),
                source,
            })?;
        let status = resp.status();
        let text = resp.text().await.map_err(|source| FeedError::Http {
            endpoint: url.to_string(),
            source,
        })?;
        if !status.is_success() {
            return Err(FeedError::HttpStatus {
                endpoint: url.to_string(),
                status: status.as_u16(),
                body_snippet: sanitize_snippet(&text),
            });
        }
        serde_json::from_str::<T>(&text).map_err(|source| FeedError::Json { context, source })
    }
}

/// Parses a Gamma market object (`v1` `clobTokenIds` or `v2` `positionIds`) strictly by protocol version.
pub fn parse_gamma_market_tokens(
    version: Option<&str>,
    outcomes: &[String],
    clob_token_ids: &[String],
    position_ids: &[String],
) -> Result<(String, String), FeedError> {
    let ids = select_versioned_token_ids(version, clob_token_ids, position_ids)?;
    if outcomes.len() != 2 || ids.len() != 2 {
        return Err(FeedError::InvalidMarket {
            detail: format!(
                "expected 2 outcomes and 2 token IDs, got outcomes={}, ids={}",
                outcomes.len(),
                ids.len()
            ),
        });
    }
    let (mut up_id, mut dn_id) = (None, None);
    for (label, id) in outcomes.iter().zip(ids.iter()) {
        match parse_outcome_label(label) {
            Some(core_types::Outcome::Up) => up_id = Some(id.trim().to_string()),
            Some(core_types::Outcome::Down) => dn_id = Some(id.trim().to_string()),
            None => {
                return Err(FeedError::InvalidMarket {
                    detail: format!("unrecognized outcome label `{label}`"),
                });
            }
        }
    }
    match (up_id, dn_id) {
        (Some(u), Some(d)) if !u.is_empty() && !d.is_empty() => Ok((u, d)),
        (Some(_), Some(_)) => Err(FeedError::InvalidMarket {
            detail: "market contains empty Up or Down token ID".to_string(),
        }),
        _ => Err(FeedError::InvalidMarket {
            detail: "market missing distinct Up/Yes and Down/No outcomes".to_string(),
        }),
    }
}

fn select_versioned_token_ids<'a>(
    version: Option<&str>,
    clob_token_ids: &'a [String],
    position_ids: &'a [String],
) -> Result<&'a [String], FeedError> {
    match version {
        Some(v) => match parse_explicit_version(v)? {
            ProtocolVersion::CtfV1 if !clob_token_ids.is_empty() => Ok(clob_token_ids),
            ProtocolVersion::PolyV2 if !position_ids.is_empty() => Ok(position_ids),
            ProtocolVersion::CtfV1 => Err(FeedError::InvalidMarket {
                detail: "v1 market missing clobTokenIds".to_string(),
            }),
            ProtocolVersion::PolyV2 => Err(FeedError::InvalidMarket {
                detail: "v2 market missing positionIds".to_string(),
            }),
        },
        None => match (clob_token_ids.is_empty(), position_ids.is_empty()) {
            (false, true) => Ok(clob_token_ids),
            (true, false) => Ok(position_ids),
            _ => Err(FeedError::InvalidMarket {
                detail: "omitted version requires unambiguous non-empty V1 or V2 token IDs"
                    .to_string(),
            }),
        },
    }
}

fn trim_trailing_slash(mut s: String) -> String {
    while s.ends_with('/') {
        s.pop();
    }
    s
}

fn midpoint_ms(t0: u64, t1: u64) -> u64 {
    t0.saturating_add(t1.saturating_sub(t0) / 2)
}

fn parse_clob_time_value(val: &serde_json::Value) -> Result<u64, FeedError> {
    const TWO_POW_64: f64 = 18_446_744_073_709_551_616.0;
    if let Some(s) = val.as_u64() {
        return s
            .checked_mul(1_000)
            .and_then(|m| m.checked_add(500))
            .ok_or_else(|| FeedError::InvalidMarket {
                detail: "clob /time overflow".to_string(),
            });
    }
    if let Some(f) = val.as_f64()
        && f.is_finite()
        && f >= 0.0
    {
        let ms = (f * 1_000.0).round();
        if (0.0..TWO_POW_64).contains(&ms) {
            return Ok(ms as u64);
        }
    }
    Err(FeedError::TimestampParse {
        raw: val.to_string(),
        detail: "expected Unix seconds integer from CLOB /time".to_string(),
    })
}

fn select_matching_market(
    events: &[GammaEventDto],
    now_ms: u64,
    min_remaining_ms: u64,
) -> Option<GammaMarketDto> {
    let mut best: Option<(u64, GammaMarketDto)> = None;
    let min_expiry = now_ms.saturating_add(min_remaining_ms);
    let max_expiry = now_ms.saturating_add(DISCOVERY_HORIZON_MS);

    for ev in events {
        for m in &ev.markets {
            if !is_active_5m_btc_market(m) {
                continue;
            }
            let Some(end_raw) = m.end_date.as_deref() else {
                continue;
            };
            let Ok(expiry_ms) = parse_rfc3339_ms(end_raw) else {
                continue;
            };
            if expiry_ms <= min_expiry || expiry_ms > max_expiry {
                continue;
            }
            if best
                .as_ref()
                .is_none_or(|(best_exp, _)| expiry_ms < *best_exp)
            {
                best = Some((expiry_ms, m.clone()));
            }
        }
    }
    best.map(|(_, m)| m)
}

fn is_active_5m_btc_market(m: &GammaMarketDto) -> bool {
    if m.closed == Some(true) || m.active == Some(false) || m.enable_order_book == Some(false) {
        return false;
    }
    let slug_lower = m.slug.to_ascii_lowercase();
    if slug_lower.starts_with("btc-updown-5m-") {
        return true;
    }
    let q_lower = m.question.to_ascii_lowercase();
    (slug_lower.contains("btc") || q_lower.contains("bitcoin"))
        && (slug_lower.contains("5m") || has_five_min_span(m))
}

fn has_five_min_span(m: &GammaMarketDto) -> bool {
    let (Some(start_raw), Some(end_raw)) = (m.event_start_time.as_deref(), m.end_date.as_deref())
    else {
        return false;
    };
    let (Ok(start_ms), Ok(end_ms)) = (parse_rfc3339_ms(start_raw), parse_rfc3339_ms(end_raw))
    else {
        return false;
    };
    let span_ms = end_ms.saturating_sub(start_ms);
    (240_000..=360_000).contains(&span_ms)
}

fn extract_up_dn_tokens(
    m: &GammaMarketDto,
    version_override: Option<&str>,
) -> Result<(String, String), FeedError> {
    parse_gamma_market_tokens(
        m.version.as_deref().or(version_override),
        &m.outcomes,
        &m.clob_token_ids,
        &m.position_ids,
    )
}

fn resolve_epoch_window_ms(m: &GammaMarketDto) -> Result<(u64, u64), FeedError> {
    let end_raw = m
        .end_date
        .as_deref()
        .ok_or_else(|| FeedError::InvalidMarket {
            detail: format!("market {} missing endDate", m.slug),
        })?;
    let expiry_ts_ms = parse_rfc3339_ms(end_raw)?;
    let start_ts_ms = match m.event_start_time.as_deref() {
        Some(raw) => parse_rfc3339_ms(raw)?,
        None => expiry_ts_ms.saturating_sub(FIVE_MIN_MS),
    };
    Ok((start_ts_ms, expiry_ts_ms))
}

fn assemble_descriptor(
    m: &GammaMarketDto,
    book: &ClobBookResponse,
    cm: &ClobMarketInfoResponse,
    version: ProtocolVersion,
    up_id: String,
    dn_id: String,
) -> Result<MarketDescriptor, FeedError> {
    let (start_ts_ms, expiry_ts_ms) = resolve_epoch_window_ms(m)?;
    let (tick_size_micro, min_order_size_micro) = parse_tick_and_min_size(m, book, cm)?;
    let fee_schedule = resolve_fee_schedule(m, cm)?;
    let neg_risk = book
        .neg_risk
        .or(m.neg_risk)
        .ok_or_else(|| FeedError::InvalidMarket {
            detail: format!(
                "market {} missing neg_risk flag from Gamma and CLOB",
                m.slug
            ),
        })?;
    let taker_delay_ms = cm.taker_delay_ms.unwrap_or(if cm.itode.unwrap_or(false) {
        DEFAULT_TAKER_DELAY_MS
    } else {
        0
    });

    let descriptor = MarketDescriptor {
        condition_id: m.condition_id.clone(),
        slug: m.slug.clone(),
        question: m.question.clone(),
        version,
        neg_risk,
        asset_id_up: up_id,
        asset_id_dn: dn_id,
        tick_size_micro,
        min_order_size_micro,
        taker_delay_ms,
        fee_schedule,
        start_ts_ms,
        expiry_ts_ms,
        strike_price_usd: None,
    };
    descriptor.validate()?;
    Ok(descriptor)
}

fn parse_tick_and_min_size(
    m: &GammaMarketDto,
    book: &ClobBookResponse,
    cm: &ClobMarketInfoResponse,
) -> Result<(MicroUsdc, SharesMicro), FeedError> {
    let tick_str = book
        .tick_size
        .as_deref()
        .or(cm.mts.as_deref())
        .or(m.order_price_min_tick_size.as_deref())
        .ok_or_else(|| FeedError::InvalidMarket {
            detail: format!("market {} missing tick_size", m.slug),
        })?;
    let tick_size_micro = MicroUsdc(
        parse_decimal_micro_flexible(tick_str.trim().as_bytes()).map_err(|source| {
            FeedError::ParseDecimal {
                field: "tick_size",
                source,
            }
        })?,
    );

    let min_str = book
        .min_order_size
        .as_deref()
        .or(cm.mos.as_deref())
        .or(m.order_min_size.as_deref())
        .ok_or_else(|| FeedError::InvalidMarket {
            detail: format!("market {} missing min_order_size", m.slug),
        })?;
    let min_order_size_micro = SharesMicro(
        parse_decimal_micro_flexible(min_str.trim().as_bytes()).map_err(|source| {
            FeedError::ParseDecimal {
                field: "min_order_size",
                source,
            }
        })?,
    );
    Ok((tick_size_micro, min_order_size_micro))
}

fn parse_explicit_version(raw: &str) -> Result<ProtocolVersion, FeedError> {
    let trimmed = raw.trim();
    if trimmed.eq_ignore_ascii_case("v1") {
        Ok(ProtocolVersion::CtfV1)
    } else if trimmed.eq_ignore_ascii_case("v2") {
        Ok(ProtocolVersion::PolyV2)
    } else {
        Err(FeedError::InvalidMarket {
            detail: format!("unsupported Polymarket protocol version `{trimmed}`"),
        })
    }
}

fn resolve_consistent_version(
    gamma_ver: Option<&str>,
    clob_ver: Option<&str>,
    book_ver: Option<&str>,
) -> Result<ProtocolVersion, FeedError> {
    let mut resolved: Option<ProtocolVersion> = None;
    for candidate in [clob_ver, book_ver, gamma_ver].into_iter().flatten() {
        let parsed = parse_explicit_version(candidate)?;
        if let Some(prev) = resolved
            && prev != parsed
        {
            return Err(FeedError::InvalidMarket {
                detail: format!("conflicting protocol versions ({prev:?} vs {parsed:?})"),
            });
        }
        resolved = Some(parsed);
    }
    resolved.ok_or_else(|| FeedError::InvalidMarket {
        detail: "missing protocol version (`v` / `version`) across Gamma and CLOB".to_string(),
    })
}

fn resolve_fee_schedule(
    m: &GammaMarketDto,
    cm: &ClobMarketInfoResponse,
) -> Result<FeeSchedule, FeedError> {
    if let Some(fd) = &cm.fd
        && let (Some(rate_str), Some(taker_only)) = (&fd.r, fd.to)
    {
        let rate_e6 =
            parse_decimal_micro_flexible(rate_str.trim().as_bytes()).map_err(|source| {
                FeedError::ParseDecimal {
                    field: "fd.r",
                    source,
                }
            })?;
        return Ok(FeeSchedule::new(rate_e6, taker_only)?);
    }
    if let Some(gs) = &m.fee_schedule
        && let (Some(rate_str), Some(taker_only)) = (&gs.rate, gs.taker_only)
    {
        let rate_e6 =
            parse_decimal_micro_flexible(rate_str.trim().as_bytes()).map_err(|source| {
                FeedError::ParseDecimal {
                    field: "feeSchedule.rate",
                    source,
                }
            })?;
        return Ok(FeeSchedule::new(rate_e6, taker_only)?);
    }
    Err(FeedError::InvalidMarket {
        detail: format!(
            "market {} missing fee curve parameters (`fd` / `feeSchedule`)",
            m.slug
        ),
    })
}

pub(crate) fn parse_clob_book_levels(
    book: &ClobBookResponse,
) -> Result<(Vec<BookLevel>, Vec<BookLevel>, u64), FeedError> {
    let ts_ms = book.timestamp.ok_or_else(|| FeedError::InvalidMarket {
        detail: "CLOB /book REST snapshot missing timestamp".to_string(),
    })?;
    let mut bids = Vec::with_capacity(book.bids.len());
    let mut asks = Vec::with_capacity(book.asks.len());
    for lvl in &book.bids {
        bids.push(parse_book_level(lvl)?);
    }
    for lvl in &book.asks {
        asks.push(parse_book_level(lvl)?);
    }
    Ok((bids, asks, ts_ms))
}

fn parse_book_level(lvl: &RawBookLevelDto) -> Result<BookLevel, FeedError> {
    let price = MicroUsdc(
        parse_decimal_micro_flexible(lvl.price.trim().as_bytes()).map_err(|source| {
            FeedError::ParseDecimal {
                field: "book_level.price",
                source,
            }
        })?,
    );
    let size = SharesMicro(
        parse_decimal_micro_flexible(lvl.size.trim().as_bytes()).map_err(|source| {
            FeedError::ParseDecimal {
                field: "book_level.size",
                source,
            }
        })?,
    );
    Ok(BookLevel { price, size })
}

#[derive(Debug, Deserialize)]
struct BinanceTimeResponse {
    #[serde(rename = "serverTime")]
    server_time: u64,
}

#[derive(Debug, Deserialize)]
struct GammaEventDto {
    #[serde(default)]
    markets: Vec<GammaMarketDto>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct GammaMarketDto {
    pub(crate) slug: String,
    #[serde(default)]
    pub(crate) question: String,
    pub(crate) condition_id: String,
    #[serde(default)]
    pub(crate) version: Option<String>,
    #[serde(default)]
    pub(crate) neg_risk: Option<bool>,
    #[serde(default, deserialize_with = "de_string_vec_or_json_str")]
    pub(crate) clob_token_ids: Vec<String>,
    #[serde(default, deserialize_with = "de_string_vec_or_json_str")]
    pub(crate) position_ids: Vec<String>,
    #[serde(default, deserialize_with = "de_string_vec_or_json_str")]
    pub(crate) outcomes: Vec<String>,
    #[serde(default)]
    pub(crate) end_date: Option<String>,
    #[serde(default)]
    pub(crate) event_start_time: Option<String>,
    #[serde(default, deserialize_with = "de_opt_decimal_as_string")]
    pub(crate) order_price_min_tick_size: Option<String>,
    #[serde(default, deserialize_with = "de_opt_decimal_as_string")]
    pub(crate) order_min_size: Option<String>,
    #[serde(default)]
    pub(crate) fee_schedule: Option<GammaFeeScheduleDto>,
    #[serde(default)]
    pub(crate) active: Option<bool>,
    #[serde(default)]
    pub(crate) closed: Option<bool>,
    #[serde(default)]
    pub(crate) enable_order_book: Option<bool>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct GammaFeeScheduleDto {
    #[serde(default, deserialize_with = "de_opt_decimal_as_string")]
    pub(crate) rate: Option<String>,
    #[serde(default)]
    pub(crate) taker_only: Option<bool>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct ClobBookResponse {
    #[serde(default, deserialize_with = "de_opt_string_or_u64")]
    pub(crate) timestamp: Option<u64>,
    #[serde(default)]
    pub(crate) tick_size: Option<String>,
    #[serde(default)]
    pub(crate) min_order_size: Option<String>,
    #[serde(default)]
    pub(crate) neg_risk: Option<bool>,
    #[serde(default)]
    pub(crate) version: Option<String>,
    #[serde(default)]
    pub(crate) bids: Vec<RawBookLevelDto>,
    #[serde(default)]
    pub(crate) asks: Vec<RawBookLevelDto>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct RawBookLevelDto {
    pub(crate) price: String,
    pub(crate) size: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct ClobMarketInfoResponse {
    #[serde(default, deserialize_with = "de_opt_decimal_as_string")]
    pub(crate) mos: Option<String>,
    #[serde(default, deserialize_with = "de_opt_decimal_as_string")]
    pub(crate) mts: Option<String>,
    #[serde(default)]
    pub(crate) itode: Option<bool>,
    #[serde(default, rename = "takerDelayMs")]
    pub(crate) taker_delay_ms: Option<u64>,
    #[serde(default)]
    pub(crate) fd: Option<ClobFeeCurveDto>,
    #[serde(default)]
    pub(crate) v: Option<String>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct ClobFeeCurveDto {
    #[serde(default, deserialize_with = "de_opt_decimal_as_string")]
    pub(crate) r: Option<String>,
    #[serde(default)]
    pub(crate) to: Option<bool>,
}
