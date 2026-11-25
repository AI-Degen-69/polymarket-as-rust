use core_types::{
    BookLevel, FeeSchedule, FinitePosF64, FixedBook, MarketDescriptor, MicroUsdc, OrderSide,
    Outcome, ProtocolVersion, SharesMicro,
};
use serde::{Deserialize, Serialize};

use crate::config::{BookQualityFilter, SourceFilter, UpDownInterval};
use crate::error::HistoricalError;

/// Canonical event schema version written to normalized JSONL files.
pub const CANONICAL_SCHEMA_VERSION: u16 = 1;

/// Origin feed/archive of a canonical event.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DataSource {
    /// Local live WebSocket JSONL recorder (`poly-as feeds --discover`).
    LiveJsonl,
    /// PMData historical Parquet / Day Bundle archive (`https://pmdata.dev`).
    Pmdata,
}

impl DataSource {
    /// Stable lowercase identifier.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::LiveJsonl => "live_jsonl",
            Self::Pmdata => "pmdata",
        }
    }
}

/// Precision and clock-synchronization quality of an event's timestamps.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TimingQuality {
    /// Live recorder QPC `mono_ns` plus verified Binance/CLOB clock sync offset.
    LiveMonotonicSynced,
    /// Live recorder QPC `mono_ns` without valid/in-tolerance clock sync offset.
    LiveMonotonicUnsynced,
    /// PMData WebSocket event carrying both exchange `timestamp` and AWS PHC `local_timestamp` (`us`).
    ExchangeAndProviderUtc,
    /// PMData event where only exchange `timestamp` is valid (`local_timestamp` absent/synthetic).
    ExchangeOnlyUtc,
    /// Polygon on-chain fill timestamped by block timestamp only (`us`).
    BlockTimestampOnly,
}

/// How an outcome's order book or delta was obtained.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BookDerivation {
    /// Directly observed from live Polymarket CLOB WebSocket / REST resync.
    DirectLiveWs,
    /// Directly observed from PMData `l2` default (`Yes`) stream.
    DirectPmdataYes,
    /// Directly observed from PMData `l2?outcome=No` stream.
    DirectPmdataNo,
    /// Derived from validated complementary Yes L2 (`1 - p`, Bid<->Ask, Buy<->Sell) in `quota-saving` mode.
    ComplementaryFromYesL2,
    /// Event does not represent an L2 book snapshot/delta (e.g. trade, metadata, spot tick).
    NotApplicable,
}

/// Authoritative source of a market's winning outcome resolution.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResolutionSource {
    /// Live WebSocket `market_resolved` health/status signal.
    LiveFeedHealth,
    /// PMData `l2` row with `event_type == "market_resolved"`.
    PmdataMarketResolved,
    /// Verified Polymarket Gamma API metadata.
    GammaApi,
    /// Explicit operator-supplied override (e.g. for `2026-08-07..2026-11-17` gap).
    ExternalOverride,
    /// Inferred from terminal CLOB book consensus (fallback when explicit resolution is absent).
    TerminalBookConsensus,
}

/// Explicit data-quality or gap flags attached per event for auditability and filtering.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QualityFlag {
    /// Trade lacked an exchange/on-chain `trade_id` (standard pre-`2026-10-12` in PMData).
    MissingTradeId,
    /// Market stream ended without an explicit `market_resolved` event.
    MissingMarketResolved,
    /// Down book/delta was derived from Yes L2 in `quota-saving` mode (`ComplementaryFromYesL2`).
    DerivedComplementaryDownBook,
    /// Stream has no Binance spot reference feed (`spot_usd`/`strike_usd` unavailable).
    MissingBinanceSpot,
    /// Duplicate trade row suppressed during normalization.
    DuplicateTradeFiltered,
    /// Converted price was not an exact multiple of market tick size.
    SubTickPrice,
    /// Converted share size fell below market `min_order_size_micro`.
    BelowMinOrderSize,
    /// Source book snapshot or delta produced a crossed state (`best_bid >= best_ask`).
    CrossedBookObserved,
    /// Market lacked explicit outcome metadata in catalog and required verified slug/fallback check.
    MissingOutcomeMetadata,
    /// Replay order timestamp was derived from exchange/provider UTC because `mono_ns` is absent.
    EstimatedReplayClock,
    /// Floating-point source value required rounding beyond 1e-6 micro-units.
    FloatConversionRounding,
    /// PMData `00:00 UTC` synthetic boundary `book_snapshot` (`timestamp == local_timestamp == 00:00 UTC`).
    DailyBoundarySnapshot,
}

/// Clock-offset snapshot preserved exclusively from live JSONL captures.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClockSyncMeta {
    pub source: String,
    pub offset_ms: i64,
    pub clob_offset_ms: Option<i64>,
    pub binance_offset_ms: Option<i64>,
    pub drift_exceeded: bool,
}

/// Explicit documentation of any derived replay timing when `mono_ns` is absent.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TimingEstimate {
    /// Derivation rule (e.g., `"exchange_ts_us_to_ms_floor"` or `"provider_local_ts_us_to_ms_floor"`).
    pub method: String,
    /// Explicit warning describing timing limitations relative to local monotonic QPC.
    pub uncertainty_note: String,
}

/// Multi-clock timestamp bundle keeping exchange, provider, local wall, and QPC monotonic clocks separate.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CanonicalTimestamp {
    /// Exchange-reported Unix timestamp in milliseconds (`None` if unavailable).
    pub exchange_ts_ms: Option<u64>,
    /// Exchange-reported Unix timestamp in microseconds (preserved at native PMData precision).
    pub exchange_ts_us: Option<i64>,
    /// Provider collector receive timestamp in UTC microseconds (`PMData.local_timestamp`, NEVER equated to `mono_ns`).
    pub provider_local_ts_us: Option<i64>,
    /// Local recorder wall-clock timestamp in Unix milliseconds (`live_jsonl` only; `None` for PMData).
    pub unix_ts_ms: Option<u64>,
    /// Local recorder QPC monotonic nanoseconds (`live_jsonl` only; ALWAYS `None` for PMData).
    pub mono_ns: Option<u64>,
    /// Local recorder exchange clock-offset metadata (`live_jsonl` only; ALWAYS `None` for PMData).
    pub clock_sync: Option<ClockSyncMeta>,
    /// Causal replay ordering timestamp in milliseconds used by unified replay adapters.
    pub replay_order_ts_ms: u64,
    /// Explicit estimation disclosure whenever `mono_ns` is `None`.
    pub timing_estimation: Option<TimingEstimate>,
}

/// Source provenance and raw audit metadata attached to every canonical event.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EventProvenance {
    /// Origin (`live_jsonl` or `pmdata`).
    pub source: DataSource,
    /// Original event name in source stream (e.g. `"poly_book"`, `"book"`, `"price_change"`, `"last_trade_price"`).
    pub original_event_type: String,
    /// Market slug (`btc-updown-5m-...` or `btc-updown-15m-...`).
    pub market_slug: String,
    /// Timestamp quality classification.
    pub timing_quality: TimingQuality,
    /// How book/price levels were obtained.
    pub book_derivation: BookDerivation,
    /// Canonical normalizer version tag.
    pub normalization_version: u16,
    /// Preserved original raw floating-point price(s) when sourced from float Parquet columns.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub raw_price_f64: Option<f64>,
    /// Preserved original raw floating-point size when sourced from float Parquet columns.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub raw_size_f64: Option<f64>,
    /// Explicit quality/gap flags for this event.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub quality_flags: Vec<QualityFlag>,
}

/// Canonical normalized event envelope consumed by unified replay and reconciliation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CanonicalEvent {
    /// Schema version (`1`).
    pub schema_version: u16,
    /// Provenance & quality metadata.
    pub provenance: EventProvenance,
    /// Separated clock fields (never conflating provider UTC with local `mono_ns`).
    pub timestamp: CanonicalTimestamp,
    /// Typed market-data payload.
    pub kind: CanonicalEventKind,
}

/// Canonical typed event variants distinguishing trades, quotes/book deltas, snapshots, metadata, and settlements.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum CanonicalEventKind {
    /// Static & dynamic market contract metadata (supports both 5m=300s and 15m=900s).
    MarketMetadata {
        condition_id: Option<String>,
        slug: String,
        question: Option<String>,
        interval: UpDownInterval,
        version: Option<ProtocolVersion>,
        neg_risk: Option<bool>,
        asset_id_up: Option<String>,
        asset_id_dn: Option<String>,
        yes_outcome_label: Option<String>,
        no_outcome_label: Option<String>,
        tick_size_micro: MicroUsdc,
        min_order_size_micro: SharesMicro,
        taker_delay_ms: Option<u64>,
        fee_schedule: FeeSchedule,
        start_ts_ms: u64,
        expiry_ts_ms: u64,
        strike_price_usd: Option<f64>,
    },
    /// Live recorder clock-sync event (`live_jsonl` only).
    ClockSync { report: ClockSyncMeta },
    /// Full L2 order book snapshot (preserving arbitrary depth; backtester takes top 10).
    BookSnapshot {
        outcome: Outcome,
        bids: Vec<BookLevel>,
        asks: Vec<BookLevel>,
    },
    /// Incremental single-level quote/book update (NEVER confused with an executed trade).
    PriceChange {
        outcome: Outcome,
        side: OrderSide,
        price: MicroUsdc,
        size: SharesMicro,
        best_bid: Option<MicroUsdc>,
        best_ask: Option<MicroUsdc>,
    },
    /// Executed public trade print (`last_trade_price` / `poly_trade`).
    TradePrint {
        outcome: Outcome,
        side: OrderSide,
        price: MicroUsdc,
        size: SharesMicro,
        trade_id: Option<String>,
    },
    /// Tick-size transition event.
    TickSizeChange {
        outcome: Option<Outcome>,
        new_tick_micro: MicroUsdc,
    },
    /// Best bid/ask summary update (`live_jsonl` `poly_bba`).
    BestBidAsk {
        outcome: Outcome,
        best_bid: Option<MicroUsdc>,
        best_ask: Option<MicroUsdc>,
    },
    /// Market resolution event specifying winning binary outcome and its source.
    MarketResolution {
        winning_outcome: Outcome,
        resolution_source: ResolutionSource,
        raw_winning_label: Option<String>,
    },
    /// Polygon on-chain fill event (kept distinct from WebSocket `TradePrint`).
    OnchainFill {
        outcome: Outcome,
        maker: String,
        taker: String,
        maker_side: OrderSide,
        taker_side: OrderSide,
        price: MicroUsdc,
        usd_amount_micro: MicroUsdc,
        token_amount_micro: SharesMicro,
        transaction_hash: String,
        log_index: Option<i64>,
    },
    /// Live Binance Spot reference tick (`live_jsonl` only).
    BinanceSpot { event: feeds::BinanceSpotEvent },
}

impl CanonicalEvent {
    /// Validates structural provenance invariants (e.g., PMData MUST NEVER carry `mono_ns` or `clock_sync`).
    pub fn validate_provenance_invariants(&self) -> Result<(), HistoricalError> {
        if self.provenance.source == DataSource::Pmdata {
            if self.timestamp.mono_ns.is_some() {
                return Err(HistoricalError::Normalization {
                    context: self.provenance.market_slug.clone(),
                    detail: "PMData canonical event must have mono_ns == None".to_string(),
                });
            }
            if self.timestamp.clock_sync.is_some() {
                return Err(HistoricalError::Normalization {
                    context: self.provenance.market_slug.clone(),
                    detail: "PMData canonical event must have clock_sync == None".to_string(),
                });
            }
            if self.timestamp.timing_estimation.is_none() {
                return Err(HistoricalError::Normalization {
                    context: self.provenance.market_slug.clone(),
                    detail: "PMData canonical event must disclose timing_estimation for replay_order_ts_ms".to_string(),
                });
            }
        }
        Ok(())
    }

    /// Converts a canonical `BookSnapshot` payload into `FixedBook<10>` (taking top 10 levels).
    pub fn to_fixed_book_10(
        outcome: Outcome,
        bids: &[BookLevel],
        asks: &[BookLevel],
        last_update_ns: u64,
        exchange_ts_ms: u64,
    ) -> Result<FixedBook<10>, core_types::MathError> {
        FixedBook::<10>::try_from_slices(outcome, bids, asks, last_update_ns, exchange_ts_ms)
    }
}

/// Returns `true` when the first non-empty JSONL line looks like a `CanonicalEvent` envelope
/// (`"schema_version"` and `"provenance"`), distinguishing it from legacy `RawJsonlEnvelope`.
#[must_use]
pub fn is_canonical_jsonl_line(line: &str) -> bool {
    let Ok(val) = serde_json::from_str::<serde_json::Value>(line) else {
        return false;
    };
    val.get("schema_version").is_some() && val.get("provenance").is_some()
}

/// Evaluates whether `ev` satisfies both `--source-filter` and `--min-book-quality`.
#[must_use]
pub fn canonical_event_matches_filters(
    ev: &CanonicalEvent,
    source_filter: SourceFilter,
    book_quality: BookQualityFilter,
) -> bool {
    let src_ok = match source_filter {
        SourceFilter::All => true,
        SourceFilter::LiveOnly => ev.provenance.source == DataSource::LiveJsonl,
        SourceFilter::PmdataOnly => ev.provenance.source == DataSource::Pmdata,
    };
    if !src_ok {
        return false;
    }

    let is_book_event = matches!(
        ev.kind,
        CanonicalEventKind::BookSnapshot { .. }
            | CanonicalEventKind::PriceChange { .. }
            | CanonicalEventKind::BestBidAsk { .. }
    );
    if !is_book_event {
        return true;
    }

    match book_quality {
        BookQualityFilter::Any => true,
        BookQualityFilter::DirectYesRequired => matches!(
            ev.provenance.book_derivation,
            BookDerivation::DirectLiveWs
                | BookDerivation::DirectPmdataYes
                | BookDerivation::DirectPmdataNo
                | BookDerivation::ComplementaryFromYesL2
        ),
        BookQualityFilter::DirectBothRequired => matches!(
            ev.provenance.book_derivation,
            BookDerivation::DirectLiveWs
                | BookDerivation::DirectPmdataYes
                | BookDerivation::DirectPmdataNo
        ),
    }
}

/// Builds a validated `MarketDescriptor` from `CanonicalEventKind::MarketMetadata` for replay
/// engines without fabricating canonical provenance (`condition_id`/`asset_id_*` fall back to
/// explicit placeholder strings only in the ephemeral simulator descriptor when catalog metadata
/// was absent).
pub fn canonical_metadata_to_descriptor(
    kind: &CanonicalEventKind,
) -> Result<(MarketDescriptor, UpDownInterval), HistoricalError> {
    let CanonicalEventKind::MarketMetadata {
        condition_id,
        slug,
        question,
        interval,
        version,
        neg_risk,
        asset_id_up,
        asset_id_dn,
        tick_size_micro,
        min_order_size_micro,
        taker_delay_ms,
        fee_schedule,
        start_ts_ms,
        expiry_ts_ms,
        strike_price_usd,
        ..
    } = kind
    else {
        return Err(HistoricalError::Normalization {
            context: "canonical_metadata_to_descriptor".to_string(),
            detail: "expected MarketMetadata canonical event".to_string(),
        });
    };

    let cid = condition_id
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map_or_else(|| format!("pmdata-missing-cid:{slug}"), str::to_string);
    let up_id = asset_id_up
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map_or_else(|| format!("pmdata-missing-up:{slug}"), str::to_string);
    let dn_id = asset_id_dn
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map_or_else(|| format!("pmdata-missing-dn:{slug}"), str::to_string);

    let desc = MarketDescriptor {
        condition_id: cid,
        slug: slug.clone(),
        question: question.clone().unwrap_or_else(|| format!("PMData {slug}")),
        version: version.unwrap_or(ProtocolVersion::CtfV1),
        neg_risk: neg_risk.unwrap_or(false),
        asset_id_up: up_id,
        asset_id_dn: dn_id,
        tick_size_micro: *tick_size_micro,
        min_order_size_micro: *min_order_size_micro,
        taker_delay_ms: taker_delay_ms.unwrap_or(50),
        fee_schedule: *fee_schedule,
        start_ts_ms: *start_ts_ms,
        expiry_ts_ms: *expiry_ts_ms,
        strike_price_usd: strike_price_usd.map(FinitePosF64::try_new).transpose()?,
    };
    desc.validate()
        .map_err(|err| HistoricalError::Normalization {
            context: slug.clone(),
            detail: err.to_string(),
        })?;
    Ok((desc, *interval))
}

/// Validates market-wide book-quality requirements across filtered replay events.
/// For `BookQualityFilter::DirectBothRequired`, every market in `events` must have
/// directly observed books (`DirectLiveWs` / `DirectPmdataYes` / `DirectPmdataNo`) for BOTH
/// `Outcome::Up` and `Outcome::Down`.
pub fn validate_canonical_replay_quality(
    events: &[CanonicalEvent],
    book_quality: BookQualityFilter,
) -> Result<(), HistoricalError> {
    if book_quality != BookQualityFilter::DirectBothRequired {
        return Ok(());
    }
    let mut seen_sides: std::collections::BTreeMap<&str, (bool, bool)> =
        std::collections::BTreeMap::new();
    for ev in events {
        let slug = ev.provenance.market_slug.as_str();
        let entry = seen_sides.entry(slug).or_insert((false, false));
        let is_direct = matches!(
            ev.provenance.book_derivation,
            BookDerivation::DirectLiveWs
                | BookDerivation::DirectPmdataYes
                | BookDerivation::DirectPmdataNo
        );
        if !is_direct {
            continue;
        }
        match &ev.kind {
            CanonicalEventKind::BookSnapshot { outcome, .. }
            | CanonicalEventKind::PriceChange { outcome, .. }
            | CanonicalEventKind::BestBidAsk { outcome, .. } => match outcome {
                Outcome::Up => entry.0 = true,
                Outcome::Down => entry.1 = true,
            },
            _ => {}
        }
    }
    for (slug, (has_up, has_dn)) in seen_sides {
        if !has_up || !has_dn {
            return Err(HistoricalError::Normalization {
                context: slug.to_string(),
                detail: format!(
                    "min-book-quality direct-both requires direct Up and Down books for market `{slug}` (has_up={has_up}, has_dn={has_dn})"
                ),
            });
        }
    }
    Ok(())
}
