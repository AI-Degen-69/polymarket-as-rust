//! Quota-safe PMData historical downloader, Parquet reader, provenance-aware
//! canonical normalizer, and live-vs-PMData reconciliation engine.

#![forbid(unsafe_code)]
#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::indexing_slicing
    )
)]

pub mod canonical;
pub mod catalog;
pub mod config;
pub mod downloader;
pub mod error;
pub mod manifest;
pub mod normalizer;
pub mod parquet_reader;
pub mod reconcile;

pub use canonical::{
    BookDerivation, CANONICAL_SCHEMA_VERSION, CanonicalEvent, CanonicalEventKind,
    CanonicalTimestamp, ClockSyncMeta, DataSource, EventProvenance, QualityFlag, ResolutionSource,
    TimingEstimate, TimingQuality, canonical_event_matches_filters,
    canonical_metadata_to_descriptor, is_canonical_jsonl_line,
};
pub use catalog::{
    OutcomeMapping, catalog_available_dates, filter_updown_markets, infer_interval_from_slug,
    parse_slug_start_secs, parse_utc_date,
};
pub use config::{
    BookQualityFilter, DEFAULT_PMDATA_BASE_URL, DEFAULT_PMDATA_CACHE_DIR,
    DEFAULT_PMDATA_CATALOG_URL, DEFAULT_PMDATA_MANIFEST_PATH, HARD_MAX_QUOTA_CAP, IngestionMode,
    PmdataConfig, PmdataDataset, PmdataL2Outcome, SourceFilter, UpDownInterval,
};
pub use downloader::{
    DownloadOutcome, DownloadPlanItem, DownloadPlanSummary, PmdataClient, bundle_entry_key,
    market_entry_key, verify_and_extract_zip,
};
pub use error::HistoricalError;
pub use manifest::{
    EndpointKind, EntryStatus, ManifestEntry, ManifestStore, QuotaManifest, ReserveMetadata,
    ReserveOutcome,
};
pub use normalizer::{
    NormalizationSummary, NormalizedMarketStream, PmdataMarketFiles, convert_price_to_micro,
    convert_size_to_micro, historical_fee_schedule, normalize_live_jsonl, normalize_pmdata_market,
    read_canonical_jsonl, verify_reconstructed_books_for_stream, write_canonical_jsonl,
};
pub use parquet_reader::{
    CatalogEntry, ParquetSummary, RawPmdataL2Row, RawPmdataOnchainFillRow, RawPmdataTradeRow,
    read_catalog_parquet, read_catalog_parquet_filtered, read_l2_parquet,
    read_onchain_fills_parquet, read_trades_parquet, verify_parquet_readable,
};
pub use reconcile::{
    DistributionStats, FidelityVerdict, HistoricalMarketAudit, HistoricalPhase0Report,
    LiveVsPmdataReport, MarketReconcileResult, audit_pmdata_historical_market,
    reconcile_live_vs_pmdata, render_historical_phase0_markdown, render_reconcile_markdown,
};
