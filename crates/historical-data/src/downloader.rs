use std::fs::{self, File, OpenOptions};
use std::io::{BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime};

use chrono::NaiveDate;
use reqwest::header::{CONTENT_RANGE, HeaderMap, HeaderValue, RANGE};
use sha2::{Digest, Sha256};
use zip::ZipArchive;

const MAX_ZIP_ENTRIES: usize = 10_000;
const MAX_ZIP_ENTRY_BYTES: u64 = 512 * 1024 * 1024;
const MAX_ZIP_TOTAL_BYTES: u64 = 2 * 1024 * 1024 * 1024;
static TEMP_SEQ: AtomicU64 = AtomicU64::new(1);

use crate::config::{IngestionMode, PmdataConfig, PmdataDataset, PmdataL2Outcome, UpDownInterval};
use crate::error::{HistoricalError, sanitize_snippet};
use crate::manifest::{
    EndpointKind, EntryStatus, ManifestEntry, ManifestStore, QuotaManifest, ReserveMetadata,
    ReserveOutcome, existing_valid_cache_entry,
};
use crate::parquet_reader::verify_parquet_readable;

/// Dry-run or planned download descriptor with zero network calls or quota side effects.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DownloadPlanItem {
    pub entry_key: String,
    pub endpoint_kind: EndpointKind,
    pub dataset: PmdataDataset,
    pub outcome: Option<PmdataL2Outcome>,
    pub asset: String,
    pub interval: String,
    pub utc_date: String,
    pub market_slug: Option<String>,
    pub url: String,
    pub cache_path: PathBuf,
    pub already_cached: bool,
    pub quota_cost: u32,
}

/// Summary returned after planning a multi-day or multi-market batch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DownloadPlanSummary {
    pub items: Vec<DownloadPlanItem>,
    pub total_quota_needed: u32,
    pub manifest_charged_before: u32,
    pub manifest_reserved_before: u32,
    pub quota_cap: u32,
    pub exceeds_quota: bool,
}

/// Outcome of a single download invocation (`dry-run`, `cache-hit`, `downloaded`, or `not-found`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DownloadOutcome {
    DryRun(DownloadPlanItem),
    CacheHit(ManifestEntry),
    Downloaded {
        entry: ManifestEntry,
        extracted_files: Vec<PathBuf>,
    },
    NotFound {
        entry: ManifestEntry,
    },
}

/// Quota-protected, idempotent PMData downloader backed by `ManifestStore`.
#[derive(Debug, Clone)]
pub struct PmdataClient {
    config: PmdataConfig,
    http: reqwest::Client,
    manifest: ManifestStore,
}

impl PmdataClient {
    /// Creates a new `PmdataClient` and initializes its persistent `ManifestStore`.
    pub fn new(config: PmdataConfig) -> Result<Self, HistoricalError> {
        config.validate()?;
        feeds::install_tls_provider();
        let http = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(10))
            .timeout(Duration::from_secs(config.request_timeout_secs))
            .redirect(reqwest::redirect::Policy::none())
            .tcp_nodelay(true)
            .user_agent("poly-as-historical/0.1.0")
            .build()
            .map_err(|err| HistoricalError::Http {
                endpoint: "reqwest::Client::builder".to_string(),
                detail: err.to_string(),
            })?;
        let manifest = ManifestStore::new(config.manifest_path.clone(), config.quota_cap)?;
        Ok(Self {
            config,
            http,
            manifest,
        })
    }

    /// Returns client configuration (with redacted `api_key` in `Debug`).
    #[must_use]
    pub const fn config(&self) -> &PmdataConfig {
        &self.config
    }

    /// Returns manifest store handle.
    #[must_use]
    pub const fn manifest(&self) -> &ManifestStore {
        &self.manifest
    }

    /// Loads current quota snapshot from persistent manifest.
    pub fn load_manifest(&self) -> Result<QuotaManifest, HistoricalError> {
        self.manifest.load()
    }

    /// Constructs a validated single-market URL (never embeds credentials; rejects `outcome` on non-`l2`).
    pub fn build_market_url(
        &self,
        dataset: PmdataDataset,
        date: NaiveDate,
        market_slug: &str,
        outcome: Option<PmdataL2Outcome>,
    ) -> Result<String, HistoricalError> {
        let slug = market_slug.trim();
        if slug.is_empty() || slug.contains('/') || slug.contains('\\') || slug.contains("..") {
            return Err(HistoricalError::Config {
                key: "market_slug",
                detail: format!("invalid market_slug `{market_slug}`"),
            });
        }
        validate_outcome_for_dataset(dataset, outcome)?;
        let date_path = date.format("%Y/%m/%d");
        let base = format!(
            "{}/polymarket/{}/{}/{}.parquet",
            self.config.base_url,
            dataset.as_str(),
            date_path,
            slug
        );
        Ok(append_outcome_query(base, outcome))
    }

    /// Constructs a validated Day Bundle ZIP URL (rejects `outcome` on non-`l2`).
    pub fn build_bundle_url(
        &self,
        dataset: PmdataDataset,
        date: NaiveDate,
        asset: &str,
        interval: UpDownInterval,
        outcome: Option<PmdataL2Outcome>,
    ) -> Result<String, HistoricalError> {
        let asset_lc = asset.trim().to_ascii_lowercase();
        if asset_lc.is_empty() || !asset_lc.chars().all(|c| c.is_ascii_alphanumeric()) {
            return Err(HistoricalError::Config {
                key: "asset",
                detail: format!("invalid UpDown asset `{asset}`"),
            });
        }
        validate_outcome_for_dataset(dataset, outcome)?;
        let date_path = date.format("%Y/%m/%d");
        let base = format!(
            "{}/polymarket/{}/{}/{}-{}.zip",
            self.config.base_url,
            dataset.as_str(),
            date_path,
            asset_lc,
            interval.as_str()
        );
        Ok(append_outcome_query(base, outcome))
    }

    /// Builds a dry-run/quota plan for a Day Bundle request set (`Full` vs `QuotaSaving`) without network calls.
    pub fn plan_day_bundles(
        &self,
        asset: &str,
        interval: UpDownInterval,
        dates: &[NaiveDate],
        mode: IngestionMode,
        include_onchain: bool,
    ) -> Result<DownloadPlanSummary, HistoricalError> {
        let manifest = self.manifest.load()?;
        let mut items = Vec::new();
        for &date in dates {
            items.push(self.build_bundle_plan_item(
                &manifest,
                PmdataDataset::L2,
                Some(PmdataL2Outcome::Yes),
                date,
                asset,
                interval,
            )?);
            if mode == IngestionMode::Full {
                items.push(self.build_bundle_plan_item(
                    &manifest,
                    PmdataDataset::L2,
                    Some(PmdataL2Outcome::No),
                    date,
                    asset,
                    interval,
                )?);
            }
            items.push(self.build_bundle_plan_item(
                &manifest,
                PmdataDataset::Trades,
                None,
                date,
                asset,
                interval,
            )?);
            if include_onchain {
                items.push(self.build_bundle_plan_item(
                    &manifest,
                    PmdataDataset::OnchainFills,
                    None,
                    date,
                    asset,
                    interval,
                )?);
            }
        }
        let total_quota_needed: u32 = items
            .iter()
            .filter(|i| !i.already_cached)
            .map(|i| i.quota_cost)
            .fold(0_u32, u32::saturating_add);
        let exceeds_quota = manifest
            .charged_units
            .saturating_add(manifest.reserved_units)
            .saturating_add(total_quota_needed)
            > manifest.quota_cap;
        Ok(DownloadPlanSummary {
            items,
            total_quota_needed,
            manifest_charged_before: manifest.charged_units,
            manifest_reserved_before: manifest.reserved_units,
            quota_cap: manifest.quota_cap,
            exceeds_quota,
        })
    }

    /// Downloads the free public Polymarket markets catalog (`0` quota, no API key required).
    pub async fn download_catalog(
        &self,
        force_refresh: bool,
        dry_run: bool,
    ) -> Result<DownloadOutcome, HistoricalError> {
        let entry_key = "catalog:polymarket:markets".to_string();
        let cache_path = self.catalog_cache_path();
        let cached_ok =
            !force_refresh && cache_path.is_file() && verify_parquet_readable(&cache_path).is_ok();
        let plan = DownloadPlanItem {
            entry_key: entry_key.clone(),
            endpoint_kind: EndpointKind::Catalog,
            dataset: PmdataDataset::L2,
            outcome: None,
            asset: "all".to_string(),
            interval: "catalog".to_string(),
            utc_date: chrono::Utc::now().format("%Y-%m-%d").to_string(),
            market_slug: None,
            url: self.config.catalog_url.clone(),
            cache_path: cache_path.clone(),
            already_cached: cached_ok,
            quota_cost: 0,
        };
        if dry_run {
            return Ok(DownloadOutcome::DryRun(plan));
        }
        if cached_ok && let Some(entry) = self.manifest.check_cached(&entry_key)? {
            return Ok(DownloadOutcome::CacheHit(entry));
        }
        if !force_refresh && let Some(entry) = self.manifest.check_cached(&entry_key)? {
            return Ok(DownloadOutcome::CacheHit(entry));
        }

        let meta = ReserveMetadata {
            entry_key: entry_key.clone(),
            endpoint_kind: EndpointKind::Catalog,
            dataset_type: "catalog".to_string(),
            outcome_param: None,
            asset: "all".to_string(),
            interval: "catalog".to_string(),
            utc_date: chrono::Utc::now().format("%Y-%m-%d").to_string(),
            market_slug: None,
            requested_url: self.config.catalog_url.clone(),
            quota_cost: 0,
        };
        if !force_refresh {
            if let ReserveOutcome::AlreadyCached(entry) = self.manifest.reserve(meta.clone())? {
                return Ok(DownloadOutcome::CacheHit(*entry));
            }
            let bytes = match self.fetch_public_bytes(&self.config.catalog_url).await {
                Ok((status, b)) => (status, b),
                Err(err) => {
                    if let Err(merr) = self.manifest.release_uncharged(
                        &entry_key,
                        0,
                        None,
                        EntryStatus::FailedUncharged,
                        err.to_string(),
                    ) {
                        return Err(HistoricalError::ManifestIo {
                            path: self.manifest.path().to_path_buf(),
                            detail: format!(
                                "catalog fetch failed ({err}) and manifest release failed: {merr}"
                            ),
                        });
                    }
                    return Err(err);
                }
            };
            let (size, sha) = write_verified_parquet_atomic(&cache_path, &bytes.1)?;
            let entry = self
                .manifest
                .commit_success(&entry_key, 0, bytes.0, size, sha, cache_path)?;
            return Ok(DownloadOutcome::Downloaded {
                entry,
                extracted_files: Vec::new(),
            });
        }

        // Force-refresh keeps any existing completed catalog entry intact until the replacement
        // is verified on disk.
        let bytes = self.fetch_public_bytes(&self.config.catalog_url).await?;
        let (size, sha) = write_verified_parquet_atomic(&cache_path, &bytes.1)?;
        let entry = self
            .manifest
            .upsert_completed(meta, bytes.0, size, sha, cache_path)?;
        Ok(DownloadOutcome::Downloaded {
            entry,
            extracted_files: Vec::new(),
        })
    }

    /// Downloads one individual market Parquet file (1 quota unit on `200 OK`, 0 if cached or `404`).
    pub async fn download_market_file(
        &self,
        dataset: PmdataDataset,
        date: NaiveDate,
        market_slug: &str,
        outcome: Option<PmdataL2Outcome>,
        dry_run: bool,
    ) -> Result<DownloadOutcome, HistoricalError> {
        let url = self.build_market_url(dataset, date, market_slug, outcome)?;
        let cache_path = self.market_cache_path(dataset, date, market_slug, outcome);
        let entry_key = market_entry_key(dataset, date, market_slug, outcome);
        let cached = self.manifest.check_cached(&entry_key)?;
        let plan = DownloadPlanItem {
            entry_key: entry_key.clone(),
            endpoint_kind: EndpointKind::SingleMarket,
            dataset,
            outcome,
            asset: "btc".to_string(),
            interval: infer_interval_label(market_slug),
            utc_date: date.format("%Y-%m-%d").to_string(),
            market_slug: Some(market_slug.to_string()),
            url: url.clone(),
            cache_path: cache_path.clone(),
            already_cached: cached.is_some(),
            quota_cost: 1,
        };
        if dry_run {
            return Ok(DownloadOutcome::DryRun(plan));
        }
        if let Some(entry) = cached {
            return Ok(DownloadOutcome::CacheHit(entry));
        }

        let api_key = self.config.require_api_key()?.to_string();
        let meta = ReserveMetadata {
            entry_key: entry_key.clone(),
            endpoint_kind: EndpointKind::SingleMarket,
            dataset_type: dataset.as_str().to_string(),
            outcome_param: outcome.and_then(|o| o.query_param_value().map(str::to_string)),
            asset: "btc".to_string(),
            interval: infer_interval_label(market_slug),
            utc_date: date.format("%Y-%m-%d").to_string(),
            market_slug: Some(market_slug.to_string()),
            requested_url: url.clone(),
            quota_cost: 1,
        };
        if let ReserveOutcome::AlreadyCached(entry) = self.manifest.reserve(meta)? {
            return Ok(DownloadOutcome::CacheHit(*entry));
        }

        match self.fetch_authenticated_bytes(&url, &api_key, None).await {
            FetchResult::Ok { status, bytes } => {
                match write_verified_parquet_atomic(&cache_path, &bytes) {
                    Ok((size, sha)) => {
                        let entry = self
                            .manifest
                            .commit_success(&entry_key, 1, status, size, sha, cache_path)?;
                        Ok(DownloadOutcome::Downloaded {
                            entry,
                            extracted_files: Vec::new(),
                        })
                    }
                    Err(err) => {
                        if let Err(merr) = self.manifest.commit_ambiguous_failure(
                            &entry_key,
                            1,
                            Some(status),
                            err.to_string(),
                        ) {
                            return Err(HistoricalError::ManifestIo {
                                path: self.manifest.path().to_path_buf(),
                                detail: format!(
                                    "market write failed ({err}) and manifest ambiguous commit failed: {merr}"
                                ),
                            });
                        }
                        Err(err)
                    }
                }
            }
            FetchResult::UnchargedHttp {
                status,
                entry_status,
                message,
            } => {
                let entry = self.manifest.release_uncharged(
                    &entry_key,
                    1,
                    Some(status),
                    entry_status,
                    message.clone(),
                )?;
                if status == 404 {
                    return Ok(DownloadOutcome::NotFound { entry });
                }
                Err(HistoricalError::HttpStatus {
                    status,
                    endpoint: url,
                    message,
                })
            }
            FetchResult::PreSendError(err) => {
                if let Err(merr) = self.manifest.release_uncharged(
                    &entry_key,
                    1,
                    None,
                    EntryStatus::FailedUncharged,
                    err.to_string(),
                ) {
                    return Err(HistoricalError::ManifestIo {
                        path: self.manifest.path().to_path_buf(),
                        detail: format!(
                            "pre-send fetch failed ({err}) and manifest release failed: {merr}"
                        ),
                    });
                }
                Err(err)
            }
            FetchResult::AmbiguousFailure { status, detail } => {
                if let Err(merr) =
                    self.manifest
                        .commit_ambiguous_failure(&entry_key, 1, status, detail.clone())
                {
                    return Err(HistoricalError::ManifestIo {
                        path: self.manifest.path().to_path_buf(),
                        detail: format!(
                            "ambiguous fetch failed ({detail}) and manifest commit failed: {merr}"
                        ),
                    });
                }
                Err(HistoricalError::Http {
                    endpoint: url,
                    detail,
                })
            }
        }
    }

    /// Downloads and verifies a Day Bundle ZIP archive, supports byte-range resume (`Range: bytes=START-`),
    /// verifies every internal Parquet file's CRC32 and `PAR1` readability, and extracts atomically.
    pub async fn download_day_bundle(
        &self,
        dataset: PmdataDataset,
        date: NaiveDate,
        asset: &str,
        interval: UpDownInterval,
        outcome: Option<PmdataL2Outcome>,
        dry_run: bool,
    ) -> Result<DownloadOutcome, HistoricalError> {
        let url = self.build_bundle_url(dataset, date, asset, interval, outcome)?;
        let cache_path = self.bundle_cache_path(dataset, date, asset, interval, outcome);
        let extract_dir = self.bundle_extract_dir(dataset, date, asset, interval, outcome);
        let entry_key = bundle_entry_key(dataset, date, asset, interval, outcome);
        let raw_cost = interval.day_bundle_quota_units();
        let part_path = partial_path_for(&cache_path);
        let existing_part_bytes = fs::metadata(&part_path).ok().map_or(0, |m| m.len());
        let manifest_snapshot = self.manifest.load()?;
        let cached = existing_valid_cache_entry(&manifest_snapshot, &entry_key);
        let prior_credit = ManifestStore::resumable_prior_credit(
            &manifest_snapshot,
            &entry_key,
            existing_part_bytes > 0,
        );
        let cost = raw_cost.saturating_sub(prior_credit);
        let plan = DownloadPlanItem {
            entry_key: entry_key.clone(),
            endpoint_kind: EndpointKind::DayBundle,
            dataset,
            outcome,
            asset: asset.to_ascii_lowercase(),
            interval: interval.as_str().to_string(),
            utc_date: date.format("%Y-%m-%d").to_string(),
            market_slug: None,
            url: url.clone(),
            cache_path: cache_path.clone(),
            already_cached: cached.is_some(),
            quota_cost: cost,
        };
        if dry_run {
            return Ok(DownloadOutcome::DryRun(plan));
        }
        if let Some(entry) = cached {
            if !dir_has_parquet(&extract_dir) && cache_path.is_file() {
                let _ = extract_verified_zip_into(&cache_path, &extract_dir)?;
            }
            return Ok(DownloadOutcome::CacheHit(entry));
        }

        // Recover a locally promoted/verified ZIP if a prior attempt finished extracting/promoting
        // `part_path -> cache_path` before `commit_success` failed to save the manifest.
        if cache_path.is_file()
            && let Some(prev_entry) = manifest_snapshot.entries.get(&entry_key)
            && matches!(
                prev_entry.status,
                EntryStatus::Reserved | EntryStatus::FailedAmbiguous
            )
            && let Ok((size, sha, _)) = extract_verified_zip_into(&cache_path, &extract_dir)
        {
            let recovered_cost = if prev_entry.status == EntryStatus::Reserved {
                raw_cost.saturating_sub(prev_entry.quota_units_charged)
            } else {
                0
            };
            let entry = self.manifest.commit_success_for(
                &entry_key,
                prev_entry.reservation_id.as_deref(),
                recovered_cost,
                prev_entry.http_status.unwrap_or(200),
                size,
                sha,
                cache_path,
            )?;
            return Ok(DownloadOutcome::CacheHit(entry));
        }

        let api_key = self.config.require_api_key()?.to_string();

        // If a partial `.part` exists (> 0 bytes) and a prior ambiguous attempt already charged
        // quota for `entry_key`, resuming via `Range: bytes=START-` does NOT consume extra quota.
        let meta = ReserveMetadata {
            entry_key: entry_key.clone(),
            endpoint_kind: EndpointKind::DayBundle,
            dataset_type: dataset.as_str().to_string(),
            outcome_param: outcome.and_then(|o| o.query_param_value().map(str::to_string)),
            asset: asset.to_ascii_lowercase(),
            interval: interval.as_str().to_string(),
            utc_date: date.format("%Y-%m-%d").to_string(),
            market_slug: None,
            requested_url: url.clone(),
            quota_cost: raw_cost,
        };
        let (reserve_outcome, attempt_cost, res_guard) = self
            .manifest
            .reserve_with_resume_guarded(meta, existing_part_bytes > 0)?;
        if let ReserveOutcome::AlreadyCached(entry) = reserve_outcome {
            if !dir_has_parquet(&extract_dir) && cache_path.is_file() {
                let _ = extract_verified_zip_into(&cache_path, &extract_dir)?;
            }
            return Ok(DownloadOutcome::CacheHit(*entry));
        }
        let res_id = res_guard.as_ref().map(|g| g.reservation_id.as_str());

        let range_start = (existing_part_bytes > 0).then_some(existing_part_bytes);
        match self
            .fetch_authenticated_stream_to_part(&url, &api_key, range_start, &part_path)
            .await
        {
            FetchStreamResult::Ok { status } => {
                match verify_and_extract_zip(&part_path, &cache_path, &extract_dir) {
                    Ok((size, sha, extracted_files)) => {
                        let entry = self.manifest.commit_success_for(
                            &entry_key,
                            res_id,
                            attempt_cost,
                            status,
                            size,
                            sha,
                            cache_path,
                        )?;
                        Ok(DownloadOutcome::Downloaded {
                            entry,
                            extracted_files,
                        })
                    }
                    Err(err) => {
                        if status == 200 {
                            // Full 200 OK payload is corrupt; remove `.part` so retries do not
                            // issue `Range: bytes=<corrupt_len>-` and wedge on HTTP 416.
                            let _ = fs::remove_file(&part_path);
                        }
                        if let Err(merr) = self.manifest.commit_ambiguous_failure_for(
                            &entry_key,
                            res_id,
                            attempt_cost,
                            Some(status),
                            err.to_string(),
                        ) {
                            return Err(HistoricalError::ManifestIo {
                                path: self.manifest.path().to_path_buf(),
                                detail: format!(
                                    "zip verify failed ({err}) and manifest commit failed: {merr}"
                                ),
                            });
                        }
                        Err(err)
                    }
                }
            }
            FetchStreamResult::UnchargedHttp {
                status,
                entry_status,
                message,
            } => {
                if status == 416 {
                    let _ = fs::remove_file(&part_path);
                }
                let entry = self.manifest.release_uncharged_for(
                    &entry_key,
                    res_id,
                    attempt_cost,
                    Some(status),
                    entry_status,
                    message.clone(),
                )?;
                if status == 404 {
                    return Ok(DownloadOutcome::NotFound { entry });
                }
                Err(HistoricalError::HttpStatus {
                    status,
                    endpoint: url,
                    message,
                })
            }
            FetchStreamResult::PreSendError(err) => {
                if let Err(merr) = self.manifest.release_uncharged_for(
                    &entry_key,
                    res_id,
                    attempt_cost,
                    None,
                    EntryStatus::FailedUncharged,
                    err.to_string(),
                ) {
                    return Err(HistoricalError::ManifestIo {
                        path: self.manifest.path().to_path_buf(),
                        detail: format!(
                            "pre-send bundle fetch failed ({err}) and manifest release failed: {merr}"
                        ),
                    });
                }
                Err(err)
            }
            FetchStreamResult::AmbiguousFailure { status, detail } => {
                if let Err(merr) = self.manifest.commit_ambiguous_failure_for(
                    &entry_key,
                    res_id,
                    attempt_cost,
                    status,
                    detail.clone(),
                ) {
                    return Err(HistoricalError::ManifestIo {
                        path: self.manifest.path().to_path_buf(),
                        detail: format!(
                            "ambiguous bundle fetch failed ({detail}) and manifest commit failed: {merr}"
                        ),
                    });
                }
                Err(HistoricalError::Http {
                    endpoint: url,
                    detail,
                })
            }
        }
    }

    /// Returns expected local path for the cached markets catalog Parquet.
    #[must_use]
    pub fn catalog_cache_path(&self) -> PathBuf {
        self.config
            .cache_dir
            .join("catalog")
            .join("markets.parquet")
    }

    /// Returns expected local path for a cached single-market Parquet file.
    #[must_use]
    pub fn market_cache_path(
        &self,
        dataset: PmdataDataset,
        date: NaiveDate,
        market_slug: &str,
        outcome: Option<PmdataL2Outcome>,
    ) -> PathBuf {
        let suffix = match (dataset, outcome) {
            (PmdataDataset::L2, Some(PmdataL2Outcome::No)) => "_no",
            (PmdataDataset::L2, _) => "_yes",
            _ => "",
        };
        self.config
            .cache_dir
            .join("markets")
            .join(date.format("%Y-%m-%d").to_string())
            .join(dataset.as_str())
            .join(format!("{market_slug}{suffix}.parquet"))
    }

    /// Returns expected local path for a cached Day Bundle ZIP archive.
    #[must_use]
    pub fn bundle_cache_path(
        &self,
        dataset: PmdataDataset,
        date: NaiveDate,
        asset: &str,
        interval: UpDownInterval,
        outcome: Option<PmdataL2Outcome>,
    ) -> PathBuf {
        let suffix = match (dataset, outcome) {
            (PmdataDataset::L2, Some(PmdataL2Outcome::No)) => "_no",
            (PmdataDataset::L2, _) => "_yes",
            _ => "",
        };
        self.config
            .cache_dir
            .join("bundles")
            .join(date.format("%Y-%m-%d").to_string())
            .join(format!(
                "{}-{}_{}{}.zip",
                asset.to_ascii_lowercase(),
                interval.as_str(),
                dataset.as_str(),
                suffix
            ))
    }

    /// Returns extraction directory for a cached Day Bundle ZIP archive.
    #[must_use]
    pub fn bundle_extract_dir(
        &self,
        dataset: PmdataDataset,
        date: NaiveDate,
        asset: &str,
        interval: UpDownInterval,
        outcome: Option<PmdataL2Outcome>,
    ) -> PathBuf {
        let suffix = match (dataset, outcome) {
            (PmdataDataset::L2, Some(PmdataL2Outcome::No)) => "_no",
            (PmdataDataset::L2, _) => "_yes",
            _ => "",
        };
        self.config
            .cache_dir
            .join("extracted")
            .join(date.format("%Y-%m-%d").to_string())
            .join(format!(
                "{}-{}_{}{}",
                asset.to_ascii_lowercase(),
                interval.as_str(),
                dataset.as_str(),
                suffix
            ))
    }

    fn build_bundle_plan_item(
        &self,
        manifest: &QuotaManifest,
        dataset: PmdataDataset,
        outcome: Option<PmdataL2Outcome>,
        date: NaiveDate,
        asset: &str,
        interval: UpDownInterval,
    ) -> Result<DownloadPlanItem, HistoricalError> {
        let entry_key = bundle_entry_key(dataset, date, asset, interval, outcome);
        let url = self.build_bundle_url(dataset, date, asset, interval, outcome)?;
        let cache_path = self.bundle_cache_path(dataset, date, asset, interval, outcome);
        let part_path = partial_path_for(&cache_path);
        let has_partial = fs::metadata(&part_path).ok().is_some_and(|m| m.len() > 0);
        let already_cached = existing_valid_cache_entry(manifest, &entry_key).is_some();
        let raw_cost = interval.day_bundle_quota_units();
        let prior_credit = ManifestStore::resumable_prior_credit(manifest, &entry_key, has_partial);
        let quota_cost = if already_cached {
            0
        } else {
            raw_cost.saturating_sub(prior_credit)
        };
        Ok(DownloadPlanItem {
            entry_key,
            endpoint_kind: EndpointKind::DayBundle,
            dataset,
            outcome,
            asset: asset.to_ascii_lowercase(),
            interval: interval.as_str().to_string(),
            utc_date: date.format("%Y-%m-%d").to_string(),
            market_slug: None,
            url,
            cache_path,
            already_cached,
            quota_cost,
        })
    }

    async fn fetch_public_bytes(&self, url: &str) -> Result<(u16, Vec<u8>), HistoricalError> {
        let mut last_err = None;
        for attempt in 0..=self.config.max_retries {
            if attempt > 0 {
                tokio::time::sleep(Duration::from_millis(200_u64 * u64::from(attempt))).await;
            }
            match self.http.get(url).send().await {
                Ok(resp) => {
                    let status = resp.status().as_u16();
                    let bytes = resp.bytes().await.map_err(|err| HistoricalError::Http {
                        endpoint: url.to_string(),
                        detail: sanitize_snippet(&err.to_string(), None),
                    })?;
                    if !(200..300).contains(&status) {
                        let msg = sanitize_snippet(&String::from_utf8_lossy(&bytes), None);
                        return Err(HistoricalError::HttpStatus {
                            status,
                            endpoint: url.to_string(),
                            message: msg,
                        });
                    }
                    return Ok((status, bytes.to_vec()));
                }
                Err(err) => {
                    last_err = Some(HistoricalError::Http {
                        endpoint: url.to_string(),
                        detail: sanitize_snippet(&err.to_string(), None),
                    });
                }
            }
        }
        Err(last_err.unwrap_or_else(|| HistoricalError::Http {
            endpoint: url.to_string(),
            detail: "unknown catalog download error".to_string(),
        }))
    }

    async fn fetch_authenticated_bytes(
        &self,
        url: &str,
        api_key: &str,
        range_start: Option<u64>,
    ) -> FetchResult {
        let mut headers = HeaderMap::new();
        let Ok(val) = HeaderValue::from_str(api_key) else {
            return FetchResult::PreSendError(HistoricalError::Config {
                key: "PMDATA_API_KEY",
                detail: "API key contains invalid HTTP header characters".to_string(),
            });
        };
        headers.insert("api_key", val);
        if let Some(start) = range_start {
            let Ok(rval) = HeaderValue::from_str(&format!("bytes={start}-")) else {
                return FetchResult::PreSendError(HistoricalError::Config {
                    key: "range_start",
                    detail: "invalid Range header value".to_string(),
                });
            };
            headers.insert(RANGE, rval);
        }

        // Only retry strictly BEFORE a request has successfully connected/sent (`is_connect()`).
        // Once a request may have reached PMData servers, never blindly retry because quota
        // accounting could double-charge.
        let mut attempt = 0_u32;
        loop {
            let resp_res = self.http.get(url).headers(headers.clone()).send().await;
            match resp_res {
                Ok(resp) => {
                    let status = resp.status().as_u16();
                    let content_range = resp
                        .headers()
                        .get(CONTENT_RANGE)
                        .and_then(|v| v.to_str().ok())
                        .map(str::to_string);
                    if let Some(entry_status) = classify_uncharged_http_status(status) {
                        let msg = match resp.bytes().await {
                            Ok(b) => sanitize_snippet(&String::from_utf8_lossy(&b), Some(api_key)),
                            Err(err) => sanitize_snippet(&err.to_string(), Some(api_key)),
                        };
                        return FetchResult::UnchargedHttp {
                            status,
                            entry_status,
                            message: msg,
                        };
                    }
                    let body = match resp.bytes().await {
                        Ok(b) => b,
                        Err(err) => {
                            return FetchResult::AmbiguousFailure {
                                status: Some(status),
                                detail: format!(
                                    "response body read interrupted after HTTP {status}: {}",
                                    sanitize_snippet(&err.to_string(), Some(api_key))
                                ),
                            };
                        }
                    };
                    if status == 206 {
                        let expected_start = range_start.unwrap_or(0);
                        if !valid_content_range(
                            content_range.as_deref(),
                            expected_start,
                            body.len() as u64,
                        ) {
                            return FetchResult::AmbiguousFailure {
                                status: Some(status),
                                detail: format!(
                                    "invalid or mismatched Content-Range {:?} for resume offset {expected_start} (body_len={})",
                                    content_range.as_deref().unwrap_or("<missing>"),
                                    body.len()
                                ),
                            };
                        }
                        return FetchResult::Ok {
                            status,
                            bytes: body.to_vec(),
                        };
                    }
                    if status == 200 {
                        return FetchResult::Ok {
                            status,
                            bytes: body.to_vec(),
                        };
                    }
                    let msg = sanitize_snippet(&String::from_utf8_lossy(&body), Some(api_key));
                    if (500..=599).contains(&status) {
                        return FetchResult::AmbiguousFailure {
                            status: Some(status),
                            detail: msg,
                        };
                    }
                    return FetchResult::UnchargedHttp {
                        status,
                        entry_status: EntryStatus::FailedUncharged,
                        message: msg,
                    };
                }
                Err(err) => {
                    let detail = sanitize_snippet(&err.to_string(), Some(api_key));
                    if err.is_connect() && attempt < self.config.max_retries {
                        attempt += 1;
                        tokio::time::sleep(Duration::from_millis(200_u64 * u64::from(attempt)))
                            .await;
                        continue;
                    }
                    if err.is_connect() {
                        return FetchResult::PreSendError(HistoricalError::Http {
                            endpoint: url.to_string(),
                            detail,
                        });
                    }
                    return FetchResult::AmbiguousFailure {
                        status: None,
                        detail: format!(
                            "post-send transport/timeout error (conservatively accounted): {detail}"
                        ),
                    };
                }
            }
        }
    }

    async fn fetch_authenticated_stream_to_part(
        &self,
        url: &str,
        api_key: &str,
        range_start: Option<u64>,
        part_path: &Path,
    ) -> FetchStreamResult {
        let mut headers = HeaderMap::new();
        let Ok(val) = HeaderValue::from_str(api_key) else {
            return FetchStreamResult::PreSendError(HistoricalError::Config {
                key: "PMDATA_API_KEY",
                detail: "API key contains invalid HTTP header characters".to_string(),
            });
        };
        headers.insert("api_key", val);
        if let Some(start) = range_start {
            let Ok(rval) = HeaderValue::from_str(&format!("bytes={start}-")) else {
                return FetchStreamResult::PreSendError(HistoricalError::Config {
                    key: "range_start",
                    detail: "invalid Range header value".to_string(),
                });
            };
            headers.insert(RANGE, rval);
        }

        let mut attempt = 0_u32;
        loop {
            let resp_res = self.http.get(url).headers(headers.clone()).send().await;
            match resp_res {
                Ok(mut resp) => {
                    let status = resp.status().as_u16();
                    let content_range = resp
                        .headers()
                        .get(CONTENT_RANGE)
                        .and_then(|v| v.to_str().ok())
                        .map(str::to_string);
                    if let Some(entry_status) = classify_uncharged_http_status(status) {
                        let msg = match resp.bytes().await {
                            Ok(b) => sanitize_snippet(&String::from_utf8_lossy(&b), Some(api_key)),
                            Err(err) => sanitize_snippet(&err.to_string(), Some(api_key)),
                        };
                        return FetchStreamResult::UnchargedHttp {
                            status,
                            entry_status,
                            message: msg,
                        };
                    }
                    if status != 200 && status != 206 {
                        let msg = match resp.bytes().await {
                            Ok(b) => sanitize_snippet(&String::from_utf8_lossy(&b), Some(api_key)),
                            Err(err) => sanitize_snippet(&err.to_string(), Some(api_key)),
                        };
                        if (500..=599).contains(&status) {
                            return FetchStreamResult::AmbiguousFailure {
                                status: Some(status),
                                detail: msg,
                            };
                        }
                        return FetchStreamResult::UnchargedHttp {
                            status,
                            entry_status: EntryStatus::FailedUncharged,
                            message: msg,
                        };
                    }

                    let expected_range_len = if status == 206 {
                        let expected_start = range_start.unwrap_or(0);
                        let Some(decl_len) =
                            parse_content_range_len(content_range.as_deref(), expected_start)
                        else {
                            return FetchStreamResult::AmbiguousFailure {
                                status: Some(status),
                                detail: format!(
                                    "invalid or mismatched Content-Range {:?} for resume offset {expected_start}",
                                    content_range.as_deref().unwrap_or("<missing>")
                                ),
                            };
                        };
                        Some(decl_len)
                    } else {
                        None
                    };

                    if let Err(err) = ensure_parent(part_path) {
                        return FetchStreamResult::AmbiguousFailure {
                            status: Some(status),
                            detail: err.to_string(),
                        };
                    }
                    let mut file = if range_start.is_some() && status == 206 {
                        match OpenOptions::new().append(true).open(part_path) {
                            Ok(mut f) => {
                                if let Err(err) = f.seek(SeekFrom::End(0)) {
                                    return FetchStreamResult::AmbiguousFailure {
                                        status: Some(status),
                                        detail: format!("seek partial zip failed: {err}"),
                                    };
                                }
                                f
                            }
                            Err(err) => {
                                return FetchStreamResult::AmbiguousFailure {
                                    status: Some(status),
                                    detail: format!("open partial zip failed: {err}"),
                                };
                            }
                        }
                    } else {
                        match File::create(part_path) {
                            Ok(f) => f,
                            Err(err) => {
                                return FetchStreamResult::AmbiguousFailure {
                                    status: Some(status),
                                    detail: format!("create partial zip failed: {err}"),
                                };
                            }
                        }
                    };

                    let mut written_this_resp = 0_u64;
                    loop {
                        match resp.chunk().await {
                            Ok(Some(chunk)) => {
                                if let Err(err) = file.write_all(&chunk) {
                                    let _ = file.sync_all();
                                    return FetchStreamResult::AmbiguousFailure {
                                        status: Some(status),
                                        detail: format!("write bundle chunk failed: {err}"),
                                    };
                                }
                                written_this_resp =
                                    written_this_resp.saturating_add(chunk.len() as u64);
                            }
                            Ok(None) => break,
                            Err(err) => {
                                let _ = file.sync_all();
                                return FetchStreamResult::AmbiguousFailure {
                                    status: Some(status),
                                    detail: format!(
                                        "response body read interrupted after HTTP {status}: {}",
                                        sanitize_snippet(&err.to_string(), Some(api_key))
                                    ),
                                };
                            }
                        }
                    }
                    if let Err(err) = file.sync_all() {
                        return FetchStreamResult::AmbiguousFailure {
                            status: Some(status),
                            detail: format!("sync partial zip failed: {err}"),
                        };
                    }
                    if let Some(decl_len) = expected_range_len
                        && written_this_resp != decl_len
                    {
                        return FetchStreamResult::AmbiguousFailure {
                            status: Some(status),
                            detail: format!(
                                "Content-Range declared {decl_len} bytes, received {written_this_resp}"
                            ),
                        };
                    }
                    return FetchStreamResult::Ok { status };
                }
                Err(err) => {
                    let detail = sanitize_snippet(&err.to_string(), Some(api_key));
                    if err.is_connect() && attempt < self.config.max_retries {
                        attempt += 1;
                        tokio::time::sleep(Duration::from_millis(200_u64 * u64::from(attempt)))
                            .await;
                        continue;
                    }
                    if err.is_connect() {
                        return FetchStreamResult::PreSendError(HistoricalError::Http {
                            endpoint: url.to_string(),
                            detail,
                        });
                    }
                    return FetchStreamResult::AmbiguousFailure {
                        status: None,
                        detail: format!(
                            "post-send transport/timeout error (conservatively accounted): {detail}"
                        ),
                    };
                }
            }
        }
    }
}

enum FetchResult {
    Ok {
        status: u16,
        bytes: Vec<u8>,
    },
    UnchargedHttp {
        status: u16,
        entry_status: EntryStatus,
        message: String,
    },
    PreSendError(HistoricalError),
    AmbiguousFailure {
        status: Option<u16>,
        detail: String,
    },
}

enum FetchStreamResult {
    Ok {
        status: u16,
    },
    UnchargedHttp {
        status: u16,
        entry_status: EntryStatus,
        message: String,
    },
    PreSendError(HistoricalError),
    AmbiguousFailure {
        status: Option<u16>,
        detail: String,
    },
}

fn classify_uncharged_http_status(status: u16) -> Option<EntryStatus> {
    match status {
        404 => Some(EntryStatus::NotFound),
        400 | 401 | 403 | 429 => Some(EntryStatus::Forbidden),
        416 => Some(EntryStatus::FailedUncharged),
        _ => None,
    }
}

/// Validates that `?outcome=No` is only used on `l2`.
fn validate_outcome_for_dataset(
    dataset: PmdataDataset,
    outcome: Option<PmdataL2Outcome>,
) -> Result<(), HistoricalError> {
    if outcome.is_some() && dataset != PmdataDataset::L2 {
        return Err(HistoricalError::Config {
            key: "outcome",
            detail: format!(
                "`outcome` parameter is valid only for `l2`, not `{}`",
                dataset.as_str()
            ),
        });
    }
    Ok(())
}

fn append_outcome_query(url: String, outcome: Option<PmdataL2Outcome>) -> String {
    match outcome.and_then(PmdataL2Outcome::query_param_value) {
        Some(v) => format!("{url}?outcome={v}"),
        None => url,
    }
}

/// Canonical manifest key for a single-market Parquet file.
#[must_use]
pub fn market_entry_key(
    dataset: PmdataDataset,
    date: NaiveDate,
    market_slug: &str,
    outcome: Option<PmdataL2Outcome>,
) -> String {
    let out_tag = match (dataset, outcome) {
        (PmdataDataset::L2, Some(o)) => format!(":{}", o.as_slug_suffix()),
        (PmdataDataset::L2, None) => ":yes".to_string(),
        _ => String::new(),
    };
    format!(
        "market:{}{}:{}:{}",
        dataset.as_str(),
        out_tag,
        date.format("%Y-%m-%d"),
        market_slug.trim()
    )
}

/// Canonical manifest key for a Day Bundle ZIP archive.
#[must_use]
pub fn bundle_entry_key(
    dataset: PmdataDataset,
    date: NaiveDate,
    asset: &str,
    interval: UpDownInterval,
    outcome: Option<PmdataL2Outcome>,
) -> String {
    let out_tag = match (dataset, outcome) {
        (PmdataDataset::L2, Some(o)) => format!(":{}", o.as_slug_suffix()),
        (PmdataDataset::L2, None) => ":yes".to_string(),
        _ => String::new(),
    };
    format!(
        "bundle:{}{}:{}:{}-{}",
        dataset.as_str(),
        out_tag,
        date.format("%Y-%m-%d"),
        asset.to_ascii_lowercase(),
        interval.as_str()
    )
}

fn infer_interval_label(slug: &str) -> String {
    let lower = slug.to_ascii_lowercase();
    if lower.contains("-updown-5m-") {
        "5m".to_string()
    } else if lower.contains("-updown-15m-") {
        "15m".to_string()
    } else if lower.contains("-updown-1h-") {
        "1h".to_string()
    } else {
        "unknown".to_string()
    }
}

fn partial_path_for(target: &Path) -> PathBuf {
    let mut os = target.as_os_str().to_os_string();
    os.push(".part");
    PathBuf::from(os)
}

fn ensure_parent(path: &Path) -> Result<(), HistoricalError> {
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        fs::create_dir_all(parent).map_err(|source| HistoricalError::Io {
            context: format!("create parent dir `{}`", parent.display()),
            source,
        })?;
    }
    Ok(())
}

pub(crate) fn sha256_file(path: &Path) -> Result<(u64, String), HistoricalError> {
    let mut file = File::open(path).map_err(|source| HistoricalError::Io {
        context: format!("hash file `{}`", path.display()),
        source,
    })?;
    let mut hasher = Sha256::new();
    let mut buf = [0_u8; 64 * 1024];
    let mut total = 0_u64;
    loop {
        let n = file.read(&mut buf).map_err(|source| HistoricalError::Io {
            context: format!("read file for sha256 `{}`", path.display()),
            source,
        })?;
        if n == 0 {
            break;
        }
        if let Some(slice) = buf.get(..n) {
            hasher.update(slice);
            total = total.saturating_add(n as u64);
        }
    }
    Ok((total, hex::encode(hasher.finalize())))
}

fn write_verified_parquet_atomic(
    dest: &Path,
    bytes: &[u8],
) -> Result<(u64, String), HistoricalError> {
    ensure_parent(dest)?;
    let nanos = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let tmp = dest.with_file_name(format!(
        "{}.tmp.{}.{}",
        dest.file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("file.parquet"),
        std::process::id(),
        nanos
    ));
    {
        let mut f = File::create(&tmp).map_err(|source| HistoricalError::Io {
            context: format!("create temp parquet `{}`", tmp.display()),
            source,
        })?;
        f.write_all(bytes).map_err(|source| HistoricalError::Io {
            context: format!("write temp parquet `{}`", tmp.display()),
            source,
        })?;
        f.sync_all().map_err(|source| HistoricalError::Io {
            context: format!("sync temp parquet `{}`", tmp.display()),
            source,
        })?;
    }
    if let Err(err) = verify_parquet_readable(&tmp) {
        let _ = fs::remove_file(&tmp);
        return Err(err);
    }
    let (size, sha) = sha256_file(&tmp)?;
    if let Err(err) = fs::rename(&tmp, dest) {
        let _ = fs::remove_file(&tmp);
        return Err(HistoricalError::Io {
            context: format!("move verified parquet to `{}`", dest.display()),
            source: err,
        });
    }
    Ok((size, sha))
}

fn parse_content_range_len(header: Option<&str>, expected_start: u64) -> Option<u64> {
    let raw = header?.trim();
    let rest = raw.strip_prefix("bytes ")?;
    let (range_part, total_part) = rest.split_once('/')?;
    let (start_str, end_str) = range_part.split_once('-')?;
    let start = start_str.trim().parse::<u64>().ok()?;
    let end = end_str.trim().parse::<u64>().ok()?;
    if start != expected_start || end < start {
        return None;
    }
    let total_str = total_part.trim();
    if total_str != "*" {
        let total = total_str.parse::<u64>().ok()?;
        if end >= total {
            return None;
        }
    }
    end.checked_sub(start)?.checked_add(1)
}

fn valid_content_range(header: Option<&str>, expected_start: u64, body_len: u64) -> bool {
    parse_content_range_len(header, expected_start) == Some(body_len)
}

fn dir_has_parquet(dir: &Path) -> bool {
    let Ok(entries) = fs::read_dir(dir) else {
        return false;
    };
    for ent in entries.flatten() {
        let p = ent.path();
        if p.is_file()
            && p.extension()
                .and_then(|e| e.to_str())
                .is_some_and(|e| e.eq_ignore_ascii_case("parquet"))
        {
            return true;
        }
        if p.is_dir() && dir_has_parquet(&p) {
            return true;
        }
    }
    false
}

fn extract_verified_zip_into(
    zip_path: &Path,
    extract_dir: &Path,
) -> Result<(u64, String, Vec<PathBuf>), HistoricalError> {
    let file = File::open(zip_path).map_err(|source| HistoricalError::Io {
        context: format!("open zip `{}`", zip_path.display()),
        source,
    })?;
    let mut archive = ZipArchive::new(file).map_err(|err| HistoricalError::InvalidZip {
        path: zip_path.to_path_buf(),
        detail: err.to_string(),
    })?;
    if archive.is_empty() {
        return Err(HistoricalError::InvalidZip {
            path: zip_path.to_path_buf(),
            detail: "ZIP archive contains 0 entries".to_string(),
        });
    }
    if archive.len() > MAX_ZIP_ENTRIES {
        return Err(HistoricalError::InvalidZip {
            path: zip_path.to_path_buf(),
            detail: format!(
                "ZIP entry count {} exceeds safety limit {MAX_ZIP_ENTRIES}",
                archive.len()
            ),
        });
    }

    let seq = TEMP_SEQ.fetch_add(1, Ordering::Relaxed);
    let tmp_extract = extract_dir.with_file_name(format!(
        "{}.tmp.{}.{}",
        extract_dir
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("extract"),
        std::process::id(),
        seq
    ));
    fs::create_dir_all(&tmp_extract).map_err(|source| HistoricalError::Io {
        context: format!("create temp extract dir `{}`", tmp_extract.display()),
        source,
    })?;
    let mut tmp_guard = TempDirGuard::new(tmp_extract.clone());

    let mut total_uncompressed = 0_u64;
    let mut extracted_rel = Vec::new();
    for i in 0..archive.len() {
        let mut entry = archive
            .by_index(i)
            .map_err(|err| HistoricalError::InvalidZip {
                path: zip_path.to_path_buf(),
                detail: format!("failed to open ZIP entry #{i}: {err}"),
            })?;
        if entry.is_symlink() {
            return Err(HistoricalError::InvalidZip {
                path: zip_path.to_path_buf(),
                detail: format!("symlink entry `{}` is not permitted", entry.name()),
            });
        }
        let Some(safe_rel) = entry.enclosed_name() else {
            return Err(HistoricalError::InvalidZip {
                path: zip_path.to_path_buf(),
                detail: format!("unsafe path traversal entry `{}`", entry.name()),
            });
        };
        let out_path = tmp_extract.join(&safe_rel);
        if entry.is_dir() {
            fs::create_dir_all(&out_path).map_err(|source| HistoricalError::Io {
                context: format!("create extracted dir `{}`", out_path.display()),
                source,
            })?;
            continue;
        }
        ensure_parent(&out_path)?;
        let written = {
            let out_file = File::create(&out_path).map_err(|source| HistoricalError::Io {
                context: format!("create extracted file `{}`", out_path.display()),
                source,
            })?;
            let mut writer = BufWriter::new(out_file);
            let mut bounded = (&mut entry).take(MAX_ZIP_ENTRY_BYTES.saturating_add(1));
            let n = std::io::copy(&mut bounded, &mut writer).map_err(|err| {
                HistoricalError::InvalidZip {
                    path: zip_path.to_path_buf(),
                    detail: format!("CRC/decompression error on `{}`: {err}", safe_rel.display()),
                }
            })?;
            writer.flush().map_err(|source| HistoricalError::Io {
                context: format!("flush extracted file `{}`", out_path.display()),
                source,
            })?;
            n
        };
        if written > MAX_ZIP_ENTRY_BYTES {
            return Err(HistoricalError::InvalidZip {
                path: zip_path.to_path_buf(),
                detail: format!(
                    "ZIP entry `{}` exceeds per-entry cap ({MAX_ZIP_ENTRY_BYTES} bytes)",
                    safe_rel.display()
                ),
            });
        }
        total_uncompressed = total_uncompressed.saturating_add(written);
        if total_uncompressed > MAX_ZIP_TOTAL_BYTES {
            return Err(HistoricalError::InvalidZip {
                path: zip_path.to_path_buf(),
                detail: format!(
                    "cumulative ZIP uncompressed size exceeds cap ({MAX_ZIP_TOTAL_BYTES} bytes)"
                ),
            });
        }
        if out_path
            .extension()
            .and_then(|e| e.to_str())
            .is_some_and(|e| e.eq_ignore_ascii_case("parquet"))
        {
            verify_parquet_readable(&out_path)?;
        }
        extracted_rel.push(safe_rel);
    }

    if extract_dir.exists() {
        fs::remove_dir_all(extract_dir).map_err(|source| HistoricalError::Io {
            context: format!("replace extract dir `{}`", extract_dir.display()),
            source,
        })?;
    }
    ensure_parent(extract_dir)?;
    fs::rename(&tmp_extract, extract_dir).map_err(|source| HistoricalError::Io {
        context: format!("promote extract dir `{}`", extract_dir.display()),
        source,
    })?;
    tmp_guard.disarm();

    let (size, sha) = sha256_file(zip_path)?;
    let extracted_paths = extracted_rel
        .into_iter()
        .map(|rel| extract_dir.join(rel))
        .collect();
    Ok((size, sha, extracted_paths))
}

struct TempDirGuard {
    path: PathBuf,
    armed: bool,
}

impl TempDirGuard {
    fn new(path: PathBuf) -> Self {
        Self { path, armed: true }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for TempDirGuard {
    fn drop(&mut self) {
        if self.armed {
            let _ = fs::remove_dir_all(&self.path);
        }
    }
}

/// Verifies every ZIP entry's CRC32 and extracts safely into `extract_dir`, then promotes `part_path -> zip_dest`.
pub fn verify_and_extract_zip(
    part_path: &Path,
    zip_dest: &Path,
    extract_dir: &Path,
) -> Result<(u64, String, Vec<PathBuf>), HistoricalError> {
    let (size, sha, extracted_paths) = extract_verified_zip_into(part_path, extract_dir)?;
    ensure_parent(zip_dest)?;
    if let Err(err) = fs::rename(part_path, zip_dest) {
        if zip_dest.exists() {
            let _ = fs::remove_file(zip_dest);
        }
        fs::rename(part_path, zip_dest).map_err(|rename_err| HistoricalError::Io {
            context: format!("promote zip `{}` (initial: {err})", zip_dest.display()),
            source: rename_err,
        })?;
    }
    Ok((size, sha, extracted_paths))
}
