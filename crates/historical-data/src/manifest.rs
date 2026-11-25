use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

use chrono::Utc;
use serde::{Deserialize, Serialize};

use crate::config::HARD_MAX_QUOTA_CAP;
use crate::error::HistoricalError;

const MANIFEST_SCHEMA_VERSION: u16 = 1;
const LOCK_STALE_SECS: u64 = 60;
const LOCK_WAIT_ITERS: usize = 200;
const LOCK_RETRY_MS: u64 = 25;

/// Logical endpoint family recorded in the persistent manifest.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EndpointKind {
    /// UpDown daily ZIP archive (`/v1/polymarket/{type}/{YYYY}/{MM}/{DD}/{asset}-{interval}.zip`).
    DayBundle,
    /// Single-market Parquet file (`/v1/polymarket/{type}/{YYYY}/{MM}/{DD}/{slug}.parquet`).
    SingleMarket,
    /// Free public markets catalog (`/v1/polymarket/markets.parquet`).
    Catalog,
}

/// Lifecycle state of a single manifest download entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EntryStatus {
    /// Quota reserved before dispatching request.
    Reserved,
    /// Successfully downloaded, integrity-verified, and cached locally.
    Completed,
    /// Upstream returned `404 Not Found` (0 quota charged by PMData).
    NotFound,
    /// Upstream returned `401` / `403` / `429` (0 quota charged by PMData).
    Forbidden,
    /// Local pre-send failure before HTTP dispatch (0 quota charged).
    FailedUncharged,
    /// Ambiguous failure after HTTP dispatch (timeout/drop): quota conservatively charged.
    FailedAmbiguous,
}

/// Persistent audit record for one PMData request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManifestEntry {
    /// Deterministic deduplication key (dataset + outcome + date + slug/bundle).
    pub entry_key: String,
    /// Data provider name (`"pmdata"`).
    pub provider: String,
    /// Endpoint family (`day_bundle`, `single_market`, `catalog`).
    pub endpoint_kind: EndpointKind,
    /// Dataset type (`l2`, `trades`, `onchain_fills`, `catalog`).
    pub dataset_type: String,
    /// Optional outcome query parameter (`None` or `"No"`).
    pub outcome_param: Option<String>,
    /// Target asset (`"btc"` or `"all"`).
    pub asset: String,
    /// Target market interval (`"5m"`, `"15m"`, `"1h"`, or `"catalog"`).
    pub interval: String,
    /// UTC date string (`YYYY-MM-DD`).
    pub utc_date: String,
    /// Market slug for single-market downloads (`None` for day bundles / catalog).
    pub market_slug: Option<String>,
    /// Credential-free requested URL.
    pub requested_url: String,
    /// Observed HTTP status code if a response arrived.
    pub http_status: Option<u16>,
    /// UTC RFC3339 timestamp of last state transition.
    pub download_timestamp_utc: String,
    /// Local cached file size in bytes (0 until completed).
    pub file_size_bytes: u64,
    /// Local cache path (`None` if not cached).
    pub local_cache_path: Option<PathBuf>,
    /// Lowercase hex SHA-256 of cached payload (`None` if not completed).
    pub content_sha256: Option<String>,
    /// Quota units charged against the 650-unit cycle allowance.
    pub quota_units_charged: u32,
    /// Remaining estimated quota (`quota_cap - charged - reserved`) after this transition.
    pub remaining_quota_after: u32,
    /// Current entry status.
    pub status: EntryStatus,
    /// Sanitized error detail if any (never includes API keys).
    pub error_detail: Option<String>,
}

/// Metadata required to reserve a manifest slot before an HTTP request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReserveMetadata {
    pub entry_key: String,
    pub endpoint_kind: EndpointKind,
    pub dataset_type: String,
    pub outcome_param: Option<String>,
    pub asset: String,
    pub interval: String,
    pub utc_date: String,
    pub market_slug: Option<String>,
    pub requested_url: String,
    pub quota_cost: u32,
}

/// Outcome of attempting to reserve quota in the manifest.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReserveOutcome {
    /// Matching verified entry already exists on disk -> 0 quota, skip HTTP request.
    AlreadyCached(Box<ManifestEntry>),
    /// Quota slot reserved atomically in manifest -> proceed with HTTP request.
    Reserved {
        /// Quota remaining after this reservation.
        remaining_after: u32,
    },
}

/// Persistent download and quota-accounting ledger stored as JSON on disk.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QuotaManifest {
    /// Manifest format version.
    pub schema_version: u16,
    /// Hard quota cap for this billing cycle (`<= 650`).
    pub quota_cap: u32,
    /// Cumulative confirmed + ambiguous-failure quota units charged.
    pub charged_units: u32,
    /// In-flight reserved units (not yet finalized).
    pub reserved_units: u32,
    /// Map of `entry_key` -> `ManifestEntry`.
    pub entries: BTreeMap<String, ManifestEntry>,
}

impl QuotaManifest {
    /// Creates an empty manifest bounded by `quota_cap` (`1..=650`).
    pub fn new(quota_cap: u32) -> Result<Self, HistoricalError> {
        if quota_cap == 0 || quota_cap > HARD_MAX_QUOTA_CAP {
            return Err(HistoricalError::Config {
                key: "quota_cap",
                detail: format!("quota_cap ({quota_cap}) must be in 1..={HARD_MAX_QUOTA_CAP}"),
            });
        }
        Ok(Self {
            schema_version: MANIFEST_SCHEMA_VERSION,
            quota_cap,
            charged_units: 0,
            reserved_units: 0,
            entries: BTreeMap::new(),
        })
    }

    /// Remaining available quota (`quota_cap - charged_units - reserved_units`).
    #[must_use]
    pub fn remaining_quota(&self) -> u32 {
        self.quota_cap
            .saturating_sub(self.charged_units)
            .saturating_sub(self.reserved_units)
    }

    /// Returns a completed entry if present.
    #[must_use]
    pub fn get_completed(&self, entry_key: &str) -> Option<&ManifestEntry> {
        self.entries
            .get(entry_key)
            .filter(|e| e.status == EntryStatus::Completed)
    }
}

/// Concurrency-safe handle to a persistent `QuotaManifest` guarded by an exclusive lock file.
#[derive(Debug, Clone)]
pub struct ManifestStore {
    path: PathBuf,
    quota_cap: u32,
}

struct LockGuard {
    lock_path: PathBuf,
}

impl Drop for LockGuard {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.lock_path);
    }
}

impl ManifestStore {
    /// Creates a store targeting `path` with hard cap `quota_cap` (`<= 650`).
    pub fn new(path: impl Into<PathBuf>, quota_cap: u32) -> Result<Self, HistoricalError> {
        if quota_cap == 0 || quota_cap > HARD_MAX_QUOTA_CAP {
            return Err(HistoricalError::Config {
                key: "quota_cap",
                detail: format!("quota_cap ({quota_cap}) must be in 1..={HARD_MAX_QUOTA_CAP}"),
            });
        }
        Ok(Self {
            path: path.into(),
            quota_cap,
        })
    }

    /// Returns manifest file path.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Loads current manifest snapshot safely under lock (returns fresh empty manifest if missing).
    pub fn load(&self) -> Result<QuotaManifest, HistoricalError> {
        let _lock = self.acquire_lock()?;
        self.load_unlocked()
    }

    /// Checks whether `entry_key` is already cached on disk without mutating the manifest.
    pub fn check_cached(&self, entry_key: &str) -> Result<Option<ManifestEntry>, HistoricalError> {
        let manifest = self.load()?;
        Ok(existing_valid_cache_entry(&manifest, entry_key))
    }

    /// Atomically checks cache, validates quota headroom (`charged + reserved + cost <= cap`),
    /// and persists a `Reserved` entry BEFORE sending any network request.
    ///
    /// When `has_partial_file` is `true` and a prior `FailedAmbiguous` attempt already charged
    /// quota for `entry_key`, reserves `0` additional units for a `Range` resume. Returns the
    /// `(ReserveOutcome, reserved_cost_for_this_attempt)` pair.
    pub fn reserve_with_resume(
        &self,
        meta: ReserveMetadata,
        has_partial_file: bool,
    ) -> Result<(ReserveOutcome, u32), HistoricalError> {
        let _lock = self.acquire_lock()?;
        let mut manifest = self.load_unlocked()?;

        if let Some(cached) = existing_valid_cache_entry(&manifest, &meta.entry_key) {
            return Ok((ReserveOutcome::AlreadyCached(Box::new(cached)), 0));
        }

        let prior_charged = if has_partial_file {
            manifest
                .entries
                .get(&meta.entry_key)
                .filter(|e| e.status == EntryStatus::FailedAmbiguous && e.quota_units_charged > 0)
                .map_or(0, |e| e.quota_units_charged)
        } else {
            0
        };
        let attempt_cost = meta.quota_cost.saturating_sub(prior_charged);

        let total_after = manifest
            .charged_units
            .checked_add(manifest.reserved_units)
            .and_then(|u| u.checked_add(attempt_cost))
            .ok_or(HistoricalError::QuotaExceeded {
                needed_units: attempt_cost,
                charged_units: manifest.charged_units,
                reserved_units: manifest.reserved_units,
                quota_cap: manifest.quota_cap,
            })?;

        if total_after > manifest.quota_cap {
            return Err(HistoricalError::QuotaExceeded {
                needed_units: attempt_cost,
                charged_units: manifest.charged_units,
                reserved_units: manifest.reserved_units,
                quota_cap: manifest.quota_cap,
            });
        }

        manifest.reserved_units = manifest.reserved_units.saturating_add(attempt_cost);
        let remaining_after = manifest.remaining_quota();
        let entry = ManifestEntry {
            entry_key: meta.entry_key.clone(),
            provider: "pmdata".to_string(),
            endpoint_kind: meta.endpoint_kind,
            dataset_type: meta.dataset_type,
            outcome_param: meta.outcome_param,
            asset: meta.asset,
            interval: meta.interval,
            utc_date: meta.utc_date,
            market_slug: meta.market_slug,
            requested_url: meta.requested_url,
            http_status: None,
            download_timestamp_utc: Utc::now().to_rfc3339(),
            file_size_bytes: 0,
            local_cache_path: None,
            content_sha256: None,
            quota_units_charged: prior_charged,
            remaining_quota_after: remaining_after,
            status: EntryStatus::Reserved,
            error_detail: None,
        };
        manifest.entries.insert(meta.entry_key, entry);
        self.save_unlocked(&manifest)?;
        Ok((ReserveOutcome::Reserved { remaining_after }, attempt_cost))
    }

    /// Atomically checks cache, validates quota headroom (`charged + reserved + cost <= cap`),
    /// and persists a `Reserved` entry BEFORE sending any network request.
    pub fn reserve(&self, meta: ReserveMetadata) -> Result<ReserveOutcome, HistoricalError> {
        let (outcome, _) = self.reserve_with_resume(meta, false)?;
        Ok(outcome)
    }

    /// Finalizes a reserved entry on verified download success (`reserved -> charged`).
    pub fn commit_success(
        &self,
        entry_key: &str,
        quota_cost: u32,
        http_status: u16,
        file_size_bytes: u64,
        content_sha256: String,
        local_cache_path: PathBuf,
    ) -> Result<ManifestEntry, HistoricalError> {
        let _lock = self.acquire_lock()?;
        let mut manifest = self.load_unlocked()?;
        manifest.reserved_units = manifest.reserved_units.saturating_sub(quota_cost);
        manifest.charged_units = manifest.charged_units.saturating_add(quota_cost);
        let remaining_after = manifest.remaining_quota();

        let entry =
            manifest
                .entries
                .get_mut(entry_key)
                .ok_or_else(|| HistoricalError::ManifestIo {
                    path: self.path.clone(),
                    detail: format!("missing reserved manifest entry `{entry_key}` on commit"),
                })?;
        entry.http_status = Some(http_status);
        entry.download_timestamp_utc = Utc::now().to_rfc3339();
        entry.file_size_bytes = file_size_bytes;
        entry.local_cache_path = Some(local_cache_path);
        entry.content_sha256 = Some(content_sha256);
        entry.quota_units_charged = entry.quota_units_charged.saturating_add(quota_cost);
        entry.remaining_quota_after = remaining_after;
        entry.status = EntryStatus::Completed;
        entry.error_detail = None;
        let cloned = entry.clone();
        self.save_unlocked(&manifest)?;
        Ok(cloned)
    }

    /// Releases a reservation when PMData definitely did NOT charge quota (`400/401/403/404/429` or pre-send error).
    pub fn release_uncharged(
        &self,
        entry_key: &str,
        quota_cost: u32,
        http_status: Option<u16>,
        status: EntryStatus,
        error_detail: String,
    ) -> Result<ManifestEntry, HistoricalError> {
        let _lock = self.acquire_lock()?;
        let mut manifest = self.load_unlocked()?;
        manifest.reserved_units = manifest.reserved_units.saturating_sub(quota_cost);
        let remaining_after = manifest.remaining_quota();

        let entry =
            manifest
                .entries
                .get_mut(entry_key)
                .ok_or_else(|| HistoricalError::ManifestIo {
                    path: self.path.clone(),
                    detail: format!("missing reserved manifest entry `{entry_key}` on release"),
                })?;
        entry.http_status = http_status;
        entry.download_timestamp_utc = Utc::now().to_rfc3339();
        if quota_cost > 0 {
            entry.quota_units_charged = 0;
        }
        entry.remaining_quota_after = remaining_after;
        entry.status = status;
        entry.error_detail = Some(error_detail);
        let cloned = entry.clone();
        self.save_unlocked(&manifest)?;
        Ok(cloned)
    }

    /// Conservatively charges quota when a failure happens after the request may have been billed.
    pub fn commit_ambiguous_failure(
        &self,
        entry_key: &str,
        quota_cost: u32,
        http_status: Option<u16>,
        error_detail: String,
    ) -> Result<ManifestEntry, HistoricalError> {
        let _lock = self.acquire_lock()?;
        let mut manifest = self.load_unlocked()?;
        manifest.reserved_units = manifest.reserved_units.saturating_sub(quota_cost);
        manifest.charged_units = manifest.charged_units.saturating_add(quota_cost);
        let remaining_after = manifest.remaining_quota();

        let entry =
            manifest
                .entries
                .get_mut(entry_key)
                .ok_or_else(|| HistoricalError::ManifestIo {
                    path: self.path.clone(),
                    detail: format!(
                        "missing reserved manifest entry `{entry_key}` on ambiguous commit"
                    ),
                })?;
        entry.http_status = http_status;
        entry.download_timestamp_utc = Utc::now().to_rfc3339();
        entry.quota_units_charged = entry.quota_units_charged.saturating_add(quota_cost);
        entry.remaining_quota_after = remaining_after;
        entry.status = EntryStatus::FailedAmbiguous;
        entry.error_detail = Some(error_detail);
        let cloned = entry.clone();
        self.save_unlocked(&manifest)?;
        Ok(cloned)
    }

    fn ensure_parent_dir(&self) -> Result<(), HistoricalError> {
        if let Some(parent) = self.path.parent()
            && !parent.as_os_str().is_empty()
        {
            fs::create_dir_all(parent).map_err(|source| HistoricalError::Io {
                context: format!("create manifest dir `{}`", parent.display()),
                source,
            })?;
        }
        Ok(())
    }

    fn lock_path(&self) -> PathBuf {
        let mut os = self.path.as_os_str().to_os_string();
        os.push(".lock");
        PathBuf::from(os)
    }

    fn acquire_lock(&self) -> Result<LockGuard, HistoricalError> {
        self.ensure_parent_dir()?;
        let lock_path = self.lock_path();
        for _ in 0..LOCK_WAIT_ITERS {
            match OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&lock_path)
            {
                Ok(mut f) => {
                    let _ = writeln!(f, "pid={}", std::process::id());
                    return Ok(LockGuard { lock_path });
                }
                Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => {
                    if let Ok(meta) = fs::metadata(&lock_path)
                        && let Ok(modified) = meta.modified()
                        && let Ok(age) = SystemTime::now().duration_since(modified)
                        && age > Duration::from_secs(LOCK_STALE_SECS)
                    {
                        let _ = fs::remove_file(&lock_path);
                        continue;
                    }
                    std::thread::sleep(Duration::from_millis(LOCK_RETRY_MS));
                }
                Err(err) => {
                    return Err(HistoricalError::ManifestLocked {
                        path: lock_path,
                        detail: err.to_string(),
                    });
                }
            }
        }
        Err(HistoricalError::ManifestLocked {
            path: lock_path,
            detail: "timed out waiting for exclusive manifest lock".to_string(),
        })
    }

    fn load_unlocked(&self) -> Result<QuotaManifest, HistoricalError> {
        if !self.path.exists() {
            return QuotaManifest::new(self.quota_cap);
        }
        let raw = fs::read_to_string(&self.path).map_err(|err| HistoricalError::ManifestIo {
            path: self.path.clone(),
            detail: format!("read failed: {err}"),
        })?;
        let mut manifest: QuotaManifest =
            serde_json::from_str(&raw).map_err(|err| HistoricalError::ManifestIo {
                path: self.path.clone(),
                detail: format!("JSON parse failed: {err}"),
            })?;
        if manifest.quota_cap > HARD_MAX_QUOTA_CAP || manifest.quota_cap == 0 {
            return Err(HistoricalError::ManifestIo {
                path: self.path.clone(),
                detail: format!(
                    "manifest quota_cap {} violates 1..={HARD_MAX_QUOTA_CAP}",
                    manifest.quota_cap
                ),
            });
        }
        // Respect tighter runtime cap if configured, never exceeding manifest or 650.
        manifest.quota_cap = manifest.quota_cap.min(self.quota_cap);
        Ok(manifest)
    }

    fn save_unlocked(&self, manifest: &QuotaManifest) -> Result<(), HistoricalError> {
        self.ensure_parent_dir()?;
        let nanos = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        let tmp_name = format!(
            "{}.tmp.{}.{}",
            self.path
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or("manifest.json"),
            std::process::id(),
            nanos
        );
        let tmp_path = self.path.with_file_name(tmp_name);
        let body =
            serde_json::to_string_pretty(manifest).map_err(|source| HistoricalError::Json {
                context: format!("serialize manifest `{}`", self.path.display()),
                source,
            })?;
        {
            let mut file = File::create(&tmp_path).map_err(|source| HistoricalError::Io {
                context: format!("create temp manifest `{}`", tmp_path.display()),
                source,
            })?;
            file.write_all(body.as_bytes())
                .map_err(|source| HistoricalError::Io {
                    context: format!("write temp manifest `{}`", tmp_path.display()),
                    source,
                })?;
            file.sync_all().map_err(|source| HistoricalError::Io {
                context: format!("sync temp manifest `{}`", tmp_path.display()),
                source,
            })?;
        }
        if self.path.exists() {
            // On Windows, `fs::rename` fails or behaves inconsistently when replacing an existing file.
            let backup_path = self.path.with_file_name(format!(
                "{}.bak.{}.{}",
                self.path
                    .file_name()
                    .and_then(|n| n.to_str())
                    .unwrap_or("manifest.json"),
                std::process::id(),
                nanos
            ));
            fs::rename(&self.path, &backup_path).map_err(|source| HistoricalError::Io {
                context: format!("rotate manifest backup `{}`", backup_path.display()),
                source,
            })?;
            if let Err(err) = fs::rename(&tmp_path, &self.path) {
                let _ = fs::rename(&backup_path, &self.path);
                return Err(HistoricalError::Io {
                    context: format!("replace manifest `{}`", self.path.display()),
                    source: err,
                });
            }
            let _ = fs::remove_file(&backup_path);
        } else {
            fs::rename(&tmp_path, &self.path).map_err(|source| HistoricalError::Io {
                context: format!("write manifest `{}`", self.path.display()),
                source,
            })?;
        }
        Ok(())
    }
}

fn existing_valid_cache_entry(manifest: &QuotaManifest, entry_key: &str) -> Option<ManifestEntry> {
    let entry = manifest.get_completed(entry_key)?;
    let path = entry.local_cache_path.as_ref()?;
    let meta = fs::metadata(path).ok()?;
    if meta.is_file() && meta.len() > 0 && meta.len() == entry.file_size_bytes {
        Some(entry.clone())
    } else {
        None
    }
}
