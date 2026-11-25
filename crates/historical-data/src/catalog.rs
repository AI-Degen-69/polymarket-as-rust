use std::collections::BTreeSet;

use chrono::NaiveDate;
use core_types::Outcome;
use feeds::parse_outcome_label;

use crate::config::UpDownInterval;
use crate::error::HistoricalError;
use crate::parquet_reader::CatalogEntry;

/// Validated outcome mapping between PMData's `Yes`/`No` representation and `Outcome::{Up, Down}`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OutcomeMapping {
    /// Outcome mapped from PMData `Yes` (default L2 stream).
    pub yes_maps_to: Outcome,
    /// Outcome mapped from PMData `No` (`?outcome=No` L2 stream).
    pub no_maps_to: Outcome,
    /// Whether this mapping came directly from catalog/Gamma metadata (`true`) vs verified slug convention.
    pub verified_by_metadata: bool,
}

impl OutcomeMapping {
    /// Derives and validates outcome mapping from catalog metadata, never assuming `Yes = Up` blindly.
    pub fn from_catalog(entry: Option<&CatalogEntry>, slug: &str) -> Result<Self, HistoricalError> {
        let Some(meta) = entry else {
            return Err(HistoricalError::Normalization {
                context: slug.to_string(),
                detail: "missing catalog outcome metadata; cannot validate Yes/No -> Up/Down mapping (run `pmdata catalog` first)".to_string(),
            });
        };
        match (meta.yes_outcome.as_deref(), meta.no_outcome.as_deref()) {
            (Some(yes_raw), Some(no_raw)) => {
                let yes_mapped =
                    parse_outcome_label(yes_raw).ok_or_else(|| HistoricalError::Normalization {
                        context: slug.to_string(),
                        detail: format!("unrecognized catalog yes_outcome label `{yes_raw}`"),
                    })?;
                let no_mapped =
                    parse_outcome_label(no_raw).ok_or_else(|| HistoricalError::Normalization {
                        context: slug.to_string(),
                        detail: format!("unrecognized catalog no_outcome label `{no_raw}`"),
                    })?;
                if yes_mapped == no_mapped {
                    return Err(HistoricalError::Normalization {
                        context: slug.to_string(),
                        detail: format!(
                            "ambiguous catalog outcomes: yes_outcome=`{yes_raw}`, no_outcome=`{no_raw}`"
                        ),
                    });
                }
                Ok(Self {
                    yes_maps_to: yes_mapped,
                    no_maps_to: no_mapped,
                    verified_by_metadata: true,
                })
            }
            _ => Err(HistoricalError::Normalization {
                context: slug.to_string(),
                detail: "catalog entry missing yes_outcome/no_outcome labels".to_string(),
            }),
        }
    }

    /// Maps a PMData outcome token string (`"Yes"`, `"yes"`, `"No"`, `"no"`, `"Up"`, `"Down"`)
    /// using the validated mapping.
    pub fn map_pmdata_outcome(&self, raw: &str, context: &str) -> Result<Outcome, HistoricalError> {
        let trimmed = raw.trim();
        if trimmed.eq_ignore_ascii_case("yes") {
            return Ok(self.yes_maps_to);
        }
        if trimmed.eq_ignore_ascii_case("no") {
            return Ok(self.no_maps_to);
        }
        if trimmed.eq_ignore_ascii_case("up") {
            return Ok(Outcome::Up);
        }
        if trimmed.eq_ignore_ascii_case("down") {
            return Ok(Outcome::Down);
        }
        Err(HistoricalError::Normalization {
            context: context.to_string(),
            detail: format!("unrecognized PMData outcome label `{raw}`"),
        })
    }
}

/// Filters catalog entries for a given UpDown asset (`"btc"`) and interval (`5m`, `15m`, `1h`).
#[must_use]
pub fn filter_updown_markets<'a>(
    entries: &'a [CatalogEntry],
    asset: &str,
    interval: UpDownInterval,
    from_date: Option<NaiveDate>,
    to_date: Option<NaiveDate>,
) -> Vec<&'a CatalogEntry> {
    let asset_lc = asset.trim().to_ascii_lowercase();
    let expected_series = format!("updown.{}.{}", interval.as_str(), asset_lc);
    let expected_slug_prefix = format!("{asset_lc}-updown-{}-", interval.as_str());

    entries
        .iter()
        .filter(|e| {
            let series_match = e
                .series
                .as_deref()
                .is_some_and(|s| s.eq_ignore_ascii_case(&expected_series));
            let slug_match = e
                .market_slug
                .to_ascii_lowercase()
                .starts_with(&expected_slug_prefix);
            if !series_match && !slug_match {
                return false;
            }
            let Ok(start_d) = NaiveDate::parse_from_str(&e.record_start, "%Y-%m-%d") else {
                return false;
            };
            let Ok(end_d) = NaiveDate::parse_from_str(&e.record_end, "%Y-%m-%d") else {
                return false;
            };
            if let Some(from) = from_date
                && end_d < from
            {
                return false;
            }
            if let Some(to) = to_date
                && start_d > to
            {
                return false;
            }
            true
        })
        .collect()
}

/// Returns sorted unique UTC dates covered in the catalog for `(asset, interval)` within `[from, to]`.
#[must_use]
pub fn catalog_available_dates(
    entries: &[CatalogEntry],
    asset: &str,
    interval: UpDownInterval,
    from_date: NaiveDate,
    to_date: NaiveDate,
) -> BTreeSet<NaiveDate> {
    let mut dates = BTreeSet::new();
    for entry in filter_updown_markets(entries, asset, interval, Some(from_date), Some(to_date)) {
        let Ok(start_d) = NaiveDate::parse_from_str(&entry.record_start, "%Y-%m-%d") else {
            continue;
        };
        let Ok(end_d) = NaiveDate::parse_from_str(&entry.record_end, "%Y-%m-%d") else {
            continue;
        };
        let mut cur = start_d.max(from_date);
        let hi = end_d.min(to_date);
        while cur <= hi {
            dates.insert(cur);
            let Some(next) = cur.succ_opt() else {
                break;
            };
            cur = next;
        }
    }
    dates
}

/// Parses UTC `YYYY-MM-DD` string into `NaiveDate`.
pub fn parse_utc_date(raw: &str) -> Result<NaiveDate, HistoricalError> {
    NaiveDate::parse_from_str(raw.trim(), "%Y-%m-%d").map_err(|err| HistoricalError::Config {
        key: "utc_date",
        detail: format!("invalid UTC date `{raw}` (expected YYYY-MM-DD): {err}"),
    })
}

/// Parses the epoch start timestamp in Unix seconds from an UpDown slug (`btc-updown-5m-<epoch>`).
#[must_use]
pub fn parse_slug_start_secs(slug: &str) -> Option<u64> {
    slug.rsplit('-').next()?.parse::<u64>().ok()
}

/// Infers `UpDownInterval` from a market slug (`btc-updown-5m-...`, `btc-updown-15m-...`, `btc-updown-1h-...`).
#[must_use]
pub fn infer_interval_from_slug(slug: &str) -> Option<UpDownInterval> {
    let lower = slug.to_ascii_lowercase();
    if lower.contains("-updown-5m-") {
        Some(UpDownInterval::M5)
    } else if lower.contains("-updown-15m-") {
        Some(UpDownInterval::M15)
    } else if lower.contains("-updown-1h-") {
        Some(UpDownInterval::H1)
    } else {
        None
    }
}
