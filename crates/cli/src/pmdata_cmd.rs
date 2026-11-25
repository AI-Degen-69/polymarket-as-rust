use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fs;
use std::path::{Path, PathBuf};

use chrono::{NaiveDate, Utc};
use clap::{Subcommand, ValueEnum};
use core_types::Outcome;
use historical_data::{
    CatalogEntry, DataSource, DownloadOutcome, EntryStatus, FidelityVerdict,
    HistoricalPhase0Report, IngestionMode, LiveVsPmdataReport, PmdataClient, PmdataConfig,
    PmdataDataset, PmdataL2Outcome, PmdataMarketFiles, UpDownInterval,
    audit_pmdata_historical_market, catalog_available_dates, filter_updown_markets,
    infer_interval_from_slug, market_entry_key, normalize_live_jsonl, normalize_pmdata_market,
    parse_slug_start_secs, parse_utc_date, read_catalog_parquet_filtered, reconcile_live_vs_pmdata,
    render_historical_phase0_markdown, render_reconcile_markdown, write_canonical_jsonl,
};

#[derive(Debug, Clone, Copy, ValueEnum)]
pub(crate) enum CliIngestionMode {
    Full,
    #[value(name = "quota-saving")]
    QuotaSaving,
}

impl From<CliIngestionMode> for IngestionMode {
    fn from(value: CliIngestionMode) -> Self {
        match value {
            CliIngestionMode::Full => Self::Full,
            CliIngestionMode::QuotaSaving => Self::QuotaSaving,
        }
    }
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub(crate) enum CliNormalizeSource {
    Pmdata,
    #[value(name = "live-jsonl")]
    LiveJsonl,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub(crate) enum CliOutcomeArg {
    Up,
    Down,
}

impl From<CliOutcomeArg> for Outcome {
    fn from(value: CliOutcomeArg) -> Self {
        match value {
            CliOutcomeArg::Up => Self::Up,
            CliOutcomeArg::Down => Self::Down,
        }
    }
}

/// PMData historical dataset discovery, quota-safe downloading, normalization, and reconciliation.
#[derive(Debug, Clone, Subcommand)]
pub(crate) enum PmdataCommands {
    /// Download (0 quota) and query the public PMData markets catalog Parquet.
    Catalog {
        /// Force re-downloading `markets.parquet` (0 quota cost).
        #[arg(long, default_value_t = false)]
        force_refresh: bool,
        /// Optional substring filter applied to market slugs (default: `updown`).
        #[arg(long)]
        filter: Option<String>,
        /// Optional interval filter (`5m`, `15m`, `1h`).
        #[arg(long)]
        interval: Option<String>,
        /// Optional asset filter (default: `btc`).
        #[arg(long, default_value = "btc")]
        asset: String,
        /// Optional inclusive start UTC date (`YYYY-MM-DD`).
        #[arg(long)]
        from_date: Option<String>,
        /// Optional inclusive end UTC date (`YYYY-MM-DD`).
        #[arg(long)]
        to_date: Option<String>,
        /// Maximum number of sample rows to print.
        #[arg(long, default_value_t = 20)]
        limit: usize,
    },
    /// Download historical PMData files (single-market or day-bundle) under the 650-unit quota guard.
    Download {
        /// Single UTC date (`YYYY-MM-DD`) when fetching a Day Bundle or resolving slug date.
        #[arg(long)]
        date: Option<String>,
        /// Inclusive start UTC date (`YYYY-MM-DD`) for range day-bundle downloads.
        #[arg(long)]
        from_date: Option<String>,
        /// Inclusive end UTC date (`YYYY-MM-DD`) for range day-bundle downloads.
        #[arg(long)]
        to_date: Option<String>,
        /// Underlying asset symbol (`btc` default).
        #[arg(long, default_value = "btc")]
        asset: String,
        /// Up/Down interval (`5m`, `15m`, `1h`).
        #[arg(long, default_value = "5m")]
        interval: String,
        /// Download individual-market files (`yes_l2`, optional `no_l2`, `trades`) for this slug.
        #[arg(long)]
        market_slug: Option<String>,
        /// L2 ingestion mode (`full` = Yes+No L2 + trades; `quota-saving` = Yes L2 + trades).
        #[arg(long, value_enum, default_value_t = CliIngestionMode::Full)]
        mode: CliIngestionMode,
        /// Also download `onchain_fills` (`+1` market unit or `+6..72` bundle units).
        #[arg(long, default_value_t = false)]
        include_onchain: bool,
        /// Print exact URLs, cache paths, and quota units without spending quota or sending network requests.
        #[arg(long, default_value_t = false)]
        dry_run: bool,
        /// Explicitly confirm spending quota on Day Bundle downloads (required unless `--dry-run`).
        #[arg(long, default_value_t = false)]
        yes: bool,
    },
    /// Print persistent PMData quota manifest statistics (`charged`, `reserved`, `remaining / 650`).
    Status,
    /// Normalize PMData Parquets or live capture JSONL into unified Canonical Event JSONL.
    Normalize {
        /// Source format (`pmdata` or `live-jsonl`).
        #[arg(long, value_enum)]
        source: CliNormalizeSource,
        /// Input live capture JSONL (required when `--source live-jsonl`).
        #[arg(long)]
        input: Option<PathBuf>,
        /// PMData Yes/Up L2 Parquet file (when `--source pmdata`).
        #[arg(long)]
        yes_l2: Option<PathBuf>,
        /// PMData No/Down L2 Parquet file (when `--source pmdata` in `full` mode).
        #[arg(long)]
        no_l2: Option<PathBuf>,
        /// PMData trades Parquet file (when `--source pmdata`).
        #[arg(long)]
        trades: Option<PathBuf>,
        /// Optional PMData on-chain fills Parquet file.
        #[arg(long)]
        onchain_fills: Option<PathBuf>,
        /// Market slug (inferred from `--yes-l2` / `--trades` parent dir or filename if omitted).
        #[arg(long)]
        market_slug: Option<String>,
        /// Optional `markets.parquet` catalog path (defaults to `<PMDATA_CACHE_DIR>/catalog/markets.parquet` if present).
        #[arg(long)]
        catalog: Option<PathBuf>,
        /// L2 ingestion mode (`full` or `quota-saving`).
        #[arg(long, value_enum, default_value_t = CliIngestionMode::Full)]
        mode: CliIngestionMode,
        /// Authoritative resolution override (`up` or `down`) for markets affected by PMData resolution gap.
        #[arg(long, value_enum)]
        resolution_override: Option<CliOutcomeArg>,
        /// Output Canonical JSONL path.
        #[arg(long)]
        output: PathBuf,
    },
    /// Phase 0 historical PMData verification across downloaded sample markets (zero extra quota).
    VerifyHistorical {
        /// Historical market slugs already cached locally under `--data-dir` / manifest.
        #[arg(long, num_args = 1..)]
        samples: Vec<String>,
        /// Optional `markets.parquet` catalog path.
        #[arg(long)]
        catalog: Option<PathBuf>,
        /// Optional PMData cache dir (defaults to `PMDATA_CACHE_DIR` / `data/pmdata`).
        #[arg(long)]
        data_dir: Option<PathBuf>,
        /// Optional Markdown report output path.
        #[arg(long)]
        report_out: Option<PathBuf>,
    },
    /// Reconcile an overlapping Live JSONL capture against PMData files across all 10 fidelity axes.
    Reconcile {
        /// Live capture JSONL path.
        #[arg(long)]
        live: PathBuf,
        /// PMData Yes/Up L2 Parquet path.
        #[arg(long)]
        yes_l2: PathBuf,
        /// Optional PMData No/Down L2 Parquet path (required for `full` mode).
        #[arg(long)]
        no_l2: Option<PathBuf>,
        /// PMData trades Parquet path.
        #[arg(long)]
        trades: PathBuf,
        /// Optional `markets.parquet` catalog path.
        #[arg(long)]
        catalog: Option<PathBuf>,
        /// Optional market slug override when filename/parent directory does not encode the slug.
        #[arg(long)]
        market_slug: Option<String>,
        /// L2 mode (`full` or `quota-saving`).
        #[arg(long, value_enum, default_value_t = CliIngestionMode::Full)]
        mode: CliIngestionMode,
        /// Optional Markdown report output path.
        #[arg(long)]
        report_out: Option<PathBuf>,
    },
}

pub(crate) fn run(action: PmdataCommands) -> Result<(), Box<dyn Error>> {
    match action {
        PmdataCommands::Catalog {
            force_refresh,
            filter,
            interval,
            asset,
            from_date,
            to_date,
            limit,
        } => run_catalog(
            force_refresh,
            filter.as_deref(),
            interval.as_deref(),
            &asset,
            from_date.as_deref(),
            to_date.as_deref(),
            limit,
        ),
        PmdataCommands::Download {
            date,
            from_date,
            to_date,
            asset,
            interval,
            market_slug,
            mode,
            include_onchain,
            dry_run,
            yes,
        } => run_download(
            date.as_deref(),
            from_date.as_deref(),
            to_date.as_deref(),
            &asset,
            &interval,
            market_slug.as_deref(),
            mode.into(),
            include_onchain,
            dry_run,
            yes,
        ),
        PmdataCommands::Status => run_status(),
        PmdataCommands::Normalize {
            source,
            input,
            yes_l2,
            no_l2,
            trades,
            onchain_fills,
            market_slug,
            catalog,
            mode,
            resolution_override,
            output,
        } => run_normalize(NormalizeParams {
            source,
            input,
            yes_l2,
            no_l2,
            trades,
            onchain_fills,
            market_slug,
            catalog,
            mode: mode.into(),
            resolution_override: resolution_override.map(Outcome::from),
            output,
        }),
        PmdataCommands::VerifyHistorical {
            samples,
            catalog,
            data_dir,
            report_out,
        } => run_verify_historical(
            &samples,
            catalog.as_deref(),
            data_dir.as_deref(),
            report_out.as_deref(),
        ),
        PmdataCommands::Reconcile {
            live,
            yes_l2,
            no_l2,
            trades,
            catalog,
            market_slug,
            mode,
            report_out,
        } => run_reconcile(
            &live,
            &yes_l2,
            no_l2.as_deref(),
            &trades,
            catalog.as_deref(),
            market_slug.as_deref(),
            mode.into(),
            report_out.as_deref(),
        ),
    }
}

fn run_catalog(
    force_refresh: bool,
    filter: Option<&str>,
    interval: Option<&str>,
    asset: &str,
    from_date: Option<&str>,
    to_date: Option<&str>,
    limit: usize,
) -> Result<(), Box<dyn Error>> {
    let cfg = PmdataConfig::from_env()?;
    let client = PmdataClient::new(cfg)?;
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let outcome = rt.block_on(client.download_catalog(force_refresh, false))?;
    let catalog_path = match &outcome {
        DownloadOutcome::Downloaded { entry, .. } => entry
            .local_cache_path
            .clone()
            .unwrap_or_else(|| client.catalog_cache_path()),
        DownloadOutcome::CacheHit(entry) => entry
            .local_cache_path
            .clone()
            .unwrap_or_else(|| client.catalog_cache_path()),
        _ => client.catalog_cache_path(),
    };
    let needle = filter.unwrap_or("updown").to_ascii_lowercase();
    let entries = read_catalog_parquet_filtered(&catalog_path, Some(&needle))?;
    let intervals: Vec<UpDownInterval> = match interval {
        Some(s) => vec![UpDownInterval::parse(s)?],
        None => vec![UpDownInterval::M5, UpDownInterval::M15, UpDownInterval::H1],
    };
    let from_parsed = from_date.map(parse_utc_date).transpose()?;
    let to_parsed = to_date.map(parse_utc_date).transpose()?;

    let min_date = NaiveDate::from_ymd_opt(2026, 1, 1).unwrap_or_else(|| Utc::now().date_naive());
    let max_date = Utc::now().date_naive();
    let mut filtered: Vec<&CatalogEntry> = Vec::new();
    let mut dates = BTreeSet::new();
    for iv in intervals {
        let mut matched = filter_updown_markets(&entries, asset, iv, from_parsed, to_parsed);
        filtered.append(&mut matched);
        dates.extend(catalog_available_dates(
            &entries,
            asset,
            iv,
            from_parsed.unwrap_or(min_date),
            to_parsed.unwrap_or(max_date),
        ));
    }

    println!(
        "Catalog cached at `{}` ({} filtered rows across {} UTC dates; min={:?}, max={:?})",
        catalog_path.display(),
        filtered.len(),
        dates.len(),
        dates.first(),
        dates.last()
    );
    let mut by_interval: BTreeMap<String, usize> = BTreeMap::new();
    for m in &filtered {
        let iv = infer_interval_from_slug(&m.market_slug)
            .map_or("unknown", UpDownInterval::as_str)
            .to_string();
        let slot = by_interval.entry(iv).or_insert(0_usize);
        *slot = slot.saturating_add(1);
    }
    println!("Counts by interval: {by_interval:?}");

    let shown = limit.min(filtered.len());
    println!("\nSample markets (showing up to {shown}):");
    for m in filtered.iter().take(shown) {
        let q = m.question.as_deref().unwrap_or("-");
        println!(
            "  {} | range={}..={} | yes={:?} | no={:?} | q={}",
            m.market_slug, m.record_start, m.record_end, m.yes_outcome, m.no_outcome, q
        );
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn run_download(
    date: Option<&str>,
    from_date: Option<&str>,
    to_date: Option<&str>,
    asset: &str,
    interval_str: &str,
    market_slug: Option<&str>,
    mode: IngestionMode,
    include_onchain: bool,
    dry_run: bool,
    yes: bool,
) -> Result<(), Box<dyn Error>> {
    let cfg = PmdataConfig::from_env()?;
    let client = PmdataClient::new(cfg)?;
    let interval = UpDownInterval::parse(interval_str)?;
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;

    if let Some(slug) = market_slug {
        let target_date = match date {
            Some(d) => parse_utc_date(d)?,
            None => parse_slug_start_secs(slug)
                .and_then(|secs| i64::try_from(secs).ok())
                .and_then(|secs| chrono::DateTime::from_timestamp(secs, 0))
                .map(|dt| dt.date_naive())
                .ok_or("cannot infer UTC date from --market-slug; pass --date YYYY-MM-DD")?,
        };
        let mut items: Vec<(PmdataDataset, Option<PmdataL2Outcome>)> = vec![
            (PmdataDataset::L2, Some(PmdataL2Outcome::Yes)),
            (PmdataDataset::Trades, None),
        ];
        if mode == IngestionMode::Full {
            items.insert(1, (PmdataDataset::L2, Some(PmdataL2Outcome::No)));
        }
        if include_onchain {
            items.push((PmdataDataset::OnchainFills, None));
        }

        let mut total_planned_cost = 0_u32;
        for (dataset, outcome) in items {
            let res = rt.block_on(client.download_market_file(
                dataset,
                target_date,
                slug,
                outcome,
                dry_run,
            ))?;
            print_download_outcome(&res, &mut total_planned_cost);
        }
        if dry_run {
            println!("Dry-run planned single-market quota cost: {total_planned_cost} units");
        }
        print_manifest_summary(&client)?;
        return Ok(());
    }

    let (start_date, end_date) = match (date, from_date, to_date) {
        (Some(d), None, None) => {
            let parsed = parse_utc_date(d)?;
            (parsed, parsed)
        }
        (None, Some(f), Some(t)) => (parse_utc_date(f)?, parse_utc_date(t)?),
        _ => {
            return Err(
                "specify either --market-slug, --date YYYY-MM-DD, or --from-date YYYY-MM-DD --to-date YYYY-MM-DD"
                    .into(),
            );
        }
    };
    if end_date < start_date {
        return Err("--to-date must not precede --from-date".into());
    }

    let mut dates = Vec::new();
    let mut cur = start_date;
    while cur <= end_date {
        dates.push(cur);
        let Some(next) = cur.succ_opt() else {
            break;
        };
        cur = next;
    }

    let plan = client.plan_day_bundles(asset, interval, &dates, mode, include_onchain)?;
    let remaining_before = plan
        .quota_cap
        .saturating_sub(plan.manifest_charged_before)
        .saturating_sub(plan.manifest_reserved_before);
    println!(
        "Planned {} bundle(s) across {}..={} (mode={:?}, new quota needed: {} units, remaining quota: {}/{})",
        plan.items.len(),
        start_date,
        end_date,
        mode,
        plan.total_quota_needed,
        remaining_before,
        plan.quota_cap
    );
    for item in &plan.items {
        println!(
            "  [{}] {} ({:?}, cost={}, cached={}) -> {}",
            item.utc_date,
            item.url,
            item.dataset,
            item.quota_cost,
            item.already_cached,
            item.cache_path.display()
        );
    }
    if dry_run {
        return Ok(());
    }
    if plan.exceeds_quota {
        return Err(format!(
            "planned bundles need {} quota units but only {remaining_before}/{} remain (refusing to exceed quota cap)",
            plan.total_quota_needed, plan.quota_cap
        )
        .into());
    }
    if plan.total_quota_needed > 0 && !yes {
        return Err(format!(
            "refusing to spend {} quota units on Day Bundles without `--yes` (rerun with `--dry-run` to preview or `--yes` to proceed)",
            plan.total_quota_needed
        )
        .into());
    }

    let mut spent = 0_u32;
    for item in &plan.items {
        let d = parse_utc_date(&item.utc_date)?;
        let res = rt.block_on(client.download_day_bundle(
            item.dataset,
            d,
            &item.asset,
            interval,
            item.outcome,
            false,
        ))?;
        print_download_outcome(&res, &mut spent);
    }
    print_manifest_summary(&client)?;
    Ok(())
}

fn print_download_outcome(outcome: &DownloadOutcome, planned_units: &mut u32) {
    match outcome {
        DownloadOutcome::DryRun(plan) => {
            let cost = if plan.already_cached {
                0
            } else {
                plan.quota_cost
            };
            *planned_units = planned_units.saturating_add(cost);
            println!(
                "  [DRY-RUN] {} -> {} (cached={}, est_cost={})",
                plan.url,
                plan.cache_path.display(),
                plan.already_cached,
                cost
            );
        }
        DownloadOutcome::CacheHit(entry) => {
            println!(
                "  [CACHE HIT] {} -> {} (0 quota charged)",
                entry.entry_key,
                entry
                    .local_cache_path
                    .as_ref()
                    .map_or_else(String::new, |p| p.display().to_string())
            );
        }
        DownloadOutcome::Downloaded {
            entry,
            extracted_files,
        } => {
            println!(
                "  [DOWNLOADED] {} (status={:?}, bytes={}, cost={}, remaining={}, extracted={})",
                entry.entry_key,
                entry.http_status,
                entry.file_size_bytes,
                entry.quota_units_charged,
                entry.remaining_quota_after,
                extracted_files.len()
            );
        }
        DownloadOutcome::NotFound { entry } => {
            println!(
                "  [404 NOT FOUND] {} (0 quota charged, remaining={})",
                entry.entry_key, entry.remaining_quota_after
            );
        }
    }
}

fn run_status() -> Result<(), Box<dyn Error>> {
    let cfg = PmdataConfig::from_env()?;
    let client = PmdataClient::new(cfg)?;
    print_manifest_summary(&client)?;
    let manifest = client.load_manifest()?;
    let mut by_status: BTreeMap<String, usize> = BTreeMap::new();
    let mut by_dataset: BTreeMap<String, usize> = BTreeMap::new();
    for entry in manifest.entries.values() {
        let st = match entry.status {
            EntryStatus::Reserved => "reserved",
            EntryStatus::Completed => "completed",
            EntryStatus::NotFound => "not_found",
            EntryStatus::Forbidden => "forbidden",
            EntryStatus::FailedUncharged => "failed_uncharged",
            EntryStatus::FailedAmbiguous => "failed_ambiguous",
        };
        let slot_st = by_status.entry(st.to_string()).or_insert(0_usize);
        *slot_st = slot_st.saturating_add(1);
        let slot_ds = by_dataset
            .entry(entry.dataset_type.clone())
            .or_insert(0_usize);
        *slot_ds = slot_ds.saturating_add(1);
    }
    println!(
        "Manifest entries ({} total): status={by_status:?}, dataset={by_dataset:?}",
        manifest.entries.len()
    );
    Ok(())
}

fn print_manifest_summary(client: &PmdataClient) -> Result<(), Box<dyn Error>> {
    let manifest = client.load_manifest()?;
    println!(
        "PMData Quota Manifest (`{}`): charged={} | reserved={} | remaining={} / cap={}",
        client.manifest().path().display(),
        manifest.charged_units,
        manifest.reserved_units,
        manifest.remaining_quota(),
        manifest.quota_cap
    );
    Ok(())
}

struct NormalizeParams {
    source: CliNormalizeSource,
    input: Option<PathBuf>,
    yes_l2: Option<PathBuf>,
    no_l2: Option<PathBuf>,
    trades: Option<PathBuf>,
    onchain_fills: Option<PathBuf>,
    market_slug: Option<String>,
    catalog: Option<PathBuf>,
    mode: IngestionMode,
    resolution_override: Option<Outcome>,
    output: PathBuf,
}

fn run_normalize(p: NormalizeParams) -> Result<(), Box<dyn Error>> {
    let events = match p.source {
        CliNormalizeSource::LiveJsonl => {
            let input = p
                .input
                .ok_or("--input <live.jsonl> is required when --source live-jsonl")?;
            normalize_live_jsonl(&input)?
        }
        CliNormalizeSource::Pmdata => {
            let cfg = PmdataConfig::from_env()?;
            let client = PmdataClient::new(cfg)?;
            let yes_l2 = p
                .yes_l2
                .ok_or("--yes-l2 <yes_l2.parquet> is required when --source pmdata")?;
            let trades = p
                .trades
                .ok_or("--trades <trades.parquet> is required when --source pmdata")?;
            let slug = p
                .market_slug
                .or_else(|| infer_slug_from_path(&yes_l2))
                .ok_or("cannot infer --market-slug; pass --market-slug explicitly")?;
            let explicit_catalog = p.catalog.is_some();
            let catalog_path = p.catalog.unwrap_or_else(|| client.catalog_cache_path());
            let catalog_entry =
                find_catalog_entry_if_present(&catalog_path, &slug, explicit_catalog)?;
            let files = PmdataMarketFiles {
                market_slug: slug,
                yes_l2_path: yes_l2,
                no_l2_path: p.no_l2,
                trades_path: Some(trades),
                onchain_fills_path: p.onchain_fills,
            };
            normalize_pmdata_market(
                &files,
                p.mode,
                catalog_entry.as_ref(),
                p.resolution_override,
            )?
            .events
        }
    };

    for ev in &events {
        ev.validate_provenance_invariants()?;
    }
    write_canonical_jsonl(&p.output, &events)?;

    let mut counts: BTreeMap<&'static str, usize> = BTreeMap::new();
    let mut flags: BTreeMap<String, usize> = BTreeMap::new();
    for ev in &events {
        let src_tag = match ev.provenance.source {
            DataSource::LiveJsonl => "live_jsonl",
            DataSource::Pmdata => "pmdata",
        };
        let slot = counts.entry(src_tag).or_insert(0_usize);
        *slot = slot.saturating_add(1);
        for f in &ev.provenance.quality_flags {
            let k = format!("{f:?}");
            let fslot = flags.entry(k).or_insert(0_usize);
            *fslot = fslot.saturating_add(1);
        }
    }
    println!(
        "Wrote {} canonical events to `{}` (sources={counts:?}, quality_flags={flags:?})",
        events.len(),
        p.output.display()
    );
    Ok(())
}

fn run_verify_historical(
    samples: &[String],
    catalog_opt: Option<&Path>,
    data_dir_opt: Option<&Path>,
    report_out: Option<&Path>,
) -> Result<(), Box<dyn Error>> {
    if samples.is_empty() {
        return Err("provide at least 1 market slug via --samples <slug1> <slug2> ...".into());
    }
    let cfg = PmdataConfig::from_env()?;
    let cache_dir = data_dir_opt
        .map(Path::to_path_buf)
        .unwrap_or_else(|| cfg.cache_dir.clone());
    let client = PmdataClient::new(PmdataConfig {
        cache_dir: cache_dir.clone(),
        ..cfg
    })?;
    let explicit_catalog = catalog_opt.is_some();
    let catalog_path = catalog_opt
        .map(Path::to_path_buf)
        .unwrap_or_else(|| client.catalog_cache_path());
    let manifest = client.load_manifest()?;

    let mut audits = Vec::with_capacity(samples.len());
    for slug in samples {
        let files = locate_cached_market_files(&client, &manifest, slug)?;
        let catalog_entry = find_catalog_entry_if_present(&catalog_path, slug, explicit_catalog)?;
        let audit = audit_pmdata_historical_market(&files, catalog_entry.as_ref())?;
        println!(
            "Verified historical `{}`: yes_l2={} (books={}, pc={}, mr={}) | no_l2={} (books={}, pc={}) | trades={} (missing_id={}) | exact_comp_snaps={}/{}",
            audit.market_slug,
            audit.yes_l2_rows,
            audit.yes_books,
            audit.yes_price_changes,
            audit.yes_market_resolved,
            audit.no_l2_rows,
            audit.no_books,
            audit.no_price_changes,
            audit.trades_rows,
            audit.trades_missing_trade_id,
            audit.yes_no_exact_mirror_snapshots,
            audit.yes_no_matched_book_snapshots,
        );
        audits.push(audit);
    }

    let report = HistoricalPhase0Report {
        generated_utc: Utc::now().to_rfc3339(),
        quota_units_consumed: manifest.charged_units,
        verdict: FidelityVerdict::HistoricalSchemaVerifiedOnly,
        disclaimer: "Historical PMData schema, complementarity, and book reconstruction verified ONLY on pre-cutoff (`<= 2026-11-03`) historical markets. Live JSONL vs. PMData equivalence remains UNVERIFIED pending overlapping live 5m and 15m captures (`2026-11-22..25` live captures blocked by free-tier 21-day cutoff; zero live 15m captures exist).".to_string(),
        markets: audits,
    };
    let md = render_historical_phase0_markdown(&report);
    if let Some(out) = report_out {
        if let Some(parent) = out.parent()
            && !parent.as_os_str().is_empty()
        {
            fs::create_dir_all(parent)?;
        }
        fs::write(out, &md)?;
        println!("Wrote historical Phase 0 report to `{}`", out.display());
    } else {
        println!("\n{md}");
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn run_reconcile(
    live_path: &Path,
    yes_l2_path: &Path,
    no_l2_path: Option<&Path>,
    trades_path: &Path,
    catalog_opt: Option<&Path>,
    market_slug_opt: Option<&str>,
    mode: IngestionMode,
    report_out: Option<&Path>,
) -> Result<(), Box<dyn Error>> {
    let cfg = PmdataConfig::from_env()?;
    let client = PmdataClient::new(cfg)?;
    let slug = market_slug_opt
        .map(str::to_string)
        .or_else(|| infer_slug_from_path(yes_l2_path))
        .ok_or(
            "cannot infer market slug from --yes-l2 filename/parent directory; pass --market-slug",
        )?;
    let explicit_catalog = catalog_opt.is_some();
    let catalog_path = catalog_opt
        .map(Path::to_path_buf)
        .unwrap_or_else(|| client.catalog_cache_path());
    let catalog_entry = find_catalog_entry_if_present(&catalog_path, &slug, explicit_catalog)?;
    let files = PmdataMarketFiles {
        market_slug: slug,
        yes_l2_path: yes_l2_path.to_path_buf(),
        no_l2_path: no_l2_path.map(Path::to_path_buf),
        trades_path: Some(trades_path.to_path_buf()),
        onchain_fills_path: None,
    };
    let res = reconcile_live_vs_pmdata(live_path, &files, catalog_entry.as_ref(), mode)?;
    let overall_verdict = res.verdict;
    let report = LiveVsPmdataReport {
        generated_utc: Utc::now().to_rfc3339(),
        matching_markets: 1,
        overall_verdict,
        results: vec![res],
    };
    let md = render_reconcile_markdown(&report);
    if let Some(out) = report_out {
        if let Some(parent) = out.parent()
            && !parent.as_os_str().is_empty()
        {
            fs::create_dir_all(parent)?;
        }
        fs::write(out, &md)?;
        println!(
            "Wrote live-vs-PMData reconciliation report ({overall_verdict:?}) to `{}`",
            out.display()
        );
    } else {
        println!("{md}");
    }
    Ok(())
}

fn locate_cached_market_files(
    client: &PmdataClient,
    manifest: &historical_data::QuotaManifest,
    slug: &str,
) -> Result<PmdataMarketFiles, Box<dyn Error>> {
    let date: NaiveDate = parse_slug_start_secs(slug)
        .and_then(|secs| i64::try_from(secs).ok())
        .and_then(|secs| chrono::DateTime::from_timestamp(secs, 0))
        .map(|dt| dt.date_naive())
        .ok_or_else(|| format!("cannot infer UTC date from sample slug `{slug}`"))?;

    let lookup = |dataset: PmdataDataset, outcome: Option<PmdataL2Outcome>| {
        let key = market_entry_key(dataset, date, slug, outcome);
        if let Some(p) = manifest
            .get_completed(&key)
            .and_then(|e| e.local_cache_path.clone())
            .filter(|p| p.is_file())
        {
            return Some(p);
        }
        let disk_path = client.market_cache_path(dataset, date, slug, outcome);
        if disk_path.is_file() {
            return Some(disk_path);
        }
        // Fallback: check extracted Day Bundle directories (`<cache_dir>/extracted/<YYYY-MM-DD>/*/`).
        let extracted_date_dir = client
            .config()
            .cache_dir
            .join("extracted")
            .join(date.format("%Y-%m-%d").to_string());
        if let Ok(entries) = fs::read_dir(&extracted_date_dir) {
            let suffix = match (dataset, outcome) {
                (PmdataDataset::L2, Some(PmdataL2Outcome::No)) => "_no",
                (PmdataDataset::L2, _) => "_yes",
                _ => "",
            };
            let needle_tag = format!("_{}", dataset.as_str());
            for dir_ent in entries.flatten() {
                let p = dir_ent.path();
                if !p.is_dir() {
                    continue;
                }
                let dir_name = dir_ent.file_name().to_string_lossy().to_string();
                if !dir_name.contains(&needle_tag) {
                    continue;
                }
                if dataset == PmdataDataset::L2 {
                    let expect_no = outcome == Some(PmdataL2Outcome::No);
                    if expect_no != dir_name.ends_with("_no") {
                        continue;
                    }
                }
                for candidate in [
                    p.join(format!("{slug}{suffix}.parquet")),
                    p.join(format!("{slug}.parquet")),
                ] {
                    if candidate.is_file() {
                        return Some(candidate);
                    }
                }
            }
        }
        None
    };

    let yes_l2_path = lookup(PmdataDataset::L2, Some(PmdataL2Outcome::Yes)).ok_or_else(|| {
        format!(
            "missing cached yes_l2 Parquet for `{slug}` (checked manifest + `{}`)",
            client
                .market_cache_path(PmdataDataset::L2, date, slug, Some(PmdataL2Outcome::Yes))
                .display()
        )
    })?;
    let no_l2_path = lookup(PmdataDataset::L2, Some(PmdataL2Outcome::No));
    let trades_path = lookup(PmdataDataset::Trades, None);
    let onchain_fills_path = lookup(PmdataDataset::OnchainFills, None);

    Ok(PmdataMarketFiles {
        market_slug: slug.to_string(),
        yes_l2_path,
        no_l2_path,
        trades_path,
        onchain_fills_path,
    })
}

fn find_catalog_entry_if_present(
    catalog_path: &Path,
    slug: &str,
    explicit: bool,
) -> Result<Option<CatalogEntry>, Box<dyn Error>> {
    if !catalog_path.is_file() {
        if explicit {
            return Err(format!(
                "explicit --catalog file `{}` does not exist",
                catalog_path.display()
            )
            .into());
        }
        return Ok(None);
    }
    let rows = read_catalog_parquet_filtered(catalog_path, Some(slug))?;
    Ok(rows.into_iter().find(|r| r.market_slug == slug))
}

fn infer_slug_from_path(path: &Path) -> Option<String> {
    if let Some(parent_name) = path
        .parent()
        .and_then(|p| p.file_name())
        .and_then(|s| s.to_str())
        && parent_name.contains("-updown-")
    {
        return Some(parent_name.to_string());
    }
    let stem = path.file_stem().and_then(|s| s.to_str())?;
    if let Some(stripped) = stem
        .strip_suffix("_yes")
        .or_else(|| stem.strip_suffix("_no"))
        && stripped.contains("-updown-")
    {
        return Some(stripped.to_string());
    }
    if stem.contains("-updown-") {
        return Some(stem.to_string());
    }
    None
}
