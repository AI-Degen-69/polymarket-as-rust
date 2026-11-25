# `pmdata verify-historical`

Phase 0 audit of already-cached PMData markets. Checks schemas, Yes/No complementarity, and causal
book reconstruction without any live capture. Offline, `0` extra quota.

The verdict is always `HistoricalSchemaVerifiedOnly`. This command does **not** claim live
equivalence. That requires [`reconcile`](reconcile.md) on overlapping data.

## Syntax

```powershell
poly-as pmdata verify-historical --samples <slug> [<slug> ...] [OPTIONS]
```

## Options

| Flag | Default | Meaning |
| :--- | :--- | :--- |
| `--samples <SLUG>...` | required | One or more cached market slugs. Accepts multiple values after one flag. |
| `--catalog <PATH>` | `<cache_dir>/catalog/markets.parquet` | Catalog Parquet for outcome metadata. |
| `--data-dir <PATH>` | `PMDATA_CACHE_DIR` or `data/pmdata` | Cache root to search. |
| `--report-out <PATH>` | none | Markdown report path. Missing parents are created. Without it the report prints. |

## Requirements

- Each slug must be already downloaded in **`full` mode**. The audit needs both `yes_l2` and
  `no_l2`; a slug without a No Parquet fails with
  `Phase 0 audit for '<slug>' requires no_l2_path`.
- The slug must end in epoch seconds (`btc-updown-<interval>-<epoch>`), because the audit derives the
  UTC date from it.
- The catalog must contain the market, because outcome mapping is validated from catalog labels.

File lookup order per dataset: manifest `completed` entry → `data/pmdata/markets/<date>/<dataset>/` →
extracted Day Bundle directory `data/pmdata/extracted/<date>/*_<dataset>[_yes|_no]/`.

## Checks Performed

| Area | What is measured |
| :--- | :--- |
| Schema | Rows, books, price changes, tick changes, resolutions per side. |
| Complementarity | Yes/No snapshot pairs, exact `1 - p` mirrors, mirror BBO and size mismatches. |
| Reconstruction | Causal book rebuild checkpoints and how many match the exact mirror. |
| Trades | Rows, missing `trade_id`, duplicates filtered, distinct outcome labels, trades with a prior book. |
| Numeric fidelity | Max float price/size rounding delta, sub-tick events, crossed snapshots. |
| Timing | Non-monotonic L2 timestamps, provider latency distribution. |
| Resolution | Explicit `market_resolved` presence and winning label. |

## Output

Per market:

```text
Verified historical `btc-updown-15m-1793146500`: yes_l2=18422 (books=96, pc=18120, mr=1) | no_l2=18398 (books=96, pc=18201) | trades=214 (missing_id=0) | exact_comp_snaps=96/96
```

Then a Markdown report. With `--report-out`, the file is written and one confirmation line prints.

Report header fields:

- `quota_units_consumed`: cumulative `charged` units in the manifest at run time.
- `verdict`: `HistoricalSchemaVerifiedOnly`.
- `disclaimer`: names the audited UTC date scope and sample count, and states that live-vs-PMData
  equivalence stays unverified.

## Example

```powershell
poly-as pmdata verify-historical `
  --samples btc-updown-5m-1791417900 btc-updown-5m-1793145900 btc-updown-15m-1793146500 `
  --report-out docs/pmdata-phase0-historical-report.md
```

## Notes

- A market in the resolution gap (`2026-08-07`–`2026-11-17`) shows `mr=0` and is flagged
  `MissingMarketResolved`. That is a provider gap, not an audit failure.
- `trades_missing_trade_id > 0` is expected for markets traded before `2026-10-12`.
- `yes_no_matched_book_snapshots` below `yes_no_reconstructed_checkpoints` means the reconstruction
  path saw snapshots the raw mirror check could not pair; the mismatch counts name the difference.
- This command spends nothing and writes nothing unless `--report-out` is given.
