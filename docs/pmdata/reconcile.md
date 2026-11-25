# `pmdata reconcile`

Compares one live JSONL capture against PMData files for the same market across ten fidelity axes.
This is the only command that tests whether PMData can substitute for a live capture. Offline,
`0` quota.

## Syntax

```powershell
poly-as pmdata reconcile --live <capture.jsonl> --yes-l2 <yes.parquet> --trades <trades.parquet> [OPTIONS]
```

## Options

| Flag | Default | Meaning |
| :--- | :--- | :--- |
| `--live <PATH>` | required | Live capture JSONL for the overlapping window. |
| `--yes-l2 <PATH>` | required | PMData Yes/Up L2 Parquet. |
| `--no-l2 <PATH>` | none | PMData No/Down L2 Parquet. Required in `full` mode. |
| `--trades <PATH>` | required | PMData trades Parquet. |
| `--catalog <PATH>` | `<cache_dir>/catalog/markets.parquet` | Catalog Parquet for outcome metadata. |
| `--market-slug <SLUG>` | inferred | Inferred from the `--yes-l2` filename or parent directory when it contains `-updown-`. |
| `--mode <full\|quota-saving>` | `full` | Comparison mode. |
| `--report-out <PATH>` | none | Markdown report path. Without it the report prints. |

## When It Can Run

The live capture and the PMData files must cover the same market and an overlapping time window.
Because PMData blocks the trailing 21 days, a recent live capture has no PMData counterpart yet.
Download the matching PMData market once the capture ages past that window.

## The Ten Axes

| # | Axis | Fields |
| :--- | :--- | :--- |
| 1 | Event counts by type | `live_events_by_type`, `pmdata_events_by_type` |
| 2 | Book snapshot matching | `matched_snapshots` |
| 3 | Snapshot BBO equality | `snapshot_bbo_mismatches` |
| 4 | Snapshot depth equality | `snapshot_depth_mismatches` |
| 5 | Price-change coverage | `matched_price_changes`, `missing_in_pmdata_price_changes`, `additional_in_pmdata_price_changes` |
| 6 | Trade coverage | `matched_trades`, `missing_in_pmdata_trades`, `additional_in_pmdata_trades` |
| 7 | Duplicate trade filtering | `duplicate_pmdata_trades` |
| 8 | Trade price and size equality | `trade_price_mismatches`, `trade_size_mismatches` |
| 9 | Exchange timestamp distribution | `exchange_ts_diff_ms` (count, min, p50, p95, p99, max) |
| 10 | Reconstructed BBO agreement | `reconstructed_bbo_matches` / `reconstructed_checkpoints` |

Settlement resolution is compared as a gate: `resolution_live` versus `resolution_pmdata`.

## Clock Rules

- PMData `local_timestamp` is never compared to live `mono_ns`. Only exchange UTC timestamps are
  compared.
- Trades are matched inside a `±1000 ms` exchange-timestamp window. Matching is bipartite, so one
  PMData trade cannot satisfy two live trades.
- `resolution_live` and `resolution_pmdata` both come from the last `market_resolved` event in each
  stream. `None` means the stream carried no resolution event.

## Verdicts

| Verdict | Meaning |
| :--- | :--- |
| `Equivalent` | Reserved for exact agreement. The current CLI classifier never returns it. |
| `CompatibleWithLimitations` | No mismatches found, but PMData lacks `mono_ns` and `clock_sync`, so latency conclusions stay limited. |
| `NotEquivalent` | Material discrepancy. Do not substitute PMData for live data. |
| `HistoricalSchemaVerifiedOnly` | Produced by `verify-historical`, not by this command. |

`NotEquivalent` is returned when any of these hold:

- Settlement winners disagree.
- No book or trade coverage overlaps at all.
- Any snapshot BBO or depth mismatch, or any trade price or size mismatch.
- Reconstructed BBO agreement falls below `90%`.
- More than `10%` of live trades are missing from PMData.

Missing price changes or trades alone, with no hard mismatch, yields `CompatibleWithLimitations`.

## Output

```text
## `btc-updown-5m-1793145900` (5m)
- **Verdict:** `CompatibleWithLimitations`
- **Snapshots:** matched=288, bbo_mismatches=0, depth_mismatches=0
- **Price Changes:** matched=9102, missing_in_pmdata=4, additional_in_pmdata=0
- **Trades:** matched=37, missing_in_pmdata=0, additional_in_pmdata=1, duplicates_filtered=2, price_mismatches=0, size_mismatches=0
- **Reconstructed BBO:** matched=288/288
- **Trade Timestamp Diff (pmdata - live, ms):** count=37, min=0ms, p50=1ms, p95=3ms, p99=5ms, max=8ms
- **Event Counts:** live={"book_snapshot": 288, ...} | pmdata={"book_snapshot": 288, ...}
- **Settlement Resolution:** live=Up, pmdata=Up
```

With `--report-out`, the file is written and one line reports the verdict and path.

## Example

```powershell
poly-as pmdata reconcile `
  --live logs/captures/<matching-capture>.jsonl `
  --yes-l2 data/pmdata/markets/2026-11-01/l2/btc-updown-5m-1793145900_yes.parquet `
  --no-l2  data/pmdata/markets/2026-11-01/l2/btc-updown-5m-1793145900_no.parquet `
  --trades  data/pmdata/markets/2026-11-01/trades/btc-updown-5m-1793145900.parquet `
  --mode full --report-out docs/pmdata-live-reconcile-btc-updown-5m-1793145900.md
```

## Current Status

As of Phase 0, no overlapping pair exists. All 20 live 5m captures (`2026-11-22`..`2026-11-25`) sit
inside the 21-day restriction, and no live 15m captures exist. Re-run this command when a matching
capture ages out. Until then, live-vs-PMData equivalence stays **UNVERIFIED**.
