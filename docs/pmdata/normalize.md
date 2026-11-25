# `pmdata normalize`

Converts PMData Parquet files, or a live capture JSONL, into `CanonicalEvent` v1 JSONL. Costs `0`
quota and needs no API key.

Normalization never invents data. PMData rows keep only their own clocks, and every derivation is
recorded as a provenance stamp or a quality flag.

## Syntax

```powershell
# PMData Parquets
poly-as pmdata normalize --source pmdata --yes-l2 <yes.parquet> [OPTIONS] --output <out.jsonl>

# Live capture JSONL
poly-as pmdata normalize --source live-jsonl --input <capture.jsonl> --output <out.jsonl>
```

## Options

| Flag | Applies to | Default | Meaning |
| :--- | :--- | :--- | :--- |
| `--source <pmdata\|live-jsonl>` | both | required | Input format. |
| `--output <PATH>` | both | required | Canonical JSONL destination. Written atomically. |
| `--input <PATH>` | `live-jsonl` | required | Live capture file. |
| `--yes-l2 <PATH>` | `pmdata` | required | PMData Yes/Up L2 Parquet. |
| `--no-l2 <PATH>` | `pmdata` | none | PMData No/Down L2 Parquet. Required in `full` mode. |
| `--trades <PATH>` | `pmdata` | none | PMData trades Parquet. Omit for markets with zero trades. |
| `--onchain-fills <PATH>` | `pmdata` | none | PMData on-chain fills Parquet. |
| `--market-slug <SLUG>` | `pmdata` | inferred | Inferred from the `--yes-l2` parent directory or filename when it contains `-updown-`. |
| `--catalog <PATH>` | `pmdata` | `<cache_dir>/catalog/markets.parquet` | Catalog Parquet used for outcome mapping. |
| `--mode <full\|quota-saving>` | `pmdata` | `full` | `full` needs `--no-l2`; `quota-saving` derives the No book from Yes. |
| `--resolution-override <up\|down>` | `pmdata` | none | Authoritative winner when the stream lacks `market_resolved`. |

## Behavior

1. **Outcome mapping.** `Yes`/`No` are mapped from the catalog row's `yes_outcome`/`no_outcome`
   labels, never assumed. A missing catalog row, missing labels, or a label collision aborts the run.
2. **Window.** Start and expiry come from the slug's epoch seconds plus the interval duration, with
   the catalog `market_start_ms`/`market_end_ms` as a fallback when the slug has no epoch.
3. **Events.** L2 rows become `book_snapshot`, `price_change`, `tick_size_change`, and
   `market_resolution`. Trades become `trade_print`. On-chain fills become `onchain_fill`.
   Duplicate trade rows are dropped and flagged.
4. **Quota-saving derivation.** The Down book is built from Yes as `1 - p` with Bid↔Ask and
   Buy↔Sell swapped, only when the result is valid and strictly inside `(0, 1)`. Each derived event
   carries `ComplementaryFromYesL2` plus `DerivedComplementaryDownBook`.
5. **Resolution.** A PMData `market_resolved` row wins. Otherwise `--resolution-override` supplies
   the winner as `external_override` at expiry. An override that contradicts an observed resolution
   is rejected.
6. **Validation.** Every event passes `validate_provenance_invariants()` before the file is written.
   PMData events must have `mono_ns == null`, `clock_sync == null`, and a `timing_estimation`
   disclosure. A violation aborts the run with a non-zero exit.
7. **Write.** The JSONL is written atomically to `--output`, then a summary is printed.

## Output

```text
Wrote 18422 canonical events to `data/canonical/btc-updown-15m-1793146500.canonical.jsonl`
  (sources={"pmdata": 18422}, quality_flags={"MissingBinanceSpot": 18422, "EstimatedReplayClock": 18422, ...})
```

Each line is one `CanonicalEvent`:

```json
{
  "schema_version": 1,
  "provenance": {
    "source": "pmdata",
    "original_event_type": "book",
    "market_slug": "btc-updown-15m-1793146500",
    "timing_quality": "exchange_and_provider_utc",
    "book_derivation": "direct_pmdata_yes",
    "normalization_version": 1,
    "quality_flags": ["missing_binance_spot", "estimated_replay_clock"]
  },
  "timestamp": {
    "exchange_ts_ms": 1793146500123,
    "exchange_ts_us": 1793146500123456,
    "provider_local_ts_us": 1793146500987654,
    "unix_ts_ms": null,
    "mono_ns": null,
    "clock_sync": null,
    "replay_order_ts_ms": 1793146500123,
    "timing_estimation": { "method": "exchange_ts_us_to_ms_floor", "uncertainty_note": "..." }
  },
  "kind": { "type": "book_snapshot", "outcome": "Up", "bids": [], "asks": [] }
}
```

`kind.type` is one of `market_metadata`, `clock_sync`, `book_snapshot`, `price_change`,
`trade_print`, `tick_size_change`, `best_bid_ask`, `market_resolution`, `onchain_fill`,
`binance_spot`. `Outcome` values serialize as `Up`/`Down`, not lowercase.

## Quality Flags

| Flag | Meaning |
| :--- | :--- |
| `missing_trade_id` | Trade has no exchange `trade_id` (common before `2026-10-12`). |
| `missing_market_resolved` | Stream ended without a resolution event. |
| `derived_complementary_down_book` | Down book derived from Yes in `quota-saving` mode. |
| `missing_binance_spot` | No Binance reference feed, so `spot_usd`/`strike_usd` are unavailable. |
| `duplicate_trade_filtered` | Duplicate trade row was dropped. |
| `sub_tick_price` | Price was not an exact tick multiple. |
| `below_min_order_size` | Converted size fell below the market minimum. |
| `crossed_book_observed` | Source produced `best_bid >= best_ask`. |
| `missing_outcome_metadata` | Catalog lacked explicit outcome labels. |
| `estimated_replay_clock` | `replay_order_ts_ms` derived from UTC because `mono_ns` is absent. |
| `float_conversion_rounding` | Rounding beyond 1e-6 micro-units was required. |
| `daily_boundary_snapshot` | PMData synthetic `00:00 UTC` boundary snapshot. |

## Replay After Normalize

```powershell
poly-as lifecycle --source replay `
  --input data/canonical/btc-updown-15m-1793146500.canonical.jsonl `
  --source-filter pmdata --min-book-quality direct-both
```

- `--source` is auto-detected from the `schema_version` + `provenance` envelope.
- `--source-filter <all\|live\|pmdata>` restricts events by origin. `pmdata` also lets replay run
  without any live capture present.
- `--min-book-quality <any\|direct-yes\|direct-both>` rejects synthetic books.
  `direct-both` refuses `ComplementaryFromYesL2` and therefore needs `--mode full`.
- `epoch_secs` scales automatically from the contract window: `300` (5m), `900` (15m), `3600` (1h).

## Examples

```powershell
# Full mode, explicit catalog, resolved gap override
poly-as pmdata normalize --source pmdata `
  --yes-l2 data/pmdata/markets/2026-10-28/l2/btc-updown-15m-1793146500_yes.parquet `
  --no-l2  data/pmdata/markets/2026-10-28/l2/btc-updown-15m-1793146500_no.parquet `
  --trades  data/pmdata/markets/2026-10-28/trades/btc-updown-15m-1793146500.parquet `
  --catalog data/pmdata/catalog/markets.parquet `
  --mode full --resolution-override up `
  --output data/canonical/btc-updown-15m-1793146500.canonical.jsonl

# Live capture, no Parquets involved
poly-as pmdata normalize --source live-jsonl `
  --input logs/captures/<capture>.jsonl --output data/canonical/live.canonical.jsonl
```

## Notes

- `--source live-jsonl` preserves `mono_ns`, `unix_ts_ms`, `clock_sync`, and Binance ticks. It skips
  `polybolt_event`, `feed_health`, and `feeds_summary` rows.
- Row-level `market_slug` must agree with `--market-slug` (or the inferred slug). A mismatch aborts.
- Pre-`2026-10-28` L2 files may encode `new_tick_size` as a string (`"0.001"`). The reader decodes
  both forms.
