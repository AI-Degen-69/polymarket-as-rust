# `pmdata catalog`

Downloads the free public Polymarket catalog (`markets.parquet`) and queries it. Cost: **`0` quota**.
No API key required.

The catalog supplies the outcome metadata (`yes_outcome` / `no_outcome`) that `normalize`,
`verify-historical`, and `reconcile` need to map PMData `Yes`/`No` to `Up`/`Down`.

## Syntax

```powershell
poly-as pmdata catalog [OPTIONS]
```

## Options

| Flag | Default | Meaning |
| :--- | :--- | :--- |
| `--force-refresh` | off | Re-download `markets.parquet` even when the cached copy is valid. |
| `--filter <SUBSTR>` | `updown` | Case-insensitive substring filter on market slugs. |
| `--interval <5m\|15m\|1h>` | all three | Restrict to one UpDown interval. |
| `--asset <SYMBOL>` | `btc` | Underlying asset. Matches the catalog `series` tag (`updown.<interval>.<asset>`) or the slug prefix. |
| `--from-date <YYYY-MM-DD>` | none | Inclusive lower bound on the market recording window. |
| `--to-date <YYYY-MM-DD>` | none | Inclusive upper bound on the market recording window. |
| `--limit <N>` | `20` | Maximum sample rows printed. |

## Behavior

1. Loads `PmdataConfig::from_env()` and uses `data/pmdata/catalog/markets.parquet`.
2. Downloads the catalog only when the cache is missing, unreadable, or `--force-refresh` is set.
   A valid cache is reused (`[CACHE HIT]`).
3. Filters by slug substring, asset, interval, and the `record_start`/`record_end` overlap test.
4. Prints the cache path, the filtered row count, the covered UTC date span, per-interval counts, and
   up to `--limit` sample rows.

The date range uses *overlap*, not containment: a market whose window straddles `--from-date` is
included.

## Example

```powershell
poly-as pmdata catalog --asset btc --interval 15m --from-date 2026-10-28 --to-date 2026-11-03 --limit 20
```

```text
Catalog cached at `data/pmdata/catalog/markets.parquet` (68 filtered rows across 7 UTC dates; min=Some(2026-10-28), max=Some(2026-11-03))
Counts by interval: {"15m": 68}

Sample markets (showing up to 20):
  btc-updown-15m-1793146500 | range=2026-10-28..=2026-10-28 | yes=Some("Up") | no=Some("Down") | q=BTC up or down 15m ...
```

## Notes

- Without `--from-date` / `--to-date`, `catalog` counts dates over `2026-01-01..today`.
- `--filter` runs before the asset and interval filters. Keep the default `updown` unless you are
  inspecting a different market family; a mismatched filter yields zero rows.
- Cached rows whose `record_start`/`record_end` fail to parse as `YYYY-MM-DD` are dropped silently.
