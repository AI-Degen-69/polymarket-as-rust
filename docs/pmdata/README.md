# PMData Historical Data — Usage Guide

`poly-as pmdata` downloads Polymarket historical market data, normalizes it into the canonical
event schema, and audits it against live captures. The crate is `crates/historical-data`.

Architecture, ingestion-mode design, and the research suitability matrix live in
[`docs/hybrid-historical-pipeline.md`](../hybrid-historical-pipeline.md). This folder is the
per-command reference.

## Prerequisites

- Build the CLI: `cargo build --release -p cli` produces `poly-as`.
  Without a built binary, replace `poly-as` with `cargo run -p cli --` in every command below.
- Set `PMDATA_API_KEY` for authenticated downloads. `catalog` and `--dry-run` work without a key.
- Run `pmdata catalog` before `normalize`, `verify-historical`, or `reconcile`. Those commands need
  catalog outcome metadata to map PMData `Yes`/`No` to `Up`/`Down`. The catalog costs 0 quota.
- Every subcommand accepts the global `--color <auto|always|never>` flag.

```powershell
$env:PMDATA_API_KEY = "sk-..."   # session only; .env is gitignored, never commit secrets
```

## Subcommand Map

| Command | Purpose | Quota cost | API key |
| :--- | :--- | :--- | :--- |
| [`catalog`](catalog.md) | Download and query the public `markets.parquet` catalog | `0` | No |
| [`download`](download.md) | Download single-market Parquets or Day Bundle ZIPs | `1`/file or `6`–`72`/bundle | Yes |
| [`status`](status.md) | Print the persistent quota ledger and entry counters | `0` | No |
| [`normalize`](normalize.md) | Convert PMData Parquets or live JSONL into canonical JSONL | `0` | No |
| [`verify-historical`](verify-historical.md) | Phase 0 historical schema/reconstruction audit of cached markets | `0` | No |
| [`reconcile`](reconcile.md) | 10-axis comparison of one overlapping live capture against PMData | `0` | No |

## Environment Variables

| Variable | Default | Meaning |
| :--- | :--- | :--- |
| `PMDATA_API_KEY` | unset | Bearer token for authenticated downloads. Redacted in all logs. |
| `PMDATA_BASE_URL` | `https://api.pmdata.dev/v1` | Download API base URL. |
| `PMDATA_CATALOG_URL` | `https://public-data.pmdata.dev/v1/polymarket/markets.parquet` | Free catalog URL. |
| `PMDATA_CACHE_DIR` | `data/pmdata` | Root for archives, extracted Parquets, and the catalog. |
| `PMDATA_MANIFEST_PATH` | `data/pmdata/manifest.json` | Persistent quota ledger. |
| `PMDATA_QUOTA_CAP` | `650` | Billing-cycle ceiling. Valid range `1..=650`; the hard maximum cannot be raised. |

## Cache Layout

```text
data/pmdata/
  catalog/markets.parquet                         # free catalog
  manifest.json                                   # quota ledger (reserve-before-send)
  manifest.json.lock                              # exclusive lock
  markets/<YYYY-MM-DD>/l2/<slug>_yes.parquet      # single-market Yes L2
  markets/<YYYY-MM-DD>/l2/<slug>_no.parquet       # single-market No L2 (`full` mode)
  markets/<YYYY-MM-DD>/trades/<slug>.parquet      # single-market trades
  markets/<YYYY-MM-DD>/onchain_fills/<slug>.parquet
  bundles/<YYYY-MM-DD>/<asset>-<interval>_<dataset>[_yes|_no].zip
  bundles/<YYYY-MM-DD>/<asset>-<interval>_<dataset>[_yes|_no].zip.part   # resumable partial
  extracted/<YYYY-MM-DD>/<asset>-<interval>_<dataset>[_yes|_no>/<slug>*.parquet
```

## Quota Model

- **Single market:** `1` unit per file request. `full` mode fetches Yes L2 + No L2 + trades = `3`.
  `quota-saving` fetches Yes L2 + trades = `2`. `--include-onchain` adds `1`.
- **Day Bundle:** charged per archive, per UTC date. `--include-onchain` adds one archive at the
  interval's rate.

| Interval | Bundle units | `quota-saving` day | `full` day | `full` + onchain |
| :--- | :--- | :--- | :--- | :--- |
| `5m` | `72` | `144` | `216` | `288` |
| `15m` | `24` | `48` | `72` | `96` |
| `1h` | `6` | `12` | `18` | `24` |

- Every request reserves quota in `manifest.json` under an exclusive lock before the HTTP send.
- `400`, `401`, `403`, `404`, and `429` responses release the reservation at `0` charged.
- Corrupt bodies and post-send disconnects commit as `failed_ambiguous` (charged conservatively).
  This keeps the ledger under `650` even after a crash.
- Interrupted Day Bundles resume from a valid `.part` file via `Range: bytes=` and do not pay twice.

## End-to-End Workflow

```powershell
# 1. Free catalog and quota ledger
poly-as pmdata catalog --asset btc --interval 15m --from-date 2026-10-28 --to-date 2026-11-03
poly-as pmdata status

# 2. Always preview Day Bundle costs before spending
poly-as pmdata download --date 2026-10-28 --asset btc --interval 15m --mode full --dry-run

# 3. Download (Day Bundles need --yes; single markets do not)
poly-as pmdata download --from-date 2026-10-28 --to-date 2026-10-28 --asset btc --interval 15m --mode full --yes

# 4. Normalize one market into canonical JSONL
poly-as pmdata normalize --source pmdata `
  --yes-l2 data/pmdata/markets/2026-10-28/l2/btc-updown-15m-1793146500_yes.parquet `
  --no-l2  data/pmdata/markets/2026-10-28/l2/btc-updown-15m-1793146500_no.parquet `
  --trades  data/pmdata/markets/2026-10-28/trades/btc-updown-15m-1793146500.parquet `
  --mode full --output data/canonical/btc-updown-15m-1793146500.canonical.jsonl

# 5. Replay the canonical stream (5m/15m/1h epoch is auto-detected)
poly-as lifecycle --source replay `
  --input data/canonical/btc-updown-15m-1793146500.canonical.jsonl `
  --source-filter pmdata --min-book-quality direct-both

# 6. Audit what you cached (0 quota, offline)
poly-as pmdata verify-historical `
  --samples btc-updown-15m-1793146500 --report-out docs/pmdata-phase0-historical-report.md
```

## Fidelity Boundary

Read this before drawing execution-sensitive conclusions from PMData data.

- **21-day free-tier cutoff:** requests inside the trailing 21 days return `HTTP 403` (uncharged).
- **No local clocks:** PMData events always carry `mono_ns: null` and `clock_sync: null`, and are
  stamped `EstimatedReplayClock` plus `MissingBinanceSpot`. Do not use PMData for wire-latency or
  queue-priority research. That work requires `live_jsonl`.
- **Resolution gap (`2026-08-07`–`2026-11-17`):** some markets lack `market_resolved`. Supply
  `--resolution-override up|down` when the winner is known; a conflicting override is rejected.
- **Live-vs-PMData equivalence is unverified** until `reconcile` runs on an overlapping capture that
  has aged past the 21-day window.

## Troubleshooting

| Symptom | Cause | Fix |
| :--- | :--- | :--- |
| `HTTP 403`, `0 quota charged` | Date inside the trailing 21-day window | Wait until the capture ages out, or use a live capture |
| `missing catalog outcome metadata` | No `markets.parquet` at the expected path | Run `poly-as pmdata catalog` (or pass `--catalog`) |
| `ambiguous catalog outcomes` | `yes_outcome` and `no_outcome` map to the same side | Inspect the catalog row; do not guess the mapping |
| `cannot infer --market-slug` | Filename and parent directory lack an updown slug | Pass `--market-slug` explicitly |
| `cannot infer UTC date from sample slug` | Slug has no trailing epoch seconds | Pass slugs shaped `btc-updown-<interval>-<epoch_secs>` |
| `refusing to spend N quota units ... without --yes` | Day Bundle spend needs confirmation | Re-run with `--dry-run` to preview, `--yes` to proceed |
| `refusing to exceed quota cap` | Plan needs more than the remaining quota | Use `quota-saving`, fewer dates, or a larger interval |
| `resolution override ... conflicts` | Override disagrees with a PMData `market_resolved` row | Drop the override; trust the observed event |
