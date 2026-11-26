# Hybrid BTC 5m / 15m Historical Data Pipeline

Per-command reference: [`docs/pmdata/README.md`](pmdata/README.md).

## 1. Architecture Overview

The pipeline unifies two distinct data sources without erasing clock, precision, or execution-fidelity differences:

1. **Live JSONL Recorder (`poly-as feeds --discover`) — Ground Truth**
   - Captures local QPC monotonic nanoseconds (`mono_ns`), `clock_sync` offsets (`offset_ms`, `clob_offset_ms`, `binance_offset_ms`), 10-level L2 snapshots (`poly_book`), quote/level updates (`poly_price_change`), public trades (`poly_trade`), tick transitions (`poly_tick_size`), Binance spot ticks (`binance_tick`), and Chainlink oracle TWAP ticks (`polybolt_event`). Note: `pmdata normalize --source live-jsonl` currently preserves market metadata, `clock_sync`, `binance_tick`, and `poly_*` events while skipping `polybolt_event`, `feed_health`, and `feeds_summary`.
   - Legacy `RawJsonlEnvelope` replay paths remain untouched for 100% backward compatibility.
2. **PMData Historical Archive (`https://pmdata.dev`) — Broad Historical Coverage**
   - Downloads markets catalog metadata (`markets.parquet`, `0` quota), single-market Parquet files (`1` unit/request), and compressed Day Bundle ZIPs (`5m=72`, `15m=24`, `1h=6` units) under a persistent exclusive-lock quota ledger (`data/pmdata/manifest.json`) strictly bounded to **650 units**.
   - Preserves floating-point prices (`[0, 1]`), exchange UTC microseconds (`timestamp`), and provider capture UTC microseconds (`local_timestamp`) without equating `local_timestamp` to `mono_ns` or fabricating `clock_sync`.
3. **Unified Canonical Schema (`CanonicalEvent` v1)**
   - Normalized JSONL (`data/canonical/*.canonical.jsonl`) carries explicit provenance (`DataSource::{LiveJsonl, Pmdata}`, `TimingQuality`, `BookDerivation`, `QualityFlag` list, and separated `CanonicalTimestamp` clocks).
   - Strictly separates incremental quote/book updates (`PriceChange`) from executed trades (`TradePrint`) and on-chain settlements (`OnchainFill`).
   - Automatically detects canonical vs. legacy live JSONL inside `poly-as lifecycle --source replay`, and dynamically scales `Station2Config::epoch_secs` to `300.0` (5m), `900.0` (15m), or `3600.0` (1h) from contract `(expiry_ts_ms - start_ts_ms)`.

## 2. L2 Ingestion Modes (`full` vs. `quota-saving`)

| Mode | Downloaded Files | Down/No Book Origin | Provenance Stamp | Quota Cost (5m / 15m Bundle) |
| :--- | :--- | :--- | :--- | :--- |
| `full` *(default, reference quality)* | `l2` Yes + `l2?outcome=No` + `trades` | Directly observed PMData No L2 stream | `DirectPmdataNo` | `3` single / `216` (5m) or `72` (15m) |
| `quota-saving` | `l2` Yes + `trades` | Complemented from Yes (`1 - p`, Bid↔Ask, Buy↔Sell) ONLY when uncrossed and strictly inside `(0, 1)` | `ComplementaryFromYesL2` + `DerivedComplementaryDownBook` | `2` single / `144` (5m) or `48` (15m) |

## 3. Quota Accounting & Protections (`<= 650` Cap)

- **Reserve-Before-Send Ledger:** Every authenticated download reserves quota in `data/pmdata/manifest.json` under an exclusive `.lock` file before sending any HTTP request.
- **Uncharged Status Releases:** `400`, `401`, `403`, `404`, `429`, and pre-send network errors release the reservation at `0` units charged.
- **Ambiguous Post-Send Failure Protection:** Corrupt bodies or post-send disconnects commit `FailedAmbiguous` conservatively so the client never exceeds `650` even across crashes or restarts.
- **Byte-Range Resume:** Interrupted Day Bundles holding a valid `.part` file resume via `Range: bytes=START-` (`206 Partial Content`) without double-charging previously billed quota.
- **Catalog Is Free (`0` Quota):** `poly-as pmdata catalog` downloads and queries `markets.parquet` at `0` quota cost.

## 4. Research Suitability Matrix

> [!IMPORTANT]
> **Fidelity Boundary:** Historical Phase 0 (`docs/pmdata-phase0-historical-report.md`) verified Parquet schemas, Yes/No complementarity, and causal book reconstruction on pre-cutoff historical markets (`<= 2026-11-03`). **Live JSONL vs. PMData equivalence remains UNVERIFIED** because all 20 existing 5m live captures (`2026-11-22..25`) sit inside PMData's 21-day free-tier restriction, and zero 15m live captures exist yet.

| Research / Strategy Task | Suitable Source | Current Status | Constraints |
| :--- | :--- | :--- | :--- |
| Macro / regime win-rate, spreads, volume, probability dynamics | `pmdata` (`full` or `quota-saving`) | Permitted (schema verified) | Disclose `EstimatedReplayClock`, verify missing `market_resolved` (`2026-08-07..2026-11-17`) with `--resolution-override` when absent |
| Multi-window 5m & 15m signal / inventory curve calibration | `pmdata` (`full` recommended) | Permitted with caveats | Disclose `MissingBinanceSpot`; lead-lag timing sensitivity untested against live clock |
| Sub-100ms FIFO queue priority, wire latency, Binance-to-CLOB lead-lag validation | `live_jsonl` ONLY | Live required | Do NOT use PMData for execution-sensitive conclusions until `reconcile` passes on overlapping 5m & 15m captures |

## 5. Known Provider Limitations (Verified in Phase 0)

1. **Free-Tier 21-Day Cutoff:** Requests within the trailing 21 days return `HTTP 403` (uncharged).
2. **`trade_id` Schema Transition (`2026-10-12`):** Older files omit `trade_id` (e.g., `btc-updown-5m-1791417900` on `2026-10-08` also exhibited `2` rows with null/missing IDs); newer files populate string `trade_id`.
3. **String-Encoded `new_tick_size`:** Observed `LargeUtf8` string column (`"0.001"`) in post-update L2 files (`2026-10-28`); decoded safely alongside float types.
4. **Missing `market_resolved` (`2026-08-07..2026-11-17`):** Observed directly in Phase 0 (`btc-updown-5m-1793145900` included `mr=1`, whereas `btc-updown-15m-1793146500` had `mr=0` and was flagged `MissingMarketResolved`). Supply `--resolution-override up|down` when known.
5. **No `mono_ns`, `clock_sync`, or Binance Spot:** All PMData canonical events carry `mono_ns: null`, `clock_sync: null`, and `MissingBinanceSpot`.

## 6. Windows PowerShell Operator Commands

```powershell
# 1. Configure session API key without storing it in git (.env is gitignored; NEVER commit secrets)
$env:PMDATA_API_KEY = "sk-..."

# 2. Fetch or query the free public markets catalog (0 quota)
cargo run -p cli -- pmdata catalog --asset btc --interval 15m --from-date 2026-10-28 --to-date 2026-11-03 --limit 20

# 3. Check persistent quota ledger
cargo run -p cli -- pmdata status

# 4. Always preview Day Bundle quota costs before downloading
cargo run -p cli -- pmdata download --date 2026-10-28 --asset btc --interval 15m --mode full --dry-run

# 5a. Download a single market (3 quota units in full mode; 2 in quota-saving)
cargo run -p cli -- pmdata download --market-slug btc-updown-15m-1793146500 --mode full

# 5b. Download a full Day Bundle or date range (requires explicit --yes confirmation; optional --include-onchain)
cargo run -p cli -- pmdata download --from-date 2026-10-28 --to-date 2026-10-28 --asset btc --interval 15m --mode full --yes

# 6a. Normalize PMData Parquets into Canonical JSONL (`--trades` is optional for zero-trade markets; supply `--resolution-override` when `market_resolved` is absent)
cargo run -p cli -- pmdata normalize `
  --source pmdata `
  --yes-l2 data/pmdata/markets/2026-10-28/l2/btc-updown-15m-1793146500_yes.parquet `
  --no-l2 data/pmdata/markets/2026-10-28/l2/btc-updown-15m-1793146500_no.parquet `
  --trades data/pmdata/markets/2026-10-28/trades/btc-updown-15m-1793146500.parquet `
  --mode full `
  --output data/canonical/btc-updown-15m-1793146500.canonical.jsonl

# 6b. Normalize a live capture JSONL into Canonical JSONL
cargo run -p cli -- pmdata normalize --source live-jsonl --input logs/captures/<capture>.jsonl --output data/canonical/live.canonical.jsonl

# 7. Replay canonical JSONL (auto-detected) through Lifecycle (`--min-book-quality direct-both` verifies every market has direct Up+Down books)
cargo run -p cli -- lifecycle --source replay `
  --input data/canonical/btc-updown-15m-1793146500.canonical.jsonl `
  --source-filter pmdata `
  --min-book-quality direct-both

# 8a. Cache the three Phase 0 sample markets first if not already downloaded (3 quota units each in `full` mode)
cargo run -p cli -- pmdata download --market-slug btc-updown-5m-1791417900 --mode full
cargo run -p cli -- pmdata download --market-slug btc-updown-5m-1793145900 --mode full
cargo run -p cli -- pmdata download --market-slug btc-updown-15m-1793146500 --mode full

# 8b. Re-verify cached historical Phase 0 samples locally (0 extra quota)
cargo run -p cli -- pmdata verify-historical `
  --samples btc-updown-5m-1791417900 btc-updown-5m-1793145900 btc-updown-15m-1793146500 `
  --report-out docs/pmdata-phase0-historical-report.md

# 9. Future 10-axis Live-vs-PMData reconciliation (once overlapping capture exits 21-day window)
cargo run -p cli -- pmdata reconcile `
  --live logs/captures/<matching-live-capture>.jsonl `
  --yes-l2 data/pmdata/markets/<YYYY-MM-DD>/l2/<slug>_yes.parquet `
  --no-l2 data/pmdata/markets/<YYYY-MM-DD>/l2/<slug>_no.parquet `
  --trades data/pmdata/markets/<YYYY-MM-DD>/trades/<slug>.parquet `
  --mode full `
  --report-out docs/pmdata-live-reconcile-<slug>.md
```
