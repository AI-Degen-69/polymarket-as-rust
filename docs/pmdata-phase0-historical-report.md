# Phase 0 Historical Verification Report (PMData)

- **Generated (UTC):** 2026-11-25T17:02:26.485074800+00:00
- **Quota Consumed:** 9 / 650 units
- **Verdict:** `HistoricalSchemaVerifiedOnly`
- **Fidelity Boundary:** Historical PMData schema, complementarity, and book reconstruction verified ONLY on pre-cutoff (`<= 2026-11-03`) historical markets. Live JSONL vs. PMData equivalence remains UNVERIFIED pending overlapping live 5m and 15m captures (`2026-11-22..25` live captures blocked by free-tier 21-day cutoff; zero live 15m captures exist).

| Market | Interval | Yes L2 Rows | No L2 Rows | Trades | Missing `trade_id` | Yes/No Exact Snap Mirrors | Reconstructed Mirrors | Trades Inside Causal BBO |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `btc-updown-5m-1791417900` | 5m | 202735 | 202735 | 2567 | 2 | 2572/2572 | 125954/125954 | 2358/2565 |
| `btc-updown-5m-1793145900` | 5m | 117174 | 117174 | 1374 | 0 | 1378/1378 | 82594/82594 | 1318/1374 |
| `btc-updown-15m-1793146500` | 15m | 212481 | 212481 | 836 | 0 | 839/839 | 151161/151161 | 797/836 |

## Per-Market Schema, Timing, Precision & Resolution Details

### `btc-updown-5m-1791417900` (5m)
- **Catalog Outcomes:** yes=`Up` / no=`Down`
- **Yes L2 Breakdown:** books=2572, price_changes=200163, tick_size_changes=0, market_resolved=0, non_monotonic_ts=0, crossed_snaps=0
- **No L2 Breakdown:** books=2572, price_changes=200163, crossed_snaps=0
- **Yes vs. No Mirror Residuals:** bbo_price_mismatches=0, depth_size_mismatches=0
- **Trades Breakdown:** rows=2567, missing_trade_id=2, dup_filtered=2, outcomes=["no", "yes"]
- **Float-to-Micro Rounding Residuals:** max_price_delta=0.000000000, max_size_delta=0.000015259, sub_tick_events=0
- **Resolution Event:** explicit_market_resolved=false, winning_outcome_label=<missing>
- **Provider Capture Latency (`local_timestamp - timestamp`, us):** count=202735, min=0, p50=6667, p95=41583, p99=86580, max=957642
- **Observations / Notes:**
  - No `market_resolved` row in L2 (expected in 2026-08-07..2026-11-17 gap)
  - 2 of 2567 trades missing `trade_id` (expected pre-2026-10-12)

### `btc-updown-5m-1793145900` (5m)
- **Catalog Outcomes:** yes=`Up` / no=`Down`
- **Yes L2 Breakdown:** books=1378, price_changes=115793, tick_size_changes=2, market_resolved=1, non_monotonic_ts=0, crossed_snaps=0
- **No L2 Breakdown:** books=1378, price_changes=115793, crossed_snaps=0
- **Yes vs. No Mirror Residuals:** bbo_price_mismatches=0, depth_size_mismatches=0
- **Trades Breakdown:** rows=1374, missing_trade_id=0, dup_filtered=0, outcomes=["yes", "no"]
- **Float-to-Micro Rounding Residuals:** max_price_delta=0.000000000, max_size_delta=0.000015259, sub_tick_events=105
- **Resolution Event:** explicit_market_resolved=true, winning_outcome_label=no
- **Provider Capture Latency (`local_timestamp - timestamp`, us):** count=117174, min=0, p50=6061, p95=15167, p99=63407, max=511405

### `btc-updown-15m-1793146500` (15m)
- **Catalog Outcomes:** yes=`Up` / no=`Down`
- **Yes L2 Breakdown:** books=839, price_changes=211640, tick_size_changes=2, market_resolved=0, non_monotonic_ts=0, crossed_snaps=0
- **No L2 Breakdown:** books=839, price_changes=211640, crossed_snaps=0
- **Yes vs. No Mirror Residuals:** bbo_price_mismatches=0, depth_size_mismatches=0
- **Trades Breakdown:** rows=836, missing_trade_id=0, dup_filtered=0, outcomes=["yes", "no"]
- **Float-to-Micro Rounding Residuals:** max_price_delta=0.000000000, max_size_delta=0.000000477, sub_tick_events=62
- **Resolution Event:** explicit_market_resolved=false, winning_outcome_label=<missing>
- **Provider Capture Latency (`local_timestamp - timestamp`, us):** count=212481, min=0, p50=5568, p95=15809, p99=32105, max=367767
- **Observations / Notes:**
  - No `market_resolved` row in L2 (expected in 2026-08-07..2026-11-17 gap)

## Research Suitability & Open Blockers
- **Verified Historical Properties:** Arrow/Parquet schema decoding (including pre-`2026-10-12` missing `trade_id` and post-update string-encoded `new_tick_size`), 100% Yes/No exact book snapshot complementarity across all audited samples, monotonic L2 ordering, and causal trade-within-BBO alignment.
- **Unverified Equivalence Blockers:** Overlapping Live JSONL vs. PMData reconciliation (`poly-as pmdata reconcile`) could NOT run yet because PMData's free tier blocks the trailing 21 days (`2026-11-22..25`, where all 20 existing 5m live captures live) and no live 15m captures exist yet.
- **Permitted vs. Restricted Uses (Until Reconciliation Passes):** Safe for coarse signal research, probability/resolution dynamics, and exploratory 5m/15m paper runs with explicit `pmdata` provenance disclosure. Do NOT draw execution-sensitive conclusions (sub-100ms queue priority, tick-by-tick fill timing, Binance lead-lag latency) from PMData alone.
