# Sweep Results Re-Evaluation: Corrected Telemetry, Repaired Gates G3–G6, and Promotion of Config #13

> [!IMPORTANT]
> **Follow-Up Promotion Notice:** A subsequent `9`-configuration boundary sweep (`logs/sweeps/25-11_21-55-50Z_boundary-9runs_20caps-226w`) promoted **Config `#6/9` (`max_entry_price = 0.65, entry_cutoff_secs = 120, tier1_hedge_trigger_price = off`)** to `DEFAULTS.json` as the provisional research baseline (`79.9` composite score vs `77.7` for Config `#13` / `#3/9`). See [boundary-sweep-selection-report.md](docs/boundary-sweep-selection-report.md).

> [!IMPORTANT]
> **Supersession Notice:** This report **completely supersedes** the earlier `docs/sweep-results-investigation.md`. The prior investigation relied on broken G3–G6 survival gates and synthetic volume (`$9.80/fill`). Every metric, ranking, and gate verdict below is recomputed from the underlying event stream using the repaired Rust accounting telemetry and the repaired G3–G6 gates in [sweep-params.ps1](scripts/sweep-params.ps1).

- **Dataset:** `C:\Users\Tiger\Agents\Projects\AI Trading\polymarket-as-rust\logs\sweeps\25-11_16-21-55Z_18runs_20caps-226w` (`18` configurations × `20` captures = `360` JSONL files, `226` 5-minute windows, `QueueDecay=conservative`)
- **Sweep Summary Artifact:** `logs/sweeps/25-11_16-21-55Z_18runs_20caps-226w/summary.txt` *(git-ignored local sweep output)* *(original sweep output using pre-fix gates)*
- **Verification Standard:** [strategy-verification-spec.md](docs/strategy-verification-spec.md)

---

## 1. Accounting Telemetry Validation & Event-Level Reconstruction

### 1.1 Validation of New Telemetry Fields

Five new accounting fields (`#[serde(default)]`) were added to `PaperLedger` ([accounting.rs](crates/paper-sim/src/accounting.rs#L140-L398)) and exported via `LifecycleSummaryEvent` ([lifecycle_report.rs](crates/cli/src/lifecycle_report.rs#L333-L371)) and `LivePaperSimulator::finalize_stats` ([lifecycle_cmd.rs](crates/cli/src/lifecycle_cmd.rs#L2532-L2554)):

| Field | Writer(s) | Validation Result |
| --- | --- | --- |
| `total_shares_bought_micro` | `record_fill` (`OrderSide::Buy`) and `record_taker_flatten_with_merge` (`OrderSide::Buy`) | Sum of all BUY shares across passive fills and mandatory buy-side flattens. |
| `volume_in_micro` | `record_fill` (`OrderSide::Buy`) and `record_taker_flatten_with_merge` (`OrderSide::Buy`) | **No double-counting:** `record_fill` only handles passive FIFO fills (`on_fill`), while `record_taker_flatten_with_merge` only handles `ExecuteFlattenFak`. Neither calls the other. Both use exact integer micro-USDC truncation (`(shares * price) / 1_000_000`). |
| `total_merge_gain_micro` | `execute_pair_merge` | Signed net merge P&L across all pair merges (`redemption_micro - cost_up_deducted - cost_dn_deducted`). |
| `positive_merge_gain_micro` | `execute_pair_merge` | Sum of `realized_gain_micro >= 0` across all merges. |
| `negative_merge_loss_micro` | `execute_pair_merge` | Sum of `|realized_gain_micro|` where `realized_gain_micro < 0`. Reconciles identically: `total_merge_gain_micro == positive_merge_gain_micro - negative_merge_loss_micro`. |

**Determinism and test proof:**
- Unit vector `test_vector_9_volume_in_and_signed_merge_gain_tracking` ([vectors.rs](crates/paper-sim/tests/vectors.rs#L409-L468)) verifies buy notional (`$30.50`) and signed merge splits (`+$1.00`, `-$1.50`, net `-$0.50`).
- Replay determinism test `replay_is_deterministic_across_runs` ([lifecycle_cmd.rs](crates/cli/src/lifecycle_cmd.rs)) asserts bit-identical output across repeated runs for all five fields.

### 1.2 Why No Sweep Rerun Was Needed

Event-level reconstruction across all `360` original JSONL files succeeded with **`0` reconciliation mismatches**:
- Every file already emits full `"event":"fill"` (`filled_shares_micro`, `price_micro`, `side`), `"event":"flatten"` (`size_micro`, `price_micro`, `side`, `fee_micro`), `"event":"merge"` (`realized_gain_micro`), `"event":"window_end"`, and `"event":"summary"` rows.
- Exact identities hold in all `360` files:
  - `count(fill) == summary.fills_count` and `count(merge) == summary.merges_count`
  - `sum(merge.realized_gain_micro) + summary.settlement_pnl_micro - summary.fees_paid_micro == summary.realized_pnl_micro`
  - `sum(flatten.fee_micro) == summary.fees_paid_micro`
- **Buy-side flatten correction (`9` events across the dataset):** Earlier ad-hoc totals summed only `"event":"fill"` notional (`$840.50` for `#13`, `$1,258.50` for `#14`). Nine mandatory buy-side flattens also occurred (`Cap#10` `btc-updown-5m-1795451400` BUY `7.38sh @ $0.06 = $0.4428` on `#2/#8/#14`; `Cap#20` `btc-updown-5m-1795613100` BUY `16.92sh @ $0.01 = $0.1692` on `#13–#18`). Including buy-side flattens matches Rust's `volume_in_micro` exactly (`$840.63` for `#13`, `$1,259.11` for `#14`).

### 1.3 Safe Legacy JSONL Handling

In `Read-CaptureJsonlMetrics` ([sweep-params.ps1](scripts/sweep-params.ps1)), legacy JSONL files missing the five new summary keys are reconstructed safely:
1. When `volume_in_micro` or `total_shares_bought_micro` is absent from `summary`, the parser scans `"event":"fill"` (`filled_shares_micro`) and `"event":"flatten"` (`size_micro`) where `side == "BUY"`.
2. When merge breakdown keys are absent, it scans `"event":"merge"` events (`realized_gain_micro`).
3. **Missing vs. genuine zero protection:** Event-scan totals are only accepted when matching events exist (`scannedBuyEvents > 0` / `scannedMergeEvents > 0`) or when `summary` itself reports zero activity (`fills == 0 && flattens == 0` / `merges == 0`). If a summary-only JSONL reports nonzero `merges_count` or `fills_count` with no event rows, `MergePos`/`MergeNegLoss` stay `NaN` (not `0.0`), and `VolumeIn` uses the explicit synthetic fallback.

---

## 2. Recalculated 18-Configuration Leaderboard

All `18` configurations span `MaxEntryPrice ∈ {0.50, 0.55, 0.60}` × `Tier1HedgeTrigger ∈ {off, 0.65, 0.70}` × `EntryCutoff ∈ {120s, 150s}` over `226` windows (`158` IS / `68` OOS), with `VMax = $50.00` allocated capital per leg and `QueueDecay = conservative`. Both `fix_tier1_cap_paralysis` and `parallelize_active_sweep` scripts produced **`0` diffs** across all `360` files.

| Rk | Cfg | `(MaxPx, T1, Cut)` | Net @ 0% | Net @ 3% | Actual `volume_in` | Actual ROI0 | Fills | Qty (`sh`) | Pairs (`sh`) | Fees | `MergePos` | `MergeNegLoss` | `Settle` | `Unreal` | V0 | V3 | All Fails (0%) |
|---:|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|---|---|
| 1 | **#13** | `($0.60, off, 120s)` | **+$56.59** | **+$57.22** | **$840.63** | **+6.73%** | 285 | 1,861.2 | 878.2 | $0.01 | +$89.03 | $10.42 | -$23.62 | +$1.61 | **APPROVED** | **APPROVED** | *(none)* |
| 2 | #14 | `($0.60, off, 150s)` | +$54.15 | +$55.09 | $1,259.11 | +4.30% | 401 | 2,797.3 | 1,286.1 | $0.03 | +$120.75 | $15.29 | -$52.90 | +$1.62 | FAIL(G4) | FAIL(G4) | G4, G5 |
| 3 | #7 | `($0.55, off, 120s)` | +$40.21 | +$40.69 | $633.05 | +6.35% | 212 | 1,411.9 | 657.1 | $0.00 | +$69.34 | $8.12 | -$22.61 | +$1.61 | FAIL(G5) | FAIL(G5) | G5 |
| 4 | #8 | `($0.55, off, 150s)` | +$36.61 | +$37.37 | $1,014.94 | +3.61% | 314 | 2,271.7 | 1,027.3 | $0.02 | +$99.18 | $12.64 | -$51.53 | +$1.62 | FAIL(G3) | FAIL(G3) | G3, G4, G5 |
| 5 | #18 | `($0.60, $0.70, 150s)` | +$35.46 | +$36.43 | $1,410.63 | +2.51% | 430 | 2,967.5 | 1,429.9 | $0.01 | +$104.38 | $46.39 | -$22.66 | +$0.14 | FAIL(G4) | FAIL(G5)* | G4, G5 |
| 6 | #1 | `($0.50, off, 120s)` | +$32.97 | +$33.37 | $525.91 | +6.27% | 160 | 1,179.1 | 539.8 | $0.00 | +$56.20 | $7.30 | -$17.53 | +$1.61 | FAIL(G4) | FAIL(G4) | G4, G5 |
| 7 | #17 | `($0.60, $0.70, 120s)` | +$32.96 | +$33.61 | $908.54 | +3.63% | 298 | 1,931.1 | 933.4 | $0.01 | +$77.18 | $28.35 | -$15.98 | +$0.12 | FAIL(G5) | FAIL(G5) | G5 |
| 8 | #15 | `($0.60, $0.65, 120s)` | +$30.61 | +$31.26 | $912.77 | +3.35% | 299 | 1,937.9 | 935.2 | $0.01 | +$77.30 | $29.36 | -$17.44 | +$0.12 | FAIL(G5) | FAIL(G5) | G5 |
| 9 | #16 | `($0.60, $0.65, 150s)` | +$29.07 | +$30.05 | $1,424.46 | +2.04% | 435 | 2,989.9 | 1,437.3 | $0.01 | +$103.21 | $48.63 | -$25.64 | +$0.14 | FAIL(G4) | FAIL(G5)* | G4, G5 |
| 10 | #2 | `($0.50, off, 150s)` | +$26.48 | +$27.10 | $834.39 | +3.17% | 255 | 1,891.7 | 838.0 | $0.02 | +$83.66 | $10.83 | -$47.95 | +$1.62 | FAIL(G3) | FAIL(G3) | G3, G4, G5 |
| 11 | #5 | `($0.50, $0.70, 120s)` | +$20.99 | +$21.40 | $587.56 | +3.57% | 165 | 1,239.7 | 600.4 | $0.00 | +$48.90 | $22.13 | -$5.89 | +$0.12 | **FAIL(G5)** | **FAIL(G5)** | G5 |
| 12 | #3 | `($0.50, $0.65, 120s)` | +$19.64 | +$20.05 | $585.79 | +3.35% | 164 | 1,236.6 | 597.3 | $0.00 | +$48.90 | $22.26 | -$7.11 | +$0.12 | FAIL(G5) | FAIL(G5) | G5 |
| 13 | #12 | `($0.55, $0.70, 150s)` | +$17.92 | +$18.71 | $1,166.46 | +1.54% | 343 | 2,441.8 | 1,171.0 | $0.00 | +$82.82 | $43.74 | -$21.29 | +$0.14 | FAIL(G3) | FAIL(G3) | G3, G4, G5 |
| 14 | #11 | `($0.55, $0.70, 120s)` | +$16.58 | +$17.08 | $700.97 | +2.37% | 225 | 1,481.8 | 712.3 | $0.00 | +$57.48 | $26.05 | -$14.97 | +$0.12 | FAIL(G5) | FAIL(G5) | G5 |
| 15 | #9 | `($0.55, $0.65, 120s)` | +$15.81 | +$16.30 | $701.54 | +2.25% | 225 | 1,481.6 | 712.1 | $0.00 | +$57.72 | $26.98 | -$15.05 | +$0.12 | FAIL(G5) | FAIL(G5) | G5 |
| 16 | #10 | `($0.55, $0.65, 150s)` | +$12.93 | +$13.73 | $1,171.25 | +1.10% | 345 | 2,442.3 | 1,170.8 | $0.00 | +$82.14 | $47.71 | -$21.63 | +$0.14 | FAIL(G2) | FAIL(G2) | G2, G3, G4, G5 |
| 17 | #6 | `($0.50, $0.70, 150s)` | +$9.52 | +$10.17 | $982.09 | +0.97% | 271 | 2,035.2 | 979.6 | $0.00 | +$64.91 | $42.47 | -$13.06 | +$0.14 | FAIL(G4) | FAIL(G4) | G4, G5 |
| 18 | #4 | `($0.50, $0.65, 150s)` | +$8.17 | +$8.82 | $980.32 | +0.83% | 270 | 2,032.0 | 976.5 | $0.00 | +$64.91 | $42.60 | -$14.27 | +$0.14 | FAIL(G4) | FAIL(G4) | G4, G5 |

*\*At the 3% Tier-1 rebate tier, `#18` and `#16` edge down from `25.3%` to `24.9%` max drawdown (passing G4), so their first failed gate shifts from `FAIL(G4)` to `FAIL(G5)`. **Config #13 is the sole `APPROVED` configuration at both 0% and 3% tiers.***

---

## 3. Repaired Survival Gates G1–G6: Numerator & Denominator Audit

All gates are evaluated in `Get-SurvivalInputs`, `Evaluate-SurvivalGateRow`, and `Update-SurvivalVerdicts` ([sweep-params.ps1](scripts/sweep-params.ps1)):

- **G1 (`IS > 0`) & G2 (`OOS > 0`):** First `158` `window_end` events form In-Sample (`floor(226 * 0.7)`); remaining `68` form Out-of-Sample.
- **G3 (`Retain >= 50%`):** Window-normalized retention: `(OOS / 68) / (IS / 158) * 100`.
- **G4 (`MaxDD <= 25%`):** Peak-to-trough dollar drawdown over all `226` cumulative window nets divided by allocated working capital `$VMax = $50.00` (not cumulative turnover).
- **G5 (`Drag <= 30%` AND `TailDrag <= 50%`):**
  - `SettleLoss = max(0, -(Settle + min(Unreal, 0)))` (note `Unreal > 0` on all 18 configs, so `SettleLoss = -Settle`).
  - `Drag = SettleLoss / MergePos * 100`.
  - `TailDrag = (SettleLoss + MergeNegLoss) / MergePos * 100`.
- **G6 (`Neighbors >= 60%`):** Ordered Manhattan-1 adjacency across `15` axes (`MaxPx: $0.50 < $0.55 < $0.60`, `T1Trig: $0.65 < $0.70 < off`, `EntryCut: 120s < 150s`), requiring each neighbor to have both `IS > 0` and `OOS > 0`.

| Cfg | G1 (`IS0`, `158w`) | G2 (`OOS0`, `68w`) | G3 `(OOS/68)/(IS/158)` | G4 `PeakDD / $50` (`3% DD`) | G5 `Drag` (`SettleLoss / MergePos`) | G5 `TailDrag` (`(SettleLoss+MNeg) / MergePos`) | G6 Neighbors (`IS>0 && OOS>0`) |
|---:|---:|---:|---:|---:|---:|---:|---|
| **#13** | +$33.12 | +$23.47 | `+$0.345 / +$0.210` = **164.6%** | `$9.71 / $50` = **19.4%** (`19.4%`) | `$23.62 / $89.03` = **26.5%** | `$34.04 / $89.03` = **38.2%** | **3/3 (100%)** `[#7, #14, #17]` |
| #14 | +$40.01 | +$14.14 | `+$0.208 / +$0.253` = 82.1% | `$14.41 / $50` = **28.8%** (`28.8%`) | `$52.90 / $120.75` = **43.8%** | `$68.19 / $120.75` = **56.5%** | 3/3 (100%) `[#8, #13, #18]` |
| #7 | +$28.63 | +$11.58 | `+$0.170 / +$0.181` = 94.0% | `$9.71 / $50` = 19.4% (`19.4%`) | `$22.61 / $69.34` = **32.6%** | `$30.73 / $69.34` = 44.3% | 4/4 (100%) `[#1, #8, #11, #13]` |
| #8 | +$32.63 | +$3.98 | `+$0.059 / +$0.207` = **28.3%** | `$14.47 / $50` = **28.9%** (`28.9%`) | `$51.53 / $99.18` = **52.0%** | `$64.17 / $99.18` = **64.7%** | 4/4 (100%) `[#2, #7, #12, #14]` |
| #18 | +$22.62 | +$12.84 | `+$0.189 / +$0.143` = 131.9% | `$12.66 / $50` = **25.3%** (`24.9%`) | `$22.66 / $104.38` = 21.7% | `$69.05 / $104.38` = **66.2%** | 4/4 (100%) `[#12, #14, #16, #17]` |
| #1 | +$20.49 | +$12.48 | `+$0.184 / +$0.130` = 141.5% | `$13.60 / $50` = **27.2%** (`27.2%`) | `$17.53 / $56.20` = **31.2%** | `$24.83 / $56.20` = 44.2% | 3/3 (100%) `[#2, #5, #7]` |
| #17 | +$15.24 | +$17.72 | `+$0.261 / +$0.096` = 270.0% | `$9.87 / $50` = 19.7% (`19.6%`) | `$15.98 / $77.18` = 20.7% | `$44.33 / $77.18` = **57.4%** | 4/4 (100%) `[#11, #13, #15, #18]` |
| #15 | +$14.10 | +$16.52 | `+$0.243 / +$0.089` = 272.2% | `$9.87 / $50` = 19.7% (`19.6%`) | `$17.44 / $77.30` = 22.6% | `$46.80 / $77.30` = **60.5%** | 3/3 (100%) `[#9, #16, #17]` |
| #16 | +$17.43 | +$11.64 | `+$0.171 / +$0.110` = 155.2% | `$12.66 / $50` = **25.3%** (`24.9%`) | `$25.64 / $103.21` = 24.8% | `$74.27 / $103.21` = **72.0%** | 2/3 (66.7%) `[#10(X), #15, #18]` |
| #2 | +$22.67 | +$3.81 | `+$0.056 / +$0.143` = **39.1%** | `$18.30 / $50` = **36.6%** (`36.6%`) | `$47.95 / $83.66` = **57.3%** | `$58.78 / $83.66` = **70.3%** | 3/3 (100%) `[#1, #6, #8]` |
| **#5** | +$14.26 | +$6.73 | `+$0.099 / +$0.090` = 109.6% | `$10.45 / $50` = 20.9% (`20.8%`) | `$5.89 / $48.90` = 12.1% | `$28.02 / $48.90` = **57.3%** | 4/4 (100%) `[#1, #3, #6, #11]` |
| #3 | +$13.32 | +$6.33 | `+$0.093 / +$0.084` = 110.4% | `$10.45 / $50` = 20.9% (`20.8%`) | `$7.11 / $48.90` = 14.5% | `$29.37 / $48.90` = **60.1%** | 3/3 (100%) `[#4, #5, #9]` |
| #12 | +$15.24 | +$2.68 | `+$0.039 / +$0.096` = **40.9%** | `$14.91 / $50` = **29.8%** (`29.2%`) | `$21.29 / $82.82` = 25.7% | `$65.03 / $82.82` = **78.5%** | 4/5 (80.0%) `[#6, #8, #10(X), #11, #18]` |
| #11 | +$10.76 | +$5.83 | `+$0.086 / +$0.068` = 125.9% | `$11.08 / $50` = 22.2% (`22.1%`) | `$14.97 / $57.48` = 26.0% | `$41.02 / $57.48` = **71.4%** | 5/5 (100%) `[#5, #7, #9, #12, #17]` |
| #9 | +$11.18 | +$4.63 | `+$0.068 / +$0.071` = 96.2% | `$11.08 / $50` = 22.2% (`22.1%`) | `$15.05 / $57.72` = 26.1% | `$42.03 / $57.72` = **72.8%** | 3/4 (75.0%) `[#3, #10(X), #11, #15]` |
| #10 | +$15.66 | **-$2.73** | `-$0.040 / +$0.099` = **-40.4%** | `$14.91 / $50` = **29.8%** (`29.5%`) | `$21.63 / $82.14` = 26.3% | `$69.34 / $82.14` = **84.4%** | 4/4 (100%) `[#4, #9, #12, #16]` |
| #6 | +$7.01 | +$2.51 | `+$0.037 / +$0.044` = 83.4% | `$17.14 / $50` = **34.3%** (`33.9%`) | `$13.06 / $64.91` = 20.1% | `$55.53 / $64.91` = **85.6%** | 4/4 (100%) `[#2, #4, #5, #12]` |
| #4 | +$6.06 | +$2.11 | `+$0.031 / +$0.038` = 81.1% | `$17.14 / $50` = **34.3%** (`33.9%`) | `$14.27 / $64.91` = 22.0% | `$56.87 / $64.91` = **87.6%** | 2/3 (66.7%) `[#3, #6, #10(X)]` |

*(Bold cells mark gate failures; `(X)` in G6 marks neighbor `#10`, which fails `OOS > 0`.)*

---

## 4. What Changed vs. the Previous Investigation

| Metric / Config | Old Report Verdict / Figure | Corrected Verdict / Figure | Cause of Change |
| --- | --- | --- | --- |
| **Config #13** | `FAIL(G5)` (`42%` drag), `+2.03%` ROI on `$2,793` | **`APPROVED`** (`26.5% / 38.2%` drag), **`+6.73%` ROI** on `$840.63` | Replaced post-settlement `SumGross` denominator with `MergePos` (`$89.03`) and synthetic `$9.80/fill` volume with actual buy notional. |
| **Config #5** | Sole `APPROVED` (`28%` drag), `+1.30%` ROI | **`FAIL(G5)`** (`12.1% / 57.3%` tail drag), `+3.57%` ROI on `$587.56` | Combined `TailDrag` catches `$22.13` negative merges caused by `T1 = $0.70` relabeling expiry loss into merge loss. |
| **G4 Max Drawdown** | `0%–1%` (`PASS`) on all 18 configs | `19.4%–36.6%` (`10` configs `FAIL` at 0%; `8` at 3%) | Divided single-window-curve dollar drawdown (`$9.71–$18.30`) by allocated `$50 VMax` instead of cumulative volume (`$1.57K–$4.26K`). |
| **G3 Retention** | Falsely failed `#14` (`35%`), `#6` (`36%`), `#4` (`35%`) | `#14` `82.1%`, `#6` `83.4%`, `#4` `81.1%` (**all `PASS >= 50%`**) | Normalized OOS (`68w`) and IS (`158w`) by window counts before dividing; `#8, #2, #12, #10` still fail (`-40.4%` to `40.9%`). |
| **G6 Neighbors** | All 18 reported `100%` (`5/5`) | `#16` `66.7%`, `#9` `75.0%`, `#12` `80.0%`, `#4` `66.7%`; others `100%` | Enforced ordered Manhattan-1 `±1` adjacency (`3–5` neighbors) and required neighbor `OOS > 0` (rejecting `#10`). |
| **Actual `volume_in` (`#13`, `#14`)** | Fill-only `$840.50`, `$1,258.50` | Exact `$840.63` (`+0.17`), `$1,259.11` (`+0.61`) | Added BUY-side `flatten` notional matching `PaperLedger::record_taker_flatten_with_merge`. |

---

## 5. Why Config #13 Is Strongest, Controlled Comparisons, and Remaining Weaknesses

### 5.1 What Remains Supported by Recalculated Data

All three single-parameter findings from the earlier investigation hold after full telemetry and gate recalculation:

1. **`T1 Trigger` destroys risk-adjusted return across every tested pair:** At `($0.60, 120s)`, `#13 (T1=off)` earns `+$56.59` (`+6.73%` ROI, `38.2%` tail drag) versus `#17 ($0.70)` at `+$32.96` (`+3.63%`, `57.4%`) and `#15 ($0.65)` at `+$30.61` (`+3.35%`, `60.5%`). The cross-tick override latch in `DeficitHedgeOverrideLatch` ([station7_risk.rs](crates/risk-lifecycle/src/station7_risk.rs#L44-L124)) locks until imbalance drops below 5 shares, forcing expensive cross-tick merges (`$28.35–$29.36` vs `$10.42`) whenever spreads widen transiently.
2. **`120s` entry cutoff dominates `150s`:** At `($0.60, off)`, moving from `#13 (120s)` to `#14 (150s)` deploys `$418.48` extra capital (`+49.8%`) and adds `+$29.28` settlement loss for `-$2.44` lower Net P&L and a `FAIL(G4)` drawdown jump (`19.4% -> 28.8%`). Every single `150s` config fails `G4` at the 0% floor (`25.3%–36.6%`).
3. **Raising `MaxEntryPrice` (`$0.50 -> $0.55 -> $0.60`) with `T1=off, 120s` improves P&L and efficiency monotonically:** Net P&L rises `+$32.97 (#1) -> +$40.21 (#7) -> +$56.59 (#13)`, actual ROI rises `+6.27% -> +6.35% -> +6.73%`, and drawdown drops (`27.2% -> 19.4% -> 19.4%`).
4. **Repeatability vs. Config #5:** Config `#13` outperforms `#5` in total P&L (`2.70×`), ROI (`1.89×`), active windows (`66` vs `53`), active win rate (`57.6%` vs `50.9%`), dollar drawdown (`$9.71` vs `$10.45`), statistical significance (`t = 2.53` vs `1.47`), and profit excluding top 5 windows (`+$23.86` vs `-$0.55`).

### 5.2 Remaining Weaknesses and Sample-Size Limits

- **Fill count below target (`285` vs `>= 400`):** Config `#13` generates `285` fills across `66` active windows out of `226` (`13`th ranks first, but sample size remains moderate).
- **Single-window tail exposure (`btc-updown-5m-1795517700`):** One stranded `20.0`-share Down position (`-$9.60`) in `Cap#11` creates `40.6%` of `#13`'s settlement loss and drives nearly its entire `$9.71` drawdown when liquidity vanished before mandatory flatten.
- **Incomplete trailing capture windows (`15` windows):** Fifteen of the twenty captures ended mid-window (`complete: false`), contributing `+$1.61` unrealized P&L to `#13`.
- **Tested grid boundary:** Config `#13` sits on the upper edge of `MaxEntryPrice` (`$0.60`) and the lower edge of `EntryCutoff` (`120s`).

### 5.3 Next Targeted Experiment (For Later Execution)

Run a small `2 × 3` (or `3 × 3`) boundary follow-up holding `T1 = off`, `CapTier1 = true`, `EntryOpenDelay = 15s`, and `QueueDecay = conservative` fixed while testing `MaxEntryPrice ∈ {0.60, 0.65, 0.70}` × `EntryCutoff ∈ {90s, 105s, 120s}` as additional captures push fill counts past `400`.

---

## 6. Config #6/9 Runtime Resolution & Conflicting Defaults Audit

### 6.1 Runtime Loading Path Verified

Root [DEFAULTS.json](DEFAULTS.json) is promoted to **Config #6/9** (`max_entry_price: 0.65`, `tier1_hedge_trigger_price: null`, `entry_cutoff_secs: 120`, `cap_tier1_reducing_to_delta: true`, `entry_open_delay_secs: 15`) alongside all six calibration sync files (`AGENTS.md` §7). The earlier Config #13 (`$0.60`) baseline recorded in §2–§5 was superseded by the boundary sweep described in [boundary-sweep-selection-report.md](boundary-sweep-selection-report.md):
- [DEFAULTS.json](DEFAULTS.json#L23-L32)
- [.env.example](.env.example#L86-L104)
- `EngineConfig::default()` / `EngineConfig::hardcoded_fallback()` ([config.rs](crates/core-types/src/config.rs#L509-L554)) embeds `DEFAULTS.json` at compile time via `include_str!("../../../DEFAULTS.json")`.
- Live and replay CLI entrypoints (`lifecycle_cmd.rs`, `paper_cmd.rs`, `trace_cmd.rs`, `feeds_cmd.rs`, `doctor.rs`, `main.rs`) load `EngineConfig::from_env()?`, which starts from `EngineConfig::default()` (`DEFAULTS.json`). `lifecycle_cmd.rs` explicitly passes `cfg.cap_tier1_reducing_to_delta`, `cfg.entry_open_delay_secs`, `cfg.entry_cutoff_secs`, and `MathPipelineConfig::from_engine_config(cfg)`. `exec_cmd.rs` dry-run loads `MathPipelineConfig::try_default()?`, which calls `MathPipelineConfig::from_engine_config(&EngineConfig::default())`.
- Verified by `embedded_defaults_json_parses_validates_and_matches_default` ([config.rs](crates/core-types/src/config.rs#L1344)), `config_locked_defaults_validate` ([lib.rs](crates/core-types/src/lib.rs#L227)), and `cli_lifecycle_and_pipeline_defaults_resolve_promoted_defaults` ([main.rs](crates/cli/src/main.rs)).

### 6.2 Conflicting Library Struct Defaults (Non-Runtime)

Three standalone library constructors keep conservative/minimal struct defaults for unit-test isolation and are **always overridden** by `EngineConfig` in the CLI runtime:

| Struct & Constructor | Field(s) Differing from `DEFAULTS.json` | Impact at Runtime |
| --- | --- | --- |
| `RiskConfig::default()` ([station7_risk.rs](crates/risk-lifecycle/src/station7_risk.rs#L187-L199)) | `cap_tier1_reducing_to_delta: false` (`DEFAULTS.json` = `true`) | **None:** `spawn_managed_session` (`lifecycle_cmd.rs:2627`) overrides from `cfg.cap_tier1_reducing_to_delta`. |
| `LifecycleConfig::default()` ([lifecycle.rs](crates/risk-lifecycle/src/lifecycle.rs#L91-L103)) | `entry_open_delay_secs: 0`, `entry_cutoff_secs: None` (`DEFAULTS.json` = `15`, `Some(120)`) | **None:** `build_risk_actor` (`lifecycle_cmd.rs:2577-2578`) overrides from `cfg.entry_open_delay_secs` and `cfg.entry_cutoff_secs`. |
| `PricerConfig::from_gamma()` ([station4_6_pricer.rs](crates/math-engine/src/station4_6_pricer.rs#L37-L50)) | `max_cross_tick_loss_micro: None` (`DEFAULTS.json` = `Some(80_000)`); `max_entry_price: Some(0.60)` differs from `DEFAULTS.json` (`Some(0.65)`) | **None:** CLI and `PricerConfig::try_default()` construct from `EngineConfig`. |
