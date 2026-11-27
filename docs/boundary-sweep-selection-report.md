# Boundary Sweep Selection & Next Research Phase: Promotion of Config #6/9 (`$0.65 / 120s / T1=off`)

- **Run ID:** `boundary-9runs_20caps-226w`
- **Source Sweep Directory:** `C:\Users\Tiger\Agents\Projects\AI Trading\polymarket-as-rust\logs\sweeps\25-11_21-55-50Z_boundary-9runs_20caps-226w`
- **Summary Artifact:** `logs/sweeps/25-11_21-55-50Z_boundary-9runs_20caps-226w/summary.txt` *(git-ignored local sweep output)*
- **Run Date / Runtime:** November 25–26, 2026 (`23:56` local start, `741.99s` across `6` parallel workers)
- **Prior Baseline Reference:** [sweep-results-investigation.md](sweep-results-investigation.md)
- **Classification:** **`Config #6/9` (`$0.65, 120s, T1=off`) selected as provisional research baseline** (`336` fills across `226` windows, below the `400`-fill validation bar).
- **No qualifying survival pass was earned.** The `21`-capture replay on the corrected `5cf74b9` binary (`351` fills, `26-11_07-55-07Z_1runs_21caps-241w`) fails G3 and G5, and is itself below the `400`-fill bar, so under spec Filter 4 the result reads **`UNDERPOWERED`** — neither a pass nor a failure. Values are unchanged. Analysis: [sweep-results-investigation.md](sweep-results-investigation.md) Part III.

> [!NOTE]
> **Epistemic Labeling Throughout This Report:** Sections and columns explicitly distinguish **(Observed)** raw event/summary data from the `180` JSONL files, **(Derived)** ratios and normalized scores, **(Interpretation)** causal trade-offs, and **(Recommendations)** for the next research phase.

---

## 1. Dataset Completeness & Accounting Integrity Verification *(Observed)*

All `9` configurations (`MaxEntryPrice ∈ {0.60, 0.65, 0.70}` × `EntryCutoff ∈ {90s, 105s, 120s}`, `Tier1HedgeTrigger = off`) ran over the exact `20` live captures (`226` 5-minute windows, `158` In-Sample / `68` Out-of-Sample, `VMax = $50`, `BaseSize = 20sh`, `MaxPair = $0.98`, `QueueDecay = conservative`).

Event-level verification across all `180` (`9 × 20`) JSONL files confirmed **`0` data-integrity errors**:
- **File & summary completeness:** `20` capture folders × `9` expected config stems = `180/180` JSONLs, each terminating in exactly `1` `"event":"summary"` row (`BadFinalSummary = 0`, `WrongSummaryCount = 0`, `MissingTelemetry = 0`).
- **Buy-volume identity (`volMismatch = 0`):** Re-summing integer micro-USDC buy notional across `"event":"fill"` (`filled_shares_micro * price_micro / 1e6`) and `"event":"flatten"` (`size_micro * price_micro / 1e6`) matched `summary.volume_in_micro` identically in every capture.
- **Merge-gain identity (`mergeMismatch = 0`):** Summing `"event":"merge"` `realized_gain_micro` matched `summary.total_merge_gain_micro == summary.positive_merge_gain_micro - summary.negative_merge_loss_micro` in every capture.
- **Realized-PnL identity (`pnlMismatch = 0`):** `total_merge_gain_micro + settlement_pnl_micro - fees_paid_micro == realized_pnl_micro` held in all `180` files.
- **Cross-sweep anchor determinism:** Config `#3/9` (`$0.60, 120s, T1=off`) reproduced Config `#13/18` from the earlier 18-configuration sweep (`logs/sweeps/25-11_16-21-55Z_18runs_20caps-226w`, raw output since pruned) to the micro-dollar (`+$56.59` Net0, `$840.63` deployed, `285` fills, `19.4%` DD, `26.5% / 38.2%` drag/tail).

---

## 2. Pre-Committed Balanced Scoring Methodology *(Derived Model Definition)*

Weights, normalization bounds, and survival eligibility were fixed **before ranking** and applied identically to all `9` configurations.

### 2.1 Dimension Weights

| Dimension | Weight | Purpose |
| --- | ---: | --- |
| **A. Profitability & Capital Efficiency** | **25%** | Balances raw profit against capital efficiency and tail-trimmed durability. |
| **B. Out-of-Sample Performance** | **30%** | Highest weight to guard against in-sample overfitting on a 226-window sample. |
| **C. Risk & Downside Exposure** | **25%** | Penalizes equity drawdown, settlement drag, negative merges, and single-capture/window tail losses. |
| **D. Consistency & Robustness** | **20%** | Rewards broad capture/window breadth, top-window non-concentration, and Manhattan-1 neighbor plateau stability. |

**Mandatory Eligibility Gate:** A configuration must pass **all six unmodified survival gates (`G1–G6`)** at `QueueDecay=conservative` to be promoted, regardless of composite score.

> [!IMPORTANT]
> **This sweep does not clear Filter 4.** Every gate figure below was produced on a `336`-fill sample, under spec Filter 4's `400`-fill minimum ([strategy-verification-spec.md](strategy-verification-spec.md) §5.1.4). `scripts/sweep-params.ps1` now reports such a sample as `UNDERPOWERED`, so the `APPROVED` cells in §3.4 and §3.5 are **measured gate output**, not an earned eligibility. `#6/9` was selected as a research baseline on that basis; it is not a validated strategy and its values have not changed since.

### 2.2 Sub-Metric Weights and Normalization

- **Normalization:** Min-Max scaling onto `[0, 100]` across the `9` candidates: `100 * (x - min) / (max - min)` for higher-is-better metrics, and `100 * (max - x) / (max - min)` for lower-is-better risk metrics (using negative average loss magnitude as higher-is-better for `AvgLossWin`).
- **Retention Cap (`150%`):** `OOS/IS` window-normalized retention is capped at `150%` before scoring so weak In-Sample configs (`#5` at `307%`, `#2` at `289%`) do not earn artificial retention inflation.

| Dimension | Exact Sub-Metrics & Intra-Dimension Weights |
| --- | --- |
| **A (25%)** | `0.35 * Net0` + `0.30 * ActualROI0` + `0.15 * ProfitPerActiveWindow` + `0.20 * NetExTop5Windows` |
| **B (30%)** | `0.35 * OOS0` + `0.25 * OOS_ROI0` + `0.20 * min(Retain0, 150%)` + `0.20 * OOSActiveWin%` |
| **C (25%)** | `0.25 * (1 - MaxDD0%)` + `0.20 * (1 - Drag%)` + `0.25 * (1 - TailDrag%)` + `0.15 * WorstCapture` + `0.15 * AvgLosingActiveWin` |
| **D (20%)** | `0.25 * ProfitableCaptures%` + `0.20 * ActiveWin%` + `0.25 * NetExBestCapture` + `0.15 * (100% - Top3WinShare%)` + `0.15 * Manhattan1NeighborMeanNet0` |

*(Equal-weight `25/25/25/25` sensitivity is also computed below to confirm winner stability against reasonable weight perturbations.)*

---

## 3. Complete 9-Configuration Comparison *(Observed & Derived)*

### 3.1 Dimension A — Profitability & Capital Efficiency

| Cfg | `(MaxPx, Cut)` | Net @ 0% *(Obs)* | Net @ 3% *(Obs)* | Actual `volume_in` *(Obs)* | Actual ROI0 *(Der)* | Qty (`sh`) *(Obs)* | Fills *(Obs)* | Pairs (`sh`) *(Obs)* | Profit / Active Win *(Der)* | Profit / Capture *(Der)* | Net Ex-Top5 Win *(Der)* |
|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `#1` | `($0.60, 90s)` | +$36.57 | +$36.98 | $534.78 | **+6.84%** | 1,196.2 | 195 | 557.8 | +$0.87 | +$1.83 | +$4.68 |
| `#2` | `($0.60, 105s)` | +$38.72 | +$39.25 | $694.61 | +5.58% | 1,535.0 | 249 | 719.8 | +$0.72 | +$1.94 | +$6.83 |
| `#3` | `($0.60, 120s)` | +$56.59 | +$57.22 | $840.63 | +6.73% | 1,861.2 | 285 | 878.2 | **$0.86** | +$2.83 | +$23.86 |
| `#4` | `($0.65, 90s)` | +$32.65 | +$33.12 | $634.26 | +5.15% | 1,381.1 | 227 | 653.4 | +$0.74 | +$1.63 | +$0.75 |
| `#5` | `($0.65, 105s)` | +$39.00 | +$39.61 | $834.90 | +4.67% | 1,809.2 | 292 | 860.3 | +$0.66 | +$1.95 | +$7.10 |
| **`#6`** | **`($0.65, 120s)`** | **+$58.79** | **+$59.53** | `$1,010.04` | +5.82% | 2,195.9 | 336 | 1,048.1 | +$0.79 | **+$2.94** | +$26.06 |
| `#7` | `($0.70, 90s)` | +$29.08 | +$29.55 | $643.20 | +4.52% | 1,384.2 | 227 | 650.0 | +$0.59 | +$1.45 | +$5.88 |
| `#8` | `($0.70, 105s)` | +$36.48 | +$37.10 | $857.78 | +4.25% | 1,842.3 | 298 | 871.9 | +$0.57 | +$1.82 | +$12.66 |
| `#9` | `($0.70, 120s)` | +$55.53 | +$56.29 | $1,040.74 | +5.34% | 2,244.6 | **346** | **1,066.8** | +$0.69 | +$2.78 | **+$30.30** |

### 3.2 Dimension B — Out-of-Sample Performance (`158w` IS / `68w` OOS)

| Cfg | `(MaxPx, Cut)` | IS Net0 *(Obs)* | OOS Net0 *(Obs)* | IS `Vol` *(Der)* | IS ROI0 *(Der)* | OOS `Vol` *(Der)* | OOS ROI0 *(Der)* | Raw Ret0 *(Der)* | Scored Ret (`<=150%`) | OOS Active W/L (`Win%`) *(Obs)* |
|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---|
| `#1` | `($0.60, 90s)` | +$19.58 | +$16.99 | $353.03 | +5.55% | $181.75 | +9.35% | 202% | 150.0% | `7W / 3L` (**70.0%**) |
| `#2` | `($0.60, 105s)` | +$17.25 | +$21.47 | $467.39 | +3.69% | $227.22 | **+9.45%** | 289% | 150.0% | `10W / 5L` (66.7%) |
| `#3` | `($0.60, 120s)` | +$33.12 | +$23.47 | $540.91 | +6.12% | $299.72 | +7.83% | 165% | 150.0% | `12W / 7L` (63.2%) |
| `#4` | `($0.65, 90s)` | +$14.94 | +$17.71 | $430.89 | +3.47% | $203.37 | +8.71% | 276% | 150.0% | `7W / 3L` (**70.0%**) |
| `#5` | `($0.65, 105s)` | +$16.80 | +$22.19 | $585.81 | +2.87% | $249.09 | +8.91% | 307% | 150.0% | `10W / 5L` (66.7%) |
| **`#6`** | **`($0.65, 120s)`** | +$34.60 | **+$24.19** | $688.50 | +5.03% | $321.54 | +7.52% | 162% | **150.0%** | `12W / 7L` (63.2%) |
| `#7` | `($0.70, 90s)` | +$22.32 | +$6.76 | $429.40 | +5.20% | $213.80 | +3.16% | 70% | 70.0% | `8W / 4L` (66.7%) |
| `#8` | `($0.70, 105s)` | +$24.89 | +$11.59 | $593.56 | +4.19% | $264.22 | +4.39% | 108% | 108.0% | `11W / 6L` (64.7%) |
| `#9` | `($0.70, 120s)` | **+$42.68** | +$12.84 | $696.16 | **+6.13%** | $344.58 | +3.73% | 70% | 70.0% | `13W / 9L` (59.1%) |

### 3.3 Dimension C — Risk & Downside Exposure

| Cfg | `(MaxPx, Cut)` | MaxDD $ / `%$50` *(Obs)* | `Settle` *(Obs)* | `MergePos` *(Obs)* | `MergeNegLoss` *(Obs)* | `Drag%` *(Der)* | `TailDrag%` *(Der)* | Worst Win (`btc-updown-5m-`) *(Obs)* | Worst Capture *(Obs)* | Lose Active Win (`Avg`) *(Obs/Der)* | Flattens / Stranded *(Obs)* |
|---:|---|---:|---:|---:|---:|---:|---:|---|---:|---:|---|
| `#1` | `($0.60, 90s)` | **$9.71 / 19.4%** | -$19.90 | +$60.09 | **$5.22** | 33.1% | 41.8% | -$9.60 (`1795517700`) | -$5.30 (`Cap#11`) | `17` (-$1.26) | `1` / `1 (-$9.60)` |
| `#2` | `($0.60, 105s)` | **$9.71 / 19.4%** | -$25.30 | +$70.28 | $7.84 | 36.0% | 47.2% | -$9.60 (`1795517700`) | -$4.85 (`Cap#11`) | `24` (-$1.04) | `1` / `1 (-$9.60)` |
| `#3` | `($0.60, 120s)` | **$9.71 / 19.4%** | -$23.62 | +$89.03 | $10.42 | 26.5% | 38.2% | -$9.60 (`1795517700`) | -$4.85 (`Cap#11`) | `28` (-$0.98) | `1` / `1 (-$9.60)` |
| `#4` | `($0.65, 90s)` | $11.15 / 22.3% | -$16.39 | +$62.13 | $14.68 | 26.4% | 50.0% | -$9.60 (`1795517700`) | -$5.30 (`Cap#11`) | `18` (-$1.36) | `1` / `1 (-$9.60)` |
| `#5` | `($0.65, 105s)` | $11.45 / 22.9% | -$21.38 | +$76.40 | $17.62 | 28.0% | 51.0% | -$9.60 (`1795517700`) | -$5.21 (`Cap#14`) | `25` (-$1.15) | `1` / `1 (-$9.60)` |
| **`#6`** | **`($0.65, 120s)`** | **$9.71 / 19.4%** | -$19.10 | **+$96.71** | $20.42 | **19.8%** | 40.9% | -$9.60 (`1795517700`) | -$4.11 (`Cap#14`) | `30` (-$1.04) | `1` / `1 (-$9.60)` |
| `#7` | `($0.70, 90s)` | $10.25 / 20.5% | **-$14.93** | +$50.00 | $7.59 | 29.9% | 45.0% | -$9.60 (`1795517700`) | -$5.85 (`Cap#11`) | `19` (-$1.08) | `0` / `1 (-$9.60)` |
| `#8` | `($0.70, 105s)` | $10.25 / 20.5% | -$19.45 | +$64.97 | $10.65 | 29.9% | 46.3% | -$9.60 (`1795517700`) | **-$1.90 (`Cap#7`)** | `26` (-$0.96) | `0` / `1 (-$9.60)` |
| `#9` | `($0.70, 120s)` | $10.25 / 20.5% | -$17.87 | +$85.44 | $13.65 | 20.9% | **36.9%** | -$9.60 (`1795517700`) | **-$1.90 (`Cap#7`)** | `32` (**-$0.88**) | `0` / `1 (-$9.60)` |

### 3.4 Dimension D — Consistency, Robustness & Existing G1–G6 Verdicts *(Observed & Derived)*

| Cfg | `(MaxPx, Cut)` | Profit Caps / 20 | Active Win% (`W/Tot`) | Top 1 / 3 / 5 Share | Ex-Best Cap | Ex-Worst Cap | Manhattan-1 Mean Net0 (`k`) | `MO1s / MO5s` (`bps`) | G1–G6 Gate Output † (`0% / 3%`) |
|---:|---|---:|---:|---|---:|---:|---:|---|---|
| `#1` | `($0.60, 90s)` | `9/20` (45%) | 59.5% (`25/42`) | `30% / 62% / 87%` | +$18.97 | +$41.87 | +$35.69 (`2`) | +56.4 / +109.5 | FAIL(G5) / FAIL(G5) |
| `#2` | `($0.60, 105s)` | `9/20` (45%) | 55.6% (`30/54`) | `28% / 59% / 82%` | +$17.48 | +$43.57 | +$44.05 (`3`) | +43.0 / +89.4 | FAIL(G5) / FAIL(G5) |
| `#3` | `($0.60, 120s)` | `11/20` (55%) | 57.6% (`38/66`) | `19% / 40% / 58%` | +$35.35 | +$61.44 | +$48.76 (`2`) | +47.0 / +87.2 | **APPROVED / APPROVED** |
| `#4` | `($0.65, 90s)` | `10/20` (50%) | 59.1% (`26/44`) | `34% / 70% / 98%` | +$15.05 | +$37.95 | +$34.88 (`3`) | +58.6 / +99.6 | FAIL(G5) / FAIL(G5) |
| `#5` | `($0.65, 105s)` | `9/20` (45%) | 57.6% (`34/59`) | `28% / 58% / 82%` | +$17.75 | +$44.21 | +$41.66 (`4`) | +42.3 / +77.1 | FAIL(G5) / FAIL(G5) |
| **`#6`** | **`($0.65, 120s)`** | `12/20` (60%) | 59.5% (`44/74`) | `19% / 39% / 56%` | +$37.54 | **+$62.90** | **+$50.37** (`3`) | +43.3 / +70.5 | **APPROVED / APPROVED** |
| `#7` | `($0.70, 90s)` | `11/20` (55%) | **61.2%** (`30/49`) | `21% / 52% / 80%` | +$18.94 | +$34.93 | +$34.56 (`2`) | +54.2 / +40.3 | **APPROVED / APPROVED** |
| `#8` | `($0.70, 105s)` | `10/20` (50%) | 59.4% (`38/64`) | `16% / 42% / 65%` | +$26.74 | +$38.38 | +$41.20 (`3`) | +37.6 / +28.2 | **APPROVED / APPROVED** |
| `#9` | `($0.70, 120s)` | **13/20 (65%)** | 60.0% (`48/80`) | **`11% / 29% / 45%`** | **+$43.95** | +$57.42 | +$47.63 (`2`) | +40.9 / +23.8 | **APPROVED / APPROVED** |

*(In all nine configs, G1, G2, G3, G4, and G6 = `100%` PASS. Failures in `#1, #2, #4, #5` are strictly G5: `Drag > 30%` on `#1/#2` and `TailDrag >= 50.04%` / `51.0%` on `#4/#5`.)*

**† Measured gate output, not an eligibility pass.** All nine rows carry `336` fills against spec Filter 4's `400`-fill bar, so under the enforcement added in `scripts/sweep-params.ps1` (`Evaluate-SurvivalGateRow -TotalFills`, PR #77) each of them reads **`UNDERPOWERED`** rather than `APPROVED` or `FAIL(G5)`. The per-gate cells are kept because they still describe the sample's shape; they do not establish that the thresholds were met on an adequate sample.

### 3.5 Final Scored Ranking Table *(Derived)*

| Final Rank | Cfg | `(MaxPx, Cut)` | Dim A `(25%)` | Dim B `(30%)` | Dim C `(25%)` | Dim D `(20%)` | Weighted Total | Equal-Weight `(25/25/25/25)` | G1–G6 Gate Output † | Selection Status |
|---:|---:|---|---:|---:|---:|---:|---:|---:|---|---|
| **1** | **`#6/9`** | **`($0.65, 120s)`** | `81.5` | **`79.8`** | `79.5` | `78.4` | **`79.9`** | **`79.8`** | **APPROVED** | **Selected Winner (Provisional)** |
| **2** | `#3/9` | `($0.60, 120s)` | **`91.2`** | `79.6` | `75.1` | `61.5` | `77.7` | `76.8` | **APPROVED** | Runner-Up / Primary Challenger |
| **3** | `#9/9` | `($0.70, 120s)` | `69.9` | `14.5` | **`90.7`** | **`93.1`** | `63.1` | `67.0` | **APPROVED** | Eligible — Weak OOS Collapse |
| **4** | `#1/9` | `($0.60, 90s)` | `56.5` | `85.2` | `50.1` | `21.2` | `56.4` | `53.2` | FAIL(G5) | Ineligible (`Drag 33.1% > 30%`) |
| **5** | `#2/9` | `($0.60, 105s)` | `38.2` | **`88.4`** | `45.5` | `15.1` | `50.5` | `46.8` | FAIL(G5) | Ineligible (`Drag 36.0% > 30%`) |
| **6** | `#5/9` | `($0.65, 105s)` | `25.4` | `87.7` | `18.8` | `20.5` | `41.5` | `38.1` | FAIL(G5) | Ineligible (`Tail 51.0% > 50%`) |
| **7** | `#4/9` | `($0.65, 90s)` | `23.2` | `84.0` | `19.8` | `19.0` | `39.8` | `36.5` | FAIL(G5) | Ineligible (`Tail 50.04% > 50%`) |
| **8** | `#8/9` | `($0.70, 105s)` | `16.8` | `34.4` | `60.5` | `46.5` | `38.9` | `39.5` | **APPROVED** | Low Volume / Weak OOS |
| **9** | `#7/9` | `($0.70, 90s)` | `7.8` | `14.0` | `43.9` | `42.3` | `25.6` | `27.0` | **APPROVED** | Lowest P&L & OOS |

† As in §3.4: every rank in this table rests on `336` fills, so under Filter 4 each row is `UNDERPOWERED`. The ranking orders candidates by score; it does not certify any of them as survival-approved.

---

## 4. Winner Justification & `120s` Head-to-Head Trade-Off Analysis *(Interpretation)*

### 4.1 Explicit Superlatives Across Distinct Criteria

- **Highest Raw Net P&L (`+$58.79` @ 0%, `+$59.53` @ 3%):** Config `#6/9` (`$0.65, 120s`).
- **Highest Capital ROI:** Config `#1/9` (`+6.84%`, **ineligible** — fails G5 at `33.1%`); highest *eligible* ROI is Config `#3/9` (`+6.73%`).
- **Best Balanced & Risk-Adjusted Configuration of the Grid (`79.9` Weighted / `79.8` Equal):** Config **`#6/9`** — highest composite score, on a `336`-fill sample that no configuration in the grid clears Filter 4 with.
- **Final Selected Baseline for `DEFAULTS.json`:** Config **`#6/9`** (provisional research baseline, with `#3/9` held as the active challenger).

### 4.2 Comparing the Three `120s` Contenders (`#6` vs `#3` vs `#9`)

All `120s` configs are the best entry cutoff tested in this sweep (`120s` is the sweep's upper boundary, so this is a tested-set result rather than an interior optimum) (`90s` and `105s` lose `-$17.87` to `-$26.14` Net P&L without preventing the `t = 15s` `-$9.60` stranding). Among the three `120s` candidates:

1. **Why `#9/9` (`$0.70`) finishes third (`63.1`):** Despite strong diversification (`13/20` caps positive, `45%` Top-5 share, `$43.95` Ex-Best-Cap), it hits an OOS cliff in `Cap#20` (`+$9.73` vs `+$21.24` on `#3/#6`), cutting OOS Net0 nearly in half (`+$12.84`, `3.73%` OOS ROI vs `6.13%` IS ROI) and collapsing `MO5s` markout to `+23.8 bps` (versus `+87.2` and `+70.5 bps`).
2. **Why `#6/9` (`$0.65`) edges out `#3/9` (`$0.60`) (`79.9` vs `77.7`):**
   - **Out-of-sample & breadth:** Contrary to a naive summary glance, `#6/9` actually delivers slightly higher total **OOS Net0 (`+$24.19` vs `+$23.47`)** with the exact same OOS active win rate (`12/19 = 63.2%`), more total fills (`336` vs `285`, `+17.9%` faster sample accumulation), more profitable captures (`12/20` vs `11/20`), higher Ex-Best-Cap (`+$37.54` vs `+$35.35`), and better Ex-Worst-Cap (`+$62.90` vs `+$61.44`).
   - **Better drawdown & settlement drag:** Both share the exact same `$9.71` (`19.4%`) max drawdown, while `#6/9` lowers settlement drag (`19.8%` vs `26.5%`), recovers `+$4.10` inside `#3`'s worst capture (`Cap#11`: `-$0.75` vs `-$4.85`), and outperforms `#3` in **`10` of the `12` captures** where the two differ (`Cap#1,2,5,6,9,11,12,13,16,17` vs `#3` winning only `Cap#7` by `+$0.71` and `Cap#14` by `+$8.02`).
   - **Plateau centroid:** `#6/9` sits at the interior of `{0.60, 0.65, 0.70}` with the highest neighbor mean Net0 (`+$50.37` across `#3, #5, #9`).
   - **Where `#3/9` wins:** `#3/9` is more capital-efficient (`+6.73%` vs `+5.82%` overall ROI; `+7.83%` vs `+7.52%` OOS ROI) and has half the negative merge losses (`$10.42` vs `$20.42`), largely because `#6/9` took one bad `-$4.11` adverse capture in `Cap#14`.

### 4.3 Criticality: Why `#6/9` Is a *Provisional* Research Baseline (Not Production-Validated)

The `+2.2`-point weighted margin (`79.9` vs `77.7`) over `#3/9` rests on a **226-window sample** with **`336` fills** (below [strategy-verification-spec.md](strategy-verification-spec.md#L159)'s `400`-fill minimum) where `20` captures are heavily influenced by `Cap#20` (`+$21.24`, `36.1%` of `#6`'s total Net P&L). Therefore, `#6/9` is promoted strictly as the **next research baseline**, while `#3/9` remains the top benchmark challenger for independent sample validation.

**This holds after revalidation.** Replaying `21` captures on the corrected `5cf74b9` binary (`351` fills, `241` windows) gives `+$31.68` IS / `+$1.61` OOS, `11.66%` retention and `39.11% / 63.08%` drag/tail — a G3 and G5 failure. That failure is not evidence against `$0.65 / 120s`: it is the same config on both sides of the comparison, and at `351` fills the estimate is undecidable. One `-$9.00` mandatory flatten moves retention from `12%` to `78%`; leave-one-capture-out spans `-30.7%` to `78.1%`; the OOS mean is `+$0.0220`/window with `t = 0.13` (`95%` CI `[-$0.3247, +$0.3686]`). Under Filter 4 the verdict is therefore `UNDERPOWERED`. Consequences: keep the values, gather captures past the `400`-fill bar with OOS spread across several days, **do not** re-promote from this grid (its ranking still contains `Cap#20`'s `-$10.50` timing-bug profit, which `5cf74b9` removed), and **do not** loosen the `50% / 30% / 50%` thresholds. Full analysis: [sweep-results-investigation.md](sweep-results-investigation.md) Part III.

---

## 5. Promoted Defaults & Reproducibility

Promoted cleanly to [DEFAULTS.json](../DEFAULTS.json) (with strict JSON `"provenance"` object validated by `DefaultsProvenance` in `crates/core-types/src/config.rs`) — a provisional promotion: the values are unchanged and remain unvalidated at the `400`-fill bar (§4.3), and synchronized across all six calibration files in the surface at selection time (`DEFAULTS.json`, `.env.example`, `crates/core-types/src/config.rs`, `crates/core-types/src/lib.rs`, `crates/cli/src/main.rs`, `scripts/sweep-params.ps1`; see `AGENTS.md` §10):
- `max_entry_price = 0.65` (`MAX_ENTRY_PRICE=0.65`)
- `entry_cutoff_secs = 120` (`ENTRY_CUTOFF_SECS=120`)
- `entry_open_delay_secs = 15` (`ENTRY_OPEN_DELAY_SECS=15`)
- `cap_tier1_reducing_to_delta = true` (`CAP_TIER1_REDUCING_TO_DELTA=true`)
- `tier1_hedge_trigger_price = null` (`TIER1_HEDGE_TRIGGER_PRICE=off`)

---

## 6. Next Research Phase Roadmap *(Recommendations)*

### A. Historical Dataset Expansion via PMData (Respecting Fidelity Boundaries)

Per the Phase 0 historical verification audit ([pmdata-phase0-historical-report.md](pmdata-phase0-historical-report.md) and [pmdata/reconcile.md](pmdata/reconcile.md)):
- **Hard fidelity limits (cannot skip):**
  1. **21-day free-tier cutoff:** Trailing `21` days return `HTTP 403`; live captures from `2026-11-22..25` cannot be reconciled against PMData until **`>= 2026-12-13..16`**.
  2. **Missing live-latency semantics:** PMData rows carry `mono_ns = null`, `clock_sync = null`, `EstimatedReplayClock`, and **`MissingBinanceSpot`**. Never use PMData to tune lead-lag latency (`lead_lag_window_ms`) or sub-100ms queue priority.
  3. **Resolution gap (`2026-08-07`–`2026-11-17`):** Some markets omit `market_resolved`, requiring explicit catalog/external verification rather than inferred winners.
  4. **Quota cap (`650` units):** Full daily bundles cost `216` units (`5m`), `72` (`15m`), `18` (`1h`); targeted single-market downloads cost `3` units (`full` mode). Keep strict `source-filter` separation (`live_jsonl` vs `pmdata`) and never pool without provenance flags.

**Staged expansion & split protocol:**
1. **Reconciliation Gate First (`>= Dec 16`):** Run `poly-as pmdata reconcile` on at least `3–5` overlapping `2026-11-22..25` live captures (`Cap#6, #11, #14, #20`) to verify whether PMData passes the 10-axis `CompatibleWithLimitations` bar before trusting historical L2 replay.
2. **Stage 1 (`+300`–`500` historical `5m` windows, ~`2–3` days across distinct UTC sessions via single-market or selective day bundles within the `650` cap):** Run `#6/9` (`$0.65`) and `#3/9` (`$0.60`) head-to-head on disjoint temporal splits:
   - **Development (`40%` chronological):** exploratory diagnostics only.
   - **Validation (`30%` chronological):** model/parameter comparison (`#6` vs `#3`, bounded cross-tick loss).
   - **Untouched Holdout (`30%` final chronological):** locked single-shot verification; **never** re-select parameters after opening holdout.
3. **Stage 2 (`+150`–`200` `15m` windows & `+50` `1h` windows, lower quota cost at `72`/`18` per day):** Evaluate longer horizons *only* after scaling lifecycle time ratios (`entry_open_delay_secs`, `entry_cutoff_secs`, `pre_expiry_lockout_secs`) proportionally rather than blindly applying `5m` `120s` cutoffs.

### B. Adversarial Stress Testing of `Config #6/9`

Prioritize failure modes already exposed by the `226w` replay:
1. **Twin 2 Latency Jitter (`40ms -> 250ms` REST/WS spikes):** Verify `#6/9`'s `$0.65` adverse selection (`MO5s = +70.5 bps`, `$20.42` negative merges) stays positive when cancel propagation slows.
2. **Pre-Flatten Book Drought Simulation:** Test empty opposing ask / bid books during `MandatoryFlatten` (`tau = 30..15s`) as seen in `btc-updown-5m-1795517700` (`-$9.60`).
3. **Lead-Lag Feed Dropout (`MissingBinanceSpot` & stale clock):** Verify safe quote widening/suppression without stale-fair-value toxic fills.
4. **Fee & Maker-Rebate Shock:** Stress `0%` floor vs `1%–3%` tier and `2×` aggressive taker flatten fee sensitivity.
5. **Partial-Fill Dust (`< 5sh`) Escalation:** Replay bursty `1–4` share partial fills to quantify residual expiry exposure when `MandatoryFlatten` cannot legally send `< 5sh` orders.

### C. Targeted Follow-Up Experiments (Zero Broad Grid Search)

1. **Fresh Live Capture Bake-Off (`+150`–`250` new live `5m` windows, ~`12–20h` recording):** Freeze `#6/9` (`$0.65, 120s`) and `#3/9` (`$0.60, 120s`) and test exclusively on brand-new live captures (`> 400` fills target for both) to resolve their `+2.2`-pt tie without snooping the existing `226w` set.
2. **Bounded Cross-Tick Loss Grid on `$0.65` (`3` runs):** Vary only `MaxCrossTickLossMicros ∈ {40000, 60000, 80000}` holding `$0.65, 120s, T1=off` fixed to test whether tightening cross-tick loss trims `#6/9`'s `$20.42` negative merges (`Cap#14`) without starving pair completions.
3. **Early-Epoch Entry Delay Check (`3` runs, once live sample > `400` fills):** Test `EntryOpenDelays ∈ {15, 30, 45}` specifically to see whether skipping opening-15s chop avoids instant one-way crash traps like `1795517700`.

### D. Live Execution Readiness Checklist (Mandatory Before Canary Capital)

Paper/replay gate approval is **not** sufficient for live trading. Before any `canary-live` capital:
- [ ] **NEX Sandbox end-to-end soak:** Run `poly-as sandbox health|markets|book|validate|feeds` under sustained sessions (`EIP-712` DepositWallet1271 sig type 3, L2 HMAC auth, heartbeat timer, cancel/replace FSM).
- [ ] **Live queue vs FIFO paper calibration:** Compare real maker fill rates and `MO1s`/`MO5s` markouts on `$10–$25` micro-orders against conservative FIFO replay predictions.
- [ ] **Partial-fill & exchange inventory reconciliation:** Verify real-time CLOB balance/position polling reconciles partial fills and prevents double-merging or over-hedging after missed WS acks.
- [ ] **Latency SLA & cancel reliability:** Measure real REST cancel p95/p99 (`<= 150ms`) under Binance lead-lag triggers.
- [ ] **Kill switches & crash recovery:** Test mid-window `Ctrl+C` / process restart inventory re-hydration, max-loss session circuit breaker, and stale-book halt.
- [ ] **On-chain settlement & fee verification:** Reconcile Relayer v2 gasless merge receipts, actual USDC settlement payouts, and actual exchange maker rebates against `PaperLedger` predictions.

### E. Prioritized Strategy Research Beyond Parameter Tuning

| Priority | Idea | Target Problem Observed | Research Value | Impl. Cost |
|---:|---|---|---:|---:|
| **1** | **Early-window jump/volatility entry veto** | `1795517700` filled at `t=15s` right before a vertical move to `$0.99`, costing `-$9.60` in all `9` configs. | High | Low |
| **2** | **Bounded / price-capped T1 hedge redesign** | Current un-capped `T1` latch chases `$0.80–$0.89` cleanly, blowing tail merges (`$22–$48`). A bounded max pair cost (`<= $1.08–$1.12`) or single-shot budget might rescue `$0.48` positions safely. | High | Medium |
| **3** | **Order-book imbalance (OBI) entry skew** | `Cap#14` / `Cap#20` divergence at `$0.65` vs `$0.70` shows taker toxicity increases on asymmetric thin opposite books. | Medium–High | Low–Medium |
| **4** | **Dynamic inventory-aware hedge sizing** | Scaling deficit-leg aggression smoothly with `abs(q_up - q_dn)` and remaining time-to-cutoff instead of binary latch switches. | Medium | Medium |
| **5** | **Residual-dust (`< 5sh`) aggregation / fill-floor guard** | `$14.02` (`#3`)–`$10+` (`#6`) structural settlement loss comes from un-flattenable `< 5sh` remainders. | Medium | Medium–High |
