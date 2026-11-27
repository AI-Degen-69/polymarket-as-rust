# Merge-Edge Retention Feasibility Investigation

**Date:** 2026-11-26  
**Baseline Config:** `Config #6/9` (`gamma=0.12`, `max_entry_price=$0.65`, `entry_cutoff_secs=120s`, `tier1_hedge_trigger_price=off`, `QueueDecay=conservative`) in [DEFAULTS.json](../DEFAULTS.json)  
**Primary Corpus:** 21 replayed captures (`241` windows) from `logs/sweeps/26-11_07-55-07Z_1runs_21caps-241w/sweep-manifest.json`  
**Reproducibility Script:** [analyze_merge_edge_feasibility.py](../scripts/analyze_merge_edge_feasibility.py) (`python scripts/analyze_merge_edge_feasibility.py`)

---

## A. Executive Conclusion

1. **Is the current window-level Net P&L retention gate suitable?** **No.** Window Net P&L combines two opposing forces with high variance (`sd = $1.3672/w` full, `$1.4883/w` OOS). Its `OOS / IS` retention ratio (`11.66%`) is statistically unstable because the IS mean (`+$0.1886/w`) has a `53.7%` relative standard error. Achieving 80% power to detect a 50% drop in Net P&L requires `1,300–1,541` IID OOS windows (`1,773` cluster-adjusted), and the current `73`-window OOS segment has `t = +0.13` (`95% CI: [-$0.3194, +$0.3634]`).
2. **Is `merge.realized_gain_micro` a valid replacement on its own?** **No.** It isolates a strong, repeatable pair-merging edge (`+$65.2415` across `1,090.43` pairs, `+$0.2707/w`, `t = +3.89`, `P(mean <= 0) = 0.0000`, `87.25%` OOS retention). However, using it alone hides a statistically significant structural loss: terminal settlement on unpaired inventory lost money in **34 of 40** non-zero settlement windows (`85.0%` loss rate), producing `-$33.5607` (`-$0.1393/w`, `t = -2.61`, capture-block bootstrap `P(mean <= 0) = 99.89%`). Furthermore, `merge.realized_gain_micro` is gross of taker fees when a BUY-side flatten creates a merge.
3. **What did the existing dataset actually reveal?** The strategy has **both** a genuine repeatable pair-merging edge (`+$0.2816/w` IS, `+$0.2457/w` OOS) **and** a real adverse-selection settlement drag (`-$0.1026/w` IS, `-$0.2237/w` OOS) with **zero executed mandatory flattens** (`0` flattens across `241` windows). Overall Net P&L (`+$33.2864`, `+$0.1381/w`) sits near the margin of statistical noise (`t = +1.57`, capture-block bootstrap `P(mean <= 0) = 8.68%`).
4. **What remains uncertain?**
   - Whether true long-run Net P&L is positive after adverse-selection settlement drag (`95%` capture-block bootstrap CI spans `-$0.0586/w` to `+$0.3396/w`).
   - How much taker fee and slippage mandatory flattens incur under live pressure, because `0` flattens fired in the `21`-capture baseline replay.
   - Whether the `73`-window OOS drop in Net P&L (`+$1.6057`, driven by a `-$9.00` single-leg loss in `win#238`) reflects transient variance or regime sensitivity on `26-11`.

### Direct Answers to the Five Core Research Questions

| Research Question | Evidence-Based Answer |
| :--- | :--- |
| **1. Are we confusing repeatable edge with random settlement?** | **Partially.** Settlement is not zero-mean noise. Residual inventory loses `85.0%` of the time (`-$0.1393/w`, `P(mean <= 0) = 99.89%`), proving directional adverse selection. Still, binary settlement variance (`sd = $1.1996/w` OOS) drowns out the steady merge signal. |
| **2. Does pair-merge gain isolate edge or hide risk?** | **It isolates execution edge, and hides inventory risk.** Merge gain correlates weakly with settlement PnL (`r = +0.0031` across `241w`), so it ignores the unmerged leg left behind when the opposite leg runs away. |
| **3. Can a metric capture edge while penalizing inventory risk?** | **Yes, via complementary gates, not one blended ratio.** Pair net maker merge edge (`$/pair` and `$/window`) with an explicit bound on realized settlement plus flatten drag (`ExpiryDrag`, `TailDrag`, and realized Net P&L floor). |
| **4. What does the existing dataset tell us right now?** | Maker pair merging works (`+$0.05983/pair`, `87.25%` OOS retention), mandatory flattening did not trigger (`0` events), and adverse-selection settlement erases `51.4%` of gross merge gains (`91.0%` OOS). |
| **5. Smallest defensible change for next OOS validation?** | Freeze the `21` captures as IS, seal Capture `#22` plus new multi-day captures in a SHA-256 Holdout Vault, fix three telemetry gaps (merge provenance, realized-only window gating, per-window merge gain), and gate on complementary merge-edge + drag + realized-PnL criteria. |

---

## B. Metric Audit: Lifecycle of `merge.realized_gain_micro`

### Source-Code Trace and Accounting Semantics

1. **Where calculated:** `PaperLedger::execute_pair_merge` in [accounting.rs](../crates/paper-sim/src/accounting.rs#L194-L266).
2. **Which component emits it:** `PaperLedger::record_fill` ([accounting.rs](../crates/paper-sim/src/accounting.rs#L116-L188)) and `PaperLedger::record_taker_flatten_with_merge` ([accounting.rs](../crates/paper-sim/src/accounting.rs#L285-L401)) call `execute_pair_merge` whenever `min(filled_up, filled_dn) > 0`. Both callers forward `MergeEvent` to `LifecycleEngine::record_merge_event` in [lifecycle_cmd.rs](../crates/cli/src/lifecycle_cmd.rs#L1581-L1613), which writes `"event":"merge"` via `LifecycleMergeEvent` in [lifecycle_report.rs](../crates/cli/src/lifecycle_report.rs#L276-L285).
3. **Unit represented:** One atomic `MergeEvent` triggered by a single fill or BUY-side flatten step. One event merges `pairs = min(filled_up, filled_dn)` micro-shares (`merged_pairs_micro`), which can span fractional shares up to the full paired inventory.
4. **Gross or net of fees:**
   - For passive **maker fills**, Polymarket maker fee is `$0.00`, so `realized_gain_micro` equals net trading gain before optional `estimated_rebate_micro` (`3%` memo field).
   - For **taker BUY flattens** (`record_taker_flatten_with_merge`), `execute_pair_merge` runs **without deducting the taker fee**. The fee (`fee_micro`) is subtracted separately from `realized_pnl_micro` at [accounting.rs#L297-L300](../crates/paper-sim/src/accounting.rs#L297-L300). Thus `merge.realized_gain_micro` is **gross of taker fees** on flatten-triggered merges.
   - `LifecycleMergeEvent` contains no provenance field (`is_maker` or `source`) to distinguish maker-fill merges from taker-BUY flatten merges.
5. **Partial merges:** Handled proportionally by VWAP inventory pooling. When `pairs < total_up` (or `total_dn`), `cost_up_deducted = floor(pairs * cost_basis_up / total_up)` and `cost_dn_deducted = floor(pairs * cost_basis_dn / total_dn)` ([accounting.rs#L202-L205](../crates/paper-sim/src/accounting.rs#L202-L205)). Remaining shares keep the exact remaining `cost_basis` (`cost_basis.saturating_sub(cost_deducted)`).
6. **Zero, failed, reversed, or unmatched operations:**
   - `record_fill` only calls `execute_pair_merge` when `pairs_to_merge > 0`. Zero-share merges cannot emit.
   - Cross-tick losses (`pair_cost > $1.00`) produce negative `realized_gain_micro` (`80` events, `-$20.5644`).
   - Failed flattens emit `"event":"flatten_unfilled"` and never touch `PaperLedger`.
   - Unmatched single-leg positions never trigger `execute_pair_merge`. They stay in `filled_up`/`filled_dn` until a SELL flatten or `settle_epoch`.
7. **Double-counting risk:** **None inside `PaperLedger`.** Every merged pair decrements both `filled_up` and `filled_dn` (`saturating_sub(pairs)`), clears corresponding `cost_basis` (zeroing cleanly when `filled == 0`), and increments `realized_pnl_micro` and `total_merge_gain_micro` once.
8. **Relation to cost basis and proceeds:**
   - Exact formula: `redemption_micro = floor(pairs * 1_000_000 / 1_000_000) = pairs`, and `realized_gain_micro = redemption_micro - (cost_up_deducted + cost_dn_deducted)` ([accounting.rs#L214-L217](../crates/paper-sim/src/accounting.rs#L214-L217)).
   - Note integer truncation: `pair_cost_micro` rounds `avg_cost_up` and `avg_cost_dn` independently before summing. Across all `255` merges, `|realized_gain_micro - floor(pairs * (1e6 - pair_cost) / 1e6)|` reaches up to `15` micro-USDC (`$0.000015`). Always use `realized_gain_micro` directly rather than reconstructing from `pair_cost_micro`.
9. **Execution costs, fees, slippage:** Fill price slippage is embedded in `cost_basis` because fills record at actual simulated FIFO execution prices. Gas is `$0` (Polymarket Relayer v2 gasless merge, [strategy-verification-spec.md](strategy-verification-spec.md#L38)). Taker fees on BUY flattens are **excluded** from `merge.realized_gain_micro`, and SELL-side flatten PnL (`proceeds - cost_deducted - fee`) is **excluded** from merge accounting.
10. **Ledger reconciliation:** Exact across all `21` captures and `241` windows (`0` reconciliation errors).

### Additional Audit Findings (Schema and Window Accounting)

- **Incomplete-window MTM contamination:** At replay EOF, [lifecycle_cmd.rs#L2817-L2885](../crates/cli/src/lifecycle_cmd.rs#L2817-L2885) emits `"event":"window_end"` with `complete: false` and sets `net_pnl_micro = window_realized + unrealized_pnl_micro`. Across `16` incomplete trailing windows, Window `#33` (`22-11_23-15-00Z_30m_6w`, `btc-updown-5m-1795390800`) injects `+$1.6056` of unsettled mark-to-market profit into IS Net P&L (`realized=-$0.1303`, `unrealized=+$1.6056`, `net=+$1.4753`).
- **Missing per-window merge breakdown in `window_end`:** `WindowEndEvent` ([lifecycle_report.rs#L323-L345](../crates/cli/src/lifecycle_report.rs#L323-L345)) records `merges_count` and `total_pairs_merged_micro`, but **not** per-window `merge_gain_micro`, `positive_merge_gain_micro`, or `negative_merge_loss_micro`. Computing window-level merge gains currently requires scanning raw `"event":"merge"` rows.
- **Schema evolution across logs:** The `21`-capture replay logs (`5cf74b9` binary) omit newer `WindowEndEvent` fields (`flatten_attempts`, `residual_q_up_micro`, `max_abs_unpaired_micro`) present in HEAD `lifecycle_report.rs`. Scripts must read optional fields defensively.

---

## C. Measured Results (Existing Dataset)

### 1. Capture Inventory & Untouched Holdout Audit

| Inventory Category | Count | Details |
| :--- | :---: | :--- |
| Complete `.jsonl` captures on disk | **22** | Verified in `logs/captures` (`22-11` through `26-11_07-40-00Z`) |
| `.inprogress.jsonl` capture on disk | **1** | `26-11_14-45-01Z_1h15m.inprogress.jsonl` (excluded) |
| Replayed captures in `26-11_07-55-07Z` | **21** | `241` windows total (`168` IS / `73` OOS), `351` fills, `0` flattens |
| Untouched complete captures | **1** | `26-11_07-40-00Z_1h15m_15w.jsonl` (zero matches in `logs/sweeps` or `logs/lifecycle`) |

### 2. Data Quality & Coverage

- **Capture coverage:** `20` of `21` captures contain merge events (`Cap #4`, `22-11_16-40-00Z_29m59s_6w`, had `1` fill and `0` merges).
- **Window coverage:** Out of `241` windows, `83` (`34.4%`) had fills, `75` (`31.1%`) had merge events, and `166` (`68.9%`) had **zero merge activity** (`158` had zero fills).
- **Accounting completeness:** `100%` (`255 / 255`) of merge events have full `merged_pairs_micro`, `avg_cost_up_micro`, `avg_cost_dn_micro`, `pair_cost_micro`, and `realized_gain_micro` fields, and reconcile with `0` errors against `summary` and `window_end` events.
- **Provenance match:** Timestamp `(ts_ms, mono_ns)` matching confirms all `255 / 255` merges originated from maker fills (`0` from taker-BUY flattens).

### 3. Statistical Unit Warning

Per-pair (`n = 1,090.43`) and per-event (`n = 255`) counts **are not independent observations**:
- One fill against resting inventory often creates one merge batch spanning `10–20+` pairs (`255` events across only `75` windows).
- Within a single 5-minute window, consecutive merges share the same BTC volatility regime, order-book spread, and VWAP inventory pool (`lag-1` window autocorrelation `ACF(1) = +0.066` for merge gain, `+0.048` for Net PnL).
- Windows inside the same 75-minute capture share session regime (`DEFF_cap = 1.28x` for merge gain, `1.36x` for Net PnL, `1.76x` for settlement PnL).
- **Appropriate unit:** Report per-pair economics for economic magnitude (`$/pair`), and use **capture-clustered window-level observations** (`n = 241`, or `n = 75` merge-active windows for conditional spread quality) for statistical inference.

### 4. Pair-Merging Distribution (`n = 255` Events, `1,090.43` Pairs)

| Unit | Count | Mean | Median | Std Dev | Win / Zero / Loss | Min (p0) | p10 | p25 | p75 | p90 | Max (p100) |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **Per Merge Event (`$`)** | `255` | `+$0.2558` | `+$0.0500` | `$0.9051` | `62.7% / 5.9% / 31.4%` (`160/15/80`) | `-$8.0256` | `-$0.1303` | `-$0.0132` | `+$0.3500` | `+$0.8886` | `+$4.7476` |
| **Per Pair, Pair-Weighted (`$/pair`)** | `1,090.43` | `+$0.05983` | `+$0.03000` | `$0.13690` | `62.0% / 4.4% / 33.6%` (`676.25/48.18/366.00`) | `-$0.5700` | `-$0.0500` | `-$0.0200` | `+$0.1181` | `+$0.2500` | `+$0.5100` |
| **Per Pair, Unweighted Event (`$/pair`)** | `255` | `+$0.06392` | `+$0.03000` | `$0.12480` | `62.7% / 5.9% / 31.4%` | `-$0.5700` | `-$0.0500` | `-$0.0200` | `+$0.1191` | `+$0.2500` | `+$0.5100` |
| **Per Merge-Active Window (`$`)** | `75` | `+$0.8699` | — | `$1.8053` | — | — | — | — | — | — | — |
| **Per All Windows (`$`)** | `241` | `+$0.2707` | `$0.0000` | `$1.0806` | `68.9%` zero-merge windows (`166/241`) | — | — | — | — | — | — |

### 5. Per-Capture Breakdown (`21` Captures, `241` Windows)

| Cap# | Capture Stem | Split | Win | Fills | Merges | Pairs | Merge Gain (`$`) | Gain/Pair (`$/p`) | Settlement (`$`) | Unrealized (`$`) | Net PnL (`$`) |
| :---: | :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| `1` | `22-11_10-10-00Z_29m59s_6w` | IS | `6` | `7` | `4` | `6.92` | `+$0.3079` | `+$0.0445` | `-$1.7910` | `$0.0000` | `-$1.4831` |
| `2` | `22-11_14-25-00Z_25m_5w` | IS | `5` | `38` | `32` | `73.33` | `+$1.9138` | `+$0.0261` | `-$0.9910` | `$0.0000` | `+$0.9228` |
| `3` | `22-11_15-35-00Z_25m_5w` | IS | `5` | `3` | `2` | `0.02` | `-$0.0006` | `-$0.0300` | `-$4.7952` | `$0.0000` | `-$4.7958` |
| `4` | `22-11_16-40-00Z_29m59s_6w` | IS | `6` | `1` | `0` | `0.00` | `$0.0000` | `n/a` | `-$0.0041` | `$0.0000` | `-$0.0041` |
| `5` | `22-11_19-45-00Z_30m_6w` | IS | `6` | `40` | `34` | `83.71` | `+$4.1231` | `+$0.0493` | `-$1.5929` | `$0.0000` | `+$2.5302` |
| `6` | `22-11_23-15-00Z_30m_6w` | IS | `6` | `18` | `13` | `50.67` | `+$9.9726` | `+$0.1968` | `$0.0000` | `+$1.6056` | `+$11.5782` |
| `7` | `23-11_00-55-00Z_30m_6w` | IS | `6` | `16` | `11` | `30.01` | `-$0.5139` | `-$0.0171` | `-$1.1822` | `$0.0000` | `-$1.6961` |
| `8` | `23-11_01-40-00Z_49m59s_10w` | IS | `10` | `22` | `16` | `100.48` | `+$10.1066` | `+$0.1006` | `-$0.3681` | `$0.0000` | `+$9.7385` |
| `9` | `23-11_05-45-00Z_1h14m59s_15w` | IS | `15` | `10` | `7` | `26.56` | `+$0.7167` | `+$0.0270` | `-$1.2159` | `$0.0000` | `-$0.4992` |
| `10` | `23-11_15-50-00Z_1h15m2s_15w` | IS | `15` | `10` | `8` | `36.27` | `+$4.5936` | `+$0.1267` | `-$0.5796` | `$0.0000` | `+$4.0140` |
| `11` | `24-11_10-15-00Z_58m31s_12w` | IS | `12` | `15` | `9` | `45.86` | `+$5.4632` | `+$0.1191` | `-$4.9200` | `$0.0000` | `+$0.5432` |
| `12` | `24-11_11-40-00Z_1h15m_14w` | IS | `14` | `15` | `10` | `54.40` | `+$5.8816` | `+$0.1081` | `-$0.4545` | `$0.0000` | `+$5.4271` |
| `13` | `24-11_13-40-00Z_1h14m59s_15w` | IS | `15` | `12` | `8` | `50.00` | `+$0.3450` | `+$0.0069` | `-$0.1875` | `$0.0000` | `+$0.1575` |
| `14` | `24-11_15-30-00Z_1h15m_15w` | IS | `15` | `17` | `13` | `62.81` | `-$3.3188` | `-$0.0528` | `-$0.0437` | `$0.0000` | `-$3.3625` |
| `15` | `24-11_16-55-00Z_1h14m59s_15w` | IS | `15` | `9` | `5` | `34.83` | `+$5.7679` | `+$0.1656` | `-$0.7728` | `$0.0000` | `+$4.9951` |
| `16` | `24-11_19-45-00Z_1h14m59s_15w` | IS | `15` | `30` | `24` | `90.56` | `+$1.9476` | `+$0.0215` | `+$1.6673` | `$0.0000` | `+$3.6149` |
| `17` | `24-11_21-35-00Z_1h14m59s_15w` | SPLIT (`2` IS / `13` OOS) | `15` | `16` | `12` | `59.14` | `-$0.1051` | `-$0.0018` | `+$1.8319` | `$0.0000` | `+$1.7268` |
| `18` | `24-11_23-05-00Z_1h14m59s_15w` | OOS | `15` | `2` | `1` | `20.00` | `-$0.4000` | `-$0.0200` | `$0.0000` | `$0.0000` | `-$0.4000` |
| `19` | `25-11_10-35-01Z_1h14m59s_15w` | OOS | `15` | `13` | `8` | `72.16` | `+$2.4496` | `+$0.0339` | `-$0.6720` | `$0.0000` | `+$1.7776` |
| `20` | `25-11_12-50-01Z_1h14m58s_15w` | OOS | `15` | `27` | `19` | `90.81` | `+$12.1742` | `+$0.1341` | `-$1.4286` | `$0.0000` | `+$10.7456` |
| `21` | `26-11_00-50-00Z_1h14m59s_15w` | OOS | `15` | `30` | `19` | `101.89` | `+$3.8165` | `+$0.0375` | `-$16.0608` | `$0.0000` | `-$12.2443` |

### 6. Per-Day Breakdown (`5` UTC Days)

| Day | Windows (`m_act`) | Fills | Pairs | Merge Gain (`$`) | Gain/Pair (`$/p`) | Merge/Win (`$/w`) | Settlement (`$`) | Unrealized (`$`) | Net PnL (`$`) | Net/Win (`$/w`) |
| :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| `22-11` | `34` (`13`) | `107` | `214.65` | `+$16.3168` | `+$0.0760` | `+$0.4799` | `-$9.1742` | `+$1.6056` | `+$8.7482` | `+$0.2573` |
| `23-11` | `46` (`14`) | `58` | `193.32` | `+$14.9030` | `+$0.0771` | `+$0.3240` | `-$3.3458` | `$0.0000` | `+$11.5572` | `+$0.2512` |
| `24-11` | `116` (`29`) | `116` | `417.60` | `+$15.5814` | `+$0.0373` | `+$0.1343` | `-$2.8793` | `$0.0000` | `+$12.7021` | `+$0.1095` |
| `25-11` | `30` (`12`) | `40` | `162.97` | `+$14.6238` | `+$0.0897` | `+$0.4875` | `-$2.1006` | `$0.0000` | `+$12.5232` | `+$0.4174` |
| `26-11` | `15` (`7`) | `30` | `101.89` | `+$3.8165` | `+$0.0375` | `+$0.2544` | `-$16.0608` | `$0.0000` | `-$12.2443` | `-$0.8163` |

---

## D. Economic Performance Decomposition

The accounting identity verified across all `241` windows (`0` discrepancy) is:

$$\text{Net PnL} = \underbrace{\text{Maker Merge Gain} + \text{Taker-BUY Flatten Merge}}_{\text{total\_merge\_gain\_micro}} + \text{Taker-SELL Flatten PnL} - \text{Taker Fees} + \text{Settlement PnL} + \text{Incomplete MTM}$$

| Economic Component | IS (`168w`) | OOS (`73w`) | All (`241w`) | Notes |
| :--- | :---: | :---: | :---: | :--- |
| **1. Routine maker pair-merge edge** | `+$47.306300` | `+$17.935200` | `+$65.241500` | `pos = +$85.8059`, `neg = -$20.5644`; `+$0.05983/pair` |
| **2. Taker-BUY flatten merge gain** | `$0.000000` | `$0.000000` | `$0.000000` | `0` BUY-side flattens in dataset |
| **3. Taker-SELL flatten trade PnL** | `$0.000000` | `$0.000000` | `$0.000000` | `0` SELL-side flattens in dataset |
| **4. Trading costs (taker fees)** | `$0.000000` | `$0.000000` | `$0.000000` | Maker fills pay `0%` fee; `0` taker flattens |
| **5. Terminal settlement exposure** | `-$17.231200` | `-$16.329500` | `-$33.560700` | `40` active windows: `6` wins (`+$7.7152`), `34` losses (`-$41.2759`) |
| **Realized Net PnL (`1+2+3+4+5`)** | **`+$30.075100`** | **`+$1.605700`** | **`+$31.680800`** | Exact sum of realized ledger cashflows |
| **6. Other: Incomplete-window MTM** | `+$1.605600` | `$0.000000` | `+$1.605600` | Window `#33` (`complete=false`) unrealized MTM |
| **Reported Net PnL (`@0%` rebate)** | **`+$31.680700`** | **`+$1.605700`** | **`+$33.286400`** | Matches `sum(summary.net_pnl_micro)` to `$0.000000` |
| *Memo: 3% Tier-1 maker rebate* | *`+$0.529402`* | *`+$0.254918`* | *`+$0.784320`* | Excluded from `@0%` floor gate |
| **G5a Expiry Drag / G5b Tail Drag** | `27.24% / 52.45%` | `72.41% / 92.88%` | `39.11% / 63.08%` | `|sett_loss| / pos_merge` and `(sett_loss + neg_merge) / pos_merge` |

### Key Diagnosis: Repeatable Edge vs Structural Adverse Selection

1. **The pair-merging engine works and holds OOS:** Routine merge gain delivers `+$0.2816/w` in IS (`t = +3.28`) and `+$0.2457/w` in OOS (`t = +2.08`), retaining **`87.25%`** per window (`58.73%` per pair across merge-active windows).
2. **Unpaired residual exposure is NOT random 50/50 variance:** Across the `40` windows ending with non-zero settlement PnL, **85.0% (`34 / 40`) lost money** (`-$41.2759`) and only **15.0% (`6 / 40`) won** (`+$7.7152`). The capture-block bootstrap 95% CI for window settlement PnL is `[-$0.3009, -$0.0320]` with `P(mean <= 0) = 99.89%`.
3. **Mandatory flattening never executed (`0` events):** Even in single-leg blowups like `win#238` (`26-11_00-50-00Z`, `btc-updown-5m-1795657800`: bought `20` Down shares at `$0.45`, settled `-$9.0000` when Up won), the engine transitioned through `MandatoryFlatten` (`tau=30s`) without filling a single flatten share.
4. **Consequence:** A `51.4%` slice of all merge gains (`$33.56 / $65.24`) is systematically surrendered to adverse-selection settlement on unmerged inventory (`91.0%` in OOS). Ignoring settlement risk would falsely certify an execution loop whose unhedged tail currently consumes most of its spread edge.

---

## E. Current Gate Reconstruction, Candidate Comparison & Statistical Feasibility

### 1. Exact Current Retention Gate (`scripts/sweep-params.ps1`)

Implemented in `Get-SurvivalInputs` ([sweep-params.ps1#L474-L570](../scripts/sweep-params.ps1#L474-L570)) and `Evaluate-SurvivalGateRow` ([sweep-params.ps1#L577-L610](../scripts/sweep-params.ps1#L577-L610)):
- **IS / OOS split:** Chronological `70 / 30` slice across `WindowNets` (`N_IS = floor(0.7 * N)`, `N_OOS = N - N_IS`). On `241` windows, `N_IS = 168` and `N_OOS = 73`.
- **Formula:** `IS_avg = sum(IS) / N_IS`, `OOS_avg = sum(OOS) / N_OOS`, `Retention = (OOS_avg / IS_avg) * 100`.
- **Gate rules (`G1–G6` & Filter 4):**
  - `G1`: `IS > 0`
  - `G2`: `OOS > 0`
  - `G3`: `Retention >= 50.0%`
  - `G4`: `MaxDD <= 25.0%`
  - `G5`: `ExpiryDrag <= 30.0%` and `TailDrag <= 50.0%`
  - `G6`: Neighbor profitability `>= 60.0%` (requires multi-config grid)
  - **Verdict precedence:** If `Fills < 400`, verdict is forced to **`UNDERPOWERED`** regardless of `G1–G6`. Otherwise first failing gate returns `FAIL(Gx)`, missing `G1–G3` returns `NEEDS-OOS`, missing `G4–G6` returns `N/A`, and all passing returns `APPROVED`.
- **Why retention jumped (`162% -> 99.88% -> 11.66%`):** Documented in [sweep-results-investigation.md](sweep-results-investigation.md). Re-running the same `20` captures (`226w`) on the fixed `5cf74b9` binary removed `-$10.50` of phantom timing-bug profit in `Cap #20` (`162% -> 99.88%`), and appending `Cap #21` (`26-11_00-50-00Z`, `-$12.24` net driven by `-$16.06` settlement) dropped OOS from `+$13.69` to `+$1.61` (`11.66%`). Repeatedly re-evaluating the same chronological tail while appending captures invalidates `G3` as a clean holdout test.

### 2. Candidate Metric Comparison

| Candidate | Formula / Unit | What It Measures | What It Misses | Calculable Now? | Statistical Suitability |
| :--- | :--- | :--- | :--- | :---: | :--- |
| **A: Window Net PnL** | `net_pnl_micro` across all `241w` (`$/w`) | Total bottom-line PnL incl. settlement | Overwhelmed by binary expiry variance; mixes `+$1.61` incomplete MTM | Yes | **Poor.** Needs `1,300–1,773` OOS windows; `CV(IS)=53.7%` makes ratio unstable |
| **A-real: Realized Window PnL** | `realized_pnl_micro` across `241w` (`$/w`) | Clean realized PnL without incomplete MTM | Still overwhelmed by binary settlement variance (`sd=$1.3645/w`) | Yes | **Poor.** Needs `1,437–1,858` OOS windows; same variance bottleneck |
| **B1: Window Merge Gain (All)** | `sum(merge.realized_gain_micro)` across `241w` (`$/w`) | Gross pair-merge edge rate per elapsed window | Misses settlement drag, SELL flattens, and taker-BUY flatten fees | Yes (needs merge scan) | **Moderate.** Needs `364` IID (`467` clustered) OOS windows; `0` in `68.9%` of windows |
| **B2: Merge Gain (Active Win)** | `sum(merge.realized_gain_micro)` on `75` merge-active windows (`$/w`) | Merge profit conditional on activity | Conditioned on post-hoc activity; misses zero-merge single-leg losses | Yes | **Conditional only.** Useful diagnostic (`97` IID active windows), blind to fill rate |
| **B3: Merge Gain per Pair** | `window_merge_gain / pairs` on `75` active windows (`$/pair`) | Execution spread quality per matched pair | Blind to trade volume, fill rate, and all unpaired inventory risk | Yes | **Good secondary diagnostic.** Lowest variance (`46` IID active windows) |
| **C: Net Merge Gain** | `merge_gain - fees` across `241w` (`$/w`) | Merge edge net of taker flatten fees | Still misses SELL-flatten trade losses and unhedged settlement drag | Identical to B1 now (`fees=0`) | **Better than B1** once flattens execute; incomplete without settlement drag |
| **D: Complementary Multi-Gate** | **(1)** Net Maker Merge Edge (`$/w`, `$/pair`) + **(2)** Settlement/Flatten Drag (`ExpiryDrag`, `TailDrag`, `net_sett / net_merge`) + **(3)** Realized Net PnL floor | Separates spread-capture engine from inventory-risk leakage | Requires tagging merge provenance (`maker` vs `flatten`) to stay exact | Needs provenance tag when `flattens > 0` | **Best.** High statistical power on execution edge while blocking toxic unhedged tails |

### 3. Statistical Feasibility & Power Recalculations

Using one-sided $\alpha = 0.05$ ($z_{\alpha} = 1.6449$) and $80\%$ power ($z_{\beta} = 0.8416$, $(z_\alpha + z_\beta)^2 = 6.1826$), detecting a minimum effect $\Delta = 0.5 \times \mu_{\text{IS}}$ (matching the 50% retention threshold) under IID assumptions uses:

$$N_{\text{IID}} = \frac{(z_\alpha + z_\beta)^2 \cdot \sigma^2}{\Delta^2}$$

Cluster-adjusted sample size multiplies $N_{\text{IID}}$ by the capture-level Design Effect ($\text{DEFF}_{\text{cap}} = (\text{SE}_{\text{cluster}} / \text{SE}_{\text{IID}})^2$):

| Metric | IS Mean (`SD`) | OOS Mean (`SD`) | Full Mean (`SD`) | `OOS` `t`-stat (`95% CI`) | Ret. (`OOS/IS`) | `N_IID` (`OOS SD`) | `N_IID` (`Full SD`) | `DEFF_cap` | `N_Clust` (`Full SD`) | Collection Burden (`N_Clust @288w/d`) |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **A: Window Net PnL (`241w`)** | `+$0.1886` (`$1.3125`) | `+$0.0220` (`$1.4883`) | `+$0.1381` (`$1.3672`) | `+0.13` (`[-0.319, +0.363]`) | `11.66%` | **`1,540.5`** | `1,299.9` | `1.36x` | **`1,773.4`** | **`6.16` days** (`2,583` fills) |
| **A-real: Realized PnL (`241w`)** | `+$0.1790` (`$1.3089`) | `+$0.0220` (`$1.4883`) | `+$0.1315` (`$1.3645`) | `+0.13` (`[-0.319, +0.363]`) | `12.29%` | `1,709.4` | `1,436.8` | `1.29x` | `1,858.2` | `6.45` days (`2,706` fills) |
| **B1/C: Merge Gain (`241w`)** | `+$0.2816` (`$1.1134`) | `+$0.2457` (`$1.0082`) | `+$0.2707` (`$1.0806`) | `+2.08` (`[+0.014, +0.477]`) | `87.25%` | `317.0` | `364.2` | `1.28x` | **`467.3`** | **`1.62` days** (`681` fills) |
| **B2: Merge Gain (`75` act-w)** | `+$0.9097` (`$1.8645`) | `+$0.7798` (`$1.7003`) | `+$0.8699` (`$1.8053`) | `+2.20` (`[+0.085, +1.475]`) | `85.72%` | `86.4` | `97.4` | — | — | `~313` total windows (`1.09` days) |
| **B3: Merge/Pair (`75` act-w)** | `+$0.0697` (`$0.1033`) | `+$0.0409` (`$0.0696`) | `+$0.0609` (`$0.0947`) | `+2.82` (`[+0.013, +0.069]`) | `58.73%` | `24.7` | `45.7` | — | — | `~147` total windows (`0.51` days) |
| **D1: Fill-Active Net (`83w`)** | `+$0.5462` (`$2.2019`) | `+$0.0642` (`$2.5773`) | `+$0.4010` (`$2.3160`) | `+0.12` (`[-0.946, +1.075]`) | `11.76%` | `550.6` | `444.6` | — | — | `~1,291` total windows (`4.48` days) |

- **Brainstorming reproduction:** Exact match! With `OOS sd = $1.48834` and `delta = 0.5 * $0.18858 = $0.09429`, $N_{\text{IID}} = 6.18256 \times (1.48834 / 0.09429)^2 = \mathbf{1,540.5}$ OOS windows, equal to **`5.35` days** at `288` windows/day and **`2,244` fills** at the observed `1.456` fills/window (`351 / 241`).
- **Capture-block bootstrap (`10,000` draws, seed `42`):**
  - **Window Net PnL (`241w`):** `95% CI = [-$0.0586, +$0.3396]`, `P(mean <= 0) = 8.68%` (fails 5% significance).
  - **Window Merge Gain (`241w`):** `95% CI = [+$0.1313, +$0.4335]`, `P(mean <= 0) = 0.00%` (highly significant).
  - **Window Settlement PnL (`241w`):** `95% CI = [-$0.3009, -$0.0320]`, `P(mean <= 0) = 99.89%` (statistically significant loss).
- **Instability of the `OOS / IS` Retention Ratio:** Because `IS_mean` itself has high sampling error (`CV = 53.7%` for Net PnL vs `30.5%` for Merge Gain), `OOS / IS` is a ratio of two noisy variables. Under capture-block bootstrap, Net PnL retention spans `[-274.2%, +311.1%]` (5th–95th percentiles) and `IS <= 0` occurs in `2.75%` of draws. Even Merge Gain retention spans `[+14.4%, +234.1%]` (`P(Retain >= 50%) = 74.3%`). **Absolute OOS edge floors with lower confidence bounds (`mean_OOS - t_crit * SE_cluster`) are far more stable than ratios.**

---

## F. Experiment Integrity & Recommendation

### 1. Holdout Contamination Assessment

The `21`-capture corpus (`241` windows) has been repeatedly inspected across `180` boundary-grid runs, binary timing fixes, and this diagnostic study. **It is permanently burned as an out-of-sample holdout** and must be frozen as the **Development / In-Sample** dataset.

Fortunately, Capture `#22` (`26-11_07-40-00Z_1h15m_15w.jsonl`) has **never been replayed** in `logs/sweeps` or `logs/lifecycle` and can seed a clean Holdout Vault.

### 2. Defensible Holdout Protocol

1. **Freeze IS vs OOS boundary:** Lock all `21` existing captures (`22-11_10-10-00Z` through `26-11_00-50-00Z`) as Development/IS. Place Capture `#22` (`26-11_07-40-00Z`) plus all newly collected complete captures into a dedicated Holdout Vault (`logs/holdout/` or manifest-listed files).
2. **SHA-256 manifest:** Generate `holdout-manifest.sha256` recording filename, byte size, UTC time span, and SHA-256 hash for every holdout file before any run.
3. **Frozen code and gate commit:** Before unlocking the holdout, commit the exact engine binary git SHA, [DEFAULTS.json](../DEFAULTS.json), and frozen evaluation script to git.
4. **Inspection ledger & one-time unlock:** Maintain `docs/holdout-inspection-ledger.md`. Each holdout unlock logs: date, git SHA, manifest SHA-256, gate thresholds, and verdict. Once evaluated, all files in that manifest move to Development/IS and a fresh holdout batch is collected for any subsequent parameter change.
5. **Minimum informative OOS target:** Collect at least **`468` OOS windows (`~39` 1h captures, `~1.6` days across Asia/London/US sessions, `~680+` fills)** before running the one-time holdout gate. `468` is a planning estimate for detecting a positive mean against zero at `80%` cluster-adjusted power (`DEFF = 1.28x`), and gives `>= 140` merge-active windows to bound `ExpiryDrag` and `TailDrag`. It does not size Gate 1's absolute floors (`>= +$0.14/w`, `>= +$0.03/pair`). Before opening the holdout, estimate the chance of passing the full Gate 1 rule at a stated alternative using complete windows.

### 3. Recommendation

**Recommendation: Replace the single Net P&L retention gate with multiple complementary gates (do not use `merge.realized_gain_micro` alone).**

Specifically, structure the OOS validation around three complementary gates evaluated on `complete=true` windows only:

1. **Gate 1 — Repeatable Execution Edge (Primary Statistical Power Gate):**
   - Measure **Net Maker Pair-Merge Gain** (`maker_merge_gain_micro / window` and `$/pair`), excluding taker-BUY flatten merges unless net of flatten fees.
   - Replace the unstable `OOS / IS >= 50%` ratio check with a dual criterion: absolute OOS floor (`>= +$0.14/w` and `>= +$0.03/pair`) **and** cluster-robust lower 90% confidence bound `> $0.00` (one-sided `p < 0.05`).
2. **Gate 2 — Inventory & Adverse-Selection Risk Containment (Mandatory Guardrail):**
   - Require `ExpiryDrag <= 30%`, `TailDrag <= 50%`, and monitor `Settlement + Flatten Net / Net Merge Gain` (must remain above `-50%`, vs `-51.4%` full / `-91.0%` OOS currently observed).
3. **Gate 3 — Bottom-Line Realized Net P&L Sanity:**
   - Require realized OOS Net P&L (`realized_pnl_micro` summed over `complete=true` windows) `> $0.00` (`@0%` rebate floor) and `MaxDD <= 25%`.

---

## G. Ranked Next Actions

1. **Seal the Holdout Vault now:** Create `holdout-manifest.sha256` starting with the untouched Capture `#22` (`26-11_07-40-00Z_1h15m_15w.jsonl`) and isolate it from all parameter sweeps until `~468` OOS windows across multi-day UTC sessions accumulate.
2. **Fix three telemetry gaps before any holdout unlock:**
   - Add a `source` / `is_maker` field to `LifecycleMergeEvent` ([lifecycle_report.rs](../crates/cli/src/lifecycle_report.rs#L276-L285)) and record `flatten_fee_micro` alongside flatten-triggered merges.
   - Add `merge_gain_micro`, `positive_merge_gain_micro`, and `negative_merge_loss_micro` to `WindowEndEvent` ([lifecycle_report.rs#L323-L345](../crates/cli/src/lifecycle_report.rs#L323-L345)) so window-level merge evaluation requires no full-log JSONL rescan.
   - Exclude `complete=false` trailing windows from `Get-SurvivalInputs` OOS/IS scoring in [sweep-params.ps1](../scripts/sweep-params.ps1#L474-L570) so unrealized MTM (like Window `#33`'s `+$1.6056`) cannot skew realized edge gates.
3. **Diagnose why mandatory flatten never filled (`0` executions) and why `85%` of unpaired settlements lost:** Investigate `MandatoryFlatten` order pricing and book liquidity on the `40` non-zero settlement windows (especially `win#238`, `-$9.0000`) on the frozen `21`-capture IS corpus before tuning or unlocking any OOS holdout.
4. **Update the offline gate script (`sweep-params.ps1`)** to enforce the complementary 3-gate architecture (Maker Merge Edge + Risk Drag + Realized Net PnL) before freezing the commit SHA for the first clean Holdout Vault evaluation.
