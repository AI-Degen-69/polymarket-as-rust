# Deep Investigation of Settlement Losses and Mandatory Flatten Failures

**Date:** 2026-11-26  
**Corpus:** 21 Development/In-Sample captures (`241` windows, `351` fills, `0` flattens) in `logs/sweeps/26-11_07-55-07Z_1runs_21caps-241w/sweep-manifest.json`  
**Baseline Config:** `Config #6/9` in [DEFAULTS.json](../DEFAULTS.json) (`gamma=0.12`, `max_entry_price=$0.65`, `entry_cutoff_secs=120s`, `tier1_hedge_trigger_price=null`, `QueueDecay=conservative`)  
**Reproducibility Script:** [analyze_mandatory_flatten_root_cause.py](../scripts/analyze_mandatory_flatten_root_cause.py) (`rtk python scripts/analyze_mandatory_flatten_root_cause.py`)  
**Guardrail Note:** Capture `#22` (`26-11_07-40-00Z_1h15m_15w.jsonl`) and `.inprogress.jsonl` were not inspected, opened, or replayed.

---

## A. Executive Diagnosis

Across `241` baseline windows, the strategy earned `+$65.2415` from routine Maker Pair-Merging, realized `-$33.5607` in terminal settlement (`40` windows with non-zero settlement), carried `+$1.6056` in incomplete-window MTM (`win#33`), and executed **`0` MandatoryFlatten orders**.

Two distinct, disjoint mechanisms explain **100%** (`40 / 40`) of the zero-flatten settlement outcomes:

**Attribution basis:** `H4` classifies a window from its residual inventory at settlement. It does not re-derive `|q|` on every `MandatoryFlatten` tick. A window can end below `5.0` shares after holding more during the phase, so the classification is a settlement-level attribution, not a per-tick decision record.

1. **The Exchange Minimum-Size Dust Trap (`36 / 40` windows, `-$12.7415` or `38.0%` of settlement PnL — Hypothesis H4):**
   - Thirty-six windows ended with `0 < |q| < 5.0` unpaired shares (either from partial maker fills or partial merges leaving `< 5.0` shares).
   - Polymarket enforces `min_order_size = 5.0` shares (`5_000_000` micro-shares).
   - `EpochLifecycleController::evaluate` ([lifecycle.rs#L305-L311](../crates/risk-lifecycle/src/lifecycle.rs#L305-L311)) explicitly classifies `|q| < 5.0` as `ResidualDustUnflattenable`, suppresses dispatch, and emits `CancelAll`.
2. **Pre-Flatten Book Collapse + Active-Phase Cross-Tick Cap Stranding (`4 / 40` windows, `-$20.8192` or `62.0%` of settlement PnL — Hypotheses H2 & H5):**
   - Only `4` of `40` windows reached settlement with flattenable inventory (`|q| >= 5.0`): `win#238` (`-$9.0000`), `win#15` (`-$4.7952`), `win#86` (`-$4.0000`), and `win#227` (`-$3.0240`).
   - In **all four**, the Polymarket L2 book had already collapsed to an empty one-sided `$0.99 / $0.01` terminal state **24.8s to 60.8s before `MandatoryFlatten` started at `tau = 30s`** (`0` book snapshots with opposite asks or heavy-leg bids during `15s < tau <= 30s`).
   - Upstream of `tau = 30s`, `station4_6_pricer.rs` ([station4_6_pricer.rs#L442-L468](../crates/math-engine/src/station4_6_pricer.rs#L442-L468)) capped passive hedge bids at `$1.06 - vwap` (`max_combined_bid $0.98 + max_cross_tick_loss $0.08`) because `tier1_hedge_trigger_price` is `off` and `allow_cross_tick_override` ([lifecycle_cmd.rs#L2231-L2247](../crates/cli/src/lifecycle_cmd.rs#L2231-L2247)) only activates in `PreExpiryLockout` (`tau <= 60s`) or `Tier3OneSided` (`u > 75%` of `$50` `V_max`).

### Execution Pipeline Stage Audit & Hypothesis Verdict

| Pipeline Stage | Dust Windows (`36/40`) | Eligible Windows (`4/40`) | Source Location |
| :--- | :--- | :--- | :--- |
| **1. Evaluation** | Evaluated every tick in `MandatoryFlatten` (`15s < tau <= 30s`) | Evaluated every tick in `MandatoryFlatten` (`15s < tau <= 30s`) | [lifecycle.rs#L296-L311](../crates/risk-lifecycle/src/lifecycle.rs#L296-L311) |
| **2. Eligibility** | **Rejected (`ResidualDustUnflattenable`)** because `|q| < 5.0` shares | Eligible (`|q| >= 5.0` shares) | [lifecycle.rs#L305-L311](../crates/risk-lifecycle/src/lifecycle.rs#L305-L311) |
| **3. Dispatch** | Not dispatched (`CancelAll` emitted) | **Not dispatched** (`CancelAll: "Mandatory flatten liquidity unavailable"`) | [lifecycle.rs#L344-L378](../crates/risk-lifecycle/src/lifecycle.rs#L344-L378) |
| **4. Feasibility** | `n/a` (sub-minimum size; some had `tau<=30s` book liquidity) | **Zero liquidity** (`opp_ask = None` and `heavy_bid = None` in `15s < tau <= 30s`) | Raw capture L2 audit |
| **5. Matching** | Never reached `match_fak_against_book` | Never reached `match_fak_against_book` | [lifecycle_cmd.rs#L1945-L2084](../crates/cli/src/lifecycle_cmd.rs#L1945-L2084) |
| **6. Accounting** | Settled at expiry (`-$12.7415`) | Settled at expiry (`-$20.8192`) | [accounting.rs#L511-L520](../crates/paper-sim/src/accounting.rs#L511-L520) |

| Hypothesis | Verdict | Evidence |
| :--- | :---: | :--- |
| **H1: Flatten prices too conservative** | **Refuted as primary cause** | Zero opposite asks and zero heavy bids existed at *any* price (`$0.01–$0.99`) during `15s < tau <= 30s` in all `4` eligible windows. |
| **H2: Liquidity disappears before `tau = 30s`** | **Confirmed (Primary for `4/4` eligible)** | Last executable ask/bid vanished at `tau = 63.3s` (`win#238`), `86.1s` (`win#15`), `90.8s` (`win#86`), and `54.8s` (`win#227`). |
| **H3: Wrong side / quantity constructed** | **Refuted** | `lifecycle.rs:326-380` correctly maps `Long Down -> BUY Up at ask / SELL Down at bid` with `size = |q_up - q_dn|`. |
| **H4: Minimum order size blocks execution** | **Confirmed (Primary for `36/40` dust)** | `36` windows (`90%` by count, `38%` by dollars) ended with `0.01–4.29` residual shares (`< 5.0` min order size). Per-tick eligibility during `MandatoryFlatten` was not re-derived. |
| **H5: Risk / lifecycle gates suppress hedges** | **Confirmed upstream** | Active-phase `max_cross_tick_loss = $0.08` + `tier1_hedge_trigger = off` + post-only `CrossesBook` + `DefensiveCancel` froze passive hedging before `tau = 60s`. |
| **H6: Simulator / matching bug blocks fills** | **Not established** | Zero `flatten` events do not show that `match_fak_against_book` was not called. Crossed-book attempts log `flatten_unfilled`. Other attempts call the matcher and may log no event when nothing fills. Refute H6 only with a dispatch record or matcher invocation record for the eligible windows. |
| **H7: Entries created too late or too large** | **Refuted on timing; partial on size** | All `40` exposures began at `tau >= 180.5s` (`entry_cutoff_secs = 120s` worked). Sizing `20` shares (`$4–$9`) stayed below `Tier 3` (`75%` of `$50`), delaying aggressive skew. |

---

## B. Forty-Window Analysis

### 1. Ranked Worst-10 Settlement Windows

| Rank | Win# | Cap# | Slug | Dir / Win | Res Qty | Entry VWAP | Cost Basis | First Unp `tau` | Last Opp Ask `tau` | `MandatoryFlatten` `opp_ask` ticks | Settlement PnL | Root-Cause Class |
| :---: | :---: | :---: | :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :--- |
| `1` | `#238` | `21` | `btc-updown-5m-1795657800` | `Down / Up` | `20.00` | `$0.45` | `$9.00` | `187.0s` | `63.28s` | `0` | **`-$9.0000`** | `H2` + `H5` |
| `2` | `#15` | `3` | `btc-updown-5m-1795362900` | `Up / Down` | `19.98` | `$0.24` | `$4.80` | `180.6s` | `86.11s` | `0` | **`-$4.7952`** | `H2` + `H5` (+ underdog `<$0.35`) |
| `3` | `#86` | `11` | `btc-updown-5m-1795517100` | `Up / Down` | `20.00` | `$0.20` | `$4.00` | `184.1s` | `90.80s` | `0` | **`-$4.0000`** | `H2` + `H5` (+ underdog `<$0.35`) |
| `4` | `#227` | `21` | `btc-updown-5m-1795654500` | `Down / Up` | `9.45` | `$0.32` | `$3.02` | `264.0s` | `54.75s` | `0` | **`-$3.0240`** | `H2` + `H5` (+ underdog `<$0.35`) |
| `5` | `#234` | `21` | `btc-updown-5m-1795656600` | `Up / Down` | `3.36` | `$0.63` | `$2.12` | `185.0s` | `56.84s` | `0` | **`-$2.1168`** | `H4` (Dust) + `H2` |
| `6` | `#237` | `21` | `btc-updown-5m-1795657500` | `Down / Up` | `3.00` | `$0.64` | `$1.92` | `201.0s` | `61.56s` | `0` | **`-$1.9200`** | `H4` (Dust) + `H2` |
| `7` | `#2` | `1` | `btc-updown-5m-1795342800` | `Up / Down` | `3.09` | `$0.58` | `$1.79` | `248.2s` | `25.93s` | `4,820` | **`-$1.7922`** | `H4` (Dust) |
| `8` | `#42` | `8` | `btc-updown-5m-1795398600` | `Up / Down` | `3.55` | `$0.38` | `$1.35` | `237.1s` | `0.00s` | `0` | **`-$1.3490`** | `H4` (Dust) |
| `9` | `#101` | `12` | `btc-updown-5m-1795523100` | `Up / Down` | `2.43` | `$0.43` | `$1.04` | `191.2s` | `37.33s` | `0` | **`-$1.0449`** | `H4` (Dust) + `H2` |
| `10` | `#59` | `9` | `btc-updown-5m-1795415400` | `Up / Down` | `3.44` | `$0.27` | `$0.93` | `207.1s` | `0.00s` | `0` | **`-$0.9288`** | `H4` (Dust) |

### 2. Complete Table of All 40 Settlement-Exposed Windows

| Win# | Cap# | Slug | Dir / Win | Init `|q|` | Max `|q|` | Res `|q|` | VWAP | Cost | 1st Unp `tau` | Last Hedge Bid | Entry Opp Ask (Depth) | Last Opp Ask `tau` | `tau<=30` Opp Ticks | Settle PnL | Primary Class |
| :---: | :---: | :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :--- |
| `#2` | `1` | `btc-updown-5m-1795342800` | `Up / Dn` | `0.01` | `3.09` | `3.09` | `$0.58` | `$1.79` | `248.2s` | `$0.38` | `$0.51 (15,354)` | `25.9s` | `4,820` | `-$1.7922` | `H4:Dust` |
| `#3` | `1` | `btc-updown-5m-1795343100` | `Dn / Up` | `0.01` | `0.01` | `0.01` | `$0.23` | `$0.00` | `270.0s` | `$0.64` | `$0.79 (14,549)` | `28.0s` | `537` | `-$0.0023` | `H4:Dust` |
| `#5` | `1` | `btc-updown-5m-1795343700` | `Up / Up` | `0.01` | `0.01` | `0.01` | `$0.65` | `$0.01` | `181.5s` | `$0.23` | `$0.36 (14,871)` | `0.0s` | `12,176` | `+$0.0035` | `H4:Dust` |
| `#6` | `2` | `btc-updown-5m-1795357500` | `Up / Dn` | `0.01` | `0.01` | `0.01` | `$0.22` | `$0.00` | `181.1s` | `$0.65` | `$0.79 (20,182)` | `61.0s` | `0` | `-$0.0022` | `H4:Dust+H2` |
| `#8` | `2` | `btc-updown-5m-1795358100` | `Dn / Up` | `1.92` | `3.10` | `3.10` | `$0.27` | `$0.84` | `235.4s` | `$0.60` | `$0.53 (1,940)` | `61.4s` | `0` | `-$0.8370` | `H4:Dust+H2` |
| `#9` | `2` | `btc-updown-5m-1795358400` | `Dn / Up` | `1.00` | `3.77` | `0.33` | `$0.46` | `$0.15` | `236.3s` | `$0.60` | `$0.55 (2,695)` | `95.5s` | `0` | `-$0.1518` | `H4:Dust+H2` |
| `#15` | `3` | `btc-updown-5m-1795362900` | `Up / Dn` | `20.00` | `20.00` | **`19.98`** | `$0.24` | `$4.80` | `180.6s` | `$0.81` | `$0.74 (21,397)` | **`86.1s`** | **`0`** | **`-$4.7952`** | **`H2+H5 (Elig)`** |
| `#16` | `4` | `btc-updown-5m-1795365600` | `Dn / Up` | `0.01` | `0.01` | `0.01` | `$0.41` | `$0.00` | `252.4s` | `$0.65` | `$0.60 (18,609)` | `59.1s` | `0` | `-$0.0041` | `H4:Dust+H2` |
| `#22` | `5` | `btc-updown-5m-1795376700` | `Up / Dn` | `2.00` | `19.66` | `0.35` | `$0.21` | `$0.07` | `257.4s` | `$0.28` | `$0.55 (3,289)` | `46.5s` | `0` | `-$0.0735` | `H4:Dust+H2` |
| `#23` | `5` | `btc-updown-5m-1795377000` | `Dn / Up` | `15.00` | `20.00` | `2.02` | `$0.39` | `$0.79` | `202.4s` | `$0.58` | `$0.61 (7,976)` | `21.5s` | `8,713` | `-$0.7878` | `H4:Dust` |
| `#24` | `5` | `btc-updown-5m-1795377300` | `Dn / Up` | `0.01` | `3.74` | `1.24` | `$0.59` | `$0.73` | `274.7s` | `$0.47` | `$0.51 (16,985)` | `13.9s` | `17,734` | `-$0.7316` | `H4:Dust` |
| `#34` | `7` | `btc-updown-5m-1795395300` | `Dn / Up` | `0.01` | `5.01` | `0.01` | `$0.24` | `$0.00` | `231.4s` | `$0.61` | `$0.77 (3,513)` | `0.0s` | `0` | `-$0.0024` | `H4:Dust` |
| `#37` | `7` | `btc-updown-5m-1795396200` | `Dn / Up` | `1.63` | `3.37` | `1.63` | `$0.46` | `$0.75` | `199.8s` | `$0.60` | `$0.57 (5,094)` | `68.8s` | `0` | `-$0.7498` | `H4:Dust+H2` |
| `#38` | `7` | `btc-updown-5m-1795396500` | `Up / Dn` | `5.00` | `5.00` | `1.00` | `$0.43` | `$0.43` | `198.6s` | `$0.63` | `$0.63 (3,475)` | `58.5s` | `0` | `-$0.4300` | `H4:Dust+H2` |
| `#42` | `8` | `btc-updown-5m-1795398600` | `Up / Dn` | `2.27` | `3.55` | `3.55` | `$0.38` | `$1.35` | `237.1s` | `$0.64` | `$0.46 (13,941)` | `0.0s` | `0` | `-$1.3490` | `H4:Dust` |
| `#46` | `8` | `btc-updown-5m-1795399800` | `Up / Up` | `20.00` | `20.00` | `2.88` | `$0.57` | `$1.64` | `251.6s` | `$0.04` | `$0.40 (44,132)` | `0.0s` | `7,684` | `+$1.2384` | `H4:Dust` |
| `#47` | `8` | `btc-updown-5m-1795400100` | `Up / Dn` | `11.91` | `11.91` | `1.03` | `$0.25` | `$0.26` | `212.3s` | `$0.65` | `$0.41 (12,081)` | `0.0s` | `0` | `-$0.2575` | `H4:Dust` |
| `#59` | `9` | `btc-updown-5m-1795415400` | `Up / Dn` | `1.56` | `3.44` | `3.44` | `$0.27` | `$0.93` | `207.1s` | `$0.64` | `$0.53 (8,404)` | `0.0s` | `0` | `-$0.9288` | `H4:Dust` |
| `#62` | `9` | `btc-updown-5m-1795416300` | `Up / Dn` | `20.00` | `20.00` | `0.99` | `$0.29` | `$0.29` | `212.9s` | `$0.61` | `$0.66 (23,315)` | `0.0s` | `6,967` | `-$0.2871` | `H4:Dust` |
| `#65` | `10` | `btc-updown-5m-1795449000` | `Dn / Up` | `20.00` | `20.00` | `3.28` | `$0.27` | `$0.89` | `254.4s` | `$0.64` | `$0.72 (3,926)` | `0.0s` | `0` | `-$0.8856` | `H4:Dust` |
| `#66` | `10` | `btc-updown-5m-1795449300` | `Up / Up` | `20.00` | `20.00` | `0.45` | `$0.32` | `$0.14` | `193.1s` | `$0.37` | `$0.65 (15,335)` | `0.0s` | `48,156` | `+$0.3060` | `H4:Dust` |
| `#86` | `11` | `btc-updown-5m-1795517100` | `Up / Dn` | `15.00` | `20.00` | **`20.00`** | `$0.20` | `$4.00` | `184.1s` | `$0.86` | `$0.81 (4,957)` | **`90.8s`** | **`0`** | **`-$4.0000`** | **`H2+H5 (Elig)`** |
| `#90` | `11` | `btc-updown-5m-1795518300` | `Up / Dn` | `4.00` | `4.00` | `4.00` | `$0.23` | `$0.92` | `235.3s` | `$0.51` | `$0.72 (7,329)` | `23.4s` | `8,025` | `-$0.9200` | `H4:Dust` |
| `#101` | `12` | `btc-updown-5m-1795523100` | `Up / Dn` | `2.43` | `2.43` | `2.43` | `$0.43` | `$1.04` | `191.2s` | `$0.63` | `$0.58 (6,314)` | `37.3s` | `0` | `-$1.0449` | `H4:Dust+H2` |
| `#103` | `12` | `btc-updown-5m-1795523700` | `Up / Up` | `20.00` | `20.00` | `1.64` | `$0.64` | `$1.05` | `188.3s` | `$0.23` | `$0.36 (13,972)` | `0.0s` | `9,330` | `+$0.5904` | `H4:Dust` |
| `#115` | `13` | `btc-updown-5m-1795530300` | `Dn / Up` | `1.75` | `3.25` | `0.75` | `$0.25` | `$0.19` | `189.3s` | `$0.64` | `$0.49 (23,674)` | `50.3s` | `0` | `-$0.1875` | `H4:Dust+H2` |
| `#125` | `14` | `btc-updown-5m-1795535400` | `Dn / Up` | `20.00` | `20.00` | `0.19` | `$0.23` | `$0.04` | `180.5s` | `$0.73` | `$0.77 (13,344)` | `117.7s` | `0` | `-$0.0437` | `H4:Dust+H2` |
| `#146` | `15` | `btc-updown-5m-1795542300` | `Up / Dn` | `6.32` | `6.32` | `3.68` | `$0.21` | `$0.77` | `239.5s` | `$0.65` | `$0.56 (14,303)` | `62.8s` | `0` | `-$0.7728` | `H4:Dust+H2` |
| `#152` | `16` | `btc-updown-5m-1795549800` | `Up / Dn` | `11.66` | `11.66` | `0.69` | `$0.63` | `$0.43` | `197.7s` | `$0.43` | `$0.55 (12,568)` | `40.3s` | `0` | `-$0.4347` | `H4:Dust+H2` |
| `#155` | `16` | `btc-updown-5m-1795550700` | `Up / Up` | `1.00` | `4.00` | `4.00` | `$0.26` | `$1.04` | `225.9s` | `$0.64` | `$0.30 (1,620)` | `0.1s` | `9,188` | `+$2.9600` | `H4:Dust` |
| `#160` | `16` | `btc-updown-5m-1795552200` | `Up / Dn` | `20.00` | `20.00` | `2.60` | `$0.33` | `$0.86` | `198.6s` | `$0.29` | `$0.34 (6,313)` | `49.6s` | `0` | `-$0.8580` | `H4:Dust+H2` |
| `#168` | `17` | `btc-updown-5m-1795556700` | `Dn / Up` | `3.43` | `8.43` | `1.57` | `$0.50` | `$0.79` | `272.8s` | `$0.56` | `$0.70 (1,428)` | `93.2s` | `0` | `-$0.7850` | `H4:Dust+H2` |
| `#176` | `17` | `btc-updown-5m-1795559100` | `Up / Up` | `20.00` | `20.00` | `4.29` | `$0.39` | `$1.67` | `183.7s` | `$0.74` | `$0.60 (15,364)` | `0.1s` | `29,306` | `+$2.6169` | `H4:Dust` |
| `#204` | `19` | `btc-updown-5m-1795605300` | `Dn / Up` | `9.76` | `9.76` | `2.24` | `$0.30` | `$0.67` | `272.0s` | `$0.41` | `$0.57 (3,188)` | `89.6s` | `0` | `-$0.6720` | `H4:Dust+H2` |
| `#211` | `20` | `btc-updown-5m-1795611000` | `Up / Dn` | `20.00` | `20.00` | `1.69` | `$0.37` | `$0.63` | `266.7s` | `$0.65` | `$0.73 (12,153)` | `8.0s` | `17,888` | `-$0.6253` | `H4:Dust` |
| `#217` | `20` | `btc-updown-5m-1795612800` | `Dn / Up` | `2.77` | `2.77` | `2.77` | `$0.29` | `$0.80` | `204.9s` | `$0.65` | `$0.70 (12,796)` | `69.4s` | `0` | `-$0.8033` | `H4:Dust+H2` |
| `#227` | `21` | `btc-updown-5m-1795654500` | `Dn / Up` | `5.00` | `9.45` | **`9.45`** | `$0.32` | `$3.02` | `264.0s` | `$0.98` | `$0.67 (5,141)` | **`54.8s`** | **`0`** | **`-$3.0240`** | **`H2+H5 (Elig)`** |
| `#234` | `21` | `btc-updown-5m-1795656600` | `Up / Dn` | `8.00` | `9.00` | `3.36` | `$0.63` | `$2.12` | `185.0s` | `$0.43` | `$0.45 (2,533)` | `56.8s` | `0` | `-$2.1168` | `H4:Dust+H2` |
| `#237` | `21` | `btc-updown-5m-1795657500` | `Dn / Up` | `1.00` | `21.00` | `3.00` | `$0.64` | `$1.92` | `201.0s` | `$0.42` | `$0.36 (3,557)` | `61.6s` | `0` | `-$1.9200` | `H4:Dust+H2` |
| `#238` | `21` | `btc-updown-5m-1795657800` | `Dn / Up` | `20.00` | `20.00` | **`20.00`** | `$0.45` | `$9.00` | `187.0s` | `$0.61` | `$0.55 (7,683)` | **`63.3s`** | **`0`** | **`-$9.0000`** | **`H2+H5 (Elig)`** |

---

## C. Window #238 Forensic Timeline (`btc-updown-5m-1795657800`)

`btc-updown-5m-1795657800` ran from `1795657800000` ms to `1795658100000` ms (`Cap #21`, `26-11_00-50-00Z_1h14m59s_15w`).

### Chronological Tick-by-Tick Reconstruction

| Timestamp / `tau` | Event / Strategy State | Up Book Ask (Total Depth) | Down Book Bid (Total Depth) | Causal 20-sh Exit Net PnL (Fee) |
| :--- | :--- | :---: | :---: | :---: |
| `1795657912986` (`tau=187.01s`) | **Fill #191:** Passive `BUY 20.00 Down @ $0.45` (`$9.00` cost). `Up` bid resting at `$0.51` | `$0.55 x 47` (`7,683`) | `$0.45 x 47` (`7,662`) | `-$0.2475` (`$0.2475`) |
| `tau=185.00s` | Passive `Up` bid still lagging as spot climbs | `$0.55 x 196` (`26,205`) | `$0.45 x 196` (`26,160`) | `-$0.2475` (`$0.2475`) |
| `tau=182.10s` | Strategy replaces `Up` bid to **`$0.61`** — hits hard cap (`$1.06 - $0.45 = $0.61`) | — | — | — |
| `tau=180.80s..180.00s` | `Up` ask drops briefly to `$0.60`. Post-only `CrossesBook` cancels `$0.61` bid -> re-quotes `$0.60`, cancels again -> `$0.59` | `$0.60 x 1,706` (`19,169`) | `$0.40 x 1,734` (`19,197`) | `-$1.2400` (`$0.2400`) |
| `tau=179.20s` | Binance jump triggers `DefensiveCancel`, pulling the passive `Up` hedge bid | — | — | — |
| `tau=174.86s` | `Up` ask jumps to `$0.67`. Strategy is now **permanently capped below the ask** (`$0.61 < $0.67`) | `$0.67 x 56` (`3,468`) | `$0.33 x 54` (`3,466`) | `-$2.6211` (`$0.2211`) |
| `tau=165.00s` | Brief spike to `$0.77`, then pullback | `$0.77 x 74` (`10,140`) | `$0.23 x 74` (`10,140`) | `-$4.5771` (`$0.1771`) |
| `tau=159.98s` | **Last moderate pullback:** `Up` ask dips to `$0.64`, `3c` above the `$0.61` passive cap | `$0.64 x 365` (`20,945`) | `$0.36 x 365` (`20,945`) | `-$2.0304` (`$0.2304`) |
| `tau=149.99s` | `Up` breaks out to `$0.79` | `$0.79 x 783` (`18,665`) | `$0.21 x 783` (`18,665`) | `-$4.9659` (`$0.1659`) |
| `tau=120.00s` | `Up` at `$0.82`; passive bid remains pinned at `$0.61` | `$0.82 x 104` (`7,227`) | `$0.18 x 104` (`7,232`) | `-$5.5476` (`$0.1476`) |
| `tau=89.95s` | `Up` hits `$0.97`; `DefensiveCancel` fires periodically; passive bid still capped at `$0.61` | `$0.97 x 751` (`8,308`) | `$0.03 x 751` (`8,308`) | `-$8.4291` (`$0.0291`) |
| `tau=63.30s` | Strategy executes `RiskSuppressed` cancel on `Up` (`$0.61`). Last executable book tick | `$0.99 x 1,797` (`1,797`) | `$0.01 x 1,797` (`1,797`) | `-$8.8099` (`$0.0099`) |
| `tau=63.28s..0.00s` | **Book empty on exit sides.** `PreExpiryLockout` (`60s`), `MandatoryFlatten` (`30s`), `TerminalLockout` (`15s`) see `0` `Up` asks and `0` `Down` bids | `None` (`0`) | `None` (`0`) | **UNFILLED** |
| `tau=0.00s` | Market settles **Up**. `20.00 Down @ $0.45` expires worthless | — | — | **`-$9.0000`** |

### Answers to the 12 Forensic Questions (§4)

1. **Exact exposure timestamp:** `1795657912986` ms (`tau = 187.01s`).
2. **Up order book at entry:** Best ask `$0.55 x 47` (`7,683` total Up ask depth); best bid `$0.51`.
3. **Executable Up liquidity at entry:** Yes — `47` shares at `$0.55` (`$0.45 + $0.55 = $1.00`, break-even before `$0.2475` taker fee).
4. **When liquidity disappeared:** At `tau = 63.284s` (Up ask last at `$0.99`), **`33.28s` before `MandatoryFlatten` began**.
5. **Passive hedge attempt before `MandatoryFlatten`:** Yes — continuously quoted `Up` from `tau = 187.0s` to `tau = 63.3s` (`SubmitNew` / `CancelAndReplace`), interrupted by `2` `CrossesBook` cancels (`180.8s`, `180.1s`) and `6` `DefensiveCancel` pulls (`179.2s..71.3s`).
6. **Limit price:** Pinned at **`$0.61`** (`max_combined_bid $0.98 + max_cross_tick_loss $0.08 - $0.45 = $0.61`).
7. **Distance from executable ask:** `-$0.01` to `+$0.01` around `180.0s` (when `CrossesBook` prevented crossing `$0.60`), widening to `3c` at `159.98s` (`$0.64`), `18c` at `150s` (`$0.79`), `21c` at `120s` (`$0.82`), and `36c` at `90s` (`$0.97`).
8. **Visible depth sufficiency:** Yes — `1,797–26,205` Up ask shares (and matching Down bid depth) existed until `tau = 63.284s`.
9. **Best available price and quantity at `MandatoryFlatten` (`tau = 30s`):** `Up ask = None (0 shares)` and `Down bid = None (0 shares)`.
10. **Would a more aggressive FAK order at `tau = 30s` have filled?** **No.** Neither side had a single share available to cross between `tau = 30s` and `tau = 15s`.
11. **Execution cost at earlier points:** `-$0.2475` net (`$0.2475` taker fee) at `187s`, `-$2.0304` net (`$0.2304` fee) at `160s`, `-$4.9659` (`$0.1659` fee) at `150s`, `-$5.5476` (`$0.1476` fee) at `120s`, `-$8.4291` (`$0.0291` fee) at `90s`, `-$8.8099` (`$0.0099` fee) at `63.3s`.
12. **Earliest causal intervention point:** Two causal points existed without lookahead:
    - **Passive cross at `tau = 180.80s` (`Up ask = $0.60`):** Allowing a passive reducing-leg quote to cross `$0.60` (instead of canceling for `CrossesBook`) would have paid `$0.45 + $0.60 = $1.05` (`-$1.00` + `$0.24` taker fee = `-$1.24` total vs `-$9.00`).
    - **First severe drift (`tau = 174.86s` at `$0.67` or `tau = 150s` at `$0.79`):** Crossing at `tau = 149.99s` (`comb = $1.24`) saves `$4.03` vs `-$9.00`, **but** as Section E proves, triggering taker cross-outs this early on all windows destroys far more Maker Merge Gain on mean-reverting windows than it saves.

---

## D. Root-Cause Classification

Every non-zero settlement window falls into one or more of the following root causes:

| Root-Cause Category | Windows Affected | Settlement PnL Attribution | Mechanism |
| :--- | :---: | :---: | :--- |
| **Minimum-size constraint (`H4: |q| < 5.0`)** | **`36 / 40`** | **`-$12.7415` (`38.0%`)** | Polymarket `min_order_size = 5.0` shares blocks any CLOB flatten order (`ResidualDustUnflattenable`). |
| **No feasible liquidity at `tau <= 30s` (`H2`)** | **`4 / 4` eligible** (plus `19` dust) | **`-$20.8192` (`62.0%`)** | Exit books went empty (`$0.99 / $0.01`, `0` depth) `24.8s–60.8s` before `MandatoryFlatten`. |
| **Active-phase cross-tick cap stranding (`H5`)** | **`3 / 4` eligible** (`#238`, `#15`, `#86`) | **`-$17.7952` (`53.0%`)** | `allow_cross_tick_override = false` in `Tier 1 ActiveQuoting` locked passive hedges at `$1.06 - vwap` until `tau = 60s` (`0` liquidity left). |
| **Underdog Leg-1 entry (`<$0.35`)** | **`3 / 4` eligible** (`#15`, `#86`, `#227`) + `10` dust | **`-$12.6936` (`37.8%`)** | Cheap underdogs (`$0.20–$0.32`) face steeper adverse-move drift when spot trends away. |
| **Limit price too conservative (`H1`)** | **`0 / 4` eligible** | **`$0.0000`** | Never binding at `tau <= 30s` because books were already empty (`None`). |
| **Order never dispatched (`H3`/`H5`/`H6`)** | **`40 / 40`** | **`-$33.5607` (`100%`)** | Correctly suppressed for `36` dust windows (`H4`) and `4` empty-book windows (`H2`); no dispatch or matching bug. |
| **Position created too late (`H7`)** | **`0 / 40`** | **`$0.0000`** | `entry_cutoff_secs = 120s` prevented all entries after `tau = 180s` (earliest `first_unp_tau = 180.5s`). |

---

## E. Counterfactual Results (Causal L2 Depth + Exact Taker Fees)

All policies were simulated causally across **all `241` windows** (`67` windows ever held `>= 5.0` unpaired shares), walking visible L2 depth, charging exact `compute_taker_fee_micro` (`feeRate = 0.05`), and canceling any post-intervention fills/merges in the same window so lost Maker Merge Gain is measured honestly.

| Policy | Rule | Trig / Filled | Shares Flat | Taker Fees | New Merge Gain ($\Delta$) | New Settle PnL ($\Delta$) | New Net PnL ($\Delta$) | Worst Window |
| :--- | :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **Baseline (A)** | `tau <= 30s`, best-1 limit | `5 / 0` | `0.0` | `$0.000` | `$+65.2415` (`$0.000`) | `-$33.5607` (`$0.000`) | **`+$33.2864`** (`$0.000`) | `-$9.0000` |
| **B: Early 60s** | Sweep L2 at `tau <= 60s` | `8 / 4` | `51.8` | `$0.397` | `$+61.3681` (`-$3.873`) | `-$33.4596` (`+$0.101`) | `+$29.1173` (`-$4.169`) | `-$9.0000` |
| **B: Early 90s** | Sweep L2 at `tau <= 90s` | `10 / 9` | `139.2` | `$0.925` | `$+42.2980` (`-$22.944`) | `-$19.5909` (`+$13.970`) | `+$21.7819` (`-$11.505`) | `-$8.4291` |
| **B: Early 120s** | Sweep L2 at `tau <= 120s` | `14 / 14` | `213.2` | `$1.790` | `$+44.5383` (`-$20.703`) | `-$14.8738` (`+$18.687`) | `+$27.8744` (`-$5.412`) | `-$5.5476` |
| **C: Graduated 30s** | `+2c / +5c / sweep` in `30s..15s` | `5 / 0` | `0.0` | `$0.000` | `$+65.2415` (`$0.000`) | `-$33.5607` (`$0.000`) | `+$33.2864` (`$0.000`) | `-$9.0000` |
| **C: Graduated 60s** | `+2c / +5c / sweep` in `60s..15s` | `8 / 4` | `51.8` | `$0.397` | `$+61.3681` (`-$3.873`) | `-$33.4596` (`+$0.101`) | `+$29.1173` (`-$4.169`) | `-$9.0000` |
| **D: Partial 30s** | Allow partial fills in `30s..15s` | `5 / 0` | `0.0` | `$0.000` | `$+65.2415` (`$0.000`) | `-$33.5607` (`$0.000`) | `+$33.2864` (`$0.000`) | `-$9.0000` |
| **E: Stop-Cross `$1.10`** | Flatten if `vwap + opp_ask >= $1.10` | `21 / 21` | `316.5` | `$3.201` | `$+23.3095` (`-$41.932`) | `-$12.6486` (`+$20.912`) | `+$7.4594` (`-$25.827`) | `-$2.3972` |
| **E: Stop-Cross `$1.15`** | Flatten if `vwap + opp_ask >= $1.15` | `17 / 17` | `259.2` | `$2.261` | `$+17.2285` (`-$48.013`) | `-$12.6486` (`+$20.912`) | `-$0.2810` (`-$33.567`) | `-$3.6016` |
| **E: Stop-Cross `$1.20`** | Flatten if `vwap + opp_ask >= $1.20` | `13 / 13` | `198.9` | `$1.710` | `$+25.3811` (`-$39.860`) | `-$18.9473` (`+$14.613`) | `+$2.1237` (`-$31.163`) | `-$4.4074` |
| **E: Stop-Cross `$1.25`** | Flatten if `vwap + opp_ask >= $1.25` | `10 / 10` | `153.9` | `$1.344` | `$+27.8942` (`-$37.347`) | `-$23.4365` (`+$10.124`) | `+$3.1134` (`-$30.173`) | `-$5.2400` |
| **E: Stop-Cross `$1.30`** | Flatten if `vwap + opp_ask >= $1.30` | `6 / 6` | `76.2` | `$0.453` | `$+51.9871` (`-$13.254`) | `-$23.7236` (`+$9.837`) | `+$27.8105` (`-$5.476`) | `-$6.1275` |
| **E: `$1.20` @ `tau<=90s`** | Stop-cross `$1.20` only at `tau<=90s` | `6 / 6` | `91.2` | `$0.311` | `$+45.7040` (`-$19.538`) | `-$19.3584` (`+$14.202`) | `+$26.0348` (`-$7.252`) | `-$8.4291` |
| **E: `$1.30` @ `tau<=90s`** | Stop-cross `$1.30` only at `tau<=90s` | `5 / 5` | `71.2` | `$0.245` | `$+49.0117` (`-$16.230`) | `-$24.1536` (`+$9.407`) | `+$24.6133` (`-$8.673`) | `-$8.4291` |
| **E: `$1.20` @ `tau<=60s`** | Stop-cross `$1.20` only at `tau<=60s` | `2 / 2` | `23.5` | `$0.055` | `$+62.6919` (`-$2.550`) | `-$30.5367` (`+$3.024`) | **`+$33.7055`** (`+$0.419`) | `-$9.0000` |
| **Pre-Entry Floor `<$0.25`** | Skip windows with Leg 1 `<$0.25` | `11` dropped | — | `$0.000` | `$+54.4548` (`-$10.787`) | `-$24.7173` (`+$8.843`) | `+$31.3431` (`-$1.943`) | `-$9.0000` |
| **Pre-Entry Floor `<$0.30`** | Skip windows with Leg 1 `<$0.30` | `19` dropped | — | `$0.000` | `$+46.5640` (`-$18.678`) | `-$24.4431` (`+$9.118`) | `+$23.7265` (`-$9.560`) | `-$9.0000` |
| **Pre-Entry Floor `<$0.35`** | Skip windows with Leg 1 `<$0.35` | `25` dropped | — | `$0.000` | `$+39.5559` (`-$25.686`) | `-$20.8671` (`+$12.694`) | `+$20.2944` (`-$12.992`) | `-$9.0000` |

**Policy D is not an independent test.** `D_partial_30s` uses the same limits, timing, and fill logic as `A_baseline_30s`, so its row duplicates the baseline. It does not isolate the effect of allowing partial fills.

**Stop-cross trigger fallback.** When no opposite ask exists, the simulation also fires if `vwap + (1 - heavy_bid) >= threshold`. The rule above states only the ask-based form.

---

## F. Economic Impact & Reassessment of the 85% Loss Rate

### 1. Why Almost Every Taker-Flatten Policy Hurts Total Net PnL

The counterfactual table reveals a critical structural fact: **BTC 5-minute binaries mean-revert heavily between `tau = 250s` and `tau = 60s`.**
- Unconstrained stop-crosses (`$1.10–$1.30`) and early mandatory flattens (`tau <= 90s` or `120s`) repeatedly trigger on windows that temporarily drifted against Leg 1 (`win#29`, `win#33`, `win#66`, `win#81`, `win#176`) and then **recovered to merge passively for `+$1.48` to `+$5.23` profit**.
- Under `E_stopcross_115`, cutting `17` windows saves `+$20.91` of settlement losses, **but destroys `-$48.01` of Maker Merge Gain and adds `$2.26` in taker fees**, turning `+$33.29` Net PnL into **`-$0.28`**.
- Only **`E_stop_120_tau60`** (`vwap + opp_ask >= $1.20` inside `tau <= 60s`) improves Net PnL (`+$33.71`, `+$0.42`), because restricting taker exits to `tau <= 60s` avoids false-positive stops on mid-window mean reversion (`win#29`, `win#66`, `win#81`). However, `tau <= 60s` still misses `win#238`, `win#15`, and `win#86` whose books collapsed at `63.3s–90.8s`.
- Similarly, blocking underdog entries (`<$0.25`, `<$0.30`, `<$0.35`) is net-negative (`-$1.94` to `-$12.99`) because underdog fills (`<$0.35`) generated **`+$25.6856` of Maker Merge Gain** vs `-$12.6936` of settlement loss (`+$12.9920` net positive).

### 2. Reassessment of the 85% Settlement Loss Rate (§8)

| Dimension | Segment | `n` (`W / L`) | Loss Rate | Settlement PnL | Merge Gain (Same Windows) | Net PnL |
| :--- | :--- | :---: | :---: | :---: | :---: | :---: |
| **Severity** | Wins (`6`) / Losses (`34`) | `6` / `34` | `85.0%` | Win mean `+$1.2859` (`max +$2.96`) | Loss mean `-$1.2140` (`min -$9.00`) | — |
| **Dust vs Eligible** | Dust (`<5 sh`) | `36` (`6 / 30`) | `83.3%` | `-$12.7415` | `+$35.8919` | `+$23.1504` |
| **Dust vs Eligible** | Eligible (`>=5 sh`) | `4` (`0 / 4`) | **`100.0%`** | **`-$20.8192`** | `+$0.1687` | **`-$20.6505`** |
| **Direction** | `Down` residual | `17` (`0 / 17`) | **`100.0%`** | `-$20.5879` | `+$5.1475` | `-$15.4404` |
| **Direction** | `Up` residual | `23` (`6 / 17`) | `73.9%` | `-$12.9728` | `+$30.9131` | `+$17.9403` |
| **Entry VWAP** | `<$0.35` | `21` (`2 / 19`) | `90.5%` | `-$16.0869` | `+$18.2737` | `+$2.1868` |
| **Entry VWAP** | `$0.35–$0.50` | `11` (`1 / 10`) | `90.9%` | `-$12.3108` | `+$6.2622` | `-$6.0486` |
| **Entry VWAP** | `>$0.50` | `8` (`3 / 5`) | `62.5%` | `-$5.1630` | `+$11.5247` | `+$6.3617` |
| **Max Unpaired** | `<5 sh` | `16` (`2 / 14`) | `87.5%` | `-$6.5410` | `+$4.2315` | `-$2.3095` |
| **Max Unpaired** | `5–19.99 sh` | `10` (`0 / 10`) | **`100.0%`** | `-$8.5687` | `+$8.3739` | `-$0.1948` |
| **Max Unpaired** | `>=20 sh` | `14` (`4 / 10`) | `71.4%` | `-$18.4510` | `+$23.4552` | `+$5.0042` |

- **Diagnosis of the 85% loss rate (in-sample only):** The `83` fill-active windows are net positive in every Leg-1 price bucket (`+$9.56`, `+$3.43`, `+$3.40`, `+$16.89`). Merged positions exit the settlement sample, so the remaining `40` trajectories carry a selection effect. Window `#238` pulled back before settling, and six residual positions settled at a profit. Positive in-sample PnL does not rule out negative expected value. Leave that conclusion open for holdout validation.
- **Concentration:** Just **`4` eligible windows (`1.7%` of all `241` windows)** account for **`62.0%` (`-$20.8192`)** of all settlement losses, while **`36` dust windows** contribute the remaining `-$12.7415` despite already generating `+$35.8919` in Maker Merge Gain (`+$23.1504` net positive).

---

## G. Ranked Recommendations

The cheapest and earliest intervention must **unlock passive maker hedging** before books collapse (`tau = 90s..60s`) without paying premature taker fees on mean-reverting mid-window swings, and eliminate sub-minimum odd-lot dust creation where possible.

| Rank | Intervention | Loss Reduction | Merge Edge Preservation | Feasibility | Complexity | Robustness |
| :---: | :--- | :--- | :--- | :--- | :--- | :--- |
| **1** | **Dynamic Passive Cross-Tick Override + Post-Only Exception on Reducing Leg (`Tier1` when `|q| >= 5sh` and `tau <= 90s` or `deficit_hedge_trigger`)** | Targets the `4` eligible stranded windows (`-$20.82`) before books empty at `63–91s` | **High:** Zero premature taker exit before `tau = 90s`. Resting bids fill passively (`0%` maker fee) when pullbacks touch `$0.62–$0.85` instead of locking at `$1.06 - vwap`. An order allowed to cross the tick takes liquidity and pays a taker fee, as the `$0.61` buy against a `$0.60` ask shows at line 150. Fill and fee outcomes for both behaviours remain unverified until the replay in Section H tests them | High (books had `1,797–20,945` shares at `63–160s` in `win#238`) | Low (already wired via `tier1_hedge_trigger_price` & `allows_cross_tick_override` in `station7_risk.rs` / `lifecycle_cmd.rs`) | High across regimes (replaces rigid `$1.06` freeze with gradual passive chase) |
| **2** | **Time-Gated Taker Stop-Cross in `PreExpiryLockout` (`E_stop_120_tau60`, `tau <= 60s`, `vwap + opp_ask >= $1.20`)** | Saved `+$3.02` in `win#227`; net `+$0.42` overall (`+$33.71`) | **High:** Never fires above `tau = 60s`, avoiding mid-window mean-reversion false positives | Proven feasible in `2/2` triggering windows (`$0.055` fees) | Low (one phase check in `lifecycle.rs`) | Moderate (only catches positions whose book survives past `tau = 60s`) |
| **3** | **Avoid Sub-`5`-Share Odd-Lot Residue Upstream (`min_order_size`-aware sizing / rounding)** | Targets the `-$12.74` across `36` dust windows (`-$2.12` worst dust loss) | **High:** Only adjusts residual odd lots (`<5` sh) that cannot be flattened on CLOB | High (no exchange `min_order_size` violation) | Medium (requires broker fill/quote size rounding review) | High |
| **4** | **Do NOT deploy early taker flatten (`tau <= 90s/120s`), unconstrained stop-cross (`$1.10–$1.30`), or `<$0.35` entry floors** | Reduces settlement loss `+$9–$21`, **but** destroys `-$11–$48` of Maker Merge Gain | **Severe damage:** Cuts total strategy Net PnL by `-$1.94` to `-$33.57` | — | — | Poor (over-reacts to normal BTC 5m volatility) |

---

## H. Remaining Uncertainties

1. **Passive hedge relaxation counterfactual is unsimulated at the tick level:** Because relaxing `max_cross_tick_loss` on the reducing leg (`Rank 1`) alters resting maker order queue positions across subsequent ticks, an exact L2 replay with `PolySimulator` (on the frozen `21`-capture IS set, without touching Capture `#22`) is required to measure its exact net gain and verify it does not trigger adverse cross-fills.
2. **Zero live taker-flatten empirical friction:** Because `0` flattens fired in the `21`-capture baseline, actual Polymarket `40ms` wire latency and L2 book slippage under `MandatoryFlatten` remain untested in live or replay data.
3. **Dust (`< 5` shares) creation mechanics:** `36` windows ended with sub-minimum dust (`0.01–4.29` shares) largely from partial passive fills on the reducing or accumulating leg; whether `cap_tier1_reducing_to_delta` rounding or `max_combined_bid` adjustment can safely absorb `0.01–4.99` share remnants without creating opposite-side overshoot requires targeted simulation.
4. **Script `[cause]` column is a summary label, not a per-window verdict:** `analyze_mandatory_flatten_root_cause.py` stamps every non-dust window `H2:EmptyBook@30s+H5:CrossTickCap` from the residual quantity alone. The attribution in Section D rests on the computed `Last Opp Ask` `tau` and `MandatoryFlatten` `opp_ask` columns. Treat the label as a summary, not as independent evidence.
4. **Regime generalization beyond 21 IS captures:** `win#238` (`-$9.00`) occurred in `Cap #21` (`26-11`), and `17 / 17` `Down` residuals lost in this `5`-day window. Out-of-sample validation on the sealed Holdout Vault (starting with Capture `#22`) is still required after any Rank 1–3 code change.
