# Dynamic Passive Reducing-Leg Hedge Override — Exact L2 Replay Evaluation

**Verdict: NO-GO (Do Not Promote).** The hypothesis is **falsified** by exact causal L2 replay across the 21 Development/IS captures (`241` windows).

Relaxing the `$1.06 - VWAP` cross-tick cap on the reducing leg successfully rescued all four large settlement-loss windows (`#15`, `#86`, `#227`, `#238`) with 100% Post-Only Maker fills, reducing `>= 5` share settlement windows from `4` to `0` and saving **`+$9.5200`** across those four windows. However, the same rule triggered early on mean-reverting windows (`#29`, `#33`, `#66`, `#81`, `#176`, `#23`, `#211`), locking in **`-$35.8843`** of `> $1.06` negative merges where the baseline waited for mean reversion and earned positive merges.

Across the full 21-capture corpus:

- **Primary Candidate (`exp_vwap_108`):** Reported Net PnL fell from **`+$33.2864`** to **`+$8.2905`** (**`-$24.9959`**, `-75.1%`). Only `35.4%` of baseline Maker Merge Gain survived (`+$23.1019` vs `+$65.2415`).
- **Timing Sensitivity (`exp_vwap_108_t120`, `tau <= 120s`):** Reported Net PnL still fell to **`+$25.4308`** (**`-$7.8556`**, `-23.6%`), preserving `76.8%` of Maker Merge Gain (`+$50.0886`).
- **Pre-existing Wired Latch (`t1trig_065`, `--tier1-hedge-trigger-price 0.65`):** Reported Net PnL fell to **`+$16.3133`** (**`-$16.9731`**, `-51.0%`), preserving `48.9%` of Maker Merge Gain (`+$31.9272`).

Production defaults (`DEFAULTS.json`, `.env.example`, `crates/core-types/src/config.rs`, `scripts/sweep-params.ps1`) remain untouched. Capture `#22` and `.inprogress.jsonl` were never opened or replayed.

---

## 1. Exact Baseline Reproduction

Before testing any counterfactual, we replayed Config `#6` (`--tier1-hedge-trigger-price off`, `--queue-decay conservative`, `--v-max 50`, `--base-size 20`, `--latency-ms 40`) through the release binary (`poly-as lifecycle --source replay`) across all 21 Development/IS captures listed in `sweep-manifest.json`. Each run executed in an isolated temporary working directory with a clean environment so local `.env` files could not alter Config `#6` defaults.

| Parity Check | Replayed Value | Expected Baseline | Delta | Status |
| --- | ---: | ---: | ---: | :---: |
| Maker Pair-Merge Gain | `+$65.2415` (`65,241,500`) | `+$65.2415` (`65,241,500`) | `0` | **PASS** |
| Terminal Settlement PnL | `-$33.5607` (`-33,560,700`) | `-$33.5607` (`-33,560,700`) | `0` | **PASS** |
| Incomplete-Window MTM (`#33`) | `+$1.6056` (`1,605,600`) | `+$1.6056` (`1,605,600`) | `0` | **PASS** |
| Reported Net PnL | `+$33.2864` (`33,286,400`) | `+$33.2864` (`33,286,400`) | `0` | **PASS** |
| Total Fills | `351` | `351` | `0` | **PASS** |
| MandatoryFlatten Executions | `0` | `0` | `0` | **PASS** |

Every metric matched to exact single micro-USDC parity (`0` discrepancy across `241` windows and `1,432,708` simulated order decisions).

---

## 2. Implementation and Execution-Semantics Audit

### Code Paths Inspected and Wired

- **Pricer & Post-Only Clamping:** [station4_6_pricer.rs](../crates/math-engine/src/station4_6_pricer.rs#L431-L468) and [station4_6_pricer.rs](../crates/math-engine/src/station4_6_pricer.rs#L655-L685)
- **Risk & Experimental Gate:** [station7_risk.rs](../crates/risk-lifecycle/src/station7_risk.rs#L126-L245) and [station7_risk.rs](../crates/risk-lifecycle/src/station7_risk.rs#L624-L720)
- **Replay Wiring & Hard Offline Guard:** `crates/cli/src/lifecycle_cmd.rs` and `crates/cli/src/main.rs` (wiring only, archived)
- **FIFO Book & Maker/Taker Enforcement:** [fifo_book.rs](../crates/paper-sim/src/fifo_book.rs#L225-L247)
- **Integration Tests:** `crates/cli/tests/passive_hedge_override.rs` (archived)
- **Reproducible Offline Runner:** `scripts/run_passive_override_replay.py` (archived)

The falsified replay-only CLI harness (the two CLI flags, the `station7_risk.rs` evaluator, `crates/cli/tests/passive_hedge_override.rs`, and `scripts/run_passive_override_replay.py`) remains archived on the unmerged `validate_strategy_edge_metric` branch and is not part of this change.

### Exact Execution Semantics Verified

The experimental override **only** sets `CostContext::allow_cross_tick_override = true` for the current tick inside `LivePaperSimulator::step_tick`. It does **not** send marketable orders, does **not** disable Post-Only clamping, and does **not** relax accumulating-leg limits:

1. **Reducing-leg-only bypass:** In `station4_6_pricer.rs`, `allow_cross_tick_override = true` bypasses `apply_cross_tick_cap` (`$1.06 - VWAP`) strictly on the inventory-reducing leg. The accumulating leg remains constrained by `apply_entry_prob_guard` (`max_entry_prob = 0.80`, `max_entry_price = $0.65`) and `max_combined_bid = $0.98`.
2. **Strict Post-Only clamping:** `apply_post_only_leg` clamps every resting bid to `min(raw_bid, tightest_ask - 1 tick)`. No bid is quoted at or above the visible opposing ask.
3. **Hard Post-Only broker rejection:** Inside `FifoPaperBroker::submit_new`, any Buy arriving after the `40ms` wire delay with `price >= current_best_ask` fails with `PaperSimError::PostOnlyCrossed` instead of executing as a taker.
4. **Resting FIFO fills only:** Every fill occurs only after prior queue depth ahead of the resting order depletes from historical sell trade prints (`QueueDecayPolicy::Conservative`).
5. **Zero hidden taker executions:** Across all `84` replays (`1,435` total fills across the four variants), `is_maker = true` on `100%` of fills, `taker_fills = 0`, `fees_paid = $0.0000`, and `post_only_rej = 0` (because `apply_post_only_leg` and `CrossesBook` cancellations keep resting bids strictly below the live ask).

| Execution / Order-Action Class | Baseline | `exp_vwap_108` | `exp_vwap_108_t120` | `t1trig_065` |
| --- | ---: | ---: | ---: | ---: |
| Maker Fills (`$0` fee) | `351` | `362` | `353` | `369` |
| Taker Fills (`> $0` fee) | `0` | `0` | `0` | `0` |
| Partial Fills (`< 20sh`) | `311` | `318` | `312` | `326` |
| Post-Only Broker Rejections | `0` | `0` | `0` | `0` |
| `SubmitNew` Actions | `6,480` | `6,424` | `6,443` | `6,468` |
| `CancelAndReplace` Actions | `25,405` | `25,574` | `25,392` | `25,533` |
| `Cancel: CrossesBook` | `1,413` | `1,405` | `1,400` | `1,431` |
| `Cancel: DefensiveCancel` | `3,215` | `3,151` | `3,194` | `3,168` |
| `Cancel: RiskSuppressed` | `1,720` | `1,728` | `1,715` | `1,728` |
| `Hold` Decisions (No Change) | `1,394,475` | `1,394,426` | `1,394,564` | `1,394,380` |

---

## 3. Candidate Policy Definition

We implemented an isolated, non-latching, tick-level evaluator (`evaluate_dynamic_passive_override` in [station7_risk.rs](../crates/risk-lifecycle/src/station7_risk.rs#L170-L245)) gated by two CLI-only flags that fail validation unless `--source replay` and `cap_tier1_reducing_to_delta = true` are active.

### Trigger Rule (`evaluate_dynamic_passive_override`)

At each tick, `allow_cross_tick_override` activates on the reducing leg if and only if **all six** conditions hold:

1. Experimental flag `--exp-passive-override-combined-micro` is set (default: `None` / disabled).
2. `cap_tier1_reducing_to_delta == true` (prevents reducing-leg overshoot for `|q| >= 5` shares).
3. `tier` is `Tier1Normal` or `Tier2ModerateSkew` (`Tier3OneSided` already enables the override in baseline at `tau <= 60s`).
4. Unpaired exposure `|inventory_q| >= 5.00` shares (`min_order_size_micro`), ignoring sub-minimum dust.
5. Optional time gate: `tau_secs <= max_tau_secs` when `--exp-passive-override-max-tau-secs` is provided.
6. Combined adverse cost: `heavy_leg_vwap + opposing_best_ask >= combined_trigger_micro` (`$1.08`), meaning the `$1.06 - VWAP` cap is actively stranding the reducing bid at least `2` ticks below the executable ask (`>= 1` tick below `best_ask - 1 tick`).

### Evaluated Variants (No Parameter Grid)

| Variant ID | CLI Flags (Added to Config `#6`) | Economic Intent |
| --- | --- | --- |
| `baseline` | `--tier1-hedge-trigger-price off` | Immutable Config `#6` baseline |
| `exp_vwap_108` | `--exp-passive-override-combined-micro 1080000` | Primary candidate: non-latching VWAP-aware trigger at `VWAP + Ask >= $1.08` for `|q| >= 5sh` at any `tau` |
| `exp_vwap_108_t120` | `--exp-passive-override-combined-micro 1080000 --exp-passive-override-max-tau-secs 120` | Narrow timing sensitivity restricting the same rule to `tau <= 120s` |
| `t1trig_065` | `--tier1-hedge-trigger-price 0.65` | Comparison against the pre-existing latched Tier-1 trigger wired in `station7_risk.rs` |

---

## 4. Tick-Level Before/After Analysis: Four Eligible Settlement Windows

All four large settlement windows (`>= 5` residual shares, combined baseline settlement loss `-$20.8192`, net `-$20.6505`) were completely flattened by `exp_vwap_108` as `100%` Post-Only Maker orders with `$0` fees.

### Summary Table (4 Target Windows)

| Win # | Cap # | Baseline Net (Settle) | `exp_vwap_108` Net (Merge) | Delta (`C1 - Base`) | `exp_vwap_108_t120` Net | Delta (`C2 - Base`) | Residual (`Base -> C1 -> C2`) |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| `#15` | `#3` | `-$4.7958` (`-$4.7952`) | `-$2.7974` (`-$2.7974`) | **`+$1.9984`** | `-$4.7958` | `$0.0000` | `19.98 -> 0.00 -> 19.98` |
| `#86` | `#11` | `-$4.0000` (`-$4.0000`) | `-$1.8000` (`-$1.8000`) | **`+$2.2000`** | `-$3.4175` | `+$0.5825` | `20.00 -> 0.00 -> 8.35` |
| `#227` | `#21` | `-$2.8547` (`-$3.0240`) | `-$2.3822` (`-$2.3822`) | **`+$0.4725`** | `-$2.3822` | `+$0.4725` | `9.45 -> 0.00 -> 0.00` |
| `#238` | `#21` | `-$9.0000` (`-$9.0000`) | `-$4.1509` (`-$4.1509`) | **`+$4.8491`** | `-$4.5550` | `+$4.4450` | `20.00 -> 0.00 -> 2.22` |
| **Total** | | **`-$20.6505`** | **`-$11.1305`** | **`+$9.5200`** | **`-$15.1505`** | **`+$5.5000`** | `69.43 -> 0.00 -> 30.55` |

### Chronological Timelines

#### Window `#15` (Cap `#3`, `btc-updown-5m-1795362900`, Winner: `Down`)
- **Entry (all variants):** Bought `20.00` Up @ `$0.24` (`tau = 180.6s`, UpAsk `$0.27 x 5.51`, DnAsk `$0.77 x 1,328.99`). Two tiny Down fills (`0.01` @ `$0.79` at `177.5s` and `172.8s`) left `+19.98` Up (`VWAP = $0.24`, baseline cap `$0.82`).
- **Baseline:** Down bid stayed capped at `$0.81` as DnAsk rose through `$0.85 -> $0.99` and vanished at `tau = 86.1s`. Settled `+19.98` Up @ `$0.00` (`-$4.7952` settle, `-$4.7958` net).
- **`exp_vwap_108` (`+$1.9984`):** Override activated at `tau = 148s` (`Down bid = $0.84 x 19.98`, DnAsk `$0.85 x 46.09`, Post-Only). Filled `0.01` @ `$0.86` (`tau = 148.0s`, DnAsk `$0.87 x 256.30`) and `19.97` @ `$0.90` (`tau = 133.5s`, DnAsk `$0.91 x 1,210.66`, Maker, `$0` fee). Merged `19.97` pairs @ `$1.14` (`-$2.7958`), ending `0.00` residual, `-$2.7974` net.
- **`exp_vwap_108_t120` (`$0.0000`):** Override activated too late at `tau = 120s` when DnAsk was already `$0.95 x 2,053.68` and climbed to `$0.99` by `tau = 89s`. Zero fills occurred; ended identical to baseline (`-$4.7958`).

#### Window `#86` (Cap `#11`, `btc-updown-5m-1795517100`, Winner: `Down`)
- **Entry (all variants):** Bought `20.00` Up @ `$0.20` (`15.00 + 5.00` at `tau = 184.1s`, DnAsk `$0.81 x 250.22`). Baseline cap = `$0.86`.
- **Baseline:** Down bid capped at `$0.86`; zero hedge fills before liquidity vanished at `tau = 90.8s`. Settled `+20.00` Up @ `$0.00` (`-$4.0000`).
- **`exp_vwap_108` (`+$2.2000`):** Override activated at `tau = 175s` (`Down bid = $0.87 x 20.00`, DnAsk `$0.88 x 36.75`, Post-Only). Followed up to `$0.89`, filling full `20.00` Down @ `$0.89` at `tau = 145.8s` (DnAsk `$0.90 x 2,753.17`, Maker, `$0` fee). Merged `20.00` pairs @ `$1.09` (`-$1.8000`), ending `0.00` residual.
- **`exp_vwap_108_t120` (`+$0.5825`):** Activated at `tau = 120s` (DnAsk `$0.99 x 3,332.96`). Caught a brief pullback at `tau = 98.1s`, partially filling `11.65` Down @ `$0.95` (DnAsk `$0.96 x 202.94`, `-$1.7475` merge). Left `8.35` Up unhedged (`-$1.6700` settle, `-$3.4175` net).

#### Window `#227` (Cap `#21`, `btc-updown-5m-1795654500`, Winner: `Up`)
- **Prior activity (all variants):** Five profitable/near-par merges (`9.89` pairs, `+$0.1693` merge gain) ended with `+0.55` Up. At `tau = 79.4s`, a `10.00` Down fill @ `$0.32` flipped inventory to `-9.45` Down (`VWAP = $0.32`, baseline cap `$0.74`).
- **Baseline:** Stayed capped until Tier-3 activated at `tau = 60s` (`Up bid = $0.90`, UpAsk `$0.91 x 120.50`), when UpAsk raced to `$0.99` by `tau = 56s` and disappeared at `tau = 54.8s`. Settled `-9.45` Down @ `$0.00` (`-$3.0240` settle, `-$2.8547` net).
- **`exp_vwap_108` and `exp_vwap_108_t120` (`+$0.4725`):** Activated `18s` earlier at `tau = 78s` (`Up bid = $0.83 x 9.45`, UpAsk `$0.84 x 68.94`, Post-Only). Filled full `9.45` Up @ `$0.95` at `tau = 68.2s` (UpAsk `$0.96 x 694.35`, Maker, `$0` fee). Merged `9.45` pairs @ `$1.27` (`-$2.5515`), ending `0.00` residual, `-$2.3822` net.

#### Window `#238` (Cap `#21`, `btc-updown-5m-1795657800`, Winner: `Up`)
- **Entry (all variants):** Bought `20.00` Down @ `$0.45` at `tau = 187.0s` (UpAsk `$0.55 x 42.00`). Baseline cap = `$0.61`.
- **Baseline:** Up bid stayed frozen at `$0.61` while Up climbed to `$0.82` and vanished at `tau = 63.3s`. Settled `-20.00` Down @ `$0.00` (`-$9.0000` worst window in corpus).
- **`exp_vwap_108` (`+$4.8491`):** Override activated at `tau = 176s` (`Up bid = $0.62 x 20.00`, Post-Only). Filled `8.07` Up @ `$0.68` (`tau = 158.8s`, `pc = $1.13`, `-$1.0491`), `5.00` Up @ `$0.81` (`tau = 140.0s`, UpAsk `$0.82 x 472.35`, `pc = $1.26`, `-$1.3000`), and `6.93` Up @ `$0.81` (`tau = 132.5s`, UpAsk `$0.82 x 202.73`, `pc = $1.26`, `-$1.8018`). All three fills were Maker (`$0` fee), leaving `0.00` residual and `-$4.1509` net.
- **`exp_vwap_108_t120` (`+$4.4450`):** Activated at `tau = 115s` on a pullback (`Up bid = $0.66`, UpAsk `$0.67 x 2.54`). Filled `17.78` Up @ `$0.75` at `tau = 111.2s` (UpAsk `$0.76 x 32.00`, `pc = $1.20`, `-$3.5560`), leaving `2.22` Down dust (`-$0.9990` settle, `-$4.5550` net).

---

## 5. Full-Corpus Economic Comparison (21 Captures / 241 Windows)

| Metric | Baseline (Config `#6`) | `exp_vwap_108` (Primary) | `exp_vwap_108_t120` (`<=120s`) | `t1trig_065` (Wired Latch) |
| --- | ---: | ---: | ---: | ---: |
| **Total Maker Merge Gain** | **`+$65.2415`** | `+$23.1019` (`-$42.1396`) | `+$50.0886` (`-$15.1529`) | `+$31.9272` (`-$33.3143`) |
| Merge Gain Retained (% of Base) | `100.0%` | **`35.4%`** | `76.8%` | `48.9%` |
| Positive Merge Gain | `+$85.8059` | `+$68.2728` | `+$80.8779` | `+$74.0760` |
| Negative Merge Loss | `-$20.5644` | `-$45.1709` (`+119.7%`) | `-$30.7893` (`+49.7%`) | `-$42.1488` (`+105.0%`) |
| Total Pairs Merged | `1,090.43` | `1,182.29` | `1,144.27` | `1,196.94` |
| Merge Events | `255` | `265` | `257` | `272` |
| Total Fills (Maker / Taker) | `351` (`351` / `0`) | `362` (`362` / `0`) | `353` (`353` / `0`) | `369` (`369` / `0`) |
| Taker Fees Paid | `$0.0000` | `$0.0000` | `$0.0000` | `$0.0000` |
| **Terminal Settlement PnL** | `-$33.5607` | `-$14.9350` (`+$18.6257`) | `-$24.6578` (`+$8.9029`) | `-$15.7375` (`+$17.8232`) |
| Residual Unpaired Shares (Settled) | `136.76` | `59.24` | `95.30` | `61.22` |
| Windows with `>= 5` Residual Shares | `4` | **`0`** | `2` | **`0`** |
| Windows with Dust (`0 < res < 5`) | `36` | `33` | `35` | `33` |
| MandatoryFlatten Attempts / Execs | `0` / `0` | `0` / `0` | `0` / `0` | `0` / `0` |
| **Total Realized Net PnL** | **`+$31.6808`** | `+$8.1669` (`-$23.5139`) | `+$25.4308` (`-$6.2500`) | `+$16.1897` (`-$15.4911`) |
| Incomplete-Window MTM (`#33`) | `+$1.6056` | `+$0.1236` | `$0.0000` | `+$0.1236` |
| **Reported Net PnL** | **`+$33.2864`** | **`+$8.2905` (`-$24.9959`)** | **`+$25.4308` (`-$7.8556`)** | **`+$16.3133` (`-$16.9731`)** |
| Net PnL + 3% Est. Maker Rebate | `+$34.0707` | `+$9.0985` | `+$26.2252` | `+$17.1243` |
| Max Drawdown (% of `$50` cap) | `$12.2443` (`24.49%`) | `$9.8056` (`19.61%`) | **`$7.3268` (`14.65%`)** | `$10.3199` (`20.64%`) |
| Worst Individual Window | `-$9.0000` (`#238`) | **`-$4.1509` (`#238`)** | `-$4.7958` (`#15`) | `-$4.8510` (`#29`) |
| Captures Improved / Same / Degraded | — | `4` / `9` / **`8`** | `3` / `14` / **`4`** | `4` / `10` / **`7`** |

### Merge Pair-Cost Distribution (Why Merge Gain Collapsed)

| Pair-Cost Bucket | Baseline (Pairs / Gain) | `exp_vwap_108` (Pairs / Gain) | `exp_vwap_108_t120` (Pairs / Gain) | `t1trig_065` (Pairs / Gain) |
| --- | ---: | ---: | ---: | ---: |
| `<= $0.98` (Core Maker Edge) | `603.14` / `+$85.0748` | `515.58` / `+$67.5918` (`-$17.48`) | `588.59` / `+$80.1968` (`-$4.88`) | `562.00` / `+$73.3949` (`-$11.68`) |
| `$0.98 – $1.00` | `121.29` / `+$0.7311` | `118.07` / `+$0.6810` | `116.29` / `+$0.6811` | `118.08` / `+$0.6811` |
| `$1.00 – $1.06` | `341.92` / `-$11.2388` | `295.01` / `-$9.2866` | `336.92` / `-$10.9588` | `300.01` / `-$9.5866` |
| `> $1.06` (Override Merges) | `24.08` / `-$9.3256` | **`253.63` / `-$35.8843`** (`-$26.56`) | `102.47` / `-$19.8305` (`-$10.50`) | `216.85` / `-$32.5622` (`-$23.24`) |

`exp_vwap_108` expanded `> $1.06` merges by **`10.5x`** (`24.08 -> 253.63` pairs, adding `-$26.5587` in direct override losses) while simultaneously cannibalizing `87.56` pairs of `<= $0.98` profitable mean-reversion merges (`-$17.4830`).

---

## 6. Detailed Review: Five Mean-Reverting Control Windows (and Key Side Effects)

| Win # | Cap # | Baseline Net | `exp_vwap_108` Net (Delta) | `exp_vwap_108_t120` Net (Delta) | Failure Mechanism |
| ---: | ---: | ---: | ---: | ---: | --- |
| `#29` | `#6` | `+$4.5981` | `-$3.6969` (**`-$8.2950`**) | `+$4.5981` (`$0.0000`) | Bought `20` Up @ `$0.51` (`185s`). Down spiked to `$0.69` (`177s`), then crashed to `$0.26` (`95.7s`). `C1` chased the spike (`pc=$1.14–$1.20`). |
| `#33` | `#6` | `+$1.4753` | `-$2.8370` (**`-$4.3123`**) | `-$3.6091` (**`-$5.0844`**) | Bought `20` Up @ `$0.38` (`255s`), `13.38` unhedged. `C1` chased Down @ `$0.79–$0.89`; `C2` still panicked @ `$0.88` (`117s`) before reversal (`+$1.61` MTM). |
| `#66` | `#10` | `+$5.2340` | `-$2.2000` (**`-$7.4340`**) | `-$3.0000` (**`-$8.2340`**) | Bought `20` Up @ `$0.32` (`193s`). Baseline held cap until reversal at `64.9s–52.4s` (filled Down @ `$0.26–$0.67`). `C1` panicked @ `$0.79` (`153s`); `C2` panicked @ `$0.83` (`116s`). |
| `#81` | `#11` | `+$4.1000` | `-$3.2400` (**`-$7.3400`**) | `+$4.1000` (`$0.0000`) | Bought `20` Down @ `$0.65` (`203s`). Up spiked to `$0.59` (`148s`), then collapsed to `$0.13–$0.16` (`115s–106s`). `C1` chased Up @ `$0.43–$0.59` (`pc=$1.08–$1.24`). |
| `#176` | `#17` | `+$2.1734` | `-$0.7154` (**`-$2.8888`**) | `-$0.7154` (**`-$2.8888`**) | Bought `20` Up @ `$0.39` (`184s`), `14.29` unhedged. `C1` and `C2` both panicked at `113s` @ `$0.72` (`pc=$1.11`), erasing the `+$2.62` settlement win on the remaining `4.29` Up. |
| **5 Controls** | | **`+$17.5808`** | **`-$12.6893` (`-$30.2701`)** | **`+$1.3736` (`-$16.2072`)** | **Collateral damage on just 5 controls exceeded target-window savings by `3.2x` (`C1`) and `2.9x` (`C2`).** |

### Other Notable Causal Replay Side Effects (`exp_vwap_108`)

- **Window `#123` (Cap `#14`, `+$4.7792` gain):** The only non-target material winner. Baseline entered `+14.08` Up @ `$0.62` and paid `$0.95` in Tier-3 (`tau = 54.3s`, `pc = $1.57`, `-$8.0256` merge). `exp_vwap_108` exited earlier at `$0.57–$0.65` (`tau = 158s–150s`, `pc = $1.19–$1.27`).
- **Window `#211` (Cap `#20`, `-$2.8621` loss):** Bought `20` Up @ `$0.27`. Brief Down spike to `$0.85` at `tau = 240s` triggered `C1` on `6.33` shares (`pc = $1.12`) before Down dropped to `$0.32–$0.43`.
- **Window `#23` (Cap `#5`, `-$2.0023` loss):** Bought `20` Up @ `$0.59`. Brief Down spike to `$0.51` (`6.7s` later) triggered `C1` on all `20` shares (`pc = $1.10`, `-$2.00`) before Down fell to `$0.35–$0.40` (baseline `+$0.79` merge).
- **Window `#157` (Cap `#16`, `-$1.7484` loss — Causal Dust Creation):** Baseline bought `5.00` Up @ `$0.39` (`257s`) and cleanly hedged `5.00` Down @ `$0.57` (`205s`, `+$0.2000`, `0` residual). `C1` triggered `2.1s` after entry (`tau = 254.7s`) and received a **`1.88`-share partial fill** @ `$0.68`, creating `+3.12` Up dust (`< 5sh`). That forced two `5sh` min-lot sign flips (`+3.12 -> -1.88 -> +3.12`), ending in a `-$1.4664` settlement loss.

---

## 7. Maker-Edge Preservation & Falsification Audit

### Direct Answers to All 10 Falsification Questions

1. **Does the candidate fill earlier than baseline?**
   **Yes.** `exp_vwap_108` triggered quotes above `$1.06 - VWAP` in `26/241` windows (vs `3/241` in baseline Tier-3) and filled in all `26`, rescuing `#15` (`133.5s`), `#86` (`145.8s`), `#227` (`68.2s`), and `#238` (`158.8s–132.5s`) well before book collapse.
2. **Does it preserve maker status?**
   **Yes.** `100%` of fills (`362/362` in `C1`, `353/353` in `C2`) remained resting Post-Only Maker fills with `$0.0000` taker fees and `0` Post-Only broker rejections.
3. **Does the new queue position improve fill probability, or merely move quotes?**
   **It materially changed executions.** Moving passive bids up from the capped level to `best_ask - 1 tick` converted `4` zero-fill/stranded target windows into `100%` complete fills (`0.00` residual), and changed final outcomes in `20/241` windows (`6` improved, `14` degraded).
4. **Does it create additional adverse fills when prices mean-revert?**
   **Yes — severely.** In `14` windows, sellers dumped into the elevated reducing bid right at the peak of temporary adverse spikes (`#29`, `#33`, `#66`, `#81`, `#176`, `#23`, `#211`, `#30`, `#157`, `#161`, `#50`, `#65`, `#168`, `#221`), causing `-$39.4051` in combined degradation (`-$30.2701` on the 5 control windows alone).
5. **Does it increase expensive fills where pair cost exceeds `$1.00`?**
   **Yes.** Pairs merged at `> $1.06` jumped `10.5x` in `exp_vwap_108` (`24.08 -> 253.63` pairs) and `4.3x` in `exp_vwap_108_t120` (`24.08 -> 102.47` pairs).
6. **Does it increase negative Merge Gain?**
   **Yes.** Gross negative Merge Loss more than doubled in `exp_vwap_108` (`-$20.5644 -> -$45.1709`, `+119.7%`) and rose `+49.7%` in `exp_vwap_108_t120` (`-$20.5644 -> -$30.7893`).
7. **Does it create more dust positions through partial execution?**
   **Mixed.** Corpus dust-window count dropped slightly (`36 -> 33` in `C1`, `36 -> 35` in `C2`) because `C1` cleared several mid-sized exposures. However, partial execution under override actively created brand-new dust and settlement loss in `#157` (`0sh -> +3.12sh`, `-$1.7484`) and in `#238` under `C2` (`2.22sh` residual).
8. **Does it create opposite-side overshoot or unintended inventory?**
   **No reducing-leg overshoot for `|q| >= 5sh`.** True reducing-leg overshoots from `|q| >= 5sh` were **`0`** across all four variants because `cap_tier1_reducing_to_delta = true` held. All `89` sign flips in baseline and `exp_vwap_108` (`88` in `C2`) occurred strictly when `|q| < 5sh` (`86` mandatory `5sh` min-lot reflections for `1 <= |q| < 5sh`, plus `3` sub-`1sh` flat resets). Only the latched `t1trig_065` added `1` extra flip (`#29`).
9. **Does the improvement survive across all 21 captures, or only the four worst windows?**
   **It fails across the corpus.** In `exp_vwap_108`, `8` captures degraded vs only `4` improved (`9` unchanged). In `exp_vwap_108_t120`, `4` captures degraded vs `3` improved (`14` unchanged). Even excluding the worst degraded capture (`Cap #6`), `exp_vwap_108` still lost `-$11.15` vs baseline.
10. **Does the strategy still have a positive bottom line after full accounting?**
    **Barely positive, but severely damaged.** Realized Net PnL collapsed from `+$31.6808` to `+$8.1669` (`-74.2%`) in `C1` and to `+$25.4308` (`-19.7%`) in `C2`.

### Economic Root Cause: Why the Hypothesis Failed

Two structural facts explain this result:

1. **The `$1.06 - VWAP` cap is an implicit mean-reversion hold filter.** Baseline's `+$85.0748` of `<= $0.98` Merge Gain averages `+$0.141` per pair in markets with `$0.01–$0.03` spreads. That edge comes from refusing to lock in losses during temporary adverse swings and capturing `+$0.19` to `+$0.42` per pair when BTC mean-reverts `30–130s` later.
2. **Martingale pricing plus passive winner's curse.** Holding `1` unhedged share bought at VWAP $v$ when the opposing ask is $a$ (implied opposing win probability $\approx a$) has expected settlement PnL $\mathbb{E}[\text{Settle}] = (1 - a)(1 - v) - a v = 1 - a - v$. Merging passively at $b = a - 0.01$ locks in $1 - a - v + 0.01$, offering at most `1` tick of theoretical advantage in an efficient market while suffering severe **adverse selection**: resting override bids fill fastest at local price peaks right before mean-reversion (`#29`, `#66`, `#81`, `#211`, `#23`).
3. **No clean `tau` separator exists.** Restricting activation to `tau <= 120s` (`C2`) was simultaneously **too late** for `#15` (already `$0.95` at `120s`) and `#86` (already `$0.99` at `120s`), yet **too early** for `#66` (reversed at `64.9s–52.4s`), `#33` (reversed after `117s`), and `#176`.

### Smallest Correction to the Hypothesis

Do **not** chase adverse moves passively during active quoting. Instead, redirect research toward **preventing entry into one-sided runaway tails in the first place** (upstream entry selection):

- Look at the entries of the four failed windows: `#15` bought Up @ `$0.24` (`tau = 180.6s`), `#86` bought Up @ `$0.20` (`tau = 184.1s`), `#227` bought Down @ `$0.32` (`tau = 79.4s`), and `#238` bought Down @ `$0.45` (`tau = 187.0s`). Three of the four were cheap tail entries (`$0.20–$0.32`) caught against an accelerating directional trend.
- Tightening `max_entry_prob` (for example, `0.80 -> 0.70–0.75`, refusing to open fresh unhedged inventory below `$0.25–$0.30` or late in the window) or gating new accumulating fills on Binance trend velocity directly targets the entry leg **before** exposure exists, without destroying mean-reversion recovery on already-open inventory.

---

## 8. Secondary Analysis (Separate Diagnostic): 36 Baseline Dust Windows

Per §8, no dust modifications were implemented. Below is the exact baseline diagnostic of all `36` windows ending with `0 < residual < 5.00` shares (combined Merge Gain `+$35.8919`, Settlement PnL `-$12.7415`, Net PnL **`+$23.1504`**).

### Breakdown by Mechanism and Residual Size

| Dust Category / Bucket | Windows | Merge Gain | Settlement PnL | Net PnL |
| --- | ---: | ---: | ---: | ---: |
| **A. Sub-5sh initial partial fill (`max_unp < 5sh`)** | `16` (`2W/14L`) | `+$4.2315` | `-$6.5410` | **`-$2.3095`** |
| **B. Partial-merge odd-lot remnant (`max_unp >= 5sh -> res < 5sh`)** | `20` (`4W/16L`) | `+$31.6604` | `-$6.2005` | **`+$25.4599`** |
| Residual `0 – 1sh` | `13` | `+$10.4393` | `-$1.3098` | `+$9.1295` |
| Residual `1 – 2sh` | `6` | `+$10.8327` | `-$2.5588` | `+$8.2739` |
| Residual `2 – 3sh` | `7` | `+$10.5278` | `-$4.8476` | `+$5.6802` |
| Residual `3 – 4sh` | `9` | `+$4.5356` | `-$6.6422` | **`-$2.1066`** |
| Residual `4 – 5sh` | `1` | `-$0.4435` | `+$2.6169` | `+$2.1734` |

### Answers to the Five Dust Design Questions

1. **How partial fills create dust:** `33/36` dust windows experienced fractional-share fills (e.g., `0.01`, `1.88`, `3.12` shares) and `32/36` experienced sub-`5sh` partial fills from live FIFO trade prints.
2. **Does `cap_tier1_reducing_to_delta` contribute?** **Yes, specifically for `1.0 <= |q| < 5.0` shares.** In [station7_risk.rs](../crates/risk-lifecycle/src/station7_risk.rs#L676-L690), when `|q| >= 5.0`, `cap_tier1_reducing_to_delta` sets exact fractional reducing size (e.g. `19.97sh`) and **prevents** dust. However, when `1.0 <= |q| < 5.0`, it clamps the reducing order to `min_order_size = 5.00` shares. Filling `5.00` shares against fractional dust `d` deterministically reflects inventory to `-(5.0 - d)` (observed in **`21/36`** dust windows, e.g., `#211`: `+1.69 -> -3.31 -> +1.69` six consecutive times), making exact zero unreachable unless another fractional partial fill occurs.
3. **Do `max_combined_bid` adjustments create odd-lot residuals?** **No directly.** `max_combined_bid` alters quote price, not order quantity. It only affects dust indirectly by keeping a stranded sub-`5sh` remnant unhedged after price moves away.
4. **Can partial fills be safely constrained?** **No.** Resting CLOB maker orders cannot reject partial fills on Polymarket, and suppressing/avoiding windows that encounter sub-`5sh` fills would destroy massive edge: `44/241` windows had at least one sub-`5sh` fill (`9` ended with `0` residual earning `+$14.9804`, and `36` dust windows earned `+$35.8919` Merge Gain / `+$23.1504` Net PnL).
5. **Can reducing-leg sizing or accumulation clear dust?** **Yes, as a low-priority separate follow-up.** Because Polymarket accepts fractional order sizes `>= 5.00` shares (verified in live book prints and replay fills), two targeted ideas warrant future testing without touching `|q| >= 5sh` logic:
   - When `1.0 <= |q| < 5.0`, allow the reducing leg to quote `5.0 + frac(|q|)` (or integer-align the post-fill residual) instead of exact `5.00`, breaking the `d <-> -(5 - d)` fractional trap.
   - Or allow the accumulating leg to top up `1.0 <= |q| < 5.0` back above `5.0` shares when pricing is favorable (`<= $0.98`), converting unflattenable `3–4sh` dust (`-$2.1066` net) into a standard executable hedge size.
   - **Trade-off:** Total recoverable dust settlement loss is capped by the `-$2.3095` net loss in Type A (`16` sub-`5sh` entry windows) and `-$2.1066` in `3–4sh` dust — far smaller than the `+$65.24` core Maker Merge edge.

---

## 9. Remaining Weaknesses and Limitations

1. **Development/IS corpus only (`241` windows across `21` captures):** Capture `#22` and `.inprogress.jsonl` were strictly excluded to preserve the Holdout Vault. Because the hypothesis already failed decisively in-sample, spending holdout data is unnecessary.
2. **Incomplete Window `#33`:** Capture `#6` ends mid-window on `#33`, leaving `+$1.6056` unrealized MTM in baseline vs `+$0.1236` (`C1`) and `$0.0000` (`C2`). Even evaluating **Realized Net PnL only** (excluding `#33` MTM completely), `exp_vwap_108` still lost **`-$23.5139`** (`+$31.6808 -> +$8.1669`) and `exp_vwap_108_t120` still lost **`-$6.2500`** (`+$31.6808 -> +$25.4308`).
3. **Conservative FIFO queue model:** Replay uses `QueueDecayPolicy::Conservative` and `40ms` latency. More optimistic queue fills would increase override fill frequency on both target windows and mean-reverting spikes, leaving the adverse-selection asymmetry intact.
4. **No live taker-exit counterfactual tested here:** This study evaluated passive Maker overrides only (preserving `$0` fees). A marketable taker exit incurs Polymarket's dynamic taker fee on top of crossing the spread, worsening the martingale payoff equation further.

---

## 10. Final Recommendation & Decision Gate Scorecard (§11)

| §11 Decision Criterion | `exp_vwap_108` | `exp_vwap_108_t120` | `t1trig_065` | Gate |
| --- | --- | --- | --- | :---: |
| Material settlement-loss reduction on 4 target windows | `+$9.5200` (`4 -> 0` windows `>= 5sh`) | `+$5.5000` (`4 -> 2` windows) | `+$9.5200` (`4 -> 0` windows) | **PASS** |
| Positive / meaningfully improved full-corpus Net PnL | **`-$24.9959`** (`-75.1%`) | **`-$7.8556`** (`-23.6%`) | **`-$16.9731`** (`-51.0%`) | **FAIL** |
| Most existing Maker Pair-Merge Gain preserved | **`35.4%`** (`-$42.14`) | `76.8%` (`-$15.15`) | **`48.9%`** (`-$33.31`) | **FAIL** |
| Zero false-positive taker fills hidden as Maker | `0` taker / `0` fees / `0` PO-rej | `0` taker / `0` fees / `0` PO-rej | `0` taker / `0` fees / `0` PO-rej | **PASS** |
| No material worsening of drawdown or worst window | DD `$9.81` (`-20%`), Worst `-$4.15` | DD `$7.33` (`-40%`), Worst `-$4.80` | DD `$10.32` (`-16%`), Worst `-$4.85` | **PASS** |
| Correct accounting & deterministic replay parity | `6/6` exact micro-USDC parity | `6/6` exact micro-USDC parity | `6/6` exact micro-USDC parity | **PASS** |

**Recommendation: NO-GO.** Do **not** enable any passive cross-tick override as a production default, and keep `--tier1-hedge-trigger-price off` in Config `#6`. Although the override reduced Max Drawdown (`$12.24 -> $7.33–$9.81`) and cut worst-window loss (`-$9.00 -> -$4.15`), it failed the core economic gate by destroying **`24%–75%` of total strategy Net PnL** through premature negative merges on mean-reverting windows.

### Reproducible Offline Command

> **Archived script note:** As noted in §2, `scripts/run_passive_override_replay.py` and the experimental replay-only CLI flags are archived on the unmerged `validate_strategy_edge_metric` branch and are not present on `main`.

```powershell
python scripts/run_passive_override_replay.py --workers 6
```
