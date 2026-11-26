# Sweep Results Investigation: Accounting Telemetry, Default Resolution & 21-Capture G3/G5 Failure Audit

> [!NOTE]
> **Scope.** Part I preserves the current accounting telemetry validation and runtime default-resolution audit. Part II documents the diagnostic investigation comparing the promoted `20`-capture boundary sweep (`25-11_21-55-50Z_boundary-9runs_20caps-226w`) against the `21`-capture single-config sweep (`26-11_07-55-07Z_1runs_21caps-241w`) that failed survival gates `G3` and `G5`.

- **Part I Dataset (raw output since pruned):** `logs/sweeps/25-11_16-21-55Z_18runs_20caps-226w` (`18` configurations × `20` captures = `360` JSONL files, `226` 5-minute windows, `QueueDecay=conservative`); the findings below are the retained record of that audit.
- **Part II Runs Compared:** `logs/sweeps/25-11_21-55-50Z_boundary-9runs_20caps-226w` (Config `#6/9`) vs. `logs/sweeps/26-11_07-55-07Z_1runs_21caps-241w` (Config `#1/1`).
- **Verification Standard:** [strategy-verification-spec.md](strategy-verification-spec.md)

---

## Part I — Accounting Telemetry & Default-Resolution Audit

## 1. Accounting Telemetry Validation & Event-Level Reconstruction

### 1.1 Validation of New Telemetry Fields

Five new accounting fields (`#[serde(default)]`) were added to `PaperLedger` ([accounting.rs](../crates/paper-sim/src/accounting.rs#L59-L73)) and exported via `LifecycleSummaryEvent` ([lifecycle_report.rs](../crates/cli/src/lifecycle_report.rs#L333-L371)) and `LivePaperSimulator::finalize_stats` ([lifecycle_cmd.rs](../crates/cli/src/lifecycle_cmd.rs#L2459-L2586)):

| Field | Writer(s) | Validation Result |
| --- | --- | --- |
| `total_shares_bought_micro` | `record_fill` (`OrderSide::Buy`) and `record_taker_flatten_with_merge` (`OrderSide::Buy`) | Sum of all BUY shares across passive fills and mandatory buy-side flattens. |
| `volume_in_micro` | `record_fill` (`OrderSide::Buy`) and `record_taker_flatten_with_merge` (`OrderSide::Buy`) | **No double-counting:** `record_fill` only handles passive FIFO fills (`on_fill`), while `record_taker_flatten_with_merge` only handles `ExecuteFlattenFak`. Neither calls the other. Both use exact integer micro-USDC truncation (`(shares * price) / 1_000_000`). |
| `total_merge_gain_micro` | `execute_pair_merge` | Signed net merge P&L across all pair merges (`redemption_micro - cost_up_deducted - cost_dn_deducted`). |
| `positive_merge_gain_micro` | `execute_pair_merge` | Sum of `realized_gain_micro >= 0` across all merges. |
| `negative_merge_loss_micro` | `execute_pair_merge` | Sum of `abs(realized_gain_micro)` where `realized_gain_micro < 0`. Reconciles identically: `total_merge_gain_micro == positive_merge_gain_micro - negative_merge_loss_micro`. |

**Determinism and test proof:**
- Unit vector `test_vector_9_volume_in_and_signed_merge_gain_tracking` ([vectors.rs](../crates/paper-sim/tests/vectors.rs#L409-L468)) verifies buy notional (`$30.50`) and signed merge splits (`+$1.00`, `-$1.50`, net `-$0.50`).
- Replay determinism test `replay_is_deterministic_across_runs` ([lifecycle_cmd.rs](../crates/cli/src/lifecycle_cmd.rs)) asserts bit-identical output across repeated runs for all five fields.

### 1.2 Why No Sweep Rerun Was Needed

Event-level reconstruction across all `360` dataset JSONL files (since pruned) succeeded with **`0` reconciliation mismatches**:
- Every file already emits full `"event":"fill"` (`filled_shares_micro`, `price_micro`, `side`), `"event":"flatten"` (`size_micro`, `price_micro`, `side`, `fee_micro`), `"event":"merge"` (`realized_gain_micro`), `"event":"window_end"`, and `"event":"summary"` rows.
- Exact identities hold in all `360` files of that dataset:
  - `count(fill) == summary.fills_count` and `count(merge) == summary.merges_count`
  - `sum(merge.realized_gain_micro) + summary.settlement_pnl_micro - summary.fees_paid_micro == summary.realized_pnl_micro`
  - `sum(flatten.fee_micro) == summary.fees_paid_micro`
- **Buy-side flatten correction (`9` events across the dataset):** Earlier ad-hoc totals summed only `"event":"fill"` notional (`$840.46` for `#13`, `$1,258.49` for `#14`). Nine mandatory buy-side flattens also occurred (`Cap#10` `btc-updown-5m-1795451400` BUY `7.38sh @ $0.06 = $0.4428` on `#2/#8/#14`; `Cap#20` `btc-updown-5m-1795613100` BUY `16.92sh @ $0.01 = $0.1692` on `#13–#18`). Including buy-side flattens matches Rust's `volume_in_micro` exactly (`$840.63` for `#13`, `$1,259.11` for `#14`). The `#13`/`#14` labels here are retired `18`-configuration ids, kept only as dataset coordinates.

### 1.3 Safe Legacy JSONL Handling

In `Read-CaptureJsonlMetrics` ([sweep-params.ps1](../scripts/sweep-params.ps1)), legacy JSONL files missing the five new summary keys are reconstructed safely:
1. When `volume_in_micro` or `total_shares_bought_micro` is absent from `summary`, the parser scans `"event":"fill"` (`filled_shares_micro`) and `"event":"flatten"` (`size_micro`) where `side == "BUY"`.
2. When merge breakdown keys are absent, it scans `"event":"merge"` events (`realized_gain_micro`).
3. **Missing vs. genuine zero protection:** Event-scan totals are only accepted when matching events exist (`scannedBuyEvents > 0` / `scannedMergeEvents > 0`) or when `summary` itself reports zero activity (`fills == 0 && flattens == 0` / `merges == 0`). If a summary-only JSONL reports nonzero `merges_count` or `fills_count` with no event rows, `MergePos`/`MergeNegLoss` stay `NaN` (not `0.0`), and `VolumeIn` uses the explicit synthetic fallback.

---

## 2. Promoted Config Runtime Resolution & Conflicting Defaults Audit

### 2.1 Runtime Loading Path Verified

Root [DEFAULTS.json](../DEFAULTS.json) is promoted to **Config #6/9** (`max_entry_price: 0.65`, `tier1_hedge_trigger_price: null`, `entry_cutoff_secs: 120`, `cap_tier1_reducing_to_delta: true`, `entry_open_delay_secs: 15`) alongside all six calibration sync files (`AGENTS.md` §7). The earlier Config #13 (`$0.60`) baseline is superseded by the boundary sweep described in [boundary-sweep-selection-report.md](boundary-sweep-selection-report.md):
- [DEFAULTS.json](../DEFAULTS.json#L13-L44)
- [.env.example](../.env.example#L86-L104)
- `EngineConfig::default()` / `EngineConfig::hardcoded_fallback()` ([config.rs](../crates/core-types/src/config.rs#L521-L592)) embeds `DEFAULTS.json` at compile time via `include_str!("../../../DEFAULTS.json")`.
- Live and replay CLI entrypoints (`lifecycle_cmd.rs`, `paper_cmd.rs`, `trace_cmd.rs`, `feeds_cmd.rs`, `doctor.rs`, `main.rs`) load `EngineConfig::from_env()?`, which starts from `EngineConfig::default()` (`DEFAULTS.json`). `lifecycle_cmd.rs` explicitly passes `cfg.cap_tier1_reducing_to_delta`, `cfg.entry_open_delay_secs`, `cfg.entry_cutoff_secs`, and `MathPipelineConfig::from_engine_config(cfg)`. `exec_cmd.rs` dry-run loads `MathPipelineConfig::try_default()?`, which calls `MathPipelineConfig::from_engine_config(&EngineConfig::default())`.
- Verified by `embedded_defaults_json_parses_validates_and_matches_default` ([config.rs](../crates/core-types/src/config.rs#L1382)), `config_locked_defaults_validate` ([lib.rs](../crates/core-types/src/lib.rs#L227)), and `cli_lifecycle_and_pipeline_defaults_resolve_promoted_defaults` ([main.rs](../crates/cli/src/main.rs)).

### 2.2 Conflicting Library Struct Defaults (Non-Runtime)

Three standalone library constructors keep conservative/minimal struct defaults for unit-test isolation and are **always overridden** by `EngineConfig` in the CLI runtime:

| Struct & Constructor | Field(s) Differing from `DEFAULTS.json` | Impact at Runtime |
| --- | --- | --- |
| `RiskConfig::default()` ([station7_risk.rs](../crates/risk-lifecycle/src/station7_risk.rs#L187-L199)) | `cap_tier1_reducing_to_delta: false` (`DEFAULTS.json` = `true`) | **None:** `spawn_managed_session` (`lifecycle_cmd.rs:2661`) overrides from `cfg.cap_tier1_reducing_to_delta`. |
| `LifecycleConfig::default()` ([lifecycle.rs](../crates/risk-lifecycle/src/lifecycle.rs#L91-L103)) | `entry_open_delay_secs: 0`, `entry_cutoff_secs: None` (`DEFAULTS.json` = `15`, `Some(120)`) | **None:** `build_risk_actor` (`lifecycle_cmd.rs:2664-2665`) overrides from `cfg.entry_open_delay_secs` and `cfg.entry_cutoff_secs`. |
| `PricerConfig::from_gamma()` ([station4_6_pricer.rs](../crates/math-engine/src/station4_6_pricer.rs#L37-L50)) | `max_cross_tick_loss_micro: None` (`DEFAULTS.json` = `Some(80_000)`); `max_entry_price: Some(0.60)` differs from `DEFAULTS.json` (`Some(0.65)`) | **None:** CLI and `PricerConfig::try_default()` construct from `EngineConfig`. |

---

## 3. Re-Validating This Audit

1. `cargo test --workspace` covers the defaults-parity assertions named in 2.1.
2. `poly-as lifecycle --source replay --input <capture>.jsonl --jsonl <out>.jsonl`, then check the `"event":"summary"` row: all five fields from 1.1 are present, and `positive_merge_gain_micro - negative_merge_loss_micro == total_merge_gain_micro`.
3. Event-level re-derivation (fill/flatten/merge totals against the summary row) must report zero mismatches, exactly as in 1.2.


---

## Part II — 21-Capture Sweep G3/G5 Failure Investigation (`25-11_21-55-50Z` vs. `26-11_07-55-07Z`)

### 4. Executive Verdict & Root-Cause Summary *(Observed, Derived & Interpretation)*

Adding the 21st capture (`26-11_00-50-00Z_1h14m59s_15w`) did **not** act alone. The drop from `+$58.79` (`+$24.19` OOS, `162%` retention, `19.8%/40.9%` drag/tail) to `+$33.29` (`+$1.61` OOS, `12%` retention, `39%/63%` drag/tail) is a **compound effect split almost evenly (~50/50) between a replay-binary timing fix and genuine out-of-sample deterioration**:

| Component | Total `Net0` Impact | OOS `Net0` Impact | Classification |
|---|---:|---:|---|
| **Previous boundary sweep (`S1`, 20 caps / 226w, pre-fix binary)** | **+$58.7865** | **+$24.1864** | Baseline |
| 1. Replay binary timing fix (`5cf74b9`, `elapsed_secs` from `tau_ms`) on shared 20 caps | `-$13.2558` | `-$10.4987` | **Replay binary / code difference** |
| 2. 70/30 split shift (`w#159..w#168`, 10 windows moved OOS -> IS) | `$0.0000` | `+$0.1623` | **Split boundary shift** |
| 3. New capture `#21` (`26-11_00-50-00Z_1h14m59s_15w`, `w#227..w#241`) | `-$12.2443` | `-$12.2443` | **Genuine OOS deterioration** |
| **Latest sweep (`S2`, 21 caps / 241w, post-fix binary)** | **+$33.2864** | **+$1.6057** | Latest sweep |

- **Root Cause of G3 Failure (`12% < 50%`):**
  1. **Replay-binary artifact (`-$10.4987` OOS):** S1 ran before the PR `#58` CodeRabbit review fix (`5cf74b9`) was merged. In that pre-fix binary (`target/sweep-work/release/poly-as.exe`), `EpochLifecycleController::evaluate` truncated `tau_secs` before computing `elapsed_secs`, opening `entry_open_delay_secs = 15` up to `999ms` early (`t = 14.001s`). In `Cap #20` (`25-11_12-50-01Z`, `btc-updown-5m-1795613100`), that bug allowed a `2.17sh` Up fill at `t = 14.885s`, cascading into `19` fills and a `+$9.31` flatten merge (`+$10.9487` window Net0 — `45.3%` of S1's entire OOS profit). In S2 (`target/release/poly-as.exe`, post-`5cf74b9`), `t = 14.885s` was properly blocked (`elapsed = 14 < 15`), reducing that window to `+$0.4500` (`-$10.4987`) and cutting `Cap #20` from `+$21.2443` to `+$10.7456`.
  2. **Genuine OOS loss (`-$12.2443` OOS):** `Cap #21` (`26-11_00-50-00Z`) lost `-$12.2443`, led by `w#13` (`btc-updown-5m-1795657800`, `-$9.0000` unhedged settlement when `MandatoryFlatten` hit an empty opposing book `Bk 99/-----/1`), `w#2` (`1795654500`, `-$2.8547`), and `w#9` (`1795656600`, `-$1.2236`).
  3. **Compound threshold breach:** On the S2 binary without `Cap #21`, the original `20` captures still pass `G3` (`99.88%`, `+$13.69` OOS). On the S1 binary with `Cap #21` (whose fills all occurred at `35.97s..118.69s`, outside the `14.xxx` / `119.xxx` bug windows), OOS would have been `+$11.78` (`~79%` retention, G3 PASS). Only the combination of the `-$10.50` binary correction and `Cap #21`'s `-$12.24` loss collapsed OOS to `+$1.61` (`12%`).
- **Root Cause of G5 Failure (`Drag 39% > 30%`, `TailDrag 63% > 50%`):**
  1. Note that S2's `summary.txt` prints `FAIL(G3)` because `Evaluate-SurvivalGateRow` reports only the **first** failing gate; **G5 also failed** on both metrics (`39.11% > 30%`, `63.08% > 50%`).
  2. G5 failed due to **both** a `+75.7%` jump in unhedged settlement losses (`$19.1010 -> $33.5607`, driven by `Cap #21`'s `-$16.0608` settlement loss) **and** an `11.3%` drop in the positive-merge denominator (`$96.7065 -> $85.8059`, driven by the `5cf74b9` binary change removing `$15.48` of positive merges in `Cap #11` and `Cap #20`, only partially offset by `Cap #21`'s `+$4.5834`). Negative merge losses stayed nearly unchanged (`$20.4162 -> $20.5644`).

---

### 5. Dataset, Binary, and Configuration Identity Verification *(Observed)*

#### 5.1 Capture Files & Byte-Identity Proof
- **Shared `20` captures:** Both sweeps share the exact same `20` captures (`22-11_10-10-00Z_29m59s_6w` through `25-11_12-50-01Z_1h14m58s_15w`, `226` windows total).
- **Additional `21`st capture in S2:** `logs/captures/26-11_00-50-00Z_1h14m59s_15w.jsonl` (`1,320,915,789` bytes, `CreationTimeUtc 2026-11-26 00:50:00`, `LastWriteTimeUtc 2026-11-26 02:05:00`, `15` `window_end` rows).
- **Capture immutability between S1 and S2:** All `20` raw `.jsonl` capture files in `logs/captures/` have `LastWriteTimeUtc <= 2026-11-25 14:04:59` (~`8` hours before S1 started at `21:55:50Z`). Across all `20` shared captures, S1 and S2 report **identical** `ticks_evaluated`, `epochs_completed`, and `phase_transitions`, and `16` of the `20` shared captures match to the micro-dollar in every accounting field (`net_pnl_micro`, `volume_in_micro`, `fills_count`, `merges_count`, `settlement_pnl_micro`).

#### 5.2 Binary & Code Version Divergence
S1 and S2 did **not** run on the same compiled binary:

| Artifact | S1 (`25-11_21-55-50Z`) | S2 (`26-11_07-55-07Z`) |
|---|---|---|
| **Sweep Start / End (Local UTC+2)** | `25-11 23:56` – `26-11 00:08:29` | `26-11 09:55` – `26-11 10:00:44` |
| **Executable Artifact** | `target/sweep-work/release/poly-as.exe` (`10,198,016` bytes, built `25-11 23:36:53`) | `target/release/poly-as.exe` (`20,105,728` bytes, built `26-11 04:40:37`) |
| **Git State** | Pre-merge PR `#58` working code (before `5cf74b9` at `26-11 01:40:27`) | Post-`5cf74b9` / PR `#65` (`84bf56b` at `26-11 04:20:58`; lacks `--latency-ms` from `3e49dd1`) |
| **Manifest Guard** | No `sweep-manifest.json` | Emits `sweep-manifest.json` (added in `5cf74b9` review fix) |

Inspection of `git diff 90419cd..84bf56b` across all Rust crates confirms two runtime changes in `5cf74b9`:
1. **`crates/risk-lifecycle/src/lifecycle.rs` (`EpochLifecycleController::evaluate`):** Fixed sub-second truncation (`tau_secs = tau_ms / 1000` before `300 - tau_secs`) by computing `elapsed_secs = (epoch_duration_secs * 1000 - tau_ms) / 1000`. That eliminated a `999ms` early-open / early-cutoff skew on `[entry_open_delay_secs, entry_cutoff_secs)`.
2. **`crates/risk-lifecycle/src/station7_risk.rs` (`DeficitHedgeOverrideLatch`):** Immediately returns `false` when `tier1_hedge_trigger_price` is `None` (`off`), so it had zero effect on Config `#6/9` / `#1/1`.

#### 5.3 Effective Configuration Parity
Comparing the `Active Config's Parameters` table printed in S1 (`22-11_10-10-00Z/...ec-120.log`) and S2 (`22-11_10-10-00Z/...ep-0p8.log`) confirms **all `24` lines are identical**:
- `Gamma = 0.12`, `Include Variance Spread = true`, `Min Combined Spread = $0.01 (1 tick)`, `Max Combined Bid = $0.98`, `Cross Tick Loss Cap = 80000 micro-USDC`, `Max Entry Prob = 0.8`, `Max Entry Price = 0.65`, `Lead/Lag = 0.015 / 1000 ms / 1000 ms / true`, `Hedge Skew Multiplier = 1`, `Cap Tier-1 Reducing = true`, `Tier-1 Hedge Trigger = off`, `Entry Open Delay = 15 s`, `Entry Cutoff = 120 s`, `V_max = $50.00`, `Base Order Size = 20 shares`, `QueueDecay = conservative`.
- *CLI help-text note:* `crates/cli/src/main.rs` (`#L321`, `#L339`) still displays `(0.60 default)` and `(false default)` in `--help` docstrings, which is cosmetic only; runtime defaults come from `DEFAULTS.json` (`0.65` and `true`).

---

### 6. IS/OOS Split Audit & Calculation Inconsistencies *(Observed & Derived)*

#### 6.1 Exact Split Rule & Window Boundaries
In `scripts/sweep-params.ps1`:
1. Captures are sorted chronologically by `LastWriteTimeUtc` ascending (`#L1737`).
2. `Read-CaptureJsonlMetrics` extracts one `net_pnl_micro` entry per unique `market_slug` from `window_end` events in replay order (`$WindowNets`).
3. `Get-SurvivalInputs` (`#L506-L511`) sets `$isTarget = [int][Math]::Floor($WindowNets.Count * 0.7)`. Indices `0 .. ($isTarget - 1)` form IS; `$isTarget .. ($N - 1)` form OOS.

| Metric | S1 (`226w`) | S2 (`241w`) |
|---|---:|---:|
| **IS Window Range / Count** | `w#1 .. w#158` (`158` windows) | `w#1 .. w#168` (`168` windows) |
| **Last IS Window** | `Cap #16` (`24-11_19-45-00Z`) / `btc-updown-5m-1795551300` | `Cap #17` (`24-11_21-35-00Z`) / `btc-updown-5m-1795556400` |
| **First OOS Window** | `Cap #16` (`24-11_19-45-00Z`) / `btc-updown-5m-1795551600` | `Cap #17` (`24-11_21-35-00Z`) / `btc-updown-5m-1795556700` |
| **OOS Window Range / Count** | `w#159 .. w#226` (`68` windows) | `w#169 .. w#241` (`73` windows) |

#### 6.2 The `10` Windows Reassigned from OOS (S1) to IS (S2)
Adding `15` windows in `Cap #21` shifted `isTarget` from `158` to `168`, moving `10` previously evaluated windows from OOS into IS (`net = -$0.1623` total, identical in both runs):

| Global Index | Capture | Slug | S1 Split | S2 Split | `Net0` |
|---:|---|---|---|---|---:|
| `159` | `Cap #16` (`24-11_19-45-00Z`) | `btc-updown-5m-1795551600` | OOS | IS | `$0.0000` |
| `160` | `Cap #16` (`24-11_19-45-00Z`) | `btc-updown-5m-1795551900` | OOS | IS | `$0.0000` |
| `161` | `Cap #16` (`24-11_19-45-00Z`) | `btc-updown-5m-1795552200` | OOS | IS | `+$0.8377` |
| `162` | `Cap #16` (`24-11_19-45-00Z`) | `btc-updown-5m-1795552500` | OOS | IS | `-$1.0000` |
| `163–166` | `Cap #16` (`24-11_19-45-00Z`) | `1795552800..1795553700` (`4w`) | OOS | IS | `$0.0000` |
| `167–168` | `Cap #17` (`24-11_21-35-00Z`) | `1795556100..1795556400` (`2w`) | OOS | IS | `$0.0000` |
| **Total shifted** | `Cap #16–#17` | `10` windows | **OOS** | **IS** | **`-$0.1623`** |

#### 6.3 Split & Reporting Nuances Observed
1. **Trailing `complete: false` windows included (`15` in S1, `16` in S2):** `sweep-params.ps1` counts every `window_end` row, including trailing partial windows where capture recording ended mid-epoch (`3` of which carry nonzero P&L from merges/unrealized mark-to-market: `Cap #6 +$1.4753`, `Cap #8 -$0.6000`, `Cap #15 +$4.6275`). Even on `complete: true` windows only (`225w`: `157` IS `+$24.6313`, `68` OOS `+$3.1523`), S2 retention is `29.55%` (still `FAIL(G3)`).
2. **First-failure gate masking:** `Evaluate-SurvivalGateRow` (`#L593-L595`) breaks on the first failing gate in `G1..G6` order, so S2 prints `FAIL(G3)` even though `G5` (`39%/63%`) also failed.
3. **`G6 = n/a` on single-config runs:** With `1` config there are `0` Manhattan-1 neighbors (`NbrNum = NaN`), so `Verdict` reads `N/A` even when `G1–G5` all pass (as seen in the controlled 20-capture replay below).
4. **Integer rounding in `summary.txt`:** `MaxDD`, `Drag`, `TailDrag`, and `Retention` display as rounded integers (`19.42% -> 19%`, `24.49% -> 24%`, `11.66% -> 12%`, `39.11%/63.08% -> 39%/63%`).

---

### 7. Capture-by-Capture Attribution & OOS Bridge *(Observed & Derived)*

#### 7.1 Full `21`-Capture Comparison (`S1` vs. `S2`)
`S1_DD` / `S2_DD` reports the maximum running drawdown (`Peak - Eq`, in USD over `$50` `V_max`) reached during any window inside that capture. Zero `window_end` vs. `summary.net_pnl_micro` mismatches occurred in any capture.

| # | Capture (`_Nw`) | W | Split (`S1 -> S2`) | S1 `Net0` | S2 `Net0` | `ΔNet0` | S1 `Vol` | S2 `Vol` | S1 `Merge+` | S2 `Merge+` | S1 `Merge-` | S2 `Merge-` | S1 `Settle` | S2 `Settle` | Fees (`S1/S2`) | S1 `MaxDD` | S2 `MaxDD` |
|---:|---|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|---:|---:|
| `1` | `22-11_10-10-00Z_29m59s_6w` | `6` | `IS -> IS` | `-$1.4831` | `-$1.4831` | `$0.0000` | `$8.41` | `$8.41` | `$0.3079` | `$0.3079` | `$0.0000` | `$0.0000` | `-$1.7910` | `-$1.7910` | `$0 / $0` | `$1.4866` | `$1.4866` |
| `2` | `22-11_14-25-00Z_25m_5w` | `5` | `IS -> IS` | `+$0.9228` | `+$0.9228` | `$0.0000` | `$72.41` | `$72.41` | `$2.3863` | `$2.3863` | `$0.4725` | `$0.4725` | `-$0.9910` | `-$0.9910` | `$0 / $0` | `$2.1561` | `$2.1561` |
| **`3`** | **`22-11_15-35-00Z_25m_5w`** | `5` | `IS -> IS` | **`$0.0000`** | **`-$4.7958`** | **`-$4.7958`** | `$0.00` | `$4.82` | `$0.0000` | `$0.0000` | `$0.0000` | `$0.0006` | `$0.0000` | `-$4.7952` | `$0 / $0` | `$0.5603` | `$5.3561` |
| `4` | `22-11_16-40-00Z_29m59s_6w` | `6` | `IS -> IS` | `-$0.0041` | `-$0.0041` | `$0.0000` | `$0.00` | `$0.00` | `$0.0000` | `$0.0000` | `$0.0000` | `$0.0000` | `-$0.0041` | `-$0.0041` | `$0 / $0` | `$0.5644` | `$5.3602` |
| `5` | `22-11_19-45-00Z_30m_6w` | `6` | `IS -> IS` | `+$2.5302` | `+$2.5302` | `$0.0000` | `$81.18` | `$81.18` | `$4.3469` | `$4.3469` | `$0.2238` | `$0.2238` | `-$1.5929` | `-$1.5929` | `$0 / $0` | `$0.5342` | `$3.4300` |
| `6` | `22-11_23-15-00Z_30m_6w` | `6` | `IS -> IS` | `+$11.5782` | `+$11.5782` | `$0.0000` | `$45.78` | `$45.78` | `$10.1035` | `$10.1035` | `$0.1309` | `$0.1309` | `$0.0000` | `$0.0000` | `$0 / $0` | `$0.0000` | `$0.0000` |
| `7` | `23-11_00-55-00Z_30m_6w` | `6` | `IS -> IS` | `-$1.6961` | `-$1.6961` | `$0.0000` | `$31.71` | `$31.71` | `$0.5337` | `$0.5337` | `$1.0476` | `$1.0476` | `-$1.1822` | `-$1.1822` | `$0 / $0` | `$1.6961` | `$1.6961` |
| `8` | `23-11_01-40-00Z_49m59s_10w` | `10` | `IS -> IS` | `+$9.7385` | `+$9.7385` | `$0.0000` | `$93.62` | `$93.62` | `$11.1747` | `$11.1747` | `$1.0681` | `$1.0681` | `-$0.3681` | `-$0.3681` | `$0 / $0` | `$2.8958` | `$2.8958` |
| `9` | `23-11_05-45-00Z_1h14m59s_15w` | `15` | `IS -> IS` | `-$0.4992` | `-$0.4992` | `$0.0000` | `$27.06` | `$27.06` | `$1.0682` | `$1.0682` | `$0.3515` | `$0.3515` | `-$1.2159` | `-$1.2159` | `$0 / $0` | `$1.3012` | `$1.3012` |
| `10` | `23-11_15-50-00Z_1h15m2s_15w` | `15` | `IS -> IS` | `+$4.0140` | `+$4.0140` | `$0.0000` | `$32.71` | `$32.71` | `$4.9280` | `$4.9280` | `$0.3344` | `$0.3344` | `-$0.5796` | `-$0.5796` | `$0 / $0` | `$2.3192` | `$2.3192` |
| **`11`** | **`24-11_10-15-00Z_58m31s_12w`** | `12` | `IS -> IS` | **`-$0.7468`** | **`+$0.5432`** | **`+$1.2900`** | `$66.61` | `$45.32` | `$11.0732` | `$5.9232` | `$0.4600` | `$0.4600` | `-$11.3600` | `-$4.9200` | `$0 / $0` | **`$9.7100`** | `$4.1100` |
| `12` | `24-11_11-40-00Z_1h15m_14w` | `14` | `IS -> IS` | `+$5.4271` | `+$5.4271` | `$0.0000` | `$50.61` | `$50.61` | `$6.1024` | `$6.1024` | `$0.2208` | `$0.2208` | `-$0.4545` | `-$0.4545` | `$0 / $0` | `$6.5757` | `$1.2657` |
| `13` | `24-11_13-40-00Z_1h14m59s_15w` | `15` | `IS -> IS` | `+$0.1575` | `+$0.1575` | `$0.0000` | `$49.84` | `$49.84` | `$1.5450` | `$1.5450` | `$1.2000` | `$1.2000` | `-$0.1875` | `-$0.1875` | `$0 / $0` | `$5.2829` | `$1.2000` |
| **`14`** | **`24-11_15-30-00Z_1h15m_15w`** | `15` | `IS -> IS` | **`-$4.1112`** | **`-$3.3625`** | **`+$0.7487`** | `$47.11` | `$66.17` | `$4.0896` | `$4.8820` | `$8.2008` | `$8.2008` | `$0.0000` | `-$0.0437` | `$0 / $0` | `$9.3366` | `$5.2537` |
| `15` | `24-11_16-55-00Z_1h14m59s_15w` | `15` | `IS -> IS` | `+$4.9951` | `+$4.9951` | `$0.0000` | `$30.58` | `$30.58` | `$5.7679` | `$5.7679` | `$0.0000` | `$0.0000` | `-$0.7728` | `-$0.7728` | `$0 / $0` | `$8.2366` | `$3.4050` |
| `16` | `24-11_19-45-00Z_1h14m59s_15w` | `15` | `MIX(7/8) -> IS` | `+$3.6149` | `+$3.6149` | `$0.0000` | `$90.95` | `$90.95` | `$4.1833` | `$4.1833` | `$2.2357` | `$2.2357` | `+$1.6673` | `+$1.6673` | `$0 / $0` | `$4.0926` | `$1.0000` |
| `17` | `24-11_21-35-00Z_1h14m59s_15w` | `15` | `OOS -> MIX(2/13)` | `+$1.7268` | `+$1.7268` | `$0.0000` | `$61.70` | `$61.70` | `$2.0193` | `$2.0193` | `$2.1244` | `$2.1244` | `+$1.8319` | `+$1.8319` | `$0 / $0` | `$2.5466` | `$2.5466` |
| `18` | `24-11_23-05-00Z_1h14m59s_15w` | `15` | `OOS -> OOS` | `-$0.4000` | `-$0.4000` | `$0.0000` | `$20.40` | `$20.40` | `$0.0000` | `$0.0000` | `$0.4000` | `$0.4000` | `$0.0000` | `$0.0000` | `$0 / $0` | `$0.4000` | `$0.4000` |
| `19` | `25-11_10-35-01Z_1h14m59s_15w` | `15` | `OOS -> OOS` | `+$1.7776` | `+$1.7776` | `$0.0000` | `$70.38` | `$70.38` | `$3.4774` | `$3.4774` | `$1.0278` | `$1.0278` | `-$0.6720` | `-$0.6720` | `$0 / $0` | `$1.2778` | `$1.2778` |
| **`20`** | **`25-11_12-50-01Z_1h14m58s_15w`** | `15` | `OOS -> OOS` | **`+$21.2443`** | **`+$10.7456`** | **`-$10.4987`** | `$128.98` | `$80.06` | `$23.5992` | `$12.4728` | `$0.9179` | `$0.2986` | `-$1.4286` | `-$1.4286` | `$0.0084 / $0` | `$0.8033` | `$0.8033` |
| **`21`** | **`26-11_00-50-00Z_1h14m59s_15w`** | `15` | `N/A -> OOS` | `N/A` | **`-$12.2443`** | **`-$12.2443`** | `N/A` | `$114.13` | `N/A` | `$4.5834` | `N/A` | `$0.7669` | `N/A` | **`-$16.0608`** | `N/A / $0` | `N/A` | **`$12.2443`** |
| **Σ** | **Total (`226w -> 241w`)** | `226/241` | `158/68 -> 168/73` | **`+$58.7865`** | **`+$33.2864`** | **`-$25.5001`** | `$1010.04` | `$1077.85` | `$96.7065` | `$85.8059` | `$20.4162` | `$20.5644` | `-$19.1010` | `-$33.5607` | `$0.0084 / $0` | **`$9.7100`** | **`$12.2443`** |

*(Note: `Cap #6` also carries `+$1.6056` `unrealized_pnl_micro` in both runs from its incomplete trailing window, and `Cap #20` paid `$0.00838` flatten fee in S1.)*

#### 7.2 Exact Window-Level Proof of the `4` Changed Shared Captures (`5cf74b9` Timing Fix)
Every changed window across the `20` shared captures had its first diverging fill inside `14.000s–14.999s` or `119.000s–119.999s`:

1. **`Cap #3` (`22-11_15-35-00Z`, `btc-updown-5m-1795362900`):** S1 blocked `t = 119.411s` (`tau_secs = 180 -> elapsed_secs = 120 >= 120`) and recorded `$0.00`. S2 allowed `Up 20.00sh @ $0.24` at `t = 119.411s`; only `0.02sh` paired at `$0.79` (`-$0.0006` merge) and `19.98sh` Up expired worthless (`-$4.7952`), totaling **`-$4.7958`**.
2. **`Cap #11` (`24-11_10-15-00Z`, `1795517100` & `1795517700`):**
   - `1795517100`: S1 opened early at `t = 14.595s` (`Down 20sh @ $0.55`), cascading into `5` fills for `+$4.3100`. S2 blocked `t = 14.595s`, did not fill until `t = 115.937s` (`Up 20sh @ $0.20`), and expired **`-$4.0000`** (`-$8.3100` swing).
   - `1795517700`: S1 opened early at `t = 14.866s` (`Down 20sh @ $0.48`) right before an Up sweep, losing **`-$9.6000`**. S2 blocked `t = 14.866s` and stayed flat (`$0.0000`, `+$9.6000` swing). Net `Cap #11` delta: **`+$1.2900`**.
3. **`Cap #14` (`24-11_15-30-00Z`, `1795535400`):** S1 blocked `t = 119.537s` (`$0.00`). S2 filled `Down 20.00sh @ $0.23` at `t = 119.537s` and hedged `19.81sh` Up @ `$0.73` for **`+$0.7487`**.
4. **`Cap #20` (`25-11_12-50-01Z`, `1795613100` — OOS):** S1 opened early at `t = 14.885s` (`Up 2.17sh @ $0.60`), triggering `19` fills (`64.42` merged pairs) and a mandatory flatten `16.92sh` Down @ `$0.01` (`+$9.3060` merge gain, `$0.00838` fee) for **`+$10.94872`**. S2 blocked `t = 14.885s`, entered at `t = 66.374s` (`Up 5sh @ $0.56`), hedged at `t = 92.850s` (`Down 5sh @ $0.35`), and finished **`+$0.45000`** (**`-$10.49872` OOS loss**). *(Window `1795614300` also differed by `1` micro-dollar in `estimated_rebate_micro`, `$0.012695` vs. `$0.012696`, with identical `+$4.3440` Net0.)*

#### 7.3 Window-Level Anatomy of Capture `#21` (`26-11_00-50-00Z_1h14m59s_15w`)
All fills in `Cap #21` occurred cleanly inside `[35.966s, 118.693s]` (`30` fills, `19` merges, `0` flattens). Its `-$12.2443` loss came from unhedged settlement (`-$16.0608`):
- **`w#13` (`btc-updown-5m-1795657800`, `-$9.0000`):** Filled `Down 20.00sh @ $0.45` at `t = 112.986s`. During `MandatoryFlatten` (`T-030s..T-015s`), the book sat at `Bk 99/-----/1` (`0` opposing ask/bid liquidity), preventing flatten execution (`Mandatory flatten liquidity unavailable`), so `20sh` Down settled at `$0` when `Up` won (`-$9.0000`).
- **`w#2` (`1795654500`, `-$2.8547`):** `-$3.0240` settlement + `+$0.2862` positive merges `- $0.1169` negative merges (`19.07sh` vs `10.82sh` partial fills).
- **`w#9` (`1795656600`, `-$1.2236`):** `-$2.1168` settlement + `+$0.8932` positive merges.
- **`w#12` (`1795657500`, `+$0.8840`):** `+$2.8040` positive merges `- $1.9200` settlement (`26sh` Down vs `23sh` Up).
- **`w#1, w#7, w#8, w#10`:** Balanced merges totaling `+$0.6000` (`w#7`) and `-$0.6500` negative merges (`-$0.4000`, `-$0.2000`, `-$0.0500`). Remaining `7` windows (`w#3–6, w#11, w#14`, plus incomplete `w#15`) had `0` fills (`$0.0000`).

---

### 8. Independent G3 & G5 Recalculations *(Observed & Derived)*

#### 8.1 G3 Retention Formula Verification
`Get-SurvivalInputs` (`scripts/sweep-params.ps1#L506-L516`) defines G3 as:
$$\text{Retention} = \frac{\text{OOS Net} / N_{\text{OOS}}}{\text{IS Net} / N_{\text{IS}}} \times 100\%, \quad N_{\text{IS}} = \lfloor 0.7 N \rfloor, \quad N_{\text{OOS}} = N - N_{\text{IS}}$$

| Input / Output | S1 (`20` caps, `0%`) | S2 (`21` caps, `0%`) | S2 (`21` caps, `3%`) |
|---|---:|---:|---:|
| Total Windows (`N`) | `226` | `241` | `241` |
| IS Window Count (`N_IS`) | `158` | `168` | `168` |
| OOS Window Count (`N_OOS`) | `68` | `73` | `73` |
| IS Performance (`IS Net`) | `+$34.600100` | `+$31.680700` | `+$32.210102` |
| OOS Performance (`OOS Net`) | `+$24.186420` | `+$1.605700` | `+$1.860618` |
| IS Normalized (`IS Net / N_IS`) | `+$0.21898797 / w` | `+$0.18857560 / w` | `+$0.19172680 / w` |
| OOS Normalized (`OOS Net / N_OOS`) | `+$0.35568265 / w` | `+$0.02199589 / w` | `+$0.02548792 / w` |
| Raw `OOS / IS` Ratio (`OOS Net / IS Net`) | `69.9027%` | `5.0684%` | `5.7765%` |
| Window Normalization Multiplier (`N_IS / N_OOS`) | `2.323529x` | `2.301370x` | `2.301370x` |
| **Unrounded Retention** | **`162.4211%`** | **`11.6642%`** | **`13.2939%`** |
| **Final Rounded Retention (`summary.txt`)** | **`162%`** (`PASS`) | **`12%`** (`FAIL < 50%`) | **`13%`** (`FAIL < 50%`) |

- **Why `12%` differs from `~5.1%`:** `5.0684%` is the **raw** P&L ratio (`$1.6057 / $31.6807`). Because OOS covers `73` windows vs. `168` in IS, dividing by window count scales the raw ratio by `168 / 73 = 2.301370`, yielding `5.0684% × 2.301370 = 11.6642%` (`12%` rounded). Both S1 and S2 used the exact same formula and chronological split rule.

#### 8.2 G5 Drag & TailDrag Formula Verification
`Get-SurvivalInputs` (`scripts/sweep-params.ps1#L550-L568`) defines:
$$\text{SettleLoss} = \max\!\big(0,\; -(\text{Settle} + \min(\text{Unreal}, 0))\big), \quad \text{Drag} = \frac{\text{SettleLoss}}{\text{MergePos}} \times 100\%, \quad \text{TailDrag} = \frac{\text{SettleLoss} + \text{MergeNegLoss}}{\text{MergePos}} \times 100\%$$

| Term | S1 (`20` caps) | S2 (`21` caps) | Delta (`S2 - S1`) |
|---|---:|---:|---:|
| `settlement_pnl_micro` | `-$19.101000` | `-$33.560700` | `-$14.459700` |
| `unrealized_pnl_micro` (clamped at `<= 0`) | `+$1.605600` (`0`) | `+$1.605600` (`0`) | `$0.000000` |
| **Drag Numerator (`SettleLoss`)** | **`$19.101000`** | **`$33.560700`** | **`+$14.459700`** (`+75.7%`) |
| `negative_merge_loss_micro` | `$20.416200` | `$20.564400` | `+$0.148200` (`+0.7%`) |
| **TailDrag Numerator (`SettleLoss + MergeNegLoss`)** | **`$39.517200`** | **`$54.125100`** | **`+$14.607900`** (`+37.0%`) |
| **Common Denominator (`positive_merge_gain_micro`)** | **`$96.706500`** | **`$85.805900`** | **`-$10.900600`** (`-11.3%`) |
| **Exact `Drag%` (rounded / report)** | **`19.7515%`** (`20%` / `19.8%`) | **`39.1123%`** (`39%`) | **`+19.3608` pp** |
| **Exact `TailDrag%` (rounded / report)** | **`40.8630%`** (`41%` / `40.9%`) | **`63.0785%`** (`63%`) | **`+22.2155` pp** |

- **Primary driver:** G5 failure is driven primarily by **unhedged settlement losses (`+$14.4597`, from `Cap #21`'s `-$16.0608`)** compounded by the **`-$10.9006` shrinkage in `MergePos`** from the `5cf74b9` binary change (`-$15.4840` on the shared 20 captures, partially offset by `Cap #21`'s `+$4.5834`). Negative merge losses barely changed (`+$0.1482`; `Cap #14` remains `39.9%` of total negative merges at `$8.2008`, while `Cap #21` added only `$0.7669`).

---

### 9. Controlled Replay Verification (Exact S2 Binary: 20 Caps vs. 21 Caps) *(Observed)*

To isolate the `5cf74b9` binary difference from `Cap #21`, both capture sets were replayed cleanly using the exact S2 binary (`target/release/poly-as.exe`, `-SkipBuild -BinPath`) and exact S2 config (`-QueueDecay conservative`, promoted `DEFAULTS.json`):

| Run | Caps / Win | Deployed | `Net0` | `IS Net0` | `OOS Net0` | `Retention` | `MaxDD` | `Drag / Tail` | Gates (`G1–G5`) |
|---|---|---:|---:|---:|---:|---:|---:|---|---|
| **S1 (`25-11_21-55-50Z`, pre-fix bin)** | `20 / 226w` | `$1010.04` | `+$58.79` | `+$34.60` | `+$24.19` | `162%` | `19%` | `20% / 41%` | All `G1–G5` PASS (`G6 100%`) |
| **Controlled 20-Cap (S2 bin)** | `20 / 226w` | `$963.71` | `+$45.53` | `+$31.84` | `+$13.69` | `100%` (`99.88%`) | `11%` (`10.72%`) | `22% / 46%` (`21.55%/45.92%`) | **All `G1–G5` PASS** (`G6 n/a`) |
| **Controlled 21-Cap (S2 bin)** | `21 / 241w` | `$1077.85` | `+$33.29` | `+$31.68` | `+$1.61` | `12%` (`11.66%`) | `24%` (`24.49%`) | `39% / 63%` (`39.11%/63.08%`) | **`FAIL(G3)` + `FAIL(G5)`** (`G6 n/a`) |
| **Latest S2 (`26-11_07-55-07Z`)** | `21 / 241w` | `$1077.85` | `+$33.29` | `+$31.68` | `+$1.61` | `12%` | `24%` | `39% / 63%` | **`FAIL(G3)` + `FAIL(G5)`** (`G6 n/a`) |

- **Micro-dollar determinism confirmed:** Comparing all `17` `summary` accounting/telemetry fields (`ticks_evaluated`, `fills_count`, `merges_count`, `volume_in_micro`, `positive_merge_gain_micro`, `negative_merge_loss_micro`, `settlement_pnl_micro`, `net_pnl_micro`, etc.) yielded **`0` discrepancies** between `controlled_20caps_run2` and the first `20` captures of `S2`, and **`0` discrepancies** between `controlled_21caps_run2` and `S2`.
- **Conclusion:** On the corrected `5cf74b9` binary, the original `20`-capture dataset is reproducible at `+$45.53` (`100%` retention, `22%/46%` drag/tail, `11%` MaxDD) and **still passes G1–G5**. Adding `Cap #21` genuinely pushes the combined `21`-capture sample into **both G3 failure (`12%`) and G5 failure (`39%/63%`)**, exposed because `5cf74b9` had already removed `S1`'s `-$10.50` sub-second timing-bug profit in `Cap #20`.
