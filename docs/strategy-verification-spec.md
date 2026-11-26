# Strategy Verification Specification: Testing & Validation Framework

> **Reference Material:**  
> This specification adapts the 5-stage trading verification methodology presented in [Claude Tested 10,000 Trading Strategies (Only These Won)](http://www.youtube.com/watch?v=hKstXh-zT7c) for high-frequency binary market-making on Polymarket in `polymarket-as-rust`.
>
> **Status Tags:** Items marked `[Implemented]` reflect live capabilities in this repository. Items marked `[Planned]` denote verification gates or sweep extensions scheduled for implementation.

---

## 1. Executive Context & Scope

The `polymarket-as-rust` codebase is engineered as a zero-allocation, high-frequency binary market maker for Polymarket's 5-minute (`300s`) Bitcoin Up/Down series. Unlike traditional spot swing trading, market-making edge stems from:

1. **Pair-Merge Spread Capture & Maker Rebates** minus **Adverse Selection** and **Unhedged Expiry Settlement Drag**.
2. **Dynamic Fair-Value Skewing & Risk Aversion Tuning** ($\gamma$, dynamic $\kappa$).
3. **Queue Priority & Microsecond-Level Latency Control**.
4. **Binary Expiry Settlement Risk Management** ($0$ or $1$ payout bounds at window close, enforced via 4-phase lifecycle cutoffs).

Any AI agent modifying or extending trading strategies, pricing math, risk filters, or execution loops in this repository **must validate changes** using the 5-stage framework below.

---

## 2. Stage 1: Pre-Test Code Verification & Simulation Sanity

Before running backtests or parameter sweeps, agents must enforce baseline invariants to prevent lookahead bias, silent `NaN` propagation, and simulation drift.

### 1.1 Invariant Rules & Peeking Checks
* **Discrete Monotonic Clock & Replay Determinism `[Implemented]`:** All timing (`MonotonicClock` in `crates/core-types/src/clock.rs`), math engines (`crates/math-engine`), and simulation logic (`crates/paper-sim`) must process ticks strictly in chronological order. Replay code must never inspect future event frames or post-resolution settlement outcomes.
* **Calibration Sync Surface `[Implemented]`:** Strategy defaults and thresholds in `crates/core-types/src/config.rs` (`EngineConfig`) must stay synchronized across all five calibration files (`config.rs`, `crates/core-types/src/lib.rs`, `crates/cli/src/main.rs`, `scripts/sweep-params.ps1`, and `.env.example`). Post-hoc threshold shifting based on sweep outputs is prohibited.

### 1.2 Synthetic & Adversarial Determinism `[Implemented]`
* Execute deterministic replay and edge-case tests via `crates/paper-sim/tests/adversarial.rs` and `crates/paper-sim/tests/vectors.rs`.
* Verify zero-allocation math invariants under extreme input ticks via `crates/math-engine/tests/zero_alloc.rs`.
* Confirm `SavitzkyGolayFilter` (`crates/math-engine/src/station1_sg.rs`) uses the fixed 5-point stack-allocated `[f64; 5]` denoiser (`CAUSAL_WEIGHTS = [3, -5, -3, 9, 31] / 35`) to produce denoised mid-price `s_i` without leaking future ticks (`s_i` only; Station 1 does not compute velocity or acceleration).

### 1.3 Realistic Microstructure Friction
Simulation runs in `crates/paper-sim` must model real-world Polymarket execution mechanics:
* **Zero Gas on Relayer Merges `[Implemented]`:** Polymarket Relayer v2 merges are gasless (`gasless_relayer_enabled: true` in `EngineConfig`); do not add synthetic merge gas fees.
* **Fee & Rebate Schedule `[Implemented]`:** Apply the dynamic taker fee schedule (`feeRate = 0.05` post-Nov 20, 2026) on aggressive flatten orders and `0.00%` maker fee + estimated maker rebates on passive fills.
* **Pessimistic FIFO Queue & Wire Latency `[Implemented]`:** `FifoPaperBroker` (`crates/paper-sim/src/fifo_book.rs`) models `40ms` default order/cancel wire latency and keeps `PendingCancel` orders fillable while in flight. Promotion runs **must** pass `-QueueDecay conservative` (`QueueDecayPolicy::Conservative`) in `scripts/sweep-params.ps1` (note: `sweep-params.ps1` currently defaults to `optimistic` for fast exploratory runs).
* **Latency-Jitter Stress Testing `[Planned]`:** Add seeded wire latency jitter (`40ms–250ms` / `50ms–300ms` WS/REST delay) in `fifo_book.rs` to verify edge durability under network spikes (Twin 2).

---

## 3. Stage 2: Multi-Variable & Out-of-Sample Testing (300s Windows)

Standard multi-year daily backtesting is replaced with 300s window-chunked, multi-regime dataset splitting across distinct UTC sessions (Asia, London, US).

```text
+-------------------------------------------------------------------------+
|                  HISTORICAL L2 & BINANCE REPLAY DATA                    |
+------------------------------------+------------------------------------+
|   70% IN-SAMPLE (Training / Tuning)  | 30% OUT-OF-SAMPLE (Hidden Holdout) |
|   - Parameter Tuning via Sweep     | - Frozen Parameter Evaluation      |
|   - Avellaneda-Stoikov Calibration | - Performance & Risk Gate Check    |
+------------------------------------+------------------------------------+
```

### 2.1 Multi-Dimension Parameter Sweep Matrix
Agents use `scripts/sweep-params.ps1` (driving `poly-as lifecycle --source replay`) across real `EngineConfig` parameters:

| Axis | Parameters & Units | Target Module / Status |
| :--- | :--- | :--- |
| **Risk Aversion ($\gamma$)** | `[0.02, 0.05, 0.08, 0.12, 0.15, 0.25]` (`USD^-1`) | `crates/math-engine/src/station4_6_pricer.rs` (`-Gammas`) `[Implemented]` |
| **Spread & Pair-Cost Bounds** | `include_variance_spread` (`false`/`true`), `min_spread_ticks` (`1`, `2`), `max_combined_bid_micro` (`980000`, `990000`) | `crates/math-engine/src/station4_6_pricer.rs` `[Implemented]` |
| **Entry Guards** | `max_entry_price` (`0.65` promoted; `off` disables), `max_entry_prob` (`0.80`, `off`), `max_cross_tick_loss_micro` (`80000`) | `crates/math-engine/src/station4_6_pricer.rs` `[Implemented]` |
| **Entry-Price Cap Boundary** | `max_entry_price` ∈ `{0.60, 0.65, 0.70}` — the promotion `3×3` grid | `crates/math-engine/src/station4_6_pricer.rs`, env `MAX_ENTRY_PRICE` `[Implemented]` |
| **Entry-Cutoff Boundary** | `entry_cutoff_secs` ∈ `{90, 105, 120}` — the promotion `3×3` grid | `crates/risk-lifecycle/src/lifecycle.rs`, env `ENTRY_CUTOFF_SECS` `[Implemented]` |
| **Entry Timing Window** | `entry_open_delay_secs` (`15`; `0` disables the opening skip), `entry_cutoff_secs` (`120`; `off` disables the cutoff) | `crates/risk-lifecycle/src/lifecycle.rs` (`LifecycleConfig`), wired from `EngineConfig` by `crates/cli/src/lifecycle_cmd.rs` `[Implemented]` |
| **Binance Lead-Lag Cancel** | `lead_lag_cancel_threshold` (`0.015`), `lead_lag_window_ms` (`1000`), `lead_lag_cooldown_ms` (`1000`), `lead_lag_directional` (`true`) | `crates/math-engine/src/station2_twap.rs` `[Implemented]` |
| **Hedge Skew & Tier-1 Cap** | `hedge_skew_mult` (`0.0..1.0`), `cap_tier1_reducing_to_delta` (`false`/`true`) | `crates/risk-lifecycle/src/station7_risk.rs` `[Implemented]` |
| **Hysteresis Deadband** | `hysteresis_tick_buffer` in **ticks** (e.g. `[0.10, 0.25, 0.50]` ticks; `$0.01` = `100` bps) | `crates/risk-lifecycle/src/station8_hysteresis.rs` `[Planned for sweep CLI]` |
| **Expiry Lifecycle Cutoffs** | `pre_expiry_lockout_secs` (`60s`), `mandatory_flatten_secs` (`30s`), `terminal_lockout_secs` (`15s`) | `crates/risk-lifecycle/src/lifecycle.rs` (`LifecycleConfig`) `[Planned for sweep CLI]` |

> **Promotion grid (boundary sweep):** promotion is decided on a full `3×3` grid — `MaxEntryPrice ∈ {0.60, 0.65, 0.70}` × `EntryCutoff ∈ {90s, 105s, 120s}` at `Tier1HedgeTrigger = off` — run over the same `20` live captures (`226` windows, `158` IS / `68` OOS), i.e. `9` configurations × `20` captures = `180` replays. This grid, not a single-axis sweep, produced the promoted `#6/9` (`$0.65 / 120s`); the ranking, scoring and `G1–G6` verdicts are in [boundary-sweep-selection-report.md](boundary-sweep-selection-report.md).

> **Design Exclusions & Module Note:**
> * **No static $\kappa$ sweep (`0.5..10.0`):** `crates/math-engine/src/station3_kappa.rs` estimates $\kappa$ dynamically from live L2 depth and clamps it to `[15.0, 200.0]` (fallback `50.0`); values in `0.5..10.0` blow up quoted spreads beyond `[0, 1]`.
> * **No variable SG window $N$:** `station1_sg.rs` uses a fixed 5-point buffer (`[f64; 5]`) for zero-allocation hot-path determinism.
> * **`window_gate.rs` scope:** `crates/cli/src/window_gate.rs` only aligns CLI session startup to fresh 300s UTC boundaries; intra-window expiry cutoffs live in `crates/risk-lifecycle/src/lifecycle.rs`.

### 2.2 Rolling Walk-Forward Splitting `[Planned in sweep-params.ps1]`
* **Multi-Regime Partitioning:** Collect chunked captures across multiple UTC regimes (Asia/London/US sessions; low-vol chop, trend breakouts, news spikes).
* **70 / 30 Window Split:** Split 300-second windows sequentially:
  * First **70%** of windows: In-Sample tuning for $\gamma$, entry guards, lead-lag thresholds, and expiry cutoffs.
  * Final **30%** of windows: Out-of-Sample holdout. Promotion scoring evaluates frozen parameters on this hidden 30% segment.

---

## 4. Stage 3: The "Random Twin" Luck Benchmark

To prove PnL is structural spread-capture edge rather than favorable drift or binary settlement luck, strategies must beat two benchmark twins inside `crates/paper-sim`.

### 3.1 Twin 1: The Symmetric / Naive Quoting Twin `[Implemented]`
* **Purpose:** Isolates whether Stations 1–6 (SG denoising, $\kappa$, A-S spread/skew, lead-lag cancel) generate alpha over passive quoting.
* **Mechanism:** `NaiveBaselineEngine` (`crates/paper-sim/src/telemetry.rs`) runs in parallel on the same feed and `FifoBrokerConfig`, quoting `mid - 1 tick` passively on both legs without inventory skew or lead-lag defensive cancels.
* **Requirement:** The candidate strategy's `Net+Reb` and `AlphaEdge` (`Strategy Net - Baseline Net`) must outperform `NaiveBaselineEngine` with paired bootstrap / one-sided `p < 0.05` (`t >= 1.65`) across multi-session windows.

### 3.2 Twin 2: The Jittered-Timing Twin `[Planned]`
* **Purpose:** Tests whether edge survives WebSocket jitter and REST roundtrip spikes.
* **Mechanism:** Uses identical pricing math while injecting seeded randomized order/cancel latency between `40ms` and `250ms` (up to `300ms` stress) in `crates/paper-sim/src/fifo_book.rs`.
* **Requirement:** Performance decay under jitter must remain bounded; total `Net+Reb` must stay positive.

---

## 5. Stage 4: The Six Filtering Checks (Market-Making Edition)

Candidate configurations generated from `scripts/sweep-params.ps1` must pass all six survival gates before promotion.

```text
       [ Candidate Strategy Sweep Matrix ]
                       │
                       ▼
┌──────────────────────────────────────────────────────────┐
│  Filter 1: Zero-Edge & Conservative Queue                │  --> Fails if Net+Reb <= 0 or AlphaEdge <= 0
└──────────────────────┬───────────────────────────────────┘
                       │ Pass
                       ▼
┌──────────────────────────────────────────────────────────┐
│  Filter 2: Expiry Drag, WinCap & Drawdown Cap            │  --> Fails if MaxDD > 25% or Drag > 30% or TailDrag > 50%
└──────────────────────┬───────────────────────────────────┘
                       │ Pass
                       ▼
┌──────────────────────────────────────────────────────────┐
│  Filter 3: Out-of-Sample Consistency (>= 50%)            │  --> Fails if OOS return/win < 50% of IS
└──────────────────────┬───────────────────────────────────┘
                       │ Pass
                       ▼
┌──────────────────────────────────────────────────────────┐
│  Filter 4: Minimum Sample Size (150-200+ Windows)        │  --> Fails if underpowered (<150w / <400 fills)
└──────────────────────┬───────────────────────────────────┘
                       │ Pass
                       ▼
┌──────────────────────────────────────────────────────────┐
│  Filter 5: Parameter Stability Plateau                   │  --> Fails if +/-1 neighbor cells collapse
└──────────────────────┬───────────────────────────────────┘
                       │ Pass
                       ▼
┌──────────────────────────────────────────────────────────┐
│  Filter 6: Adverse Selection Markout (MO1s > 0)          │  --> Fails if MO1s <= 0 or net spread negative
└──────────────────────┬───────────────────────────────────┘
                       │ Pass
                       ▼
       [ Promoted Candidate Strategy ]
```

### 5.1 Detailed Filter Criteria

1. **Zero-Edge & Conservative Queue Baseline:**
   * Under `-QueueDecay conservative`, `Net+Reb` (Net PnL + Maker Rebates) must strictly exceed `$0`, and `AlphaEdge` over `NaiveBaselineEngine` must exceed `$0`.
2. **Binary Expiry Drag, `V_max` & Drawdown Cap:**
   * Maximum Drawdown across a session must stay below **25%** of allocated working capital.
   * Unhedged notional and final-minute risk must stay strictly within `V_max` (`max_notional_per_leg_micro`) and `LifecycleConfig` bounds enforced by `crates/risk-lifecycle/src/station7_risk.rs`, `lifecycle.rs`, and `breakers.rs`.
   * **G5a — unhedged expiry drag `<= 30%`:** net unhedged settlement losses must erase at most **30%** of gross positive pair-merge gains (`Drag = unhedged settlement loss / positive merge gains`). This is the rule the boundary sweep actually applied: Config `#1/9` is ineligible at `33.1%` and `#2/9` at `36.0%` ([boundary-sweep-selection-report.md](boundary-sweep-selection-report.md) §3.4). The drag leak itself was first measured in `docs/post-run-analysis/24-11_58w/REPORT.md`.
   * **G5b — tail drag `<= 50%` (separate rule):** once negative merge losses are added to the settlement loss (`TailDrag`), at most **50%** of gross positive pair-merge gains may be erased. This is what disqualifies `#4/9` (`50.04%`) and `#5/9` (`51.0%`). `TailDrag` must be measured: an unknown value reports `G5 = N/A` and can never read as `PASS`.
3. **Out-of-Sample Consistency ($\ge 50\%$ Retention) `[Planned gate]`:**
   * The hidden 30% out-of-sample window set must retain at least **50%** of the `Net+Reb` per window achieved during the 70% in-sample run.
4. **Minimum Sample Size:**
   * Target **150–200+ completed 300s windows** across multiple UTC regimes (`~400–500` filled maker orders at observed `~2.5` fills/window), providing statistical power (`t >= 1.65`, one-sided `p < 0.05`) across both `Up` and `Down` outcomes.
5. **Parameter Stability (Plateau Neighborhood):**
   * Performance must not depend on isolated spikes. Immediate neighbor cells in `scripts/sweep-params.ps1` ($\pm 1$ step in $\gamma$, entry price/prob caps, or lead-lag thresholds) must remain profitable.
6. **Adverse-Selection Markout Stress Margin:**
   * Evaluates post-fill toxicity via `FillMarkoutProbe` (`crates/paper-sim/src/telemetry.rs`). `MO1s` must remain strictly positive (`MO1s > 0`) alongside positive net merge spread. Monitor `MO5s` as a toxicity diagnostic rather than a hard rejection filter (in binary pair-merge MM, positive `MO1s` + strong pair-merge spread remains profitable even when `MO5s` is slightly negative).

---

## 6. Stage 5: Plateau Selection & Promotion Pipeline

### 5.1 Parameter-Plateau Selection & Regime Discipline
* **Plateau Center Selection (No Multi-Order Laddering):** Rather than picking an extreme outlier row from `scripts/sweep-params.ps1`, select the **centroid configuration** of a contiguous 3–5 cell profitable plateau. Do **not** ladder multiple simultaneous parameter configs per leg inside `pipeline.rs`: `crates/risk-lifecycle/src/station8_hysteresis.rs` maintains a single resting order slot per leg (`Up` / `Down`) to protect CLOB rate limits and prevent self-crossing under `$50/leg` sizing.
* **Regime & Lifecycle Gating:**
  * *Microstructure Toxicity / Spot Jumps:* `station2_twap.rs` triggers fast-path directional defensive cancels (`lead_lag_cancel_threshold`) to pull endangered resting bids before informed takers sweep them.
  * *Expiry Convergence:* `crates/risk-lifecycle/src/lifecycle.rs` transitions through `ActiveQuoting` (`>60s`), `PreExpiryLockout` (`30–60s`), `MandatoryFlatten` (`15–30s`), and `TerminalLockout` (`<=15s`) to cap unhedged settlement exposure.

### 5.2 3-Tier Production Promotion Workflow `[Implemented]`

```text
┌────────────────────────────────────────────────────────────────────────┐
│ TIER 1: Offline Replay & Parameter Sweep                               │
│ - Run `poly-as lifecycle --source replay` via `scripts/sweep-params.ps1`│
│ - Require `-QueueDecay conservative` and Six Filtering Checks          │
└──────────────────────────────────┬─────────────────────────────────────┘
                                   │
                                   ▼
┌────────────────────────────────────────────────────────────────────────┐
│ TIER 2: NEX Sandbox Live Validation                                    │
│ - Run `poly-as sandbox health|markets|book|validate|feeds`             │
│   (`crates/execution/src/nex.rs` & `crates/cli/src/sandbox_cmd.rs`)    │
│ - Verify EIP-712 signing, L2 HMAC, heartbeat timers, and order FSM     │
└──────────────────────────────────┬─────────────────────────────────────┘
                                   │
                                   ▼
┌────────────────────────────────────────────────────────────────────────┐
│ TIER 3: Live Canary Deployment                                         │
│ - Run `poly-as exec --mode canary-live --confirm-live-capital`         │
│   with `--canary-max-usd <= 25`                                        │
│ - Compare real MO1s / MO5s and unhedged expiry drag vs paper-sim       │
└────────────────────────────────────────────────────────────────────────┘
```

---

## 7. Mandatory AI Agent Checklist

Before opening a Pull Request that alters trading math, risk filters, or execution logic:

- [ ] Code preserves lookahead-free replay determinism and updates all 5 files in the Calibration Sync Surface if defaults change (`AGENTS.md` §7).
- [ ] Workspace gates pass cleanly:
  * `rtk cargo fmt --all -- --check`
  * `rtk cargo clippy --workspace --all-targets -- -D warnings`
  * `rtk cargo test --workspace` (including `crates/paper-sim/tests/adversarial.rs`, `vectors.rs`, and `crates/math-engine/tests/zero_alloc.rs`).
- [ ] Offline sweep verified with `scripts/sweep-params.ps1 -QueueDecay conservative` (and 70/30 OOS split once enabled).
- [ ] Candidate strategy outperforms `NaiveBaselineEngine` (`AlphaEdge > 0`) and passes the **Six Filtering Checks** (Zero-Edge + Conservative Queue, Drawdown/WinCap/Expiry Drag, OOS Retention, Sample Size, Plateau Stability, `MO1s > 0`).
- [ ] For execution or lifecycle changes, validated order lifecycle against NEX Sandbox:
  * `rtk cargo run -p cli --bin poly-as -- sandbox health`
  * `rtk cargo run -p cli --bin poly-as -- sandbox validate --asset-id <id> --confirm-sandbox-trade`
