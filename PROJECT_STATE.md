# Project State

- **Last verified (repository state on `main`):** `2026-11-27T18:06+02:00` (`2026-11-27 16:06 UTC`) at commit `45fc9e3`
- **Operational audit snapshot date:** `2026-11-27` (point-in-time findings for active processes, live recordings, queued sessions, and local data directories)
- **Status Volatility Notice:** Repository commit facts below were verified directly against `main`. Process, recording, session-queue, and unmerged worktree observations reflect a point-in-time operational audit and can change while background jobs run. Active captures, worktrees, and sealed Holdout files were intentionally not probed or opened when writing this document.

> **Epistemic Labels:** Entries distinguish **[Verified Fact]** (checked against tracked repository state), **[Audit Snapshot]** (point-in-time operational observation from `2026-11-27`), **[Hypothesis]** (unverified candidate claim under test), and **[Recommendation]** (prioritized next action).

---

## 1. Mission and Research Principles

**[Verified Fact]** This project builds and validates a pure-Rust, terminal-first Avellaneda-Stoikov dual-bid market maker (`poly-as`) for Polymarket cryptocurrency prediction markets, starting with 5-minute BTC UP/DOWN binary epochs.

The primary goal is sustainable, repeatable net profitability that survives realistic queue priority, wire latency, fees, adverse selection, and terminal settlement exposure. Profitability is an objective to prove, never an assumption.

Key operating rules (governed in full by [AGENTS.md](AGENTS.md), [CONSTRAINTS.md](CONSTRAINTS.md), and [docs/strategy-verification-spec.md](docs/strategy-verification-spec.md)):

- **Strict dataset separation:** Keep Development (In-Sample), Candidate Selection, Holdout, Forward Testing, and Live Trading strictly separate. Never reuse inspected data as an untouched Out-of-Sample gate.
- **Frozen experimental discipline:** Freeze engine code, defaults, and candidate flags before forward or holdout evaluation. Never alter a frozen experiment mid-run or relax failure thresholds (`Filter 4` requires `>= 400` fills under conservative queue decay).
- **Capture and worktree protection:** Never terminate an active `poly-as.exe` process, modify its logs or manifests, or run `cargo build`/`cargo clean` inside a worktree and build profile whose binary is active.
- **Live capital boundary:** Mainnet `canary-live` (`poly-as exec --mode canary-live`) is hard-gated off in the current build ([crates/cli/src/exec_cmd.rs](crates/cli/src/exec_cmd.rs)). User authorization does not bypass that implementation hard gate, and historical Phase 5 documentation in [IMPLEMENTATION_PLAN.md](IMPLEMENTATION_PLAN.md) is not authorization to attempt live execution.

---

## 2. Main Repository State

### Verified Permanent / Tracked State (`main`)

- **`main` Baseline Commit [Verified Fact]:** `45fc9e3` (`Merge pull request #82 from AI-Degen-69/docs/documentation-sync`), synced with `origin/main`.
- **Recent Merged Milestones [Verified Fact]:**
  - **PR #80 (`3a13a23`):** Recorded merge-edge retention feasibility, mandatory-flatten root-cause (`0` flattens across `40` exposed windows), and **NO-GO** passive reducing-hedge override replay findings.
  - **PR #81 (`929a2f4`):** Corrected real flatten test coverage counts and settlement formula documentation.
  - **PR #82 (`45fc9e3`):** Synchronized `IMPLEMENTATION_PLAN.md`, `README.md`, `docs/README.md`, `docs/poly-as-cheatsheet.md`, governance rules, and marked historical Phase 5 live-canary notes as non-executable.
- **Core Architecture [Verified Fact]:** Phases `0`–`5` (`core-types`, `feeds`, `math-engine`, `paper-sim`, `risk-lifecycle`, `execution`, `cli`), `historical-data` (`poly-as pmdata`), and the NEX Amoy sandbox (`poly-as sandbox`) are implemented on `main` (see [IMPLEMENTATION_PLAN.md](IMPLEMENTATION_PLAN.md) and [docs/README.md](docs/README.md)).

### Point-in-Time Checkout and Worktree Layout (`2026-11-27`)

| Location / Ref | Branch | Commit | Point-in-Time Status | Protection Rule |
| :--- | :--- | :--- | :--- | :--- |
| Primary workspace (`polymarket-as-rust`) | `main` (prior to PR branch) | `45fc9e3` | **[Verified Fact]** Clean working tree | Standard short-lived PR workflow (`AGENTS.md` §8) |
| Parallel worktree (`entry_selection_adverse_trend`) | `entry_selection_adverse_trend` | `3870cbb` | **[Audit Snapshot]** Active forward-testing capture running | **Protected — do not touch, build, or clean** |
| Local branch ref (no worktree checked out) | `validate_strategy_edge_metric` | Historical local ref | **[Verified Fact]** Inactive local branch | Leave undisturbed until explicit branch cleanup |

---

## 3. Strategy and Evaluation

### Provisional Baseline: Config #6 (`DEFAULTS.json`)

**[Verified Fact]** Repository root [DEFAULTS.json](DEFAULTS.json) defines `Config #6/9` as the **provisional research baseline** (selected in [docs/boundary-sweep-selection-report.md](docs/boundary-sweep-selection-report.md)). It is **not** a production-validated strategy:

- **Mode & safety:** `execution_mode = "Paper"`, `dry_run = true`, `queue_decay = "conservative"`, `40ms` simulated FIFO wire latency.
- **Sizing & spread:** `gamma = 0.12`, `V_max = $50/leg` (`50_000_000` micro-USDC), `base_order_size_shares = 20sh`, `min_spread_ticks = 1`, `max_combined_bid_micro = 980_000` (`$0.98`), `include_variance_spread = true`, `hysteresis_tick_buffer = 0.25`.
- **Entry & timing guards:** `max_entry_price = $0.65`, `max_entry_prob = 0.80`, `max_cross_tick_loss_micro = 80_000` (`$0.08`), `entry_open_delay_secs = 15`, `entry_cutoff_secs = 120`.
- **Risk & lead-lag:** `cap_tier1_reducing_to_delta = true`, `tier1_hedge_trigger_price = null` (`off`), `hedge_skew_mult = 1.0`, `lead_lag_cancel_threshold = 0.015` (`1000ms` window / `1000ms` cooldown, directional `true`).
- **Baseline diagnostic status:** On the 21-capture (`241`-window, `351`-fill) development corpus, Config #6 showed repeatable maker pair-merge edge (`+$65.24`, `+$0.0598/pair`) offset by directional adverse-selection settlement losses (`-$33.56`, `85%` loss rate across `40` exposed windows, `0` executed flattens). At `351 < 400` fills, its survival gate verdict is **`UNDERPOWERED`** ([docs/sweep-results-investigation.md](docs/sweep-results-investigation.md), [docs/merge-edge-retention-feasibility.md](docs/merge-edge-retention-feasibility.md)).

### Frozen Candidate: `cand_b_admit_cancel`

**[Verified Fact — branch commit `3870cbb`]** `cand_b_admit_cancel` is the exact frozen candidate rule under forward-testing evaluation, not a general description of toxic-flow gating. It tests **[Hypothesis]** that suppressing flat first-leg quotes when its frozen predicate fires reduces single-leg adverse selection without destroying pair-merge edge.

- **Frozen predicate and action (all conditions locked):**
  - **Spread threshold:** Spread `>= 30,000` micro-USDC (`$0.03`).
  - **Flow window:** `5,000ms` rolling window.
  - **Order-flow sign:** Signed Polymarket `Sell - Buy` shares `> 0` over the `5,000ms` window.
  - **Flat inventory only:** `q_up == 0 && q_dn == 0` (applies only when flat).
  - **Frozen action (`admit-cancel`):** Block new first-leg quotes and cancel existing resting first-leg quotes while the predicate holds.
- **Exact CLI delta vs. Baseline Config #6:** Identical to Config #6 on every parameter, plus three explicit CLI flags:
  - `--exp-entry-toxic-spread-micro 30000` (`$0.03` / 3 ticks)
  - `--exp-entry-toxic-flow-window-ms 5000` (`5000ms`)
  - `--exp-entry-toxic-mode admit-cancel`
- **Replay-only isolation:** These flags do not exist in `DEFAULTS.json` or `EngineConfig`. The CLI rejects them at startup unless `--source replay` is active, and `lifecycle` imports zero live order-submission clients from `crates/execution`.

### Paired-Replay Evaluation Approach

**[Verified Fact — branch commit `3870cbb`]** Forward-testing captures record raw live market feeds (`feeds --discover`) with zero strategy logic in the capture path. Offline evaluation runs **paired deterministic replay** (`paired-replay --repeat-baseline`) over each finalized raw capture:

1. Replay **Baseline Config #6** twice (`baseline_run1` and `baseline_run2`) to verify bit-exact replay determinism.
2. Replay **`cand_b_admit_cancel`** once over the identical raw event file under identical conservative FIFO queue assumptions.
3. Verify run/finalization SHA-256 manifests, zero secret leakage, and exact micro-USDC accounting reconciliation (`reconciliation_diff_micro == 0`).

### Evaluation Stage Boundaries

| Stage | Role | Allowed Actions | Current Status |
| :--- | :--- | :--- | :--- |
| **Development (IS)** | Exploratory diagnostics, bug fixes, feature design | Inspect freely, test root causes | `21` captures (`241w`) frozen as IS (burned for OOS) |
| **Candidate Selection** | Rank parameter grids or candidate rules before freezing | Structured sweeps on non-Holdout splits | `cand_b_admit_cancel` frozen for forward replay |
| **Holdout** | Single-shot confirmatory OOS gate under locked SHA & manifest | Sealed storage until pre-registered unlock | Seed `#22` + `#24`–`#26` sealed (manifest pending) |
| **Forward Testing** | Prospective raw feed collection + offline paired replay | Record fresh windows, run frozen paired replay | **[Audit Snapshot — 2026-11-27]** `fwd-20261127-cand_b_v1` recorded as in collection at audit time (not a live state claim — see §4) |
| **Live Trading** | Real-capital CLOB execution & Relayer settlement | **Unavailable in current build** (user authorization does not bypass the code hard gate, and historical Phase 5 docs are not authorization to attempt live execution) | Mainnet `canary-live` hard-gated off in [crates/cli/src/exec_cmd.rs](crates/cli/src/exec_cmd.rs) |

> **Profitability Status [Verified Fact]:** **No forward-testing profitability conclusion has been established** for either Baseline Config #6 or `cand_b_admit_cancel`.

---

## 4. Active Forward-Testing Campaign (`fwd-20261127-cand_b_v1`)

> **Snapshot Warning:** All process, recording, and session counts below represent **[Audit Snapshot — 2026-11-27]** findings. They are not live status guarantees and will advance or terminate independently of this document.

- **Experiment ID:** `fwd-20261127-cand_b_v1`
- **Active Worktree & Branch [Verified Fact / Audit Snapshot]:** Dedicated parallel worktree `entry_selection_adverse_trend` on branch `entry_selection_adverse_trend` (`3870cbb`).
- **Capture Mechanism [Audit Snapshot]:** An active `poly-as.exe` process (`feeds --discover`) was recording live market data inside the `entry_selection_adverse_trend` worktree under `forward_test_pipeline.py` orchestration (`.env` absent by fail-closed check, `EXECUTION_MODE=paper`, `DRY_RUN=true`).
- **Collection Progress at Audit Time [Audit Snapshot]:**
  - **`30` finalized complete 5-minute windows** on disk (`2` completed 15-window capture sessions).
  - **`1` active 15-window recording** in progress at the time of the audit.
  - **`1` additional 15-window session** queued behind the active recording in the current batch.
- **Paired-Replay Progress at Audit Time [Audit Snapshot]:** **`0` paired replays completed** for `fwd-20261127-cand_b_v1`.
- **Observed Operational Anomaly [Audit Snapshot]:** **Session 2 experienced feed-health interruptions** during live recording. During upcoming capture validation and paired replay, inspect Session 2 feed-health events, book-staleness gaps, and window completeness before pooling those windows into headline comparisons.
- **Campaign Collection Target [Planning Target]:** `400`–`500` fresh 5-minute windows spread across `7`–`10` distinct UTC dates. Treat this range strictly as an **operational planning target** for regime breadth and fill accumulation, not a statistical guarantee of adequacy or positive edge.

---

## 5. Data Governance and Holdout

> **Safety Confirmation:** No sealed Holdout files were opened, hashed, or inspected to produce this section. Classification entries combine tracked reports ([docs/merge-edge-retention-feasibility.md](docs/merge-edge-retention-feasibility.md)) and metadata-level **[Audit Snapshot]** inventory findings.

| Capture Group | Governance Classification | Permitted Use | Current State |
| :--- | :--- | :--- | :--- |
| **Captures `#1`–`#21`** (`241` windows, `22-11` to `26-11_00-50-00Z`) | **Development / In-Sample (Burned for OOS)** | Exploratory analysis, replay regression tests, root-cause studies | Permanently burned for OOS after `180`-run sweeps and diagnostics |
| **Capture `#22`** | **Sealed Holdout Seed** | **None** until formal Holdout protocol unlock | Untouched in sweeps and lifecycle runs (must remain sealed) |
| **Capture `#23`** | **Excluded (Incomplete)** | Excluded from Holdout and clean OOS gates | Incomplete `.inprogress.jsonl` capture |
| **Captures `#24`–`#26`** | **Sealed Holdout Candidates** | **None** until formal Holdout protocol unlock | Uninspected candidates reserved for Holdout Vault |
| **`fwd-20261127-cand_b_v1` raw captures** | **Prospective Forward-Testing Corpus** | Frozen paired replay (`Config #6` vs `cand_b_admit_cancel`) only | Isolated under worktree `logs/forward-testing/` |

### Missing Holdout Governance Artifacts

**[Audit Snapshot & Verified on `main`]** A formal git-tracked Holdout manifest (`holdout-manifest.sha256`) and Holdout inspection ledger (`docs/holdout-inspection-ledger.md`), recommended in [docs/merge-edge-retention-feasibility.md](docs/merge-edge-retention-feasibility.md) §F.2, **do not yet exist on `main`**. Do not unlock, replay, or inspect `#22` or `#24`–`#26` until that manifest and single-shot unlock protocol are committed on `main`.

---

## 6. NEX Sandbox (`Amoy 80002`)

**[Verified Fact & Audit Snapshot]** Polymarket's NEX preprod environment (`Chain ID 80002`) is integrated via `poly-as sandbox` (`crates/execution/src/nex.rs`, operator guide in [docs/nex-sandbox-runbook.md](docs/nex-sandbox-runbook.md)):

- **What was previously tested:** Public/read-only connectivity (`sandbox health`, pinned ingress DNS check, market discovery fallback, order-book reads, and public market WebSocket feed reconnect/snapshot rebuilds) has been exercised. `SandboxProfile` fails closed on non-sandbox hosts, non-`80002` chain IDs, and known production exchange contracts, tagging records with `"origin": "sandbox"` and `"network": "nex-amoy-80002"`.
- **What remains unconfigured and unverified:** Authenticated L2 trading (`NEX_API_*`), Amoy POL gas and test `pUSD` funding/token approvals, authenticated user WebSocket reconciliation, and end-to-end order placement/read-back/cancel (`sandbox validate --confirm-sandbox-trade`) remain unconfigured and unverified.
- **Calibration Restriction:** Sampled NEX books lack organic maker/taker microstructure and often sit empty. **NEX is an execution-plumbing testbed only, not a validated strategy-calibration venue.**

---

## 7. Outstanding Engineering Work

**[Verified Fact on `main` (`45fc9e3`)]** PR #80 (`3a13a23`) is already merged on `main` and documented three follow-up telemetry and scoring gaps ([docs/merge-edge-retention-feasibility.md](docs/merge-edge-retention-feasibility.md) §B & §G.2). Direct inspection of `main` confirms all three remain open follow-up engineering tasks:

1. **Merge provenance and flatten fee in `LifecycleMergeEvent` ([crates/cli/src/lifecycle_report.rs](crates/cli/src/lifecycle_report.rs#L276-L285)):** `LifecycleMergeEvent` currently emits `merged_pairs_micro`, `avg_cost_up_micro`, `avg_cost_dn_micro`, `pair_cost_micro`, `realized_gain_micro`, and `total_merged_micro`, with no maker-vs-flatten provenance (`source` / `is_maker`) and no `flatten_fee_micro` when a taker-BUY flatten triggers a merge.
2. **Per-window merge gain/loss breakdown in `WindowEndEvent` ([crates/cli/src/lifecycle_report.rs](crates/cli/src/lifecycle_report.rs#L323-L345)):** `WindowEndEvent` records `merges_count` and `total_pairs_merged_micro`, but omits per-window `merge_gain_micro`, `positive_merge_gain_micro`, and `negative_merge_loss_micro`, forcing external scripts to rescan raw `"event":"merge"` rows.
3. **Incomplete-window exclusion in `Get-SurvivalInputs` ([scripts/sweep-params.ps1](scripts/sweep-params.ps1#L474-L570)):** `Get-SurvivalInputs` currently scores `WindowNets` without filtering out `complete=false` trailing EOF windows, allowing unsettled mark-to-market (`unrealized_pnl_micro`, such as Window `#33`'s `+$1.6056`) to enter realized IS/OOS gate metrics.

**Unmerged Worktree Telemetry Note [Verified Fact — branch commit `3870cbb`]:** Separate forward-testing telemetry (`entry_gate`, `order_lifecycle`, and immutable run/finalization manifest sidecars) exists on branch `entry_selection_adverse_trend` in the active worktree and is not yet merged into `main`. That worktree telemetry supports paired-replay verification for `cand_b_admit_cancel` and does **not** replace the three `main`-branch follow-up tasks listed above.

---

## 8. Recommended Next Actions

**[Recommendation]** Execute these steps in strict priority order:

1. **Protect the active forward-testing batch:** Let the running `fwd-20261127-cand_b_v1` capture sessions finish naturally without touching, rebuilding, or cleaning the `entry_selection_adverse_trend` worktree or its `poly-as.exe` binary.
2. **Validate finalized captures and run paired replay:** Run capture-completeness checks and deterministic paired replay (`Baseline Config #6` vs `cand_b_admit_cancel`) on the completed batch, and specifically investigate the **Session 2 feed-health interruptions** before interpreting window results.
3. **Implement the three `main` telemetry/scoring fixes separately:** Plan and deliver the `LifecycleMergeEvent`, `WindowEndEvent`, and `Get-SurvivalInputs` (`complete=false`) fixes on a dedicated short-lived branch cut from `main`, without touching the active capture worktree.
4. **Formalize Holdout Vault governance before any unlock:** Design and commit a tracked Holdout manifest (`holdout-manifest.sha256` protocol) and inspection ledger (`docs/holdout-inspection-ledger.md`) before unlocking or evaluating Holdout seed `#22` or candidates `#24`–`#26`.
5. **Reassess research direction from paired-replay evidence:** Review actual `fwd-20261127-cand_b_v1` paired-replay outcomes first, then decide the next experiment or collection step. **Do not start another capture batch automatically.**
