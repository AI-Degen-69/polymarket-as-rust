# Accounting Telemetry & Default-Resolution Audit

> [!NOTE]
> **Scope.** This report keeps the two sections that are still current: validation of the five accounting telemetry
> fields, and the audit of how the promoted configuration resolves at runtime. The `18`-configuration leaderboard and
> the G1–G6 repair history it previously carried were superseded by the `9`-configuration boundary sweep and were
> removed; the current ranking, winner justification and promoted defaults live in
> [boundary-sweep-selection-report.md](boundary-sweep-selection-report.md).

- **Dataset (raw output since pruned):** `logs/sweeps/25-11_16-21-55Z_18runs_20caps-226w` (`18` configurations × `20` captures = `360` JSONL files, `226` 5-minute windows, `QueueDecay=conservative`); the findings below are the retained record of that audit.
- **Verification Standard:** [strategy-verification-spec.md](strategy-verification-spec.md)

---

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
