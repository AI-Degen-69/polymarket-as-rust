# Coding Standards

Judgment-call conventions that cannot be enforced by a linter or hook. See [CONSTRAINTS.md](CONSTRAINTS.md) for the mandatory mechanical checks (Rust fmt/clippy/tests, PowerShell checks, and hooks).

## Time and resolution parameters

Validate any time-window or resolution parameter against the granularity of the backing data structure at every boundary that accepts the parameter: config parsing, CLI flags, and sweep-script overrides.

Example: `station2_twap.rs` names `LEAD_LAG_BUCKET_MS = 50` and rejects lookback windows in `1..49` ms:

```rust
if self.lead_lag_window_ms > 0 && self.lead_lag_window_ms < LEAD_LAG_BUCKET_MS {
    return Err(err_desc("station2 lead_lag_window_ms must be 0 or >= 50ms"));
}
```

A value below the ring-buffer resolution silently misfires. Reject it where it is parsed, not where it is read.

## Option defaults and CLI disablement

Any `Option<T>` config field defaulting to `Some(...)` must expose a CLI parser that accepts an explicit `off`/`none`.

Example: `MaxCrossTickLossOverride` in `cli/src/main.rs` parses the literal `off` into the `Off` variant, which maps to `None`:

```rust
"off" => return Ok(MaxCrossTickLossOverride::Off),
// ...
MaxCrossTickLossOverride::Off => None,
MaxCrossTickLossOverride::Value(v) => Some(v),
```

Without an explicit `off`, a `Some(...)` default can never be disabled by the operator.

## Fixed-width console output contract

All live progress tickers, status lines, and framed terminal reports must preserve static column alignment:

- **Single source for CLI widths:** Define and reuse column widths in `crates/cli/src/width.rs` (`TERMINAL_WIDTH = 140`, `PHASE_RULE_WIDTH = 124`, `RULE_WIDTH = 82`, `EVENT_BADGE_WIDTH = 9`, `PHASE_BADGE_WIDTH = 10`, `TIER_BADGE_WIDTH = 4`, `LEG_WIDTH = 21`, `LATENCY_VALUE_WIDTH = 10`, `SUMMARY_LABEL_WIDTH = 16`, `SUMMARY_VALUE_WIDTH = 12`) instead of ad-hoc local widths.
- **ANSI-aware padding:** Measure visible scalar width after stripping CSI/SGR escapes and pad values to fixed width *before* wrapping in ANSI color codes (`strip_ansi`, `visible_width`, `pad_visible_left`, `pad_visible_right` in `crates/cli/src/width.rs`; matching ANSI helpers in `scripts/sweep-params.ps1` verified by `scripts/sweep-style.tests.ps1`).
- **Fixed-width fields and breathing room:** Pin timestamps on the left, pad badges and numeric/composite columns so vertical delimiters stay aligned as counters grow, and insert blank lines around phase and epoch transitions.

## Runtime configuration derivation

Runtime entrypoints, simulation harnesses, and CLI commands must derive strategy, risk, and lifecycle configuration from `EngineConfig` (`EngineConfig::from_env()` / `EngineConfig::default()`, backed by compile-time embedded `DEFAULTS.json`).

Do not rely on standalone library constructors (`PricerConfig::from_gamma()`, `RiskConfig::default()`, `LifecycleConfig::default()`) for runtime defaults: those exist for unit-test isolation and intentionally differ from `DEFAULTS.json`.
