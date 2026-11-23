# Coding Standards

Judgment-call conventions that cannot be enforced by a linter or hook. See `AGENTS.md` for the mechanical checks (fmt, clippy, tests, hooks).

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
