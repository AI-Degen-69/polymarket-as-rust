## Summary

- Scope:
- Why this change exists:
- Issue (if applicable):

## Key Changes

-

## Verification

Paste the commands run and their outputs. Name any checks not run and why (for example, Rust and PowerShell checks skipped on a docs-only PR).

```text
<paste verification output>
```

## Checklist

- [ ] `cargo fmt --all -- --check` passes
- [ ] `cargo clippy --workspace --all-targets -- -D warnings` passes (if Rust code changed)
- [ ] `cargo test --workspace` passes with zero skipped tests (if Rust code changed)
- [ ] PowerShell AST syntax check and `pwsh -NoProfile -File scripts/sweep-style.tests.ps1` pass (if `scripts/*.ps1` changed)
- [ ] Zero `.env`, `.env.*` (except `.env.example`), secrets, private keys, `logs/`, `data/`, or `*.jsonl` artifacts in diff
- [ ] Relevant documentation updated, and if defaults or calibration changed, `DEFAULTS.json` and all consumers are synchronized (`AGENTS.md` §10)
