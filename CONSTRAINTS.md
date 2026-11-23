# Constraints

The written quality bar for this repo. These are non-negotiable gates; anything that weakens them fails review.

## Quality bars

- **Zero clippy warnings.** `cargo clippy --workspace --all-targets -- -D warnings` must pass.
- **Zero skipped tests.** `#[ignore]` and test-level `#[cfg(feature = "...")]` skips stay out of `main`.
- **Zero `.env` / secret / `logs/` commits.** No `.env`, API key, private key, or `logs/*.jsonl` artifact is ever staged.
- **100% pass** on `cargo test --workspace`.
- **Formatting clean.** `cargo fmt --all -- --check` passes.

## Rules

- Do not lower, relax, or annotate around these bars.
- Do not bypass hooks with `--no-verify`.
- A bar that cannot be met is a reason to stop and ask, not to silently weaken the gate.
