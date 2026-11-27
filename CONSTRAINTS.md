# Constraints

The written quality bar for this repo. These are non-negotiable gates; anything that weakens them fails review.

## Quality bars

### Rust

- **Formatting clean.** `cargo fmt --all -- --check` must pass.
- **Zero clippy warnings.** `cargo clippy --workspace --all-targets -- -D warnings` must pass. Workspace lints in `Cargo.toml` also deny `unwrap_used`, `expect_used`, `panic`, `todo`, `unimplemented`, `indexing_slicing`, and `unsafe_op_in_unsafe_fn` in non-test targets.
- **100% pass** on `cargo test --workspace`.
- **Zero skipped tests.** `#[ignore]` and test-level `#[cfg(feature = "...")]` skips stay out of `main`.

### PowerShell (`scripts/`)

- **PowerShell AST syntax clean.** Every `scripts/*.ps1` file must parse with zero errors under `[System.Management.Automation.Language.Parser]::ParseFile(...)`.
- **PowerShell style tests pass.** `pwsh -NoProfile -File scripts/sweep-style.tests.ps1` must pass.

### Prohibited files and artifacts

- **Zero secrets or runtime data commits.** Never stage or commit `.env`, `.env.*` (except `.env.example`), API keys, private keys, `logs/`, `data/`, `*.jsonl`, or `*.log`.

## Enforcement matrix

| Gate | `.githooks/pre-commit` | `.github/workflows/ci.yml` | Manual verification |
| :--- | :--- | :--- | :--- |
| `cargo fmt --all -- --check` | **Enforced** | **Enforced** (unless docs-only PR) | Required before commit |
| `cargo clippy --workspace --all-targets -- -D warnings` | Not run | **Enforced** (unless docs-only PR) | Required before commit (except docs-only) |
| `cargo test --workspace` | Not run | **Enforced** (unless docs-only PR) | Required before commit (except docs-only) |
| Zero `#[ignore]` / feature-skipped tests | Not checked | Not checked | **Manual review required** |
| PowerShell AST syntax (`scripts/*.ps1`) | Not run | **Enforced** (unless docs-only PR) | Required when editing `scripts/*.ps1` |
| `pwsh -NoProfile -File scripts/sweep-style.tests.ps1` | Not run | **Enforced** (unless docs-only PR) | Required when editing `scripts/*.ps1` |
| Prohibited files (`.env`, `logs/`) | **Enforced** (regex `^(.*/)?\.env$\|^logs/`) | Not checked | **Manual diff review required** |
| Prohibited files (`.env.*`, `data/`, `*.jsonl`, `*.log`, secret content) | Not checked by hook regex | Not checked | **Manual diff review required** |

> Note: `.github/workflows/ci.yml` skips the `rust-windows` job (both Rust and PowerShell checks) when a PR modifies only `*.md` or `docs/**` via `paths-ignore`.

## Hook activation

Enable the tracked pre-commit hook once per clone:

```powershell
git config core.hooksPath .githooks
```

## Rules

- Do not lower, relax, or annotate around these bars.
- Do not bypass hooks with `--no-verify`.
- A bar that cannot be met is a reason to stop and ask, not to silently weaken the gate.
